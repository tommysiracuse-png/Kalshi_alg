"""API and capital admission gates for the sharded fleet."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Literal, Mapping, Optional

from core.fleet_models import FleetCapacity, QuoteSide

# Hard ceiling for ``fleetRuntime.allocationOversubscription``.  Budgets are a
# proxy for expected exposure; the venue balance (not the multiplier) is the
# hard limit, but a multiplier past this is a configuration mistake.
MAX_ALLOCATION_OVERSUBSCRIPTION = 10.0

# Live-cap hysteresis (oversubscribed fleets only).  New sides on a shard are
# withheld once its live exposure reaches 100% of allocatable cash and are
# re-admitted only when it has fallen below this fraction of allocatable, so a
# shard hovering at the cap does not cancel and re-grant every refresh.
LIVE_CAP_RELEASE_FRACTION = 0.8


def calculate_fleet_capacity(
    *,
    api_tier: str,
    read_refill_rate: int,
    write_refill_rate: int,
    requested_markets: int,
    cash_available_units: int,
    cash_committed_units: int = 0,
    freshness_seconds: float = 20.0,
    write_utilization_limit: float = 0.85,
    cash_reserve_fraction: float = 0.20,
    normal_request_cost: int = 10,
) -> FleetCapacity:
    if requested_markets < 0:
        raise ValueError("requested_markets must be non-negative")
    if freshness_seconds <= 0 or normal_request_cost <= 0:
        raise ValueError("freshness_seconds and normal_request_cost must be positive")
    if not 0 < write_utilization_limit < 1 or not 0 <= cash_reserve_fraction < 1:
        raise ValueError("utilization and reserve fractions are invalid")

    normal_sides = math.floor(
        max(0, write_refill_rate) * write_utilization_limit * freshness_seconds / normal_request_cost
    )
    available = max(0, int(cash_available_units))
    allocatable = math.floor(available * (1.0 - cash_reserve_fraction))
    reserved = available - allocatable
    committed = max(0, int(cash_committed_units))
    api_known = bool(str(api_tier).strip()) and read_refill_rate > 0 and write_refill_rate > 0
    # A normal market needs two quote sides.  Capacity is therefore measured
    # in complete markets for admission; one unused side is intentionally not
    # enough to start a normal market.
    capacity_market_limit = normal_sides // 2
    capacity_ok = capacity_market_limit >= requested_markets
    cash_consistent = committed <= allocatable
    usable_capacity = capacity_market_limit > 0 or requested_markets == 0
    gate_open = api_known and usable_capacity and cash_consistent
    errors = []
    if not api_known:
        errors.append("API tier or token limits unavailable")
    if not capacity_ok:
        errors.append(f"write capacity admits {normal_sides} quote sides for {requested_markets} markets")
    if not cash_consistent:
        errors.append("committed capital exceeds allocatable cash")
    admitted_markets = min(requested_markets, capacity_market_limit) if gate_open else 0
    admitted_sides = min(normal_sides, admitted_markets * 2) if gate_open else 0
    fatal_errors = []
    if not api_known:
        fatal_errors.append("API tier or token limits unavailable")
    if not cash_consistent:
        fatal_errors.append("committed capital exceeds allocatable cash")
    return FleetCapacity(
        api_tier=str(api_tier),
        read_refill_rate=max(0, int(read_refill_rate)),
        write_refill_rate=max(0, int(write_refill_rate)),
        normal_quote_side_capacity=normal_sides,
        requested_markets=requested_markets,
        admitted_markets=admitted_markets,
        admitted_quote_sides=admitted_sides,
        cash_available_units=available,
        cash_allocatable_units=allocatable,
        cash_committed_units=committed,
        reserved_cash_units=reserved,
        freshness_seconds=float(freshness_seconds),
        gate_open=gate_open,
        reduction_only=not gate_open,
        # A shortfall against the request is an expected partial-capacity
        # condition, not a fail-closed error.  Keep it in omitted_reason and
        # leave error empty so usable markets can quote.
        error="; ".join(fatal_errors),
        capacity_market_limit=capacity_market_limit,
        capacity_limited=bool(gate_open and not capacity_ok),
        omitted_markets=max(0, requested_markets - admitted_markets),
        omitted_reason=("capacity" if gate_open and not capacity_ok else "; ".join(errors)),
    )


@dataclass(frozen=True)
class AllocationRequest:
    ticker: str
    series_id: str
    rank: int
    preferred_side: QuoteSide
    first_side_notional_units: int
    second_side_notional_units: int = 0
    reducing_sides: tuple[QuoteSide, ...] = ()
    # Kalshi exchange shard hosting the market. Cash is local to a shard, so a
    # market can only be funded from its own shard's balance.
    exchange_index: int = 0


@dataclass(frozen=True)
class ShardAllocation:
    """Per-exchange-shard accounting of one allocation pass (all cash units)."""

    exchange_index: int
    cash_units: int
    # Cash minus the reserve: the hard cap for LIVE exposure on the shard.
    allocatable_units: int
    # allocatable x oversubscription: the cap for committed side budgets.
    budget_cap_units: int
    committed_units: int
    live_exposure_units: int
    markets: int
    funded_markets: int
    granted_sides: int
    skipped_markets: int
    withheld_markets: int
    # Extra budget cap needed to fund every skipped first side on the shard.
    shortfall_units: int
    # Live-cap withholding is in force on the shard: markets without a held
    # side get no new side until live exposure falls below
    # LIVE_CAP_RELEASE_FRACTION x allocatable (oversubscribed fleets only).
    reduction_only: bool


@dataclass(frozen=True)
class AllocationResult:
    gate_open: bool
    sides_by_ticker: Mapping[str, tuple[QuoteSide, ...]]
    committed_units: int
    allocatable_units: int
    reserved_units: int
    error: str = ""
    oversubscription: float = 1.0
    budget_cap_units: int = 0
    live_exposure_units: int = 0
    shards: tuple[ShardAllocation, ...] = ()
    # Fleet-wide live-cap withholding (only used when the venue reported no
    # per-shard balance breakdown); feeds the next pass's hysteresis.
    withholding: bool = False
    requested_markets: int = 0
    admitted_markets: int = 0
    omitted_markets: int = 0
    omitted_reason: str = ""


def _dollars(units: int) -> str:
    return f"${units / 10_000:,.2f}"


def _release_threshold(base: int) -> int:
    return int(math.ceil(base * LIVE_CAP_RELEASE_FRACTION))


def live_cap_withholding(live: int, base: int, *, was_withholding: bool) -> bool:
    """Sticky live-cap state: trip at >= 100% of ``base``, release below 80%."""
    if base <= 0:
        return False
    if live >= base:
        return True
    return bool(was_withholding and live >= _release_threshold(base))


class CapitalAllocator:
    def __init__(
        self,
        *,
        cash_reserve_fraction: float = 0.20,
        series_exposure_fraction: float = 0.10,
        oversubscription: float = 1.0,
    ) -> None:
        if not 0 <= cash_reserve_fraction < 1:
            raise ValueError("cash_reserve_fraction must be in [0, 1)")
        if not 0 < series_exposure_fraction <= 1:
            raise ValueError("series_exposure_fraction must be in (0, 1]")
        if not 1.0 <= float(oversubscription) <= MAX_ALLOCATION_OVERSUBSCRIPTION:
            raise ValueError(f"oversubscription must be in [1.0, {MAX_ALLOCATION_OVERSUBSCRIPTION}]")
        self.cash_reserve_fraction = cash_reserve_fraction
        self.series_exposure_fraction = series_exposure_fraction
        self.oversubscription = float(oversubscription)

    def allocate(
        self,
        requests: Iterable[AllocationRequest],
        *,
        available_cash_units: int,
        existing_committed_units: int = 0,
        existing_series_units: Optional[Mapping[str, int]] = None,
        quote_side_capacity: Optional[int] = None,
        cash_by_exchange_units: Optional[Mapping[int, int]] = None,
        exposure_by_ticker: Optional[Mapping[str, int]] = None,
        held_tickers: Optional[Iterable[str] | Mapping[str, Iterable[QuoteSide]]] = None,
        withheld_shards: Optional[Iterable[int]] = None,
        fleet_withholding: bool = False,
    ) -> AllocationResult:
        """Grant quote sides market by market.

        At ``oversubscription == 1.0`` (the default) this is exactly the
        historical allocator: a side is granted only when its whole budget
        fits in un-multiplied allocatable cash, per shard and fleet-wide, in
        screener rank order; ``exposure_by_ticker``, ``held_tickers``,
        ``withheld_shards`` and ``fleet_withholding`` are ignored.

        Above 1.0 two caps apply, per shard and fleet-wide:

        * ``budget cap`` = allocatable cash x ``oversubscription``: every
          granted side commits its market's full side budget against it.
        * ``live cap`` = allocatable cash (reserve kept, never multiplied) for
          ACTUAL exposure - resting-order notional plus position exposure,
          ``existing_committed_units`` fleet-wide and ``exposure_by_ticker``
          per shard.  This pass only decides which markets may *start* a new
          side; the execution broker enforces the same cap on every order
          between passes.  The withhold is sticky and hysteretic: markets
          that already hold a side keep it - a key of ``exposure_by_ticker``
          keeps its first side, and ``held_tickers`` (the previous pass's
          ``sides_by_ticker`` mapping, or a plain list meaning first side)
          keeps exactly the sides granted then; no other side is granted on
          the shard while its live exposure is at or above 100% of
          allocatable, and new sides are re-admitted only once it falls
          below ``LIVE_CAP_RELEASE_FRACTION`` (80%).
          ``withheld_shards`` / ``fleet_withholding`` carry the previous
          pass's state in; ``ShardAllocation.reduction_only`` /
          ``AllocationResult.withholding`` carry the new state out.
          Reducing sides never need capital.

        Within un-multiplied cash sides are granted in screener rank order.
        In the oversubscribed region (budgets past un-multiplied cash) markets
        holding a side are served first, then the rest by rank, so a market
        with risk to manage keeps its side.
        """
        ordered = sorted(requests, key=lambda item: (item.rank, item.ticker))
        requested_count = len(ordered)
        available = max(0, int(available_cash_units))
        allocatable = math.floor(available * (1.0 - self.cash_reserve_fraction))
        reserved = available - allocatable
        oversubscription = self.oversubscription
        oversubscribed = oversubscription > 1.0
        budget_cap = math.floor(allocatable * oversubscription)
        live_total = max(0, int(existing_committed_units))
        committed = live_total
        series_used = {key: max(0, int(value)) for key, value in (existing_series_units or {}).items()}
        # The series cap applies to the same (oversubscribed) budget pool, so
        # the relative concentration limit is unchanged by the multiplier.
        series_limit = math.floor(budget_cap * self.series_exposure_fraction)
        side_capacity = quote_side_capacity if quote_side_capacity is not None else len(ordered) * 2
        exposure = {
            str(ticker): max(0, int(units or 0)) for ticker, units in (exposure_by_ticker or {}).items()
        }
        # Markets that keep a side under a live-cap withhold: anything with
        # inventory or a resting order (first side), plus the previous pass's
        # grants (exactly the sides granted then when ``held_tickers`` is a
        # mapping ticker -> sides; first side only for a plain list).
        held_sides: dict[str, set[str]] = {}
        if isinstance(held_tickers, Mapping):
            for ticker, sides in held_tickers.items():
                held_sides[str(ticker)] = {str(side) for side in (sides or ())}
        else:
            for ticker in (held_tickers or ()):
                held_sides.setdefault(str(ticker), set())
        held = set(exposure) | set(held_sides)
        previously_withheld = {int(index) for index in (withheld_shards or ())}

        def keeps_side(item: AllocationRequest, side: QuoteSide, *, first: bool) -> bool:
            """Under a withhold: a held market keeps its held sides (first side if unknown)."""
            if str(side) in held_sides.get(item.ticker, ()):
                return True
            return first and item.ticker in held
        # Kalshi balances are local to an exchange shard: a market on shard 3
        # (tennis/baseball) cannot be funded from shard-0 cash, and the venue
        # rejects every such order with user_not_found. When the venue reports a
        # per-shard breakdown, each market draws only from its own shard's pool.
        shard_cash: Optional[dict[int, int]] = None
        shard_base: dict[int, int] = {}
        shard_cap: dict[int, int] = {}
        shard_live: dict[int, int] = {}
        shard_used: dict[int, int] = {}
        shard_withholding: dict[int, bool] = {}
        if cash_by_exchange_units is not None:
            shard_cash = {int(index): max(0, int(cash)) for index, cash in cash_by_exchange_units.items()}
            shard_base = {
                index: math.floor(cash * (1.0 - self.cash_reserve_fraction)) for index, cash in shard_cash.items()
            }
            shard_cap = {index: math.floor(base * oversubscription) for index, base in shard_base.items()}
            for item in ordered:
                index = int(item.exchange_index)
                shard_live[index] = shard_live.get(index, 0) + exposure.get(item.ticker, 0)
            if oversubscribed:
                for index in set(shard_base) | set(shard_live):
                    shard_withholding[index] = live_cap_withholding(
                        shard_live.get(index, 0), shard_base.get(index, 0),
                        was_withholding=index in previously_withheld,
                    )
        # Fleet-wide live cap: only meaningful without a shard breakdown (with
        # one, every desired market is covered by its shard's cap).
        withholding = bool(
            oversubscribed and shard_cash is None
            and live_cap_withholding(live_total, allocatable, was_withholding=bool(fleet_withholding))
        )

        def fund(
            item: AllocationRequest, notional: int, side: QuoteSide, *, past_cash: bool, first: bool,
        ) -> Optional[str]:
            """Commit ``notional`` for ``item``'s ``side`` or return the skip reason."""
            nonlocal committed
            index = int(item.exchange_index)
            series_total = series_used.get(item.series_id, 0)
            if shard_cash is not None:
                base = shard_base.get(index, 0)
                limit = shard_cap.get(index, 0) if past_cash else base
                if shard_used.get(index, 0) + notional > limit:
                    return f"exchange shard {index} unfunded"
                if notional > 0 and shard_withholding.get(index) and not keeps_side(item, side, first=first):
                    return f"exchange shard {index} live exposure at allocatable cash"
            elif withholding and notional > 0 and not keeps_side(item, side, first=first):
                return "live exposure at allocatable cash"
            limit = budget_cap if past_cash else allocatable
            if committed + notional > limit:
                return "capital"
            if series_total + notional > series_limit:
                return "series limit"
            committed += notional
            series_used[item.series_id] = series_total + notional
            shard_used[index] = shard_used.get(index, 0) + notional
            return None

        # Capacity shortfalls are partial admission.  Keep reducing-side
        # carryovers first, then admit only complete two-sided normal markets
        # in deterministic rank order.  This prevents a half-admitted market
        # from reaching a worker when the venue can only fund one side.
        reducing = [item for item in ordered if item.reducing_sides]
        normal = [item for item in ordered if not item.reducing_sides]
        if side_capacity < len(ordered):
            reducing = reducing[:side_capacity]
            normal_slots = max(0, side_capacity - len(reducing)) // 2
            ordered = sorted(reducing + normal[:normal_slots], key=lambda item: (item.rank, item.ticker))

        result: dict[str, list[QuoteSide]] = {item.ticker: [] for item in ordered}
        # Reducing actions consume an admission slot but never consume additional capital.
        for item in ordered:
            if item.reducing_sides:
                side = item.reducing_sides[0]
                result[item.ticker].append(side)
                side_capacity -= 1

        skipped: dict[str, str] = {}

        def grant_pass(
            candidates: list[tuple[AllocationRequest, int, QuoteSide]],
            *,
            record_skips: bool,
            respect_capacity: bool,
            first: bool,
        ) -> None:
            """Rank order within un-multiplied cash, then inventory-first past it."""
            nonlocal side_capacity
            pending: list[tuple[AllocationRequest, int, QuoteSide, str]] = []
            for item, notional, side in candidates:
                if respect_capacity and side_capacity <= 0:
                    break
                reason = fund(item, notional, side, past_cash=False, first=first)
                if reason is None:
                    result[item.ticker].append(side)
                    side_capacity -= 1
                else:
                    pending.append((item, notional, side, reason))
            if oversubscribed:
                pending.sort(key=lambda entry: (0 if entry[0].ticker in held else 1, entry[0].rank, entry[0].ticker))
            for item, notional, side, reason in pending:
                if oversubscribed and not (respect_capacity and side_capacity <= 0):
                    reason = fund(item, notional, side, past_cash=True, first=first) or ""
                    if not reason:
                        result[item.ticker].append(side)
                        side_capacity -= 1
                        continue
                if record_skips and reason:
                    skipped[item.ticker] = reason

        # First sides are funded in rank order. A market whose first side cannot
        # be funded (capital or series cap) is skipped so the rest of the fleet
        # still quotes; skips are surfaced through the result error summary.
        grant_pass(
            [
                (item, max(0, item.first_side_notional_units), item.preferred_side)
                for item in ordered
                if not result[item.ticker]
            ],
            record_skips=True,
            respect_capacity=False,
            first=True,
        )

        # Second sides are opportunistic and retain screener rank order.
        second_sides: list[tuple[AllocationRequest, int, QuoteSide]] = []
        for item in ordered:
            if item.second_side_notional_units <= 0:
                continue
            other: Literal["yes", "no"] = "no" if item.preferred_side == "yes" else "yes"
            if other in result[item.ticker]:
                continue
            second_sides.append((item, item.second_side_notional_units, other))
        grant_pass(second_sides, record_skips=False, respect_capacity=True, first=False)

        frozen = {ticker: tuple(sides) for ticker, sides in result.items()}
        shards = self._shard_summary(
            ordered, frozen, skipped, shard_cash, shard_base, shard_cap, shard_used, shard_live, shard_withholding,
        )
        admitted = sum(1 for sides in frozen.values() if sides)
        if admitted == 0 and ordered:
            reasons = ", ".join(f"{t} ({r})" for t, r in list(skipped.items())[:3])
            return AllocationResult(
                False, {}, committed, allocatable, reserved, f"no market could be funded: {reasons}",
                oversubscription, budget_cap, live_total, shards, withholding,
                requested_markets=requested_count, admitted_markets=0, omitted_markets=requested_count, omitted_reason="capital",
            )
        error = ""
        if skipped:
            sample = ", ".join(f"{t} ({r})" for t, r in list(skipped.items())[:3])
            detail = self._shortfall_detail(shards, skipped, allocatable, budget_cap, live_total, committed)
            detail_text = f" ({detail})" if detail else ""
            error = f"skipped {len(skipped)} of {len(ordered)} markets{detail_text}: {sample}"
        return AllocationResult(
            True, frozen, committed, allocatable, reserved, error,
            oversubscription, budget_cap, live_total, shards, withholding,
            requested_markets=requested_count, admitted_markets=admitted,
            omitted_markets=max(0, requested_count - admitted), omitted_reason="capacity" if requested_count > admitted else "",
        )

    @staticmethod
    def _shard_summary(
        ordered: list[AllocationRequest],
        sides: Mapping[str, tuple[QuoteSide, ...]],
        skipped: Mapping[str, str],
        shard_cash: Optional[dict[int, int]],
        shard_base: Mapping[int, int],
        shard_cap: Mapping[int, int],
        shard_used: Mapping[int, int],
        shard_live: Mapping[int, int],
        shard_withholding: Mapping[int, bool],
    ) -> tuple[ShardAllocation, ...]:
        if shard_cash is None:
            return ()
        indexes = sorted(set(shard_cash) | {int(item.exchange_index) for item in ordered})
        summary = []
        for index in indexes:
            members = [item for item in ordered if int(item.exchange_index) == index]
            base = shard_base.get(index, 0)
            cap = shard_cap.get(index, 0)
            used = shard_used.get(index, 0)
            live = shard_live.get(index, 0)
            unfunded = [
                item for item in members
                if skipped.get(item.ticker, "").endswith("unfunded")
            ]
            withheld = sum(1 for item in members if "live exposure" in skipped.get(item.ticker, ""))
            need = sum(max(0, item.first_side_notional_units) for item in unfunded)
            shortfall = max(0, need - max(0, cap - used)) if unfunded else 0
            summary.append(ShardAllocation(
                exchange_index=index,
                cash_units=shard_cash.get(index, 0),
                allocatable_units=base,
                budget_cap_units=cap,
                committed_units=used,
                live_exposure_units=live,
                markets=len(members),
                funded_markets=sum(1 for item in members if sides.get(item.ticker)),
                granted_sides=sum(len(sides.get(item.ticker, ())) for item in members),
                skipped_markets=sum(1 for item in members if item.ticker in skipped),
                withheld_markets=withheld,
                shortfall_units=shortfall,
                reduction_only=bool(shard_withholding.get(index, False)),
            ))
        return tuple(summary)

    @staticmethod
    def _shortfall_detail(
        shards: tuple[ShardAllocation, ...],
        skipped: Mapping[str, str],
        allocatable: int,
        budget_cap: int,
        live_total: int,
        committed: int,
    ) -> str:
        parts = []
        for shard in shards:
            if shard.skipped_markets == 0:
                continue
            if shard.withheld_markets:
                parts.append(
                    f"shard {shard.exchange_index}: {shard.withheld_markets} withheld, live exposure "
                    f"{_dollars(shard.live_exposure_units)} >= allocatable {_dollars(shard.allocatable_units)}"
                )
            other = shard.skipped_markets - shard.withheld_markets
            if other > 0:
                parts.append(
                    f"shard {shard.exchange_index}: {other} skipped, short "
                    f"{_dollars(shard.shortfall_units)} of budget cap {_dollars(shard.budget_cap_units)}"
                )
        if not shards and any(reason in {"capital", "live exposure at allocatable cash"} for reason in skipped.values()):
            if any(reason == "live exposure at allocatable cash" for reason in skipped.values()):
                parts.append(f"live exposure {_dollars(live_total)} >= allocatable {_dollars(allocatable)}")
            else:
                parts.append(f"budget cap {_dollars(budget_cap)} reached, committed {_dollars(committed)}")
        return "; ".join(parts)
