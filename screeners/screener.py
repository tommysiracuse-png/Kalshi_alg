"""Venue-neutral market screening and desired-fleet reconciliation."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

from clients.base_client import BaseClient
from clients.models import Market, MarketQuery
from core.fleet_models import ScreenerEvent, ScreenerPick, ScreenerUpdate
from screeners.kalshi_screener import bounded_market_scan_limit, build_export_dataframe, screen_markets

# Running markets keep their fleet slot while they rank within this multiple of
# max_bots. Restarting a market costs its queue position and warm models and
# parks it in `startup` (unable to quote), so churn is far more expensive than
# holding a slightly lower-ranked incumbent.
FLEET_RETENTION_MULTIPLIER = 1.5

# How long a funded-shard lookup stays valid. Kalshi balances are local to an
# exchange shard (0 default, 2 crypto, 3 tennis/baseball); a market on a shard
# holding no cash rejects every order with user_not_found, so such markets are
# excluded from the screen up front rather than wasting fleet slots and API
# budget on doomed orders.
FUNDED_SHARDS_TTL_SECONDS = 60.0

# ScreenerPick.selection_reason of a restart carryover: an exchange position
# found at fleet start in a market the screener did not pick.  The manager
# grants such a market only the side that reduces the position (see
# fleet_runtime/manager.py) - it exists to manage inventory, not to add to it.
EXCHANGE_POSITION_REASON = "exchange_position"

# Venue market statuses in which a position can no longer be traded out of;
# positions there are left alone at start (an empty status is treated as open).
CLOSED_MARKET_STATUSES = frozenset({"closed", "settled", "finalized", "determined", "expired", "inactive"})
# Venue spellings of "trading now": the screener lists ``status=open`` and the
# market endpoint answers ``active`` for the same markets.
OPEN_MARKET_STATUSES = frozenset({"open", "active"})

LOGGER = logging.getLogger(__name__)


def screener_horizon_verdict(market: Any, settings: Mapping[str, object], *, now_ms: Optional[int] = None) -> str:
    """Why ``market`` lies outside the session screener's status / time-to-close
    window; ``""`` when it is inside (or the session carries no such setting).

    Reads the same fields ``kalshi_screener.market_passes_safety_filters``
    does: the listing ``status`` and the earliest of close / expected
    expiration / expiration (``compute_cutoff_times``) against
    ``min_time_to_close_hrs`` / ``max_time_to_close_hrs``.
    """
    wanted = str(settings.get("status") or "").strip().lower()
    status = str(getattr(market, "status", "") or "").strip().lower()
    if wanted and status:
        same = status == wanted or (status in OPEN_MARKET_STATUSES and wanted in OPEN_MARKET_STATUSES)
        if not same:
            return f"status {status!r} is not the screened {wanted!r}"
    minimum = settings.get("min_time_to_close_hrs")
    maximum = settings.get("max_time_to_close_hrs")
    if minimum is None and maximum is None:
        return ""
    stamps = [
        int(value) for value in (
            getattr(market, "close_time_ms", None),
            getattr(market, "expected_expiration_time_ms", None),
            getattr(market, "expiration_time_ms", None),
        ) if value is not None
    ]
    if not stamps:
        return "no close time"
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    hours = (min(stamps) - current) / 3_600_000.0
    if minimum is not None and hours < float(minimum):
        return f"closes in {hours:.1f} h, under the {float(minimum):g} h minimum"
    if maximum is not None and hours > float(maximum):
        return f"closes in {hours:.1f} h, over the {float(maximum):g} h maximum"
    return ""


def _iso_from_ms(value: Optional[int]) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _monitor_rank(value: object) -> Optional[int]:
    try:
        number = float(value)
        return int(number) if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def market_to_screen_payload(market: Market) -> Dict[str, object]:
    def dollars(value: Optional[int]) -> Optional[str]:
        return None if value is None else f"{value / 10_000:.4f}"

    def count(value: Optional[int]) -> Optional[str]:
        return None if value is None else f"{value / 100:.2f}"

    return {
        "ticker": market.market_id,
        "exchange_index": market.exchange_index,
        "title": market.title,
        "status": market.status,
        "series_ticker": market.series_id,
        "event_ticker": market.event_id,
        "close_time": _iso_from_ms(market.close_time_ms),
        "expected_expiration_time": _iso_from_ms(market.expected_expiration_time_ms),
        "expiration_time": _iso_from_ms(market.expiration_time_ms),
        "tick_size": max(1, market.legacy_tick_size_units // 100),
        "price_ranges": [
            {
                "start": f"{item.start_units / 10_000:.4f}",
                "end": f"{item.end_units / 10_000:.4f}",
                "step": f"{item.step_units / 10_000:.4f}",
            }
            for item in market.price_ranges
        ],
        "yes_bid_dollars": dollars(market.yes_bid_units),
        "yes_ask_dollars": dollars(market.yes_ask_units),
        "no_bid_dollars": dollars(market.no_bid_units),
        "no_ask_dollars": dollars(market.no_ask_units),
        "last_price_dollars": dollars(market.last_price_units),
        "yes_bid_size_fp": count(market.yes_bid_size_units),
        "yes_ask_size_fp": count(market.yes_ask_size_units),
        "no_bid_size_fp": count(market.no_bid_size_units),
        "no_ask_size_fp": count(market.no_ask_size_units),
        "volume_24h_fp": count(market.volume_24h_units),
        "open_interest_fp": count(market.open_interest_units),
    }


class _BaseClientMarketSource:
    """Compatibility facade for the existing pure screening algorithm."""

    def __init__(self, client: BaseClient) -> None:
        self.client = client

    def list_markets(self, *, status: str, limit: int, max_total: int, mve_filter: Optional[str]):
        query = MarketQuery(
            status=status,
            page_size=limit,
            max_results=bounded_market_scan_limit(max_total),
            venue_filters={"mve_filter": mve_filter} if mve_filter else {},
        )
        # Yield payloads so the normalized Market list and a second full list
        # of dictionaries are not resident at the same time.
        return (market_to_screen_payload(market) for market in self.client.list_markets(query))


class Screener:
    def __init__(
        self,
        *,
        client: BaseClient,
        settings: Mapping[str, object],
        output_path: Path,
        default_yes_budget_cents: int,
        default_no_budget_cents: int,
        max_bots: int,
        minimum_carryover_value_cents: float,
        yes_budget_column: str = "",
        no_budget_column: str = "",
        disabled_market_ids: Optional[Callable[[], Set[str]]] = None,
        market_class_resolver: Optional[
            Callable[[Mapping[str, object]], Tuple[str, Tuple[Tuple[str, Any], ...]]]
        ] = None,
        cash_reserve_fraction: float = 0.0,
        allocation_oversubscription: float = 1.0,
    ) -> None:
        self.client = client
        self.settings = dict(settings)
        self.output_path = output_path
        self.default_yes_budget_cents = int(default_yes_budget_cents)
        self.default_no_budget_cents = int(default_no_budget_cents)
        self.max_bots = int(max_bots)
        # Mirror of fleetRuntime.cashReserveFraction / allocationOversubscription.
        # Only when the multiplier is above 1.0 are picks per exchange shard
        # capped at the first sides the allocator could fund there
        # (allocatable shard cash x oversubscription / first-side budget); at
        # the default 1.0 the screen is exactly the historical one.
        self.cash_reserve_fraction = min(max(float(cash_reserve_fraction), 0.0), 0.999)
        self.allocation_oversubscription = max(float(allocation_oversubscription), 1.0)
        self.minimum_carryover_value_cents = float(minimum_carryover_value_cents)
        self.yes_budget_column = str(yes_budget_column or "")
        self.no_budget_column = str(no_budget_column or "")
        self.disabled_market_ids = disabled_market_ids or (lambda: set())
        # Maps one screener export row to (market_class, settings_overrides);
        # see market_classes.build_market_class_resolver. None = every pick
        # is "default" with no overrides.
        self.market_class_resolver = market_class_resolver
        # Funded Kalshi exchange shards, refreshed at most every
        # FUNDED_SHARDS_TTL_SECONDS. None = unknown (venue gave no breakdown or
        # the client cannot report balances), in which case nothing is excluded.
        self._funded_shards: Optional[frozenset[int]] = None
        self._funded_shards_at: float = 0.0
        # Cash per shard from the same lookup (units), None when unknown.
        self._shard_cash: Optional[Dict[int, int]] = None
        # Per-shard pick caps of the latest screen: {shard: {cap, dropped, ...}}.
        self._last_shard_caps: Dict[str, Dict[str, object]] = {}
        # Restart carryover of exchange positions (see carry_exchange_positions).
        self._last_exchange_carryover: Dict[str, object] = {}
        self.events: "asyncio.Queue[ScreenerEvent]" = asyncio.Queue()
        self._latest_update: Optional[ScreenerUpdate] = None
        self._generation = 0
        self._running = False
        self._current_reason: Optional[str] = None
        self._current_started_at_ms: Optional[int] = None
        self._last_started_at_ms: Optional[int] = None
        self._last_completed_at_ms: Optional[int] = None
        self._last_duration_ms: Optional[int] = None
        self._last_success_at_ms: Optional[int] = None
        self._last_error: Optional[str] = None
        self._last_warnings: tuple[str, ...] = ()
        self._last_scan_metadata: Dict[str, object] = {}
        self._last_run_metrics: Dict[str, object] = {}

    def get_latest_picks(self) -> tuple[ScreenerPick, ...]:
        return self._latest_update.picks if self._latest_update is not None else ()

    def accept_snapshot(self, update: ScreenerUpdate, warnings: Sequence[str] = ()) -> None:
        """Seed monitoring from an already validated launcher/CSV generation."""
        self._latest_update = update
        self._generation = max(self._generation, update.generation_id)
        if warnings:
            self._last_warnings = tuple(str(item) for item in warnings)

    def status_snapshot(self) -> Dict[str, object]:
        now = int(time.time() * 1000)
        update = self._latest_update
        try:
            api_activity = self.client.activity_snapshot()
        except Exception:
            api_activity = {"rest": {}, "stream": {}}
        picks = [
            {
                "marketId": pick.market_id,
                "title": pick.title,
                "yesBudgetCents": pick.yes_budget_cents,
                "noBudgetCents": pick.no_budget_cents,
                "selectionReason": pick.selection_reason,
                "marketClass": pick.market_class,
                "rank": _monitor_rank(pick.ranking.get("Rank") if hasattr(pick.ranking, "get") else None),
            }
            for pick in self.get_latest_picks()
        ]
        return {
            "running": self._running,
            "currentReason": self._current_reason,
            "currentStartedAtMs": self._current_started_at_ms,
            "currentDurationMs": (
                max(0, now - self._current_started_at_ms)
                if self._running and self._current_started_at_ms is not None
                else None
            ),
            "lastStartedAtMs": self._last_started_at_ms,
            "lastCompletedAtMs": self._last_completed_at_ms,
            "lastDurationMs": self._last_duration_ms,
            "lastSuccessAtMs": self._last_success_at_ms,
            "lastError": self._last_error,
            "warnings": list(self._last_warnings),
            "lastRun": dict(self._last_run_metrics),
            "scanMetadata": dict(self._last_scan_metadata),
            "shardCaps": {key: dict(value) for key, value in self._last_shard_caps.items()},
            "exchangeCarryover": dict(self._last_exchange_carryover),
            "generationId": update.generation_id if update else None,
            "generatedAtMs": update.generated_at_ms if update else None,
            "reason": update.reason if update else None,
            "picks": picks,
            "changes": {
                "added": list(update.added) if update else [],
                "kept": list(update.kept) if update else [],
                "changed": list(update.changed) if update else [],
                "removed": list(update.removed) if update else [],
                "inventoryCarried": list(update.inventory_carried) if update else [],
                "inventoryUnknown": list(update.inventory_unknown) if update else [],
            },
            "apiActivity": api_activity,
        }

    def last_run_metrics(self) -> Dict[str, object]:
        """Return the completed refresh metrics for persistence by the launcher."""
        return dict(self._last_run_metrics)

    def _atomic_export(self, frame: pd.DataFrame) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=self.output_path.parent, suffix=".csv", encoding="utf-8", newline=""
        ) as handle:
            temporary_path = Path(handle.name)
            frame.to_csv(handle, index=False)
        os.replace(temporary_path, self.output_path)

    def _refresh_funded_shards(self) -> Optional[frozenset[int]]:
        """Exchange shards where this account holds cash, or None if unknown.

        Unknown (no breakdown from the venue, or a client that cannot report
        balances such as test doubles) disables the exclusion rather than
        guessing — a wrong guess would silently blacklist tradable markets.
        """
        now = time.monotonic()
        if self._funded_shards_at and now - self._funded_shards_at < FUNDED_SHARDS_TTL_SECONDS:
            return self._funded_shards
        getter = getattr(self.client, "get_account_balance", None)
        if getter is None:
            return self._funded_shards
        try:
            balance = getter()
        except Exception:
            return self._funded_shards  # keep the last known answer
        breakdown = tuple(getattr(balance, "balance_by_exchange", ()) or ())
        self._funded_shards = (
            frozenset(int(index) for index, units in breakdown if int(units) > 0) if breakdown else None
        )
        self._shard_cash = {int(index): max(0, int(units)) for index, units in breakdown} if breakdown else None
        self._funded_shards_at = now
        return self._funded_shards

    def first_side_budget_cents(self) -> int:
        """The budget a market needs to quote at all: its larger (first) side."""
        return max(0, self.default_yes_budget_cents, self.default_no_budget_cents)

    def _shard_pick_caps(self) -> Optional[Dict[int, int]]:
        """Markets each exchange shard can fund one side of, or None when not capping.

        Only an oversubscribed fleet (``allocationOversubscription > 1.0``) is
        capped - at 1.0 the screen must be exactly the historical one.  The
        cap mirrors the allocator's first-side pass (a market needs one side
        to quote; second sides are opportunistic):

            cap = floor(shard cash x (1 - reserve) x oversubscription / first-side budget)
        """
        if self._shard_cash is None or self.allocation_oversubscription <= 1.0:
            return None
        first_side_units = self.first_side_budget_cents() * 100
        if first_side_units <= 0:
            return None
        caps: Dict[int, int] = {}
        for shard, cash in self._shard_cash.items():
            allocatable = math.floor(cash * (1.0 - self.cash_reserve_fraction))
            caps[shard] = int(math.floor(allocatable * self.allocation_oversubscription / first_side_units))
        return caps

    def _resolve_market_class(
        self, row: Mapping[str, object], failures: list[str]
    ) -> tuple[str, tuple[tuple[str, Any], ...]]:
        """Stamp one row; a failing resolver degrades that market to default."""
        if self.market_class_resolver is None:
            return "default", ()
        try:
            market_class, overrides = self.market_class_resolver(row)
            return str(market_class or "default"), tuple((str(name), value) for name, value in (overrides or ()))
        except Exception as exc:
            failures.append(f"{row.get('Ticker')}: {exc}")
            return "default", ()

    def _screen_picks(
        self, current_picks: Mapping[str, ScreenerPick] = MappingProxyType({})
    ) -> tuple[list[ScreenerPick], pd.DataFrame, list[str]]:
        """Select the fleet, keeping already-running markets that still screen well.

        Ranks move constantly (short-dated markets expire, EV re-ranks every
        cycle), so taking a strict top-``max_bots`` slice each refresh evicts
        healthy markets that slipped a single position. Every eviction restarts
        a market from scratch — new actor, cold models, lost queue position, and
        a spell back in ``startup`` where it cannot quote. Incumbents therefore
        keep their slot while they remain within ``FLEET_RETENTION_MULTIPLIER``
        x ``max_bots``; only markets that leave the screen, fall out of that
        band, or are disabled give up their place.
        """
        screen_frame = screen_markets(_BaseClientMarketSource(self.client), self.settings)
        self._last_scan_metadata = dict(screen_frame.attrs.get("market_scan") or {})
        warnings = [str(item) for item in screen_frame.attrs.get("warnings", ())]
        export_frame = build_export_dataframe(screen_frame, self.settings)
        disabled = set(self.disabled_market_ids())
        funded_shards = self._refresh_funded_shards()
        # Per-shard pick caps (oversubscribed fleets only): a shard can only
        # fund so many first sides at the session budgets, so the rest of its
        # screened markets must not take fleet slots they could never quote
        # from. Incumbents keep priority inside the cap by displacing the
        # lowest-ranked newcomer.
        shard_caps = self._shard_pick_caps() if funded_shards is not None else None
        shard_counts: Dict[int, int] = {}
        shard_newcomers: Dict[int, list[str]] = {}
        capped_counts: Dict[int, int] = {}
        unfunded_counts: Dict[int, int] = {}
        class_failures: list[str] = []
        picks: list[ScreenerPick] = []
        retention_band = (
            max(self.max_bots, int(self.max_bots * FLEET_RETENTION_MULTIPLIER))
            if self.max_bots > 0
            else 0
        )
        for row in export_frame.to_dict(orient="records"):
            market_id = str(row.get("Ticker") or "").strip()
            if not market_id or market_id in disabled:
                continue
            if funded_shards is not None:
                try:
                    shard = int(row.get("Exchange Index") or 0)
                except (TypeError, ValueError):
                    shard = 0
                if shard not in funded_shards:
                    unfunded_counts[shard] = unfunded_counts.get(shard, 0) + 1
                    continue
                if shard_caps is not None and shard_counts.get(shard, 0) >= shard_caps.get(shard, 0):
                    newcomers = shard_newcomers.get(shard) or []
                    if market_id in current_picks and newcomers:
                        victim = newcomers.pop()
                        picks = [pick for pick in picks if pick.market_id != victim]
                        shard_counts[shard] -= 1
                        capped_counts[shard] = capped_counts.get(shard, 0) + 1
                    else:
                        capped_counts[shard] = capped_counts.get(shard, 0) + 1
                        continue
                shard_counts[shard] = shard_counts.get(shard, 0) + 1
                if market_id not in current_picks:
                    shard_newcomers.setdefault(shard, []).append(market_id)
            yes_budget = (
                int(float(row[self.yes_budget_column]))
                if self.yes_budget_column and row.get(self.yes_budget_column) not in (None, "")
                else self.default_yes_budget_cents
            )
            no_budget = (
                int(float(row[self.no_budget_column]))
                if self.no_budget_column and row.get(self.no_budget_column) not in (None, "")
                else self.default_no_budget_cents
            )
            if yes_budget < 0 or no_budget < 0:
                raise ValueError(f"Ticker {market_id} has a negative budget")
            market_class, settings_overrides = self._resolve_market_class(row, class_failures)
            picks.append(
                ScreenerPick(
                    market_id=market_id,
                    title=str(row.get("SearchText") or ""),
                    yes_budget_cents=yes_budget,
                    no_budget_cents=no_budget,
                    ranking=row,
                    selection_reason="screen",
                    market_class=market_class,
                    settings_overrides=settings_overrides,
                )
            )
            if retention_band > 0 and len(picks) >= retention_band:
                break
        if class_failures:
            sample = "; ".join(class_failures[:3])
            warnings.append(
                f"Market class resolver failed for {len(class_failures)} market(s); they run with the "
                f"default class and no overrides ({sample})."
            )
        if unfunded_counts:
            detail = ", ".join(f"shard {shard}: {count}" for shard, count in sorted(unfunded_counts.items()))
            warnings.append(
                f"Excluded {sum(unfunded_counts.values())} screened market(s) on unfunded Kalshi exchange "
                f"shard(s) ({detail}). Orders there are rejected with user_not_found until cash is moved "
                "to that shard (Kalshi Intra Account Transfer)."
            )
        shard_cap_status: Dict[str, Dict[str, object]] = {}
        if shard_caps is not None:
            first_side_cents = self.first_side_budget_cents()
            for shard, cap in sorted(shard_caps.items()):
                cash_units = (self._shard_cash or {}).get(shard, 0)
                if cash_units <= 0 and shard not in shard_counts and shard not in capped_counts:
                    continue
                allocatable_units = math.floor(cash_units * (1.0 - self.cash_reserve_fraction))
                dropped = capped_counts.get(shard, 0)
                shard_cap_status[str(shard)] = {
                    "cap": int(cap),
                    "selected": int(shard_counts.get(shard, 0)),
                    "dropped": int(dropped),
                    "cashUnits": int(cash_units),
                    "allocatableUnits": int(allocatable_units),
                    "oversubscription": float(self.allocation_oversubscription),
                    "firstSideBudgetCents": int(first_side_cents),
                }
                if dropped:
                    message = (
                        f"Capped Kalshi exchange shard {shard} at {cap} market(s) (allocatable "
                        f"${allocatable_units / 10_000:,.2f} x {self.allocation_oversubscription:g} oversubscription "
                        f"/ ${first_side_cents / 100:,.2f} first-side budget); dropped {dropped} screened market(s) "
                        "the shard could not fund a first side for."
                    )
                    warnings.append(message)
                    LOGGER.warning(
                        "SCREENER_SHARD_CAP | shard=%s cap=%s selected=%s dropped=%s allocatable_units=%s "
                        "oversubscription=%s first_side_cents=%s",
                        shard, cap, shard_counts.get(shard, 0), dropped, allocatable_units,
                        self.allocation_oversubscription, first_side_cents,
                    )
        self._last_shard_caps = shard_cap_status
        if self.max_bots <= 0 or len(picks) <= self.max_bots:
            return picks, export_frame, warnings
        rank_order = {pick.market_id: index for index, pick in enumerate(picks)}
        incumbents = [pick for pick in picks if pick.market_id in current_picks]
        newcomers = [pick for pick in picks if pick.market_id not in current_picks]
        selected = incumbents[: self.max_bots]
        selected.extend(newcomers[: max(0, self.max_bots - len(selected))])
        selected.sort(key=lambda pick: rank_order[pick.market_id])
        return selected, export_frame, warnings

    def _with_inventory_carryover(
        self,
        picks: Sequence[ScreenerPick],
        current_picks: Mapping[str, ScreenerPick],
    ) -> tuple[list[ScreenerPick], list[str], list[str], list[str]]:
        screened_ids = {pick.market_id for pick in picks}
        disabled = set(self.disabled_market_ids())
        # Unknown inventory wins first (fail safe), then known inventory by
        # marked value if carryovers alone exceed the hard fleet cap.
        carry_candidates: list[tuple[tuple[int, float, int], ScreenerPick, bool]] = []
        for order, (market_id, previous) in enumerate(current_picks.items()):
            if market_id in screened_ids or market_id in disabled:
                continue
            marked_value_cents = 0.0
            is_unknown = False
            try:
                positions = self.client.get_positions(market_id)
                position_units = positions[0].position_units if positions else 0
                if position_units == 0:
                    continue
                quote = self.client.get_market_quote(market_id)
                bid_units = quote.yes_bid_units if position_units > 0 else quote.no_bid_units
                if bid_units is None:
                    raise RuntimeError("inventory side has no bid")
                marked_value_cents = abs(position_units) * bid_units / 10_000.0
                if marked_value_cents < self.minimum_carryover_value_cents:
                    continue
                # A restart carryover stays reduce-only for its lifetime: the
                # screener never picked it, so it must not start adding.
                reason = (
                    EXCHANGE_POSITION_REASON
                    if previous.selection_reason == EXCHANGE_POSITION_REASON
                    else "inventory"
                )
            except Exception:
                is_unknown = True
                reason = "inventory_unknown"
            carry_pick = ScreenerPick(
                market_id=previous.market_id,
                title=previous.title or "Inventory carryover",
                yes_budget_cents=previous.yes_budget_cents,
                no_budget_cents=previous.no_budget_cents,
                ranking=previous.ranking,
                selection_reason=reason,
                # Keep the running actor's class/overrides: a carryover exists
                # to avoid a restart, so its runtime key must not change.
                market_class=previous.market_class,
                settings_overrides=previous.settings_overrides,
            )
            priority = (0, 0.0, order) if is_unknown else (1, -marked_value_cents, order)
            carry_candidates.append((priority, carry_pick, is_unknown))

        carry_candidates.sort(key=lambda item: item[0])
        warnings: list[str] = []
        if self.max_bots > 0:
            selected_carryovers = carry_candidates[: self.max_bots]
            omitted_carryovers = carry_candidates[self.max_bots :]
            normal_slots = max(0, self.max_bots - len(selected_carryovers))
            selected_screened = list(picks[:normal_slots])
        else:
            selected_carryovers = carry_candidates
            omitted_carryovers = []
            selected_screened = list(picks)

        displaced_count = len(picks) - len(selected_screened)
        if displaced_count:
            warnings.append(
                f"Retained {len(selected_carryovers)} inventory carryover(s), displacing "
                f"{displaced_count} lower-ranked screened market(s) to honor max_bots={self.max_bots}."
            )
        if omitted_carryovers:
            omitted_ids = [item[1].market_id for item in omitted_carryovers]
            sample = ", ".join(omitted_ids[:10])
            suffix = "" if len(omitted_ids) <= 10 else f", and {len(omitted_ids) - 10} more"
            warnings.append(
                f"Inventory carryovers exceeded max_bots={self.max_bots}; omitted "
                f"{len(omitted_ids)} lower-priority market(s): {sample}{suffix}."
            )

        carry_picks = [item[1] for item in selected_carryovers]
        carried = [pick.market_id for pick in carry_picks]
        unknown = [item[1].market_id for item in selected_carryovers if item[2]]
        return selected_screened + carry_picks, carried, unknown, warnings

    def carry_exchange_positions(
        self,
        picks: Sequence[ScreenerPick],
        positions: Sequence[Any],
        *,
        disabled: Optional[Set[str]] = None,
        traded_tickers: Optional[Set[str]] = None,
    ) -> tuple[list[ScreenerPick], list[str], list[str]]:
        """Restart carryover: give every open exchange position the fleet could
        have traded a bot.

        ``_with_inventory_carryover`` only knows the running fleet
        (``current_picks``), so at a fleet Start every position left by an
        earlier run whose market the screener does not re-pick would be
        orphaned.  Here each nonzero position from the account's position
        list gets a pick even though it never passed the screener filters,
        marked ``selection_reason="exchange_position"``: the manager grants
        it only the side that reduces the position (reduce-only) unless the
        screener also picked it, in which case it is a normal pick.

        Adoption rule: a position is adopted only if its market is one the
        fleet could have traded - it passes the session screener's status and
        time-to-close window (``screener_horizon_verdict``) OR the fleet has
        telemetry fills for it in the run artifacts (``traded_tickers``).
        Anything else (an operator's own long-dated position, say) is left
        untouched and reported.

        Skipped: positions whose market is already picked, closed/settled
        (``CLOSED_MARKET_STATUSES``), disabled, or on an exchange shard
        holding no cash (the venue rejects every order there).  A market the
        venue cannot describe is carried anyway (fail safe, like unknown
        inventory) with a warning.  ``max_bots`` is honoured: carryovers
        displace the lowest-ranked seeded picks (screened, CSV or fixed
        ticker alike), and if carryovers alone exceed the cap the
        lowest-value ones are omitted with a warning.

        Returns ``(picks, carried_ids, warnings)``.
        """
        existing = {pick.market_id for pick in picks}
        disabled_ids = set(disabled if disabled is not None else self.disabled_market_ids())
        traded = {str(item) for item in (traded_tickers or ())}
        funded_shards = self._refresh_funded_shards()
        warnings: list[str] = []
        skipped_closed: list[str] = []
        skipped_unfunded: list[tuple[int, str]] = []
        skipped_disabled: list[str] = []
        left_alone: list[tuple[str, str]] = []
        unknown_market: list[str] = []
        candidates: list[tuple[float, ScreenerPick]] = []
        seen: Set[str] = set()
        for position in positions or ():
            market_id = str(getattr(position, "market_id", "") or "").strip()
            units = int(getattr(position, "position_units", 0) or 0)
            if not market_id or units == 0 or market_id in seen:
                continue
            seen.add(market_id)
            if market_id in existing:
                continue
            if market_id in disabled_ids:
                skipped_disabled.append(market_id)
                continue
            title = market_id
            series_id = market_id.split("-")[0]
            exchange_index = 0
            status = ""
            market = None
            try:
                market = self.client.get_market(market_id)
                title = str(getattr(market, "title", "") or market_id)
                series_id = str(getattr(market, "series_id", "") or series_id)
                exchange_index = int(getattr(market, "exchange_index", 0) or 0)
                status = str(getattr(market, "status", "") or "").strip().lower()
            except Exception as exc:
                unknown_market.append(f"{market_id}: {exc}")
            if status in CLOSED_MARKET_STATUSES:
                skipped_closed.append(market_id)
                continue
            if funded_shards is not None and exchange_index not in funded_shards:
                skipped_unfunded.append((exchange_index, market_id))
                continue
            if market is not None and market_id not in traded:
                verdict = screener_horizon_verdict(market, self.settings)
                if verdict:
                    left_alone.append((market_id, verdict))
                    continue
            exposure = getattr(position, "market_exposure_units", None)
            value_units = abs(int(exposure)) if exposure is not None else abs(units)
            candidates.append((float(value_units), ScreenerPick(
                market_id=market_id,
                title=title,
                yes_budget_cents=self.default_yes_budget_cents,
                no_budget_cents=self.default_no_budget_cents,
                ranking={
                    "Ticker": market_id, "SearchText": title, "Exchange Index": exchange_index,
                    "Series Ticker": series_id, "Position": units / 100.0,
                },
                selection_reason=EXCHANGE_POSITION_REASON,
            )))
        candidates.sort(key=lambda item: (-item[0], item[1].market_id))
        selected = list(picks)
        omitted: list[str] = []
        displaced = 0
        if self.max_bots > 0:
            room = self.max_bots - len(selected)
            carry = candidates
            if len(carry) > room:
                # Displace the lowest-ranked seeded picks first (screened, CSV
                # or fixed ticker: every non-carryover pick), then omit the
                # lowest-value carryovers if positions alone exceed the cap.
                screened_indexes = [
                    index for index, pick in enumerate(selected)
                    if pick.selection_reason != EXCHANGE_POSITION_REASON
                ]
                while len(carry) > room and screened_indexes:
                    selected.pop(screened_indexes.pop())
                    displaced += 1
                    room += 1
                if len(carry) > room:
                    omitted = [pick.market_id for _, pick in carry[max(0, room):]]
                    carry = carry[:max(0, room)]
            carried_picks = [pick for _, pick in carry]
        else:
            carried_picks = [pick for _, pick in candidates]
        selected.extend(carried_picks)
        carried = [pick.market_id for pick in carried_picks]
        if carried:
            sample = ", ".join(carried[:10]) + ("" if len(carried) <= 10 else f", and {len(carried) - 10} more")
            warnings.append(
                f"Carried {len(carried)} exchange position(s) into the fleet at start "
                f"(reduce-only, not screened): {sample}."
            )
        if displaced:
            warnings.append(
                f"Exchange position carryover displaced {displaced} lower-ranked seeded market(s) "
                f"to honor max_bots={self.max_bots}."
            )
        if left_alone:
            tickers = ", ".join(market for market, _ in left_alone[:10]) + (
                "" if len(left_alone) <= 10 else f", and {len(left_alone) - 10} more"
            )
            reasons = "; ".join(f"{market}: {why}" for market, why in left_alone[:3])
            warnings.append(
                f"left {len(left_alone)} exchange position(s) alone (outside the screener's horizon): "
                f"{tickers}. {reasons}"
            )
        if omitted:
            warnings.append(
                f"Exchange position carryover exceeded max_bots={self.max_bots}; omitted "
                f"{len(omitted)} lower-value position(s): {', '.join(omitted[:10])}."
            )
        if skipped_closed:
            warnings.append(
                f"Skipped {len(skipped_closed)} exchange position(s) in closed/settled market(s): "
                f"{', '.join(skipped_closed[:10])}."
            )
        if skipped_unfunded:
            detail = ", ".join(f"shard {shard}: {market}" for shard, market in skipped_unfunded[:10])
            warnings.append(
                f"Skipped {len(skipped_unfunded)} exchange position(s) on unfunded Kalshi exchange shard(s) "
                f"({detail}); orders there are rejected with user_not_found until cash is moved to that shard."
            )
        if skipped_disabled:
            warnings.append(
                f"Skipped {len(skipped_disabled)} exchange position(s) in disabled market(s): "
                f"{', '.join(skipped_disabled[:10])}."
            )
        if unknown_market:
            warnings.append(
                f"Carried {len(unknown_market)} exchange position(s) whose market the venue could not describe "
                f"(shard assumed 0): {'; '.join(unknown_market[:3])}."
            )
        self._last_exchange_carryover = {
            "carried": list(carried),
            "displaced": int(displaced),
            "omitted": list(omitted),
            "skippedClosed": list(skipped_closed),
            "skippedUnfunded": [f"{shard}:{market}" for shard, market in skipped_unfunded],
            "skippedDisabled": list(skipped_disabled),
            "leftAlone": [market for market, _ in left_alone],
            "unknownMarket": [item.split(":", 1)[0] for item in unknown_market],
        }
        for message in warnings:
            LOGGER.warning("SCREENER_EXCHANGE_CARRYOVER | %s", message)
        return selected, carried, warnings

    def _refresh_sync(
        self,
        current_picks: Mapping[str, ScreenerPick],
        reason: str,
        exchange_positions: Optional[Sequence[Any]] = None,
        traded_tickers: Optional[Set[str]] = None,
    ) -> ScreenerUpdate:
        screened, export_frame, warnings = self._screen_picks(current_picks)
        picks, carried, unknown, carryover_warnings = self._with_inventory_carryover(screened, current_picks)
        warnings.extend(carryover_warnings)
        if exchange_positions is not None:
            picks, exchange_carried, exchange_warnings = self.carry_exchange_positions(
                picks, exchange_positions, traded_tickers=traded_tickers,
            )
            carried = sorted(set(carried) | set(exchange_carried))
            warnings.extend(exchange_warnings)
        desired = {pick.market_id: pick for pick in picks}
        current_ids = set(current_picks)
        desired_ids = set(desired)
        common = current_ids & desired_ids
        changed = sorted(
            market_id
            for market_id in common
            if current_picks[market_id].runtime_key() != desired[market_id].runtime_key()
        )
        kept = sorted(common - set(changed))
        self._generation += 1
        update = ScreenerUpdate(
            generation_id=self._generation,
            generated_at_ms=int(time.time() * 1000),
            reason=reason,
            picks=tuple(picks),
            added=tuple(sorted(desired_ids - current_ids)),
            kept=tuple(kept),
            changed=tuple(changed),
            removed=tuple(sorted(current_ids - desired_ids)),
            inventory_carried=tuple(sorted(carried)),
            inventory_unknown=tuple(sorted(unknown)),
        )
        self._atomic_export(export_frame)
        self._latest_update = update
        self._last_warnings = tuple(warnings)
        return update

    async def refresh(
        self,
        current_picks: Mapping[str, ScreenerPick],
        *,
        reason: str = "scheduled",
        exchange_positions: Optional[Sequence[Any]] = None,
        traded_tickers: Optional[Set[str]] = None,
    ) -> Optional[ScreenerUpdate]:
        """Screen and reconcile; ``exchange_positions`` (the account's open
        positions, launcher startup only) become reduce-only carryover picks
        when their market is inside the screener's horizon or in
        ``traded_tickers`` (markets the fleet's telemetry has fills for)."""
        started_at_ms = int(time.time() * 1000)
        self._running = True
        self._current_reason = reason
        self._current_started_at_ms = started_at_ms
        self._last_started_at_ms = started_at_ms
        self._last_run_metrics = {
            "status": "running",
            "reason": reason,
            "startedAtMs": started_at_ms,
            "endedAtMs": None,
            "durationMs": None,
            "generationId": None,
            "configuredLimit": self.settings.get("max_markets_to_scan"),
            "scannedMarkets": None,
            "apiRequests": None,
            "apiErrors": None,
            "added": 0,
            "changed": 0,
            "removed": 0,
            "inventoryCarried": 0,
            "inventoryUnknown": 0,
            "warnings": [],
            "error": None,
        }
        self._last_scan_metadata = {}
        self._last_warnings = ()
        activity_before: Dict[str, object] = {}
        try:
            activity_before = dict((self.client.activity_snapshot().get("rest") or {}))
        except Exception:
            pass
        try:
            update = await asyncio.to_thread(
                self._refresh_sync, dict(current_picks), reason,
                list(exchange_positions) if exchange_positions is not None else None,
                set(traded_tickers) if traded_tickers is not None else None,
            )
            completed_at_ms = int(time.time() * 1000)
            self._last_success_at_ms = completed_at_ms
            self._last_error = None
            status = "succeeded"
            await self.events.put(ScreenerEvent(True, reason, update.generated_at_ms, update=update))
            return update
        except Exception as exc:
            completed_at_ms = int(time.time() * 1000)
            self._last_error = str(exc)
            status = "failed"
            await self.events.put(
                ScreenerEvent(False, reason, completed_at_ms, error=str(exc))
            )
            return None
        finally:
            completed_at_ms = int(time.time() * 1000)
            self._last_completed_at_ms = completed_at_ms
            self._last_duration_ms = max(0, completed_at_ms - started_at_ms)
            activity_after: Dict[str, object] = {}
            try:
                activity_after = dict((self.client.activity_snapshot().get("rest") or {}))
            except Exception:
                pass
            before_requests = activity_before.get("total")
            after_requests = activity_after.get("total")
            before_errors = activity_before.get("errors")
            after_errors = activity_after.get("errors")
            request_delta = (
                max(0, int(after_requests) - int(before_requests))
                if isinstance(before_requests, (int, float)) and isinstance(after_requests, (int, float))
                else None
            )
            error_delta = (
                max(0, int(after_errors) - int(before_errors))
                if isinstance(before_errors, (int, float)) and isinstance(after_errors, (int, float))
                else None
            )
            update_value = locals().get("update")
            run_status = locals().get("status", "failed")
            self._last_run_metrics = {
                "status": run_status,
                "reason": reason,
                "startedAtMs": started_at_ms,
                "endedAtMs": completed_at_ms,
                "durationMs": self._last_duration_ms,
                "generationId": update_value.generation_id if update_value is not None else None,
                "configuredLimit": self.settings.get("max_markets_to_scan"),
                "scannedMarkets": self._last_scan_metadata.get("scannedMarkets"),
                "effectiveLimit": self._last_scan_metadata.get("effectiveLimit"),
                "apiRequests": request_delta,
                "apiErrors": error_delta,
                "added": len(update_value.added) if update_value is not None else 0,
                "changed": len(update_value.changed) if update_value is not None else 0,
                "removed": len(update_value.removed) if update_value is not None else 0,
                "inventoryCarried": len(update_value.inventory_carried) if update_value is not None else 0,
                "inventoryUnknown": len(update_value.inventory_unknown) if update_value is not None else 0,
                "warnings": list(self._last_warnings),
                "error": self._last_error,
            }
            self._running = False
            self._current_reason = None
            self._current_started_at_ms = None
