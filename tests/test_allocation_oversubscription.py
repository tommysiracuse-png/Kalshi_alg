"""Capital allocation oversubscription: allocator (legacy-identical at 1.0,
sticky/hysteretic live cap above it), screener first-side caps, session
schema, venue error mapping, bot cooldown, the manager's broker limits
message and the Overview warning."""

from __future__ import annotations

import asyncio
import json
import math
import queue
import random
from pathlib import Path

import pytest

from clients.http_client import HTTPClientError
from clients.models import (
    AccountBalance, AccountLimits, AccountPosition, InsufficientBalanceError, MarketQuote, Position, RateLimitBucket,
)
from fleet_runtime.capacity import (
    LIVE_CAP_RELEASE_FRACTION, AllocationRequest, CapitalAllocator, live_cap_withholding,
)
from session_config import default_session_configuration, validate_session_configuration


# ---------------------------------------------------------------------------
# Live case (run 6d30083e): shard 0 $364.36 for 27 markets, shard 3 $159.64 for
# 73 markets, budgets 825 / 975 cents per side, reserve 0.1.  Cash units are
# 1/10000 dollar; side budgets are cents x 100.
# ---------------------------------------------------------------------------
SHARD0_CASH = 3_643_600
SHARD3_CASH = 1_596_400
OTHER_CASH = {1: 1_000_000, 2: 999_947}
TOTAL_CASH = SHARD0_CASH + SHARD3_CASH + sum(OTHER_CASH.values())  # 7_239_947 as reported live
YES_BUDGET = 825 * 100
NO_BUDGET = 975 * 100
LIVE_COMMITTED = 65_800
RESERVE = 0.1
SIDE_CAPACITY = 540
SHARD3_ALLOCATABLE = math.floor(SHARD3_CASH * 0.9)   # 1_436_760


def live_requests() -> list[AllocationRequest]:
    """27 shard-0 and 73 shard-3 markets interleaved in screener rank order."""
    requests = []
    shard3_left, shard0_left = 73, 27
    rank = 0
    while shard3_left or shard0_left:
        for _ in range(3):
            if shard3_left:
                requests.append(AllocationRequest(
                    f"KXMLB-{rank:03d}", f"KXMLB{rank % 9}", rank, "no", NO_BUDGET, YES_BUDGET, exchange_index=3,
                ))
                rank += 1
                shard3_left -= 1
        if shard0_left:
            requests.append(AllocationRequest(
                f"KXINX-{rank:03d}", f"KXINX{rank % 5}", rank, "no", NO_BUDGET, YES_BUDGET, exchange_index=0,
            ))
            rank += 1
            shard0_left -= 1
    assert len(requests) == 100
    return requests


def live_cash() -> dict[int, int]:
    return {0: SHARD0_CASH, 3: SHARD3_CASH, **OTHER_CASH}


def legacy_allocate(requests, *, cash_reserve_fraction, series_exposure_fraction, available_cash_units,
                    existing_committed_units=0, quote_side_capacity=None, cash_by_exchange_units=None):
    """Verbatim port of the pre-oversubscription allocator (the oracle for 1.0)."""
    ordered = sorted(requests, key=lambda item: (item.rank, item.ticker))
    available = max(0, int(available_cash_units))
    allocatable = math.floor(available * (1.0 - cash_reserve_fraction))
    committed = max(0, int(existing_committed_units))
    series_used: dict[str, int] = {}
    series_limit = math.floor(allocatable * series_exposure_fraction)
    side_capacity = quote_side_capacity if quote_side_capacity is not None else len(ordered) * 2
    shard_pool = None
    if cash_by_exchange_units is not None:
        shard_pool = {
            int(index): math.floor(max(0, int(cash)) * (1.0 - cash_reserve_fraction))
            for index, cash in cash_by_exchange_units.items()
        }
    shard_used: dict[int, int] = {}

    def shard_can_fund(item, notional):
        if shard_pool is None:
            return True
        return shard_used.get(int(item.exchange_index), 0) + notional <= shard_pool.get(int(item.exchange_index), 0)

    if side_capacity < len(ordered):
        return False, {}, committed, ""
    result = {item.ticker: [] for item in ordered}
    for item in ordered:
        if item.reducing_sides:
            result[item.ticker].append(item.reducing_sides[0])
            side_capacity -= 1
    skipped = {}
    for item in ordered:
        if result[item.ticker]:
            continue
        notional = max(0, item.first_side_notional_units)
        series_total = series_used.get(item.series_id, 0)
        if not shard_can_fund(item, notional):
            skipped[item.ticker] = f"exchange shard {int(item.exchange_index)} unfunded"
            continue
        if committed + notional > allocatable:
            skipped[item.ticker] = "capital"
            continue
        if series_total + notional > series_limit:
            skipped[item.ticker] = "series limit"
            continue
        result[item.ticker].append(item.preferred_side)
        committed += notional
        series_used[item.series_id] = series_total + notional
        shard_used[int(item.exchange_index)] = shard_used.get(int(item.exchange_index), 0) + notional
        side_capacity -= 1
    for item in ordered:
        if side_capacity <= 0:
            break
        if item.second_side_notional_units <= 0:
            continue
        other = "no" if item.preferred_side == "yes" else "yes"
        if other in result[item.ticker]:
            continue
        notional = item.second_side_notional_units
        series_total = series_used.get(item.series_id, 0)
        if committed + notional > allocatable or series_total + notional > series_limit or not shard_can_fund(item, notional):
            continue
        result[item.ticker].append(other)
        committed += notional
        series_used[item.series_id] = series_total + notional
        shard_used[int(item.exchange_index)] = shard_used.get(int(item.exchange_index), 0) + notional
        side_capacity -= 1
    frozen = {ticker: tuple(sides) for ticker, sides in result.items()}
    admitted = sum(1 for sides in frozen.values() if sides)
    if admitted == 0 and ordered:
        return False, {}, committed, "no market could be funded"
    error = f"skipped {len(skipped)} of {len(ordered)} markets" if skipped else ""
    return True, frozen, committed, error


def summarize(sides, requests):
    by_shard = {}
    for item in requests:
        granted = len(sides.get(item.ticker, ()))
        bucket = by_shard.setdefault(int(item.exchange_index), {"none": 0, "single": 0, "both": 0})
        bucket["none" if granted == 0 else "single" if granted == 1 else "both"] += 1
    return by_shard


def shard(result, index):
    return next(item for item in result.shards if item.exchange_index == index)


# ---------------------------------------------------------------------------
# 1.0: byte-identical to the legacy allocator
# ---------------------------------------------------------------------------

def test_default_oversubscription_reproduces_legacy_grants_on_the_live_numbers():
    requests = live_requests()
    allocator = CapitalAllocator(cash_reserve_fraction=RESERVE, series_exposure_fraction=1.0)
    shard3_ranked = [item.ticker for item in requests if item.exchange_index == 3]
    # Every new-behaviour input is supplied and must be ignored at 1.0: live
    # exposure at the shard cap, previous grants, a withheld shard.
    result = allocator.allocate(
        requests, available_cash_units=TOTAL_CASH, existing_committed_units=LIVE_COMMITTED,
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
        exposure_by_ticker={shard3_ranked[-1]: SHARD3_ALLOCATABLE},
        held_tickers=shard3_ranked[-5:], withheld_shards=[3, 0], fleet_withholding=True,
    )
    gate, legacy_sides, legacy_committed, legacy_error = legacy_allocate(
        requests, cash_reserve_fraction=RESERVE, series_exposure_fraction=1.0, available_cash_units=TOTAL_CASH,
        existing_committed_units=LIVE_COMMITTED, quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
    )
    assert gate and result.gate_open
    assert dict(result.sides_by_ticker) == legacy_sides
    assert result.committed_units == legacy_committed
    assert result.error.startswith(legacy_error)
    # Today's outcome: shard 3 funds 14 single sides and 59 markets get nothing;
    # shard 0 funds every first side plus 7 second sides.
    assert summarize(result.sides_by_ticker, requests) == {
        3: {"none": 59, "single": 14, "both": 0},
        0: {"none": 0, "single": 20, "both": 7},
    }
    assert result.committed_units == LIVE_COMMITTED + 14 * NO_BUDGET + 27 * NO_BUDGET + 7 * YES_BUDGET
    assert result.oversubscription == 1.0 and result.budget_cap_units == result.allocatable_units
    assert not result.withholding
    shard3 = shard(result, 3)
    assert (shard3.markets, shard3.funded_markets, shard3.skipped_markets, shard3.withheld_markets) == (73, 14, 59, 0)
    assert shard3.allocatable_units == shard3.budget_cap_units == SHARD3_ALLOCATABLE
    assert shard3.shortfall_units == 59 * NO_BUDGET - (shard3.budget_cap_units - 14 * NO_BUDGET)
    assert not shard3.reduction_only and not shard(result, 0).reduction_only
    assert "skipped 59 of 100 markets (shard 3: 59 skipped, short $" in result.error
    assert "exchange shard 3 unfunded" in result.error
    assert "live exposure" not in result.error


def test_default_oversubscription_matches_legacy_on_random_fleets():
    rng = random.Random(1234)
    for _ in range(60):
        requests = []
        for rank in range(rng.randint(1, 60)):
            shard_index = rng.choice([0, 0, 2, 3])
            side = rng.choice(["yes", "no"])
            requests.append(AllocationRequest(
                f"M-{rank}", f"S{rng.randint(0, 6)}", rank, side, rng.randint(0, 120_000), rng.randint(0, 120_000),
                reducing_sides=("yes",) if rng.random() < 0.1 else (), exchange_index=shard_index,
            ))
        cash = {0: rng.randint(0, 3_000_000), 2: rng.randint(0, 500_000), 3: rng.randint(0, 1_500_000)}
        kwargs = dict(
            available_cash_units=sum(cash.values()), existing_committed_units=rng.randint(0, 3_000_000),
            quote_side_capacity=rng.choice([None, 400, len(requests) * 2, len(requests) + 3]),
            cash_by_exchange_units=cash if rng.random() < 0.8 else None,
        )
        # Live-cap inputs the legacy allocator never had: exposure at or past
        # the shard cap, held tickers, withheld shards, fleet withholding.
        tickers = [item.ticker for item in requests]
        extra = dict(
            exposure_by_ticker={t: rng.randint(0, 2_000_000) for t in rng.sample(tickers, k=min(len(tickers), rng.randint(0, 8)))},
            held_tickers=rng.sample(tickers, k=min(len(tickers), rng.randint(0, 5))),
            withheld_shards=rng.sample([0, 2, 3], k=rng.randint(0, 3)),
            fleet_withholding=rng.random() < 0.5,
        )
        reserve, series = rng.choice([0.0, 0.1, 0.2]), rng.choice([0.1, 0.5, 1.0])
        result = CapitalAllocator(cash_reserve_fraction=reserve, series_exposure_fraction=series).allocate(requests, **kwargs, **extra)
        gate, sides, committed, error = legacy_allocate(requests, cash_reserve_fraction=reserve, series_exposure_fraction=series, **kwargs)
        assert result.gate_open == gate
        assert dict(result.sides_by_ticker) == sides
        assert result.committed_units == committed
        assert result.error.startswith(error)
        assert not result.withholding and not any(item.reduction_only for item in result.shards)
        assert "live exposure" not in result.error


# ---------------------------------------------------------------------------
# > 1.0: budget cap, sticky/hysteretic live cap
# ---------------------------------------------------------------------------

def test_oversubscription_three_grants_sides_to_most_shard3_markets():
    requests = live_requests()
    allocator = CapitalAllocator(cash_reserve_fraction=RESERVE, series_exposure_fraction=1.0, oversubscription=3.0)
    result = allocator.allocate(
        requests, available_cash_units=TOTAL_CASH, existing_committed_units=LIVE_COMMITTED,
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
    )
    assert result.gate_open
    shard3_cap = math.floor(SHARD3_ALLOCATABLE * 3.0)
    assert shard3_cap == 4_310_280
    assert summarize(result.sides_by_ticker, requests) == {
        3: {"none": 29, "single": 44, "both": 0},   # 44 x $9.75 = $4,290.00 of the $4,310.28 budget cap
        0: {"none": 0, "single": 0, "both": 27},    # 27 x $18.00 = $486.00 of the $983.77 budget cap
    }
    shard3 = shard(result, 3)
    assert shard3.budget_cap_units == shard3_cap and shard3.allocatable_units == SHARD3_ALLOCATABLE
    assert shard3.committed_units == 44 * NO_BUDGET and shard3.funded_markets == 44
    assert result.budget_cap_units == math.floor(result.allocatable_units * 3.0)
    # Rank order is kept inside un-multiplied cash: the 14 highest-ranked
    # shard-3 markets are the first 14 funded, as at 1.0.
    shard3_ranked = [item.ticker for item in requests if item.exchange_index == 3]
    assert all(result.sides_by_ticker[ticker] for ticker in shard3_ranked[:44])
    assert not any(result.sides_by_ticker[ticker] for ticker in shard3_ranked[44:])


def test_live_cap_hysteresis_helper():
    assert LIVE_CAP_RELEASE_FRACTION == 0.8
    assert live_cap_withholding(100, 100, was_withholding=False)
    assert not live_cap_withholding(99, 100, was_withholding=False)
    assert live_cap_withholding(85, 100, was_withholding=True)       # sticky above 80%
    assert live_cap_withholding(80, 100, was_withholding=True)
    assert not live_cap_withholding(79, 100, was_withholding=True)   # released below 80%
    assert not live_cap_withholding(0, 0, was_withholding=True)      # zero cash is unfunded, not withheld


def test_live_cap_is_sticky_for_holders_and_hysteretic_for_the_shard():
    requests = live_requests()
    allocator = CapitalAllocator(cash_reserve_fraction=RESERVE, series_exposure_fraction=1.0, oversubscription=3.0)
    shard3_ranked = [item.ticker for item in requests if item.exchange_index == 3]

    # Inventory-first past un-multiplied cash: the lowest-ranked shard-3 market
    # holds a resting order, so it is served before newcomers in the
    # oversubscribed region while the 14 sides inside cash stay rank-ordered.
    holder = shard3_ranked[-1]
    preferred = allocator.allocate(
        requests, available_cash_units=TOTAL_CASH, existing_committed_units=LIVE_COMMITTED,
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
        exposure_by_ticker={holder: 5_000},
    )
    assert preferred.sides_by_ticker[holder] == ("no",)
    assert all(preferred.sides_by_ticker[t] for t in shard3_ranked[:43])
    assert preferred.sides_by_ticker[shard3_ranked[43]] == ()   # displaced by the inventory holder
    assert sum(1 for t in shard3_ranked if preferred.sides_by_ticker[t]) == 44

    # Shard 3 live exposure at 100% of allocatable, spread over 10 markets that
    # rest quotes (the routine oversubscribed state): those 10 KEEP their
    # first side, the 20 markets granted last pass keep exactly the sides
    # they held (one of them both sides), every other shard-3 market is
    # withheld and nobody gains a side, reducing sides pass, shard 0 is
    # untouched.
    holders = shard3_ranked[20:30]
    previous_grants = {t: ("no",) for t in shard3_ranked[:20]}
    previous_grants[shard3_ranked[0]] = ("no", "yes")
    exposure = {t: SHARD3_ALLOCATABLE // 10 for t in holders}
    exposure[holders[0]] += SHARD3_ALLOCATABLE - sum(exposure.values())
    reducing = AllocationRequest("KXMLB-RED", "KXMLB0", 500, "no", NO_BUDGET, YES_BUDGET, reducing_sides=("yes",), exchange_index=3)
    capped = allocator.allocate(
        requests + [reducing], available_cash_units=TOTAL_CASH, existing_committed_units=SHARD3_ALLOCATABLE,
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
        exposure_by_ticker=exposure, held_tickers=previous_grants,
    )
    assert capped.gate_open
    assert all(capped.sides_by_ticker[t] == ("no",) for t in holders)
    assert {t: capped.sides_by_ticker[t] for t in previous_grants} == previous_grants
    assert not any(capped.sides_by_ticker[t] for t in shard3_ranked[30:])
    assert capped.sides_by_ticker["KXMLB-RED"] == ("yes",)
    assert summarize(capped.sides_by_ticker, requests)[0] == {"none": 0, "single": 0, "both": 27}
    shard3 = shard(capped, 3)
    assert shard3.reduction_only and shard3.withheld_markets == 43 and shard3.live_exposure_units == SHARD3_ALLOCATABLE
    assert shard3.funded_markets == 31 and shard3.granted_sides == 32 and shard3.skipped_markets == 43
    assert not shard(capped, 0).reduction_only
    assert "exchange shard 3 live exposure at allocatable cash" in capped.error
    assert "shard 3: 43 withheld, live exposure $143.68 >= allocatable $143.68" in capped.error
    assert "shard 3: 43 skipped" not in capped.error
    # A plain list of held tickers means "first side": the both-sided market
    # keeps only its first side then.
    listed = allocator.allocate(
        requests, available_cash_units=TOTAL_CASH, existing_committed_units=SHARD3_ALLOCATABLE,
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
        exposure_by_ticker=exposure, held_tickers=list(previous_grants),
    )
    assert listed.sides_by_ticker[shard3_ranked[0]] == ("no",)

    # Hysteresis: live back at 90% with the shard flagged last pass -> still
    # withheld (no cancel/re-grant oscillation) ...
    ninety = {t: int(SHARD3_ALLOCATABLE * 0.9) // 10 for t in holders}
    sticky = allocator.allocate(
        requests, available_cash_units=TOTAL_CASH, existing_committed_units=sum(ninety.values()),
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
        exposure_by_ticker=ninety, held_tickers=previous_grants, withheld_shards=[3],
    )
    assert shard(sticky, 3).reduction_only
    assert not any(sticky.sides_by_ticker[t] for t in shard3_ranked[30:])
    assert all(sticky.sides_by_ticker[t] for t in holders + list(previous_grants))
    # ... the same 90% without the flag never trips (trip is at 100%) ...
    fresh = allocator.allocate(
        requests, available_cash_units=TOTAL_CASH, existing_committed_units=sum(ninety.values()),
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
        exposure_by_ticker=ninety, held_tickers=previous_grants,
    )
    assert not shard(fresh, 3).reduction_only
    assert sum(1 for t in shard3_ranked if fresh.sides_by_ticker[t]) == 44
    # ... and below 80% the flagged shard is released.
    seventy = {t: int(SHARD3_ALLOCATABLE * 0.79) // 10 for t in holders}
    released = allocator.allocate(
        requests, available_cash_units=TOTAL_CASH, existing_committed_units=sum(seventy.values()),
        quote_side_capacity=SIDE_CAPACITY, cash_by_exchange_units=live_cash(),
        exposure_by_ticker=seventy, held_tickers=previous_grants, withheld_shards=[3],
    )
    assert not shard(released, 3).reduction_only
    assert sum(1 for t in shard3_ranked if released.sides_by_ticker[t]) == 44


def test_fleet_wide_live_cap_without_shard_breakdown_is_sticky_too():
    requests = [AllocationRequest(f"M-{i}", f"S{i}", i, "yes", 10_000, 0) for i in range(6)]
    allocator = CapitalAllocator(cash_reserve_fraction=0.0, series_exposure_fraction=1.0, oversubscription=2.0)
    # Live at 100% of allocatable: only the holder and last pass's grant get a side.
    at_cap = allocator.allocate(
        requests, available_cash_units=30_000, existing_committed_units=30_000,
        exposure_by_ticker={"M-4": 30_000}, held_tickers=["M-1"],
    )
    assert at_cap.gate_open and at_cap.withholding
    assert {t for t, s in at_cap.sides_by_ticker.items() if s} == {"M-1", "M-4"}
    assert "live exposure $3.00 >= allocatable $3.00" in at_cap.error
    # 85% with the flag: still withheld; 85% without: normal; 70% with: released.
    sticky = allocator.allocate(
        requests, available_cash_units=30_000, existing_committed_units=25_500,
        exposure_by_ticker={"M-4": 25_500}, fleet_withholding=True,
    )
    assert sticky.withholding and {t for t, s in sticky.sides_by_ticker.items() if s} == {"M-4"}
    fresh = allocator.allocate(
        requests, available_cash_units=30_000, existing_committed_units=25_500, exposure_by_ticker={"M-4": 25_500},
    )
    # Budget cap 60_000: 25_500 live + 3 x 10_000 fit, the 4th does not.
    assert not fresh.withholding and sum(1 for s in fresh.sides_by_ticker.values() if s) == 3
    released = allocator.allocate(
        requests, available_cash_units=30_000, existing_committed_units=21_000,
        exposure_by_ticker={"M-4": 21_000}, fleet_withholding=True,
    )
    assert not released.withholding and sum(1 for s in released.sides_by_ticker.values() if s) == 3
    # At 1.0 the flag never appears.
    legacy = CapitalAllocator(cash_reserve_fraction=0.0, series_exposure_fraction=1.0).allocate(
        requests, available_cash_units=30_000, existing_committed_units=30_000,
        exposure_by_ticker={"M-4": 30_000}, fleet_withholding=True,
    )
    assert not legacy.withholding and "live exposure" not in legacy.error


def test_zero_cash_shard_is_reported_unfunded_not_withheld():
    requests = [
        AllocationRequest("GAS-0", "GAS", 0, "yes", 50, exchange_index=0),
        AllocationRequest("TENNIS-1", "TEN", 1, "yes", 50, exchange_index=3),
        AllocationRequest("GAS-2", "GAS2", 2, "yes", 50, exchange_index=0),
    ]
    for oversubscription in (1.0, 3.0):
        result = CapitalAllocator(series_exposure_fraction=1.0, oversubscription=oversubscription).allocate(
            requests, available_cash_units=1_000, quote_side_capacity=6,
            cash_by_exchange_units={0: 1_000, 3: 0},
        )
        assert result.gate_open
        assert result.sides_by_ticker["TENNIS-1"] == ()
        shard3 = shard(result, 3)
        assert not shard3.reduction_only and shard3.withheld_markets == 0 and shard3.skipped_markets == 1
        assert shard3.shortfall_units == 50
        assert "shard 3: 1 skipped, short $0.01 of budget cap $0.00" in result.error
        assert "TENNIS-1 (exchange shard 3 unfunded)" in result.error
        assert "withheld" not in result.error


def test_allocator_rejects_out_of_range_oversubscription():
    with pytest.raises(ValueError, match="oversubscription"):
        CapitalAllocator(oversubscription=0.5)
    with pytest.raises(ValueError, match="oversubscription"):
        CapitalAllocator(oversubscription=10.5)
    CapitalAllocator(oversubscription=10.0)


# ---------------------------------------------------------------------------
# Session configuration
# ---------------------------------------------------------------------------

def test_session_configuration_validates_and_migrates_allocation_oversubscription():
    from test_sessions import STORED_V2_CONFIGURATION_JSON

    assert default_session_configuration()["fleetRuntime"]["allocationOversubscription"] == 1.0
    validated = validate_session_configuration(default_session_configuration())
    assert validated["fleetRuntime"]["allocationOversubscription"] == 1.0

    stored_v2 = json.loads(STORED_V2_CONFIGURATION_JSON)
    assert "allocationOversubscription" not in stored_v2["fleetRuntime"]
    migrated = validate_session_configuration(stored_v2)
    assert migrated["fleetRuntime"]["allocationOversubscription"] == 1.0
    assert migrated["fleetRuntime"]["startupTimeoutSeconds"] == 410.0
    for version in (1, 3, 4):
        stored = json.loads(STORED_V2_CONFIGURATION_JSON)
        stored["schemaVersion"] = version
        if version == 1:
            del stored["fleetRuntime"]
        assert validate_session_configuration(stored)["fleetRuntime"]["allocationOversubscription"] == 1.0

    config = default_session_configuration()
    config["fleetRuntime"]["allocationOversubscription"] = 3
    assert validate_session_configuration(config)["fleetRuntime"]["allocationOversubscription"] == 3.0
    for value, message in ((0.9, "between 1.0 and 10.0"), (11, "between 1.0 and 10.0"), ("3", "must be a number")):
        bad = default_session_configuration()
        bad["fleetRuntime"]["allocationOversubscription"] = value
        with pytest.raises(ValueError, match=message):
            validate_session_configuration(bad)


# ---------------------------------------------------------------------------
# Screener per-shard pick cap (oversubscribed fleets only)
# ---------------------------------------------------------------------------

class _ShardClient:
    def __init__(self, cash_by_shard):
        self.cash_by_shard = dict(cash_by_shard)

    def list_markets(self, query):
        return []

    def get_positions(self, market_id):
        return [Position(market_id, 0)]

    def get_market_quote(self, market_id):
        return MarketQuote(market_id, 4000, 5000)

    def get_account_balance(self):
        return AccountBalance(
            available_cash_units=sum(self.cash_by_shard.values()), portfolio_value_units=0,
            balance_by_exchange=tuple(sorted(self.cash_by_shard.items())),
        )


def _screener(tmp_path, monkeypatch, frame, cash, *, oversubscription=1.0, reserve=0.0, name="screen.csv",
              yes_budget=100, no_budget=150):
    import screener as screener_module
    from screener import Screener

    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)
    return Screener(
        client=_ShardClient(cash), settings={}, output_path=tmp_path / name,
        default_yes_budget_cents=yes_budget, default_no_budget_cents=no_budget, max_bots=10,
        minimum_carryover_value_cents=20, cash_reserve_fraction=reserve,
        allocation_oversubscription=oversubscription,
    )


def test_screener_caps_picks_per_shard_only_when_oversubscribed(tmp_path, monkeypatch, caplog):
    pd = pytest.importorskip("pandas")
    from fleet_models import ScreenerPick

    rows = [{"Rank": index + 1, "Ticker": f"TEN-{index}", "SearchText": "Tennis", "Exchange Index": 3} for index in range(6)]
    rows += [{"Rank": 7 + index, "Ticker": f"IDX-{index}", "SearchText": "Index", "Exchange Index": 0} for index in range(3)]
    frame = pd.DataFrame(rows)
    # Shard 3 holds $5.00; the first (larger) side budget is $1.50.
    cash = {0: 1_000_000, 3: 50_000}

    # Default 1.0: the historical screen - a funded shard is never capped, only
    # zero-cash shards are excluded - even though at $1.50 a side the shard
    # could fund just 3 first sides.
    legacy = _screener(tmp_path, monkeypatch, frame, cash)
    update = asyncio.run(legacy.refresh({}, reason="test"))
    assert [pick.market_id for pick in update.picks] == [f"TEN-{i}" for i in range(6)] + [f"IDX-{i}" for i in range(3)]
    assert legacy.status_snapshot()["shardCaps"] == {}
    assert not any("Capped" in w for w in legacy.status_snapshot()["warnings"])

    # 1.5: cap = floor($5.00 x 1.5 / $1.50 first side) = 5 -> one tennis market dropped.
    capped = _screener(tmp_path, monkeypatch, frame, cash, oversubscription=1.5, name="screen2.csv")
    with caplog.at_level("WARNING", logger="screener"):
        update = asyncio.run(capped.refresh({}, reason="test"))
    assert [pick.market_id for pick in update.picks] == [f"TEN-{i}" for i in range(5)] + [f"IDX-{i}" for i in range(3)]
    status = capped.status_snapshot()
    assert status["shardCaps"]["3"] == {
        "cap": 5, "selected": 5, "dropped": 1, "cashUnits": 50_000, "allocatableUnits": 50_000,
        "oversubscription": 1.5, "firstSideBudgetCents": 150,
    }
    assert status["shardCaps"]["0"]["cap"] == 100 and status["shardCaps"]["0"]["dropped"] == 0
    assert any(
        "Capped Kalshi exchange shard 3 at 5 market(s)" in w and "$1.50 first-side budget" in w and "dropped 1" in w
        for w in status["warnings"]
    )
    assert any("SCREENER_SHARD_CAP" in r.getMessage() and "shard=3 cap=5" in r.getMessage() for r in caplog.records)

    # The reserve is kept: 0.2 reserve -> $4.00 x 1.5 / $1.50 -> cap 4.
    reserved = _screener(tmp_path, monkeypatch, frame, cash, oversubscription=1.5, reserve=0.2, name="screen3.csv")
    asyncio.run(reserved.refresh({}, reason="test"))
    assert reserved.status_snapshot()["shardCaps"]["3"]["allocatableUnits"] == 40_000
    assert reserved.status_snapshot()["shardCaps"]["3"]["cap"] == 4

    # A running incumbent ranked past the cap displaces the lowest-ranked
    # newcomer inside it instead of being restarted later.
    incumbent = _screener(tmp_path, monkeypatch, frame, cash, oversubscription=1.5, name="screen4.csv")
    current = {"TEN-5": ScreenerPick("TEN-5", "Tennis", 100, 150, {"Ticker": "TEN-5", "Exchange Index": 3})}
    update = asyncio.run(incumbent.refresh(current, reason="test"))
    assert [pick.market_id for pick in update.picks] == [f"TEN-{i}" for i in range(4)] + ["TEN-5", "IDX-0", "IDX-1", "IDX-2"]
    assert incumbent.status_snapshot()["shardCaps"]["3"]["dropped"] == 1


# ---------------------------------------------------------------------------
# Venue error mapping and bot cooldown
# ---------------------------------------------------------------------------

def test_kalshi_insufficient_balance_rejection_maps_to_typed_error():
    from test_kalshi_adaptor import FakeHTTP, make_client
    from fleet_runtime.execution import _ERROR_TYPES, ShardExposureCapError

    http = FakeHTTP()
    http.responses = [HTTPClientError(
        method="POST", path="/portfolio/orders", status_code=400,
        response_text='{"error":{"code":"insufficient_balance","message":"Insufficient balance"}}',
    )]
    client = make_client(http=http)
    with pytest.raises(InsufficientBalanceError):
        client.get_market("MKT")
    assert _ERROR_TYPES["InsufficientBalanceError"] is InsufficientBalanceError
    assert _ERROR_TYPES["ShardExposureCapError"] is ShardExposureCapError
    assert issubclass(ShardExposureCapError, InsufficientBalanceError)


def test_bot_backs_off_creates_after_an_insufficient_balance_rejection():
    from test_top_of_book_boundary import make_bot
    from fleet_runtime.execution import ShardExposureCapError
    from top_of_book_bot import INSUFFICIENT_BALANCE_COOLDOWN_SECONDS, is_insufficient_balance_error, now_ms

    assert INSUFFICIENT_BALANCE_COOLDOWN_SECONDS == 30.0
    assert is_insufficient_balance_error(InsufficientBalanceError("insufficient_balance"))
    # The broker's live-cap refusal takes the same cooldown path.
    assert is_insufficient_balance_error(ShardExposureCapError("shard 3 exposure cap"))
    assert not is_insufficient_balance_error(RuntimeError("insufficient_balance"))

    bot, client = make_bot()
    assert not bot.insufficient_balance_cooldown_active()
    calls = []
    client.create_order = lambda request: calls.append(request) or (_ for _ in ()).throw(InsufficientBalanceError("insufficient_balance"))
    bot.clear_untracked_owned_orders_before_create = _async_false

    deadline = bot.enter_insufficient_balance_cooldown("insufficient_balance")
    assert bot.insufficient_balance_cooldown_active()
    assert 29_000 <= deadline - now_ms() <= 30_000
    # Repeated rejections never extend the cooldown past one window.
    assert bot.enter_insufficient_balance_cooldown("again") <= now_ms() + 30_000

    # No create/amend leaves the bot while the cooldown runs.
    asyncio.run(bot.ensure_side_quote("yes", 4_000))
    assert calls == [] and bot.orders["yes"].order_id is None
    # Cancels are unaffected: a None target still clears the side.
    asyncio.run(bot.ensure_side_quote("yes", None))

    bot.insufficient_balance_cooldown_until_ms = now_ms() - 1
    assert not bot.insufficient_balance_cooldown_active()
    with pytest.raises(InsufficientBalanceError):
        asyncio.run(bot.ensure_side_quote("yes", 4_000))
    assert len(calls) == 1


async def _async_false(side):
    return False


# ---------------------------------------------------------------------------
# Manager: broker limits message and reduce-only restart carryovers
# ---------------------------------------------------------------------------

class _SnapshotAdmin:
    """Broker admin double answering admission_snapshot from canned data."""

    def __init__(self, *, cash_by_shard, positions=(), orders=()):
        self.cash_by_shard = dict(cash_by_shard)
        self.positions = list(positions)
        self.orders = list(orders)
        self.calls = []

    async def call(self, operation, payload=None, timeout=60.0):
        self.calls.append((operation, payload))
        assert operation == "admission_snapshot"
        return {
            "limits": AccountLimits("advanced", RateLimitBucket(300, 300), RateLimitBucket(300, 300)),
            "balance": AccountBalance(
                available_cash_units=sum(self.cash_by_shard.values()), portfolio_value_units=0,
                balance_by_exchange=tuple(sorted(self.cash_by_shard.items())),
            ),
            "orders": self.orders,
            "positions": self.positions,
        }


def _live_manager(tmp_path, *, oversubscription, admin):
    from test_fleet_stall import FakeProcess, RecordingQueue, manager_config
    from fleet_runtime.manager import ManagedWorker, ShardedBotManager
    from session_config import default_session_configuration

    configuration = default_session_configuration()
    configuration["fleetRuntime"]["allocationOversubscription"] = oversubscription
    configuration["fleetRuntime"]["cashReserveFraction"] = 0.1
    configuration["fleetRuntime"]["seriesExposureFraction"] = 0.9
    configuration["launcher"]["yesBudgetCents"] = 825
    configuration["launcher"]["noBudgetCents"] = 975
    from dataclasses import replace
    manager = ShardedBotManager(replace(manager_config(tmp_path), session_configuration=configuration))
    manager._started = True
    manager._planning_only = False
    manager._admin = admin
    manager._broker = FakeProcess(pid=500)
    manager._broker_status_queue = queue.Queue()
    manager._heartbeat_queue = queue.Queue()
    manager._control_ack_queue = queue.Queue()
    manager._request_queue = RecordingQueue()
    commands = RecordingQueue()
    manager._workers = {"worker-00": ManagedWorker("worker-00", FakeProcess(), commands, queue.Queue(), phase="healthy")}
    return manager, commands


def _pick(ticker, shard, reason="screen", yes=825, no=975):
    from fleet_models import ScreenerPick
    return ScreenerPick(ticker, ticker, yes, no, {"Ticker": ticker, "Exchange Index": shard}, selection_reason=reason)


def test_manager_arms_the_broker_live_cap_only_when_oversubscribed(tmp_path):
    from fleet_models import ScreenerUpdate
    from fleet_runtime.execution import BrokerRequest

    async def scenario(oversubscription):
        admin = _SnapshotAdmin(
            cash_by_shard={0: SHARD0_CASH, 3: SHARD3_CASH},
            positions=[AccountPosition("KXMLB-1", 1_100, market_exposure_units=5_000)],
        )
        manager, commands = _live_manager(tmp_path, oversubscription=oversubscription, admin=admin)
        picks = (_pick("KXMLB-1", 3), _pick("KXMLB-2", 3), _pick("KXINX-1", 0))
        await manager.apply_update(ScreenerUpdate(1, 1, "test", picks, tuple(p.market_id for p in picks), (), (), ()))
        limits = [item for item in manager._request_queue.items if isinstance(item, BrokerRequest)]
        assert [item.operation for item in limits] == ["shard_exposure_limits"]
        message = limits[0]
        assert message.response_channel == ""     # fire-and-forget: no response waited on
        payload = message.payload
        assert payload["shard_by_ticker"] == {"KXMLB-1": 3, "KXMLB-2": 3, "KXINX-1": 0}
        assert payload["position_exposure_by_ticker"] == {"KXMLB-1": 5_000}
        assert payload["allocatable_by_shard"] == {0: math.floor(SHARD0_CASH * 0.9), 3: SHARD3_ALLOCATABLE}
        assert payload["enabled"] is (oversubscription > 1.0)
        # The limits are queued before the workers are told to quote.
        assert manager._request_queue.items and commands.items[-1]["action"] == "enable_quoting"
        assert commands.items[-1]["enabled"] is True
        assert manager.status_snapshot()["broker"]["exposureLimits"]["enabled"] is (oversubscription > 1.0)
        return manager

    asyncio.run(scenario(1.0))
    manager = asyncio.run(scenario(3.0))
    assert manager._allocation.oversubscription == 3.0


def test_manager_grants_exchange_position_carryovers_only_their_reducing_side(tmp_path):
    from fleet_models import ScreenerUpdate

    async def scenario():
        admin = _SnapshotAdmin(
            cash_by_shard={0: SHARD0_CASH, 3: SHARD3_CASH},
            positions=[
                AccountPosition("KXCS2-BIG", -1_100, market_exposure_units=6_000),   # 11 NO contracts
                AccountPosition("KXMLB-LONG", 300, market_exposure_units=1_500),     # 3 YES contracts
            ],
        )
        manager, commands = _live_manager(tmp_path, oversubscription=1.0, admin=admin)
        picks = (
            _pick("KXINX-1", 0),
            _pick("KXCS2-BIG", 0, reason="exchange_position"),
            _pick("KXMLB-LONG", 3, reason="exchange_position"),
            _pick("KXMLB-FLAT", 3, reason="exchange_position"),   # flat at the venue: no side
        )
        await manager.apply_update(ScreenerUpdate(1, 1, "test", picks, tuple(p.market_id for p in picks), (), (), ()))
        allocation = manager._allocation
        assert allocation.gate_open
        assert allocation.sides_by_ticker["KXINX-1"] == ("no", "yes")
        assert allocation.sides_by_ticker["KXCS2-BIG"] == ("yes",)     # short NO -> buy YES reduces
        assert allocation.sides_by_ticker["KXMLB-LONG"] == ("no",)     # long YES -> buy NO reduces
        assert "KXMLB-FLAT" not in allocation.sides_by_ticker
        # Reducing sides commit no capital: only the screened market's budgets.
        assert allocation.committed_units == 7_500 + NO_BUDGET + YES_BUDGET
        assert commands.items[-1]["allocations"]["KXCS2-BIG"] == ("yes",)
        assert manager._position_units_by_ticker == {"KXCS2-BIG": -1_100, "KXMLB-LONG": 300}

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Dashboard Overview warning
# ---------------------------------------------------------------------------

def test_overview_attention_warnings_include_allocation_skips(tmp_path):
    import csv

    from ui_api.config import Settings
    from ui_api.store import OperationsStore

    root = Path(tmp_path)
    for name in ("runtime", "logs", "watchdog_state"):
        (root / name).mkdir()
    with (root / "screener_export.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["Rank", "Ticker", "SearchText", "Best EV(c)"])
        writer.writeheader()
        writer.writerow({"Rank": "1", "Ticker": "TEST-1", "SearchText": "Test market", "Best EV(c)": "4.2"})
    error = "skipped 59 of 100 markets (shard 3: 59 skipped, short $575.25 of budget cap $143.68): KXMLB-1 (exchange shard 3 unfunded)"
    status = {
        "launcher": {"lifecycle": "running", "heartbeatAt": 9999999999999}, "bots": [], "counts": {},
        "allocation": {"gate_open": True, "sides_by_ticker": {}, "committed_units": 1, "allocatable_units": 2,
                       "reserved_units": 0, "error": error},
    }
    (root / "runtime" / "launcher_status.json").write_text(json.dumps(status))
    store = OperationsStore(Settings(root, root / "runtime", root / "logs", root / "watchdog_state", "test.service"))
    warnings = store.overview()["warnings"]
    assert f"allocation: {error}" in warnings
    assert any(w.startswith("allocation: skipped 59 of 100 markets (shard 3: 59 skipped") for w in warnings)

    status["allocation"]["error"] = ""
    (root / "runtime" / "launcher_status.json").write_text(json.dumps(status))
    assert not any(w.startswith("allocation:") for w in store.overview()["warnings"])
