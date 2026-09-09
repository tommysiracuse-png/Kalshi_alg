from __future__ import annotations

import json
from pathlib import Path

import pytest

from clients.models import (
    Fill,
    OrderBookDelta,
    OrderBookSnapshot,
    OrderUpdate,
    PositionUpdate,
    PublicTrade,
    StreamReset,
    TickerUpdate,
)
from fleet_models import IntentUrgency, QuoteIntent
from fleet_runtime.assignment import assign_markets, derive_worker_count
from fleet_runtime.broker import IntentQueue
from fleet_runtime.capacity import AllocationRequest, CapitalAllocator, calculate_fleet_capacity
from fleet_runtime.risk import (
    RISK_FLATTEN_LATCH_MS,
    RiskDecision,
    RiskSample,
    RiskThresholds,
    evaluate_risk,
    latch_flatten_only,
    tighten,
)
from fleet_runtime.worker import (
    evaluate_market_risk,
    is_market_data_event,
    observe_stream_event,
    quiet_after_ms_for,
    quiet_market_sample,
    watchdog_exit_allowed,
)
from session_config import default_session_configuration, validate_session_configuration
from session_store import SessionStore
from top_of_book_bot import TelemetryStore


def intent(ticker: str, side: str, generation: int, urgency: IntentUrgency, *, expires: int = 10_000, increasing: bool = True):
    return QuoteIntent(ticker, side, 4_000, 100, generation, urgency, 1, expires, exposure_increasing=increasing)


def test_500_markets_fill_twenty_stable_bounded_shards():
    tickers = [f"MKT-{index:03d}" for index in range(500)]
    assert derive_worker_count(500) == 20
    initial = assign_markets(tickers, worker_count=20)
    assert set(initial) == {f"worker-{index:02d}" for index in range(20)}
    assert all(len(items) == 25 for items in initial.values())
    prior = {ticker: worker for worker, items in initial.items() for ticker in items}
    refreshed = assign_markets(tickers[:-1], worker_count=20, previous=prior)
    assert all(ticker in refreshed[worker] for ticker, worker in prior.items() if ticker != tickers[-1])


def test_advanced_capacity_admits_one_side_per_500_and_reserves_cash():
    capacity = calculate_fleet_capacity(
        api_tier="advanced", read_refill_rate=300, write_refill_rate=300,
        requested_markets=500, cash_available_units=1_000_000,
    )
    assert capacity.normal_quote_side_capacity == 510
    assert capacity.admitted_markets == 500
    assert capacity.admitted_quote_sides == 510
    assert capacity.cash_allocatable_units == 800_000
    assert capacity.reserved_cash_units == 200_000
    assert capacity.gate_open

    downgraded = calculate_fleet_capacity(
        api_tier="basic", read_refill_rate=200, write_refill_rate=100,
        requested_markets=500, cash_available_units=1_000_000,
    )
    assert not downgraded.gate_open
    assert downgraded.reduction_only


def test_fleet_startup_resource_settings_have_bounded_defaults_and_validation():
    config = validate_session_configuration(default_session_configuration())
    fleet = config["fleetRuntime"]
    assert fleet["workerIoThreads"] == 8
    assert fleet["startupProgressTimeoutSeconds"] == 90.0

    invalid = default_session_configuration()
    invalid["fleetRuntime"]["workerIoThreads"] = 0
    with pytest.raises(ValueError, match="workerIoThreads"):
        validate_session_configuration(invalid)


def test_capital_allocation_is_one_side_first_and_enforces_series_limit():
    requests = [
        AllocationRequest(f"MKT-{index}", f"SERIES-{index}", index, "yes", 50, 25)
        for index in range(3)
    ]
    result = CapitalAllocator().allocate(
        requests, available_cash_units=1_000, quote_side_capacity=4,
    )
    assert result.gate_open
    assert all(result.sides_by_ticker[item.ticker] for item in requests)
    assert sum(len(value) for value in result.sides_by_ticker.values()) == 4
    assert result.reserved_units == 200

    # A same-series market that would breach the series cap is skipped while the
    # rest of the fleet keeps quoting; the skip is surfaced in the error summary.
    same_series = [AllocationRequest(f"MKT-{index}", "ONE", index, "yes", 50) for index in range(2)]
    partial = CapitalAllocator(series_exposure_fraction=0.10).allocate(
        same_series, available_cash_units=1_000, quote_side_capacity=2,
    )
    assert partial.gate_open
    assert partial.sides_by_ticker["MKT-0"] == ("yes",)
    assert partial.sides_by_ticker["MKT-1"] == ()
    assert "skipped 1 of 2" in partial.error

    # When nothing at all can be funded the gate still closes.
    unfundable = [AllocationRequest("MKT-0", "ONE", 0, "yes", 5_000)]
    closed = CapitalAllocator(series_exposure_fraction=0.10).allocate(
        unfundable, available_cash_units=1_000, quote_side_capacity=2,
    )
    assert not closed.gate_open
    assert closed.sides_by_ticker == {}


def test_intent_queue_coalesces_and_orders_risk_before_normal():
    queue = IntentQueue()
    queue.submit(intent("A", "yes", 1, IntentUrgency.NORMAL))
    queue.submit(intent("A", "yes", 2, IntentUrgency.NORMAL))
    queue.submit(intent("B", "no", 1, IntentUrgency.RISK_REDUCE, increasing=False))
    first = queue.pop(now_ms=2)
    second = queue.pop(now_ms=2)
    assert first and first.ticker == "B"
    assert second and second.strategy_generation == 2
    assert len(queue) == 0


def test_reduction_only_drops_new_exposure_and_expired_intents():
    queue = IntentQueue()
    queue.submit(intent("A", "yes", 1, IntentUrgency.NORMAL, expires=3))
    queue.submit(intent("B", "yes", 1, IntentUrgency.NORMAL, increasing=True))
    queue.submit(intent("C", "yes", 1, IntentUrgency.RISK_REDUCE, increasing=False))
    selected = queue.pop(now_ms=4, reduction_only=True)
    assert selected and selected.ticker == "C"
    assert queue.pop(now_ms=4, reduction_only=True) is None


def test_shard_telemetry_views_share_one_writer_without_disabling_each_other(tmp_path: Path):
    path = tmp_path / "shards" / "worker-00" / "telemetry.sqlite3"
    first = TelemetryStore(str(path), enabled=True, shard_mode=True)
    first.record_market_metadata(
        ticker="A", title="Market A", series_ticker="SERIES", event_ticker="EVENT-A"
    )
    # The first metadata write intentionally leaves a batched transaction open.
    # Creating the second ticker view must not reapply connection PRAGMAs.
    second = TelemetryStore(str(path), enabled=True, shard_mode=True)
    second.record_market_metadata(
        ticker="B", title="Market B", series_ticker="SERIES", event_ticker="EVENT-B"
    )
    for telemetry, ticker in ((first, "A"), (second, "B")):
        telemetry.start_order_revision(
            revision_key=f"revision-{ticker}", action="create", side="yes",
            client_order_id=f"mm:yes:{ticker}", order_id=f"order-{ticker}",
            placed_at_ms=1_000, size_units=100, price_units=4_000,
            book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
        )
    first.flush()

    assert first.health_snapshot()["available"] is True
    assert second.health_snapshot()["available"] is True
    import sqlite3
    with sqlite3.connect(path) as db:
        assert dict(db.execute(
            "SELECT ticker,COUNT(*) FROM order_revisions GROUP BY ticker"
        )) == {"A": 1, "B": 1}


def test_risk_staleness_tightens_but_does_not_restore():
    healthy = evaluate_risk(
        [RiskSample(1_000, 4_000, 5_000)], now_ms=1_100,
        position_units=0, has_resting_orders=False,
    )
    assert healthy.mode == "normal"
    stale = evaluate_risk(
        [RiskSample(1_000, 4_000, 5_000)], now_ms=200_000,
        position_units=100, has_resting_orders=True,
    )
    assert stale.mode == "flatten_only"
    assert tighten(stale, healthy) == stale


def test_risk_move_window_is_time_bounded_not_count_bounded():
    # A 10c mid move at t=0 followed by a flat book: while the move is inside
    # the window it demotes, once it has aged out of the window it no longer
    # counts even though the sample is still in the deque.
    samples = [
        RiskSample(0, 4_000, 5_800),        # mid 41c
        RiskSample(1_000, 5_000, 4_800),    # mid 51c  -> 10c move
        *[RiskSample(60_000 + index * 10_000, 5_000, 4_800) for index in range(30)],
    ]
    inside = evaluate_risk(samples, now_ms=100_000, position_units=0, has_resting_orders=False)
    assert (inside.mode, inside.reason) == ("reduction_only", "elevated_price_move")
    aged_out = evaluate_risk(samples, now_ms=360_000, position_units=0, has_resting_orders=False)
    assert (aged_out.mode, aged_out.reason) == ("normal", "risk_window_healthy")
    # An explicit window narrows the look-back further.
    narrow = evaluate_risk(samples, now_ms=100_000, position_units=0, has_resting_orders=False, window_ms=30_000)
    assert narrow.mode == "normal"
    # Staleness is still judged on the newest sample alone.
    stale = evaluate_risk(samples, now_ms=360_000 + 120_001, position_units=0, has_resting_orders=False)
    assert stale.reason == "risk_inputs_stale"


def test_risk_thresholds_come_from_fleet_config_and_reproduce_defaults():
    assert RiskThresholds.from_fleet_config({}) == RiskThresholds(120_000, 120_000, 750, 2_000)
    assert RiskThresholds.from_fleet_config(default_session_configuration()["fleetRuntime"]) == RiskThresholds()
    custom = RiskThresholds.from_fleet_config({
        "riskWindowSeconds": 45, "riskStaleSeconds": 300.0,
        "riskElevatedMoveCents": 12.5, "riskExtremeMoveCents": 30,
    })
    assert custom == RiskThresholds(45_000, 300_000, 1_250, 3_000)
    # Garbage / non-positive values fall back to the defaults instead of raising.
    assert RiskThresholds.from_fleet_config({"riskWindowSeconds": "x", "riskElevatedMoveCents": 0}) == RiskThresholds()

    samples = [RiskSample(0, 4_000, 5_800), RiskSample(1_000, 5_000, 4_800)]  # 10c move
    kwargs = dict(now_ms=2_000, position_units=100, has_resting_orders=False)
    assert evaluate_risk(samples, **kwargs).reason == "elevated_price_move"
    relaxed = evaluate_risk(samples, **kwargs, elevated_move_units=custom.elevated_move_units,
                            extreme_move_units=custom.extreme_move_units)
    assert relaxed.mode == "normal"
    tight = evaluate_risk(samples, **kwargs, elevated_move_units=300, extreme_move_units=900)
    assert (tight.mode, tight.reason) == ("flatten_only", "extreme_price_move")
    # A longer stale threshold keeps an old-but-trusted sample usable.
    assert evaluate_risk(samples, now_ms=200_000, position_units=0, has_resting_orders=False).reason == "risk_inputs_stale"
    assert evaluate_risk(samples, now_ms=200_000, position_units=0, has_resting_orders=False,
                         stale_after_ms=custom.stale_after_ms).mode == "normal"


def _actor(position: int = 0, *, book_ready: bool = True, yes=None, no=None):
    from types import SimpleNamespace

    return SimpleNamespace(
        book_ready=book_ready,
        book_yes={4_000: 10, 3_900: 5} if yes is None else yes,
        book_no={5_800: 8} if no is None else no,
        net_position_units=position,
    )


RISK_DEFAULTS = {"stale_after_ms": 120_000, "window_ms": 120_000, "elevated_move_units": 750, "extreme_move_units": 2_000}


def test_quiet_market_is_resampled_only_with_fresh_per_market_evidence():
    actor = _actor()
    window = [RiskSample(1_000, 4_000, 5_800)]
    now = 1_000 + 90_000  # quiet for 90 s, last real event 90 s ago
    common = dict(quiet_after_ms=60_000, stale_after_ms=120_000, stream_connected=True)

    # Healthy: own event within stale_after -> current best bids stamped now,
    # tagged synthetic so it never counts as liveness evidence.
    synthetic = quiet_market_sample(actor, window, now_ms=now, last_event_ms=1_000, **common)
    assert synthetic == RiskSample(now, 4_000, 5_800, synthetic=True)
    assert synthetic.synthetic
    window.append(synthetic)
    assert evaluate_risk(window, now_ms=now, position_units=0, has_resting_orders=False).mode == "normal"
    # Not quiet yet: a fresh sample means no synthetic one.
    assert quiet_market_sample(actor, window, now_ms=now + 1_000, last_event_ms=1_000, **common) is None

    later = 1_000 + 120_001  # own evidence just older than stale_after
    assert quiet_market_sample(actor, [], now_ms=later, last_event_ms=1_000, **common) is None
    # No evidence at all (no real event since the actor started) -> none.
    assert quiet_market_sample(actor, [], now_ms=now, last_event_ms=None, **common) is None
    # The shard flag is only a veto: connected with stale evidence is still None ...
    assert quiet_market_sample(actor, [], now_ms=later, last_event_ms=1_000, **common) is None
    # ... and disconnected with fresh evidence is None too.
    assert quiet_market_sample(actor, [], now_ms=now, last_event_ms=1_000, quiet_after_ms=60_000, stale_after_ms=120_000, stream_connected=False) is None
    # A book awaiting its snapshot after a reset yields None.
    assert quiet_market_sample(_actor(book_ready=False, yes={}, no={}), [], now_ms=now, last_event_ms=1_000, **common) is None
    # An empty window on a healthy market is seeded (one-sided books keep None).
    assert quiet_market_sample(_actor(yes={4_100: 1}, no={}), [], now_ms=now, last_event_ms=1_000, **common) == RiskSample(now, 4_100, None, synthetic=True)


def test_synthetic_samples_feed_the_move_window_but_never_liveness():
    real = RiskSample(0, 4_000, 5_800)
    kwargs = dict(position_units=100, has_resting_orders=False, **RISK_DEFAULTS)
    # A synthetic sample stamped 'now' cannot mask a real sample older than
    # stale_after: staleness is judged on real samples only.
    assert evaluate_risk([real], now_ms=100_000, **kwargs).mode == "normal"
    masked = evaluate_risk([real, RiskSample(130_000, 4_000, 5_800, synthetic=True)], now_ms=130_000, **kwargs)
    assert (masked.mode, masked.reason) == ("flatten_only", "risk_inputs_stale")
    # A window of synthetic samples alone has no venue evidence at all.
    only_synthetic = evaluate_risk([RiskSample(1_000, 4_000, 5_800, synthetic=True)], now_ms=1_000, **kwargs)
    assert only_synthetic.reason == "risk_inputs_stale"
    # ... but inside the stale bound a synthetic sample does take part in the move window.
    moved = evaluate_risk([real, RiskSample(50_000, 6_500, 3_300, synthetic=True)], now_ms=60_000, **kwargs)
    assert (moved.mode, moved.reason) == ("flatten_only", "extreme_price_move")


def test_only_venue_market_data_events_refresh_liveness_and_samples():
    market_data = (
        OrderBookSnapshot("M", 1, {4_000: 10}, {5_800: 8}),
        OrderBookDelta("M", 2, "yes", 4_000, 1, 5_000),
        PublicTrade("M", "t1", 6_000, 4_100, 5_900, 1),
        TickerUpdate("M", 7_000, price_units=4_100),
    )
    own_traffic = (
        OrderUpdate("M", "yes", "o1", "mm:yes:M", "canceled"),
        Fill("M", "o1", "t2", 8_000, 10),
        PositionUpdate("M", 90),
        StreamReset("M"),
    )
    assert all(is_market_data_event(event) for event in market_data)
    assert not any(is_market_data_event(event) for event in own_traffic)

    actor = _actor(90)
    window: list[RiskSample] = []
    last_event: dict[str, int] = {}
    for index, event in enumerate(own_traffic):
        assert observe_stream_event(event, actor, window, last_event, now_ms=1_000 + index) is False
    assert last_event == {} and window == []
    # Events one second apart: each clears the RISK_SAMPLE_MIN_INTERVAL_MS cap.
    for index, event in enumerate(market_data):
        assert observe_stream_event(event, actor, window, last_event, now_ms=10_000 + index * 1_000) is True
    assert last_event == {"M": 13_000}
    assert [sample.timestamp_ms for sample in window] == [10_000, 11_000, 12_000, 13_000]
    assert not any(sample.synthetic for sample in window)
    # Market data on a book that is not ready (awaiting its snapshot after a
    # reset) refreshes liveness but yields no sample.
    reset_actor = _actor(book_ready=False, yes={}, no={})
    assert observe_stream_event(market_data[1], reset_actor, window, last_event, now_ms=20_000) is False
    assert last_event == {"M": 20_000} and len(window) == 4


def test_dead_feed_goes_stale_within_one_stale_period_despite_own_order_events():
    # Two markets on one shard.  A keeps receiving deltas (so the shard-wide
    # stream flag stays True the whole time); B's market-data subscription
    # silently died at t=0 after its last delta, but B's OWN order traffic
    # (cancel acks, the watchdog IOC's update/fill, position updates) keeps
    # arriving every 20 s.  Simulate the ~60 s risk loop.
    windows = {"A": [RiskSample(0, 4_000, 5_800)], "B": [RiskSample(0, 4_000, 5_800)]}
    last_event = {"A": 0, "B": 0}
    actors = {"A": _actor(0), "B": _actor(80)}
    latch: dict[str, int] = {}
    quiet_after = quiet_after_ms_for(120_000)
    seen: dict[str, list[tuple[int, str, str]]] = {"A": [], "B": []}
    for now in range(60_000, 480_001, 60_000):
        # A is live: a real delta shortly before every visit.
        observe_stream_event(OrderBookDelta("A", 1, "yes", 4_000, 1, now - 5_000), actors["A"], windows["A"], last_event, now_ms=now - 5_000)
        for offset in (-45_000, -25_000, -5_000):
            for event in (
                OrderUpdate("B", "yes", "o1", "mm:yes:B", "canceled"),
                Fill("B", "o1", f"t{now + offset}", now + offset, 10),
                PositionUpdate("B", 70),
            ):
                assert observe_stream_event(event, actors["B"], windows["B"], last_event, now_ms=now + offset) is False
        for ticker in ("A", "B"):
            decision = evaluate_market_risk(
                actors[ticker], windows[ticker], ticker=ticker, now_ms=now,
                last_event_ms=last_event[ticker], stream_connected=True,
                quiet_after_ms=quiet_after, flatten_latch=latch, risk_kwargs=RISK_DEFAULTS,
                has_resting_orders=False,
            )
            seen[ticker].append((now, decision.mode, decision.reason))
    assert all(mode == "normal" for _, mode, _ in seen["A"])
    # Own order traffic refreshed nothing: B's only real evidence is the t=0 delta.
    assert last_event["B"] == 0
    assert [sample.timestamp_ms for sample in windows["B"] if not sample.synthetic] == [0]
    by_time = {now: (mode, reason) for now, mode, reason in seen["B"]}
    assert by_time[60_000] == ("normal", "risk_window_healthy")
    assert by_time[120_000] == ("normal", "risk_window_healthy")   # exactly stale_after: not yet stale
    # Stale at the first visit after t + stale_after, i.e. within
    # stale_after (120 s) + one loop period (60 s); flatten_only because it
    # holds inventory, and it stays stale while own events keep flowing.
    assert by_time[180_000] == ("flatten_only", "risk_inputs_stale")
    assert all(by_time[now] == ("flatten_only", "risk_inputs_stale") for now in range(180_000, 480_001, 60_000))


def test_quiet_healthy_market_never_stale_at_defaults_across_many_visits():
    # Illiquid but alive: one venue market-data event every 100 s (inside
    # riskStaleSeconds=120), nothing else.  An hour of ~60 s visits at every
    # loop phase never demotes it, and the quiet spells are re-sampled.
    for phase in (0, 7_000, 19_000, 31_000, 43_000, 59_000):
        window: list[RiskSample] = []
        last_event: dict[str, int] = {}
        actor = _actor(50)
        emitted = 0
        for now in range(60_000 + phase, 3_600_000, 60_000):
            while emitted * 100_000 <= now:
                stamp = emitted * 100_000
                observe_stream_event(TickerUpdate("Q", stamp, price_units=4_100), actor, window, last_event, now_ms=stamp)
                emitted += 1
            decision = evaluate_market_risk(
                actor, window, ticker="Q", now_ms=now, last_event_ms=last_event.get("Q"), stream_connected=True,
                quiet_after_ms=quiet_after_ms_for(120_000), flatten_latch={},
                risk_kwargs=RISK_DEFAULTS, has_resting_orders=True,
            )
            assert (decision.mode, decision.reason) == ("normal", "risk_window_healthy"), (phase, now)
        assert any(sample.synthetic for sample in window)
        real = [sample.timestamp_ms for sample in window if not sample.synthetic]
        assert real == list(range(0, real[-1] + 1, 100_000))


def test_quiet_after_leaves_more_than_a_loop_period_to_re_sample():
    assert quiet_after_ms_for(120_000) == 40_000
    assert quiet_after_ms_for(300_000) == 100_000
    assert quiet_after_ms_for(1_500) == 1_000   # floored at 1 s
    # The re-sample window (quiet_after, stale_after] is 80 s at the defaults,
    # wider than the 60 s loop period plus processing.
    assert 120_000 - quiet_after_ms_for(120_000) > 60_000
    # A market quiet since t=0, visited every 61.5 s (60 s loop + processing)
    # at every phase, is re-sampled before stale_after at the derived
    # cadence; at the old stale_after // 2 cadence some phases miss it.
    for quiet_after, expect_every_phase in ((quiet_after_ms_for(120_000), True), (120_000 // 2, False)):
        hits = []
        for phase in range(0, 61_500, 500):
            window = [RiskSample(0, 4_000, 5_800)]
            actor = _actor(50)
            for now in range(phase, 120_001, 61_500):
                sample = quiet_market_sample(
                    actor, window, now_ms=now, quiet_after_ms=quiet_after, last_event_ms=0, stale_after_ms=120_000,
                )
                if sample is not None:
                    window.append(sample)
            hits.append(any(sample.synthetic for sample in window))
        assert all(hits) is expect_every_phase, quiet_after


def test_extreme_move_flatten_only_latches_while_inventory_is_held():
    move = [RiskSample(0, 4_000, 5_800), RiskSample(1_000, 6_500, 3_300)]  # 41c -> 66c
    actor = _actor(100)
    latch: dict[str, int] = {}

    def visit(now, *, last_event):
        return evaluate_market_risk(
            actor, move, ticker="X", now_ms=now, last_event_ms=last_event, stream_connected=True,
            quiet_after_ms=60_000, flatten_latch=latch, risk_kwargs=RISK_DEFAULTS, has_resting_orders=False,
        )

    first = visit(50_000, last_event=1_000)
    assert (first.mode, first.reason) == ("flatten_only", "extreme_price_move")
    assert latch == {"X": 50_000}
    # Keep the feed alive with a real event so the window is never stale, and
    # step past the move window: without the latch this would be 'normal'.
    move.append(RiskSample(170_000, 6_500, 3_300))
    later = visit(180_000, last_event=170_000)
    assert (later.mode, later.reason) == ("flatten_only", "extreme_price_move_latched")
    assert evaluate_risk(move, now_ms=180_000, position_units=100, has_resting_orders=False, **RISK_DEFAULTS).mode == "normal"
    # A stale window keeps its own flatten reason (still flatten_only).
    stale = visit(400_000, last_event=170_000)
    assert (stale.mode, stale.reason) == ("flatten_only", "risk_inputs_stale")
    # Elevated moves are never latched: reduction_only relaxes on expiry.
    elevated_window = [RiskSample(0, 4_000, 5_800), RiskSample(1_000, 5_000, 4_800), RiskSample(50_000, 5_000, 4_800)]
    other_latch: dict[str, int] = {}
    early = latch_flatten_only(
        evaluate_risk(elevated_window, now_ms=60_000, position_units=100, has_resting_orders=False, **RISK_DEFAULTS),
        other_latch, "E", now_ms=60_000, position_units=100,
    )
    assert (early.mode, early.reason) == ("reduction_only", "elevated_price_move") and other_latch == {}
    relaxed = latch_flatten_only(
        evaluate_risk(elevated_window, now_ms=170_000, position_units=100, has_resting_orders=False, **RISK_DEFAULTS),
        other_latch, "E", now_ms=170_000, position_units=100,
    )
    assert (relaxed.mode, relaxed.reason) == ("normal", "risk_window_healthy")


def test_flatten_latch_downgrades_to_reduction_only_after_cooldown_until_flat():
    healthy = RiskDecision("normal", "risk_window_healthy", 0, 0.9)
    extreme = RiskDecision("flatten_only", "extreme_price_move", 0, 0.3)
    elevated = RiskDecision("reduction_only", "elevated_price_move", 0, 0.6)
    stale = RiskDecision("flatten_only", "risk_inputs_stale", 0, 0.0)
    cap = RISK_FLATTEN_LATCH_MS

    # Flat position: latch dropped immediately, evaluation passes through.
    latch = {"X": 10_000}
    assert latch_flatten_only(healthy, latch, "X", now_ms=20_000, position_units=0) == healthy
    assert latch == {}
    # Held position: flatten_only until the cooldown expires with no new move.
    latch = {}
    assert latch_flatten_only(extreme, latch, "X", now_ms=10_000, position_units=100) == extreme
    held = latch_flatten_only(healthy, latch, "X", now_ms=10_000 + cap - 1, position_units=100)
    assert (held.mode, held.reason) == ("flatten_only", "extreme_price_move_latched")
    # Cooldown elapsed, inventory still held: never 'normal' - downgraded to
    # reduction_only and the latch is retained, for as long as it takes.
    capped = latch_flatten_only(healthy, latch, "X", now_ms=10_000 + cap, position_units=100)
    assert (capped.mode, capped.reason) == ("reduction_only", "extreme_price_move_cooldown")
    assert capped.confidence == healthy.confidence and latch == {"X": 10_000}
    much_later = latch_flatten_only(healthy, latch, "X", now_ms=10_000 + 10 * cap, position_units=100)
    assert (much_later.mode, much_later.reason) == ("reduction_only", "extreme_price_move_cooldown")
    # Evaluations at least as restrictive pass through with their own reason.
    assert latch_flatten_only(elevated, latch, "X", now_ms=10_000 + cap, position_units=100) == elevated
    assert latch_flatten_only(stale, latch, "X", now_ms=10_000 + cap, position_units=100) == stale
    # A new extreme move after the cap re-arms full flatten_only from that moment.
    assert latch_flatten_only(extreme, latch, "X", now_ms=10_000 + 2 * cap, position_units=100) == extreme
    assert latch == {"X": 10_000 + 2 * cap}
    assert latch_flatten_only(healthy, latch, "X", now_ms=10_000 + 3 * cap - 1, position_units=100).mode == "flatten_only"
    assert latch_flatten_only(healthy, latch, "X", now_ms=10_000 + 3 * cap, position_units=100).mode == "reduction_only"
    # Only a flat position releases it.
    assert latch_flatten_only(healthy, latch, "X", now_ms=10_000 + 4 * cap, position_units=0) == healthy
    assert latch == {}
    # A new extreme move inside the cooldown re-arms it from that moment.
    latch = {"X": 10_000}
    assert latch_flatten_only(extreme, latch, "X", now_ms=300_000, position_units=100) == extreme
    assert latch == {"X": 300_000}
    assert latch_flatten_only(healthy, latch, "X", now_ms=10_000 + cap, position_units=100).mode == "flatten_only"
    # Unknown ticker and a custom cooldown.
    assert latch_flatten_only(healthy, {}, "Y", now_ms=1, position_units=5) == healthy
    latch = {"X": 0}
    assert latch_flatten_only(healthy, latch, "X", now_ms=4_999, position_units=5, latch_ms=5_000).mode == "flatten_only"
    assert latch_flatten_only(healthy, latch, "X", now_ms=5_000, position_units=5, latch_ms=5_000).mode == "reduction_only"

    # End to end through the loop visit: after the cap the market no longer
    # qualifies for watchdog IOC exits, keeps the adding side suppressed, and
    # returns to normal only once flat.
    move = [RiskSample(0, 4_000, 5_800), RiskSample(1_000, 6_500, 3_300)]
    actor = _actor(100)
    latch = {}

    def visit(now):
        move.append(RiskSample(now - 1_000, 6_500, 3_300))
        return evaluate_market_risk(
            actor, move, ticker="X", now_ms=now, last_event_ms=now - 1_000, stream_connected=True,
            quiet_after_ms=40_000, flatten_latch=latch, risk_kwargs=RISK_DEFAULTS,
            has_resting_orders=False, latch_ms=300_000,
        )

    assert visit(50_000).reason == "extreme_price_move"
    assert visit(200_000).reason == "extreme_price_move_latched"
    assert watchdog_exit_allowed(mode="flatten_only", position_units=100, shutdown_frozen=False)
    after_cap = visit(360_000)
    assert (after_cap.mode, after_cap.reason) == ("reduction_only", "extreme_price_move_cooldown")
    assert not watchdog_exit_allowed(mode=after_cap.mode, position_units=100, shutdown_frozen=False)
    assert visit(420_000).mode == "reduction_only"
    actor.net_position_units = 0
    assert (visit(480_000).mode, latch) == ("normal", {})


def test_risk_config_fallback_logs_one_warning(caplog):
    import logging

    logger_name = "kalshi_top_of_book_bot.fleet_risk"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        assert RiskThresholds.from_fleet_config(
            {"riskWindowSeconds": "x", "riskElevatedMoveCents": 0, "riskStaleSeconds": 120, "riskExtremeMoveCents": True}
        ) == RiskThresholds()
    records = [record for record in caplog.records if record.name == logger_name]
    assert len(records) == 1
    assert "RISK_CONFIG_FALLBACK" in records[0].getMessage()
    for key in ("riskWindowSeconds='x'", "riskElevatedMoveCents=0", "riskExtremeMoveCents=True"):
        assert key in records[0].getMessage()
    assert "riskStaleSeconds" not in records[0].getMessage()

    # Missing keys (older sessions) and valid values are silent.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=logger_name):
        RiskThresholds.from_fleet_config({})
        RiskThresholds.from_fleet_config(default_session_configuration()["fleetRuntime"])
    assert not [record for record in caplog.records if record.name == logger_name]

    # An explicit logger (the worker passes its own) receives the line instead.
    custom = logging.getLogger("test.fleet_risk.custom")
    with caplog.at_level(logging.WARNING, logger=custom.name):
        RiskThresholds.from_fleet_config({"riskWindowSeconds": -1}, log=custom)
    assert [record.name for record in caplog.records if "RISK_CONFIG_FALLBACK" in record.getMessage()] == [custom.name]


def test_fleet_runtime_risk_fields_validate_and_default_for_older_rows():
    config = default_session_configuration()
    fleet = validate_session_configuration(config)["fleetRuntime"]
    assert (fleet["riskWindowSeconds"], fleet["riskElevatedMoveCents"], fleet["riskExtremeMoveCents"], fleet["riskStaleSeconds"]) == (120.0, 7.5, 20.0, 120.0)

    # Stored v1..v4 rows predate the fields and gain the defaults on read.
    for version in (1, 2, 3, 4):
        stored = default_session_configuration()
        stored["schemaVersion"] = version
        if version == 1:
            stored.pop("fleetRuntime")
        else:
            for key in ("riskWindowSeconds", "riskElevatedMoveCents", "riskExtremeMoveCents", "riskStaleSeconds"):
                stored["fleetRuntime"].pop(key)
            stored["fleetRuntime"]["startupTimeoutSeconds"] = 410.0
        if version < 3:
            stored.pop("botClasses")
        if version < 4:
            stored.pop("screener")
        migrated = validate_session_configuration(stored)
        assert migrated["schemaVersion"] == 4
        assert migrated["fleetRuntime"]["riskElevatedMoveCents"] == 7.5
        assert migrated["fleetRuntime"]["riskStaleSeconds"] == 120.0
        if version > 1:
            assert migrated["fleetRuntime"]["startupTimeoutSeconds"] == 410.0
        assert RiskThresholds.from_fleet_config(migrated["fleetRuntime"]) == RiskThresholds()

    config["fleetRuntime"].update(riskWindowSeconds=45, riskElevatedMoveCents=12.5, riskExtremeMoveCents=30, riskStaleSeconds=300)
    fleet = validate_session_configuration(config)["fleetRuntime"]
    assert fleet["riskWindowSeconds"] == 45.0 and fleet["riskElevatedMoveCents"] == 12.5
    assert RiskThresholds.from_fleet_config(fleet) == RiskThresholds(45_000, 300_000, 1_250, 3_000)

    import pytest

    for key, value, message in [
        ("riskWindowSeconds", 0, "riskWindowSeconds must be > 0"),
        ("riskStaleSeconds", -1, "riskStaleSeconds must be > 0"),
        ("riskElevatedMoveCents", 0, "riskElevatedMoveCents must be between 0 and 100"),
        ("riskExtremeMoveCents", 101, "riskExtremeMoveCents must be between 0 and 100"),
        ("riskElevatedMoveCents", 40, "riskElevatedMoveCents must be <= fleetRuntime.riskExtremeMoveCents"),
        ("riskElevatedMoveCents", "7.5", "riskElevatedMoveCents must be a number"),
    ]:
        bad = default_session_configuration()
        bad["fleetRuntime"][key] = value
        with pytest.raises(ValueError, match=message):
            validate_session_configuration(bad)


def test_schema_v1_is_migrated_and_500_is_explicitly_valid():
    legacy = default_session_configuration()
    legacy["schemaVersion"] = 1
    legacy.pop("fleetRuntime")
    migrated = validate_session_configuration(legacy)
    assert migrated["schemaVersion"] == 4  # v1 -> v2 (fleetRuntime) -> v3 (botClasses) -> v4 (screener)
    assert migrated["fleetRuntime"]["shardSize"] == 25
    assert migrated["botClasses"]["enabled"] is False
    assert migrated["screener"]["status"] == "open"
    migrated["launcher"]["maxBots"] = 500
    assert validate_session_configuration(migrated)["launcher"]["maxBots"] == 500


def test_shard_telemetry_uses_one_connection_and_history_routes_by_manifest(tmp_path: Path):
    sessions = SessionStore(tmp_path / "session_data")
    run = sessions.prepare_run()
    artifact = Path(run["artifactPath"])
    database = artifact / "shards" / "worker-00" / "telemetry.sqlite3"
    first = TelemetryStore(str(database), enabled=True, shard_mode=True)
    second = TelemetryStore(str(database), enabled=True, shard_mode=True)
    assert first._connection is second._connection

    for telemetry, ticker in ((first, "A"), (second, "B")):
        telemetry.record_market_metadata(
            ticker=ticker, title=f"Market {ticker}", series_ticker="SERIES",
            event_ticker="EVENT", market_url=None,
        )
        telemetry.record_fill(
            fill_key=f"fill-{ticker}", ts_ms=1_000, ticker=ticker, side="yes",
            trade_id=f"trade-{ticker}", order_id=f"order-{ticker}", price_units=4_000,
            size_units=100, fee_units=0, is_taker=False, inventory_before_units=0,
            inventory_after_units=100, fair_yes_before_units=4_000,
            best_yes_bid_units=3_900, best_no_bid_units=5_900, queue_ahead_units=0,
        )
        telemetry.start_order_revision(
            revision_key=f"revision-{ticker}", action="create", side="yes",
            client_order_id=f"mm:yes:{ticker}", order_id=f"order-{ticker}",
            placed_at_ms=900, size_units=100, price_units=4_000,
            book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
        )
    first.flush()
    (artifact / "fleet_manifest.json").write_text(json.dumps({
        "tickerToShard": {"A": "worker-00", "B": "worker-00"},
        "workers": {"worker-00": ["A", "B"]},
    }))

    markets = {item["ticker"]: item for item in sessions.run_markets(run["id"])["items"]}
    assert set(markets) == {"A", "B"}
    assert markets["A"]["fillCount"] == 1
    assert markets["B"]["fillCount"] == 1
    assert markets["A"]["description"] == "Market A"


def test_capital_allocation_funds_markets_only_from_their_own_exchange_shard():
    """Kalshi cash is local to an exchange shard; a market on an unfunded shard
    must be skipped with a reason, never funded from another shard's cash."""
    requests = [
        AllocationRequest("GAS-0", "GAS", 0, "yes", 50, exchange_index=0),
        AllocationRequest("TENNIS-1", "TEN", 1, "yes", 50, exchange_index=3),
        AllocationRequest("GAS-2", "GAS2", 2, "yes", 50, exchange_index=0),
    ]
    result = CapitalAllocator(series_exposure_fraction=1.0).allocate(
        requests, available_cash_units=1_000, quote_side_capacity=6,
        cash_by_exchange_units={0: 1_000, 3: 0},
    )
    assert result.gate_open
    assert result.sides_by_ticker["GAS-0"] == ("yes",)
    assert result.sides_by_ticker["GAS-2"] == ("yes",)
    assert result.sides_by_ticker["TENNIS-1"] == ()
    assert "exchange shard 3 unfunded" in result.error

    # Without a breakdown the venue gave no per-shard information: legacy
    # behaviour, every market draws from the single pool.
    legacy = CapitalAllocator(series_exposure_fraction=1.0).allocate(
        requests, available_cash_units=1_000, quote_side_capacity=6,
    )
    assert legacy.sides_by_ticker["TENNIS-1"] == ("yes",)


def test_risk_move_ignores_samples_whose_spread_is_not_a_price():
    """2026-09-02 KXAAAGASDOH-26SEP03-3.885: the YES ask stayed at 71-79c while
    the YES bid flickered between 74c and 3c, so raw mids swung 40c <-> 75c and
    a position was flattened into the vacuum on a 35c "extreme move" while the
    reliable quotes had moved 7c.  Samples wider than the reliable-spread limit
    carry no mid; the move is measured on the rest."""
    kwargs = dict(position_units=-3_000, has_resting_orders=False, elevated_move_units=1_450, extreme_move_units=3_000)
    samples = [
        RiskSample(1_000, 6_600, 2_700),                  # 66 / 73: mid 69.5
        RiskSample(30_000, 300, 2_400),                   # 3 / 76: 73c wide, not a price
        RiskSample(31_000, 7_400, 2_100),                 # 74 / 79: mid 76.5
        RiskSample(32_000, 300, 2_100),                   # 3 / 79: not a price
        RiskSample(60_000, 7_500, 2_100),                 # 75 / 79: mid 77
    ]
    decision = evaluate_risk(samples, now_ms=60_000, **kwargs)
    assert decision.mode == "normal" and decision.reason == "risk_window_healthy"
    # The old reading of the same window (every sample a price) was an extreme move.
    raw = evaluate_risk(samples, now_ms=60_000, max_spread_units=10_000, **kwargs)
    assert raw.mode == "flatten_only" and raw.reason == "extreme_price_move"
    # A real move measured on reliable quotes still trips: mid 67.5 -> 97.5 with tight spreads.
    real = [RiskSample(1_000, 6_500, 3_000), RiskSample(50_000, 9_700, 200)]
    assert evaluate_risk(real, now_ms=60_000, **kwargs).mode == "flatten_only"
    # Wide samples still count as liveness and for the crossed-book check.
    crossed = [RiskSample(1_000, 6_600, 2_900), RiskSample(59_000, 7_000, 3_100)]
    assert evaluate_risk(crossed, now_ms=60_000, **kwargs).reason == "crossed_or_locked_book"


def test_risk_window_with_no_usable_mid_cannot_measure_risk():
    wide = [RiskSample(1_000, 300, 2_400), RiskSample(50_000, 300, 2_100)]
    holding = evaluate_risk(wide, now_ms=60_000, position_units=-3_000, has_resting_orders=False)
    assert (holding.mode, holding.reason) == ("reduction_only", "book_too_wide")
    flat = evaluate_risk(wide, now_ms=60_000, position_units=0, has_resting_orders=False)
    assert (flat.mode, flat.reason) == ("normal", "book_too_wide")
    # Thresholds come from fleetRuntime when present, default 20c otherwise.
    from fleet_runtime.risk import DEFAULT_MAX_RELIABLE_SPREAD_UNITS, RiskThresholds
    assert RiskThresholds.from_fleet_config({}).max_spread_units == DEFAULT_MAX_RELIABLE_SPREAD_UNITS == 2_000
    assert RiskThresholds.from_fleet_config({"riskMaxSpreadCents": 12.5}).max_spread_units == 1_250
    loose = evaluate_risk(wide, now_ms=60_000, position_units=-3_000, has_resting_orders=False, max_spread_units=8_000)
    assert loose.reason == "risk_window_healthy"
