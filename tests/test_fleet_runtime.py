from __future__ import annotations

import json
from pathlib import Path

from fleet_models import IntentUrgency, QuoteIntent
from fleet_runtime.assignment import assign_markets, derive_worker_count
from fleet_runtime.broker import IntentQueue
from fleet_runtime.capacity import AllocationRequest, CapitalAllocator, calculate_fleet_capacity
from fleet_runtime.risk import RiskDecision, RiskSample, evaluate_risk, tighten
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

    same_series = [AllocationRequest(f"MKT-{index}", "ONE", index, "yes", 50) for index in range(2)]
    failed = CapitalAllocator(series_exposure_fraction=0.10).allocate(
        same_series, available_cash_units=1_000, quote_side_capacity=2,
    )
    assert not failed.gate_open
    assert failed.sides_by_ticker == {}


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


def test_schema_v1_is_migrated_and_500_is_explicitly_valid():
    legacy = default_session_configuration()
    legacy["schemaVersion"] = 1
    legacy.pop("fleetRuntime")
    migrated = validate_session_configuration(legacy)
    assert migrated["schemaVersion"] == 2
    assert migrated["fleetRuntime"]["shardSize"] == 25
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
