import sqlite3
import json
from pathlib import Path

import pytest

from session_store import SessionStore, now_ms
from top_of_book_bot import TelemetryStore


def telemetry_for_run(store: SessionStore, run: dict, ticker: str = "TEST-1") -> tuple[TelemetryStore, Path]:
    path = Path(run["artifactPath"]) / "markets" / ticker / "telemetry.sqlite3"
    telemetry = TelemetryStore(str(path), enabled=True)
    telemetry.record_market_metadata(
        ticker=ticker,
        title="Will TEST settle yes?",
        series_ticker="TEST",
        event_ticker="TEST-EVENT",
        series_title="Test series",
        market_url="https://kalshi.com/markets/test/test-series/test-event",
    )
    return telemetry, path


def record_fill(
    telemetry: TelemetryStore,
    *,
    key: str,
    timestamp: int,
    side: str,
    price: int | None,
    size: int,
    fee: int | None,
    yes_bid: int | None,
    no_bid: int | None,
) -> None:
    telemetry.record_fill(
        fill_key=key, ts_ms=timestamp, ticker="TEST-1", side=side, trade_id=key,
        order_id=f"{key}-order", price_units=price, size_units=size, fee_units=fee,
        is_taker=False, inventory_before_units=0, inventory_after_units=0,
        fair_yes_before_units=None, best_yes_bid_units=yes_bid,
        best_no_bid_units=no_bid, queue_ahead_units=0,
    )


def test_additive_telemetry_tables_and_revision_transitions(tmp_path: Path):
    database = tmp_path / "telemetry.sqlite3"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE legacy_data(id INTEGER PRIMARY KEY, value TEXT)")
        db.execute("INSERT INTO legacy_data(value) VALUES('preserved')")

    telemetry = TelemetryStore(str(database), enabled=True)
    placed = now_ms() - 1_000
    telemetry.start_order_revision(
        revision_key="create-1", action="create", side="yes", client_order_id="client-1",
        order_id=None, placed_at_ms=placed, size_units=200, price_units=4_000,
        book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
    )
    telemetry.accept_order_revision(revision_key="create-1", order_id="order-1", client_order_id="client-1")
    telemetry.start_order_revision(
        revision_key="amend-1", action="amend", side="yes", client_order_id="client-2",
        order_id="order-1", placed_at_ms=placed + 100, size_units=100, price_units=4_100,
        book_bid_units=4_000, book_ask_units=4_200, book_mid_units=4_100,
    )
    telemetry.accept_order_revision(
        revision_key="amend-1", order_id="order-2", client_order_id="client-2",
        previous_order_id="order-1", amended=True,
    )
    telemetry.start_order_revision(
        revision_key="reject-1", action="amend", side="yes", client_order_id="client-3",
        order_id="order-2", placed_at_ms=placed + 200, size_units=100, price_units=4_200,
        book_bid_units=4_000, book_ask_units=4_200, book_mid_units=4_100,
    )
    telemetry.reject_order_revision("reject-1", "post only would cross")

    with sqlite3.connect(database) as db:
        assert db.execute("SELECT value FROM legacy_data").fetchone()[0] == "preserved"
        states = dict(db.execute("SELECT revision_key,ended_state FROM order_revisions"))
    assert states == {"create-1": "Amended", "amend-1": "Resting", "reject-1": "Rejected"}


def test_market_and_fill_calculations_use_persisted_fill_time_marks(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    telemetry, _ = telemetry_for_run(store, run)
    placed = now_ms() - 2_000
    telemetry.start_order_revision(
        revision_key="yes-revision", action="create", side="yes", client_order_id="yes-client",
        order_id="yes-order", placed_at_ms=placed, size_units=200, price_units=4_000,
        book_bid_units=3_500, book_ask_units=4_500, book_mid_units=4_000,
    )
    telemetry.accept_order_revision(revision_key="yes-revision", order_id="yes-order", client_order_id="yes-client")
    telemetry.start_order_revision(
        revision_key="no-revision", action="create", side="no", client_order_id="no-client",
        order_id="no-order", placed_at_ms=placed, size_units=100, price_units=5_000,
        book_bid_units=4_500, book_ask_units=5_500, book_mid_units=5_000,
    )
    telemetry.accept_order_revision(revision_key="no-revision", order_id="no-order", client_order_id="no-client")
    telemetry.record_fill(
        fill_key="yes-fill", ts_ms=placed + 500, ticker="TEST-1", side="yes", trade_id="yes-trade",
        order_id="yes-order", price_units=4_000, size_units=200, fee_units=100, is_taker=False,
        inventory_before_units=0, inventory_after_units=200, fair_yes_before_units=4_000,
        best_yes_bid_units=3_500, best_no_bid_units=5_500, queue_ahead_units=0,
    )
    telemetry.record_fill(
        fill_key="no-fill", ts_ms=placed + 700, ticker="TEST-1", side="no", trade_id="no-trade",
        order_id="no-order", price_units=5_000, size_units=100, fee_units=50, is_taker=False,
        inventory_before_units=200, inventory_after_units=100, fair_yes_before_units=5_000,
        best_yes_bid_units=4_500, best_no_bid_units=4_500, queue_ahead_units=0,
    )

    markets = store.run_markets(run["id"])
    assert len(markets["items"]) == 1
    market = markets["items"][0]
    assert market["side"] == "BOTH"
    assert market["yesAverageCostPriceUnits"] == 4_000
    assert market["noAverageCostPriceUnits"] == 5_000
    assert market["totalCostUnits"] == 13_150
    assert market["realizedPnlUnits"] == 850
    assert market["realizedReturnBps"] == 646

    activity = store.run_market_activity(run["id"], "TEST-1", fill_limit=1, order_limit=1)
    assert activity["fills"]["truncated"] is True
    assert activity["orders"]["truncated"] is True
    latest_fill = activity["fills"]["items"][0]
    assert latest_fill["timeToFillMs"] == 700
    assert latest_fill["liquidationValueUnits"] == 4_500
    assert latest_fill["unrealizedValueUnits"] == 5_000
    assert latest_fill["totalPaidUnits"] == 5_050
    assert latest_fill["matchedContractsUnits"] == 100
    assert latest_fill["openContractsUnits"] == 0
    assert latest_fill["realizedPnlUnits"] == 900
    assert latest_fill["unrealizedPnlUnits"] == 0
    assert latest_fill["fillPnlUnits"] == 900

    all_activity = store.run_market_activity(run["id"], "TEST-1", fill_limit=2, order_limit=1)
    opening_fill = all_activity["fills"]["items"][1]
    assert opening_fill["matchedContractsUnits"] == 100
    assert opening_fill["openContractsUnits"] == 100
    assert opening_fill["realizedPnlUnits"] == 0
    assert opening_fill["unrealizedPnlUnits"] == 450
    assert opening_fill["fillPnlUnits"] == 450


def test_open_fill_uses_latest_market_state_same_side_bid(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    telemetry, path = telemetry_for_run(store, run)
    timestamp = now_ms() - 1_000
    record_fill(
        telemetry, key="open-yes", timestamp=timestamp, side="yes", price=4_000,
        size=100, fee=100, yes_bid=3_500, no_bid=5_500,
    )
    with sqlite3.connect(path) as db:
        db.execute(
            """INSERT INTO market_state(
                   ts_ms,ticker,source,best_yes_bid_units,best_no_bid_units
               ) VALUES(?,?,?,?,?)""",
            (timestamp + 500, "TEST-1", "snapshot", 5_000, 4_500),
        )

    activity = store.run_market_activity(run["id"], "TEST-1", fill_limit=25, order_limit=25)
    fill = activity["fills"]["items"][0]
    assert fill["realizedPnlUnits"] == 0
    assert fill["unrealizedPnlUnits"] == 900
    assert fill["fillPnlUnits"] == 900
    assert fill["matchedContractsUnits"] == 0
    assert fill["openContractsUnits"] == 100
    # Existing gross field retains its fill-time snapshot semantics.
    assert fill["liquidationValueUnits"] == 3_500


def test_fifo_partial_exit_allocates_fees_once_and_uses_history_outside_cap(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    telemetry, _ = telemetry_for_run(store, run)
    timestamp = now_ms() - 1_000
    # Identical timestamps intentionally verify id insertion order breaks ties.
    record_fill(
        telemetry, key="yes-oldest", timestamp=timestamp, side="yes", price=3_000,
        size=100, fee=30, yes_bid=6_000, no_bid=4_500,
    )
    record_fill(
        telemetry, key="yes-second", timestamp=timestamp, side="yes", price=5_000,
        size=100, fee=50, yes_bid=6_000, no_bid=4_500,
    )
    record_fill(
        telemetry, key="no-close-and-open", timestamp=timestamp + 1, side="no", price=4_000,
        size=250, fee=100, yes_bid=6_000, no_bid=4_500,
    )

    capped = store.run_market_activity(run["id"], "TEST-1", fill_limit=1, order_limit=25)
    closing = capped["fills"]["items"][0]
    assert capped["fills"]["truncated"] is True
    assert closing["matchedContractsUnits"] == 200
    assert closing["openContractsUnits"] == 50
    assert closing["realizedPnlUnits"] == 3_840
    assert closing["unrealizedPnlUnits"] == 230
    assert closing["fillPnlUnits"] == 4_070

    all_rows = store.run_market_activity(run["id"], "TEST-1", fill_limit=3, order_limit=25)["fills"]["items"]
    opening_rows = all_rows[1:]
    assert all(row["matchedContractsUnits"] == 100 for row in opening_rows)
    assert all(row["openContractsUnits"] == 0 for row in opening_rows)
    assert all(row["fillPnlUnits"] == 0 for row in opening_rows)
    assert sum(row["fillPnlUnits"] or 0 for row in all_rows) == 4_070


def test_fifo_fill_pnl_preserves_partial_results_when_data_is_missing():
    rows = [
        {"id": 1, "side": "yes", "size_units": 100, "price_units": 4_000, "fee_units": None},
        {"id": 2, "side": "no", "size_units": 150, "price_units": 5_000, "fee_units": 50},
    ]
    result, warnings = SessionStore._fill_pnl_breakdown(
        rows, latest_bids={"yes": 4_500, "no": 4_500}
    )
    assert result[2]["matchedContractsUnits"] == 100
    assert result[2]["openContractsUnits"] == 50
    assert result[2]["realizedPnlUnits"] is None
    assert result[2]["unrealizedPnlUnits"] == -267
    assert result[2]["fillPnlUnits"] is None
    assert result[1]["unrealizedPnlUnits"] == 0
    assert any("Realized fill P&L" in warning for warning in warnings)


def test_missing_fill_values_stay_unavailable_and_finalize_unknown(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    telemetry, path = telemetry_for_run(store, run)
    placed = now_ms() - 500
    telemetry.start_order_revision(
        revision_key="open", action="create", side="yes", client_order_id="client",
        order_id="order", placed_at_ms=placed, size_units=100, price_units=4_000,
        book_bid_units=None, book_ask_units=None, book_mid_units=None,
    )
    telemetry.accept_order_revision(revision_key="open", order_id="order", client_order_id="client")
    telemetry.record_fill(
        fill_key="missing", ts_ms=placed + 100, ticker="TEST-1", side="yes", trade_id="trade",
        order_id="order", price_units=None, size_units=100, fee_units=None, is_taker=False,
        inventory_before_units=0, inventory_after_units=100, fair_yes_before_units=None,
        best_yes_bid_units=None, best_no_bid_units=None, queue_ahead_units=None,
    )
    activity = store.run_market_activity(run["id"], "TEST-1", fill_limit=25, order_limit=25)
    fill = activity["fills"]["items"][0]
    assert fill["totalPaidUnits"] is None
    assert fill["liquidationValueUnits"] is None
    assert fill["unrealizedValueUnits"] is None
    assert fill["realizedPnlUnits"] == 0
    assert fill["unrealizedPnlUnits"] is None
    assert fill["fillPnlUnits"] is None
    assert any("Unrealized fill P&L" in warning for warning in activity["warnings"])
    assert any("price or fee" in warning for warning in activity["warnings"])
    assert activity["market"]["totalCostUnits"] is None
    assert activity["market"]["realizedPnlUnits"] is None
    assert activity["warnings"]

    store.finish_run(run["id"], "stopped")
    with sqlite3.connect(path) as db:
        state, ended_at = db.execute("SELECT ended_state,ended_at_ms FROM order_revisions WHERE revision_key='open'").fetchone()
    assert state == "Unknown"
    assert ended_at is not None


def test_run_market_reader_rejects_ticker_and_artifact_path_traversal(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    telemetry_for_run(store, run)
    with pytest.raises(KeyError):
        store.run_market_activity(run["id"], "../TEST-1", fill_limit=25, order_limit=25)
    with store._connect() as db:
        db.execute("UPDATE runs SET artifact_path=? WHERE id=?", (str(tmp_path), run["id"]))
    with pytest.raises(KeyError):
        store.run_markets(run["id"])


def test_launcher_restart_finalizes_interrupted_resting_revisions(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.claim_run()
    telemetry, path = telemetry_for_run(store, run)
    telemetry.start_order_revision(
        revision_key="open-on-restart", action="create", side="yes", client_order_id="client",
        order_id="order", placed_at_ms=now_ms() - 100, size_units=100, price_units=4_000,
        book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
    )
    telemetry.accept_order_revision(revision_key="open-on-restart", order_id="order", client_order_id="client")

    replacement = store.claim_run()
    assert replacement["id"] != run["id"]
    assert store.get_run(run["id"])["status"] == "interrupted"
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT ended_state FROM order_revisions WHERE revision_key='open-on-restart'"
        ).fetchone()[0] == "Unknown"


def test_metric_samples_are_lightweight_but_current_run_keeps_market_counters(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    metrics = {
        "runtimeMs": 1234,
        "orders": 2,
        "fills": 1,
        "apiCalls": 4,
        "markets": {"TEST-1": {"orders": 2, "orderPlacementsAttempted": 1, "fills": 1}},
    }
    store.record_metrics(run["id"], metrics)
    with store._connect() as db:
        sample = json.loads(db.execute(
            "SELECT payload_json FROM metric_samples WHERE run_id=?", (run["id"],)
        ).fetchone()[0])
    assert "markets" not in sample
    assert store.get_run(run["id"], include_artifact_bytes=False)["metrics"]["markets"] == metrics["markets"]


def test_incremental_market_cache_reopens_only_changed_market(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    for ticker in ("TEST-1", "TEST-2"):
        telemetry, _ = telemetry_for_run(store, run, ticker)
        telemetry.start_order_revision(
            revision_key=f"{ticker}-order", action="create", side="yes", client_order_id=ticker,
            order_id=ticker, placed_at_ms=now_ms(), size_units=100, price_units=4_000,
            book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
        )
    store.record_metrics(run["id"], {
        "markets": {
            "TEST-1": {"orders": 1, "orderPlacementsAttempted": 1, "fills": 0},
            "TEST-2": {"orders": 1, "orderPlacementsAttempted": 1, "fills": 0},
        }
    }, sample=False)
    original = store._market_summary
    opened: list[str] = []

    def counted(path: Path, **kwargs):
        opened.append(kwargs["ticker"])
        return original(path, **kwargs)

    monkeypatch.setattr(store, "_market_summary", counted)
    assert len(store.run_markets(run["id"])["items"]) == 2
    assert opened == ["TEST-1", "TEST-2"]
    store.run_markets(run["id"])
    assert opened == ["TEST-1", "TEST-2"]

    store.record_metrics(run["id"], {
        "markets": {
            "TEST-1": {"orders": 2, "orderPlacementsAttempted": 2, "fills": 0},
            "TEST-2": {"orders": 1, "orderPlacementsAttempted": 1, "fills": 0},
        }
    }, sample=False)
    store.run_markets(run["id"])
    assert opened == ["TEST-1", "TEST-2", "TEST-1"]


def test_current_run_lists_admitted_market_before_first_order(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    artifact = Path(run["artifactPath"])
    path = artifact / "shards" / "worker-00" / "telemetry.sqlite3"
    telemetry = TelemetryStore(str(path), enabled=True, shard_mode=True)
    telemetry.record_market_metadata(
        ticker="TEST-1", title="Waiting for first quote", series_ticker="TEST",
        event_ticker="TEST-EVENT",
    )
    telemetry.flush()
    (artifact / "fleet_manifest.json").write_text(json.dumps({
        "tickerToShard": {"TEST-1": "worker-00"},
        "workers": {"worker-00": ["TEST-1"]},
    }))
    store.record_metrics(run["id"], {
        "markets": {
            "TEST-1": {"orders": 0, "orderPlacementsAttempted": 0, "fills": 0},
        },
    }, sample=False)

    response = store.run_markets(run["id"])
    assert len(response["items"]) == 1
    assert response["items"][0]["ticker"] == "TEST-1"
    assert response["items"][0]["orderCount"] == 0
    assert response["items"][0]["fillCount"] == 0


def test_current_run_reads_per_venue_shard_artifacts(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    artifact = Path(run["artifactPath"])
    venue_root = artifact / "kalshi"
    shard_path = venue_root / "shards" / "worker-00" / "telemetry.sqlite3"
    telemetry = TelemetryStore(str(shard_path), enabled=True, shard_mode=True)
    telemetry.record_market_metadata(
        ticker="VENUE-1", title="Per venue market", series_ticker="TEST",
        event_ticker="TEST-EVENT",
    )
    telemetry.start_order_revision(
        revision_key="venue-order", action="create", side="yes",
        client_order_id="venue-client", order_id="venue-order",
        placed_at_ms=now_ms(), size_units=100, price_units=4_000,
        book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
    )
    telemetry.flush()
    venue_root.mkdir(parents=True, exist_ok=True)
    (venue_root / "fleet_manifest.json").write_text(json.dumps({
        "tickerToShard": {"VENUE-1": "worker-00"},
    }))
    store.record_metrics(run["id"], {
        "markets": {"VENUE-1": {"orders": 1, "orderPlacementsAttempted": 1, "fills": 0}},
    }, sample=False)

    response = store.run_markets(run["id"])
    assert response["source"]["available"] is True
    assert response["items"][0]["ticker"] == "VENUE-1"
    assert response["items"][0]["orderCount"] == 1


def test_removed_market_resolves_from_immutable_settings_after_manifest_refresh(tmp_path: Path):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    artifact = Path(run["artifactPath"])
    shard_path = artifact / "shards" / "worker-02" / "telemetry.sqlite3"
    telemetry = TelemetryStore(str(shard_path), enabled=True, shard_mode=True)
    telemetry.record_market_metadata(
        ticker="REMOVED", title="Removed after refresh", series_ticker="TEST",
        event_ticker="TEST-EVENT",
    )
    telemetry.start_order_revision(
        revision_key="removed-order", action="create", side="no",
        client_order_id="mm:no:removed", order_id="order-removed",
        placed_at_ms=1_000, size_units=100, price_units=4_000,
        book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
    )
    telemetry.flush()
    market_dir = artifact / "markets" / "REMOVED"
    market_dir.mkdir(parents=True)
    (market_dir / "settings.json").write_text(json.dumps({
        "telemetry_sqlite_path": str(shard_path),
    }))
    # Simulate a later screener generation overwriting the legacy manifest and
    # retaining only currently active markets.
    (artifact / "fleet_manifest.json").write_text(json.dumps({
        "tickerToShard": {"ACTIVE": "worker-00"},
    }))
    store.record_metrics(run["id"], {
        "markets": {"REMOVED": {"orders": 1, "orderPlacementsAttempted": 1, "fills": 0}},
    }, sample=False)

    response = store.run_markets(run["id"])
    assert response["source"]["available"] is True
    assert response["items"][0]["ticker"] == "REMOVED"
    assert response["items"][0]["orderCount"] == 1


def test_market_cache_retains_last_summary_on_read_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = SessionStore(tmp_path / "sessions")
    run = store.prepare_run()
    telemetry, _ = telemetry_for_run(store, run)
    telemetry.start_order_revision(
        revision_key="one", action="create", side="yes", client_order_id="one", order_id="one",
        placed_at_ms=now_ms(), size_units=100, price_units=4_000,
        book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
    )
    store.record_metrics(run["id"], {"markets": {"TEST-1": {"orders": 1}}}, sample=False)
    assert store.run_markets(run["id"])["items"][0]["ticker"] == "TEST-1"
    store.record_metrics(run["id"], {"markets": {"TEST-1": {"orders": 2}}}, sample=False)

    def broken(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "_market_summary", broken)
    response = store.run_markets(run["id"])
    assert response["items"][0]["ticker"] == "TEST-1"
    assert response["source"]["stale"] is True
    assert "database is locked" in response["source"]["error"]


def test_telemetry_storage_failure_disables_writer_without_raising(tmp_path: Path):
    database_directory = tmp_path / "not-a-database"
    database_directory.mkdir()
    telemetry = TelemetryStore(str(database_directory), enabled=True)
    telemetry.record_market_metadata(
        ticker="TEST-1", title="Test", series_ticker="TEST", event_ticker="EVENT"
    )
    health = telemetry.health_snapshot()
    assert health["available"] is False
    assert health["stale"] is True
    assert health["error"]
