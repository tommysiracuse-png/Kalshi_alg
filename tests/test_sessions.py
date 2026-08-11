import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from session_config import default_session_configuration, validate_session_configuration
from session_store import RunMetricsAccumulator, SessionConflictError, SessionStore
from top_of_book_bot import build_settings_from_args


class TrackingConnection(sqlite3.Connection):
    closed = False

    def close(self) -> None:
        self.closed = True
        super().close()


def test_session_store_closes_connections_on_success_and_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    original_connect = sqlite3.connect
    connections: list[TrackingConnection] = []

    def connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs, factory=TrackingConnection)
        connections.append(connection)
        return connection

    monkeypatch.setattr("session_store.sqlite3.connect", connect)
    store = SessionStore(tmp_path / "session_data")
    original_description = store.list_sessions()[0]["description"]
    with pytest.raises(RuntimeError, match="rollback"):
        with store._connect() as connection:
            connection.execute("UPDATE sessions SET description='not committed'")
            raise RuntimeError("rollback")

    assert store.list_sessions()[0]["description"] == original_description
    assert connections
    assert all(connection.closed for connection in connections)


def test_configuration_round_trip_and_unknown_rejection():
    config = default_session_configuration()
    config["launcher"]["fixedTicker"] = "TEST-MARKET"
    config["launcher"]["yesBudgetCents"] = 321
    config["watchdog"]["flattenRetries"] = 4
    config["bot"]["markout_horizons_seconds"] = [1, 15, 60]
    normalized = validate_session_configuration(config)
    assert normalized["launcher"]["fixedTicker"] == "TEST-MARKET"
    assert len(normalized["bot"]) >= 75
    config["bot"]["not_a_setting"] = 1
    with pytest.raises(ValueError, match="unknown configuration"):
        validate_session_configuration(config)


def test_bot_settings_file_applies_full_payload_and_cli_override(tmp_path: Path):
    config = default_session_configuration()
    payload = dict(config["bot"])
    payload.update(
        market_ticker="FILE-TICKER", yes_order_budget_cents=222, no_order_budget_cents=333,
        watchdog_state_file="state.json", watchdog_refresh_seconds=9,
        watchdog_extreme_stale_seconds=99, watchdog_flatten_retries=5,
        telemetry_sqlite_path="telemetry.sqlite3", pnl_tracker_path="fills.jsonl",
    )
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(payload))
    args = Namespace(
        settings_file=str(path), ticker="CLI-TICKER", yes_budget_cents=None, no_budget_cents=None,
        maximum_projected_contracts_per_line=None, watchdog_state_file="",
        watchdog_refresh_seconds=None, watchdog_extreme_stale_seconds=None, watchdog_flatten_retries=None,
    )
    settings = build_settings_from_args(args)
    assert settings.market_ticker == "CLI-TICKER"
    assert settings.yes_order_budget_cents == 222
    assert settings.watchdog_flatten_retries == 5
    assert settings.pnl_tracker_path == "fills.jsonl"


def test_session_lifecycle_archive_and_weighted_metrics(tmp_path: Path):
    store = SessionStore(tmp_path / "session_data")
    default = store.list_sessions()[0]
    other = store.create_session({"name": "Other", "configuration": default["configuration"]})
    store.select_session(other["id"])
    run = store.prepare_run()
    assert run["configuration"] == other["configuration"]
    with pytest.raises(SessionConflictError, match="locked"):
        store.select_session(default["id"])
    claimed = store.claim_run(run["id"])
    assert claimed["status"] == "starting"
    store.mark_running(run["id"])
    store.finish_run(run["id"], "stopped", metrics={
        "runtimeMs": 120_000, "orders": 12, "fills": 4, "apiCalls": 30,
        "apiErrors": 1, "totalCents": 25.5, "pnlComplete": True,
        "apiByComponent": {"bots": 20, "screener": 10},
    })
    summary = store.metrics(session_id=other["id"])["summary"]
    assert summary["ordersPerMinute"] == 6
    assert summary["fillsPerMinute"] == 2
    assert summary["totalCents"] == 25.5
    archived = store.archive_session(other["id"])
    assert archived["archivedAt"] is not None
    assert store.list_sessions()[0]["id"] == default["id"]


def test_metrics_accumulator_keeps_completed_process_counters():
    recorder = RunMetricsAccumulator(1)
    first = {
        "clients": [{"marketId": "A", "pid": 1, "runtime": {"startedAtMs": 10},
            "fills": {"count": 2}, "orderActivity": {"byAction": {"create": {"attempts": 3, "successes": 2, "errors": 1}}},
            "apiActivity": {"rest": {"total": 5, "errors": 1}},
            "pnl": {"realizedCents": 2, "unrealizedCents": 1, "totalCents": 3, "sessionPositionUnits": 0}}],
        "screener": {"apiActivity": {"rest": {"total": 4}}}, "portfolio": {},
    }
    assert recorder.observe(first)["orders"] == 3
    second = {"clients": [{**first["clients"][0], "pid": 2, "runtime": {"startedAtMs": 20}, "fills": {"count": 1}}], "screener": {}, "portfolio": {}}
    result = recorder.observe(second)
    assert result["fills"] == 3
    assert result["apiByComponent"]["screener"] == 4
