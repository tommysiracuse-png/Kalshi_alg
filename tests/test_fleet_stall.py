"""Broker-stall resilience: bounded broker calls, stall detection -> broker
restart -> reconcile re-issue, slow-reconcile tolerance, shutdown fallback."""

from __future__ import annotations

import asyncio
import os
import queue
import threading
import time
from pathlib import Path

import pytest

from bot_manager import BotManagerConfig
from fleet_models import WorkerControlAck, WorkerHeartbeat
from fleet_runtime.execution import (
    BrokerRequest,
    BrokerRpcClient,
    ExecutionBrokerProcess,
)
from fleet_runtime.manager import ManagedWorker, ShardedBotManager
from fleet_runtime.worker import add_retry_delay_seconds
import lip_launcher


# ----------------------------------------------------------------------
# helpers


def manager_config(tmp_path: Path, *, dry_run: bool = True) -> BotManagerConfig:
    return BotManagerConfig(
        bot_script_path=tmp_path / "V1.py",
        logs_directory=tmp_path / "logs",
        runtime_dir=tmp_path / "runtime",
        watchdog_state_dir=tmp_path / "watchdog",
        watchdog_disable_file=tmp_path / "disabled.json",
        watchdog_runner_script_path=None,
        watchdog_profiler_script_path=None,
        dry_run=dry_run,
        shutdown_cleanup_delay_seconds=0,
    )


class FakeProcess:
    def __init__(self, *, running: bool = True, pid: int = 100) -> None:
        self.running = running
        self.pid = pid
        self.started = False
        self.terminated = False

    def is_alive(self) -> bool:
        return self.running

    def start(self) -> None:
        self.started = True
        self.running = True

    def join(self, timeout=None) -> None:
        return None

    def terminate(self) -> None:
        self.terminated = True
        self.running = False

    def kill(self) -> None:
        self.running = False


class RecordingQueue:
    def __init__(self) -> None:
        self.items: list[dict] = []

    def put(self, value) -> None:
        self.items.append(value)

    def actions(self) -> list[str]:
        return [str(item.get("action")) for item in self.items]


class AutoAckQueue(RecordingQueue):
    def __init__(self, worker_id: str, ack_queue, process: FakeProcess) -> None:
        super().__init__()
        self.worker_id = worker_id
        self.ack_queue = ack_queue
        self.process = process

    def put(self, value) -> None:
        super().put(value)
        request_id = str(value.get("request_id") or "")
        if request_id:
            self.ack_queue.put(WorkerControlAck(
                self.worker_id, request_id, str(value["action"]), True, int(time.time() * 1000)
            ))
        if value.get("action") == "stop":
            self.process.running = False


class FakeAdmin:
    def __init__(self, *, fail: set[str] | None = None) -> None:
        self.calls: list[tuple[str, object, float]] = []
        self.fail = set(fail or ())

    async def call(self, operation, payload=None, timeout=60.0):
        self.calls.append((operation, payload, timeout))
        if operation in self.fail:
            raise TimeoutError(f"broker {operation} timed out")
        return 0


class FakeVenue:
    """Direct venue client double: counts calls, can block one operation."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.block = threading.Event()
        self.block.set()
        self.blocking_markets: set[str] = set()

    def get_market(self, market_id: str):
        self.calls.append(("get_market", (market_id,)))
        if market_id in self.blocking_markets:
            self.block.wait(timeout=5.0)
        return {"market": market_id}

    def get_incentive_programs(self, *, status="active", incentive_type="all", limit=10_000):
        self.calls.append(("get_incentive_programs", (status, incentive_type, limit)))
        return ["program"]

    def get_series_fee_changes(self, series_id: str, *, show_historical: bool = False):
        self.calls.append(("get_series_fee_changes", (series_id, show_historical)))
        return []

    def get_resting_orders(self, market_id: str):
        self.calls.append(("get_resting_orders", (market_id,)))
        return []

    def cancel_order(self, *, order_id: str):
        self.calls.append(("cancel_order", (order_id,)))
        return {"order_id": order_id}

    def get_positions(self, market_id: str):
        self.calls.append(("get_positions", (market_id,)))
        return []

    venue_name = "kalshi"
    environment_name = "prod"
    dry_run = False
    rate_limit_backoff_seconds = 1.0


def make_broker(**overrides) -> ExecutionBrokerProcess:
    from adaptors.kalshi import KalshiClientConfig

    kwargs = dict(
        client_config=KalshiClientConfig(public_only=True),
        request_queue=queue.Queue(),
        response_queues={"worker-00": queue.Queue()},
        status_queue=queue.Queue(),
    )
    kwargs.update(overrides)
    return ExecutionBrokerProcess(**kwargs)


def request(operation: str, payload: dict | None = None, *, age_ms: int = 0) -> BrokerRequest:
    return BrokerRequest(
        f"req-{operation}-{time.monotonic_ns()}", "worker-00", operation, dict(payload or {}),
        int(time.time() * 1000) - age_ms,
    )


# ----------------------------------------------------------------------
# (1) bounded per-request execution in the broker


def test_stuck_venue_call_times_out_and_does_not_block_the_next_request():
    broker = make_broker(execute_timeout_seconds=0.1)
    venue = FakeVenue()
    venue.block.clear()
    venue.blocking_markets.add("STUCK")
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="timed out executing get_market"):
            broker._execute_bounded(venue, request("get_market", {"market_id": "STUCK"}))
        assert time.monotonic() - started < 2.0
        # The stuck call is still parked on its thread; the next request is served.
        result = broker._execute_bounded(venue, request("get_market", {"market_id": "OTHER"}))
        assert result == {"market": "OTHER"}
    finally:
        venue.block.set()
        broker._get_executor().shutdown(wait=True)


def test_stale_requests_are_dropped_except_cancellations():
    broker = make_broker(request_ttl_seconds=30.0)
    assert broker.is_stale(request("get_market", {"market_id": "A"}, age_ms=31_000))
    assert not broker.is_stale(request("get_market", {"market_id": "A"}, age_ms=5_000))
    assert not broker.is_stale(request("cancel_order", {"order_id": "o"}, age_ms=120_000))
    assert not broker.is_stale(request("cancel_all", age_ms=120_000))
    assert not broker.is_stale(request("quiesce_all", age_ms=120_000))


def test_broker_caches_incentive_catalog_and_short_lived_market_reads():
    broker = make_broker()
    venue = FakeVenue()
    for _ in range(3):
        broker._execute(venue, request("get_incentive_programs", {"status": "active", "incentive_type": "all", "limit": 10_000}))
    assert sum(1 for name, _ in venue.calls if name == "get_incentive_programs") == 1
    broker._execute(venue, request("get_series_fee_changes", {"series_id": "KXRAIN"}))
    broker._execute(venue, request("get_series_fee_changes", {"series_id": "KXRAIN"}))
    assert sum(1 for name, _ in venue.calls if name == "get_series_fee_changes") == 1
    broker._execute(venue, request("get_market", {"market_id": "A"}))
    broker._execute(venue, request("get_market", {"market_id": "A"}))
    broker._execute(venue, request("get_market", {"market_id": "B"}))
    assert [args for name, args in venue.calls if name == "get_market"] == [("A",), ("B",)]
    assert broker._execute(venue, request("ping")) == "pong"


def test_rest_client_config_carries_bounded_connect_and_read_timeouts():
    from adaptors.kalshi import KalshiApiClient, KalshiClientConfig

    client = KalshiApiClient(KalshiClientConfig(public_only=True, rest_timeout_seconds=7, rest_connect_timeout_seconds=3))
    assert client.http_client.request_timeout == (3.0, 7.0)
    with pytest.raises(ValueError):
        KalshiClientConfig(public_only=True, rest_timeout_seconds=0)


# ----------------------------------------------------------------------
# (4) worker-side shutdown fallback when the broker is dead


def test_rpc_client_shutdown_probe_falls_back_to_direct_venue_when_broker_is_silent():
    venue = FakeVenue()
    client = BrokerRpcClient(venue, queue.Queue(), queue.Queue(), "worker-00")
    try:
        assert client.prepare_for_shutdown(probe_timeout=0.05) is False
        assert client.direct_only
        started = time.monotonic()
        assert client.get_resting_orders("MKT") == []
        assert client.cancel_order(order_id="o1") == {"order_id": "o1"}
        assert time.monotonic() - started < 1.0
        assert ("get_resting_orders", ("MKT",)) in venue.calls
        assert ("cancel_order", ("o1",)) in venue.calls
    finally:
        client.close_dispatcher()


def test_rpc_client_shutdown_bounds_waits_and_falls_back_when_a_live_broker_stops_answering():
    venue = FakeVenue()
    requests: "queue.Queue[BrokerRequest]" = queue.Queue()
    responses: "queue.Queue[dict]" = queue.Queue()
    client = BrokerRpcClient(venue, requests, responses, "worker-00")

    def answer_ping_only() -> None:
        while True:
            item = requests.get(timeout=2.0)
            if item.operation == "ping":
                responses.put({"request_id": item.request_id, "ok": True, "result": "pong"})
            elif item.operation == "stop-thread":
                return
    thread = threading.Thread(target=answer_ping_only, daemon=True)
    thread.start()
    try:
        assert client.prepare_for_shutdown(probe_timeout=1.0, rpc_timeout=0.05) is True
        assert not client.direct_only
        assert client.rpc_timeout_seconds == 0.05
        started = time.monotonic()
        assert client.get_resting_orders("MKT") == []   # broker silent -> direct
        assert time.monotonic() - started < 1.0
        assert ("get_resting_orders", ("MKT",)) in venue.calls
    finally:
        requests.put(BrokerRequest("x", "worker-00", "stop-thread", {}, 0))
        client.close_dispatcher()


def test_add_retry_backoff_is_bounded():
    assert [add_retry_delay_seconds(n) for n in (1, 2, 3, 4, 5, 9)] == [30, 60, 120, 240, 300, 300]


# ----------------------------------------------------------------------
# (2) manager: stall detection -> broker restart -> reconcile re-issue


def stalled_manager(tmp_path: Path) -> tuple[ShardedBotManager, FakeProcess, RecordingQueue, list[FakeProcess]]:
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._started = True
    broker = FakeProcess(pid=500)
    manager._broker = broker
    manager._broker_status_queue = queue.Queue()
    manager._heartbeat_queue = queue.Queue()
    manager._control_ack_queue = queue.Queue()
    manager._admin = FakeAdmin()
    replacements: list[FakeProcess] = []

    def make_broker():
        replacement = FakeProcess(pid=600 + len(replacements))
        replacements.append(replacement)
        return replacement

    manager._make_broker = make_broker
    now = int(time.time() * 1000)
    manager._broker_started_at_ms = now - 600_000
    commands = RecordingQueue()
    worker = FakeProcess(pid=200)
    manager._workers = {
        "worker-00": ManagedWorker(
            "worker-00", worker, commands, queue.Queue(),
            heartbeat=WorkerHeartbeat("worker-00", ("MKT",), {}, 0, 0, 0, now),
            phase="healthy", last_liveness_at_ms=now,
        )
    }
    from fleet_models import ScreenerPick
    pick = ScreenerPick(market_id="MKT", title="Market", yes_budget_cents=100, no_budget_cents=100, ranking={})
    manager._desired = {"MKT": pick}
    manager._assignments = {"worker-00": ("MKT",)}
    return manager, broker, commands, replacements


def test_status_snapshot_publishes_the_worker_risk_reason_per_bot(tmp_path: Path):
    from fleet_models import MarketHealth

    manager, _broker, _commands, _replacements = stalled_manager(tmp_path)
    now = int(time.time() * 1000)
    health = {
        "MKT": MarketHealth(
            book_available=True, book_age_ms=500, decision_age_ms=100,
            risk_mode="reduction_only", risk_age_ms=1_000, risk_reason="elevated_price_move",
            last_quote_at_ms=now - 100, last_order_create_at_ms=now - 200,
            last_fill_at_ms=now - 300, price_units=5_100, price_source="ticker", price_at_ms=now,
        ),
    }
    manager._workers["worker-00"].heartbeat = WorkerHeartbeat(
        "worker-00", ("MKT",), health, 0, 0, 0, now, "kalshi",
        {"startedAtMs": now - 1_000, "rest": {"total": 4, "successes": 4, "errors": 0}},
    )
    snapshot = manager.status_snapshot()
    bot = snapshot["bots"][0]
    assert (bot["ticker"], bot["watchdogMode"], bot["watchdogReason"]) == ("MKT", "reduction_only", "elevated_price_move")
    assert snapshot["clients"][0]["watchdog"]["reason"] == "elevated_price_move"
    assert snapshot["counts"]["watchdogModes"] == {"reduction_only": 1}
    assert snapshot["counts"]["activeBots"] == 1
    assert snapshot["workers"][0]["watchdog"] == {"mode": "reduction_only", "counts": {"reduction_only": 1}}
    assert snapshot["workers"][0]["marketIds"] == ["MKT"]
    assert snapshot["apiActivity"]["rest"]["total"] == 4
    client = snapshot["clients"][0]
    assert client["market"]["lastQuoteAtMs"] == now - 100
    assert client["orderActivity"]["lastCreateAtMs"] == now - 200
    assert client["fills"]["lastFillAtMs"] == now - 300

    # A heartbeat from a worker running older code (no reason) and a market
    # still waiting for its actor keep the previous shape.
    legacy = {"MKT": MarketHealth(True, 500, 100, "normal", 1_000)}
    manager._workers["worker-00"].heartbeat = WorkerHeartbeat("worker-00", ("MKT",), legacy, 0, 0, 0, now)
    assert manager.status_snapshot()["bots"][0]["watchdogReason"] is None
    pending = {"MKT": MarketHealth(False, None, None, "startup", None, error="actor start pending")}
    manager._workers["worker-00"].heartbeat = WorkerHeartbeat("worker-00", ("MKT",), pending, 0, 0, 0, now)
    assert manager.status_snapshot()["bots"][0]["watchdogReason"] == "actor start pending"


def test_broker_reporting_sustained_venue_timeouts_is_restarted_and_workers_reconcile(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 120_000,
            "consecutive_timeouts": 5, "pending": 40, "in_flight": 0, "dropped_stale": 12,
        })
        await manager.monitor_once()

        assert broker.terminated and not broker.is_alive()
        assert len(replacements) == 1 and replacements[0].started
        assert manager._broker is replacements[0]
        assert "reduction-only" in manager._capacity_error
        # Quoting is disabled before the old broker dies and the (closed) gate
        # is rebroadcast afterwards; the live assignment is re-issued only
        # once the replacement broker reports in.
        assert set(commands.actions()) == {"enable_quoting"}
        assert all(item["enabled"] is False for item in commands.items if item["action"] == "enable_quoting")
        events = []
        while not manager.events.empty():
            events.append(await manager.events.get())
        assert [event.event_type for event in events][0] == "broker_restarted"
        assert "consecutive venue timeouts" in str(events[0].detail["reason"])
        snapshot = manager.status_snapshot()["broker"]
        assert snapshot["restarts"] == 1 and snapshot["pid"] == 600

        # A second pass right away must not bounce the fresh broker again;
        # its first heartbeat triggers the reconcile.
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now,
            "consecutive_timeouts": 0, "pending": 0, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert len(replacements) == 1
        assert commands.actions().count("reconcile") == 1
        assert commands.items[-1]["action"] == "reconcile"
        assert [pick.market_id for pick in commands.items[-1]["picks"]] == ["MKT"]

    asyncio.run(scenario())


def test_silent_broker_heartbeat_is_restarted_after_stall_window(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        manager.broker_stall_seconds = 0.5
        manager.broker_startup_grace_seconds = 0.5
        now = int(time.time() * 1000)
        manager._broker_last_seen_ms = now - 2_000
        await manager.monitor_once()
        assert not broker.is_alive() and len(replacements) == 1
        assert manager.status_snapshot()["broker"]["running"] is True

    asyncio.run(scenario())


def test_stall_flag_clears_when_broker_recovers_while_restart_is_rate_limited(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 120_000,
            "consecutive_timeouts": 5, "pending": 40, "in_flight": 0, "dropped_stale": 12,
        })
        await manager.monitor_once()
        assert len(replacements) == 1 and not manager._broker_stalled

        # The replacement stalls too, but a second restart is rate-limited:
        # the flag is raised and must not stick once the broker reports healthy.
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 120_000,
            "consecutive_timeouts": 5, "pending": 40, "in_flight": 0, "dropped_stale": 12,
        })
        await manager.monitor_once()
        assert len(replacements) == 1
        assert manager._broker_stalled and not manager._broker_usable()
        assert manager.status_snapshot()["broker"]["stalled"] is True

        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 500,
            "consecutive_timeouts": 0, "pending": 3, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert len(replacements) == 1
        assert not manager._broker_stalled and manager._broker_stall_reason == ""
        assert manager._broker_usable()
        assert manager.status_snapshot()["broker"]["stalled"] is False

    asyncio.run(scenario())


def test_broker_instance_pickled_by_an_older_parent_still_has_its_tunables():
    # On Windows the parent pickles the ExecutionBrokerProcess to spawn it and
    # the child unpickles it against the module on disk. A parent built from
    # an older __init__ ships an instance without the newer attributes.
    broker = make_broker()
    for name in (
        "execute_timeout_seconds", "cleanup_timeout_seconds", "request_ttl_seconds",
        "heartbeat_seconds", "refresh_limits_seconds", "cleanup_attempts", "cleanup_delay_seconds",
    ):
        del broker.__dict__[name]
    assert broker.execute_timeout_seconds == ExecutionBrokerProcess.EXECUTE_TIMEOUT_SECONDS
    assert broker.cleanup_timeout_seconds == ExecutionBrokerProcess.CLEANUP_TIMEOUT_SECONDS
    assert broker.heartbeat_seconds == ExecutionBrokerProcess.HEARTBEAT_SECONDS
    assert broker.request_ttl_seconds == 30.0
    assert (broker.refresh_limits_seconds, broker.cleanup_attempts, broker.cleanup_delay_seconds) == (60.0, 8, 0.5)
    assert broker.is_stale(request("get_market", age_ms=31_000))
    assert not broker.is_stale(request("get_market", age_ms=1_000))


def test_healthy_broker_heartbeat_is_left_alone(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 500,
            "consecutive_timeouts": 0, "pending": 3, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert broker.is_alive() and replacements == []
        assert commands.items == []
        assert manager.broker_stall_reason() == ""

    asyncio.run(scenario())


def test_dead_broker_is_restarted_immediately(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        broker.running = False
        await manager.monitor_once()
        assert len(replacements) == 1
        assert "exited" in manager._capacity_error

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# (1, coordinator) slow reconcile on a live worker must not trigger recovery


def reconcile_manager(tmp_path: Path, *, heartbeat_age_ms: int, sent_age_ms: int) -> tuple[ShardedBotManager, list[str]]:
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._started = True
    manager._broker = FakeProcess(pid=500)
    manager._broker_status_queue = queue.Queue()
    manager._heartbeat_queue = queue.Queue()
    manager._control_ack_queue = queue.Queue()
    manager._broker_started_at_ms = int(time.time() * 1000)
    manager._broker_last_seen_ms = int(time.time() * 1000)
    now = int(time.time() * 1000)
    startup_timeout_ms = int(float(manager.fleet_config["startupTimeoutSeconds"]) * 1000)
    managed = ManagedWorker(
        "worker-00", FakeProcess(), RecordingQueue(), queue.Queue(),
        heartbeat=WorkerHeartbeat("worker-00", (), {}, 0, 0, 0, now - heartbeat_age_ms),
        pending_action="reconcile", pending_request_id="pending",
        control_sent_at_ms=now - sent_age_ms,
        control_deadline_ms=now - sent_age_ms + startup_timeout_ms,
        last_liveness_at_ms=now - heartbeat_age_ms,
    )
    manager._workers = {"worker-00": managed}
    recovered: list[str] = []

    async def recover(worker_id, _managed):
        recovered.append(worker_id)

    manager._recover_worker = recover
    return manager, recovered


def test_slow_reconcile_on_heartbeating_worker_is_not_recovered(tmp_path: Path):
    async def scenario():
        # Deadline (300 s) passed 10 s ago, heartbeat 1 s old: the worker is
        # alive and merely slow -> leave it alone (this tore the fleet down).
        manager, recovered = reconcile_manager(tmp_path, heartbeat_age_ms=1_000, sent_age_ms=310_000)
        await manager.monitor_once()
        assert recovered == []

    asyncio.run(scenario())


def test_reconcile_timeout_with_silent_worker_is_recovered(tmp_path: Path):
    async def scenario():
        manager, recovered = reconcile_manager(tmp_path, heartbeat_age_ms=20_000, sent_age_ms=310_000)
        await manager.monitor_once()
        assert recovered == ["worker-00"]

    asyncio.run(scenario())


def test_reconcile_past_hard_cap_is_recovered_even_with_fresh_heartbeat(tmp_path: Path):
    async def scenario():
        manager, recovered = reconcile_manager(tmp_path, heartbeat_age_ms=1_000, sent_age_ms=4 * 300_000 + 5_000)
        await manager.monitor_once()
        assert recovered == ["worker-00"]

    asyncio.run(scenario())


def test_stale_worker_is_left_alone_during_broker_restart_grace(tmp_path: Path):
    async def scenario():
        manager, recovered = reconcile_manager(tmp_path, heartbeat_age_ms=20_000, sent_age_ms=0)
        managed = manager._workers["worker-00"]
        managed.pending_action = ""
        managed.pending_request_id = ""
        managed.control_deadline_ms = 0
        manager._broker_restarted_at_ms = int(time.time() * 1000) - 1_000
        await manager.monitor_once()
        assert recovered == []
        manager._broker_restarted_at_ms = int(time.time() * 1000) - 120_000
        await manager.monitor_once()
        assert recovered == ["worker-00"]

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# admission on a stalled broker fails closed instead of aborting the refresh


def test_admission_timeout_fails_closed_and_recovers_when_broker_answers(tmp_path: Path):
    async def scenario():
        from fleet_models import ScreenerPick, ScreenerUpdate

        manager = ShardedBotManager(manager_config(tmp_path))
        manager._started = True
        manager._planning_only = False
        manager._broker = FakeProcess(pid=500)
        manager._broker_status_queue = queue.Queue()
        manager._heartbeat_queue = queue.Queue()
        manager._control_ack_queue = queue.Queue()
        manager._admin = FakeAdmin()
        manager._broker_started_at_ms = int(time.time() * 1000)
        manager._broker_last_seen_ms = int(time.time() * 1000)
        commands = RecordingQueue()
        manager._workers = {"worker-00": ManagedWorker("worker-00", FakeProcess(), commands, queue.Queue(), phase="healthy")}
        attempts = 0
        planning = ShardedBotManager(manager_config(tmp_path))

        async def admission(picks):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TimeoutError("broker admission_snapshot timed out")
            return await planning._admission(picks)

        manager._admission = admission
        pick = ScreenerPick(market_id="MKT", title="Market", yes_budget_cents=100, no_budget_cents=100, ranking={})
        update = ScreenerUpdate(1, 1, "test", (pick,), ("MKT",), (), (), ())
        await manager.apply_update(update)   # must not raise
        assert "admission snapshot failed" in manager._capacity_error
        assert manager._admission_pending
        assert commands.items[-1]["action"] == "enable_quoting" and commands.items[-1]["enabled"] is False

        manager._admission_retry_at_ms = 0
        # The retry waits for the broker to have reported in (heartbeat).
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now,
            "consecutive_timeouts": 0, "pending": 0, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert not manager._admission_pending
        assert manager._capacity_error == ""
        assert commands.items[-1]["action"] == "enable_quoting" and commands.items[-1]["enabled"] is True

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# (4) manager shutdown: never hang on a stalled broker


def shutdown_manager(tmp_path: Path, *, stalled: bool) -> tuple[ShardedBotManager, FakeProcess, FakeAdmin, AutoAckQueue]:
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._started = True
    manager._planning_only = False
    manager._control_ack_queue = queue.Queue()
    manager._broker_status_queue = queue.Queue()
    manager._heartbeat_queue = queue.Queue()
    manager._broker = FakeProcess(pid=500)
    manager._broker_started_at_ms = int(time.time() * 1000)
    manager._broker_last_seen_ms = int(time.time() * 1000)
    admin = FakeAdmin()
    manager._admin = admin
    if stalled:
        manager._broker_stalled = True
        manager._broker_stall_reason = "no broker heartbeat for 70s"
    process = FakeProcess()
    commands = AutoAckQueue("worker-00", manager._control_ack_queue, process)
    manager._workers = {"worker-00": ManagedWorker("worker-00", process, commands, queue.Queue())}
    return manager, process, admin, commands


def test_shutdown_with_stalled_broker_skips_broker_and_verifies_directly(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=True)
        cleanups: list[str] = []

        async def cleanup(market_id=""):
            cleanups.append(market_id)
            return 0

        manager._cancel_owned = cleanup
        started = time.monotonic()
        await manager.stop_all()
        assert time.monotonic() - started < 10.0
        assert admin.calls == []
        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "verified"
        assert result["ordersVerifiedAbsent"] is True
        assert result["brokerStopped"] is True and result["workersStopped"] is True
        assert any("execution broker is unavailable" in item for item in result["warnings"])
        assert len(cleanups) == 2
        assert not process.is_alive() and not manager._broker.is_alive()

    asyncio.run(scenario())


def test_shutdown_with_healthy_broker_fences_and_tells_workers_the_broker_answers(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=False)

        async def cleanup(market_id=""):
            return 0

        manager._cancel_owned = cleanup
        await manager.stop_all()
        assert [call[0] for call in admin.calls] == ["quiesce_all", "stop"]
        assert all(call[2] <= 15.0 for call in admin.calls)
        stop_commands = [item for item in commands.items if item.get("action") == "stop"]
        assert stop_commands and stop_commands[0]["broker_responsive"] is True
        assert manager.status_snapshot()["monitoring"]["shutdownCleanup"]["state"] == "verified"

    asyncio.run(scenario())


def test_shutdown_fence_timeout_marks_broker_stalled_and_finishes(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=False)
        admin.fail.add("quiesce_all")

        async def cleanup(market_id=""):
            return 0

        manager._cancel_owned = cleanup
        await manager.stop_all()
        assert [call[0] for call in admin.calls] == ["quiesce_all"]
        assert manager._broker_stalled
        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "verified" and result["brokerStopped"]

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# (6) broker live-cap ledger: the hard limit between allocations


class LedgerVenue(FakeVenue):
    """Venue double for order writes: every create/amend rests as requested
    (an immediate-or-cancel create fills in full on placement).

    ``normalize_sides=True`` makes the double answer like the real adaptor
    (``adaptors.kalshi._outcome_side``): the returned Order carries the
    OUTCOME side, so a ``sell`` of YES comes back as side "no".  The default
    echoes the request's raw side, which the ledger tests written against
    it relied on; new tests should use the adaptor contract.  Every request
    the venue saw is kept in ``created`` / ``amended``.
    """

    def __init__(self, *, normalize_sides: bool = False) -> None:
        super().__init__()
        self.sequence = 0
        self.normalize_sides = normalize_sides
        self.created: list = []
        self.amended: list = []

    def _answer_side(self, side, action="buy"):
        if not self.normalize_sides:
            return side
        from adaptors.kalshi import _outcome_side
        return _outcome_side({"side": side, "action": action}, side)

    def create_order(self, request):
        from clients.models import Order

        self.sequence += 1
        self.created.append(request)
        self.calls.append(("create_order", (request.market_id, request.side, request.price_units, request.count_units)))
        immediate = str(getattr(request, "time_in_force", "") or "") == "immediate_or_cancel"
        return Order(
            f"o{self.sequence}", request.market_id, self._answer_side(request.side, request.action),
            request.client_order_id, "executed" if immediate else "resting", request.price_units,
            request.count_units if immediate else 0, 0 if immediate else request.count_units,
        )

    def amend_order(self, request):
        from clients.models import Order

        self.amended.append(request)
        self.calls.append(("amend_order", (request.order_id, request.new_price_units, request.new_total_fillable_count_units)))
        return Order(
            request.order_id, request.market_id, request.side, request.updated_client_order_id, "resting",
            request.new_price_units, 0, request.new_total_fillable_count_units,
        )

    def decrease_order_to(self, *, order_id: str, remaining_count_units: int):
        self.calls.append(("decrease_order_to", (order_id, remaining_count_units)))
        return {"order_id": order_id, "remaining": remaining_count_units}

    def get_account_limits(self):
        from clients.models import AccountLimits, RateLimitBucket
        return AccountLimits("advanced", RateLimitBucket(300, 300), RateLimitBucket(300, 300))

    def get_account_balance(self):
        from clients.models import AccountBalance
        return AccountBalance(1_000_000, 0, balance_by_exchange=((0, 900_000), (3, 100_000)))

    def list_account_orders(self, query=None):
        return []

    def list_account_positions(self, query=None):
        from clients.models import AccountPosition
        return [AccountPosition("TEN-A", 300, market_exposure_units=30_000)]


def _create(ticker, side, price, count, *, reduce_only=False):
    from clients.models import CreateOrderRequest
    from fleet_models import IntentUrgency, QuoteIntent

    now = int(time.time() * 1000)
    return request("quote_intent", {
        "action": "create_order",
        "request": CreateOrderRequest(ticker, side, price, count, f"mm:{side}:x", None, reduce_only=reduce_only),
        "intent": QuoteIntent(ticker, side, price, count, 1, IntentUrgency.NORMAL, now, now + 20_000),
    })


def _amend(ticker, side, order_id, price, total):
    from clients.models import AmendOrderRequest
    from fleet_models import IntentUrgency, QuoteIntent

    now = int(time.time() * 1000)
    return request("quote_intent", {
        "action": "amend_order",
        "request": AmendOrderRequest(order_id, ticker, side, price, total, "mm:a", "mm:b"),
        "intent": QuoteIntent(ticker, side, price, total, 1, IntentUrgency.NORMAL_CANCEL_OR_AMEND, now, now + 20_000),
    })


def _writes(venue):
    return [name for name, _ in venue.calls if name in {"create_order", "amend_order", "decrease_order_to", "cancel_order"}]


def test_ledger_venue_can_answer_on_the_adaptor_outcome_side():
    from clients.models import CreateOrderRequest

    sell_yes = CreateOrderRequest(
        "TEN-A", "yes", 4_000, 300, "wd:yes:1", None, action="sell", reduce_only=True, time_in_force="immediate_or_cancel",
    )
    assert LedgerVenue().create_order(sell_yes).side == "yes"                 # legacy double: echoes the raw side
    venue = LedgerVenue(normalize_sides=True)
    order = venue.create_order(sell_yes)                                       # the adaptor's answer: outcome side
    assert order.side == "no" and order.status == "executed"
    assert (order.fill_count_units, order.remaining_count_units) == (300, 0)
    resting = venue.create_order(CreateOrderRequest("TEN-A", "no", 4_000, 100, "mm:no:1", None))
    assert resting.side == "no" and resting.status == "resting" and resting.remaining_count_units == 100
    assert [item.count_units for item in venue.created] == [300, 100]


def test_broker_ledger_refuses_new_exposure_past_the_shard_cap_and_passes_reducing_work():
    from clients.models import InsufficientBalanceError, OrderUpdate
    from fleet_runtime.execution import ShardExposureCapError, _urgency

    broker = make_broker()
    venue = LedgerVenue()
    # The controller's limits message: shard 3 may carry $10.00 of live exposure.
    limits = {
        "enabled": True, "allocatable_by_shard": {3: 100_000, 0: 500_000},
        "shard_by_ticker": {"TEN-A": 3, "TEN-B": 3, "IDX": 0},
        "position_exposure_by_ticker": {"TEN-A": 30_000},
    }
    assert broker._execute(venue, request("shard_exposure_limits", limits)) == "configured"
    assert not broker.is_stale(request("shard_exposure_limits", limits, age_ms=120_000))
    assert int(_urgency("shard_exposure_limits", limits)) == 0
    ledger = broker._ledger
    assert ledger.enabled and ledger.snapshot()["shards"]["3"]["positions_units"] == 30_000

    # $5.00 resting on TEN-A fits ($3.00 positions + $5.00 = $8.00 <= $10.00).
    first = broker._execute(venue, _create("TEN-A", "yes", 5_000, 1_000))
    assert first.order_id == "o1"
    assert ledger.live_exposure_units(3, broker._order_registry) == 80_000

    # A new $2.50 side on TEN-B would reach $10.50: refused before the venue
    # is called, typed as the balance error the bot cools down on.
    with pytest.raises(ShardExposureCapError) as refused:
        broker._execute(venue, _create("TEN-B", "no", 5_000, 500))
    assert isinstance(refused.value, InsufficientBalanceError)
    assert refused.value.detail["shard"] == 3 and refused.value.detail["live_units"] == 80_000
    assert refused.value.detail["notional_units"] == 25_000 and refused.value.detail["allocatable_units"] == 100_000
    assert _writes(venue) == ["create_order"]
    assert ledger.rejections == 1

    # Amend-up by $1.00 fits ($9.00); a further $1.50 does not; amend-down passes.
    broker._execute(venue, _amend("TEN-A", "yes", "o1", 5_000, 1_200))
    assert ledger.live_exposure_units(3, broker._order_registry) == 90_000
    with pytest.raises(ShardExposureCapError):
        broker._execute(venue, _amend("TEN-A", "yes", "o1", 5_000, 1_500))
    broker._execute(venue, _amend("TEN-A", "yes", "o1", 5_000, 800))
    assert ledger.live_exposure_units(3, broker._order_registry) == 70_000
    # Decreases and cancels always pass, and a cancel frees the notional.
    broker._execute(venue, request("decrease_order_to", {"order_id": "o1", "remaining_count_units": 100}))
    broker._execute(venue, request("cancel_order", {"order_id": "o1"}))
    assert ledger.live_exposure_units(3, broker._order_registry) == 30_000
    assert _writes(venue) == ["create_order", "amend_order", "amend_order", "decrease_order_to", "cancel_order"]

    # Reduce-only creates never reserve; shard 0 and untracked tickers are unaffected.
    broker._execute(venue, _create("TEN-B", "no", 5_000, 2_000, reduce_only=True))
    broker._execute(venue, _create("IDX", "yes", 5_000, 8_000))
    broker._execute(venue, _create("ZZZ", "yes", 5_000, 90_000))
    # Replacing the order on the same (ticker, side) excludes the one it replaces.
    broker._execute(venue, _create("TEN-B", "no", 5_000, 1_200))
    assert ledger.live_exposure_units(3, broker._order_registry) == 30_000 + 60_000

    # Fills since the position snapshot count as exposure until the next
    # snapshot replaces them; the venue's order update carries the remaining size.
    fill = OrderUpdate("TEN-B", "no", "o5", "mm:no:x", "resting", fill_count_units=200, remaining_count_units=1_000, price_units=5_000)
    broker._execute(venue, request("order_update", {"event": fill}))
    parts = ledger.snapshot(broker._order_registry)["shards"]["3"]
    assert (parts["positions_units"], parts["resting_units"], parts["fills_units"], parts["live_units"]) == (30_000, 50_000, 10_000, 90_000)
    with pytest.raises(ShardExposureCapError):
        broker._execute(venue, _create("TEN-A", "yes", 5_000, 300))    # $1.50 -> $10.50
    broker._execute(venue, _create("TEN-A", "yes", 5_000, 200))        # $1.00 -> exactly the cap
    assert ledger.live_exposure_units(3, broker._order_registry) == 100_000

    # An admission snapshot served by the broker re-seeds positions and
    # clears the fill delta (the venue now reports them inside positions).
    broker._execute(venue, request("admission_snapshot"))
    parts = ledger.snapshot(broker._order_registry)["shards"]["3"]
    assert parts["positions_units"] == 30_000 and parts["fills_units"] == 0

    # Disabled (the controller sends enabled=False at 1.0): everything passes.
    broker._execute(venue, request("shard_exposure_limits", {**limits, "enabled": False}))
    broker._execute(venue, _create("TEN-B", "yes", 5_000, 90_000))
    assert ledger.rejections == 3


def test_broker_ledger_reservation_stops_concurrent_creates_sharing_headroom():
    broker = make_broker()
    venue = LedgerVenue()
    broker._ledger.configure(enabled=True, allocatable_by_shard={3: 100_000}, shard_by_ticker={"A": 3, "B": 3})
    registry = broker._order_registry
    tokens = [broker._ledger.reserve("A", 60_000, orders=registry), broker._ledger.reserve("B", 30_000, orders=registry)]
    from fleet_runtime.execution import ShardExposureCapError
    with pytest.raises(ShardExposureCapError):
        broker._ledger.reserve("B", 20_000, orders=registry)   # 90_000 reserved + 20_000 > cap
    for token in tokens:
        broker._ledger.release(token)
    assert broker._ledger.reserve("B", 20_000, orders=registry)
    assert venue.calls == []


def test_shard_exposure_cap_error_crosses_the_rpc_boundary_as_a_balance_error():
    from clients.models import InsufficientBalanceError
    from fleet_runtime.execution import ShardExposureCapError

    broker = make_broker()
    responses = broker.response_queues["worker-00"]
    error = ShardExposureCapError("shard 3 exposure cap", detail={"shard": 3, "live_units": 1})
    broker._respond(request("quote_intent"), error=error)
    message = responses.get_nowait()
    # Serialised under the base name (older workers map it to the cooldown) with
    # the precise class in the detail for current workers.
    assert message["error_type"] == "InsufficientBalanceError"
    assert message["error_detail"]["error_class"] == "ShardExposureCapError" and message["error_detail"]["shard"] == 3

    venue = FakeVenue()
    requests_queue: "queue.Queue[BrokerRequest]" = queue.Queue()
    answers: "queue.Queue[dict]" = queue.Queue()
    client = BrokerRpcClient(venue, requests_queue, answers, "worker-00")

    def answer() -> None:
        item = requests_queue.get(timeout=2.0)
        answers.put({**message, "request_id": item.request_id})
    thread = threading.Thread(target=answer, daemon=True)
    thread.start()
    try:
        from clients.models import CreateOrderRequest
        with pytest.raises(ShardExposureCapError) as raised:
            client.create_order(CreateOrderRequest("TEN-A", "yes", 5_000, 100, "mm:yes:1", None))
        assert isinstance(raised.value, InsufficientBalanceError)
        # A response without the detail (an older broker) still maps to the base class.
        thread.join(timeout=2.0)
        threading.Thread(
            target=lambda: answers.put({
                **{k: v for k, v in message.items() if k != "error_detail"}, "error_detail": None,
                "request_id": requests_queue.get(timeout=2.0).request_id,
            }),
            daemon=True,
        ).start()
        with pytest.raises(InsufficientBalanceError) as plain:
            client.create_order(CreateOrderRequest("TEN-A", "yes", 5_000, 100, "mm:yes:2", None))
        assert type(plain.value) is InsufficientBalanceError
    finally:
        client.close_dispatcher()


# ----------------------------------------------------------------------
# (5) status snapshot replace tolerates a reader holding the file


def test_atomic_write_json_retries_while_a_reader_holds_the_target(tmp_path: Path, monkeypatch):
    target = tmp_path / "launcher_status.json"
    target.write_text("{}", encoding="utf-8")
    real_replace = os.replace
    failures = {"left": 2}

    def flaky_replace(src, dst):
        if failures["left"] > 0:
            failures["left"] -= 1
            raise PermissionError(5, "Access is denied")
        return real_replace(src, dst)

    monkeypatch.setattr(lip_launcher.os, "replace", flaky_replace)
    monkeypatch.setattr(lip_launcher, "ATOMIC_REPLACE_RETRY_SECONDS", 0.001)
    lip_launcher.atomic_write_json(target, {"ok": True})
    assert target.read_text(encoding="utf-8").strip() == '{\n  "ok": true\n}'
    assert sorted(p.name for p in tmp_path.iterdir()) == ["launcher_status.json"]


def test_atomic_write_json_gives_up_without_leaving_temp_files(tmp_path: Path, monkeypatch):
    target = tmp_path / "launcher_status.json"

    def always_denied(src, dst):
        raise PermissionError(5, "Access is denied")

    monkeypatch.setattr(lip_launcher.os, "replace", always_denied)
    monkeypatch.setattr(lip_launcher, "ATOMIC_REPLACE_RETRY_SECONDS", 0.001)
    with pytest.raises(PermissionError):
        lip_launcher.atomic_write_json(target, {"ok": True})
    assert list(tmp_path.iterdir()) == []
