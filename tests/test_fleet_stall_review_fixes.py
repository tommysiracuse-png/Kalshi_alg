"""Review follow-ups to the broker-stall fix: HTTP-layer failures feed the
stall signal, only real venue calls count as successes, broker generations
survive a terminate inside Queue.get, the order registry is locked and
reconciles abandoned creates, recovery/shutdown time budgets, reconcile makes
pending adds due, deferred reconcile after a broker restart, ping urgency,
and manager events reaching the launcher log."""

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import queue
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import requests

from clients.http_client import HTTPClientError
from clients.models import ClientError, OrderNotFoundError, OrderUpdate
from fleet_models import BotManagerEvent, IntentUrgency, QuoteIntent, ScreenerPick, WorkerHeartbeat
from fleet_runtime import manager as manager_module
from fleet_runtime.execution import (
    BrokerRequest,
    ExecutionBrokerProcess,
    OrderRegistry,
    _urgency,
    cancel_and_verify_owned_orders,
    is_venue_transport_failure,
)
from fleet_runtime.manager import ManagedWorker, ShardedBotManager
from fleet_runtime.worker import PendingAdd, queue_pending_adds
from launcher import Launcher

from tests.test_fleet_stall import (
    FakeAdmin,
    FakeProcess,
    FakeVenue,
    RecordingQueue,
    make_broker,
    manager_config,
    request,
    shutdown_manager,
    stalled_manager,
)


# ----------------------------------------------------------------------
# (1) HTTP-layer failures count as venue failures


def test_requests_transport_errors_are_venue_failures_even_when_wrapped():
    assert is_venue_transport_failure(requests.exceptions.ReadTimeout("read timed out"))
    assert is_venue_transport_failure(requests.exceptions.ConnectTimeout("connect timed out"))
    assert is_venue_transport_failure(requests.exceptions.ConnectionError("connection reset"))
    assert is_venue_transport_failure(TimeoutError("broker deadline"))
    assert is_venue_transport_failure(ConnectionResetError())
    # KalshiApiClient maps HTTP errors to ClientError(...) from the transport error.
    gateway = HTTPClientError(method="GET", path="/x", status_code=504, response_text="gateway timeout")
    wrapped = ClientError("GET /x failed 504")
    wrapped.__cause__ = gateway
    assert is_venue_transport_failure(wrapped)
    chained = RuntimeError("cleanup")
    chained.__context__ = requests.exceptions.ReadTimeout("slow")
    assert is_venue_transport_failure(chained)
    # Venue rejections are not transport failures.
    assert not is_venue_transport_failure(OrderNotFoundError("gone"))
    assert not is_venue_transport_failure(ClientError("post_only_cross"))
    denied = HTTPClientError(method="GET", path="/x", status_code=403, response_text="forbidden")
    assert not is_venue_transport_failure(denied)


# ----------------------------------------------------------------------
# (2) only calls that reached the venue count as successes


def test_broker_local_operations_and_cache_hits_do_not_count_as_venue_calls():
    broker = make_broker()
    venue = FakeVenue()
    event = OrderUpdate("MKT", "yes", "o1", "mm:yes:1", "resting")
    for operation, payload in (
        ("ping", {}), ("order_update", {"event": event}), ("stop", {}),
    ):
        _result, touched = broker._execute_traced(venue, request(operation, payload))
        assert touched is False, operation
    _result, touched = broker._execute_traced(venue, request("get_market", {"market_id": "A"}))
    assert touched is True
    _result, touched = broker._execute_traced(venue, request("get_market", {"market_id": "A"}))
    assert touched is False   # served from the 5 s cache: no venue call happened
    _result, touched = broker._execute_traced(venue, request("get_resting_orders", {"market_id": "A"}))
    assert touched is True
    result, touched = broker._execute_bounded_traced(venue, request("ping"))
    assert (result, touched) == ("pong", False)
    assert broker._execute_bounded(venue, request("ping")) == "pong"
    broker._get_executor().shutdown(wait=True)


# ----------------------------------------------------------------------
# (3) broker generations and the shared request queue lock


def test_broker_reads_request_queue_without_the_reader_lock():
    source: Any = mp.Queue()
    broker = make_broker(request_queue=source)
    try:
        # An orphaned reader lock (left by a broker killed inside get()).
        assert source._rlock.acquire(block=False)
        source.put(request("ping"))
        started = time.monotonic()
        item = broker._next_request(2.0)
        assert time.monotonic() - started < 2.0
        assert isinstance(item, BrokerRequest) and item.operation == "ping"
        with pytest.raises(queue.Empty):
            broker._next_request(0.0)
        with pytest.raises(queue.Empty):
            broker._next_request(0.05)
    finally:
        try:
            source._rlock.release()
        except ValueError:
            pass
        source.close()
        source.join_thread()


def test_broker_falls_back_to_plain_get_for_thread_queues():
    broker = make_broker()
    broker.request_queue.put(request("ping"))
    assert broker._next_request(0.0).operation == "ping"
    with pytest.raises(queue.Empty):
        broker._next_request(0.0)
    with pytest.raises(queue.Empty):
        broker._next_request(0.01)


def test_manager_releases_an_orphaned_request_queue_lock_on_broker_restart(tmp_path: Path):
    source = mp.Queue()
    try:
        manager = ShardedBotManager(manager_config(tmp_path))
        manager._request_queue = source
        assert manager._release_request_queue_lock() is False      # not held: nothing to do
        assert source._rlock.acquire(block=False)                   # a dead broker "holds" it
        assert manager._release_request_queue_lock() is True
        assert source._rlock.acquire(block=False)                   # usable again
        source._rlock.release()
        manager._request_queue = None
        assert manager._release_request_queue_lock() is False
    finally:
        source.close()
        source.join_thread()


def test_broker_restart_releases_the_queue_lock_after_terminating(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        released: list[bool] = []
        manager._release_request_queue_lock = lambda: released.append(True) or True
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 120_000,
            "consecutive_timeouts": 5, "pending": 40, "in_flight": 0, "dropped_stale": 12,
        })
        await manager.monitor_once()
        assert broker.terminated and released == [True]

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# (4) order registry: lock-protected, abandoned creates reconciled


class OrderVenue(FakeVenue):
    """Venue double for create/amend/cancel with a controllable slow create."""

    def __init__(self) -> None:
        super().__init__()
        self.created: list[str] = []
        self.canceled: list[str] = []
        self.next_id = 0
        self.slow_create = threading.Event()
        self.slow_create.set()
        self.fail_cancel: set[str] = set()
        self.lock = threading.Lock()

    def create_order(self, request):
        self.slow_create.wait(timeout=5.0)
        with self.lock:
            self.next_id += 1
            order_id = f"O{self.next_id}"
            self.created.append(order_id)
        return SimpleNamespace(order_id=order_id, market_id=request.market_id, side=request.side, status="resting")

    def amend_order(self, request):
        with self.lock:
            self.next_id += 1
            order_id = f"O{self.next_id}"
            self.created.append(order_id)
        return SimpleNamespace(order_id=order_id, market_id=request.market_id, side=request.side, status="resting")

    def cancel_order(self, *, order_id: str):
        with self.lock:
            self.canceled.append(order_id)
        if order_id in self.fail_cancel:
            raise requests.exceptions.ReadTimeout("cancel timed out")
        return {"order_id": order_id}


def create_request(ticker: str = "MKT", side: str = "yes") -> BrokerRequest:
    intent = QuoteIntent(ticker, side, 4_000, 100, 1, IntentUrgency.NORMAL, 1, 20_000)
    order = SimpleNamespace(market_id=ticker, side=side, reduce_only=False)
    return request("quote_intent", {"action": "create_order", "request": order, "intent": intent})


def test_order_registry_is_thread_safe_and_keeps_newer_registrations():
    registry = OrderRegistry()
    key = ("MKT", "yes")
    errors: list[BaseException] = []

    def hammer(offset: int) -> None:
        try:
            for index in range(500):
                registry.set(key, SimpleNamespace(order_id=f"o{offset}-{index}"), stamp=offset * 1_000 + index)
                registry.get(key)
                registry.add_orphan(key, f"x{offset}-{index}")
                registry.forget_order(f"x{offset}-{index}")
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=hammer, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert registry.get(key).order_id == "o3-499"
    assert registry.orphans(key) == []

    # An older stamp never overwrites: the late order becomes an orphan.
    assert registry.set(key, SimpleNamespace(order_id="late"), stamp=0) is False
    assert registry.get(key).order_id == "o3-499"
    assert registry.orphans(key) == ["late"]
    registry.forget_order("late")
    assert registry.orphans(key) == []


def test_abandoned_create_that_completes_late_is_cancelled_not_left_resting():
    broker = make_broker(execute_timeout_seconds=0.2)
    venue = OrderVenue()
    venue.slow_create.clear()
    try:
        with pytest.raises(TimeoutError):
            broker._execute_bounded(venue, create_request())
        # The requester has been told TimeoutError; the create lands later.
        venue.slow_create.set()
        deadline = time.monotonic() + 3.0
        while "O1" not in venue.canceled and time.monotonic() < deadline:
            time.sleep(0.01)
        assert venue.created == ["O1"]
        assert venue.canceled == ["O1"]
        assert broker._order_registry == {}
        # The next create on this side starts clean: no stale cancel, one order.
        result = broker._execute_bounded(venue, create_request())
        assert result.order_id == "O2"
        assert venue.canceled == ["O1"]
        assert broker._order_registry[("MKT", "yes")].order_id == "O2"
    finally:
        venue.slow_create.set()
        broker._get_executor().shutdown(wait=True)


def test_abandoned_create_whose_cancel_fails_is_cancelled_by_the_next_create():
    broker = make_broker(execute_timeout_seconds=0.2)
    venue = OrderVenue()
    venue.slow_create.clear()
    venue.fail_cancel.add("O1")
    try:
        with pytest.raises(TimeoutError):
            broker._execute_bounded(venue, create_request())
        venue.slow_create.set()
        deadline = time.monotonic() + 3.0
        while "O1" not in venue.canceled and time.monotonic() < deadline:
            time.sleep(0.01)
        # Interleaving: a fresh create ran while O1 was still an orphan.
        deadline = time.monotonic() + 3.0
        while not broker._registry.orphans(("MKT", "yes")) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert broker._registry.orphans(("MKT", "yes")) == ["O1"]
        venue.fail_cancel.clear()
        result = broker._execute_bounded(venue, create_request())
        assert result.order_id == "O2"
        assert venue.canceled.count("O1") == 2          # retried before the new create
        assert broker._registry.orphans(("MKT", "yes")) == []
        assert broker._order_registry[("MKT", "yes")].order_id == "O2"
    finally:
        venue.slow_create.set()
        broker._get_executor().shutdown(wait=True)


def test_order_update_events_do_not_forget_a_different_live_order():
    broker = make_broker()
    venue = OrderVenue()
    result = broker._execute(venue, create_request())
    assert result.order_id == "O1"
    broker._registry.add_orphan(("MKT", "yes"), "orphan")
    # A cancel/expiry event for the orphan must not drop the live order.
    broker._execute(venue, request("order_update", {"event": OrderUpdate("MKT", "yes", "orphan", "mm:yes", "canceled")}))
    assert broker._order_registry[("MKT", "yes")].order_id == "O1"
    assert broker._registry.orphans(("MKT", "yes")) == []
    # A resting event for an orphan leaves it an orphan; for the live order it refreshes it.
    broker._registry.add_orphan(("MKT", "yes"), "orphan2")
    broker._execute(venue, request("order_update", {"event": OrderUpdate("MKT", "yes", "orphan2", "mm:yes", "resting")}))
    assert broker._order_registry[("MKT", "yes")].order_id == "O1"
    assert broker._registry.orphans(("MKT", "yes")) == ["orphan2"]
    broker._execute(venue, request("order_update", {"event": OrderUpdate("MKT", "yes", "O1", "mm:yes", "canceled")}))
    assert ("MKT", "yes") not in broker._order_registry
    # cancel_order forgets by id.
    broker._execute(venue, create_request())
    broker._execute(venue, request("cancel_order", {"order_id": "O2"}))
    assert ("MKT", "yes") not in broker._order_registry


# ----------------------------------------------------------------------
# (5) worker recovery: bounded broker waits, no 60 s per ticker


def test_worker_recovery_uses_bounded_broker_timeouts_and_goes_direct_after_a_timeout(tmp_path: Path):
    async def scenario():
        manager = ShardedBotManager(manager_config(tmp_path, dry_run=False))
        admin = FakeAdmin(fail={"cancel_market"})
        manager._admin = admin
        manager._broker = FakeProcess(pid=500)
        manager._broker_started_at_ms = int(time.time() * 1000)
        manager._broker_last_seen_ms = int(time.time() * 1000)
        manager._assignments = {"worker-00": ("A", "B", "C")}
        pick = lambda t: ScreenerPick(market_id=t, title=t, yes_budget_cents=1, no_budget_cents=1, ranking={})
        manager._desired = {t: pick(t) for t in ("A", "B", "C")}
        direct_calls: list[str] = []

        class DirectClient:
            def list_account_orders(self, query):
                direct_calls.append(query.market_id)
                return []

        manager.cleanup_client = DirectClient()
        spawned: list[str] = []

        def make_worker(worker_id, response_queue, command_queue):
            spawned.append(worker_id)
            return FakeProcess(pid=700)

        manager._make_worker = make_worker
        managed = ManagedWorker("worker-00", FakeProcess(running=False), RecordingQueue(), queue.Queue())
        manager._workers = {"worker-00": managed}
        await manager._recover_worker("worker-00", managed)

        operations = [call[0] for call in admin.calls]
        assert operations == ["quiesce_channel", "cancel_market"]     # one broker timeout, then direct
        assert all(call[2] <= manager_module.BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS for call in admin.calls)
        assert direct_calls == ["A", "B", "C"]
        assert spawned == ["worker-00"]
        assert managed.phase == "reconciling"

    asyncio.run(scenario())


def test_recovery_admin_timeout_is_shortened_further_when_broker_is_flagged_stalled(tmp_path: Path):
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._broker = FakeProcess(pid=500)
    manager._broker_started_at_ms = int(time.time() * 1000)
    manager._broker_last_seen_ms = int(time.time() * 1000)
    assert manager._admin_timeout(manager_module.BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS) == 20.0
    manager._broker_stalled = True
    assert manager._admin_timeout(manager_module.BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS) == 10.0


# ----------------------------------------------------------------------
# (6) stop_all runs inside a wall-clock budget under the API's 120 s kill


def test_shutdown_budget_documented_below_api_stop_grace():
    from ui_api.store import OperationsStore

    api_grace = float(OperationsStore._windows_stop_timeout_seconds)
    # budget + the venue call in flight when the deadline passes + the single
    # post-budget verification read + the two terminate/kill escalations
    worst_case = (
        manager_module.SHUTDOWN_BUDGET_SECONDS
        + manager_module.SHUTDOWN_VENUE_CALL_SECONDS
        + manager_module.SHUTDOWN_POST_BUDGET_READ_SECONDS
        + 2 * manager_module.SHUTDOWN_ESCALATION_SECONDS
    )
    assert worst_case == 114.0
    assert worst_case < api_grace
    caps = (
        manager_module.SHUTDOWN_FENCE_SECONDS + manager_module.SHUTDOWN_FREEZE_ACK_SECONDS
        + manager_module.SHUTDOWN_BROKER_CLEANUP_SECONDS + manager_module.SHUTDOWN_STOP_ACK_SECONDS
        + manager_module.SHUTDOWN_WORKER_GRACE_SECONDS + manager_module.SHUTDOWN_BROKER_CLEANUP_SECONDS
        + manager_module.SHUTDOWN_BROKER_STOP_SECONDS + manager_module.SHUTDOWN_BROKER_GRACE_SECONDS
    )
    assert caps == 85.0


def test_shutdown_phases_are_clamped_to_the_remaining_budget(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=False)
        manager.shutdown_budget_seconds = 1.0
        policies: list[manager_module.CleanupPolicy] = []
        graces: list[float] = []

        async def cleanup(market_id=""):
            policies.append(manager._cleanup_policy_state)
            await asyncio.sleep(1.2)     # burns the whole budget on the first cleanup
            return 0

        async def terminate(target, *, graceful_timeout=0.0):
            graces.append(graceful_timeout)
            target.running = False
            return True

        manager._cancel_owned = cleanup
        manager._terminate_process = terminate
        started = time.monotonic()
        await manager.stop_all()
        assert time.monotonic() - started < 5.0
        assert [call[0] for call in admin.calls] == ["quiesce_all"]
        assert admin.calls[0][2] <= 1.0
        assert len(policies) == 2
        assert all(item.mark_stall_on_timeout and item.direct_deadline is not None for item in policies)
        assert policies[0].via_broker is True and 0 < policies[0].broker_timeout <= 1.0
        # Second cleanup ran after the budget was gone: direct client only, the
        # broker stop command was skipped, and every graceful join clamped to 0.
        assert policies[1].via_broker is False and policies[1].broker_timeout == 0.0
        assert graces and all(grace == 0.0 for grace in graces)
        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "verified" and result["brokerStopped"] and result["workersStopped"]

    asyncio.run(scenario())


def test_shutdown_broker_cleanup_timeout_marks_broker_stalled_and_skips_it_afterwards(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=False)
        manager.config = replace(manager.config, dry_run=False)
        admin.fail.add("cancel_all")
        direct: list[str] = []

        class DirectClient:
            def list_account_orders(self, query):
                direct.append("list")
                return []

        manager.cleanup_client = DirectClient()
        await manager.stop_all()
        operations = [call[0] for call in admin.calls]
        assert operations == ["quiesce_all", "cancel_all"]      # second cancel_all and stop skipped
        assert manager._broker_stalled and "cancel_all" in manager._broker_stall_reason
        assert direct == ["list", "list"]
        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "verified" and result["ordersVerifiedAbsent"] is True

    asyncio.run(scenario())


def test_stop_all_drains_broker_heartbeats_before_judging_the_broker(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=False)
        manager.broker_stall_seconds = 2.0
        manager._broker_last_seen_ms = int(time.time() * 1000) - 5_000      # stale stamp from a busy loop
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": int(time.time() * 1000), "last_success_at_ms": int(time.time() * 1000),
            "consecutive_timeouts": 0, "pending": 0, "in_flight": 0, "dropped_stale": 0,
        })

        async def cleanup(market_id=""):
            return 0

        manager._cancel_owned = cleanup
        await manager.stop_all()
        assert [call[0] for call in admin.calls] == ["quiesce_all", "stop"]

    asyncio.run(scenario())


def test_direct_cleanup_stops_starting_attempts_after_its_deadline():
    class SlowVenue:
        def __init__(self) -> None:
            self.reads = 0

        def list_account_orders(self, query):
            self.reads += 1
            raise requests.exceptions.ReadTimeout("venue silent")

    venue = SlowVenue()
    slept: list[float] = []
    with pytest.raises(Exception, match="could not verify"):
        cancel_and_verify_owned_orders(
            venue, attempts=8, delay_seconds=0.5, sleep=slept.append,
            deadline=time.monotonic() - 1.0,
        )
    # One attempt; the venue did not answer it, so the verification read that
    # would only time out again is skipped (see round-3 tests for the answered case).
    assert venue.reads == 1
    assert slept == [0.0] or slept == []


# ----------------------------------------------------------------------
# (7) a re-issued reconcile makes every pending add due immediately


def test_reconcile_resets_pending_add_backoff_and_drops_undesired_markets():
    pick = lambda t: ScreenerPick(market_id=t, title=t, yes_budget_cents=1, no_budget_cents=1, ranking={})
    later = int(time.time() * 1000) + 240_000
    pending = {
        "A": PendingAdd(pick("A"), attempts=4, retry_at_ms=later, error="broker timed out"),
        "GONE": PendingAdd(pick("GONE"), attempts=1, retry_at_ms=later),
    }
    new_a = pick("A")
    due = queue_pending_adds(pending, {"A": new_a, "B": pick("B"), "RUNNING": pick("RUNNING")}, ["RUNNING"])
    assert due == ["A", "B"]
    assert set(pending) == {"A", "B"}
    assert pending["A"].retry_at_ms == 0 and pending["A"].attempts == 4 and pending["A"].pick is new_a
    assert pending["B"].attempts == 0 and pending["B"].retry_at_ms == 0


# ----------------------------------------------------------------------
# (8) the assignment is re-issued once the replacement broker is up, not per attempt


def test_reconcile_is_deferred_until_the_replacement_broker_heartbeats(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 120_000,
            "consecutive_timeouts": 5, "pending": 40, "in_flight": 0, "dropped_stale": 12,
        })
        await manager.monitor_once()
        assert len(replacements) == 1
        # Quoting gate only (restart + closed-gate rebroadcast): no reconcile yet.
        assert set(commands.actions()) == {"enable_quoting"}
        assert manager._reconcile_after_broker_ready
        assert manager.status_snapshot()["broker"]["reconcilePendingBrokerReady"] is True

        # A startup_error from the replacement is not readiness.
        manager._broker_status_queue.put({"type": "startup_error", "at_ms": int(time.time() * 1000), "error": "cleanup failed"})
        await manager.monitor_once()
        assert "reconcile" not in commands.actions()

        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now,
            "consecutive_timeouts": 0, "pending": 0, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert commands.actions().count("reconcile") == 1
        assert [pick.market_id for pick in commands.items[-1]["picks"]] == ["MKT"]
        assert not manager._reconcile_after_broker_ready

        # Later heartbeats do not repeat it.
        manager._workers["worker-00"].pending_request_id = ""
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": int(time.time() * 1000), "last_success_at_ms": now,
            "consecutive_timeouts": 0, "pending": 0, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert commands.actions().count("reconcile") == 1

    asyncio.run(scenario())


def test_dead_broker_restart_loop_does_not_reconcile_workers_per_attempt(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        broker.running = False
        for attempt in range(4):
            await manager.monitor_once()
            # Each replacement dies during startup; the dead-restart interval elapses.
            replacements[-1].running = False
            manager._broker_status_queue.put({"type": "startup_error", "at_ms": int(time.time() * 1000), "error": "cancel failed"})
            manager._broker_restarted_at_ms -= 6_000
        assert len(replacements) == 4
        # Only quoting gates (from the restart and the fail-closed rebroadcast)
        # reach the workers: no reconcile, so no websocket churn per attempt.
        assert set(commands.actions()) == {"enable_quoting"}
        assert manager._broker_restart_count == 4
        # Then a replacement comes up: exactly one reconcile.
        await manager.monitor_once()
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now,
            "consecutive_timeouts": 0, "pending": 0, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert commands.actions().count("reconcile") == 1

    asyncio.run(scenario())


def test_no_reconcile_is_issued_when_the_broker_comes_up_during_shutdown(tmp_path: Path):
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._broker_started_at_ms = int(time.time() * 1000)
    manager._reconcile_after_broker_ready = True
    commands = RecordingQueue()
    manager._workers = {"worker-00": ManagedWorker("worker-00", FakeProcess(), commands, queue.Queue(), phase="healthy")}
    manager.begin_shutdown()
    manager._ingest_broker_status({"type": "heartbeat", "at_ms": int(time.time() * 1000), "last_success_at_ms": 1, "consecutive_timeouts": 0})
    assert commands.items == [] and manager._reconcile_after_broker_ready


# ----------------------------------------------------------------------
# (9) ping bypasses a quote burst


def test_ping_is_a_control_operation_that_outranks_quotes():
    assert _urgency("ping", {}) == IntentUrgency.EMERGENCY_CANCEL
    assert int(_urgency("ping", {})) < int(_urgency("get_market", {"market_id": "A"}))
    intent = QuoteIntent("MKT", "yes", 4_000, 100, 1, IntentUrgency.NORMAL, 1, 20_000)
    assert int(_urgency("ping", {})) < int(_urgency("quote_intent", {"intent": intent}))


# ----------------------------------------------------------------------
# (10) manager events reach the launcher console log and run log


def _launcher_for_events(tmp_path: Path) -> Launcher:
    launcher = Launcher.__new__(Launcher)
    launcher.manager = SimpleNamespace(events=asyncio.Queue())
    launcher.run_artifact_path = tmp_path / "run"
    launcher._observability_warning = None
    launcher._last_observability_warning_log_ms = 0
    return launcher


def test_launcher_logs_manager_events_instead_of_discarding_them(tmp_path: Path, caplog):
    launcher = _launcher_for_events(tmp_path)
    now = int(time.time() * 1000)
    launcher.manager.events.put_nowait(BotManagerEvent(
        "broker_restarted", "*", now, {"pid": 4242, "reason": "restarted: 3 consecutive venue timeouts"},
    ))
    launcher.manager.events.put_nowait(BotManagerEvent(
        "worker_recovery_failed", "*", now, {"worker_id": "worker-02", "error": "broker quiesce: timed out", "remaining_order_ids": ["o1", "o2"]},
    ))
    launcher.manager.events.put_nowait(BotManagerEvent("admission_recovered", "*", now, {"gate_open": True}))
    launcher.manager.events.put_nowait(BotManagerEvent("bot_started", "MKT-1", now, {}))
    with caplog.at_level(logging.INFO, logger="launcher"):
        launcher._drain_manager_events()
    assert launcher.manager.events.empty()
    records = [(record.levelno, record.getMessage()) for record in caplog.records if record.name == "launcher"]
    assert records[0][0] == logging.WARNING
    assert "MANAGER_EVENT | type=broker_restarted pid=4242 reason=restarted: 3 consecutive venue timeouts" in records[0][1]
    assert records[1][0] == logging.WARNING and "worker_id=worker-02" in records[1][1] and "remaining_order_ids=o1,o2" in records[1][1]
    assert records[2][0] == logging.INFO and "type=admission_recovered gate_open=True" in records[2][1]
    assert records[3][0] == logging.INFO and "type=bot_started market=MKT-1" in records[3][1]
    run_log = (tmp_path / "run" / "launcher.log").read_text(encoding="utf-8")
    assert "type=broker_restarted" in run_log
    assert "type=worker_recovery_failed" in run_log
    assert "type=admission_recovered" in run_log
    assert "bot_started" not in run_log


def test_launcher_event_drain_tolerates_missing_run_artifact_path(tmp_path: Path):
    launcher = _launcher_for_events(tmp_path)
    launcher.run_artifact_path = None
    launcher.manager.events.put_nowait(BotManagerEvent("shutdown_cleanup_failed", "*", 1, {"error": "x"}))
    launcher._drain_manager_events()
    assert launcher.manager.events.empty()
