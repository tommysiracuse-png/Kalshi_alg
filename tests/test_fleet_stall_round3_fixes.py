"""Round-3 review follow-ups: replacement workers survive an orphaned response
queue lock, the direct-client shutdown path is bounded to the API's 120 s,
admission retries wait for the replacement broker, owed reconciles reach busy
workers, abandoned creates are not placed, orphans are never dropped, broker
reader errors do not spin, and a skipped broker stop terminates at once."""

from __future__ import annotations

import asyncio
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

from clients.models import AccountOrder, ClientError
from fleet_models import ScreenerPick, WorkerControlAck, WorkerHeartbeat
from fleet_runtime import manager as manager_module
from fleet_runtime.execution import (
    BrokerQueueUnreadable,
    BrokerRequestDecodeError,
    BrokerRpcClient,
    cancel_and_verify_owned_orders,
    read_queue_lockfree,
    release_queue_reader_lock,
)
from fleet_runtime.manager import ManagedWorker, ShardedBotManager

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
from tests.test_fleet_stall_review_fixes import OrderVenue, create_request


def _pick(ticker: str) -> ScreenerPick:
    return ScreenerPick(market_id=ticker, title=ticker, yes_budget_cents=1, no_budget_cents=1, ranking={})


def _close_queue(source: Any) -> None:
    try:
        source._rlock.release()
    except ValueError:
        pass
    source.close()
    source.join_thread()


# ----------------------------------------------------------------------
# (1) a replacement worker on a response queue whose reader lock is orphaned


def test_rpc_client_dispatcher_reads_responses_without_the_reader_lock():
    responses: Any = mp.Queue()
    requests_queue: "queue.Queue[Any]" = queue.Queue()
    # The terminated worker died inside Queue.get(): its reader lock is held.
    assert responses._rlock.acquire(block=False)
    client = BrokerRpcClient(FakeVenue(), requests_queue, responses, "worker-00")

    def broker() -> None:
        item = requests_queue.get(timeout=3.0)
        responses.put({"request_id": item.request_id, "ok": True, "result": "pong"})

    thread = threading.Thread(target=broker, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        assert client.ping(timeout=3.0) == "pong"
        assert time.monotonic() - started < 3.0
    finally:
        client.close_dispatcher()
        thread.join(timeout=1.0)
        _close_queue(responses)


def test_release_queue_reader_lock_frees_only_an_orphaned_lock():
    source: Any = mp.Queue()
    try:
        assert release_queue_reader_lock(source) is False          # not held
        assert source._rlock.acquire(block=False)
        assert release_queue_reader_lock(source) is True
        assert source._rlock.acquire(block=False)                  # usable again
        source._rlock.release()
        assert release_queue_reader_lock(queue.Queue()) is False   # thread queues: nothing to do
        assert release_queue_reader_lock(None) is False
    finally:
        _close_queue(source)


def test_worker_replaced_after_a_mid_get_termination_still_gets_responses(tmp_path: Path):
    responses: Any = mp.Queue()
    client = None
    thread = None
    try:
        async def scenario():
            nonlocal client, thread
            manager = ShardedBotManager(manager_config(tmp_path))
            manager._admin = FakeAdmin()
            manager._broker = FakeProcess(pid=500)
            manager._broker_started_at_ms = int(time.time() * 1000)
            manager._broker_last_seen_ms = int(time.time() * 1000)
            manager._assignments = {"worker-00": ("A",)}
            manager._desired = {"A": _pick("A")}
            spawned: list[tuple[str, Any]] = []

            def make_worker(worker_id, response_queue, command_queue):
                spawned.append((worker_id, response_queue))
                return FakeProcess(pid=700)

            manager._make_worker = make_worker
            # The old worker was terminated inside Queue.get(): lock orphaned.
            assert responses._rlock.acquire(block=False)
            managed = ManagedWorker("worker-00", FakeProcess(running=False), RecordingQueue(), responses)
            manager._workers = {"worker-00": managed}
            await manager._recover_worker("worker-00", managed)
            assert [item[0] for item in spawned] == ["worker-00"]
            assert spawned[0][1] is responses            # same channel the broker routes to
            assert managed.phase == "reconciling"
            # The orphaned lock was released before the replacement started...
            assert responses._rlock.acquire(block=False)
            responses._rlock.release()
            # ...and even if it were still held, the replacement reads lock-free.
            assert responses._rlock.acquire(block=False)
            requests_queue: "queue.Queue[Any]" = queue.Queue()
            client = BrokerRpcClient(FakeVenue(), requests_queue, responses, "worker-00")

            def broker() -> None:
                item = requests_queue.get(timeout=3.0)
                responses.put({"request_id": item.request_id, "ok": True, "result": {"market": "A"}})

            thread = threading.Thread(target=broker, daemon=True)
            thread.start()
            assert await asyncio.to_thread(client.get_market, "A") == {"market": "A"}

        asyncio.run(scenario())
    finally:
        if client is not None:
            client.close_dispatcher()
        if thread is not None:
            thread.join(timeout=1.0)
        _close_queue(responses)


def test_read_queue_lockfree_reports_decode_errors_and_pipe_errors_distinctly():
    class Reader:
        def __init__(self, *, poll_error=None, data=b"garbage") -> None:
            self.poll_error = poll_error
            self.data = data

        def poll(self, timeout=0):
            if self.poll_error is not None:
                raise self.poll_error
            return True

        def recv_bytes(self):
            return self.data

    with pytest.raises(BrokerRequestDecodeError):
        read_queue_lockfree(SimpleNamespace(_reader=Reader(), _sem=None), 0.0)
    with pytest.raises(OSError):
        read_queue_lockfree(SimpleNamespace(_reader=Reader(poll_error=OSError("handle is invalid")), _sem=None), 0.0)
    plain: "queue.Queue[Any]" = queue.Queue()
    with pytest.raises(queue.Empty):
        read_queue_lockfree(plain, 0.0)
    plain.put("x")
    assert read_queue_lockfree(plain, 0.01) == "x"


# ----------------------------------------------------------------------
# (2) direct-client cleanup is bounded by the deadline; stop_all stays < 120 s


class AnsweringVenue:
    """Venue whose bot order disappears after ``visible_reads`` list calls."""

    def __init__(self, *, visible_reads: int) -> None:
        self.visible_reads = visible_reads
        self.reads = 0
        self.canceled: list[str] = []

    def list_account_orders(self, query):
        self.reads += 1
        if self.reads <= self.visible_reads:
            return [AccountOrder("bot-1", "MKT", client_order_id="mm:yes:1", status="resting")]
        return []

    def cancel_order(self, *, order_id: str):
        self.canceled.append(order_id)


def test_direct_cleanup_past_deadline_still_cancels_reported_orders_and_verifies_once():
    venue = AnsweringVenue(visible_reads=1)
    canceled = cancel_and_verify_owned_orders(venue, attempts=8, delay_seconds=0.5, sleep=lambda _s: None, deadline=time.monotonic() - 1.0)
    assert canceled == 1 and venue.canceled == ["bot-1"]
    assert venue.reads == 2          # the cancel pass + the single verification read


def test_direct_cleanup_past_deadline_reports_a_persisting_order_after_one_verification_read():
    venue = AnsweringVenue(visible_reads=99)
    with pytest.raises(Exception) as raised:
        cancel_and_verify_owned_orders(venue, attempts=8, delay_seconds=0.5, sleep=lambda _s: None, deadline=time.monotonic() - 1.0)
    assert raised.value.remaining_order_ids == ("bot-1",)
    assert venue.reads == 2 and venue.canceled == ["bot-1"]


def test_direct_cleanup_silent_venue_costs_one_read_once_the_deadline_passes():
    class SilentVenue:
        def __init__(self) -> None:
            self.reads = 0

        def list_account_orders(self, query):
            self.reads += 1
            raise requests.exceptions.ReadTimeout("venue silent")

    venue = SilentVenue()
    slept: list[float] = []
    deadline = time.monotonic() + 0.05

    def sleep(seconds: float) -> None:
        # Over-sleep: Windows' monotonic clock ticks every ~16 ms, so a sleep
        # of exactly the remaining budget need not move it past the deadline.
        slept.append(seconds)
        time.sleep(0.15)

    with pytest.raises(Exception, match="did not answer before the deadline") as raised:
        cancel_and_verify_owned_orders(venue, attempts=8, delay_seconds=0.5, sleep=sleep, deadline=deadline)
    # Attempt 0 ran and went unanswered; the backoff was clipped to the budget;
    # attempt 1 never started and no verification read followed.
    assert venue.reads == 1
    assert len(slept) == 1 and 0 < slept[0] <= 0.05 + 1e-9  # deadline arithmetic rounds in the last ulp
    assert isinstance(raised.value.__cause__, requests.exceptions.ReadTimeout)
    # Without a deadline the behaviour is unchanged: every attempt plus the final read.
    venue = SilentVenue()
    with pytest.raises(Exception, match="could not verify"):
        cancel_and_verify_owned_orders(venue, attempts=3, delay_seconds=0, sleep=lambda _s: None)
    assert venue.reads == 4


def test_shutdown_budget_worst_case_with_every_venue_call_timing_out_is_below_api_grace():
    from ui_api.store import OperationsStore

    api_grace = float(OperationsStore._windows_stop_timeout_seconds)
    read_timeout = 15.0
    # Every venue call = one 15 s read timeout.  Fence and freeze answer, the
    # broker cancel_all times out, the direct client fills the budget with
    # attempts, the attempt in flight at the deadline finishes, cleanup #2 is
    # skipped (venue unanswered + budget gone), the broker stop is skipped and
    # the join is immediate; escalations are the only extra.
    budget = manager_module.SHUTDOWN_BUDGET_SECONDS
    in_flight_at_deadline = manager_module.SHUTDOWN_VENUE_CALL_SECONDS
    escalations = 2 * manager_module.SHUTDOWN_ESCALATION_SECONDS
    assert budget + in_flight_at_deadline + escalations < api_grace
    # Even if cleanup #1 verified just before the deadline and the venue went
    # silent afterwards, cleanup #2 costs one post-budget read.
    assert budget + in_flight_at_deadline + manager_module.SHUTDOWN_POST_BUDGET_READ_SECONDS + escalations < api_grace
    assert manager_module.SHUTDOWN_POST_BUDGET_READ_SECONDS == read_timeout


def test_stop_all_skips_the_final_cleanup_when_budget_is_gone_and_the_venue_never_answered(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=True)
        manager.config = replace(manager.config, dry_run=False)
        manager.shutdown_budget_seconds = 1.0
        reads: list[float] = []

        class SilentClient:
            def list_account_orders(self, query):
                reads.append(time.monotonic())
                time.sleep(1.2)                     # a 15 s read timeout, scaled down
                raise requests.exceptions.ReadTimeout("venue silent")

        manager.cleanup_client = SilentClient()
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="final account cleanup skipped"):
            await manager.stop_all()
        assert time.monotonic() - started < 4.0
        assert len(reads) == 1                      # cleanup #1 only; cleanup #2 skipped
        assert admin.calls == []
        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "failed" and result["ordersVerifiedAbsent"] is False
        assert result["workersStopped"] and result["brokerStopped"]
        assert any("venue did not answer before the deadline" in item for item in result["warnings"])

    asyncio.run(scenario())


def test_stop_all_still_runs_the_final_cleanup_when_the_venue_answered_the_first(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=True)
        manager.config = replace(manager.config, dry_run=False)
        manager.shutdown_budget_seconds = 1.0
        reads = 0

        class SlowButAnsweringClient:
            def list_account_orders(self, query):
                nonlocal reads
                reads += 1
                if reads == 1:
                    time.sleep(1.2)                 # burns the budget, but answers
                return []

        manager.cleanup_client = SlowButAnsweringClient()
        await manager.stop_all()
        assert reads == 2                           # cleanup #2 verified past the budget
        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "verified" and result["ordersVerifiedAbsent"] is True

    asyncio.run(scenario())


def test_worker_recovery_direct_cleanup_is_deadline_bounded_per_market(tmp_path: Path):
    async def scenario():
        manager = ShardedBotManager(manager_config(tmp_path, dry_run=False))
        manager._admin = FakeAdmin(fail={"cancel_market"})
        manager._broker = FakeProcess(pid=500)
        manager._broker_started_at_ms = int(time.time() * 1000)
        manager._broker_last_seen_ms = int(time.time() * 1000)
        manager._assignments = {"worker-00": ("A", "B")}
        manager._desired = {t: _pick(t) for t in ("A", "B")}
        policies: list[tuple[str, manager_module.CleanupPolicy, float]] = []
        real_cancel_owned = manager._cancel_owned

        async def cancel_owned(market_id=""):
            policies.append((market_id, manager._cleanup_policy_state, time.monotonic()))
            return await real_cancel_owned(market_id)

        manager._cancel_owned = cancel_owned

        class DirectClient:
            def list_account_orders(self, query):
                return []

        manager.cleanup_client = DirectClient()
        manager._make_worker = lambda worker_id, response_queue, command_queue: FakeProcess(pid=700)
        managed = ManagedWorker("worker-00", FakeProcess(running=False), RecordingQueue(), queue.Queue())
        manager._workers = {"worker-00": managed}
        await manager._recover_worker("worker-00", managed)
        assert [item[0] for item in policies] == ["A", "B"]
        for _market, policy, at in policies:
            assert policy.direct_deadline is not None
            assert 0 < policy.direct_deadline - at <= manager_module.BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS + 0.5
        assert managed.phase == "reconciling"

    asyncio.run(scenario())


def test_worker_recovery_direct_cleanup_on_a_silent_venue_aborts_the_pass_after_one_market(tmp_path: Path):
    async def scenario():
        manager = ShardedBotManager(manager_config(tmp_path, dry_run=False))
        manager._admin = FakeAdmin(fail={"cancel_market"})
        manager._broker = FakeProcess(pid=500)
        manager._broker_started_at_ms = int(time.time() * 1000)
        manager._broker_last_seen_ms = int(time.time() * 1000)
        manager._assignments = {"worker-00": ("A", "B", "C")}
        manager._desired = {t: _pick(t) for t in ("A", "B", "C")}
        manager.config = replace(manager.config, shutdown_cleanup_attempts=2)
        reads: list[str] = []

        class SilentClient:
            def list_account_orders(self, query):
                reads.append(query.market_id)
                raise requests.exceptions.ReadTimeout("venue silent")

        manager.cleanup_client = SilentClient()
        spawned: list[str] = []
        manager._make_worker = lambda worker_id, response_queue, command_queue: spawned.append(worker_id) or FakeProcess(pid=700)
        managed = ManagedWorker("worker-00", FakeProcess(running=False), RecordingQueue(), queue.Queue())
        manager._workers = {"worker-00": managed}
        started = time.monotonic()
        await manager._recover_worker("worker-00", managed)
        assert time.monotonic() - started < 5.0
        assert set(reads) == {"A"} and spawned == []
        assert managed.phase == "cleanup_pending" and managed.recovery_retry_at_ms > 0
        event = await manager.events.get()
        assert event.event_type == "worker_recovery_failed" and "order cleanup" in event.detail["error"]

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# (3) admission is retried only once the replacement broker has reported


def test_admission_retry_waits_for_the_replacement_broker_to_report(tmp_path: Path):
    async def scenario():
        manager, broker, commands, replacements = stalled_manager(tmp_path)
        manager._planning_only = False
        admissions: list[int] = []
        planning = ShardedBotManager(manager_config(tmp_path))

        async def admission(picks):
            admissions.append(len(picks))
            return await planning._admission(picks)

        manager._admission = admission
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now - 120_000,
            "consecutive_timeouts": 5, "pending": 40, "in_flight": 0, "dropped_stale": 12,
        })
        await manager.monitor_once()
        assert len(replacements) == 1 and manager._admission_pending
        assert admissions == []                     # not against a just-spawned broker
        assert not manager._broker_has_reported()
        await manager.monitor_once()
        assert admissions == []
        # A startup_error is not "reported".
        manager._broker_status_queue.put({"type": "startup_error", "at_ms": int(time.time() * 1000), "error": "cleanup failed"})
        await manager.monitor_once()
        assert admissions == [] and not manager._broker_has_reported()
        now = int(time.time() * 1000)
        manager._broker_status_queue.put({
            "type": "heartbeat", "at_ms": now, "last_success_at_ms": now,
            "consecutive_timeouts": 0, "pending": 0, "in_flight": 0, "dropped_stale": 0,
        })
        await manager.monitor_once()
        assert manager._broker_has_reported()
        assert admissions == [1] and not manager._admission_pending
        events = []
        while not manager.events.empty():
            events.append((await manager.events.get()).event_type)
        assert "admission_recovered" in events

    asyncio.run(scenario())


def test_capacity_message_counts_as_the_broker_reporting(tmp_path: Path):
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._broker_started_at_ms = int(time.time() * 1000)
    assert not manager._broker_has_reported()
    manager._ingest_broker_status({"type": "capacity", "at_ms": int(time.time() * 1000), "limits": None})
    assert manager._broker_has_reported()
    # A restart resets it for the new generation.
    manager._broker_reported_at_ms = 0
    manager._broker_started_at_ms = int(time.time() * 1000)
    assert not manager._broker_has_reported()


# ----------------------------------------------------------------------
# (4) a worker busy with a control request is owed the re-issue


def test_reissue_owed_to_a_busy_worker_is_sent_when_its_control_acks(tmp_path: Path):
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._broker_started_at_ms = int(time.time() * 1000)
    manager._control_ack_queue = queue.Queue()
    manager._desired = {"A": _pick("A"), "B": _pick("B")}
    manager._assignments = {"worker-00": ("A", "B"), "worker-01": ()}
    busy_commands = RecordingQueue()
    idle_commands = RecordingQueue()
    busy = ManagedWorker(
        "worker-00", FakeProcess(pid=1), busy_commands, queue.Queue(), phase="reconciling",
        pending_request_id="in-flight", pending_action="reconcile",
        control_deadline_ms=int(time.time() * 1000) + 300_000,
    )
    idle = ManagedWorker("worker-01", FakeProcess(pid=2), idle_commands, queue.Queue(), phase="healthy")
    manager._workers = {"worker-00": busy, "worker-01": idle}
    manager._reconcile_after_broker_ready = True
    now = int(time.time() * 1000)
    manager._ingest_broker_status({"type": "heartbeat", "at_ms": now, "last_success_at_ms": now, "consecutive_timeouts": 0})
    assert idle_commands.actions() == ["reconcile"]
    assert busy_commands.items == [] and busy.reconcile_owed
    assert not manager._reconcile_after_broker_ready
    # Later heartbeats do not touch it; the ack of the in-flight request does.
    manager._ingest_broker_status({"type": "heartbeat", "at_ms": now + 1, "last_success_at_ms": now, "consecutive_timeouts": 0})
    assert busy_commands.items == []
    manager._control_ack_queue.put(WorkerControlAck("worker-00", "in-flight", "reconcile", True, now + 2))
    manager._drain_control_acks()
    assert busy_commands.actions() == ["reconcile"]
    assert [pick.market_id for pick in busy_commands.items[0]["picks"]] == ["A", "B"]
    assert not busy.reconcile_owed and busy.pending_action == "reconcile" and busy.phase == "reconciling"
    # The owed re-issue acks like any other; nothing further is sent.
    manager._control_ack_queue.put(WorkerControlAck("worker-00", busy.pending_request_id, "reconcile", True, now + 3))
    manager._drain_control_acks()
    assert busy_commands.actions() == ["reconcile"] and busy.phase == "healthy"


def test_owed_reissue_is_dropped_when_the_worker_fails_its_reconcile_or_the_fleet_stops(tmp_path: Path):
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._control_ack_queue = queue.Queue()
    manager._desired = {"A": _pick("A")}
    manager._assignments = {"worker-00": ("A",)}
    commands = RecordingQueue()
    busy = ManagedWorker(
        "worker-00", FakeProcess(pid=1), commands, queue.Queue(), phase="reconciling",
        pending_request_id="in-flight", pending_action="reconcile", control_deadline_ms=1,
    )
    manager._workers = {"worker-00": busy}
    manager._reissue_assignments()
    assert busy.reconcile_owed and commands.items == []
    # A failed reconcile sends the worker into recovery, which reconciles the replacement itself.
    manager._control_ack_queue.put(WorkerControlAck("worker-00", "in-flight", "reconcile", False, 1, "boom"))
    manager._drain_control_acks()
    assert busy.recovering and not busy.reconcile_owed and commands.items == []
    # A superseding reconcile (screener refresh) clears the debt too.
    busy.recovering = False
    busy.pending_request_id = "x"
    busy.reconcile_owed = True
    manager._send_control(busy, "reconcile", picks=())
    assert not busy.reconcile_owed
    # During shutdown nothing is re-issued.
    busy.reconcile_owed = True
    manager.begin_shutdown()
    manager._control_ack_queue.put(WorkerControlAck("worker-00", busy.pending_request_id, "reconcile", True, 2))
    manager._drain_control_acks()
    assert not busy.reconcile_owed and commands.actions() == ["reconcile"]


# ----------------------------------------------------------------------
# (5) an abandoned request is not placed after _cancel_prior


class SlowCancelVenue(OrderVenue):
    def __init__(self) -> None:
        super().__init__()
        self.slow_cancel = threading.Event()
        self.slow_cancel.set()
        self.reject_cancel: set[str] = set()

    def cancel_order(self, *, order_id: str):
        self.slow_cancel.wait(timeout=5.0)
        if order_id in self.reject_cancel:
            with self.lock:
                self.canceled.append(order_id)
            raise ClientError("order is not resting")
        return super().cancel_order(order_id=order_id)


def test_request_abandoned_during_cancel_prior_is_not_placed():
    broker = make_broker(execute_timeout_seconds=0.2)
    venue = SlowCancelVenue()
    broker._registry.set(("MKT", "yes"), SimpleNamespace(order_id="EXISTING"))
    venue.slow_cancel.clear()
    try:
        with pytest.raises(TimeoutError):
            broker._execute_bounded(venue, create_request())
        venue.slow_cancel.set()
        deadline = time.monotonic() + 3.0
        while "EXISTING" not in venue.canceled and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.2)
        assert venue.canceled == ["EXISTING"]
        assert venue.created == []                  # no order placed for a requester that is gone
        # The side is not wedged: the next create goes through.
        result = broker._execute_bounded(venue, create_request())
        assert result.order_id == "O1" and venue.created == ["O1"]
    finally:
        venue.slow_cancel.set()
        broker._get_executor().shutdown(wait=True)


def test_abandoned_amend_is_not_sent():
    broker = make_broker()
    venue = OrderVenue()
    abandoned = threading.Event()
    abandoned.set()
    intent = create_request().payload["intent"]
    amend = request("quote_intent", {"action": "amend_order", "request": SimpleNamespace(market_id="MKT", side="yes"), "intent": intent})
    with pytest.raises(TimeoutError, match="abandoned amend_order"):
        broker._execute(venue, amend, abandoned=abandoned)
    assert venue.created == []


# ----------------------------------------------------------------------
# (6) orphans are never dropped on a failed cancel


def test_failed_orphan_cancel_keeps_every_untried_orphan_and_drops_rejected_ones():
    broker = make_broker()
    venue = SlowCancelVenue()
    key = ("MKT", "yes")
    for order_id in ("A", "B", "C"):
        broker._registry.add_orphan(key, order_id)
    venue.fail_cancel.add("A")                      # transport failure on the first orphan
    with pytest.raises(requests.exceptions.ReadTimeout):
        broker._execute(venue, create_request())
    assert broker._registry.orphans(key) == ["A", "B", "C"]     # B and C were never attempted: kept
    assert venue.created == []
    venue.fail_cancel.clear()
    venue.fail_cancel.add("B")                      # now the second one fails
    with pytest.raises(requests.exceptions.ReadTimeout):
        broker._execute(venue, create_request())
    assert broker._registry.orphans(key) == ["B", "C"]          # A cancelled; B (failed) and C (untried) kept
    venue.fail_cancel.clear()
    venue.reject_cancel.add("B")                    # venue says B is no longer resting: dropped, not a wedge
    result = broker._execute(venue, create_request())
    assert result.order_id == "O1"
    assert broker._registry.orphans(key) == []
    assert venue.canceled.count("C") == 1 and venue.canceled.count("B") == 2


# ----------------------------------------------------------------------
# (7) persistent reader errors do not spin the broker loop


def test_broker_reader_errors_back_off_and_end_the_broker_once_persistent(monkeypatch):
    from fleet_runtime import execution as execution_module

    sleeps: list[float] = []
    monkeypatch.setattr(execution_module.time, "sleep", sleeps.append)

    class BrokenReader:
        def __init__(self) -> None:
            self.polls = 0

        def poll(self, timeout=0):
            self.polls += 1
            raise OSError("The handle is invalid")

        def recv_bytes(self):  # pragma: no cover - never reached
            raise AssertionError

    broken = SimpleNamespace(_reader=BrokenReader(), _sem=None)
    broker = make_broker(request_queue=broken)
    broker.READER_ERROR_BACKOFF_SECONDS = 0.001
    broker.READER_ERROR_LIMIT = 5
    outcomes = []
    for _ in range(4):
        outcomes.append(broker._read_request(0.0))
    assert outcomes == [(None, "reader_error")] * 4
    assert broker._reader_errors == 4
    assert sleeps == [0.001] * 4                        # backed off every time, did not spin
    with pytest.raises(BrokerQueueUnreadable, match="5 consecutive read errors"):
        broker._read_request(0.0)
    assert broken._reader.polls == 5
    assert sleeps == [0.001] * 4                        # the fatal read does not sleep

    # A successful read (or a decode error) resets the run.
    class FlakyReader:
        def __init__(self) -> None:
            self.calls = 0

        def poll(self, timeout=0):
            self.calls += 1
            if self.calls % 2:
                raise OSError("transient")
            return True

        def recv_bytes(self):
            return b"not a pickle"

    flaky = SimpleNamespace(_reader=FlakyReader(), _sem=None)
    broker = make_broker(request_queue=flaky)
    broker.READER_ERROR_BACKOFF_SECONDS = 0.001
    broker.READER_ERROR_LIMIT = 2
    for _ in range(6):
        outcome = broker._read_request(0.0)[1]
        assert outcome in {"reader_error", "decode_error"}
    assert broker._reader_errors == 0

    # Thread queues: the ordinary outcomes.
    broker = make_broker()
    assert broker._read_request(0.0) == (None, "empty")
    broker.request_queue.put(request("ping"))
    item, outcome = broker._read_request(0.0)
    assert outcome == "request" and item.operation == "ping"
    broker.request_queue.put("not a request")
    assert broker._read_request(0.0) == (None, "decode_error")


# ----------------------------------------------------------------------
# (8) a skipped broker stop terminates the broker immediately


def test_stop_all_terminates_the_broker_immediately_when_the_stop_command_was_skipped(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=False)
        manager.config = replace(manager.config, dry_run=False)
        admin.fail.add("cancel_all")                # marks the broker stalled -> stop skipped

        class DirectClient:
            def list_account_orders(self, query):
                return []

        manager.cleanup_client = DirectClient()
        graces: list[tuple[Any, float]] = []
        real_terminate = manager._terminate_process

        async def terminate(target, *, graceful_timeout=0.0):
            graces.append((target, graceful_timeout))
            return await real_terminate(target, graceful_timeout=graceful_timeout)

        manager._terminate_process = terminate
        await manager.stop_all()
        assert [call[0] for call in admin.calls] == ["quiesce_all", "cancel_all"]
        broker_graces = [grace for target, grace in graces if target is manager._broker]
        assert broker_graces and broker_graces[0] == 0.0
        assert manager.status_snapshot()["monitoring"]["shutdownCleanup"]["state"] == "verified"

    asyncio.run(scenario())


def test_stop_all_keeps_the_graceful_broker_join_after_an_acknowledged_stop(tmp_path: Path):
    async def scenario():
        manager, process, admin, commands = shutdown_manager(tmp_path, stalled=False)

        async def cleanup(market_id=""):
            return 0

        manager._cancel_owned = cleanup
        graces: list[tuple[Any, float]] = []
        real_terminate = manager._terminate_process

        async def terminate(target, *, graceful_timeout=0.0):
            graces.append((target, graceful_timeout))
            return await real_terminate(target, graceful_timeout=graceful_timeout)

        manager._terminate_process = terminate
        await manager.stop_all()
        assert [call[0] for call in admin.calls] == ["quiesce_all", "stop"]
        broker_graces = [grace for target, grace in graces if target is manager._broker]
        assert broker_graces and 0 < broker_graces[0] <= manager_module.SHUTDOWN_BROKER_GRACE_SECONDS

    asyncio.run(scenario())
