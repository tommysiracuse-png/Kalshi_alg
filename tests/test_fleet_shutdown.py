from __future__ import annotations

import asyncio
import queue
import time
from pathlib import Path

import pytest

from bot_manager import BotManagerConfig
from clients.models import AccountOrder
from fleet_models import IntentUrgency, QuoteIntent, WorkerControlAck, WorkerHeartbeat
from fleet_runtime.execution import (
    BotOrderCleanupError,
    BrokerRequest,
    ExecutionBrokerProcess,
    cancel_and_verify_owned_orders,
)
from fleet_runtime.manager import ManagedWorker, ShardedBotManager
from fleet_runtime.worker import watchdog_exit_allowed


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


class DelayedVisibilityClient:
    def __init__(self, *, stale_reads: int, persistent: bool = False) -> None:
        self.stale_reads = stale_reads
        self.persistent = persistent
        self.cancel_calls: list[str] = []
        self.bot_order = AccountOrder(
            "bot-order", "MKT", client_order_id="mm:yes:generation", status="resting"
        )
        self.manual_order = AccountOrder(
            "manual-order", "MKT", client_order_id="operator", status="resting"
        )

    def list_account_orders(self, query):
        orders = [self.manual_order]
        if self.persistent or len(self.cancel_calls) <= self.stale_reads:
            orders.insert(0, self.bot_order)
        return orders

    def cancel_order(self, *, order_id: str):
        self.cancel_calls.append(order_id)


def test_cleanup_retries_until_canceled_order_disappears_and_preserves_manual_order():
    client = DelayedVisibilityClient(stale_reads=2)

    canceled = cancel_and_verify_owned_orders(client, attempts=3, delay_seconds=0)

    assert canceled == 1
    assert client.cancel_calls == ["bot-order", "bot-order", "bot-order"]


def test_cleanup_reports_persistent_bot_order_ids():
    client = DelayedVisibilityClient(stale_reads=0, persistent=True)

    with pytest.raises(BotOrderCleanupError) as raised:
        cancel_and_verify_owned_orders(client, attempts=3, delay_seconds=0)

    assert raised.value.remaining_order_ids == ("bot-order",)
    assert "bot-order" in str(raised.value)
    assert "manual-order" not in client.cancel_calls


def test_quiesced_worker_writes_are_blocked_but_cancellation_is_allowed():
    intent = QuoteIntent(
        "MKT", "yes", 4_000, 100, 1, IntentUrgency.NORMAL, 1, 20_000,
    )
    create = BrokerRequest("create", "worker-00", "quote_intent", {"intent": intent}, 1)
    cancel = BrokerRequest("cancel", "worker-00", "cancel_order", {"order_id": "order"}, 1)
    controller = BrokerRequest("admin", "controller", "quote_intent", {"intent": intent}, 1)

    assert ExecutionBrokerProcess._write_blocked(
        create, all_workers_quiesced=False, quiesced_channels={"worker-00"}
    )
    assert not ExecutionBrokerProcess._write_blocked(
        cancel, all_workers_quiesced=False, quiesced_channels={"worker-00"}
    )
    assert not ExecutionBrokerProcess._write_blocked(
        controller, all_workers_quiesced=True, quiesced_channels=set()
    )


def test_worker_freeze_blocks_watchdog_exit_orders():
    assert watchdog_exit_allowed(mode="flatten_only", position_units=100, shutdown_frozen=False)
    assert not watchdog_exit_allowed(mode="flatten_only", position_units=100, shutdown_frozen=True)


class FakeProcess:
    def __init__(self, *, running: bool = True, pid: int = 100) -> None:
        self.running = running
        self.pid = pid
        self.started = False

    def is_alive(self) -> bool:
        return self.running

    def start(self) -> None:
        self.started = True
        self.running = True

    def join(self, timeout=None) -> None:
        return None

    def terminate(self) -> None:
        self.running = False

    def kill(self) -> None:
        self.running = False


class FakeBroker(FakeProcess):
    pass


class RecordingQueue:
    def __init__(self) -> None:
        self.items: list[dict] = []

    def put(self, value) -> None:
        self.items.append(value)


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
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def call(self, operation, payload=None, timeout=60.0):
        self.calls.append((operation, payload))
        return 0


def test_reconciliation_grace_suppresses_stale_worker_recovery(tmp_path: Path):
    async def scenario():
        manager = ShardedBotManager(manager_config(tmp_path))
        manager._started = True
        manager._broker = FakeBroker()
        manager._broker_status_queue = queue.Queue()
        manager._heartbeat_queue = queue.Queue()
        manager._control_ack_queue = queue.Queue()
        process = FakeProcess()
        managed = ManagedWorker(
            "worker-00",
            process,
            RecordingQueue(),
            queue.Queue(),
            heartbeat=WorkerHeartbeat("worker-00", (), {}, 0, 0, 0, 1),
            pending_action="reconcile",
            pending_request_id="pending",
            control_deadline_ms=int(time.time() * 1000) + 60_000,
        )
        manager._workers = {"worker-00": managed}
        recovered: list[str] = []

        async def recover(worker_id, _managed):
            recovered.append(worker_id)

        manager._recover_worker = recover
        await manager.monitor_once()
        assert recovered == []

    asyncio.run(scenario())


def test_failed_worker_cleanup_keeps_shard_offline(tmp_path: Path):
    async def scenario():
        manager = ShardedBotManager(manager_config(tmp_path, dry_run=False))
        manager._admin = FakeAdmin()
        manager._broker = FakeBroker()
        manager._assignments = {"worker-00": ("MKT",)}
        managed = ManagedWorker(
            "worker-00", FakeProcess(running=False), RecordingQueue(), queue.Queue()
        )
        manager._workers = {"worker-00": managed}

        async def fail_cleanup(market_id=""):
            raise BotOrderCleanupError(
                "still resting", remaining_order_ids=("order-1",)
            )

        manager._cancel_owned = fail_cleanup
        await manager._recover_worker("worker-00", managed)

        assert managed.phase == "cleanup_pending"
        assert managed.recovering is True
        assert managed.process.started is False
        event = await manager.events.get()
        assert event.event_type == "worker_recovery_failed"
        assert event.detail["remaining_order_ids"] == ["order-1"]

    asyncio.run(scenario())


def shutdown_manager(tmp_path: Path) -> tuple[ShardedBotManager, FakeProcess]:
    manager = ShardedBotManager(manager_config(tmp_path))
    manager._started = True
    manager._planning_only = True
    manager._control_ack_queue = queue.Queue()
    process = FakeProcess()
    commands = AutoAckQueue("worker-00", manager._control_ack_queue, process)
    manager._workers = {
        "worker-00": ManagedWorker("worker-00", process, commands, queue.Queue())
    }
    return manager, process


def test_transient_first_cleanup_failure_is_superseded_by_final_verification(tmp_path: Path):
    async def scenario():
        manager, process = shutdown_manager(tmp_path)
        calls = 0

        async def cleanup(market_id=""):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary venue lag")
            return 1

        manager._cancel_owned = cleanup
        await manager.stop_all()

        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "verified"
        assert result["ordersVerifiedAbsent"] is True
        assert result["workersStopped"] is True
        assert result["brokerStopped"] is True
        assert "temporary venue lag" in result["warnings"][0]
        assert not process.is_alive()

    asyncio.run(scenario())


def test_final_cleanup_failure_reports_failure_after_process_teardown(tmp_path: Path):
    async def scenario():
        manager, process = shutdown_manager(tmp_path)

        async def cleanup(market_id=""):
            raise BotOrderCleanupError(
                "venue still reports order", remaining_order_ids=("order-1",)
            )

        manager._cancel_owned = cleanup
        with pytest.raises(RuntimeError, match="final account cleanup"):
            await manager.stop_all()

        result = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert result["state"] == "failed"
        assert result["remainingBotOrderIds"] == ["order-1"]
        assert result["workersStopped"] is True
        assert result["brokerStopped"] is True
        assert not process.is_alive()

    asyncio.run(scenario())
