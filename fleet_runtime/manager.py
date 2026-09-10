"""Controller-side lifecycle for the sharded worker fleet."""

from __future__ import annotations

import asyncio
import dataclasses
import contextlib
import json
import multiprocessing as mp
import os
import queue
import shutil
import sys
import time
import uuid
try:
    import resource as _resource
except ImportError:  # Windows has no POSIX resource module.
    _resource = None  # type: ignore[assignment]
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

from core.bot_manager import BotManagerConfig
from clients.base_client import BaseClient
from clients.factory import build_client_config
from core.fleet_models import (
    BotManagerEvent,
    FleetCapacity,
    ScreenerPick,
    ScreenerUpdate,
    WorkerControlAck,
    WorkerHeartbeat,
)
from clients.models import StartupAccountSnapshot
from clients.monitoring import merge_activity_snapshots
from core.session_config import default_session_configuration, validate_session_configuration
from .assignment import assign_markets, derive_worker_count
from .capacity import AllocationRequest, AllocationResult, CapitalAllocator, calculate_fleet_capacity
from .execution import (
    BrokerRequest,
    ExecutionBrokerProcess,
    cancel_and_verify_owned_orders,
    is_venue_transport_failure,
    read_queue_lockfree,
    release_queue_reader_lock,
)
from .worker import FleetWorkerProcess

# ScreenerPick.selection_reason of a restart carryover: an exchange position
# the screener did not pick, run reduce-only (see screener.py).
EXCHANGE_POSITION_REASON = "exchange_position"


# Broker stall handling.  The broker process can be alive yet useless: every
# venue call wedged or timing out.  Its status heartbeat carries the last
# venue success and the consecutive-timeout count; silence or sustained
# failure restarts it.  Worker recovery is suppressed briefly afterwards, since
# a worker blocked on the old broker looks stale through no fault of its own.
BROKER_STALL_SECONDS = 45.0
BROKER_STARTUP_GRACE_SECONDS = 120.0
BROKER_STALL_TIMEOUTS = 3
BROKER_RESTART_MIN_INTERVAL_SECONDS = 60.0
BROKER_DEAD_RESTART_INTERVAL_SECONDS = 5.0
BROKER_RESTART_GRACE_SECONDS = 60.0
BROKER_ADMIN_STALLED_TIMEOUT_SECONDS = 10.0
# Worker recovery talks to the broker with this bound instead of the 60 s
# default: a slow broker must not hold monitor_once (and with it every
# heartbeat, stall verdict and status publish) for a minute per call.
BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS = 20.0
ADMISSION_RETRY_SECONDS = 30.0
# A live, heartbeating worker is never torn down merely because its reconcile
# is slow; only heartbeat silence or this hard cap (x startupTimeoutSeconds)
# does that.  Tearing down a working shard for slowness re-issues the very
# venue burst that made it slow (the 23:59 / 00:12 collapses).
RECONCILE_HARD_CAP_MULTIPLIER = 4.0

# Shutdown budget.  The dashboard API hard-kills the launcher 120 s after it
# signals a stop (ui_api/store.py: _windows_stop_timeout_seconds = 120), after
# which order cancellation is reported as NOT verified.  stop_all therefore
# runs against a wall-clock budget and every phase is clamped to what is left:
#
#   phase                                   cap (s)   note
#   broker write fence (quiesce_all)           10     skipped when broker unusable
#   worker freeze acknowledgements              5
#   account cleanup #1 via broker              15     then direct client, bounded by the deadline
#   worker stop acknowledgements                5
#   worker graceful join                       20     +7 s terminate/kill escalation
#   account cleanup #2 via broker              15     then direct client, bounded by the deadline
#   broker stop command                         5
#   broker graceful join                       10     only after an acknowledged stop; +7 s escalation
#   sum of caps                                85     clamped to SHUTDOWN_BUDGET_SECONDS overall
#
# The direct client (cancel_and_verify_owned_orders) gets the same deadline:
# past it no new attempt starts and at most ONE more venue read happens (the
# verification read after a cancel pass; skipped when the venue did not
# answer the pass).  Cleanup #2 is skipped outright when the budget is gone
# and cleanup #1 already failed to reach the venue.  Worst case therefore =
# SHUTDOWN_BUDGET_SECONDS + one venue call already in flight when the deadline
# passes (HTTP connect 10 s + read 15 s) + one post-budget verification read
# (15 s) + the two escalation windows (~7 s each) = ~114 s, inside the API's
# 120 s even when every venue call hits the 15 s read timeout.  A healthy
# fleet finishes in well under 30 s; the budget only bites when the venue or
# broker is degraded, and then the outcome is a bounded "not verified"
# instead of a hard kill mid-verification.
SHUTDOWN_BUDGET_SECONDS = 60.0
SHUTDOWN_VENUE_CALL_SECONDS = 25.0        # HTTP connect 10 s + read 15 s
SHUTDOWN_POST_BUDGET_READ_SECONDS = 15.0  # the single verification read past the deadline
SHUTDOWN_ESCALATION_SECONDS = 7.0         # terminate join 5 s + kill join 2 s, per process class
SHUTDOWN_FENCE_SECONDS = 10.0
SHUTDOWN_FREEZE_ACK_SECONDS = 5.0
SHUTDOWN_BROKER_CLEANUP_SECONDS = 15.0
SHUTDOWN_STOP_ACK_SECONDS = 5.0
SHUTDOWN_WORKER_GRACE_SECONDS = 20.0
SHUTDOWN_BROKER_STOP_SECONDS = 5.0
SHUTDOWN_BROKER_GRACE_SECONDS = 10.0


@dataclass
class ManagedMarket:
    pick: ScreenerPick
    worker_id: str


@dataclass
class ManagedWorker:
    worker_id: str
    process: FleetWorkerProcess
    command_queue: Any
    response_queue: Any
    heartbeat: Optional[WorkerHeartbeat] = None
    phase: str = "starting"
    last_error: str = ""
    pending_request_id: str = ""
    pending_action: str = ""
    control_deadline_ms: int = 0
    control_sent_at_ms: int = 0
    last_liveness_at_ms: int = 0
    # Process start time used to bound the period before the first worker
    # heartbeat reaches the manager.  Reconciliation acknowledgements can
    # arrive before the actor startup work is complete, so they are not a
    # substitute for this first liveness signal.
    started_at_ms: int = 0
    recovery_retry_at_ms: int = 0
    recovering: bool = False
    channel_quiesced: bool = False
    # A post-broker-restart re-issue that found this worker busy with another
    # control request; it is sent as soon as that request acknowledges.
    reconcile_owed: bool = False
    # Startup progress is tracked separately from heartbeat liveness.  A
    # worker may keep heartbeating while its command/reconcile path is wedged.
    startup_target_tickers: tuple[str, ...] = ()
    startup_pending_tickers: tuple[str, ...] = ()
    startup_attempts: Mapping[str, int] = dataclasses.field(default_factory=dict)
    startup_started_at_ms: int = 0
    startup_progress_at_ms: int = 0
    startup_error: str = ""
    worker_error: str = ""
    recovery_reason: str = ""
    last_heartbeat_received_at_ms: int = 0
    recovery_count: int = 0
    last_recovery_at_ms: int = 0


def _startup_snapshot_from_payload(payload: Mapping[str, Any]) -> StartupAccountSnapshot:
    """Convert broker account rows into a worker-safe, market-indexed snapshot."""
    positions: dict[str, list[Any]] = {}
    orders: dict[str, list[Any]] = {}
    for item in payload.get("positions") or ():
        market_id = str(getattr(item, "market_id", "") or "")
        if market_id:
            positions.setdefault(market_id, []).append(item)
    for item in payload.get("orders") or ():
        market_id = str(getattr(item, "market_id", "") or "")
        if market_id:
            orders.setdefault(market_id, []).append(item)
    return StartupAccountSnapshot(
        captured_at_ms=int(payload.get("captured_at_ms") or payload.get("positions_at_ms") or time.time() * 1000),
        positions_by_market={key: tuple(value) for key, value in positions.items()},
        orders_by_market={key: tuple(value) for key, value in orders.items()},
        positions_available=bool(payload.get("positions_available", True)),
        orders_available=bool(payload.get("orders_available", True)),
        positions_error=str(payload.get("positions_error") or ""),
        orders_error=str(payload.get("orders_error") or ""),
    )


@dataclass(frozen=True)
class CleanupPolicy:
    """How _cancel_owned may spend time: broker first, then the direct client."""

    via_broker: bool = True
    broker_timeout: Optional[float] = None      # None -> the 60 s default
    direct_deadline: Optional[float] = None     # time.monotonic() instant
    mark_stall_on_timeout: bool = False


class BrokerAdminError(RuntimeError):
    def __init__(self, message: str, *, detail: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message)
        self.detail = dict(detail or {})


def thread_budget_snapshot(
    *, worker_count: int, worker_io_threads: int, adaptor_threads: int = 0,
) -> dict[str, int | None]:
    """Estimate task capacity before spawning a multi-venue fleet.

    The estimate intentionally includes only persistent runtime threads.  The
    bounded worker I/O pool, RPC dispatcher, heartbeat fallback, and broker
    dispatcher/executor are all long-lived; short-lived HTTP pools are capped
    separately by their adaptor.
    """
    current = 0
    limit: int | None = None
    try:
        with open("/sys/fs/cgroup/pids.current", "r", encoding="utf-8") as handle:
            current = int(handle.read().strip())
        with open("/sys/fs/cgroup/pids.max", "r", encoding="utf-8") as handle:
            raw_limit = handle.read().strip()
        if raw_limit != "max":
            limit = int(raw_limit)
    except (OSError, ValueError):
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as handle:
                values = {
                    line.split(":", 1)[0]: line.split(":", 1)[1].strip()
                    for line in handle if ":" in line
                }
            current = int(values.get("Threads", "0"))
        except (OSError, ValueError):
            current = 0
    if limit is None and _resource is not None:
        try:
            soft_limit, _hard_limit = _resource.getrlimit(_resource.RLIMIT_NPROC)
            if soft_limit != _resource.RLIM_INFINITY:
                limit = int(soft_limit)
        except (AttributeError, OSError, ValueError):
            pass
    # Per worker: bounded I/O pool + broker response dispatcher + heartbeat
    # fallback + one request and one response Queue feeder.  The execution
    # broker has four executor workers and one dispatcher; reserve three more
    # controller/queue tasks.  Heartbeat/control/command channels use
    # SimpleQueue and therefore do not add feeder threads.
    estimated_additional = int(worker_count) * (
        int(worker_io_threads) + 4 + max(0, int(adaptor_threads))
    ) + 8
    estimated_total = current + estimated_additional
    return {
        "current": current,
        "limit": limit,
        "estimatedAdditional": estimated_additional,
        "estimatedTotal": estimated_total,
        "headroom": max(0, limit - estimated_total) if limit is not None else None,
    }


def validate_host_resources(
    *, strict: bool, worker_count: int = 0, worker_io_threads: int = 8,
    adaptor_threads: int = 0,
) -> list[str]:
    warnings: list[str] = []
    cpu_count = os.cpu_count() or 0
    if cpu_count < 8:
        warnings.append(f"host has {cpu_count} vCPU; 8 required")
    memory_bytes = 0
    if sys.platform == "win32":
        try:
            import ctypes

            class _MemoryStatusEx(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            status = _MemoryStatusEx()
            status.dwLength = ctypes.sizeof(_MemoryStatusEx)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
                memory_bytes = int(status.ullTotalPhys)
            else:
                warnings.append("host memory could not be determined")
        except Exception:
            warnings.append("host memory could not be determined")
    else:
        try:
            with open("/proc/meminfo", "r", encoding="utf-8") as handle:
                values = {line.split(":", 1)[0]: line.split(":", 1)[1].strip() for line in handle if ":" in line}
            memory_bytes = int(values.get("MemTotal", "0 kB").split()[0]) * 1024
        except (OSError, ValueError):
            warnings.append("host memory could not be determined")
    # if memory_bytes and memory_bytes < 32 * 1024**3:
    #     warnings.append(f"host has {memory_bytes / 1024**3:.1f} GiB RAM; 32 GiB required")
    free = shutil.disk_usage(Path.cwd()).free
    if free < 100 * 1024**3:
        warnings.append(f"host has {free / 1024**3:.1f} GiB free; 100 GiB required")
    budget = thread_budget_snapshot(
        worker_count=worker_count,
        worker_io_threads=worker_io_threads,
        adaptor_threads=adaptor_threads,
    )
    limit = budget.get("limit")
    if limit is not None and int(budget["estimatedTotal"] or 0) > int(limit):
        warnings.append(
            "fleet requires approximately "
            f"{budget['estimatedAdditional']} additional tasks but only "
            f"{max(0, int(limit) - int(budget['current'] or 0))} are available"
        )
    if strict and warnings:
        raise RuntimeError("fleet host validation failed: " + "; ".join(warnings))
    return warnings


class _BrokerAdmin:
    def __init__(self, request_queue: Any, response_queue: Any) -> None:
        self.request_queue = request_queue
        self.response_queue = response_queue
        self._lock = asyncio.Lock()

    def _call_sync(self, operation: str, payload: Optional[Mapping[str, Any]] = None, timeout: float = 60.0) -> Any:
        request_id = uuid.uuid4().hex
        self.request_queue.put(BrokerRequest(request_id, "controller", operation, dict(payload or {}), int(time.time() * 1000)))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                response = self.response_queue.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                break
            if response.get("request_id") != request_id:
                continue
            if not response.get("ok"):
                raise BrokerAdminError(
                    str(response.get("error") or f"broker {operation} failed"),
                    detail=response.get("error_detail"),
                )
            return response.get("result")
        raise TimeoutError(f"broker {operation} timed out")

    async def call(self, operation: str, payload: Optional[Mapping[str, Any]] = None, timeout: float = 60.0) -> Any:
        async with self._lock:
            return await asyncio.to_thread(self._call_sync, operation, payload, timeout)


class ShardedBotManager:
    """Drop-in launcher manager backed by workers rather than ticker children."""

    def __init__(self, config: BotManagerConfig, cleanup_client: Optional[BaseClient] = None) -> None:
        self.config = config
        self.cleanup_client = cleanup_client
        self.configuration = validate_session_configuration(config.session_configuration or default_session_configuration())
        self.fleet_config = self.configuration["fleetRuntime"]
        configured_max = int(self.configuration["launcher"]["maxBots"])
        venue_max = getattr(config, "venue_max_bots", None)
        self.max_bots = min(configured_max, int(venue_max)) if venue_max is not None else configured_max
        self._configured_manager_max_bots = self.max_bots
        self._system_capacity_limit = self.max_bots
        self.shard_size = int(self.fleet_config["shardSize"])
        self.worker_count = derive_worker_count(self.max_bots, self.shard_size)
        self._worker_slot_count = self.worker_count
        self.bots: dict[str, ManagedMarket] = {}
        self.events: "asyncio.Queue[BotManagerEvent]" = asyncio.Queue()
        self._desired: dict[str, ScreenerPick] = {}
        self._assignments: dict[str, tuple[str, ...]] = {}
        self._market_to_worker: dict[str, str] = {}
        # Run artifacts must continue resolving markets removed by later
        # screener generations.  This map is append-only for the run while
        # _market_to_worker remains the current live assignment.
        self._market_shard_history: dict[str, str] = {}
        self._workers: dict[str, ManagedWorker] = {}
        # Worker recovery includes bounded venue cleanup and can take longer
        # than one controller poll.  Keep it off the monitor path so status
        # publication and heartbeat draining continue while a shard recovers.
        self._recovery_tasks: dict[str, asyncio.Task[Any]] = {}
        self._started = False
        self._shutdown_started = False
        self._request_queue: Any = None
        self._heartbeat_queue: Any = None
        self._control_ack_queue: Any = None
        self._broker_status_queue: Any = None
        self._broker: Optional[ExecutionBrokerProcess] = None
        self._response_queues: dict[str, Any] = {}
        self._admin: Optional[_BrokerAdmin] = None
        self._control_results: dict[str, WorkerControlAck] = {}
        self.venue = str(getattr(config, "venue", None) or self.configuration.get("venue", "kalshi"))
        self._client_config: Optional[Any] = config.client_config
        self._capacity: Optional[FleetCapacity] = None
        self._allocation: Optional[AllocationResult] = None
        self._capacity_error: str = ""
        self._host_warnings: list[str] = []
        self._thread_budget: dict[str, int | None] = {}
        self._planning_only = bool(config.dry_run and not (config.api_key_id and config.private_key_path))
        # Broker liveness (see BROKER_* constants).  Tunables are instance
        # attributes so tests can shrink them.
        self.broker_stall_seconds = BROKER_STALL_SECONDS
        self.broker_startup_grace_seconds = BROKER_STARTUP_GRACE_SECONDS
        self.broker_restart_min_interval_seconds = BROKER_RESTART_MIN_INTERVAL_SECONDS
        self.broker_restart_grace_seconds = BROKER_RESTART_GRACE_SECONDS
        self.shutdown_budget_seconds = SHUTDOWN_BUDGET_SECONDS
        self._broker_started_at_ms = 0
        self._broker_last_seen_ms = 0
        self._broker_last_success_ms = 0
        self._broker_consecutive_timeouts = 0
        self._broker_stats: dict[str, Any] = {}
        self._broker_api_activity: dict[str, Any] = {}
        self._broker_api_errors: dict[str, Any] = {}
        self._broker_rpc_timeouts: list[int] = []
        self._broker_rpc_timeout_total = 0
        self._broker_restarted_at_ms = 0
        self._broker_restart_count = 0
        self._broker_stalled = False
        self._broker_stall_reason = ""
        self._broker_stall_flagged_at_ms = 0
        # Set by a broker restart; the live assignment is re-issued to the
        # workers once the replacement reports in (never per failed attempt).
        self._reconcile_after_broker_ready = False
        # Last heartbeat/capacity stamp from the CURRENT broker generation
        # (startup_error is not "reported").  Admission is only retried once
        # this is set: a just-spawned broker cannot answer, and the 90 s wait
        # would hold monitor_once (status publishing included) the whole time.
        self._broker_reported_at_ms = 0
        self._admission_pending = False
        self._admission_retry_at_ms = 0
        self._admission_retry_task: Optional[asyncio.Task[Any]] = None
        # Per-ticker state from the last admission snapshot: position exposure
        # (cash units) and signed position, plus the exchange shard of every
        # desired market.  They feed the broker's live-cap ledger
        # (shard_exposure_limits) and the reduce-only side of restart
        # carryovers (ScreenerPick.selection_reason == "exchange_position").
        self._position_exposure_by_ticker: dict[str, int] = {}
        self._position_units_by_ticker: dict[str, int] = {}
        # When the admission position snapshot was taken (the broker's stamp,
        # else the instant the controller asked): the ledger only re-baselines
        # a ticker from a snapshot at/after its last applied fill.
        self._position_snapshot_at_ms = 0
        self._startup_account_snapshot: Optional[StartupAccountSnapshot] = None
        self._startup_snapshot_lock = asyncio.Lock()
        # Restart carryover tickers granted only their position-reducing side.
        self._reduce_only_tickers: set[str] = set()
        self._shard_by_ticker: dict[str, int] = {}
        self._last_exposure_limits: dict[str, Any] = {}
        # The broker's own live-cap ledger as reported in its heartbeat.
        self._broker_shard_exposure: dict[str, Any] = {}
        # Carryover tickers the broker's ledger currently sees flat: dropped
        # from the workers' allowed sides until the next admission drops them.
        self._broker_flat_reduce_only: frozenset[str] = frozenset()
        self._cleanup_policy_state = CleanupPolicy()
        self._shutdown_cleanup: dict[str, Any] = {
            "state": "idle", "startedAtMs": None, "completedAtMs": None,
            "canceledOrders": 0, "ordersVerifiedAbsent": False, "error": None,
            "workersStopped": False, "brokerStopped": False,
            "remainingBotOrderIds": [], "warnings": [],
        }

    def set_system_capacity_limit(self, limit: int) -> None:
        """Set the fleet coordinator's current per-venue actor allowance.

        This is intentionally separate from venue admission.  The coordinator
        may lower this value for host pressure while the venue's API capacity
        remains unchanged.
        """

        self._system_capacity_limit = max(0, int(limit))
        target = min(self._configured_manager_max_bots, self._system_capacity_limit)
        # A zero allowance is represented by an empty update; keep one worker
        # as the valid assignment floor for compatibility with the worker
        # lifecycle code when a later update restores capacity.
        self.max_bots = max(1, target)
        self.worker_count = derive_worker_count(self.max_bots, self.shard_size)

    @property
    def current_picks(self) -> dict[str, ScreenerPick]:
        return dict(self._desired)

    def begin_shutdown(self) -> None:
        self._shutdown_started = True

    async def _emit(self, event_type: str, market_id: str, **detail: object) -> None:
        detail.setdefault("venue", self.venue)
        await self.events.put(BotManagerEvent(event_type, market_id, int(time.time() * 1000), detail, self.venue))

    def _artifact_root(self) -> Path:
        path = self.config.bot_artifacts_root or self.config.logs_directory.parent
        path = Path(path)
        return path.parent if path.name == "markets" else path

    def _make_worker(self, worker_id: str, response_queue: Any, command_queue: Any) -> FleetWorkerProcess:
        assert self._client_config is not None
        return FleetWorkerProcess(
            worker_id=worker_id,
            venue=self.venue,
            client_config=self._client_config,
            session_configuration=self.configuration,
            artifact_root=self._artifact_root(),
            broker_request_queue=self._request_queue,
            broker_response_queue=response_queue,
            command_queue=command_queue,
            heartbeat_queue=self._heartbeat_queue,
            control_ack_queue=self._control_ack_queue,
        )

    def _make_broker(self) -> ExecutionBrokerProcess:
        assert self._client_config is not None
        return ExecutionBrokerProcess(
            venue=self.venue,
            client_config=self._client_config,
            request_queue=self._request_queue,
            response_queues=self._response_queues,
            status_queue=self._broker_status_queue,
            write_utilization_limit=float(self.fleet_config["writeUtilizationLimit"]),
            cleanup_attempts=self.config.shutdown_cleanup_attempts,
            cleanup_delay_seconds=self.config.shutdown_cleanup_delay_seconds,
        )

    def _send_control(self, managed: ManagedWorker, action: str, **payload: Any) -> str:
        request_id = uuid.uuid4().hex
        managed.pending_request_id = request_id
        managed.pending_action = action
        if action == "reconcile":
            managed.reconcile_owed = False
        timeout_seconds = (
            float(self.fleet_config["startupTimeoutSeconds"])
            if action == "reconcile" else 5.0
        )
        now = int(time.time() * 1000)
        managed.control_sent_at_ms = now
        managed.control_deadline_ms = int(now + timeout_seconds * 1000)
        if action == "reconcile":
            if self.venue.lower() == "polymarket" and "startup_account_snapshot" not in payload:
                payload["startup_account_snapshot"] = self._startup_account_snapshot
            picks = tuple(payload.get("picks") or ())
            managed.startup_target_tickers = tuple(
                str(getattr(item, "market_id", "")) for item in picks
                if str(getattr(item, "market_id", ""))
            )
            managed.startup_started_at_ms = now if managed.startup_target_tickers else 0
            managed.startup_progress_at_ms = now if managed.startup_target_tickers else 0
            managed.startup_pending_tickers = managed.startup_target_tickers
            managed.startup_attempts = {}
            managed.startup_error = ""
            managed.worker_error = ""
        managed.phase = "reconciling" if action == "reconcile" else "quiescing" if action == "freeze" else "stopping"
        managed.command_queue.put({
            "request_id": request_id,
            "action": action,
            "queuedAtMs": now,
            **payload,
        })
        return request_id

    def _drain_control_acks(self) -> None:
        if self._control_ack_queue is None:
            return
        while True:
            try:
                ack = read_queue_lockfree(self._control_ack_queue, 0)
            except queue.Empty:
                break
            if not isinstance(ack, WorkerControlAck):
                continue
            if ack.action in {"freeze", "stop"}:
                self._control_results[ack.request_id] = ack
            managed = self._workers.get(ack.worker_id)
            if managed is None:
                continue
            managed.last_liveness_at_ms = max(managed.last_liveness_at_ms, ack.generated_at_ms)
            if managed.pending_request_id != ack.request_id:
                continue
            managed.pending_request_id = ""
            managed.pending_action = ""
            managed.control_deadline_ms = 0
            managed.control_sent_at_ms = 0
            managed.last_error = ack.error
            if ack.action == "reconcile":
                if ack.ok:
                    if managed.recovering:
                        managed.phase = "resume_pending"
                    else:
                        managed.phase = "starting" if managed.startup_target_tickers else "healthy"
                else:
                    managed.phase = "cleanup_pending"
                    managed.recovering = True
                    managed.recovery_retry_at_ms = 0
                    managed.recovery_reason = "reconcile_failed"
            elif ack.action == "freeze":
                managed.phase = "quiesced" if ack.ok else "quiesce_failed"
            if managed.reconcile_owed:
                self._settle_owed_reconcile(managed)

    def _settle_owed_reconcile(self, managed: ManagedWorker) -> None:
        """Send the re-issue a busy worker missed, now that its control acked."""
        if managed.pending_request_id or managed.recovering or self._shutdown_started \
                or not managed.process.is_alive() or managed.phase in {"quiesced", "quiesce_failed", "stopping"}:
            managed.reconcile_owed = False
            return
        tickers = self._assignments.get(managed.worker_id, ())
        self._send_control(
            managed,
            "reconcile",
            picks=tuple(self._desired[ticker] for ticker in tickers if ticker in self._desired),
        )

    async def _wait_for_control(self, request_id: str, timeout: float) -> Optional[WorkerControlAck]:
        deadline = time.monotonic() + max(0.0, timeout)
        while time.monotonic() < deadline:
            self._drain_control_acks()
            ack = self._control_results.pop(request_id, None)
            if ack is not None:
                return ack
            await asyncio.sleep(0.05)
        self._drain_control_acks()
        return self._control_results.pop(request_id, None)

    @staticmethod
    def _remaining_ids(error: BaseException) -> list[str]:
        if isinstance(error, BrokerAdminError):
            return [str(item) for item in error.detail.get("remaining_order_ids", [])]
        return [str(item) for item in getattr(error, "remaining_order_ids", ())]

    # ------------------------------------------------------------------
    # Broker liveness

    def _note_broker_timeout(self) -> None:
        now = int(time.time() * 1000)
        window_ms = int(self.broker_stall_seconds * 1000)
        self._broker_rpc_timeouts = [
            item for item in self._broker_rpc_timeouts if now - item <= window_ms
        ] + [now]
        self._broker_rpc_timeout_total += 1

    def broker_stall_reason(self, now_ms: Optional[int] = None) -> str:
        """Why the (alive) broker counts as stalled; empty when it looks healthy."""
        if self._broker is None or not self._broker.is_alive():
            return ""
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        stall_ms = int(self.broker_stall_seconds * 1000)
        if self._broker_last_seen_ms:
            silence_ms = now - self._broker_last_seen_ms
            if silence_ms > stall_ms:
                return f"no broker heartbeat for {silence_ms // 1000}s"
        elif self._broker_started_at_ms:
            silence_ms = now - self._broker_started_at_ms
            if silence_ms > max(stall_ms, int(self.broker_startup_grace_seconds * 1000)):
                return f"no broker heartbeat {silence_ms // 1000}s after start"
        if (
            self._broker_consecutive_timeouts >= BROKER_STALL_TIMEOUTS
            and self._broker_last_success_ms
            and now - self._broker_last_success_ms > stall_ms
        ):
            return (
                f"{self._broker_consecutive_timeouts} consecutive venue timeouts, "
                f"no venue success for {(now - self._broker_last_success_ms) // 1000}s"
            )
        window_ms = stall_ms
        recent = [item for item in self._broker_rpc_timeouts if now - item <= window_ms]
        if len(recent) >= 2:
            return f"{len(recent)} controller broker calls timed out"
        return ""

    def _broker_usable(self) -> bool:
        return bool(
            self._admin is not None
            and self._broker is not None
            and self._broker.is_alive()
            and not self._broker_stalled
            and not self.broker_stall_reason()
        )

    def _admin_timeout(self, healthy_timeout: float) -> float:
        if self._broker_stalled or self.broker_stall_reason():
            return min(float(healthy_timeout), BROKER_ADMIN_STALLED_TIMEOUT_SECONDS)
        return float(healthy_timeout)

    def _ingest_broker_status(self, message: Mapping[str, Any]) -> bool:
        """Apply one broker status message; returns whether capacity changed."""
        message_type = str(message.get("type") or "")
        at_ms = int(message.get("at_ms") or 0)
        if at_ms and self._broker_started_at_ms and at_ms < self._broker_started_at_ms - 1_000:
            # Left over from a broker that has since been replaced.
            return False
        if message_type == "heartbeat":
            self._broker_last_seen_ms = max(self._broker_last_seen_ms, at_ms)
            self._broker_reported_at_ms = max(self._broker_reported_at_ms, at_ms)
            self._broker_last_success_ms = max(
                self._broker_last_success_ms, int(message.get("last_success_at_ms") or 0)
            )
            self._broker_consecutive_timeouts = int(message.get("consecutive_timeouts") or 0)
            self._broker_stats = {
                key: message.get(key)
                for key in ("pending", "in_flight", "dropped_stale", "decode_errors", "reader_errors")
            }
            if isinstance(message.get("queue_wait_ms"), Mapping):
                self._broker_stats["queueWaitMs"] = dict(message["queue_wait_ms"])
            if isinstance(message.get("api_errors"), Mapping):
                self._broker_api_errors = dict(message["api_errors"])
            activity = message.get("api_activity")
            if isinstance(activity, Mapping):
                self._broker_api_activity = dict(activity)
            exposure = message.get("shard_exposure")
            flat_changed = False
            if isinstance(exposure, Mapping):
                self._broker_shard_exposure = dict(exposure)
                flat = frozenset(str(ticker) for ticker in (exposure.get("flat_reduce_only") or ()))
                if flat != self._broker_flat_reduce_only:
                    granted = self._allocation.sides_by_ticker if self._allocation else {}
                    # A carryover that went flat since the last broadcast loses
                    # its side now (heartbeat cadence) rather than at the next
                    # admission; the broker already refuses its creates.
                    flat_changed = any(ticker in granted for ticker in flat ^ self._broker_flat_reduce_only)
                    self._broker_flat_reduce_only = flat
            self._note_broker_ready()
            return flat_changed
        if message_type == "capacity":
            self._broker_last_seen_ms = max(self._broker_last_seen_ms, at_ms)
            self._broker_reported_at_ms = max(self._broker_reported_at_ms, at_ms)
            self._broker_last_success_ms = max(self._broker_last_success_ms, at_ms)
            self._broker_consecutive_timeouts = 0
            self._note_broker_ready()
            limits = message.get("limits")
            if limits is None:
                return False
            prior = self._capacity
            self._capacity = calculate_fleet_capacity(
                api_tier=limits.usage_tier,
                read_refill_rate=limits.read.refill_rate,
                write_refill_rate=limits.write.refill_rate,
                requested_markets=len(self._desired),
                cash_available_units=prior.cash_available_units if prior else 0,
                cash_committed_units=prior.cash_committed_units if prior else 0,
                freshness_seconds=float(self.fleet_config["quoteFreshnessSeconds"]),
                write_utilization_limit=float(self.fleet_config["writeUtilizationLimit"]),
                cash_reserve_fraction=float(self.fleet_config["cashReserveFraction"]),
            )
            self._capacity_error = self._capacity.error
            return True
        if message_type in {"capacity_error", "startup_error"}:
            self._broker_last_seen_ms = max(self._broker_last_seen_ms, at_ms)
            self._capacity_error = str(message.get("error") or "execution broker unavailable")
            return True
        return False

    def _drain_broker_status(self) -> bool:
        """Ingest every queued broker status message; True if capacity changed."""
        changed = False
        if self._broker_status_queue is None:
            return changed
        while True:
            try:
                message = read_queue_lockfree(self._broker_status_queue, 0)
            except queue.Empty:
                break
            if self._ingest_broker_status(message):
                changed = True
        return changed

    def _note_broker_ready(self) -> None:
        """The current broker's loop is up: re-issue the assignment once."""
        if not self._reconcile_after_broker_ready or self._shutdown_started:
            return
        # Polymarket must refresh admission first so the replacement workers
        # receive the shared account snapshot instead of falling back to one
        # account read per actor during broker recovery.
        if self.venue.lower() == "polymarket":
            return
        self._reconcile_after_broker_ready = False
        self._reissue_assignments()

    def _reissue_assignments(self) -> None:
        # Re-issue the live assignment: any actor whose start the old broker
        # swallowed is re-added by its worker (idempotent for running actors,
        # and it makes every pending add due immediately).  A worker busy with
        # another control request (a screener refresh mid-flight) is owed the
        # re-issue and gets it when that request acknowledges; a dead or
        # recovering worker is re-issued by its recovery instead.
        for worker_id, managed in self._workers.items():
            if not managed.process.is_alive() or managed.recovering:
                continue
            if managed.pending_request_id:
                managed.reconcile_owed = True
                continue
            tickers = self._assignments.get(worker_id, ())
            self._send_control(
                managed,
                "reconcile",
                picks=tuple(self._desired[ticker] for ticker in tickers if ticker in self._desired),
            )

    def _release_request_queue_lock(self) -> bool:
        """Free the request queue's reader lock if a dead broker took it down.

        ``multiprocessing.Queue.get`` holds ``_rlock`` while it polls, and a
        broker terminated at that moment never releases it.  The broker is the
        queue's only reader (and the current build reads the pipe lock-free),
        so releasing here can only ever free an orphaned lock.  Returns whether
        a lock was released.
        """
        return release_queue_reader_lock(self._request_queue)

    async def _restart_broker(self, reason: str, *, force: bool = False) -> bool:
        now = int(time.time() * 1000)
        interval = (
            BROKER_DEAD_RESTART_INTERVAL_SECONDS if force
            else self.broker_restart_min_interval_seconds
        )
        if self._broker_restarted_at_ms and now - self._broker_restarted_at_ms < interval * 1000:
            return False
        for managed in self._workers.values():
            managed.command_queue.put({
                "action": "enable_quoting", "enabled": False, "allocations": {},
                "queuedAtMs": int(time.time() * 1000),
            })
        self._capacity_error = f"execution broker {reason}; fleet is reduction-only during restart"
        previous = self._broker
        if previous is not None and previous.is_alive():
            # It is wedged: there is nothing graceful left to wait for.
            await self._terminate_process(previous)
        self._release_request_queue_lock()
        self._broker = self._make_broker()
        self._broker.start()
        self._broker_started_at_ms = int(time.time() * 1000)
        self._broker_last_seen_ms = 0
        self._broker_reported_at_ms = 0
        self._broker_last_success_ms = 0
        self._broker_consecutive_timeouts = 0
        self._broker_rpc_timeouts = []
        self._broker_stats = {}
        self._broker_api_activity = {}
        self._broker_api_errors = {}
        self._startup_account_snapshot = None
        self._broker_shard_exposure = {}
        self._broker_flat_reduce_only = frozenset()
        self._broker_restarted_at_ms = self._broker_started_at_ms
        self._broker_restart_count += 1
        self._broker_stalled = False
        self._broker_stall_reason = ""
        self._broker_stall_flagged_at_ms = 0
        # The replacement broker needs a fresh admission snapshot before new
        # exposure reopens; monitor_once retries it once the broker has
        # reported in (heartbeat/capacity), never against a just-spawned one.
        self._admission_pending = bool(self._desired) and not self._planning_only
        self._admission_retry_at_ms = 0
        # The assignment is re-issued when the replacement's first heartbeat
        # arrives (see _note_broker_ready), not here: a broker whose startup
        # keeps failing is retried every 5 s, and a reconcile per attempt made
        # every worker tear down and re-subscribe its websocket each time.
        self._reconcile_after_broker_ready = True
        await self._emit("broker_restarted", "*", pid=self._broker.pid, reason=reason)
        return True

    # ------------------------------------------------------------------

    @contextlib.contextmanager
    def _cleanup_policy(
        self,
        *,
        via_broker: bool = True,
        broker_timeout: Optional[float] = None,
        direct_deadline: Optional[float] = None,
        mark_stall_on_timeout: bool = False,
    ) -> Iterator[None]:
        """Scope a CleanupPolicy over the _cancel_owned calls inside the block."""
        previous = self._cleanup_policy_state
        self._cleanup_policy_state = CleanupPolicy(
            via_broker=via_broker,
            broker_timeout=broker_timeout,
            direct_deadline=direct_deadline,
            mark_stall_on_timeout=mark_stall_on_timeout,
        )
        try:
            yield
        finally:
            self._cleanup_policy_state = previous

    async def _cancel_owned(self, market_id: str = "") -> int:
        if self.config.dry_run:
            return 0
        policy = self._cleanup_policy_state
        errors: list[BaseException] = []
        if policy.via_broker and self._broker_usable():
            try:
                operation = "cancel_market" if market_id else "cancel_all"
                timeout = self._admin_timeout(
                    60.0 if policy.broker_timeout is None else float(policy.broker_timeout)
                )
                return int(await self._admin.call(
                    operation, {"market_id": market_id} if market_id else None, timeout=timeout,
                ) or 0)
            except Exception as exc:
                if isinstance(exc, TimeoutError):
                    self._note_broker_timeout()
                    if policy.mark_stall_on_timeout:
                        self._broker_stalled = True
                        self._broker_stall_reason = str(exc)
                errors.append(exc)
        if self.cleanup_client is not None:
            try:
                return await asyncio.to_thread(
                    cancel_and_verify_owned_orders,
                    self.cleanup_client,
                    market_id,
                    attempts=self.config.shutdown_cleanup_attempts,
                    delay_seconds=self.config.shutdown_cleanup_delay_seconds,
                    deadline=policy.direct_deadline,
                )
            except Exception as exc:
                errors.append(exc)
        if not errors:
            raise RuntimeError("no account cleanup client is available")
        remaining = tuple(sorted({item for error in errors for item in self._remaining_ids(error)}))
        detail = "; ".join(str(error) for error in errors)
        error = RuntimeError(detail)
        setattr(error, "remaining_order_ids", remaining)
        # Chain the last underlying error so callers can classify it (a venue
        # transport failure vs. orders that really remain).
        raise error from errors[-1]

    @staticmethod
    async def _join_process(process: Any, timeout: float) -> bool:
        # ``Process.join`` was previously dispatched through the event-loop
        # executor.  Besides consuming another thread during every recovery,
        # that left a worker in the executor while the loop was shutting down
        # on Python builds where the executor join is not promptly woken.
        # ``is_alive`` performs a non-blocking waitpid check, so poll it from
        # the event loop with a small cooperative sleep instead.
        deadline = time.monotonic() + max(0.0, float(timeout))
        while process.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        return not process.is_alive()

    @classmethod
    async def _terminate_process(cls, process: Any, *, graceful_timeout: float = 0.0) -> bool:
        if not process.is_alive():
            return True
        if graceful_timeout > 0 and await cls._join_process(process, graceful_timeout):
            return True
        try:
            process.terminate()
        except Exception:
            pass
        if await cls._join_process(process, 5.0):
            return True
        try:
            killer = getattr(process, "kill", process.terminate)
            killer()
        except Exception:
            pass
        return await cls._join_process(process, 2.0)

    async def _ensure_started(self) -> None:
        if self._started:
            return
        if self._shutdown_started:
            raise RuntimeError("fleet is shutting down")
        worker_io_threads = int(self.fleet_config.get("workerIoThreads", 8))
        self._thread_budget = thread_budget_snapshot(
            worker_count=self.worker_count,
            worker_io_threads=worker_io_threads,
            adaptor_threads=16 if self.venue == "polymarket" else 0,
        )
        self._host_warnings = validate_host_resources(
            strict=self.max_bots > 40 and not self.config.dry_run,
            worker_count=self.worker_count,
            worker_io_threads=worker_io_threads,
            adaptor_threads=16 if self.venue == "polymarket" else 0,
        )
        if self._planning_only:
            self._started = True
            return
        self._request_queue = mp.Queue()
        # These channels are one-way and have a single consumer.  SimpleQueue
        # writes directly to the pipe instead of starting one feeder thread
        # per producer process, which is important when both venues start
        # workers at the same time.
        self._heartbeat_queue = mp.SimpleQueue()
        self._control_ack_queue = mp.SimpleQueue()
        self._broker_status_queue = mp.SimpleQueue()
        response_queues: dict[str, Any] = {"controller": mp.Queue()}
        for index in range(self._worker_slot_count):
            response_queues[f"worker-{index:02d}"] = mp.Queue()
        if self._client_config is None:
            self._client_config = build_client_config(
                self.venue,
                api_key_id=self.config.api_key_id or "",
                private_key_path=self.config.private_key_path or "",
                public_only=not bool(self.config.api_key_id and self.config.private_key_path),
                use_demo_environment=self.config.use_demo,
                dry_run=self.config.dry_run,
                subaccount_number=int(self.config.subaccount or 0),
            )
        self._response_queues = response_queues
        self._broker = self._make_broker()
        self._broker.start()
        self._broker_started_at_ms = int(time.time() * 1000)
        self._admin = _BrokerAdmin(self._request_queue, response_queues["controller"])
        for index in range(self.worker_count):
            self._spawn_worker(index)
        self._started = True

    def _spawn_worker(self, index: int) -> None:
        worker_id = f"worker-{int(index):02d}"
        response_queue = self._response_queues[worker_id]
        existing = self._workers.get(worker_id)
        if existing is not None and existing.process.is_alive():
            return
        command_queue = mp.SimpleQueue()
        process = self._make_worker(worker_id, response_queue, command_queue)
        process.start()
        self._workers[worker_id] = ManagedWorker(
            worker_id, process, command_queue, response_queue,
            started_at_ms=int(time.time() * 1000),
        )

    def _ensure_worker_processes(self) -> None:
        """Add workers when the system cap grows after an earlier downscale."""

        if self._planning_only:
            return
        for index in range(self.worker_count):
            worker_id = f"worker-{index:02d}"
            # Lightweight manager doubles used by shutdown/admission tests may
            # install their own worker map without constructing multiprocessing
            # response queues.  Do not replace those test/runtime-managed
            # workers or manufacture a queue outside normal startup.
            if worker_id not in self._response_queues:
                continue
            self._spawn_worker(index)

    async def _admission(self, picks: Mapping[str, ScreenerPick]) -> tuple[FleetCapacity, AllocationResult]:
        if self._planning_only:
            capacity = calculate_fleet_capacity(
                api_tier="dry-run", read_refill_rate=300, write_refill_rate=300,
                requested_markets=len(picks), cash_available_units=10**15,
                freshness_seconds=float(self.fleet_config["quoteFreshnessSeconds"]),
                write_utilization_limit=float(self.fleet_config["writeUtilizationLimit"]),
                cash_reserve_fraction=float(self.fleet_config["cashReserveFraction"]),
            )
            available, committed = 10**15, 0
            exposure_by_ticker: dict[str, int] = {}
            position_exposure_by_ticker: dict[str, int] = {}
            position_units_by_ticker: dict[str, int] = {}
        else:
            if self._admin is None:
                raise RuntimeError("execution broker is unavailable for admission")
            # The broker bounds each venue call itself, so a stalled broker
            # answers with an error well inside this wait instead of holding
            # the controller loop for the whole startup window.
            asked_at_ms = int(time.time() * 1000)
            snapshot = await self._admin.call(
                "admission_snapshot",
                timeout=min(float(self.fleet_config["startupTimeoutSeconds"]), 90.0),
            )
            if self.venue.lower() == "polymarket" and isinstance(snapshot, Mapping):
                self._startup_account_snapshot = _startup_snapshot_from_payload(snapshot)
            # The broker stamps the positions with the instant its venue read
            # started; an older broker does not, and the instant we asked is
            # the latest moment that is certainly not after the read.
            snapshot_at_ms = int(snapshot.get("positions_at_ms") or 0) if isinstance(snapshot, Mapping) else 0
            self._position_snapshot_at_ms = snapshot_at_ms or asked_at_ms
            limits = snapshot["limits"]
            balance = snapshot["balance"]
            orders = snapshot["orders"]
            positions = snapshot["positions"]
            order_notional = sum(max(0, int(item.price_units or 0)) * max(0, int(item.remaining_count_units)) // 100 for item in orders)
            position_notional = sum(max(0, int(item.market_exposure_units or 0)) for item in positions)
            committed = order_notional + position_notional
            available = balance.available_cash_units
            # Live exposure per market (resting notional + position exposure):
            # the allocator caps it per exchange shard at un-multiplied
            # allocatable cash and serves these markets first when budgets are
            # oversubscribed. Presence of a key means the market holds an
            # order or a position even when its notional rounds to zero.
            exposure_by_ticker = {}
            position_exposure_by_ticker = {}
            position_units_by_ticker = {}
            for item in orders:
                ticker = str(getattr(item, "market_id", "") or "")
                if ticker:
                    notional = max(0, int(item.price_units or 0)) * max(0, int(item.remaining_count_units)) // 100
                    exposure_by_ticker[ticker] = exposure_by_ticker.get(ticker, 0) + notional
            for item in positions:
                ticker = str(getattr(item, "market_id", "") or "")
                exposure_units = max(0, int(item.market_exposure_units or 0))
                units = int(getattr(item, "position_units", 0) or 0)
                if ticker and (exposure_units > 0 or units != 0):
                    exposure_by_ticker[ticker] = exposure_by_ticker.get(ticker, 0) + exposure_units
                    position_exposure_by_ticker[ticker] = position_exposure_by_ticker.get(ticker, 0) + exposure_units
                    position_units_by_ticker[ticker] = position_units_by_ticker.get(ticker, 0) + units
            capacity = calculate_fleet_capacity(
                api_tier=limits.usage_tier, read_refill_rate=limits.read.refill_rate,
                write_refill_rate=limits.write.refill_rate, requested_markets=len(picks),
                cash_available_units=available, cash_committed_units=committed,
                freshness_seconds=float(self.fleet_config["quoteFreshnessSeconds"]),
                write_utilization_limit=float(self.fleet_config["writeUtilizationLimit"]),
                cash_reserve_fraction=float(self.fleet_config["cashReserveFraction"]),
            )
        requests = []
        shard_by_ticker: dict[str, int] = {}
        reduce_only_tickers: set[str] = set()
        for rank, pick in enumerate(picks.values()):
            ranking = pick.ranking
            series_id = str(ranking.get("Series Ticker") or ranking.get("series_ticker") or pick.market_id.split("-")[0])
            first_side = "yes" if pick.yes_budget_cents >= pick.no_budget_cents else "no"
            first_budget = pick.yes_budget_cents if first_side == "yes" else pick.no_budget_cents
            second_budget = pick.no_budget_cents if first_side == "yes" else pick.yes_budget_cents
            try:
                exchange_index = int(ranking.get("Exchange Index") or ranking.get("exchange_index") or 0)
            except (TypeError, ValueError):
                exchange_index = 0
            shard_by_ticker[pick.market_id] = exchange_index
            if str(getattr(pick, "selection_reason", "") or "") == EXCHANGE_POSITION_REASON:
                # Restart carryover of an exchange position the screener did
                # not pick: the bot may only quote the side that reduces the
                # position (no capital needed, no other side).  Flat or
                # unknown at the venue -> no side at all until the next
                # refresh drops it.
                units = position_units_by_ticker.get(pick.market_id, 0)
                if units == 0:
                    continue
                reducing: tuple[Any, ...] = ("no",) if units > 0 else ("yes",)
                reduce_only_tickers.add(pick.market_id)
                requests.append(AllocationRequest(
                    pick.market_id, series_id, rank, first_side, 0, 0,
                    reducing_sides=reducing, exchange_index=exchange_index,
                ))
                continue
            requests.append(AllocationRequest(
                pick.market_id, series_id, rank, first_side,
                max(0, int(first_budget)) * 100, max(0, int(second_budget)) * 100,
                exchange_index=exchange_index,
            ))
        # Per-shard cash so markets on unfunded shards are skipped (with a
        # reason) instead of firing orders the venue rejects as user_not_found.
        cash_by_exchange = None
        if not self._planning_only:
            breakdown = tuple(getattr(balance, "balance_by_exchange", ()) or ())
            if breakdown:
                cash_by_exchange = {int(index): int(units) for index, units in breakdown}
        # Sticky live-cap state from the previous pass (oversubscribed fleets
        # only; ignored at 1.0): markets granted a side last time keep it, and
        # a shard stays withheld until its live exposure drops below 80%.
        previous = self._allocation
        held_tickers = (
            {ticker: tuple(sides) for ticker, sides in previous.sides_by_ticker.items() if sides} if previous else {}
        )
        withheld_shards = (
            [shard.exchange_index for shard in previous.shards if shard.reduction_only] if previous else []
        )
        fleet_withholding = bool(getattr(previous, "withholding", False)) if previous else False
        allocation = CapitalAllocator(
            cash_reserve_fraction=float(self.fleet_config["cashReserveFraction"]),
            series_exposure_fraction=float(self.fleet_config["seriesExposureFraction"]),
            oversubscription=float(self.fleet_config.get("allocationOversubscription", 1.0)),
        ).allocate(
            requests, available_cash_units=available, existing_committed_units=committed,
            quote_side_capacity=capacity.normal_quote_side_capacity,
            cash_by_exchange_units=cash_by_exchange,
            exposure_by_ticker=exposure_by_ticker,
            held_tickers=held_tickers,
            withheld_shards=withheld_shards,
            fleet_withholding=fleet_withholding,
        )
        self._position_exposure_by_ticker = position_exposure_by_ticker
        self._position_units_by_ticker = position_units_by_ticker
        self._reduce_only_tickers = reduce_only_tickers
        self._shard_by_ticker = shard_by_ticker
        capacity = dataclasses.replace(capacity, venue=self.venue)
        return capacity, allocation

    def shard_exposure_limits_payload(self) -> dict[str, Any]:
        """The live-cap message for the broker (see execution.ShardExposureLedger).

        Enabled only when the fleet is oversubscribed: at the default 1.0 the
        budgets already fit inside allocatable cash and the broker must
        behave exactly as before.
        """
        allocation = self._allocation
        oversubscribed = bool(allocation and float(getattr(allocation, "oversubscription", 1.0)) > 1.0)
        allocatable_by_shard = {
            int(shard.exchange_index): int(shard.allocatable_units)
            for shard in (allocation.shards if allocation else ())
        }
        return {
            "enabled": oversubscribed and bool(allocatable_by_shard),
            "allocatable_by_shard": allocatable_by_shard,
            "shard_by_ticker": dict(self._shard_by_ticker),
            # The admission snapshot as taken, stamped: re-sent every
            # broadcast, it can never roll the ledger back past a later fill.
            "position_exposure_by_ticker": dict(self._position_exposure_by_ticker),
            "position_units_by_ticker": dict(self._position_units_by_ticker),
            "positions_at_ms": int(self._position_snapshot_at_ms),
            "reduce_only_tickers": sorted(self._reduce_only_tickers),
            "oversubscription": float(getattr(allocation, "oversubscription", 1.0)) if allocation else 1.0,
            "generated_at_ms": int(time.time() * 1000),
        }

    def _push_broker_control(self, operation: str, payload: Mapping[str, Any]) -> bool:
        """Queue a fire-and-forget control op for the broker (no response waited on)."""
        if self._request_queue is None or self._planning_only:
            return False
        try:
            self._request_queue.put(BrokerRequest(
                uuid.uuid4().hex, "", operation, dict(payload), int(time.time() * 1000),
            ))
            return True
        except Exception:
            return False

    def _publish_shard_exposure_limits(self) -> dict[str, Any]:
        """Hand the broker the per-shard live cap whenever capacity is recomputed."""
        payload = self.shard_exposure_limits_payload()
        self._last_exposure_limits = {
            "enabled": payload["enabled"],
            "allocatableByShard": {str(k): v for k, v in payload["allocatable_by_shard"].items()},
            "tickers": len(payload["shard_by_ticker"]),
            "positionsAtMs": payload["positions_at_ms"],
            "reduceOnlyTickers": len(payload["reduce_only_tickers"]),
            "generatedAtMs": payload["generated_at_ms"],
        }
        self._push_broker_control("shard_exposure_limits", payload)
        return payload

    def _broadcast_quoting_gate(self) -> tuple[bool, Mapping[str, Any]]:
        gate_open, allocations = self._quoting_gate()
        # The broker's live cap is (re)armed before any worker is told to quote.
        self._publish_shard_exposure_limits()
        for managed in self._workers.values():
            if not managed.recovering and managed.process.is_alive():
                managed.command_queue.put({
                    "action": "enable_quoting", "enabled": gate_open,
                    "allocations": allocations, "queuedAtMs": int(time.time() * 1000),
                })
        return gate_open, allocations

    async def _ensure_startup_account_snapshot(self) -> Optional[StartupAccountSnapshot]:
        """Reuse a recent Polymarket admission read, refreshing it once when stale."""
        if self.venue.lower() != "polymarket" or self._planning_only:
            return self._startup_account_snapshot
        async with self._startup_snapshot_lock:
            max_age_ms = int(float(self.fleet_config.get("startupAccountSnapshotMaxAgeSeconds", 30.0)) * 1000)
            current = self._startup_account_snapshot
            if current is not None and int(time.time() * 1000) - int(current.captured_at_ms) <= max_age_ms:
                return current
            if self._admin is None or not self._broker_usable():
                return current
            try:
                payload = await self._admin.call(
                    "startup_account_snapshot",
                    timeout=min(float(self.fleet_config.get("startupTimeoutSeconds", 90.0)), 30.0),
                )
                if isinstance(payload, Mapping):
                    self._startup_account_snapshot = _startup_snapshot_from_payload(payload)
            except Exception as exc:
                # A failed component is represented by the snapshot operation
                # when possible. A total broker failure leaves the worker to
                # use its bounded per-market fallback.
                self._startup_account_snapshot = None
                await self._emit("startup_account_snapshot_failed", "*", error=str(exc)[:200])
            return self._startup_account_snapshot

    async def apply_update(self, update: ScreenerUpdate) -> None:
        if self._shutdown_started:
            await self._emit("update_skipped_shutdown", "*", generation_id=update.generation_id)
            return
        if len(update.picks) > self.max_bots:
            raise ValueError(f"screener returned {len(update.picks)} markets; configured maximum is {self.max_bots}")
        await self._ensure_started()
        self._ensure_worker_processes()
        # A newer screener generation supersedes any background retry for the
        # previous allocation.  Do not let an old admission result reopen the
        # quoting gate while this update is being reconciled.
        if self._admission_retry_task is not None:
            self._admission_retry_task.cancel()
            await asyncio.gather(self._admission_retry_task, return_exceptions=True)
            self._admission_retry_task = None
        desired = update.pick_by_market_id
        previous = dict(self._market_to_worker)
        assignments = assign_markets(
            desired, worker_count=self.worker_count, shard_size=self.shard_size, previous=previous,
        )
        self._assignments = assignments
        self._market_to_worker = {
            ticker: worker_id for worker_id, tickers in assignments.items() for ticker in tickers
        }
        self._market_shard_history.update(self._market_to_worker)
        self._desired = dict(desired)
        self.bots = {ticker: ManagedMarket(pick, self._market_to_worker[ticker]) for ticker, pick in desired.items()}
        # Polymarket actor startup is intentionally held until admission has
        # supplied the account-wide snapshot.  Kalshi keeps its existing
        # eager reconcile path.
        for worker_id, managed in self._workers.items():
            if not managed.process.is_alive():
                continue
            if managed.recovering and managed.phase not in {"reconciling", "resume_pending"}:
                continue
            if self.venue.lower() == "polymarket":
                continue
            self._send_control(
                managed,
                "reconcile",
                picks=tuple(desired[ticker] for ticker in assignments.get(worker_id, ())),
                generation_id=update.generation_id,
            )
        try:
            self._capacity, self._allocation = await self._admission(desired)
            self._capacity_error = self._capacity.error
            self._admission_pending = False
        except Exception as exc:
            # A stalled broker must not abort the refresh (which would take the
            # launcher down): fail closed now and retry admission from
            # monitor_once once the broker answers again.
            if isinstance(exc, TimeoutError):
                self._note_broker_timeout()
            self._capacity_error = f"admission snapshot failed: {exc}"
            self._admission_pending = not self._planning_only
            self._admission_retry_at_ms = int(time.time() * 1000 + ADMISSION_RETRY_SECONDS * 1000)
        if self.venue.lower() == "polymarket":
            await self._ensure_startup_account_snapshot()
        # Admission may be partial (for example Polymarket can expose 51
        # quote sides while 200 markets were screened).  Trim before workers
        # are allowed to quote, preserving the deterministic allocator order.
        admission_trimmed = False
        if self._capacity and self._allocation and self._allocation.gate_open:
            admitted_keys = {
                ticker for ticker, sides in self._allocation.sides_by_ticker.items() if sides
            }
            if len(admitted_keys) < len(desired):
                admission_trimmed = True
                desired = {
                    ticker: pick for ticker, pick in desired.items() if ticker in admitted_keys
                }
                assignments = assign_markets(
                    desired, worker_count=self.worker_count, shard_size=self.shard_size, previous=previous,
                )
                self._assignments = assignments
                self._market_to_worker = {
                    ticker: worker_id for worker_id, tickers in assignments.items() for ticker in tickers
                }
                self._market_shard_history.update(self._market_to_worker)
                self._desired = dict(desired)
                self.bots = {ticker: ManagedMarket(pick, self._market_to_worker[ticker]) for ticker, pick in desired.items()}
                for worker_id, managed in self._workers.items():
                    if managed.process.is_alive() and not managed.recovering:
                        self._send_control(
                            managed,
                            "reconcile",
                            picks=tuple(desired[ticker] for ticker in assignments.get(worker_id, ())),
                            generation_id=update.generation_id,
                        )
                await self._emit(
                    "capacity_partial",
                    "*",
                    requestedMarkets=len(update.picks),
                    admittedMarkets=len(desired),
                    omittedMarkets=max(0, len(update.picks) - len(desired)),
                    capacityMarketLimit=self._capacity.capacity_market_limit,
                    omittedReason=self._capacity.omitted_reason or "capacity",
                )
        if self.venue.lower() == "polymarket" and not admission_trimmed:
            # No capacity trimming was needed, but the workers still need the
            # post-admission snapshot before their first actor starts.
            for worker_id, managed in self._workers.items():
                if managed.process.is_alive() and not managed.recovering:
                    self._send_control(
                        managed,
                        "reconcile",
                        picks=tuple(desired[ticker] for ticker in assignments.get(worker_id, ())),
                        generation_id=update.generation_id,
                    )
        gate_open = bool(
            self._capacity and self._capacity.gate_open
            and self._allocation and self._allocation.gate_open
            and not self._capacity_error
        )
        allocations = self._effective_allocations(gate_open)
        # The broker's per-shard live cap is armed before the workers are
        # told which sides they may quote (control ops jump the quote queue).
        self._publish_shard_exposure_limits()
        for managed in self._workers.values():
            if not managed.recovering and managed.process.is_alive():
                managed.command_queue.put({
                    "action": "enable_quoting", "enabled": gate_open,
                    "allocations": allocations, "queuedAtMs": int(time.time() * 1000),
                })
        if not gate_open:
            await self._emit(
                "capacity_fail_closed", "*",
                capacity_error=self._capacity_error or (self._capacity.error if self._capacity else ""),
                allocation_error=self._allocation.error if self._allocation else "",
            )
        manifest = {
            "schemaVersion": 1,
            "venue": self.venue,
            "generatedAtMs": int(time.time() * 1000),
            "workers": {worker_id: list(tickers) for worker_id, tickers in assignments.items()},
            "tickerToShard": self._market_shard_history,
            "activeTickerToShard": self._market_to_worker,
        }
        target = self._artifact_root() / "fleet_manifest.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, target)

    def _broker_has_reported(self) -> bool:
        """The current broker generation sent a heartbeat or capacity message."""
        return bool(
            self._broker_reported_at_ms
            and self._broker_reported_at_ms >= self._broker_started_at_ms - 1_000
        )

    async def _run_admission_retry(self) -> None:
        """Retry admission without occupying the manager monitor loop."""
        try:
            self._capacity, self._allocation = await self._admission(self._desired)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, TimeoutError):
                self._note_broker_timeout()
            self._capacity_error = f"admission snapshot failed: {exc}"
            self._admission_retry_at_ms = int(time.time() * 1000 + ADMISSION_RETRY_SECONDS * 1000)
            return
        self._capacity_error = self._capacity.error
        self._admission_pending = False
        gate_open, _allocations = self._broadcast_quoting_gate()
        if self.venue.lower() == "polymarket":
            self._reconcile_after_broker_ready = False
            self._reissue_assignments()
        await self._emit("admission_recovered", "*", gate_open=gate_open)

    async def _retry_admission(self) -> None:
        if not self._admission_pending or not self._desired or self._shutdown_started:
            return
        if self._admission_retry_task is not None:
            if not self._admission_retry_task.done():
                return
            self._admission_retry_task = None
        now = int(time.time() * 1000)
        if now < self._admission_retry_at_ms or not self._broker_usable():
            return
        if not self._broker_has_reported():
            # A replacement broker still in its startup cleanup cannot answer
            # admission_snapshot; waiting the full 90 s on it would freeze
            # monitor_once (and every status publish) and delay the restart of
            # a replacement that dies during startup by the same 90 s.
            return
        self._admission_retry_task = asyncio.create_task(self._run_admission_retry())

    async def stop_bot(self, market_id: str, *, reason: str = "reconcile", remove_desired: bool = True) -> None:
        if market_id not in self._desired:
            return
        desired = dict(self._desired)
        desired.pop(market_id, None)
        update = ScreenerUpdate(
            generation_id=int(time.time() * 1000), generated_at_ms=int(time.time() * 1000),
            reason=reason, picks=tuple(desired.values()), added=(), kept=tuple(desired), changed=(), removed=(market_id,),
        )
        await self.apply_update(update)

    def _quoting_gate(self) -> tuple[bool, Mapping[str, Any]]:
        gate_open = bool(
            self._capacity and self._capacity.gate_open
            and self._allocation and self._allocation.gate_open
            and not self._capacity_error
        )
        return gate_open, self._effective_allocations(gate_open)

    def _effective_allocations(self, gate_open: bool) -> dict[str, Any]:
        """The allowed sides per ticker to broadcast: the allocation minus
        restart carryovers whose position the broker's ledger reports flat
        (an absent ticker means no side for the worker)."""
        if not gate_open or not self._allocation:
            return {}
        allocations = dict(self._allocation.sides_by_ticker)
        for ticker in self._broker_flat_reduce_only:
            if ticker in self._reduce_only_tickers:
                allocations.pop(ticker, None)
        return allocations

    async def _resume_recovered_worker(self, managed: ManagedWorker) -> None:
        now = int(time.time() * 1000)
        if managed.recovery_retry_at_ms > now:
            return
        try:
            if not self._broker_usable():
                raise RuntimeError("execution broker is unavailable")
            await self._admin.call(
                "resume_channel", {"channel": managed.worker_id},
                timeout=self._admin_timeout(BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS),
            )
            managed.channel_quiesced = False
            gate_open, allocations = self._quoting_gate()
            managed.command_queue.put({
                "action": "enable_quoting",
                "enabled": gate_open,
                "allocations": allocations,
                "queuedAtMs": int(time.time() * 1000),
            })
            managed.phase = "healthy"
            managed.recovering = False
            managed.last_error = ""
            managed.recovery_retry_at_ms = 0
            await self._emit(
                "worker_restarted",
                "*",
                worker_id=managed.worker_id,
                affected_markets=len(self._assignments.get(managed.worker_id, ())),
            )
        except Exception as exc:
            if isinstance(exc, TimeoutError):
                self._note_broker_timeout()
            managed.last_error = str(exc)
            managed.recovery_retry_at_ms = now + 5_000

    async def _recover_worker(self, worker_id: str, managed: ManagedWorker) -> None:
        now = int(time.time() * 1000)
        if managed.recovery_retry_at_ms > now:
            return
        managed.recovery_count += 1
        managed.last_recovery_at_ms = now
        managed.recovering = True
        managed.reconcile_owed = False     # the replacement is reconciled below
        managed.phase = "quiescing"
        errors: list[str] = []

        try:
            if not self._broker_usable():
                raise RuntimeError("execution broker is unavailable")
            await self._admin.call(
                "quiesce_channel", {"channel": worker_id},
                timeout=self._admin_timeout(BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS),
            )
            managed.channel_quiesced = True
        except Exception as exc:
            if isinstance(exc, TimeoutError):
                self._note_broker_timeout()
            errors.append(f"broker quiesce: {exc}")

        if managed.process.is_alive():
            freeze_request = self._send_control(managed, "freeze")
            freeze_ack = await self._wait_for_control(freeze_request, 5.0)
            if freeze_ack is None:
                errors.append("worker freeze acknowledgement timed out")
            elif not freeze_ack.ok:
                errors.append(f"worker freeze failed: {freeze_ack.error}")
            if freeze_ack is not None:
                stop_request = self._send_control(
                    managed, "stop", broker_responsive=managed.channel_quiesced,
                )
                await self._wait_for_control(stop_request, 1.0)
            stopped = await self._terminate_process(managed.process, graceful_timeout=5.0)
            if not stopped:
                errors.append("worker process survived terminate/kill escalation")

        managed.phase = "cleanup_pending"
        if managed.process.is_alive() or not managed.channel_quiesced:
            managed.last_error = "; ".join(errors) or "worker could not be quiesced"
            managed.recovery_retry_at_ms = int(time.time() * 1000) + 5_000
            await self._emit(
                "worker_recovery_failed", "*", worker_id=worker_id, error=managed.last_error,
            )
            return

        try:
            # Per-market cleanup keeps other shards' orders untouched.  The
            # broker gets one bounded chance per market; the first broker
            # timeout sends the rest of this pass straight to the direct
            # client instead of serialising a timeout per market.  The direct
            # client is bounded per market too (direct_deadline): on a silent
            # venue the first failing market aborts the pass (retried in 5 s)
            # instead of holding monitor_once for minutes per market.
            via_broker = True
            for ticker in self._assignments.get(worker_id, ()):
                timeouts_before = self._broker_rpc_timeout_total
                with self._cleanup_policy(
                    via_broker=via_broker,
                    broker_timeout=BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS,
                    direct_deadline=time.monotonic() + BROKER_ADMIN_RECOVERY_TIMEOUT_SECONDS,
                ):
                    await self._cancel_owned(ticker)
                if self._broker_rpc_timeout_total > timeouts_before:
                    via_broker = False
        except Exception as exc:
            errors.append(f"order cleanup: {exc}")
            managed.last_error = "; ".join(errors)
            managed.recovery_retry_at_ms = int(time.time() * 1000) + 5_000
            await self._emit(
                "worker_recovery_failed",
                "*",
                worker_id=worker_id,
                error=managed.last_error,
                remaining_order_ids=self._remaining_ids(exc),
            )
            return

        # The replacement gets a fresh command queue (the old one's reader lock
        # may be orphaned by the terminate) but must keep the response queue:
        # the broker's per-channel map was fixed when it was spawned and a
        # multiprocessing queue cannot be shipped to a running process.  The
        # terminated worker's dispatcher was that queue's sole reader and was
        # very likely inside Queue.get, so its reader lock is orphaned: free
        # it, and the replacement's dispatcher reads lock-free regardless
        # (BrokerRpcClient._read_response), so either build of the replacement
        # still gets its responses.
        release_queue_reader_lock(managed.response_queue)
        command_queue = mp.SimpleQueue()
        process = self._make_worker(worker_id, managed.response_queue, command_queue)
        process.start()
        managed.process = process
        managed.command_queue = command_queue
        managed.heartbeat = None
        managed.started_at_ms = int(time.time() * 1000)
        managed.last_liveness_at_ms = int(time.time() * 1000)
        managed.last_error = "; ".join(errors)
        managed.recovery_retry_at_ms = 0
        tickers = self._assignments.get(worker_id, ())
        await self._ensure_startup_account_snapshot()
        self._send_control(
            managed,
            "reconcile",
            picks=tuple(self._desired[ticker] for ticker in tickers if ticker in self._desired),
        )

    def _schedule_recovery(self, worker_id: str, coroutine: Any) -> None:
        """Run one worker recovery without blocking the monitor loop."""
        existing = self._recovery_tasks.get(worker_id)
        if existing is not None and not existing.done():
            close = getattr(coroutine, "close", None)
            if close is not None:
                close()
            return
        task = asyncio.create_task(coroutine)
        self._recovery_tasks[worker_id] = task

        def finished(done: asyncio.Task[Any]) -> None:
            if self._recovery_tasks.get(worker_id) is done:
                self._recovery_tasks.pop(worker_id, None)
            try:
                done.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                current = self._workers.get(worker_id)
                if current is not None:
                    current.last_error = str(exc)

        task.add_done_callback(finished)

    async def monitor_once(self) -> None:
        if not self._started:
            return
        if self._broker is None or not self._broker.is_alive():
            await self._restart_broker("exited", force=True)
        broker_changed = self._drain_broker_status()
        stall_reason = self.broker_stall_reason()
        if stall_reason:
            if not self._broker_stalled:
                self._broker_stall_flagged_at_ms = int(time.time() * 1000)
            self._broker_stalled = True
            self._broker_stall_reason = stall_reason
            if await self._restart_broker(f"restarted: {stall_reason}"):
                broker_changed = True
        elif (
            self._broker_stalled
            and self._broker is not None
            and self._broker.is_alive()
            and self._broker_last_seen_ms >= self._broker_stall_flagged_at_ms
        ):
            # Flagged stalled while a restart was rate-limited (or by a
            # controller RPC timeout), and the broker has since reported in
            # healthy: the flag must not stick, or every later worker recovery,
            # admission retry and graceful shutdown treats the broker as dead.
            self._broker_stalled = False
            self._broker_stall_reason = ""
            self._broker_stall_flagged_at_ms = 0
        if broker_changed:
            gate_open, _allocations = self._broadcast_quoting_gate()
            if not gate_open:
                await self._emit("runtime_capacity_fail_closed", "*", error=self._capacity_error)
        await self._retry_admission()
        while True:
            try:
                # A worker can be terminated while its multiprocessing queue
                # feeder is active.  The normal Queue.get_nowait() path then
                # inherits an orphaned reader lock and silently reports an
                # empty queue forever, so replacement workers keep quoting
                # while their watchdog state remains startup.  This queue has
                # one consumer; use the same lock-free reader as broker RPC.
                heartbeat = read_queue_lockfree(self._heartbeat_queue, 0)
            except queue.Empty:
                break
            managed = self._workers.get(heartbeat.worker_id)
            if managed:
                previous = managed.heartbeat
                previous_sequence = int(getattr(previous, "heartbeat_sequence", 0) or 0) if previous else 0
                incoming_sequence = int(getattr(heartbeat, "heartbeat_sequence", 0) or 0)
                if previous_sequence and incoming_sequence and incoming_sequence <= previous_sequence:
                    # A delayed queue sample must not roll the manager back to
                    # an older liveness/resource state.
                    continue
                managed.heartbeat = heartbeat
                managed.last_heartbeat_received_at_ms = int(time.time() * 1000)
                managed.last_liveness_at_ms = max(managed.last_liveness_at_ms, heartbeat.generated_at_ms)
                previous_actors = set(previous.assigned_tickers) if previous else set()
                previous_pending = set(getattr(previous, "pending_tickers", ()) or ()) if previous else set()
                current_actors = set(heartbeat.assigned_tickers)
                current_pending = set(getattr(heartbeat, "pending_tickers", ()) or ())
                managed.startup_pending_tickers = tuple(sorted(current_pending))
                managed.startup_attempts = dict(getattr(heartbeat, "startup_attempts", {}) or {})
                managed.startup_error = str(getattr(heartbeat, "startup_error", "") or "")
                managed.worker_error = str(getattr(heartbeat, "worker_error", "") or "")
                if (
                    len(current_actors) > len(previous_actors)
                    or len(current_pending) < len(previous_pending)
                ):
                    managed.startup_progress_at_ms = heartbeat.generated_at_ms
                elif int(getattr(heartbeat, "startup_progress_at_ms", 0) or 0):
                    managed.startup_progress_at_ms = max(
                        managed.startup_progress_at_ms,
                        int(heartbeat.startup_progress_at_ms),
                    )
                target = set(managed.startup_target_tickers)
                if target and target.issubset(current_actors) and not current_pending:
                    managed.startup_target_tickers = ()
                    managed.startup_pending_tickers = ()
                    managed.phase = "healthy" if managed.pending_action != "reconcile" else managed.phase
        self._drain_control_acks()
        current = int(time.time() * 1000)
        stale_ms = int(float(self.fleet_config["workerStaleSeconds"]) * 1000)
        hard_cap_ms = int(
            float(self.fleet_config["startupTimeoutSeconds"]) * RECONCILE_HARD_CAP_MULTIPLIER * 1000
        )
        startup_progress_timeout_ms = int(
            float(self.fleet_config.get("startupProgressTimeoutSeconds", 90.0)) * 1000
        )
        broker_grace = bool(
            self._broker_restarted_at_ms
            and current - self._broker_restarted_at_ms <= self.broker_restart_grace_seconds * 1000
        )
        for worker_id, managed in list(self._workers.items()):
            first_heartbeat_pending = managed.heartbeat is None and bool(managed.started_at_ms)
            first_heartbeat_grace = bool(
                first_heartbeat_pending
                and current - managed.started_at_ms <= hard_cap_ms
            )
            if managed.phase == "resume_pending":
                # Reconcile is acknowledged before the worker finishes
                # starting all of its actors.  Keep the broker channel
                # quiesced until the replacement emits its first heartbeat;
                # otherwise the manager declares it recovered and then
                # immediately recycles it for the still-missing heartbeat.
                if not managed.process.is_alive():
                    self._schedule_recovery(worker_id, self._recover_worker(worker_id, managed))
                    continue
                if first_heartbeat_grace:
                    continue
                self._schedule_recovery(worker_id, self._resume_recovered_worker(managed))
                continue
            if managed.phase in {"cleanup_pending", "quiesce_failed"}:
                self._schedule_recovery(worker_id, self._recover_worker(worker_id, managed))
                continue
            reconcile_pending = managed.pending_action == "reconcile" and bool(managed.control_deadline_ms)
            reconcile_grace = reconcile_pending and current <= managed.control_deadline_ms
            liveness_at_ms = max(
                managed.last_liveness_at_ms,
                managed.heartbeat.generated_at_ms if managed.heartbeat else 0,
            )
            stale = bool(liveness_at_ms and current - liveness_at_ms > stale_ms)
            dead = not managed.process.is_alive()
            reconcile_timed_out = reconcile_pending and current > managed.control_deadline_ms
            reconcile_hard_cap = bool(
                reconcile_pending
                and managed.control_sent_at_ms
                and current - managed.control_sent_at_ms > hard_cap_ms
            )
            target = set(managed.startup_target_tickers)
            actor_tickers = set(managed.heartbeat.assigned_tickers if managed.heartbeat else ())
            startup_pending = bool(
                target
                and (
                    bool(managed.startup_pending_tickers)
                    or not target.issubset(actor_tickers)
                )
            )
            progress_at_ms = managed.startup_progress_at_ms or managed.startup_started_at_ms
            startup_no_progress = bool(
                startup_pending
                and progress_at_ms
                and current - progress_at_ms > startup_progress_timeout_ms
            )
            if reconcile_grace and not dead and not startup_no_progress:
                continue
            if dead:
                self._schedule_recovery(worker_id, self._recover_worker(worker_id, managed))
                continue
            # A worker can be busy with its first reconciliation before its
            # first heartbeat reaches the parent (the controller may also be
            # finishing a long admission RPC at this point).  Treating that
            # initial silence as a stale worker makes every shard get recycled
            # just as its actors finish startup.  Once a heartbeat exists, the
            # ordinary stale-worker rule below remains the safety net; a
            # genuinely wedged first reconcile is still bounded by the hard
            # startup cap.
            if first_heartbeat_grace and not startup_no_progress:
                continue
            if stale and broker_grace:
                # The worker may be blocked on a broker that was just replaced;
                # give it the restart grace before treating silence as death.
                continue
            if startup_no_progress and not broker_grace:
                managed.recovery_reason = "startup_no_progress"
                if managed.worker_error and (
                    "can't start new thread" in managed.worker_error.lower()
                    or "cannot start new thread" in managed.worker_error.lower()
                ):
                    managed.recovery_reason = "thread_resource_failure"
                self._schedule_recovery(worker_id, self._recover_worker(worker_id, managed))
                continue
            if stale:
                self._schedule_recovery(worker_id, self._recover_worker(worker_id, managed))
                continue
            if reconcile_timed_out and reconcile_hard_cap:
                # Alive and heartbeating, but the reconcile never acknowledged
                # for several startup windows: something is wedged in-process.
                self._schedule_recovery(worker_id, self._recover_worker(worker_id, managed))
                continue
            # A slow reconcile on a live, heartbeating worker is left alone.
        # Start scheduled recoveries before returning so lightweight/test
        # recoveries take effect in this monitor pass, while the first await
        # inside a real broker cleanup still yields back to the launcher.
        if self._recovery_tasks:
            await asyncio.sleep(0)
        if self._admission_retry_task is not None:
            # Let an already-ready retry (common in tests and during a fast
            # broker recovery) publish its result without ever waiting for a
            # slow venue call here.
            await asyncio.sleep(0)
            if self._admission_retry_task.done():
                self._admission_retry_task = None

    async def stop_all(self, *, reason: str = "launcher_shutdown") -> None:
        """Stop workers and broker, cancelling and verifying bot orders.

        Runs against ``shutdown_budget_seconds`` (see SHUTDOWN_BUDGET_SECONDS
        for the phase table): every broker wait and graceful join is clamped
        to the time left, and once the budget is gone the remaining phases
        terminate processes outright and the direct-client verification stops
        starting new attempts.
        """
        self.begin_shutdown()
        if self._admission_retry_task is not None:
            self._admission_retry_task.cancel()
            await asyncio.gather(self._admission_retry_task, return_exceptions=True)
            self._admission_retry_task = None
        recovery_tasks = list(self._recovery_tasks.values())
        for task in recovery_tasks:
            task.cancel()
        if recovery_tasks:
            await asyncio.gather(*recovery_tasks, return_exceptions=True)
        self._recovery_tasks.clear()
        has_live_children = any(item.process.is_alive() for item in self._workers.values()) or bool(
            self._broker is not None and self._broker.is_alive()
        )
        if not self._started and not has_live_children:
            return
        started = int(time.time() * 1000)
        self._shutdown_cleanup = {
            "state": "running",
            "startedAtMs": started,
            "completedAtMs": None,
            "canceledOrders": 0,
            "ordersVerifiedAbsent": False,
            "workersStopped": False,
            "brokerStopped": False,
            "remainingBotOrderIds": [],
            "warnings": [],
            "error": None,
        }
        canceled = 0
        warnings: list[str] = []
        terminal_errors: list[str] = []
        remaining_ids: list[str] = []
        final_orders_verified = False
        broker_fenced = self._planning_only
        deadline = time.monotonic() + max(1.0, float(self.shutdown_budget_seconds))

        def remaining() -> float:
            return max(0.0, deadline - time.monotonic())

        def phase(cap: float) -> float:
            return min(float(cap), remaining())

        def cleanup_policy() -> Any:
            return self._cleanup_policy(
                via_broker=remaining() > 0,
                broker_timeout=phase(SHUTDOWN_BROKER_CLEANUP_SECONDS),
                direct_deadline=deadline,
                mark_stall_on_timeout=True,
            )

        try:
            if not self._planning_only:
                # Every broker wait in here is short: a broker that cannot
                # answer within these bounds is treated as gone and the
                # account cleanup goes through the direct client instead.
                # Heartbeats queued since the last monitor pass are applied
                # first so a healthy broker is not judged on a stale stamp.
                self._drain_broker_status()
                try:
                    if not self._broker_usable():
                        raise RuntimeError(
                            "execution broker is unavailable"
                            + (f" ({self._broker_stall_reason})" if self._broker_stall_reason else "")
                        )
                    if remaining() <= 0:
                        raise RuntimeError("shutdown budget exhausted before the write fence")
                    await self._admin.call("quiesce_all", timeout=phase(SHUTDOWN_FENCE_SECONDS))
                    broker_fenced = True
                except Exception as exc:
                    if isinstance(exc, TimeoutError):
                        self._note_broker_timeout()
                        self._broker_stalled = True
                        self._broker_stall_reason = str(exc)
                    warnings.append(f"broker write fence failed: {exc}")

            freeze_requests: list[tuple[ManagedWorker, str]] = []
            for managed in self._workers.values():
                if managed.process.is_alive():
                    freeze_requests.append((managed, self._send_control(managed, "freeze")))
            freeze_wait = phase(SHUTDOWN_FREEZE_ACK_SECONDS)
            freeze_results = await asyncio.gather(*(
                self._wait_for_control(request_id, freeze_wait)
                for _, request_id in freeze_requests
            ))
            for (managed, _), ack in zip(freeze_requests, freeze_results):
                if ack is None:
                    warnings.append(f"{managed.worker_id}: freeze acknowledgement timed out")
                    await self._terminate_process(managed.process)
                elif not ack.ok:
                    warnings.append(f"{managed.worker_id}: freeze failed: {ack.error}")

            # If the broker could not establish the write barrier, terminate
            # every producer and the broker before attempting account cleanup.
            if not broker_fenced:
                for managed in self._workers.values():
                    await self._terminate_process(managed.process)
                if self._broker is not None:
                    await self._terminate_process(self._broker)

            initial_cleanup_unreachable = False
            initial_remaining_ids: list[str] = []
            try:
                with cleanup_policy():
                    canceled += await self._cancel_owned()
            except Exception as exc:
                warnings.append(f"initial account cleanup: {exc}")
                initial_cleanup_unreachable = is_venue_transport_failure(exc)
                initial_remaining_ids = self._remaining_ids(exc)

            stop_requests: list[tuple[ManagedWorker, str]] = []
            for managed in self._workers.values():
                if managed.process.is_alive():
                    stop_requests.append((
                        managed,
                        self._send_control(managed, "stop", broker_responsive=broker_fenced),
                    ))
            stop_wait = phase(SHUTDOWN_STOP_ACK_SECONDS)
            await asyncio.gather(*(
                self._wait_for_control(request_id, stop_wait)
                for _, request_id in stop_requests
            ))
            worker_grace = phase(SHUTDOWN_WORKER_GRACE_SECONDS)
            await asyncio.gather(*(
                self._terminate_process(managed.process, graceful_timeout=worker_grace)
                for managed in self._workers.values()
            ))

            if remaining() <= 0 and initial_cleanup_unreachable:
                # The budget is gone and the venue did not answer the first
                # cleanup: a second identical call cannot verify anything, it
                # would only add another read timeout past the API's kill.
                terminal_errors.append(
                    "final account cleanup skipped: shutdown budget exhausted and the venue "
                    "did not answer the initial cleanup"
                )
                remaining_ids.extend(initial_remaining_ids)
            else:
                try:
                    with cleanup_policy():
                        canceled += await self._cancel_owned()
                    final_orders_verified = True
                except Exception as exc:
                    terminal_errors.append(f"final account cleanup: {exc}")
                    remaining_ids.extend(self._remaining_ids(exc))

            # A graceful broker join only makes sense after the broker took
            # the stop command; when it was skipped (stalled, budget gone) or
            # went unanswered, the join could only expire: terminate at once.
            broker_stop_acknowledged = False
            if self._broker_usable() and remaining() > 0:
                try:
                    await self._admin.call("stop", timeout=phase(SHUTDOWN_BROKER_STOP_SECONDS))
                    broker_stop_acknowledged = True
                except Exception as exc:
                    if isinstance(exc, TimeoutError):
                        self._note_broker_timeout()
                    warnings.append(f"broker stop command failed: {exc}")
            if self._broker is not None:
                await self._terminate_process(
                    self._broker,
                    graceful_timeout=(
                        phase(SHUTDOWN_BROKER_GRACE_SECONDS) if broker_stop_acknowledged else 0.0
                    ),
                )
        except Exception as exc:
            terminal_errors.append(f"shutdown orchestration: {exc}")
        finally:
            for managed in self._workers.values():
                try:
                    await self._terminate_process(managed.process)
                except Exception as exc:
                    terminal_errors.append(f"{managed.worker_id} teardown: {exc}")
            if self._broker is not None:
                try:
                    await self._terminate_process(self._broker)
                except Exception as exc:
                    terminal_errors.append(f"broker teardown: {exc}")
            self._started = False
            self.bots.clear()
            for managed in self._workers.values():
                managed.phase = "stopped"
                managed.recovering = False

        workers_stopped = all(not managed.process.is_alive() for managed in self._workers.values())
        broker_stopped = self._broker is None or not self._broker.is_alive()
        if not workers_stopped:
            terminal_errors.append("one or more worker processes remain alive")
        if not broker_stopped:
            terminal_errors.append("execution broker process remains alive")
        successful = final_orders_verified and workers_stopped and broker_stopped
        error = "; ".join(terminal_errors) or None
        self._shutdown_cleanup.update(
            state="verified" if successful else "failed",
            completedAtMs=int(time.time() * 1000),
            canceledOrders=canceled,
            ordersVerifiedAbsent=final_orders_verified,
            workersStopped=workers_stopped,
            brokerStopped=broker_stopped,
            remainingBotOrderIds=sorted(set(remaining_ids)),
            warnings=warnings,
            error=error,
        )
        if successful:
            await self._emit(
                "fleet_shutdown_cleanup_verified",
                "*",
                canceled_orders=canceled,
                orders_verified_absent=True,
            )
            return
        await self._emit("shutdown_cleanup_failed", "*", reason=reason, error=error or "unknown shutdown failure")
        raise RuntimeError("launcher shutdown cleanup failed: " + (error or "unknown shutdown failure"))

    def status_snapshot(self) -> dict[str, Any]:
        now = int(time.time() * 1000)
        rows = []
        clients = []
        workers = []
        risk_modes: dict[str, int] = {}
        pnl_totals: dict[str, float | int] = {
            "fills": 0, "feesCents": 0.0, "realizedCents": 0.0,
            "unrealizedCents": 0.0, "totalCents": 0.0,
        }
        portfolio_items: list[dict[str, Any]] = []
        activity_sources: list[Mapping[str, Any]] = []
        if self._broker_api_activity:
            activity_sources.append(self._broker_api_activity)
        stale_ms = int(float(self.fleet_config["workerStaleSeconds"]) * 1000)
        watchdog_priority = {"normal": 0, "startup": 1, "reduction_only": 2, "flatten_only": 3}
        for worker_id, managed in self._workers.items():
            heartbeat = managed.heartbeat
            liveness_at_ms = max(
                managed.last_liveness_at_ms,
                heartbeat.generated_at_ms if heartbeat else 0,
            )
            worker_stale = bool(
                managed.pending_action != "reconcile" or now > managed.control_deadline_ms
            ) and (not liveness_at_ms or now - liveness_at_ms > stale_ms)
            assigned_tickers = tuple(self._assignments.get(worker_id, ()))
            actor_tickers = set(heartbeat.assigned_tickers if heartbeat else ())
            running_tickers = actor_tickers if managed.process.is_alive() and not worker_stale else set()
            health = heartbeat.market_health if heartbeat else {}
            shard_modes: dict[str, int] = {}
            for ticker in assigned_tickers:
                mode = health[ticker].risk_mode if ticker in health else "startup"
                shard_modes[mode] = shard_modes.get(mode, 0) + 1
            if not managed.process.is_alive():
                shard_watchdog = "stopped"
            elif worker_stale or heartbeat is None:
                shard_watchdog = "unavailable"
            else:
                shard_watchdog = max(
                    shard_modes or {"startup": 1},
                    key=lambda mode: watchdog_priority.get(mode, 1),
                )
            if heartbeat and managed.process.is_alive() and not worker_stale and isinstance(heartbeat.api_activity, Mapping):
                activity_sources.append(heartbeat.api_activity)
            heartbeat_at_ms = (
                int(getattr(heartbeat, "published_at_ms", 0) or heartbeat.generated_at_ms)
                if heartbeat else None
            )
            heartbeat_received_at_ms = managed.last_heartbeat_received_at_ms or heartbeat_at_ms
            heartbeat_age_ms = (
                max(0, now - heartbeat_received_at_ms)
                if heartbeat_received_at_ms else None
            )
            heartbeat_queue_lag_ms = (
                max(0, heartbeat_received_at_ms - int(getattr(heartbeat, "published_at_ms", 0)))
                if heartbeat and heartbeat_received_at_ms and getattr(heartbeat, "published_at_ms", 0)
                else 0
            )
            api_error_total = int((getattr(heartbeat, "api_errors", {}) or {}).get("total", 0) or 0) if heartbeat else 0
            event_loop_lag_ms = int(getattr(heartbeat, "event_loop_lag_ms", 0) or 0) if heartbeat else None
            command_oldest_age_ms = int(getattr(heartbeat, "command_oldest_age_ms", 0) or 0) if heartbeat else None
            starvation_signal = bool(
                heartbeat and not worker_stale and (
                    (event_loop_lag_ms or 0) > stale_ms
                    or (command_oldest_age_ms or 0) > stale_ms
                )
            )
            degraded = bool(
                worker_stale or managed.recovering or starvation_signal
                or (heartbeat and managed.phase not in {"healthy", "starting", "reconciling"})
                or (heartbeat and api_error_total > 0 and bool(managed.startup_pending_tickers))
            )
            workers.append({
                "venue": self.venue,
                "workerId": worker_id, "pid": managed.process.pid,
                "running": managed.process.is_alive(),
                "phase": managed.phase,
                "lastRecoveryError": managed.last_error or None,
                "recoveryReason": managed.recovery_reason or None,
                "startedAtMs": managed.started_at_ms or None,
                "runningForMs": max(0, now - managed.started_at_ms) if managed.started_at_ms else None,
                "assignedMarkets": len(assigned_tickers),
                "marketIds": list(assigned_tickers),
                "botsRunning": len(running_tickers),
                "startupPendingMarkets": list(managed.startup_pending_tickers),
                "startupAttempts": dict(managed.startup_attempts),
                "startupProgressAtMs": managed.startup_progress_at_ms or None,
                "startupElapsedMs": (
                    max(0, now - managed.startup_started_at_ms)
                    if managed.startup_started_at_ms and managed.startup_target_tickers else 0
                ),
                "lastStartupError": managed.startup_error or None,
                "workerError": managed.worker_error or None,
                "watchdog": {"mode": shard_watchdog, "counts": shard_modes},
                "heartbeatAtMs": heartbeat.generated_at_ms if heartbeat else None,
                "heartbeatReceivedAtMs": heartbeat_received_at_ms,
                "heartbeatAgeMs": heartbeat_age_ms,
                "heartbeatSequence": int(getattr(heartbeat, "heartbeat_sequence", 0) or 0) if heartbeat else None,
                "heartbeatSource": str(getattr(heartbeat, "heartbeat_source", "") or "") if heartbeat else None,
                "heartbeatQueueLagMs": heartbeat_queue_lag_ms,
                "stale": worker_stale,
                "degraded": degraded,
                "starved": starvation_signal,
                "recovering": managed.recovering,
                "memoryRssBytes": heartbeat.memory_rss_bytes if heartbeat else None,
                "cpuPercent": float(getattr(heartbeat, "cpu_percent", 0.0) or 0.0) if heartbeat else None,
                "threadCount": int(getattr(heartbeat, "thread_count", 0) or 0) if heartbeat else None,
                "queueDepth": heartbeat.queue_depth if heartbeat else None,
                "commandQueueDepth": int(getattr(heartbeat, "command_queue_depth", 0) or 0) if heartbeat else None,
                "commandOldestAgeMs": command_oldest_age_ms,
                "commandWaitMs": int(getattr(heartbeat, "command_wait_ms", 0) or 0) if heartbeat else None,
                "eventLagMs": heartbeat.event_lag_ms if heartbeat else None,
                "eventLoopLagMs": event_loop_lag_ms,
                "fallbackHeartbeatCount": int(getattr(heartbeat, "fallback_heartbeat_count", 0) or 0) if heartbeat else 0,
                "lastFallbackAtMs": int(getattr(heartbeat, "last_fallback_at_ms", 0) or 0) if heartbeat else None,
                "warningCount": int(getattr(heartbeat, "warning_count", 0) or 0) if heartbeat else 0,
                "errorCount": int(getattr(heartbeat, "error_count", 0) or 0) if heartbeat else 0,
                "lastWarning": str(getattr(heartbeat, "last_warning", "") or "") if heartbeat else None,
                "lastError": str(getattr(heartbeat, "last_error", "") or "") if heartbeat else None,
                "recoveryCount": managed.recovery_count,
                "lastRecoveryAtMs": managed.last_recovery_at_ms or None,
                "apiActivity": dict(heartbeat.api_activity) if heartbeat else {},
                "apiErrors": dict(getattr(heartbeat, "api_errors", {}) or {}) if heartbeat else {},
            })
            for ticker in assigned_tickers:
                item = health.get(ticker)
                pick = self._desired.get(ticker)
                risk_mode = item.risk_mode if item else "startup"
                risk_modes[risk_mode] = risk_modes.get(risk_mode, 0) + 1
                bot_running = ticker in running_tickers
                # Evaluator reason (``elevated_price_move`` ...) or, for a
                # market still waiting on its actor, the retry state.
                risk_reason = (
                    (str(getattr(item, "risk_reason", "") or "") or item.error or None) if item else None
                )
                rows.append({
                    "venue": self.venue,
                    "ticker": ticker, "marketId": ticker, "title": pick.title if pick else ticker,
                    "workerId": worker_id, "pid": managed.process.pid,
                    # Fleet watchdog evaluation is emitted in the worker
                    # heartbeat rather than by a separate child process.
                    # Report it as running only when this market has a current
                    # heartbeat health record.
                    "botRunning": bot_running,
                    "watchdogRunning": bool(bot_running and item is not None),
                    "yesBudgetCents": pick.yes_budget_cents if pick else 0,
                    "noBudgetCents": pick.no_budget_cents if pick else 0,
                    "watchdogMode": risk_mode,
                    "watchdogReason": risk_reason,
                    "bookReady": item.book_available if item else False,
                    "quoteAgeMs": item.decision_age_ms if item else None,
                    "eventLagMs": item.book_age_ms if item else None,
                    "positionUnits": item.position_units if item else None,
                    "restingOrderCount": item.resting_order_count if item else 0,
                    "socketHealthy": bool(heartbeat is not None and not worker_stale),
                })
                position_units = item.position_units if item else None
                portfolio_items.append({
                    "venue": self.venue, "marketId": ticker,
                    "title": pick.title if pick else ticker,
                    "positionUnits": position_units,
                    "updatedAtMs": heartbeat.generated_at_ms if heartbeat else None,
                    "stale": worker_stale or item is None,
                    "available": item is not None,
                })
                if item:
                    for key in pnl_totals:
                        value = item.pnl.get(key)
                        if isinstance(value, (int, float)):
                            pnl_totals[key] += value
                clients.append({
                    "venue": self.venue,
                    "workerId": worker_id,
                    "marketId": ticker,
                    "title": pick.title if pick else ticker,
                    "pid": managed.process.pid,
                    "lifecycle": "running" if bot_running else "startup" if managed.process.is_alive() else "stopped",
                    "socketHealthy": bool(item is not None and heartbeat is not None and not worker_stale),
                    "market": {
                        "marketId": ticker, "title": pick.title if pick else ticker,
                        "priceUnits": item.price_units if item else None,
                        "priceSource": item.price_source if item else "unavailable",
                        "priceAtMs": item.price_at_ms if item else None,
                        "lastQuoteAtMs": item.last_quote_at_ms or None if item else None,
                    },
                    "portfolio": {
                        "currentPositionUnits": item.position_units if item else None,
                        "updatedAtMs": heartbeat.generated_at_ms if heartbeat else None,
                    },
                    "runtime": {
                        "startedAtMs": item.started_at_ms if item else None,
                        "runningForMs": max(0, now - item.started_at_ms) if item and item.started_at_ms else None,
                    },
                    "pnl": dict(item.pnl) if item else {},
                    "markouts": dict(item.markouts) if item else {},
                    "fills": {
                        "count": item.fill_count if item else 0,
                        "lastFillAtMs": item.last_fill_at_ms or None if item else None,
                        "recent": [],
                    },
                    "orderActivity": {
                        "byAction": dict(item.order_activity) if item else {},
                        "lastCreateAtMs": item.last_order_create_at_ms or None if item else None,
                        "active": {}, "recent": [],
                    },
                    "watchdog": {
                        "running": bool(bot_running and item is not None), "mode": risk_mode,
                        "reason": risk_reason,
                        "updatedAtMs": heartbeat.generated_at_ms if heartbeat else None,
                    },
                    "quoteAgeMs": item.decision_age_ms if item else None,
                    "eventLagMs": item.book_age_ms if item else None,
                })
        capacity = asdict(self._capacity) if self._capacity else None
        if capacity is not None:
            venue_cfg = (self.configuration.get("venues") or {}).get(self.venue, {})
            capacity.update({
                "globalMaxBots": int(self.configuration["launcher"]["maxBots"]),
                "venueMaxBots": int(venue_cfg.get("maxBots", self.max_bots)),
                "systemMaxBots": int(self._system_capacity_limit),
                "priority": int(venue_cfg.get("priority", 100)),
                "globalSlotsRemaining": max(0, int(self._system_capacity_limit) - len(self._desired)),
                "capacityLimited": bool(capacity.get("capacity_limited", False)),
                "capacityMarketLimit": int(capacity.get("capacity_market_limit", 0)),
                "venueCapacityLimit": int(
                    capacity.get("venue_capacity_limit", capacity.get("capacity_market_limit", 0))
                ),
                "venueQuoteSideCapacity": int(
                    capacity.get("venue_quote_side_capacity", capacity.get("normal_quote_side_capacity", 0))
                ),
                "venueCapacityLimited": bool(
                    capacity.get("venue_capacity_limited", capacity.get("capacity_limited", False))
                ),
                "venueCapacityReason": capacity.get(
                    "venue_capacity_reason", capacity.get("omitted_reason", "")
                ),
                "requestedMarkets": int(capacity.get("requested_markets", 0)),
                "admittedMarkets": int(capacity.get("admitted_markets", 0)),
                "omittedMarkets": int(capacity.get("omitted_markets", 0)),
                "omittedReason": capacity.get("omitted_reason", ""),
                "admittedQuoteSides": int(capacity.get("admitted_quote_sides", 0)),
            })
        allocation = asdict(self._allocation) if self._allocation else None
        gross_units = sum(
            abs(int(item["positionUnits"]))
            for item in portfolio_items if isinstance(item.get("positionUnits"), (int, float))
        )
        net_units = sum(
            int(item["positionUnits"])
            for item in portfolio_items if isinstance(item.get("positionUnits"), (int, float))
        )
        api_activity = merge_activity_snapshots(activity_sources)
        bots_running = sum(int(item["botsRunning"]) for item in workers)
        rounded_pnl = {
            key: round(value, 4) if isinstance(value, float) else value
            for key, value in pnl_totals.items()
        }
        portfolio = {
            "items": portfolio_items,
            "grossPositionUnits": gross_units,
            "netPositionUnits": net_units,
            "unknownMarkets": sum(1 for item in portfolio_items if not item["available"]),
            "staleMarkets": sum(1 for item in portfolio_items if item["stale"]),
        }
        heartbeat_ages = [int(item["heartbeatAgeMs"]) for item in workers if item.get("heartbeatAgeMs") is not None]
        heartbeat_times = [int(item["heartbeatAtMs"]) for item in workers if item.get("heartbeatAtMs")]
        shard_health = {
            "activeShards": sum(1 for item in workers if item.get("running") and not item.get("stale")),
            "totalShards": len(workers),
            "activeActors": bots_running,
            "totalMemoryRssBytes": sum(int(item.get("memoryRssBytes") or 0) for item in workers),
            "totalCpuPercent": round(sum(float(item.get("cpuPercent") or 0.0) for item in workers), 3),
            "staleShards": sum(1 for item in workers if item.get("stale")),
            "degradedShards": sum(1 for item in workers if item.get("degraded")),
            "starvedShards": sum(1 for item in workers if item.get("starved")),
            "recoveringShards": sum(1 for item in workers if item.get("recovering")),
            "oldestHeartbeatAgeMs": max(heartbeat_ages, default=None),
            "latestHeartbeatAtMs": max(heartbeat_times, default=None),
            "recoveryCount": sum(int(item.get("recoveryCount") or 0) for item in workers),
            "lastRecoveryAtMs": max(
                (int(item["lastRecoveryAtMs"]) for item in workers if item.get("lastRecoveryAtMs")),
                default=None,
            ),
        }
        return {
            "venue": self.venue,
            "bots": rows,
            "workers": workers,
            "broker": {
                "pid": self._broker.pid if self._broker else None,
                "running": self._broker.is_alive() if self._broker else False,
                "stalled": bool(self._broker_stalled or self.broker_stall_reason()),
                "stallReason": self._broker_stall_reason or self.broker_stall_reason() or None,
                "heartbeatAtMs": self._broker_last_seen_ms or None,
                "lastVenueSuccessAtMs": self._broker_last_success_ms or None,
                "consecutiveTimeouts": self._broker_consecutive_timeouts,
                "restarts": self._broker_restart_count,
                "reconcilePendingBrokerReady": self._reconcile_after_broker_ready,
                "queue": dict(self._broker_stats),
                "apiActivity": dict(self._broker_api_activity),
                "apiErrors": dict(self._broker_api_errors),
                "exposureLimits": dict(self._last_exposure_limits),
                "shardExposure": dict(self._broker_shard_exposure),
            },
            "capacity": capacity,
            "allocation": allocation,
            "counts": {
                "desiredBots": len(self._desired), "managedBots": len(rows),
                "runningBots": bots_running,
                "activeBots": bots_running,
                "configuredBots": len(self._desired),
                "workers": len(workers), "staleWorkers": sum(1 for item in workers if item["stale"]),
                "degradedWorkers": sum(1 for item in workers if item.get("degraded")),
                "starvedWorkers": sum(1 for item in workers if item.get("starved")),
                "watchdogModes": risk_modes,
            },
            "monitoring": {
                "shutdownCleanup": dict(self._shutdown_cleanup), "hostWarnings": self._host_warnings,
                "threadBudget": dict(self._thread_budget),
                "botsRunning": bots_running,
                "queueDepth": sum(int(item.get("queueDepth") or 0) for item in workers),
                "portfolio": portfolio,
                "pnl": rounded_pnl,
                "apiActivity": api_activity,
                "shardHealth": shard_health,
            },
            "shardHealth": shard_health,
            "clients": clients,
            "portfolio": portfolio,
            "pnl": rounded_pnl,
            "apiActivity": api_activity,
            "threadBudget": dict(self._thread_budget),
        }
