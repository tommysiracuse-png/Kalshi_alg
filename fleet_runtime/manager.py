"""Controller-side lifecycle for the sharded worker fleet."""

from __future__ import annotations

import asyncio
import json
import math
import multiprocessing as mp
import os
import queue
import shutil
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from adaptors.kalshi import KalshiClientConfig
from bot_manager import BotManagerConfig
from clients.base_client import BaseClient
from clients.models import AccountOrderQuery, AccountPositionQuery
from fleet_models import BotManagerEvent, FleetCapacity, ScreenerPick, ScreenerUpdate, WorkerHeartbeat
from session_config import default_session_configuration, validate_session_configuration
from .assignment import assign_markets, derive_worker_count
from .capacity import AllocationRequest, AllocationResult, CapitalAllocator, calculate_fleet_capacity
from .execution import BrokerRequest, ExecutionBrokerProcess
from .worker import FleetWorkerProcess


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


def validate_host_resources(*, strict: bool) -> list[str]:
    warnings: list[str] = []
    cpu_count = os.cpu_count() or 0
    if cpu_count < 8:
        warnings.append(f"host has {cpu_count} vCPU; 8 required")
    memory_bytes = 0
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
            response = self.response_queue.get(timeout=max(0.01, deadline - time.monotonic()))
            if response.get("request_id") != request_id:
                continue
            if not response.get("ok"):
                raise RuntimeError(str(response.get("error") or f"broker {operation} failed"))
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
        self.max_bots = int(self.configuration["launcher"]["maxBots"])
        self.shard_size = int(self.fleet_config["shardSize"])
        self.worker_count = derive_worker_count(self.max_bots, self.shard_size)
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
        self._started = False
        self._shutdown_started = False
        self._request_queue: Any = None
        self._heartbeat_queue: Any = None
        self._broker_status_queue: Any = None
        self._broker: Optional[ExecutionBrokerProcess] = None
        self._response_queues: dict[str, Any] = {}
        self._admin: Optional[_BrokerAdmin] = None
        self._client_config: Optional[KalshiClientConfig] = None
        self._capacity: Optional[FleetCapacity] = None
        self._allocation: Optional[AllocationResult] = None
        self._capacity_error: str = ""
        self._host_warnings: list[str] = []
        self._planning_only = bool(config.dry_run and not (config.api_key_id and config.private_key_path))
        self._shutdown_cleanup: dict[str, Any] = {
            "state": "idle", "startedAtMs": None, "completedAtMs": None,
            "canceledOrders": 0, "ordersVerifiedAbsent": False, "error": None,
        }

    @property
    def current_picks(self) -> dict[str, ScreenerPick]:
        return dict(self._desired)

    def begin_shutdown(self) -> None:
        self._shutdown_started = True

    async def _emit(self, event_type: str, market_id: str, **detail: object) -> None:
        await self.events.put(BotManagerEvent(event_type, market_id, int(time.time() * 1000), detail))

    def _artifact_root(self) -> Path:
        path = self.config.bot_artifacts_root or self.config.logs_directory.parent
        path = Path(path)
        return path.parent if path.name == "markets" else path

    def _make_worker(self, worker_id: str, response_queue: Any, command_queue: Any) -> FleetWorkerProcess:
        assert self._client_config is not None
        return FleetWorkerProcess(
            worker_id=worker_id,
            client_config=self._client_config,
            session_configuration=self.configuration,
            artifact_root=self._artifact_root(),
            broker_request_queue=self._request_queue,
            broker_response_queue=response_queue,
            command_queue=command_queue,
            heartbeat_queue=self._heartbeat_queue,
        )

    async def _ensure_started(self) -> None:
        if self._started:
            return
        if self._shutdown_started:
            raise RuntimeError("fleet is shutting down")
        self._host_warnings = validate_host_resources(strict=self.max_bots > 40 and not self.config.dry_run)
        if self._planning_only:
            self._started = True
            return
        self._request_queue = mp.Queue()
        self._heartbeat_queue = mp.Queue()
        self._broker_status_queue = mp.Queue()
        response_queues: dict[str, Any] = {"controller": mp.Queue()}
        for index in range(self.worker_count):
            response_queues[f"worker-{index:02d}"] = mp.Queue()
        self._client_config = KalshiClientConfig(
            api_key_id=self.config.api_key_id or "",
            private_key_path=self.config.private_key_path or "",
            public_only=not bool(self.config.api_key_id and self.config.private_key_path),
            use_demo_environment=self.config.use_demo,
            dry_run=self.config.dry_run,
            subaccount_number=int(self.config.subaccount or 0),
        )
        self._response_queues = response_queues
        self._broker = ExecutionBrokerProcess(
            client_config=self._client_config,
            request_queue=self._request_queue,
            response_queues=response_queues,
            status_queue=self._broker_status_queue,
            write_utilization_limit=float(self.fleet_config["writeUtilizationLimit"]),
        )
        self._broker.start()
        self._admin = _BrokerAdmin(self._request_queue, response_queues["controller"])
        for index in range(self.worker_count):
            worker_id = f"worker-{index:02d}"
            command_queue = mp.Queue()
            process = self._make_worker(worker_id, response_queues[worker_id], command_queue)
            process.start()
            self._workers[worker_id] = ManagedWorker(worker_id, process, command_queue, response_queues[worker_id])
        self._started = True

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
        else:
            if self._admin is None:
                raise RuntimeError("execution broker is unavailable for admission")
            snapshot = await self._admin.call("admission_snapshot", timeout=float(self.fleet_config["startupTimeoutSeconds"]))
            limits = snapshot["limits"]
            balance = snapshot["balance"]
            orders = snapshot["orders"]
            positions = snapshot["positions"]
            order_notional = sum(max(0, int(item.price_units or 0)) * max(0, int(item.remaining_count_units)) // 100 for item in orders)
            position_notional = sum(max(0, int(item.market_exposure_units or 0)) for item in positions)
            committed = order_notional + position_notional
            available = balance.available_cash_units
            capacity = calculate_fleet_capacity(
                api_tier=limits.usage_tier, read_refill_rate=limits.read.refill_rate,
                write_refill_rate=limits.write.refill_rate, requested_markets=len(picks),
                cash_available_units=available, cash_committed_units=committed,
                freshness_seconds=float(self.fleet_config["quoteFreshnessSeconds"]),
                write_utilization_limit=float(self.fleet_config["writeUtilizationLimit"]),
                cash_reserve_fraction=float(self.fleet_config["cashReserveFraction"]),
            )
        requests = []
        for rank, pick in enumerate(picks.values()):
            ranking = pick.ranking
            series_id = str(ranking.get("Series Ticker") or ranking.get("series_ticker") or pick.market_id.split("-")[0])
            first_side = "yes" if pick.yes_budget_cents >= pick.no_budget_cents else "no"
            first_budget = pick.yes_budget_cents if first_side == "yes" else pick.no_budget_cents
            second_budget = pick.no_budget_cents if first_side == "yes" else pick.yes_budget_cents
            requests.append(AllocationRequest(
                pick.market_id, series_id, rank, first_side,
                max(0, int(first_budget)) * 100, max(0, int(second_budget)) * 100,
            ))
        allocation = CapitalAllocator(
            cash_reserve_fraction=float(self.fleet_config["cashReserveFraction"]),
            series_exposure_fraction=float(self.fleet_config["seriesExposureFraction"]),
        ).allocate(
            requests, available_cash_units=available, existing_committed_units=committed,
            quote_side_capacity=capacity.normal_quote_side_capacity,
        )
        return capacity, allocation

    async def apply_update(self, update: ScreenerUpdate) -> None:
        if self._shutdown_started:
            await self._emit("update_skipped_shutdown", "*", generation_id=update.generation_id)
            return
        if len(update.picks) > self.max_bots:
            raise ValueError(f"screener returned {len(update.picks)} markets; configured maximum is {self.max_bots}")
        await self._ensure_started()
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
        for worker_id, managed in self._workers.items():
            managed.command_queue.put({
                "action": "reconcile",
                "picks": tuple(desired[ticker] for ticker in assignments[worker_id]),
                "generation_id": update.generation_id,
            })
        self._capacity, self._allocation = await self._admission(desired)
        self._capacity_error = self._capacity.error
        gate_open = self._capacity.gate_open and self._allocation.gate_open
        allocations = self._allocation.sides_by_ticker if gate_open else {}
        for managed in self._workers.values():
            managed.command_queue.put({"action": "enable_quoting", "enabled": gate_open, "allocations": allocations})
        if not gate_open:
            await self._emit(
                "capacity_fail_closed", "*",
                capacity_error=self._capacity.error, allocation_error=self._allocation.error,
            )
        manifest = {
            "schemaVersion": 1,
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

    async def monitor_once(self) -> None:
        if not self._started:
            return
        if self._broker is None or not self._broker.is_alive():
            for managed in self._workers.values():
                managed.command_queue.put({"action": "enable_quoting", "enabled": False, "allocations": {}})
            self._capacity_error = "execution broker exited; fleet is reduction-only during restart"
            assert self._client_config is not None
            self._broker = ExecutionBrokerProcess(
                client_config=self._client_config,
                request_queue=self._request_queue,
                response_queues=self._response_queues,
                status_queue=self._broker_status_queue,
                write_utilization_limit=float(self.fleet_config["writeUtilizationLimit"]),
            )
            self._broker.start()
            await self._emit("broker_restarted", "*", pid=self._broker.pid)
        broker_changed = False
        while True:
            try:
                message = self._broker_status_queue.get_nowait()
            except queue.Empty:
                break
            message_type = str(message.get("type") or "")
            if message_type == "capacity":
                limits = message.get("limits")
                if limits is not None:
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
                    broker_changed = True
            elif message_type in {"capacity_error", "startup_error"}:
                self._capacity_error = str(message.get("error") or "execution broker unavailable")
                broker_changed = True
        if broker_changed:
            gate_open = bool(
                self._capacity and self._capacity.gate_open and self._allocation and self._allocation.gate_open
                and not self._capacity_error
            )
            allocations = self._allocation.sides_by_ticker if gate_open and self._allocation else {}
            for managed in self._workers.values():
                managed.command_queue.put({"action": "enable_quoting", "enabled": gate_open, "allocations": allocations})
            if not gate_open:
                await self._emit("runtime_capacity_fail_closed", "*", error=self._capacity_error)
        while True:
            try:
                heartbeat = self._heartbeat_queue.get_nowait()
            except queue.Empty:
                break
            managed = self._workers.get(heartbeat.worker_id)
            if managed:
                managed.heartbeat = heartbeat
        current = int(time.time() * 1000)
        stale_ms = int(float(self.fleet_config["workerStaleSeconds"]) * 1000)
        for worker_id, managed in list(self._workers.items()):
            stale = managed.heartbeat is not None and current - managed.heartbeat.generated_at_ms > stale_ms
            dead = not managed.process.is_alive()
            if not stale and not dead:
                continue
            tickers = self._assignments.get(worker_id, ())
            if self._admin is not None:
                for ticker in tickers:
                    await self._admin.call("cancel_market", {"market_id": ticker})
            managed.command_queue.put({"action": "stop"})
            await asyncio.to_thread(managed.process.join, 5.0)
            if managed.process.is_alive():
                managed.process.terminate()
                await asyncio.to_thread(managed.process.join, 2.0)
            process = self._make_worker(worker_id, managed.response_queue, managed.command_queue)
            process.start()
            managed.process = process
            managed.heartbeat = None
            managed.command_queue.put({"action": "reconcile", "picks": tuple(self._desired[ticker] for ticker in tickers)})
            await self._emit("worker_restarted", "*", worker_id=worker_id, affected_markets=len(tickers))

    async def stop_all(self, *, reason: str = "launcher_shutdown") -> None:
        self.begin_shutdown()
        if not self._started:
            return
        started = int(time.time() * 1000)
        self._shutdown_cleanup.update(state="running", startedAtMs=started)
        for managed in self._workers.values():
            managed.command_queue.put({"action": "freeze"})
        await asyncio.sleep(0.1)
        canceled = 0
        try:
            if self._admin is not None:
                canceled += int(await self._admin.call("cancel_all") or 0)
            for managed in self._workers.values():
                managed.command_queue.put({"action": "stop"})
            await asyncio.gather(*(asyncio.to_thread(item.process.join, 30.0) for item in self._workers.values()))
            for managed in self._workers.values():
                if managed.process.is_alive():
                    managed.process.terminate()
                    await asyncio.to_thread(managed.process.join, 5.0)
            if self._admin is not None:
                canceled += int(await self._admin.call("verify_clear") or 0)
                await self._admin.call("stop")
            if self._broker is not None:
                await asyncio.to_thread(self._broker.join, 30.0)
                if self._broker.is_alive():
                    self._broker.terminate()
                    await asyncio.to_thread(self._broker.join, 5.0)
            self._shutdown_cleanup.update(
                state="verified", completedAtMs=int(time.time() * 1000), canceledOrders=canceled,
                ordersVerifiedAbsent=True,
            )
        except Exception as exc:
            self._shutdown_cleanup.update(state="failed", completedAtMs=int(time.time() * 1000), error=str(exc))
            raise
        finally:
            self._started = False
            self.bots.clear()

    def status_snapshot(self) -> dict[str, Any]:
        now = int(time.time() * 1000)
        rows = []
        clients = []
        workers = []
        risk_modes: dict[str, int] = {}
        for worker_id, managed in self._workers.items():
            heartbeat = managed.heartbeat
            workers.append({
                "workerId": worker_id, "pid": managed.process.pid,
                "running": managed.process.is_alive(),
                "assignedMarkets": len(self._assignments.get(worker_id, ())),
                "heartbeatAtMs": heartbeat.generated_at_ms if heartbeat else None,
                "stale": heartbeat is None or now - heartbeat.generated_at_ms > int(float(self.fleet_config["workerStaleSeconds"]) * 1000),
                "memoryRssBytes": heartbeat.memory_rss_bytes if heartbeat else None,
                "queueDepth": heartbeat.queue_depth if heartbeat else None,
                "eventLagMs": heartbeat.event_lag_ms if heartbeat else None,
            })
            health = heartbeat.market_health if heartbeat else {}
            for ticker in self._assignments.get(worker_id, ()):
                item = health.get(ticker)
                pick = self._desired.get(ticker)
                risk_mode = item.risk_mode if item else "startup"
                risk_modes[risk_mode] = risk_modes.get(risk_mode, 0) + 1
                rows.append({
                    "ticker": ticker, "marketId": ticker, "title": pick.title if pick else ticker,
                    "workerId": worker_id, "pid": managed.process.pid,
                    "botRunning": managed.process.is_alive(), "watchdogRunning": False,
                    "yesBudgetCents": pick.yes_budget_cents if pick else 0,
                    "noBudgetCents": pick.no_budget_cents if pick else 0,
                    "watchdogMode": risk_mode,
                    "bookReady": item.book_available if item else False,
                    "quoteAgeMs": item.decision_age_ms if item else None,
                    "eventLagMs": item.book_age_ms if item else None,
                    "positionUnits": item.position_units if item else None,
                    "restingOrderCount": item.resting_order_count if item else 0,
                    "socketHealthy": heartbeat is not None,
                })
                clients.append({
                    "marketId": ticker,
                    "title": pick.title if pick else ticker,
                    "pid": managed.process.pid,
                    "lifecycle": "running" if managed.process.is_alive() else "stopped",
                    "socketHealthy": heartbeat is not None,
                    "market": {"marketId": ticker, "title": pick.title if pick else ticker},
                    "portfolio": {
                        "currentPositionUnits": item.position_units if item else None,
                        "updatedAtMs": heartbeat.generated_at_ms if heartbeat else None,
                    },
                    "runtime": {
                        "startedAtMs": item.started_at_ms if item else None,
                        "runningForMs": max(0, now - item.started_at_ms) if item and item.started_at_ms else None,
                    },
                    "pnl": dict(item.pnl) if item else {},
                    "fills": {"count": item.fill_count if item else 0, "recent": []},
                    "orderActivity": {
                        "byAction": dict(item.order_activity) if item else {},
                        "active": {}, "recent": [],
                    },
                    "apiActivity": {"rest": {}},
                    "watchdog": {
                        "running": managed.process.is_alive(), "mode": risk_mode,
                        "reason": item.error if item and item.error else None,
                        "updatedAtMs": heartbeat.generated_at_ms if heartbeat else None,
                    },
                    "quoteAgeMs": item.decision_age_ms if item else None,
                    "eventLagMs": item.book_age_ms if item else None,
                })
        capacity = asdict(self._capacity) if self._capacity else None
        allocation = asdict(self._allocation) if self._allocation else None
        return {
            "bots": rows,
            "workers": workers,
            "broker": {"pid": self._broker.pid if self._broker else None, "running": self._broker.is_alive() if self._broker else False},
            "capacity": capacity,
            "allocation": allocation,
            "counts": {
                "desiredBots": len(self._desired), "managedBots": len(rows),
                "runningBots": sum(1 for item in rows if item["botRunning"]),
                "activeBots": sum(1 for item in rows if item["botRunning"]),
                "configuredBots": len(self._desired),
                "workers": len(workers), "staleWorkers": sum(1 for item in workers if item["stale"]),
                "watchdogModes": risk_modes,
            },
            "monitoring": {
                "shutdownCleanup": dict(self._shutdown_cleanup), "hostWarnings": self._host_warnings,
                "botsRunning": sum(1 for item in rows if item["botRunning"]),
                "queueDepth": sum(int(item.get("queueDepth") or 0) for item in workers),
            },
            "clients": clients,
            "portfolio": {"items": []},
            "pnl": {"fills": 0, "feesCents": 0.0, "realizedCents": 0.0, "unrealizedCents": 0.0, "totalCents": 0.0},
        }
