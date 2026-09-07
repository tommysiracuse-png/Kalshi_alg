"""Incremental lifecycle manager for TopOfBookBot subprocesses."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, Mapping, Optional

from clients.base_client import BaseClient
from clients.models import AccountOrderQuery, OrderNotFoundError
from core.fleet_models import MAX_CONCURRENT_BOTS, BotManagerEvent, ScreenerPick, ScreenerUpdate
from apps.lip_launcher import (
    WATCHDOG_EXIT_CODES,
    ChildProcess,
    close_child_log,
    disable_ticker,
    load_disable_list,
    load_json_file,
    safe_ticker_filename,
    spawn_bots,
)
from core.runtime_control import send_bot_command


def _merge_counts(target: Dict[str, object], source: Dict[str, object]) -> None:
    for key, value in source.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if key.endswith("AtMs"):
                target[key] = max(float(target.get(key, 0) or 0), float(value))
            elif key != "averageLatencyMs":
                target[key] = target.get(key, 0) + value  # type: ignore[operator]
        elif isinstance(value, dict):
            child = target.setdefault(key, {})
            if isinstance(child, dict):
                _merge_counts(child, value)


@dataclass(frozen=True)
class BotManagerConfig:
    bot_script_path: Path
    logs_directory: Path
    runtime_dir: Path
    watchdog_state_dir: Path
    watchdog_disable_file: Path
    watchdog_runner_script_path: Optional[Path]
    watchdog_profiler_script_path: Optional[Path]
    api_key_id: Optional[str] = None
    private_key_path: Optional[str] = None
    use_demo: bool = False
    dry_run: bool = False
    subaccount: Optional[int] = None
    launch_delay_seconds: float = 0.0
    pass_credentials_via_cli: bool = False
    watchdog_interval_seconds: float = 180.0
    watchdog_state_refresh_seconds: float = 3.0
    watchdog_extreme_stale_seconds: float = 30.0
    watchdog_sample_seconds: float = 4.0
    watchdog_poll_interval_seconds: float = 0.35
    watchdog_confidence_reduction_threshold: float = 0.70
    watchdog_confidence_flatten_threshold: float = 0.55
    restart_limit: int = 3
    restart_window_seconds: float = 600.0
    shutdown_cleanup_attempts: int = 8
    shutdown_cleanup_delay_seconds: float = 0.5
    session_configuration: Optional[Dict[str, object]] = None
    bot_artifacts_root: Optional[Path] = None
    # Venue-neutral construction inputs.  The legacy credential fields remain
    # for CLI compatibility and are translated by the launcher/factory.
    venue: str = "kalshi"
    client_config: Optional[Any] = None


@dataclass
class ManagedBot:
    pick: ScreenerPick
    child: ChildProcess
    restart_times: Deque[float] = field(default_factory=deque)
    last_status: Dict[str, object] = field(default_factory=dict)
    socket_healthy: bool = False
    stopping: bool = False


class BotManager:
    BOT_ORDER_PREFIXES = ("mm:", "tob:", "wd:")

    def __init__(self, config: BotManagerConfig, cleanup_client: Optional[BaseClient] = None) -> None:
        self.config = config
        self.cleanup_client = cleanup_client
        self.bots: Dict[str, ManagedBot] = {}
        self.events: "asyncio.Queue[BotManagerEvent]" = asyncio.Queue()
        self._desired: Dict[str, ScreenerPick] = {}
        self._restart_history: Dict[str, Deque[float]] = {}
        self._generation = 0
        self._shutdown_started = False
        self._shutdown_cleanup: Dict[str, object] = {
            "state": "idle",
            "startedAtMs": None,
            "completedAtMs": None,
            "canceledOrders": 0,
            "ordersVerifiedAbsent": False,
            "error": None,
        }

    @property
    def current_picks(self) -> Dict[str, ScreenerPick]:
        return dict(self._desired)

    def begin_shutdown(self) -> None:
        """Synchronously close the process-start gate as soon as a stop is requested."""
        self._shutdown_started = True

    async def _emit(self, event_type: str, market_id: str, **detail: object) -> None:
        venue = str(self.config.venue or "kalshi")
        detail.setdefault("venue", venue)
        await self.events.put(
            BotManagerEvent(event_type, market_id, int(time.time() * 1000), detail, venue)
        )

    def _spawn_sync(self, pick: ScreenerPick, slot_index: int) -> Optional[ChildProcess]:
        children = spawn_bots(
            picks=[pick],
            bot_script_path=self.config.bot_script_path,
            logs_directory=self.config.logs_directory,
            api_key_id=self.config.api_key_id,
            private_key_path=self.config.private_key_path,
            use_demo=self.config.use_demo,
            dry_run=self.config.dry_run,
            subaccount=self.config.subaccount,
            launch_delay_seconds=0,
            pass_credentials_via_cli=self.config.pass_credentials_via_cli,
            watchdog_runner_script_path=self.config.watchdog_runner_script_path,
            watchdog_profiler_script_path=self.config.watchdog_profiler_script_path,
            watchdog_state_dir=self.config.watchdog_state_dir,
            watchdog_disable_file=self.config.watchdog_disable_file,
            watchdog_interval_seconds=self.config.watchdog_interval_seconds,
            watchdog_state_refresh_seconds=self.config.watchdog_state_refresh_seconds,
            watchdog_extreme_stale_seconds=self.config.watchdog_extreme_stale_seconds,
            watchdog_sample_seconds=self.config.watchdog_sample_seconds,
            watchdog_poll_interval_seconds=self.config.watchdog_poll_interval_seconds,
            watchdog_confidence_reduction_threshold=self.config.watchdog_confidence_reduction_threshold,
            watchdog_confidence_flatten_threshold=self.config.watchdog_confidence_flatten_threshold,
            control_socket_directory=self.config.runtime_dir / "bots",
            session_configuration=self.config.session_configuration,
            bot_artifacts_root=self.config.bot_artifacts_root,
        )
        if not children:
            return None
        child = children[0]
        child.slot_index = slot_index
        return child

    async def start_bot(self, pick: ScreenerPick, *, restart: bool = False) -> Optional[ManagedBot]:
        if self._shutdown_started:
            await self._emit("start_skipped_shutdown", pick.market_id)
            return None
        if pick.market_id not in self.bots and len(self.bots) >= MAX_CONCURRENT_BOTS:
            await self._emit(
                "start_skipped_capacity",
                pick.market_id,
                active_bots=len(self.bots),
                maximum_bots=MAX_CONCURRENT_BOTS,
            )
            return None
        if pick.market_id in load_disable_list(self.config.watchdog_disable_file):
            await self._emit("start_skipped_disabled", pick.market_id)
            return None
        child = await asyncio.to_thread(self._spawn_sync, pick, len(self.bots))
        if child is None:
            await self._emit("dry_run", pick.market_id)
            return None
        if self._shutdown_started:
            self._signal_process(child.process, "terminate")
            if child.watchdog_process is not None:
                self._signal_process(child.watchdog_process, "terminate")
            close_child_log(child)
            await self._emit("start_aborted_shutdown", pick.market_id)
            return None
        managed = ManagedBot(
            pick=pick,
            child=child,
            restart_times=self._restart_history.setdefault(pick.market_id, deque()),
        )
        self.bots[pick.market_id] = managed
        await self._emit("restarted" if restart else "started", pick.market_id, pid=child.process.pid)
        if self.config.launch_delay_seconds > 0:
            await asyncio.sleep(self.config.launch_delay_seconds)
        return managed

    @staticmethod
    def _signal_process(process: object, method: str) -> None:
        try:
            getattr(process, method)()
        except Exception:
            pass

    async def _wait_process(self, process: object, timeout: float) -> bool:
        try:
            await asyncio.to_thread(process.wait, timeout=timeout)
            return True
        except (subprocess.TimeoutExpired, AttributeError):
            return getattr(process, "poll")() is not None

    @classmethod
    def _is_bot_order(cls, client_order_id: str) -> bool:
        return bool(client_order_id) and client_order_id.startswith(cls.BOT_ORDER_PREFIXES)

    def _cancel_and_verify_owned_orders_sync(self, market_id: str = "") -> int:
        """Cancel bot-tagged resting orders and verify they no longer exist.

        This is deliberately account-backed rather than based on a child's
        cached state, and is the manager fallback when a child socket is absent
        or a process dies during shutdown.
        """
        if self.cleanup_client is None or self.config.dry_run:
            return 0

        canceled_ids: set[str] = set()
        last_error: Optional[Exception] = None
        attempts = max(1, int(self.config.shutdown_cleanup_attempts))
        for attempt in range(attempts):
            try:
                orders = self.cleanup_client.list_account_orders(
                    AccountOrderQuery(status="resting", market_id=market_id, page_size=1_000)
                )
                owned = [order for order in orders if order.order_id and self._is_bot_order(order.client_order_id)]
                if not owned:
                    return len(canceled_ids)
                for order in owned:
                    try:
                        self.cleanup_client.cancel_order(order_id=order.order_id)
                        canceled_ids.add(order.order_id)
                    except OrderNotFoundError:
                        canceled_ids.add(order.order_id)
                last_error = None
            except Exception as exc:
                last_error = exc

            if attempt + 1 < attempts:
                time.sleep(min(4.0, self.config.shutdown_cleanup_delay_seconds * (2 ** attempt)))

        scope = market_id or "the account"
        detail = f": {last_error}" if last_error is not None else ""
        raise RuntimeError(f"could not verify cancellation of all bot-owned orders for {scope}{detail}")

    async def _cancel_and_verify_owned_orders(self, market_id: str = "") -> int:
        if self.cleanup_client is None or self.config.dry_run:
            return 0
        # Shutdown is intentionally serialized around this authoritative REST
        # sweep; no launcher work should race it or start new bots.
        return self._cancel_and_verify_owned_orders_sync(market_id)

    async def stop_bot(self, market_id: str, *, reason: str = "reconcile", remove_desired: bool = True) -> None:
        managed = self.bots.get(market_id)
        if remove_desired:
            self._desired.pop(market_id, None)
        if managed is None:
            return
        managed.stopping = True
        socket_path = managed.child.control_socket
        if socket_path is not None and socket_path.exists():
            try:
                response = await asyncio.to_thread(
                    send_bot_command,
                    socket_path,
                    {"request_id": f"shutdown-{uuid.uuid4().hex}", "action": "shutdown"},
                    15.0,
                )
                if not response.get("ok"):
                    await self._emit("shutdown_socket_error", market_id, response=response)
            except Exception as exc:
                await self._emit("shutdown_socket_error", market_id, error=str(exc))
        exited = await self._wait_process(managed.child.process, 5.0)
        if not exited:
            try:
                managed.child.process.send_signal(signal.SIGINT)
            except Exception:
                pass
            exited = await self._wait_process(managed.child.process, 5.0)
        if not exited:
            self._signal_process(managed.child.process, "terminate")
            exited = await self._wait_process(managed.child.process, 0.5)
        if not exited:
            self._signal_process(managed.child.process, "kill")
            await self._wait_process(managed.child.process, 1.0)
        if managed.child.watchdog_process is not None:
            self._signal_process(managed.child.watchdog_process, "terminate")

        cleanup_error: Optional[Exception] = None
        canceled = 0
        try:
            canceled = await self._cancel_and_verify_owned_orders(market_id)
        except Exception as exc:
            cleanup_error = exc
        close_child_log(managed.child)
        if socket_path is not None:
            try:
                socket_path.unlink()
            except FileNotFoundError:
                pass
        self.bots.pop(market_id, None)
        if cleanup_error is not None:
            await self._emit("shutdown_cleanup_failed", market_id, reason=reason, error=str(cleanup_error))
            raise cleanup_error
        await self._emit("stopped", market_id, reason=reason, canceled_orders=canceled, orders_verified_absent=True)

    async def stop_all(self, *, reason: str = "launcher_shutdown") -> None:
        self.begin_shutdown()
        started_at_ms = int(time.time() * 1000)
        self._shutdown_cleanup = {
            "state": "running",
            "startedAtMs": started_at_ms,
            "completedAtMs": None,
            "canceledOrders": 0,
            "ordersVerifiedAbsent": False,
            "error": None,
        }
        self._desired.clear()

        # Fleet shutdown is intentionally different from incremental removal.
        # Stopping dozens of children serially made the UI/systemd stop exceed
        # its timeout before later bots were reached. Freeze every order source
        # first, perform one authoritative account cleanup, and only then retire
        # the processes.
        if self.cleanup_client is not None and not self.config.dry_run:
            managed_bots = list(self.bots.items())
            for market_id, managed in managed_bots:
                managed.stopping = True
                if managed.child.process.poll() is None:
                    try:
                        managed.child.process.send_signal(signal.SIGSTOP)
                    except Exception as exc:
                        await self._emit("shutdown_quiesce_failed", market_id, error=str(exc))
                        self._signal_process(managed.child.process, "terminate")
                if managed.child.watchdog_process is not None:
                    self._signal_process(managed.child.watchdog_process, "terminate")
                await self._emit("shutdown_quiesced", market_id, reason=reason)

            cleanup_error: Optional[Exception] = None
            canceled = 0
            try:
                canceled += await self._cancel_and_verify_owned_orders()
            except Exception as exc:
                cleanup_error = exc
                await self._emit("shutdown_cleanup_failed", "*", reason=reason, error=str(exc))

            # Frozen processes cannot place another order. Resume only so they
            # can receive termination; do not invoke their per-market REST
            # cleanup again after the account-wide verification.
            for _, managed in managed_bots:
                process = managed.child.process
                if process.poll() is None:
                    try:
                        process.send_signal(signal.SIGCONT)
                    except Exception:
                        pass
                    self._signal_process(process, "terminate")

            exit_results = await asyncio.gather(
                *(self._wait_process(managed.child.process, 5.0) for _, managed in managed_bots)
            )
            kill_targets = [
                managed
                for (_, managed), exited in zip(managed_bots, exit_results)
                if not exited
            ]
            for managed in kill_targets:
                self._signal_process(managed.child.process, "kill")
            if kill_targets:
                await asyncio.gather(
                    *(self._wait_process(managed.child.process, 1.0) for managed in kill_targets)
                )

            if cleanup_error is None:
                try:
                    canceled += await self._cancel_and_verify_owned_orders()
                except Exception as exc:
                    cleanup_error = exc
                    await self._emit("shutdown_cleanup_failed", "*", reason=reason, error=str(exc))

            for market_id, managed in managed_bots:
                close_child_log(managed.child)
                socket_path = managed.child.control_socket
                if socket_path is not None:
                    try:
                        socket_path.unlink()
                    except FileNotFoundError:
                        pass
                self.bots.pop(market_id, None)
                if cleanup_error is None:
                    await self._emit(
                        "stopped",
                        market_id,
                        reason=reason,
                        canceled_orders=canceled,
                        orders_verified_absent=True,
                    )

            if cleanup_error is not None:
                self._shutdown_cleanup.update(
                    state="failed",
                    completedAtMs=int(time.time() * 1000),
                    canceledOrders=canceled,
                    error=str(cleanup_error),
                )
                raise RuntimeError(f"launcher shutdown order cleanup failed: {cleanup_error}")
            self._shutdown_cleanup.update(
                state="verified",
                completedAtMs=int(time.time() * 1000),
                canceledOrders=canceled,
                ordersVerifiedAbsent=True,
            )
            await self._emit(
                "fleet_shutdown_cleanup_verified",
                "*",
                canceled_orders=canceled,
                orders_verified_absent=True,
            )
            return

        failures: list[str] = []
        for market_id in list(self.bots):
            try:
                await self.stop_bot(market_id, reason=reason, remove_desired=False)
            except Exception as exc:
                failures.append(f"{market_id}: {exc}")

        # Final account-wide sweep also catches stale orders from a bot that
        # exited before it entered this manager's current process registry.
        account_verified = False
        try:
            canceled = await self._cancel_and_verify_owned_orders()
            account_verified = True
            await self._emit(
                "fleet_shutdown_cleanup_verified",
                "*",
                canceled_orders=canceled,
                orders_verified_absent=True,
            )
        except Exception as exc:
            failures.append(f"account: {exc}")
            await self._emit("shutdown_cleanup_failed", "*", reason=reason, error=str(exc))

        if account_verified:
            # The final account query is authoritative and supersedes any
            # transient per-market verification failure.
            failures = [item for item in failures if item.startswith("account:")]
        if failures:
            error = "; ".join(failures)
            self._shutdown_cleanup.update(
                state="failed",
                completedAtMs=int(time.time() * 1000),
                canceledOrders=canceled if account_verified else 0,
                error=error,
            )
            raise RuntimeError("launcher shutdown order cleanup failed: " + error)
        self._shutdown_cleanup.update(
            state="verified",
            completedAtMs=int(time.time() * 1000),
            canceledOrders=canceled,
            ordersVerifiedAbsent=True,
        )

    async def apply_update(self, update: ScreenerUpdate) -> None:
        if self._shutdown_started:
            await self._emit("update_skipped_shutdown", "*", generation_id=update.generation_id)
            return
        if update.generation_id != self._generation:
            for market_id in update.pick_by_market_id:
                self._restart_history.setdefault(market_id, deque()).clear()
        self._generation = update.generation_id
        desired = update.pick_by_market_id
        self._desired = dict(desired)
        for market_id in update.removed:
            await self.stop_bot(market_id, reason="screen_removed", remove_desired=False)
        for market_id in update.changed:
            await self.stop_bot(market_id, reason="configuration_changed", remove_desired=False)
        for market_id in update.kept:
            if market_id in self.bots:
                self.bots[market_id].pick = desired[market_id]
            else:
                await self.start_bot(desired[market_id], restart=True)
        for market_id in (*update.added, *update.changed):
            await self.start_bot(desired[market_id])

    def _can_restart(self, market_id: str) -> tuple[bool, int]:
        now = time.time()
        history = self._restart_history.setdefault(market_id, deque())
        while history and now - history[0] > self.config.restart_window_seconds:
            history.popleft()
        return len(history) < self.config.restart_limit, len(history)

    async def _restart(self, market_id: str) -> None:
        pick = self._desired.get(market_id)
        if pick is None:
            return
        allowed, prior_count = self._can_restart(market_id)
        if not allowed:
            await self._emit("restart_exhausted", market_id, attempts=prior_count)
            return
        history = self._restart_history.setdefault(market_id, deque())
        history.append(time.time())
        await asyncio.sleep(2 ** prior_count)
        await self.start_bot(pick, restart=True)

    async def _poll_bot_status(self, managed: ManagedBot) -> None:
        socket_path = managed.child.control_socket
        if socket_path is None or not socket_path.exists():
            managed.socket_healthy = False
            return
        try:
            response = await asyncio.to_thread(
                send_bot_command,
                socket_path,
                {"request_id": f"status-{uuid.uuid4().hex}", "action": "status"},
                1.0,
            )
            managed.last_status = dict(response.get("result") or {})
            managed.socket_healthy = bool(response.get("ok"))
        except Exception:
            managed.socket_healthy = False

    async def monitor_once(self) -> None:
        for market_id, managed in list(self.bots.items()):
            return_code = managed.child.process.poll()
            if return_code is None:
                watchdog_process = managed.child.watchdog_process
                if watchdog_process is not None and watchdog_process.poll() is not None:
                    await self._emit(
                        "watchdog_exited",
                        market_id,
                        return_code=watchdog_process.poll(),
                    )
                    await self.stop_bot(
                        market_id,
                        reason="watchdog_process_exited",
                        remove_desired=False,
                    )
                    await self._restart(market_id)
                    continue
                await self._poll_bot_status(managed)
                continue
            if managed.child.watchdog_process is not None:
                self._signal_process(managed.child.watchdog_process, "terminate")
            close_child_log(managed.child)
            self.bots.pop(market_id, None)
            await self._emit("exited", market_id, return_code=return_code)
            if int(return_code) in WATCHDOG_EXIT_CODES:
                disable_ticker(
                    self.config.watchdog_disable_file,
                    market_id,
                    f"watchdog_exit_{int(return_code)}",
                    exit_code=int(return_code),
                )
                self._desired.pop(market_id, None)
                await self._emit("watchdog_disabled", market_id, return_code=return_code)
                continue
            await self._restart(market_id)

    def status_snapshot(self) -> Dict[str, object]:
        rows = []
        clients = []
        watchdog_modes: Dict[str, int] = {}
        portfolio_items = []
        pnl_totals: Dict[str, object] = {
            "fills": 0, "feesCents": 0.0, "realizedCents": 0.0,
            "unrealizedCents": 0.0, "totalCents": 0.0,
        }
        api_totals: Dict[str, object] = {"rest": {}, "stream": {}}
        current_ms = int(time.time() * 1000)
        for managed in self.bots.values():
            child = managed.child
            watchdog = load_json_file(child.watchdog_state_file) if child.watchdog_state_file else {}
            mode = str(watchdog.get("mode") or "unknown")
            watchdog_modes[mode] = watchdog_modes.get(mode, 0) + 1
            row = {
                    "ticker": child.ticker,
                    "title": managed.pick.title,
                    "slotIndex": child.slot_index,
                    "pid": child.process.pid,
                    "watchdogPid": child.watchdog_process.pid if child.watchdog_process else None,
                    "botRunning": child.process.poll() is None,
                    "watchdogRunning": child.watchdog_process.poll() is None if child.watchdog_process else False,
                    "yesBudgetCents": managed.pick.yes_budget_cents,
                    "noBudgetCents": managed.pick.no_budget_cents,
                    "logPath": str(child.log_path),
                    "watchdogLogPath": str(child.watchdog_log_path) if child.watchdog_log_path else None,
                    "watchdogMode": mode,
                    "watchdogConfidence": watchdog.get("confidence"),
                    "watchdogReason": watchdog.get("reason"),
                    "watchdogUpdatedAtMs": watchdog.get("runner_updated_at_ms") or watchdog.get("generated_at_ms"),
                    "socketPath": str(child.control_socket) if child.control_socket else None,
                    "socketHealthy": managed.socket_healthy,
                    "restartCount": len(managed.restart_times),
                    "botStatus": dict(managed.last_status),
                }
            rows.append(row)
            bot_status = managed.last_status
            monitoring = bot_status.get("monitoring") if isinstance(bot_status, dict) else None
            monitoring = monitoring if isinstance(monitoring, dict) else {}
            portfolio = monitoring.get("portfolio") if isinstance(monitoring.get("portfolio"), dict) else {}
            pnl = monitoring.get("pnl") if isinstance(monitoring.get("pnl"), dict) else {}
            api = monitoring.get("apiActivity") if isinstance(monitoring.get("apiActivity"), dict) else {}
            last_event_at = bot_status.get("lastMarketEventAtMs") if isinstance(bot_status, dict) else None
            fresh = (
                managed.socket_healthy
                and isinstance(last_event_at, (int, float))
                and current_ms - int(last_event_at) <= 15_000
            )
            position_units = portfolio.get("currentPositionUnits")
            portfolio_items.append(
                {
                    "marketId": child.ticker,
                    "title": managed.pick.title,
                    "positionUnits": position_units,
                    "updatedAtMs": portfolio.get("updatedAtMs"),
                    "stale": not fresh,
                    "available": position_units is not None,
                }
            )
            for key in pnl_totals:
                value = pnl.get(key)
                if isinstance(value, (int, float)):
                    pnl_totals[key] = pnl_totals[key] + value  # type: ignore[operator]
            _merge_counts(
                api_totals,
                {
                    "rest": api.get("rest", {}),
                    "stream": api.get("stream", {}),
                },
            )
            clients.append(
                {
                    "marketId": child.ticker,
                    "title": managed.pick.title,
                    "pid": child.process.pid,
                    "lifecycle": bot_status.get("lifecycle") or ("running" if row["botRunning"] else "stopped"),
                    "socketHealthy": managed.socket_healthy,
                    "restartCount": len(managed.restart_times),
                    "watchdog": {
                        "running": row["watchdogRunning"],
                        "mode": mode,
                        "confidence": row["watchdogConfidence"],
                        "reason": row["watchdogReason"],
                        "updatedAtMs": row["watchdogUpdatedAtMs"],
                    },
                    **monitoring,
                }
            )
        gross_units = sum(
            abs(int(item["positionUnits"]))
            for item in portfolio_items
            if isinstance(item.get("positionUnits"), (int, float))
        )
        net_units = sum(
            int(item["positionUnits"])
            for item in portfolio_items
            if isinstance(item.get("positionUnits"), (int, float))
        )
        monitoring_snapshot = {
            "running": True,
            "botsRunning": sum(1 for row in rows if row["botRunning"]),
            "portfolio": {
                "items": portfolio_items,
                "grossPositionUnits": gross_units,
                "netPositionUnits": net_units,
                "unknownMarkets": sum(1 for item in portfolio_items if not item["available"]),
                "staleMarkets": sum(1 for item in portfolio_items if item["stale"]),
            },
            "pnl": {key: round(value, 4) if isinstance(value, float) else value for key, value in pnl_totals.items()},
            "apiActivity": api_totals,
            "shutdownCleanup": dict(self._shutdown_cleanup),
        }
        rest_totals = api_totals.get("rest")
        if isinstance(rest_totals, dict):
            total = rest_totals.get("total")
            latency = rest_totals.get("totalLatencyMs")
            rest_totals["averageLatencyMs"] = round(float(latency or 0) / float(total), 3) if total else 0.0
        return {
            "bots": rows,
            "clients": clients,
            "monitoring": monitoring_snapshot,
            "counts": {
                "activeBots": sum(1 for row in rows if row["botRunning"]),
                "configuredBots": len(self._desired),
                "watchdogModes": watchdog_modes,
            },
        }
