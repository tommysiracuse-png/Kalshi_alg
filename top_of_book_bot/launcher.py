"""Async fleet coordinator that connects Screener, BotManager, and control events."""

from __future__ import annotations

import asyncio
import logging
import os
import queue
import signal
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Optional

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from core.bot_manager import BotManager, BotManagerConfig
from core.fleet_models import ScreenerPick, ScreenerUpdate
from screeners.kalshi_screener import build_parser as build_screener_parser
from screeners.kalshi_screener import build_settings_from_args as build_screener_settings
from apps.lip_launcher import (
    atomic_write_json,
    clear_disabled_ticker,
    disable_ticker,
    load_disable_list,
    load_screen_picks,
)
from core.runtime_control import ControlRequest, ControlServer, STATUS_SCHEMA_VERSION
from screeners.screener import Screener
from portfolio.portfolio_monitor import PortfolioMonitor, PortfolioMonitorConfig


LOGGER = logging.getLogger(__name__)


class Launcher:
    def __init__(self, arguments, *, api_key_id: Optional[str], private_key_path: Optional[str], session_store=None, session_run=None) -> None:
        self.arguments = arguments
        self.session_store = session_store
        self.session_run = session_run
        self.session_configuration = session_run["configuration"] if session_run else None
        self.run_id = session_run["id"] if session_run else None
        self.run_artifact_path = Path(session_run["artifactPath"]) if session_run else None
        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.bot_script_path = Path(arguments.bot_script).expanduser().resolve()
        self.screen_path = Path(arguments.screener_output or arguments.screen_file).expanduser().resolve()
        self.logs_directory = self.run_artifact_path / "logs" if self.run_artifact_path else (
            Path(arguments.logs_dir).expanduser().resolve()
            if arguments.logs_dir
            else self.bot_script_path.parent / "logs"
        )
        self.runtime_dir = (
            Path(arguments.runtime_dir).expanduser().resolve()
            if arguments.runtime_dir
            else self.bot_script_path.parent / "runtime"
        )
        self.watchdog_state_dir = self.run_artifact_path / "watchdog" if self.run_artifact_path else (
            Path(arguments.watchdog_state_dir).expanduser().resolve()
            if arguments.watchdog_state_dir
            else self.bot_script_path.parent / "watchdog_state"
        )
        self.watchdog_disable_file = (
            Path(arguments.watchdog_disable_file).expanduser().resolve()
            if arguments.watchdog_disable_file
            else self.bot_script_path.parent / "watchdog_disable_list.json"
        )
        watchdog_runner = (
            Path(arguments.watchdog_runner_script).expanduser().resolve()
            if arguments.watchdog_runner_script
            else self.bot_script_path.parent / "market_watchdog_runner.py"
        )
        watchdog_profiler = (
            Path(arguments.watchdog_profiler_script).expanduser().resolve()
            if arguments.watchdog_profiler_script
            else self.bot_script_path.parent / "market_risk_profiler.py"
        )

        client_config = KalshiClientConfig(
            api_key_id=api_key_id or "",
            private_key_path=private_key_path or "",
            public_only=not bool(api_key_id and private_key_path),
            use_demo_environment=bool(arguments.use_demo),
            dry_run=bool(arguments.dry_run),
            subaccount_number=int(arguments.subaccount or 0),
        )
        self.client = KalshiApiClient(client_config)
        self.portfolio_client = KalshiApiClient(client_config)
        self.portfolio = PortfolioMonitor(
            self.portfolio_client,
            PortfolioMonitorConfig(
                subaccount_number=int(arguments.subaccount or 0),
                analytics_path=self.runtime_dir / "portfolio_analytics.sqlite3",
            ),
        )
        screener_args = build_screener_parser().parse_args([])
        screener_args.output = str(self.screen_path)
        screener_settings = build_screener_settings(screener_args)
        if arguments.max_bots > 0:
            screener_settings["top_n"] = max(int(screener_settings["top_n"]), int(arguments.max_bots))
        self.screener = Screener(
            client=self.client,
            settings=screener_settings,
            output_path=self.screen_path,
            default_yes_budget_cents=arguments.yes_budget_cents,
            default_no_budget_cents=arguments.no_budget_cents,
            max_bots=int(arguments.max_bots),
            minimum_carryover_value_cents=arguments.minimum_carryover_value_cents,
            yes_budget_column=arguments.yes_budget_column,
            no_budget_column=arguments.no_budget_column,
            disabled_market_ids=lambda: set(load_disable_list(self.watchdog_disable_file)),
        )
        self.manager = BotManager(
            BotManagerConfig(
                bot_script_path=self.bot_script_path,
                logs_directory=self.logs_directory,
                runtime_dir=self.runtime_dir,
                watchdog_state_dir=self.watchdog_state_dir,
                watchdog_disable_file=self.watchdog_disable_file,
                watchdog_runner_script_path=watchdog_runner,
                watchdog_profiler_script_path=watchdog_profiler,
                api_key_id=api_key_id,
                private_key_path=private_key_path,
                use_demo=arguments.use_demo,
                dry_run=arguments.dry_run,
                subaccount=arguments.subaccount,
                launch_delay_seconds=arguments.launch_delay_seconds,
                pass_credentials_via_cli=arguments.pass_credentials_via_cli,
                watchdog_interval_seconds=arguments.watchdog_interval_seconds,
                watchdog_state_refresh_seconds=arguments.watchdog_state_refresh_seconds,
                watchdog_extreme_stale_seconds=arguments.watchdog_extreme_stale_seconds,
                watchdog_sample_seconds=arguments.watchdog_sample_seconds,
                watchdog_poll_interval_seconds=arguments.watchdog_poll_interval_seconds,
                watchdog_confidence_reduction_threshold=arguments.watchdog_confidence_reduction_threshold,
                watchdog_confidence_flatten_threshold=arguments.watchdog_confidence_flatten_threshold,
                session_configuration=self.session_configuration,
                bot_artifacts_root=(self.run_artifact_path / "markets") if self.run_artifact_path else None,
            ),
            cleanup_client=self.client if api_key_id and private_key_path else None,
        )
        self.control_requests: "queue.Queue[ControlRequest]" = queue.Queue()
        self.shutdown_requested = asyncio.Event()
        self.started_at_ms = int((session_run or {}).get("startedAt") or time.time() * 1000)
        self.next_refresh_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self.pending_action: Optional[Dict[str, object]] = None
        self.latest_status: Dict[str, object] = {}
        self.status_lock = threading.Lock()
        self.status_path = self.runtime_dir / "launcher_status.json"
        self.socket_path = self.runtime_dir / "launcher.sock"
        self._portfolio_task: Optional["asyncio.Task[bool]"] = None
        self._run_error: Optional[str] = None
        self._last_metric_sample_ms = 0
        self._observability_warning: Optional[str] = None
        self._last_observability_warning_log_ms = 0
        self._metrics = None
        if self.session_store is not None:
            from core.session_store import RunMetricsAccumulator
            self._metrics = RunMetricsAccumulator(self.started_at_ms)
        self._append_run_log("launcher initialized")

    def _append_run_log(self, message: str) -> None:
        if self.run_artifact_path is None:
            return
        self.run_artifact_path.mkdir(parents=True, exist_ok=True)
        with (self.run_artifact_path / "launcher.log").open("a", encoding="utf-8") as handle:
            handle.write(f"{int(time.time() * 1000)} {message}\n")

    def _save_screener_snapshot(self) -> None:
        if self.run_artifact_path is None:
            return
        target = self.run_artifact_path / "screener"
        target.mkdir(parents=True, exist_ok=True)
        snapshot = self.screener.status_snapshot()
        generated = int(snapshot.get("generatedAtMs") or time.time() * 1000)
        atomic_write_json(target / "latest.json", snapshot)
        atomic_write_json(target / f"{generated}.json", snapshot)

    def status_snapshot(self, lifecycle: str = "running") -> Dict[str, object]:
        manager_status = self.manager.status_snapshot()
        now_ms = int(time.time() * 1000)
        disabled = load_disable_list(self.watchdog_disable_file)
        counts = dict(manager_status["counts"])
        counts["disabledTickers"] = len(disabled)
        manager_monitoring = dict(manager_status.get("monitoring") or {})
        manager_monitoring.update(
            {
                "running": lifecycle in {"starting", "running"},
                "lifecycle": lifecycle,
                "startedAtMs": self.started_at_ms,
                "runningForMs": max(0, now_ms - self.started_at_ms),
            }
        )
        result = {
            "schemaVersion": STATUS_SCHEMA_VERSION,
            "generatedAt": now_ms,
            "launcher": {
                "pid": os.getpid(),
                "environment": "demo" if self.arguments.use_demo else "production",
                "lifecycle": lifecycle,
                "startedAt": self.started_at_ms,
                "heartbeatAt": now_ms,
                "nextRefreshAt": int(self.next_refresh_at * 1000) if self.next_refresh_at else None,
                "lastError": self.last_error,
                "pendingAction": self.pending_action,
                "observabilityWarning": self._observability_warning,
            },
            "counts": counts,
            "bots": manager_status["bots"],
            "manager": manager_monitoring,
            "clients": manager_status.get("clients", []),
            "screener": self.screener.status_snapshot(),
            "portfolio": self.portfolio.status_snapshot(),
            "disabled": disabled,
        }
        if self.session_run:
            result["session"] = {
                "id": self.session_run["sessionId"],
                "name": self.session_run["sessionName"],
                "configurationVersion": self.session_run["configurationVersion"],
            }
            result["run"] = {"id": self.run_id, "artifactPath": str(self.run_artifact_path)}
        return result

    def publish_status(self, lifecycle: str = "running") -> Dict[str, object]:
        status = self.status_snapshot(lifecycle)
        with self.status_lock:
            self.latest_status = status
        # In-memory status is published first so trading/control loops remain
        # usable even if an observability filesystem is temporarily unavailable.
        runtime_snapshot_ok = True
        try:
            atomic_write_json(self.status_path, status)
        except OSError as exc:
            runtime_snapshot_ok = False
            self._record_observability_failure("runtime snapshot", exc)
        if self.session_store is not None and self.run_id and self._metrics is not None:
            metrics = self._metrics.observe(status)
            current = int(time.time() * 1000)
            sample = current - self._last_metric_sample_ms >= 15_000
            try:
                self.session_store.record_metrics(self.run_id, metrics, sample=sample)
                if sample:
                    self._last_metric_sample_ms = current
                if runtime_snapshot_ok:
                    self._observability_warning = None
            except (sqlite3.Error, OSError) as exc:
                self._record_observability_failure("session metrics", exc)
        return status

    def _record_observability_failure(self, operation: str, exc: BaseException) -> None:
        current = int(time.time() * 1000)
        self._observability_warning = f"{operation} unavailable: {exc}"
        if current - self._last_observability_warning_log_ms >= 60_000:
            LOGGER.warning("observability failure does not stop trading: %s", self._observability_warning)
            self._last_observability_warning_log_ms = current

    def _status_provider(self) -> Dict[str, object]:
        with self.status_lock:
            return dict(self.latest_status)

    async def _seed_from_csv(self) -> None:
        if not self.screen_path.exists():
            return
        legacy = await asyncio.to_thread(
            load_screen_picks,
            screen_file_path=self.screen_path,
            ticker_column=self.arguments.ticker_column,
            title_column=self.arguments.title_column,
            yes_budget_cents=self.arguments.yes_budget_cents,
            no_budget_cents=self.arguments.no_budget_cents,
            yes_budget_column=self.arguments.yes_budget_column,
            no_budget_column=self.arguments.no_budget_column,
            max_bots=self.arguments.max_bots,
        )
        picks = tuple(
            ScreenerPick(
                market_id=pick.ticker,
                title=pick.title,
                yes_budget_cents=pick.yes_budget_cents,
                no_budget_cents=pick.no_budget_cents,
                ranking=pick.raw_row,
                selection_reason="csv_seed",
            )
            for pick in legacy
            if pick.ticker not in load_disable_list(self.watchdog_disable_file)
        )
        update = ScreenerUpdate(
            generation_id=0,
            generated_at_ms=int(time.time() * 1000),
            reason="csv_seed",
            picks=picks,
            added=tuple(pick.market_id for pick in picks),
            kept=(),
            changed=(),
            removed=(),
        )
        self.screener.accept_snapshot(update)
        if self.shutdown_requested.is_set():
            return
        await self.manager.apply_update(update)
        self._save_screener_snapshot()

    async def _seed_fixed_ticker(self, ticker: str) -> None:
        pick = ScreenerPick(
            market_id=ticker,
            title=ticker,
            yes_budget_cents=int(self.arguments.yes_budget_cents),
            no_budget_cents=int(self.arguments.no_budget_cents),
            ranking={"Ticker": ticker, "SearchText": ticker},
            selection_reason="fixed_session_ticker",
        )
        update = ScreenerUpdate(
            generation_id=0,
            generated_at_ms=int(time.time() * 1000),
            reason="fixed_session_ticker",
            picks=(pick,), added=(ticker,), kept=(), changed=(), removed=(),
        )
        self.screener.accept_snapshot(update)
        await self.manager.apply_update(update)
        self._save_screener_snapshot()

    async def refresh(self, reason: str) -> bool:
        refresh_task = asyncio.create_task(
            self.screener.refresh(self.manager.current_picks, reason=reason)
        )
        while not refresh_task.done():
            self.publish_status("stopping" if self.shutdown_requested.is_set() else "running")
            try:
                await asyncio.wait_for(asyncio.shield(refresh_task), timeout=0.5)
            except asyncio.TimeoutError:
                pass
        update = await refresh_task
        event = await self.screener.events.get()
        if self.shutdown_requested.is_set():
            return False
        if update is None:
            self.last_error = event.error or "screener refresh failed"
            self.next_refresh_at = (
                time.time() + self.arguments.refresh_interval_seconds
                if self.arguments.refresh_interval_seconds > 0
                else None
            )
            return False
        await self.manager.apply_update(update)
        self._save_screener_snapshot()
        self.last_error = None
        self.next_refresh_at = (
            time.time() + self.arguments.refresh_interval_seconds
            if self.arguments.refresh_interval_seconds > 0
            else None
        )
        return True

    async def _handle_control(self, request: ControlRequest) -> None:
        self.pending_action = {
            "requestId": request.request_id,
            "action": request.action,
            "ticker": request.ticker,
            "receivedAt": request.received_at_ms,
        }
        try:
            known = set(self.manager.current_picks) | set(load_disable_list(self.watchdog_disable_file))
            try:
                known.update(
                    pick.ticker
                    for pick in load_screen_picks(
                        screen_file_path=self.screen_path,
                        ticker_column=self.arguments.ticker_column,
                        title_column=self.arguments.title_column,
                        yes_budget_cents=self.arguments.yes_budget_cents,
                        no_budget_cents=self.arguments.no_budget_cents,
                        yes_budget_column=self.arguments.yes_budget_column,
                        no_budget_column=self.arguments.no_budget_column,
                        max_bots=0,
                    )
                )
            except Exception:
                pass
            if request.ticker and request.ticker not in known:
                raise ValueError(f"unknown ticker: {request.ticker}")
            if request.action == "disable_ticker":
                assert request.ticker
                disable_ticker(self.watchdog_disable_file, request.ticker, "operator_ui")
                await self.manager.stop_bot(request.ticker, reason="operator_disabled")
                result = {"ticker": request.ticker, "disabled": True, "inventoryRetained": True}
            elif request.action == "enable_ticker":
                assert request.ticker
                removed = clear_disabled_ticker(self.watchdog_disable_file, request.ticker)
                await self.refresh("operator_enable")
                result = {"ticker": request.ticker, "disabled": False, "wasDisabled": removed, "refreshed": True}
            elif request.action == "refresh":
                refreshed = await self.refresh("operator_refresh")
                result = {"refreshed": refreshed, "activeBots": len(self.manager.bots)}
            else:
                raise ValueError(f"unsupported action: {request.action}")
            request.result = {"ok": True, "result": result}
        except Exception as exc:
            self.last_error = str(exc)
            request.result = {"ok": False, "code": "control_failed", "message": str(exc)}
        finally:
            request.done.set()
            self.pending_action = None

    async def run(self) -> int:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.publish_status("starting")
        control_server = ControlServer(self.socket_path, self.control_requests, self._status_provider)
        control_server.start()
        loop = asyncio.get_running_loop()
        installed_signals = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                def request_shutdown() -> None:
                    self.manager.begin_shutdown()
                    self.shutdown_requested.set()

                loop.add_signal_handler(sig, request_shutdown)
                installed_signals.append(sig)
            except NotImplementedError:
                pass
        try:
            self._portfolio_task = asyncio.create_task(self.portfolio.refresh())
            fixed_ticker = str(getattr(self.arguments, "fixed_ticker", "") or "").strip()
            if fixed_ticker:
                await self._seed_fixed_ticker(fixed_ticker)
                self.next_refresh_at = None
            elif self.arguments.run_screener_on_start:
                await self.refresh("startup")
            else:
                await self._seed_from_csv()
                self.next_refresh_at = (
                    time.time() + self.arguments.refresh_interval_seconds
                    if self.arguments.refresh_interval_seconds > 0
                    else None
                )
            if self.shutdown_requested.is_set():
                return 0
            if self.arguments.dry_run:
                await self._portfolio_task
                self.publish_status("running")
                return 0
            self.publish_status("running")
            self._append_run_log("launcher running")
            if self.session_store is not None and self.run_id:
                try:
                    self.session_store.mark_running(self.run_id)
                except (sqlite3.Error, OSError) as exc:
                    self._record_observability_failure("mark run running", exc)
            while not self.shutdown_requested.is_set():
                while True:
                    try:
                        request = self.control_requests.get_nowait()
                    except queue.Empty:
                        break
                    await self._handle_control(request)
                await self.manager.monitor_once()
                if self._portfolio_task is not None and self._portfolio_task.done():
                    try:
                        self._portfolio_task.result()
                    except Exception as exc:
                        self.last_error = str(exc)
                    self._portfolio_task = None
                if self._portfolio_task is None and self.portfolio.due():
                    self._portfolio_task = asyncio.create_task(self.portfolio.refresh())
                if self.next_refresh_at is not None and time.time() >= self.next_refresh_at:
                    await self.refresh("scheduled")
                while not self.manager.events.empty():
                    await self.manager.events.get()
                self.publish_status()
                try:
                    await asyncio.wait_for(
                        self.shutdown_requested.wait(),
                        timeout=min(float(self.arguments.poll_seconds), 2.0),
                    )
                except asyncio.TimeoutError:
                    pass
        except Exception as exc:
            self._run_error = str(exc)
            self._append_run_log(f"launcher failed: {exc}")
            raise
        finally:
            self.publish_status("stopping")
            while True:
                try:
                    pending = self.control_requests.get_nowait()
                except queue.Empty:
                    break
                pending.result = {
                    "ok": False,
                    "code": "launcher_stopping",
                    "message": "launcher stopped before the command completed",
                }
                pending.done.set()
            shutdown_error: Optional[Exception] = None
            try:
                await self.manager.stop_all()
            except Exception as exc:
                shutdown_error = exc
                self.last_error = str(exc)
            finally:
                if self._portfolio_task is not None:
                    try:
                        await self._portfolio_task
                    except Exception:
                        pass
                    self._portfolio_task = None
                if shutdown_error is None:
                    # Replace the pre-stop portfolio cache so the final status
                    # cannot keep showing orders that cleanup just removed.
                    await self.portfolio.refresh()
                await self.client.close()
                await self.portfolio_client.close()
                control_server.stop()
                self.publish_status("shutdown_failed" if shutdown_error else "stopped")
                if self.session_store is not None and self.run_id:
                    final_metrics = self._metrics.observe(self.latest_status) if self._metrics is not None else None
                    final_status = "shutdown_failed" if shutdown_error else "failed" if self._run_error else "stopped"
                    try:
                        self.session_store.finish_run(self.run_id, final_status, metrics=final_metrics, error=str(shutdown_error or self._run_error or "") or None)
                        self._append_run_log(f"launcher finalized status={final_status}")
                    except (sqlite3.Error, OSError) as exc:
                        self._record_observability_failure("final session metrics", exc)
                for sig in installed_signals:
                    loop.remove_signal_handler(sig)
            if shutdown_error is not None:
                raise shutdown_error
        return 0
