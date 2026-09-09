"""Async fleet coordinator that connects Screener, BotManager, and control events."""

from __future__ import annotations

import asyncio
import argparse
import logging
import os
import queue
import signal
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Optional

from core.bot_manager import BotManagerConfig
from clients.factory import build_client_config
from clients.monitoring import SessionActivityAccumulator, merge_activity_snapshots
from clients.models import AccountPositionQuery
from fleet_runtime.manager import ShardedBotManager
from fleet_runtime.multi_manager import MultiVenueBotManager
from core.fleet_models import ScreenerPick, ScreenerUpdate
from screeners.kalshi_screener import build_settings_from_configuration as build_screener_settings
from core.market_classes import HistorySeriesStatsSource, build_market_class_resolver
from apps.lip_launcher import (
    atomic_write_json,
    clear_disabled_ticker,
    disable_ticker,
    load_disable_list,
    load_screen_picks,
)
from core.runtime_control import ControlRequest, ControlServer, STATUS_SCHEMA_VERSION
from portfolio.portfolio_monitor import PortfolioMonitorConfig
from portfolio.multi_portfolio import aggregate_portfolio_status
from venue_runtime import build_runtime
from core.session_config import default_session_configuration, validate_session_configuration, enabled_venues, effective_screener_configuration
from polymarket_mirror import MirrorConfig, PolymarketMirrorProcess


LOGGER = logging.getLogger(__name__)


def screener_settings_for_session(session_configuration: Dict[str, object], max_bots: int, venue: str = "kalshi") -> Dict[str, object]:
    """Screener settings the fleet runs with: the session's ``screener`` section plus the ``maxBots`` floor.

    The direct-CLI path passes ``default_session_configuration()`` (its
    ``screener`` defaults are the ``kalshi_screener_config`` constants), so it
    screens exactly as before sessions carried the section. ``top_n`` keeps
    its historical semantics: the export must hold at least ``maxBots`` rows.
    """
    settings = build_screener_settings(session_configuration, venue)
    if int(max_bots) > 0:
        settings["top_n"] = max(int(settings["top_n"]), int(max_bots))
    return settings


class Launcher:
    def __init__(self, arguments, *, api_key_id: Optional[str], private_key_path: Optional[str], session_store=None, session_run=None) -> None:
        self.arguments = arguments
        self.session_store = session_store
        self.session_run = session_run
        if session_run:
            self.session_configuration = validate_session_configuration(session_run["configuration"])
        else:
            direct_configuration = default_session_configuration()
            direct_configuration["execution"].update(
                useDemo=bool(arguments.use_demo), dryRun=bool(arguments.dry_run),
                subaccount=int(arguments.subaccount or 0),
            )
            requested = [item.strip().lower() for item in str(getattr(arguments, "venues", "") or "").split(",") if item.strip()]
            if not requested:
                requested = [str(getattr(arguments, "venue", "kalshi") or "kalshi").lower()]
            if any(item not in {"kalshi", "polymarket"} for item in requested):
                raise ValueError("unsupported venue in --venues")
            direct_configuration["venue"] = requested[0]
            for name in ("kalshi", "polymarket"):
                direct_configuration["venues"][name]["enabled"] = name in requested
                direct_configuration["venues"][name]["maxBots"] = int(arguments.max_bots)
            direct_configuration["launcher"].update(
                fixedTicker=str(getattr(arguments, "fixed_ticker", "") or ""),
                maxBots=int(arguments.max_bots),
                yesBudgetCents=int(arguments.yes_budget_cents),
                noBudgetCents=int(arguments.no_budget_cents),
                launchDelaySeconds=float(arguments.launch_delay_seconds),
                runScreenerOnStart=bool(arguments.run_screener_on_start),
                refreshIntervalSeconds=float(arguments.refresh_interval_seconds),
                pollSeconds=float(arguments.poll_seconds),
                minimumCarryoverValueCents=float(arguments.minimum_carryover_value_cents),
            )
            self.session_configuration = validate_session_configuration(direct_configuration)
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

        self.enabled_venues = enabled_venues(self.session_configuration)
        self.venue = self.enabled_venues[0]
        fleet_runtime = self.session_configuration.get("fleetRuntime") or {}
        runtimes = {}
        manager_configs = {}
        portfolio_map = {}
        client_configs = {}
        cleanup_by_venue = {}
        for venue in self.enabled_venues:
            if venue == "polymarket":
                private_path = os.getenv("POLYMARKET_PRIVATE_KEY_PATH", "").strip() or private_key_path or ""
                private_value = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
                values = {
                    "private_key": private_value, "private_key_path": private_path,
                    "gamma_base_url": os.getenv("POLYMARKET_GAMMA_URL", "https://gamma-api.polymarket.com").strip(),
                    "clob_base_url": os.getenv("POLYMARKET_CLOB_URL", "https://clob.polymarket.com").strip(),
                    "data_base_url": os.getenv("POLYMARKET_DATA_URL", "https://data-api.polymarket.com").strip(),
                    "websocket_url": os.getenv("POLYMARKET_MARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market").strip(),
                    "user_websocket_url": os.getenv("POLYMARKET_USER_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/user").strip(),
                    "signature_type": int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0") or 0),
                    "api_key": api_key_id or os.getenv("POLYMARKET_API_KEY", "").strip() or os.getenv("POLYMARKET_API_KEY_ID", "").strip(),
                    "api_secret": os.getenv("POLYMARKET_API_SECRET", "").strip(),
                    "api_passphrase": os.getenv("POLYMARKET_API_PASSPHRASE", "").strip(),
                    "proxy_url": str(getattr(arguments, "polymarket_proxy_url", "") or "").strip() or os.getenv("POLYMARKET_PROXY_URL", "").strip(),
                    "funder_address": str(getattr(arguments, "polymarket_funder_address", "") or "").strip() or os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip(),
                    "catalog_path": str(self.runtime_dir / "polymarket_catalog.sqlite3"),
                    "book_cache_path": str(self.runtime_dir / "polymarket_books.sqlite3"),
                    "scan_mode": os.getenv("POLYMARKET_SCAN_MODE", "catalog_plus_cached_books").strip(),
                    "mirror_db_path": str(self.runtime_dir / "polymarket_mirror.sqlite3"),
                    "mirror_status_path": str(self.runtime_dir / "polymarket_mirror_status.json"),
                    "rate_limit_profile": os.getenv("POLYMARKET_RATE_LIMIT_PROFILE", "standard").strip(),
                    "rest_timeout_seconds": float(os.getenv("POLYMARKET_REST_TIMEOUT_SECONDS", "15") or 15),
                    "rest_connect_timeout_seconds": float(os.getenv("POLYMARKET_REST_CONNECT_TIMEOUT_SECONDS", "10") or 10),
                    "dry_run": bool(arguments.dry_run), "public_only": not bool(private_value or private_path),
                }
                values.update({key: value for key, value in (self.session_configuration["venues"][venue].get("client") or {}).items() if key not in {"private_key", "private_key_path", "api_secret", "api_passphrase"}})
                cleanup_by_venue[venue] = bool(private_value or private_path)
                client_config = build_client_config(venue, values)
            else:
                client_config = build_client_config(venue, api_key_id=api_key_id or "", private_key_path=private_key_path or "", public_only=not bool(api_key_id and private_key_path), use_demo_environment=bool(arguments.use_demo), dry_run=bool(arguments.dry_run), subaccount_number=int(arguments.subaccount or 0))
                cleanup_by_venue[venue] = bool(api_key_id and private_key_path)
            client_configs[venue] = client_config
            max_bots = int(self.session_configuration["venues"][venue]["maxBots"])
            screener_settings = screener_settings_for_session(self.session_configuration, max_bots, venue)
            output = self.screen_path.parent / "screener" / venue / self.screen_path.name
            runtime = build_runtime(venue, client_config=client_config, screener_settings=screener_settings, screener_kwargs={
                "output_path": output, "default_yes_budget_cents": arguments.yes_budget_cents, "default_no_budget_cents": arguments.no_budget_cents,
                "max_bots": max_bots, "minimum_carryover_value_cents": arguments.minimum_carryover_value_cents,
                "cash_reserve_fraction": float(fleet_runtime.get("cashReserveFraction", 0.0)), "allocation_oversubscription": float(fleet_runtime.get("allocationOversubscription", 1.0)),
                "yes_budget_column": arguments.yes_budget_column, "no_budget_column": arguments.no_budget_column,
                "disabled_market_ids": lambda: set(load_disable_list(self.watchdog_disable_file)),
                "market_class_resolver": build_market_class_resolver(self.session_configuration, series_stats=HistorySeriesStatsSource(Path(__file__).resolve().parent.parent / "history_data" / "history.sqlite3")),
            }, portfolio_config=PortfolioMonitorConfig(subaccount_number=int(arguments.subaccount or 0), analytics_path=self.runtime_dir / f"portfolio_{venue}.sqlite3"))
            runtimes[venue] = runtime
            portfolio_map[venue] = runtime.portfolio
            manager_configs[venue] = BotManagerConfig(
                bot_script_path=self.bot_script_path,
                logs_directory=self.logs_directory / venue,
                runtime_dir=self.runtime_dir / venue,
                watchdog_state_dir=self.watchdog_state_dir / venue,
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
                venue=venue,
                client_config=client_config,
                bot_artifacts_root=(self.run_artifact_path / venue) if self.run_artifact_path else None,
                venue_max_bots=int(self.session_configuration["venues"][venue]["maxBots"]),
            )
        self.runtimes = runtimes
        self.client_configs = client_configs
        self.clients = {name: runtime.client for name, runtime in runtimes.items()}
        self.screeners = {name: runtime.screener for name, runtime in runtimes.items()}
        self.portfolios = portfolio_map
        self.managers = {
            name: ShardedBotManager(manager_configs[name], cleanup_client=self.clients[name] if cleanup_by_venue.get(name) else None)
            for name in manager_configs
        }
        self.mirror_processes = {}
        for name, client_config in self.client_configs.items():
            if name != "polymarket" or not bool(getattr(client_config, "mirror_enabled", False)):
                continue
            mirror_config = MirrorConfig(
                enabled=True,
                required_complete_snapshot=bool(getattr(client_config, "mirror_required_complete_snapshot", False)),
                snapshot_max_age_seconds=float(getattr(client_config, "mirror_snapshot_max_age_seconds", 60.0)),
                book_stale_after_seconds=float(getattr(client_config, "mirror_book_stale_after_seconds", 60.0)),
                ws_shards=int(getattr(client_config, "mirror_ws_shards", 1)),
                book_recovery_parallelism=int(getattr(client_config, "mirror_book_recovery_parallelism", 16)),
                catalog_page_size=int(getattr(client_config, "mirror_catalog_page_size", 100)),
                sync_interval_seconds=float(getattr(client_config, "mirror_sync_interval_seconds", 30.0)),
                max_markets=int(getattr(client_config, "mirror_max_markets", 0)),
            )
            self.mirror_processes[name] = PolymarketMirrorProcess(
                client_config,
                getattr(client_config, "mirror_db_path", str(self.runtime_dir / "polymarket_mirror.sqlite3")),
                getattr(client_config, "mirror_status_path", str(self.runtime_dir / "polymarket_mirror_status.json")),
                mirror_config,
            )
        self.manager = MultiVenueBotManager(
            self.managers,
            global_max_bots=int(self.session_configuration["launcher"]["maxBots"]),
            venue_configs=self.session_configuration["venues"],
        )
        # Compatibility aliases for single-venue integrations.
        self.client_config = self.client_configs[self.venue]
        self.client = self.clients[self.venue]
        self.portfolio_client = self.client
        self.screener = self.screeners[self.venue]
        self.portfolio = self.portfolios[self.venue]
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
        self._pending_updates: Dict[str, ScreenerUpdate] = {}
        self._active_screener_record_ids: Dict[str, str] = {}
        self._run_error: Optional[str] = None
        self._last_metric_sample_ms = 0
        self._observability_warning: Optional[str] = None
        self._last_observability_warning_log_ms = 0
        self._last_mirror_generation_seen: Optional[str] = None
        self._last_mirror_refresh_ms = 0
        from core.session_store import RunMetricsAccumulator
        self._metrics = RunMetricsAccumulator(self.started_at_ms)
        self._activity = SessionActivityAccumulator()
        self._append_run_log("launcher initialized")

    def _append_run_log(self, message: str) -> None:
        if getattr(self, "run_artifact_path", None) is None:
            return
        self.run_artifact_path.mkdir(parents=True, exist_ok=True)
        with (self.run_artifact_path / "launcher.log").open("a", encoding="utf-8") as handle:
            handle.write(f"{int(time.time() * 1000)} {message}\n")

    def _start_mirrors(self) -> None:
        for venue, process in self.mirror_processes.items():
            try:
                process.start()
                LOGGER.info("started %s mirror process pid=%s", venue, process.pid)
            except Exception as exc:
                self.last_error = f"{venue} mirror failed to start: {exc}"
                LOGGER.exception("POLYMARKET_MIRROR_START_ERROR")

    def _stop_mirrors(self) -> None:
        for venue, process in self.mirror_processes.items():
            try:
                process.stop()
            except Exception:
                LOGGER.exception("POLYMARKET_MIRROR_STOP_ERROR venue=%s", venue)

    def _ready_mirror_generation(self, venue: str = "polymarket") -> Optional[str]:
        """Return a newly published mirror generation, if one is ready."""
        process = self.mirror_processes.get(venue)
        if process is None:
            return None
        try:
            status = process.status()
        except Exception:
            return None
        if not bool(status.get("screenable") or status.get("complete")):
            return None
        generation = str(status.get("generationId") or "").strip()
        if not generation:
            return None
        # ``capturedAtMs`` is a heartbeat and changes even when no book did.
        # Use the book revision so a refresh is requested only after the
        # mirror actually publishes new book data. The generation is retained
        # so a newly cataloged partial generation is screened immediately even
        # when it reuses already cached books.
        revision = int(status.get("bookRevision") or 0)
        return f"{generation}:{revision}"

    # Manager events that mean the fleet degraded or recovered.  They are
    # logged at WARNING and appended to the run log so an operator reading
    # launcher_console.log can reconstruct a stall/restart history; every
    # other event is logged at INFO.
    MANAGER_EVENT_WARNINGS = frozenset({
        "broker_restarted", "worker_recovery_failed", "runtime_capacity_fail_closed",
        "capacity_fail_closed", "capacity_partial", "shutdown_cleanup_failed", "update_skipped_shutdown",
    })
    MANAGER_EVENT_RUN_LOG = MANAGER_EVENT_WARNINGS | frozenset({
        "admission_recovered", "worker_restarted", "fleet_shutdown_cleanup_verified",
    })

    @staticmethod
    def format_manager_event(event) -> str:
        detail = getattr(event, "detail", None) or {}
        parts = [f"MANAGER_EVENT | type={getattr(event, 'event_type', '?')}"]
        market_id = getattr(event, "market_id", "")
        if market_id and market_id != "*":
            parts.append(f"market={market_id}")
        for key in sorted(detail):
            value = detail[key]
            if isinstance(value, (list, tuple)):
                value = ",".join(str(item) for item in value) or "-"
            parts.append(f"{key}={value}")
        return " ".join(parts)

    def _log_manager_event(self, event) -> None:
        line = self.format_manager_event(event)
        event_type = str(getattr(event, "event_type", "") or "")
        LOGGER.log(logging.WARNING if event_type in self.MANAGER_EVENT_WARNINGS else logging.INFO, line)
        if event_type in self.MANAGER_EVENT_RUN_LOG:
            try:
                self._append_run_log(line)
            except OSError as exc:
                self._record_observability_failure("append manager event", exc)

    def _drain_manager_events(self) -> None:
        """Log every queued manager event instead of discarding it."""
        queues = [getattr(self.manager, "events", None)]
        queues.extend(getattr(manager, "events", None) for manager in getattr(self, "managers", {}).values())
        for events in queues:
            if events is None:
                continue
            while not events.empty():
                try:
                    event = events.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._log_manager_event(event)

    def _save_screener_snapshot(self) -> None:
        if self.run_artifact_path is None:
            return
        target = self.run_artifact_path / "screener"
        target.mkdir(parents=True, exist_ok=True)
        snapshot = self.screener.status_snapshot()
        generated = int(snapshot.get("generatedAtMs") or time.time() * 1000)
        atomic_write_json(target / "latest.json", snapshot)
        atomic_write_json(target / f"{generated}.json", snapshot)
        for venue, screener in self.screeners.items():
            venue_target = target / venue
            venue_target.mkdir(parents=True, exist_ok=True)
            venue_snapshot = screener.status_snapshot()
            venue_generated = int(venue_snapshot.get("generatedAtMs") or time.time() * 1000)
            atomic_write_json(venue_target / "latest.json", venue_snapshot)
            atomic_write_json(venue_target / f"{venue_generated}.json", venue_snapshot)

    def status_snapshot(self, lifecycle: str = "running") -> Dict[str, object]:
        manager_status = self.manager.status_snapshot()
        portfolio_statuses = {venue: portfolio.status_snapshot() for venue, portfolio in self.portfolios.items()}
        now_ms = int(time.time() * 1000)
        disabled = load_disable_list(self.watchdog_disable_file)
        counts = dict(manager_status["counts"])
        counts["disabledTickers"] = len(disabled)
        manager_monitoring = dict(manager_status.get("monitoring") or {})
        venue_rows = []
        venue_activity = []
        for venue in self.enabled_venues:
            venue_manager = (manager_status.get("venues") or {}).get(venue, {})
            launcher_activity = self.clients[venue].activity_snapshot()
            sources = [(
                f"launcher:{int(launcher_activity.get('startedAtMs') or self.started_at_ms)}",
                launcher_activity,
            )]
            mirror_status = (launcher_activity.get("mirror") or {}) if isinstance(launcher_activity, dict) else {}
            mirror_activity = mirror_status.get("apiActivity") if isinstance(mirror_status, dict) else None
            if mirror_activity and venue in self.mirror_processes:
                sources.append((
                    f"mirror:{venue}",
                    mirror_activity,
                ))
            broker = venue_manager.get("broker") or {}
            broker_activity = broker.get("apiActivity") or {}
            if broker_activity and broker.get("running"):
                sources.append((
                    f"broker:{broker.get('pid')}:{int(broker_activity.get('startedAtMs') or 0)}",
                    broker_activity,
                ))
            for worker in venue_manager.get("workers") or []:
                activity = worker.get("apiActivity") or {}
                if activity and worker.get("running") and not worker.get("stale"):
                    sources.append((
                        f"worker:{worker.get('workerId')}:{worker.get('pid')}:{int(activity.get('startedAtMs') or 0)}",
                        activity,
                    ))
            activity = self._activity.observe(venue, sources)
            venue_activity.append(activity)
            venue_rows.append({
                "venue": venue,
                "active": lifecycle in {"starting", "running"},
                "botsRunning": int((venue_manager.get("counts") or {}).get("activeBots") or 0),
                "configuredBots": int((venue_manager.get("counts") or {}).get("configuredBots") or 0),
                "apiActivity": activity,
            })
        manager_monitoring["activeVenues"] = [item["venue"] for item in venue_rows if item["active"]]
        manager_monitoring["apiActivity"] = merge_activity_snapshots(venue_activity)
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
            "workers": manager_status.get("workers", []),
            "broker": manager_status.get("broker", {}),
            "capacity": manager_status.get("capacity"),
            "allocation": manager_status.get("allocation"),
            "manager": manager_monitoring,
            "clients": manager_status.get("clients", []),
            "venueMonitoring": venue_rows,
            "mirrors": {
                venue: {
                    "pid": process.pid,
                    "running": process.is_alive(),
                    "status": process.status(),
                }
                for venue, process in self.mirror_processes.items()
            },
            "screener": self.screeners[self.enabled_venues[0]].status_snapshot(),
            "portfolio": self.portfolios[self.enabled_venues[0]].status_snapshot(),
            "portfolioAggregate": aggregate_portfolio_status(portfolio_statuses, list(self.enabled_venues)),
            "venues": {
                venue: {
                    "screener": screener.status_snapshot(),
                    "portfolio": portfolio_statuses[venue],
                    "manager": manager_status.get("venues", {}).get(venue, {}),
                    "capacity": manager_status.get("capacity", {}).get(venue),
                }
                for venue, screener in self.screeners.items()
            },
            "disabledSummary": {"count": len(disabled)},
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
        metrics = self._metrics.observe(status)
        manager = status.get("manager") if isinstance(status.get("manager"), dict) else {}
        manager["pnl"] = {
            "fills": int(metrics.get("fills") or 0),
            "feesCents": float(metrics.get("feesCents") or 0),
            "realizedCents": float(metrics.get("realizedCents") or 0),
            "unrealizedCents": float(metrics.get("unrealizedCents") or 0),
            "totalCents": float(metrics.get("totalCents") or 0),
            "complete": bool(metrics.get("pnlComplete", False)),
        }
        status["manager"] = manager
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
        if self.session_store is not None and self.run_id:
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

    async def _startup_exchange_positions(self, venue: Optional[str] = None) -> Optional[list]:
        """The account's open exchange positions for the restart carryover.

        Read once at fleet start through the launcher's own client (the same
        positions call PortfolioMonitor uses) so every position left by an
        earlier run gets a bot even when the screener does not re-pick its
        market.  ``None`` when unavailable (no credentials, venue error): the
        start proceeds without carryover and the reason is surfaced.
        """
        selected_venue = str(venue or getattr(self, "venue", "kalshi")).lower()
        clients = getattr(self, "clients", {})
        client_configs = getattr(self, "client_configs", {})
        client = clients.get(selected_venue, getattr(self, "client", None))
        client_config = client_configs.get(selected_venue, getattr(self, "client_config", None))
        configured_auth = bool(self.api_key_id and self.private_key_path)
        if selected_venue == "polymarket":
            configured_auth = bool(
                getattr(client_config, "private_key", "")
                or getattr(client_config, "private_key_path", "")
            ) and not bool(getattr(client_config, "public_only", False))
        if not configured_auth:
            return None
        try:
            positions = await asyncio.to_thread(
                client.list_account_positions, AccountPositionQuery(nonzero_only=True),
            )
        except Exception as exc:
            message = f"exchange positions unavailable at start; restart carryover skipped: {exc}"
            LOGGER.warning("STARTUP_EXCHANGE_POSITIONS | %s", message)
            self.last_error = message
            return None
        return [item for item in positions if int(getattr(item, "position_units", 0) or 0) != 0]

    # Run artifacts scanned for fleet fills at start (newest first).
    TRADED_TICKERS_RUN_LIMIT = 40

    def _fleet_traded_tickers_sync(self) -> set:
        """Tickers the fleet's own telemetry has fills for, from the run artifacts.

        Every run under this session's artifact root (``<artifacts>/<session>/
        <run>``: the current run's siblings, newest first, at most
        ``TRADED_TICKERS_RUN_LIMIT``) is scanned for its per-shard telemetry
        databases and their ``fills`` tickers.  Used by the restart carryover
        as proof that a position outside the screener's horizon is the
        fleet's after all.  Read-only, best effort: an unreadable database is
        skipped.  Empty when the launcher runs without a session run.
        """
        import sqlite3
        from contextlib import closing

        from portfolio.pnl_core import find_telemetry_databases

        artifact = getattr(self, "run_artifact_path", None)
        if artifact is None:
            return set()
        session_root = Path(artifact).parent
        try:
            runs = [path for path in session_root.iterdir() if path.is_dir()]
        except OSError:
            return set()
        runs.sort(key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)
        tickers: set = set()
        for run in runs[: self.TRADED_TICKERS_RUN_LIMIT]:
            for database in find_telemetry_databases(run):
                try:
                    uri = f"file:{database.as_posix()}?mode=ro"
                    with closing(sqlite3.connect(uri, uri=True, timeout=0.25)) as db:
                        tables = {str(row[0]) for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                        if "fills" not in tables:
                            continue
                        tickers.update(
                            str(row[0]) for row in db.execute("SELECT DISTINCT ticker FROM fills") if row[0]
                        )
                except Exception as exc:
                    LOGGER.debug("telemetry fills unreadable for carryover: %s: %s", database, exc)
        return tickers

    async def _fleet_traded_tickers(self) -> set:
        try:
            return await asyncio.to_thread(self._fleet_traded_tickers_sync)
        except Exception as exc:
            LOGGER.warning("STARTUP_EXCHANGE_POSITIONS | fleet fill history unavailable: %s", exc)
            return set()

    async def _with_startup_carryover(
        self, picks: tuple[ScreenerPick, ...]
    ) -> tuple[tuple[ScreenerPick, ...], tuple[str, ...], list[str]]:
        """Direct seed paths: append reduce-only picks for open exchange positions."""
        positions = await self._startup_exchange_positions()
        if not positions:
            return picks, (), []
        traded = await self._fleet_traded_tickers()
        selected, carried, warnings = await asyncio.to_thread(
            self.screener.carry_exchange_positions, list(picks), positions, traded_tickers=traded,
        )
        return tuple(selected), tuple(sorted(carried)), warnings

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
                venue=self.venue,
            )
            for pick in legacy
            if pick.ticker not in load_disable_list(self.watchdog_disable_file)
        )
        picks, carried, warnings = await self._with_startup_carryover(picks)
        update = ScreenerUpdate(
            generation_id=0,
            generated_at_ms=int(time.time() * 1000),
            reason="csv_seed",
            picks=picks,
            added=tuple(pick.market_id for pick in picks),
            kept=(),
            changed=(),
            removed=(),
            inventory_carried=carried,
        )
        self.screener.accept_snapshot(update, warnings=warnings)
        if self.shutdown_requested.is_set():
            return
        await self._apply_manager_updates({self.venue: update})
        self._save_screener_snapshot()

    async def _seed_fixed_ticker(self, ticker: str, venue: str | None = None) -> None:
        if venue:
            old = (self.venue, self.client, self.client_config, self.screener, self.portfolio, self.manager)
            self.venue, self.client, self.client_config = venue, self.clients[venue], self.client_configs[venue]
            self.screener, self.portfolio, self.manager = self.screeners[venue], self.portfolios[venue], self.managers[venue]
            try:
                return await self._seed_fixed_ticker(ticker)
            finally:
                self.venue, self.client, self.client_config, self.screener, self.portfolio, self.manager = old
        pick = ScreenerPick(
            market_id=ticker,
            title=ticker,
            yes_budget_cents=int(self.arguments.yes_budget_cents),
            no_budget_cents=int(self.arguments.no_budget_cents),
            ranking={"Ticker": ticker, "SearchText": ticker},
            selection_reason="fixed_session_ticker",
            venue=self.venue,
        )
        picks, carried, warnings = await self._with_startup_carryover((pick,))
        update = ScreenerUpdate(
            generation_id=0,
            generated_at_ms=int(time.time() * 1000),
            reason="fixed_session_ticker",
            picks=picks, added=tuple(item.market_id for item in picks), kept=(), changed=(), removed=(),
            inventory_carried=carried,
        )
        self.screener.accept_snapshot(update, warnings=warnings)
        await self._apply_manager_updates({self.venue: update})
        self._save_screener_snapshot()

    async def _apply_manager_updates(self, updates: dict[str, ScreenerUpdate]) -> object:
        """Apply a screener update while keeping worker liveness serviced.

        ``ShardedBotManager.apply_update`` starts/reconciles workers before it
        performs the broker admission call.  That call can legitimately take
        longer than the worker-stale window.  Waiting for it without polling
        the manager leaves heartbeat/control queues undrained, so healthy
        workers look stale and are recycled while their actors are starting.
        Keep the manager's monitor loop running until the update is complete.
        """
        apply_updates = getattr(self.manager, "apply_updates", None)
        if callable(apply_updates):
            update_task = asyncio.create_task(apply_updates(updates))
        else:
            if len(updates) != 1:
                raise RuntimeError("single-venue manager cannot apply multiple venue updates")
            update = next(iter(updates.values()))
            update_task = asyncio.create_task(self.manager.apply_update(update))
        try:
            while not update_task.done():
                if self.shutdown_requested.is_set():
                    update_task.cancel()
                    await asyncio.gather(update_task, return_exceptions=True)
                    raise asyncio.CancelledError
                try:
                    await asyncio.wait_for(asyncio.shield(update_task), timeout=0.25)
                except asyncio.TimeoutError:
                    await self.manager.monitor_once()
                    self._drain_manager_events()
                    self.publish_status("stopping" if self.shutdown_requested.is_set() else "running")
            result = await update_task
            await self.manager.monitor_once()
            self._drain_manager_events()
            return result
        except asyncio.CancelledError:
            if not update_task.done():
                update_task.cancel()
                await asyncio.gather(update_task, return_exceptions=True)
            raise

    async def _refresh_one(self, reason: str, venue: str | None = None, apply_update: bool = True) -> bool:
        selected_venue = str(venue or self.venue).lower()
        if selected_venue not in self.screeners:
            raise ValueError(f"unknown venue: {selected_venue}")
        # Do not switch the launcher's compatibility aliases here.  A refresh
        # can run for every enabled venue at once, and those aliases are shared
        # mutable state.  Keeping all objects local makes a slow Polymarket
        # request independent from Kalshi's screener and portfolio state.
        screener = self.screeners[selected_venue]
        manager = self.managers[selected_venue]
        # Fleet start: every open exchange position becomes a (reduce-only)
        # carryover pick even when the screener does not re-pick its market.
        exchange_positions = await self._startup_exchange_positions(selected_venue) if reason == "startup" else None
        traded_tickers = await self._fleet_traded_tickers() if exchange_positions else None
        record_id: Optional[str] = None
        if self.session_store is not None and self.run_id:
            try:
                configured_limit = screener.settings.get("max_markets_to_scan")
                record_id = self.session_store.start_screener_run(
                    self.run_id,
                    reason=reason,
                    started_at_ms=int(time.time() * 1000),
                    configured_limit=int(configured_limit) if configured_limit is not None else None,
                    artifact_path=str(self.run_artifact_path) if self.run_artifact_path else None,
                    venue=selected_venue,
                )
                self._active_screener_record_ids[selected_venue] = record_id
            except Exception as exc:
                self._record_observability_failure("start screener history", exc)
        refresh_task = asyncio.create_task(
            screener.refresh(
                manager.current_picks, reason=reason, exchange_positions=exchange_positions,
                traded_tickers=traded_tickers,
            )
        )
        try:
            while not refresh_task.done():
                # A signal can arrive while the startup/scheduled screener is
                # doing synchronous venue I/O in its worker thread. Do not
                # make shutdown wait for the full catalog scan; cancel the
                # asyncio coordination task and let its daemon worker unwind
                # independently.
                if self.shutdown_requested.is_set():
                    self.publish_status("stopping")
                    cancel_scan = getattr(screener.client, "cancel_screener_scan", None)
                    if callable(cancel_scan):
                        cancel_scan()
                    refresh_task.cancel()
                    raise asyncio.CancelledError
                self.publish_status("stopping" if self.shutdown_requested.is_set() else "running")
                # A single-venue refresh (and a mirror-triggered refresh)
                # can outlast the worker stale window.  Keep draining worker
                # heartbeats while the screener is doing synchronous venue I/O.
                # Multi-venue refreshes do this in the outer coordinator loop
                # because both venue scans are alive at the same time.
                if apply_update:
                    await self.manager.monitor_once()
                    self._drain_manager_events()
                try:
                    await asyncio.wait_for(asyncio.shield(refresh_task), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
            update = await refresh_task
            event = await screener.events.get()
            run_metrics = screener.last_run_metrics()
            if record_id and self.session_store is not None:
                try:
                    self.session_store.finish_screener_run(
                        record_id, status="succeeded" if update is not None else "failed",
                        metrics=run_metrics,
                    )
                except Exception as exc:
                    self._record_observability_failure("finish screener history", exc)
                record_id = None
                self._active_screener_record_ids.pop(selected_venue, None)
            self._save_screener_snapshot()
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
            if apply_update:
                await self._apply_manager_updates({selected_venue: update})
            else:
                self._pending_updates[selected_venue] = update
            self.last_error = None
            self.next_refresh_at = (
                time.time() + self.arguments.refresh_interval_seconds
                if self.arguments.refresh_interval_seconds > 0
                else None
            )
            return True
        except asyncio.CancelledError:
            cancel_scan = getattr(screener.client, "cancel_screener_scan", None)
            if callable(cancel_scan):
                cancel_scan()
            if not refresh_task.done():
                refresh_task.cancel()
                try:
                    await refresh_task
                except (asyncio.CancelledError, Exception):
                    pass
            if record_id and self.session_store is not None:
                try:
                    self.session_store.finish_screener_run(
                        record_id, status="interrupted",
                        metrics=screener.last_run_metrics(),
                    )
                except Exception as exc:
                    self._record_observability_failure("interrupt screener history", exc)
                self._active_screener_record_ids.pop(selected_venue, None)
            raise
        except Exception as exc:
            # A status/coordination failure must not leave a durable screener
            # record marked ``running`` forever.  The other venue can still
            # complete and launch its own bots.
            if not refresh_task.done():
                refresh_task.cancel()
                try:
                    await refresh_task
                except (asyncio.CancelledError, Exception):
                    pass
            if record_id and self.session_store is not None:
                try:
                    metrics = dict(screener.last_run_metrics() or {})
                    metrics.setdefault("startedAtMs", int(time.time() * 1000))
                    metrics.setdefault("durationMs", 0)
                    metrics["error"] = str(exc)
                    self.session_store.finish_screener_run(
                        record_id, status="failed", metrics=metrics,
                    )
                except Exception as finish_exc:
                    self._record_observability_failure("finish screener history", finish_exc)
                self._active_screener_record_ids.pop(selected_venue, None)
            LOGGER.exception("venue screener coordination failed venue=%s", selected_venue)
            self.last_error = str(exc)
            return False

    async def refresh(self, reason: str) -> bool:
        if len(self.enabled_venues) <= 1:
            return await self._refresh_one(reason)
        self._pending_updates = {}

        async def run_venue(item: str) -> tuple[str, object]:
            try:
                return item, await self._refresh_one(reason, item, False)
            except Exception as exc:
                return item, exc

        # Consume completions as they arrive.  A fast Kalshi scan can therefore
        # launch/reconcile its bots while a slow or unavailable Polymarket API
        # is still retrying.  Each partial allocation includes the other
        # venue's current picks, so the global bot ceiling remains enforced.
        tasks = {asyncio.create_task(run_venue(item)) for item in self.enabled_venues}
        updates_ok = False
        try:
            # A fast venue may launch workers while another venue's screener
            # is still scanning.  Keep each manager's heartbeat/control loop
            # running during that wait; otherwise the slow scan blocks
            # ``monitor_once`` until the startup hard cap expires and the
            # already-quoting fast-venue workers are recycled as stale.
            pending = set(tasks)
            while pending:
                if self.shutdown_requested.is_set():
                    self.publish_status("stopping")
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    return False
                completed, pending = await asyncio.wait(
                    pending, timeout=0.5, return_when=asyncio.FIRST_COMPLETED,
                )
                for task in completed:
                    item, result = task.result()
                    if isinstance(result, Exception):
                        LOGGER.warning("venue refresh failed venue=%s: %s", item, result)
                        self.last_error = str(result)
                        continue
                    updates_ok = updates_ok or bool(result)
                    update = self._pending_updates.pop(item, None)
                    if update is not None and not self.shutdown_requested.is_set():
                        await self._apply_manager_updates({item: update})
                if not self.shutdown_requested.is_set():
                    await self.manager.monitor_once()
                    self._drain_manager_events()
                    self.publish_status("running")
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            pending_tasks = [task for task in tasks if not task.done()]
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
        return updates_ok

    async def _handle_control(self, request: ControlRequest) -> None:
        self.pending_action = {
            "requestId": request.request_id,
            "action": request.action,
            "ticker": request.ticker,
            "receivedAt": request.received_at_ms,
        }
        try:
            known = {key[1] if isinstance(key, tuple) else key for key in self.manager.current_picks} | set(load_disable_list(self.watchdog_disable_file))
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
            elif request.action == "shutdown":
                # Console-independent stop: on Windows the launcher runs on its
                # own hidden console, so the dashboard's CTRL_BREAK never
                # reaches it; the control endpoint is the reliable path.
                self.manager.begin_shutdown()
                self.shutdown_requested.set()
                result = {"shutdownRequested": True}
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
        self._start_mirrors()
        loop = asyncio.get_running_loop()

        def request_shutdown() -> None:
            self.manager.begin_shutdown()
            self.shutdown_requested.set()

        installed_signals = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, request_shutdown)
                installed_signals.append(sig)
            except NotImplementedError:
                pass
        if not installed_signals:
            # Windows: asyncio has no loop signal handlers, so SIGINT/SIGBREAK
            # go through signal.signal and wake the loop from the main thread.
            def _signal_fallback(_signum: int, _frame: object) -> None:
                loop.call_soon_threadsafe(request_shutdown)

            for sig in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
                if sig is None:
                    continue
                try:
                    signal.signal(sig, _signal_fallback)
                except (ValueError, OSError):
                    pass
        try:
            self._portfolio_tasks = {venue: asyncio.create_task(portfolio.refresh()) for venue, portfolio in self.portfolios.items()}
            self._portfolio_task = self._portfolio_tasks.get(self.venue)
            fixed_ticker = str(getattr(self.arguments, "fixed_ticker", "") or "").strip()
            if fixed_ticker:
                for venue in self.enabled_venues:
                    await self._seed_fixed_ticker(fixed_ticker, venue)
                self.next_refresh_at = None
            elif self.arguments.run_screener_on_start:
                await self.refresh("startup")
                self._last_mirror_generation_seen = self._ready_mirror_generation()
            else:
                for venue in self.enabled_venues:
                    if venue == self.venue:
                        await self._seed_from_csv()
                    else:
                        old = (self.venue, self.client, self.client_config, self.screener, self.portfolio, self.manager)
                        self.venue, self.client, self.client_config = venue, self.clients[venue], self.client_configs[venue]
                        self.screener, self.portfolio, self.manager = self.screeners[venue], self.portfolios[venue], self.managers[venue]
                        try:
                            await self._seed_from_csv()
                        finally:
                            self.venue, self.client, self.client_config, self.screener, self.portfolio, self.manager = old
                self.next_refresh_at = (
                    time.time() + self.arguments.refresh_interval_seconds
                    if self.arguments.refresh_interval_seconds > 0
                    else None
                )
            if self.shutdown_requested.is_set():
                return 0
            if self.arguments.dry_run:
                await asyncio.gather(*self._portfolio_tasks.values())
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
                for venue, task in list(self._portfolio_tasks.items()):
                    if task.done():
                        try:
                            task.result()
                        except Exception as exc:
                            self.last_error = f"{venue} portfolio: {exc}"
                        self._portfolio_tasks.pop(venue, None)
                for venue, portfolio in self.portfolios.items():
                    if venue not in self._portfolio_tasks and portfolio.due():
                        self._portfolio_tasks[venue] = asyncio.create_task(portfolio.refresh())
                self._portfolio_task = self._portfolio_tasks.get(self.venue)
                # A cold mirror publishes usable pages while its catalog is
                # still syncing. Trigger throttled Polymarket screens from
                # those partial generations instead of waiting for the full
                # catalog or the normal refresh interval.
                mirror_generation = self._ready_mirror_generation()
                polymarket_screener = self.screeners.get("polymarket")
                now_ms = int(time.time() * 1000)
                if (
                    mirror_generation
                    and mirror_generation != self._last_mirror_generation_seen
                    and polymarket_screener is not None
                    and not bool(polymarket_screener.status_snapshot().get("running"))
                    and now_ms - self._last_mirror_refresh_ms >= 5_000
                ):
                    self._last_mirror_refresh_ms = now_ms
                    refreshed = await self._refresh_one("mirror_ready", "polymarket", True)
                    # Leave the revision pending after a failed read (for
                    # example a transient SQLite busy error) so the same
                    # fresh batch is retried after the normal throttle.
                    if refreshed:
                        self._last_mirror_generation_seen = mirror_generation
                if self.next_refresh_at is not None and time.time() >= self.next_refresh_at:
                    await self.refresh("scheduled")
                self._drain_manager_events()
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
                # stop_all's own events (cleanup verified / failed) would
                # otherwise never be logged: the loop above has exited.
                try:
                    self._drain_manager_events()
                except Exception as exc:
                    self._record_observability_failure("log manager events", exc)
                # Portfolio refreshes use daemon worker threads for blocking
                # venue calls. Await them only briefly during shutdown; an
                # unresponsive REST call must not hold the launcher process
                # open after the fleet has already been stopped.
                portfolio_tasks = list(getattr(self, "_portfolio_tasks", {}).values())
                for task in portfolio_tasks:
                    try:
                        await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
                    except asyncio.TimeoutError:
                        task.cancel()
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
                self._portfolio_tasks = {}
                self._portfolio_task = None
                if shutdown_error is None:
                    # Replace the pre-stop portfolio cache so the final status
                    # cannot keep showing orders that cleanup just removed.
                    # This is best-effort and bounded: shutdown must not
                    # depend on another venue round trip.
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*(portfolio.refresh() for portfolio in self.portfolios.values()), return_exceptions=True),
                            timeout=5.0,
                        )
                    except asyncio.TimeoutError:
                        pass
                async def close_client(client: object) -> None:
                    try:
                        await asyncio.wait_for(client.close(), timeout=5.0)  # type: ignore[attr-defined]
                    except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                        pass
                await asyncio.gather(*(close_client(client) for client in self.clients.values()))
                self._stop_mirrors()
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Venue-neutral multi-market launcher runtime")
    parser.add_argument("--venue", choices=("kalshi", "polymarket"), help="single-venue compatibility mode")
    parser.add_argument("--venues", help="comma-separated enabled venues")
    parser.add_argument("--max-bots", type=int, help="global bot ceiling")
    parser.parse_args()
