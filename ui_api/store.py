from __future__ import annotations

import csv
import json
import os
import sqlite3
import subprocess
import threading
import time
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from pnl_core import load_fills, summarize_pnl
from portfolio_analytics import PortfolioAnalyticsStore
from kalshi_urls import is_canonical_market_url
from runtime_control import read_json, send_control_command
from ui_api.config import Settings
from session_store import SessionConflictError, SessionStore


def now_ms() -> int:
    return int(time.time() * 1000)


def freshness(path: Path, *, stale_after_ms: int = 10_000) -> Dict[str, Any]:
    try:
        modified = int(path.stat().st_mtime * 1000)
        return {"available": True, "updatedAt": modified, "stale": now_ms() - modified > stale_after_ms}
    except OSError as exc:
        return {"available": False, "updatedAt": None, "stale": True, "error": str(exc)}


class AuditStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL UNIQUE,
                    timestamp_ms INTEGER NOT NULL, action TEXT NOT NULL, target TEXT,
                    operator TEXT NOT NULL, result TEXT NOT NULL, error TEXT
                )
            """)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=1)

    def record(self, request_id: str, action: str, target: Optional[str], operator: str, result: str, error: Optional[str]) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO audit(request_id,timestamp_ms,action,target,operator,result,error) VALUES(?,?,?,?,?,?,?)",
                (request_id, now_ms(), action, target, operator, result, error),
            )

    def list(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (min(max(limit, 1), 500),)).fetchall()
        return [dict(row) for row in rows]


class OperationsStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.audit = AuditStore(settings.runtime_dir / "ui_audit.sqlite3")
        self.sessions = SessionStore(settings.session_dir or settings.workspace / "session_data")
        self.portfolio_analytics = PortfolioAnalyticsStore(settings.runtime_dir / "portfolio_analytics.sqlite3")
        self._position_cache: Dict[str, tuple[int, Dict[str, Any]]] = {}
        self._heartbeat_lock = threading.Lock()
        self._heartbeat_cache: Optional[tuple[int, Dict[str, Any]]] = None

    def position(self, ticker: str) -> Dict[str, Any]:
        """Fetch canonical exchange inventory with a short, explicitly timestamped cache."""
        self.require_ticker(ticker)
        cached = self._position_cache.get(ticker)
        if cached and now_ms() - cached[0] < 15_000:
            return cached[1]
        api_key_id = os.getenv("KALSHI_API_KEY_ID")
        private_key = os.getenv("KALSHI_PRIVATE_KEY_PATH")
        if not api_key_id or not private_key:
            return {"available": False, "stale": True, "fetchedAt": None, "positionUnits": None, "source": "exchange", "error": "exchange credentials are not configured"}
        try:
            from lip_launcher import KalshiPositionClient
            status = self.status()["data"]
            use_demo = ((status.get("launcher") or {}).get("environment")) == "demo"
            client = KalshiPositionClient(api_key_id=api_key_id, private_key_path=private_key, use_demo=use_demo, subaccount=int(os.getenv("KALSHI_SUBACCOUNT", "0")))
            fetched = now_ms()
            result = {"available": True, "stale": False, "fetchedAt": fetched, "positionUnits": int(client.get_position_count_units(ticker)), "source": "exchange"}
            self._position_cache[ticker] = (fetched, result)
            return result
        except Exception as exc:
            if cached:
                return {**cached[1], "stale": True, "error": str(exc)}
            return {"available": False, "stale": True, "fetchedAt": None, "positionUnits": None, "source": "exchange", "error": str(exc)}

    @property
    def status_path(self) -> Path:
        return self.settings.runtime_dir / "launcher_status.json"

    @property
    def control_socket(self) -> Path:
        return self.settings.runtime_dir / "launcher.sock"

    def status(self) -> Dict[str, Any]:
        status = read_json(self.status_path)
        source = freshness(self.status_path)
        heartbeat = ((status.get("launcher") or {}).get("heartbeatAt"))
        if isinstance(heartbeat, (int, float)):
            source["stale"] = now_ms() - int(heartbeat) > 10_000
        launcher = status.get("launcher") if isinstance(status.get("launcher"), dict) else {}
        if source.get("stale") and launcher.get("lifecycle") == "stopping":
            try:
                completed = subprocess.run(
                    ["systemctl", "--user", "is-active", self.settings.service_name],
                    capture_output=True,
                    text=True,
                    timeout=3,
                    check=False,
                )
                service_state = (completed.stdout or completed.stderr or "").strip()
                source["serviceState"] = service_state
                if service_state in {"inactive", "failed"}:
                    status = dict(status)
                    status["launcher"] = {**launcher, "lifecycle": "stopped"}
                    counts = status.get("counts") if isinstance(status.get("counts"), dict) else {}
                    status["counts"] = {**counts, "activeBots": 0}
                    status["bots"] = [
                        {**item, "botRunning": False, "watchdogRunning": False}
                        for item in status.get("bots", [])
                        if isinstance(item, dict)
                    ]
                    manager = status.get("manager") if isinstance(status.get("manager"), dict) else {}
                    status["manager"] = {**manager, "running": False, "lifecycle": "stopped"}
            except (OSError, subprocess.SubprocessError):
                pass
        return {"data": status, "source": source}

    def screener(self) -> tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
        path = self.settings.workspace / "screener_export.csv"
        warnings: List[str] = []
        rows: List[Dict[str, Any]] = []
        try:
            with path.open(newline="", encoding="utf-8-sig") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
        except (OSError, csv.Error) as exc:
            warnings.append(f"screener unavailable: {exc}")
        return rows, freshness(path, stale_after_ms=30 * 60 * 1000), warnings

    def disabled(self) -> Dict[str, Any]:
        return read_json(self.settings.workspace / "watchdog_disable_list.json")

    def allowed_tickers(self) -> set[str]:
        rows, _, _ = self.screener()
        tickers = {str(row.get("Ticker") or "").strip() for row in rows}
        tickers.update(str(item) for item in self.disabled())
        status = self.status()["data"]
        tickers.update(str(item.get("ticker")) for item in status.get("bots", []) if item.get("ticker"))
        for path in self.settings.watchdog_dir.glob("*.json"):
            tickers.add(path.stem)
        return {ticker for ticker in tickers if ticker}

    def require_ticker(self, ticker: str) -> str:
        if ticker not in self.allowed_tickers():
            raise KeyError(ticker)
        return ticker

    def watchdog(self, ticker: str, *, validate: bool = True) -> tuple[Dict[str, Any], Dict[str, Any]]:
        if validate:
            self.require_ticker(ticker)
        active = self.sessions.active_run()
        active_path = Path(active["artifactPath"]) / "watchdog" / f"{ticker}.json" if active else None
        path = active_path if active_path is not None and active_path.exists() else self.settings.watchdog_dir / f"{ticker}.json"
        return read_json(path), freshness(path, stale_after_ms=120_000)

    def pnl(self, window: str = "all") -> Dict[str, Any]:
        windows = {"1h": 3_600_000, "24h": 86_400_000, "7d": 604_800_000, "all": None}
        if window not in windows:
            raise ValueError("window must be one of 1h, 24h, 7d, all")
        duration = windows[window]
        active = self.sessions.active_run()
        active_path = Path(active["artifactPath"]) / "pnl_tracker.jsonl" if active else None
        pnl_path = active_path if active_path is not None and active_path.exists() else self.settings.logs_dir / "pnl_tracker.jsonl"
        fills, warnings = load_fills(
            pnl_path,
            since_ms=(now_ms() - duration) if duration else None,
        )
        return {**summarize_pnl(fills), "window": window, "warnings": warnings, "source": freshness(pnl_path, stale_after_ms=300_000)}

    def telemetry_path(self, ticker: str) -> Path:
        active = self.sessions.active_run()
        candidates = []
        if active:
            candidates.append(Path(active["artifactPath"]) / "markets" / ticker.replace("/", "_").replace("\\", "_") / "telemetry.sqlite3")
        candidates.extend([
            self.settings.workspace / "telemetry" / f"telemetry_{ticker}.sqlite3",
            self.settings.workspace / f"telemetry_{ticker}.sqlite3",
        ])
        return next((path for path in candidates if path.exists()), candidates[-1])

    def markets(self, *, search: str = "", watchdog_mode: str = "", disabled: Optional[bool] = None, sort: str = "rank") -> List[Dict[str, Any]]:
        rows, source, warnings = self.screener()
        status = self.status()["data"]
        bots = {item.get("ticker"): item for item in status.get("bots", [])}
        disabled_rows = self.disabled()
        pnl = {item["ticker"]: item for item in self.pnl()["tickers"]}
        result: List[Dict[str, Any]] = []
        for row in rows:
            ticker = str(row.get("Ticker") or "").strip()
            bot = bots.get(ticker, {})
            watchdog, watchdog_source = self.watchdog(ticker, validate=False)
            item = {
                "ticker": ticker, "title": row.get("SearchText") or ticker,
                "rank": _int_or_none(row.get("Rank")), "expectedEdgeCents": _float_or_none(row.get("Best EV(c)")),
                "quotedEdgeCents": _float_or_none(row.get("Quoted edge(c)")),
                "yesBidCents": _float_or_none(row.get("YES bid(c)")), "noBidCents": _float_or_none(row.get("NO bid(c)")),
                "watchdogMode": watchdog.get("mode") or bot.get("watchdogMode") or "unknown",
                "watchdogConfidence": watchdog.get("confidence"), "watchdogReason": watchdog.get("reason"),
                "botRunning": bool(bot.get("botRunning")), "disabled": ticker in disabled_rows,
                "pnl": pnl.get(ticker), "source": {"screener": source, "watchdog": watchdog_source}, "warnings": warnings,
            }
            if search and search.lower() not in f"{ticker} {item['title']}".lower():
                continue
            if watchdog_mode and item["watchdogMode"] != watchdog_mode:
                continue
            if disabled is not None and item["disabled"] is not disabled:
                continue
            result.append(item)
        keys = {"rank": lambda x: x["rank"] if x["rank"] is not None else 10**9,
                "edge": lambda x: -(x["expectedEdgeCents"] or -10**9),
                "ticker": lambda x: x["ticker"]}
        result.sort(key=keys.get(sort, keys["rank"]))
        return result

    def telemetry(self, ticker: str, table: str, limit: int = 100) -> List[Dict[str, Any]]:
        self.require_ticker(ticker)
        allowed = {"fills", "quotes", "markouts", "market_state"}
        if table not in allowed:
            raise ValueError("unsupported telemetry table")
        path = self.telemetry_path(ticker)
        if not path.exists():
            return []
        uri = f"file:{path}?mode=ro"
        try:
            with sqlite3.connect(uri, uri=True, timeout=0.25) as db:
                db.row_factory = sqlite3.Row
                rows = db.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (min(limit, 500),)).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def market_detail(self, ticker: str) -> Dict[str, Any]:
        self.require_ticker(ticker)
        market = next((item for item in self.markets() if item["ticker"] == ticker), None)
        watchdog, watchdog_source = self.watchdog(ticker)
        return {
            "market": market or {"ticker": ticker, "title": ticker}, "watchdog": watchdog,
            "position": self.position(ticker),
            "telemetry": {table: self.telemetry(ticker, table) for table in ("fills", "quotes", "markouts", "market_state")},
            "source": {"watchdog": watchdog_source, "telemetry": freshness(self.telemetry_path(ticker), stale_after_ms=300_000)},
        }

    def overview(self) -> Dict[str, Any]:
        status = self.status()
        pnl = self.pnl()
        markets = self.markets()
        warnings = list(pnl["warnings"])
        if status["source"].get("stale"):
            warnings.append("launcher heartbeat is stale or unavailable")
        positions = [item for item in pnl["tickers"] if item["netPosition"]]
        position_summary = {
            "markets": len(positions),
            "grossContracts": round(sum(abs(float(item["netPosition"])) for item in positions), 2),
            "netContracts": round(sum(float(item["netPosition"]) for item in positions), 2),
            "source": "fill_telemetry",
        }
        return {
            "generatedAt": now_ms(), "fleet": status["data"], "fleetSource": status["source"],
            "pnl": pnl["totals"], "positions": positions, "positionSummary": position_summary,
            "marketCounts": {"screened": len(markets), "disabled": sum(1 for item in markets if item["disabled"])},
            "warnings": warnings,
        }

    def monitoring(self) -> Dict[str, Any]:
        status = self.status()
        data = status["data"]
        warnings: List[str] = []
        if status["source"].get("stale"):
            warnings.append("launcher monitoring snapshot is stale or unavailable")
        clients = data.get("clients") if isinstance(data.get("clients"), list) else []
        return {
            "generatedAt": now_ms(),
            "schemaVersion": data.get("schemaVersion"),
            "source": status["source"],
            "manager": data.get("manager") or {},
            "clients": clients,
            "screener": data.get("screener") or {},
            "warnings": warnings,
        }

    def portfolio(self) -> Dict[str, Any]:
        status = self.status()
        data = status["data"]
        portfolio = data.get("portfolio") if isinstance(data.get("portfolio"), dict) else {}
        warnings = list(portfolio.get("warnings") or [])
        if status["source"].get("stale"):
            warnings.append("launcher portfolio snapshot is stale or unavailable")
        return {
            "generatedAt": now_ms(),
            "source": status["source"],
            **portfolio,
            "warnings": list(dict.fromkeys(warnings)),
        }

    def portfolio_summary(self, window: str = "24h") -> Dict[str, Any]:
        if window != "24h":
            raise ValueError("window must be 24h")
        return self.portfolio_analytics.summary(window_ms=86_400_000)

    def portfolio_positions(self) -> Dict[str, Any]:
        result = self.portfolio_analytics.positions(active_run=self.sessions.active_run())
        return self._apply_portfolio_links(result, self.status()["data"])

    def portfolio_fills(self, ticker: str, *, limit: int = 100, cursor: str = "") -> Dict[str, Any]:
        if not self.portfolio_analytics.has_market(ticker):
            raise KeyError(ticker)
        return self.portfolio_analytics.fills(ticker, limit=limit, cursor=cursor)

    def portfolio_orders(self) -> Dict[str, Any]:
        active_run = self.sessions.active_run()
        attempts: Dict[str, int] = {}
        market_metrics = ((active_run or {}).get("metrics") or {}).get("markets") or {}
        for market_id, values in market_metrics.items():
            if isinstance(values, dict):
                attempts[str(market_id)] = int(values.get("orderPlacementsAttempted") or 0)
        status = self.status()["data"]
        for client in (status.get("clients") or []) if active_run else []:
            if not isinstance(client, dict):
                continue
            market_id = str(client.get("marketId") or "")
            create = (((client.get("orderActivity") or {}).get("byAction") or {}).get("create") or {})
            if market_id:
                attempts[market_id] = max(attempts.get(market_id, 0), int(create.get("attempts") or 0))
        result = self.portfolio_analytics.orders(active_run=active_run, placement_attempts=attempts)
        return self._apply_portfolio_links(result, status)

    def run_markets(self, run_id: str) -> Dict[str, Any]:
        return self.sessions.run_markets(run_id, market_links=self.portfolio_analytics.market_links())

    def metrics_heartbeat(self) -> Dict[str, Any]:
        """Share a brief session-row result across all SSE/polling clients."""
        current = now_ms()
        with self._heartbeat_lock:
            if self._heartbeat_cache and current - self._heartbeat_cache[0] < 1_500:
                return deepcopy(self._heartbeat_cache[1])
            try:
                payload = self.sessions.metrics_heartbeat()
                self._heartbeat_cache = (current, payload)
                return deepcopy(payload)
            except (sqlite3.Error, OSError, ValueError) as exc:
                if self._heartbeat_cache:
                    payload = deepcopy(self._heartbeat_cache[1])
                    payload["generatedAt"] = current
                    payload["source"] = {
                        **(payload.get("source") or {}),
                        "available": False,
                        "stale": True,
                        "error": str(exc),
                    }
                    if isinstance(payload.get("activeRun"), dict):
                        payload["activeRun"]["source"] = payload["source"]
                    return payload
                return {
                    "generatedAt": current,
                    "activeRun": None,
                    "source": {"available": False, "updatedAt": None, "stale": True, "error": str(exc)},
                }

    def run_market_activity(self, run_id: str, ticker: str, *, fill_limit: int, order_limit: int) -> Dict[str, Any]:
        return self.sessions.run_market_activity(
            run_id,
            ticker,
            fill_limit=fill_limit,
            order_limit=order_limit,
            market_links=self.portfolio_analytics.market_links(),
        )

    def _apply_portfolio_links(self, payload: Dict[str, Any], status: Mapping[str, Any]) -> Dict[str, Any]:
        current_links: Dict[str, str] = {}
        for client in status.get("clients") or []:
            if not isinstance(client, dict):
                continue
            market = client.get("market") if isinstance(client.get("market"), dict) else {}
            market_id = str(client.get("marketId") or market.get("marketId") or "")
            url = market.get("marketUrl")
            if market_id and is_canonical_market_url(url):
                current_links[market_id] = str(url)
        self.portfolio_analytics.cache_market_links(current_links)
        cached_links = self.portfolio_analytics.market_links()
        items = []
        for raw in payload.get("items") or []:
            if not isinstance(raw, dict):
                continue
            item = dict(raw)
            market_id = str(item.get("marketId") or item.get("ticker") or "")
            existing = item.get("marketUrl")
            item["marketUrl"] = cached_links.get(market_id) or (existing if is_canonical_market_url(existing) else None)
            items.append(item)
        return {**payload, "items": items}

    def client_monitoring(self, market_id: str) -> Dict[str, Any]:
        self.require_ticker(market_id)
        snapshot = self.monitoring()
        client = next(
            (item for item in snapshot["clients"] if item.get("marketId") == market_id),
            None,
        )
        if client is None:
            raise KeyError(market_id)
        return {
            "generatedAt": snapshot["generatedAt"],
            "source": snapshot["source"],
            "client": client,
        }

    def tail_log(self, ticker: str, source: str, lines: int = 200) -> List[str]:
        path = self.log_path(ticker, source)
        try:
            with path.open(encoding="utf-8", errors="replace") as handle:
                return handle.readlines()[-min(max(lines, 1), 1000):]
        except OSError:
            return []

    def log_path(self, ticker: str, source: str) -> Path:
        self.require_ticker(ticker)
        if source not in {"bot", "watchdog"}:
            raise ValueError("source must be bot or watchdog")
        suffix = ".watchdog.log" if source == "watchdog" else ".log"
        active = self.sessions.active_run()
        active_path = (Path(active["artifactPath"]) / "logs" / f"{ticker}{suffix}").resolve() if active else None
        path = active_path if active_path is not None and active_path.exists() else (self.settings.logs_dir / f"{ticker}{suffix}").resolve()
        allowed_parent = (Path(active["artifactPath"]) / "logs").resolve() if active_path is not None and active_path.exists() else self.settings.logs_dir
        if path.parent != allowed_parent:
            raise ValueError("invalid log path")
        return path

    def systemd(self, action: str) -> Dict[str, Any]:
        if action not in {"start", "stop", "is-active"}:
            raise ValueError("unsupported service action")
        requested_at_ms = now_ms()
        was_active = False
        if action == "stop":
            before = subprocess.run(
                ["systemctl", "--user", "is-active", self.settings.service_name],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            was_active = (before.stdout or "").strip() == "active"
        completed = subprocess.run(
            ["systemctl", "--user", action, self.settings.service_name],
            capture_output=True,
            text=True,
            timeout=135 if action == "stop" else 30,
            check=False,
        )
        output = (completed.stdout or completed.stderr or "").strip()
        if action != "is-active" and completed.returncode != 0:
            raise RuntimeError(output or f"systemctl returned {completed.returncode}")
        if action == "start":
            # Type=simple can acknowledge before Python imports finish. Require
            # the service to survive the common immediate-crash window.
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                check = subprocess.run(
                    ["systemctl", "--user", "is-active", self.settings.service_name],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                if (check.stdout or "").strip() != "active":
                    raise RuntimeError("service exited during startup; inspect its systemd logs")
                time.sleep(0.2)
        if action != "is-active":
            check = subprocess.run(["systemctl", "--user", "is-active", self.settings.service_name], capture_output=True, text=True, timeout=10, check=False)
            active = (check.stdout or "").strip() == "active"
        else:
            active = output == "active"
        expected = action != "stop"
        if action in {"start", "stop"} and active != expected:
            raise RuntimeError(f"service acknowledgement mismatch: active={active}")
        result = {"service": self.settings.service_name, "action": action, "active": active, "output": output}
        if action == "stop" and was_active:
            status = self.status().get("data") or {}
            manager = status.get("manager") if isinstance(status.get("manager"), dict) else {}
            cleanup = manager.get("shutdownCleanup") if isinstance(manager.get("shutdownCleanup"), dict) else {}
            completed_at = cleanup.get("completedAtMs")
            verified = cleanup.get("ordersVerifiedAbsent") is True
            current_attempt = isinstance(completed_at, (int, float)) and int(completed_at) >= requested_at_ms - 1_000
            if not verified or not current_attempt:
                detail = cleanup.get("error") or "the launcher did not publish a verified order-cleanup result"
                raise RuntimeError(f"service stopped, but bot order cancellation was not verified: {detail}")
            result["shutdownCleanup"] = cleanup
        return result

    def control(self, action: str, *, ticker: Optional[str], operator: str, request_id: Optional[str]) -> Dict[str, Any]:
        request_id = request_id or str(uuid.uuid4())
        target = ticker or "fleet"
        try:
            if action in {"start", "stop"}:
                prepared_run = None
                if action == "start":
                    current = self.systemd("is-active")
                    if current["active"]:
                        raise SessionConflictError("the trading fleet is already running")
                    prepared_run = self.sessions.prepare_run()
                try:
                    result = self.systemd(action)
                except Exception as exc:
                    if prepared_run is not None:
                        self.sessions.fail_pending_run(prepared_run["id"], str(exc))
                    raise
                if prepared_run is not None:
                    result["run"] = self.sessions.get_run(prepared_run["id"])
            else:
                socket_action = {"refresh": "refresh", "disable": "disable_ticker", "enable": "enable_ticker"}[action]
                if ticker:
                    self.require_ticker(ticker)
                if action in {"disable", "enable"} and not self.control_socket.exists():
                    from lip_launcher import clear_disabled_ticker, disable_ticker
                    disable_path = self.settings.workspace / "watchdog_disable_list.json"
                    if action == "disable":
                        assert ticker is not None
                        disable_ticker(disable_path, ticker, "operator_ui_fleet_stopped")
                        result = {"ok": True, "result": {"ticker": ticker, "disabled": True, "fleetRunning": False, "inventoryRetained": True}}
                    else:
                        assert ticker is not None
                        removed = clear_disabled_ticker(disable_path, ticker)
                        result = {"ok": True, "result": {"ticker": ticker, "disabled": False, "wasDisabled": removed, "fleetRunning": False}}
                else:
                    result = send_control_command(self.control_socket, {"request_id": request_id, "action": socket_action, "ticker": ticker})
                if not result.get("ok"):
                    raise RuntimeError(str(result.get("message") or result.get("code") or "launcher rejected command"))
            self.audit.record(request_id, action, target, operator, "success", None)
            return {"generatedAt": now_ms(), "requestId": request_id, "result": result}
        except Exception as exc:
            self.audit.record(request_id, action, target, operator, "failed", str(exc))
            raise

    def record_session_audit(self, request_id: str, action: str, target: str, operator: str) -> None:
        self.audit.record(request_id, action, target, operator, "success", None)


def _float_or_none(value: object) -> Optional[float]:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _int_or_none(value: object) -> Optional[int]:
    try:
        return int(float(value)) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None
