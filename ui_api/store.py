from __future__ import annotations

import csv
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from pnl_core import COUNT_SCALE, find_telemetry_databases, load_fills, load_telemetry_fills, summarize_pnl
from portfolio_analytics import PortfolioAnalyticsStore
from kalshi_urls import is_canonical_market_url
from runtime_control import control_endpoint_available, read_json, send_control_command
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
        # closing() matters on Windows: an unclosed connection lingers until
        # cyclic GC and keeps the file locked against deletion.
        with closing(self._connect()) as db, db:
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
        with closing(self._connect()) as db, db:
            db.execute(
                "INSERT OR REPLACE INTO audit(request_id,timestamp_ms,action,target,operator,result,error) VALUES(?,?,?,?,?,?,?)",
                (request_id, now_ms(), action, target, operator, result, error),
            )

    def list(self, limit: int = 100) -> List[Dict[str, Any]]:
        with closing(self._connect()) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (min(max(limit, 1), 500),)).fetchall()
        return [dict(row) for row in rows]


OPTIMIZER_RUN_ID_PATTERN = re.compile(r"^[0-9a-zA-Z-]+$")
OPTIMIZER_QUEUE_ID_PATTERN = re.compile(r"^q-[0-9a-f]{12}$")


class OptimizerService:
    """Read optimizer artifacts and manage the single UI-launched optimizer process.

    The on-disk contracts (results.sqlite3, report_<run_id>.md, console log)
    are owned by the optimizer package and treated as read-only here. Process
    management mirrors the native Windows launcher runner in OperationsStore
    but keeps its own PID file so the trading fleet is never touched.

    ``start`` maps a JSON payload onto ``python -m optimizer.main``'s CLI
    (tier, class, targeted ``--only-params`` runs, base/screener sessions,
    seeding from a session's bot fields) and can queue a request to launch
    once the running optimizer finishes (``queueAfterCurrent``); a queued
    ``baseSession`` of ``$previous`` resolves to the session the just-finished
    run wrote back, which is how targeted runs are chained.
    """

    # Default worker count; MAX_WORKERS is a HARD LIMIT for this host (it
    # hard-crashes under heavier parallel replay load).
    WORKERS = 8
    MAX_WORKERS = 12
    DEFAULT_TOP_PARAMS = 25
    LOG_TAIL_LINES = 20
    QUEUE_POLL_SECONDS = 30.0
    QUEUE_HISTORY_LIMIT = 20
    PREVIOUS_SESSION = "$previous"
    # Convenience presets for the "only these parameters" multi-select. Each
    # is filtered to the tier's searchable dims when served by ``params``.
    PARAM_PRESETS: Dict[str, List[str]] = {
        "toxicity": [
            "default_toxicity_cents",
            "bucket_pessimism_enabled",
            "bucket_pessimism_max_cents",
            "bucket_pessimism_min_observations",
            "inventory_reduction_max_negative_edge_cents",
            "minimum_expected_edge_cents_to_quote",
            "minimum_expected_edge_cents_to_keep_quote",
            "strong_edge_threshold_cents",
            "fill_probability_prior_fills",
            "fill_probability_prior_misses",
        ],
        "priceFloors": [
            "minimum_best_bid_cents_required_to_quote",
            "minimum_implied_ask_cents_required_to_quote",
            "minimum_market_best_bid_cents_required_to_quote_any_side",
            "maximum_combined_bid_cents",
        ],
    }
    PARAM_GROUPS: List[tuple[str, tuple[str, ...]]] = [
        ("Toxicity & edge", ("toxicity", "edge", "bucket_pessimism", "fill_probability")),
        ("Price floors", ("cents_required_to_quote", "maximum_combined_bid_cents")),
        ("Fair value", ("fair_value",)),
        ("Depth & queue (Tier 2)", ("queue_abandonment", "orderbook_pull", "top_level", "queue_ahead")),
        ("Inventory & guards", ("inventory", "pair_guard", "one_way")),
        ("Sizing", ("budget", "contracts", "quote_size", "fractional", "refill")),
        ("Timing", ("expiration", "cooldown", "requote", "reprice", "refresh", "window", "stagger", "horizon")),
        ("Quoting", ("spread", "improvement", "offset", "join", "safety", "levels_to_scan", "fee", "buffer")),
    ]

    def __init__(self, settings: Settings, audit: AuditStore) -> None:
        self.settings = settings
        self.audit = audit
        self._process: Optional[subprocess.Popen] = None
        self._sessions: Optional[SessionStore] = None
        self._space_cache: Dict[int, Any] = {}
        self._coverage_cache: Optional[tuple[str, float, Dict[str, Any]]] = None
        self._launch_lock = threading.Lock()
        self._queue_lock = threading.Lock()
        self._queue: List[Dict[str, Any]] = []
        self._queue_history: List[Dict[str, Any]] = []
        self._queue_thread: Optional[threading.Thread] = None
        self._queue_stop = threading.Event()

    @property
    def sessions(self) -> SessionStore:
        """Saved sessions (shared sqlite file with OperationsStore.sessions)."""
        if self._sessions is None:
            self._sessions = SessionStore(self.settings.session_dir or self.settings.workspace / "session_data")
        return self._sessions

    @sessions.setter
    def sessions(self, value: SessionStore) -> None:
        self._sessions = value

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    @property
    def optimizer_dir(self) -> Path:
        return self.settings.runtime_dir / "optimizer"

    @property
    def results_db(self) -> Path:
        return self.optimizer_dir / "results.sqlite3"

    @property
    def pid_path(self) -> Path:
        return self.optimizer_dir / "optimizer.pid"

    @property
    def console_log(self) -> Path:
        return self.optimizer_dir / "ui_run_console.log"

    @property
    def base_params_path(self) -> Path:
        return self.optimizer_dir / "base_params_winner.json"

    @property
    def history_db(self) -> Path:
        # Read-only recorded market history; the optimizer replays against it.
        return self.settings.workspace / "history_data" / "history.sqlite3"

    @property
    def record_root(self) -> Path:
        # Tier-2 recording root written by the recorder (read-only here).
        return self.settings.workspace / "record_data"

    @property
    def launch_journal(self) -> Path:
        # One JSON line per dashboard launch (argv, request id, pid) so a
        # run's exact command line can be shown next to its args_json.
        return self.optimizer_dir / "ui_launches.jsonl"

    # ------------------------------------------------------------------
    # Process liveness
    # ------------------------------------------------------------------

    def _pid_alive(self, pid: int) -> bool:
        if sys.platform == "win32":
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return False
                return exit_code.value == STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        try:
            os.kill(int(pid), 0)
            return True
        except OSError:
            return False

    def _read_pid(self) -> Optional[int]:
        try:
            pid = int(self.pid_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return None
        return pid if pid > 0 else None

    def _remove_pid(self) -> None:
        try:
            self.pid_path.unlink()
        except OSError:
            pass

    def running(self) -> bool:
        pid = self._read_pid()
        return pid is not None and self._pid_alive(pid)

    def _db_recently_written(self, within_s: float = 600.0) -> bool:
        """True if the results DB (or its WAL) changed recently.

        The live optimizer appends candidate rows continuously, so a fresh mtime
        means a run is genuinely active — regardless of whether it was launched
        from the dashboard (pid file) or the console (no pid file). Cheap enough
        for the 5s status poll, unlike scanning process command lines.
        """
        newest = 0.0
        for suffix in ("", "-wal"):
            path = Path(str(self.results_db) + suffix)
            try:
                newest = max(newest, path.stat().st_mtime)
            except OSError:
                continue
        return newest > 0.0 and (time.time() - newest) <= within_s

    def _optimizer_active(self) -> bool:
        """Is an optimizer alive? Cheap checks first, process scan as the veto.

        Final-stage evaluations can run many minutes between database writes, so
        write-recency alone wrongly declares a live run dead (it did: a nearly
        finished run was mislabeled 'interrupted'). Never conclude 'dead' from
        the cheap signals alone — confirm with an actual process scan before any
        caller acts on it destructively.
        """
        if self.running():
            return True
        if not self._db_recently_written():
            return False
        # A fresh database only means *something* wrote recently — including
        # this API reconciling a finished run. Confirm with a process scan
        # before reporting a run as live, or the tab shows a phantom run.
        return self._console_optimizer_running()

    # ------------------------------------------------------------------
    # Read models
    # ------------------------------------------------------------------

    def _connect_results(self) -> sqlite3.Connection:
        db = sqlite3.connect(f"file:{self.results_db}?mode=ro", uri=True, timeout=1)
        db.row_factory = sqlite3.Row
        return db

    def _connect_results_rw(self) -> sqlite3.Connection:
        # Read-write handle for orphan reconciliation; short busy timeout so it
        # yields to the live optimizer's WAL writes rather than blocking a list.
        db = sqlite3.connect(str(self.results_db), timeout=5)
        db.execute("PRAGMA busy_timeout=5000")
        return db

    @staticmethod
    def _parse_args_json(args_json: Optional[str]) -> Dict[str, Any]:
        try:
            args = json.loads(args_json or "{}")
        except (TypeError, ValueError):
            args = {}
        return args if isinstance(args, dict) else {}

    @classmethod
    def _args_summary(cls, args_json: Optional[str]) -> Dict[str, Any]:
        """Key arguments of a run, camelCased from optimizer.main's stored ``vars(args)``.

        ``screener_filter`` is either the CLI bool (older rows) or the record
        ``apply_screener_filter`` stores (``{"enabled", "source", ...}``).
        """
        args = cls._parse_args_json(args_json)
        only_params = args.get("only_params")
        if isinstance(only_params, str):
            only_list = [name.strip() for name in only_params.split(",") if name.strip()]
        elif isinstance(only_params, list):
            only_list = [str(name) for name in only_params]
        else:
            only_list = []
        screener = args.get("screener_filter")
        screener_enabled: Optional[bool] = None
        screener_source: Optional[str] = None
        if isinstance(screener, dict):
            screener_enabled = bool(screener.get("enabled")) if "enabled" in screener else None
            screener_source = screener.get("source")
        elif isinstance(screener, bool):
            screener_enabled = screener
        data_source = args.get("data_source") if isinstance(args.get("data_source"), dict) else {}
        try:
            tier = int(args.get("tier") or data_source.get("tier") or 1)
        except (TypeError, ValueError):
            tier = 1
        return {
            "candidates": args.get("candidates"),
            "workers": args.get("workers"),
            "markets": args.get("markets"),
            "budgetMinutes": args.get("budget_minutes"),
            "writeBack": bool(args.get("write_back")),
            "seed": args.get("seed"),
            "baseParams": bool(args.get("base_params_json")),
            "baseParamsJson": args.get("base_params_json") or None,
            "tier": tier,
            "recordRoot": (data_source.get("record_root") or args.get("record_root")) if tier == 2 else None,
            "marketClass": args.get("market_class") or None,
            "onlyParams": only_list,
            "topParams": args.get("top_params"),
            "splits": args.get("splits"),
            "fillShare": args.get("fill_share"),
            "baseSession": args.get("base_session") or None,
            "screenerSession": args.get("screener_session") or None,
            "screenerFilter": screener_enabled,
            "screenerSource": screener_source,
            "screenerEvalHours": args.get("screener_eval_hours"),
            "lastDays": args.get("last_days") or None,
            "fromDate": args.get("from_date") or None,
            "toDate": args.get("to_date") or None,
        }

    def _read_launch_journal(self) -> List[Dict[str, Any]]:
        try:
            text = self.launch_journal.read_text(encoding="utf-8")
        except OSError:
            return []
        entries: List[Dict[str, Any]] = []
        for line in text.splitlines()[-500:]:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if isinstance(entry, dict):
                entries.append(entry)
        return entries

    @staticmethod
    def _launch_for_run(entries: List[Dict[str, Any]], started_ms: Optional[int]) -> Optional[Dict[str, Any]]:
        """The dashboard launch that most plausibly produced a run.

        optimizer.main generates the run id itself, so the only link is time:
        the run row is created shortly (loading markets) after the spawn.
        """
        if started_ms is None:
            return None
        best: Optional[Dict[str, Any]] = None
        for entry in entries:
            launched = entry.get("launchedMs")
            if not isinstance(launched, (int, float)):
                continue
            lag = int(started_ms) - int(launched)
            if -5_000 <= lag <= 600_000 and (best is None or lag < int(started_ms) - int(best["launchedMs"])):
                best = entry
        return best

    @classmethod
    def _run_row(cls, row: sqlite3.Row) -> Dict[str, Any]:
        try:
            markets = json.loads(row["markets_json"] or "[]")
        except (TypeError, ValueError):
            markets = []
        return {
            "id": row["id"],
            "status": row["status"],
            "startedMs": row["started_ms"],
            "finishedMs": row["finished_ms"],
            "dataFromMs": row["data_from_ms"],
            "dataToMs": row["data_to_ms"],
            "marketCount": len(markets) if isinstance(markets, list) else 0,
            "args": cls._args_summary(row["args_json"]),
        }

    def _tail_console(self, count: int) -> List[str]:
        try:
            with self.console_log.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(max(0, size - 65_536))
                data = handle.read()
        except OSError:
            return []
        text = data.decode("utf-8", errors="replace").replace("\x00", "")
        lines = [line.rstrip() for line in text.splitlines()]
        if size > 65_536 and lines:
            lines = lines[1:]  # the first decoded line is usually partial
        return [line for line in lines if line.strip()][-count:]

    def _reconcile_orphans(self) -> None:
        """Mark crash/kill-orphaned 'running'/'starting' rows as 'interrupted'.

        A run row stays 'running' if its process died before writing a terminal
        status (machine crash, TaskStop). At most one optimizer runs at a time
        (enforced at start), so when a process is alive only the newest such row
        is live; every older one — and all of them when nothing is running — is
        stale and must not display as live.
        """
        if not self.results_db.exists():
            return
        try:
            with closing(self._connect_results_rw()) as db:
                rows = db.execute(
                    "SELECT id FROM opt_runs WHERE status IN ('running','starting')"
                    " ORDER BY started_ms DESC"
                ).fetchall()
                if not rows:
                    return
                keep_live = rows[0][0] if self._optimizer_active() else None
                stale = [r[0] for r in rows if r[0] != keep_live]
                if stale:
                    placeholders = ",".join("?" for _ in stale)
                    db.execute(
                        f"UPDATE opt_runs SET status='interrupted', finished_ms=?"
                        f" WHERE id IN ({placeholders})",
                        [now_ms(), *stale],
                    )
                    db.commit()
        except sqlite3.Error:
            pass  # best-effort; never block the listing

    def list_runs(self) -> Dict[str, Any]:
        self._reconcile_orphans()
        items: List[Dict[str, Any]] = []
        if self.results_db.exists():
            try:
                with closing(self._connect_results()) as db:
                    rows = db.execute(
                        "SELECT id, status, started_ms, finished_ms, data_from_ms, data_to_ms,"
                        " markets_json, args_json FROM opt_runs ORDER BY started_ms DESC"
                    ).fetchall()
                items = [self._run_row(row) for row in rows]
            except sqlite3.Error:
                items = []
        return {
            "generatedAt": now_ms(),
            "running": self._optimizer_active(),
            "items": items,
            "queue": self.queue_snapshot(),
            "lastLogLines": self._tail_console(self.LOG_TAIL_LINES),
        }

    def run_detail(self, run_id: str) -> Dict[str, Any]:
        if not OPTIMIZER_RUN_ID_PATTERN.match(run_id or ""):
            raise ValueError("invalid optimizer run id")
        if not self.results_db.exists():
            raise KeyError(run_id)
        with closing(self._connect_results()) as db:
            row = db.execute(
                "SELECT id, status, started_ms, finished_ms, data_from_ms, data_to_ms,"
                " markets_json, args_json FROM opt_runs WHERE id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(run_id)
            # One candidates row exists per (candidate, split); the ranking
            # score is the mean over out-of-sample test splits, mirroring the
            # optimizer's own OOS leaderboard.
            candidate_rows = db.execute(
                """
                SELECT candidate_id, MAX(params_json) AS params_json,
                       AVG(CASE WHEN split_id LIKE 'test%' THEN score_units END) AS oos_score,
                       AVG(CASE WHEN split_id LIKE 'train%' THEN score_units END) AS train_score,
                       SUM(fills) AS fills,
                       SUM(error_count) AS errors
                FROM candidates WHERE opt_run_id=? AND stage='final'
                GROUP BY candidate_id
                ORDER BY oos_score DESC
                LIMIT 20
                """,
                (run_id,),
            ).fetchall()
            sensitivity_rows = db.execute(
                "SELECT field, delta_units, rank FROM sensitivity WHERE opt_run_id=? ORDER BY rank ASC",
                (run_id,),
            ).fetchall()
        leaderboard = []
        for candidate in candidate_rows:
            try:
                params = json.loads(candidate["params_json"] or "{}")
            except (TypeError, ValueError):
                params = {}
            leaderboard.append({
                "candidateId": candidate["candidate_id"],
                "scoreUnits": candidate["oos_score"],
                "trainScoreUnits": candidate["train_score"],
                "fills": int(candidate["fills"] or 0),
                "errorCount": int(candidate["errors"] or 0),
                "params": params if isinstance(params, dict) else {},
            })
        report_path = self.optimizer_dir / f"report_{run_id}.md"
        try:
            report = report_path.read_text(encoding="utf-8")
        except OSError:
            report = None
        launch = self._launch_for_run(self._read_launch_journal(), row["started_ms"])
        return {
            "generatedAt": now_ms(),
            "run": self._run_row(row),
            "argsFull": self._parse_args_json(row["args_json"]),
            "commandLine": launch.get("commandLine") if launch else None,
            "launch": (
                {key: launch.get(key) for key in ("launchId", "requestId", "operator", "launchedMs", "pid", "queueId")}
                if launch else None
            ),
            "leaderboard": leaderboard,
            "sensitivity": [
                {"field": item["field"], "deltaUnits": item["delta_units"], "rank": item["rank"]}
                for item in sensitivity_rows
            ],
            "report": report,
        }

    def data_availability(self) -> Dict[str, Any]:
        """Summarize the recorded history so operators know what --markets and
        time-range values are feasible. Read-only; the history db is owned by
        the backfill/recorder and never written here."""
        path = self.history_db
        if not path.exists():
            return {"available": False}
        try:
            db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
            try:
                market_count = int(db.execute("SELECT COUNT(*) FROM markets").fetchone()[0])
                markets_with_trades = int(
                    db.execute("SELECT COUNT(DISTINCT ticker) FROM trades").fetchone()[0]
                )
                settled_count = int(
                    db.execute("SELECT COUNT(*) FROM markets WHERE status='finalized'").fetchone()[0]
                )
                trade_count = int(db.execute("SELECT COUNT(*) FROM trades").fetchone()[0])
                candle_count = int(db.execute("SELECT COUNT(*) FROM candles").fetchone()[0])
                trade_range = db.execute("SELECT MIN(ts_ms), MAX(ts_ms) FROM trades").fetchone()
                candle_range = db.execute("SELECT MIN(ts_ms), MAX(ts_ms) FROM candles").fetchone()
            finally:
                db.close()
        except sqlite3.Error:
            return {"available": False}
        lows = [value for value in (trade_range[0], candle_range[0]) if value is not None]
        highs = [value for value in (trade_range[1], candle_range[1]) if value is not None]
        from_ms = int(min(lows)) if lows else None
        to_ms = int(max(highs)) if highs else None
        span_days = (
            round((to_ms - from_ms) / 86_400_000, 1)
            if from_ms is not None and to_ms is not None
            else None
        )
        return {
            "available": True,
            "marketCount": market_count,
            "marketsWithTrades": markets_with_trades,
            "settledCount": settled_count,
            "tradeCount": trade_count,
            "candleCount": candle_count,
            "fromMs": from_ms,
            "toMs": to_ms,
            "spanDays": span_days,
        }

    # ------------------------------------------------------------------
    # Controls (audited)
    # ------------------------------------------------------------------

    @staticmethod
    def _bounded_int(payload: Mapping[str, Any], key: str, maximum: int) -> Optional[int]:
        value = payload.get(key)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
            raise ValueError(f"{key} must be an integer")
        value = int(value)
        if not 1 <= value <= maximum:
            raise ValueError(f"{key} must be between 1 and {maximum}")
        return value

    @staticmethod
    def _bounded_float(payload: Mapping[str, Any], key: str, maximum: float) -> Optional[float]:
        value = payload.get(key)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{key} must be a number")
        value = float(value)
        if not 0 < value <= maximum:
            raise ValueError(f"{key} must be greater than 0 and at most {maximum}")
        return value

    @staticmethod
    def _bounded_last_days(payload: Mapping[str, Any]) -> Optional[float]:
        value = payload.get("lastDays")
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("lastDays must be a number")
        value = float(value)
        if not 0.1 <= value <= 3650:
            raise ValueError("lastDays must be between 0.1 and 3650")
        return value

    _DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    @classmethod
    def _validate_date(cls, payload: Mapping[str, Any], key: str) -> Optional[str]:
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{key} must be a string (YYYY-MM-DD or epoch milliseconds)")
        value = value.strip()
        if not value:
            return None
        if cls._DATE_PATTERN.match(value) or value.isdigit():
            return value
        raise ValueError(f"{key} must be YYYY-MM-DD or epoch milliseconds")

    @staticmethod
    def _validate_bool(payload: Mapping[str, Any], key: str, default: bool) -> bool:
        value = payload.get(key)
        if value is None:
            return default
        if not isinstance(value, bool):
            raise ValueError(f"{key} must be true or false")
        return value

    @staticmethod
    def _validate_tier(payload: Mapping[str, Any]) -> int:
        value = payload.get("tier")
        if value is None:
            return 1
        if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
            raise ValueError("tier must be 1 or 2")
        if int(value) not in (1, 2):
            raise ValueError("tier must be 1 or 2")
        return int(value)

    def _validate_record_root(self, payload: Mapping[str, Any], tier: int) -> str:
        value = payload.get("recordRoot")
        if value is None:
            value = "record_data"
        if not isinstance(value, str) or not value.strip():
            raise ValueError("recordRoot must be a non-empty path")
        value = value.strip()
        if tier == 2 and not self._resolve_workspace_path(value).is_dir():
            raise ValueError(f"recordRoot {value!r} is not a directory (Tier 2 needs a recording)")
        return value

    def _resolve_workspace_path(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else self.settings.workspace / path

    @staticmethod
    def _validate_seed(payload: Mapping[str, Any]) -> Optional[int]:
        value = payload.get("seed")
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
            raise ValueError("seed must be an integer")
        if not 0 <= int(value) <= 2_147_483_647:
            raise ValueError("seed must be between 0 and 2147483647")
        return int(value)

    @staticmethod
    def _validate_eval_hours(payload: Mapping[str, Any]) -> Optional[float]:
        value = payload.get("screenerEvalHours")
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("screenerEvalHours must be a number")
        value = float(value)
        if not 0 <= value <= 168:
            raise ValueError("screenerEvalHours must be between 0 (boundaries only) and 168")
        return value

    @staticmethod
    def _validate_class(payload: Mapping[str, Any]) -> Optional[str]:
        value = payload.get("marketClass")
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("marketClass must be a string")
        value = value.strip()
        if not value:
            return None
        from session_config import MARKET_CLASS_NAMES

        if value not in MARKET_CLASS_NAMES:
            raise ValueError("marketClass must be one of " + ", ".join(MARKET_CLASS_NAMES))
        return value

    def _validate_only_params(self, payload: Mapping[str, Any], tier: int) -> List[str]:
        value = payload.get("onlyParams")
        if value is None:
            return []
        if isinstance(value, str):
            value = [name for name in value.split(",")]
        if not isinstance(value, list):
            raise ValueError("onlyParams must be a list of bot field names")
        names: List[str] = []
        for raw in value:
            if not isinstance(raw, str):
                raise ValueError("onlyParams entries must be strings")
            name = raw.strip()
            if name and name not in names:
                names.append(name)
        if not names:
            return []
        space = self._space(tier)
        unknown = [name for name in names if name not in space.dims]
        if unknown:
            details = []
            for name in unknown:
                reason = space.excluded.get(name)
                details.append(f"{name} ({reason})" if reason else f"{name} (unknown field)")
            raise ValueError(f"onlyParams not searchable at tier {tier}: " + ", ".join(details))
        return names

    def _validate_session_name(self, payload: Mapping[str, Any], key: str) -> Optional[str]:
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{key} must be a session name")
        value = value.strip()
        if not value:
            return None
        if value == self.PREVIOUS_SESSION:
            return value
        if len(value) > 80:
            raise ValueError(f"{key} must be at most 80 characters")
        if self._session_by_name(value) is None:
            raise ValueError(f"{key}: no saved session named {value!r}")
        return value

    def _validate_base_params_json(self, payload: Mapping[str, Any]) -> Optional[str]:
        value = payload.get("baseParamsJson")
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("baseParamsJson must be a file path")
        value = value.strip()
        if not value:
            return None
        path = self._resolve_workspace_path(value)
        if not path.is_file():
            raise ValueError(f"baseParamsJson {value!r} does not exist")
        return str(path)

    def _validate_options(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        """Normalize and validate a start payload into the option set ``_build_command`` maps."""
        if not isinstance(payload, Mapping):
            raise ValueError("request body must be an object")
        tier = self._validate_tier(payload)
        options: Dict[str, Any] = {
            "tier": tier,
            "recordRoot": self._validate_record_root(payload, tier),
            "candidates": self._bounded_int(payload, "candidates", 500),
            "markets": self._bounded_int(payload, "markets", 200),
            "budgetMinutes": self._bounded_float(payload, "budgetMinutes", 600.0),
            "topParams": self._bounded_int(payload, "topParams", 100),
            "workers": self._bounded_int(payload, "workers", self.MAX_WORKERS) or self.WORKERS,
            "splits": self._bounded_int(payload, "splits", 10),
            "fillShare": self._bounded_float(payload, "fillShare", 1.0),
            "seed": self._validate_seed(payload),
            "marketClass": self._validate_class(payload),
            "onlyParams": self._validate_only_params(payload, tier),
            "baseSession": self._validate_session_name(payload, "baseSession"),
            "screenerSession": self._validate_session_name(payload, "screenerSession"),
            "screenerFilter": self._validate_bool(payload, "screenerFilter", True),
            "screenerEvalHours": self._validate_eval_hours(payload),
            "writeBack": bool(payload.get("writeBack", True)),
            "useBaseParams": bool(payload.get("useBaseParams", True)),
            "baseParamsJson": self._validate_base_params_json(payload),
            "lastDays": self._bounded_last_days(payload),
            "fromDate": self._validate_date(payload, "fromDate"),
            "toDate": self._validate_date(payload, "toDate"),
        }
        from_date, to_date = options["fromDate"], options["toDate"]
        if (from_date and to_date and self._DATE_PATTERN.match(from_date)
                and self._DATE_PATTERN.match(to_date) and from_date > to_date):
            raise ValueError("fromDate must not be after toDate")
        return options

    def _python_executable(self) -> str:
        python = self.settings.workspace / ".venv" / "Scripts" / "python.exe"
        return str(python) if python.exists() else sys.executable

    def _command_prefix(self) -> List[str]:
        return [
            self._python_executable(), "-m", "optimizer.main",
            "--history", str(self.settings.workspace / "history_data" / "history.sqlite3"),
        ]

    @staticmethod
    def _number_arg(value: Any) -> str:
        number = float(value)
        return str(int(number)) if number.is_integer() else str(number)

    def _build_command(self, options: Mapping[str, Any]) -> List[str]:
        """Map validated options onto optimizer.main's argv (pure; no side effects).

        ``baseParamsJson`` must already be resolved (``_prepare_base_params``);
        ``$previous`` session names are passed through verbatim so a queued
        item's preview shows the placeholder until launch resolves it.
        """
        tier = int(options.get("tier") or 1)
        seed = options.get("seed")
        command = [*self._command_prefix(), "--tier", str(tier)]
        if tier == 2:
            command += ["--record-root", str(options.get("recordRoot") or "record_data")]
        command += [
            "--workers", str(options.get("workers") or self.WORKERS),
            "--seed", str(int(time.time()) if seed is None else int(seed)),
            "--top-params", str(options.get("topParams") or self.DEFAULT_TOP_PARAMS),
            "--output-dir", str(self.optimizer_dir),
        ]
        if options.get("candidates") is not None:
            command += ["--candidates", str(options["candidates"])]
        if options.get("markets") is not None:
            command += ["--markets", str(options["markets"])]
        if options.get("budgetMinutes") is not None:
            command += ["--budget-minutes", str(options["budgetMinutes"])]
        if options.get("splits") is not None:
            command += ["--splits", str(options["splits"])]
        if options.get("fillShare") is not None:
            command += ["--fill-share", str(options["fillShare"])]
        if options.get("marketClass"):
            command += ["--class", str(options["marketClass"])]
        if options.get("onlyParams"):
            command += ["--only-params", ",".join(options["onlyParams"])]
        if options.get("baseSession"):
            command += ["--base-session", str(options["baseSession"])]
        if options.get("screenerSession"):
            command += ["--screener-session", str(options["screenerSession"])]
        if options.get("screenerFilter") is False:
            command.append("--no-screener-filter")
        if options.get("screenerEvalHours") is not None:
            command += ["--screener-eval-hours", self._number_arg(options["screenerEvalHours"])]
        if options.get("baseParamsJson"):
            command += ["--base-params-json", str(options["baseParamsJson"])]
        elif options.get("useBaseParams") and not options.get("baseSession") and self.base_params_path.exists():
            command += ["--base-params-json", str(self.base_params_path)]
        if options.get("writeBack"):
            command.append("--write-back")
        if options.get("lastDays") is not None:
            command += ["--last-days", self._number_arg(options["lastDays"])]
        if options.get("fromDate"):
            command += ["--from-date", str(options["fromDate"])]
        if options.get("toDate"):
            command += ["--to-date", str(options["toDate"])]
        return command

    @staticmethod
    def _command_line(command: List[str]) -> str:
        return subprocess.list2cmdline(command)

    # ------------------------------------------------------------------
    # Search space, sessions and recorder coverage (form data)
    # ------------------------------------------------------------------

    def _space(self, tier: int):
        space = self._space_cache.get(int(tier))
        if space is None:
            from optimizer.space import build_space

            space = build_space(int(tier))
            self._space_cache[int(tier)] = space
        return space

    @classmethod
    def _param_group(cls, name: str) -> str:
        for label, needles in cls.PARAM_GROUPS:
            if any(needle in name for needle in needles):
                return label
        return "Other"

    def params(self, tier: int = 1) -> Dict[str, Any]:
        """Searchable fields (with bounds) and pinned fields (with reasons) for a tier."""
        tier = self._validate_tier({"tier": tier})
        space = self._space(tier)
        searchable = [
            {
                "name": name, "kind": dim.kind, "low": dim.low, "high": dim.high,
                "default": dim.default, "group": self._param_group(name),
            }
            for name, dim in sorted(space.dims.items())
        ]
        pinned = [{"name": name, "reason": reason} for name, reason in sorted(space.excluded.items())]
        presets = {
            key: [name for name in names if name in space.dims]
            for key, names in self.PARAM_PRESETS.items()
        }
        groups = [label for label, _ in self.PARAM_GROUPS] + ["Other"]
        return {
            "generatedAt": now_ms(), "tier": tier, "searchable": searchable, "pinned": pinned,
            "presets": presets, "groups": [g for g in groups if any(p["group"] == g for p in searchable)],
        }

    def _session_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        for session in self.sessions.list_sessions(include_archived=True):
            if session.get("name") == name:
                return session
        return None

    def recorder_coverage(self, record_root: Optional[Path] = None) -> Dict[str, Any]:
        """Cheap Tier-2 coverage: recorded hours/days and the markets named in
        each hour's ``meta.json`` (no decoding of the multi-GB message files)."""
        root = record_root or self.record_root
        cached = self._coverage_cache
        if cached and cached[0] == str(root) and time.time() - cached[1] < 60.0:
            return cached[2]
        hours = 0
        days: set = set()
        markets: set = set()
        total_bytes = 0
        first_ms: Optional[int] = None
        last_ms: Optional[int] = None
        if root.is_dir():
            for day_dir in sorted(root.iterdir()):
                if not (day_dir.is_dir() and day_dir.name.isdigit() and len(day_dir.name) == 8):
                    continue
                for hour_dir in sorted(day_dir.iterdir()):
                    if not (hour_dir.is_dir() and hour_dir.name.isdigit() and len(hour_dir.name) == 2):
                        continue
                    files = [p for p in hour_dir.iterdir() if p.is_file() and p.name.startswith("conn") and ".jsonl" in p.name]
                    if not files:
                        continue
                    try:
                        import datetime as _dt

                        hour_start = int(_dt.datetime(
                            int(day_dir.name[0:4]), int(day_dir.name[4:6]), int(day_dir.name[6:8]),
                            int(hour_dir.name), tzinfo=_dt.timezone.utc,
                        ).timestamp() * 1000)
                    except ValueError:
                        continue
                    hours += 1
                    days.add(day_dir.name)
                    for path in files:
                        try:
                            total_bytes += path.stat().st_size
                        except OSError:
                            pass
                    first_ms = hour_start if first_ms is None else min(first_ms, hour_start)
                    last_ms = hour_start + 3_600_000 if last_ms is None else max(last_ms, hour_start + 3_600_000)
                    try:
                        meta = json.loads((hour_dir / "meta.json").read_text(encoding="utf-8"))
                        connections = meta.get("connections") if isinstance(meta, dict) else None
                        for tickers in (connections or {}).values():
                            if isinstance(tickers, list):
                                markets.update(str(t) for t in tickers)
                    except (OSError, ValueError, AttributeError):
                        pass
        result = {
            "available": hours > 0,
            "recordRoot": str(root),
            "hours": hours,
            "days": len(days),
            "markets": len(markets),
            "bytes": total_bytes,
            "fromMs": first_ms,
            "toMs": last_ms,
        }
        self._coverage_cache = (str(root), time.time(), result)
        return result

    def options(self) -> Dict[str, Any]:
        """Everything the start form needs to populate itself."""
        from session_config import MARKET_CLASS_NAMES

        sessions = []
        for session in self.sessions.list_sessions(include_archived=False):
            configuration = session.get("configuration") or {}
            sessions.append({
                "id": session.get("id"),
                "name": session.get("name"),
                "hasScreener": isinstance(configuration.get("screener"), dict),
                "selected": bool(session.get("selected")),
                "updatedAt": session.get("updatedAt"),
                "description": session.get("description"),
            })
        coverage = self.recorder_coverage()
        return {
            "generatedAt": now_ms(),
            "classes": list(MARKET_CLASS_NAMES),
            "sessions": sessions,
            "tiers": [
                {"tier": 1, "label": "Tier 1 - candles + trades (history db)", "available": self.history_db.exists()},
                {"tier": 2, "label": "Tier 2 - recorded order books (full depth, queue model)", "available": bool(coverage["available"])},
            ],
            "recorder": coverage,
            "workers": {"default": self.WORKERS, "max": self.MAX_WORKERS},
            "defaults": {"topParams": self.DEFAULT_TOP_PARAMS, "splits": 3, "fillShare": 0.5, "screenerEvalHours": None},
            "presets": {key: list(names) for key, names in self.PARAM_PRESETS.items()},
            "previousSessionToken": self.PREVIOUS_SESSION,
            "baseParamsWinnerAvailable": self.base_params_path.exists(),
            "commandPrefix": self._command_prefix(),
            "outputDir": str(self.optimizer_dir),
        }

    # ------------------------------------------------------------------
    # Launch helpers (session resolution, base-params export, journal)
    # ------------------------------------------------------------------

    def _last_written_back_session(self) -> Dict[str, Any]:
        """Session written back by the most recently finished run (``$previous``)."""
        if not self.results_db.exists():
            raise ValueError("$previous: no optimizer results database yet")
        with closing(self._connect_results()) as db:
            row = db.execute(
                "SELECT id FROM opt_runs WHERE status='finished' ORDER BY finished_ms DESC, started_ms DESC LIMIT 1"
            ).fetchone()
        if row is None:
            raise ValueError("$previous: no finished optimizer run to chain from")
        run_id = row["id"]
        marker = f"optimizer run {run_id}"
        candidates = [
            session for session in self.sessions.list_sessions(include_archived=True)
            if marker in str(session.get("description") or "")
        ]
        if not candidates:
            raise ValueError(f"$previous: run {run_id} did not write back a session (was --write-back set?)")
        candidates.sort(key=lambda s: int(s.get("createdAt") or 0), reverse=True)
        return candidates[0]

    def _resolve_sessions(self, options: Mapping[str, Any]) -> Dict[str, Any]:
        resolved = dict(options)
        previous: Optional[Dict[str, Any]] = None
        for key in ("baseSession", "screenerSession"):
            if resolved.get(key) == self.PREVIOUS_SESSION:
                previous = previous or self._last_written_back_session()
                resolved[key] = previous["name"]
        return resolved

    def _export_session_bot_params(self, name: str, launch_id: str) -> Path:
        session = self._session_by_name(name)
        if session is None:
            raise ValueError(f"baseSession: no saved session named {name!r}")
        bot = (session.get("configuration") or {}).get("bot") or {}
        if not isinstance(bot, dict) or not bot:
            raise ValueError(f"baseSession {name!r} has no bot section to seed from")
        self.optimizer_dir.mkdir(parents=True, exist_ok=True)
        path = self.optimizer_dir / f"base_params_{launch_id}.json"
        path.write_text(json.dumps(bot, indent=1, sort_keys=True), encoding="utf-8")
        return path

    def _prepare_base_params(self, options: Mapping[str, Any], launch_id: str) -> Dict[str, Any]:
        """Decide the ``--base-params-json`` file: explicit file > base session export > last winner."""
        prepared = dict(options)
        if prepared.get("baseParamsJson"):
            return prepared
        if prepared.get("baseSession"):
            prepared["baseParamsJson"] = str(self._export_session_bot_params(prepared["baseSession"], launch_id))
        return prepared

    def _journal(self, entry: Dict[str, Any]) -> None:
        try:
            self.optimizer_dir.mkdir(parents=True, exist_ok=True)
            with self.launch_journal.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
        except OSError:
            pass  # the journal is informational; never fail a launch over it

    def _launch(self, options: Mapping[str, Any], operator: str, request_id: str, queue_id: Optional[str] = None) -> Dict[str, Any]:
        launch_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
        resolved = self._prepare_base_params(self._resolve_sessions(options), launch_id)
        command = self._build_command(resolved)
        pid = self._spawn(command)
        command_line = self._command_line(command)
        self._journal({
            "launchId": launch_id, "requestId": request_id, "operator": operator, "queueId": queue_id,
            "launchedMs": now_ms(), "pid": pid, "command": command, "commandLine": command_line,
            "options": resolved,
        })
        return {
            "started": True,
            "pid": pid,
            "launchId": launch_id,
            "workers": resolved.get("workers") or self.WORKERS,
            "consoleLog": str(self.console_log),
            "command": command,
            "commandLine": command_line,
            "options": resolved,
        }

    # ------------------------------------------------------------------
    # Queue: launch after the running optimizer finishes
    # ------------------------------------------------------------------

    def _queue_item_view(self, item: Dict[str, Any], position: Optional[int]) -> Dict[str, Any]:
        options = item.get("options") or {}
        return {
            "id": item["id"],
            "position": position,
            "status": item.get("status", "queued"),
            "requestId": item.get("requestId"),
            "operator": item.get("operator"),
            "queuedAtMs": item.get("queuedAtMs"),
            "launchedMs": item.get("launchedMs"),
            "finishedMs": item.get("finishedMs"),
            "error": item.get("error"),
            "options": options,
            "commandLine": item.get("commandLine"),
            "pid": (item.get("result") or {}).get("pid"),
        }

    def queue_snapshot(self) -> List[Dict[str, Any]]:
        with self._queue_lock:
            pending = [self._queue_item_view(item, index + 1) for index, item in enumerate(self._queue)]
            history = [self._queue_item_view(item, None) for item in self._queue_history]
        return pending + history

    def _enqueue(self, options: Dict[str, Any], operator: str, request_id: str) -> Dict[str, Any]:
        item = {
            "id": "q-" + uuid.uuid4().hex[:12],
            "status": "queued",
            "requestId": request_id,
            "operator": operator,
            "queuedAtMs": now_ms(),
            "options": options,
            "commandLine": self._command_line(self._build_command(options)),
        }
        with self._queue_lock:
            self._queue.append(item)
            position = len(self._queue)
        self._ensure_queue_thread()
        return {**self._queue_item_view(item, position)}

    def _ensure_queue_thread(self) -> None:
        if self._queue_thread is not None and self._queue_thread.is_alive():
            return
        self._queue_stop.clear()
        self._queue_thread = threading.Thread(target=self._queue_loop, name="optimizer-queue", daemon=True)
        self._queue_thread.start()

    def _queue_loop(self) -> None:
        while not self._queue_stop.wait(self.QUEUE_POLL_SECONDS):
            with self._queue_lock:
                idle = not self._queue
            if idle:
                continue
            try:
                self.process_queue()
            except Exception:
                pass  # the next poll retries; failures are recorded on the item

    def _record_queue_history(self, item: Dict[str, Any]) -> None:
        with self._queue_lock:
            self._queue_history.insert(0, item)
            del self._queue_history[self.QUEUE_HISTORY_LIMIT:]

    def process_queue(self) -> Optional[Dict[str, Any]]:
        """Launch the head of the queue if no optimizer is alive. Returns the item handled, if any."""
        with self._launch_lock:
            with self._queue_lock:
                if not self._queue:
                    return None
            if self._optimizer_active():
                return None
            with self._queue_lock:
                if not self._queue:
                    return None
                item = self._queue.pop(0)
            try:
                result = self._launch(item["options"], item["operator"], item["requestId"], queue_id=item["id"])
                item.update(status="launched", launchedMs=now_ms(), result=result, commandLine=result["commandLine"])
                self.audit.record(item["requestId"], "optimizer_queue_launch", item["id"], item["operator"], "success", None)
            except Exception as exc:
                item.update(status="failed", finishedMs=now_ms(), error=str(exc))
                self.audit.record(item["requestId"], "optimizer_queue_launch", item["id"], item["operator"], "failed", str(exc))
        self._record_queue_history(item)
        return self._queue_item_view(item, None)

    def cancel_queued(self, queue_id: str, operator: str, request_id: str) -> Dict[str, Any]:
        if not OPTIMIZER_QUEUE_ID_PATTERN.match(queue_id or ""):
            raise ValueError("invalid optimizer queue id")
        with self._queue_lock:
            index = next((i for i, item in enumerate(self._queue) if item["id"] == queue_id), None)
            if index is None:
                self.audit.record(request_id, "optimizer_queue_cancel", queue_id, operator, "failed", "not queued")
                raise KeyError(queue_id)
            item = self._queue.pop(index)
        item.update(status="cancelled", finishedMs=now_ms())
        self._record_queue_history(item)
        self.audit.record(request_id, "optimizer_queue_cancel", queue_id, operator, "success", None)
        return {"generatedAt": now_ms(), "requestId": request_id, "result": self._queue_item_view(item, None)}

    def _clear_queue(self, reason: str) -> int:
        with self._queue_lock:
            items, self._queue = self._queue, []
        for item in items:
            item.update(status="cancelled", finishedMs=now_ms(), error=reason)
            self._record_queue_history(item)
        return len(items)

    def _spawn(self, command: List[str]) -> int:
        self.optimizer_dir.mkdir(parents=True, exist_ok=True)
        creationflags = (
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
        with self.console_log.open("ab") as console:
            console.write(f"----- optimizer start requested at {now_ms()} -----\n".encode("utf-8"))
            console.flush()
            process = subprocess.Popen(
                command,
                cwd=str(self.settings.workspace),
                stdin=subprocess.DEVNULL,
                stdout=console,
                stderr=console,
                creationflags=creationflags,
            )
        self._process = process
        self.pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
        # Require the process to survive the common immediate-crash window
        # (bad args, missing history db) so failures surface to the operator.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if not self._pid_alive(process.pid):
                self._remove_pid()
                raise RuntimeError(f"the optimizer exited during startup; inspect {self.console_log}")
            time.sleep(0.2)
        return process.pid

    def _console_optimizer_running(self) -> bool:
        """Detect optimizer.main processes not started by the dashboard.

        Concurrent optimizer runs double the worker count, and this host
        hard-crashes under that load — refuse rather than stack runs.
        """
        try:
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\""
                 " | Select-Object -ExpandProperty CommandLine"],
                capture_output=True, text=True, timeout=15, check=False,
            )
            return "optimizer.main" in (completed.stdout or "")
        except Exception:
            return False

    def start(self, payload: Mapping[str, Any], operator: str, request_id: str) -> Dict[str, Any]:
        """Validate the payload and launch the optimizer, or queue it behind the running one.

        ``queueAfterCurrent: true`` records the request and lets the queue
        poller launch it once nothing is running (launches immediately when
        the optimizer is idle and the queue is empty). Without it a busy
        optimizer is a 409 conflict, as before.
        """
        try:
            options = self._validate_options(payload)
            queue_after = self._validate_bool(payload, "queueAfterCurrent", False)
            if not queue_after and self.PREVIOUS_SESSION in (options.get("baseSession"), options.get("screenerSession")):
                # Immediate launches resolve $previous now so a bad chain fails fast.
                options = self._resolve_sessions(options)
            with self._launch_lock:
                with self._queue_lock:
                    queue_length = len(self._queue)
                pid_alive = self.running()
                console_alive = False if pid_alive else self._console_optimizer_running()
                if queue_after and (pid_alive or console_alive or queue_length > 0):
                    item = self._enqueue(options, operator, request_id)
                    result = {"started": False, "queued": True, "queueId": item["id"], "position": item["position"], "commandLine": item["commandLine"], "options": options}
                    self.audit.record(request_id, "optimizer_start", "optimizer", operator, "success", None)
                    return {"generatedAt": now_ms(), "requestId": request_id, "result": result}
                if pid_alive:
                    raise SessionConflictError("an optimizer run is already in progress")
                if console_alive:
                    raise SessionConflictError(
                        "an optimizer run started outside the dashboard is in progress; "
                        "wait for it to finish (concurrent runs overload this host)"
                    )
                result = self._launch(options, operator, request_id)
            result["queued"] = False
            self.audit.record(request_id, "optimizer_start", "optimizer", operator, "success", None)
            return {"generatedAt": now_ms(), "requestId": request_id, "result": result}
        except Exception as exc:
            self.audit.record(request_id, "optimizer_start", "optimizer", operator, "failed", str(exc))
            raise

    def _terminate(self, pid: int) -> None:
        if sys.platform == "win32":
            # /T kills the whole tree so multiprocessing pool children die too.
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        else:
            import signal

            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except OSError:
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass
        process = self._process
        if process is not None and process.pid == pid:
            try:
                process.kill()
            except OSError:
                pass
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and self._pid_alive(pid):
            time.sleep(0.25)
        if self._pid_alive(pid):
            raise RuntimeError(f"optimizer pid {pid} did not exit after termination")

    def stop(self, operator: str, request_id: str) -> Dict[str, Any]:
        try:
            pid = self._read_pid()
            was_running = pid is not None and self._pid_alive(pid)
            if was_running and pid is not None:
                self._terminate(pid)
            self._remove_pid()
            # A stop means "halt the chain": queued items must not launch on
            # the next poll (their $previous would resolve to an older run).
            cleared = self._clear_queue("cancelled: optimizer stopped by operator")
            result = {
                "stopped": was_running,
                "queueCleared": cleared,
                "output": f"terminated pid {pid}" if was_running else "no optimizer process is running",
            }
            self.audit.record(request_id, "optimizer_stop", "optimizer", operator, "success", None)
            return {"generatedAt": now_ms(), "requestId": request_id, "result": result}
        except Exception as exc:
            self.audit.record(request_id, "optimizer_stop", "optimizer", operator, "failed", str(exc))
            raise


class OperationsStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.audit = AuditStore(settings.runtime_dir / "ui_audit.sqlite3")
        self.optimizer = OptimizerService(settings, self.audit)
        self.sessions = SessionStore(settings.session_dir or settings.workspace / "session_data")
        self.portfolio_analytics = PortfolioAnalyticsStore(settings.runtime_dir / "portfolio_analytics.sqlite3")
        self._position_cache: Dict[str, tuple[int, Dict[str, Any]]] = {}
        self._pnl_lock = threading.Lock()
        self._pnl_cache: Dict[str, tuple[int, Dict[str, Any]]] = {}
        self._heartbeat_lock = threading.Lock()
        self._heartbeat_cache: Optional[tuple[int, Dict[str, Any]]] = None
        self._service_state_lock = threading.Lock()
        self._service_state_cache: Optional[tuple[int, str]] = None
        self._launcher_process: Optional[subprocess.Popen] = None

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

    def _service_state(self) -> str:
        current = now_ms()
        with self._service_state_lock:
            if self._service_state_cache and current - self._service_state_cache[0] < 1_000:
                return self._service_state_cache[1]
            try:
                completed = subprocess.run(
                    ["systemctl", "--user", "is-active", self.settings.service_name],
                    capture_output=True,
                    text=True,
                    timeout=3,
                    check=False,
                )
                state = (completed.stdout or completed.stderr or "").strip()
            except FileNotFoundError:
                # No systemctl on this host (Windows): report the native
                # launcher process state instead.
                if sys.platform != "win32":
                    raise
                state = "active" if self._windows_launcher_active() else "inactive"
            self._service_state_cache = (current, state)
            return state

    @staticmethod
    def _stopped_status(status: Dict[str, Any]) -> Dict[str, Any]:
        status = dict(status)
        launcher = status.get("launcher") if isinstance(status.get("launcher"), dict) else {}
        status["launcher"] = {
            **launcher,
            "lifecycle": "stopped",
            "nextRefreshAt": None,
            "pendingAction": None,
        }
        counts = status.get("counts") if isinstance(status.get("counts"), dict) else {}
        status["counts"] = {**counts, "activeBots": 0, "watchdogModes": {}}
        status["bots"] = [
            {**item, "botRunning": False, "watchdogRunning": False, "socketHealthy": False}
            for item in status.get("bots", [])
            if isinstance(item, dict)
        ]
        manager = status.get("manager") if isinstance(status.get("manager"), dict) else {}
        status["manager"] = {
            **manager,
            "running": False,
            "lifecycle": "stopped",
            "botsRunning": 0,
        }
        status["clients"] = [
            {
                **item,
                "lifecycle": "stopped",
                "socketHealthy": False,
                "watchdog": {
                    **(item.get("watchdog") if isinstance(item.get("watchdog"), dict) else {}),
                    "running": False,
                },
            }
            for item in status.get("clients", [])
            if isinstance(item, dict)
        ]
        screener = status.get("screener") if isinstance(status.get("screener"), dict) else {}
        status["screener"] = {**screener, "running": False, "currentStartedAtMs": None}
        return status

    def status(self) -> Dict[str, Any]:
        status = read_json(self.status_path)
        source = freshness(self.status_path)
        heartbeat = ((status.get("launcher") or {}).get("heartbeatAt"))
        if isinstance(heartbeat, (int, float)):
            source["stale"] = now_ms() - int(heartbeat) > 10_000
        launcher = status.get("launcher") if isinstance(status.get("launcher"), dict) else {}
        if launcher.get("lifecycle") == "stopped":
            # A stopped launcher has no live watchdogs; its final snapshot can
            # still carry non-empty watchdogModes/bot flags, which render as
            # phantom running watchdogs on Overview. Clear them.
            status = self._stopped_status(status)
        elif source.get("stale") and launcher.get("lifecycle") in {"starting", "running", "stopping"}:
            try:
                service_state = self._service_state()
                source["serviceState"] = service_state
                if service_state in {"inactive", "failed"}:
                    status = self._stopped_status(status)
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

    def disabled_page(self, *, limit: int, offset: int = 0) -> Dict[str, Any]:
        rows = [
            {"ticker": ticker, **(value if isinstance(value, dict) else {"reason": str(value)})}
            for ticker, value in sorted(self.disabled().items())
        ]
        offset = max(0, int(offset))
        page = rows[offset:offset + limit]
        return {
            "items": page, "totalCount": len(rows),
            "nextCursor": str(offset + len(page)) if offset + len(page) < len(rows) else None,
        }

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

    PNL_CACHE_TTL_MS = 2_000

    PNL_MAX_RUNS = 60

    def _pnl_artifacts(self, since_ms: Optional[int] = None, scope: str = "session") -> List[Path]:
        """Artifact directories the P&L view reads, newest first.

        ``scope="session"`` (the Overview) is the current run only: the
        Overview reports this session, the Portfolio page reports the exchange
        across sessions. ``scope="all"`` unions every run that overlaps the
        window (each run writes its own telemetry).
        """
        artifacts: List[Path] = []
        seen: set = set()

        def add(run: Mapping[str, Any]) -> None:
            path = run.get("artifactPath")
            if path and str(path) not in seen:
                seen.add(str(path))
                artifacts.append(Path(str(path)))

        active = self.sessions.active_run()
        if active:
            add(active)
        if scope == "session" and artifacts:
            return artifacts
        try:
            runs = self.sessions.list_runs(include_artifact_bytes=False)
        except (sqlite3.Error, OSError, ValueError, KeyError):
            return artifacts
        if scope == "session":
            # No active run: the newest run stands in for "this session".
            newest = sorted(runs, key=lambda run: int(run.get("createdAt") or 0), reverse=True)
            for run in newest:
                if run.get("artifactPath"):
                    add(run)
                    break
            return artifacts
        runs = sorted(runs, key=lambda run: int(run.get("createdAt") or 0), reverse=True)
        for run in runs:
            if len(artifacts) >= self.PNL_MAX_RUNS:
                break
            ended = run.get("endedAt")
            if since_ms is not None and isinstance(ended, (int, float)) and int(ended) < since_ms:
                continue  # finished before the window opened
            add(run)
        return artifacts

    def _pnl_artifact(self) -> Optional[Path]:
        """Artifact directory of the active run, else the newest run, else None."""
        artifacts = self._pnl_artifacts()
        return artifacts[0] if artifacts else None

    def pnl_fill_source(self, since_ms: Optional[int] = None, scope: str = "session") -> Dict[str, Any]:
        """Resolve where P&L fills come from without reading them.

        Preference order: ``<artifact>/shards/*/telemetry.sqlite3`` of the
        run(s) in scope (sharded fleet; the only source that records inventory
        before/after each fill and therefore sells), then
        ``<artifact>/pnl_tracker.jsonl`` (legacy V1 bot, buys only), then the
        workspace-level ``logs/pnl_tracker.jsonl``.
        """
        artifacts = self._pnl_artifacts(since_ms, scope)
        databases: List[Path] = []
        for artifact in artifacts:
            databases.extend(find_telemetry_databases(artifact))
        if databases:
            return {"kind": "shard_telemetry", "path": artifacts[0], "paths": databases, "artifact": artifacts[0], "runs": len(artifacts)}
        artifact = artifacts[0] if artifacts else None
        if artifact is not None:
            jsonl = artifact / "pnl_tracker.jsonl"
            if jsonl.exists():
                return {"kind": "pnl_tracker", "path": jsonl, "paths": [jsonl], "artifact": artifact, "runs": 1}
        legacy = self.settings.logs_dir / "pnl_tracker.jsonl"
        return {"kind": "pnl_tracker", "path": legacy, "paths": [legacy], "artifact": artifact, "runs": 0}

    @staticmethod
    def _telemetry_freshness(paths: Iterable[Path], *, stale_after_ms: int) -> Dict[str, Any]:
        latest: Optional[int] = None
        errors: List[str] = []
        for path in paths:
            for candidate in (path, path.with_name(path.name + "-wal")):
                try:
                    modified = int(candidate.stat().st_mtime * 1000)
                except OSError as exc:
                    if candidate == path:
                        errors.append(str(exc))
                    continue
                latest = modified if latest is None or modified > latest else latest
        if latest is None:
            return {"available": False, "updatedAt": None, "stale": True, "error": "; ".join(errors) or "no telemetry databases"}
        return {"available": True, "updatedAt": latest, "stale": now_ms() - latest > stale_after_ms}

    def pnl(self, window: str = "all", scope: str = "session") -> Dict[str, Any]:
        windows = {"1h": 3_600_000, "24h": 86_400_000, "7d": 604_800_000, "all": None}
        if window not in windows:
            raise ValueError("window must be one of 1h, 24h, 7d, all")
        if scope not in {"session", "all"}:
            raise ValueError("scope must be session or all")
        current = now_ms()
        cache_key = f"{window}:{scope}"
        with self._pnl_lock:
            cached = self._pnl_cache.get(cache_key)
            if cached and current - cached[0] < self.PNL_CACHE_TTL_MS:
                return deepcopy(cached[1])
        duration = windows[window]
        since_ms = (current - duration) if duration else None
        source = self.pnl_fill_source(since_ms, scope)
        if source["kind"] == "shard_telemetry":
            fills, warnings = load_telemetry_fills(list(source["paths"]), since_ms=since_ms)
            freshness_state = self._telemetry_freshness(source["paths"], stale_after_ms=300_000)
        else:
            fills, warnings = load_fills(source["path"], since_ms=since_ms)
            freshness_state = freshness(source["path"], stale_after_ms=300_000)
        summary = summarize_pnl(fills)
        estimated = [item["ticker"] for item in summary["tickers"] if item.get("basisEstimated")]
        if estimated:
            gaps = sum(int(item.get("basisGapFills") or 0) for item in summary["tickers"])
            warnings = list(warnings) + [
                f"cost basis estimated for {len(estimated)} market(s): position carried into the "
                f"{'window' if since_ms is not None else 'run'}"
                + (f" or changed between fills ({gaps} gap(s))" if gaps else "")
                + " was seeded at pre-fill fair value: " + ", ".join(estimated[:5])
            ]
        result = {
            **summary, "window": window, "scope": scope, "warnings": warnings,
            "source": {
                **freshness_state, "kind": source["kind"],
                "path": str(source["path"]), "shards": len(source["paths"]) if source["kind"] == "shard_telemetry" else 0,
                "runs": int(source.get("runs") or 0),
            },
        }
        with self._pnl_lock:
            self._pnl_cache[cache_key] = (current, deepcopy(result))
        return result

    def telemetry_path(self, ticker: str) -> Path:
        active = self.sessions.active_run()
        candidates = []
        if active:
            artifact = Path(active["artifactPath"])
            candidates.append(artifact / "markets" / ticker.replace("/", "_").replace("\\", "_") / "telemetry.sqlite3")
            try:
                manifest = json.loads((artifact / "fleet_manifest.json").read_text(encoding="utf-8"))
                worker_id = str((manifest.get("tickerToShard") or {}).get(ticker) or "")
                if worker_id and "/" not in worker_id and "\\" not in worker_id:
                    candidates.insert(0, artifact / "shards" / worker_id / "telemetry.sqlite3")
            except (OSError, ValueError, TypeError):
                pass
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
                "watchdogConfidence": watchdog.get("confidence"),
                # Fleet workers publish their evaluator reason on the bot row
                # (no per-ticker watchdog state file exists for them).
                "watchdogReason": watchdog.get("reason") or bot.get("watchdogReason") or None,
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
                rows = db.execute(
                    f"SELECT * FROM {table} WHERE ticker=? ORDER BY id DESC LIMIT ?",
                    (ticker, min(limit, 500)),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def telemetry_page(self, ticker: str, table: str, *, limit: int, offset: int = 0) -> Dict[str, Any]:
        self.require_ticker(ticker)
        allowed = {"fills", "quotes", "markouts", "market_state", "ticker_updates", "public_trades", "runtime_events"}
        if table not in allowed:
            raise ValueError("unsupported telemetry table")
        path = self.telemetry_path(ticker)
        if not path.exists():
            return {"items": [], "totalCount": 0, "nextCursor": None}
        offset = max(0, int(offset))
        try:
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.25) as db:
                db.row_factory = sqlite3.Row
                if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                    return {"items": [], "totalCount": 0, "nextCursor": None}
                total = int(db.execute(f"SELECT COUNT(*) FROM {table} WHERE ticker=?", (ticker,)).fetchone()[0])
                rows = db.execute(
                    f"SELECT * FROM {table} WHERE ticker=? ORDER BY id DESC LIMIT ? OFFSET ?",
                    (ticker, limit, offset),
                ).fetchall()
                items = [dict(row) for row in rows]
                return {
                    "items": items, "totalCount": total,
                    "nextCursor": str(offset + len(items)) if offset + len(items) < total else None,
                }
        except sqlite3.Error:
            return {"items": [], "totalCount": 0, "nextCursor": None}

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
        allocation = status["data"].get("allocation") if isinstance(status["data"].get("allocation"), dict) else {}
        if allocation.get("error"):
            warnings.append(f"allocation: {allocation['error']}")
        exchange = self._exchange_positions(status["data"])
        if exchange["available"]:
            # The exchange is the truth for what is open. Show it for the
            # markets this session manages (fleet bots + session fills); other
            # account positions belong to the Portfolio page, not the session.
            positions, mismatches = self._session_positions_from_exchange(
                pnl["tickers"], exchange["positions"], status["data"].get("bots") or [],
            )
            position_source = "exchange"
        else:
            positions, mismatches = self._cross_check_positions(pnl["tickers"], None)
            position_source = "fill_telemetry"
        if mismatches:
            warnings.append(
                f"session fill telemetry differs from the exchange for {len(mismatches)} market(s): "
                + ", ".join(mismatches[:5])
            )
        if exchange["available"]:
            # A position the fleet opened in an earlier run, in a market no bot
            # runs now, has nobody managing it (a restart does not carry
            # inventory over the way a refresh does). Say so.
            managed = {str(bot.get("ticker") or "") for bot in status["data"].get("bots") or [] if isinstance(bot, dict)}
            unmanaged = {
                ticker for ticker, net in exchange["positions"].items()
                if net and ticker not in managed
            }
            try:
                fleet_traded = self.portfolio_analytics.bot_traded_markets(unmanaged)
            except Exception:
                fleet_traded = set()
            orphaned = sorted(
                ticker for ticker in unmanaged if ticker in fleet_traded
            )
            if orphaned:
                warnings.append(
                    f"{len(orphaned)} exchange position(s) in fleet-traded market(s) no bot is running now: "
                    + ", ".join(orphaned[:5])
                )
        position_summary = {
            "markets": len(positions),
            "grossContracts": round(sum(abs(float(item["netPosition"])) for item in positions), 2),
            "netContracts": round(sum(float(item["netPosition"]) for item in positions), 2),
            "source": position_source,
            "fillSource": (pnl.get("source") or {}).get("kind"),
            "exchangeCheck": {
                "available": exchange["available"], "source": exchange["source"], "updatedAt": exchange["updatedAt"],
                "checked": sum(1 for item in positions if item.get("positionMatchesExchange") is not None),
                "mismatches": len(mismatches),
            },
        }
        return {
            "generatedAt": now_ms(), "fleet": status["data"], "fleetSource": status["source"],
            "pnl": pnl["totals"], "positions": positions, "positionSummary": position_summary,
            "marketCounts": {"screened": len(markets), "disabled": sum(1 for item in markets if item["disabled"])},
            "warnings": warnings,
        }

    @staticmethod
    def _position_rows_to_contracts(items: Iterable[Any]) -> Dict[str, float]:
        """Signed contracts per ticker from portfolio-monitor position rows (``side`` + ``contractsUnits``)."""
        result: Dict[str, float] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("marketId") or item.get("ticker") or "")
            units = item.get("contractsUnits")
            if not ticker or isinstance(units, bool) or not isinstance(units, (int, float)):
                continue
            contracts = abs(float(units)) / COUNT_SCALE
            result[ticker] = -contracts if item.get("side") == "no" else contracts
        return result

    def _exchange_positions(self, status: Mapping[str, Any]) -> Dict[str, Any]:
        """Exchange positions the dashboard already receives, without a new venue call.

        The launcher's portfolio monitor publishes the exchange positions
        snapshot in ``launcher_status.json`` (``portfolio.positions``); when the
        launcher is not running, the persisted snapshot in
        ``runtime/portfolio_analytics.sqlite3`` is used instead. Returns
        ``available=False`` when neither carries a snapshot.
        """
        portfolio = status.get("portfolio") if isinstance(status.get("portfolio"), dict) else {}
        if portfolio.get("available") and isinstance(portfolio.get("positions"), list):
            updated = portfolio.get("lastSuccessAtMs") or portfolio.get("generatedAtMs")
            return {
                "available": True, "source": "launcher_portfolio", "updatedAt": int(updated) if isinstance(updated, (int, float)) else None,
                "positions": self._position_rows_to_contracts(portfolio["positions"]),
            }
        try:
            snapshot = self.portfolio_analytics.positions(active_run=None)
        except (sqlite3.Error, OSError, ValueError, TypeError):
            snapshot = {}
        source = snapshot.get("source") or {}
        if source.get("available") and isinstance(snapshot.get("items"), list):
            return {
                "available": True, "source": "portfolio_analytics", "updatedAt": snapshot.get("snapshotAtMs"),
                "positions": self._position_rows_to_contracts(snapshot["items"]),
            }
        return {"available": False, "source": None, "updatedAt": None, "positions": {}}

    @staticmethod
    def _session_positions_from_exchange(
        rows: Iterable[Mapping[str, Any]], exchange: Mapping[str, float], bots: Iterable[Mapping[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """Open positions for this session, sized by the exchange.

        A market is the session's when a fleet bot runs it or the session's
        telemetry has fills in it. ``netPosition`` is the exchange's number;
        the telemetry row (P&L, basis) is attached when present, and a
        telemetry position that disagrees with the exchange is reported as a
        mismatch. Exchange positions in unrelated markets are left to the
        Portfolio page.
        """
        telemetry = {str(row.get("ticker") or ""): row for row in rows}
        managed = {str(bot.get("ticker") or "") for bot in bots if isinstance(bot, dict) and bot.get("ticker")}
        positions: List[Dict[str, Any]] = []
        mismatches: List[str] = []
        for ticker, net in sorted(exchange.items()):
            if not net or (ticker not in managed and ticker not in telemetry):
                continue
            row = dict(telemetry.get(ticker) or {"ticker": ticker})
            recorded = telemetry.get(ticker)
            recorded_net = float(recorded.get("netPosition") or 0) if recorded else None
            row.update({
                "netPosition": float(net), "exchangePosition": float(net),
                "positionMatchesExchange": None if recorded_net is None else abs(recorded_net - float(net)) < 0.005,
                "positionSource": "exchange",
            })
            positions.append(row)
        for ticker, row in telemetry.items():
            recorded_net = float(row.get("netPosition") or 0)
            if recorded_net and abs(recorded_net - float(exchange.get(ticker, 0.0))) >= 0.005:
                mismatches.append(ticker)
        return positions, mismatches

    @staticmethod
    def _cross_check_positions(
        rows: Iterable[Mapping[str, Any]], exchange: Optional[Mapping[str, float]],
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """Open positions from the fill summary, annotated with the exchange's position.

        ``netPosition`` (contracts) comes from the latest fill's post-trade
        inventory. When an exchange positions snapshot is available every
        telemetry position is compared with it (a ticker absent from the
        snapshot counts as flat, e.g. a settled market) and disagreements are
        returned as mismatches so the Overview can flag them. Exchange
        positions in markets without telemetry fills are not the fleet's and
        are left alone. Without a snapshot no claim is made
        (``positionMatchesExchange`` is ``None``).
        """
        positions: List[Dict[str, Any]] = []
        mismatches: List[str] = []
        for row in rows:
            ticker = str(row.get("ticker") or "")
            net = float(row.get("netPosition") or 0)
            live = None if exchange is None else float(exchange.get(ticker, 0.0))
            matches = None if live is None else abs(live - net) < 0.005
            if matches is False:
                mismatches.append(ticker)
            if not net:
                continue
            item = dict(row)
            item["exchangePosition"] = live
            item["positionMatchesExchange"] = matches
            positions.append(item)
        return positions, mismatches

    def monitoring(self) -> Dict[str, Any]:
        status = self.status()
        data = status["data"]
        warnings: List[str] = []
        if status["source"].get("stale"):
            warnings.append("launcher monitoring snapshot is stale or unavailable")
        clients = data.get("clients") if isinstance(data.get("clients"), list) else []
        try:
            screener_history = self.sessions.screener_runs(limit=100)
        except (OSError, sqlite3.Error, ValueError) as exc:
            screener_history = {
                "items": [], "summary": {}, "nextCursor": None,
            }
            warnings.append(f"screener history unavailable: {exc}")
        screener = dict(data.get("screener") or {})
        screener.update({
            "history": screener_history.get("items", []),
            "historySummary": screener_history.get("summary", {}),
            "historyNextCursor": screener_history.get("nextCursor"),
            "historyWarnings": screener_history.get("warnings", []),
        })
        return {
            "generatedAt": now_ms(),
            "schemaVersion": data.get("schemaVersion"),
            "source": status["source"],
            "manager": data.get("manager") or {},
            "workers": data.get("workers") or [],
            "broker": data.get("broker") or {},
            "capacity": data.get("capacity"),
            "allocation": data.get("allocation"),
            "clients": clients,
            "screener": screener,
            "warnings": warnings,
        }

    def screener_runs(self, *, session_id: str = "", limit: int = 100, cursor: str = "") -> Dict[str, Any]:
        """Return local, persisted screener refresh history and aggregates."""
        generated = now_ms()
        try:
            payload = self.sessions.screener_runs(session_id=session_id, limit=limit, cursor=cursor)
        except (OSError, sqlite3.Error) as exc:
            return {
                "items": [], "summary": {}, "nextCursor": None, "generatedAt": generated,
                "source": {"available": False, "updatedAt": generated, "stale": True},
                "warnings": [f"screener history unavailable: {exc}"],
            }
        payload["generatedAt"] = generated
        payload["source"] = {"available": True, "updatedAt": generated, "stale": False}
        payload.setdefault("warnings", [])
        return payload

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
        windows = {"24h": 86_400_000, "7d": 7 * 86_400_000, "30d": 30 * 86_400_000}
        if window not in windows:
            raise ValueError("window must be one of 24h, 7d, or 30d")
        return self.portfolio_analytics.summary(window_ms=windows[window])

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
        if active:
            artifact = Path(active["artifactPath"])
            try:
                manifest = json.loads((artifact / "fleet_manifest.json").read_text(encoding="utf-8"))
                worker_id = str((manifest.get("tickerToShard") or {}).get(ticker) or "")
                shard_log = (artifact / "shards" / worker_id / "worker.log").resolve()
                shard_root = (artifact / "shards").resolve()
                shard_log.relative_to(shard_root)
                if shard_log.exists():
                    return shard_log
            except (OSError, ValueError, TypeError):
                pass
        path = active_path if active_path is not None and active_path.exists() else (self.settings.logs_dir / f"{ticker}{suffix}").resolve()
        allowed_parent = (Path(active["artifactPath"]) / "logs").resolve() if active_path is not None and active_path.exists() else self.settings.logs_dir
        if path.parent != allowed_parent:
            raise ValueError("invalid log path")
        return path

    def systemd(self, action: str) -> Dict[str, Any]:
        if action not in {"start", "stop", "is-active"}:
            raise ValueError("unsupported service action")
        try:
            return self._systemd_linux(action)
        except FileNotFoundError:
            # systemctl does not exist on this host. On Windows fall back to
            # the native launcher runner; elsewhere surface the real error.
            if sys.platform != "win32":
                raise
            return self._windows_service(action)

    def _systemd_linux(self, action: str) -> Dict[str, Any]:
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

    # ------------------------------------------------------------------
    # Native Windows fleet runner (used when systemctl is unavailable)
    # ------------------------------------------------------------------

    _windows_stop_timeout_seconds: float = 120.0

    @property
    def launcher_pid_path(self) -> Path:
        return self.settings.runtime_dir / "launcher.pid"

    @property
    def launcher_console_log(self) -> Path:
        return self.settings.runtime_dir / "launcher_console.log"

    def _read_launcher_pids(self) -> List[int]:
        """Every pid recorded for the managed launcher: the venv stub first,
        then the interpreter it hands off to (_windows_record_launcher_children)."""
        try:
            tokens = self.launcher_pid_path.read_text(encoding="utf-8").split()
        except OSError:
            return []
        pids: List[int] = []
        for token in tokens:
            try:
                value = int(token)
            except ValueError:
                continue
            if value > 0 and value not in pids:
                pids.append(value)
        return pids

    def _read_launcher_pid(self) -> Optional[int]:
        pids = self._read_launcher_pids()
        return pids[0] if pids else None

    def _windows_record_launcher_children(self, pid: int, *, timeout_s: float = 5.0) -> List[int]:
        """Append the interpreter(s) the venv stub hands off to.

        `.venv\\Scripts\\python.exe` is a redirector: it spawns the base
        interpreter and waits for it. Only the stub's pid is known at spawn
        time, but the stub can die while that interpreter (and the whole
        fleet) keeps running, so liveness and stop must track both.
        """
        deadline = time.monotonic() + timeout_s
        children: List[int] = []
        while True:
            for child, parent, command in self._windows_list_python_processes():
                if parent == pid and "lip_launcher" in command.lower() and child not in children:
                    children.append(child)
            if children or time.monotonic() >= deadline:
                break
            time.sleep(0.25)
        if children:
            with self.launcher_pid_path.open("a", encoding="utf-8") as handle:
                handle.write("".join(f"{child}\n" for child in children))
        return children

    def _remove_launcher_pid(self) -> None:
        try:
            self.launcher_pid_path.unlink()
        except OSError:
            pass

    def _windows_pid_alive(self, pid: int) -> bool:
        """Liveness via OpenProcess/GetExitCodeProcess (no psutil needed)."""
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)

    def _windows_launcher_active(self) -> bool:
        return any(self._windows_pid_alive(pid) for pid in self._read_launcher_pids())

    def _windows_launcher_command(self) -> List[str]:
        workspace = self.settings.workspace
        session_dir = self.settings.session_dir or (workspace / "session_data")
        # Mirrors the path arguments run_lip.sh passes on Linux; all numeric
        # tuning comes from the saved-session configuration that lip_launcher
        # applies via --session-store.
        return [
            str(workspace / ".venv" / "Scripts" / "python.exe"),
            str(workspace / "lip_launcher.py"),
            "--screen-file", str(workspace / "screener_export.csv"),
            "--bot-script", str(workspace / "V1.py"),
            "--screener-script", str(workspace / "kalshi_screener.py"),
            "--screener-output", str(workspace / "screener_export.csv"),
            "--watchdog-runner-script", str(workspace / "market_watchdog_runner.py"),
            "--watchdog-profiler-script", str(workspace / "market_risk_profiler.py"),
            "--watchdog-state-dir", str(self.settings.watchdog_dir),
            "--watchdog-disable-file", str(workspace / "watchdog_disable_list.json"),
            "--logs-dir", str(self.settings.logs_dir),
            "--runtime-dir", str(self.settings.runtime_dir),
            "--session-store", str(session_dir),
        ]

    def _windows_spawn_launcher(self) -> int:
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        # CREATE_NEW_PROCESS_GROUP isolates live trading from stray console
        # signals: without it the fleet shares this API process's console, so
        # any Ctrl+C there (restarting the dashboard, a broadcast console event)
        # killed the running fleet mid-session. The launcher now installs a
        # SIGBREAK handler running the same graceful shutdown as SIGINT, so stop
        # targets this group with CTRL_BREAK (see _windows_signal_launcher).
        creationflags = (
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
        with self.launcher_console_log.open("ab") as console:
            console.write(f"----- launcher start requested at {now_ms()} -----\n".encode("utf-8"))
            console.flush()
            process = subprocess.Popen(
                self._windows_launcher_command(),
                cwd=str(self.settings.workspace),
                stdin=subprocess.DEVNULL,
                stdout=console,
                stderr=console,
                creationflags=creationflags,
            )
        self._launcher_process = process
        self.launcher_pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
        return process.pid

    def _windows_signal_launcher(self, pid: int) -> None:
        """Deliver CTRL_BREAK to the launcher's own process group.

        The launcher is spawned with CREATE_NEW_PROCESS_GROUP, so its group id
        equals its pid and this signal reaches only the fleet — never this API
        process or anything else sharing a console. lip_launcher installs a
        SIGBREAK handler that runs the same graceful shutdown (order
        cancellation + verification) as SIGINT.

        The previous implementation attached to the launcher's console and
        called GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0); group 0 broadcasts to
        every process on that console, which could kill unrelated processes and
        left the fleet itself killable by any stray console Ctrl+C.
        """
        try:
            os.kill(pid, signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        except Exception as exc:  # os.kill raises SystemError on Windows when the event cannot be generated
            raise RuntimeError(f"failed to signal the launcher (pid {pid}): {exc}") from exc

    def _windows_request_shutdown_via_control(self) -> bool:
        """Ask the launcher to shut down over its control endpoint; True when acknowledged."""
        try:
            if not control_endpoint_available(self.control_socket):
                return False
            response = send_control_command(
                self.control_socket,
                {"request_id": f"stop-{uuid.uuid4()}", "action": "shutdown"},
                timeout=15.0,
            )
        except Exception:
            return False
        return bool(isinstance(response, dict) and response.get("ok"))

    def _windows_terminate_launcher(self, pid: int) -> None:
        process = self._launcher_process
        if process is not None and process.pid == pid:
            try:
                process.kill()
            except OSError:
                pass
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )

    @staticmethod
    def _windows_list_python_processes() -> List[tuple[int, int, str]]:
        """(pid, parent_pid, command_line) for every python.exe on the host."""
        try:
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
                 "ForEach-Object { \"$($_.ProcessId)|$($_.ParentProcessId)|$($_.CommandLine)\" }"],
                capture_output=True, text=True, timeout=20, check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception:  # a failed scan must never block start/stop
            return []
        rows: List[tuple[int, int, str]] = []
        for line in (completed.stdout or "").splitlines():
            parts = line.split("|", 2)
            if len(parts) != 3:
                continue
            try:
                rows.append((int(parts[0]), int(parts[1]), parts[2]))
            except ValueError:
                continue
        return rows

    def _windows_status_worker_pids(self) -> tuple[Optional[int], List[int]]:
        """(launcher pid, worker pids) as last published in launcher_status.json."""
        try:
            status = json.loads((self.settings.runtime_dir / "launcher_status.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None, []
        launcher = status.get("launcher") if isinstance(status, dict) else None
        launcher_pid = launcher.get("pid") if isinstance(launcher, dict) else None
        workers = status.get("workers") if isinstance(status, dict) else None
        pids: List[int] = []
        for worker in workers if isinstance(workers, list) else []:
            pid = worker.get("pid") if isinstance(worker, dict) else None
            if isinstance(pid, int) and pid > 0:
                pids.append(pid)
        return (launcher_pid if isinstance(launcher_pid, int) and launcher_pid > 0 else None), pids

    def _windows_sweep_orphaned_workers(self, *, processes: Optional[List[tuple[int, int, str]]] = None) -> List[int]:
        """Terminate fleet worker/broker processes whose launcher is gone.

        Workers are multiprocessing children of the launcher. If the launcher
        dies without shutting them down (crash, forced kill, host hiccup) they
        keep streaming, evaluating and logging with stale settings — a second
        phantom fleet sharing the console log and API budget, invisible to the
        dashboard. Two signals identify them: a multiprocessing spawn child
        (`spawn_main` on its command line — that line never names the
        workspace) whose parent pid no longer exists, and the worker pids the
        dead launcher last published in launcher_status.json. A spawn child
        with a live parent (the optimizer's pool, a running fleet) is never
        touched.
        """
        rows = self._windows_list_python_processes() if processes is None else processes
        alive = {pid for pid, _, _ in rows}
        spawn_children = {pid for pid, _, command in rows if "spawn_main" in command.lower()}
        targets: List[int] = [pid for pid, parent, _ in rows if pid in spawn_children and parent not in alive]
        status_launcher, status_workers = self._windows_status_worker_pids()
        if status_launcher is not None and status_launcher not in alive:
            targets.extend(pid for pid in status_workers if pid in spawn_children and pid not in targets)
        killed: List[int] = []
        for pid in targets:
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/F"], capture_output=True, text=True,
                    timeout=15, check=False, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                killed.append(pid)
            except (OSError, subprocess.SubprocessError):
                continue
        if killed:
            with self.launcher_console_log.open("ab") as console:
                console.write(
                    f"----- swept {len(killed)} orphaned worker process(es) at {now_ms()}: {killed} -----\n".encode("utf-8")
                )
        return killed

    def _windows_service(self, action: str) -> Dict[str, Any]:
        requested_at_ms = now_ms()
        service = self.settings.service_name
        if action == "is-active":
            active = self._windows_launcher_active()
            return {"service": service, "action": action, "active": active, "output": "active" if active else "inactive"}
        if action == "start":
            if self._windows_launcher_active():
                raise RuntimeError("the launcher process is already running")
            # Leftover workers from a crashed/killed launcher would run beside
            # the new fleet with stale settings; clear them first.
            self._windows_sweep_orphaned_workers()
            pid = self._windows_spawn_launcher()
            # Require the launcher to survive the common immediate-crash
            # window (mirrors the Linux Type=simple startup check).
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                if not self._windows_pid_alive(pid):
                    self._remove_launcher_pid()
                    raise RuntimeError(f"service exited during startup; inspect {self.launcher_console_log}")
                time.sleep(0.2)
            children = self._windows_record_launcher_children(pid)
            output = f"started pid {pid}" + (f" (interpreter {', '.join(map(str, children))})" if children else "")
            return {"service": service, "action": action, "active": True, "output": output}
        # stop
        pids = self._read_launcher_pids()
        was_active = any(self._windows_pid_alive(pid) for pid in pids)
        if was_active:
            # Preferred: the launcher's control endpoint. The launcher runs on
            # its own hidden console (CREATE_NO_WINDOW), which a CTRL_BREAK
            # from this process cannot reach, so the signal is only a fallback.
            if not self._windows_request_shutdown_via_control():
                try:
                    self._windows_signal_launcher(pids[0])
                except RuntimeError:
                    for pid in pids:
                        if self._windows_pid_alive(pid):
                            self._windows_terminate_launcher(pid)
                    self._remove_launcher_pid()
                    self._windows_sweep_orphaned_workers()
                    raise RuntimeError(
                        "the launcher could not be signalled and was forcibly terminated; bot order cancellation "
                        "was NOT verified - check open orders on the exchange"
                    )
            deadline = time.monotonic() + float(self._windows_stop_timeout_seconds)
            while time.monotonic() < deadline and any(self._windows_pid_alive(pid) for pid in pids):
                time.sleep(0.5)
            lingering = [pid for pid in pids if self._windows_pid_alive(pid)]
            if lingering:
                for pid in lingering:
                    self._windows_terminate_launcher(pid)
                self._remove_launcher_pid()
                self._windows_sweep_orphaned_workers()
                raise RuntimeError(
                    f"the launcher did not stop within {int(self._windows_stop_timeout_seconds)} seconds and was "
                    "forcibly terminated; bot order cancellation was NOT verified - check open orders on the exchange"
                )
        self._remove_launcher_pid()
        # The launcher is gone; any worker it left behind is now an orphan.
        swept = self._windows_sweep_orphaned_workers()
        result: Dict[str, Any] = {
            "service": service,
            "action": action,
            "active": False,
            "output": "stopped" if was_active else "inactive",
        }
        if swept:
            result["sweptOrphanWorkers"] = swept
        if was_active:
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
                if action in {"disable", "enable"} and not control_endpoint_available(self.control_socket):
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

    def reconcile_stale_runs(self) -> None:
        """Release session selection when a run outlives its launcher.

        A run row stays 'running' if the launcher exits without finalizing
        (crash, kill, host reboot), which locks session selection and blocks
        the next start with no way to clear it from the UI. Only reconcile once
        the service is confirmed not running, so a live fleet is never disturbed.
        """
        try:
            if self.sessions.active_run() is None:
                return
            if self._service_state() not in {"inactive", "failed"}:
                return
            self.sessions.reconcile_orphaned_runs()
        except Exception:
            # Best effort: a listing must never fail because cleanup did.
            pass

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
