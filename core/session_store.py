"""SQLite persistence for saved sessions, immutable runs, and local metrics."""

from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import time
import uuid
from collections import OrderedDict, deque
from contextlib import closing, contextmanager
from copy import deepcopy
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional

from core.markout_metrics import (
    MARKOUT_HORIZONS_MS,
    add_observation,
    combine_markout_maps,
    empty_markout_aggregate,
    finalize_aggregate,
    monetary_markout_units,
)
from core.session_config import default_session_configuration, validate_session_configuration


ACTIVE_RUN_STATES = ("pending", "starting", "running")
MARKET_CACHE_RUN_LIMIT = 32
HEARTBEAT_STALE_AFTER_MS = 10_000


class SessionConflictError(RuntimeError):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


def _screener_venue(configuration: Mapping[str, Any], reason: str = "") -> str:
    """Infer a historical screener venue when older rows lack the column."""
    configured = str(configuration.get("venue") or "").strip().lower()
    if configured in {"kalshi", "polymarket"}:
        return configured
    if str(reason or "").strip().lower() == "mirror_ready":
        return "polymarket"
    venues = configuration.get("venues")
    if isinstance(venues, Mapping):
        enabled = [
            str(name).strip().lower()
            for name, value in venues.items()
            if str(name).strip().lower() in {"kalshi", "polymarket"}
            and isinstance(value, Mapping) and bool(value.get("enabled"))
        ]
        if len(enabled) == 1:
            return enabled[0]
    return "unknown"


class SessionStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.path = self.root / "sessions.sqlite3"
        self.artifacts_root = self.root / "artifacts"
        self._market_cache_lock = RLock()
        self._market_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self.root.mkdir(parents=True, exist_ok=True)
        self.artifacts_root.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self.bootstrap_default()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=5)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=5000")
            db.execute("PRAGMA foreign_keys=ON")
            with db:
                yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._connect() as db:
            # Journal mode is a persistent database setting. Applying it on every
            # read connection can require a write lock and turn observability reads
            # into a source of contention for the running launcher.
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL COLLATE NOCASE UNIQUE,
                    description TEXT NOT NULL DEFAULT '', configuration_json TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1, created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL, archived_at_ms INTEGER
                );
                CREATE TABLE IF NOT EXISTS selection (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), session_id TEXT NOT NULL,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, session_name TEXT NOT NULL,
                    configuration_version INTEGER NOT NULL, configuration_json TEXT NOT NULL,
                    status TEXT NOT NULL, created_at_ms INTEGER NOT NULL, started_at_ms INTEGER,
                    ended_at_ms INTEGER, heartbeat_at_ms INTEGER, artifact_path TEXT NOT NULL,
                    metrics_json TEXT, error TEXT,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                );
                CREATE INDEX IF NOT EXISTS idx_runs_session_started ON runs(session_id, created_at_ms DESC);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
                CREATE TABLE IF NOT EXISTS metric_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, timestamp_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL, FOREIGN KEY(run_id) REFERENCES runs(id)
                );
                CREATE INDEX IF NOT EXISTS idx_metric_samples_run_ts ON metric_samples(run_id, timestamp_ms);
                CREATE TABLE IF NOT EXISTS screener_runs (
                    id TEXT PRIMARY KEY,
                    source_key TEXT NOT NULL UNIQUE,
                    fleet_run_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    session_name TEXT NOT NULL,
                    venue TEXT NOT NULL DEFAULT 'unknown',
                    generation_id INTEGER,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at_ms INTEGER NOT NULL,
                    ended_at_ms INTEGER,
                    duration_ms INTEGER,
                    configured_limit INTEGER,
                    effective_limit INTEGER,
                    scanned_markets INTEGER,
                    api_requests INTEGER,
                    api_errors INTEGER,
                    added_count INTEGER NOT NULL DEFAULT 0,
                    changed_count INTEGER NOT NULL DEFAULT 0,
                    removed_count INTEGER NOT NULL DEFAULT 0,
                    inventory_carried_count INTEGER NOT NULL DEFAULT 0,
                    inventory_unknown_count INTEGER NOT NULL DEFAULT 0,
                    warnings_json TEXT NOT NULL DEFAULT '[]',
                    error TEXT,
                    artifact_path TEXT,
                    created_at_ms INTEGER NOT NULL,
                    FOREIGN KEY(fleet_run_id) REFERENCES runs(id)
                );
                CREATE INDEX IF NOT EXISTS idx_screener_runs_started ON screener_runs(started_at_ms DESC, id DESC);
                CREATE INDEX IF NOT EXISTS idx_screener_runs_session_started ON screener_runs(session_id, started_at_ms DESC, id DESC);
                CREATE TABLE IF NOT EXISTS screener_run_backfill (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), completed_at_ms INTEGER NOT NULL
                );
                PRAGMA user_version=1;
            """)
            screener_columns = {
                str(row["name"])
                for row in db.execute("PRAGMA table_info(screener_runs)").fetchall()
            }
            venue_column_added = "venue" not in screener_columns
            if venue_column_added:
                db.execute(
                    "ALTER TABLE screener_runs ADD COLUMN venue TEXT NOT NULL DEFAULT 'unknown'"
                )
            # Older live rows predate the venue field. Recover it when the
            # parent run configuration makes the answer unambiguous; rows
            # from multi-venue sessions remain explicitly unknown.
            if venue_column_added:
                legacy_rows = db.execute(
                    """SELECT screener_runs.id, screener_runs.reason, runs.configuration_json
                       FROM screener_runs JOIN runs ON runs.id=screener_runs.fleet_run_id
                       WHERE screener_runs.venue='unknown'"""
                ).fetchall()
                for row in legacy_rows:
                    try:
                        configuration = json.loads(row["configuration_json"] or "{}")
                    except (TypeError, ValueError):
                        configuration = {}
                    venue = _screener_venue(configuration, row["reason"])
                    if venue != "unknown":
                        db.execute(
                            "UPDATE screener_runs SET venue=? WHERE id=?",
                            (venue, row["id"]),
                        )

    @staticmethod
    def _configuration(row: sqlite3.Row) -> Dict[str, Any]:
        return validate_session_configuration(json.loads(row["configuration_json"]))

    def bootstrap_default(self) -> None:
        with self._connect() as db:
            if db.execute("SELECT 1 FROM sessions LIMIT 1").fetchone():
                return
            session_id = str(uuid.uuid4())
            timestamp = now_ms()
            config = validate_session_configuration(default_session_configuration())
            db.execute(
                "INSERT INTO sessions(id,name,description,configuration_json,version,created_at_ms,updated_at_ms) VALUES(?,?,?,?,1,?,?)",
                (session_id, "Default", "Seeded from the current launcher and bot defaults.", json.dumps(config, separators=(",", ":")), timestamp, timestamp),
            )
            db.execute("INSERT INTO selection(singleton,session_id) VALUES(1,?)", (session_id,))

    def _session_dict(self, row: sqlite3.Row, *, selected_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "id": row["id"], "name": row["name"], "description": row["description"],
            "configuration": self._configuration(row), "version": row["version"],
            "createdAt": row["created_at_ms"], "updatedAt": row["updated_at_ms"],
            "archivedAt": row["archived_at_ms"], "selected": row["id"] == selected_id,
            "runCount": int(row["run_count"]) if "run_count" in row.keys() else 0,
        }

    def selected_id(self, db: Optional[sqlite3.Connection] = None) -> str:
        if db is not None:
            row = db.execute("SELECT session_id FROM selection WHERE singleton=1").fetchone()
            if not row:
                raise SessionConflictError("no session is selected")
            return str(row[0])
        with self._connect() as connection:
            return self.selected_id(connection)

    def list_sessions(self, *, include_archived: bool = False) -> list[Dict[str, Any]]:
        with self._connect() as db:
            selected = self.selected_id(db)
            where = "" if include_archived else "WHERE s.archived_at_ms IS NULL"
            rows = db.execute(f"""SELECT s.*, COUNT(r.id) AS run_count FROM sessions s
                LEFT JOIN runs r ON r.session_id=s.id {where} GROUP BY s.id ORDER BY s.archived_at_ms IS NOT NULL, s.updated_at_ms DESC""").fetchall()
            return [self._session_dict(row, selected_id=selected) for row in rows]

    def get_session(self, session_id: str) -> Dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT s.*, COUNT(r.id) AS run_count FROM sessions s LEFT JOIN runs r ON r.session_id=s.id WHERE s.id=? GROUP BY s.id", (session_id,)).fetchone()
            if not row:
                raise KeyError(session_id)
            return self._session_dict(row, selected_id=self.selected_id(db))

    @staticmethod
    def _validate_identity(name: Any, description: Any) -> tuple[str, str]:
        name = str(name or "").strip()
        description = str(description or "").strip()
        if not 1 <= len(name) <= 80:
            raise ValueError("session name must contain 1 to 80 characters")
        if len(description) > 500:
            raise ValueError("session description must be at most 500 characters")
        return name, description

    def create_session(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        unknown = set(payload) - {"name", "description", "configuration"}
        if unknown:
            raise ValueError("unknown session field(s): " + ", ".join(sorted(unknown)))
        name, description = self._validate_identity(payload.get("name"), payload.get("description"))
        config = validate_session_configuration(payload.get("configuration") or default_session_configuration())
        session_id, timestamp = str(uuid.uuid4()), now_ms()
        try:
            with self._connect() as db:
                db.execute("INSERT INTO sessions VALUES(?,?,?,?,1,?,?,NULL)", (session_id, name, description, json.dumps(config, separators=(",", ":")), timestamp, timestamp))
        except sqlite3.IntegrityError as exc:
            raise SessionConflictError(f"a session named {name!r} already exists") from exc
        return self.get_session(session_id)

    def update_session(self, session_id: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
        unknown = set(payload) - {"name", "description", "configuration", "version"}
        if unknown:
            raise ValueError("unknown session field(s): " + ", ".join(sorted(unknown)))
        with self._connect() as db:
            row = db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if not row:
                raise KeyError(session_id)
            expected = payload.get("version")
            if not isinstance(expected, int) or expected != row["version"]:
                raise SessionConflictError("session configuration was modified; reload before saving")
            name, description = self._validate_identity(payload.get("name", row["name"]), payload.get("description", row["description"]))
            config = validate_session_configuration(payload.get("configuration", self._configuration(row)))
            try:
                db.execute("UPDATE sessions SET name=?,description=?,configuration_json=?,version=version+1,updated_at_ms=? WHERE id=?", (name, description, json.dumps(config, separators=(",", ":")), now_ms(), session_id))
            except sqlite3.IntegrityError as exc:
                raise SessionConflictError(f"a session named {name!r} already exists") from exc
        return self.get_session(session_id)

    def active_run(self, db: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM runs WHERE status IN ('pending','starting','running') ORDER BY created_at_ms DESC LIMIT 1"
        if db is not None:
            row = db.execute(query).fetchone()
            return self._run_dict(row) if row else None
        with self._connect() as connection:
            return self.active_run(connection)

    def select_session(self, session_id: str) -> Dict[str, Any]:
        with self._connect() as db:
            if self.active_run(db):
                raise SessionConflictError("session selection is locked while a run is pending or active")
            row = db.execute("SELECT archived_at_ms FROM sessions WHERE id=?", (session_id,)).fetchone()
            if not row:
                raise KeyError(session_id)
            if row[0] is not None:
                raise SessionConflictError("an archived session cannot be selected")
            db.execute("INSERT INTO selection(singleton,session_id) VALUES(1,?) ON CONFLICT(singleton) DO UPDATE SET session_id=excluded.session_id", (session_id,))
        return self.get_session(session_id)

    def archive_session(self, session_id: str) -> Dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if not row:
                raise KeyError(session_id)
            active = self.active_run(db)
            if active and active["sessionId"] == session_id:
                raise SessionConflictError("the session for an active run cannot be archived")
            remaining = db.execute("SELECT id FROM sessions WHERE archived_at_ms IS NULL AND id<>? ORDER BY updated_at_ms DESC", (session_id,)).fetchall()
            if not remaining:
                raise SessionConflictError("the last active session cannot be archived")
            db.execute("UPDATE sessions SET archived_at_ms=?,updated_at_ms=? WHERE id=?", (now_ms(), now_ms(), session_id))
            if self.selected_id(db) == session_id:
                db.execute("UPDATE selection SET session_id=? WHERE singleton=1", (remaining[0][0],))
        return self.get_session(session_id)

    def restore_session(self, session_id: str) -> Dict[str, Any]:
        with self._connect() as db:
            if not db.execute("SELECT 1 FROM sessions WHERE id=?", (session_id,)).fetchone():
                raise KeyError(session_id)
            db.execute("UPDATE sessions SET archived_at_ms=NULL,updated_at_ms=? WHERE id=?", (now_ms(), session_id))
        return self.get_session(session_id)

    def prepare_run(self) -> Dict[str, Any]:
        with self._connect() as db:
            if self.active_run(db):
                raise SessionConflictError("a launcher run is already pending or active")
            session_id = self.selected_id(db)
            session = db.execute("SELECT * FROM sessions WHERE id=? AND archived_at_ms IS NULL", (session_id,)).fetchone()
            if not session:
                raise SessionConflictError("the selected session is unavailable")
            run_id, timestamp = str(uuid.uuid4()), now_ms()
            artifact = self.artifacts_root / session_id / run_id
            artifact.mkdir(parents=True, exist_ok=False)
            config = self._configuration(session)
            (artifact / "configuration.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
            db.execute("""INSERT INTO runs(id,session_id,session_name,configuration_version,configuration_json,status,created_at_ms,artifact_path)
                VALUES(?,?,?,?,?,'pending',?,?)""", (run_id, session_id, session["name"], session["version"], session["configuration_json"], timestamp, str(artifact)))
        return self.get_run(run_id)

    def reconcile_orphaned_runs(self) -> int:
        """Finalize runs whose launcher exited without writing a terminal status.

        A stranded 'running' row blocks session selection and new runs, so this
        is safe to call whenever the launcher is known to be gone rather than
        only when the next run claims.
        """
        with self._connect() as db:
            return self._interrupt_active_runs(db, now_ms())

    def _interrupt_active_runs(self, db: sqlite3.Connection, timestamp: int) -> int:
        interrupted_runs = db.execute(
            """SELECT id,artifact_path,heartbeat_at_ms,metrics_json FROM runs
               WHERE status IN ('starting','running')"""
        ).fetchall()
        for interrupted in interrupted_runs:
            artifact = Path(interrupted["artifact_path"])
            self._finalize_order_revisions(artifact, timestamp)
            totals, markets, warnings = self._summarize_markouts(
                artifact, active=False,
                as_of_ms=int(interrupted["heartbeat_at_ms"] or timestamp),
            )
            metrics = json.loads(interrupted["metrics_json"] or "{}")
            metrics["markoutsByHorizon"] = totals
            metrics["markoutWarnings"] = warnings
            market_metrics = dict(metrics.get("markets") or {})
            for ticker, values in markets.items():
                market_metrics.setdefault(ticker, {})["markoutsByHorizon"] = values
            metrics["markets"] = market_metrics
            db.execute(
                "UPDATE runs SET metrics_json=? WHERE id=?",
                (json.dumps(metrics, separators=(",", ":")), interrupted["id"]),
            )
        db.execute("UPDATE runs SET status='interrupted',ended_at_ms=COALESCE(heartbeat_at_ms,?),error=COALESCE(error,'launcher exited without finalizing') WHERE status IN ('starting','running')", (timestamp,))
        db.execute(
            """UPDATE screener_runs SET status='interrupted',ended_at_ms=?,
               duration_ms=MAX(0,? - started_at_ms),
               error=COALESCE(error,'fleet run exited without finalizing screener refresh')
               WHERE fleet_run_id IN (SELECT id FROM runs WHERE status='interrupted')
                 AND status='running'""",
            (timestamp, timestamp),
        )
        return len(interrupted_runs)

    def claim_run(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        with self._connect() as db:
            timestamp = now_ms()
            self._interrupt_active_runs(db, timestamp)
            row = db.execute("SELECT * FROM runs WHERE id=? AND status='pending'", (run_id,)).fetchone() if run_id else db.execute("SELECT * FROM runs WHERE status='pending' ORDER BY created_at_ms LIMIT 1").fetchone()
            if not row:
                # Boot-time/autonomous start snapshots the selected session.
                session_id = self.selected_id(db)
                session = db.execute("SELECT * FROM sessions WHERE id=? AND archived_at_ms IS NULL", (session_id,)).fetchone()
                if not session:
                    raise SessionConflictError("the selected session is unavailable")
                claimed_id = str(uuid.uuid4())
                artifact = self.artifacts_root / session_id / claimed_id
                artifact.mkdir(parents=True, exist_ok=False)
                (artifact / "configuration.json").write_text(json.dumps(self._configuration(session), indent=2) + "\n", encoding="utf-8")
                db.execute("""INSERT INTO runs(id,session_id,session_name,configuration_version,configuration_json,status,created_at_ms,started_at_ms,heartbeat_at_ms,artifact_path)
                    VALUES(?,?,?,?,?,'starting',?,?,?,?)""", (claimed_id, session_id, session["name"], session["version"], session["configuration_json"], timestamp, timestamp, timestamp, str(artifact)))
                row = db.execute("SELECT * FROM runs WHERE id=?", (claimed_id,)).fetchone()
            else:
                db.execute("UPDATE runs SET status='starting',started_at_ms=?,heartbeat_at_ms=? WHERE id=?", (timestamp, timestamp, row["id"]))
                row = db.execute("SELECT * FROM runs WHERE id=?", (row["id"],)).fetchone()
            return self._run_dict(row)

    def mark_running(self, run_id: str) -> None:
        with self._connect() as db:
            db.execute("UPDATE runs SET status='running',heartbeat_at_ms=? WHERE id=?", (now_ms(), run_id))

    def record_metrics(self, run_id: str, metrics: Mapping[str, Any], *, sample: bool = True) -> None:
        payload = json.dumps(dict(metrics), separators=(",", ":"))
        timestamp = now_ms()
        with self._connect() as db:
            db.execute("UPDATE runs SET metrics_json=?,heartbeat_at_ms=? WHERE id=?", (payload, timestamp, run_id))
            if sample:
                # The current run row remains authoritative and retains the full
                # per-market map. Historical samples need only aggregate counters.
                sample_fields = (
                    "runtimeMs", "orders", "orderSuccesses", "orderPlacementsAttempted",
                    "orderErrors", "fills", "apiCalls", "apiErrors", "apiByComponent",
                    "feesCents", "realizedCents", "unrealizedCents", "totalCents", "pnlComplete",
                    "markoutsByHorizon",
                )
                sample_payload = json.dumps(
                    {key: metrics[key] for key in sample_fields if key in metrics},
                    separators=(",", ":"),
                )
                db.execute("INSERT INTO metric_samples(run_id,timestamp_ms,payload_json) VALUES(?,?,?)", (run_id, timestamp, sample_payload))

    @staticmethod
    def _screener_run_dict(row: sqlite3.Row) -> Dict[str, Any]:
        try:
            warnings = json.loads(row["warnings_json"] or "[]")
        except (TypeError, ValueError):
            warnings = []
        if not isinstance(warnings, list):
            warnings = []
        return {
            "id": row["id"], "fleetRunId": row["fleet_run_id"],
            "sessionId": row["session_id"], "sessionName": row["session_name"],
            "venue": row["venue"] or "unknown",
            "generationId": row["generation_id"], "reason": row["reason"],
            "status": row["status"], "startedAt": row["started_at_ms"],
            "endedAt": row["ended_at_ms"], "durationMs": row["duration_ms"],
            "configuredLimit": row["configured_limit"], "effectiveLimit": row["effective_limit"],
            "scannedMarkets": row["scanned_markets"], "apiRequests": row["api_requests"],
            "apiErrors": row["api_errors"], "added": row["added_count"],
            "changed": row["changed_count"], "removed": row["removed_count"],
            "inventoryCarried": row["inventory_carried_count"],
            "inventoryUnknown": row["inventory_unknown_count"],
            "warnings": warnings, "error": row["error"],
        }

    def start_screener_run(
        self, fleet_run_id: str, *, reason: str, started_at_ms: int,
        configured_limit: Optional[int], artifact_path: Optional[str] = None,
        venue: str = "unknown",
    ) -> Optional[str]:
        """Create a durable running screener record for a fleet refresh."""
        with self._connect() as db:
            parent = db.execute(
                "SELECT session_id,session_name FROM runs WHERE id=?", (fleet_run_id,)
            ).fetchone()
            if not parent:
                return None
            record_id = str(uuid.uuid4())
            source_key = f"live:{record_id}"
            db.execute(
                """INSERT INTO screener_runs(
                    id,source_key,fleet_run_id,session_id,session_name,venue,reason,status,
                    started_at_ms,configured_limit,artifact_path,created_at_ms
                ) VALUES(?,?,?,?,?,?,?,'running',?,?,?,?)""",
                (
                    record_id, source_key, fleet_run_id, parent["session_id"], parent["session_name"],
                    str(venue or "unknown").strip().lower() or "unknown",
                    str(reason or "scheduled"), int(started_at_ms),
                    int(configured_limit) if configured_limit is not None else None,
                    artifact_path, now_ms(),
                ),
            )
        return record_id

    def finish_screener_run(
        self, record_id: Optional[str], *, status: str, metrics: Mapping[str, Any],
        ended_at_ms: Optional[int] = None,
    ) -> None:
        if not record_id:
            return
        if status not in {"succeeded", "failed", "interrupted"}:
            raise ValueError("invalid screener run status")
        ended = int(ended_at_ms or now_ms())
        values = dict(metrics)
        started = int(values.get("startedAtMs") or ended)
        duration = values.get("durationMs")
        if duration is None:
            duration = max(0, ended - started)
        warnings = values.get("warnings") or []
        if not isinstance(warnings, list):
            warnings = [str(warnings)]
        with self._connect() as db:
            db.execute(
                """UPDATE screener_runs SET status=?,generation_id=?,ended_at_ms=?,duration_ms=?,
                   configured_limit=?,effective_limit=?,scanned_markets=?,api_requests=?,api_errors=?,
                   added_count=?,changed_count=?,removed_count=?,inventory_carried_count=?,
                   inventory_unknown_count=?,warnings_json=?,error=? WHERE id=?""",
                (
                    status, values.get("generationId"), ended, int(duration),
                    values.get("configuredLimit"), values.get("effectiveLimit"),
                    values.get("scannedMarkets"), values.get("apiRequests"), values.get("apiErrors"),
                    int(values.get("added") or 0), int(values.get("changed") or 0),
                    int(values.get("removed") or 0), int(values.get("inventoryCarried") or 0),
                    int(values.get("inventoryUnknown") or 0), json.dumps([str(item) for item in warnings]),
                    values.get("error"), record_id,
                ),
            )

    def interrupt_screener_runs(self, fleet_run_id: str, *, ended_at_ms: Optional[int] = None) -> int:
        ended = int(ended_at_ms or now_ms())
        with self._connect() as db:
            rows = db.execute(
                "SELECT id,started_at_ms FROM screener_runs WHERE fleet_run_id=? AND status='running'",
                (fleet_run_id,),
            ).fetchall()
            for row in rows:
                db.execute(
                    "UPDATE screener_runs SET status='interrupted',ended_at_ms=?,duration_ms=?,error=COALESCE(error,?) WHERE id=?",
                    (ended, max(0, ended - int(row["started_at_ms"])), "fleet run ended before screener refresh completed", row["id"]),
                )
        return len(rows)

    def _backfill_screener_runs(self) -> int:
        """Import recoverable numeric screener snapshots incrementally and idempotently."""
        with self._connect() as db:
            marker = db.execute(
                "SELECT completed_at_ms FROM screener_run_backfill WHERE singleton=1"
            ).fetchone()
            # Live refreshes create durable rows directly, so artifact import is
            # a one-time migration for runs that predate this table. Rewalking
            # every artifact directory on each two-second monitoring poll makes
            # the UI progressively slower as session history grows.
            if marker:
                return 0
            runs = db.execute(
                "SELECT id,session_id,session_name,configuration_json,artifact_path FROM runs"
            ).fetchall()
            imported = 0
            for run in runs:
                artifact = Path(str(run["artifact_path"]))
                screener_dir = artifact / "screener"
                try:
                    paths = sorted(
                        (path for path in screener_dir.rglob("*.json") if path.name != "latest.json"),
                        key=lambda path: path.name,
                    )
                except OSError:
                    continue
                configuration: Dict[str, Any] = {}
                configured_venue = "unknown"
                try:
                    configuration = json.loads(run["configuration_json"] or "{}")
                    screener = configuration.get("screener") or {}
                    if isinstance(screener, dict) and isinstance(screener.get("general"), dict):
                        screener = screener["general"]
                    configured = screener.get("maxMarketsToScan") if isinstance(screener, dict) else None
                    configured_venue = _screener_venue(configuration)
                except (TypeError, ValueError, AttributeError):
                    configured = None
                for path in paths:
                    source_key = str(path.resolve())
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                    except (OSError, ValueError, TypeError):
                        continue
                    if not isinstance(payload, dict):
                        continue
                    last_run = payload.get("lastRun") if isinstance(payload.get("lastRun"), dict) else {}
                    started = last_run.get("startedAtMs", payload.get("lastStartedAtMs"))
                    ended = last_run.get("endedAtMs", payload.get("lastCompletedAtMs"))
                    if not isinstance(started, (int, float)):
                        continue
                    status = str(last_run.get("status") or ("failed" if payload.get("lastError") else "succeeded"))
                    if status == "running":
                        status = "interrupted"
                    changes = payload.get("changes") if isinstance(payload.get("changes"), dict) else {}
                    reason = str(last_run.get("reason") or payload.get("reason") or "unknown")
                    venue = str(
                        last_run.get("venue") or payload.get("venue") or configured_venue
                        or ("polymarket" if reason == "mirror_ready" else "unknown")
                    ).strip().lower() or "unknown"
                    scan = payload.get("scanMetadata") if isinstance(payload.get("scanMetadata"), dict) else {}
                    record_id = str(uuid.uuid5(uuid.NAMESPACE_URL, source_key))
                    duration = last_run.get("durationMs", payload.get("lastDurationMs"))
                    if duration is None and isinstance(ended, (int, float)):
                        duration = max(0, int(ended) - int(started))
                    # A completed live refresh writes a numeric snapshot after
                    # its durable row is finalized. Treat that snapshot as the
                    # same refresh instead of creating a second history row.
                    generation = last_run.get("generationId", payload.get("generationId"))
                    if generation is not None:
                        existing = db.execute(
                            "SELECT 1 FROM screener_runs WHERE fleet_run_id=? AND generation_id=? LIMIT 1",
                            (run["id"], generation),
                        ).fetchone()
                    else:
                        existing = db.execute(
                            "SELECT 1 FROM screener_runs WHERE fleet_run_id=? AND generation_id IS NULL "
                            "AND ABS(started_at_ms-?) <= 5000 LIMIT 1",
                            (run["id"], int(started)),
                        ).fetchone()
                    if existing:
                        continue
                    try:
                        changes_before = db.total_changes
                        db.execute(
                            """INSERT OR IGNORE INTO screener_runs(
                                id,source_key,fleet_run_id,session_id,session_name,venue,generation_id,reason,status,
                                started_at_ms,ended_at_ms,duration_ms,configured_limit,effective_limit,scanned_markets,
                                api_requests,api_errors,added_count,changed_count,removed_count,inventory_carried_count,
                                inventory_unknown_count,warnings_json,error,artifact_path,created_at_ms
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (
                                record_id, source_key, run["id"], run["session_id"], run["session_name"],
                                venue, generation, reason, status,
                                int(started), int(ended) if isinstance(ended, (int, float)) else None,
                                int(duration) if isinstance(duration, (int, float)) else None,
                                last_run.get("configuredLimit", scan.get("requestedLimit", configured)),
                                last_run.get("effectiveLimit", scan.get("effectiveLimit")),
                                last_run.get("scannedMarkets", scan.get("scannedMarkets")),
                                last_run.get("apiRequests"), last_run.get("apiErrors"),
                                int(last_run.get("added", len(changes.get("added") or [])) or 0),
                                int(last_run.get("changed", len(changes.get("changed") or [])) or 0),
                                int(last_run.get("removed", len(changes.get("removed") or [])) or 0),
                                int(last_run.get("inventoryCarried", len(changes.get("inventoryCarried") or [])) or 0),
                                int(last_run.get("inventoryUnknown", len(changes.get("inventoryUnknown") or [])) or 0),
                                json.dumps(last_run.get("warnings") or payload.get("warnings") or []),
                                last_run.get("error", payload.get("lastError")), source_key, now_ms(),
                            ),
                        )
                        imported += int(db.total_changes > changes_before)
                    except sqlite3.Error:
                        continue
            timestamp = now_ms()
            db.execute(
                "INSERT INTO screener_run_backfill(singleton,completed_at_ms) VALUES(1,?) "
                "ON CONFLICT(singleton) DO UPDATE SET completed_at_ms=excluded.completed_at_ms",
                (timestamp,),
            )
            return imported

    def screener_runs(
        self, *, session_id: str = "", limit: int = 100, cursor: str = "",
    ) -> Dict[str, Any]:
        self._backfill_screener_runs()
        limit = min(max(int(limit), 1), 500)
        clauses: list[str] = []
        values: list[Any] = []
        if session_id:
            clauses.append("session_id=?"); values.append(session_id)
        if cursor:
            try:
                cursor_started, cursor_id = cursor.split("|", 1)
                cursor_started_i = int(cursor_started)
            except (ValueError, TypeError):
                raise ValueError("invalid screener history cursor")
            clauses.append("(started_at_ms < ? OR (started_at_ms=? AND id<?))")
            values.extend((cursor_started_i, cursor_started_i, cursor_id))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as db:
            rows = db.execute(
                "SELECT * FROM screener_runs" + where + " ORDER BY started_at_ms DESC,id DESC LIMIT ?",
                values + [limit + 1],
            ).fetchall()
            summary_clauses = [item for item in clauses if not item.startswith("(started_at_ms <")]
            summary_values = values[: len(values) - (3 if cursor else 0)]
            summary_where = " WHERE " + " AND ".join(summary_clauses) if summary_clauses else ""
            summary = db.execute(
                """SELECT COUNT(*) AS total,
                    SUM(status='succeeded') AS succeeded, SUM(status='failed') AS failed,
                    SUM(status='interrupted') AS interrupted, SUM(status='running') AS running,
                    SUM(COALESCE(scanned_markets,0)) AS scanned, SUM(COALESCE(api_requests,0)) AS requests,
                    AVG(CASE WHEN status<>'running' THEN duration_ms END) AS average_duration,
                    SUM(added_count) AS added, SUM(changed_count) AS changed, SUM(removed_count) AS removed
                   FROM screener_runs""" + summary_where,
                summary_values,
            ).fetchone()
        page = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = page[-1]
            next_cursor = f"{last['started_at_ms']}|{last['id']}"
        return {
            "items": [self._screener_run_dict(row) for row in page],
            "nextCursor": next_cursor,
            "summary": {
                "totalRuns": int(summary["total"] or 0), "succeeded": int(summary["succeeded"] or 0),
                "failed": int(summary["failed"] or 0), "interrupted": int(summary["interrupted"] or 0),
                "running": int(summary["running"] or 0), "scannedMarkets": int(summary["scanned"] or 0),
                "apiRequests": int(summary["requests"] or 0),
                "averageDurationMs": float(summary["average_duration"]) if summary["average_duration"] is not None else None,
                "added": int(summary["added"] or 0), "changed": int(summary["changed"] or 0),
                "removed": int(summary["removed"] or 0),
            },
        }

    def finish_run(self, run_id: str, status: str, *, metrics: Optional[Mapping[str, Any]] = None, error: Optional[str] = None) -> None:
        if status not in {"stopped", "failed", "shutdown_failed", "interrupted"}:
            raise ValueError("invalid final run status")
        final_metrics = dict(metrics or {})
        finished_at = now_ms()
        with self._connect() as db:
            row = db.execute("SELECT artifact_path FROM runs WHERE id=?", (run_id,)).fetchone()
            # Commit the terminal state before best-effort artifact processing.
            # A large run may contain hundreds of telemetry databases; failure
            # to summarize one must never leave the run blocking future starts.
            db.execute(
                "UPDATE runs SET status=?,ended_at_ms=?,heartbeat_at_ms=?,error=? WHERE id=?",
                (status, finished_at, finished_at, error, run_id),
            )
            db.execute(
                """UPDATE screener_runs SET status='interrupted',ended_at_ms=?,
                   duration_ms=MAX(0,? - started_at_ms),
                   error=COALESCE(error,'fleet run ended before screener refresh completed')
                   WHERE fleet_run_id=? AND status='running'""",
                (finished_at, finished_at, run_id),
            )
        if row:
            self._finalize_order_revisions(Path(row["artifact_path"]), finished_at)
            pnl_path = Path(row["artifact_path"]) / "pnl_tracker.jsonl"
            if pnl_path.exists():
                from portfolio.pnl_core import load_fills, summarize_pnl
                fills, warnings = load_fills(pnl_path)
                pnl = summarize_pnl(fills)
                totals = pnl["totals"]
                complete = all(not item["netPosition"] or item["lastFairCents"] is not None for item in pnl["tickers"])
                final_metrics.update(
                    fills=int(totals["fills"]), feesCents=totals["feesCents"],
                    realizedCents=totals["realizedCents"], unrealizedCents=totals["unrealizedCents"],
                    totalCents=totals["totalCents"], pnlComplete=complete,
                    pnlWarnings=warnings + ([] if complete else ["one or more ending positions have no recorded fair-value mark"]),
                )
                markets = dict(final_metrics.get("markets") or {})
                for item in pnl["tickers"]:
                    markets.setdefault(item["ticker"], {}).update(
                        fills=item["fills"], realizedCents=item["realizedCents"],
                        unrealizedCents=item["unrealizedCents"], totalCents=item["totalCents"],
                        netPosition=item["netPosition"], lastFairCents=item["lastFairCents"],
                    )
                final_metrics["markets"] = markets
            markouts, market_markouts, markout_warnings = self._summarize_markouts(
                Path(row["artifact_path"]), active=False, as_of_ms=finished_at
            )
            final_metrics["markoutsByHorizon"] = markouts
            final_metrics["markoutWarnings"] = markout_warnings
            markets = dict(final_metrics.get("markets") or {})
            for ticker, values in market_markouts.items():
                markets.setdefault(ticker, {})["markoutsByHorizon"] = values
            final_metrics["markets"] = markets
        if final_metrics:
            self.record_metrics(run_id, final_metrics, sample=False)

    @staticmethod
    def _finalize_order_revisions(artifact_path: Path, ended_at_ms: int) -> None:
        paths = list(artifact_path.glob("markets/*/telemetry.sqlite3"))
        paths.extend(artifact_path.glob("shards/*/telemetry.sqlite3"))
        for path in dict.fromkeys(paths):
            try:
                # A sqlite3 connection's context manager controls only the
                # transaction; it does not close the connection. Explicitly
                # close each database so large runs cannot exhaust RLIMIT_NOFILE.
                with closing(sqlite3.connect(path, timeout=1)) as db:
                    with db:
                        if not db.execute(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='order_revisions'"
                        ).fetchone():
                            continue
                        db.execute(
                            """UPDATE order_revisions SET ended_state='Unknown',ended_at_ms=?
                               WHERE ended_at_ms IS NULL AND ended_state='Resting'""",
                            (int(ended_at_ms),),
                        )
            except sqlite3.Error:
                continue

    @staticmethod
    def _telemetry_paths(artifact_path: Path) -> list[Path]:
        paths = list(artifact_path.glob("markets/*/telemetry.sqlite3"))
        paths.extend(artifact_path.glob("shards/*/telemetry.sqlite3"))
        unique: dict[str, Path] = {}
        for path in paths:
            try:
                unique[str(path.resolve())] = path.resolve()
            except OSError:
                unique[str(path)] = path
        return list(unique.values())

    @classmethod
    def _summarize_markouts(
        cls,
        artifact_path: Path,
        *,
        active: bool,
        as_of_ms: int,
    ) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Dict[str, Any]]], list[str]]:
        fills: Dict[tuple[str, str], Dict[str, Any]] = {}
        markouts: Dict[tuple[str, str, int], tuple[tuple[int, int], Dict[str, Any]]] = {}
        warnings: list[str] = []
        for path in cls._telemetry_paths(artifact_path):
            try:
                with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)) as db:
                    db.row_factory = sqlite3.Row
                    tables = {str(row[0]) for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )}
                    if "fills" not in tables:
                        continue
                    for row in db.execute(
                        "SELECT fill_key,ts_ms,ticker,side,price_units,size_units,fee_units FROM fills"
                    ):
                        key = (str(row["ticker"]), str(row["fill_key"]))
                        fills[key] = dict(row)
                    if "markouts" not in tables:
                        continue
                    for row in db.execute(
                        """SELECT id,fill_key,ts_ms,ticker,side,horizon_ms,fill_price_units,
                                  future_mid_yes_units FROM markouts"""
                    ):
                        key = (str(row["ticker"]), str(row["fill_key"]), int(row["horizon_ms"]))
                        revision = (int(row["ts_ms"]), int(row["id"]))
                        if key not in markouts or revision > markouts[key][0]:
                            markouts[key] = (revision, dict(row))
            except (sqlite3.Error, OSError) as exc:
                warnings.append(f"Could not read markouts from {path}: {exc}")

        tickers = sorted({ticker for ticker, _ in fills})
        market_results: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for ticker in tickers:
            ticker_fills = {key: value for key, value in fills.items() if key[0] == ticker}
            horizon_results: Dict[str, Dict[str, Any]] = {}
            for horizon_ms in MARKOUT_HORIZONS_MS:
                aggregate = empty_markout_aggregate(horizon_ms)
                pending = 0
                for (fill_ticker, fill_key), fill in ticker_fills.items():
                    markout_entry = markouts.get((fill_ticker, fill_key, horizon_ms))
                    if markout_entry is None:
                        if active and int(fill["ts_ms"]) + horizon_ms > as_of_ms:
                            pending += 1
                        continue
                    markout = markout_entry[1]
                    if (
                        markout.get("future_mid_yes_units") is None
                        or fill.get("price_units") is None
                        or fill.get("size_units") is None
                        or fill.get("fee_units") is None
                    ):
                        continue
                    fill_price = int(fill["price_units"])
                    future_mid_yes = int(markout["future_mid_yes_units"])
                    signed = (
                        future_mid_yes - fill_price
                        if str(fill["side"]) == "yes"
                        else 10_000 - future_mid_yes - fill_price
                    )
                    add_observation(
                        aggregate,
                        signed_price_units=signed,
                        size_units=int(fill["size_units"]),
                        fee_units=int(fill["fee_units"]),
                    )
                horizon_results[str(horizon_ms)] = finalize_aggregate(
                    aggregate,
                    total_fill_count=len(ticker_fills),
                    pending_fill_count=pending,
                )
            market_results[ticker] = horizon_results
        totals = combine_markout_maps(market_results.values())
        for horizon_ms in MARKOUT_HORIZONS_MS:
            totals.setdefault(str(horizon_ms), empty_markout_aggregate(horizon_ms))
        return totals, market_results, warnings

    def backfill_markouts(self, *, include_active: bool = False) -> Dict[str, Any]:
        statuses = "" if include_active else "WHERE status NOT IN ('pending','starting','running')"
        with self._connect() as db:
            rows = db.execute(f"SELECT id,status,artifact_path,ended_at_ms,metrics_json FROM runs {statuses}").fetchall()
        report = {"runsProcessed": 0, "runsWithWarnings": 0, "runs": [], "warnings": []}
        for row in rows:
            active = str(row["status"]) in ACTIVE_RUN_STATES
            totals, markets, warnings = self._summarize_markouts(
                Path(row["artifact_path"]),
                active=active,
                as_of_ms=now_ms() if active else int(row["ended_at_ms"] or now_ms()),
            )
            metrics = json.loads(row["metrics_json"] or "{}")
            metrics["markoutsByHorizon"] = totals
            metrics["markoutWarnings"] = warnings
            market_metrics = dict(metrics.get("markets") or {})
            for ticker, values in markets.items():
                market_metrics.setdefault(ticker, {})["markoutsByHorizon"] = values
            metrics["markets"] = market_metrics
            self.record_metrics(str(row["id"]), metrics, sample=False)
            report["runsProcessed"] += 1
            report["runs"].append({
                "runId": str(row["id"]),
                "status": str(row["status"]),
                "horizons": totals,
                "warnings": warnings,
            })
            if warnings:
                report["runsWithWarnings"] += 1
                report["warnings"].extend(f"{row['id']}: {warning}" for warning in warnings)
        return report

    def fail_pending_run(self, run_id: str, error: str) -> None:
        with self._connect() as db:
            db.execute("UPDATE runs SET status='failed',ended_at_ms=?,error=? WHERE id=? AND status='pending'", (now_ms(), error, run_id))

    @staticmethod
    def _run_dict(row: sqlite3.Row) -> Dict[str, Any]:
        metrics = json.loads(row["metrics_json"]) if row["metrics_json"] else {}
        return {
            "id": row["id"], "sessionId": row["session_id"], "sessionName": row["session_name"],
            "configurationVersion": row["configuration_version"], "configuration": json.loads(row["configuration_json"]),
            "status": row["status"], "createdAt": row["created_at_ms"], "startedAt": row["started_at_ms"],
            "endedAt": row["ended_at_ms"], "heartbeatAt": row["heartbeat_at_ms"], "artifactPath": row["artifact_path"],
            "metrics": metrics, "error": row["error"],
        }

    def get_run(self, run_id: str, *, include_artifact_bytes: bool = True) -> Dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if not row:
                raise KeyError(run_id)
            result = self._run_dict(row)
        if include_artifact_bytes:
            result["artifactBytes"] = self._artifact_size(Path(result["artifactPath"]))
        return result

    def list_runs(self, *, session_id: str = "", status: str = "", from_ms: Optional[int] = None, to_ms: Optional[int] = None, include_artifact_bytes: bool = True) -> list[Dict[str, Any]]:
        clauses, values = [], []
        if session_id:
            clauses.append("session_id=?"); values.append(session_id)
        if status:
            clauses.append("status=?"); values.append(status)
        if from_ms is not None:
            clauses.append("created_at_ms>=?"); values.append(int(from_ms))
        if to_ms is not None:
            clauses.append("created_at_ms<=?"); values.append(int(to_ms))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as db:
            rows = db.execute("SELECT * FROM runs" + where + " ORDER BY created_at_ms DESC", values).fetchall()
        items = [self._run_dict(row) for row in rows]
        if include_artifact_bytes:
            for item in items:
                item["artifactBytes"] = self._artifact_size(Path(item["artifactPath"]))
        return items

    @staticmethod
    def _artifact_size(path: Path) -> int:
        total = 0
        try:
            for root, _, files in os.walk(path):
                for name in files:
                    try:
                        total += (Path(root) / name).stat().st_size
                    except OSError:
                        pass
        except OSError:
            pass
        return total

    def metrics(self, **filters: Any) -> Dict[str, Any]:
        include_market_metrics = bool(filters.pop("include_market_metrics", True))
        runs = self.list_runs(**filters)
        totals: Dict[str, Any] = {
            "timesRun": len(runs), "runtimeMs": 0, "orders": 0, "fills": 0,
            "apiCalls": 0, "apiErrors": 0, "realizedCents": 0.0, "unrealizedCents": 0.0,
            "totalCents": 0.0, "pnlComplete": True, "outcomes": {}, "apiByComponent": {},
            "markoutsByHorizon": {},
        }
        run_markouts: list[Mapping[str, Any]] = []
        for run in runs:
            metric = run["metrics"] or {}
            duration = int(metric.get("runtimeMs") or ((run["endedAt"] or now_ms()) - (run["startedAt"] or run["createdAt"])))
            totals["runtimeMs"] += max(0, duration)
            totals["orders"] += int(metric.get("orders") or 0)
            totals["fills"] += int(metric.get("fills") or 0)
            totals["apiCalls"] += int(metric.get("apiCalls") or 0)
            totals["apiErrors"] += int(metric.get("apiErrors") or 0)
            totals["realizedCents"] += float(metric.get("realizedCents") or 0)
            totals["unrealizedCents"] += float(metric.get("unrealizedCents") or 0)
            totals["totalCents"] += float(metric.get("totalCents") or 0)
            totals["pnlComplete"] = totals["pnlComplete"] and bool(metric.get("pnlComplete", True))
            run_markouts.append(metric.get("markoutsByHorizon") or {})
            totals["outcomes"][run["status"]] = totals["outcomes"].get(run["status"], 0) + 1
            for component, count in (metric.get("apiByComponent") or {}).items():
                totals["apiByComponent"][component] = totals["apiByComponent"].get(component, 0) + int(count)
        minutes = totals["runtimeMs"] / 60_000
        totals["ordersPerMinute"] = round(totals["orders"] / minutes, 4) if minutes else 0.0
        totals["fillsPerMinute"] = round(totals["fills"] / minutes, 4) if minutes else 0.0
        for key in ("realizedCents", "unrealizedCents", "totalCents"):
            totals[key] = round(totals[key], 4)
        totals["markoutsByHorizon"] = combine_markout_maps(run_markouts)
        for horizon_ms in MARKOUT_HORIZONS_MS:
            totals["markoutsByHorizon"].setdefault(
                str(horizon_ms), empty_markout_aggregate(horizon_ms)
            )
        if not include_market_metrics:
            for run in runs:
                metrics = dict(run.get("metrics") or {})
                metrics.pop("markets", None)
                run["metrics"] = metrics
        return {"generatedAt": now_ms(), "summary": totals, "runs": runs}

    @staticmethod
    def _market_counter_signatures(metrics: Mapping[str, Any]) -> Dict[str, tuple[int, ...]]:
        signatures: Dict[str, tuple[int, ...]] = {}
        markets = metrics.get("markets") if isinstance(metrics, Mapping) else None
        if not isinstance(markets, Mapping):
            return signatures
        for ticker, value in markets.items():
            if not ticker or not isinstance(value, Mapping):
                continue
            markouts = value.get("markoutsByHorizon") or {}
            signatures[str(ticker)] = (
                int(value.get("orders") or 0),
                int(value.get("orderPlacementsAttempted") or 0),
                int(value.get("fills") or 0),
                *(int((markouts.get(str(horizon)) or {}).get("coveredFillCount") or 0)
                  for horizon in MARKOUT_HORIZONS_MS),
            )
        return signatures

    @classmethod
    def _activity_revision(cls, metrics: Mapping[str, Any]) -> str:
        signatures = cls._market_counter_signatures(metrics)
        compact = [[ticker, *signatures[ticker]] for ticker in sorted(signatures)]
        return hashlib.blake2s(
            json.dumps(compact, separators=(",", ":")).encode("utf-8"), digest_size=8
        ).hexdigest()

    def metrics_heartbeat(self) -> Dict[str, Any]:
        # active_run performs exactly one indexed row query. In particular this
        # path never enumerates artifacts or opens per-market telemetry stores.
        active = self.active_run()
        generated = now_ms()
        metric = (active or {}).get("metrics") or {}
        source = {
            "available": True,
            "updatedAt": active.get("heartbeatAt") if active else generated,
            "stale": bool(active and generated - int(active.get("heartbeatAt") or 0) > HEARTBEAT_STALE_AFTER_MS),
        }
        summary_fields = (
            "runtimeMs", "orders", "fills", "totalCents", "realizedCents",
            "unrealizedCents", "apiCalls", "apiErrors", "pnlComplete",
            "markoutsByHorizon",
        )
        return {
            "generatedAt": generated,
            "source": source,
            "activeRun": (
                {
                    "id": active["id"],
                    "sessionId": active["sessionId"],
                    "status": active["status"],
                    "heartbeatAt": active["heartbeatAt"],
                    "summary": {key: metric.get(key) for key in summary_fields if key in metric},
                    "activityRevision": self._activity_revision(metric),
                    "source": source,
                }
                if active else None
            ),
        }

    @staticmethod
    def _artifact_roots(artifact: Path) -> list[Path]:
        """Return the legacy root and any per-venue artifact roots.

        Older runs wrote ``markets`` and ``shards`` directly below the run
        artifact.  Multi-venue runs put those directories below a venue
        directory (for example ``kalshi/shards``).  Keeping this discovery
        local to the run artifact preserves the existing path-safety checks
        while allowing both formats to be read.
        """
        root = Path(artifact).resolve()
        roots = [root]
        try:
            children = sorted(root.iterdir(), key=lambda path: path.name)
        except OSError:
            children = []
        for child in children:
            if child.is_dir() and ((child / "markets").is_dir() or (child / "shards").is_dir()):
                roots.append(child.resolve())
        return roots

    @staticmethod
    def _read_screener_titles(artifact: Path) -> Dict[str, str]:
        titles: Dict[str, str] = {}
        screener_root = artifact / "screener"
        try:
            paths = sorted(screener_root.rglob("latest.json"))
        except OSError:
            paths = []
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            for item in payload.get("picks") or []:
                if isinstance(item, dict) and item.get("marketId"):
                    market_id = str(item["marketId"])
                    titles[market_id] = str(item.get("title") or market_id)
        return titles

    @staticmethod
    def _market_database(artifact: Path, ticker: str) -> Path:
        if not ticker or ticker in {".", ".."} or "/" in ticker or "\\" in ticker:
            raise KeyError(ticker)
        for root in SessionStore._artifact_roots(artifact):
            markets_root = (root / "markets").resolve()

            # A single-venue run stores markets/<ticker>.  Check this direct
            # path before scanning the market directory, which keeps legacy
            # runs with many markets inexpensive.
            candidate = (markets_root / ticker / "telemetry.sqlite3").resolve()
            try:
                candidate.relative_to(markets_root)
            except ValueError as exc:
                raise KeyError(ticker) from exc
            if candidate.exists():
                return candidate

            worker_id = ""
            try:
                manifest = json.loads((root / "fleet_manifest.json").read_text(encoding="utf-8"))
                for mapping_name in ("activeTickerToShard", "tickerToShard"):
                    worker_id = str((manifest.get(mapping_name) or {}).get(ticker) or "")
                    if worker_id:
                        break
            except (OSError, ValueError, TypeError):
                pass
            shard_root = (root / "shards").resolve()
            if worker_id and "/" not in worker_id and "\\" not in worker_id:
                shard = (shard_root / worker_id / "telemetry.sqlite3").resolve()
                try:
                    shard.relative_to(shard_root)
                except ValueError:
                    shard = None
                if shard is not None and shard.exists():
                    return shard

            # A multi-venue run can also store per-market databases at
            # markets/<venue>/<ticker>.  This fallback is intentionally after
            # the manifest lookup so a large legacy markets directory is not
            # scanned once per ticker when shard telemetry is available.
            market_directories: list[Path] = []
            try:
                market_directories = [
                    child for child in markets_root.iterdir() if child.is_dir()
                ]
            except OSError:
                pass
            for market_group in market_directories:
                nested = (market_group / ticker / "telemetry.sqlite3").resolve()
                try:
                    nested.relative_to(markets_root)
                except ValueError:
                    continue
                if nested.exists():
                    return nested

            # Older manifests were overwritten on each screener refresh and
            # therefore forgot removed markets.  Each immutable market settings
            # snapshot retains the exact shard database path, so use it as the
            # compatibility index for those runs.  Search both direct and
            # venue-nested settings directories.
            settings_candidates = [markets_root / ticker / "settings.json"]
            settings_candidates.extend(
                child / ticker / "settings.json" for child in market_directories
            )
            for settings_path in settings_candidates:
                try:
                    settings = json.loads(settings_path.read_text(encoding="utf-8"))
                    persisted = Path(str(settings.get("telemetry_sqlite_path") or "")).resolve()
                    persisted.relative_to(shard_root)
                    if persisted.exists():
                        return persisted
                except (OSError, ValueError, TypeError):
                    continue
        raise KeyError(ticker)

    def _artifact_for_run(self, run: Mapping[str, Any]) -> Path:
        artifact = Path(str(run["artifactPath"])).resolve()
        expected = (self.artifacts_root / str(run["sessionId"]) / str(run["id"])).resolve()
        if artifact != expected:
            raise KeyError(run["id"])
        return artifact

    @staticmethod
    def _table_exists(db: sqlite3.Connection, table: str) -> bool:
        return db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None

    @staticmethod
    def _value_units(count_units: int, price_units: Optional[float]) -> Optional[int]:
        if price_units is None:
            return None
        return int(round(int(count_units) * float(price_units) / 100))

    @staticmethod
    def _telemetry_updated_at(path: Path) -> Optional[int]:
        modified: list[int] = []
        for candidate in (path, Path(f"{path}-wal")):
            try:
                modified.append(int(candidate.stat().st_mtime * 1000))
            except OSError:
                continue
        return max(modified) if modified else None

    @staticmethod
    def _consume_proportionally(
        amount: Optional[int], quantity: int, remaining_quantity: int
    ) -> tuple[Optional[int], Optional[int]]:
        """Consume a proportional integer amount while preserving every unit."""
        if amount is None:
            return None, None
        if quantity >= remaining_quantity:
            return amount, 0
        allocated = int(amount) * int(quantity) // int(remaining_quantity)
        return allocated, int(amount) - allocated

    @classmethod
    def _fill_pnl_breakdown(
        cls,
        fills: Iterable[Mapping[str, Any]],
        *,
        latest_bids: Mapping[str, Optional[int]],
    ) -> tuple[Dict[int, Dict[str, Optional[int]]], list[str]]:
        """Match YES/NO lots FIFO and attribute realized P&L to closing fills."""
        queues: Dict[str, Any] = {"yes": deque(), "no": deque()}
        results: Dict[int, Dict[str, Any]] = {}
        warnings: list[str] = []
        realized_incomplete = False
        unrealized_mark_incomplete = False
        unrealized_cost_incomplete = False

        for row in fills:
            row_id = int(row["id"])
            side = str(row["side"] or "").lower()
            quantity = max(0, int(row["size_units"] or 0))
            result: Dict[str, Any] = {
                "contractsUnits": quantity,
                "openContractsUnits": 0,
                "realizedPnlUnits": 0,
                "_lot": None,
            }
            results[row_id] = result
            if side not in {"yes", "no"}:
                result["realizedPnlUnits"] = None
                result["unrealizedPnlUnits"] = None
                result["fillPnlUnits"] = None
                warnings.append("One or more fills have an unsupported side; fill P&L is unavailable.")
                continue

            price = int(row["price_units"]) if row["price_units"] is not None else None
            current_notional = cls._value_units(quantity, price)
            current_fee = int(row["fee_units"]) if row["fee_units"] is not None else None
            current_remaining = quantity
            opposite = "no" if side == "yes" else "yes"
            realized = 0
            realized_known = True

            while current_remaining and queues[opposite]:
                lot = queues[opposite][0]
                matched = min(current_remaining, int(lot["remainingQuantity"]))
                entry_notional, lot["remainingNotional"] = cls._consume_proportionally(
                    lot["remainingNotional"], matched, int(lot["remainingQuantity"])
                )
                entry_fee, lot["remainingFee"] = cls._consume_proportionally(
                    lot["remainingFee"], matched, int(lot["remainingQuantity"])
                )
                exit_notional, current_notional = cls._consume_proportionally(
                    current_notional, matched, current_remaining
                )
                exit_fee, current_fee = cls._consume_proportionally(
                    current_fee, matched, current_remaining
                )
                payout = cls._value_units(matched, 10_000)
                if None in {entry_notional, entry_fee, exit_notional, exit_fee, payout}:
                    realized_known = False
                    realized_incomplete = True
                elif realized_known:
                    realized += int(payout) - int(entry_notional) - int(entry_fee) - int(exit_notional) - int(exit_fee)

                lot["remainingQuantity"] -= matched
                lot["result"]["openContractsUnits"] -= matched
                current_remaining -= matched
                if lot["remainingQuantity"] == 0:
                    queues[opposite].popleft()

            result["realizedPnlUnits"] = realized if realized_known else None
            if current_remaining:
                result["openContractsUnits"] = current_remaining
                lot = {
                    "remainingQuantity": current_remaining,
                    "remainingNotional": current_notional,
                    "remainingFee": current_fee,
                    "result": result,
                    "side": side,
                }
                result["_lot"] = lot
                queues[side].append(lot)

        for result in results.values():
            open_quantity = int(result["openContractsUnits"])
            result["matchedContractsUnits"] = int(result["contractsUnits"]) - open_quantity
            if "unrealizedPnlUnits" in result:
                result.pop("_lot", None)
                result.pop("contractsUnits", None)
                continue
            if not open_quantity:
                unrealized: Optional[int] = 0
            else:
                lot = result["_lot"]
                mark = latest_bids.get(str(lot["side"]))
                market_value = cls._value_units(open_quantity, mark)
                remaining_notional = lot["remainingNotional"]
                remaining_fee = lot["remainingFee"]
                if market_value is None:
                    unrealized_mark_incomplete = True
                if remaining_notional is None or remaining_fee is None:
                    unrealized_cost_incomplete = True
                if market_value is None or remaining_notional is None or remaining_fee is None:
                    unrealized = None
                else:
                    unrealized = int(market_value) - int(remaining_notional) - int(remaining_fee)
            result["unrealizedPnlUnits"] = unrealized
            realized_value = result["realizedPnlUnits"]
            result["fillPnlUnits"] = (
                int(realized_value) + int(unrealized)
                if realized_value is not None and unrealized is not None else None
            )
            result.pop("_lot", None)
            result.pop("contractsUnits", None)

        if realized_incomplete:
            warnings.append(
                "Realized fill P&L is unavailable for one or more matched fills because a price or fee was not persisted."
            )
        if unrealized_mark_incomplete:
            warnings.append(
                "Unrealized fill P&L is unavailable for one or more open fills because no latest same-side bid was persisted."
            )
        if unrealized_cost_incomplete:
            warnings.append(
                "Unrealized fill P&L is unavailable for one or more open fills because a price or fee was not persisted."
            )
        return results, list(dict.fromkeys(warnings))

    def _market_summary(
        self,
        path: Path,
        *,
        ticker: str,
        fallback_title: str,
        fallback_url: Optional[str],
        legacy_metric: Mapping[str, Any],
    ) -> Dict[str, Any]:
        uri = f"file:{path}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
            db.row_factory = sqlite3.Row
            has_orders = self._table_exists(db, "order_revisions")
            has_metadata = self._table_exists(db, "market_metadata")
            has_shard_metadata = self._table_exists(db, "shard_market_metadata")
            order_columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(order_revisions)")
            } if has_orders else set()
            shard_orders = "ticker" in order_columns
            fill = db.execute(
                """SELECT COUNT(*) AS fill_count,
                    COALESCE(SUM(CASE WHEN side='yes' THEN size_units ELSE 0 END),0) AS yes_units,
                    COALESCE(SUM(CASE WHEN side='no' THEN size_units ELSE 0 END),0) AS no_units,
                    COALESCE(SUM(CASE WHEN side='yes' AND price_units IS NOT NULL THEN price_units*size_units ELSE 0 END),0) AS yes_weighted,
                    COALESCE(SUM(CASE WHEN side='no' AND price_units IS NOT NULL THEN price_units*size_units ELSE 0 END),0) AS no_weighted,
                    COALESCE(SUM(CASE WHEN side='yes' AND price_units IS NULL THEN 1 ELSE 0 END),0) AS yes_missing_prices,
                    COALESCE(SUM(CASE WHEN side='no' AND price_units IS NULL THEN 1 ELSE 0 END),0) AS no_missing_prices,
                    COALESCE(SUM(CASE WHEN fee_units IS NULL THEN 1 ELSE 0 END),0) AS missing_fees,
                    COALESCE(SUM(fee_units),0) AS fee_units,
                    MIN(ts_ms) AS first_fill_at_ms,MAX(ts_ms) AS last_fill_at_ms
                    FROM fills WHERE ticker=?""",
                (ticker,),
            ).fetchone()
            order_count = int(db.execute(
                "SELECT COUNT(*) FROM order_revisions WHERE ticker=?" if shard_orders else "SELECT COUNT(*) FROM order_revisions",
                (ticker,) if shard_orders else (),
            ).fetchone()[0]) if has_orders else int(legacy_metric.get("orderPlacementsAttempted") or 0)
            order_sides = {
                str(row[0]) for row in db.execute(
                    "SELECT DISTINCT side FROM order_revisions WHERE ticker=?" if shard_orders else "SELECT DISTINCT side FROM order_revisions",
                    (ticker,) if shard_orders else (),
                )
            } if has_orders else set()
            metadata = (
                db.execute("SELECT * FROM shard_market_metadata WHERE ticker=?", (ticker,)).fetchone()
                if has_shard_metadata else
                db.execute("SELECT * FROM market_metadata WHERE singleton=1").fetchone() if has_metadata else None
            )
        yes_units, no_units = int(fill["yes_units"]), int(fill["no_units"])
        yes_prices_complete = not int(fill["yes_missing_prices"])
        no_prices_complete = not int(fill["no_missing_prices"])
        fees_complete = not int(fill["missing_fees"])
        yes_average = int(round(int(fill["yes_weighted"]) / yes_units)) if yes_units and yes_prices_complete else None
        no_average = int(round(int(fill["no_weighted"]) / no_units)) if no_units and no_prices_complete else None
        yes_cost = int(round(int(fill["yes_weighted"]) / 100)) if yes_prices_complete else None
        no_cost = int(round(int(fill["no_weighted"]) / 100)) if no_prices_complete else None
        fees = int(fill["fee_units"] or 0)
        total_cost = yes_cost + no_cost + fees if yes_cost is not None and no_cost is not None and fees_complete else None
        matched = min(yes_units, no_units)
        matched_payout = self._value_units(matched, 10_000) or 0
        matched_prices_complete = matched == 0 or (yes_average is not None and no_average is not None)
        matched_cost = (
            (self._value_units(matched, yes_average) or 0) + (self._value_units(matched, no_average) or 0)
            if matched_prices_complete else None
        )
        realized = matched_payout - matched_cost - fees if matched_cost is not None and fees_complete else None
        sides = set(order_sides)
        if yes_units: sides.add("yes")
        if no_units: sides.add("no")
        side = "BOTH" if {"yes", "no"}.issubset(sides) else "YES" if "yes" in sides else "NO" if "no" in sides else "—"
        warnings = [] if has_orders else ["Detailed order revisions were not persisted for this legacy run."]
        if not yes_prices_complete or not no_prices_complete:
            warnings.append("One or more fill prices are unavailable; affected cost and realized values are unavailable.")
        if not fees_complete:
            warnings.append("One or more fill fees are unavailable; total cost and realized values are unavailable.")
        description = str((metadata["title"] if metadata else "") or fallback_title or ticker)
        market_url = str((metadata["market_url"] if metadata else "") or fallback_url or "") or None
        description_complete = description != ticker
        if not description_complete:
            warnings.append("Market description was not persisted for this run.")
        if market_url is None:
            warnings.append("Venue market link is unavailable from persisted data.")
        return {
            "ticker": ticker,
            "description": description,
            "marketUrl": market_url,
            "side": side,
            "yesContractsUnits": yes_units,
            "noContractsUnits": no_units,
            "yesAverageCostPriceUnits": yes_average,
            "noAverageCostPriceUnits": no_average,
            "totalCostUnits": total_cost,
            "realizedPnlUnits": realized,
            "realizedReturnBps": int(round(realized * 10_000 / total_cost)) if realized is not None and total_cost else None,
            "markoutsByHorizon": dict(legacy_metric.get("markoutsByHorizon") or {}),
            "fillCount": int(fill["fill_count"]),
            "orderCount": order_count,
            "firstFillAtMs": fill["first_fill_at_ms"],
            "lastFillAtMs": fill["last_fill_at_ms"],
            "coverage": {
                "fillsComplete": True, "ordersComplete": has_orders,
                "fillPricesComplete": yes_prices_complete and no_prices_complete,
                "fillFeesComplete": fees_complete,
                "descriptionComplete": description_complete,
                "marketLinkAvailable": market_url is not None,
            },
            "warnings": warnings,
        }

    def run_markets(self, run_id: str, *, market_links: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
        with self._market_cache_lock:
            completed_cache = self._market_cache.get(run_id)
            if completed_cache and completed_cache["completed"]:
                self._market_cache.move_to_end(run_id)
                result = deepcopy(completed_cache["response"])
                result["generatedAt"] = now_ms()
                return result
        run = self.get_run(run_id, include_artifact_bytes=False)
        artifact = self._artifact_for_run(run)
        metrics = run.get("metrics") or {}
        legacy = metrics.get("markets") if isinstance(metrics, Mapping) else {}
        legacy = legacy if isinstance(legacy, Mapping) else {}
        signatures = self._market_counter_signatures(metrics)
        revision = self._activity_revision(metrics)
        completed = run["status"] not in ACTIVE_RUN_STATES

        # A lock deliberately covers recomputation. It gives concurrent callers a
        # single-flight cache and prevents identical SSE clients from opening the
        # same telemetry database in parallel.
        with self._market_cache_lock:
            cached = self._market_cache.get(run_id)
            if cached and completed:
                cached["completed"] = True
                cached["response"]["source"]["updatedAt"] = run.get("heartbeatAt")
                cached["response"]["source"]["stale"] = False
            cached_error = bool(((cached or {}).get("response") or {}).get("source", {}).get("error"))
            if cached and (cached["completed"] or (cached["revision"] == revision and not cached_error)):
                self._market_cache.move_to_end(run_id)
                result = deepcopy(cached["response"])
                result["generatedAt"] = now_ms()
                result["source"]["updatedAt"] = run.get("heartbeatAt")
                return result

            titles = self._read_screener_titles(artifact)
            links = market_links or {}
            cached_items = dict(cached.get("items") or {}) if cached else {}
            cached_signatures = dict(cached.get("signatures") or {}) if cached else {}
            warnings: list[str] = []
            failed: list[str] = []
            failed_errors: list[str] = []

            # Counters are trusted for current-format runs. A missing counter map
            # is a legacy coverage gap, scanned only on the first cached access.
            # The metrics tab is a run-market view, not only an activity view.
            # A newly admitted market legitimately has zero orders and fills;
            # retaining every counter signature lets it appear immediately and
            # then refresh incrementally when activity starts.
            candidates = set(signatures)
            if not signatures:
                for root in self._artifact_roots(artifact):
                    legacy_paths = sorted((root / "markets").glob("**/telemetry.sqlite3"))
                    for path in legacy_paths:
                        # markets/<ticker>/telemetry.sqlite3 and
                        # markets/<venue>/<ticker>/telemetry.sqlite3 are both
                        # valid; the ticker is always the parent directory.
                        candidates.add(path.parent.name)
                    try:
                        manifest = json.loads((root / "fleet_manifest.json").read_text(encoding="utf-8"))
                        for mapping_name in ("activeTickerToShard", "tickerToShard"):
                            candidates.update(str(item) for item in (manifest.get(mapping_name) or {}))
                    except (OSError, ValueError, TypeError):
                        continue
            if not signatures and candidates:
                warnings.append(
                    "Per-market activity counters are unavailable for this legacy run; telemetry was scanned once and coverage may be incomplete."
                )

            items_by_ticker: Dict[str, Dict[str, Any]] = {}
            for ticker in sorted(candidates):
                signature = signatures.get(ticker, (0, 0, 0))
                if ticker in cached_items and cached_signatures.get(ticker) == signature:
                    items_by_ticker[ticker] = cached_items[ticker]
                    continue
                previous = cached_items.get(ticker)
                try:
                    path = self._market_database(artifact, ticker)
                    item = self._market_summary(
                        path,
                        ticker=ticker,
                        fallback_title=titles.get(ticker, ticker),
                        fallback_url=links.get(ticker),
                        legacy_metric=legacy.get(ticker) if isinstance(legacy.get(ticker), Mapping) else {},
                    )
                    items_by_ticker[ticker] = item
                except (KeyError, sqlite3.Error, OSError) as exc:
                    failed.append(ticker)
                    failed_errors.append(f"{ticker}: {exc}")
                    if previous:
                        stale_item = deepcopy(previous)
                        stale_item.setdefault("warnings", []).append(
                            f"Latest telemetry could not be read for {ticker}; showing the last successful summary."
                        )
                        items_by_ticker[ticker] = stale_item
                    warnings.append(f"Telemetry for {ticker} is unavailable: {exc}")

            # A counter cannot normally fall, but retaining an existing item across
            # a transient launcher restart is safer than dropping known activity.
            for ticker, item in cached_items.items():
                if ticker not in items_by_ticker and ticker in signatures:
                    items_by_ticker[ticker] = item

            items = list(items_by_ticker.values())
            items.sort(key=lambda item: (-(item["fillCount"] + item["orderCount"]), item["ticker"]))
            warnings.extend(warning for item in items for warning in item["warnings"])
            current = now_ms()
            stale = bool(failed) or (
                not completed and current - int(run.get("heartbeatAt") or 0) > HEARTBEAT_STALE_AFTER_MS
            )
            source: Dict[str, Any] = {
                "available": not failed or bool(items),
                "updatedAt": run.get("heartbeatAt"),
                "stale": stale,
            }
            if failed:
                source["error"] = f"Failed to refresh {len(failed)} market(s): {'; '.join(failed_errors)}"
            response = {
                "generatedAt": current,
                "runId": run_id,
                "activityRevision": revision,
                "source": source,
                "items": items,
                "warnings": list(dict.fromkeys(warnings)),
            }
            self._market_cache[run_id] = {
                "completed": completed,
                "revision": revision,
                "signatures": signatures,
                "items": items_by_ticker,
                "response": deepcopy(response),
            }
            self._market_cache.move_to_end(run_id)
            while len(self._market_cache) > MARKET_CACHE_RUN_LIMIT:
                self._market_cache.popitem(last=False)
            return response

    def run_market_activity(
        self,
        run_id: str,
        ticker: str,
        *,
        fill_limit: int,
        order_limit: int,
        market_links: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        run = self.get_run(run_id, include_artifact_bytes=False)
        artifact = self._artifact_for_run(run)
        path = self._market_database(artifact, ticker)
        titles = self._read_screener_titles(artifact)
        legacy = ((run.get("metrics") or {}).get("markets") or {}).get(ticker) or {}
        market = self._market_summary(
            path,
            ticker=ticker,
            fallback_title=titles.get(ticker, ticker),
            fallback_url=(market_links or {}).get(ticker),
            legacy_metric=legacy if isinstance(legacy, Mapping) else {},
        )
        current = now_ms()
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)) as db:
            db.row_factory = sqlite3.Row
            all_fills = list(db.execute("SELECT * FROM fills WHERE ticker=? ORDER BY ts_ms,id", (ticker,)))
            fill_total = len(all_fills)
            fills = list(reversed(all_fills[-int(fill_limit):]))
            latest_bids: Dict[str, Optional[int]] = {"yes": None, "no": None}
            if self._table_exists(db, "market_state"):
                latest_state = db.execute(
                    """SELECT best_yes_bid_units,best_no_bid_units FROM market_state
                       WHERE ticker=? AND (best_yes_bid_units IS NOT NULL OR best_no_bid_units IS NOT NULL)
                       ORDER BY ts_ms DESC,id DESC LIMIT 1""",
                    (ticker,),
                ).fetchone()
                if latest_state:
                    latest_bids["yes"] = (
                        int(latest_state["best_yes_bid_units"])
                        if latest_state["best_yes_bid_units"] is not None else None
                    )
                    latest_bids["no"] = (
                        int(latest_state["best_no_bid_units"])
                        if latest_state["best_no_bid_units"] is not None else None
                    )
            if latest_bids["yes"] is None or latest_bids["no"] is None:
                for historical_fill in reversed(all_fills):
                    if latest_bids["yes"] is None and historical_fill["best_yes_bid_units"] is not None:
                        latest_bids["yes"] = int(historical_fill["best_yes_bid_units"])
                    if latest_bids["no"] is None and historical_fill["best_no_bid_units"] is not None:
                        latest_bids["no"] = int(historical_fill["best_no_bid_units"])
                    if latest_bids["yes"] is not None and latest_bids["no"] is not None:
                        break
            has_orders = self._table_exists(db, "order_revisions")
            order_columns = {str(row[1]) for row in db.execute("PRAGMA table_info(order_revisions)")} if has_orders else set()
            shard_orders = "ticker" in order_columns
            order_total = int(db.execute(
                "SELECT COUNT(*) FROM order_revisions WHERE ticker=?" if shard_orders else "SELECT COUNT(*) FROM order_revisions",
                (ticker,) if shard_orders else (),
            ).fetchone()[0]) if has_orders else int(market["orderCount"])
            orders = list(db.execute(
                "SELECT * FROM order_revisions WHERE ticker=? ORDER BY placed_at_ms DESC,id DESC LIMIT ?" if shard_orders else "SELECT * FROM order_revisions ORDER BY placed_at_ms DESC,id DESC LIMIT ?",
                (ticker, int(order_limit)) if shard_orders else (int(order_limit),),
            )) if has_orders else []
            order_times: Dict[str, list[int]] = {}
            if has_orders:
                query = (
                    "SELECT order_id,placed_at_ms FROM order_revisions WHERE ticker=? AND order_id IS NOT NULL ORDER BY placed_at_ms"
                    if shard_orders else
                    "SELECT order_id,placed_at_ms FROM order_revisions WHERE order_id IS NOT NULL ORDER BY placed_at_ms"
                )
                for row in db.execute(query, (ticker,) if shard_orders else ()):
                    order_times.setdefault(str(row[0]), []).append(int(row[1]))
            latest_markouts: Dict[tuple[str, int], sqlite3.Row] = {}
            if self._table_exists(db, "markouts"):
                for markout in db.execute(
                    "SELECT * FROM markouts WHERE ticker=? ORDER BY ts_ms,id", (ticker,)
                ):
                    latest_markouts[(str(markout["fill_key"]), int(markout["horizon_ms"]))] = markout
        pnl_by_fill, pnl_warnings = self._fill_pnl_breakdown(all_fills, latest_bids=latest_bids)
        fill_items = []
        for row in fills:
            side = str(row["side"] or "")
            yes_bid = int(row["best_yes_bid_units"]) if row["best_yes_bid_units"] is not None else None
            no_bid = int(row["best_no_bid_units"]) if row["best_no_bid_units"] is not None else None
            bid = yes_bid if side == "yes" else no_bid
            ask = (10_000 - no_bid) if side == "yes" and no_bid is not None else (10_000 - yes_bid) if side == "no" and yes_bid is not None else None
            midpoint = int(round((bid + ask) / 2)) if bid is not None and ask is not None else None
            count = int(row["size_units"] or 0)
            price = int(row["price_units"]) if row["price_units"] is not None else None
            notional = self._value_units(count, price)
            fee = int(row["fee_units"]) if row["fee_units"] is not None else None
            total_paid = notional + fee if notional is not None and fee is not None else None
            liquidation = self._value_units(count, bid)
            unrealized = self._value_units(count, midpoint)
            placements = order_times.get(str(row["order_id"] or ""), [])
            placed = max((value for value in placements if value <= int(row["ts_ms"])), default=None)
            fill_markouts: Dict[str, Dict[str, Any]] = {}
            for horizon_ms in MARKOUT_HORIZONS_MS:
                markout = latest_markouts.get((str(row["fill_key"]), horizon_ms))
                if (
                    markout is None
                    or markout["future_mid_yes_units"] is None
                    or price is None
                    or fee is None
                ):
                    continue
                future_mid_yes = int(markout["future_mid_yes_units"])
                signed = future_mid_yes - price if side == "yes" else 10_000 - future_mid_yes - price
                gross = monetary_markout_units(signed, count)
                fill_markouts[str(horizon_ms)] = {
                    "horizonMs": horizon_ms,
                    "capturedAtMs": int(markout["ts_ms"]),
                    "futureMidYesUnits": future_mid_yes,
                    "signedMarkoutPriceUnits": signed,
                    "grossMarkoutUnits": gross,
                    "feeUnits": fee,
                    "netMarkoutUnits": gross - fee,
                }
            fill_items.append({
                "fillId": str(row["trade_id"] or row["fill_key"]),
                "orderId": row["order_id"],
                "filledAtMs": int(row["ts_ms"]),
                "side": side,
                "contractsUnits": count,
                "timeToFillMs": max(0, int(row["ts_ms"]) - placed) if placed is not None else None,
                "totalPaidUnits": total_paid,
                "liquidationValueUnits": liquidation,
                "unrealizedValueUnits": unrealized,
                "markoutsByHorizon": fill_markouts,
                **pnl_by_fill.get(int(row["id"]), {
                    "matchedContractsUnits": 0,
                    "openContractsUnits": count,
                    "realizedPnlUnits": None,
                    "unrealizedPnlUnits": None,
                    "fillPnlUnits": None,
                }),
            })
        order_items = [{
            "revisionKey": row["revision_key"],
            "orderId": row["order_id"] or row["client_order_id"],
            "placedAtMs": int(row["placed_at_ms"]),
            "side": row["side"],
            "contractsUnits": int(row["size_units"] or 0),
            "timeOnBookMs": max(0, int(row["ended_at_ms"] or current) - int(row["placed_at_ms"])),
            "bookBidPriceUnits": row["book_bid_units"],
            "bookAskPriceUnits": row["book_ask_units"],
            "bookMidPriceUnits": row["book_mid_units"],
            "orderPriceUnits": row["price_units"],
            "endedState": row["ended_state"],
        } for row in orders]
        warnings = list(dict.fromkeys([*market["warnings"], *pnl_warnings]))
        updated_at = self._telemetry_updated_at(path)
        return {
            "generatedAt": current,
            "runId": run_id,
            "market": market,
            "source": {
                "available": True,
                "updatedAt": updated_at,
                "stale": run["status"] in ACTIVE_RUN_STATES and (
                    updated_at is None or current - updated_at > HEARTBEAT_STALE_AFTER_MS
                ),
            },
            "fills": {"items": fill_items, "totalCount": fill_total, "truncated": fill_total > len(fill_items)},
            "orders": {"items": order_items, "totalCount": order_total, "truncated": order_total > len(order_items)},
            "warnings": warnings,
        }


class RunMetricsAccumulator:
    """Accumulate process-scoped counters across bot refreshes/restarts."""

    def __init__(self, started_at_ms: int) -> None:
        self.started_at_ms = started_at_ms
        self.processes: Dict[str, Dict[str, Any]] = {}
        self.components: Dict[str, Dict[str, int]] = {}

    @staticmethod
    def _sum_actions(client: Mapping[str, Any], outcome: str) -> int:
        total = 0
        for action in (client.get("orderActivity") or {}).get("byAction", {}).values():
            total += int((action or {}).get(outcome) or 0)
        return total

    @staticmethod
    def _placement_attempts(client: Mapping[str, Any]) -> int:
        create = ((client.get("orderActivity") or {}).get("byAction") or {}).get("create") or {}
        return int(create.get("attempts") or 0)

    def observe(self, status: Mapping[str, Any]) -> Dict[str, Any]:
        per_market: Dict[str, Dict[str, Any]] = {}
        for client in status.get("clients") or []:
            runtime = client.get("runtime") or {}
            if not runtime.get("startedAtMs"):
                continue
            key = f"{client.get('venue')}:{client.get('marketId')}:{client.get('pid')}:{runtime.get('startedAtMs')}"
            pnl = client.get("pnl") or {}
            rest = (client.get("apiActivity") or {}).get("rest") or {}
            self.processes[key] = {
                "marketId": client.get("marketId"), "orders": self._sum_actions(client, "attempts"),
                "orderPlacementsAttempted": self._placement_attempts(client),
                "orderSuccesses": self._sum_actions(client, "successes"), "orderErrors": self._sum_actions(client, "errors"),
                "fills": int((client.get("fills") or {}).get("count") or 0), "apiCalls": int(rest.get("total") or 0),
                "apiErrors": int(rest.get("errors") or 0), "realizedCents": float(pnl.get("realizedCents") or 0),
                "feesCents": float(pnl.get("feesCents") or 0),
                "unrealizedCents": float(pnl.get("unrealizedCents") or 0), "totalCents": float(pnl.get("totalCents") or 0),
                "pnlComplete": bool(pnl) and (not pnl.get("sessionPositionUnits") or pnl.get("markPriceUnits") is not None),
                "markoutsByHorizon": client.get("markouts") or {},
            }
        for component in ("screener", "portfolio"):
            rest = ((status.get(component) or {}).get("apiActivity") or {}).get("rest") or {}
            current = self.components.setdefault(component, {"total": 0, "errors": 0})
            current["total"] = max(current["total"], int(rest.get("total") or 0))
            current["errors"] = max(current["errors"], int(rest.get("errors") or 0))
        for value in self.processes.values():
            market = per_market.setdefault(str(value["marketId"]), {"orders": 0, "orderPlacementsAttempted": 0, "fills": 0, "totalCents": 0.0, "apiCalls": 0, "markoutsByHorizon": {}})
            for name in ("orders", "fills", "apiCalls"):
                market[name] += value[name]
            market["orderPlacementsAttempted"] += value["orderPlacementsAttempted"]
            market["totalCents"] += value["totalCents"]
            market["markoutsByHorizon"] = combine_markout_maps([
                market["markoutsByHorizon"], value["markoutsByHorizon"]
            ])
        values = list(self.processes.values())
        bot_api = sum(item["apiCalls"] for item in values)
        bot_errors = sum(item["apiErrors"] for item in values)
        api_by_component = {"bots": bot_api, **{key: item["total"] for key, item in self.components.items()}}
        timestamp = now_ms()
        return {
            "runtimeMs": max(0, timestamp - self.started_at_ms),
            "orders": sum(item["orders"] for item in values), "orderSuccesses": sum(item["orderSuccesses"] for item in values),
            "orderPlacementsAttempted": sum(item["orderPlacementsAttempted"] for item in values),
            "orderErrors": sum(item["orderErrors"] for item in values), "fills": sum(item["fills"] for item in values),
            "apiCalls": sum(api_by_component.values()), "apiErrors": bot_errors + sum(item["errors"] for item in self.components.values()),
            "apiByComponent": api_by_component, "realizedCents": round(sum(item["realizedCents"] for item in values), 4),
            "feesCents": round(sum(item["feesCents"] for item in values), 4),
            "unrealizedCents": round(sum(item["unrealizedCents"] for item in values), 4),
            "totalCents": round(sum(item["totalCents"] for item in values), 4),
            "pnlComplete": all(item["pnlComplete"] for item in values), "markets": per_market,
            "markoutsByHorizon": combine_markout_maps(
                item["markoutsByHorizon"] for item in values
            ),
        }
