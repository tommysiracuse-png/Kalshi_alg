"""SQLite persistence for saved sessions, immutable runs, and local metrics."""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional

from session_config import default_session_configuration, validate_session_configuration


ACTIVE_RUN_STATES = ("pending", "starting", "running")


class SessionConflictError(RuntimeError):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


class SessionStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.path = self.root / "sessions.sqlite3"
        self.artifacts_root = self.root / "artifacts"
        self.root.mkdir(parents=True, exist_ok=True)
        self.artifacts_root.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self.bootstrap_default()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=5)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA busy_timeout=5000")
            db.execute("PRAGMA foreign_keys=ON")
            with db:
                yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._connect() as db:
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
                PRAGMA user_version=1;
            """)

    @staticmethod
    def _configuration(row: sqlite3.Row) -> Dict[str, Any]:
        return json.loads(row["configuration_json"])

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

    def claim_run(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        with self._connect() as db:
            timestamp = now_ms()
            db.execute("UPDATE runs SET status='interrupted',ended_at_ms=COALESCE(heartbeat_at_ms,?),error=COALESCE(error,'launcher exited without finalizing') WHERE status IN ('starting','running')", (timestamp,))
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
                db.execute("INSERT INTO metric_samples(run_id,timestamp_ms,payload_json) VALUES(?,?,?)", (run_id, timestamp, payload))

    def finish_run(self, run_id: str, status: str, *, metrics: Optional[Mapping[str, Any]] = None, error: Optional[str] = None) -> None:
        if status not in {"stopped", "failed", "shutdown_failed", "interrupted"}:
            raise ValueError("invalid final run status")
        final_metrics = dict(metrics or {})
        with self._connect() as db:
            row = db.execute("SELECT artifact_path FROM runs WHERE id=?", (run_id,)).fetchone()
        if row:
            pnl_path = Path(row["artifact_path"]) / "pnl_tracker.jsonl"
            if pnl_path.exists():
                from pnl_core import load_fills, summarize_pnl
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
        if final_metrics:
            self.record_metrics(run_id, final_metrics, sample=False)
        with self._connect() as db:
            db.execute("UPDATE runs SET status=?,ended_at_ms=?,heartbeat_at_ms=?,error=? WHERE id=?", (status, now_ms(), now_ms(), error, run_id))

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

    def get_run(self, run_id: str) -> Dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if not row:
                raise KeyError(run_id)
            result = self._run_dict(row)
        result["artifactBytes"] = self._artifact_size(Path(result["artifactPath"]))
        return result

    def list_runs(self, *, session_id: str = "", status: str = "", from_ms: Optional[int] = None, to_ms: Optional[int] = None) -> list[Dict[str, Any]]:
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
        runs = self.list_runs(**filters)
        totals: Dict[str, Any] = {
            "timesRun": len(runs), "runtimeMs": 0, "orders": 0, "fills": 0,
            "apiCalls": 0, "apiErrors": 0, "realizedCents": 0.0, "unrealizedCents": 0.0,
            "totalCents": 0.0, "pnlComplete": True, "outcomes": {}, "apiByComponent": {},
        }
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
            totals["outcomes"][run["status"]] = totals["outcomes"].get(run["status"], 0) + 1
            for component, count in (metric.get("apiByComponent") or {}).items():
                totals["apiByComponent"][component] = totals["apiByComponent"].get(component, 0) + int(count)
        minutes = totals["runtimeMs"] / 60_000
        totals["ordersPerMinute"] = round(totals["orders"] / minutes, 4) if minutes else 0.0
        totals["fillsPerMinute"] = round(totals["fills"] / minutes, 4) if minutes else 0.0
        for key in ("realizedCents", "unrealizedCents", "totalCents"):
            totals[key] = round(totals[key], 4)
        return {"generatedAt": now_ms(), "summary": totals, "runs": runs}


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
            key = f"{client.get('marketId')}:{client.get('pid')}:{runtime.get('startedAtMs')}"
            pnl = client.get("pnl") or {}
            rest = (client.get("apiActivity") or {}).get("rest") or {}
            self.processes[key] = {
                "marketId": client.get("marketId"), "orders": self._sum_actions(client, "attempts"),
                "orderPlacementsAttempted": self._placement_attempts(client),
                "orderSuccesses": self._sum_actions(client, "successes"), "orderErrors": self._sum_actions(client, "errors"),
                "fills": int((client.get("fills") or {}).get("count") or 0), "apiCalls": int(rest.get("total") or 0),
                "apiErrors": int(rest.get("errors") or 0), "realizedCents": float(pnl.get("realizedCents") or 0),
                "unrealizedCents": float(pnl.get("unrealizedCents") or 0), "totalCents": float(pnl.get("totalCents") or 0),
                "pnlComplete": not pnl.get("sessionPositionUnits") or pnl.get("markPriceUnits") is not None,
            }
        for component in ("screener", "portfolio"):
            rest = ((status.get(component) or {}).get("apiActivity") or {}).get("rest") or {}
            current = self.components.setdefault(component, {"total": 0, "errors": 0})
            current["total"] = max(current["total"], int(rest.get("total") or 0))
            current["errors"] = max(current["errors"], int(rest.get("errors") or 0))
        for value in self.processes.values():
            market = per_market.setdefault(str(value["marketId"]), {"orders": 0, "orderPlacementsAttempted": 0, "fills": 0, "totalCents": 0.0, "apiCalls": 0})
            for name in ("orders", "fills", "apiCalls"):
                market[name] += value[name]
            market["orderPlacementsAttempted"] += value["orderPlacementsAttempted"]
            market["totalCents"] += value["totalCents"]
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
            "unrealizedCents": round(sum(item["unrealizedCents"] for item in values), 4),
            "totalCents": round(sum(item["totalCents"] for item in values), 4),
            "pnlComplete": all(item["pnlComplete"] for item in values), "markets": per_market,
        }
