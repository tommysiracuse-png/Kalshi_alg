"""SQLite results store and markdown report writer."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

NEG_INF = float("-inf")


class ResultsStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS opt_runs(
                    id TEXT PRIMARY KEY,
                    started_ms INTEGER,
                    data_from_ms INTEGER,
                    data_to_ms INTEGER,
                    markets_json TEXT,
                    space_json TEXT,
                    args_json TEXT,
                    status TEXT,
                    finished_ms INTEGER
                );
                CREATE TABLE IF NOT EXISTS candidates(
                    opt_run_id TEXT,
                    candidate_id TEXT,
                    params_json TEXT,
                    stage TEXT,
                    split_id TEXT,
                    score_units REAL,
                    fills INTEGER,
                    contracts_units REAL,
                    max_drawdown_units REAL,
                    error_count INTEGER,
                    PRIMARY KEY(opt_run_id, candidate_id, stage, split_id)
                );
                CREATE TABLE IF NOT EXISTS sensitivity(
                    opt_run_id TEXT,
                    field TEXT,
                    delta_units REAL,
                    rank INTEGER,
                    PRIMARY KEY(opt_run_id, field)
                );
                """
            )
            columns = {row[1] for row in db.execute("PRAGMA table_info(opt_runs)")}
            if "heartbeat_ms" not in columns:
                db.execute("ALTER TABLE opt_runs ADD COLUMN heartbeat_ms INTEGER")
                db.commit()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.execute("PRAGMA busy_timeout=30000")
        return db

    def heartbeat_run(self, run_id: str) -> None:
        """Prove the run is alive during long silent stages.

        The dashboard treats a results database untouched for 10 minutes as an
        abandoned run and labels it interrupted; a single sensitivity task on
        a busy market can stay silent longer than that.
        """
        with self._connect() as db:
            db.execute("UPDATE opt_runs SET heartbeat_ms=? WHERE id=?", (int(time.time() * 1000), run_id))
            db.commit()

    def create_run(
        self,
        run_id: str,
        data_from_ms: int,
        data_to_ms: int,
        markets: Sequence[Tuple[str, int, int]],
        space_json: Dict[str, Any],
        args_json: Dict[str, Any],
    ) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO opt_runs"
                "(id, started_ms, data_from_ms, data_to_ms, markets_json, space_json, args_json, status, finished_ms)"
                " VALUES(?,?,?,?,?,?,?,?,NULL)",
                (
                    run_id,
                    int(time.time() * 1000),
                    int(data_from_ms),
                    int(data_to_ms),
                    json.dumps([m[0] for m in markets]),
                    json.dumps(space_json),
                    json.dumps(args_json),
                    "running",
                ),
            )
            db.commit()

    def finish_run(self, run_id: str, status: str = "finished") -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE opt_runs SET status=?, finished_ms=? WHERE id=?",
                (status, int(time.time() * 1000), run_id),
            )
            db.commit()

    def record_candidate(self, run_id: str, row: Dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO candidates VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    row["candidate_id"],
                    json.dumps(row["params"], sort_keys=True),
                    row["stage"],
                    str(row["split_id"]),
                    float(row["score"]),
                    int(row["fills"]),
                    float(row["contracts"]),
                    float(row["max_drawdown"]),
                    int(row["errors"]),
                ),
            )
            db.commit()

    def record_sensitivity(self, run_id: str, ranking: Sequence[Tuple[str, float]]) -> None:
        with self._connect() as db:
            db.executemany(
                "INSERT OR REPLACE INTO sensitivity VALUES(?,?,?,?)",
                [
                    (run_id, field, float(delta), rank + 1)
                    for rank, (field, delta) in enumerate(ranking)
                ],
            )
            db.commit()


def _fmt(value: float) -> str:
    if value == NEG_INF:
        return "DQ (fills < min)"
    return f"{value:,.2f}"


def write_report(path: str, ctx: Dict[str, Any]) -> str:
    """Render the run report as markdown. ``ctx`` is assembled by main."""
    lines: List[str] = []
    add = lines.append
    add(f"# Optimizer run `{ctx['run_id']}`")
    add("")
    add(f"- Data window: {ctx['data_from_ms']} .. {ctx['data_to_ms']} ms")
    add(f"- Markets: {ctx['market_count']} | splits: {ctx['split_count']} | seed: {ctx['seed']}")
    add(f"- Candidates sampled: {ctx['n_candidates']} over {len(ctx['kept_fields'])} kept parameters")
    add(f"- Objective: MAX total P&L (sum of total_net_units; OOS = mean over test splits only)")
    add(f"- Min-fills guard: {ctx['min_fills']} summed fills")
    add("")

    add("## Leaderboard (top 10, out-of-sample)")
    add("")
    add("| rank | candidate | OOS J | train J | gap (train-test) | fills | params (diff vs default) |")
    add("|---|---|---|---|---|---|---|")
    for index, row in enumerate(ctx["leaderboard"][:10]):
        gap = (
            row["train_score"] - row["oos_raw"]
            if row["train_score"] > NEG_INF and row["oos_raw"] > NEG_INF
            else float("nan")
        )
        diff = json.dumps(row["diff_params"], sort_keys=True)
        add(
            f"| {index + 1} | {row['candidate_id']} | {_fmt(row['oos_score'])} | "
            f"{_fmt(row['train_score'])} | {gap:,.2f} | {row['fills']} | `{diff}` |"
        )
    add("")

    add("## A/B vs pure default")
    add("")
    winner = ctx["winner"]
    default = ctx["default_ab"]
    add("| config | OOS J | train J | fills |")
    add("|---|---|---|---|")
    add(f"| winner `{winner['candidate_id']}` | {_fmt(winner['oos_score'])} | {_fmt(winner['train_score'])} | {winner['fills']} |")
    add(f"| default | {_fmt(default['oos_score'])} | {_fmt(default['train_score'])} | {default['fills']} |")
    verdict = "WINNER beats default" if winner["oos_score"] > default["oos_score"] else "default holds (winner does NOT beat default)"
    add("")
    add(f"**Verdict:** {verdict}.")
    add("")

    add("## Winner fill-share sensitivity (OOS)")
    add("")
    add("| fill_share_fraction | OOS J |")
    add("|---|---|")
    for fraction, score in ctx["fill_share_rows"]:
        add(f"| {fraction} | {_fmt(score)} |")
    add("")

    sizing = ctx.get("fleet_sizing") or {}
    if sizing.get("per_market"):
        add("## Fleet sizing (winner per-market OOS P&L, best markets first)")
        add("")
        budget = sizing.get("winner_budget_cents_per_market", 0)
        add(f"- Winner budgets: {budget} cents committed per market (both sides)")
        add(f"- **Cumulative OOS P&L peaks at {sizing.get('best_market_count', 0)} markets** — "
            "markets past that rank lose money under this config; use it to set maxBots.")
        add(f"- Capital needed for N markets ~= N x {budget} cents (plus 20% cash reserve).")
        add("")
        add("| rank | ticker | OOS J | fills | cumulative J |")
        add("|---|---|---|---|---|")
        for row in sizing["per_market"][:40]:
            add(f"| {row['rank']} | {row['ticker']} | {_fmt(row['oos_units'])} |"
                f" {row['fills']} | {_fmt(row['cumulative_units'])} |")
        if len(sizing["per_market"]) > 40:
            add(f"| ... | ({len(sizing['per_market']) - 40} more markets) | | | |")
        add("")

    add("## Sensitivity ranking (Morris OAT, |delta J| on subsample)")
    add("")
    add("| rank | field | delta J | kept for search |")
    add("|---|---|---|---|")
    kept = set(ctx["kept_fields"])
    for rank, (fieldname, delta) in enumerate(ctx["sensitivity"]):
        add(f"| {rank + 1} | {fieldname} | {delta:,.4f} | {'yes' if fieldname in kept else 'no'} |")
    add("")

    add("## Excluded parameters (pinned to default)")
    add("")
    add("| field | reason |")
    add("|---|---|")
    for fieldname, reason in sorted(ctx["excluded"].items()):
        add(f"| {fieldname} | {reason} |")
    add("")

    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return str(report_path)
