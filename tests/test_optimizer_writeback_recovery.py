"""Winner file, the ``optimizer.writeback`` CLI and the non-fatal write-back path of ``optimizer.main``.

The 2026-09-02 incident: a finished 110-minute run crashed inside the
in-process write-back because the session schema on disk had changed while
the run was in flight. The result must be persisted before the write-back,
the write-back must run on the code on disk (a fresh subprocess), and a
failure there must leave the run's exit code alone.

Every session store touched here is a temp directory (``KALSHI_SESSION_STORE``
plus ``--store-root``); the real ``session_data`` is never opened.
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from optimizer import main as optimizer_main
from optimizer import writeback
from session_config import default_session_configuration
from session_store import SessionStore

WORKSPACE = Path(__file__).resolve().parents[1]
SESSION_RE = re.compile(r"write-back: created session '([^']+)'")  # same as the chain runner
RUN_ID = "20260902-170800-abc123"


# --- helpers ------------------------------------------------------------------

@pytest.fixture()
def store_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A throwaway session store; the env var points every code path at it."""
    root = tmp_path / "store"
    monkeypatch.setenv("KALSHI_SESSION_STORE", str(root))
    return root


def _base_configuration():
    configuration = default_session_configuration()
    # Operator-tuned fields the optimizer never searches.
    configuration["bot"]["orderbook_pull_side_cooldown_ms"] = 1500
    configuration["launcher"]["maxBots"] = 100
    return configuration


def _create_base_session(root: Path, name: str = "guards-v1") -> None:
    SessionStore(root).create_session({"name": name, "description": "", "configuration": _base_configuration()})


def _session_named(root: Path, name: str):
    return next(s for s in SessionStore(root).list_sessions(include_archived=True) if s["name"] == name)


def _winner_record(**overrides):
    record = {
        "run_id": RUN_ID, "candidate_id": "c_007",
        "params": {"minimum_expected_edge_cents_to_quote": 3, "yes_order_budget_cents": 825},
        "oos_score": 7.5, "train_score": 9.25, "fills": 120,
        "data_from_ms": 0, "data_to_ms": 1_000, "market_class": None, "base_session": "guards-v1",
        "write_back": True, "created_at_ms": 1_700_000_000_000,
    }
    record.update(overrides)
    return record


def make_history_db(path, tickers, start_ms=0, end_ms=4_000_000, step_ms=60_000):
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE markets(ticker TEXT PRIMARY KEY, series TEXT, title TEXT, status TEXT,"
        " close_ts_ms INTEGER, result TEXT, volume_24h_units REAL, oi_units REAL, meta_json TEXT)"
    )
    con.execute("CREATE TABLE candles(ticker TEXT, ts_ms INTEGER, open_cents REAL)")
    con.execute("CREATE TABLE trades(ticker TEXT, trade_id TEXT, ts_ms INTEGER)")
    for ticker in tickers:
        con.execute(
            "INSERT INTO markets VALUES(?,?,?,?,?,?,?,?,?)",
            (ticker, "S", ticker, "settled", end_ms, "yes", 100.0, 50.0, "{}"),
        )
        for ts in range(start_ms, end_ms + 1, step_ms):
            con.execute("INSERT INTO candles VALUES(?,?,?)", (ticker, ts, 50.0))
    con.commit()
    con.close()


@pytest.fixture()
def history(tmp_path: Path) -> Path:
    path = tmp_path / "history.sqlite3"
    make_history_db(path, ["T-A", "T-B", "T-C"])
    return path


@pytest.fixture()
def synthetic_evaluator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPTIMIZER_EVALUATOR", "optimizer._synthetic:evaluate_candidate")


def _argv(history: Path, output_dir: Path, *extra: str) -> list[str]:
    return [
        "--history", str(history), "--candidates", "9", "--workers", "1", "--seed", "3",
        "--top-params", "3", "--budget-minutes", "5", "--markets", "3",
        "--output-dir", str(output_dir), *extra,
    ]


def _run(history: Path, output_dir: Path, *extra: str):
    return optimizer_main.run_optimization(optimizer_main.build_arg_parser().parse_args(_argv(history, output_dir, *extra)))


def _stored_score(value: float):
    """What a score looks like in the JSON file (``-inf`` = DQ is stored as null)."""
    return None if not math.isfinite(value) else pytest.approx(value)


# --- 1. the winner file -------------------------------------------------------

def test_winner_file_records_the_result(tmp_path: Path, history: Path, synthetic_evaluator):
    output_dir = tmp_path / "out"
    summary = _run(history, output_dir)
    path = writeback.winner_file_path(output_dir, summary["run_id"])
    assert summary["winner_file"] == str(path) and path.is_file()
    assert summary["session_name"] is None

    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["run_id"] == summary["run_id"]
    assert record["candidate_id"] == summary["winner_id"]
    assert record["params"] == summary["winner_params"]
    assert record["oos_score"] == _stored_score(summary["winner_oos"])
    assert "train_score" in record and isinstance(record["fills"], int)
    assert isinstance(record["data_from_ms"], int) and record["data_from_ms"] < record["data_to_ms"]
    assert record["market_class"] is None and record["base_session"] is None
    assert record["write_back"] is False
    assert isinstance(record["created_at_ms"], int) and record["created_at_ms"] > 0
    # Completed after the report with the diagnostics a hand recovery wants.
    assert record["status"] == "finished"
    assert record["default_oos"] == _stored_score(summary["default_oos"])
    assert record["report_path"] == summary["report_path"]
    assert [row[0] for row in record["fill_share_rows"]] == [0.25, 0.5, 1.0]
    assert isinstance(record["finished_at_ms"], int)
    # Round-trips through the loader the CLI uses.
    loaded = writeback.load_winner_file(path)
    assert loaded["params"] == summary["winner_params"]
    assert loaded["run_id"] == summary["run_id"]


def test_winner_file_stores_disqualified_scores_as_null_and_loads_them_back(tmp_path: Path):
    path = writeback.save_winner_file(tmp_path / "w.json", _winner_record(oos_score=float("-inf")))
    assert json.loads(path.read_text(encoding="utf-8"))["oos_score"] is None
    assert writeback.load_winner_file(path)["oos_score"] == float("-inf")
    writeback.update_winner_file(path, status="finished")
    loaded = writeback.load_winner_file(path)
    assert loaded["status"] == "finished" and loaded["params"] == _winner_record()["params"]
    with pytest.raises(ValueError, match="missing field"):
        writeback.load_winner_file(writeback.save_winner_file(tmp_path / "bad.json", {"run_id": "x"}))


# --- 2. the CLI ----------------------------------------------------------------

def test_cli_recreates_the_session_from_a_winner_file(tmp_path: Path, store_root: Path, capsys):
    _create_base_session(store_root)
    path = writeback.save_winner_file(tmp_path / f"winner_{RUN_ID}.json", _winner_record())

    rc = writeback.main(["--winner-file", str(path), "--store-root", str(store_root)])
    out = capsys.readouterr().out
    assert rc == 0
    match = SESSION_RE.search(out)
    assert match, out
    name = match.group(1)
    assert out.strip().splitlines()[-1] == f"write-back: created session '{name}'"

    created = _session_named(store_root, name)
    assert f"optimizer run {RUN_ID}" in created["description"]  # the dashboard's $previous marker
    assert "OOS J=7.50" in created["description"]
    assert "unsearched fields from session 'guards-v1'" in created["description"]
    configuration = created["configuration"]
    assert configuration["bot"]["minimum_expected_edge_cents_to_quote"] == 3
    assert configuration["launcher"]["yesBudgetCents"] == 825
    # Unsearched fields come from the base session resolved by name.
    assert configuration["bot"]["orderbook_pull_side_cooldown_ms"] == 1500
    assert configuration["launcher"]["maxBots"] == 100
    assert created["selected"] is False
    # The winner file records what was created.
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["write_back_result"]["session_name"] == name
    assert record["write_back_result"]["session_id"] == created["id"]


def test_cli_fails_cleanly_and_honours_overrides(tmp_path: Path, store_root: Path, capsys):
    path = writeback.save_winner_file(tmp_path / "winner.json", _winner_record(base_session="nope"))

    rc = writeback.main(["--winner-file", str(path), "--store-root", str(store_root)])
    captured = capsys.readouterr()
    assert rc == 2
    assert "no saved session named 'nope'" in captured.err
    assert "created session" not in captured.out
    assert [s["name"] for s in SessionStore(store_root).list_sessions(include_archived=True)] == ["Default"]

    # --base-session "" drops the recorded base session: schema defaults instead.
    rc = writeback.main(["--winner-file", str(path), "--store-root", str(store_root), "--base-session", ""])
    assert rc == 0
    name = SESSION_RE.search(capsys.readouterr().out).group(1)
    created = _session_named(store_root, name)
    assert "unsearched fields" not in created["description"]
    assert created["configuration"]["launcher"]["maxBots"] == default_session_configuration()["launcher"]["maxBots"]
    assert created["configuration"]["bot"]["minimum_expected_edge_cents_to_quote"] == 3

    assert writeback.main(["--winner-file", str(tmp_path / "missing.json"), "--store-root", str(store_root)]) == 2
    assert "cannot load winner file" in capsys.readouterr().err


def test_cli_runs_as_a_fresh_interpreter(tmp_path: Path, store_root: Path):
    """The real thing: ``python -m optimizer.writeback`` in a child process, with --class."""
    _create_base_session(store_root)
    path = writeback.save_winner_file(tmp_path / f"winner_{RUN_ID}.json", _winner_record())
    env = dict(os.environ, KALSHI_SESSION_STORE=str(store_root), PYTHONIOENCODING="utf-8")
    completed = subprocess.run(
        [sys.executable, "-m", "optimizer.writeback", "--winner-file", str(path),
         "--store-root", str(store_root), "--class", "thinWide"],
        cwd=str(WORKSPACE), env=env, capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=120, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    match = SESSION_RE.search(completed.stdout)
    assert match, completed.stdout
    name = match.group(1)
    assert name.startswith("optimized-thinWide-")
    assert f"write-back: created session '{name}' (class thinWide)" in completed.stdout

    created = _session_named(store_root, name)
    configuration = created["configuration"]
    assert configuration["botClasses"]["enabled"] is True
    overrides = {o["field"]: o["value"] for o in configuration["botClasses"]["thinWide"]["overrides"]}
    assert overrides == {"minimum_expected_edge_cents_to_quote": 3}
    assert configuration["launcher"]["yesBudgetCents"] == 825
    assert configuration["launcher"]["maxBots"] == 100  # base session kept
    assert f"optimizer run {RUN_ID}" in created["description"]
    assert json.loads(path.read_text(encoding="utf-8"))["write_back_result"]["session_name"] == name


# --- 3. optimizer.main's write-back path ----------------------------------------

def test_main_write_back_failure_is_non_fatal(
    tmp_path: Path, history: Path, store_root: Path, synthetic_evaluator, monkeypatch: pytest.MonkeyPatch, capsys,
):
    _create_base_session(store_root)
    commands: list[list[str]] = []

    def failing_run(command, **kwargs):
        commands.append(list(command))
        return subprocess.CompletedProcess(
            command, 1, stdout="",
            stderr="ValueError: unknown configuration field(s) at configuration.fleetRuntime: allocationOversubscription\n",
        )

    monkeypatch.setattr(optimizer_main.subprocess, "run", failing_run)
    output_dir = tmp_path / "out"
    argv = _argv(history, output_dir, "--write-back", "--base-session", "guards-v1")

    summary = optimizer_main.run_optimization(optimizer_main.build_arg_parser().parse_args(argv))  # returns normally
    out = capsys.readouterr().out
    assert summary["session_name"] is None
    winner_file = Path(summary["winner_file"])
    assert "write-back FAILED (rc=1): ValueError: unknown configuration field(s)" in out
    assert f"re-run: python -m optimizer.writeback --winner-file {winner_file}" in out
    assert "summary: winner J=" in out
    assert "created session" not in out

    assert len(commands) == 1
    command = commands[0]
    assert command[:3] == [sys.executable, "-m", "optimizer.writeback"]
    assert command[command.index("--winner-file") + 1] == str(winner_file.resolve())
    assert command[command.index("--base-session") + 1] == "guards-v1"
    assert command[command.index("--store-root") + 1] == str(store_root.resolve())

    # The run itself finished and its result is persisted for the re-run.
    record = json.loads(winner_file.read_text(encoding="utf-8"))
    assert record["status"] == "finished" and record["write_back"] is True
    assert record["base_session"] == "guards-v1" and record["params"] == summary["winner_params"]
    assert record["write_back_error"]["rc"] == 1
    assert "allocationOversubscription" in record["write_back_error"]["detail"]
    con = sqlite3.connect(output_dir / "results.sqlite3")
    try:
        assert con.execute("SELECT status FROM opt_runs WHERE id=?", (summary["run_id"],)).fetchone()[0] == "finished"
    finally:
        con.close()
    assert {s["name"] for s in SessionStore(store_root).list_sessions()} == {"Default", "guards-v1"}

    # The CLI entry point exits 0 despite the failed write-back.
    assert optimizer_main.main(argv) == 0
    assert "write-back FAILED (rc=1)" in capsys.readouterr().out


def test_main_write_back_runs_the_cli_in_a_subprocess(
    tmp_path: Path, history: Path, store_root: Path, synthetic_evaluator, capsys,
):
    _create_base_session(store_root)
    output_dir = tmp_path / "out"
    summary = _run(history, output_dir, "--write-back", "--base-session", "guards-v1")
    out = capsys.readouterr().out

    name = summary["session_name"]
    assert name and name.startswith("optimized-")
    assert f"write-back: created session '{name}'" in out  # the child's line, forwarded
    assert f"| session '{name}'" in out  # the summary line
    assert "write-back FAILED" not in out
    assert "unsearched fields taken from session 'guards-v1'" in out

    created = _session_named(store_root, name)
    assert f"optimizer run {summary['run_id']}" in created["description"]
    assert created["configuration"]["launcher"]["maxBots"] == 100
    for key, value in summary["winner_params"].items():
        if key in created["configuration"]["bot"]:
            assert created["configuration"]["bot"][key] == value
    record = json.loads(Path(summary["winner_file"]).read_text(encoding="utf-8"))
    assert record["write_back_result"]["session_name"] == name
    assert "write_back_error" not in record
