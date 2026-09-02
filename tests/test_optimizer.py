"""Tests for the optimizer package. The replay engine is stubbed throughout."""

from __future__ import annotations

import json
import random
import shutil
import sqlite3
import tempfile
from pathlib import Path

import pytest

from optimizer import _synthetic, main, runner, search, sensitivity, walkforward, writeback
from optimizer.space import (
    FAIR_MID,
    FAIR_TICKER,
    FAIR_TRADE,
    build_space,
    finalize_params,
    is_valid_bot_overrides,
)

A_FIELD = _synthetic.INFLUENTIAL_FIELD


@pytest.fixture(scope="module")
def space():
    return build_space()


@pytest.fixture()
def tmp_dir():
    # Deliberately not pytest's tmp_path: the shared pytest-of-admin temp root
    # has broken ACLs on this machine and errors before any test runs.
    path = Path(tempfile.mkdtemp(prefix="optimizer-test-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _stub_result(ticker, score, fills=50):
    return {
        "ticker": ticker,
        "error": None,
        "decisions": 10,
        "orders_placed": 5,
        "fills": fills,
        "contracts_units": 10.0,
        "fees_units": 0.5,
        "net_markout_units_by_horizon": {1000: score, 5000: score, 30000: score, 120000: score},
        "total_net_units": score,
        "max_drawdown_units": 1.0,
        "settlement_net_units": score,
    }


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


# 1. Every sampled config passes real BotSettings validation ------------------

def test_sampled_configs_pass_bot_settings_validation(space):
    rng = random.Random(7)
    kept = sorted(space.dims)
    candidates = search.latin_hypercube_candidates(space, kept, 20, rng)
    assert len(candidates) == 20
    for candidate in candidates:
        assert is_valid_bot_overrides(candidate.params), candidate.params


# 2. Fair-value weights always sum to 1 --------------------------------------

def test_fair_value_weights_sum_to_one(space):
    forced = finalize_params(space, {FAIR_MID: 0.9, FAIR_TICKER: 0.8})
    total = forced[FAIR_MID] + forced[FAIR_TICKER] + forced[FAIR_TRADE]
    assert abs(total - 1.0) < 1e-9
    assert min(forced[FAIR_MID], forced[FAIR_TICKER], forced[FAIR_TRADE]) >= 0.0

    rng = random.Random(11)
    kept = sorted(space.dims)
    for candidate in search.latin_hypercube_candidates(space, kept, 15, rng):
        params = candidate.params
        if FAIR_MID in params:
            total = params[FAIR_MID] + params[FAIR_TICKER] + params[FAIR_TRADE]
            assert abs(total - 1.0) < 1e-9
            assert min(params[FAIR_MID], params[FAIR_TICKER], params[FAIR_TRADE]) >= 0.0


# 3. Sensitivity ranks the truly-influential synthetic param first -----------

def test_sensitivity_ranks_influential_param_first(space, monkeypatch):
    monkeypatch.setattr(runner, "EVALUATOR_OVERRIDE", _synthetic.evaluate_candidate)
    markets = [("T-A", 0, 1_000), ("T-B", 0, 1_000)]
    ranking, per_eval = sensitivity.run_screen(
        space, "unused.db", markets, (0, 1_000), workers=1, seed=0,
    )
    assert ranking[0][0] == A_FIELD
    assert ranking[0][1] > 0
    assert per_eval >= 0


# 4. Successive halving keeps the known-best candidate -----------------------

def test_halving_keeps_known_best(monkeypatch):
    monkeypatch.setattr(runner, "EVALUATOR_OVERRIDE", _synthetic.evaluate_candidate)
    candidates = [
        search.Candidate(f"c{v}", {A_FIELD: v}) for v in range(9)
    ]  # synthetic optimum is at A_FIELD == 6
    splits = walkforward.build_splits(0, 4_000, k=3)
    markets = [("T-A", 0, 4_000), ("T-B", 0, 4_000)]
    train = walkforward.train_windows(splits)
    rung_plans = [
        {"windows": [train[0]], "markets": markets},
        {"windows": train[:2], "markets": markets},
    ]
    survivors = search.successive_halving(
        "unused.db", candidates, rung_plans, workers=1, seed=0,
    )
    finals = search.final_stage("unused.db", survivors, splits, markets, workers=1, seed=0)
    assert finals[0].params[A_FIELD] == 6


# 5. Ranking is OOS-only: in-sample winner must not rank first ---------------

def test_final_ranking_is_out_of_sample_only(monkeypatch):
    def split_dependent_stub(db, ticker, overrides, start_ms, end_ms, seed=0,
                             fill_share_fraction=0.5, assumed_top_depth_contracts=100):
        a = overrides.get(A_FIELD, 2)
        if start_ms < 1_000:  # only the earliest train window: in-sample mirage
            score = 1_000.0 if a == 7 else 0.0
        else:
            score = 5.0 if a == 7 else 50.0
        return _stub_result(ticker, score)

    monkeypatch.setattr(runner, "EVALUATOR_OVERRIDE", split_dependent_stub)
    splits = walkforward.build_splits(0, 4_000, k=3)
    markets = [("T-A", 0, 4_000)]
    in_sample = search.Candidate("in_sample_winner", {A_FIELD: 7})
    steady = search.Candidate("steady", {A_FIELD: 3})
    finals = search.final_stage("unused.db", [in_sample, steady], splits, markets, workers=1, seed=0)

    by_id = {f.candidate_id: f for f in finals}
    assert by_id["in_sample_winner"].train_score > by_id["steady"].train_score
    assert finals[0].candidate_id == "steady"
    assert finals[0].candidate_id != "in_sample_winner"


# 6. Write-back creates a new session, selection unchanged -------------------

def test_write_back_creates_session_without_selecting(tmp_dir, monkeypatch):
    store_root = tmp_dir / "store"
    monkeypatch.setenv("KALSHI_SESSION_STORE", str(store_root))
    created = writeback.write_back(
        "run-test-1234", {A_FIELD: 4}, 12.5, 0, 1_000_000,
    )
    assert created["name"].startswith("optimized-")
    assert created["configuration"]["bot"][A_FIELD] == 4
    assert created["selected"] is False

    from session_store import SessionStore

    store = SessionStore(store_root)
    sessions = store.list_sessions()
    names = {s["name"]: s for s in sessions}
    assert created["name"] in names
    selected = [s for s in sessions if s["selected"]]
    assert len(selected) == 1
    assert selected[0]["name"] == "Default"


# 7. End-to-end 2-worker Pool smoke run, deterministic -----------------------

def test_end_to_end_pool_run_deterministic(tmp_dir, monkeypatch):
    monkeypatch.setenv("OPTIMIZER_EVALUATOR", "optimizer._synthetic:evaluate_candidate")
    assert runner.EVALUATOR_OVERRIDE is None
    history = tmp_dir / "history.sqlite3"
    make_history_db(history, ["T-A", "T-B", "T-C", "T-D"])

    def run(output_name):
        args = main.build_arg_parser().parse_args([
            "--history", str(history),
            "--candidates", "12",
            "--workers", "2",
            "--seed", "3",
            "--top-params", "4",
            "--budget-minutes", "5",
            "--markets", "4",
            "--output-dir", str(tmp_dir / output_name),
        ])
        return main.run_optimization(args)

    summary_one = run("out1")
    summary_two = run("out2")

    assert summary_one["winner_params"] == summary_two["winner_params"]
    assert summary_one["winner_oos"] == pytest.approx(summary_two["winner_oos"])
    assert summary_one["default_oos"] == pytest.approx(summary_two["default_oos"])
    assert Path(summary_one["report_path"]).is_file()
    report_text = Path(summary_one["report_path"]).read_text(encoding="utf-8")
    assert "Leaderboard" in report_text and "fill-share" in report_text.lower()

    results_db = tmp_dir / "out1" / "results.sqlite3"
    con = sqlite3.connect(results_db)
    try:
        runs = con.execute("SELECT id, status FROM opt_runs").fetchall()
        assert runs and runs[0][1] == "finished"
        candidate_rows = con.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
        sensitivity_rows = con.execute("SELECT COUNT(*) FROM sensitivity").fetchone()[0]
        assert candidate_rows > 0
        assert sensitivity_rows > 0
    finally:
        con.close()
