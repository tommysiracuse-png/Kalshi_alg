"""API tests for the optimizer dashboard endpoints (ui_api OptimizerService)."""

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

os.environ["KALSHI_UI_INTERNAL_TOKEN"] = "test-token"

import httpx
import pytest

import ui_api.app as app_module
from ui_api.config import Settings
from ui_api.store import OperationsStore

HEADERS = {"x-internal-token": "test-token"}
RUN_ID = "20260901-000001-abc123"

RESULTS_SCHEMA = """
CREATE TABLE opt_runs(
    id TEXT PRIMARY KEY, started_ms INTEGER, data_from_ms INTEGER, data_to_ms INTEGER,
    markets_json TEXT, space_json TEXT, args_json TEXT, status TEXT, finished_ms INTEGER
);
CREATE TABLE candidates(
    opt_run_id TEXT, candidate_id TEXT, params_json TEXT, stage TEXT, split_id TEXT,
    score_units REAL, fills INTEGER, contracts_units REAL, max_drawdown_units REAL,
    error_count INTEGER, PRIMARY KEY(opt_run_id, candidate_id, stage, split_id)
);
CREATE TABLE sensitivity(
    opt_run_id TEXT, field TEXT, delta_units REAL, rank INTEGER,
    PRIMARY KEY(opt_run_id, field)
);
"""


HISTORY_SCHEMA = """
CREATE TABLE markets(
    ticker TEXT PRIMARY KEY, series TEXT, title TEXT, status TEXT, close_ts_ms INTEGER,
    result TEXT, volume_24h_units INTEGER, oi_units INTEGER, meta_json TEXT
);
CREATE TABLE candles(ticker TEXT, ts_ms INTEGER, price_close_units INTEGER, volume_units INTEGER, oi_units INTEGER);
CREATE TABLE trades(
    ticker TEXT, trade_id TEXT, ts_ms INTEGER, yes_price_units INTEGER,
    no_price_units INTEGER, count_units INTEGER, taker_side TEXT
);
"""


def build_history_db(workspace: Path) -> None:
    """Write a tiny synthetic history.sqlite3 mirroring the read-only backend
    contract: 3 markets (2 finalized/settled, 1 active), trades on 2 of them,
    2 candles."""
    hist_dir = workspace / "history_data"
    hist_dir.mkdir()
    db = sqlite3.connect(hist_dir / "history.sqlite3")
    try:
        db.executescript(HISTORY_SCHEMA)
        db.executemany(
            "INSERT INTO markets(ticker, status) VALUES(?,?)",
            [("M-A", "finalized"), ("M-B", "finalized"), ("M-C", "active")],
        )
        db.executemany(
            "INSERT INTO trades(ticker, trade_id, ts_ms, count_units, taker_side) VALUES(?,?,?,?,?)",
            [("M-A", "t1", 1_000, 1, "yes"), ("M-A", "t2", 5_000, 1, "no"), ("M-B", "t3", 3_000, 2, "yes")],
        )
        db.executemany(
            "INSERT INTO candles(ticker, ts_ms, price_close_units) VALUES(?,?,?)",
            [("M-A", 900, 50), ("M-A", 6_000, 60)],
        )
        db.commit()
    finally:
        db.close()


def build_store(root: Path) -> OperationsStore:
    for name in ("runtime", "logs", "watchdog_state"):
        (root / name).mkdir()
    optimizer_dir = root / "runtime" / "optimizer"
    optimizer_dir.mkdir()
    db = sqlite3.connect(optimizer_dir / "results.sqlite3")
    try:
        db.executescript(RESULTS_SCHEMA)
        db.execute(
            "INSERT INTO opt_runs VALUES(?,?,?,?,?,?,?,?,?)",
            (
                RUN_ID, 1_000, 0, 500_000, json.dumps(["M-A", "M-B", "M-C"]), "{}",
                json.dumps({
                    "candidates": 50, "workers": 8, "markets": 3, "budget_minutes": 15.0,
                    "write_back": True, "seed": 7, "base_params_json": "",
                }),
                "finished", 2_000,
            ),
        )
        final_rows = [
            # candidate, split, score, fills — c-best OOS mean 150, c-mid 50, c-worst -20
            ("c-best", "test0", 100.0, 10), ("c-best", "test1", 200.0, 10), ("c-best", "train0", 40.0, 10),
            ("c-mid", "test0", 50.0, 5), ("c-mid", "test1", 50.0, 5), ("c-mid", "train0", 90.0, 5),
            ("c-worst", "test0", -10.0, 2), ("c-worst", "test1", -30.0, 2), ("c-worst", "train0", -5.0, 2),
        ]
        for candidate_id, split_id, score, fills in final_rows:
            db.execute(
                "INSERT INTO candidates VALUES(?,?,?,?,?,?,?,?,?,?)",
                (RUN_ID, candidate_id, json.dumps({"alpha": candidate_id}), "final", split_id,
                 score, fills, 1.0, 0.5, 0),
            )
        # rung0 decoy with a huge score: must never appear in the final leaderboard.
        db.execute(
            "INSERT INTO candidates VALUES(?,?,?,?,?,?,?,?,?,?)",
            (RUN_ID, "c-decoy", "{}", "rung0", "train0", 999_999.0, 1, 1.0, 0.5, 0),
        )
        db.execute("INSERT INTO sensitivity VALUES(?,?,?,?)", (RUN_ID, "field_a", 5.5, 1))
        db.execute("INSERT INTO sensitivity VALUES(?,?,?,?)", (RUN_ID, "field_b", 2.0, 2))
        db.commit()
    finally:
        db.close()
    (optimizer_dir / f"report_{RUN_ID}.md").write_text(
        f"# Optimizer run `{RUN_ID}`\n\n## Leaderboard\n", encoding="utf-8"
    )
    (optimizer_dir / "ui_run_console.log").write_text(
        "\n".join(f"[optimizer] line-{index:02d}" for index in range(1, 31)) + "\n",
        encoding="utf-8",
    )
    # The fixture models an idle optimizer with historical results. Backdate the
    # results db mtime so OptimizerService._db_recently_written() (which treats a
    # freshly-written db as a live run) reports idle; without this the just-written
    # file would make list_runs()/running report an active run.
    stale = time.time() - 3_600
    os.utime(optimizer_dir / "results.sqlite3", (stale, stale))
    return OperationsStore(Settings(root, root / "runtime", root / "logs", root / "watchdog_state", "test.service"))


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def operations():
    # Deliberately not pytest's tmp_path: the shared pytest-of-admin temp root
    # has broken ACLs on this machine (see tests/test_optimizer.py).
    root = Path(tempfile.mkdtemp(prefix="optimizer-api-test-"))
    try:
        store = build_store(root)
        # Liveness falls back to scanning the real machine for optimizer
        # processes; without this stub these tests would report a genuine
        # background run as this fixture's run.
        store.optimizer._console_optimizer_running = lambda: False  # type: ignore[method-assign]
        yield store
    finally:
        shutil.rmtree(root, ignore_errors=True)


def client(raise_app_exceptions: bool = True) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app_module.app, raise_app_exceptions=raise_app_exceptions)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.anyio
async def test_runs_list_shape_running_flag_and_log_tail(operations):
    app_module.store = operations
    async with client() as http:
        assert (await http.get("/api/v1/optimizer/runs")).status_code == 401
        response = await http.get("/api/v1/optimizer/runs", headers=HEADERS)
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["generatedAt"], int)
    assert body["running"] is False
    assert len(body["items"]) == 1
    run = body["items"][0]
    assert run["id"] == RUN_ID
    assert run["status"] == "finished"
    assert run["startedMs"] == 1_000
    assert run["finishedMs"] == 2_000
    assert run["marketCount"] == 3
    assert run["args"]["candidates"] == 50
    assert run["args"]["workers"] == 8
    assert run["args"]["markets"] == 3
    assert run["args"]["budgetMinutes"] == 15.0
    assert run["args"]["writeBack"] is True
    assert run["args"]["baseParams"] is False
    assert len(body["lastLogLines"]) == 20
    assert body["lastLogLines"][-1] == "[optimizer] line-30"
    assert body["lastLogLines"][0] == "[optimizer] line-11"


@pytest.mark.anyio
async def test_run_detail_leaderboard_ordering_sensitivity_and_report(operations):
    app_module.store = operations
    async with client() as http:
        response = await http.get(f"/api/v1/optimizer/runs/{RUN_ID}", headers=HEADERS)
    assert response.status_code == 200
    body = response.json()
    assert body["run"]["id"] == RUN_ID
    assert body["run"]["marketCount"] == 3
    leaderboard = body["leaderboard"]
    assert [item["candidateId"] for item in leaderboard] == ["c-best", "c-mid", "c-worst"]
    assert leaderboard[0]["scoreUnits"] == pytest.approx(150.0)  # mean of test splits only
    assert leaderboard[0]["trainScoreUnits"] == pytest.approx(40.0)
    assert leaderboard[0]["fills"] == 30
    assert leaderboard[0]["params"] == {"alpha": "c-best"}
    assert all(item["candidateId"] != "c-decoy" for item in leaderboard)
    assert body["sensitivity"][0] == {"field": "field_a", "deltaUnits": 5.5, "rank": 1}
    assert body["sensitivity"][1]["field"] == "field_b"
    assert f"# Optimizer run `{RUN_ID}`" in body["report"]


@pytest.mark.anyio
async def test_run_detail_rejects_path_traversal_and_unknown_ids(operations):
    app_module.store = operations
    async with client(raise_app_exceptions=False) as http:
        traversal = await http.get("/api/v1/optimizer/runs/..%5C..%5Cbase_params_winner", headers=HEADERS)
        dotted = await http.get("/api/v1/optimizer/runs/run.id.with.dots", headers=HEADERS)
        unknown = await http.get("/api/v1/optimizer/runs/does-not-exist", headers=HEADERS)
    assert traversal.status_code in (400, 404)
    assert dotted.status_code in (400, 404)
    assert unknown.status_code == 404


@pytest.mark.anyio
async def test_start_refuses_while_optimizer_process_alive(operations, monkeypatch):
    operations.optimizer.pid_path.write_text("4242\n", encoding="utf-8")
    monkeypatch.setattr(operations.optimizer, "_pid_alive", lambda pid: True)
    app_module.store = operations
    async with client(raise_app_exceptions=False) as http:
        response = await http.post(
            "/api/v1/controls/optimizer/start", headers=HEADERS, json={"candidates": 10},
        )
    assert response.status_code == 409
    audit = operations.audit.list(5)
    assert audit[0]["action"] == "optimizer_start"
    assert audit[0]["result"] == "failed"


@pytest.mark.anyio
async def test_start_validates_bounds(operations):
    app_module.store = operations
    async with client(raise_app_exceptions=False) as http:
        too_many = await http.post(
            "/api/v1/controls/optimizer/start", headers=HEADERS, json={"candidates": 501},
        )
        bad_budget = await http.post(
            "/api/v1/controls/optimizer/start", headers=HEADERS, json={"budgetMinutes": 601},
        )
    assert too_many.status_code == 400
    assert bad_budget.status_code == 400


@pytest.mark.anyio
async def test_stop_without_pid_file_is_graceful(operations):
    app_module.store = operations
    async with client() as http:
        response = await http.post("/api/v1/controls/optimizer/stop", headers=HEADERS, json={})
    assert response.status_code == 200
    body = response.json()
    assert body["result"]["stopped"] is False
    assert body["requestId"]
    audit = operations.audit.list(5)
    assert audit[0]["action"] == "optimizer_stop"
    assert audit[0]["result"] == "success"


def test_data_availability_reports_recorded_history(operations):
    build_history_db(operations.settings.workspace)
    avail = operations.optimizer.data_availability()
    assert avail["available"] is True
    assert avail["marketCount"] == 3
    assert avail["marketsWithTrades"] == 2
    assert avail["settledCount"] == 2  # status='finalized'
    assert avail["tradeCount"] == 3
    assert avail["candleCount"] == 2
    assert avail["fromMs"] == 900  # min over trades (1000) and candles (900)
    assert avail["toMs"] == 6_000  # max over trades (5000) and candles (6000)
    assert avail["spanDays"] == round((6_000 - 900) / 86_400_000, 1)


def test_data_availability_absent_when_history_db_missing(operations):
    # build_store does not create history_data/history.sqlite3.
    assert operations.optimizer.data_availability() == {"available": False}


@pytest.mark.anyio
async def test_data_availability_endpoint(operations):
    build_history_db(operations.settings.workspace)
    app_module.store = operations
    async with client() as http:
        assert (await http.get("/api/v1/optimizer/data-availability")).status_code == 401
        response = await http.get("/api/v1/optimizer/data-availability", headers=HEADERS)
    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["marketCount"] == 3
    assert body["tradeCount"] == 3


def _capture_start_command(operations, monkeypatch, payload) -> list[str]:
    captured: dict[str, list[str]] = {}

    def fake_spawn(command):
        captured["command"] = list(command)
        return 4321

    monkeypatch.setattr(operations.optimizer, "_spawn", fake_spawn)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    operations.optimizer.start(payload, "operator", "req-x")
    return captured["command"]


def test_start_appends_last_days(operations, monkeypatch):
    command = _capture_start_command(operations, monkeypatch, {"lastDays": 7})
    assert command[command.index("--last-days") + 1] == "7"
    assert "--from-date" not in command
    assert "--to-date" not in command


def test_start_appends_custom_date_range(operations, monkeypatch):
    command = _capture_start_command(
        operations, monkeypatch, {"fromDate": "2026-08-25", "toDate": "2026-08-31"}
    )
    assert command[command.index("--from-date") + 1] == "2026-08-25"
    assert command[command.index("--to-date") + 1] == "2026-08-31"
    assert "--last-days" not in command


def test_start_omits_time_range_when_not_requested(operations, monkeypatch):
    command = _capture_start_command(operations, monkeypatch, {"candidates": 10})
    assert "--last-days" not in command
    assert "--from-date" not in command
    assert "--to-date" not in command


def test_start_rejects_malformed_from_date(operations, monkeypatch):
    monkeypatch.setattr(operations.optimizer, "_spawn", lambda command: 1)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    with pytest.raises(ValueError):
        operations.optimizer.start({"fromDate": "nonsense"}, "operator", "req-bad")
    audit = operations.audit.list(5)
    assert audit[0]["action"] == "optimizer_start"
    assert audit[0]["result"] == "failed"


# ---------------------------------------------------------------------------
# Extended start payload: tier / class / only-params / sessions / screener
# ---------------------------------------------------------------------------

TOXICITY_FIELDS = ["default_toxicity_cents", "minimum_expected_edge_cents_to_quote"]


def _flag(command: list[str], flag: str) -> str:
    return command[command.index(flag) + 1]


def _start(operations, monkeypatch, payload) -> tuple[list[str], dict]:
    captured: dict[str, list[str]] = {}

    def fake_spawn(command):
        captured["command"] = list(command)
        return 4321

    monkeypatch.setattr(operations.optimizer, "_spawn", fake_spawn)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    response = operations.optimizer.start(payload, "operator", "req-ext")
    return captured["command"], response


def test_start_maps_every_new_field_to_argv(operations, monkeypatch):
    # "Default" is bootstrapped by SessionStore; a second named session proves
    # the name (not the id) reaches the CLI verbatim, spaces included.
    operations.sessions.create_session({"name": "base one", "description": "", "configuration": None})
    payload = {
        "tier": 1, "marketClass": "toxic", "onlyParams": TOXICITY_FIELDS, "topParams": 5,
        "workers": 12, "splits": 4, "fillShare": 0.25, "seed": 42,
        "baseSession": "base one", "screenerSession": "Default", "screenerFilter": False,
        "screenerEvalHours": 6, "writeBack": True, "candidates": 40, "markets": 20,
        "budgetMinutes": 30, "lastDays": 3,
    }
    command, response = _start(operations, monkeypatch, payload)
    assert command[1:3] == ["-m", "optimizer.main"]
    assert _flag(command, "--tier") == "1"
    assert "--record-root" not in command
    assert _flag(command, "--class") == "toxic"
    assert _flag(command, "--only-params") == ",".join(TOXICITY_FIELDS)
    assert _flag(command, "--top-params") == "5"
    assert _flag(command, "--workers") == "12"
    assert _flag(command, "--splits") == "4"
    assert _flag(command, "--fill-share") == "0.25"
    assert _flag(command, "--seed") == "42"
    assert _flag(command, "--base-session") == "base one"
    assert _flag(command, "--screener-session") == "Default"
    assert "--no-screener-filter" in command
    assert _flag(command, "--screener-eval-hours") == "6"
    assert "--write-back" in command
    assert _flag(command, "--candidates") == "40"
    assert _flag(command, "--markets") == "20"
    assert _flag(command, "--budget-minutes") == "30.0"
    assert _flag(command, "--last-days") == "3"
    # Base session given and no explicit file: the session's bot fields are
    # exported and passed as --base-params-json so the search is seeded from it.
    exported = Path(_flag(command, "--base-params-json"))
    assert exported.parent == operations.optimizer.optimizer_dir
    assert exported.name.startswith("base_params_") and exported.suffix == ".json"
    seed = json.loads(exported.read_text(encoding="utf-8"))
    assert "default_toxicity_cents" in seed
    result = response["result"]
    assert result["started"] is True and result["queued"] is False
    assert result["command"] == command
    assert "--only-params" in result["commandLine"] and '"base one"' in result["commandLine"]
    journal = operations.optimizer.launch_journal.read_text(encoding="utf-8").strip().splitlines()
    assert json.loads(journal[-1])["commandLine"] == result["commandLine"]


def test_start_defaults_workers_to_8_and_screener_filter_on(operations, monkeypatch):
    command, response = _start(operations, monkeypatch, {"candidates": 10})
    assert _flag(command, "--workers") == "8"
    assert _flag(command, "--tier") == "1"
    assert _flag(command, "--top-params") == "25"
    assert "--no-screener-filter" not in command
    assert "--class" not in command and "--only-params" not in command
    assert "--base-session" not in command and "--base-params-json" not in command
    assert response["result"]["workers"] == 8


def test_start_caps_workers_at_12(operations, monkeypatch):
    monkeypatch.setattr(operations.optimizer, "_spawn", lambda command: 1)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    with pytest.raises(ValueError, match="workers"):
        operations.optimizer.start({"workers": 13}, "operator", "req-w")
    command, _ = _start(operations, monkeypatch, {"workers": 12})
    assert _flag(command, "--workers") == "12"


def test_start_tier2_needs_a_recording_and_passes_record_root(operations, monkeypatch):
    monkeypatch.setattr(operations.optimizer, "_spawn", lambda command: 1)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    with pytest.raises(ValueError, match="recordRoot"):
        operations.optimizer.start({"tier": 2}, "operator", "req-t2")
    (operations.settings.workspace / "record_data").mkdir()
    command, _ = _start(operations, monkeypatch, {"tier": 2})
    assert _flag(command, "--tier") == "2"
    assert _flag(command, "--record-root") == "record_data"
    # Tier-2-only fields are searchable at tier 2 ...
    command, _ = _start(operations, monkeypatch, {"tier": 2, "onlyParams": ["minimum_top_level_depth_contracts"]})
    assert _flag(command, "--only-params") == "minimum_top_level_depth_contracts"


@pytest.mark.parametrize("payload,fragment", [
    ({"tier": 3}, "tier"),
    ({"marketClass": "bogus"}, "marketClass"),
    ({"onlyParams": ["not_a_field"]}, "unknown field"),
    ({"onlyParams": ["minimum_top_level_depth_contracts"]}, "awaiting Tier-2"),  # pinned at tier 1, reason surfaced
    ({"onlyParams": "a,b"}, "onlyParams"),
    ({"onlyParams": 5}, "onlyParams"),
    ({"topParams": 0}, "topParams"),
    ({"workers": 0}, "workers"),
    ({"splits": 11}, "splits"),
    ({"fillShare": 1.5}, "fillShare"),
    ({"fillShare": 0}, "fillShare"),
    ({"seed": -1}, "seed"),
    ({"seed": 1.5}, "seed"),
    ({"baseSession": "no-such-session"}, "baseSession"),
    ({"screenerSession": "no-such-session"}, "screenerSession"),
    ({"screenerFilter": "yes"}, "screenerFilter"),
    ({"screenerEvalHours": -1}, "screenerEvalHours"),
    ({"screenerEvalHours": 200}, "screenerEvalHours"),
    ({"recordRoot": ""}, "recordRoot"),
    ({"baseParamsJson": "missing.json"}, "baseParamsJson"),
    ({"fromDate": "2026-09-02", "toDate": "2026-09-01"}, "fromDate"),
    ({"queueAfterCurrent": "true"}, "queueAfterCurrent"),
])
def test_start_validates_new_fields(operations, monkeypatch, payload, fragment):
    monkeypatch.setattr(operations.optimizer, "_spawn", lambda command: 1)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    with pytest.raises(ValueError, match=fragment):
        operations.optimizer.start(payload, "operator", "req-v")
    assert operations.audit.list(1)[0]["result"] == "failed"


def test_start_explicit_base_params_file_wins_over_session_export(operations, monkeypatch):
    explicit = operations.optimizer.optimizer_dir / "seed.json"
    explicit.write_text(json.dumps({"default_toxicity_cents": 3}), encoding="utf-8")
    command, _ = _start(operations, monkeypatch, {"baseSession": "Default", "baseParamsJson": "runtime/optimizer/seed.json"})
    assert Path(_flag(command, "--base-params-json")) == explicit
    assert _flag(command, "--base-session") == "Default"
    assert not list(operations.optimizer.optimizer_dir.glob("base_params_2*.json"))


def test_use_base_params_keeps_seeding_from_last_winner_only_without_base_session(operations, monkeypatch):
    winner = operations.optimizer.base_params_path
    winner.write_text("{}", encoding="utf-8")
    command, _ = _start(operations, monkeypatch, {"useBaseParams": True})
    assert Path(_flag(command, "--base-params-json")) == winner
    command, _ = _start(operations, monkeypatch, {"useBaseParams": False})
    assert "--base-params-json" not in command
    command, _ = _start(operations, monkeypatch, {"useBaseParams": True, "baseSession": "Default"})
    assert Path(_flag(command, "--base-params-json")) != winner


def test_previous_session_fails_fast_when_nothing_written_back(operations, monkeypatch):
    monkeypatch.setattr(operations.optimizer, "_spawn", lambda command: 1)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    with pytest.raises(ValueError, match=r"\$previous"):
        operations.optimizer.start({"baseSession": "$previous"}, "operator", "req-prev")


# ---------------------------------------------------------------------------
# /params and /options
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_params_endpoint_lists_searchable_and_pinned_fields_per_tier(operations):
    app_module.store = operations
    async with client(raise_app_exceptions=False) as http:
        assert (await http.get("/api/v1/optimizer/params")).status_code == 401
        tier1 = await http.get("/api/v1/optimizer/params?tier=1", headers=HEADERS)
        tier2 = await http.get("/api/v1/optimizer/params?tier=2", headers=HEADERS)
        bad = await http.get("/api/v1/optimizer/params?tier=3", headers=HEADERS)
    assert tier1.status_code == 200 and tier2.status_code == 200 and bad.status_code == 422
    body = tier1.json()
    assert body["tier"] == 1
    names = {item["name"] for item in body["searchable"]}
    assert "default_toxicity_cents" in names
    dim = next(item for item in body["searchable"] if item["name"] == "default_toxicity_cents")
    assert set(dim) >= {"name", "kind", "low", "high", "default", "group"}
    assert dim["kind"] in ("int", "float") and dim["low"] <= dim["high"]
    pinned = {item["name"]: item["reason"] for item in body["pinned"]}
    assert "awaiting Tier-2" in pinned["minimum_top_level_depth_contracts"]
    assert "pinned by policy" in pinned["enable_sqlite_telemetry"]
    assert body["presets"]["toxicity"] and set(body["presets"]["toxicity"]) <= names
    assert body["presets"]["priceFloors"] and set(body["presets"]["priceFloors"]) <= names
    assert "Toxicity & edge" in body["groups"]
    tier2_names = {item["name"] for item in tier2.json()["searchable"]}
    assert "minimum_top_level_depth_contracts" in tier2_names
    assert "minimum_top_level_depth_contracts" not in {p["name"] for p in tier2.json()["pinned"]}


def _write_recording(workspace: Path, day: str, hour: str, tickers: list[str]) -> None:
    hour_dir = workspace / "record_data" / day / hour
    hour_dir.mkdir(parents=True, exist_ok=True)
    (hour_dir / "conn0.jsonl.gz").write_bytes(b"\x1f\x8b")
    (hour_dir / "meta.json").write_text(json.dumps({"connections": {"conn0": tickers}}), encoding="utf-8")


@pytest.mark.anyio
async def test_options_endpoint_reports_classes_sessions_tiers_and_coverage(operations):
    operations.sessions.create_session({"name": "chain-1", "description": "", "configuration": None})
    app_module.store = operations
    async with client() as http:
        before = await http.get("/api/v1/optimizer/options", headers=HEADERS)
    assert before.status_code == 200
    body = before.json()
    assert body["classes"] == ["toxic", "thinWide", "thickCalm", "default"]
    sessions = {item["name"]: item for item in body["sessions"]}
    assert {"Default", "chain-1"} <= set(sessions)
    assert sessions["chain-1"]["hasScreener"] is True
    assert [tier["tier"] for tier in body["tiers"]] == [1, 2]
    assert body["tiers"][0]["available"] is False  # no history db in this fixture
    assert body["tiers"][1]["available"] is False and body["recorder"]["available"] is False
    assert body["workers"] == {"default": 8, "max": 12}
    assert body["previousSessionToken"] == "$previous"
    assert body["commandPrefix"][1:3] == ["-m", "optimizer.main"]

    _write_recording(operations.settings.workspace, "20260901", "05", ["M-A", "M-B"])
    _write_recording(operations.settings.workspace, "20260901", "06", ["M-B", "M-C"])
    operations.optimizer._coverage_cache = None
    build_history_db(operations.settings.workspace)
    async with client() as http:
        after = (await http.get("/api/v1/optimizer/options", headers=HEADERS)).json()
    assert after["tiers"][0]["available"] is True
    assert after["tiers"][1]["available"] is True
    recorder = after["recorder"]
    assert recorder["hours"] == 2 and recorder["days"] == 1 and recorder["markets"] == 3
    assert recorder["fromMs"] == 1_788_238_800_000  # 2026-09-01T05:00Z
    assert recorder["toMs"] == recorder["fromMs"] + 2 * 3_600_000


# ---------------------------------------------------------------------------
# Queue: launch after the current run, chained through $previous
# ---------------------------------------------------------------------------

def _fake_active(operations, monkeypatch, active: bool) -> None:
    monkeypatch.setattr(operations.optimizer, "running", lambda: active)
    monkeypatch.setattr(operations.optimizer, "_optimizer_active", lambda: active)
    monkeypatch.setattr(operations.optimizer, "_console_optimizer_running", lambda: False)
    monkeypatch.setattr(operations.optimizer, "_ensure_queue_thread", lambda: None)


def test_queue_after_current_records_and_launches_with_previous_session(operations, monkeypatch):
    captured: list[list[str]] = []
    monkeypatch.setattr(operations.optimizer, "_spawn", lambda command: captured.append(list(command)) or 777)
    _fake_active(operations, monkeypatch, True)
    response = operations.optimizer.start(
        {"queueAfterCurrent": True, "baseSession": "$previous", "onlyParams": TOXICITY_FIELDS, "workers": 4},
        "operator", "req-q1",
    )
    result = response["result"]
    assert result["started"] is False and result["queued"] is True
    assert result["position"] == 1 and result["queueId"].startswith("q-")
    assert "--base-session $previous" in result["commandLine"]
    listed = operations.optimizer.list_runs()["queue"]
    assert [item["status"] for item in listed] == ["queued"]
    assert listed[0]["id"] == result["queueId"] and listed[0]["position"] == 1
    assert listed[0]["options"]["onlyParams"] == TOXICITY_FIELDS

    # Still running: the poller leaves the item alone.
    assert operations.optimizer.process_queue() is None
    assert captured == []

    # The run in the fixture finished and wrote back a session (description
    # carries the run id, as optimizer.writeback does).
    written = operations.sessions.create_session({
        "name": "optimized-2026", "description": f"optimizer run {RUN_ID}, window 0..500000 ms", "configuration": None,
    })
    _fake_active(operations, monkeypatch, False)
    handled = operations.optimizer.process_queue()
    assert handled["status"] == "launched" and handled["pid"] == 777
    assert len(captured) == 1
    command = captured[0]
    assert _flag(command, "--base-session") == written["name"]
    assert _flag(command, "--workers") == "4"
    assert Path(_flag(command, "--base-params-json")).exists()
    assert operations.optimizer.list_runs()["queue"][0]["status"] == "launched"
    audit = operations.audit.list(3)
    assert audit[0]["action"] == "optimizer_queue_launch" and audit[0]["result"] == "success"
    assert audit[0]["target"] == result["queueId"]
    # Nothing left to launch.
    assert operations.optimizer.process_queue() is None


def test_queue_preserves_order_and_failed_items_do_not_block_the_rest(operations, monkeypatch):
    launched: list[list[str]] = []
    monkeypatch.setattr(operations.optimizer, "_spawn", lambda command: launched.append(list(command)) or 1)
    _fake_active(operations, monkeypatch, True)
    first = operations.optimizer.start({"queueAfterCurrent": True, "baseSession": "$previous"}, "operator", "req-a")
    second = operations.optimizer.start({"queueAfterCurrent": True, "candidates": 7}, "operator", "req-b")
    assert second["result"]["position"] == 2
    _fake_active(operations, monkeypatch, False)
    # No run wrote back a session: the first item fails, the second still launches.
    failed = operations.optimizer.process_queue()
    assert failed["id"] == first["result"]["queueId"] and failed["status"] == "failed"
    assert "$previous" in failed["error"]
    ok = operations.optimizer.process_queue()
    assert ok["id"] == second["result"]["queueId"] and ok["status"] == "launched"
    assert _flag(launched[0], "--candidates") == "7"
    statuses = {item["id"]: item["status"] for item in operations.optimizer.list_runs()["queue"]}
    assert statuses == {first["result"]["queueId"]: "failed", second["result"]["queueId"]: "launched"}


def test_queue_after_current_launches_immediately_when_idle(operations, monkeypatch):
    command, response = _start(operations, monkeypatch, {"queueAfterCurrent": True, "candidates": 12})
    assert response["result"]["started"] is True and response["result"]["queued"] is False
    assert _flag(command, "--candidates") == "12"


@pytest.mark.anyio
async def test_queue_cancel_endpoint_and_stop_clears_queue(operations, monkeypatch):
    _fake_active(operations, monkeypatch, True)
    app_module.store = operations
    async with client(raise_app_exceptions=False) as http:
        queued = await http.post("/api/v1/controls/optimizer/start", headers=HEADERS, json={"queueAfterCurrent": True})
        assert queued.status_code == 200
        queue_id = queued.json()["result"]["queueId"]
        runs = (await http.get("/api/v1/optimizer/runs", headers=HEADERS)).json()
        assert runs["queue"][0]["id"] == queue_id
        cancelled = await http.post(f"/api/v1/controls/optimizer/queue/{queue_id}/cancel", headers=HEADERS)
        assert cancelled.status_code == 200
        assert cancelled.json()["result"]["status"] == "cancelled"
        missing = await http.post(f"/api/v1/controls/optimizer/queue/{queue_id}/cancel", headers=HEADERS)
        assert missing.status_code == 404
        bad = await http.post("/api/v1/controls/optimizer/queue/not-an-id/cancel", headers=HEADERS)
        assert bad.status_code == 400
        # Stop halts the chain: a queued item is cancelled rather than launched later.
        again = await http.post("/api/v1/controls/optimizer/start", headers=HEADERS, json={"queueAfterCurrent": True})
        assert again.status_code == 200
        _fake_active(operations, monkeypatch, False)
        stopped = await http.post("/api/v1/controls/optimizer/stop", headers=HEADERS, json={})
    assert stopped.status_code == 200
    assert stopped.json()["result"]["queueCleared"] == 1
    statuses = [item["status"] for item in operations.optimizer.list_runs()["queue"]]
    assert statuses.count("cancelled") == 2 and "queued" not in statuses


# ---------------------------------------------------------------------------
# Run listing / detail: key arguments and the full command line
# ---------------------------------------------------------------------------

def test_args_summary_exposes_targeted_run_arguments():
    args = {
        "tier": 1, "only_params": "a,b", "market_class": "toxic", "base_session": "s1", "screener_session": "",
        "screener_filter": {"enabled": True, "source": "session 's1' (--base-session)", "total": 10, "passed": 4},
        "top_params": 5, "splits": 3, "fill_share": 0.5, "last_days": 7.0, "from_date": "", "to_date": "",
        "workers": 12, "candidates": 80, "markets": 80, "budget_minutes": 120.0, "write_back": True,
        "base_params_json": "x.json", "data_source": {"tier": 1},
    }
    view = operations_args(args)
    assert view["tier"] == 1 and view["marketClass"] == "toxic"
    assert view["onlyParams"] == ["a", "b"]
    assert view["baseSession"] == "s1" and view["screenerSession"] is None
    assert view["screenerFilter"] is True and view["screenerSource"] == "session 's1' (--base-session)"
    assert view["topParams"] == 5 and view["fillShare"] == 0.5 and view["lastDays"] == 7.0
    assert view["fromDate"] is None and view["baseParams"] is True and view["recordRoot"] is None
    tier2 = operations_args({"tier": 2, "record_root": "record_data", "data_source": {"tier": 2, "record_root": "record_data"}, "screener_filter": False})
    assert tier2["tier"] == 2 and tier2["recordRoot"] == "record_data" and tier2["screenerFilter"] is False
    legacy = operations_args({"candidates": 50})
    assert legacy["tier"] == 1 and legacy["onlyParams"] == [] and legacy["screenerFilter"] is None


def operations_args(args: dict) -> dict:
    from ui_api.store import OptimizerService

    return OptimizerService._args_summary(json.dumps(args))


@pytest.mark.anyio
async def test_run_detail_includes_full_args_and_launch_command_line(operations):
    operations.optimizer._journal({
        "launchId": "L1", "requestId": "req-1", "operator": "operator", "launchedMs": 400, "pid": 5,
        "command": ["python", "-m", "optimizer.main", "--candidates", "50"],
        "commandLine": "python -m optimizer.main --candidates 50",
    })
    operations.optimizer._journal({"launchId": "L2", "launchedMs": 900_000, "commandLine": "later launch"})
    app_module.store = operations
    async with client() as http:
        runs = (await http.get("/api/v1/optimizer/runs", headers=HEADERS)).json()
        detail = (await http.get(f"/api/v1/optimizer/runs/{RUN_ID}", headers=HEADERS)).json()
    assert runs["items"][0]["args"]["tier"] == 1
    assert runs["items"][0]["args"]["onlyParams"] == []
    assert runs["queue"] == []
    assert detail["argsFull"]["candidates"] == 50 and detail["argsFull"]["seed"] == 7
    assert detail["commandLine"] == "python -m optimizer.main --candidates 50"  # launched 600 ms before started_ms=1000
    assert detail["launch"]["launchId"] == "L1" and detail["launch"]["requestId"] == "req-1"
