"""Historical screener filter: the optimizer trains on the fleet's market universe.

A synthetic ``HistoryStore`` holds one market per live-screener filter so every
reason is exercised, plus the 24 h volume window, keyword exclusion, the
disable flag and the CLI / args_json wiring.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from history.store import HistoryStore
from optimizer import data as data_mod
from optimizer import main as optimizer_main

BASE = 1_700_000_040_000  # minute-aligned like Kalshi candle period ends
assert BASE % 60_000 == 0
T0 = BASE + data_mod.DAY_MS  # evaluation time: 24 h of prints can precede it
CANDLE_STEP = 5 * 60_000
DEFAULT_CLOSE = T0 + 24 * data_mod.HOUR_MS
GOOD_BOOK = (4_000, 5_000)  # yes bid 40c / yes ask 50c -> no bid 50c, spread 10c
GOOD_OI_UNITS = 500 * 100   # 500 contracts
GOOD_VOLUME_CONTRACTS = 1_000

# ticker -> (book (bid, ask) units, oi units, close ts, volume contracts, volume ts, candle range)
MARKETS = {
    "GOOD-1": dict(),
    "TIGHT-1": dict(book=(4_500, 4_700)),            # spread 2c < 4
    "WIDE-1": dict(book=(2_000, 6_000)),             # spread 40c > 35
    "LOWYES-1": dict(book=(300, 1_000)),             # yes bid 3c < 5 (spread 7c ok)
    "LOWNO-1": dict(book=(9_000, 9_700)),            # no bid 3c < 5
    "NOBOOK-1": dict(book=(0, 5_000)),               # yes bid 0 -> no valid book
    "THINVOL-1": dict(volume=100),                   # 100 contracts / 24 h < 500
    "THINOI-1": dict(oi=50 * 100),                   # 50 contracts < 100
    "SOON-1": dict(close=T0 + 1 * data_mod.HOUR_MS),     # 1 h to close < 3
    "LATE-1": dict(close=T0 + 100 * data_mod.HOUR_MS),   # 100 h to close > 50
    "CLOSED-1": dict(close=T0 - 1 * data_mod.HOUR_MS),   # already closed at T0
    "KXHIGHNY-1": dict(),                            # keyword "HIGH" when configured
    "KXMVECROSSCATEGORY-1": dict(),                  # multivariate combo series
    "STALE-1": dict(volume_ts=T0 - 25 * data_mod.HOUR_MS),   # prints older than 24 h
    "EDGE-IN-1": dict(volume_ts=T0 - data_mod.DAY_MS + 60_000),  # bucket starts at eval-24h+1min
    "EDGE-OUT-1": dict(volume_ts=T0 - data_mod.DAY_MS - 10),    # all ten prints in the minute before eval-24h
    "LATER-1": dict(candles=(T0 + 2 * data_mod.HOUR_MS, T0 + 20 * data_mod.HOUR_MS),
                    volume_ts=T0 + 2 * data_mod.HOUR_MS + 1),  # not live at T0, passes at T0+3h
}
# Markets passing at SOME hourly checkpoint over the two recorded days.
HOURLY_PASSING = {"GOOD-1", "KXHIGHNY-1", "EDGE-IN-1", "EDGE-OUT-1", "LATER-1", "STALE-1", "SOON-1", "CLOSED-1"}


def _trade(trade_id, ts_ms, count_units, price_units=4_500, taker_side="yes"):
    return SimpleNamespace(
        trade_id=trade_id, timestamp_ms=ts_ms, yes_price_units=price_units,
        no_price_units=10_000 - price_units, count_units=count_units, taker_side=taker_side,
    )


def make_history(path: Path) -> None:
    store = HistoryStore(path)
    for ticker, spec in MARKETS.items():
        bid, ask = spec.get("book", GOOD_BOOK)
        close_ts = spec.get("close", DEFAULT_CLOSE)
        first, last = spec.get("candles", (BASE, min(close_ts, BASE + 2 * data_mod.DAY_MS)))
        store.upsert_market(
            ticker=ticker, series=ticker.split("-")[0], title=ticker, status="settled",
            close_ts_ms=close_ts, result="yes", volume_24h_units=None, oi_units=None,
        )
        store.insert_candles(ticker, [
            {
                "ts_ms": ts, "yes_bid_close_units": bid, "yes_ask_close_units": ask,
                "oi_units": spec.get("oi", GOOD_OI_UNITS), "volume_units": 100,
            }
            for ts in range(first, last + 1, CANDLE_STEP)
        ])
        volume_ts = spec.get("volume_ts", T0 - 23 * data_mod.HOUR_MS)
        contracts = spec.get("volume", GOOD_VOLUME_CONTRACTS)
        # ten prints so the trade-count ranking differs from ticker order; count_units are contracts * 100
        store.insert_trades(ticker, [
            _trade(f"{ticker}-{i}", volume_ts + i, contracts * 100 // 10) for i in range(10)
        ])
    # extra prints (24 h before T0 is far in the past for them) make GOOD-1 the most-traded market
    store.insert_trades("GOOD-1", [_trade(f"GOOD-1-x{i}", BASE - 3 * data_mod.DAY_MS + i, 100) for i in range(50)])
    store.close()


@pytest.fixture()
def tmp_dir():
    # Deliberately not pytest's tmp_path: the shared pytest-of-admin temp root
    # has broken ACLs on this machine and errors before any test runs.
    path = Path(tempfile.mkdtemp(prefix="optimizer-screener-test-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture()
def history(tmp_dir):
    path = tmp_dir / "history.sqlite3"
    make_history(path)
    return str(path)


# --- settings ----------------------------------------------------------------

def test_normalize_settings_accepts_constant_snake_and_camel_names():
    defaults = data_mod.default_screener_settings()
    import kalshi_screener_config as config

    assert defaults["min_spread_cents"] == float(config.MIN_SPREAD_CENTS)
    assert defaults["max_time_to_close_hrs"] == float(config.MAX_TIME_TO_CLOSE_HRS)
    assert defaults["mve_filter"] == config.MVE_FILTER

    constants = data_mod.normalize_screener_settings({"MIN_SPREAD_CENTS": 7, "EXCLUDED_TICKER_KEYWORDS": ["lowt"]})
    snake = data_mod.normalize_screener_settings({"min_spread_cents": 7, "excluded_ticker_keywords": "lowt"})
    camel = data_mod.normalize_screener_settings({"minSpreadCents": 7, "excludedTickerKeywords": ["LOWT"], "topN": 5})
    assert constants == snake == camel
    assert camel["min_spread_cents"] == 7.0
    assert camel["excluded_ticker_keywords"] == ["LOWT"]
    assert camel["min_vol24h"] == defaults["min_vol24h"]  # missing keys keep the constants
    assert data_mod.normalize_screener_settings({"maxSpreadCents": None})["max_spread_cents"] is None

    assert data_mod.screener_settings_from_configuration({"bot": {}}) is None
    assert data_mod.screener_settings_from_configuration(None) is None
    from_session = data_mod.screener_settings_from_configuration({"screener": {"minVol24h": 50}})
    assert from_session["min_vol24h"] == 50.0


# --- one market per filter -------------------------------------------------------

def test_each_filter_reports_its_reason(history):
    verdicts = data_mod.evaluate_screener_at(history, {}, T0)
    reasons = {ticker: verdict.reason for ticker, verdict in verdicts.items()}
    assert reasons == {
        "GOOD-1": data_mod.REASON_PASS,
        "TIGHT-1": data_mod.REASON_SPREAD_MIN,
        "WIDE-1": data_mod.REASON_SPREAD_MAX,
        "LOWYES-1": data_mod.REASON_YES_BID,
        "LOWNO-1": data_mod.REASON_NO_BID,
        "NOBOOK-1": data_mod.REASON_NO_BOOK,
        "THINVOL-1": data_mod.REASON_VOL24H,
        "THINOI-1": data_mod.REASON_OI,
        "SOON-1": data_mod.REASON_TOO_SOON,
        "LATE-1": data_mod.REASON_TOO_LATE,
        "CLOSED-1": data_mod.REASON_CLOSED,
        "KXHIGHNY-1": data_mod.REASON_PASS,          # no keywords configured by default
        "KXMVECROSSCATEGORY-1": data_mod.REASON_MVE,
        "STALE-1": data_mod.REASON_VOL24H,
        "EDGE-IN-1": data_mod.REASON_PASS,
        "EDGE-OUT-1": data_mod.REASON_VOL24H,
        "LATER-1": data_mod.REASON_NO_CANDLE,
    }
    good = verdicts["GOOD-1"]
    assert good.passed and good.eval_ms == T0
    assert good.spread_cents == 10.0 and good.yes_bid_cents == 40.0 and good.no_bid_cents == 50.0
    assert good.vol24h_contracts == pytest.approx(GOOD_VOLUME_CONTRACTS)
    assert good.oi_contracts == pytest.approx(500.0)
    assert good.hours_to_close == pytest.approx(24.0)
    assert good.candle_ts_ms == T0
    assert verdicts["WIDE-1"].spread_cents == 40.0
    assert verdicts["THINOI-1"].oi_contracts == pytest.approx(50.0)
    assert verdicts["SOON-1"].hours_to_close == pytest.approx(1.0)


def test_each_threshold_can_be_relaxed_or_tightened_individually(history):
    def reason(ticker, **settings):
        return data_mod.evaluate_screener_at(history, settings, T0)[ticker].reason

    assert reason("TIGHT-1", min_spread_cents=2) == data_mod.REASON_PASS
    assert reason("WIDE-1", max_spread_cents=None) == data_mod.REASON_PASS
    assert reason("WIDE-1", max_spread_cents=40) == data_mod.REASON_PASS
    assert reason("LOWYES-1", min_yes_bid_cents=3) == data_mod.REASON_PASS
    assert reason("LOWNO-1", min_no_bid_cents=3) == data_mod.REASON_PASS
    assert reason("THINVOL-1", min_vol24h=100) == data_mod.REASON_PASS
    assert reason("THINOI-1", min_oi=50) == data_mod.REASON_PASS
    assert reason("SOON-1", min_time_to_close_hrs=1) == data_mod.REASON_PASS
    assert reason("LATE-1", max_time_to_close_hrs=None) == data_mod.REASON_PASS
    assert reason("KXMVECROSSCATEGORY-1", mve_filter="") == data_mod.REASON_PASS
    assert reason("GOOD-1", excluded_series=["GOOD"]) == data_mod.REASON_SERIES
    # tightening turns the good market away for the right reason
    assert reason("GOOD-1", min_spread_cents=11) == data_mod.REASON_SPREAD_MIN
    assert reason("GOOD-1", min_vol24h=1_001) == data_mod.REASON_VOL24H
    assert reason("GOOD-1", min_time_to_close_hrs=25) == data_mod.REASON_TOO_SOON
    assert reason("GOOD-1", max_time_to_close_hrs=23) == data_mod.REASON_TOO_LATE
    # a market that is already closed never passes whatever the thresholds
    assert reason("CLOSED-1", min_time_to_close_hrs=0) == data_mod.REASON_CLOSED


def test_keyword_exclusion_is_a_case_insensitive_ticker_substring(history):
    with_keyword = data_mod.evaluate_screener_at(history, {"excludedTickerKeywords": ["high"]}, T0)
    assert with_keyword["KXHIGHNY-1"].reason == data_mod.REASON_KEYWORD
    assert with_keyword["GOOD-1"].passed
    constants = data_mod.evaluate_screener_at(history, {"EXCLUDED_TICKER_KEYWORDS": ["NY-"]}, T0)
    assert constants["KXHIGHNY-1"].reason == data_mod.REASON_KEYWORD
    assert data_mod.evaluate_screener_at(history, {"excluded_ticker_keywords": ["RAIN"]}, T0)["KXHIGHNY-1"].passed


def test_24h_volume_window_counts_only_the_preceding_day(history):
    verdicts = data_mod.evaluate_screener_at(history, {}, T0)
    assert verdicts["GOOD-1"].vol24h_contracts == pytest.approx(GOOD_VOLUME_CONTRACTS)
    assert verdicts["STALE-1"].vol24h_contracts == 0.0                     # prints 25 h old
    assert verdicts["EDGE-IN-1"].vol24h_contracts == pytest.approx(GOOD_VOLUME_CONTRACTS)
    assert verdicts["EDGE-OUT-1"].vol24h_contracts == 0.0                  # bucket before eval-24h
    # An hour earlier the STALE prints (25 h before T0) are inside the window.
    earlier = data_mod.evaluate_screener_at(history, {}, T0 - 2 * data_mod.HOUR_MS)
    assert earlier["STALE-1"].vol24h_contracts == pytest.approx(GOOD_VOLUME_CONTRACTS)
    assert earlier["STALE-1"].passed
    # Prints at/after the evaluation time never count (no look-ahead).
    assert verdicts["LATER-1"].vol24h_contracts == 0.0


# --- windows: any-checkpoint pass and ranking ---------------------------------------

def test_screen_history_windows_keeps_trade_count_ranking_within_passing_set(history):
    windows = data_mod.load_market_windows(history)
    assert windows[0][0] == "GOOD-1"  # most-traded market ranks first
    report = data_mod.screen_history_windows(history, {}, windows, [T0, T0 + 3 * data_mod.HOUR_MS])
    assert report.eval_ms == [T0, T0 + 3 * data_mod.HOUR_MS]
    assert report.total == len(MARKETS)
    assert set(report.passing) == {"GOOD-1", "KXHIGHNY-1", "EDGE-IN-1", "LATER-1"}
    assert report.verdicts["LATER-1"].eval_ms == T0 + 3 * data_mod.HOUR_MS  # first passing checkpoint
    assert report.verdicts["GOOD-1"].eval_ms == T0
    # failures report the reason from the latest checkpoint at which the market was live
    assert report.verdicts["TIGHT-1"].reason == data_mod.REASON_SPREAD_MIN
    assert report.verdicts["CLOSED-1"].reason == data_mod.REASON_CLOSED
    kept = data_mod.passing_windows(report, windows)
    assert kept[0][0] == "GOOD-1"
    assert [w[0] for w in kept] == [w[0] for w in windows if w[0] in set(report.passing)]
    counts = report.reason_counts()
    assert counts[data_mod.REASON_VOL24H] == 3  # THINVOL, STALE, EDGE-OUT
    summary = report.summary()
    assert summary["passed"] == 4 and summary["total"] == len(MARKETS)
    assert summary["failed"]["WIDE-1"] == data_mod.REASON_SPREAD_MAX
    assert summary["first_pass_ms"]["LATER-1"] == T0 + 3 * data_mod.HOUR_MS
    json.dumps(summary)  # args_json-safe


def test_checkpoints_cover_window_start_split_boundaries_and_cadence():
    points = data_mod.screener_checkpoints(0, 4 * data_mod.HOUR_MS, splits=3, interval_hours=0)
    assert points == [0, data_mod.HOUR_MS, 2 * data_mod.HOUR_MS, 3 * data_mod.HOUR_MS]
    dense = data_mod.screener_checkpoints(0, 4 * data_mod.HOUR_MS, splits=1, interval_hours=0.5)
    assert dense == [i * 30 * 60_000 for i in range(8)]
    assert data_mod.screener_checkpoints(5, 5) == [5]


# --- CLI wiring ------------------------------------------------------------------------

def _args(history, *extra):
    return optimizer_main.build_arg_parser().parse_args(["--history", history, *extra])


def test_cli_flags_parse_with_screener_filter_on_by_default(history):
    args = _args(history)
    assert args.screener_filter is True
    assert args.screener_session == ""
    assert args.screener_eval_hours == data_mod.DEFAULT_SCREENER_EVAL_HOURS
    off = _args(history, "--no-screener-filter", "--screener-session", "live-v2", "--screener-eval-hours", "0")
    assert off.screener_filter is False and off.screener_session == "live-v2" and off.screener_eval_hours == 0.0


def test_disable_flag_keeps_every_market(history):
    windows = data_mod.load_market_windows(history)
    logs = []
    kept, record = optimizer_main.apply_screener_filter(_args(history, "--no-screener-filter"), windows, logs.append)
    assert kept == windows
    assert record == {"enabled": False}
    assert logs == ["screener filter: disabled (--no-screener-filter)"]


def test_apply_screener_filter_logs_counts_and_records_settings(history):
    windows = data_mod.load_market_windows(history)
    logs = []
    kept, record = optimizer_main.apply_screener_filter(_args(history, "--screener-eval-hours", "1"), windows, logs.append)
    # Hourly checkpoints across the two days: markets that were live and liquid
    # a day earlier (SOON/CLOSED/STALE/EDGE-OUT) pass at that earlier refresh.
    assert set(w[0] for w in kept) == HOURLY_PASSING
    assert [w[0] for w in kept] == [w[0] for w in windows if w[0] in HOURLY_PASSING]
    assert record["enabled"] is True
    assert record["source"] == "kalshi_screener_config constants"
    assert record["settings"] == data_mod.default_screener_settings()
    assert record["passed"] == len(kept) and record["total"] == len(windows)
    assert record["reasons"] == {
        data_mod.REASON_SPREAD_MIN: 1, data_mod.REASON_SPREAD_MAX: 1, data_mod.REASON_YES_BID: 1,
        data_mod.REASON_NO_BID: 1, data_mod.REASON_NO_BOOK: 1, data_mod.REASON_VOL24H: 1,
        data_mod.REASON_OI: 1, data_mod.REASON_TOO_LATE: 1, data_mod.REASON_MVE: 1,
    }
    assert record["failed"]["TIGHT-1"] == data_mod.REASON_SPREAD_MIN
    assert record["first_pass_ms"]["GOOD-1"] == T0 - 22 * data_mod.HOUR_MS
    line = [entry for entry in logs if entry.startswith("screener filter:")][-1]
    assert line.startswith(f"screener filter: {len(kept)} of {len(windows)} history markets pass (top reasons: ")
    assert "=1" in line and "kalshi_screener_config constants" in line

    with pytest.raises(SystemExit, match="none of the .* history markets would have passed"):
        optimizer_main.apply_screener_filter(
            _args(history, "--screener-eval-hours", "0"),
            [("CLOSED-1", BASE, BASE + data_mod.HOUR_MS)], logs.append,  # no prints in the 24 h before any checkpoint
        )


def test_screener_settings_precedence_session_then_base_then_constants(monkeypatch, history):
    sessions = {
        "live": {"screener": {"minSpreadCents": 9, "excludedTickerKeywords": ["HIGH"]}},
        "guards": {"screener": {"MIN_SPREAD_CENTS": 1}},
        "old-schema": {"bot": {}},
    }

    def fake_load(name):
        if name not in sessions:
            raise SystemExit(f"--base-session: no saved session named {name!r}")
        return sessions[name]

    monkeypatch.setattr(optimizer_main, "load_base_session_configuration", fake_load)
    logs = []
    settings, source = optimizer_main.resolve_screener_settings(
        _args(history, "--screener-session", "live", "--base-session", "guards"), logs.append,
    )
    assert settings["min_spread_cents"] == 9.0 and settings["excluded_ticker_keywords"] == ["HIGH"]
    assert source == "session 'live' (--screener-session)"

    settings, source = optimizer_main.resolve_screener_settings(_args(history, "--base-session", "guards"), logs.append)
    assert settings["min_spread_cents"] == 1.0 and source == "session 'guards' (--base-session)"

    settings, source = optimizer_main.resolve_screener_settings(
        _args(history, "--screener-session", "old-schema", "--base-session", "guards"), logs.append,
    )
    assert settings["min_spread_cents"] == 1.0 and source == "session 'guards' (--base-session)"
    assert any("has no 'screener' section" in entry for entry in logs)

    settings, source = optimizer_main.resolve_screener_settings(_args(history, "--screener-session", "old-schema"), logs.append)
    assert settings == data_mod.default_screener_settings() and source == "kalshi_screener_config constants"

    with pytest.raises(SystemExit, match="no saved session named"):
        optimizer_main.resolve_screener_settings(_args(history, "--screener-session", "missing"), logs.append)

    # the session's settings drive the filter: 9c min spread rejects the 10c markets? no - 10 >= 9 passes; keyword drops KXHIGHNY
    kept, record = optimizer_main.apply_screener_filter(
        _args(history, "--screener-session", "live", "--screener-eval-hours", "1"), data_mod.load_market_windows(history), logs.append,
    )
    assert "KXHIGHNY-1" not in {w[0] for w in kept} and "GOOD-1" in {w[0] for w in kept}
    assert record["source"] == "session 'live' (--screener-session)"
    assert record["settings"]["excluded_ticker_keywords"] == ["HIGH"]
    assert record["failed"]["KXHIGHNY-1"] == data_mod.REASON_KEYWORD


def test_select_markets_applies_filter_before_the_cap_and_class_filter(history, monkeypatch):
    args = _args(history, "--markets", "2", "--screener-eval-hours", "1")
    markets, record = optimizer_main._select_markets(args, None, None, None, "")
    # trade-count ranking (GOOD-1 has the extra prints), then ticker, within the passing set
    assert [m[0] for m in markets] == ["GOOD-1", "CLOSED-1"]
    assert set(record["first_pass_ms"]) == HOURLY_PASSING
    assert record["enabled"] is True

    # --class runs see the same filtered universe: a member that fails the screen is not selected
    monkeypatch.setattr(optimizer_main, "classify_markets_for_class", lambda *a, **k: {"TIGHT-1", "GOOD-1"})
    markets, _ = optimizer_main._select_markets(_args(history, "--class", "thickCalm"), None, None, None, "thickCalm")
    assert [m[0] for m in markets] == ["GOOD-1"]
    monkeypatch.setattr(optimizer_main, "classify_markets_for_class", lambda *a, **k: {"TIGHT-1"})
    with pytest.raises(SystemExit, match="no replayable markets classified"):
        optimizer_main._select_markets(_args(history, "--class", "thickCalm"), None, None, None, "thickCalm")


def test_unscreenable_history_schema_falls_back_to_the_full_universe(tmp_dir):
    path = tmp_dir / "bare.sqlite3"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE markets(ticker TEXT PRIMARY KEY, series TEXT, close_ts_ms INTEGER)")
    con.execute("CREATE TABLE candles(ticker TEXT, ts_ms INTEGER, open_cents REAL)")
    con.execute("CREATE TABLE trades(ticker TEXT, trade_id TEXT, ts_ms INTEGER)")
    con.execute("INSERT INTO markets VALUES('T-A', 'T', 999999)")
    for ts in range(0, 600_001, 60_000):
        con.execute("INSERT INTO candles VALUES('T-A', ?, 50.0)", (ts,))
    con.commit()
    con.close()
    windows = data_mod.load_market_windows(str(path))
    logs = []
    kept, record = optimizer_main.apply_screener_filter(_args(str(path)), windows, logs.append)
    assert kept == windows
    assert record["enabled"] is False and "no such column" in record["error"]
    assert any("cannot be screened" in entry for entry in logs)


def test_end_to_end_run_records_screener_filter_in_args_json(tmp_dir, history, monkeypatch):
    monkeypatch.setenv("OPTIMIZER_EVALUATOR", "optimizer._synthetic:evaluate_candidate")
    args = _args(
        history, "--candidates", "9", "--workers", "2", "--seed", "3", "--top-params", "3",
        "--budget-minutes", "5", "--markets", "4", "--screener-eval-hours", "1",
        "--output-dir", str(tmp_dir / "out"),
    )
    summary = optimizer_main.run_optimization(args)
    assert Path(summary["report_path"]).is_file()
    con = sqlite3.connect(tmp_dir / "out" / "results.sqlite3")
    try:
        markets_json, args_json = con.execute("SELECT markets_json, args_json FROM opt_runs").fetchone()
    finally:
        con.close()
    markets = json.loads(markets_json)
    record = json.loads(args_json)["screener_filter"]
    assert record["enabled"] is True
    assert record["settings"] == data_mod.default_screener_settings()
    assert record["source"] == "kalshi_screener_config constants"
    assert markets == ["GOOD-1", "CLOSED-1", "EDGE-IN-1", "EDGE-OUT-1"]  # --markets 4 cap after the filter
    assert set(record["first_pass_ms"]) == HOURLY_PASSING
    assert "TIGHT-1" not in markets and "KXMVECROSSCATEGORY-1" not in markets
    assert record["failed"]["TIGHT-1"] == data_mod.REASON_SPREAD_MIN
    assert record["passed"] == len(HOURLY_PASSING) and record["total"] == len(MARKETS)
    assert record["eval_ms"][0] == BASE
