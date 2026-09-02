"""Market classes: classifier precedence, override resolution, Tier-1 history stats."""

from __future__ import annotations

import pickle
from pathlib import Path
from types import SimpleNamespace

import pytest

import market_classes as mc
from history.store import HistoryStore
from markout_history import GroupStats
from session_config import MARKET_CLASS_NAMES, SCHEMA_VERSION, bot_settings_payload, default_session_configuration

THRESHOLDS = mc.ClassifierThresholds(
    thin_depth_contracts=30, wide_spread_cents=12, toxic_net_markout_cents_per_contract=-1.0, min_series_fills=10,
)


def row(ticker="KXTEST-26SEP01-T1", spread=None, yes_size=None, no_size=None):
    values: dict = {"Ticker": ticker}
    if spread is not None:
        values["Spread(c)"] = spread
    if yes_size is not None:
        values["YES bid size"] = yes_size
    if no_size is not None:
        values["NO bid size"] = no_size
    return values


def test_classifier_precedence_toxic_over_thin_wide_over_thick_calm():
    toxic = mc.SeriesStats("KXTEST", fills=10, net_markout_cents_per_contract=-1.0)
    healthy = mc.SeriesStats("KXTEST", fills=500, net_markout_cents_per_contract=0.4, realized_spread_cents=3.0)
    thin_and_wide = row(spread=20, yes_size=5, no_size=200)

    assert mc.classify_market(thin_and_wide, toxic, THRESHOLDS) == "toxic"
    assert mc.classify_market(thin_and_wide, healthy, THRESHOLDS) == "thinWide"
    assert mc.classify_market(row(spread=2, yes_size=5, no_size=200), healthy, THRESHOLDS) == "thinWide"  # thin side
    assert mc.classify_market(row(spread=15, yes_size=100, no_size=100), None, THRESHOLDS) == "thinWide"  # wide
    assert mc.classify_market(row(spread=3, yes_size=100, no_size=90), healthy, THRESHOLDS) == "thickCalm"
    assert mc.classify_market(row(spread="3", yes_size="100", no_size="90"), None, THRESHOLDS) == "thickCalm"  # raw_row
    assert mc.classify_market(row(), None, THRESHOLDS) == "default"
    assert mc.classify_market(row(yes_size=100), healthy, THRESHOLDS) == "thickCalm"  # series spread fills in
    assert mc.classify_market(row(yes_size=100), mc.SeriesStats("KXTEST", fills=500, realized_spread_cents=14.0), THRESHOLDS) == "thinWide"
    # Toxic evidence needs the fill floor and the (inclusive) markout threshold.
    calm = row(spread=3, yes_size=100, no_size=100)
    assert mc.classify_market(calm, mc.SeriesStats("KXTEST", fills=9, net_markout_cents_per_contract=-5.0), THRESHOLDS) == "thickCalm"
    assert mc.classify_market(calm, mc.SeriesStats("KXTEST", fills=50, net_markout_cents_per_contract=-0.99), THRESHOLDS) == "thickCalm"
    assert mc.classify_market(calm, mc.SeriesStats("KXTEST", fills=50), THRESHOLDS) == "thickCalm"  # no markout evidence
    assert set(MARKET_CLASS_NAMES) == {"toxic", "thinWide", "thickCalm", "default"}


def test_thresholds_from_configuration_and_group_stats_adapter():
    config = default_session_configuration()
    config["botClasses"]["classifier"].update(
        thinDepthContracts=5, wideSpreadCents=7, toxicNetMarkoutCentsPerContract=-2.5, minSeriesFills=3,
    )
    thresholds = mc.ClassifierThresholds.from_configuration(config)
    assert thresholds == mc.ClassifierThresholds(5.0, 7.0, -2.5, 3)

    live = GroupStats(
        key="KXX", fills=12, avg_edge_cents=1.0, avg_adverse_cents=2.0, avg_fee_cents=0.5,
        avg_net_cents=-0.7, total_net_cents=-210.0, total_adverse_cents=10.0, total_contracts=70.0,
    )
    stats = mc.SeriesStats.from_group_stats(live)
    assert stats.fills == 12
    assert stats.net_markout_cents_per_contract == pytest.approx(-3.0)  # size-weighted, not avg_net
    assert mc.classify_market(row(spread=2, yes_size=100, no_size=100), stats, thresholds) == "toxic"


def test_resolve_overrides_is_frozen_hashable_and_picklable():
    config = default_session_configuration()
    config["botClasses"]["thinWide"]["overrides"] = [
        {"field": "maximum_contracts_per_order", "value": 2},
        {"field": "markout_horizons_seconds", "value": [1, 5]},
    ]
    assert mc.resolve_overrides(config, "thinWide") == ()  # classification disabled

    config["botClasses"]["enabled"] = True
    resolved = mc.resolve_overrides(config, "thinWide")
    assert resolved == (("maximum_contracts_per_order", 2), ("markout_horizons_seconds", (1, 5)))
    assert isinstance(hash(resolved), int)
    assert pickle.loads(pickle.dumps(resolved)) == resolved
    assert mc.resolve_overrides(config, "default") == ()
    with pytest.raises(ValueError, match="unknown market class"):
        mc.resolve_overrides(config, "thick")


def test_bot_settings_payload_applies_overrides_and_keeps_managed_fields():
    config = default_session_configuration()
    payload = bot_settings_payload(
        config,
        overrides=(("maximum_contracts_per_order", 9), ("markout_horizons_seconds", (2, 4))),
        market_ticker="MKT-1", yes_budget_cents=250, no_budget_cents=300,
        telemetry_sqlite_path="t.sqlite3", pnl_tracker_path="p.sqlite3", watchdog_state_file="w.json",
    )
    assert payload["maximum_contracts_per_order"] == 9
    assert payload["markout_horizons_seconds"] == [2, 4]
    assert payload["market_ticker"] == "MKT-1"
    assert (payload["yes_order_budget_cents"], payload["no_order_budget_cents"]) == (250, 300)
    assert payload["telemetry_sqlite_path"] == "t.sqlite3"
    assert payload["pnl_tracker_path"] == "p.sqlite3"
    assert payload["watchdog_state_file"] == "w.json"
    assert payload["watchdog_flatten_retries"] == config["watchdog"]["flattenRetries"]

    baseline = bot_settings_payload(config, market_ticker="MKT-1")
    assert baseline["maximum_contracts_per_order"] == config["bot"]["maximum_contracts_per_order"]
    assert set(baseline) == set(payload)

    for bad, message in (
        ((("yes_order_budget_cents", 1),), "launcher-managed"),
        ((("market_ticker", "X"),), "launcher-managed"),
        ((("nope", 1),), "not a session bot field"),
        ((("maximum_contracts_per_order", 0),), "invalid bot settings"),
    ):
        with pytest.raises(ValueError, match=message):
            bot_settings_payload(config, overrides=bad, market_ticker="MKT-1")


def _trade(trade_id, ts_ms, price_units, count_units, taker_side):
    return SimpleNamespace(
        trade_id=trade_id, timestamp_ms=ts_ms, yes_price_units=price_units,
        no_price_units=10_000 - price_units, count_units=count_units, taker_side=taker_side,
    )


def make_history(path: Path) -> None:
    """Three one-market series: toxic (tight but adverse), tight/healthy, wide."""
    base = 1_700_000_040_000  # Kalshi candle timestamps are minute-aligned period ends
    assert base % 60_000 == 0
    store = HistoryStore(path)
    books = {
        # ticker: (bid_close_units, ask_close_units) for every candle
        "KXTOX-1": (5500, 5700),     # spread 2c; mid 56c after 50c prints -> maker loses ~6c
        "KXTIGHT-1": (5100, 5300),   # spread 2c; mid 52c after 50c maker buys -> +2c
        "KXWIDE-1": (4000, 6000),    # spread 20c
    }
    for ticker, (bid, ask) in books.items():
        store.upsert_market(
            ticker=ticker, series=ticker.split("-")[0], title=ticker, status="settled",
            close_ts_ms=base + 3_600_000, result="yes", volume_24h_units=12_345, oi_units=6_700,
        )
        store.insert_candles(ticker, [
            {
                "ts_ms": base + 60_000 * minute,
                "yes_bid_close_units": bid, "yes_ask_close_units": ask, "volume_units": 100,
            }
            for minute in range(1, 61)
        ])
    # Prints inside minute 1 are marked against the candle closing at base+60s.
    store.insert_trades("KXTOX-1", [_trade(f"tox-{i}", base + 10_000 + i, 5000, 100, "yes") for i in range(12)])
    store.insert_trades("KXTIGHT-1", [_trade(f"tight-{i}", base + 10_000 + i * 1000, 5000, 100, "no") for i in range(12)])
    store.insert_trades("KXWIDE-1", [_trade("wide-0", base + 10_000, 5000, 100, "yes")])
    store.close()


def test_history_series_stats_and_classification(tmp_path: Path):
    path = tmp_path / "history.sqlite3"
    make_history(path)

    stats = mc.load_series_stats_from_history(path)
    assert set(stats) == {"KXTOX", "KXTIGHT", "KXWIDE"}
    assert stats["KXTOX"].fills == 12
    assert stats["KXTOX"].net_markout_cents_per_contract == pytest.approx(-6.0 - 7 * 0.25 * mc.HISTORY_MAKER_FEE_FACTOR)
    assert stats["KXTIGHT"].net_markout_cents_per_contract == pytest.approx(2.0 - 7 * 0.25 * mc.HISTORY_MAKER_FEE_FACTOR)
    assert stats["KXTIGHT"].realized_spread_cents == pytest.approx(2.0)
    assert stats["KXWIDE"].realized_spread_cents == pytest.approx(20.0)
    assert stats["KXTIGHT"].trades_per_hour == pytest.approx(12 / (11_000 / 3_600_000))
    assert stats["KXTOX"].volume_contracts == pytest.approx(12.0)

    classes = mc.classify_history_markets(path, THRESHOLDS)
    assert classes == {"KXTOX-1": "toxic", "KXTIGHT-1": "thickCalm", "KXWIDE-1": "thinWide"}
    assert mc.load_series_stats_from_history(tmp_path / "missing.sqlite3") == {}

    source = mc.HistorySeriesStatsSource(path, ttl_seconds=3600)
    first = source()
    assert first["KXTOX"].fills == 12
    assert source() is first  # cached inside the TTL

    config = default_session_configuration()
    config["botClasses"]["enabled"] = True
    config["botClasses"]["toxic"]["overrides"] = [{"field": "minimum_expected_edge_cents_to_quote", "value": 6}]
    config["botClasses"]["thinWide"]["overrides"] = [{"field": "maximum_contracts_per_order", "value": 2}]
    resolver = mc.build_market_class_resolver(config, series_stats=source)
    assert resolver({"Ticker": "KXTOX-1", "Spread(c)": "2", "YES bid size": "100", "NO bid size": "100"}) == (
        "toxic", (("minimum_expected_edge_cents_to_quote", 6),)
    )
    assert resolver({"Ticker": "KXWIDE-1", "Spread(c)": "20"}) == ("thinWide", (("maximum_contracts_per_order", 2),))
    assert resolver({"Ticker": "KXTIGHT-1", "Spread(c)": "2", "YES bid size": "100", "NO bid size": "100"}) == ("thickCalm", ())
    assert resolver({"Ticker": "KXNEW-1"}) == ("default", ())

    config["botClasses"]["enabled"] = False
    disabled = mc.build_market_class_resolver(config, series_stats=dict(first))
    assert disabled({"Ticker": "KXTOX-1", "Spread(c)": "2"}) == ("default", ())


def test_optimizer_class_selection_and_write_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from optimizer import main as optimizer_main
    from optimizer import writeback

    path = tmp_path / "history.sqlite3"
    make_history(path)
    args = optimizer_main.build_arg_parser().parse_args(["--class", "toxic"])
    assert args.market_class == "toxic"
    with pytest.raises(SystemExit):
        optimizer_main.build_arg_parser().parse_args(["--class", "sideways"])
    members = optimizer_main.classify_markets_for_class(str(path), "toxic", None, None, log_fn=lambda _: None)
    assert members == {"KXTOX-1"}
    assert optimizer_main.classify_markets_for_class(str(path), "thinWide", None, None, log_fn=lambda _: None) == {"KXWIDE-1"}

    monkeypatch.setenv("KALSHI_SESSION_STORE", str(tmp_path / "store"))
    default_bot = default_session_configuration()["bot"]
    winner = {
        "maximum_contracts_per_order": 2,
        "minimum_expected_edge_cents_to_quote": default_bot["minimum_expected_edge_cents_to_quote"],  # unchanged -> dropped
        "yes_order_budget_cents": 300,
    }
    created = writeback.write_back("run-class-1", winner, 42.0, 0, 1_000, market_class="thinWide")
    configuration = created["configuration"]
    assert created["name"].startswith("optimized-thinWide-")
    assert "class thinWide (1 override(s))" in created["description"]
    assert created["selected"] is False
    assert configuration["schemaVersion"] == SCHEMA_VERSION
    assert configuration["botClasses"]["enabled"] is True
    assert configuration["botClasses"]["thinWide"]["overrides"] == [{"field": "maximum_contracts_per_order", "value": 2}]
    assert configuration["botClasses"]["toxic"]["overrides"] == []
    assert configuration["bot"]["maximum_contracts_per_order"] == default_bot["maximum_contracts_per_order"]  # base untouched
    assert configuration["launcher"]["yesBudgetCents"] == 300  # budgets stay fleet-wide
    assert mc.resolve_overrides(configuration, "thinWide") == (("maximum_contracts_per_order", 2),)
    with pytest.raises(ValueError, match="unknown market class"):
        writeback.write_back("run-class-2", winner, 1.0, 0, 1, market_class="sideways")
