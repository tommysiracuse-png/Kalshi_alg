import json
import sqlite3
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import kalshi_screener
import kalshi_screener_config
from fleet_models import MAX_CONCURRENT_BOTS
from launcher import screener_settings_for_session
from market_classes import resolve_overrides
from session_config import (
    SCHEMA_VERSION,
    SCREENER_MVE_FILTER_VALUES,
    SCREENER_STATUS_VALUES,
    bot_settings_payload,
    default_bot_classes_configuration,
    default_screener_configuration,
    default_session_configuration,
    screener_settings_from_configuration,
    validate_session_configuration,
)
from session_store import RunMetricsAccumulator, SessionConflictError, SessionStore
from top_of_book_bot import build_settings_from_args

# Verbatim copy of a schema-v2 configuration.json stored by a real run
# (session_data/artifacts/<session>/<run>/configuration.json, 2026-09-01).
STORED_V2_CONFIGURATION_JSON = """{
  "schemaVersion": 2,
  "execution": {"useDemo": false, "dryRun": false, "subaccount": 0},
  "launcher": {"fixedTicker": "", "maxBots": 40, "yesBudgetCents": 800, "noBudgetCents": 800,
    "launchDelaySeconds": 0.5, "runScreenerOnStart": true, "refreshIntervalSeconds": 1200.0,
    "pollSeconds": 60.0, "minimumCarryoverValueCents": 20.0},
  "watchdog": {"intervalSeconds": 60.0, "refreshSeconds": 3.0, "extremeStaleSeconds": 120.0,
    "sampleSeconds": 2.5, "pollIntervalSeconds": 0.35, "confidenceReductionThreshold": 0.68,
    "confidenceFlattenThreshold": 0.55, "flattenRetries": 2},
  "fleetRuntime": {"shardSize": 25, "quoteFreshnessSeconds": 8.0, "writeUtilizationLimit": 0.95,
    "readUtilizationLimit": 0.95, "cashReserveFraction": 0.2, "seriesExposureFraction": 0.5,
    "workerHeartbeatSeconds": 3.0, "workerStaleSeconds": 10.0, "startupTimeoutSeconds": 410.0},
  "bot": {
    "maximum_contracts_per_order": 5, "maximum_projected_contracts_per_line": 10,
    "budget_fee_buffer_cents": 3, "allow_fractional_order_entry_when_supported": false,
    "refill_resting_size_after_partial_fill": false, "post_only_quotes": true,
    "cancel_quotes_if_exchange_pauses": false, "minimum_milliseconds_between_requotes": 250,
    "resting_order_expiration_seconds": 500, "expiration_refresh_lead_seconds": 30,
    "expiration_jitter_seconds": 20, "stagger_yes_no_expiration_offsets_seconds": 10,
    "aggressive_improvement_ticks_when_spread_is_wide": 6,
    "minimum_spread_ticks_required_for_aggressive_improvement": 8,
    "passive_offset_ticks_when_not_improving": 0,
    "join_current_best_bid_when_starting_new_quote_cycle": false,
    "post_fill_no_improve_cooldown_ms": 2000, "same_side_reentry_cooldown_ms": 5000,
    "suppress_same_side_quotes_during_reentry_cooldown": true,
    "minimum_upward_reprice_ticks_required": 4, "minimum_best_bid_cents_required_to_quote": 8,
    "minimum_implied_ask_cents_required_to_quote": 8,
    "minimum_market_best_bid_cents_required_to_quote_any_side": 8,
    "enforce_one_tick_safety_below_implied_ask": false, "enable_one_way_inventory_guard": false,
    "one_way_inventory_guard_contracts": 5, "inventory_skew_contracts_per_tick": 5,
    "maximum_inventory_skew_ticks": 6, "minimum_top_level_depth_contracts": 15,
    "maximum_top_level_gap_cents": 15, "enable_pair_guard": false, "maximum_combined_bid_cents": 97,
    "additional_profit_buffer_cents": 4, "pair_guard_priority": "auto",
    "maximum_post_only_reprice_attempts": 3, "post_only_reprice_cooldown_seconds": 1.5,
    "cancel_strategy_quotes_on_startup": true, "subscribe_to_market_positions_channel": true,
    "enable_queue_position_logging": true, "queue_position_log_interval_seconds": 10,
    "enable_sqlite_telemetry": true, "trade_history_window_seconds": 60,
    "model_refresh_interval_seconds": 50, "markout_horizons_seconds": [1, 5, 30, 120],
    "fill_probability_horizon_seconds": 181, "fill_probability_prior_fills": 4.905004415686011,
    "fill_probability_prior_misses": 3.3810880135308974,
    "minimum_expected_edge_cents_to_keep_quote": 1, "minimum_expected_edge_cents_to_quote": 2,
    "inventory_reduction_max_negative_edge_cents": 2, "strong_edge_threshold_cents": 8,
    "default_toxicity_cents": 4, "bucket_pessimism_enabled": true,
    "bucket_pessimism_max_cents": 1.633955764355991, "bucket_pessimism_min_observations": 5,
    "default_fee_factor_for_maker_quotes": 1.7246017472837578,
    "fair_value_mid_weight": 0.16650390051701366, "fair_value_ticker_weight": 0.22937336781353335,
    "fair_value_trade_weight": 0.604122731669453,
    "fair_value_max_orderbook_imbalance_adjust_cents": 8, "fair_value_max_trade_bias_adjust_cents": 6,
    "quote_size_min_fraction_of_budget": 0.2, "quote_size_max_fraction_of_budget": 1,
    "candidate_price_levels_to_scan": 58, "enable_orderbook_pull_toxicity_guard": true,
    "orderbook_pull_window_ms": 1500, "orderbook_pull_top_levels_to_track": 8,
    "orderbook_pull_absolute_threshold_contracts": 150, "orderbook_pull_relative_depth_threshold": 0.2,
    "orderbook_pull_side_cooldown_ms": 5000, "orderbook_pull_market_cooldown_ms": 5000,
    "orderbook_pull_penalty_cents": 7, "enable_queue_abandonment_guard": false,
    "maximum_queue_ahead_contracts_before_abandonment": 200,
    "maximum_queue_ahead_multiple_of_our_remaining_size": 25,
    "queue_abandonment_consecutive_polls_required": 3, "queue_abandonment_side_cooldown_seconds": 15,
    "queue_abandonment_market_cooldown_seconds": 15, "primary_client_order_prefix": "mm",
    "legacy_client_order_prefixes": ["mm:", "tob:"]
  }
}"""


class TrackingConnection(sqlite3.Connection):
    closed = False

    def close(self) -> None:
        self.closed = True
        super().close()


def test_session_store_closes_connections_on_success_and_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    original_connect = sqlite3.connect
    connections: list[TrackingConnection] = []

    def connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs, factory=TrackingConnection)
        connections.append(connection)
        return connection

    monkeypatch.setattr("session_store.sqlite3.connect", connect)
    store = SessionStore(tmp_path / "session_data")
    original_description = store.list_sessions()[0]["description"]
    with pytest.raises(RuntimeError, match="rollback"):
        with store._connect() as connection:
            connection.execute("UPDATE sessions SET description='not committed'")
            raise RuntimeError("rollback")

    assert store.list_sessions()[0]["description"] == original_description
    assert connections
    assert all(connection.closed for connection in connections)


def test_finish_run_closes_every_telemetry_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = SessionStore(tmp_path / "session_data")
    run = store.prepare_run()
    telemetry_path = Path(run["artifactPath"]) / "markets" / "TEST-1" / "telemetry.sqlite3"
    telemetry_path.parent.mkdir(parents=True)
    setup_connection = sqlite3.connect(telemetry_path)
    try:
        setup_connection.execute(
            """CREATE TABLE order_revisions(
                   ended_state TEXT NOT NULL, ended_at_ms INTEGER
               )"""
        )
        setup_connection.execute(
            "INSERT INTO order_revisions(ended_state,ended_at_ms) VALUES('Resting',NULL)"
        )
        setup_connection.commit()
    finally:
        setup_connection.close()

    original_connect = sqlite3.connect
    connections: list[TrackingConnection] = []

    def connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs, factory=TrackingConnection)
        connections.append(connection)
        return connection

    monkeypatch.setattr("session_store.sqlite3.connect", connect)
    store.finish_run(run["id"], "stopped")

    assert connections
    assert all(connection.closed for connection in connections)
    verification = sqlite3.connect(telemetry_path)
    try:
        state, ended_at = verification.execute(
            "SELECT ended_state,ended_at_ms FROM order_revisions"
        ).fetchone()
    finally:
        verification.close()
    assert state == "Unknown"
    assert ended_at is not None


def test_finish_run_commits_terminal_state_before_artifact_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    store = SessionStore(tmp_path / "session_data")
    run = store.prepare_run()
    store.claim_run(run["id"])
    store.mark_running(run["id"])

    def fail_finalization(*_args):
        raise OSError(24, "Too many open files")

    monkeypatch.setattr(store, "_finalize_order_revisions", fail_finalization)
    with pytest.raises(OSError, match="Too many open files"):
        store.finish_run(run["id"], "stopped")

    saved = store.get_run(run["id"], include_artifact_bytes=False)
    assert saved["status"] == "stopped"
    assert saved["endedAt"] is not None
    assert store.active_run() is None


def test_configuration_round_trip_and_unknown_rejection():
    config = default_session_configuration()
    config["launcher"]["fixedTicker"] = "TEST-MARKET"
    config["launcher"]["yesBudgetCents"] = 321
    config["watchdog"]["flattenRetries"] = 4
    config["bot"]["markout_horizons_seconds"] = [1, 15, 60]
    normalized = validate_session_configuration(config)
    assert normalized["launcher"]["fixedTicker"] == "TEST-MARKET"
    assert len(normalized["bot"]) >= 75
    config["bot"]["not_a_setting"] = 1
    with pytest.raises(ValueError, match="unknown configuration"):
        validate_session_configuration(config)


def test_stored_schema_v2_configuration_migrates_to_current_schema(tmp_path: Path):
    stored = json.loads(STORED_V2_CONFIGURATION_JSON)
    assert stored["schemaVersion"] == 2 and "botClasses" not in stored and "screener" not in stored

    migrated = validate_session_configuration(stored)
    assert migrated["schemaVersion"] == SCHEMA_VERSION == 4
    assert migrated["botClasses"] == default_bot_classes_configuration()
    assert migrated["botClasses"]["enabled"] is False
    assert migrated["screener"] == default_screener_configuration()
    for key, value in stored["bot"].items():  # every v2 bot value survives untouched
        assert migrated["bot"][key] == value
    assert migrated["launcher"]["yesBudgetCents"] == 800
    assert migrated["fleetRuntime"]["startupTimeoutSeconds"] == 410.0
    assert validate_session_configuration(migrated) == migrated  # idempotent re-validation
    assert bot_settings_payload(stored, market_ticker="MKT")["default_fee_factor_for_maker_quotes"] == 1.7246017472837578

    # Stored rows are re-validated on every read, so a v2 row must load from the store as v4.
    store = SessionStore(tmp_path / "session_data")
    created = store.create_session({"name": "Legacy v2", "configuration": stored})
    assert store.get_session(created["id"])["configuration"]["schemaVersion"] == 4

    with pytest.raises(ValueError, match="schemaVersion must be 1, 2, 3 or 4"):
        validate_session_configuration({**stored, "schemaVersion": 5})


def test_stored_schema_v1_and_v3_configurations_gain_screener_defaults_only():
    # v1 rows predate fleetRuntime; the verbatim v2 JSON minus that section is
    # exactly the shape the v1 writer stored.
    stored_v1 = json.loads(STORED_V2_CONFIGURATION_JSON)
    del stored_v1["fleetRuntime"]
    stored_v1["schemaVersion"] = 1
    migrated_v1 = validate_session_configuration(stored_v1)
    assert migrated_v1["schemaVersion"] == 4
    assert migrated_v1["fleetRuntime"] == default_session_configuration()["fleetRuntime"]
    assert migrated_v1["screener"] == default_screener_configuration()
    assert migrated_v1["bot"]["default_fee_factor_for_maker_quotes"] == 1.7246017472837578

    # A v3 row (botClasses present, customised) keeps its classes verbatim and
    # only gains the screener section.
    stored_v3 = json.loads(STORED_V2_CONFIGURATION_JSON)
    stored_v3["schemaVersion"] = 3
    stored_v3["botClasses"] = default_bot_classes_configuration()
    stored_v3["botClasses"]["enabled"] = True
    stored_v3["botClasses"]["toxic"]["overrides"] = [{"field": "maximum_contracts_per_order", "value": 1}]
    migrated_v3 = validate_session_configuration(stored_v3)
    assert migrated_v3["schemaVersion"] == 4
    assert migrated_v3["botClasses"]["enabled"] is True
    assert migrated_v3["botClasses"]["toxic"]["overrides"] == [{"field": "maximum_contracts_per_order", "value": 1}]
    assert migrated_v3["screener"] == default_screener_configuration()
    assert {key for key in migrated_v3} == {key for key in default_session_configuration()}


def test_screener_defaults_equal_kalshi_screener_config_constants():
    section = default_screener_configuration()
    expected = {
        "status": kalshi_screener_config.STATUS,
        "mveFilter": kalshi_screener_config.MVE_FILTER,
        "maxMarketsToScan": kalshi_screener_config.MAX_MARKETS_TO_SCAN,
        "topN": kalshi_screener_config.TOP_N,
        "minSpreadCents": kalshi_screener_config.MIN_SPREAD_CENTS,
        "maxSpreadCents": kalshi_screener_config.MAX_SPREAD_CENTS,
        "minYesBidCents": kalshi_screener_config.MIN_YES_BID_CENTS,
        "minNoBidCents": kalshi_screener_config.MIN_NO_BID_CENTS,
        "minVol24h": kalshi_screener_config.MIN_VOL24H,
        "minOpenInterest": kalshi_screener_config.MIN_OI,
        "minTimeToCloseHours": kalshi_screener_config.MIN_TIME_TO_CLOSE_HRS,
        "maxTimeToCloseHours": kalshi_screener_config.MAX_TIME_TO_CLOSE_HRS,
        "excludedTickerKeywords": list(kalshi_screener_config.EXCLUDED_TICKER_KEYWORDS),
        "targetEdgeCents": kalshi_screener_config.TARGET_EDGE_CENTS,
        "quoteSize": kalshi_screener_config.QUOTE_SIZE,
        "markoutFilterEnabled": kalshi_screener_config.MARKOUT_FILTER_ENABLED,
        "markoutFilterNetThresholdCents": kalshi_screener_config.MARKOUT_FILTER_NET_THRESHOLD_CENTS,
        "markoutFilterTickerMinFills": kalshi_screener_config.MARKOUT_FILTER_TICKER_MIN_FILLS,
        "markoutFilterSeriesMinFills": kalshi_screener_config.MARKOUT_FILTER_SERIES_MIN_FILLS,
        "markoutFilterHorizonSeconds": kalshi_screener_config.MARKOUT_FILTER_HORIZON_SECONDS,
        "markoutFilterLookbackDays": kalshi_screener_config.MARKOUT_FILTER_LOOKBACK_DAYS,
        "markoutFilterTotalNetThresholdCents": kalshi_screener_config.MARKOUT_FILTER_TOTAL_NET_THRESHOLD_CENTS,
    }
    assert section == expected
    assert "feeBufferCents" not in section  # retired: the filters never read it
    assert validate_session_configuration(default_session_configuration())["screener"] == expected

    # A default session produces the very settings dict the CLI/config path
    # produced before sessions carried the section (top_n floored by maxBots).
    cli_settings = kalshi_screener.build_settings_from_args(kalshi_screener.build_parser().parse_args([]))
    cli_settings["top_n"] = max(int(cli_settings["top_n"]), 40)
    assert screener_settings_for_session(default_session_configuration(), 40) == cli_settings
    # fee_buffer_cents stays an internal, config-file-only setting.
    assert cli_settings["fee_buffer_cents"] == kalshi_screener_config.FEE_BUFFER_CENTS
    assert "fee_buffer_cents" not in screener_settings_from_configuration(default_session_configuration())


def test_stored_screener_section_drops_retired_fee_buffer_field(tmp_path: Path):
    stored = default_session_configuration()
    stored["screener"]["feeBufferCents"] = 3  # written by the schema-v4 editor before the field was retired
    stored["screener"]["targetEdgeCents"] = 6
    migrated = validate_session_configuration(stored)
    assert "feeBufferCents" not in migrated["screener"]
    assert migrated["screener"]["targetEdgeCents"] == 6
    assert validate_session_configuration(migrated) == migrated
    # Any other unknown screener key is still rejected.
    stored["screener"]["feeBuffer"] = 3
    with pytest.raises(ValueError, match="unknown configuration field.*screener: feeBuffer"):
        validate_session_configuration(stored)
    del stored["screener"]["feeBuffer"]

    store = SessionStore(tmp_path / "session_data")
    created = store.create_session({"name": "Old screener row", "configuration": stored})
    assert "feeBufferCents" not in store.get_session(created["id"])["configuration"]["screener"]
    # The launcher mapping still carries the config-file value internally.
    assert screener_settings_for_session(stored, 0)["fee_buffer_cents"] == kalshi_screener_config.FEE_BUFFER_CENTS


@pytest.mark.parametrize("horizon", [1, 5, 30, 120])
def test_markout_filter_horizon_accepts_only_recorded_horizons(horizon):
    config = default_session_configuration()
    config["screener"]["markoutFilterHorizonSeconds"] = horizon
    assert validate_session_configuration(config)["screener"]["markoutFilterHorizonSeconds"] == horizon
    config["screener"]["markoutFilterHorizonSeconds"] = float(horizon)
    assert validate_session_configuration(config)["screener"]["markoutFilterHorizonSeconds"] == horizon
    for bad in (0, 10, 60, -5, 2.5):
        config["screener"]["markoutFilterHorizonSeconds"] = bad
        with pytest.raises(ValueError, match="markoutFilterHorizonSeconds must be"):
            validate_session_configuration(config)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("status", "everything", "screener.status must be one of"),
        ("status", 7, "screener.status must be a string"),
        ("mveFilter", "maybe", "screener.mveFilter must be one of"),
        ("minSpreadCents", 40, "minSpreadCents must be <= screener.maxSpreadCents"),
        ("maxSpreadCents", 2, "minSpreadCents must be <= screener.maxSpreadCents"),
        ("minTimeToCloseHours", 60.0, "minTimeToCloseHours must be <= screener.maxTimeToCloseHours"),
        ("maxTimeToCloseHours", 1.0, "minTimeToCloseHours must be <= screener.maxTimeToCloseHours"),
        ("minVol24h", -1, "screener.minVol24h must be >= 0"),
        ("minOpenInterest", -0.5, "screener.minOpenInterest must be >= 0"),
        ("minYesBidCents", -1, "screener.minYesBidCents must be >= 0"),
        ("minYesBidCents", 100, "screener.minYesBidCents must be <= 99"),
        ("topN", 0, "screener.topN must be >= 1"),
        ("maxMarketsToScan", -5, "screener.maxMarketsToScan must be >= 1"),
        ("quoteSize", 0, "screener.quoteSize must be >= 1"),
        ("quoteSize", 2.5, "screener.quoteSize must be an integer"),
        ("quoteSize", "50", "screener.quoteSize must be a number"),
        ("markoutFilterHorizonSeconds", 0, "markoutFilterHorizonSeconds must be one of 1, 5, 30, 120"),
        ("markoutFilterHorizonSeconds", 10, "markoutFilterHorizonSeconds must be one of 1, 5, 30, 120"),
        ("markoutFilterTickerMinFills", -1, "markoutFilterTickerMinFills must be >= 0"),
        ("markoutFilterLookbackDays", -1.0, "markoutFilterLookbackDays must be >= 0"),
        ("markoutFilterEnabled", "yes", "markoutFilterEnabled must be true or false"),
        ("markoutFilterEnabled", 1, "markoutFilterEnabled must be true or false"),
        ("excludedTickerKeywords", "LOWT", "excludedTickerKeywords must be a list"),
        ("excludedTickerKeywords", ["LOWT", 5], r"excludedTickerKeywords\[1\] must be a non-empty string"),
        ("excludedTickerKeywords", ["LOWT", "  "], r"excludedTickerKeywords\[1\] must be a non-empty string"),
        ("excludedTickerKeywords", [None], r"excludedTickerKeywords\[0\] must be a non-empty string"),
    ],
)
def test_screener_section_rejects_invalid_values(field, value, message):
    config = default_session_configuration()
    config["screener"][field] = value
    with pytest.raises(ValueError, match=message):
        validate_session_configuration(config)


def test_screener_section_rejects_unknown_fields_and_negative_thresholds_are_allowed():
    config = default_session_configuration()
    config["screener"]["minSpread"] = 1
    with pytest.raises(ValueError, match="unknown configuration field.*screener: minSpread"):
        validate_session_configuration(config)
    del config["screener"]["minSpread"]
    config["screener"]["markoutFilterNetThresholdCents"] = -4.5
    config["screener"]["markoutFilterTotalNetThresholdCents"] = -1000
    normalized = validate_session_configuration(config)["screener"]
    assert normalized["markoutFilterNetThresholdCents"] == -4.5
    assert normalized["markoutFilterTotalNetThresholdCents"] == -1000.0
    assert set(SCREENER_STATUS_VALUES) == {"open", "unopened", "paused", "closed", "settled"}
    assert "" in SCREENER_MVE_FILTER_VALUES and "exclude" in SCREENER_MVE_FILTER_VALUES


def test_screener_section_round_trips_through_store_normalized(tmp_path: Path):
    config = default_session_configuration()
    config["screener"].update(
        status=" Closed ", mveFilter="only", minSpreadCents=6, maxSpreadCents=20.0, minVol24h=750,
        minTimeToCloseHours=1, maxTimeToCloseHours=12, excludedTickerKeywords=[" lowt", "HIGH", "lowt"],
        targetEdgeCents=6, quoteSize=25, markoutFilterEnabled=False,
        markoutFilterNetThresholdCents=-2, markoutFilterTickerMinFills=5, markoutFilterSeriesMinFills=40,
        markoutFilterHorizonSeconds=30, markoutFilterLookbackDays=7, markoutFilterTotalNetThresholdCents=-150,
    )
    normalized = validate_session_configuration(config)["screener"]
    assert normalized["status"] == "closed"
    assert normalized["mveFilter"] == "only"
    assert normalized["maxSpreadCents"] == 20 and isinstance(normalized["maxSpreadCents"], int)
    assert normalized["minVol24h"] == 750.0 and isinstance(normalized["minVol24h"], float)
    assert normalized["minTimeToCloseHours"] == 1.0
    assert normalized["excludedTickerKeywords"] == ["lowt", "HIGH"]  # trimmed, de-duplicated, order kept
    assert normalized["markoutFilterLookbackDays"] == 7.0

    store = SessionStore(tmp_path / "session_data")
    created = store.create_session({"name": "Screener tweaks", "configuration": config})
    fetched = store.get_session(created["id"])["configuration"]
    assert fetched["screener"] == normalized
    assert fetched["schemaVersion"] == 4
    assert validate_session_configuration(fetched) == fetched
    updated = store.update_session(created["id"], {"configuration": fetched, "version": created["version"]})
    assert updated["configuration"]["screener"] == normalized
    store.select_session(created["id"])
    run = store.prepare_run()  # the immutable run snapshot the launcher screens with
    assert run["configuration"]["screener"] == normalized


def test_launcher_screener_settings_map_every_session_field_to_filter_keys():
    config = default_session_configuration()
    config["launcher"]["maxBots"] = 60
    config["screener"].update(
        status="closed", mveFilter="", maxMarketsToScan=5000, topN=30, minSpreadCents=6, maxSpreadCents=20,
        minYesBidCents=7, minNoBidCents=8, minVol24h=750, minOpenInterest=150, minTimeToCloseHours=1,
        maxTimeToCloseHours=12, excludedTickerKeywords=["LOWT", "HIGH"], targetEdgeCents=8,
        quoteSize=25, markoutFilterEnabled=False, markoutFilterNetThresholdCents=-2,
        markoutFilterTickerMinFills=5, markoutFilterSeriesMinFills=40, markoutFilterHorizonSeconds=30,
        markoutFilterLookbackDays=7, markoutFilterTotalNetThresholdCents=-150,
    )
    mapped = screener_settings_from_configuration(config)
    assert mapped == {
        "status": "closed", "mve_filter": "", "max_markets_to_scan": 5000, "top_n": 30,
        "min_spread_cents": 6, "max_spread_cents": 20, "min_yes_bid_cents": 7, "min_no_bid_cents": 8,
        "min_vol24h": 750.0, "min_oi": 150.0, "min_time_to_close_hrs": 1.0, "max_time_to_close_hrs": 12.0,
        "excluded_ticker_keywords": ["LOWT", "HIGH"], "target_edge_cents": 8,
        "quote_size": 25, "markout_filter_enabled": False, "markout_filter_net_threshold_cents": -2.0,
        "markout_filter_ticker_min_fills": 5, "markout_filter_series_min_fills": 40,
        "markout_filter_horizon_seconds": 30, "markout_filter_lookback_days": 7.0,
        "markout_filter_total_net_threshold_cents": -150.0,
    }

    # The launcher overlays the section on the CLI defaults and keeps the
    # maxBots -> top_n floor: max(section topN, maxBots).
    settings = screener_settings_for_session(config, config["launcher"]["maxBots"])
    for key, value in mapped.items():
        if key != "top_n":
            assert settings[key] == value, key
    assert settings["top_n"] == 60
    assert screener_settings_for_session(config, 10)["top_n"] == 30
    assert screener_settings_for_session(config, 0)["top_n"] == 30
    # TARGET_EDGE_CENTS only ever reached the filters through the derived EV
    # floor max(2, target // 2); the session value follows the same rule.
    assert settings["minimum_expected_edge_cents_to_quote"] == 4.0
    assert screener_settings_for_session(default_session_configuration(), 0)["minimum_expected_edge_cents_to_quote"] == 2.0
    # File-only settings survive the overlay.
    assert settings["default_tick_cents"] == kalshi_screener_config.DEFAULT_TICK_CENTS
    assert settings["markout_filter_fee_factor"] == kalshi_screener_config.MARKOUT_FILTER_FEE_FACTOR
    assert "excluded_series" in settings

    # Every key the filter code reads is present in the mapped settings.
    filter_keys = {
        "status", "mve_filter", "max_markets_to_scan", "top_n", "min_spread_cents", "max_spread_cents",
        "min_yes_bid_cents", "min_no_bid_cents", "min_vol24h", "min_oi", "min_time_to_close_hrs",
        "max_time_to_close_hrs", "excluded_ticker_keywords", "quote_size", "markout_filter_enabled",
        "markout_filter_net_threshold_cents", "markout_filter_ticker_min_fills", "markout_filter_series_min_fills",
        "markout_filter_horizon_seconds", "markout_filter_lookback_days", "markout_filter_total_net_threshold_cents",
    }
    assert filter_keys <= set(mapped)


def test_session_screener_values_reach_safety_filters_and_scan(monkeypatch: pytest.MonkeyPatch):
    config = default_session_configuration()
    config["screener"].update(
        status="closed", mveFilter="only", maxMarketsToScan=5000, minSpreadCents=6, maxSpreadCents=20,
        excludedTickerKeywords=["lowt"], markoutFilterEnabled=True, markoutFilterNetThresholdCents=-2,
        markoutFilterTickerMinFills=5, markoutFilterSeriesMinFills=40, markoutFilterHorizonSeconds=30,
        markoutFilterLookbackDays=7, markoutFilterTotalNetThresholdCents=-150,
    )
    settings = screener_settings_for_session(config, 0)
    close_time = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat().replace("+00:00", "Z")
    market = {"ticker": "KXLOWTNY-26SEP05-T70", "close_time": close_time, "volume_24h": 1000, "open_interest": 500}
    book = {"spread_c": 10, "yes_bid_c": 40, "no_bid_c": 50}
    assert kalshi_screener.market_passes_safety_filters(market, book, settings) is False  # keyword hit
    assert kalshi_screener.market_passes_safety_filters({**market, "ticker": "KXBRENT-26SEP05-T97"}, book, settings) is True
    tight = screener_settings_for_session(config, 0)
    tight["min_spread_cents"] = 12
    assert kalshi_screener.market_passes_safety_filters({**market, "ticker": "KXBRENT-26SEP05-T97"}, book, tight) is False

    captured: dict = {}

    class MarketSource:
        def list_markets(self, *, status, limit, max_total, mve_filter):
            captured.update(status=status, max_total=max_total, mve_filter=mve_filter)
            return []

    def fake_load_unprofitable(**kwargs):
        captured["markout"] = kwargs
        return set(), set(), {}, {}

    monkeypatch.setattr(kalshi_screener.markout_history, "load_unprofitable", fake_load_unprofitable)
    monkeypatch.setattr(kalshi_screener.markout_history, "format_stats_report", lambda *args, **kwargs: "")
    kalshi_screener.screen_markets(MarketSource(), settings)
    assert captured["status"] == "closed" and captured["mve_filter"] == "only" and captured["max_total"] == 5000
    markout = captured["markout"]
    assert markout["net_threshold_cents"] == -2.0 and markout["ticker_min_fills"] == 5
    assert markout["series_min_fills"] == 40 and markout["horizon_seconds"] == 30
    assert markout["lookback_days"] == 7.0 and markout["total_net_threshold_cents"] == -150.0
    assert markout["fee_factor"] == kalshi_screener_config.MARKOUT_FILTER_FEE_FACTOR


def test_bot_classes_rejects_unknown_class_field_managed_and_invalid_combinations():
    config = default_session_configuration()
    config["botClasses"]["enabled"] = True
    config["botClasses"]["weird"] = {"overrides": []}
    with pytest.raises(ValueError, match="unknown configuration field.*botClasses: weird"):
        validate_session_configuration(config)
    del config["botClasses"]["weird"]

    cases = [
        ([{"field": "not_a_setting", "value": 1}], "unknown bot field not_a_setting"),
        ([{"field": "market_ticker", "value": "X"}], "managed by the launcher"),
        ([{"field": "yes_order_budget_cents", "value": 5}], "managed by the launcher"),
        ([{"field": "telemetry_sqlite_path", "value": "x"}], "managed by the launcher"),
        # valid on its own, invalid combined with the base section (lead >= expiration)
        ([{"field": "expiration_refresh_lead_seconds", "value": config["bot"]["resting_order_expiration_seconds"]}],
         "botClasses.toxic: combined bot settings are invalid"),
        ([{"field": "maximum_contracts_per_order", "value": 0}], "combined bot settings are invalid"),
        ([{"field": "maximum_contracts_per_order", "value": "5"}], "must be an integer"),
        ([{"field": "post_only_quotes", "value": 1}], "must be true or false"),
        ([{"field": "markout_horizons_seconds", "value": 5}], "must be a list"),
        ([{"field": "maximum_contracts_per_order", "value": 5}, {"field": "maximum_contracts_per_order", "value": 6}],
         "more than once"),
        ([{"field": "maximum_contracts_per_order"}], 'exactly "field" and "value"'),
        ("nope", "must be a list"),
    ]
    for overrides, message in cases:
        config["botClasses"]["toxic"]["overrides"] = overrides
        with pytest.raises(ValueError, match=message):
            validate_session_configuration(config)
    config["botClasses"]["toxic"]["overrides"] = []

    config["botClasses"]["classifier"]["minSeriesFills"] = -1
    with pytest.raises(ValueError, match="minSeriesFills must be >= 0"):
        validate_session_configuration(config)
    config["botClasses"]["classifier"]["minSeriesFills"] = 10
    config["botClasses"]["enabled"] = "yes"
    with pytest.raises(ValueError, match="botClasses.enabled must be true or false"):
        validate_session_configuration(config)


def test_bot_classes_round_trip_through_store_and_payload(tmp_path: Path):
    config = default_session_configuration()
    config["botClasses"]["enabled"] = True
    config["botClasses"]["classifier"]["wideSpreadCents"] = 9
    config["botClasses"]["thinWide"]["overrides"] = [
        {"field": "maximum_contracts_per_order", "value": 2},
        {"field": "markout_horizons_seconds", "value": [1, 5]},
        {"field": "post_only_quotes", "value": True},
        {"field": "default_fee_factor_for_maker_quotes", "value": 1},
    ]
    normalized = validate_session_configuration(config)
    assert normalized["botClasses"]["classifier"]["wideSpreadCents"] == 9.0
    assert normalized["botClasses"]["thinWide"]["overrides"] == [
        {"field": "maximum_contracts_per_order", "value": 2},
        {"field": "markout_horizons_seconds", "value": [1, 5]},
        {"field": "post_only_quotes", "value": True},
        {"field": "default_fee_factor_for_maker_quotes", "value": 1.0},
    ]
    assert normalized["botClasses"]["toxic"] == {"overrides": []}
    assert normalized["bot"]["maximum_contracts_per_order"] == config["bot"]["maximum_contracts_per_order"]

    store = SessionStore(tmp_path / "session_data")
    created = store.create_session({"name": "Classes", "configuration": config})
    fetched = store.get_session(created["id"])["configuration"]
    assert fetched["botClasses"] == normalized["botClasses"]
    assert validate_session_configuration(fetched) == fetched

    overrides = resolve_overrides(fetched, "thinWide")
    payload = bot_settings_payload(fetched, overrides=overrides, market_ticker="MKT-1", yes_budget_cents=40)
    assert payload["maximum_contracts_per_order"] == 2
    assert payload["markout_horizons_seconds"] == [1, 5]
    assert payload["default_fee_factor_for_maker_quotes"] == 1.0
    assert payload["yes_order_budget_cents"] == 40
    assert bot_settings_payload(fetched, overrides=resolve_overrides(fetched, "default"), market_ticker="MKT-1")[
        "maximum_contracts_per_order"
    ] == config["bot"]["maximum_contracts_per_order"]


@pytest.mark.parametrize("value", [0, -1, MAX_CONCURRENT_BOTS + 1])
def test_configuration_rejects_unsafe_fleet_sizes(value):
    config = default_session_configuration()
    config["launcher"]["maxBots"] = value
    with pytest.raises(ValueError, match=f"between 1 and {MAX_CONCURRENT_BOTS}"):
        validate_session_configuration(config)


def test_bot_settings_file_applies_full_payload_and_cli_override(tmp_path: Path):
    config = default_session_configuration()
    payload = dict(config["bot"])
    payload.update(
        market_ticker="FILE-TICKER", yes_order_budget_cents=222, no_order_budget_cents=333,
        watchdog_state_file="state.json", watchdog_refresh_seconds=9,
        watchdog_extreme_stale_seconds=99, watchdog_flatten_retries=5,
        telemetry_sqlite_path="telemetry.sqlite3", pnl_tracker_path="fills.jsonl",
    )
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(payload))
    args = Namespace(
        settings_file=str(path), ticker="CLI-TICKER", yes_budget_cents=None, no_budget_cents=None,
        maximum_projected_contracts_per_line=None, watchdog_state_file="",
        watchdog_refresh_seconds=None, watchdog_extreme_stale_seconds=None, watchdog_flatten_retries=None,
    )
    settings = build_settings_from_args(args)
    assert settings.market_ticker == "CLI-TICKER"
    assert settings.yes_order_budget_cents == 222
    assert settings.watchdog_flatten_retries == 5
    assert settings.pnl_tracker_path == "fills.jsonl"


def test_session_lifecycle_archive_and_weighted_metrics(tmp_path: Path):
    store = SessionStore(tmp_path / "session_data")
    default = store.list_sessions()[0]
    other = store.create_session({"name": "Other", "configuration": default["configuration"]})
    store.select_session(other["id"])
    run = store.prepare_run()
    assert run["configuration"] == other["configuration"]
    with pytest.raises(SessionConflictError, match="locked"):
        store.select_session(default["id"])
    claimed = store.claim_run(run["id"])
    assert claimed["status"] == "starting"
    store.mark_running(run["id"])
    store.finish_run(run["id"], "stopped", metrics={
        "runtimeMs": 120_000, "orders": 12, "fills": 4, "apiCalls": 30,
        "apiErrors": 1, "totalCents": 25.5, "pnlComplete": True,
        "apiByComponent": {"bots": 20, "screener": 10},
    })
    summary = store.metrics(session_id=other["id"])["summary"]
    assert summary["ordersPerMinute"] == 6
    assert summary["fillsPerMinute"] == 2
    assert summary["totalCents"] == 25.5
    archived = store.archive_session(other["id"])
    assert archived["archivedAt"] is not None
    assert store.list_sessions()[0]["id"] == default["id"]


def test_metrics_accumulator_keeps_completed_process_counters():
    recorder = RunMetricsAccumulator(1)
    first = {
        "clients": [{"marketId": "A", "pid": 1, "runtime": {"startedAtMs": 10},
            "fills": {"count": 2}, "orderActivity": {"byAction": {"create": {"attempts": 3, "successes": 2, "errors": 1}}},
            "apiActivity": {"rest": {"total": 5, "errors": 1}},
            "pnl": {"realizedCents": 2, "unrealizedCents": 1, "totalCents": 3, "sessionPositionUnits": 0}}],
        "screener": {"apiActivity": {"rest": {"total": 4}}}, "portfolio": {},
    }
    assert recorder.observe(first)["orders"] == 3
    assert recorder.observe(first)["orderPlacementsAttempted"] == 3
    second = {"clients": [{**first["clients"][0], "pid": 2, "runtime": {"startedAtMs": 20}, "fills": {"count": 1}}], "screener": {}, "portfolio": {}}
    result = recorder.observe(second)
    assert result["fills"] == 3
    assert result["orderPlacementsAttempted"] == 6
    assert result["markets"]["A"]["orderPlacementsAttempted"] == 6
    assert result["apiByComponent"]["screener"] == 4


def test_screener_run_history_persists_and_aggregates(tmp_path: Path):
    store = SessionStore(tmp_path / "session_data")
    run = store.claim_run(store.prepare_run()["id"])
    record_id = store.start_screener_run(
        run["id"], reason="scheduled", started_at_ms=1000, configured_limit=20000,
    )
    store.finish_screener_run(
        record_id, status="succeeded",
        metrics={"startedAtMs": 1000, "endedAtMs": 1250, "durationMs": 250,
                 "configuredLimit": 20000, "scannedMarkets": 18000,
                 "apiRequests": 12, "added": 3, "changed": 4, "removed": 2},
        ended_at_ms=1250,
    )
    result = store.screener_runs(limit=10)
    assert result["items"][0]["scannedMarkets"] == 18000
    assert result["items"][0]["configuredLimit"] == 20000
    assert result["summary"] == {
        "totalRuns": 1, "succeeded": 1, "failed": 0, "interrupted": 0, "running": 0,
        "scannedMarkets": 18000, "apiRequests": 12, "averageDurationMs": 250.0,
        "added": 3, "changed": 4, "removed": 2,
    }


def test_screener_artifact_backfill_keeps_unknown_counts_null(tmp_path: Path):
    store = SessionStore(tmp_path / "session_data")
    run = store.claim_run(store.prepare_run()["id"])
    artifact = Path(run["artifactPath"]) / "screener"
    artifact.mkdir(parents=True)
    (artifact / "1000.json").write_text(json.dumps({
        "lastStartedAtMs": 1000, "lastCompletedAtMs": 1400,
        "lastDurationMs": 400, "generationId": 2,
        "changes": {"added": ["A"], "changed": [], "removed": ["B"]},
    }))
    result = store.screener_runs(limit=10)
    row = next(item for item in result["items"] if item["fleetRunId"] == run["id"])
    assert row["status"] == "succeeded"
    assert row["durationMs"] == 400
    assert row["added"] == 1 and row["removed"] == 1
    assert row["scannedMarkets"] is None
    assert row["apiRequests"] is None
    assert len(store.screener_runs(limit=10)["items"]) == 1


def test_screener_run_failure_and_parent_finalization_preserve_partial_metrics(tmp_path: Path):
    store = SessionStore(tmp_path / "session_data")
    run = store.claim_run(store.prepare_run()["id"])
    failed_id = store.start_screener_run(
        run["id"], reason="scheduled", started_at_ms=1_000, configured_limit=20_000,
    )
    pending_id = store.start_screener_run(
        run["id"], reason="scheduled", started_at_ms=2_000, configured_limit=20_000,
    )
    store.finish_screener_run(
        failed_id, status="failed", ended_at_ms=1_500,
        metrics={
            "startedAtMs": 1_000, "durationMs": 500, "scannedMarkets": 125,
            "apiRequests": 4, "apiErrors": 1, "warnings": ["partial response"],
            "error": "venue unavailable",
        },
    )
    store.finish_run(run["id"], "failed", error="launcher stopped")

    result = store.screener_runs(limit=10)
    failed = next(item for item in result["items"] if item["id"] == failed_id)
    interrupted = next(item for item in result["items"] if item["id"] == pending_id)
    assert failed["status"] == "failed"
    assert failed["scannedMarkets"] == 125
    assert failed["apiRequests"] == 4
    assert failed["warnings"] == ["partial response"]
    assert failed["error"] == "venue unavailable"
    assert interrupted["status"] == "interrupted"
    assert interrupted["endedAt"] is not None
    assert result["summary"]["failed"] == 1
    assert result["summary"]["interrupted"] == 1


def test_screener_run_cursor_paginates_without_changing_aggregate(tmp_path: Path):
    store = SessionStore(tmp_path / "session_data")
    run = store.claim_run(store.prepare_run()["id"])
    ids = []
    for started_at in (1_000, 2_000, 3_000):
        record_id = store.start_screener_run(
            run["id"], reason="scheduled", started_at_ms=started_at,
            configured_limit=20_000,
        )
        ids.append(record_id)
        store.finish_screener_run(
            record_id, status="succeeded", ended_at_ms=started_at + 100,
            metrics={"startedAtMs": started_at, "durationMs": 100},
        )

    first = store.screener_runs(limit=2)
    second = store.screener_runs(limit=2, cursor=first["nextCursor"])
    assert [item["id"] for item in first["items"]] == [ids[2], ids[1]]
    assert [item["id"] for item in second["items"]] == [ids[0]]
    assert second["nextCursor"] is None
    assert first["summary"] == second["summary"]
    assert first["summary"]["totalRuns"] == 3
    with pytest.raises(ValueError, match="cursor"):
        store.screener_runs(cursor="invalid")
