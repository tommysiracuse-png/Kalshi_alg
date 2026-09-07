"""Versioned, credential-free launcher/session configuration."""

from __future__ import annotations

import copy
from dataclasses import asdict, fields
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from core.fleet_models import DEFAULT_MAX_BOTS, DEFAULT_SHARD_SIZE, MAX_CONCURRENT_BOTS, MAX_WORKERS


SCHEMA_VERSION = 4

# Schema v4 ``screener`` section: the market screener's filters, previously
# module constants in ``kalshi_screener_config.py`` only. Every default is read
# from that module so a session that never touched the section screens exactly
# like the constants do.
SCREENER_STATUS_VALUES: Tuple[str, ...] = ("open", "unopened", "paused", "closed", "settled")
# Kalshi ``mve_filter`` query values; "" sends no filter (every market).
SCREENER_MVE_FILTER_VALUES: Tuple[str, ...] = ("exclude", "only", "all", "")
# Markout horizons the fill telemetry records; the screener's toxic-series
# filter can only be evaluated at one of these.
SCREENER_MARKOUT_HORIZON_VALUES: Tuple[int, ...] = (1, 5, 30, 120)
# Fields dropped from the ``screener`` section after they shipped (never read
# by the filters); migration strips them from stored rows.
_SCREENER_REMOVED_FIELDS: Tuple[str, ...] = ("feeBufferCents",)

# camelCase session field -> (kalshi_screener_config constant, screener settings key)
_SCREENER_FIELDS: Tuple[Tuple[str, str, str], ...] = (
    ("status", "STATUS", "status"),
    ("mveFilter", "MVE_FILTER", "mve_filter"),
    ("maxMarketsToScan", "MAX_MARKETS_TO_SCAN", "max_markets_to_scan"),
    ("topN", "TOP_N", "top_n"),
    ("minSpreadCents", "MIN_SPREAD_CENTS", "min_spread_cents"),
    ("maxSpreadCents", "MAX_SPREAD_CENTS", "max_spread_cents"),
    ("minYesBidCents", "MIN_YES_BID_CENTS", "min_yes_bid_cents"),
    ("minNoBidCents", "MIN_NO_BID_CENTS", "min_no_bid_cents"),
    ("minVol24h", "MIN_VOL24H", "min_vol24h"),
    ("minOpenInterest", "MIN_OI", "min_oi"),
    ("minTimeToCloseHours", "MIN_TIME_TO_CLOSE_HRS", "min_time_to_close_hrs"),
    ("maxTimeToCloseHours", "MAX_TIME_TO_CLOSE_HRS", "max_time_to_close_hrs"),
    ("excludedTickerKeywords", "EXCLUDED_TICKER_KEYWORDS", "excluded_ticker_keywords"),
    ("targetEdgeCents", "TARGET_EDGE_CENTS", "target_edge_cents"),
    ("quoteSize", "QUOTE_SIZE", "quote_size"),
    ("markoutFilterEnabled", "MARKOUT_FILTER_ENABLED", "markout_filter_enabled"),
    ("markoutFilterNetThresholdCents", "MARKOUT_FILTER_NET_THRESHOLD_CENTS", "markout_filter_net_threshold_cents"),
    ("markoutFilterTickerMinFills", "MARKOUT_FILTER_TICKER_MIN_FILLS", "markout_filter_ticker_min_fills"),
    ("markoutFilterSeriesMinFills", "MARKOUT_FILTER_SERIES_MIN_FILLS", "markout_filter_series_min_fills"),
    ("markoutFilterHorizonSeconds", "MARKOUT_FILTER_HORIZON_SECONDS", "markout_filter_horizon_seconds"),
    ("markoutFilterLookbackDays", "MARKOUT_FILTER_LOOKBACK_DAYS", "markout_filter_lookback_days"),
    ("markoutFilterTotalNetThresholdCents", "MARKOUT_FILTER_TOTAL_NET_THRESHOLD_CENTS", "markout_filter_total_net_threshold_cents"),
)
_SCREENER_INTEGER_FIELDS = frozenset({
    "maxMarketsToScan", "topN", "minSpreadCents", "maxSpreadCents", "minYesBidCents", "minNoBidCents",
    "targetEdgeCents", "quoteSize", "markoutFilterTickerMinFills",
    "markoutFilterSeriesMinFills", "markoutFilterHorizonSeconds",
})
_SCREENER_FLOAT_FIELDS = frozenset({
    "minVol24h", "minOpenInterest", "minTimeToCloseHours", "maxTimeToCloseHours",
    "markoutFilterNetThresholdCents", "markoutFilterLookbackDays", "markoutFilterTotalNetThresholdCents",
})
# Numbers that must be >= 0 (thresholds in cents may legitimately be negative).
_SCREENER_NON_NEGATIVE_FIELDS = frozenset({
    "minSpreadCents", "maxSpreadCents", "minYesBidCents", "minNoBidCents", "minVol24h", "minOpenInterest",
    "minTimeToCloseHours", "maxTimeToCloseHours", "targetEdgeCents",
    "markoutFilterTickerMinFills", "markoutFilterSeriesMinFills", "markoutFilterLookbackDays",
})
# Numbers that must be >= 1.
_SCREENER_POSITIVE_FIELDS = frozenset({"maxMarketsToScan", "topN", "quoteSize"})

# Per-market-class bot override groups (schema v3 ``botClasses``). The
# classifier assigns every screened market exactly one of these names; the
# class's ``overrides`` list is layered on top of ``bot`` when the market's
# settings payload is built. Order is the classifier's precedence.
MARKET_CLASS_NAMES: Tuple[str, ...] = ("toxic", "thinWide", "thickCalm", "default")

_MANAGED_BOT_FIELDS = {
    "market_ticker",
    "watchdog_state_file",
    "watchdog_refresh_seconds",
    "watchdog_extreme_stale_seconds",
    "watchdog_flatten_retries",
    "yes_order_budget_cents",
    "no_order_budget_cents",
    "telemetry_sqlite_path",
    "pnl_tracker_path",
}


def _json_value(value: Any) -> Any:
    if isinstance(value, tuple):
        return list(value)
    return value


def default_bot_classes_configuration() -> Dict[str, Any]:
    """Schema-v3 ``botClasses`` section with classification off and no overrides.

    Override lists are the deliberate escape hatch through ``_merge_known``
    (list-valued defaults pass through unrecursed), so each entry is an object
    ``{"field": <BotSettings field>, "value": <value>}`` rather than a nested
    mapping keyed by field name.
    """
    return {
        "enabled": False,
        "classifier": {
            "thinDepthContracts": 30,
            "wideSpreadCents": 12,
            "toxicNetMarkoutCentsPerContract": -1.0,
            "minSeriesFills": 10,
        },
        "thickCalm": {"overrides": []},
        "thinWide": {"overrides": []},
        "toxic": {"overrides": []},
        "default": {"overrides": []},
    }


def default_screener_configuration() -> Dict[str, Any]:
    """Schema-v4 ``screener`` section seeded from ``kalshi_screener_config``.

    The module only defines constants, so importing it here is cheap and keeps
    the session defaults equal to the file by construction: a stored v1/v2/v3
    session that gains this section at migration time screens exactly as it
    did before the section existed.
    """
    from screeners import kalshi_screener_config as screener_config

    section: Dict[str, Any] = {}
    for field_name, constant, _ in _SCREENER_FIELDS:
        value = getattr(screener_config, constant)
        if field_name == "excludedTickerKeywords":
            value = [str(item) for item in value]
        elif field_name in _SCREENER_FLOAT_FIELDS:
            value = float(value)
        elif field_name in _SCREENER_INTEGER_FIELDS:
            value = int(value)
        section[field_name] = value
    return section


def default_session_configuration() -> Dict[str, Any]:
    # Lazy import avoids a cycle when the bot entrypoint consumes settings files.
    from bots.top_of_book_bot import BotSettings

    bot = BotSettings()
    bot_values = {
        item.name: _json_value(getattr(bot, item.name))
        for item in fields(BotSettings)
        if item.name not in _MANAGED_BOT_FIELDS
    }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "venue": "kalshi",
        "execution": {"useDemo": False, "dryRun": False, "subaccount": 0},
        "launcher": {
            "fixedTicker": "",
            "maxBots": DEFAULT_MAX_BOTS,
            "yesBudgetCents": 100,
            "noBudgetCents": 100,
            "launchDelaySeconds": 0.5,
            "runScreenerOnStart": True,
            "refreshIntervalSeconds": 1200.0,
            "pollSeconds": 60.0,
            "minimumCarryoverValueCents": 20.0,
        },
        "watchdog": {
            "intervalSeconds": 60.0,
            "refreshSeconds": 3.0,
            "extremeStaleSeconds": 120.0,
            "sampleSeconds": 2.5,
            "pollIntervalSeconds": 0.35,
            "confidenceReductionThreshold": 0.68,
            "confidenceFlattenThreshold": 0.55,
            "flattenRetries": 2,
        },
        "fleetRuntime": {
            "shardSize": DEFAULT_SHARD_SIZE,
            "quoteFreshnessSeconds": 20.0,
            "writeUtilizationLimit": 0.85,
            "readUtilizationLimit": 0.80,
            "cashReserveFraction": 0.20,
            "seriesExposureFraction": 0.10,
            # Side budgets may be committed up to allocatable cash x this
            # multiplier per exchange shard. 1.0 (default) = one full budget
            # per side of un-multiplied cash, the historical rule, with no
            # other behaviour change. Above 1.0 the execution broker enforces
            # the live cap (un-multiplied allocatable shard cash) on every
            # new order between allocations, the allocator withholds new
            # sides on a shard at the cap (sticky until it falls below 80%),
            # and the screener caps picks per shard at fundable first sides.
            "allocationOversubscription": 1.0,
            "workerHeartbeatSeconds": 2.0,
            "workerStaleSeconds": 5.0,
            "startupTimeoutSeconds": 300.0,
            # Worker-local risk evaluator (fleet_runtime/risk.py). The
            # defaults reproduce the previously hard-coded 7.5c / 20c mid-move
            # and 120 s staleness rules; the move window is time-bounded.
            "riskWindowSeconds": 120.0,
            "riskElevatedMoveCents": 7.5,
            "riskExtremeMoveCents": 20.0,
            "riskStaleSeconds": 120.0,
        },
        "bot": bot_values,
        "botClasses": default_bot_classes_configuration(),
        "screener": default_screener_configuration(),
    }


def migrate_session_configuration(value: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a current-schema copy while preserving every older-schema setting.

    v1 -> v2 splices the ``fleetRuntime`` defaults; v2 -> v3 splices the
    ``botClasses`` defaults (classification disabled, no overrides); v3 -> v4
    splices the ``screener`` defaults (the ``kalshi_screener_config``
    constants), so every stored v1/v2/v3 row keeps its exact runtime
    behaviour after migration. Fields added to an existing section within a
    schema version (the ``fleetRuntime.risk*`` evaluator tunables) are
    filled with their defaults by ``validate_session_configuration``; fields
    retired from the ``screener`` section are dropped from stored rows here.
    """

    if not isinstance(value, Mapping):
        raise ValueError("configuration must be an object")
    migrated = copy.deepcopy(dict(value))
    version = migrated.get("schemaVersion", 1)
    if version not in (1, 2, 3, SCHEMA_VERSION):
        raise ValueError(f"schemaVersion must be 1, 2, 3 or {SCHEMA_VERSION}")
    if version == 1:
        migrated["fleetRuntime"] = copy.deepcopy(default_session_configuration()["fleetRuntime"])
    if version < 3:
        migrated.setdefault("botClasses", default_bot_classes_configuration())
    if version < 4:
        migrated.setdefault("screener", default_screener_configuration())
    migrated.setdefault("venue", "kalshi")
    screener = migrated.get("screener")
    if isinstance(screener, Mapping):
        migrated["screener"] = {
            key: item for key, item in screener.items() if key not in _SCREENER_REMOVED_FIELDS
        }
    migrated["schemaVersion"] = SCHEMA_VERSION
    return migrated


def _merge_known(defaults: Mapping[str, Any], supplied: Mapping[str, Any], path: str) -> Dict[str, Any]:
    unknown = sorted(set(supplied) - set(defaults))
    if unknown:
        raise ValueError(f"unknown configuration field(s) at {path}: {', '.join(unknown)}")
    result: Dict[str, Any] = {}
    for key, default in defaults.items():
        value = supplied.get(key, copy.deepcopy(default))
        if isinstance(default, dict):
            if not isinstance(value, Mapping):
                raise ValueError(f"{path}.{key} must be an object")
            result[key] = _merge_known(default, value, f"{path}.{key}")
        else:
            result[key] = value
    return result


def _require_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be true or false")
    return value


def _require_number(value: Any, name: str, *, integer: bool = False) -> float | int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return int(value) if integer else float(value)


def validate_session_configuration(value: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("configuration must be an object")
    normalized = _merge_known(default_session_configuration(), migrate_session_configuration(value), "configuration")
    if normalized["schemaVersion"] != SCHEMA_VERSION:
        raise ValueError(f"schemaVersion must be {SCHEMA_VERSION}")

    venue = normalized["venue"]
    if not isinstance(venue, str) or not venue.strip():
        raise ValueError("venue must be a non-empty string")
    venue = venue.strip().lower()
    if venue not in {"kalshi", "polymarket"}:
        raise ValueError(f"unsupported venue: {venue}")
    normalized["venue"] = venue

    execution = normalized["execution"]
    execution["useDemo"] = _require_bool(execution["useDemo"], "execution.useDemo")
    execution["dryRun"] = _require_bool(execution["dryRun"], "execution.dryRun")
    execution["subaccount"] = _require_number(execution["subaccount"], "execution.subaccount", integer=True)
    if execution["subaccount"] < 0:
        raise ValueError("execution.subaccount must be >= 0")

    launcher = normalized["launcher"]
    if not isinstance(launcher["fixedTicker"], str):
        raise ValueError("launcher.fixedTicker must be a string")
    launcher["fixedTicker"] = launcher["fixedTicker"].strip()
    launcher["maxBots"] = _require_number(launcher["maxBots"], "launcher.maxBots", integer=True)
    launcher["yesBudgetCents"] = _require_number(launcher["yesBudgetCents"], "launcher.yesBudgetCents", integer=True)
    launcher["noBudgetCents"] = _require_number(launcher["noBudgetCents"], "launcher.noBudgetCents", integer=True)
    for key in ("launchDelaySeconds", "refreshIntervalSeconds", "pollSeconds", "minimumCarryoverValueCents"):
        launcher[key] = _require_number(launcher[key], f"launcher.{key}")
    launcher["runScreenerOnStart"] = _require_bool(launcher["runScreenerOnStart"], "launcher.runScreenerOnStart")
    if not 1 <= launcher["maxBots"] <= MAX_CONCURRENT_BOTS:
        raise ValueError(
            f"launcher.maxBots must be between 1 and {MAX_CONCURRENT_BOTS}"
        )
    if launcher["yesBudgetCents"] < 0 or launcher["noBudgetCents"] < 0:
        raise ValueError("launcher budgets must be >= 0")
    if launcher["launchDelaySeconds"] < 0 or launcher["refreshIntervalSeconds"] < 0:
        raise ValueError("launcher delays must be >= 0")
    if launcher["pollSeconds"] <= 0:
        raise ValueError("launcher.pollSeconds must be > 0")
    if launcher["minimumCarryoverValueCents"] < 0:
        raise ValueError("launcher.minimumCarryoverValueCents must be >= 0")

    watchdog = normalized["watchdog"]
    for key in ("intervalSeconds", "refreshSeconds", "extremeStaleSeconds", "sampleSeconds", "pollIntervalSeconds", "confidenceReductionThreshold", "confidenceFlattenThreshold"):
        watchdog[key] = _require_number(watchdog[key], f"watchdog.{key}")
    watchdog["flattenRetries"] = _require_number(watchdog["flattenRetries"], "watchdog.flattenRetries", integer=True)
    if any(watchdog[key] < 0 for key in ("intervalSeconds", "refreshSeconds", "extremeStaleSeconds", "sampleSeconds", "pollIntervalSeconds", "flattenRetries")):
        raise ValueError("watchdog intervals and retries must be >= 0")
    if not 0 <= watchdog["confidenceFlattenThreshold"] <= watchdog["confidenceReductionThreshold"] <= 1:
        raise ValueError("watchdog confidence thresholds must satisfy 0 <= flatten <= reduction <= 1")

    fleet = normalized["fleetRuntime"]
    fleet["shardSize"] = _require_number(fleet["shardSize"], "fleetRuntime.shardSize", integer=True)
    for key in (
        "quoteFreshnessSeconds", "writeUtilizationLimit", "readUtilizationLimit",
        "cashReserveFraction", "seriesExposureFraction", "allocationOversubscription", "workerHeartbeatSeconds",
        "workerStaleSeconds", "startupTimeoutSeconds",
        "riskWindowSeconds", "riskElevatedMoveCents", "riskExtremeMoveCents", "riskStaleSeconds",
    ):
        fleet[key] = _require_number(fleet[key], f"fleetRuntime.{key}")
    if not 1.0 <= fleet["allocationOversubscription"] <= 10.0:
        raise ValueError("fleetRuntime.allocationOversubscription must be between 1.0 and 10.0")
    for key in ("riskWindowSeconds", "riskStaleSeconds"):
        if fleet[key] <= 0:
            raise ValueError(f"fleetRuntime.{key} must be > 0")
    for key in ("riskElevatedMoveCents", "riskExtremeMoveCents"):
        if not 0 < fleet[key] <= 100:
            raise ValueError(f"fleetRuntime.{key} must be between 0 and 100 cents")
    if fleet["riskElevatedMoveCents"] > fleet["riskExtremeMoveCents"]:
        raise ValueError("fleetRuntime.riskElevatedMoveCents must be <= fleetRuntime.riskExtremeMoveCents")
    if not 1 <= fleet["shardSize"] <= DEFAULT_SHARD_SIZE:
        raise ValueError(f"fleetRuntime.shardSize must be between 1 and {DEFAULT_SHARD_SIZE}")
    if launcher["maxBots"] > fleet["shardSize"] * MAX_WORKERS:
        raise ValueError("fleetRuntime.shardSize does not provide enough worker capacity for launcher.maxBots")
    if fleet["quoteFreshnessSeconds"] <= 0:
        raise ValueError("fleetRuntime.quoteFreshnessSeconds must be > 0")
    for key in ("writeUtilizationLimit", "readUtilizationLimit", "cashReserveFraction", "seriesExposureFraction"):
        if not 0 < fleet[key] < 1:
            raise ValueError(f"fleetRuntime.{key} must be between 0 and 1")
    if fleet["workerHeartbeatSeconds"] <= 0:
        raise ValueError("fleetRuntime.workerHeartbeatSeconds must be > 0")
    if fleet["workerStaleSeconds"] <= fleet["workerHeartbeatSeconds"]:
        raise ValueError("fleetRuntime.workerStaleSeconds must exceed workerHeartbeatSeconds")
    if fleet["startupTimeoutSeconds"] <= 0:
        raise ValueError("fleetRuntime.startupTimeoutSeconds must be > 0")

    from bots.top_of_book_bot import BotSettings

    bot_defaults = BotSettings()
    tuple_fields = {item.name for item in fields(BotSettings) if isinstance(getattr(bot_defaults, item.name), tuple)}
    bot_values = _bot_settings_kwargs(normalized["bot"], tuple_fields, "bot")
    _build_bot_settings(bot_values, launcher, watchdog).validate()
    normalized["bot"] = {key: _json_value(value) for key, value in bot_values.items()}
    normalized["botClasses"] = _validate_bot_classes(
        normalized["botClasses"], bot_values, launcher, watchdog, bot_defaults, tuple_fields,
    )
    normalized["screener"] = _validate_screener(normalized["screener"])
    return normalized


def _validate_screener(section: Mapping[str, Any]) -> Dict[str, Any]:
    """Type- and range-check the ``screener`` section (unknown keys were rejected by ``_merge_known``)."""
    result: Dict[str, Any] = {}
    for field_name, _, _ in _SCREENER_FIELDS:
        path = f"screener.{field_name}"
        raw = section[field_name]
        if field_name in ("status", "mveFilter"):
            if not isinstance(raw, str):
                raise ValueError(f"{path} must be a string")
            value = raw.strip().lower()
            allowed = SCREENER_STATUS_VALUES if field_name == "status" else SCREENER_MVE_FILTER_VALUES
            if value not in allowed:
                choices = ", ".join(repr(item) for item in allowed)
                raise ValueError(f"{path} must be one of {choices}")
            result[field_name] = value
        elif field_name == "excludedTickerKeywords":
            if not isinstance(raw, (list, tuple)):
                raise ValueError(f"{path} must be a list of strings")
            keywords: List[str] = []
            for index, item in enumerate(raw):
                if not isinstance(item, str) or not item.strip():
                    raise ValueError(f"{path}[{index}] must be a non-empty string")
                keyword = item.strip()
                if keyword not in keywords:
                    keywords.append(keyword)
            result[field_name] = keywords
        elif field_name == "markoutFilterEnabled":
            result[field_name] = _require_bool(raw, path)
        else:
            value = _require_number(raw, path, integer=field_name in _SCREENER_INTEGER_FIELDS)
            if field_name in _SCREENER_INTEGER_FIELDS and isinstance(raw, float) and not raw.is_integer():
                raise ValueError(f"{path} must be an integer")
            if field_name in _SCREENER_NON_NEGATIVE_FIELDS and value < 0:
                raise ValueError(f"{path} must be >= 0")
            if field_name in _SCREENER_POSITIVE_FIELDS and value < 1:
                raise ValueError(f"{path} must be >= 1")
            if field_name == "markoutFilterHorizonSeconds" and value not in SCREENER_MARKOUT_HORIZON_VALUES:
                choices = ", ".join(str(item) for item in SCREENER_MARKOUT_HORIZON_VALUES)
                raise ValueError(f"{path} must be one of {choices} (the recorded markout horizons)")
            result[field_name] = value
    if result["minSpreadCents"] > result["maxSpreadCents"]:
        raise ValueError("screener.minSpreadCents must be <= screener.maxSpreadCents")
    if result["minTimeToCloseHours"] > result["maxTimeToCloseHours"]:
        raise ValueError("screener.minTimeToCloseHours must be <= screener.maxTimeToCloseHours")
    for key in ("minYesBidCents", "minNoBidCents", "minSpreadCents", "maxSpreadCents"):
        if result[key] > 99:
            raise ValueError(f"screener.{key} must be <= 99 cents")
    return result


def screener_settings_from_configuration(configuration: Mapping[str, Any]) -> Dict[str, Any]:
    """Session ``screener`` section as the snake_case keys ``kalshi_screener`` filters read.

    Returns only the session-controlled keys; callers overlay them on the
    CLI/config defaults from ``kalshi_screener.build_settings_from_args`` so
    file-only settings (tick size, EV tuning, markout fee factor) stay intact.
    """
    section = validate_session_configuration(configuration)["screener"]
    settings: Dict[str, Any] = {}
    for field_name, _, settings_key in _SCREENER_FIELDS:
        value = section[field_name]
        settings[settings_key] = list(value) if isinstance(value, list) else value
    return settings


def _bot_settings_kwargs(section: Mapping[str, Any], tuple_fields: set, path: str) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    for key, raw in section.items():
        if key in tuple_fields:
            if not isinstance(raw, (list, tuple)):
                raise ValueError(f"{path}.{key} must be a list")
            values[key] = tuple(raw)
        else:
            values[key] = raw
    return values


def _build_bot_settings(bot_values: Mapping[str, Any], launcher: Mapping[str, Any], watchdog: Mapping[str, Any]):
    from bots.top_of_book_bot import BotSettings

    return BotSettings(
        **bot_values,
        market_ticker=launcher["fixedTicker"] or "SESSION-VALIDATION",
        watchdog_refresh_seconds=watchdog["refreshSeconds"],
        watchdog_extreme_stale_seconds=watchdog["extremeStaleSeconds"],
        watchdog_flatten_retries=watchdog["flattenRetries"],
        yes_order_budget_cents=launcher["yesBudgetCents"],
        no_order_budget_cents=launcher["noBudgetCents"],
    )


def _override_value(field_name: str, default: Any, value: Any, path: str) -> Any:
    """Type-check one override against the field's default (JSON-friendly)."""
    if isinstance(default, bool):
        if not isinstance(value, bool):
            raise ValueError(f"{path} ({field_name}) must be true or false")
        return value
    if isinstance(default, int):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{path} ({field_name}) must be an integer")
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"{path} ({field_name}) must be an integer")
        return int(value)
    if isinstance(default, float):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{path} ({field_name}) must be a number")
        return float(value)
    if isinstance(default, str):
        if not isinstance(value, str):
            raise ValueError(f"{path} ({field_name}) must be a string")
        return value
    if isinstance(default, tuple):
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"{path} ({field_name}) must be a list")
        return tuple(value)
    return value


def _validate_bot_classes(
    classes: Mapping[str, Any],
    bot_values: Mapping[str, Any],
    launcher: Mapping[str, Any],
    watchdog: Mapping[str, Any],
    bot_defaults: Any,
    tuple_fields: set,
) -> Dict[str, Any]:
    """Validate ``botClasses``: thresholds plus one merged ``BotSettings`` per class.

    Every override must name a session-exposed ``BotSettings`` field (managed
    launcher-injected fields are refused), carry a value of the field's type,
    and the base ``bot`` section with the class's overrides applied must pass
    ``BotSettings.validate()`` exactly like the base section does.
    """
    result: Dict[str, Any] = {"enabled": _require_bool(classes["enabled"], "botClasses.enabled")}
    classifier = dict(classes["classifier"])
    for key in ("thinDepthContracts", "wideSpreadCents", "toxicNetMarkoutCentsPerContract"):
        classifier[key] = _require_number(classifier[key], f"botClasses.classifier.{key}")
    classifier["minSeriesFills"] = _require_number(
        classifier["minSeriesFills"], "botClasses.classifier.minSeriesFills", integer=True
    )
    for key in ("thinDepthContracts", "wideSpreadCents", "minSeriesFills"):
        if classifier[key] < 0:
            raise ValueError(f"botClasses.classifier.{key} must be >= 0")
    result["classifier"] = classifier

    for class_name in MARKET_CLASS_NAMES:
        path = f"botClasses.{class_name}.overrides"
        raw_overrides = classes[class_name]["overrides"]
        if not isinstance(raw_overrides, (list, tuple)):
            raise ValueError(f"{path} must be a list")
        merged = dict(bot_values)
        normalized: List[Dict[str, Any]] = []
        seen: set = set()
        for index, entry in enumerate(raw_overrides):
            entry_path = f"{path}[{index}]"
            if not isinstance(entry, Mapping) or set(entry) != {"field", "value"}:
                raise ValueError(f'{entry_path} must be an object with exactly "field" and "value"')
            field_name = entry["field"]
            if not isinstance(field_name, str) or not field_name:
                raise ValueError(f"{entry_path}.field must be a non-empty string")
            if field_name in _MANAGED_BOT_FIELDS:
                raise ValueError(
                    f"{entry_path}: {field_name} is managed by the launcher and cannot be overridden per class"
                )
            if field_name not in bot_values:
                raise ValueError(f"{entry_path}: unknown bot field {field_name}")
            if field_name in seen:
                raise ValueError(f"{entry_path}: {field_name} is overridden more than once")
            seen.add(field_name)
            value = _override_value(field_name, getattr(bot_defaults, field_name), entry["value"], entry_path)
            merged[field_name] = value
            normalized.append({"field": field_name, "value": _json_value(value)})
        try:
            _build_bot_settings(merged, launcher, watchdog).validate()
        except (ValueError, TypeError) as exc:
            raise ValueError(f"botClasses.{class_name}: combined bot settings are invalid: {exc}") from exc
        result[class_name] = {"overrides": normalized}
    return result


def bot_settings_payload(
    configuration: Mapping[str, Any],
    *,
    overrides: Iterable[Tuple[str, Any]] | Mapping[str, Any] = (),
    **runtime: Any,
) -> Dict[str, Any]:
    """Credential-free ``BotSettings`` payload for one market.

    ``overrides`` (``(field, value)`` pairs, e.g. a ``ScreenerPick``'s
    ``settings_overrides`` resolved from its market class) are layered on top
    of the session's ``bot`` section BEFORE the launcher-managed fields are
    injected, so per-class settings can never touch budgets, tickers or
    telemetry paths.
    """
    config = validate_session_configuration(configuration)
    launcher = config["launcher"]
    watchdog = config["watchdog"]
    payload = dict(config["bot"])
    override_items = overrides.items() if isinstance(overrides, Mapping) else overrides
    applied = False
    for field_name, value in override_items:
        if field_name in _MANAGED_BOT_FIELDS:
            raise ValueError(f"settings override {field_name} targets a launcher-managed field")
        if field_name not in payload:
            raise ValueError(f"settings override {field_name} is not a session bot field")
        payload[field_name] = _json_value(value)
        applied = True
    if applied:
        from bots.top_of_book_bot import BotSettings

        defaults = BotSettings()
        tuple_fields = {item.name for item in fields(BotSettings) if isinstance(getattr(defaults, item.name), tuple)}
        try:
            _build_bot_settings(_bot_settings_kwargs(payload, tuple_fields, "bot"), launcher, watchdog).validate()
        except (ValueError, TypeError) as exc:
            raise ValueError(f"settings overrides produce invalid bot settings: {exc}") from exc
    payload.update(
        market_ticker=runtime["market_ticker"],
        watchdog_state_file=str(runtime.get("watchdog_state_file") or ""),
        watchdog_refresh_seconds=watchdog["refreshSeconds"],
        watchdog_extreme_stale_seconds=watchdog["extremeStaleSeconds"],
        watchdog_flatten_retries=watchdog["flattenRetries"],
        yes_order_budget_cents=int(runtime.get("yes_budget_cents", launcher["yesBudgetCents"])),
        no_order_budget_cents=int(runtime.get("no_budget_cents", launcher["noBudgetCents"])),
        telemetry_sqlite_path=str(runtime.get("telemetry_sqlite_path") or ""),
        pnl_tracker_path=str(runtime.get("pnl_tracker_path") or ""),
    )
    return payload


def apply_configuration_to_arguments(arguments: Any, configuration: Mapping[str, Any]) -> Any:
    config = validate_session_configuration(configuration)
    execution, launcher, watchdog = config["execution"], config["launcher"], config["watchdog"]
    mapping = {
        "use_demo": execution["useDemo"], "dry_run": execution["dryRun"], "subaccount": execution["subaccount"],
        "max_bots": launcher["maxBots"], "yes_budget_cents": launcher["yesBudgetCents"], "no_budget_cents": launcher["noBudgetCents"],
        "launch_delay_seconds": launcher["launchDelaySeconds"], "run_screener_on_start": launcher["runScreenerOnStart"],
        "refresh_interval_seconds": launcher["refreshIntervalSeconds"], "poll_seconds": launcher["pollSeconds"],
        "minimum_carryover_value_cents": launcher["minimumCarryoverValueCents"],
        "watchdog_interval_seconds": watchdog["intervalSeconds"], "watchdog_state_refresh_seconds": watchdog["refreshSeconds"],
        "watchdog_extreme_stale_seconds": watchdog["extremeStaleSeconds"], "watchdog_sample_seconds": watchdog["sampleSeconds"],
        "watchdog_poll_interval_seconds": watchdog["pollIntervalSeconds"],
        "watchdog_confidence_reduction_threshold": watchdog["confidenceReductionThreshold"],
        "watchdog_confidence_flatten_threshold": watchdog["confidenceFlattenThreshold"],
    }
    for name, item in mapping.items():
        setattr(arguments, name, item)
    setattr(arguments, "venue", config.get("venue", "kalshi"))
    setattr(arguments, "fixed_ticker", launcher["fixedTicker"])
    return arguments
