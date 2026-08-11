"""Versioned, credential-free launcher/session configuration."""

from __future__ import annotations

import copy
from dataclasses import asdict, fields
from typing import Any, Dict, Mapping


SCHEMA_VERSION = 1

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


def default_session_configuration() -> Dict[str, Any]:
    # Lazy import avoids a cycle when the bot entrypoint consumes settings files.
    from top_of_book_bot import BotSettings

    bot = BotSettings()
    bot_values = {
        item.name: _json_value(getattr(bot, item.name))
        for item in fields(BotSettings)
        if item.name not in _MANAGED_BOT_FIELDS
    }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "execution": {"useDemo": False, "dryRun": False, "subaccount": 0},
        "launcher": {
            "fixedTicker": "",
            "maxBots": 40,
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
        "bot": bot_values,
    }


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
    normalized = _merge_known(default_session_configuration(), value, "configuration")
    if normalized["schemaVersion"] != SCHEMA_VERSION:
        raise ValueError(f"schemaVersion must be {SCHEMA_VERSION}")

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
    if launcher["maxBots"] < 0:
        raise ValueError("launcher.maxBots must be >= 0")
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

    from top_of_book_bot import BotSettings

    bot_defaults = BotSettings()
    bot_values: Dict[str, Any] = {}
    tuple_fields = {item.name for item in fields(BotSettings) if isinstance(getattr(bot_defaults, item.name), tuple)}
    for key, raw in normalized["bot"].items():
        if key in tuple_fields:
            if not isinstance(raw, (list, tuple)):
                raise ValueError(f"bot.{key} must be a list")
            bot_values[key] = tuple(raw)
        else:
            bot_values[key] = raw
    settings = BotSettings(
        **bot_values,
        market_ticker=launcher["fixedTicker"] or "SESSION-VALIDATION",
        watchdog_refresh_seconds=watchdog["refreshSeconds"],
        watchdog_extreme_stale_seconds=watchdog["extremeStaleSeconds"],
        watchdog_flatten_retries=watchdog["flattenRetries"],
        yes_order_budget_cents=launcher["yesBudgetCents"],
        no_order_budget_cents=launcher["noBudgetCents"],
    )
    settings.validate()
    normalized["bot"] = {key: _json_value(value) for key, value in bot_values.items()}
    return normalized


def bot_settings_payload(configuration: Mapping[str, Any], **runtime: Any) -> Dict[str, Any]:
    config = validate_session_configuration(configuration)
    launcher = config["launcher"]
    watchdog = config["watchdog"]
    payload = dict(config["bot"])
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
    setattr(arguments, "fixed_ticker", launcher["fixedTicker"])
    return arguments
