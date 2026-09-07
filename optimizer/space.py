"""Auto-built parameter search space from session-exposed bot parameters.

The space is derived from ``session_config.default_session_configuration()["bot"]``
so every session-exposed field is considered automatically.  Numeric bounds start
at default x [0.25, 4.0] and are shrunk by trial against ``BotSettings.validate()``
(via ``session_config.bot_settings_payload``) so every bound is constructible.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

FAIR_MID = "fair_value_mid_weight"
FAIR_TICKER = "fair_value_ticker_weight"
FAIR_TRADE = "fair_value_trade_weight"

# Pinned by explicit policy (identity/plumbing/lifecycle fields).
EXCLUDED_EXACT = {
    "enable_sqlite_telemetry",
    "enable_queue_position_logging",
    "queue_position_log_interval_seconds",
    "primary_client_order_prefix",
    "legacy_client_order_prefixes",
    "markout_horizons_seconds",
    "subscribe_to_market_positions_channel",
    "cancel_strategy_quotes_on_startup",
    "post_only_quotes",
    "cancel_quotes_if_exchange_pauses",
    "expiration_jitter_seconds",
}

# Managed (launcher-injected) fields that the replay driver routes specially;
# searchable so sizing/budget scale can be optimized. Bounds are explicit.
MANAGED_DIMS = {
    "yes_order_budget_cents": (100, 100, 2500),   # (default, low, high) in cents
    "no_order_budget_cents": (100, 100, 2500),
}

# Explicit wide bounds for scale-relevant fields; the default x[0.25, 4] span is
# far too narrow to explore serious size (advanced-tier account headroom).
SCALE_BOUNDS = {
    "maximum_contracts_per_order": (1, 100),
    "maximum_projected_contracts_per_line": (2, 300),
    "one_way_inventory_guard_contracts": (1, 150),
    "resting_order_expiration_seconds": (30, 3600),
    "trade_history_window_seconds": (10, 600),
    "model_refresh_interval_seconds": (30, 600),
}

# Depth/queue fields the Tier-1 replay cannot measure. They become searchable
# at Tier 2 (recorded full-depth order books with a queue model).
TIER2_EXACT = {"minimum_top_level_depth_contracts", "maximum_top_level_gap_cents"}
TIER2_PREFIXES = ("queue_abandonment_", "maximum_queue_ahead_")
ORDERBOOK_PULL_KEEP = {"orderbook_pull_penalty_cents"}

# Explicit wide Tier-2 bounds (still shrunk by trial against validate()).
TIER2_BOUNDS = {
    "orderbook_pull_absolute_threshold_contracts": (50, 20_000),
    "orderbook_pull_relative_depth_threshold": (0.05, 1.0),
    "orderbook_pull_side_cooldown_ms": (250, 10_000),
    "orderbook_pull_market_cooldown_ms": (250, 10_000),
    "orderbook_pull_window_ms": (250, 5_000),
    "minimum_top_level_depth_contracts": (0, 200),
}


def is_tier2_field(name: str) -> bool:
    """True for the depth/queue/pull group pinned at Tier 1."""
    if name in TIER2_EXACT:
        return True
    if any(name.startswith(prefix) for prefix in TIER2_PREFIXES):
        return True
    return name.startswith("orderbook_pull_") and name not in ORDERBOOK_PULL_KEEP


@dataclass(frozen=True)
class ParamDim:
    name: str
    kind: str  # "bool" | "int" | "float"
    default: Any
    low: float = 0.0
    high: float = 0.0

    def to_json(self) -> Dict[str, Any]:
        return {"kind": self.kind, "default": self.default, "low": self.low, "high": self.high}


@dataclass
class SearchSpace:
    dims: Dict[str, ParamDim]
    excluded: Dict[str, str]  # field name -> reason
    defaults: Dict[str, Any]  # full default config["bot"]
    tier: int = 1  # data fidelity tier the space was built for

    def to_json(self) -> Dict[str, Any]:
        return {
            "tier": self.tier,
            "dims": {name: dim.to_json() for name, dim in sorted(self.dims.items())},
            "excluded": dict(sorted(self.excluded.items())),
        }


def restrict_space(space: SearchSpace, names: List[str]) -> SearchSpace:
    """A copy of ``space`` whose searchable dims are exactly ``names`` (in that order).

    Used for a targeted run (``--only-params``): the sensitivity screen and the
    search touch only these fields; everything else stays at the seeded base.
    """
    unknown = [name for name in names if name not in space.dims]
    if unknown:
        raise ValueError(
            "not searchable in this space: " + ", ".join(unknown)
            + " (pinned: " + ", ".join(sorted(n for n in unknown if n in space.excluded)) + ")"
        )
    dims = {name: space.dims[name] for name in names}
    excluded = dict(space.excluded)
    for name in space.dims:
        if name not in dims:
            excluded[name] = "not in --only-params"
    return SearchSpace(dims=dims, excluded=excluded, defaults=space.defaults, tier=space.tier)


def _base_configuration() -> Dict[str, Any]:
    from core import session_config

    return session_config.default_session_configuration()


def is_valid_bot_overrides(overrides: Dict[str, Any]) -> bool:
    """Trial-construct BotSettings (through the session pipeline) and validate."""
    from core import session_config
    from bots.top_of_book_bot import BotSettings

    cfg = session_config.default_session_configuration()
    managed: Dict[str, Any] = {}
    for key, value in overrides.items():
        if key in MANAGED_DIMS:
            managed[key] = value
            continue
        if key not in cfg["bot"]:
            return False
        cfg["bot"][key] = value
    try:
        payload = session_config.bot_settings_payload(
            cfg,
            market_ticker="SPACE-VALIDATION",
            yes_budget_cents=int(managed.get("yes_order_budget_cents", 100)),
            no_budget_cents=int(managed.get("no_order_budget_cents", 100)),
            telemetry_sqlite_path="",
            pnl_tracker_path="",
            watchdog_state_file="w.json",
        )
        kwargs = {k: tuple(v) if isinstance(v, list) else v for k, v in payload.items()}
        settings = BotSettings(**kwargs)
        settings.validate()
        return True
    except (ValueError, TypeError):
        return False


def _exclusion_reason(name: str, value: Any, tier: int = 1) -> Optional[str]:
    if name in EXCLUDED_EXACT:
        return "pinned by policy (identity/lifecycle/telemetry field)"
    if tier < 2:
        if name in TIER2_EXACT:
            return "awaiting Tier-2 data (top-of-book depth not replayable)"
        if any(name.startswith(prefix) for prefix in TIER2_PREFIXES):
            return "awaiting Tier-2 data (queue position not replayable)"
        if name.startswith("orderbook_pull_") and name not in ORDERBOOK_PULL_KEEP:
            return "awaiting Tier-2 data (orderbook pull events not replayable)"
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return None
    return f"unsupported type ({type(value).__name__})"


def _shrink_int_bound(name: str, default: int, bound: int) -> int:
    if is_valid_bot_overrides({name: bound}):
        return bound
    good, bad = int(default), int(bound)
    while abs(bad - good) > 1:
        mid = (good + bad) // 2
        if is_valid_bot_overrides({name: mid}):
            good = mid
        else:
            bad = mid
    return good


def _shrink_float_bound(name: str, default: float, bound: float, iterations: int = 40) -> float:
    if is_valid_bot_overrides({name: bound}):
        return bound
    good, bad = float(default), float(bound)
    for _ in range(iterations):
        mid = (good + bad) / 2.0
        if is_valid_bot_overrides({name: mid}):
            good = mid
        else:
            bad = mid
    return good


def build_space(tier: int = 1) -> SearchSpace:
    """Searchable space for the given data tier.

    Tier 1 pins the depth/queue/orderbook-pull group ("awaiting Tier-2
    data"); Tier 2 unpins it with the explicit ``TIER2_BOUNDS`` where given
    and the usual default x [0.25, 4] span otherwise, every bound shrunk by
    trial against ``BotSettings.validate()``.
    """
    tier = int(tier)
    cfg = _base_configuration()
    bot = cfg["bot"]
    dims: Dict[str, ParamDim] = {}
    excluded: Dict[str, str] = {}
    for name in sorted(bot):
        default = bot[name]
        reason = _exclusion_reason(name, default, tier)
        if reason is not None:
            excluded[name] = reason
            continue
        if isinstance(default, bool):
            dims[name] = ParamDim(name, "bool", default)
            continue
        explicit = TIER2_BOUNDS.get(name) if tier >= 2 else None
        if isinstance(default, int):
            if name in SCALE_BOUNDS:
                low, high = SCALE_BOUNDS[name]
            elif explicit is not None:
                low, high = int(explicit[0]), int(explicit[1])
            else:
                low = int(round(default * 0.25))
                high = int(round(default * 4.0))
                low = min(low, default - 2)
                high = max(high, default + 2)
            low = _shrink_int_bound(name, default, low)
            high = _shrink_int_bound(name, default, high)
            dims[name] = ParamDim(name, "int", default, float(low), float(high))
        else:
            if explicit is not None:
                low, high = float(explicit[0]), float(explicit[1])
            else:
                low = default * 0.25
                high = default * 4.0
                if high - low < 1e-12:
                    low, high = default - 2.0, default + 2.0
            if name in (FAIR_MID, FAIR_TICKER):
                high = min(high, 1.0)
                low = max(low, 0.0)
            low = _shrink_float_bound(name, default, low)
            high = _shrink_float_bound(name, default, high)
            dims[name] = ParamDim(name, "float", default, float(low), float(high))
    if FAIR_TRADE in dims:
        excluded[FAIR_TRADE] = "derived as 1 - mid_weight - ticker_weight (weights must sum to 1)"
        del dims[FAIR_TRADE]
    # Managed launcher-injected sizing fields (per-side budgets): searchable so
    # order-size scale is optimized; the replay driver routes them to
    # bot_settings_payload's managed kwargs rather than the bot section.
    for name, (default, low, high) in MANAGED_DIMS.items():
        low = _shrink_int_bound(name, default, low)
        high = _shrink_int_bound(name, default, high)
        dims[name] = ParamDim(name, "int", default, float(low), float(high))
    return SearchSpace(dims=dims, excluded=excluded, defaults=copy.deepcopy(bot), tier=tier)


def sample_value(dim: ParamDim, u: float) -> Any:
    """Map a uniform draw u in [0,1) onto the dimension."""
    if dim.kind == "bool":
        return bool(u >= 0.5)
    raw = dim.low + u * (dim.high - dim.low)
    if dim.kind == "int":
        return int(min(dim.high, max(dim.low, round(raw))))
    return float(min(dim.high, max(dim.low, raw)))


def finalize_params(space: SearchSpace, params: Dict[str, Any]) -> Dict[str, Any]:
    """Apply cross-field constraints and derived fields; returns a repaired copy."""
    out = dict(params)
    if FAIR_MID in out or FAIR_TICKER in out:
        mid = float(out.get(FAIR_MID, space.defaults[FAIR_MID]))
        tick = float(out.get(FAIR_TICKER, space.defaults[FAIR_TICKER]))
        mid = max(0.0, mid)
        tick = max(0.0, tick)
        total = mid + tick
        if total > 1.0:
            # trade weight would be negative: rescale onto the simplex boundary.
            mid /= total
            tick /= total
        trade = 1.0 - mid - tick
        if trade < 0.0:
            trade = 0.0
            tick = 1.0 - mid
        out[FAIR_MID] = mid
        out[FAIR_TICKER] = tick
        out[FAIR_TRADE] = trade
    low_key = "quote_size_min_fraction_of_budget"
    high_key = "quote_size_max_fraction_of_budget"
    low_value = out.get(low_key, space.defaults.get(low_key))
    high_value = out.get(high_key, space.defaults.get(high_key))
    if low_value is not None and high_value is not None and high_value < low_value:
        out[low_key], out[high_key] = high_value, low_value
    return out


def diff_from_default(space: SearchSpace, params: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for key in sorted(params):
        default = space.defaults.get(key)
        value = params[key]
        if isinstance(value, float) and isinstance(default, (int, float)):
            if abs(value - float(default)) < 1e-9:
                continue
        elif value == default:
            continue
        out[key] = value
    return out
