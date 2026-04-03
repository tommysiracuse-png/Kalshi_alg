#!/usr/bin/env python3
"""
Kalshi single-market top-of-book market-maker.

This version keeps the fixed-point, inventory-aware, expiration-aware structure
of the upgraded bot and adds three production-hardening behaviors that were
missing from the earlier runtime:

1. Shared write throttling across bot processes to reduce create/amend/cancel
   bursts and avoid 429 write-rate spikes.
2. Queue-abandonment logic so the bot stops wasting time in hopeless queues.
3. Same-side post-fill re-entry suppression so the bot does not immediately
   step back into the same side after getting hit.

The default settings are intentionally quieter and more conservative than the
previous production run:
- slower repricing
- longer expirations
- passive placement further behind the best bid
- stronger same-side cooldowns after fills
- queue-position polling and side shutoffs enabled by default
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import math
import os
import pathlib
import random
import sqlite3
import tempfile
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from threading import Lock
from typing import Deque, Dict, Iterable, List, Optional, Tuple

import requests
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
LOGGER = logging.getLogger("kalshi_top_of_book_bot")


# ---------------------------------------------------------------------------
# Fixed-point scales used internally
# ---------------------------------------------------------------------------

PRICE_SCALE = 10_000         # 1.0000 dollars == 10,000 price units
COUNT_SCALE = 100            # 1.00 contracts == 100 count units
PRICE_UNITS_PER_CENT = 100   # $0.01 == 100 price units

ZERO_PRICE_UNITS = 0
ONE_DOLLAR_PRICE_UNITS = PRICE_SCALE


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def now_ms() -> int:
    return int(time.time() * 1000)



def normalize_exchange_timestamp_to_ms(raw_value: object) -> int:
    if raw_value in (None, ""):
        return now_ms()
    try:
        value = int(raw_value)
    except Exception:
        parsed = parse_iso_utc_timestamp_to_ms(str(raw_value))
        return int(parsed or now_ms())
    if value < 10_000_000_000:
        return value * 1000
    return value



def utc_seconds_to_expiration_timestamp(seconds_in_future: int) -> int:
    return int(time.time()) + int(seconds_in_future)



def parse_iso_utc_timestamp_to_ms(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except Exception:
        return None



def decimal_from_any(value: object) -> Decimal:
    return Decimal(str(value))



def parse_price_units_from_dollars(price_dollars: object) -> int:
    scaled = decimal_from_any(price_dollars) * PRICE_SCALE
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))



def parse_price_units_from_legacy_cents(price_cents: object) -> int:
    return int(price_cents) * PRICE_UNITS_PER_CENT



def parse_optional_price_units(message: dict, fixed_point_field: str, legacy_cents_field: str) -> Optional[int]:
    if fixed_point_field in message and message.get(fixed_point_field) not in (None, ""):
        return parse_price_units_from_dollars(message[fixed_point_field])
    if legacy_cents_field in message and message.get(legacy_cents_field) not in (None, ""):
        return parse_price_units_from_legacy_cents(message[legacy_cents_field])
    return None



def format_price_dollars(price_units: int) -> str:
    value = (Decimal(price_units) / Decimal(PRICE_SCALE)).quantize(Decimal("0.0001"))
    return format(value, "f")



def parse_count_units_from_fp(count_fp: object) -> int:
    scaled = decimal_from_any(count_fp) * COUNT_SCALE
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))



def parse_optional_count_units(message: dict, fixed_point_field: str, legacy_integer_field: str) -> Optional[int]:
    if fixed_point_field in message and message.get(fixed_point_field) not in (None, ""):
        return parse_count_units_from_fp(message[fixed_point_field])
    if legacy_integer_field in message and message.get(legacy_integer_field) not in (None, ""):
        return int(message[legacy_integer_field]) * COUNT_SCALE
    return None



def format_count_fp(count_units: int) -> str:
    value = (Decimal(count_units) / Decimal(COUNT_SCALE)).quantize(Decimal("0.00"))
    return format(value, "f")



def cents_to_price_units(cents: int) -> int:
    return int(cents) * PRICE_UNITS_PER_CENT



def contracts_to_count_units(contracts: int) -> int:
    return int(contracts) * COUNT_SCALE



def log_event(event_name: str, **fields: object) -> None:
    if fields:
        detail = " ".join(f"{key}={value}" for key, value in fields.items())
        LOGGER.info("%s | %s", event_name, detail)
    else:
        LOGGER.info("%s", event_name)


# ---------------------------------------------------------------------------
# P&L tracker — appends one JSON line per fill to logs/pnl_tracker.jsonl
# ---------------------------------------------------------------------------

_CATEGORY_PREFIXES: List[Tuple[str, str]] = [
    # Weather
    ("KXLOWT", "weather"), ("KXHIGHT", "weather"), ("KXHIGHDEN", "weather"),
    # Commodities
    ("KXWTI", "commodities"), ("KXBRENTD", "commodities"), ("KXBRENTW", "commodities"),
    ("KXGOLDD", "commodities"), ("KXGOLDW", "commodities"),
    ("KXSILVERD", "commodities"), ("KXSILVERW", "commodities"),
    ("KXAAAGASD", "commodities"),
    # Crypto
    ("KXBTCD", "crypto"), ("KXBTCW", "crypto"),
    ("KXETHD", "crypto"), ("KXETHW", "crypto"), ("KXETH-", "crypto"),
    ("KXXRPD", "crypto"), ("KXSOLD", "crypto"), ("KXSOLE", "crypto"),
    # Baseball
    ("KXMLB", "baseball"),
    # Basketball
    ("KXNBA", "basketball"), ("KXCBAGAME", "basketball"),
    ("KXEUROCUP", "basketball"), ("KXNCAAWB", "basketball"),
    # Esports
    ("KXCS2", "esports"), ("KXVALORANT", "esports"),
    # Golf
    ("KXPGA", "golf"),
    # Hockey
    ("KXDEL", "hockey"), ("KXNHL", "hockey"),
    # Tennis
    ("KXATP", "tennis"), ("KXWTA", "tennis"),
    # Politics / econ
    ("KXPOLITICS", "politics"), ("KXMAMDANI", "politics"),
    ("KXBONDI", "politics"), ("KXAPRPOTUS", "politics"),
    ("KXUSNFP", "economics"),
    # Entertainment
    ("KXALBUMSALES", "entertainment"),
]


def categorize_ticker(ticker: str) -> str:
    upper = ticker.upper()
    for prefix, category in _CATEGORY_PREFIXES:
        if upper.startswith(prefix):
            return category
    return "other"


_PNL_TRACKER_LOCK = Lock()


def append_pnl_fill(
    ticker: str,
    side: str,
    price_cents: Optional[float],
    quantity: float,
    fee_cents: Optional[float],
    net_position_after: float,
    fair_value_cents: Optional[float],
) -> None:
    """Append a single fill record to the shared pnl_tracker.jsonl."""
    record = {
        "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "ticker": ticker,
        "category": categorize_ticker(ticker),
        "side": side,
        "price_c": price_cents,
        "qty": quantity,
        "fee_c": fee_cents,
        "net_pos": net_position_after,
        "fair_c": fair_value_cents,
    }
    line = json.dumps(record, separators=(",", ":")) + "\n"
    logs_dir = pathlib.Path(__file__).resolve().parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    pnl_path = logs_dir / "pnl_tracker.jsonl"
    with _PNL_TRACKER_LOCK:
        with open(pnl_path, "a") as f:
            f.write(line)


def is_post_only_cross_error(exception: Exception) -> bool:
    message = str(exception).lower()
    return (
        "post_only_cross" in message
        or "post only cross" in message
        or "post-only cross" in message
        or '"code":"post_only_cross"' in message
        or "'code':'post_only_cross'" in message
    )



def is_missing_or_non_resting_amend_target(exception: Exception) -> bool:
    message = str(exception).lower()
    return (
        ("amend" in message)
        and (
            "failed 404" in message
            or " 404:" in message
            or '"code":"not_found"' in message
            or "'code':'not_found'" in message
            or "order_not_found" in message
            or "not found" in message
            or "not_resting" in message
            or "order not resting" in message
            or "not resting" in message
        )
    )



def is_order_not_found_error(exception: Exception) -> bool:
    message = str(exception).lower()
    return (
        "failed 404" in message
        or " 404:" in message
        or '"code":"not_found"' in message
        or "'code':'not_found'" in message
        or "order_not_found" in message
        or "not found" in message
    )



def is_rate_limit_error(exception: Exception) -> bool:
    message = str(exception).lower()
    return (
        "failed 429" in message
        or " 429:" in message
        or "too_many_requests" in message
        or "rate limit" in message
    )


# ---------------------------------------------------------------------------
# Command-line arguments
# ---------------------------------------------------------------------------


def parse_bot_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Kalshi single-market top-of-book market-making bot."
    )
    parser.add_argument("--ticker", default=None, help="Market ticker to run.")
    parser.add_argument(
        "--yes-budget-cents",
        type=int,
        default=None,
        help="YES-side maximum working budget in cents.",
    )
    parser.add_argument(
        "--no-budget-cents",
        type=int,
        default=None,
        help="NO-side maximum working budget in cents.",
    )
    parser.add_argument(
        "--api-key-id",
        default=None,
        help="Kalshi API key ID override. If omitted, KALSHI_API_KEY_ID or API_KEY_ID is used.",
    )
    parser.add_argument(
        "--private-key",
        default=None,
        help="Path to the Kalshi private key PEM file. If omitted, KALSHI_PRIVATE_KEY_PATH or PRIVATE_KEY_PATH is used.",
    )
    parser.add_argument(
        "--use-demo",
        action="store_true",
        help="Use the Kalshi demo environment instead of production.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate and log actions, but do not create, amend, decrease, or cancel orders.",
    )
    parser.add_argument(
        "--subaccount",
        type=int,
        default=None,
        help="Optional Kalshi subaccount number. Defaults to 0 (primary account).",
    )
    parser.add_argument(
        "--watchdog-state-file",
        default="",
        help="Path to the cached watchdog state JSON file for this ticker.",
    )
    parser.add_argument(
        "--watchdog-refresh-seconds",
        type=float,
        default=3.0,
        help="How often V1 should refresh its cached watchdog state.",
    )
    parser.add_argument(
        "--watchdog-extreme-stale-seconds",
        type=float,
        default=30.0,
        help="Fail-safe flatten if the watchdog state becomes older than this.",
    )
    parser.add_argument(
        "--watchdog-flatten-retries",
        type=int,
        default=2,
        help="Number of emergency flatten retries before exiting with residual-risk code.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BotSettings:
    # --- Core identity / credentials ---
    market_ticker: str = "KXNHLGAME-26MAR11WSHPHI-WSH"
    api_key_id: str = "2622245b-4c4d-46f4-8a89-2e69069e7c70"
    private_key_path: str = "./tizzler2003.txt"
    use_demo_environment: bool = False
    dry_run: bool = False
    subaccount_number: int = 0
    watchdog_state_file: str = ""
    watchdog_refresh_seconds: float = 3.0
    watchdog_extreme_stale_seconds: float = 30.0
    watchdog_flatten_retries: int = 2

    # --- Order sizing ---
    yes_order_budget_cents: int = 100
    no_order_budget_cents: int = 100
    maximum_contracts_per_order: int = 25
    budget_fee_buffer_cents: int = 2
    allow_fractional_order_entry_when_supported: bool = False
    refill_resting_size_after_partial_fill: bool = False

    # --- Quote behavior ---
    post_only_quotes: bool = True
    cancel_quotes_if_exchange_pauses: bool = False
    minimum_milliseconds_between_requotes: int = 100

    # Use longer expirations than the earlier version so we stop resetting queue
    # priority every 45 seconds.
    resting_order_expiration_seconds: int =500
    expiration_refresh_lead_seconds: int = 30
    expiration_jitter_seconds: int = 20
    stagger_yes_no_expiration_offsets_seconds: int = 10

    aggressive_improvement_ticks_when_spread_is_wide: int = 2
    minimum_spread_ticks_required_for_aggressive_improvement: int = 8
    passive_offset_ticks_when_not_improving: int = 0
    join_current_best_bid_when_starting_new_quote_cycle: bool = True

    # After any fill, suppress same-side quoting for a while to reduce the
    # "got filled, stepped right back in, got filled again" pattern.
    post_fill_no_improve_cooldown_ms: int = 2_000
    same_side_reentry_cooldown_ms: int = 5_000
    suppress_same_side_quotes_during_reentry_cooldown: bool = True

    # Only chase upward when the market moved materially. Downward / de-risking
    # reprices still happen immediately.
    minimum_upward_reprice_ticks_required: int = 4

    minimum_best_bid_cents_required_to_quote: int = 8
    minimum_implied_ask_cents_required_to_quote: int = 8
    minimum_market_best_bid_cents_required_to_quote_any_side: int = 8
    enforce_one_tick_safety_below_implied_ask: bool = True

    # --- Inventory controls ---
    enable_one_way_inventory_guard: bool = True
    one_way_inventory_guard_contracts: int = 8
    inventory_skew_contracts_per_tick: int = 2
    maximum_inventory_skew_ticks: int = 6

    # --- Book depth / phantom-spread protection ---
    # Require at least this many contracts sitting at the best bid on a side
    # before quoting that side. Blocks thin "decoy" top-of-book orders.
    minimum_top_level_depth_contracts: int = 15
    # If the second-best bid level is more than this many cents below the best
    # bid (or there is no second level at all), do not quote that side.
    # Prevents entering a market where the apparent spread vanishes instantly.
    maximum_top_level_gap_cents: int = 20

    # --- Pair / spread protection ---
    enable_pair_guard: bool = True
    maximum_combined_bid_cents: int = 97
    additional_profit_buffer_cents: int = 2
    pair_guard_priority: str = "auto"

    # --- Post-only collision handling ---
    maximum_post_only_reprice_attempts: int = 3
    post_only_reprice_cooldown_seconds: float = 1.5

    # --- Startup / monitoring ---
    cancel_strategy_quotes_on_startup: bool = True
    subscribe_to_market_positions_channel: bool = True
    enable_queue_position_logging: bool = True
    queue_position_log_interval_seconds: float = 10.0

    # --- Telemetry / model inputs ---
    enable_sqlite_telemetry: bool = True
    telemetry_sqlite_path: str = ""
    trade_history_window_seconds: int = 60
    model_refresh_interval_seconds: int = 50
    markout_horizons_seconds: Tuple[int, ...] = (1, 5, 30, 120)

    # --- Fill-probability model ---
    fill_probability_horizon_seconds: int = 60
    fill_probability_prior_fills: float = 2.0
    fill_probability_prior_misses: float = 3.0

    # --- EV-based quoting model ---
    minimum_expected_edge_cents_to_keep_quote: int = 1
    minimum_expected_edge_cents_to_quote: int = 2
    inventory_reduction_max_negative_edge_cents: int = 6
    strong_edge_threshold_cents: int = 8
    default_toxicity_cents: int = 1
    default_fee_factor_for_maker_quotes: float = 0.45
    fair_value_mid_weight: float = 0.55
    fair_value_ticker_weight: float = 0.25
    fair_value_trade_weight: float = 0.20
    fair_value_max_orderbook_imbalance_adjust_cents: int = 2
    fair_value_max_trade_bias_adjust_cents: int = 4
    quote_size_min_fraction_of_budget: float = 0.20
    quote_size_max_fraction_of_budget: float = 1.00
    candidate_price_levels_to_scan: int = 22

    # Order-book pull toxicity guard.
    enable_orderbook_pull_toxicity_guard: bool = True
    orderbook_pull_window_ms: int = 1500
    orderbook_pull_top_levels_to_track: int = 8
    orderbook_pull_absolute_threshold_contracts: int = 150
    orderbook_pull_relative_depth_threshold: float = 0.20
    orderbook_pull_side_cooldown_ms: int = 5000
    orderbook_pull_market_cooldown_ms: int = 5000
    orderbook_pull_penalty_cents: int = 7

    # Queue-abandonment logic.
    enable_queue_abandonment_guard: bool = True
    maximum_queue_ahead_contracts_before_abandonment: int = 200
    maximum_queue_ahead_multiple_of_our_remaining_size: float = 25.0
    queue_abandonment_consecutive_polls_required: int = 3
    queue_abandonment_side_cooldown_seconds: int = 15
    queue_abandonment_market_cooldown_seconds: int = 15

    # Cross-process shared write throttling.
    enable_shared_write_rate_limiter: bool = True
    shared_write_rate_limit_writes_per_second: float = 25
    shared_write_rate_limit_burst_capacity: int = 8
    shared_write_rate_limiter_directory: str = ""
    global_rate_limit_backoff_seconds: float = 1.0

    primary_client_order_prefix: str = "mm"
    legacy_client_order_prefixes: Tuple[str, ...] = ("mm:", "tob:")

    def validate(self) -> None:
        if not self.market_ticker.strip():
            raise ValueError("market_ticker must not be empty")
        if self.yes_order_budget_cents < 0 or self.no_order_budget_cents < 0:
            raise ValueError("Order budgets must be >= 0 cents")
        if self.maximum_contracts_per_order <= 0:
            raise ValueError("maximum_contracts_per_order must be > 0")
        if self.budget_fee_buffer_cents < 0:
            raise ValueError("budget_fee_buffer_cents must be >= 0")
        if self.subaccount_number < 0:
            raise ValueError("subaccount_number must be >= 0")
        if self.watchdog_refresh_seconds < 0:
            raise ValueError("watchdog_refresh_seconds must be >= 0")
        if self.watchdog_extreme_stale_seconds < 0:
            raise ValueError("watchdog_extreme_stale_seconds must be >= 0")
        if self.watchdog_flatten_retries < 0:
            raise ValueError("watchdog_flatten_retries must be >= 0")
        if self.resting_order_expiration_seconds < 0:
            raise ValueError("resting_order_expiration_seconds must be >= 0")
        if self.expiration_refresh_lead_seconds < 0:
            raise ValueError("expiration_refresh_lead_seconds must be >= 0")
        if self.expiration_jitter_seconds < 0:
            raise ValueError("expiration_jitter_seconds must be >= 0")
        if self.stagger_yes_no_expiration_offsets_seconds < 0:
            raise ValueError("stagger_yes_no_expiration_offsets_seconds must be >= 0")
        if self.resting_order_expiration_seconds > 0 and self.expiration_refresh_lead_seconds >= self.resting_order_expiration_seconds:
            raise ValueError("expiration_refresh_lead_seconds must be less than resting_order_expiration_seconds")
        if self.minimum_milliseconds_between_requotes < 0:
            raise ValueError("minimum_milliseconds_between_requotes must be >= 0")
        if self.aggressive_improvement_ticks_when_spread_is_wide < 0:
            raise ValueError("aggressive_improvement_ticks_when_spread_is_wide must be >= 0")
        if self.minimum_spread_ticks_required_for_aggressive_improvement < 0:
            raise ValueError("minimum_spread_ticks_required_for_aggressive_improvement must be >= 0")
        if self.passive_offset_ticks_when_not_improving < 0:
            raise ValueError("passive_offset_ticks_when_not_improving must be >= 0")
        if self.post_fill_no_improve_cooldown_ms < 0:
            raise ValueError("post_fill_no_improve_cooldown_ms must be >= 0")
        if self.same_side_reentry_cooldown_ms < 0:
            raise ValueError("same_side_reentry_cooldown_ms must be >= 0")
        if self.minimum_upward_reprice_ticks_required < 0:
            raise ValueError("minimum_upward_reprice_ticks_required must be >= 0")
        if self.minimum_best_bid_cents_required_to_quote < 0 or self.minimum_implied_ask_cents_required_to_quote < 0:
            raise ValueError("Quote-threshold cents must be >= 0")
        if self.minimum_market_best_bid_cents_required_to_quote_any_side < 0:
            raise ValueError("minimum_market_best_bid_cents_required_to_quote_any_side must be >= 0") 
        if self.minimum_top_level_depth_contracts < 0:
            raise ValueError("minimum_top_level_depth_contracts must be >= 0")
        if self.maximum_top_level_gap_cents < 0:
            raise ValueError("maximum_top_level_gap_cents must be >= 0")
        if self.one_way_inventory_guard_contracts < 0:
            raise ValueError("one_way_inventory_guard_contracts must be >= 0")
        if self.inventory_skew_contracts_per_tick <= 0:
            raise ValueError("inventory_skew_contracts_per_tick must be > 0")
        if self.maximum_inventory_skew_ticks < 0:
            raise ValueError("maximum_inventory_skew_ticks must be >= 0")
        if self.maximum_combined_bid_cents < 0:
            raise ValueError("maximum_combined_bid_cents must be >= 0")
        if self.additional_profit_buffer_cents < 0:
            raise ValueError("additional_profit_buffer_cents must be >= 0")
        if self.maximum_post_only_reprice_attempts <= 0:
            raise ValueError("maximum_post_only_reprice_attempts must be > 0")
        if self.post_only_reprice_cooldown_seconds < 0:
            raise ValueError("post_only_reprice_cooldown_seconds must be >= 0")
        if self.queue_position_log_interval_seconds <= 0:
            raise ValueError("queue_position_log_interval_seconds must be > 0")
        if self.trade_history_window_seconds <= 0:
            raise ValueError("trade_history_window_seconds must be > 0")
        if self.model_refresh_interval_seconds <= 0:
            raise ValueError("model_refresh_interval_seconds must be > 0")
        if not self.markout_horizons_seconds or any(value <= 0 for value in self.markout_horizons_seconds):
            raise ValueError("markout_horizons_seconds must contain positive values")
        if self.fill_probability_horizon_seconds <= 0:
            raise ValueError("fill_probability_horizon_seconds must be > 0")
        if self.fill_probability_prior_fills < 0:
            raise ValueError("fill_probability_prior_fills must be >= 0")
        if self.fill_probability_prior_misses < 0:
            raise ValueError("fill_probability_prior_misses must be >= 0")
        if self.minimum_expected_edge_cents_to_quote < 0:
            raise ValueError("minimum_expected_edge_cents_to_quote must be >= 0")
        if self.inventory_reduction_max_negative_edge_cents < 0:
            raise ValueError("inventory_reduction_max_negative_edge_cents must be >= 0")
        if self.strong_edge_threshold_cents <= 0:
            raise ValueError("strong_edge_threshold_cents must be > 0")
        if self.default_toxicity_cents < 0:
            raise ValueError("default_toxicity_cents must be >= 0")
        if not 0 <= self.default_fee_factor_for_maker_quotes <= 2:
            raise ValueError("default_fee_factor_for_maker_quotes must be between 0 and 2")
        if self.fair_value_mid_weight < 0 or self.fair_value_ticker_weight < 0 or self.fair_value_trade_weight < 0:
            raise ValueError("fair value weights must be >= 0")
        if (self.fair_value_mid_weight + self.fair_value_ticker_weight + self.fair_value_trade_weight) <= 0:
            raise ValueError("At least one fair value weight must be > 0")
        if self.fair_value_max_orderbook_imbalance_adjust_cents < 0 or self.fair_value_max_trade_bias_adjust_cents < 0:
            raise ValueError("fair value adjustment caps must be >= 0")
        if self.quote_size_min_fraction_of_budget <= 0 or self.quote_size_max_fraction_of_budget <= 0:
            raise ValueError("quote size fractions must be > 0")
        if self.quote_size_max_fraction_of_budget < self.quote_size_min_fraction_of_budget:
            raise ValueError("quote_size_max_fraction_of_budget must be >= quote_size_min_fraction_of_budget")
        if self.candidate_price_levels_to_scan <= 0:
            raise ValueError("candidate_price_levels_to_scan must be > 0")
        if self.orderbook_pull_window_ms < 0:
            raise ValueError("orderbook_pull_window_ms must be >= 0")
        if self.orderbook_pull_top_levels_to_track <= 0:
            raise ValueError("orderbook_pull_top_levels_to_track must be > 0")
        if self.orderbook_pull_absolute_threshold_contracts < 0:
            raise ValueError("orderbook_pull_absolute_threshold_contracts must be >= 0")
        if not 0 <= self.orderbook_pull_relative_depth_threshold <= 1.0:
            raise ValueError("orderbook_pull_relative_depth_threshold must be between 0 and 1")
        if self.orderbook_pull_side_cooldown_ms < 0:
            raise ValueError("orderbook_pull_side_cooldown_ms must be >= 0")
        if self.orderbook_pull_market_cooldown_ms < 0:
            raise ValueError("orderbook_pull_market_cooldown_ms must be >= 0")
        if self.orderbook_pull_penalty_cents < 0:
            raise ValueError("orderbook_pull_penalty_cents must be >= 0")
        if self.maximum_queue_ahead_contracts_before_abandonment < 0:
            raise ValueError("maximum_queue_ahead_contracts_before_abandonment must be >= 0")
        if self.maximum_queue_ahead_multiple_of_our_remaining_size < 0:
            raise ValueError("maximum_queue_ahead_multiple_of_our_remaining_size must be >= 0")
        if self.queue_abandonment_consecutive_polls_required <= 0:
            raise ValueError("queue_abandonment_consecutive_polls_required must be > 0")
        if self.queue_abandonment_side_cooldown_seconds < 0:
            raise ValueError("queue_abandonment_side_cooldown_seconds must be >= 0")
        if self.queue_abandonment_market_cooldown_seconds < 0:
            raise ValueError("queue_abandonment_market_cooldown_seconds must be >= 0")
        if self.shared_write_rate_limit_writes_per_second <= 0:
            raise ValueError("shared_write_rate_limit_writes_per_second must be > 0")
        if self.shared_write_rate_limit_burst_capacity <= 0:
            raise ValueError("shared_write_rate_limit_burst_capacity must be > 0")
        if self.global_rate_limit_backoff_seconds < 0:
            raise ValueError("global_rate_limit_backoff_seconds must be >= 0")
        if self.pair_guard_priority.lower() not in {"yes", "no", "auto"}:
            raise ValueError("pair_guard_priority must be 'yes', 'no', or 'auto'")
        if self.minimum_expected_edge_cents_to_keep_quote < 0:
            raise ValueError("minimum_expected_edge_cents_to_keep_quote must be >= 0")



def build_settings_from_args(bot_args: argparse.Namespace) -> BotSettings:
    env_api_key = os.getenv("KALSHI_API_KEY_ID") or os.getenv("API_KEY_ID")
    env_private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH") or os.getenv("PRIVATE_KEY_PATH")

    defaults = BotSettings()

    market_ticker = bot_args.ticker.strip() if bot_args.ticker else defaults.market_ticker
    api_key_id = (bot_args.api_key_id or env_api_key or defaults.api_key_id).strip()
    private_key_path = (bot_args.private_key or env_private_key_path or defaults.private_key_path).strip()

    settings = BotSettings(
        market_ticker=market_ticker,
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        use_demo_environment=bool(bot_args.use_demo or defaults.use_demo_environment),
        dry_run=bool(bot_args.dry_run or defaults.dry_run),
        subaccount_number=int(bot_args.subaccount if bot_args.subaccount is not None else defaults.subaccount_number),
        watchdog_state_file=str(bot_args.watchdog_state_file or defaults.watchdog_state_file),
        watchdog_refresh_seconds=float(bot_args.watchdog_refresh_seconds if bot_args.watchdog_refresh_seconds is not None else defaults.watchdog_refresh_seconds),
        watchdog_extreme_stale_seconds=float(bot_args.watchdog_extreme_stale_seconds if bot_args.watchdog_extreme_stale_seconds is not None else defaults.watchdog_extreme_stale_seconds),
        watchdog_flatten_retries=int(bot_args.watchdog_flatten_retries if bot_args.watchdog_flatten_retries is not None else defaults.watchdog_flatten_retries),
        yes_order_budget_cents=int(bot_args.yes_budget_cents if bot_args.yes_budget_cents is not None else defaults.yes_order_budget_cents),
        no_order_budget_cents=int(bot_args.no_budget_cents if bot_args.no_budget_cents is not None else defaults.no_order_budget_cents),
    )
    settings.validate()
    return settings


# ---------------------------------------------------------------------------
# Cross-process shared write throttle
# ---------------------------------------------------------------------------


class CrossProcessFileLock:
    def __init__(self, path: str) -> None:
        self.path = path
        self._file = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._file = open(self.path, "a+", encoding="utf-8")
        self._file.seek(0)
        existing = self._file.read()
        if not existing:
            self._file.seek(0)
            self._file.write("{}")
            self._file.flush()
            os.fsync(self._file.fileno())
        if os.name == "nt":
            import msvcrt
            self._file.seek(0)
            while True:
                try:
                    msvcrt.locking(self._file.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
        else:
            import fcntl
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
        self._file.seek(0)
        return self._file

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is None:
            return
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
        except Exception:
            pass
        if os.name == "nt":
            import msvcrt
            self._file.seek(0)
            try:
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        self._file.close()
        self._file = None


class SharedWriteRateLimiter:
    def __init__(
        self,
        *,
        state_file_path: str,
        writes_per_second: float,
        burst_capacity: int,
        enabled: bool,
    ) -> None:
        self.state_file_path = state_file_path
        self.writes_per_second = float(writes_per_second)
        self.burst_capacity = int(burst_capacity)
        self.enabled = bool(enabled)

    def _default_state(self) -> dict:
        now_epoch_seconds = time.time()
        return {
            "tokens_available": float(self.burst_capacity),
            "last_refill_epoch_seconds": now_epoch_seconds,
            "blocked_until_epoch_seconds": 0.0,
        }

    def _load_state_locked(self, locked_file) -> dict:
        locked_file.seek(0)
        raw = locked_file.read().strip()
        if not raw:
            return self._default_state()
        try:
            state = json.loads(raw)
        except json.JSONDecodeError:
            return self._default_state()
        return {
            "tokens_available": float(state.get("tokens_available", self.burst_capacity)),
            "last_refill_epoch_seconds": float(state.get("last_refill_epoch_seconds", time.time())),
            "blocked_until_epoch_seconds": float(state.get("blocked_until_epoch_seconds", 0.0)),
        }

    def _save_state_locked(self, locked_file, state: dict) -> None:
        locked_file.seek(0)
        locked_file.truncate(0)
        locked_file.write(json.dumps(state, separators=(",", ":")))
        locked_file.flush()
        os.fsync(locked_file.fileno())

    def acquire_permit(self, *, action_name: str) -> None:
        if not self.enabled:
            return

        while True:
            sleep_seconds = 0.0
            with CrossProcessFileLock(self.state_file_path) as locked_file:
                now_epoch_seconds = time.time()
                state = self._load_state_locked(locked_file)

                blocked_until = float(state.get("blocked_until_epoch_seconds", 0.0))
                if blocked_until > now_epoch_seconds:
                    sleep_seconds = max(0.05, blocked_until - now_epoch_seconds)
                    self._save_state_locked(locked_file, state)
                else:
                    last_refill = float(state.get("last_refill_epoch_seconds", now_epoch_seconds))
                    elapsed = max(0.0, now_epoch_seconds - last_refill)
                    refilled_tokens = elapsed * self.writes_per_second
                    tokens_available = min(
                        float(self.burst_capacity),
                        float(state.get("tokens_available", self.burst_capacity)) + refilled_tokens,
                    )

                    if tokens_available >= 1.0:
                        state["tokens_available"] = tokens_available - 1.0
                        state["last_refill_epoch_seconds"] = now_epoch_seconds
                        state["blocked_until_epoch_seconds"] = 0.0
                        self._save_state_locked(locked_file, state)
                        return

                    tokens_needed = 1.0 - tokens_available
                    sleep_seconds = max(0.05, tokens_needed / self.writes_per_second)
                    state["tokens_available"] = tokens_available
                    state["last_refill_epoch_seconds"] = now_epoch_seconds
                    self._save_state_locked(locked_file, state)

            log_event("SHARED_WRITE_LIMIT_WAIT", action=action_name, seconds=f"{sleep_seconds:.3f}")
            time.sleep(sleep_seconds)

    def apply_global_cooldown(self, cooldown_seconds: float) -> None:
        if not self.enabled or cooldown_seconds <= 0:
            return
        with CrossProcessFileLock(self.state_file_path) as locked_file:
            now_epoch_seconds = time.time()
            state = self._load_state_locked(locked_file)
            state["blocked_until_epoch_seconds"] = max(
                float(state.get("blocked_until_epoch_seconds", 0.0)),
                now_epoch_seconds + float(cooldown_seconds),
            )
            state["last_refill_epoch_seconds"] = now_epoch_seconds
            self._save_state_locked(locked_file, state)


# ---------------------------------------------------------------------------
# Price grid
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceRange:
    start_units: int
    end_units: int
    step_units: int


class PriceGrid:
    def __init__(self, price_ranges: Iterable[PriceRange]) -> None:
        self.ranges: List[PriceRange] = sorted(
            price_ranges,
            key=lambda item: (item.start_units, item.end_units, item.step_units),
        )
        if not self.ranges:
            raise ValueError("PriceGrid requires at least one price range")

        self.minimum_step_units = min(price_range.step_units for price_range in self.ranges)
        self.minimum_valid_price_units = self._find_first_valid_price()
        self.maximum_valid_price_units = self._find_last_valid_price()

    @classmethod
    def from_market_response(cls, market_payload: dict) -> "PriceGrid":
        raw_price_ranges = market_payload.get("price_ranges") or []
        parsed_ranges: List[PriceRange] = []

        for raw_range in raw_price_ranges:
            try:
                start_units = parse_price_units_from_dollars(raw_range["start"])
                end_units = parse_price_units_from_dollars(raw_range["end"])
                step_units = parse_price_units_from_dollars(raw_range["step"])
            except Exception as exc:
                raise ValueError(f"Invalid price_ranges entry: {raw_range}") from exc

            if step_units <= 0:
                raise ValueError(f"Invalid price step in range: {raw_range}")
            if end_units < start_units:
                raise ValueError(f"Price range end is below start: {raw_range}")

            parsed_ranges.append(
                PriceRange(
                    start_units=start_units,
                    end_units=end_units,
                    step_units=step_units,
                )
            )

        if parsed_ranges:
            return cls(parsed_ranges)

        legacy_tick_cents = int(market_payload.get("tick_size") or 1)
        fallback_step_units = max(1, legacy_tick_cents * PRICE_UNITS_PER_CENT)
        return cls(
            [
                PriceRange(
                    start_units=0,
                    end_units=ONE_DOLLAR_PRICE_UNITS,
                    step_units=fallback_step_units,
                )
            ]
        )

    def _sanitize_candidate(self, candidate_units: int, price_range: PriceRange) -> Optional[int]:
        if candidate_units <= ZERO_PRICE_UNITS:
            candidate_units = price_range.start_units
            if candidate_units <= ZERO_PRICE_UNITS:
                candidate_units += price_range.step_units

        if candidate_units >= ONE_DOLLAR_PRICE_UNITS:
            candidate_units = ONE_DOLLAR_PRICE_UNITS - price_range.step_units

        if candidate_units <= ZERO_PRICE_UNITS or candidate_units >= ONE_DOLLAR_PRICE_UNITS:
            return None
        if candidate_units < price_range.start_units or candidate_units > price_range.end_units:
            return None
        return candidate_units

    def _find_first_valid_price(self) -> int:
        candidate = self.ceil_to_valid(1)
        if candidate is None:
            raise ValueError("Could not determine the first valid price level")
        return candidate

    def _find_last_valid_price(self) -> int:
        candidate = self.floor_to_valid(ONE_DOLLAR_PRICE_UNITS - 1)
        if candidate is None:
            raise ValueError("Could not determine the last valid price level")
        return candidate

    def floor_to_valid(self, requested_price_units: int) -> Optional[int]:
        best_candidate: Optional[int] = None

        for price_range in self.ranges:
            if requested_price_units < price_range.start_units:
                continue

            candidate = min(requested_price_units, price_range.end_units)
            offset = candidate - price_range.start_units
            candidate = price_range.start_units + (offset // price_range.step_units) * price_range.step_units
            candidate = self._sanitize_candidate(candidate, price_range)
            if candidate is None:
                continue

            if best_candidate is None or candidate > best_candidate:
                best_candidate = candidate

        return best_candidate

    def ceil_to_valid(self, requested_price_units: int) -> Optional[int]:
        best_candidate: Optional[int] = None

        for price_range in self.ranges:
            candidate = max(requested_price_units, price_range.start_units)
            offset = candidate - price_range.start_units
            remainder = offset % price_range.step_units
            if remainder:
                candidate += price_range.step_units - remainder

            candidate = self._sanitize_candidate(candidate, price_range)
            if candidate is None:
                continue

            if best_candidate is None or candidate < best_candidate:
                best_candidate = candidate

        return best_candidate

    def previous_price(self, current_price_units: int) -> Optional[int]:
        if current_price_units <= self.minimum_valid_price_units:
            return None
        return self.floor_to_valid(current_price_units - 1)

    def next_price(self, current_price_units: int) -> Optional[int]:
        if current_price_units >= self.maximum_valid_price_units:
            return None
        return self.ceil_to_valid(current_price_units + 1)

    def move_price_by_ticks(self, starting_price_units: int, ticks: int) -> int:
        price_units = self.floor_to_valid(starting_price_units)
        if price_units is None:
            raise ValueError(f"Starting price is outside the market's valid price grid: {starting_price_units}")

        if ticks > 0:
            for _ in range(ticks):
                next_price_units = self.next_price(price_units)
                if next_price_units is None:
                    return price_units
                price_units = next_price_units
        elif ticks < 0:
            for _ in range(abs(ticks)):
                previous_price_units = self.previous_price(price_units)
                if previous_price_units is None:
                    return price_units
                price_units = previous_price_units

        return price_units

    def max_safe_partner_bid(self, own_bid_price_units: int) -> Optional[int]:
        complement_units = ONE_DOLLAR_PRICE_UNITS - int(own_bid_price_units)
        return self.previous_price(complement_units)

    def tick_distance(self, price_a_units: int, price_b_units: int, *, max_iterations: int = 10_000) -> int:
        if price_a_units == price_b_units:
            return 0

        if price_b_units > price_a_units:
            cursor = price_a_units
            ticks = 0
            while cursor < price_b_units and ticks < max_iterations:
                next_price_units = self.next_price(cursor)
                if next_price_units is None:
                    break
                ticks += 1
                cursor = next_price_units
            return ticks

        cursor = price_a_units
        ticks = 0
        while cursor > price_b_units and ticks < max_iterations:
            previous_price_units = self.previous_price(cursor)
            if previous_price_units is None:
                break
            ticks += 1
            cursor = previous_price_units
        return ticks


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


class KalshiApiClient:
    def __init__(self, settings: BotSettings) -> None:
        self.settings = settings
        self.host = "https://demo-api.kalshi.co" if settings.use_demo_environment else "https://api.elections.kalshi.com"
        self.websocket_url = ("wss://demo-api.kalshi.co" if settings.use_demo_environment else "wss://api.elections.kalshi.com") + "/trade-api/ws/v2"
        self.api_prefix = "/trade-api/v2"
        self.websocket_path = "/trade-api/ws/v2"
        self.session = requests.Session()

        if not settings.api_key_id:
            raise ValueError(
                "A valid Kalshi API key ID is required. Pass --api-key-id or set KALSHI_API_KEY_ID."
            )
        if not settings.private_key_path or settings.private_key_path == "./private_key.pem":
            raise ValueError(
                "A valid Kalshi private key path is required. Pass --private-key or set KALSHI_PRIVATE_KEY_PATH."
            )
        if not os.path.exists(settings.private_key_path):
            raise FileNotFoundError(f"Private key file not found: {settings.private_key_path}")

        with open(settings.private_key_path, "rb") as private_key_file:
            self.private_key = serialization.load_pem_private_key(private_key_file.read(), password=None)

        limiter_directory = settings.shared_write_rate_limiter_directory or tempfile.gettempdir()
        limiter_namespace = hashlib.sha1(
            f"{self.host}|{self.settings.api_key_id}|{self.settings.subaccount_number}".encode("utf-8")
        ).hexdigest()[:16]
        limiter_file_path = os.path.join(limiter_directory, f"kalshi_write_limiter_{limiter_namespace}.json")
        self.shared_write_rate_limiter = SharedWriteRateLimiter(
            state_file_path=limiter_file_path,
            writes_per_second=settings.shared_write_rate_limit_writes_per_second,
            burst_capacity=settings.shared_write_rate_limit_burst_capacity,
            enabled=(settings.enable_shared_write_rate_limiter and not settings.dry_run),
        )

    def sign_message(self, message_bytes: bytes) -> str:
        signature = self.private_key.sign(
            message_bytes,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _rest_headers(self, method: str, path: str) -> dict:
        timestamp_ms = str(now_ms())
        path_without_query = path.split("?")[0]
        signed_message = f"{timestamp_ms}{method.upper()}{path_without_query}".encode("utf-8")
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.settings.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": self.sign_message(signed_message),
        }

    def websocket_headers(self) -> dict:
        timestamp_ms = str(now_ms())
        signed_message = f"{timestamp_ms}GET{self.websocket_path}".encode("utf-8")
        return {
            "KALSHI-ACCESS-KEY": self.settings.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": self.sign_message(signed_message),
        }

    def _build_url(self, path: str) -> str:
        return self.host + path

    def _apply_global_rate_limit_backoff(self) -> None:
        self.shared_write_rate_limiter.apply_global_cooldown(self.settings.global_rate_limit_backoff_seconds)

    def _before_write_api_call(self, action_name: str) -> None:
        self.shared_write_rate_limiter.acquire_permit(action_name=action_name)

    def rest_get(self, path: str, params: Optional[dict] = None) -> dict:
        response = self.session.get(
            self._build_url(path),
            headers=self._rest_headers("GET", path),
            params=params,
            timeout=15,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"GET {path} failed {response.status_code}: {response.text[:500]}")
        if not response.text.strip():
            return {}
        return response.json()

    def rest_post(self, path: str, body: dict, *, action_name: str) -> dict:
        response = self.session.post(
            self._build_url(path),
            headers=self._rest_headers("POST", path),
            json=body,
            timeout=15,
        )
        if response.status_code == 429:
            self._apply_global_rate_limit_backoff()
        if response.status_code >= 400:
            raise RuntimeError(f"POST {path} failed {response.status_code}: {response.text[:500]}")
        if not response.text.strip():
            return {}
        return response.json()

    def rest_delete(self, path: str, params: Optional[dict] = None, *, action_name: str) -> dict:
        response = self.session.delete(
            self._build_url(path),
            headers=self._rest_headers("DELETE", path),
            params=params,
            timeout=15,
        )
        if response.status_code == 429:
            self._apply_global_rate_limit_backoff()
        if response.status_code >= 400:
            raise RuntimeError(f"DELETE {path} failed {response.status_code}: {response.text[:500]}")
        if not response.text.strip():
            return {}
        return response.json()

    def get_market(self, market_ticker: str) -> dict:
        response = self.rest_get(f"{self.api_prefix}/markets/{market_ticker}")
        return response["market"]

    def get_market_quote(self, market_ticker: str) -> dict:
        market = self.get_market(market_ticker)
        return {
            "yes_bid_units": parse_optional_price_units(market, "yes_bid_dollars", "yes_bid"),
            "no_bid_units": parse_optional_price_units(market, "no_bid_dollars", "no_bid"),
        }

    def get_positions(self, market_ticker: str) -> dict:
        return self.rest_get(
            f"{self.api_prefix}/portfolio/positions",
            params={
                "ticker": market_ticker,
                "subaccount": self.settings.subaccount_number,
                "limit": 1,
            },
        )

    def get_resting_orders_for_market(self, market_ticker: str) -> List[dict]:
        all_orders: List[dict] = []
        cursor: Optional[str] = None

        while True:
            params = {
                "ticker": market_ticker,
                "status": "resting",
                "subaccount": self.settings.subaccount_number,
                "limit": 200,
            }
            if cursor:
                params["cursor"] = cursor

            response = self.rest_get(f"{self.api_prefix}/portfolio/orders", params=params)
            all_orders.extend(response.get("orders", []))
            cursor = response.get("cursor") or response.get("next_cursor") or ""
            if not cursor:
                break

        return all_orders

    def create_order(
        self,
        *,
        market_ticker: str,
        side: str,
        price_units: int,
        count_units: int,
        client_order_id: str,
        expiration_timestamp_seconds: Optional[int],
        action: str = "buy",
        post_only: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        time_in_force: Optional[str] = None,
        cancel_order_on_pause: Optional[bool] = None,
    ) -> dict:
        body = {
            "ticker": market_ticker,
            "side": side,
            "action": action,
            "client_order_id": client_order_id,
            "count_fp": format_count_fp(count_units),
            "post_only": bool(self.settings.post_only_quotes if post_only is None else post_only),
            "cancel_order_on_pause": bool(self.settings.cancel_quotes_if_exchange_pauses if cancel_order_on_pause is None else cancel_order_on_pause),
            "subaccount": self.settings.subaccount_number,
            "time_in_force": str(time_in_force or "good_till_canceled"),
        }
        if reduce_only is not None:
            body["reduce_only"] = bool(reduce_only)

        if side == "yes":
            body["yes_price_dollars"] = format_price_dollars(price_units)
        else:
            body["no_price_dollars"] = format_price_dollars(price_units)

        if expiration_timestamp_seconds and expiration_timestamp_seconds > 0 and body.get("time_in_force") != "immediate_or_cancel":
            body["expiration_ts"] = int(expiration_timestamp_seconds)

        if self.settings.dry_run:
            log_event(
                "DRY_CREATE",
                side=side,
                action=action,
                tif=body.get("time_in_force"),
                post_only=body.get("post_only"),
                reduce_only=body.get("reduce_only", False),
                price_dollars=format_price_dollars(price_units),
                contracts=format_count_fp(count_units),
            )
            return {
                "order": {
                    "order_id": f"DRY-{uuid.uuid4()}",
                    "client_order_id": client_order_id,
                    "expiration_time": None,
                }
            }

        self._before_write_api_call("create_order")
        return self.rest_post(f"{self.api_prefix}/portfolio/orders", body, action_name="create_order")

    def amend_order(
        self,
        *,
        order_id: str,
        market_ticker: str,
        side: str,
        new_price_units: int,
        new_total_fillable_count_units: int,
        previous_client_order_id: str,
        updated_client_order_id: str,
    ) -> dict:
        body = {
            "ticker": market_ticker,
            "side": side,
            "action": "buy",
            "subaccount": self.settings.subaccount_number,
            "client_order_id": previous_client_order_id,
            "updated_client_order_id": updated_client_order_id,
            "count_fp": format_count_fp(new_total_fillable_count_units),
        }

        if side == "yes":
            body["yes_price_dollars"] = format_price_dollars(new_price_units)
        else:
            body["no_price_dollars"] = format_price_dollars(new_price_units)

        if self.settings.dry_run:
            log_event(
                "DRY_AMEND",
                side=side,
                order_id=order_id,
                price_dollars=format_price_dollars(new_price_units),
                total_contracts=format_count_fp(new_total_fillable_count_units),
            )
            return {
                "order": {
                    "order_id": order_id,
                    "client_order_id": updated_client_order_id,
                    "expiration_time": None,
                }
            }

        self._before_write_api_call("amend_order")
        return self.rest_post(f"{self.api_prefix}/portfolio/orders/{order_id}/amend", body, action_name="amend_order")

    def decrease_order_to(self, *, order_id: str, remaining_count_units: int) -> dict:
        body = {
            "subaccount": self.settings.subaccount_number,
            "reduce_to_fp": format_count_fp(remaining_count_units),
        }

        if self.settings.dry_run:
            log_event(
                "DRY_DECREASE",
                order_id=order_id,
                reduce_to_contracts=format_count_fp(remaining_count_units),
            )
            return {"order": {"order_id": order_id}}

        self._before_write_api_call("decrease_order")
        return self.rest_post(f"{self.api_prefix}/portfolio/orders/{order_id}/decrease", body, action_name="decrease_order")

    def cancel_order(self, *, order_id: str) -> dict:
        if self.settings.dry_run:
            log_event("DRY_CANCEL", order_id=order_id)
            return {"order": {"order_id": order_id}}

        self._before_write_api_call("cancel_order")
        return self.rest_delete(
            f"{self.api_prefix}/portfolio/orders/{order_id}",
            params={"subaccount": self.settings.subaccount_number},
            action_name="cancel_order",
        )

    def get_order_queue_position(self, order_id: str) -> dict:
        return self.rest_get(f"{self.api_prefix}/portfolio/orders/{order_id}/queue_position")

    def get_series(self, series_ticker: str) -> dict:
        response = self.rest_get(
            f"{self.api_prefix}/series/{series_ticker}",
            params={"include_volume": True},
        )
        return response["series"]

    def get_series_fee_changes(self, series_ticker: str, *, show_historical: bool = False) -> List[dict]:
        response = self.rest_get(
            f"{self.api_prefix}/series/fee_changes",
            params={
                "series_ticker": series_ticker,
                "show_historical": bool(show_historical),
            },
        )
        return list(response.get("series_fee_change_arr") or [])

    def get_incentive_programs(self, *, status: str = "active", incentive_type: str = "all", limit: int = 10_000) -> List[dict]:
        response = self.rest_get(
            f"{self.api_prefix}/incentive_programs",
            params={
                "status": status,
                "type": incentive_type,
                "limit": max(1, min(int(limit), 10_000)),
            },
        )
        return list(response.get("incentive_programs") or [])


# ---------------------------------------------------------------------------
# Market metadata and order state
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MarketMetadata:
    ticker: str
    title: str
    status: str
    series_ticker: str
    event_ticker: str
    close_time_ms: Optional[int]
    price_level_structure: str
    fractional_trading_enabled: bool
    price_grid: PriceGrid


@dataclass
class ManagedOrderState:
    side: str
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    price_units: Optional[int] = None
    remaining_count_units: int = 0
    current_order_filled_units: int = 0
    quote_cycle_filled_units: int = 0
    status: Optional[str] = None
    expiration_time_ms: Optional[int] = None
    last_queue_position_units: Optional[int] = None
    consecutive_queue_ahead_breaches: int = 0
    same_side_reentry_cooldown_until_ms: int = 0
    queue_abandonment_cooldown_until_ms: int = 0
    last_positive_fill_timestamp_ms: int = 0

    @property
    def has_active_resting_order(self) -> bool:
        return bool(self.order_id) and self.status == "resting"

    @property
    def current_order_total_fillable_units(self) -> int:
        return int(self.remaining_count_units) + int(self.current_order_filled_units)

    def clear_active_order(self, *, preserve_quote_cycle: bool) -> None:
        self.order_id = None
        self.client_order_id = None
        self.price_units = None
        self.remaining_count_units = 0
        self.current_order_filled_units = 0
        self.status = None
        self.expiration_time_ms = None
        self.last_queue_position_units = None
        self.consecutive_queue_ahead_breaches = 0
        if not preserve_quote_cycle:
            self.quote_cycle_filled_units = 0


@dataclass
class WatchdogState:
    ticker: str
    generated_at_ms: int
    effective_close_time_ms: Optional[int]
    soft_stop_time_ms: Optional[int]
    hard_stop_time_ms: Optional[int]
    flatten_only_time_ms: Optional[int]
    confidence: float
    mode: str
    reason: str
    profile_version: str


# ---------------------------------------------------------------------------
# Strategy telemetry / models
# ---------------------------------------------------------------------------


@dataclass
class TickerState:
    market_ticker: str
    ts_ms: int
    price_units: Optional[int]
    yes_bid_units: Optional[int]
    yes_ask_units: Optional[int]
    volume_units: Optional[int]
    open_interest_units: Optional[int]
    yes_bid_size_units: Optional[int]
    yes_ask_size_units: Optional[int]
    last_trade_size_units: Optional[int]


@dataclass
class PublicTradeTick:
    trade_id: str
    market_ticker: str
    ts_ms: int
    yes_price_units: int
    no_price_units: int
    count_units: int
    taker_side: str


@dataclass
class MarketContext:
    ts_ms: int
    best_yes_bid_units: int
    best_no_bid_units: int
    implied_yes_ask_units: int
    implied_no_ask_units: int
    best_yes_bid_size_units: int
    best_no_bid_size_units: int
    ticker_price_units: Optional[int]
    ticker_yes_bid_units: Optional[int]
    ticker_yes_ask_units: Optional[int]
    ticker_yes_bid_size_units: Optional[int]
    ticker_yes_ask_size_units: Optional[int]
    last_trade_yes_units: Optional[int]
    last_trade_size_units: int
    trade_bias: float
    trade_momentum_units: int
    order_book_imbalance: float
    inventory_units: int
    queue_ahead_yes_units: Optional[int]
    queue_ahead_no_units: Optional[int]
    seconds_to_close: Optional[float]

    @property
    def mid_yes_units(self) -> int:
        return int(round((self.best_yes_bid_units + self.implied_yes_ask_units) / 2.0))

    @property
    def mid_no_units(self) -> int:
        return int(round((self.best_no_bid_units + self.implied_no_ask_units) / 2.0))

    def spread_units_for_side(self, side: str) -> int:
        if side == "yes":
            return max(0, self.implied_yes_ask_units - self.best_yes_bid_units)
        return max(0, self.implied_no_ask_units - self.best_no_bid_units)

    def queue_ahead_units_for_side(self, side: str) -> Optional[int]:
        return self.queue_ahead_yes_units if side == "yes" else self.queue_ahead_no_units


@dataclass
class FairValueEstimate:
    fair_yes_units: int
    fair_no_units: int
    raw_fair_yes_units: int
    mid_yes_units: int
    ticker_anchor_yes_units: Optional[int]
    trade_anchor_yes_units: Optional[int]
    order_book_adjust_units: int
    trade_bias_adjust_units: int

    def fair_for_side(self, side: str) -> int:
        return self.fair_yes_units if side == "yes" else self.fair_no_units


@dataclass
class SideQuoteDecision:
    side: str
    price_units: Optional[int]
    target_size_units: int
    mode: str
    fair_yes_units: Optional[int] = None
    fair_side_units: Optional[int] = None
    reservation_units: Optional[int] = None
    market_target_units: Optional[int] = None
    projected_queue_ahead_units: Optional[int] = None
    expected_edge_units: int = 0
    estimated_fee_units: int = 0
    estimated_toxicity_units: int = 0
    estimated_incentive_units: int = 0
    inventory_penalty_units: int = 0
    queue_penalty_units: int = 0
    reason: str = ""
    candidate_debug: str = ""
    estimated_fill_probability_30s: float = 0.0
    estimated_markout_units: int = 0
    score_per_hour_units: float = 0.0
    bucket_key: str = ""


@dataclass
class ToxicityStat:
    observations: int = 0
    ewma_adverse_units: float = 0.0


@dataclass
class FillProbStat:
    attempts: int = 0
    fills_30s: int = 0


@dataclass
class PendingFillAttempt:
    attempt_id: str
    side: str
    bucket_key: str
    quote_price_units: int
    started_ts_ms: int
    expiry_ts_ms: int
    filled_within_horizon: bool = False
    settled: bool = False


class TelemetryStore:
    def __init__(self, database_path: str, *, enabled: bool) -> None:
        self.enabled = bool(enabled)
        self.database_path = database_path
        self._connection: Optional[sqlite3.Connection] = None
        self._lock = Lock()
        if not self.enabled:
            return

        directory = os.path.dirname(os.path.abspath(database_path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._connection = sqlite3.connect(database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        if not self._connection:
            return
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fill_key TEXT NOT NULL UNIQUE,
                    ts_ms INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    side TEXT NOT NULL,
                    trade_id TEXT,
                    order_id TEXT,
                    price_units INTEGER,
                    size_units INTEGER,
                    fee_units INTEGER,
                    is_taker INTEGER,
                    inventory_before_units INTEGER,
                    inventory_after_units INTEGER,
                    fair_yes_before_units INTEGER,
                    best_yes_bid_units INTEGER,
                    best_no_bid_units INTEGER,
                    queue_ahead_units INTEGER
                );
                CREATE TABLE IF NOT EXISTS quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    side TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    fair_yes_units INTEGER,
                    fair_side_units INTEGER,
                    quote_units INTEGER,
                    reservation_units INTEGER,
                    market_target_units INTEGER,
                    expected_edge_units INTEGER,
                    fee_units INTEGER,
                    toxicity_units INTEGER,
                    incentive_units INTEGER,
                    inventory_penalty_units INTEGER,
                    queue_penalty_units INTEGER,
                    target_size_units INTEGER,
                    queue_ahead_units INTEGER,
                    spread_units INTEGER,
                    inventory_units INTEGER
                );
                CREATE TABLE IF NOT EXISTS markouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fill_key TEXT NOT NULL,
                    ts_ms INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    side TEXT NOT NULL,
                    horizon_ms INTEGER NOT NULL,
                    fill_price_units INTEGER NOT NULL,
                    future_mid_yes_units INTEGER,
                    future_fair_yes_units INTEGER,
                    adverse_units INTEGER,
                    bucket_key TEXT
                );
                CREATE TABLE IF NOT EXISTS market_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    source TEXT NOT NULL,
                    best_yes_bid_units INTEGER,
                    best_no_bid_units INTEGER,
                    implied_yes_ask_units INTEGER,
                    implied_no_ask_units INTEGER,
                    best_yes_bid_size_units INTEGER,
                    best_no_bid_size_units INTEGER,
                    ticker_price_units INTEGER,
                    ticker_yes_bid_units INTEGER,
                    ticker_yes_ask_units INTEGER,
                    yes_bid_size_units INTEGER,
                    yes_ask_size_units INTEGER,
                    last_trade_yes_units INTEGER,
                    last_trade_size_units INTEGER,
                    trade_bias_bp INTEGER,
                    inventory_units INTEGER
                );
                CREATE TABLE IF NOT EXISTS ticker_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    price_units INTEGER,
                    yes_bid_units INTEGER,
                    yes_ask_units INTEGER,
                    volume_units INTEGER,
                    open_interest_units INTEGER,
                    yes_bid_size_units INTEGER,
                    yes_ask_size_units INTEGER,
                    last_trade_size_units INTEGER
                );
                CREATE TABLE IF NOT EXISTS public_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    trade_id TEXT,
                    yes_price_units INTEGER,
                    no_price_units INTEGER,
                    count_units INTEGER,
                    taker_side TEXT
                );
                CREATE TABLE IF NOT EXISTS fill_prob_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempt_id TEXT NOT NULL UNIQUE,
                    ts_ms INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    side TEXT NOT NULL,
                    bucket_key TEXT NOT NULL,
                    quote_price_units INTEGER NOT NULL,
                    filled_30s INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_fill_prob_attempts_bucket
                    ON fill_prob_attempts(bucket_key, ts_ms);
                CREATE INDEX IF NOT EXISTS idx_markouts_fill_key ON markouts(fill_key);
                CREATE INDEX IF NOT EXISTS idx_markouts_bucket ON markouts(bucket_key, horizon_ms);
                CREATE INDEX IF NOT EXISTS idx_fills_ticker_ts ON fills(ticker, ts_ms);
                CREATE INDEX IF NOT EXISTS idx_quotes_ticker_ts ON quotes(ticker, ts_ms);
                """
            )
            self._connection.commit()

    def _execute(self, sql: str, params: Tuple[object, ...]) -> None:
        if not self._connection:
            return
        with self._lock:
            self._connection.execute(sql, params)
            self._connection.commit()

    def load_recent_markouts(self, *, limit: int = 5000) -> List[sqlite3.Row]:
        if not self._connection:
            return []
        with self._lock:
            cursor = self._connection.execute(
                "SELECT side, horizon_ms, adverse_units, bucket_key FROM markouts ORDER BY id DESC LIMIT ?",
                (int(limit),),
            )
            return list(cursor.fetchall())

    def load_recent_fill_prob_attempts(self, *, limit: int = 10000) -> List[sqlite3.Row]:
        if not self._connection:
            return []
        with self._lock:
            cursor = self._connection.execute(
                """
                SELECT bucket_key, filled_30s
                FROM fill_prob_attempts
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            return list(cursor.fetchall())

    def load_recent_fills(self, *, limit: int = 1000) -> List[sqlite3.Row]:
        if not self._connection:
            return []
        with self._lock:
            cursor = self._connection.execute(
                "SELECT price_units, size_units, fee_units FROM fills WHERE fee_units IS NOT NULL ORDER BY id DESC LIMIT ?",
                (int(limit),),
            )
            return list(cursor.fetchall())

    def record_fill_prob_attempt(
        self,
        *,
        attempt_id: str,
        ts_ms: int,
        ticker: str,
        side: str,
        bucket_key: str,
        quote_price_units: int,
        filled_30s: bool,
    ) -> None:
        self._execute(
            """
            INSERT OR IGNORE INTO fill_prob_attempts (
                attempt_id, ts_ms, ticker, side, bucket_key, quote_price_units, filled_30s
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attempt_id,
                int(ts_ms),
                ticker,
                side,
                bucket_key,
                int(quote_price_units),
                1 if filled_30s else 0,
            ),
        )

    def record_fill(
        self,
        *,
        fill_key: str,
        ts_ms: int,
        ticker: str,
        side: str,
        trade_id: str,
        order_id: str,
        price_units: Optional[int],
        size_units: int,
        fee_units: Optional[int],
        is_taker: bool,
        inventory_before_units: int,
        inventory_after_units: int,
        fair_yes_before_units: Optional[int],
        best_yes_bid_units: Optional[int],
        best_no_bid_units: Optional[int],
        queue_ahead_units: Optional[int],
    ) -> None:
        self._execute(
            """
            INSERT OR IGNORE INTO fills (
                fill_key, ts_ms, ticker, side, trade_id, order_id, price_units, size_units, fee_units,
                is_taker, inventory_before_units, inventory_after_units, fair_yes_before_units,
                best_yes_bid_units, best_no_bid_units, queue_ahead_units
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill_key,
                int(ts_ms),
                ticker,
                side,
                trade_id,
                order_id,
                price_units,
                int(size_units),
                fee_units,
                1 if is_taker else 0,
                int(inventory_before_units),
                int(inventory_after_units),
                fair_yes_before_units,
                best_yes_bid_units,
                best_no_bid_units,
                queue_ahead_units,
            ),
        )

    def record_quote_decision(self, *, ts_ms: int, ticker: str, decision: SideQuoteDecision, context: MarketContext) -> None:
        self._execute(
            """
            INSERT INTO quotes (
                ts_ms, ticker, side, mode, fair_yes_units, fair_side_units, quote_units, reservation_units,
                market_target_units, expected_edge_units, fee_units, toxicity_units, incentive_units,
                inventory_penalty_units, queue_penalty_units, target_size_units, queue_ahead_units,
                spread_units, inventory_units
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(ts_ms),
                ticker,
                decision.side,
                decision.mode,
                decision.fair_yes_units,
                decision.fair_side_units,
                decision.price_units,
                decision.reservation_units,
                decision.market_target_units,
                int(decision.expected_edge_units),
                int(decision.estimated_fee_units),
                int(decision.estimated_toxicity_units),
                int(decision.estimated_incentive_units),
                int(decision.inventory_penalty_units),
                int(decision.queue_penalty_units),
                int(decision.target_size_units),
                (decision.projected_queue_ahead_units if decision.projected_queue_ahead_units is not None else context.queue_ahead_units_for_side(decision.side)),
                context.spread_units_for_side(decision.side),
                int(context.inventory_units),
            ),
        )

    def record_markout(
        self,
        *,
        fill_key: str,
        ts_ms: int,
        ticker: str,
        side: str,
        horizon_ms: int,
        fill_price_units: int,
        future_mid_yes_units: Optional[int],
        future_fair_yes_units: Optional[int],
        adverse_units: int,
        bucket_key: str,
    ) -> None:
        self._execute(
            """
            INSERT INTO markouts (
                fill_key, ts_ms, ticker, side, horizon_ms, fill_price_units, future_mid_yes_units,
                future_fair_yes_units, adverse_units, bucket_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill_key,
                int(ts_ms),
                ticker,
                side,
                int(horizon_ms),
                int(fill_price_units),
                future_mid_yes_units,
                future_fair_yes_units,
                int(adverse_units),
                bucket_key,
            ),
        )

    def record_market_state(self, *, ts_ms: int, ticker: str, source: str, context: MarketContext) -> None:
        self._execute(
            """
            INSERT INTO market_state (
                ts_ms, ticker, source, best_yes_bid_units, best_no_bid_units, implied_yes_ask_units,
                implied_no_ask_units, best_yes_bid_size_units, best_no_bid_size_units, ticker_price_units,
                ticker_yes_bid_units, ticker_yes_ask_units, yes_bid_size_units, yes_ask_size_units,
                last_trade_yes_units, last_trade_size_units, trade_bias_bp, inventory_units
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(ts_ms),
                ticker,
                source,
                int(context.best_yes_bid_units),
                int(context.best_no_bid_units),
                int(context.implied_yes_ask_units),
                int(context.implied_no_ask_units),
                int(context.best_yes_bid_size_units),
                int(context.best_no_bid_size_units),
                context.ticker_price_units,
                context.ticker_yes_bid_units,
                context.ticker_yes_ask_units,
                context.ticker_yes_bid_size_units,
                context.ticker_yes_ask_size_units,
                context.last_trade_yes_units,
                int(context.last_trade_size_units),
                int(round(context.trade_bias * 10_000)),
                int(context.inventory_units),
            ),
        )

    def record_ticker(self, *, ticker_state: TickerState) -> None:
        self._execute(
            """
            INSERT INTO ticker_updates (
                ts_ms, ticker, price_units, yes_bid_units, yes_ask_units, volume_units,
                open_interest_units, yes_bid_size_units, yes_ask_size_units, last_trade_size_units
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(ticker_state.ts_ms),
                ticker_state.market_ticker,
                ticker_state.price_units,
                ticker_state.yes_bid_units,
                ticker_state.yes_ask_units,
                ticker_state.volume_units,
                ticker_state.open_interest_units,
                ticker_state.yes_bid_size_units,
                ticker_state.yes_ask_size_units,
                ticker_state.last_trade_size_units,
            ),
        )

    def record_public_trade(self, *, trade: PublicTradeTick) -> None:
        self._execute(
            """
            INSERT INTO public_trades (
                ts_ms, ticker, trade_id, yes_price_units, no_price_units, count_units, taker_side
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(trade.ts_ms),
                trade.market_ticker,
                trade.trade_id,
                trade.yes_price_units,
                trade.no_price_units,
                int(trade.count_units),
                trade.taker_side,
            ),
        )


class FeeModel:
    def __init__(self, api_client: KalshiApiClient, market: MarketMetadata, settings: BotSettings, telemetry_store: TelemetryStore) -> None:
        self.api_client = api_client
        self.market = market
        self.settings = settings
        self.telemetry_store = telemetry_store
        self.fee_type = "quadratic"
        self.fee_multiplier = 1.0
        self.maker_fee_factor = float(settings.default_fee_factor_for_maker_quotes)
        self.realized_fee_per_contract_units_ewma: Optional[float] = None
        self.upcoming_fee_changes: List[dict] = []
        self.last_refresh_ms = 0

    def refresh_from_api(self) -> None:
        try:
            series_payload = self.api_client.get_series(self.market.series_ticker)
            self.fee_type = str(series_payload.get("fee_type") or self.fee_type)
            self.fee_multiplier = float(series_payload.get("fee_multiplier") or self.fee_multiplier or 1.0)
        except Exception as exc:
            log_event("FEE_MODEL_SERIES_REFRESH_ERROR", error=str(exc), series=self.market.series_ticker)
        try:
            self.upcoming_fee_changes = self.api_client.get_series_fee_changes(self.market.series_ticker, show_historical=False)
        except Exception as exc:
            log_event("FEE_MODEL_FEE_CHANGE_REFRESH_ERROR", error=str(exc), series=self.market.series_ticker)
        self.bootstrap_from_telemetry(limit=250)
        self.last_refresh_ms = now_ms()
        log_event(
            "FEE_MODEL_REFRESHED",
            series=self.market.series_ticker,
            fee_type=self.fee_type,
            fee_multiplier=self.fee_multiplier,
            maker_fee_factor=round(self.maker_fee_factor, 4),
            upcoming_changes=len(self.upcoming_fee_changes),
        )

    def bootstrap_from_telemetry(self, *, limit: int = 250) -> None:
        for row in self.telemetry_store.load_recent_fills(limit=limit):
            price_units = row["price_units"]
            size_units = row["size_units"]
            fee_units = row["fee_units"]
            if price_units is None or size_units is None or fee_units is None:
                continue
            self.record_fill_fee(price_units=int(price_units), count_units=int(size_units), fee_units=int(fee_units))

    def baseline_fee_units(self, *, price_units: int, count_units: int) -> int:
        if price_units <= 0 or count_units <= 0:
            return 0
        if self.fee_type.lower() != "quadratic":
            return 0
        contracts = Decimal(count_units) / Decimal(COUNT_SCALE)
        price = Decimal(price_units) / Decimal(PRICE_SCALE)
        fee_dollars = Decimal("0.07") * Decimal(str(self.fee_multiplier)) * contracts * price * (Decimal("1") - price)
        return int((fee_dollars * PRICE_SCALE).to_integral_value(rounding=ROUND_HALF_UP))

    def record_fill_fee(self, *, price_units: int, count_units: int, fee_units: int) -> None:
        if fee_units < 0 or count_units <= 0:
            return
        contracts = max(0.01, count_units / COUNT_SCALE)
        per_contract_units = fee_units / contracts
        if self.realized_fee_per_contract_units_ewma is None:
            self.realized_fee_per_contract_units_ewma = per_contract_units
        else:
            self.realized_fee_per_contract_units_ewma = (0.85 * self.realized_fee_per_contract_units_ewma) + (0.15 * per_contract_units)

        baseline_units = self.baseline_fee_units(price_units=price_units, count_units=count_units)
        if baseline_units > 0:
            ratio = max(0.0, min(2.0, fee_units / baseline_units))
            self.maker_fee_factor = (0.85 * self.maker_fee_factor) + (0.15 * ratio)

    def estimate_fee_per_contract_units(self, *, price_units: int) -> int:
        baseline_units = self.baseline_fee_units(price_units=price_units, count_units=COUNT_SCALE)
        estimate = float(baseline_units) * float(self.maker_fee_factor)
        if self.realized_fee_per_contract_units_ewma is not None:
            estimate = (0.50 * estimate) + (0.50 * self.realized_fee_per_contract_units_ewma)
        return max(0, int(round(estimate)))

    def estimate_fee_units(self, *, price_units: int, count_units: int) -> int:
        if count_units <= 0:
            return 0
        contracts = Decimal(count_units) / Decimal(COUNT_SCALE)
        per_contract_units = Decimal(self.estimate_fee_per_contract_units(price_units=price_units))
        return int((per_contract_units * contracts).to_integral_value(rounding=ROUND_HALF_UP))


class IncentiveModel:
    def __init__(self, api_client: KalshiApiClient, market: MarketMetadata) -> None:
        self.api_client = api_client
        self.market = market
        self.programs: List[dict] = []
        self.last_refresh_ms = 0

    def refresh_from_api(self) -> None:
        try:
            programs = self.api_client.get_incentive_programs(status="active", incentive_type="all", limit=10_000)
            self.programs = [
                program
                for program in programs
                if str(program.get("market_ticker") or "") == self.market.ticker
            ]
            self.last_refresh_ms = now_ms()
            log_event("INCENTIVE_MODEL_REFRESHED", ticker=self.market.ticker, active_programs=len(self.programs))
        except Exception as exc:
            log_event("INCENTIVE_MODEL_REFRESH_ERROR", ticker=self.market.ticker, error=str(exc))

    def active_programs_for_market(self) -> List[dict]:
        return list(self.programs)

    def estimate_incentive_per_contract_units(
        self,
        *,
        quote_price_units: int,
        size_units: int,
        tick_distance_from_best: int,
    ) -> int:
        if quote_price_units <= 0 or size_units <= 0 or not self.programs:
            return 0

        quality = 1.0 if tick_distance_from_best <= 0 else max(0.0, 1.0 - 0.5 * tick_distance_from_best)
        total_bonus_units = 0.0
        for program in self.programs:
            discount_factor_bps = float(program.get("discount_factor_bps") or 0.0)
            if discount_factor_bps <= 0:
                continue
            target_size_units = parse_count_units_from_fp(program.get("target_size_fp") or "0") if program.get("target_size_fp") else 0
            size_scale = 1.0
            if target_size_units > 0:
                size_scale = min(1.0, size_units / target_size_units)

            per_contract_bonus_units = float(quote_price_units) * (discount_factor_bps / 10_000.0)
            incentive_type = str(program.get("incentive_type") or "")
            if incentive_type == "liquidity":
                per_contract_bonus_units *= quality * size_scale
            else:
                per_contract_bonus_units *= size_scale
            total_bonus_units += per_contract_bonus_units

        return max(0, int(round(total_bonus_units)))


class FairValueEngine:
    def __init__(self, market: MarketMetadata, settings: BotSettings) -> None:
        self.market = market
        self.settings = settings
        self.last_fair_yes_units: Optional[int] = None

    def estimate(self, context: MarketContext, *, update_state: bool = True) -> FairValueEstimate:
        weights: List[Tuple[int, float]] = []
        weights.append((context.mid_yes_units, float(self.settings.fair_value_mid_weight)))
        if context.ticker_price_units is not None:
            weights.append((context.ticker_price_units, float(self.settings.fair_value_ticker_weight)))
        if context.last_trade_yes_units is not None:
            weights.append((context.last_trade_yes_units, float(self.settings.fair_value_trade_weight)))

        total_weight = sum(weight for _, weight in weights) or 1.0
        raw_fair_yes_units = int(round(sum(price_units * weight for price_units, weight in weights) / total_weight))

        order_book_adjust_units = int(round(
            context.order_book_imbalance
            * self.settings.fair_value_max_orderbook_imbalance_adjust_cents
            * PRICE_UNITS_PER_CENT
        ))
        trade_bias_adjust_units = int(round(
            context.trade_bias
            * self.settings.fair_value_max_trade_bias_adjust_cents
            * PRICE_UNITS_PER_CENT
        ))

        urgency_multiplier = 1.0
        if context.seconds_to_close is not None:
            if context.seconds_to_close <= 900:
                urgency_multiplier = 1.25
            elif context.seconds_to_close <= 3600:
                urgency_multiplier = 1.0
            elif context.seconds_to_close <= 6 * 3600:
                urgency_multiplier = 0.80
            else:
                urgency_multiplier = 0.60

        adjusted_fair_units = raw_fair_yes_units + int(round((order_book_adjust_units + trade_bias_adjust_units) * urgency_multiplier))
        adjusted_fair_units = max(self.market.price_grid.minimum_valid_price_units, min(self.market.price_grid.maximum_valid_price_units, adjusted_fair_units))
        adjusted_fair_units = self.market.price_grid.floor_to_valid(adjusted_fair_units) or adjusted_fair_units

        if self.last_fair_yes_units is not None:
            # Use a faster EMA (more weight on new data) when the market is
            # approaching expiry.  Long-dated markets benefit from smoothing to
            # suppress noise; short-dated markets move fast and smoothing
            # introduces toxic lag (fills at stale prices by informed takers).
            if context.seconds_to_close is not None and context.seconds_to_close <= 1800:
                ema_alpha = 0.90   # <30 min: nearly no smoothing
            elif context.seconds_to_close is not None and context.seconds_to_close <= 3600:
                ema_alpha = 0.80   # 30–60 min: light smoothing
            else:
                ema_alpha = 0.60   # >60 min: original smoothing
            adjusted_fair_units = int(round(((1.0 - ema_alpha) * self.last_fair_yes_units) + (ema_alpha * adjusted_fair_units)))
            adjusted_fair_units = max(self.market.price_grid.minimum_valid_price_units, min(self.market.price_grid.maximum_valid_price_units, adjusted_fair_units))
            adjusted_fair_units = self.market.price_grid.floor_to_valid(adjusted_fair_units) or adjusted_fair_units

        if update_state:
            self.last_fair_yes_units = adjusted_fair_units

        return FairValueEstimate(
            fair_yes_units=int(adjusted_fair_units),
            fair_no_units=int(ONE_DOLLAR_PRICE_UNITS - adjusted_fair_units),
            raw_fair_yes_units=int(raw_fair_yes_units),
            mid_yes_units=int(context.mid_yes_units),
            ticker_anchor_yes_units=context.ticker_price_units,
            trade_anchor_yes_units=context.last_trade_yes_units,
            order_book_adjust_units=int(order_book_adjust_units),
            trade_bias_adjust_units=int(trade_bias_adjust_units),
        )


class ToxicityModel:
    def __init__(self, settings: BotSettings, telemetry_store: TelemetryStore) -> None:
        self.settings = settings
        self.telemetry_store = telemetry_store
        self.stats: Dict[Tuple[int, str], ToxicityStat] = {}

    def bootstrap_from_telemetry(self, *, limit: int = 5000) -> None:
        for row in self.telemetry_store.load_recent_markouts(limit=limit):
            bucket_key = row["bucket_key"]
            horizon_ms = row["horizon_ms"]
            adverse_units = row["adverse_units"]
            if not bucket_key or adverse_units is None or horizon_ms is None:
                continue
            self._update_bucket(horizon_ms=int(horizon_ms), bucket_key=str(bucket_key), adverse_units=int(adverse_units))

    def _update_bucket(self, *, horizon_ms: int, bucket_key: str, adverse_units: int) -> None:
        key = (int(horizon_ms), bucket_key)
        stat = self.stats.get(key)
        if stat is None:
            stat = ToxicityStat(observations=1, ewma_adverse_units=float(adverse_units))
            self.stats[key] = stat
            return
        stat.observations += 1
        stat.ewma_adverse_units = (0.85 * stat.ewma_adverse_units) + (0.15 * float(adverse_units))

    def time_to_close_bucket(self, seconds_to_close: Optional[float]) -> str:
        if seconds_to_close is None:
            return "na"
        if seconds_to_close <= 900:
            return "lt15m"
        if seconds_to_close <= 3600:
            return "15m_1h"
        if seconds_to_close <= 14_400:
            return "1h_4h"
        return "gt4h"

    def imbalance_bucket(self, imbalance: float) -> str:
        if imbalance <= -0.33:
            return "ask_heavy"
        if imbalance >= 0.33:
            return "bid_heavy"
        return "balanced"

    def trade_bias_bucket(self, trade_bias: float) -> str:
        if trade_bias <= -0.33:
            return "sell_pressure"
        if trade_bias >= 0.33:
            return "buy_pressure"
        return "mixed_flow"

    def queue_bucket(self, queue_ahead_units: Optional[int]) -> str:
        if queue_ahead_units is None:
            return "unknown"
        contracts = queue_ahead_units / COUNT_SCALE
        if contracts <= 0:
            return "front"
        if contracts <= 10:
            return "lt10"
        if contracts <= 100:
            return "10_100"
        return "gt100"

    def spread_bucket(self, spread_units: int) -> str:
        spread_cents = spread_units / PRICE_UNITS_PER_CENT
        if spread_cents <= 2:
            return "tight"
        if spread_cents <= 5:
            return "medium"
        return "wide"

    def inventory_regime_bucket(self, *, side: str, context: MarketContext) -> str:
        reducing_inventory = (
            (context.inventory_units > 0 and side == "no")
            or (context.inventory_units < 0 and side == "yes")
        )
        return "reduce" if reducing_inventory else "normal"

    def bucket_key(self, *, side: str, context: MarketContext) -> str:
        parts = [
            side,
            self.time_to_close_bucket(context.seconds_to_close),
            self.imbalance_bucket(context.order_book_imbalance),
            self.trade_bias_bucket(context.trade_bias),
            self.queue_bucket(context.queue_ahead_units_for_side(side)),
            self.spread_bucket(context.spread_units_for_side(side)),
            self.inventory_regime_bucket(side=side, context=context),
        ]
        return "|".join(parts)

    def record_markout(self, *, side: str, context: MarketContext, horizon_ms: int, adverse_units: int) -> str:
        bucket_key = self.bucket_key(side=side, context=context)
        self._update_bucket(horizon_ms=horizon_ms, bucket_key=bucket_key, adverse_units=adverse_units)
        return bucket_key

    def estimate_markout_per_contract_units(self, *, side: str, context: MarketContext) -> int:
        bucket_key = self.bucket_key(side=side, context=context)
        weighted_total = 0.0
        weight_sum = 0.0

        for horizon_ms, weight in ((5_000, 0.70), (30_000, 0.30)):
            stat = self.stats.get((horizon_ms, bucket_key))
            if stat and stat.observations > 0:
                weighted_total += stat.ewma_adverse_units * weight
                weight_sum += weight

        if weight_sum > 0:
            estimated_units = weighted_total / weight_sum
        else:
            estimated_units = float(cents_to_price_units(self.settings.default_toxicity_cents))

        adverse_trade_flow = max(0.0, -context.trade_bias) if side == "yes" else max(0.0, context.trade_bias)
        adverse_imbalance = max(0.0, -context.order_book_imbalance) if side == "yes" else max(0.0, context.order_book_imbalance)

        estimated_units += adverse_trade_flow * PRICE_UNITS_PER_CENT * 0.5
        estimated_units += adverse_imbalance * PRICE_UNITS_PER_CENT * 0.3

        return max(0, int(round(estimated_units)))

    def estimate_per_contract_units(self, *, side: str, context: MarketContext) -> int:
        return self.estimate_markout_per_contract_units(side=side, context=context)


class FillProbabilityModel:
    def __init__(self, settings: BotSettings, telemetry_store: TelemetryStore, toxicity_model: ToxicityModel) -> None:
        self.settings = settings
        self.telemetry_store = telemetry_store
        self.toxicity_model = toxicity_model
        self.stats: Dict[str, FillProbStat] = {}
        self.pending_by_side: Dict[str, Optional[PendingFillAttempt]] = {
            "yes": None,
            "no": None,
        }

    def bootstrap_from_telemetry(self, *, limit: int = 10000) -> None:
        self.stats.clear()
        for row in self.telemetry_store.load_recent_fill_prob_attempts(limit=limit):
            bucket_key = row["bucket_key"]
            filled_30s = row["filled_30s"]
            if not bucket_key:
                continue
            self._update_bucket(bucket_key=str(bucket_key), filled_30s=bool(filled_30s))

    def _update_bucket(self, *, bucket_key: str, filled_30s: bool) -> None:
        stat = self.stats.get(bucket_key)
        if stat is None:
            stat = FillProbStat()
            self.stats[bucket_key] = stat
        stat.attempts += 1
        if filled_30s:
            stat.fills_30s += 1

    def estimate_fill_probability_30s(self, *, bucket_key: str) -> float:
        stat = self.stats.get(bucket_key)
        attempts = stat.attempts if stat is not None else 0
        fills = stat.fills_30s if stat is not None else 0

        alpha = float(self.settings.fill_probability_prior_fills)
        beta = float(self.settings.fill_probability_prior_misses)
        probability = (fills + alpha) / (attempts + alpha + beta)

        return max(0.01, min(0.99, probability))

    def settle_expired_attempts(self, *, ticker: str, current_ts_ms: int) -> None:
        for side in ("yes", "no"):
            attempt = self.pending_by_side.get(side)
            if attempt is None or attempt.settled:
                continue
            if current_ts_ms < attempt.expiry_ts_ms:
                continue

            self._finalize_attempt(
                ticker=ticker,
                attempt=attempt,
                filled_30s=attempt.filled_within_horizon,
            )
            self.pending_by_side[side] = None

    def observe_quote_decision(
        self,
        *,
        ticker: str,
        side: str,
        decision: SideQuoteDecision,
        bucket_key: str,
        ts_ms: int,
    ) -> None:
        self.settle_expired_attempts(ticker=ticker, current_ts_ms=ts_ms)

        current_attempt = self.pending_by_side.get(side)

        if decision.price_units is None or decision.target_size_units <= 0:
            if current_attempt is not None and not current_attempt.settled:
                self._finalize_attempt(
                    ticker=ticker,
                    attempt=current_attempt,
                    filled_30s=current_attempt.filled_within_horizon,
                )
                self.pending_by_side[side] = None
            return

        if current_attempt is not None and not current_attempt.settled:
            same_attempt = (
                current_attempt.quote_price_units == decision.price_units
                and current_attempt.bucket_key == bucket_key
                and ts_ms < current_attempt.expiry_ts_ms
            )
            if same_attempt:
                return

            self._finalize_attempt(
                ticker=ticker,
                attempt=current_attempt,
                filled_30s=current_attempt.filled_within_horizon,
            )
            self.pending_by_side[side] = None

        horizon_ms = int(self.settings.fill_probability_horizon_seconds) * 1000
        self.pending_by_side[side] = PendingFillAttempt(
            attempt_id=f"{side}:{ts_ms}:{decision.price_units}",
            side=side,
            bucket_key=bucket_key,
            quote_price_units=int(decision.price_units),
            started_ts_ms=int(ts_ms),
            expiry_ts_ms=int(ts_ms + horizon_ms),
        )

    def register_fill(self, *, side: str, ts_ms: int) -> None:
        attempt = self.pending_by_side.get(side)
        if attempt is None or attempt.settled:
            return
        if ts_ms <= attempt.expiry_ts_ms:
            attempt.filled_within_horizon = True

    def _finalize_attempt(self, *, ticker: str, attempt: PendingFillAttempt, filled_30s: bool) -> None:
        if attempt.settled:
            return
        attempt.settled = True
        self._update_bucket(bucket_key=attempt.bucket_key, filled_30s=filled_30s)
        self.telemetry_store.record_fill_prob_attempt(
            attempt_id=attempt.attempt_id,
            ts_ms=attempt.started_ts_ms,
            ticker=ticker,
            side=attempt.side,
            bucket_key=attempt.bucket_key,
            quote_price_units=attempt.quote_price_units,
            filled_30s=filled_30s,
        )


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------


class TopOfBookBot:
    def __init__(self, settings: BotSettings, api_client: KalshiApiClient, market: MarketMetadata) -> None:
        self.settings = settings
        self.api_client = api_client
        self.market = market

        self.book_yes: Dict[int, int] = {}
        self.book_no: Dict[int, int] = {}
        self.ticker_state: Optional[TickerState] = None
        self.recent_trades: Deque[PublicTradeTick] = deque(maxlen=2_000)
        self.seen_fill_keys: set[str] = set()
        self.last_quote_decisions: Dict[str, SideQuoteDecision] = {
            "yes": SideQuoteDecision(side="yes", price_units=None, target_size_units=0, mode="startup"),
            "no": SideQuoteDecision(side="no", price_units=None, target_size_units=0, mode="startup"),
        }
        self.known_strategy_order_sides: Dict[str, str] = {}
        safe_ticker = self.settings.market_ticker.replace("/", "_").replace("\\", "_")
        telemetry_path = self.settings.telemetry_sqlite_path or os.path.join(os.getcwd(), f"telemetry_{safe_ticker}.sqlite3")
        self.telemetry_store = TelemetryStore(telemetry_path, enabled=self.settings.enable_sqlite_telemetry)
        self.fee_model = FeeModel(self.api_client, self.market, self.settings, self.telemetry_store)
        self.incentive_model = IncentiveModel(self.api_client, self.market)
        self.fair_value_engine = FairValueEngine(self.market, self.settings)
        self.toxicity_model = ToxicityModel(self.settings, self.telemetry_store)
        self.fill_probability_model = FillProbabilityModel(
            self.settings,
            self.telemetry_store,
            self.toxicity_model,
        )

        self.book_ready = False
        self.last_orderbook_sequence: Optional[int] = None
        self.last_fill_timestamp_ms = 0
        self.last_requote_action_timestamp_ms = 0
        self.market_wide_queue_cooldown_until_ms = 0
        self.recent_liquidity_pull_events: Dict[str, Deque[Tuple[int, int]]] = {
            "yes": deque(),
            "no": deque(),
        }
        self.side_liquidity_pull_cooldown_until_ms: Dict[str, int] = {
            "yes": 0,
            "no": 0,
        }
        self.market_wide_toxicity_cooldown_until_ms = 0
        self.last_market_probability_guard_state: Optional[bool] = None
        self.last_market_queue_cooldown_logged_state: Optional[bool] = None
        self.last_market_state_log_ms = 0

        self.net_position_units = 0

        self.orders: Dict[str, ManagedOrderState] = {
            "yes": ManagedOrderState(side="yes"),
            "no": ManagedOrderState(side="no"),
        }

        self.requote_event = asyncio.Event()
        self.requote_lock = asyncio.Lock()
        self.watchdog_state: Optional[WatchdogState] = None
        self.watchdog_state_mtime_ns: Optional[int] = None
        self.started_at_ms = now_ms()
        self.watchdog_last_mode_logged: Optional[str] = None
        self.watchdog_shutdown_in_progress = False
        self.shutdown_requested = False
        self.shutdown_exit_code: Optional[int] = None
        self.websocket_connection = None
        self.background_tasks: List[asyncio.Task] = []
        self.instance_expiration_jitter_seconds = (
            random.randint(0, self.settings.expiration_jitter_seconds)
            if self.settings.expiration_jitter_seconds > 0
            else 0
        )

    # -------------------------
    # Startup / state sync
    # -------------------------

    def is_strategy_client_order_id(self, client_order_id: str) -> bool:
        if not client_order_id:
            return False
        return client_order_id.startswith(self.settings.legacy_client_order_prefixes)

    def load_startup_position(self) -> None:
        response = self.api_client.get_positions(self.settings.market_ticker)
        market_positions = response.get("market_positions") or []
        if not market_positions:
            self.net_position_units = 0
            log_event("STARTUP_POSITION", contracts="0.00")
            return

        position_payload = market_positions[0]
        position_units = parse_optional_count_units(position_payload, "position_fp", "position")
        self.net_position_units = int(position_units or 0)
        log_event("STARTUP_POSITION", contracts=format_count_fp(self.net_position_units))

    def cancel_owned_resting_quotes_on_startup(self) -> None:
        if not self.settings.cancel_strategy_quotes_on_startup:
            return

        resting_orders = self.api_client.get_resting_orders_for_market(self.settings.market_ticker)
        canceled_count = 0

        for order in resting_orders:
            client_order_id = str(order.get("client_order_id") or "")
            if not self.is_strategy_client_order_id(client_order_id):
                continue

            order_id = order.get("order_id")
            if not order_id:
                continue

            try:
                self.api_client.cancel_order(order_id=order_id)
                canceled_count += 1
                log_event("STARTUP_CANCELLED_OWN_ORDER", order_id=order_id, client_order_id=client_order_id)
            except Exception as exc:
                if is_order_not_found_error(exc):
                    continue
                raise

        if canceled_count:
            log_event("STARTUP_CANCEL_SUMMARY", canceled_orders=canceled_count)

    # -------------------------
    # Orderbook helpers
    # -------------------------

    def best_bid(self, side: str) -> Optional[int]:
        levels = self.book_yes if side == "yes" else self.book_no
        return max(levels.keys()) if levels else None

    def best_bid_excluding_our_order(self, side: str) -> Optional[int]:
        levels = self.book_yes if side == "yes" else self.book_no
        best_price_units = self.best_bid(side)
        state = self.orders[side]

        if best_price_units is None or state.price_units is None:
            return best_price_units
        if state.price_units != best_price_units:
            return best_price_units

        quantity_at_best_units = int(levels.get(best_price_units, 0))
        our_remaining_units = int(state.remaining_count_units or 0)
        if quantity_at_best_units - our_remaining_units > 0:
            return best_price_units

        for candidate_price_units in sorted(levels.keys(), reverse=True):
            if candidate_price_units < best_price_units:
                return candidate_price_units
        return None

    def implied_yes_ask(self, best_no_bid_units: int) -> int:
        return ONE_DOLLAR_PRICE_UNITS - int(best_no_bid_units)

    def implied_no_ask(self, best_yes_bid_units: int) -> int:
        return ONE_DOLLAR_PRICE_UNITS - int(best_yes_bid_units)

    def best_bid_size_units(self, side: str) -> int:
        best_price_units = self.best_bid(side)
        if best_price_units is None:
            return 0
        levels = self.book_yes if side == "yes" else self.book_no
        return int(levels.get(best_price_units, 0) or 0)

    def second_best_bid(self, side: str) -> Optional[int]:
        """Return the second-best bid price level, or None if fewer than 2 levels exist."""
        levels = self.book_yes if side == "yes" else self.book_no
        sorted_prices = sorted(levels.keys(), reverse=True)
        return sorted_prices[1] if len(sorted_prices) >= 2 else None

    def displayed_size_units_at_price(self, side: str, price_units: Optional[int]) -> int:
        if price_units is None:
            return 0
        levels = self.book_yes if side == "yes" else self.book_no
        return int(levels.get(price_units, 0) or 0)

    def displayed_size_units_excluding_our_order(self, side: str, price_units: Optional[int]) -> int:
        if price_units is None:
            return 0
        displayed_units = self.displayed_size_units_at_price(side, price_units)
        state = self.orders[side]
        if state.has_active_resting_order and state.price_units == price_units:
            displayed_units -= state.remaining_count_units
        return max(0, displayed_units)

    def best_bid_and_size_excluding_our_order(self, side: str) -> Tuple[Optional[int], int]:
        price_units = self.best_bid_excluding_our_order(side)
        if price_units is None:
            return None, 0
        return price_units, self.displayed_size_units_excluding_our_order(side, price_units)

    def order_book_imbalance_excluding_our_quotes(self) -> float:
        _, yes_best_size_units = self.best_bid_and_size_excluding_our_order("yes")
        if self.ticker_state is not None and self.ticker_state.yes_ask_size_units is not None:
            yes_ask_size_units = int(self.ticker_state.yes_ask_size_units)
        else:
            yes_ask_size_units = self.best_bid_size_units("no")

        denominator = yes_best_size_units + max(0, yes_ask_size_units)
        if denominator <= 0:
            return 0.0
        return (yes_best_size_units - max(0, yes_ask_size_units)) / denominator

    def projected_queue_ahead_units_for_price(self, side: str, price_units: Optional[int]) -> Optional[int]:
        if price_units is None:
            return None
        state = self.orders[side]
        if state.has_active_resting_order and state.price_units == price_units and state.last_queue_position_units is not None:
            return int(state.last_queue_position_units)
        return self.displayed_size_units_at_price(side, price_units)

    def top_price_levels(self, side: str, level_count: Optional[int] = None) -> List[Tuple[int, int]]:
        levels = self.book_yes if side == "yes" else self.book_no
        if not levels:
            return []
        requested_level_count = int(level_count or self.settings.orderbook_pull_top_levels_to_track)
        if requested_level_count <= 0:
            return []
        top_prices = sorted(levels.keys(), reverse=True)[:requested_level_count]
        return [(price_units, int(levels.get(price_units, 0) or 0)) for price_units in top_prices]

    def current_top_depth_units(self, side: str, *, level_count: Optional[int] = None) -> int:
        return sum(size_units for _, size_units in self.top_price_levels(side, level_count=level_count))

    def prune_recent_liquidity_pull_events(self) -> None:
        if self.settings.orderbook_pull_window_ms <= 0:
            for events in self.recent_liquidity_pull_events.values():
                events.clear()
            return
        cutoff_ms = now_ms() - self.settings.orderbook_pull_window_ms
        for events in self.recent_liquidity_pull_events.values():
            while events and events[0][0] < cutoff_ms:
                events.popleft()

    def current_liquidity_pull_score(self, side: str) -> float:
        if not self.settings.enable_orderbook_pull_toxicity_guard:
            return 0.0
        self.prune_recent_liquidity_pull_events()
        aggregate_removed_units = float(sum(weighted_removed_units for _, weighted_removed_units in self.recent_liquidity_pull_events[side]))
        top_depth_units = max(0, self.current_top_depth_units(side))
        absolute_threshold_units = contracts_to_count_units(self.settings.orderbook_pull_absolute_threshold_contracts)
        relative_threshold_units = int(round(top_depth_units * float(self.settings.orderbook_pull_relative_depth_threshold)))
        threshold_units = max(absolute_threshold_units, relative_threshold_units, COUNT_SCALE)
        return max(0.0, aggregate_removed_units / float(threshold_units))

    def side_liquidity_pull_cooldown_active(self, side: str) -> bool:
        return now_ms() < int(self.side_liquidity_pull_cooldown_until_ms.get(side, 0))

    def market_toxicity_cooldown_active(self) -> bool:
        return now_ms() < int(self.market_wide_toxicity_cooldown_until_ms)

    def observe_orderbook_toxicity_from_delta(self, delta_message: dict) -> None:
        if not self.settings.enable_orderbook_pull_toxicity_guard:
            return

        side = str(delta_message.get("side") or "")
        if side not in {"yes", "no"}:
            return

        delta_units = parse_optional_count_units(delta_message, "delta_fp", "delta")
        price_units = parse_optional_price_units(delta_message, "price_dollars", "price")
        if delta_units is None or price_units is None or delta_units >= 0:
            return

        top_levels = self.top_price_levels(side, level_count=self.settings.orderbook_pull_top_levels_to_track)
        if not top_levels:
            return

        level_distance = None
        visible_top_depth_units = 0
        for index, (level_price_units, size_units) in enumerate(top_levels):
            visible_top_depth_units += size_units
            if level_price_units == price_units:
                level_distance = index

        if level_distance is None:
            return

        weighted_removed_units = int(round(abs(delta_units) / float(1 + level_distance)))
        event_timestamp_ms = normalize_exchange_timestamp_to_ms(delta_message.get("ts"))
        self.recent_liquidity_pull_events[side].append((event_timestamp_ms, weighted_removed_units))
        self.prune_recent_liquidity_pull_events()

        absolute_threshold_units = contracts_to_count_units(self.settings.orderbook_pull_absolute_threshold_contracts)
        relative_threshold_units = int(round(visible_top_depth_units * float(self.settings.orderbook_pull_relative_depth_threshold)))
        threshold_units = max(absolute_threshold_units, relative_threshold_units, COUNT_SCALE)
        aggregate_removed_units = int(sum(value for _, value in self.recent_liquidity_pull_events[side]))
        if aggregate_removed_units < threshold_units:
            return

        side_cooldown_until_ms = event_timestamp_ms + int(self.settings.orderbook_pull_side_cooldown_ms)
        if side_cooldown_until_ms > self.side_liquidity_pull_cooldown_until_ms[side]:
            self.side_liquidity_pull_cooldown_until_ms[side] = side_cooldown_until_ms
            log_event(
                "ORDERBOOK_PULL_TOXICITY",
                side=side,
                removed_contracts=format_count_fp(aggregate_removed_units),
                threshold_contracts=format_count_fp(threshold_units),
                cooldown_ms=self.settings.orderbook_pull_side_cooldown_ms,
                level_distance=level_distance,
            )

        opposite_side = "no" if side == "yes" else "yes"
        if self.side_liquidity_pull_cooldown_active(opposite_side):
            self.market_wide_toxicity_cooldown_until_ms = max(
                self.market_wide_toxicity_cooldown_until_ms,
                event_timestamp_ms + int(self.settings.orderbook_pull_market_cooldown_ms),
            )

    def side_reduces_inventory_risk(self, side: str) -> bool:
        return (self.net_position_units > 0 and side == "no") or (self.net_position_units < 0 and side == "yes")

    def prune_recent_trades(self) -> None:
        cutoff_ms = now_ms() - (self.settings.trade_history_window_seconds * 1000)
        while self.recent_trades and self.recent_trades[0].ts_ms < cutoff_ms:
            self.recent_trades.popleft()

    def latest_public_trade(self) -> Optional[PublicTradeTick]:
        self.prune_recent_trades()
        if not self.recent_trades:
            return None
        return self.recent_trades[-1]

    def seconds_to_close(self) -> Optional[float]:
        if self.market.close_time_ms is None:
            return None
        return max(0.0, (self.market.close_time_ms - now_ms()) / 1000.0)

    def refresh_external_models(self) -> None:
        self.fee_model.refresh_from_api()
        self.incentive_model.refresh_from_api()
        self.toxicity_model.bootstrap_from_telemetry(limit=5_000)
        self.fill_probability_model.bootstrap_from_telemetry(limit=10_000)

    def build_market_context(self) -> Optional[MarketContext]:
        best_yes_bid_units = self.best_bid("yes")
        best_no_bid_units = self.best_bid("no")
        if best_yes_bid_units is None or best_no_bid_units is None:
            return None

        implied_yes_ask_units = self.implied_yes_ask(best_no_bid_units)
        implied_no_ask_units = self.implied_no_ask(best_yes_bid_units)
        self.prune_recent_trades()
        recent_trades = list(self.recent_trades)

        signed_trade_units = 0
        trade_momentum_units = 0
        last_trade_yes_units: Optional[int] = None
        last_trade_size_units = 0
        if recent_trades:
            last_trade_yes_units = recent_trades[-1].yes_price_units
            last_trade_size_units = recent_trades[-1].count_units
            signed_trade_units = sum(
                (trade.count_units if trade.taker_side == "yes" else -trade.count_units if trade.taker_side == "no" else 0)
                for trade in recent_trades
            )
            trade_momentum_units = recent_trades[-1].yes_price_units - recent_trades[0].yes_price_units

        total_trade_units = sum(trade.count_units for trade in recent_trades)
        trade_bias = (signed_trade_units / total_trade_units) if total_trade_units > 0 else 0.0

        best_yes_bid_size_units = self.best_bid_size_units("yes")
        best_no_bid_size_units = self.best_bid_size_units("no")
        ticker_yes_ask_size_units = None
        if self.ticker_state is not None:
            ticker_yes_ask_size_units = self.ticker_state.yes_ask_size_units
        if ticker_yes_ask_size_units is None:
            ticker_yes_ask_size_units = best_no_bid_size_units

        denominator = best_yes_bid_size_units + max(0, int(ticker_yes_ask_size_units or 0))
        order_book_imbalance = 0.0
        if denominator > 0:
            order_book_imbalance = (best_yes_bid_size_units - int(ticker_yes_ask_size_units or 0)) / denominator

        return MarketContext(
            ts_ms=now_ms(),
            best_yes_bid_units=best_yes_bid_units,
            best_no_bid_units=best_no_bid_units,
            implied_yes_ask_units=implied_yes_ask_units,
            implied_no_ask_units=implied_no_ask_units,
            best_yes_bid_size_units=best_yes_bid_size_units,
            best_no_bid_size_units=best_no_bid_size_units,
            ticker_price_units=(self.ticker_state.price_units if self.ticker_state is not None else None),
            ticker_yes_bid_units=(self.ticker_state.yes_bid_units if self.ticker_state is not None else None),
            ticker_yes_ask_units=(self.ticker_state.yes_ask_units if self.ticker_state is not None else None),
            ticker_yes_bid_size_units=(self.ticker_state.yes_bid_size_units if self.ticker_state is not None else None),
            ticker_yes_ask_size_units=ticker_yes_ask_size_units,
            last_trade_yes_units=last_trade_yes_units,
            last_trade_size_units=last_trade_size_units,
            trade_bias=float(max(-1.0, min(1.0, trade_bias))),
            trade_momentum_units=int(trade_momentum_units),
            order_book_imbalance=float(max(-1.0, min(1.0, order_book_imbalance))),
            inventory_units=int(self.net_position_units),
            queue_ahead_yes_units=self.orders["yes"].last_queue_position_units,
            queue_ahead_no_units=self.orders["no"].last_queue_position_units,
            seconds_to_close=self.seconds_to_close(),
        )
    def build_signal_market_context(self, live_context: MarketContext) -> MarketContext:
        signal_yes_bid_units, signal_yes_bid_size_units = self.best_bid_and_size_excluding_our_order("yes")
        signal_no_bid_units, signal_no_bid_size_units = self.best_bid_and_size_excluding_our_order("no")

        if signal_yes_bid_units is None:
            signal_yes_bid_units = live_context.best_yes_bid_units
            signal_yes_bid_size_units = live_context.best_yes_bid_size_units

        if signal_no_bid_units is None:
            signal_no_bid_units = live_context.best_no_bid_units
            signal_no_bid_size_units = live_context.best_no_bid_size_units

        signal_implied_yes_ask_units = self.implied_yes_ask(signal_no_bid_units)
        signal_implied_no_ask_units = self.implied_no_ask(signal_yes_bid_units)

        signal_imbalance = 0.0
        denominator = signal_yes_bid_size_units + max(0, signal_no_bid_size_units)
        if denominator > 0:
            signal_imbalance = (signal_yes_bid_size_units - signal_no_bid_size_units) / denominator

        return MarketContext(
            ts_ms=live_context.ts_ms,
            best_yes_bid_units=signal_yes_bid_units,
            best_no_bid_units=signal_no_bid_units,
            implied_yes_ask_units=signal_implied_yes_ask_units,
            implied_no_ask_units=signal_implied_no_ask_units,
            best_yes_bid_size_units=signal_yes_bid_size_units,
            best_no_bid_size_units=signal_no_bid_size_units,
            ticker_price_units=live_context.ticker_price_units,
            ticker_yes_bid_units=live_context.ticker_yes_bid_units,
            ticker_yes_ask_units=live_context.ticker_yes_ask_units,
            ticker_yes_bid_size_units=live_context.ticker_yes_bid_size_units,
            ticker_yes_ask_size_units=signal_no_bid_size_units,
            last_trade_yes_units=live_context.last_trade_yes_units,
            last_trade_size_units=live_context.last_trade_size_units,
            trade_bias=live_context.trade_bias,
            trade_momentum_units=live_context.trade_momentum_units,
            order_book_imbalance=max(-1.0, min(1.0, signal_imbalance)),
            inventory_units=live_context.inventory_units,
            queue_ahead_yes_units=live_context.queue_ahead_yes_units,
            queue_ahead_no_units=live_context.queue_ahead_no_units,
            seconds_to_close=live_context.seconds_to_close,
        )
    def maybe_record_market_state(self, *, source: str, context: Optional[MarketContext]) -> None:
        if context is None:
            return
        if context.ts_ms - self.last_market_state_log_ms < 1_000 and source == "orderbook":
            return
        self.last_market_state_log_ms = context.ts_ms
        self.telemetry_store.record_market_state(
            ts_ms=context.ts_ms,
            ticker=self.market.ticker,
            source=source,
            context=context,
        )

    def log_quote_decision_transition(self, side: str, decision: SideQuoteDecision, context: MarketContext) -> None:
        live_best_units = self.best_bid(side)
        live_best_size_units = self.best_bid_size_units(side)
        exself_best_units, exself_best_size_units = self.best_bid_and_size_excluding_our_order(side)
        state = self.orders[side]

        log_event(
            "QUOTE_DECISION",
            side=side,
            mode=decision.mode,
            reason=(decision.reason or decision.mode),
            quote_c=("none" if decision.price_units is None else f"{decision.price_units / PRICE_UNITS_PER_CENT:.2f}"),
            fair_yes_c=("none" if decision.fair_yes_units is None else f"{decision.fair_yes_units / PRICE_UNITS_PER_CENT:.2f}"),
            fair_side_c=("none" if decision.fair_side_units is None else f"{decision.fair_side_units / PRICE_UNITS_PER_CENT:.2f}"),
            edge_c=f"{decision.expected_edge_units / PRICE_UNITS_PER_CENT:.2f}",
            fee_c=f"{decision.estimated_fee_units / PRICE_UNITS_PER_CENT:.2f}",
            tox_c=f"{decision.estimated_toxicity_units / PRICE_UNITS_PER_CENT:.2f}",
            pfill30=f"{decision.estimated_fill_probability_30s:.3f}",
            markout_c=f"{decision.estimated_markout_units / PRICE_UNITS_PER_CENT:.2f}",
            score_hr_c=f"{decision.score_per_hour_units / PRICE_UNITS_PER_CENT:.2f}",
            bucket=(decision.bucket_key or "na"),
            qpen_c=f"{decision.queue_penalty_units / PRICE_UNITS_PER_CENT:.2f}",
            live_best_c=("none" if live_best_units is None else f"{live_best_units / PRICE_UNITS_PER_CENT:.2f}"),
            live_best_sz=format_count_fp(live_best_size_units),
            exself_best_c=("none" if exself_best_units is None else f"{exself_best_units / PRICE_UNITS_PER_CENT:.2f}"),
            exself_best_sz=format_count_fp(exself_best_size_units),
            live_imbalance=f"{context.order_book_imbalance:.3f}",
            exself_imbalance=f"{self.order_book_imbalance_excluding_our_quotes():.3f}",
            active_order_c=("none" if state.price_units is None else f"{state.price_units / PRICE_UNITS_PER_CENT:.2f}"),
            active_remaining=format_count_fp(state.remaining_count_units),
            queue_ahead=format_count_fp(state.last_queue_position_units or 0),
        )

        if decision.candidate_debug:
            log_event("QUOTE_CANDIDATES", side=side, detail=decision.candidate_debug)

    def inventory_blocked_side(self) -> Optional[str]:
        if not self.settings.enable_one_way_inventory_guard:
            return None

        guard_threshold_units = contracts_to_count_units(self.settings.one_way_inventory_guard_contracts)
        if self.net_position_units >= guard_threshold_units:
            return "yes"
        if self.net_position_units <= -guard_threshold_units:
            return "no"
        return None

    def current_inventory_skew_ticks(self) -> int:
        if self.settings.inventory_skew_contracts_per_tick <= 0:
            return 0

        skew_ticks = math.trunc(
            self.net_position_units / contracts_to_count_units(self.settings.inventory_skew_contracts_per_tick)
        )
        skew_ticks = max(-self.settings.maximum_inventory_skew_ticks, min(self.settings.maximum_inventory_skew_ticks, skew_ticks))
        return int(skew_ticks)

    def post_fill_improvement_cooldown_active(self) -> bool:
        return now_ms() - self.last_fill_timestamp_ms < self.settings.post_fill_no_improve_cooldown_ms

    def side_same_side_reentry_cooldown_active(self, side: str) -> bool:
        state = self.orders[side]
        return now_ms() < state.same_side_reentry_cooldown_until_ms

    def side_queue_abandonment_cooldown_active(self, side: str) -> bool:
        state = self.orders[side]
        return now_ms() < state.queue_abandonment_cooldown_until_ms

    def market_queue_cooldown_active(self) -> bool:
        return now_ms() < self.market_wide_queue_cooldown_until_ms

    def quote_gate_status(
        self,
        *,
        side: str,
        best_bid_units: int,
        implied_ask_units: int,
        market_guard_active: bool = False,
    ) -> Tuple[bool, str]:
        reducing_inventory = self.side_reduces_inventory_risk(side)
        minimum_bid_units = cents_to_price_units(self.settings.minimum_best_bid_cents_required_to_quote)
        minimum_ask_units = cents_to_price_units(self.settings.minimum_implied_ask_cents_required_to_quote)
        blocked_side = self.inventory_blocked_side()

        if blocked_side == side:
            return False, "inventory_block"

        if self.settings.suppress_same_side_quotes_during_reentry_cooldown and self.side_same_side_reentry_cooldown_active(side) and not reducing_inventory:
            return False, "same_side_reentry_cooldown"

        if self.side_queue_abandonment_cooldown_active(side):
            return False, "queue_abandonment_cooldown"

        if self.side_liquidity_pull_cooldown_active(side) and not reducing_inventory:
            return False, "side_pull_cooldown"

        if self.market_queue_cooldown_active() and not reducing_inventory:
            return False, "market_queue_cooldown"

        if self.market_toxicity_cooldown_active() and not reducing_inventory:
            return False, "market_toxicity_cooldown"

        if not reducing_inventory:
            if best_bid_units < minimum_bid_units:
                return False, "best_bid_floor"
            if implied_ask_units < minimum_ask_units:
                return False, "implied_ask_floor"
            if self.settings.minimum_top_level_depth_contracts > 0:
                if self.best_bid_size_units(side) < contracts_to_count_units(self.settings.minimum_top_level_depth_contracts):
                    return False, "thin_top_level"
            if self.settings.maximum_top_level_gap_cents > 0:
                second = self.second_best_bid(side)
                if second is None or (best_bid_units - second) > cents_to_price_units(self.settings.maximum_top_level_gap_cents):
                    return False, "wide_book_gap"
            if market_guard_active:
                return False, "market_probability_guard"
        else:
            if implied_ask_units <= self.market.price_grid.minimum_valid_price_units:
                return False, "no_exit_liquidity"

        return True, "ok"

    def quote_allowed_for_side(
        self,
        *,
        side: str,
        best_bid_units: int,
        implied_ask_units: int,
        market_guard_active: bool = False,
    ) -> bool:
        allowed, _ = self.quote_gate_status(
            side=side,
            best_bid_units=best_bid_units,
            implied_ask_units=implied_ask_units,
            market_guard_active=market_guard_active,
        )
        return allowed

    def cap_bid_below_implied_ask(self, implied_ask_units: int) -> Optional[int]:
        if self.settings.enforce_one_tick_safety_below_implied_ask:
            return self.market.price_grid.previous_price(implied_ask_units)
        return self.market.price_grid.floor_to_valid(implied_ask_units)

    def choose_target_from_other_best(
        self,
        *,
        other_best_units: int,
        cap_price_units: int,
        allow_aggressive_improvement: bool,
    ) -> int:
        aggressive_ticks = (
            self.settings.aggressive_improvement_ticks_when_spread_is_wide
            if allow_aggressive_improvement
            else 0
        )

        if aggressive_ticks > 0:
            raw_target_units = self.market.price_grid.move_price_by_ticks(other_best_units, aggressive_ticks)
        elif self.settings.passive_offset_ticks_when_not_improving > 0:
            raw_target_units = self.market.price_grid.move_price_by_ticks(
                other_best_units,
                -self.settings.passive_offset_ticks_when_not_improving,
            )
        else:
            raw_target_units = other_best_units

        raw_target_units = min(raw_target_units, cap_price_units)
        valid_target_units = self.market.price_grid.floor_to_valid(raw_target_units)
        if valid_target_units is None:
            raise ValueError(f"Could not snap target price to valid grid: {raw_target_units}")
        return valid_target_units

    def clamp_pair_to_avoid_self_cross(self, yes_bid_units: int, no_bid_units: int) -> Tuple[int, int]:
        maximum_safe_no_units = self.market.price_grid.max_safe_partner_bid(yes_bid_units)
        if maximum_safe_no_units is not None and no_bid_units > maximum_safe_no_units:
            log_event(
                "SELF_CROSS_CLAMP",
                lowered_side="no",
                old_no=format_price_dollars(no_bid_units),
                new_no=format_price_dollars(maximum_safe_no_units),
                yes=format_price_dollars(yes_bid_units),
            )
            no_bid_units = maximum_safe_no_units

        maximum_safe_yes_units = self.market.price_grid.max_safe_partner_bid(no_bid_units)
        if maximum_safe_yes_units is not None and yes_bid_units > maximum_safe_yes_units:
            log_event(
                "SELF_CROSS_CLAMP",
                lowered_side="yes",
                old_yes=format_price_dollars(yes_bid_units),
                new_yes=format_price_dollars(maximum_safe_yes_units),
                no=format_price_dollars(no_bid_units),
            )
            yes_bid_units = maximum_safe_yes_units

        return yes_bid_units, no_bid_units

    def combined_bid_limit_units(self) -> int:
        hard_limit_units = cents_to_price_units(self.settings.maximum_combined_bid_cents)
        profit_buffer_units = cents_to_price_units(self.settings.additional_profit_buffer_cents)
        minimum_step_units = self.market.price_grid.minimum_step_units
        theoretical_limit_units = ONE_DOLLAR_PRICE_UNITS - minimum_step_units - profit_buffer_units
        return max(0, min(hard_limit_units, theoretical_limit_units))

    def apply_pair_guard(self, yes_bid_units: int, no_bid_units: int) -> Tuple[int, int]:
        if not self.settings.enable_pair_guard:
            return yes_bid_units, no_bid_units

        maximum_combined_units = self.combined_bid_limit_units()
        if yes_bid_units + no_bid_units <= maximum_combined_units:
            return yes_bid_units, no_bid_units

        priority = self.settings.pair_guard_priority.lower()

        def lower_one_tick(price_units: int) -> int:
            previous_price_units = self.market.price_grid.previous_price(price_units)
            return previous_price_units if previous_price_units is not None else price_units

        max_iterations = 10_000
        iteration = 0

        while yes_bid_units + no_bid_units > maximum_combined_units and iteration < max_iterations:
            iteration += 1

            if priority == "yes":
                lowered_no_units = lower_one_tick(no_bid_units)
                if lowered_no_units == no_bid_units:
                    lowered_yes_units = lower_one_tick(yes_bid_units)
                    if lowered_yes_units == yes_bid_units:
                        break
                    yes_bid_units = lowered_yes_units
                else:
                    no_bid_units = lowered_no_units
            elif priority == "no":
                lowered_yes_units = lower_one_tick(yes_bid_units)
                if lowered_yes_units == yes_bid_units:
                    lowered_no_units = lower_one_tick(no_bid_units)
                    if lowered_no_units == no_bid_units:
                        break
                    no_bid_units = lowered_no_units
                else:
                    yes_bid_units = lowered_yes_units
            else:
                if yes_bid_units >= no_bid_units:
                    lowered_yes_units = lower_one_tick(yes_bid_units)
                    if lowered_yes_units == yes_bid_units:
                        lowered_no_units = lower_one_tick(no_bid_units)
                        if lowered_no_units == no_bid_units:
                            break
                        no_bid_units = lowered_no_units
                    else:
                        yes_bid_units = lowered_yes_units
                else:
                    lowered_no_units = lower_one_tick(no_bid_units)
                    if lowered_no_units == no_bid_units:
                        lowered_yes_units = lower_one_tick(yes_bid_units)
                        if lowered_yes_units == yes_bid_units:
                            break
                        yes_bid_units = lowered_yes_units
                    else:
                        no_bid_units = lowered_no_units

        return yes_bid_units, no_bid_units

    def shift_for_inventory_skew(self, side: str, base_price_units: int, skew_ticks: int) -> int:
        if skew_ticks == 0:
            return base_price_units
        if side == "yes":
            return self.market.price_grid.move_price_by_ticks(base_price_units, -skew_ticks)
        return self.market.price_grid.move_price_by_ticks(base_price_units, +skew_ticks)

    def maybe_market_probability_guard_triggered(self, *, best_yes_bid_units: int, best_no_bid_units: int) -> bool:
        threshold_cents = self.settings.minimum_market_best_bid_cents_required_to_quote_any_side
        if threshold_cents <= 0:
            guard_active = False
        else:
            threshold_units = cents_to_price_units(threshold_cents)
            guard_active = best_yes_bid_units < threshold_units or best_no_bid_units < threshold_units

        if guard_active != self.last_market_probability_guard_state:
            log_event(
                "MARKET_PROBABILITY_GUARD",
                active=guard_active,
                best_yes_bid_cents=best_yes_bid_units // PRICE_UNITS_PER_CENT,
                best_no_bid_cents=best_no_bid_units // PRICE_UNITS_PER_CENT,
                threshold_cents=threshold_cents,
            )
            self.last_market_probability_guard_state = guard_active

        return guard_active

    def maybe_log_market_queue_cooldown(self) -> None:
        active = self.market_queue_cooldown_active()
        if active != self.last_market_queue_cooldown_logged_state:
            log_event(
                "MARKET_QUEUE_COOLDOWN",
                active=active,
                cooldown_remaining_ms=max(0, self.market_wide_queue_cooldown_until_ms - now_ms()),
            )
            self.last_market_queue_cooldown_logged_state = active

    def final_quote_meets_side_floor(self, side: str, desired_price_units: Optional[int]) -> bool:
        if desired_price_units is None:
            return False
        if self.side_reduces_inventory_risk(side):
            return desired_price_units >= self.market.price_grid.minimum_valid_price_units
        minimum_bid_units = cents_to_price_units(self.settings.minimum_best_bid_cents_required_to_quote)
        return desired_price_units >= minimum_bid_units

    def should_skip_small_upward_reprice(self, current_price_units: int, target_price_units: int) -> bool:
        if target_price_units <= current_price_units:
            return False
        minimum_ticks = self.settings.minimum_upward_reprice_ticks_required
        if minimum_ticks <= 0:
            return False
        tick_distance = self.market.price_grid.tick_distance(current_price_units, target_price_units)
        return tick_distance < minimum_ticks

    def compute_side_desired_quote_price(
        self,
        *,
        side: str,
        side_quote_allowed: bool,
        maximum_bid_units: int,
        other_best_units: Optional[int],
        allow_aggressive_improvement: bool,
    ) -> Optional[int]:
        if not side_quote_allowed:
            return None

        current_price_units = self.orders[side].price_units
        live_best_bid_units = self.best_bid(side)
        if live_best_bid_units is None:
            return None

        if current_price_units is not None and current_price_units > maximum_bid_units:
            return maximum_bid_units

        if other_best_units is None:
            if current_price_units is not None:
                return min(current_price_units, maximum_bid_units)
            return min(live_best_bid_units, maximum_bid_units)

        target_units = self.choose_target_from_other_best(
            other_best_units=other_best_units,
            cap_price_units=maximum_bid_units,
            allow_aggressive_improvement=allow_aggressive_improvement,
        )

        if current_price_units is None:
            if self.settings.join_current_best_bid_when_starting_new_quote_cycle:
                return min(live_best_bid_units, maximum_bid_units)
            return target_units

        if current_price_units > maximum_bid_units:
            return maximum_bid_units

        if target_units < current_price_units:
            return target_units
        if target_units > current_price_units and self.should_skip_small_upward_reprice(current_price_units, target_units):
            return current_price_units
        return target_units

    def estimate_inventory_penalty_units(self, *, side: str, context: MarketContext) -> int:
        if self.side_reduces_inventory_risk(side):
            return 0
        inventory_contracts = abs(context.inventory_units) / COUNT_SCALE
        if inventory_contracts <= 0:
            return 0
        penalty_ticks = min(
            float(self.settings.maximum_inventory_skew_ticks),
            inventory_contracts / max(1.0, float(self.settings.inventory_skew_contracts_per_tick)),
        )
        return int(round(penalty_ticks * self.market.price_grid.minimum_step_units))

    def estimate_queue_penalty_units(
        self,
        *,
        side: str,
        context: MarketContext,
        queue_ahead_units: Optional[int] = None,
    ) -> int:
        queue_units = context.queue_ahead_units_for_side(side) if queue_ahead_units is None else queue_ahead_units
        if queue_units is None or queue_units <= 0:
            return 0
        queue_contracts = queue_units / COUNT_SCALE
        queue_penalty_cents = min(3.0, queue_contracts / 50.0)
        return int(round(queue_penalty_cents * PRICE_UNITS_PER_CENT))

    def queue_abandonment_threshold_for_size_units(self, target_size_units: int) -> int:
        candidate_thresholds: List[int] = []
        absolute_limit_units = contracts_to_count_units(self.settings.maximum_queue_ahead_contracts_before_abandonment)
        if absolute_limit_units > 0:
            candidate_thresholds.append(int(absolute_limit_units))
        if target_size_units > 0 and self.settings.maximum_queue_ahead_multiple_of_our_remaining_size > 0:
            relative_limit_units = int(math.ceil(target_size_units * self.settings.maximum_queue_ahead_multiple_of_our_remaining_size))
            if relative_limit_units > 0:
                candidate_thresholds.append(relative_limit_units)
        if not candidate_thresholds:
            return 0
        return min(candidate_thresholds)

    def should_block_projected_queue_entry(
        self,
        *,
        projected_queue_ahead_units: Optional[int],
        target_size_units: int,
        reducing_inventory: bool,
    ) -> bool:
        if reducing_inventory:
            return False
        if projected_queue_ahead_units is None or projected_queue_ahead_units <= 0:
            return False
        threshold_units = self.queue_abandonment_threshold_for_size_units(target_size_units)
        if threshold_units <= 0:
            return False
        return projected_queue_ahead_units > threshold_units

    def ev_adjusted_order_size_units(
        self,
        *,
        side: str,
        price_units: int,
        expected_edge_units: int,
        queue_penalty_units: int,
        reducing_inventory: bool,
    ) -> int:
        if price_units <= 0:
            return 0

        base_budget_cents = self.target_budget_cents_for_side(side)
        if base_budget_cents <= 0:
            return 0

        minimum_fraction = self.settings.quote_size_min_fraction_of_budget
        maximum_fraction = self.settings.quote_size_max_fraction_of_budget
        edge_cents = expected_edge_units / PRICE_UNITS_PER_CENT

        if reducing_inventory and edge_cents < 0:
            negative_tolerance = max(1.0, float(self.settings.inventory_reduction_max_negative_edge_cents))
            fraction = minimum_fraction + (1.0 - minimum_fraction) * max(0.0, 1.0 - min(1.0, abs(edge_cents) / negative_tolerance))
        else:
            strength = max(0.0, min(1.0, edge_cents / max(1.0, float(self.settings.strong_edge_threshold_cents))))
            fraction = minimum_fraction + (maximum_fraction - minimum_fraction) * strength
            if expected_edge_units <= 0 and not reducing_inventory:
                fraction = 0.0

        if queue_penalty_units > 0:
            queue_penalty_scale = max(0.35, 1.0 - (queue_penalty_units / max(PRICE_UNITS_PER_CENT, cents_to_price_units(5))))
            fraction *= queue_penalty_scale

        dynamic_budget_cents = int(round(base_budget_cents * fraction))
        if dynamic_budget_cents <= 0:
            return 0

        budget_money_units = dynamic_budget_cents * PRICE_UNITS_PER_CENT
        quantity_units = (budget_money_units * COUNT_SCALE) // price_units
        maximum_count_units = contracts_to_count_units(self.settings.maximum_contracts_per_order)
        quantity_units = min(quantity_units, maximum_count_units)
        if quantity_units <= 0:
            return 0

        if self.market.fractional_trading_enabled and self.settings.allow_fractional_order_entry_when_supported:
            return int(quantity_units)
        return int((quantity_units // COUNT_SCALE) * COUNT_SCALE)

    def build_side_quote_decision(
        self,
        *,
        side: str,
        context: MarketContext,
        fair_value: FairValueEstimate,
        market_target_units: Optional[int],
        maximum_bid_units: int,
        side_quote_allowed: bool,
        gate_reason: str = "ok",
    ) -> SideQuoteDecision:
        fair_side_units = fair_value.fair_for_side(side)
        reducing_inventory = self.side_reduces_inventory_risk(side)
        inventory_penalty_units = self.estimate_inventory_penalty_units(side=side, context=context)
        pull_toxicity_penalty_units = int(round(
            self.current_liquidity_pull_score(side) * cents_to_price_units(self.settings.orderbook_pull_penalty_cents)
        ))
        expected_markout_units = (
            self.toxicity_model.estimate_markout_per_contract_units(side=side, context=context)
            + pull_toxicity_penalty_units
        )
        current_queue_units = context.queue_ahead_units_for_side(side)
        current_queue_penalty_units = self.estimate_queue_penalty_units(
            side=side,
            context=context,
            queue_ahead_units=current_queue_units,
        )
        minimum_edge_units = cents_to_price_units(self.settings.minimum_expected_edge_cents_to_quote)
        minimum_inventory_edge_units = -cents_to_price_units(self.settings.inventory_reduction_max_negative_edge_cents)
        candidate_debug: List[str] = []

        if not side_quote_allowed or market_target_units is None:
            mode = ("inventory_hold" if reducing_inventory else "disabled")
            return SideQuoteDecision(
                side=side,
                price_units=None,
                target_size_units=0,
                mode=mode,
                fair_yes_units=fair_value.fair_yes_units,
                fair_side_units=fair_side_units,
                projected_queue_ahead_units=current_queue_units,
                estimated_toxicity_units=expected_markout_units,
                estimated_markout_units=expected_markout_units,
                inventory_penalty_units=inventory_penalty_units,
                queue_penalty_units=current_queue_penalty_units,
                reason=(gate_reason or mode),
                candidate_debug="",
                bucket_key=self.toxicity_model.bucket_key(side=side, context=context),
            )

        provisional_size_units = max(
            COUNT_SCALE,
            self.budget_based_order_size_units(side, max(PRICE_UNITS_PER_CENT, market_target_units)),
        )
        live_best_bid_units = self.best_bid(side) or market_target_units

        initial_tick_distance_from_best = (
            max(0, self.market.price_grid.tick_distance(market_target_units, live_best_bid_units))
            if market_target_units < live_best_bid_units
            else 0
        )
        rough_incentive_units = self.incentive_model.estimate_incentive_per_contract_units(
            quote_price_units=market_target_units,
            size_units=provisional_size_units,
            tick_distance_from_best=initial_tick_distance_from_best,
        )
        rough_fee_units = self.fee_model.estimate_fee_per_contract_units(price_units=market_target_units)
        reservation_units = (
            fair_side_units
            - rough_fee_units
            - expected_markout_units
            - inventory_penalty_units
            - current_queue_penalty_units
            + rough_incentive_units
            - minimum_edge_units
        )
        reservation_units = min(maximum_bid_units, reservation_units)
        reservation_units = max(self.market.price_grid.minimum_valid_price_units, reservation_units)
        reservation_units = self.market.price_grid.floor_to_valid(reservation_units) or reservation_units

        best_candidate: Optional[dict] = None
        candidate_price_units = market_target_units
        skipped_for_queue = False

        for _ in range(max(1, int(self.settings.candidate_price_levels_to_scan))):
            if candidate_price_units is None or candidate_price_units > maximum_bid_units:
                break

            projected_queue_units = self.projected_queue_ahead_units_for_price(side, candidate_price_units)
            tick_distance_from_best = (
                max(0, self.market.price_grid.tick_distance(candidate_price_units, live_best_bid_units))
                if candidate_price_units < live_best_bid_units
                else 0
            )
            incentive_units = self.incentive_model.estimate_incentive_per_contract_units(
                quote_price_units=candidate_price_units,
                size_units=provisional_size_units,
                tick_distance_from_best=tick_distance_from_best,
            )
            fee_units = self.fee_model.estimate_fee_per_contract_units(price_units=candidate_price_units)
            queue_penalty_units = self.estimate_queue_penalty_units(
                side=side,
                context=context,
                queue_ahead_units=projected_queue_units,
            )

            bucket_key = self.toxicity_model.bucket_key(side=side, context=context)
            fill_probability_30s = self.fill_probability_model.estimate_fill_probability_30s(
                bucket_key=bucket_key
            )

            expected_edge_units = (
                fair_side_units
                - candidate_price_units
                - fee_units
                - expected_markout_units
                - inventory_penalty_units
                - queue_penalty_units
                + incentive_units
            )

            score_per_hour_units = (
                float(fill_probability_30s)
                * float(expected_edge_units)
                * (3600.0 / max(1, int(self.settings.fill_probability_horizon_seconds)))
            )

            current_state = self.orders[side]
            keep_edge_units = cents_to_price_units(self.settings.minimum_expected_edge_cents_to_keep_quote)

            is_keep_candidate = (
                current_state.has_active_resting_order
                and current_state.price_units == candidate_price_units
            )

            required_edge_units = (
                minimum_inventory_edge_units
                if reducing_inventory
                else (keep_edge_units if is_keep_candidate else minimum_edge_units)
            )

            edge_allowed = expected_edge_units >= required_edge_units

            candidate_target_size_units = 0
            candidate_status = "edge_fail"

            if edge_allowed:
                candidate_target_size_units = self.ev_adjusted_order_size_units(
                    side=side,
                    price_units=candidate_price_units,
                    expected_edge_units=expected_edge_units,
                    queue_penalty_units=queue_penalty_units,
                    reducing_inventory=reducing_inventory,
                )
                if candidate_target_size_units <= 0:
                    candidate_status = "size_zero"
                elif self.should_block_projected_queue_entry(
                    projected_queue_ahead_units=projected_queue_units,
                    target_size_units=candidate_target_size_units,
                    reducing_inventory=reducing_inventory,
                ):
                    skipped_for_queue = True
                    candidate_status = "queue_block"
                else:
                    candidate_status = "ok"

            candidate_debug.append(
                "p={:.2f}|edge={:.2f}|pfill30={:.3f}|mkout={:.2f}|fee={:.2f}|qpen={:.2f}|score_hr={:.2f}|queue={:.2f}|size={:.2f}|status={}".format(
                    candidate_price_units / PRICE_UNITS_PER_CENT,
                    expected_edge_units / PRICE_UNITS_PER_CENT,
                    fill_probability_30s,
                    expected_markout_units / PRICE_UNITS_PER_CENT,
                    fee_units / PRICE_UNITS_PER_CENT,
                    queue_penalty_units / PRICE_UNITS_PER_CENT,
                    score_per_hour_units / PRICE_UNITS_PER_CENT,
                    0.0 if projected_queue_units is None else projected_queue_units / COUNT_SCALE,
                    candidate_target_size_units / COUNT_SCALE,
                    candidate_status,
                )
            )

            if not edge_allowed:
                candidate_price_units = self.market.price_grid.next_price(candidate_price_units)
                continue

            if candidate_target_size_units <= 0:
                candidate_price_units = self.market.price_grid.next_price(candidate_price_units)
                continue

            if candidate_status == "queue_block":
                candidate_price_units = self.market.price_grid.next_price(candidate_price_units)
                continue

            candidate = {
                "price_units": candidate_price_units,
                "target_size_units": candidate_target_size_units,
                "projected_queue_units": projected_queue_units,
                "fee_units": fee_units,
                "queue_penalty_units": queue_penalty_units,
                "incentive_units": incentive_units,
                "expected_edge_units": expected_edge_units,
                "expected_markout_units": expected_markout_units,
                "fill_probability_30s": fill_probability_30s,
                "score_per_hour_units": score_per_hour_units,
                "bucket_key": bucket_key,
            }
            if best_candidate is None or candidate["score_per_hour_units"] > best_candidate["score_per_hour_units"]:
                best_candidate = candidate

            candidate_price_units = self.market.price_grid.next_price(candidate_price_units)

        if best_candidate is None:
            mode = "queue_too_deep" if skipped_for_queue and not reducing_inventory else ("inventory_wait" if reducing_inventory else "negative_ev")
            return SideQuoteDecision(
                side=side,
                price_units=None,
                target_size_units=0,
                mode=mode,
                fair_yes_units=fair_value.fair_yes_units,
                fair_side_units=fair_side_units,
                reservation_units=reservation_units,
                market_target_units=market_target_units,
                projected_queue_ahead_units=current_queue_units,
                estimated_fee_units=rough_fee_units,
                estimated_toxicity_units=expected_markout_units,
                estimated_incentive_units=rough_incentive_units,
                estimated_markout_units=expected_markout_units,
                inventory_penalty_units=inventory_penalty_units,
                queue_penalty_units=current_queue_penalty_units,
                reason=mode,
                candidate_debug="; ".join(candidate_debug),
                bucket_key=self.toxicity_model.bucket_key(side=side, context=context),
            )

        mode = "inventory_reduction" if reducing_inventory and best_candidate["expected_edge_units"] < minimum_edge_units else "ev_quote"
        return SideQuoteDecision(
            side=side,
            price_units=best_candidate["price_units"],
            target_size_units=best_candidate["target_size_units"],
            mode=mode,
            fair_yes_units=fair_value.fair_yes_units,
            fair_side_units=fair_side_units,
            reservation_units=reservation_units,
            market_target_units=market_target_units,
            projected_queue_ahead_units=best_candidate["projected_queue_units"],
            expected_edge_units=best_candidate["expected_edge_units"],
            estimated_fee_units=best_candidate["fee_units"],
            estimated_toxicity_units=expected_markout_units,
            estimated_incentive_units=best_candidate["incentive_units"],
            estimated_fill_probability_30s=best_candidate["fill_probability_30s"],
            estimated_markout_units=best_candidate["expected_markout_units"],
            score_per_hour_units=best_candidate["score_per_hour_units"],
            bucket_key=best_candidate["bucket_key"],
            inventory_penalty_units=inventory_penalty_units,
            queue_penalty_units=best_candidate["queue_penalty_units"],
            reason=mode,
            candidate_debug="; ".join(candidate_debug),
        )

    def compute_quote_decisions(self) -> Dict[str, SideQuoteDecision]:
        context = self.build_market_context()
        if context is None:
            return {
                "yes": SideQuoteDecision(side="yes", price_units=None, target_size_units=0, mode="no_book"),
                "no": SideQuoteDecision(side="no", price_units=None, target_size_units=0, mode="no_book"),
            }

        self.maybe_record_market_state(source="orderbook", context=context)
        market_guard_active = self.maybe_market_probability_guard_triggered(
            best_yes_bid_units=context.best_yes_bid_units,
            best_no_bid_units=context.best_no_bid_units,
        )
        self.maybe_log_market_queue_cooldown()

        maximum_yes_bid_units = self.cap_bid_below_implied_ask(context.implied_yes_ask_units)
        maximum_no_bid_units = self.cap_bid_below_implied_ask(context.implied_no_ask_units)
        if maximum_yes_bid_units is None or maximum_no_bid_units is None:
            return {
                "yes": SideQuoteDecision(side="yes", price_units=None, target_size_units=0, mode="no_valid_cap"),
                "no": SideQuoteDecision(side="no", price_units=None, target_size_units=0, mode="no_valid_cap"),
            }

        yes_quote_allowed, yes_gate_reason = self.quote_gate_status(
            side="yes",
            best_bid_units=context.best_yes_bid_units,
            implied_ask_units=context.implied_yes_ask_units,
            market_guard_active=market_guard_active,
        )
        no_quote_allowed, no_gate_reason = self.quote_gate_status(
            side="no",
            best_bid_units=context.best_no_bid_units,
            implied_ask_units=context.implied_no_ask_units,
            market_guard_active=market_guard_active,
        )

        allow_aggressive_improvement = (
            self.settings.aggressive_improvement_ticks_when_spread_is_wide > 0
            and not self.post_fill_improvement_cooldown_active()
        )
        if allow_aggressive_improvement:
            tick_counter = 0
            candidate_price_units = context.best_yes_bid_units
            while True:
                candidate_price_units = self.market.price_grid.next_price(candidate_price_units) if candidate_price_units is not None else None
                if candidate_price_units is None or candidate_price_units > maximum_yes_bid_units:
                    break
                tick_counter += 1
                if tick_counter >= self.settings.minimum_spread_ticks_required_for_aggressive_improvement:
                    break
            allow_aggressive_improvement = tick_counter >= self.settings.minimum_spread_ticks_required_for_aggressive_improvement

        signal_context = self.build_signal_market_context(context)
        fair_value = self.fair_value_engine.estimate(signal_context)
        yes_market_target_units = self.compute_side_desired_quote_price(
            side="yes",
            side_quote_allowed=yes_quote_allowed,
            maximum_bid_units=maximum_yes_bid_units,
            other_best_units=self.best_bid_excluding_our_order("yes"),
            allow_aggressive_improvement=allow_aggressive_improvement,
        )
        no_market_target_units = self.compute_side_desired_quote_price(
            side="no",
            side_quote_allowed=no_quote_allowed,
            maximum_bid_units=maximum_no_bid_units,
            other_best_units=self.best_bid_excluding_our_order("no"),
            allow_aggressive_improvement=allow_aggressive_improvement,
        )

        decisions = {
            "yes": self.build_side_quote_decision(
                side="yes",
                context=context,
                fair_value=fair_value,
                market_target_units=yes_market_target_units,
                maximum_bid_units=maximum_yes_bid_units,
                side_quote_allowed=yes_quote_allowed,
                gate_reason=yes_gate_reason,
            ),
            "no": self.build_side_quote_decision(
                side="no",
                context=context,
                fair_value=fair_value,
                market_target_units=no_market_target_units,
                maximum_bid_units=maximum_no_bid_units,
                side_quote_allowed=no_quote_allowed,
                gate_reason=no_gate_reason,
            ),
        }

        desired_yes_units = decisions["yes"].price_units
        desired_no_units = decisions["no"].price_units
        if desired_yes_units is not None and desired_no_units is not None:
            clamped_yes_units, clamped_no_units = self.clamp_pair_to_avoid_self_cross(desired_yes_units, desired_no_units)
            clamped_yes_units, clamped_no_units = self.apply_pair_guard(clamped_yes_units, clamped_no_units)
            decisions["yes"].price_units = clamped_yes_units
            decisions["no"].price_units = clamped_no_units

        for side in ("yes", "no"):
            decision = decisions[side]
            original_price_units = decision.price_units

            if original_price_units is not None and not self.final_quote_meets_side_floor(side, original_price_units):
                decision.price_units = None
                decision.target_size_units = 0
                decision.mode = "below_floor"
                decision.reason = "below_floor"
            elif not decision.reason:
                decision.reason = decision.mode

            self.telemetry_store.record_quote_decision(
                ts_ms=context.ts_ms,
                ticker=self.market.ticker,
                decision=decision,
                context=context,
            )
            self.log_quote_decision_transition(side, decision, context)

        for side in ("yes", "no"):
            decision = decisions[side]
            bucket_key = self.toxicity_model.bucket_key(side=side, context=context)
            self.fill_probability_model.observe_quote_decision(
                ticker=self.market.ticker,
                side=side,
                decision=decision,
                bucket_key=bucket_key,
                ts_ms=context.ts_ms,
            )

        self.last_quote_decisions = decisions
        return decisions

    def desired_quote_prices(self) -> Tuple[Optional[int], Optional[int]]:
        decisions = self.compute_quote_decisions()
        return decisions["yes"].price_units, decisions["no"].price_units

    # -------------------------
    # Sizing
    # -------------------------

    def target_budget_cents_for_side(self, side: str) -> int:
        return self.settings.yes_order_budget_cents if side == "yes" else self.settings.no_order_budget_cents

    def budget_based_order_size_units(self, side: str, price_units: int) -> int:
        budget_cents = max(0, self.target_budget_cents_for_side(side))
        if budget_cents <= 0 or price_units <= 0:
            return 0

        budget_money_units = budget_cents * PRICE_UNITS_PER_CENT
        quantity_units = (budget_money_units * COUNT_SCALE) // price_units
        maximum_count_units = contracts_to_count_units(self.settings.maximum_contracts_per_order)
        quantity_units = min(quantity_units, maximum_count_units)
        if quantity_units <= 0:
            return 0

        if self.market.fractional_trading_enabled and self.settings.allow_fractional_order_entry_when_supported:
            return int(quantity_units)

        quantity_units = (quantity_units // COUNT_SCALE) * COUNT_SCALE
        return int(quantity_units)

    def desired_cycle_total_fillable_units(self, side: str, price_units: int, quote_cycle_filled_units: int) -> int:
        decision = self.last_quote_decisions.get(side)
        if decision is not None and decision.price_units == price_units:
            target_size_units = int(decision.target_size_units)
        else:
            target_size_units = self.budget_based_order_size_units(side, price_units)
        if target_size_units <= 0:
            return 0

        if self.settings.refill_resting_size_after_partial_fill:
            return quote_cycle_filled_units + target_size_units

        return max(quote_cycle_filled_units, target_size_units)

    def desired_remaining_units(self, side: str, price_units: int, quote_cycle_filled_units: int) -> int:
        desired_cycle_total_units = self.desired_cycle_total_fillable_units(side, price_units, quote_cycle_filled_units)
        return max(0, desired_cycle_total_units - quote_cycle_filled_units)

    # -------------------------
    # Quote lifecycle
    # -------------------------

    def order_needs_expiration_refresh(self, state: ManagedOrderState) -> bool:
        if self.settings.resting_order_expiration_seconds <= 0:
            return False
        if state.expiration_time_ms is None:
            return False
        refresh_threshold_ms = self.settings.expiration_refresh_lead_seconds * 1000
        return state.expiration_time_ms - now_ms() <= refresh_threshold_ms

    def expiration_timestamp_for_side(self, side: str) -> Optional[int]:
        if self.settings.resting_order_expiration_seconds <= 0:
            return None
        total_seconds = self.settings.resting_order_expiration_seconds
        total_seconds += self.instance_expiration_jitter_seconds
        if self.settings.stagger_yes_no_expiration_offsets_seconds > 0 and side == "no":
            total_seconds += self.settings.stagger_yes_no_expiration_offsets_seconds
        return utc_seconds_to_expiration_timestamp(total_seconds)

    async def cancel_side_quote(self, side: str, *, reason: str, reset_quote_cycle: bool) -> None:
        state = self.orders[side]
        if not state.has_active_resting_order:
            if reset_quote_cycle:
                state.quote_cycle_filled_units = 0
            return

        order_id = state.order_id
        if order_id is None:
            state.clear_active_order(preserve_quote_cycle=not reset_quote_cycle)
            return

        last_decision = self.last_quote_decisions.get(side)
        log_event(
            "CANCEL_DIAG",
            side=side,
            reason=reason,
            order_id=order_id,
            active_price_c=("none" if state.price_units is None else f"{state.price_units / PRICE_UNITS_PER_CENT:.2f}"),
            active_remaining=format_count_fp(state.remaining_count_units),
            last_mode=(last_decision.mode if last_decision else "none"),
            last_reason=(last_decision.reason if last_decision else "none"),
            last_edge_c=("none" if last_decision is None else f"{last_decision.expected_edge_units / PRICE_UNITS_PER_CENT:.2f}"),
            last_fair_side_c=("none" if last_decision is None or last_decision.fair_side_units is None else f"{last_decision.fair_side_units / PRICE_UNITS_PER_CENT:.2f}"),
        )

        try:
            await asyncio.to_thread(self.api_client.cancel_order, order_id=order_id)
            log_event("CANCEL_REQUESTED", side=side, reason=reason, order_id=order_id)
        except Exception as exc:
            if is_order_not_found_error(exc):
                log_event("CANCEL_SKIPPED_ORDER_MISSING", side=side, reason=reason, order_id=order_id)
            elif is_rate_limit_error(exc):
                log_event("CANCEL_RATE_LIMITED", side=side, reason=reason, order_id=order_id, error=str(exc))
                raise
            else:
                raise

        state.clear_active_order(preserve_quote_cycle=not reset_quote_cycle)

    async def ensure_side_quote(self, side: str, desired_price_units: Optional[int]) -> None:
        state = self.orders[side]

        if desired_price_units is None:
            last_decision = self.last_quote_decisions.get(side)
            cancel_reason = "quote_disabled"
            if last_decision is not None:
                cancel_reason = last_decision.reason or last_decision.mode or cancel_reason

            await self.cancel_side_quote(side, reason=cancel_reason, reset_quote_cycle=True)
            return

        desired_remaining_units = self.desired_remaining_units(side, desired_price_units, state.quote_cycle_filled_units)
        if desired_remaining_units <= 0:
            await self.cancel_side_quote(side, reason="target_size_zero", reset_quote_cycle=True)
            return

        if state.has_active_resting_order and self.order_needs_expiration_refresh(state):
            await self.cancel_side_quote(side, reason="refresh_expiring_quote", reset_quote_cycle=False)

        attempt_price_units = desired_price_units

        for attempt_index in range(self.settings.maximum_post_only_reprice_attempts):
            state = self.orders[side]
            desired_remaining_units = self.desired_remaining_units(side, attempt_price_units, state.quote_cycle_filled_units)
            if desired_remaining_units <= 0:
                await self.cancel_side_quote(side, reason="target_size_zero", reset_quote_cycle=True)
                return

            desired_current_order_total_fillable_units = state.current_order_filled_units + desired_remaining_units
            current_order_total_fillable_units = state.current_order_total_fillable_units

            if state.has_active_resting_order:
                price_unchanged = state.price_units == attempt_price_units
                size_unchanged = current_order_total_fillable_units == desired_current_order_total_fillable_units
                size_delta_units = abs(current_order_total_fillable_units - desired_current_order_total_fillable_units)
                if price_unchanged and size_unchanged and not self.order_needs_expiration_refresh(state):
                    return
                if (
                    price_unchanged
                    and size_delta_units <= COUNT_SCALE
                    and not self.order_needs_expiration_refresh(state)
                ):
                    return

            try:
                if not state.has_active_resting_order:
                    new_client_order_id = f"{self.settings.primary_client_order_prefix}:{side}:{uuid.uuid4().hex[:16]}"
                    expiration_ts = self.expiration_timestamp_for_side(side)
                    response = await asyncio.to_thread(
                        self.api_client.create_order,
                        market_ticker=self.settings.market_ticker,
                        side=side,
                        price_units=attempt_price_units,
                        count_units=desired_remaining_units,
                        client_order_id=new_client_order_id,
                        expiration_timestamp_seconds=expiration_ts,
                    )
                    order_payload = (response or {}).get("order") or {}
                    state.order_id = order_payload.get("order_id") or (response or {}).get("order_id")
                    if state.order_id:
                        self.known_strategy_order_sides[str(state.order_id)] = side
                    state.client_order_id = order_payload.get("client_order_id") or new_client_order_id
                    state.price_units = attempt_price_units
                    state.remaining_count_units = desired_remaining_units
                    state.current_order_filled_units = 0
                    state.status = "resting"
                    state.last_queue_position_units = self.displayed_size_units_at_price(side, attempt_price_units)
                    state.expiration_time_ms = parse_iso_utc_timestamp_to_ms(order_payload.get("expiration_time"))
                    log_event(
                        "CREATE_OK",
                        side=side,
                        price_dollars=format_price_dollars(attempt_price_units),
                        remaining_contracts=format_count_fp(desired_remaining_units),
                        order_id=state.order_id,
                    )
                    return

                if not state.order_id or not state.client_order_id:
                    state.clear_active_order(preserve_quote_cycle=False)
                    continue

                updated_client_order_id = f"{self.settings.primary_client_order_prefix}:{side}:{uuid.uuid4().hex[:16]}"
                response = await asyncio.to_thread(
                    self.api_client.amend_order,
                    order_id=state.order_id,
                    market_ticker=self.settings.market_ticker,
                    side=side,
                    new_price_units=attempt_price_units,
                    new_total_fillable_count_units=desired_current_order_total_fillable_units,
                    previous_client_order_id=state.client_order_id,
                    updated_client_order_id=updated_client_order_id,
                )
                amended_order_payload = (response or {}).get("order") or {}
                returned_order_id = amended_order_payload.get("order_id") or state.order_id
                if returned_order_id:
                    state.order_id = returned_order_id
                    self.known_strategy_order_sides[str(returned_order_id)] = side
                state.client_order_id = amended_order_payload.get("client_order_id") or updated_client_order_id
                price_changed = state.price_units != attempt_price_units
                state.price_units = attempt_price_units
                state.remaining_count_units = desired_remaining_units
                if price_changed or state.last_queue_position_units is None:
                    state.last_queue_position_units = self.displayed_size_units_at_price(side, attempt_price_units)
                state.expiration_time_ms = parse_iso_utc_timestamp_to_ms(amended_order_payload.get("expiration_time"))
                log_event(
                    "AMEND_OK",
                    side=side,
                    price_dollars=format_price_dollars(attempt_price_units),
                    total_fillable_contracts=format_count_fp(desired_current_order_total_fillable_units),
                    order_id=state.order_id,
                )
                return

            except Exception as exc:
                if is_missing_or_non_resting_amend_target(exc):
                    log_event("STALE_ORDER_RECOVER", side=side, order_id=state.order_id)
                    state.clear_active_order(preserve_quote_cycle=False)
                    continue

                if is_post_only_cross_error(exc):
                    next_lower_price_units = self.market.price_grid.previous_price(attempt_price_units)
                    log_event(
                        "POST_ONLY_RETRY",
                        side=side,
                        attempt=attempt_index + 1,
                        old_price_dollars=format_price_dollars(attempt_price_units),
                        next_price_dollars=(format_price_dollars(next_lower_price_units) if next_lower_price_units is not None else "none"),
                    )
                    if next_lower_price_units is None:
                        return
                    attempt_price_units = next_lower_price_units
                    if attempt_index + 1 >= self.settings.maximum_post_only_reprice_attempts:
                        log_event("POST_ONLY_COOLDOWN", side=side, seconds=self.settings.post_only_reprice_cooldown_seconds)
                        await asyncio.sleep(self.settings.post_only_reprice_cooldown_seconds)
                    continue

                raise

    # -------------------------
    # Queue management
    # -------------------------

    def queue_abandonment_threshold_units(self, side: str) -> int:
        state = self.orders[side]
        return self.queue_abandonment_threshold_for_size_units(state.remaining_count_units)

    async def process_queue_position_update(self, side: str, queue_position_units: Optional[int]) -> None:
        state = self.orders[side]
        if queue_position_units is None or not state.has_active_resting_order:
            state.consecutive_queue_ahead_breaches = 0
            return

        if not self.settings.enable_queue_abandonment_guard:
            return

        threshold_units = self.queue_abandonment_threshold_units(side)
        if queue_position_units > threshold_units:
            state.consecutive_queue_ahead_breaches += 1
            log_event(
                "QUEUE_AHEAD_BREACH",
                side=side,
                ahead_contracts=format_count_fp(queue_position_units),
                threshold_contracts=format_count_fp(threshold_units),
                consecutive_breaches=state.consecutive_queue_ahead_breaches,
            )
        else:
            if state.consecutive_queue_ahead_breaches > 0:
                log_event(
                    "QUEUE_AHEAD_RECOVERED",
                    side=side,
                    ahead_contracts=format_count_fp(queue_position_units),
                )
            state.consecutive_queue_ahead_breaches = 0
            return

        if state.consecutive_queue_ahead_breaches < self.settings.queue_abandonment_consecutive_polls_required:
            return

        cooldown_until_ms = now_ms() + self.settings.queue_abandonment_side_cooldown_seconds * 1000
        state.queue_abandonment_cooldown_until_ms = max(state.queue_abandonment_cooldown_until_ms, cooldown_until_ms)
        state.consecutive_queue_ahead_breaches = 0

        log_event(
            "QUEUE_ABANDON_SIDE",
            side=side,
            cooldown_seconds=self.settings.queue_abandonment_side_cooldown_seconds,
            ahead_contracts=format_count_fp(queue_position_units),
            threshold_contracts=format_count_fp(threshold_units),
        )

        opposite_side = "no" if side == "yes" else "yes"
        opposite_state = self.orders[opposite_side]
        if now_ms() < opposite_state.queue_abandonment_cooldown_until_ms:
            self.market_wide_queue_cooldown_until_ms = max(
                self.market_wide_queue_cooldown_until_ms,
                now_ms() + self.settings.queue_abandonment_market_cooldown_seconds * 1000,
            )
            log_event(
                "QUEUE_ABANDON_MARKET",
                cooldown_seconds=self.settings.queue_abandonment_market_cooldown_seconds,
            )

        async with self.requote_lock:
            try:
                await self.cancel_side_quote(side, reason="queue_abandonment", reset_quote_cycle=False)
            except Exception as exc:
                if is_rate_limit_error(exc):
                    log_event("QUEUE_ABANDON_CANCEL_RATE_LIMITED", side=side, error=str(exc))
                else:
                    raise
        self.requote_event.set()

    # -------------------------
    # WebSocket / event handling
    # -------------------------

    def apply_orderbook_snapshot(self, snapshot_message: dict) -> None:
        self.book_yes.clear()
        self.book_no.clear()

        fixed_yes_levels = snapshot_message.get("yes_dollars_fp")
        fixed_no_levels = snapshot_message.get("no_dollars_fp")

        if fixed_yes_levels is not None and fixed_no_levels is not None:
            for price_dollars, quantity_fp in fixed_yes_levels:
                self.book_yes[parse_price_units_from_dollars(price_dollars)] = parse_count_units_from_fp(quantity_fp)
            for price_dollars, quantity_fp in fixed_no_levels:
                self.book_no[parse_price_units_from_dollars(price_dollars)] = parse_count_units_from_fp(quantity_fp)
        else:
            for price_cents, quantity in snapshot_message.get("yes") or []:
                self.book_yes[parse_price_units_from_legacy_cents(price_cents)] = int(quantity) * COUNT_SCALE
            for price_cents, quantity in snapshot_message.get("no") or []:
                self.book_no[parse_price_units_from_legacy_cents(price_cents)] = int(quantity) * COUNT_SCALE

        self.book_ready = True

    def apply_orderbook_delta(self, delta_message: dict) -> None:
        side = str(delta_message.get("side"))
        if side not in {"yes", "no"}:
            return

        price_units = parse_optional_price_units(delta_message, "price_dollars", "price")
        delta_units = parse_optional_count_units(delta_message, "delta_fp", "delta")
        if price_units is None or delta_units is None:
            return

        self.observe_orderbook_toxicity_from_delta(delta_message)

        levels = self.book_yes if side == "yes" else self.book_no
        levels[price_units] = levels.get(price_units, 0) + delta_units
        if levels[price_units] <= 0:
            levels.pop(price_units, None)

    def handle_market_position_update(self, position_message: dict) -> None:
        market_ticker = position_message.get("market_ticker") or position_message.get("ticker")
        if market_ticker != self.settings.market_ticker:
            return

        position_units = parse_optional_count_units(position_message, "position_fp", "position")
        if position_units is None:
            return

        self.net_position_units = int(position_units)

    def handle_ticker_update(self, ticker_message: dict) -> None:
        market_ticker = str(ticker_message.get("market_ticker") or ticker_message.get("ticker") or "")
        if market_ticker != self.settings.market_ticker:
            return

        price_units = parse_optional_price_units(ticker_message, "price_dollars", "price")
        yes_bid_units = parse_optional_price_units(ticker_message, "yes_bid_dollars", "yes_bid")
        yes_ask_units = parse_optional_price_units(ticker_message, "yes_ask_dollars", "yes_ask")
        volume_units = parse_optional_count_units(ticker_message, "volume_fp", "volume")
        open_interest_units = parse_optional_count_units(ticker_message, "open_interest_fp", "open_interest")
        yes_bid_size_units = parse_optional_count_units(ticker_message, "yes_bid_size_fp", "yes_bid_size")
        yes_ask_size_units = parse_optional_count_units(ticker_message, "yes_ask_size_fp", "yes_ask_size")
        last_trade_size_units = parse_optional_count_units(ticker_message, "last_trade_size_fp", "last_trade_size")
        ts_ms = normalize_exchange_timestamp_to_ms(ticker_message.get("ts") or ticker_message.get("time"))

        self.ticker_state = TickerState(
            market_ticker=market_ticker,
            ts_ms=ts_ms,
            price_units=price_units,
            yes_bid_units=yes_bid_units,
            yes_ask_units=yes_ask_units,
            volume_units=volume_units,
            open_interest_units=open_interest_units,
            yes_bid_size_units=yes_bid_size_units,
            yes_ask_size_units=yes_ask_size_units,
            last_trade_size_units=last_trade_size_units,
        )
        self.telemetry_store.record_ticker(ticker_state=self.ticker_state)
        context = self.build_market_context()
        self.maybe_record_market_state(source="ticker", context=context)
        self.requote_event.set()

    def handle_trade_update(self, trade_message: dict) -> None:
        market_ticker = str(trade_message.get("market_ticker") or trade_message.get("ticker") or "")
        if market_ticker != self.settings.market_ticker:
            return

        yes_price_units = parse_optional_price_units(trade_message, "yes_price_dollars", "yes_price")
        no_price_units = parse_optional_price_units(trade_message, "no_price_dollars", "no_price")
        count_units = parse_optional_count_units(trade_message, "count_fp", "count")
        if yes_price_units is None and no_price_units is not None:
            yes_price_units = ONE_DOLLAR_PRICE_UNITS - int(no_price_units)
        if no_price_units is None and yes_price_units is not None:
            no_price_units = ONE_DOLLAR_PRICE_UNITS - int(yes_price_units)
        if yes_price_units is None or no_price_units is None or count_units is None:
            return

        trade = PublicTradeTick(
            trade_id=str(trade_message.get("trade_id") or ""),
            market_ticker=market_ticker,
            ts_ms=normalize_exchange_timestamp_to_ms(trade_message.get("ts")),
            yes_price_units=int(yes_price_units),
            no_price_units=int(no_price_units),
            count_units=int(count_units),
            taker_side=str(trade_message.get("taker_side") or ""),
        )
        self.recent_trades.append(trade)
        self.prune_recent_trades()
        self.telemetry_store.record_public_trade(trade=trade)
        context = self.build_market_context()
        self.maybe_record_market_state(source="trade", context=context)
        self.requote_event.set()

    def schedule_fill_markouts(
        self,
        *,
        fill_key: str,
        side: str,
        fill_price_units: int,
        fill_timestamp_ms: int,
        fill_context: MarketContext,
    ) -> None:
        asyncio.create_task(
            self.capture_fill_markouts(
                fill_key=fill_key,
                side=side,
                fill_price_units=fill_price_units,
                fill_timestamp_ms=fill_timestamp_ms,
                fill_context=fill_context,
            )
        )

    async def capture_fill_markouts(
        self,
        *,
        fill_key: str,
        side: str,
        fill_price_units: int,
        fill_timestamp_ms: int,
        fill_context: MarketContext,
    ) -> None:
        for horizon_seconds in self.settings.markout_horizons_seconds:
            target_timestamp_ms = fill_timestamp_ms + int(horizon_seconds) * 1000
            sleep_seconds = max(0.0, (target_timestamp_ms - now_ms()) / 1000.0)
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

            future_context = self.build_market_context()
            if future_context is None:
                continue
            future_fair = self.fair_value_engine.estimate(future_context, update_state=False)
            if side == "yes":
                adverse_units = max(0, fill_price_units - future_fair.fair_yes_units)
            else:
                adverse_units = max(0, fill_price_units - future_fair.fair_no_units)
            bucket_key = self.toxicity_model.record_markout(
                side=side,
                context=fill_context,
                horizon_ms=int(horizon_seconds) * 1000,
                adverse_units=adverse_units,
            )
            self.telemetry_store.record_markout(
                fill_key=fill_key,
                ts_ms=now_ms(),
                ticker=self.market.ticker,
                side=side,
                horizon_ms=int(horizon_seconds) * 1000,
                fill_price_units=fill_price_units,
                future_mid_yes_units=future_context.mid_yes_units,
                future_fair_yes_units=future_fair.fair_yes_units,
                adverse_units=adverse_units,
                bucket_key=bucket_key,
            )

    def handle_fill_update(self, fill_message: dict) -> None:
        market_ticker = str(fill_message.get("market_ticker") or fill_message.get("ticker") or "")
        if market_ticker != self.settings.market_ticker:
            return

        order_id = str(fill_message.get("order_id") or "")
        if not order_id:
            return
        side = self.known_strategy_order_sides.get(order_id)
        if side not in {"yes", "no"}:
            return

        trade_id = str(fill_message.get("trade_id") or "")
        fill_timestamp_ms = normalize_exchange_timestamp_to_ms(fill_message.get("ts"))
        self.fill_probability_model.register_fill(side=side, ts_ms=fill_timestamp_ms)
        fill_key = f"{trade_id}:{order_id}:{fill_timestamp_ms}"
        if fill_key in self.seen_fill_keys:
            return
        self.seen_fill_keys.add(fill_key)
        if len(self.seen_fill_keys) > 20_000:
            self.seen_fill_keys = set(list(self.seen_fill_keys)[-10_000:])

        count_units = parse_optional_count_units(fill_message, "count_fp", "count")
        if count_units is None or count_units <= 0:
            return
        count_units = int(count_units)

        price_units = (
            parse_optional_price_units(fill_message, "yes_price_dollars", "yes_price")
            if side == "yes"
            else parse_optional_price_units(fill_message, "no_price_dollars", "no_price")
        )
        fee_units = parse_optional_price_units(fill_message, "fee_cost", "fee")
        inventory_before_units = int(self.net_position_units)
        context_before = self.build_market_context()
        position_units = parse_optional_count_units(fill_message, "post_position_fp", "post_position")
        if position_units is not None:
            self.net_position_units = int(position_units)
        else:
            self.net_position_units = int(self.net_position_units + count_units if side == "yes" else self.net_position_units - count_units)
        inventory_after_units = int(self.net_position_units)

        state = self.orders[side]
        state.last_positive_fill_timestamp_ms = fill_timestamp_ms
        state.same_side_reentry_cooldown_until_ms = max(
            state.same_side_reentry_cooldown_until_ms,
            fill_timestamp_ms + self.settings.same_side_reentry_cooldown_ms,
        )
        self.last_fill_timestamp_ms = fill_timestamp_ms

        if fee_units is not None and price_units is not None:
            self.fee_model.record_fill_fee(price_units=price_units, count_units=count_units, fee_units=fee_units)
        self.telemetry_store.record_fill(
            fill_key=fill_key,
            ts_ms=fill_timestamp_ms,
            ticker=self.market.ticker,
            side=side,
            trade_id=trade_id,
            order_id=order_id,
            price_units=price_units,
            size_units=count_units,
            fee_units=fee_units,
            is_taker=bool(fill_message.get("is_taker")),
            inventory_before_units=inventory_before_units,
            inventory_after_units=inventory_after_units,
            fair_yes_before_units=(self.last_quote_decisions.get(side).fair_yes_units if self.last_quote_decisions.get(side) else None),
            best_yes_bid_units=(context_before.best_yes_bid_units if context_before is not None else None),
            best_no_bid_units=(context_before.best_no_bid_units if context_before is not None else None),
            queue_ahead_units=(context_before.queue_ahead_units_for_side(side) if context_before is not None else None),
        )
        log_event(
            "FILL_TELEMETRY",
            side=side,
            fill_contracts=format_count_fp(count_units),
            fee_dollars=(format_price_dollars(fee_units) if fee_units is not None else "unknown"),
            net_position_contracts=format_count_fp(self.net_position_units),
        )
        append_pnl_fill(
            ticker=self.market.ticker,
            side=side,
            price_cents=round(price_units / PRICE_UNITS_PER_CENT, 2) if price_units is not None else None,
            quantity=round(count_units / COUNT_SCALE, 2),
            fee_cents=round(fee_units / PRICE_UNITS_PER_CENT, 2) if fee_units is not None else None,
            net_position_after=round(self.net_position_units / COUNT_SCALE, 2),
            fair_value_cents=(
                round(self.last_quote_decisions[side].fair_yes_units / PRICE_UNITS_PER_CENT, 2)
                if self.last_quote_decisions.get(side) and self.last_quote_decisions[side].fair_yes_units is not None
                else None
            ),
        )
        if price_units is not None and context_before is not None:
            self.schedule_fill_markouts(
                fill_key=fill_key,
                side=side,
                fill_price_units=price_units,
                fill_timestamp_ms=fill_timestamp_ms,
                fill_context=context_before,
            )
        self.requote_event.set()

    def handle_user_order_update(self, order_message: dict) -> None:
        market_ticker = order_message.get("ticker") or order_message.get("market_ticker")
        if market_ticker != self.settings.market_ticker:
            return

        side = str(order_message.get("side") or "")
        if side not in {"yes", "no"}:
            if "is_yes" in order_message:
                side = "yes" if bool(order_message.get("is_yes")) else "no"
            if side not in {"yes", "no"}:
                return

        client_order_id = str(order_message.get("client_order_id") or "")
        if not self.is_strategy_client_order_id(client_order_id):
            return

        state = self.orders[side]
        incoming_order_id = order_message.get("order_id")
        incoming_status = order_message.get("status")

        if incoming_order_id and state.order_id and incoming_order_id != state.order_id:
            state.order_id = incoming_order_id
            state.current_order_filled_units = 0
            state.remaining_count_units = 0

        if incoming_order_id:
            state.order_id = incoming_order_id
            self.known_strategy_order_sides[str(incoming_order_id)] = side
        state.client_order_id = client_order_id
        state.status = incoming_status

        fill_units = parse_optional_count_units(order_message, "fill_count_fp", "fill_count")
        if fill_units is not None:
            fill_units = int(fill_units)
            fill_delta_units = fill_units - state.current_order_filled_units
            if fill_delta_units > 0:
                event_timestamp_ms = now_ms()
                state.quote_cycle_filled_units += fill_delta_units
                state.last_positive_fill_timestamp_ms = event_timestamp_ms
                state.same_side_reentry_cooldown_until_ms = max(
                    state.same_side_reentry_cooldown_until_ms,
                    event_timestamp_ms + self.settings.same_side_reentry_cooldown_ms,
                )
                self.last_fill_timestamp_ms = event_timestamp_ms
                log_event(
                    "FILL_DETECTED",
                    side=side,
                    fill_contracts=format_count_fp(fill_delta_units),
                    cooldown_ms=self.settings.same_side_reentry_cooldown_ms,
                    net_position_contracts=format_count_fp(self.net_position_units),
                )
            state.current_order_filled_units = fill_units

        remaining_units = parse_optional_count_units(order_message, "remaining_count_fp", "remaining_count")
        if remaining_units is not None:
            state.remaining_count_units = int(remaining_units)

        price_units = (
            parse_optional_price_units(order_message, "yes_price_dollars", "yes_price")
            if side == "yes"
            else parse_optional_price_units(order_message, "no_price_dollars", "no_price")
        )
        if price_units is not None:
            state.price_units = price_units

        expiration_time_ms = parse_iso_utc_timestamp_to_ms(order_message.get("expiration_time"))
        if expiration_time_ms is not None:
            state.expiration_time_ms = expiration_time_ms

        if state.status != "resting":
            preserve_cycle = state.status == "canceled"
            log_event("ORDER_NOT_RESTING", side=side, status=state.status, order_id=state.order_id)
            state.clear_active_order(preserve_quote_cycle=preserve_cycle)

        self.requote_event.set()

    # -------------------------
    # Async workers
    # -------------------------

    async def requote_worker(self) -> None:
        while not self.shutdown_requested:
            await self.requote_event.wait()
            self.requote_event.clear()

            if not self.book_ready:
                continue

            milliseconds_since_last_requote = now_ms() - self.last_requote_action_timestamp_ms
            if milliseconds_since_last_requote < self.settings.minimum_milliseconds_between_requotes:
                await asyncio.sleep(
                    (self.settings.minimum_milliseconds_between_requotes - milliseconds_since_last_requote) / 1000.0
                )

            async with self.requote_lock:
                desired_yes_units, desired_no_units = self.desired_quote_prices()
                try:
                    await self.ensure_side_quote("yes", desired_yes_units)
                    await self.ensure_side_quote("no", desired_no_units)
                    self.last_requote_action_timestamp_ms = now_ms()
                except Exception as exc:
                    if is_post_only_cross_error(exc):
                        log_event("REQUOTE_POST_ONLY_CROSS_IGNORED", error=str(exc))
                    elif is_rate_limit_error(exc):
                        log_event("REQUOTE_RATE_LIMIT_BACKOFF", seconds=self.settings.global_rate_limit_backoff_seconds, error=str(exc))
                        await asyncio.sleep(self.settings.global_rate_limit_backoff_seconds)
                        self.requote_event.set()
                    else:
                        log_event("REQUOTE_ERROR", error=str(exc))
                        traceback.print_exc()

    async def queue_position_worker(self) -> None:
        if not self.settings.enable_queue_position_logging:
            return

        while True:
            await asyncio.sleep(self.settings.queue_position_log_interval_seconds)

            for side, state in self.orders.items():
                if not state.has_active_resting_order or not state.order_id:
                    state.consecutive_queue_ahead_breaches = 0
                    continue

                try:
                    response = await asyncio.to_thread(self.api_client.get_order_queue_position, state.order_id)
                    queue_position_units = parse_optional_count_units(response, "queue_position_fp", "queue_position")
                    if queue_position_units is None and isinstance(response.get("order"), dict):
                        queue_position_units = parse_optional_count_units(response["order"], "queue_position_fp", "queue_position")
                    state.last_queue_position_units = queue_position_units
                    log_event(
                        "QUEUE_POSITION",
                        side=side,
                        order_id=state.order_id,
                        ahead_contracts=(format_count_fp(queue_position_units) if queue_position_units is not None else "unknown"),
                    )
                    await self.process_queue_position_update(side, queue_position_units)
                except Exception as exc:
                    log_event("QUEUE_POSITION_ERROR", side=side, order_id=state.order_id, error=str(exc))

    async def model_refresh_worker(self) -> None:
        refresh_interval_seconds = max(60, int(self.settings.model_refresh_interval_seconds))
        while not self.shutdown_requested:
            try:
                await asyncio.to_thread(self.refresh_external_models)
            except Exception as exc:
                log_event("MODEL_REFRESH_ERROR", error=str(exc))
            await asyncio.sleep(refresh_interval_seconds)

    def sync_position_from_rest(self) -> int:
        response = self.api_client.get_positions(self.settings.market_ticker)
        market_positions = response.get("market_positions") or []
        if not market_positions:
            self.net_position_units = 0
            return 0

        position_payload = market_positions[0]
        position_units = parse_optional_count_units(position_payload, "position_fp", "position")
        self.net_position_units = int(position_units or 0)
        return self.net_position_units

    def maybe_load_watchdog_state(self) -> Optional[WatchdogState]:
        state_path = self.settings.watchdog_state_file.strip()
        if not state_path:
            return None

        try:
            stat_result = os.stat(state_path)
        except FileNotFoundError:
            return None

        if self.watchdog_state_mtime_ns == stat_result.st_mtime_ns and self.watchdog_state is not None:
            return self.watchdog_state

        with open(state_path, "r", encoding="utf-8") as state_file:
            payload = json.load(state_file)

        state = WatchdogState(
            ticker=str(payload.get("ticker") or self.settings.market_ticker),
            generated_at_ms=int(payload.get("generated_at_ms") or 0),
            effective_close_time_ms=(int(payload["effective_close_time_ms"]) if payload.get("effective_close_time_ms") not in (None, "") else None),
            soft_stop_time_ms=(int(payload["soft_stop_time_ms"]) if payload.get("soft_stop_time_ms") not in (None, "") else None),
            hard_stop_time_ms=(int(payload["hard_stop_time_ms"]) if payload.get("hard_stop_time_ms") not in (None, "") else None),
            flatten_only_time_ms=(int(payload["flatten_only_time_ms"]) if payload.get("flatten_only_time_ms") not in (None, "") else None),
            confidence=float(payload.get("confidence") or 0.0),
            mode=str(payload.get("mode") or "normal"),
            reason=str(payload.get("reason") or "unknown"),
            profile_version=str(payload.get("profile_version") or "heuristic-v2"),
        )
        self.watchdog_state = state
        self.watchdog_state_mtime_ns = stat_result.st_mtime_ns

        if self.watchdog_last_mode_logged != state.mode:
            log_event(
                "WATCHDOG_MODE_CHANGE",
                ticker=state.ticker,
                old_mode=(self.watchdog_last_mode_logged or "none"),
                new_mode=state.mode,
                reason=state.reason,
                confidence=f"{state.confidence:.3f}",
            )
            self.watchdog_last_mode_logged = state.mode

        return state

    async def request_shutdown(self, exit_code: int) -> None:
        self.shutdown_requested = True
        self.shutdown_exit_code = int(exit_code)
        self.requote_event.set()
        websocket = self.websocket_connection
        if websocket is not None:
            try:
                await websocket.close()
            except Exception:
                pass

    async def emergency_cancel_all_quotes(self, *, reason: str) -> None:
        await self.cancel_side_quote("yes", reason=reason, reset_quote_cycle=True)
        await self.cancel_side_quote("no", reason=reason, reset_quote_cycle=True)

    async def submit_watchdog_exit_order(self) -> bool:
        net_position_units = int(self.sync_position_from_rest())
        if net_position_units == 0:
            return True

        quote = await asyncio.to_thread(self.api_client.get_market_quote, self.settings.market_ticker)
        held_side = "yes" if net_position_units > 0 else "no"
        best_bid_units = int(quote.get("yes_bid_units") or 0) if held_side == "yes" else int(quote.get("no_bid_units") or 0)
        if best_bid_units <= 0:
            log_event("WATCHDOG_EXIT_NO_BID", side=held_side, net_position_contracts=format_count_fp(net_position_units))
            return False

        exit_count_units = abs(net_position_units)
        client_order_id = f"wd:{held_side}:{uuid.uuid4().hex[:16]}"
        response = await asyncio.to_thread(
            self.api_client.create_order,
            market_ticker=self.settings.market_ticker,
            side=held_side,
            action="sell",
            price_units=best_bid_units,
            count_units=exit_count_units,
            client_order_id=client_order_id,
            expiration_timestamp_seconds=None,
            post_only=False,
            reduce_only=True,
            time_in_force="immediate_or_cancel",
            cancel_order_on_pause=False,
        )
        order_payload = (response or {}).get("order") or {}
        log_event(
            "WATCHDOG_EXIT_ORDER",
            side=held_side,
            price_dollars=format_price_dollars(best_bid_units),
            contracts=format_count_fp(exit_count_units),
            status=(order_payload.get("status") or "unknown"),
            order_id=(order_payload.get("order_id") or "unknown"),
        )
        await asyncio.sleep(0.75)
        refreshed_position_units = int(self.sync_position_from_rest())
        return refreshed_position_units == 0

    async def execute_watchdog_shutdown(self, *, reason: str, requested_exit_code: int) -> None:
        if self.watchdog_shutdown_in_progress:
            return
        self.watchdog_shutdown_in_progress = True

        log_event(
            "WATCHDOG_FLATTEN_TRIGGER",
            ticker=self.settings.market_ticker,
            reason=reason,
            threshold="watchdog",
            observed_value=(self.watchdog_state.confidence if self.watchdog_state is not None else "unknown"),
        )

        residual_exit_code = 63
        async with self.requote_lock:
            try:
                await self.emergency_cancel_all_quotes(reason=f"watchdog_{reason}")
                net_position_units = int(self.sync_position_from_rest())
                if net_position_units == 0:
                    await self.request_shutdown(requested_exit_code)
                    return

                for attempt in range(max(1, self.settings.watchdog_flatten_retries)):
                    flattened = await self.submit_watchdog_exit_order()
                    if flattened:
                        await self.request_shutdown(requested_exit_code)
                        return
                    log_event(
                        "WATCHDOG_FLATTEN_RETRY",
                        ticker=self.settings.market_ticker,
                        attempt=attempt + 1,
                        net_position_contracts=format_count_fp(self.net_position_units),
                    )
                    await self.emergency_cancel_all_quotes(reason=f"watchdog_retry_{attempt + 1}")

                log_event(
                    "WATCHDOG_FLATTEN_RESIDUAL",
                    ticker=self.settings.market_ticker,
                    net_position_contracts=format_count_fp(self.net_position_units),
                )
                await self.request_shutdown(residual_exit_code)
                return
            except Exception as exc:
                log_event("WATCHDOG_FLATTEN_ERROR", ticker=self.settings.market_ticker, error=str(exc))
                await self.request_shutdown(residual_exit_code)
                return

    async def watchdog_worker(self) -> None:
        if not self.settings.watchdog_state_file.strip():
            return

        refresh_interval_seconds = max(1.0, float(self.settings.watchdog_refresh_seconds))
        while not self.shutdown_requested:
            try:
                state = self.maybe_load_watchdog_state()
                current_ms = now_ms()
                if state is None:
                    missing_age_seconds = max(0.0, (current_ms - self.started_at_ms) / 1000.0)
                    if missing_age_seconds > float(self.settings.watchdog_extreme_stale_seconds):
                        log_event(
                            "WATCHDOG_PROFILE_STALE",
                            ticker=self.settings.market_ticker,
                            age_seconds=f"{missing_age_seconds:.1f}",
                            action="flatten_only_missing_state",
                        )
                        await self.execute_watchdog_shutdown(reason="profile_missing", requested_exit_code=64)
                        return
                    await asyncio.sleep(refresh_interval_seconds)
                    continue

                if state.generated_at_ms > 0:
                    age_seconds = max(0.0, (current_ms - state.generated_at_ms) / 1000.0)
                    if age_seconds > float(self.settings.watchdog_extreme_stale_seconds):
                        log_event(
                            "WATCHDOG_PROFILE_STALE",
                            ticker=state.ticker,
                            age_seconds=f"{age_seconds:.1f}",
                            action="flatten_only",
                        )
                        await self.execute_watchdog_shutdown(reason="profile_stale", requested_exit_code=64)
                        return

                if state.effective_close_time_ms is not None and current_ms >= int(state.effective_close_time_ms):
                    await self.execute_watchdog_shutdown(reason="effective_close", requested_exit_code=61)
                    return

                if state.mode == "flatten_only":
                    exit_code = 64 if state.reason in {"invalid_effective_close", "profile_stale"} else 62
                    if state.reason == "effective_close_window":
                        exit_code = 61
                    await self.execute_watchdog_shutdown(reason=state.reason, requested_exit_code=exit_code)
                    return
            except Exception as exc:
                log_event("WATCHDOG_STATE_ERROR", ticker=self.settings.market_ticker, error=str(exc))

            await asyncio.sleep(refresh_interval_seconds)

    async def websocket_main(self) -> None:
        backoff_seconds = 1

        while True:
            try:
                headers = self.api_client.websocket_headers()
                try:
                    connection = websockets.connect(
                        self.api_client.websocket_url,
                        additional_headers=headers,
                        ping_interval=20,
                        ping_timeout=20,
                    )
                except TypeError:
                    connection = websockets.connect(
                        self.api_client.websocket_url,
                        extra_headers=headers,
                        ping_interval=20,
                        ping_timeout=20,
                    )

                async with connection as websocket:
                    self.websocket_connection = websocket
                    backoff_seconds = 1
                    self.book_ready = False
                    self.last_orderbook_sequence = None

                    subscriptions = [
                        {
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_tickers": [self.settings.market_ticker],
                            },
                        },
                        {
                            "id": 2,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["user_orders"],
                                "market_tickers": [self.settings.market_ticker],
                            },
                        },
                        {
                            "id": 3,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["fill"],
                                "market_tickers": [self.settings.market_ticker],
                            },
                        },
                        {
                            "id": 4,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["trade", "ticker"],
                                "market_tickers": [self.settings.market_ticker],
                            },
                        },
                    ]

                    if self.settings.subscribe_to_market_positions_channel:
                        subscriptions.append(
                            {
                                "id": 5,
                                "cmd": "subscribe",
                                "params": {
                                    "channels": ["market_positions"],
                                    "market_tickers": [self.settings.market_ticker],
                                },
                            }
                        )

                    for subscription in subscriptions:
                        await websocket.send(json.dumps(subscription))

                    log_event("WS_CONNECTED_AND_SUBSCRIBED")

                    async for raw_message in websocket:
                        if self.shutdown_requested:
                            return
                        data = json.loads(raw_message)
                        message_type = data.get("type")

                        if message_type == "orderbook_snapshot":
                            self.last_orderbook_sequence = data.get("seq")
                            self.apply_orderbook_snapshot(data.get("msg", {}))
                            self.requote_event.set()
                        elif message_type == "orderbook_delta":
                            new_sequence = data.get("seq")
                            if (
                                self.last_orderbook_sequence is not None
                                and new_sequence is not None
                                and new_sequence != self.last_orderbook_sequence + 1
                            ):
                                log_event(
                                    "ORDERBOOK_SEQUENCE_GAP",
                                    previous_sequence=self.last_orderbook_sequence,
                                    new_sequence=new_sequence,
                                )
                                break

                            self.last_orderbook_sequence = new_sequence
                            self.apply_orderbook_delta(data.get("msg", {}))
                            self.requote_event.set()
                        elif message_type == "user_order":
                            self.handle_user_order_update(data.get("msg", {}))
                        elif message_type == "fill":
                            self.handle_fill_update(data.get("msg", {}))
                        elif message_type == "trade":
                            self.handle_trade_update(data.get("msg", {}))
                        elif message_type == "ticker":
                            self.handle_ticker_update(data.get("msg", {}))
                        elif message_type == "market_position":
                            self.handle_market_position_update(data.get("msg", {}))
                            self.requote_event.set()
                        elif message_type == "error":
                            log_event("WS_ERROR", payload=data)

            except Exception as exc:
                if self.shutdown_requested:
                    return
                log_event("WS_DISCONNECT", error=str(exc), reconnect_backoff_seconds=backoff_seconds)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(30, backoff_seconds * 2)
            finally:
                self.websocket_connection = None

    async def run(self) -> None:
        log_event(
            "START",
            environment=("DEMO" if self.settings.use_demo_environment else "PROD"),
            ticker=self.settings.market_ticker,
            yes_budget_cents=self.settings.yes_order_budget_cents,
            no_budget_cents=self.settings.no_order_budget_cents,
            price_structure=self.market.price_level_structure or "unknown",
            fractional_trading=self.market.fractional_trading_enabled,
            dry_run=self.settings.dry_run,
            websocket=self.api_client.websocket_url,
            reprice_ms=self.settings.minimum_milliseconds_between_requotes,
            expiration_seconds=self.settings.resting_order_expiration_seconds,
            same_side_reentry_cooldown_ms=self.settings.same_side_reentry_cooldown_ms,
            queue_guard=self.settings.enable_queue_abandonment_guard,
            watchdog_state_file=(self.settings.watchdog_state_file or "none"),
            watchdog_refresh_seconds=self.settings.watchdog_refresh_seconds,
        )

        self.load_startup_position()
        self.cancel_owned_resting_quotes_on_startup()
        await asyncio.to_thread(self.refresh_external_models)

        self.background_tasks = [
            asyncio.create_task(self.requote_worker()),
            asyncio.create_task(self.queue_position_worker()),
            asyncio.create_task(self.model_refresh_worker()),
        ]
        if self.settings.watchdog_state_file.strip():
            self.background_tasks.append(asyncio.create_task(self.watchdog_worker()))

        try:
            await self.websocket_main()
        finally:
            for task in self.background_tasks:
                task.cancel()
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)

        if self.shutdown_exit_code is not None:
            raise SystemExit(self.shutdown_exit_code)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def load_market_metadata(api_client: KalshiApiClient, market_ticker: str) -> MarketMetadata:
    market_payload = api_client.get_market(market_ticker)
    price_grid = PriceGrid.from_market_response(market_payload)

    series_ticker = str(
        market_payload.get("series_ticker")
        or market_payload.get("series")
        or market_payload.get("series_name")
        or market_ticker
    )
    event_ticker = str(
        market_payload.get("event_ticker")
        or market_payload.get("event")
        or market_payload.get("event_name")
        or market_ticker
    )
    close_time_ms = None
    for key in ("close_time", "close_date", "expiration_time", "expiration_date", "settlement_time"):
        close_time_ms = parse_iso_utc_timestamp_to_ms(market_payload.get(key))
        if close_time_ms is not None:
            break

    return MarketMetadata(
        ticker=str(market_payload.get("ticker") or market_ticker),
        series_ticker=series_ticker,
        event_ticker=event_ticker,
        close_time_ms=close_time_ms,
        title=str(market_payload.get("title") or ""),
        status=str(market_payload.get("status") or ""),
        price_level_structure=str(market_payload.get("price_level_structure") or ""),
        fractional_trading_enabled=bool(market_payload.get("fractional_trading_enabled")),
        price_grid=price_grid,
    )



def main() -> None:
    bot_args = parse_bot_args()
    settings = build_settings_from_args(bot_args)

    api_client = KalshiApiClient(settings)
    market = load_market_metadata(api_client, settings.market_ticker)

    bot = TopOfBookBot(
        settings=settings,
        api_client=api_client,
        market=market,
    )
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log_event(
            "GRACEFUL_SHUTDOWN",
            ticker=settings.market_ticker,
            net_position_contracts=format_count_fp(bot.net_position_units),
            reason="SIGINT",
        )
        try:
            bot.cancel_owned_resting_quotes_on_startup()
        except Exception as exc:
            log_event("SHUTDOWN_CANCEL_ERROR", error=str(exc))


if __name__ == "__main__":
    main()
