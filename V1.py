
#!/usr/bin/env python3
"""
Kalshi single-market top-of-book market-maker.

What this version improves:
- Uses fixed-point `_dollars` and `_fp` fields for prices and quantities.
- Cancels quotes when a side is disabled or when the budget implies zero size.
- Reads startup positions and live market-position updates for accurate net inventory.
- Adds resting-order expirations and refreshes orders before they expire.
- Uses clear, descriptive configuration names with explicit units in each name.
- Avoids logging secrets and is launcher-friendly.

This bot keeps one BUY YES quote and one BUY NO quote live on a single market.
It is intentionally conservative by default: whole-contract order entry, short
resting-order expirations, post-only orders, and modest quote sizes.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import math
import os
import time
import traceback
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Iterable, List, Optional, Tuple

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
    return parser.parse_args()



# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BotSettings:
    # --- Core identity / credentials ---
    market_ticker: str = "KXNCAAMBSPREAD-26MAR03WVUKSU-WVU1"
    api_key_id: str = "<YOUR_API_KEY_ID>"
    private_key_path: str = "./private_key.pem"
    use_demo_environment: bool = False
    dry_run: bool = False
    subaccount_number: int = 0

    # --- Order sizing ---
    # These are the maximum working budgets per side.
    # Example: 100 means $1.00, 1000 means $10.00.
    yes_order_budget_cents: int = 100
    no_order_budget_cents: int = 100
    maximum_contracts_per_order: int = 2_000
    budget_fee_buffer_cents: int = 5

    # Whole-contract order entry is the safest default.
    # The bot still understands fractional positions and fills internally.
    allow_fractional_order_entry_when_supported: bool = False

    # If False (default), the bot keeps the TOTAL MAX FILLABLE size on a quote
    # at the target budget amount. After a partial fill, the resting size shrinks.
    # If True, the bot replenishes the resting size back to the full target size.
    refill_resting_size_after_partial_fill: bool = False

    # --- Quote behavior ---
    post_only_quotes: bool = True
    cancel_quotes_if_exchange_pauses: bool = True
    minimum_milliseconds_between_requotes: int = 6000

    # Use short expirations to avoid stale resting orders after crashes.
    # Set to 0 to disable expirations and use good_till_canceled instead.
    resting_order_expiration_seconds: int = 400
    expiration_refresh_lead_seconds: int = 5

    # When spread is wide enough, the bot may improve by this many ticks.
    # 0 means "do not improve; only join or sit behind".
    aggressive_improvement_ticks_when_spread_is_wide: int = 0
    minimum_spread_ticks_required_for_aggressive_improvement: int = 8

    # When not improving, the bot can sit behind the current best bid by this many ticks.
    passive_offset_ticks_when_not_improving: int = 2

    # After a fill, disable aggressive improvement briefly to reduce adverse selection.
    post_fill_no_improve_cooldown_ms: int = 10000

    # Stop quoting new interest on a side if the current market is too close to zero.
    minimum_best_bid_cents_required_to_quote: int = 22
    minimum_implied_ask_cents_required_to_quote: int = 22

    # Keep our bid at least one valid tick below the implied ask on the opposite side.
    enforce_one_tick_safety_below_implied_ask: bool = True

    # --- Inventory controls ---
    enable_one_way_inventory_guard: bool = True
    one_way_inventory_guard_contracts: int = 10

    inventory_skew_contracts_per_tick: int = 10
    maximum_inventory_skew_ticks: int = 5

    # --- Pair / spread protection ---
    enable_pair_guard: bool = True

    # Hard ceiling on YES bid + NO bid.
    # Example: 98 means the two bids together must be <= $0.98, subject to the
    # tighter minimum-profit calculation below.
    maximum_combined_bid_cents: int = 98

    # Require at least this much extra gross room beyond the self-cross boundary.
    additional_profit_buffer_cents: int = 1

    # When the pair guard must lower one side, choose:
    # "yes" -> prefer keeping YES quote higher
    # "no"  -> prefer keeping NO quote higher
    # "auto" -> lower whichever side is currently larger
    pair_guard_priority: str = "auto"

    # --- Post-only collision handling ---
    maximum_post_only_reprice_attempts: int = 3
    post_only_reprice_cooldown_seconds: float = 1.5

    # --- Startup / monitoring ---
    cancel_strategy_quotes_on_startup: bool = True
    subscribe_to_market_positions_channel: bool = True

    # Optional queue-position polling (disabled by default to save API calls).
    enable_queue_position_logging: bool = True
    queue_position_log_interval_seconds: float = 15.0

    # Client-order-id prefixes owned by this strategy. Legacy `tob:` support is
    # included so the bot can safely clean up quotes from the older version.
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
        if self.resting_order_expiration_seconds < 0:
            raise ValueError("resting_order_expiration_seconds must be >= 0")
        if self.expiration_refresh_lead_seconds < 0:
            raise ValueError("expiration_refresh_lead_seconds must be >= 0")
        if self.expiration_refresh_lead_seconds >= self.resting_order_expiration_seconds and self.resting_order_expiration_seconds > 0:
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
        if self.minimum_best_bid_cents_required_to_quote < 0 or self.minimum_implied_ask_cents_required_to_quote < 0:
            raise ValueError("Quote-threshold cents must be >= 0")
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
        if self.pair_guard_priority.lower() not in {"yes", "no", "auto"}:
            raise ValueError("pair_guard_priority must be 'yes', 'no', or 'auto'")


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
        yes_order_budget_cents=int(bot_args.yes_budget_cents if bot_args.yes_budget_cents is not None else defaults.yes_order_budget_cents),
        no_order_budget_cents=int(bot_args.no_budget_cents if bot_args.no_budget_cents is not None else defaults.no_order_budget_cents),
    )
    settings.validate()
    return settings


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def utc_seconds_to_expiration_timestamp(seconds_in_future: int) -> int:
    return int(time.time()) + int(seconds_in_future)


def parse_iso_utc_timestamp_to_ms(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        from datetime import datetime
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


def count_units_to_contracts_string(count_units: int) -> str:
    return format_count_fp(count_units)


def log_event(event_name: str, **fields: object) -> None:
    if fields:
        detail = " ".join(f"{key}={value}" for key, value in fields.items())
        LOGGER.info("%s | %s", event_name, detail)
    else:
        LOGGER.info("%s", event_name)


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


# ---------------------------------------------------------------------------
# Price grid
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PriceRange:
    start_units: int
    end_units: int
    step_units: int


class PriceGrid:
    """
    Represents all valid order-entry prices for a market.

    Prices are tracked in 1/10,000-dollar units so that:
    - $0.0100 == 100 units
    - $0.0010 == 10 units
    """

    def __init__(self, price_ranges: Iterable[PriceRange]) -> None:
        self.ranges: List[PriceRange] = sorted(price_ranges, key=lambda item: (item.start_units, item.end_units, item.step_units))
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

        # Fallback for older market payloads that only expose tick_size in legacy cents.
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
        """
        Largest valid opposite-side bid that keeps own_bid + other_bid < 1.0000.
        """
        complement_units = ONE_DOLLAR_PRICE_UNITS - int(own_bid_price_units)
        return self.previous_price(complement_units)


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

        if not settings.api_key_id or settings.api_key_id == "<YOUR_API_KEY_ID>":
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

    def rest_post(self, path: str, body: dict) -> dict:
        response = self.session.post(
            self._build_url(path),
            headers=self._rest_headers("POST", path),
            json=body,
            timeout=15,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"POST {path} failed {response.status_code}: {response.text[:500]}")
        if not response.text.strip():
            return {}
        return response.json()

    def rest_delete(self, path: str, params: Optional[dict] = None) -> dict:
        response = self.session.delete(
            self._build_url(path),
            headers=self._rest_headers("DELETE", path),
            params=params,
            timeout=15,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"DELETE {path} failed {response.status_code}: {response.text[:500]}")
        if not response.text.strip():
            return {}
        return response.json()

    def get_market(self, market_ticker: str) -> dict:
        response = self.rest_get(f"{self.api_prefix}/markets/{market_ticker}")
        return response["market"]

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
            cursor = response.get("cursor") or ""
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
    ) -> dict:
        body = {
            "ticker": market_ticker,
            "side": side,
            "action": "buy",
            "client_order_id": client_order_id,
            "count_fp": format_count_fp(count_units),
            "post_only": bool(self.settings.post_only_quotes),
            "cancel_order_on_pause": bool(self.settings.cancel_quotes_if_exchange_pauses),
            "subaccount": self.settings.subaccount_number,
        }

        if side == "yes":
            body["yes_price_dollars"] = format_price_dollars(price_units)
        else:
            body["no_price_dollars"] = format_price_dollars(price_units)

        if expiration_timestamp_seconds and expiration_timestamp_seconds > 0:
            body["expiration_ts"] = int(expiration_timestamp_seconds)
            body["time_in_force"] = "good_till_canceled"
        else:
            body["time_in_force"] = "good_till_canceled"

        if self.settings.dry_run:
            log_event(
                "DRY_CREATE",
                side=side,
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

        return self.rest_post(f"{self.api_prefix}/portfolio/orders", body)

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

        return self.rest_post(f"{self.api_prefix}/portfolio/orders/{order_id}/amend", body)

    def decrease_order_to(
        self,
        *,
        order_id: str,
        remaining_count_units: int,
    ) -> dict:
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

        return self.rest_post(f"{self.api_prefix}/portfolio/orders/{order_id}/decrease", body)

    def cancel_order(self, *, order_id: str) -> dict:
        if self.settings.dry_run:
            log_event("DRY_CANCEL", order_id=order_id)
            return {"order": {"order_id": order_id}}

        return self.rest_delete(
            f"{self.api_prefix}/portfolio/orders/{order_id}",
            params={"subaccount": self.settings.subaccount_number},
        )

    def get_order_queue_position(self, order_id: str) -> dict:
        return self.rest_get(f"{self.api_prefix}/portfolio/orders/{order_id}/queue_position")


# ---------------------------------------------------------------------------
# Market metadata and order state
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MarketMetadata:
    ticker: str
    title: str
    status: str
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
        if not preserve_quote_cycle:
            self.quote_cycle_filled_units = 0


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

        self.book_ready = False
        self.last_orderbook_sequence: Optional[int] = None
        self.last_fill_timestamp_ms = 0
        self.last_requote_action_timestamp_ms = 0

        self.net_position_units = 0

        self.orders: Dict[str, ManagedOrderState] = {
            "yes": ManagedOrderState(side="yes"),
            "no": ManagedOrderState(side="no"),
        }

        self.requote_event = asyncio.Event()
        self.requote_lock = asyncio.Lock()

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
        log_event(
            "STARTUP_POSITION",
            contracts=format_count_fp(self.net_position_units),
        )

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

    def quote_allowed_for_side(
        self,
        *,
        side: str,
        best_bid_units: int,
        implied_ask_units: int,
    ) -> bool:
        minimum_bid_units = cents_to_price_units(self.settings.minimum_best_bid_cents_required_to_quote)
        minimum_ask_units = cents_to_price_units(self.settings.minimum_implied_ask_cents_required_to_quote)
        blocked_side = self.inventory_blocked_side()

        allowed = (
            best_bid_units >= minimum_bid_units
            and implied_ask_units >= minimum_ask_units
            and blocked_side != side
        )

        # Exit-side override: if we are already long one direction, keep the opposite
        # side available so the strategy can naturally work inventory back down.
        if self.net_position_units > 0 and side == "no" and blocked_side != "no":
            allowed = True
        elif self.net_position_units < 0 and side == "yes" and blocked_side != "yes":
            allowed = True

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

    def desired_quote_prices(self) -> Tuple[Optional[int], Optional[int]]:
        best_yes_bid_units = self.best_bid("yes")
        best_no_bid_units = self.best_bid("no")
        if best_yes_bid_units is None or best_no_bid_units is None:
            return None, None

        implied_yes_ask_units = self.implied_yes_ask(best_no_bid_units)
        implied_no_ask_units = self.implied_no_ask(best_yes_bid_units)

        maximum_yes_bid_units = self.cap_bid_below_implied_ask(implied_yes_ask_units)
        maximum_no_bid_units = self.cap_bid_below_implied_ask(implied_no_ask_units)
        if maximum_yes_bid_units is None or maximum_no_bid_units is None:
            return None, None

        yes_quote_allowed = self.quote_allowed_for_side(
            side="yes",
            best_bid_units=best_yes_bid_units,
            implied_ask_units=implied_yes_ask_units,
        )
        no_quote_allowed = self.quote_allowed_for_side(
            side="no",
            best_bid_units=best_no_bid_units,
            implied_ask_units=implied_no_ask_units,
        )

        allow_aggressive_improvement = (
            self.settings.aggressive_improvement_ticks_when_spread_is_wide > 0
            and not self.post_fill_improvement_cooldown_active()
        )

        # Approximate current spread in ticks using repeated next-price moves.
        if allow_aggressive_improvement:
            tick_counter = 0
            candidate_price_units = best_yes_bid_units
            while True:
                candidate_price_units = self.market.price_grid.next_price(candidate_price_units) if candidate_price_units is not None else None
                if candidate_price_units is None or candidate_price_units > maximum_yes_bid_units:
                    break
                tick_counter += 1
                if tick_counter >= self.settings.minimum_spread_ticks_required_for_aggressive_improvement:
                    break
            allow_aggressive_improvement = tick_counter >= self.settings.minimum_spread_ticks_required_for_aggressive_improvement

        yes_other_best_units = self.best_bid_excluding_our_order("yes")
        no_other_best_units = self.best_bid_excluding_our_order("no")

        desired_yes_units: Optional[int]
        desired_no_units: Optional[int]

        current_yes_price_units = self.orders["yes"].price_units
        if not yes_quote_allowed:
            desired_yes_units = None
        elif current_yes_price_units is None:
            desired_yes_units = min(best_yes_bid_units, maximum_yes_bid_units)
        else:
            if yes_other_best_units is None:
                desired_yes_units = min(current_yes_price_units, maximum_yes_bid_units)
            else:
                target_yes_units = self.choose_target_from_other_best(
                    other_best_units=yes_other_best_units,
                    cap_price_units=maximum_yes_bid_units,
                    allow_aggressive_improvement=allow_aggressive_improvement,
                )
                desired_yes_units = (
                    target_yes_units
                    if current_yes_price_units < yes_other_best_units or current_yes_price_units > target_yes_units
                    else min(current_yes_price_units, maximum_yes_bid_units)
                )

        current_no_price_units = self.orders["no"].price_units
        if not no_quote_allowed:
            desired_no_units = None
        elif current_no_price_units is None:
            desired_no_units = min(best_no_bid_units, maximum_no_bid_units)
        else:
            if no_other_best_units is None:
                desired_no_units = min(current_no_price_units, maximum_no_bid_units)
            else:
                target_no_units = self.choose_target_from_other_best(
                    other_best_units=no_other_best_units,
                    cap_price_units=maximum_no_bid_units,
                    allow_aggressive_improvement=allow_aggressive_improvement,
                )
                desired_no_units = (
                    target_no_units
                    if current_no_price_units < no_other_best_units or current_no_price_units > target_no_units
                    else min(current_no_price_units, maximum_no_bid_units)
                )

        if desired_yes_units is not None:
            desired_yes_units = self.market.price_grid.floor_to_valid(desired_yes_units)
        if desired_no_units is not None:
            desired_no_units = self.market.price_grid.floor_to_valid(desired_no_units)

        if desired_yes_units is not None and desired_no_units is not None:
            inventory_skew_ticks = self.current_inventory_skew_ticks()
            desired_yes_units = self.shift_for_inventory_skew("yes", desired_yes_units, inventory_skew_ticks)
            desired_no_units = self.shift_for_inventory_skew("no", desired_no_units, inventory_skew_ticks)

            desired_yes_units = min(desired_yes_units, maximum_yes_bid_units)
            desired_no_units = min(desired_no_units, maximum_no_bid_units)

            desired_yes_units, desired_no_units = self.clamp_pair_to_avoid_self_cross(desired_yes_units, desired_no_units)
            desired_yes_units, desired_no_units = self.apply_pair_guard(desired_yes_units, desired_no_units)

        return desired_yes_units, desired_no_units

    # -------------------------
    # Sizing
    # -------------------------

    def target_budget_cents_for_side(self, side: str) -> int:
        return self.settings.yes_order_budget_cents if side == "yes" else self.settings.no_order_budget_cents

    def budget_based_order_size_units(self, side: str, price_units: int) -> int:
        budget_cents = max(0, self.target_budget_cents_for_side(side) - self.settings.budget_fee_buffer_cents)
        if budget_cents <= 0:
            return 0

        budget_money_units = budget_cents * PRICE_UNITS_PER_CENT
        quantity_units = (budget_money_units * COUNT_SCALE) // price_units
        maximum_count_units = contracts_to_count_units(self.settings.maximum_contracts_per_order)
        quantity_units = min(quantity_units, maximum_count_units)

        if quantity_units <= 0:
            return 0

        if self.market.fractional_trading_enabled and self.settings.allow_fractional_order_entry_when_supported:
            return int(quantity_units)

        # Whole-contract default.
        quantity_units = (quantity_units // COUNT_SCALE) * COUNT_SCALE
        return int(quantity_units)

    def desired_cycle_total_fillable_units(self, side: str, price_units: int, quote_cycle_filled_units: int) -> int:
        budget_size_units = self.budget_based_order_size_units(side, price_units)
        if budget_size_units <= 0:
            return 0

        if self.settings.refill_resting_size_after_partial_fill:
            return quote_cycle_filled_units + budget_size_units

        # Preserve the older behavior: keep the total max fillable size capped
        # at the budget target. As fills occur, the resting size shrinks.
        return max(quote_cycle_filled_units, budget_size_units)

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

        try:
            await asyncio.to_thread(self.api_client.cancel_order, order_id=order_id)
            log_event("CANCEL_REQUESTED", side=side, reason=reason, order_id=order_id)
        except Exception as exc:
            if is_order_not_found_error(exc):
                log_event("CANCEL_SKIPPED_ORDER_MISSING", side=side, reason=reason, order_id=order_id)
            else:
                raise

        state.clear_active_order(preserve_quote_cycle=not reset_quote_cycle)

    async def ensure_side_quote(self, side: str, desired_price_units: Optional[int]) -> None:
        state = self.orders[side]

        if desired_price_units is None:
            await self.cancel_side_quote(side, reason="quote_disabled", reset_quote_cycle=True)
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
                if (
                    state.price_units == attempt_price_units
                    and current_order_total_fillable_units == desired_current_order_total_fillable_units
                    and not self.order_needs_expiration_refresh(state)
                ):
                    return

            try:
                if not state.has_active_resting_order:
                    new_client_order_id = f"{self.settings.primary_client_order_prefix}:{side}:{uuid.uuid4().hex[:16]}"
                    expiration_ts = (
                        utc_seconds_to_expiration_timestamp(self.settings.resting_order_expiration_seconds)
                        if self.settings.resting_order_expiration_seconds > 0
                        else None
                    )
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
                    state.client_order_id = order_payload.get("client_order_id") or new_client_order_id
                    state.price_units = attempt_price_units
                    state.remaining_count_units = desired_remaining_units
                    state.current_order_filled_units = 0
                    state.status = "resting"
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
                state.client_order_id = amended_order_payload.get("client_order_id") or updated_client_order_id
                state.price_units = attempt_price_units
                state.remaining_count_units = desired_remaining_units
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
                    log_event(
                        "STALE_ORDER_RECOVER",
                        side=side,
                        order_id=state.order_id,
                    )
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
                        log_event(
                            "POST_ONLY_COOLDOWN",
                            side=side,
                            seconds=self.settings.post_only_reprice_cooldown_seconds,
                        )
                        await asyncio.sleep(self.settings.post_only_reprice_cooldown_seconds)
                    continue

                raise

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

    def handle_user_order_update(self, order_message: dict) -> None:
        if order_message.get("ticker") != self.settings.market_ticker:
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
            # A new order was created for the same side. Keep quote-cycle fill totals,
            # but reset the per-order tracking to the fresh order's fields.
            state.order_id = incoming_order_id
            state.current_order_filled_units = 0
            state.remaining_count_units = 0

        if incoming_order_id:
            state.order_id = incoming_order_id
        state.client_order_id = client_order_id
        state.status = incoming_status

        fill_units = parse_optional_count_units(order_message, "fill_count_fp", "fill_count")
        if fill_units is not None:
            fill_units = int(fill_units)
            fill_delta_units = fill_units - state.current_order_filled_units
            if fill_delta_units > 0:
                state.quote_cycle_filled_units += fill_delta_units
                self.last_fill_timestamp_ms = now_ms()
                if side == "yes":
                    self.net_position_units += fill_delta_units
                else:
                    self.net_position_units -= fill_delta_units
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
            log_event(
                "ORDER_NOT_RESTING",
                side=side,
                status=state.status,
                order_id=state.order_id,
            )
            state.clear_active_order(preserve_quote_cycle=preserve_cycle)

        self.requote_event.set()

    # -------------------------
    # Async workers
    # -------------------------

    async def requote_worker(self) -> None:
        while True:
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
                    continue

                try:
                    response = await asyncio.to_thread(self.api_client.get_order_queue_position, state.order_id)
                    queue_position_units = parse_optional_count_units(response, "queue_position_fp", "queue_position")
                    state.last_queue_position_units = queue_position_units
                    log_event(
                        "QUEUE_POSITION",
                        side=side,
                        order_id=state.order_id,
                        ahead_contracts=(format_count_fp(queue_position_units) if queue_position_units is not None else "unknown"),
                    )
                except Exception as exc:
                    log_event("QUEUE_POSITION_ERROR", side=side, order_id=state.order_id, error=str(exc))

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
                    backoff_seconds = 1
                    self.book_ready = False
                    self.last_orderbook_sequence = None

                    subscriptions = [
                        {
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_ticker": self.settings.market_ticker,
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
                    ]

                    if self.settings.subscribe_to_market_positions_channel:
                        subscriptions.append(
                            {
                                "id": 3,
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

                        elif message_type == "market_position":
                            self.handle_market_position_update(data.get("msg", {}))
                            self.requote_event.set()

                        elif message_type == "error":
                            log_event("WS_ERROR", payload=data)

            except Exception as exc:
                log_event("WS_DISCONNECT", error=str(exc), reconnect_backoff_seconds=backoff_seconds)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(30, backoff_seconds * 2)

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
        )

        # Startup synchronization is intentionally REST-based and synchronous so
        # the strategy starts from a correct inventory baseline.
        self.load_startup_position()
        self.cancel_owned_resting_quotes_on_startup()

        asyncio.create_task(self.requote_worker())
        asyncio.create_task(self.queue_position_worker())

        await self.websocket_main()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_market_metadata(api_client: KalshiApiClient, market_ticker: str) -> MarketMetadata:
    market_payload = api_client.get_market(market_ticker)
    price_grid = PriceGrid.from_market_response(market_payload)

    return MarketMetadata(
        ticker=str(market_payload.get("ticker") or market_ticker),
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
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
