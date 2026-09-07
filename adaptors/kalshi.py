"""Kalshi implementation of the venue-neutral client interface."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import AsyncIterator, Iterable, List, Optional, Sequence

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from clients.base_client import BaseClient
from clients.http_client import HTTPClient, HTTPClientError
from clients.models import (
    AccountBalance,
    AccountFill,
    AccountFillQuery,
    AccountLimits,
    AccountOrder,
    AccountOrderQuery,
    AccountPosition,
    AccountPositionQuery,
    AmendOrderRequest,
    AmendTargetUnavailableError,
    ClientError,
    CreateOrderRequest,
    Fill,
    IncentiveProgram,
    InsufficientBalanceError,
    Market,
    MarketEvent,
    MarketQuery,
    MarketQuote,
    Order,
    OrderBookDelta,
    OrderBookSnapshot,
    OrderNotFoundError,
    OrderUpdate,
    Position,
    PositionUpdate,
    PostOnlyCrossError,
    PriceRangeData,
    PublicTrade,
    QueuePosition,
    RateLimitError,
    RateLimitBucket,
    Series,
    SeriesFeeChange,
    StreamReset,
    TickerUpdate,
)
from clients.websocket_client import WebsocketClient


LOGGER = logging.getLogger("kalshi_top_of_book_bot")

PRICE_SCALE = 10_000
COUNT_SCALE = 100
PRICE_UNITS_PER_CENT = 100
# Upper bound on remembered order -> shard/ticker routes (oldest entries evicted).
_ORDER_ROUTE_CACHE_LIMIT = 200_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _timestamp_ms(value: object) -> int:
    if value in (None, ""):
        return _now_ms()
    try:
        numeric = int(value)
        return numeric * 1000 if numeric < 10_000_000_000 else numeric
    except Exception:
        parsed = _optional_timestamp_ms(str(value))
        return int(parsed or _now_ms())


def _optional_timestamp_ms(value: object) -> Optional[int]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        numeric = float(text)
        if numeric >= 0:
            return int(numeric if numeric >= 10_000_000_000 else numeric * 1000)
    except ValueError:
        pass
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except Exception:
        return None


def _price_units_from_dollars(value: object) -> int:
    scaled = Decimal(str(value)) * PRICE_SCALE
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def _count_units_from_fp(value: object) -> int:
    scaled = Decimal(str(value)) * COUNT_SCALE
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def _optional_price(payload: dict, fixed_field: str, legacy_field: str) -> Optional[int]:
    if payload.get(fixed_field) not in (None, ""):
        return _price_units_from_dollars(payload[fixed_field])
    if payload.get(legacy_field) not in (None, ""):
        scaled = Decimal(str(payload[legacy_field])) * PRICE_UNITS_PER_CENT
        return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))
    return None


def _optional_count(payload: dict, fixed_field: str, legacy_field: str) -> Optional[int]:
    if payload.get(fixed_field) not in (None, ""):
        return _count_units_from_fp(payload[fixed_field])
    if payload.get(legacy_field) not in (None, ""):
        scaled = Decimal(str(payload[legacy_field])) * COUNT_SCALE
        return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))
    return None


def _optional_money(payload: dict, fixed_field: str, legacy_field: str = "") -> Optional[int]:
    if payload.get(fixed_field) not in (None, ""):
        return _price_units_from_dollars(payload[fixed_field])
    if legacy_field and payload.get(legacy_field) not in (None, ""):
        scaled = Decimal(str(payload[legacy_field])) * PRICE_UNITS_PER_CENT
        return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))
    return None


def _optional_exchange_index(value: object) -> Optional[int]:
    """Parse an `exchange_index` field; None when absent or malformed."""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _outcome_side(payload: dict, fallback: Optional[str] = None) -> Optional[str]:
    outcome = str(payload.get("outcome_side") or "").lower()
    if outcome in {"yes", "no"}:
        return outcome
    book_side = str(payload.get("book_side") or "").lower()
    if book_side in {"bid", "ask"}:
        return "yes" if book_side == "bid" else "no"
    action = str(payload.get("action") or "").lower()
    legacy_side = str(payload.get("side") or "").lower()
    if action in {"buy", "sell"} and legacy_side in {"yes", "no"}:
        if (action, legacy_side) in {("buy", "yes"), ("sell", "no")}:
            return "yes"
        return "no"
    if legacy_side in {"yes", "no"}:
        return legacy_side
    return fallback if fallback in {"yes", "no"} else None


def _book_levels_from_payload(payload: dict, fixed_field: str, legacy_field: str) -> dict[int, int]:
    levels: dict[int, int] = {}
    fixed = payload.get(fixed_field)
    if fixed is not None:
        for price, count in fixed:
            levels[_price_units_from_dollars(price)] = _count_units_from_fp(count)
    else:
        for price, count in payload.get(legacy_field) or []:
            levels[int(price) * PRICE_UNITS_PER_CENT] = int(count) * COUNT_SCALE
    return levels


def parse_wire_event(data: dict, market_id: str) -> Optional[MarketEvent]:
    """Normalize one decoded Kalshi websocket message into a ``MarketEvent``.

    Pure function shared by the live client (``KalshiApiClient._event``) and
    the Tier-2 replay loader, so recorded raw messages replay through exactly
    the parsing the production bot sees. ``market_id`` is the fallback ticker
    when the payload carries none. Returns ``None`` for unknown or malformed
    messages.
    """
    event_type = data.get("type")
    payload = data.get("msg") or {}
    event_market = str(payload.get("market_ticker") or payload.get("ticker") or market_id)
    sequence = data.get("seq")
    if event_type == "orderbook_snapshot":
        return OrderBookSnapshot(
            event_market,
            sequence,
            _book_levels_from_payload(payload, "yes_dollars_fp", "yes"),
            _book_levels_from_payload(payload, "no_dollars_fp", "no"),
            venue="kalshi", native_market_id=event_market,
        )
    if event_type == "orderbook_delta":
        side = str(payload.get("side") or "")
        price = _optional_price(payload, "price_dollars", "price")
        count = _optional_count(payload, "delta_fp", "delta")
        if side not in {"yes", "no"} or price is None or count is None:
            return None
        return OrderBookDelta(event_market, sequence, side, price, count, _timestamp_ms(payload.get("ts")), venue="kalshi", native_market_id=event_market)
    if event_type == "user_order":
        side = _outcome_side(payload)
        if side not in {"yes", "no"} and "is_yes" in payload:
            side = "yes" if bool(payload.get("is_yes")) else "no"
        if side not in {"yes", "no"}:
            return None
        price = _optional_price(payload, "yes_price_dollars", "yes_price") if side == "yes" else _optional_price(payload, "no_price_dollars", "no_price")
        return OrderUpdate(
            event_market,
            side,
            str(payload.get("order_id") or ""),
            str(payload.get("client_order_id") or ""),
            str(payload.get("status") or ""),
            _optional_count(payload, "fill_count_fp", "fill_count"),
            _optional_count(payload, "remaining_count_fp", "remaining_count"),
            price,
            _optional_timestamp_ms(payload.get("expiration_time")),
            venue="kalshi", native_market_id=event_market,
        )
    if event_type == "fill":
        return Fill(
            event_market,
            str(payload.get("order_id") or ""),
            str(payload.get("trade_id") or ""),
            _timestamp_ms(payload.get("ts")),
            int(_optional_count(payload, "count_fp", "count") or 0),
            _optional_price(payload, "yes_price_dollars", "yes_price"),
            _optional_price(payload, "no_price_dollars", "no_price"),
            _optional_price(payload, "fee_cost", "fee"),
            _optional_count(payload, "post_position_fp", "post_position"),
            bool(payload.get("is_taker")),
            venue="kalshi", native_market_id=event_market,
        )
    if event_type == "trade":
        yes_price = _optional_price(payload, "yes_price_dollars", "yes_price")
        no_price = _optional_price(payload, "no_price_dollars", "no_price")
        count = _optional_count(payload, "count_fp", "count")
        if yes_price is None and no_price is not None:
            yes_price = PRICE_SCALE - no_price
        if no_price is None and yes_price is not None:
            no_price = PRICE_SCALE - yes_price
        if yes_price is None or no_price is None or count is None:
            return None
        return PublicTrade(event_market, str(payload.get("trade_id") or ""), _timestamp_ms(payload.get("ts")), yes_price, no_price, count, str(payload.get("taker_side") or ""), venue="kalshi", native_market_id=event_market)
    if event_type == "ticker":
        return TickerUpdate(
            event_market,
            _timestamp_ms(payload.get("ts") or payload.get("time")),
            _optional_price(payload, "price_dollars", "price"),
            _optional_price(payload, "yes_bid_dollars", "yes_bid"),
            _optional_price(payload, "yes_ask_dollars", "yes_ask"),
            _optional_count(payload, "volume_fp", "volume"),
            _optional_count(payload, "open_interest_fp", "open_interest"),
            _optional_count(payload, "yes_bid_size_fp", "yes_bid_size"),
            _optional_count(payload, "yes_ask_size_fp", "yes_ask_size"),
            _optional_count(payload, "last_trade_size_fp", "last_trade_size"),
            venue="kalshi", native_market_id=event_market,
        )
    if event_type == "market_position":
        position = _optional_count(payload, "position_fp", "position")
        return PositionUpdate(event_market, int(position), venue="kalshi", native_market_id=event_market) if position is not None else None
    if event_type == "error":
        LOGGER.error("WS_ERROR | payload=%s", data)
    return None


def _format_price(price_units: int) -> str:
    value = (Decimal(price_units) / Decimal(PRICE_SCALE)).quantize(Decimal("0.0001"))
    return format(value, "f")


def _format_count(count_units: int) -> str:
    value = (Decimal(count_units) / Decimal(COUNT_SCALE)).quantize(Decimal("0.00"))
    return format(value, "f")


def _translate_order(side: str, action: str, price_units: int) -> tuple[str, int]:
    if side == "yes":
        return ("bid" if action == "buy" else "ask"), price_units
    if side == "no":
        return ("ask" if action == "buy" else "bid"), PRICE_SCALE - price_units
    raise ValueError(f"Unknown order side: {side!r}")


class _CrossProcessFileLock:
    def __init__(self, path: str) -> None:
        self.path = path
        self._file = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._file = open(self.path, "a+", encoding="utf-8")
        self._file.seek(0)
        if not self._file.read():
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


class _SharedWriteRateLimiter:
    def __init__(self, *, path: str, writes_per_second: float, burst_capacity: int, enabled: bool) -> None:
        self.path = path
        self.writes_per_second = float(writes_per_second)
        self.burst_capacity = int(burst_capacity)
        self.enabled = bool(enabled)

    def _default(self) -> dict:
        return {
            "tokens_available": float(self.burst_capacity),
            "last_refill_epoch_seconds": time.time(),
            "blocked_until_epoch_seconds": 0.0,
        }

    def _load(self, file) -> dict:
        file.seek(0)
        try:
            state = json.loads(file.read().strip() or "{}")
        except json.JSONDecodeError:
            state = {}
        default = self._default()
        return {key: float(state.get(key, value)) for key, value in default.items()}

    @staticmethod
    def _save(file, state: dict) -> None:
        file.seek(0)
        file.truncate(0)
        file.write(json.dumps(state, separators=(",", ":")))
        file.flush()
        os.fsync(file.fileno())

    def acquire(self, action: str) -> None:
        if not self.enabled:
            return
        while True:
            sleep_seconds = 0.0
            with _CrossProcessFileLock(self.path) as file:
                now = time.time()
                state = self._load(file)
                if state["blocked_until_epoch_seconds"] > now:
                    sleep_seconds = max(0.05, state["blocked_until_epoch_seconds"] - now)
                else:
                    elapsed = max(0.0, now - state["last_refill_epoch_seconds"])
                    tokens = min(self.burst_capacity, state["tokens_available"] + elapsed * self.writes_per_second)
                    state["last_refill_epoch_seconds"] = now
                    if tokens >= 1:
                        state["tokens_available"] = tokens - 1
                        state["blocked_until_epoch_seconds"] = 0.0
                        self._save(file, state)
                        return
                    state["tokens_available"] = tokens
                    sleep_seconds = max(0.05, (1.0 - tokens) / self.writes_per_second)
                self._save(file, state)
            LOGGER.info("SHARED_WRITE_LIMIT_WAIT | action=%s seconds=%.3f", action, sleep_seconds)
            time.sleep(sleep_seconds)

    def cooldown(self, seconds: float) -> None:
        if not self.enabled or seconds <= 0:
            return
        with _CrossProcessFileLock(self.path) as file:
            now = time.time()
            state = self._load(file)
            state["blocked_until_epoch_seconds"] = max(state["blocked_until_epoch_seconds"], now + seconds)
            state["last_refill_epoch_seconds"] = now
            self._save(file, state)


@dataclass(frozen=True)
class KalshiClientConfig:
    api_key_id: str = ""
    private_key_path: str = ""
    public_only: bool = False
    rest_base_url: str = ""
    api_prefix: str = "/trade-api/v2"
    websocket_url: str = ""
    use_demo_environment: bool = False
    dry_run: bool = False
    subaccount_number: int = 0
    post_only_quotes: bool = True
    cancel_quotes_if_exchange_pauses: bool = False
    self_trade_prevention_type: str = "taker_at_cross"
    enable_shared_write_rate_limiter: bool = True
    shared_write_rate_limit_writes_per_second: float = 25
    shared_write_rate_limit_burst_capacity: int = 8
    shared_write_rate_limiter_directory: str = ""
    global_rate_limit_backoff_seconds: float = 1.0
    # Per-request REST bounds (seconds).  ``rest_timeout_seconds`` caps every
    # socket read, ``rest_connect_timeout_seconds`` caps the connect phase.
    rest_timeout_seconds: float = 15.0
    rest_connect_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        if self.subaccount_number < 0:
            raise ValueError("subaccount_number must be >= 0")
        if self.rest_timeout_seconds <= 0 or self.rest_connect_timeout_seconds <= 0:
            raise ValueError("rest timeouts must be > 0")
        if self.shared_write_rate_limit_writes_per_second <= 0:
            raise ValueError("shared_write_rate_limit_writes_per_second must be > 0")
        if self.shared_write_rate_limit_burst_capacity <= 0:
            raise ValueError("shared_write_rate_limit_burst_capacity must be > 0")
        if self.global_rate_limit_backoff_seconds < 0:
            raise ValueError("global_rate_limit_backoff_seconds must be >= 0")


class KalshiApiClient(BaseClient):
    venue_name = "kalshi"
    venue = "kalshi"

    def __init__(
        self,
        config: KalshiClientConfig,
        *,
        http_client: Optional[HTTPClient] = None,
        websocket_client: Optional[WebsocketClient] = None,
        private_key: Optional[object] = None,
    ) -> None:
        self.config = config
        self.environment_name = "demo" if config.use_demo_environment else "prod"
        self.dry_run = config.dry_run
        default_host = "https://external-api.demo.kalshi.co" if config.use_demo_environment else "https://external-api.kalshi.com"
        self.host = config.rest_base_url.rstrip("/") or default_host
        websocket_host = "wss://external-api-ws.demo.kalshi.co" if config.use_demo_environment else "wss://external-api-ws.kalshi.com"
        self.api_prefix = "/" + config.api_prefix.strip("/")
        self.websocket_path = "/trade-api/ws/v2"
        self.websocket_url = config.websocket_url or (websocket_host + self.websocket_path)
        self.rate_limit_backoff_seconds = float(config.global_rate_limit_backoff_seconds)

        if not config.api_key_id and not config.public_only:
            raise ValueError("A valid Kalshi API key ID is required")
        if private_key is None:
            if config.public_only:
                private_key = None
            elif not config.private_key_path:
                raise ValueError("A valid Kalshi private key path is required")
            elif not os.path.exists(config.private_key_path):
                raise FileNotFoundError(f"Private key file not found: {config.private_key_path}")
            else:
                with open(config.private_key_path, "rb") as key_file:
                    private_key = serialization.load_pem_private_key(key_file.read(), password=None)
        self.private_key = private_key

        http = http_client or HTTPClient(
            self.host,
            timeout_seconds=config.rest_timeout_seconds,
            connect_timeout_seconds=config.rest_connect_timeout_seconds,
        )
        websocket = websocket_client or WebsocketClient(self.websocket_url)
        super().__init__(http_client=http, websocket_client=websocket)

        limiter_dir = config.shared_write_rate_limiter_directory or tempfile.gettempdir()
        namespace = hashlib.sha1(
            f"{self.host}|{config.api_key_id}|{config.subaccount_number}".encode("utf-8")
        ).hexdigest()[:16]
        self._write_limiter = _SharedWriteRateLimiter(
            path=os.path.join(limiter_dir, f"kalshi_write_limiter_{namespace}.json"),
            writes_per_second=config.shared_write_rate_limit_writes_per_second,
            burst_capacity=config.shared_write_rate_limit_burst_capacity,
            enabled=config.enable_shared_write_rate_limiter and not config.dry_run,
        )
        self._closed = False
        # Exchange-shard routing caches (see _order_route for the doc citations).
        self._exchange_index_by_market: dict[str, int] = {}
        self._exchange_index_by_order: dict[str, int] = {}
        self._market_by_order: dict[str, str] = {}
        self._unrouted_order_warnings: set[str] = set()

    def _record_stream_activity(self, event: str, *, event_type: Optional[str] = None) -> None:
        monitor = getattr(self.websocket_client, "activity", None)
        if monitor is not None:
            monitor.record_stream(event, event_type=event_type)

    def sign_message(self, message_bytes: bytes) -> str:
        self._require_authentication()
        signature = self.private_key.sign(
            message_bytes,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        if self.config.public_only:
            return {"Content-Type": "application/json"}
        timestamp = str(_now_ms())
        signed_path = path.split("?")[0]
        message = f"{timestamp}{method.upper()}{signed_path}".encode("utf-8")
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.config.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": self.sign_message(message),
        }

    def websocket_headers(self) -> dict:
        self._require_authentication()
        headers = self._headers("GET", self.websocket_path)
        headers.pop("Content-Type", None)
        return headers

    def _require_authentication(self) -> None:
        if self.config.public_only or not self.config.api_key_id or self.private_key is None:
            raise ClientError("This Kalshi client is configured for public market data only")

    def _map_error(self, exc: HTTPClientError, *, action: str = "") -> ClientError:
        text = exc.response_text.lower()
        if exc.status_code == 429 or "too_many_requests" in text or "rate limit" in text:
            self._write_limiter.cooldown(self.rate_limit_backoff_seconds)
            return RateLimitError(str(exc))
        if "post_only_cross" in text or "post only cross" in text or "post-only cross" in text:
            return PostOnlyCrossError(str(exc))
        if (
            "insufficient_balance" in text or "insufficient balance" in text
            or "insufficient_funds" in text or "insufficient funds" in text
        ):
            # The venue locks collateral only when an order rests and rejects
            # writes beyond the shard balance; the bot backs off per market.
            return InsufficientBalanceError(str(exc))
        missing = exc.status_code == 404 or "not_found" in text or "order_not_found" in text or "not found" in text
        non_resting = "not_resting" in text or "not resting" in text
        if action == "amend_order" and (missing or non_resting):
            return AmendTargetUnavailableError(str(exc))
        if missing:
            return OrderNotFoundError(str(exc))
        return ClientError(str(exc))

    def _get(self, path: str, *, params: Optional[dict] = None, operation: str = "get") -> dict:
        try:
            return self.http_client.get(
                path, headers=self._headers("GET", path), params=params, operation=operation
            )
        except HTTPClientError as exc:
            raise self._map_error(exc) from exc

    def _post(self, path: str, body: dict, *, action: str, params: Optional[dict] = None) -> dict:
        self._write_limiter.acquire(action)
        try:
            return self.http_client.post(
                path, headers=self._headers("POST", path), body=body, params=params, operation=action
            )
        except HTTPClientError as exc:
            raise self._map_error(exc, action=action) from exc

    def _delete(self, path: str, *, action: str, params: Optional[dict] = None) -> dict:
        self._write_limiter.acquire(action)
        try:
            return self.http_client.delete(
                path, headers=self._headers("DELETE", path), params=params, operation=action
            )
        except HTTPClientError as exc:
            raise self._map_error(exc, action=action) from exc

    # ------------------------------------------------------------------
    # Exchange sharding (docs.kalshi.com/getting_started/exchange_sharding)
    #
    # Kalshi runs one matching engine per shard (0 default, 1 exotics/combos,
    # 2 crypto, 3 tennis+baseball since 2026-08-24).  Balances are local to a
    # shard and an order write must reach the shard hosting its market, or it
    # is rejected (user_not_found on an unfunded shard).
    #
    # Doc-verified parameter placement (docs.kalshi.com/api-reference/orders/*-v2):
    #   create   POST   /portfolio/events/orders                exchange_index -> JSON body (integer)
    #   amend    POST   /portfolio/events/orders/{id}/amend     exchange_index -> JSON body (integer)
    #   decrease POST   /portfolio/events/orders/{id}/decrease  exchange_index, market_ticker -> JSON body
    #   cancel   DELETE /portfolio/events/orders/{id}           exchange_index, market_ticker -> query string
    # Shared semantics quoted from those pages: "Exchange shard index. If
    # omitted, auto-routes when [market_]ticker is provided; otherwise defaults
    # to 0. Use -1 to require auto-routing by [market] ticker."  Cancel/decrease
    # add: market_ticker is the "Market ticker used for auto-routing when
    # exchange_index is omitted or -1."  Explicit (>= 0) targeting is billed
    # only against that shard's write budget, while auto-routed writes are
    # billed against every nonzero shard's budget, so explicit is preferred.
    # ------------------------------------------------------------------

    def exchange_index_for_market(self, market_id: str) -> Optional[int]:
        """Shard index last reported by the venue for `market_id`, if seen."""
        return self._exchange_index_by_market.get(str(market_id))

    def _remember_market_shard(self, payload: dict) -> None:
        ticker = str(payload.get("ticker") or "")
        index = _optional_exchange_index(payload.get("exchange_index"))
        # Only an explicit index is trusted for direct routing.  A payload that
        # omits the field is *not* recorded as shard 0: writes for that market
        # fall back to venue auto-routing (-1), which can never mis-route.
        if ticker and index is not None and index >= 0:
            self._exchange_index_by_market[ticker] = index

    def _normalize_market(self, payload: dict, fallback_id: str) -> Market:
        self._remember_market_shard(payload)
        return self._market(payload, fallback_id)

    def _remember_order_route(
        self, order_id: str, market_id: str = "", exchange_index: Optional[int] = None
    ) -> None:
        order_id = str(order_id or "")
        if not order_id or order_id.startswith("DRY-"):
            return
        if market_id:
            self._market_by_order[order_id] = str(market_id)
        if exchange_index is not None and exchange_index >= 0:
            self._exchange_index_by_order[order_id] = int(exchange_index)
        for cache in (self._market_by_order, self._exchange_index_by_order):
            while len(cache) > _ORDER_ROUTE_CACHE_LIMIT:
                cache.pop(next(iter(cache)))

    def _order_route(self, order_id: str, *, action: str) -> dict:
        """Routing fields for an order-id-only write (cancel / decrease).

        Resolution order: shard remembered for the order -> shard remembered
        for the order's market -> auto-route by remembered ticker (-1) ->
        omit.  Per the docs, -1 "require[s] auto-routing by market ticker", so
        without a ticker it is not sent; omitting both lets the venue default
        to shard 0, which is logged once per order.
        """
        order_id = str(order_id)
        market_id = self._market_by_order.get(order_id, "")
        index = self._exchange_index_by_order.get(order_id)
        if index is None and market_id:
            index = self._exchange_index_by_market.get(market_id)
        route: dict = {}
        if market_id:
            route["market_ticker"] = market_id
            route["exchange_index"] = index if index is not None else -1
        elif index is not None:
            route["exchange_index"] = index
        elif order_id not in self._unrouted_order_warnings:
            if len(self._unrouted_order_warnings) >= _ORDER_ROUTE_CACHE_LIMIT:
                self._unrouted_order_warnings.clear()
            self._unrouted_order_warnings.add(order_id)
            LOGGER.warning(
                "ORDER_ROUTE_UNKNOWN | action=%s order_id=%s | no cached market ticker or exchange_index; "
                "venue will default this write to exchange index 0",
                action,
                order_id,
            )
        return route

    @staticmethod
    def _market(payload: dict, fallback_id: str) -> Market:
        ranges: List[PriceRangeData] = []
        for item in payload.get("price_ranges") or []:
            ranges.append(
                PriceRangeData(
                    start_units=_price_units_from_dollars(item["start"]),
                    end_units=_price_units_from_dollars(item["end"]),
                    step_units=_price_units_from_dollars(item["step"]),
                )
            )
        close_time = None
        for key in ("close_time", "close_date", "expiration_time", "expiration_date", "settlement_time"):
            close_time = _optional_timestamp_ms(payload.get(key))
            if close_time is not None:
                break
        market_id = str(payload.get("ticker") or fallback_id)
        event_id = str(payload.get("event_ticker") or payload.get("event") or payload.get("event_name") or market_id)
        derived_series_id = event_id.split("-", 1)[0] if "-" in event_id else event_id
        series_id = str(payload.get("series_ticker") or payload.get("series") or payload.get("series_name") or derived_series_id)
        return Market(
            market_id=market_id,
            venue="kalshi",
            native_market_id=market_id,
            title=str(payload.get("title") or ""),
            status=str(payload.get("status") or ""),
            series_id=series_id,
            event_id=event_id,
            close_time_ms=close_time,
            price_level_structure=str(payload.get("price_level_structure") or ""),
            fractional_trading_enabled=bool(payload.get("fractional_trading_enabled")),
            price_ranges=tuple(ranges),
            legacy_tick_size_units=max(1, int(payload.get("tick_size") or 1) * PRICE_UNITS_PER_CENT),
            yes_bid_units=_optional_price(payload, "yes_bid_dollars", "yes_bid"),
            yes_ask_units=_optional_price(payload, "yes_ask_dollars", "yes_ask"),
            no_bid_units=_optional_price(payload, "no_bid_dollars", "no_bid"),
            no_ask_units=_optional_price(payload, "no_ask_dollars", "no_ask"),
            last_price_units=_optional_price(payload, "last_price_dollars", "last_price"),
            yes_bid_size_units=_optional_count(payload, "yes_bid_size_fp", "yes_bid_size"),
            yes_ask_size_units=_optional_count(payload, "yes_ask_size_fp", "yes_ask_size"),
            no_bid_size_units=_optional_count(payload, "no_bid_size_fp", "no_bid_size"),
            no_ask_size_units=_optional_count(payload, "no_ask_size_fp", "no_ask_size"),
            volume_24h_units=_optional_count(payload, "volume_24h_fp", "volume_24h"),
            open_interest_units=_optional_count(payload, "open_interest_fp", "open_interest"),
            expected_expiration_time_ms=_optional_timestamp_ms(payload.get("expected_expiration_time")),
            expiration_time_ms=_optional_timestamp_ms(payload.get("expiration_time")),
            market_url=None,
            series_title=str(payload.get("series_title") or ""),
            exchange_index=_optional_exchange_index(payload.get("exchange_index")) or 0,
        )

    @staticmethod
    def _order(payload: dict, *, fallback_id: str = "", side: Optional[str] = None) -> Order:
        resolved_side = _outcome_side(payload, side)
        price = None
        if resolved_side == "yes":
            price = _optional_price(payload, "yes_price_dollars", "yes_price")
        elif resolved_side == "no":
            price = _optional_price(payload, "no_price_dollars", "no_price")
        return Order(
            order_id=str(payload.get("order_id") or fallback_id),
            market_id=str(payload.get("ticker") or payload.get("market_ticker") or ""),
            venue="kalshi",
            native_market_id=str(payload.get("ticker") or payload.get("market_ticker") or ""),
            side=resolved_side if resolved_side in {"yes", "no"} else None,
            client_order_id=str(payload.get("client_order_id") or ""),
            status=str(payload.get("status") or ""),
            price_units=price,
            fill_count_units=int(_optional_count(payload, "fill_count_fp", "fill_count") or 0),
            remaining_count_units=int(_optional_count(payload, "remaining_count_fp", "remaining_count") or 0),
            expiration_time_ms=_optional_timestamp_ms(payload.get("expiration_time")),
        )

    @staticmethod
    def _account_order(payload: dict) -> AccountOrder:
        resolved_side = _outcome_side(payload)
        price = None
        if resolved_side == "yes":
            price = _optional_price(payload, "yes_price_dollars", "yes_price")
        elif resolved_side == "no":
            price = _optional_price(payload, "no_price_dollars", "no_price")
        if price is None:
            price = _optional_price(payload, "price_dollars", "price")
        return AccountOrder(
            order_id=str(payload.get("order_id") or ""),
            market_id=str(payload.get("ticker") or payload.get("market_ticker") or ""),
            venue="kalshi",
            native_market_id=str(payload.get("ticker") or payload.get("market_ticker") or ""),
            side=resolved_side if resolved_side in {"yes", "no"} else None,
            client_order_id=str(payload.get("client_order_id") or ""),
            status=str(payload.get("status") or ""),
            price_units=price,
            fill_count_units=int(_optional_count(payload, "fill_count_fp", "fill_count") or 0),
            remaining_count_units=int(_optional_count(payload, "remaining_count_fp", "remaining_count") or 0),
            initial_count_units=int(_optional_count(payload, "initial_count_fp", "initial_count") or 0),
            fill_cost_units=int(
                (_optional_money(payload, "taker_fill_cost_dollars", "taker_fill_cost") or 0)
                + (_optional_money(payload, "maker_fill_cost_dollars", "maker_fill_cost") or 0)
            ),
            fees_units=int(
                (_optional_money(payload, "taker_fees_dollars", "taker_fees") or 0)
                + (_optional_money(payload, "maker_fees_dollars", "maker_fees") or 0)
            ),
            created_at_ms=_optional_timestamp_ms(payload.get("created_time")),
            updated_at_ms=_optional_timestamp_ms(payload.get("last_update_time")),
            expiration_time_ms=_optional_timestamp_ms(payload.get("expiration_time")),
        )

    @staticmethod
    def _account_fill(payload: dict) -> AccountFill:
        resolved_side = _outcome_side(payload)
        price = None
        if resolved_side == "yes":
            price = _optional_price(payload, "yes_price_dollars", "yes_price")
        elif resolved_side == "no":
            price = _optional_price(payload, "no_price_dollars", "no_price")
        if price is None:
            price = _optional_price(payload, "price_dollars", "price")
        return AccountFill(
            fill_id=str(payload.get("fill_id") or payload.get("trade_id") or ""),
            trade_id=str(payload.get("trade_id") or ""),
            order_id=str(payload.get("order_id") or ""),
            market_id=str(payload.get("ticker") or payload.get("market_ticker") or ""),
            venue="kalshi",
            native_market_id=str(payload.get("ticker") or payload.get("market_ticker") or ""),
            side=resolved_side if resolved_side in {"yes", "no"} else None,
            count_units=int(_optional_count(payload, "count_fp", "count") or 0),
            price_units=price,
            fee_units=int(_optional_money(payload, "fee_cost", "fee") or 0),
            created_at_ms=_optional_timestamp_ms(payload.get("created_time") or payload.get("ts")),
            is_taker=bool(payload.get("is_taker")),
        )

    def get_market(self, market_id: str) -> Market:
        response = self._get(f"{self.api_prefix}/markets/{market_id}", operation="get_market")
        return self._normalize_market(response["market"], market_id)

    def list_markets(self, query: MarketQuery) -> List[Market]:
        markets: List[Market] = []
        cursor = ""
        page_size = max(1, min(int(query.page_size), 1_000))
        max_results = max(0, int(query.max_results))
        while max_results <= 0 or len(markets) < max_results:
            params = {"limit": page_size}
            if query.status:
                params["status"] = query.status
            if cursor:
                params["cursor"] = cursor
            params.update({str(key): value for key, value in query.venue_filters.items() if value not in (None, "")})
            response = self._get(f"{self.api_prefix}/markets", params=params, operation="list_markets")
            for payload in response.get("markets") or []:
                markets.append(self._normalize_market(payload, str(payload.get("ticker") or "")))
                if max_results > 0 and len(markets) >= max_results:
                    return markets
            cursor = str(response.get("cursor") or response.get("next_cursor") or "")
            if not cursor:
                return markets
        return markets

    def get_market_quote(self, market_id: str) -> MarketQuote:
        response = self._get(
            f"{self.api_prefix}/markets/{market_id}", operation="get_market_quote"
        )["market"]
        self._remember_market_shard(response)
        return MarketQuote(
            market_id=market_id,
            venue="kalshi",
            native_market_id=market_id,
            yes_bid_units=_optional_price(response, "yes_bid_dollars", "yes_bid"),
            no_bid_units=_optional_price(response, "no_bid_dollars", "no_bid"),
        )

    def get_account_balance(self) -> AccountBalance:
        self._require_authentication()
        response = self._get(
            f"{self.api_prefix}/portfolio/balance",
            params={"subaccount": self.config.subaccount_number},
            operation="get_account_balance",
        )
        available = _optional_money(response, "balance_dollars", "balance")
        portfolio = _optional_money(response, "portfolio_value_dollars", "portfolio_value")
        if available is None or portfolio is None:
            raise ClientError("Kalshi balance response omitted required balance fields")
        # Balances are local to a shard.  `balance_breakdown` items look like
        # {"exchange_index": 3, "balance": "0.0000"}: the per-shard "balance" is
        # a fixed-point dollars string (not legacy cents), so it is routed
        # through the fixed-field money path only.
        breakdown: dict[int, int] = {}
        for item in response.get("balance_breakdown") or []:
            if not isinstance(item, dict):
                continue
            index = _optional_exchange_index(item.get("exchange_index"))
            raw_cash = item.get("balance_dollars")
            if raw_cash in (None, ""):
                raw_cash = item.get("balance")
            cash = _optional_money({"balance_dollars": raw_cash}, "balance_dollars")
            if index is None or cash is None:
                continue
            breakdown[index] = int(cash)
        return AccountBalance(
            available_cash_units=int(available),
            portfolio_value_units=int(portfolio),
            venue="kalshi",
            currency="USD",
            updated_at_ms=_optional_timestamp_ms(response.get("updated_ts")),
            balance_by_exchange=tuple(sorted(breakdown.items())),
        )

    def get_account_limits(self) -> AccountLimits:
        self._require_authentication()
        response = self._get(f"{self.api_prefix}/account/limits", operation="get_account_limits")

        def bucket(name: str) -> RateLimitBucket:
            value = response.get(name) if isinstance(response.get(name), dict) else {}
            return RateLimitBucket(
                refill_rate=int(value.get("refill_rate") or 0),
                bucket_capacity=int(value.get("bucket_capacity") or 0),
            )

        return AccountLimits(str(response.get("usage_tier") or "unknown"), bucket("read"), bucket("write"))

    def list_account_positions(self, query: AccountPositionQuery = AccountPositionQuery()) -> List[AccountPosition]:
        self._require_authentication()
        positions: List[AccountPosition] = []
        cursor = ""
        while True:
            params = {
                "limit": max(1, min(int(query.page_size), 1_000)),
                "subaccount": self.config.subaccount_number,
            }
            if query.nonzero_only:
                params["count_filter"] = "position"
            if cursor:
                params["cursor"] = cursor
            response = self._get(
                f"{self.api_prefix}/portfolio/positions", params=params, operation="list_account_positions"
            )
            for item in response.get("market_positions") or []:
                positions.append(
                    AccountPosition(
                        market_id=str(item.get("ticker") or item.get("market_ticker") or ""),
                        venue="kalshi",
                        native_market_id=str(item.get("ticker") or item.get("market_ticker") or ""),
                        position_units=int(_optional_count(item, "position_fp", "position") or 0),
                        total_traded_units=_optional_money(item, "total_traded_dollars", "total_traded"),
                        market_exposure_units=_optional_money(item, "market_exposure_dollars", "market_exposure"),
                        realized_pnl_units=_optional_money(item, "realized_pnl_dollars", "realized_pnl"),
                        fees_paid_units=_optional_money(item, "fees_paid_dollars", "fees_paid"),
                        resting_order_count=int(item.get("resting_orders_count") or 0),
                        updated_at_ms=_optional_timestamp_ms(item.get("last_updated_ts")),
                    )
                )
            cursor = str(response.get("cursor") or response.get("next_cursor") or "")
            if not cursor:
                return positions

    def list_account_orders(self, query: AccountOrderQuery = AccountOrderQuery()) -> List[AccountOrder]:
        self._require_authentication()
        orders: List[AccountOrder] = []
        cursor = ""
        seen_cursors = set()
        page_size = max(1, min(int(query.page_size), 1_000))
        max_results = max(0, int(query.max_results))
        if max_results == 0:
            return orders
        while len(orders) < max_results:
            params = {
                "limit": min(page_size, max_results - len(orders)),
                "subaccount": self.config.subaccount_number,
            }
            if query.status:
                params["status"] = query.status
            if query.market_id:
                params["ticker"] = query.market_id
            if query.min_created_at_ms is not None:
                params["min_ts"] = int(query.min_created_at_ms // 1000)
            if query.max_created_at_ms is not None:
                params["max_ts"] = int(query.max_created_at_ms // 1000)
            if cursor:
                params["cursor"] = cursor
            response = self._get(
                f"{self.api_prefix}/portfolio/orders", params=params, operation="list_account_orders"
            )
            page = response.get("orders") or []
            if not page:
                return orders
            for item in page:
                order = self._account_order(item)
                # Seeds order -> ticker/shard routing for cancels after a restart.
                self._remember_order_route(
                    order.order_id, order.market_id, _optional_exchange_index(item.get("exchange_index"))
                )
                orders.append(order)
                if len(orders) >= max_results:
                    return orders
            cursor = str(response.get("cursor") or response.get("next_cursor") or "")
            if not cursor or cursor in seen_cursors:
                return orders
            seen_cursors.add(cursor)
        return orders

    def list_account_fills(self, query: AccountFillQuery = AccountFillQuery()) -> List[AccountFill]:
        self._require_authentication()
        fills: List[AccountFill] = []
        cursor = ""
        seen_cursors = set()
        page_size = max(1, min(int(query.page_size), 1_000))
        max_results = max(0, int(query.max_results))
        if max_results == 0:
            return fills
        while len(fills) < max_results:
            params = {
                "limit": min(page_size, max_results - len(fills)),
                "subaccount": self.config.subaccount_number,
            }
            if query.market_id:
                params["ticker"] = query.market_id
            if query.order_id:
                params["order_id"] = query.order_id
            if query.min_created_at_ms is not None:
                params["min_ts"] = int(query.min_created_at_ms // 1000)
            if query.max_created_at_ms is not None:
                params["max_ts"] = int(query.max_created_at_ms // 1000)
            if cursor:
                params["cursor"] = cursor
            response = self._get(
                f"{self.api_prefix}/portfolio/fills", params=params, operation="list_account_fills"
            )
            page = response.get("fills") or []
            if not page:
                return fills
            for item in page:
                fills.append(self._account_fill(item))
                if len(fills) >= max_results:
                    return fills
            cursor = str(response.get("cursor") or response.get("next_cursor") or "")
            if not cursor or cursor in seen_cursors:
                return fills
            seen_cursors.add(cursor)
        return fills

    def get_positions(self, market_id: str) -> List[Position]:
        self._require_authentication()
        response = self._get(
            f"{self.api_prefix}/portfolio/positions",
            params={"ticker": market_id, "subaccount": self.config.subaccount_number, "limit": 1},
            operation="get_positions",
        )
        return [
            Position(
                str(item.get("ticker") or item.get("market_ticker") or market_id),
                int(_optional_count(item, "position_fp", "position") or 0),
            )
            for item in response.get("market_positions") or []
        ]

    def get_resting_orders(self, market_id: str) -> List[Order]:
        account_orders = self.list_account_orders(AccountOrderQuery(status="resting", market_id=market_id, page_size=200))
        return [
            Order(
                order_id=item.order_id,
                market_id=item.market_id,
                side=item.side,
                client_order_id=item.client_order_id,
                status=item.status,
                price_units=item.price_units,
                fill_count_units=item.fill_count_units,
                remaining_count_units=item.remaining_count_units,
                expiration_time_ms=item.expiration_time_ms,
            )
            for item in account_orders
        ]

    def create_order(self, request: CreateOrderRequest) -> Order:
        self._require_authentication()
        book_side, yes_price = _translate_order(request.side, request.action, request.price_units)
        tif = str(request.time_in_force or "good_till_canceled")
        body = {
            "ticker": request.market_id,
            "side": book_side,
            "client_order_id": request.client_order_id,
            "count": _format_count(request.count_units),
            "price": _format_price(yes_price),
            "time_in_force": tif,
            "self_trade_prevention_type": str(request.self_trade_prevention_type or self.config.self_trade_prevention_type),
            "post_only": bool(self.config.post_only_quotes if request.post_only is None else request.post_only),
            "cancel_order_on_pause": bool(
                self.config.cancel_quotes_if_exchange_pauses
                if request.cancel_order_on_pause is None
                else request.cancel_order_on_pause
            ),
            "subaccount": self.config.subaccount_number,
        }
        if request.reduce_only is not None:
            body["reduce_only"] = bool(request.reduce_only)
        if request.expiration_timestamp_seconds and tif != "immediate_or_cancel":
            body["expiration_time"] = int(request.expiration_timestamp_seconds)
        # Explicit shard when the venue told us one for this market; otherwise
        # -1 = "require auto-routing by ticker" (create-order-v2, JSON body).
        exchange_index = self._exchange_index_by_market.get(request.market_id)
        body["exchange_index"] = -1 if exchange_index is None else int(exchange_index)
        if self.config.dry_run:
            LOGGER.info("DRY_CREATE | side=%s action=%s price_dollars=%s contracts=%s", request.side, request.action, _format_price(yes_price), _format_count(request.count_units))
            # Same contract as the live answer: Order.side is the OUTCOME side
            # (a sell of YES reports as "no"), which the execution ledger relies on.
            return Order(
                order_id=f"DRY-{uuid.uuid4()}", market_id=request.market_id,
                side=_outcome_side({"side": request.side, "action": request.action}, request.side),
                client_order_id=request.client_order_id,
            )
        response = self._post(f"{self.api_prefix}/portfolio/events/orders", body, action="create_order")
        payload = response.get("order") or response
        order = self._order(payload, side=request.side)
        resolved_index = _optional_exchange_index(payload.get("exchange_index"))
        self._remember_order_route(
            order.order_id, request.market_id, exchange_index if resolved_index is None else resolved_index
        )
        return order

    def amend_order(self, request: AmendOrderRequest) -> Order:
        self._require_authentication()
        book_side, yes_price = _translate_order(request.side, request.action, request.new_price_units)
        body = {
            "ticker": request.market_id,
            "side": book_side,
            "client_order_id": request.previous_client_order_id,
            "updated_client_order_id": request.updated_client_order_id,
            "count": _format_count(request.new_total_fillable_count_units),
            "price": _format_price(yes_price),
        }
        # amend-order-v2 carries `ticker`, so exchange_index (JSON body) works
        # exactly like create: explicit shard when known, else -1 auto-route.
        exchange_index = self._exchange_index_by_market.get(request.market_id)
        if exchange_index is None:
            exchange_index = self._exchange_index_by_order.get(str(request.order_id))
        body["exchange_index"] = -1 if exchange_index is None else int(exchange_index)
        if self.config.dry_run:
            LOGGER.info("DRY_AMEND | side=%s order_id=%s", request.side, request.order_id)
            return Order(order_id=request.order_id, market_id=request.market_id, side=request.side, client_order_id=request.updated_client_order_id)
        response = self._post(
            f"{self.api_prefix}/portfolio/events/orders/{request.order_id}/amend", body, action="amend_order"
        )
        payload = response.get("order") or response
        order = self._order(payload, fallback_id=request.order_id, side=request.side)
        resolved_index = _optional_exchange_index(payload.get("exchange_index"))
        route_index = exchange_index if resolved_index is None else resolved_index
        for order_id in {str(request.order_id), order.order_id}:
            self._remember_order_route(order_id, request.market_id, route_index)
        return order

    def decrease_order_to(self, *, order_id: str, remaining_count_units: int) -> Order:
        self._require_authentication()
        if self.config.dry_run:
            LOGGER.info("DRY_DECREASE | order_id=%s reduce_to_contracts=%s", order_id, _format_count(remaining_count_units))
            return Order(order_id=order_id)
        # decrease-order-v2: exchange_index and market_ticker are JSON body fields.
        body = {"reduce_to": _format_count(remaining_count_units)}
        body.update(self._order_route(order_id, action="decrease_order"))
        response = self._post(
            f"{self.api_prefix}/portfolio/events/orders/{order_id}/decrease",
            body,
            action="decrease_order",
            params={"subaccount": self.config.subaccount_number},
        )
        return self._order(response.get("order") or response, fallback_id=order_id)

    def cancel_order(self, *, order_id: str) -> Order:
        self._require_authentication()
        if self.config.dry_run:
            LOGGER.info("DRY_CANCEL | order_id=%s", order_id)
            return Order(order_id=order_id)
        # cancel-order-v2: exchange_index and market_ticker are query parameters.
        params = {"subaccount": self.config.subaccount_number}
        params.update(self._order_route(order_id, action="cancel_order"))
        response = self._delete(
            f"{self.api_prefix}/portfolio/events/orders/{order_id}",
            action="cancel_order",
            params=params,
        )
        return self._order(response.get("order") or response, fallback_id=order_id)

    def get_order_queue_position(self, order_id: str) -> QueuePosition:
        self._require_authentication()
        response = self._get(
            f"{self.api_prefix}/portfolio/orders/{order_id}/queue_position",
            operation="get_order_queue_position",
        )
        candidate = response.get("order") if isinstance(response.get("order"), dict) else response
        value = _optional_count(candidate, "queue_position_fp", "queue_position")
        return QueuePosition(order_id, value)

    def get_series(self, series_id: str) -> Series:
        item = self._get(
            f"{self.api_prefix}/series/{series_id}",
            params={"include_volume": True},
            operation="get_series",
        )["series"]
        return Series(
            series_id=str(item.get("ticker") or item.get("series_ticker") or series_id),
            fee_type=str(item.get("fee_type") or ""),
            fee_multiplier=float(item.get("fee_multiplier") or 1.0),
            title=str(item.get("title") or ""),
        )

    def get_series_fee_changes(self, series_id: str, *, show_historical: bool = False) -> List[SeriesFeeChange]:
        response = self._get(
            f"{self.api_prefix}/series/fee_changes",
            params={"series_ticker": series_id, "show_historical": bool(show_historical)},
            operation="get_series_fee_changes",
        )
        return [
            SeriesFeeChange(
                series_id=str(item.get("series_ticker") or series_id),
                fee_type=str(item.get("fee_type") or ""),
                fee_multiplier=(float(item["fee_multiplier"]) if item.get("fee_multiplier") not in (None, "") else None),
                effective_time_ms=_optional_timestamp_ms(item.get("effective_time") or item.get("effective_date")),
            )
            for item in response.get("series_fee_change_arr") or []
        ]

    def get_incentive_programs(self, *, status: str = "active", incentive_type: str = "all", limit: int = 10_000) -> List[IncentiveProgram]:
        response = self._get(
            f"{self.api_prefix}/incentive_programs",
            params={"status": status, "type": incentive_type, "limit": max(1, min(int(limit), 10_000))},
            operation="get_incentive_programs",
        )
        return [
            IncentiveProgram(
                market_id=str(item.get("market_ticker") or ""),
                incentive_type=str(item.get("incentive_type") or ""),
                discount_factor_bps=float(item.get("discount_factor_bps") or 0),
                target_size_units=int(_optional_count(item, "target_size_fp", "target_size") or 0),
            )
            for item in response.get("incentive_programs") or []
        ]

    def get_market_candlesticks(
        self,
        series_id: str,
        market_id: str,
        *,
        start_ts_s: int,
        end_ts_s: int,
        period_interval_minutes: int = 1,
    ) -> List[dict]:
        """Historical OHLC candlesticks for one market, normalized to fixed-point units.

        Kalshi caps each response at 5000 candlesticks; callers chunk the range.
        Returns dicts keyed: ts_ms (period END), price_/yes_bid_/yes_ask_ open/high/low/close
        _units (Optional[int]), volume_units, oi_units.
        """
        response = self._get(
            f"{self.api_prefix}/series/{series_id}/markets/{market_id}/candlesticks",
            params={
                "start_ts": int(start_ts_s),
                "end_ts": int(end_ts_s),
                "period_interval": int(period_interval_minutes),
            },
            operation="get_market_candlesticks",
        )

        def _ohlc(item: dict, group: str) -> dict:
            payload = item.get(group) or {}
            out = {}
            for bound in ("open", "high", "low", "close"):
                out[f"{group}_{bound}_units"] = _optional_money(
                    payload, f"{bound}_dollars", bound
                )
            return out

        rows: List[dict] = []
        for item in response.get("candlesticks") or []:
            row = {
                "ts_ms": int(item.get("end_period_ts") or 0) * 1000,
                "volume_units": _optional_count(item, "volume_fp", "volume"),
                "oi_units": _optional_count(item, "open_interest_fp", "open_interest"),
            }
            row.update(_ohlc(item, "price"))
            row.update(_ohlc(item, "yes_bid"))
            row.update(_ohlc(item, "yes_ask"))
            rows.append(row)
        return rows

    def list_public_trades(
        self,
        market_id: str = "",
        *,
        min_ts_s: Optional[int] = None,
        max_ts_s: Optional[int] = None,
        page_size: int = 1_000,
        max_results: int = 0,
        cursor: str = "",
    ) -> tuple[List[PublicTrade], str]:
        """Historical public trades (newest first). Returns (trades, next_cursor)."""
        trades: List[PublicTrade] = []
        page_size = max(1, min(int(page_size), 1_000))
        while max_results <= 0 or len(trades) < max_results:
            params: dict = {"limit": page_size}
            if market_id:
                params["ticker"] = market_id
            if min_ts_s is not None:
                params["min_ts"] = int(min_ts_s)
            if max_ts_s is not None:
                params["max_ts"] = int(max_ts_s)
            if cursor:
                params["cursor"] = cursor
            response = self._get(
                f"{self.api_prefix}/markets/trades", params=params, operation="list_public_trades"
            )
            for item in response.get("trades") or []:
                yes_price = _optional_money(item, "yes_price_dollars", "yes_price")
                no_price = _optional_money(item, "no_price_dollars", "no_price")
                if yes_price is None and no_price is not None:
                    yes_price = PRICE_SCALE - no_price
                if no_price is None and yes_price is not None:
                    no_price = PRICE_SCALE - yes_price
                trades.append(
                    PublicTrade(
                        market_id=str(item.get("ticker") or market_id),
                        trade_id=str(item.get("trade_id") or ""),
                        timestamp_ms=_timestamp_ms(item.get("created_time") or item.get("ts")),
                        yes_price_units=int(yes_price or 0),
                        no_price_units=int(no_price or 0),
                        count_units=int(_optional_count(item, "count_fp", "count") or 0),
                        taker_side=str(item.get("taker_side") or ""),
                    )
                )
            cursor = str(response.get("cursor") or response.get("next_cursor") or "")
            if not cursor:
                break
        return trades, cursor

    def list_markets_raw(self, *, status: str = "", venue_filters: Optional[dict] = None, page_size: int = 1_000, max_results: int = 10_000, page_pause_s: float = 0.0) -> List[dict]:
        """Paginated raw market payloads (includes fields the Market model drops, e.g. result)."""
        rows: List[dict] = []
        cursor = ""
        page_size = max(1, min(int(page_size), 1_000))
        while max_results <= 0 or len(rows) < max_results:
            if rows and page_pause_s > 0:
                time.sleep(page_pause_s)
            params: dict = {"limit": page_size}
            if status:
                params["status"] = status
            if cursor:
                params["cursor"] = cursor
            for key, value in (venue_filters or {}).items():
                if value not in (None, ""):
                    params[str(key)] = value
            response = self._get(f"{self.api_prefix}/markets", params=params, operation="list_markets")
            page = response.get("markets") or []
            for payload in page:
                if isinstance(payload, dict):
                    self._remember_market_shard(payload)
            rows.extend(page)
            cursor = str(response.get("cursor") or response.get("next_cursor") or "")
            if not cursor or not page:
                break
        return rows[:max_results] if max_results > 0 else rows

    @staticmethod
    def _book_levels(payload: dict, fixed_field: str, legacy_field: str) -> dict[int, int]:
        return _book_levels_from_payload(payload, fixed_field, legacy_field)

    def _event(self, data: dict, market_id: str) -> Optional[MarketEvent]:
        event = parse_wire_event(data, market_id)
        if isinstance(event, OrderUpdate):
            # Seeds order -> ticker routing for orders this process did not create.
            payload = data.get("msg") or {}
            self._remember_order_route(
                event.order_id, event.market_id, _optional_exchange_index(payload.get("exchange_index"))
            )
        return event

    def _subscriptions_many(self, market_ids: Iterable[str], include_position_updates: bool) -> Iterable[str]:
        market_ids = tuple(dict.fromkeys(str(item) for item in market_ids if str(item)))
        if not market_ids:
            raise ValueError("at least one market ticker is required")
        channel_groups = [(1, ["orderbook_delta"]), (2, ["user_orders"]), (3, ["fill"]), (4, ["trade", "ticker"])]
        if include_position_updates:
            channel_groups.append((5, ["market_positions"]))
        for subscription_id, channels in channel_groups:
            yield json.dumps(
                {
                    "id": subscription_id,
                    "cmd": "subscribe",
                    "params": {"channels": channels, "market_tickers": list(market_ids)},
                }
            )

    def _subscriptions(self, market_id: str, include_position_updates: bool) -> Iterable[str]:
        return self._subscriptions_many((market_id,), include_position_updates)

    async def stream_events(self, market_id: str, *, include_position_updates: bool = True) -> AsyncIterator[MarketEvent]:
        async for event in self.stream_events_many((market_id,), include_position_updates=include_position_updates):
            yield event

    async def _update_subscription(self, market_ids: Iterable[str], action: str, *, channels: Optional[set[str]] = None) -> None:
        market_ids = tuple(dict.fromkeys(str(item) for item in market_ids if str(item)))
        if not market_ids:
            return
        sids = getattr(self, "_subscription_sids", {})
        command_id = getattr(self, "_subscription_command_id", 100)
        matched = [(channel, sid) for channel, sid in sids.items() if channels is None or channel in channels]
        if not matched:
            raise RuntimeError("WebSocket subscriptions are not ready")
        for _channel, sid in matched:
            command_id += 1
            await self.websocket_client.send(
                json.dumps({
                    "id": command_id,
                    "cmd": "update_subscription",
                    "params": {"sids": [sid], "market_tickers": list(market_ids), "action": action},
                })
            )
        self._subscription_command_id = command_id

    async def update_market_subscriptions(self, *, add: Sequence[str], remove: Sequence[str]) -> None:
        add_set = {str(item) for item in add if str(item)}
        remove_set = {str(item) for item in remove if str(item)} - add_set
        assigned = getattr(self, "_stream_market_ids", set())
        if remove_set:
            await self._update_subscription(sorted(remove_set), "delete_markets")
            assigned.difference_update(remove_set)
        if add_set:
            await self._update_subscription(sorted(add_set), "add_markets")
            assigned.update(add_set)
        unavailable = getattr(self, "_unavailable_books", set())
        unavailable.difference_update(remove_set)
        unavailable.update(add_set)

    async def stream_events_many(
        self,
        market_ids: Sequence[str],
        *,
        include_position_updates: bool = True,
    ) -> AsyncIterator[MarketEvent]:
        self._require_authentication()
        normalized = tuple(dict.fromkeys(str(item) for item in market_ids if str(item)))
        if not normalized:
            raise ValueError("at least one market ticker is required")
        self._closed = False
        self._stream_market_ids = set(normalized)
        self._subscription_sids: dict[str, int] = {}
        self._subscription_command_id = 100
        self._unavailable_books = set(normalized)
        backoff = 1
        connected_once = False
        while not self._closed:
            try:
                if connected_once:
                    self._record_stream_activity("reconnects")
                await self.websocket_client.subscribe(
                    self._subscriptions_many(sorted(self._stream_market_ids), include_position_updates), headers=self.websocket_headers()
                )
                connected_once = True
                backoff = 1
                # Kalshi sequences messages per subscription (sid), not per
                # market.  An orderbook subscription can contain many markets,
                # so updates for A and B legitimately look like 10/A, 11/B,
                # 12/A.  Treating A's 10 -> 12 as a gap causes a false reset and
                # immediately cancels otherwise valid quotes.
                expected_sequences_by_sid: dict[int, int] = {}
                # Older fixtures/compatible venues may omit sid.  Keep the
                # single-market behavior for that case only.
                legacy_expected_sequences: dict[str, int] = {}
                self._subscription_sids.clear()
                self._unavailable_books = set(self._stream_market_ids)
                for ticker in sorted(self._stream_market_ids):
                    self._record_stream_activity("event", event_type="reset")
                    yield StreamReset(ticker)
                LOGGER.info("WS_CONNECTED_AND_SUBSCRIBED | market_count=%s", len(self._stream_market_ids))
                async for raw_message in self.websocket_client:
                    if self._closed:
                        return
                    data = json.loads(raw_message)
                    if data.get("type") == "subscribed":
                        message = data.get("msg") or {}
                        channel = str(message.get("channel") or "")
                        sid = message.get("sid")
                        if channel and isinstance(sid, int):
                            self._subscription_sids[channel] = sid
                        continue
                    fallback_market = next(iter(self._stream_market_ids)) if len(self._stream_market_ids) == 1 else ""
                    event = self._event(data, fallback_market)
                    if event is not None and event.market_id not in self._stream_market_ids:
                        continue
                    is_orderbook_event = isinstance(event, (OrderBookSnapshot, OrderBookDelta))
                    raw_sid = data.get("sid")
                    raw_sequence = data.get("seq")
                    orderbook_sid = self._subscription_sids.get("orderbook_delta")
                    sequence_gap = False
                    previous_sequence: Optional[int] = None
                    affected_markets: set[str] = set()
                    if (
                        isinstance(raw_sid, int)
                        and isinstance(raw_sequence, int)
                        and (raw_sid == orderbook_sid or is_orderbook_event)
                    ):
                        previous_sequence = expected_sequences_by_sid.get(raw_sid)
                        sequence_gap = previous_sequence is not None and raw_sequence != previous_sequence + 1
                        expected_sequences_by_sid[raw_sid] = raw_sequence
                        if sequence_gap:
                            # A missing message on a shared orderbook sid may
                            # belong to any subscribed ticker.  Re-snapshot all
                            # books on that sid before allowing new exposure.
                            affected_markets = set(self._stream_market_ids)
                    elif is_orderbook_event and event.sequence is not None:
                        previous_sequence = legacy_expected_sequences.get(event.market_id)
                        sequence_gap = previous_sequence is not None and event.sequence != previous_sequence + 1
                        legacy_expected_sequences[event.market_id] = event.sequence
                        if sequence_gap:
                            affected_markets = {event.market_id}

                    if sequence_gap:
                        self._record_stream_activity("sequenceResets")
                        LOGGER.info(
                            "ORDERBOOK_SEQUENCE_GAP | sid=%s affected_markets=%s previous_sequence=%s new_sequence=%s",
                            raw_sid, len(affected_markets), previous_sequence, raw_sequence,
                        )
                        self._unavailable_books.update(affected_markets)
                        if "orderbook_delta" not in self._subscription_sids:
                            # Without the server SID a targeted snapshot cannot
                            # be requested, so use the safe reconnect path.
                            break
                        await self._update_subscription(
                            tuple(sorted(affected_markets)), "get_snapshot", channels={"orderbook_delta"}
                        )
                        for ticker in sorted(affected_markets):
                            yield StreamReset(ticker)
                        continue

                    if isinstance(event, OrderBookSnapshot):
                        self._unavailable_books.discard(event.market_id)
                    elif isinstance(event, OrderBookDelta) and event.market_id in self._unavailable_books:
                        continue
                    if event is not None:
                        self._record_stream_activity(
                            "event", event_type=type(event).__name__
                        )
                        yield event
            except Exception as exc:
                if self._closed:
                    return
                self._record_stream_activity("adapterErrors")
                LOGGER.info("WS_DISCONNECT | error=%s reconnect_backoff_seconds=%s", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(30, backoff * 2)
            finally:
                await self.websocket_client.close()

    async def close(self) -> None:
        self._closed = True
        await super().close()
