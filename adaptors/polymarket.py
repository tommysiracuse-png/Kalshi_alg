"""Polymarket Gamma/Data/CLOB adaptor.

The adaptor deliberately keeps the rest of the application independent of the
Polymarket SDK.  The SDK is loaded lazily for authenticated operations, which
also keeps public screening and unit tests usable without wallet credentials.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from typing import Any, AsyncIterator, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlparse

from clients.base_client import BaseClient
from clients.http_client import HTTPClient, HTTPClientError, _redact_proxy_text
from clients.monitoring import ActivityMonitor, merge_activity_snapshots
from clients.models import (
    AccountBalance, AccountFill, AccountFillQuery, AccountLimits, AccountOrder,
    AccountOrderQuery, AccountPosition, AccountPositionQuery, AmendOrderRequest,
    CreateOrderRequest, Fill, IncentiveProgram, Market, MarketEvent, MarketQuery,
    MarketQuote, Order, OrderBookDelta, OrderBookSnapshot, OrderNotFoundError,
    OrderUpdate, Position, PositionUpdate, PostOnlyCrossError, PriceRangeData,
    PublicTrade, QueuePosition, RateLimitBucket, Series, SeriesFeeChange,
    StreamReset, TickerUpdate,
)
from clients.websocket_client import WebsocketClient
from market_rules import PolymarketMarketRules
from money_units import decimal_money_units, nonnegative_base_money_units
from polymarket_cache import BookTop, PolymarketBookCache, PolymarketCatalogStore
from polymarket_rate_limit import PolymarketRateLimiter


LOGGER = logging.getLogger(__name__)


PRICE_SCALE = 10_000
# The rest of the runtime uses fixed-point contract units (one contract is
# 100 units), even though Polymarket's wire API accepts decimal token sizes.
TOKEN_SCALE = 100
# CLOB balance/allowance values are six-decimal pUSD base units.  Keep this
# wire scale separate from the shared account-money scale used by Kalshi and
# the UI.
POLYMARKET_COLLATERAL_BASE_SCALE = 1_000_000
# Polymarket does not expose account-specific token-bucket limits through the
# CLOB API. These are conservative local pacing limits used by the shared
# execution broker so an unknown provider limit does not become a zero-rate
# bucket that permanently queues every worker read.
LOCAL_READ_RATE = 60
LOCAL_WRITE_RATE = 30
OPEN_INTEREST_BATCH_SIZE = 100


_SDK_PROXY_LOCK = threading.Lock()


def _configure_sdk_proxy(proxy_url: Optional[str]) -> None:
    """Route py-clob-client-v2's shared HTTP transport through ``proxy_url``.

    py-clob-client-v2 currently keeps one module-level ``httpx.Client`` rather
    than accepting a transport/client in ``ClobClient``.  Replace that client
    once, before constructing our SDK client, so API-key derivation, account
    reads, and order mutations all use the same proxy as the adaptor's REST
    transports.  The application has one Polymarket transport configuration;
    a second client with a different SDK proxy would otherwise change the
    route for the first client as well.
    """

    normalized = str(proxy_url or "").strip()
    try:
        import httpx  # type: ignore
        from py_clob_client_v2.http_helpers import helpers  # type: ignore
    except ImportError as exc:
        raise RuntimeError("a Polymarket proxy requires py-clob-client-v2 and httpx") from exc

    with _SDK_PROXY_LOCK:
        current = getattr(helpers, "_http_client", None)
        if getattr(current, "_codex_proxy_url", "__unset__") == normalized:
            return
        if normalized:
            try:
                replacement = httpx.Client(http2=True, proxy=normalized, trust_env=False)
            except TypeError:  # httpx < 0.28 used ``proxies`` instead of ``proxy``.
                replacement = httpx.Client(
                    http2=True,
                    proxies={"http://": normalized, "https://": normalized},
                    trust_env=False,
                )
        else:
            # Explicit direct mode prevents HTTP(S)_PROXY in the launcher
            # environment from changing the route of a no-proxy client.
            replacement = httpx.Client(http2=True, trust_env=False)
        replacement._codex_proxy_url = normalized  # type: ignore[attr-defined]
        if current is not None and hasattr(current, "close"):
            current.close()
        helpers._http_client = replacement


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _price_units(value: Any) -> int:
    return int((_decimal(value) * PRICE_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


def _size_units(value: Any) -> int:
    return int((_decimal(value) * TOKEN_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


def _optional_size_units(value: Any) -> Optional[int]:
    """Convert an optional token/contract count without inventing zero."""

    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        parsed = Decimal(str(value))
        if not parsed.is_finite():
            return None
        return int((parsed * TOKEN_SCALE).to_integral_value(rounding=ROUND_HALF_UP))
    except (ArithmeticError, TypeError, ValueError):
        return None


def _money_units(value: Any) -> int:
    return decimal_money_units(value)


def _collateral_units(value: Any) -> int:
    """Normalize a CLOB six-decimal collateral amount to shared money units."""

    return nonnegative_base_money_units(value, base_scale=POLYMARKET_COLLATERAL_BASE_SCALE)


def _collateral_allowance(payload: Any) -> int:
    """Read scalar or plural allowance fields returned by CLOB versions."""
    if isinstance(payload, Mapping):
        scalar = payload.get("allowance")
        plural = payload.get("allowances")
    else:
        scalar = getattr(payload, "allowance", None)
        plural = getattr(payload, "allowances", None)
    if scalar not in (None, ""):
        return _collateral_units(scalar)
    values = plural.values() if isinstance(plural, Mapping) else plural if isinstance(plural, (list, tuple)) else ()
    parsed = [_collateral_units(value) for value in values if value not in (None, "")]
    return max(parsed, default=0)


def _timestamp_ms(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        number = float(value)
        return int(number if number >= 10_000_000_000 else number * 1000)
    except Exception:
        text = str(value).replace("Z", "+00:00")
        try:
            return int(datetime.fromisoformat(text).timestamp() * 1000)
        except Exception:
            return None


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return list(parsed) if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _book_levels(value: Any) -> dict[int, int]:
    levels: dict[int, int] = {}
    for item in value or ():
        if isinstance(item, dict):
            price, size = item.get("price"), item.get("size")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price, size = item[0], item[1]
        else:
            continue
        try:
            levels[_price_units(price)] = _size_units(size)
        except Exception:
            continue
    return levels


def _best(levels: dict[int, int], *, high: bool) -> tuple[Optional[int], Optional[int]]:
    if not levels:
        return None, None
    price = (max if high else min)(levels)
    return price, levels[price]


@dataclass(frozen=True)
class PolymarketClientConfig:
    private_key: str = field(default="", repr=False)
    private_key_path: str = field(default="", repr=False)
    funder_address: str = ""
    signature_type: int = 0
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    api_passphrase: str = field(default="", repr=False)
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    data_base_url: str = "https://data-api.polymarket.com"
    websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    user_websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    # Optional HTTP(S) or SOCKS proxy applied to every Polymarket transport.
    # Credentials, when required, may be embedded in the URL or supplied by
    # the proxy's local authentication mechanism; never persist them in the
    # session store.
    proxy_url: Optional[str] = field(default=None, repr=False)
    chain_id: int = 137
    dry_run: bool = False
    public_only: bool = False
    rest_timeout_seconds: float = 15.0
    rest_connect_timeout_seconds: float = 10.0
    open_interest_retry_attempts: int = 4
    open_interest_retry_base_delay_seconds: float = 1.0
    open_interest_retry_max_delay_seconds: float = 10.0
    open_interest_retry_jitter_fraction: float = 0.25
    # A persistent Cloudflare block should not make every catalog page spend
    # four more attempts on the same endpoint.  After one batch exhausts its
    # bounded retries, temporarily suppress later OI batches and let catalog
    # and book synchronization continue.  The next sync retries after this
    # cooldown expires.
    open_interest_403_cooldown_seconds: float = 60.0
    websocket_max_message_bytes: int = 8 * 1024 * 1024
    websocket_ping_interval_seconds: float = 20.0
    websocket_ping_timeout_seconds: float = 60.0
    catalog_path: str = ""
    book_cache_path: str = ""
    catalog_page_size: int = 1_000
    catalog_parallelism: int = 4
    book_batch_size: int = 100
    book_parallelism: int = 16
    book_bootstrap_timeout_seconds: float = 45.0
    book_readiness_fraction: float = 0.99
    scan_mode: str = "catalog_plus_cached_books"
    rate_limit_profile: str = "standard"
    rate_limit_overrides: Optional[Mapping[str, Any]] = None
    venue: str = "polymarket"
    # Local mirror settings.  The launcher child owns writes; the foreground
    # client only opens the mirror read-only for screening.
    mirror_enabled: bool = False
    mirror_required_complete_snapshot: bool = True
    mirror_db_path: str = ""
    mirror_status_path: str = ""
    mirror_snapshot_max_age_seconds: float = 60.0
    mirror_book_stale_after_seconds: float = 60.0
    mirror_ws_shards: int = 1
    mirror_book_recovery_parallelism: int = 16
    mirror_catalog_page_size: int = 100
    mirror_sync_interval_seconds: float = 30.0
    mirror_max_markets: int = 0

    def __post_init__(self) -> None:
        if self.rest_timeout_seconds <= 0:
            raise ValueError("rest_timeout_seconds must be > 0")
        if self.rest_connect_timeout_seconds <= 0:
            raise ValueError("rest_connect_timeout_seconds must be > 0")
        if self.open_interest_retry_attempts < 1 or self.open_interest_retry_attempts > 10:
            raise ValueError("open_interest_retry_attempts must be between 1 and 10")
        if self.open_interest_retry_base_delay_seconds <= 0:
            raise ValueError("open_interest_retry_base_delay_seconds must be > 0")
        if self.open_interest_retry_max_delay_seconds <= 0:
            raise ValueError("open_interest_retry_max_delay_seconds must be > 0")
        if self.open_interest_retry_max_delay_seconds < self.open_interest_retry_base_delay_seconds:
            raise ValueError("open_interest_retry_max_delay_seconds must be >= the base delay")
        if not 0 <= self.open_interest_retry_jitter_fraction <= 1:
            raise ValueError("open_interest_retry_jitter_fraction must be between 0 and 1")
        if self.open_interest_403_cooldown_seconds <= 0:
            raise ValueError("open_interest_403_cooldown_seconds must be > 0")
        if self.websocket_max_message_bytes <= 0:
            raise ValueError("websocket_max_message_bytes must be > 0")
        if self.websocket_ping_interval_seconds <= 0 or self.websocket_ping_timeout_seconds <= 0:
            raise ValueError("Polymarket websocket ping intervals must be > 0")
        if self.catalog_page_size <= 0 or self.catalog_page_size > 10_000:
            raise ValueError("catalog_page_size must be between 1 and 10000")
        if self.catalog_parallelism <= 0 or self.catalog_parallelism > 32:
            raise ValueError("catalog_parallelism must be between 1 and 32")
        if self.book_batch_size <= 0 or self.book_batch_size > 1_000:
            raise ValueError("book_batch_size must be between 1 and 1000")
        if self.book_parallelism <= 0 or self.book_parallelism > 64:
            raise ValueError("book_parallelism must be between 1 and 64")
        if self.book_bootstrap_timeout_seconds <= 0:
            raise ValueError("book_bootstrap_timeout_seconds must be > 0")
        if not 0 < self.book_readiness_fraction <= 1:
            raise ValueError("book_readiness_fraction must be in (0, 1]")
        if self.scan_mode not in {"catalog_only", "catalog_plus_cached_books", "bootstrap_missing_books"}:
            raise ValueError("scan_mode must be catalog_only, catalog_plus_cached_books, or bootstrap_missing_books")
        if str(self.rate_limit_profile).strip().lower() not in {"standard", "copper", "bronze", "silver", "gold", "platinum", "diamond", "elite"}:
            raise ValueError("unsupported Polymarket rate_limit_profile")
        if self.chain_id != 137:
            raise ValueError("Polymarket production trading requires Polygon chain 137")
        if self.signature_type not in {0, 1, 2, 3}:
            raise ValueError("Polymarket signature_type must be 0 (EOA), 1 (proxy), 2 (Safe), or 3 (deposit wallet)")
        if self.mirror_snapshot_max_age_seconds <= 0 or self.mirror_book_stale_after_seconds <= 0:
            raise ValueError("mirror freshness intervals must be > 0")
        if self.mirror_ws_shards <= 0 or self.mirror_book_recovery_parallelism <= 0:
            raise ValueError("mirror concurrency settings must be > 0")
        if self.mirror_catalog_page_size <= 0 or self.mirror_catalog_page_size > 100:
            raise ValueError("mirror_catalog_page_size must be between 1 and 100")
        if self.mirror_sync_interval_seconds <= 0:
            raise ValueError("mirror_sync_interval_seconds must be > 0")
        if self.proxy_url:
            parsed_proxy = urlparse(str(self.proxy_url))
            scheme = parsed_proxy.scheme.lower()
            if scheme not in {"http", "https", "socks4", "socks4a", "socks5", "socks5h"} or not parsed_proxy.netloc:
                raise ValueError(
                    "Polymarket proxy_url must be a URL using http, https, socks4, socks4a, socks5, or socks5h"
                )


class _BalanceAllowanceParams:
    def __init__(self, asset_type: str, token_id: Optional[str] = None) -> None:
        self.asset_type = asset_type
        self.token_id = token_id


class PolymarketClient(BaseClient):
    venue_name = "polymarket"
    venue = "polymarket"
    environment_name = "prod"

    def __init__(
        self,
        config: PolymarketClientConfig,
        *,
        http_client: Optional[HTTPClient] = None,
        websocket_client: Optional[WebsocketClient] = None,
        clob_client: Optional[Any] = None,
    ) -> None:
        self.config = config
        self.dry_run = bool(config.dry_run)
        self.host = config.clob_base_url.rstrip("/")
        # ``None`` means use the Polymarket-specific environment setting;
        # an explicit empty string means direct mode. This keeps ambient
        # proxy variables from changing a caller's explicit route.
        proxy_url = (
            os.getenv("POLYMARKET_PROXY_URL", "")
            if config.proxy_url is None
            else str(config.proxy_url)
        ).strip() or None
        self.proxy_url = proxy_url
        self.gamma_http = HTTPClient(
            config.gamma_base_url,
            timeout_seconds=config.rest_timeout_seconds,
            connect_timeout_seconds=config.rest_connect_timeout_seconds,
            proxy_url=proxy_url,
            trust_env=False,
            pool_connections=max(10, int(config.catalog_parallelism)),
            pool_maxsize=max(10, int(config.catalog_parallelism)),
        )
        self.data_http = HTTPClient(
            config.data_base_url,
            timeout_seconds=config.rest_timeout_seconds,
            connect_timeout_seconds=config.rest_connect_timeout_seconds,
            proxy_url=proxy_url,
            trust_env=False,
            activity_error_body_limit=0,
            pool_connections=max(10, int(config.catalog_parallelism)),
            pool_maxsize=max(10, int(config.catalog_parallelism)),
        )
        super().__init__(
            http_client=http_client or HTTPClient(
                self.host,
                timeout_seconds=config.rest_timeout_seconds,
                connect_timeout_seconds=config.rest_connect_timeout_seconds,
                proxy_url=proxy_url,
                trust_env=False,
                pool_connections=max(10, int(config.book_parallelism)),
                pool_maxsize=max(10, int(config.book_parallelism)),
            ),
            websocket_client=websocket_client or WebsocketClient(
                config.websocket_url,
                ping_interval_seconds=config.websocket_ping_interval_seconds,
                ping_timeout_seconds=config.websocket_ping_timeout_seconds,
                max_size=config.websocket_max_message_bytes,
                proxy_url=proxy_url,
            ),
        )
        self._user_websocket = WebsocketClient(
            config.user_websocket_url,
            ping_interval_seconds=config.websocket_ping_interval_seconds,
            ping_timeout_seconds=config.websocket_ping_timeout_seconds,
            max_size=config.websocket_max_message_bytes,
            proxy_url=proxy_url,
        )
        self._clob_client = clob_client
        self._active_api_creds: Optional[Any] = None
        self._api_credentials_recovery_attempted = False
        # Authenticated SDK calls do not necessarily use this adaptor's
        # HTTPClient, so keep a small logical-call monitor for them as well.
        self._sdk_activity = ActivityMonitor()
        self._rate_limiter = PolymarketRateLimiter(
            config.rate_limit_profile,
            config.rate_limit_overrides or {},
        )
        self._market_cache: dict[str, Market] = {}
        self._asset_market: dict[str, tuple[str, str]] = {}
        self.book_cache = PolymarketBookCache(config.book_cache_path or None)
        self.catalog_store = PolymarketCatalogStore(config.catalog_path) if config.catalog_path else None
        self.mirror_store = None
        if config.mirror_enabled and config.mirror_db_path:
            from polymarket_mirror import PolymarketMirrorStore
            self.mirror_store = PolymarketMirrorStore(config.mirror_db_path)
        self._catalog_last_refresh_ms = 0
        self._catalog_loaded_statuses: set[str] = set()
        self._catalog_refresh_lock = threading.Lock()
        self._catalog_metrics: dict[str, Any] = {}
        self._open_interest_metrics: dict[str, Any] = {
            "batches": 0,
            "marketsRequested": 0,
            "marketsResolved": 0,
            "marketsMissing": 0,
            "apiErrors": 0,
            "forbiddenResponses": 0,
            "cloudflare403": 0,
            "retries": 0,
            "retryExhausted": 0,
            "suppressedRequests": 0,
        }
        self._open_interest_diagnostics: dict[str, Any] = {"lastCloudflare": None}
        self._open_interest_403_blocked_until = 0.0
        self._book_metrics: dict[str, Any] = {}
        self._market_stream_task: Optional[asyncio.Task[Any]] = None
        self._market_stream_started = False
        self._market_stream_assets: tuple[str, ...] = ()
        self._seen_user_events: set[str] = set()
        self._screener_cancel = threading.Event()
        # Catalog and book hydration used to create short-lived executors on
        # every call.  Reuse one bounded pool so concurrent mirror/screener
        # refreshes cannot create a thread burst in a worker process.
        self._parallel_executor = ThreadPoolExecutor(
            max_workers=max(1, min(32, max(config.catalog_parallelism, config.book_parallelism))),
            thread_name_prefix="polymarket-io",
        )

    def begin_screener_scan(self) -> None:
        self._screener_cancel.clear()

    def cancel_screener_scan(self) -> None:
        self._screener_cancel.set()

    def screener_scan_cancelled(self) -> bool:
        return self._screener_cancel.is_set()

    def book_readiness(self) -> float:
        expected = int(self.book_cache.expected_assets)
        if expected <= 0:
            return 0.0
        return self.book_cache.ready_count() / expected

    def should_bootstrap_books(self) -> bool:
        """Whether the next screener pass should hydrate missing books.

        A cold/partially warm cache is bootstrapped automatically. Once the
        configured completeness threshold is reached, scheduled scans stay
        local and only the websocket/recovery path updates books.
        """
        mode = str(self.config.scan_mode or "catalog_plus_cached_books")
        return mode == "bootstrap_missing_books" or (
            mode == "catalog_plus_cached_books"
            and self.book_readiness() < float(self.config.book_readiness_fraction)
        )

    def _sdk_call(self, operation: str, call: Any) -> Any:
        started = time.perf_counter()
        rate_operation = self._rate_operation(operation)
        self._rate_limiter.acquire(rate_operation)
        try:
            result = call()
        except Exception as exc:
            status_code = getattr(exc, "status_code", None)
            self._rate_limiter.observe_response(
                status_code,
                getattr(exc, "headers", None),
                operation=rate_operation,
            )
            text = str(exc).lower()
            invalid_api_key = (
                status_code == 401
                or "unauthorized/invalid api key" in text
                or "invalid api key" in text
            )
            if invalid_api_key and self.config.api_key and not self._api_credentials_recovery_attempted:
                # Credentials persisted in bot.env can be stale or belong to
                # a prior signer. Re-derive once from the configured private
                # key, then retry the original operation. A failed retry is
                # reported below as the real authentication error.
                self._api_credentials_recovery_attempted = True
                try:
                    sdk = self._require_auth()
                    derive = getattr(sdk, "create_or_derive_api_key", None)
                    if not callable(derive):
                        raise RuntimeError("Polymarket CLOB SDK cannot derive API credentials")
                    self._active_api_creds = self._sdk_call(
                        "polymarket_create_or_derive_api_key", derive
                    )
                    sdk.creds = self._active_api_creds
                    result = call()
                except Exception as retry_exc:
                    status_code = getattr(retry_exc, "status_code", None)
                    self._sdk_activity.record_rest(
                        method="SDK",
                        operation=operation,
                        status_code=int(status_code) if status_code is not None else None,
                        latency_ms=(time.perf_counter() - started) * 1000,
                        error=True,
                        error_message=(
                            f"configured API credentials rejected; re-derivation failed: "
                            f"{_redact_proxy_text(retry_exc, self.proxy_url)}"
                        ),
                    )
                    raise RuntimeError(
                        f"Polymarket authentication failed: configured API credentials were rejected "
                        f"and re-derivation failed: {_redact_proxy_text(retry_exc, self.proxy_url)}"
                    ) from retry_exc
                self._sdk_activity.record_rest(
                    method="SDK",
                    operation=operation,
                    status_code=200,
                    latency_ms=(time.perf_counter() - started) * 1000,
                    error=False,
                )
                return result
            status_code = getattr(exc, "status_code", None)
            safe_error = _redact_proxy_text(exc, self.proxy_url)
            if safe_error != str(exc):
                try:
                    exc.args = (safe_error,)
                except Exception:
                    pass
            self._sdk_activity.record_rest(
                method="SDK",
                operation=operation,
                status_code=int(status_code) if status_code is not None else None,
                latency_ms=(time.perf_counter() - started) * 1000,
                error=True,
                error_message=safe_error,
            )
            raise
        self._sdk_activity.record_rest(
            method="SDK",
            operation=operation,
            status_code=200,
            latency_ms=(time.perf_counter() - started) * 1000,
            error=False,
        )
        return result

    def activity_snapshot(self) -> dict[str, Any]:
        """Return merged activity while retaining Polymarket API components."""
        clob = merge_activity_snapshots((
            self.http_client.activity_snapshot(),
            self.websocket_client.activity_snapshot(),
        ))
        gamma = self.gamma_http.activity_snapshot()
        data = self.data_http.activity_snapshot()
        sdk = self._sdk_activity.snapshot()
        user = self._user_websocket.activity_snapshot()
        merged = merge_activity_snapshots((clob, gamma, data, sdk, user))
        merged["venue"] = self.venue_name
        merged["proxyConfigured"] = bool(self.proxy_url)
        merged["rateLimit"] = self._rate_limiter.snapshot()
        merged["catalog"] = dict(self._catalog_metrics)
        merged["openInterest"] = dict(self._open_interest_metrics)
        merged["openInterestDiagnostics"] = dict(self._open_interest_diagnostics)
        merged["book"] = dict(self._book_metrics)
        merged["bookReadiness"] = self.book_readiness()
        if self.mirror_store is not None:
            try:
                mirror_status = self.mirror_store.status()
                # The SQLite status is only updated after a complete catalog
                # or book publication. During a cold sync the child writes a
                # heartbeat/status file so the launcher can expose
                # ``syncing_catalog`` (and the child's API counters) instead
                # of reporting an ambiguous unavailable mirror.
                status_path = str(getattr(self.config, "mirror_status_path", "") or "")
                if status_path:
                    try:
                        with open(status_path, "r", encoding="utf-8") as handle:
                            child_status = json.load(handle)
                        if isinstance(child_status, Mapping):
                            from polymarket_mirror import merge_mirror_status
                            mirror_status = merge_mirror_status(mirror_status, child_status)
                    except (OSError, ValueError, TypeError):
                        pass
                merged["mirror"] = mirror_status
            except Exception as exc:
                merged["mirror"] = {"complete": False, "reason": str(exc)}
        merged["components"] = {
            "clob": clob,
            "gamma": gamma,
            "data": data,
            "sdk": sdk,
            "userWebSocket": user,
        }
        return merged

    def _rate_operation(self, operation: str) -> str:
        name = str(operation or "").lower()
        if "api_key" in name or "derive" in name or "credential" in name:
            return "clob_auth"
        if "catalog" in name or "list_market" in name or "get_market" in name:
            return "gamma_markets"
        if "open_interest" in name:
            return "data_open_interest"
        if "position" in name:
            return "data_positions"
        if "fill" in name or "trade" in name:
            return "data_trades" if "list_account" not in name else "clob_ledger"
        if "books" in name:
            return "clob_books"
        if "order_book" in name:
            return "clob_book"
        if "create_order" in name:
            return "order"
        if "cancel" in name or "decrease" in name:
            return "cancel"
        if "account_orders" in name or "balance" in name:
            return "clob_ledger"
        return "gamma_general"

    def _http_call(self, transport: Any, method: str, path: str, *, operation: str, **kwargs: Any) -> Any:
        self._rate_limiter.acquire(self._rate_operation(operation))
        try:
            result = getattr(transport, method)(path, operation=operation, **kwargs)
        except HTTPClientError as exc:
            self._rate_limiter.observe_response(
                exc.status_code,
                getattr(exc, "headers", None),
                operation=self._rate_operation(operation),
            )
            if exc.status_code == 429:
                retry_after = 0.0
                try:
                    headers = {
                        str(key).lower(): value
                        for key, value in (getattr(exc, "headers", None) or {}).items()
                    }
                    retry_after = float(headers.get("retry-after", 0) or 0)
                except (TypeError, ValueError):
                    pass
                if retry_after > 0:
                    self._rate_limiter.record_retry()
                    time.sleep(min(retry_after, 10.0))
            raise
        self._rate_limiter.observe_response(
            200,
            getattr(transport, "last_response_headers", None),
            operation=self._rate_operation(operation),
        )
        return result

    def _private_key_value(self) -> str:
        if self.config.private_key:
            return str(self.config.private_key).strip()
        if self.config.private_key_path:
            with open(self.config.private_key_path, encoding="utf-8") as handle:
                # Secret files commonly end with a newline. The CLOB V2
                # signer expects the raw 32-byte hex key and rejects that
                # otherwise harmless formatting character as a non-hex digit.
                return handle.read().strip()
        return os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip()

    def _configure_sdk_transport(self) -> None:
        try:
            _configure_sdk_proxy(self.proxy_url)
        except Exception as exc:
            safe = _redact_proxy_text(exc, self.proxy_url)
            if safe != str(exc):
                try:
                    exc.args = (safe,)
                except Exception:
                    pass
            raise RuntimeError(f"Polymarket SDK proxy setup failed: {safe}") from exc

    def _require_auth(self) -> Any:
        if self.config.public_only or not self._private_key_value():
            raise ValueError("Polymarket private credentials are required for this operation")
        if self._clob_client is not None:
            # Test doubles and applications may inject a prebuilt SDK client;
            # still configure the SDK module transport before using it so an
            # explicit Polymarket proxy cannot be bypassed accidentally.
            self._configure_sdk_transport()
            return self._clob_client
        try:
            from py_clob_client_v2 import ApiCreds, ClobClient  # type: ignore
        except ImportError as exc:
            raise RuntimeError("install the Polymarket CLOB V2 client for authenticated operations") from exc
        self._configure_sdk_transport()
        creds = None
        if self.config.api_key:
            creds = ApiCreds(
                api_key=self.config.api_key,
                api_secret=self.config.api_secret,
                api_passphrase=self.config.api_passphrase,
            )
            self._active_api_creds = creds
        self._clob_client = ClobClient(
            self.config.clob_base_url,
            self.config.chain_id,
            key=self._private_key_value(),
            creds=creds,
            signature_type=self.config.signature_type,
            funder=self.config.funder_address or None,
        )
        if creds is None and hasattr(self._clob_client, "create_or_derive_api_key"):
            self._active_api_creds = self._sdk_call(
                "polymarket_create_or_derive_api_key",
                self._clob_client.create_or_derive_api_key,
            )
            self._clob_client.creds = self._active_api_creds
        return self._clob_client

    @staticmethod
    def _market_id(payload: dict) -> str:
        return str(
            payload.get("conditionId")
            or payload.get("condition_id")
            or payload.get("native_market_id")
            or payload.get("ticker")
            or payload.get("id")
            or payload.get("slug")
            or ""
        )

    def _normalize_market(self, payload: dict) -> Optional[Market]:
        market_id = self._market_id(payload)
        if not market_id:
            return None
        outcomes = _json_list(payload.get("outcomes"))
        prices = _json_list(payload.get("outcomePrices"))
        tokens = _json_list(payload.get("clobTokenIds") or payload.get("clob_token_ids"))
        if not tokens and (payload.get("yes_token_id") or payload.get("no_token_id")):
            tokens = [payload.get("yes_token_id", ""), payload.get("no_token_id", "")]
        if not outcomes and len(tokens) >= 2:
            outcomes = ["Yes", "No"]
        token_rows = payload.get("tokens")
        if isinstance(token_rows, list) and token_rows and isinstance(token_rows[0], dict):
            outcomes = [str(row.get("outcome") or row.get("name") or "") for row in token_rows]
            tokens = [str(row.get("token_id") or row.get("tokenId") or row.get("asset_id") or "") for row in token_rows]
        yes_index = next((i for i, value in enumerate(outcomes) if str(value).lower() in {"yes", "y"}), 0)
        no_index = next((i for i, value in enumerate(outcomes) if str(value).lower() in {"no", "n"}), 1)
        yes_token = str(tokens[yes_index]) if yes_index < len(tokens) else ""
        no_token = str(tokens[no_index]) if no_index < len(tokens) else ""
        if not yes_token or not no_token or len(outcomes) < 2:
            return None
        if {str(value).lower() for value in outcomes[:2]} != {"yes", "no"}:
            return None
        close = _timestamp_ms(payload.get("endDate") or payload.get("end_date_iso") or payload.get("endDateIso"))
        status = "active" if payload.get("active", True) and not payload.get("closed", False) else "closed"
        if payload.get("acceptingOrders") is False or payload.get("accepting_orders") is False:
            status = "paused"
        if payload.get("enableOrderBook") is False or payload.get("enable_order_book") is False:
            status = "paused"
        open_interest = payload.get("openInterest")
        if open_interest is None:
            open_interest = payload.get("open_interest")
        market = Market(
            market_id=market_id,
            native_market_id=market_id,
            venue="polymarket",
            title=str(payload.get("question") or payload.get("title") or ""),
            status=status,
            series_id=str(payload.get("eventId") or payload.get("event_id") or ""),
            event_id=str(payload.get("eventId") or payload.get("event_id") or ""),
            close_time_ms=close,
            price_level_structure="decimal",
            fractional_trading_enabled=True,
            legacy_tick_size_units=_price_units(payload.get("tickSize") or payload.get("tick_size") or "0.01"),
            market_url=(f"https://polymarket.com/event/{payload.get('slug')}" if payload.get("slug") else None),
            series_title=str(payload.get("eventTitle") or ""),
            volume_24h_units=_size_units(payload.get("volume24hr") or payload.get("volume24h") or payload.get("volume") or 0),
            open_interest_units=_optional_size_units(open_interest),
            yes_token_id=yes_token,
            no_token_id=no_token,
            min_order_size_units=_size_units(payload.get("minOrderSize") or payload.get("minimum_order_size") or 0),
            market_rules="direct_token_books",
        )
        if yes_token:
            self._asset_market[yes_token] = (market_id, "yes")
        if no_token:
            self._asset_market[no_token] = (market_id, "no")
        self._market_cache[market_id] = market
        return market

    def hydrate_market_open_interest(
        self,
        markets: Sequence[Market],
    ) -> tuple[list[Market], dict[str, int]]:
        """Hydrate per-market OI from Polymarket's Data API.

        Gamma catalog pages do not reliably carry a market-level open-interest
        value.  The Data API exposes that value through ``/oi`` and accepts
        a comma-separated ``market`` query parameter.  Missing or failed lookups stay
        ``None`` so callers can distinguish unavailable data from an explicit
        API value of zero.
        """

        source = list(markets)
        stats = {
            "batches": 0,
            "marketsRequested": len(source),
            "marketsResolved": 0,
            "marketsMissing": 0,
            "apiErrors": 0,
            "forbiddenResponses": 0,
            "cloudflare403": 0,
            "retries": 0,
            "retryExhausted": 0,
            "suppressedRequests": 0,
        }
        if not source:
            return [], stats

        requested_ids = [str(market.market_id) for market in source if market.market_id]
        resolved: dict[str, Optional[int]] = {}
        for offset in range(0, len(requested_ids), OPEN_INTEREST_BATCH_SIZE):
            batch = requested_ids[offset:offset + OPEN_INTEREST_BATCH_SIZE]
            if not batch:
                continue
            stats["batches"] += 1
            batch_number = offset // OPEN_INTEREST_BATCH_SIZE
            response = None
            blocked_remaining = self._open_interest_403_blocked_until - time.monotonic()
            if blocked_remaining > 0:
                # Do not amplify a provider-wide Cloudflare block by issuing
                # the same retry sequence for every catalog page.  Markets
                # remain explicitly missing and the mirror can still publish
                # books/catalog pages; a later sync retries after cooldown.
                stats["suppressedRequests"] += 1
                continue
            for attempt in range(1, int(self.config.open_interest_retry_attempts) + 1):
                try:
                    response = self._http_call(
                        self.data_http,
                        "get",
                        "/oi",
                        # The live Data API treats repeated ``market`` keys as a
                        # single value and only returns the last one. Its array
                        # parameter must be encoded as a comma-separated list.
                        params={"market": ",".join(batch)},
                        operation="polymarket_get_open_interest",
                    )
                    self._open_interest_403_blocked_until = 0.0
                    break
                except HTTPClientError as exc:
                    if exc.status_code != 403:
                        stats["apiErrors"] += 1
                        LOGGER.warning(
                            "POLYMARKET_OPEN_INTEREST_ERROR | batch=%s size=%s "
                            "status=%s attempt=%s/%s",
                            batch_number,
                            len(batch),
                            exc.status_code,
                            attempt,
                            self.config.open_interest_retry_attempts,
                        )
                        break

                    stats["forbiddenResponses"] += 1
                    metadata, is_cloudflare = self._open_interest_cloudflare_metadata(
                        exc,
                        batch=batch_number,
                        attempt=attempt,
                    )
                    if is_cloudflare:
                        stats["cloudflare403"] += 1
                        self._open_interest_diagnostics["lastCloudflare"] = metadata

                    if attempt >= int(self.config.open_interest_retry_attempts):
                        stats["apiErrors"] += 1
                        stats["retryExhausted"] += 1
                        self._open_interest_403_blocked_until = max(
                            self._open_interest_403_blocked_until,
                            time.monotonic() + float(self.config.open_interest_403_cooldown_seconds),
                        )
                        LOGGER.warning(
                            "POLYMARKET_OPEN_INTEREST_ERROR | batch=%s size=%s "
                            "status=403 attempt=%s/%s cfRay=%s exhausted=true",
                            batch_number,
                            len(batch),
                            attempt,
                            self.config.open_interest_retry_attempts,
                            metadata.get("cfRay") or "-",
                        )
                        break

                    delay = self._open_interest_retry_delay(attempt, exc.headers)
                    stats["retries"] += 1
                    LOGGER.warning(
                        "POLYMARKET_OPEN_INTEREST_RETRY | batch=%s size=%s "
                        "status=403 attempt=%s/%s cfRay=%s delayMs=%s",
                        batch_number,
                        len(batch),
                        attempt,
                        self.config.open_interest_retry_attempts,
                        metadata.get("cfRay") or "-",
                        int(round(delay * 1000)),
                    )
                    time.sleep(delay)
                except Exception as exc:
                    stats["apiErrors"] += 1
                    LOGGER.warning(
                        "POLYMARKET_OPEN_INTEREST_ERROR | batch=%s size=%s "
                        "error=%s",
                        batch_number,
                        len(batch),
                        _redact_proxy_text(exc, self.proxy_url),
                    )
                    break

            if response is None:
                continue

            if isinstance(response, Mapping):
                entries = response.get("data") or response.get("oi") or response.get("markets") or []
            else:
                entries = response
            if not isinstance(entries, list):
                stats["apiErrors"] += 1
                LOGGER.warning(
                    "POLYMARKET_OPEN_INTEREST_ERROR | batch=%s invalid response=%s",
                    offset // OPEN_INTEREST_BATCH_SIZE,
                    type(entries).__name__,
                )
                continue

            batch_ids = set(batch)
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                market_id = entry.get("market") or entry.get("conditionId") or entry.get("condition_id")
                if market_id is None or str(market_id) not in batch_ids:
                    continue
                value = entry.get("value")
                parsed = _optional_size_units(value)
                if value is not None and parsed is not None:
                    resolved[str(market_id)] = parsed

        stats["marketsResolved"] = sum(1 for market in source if str(market.market_id) in resolved)
        stats["marketsMissing"] = max(0, len(source) - stats["marketsResolved"])
        self._open_interest_metrics["batches"] += stats["batches"]
        self._open_interest_metrics["marketsRequested"] += stats["marketsRequested"]
        self._open_interest_metrics["marketsResolved"] += stats["marketsResolved"]
        self._open_interest_metrics["marketsMissing"] += stats["marketsMissing"]
        self._open_interest_metrics["apiErrors"] += stats["apiErrors"]
        self._open_interest_metrics["forbiddenResponses"] += stats["forbiddenResponses"]
        self._open_interest_metrics["cloudflare403"] += stats["cloudflare403"]
        self._open_interest_metrics["retries"] += stats["retries"]
        self._open_interest_metrics["retryExhausted"] += stats["retryExhausted"]
        self._open_interest_metrics["suppressedRequests"] += stats["suppressedRequests"]

        hydrated = [
            replace(market, open_interest_units=resolved.get(str(market.market_id)))
            for market in source
        ]
        for market in hydrated:
            self._market_cache[market.market_id] = market
        return hydrated, stats

    @staticmethod
    def _open_interest_headers(headers: Optional[Mapping[str, Any]]) -> dict[str, str]:
        return {
            str(key).lower(): str(value)[:200]
            for key, value in (headers or {}).items()
            if value is not None
        }

    def _open_interest_cloudflare_metadata(
        self,
        exc: HTTPClientError,
        *,
        batch: int,
        attempt: int,
    ) -> tuple[dict[str, Any], bool]:
        headers = self._open_interest_headers(getattr(exc, "headers", None))
        body = str(getattr(exc, "response_text", "") or "").lower()
        server = headers.get("server", "")
        is_cloudflare = bool(
            headers.get("cf-ray")
            or headers.get("cf-mitigated")
            or "cloudflare" in server.lower()
            or "cloudflare" in body
            or "attention required" in body
        )
        metadata = {
            "atMs": int(time.time() * 1000),
            "statusCode": int(exc.status_code),
            "batch": int(batch),
            "attempt": int(attempt),
            "cfRay": headers.get("cf-ray"),
            "cfCacheStatus": headers.get("cf-cache-status"),
            "server": headers.get("server"),
            "retryAfter": headers.get("retry-after"),
        }
        return metadata, is_cloudflare

    def _open_interest_retry_delay(
        self,
        attempt: int,
        headers: Optional[Mapping[str, Any]],
    ) -> float:
        base = min(
            float(self.config.open_interest_retry_max_delay_seconds),
            float(self.config.open_interest_retry_base_delay_seconds) * (2 ** max(0, attempt - 1)),
        )
        normalized = self._open_interest_headers(headers)
        retry_after = 0.0
        try:
            retry_after = max(0.0, float(normalized.get("retry-after", 0) or 0))
        except (TypeError, ValueError):
            pass
        delay = min(
            float(self.config.open_interest_retry_max_delay_seconds),
            max(base, retry_after),
        )
        jitter_fraction = float(self.config.open_interest_retry_jitter_fraction)
        if jitter_fraction > 0:
            delay += random.uniform(0.0, delay * jitter_fraction)
        return min(float(self.config.open_interest_retry_max_delay_seconds), delay)

    def prime_market(self, payload: Mapping[str, Any]) -> Optional[Market]:
        """Seed the worker cache from a screener pick's normalized metadata.

        Workers are separate processes from the screener and otherwise fall
        back to a full Gamma catalog scan for every condition ID. The pick
        already contains the condition and token IDs, so cache that identity
        before the first book request.
        """
        market = self._normalize_market(dict(payload))
        if market is not None:
            return market
        return None

    @staticmethod
    def _is_condition_id(identifier: str) -> bool:
        value = str(identifier).strip().lower()
        return (
            len(value) == 66
            and value.startswith("0x")
            and all(character in "0123456789abcdef" for character in value[2:])
        )

    def _gamma_market_lookup(self, identifier: str) -> Any:
        """Fetch a market using the identifier's Gamma API representation.

        The runtime uses condition IDs as its stable Polymarket market IDs.
        Gamma's ``/markets/{id}`` route expects the separate numeric market
        database ID, so condition IDs must be sent through the list endpoint's
        ``condition_ids`` filter.
        """

        if self._is_condition_id(identifier):
            response = self._http_call(self.gamma_http, "get",
                "/markets",
                params={"condition_ids": [str(identifier)], "limit": 1},
                operation="polymarket_get_market",
            )
            if isinstance(response, list):
                return response[0] if response else None
            if isinstance(response, dict):
                rows = response.get("data") or response.get("markets")
                if isinstance(rows, list):
                    return rows[0] if rows else None
            return response
        return self._http_call(self.gamma_http, "get", f"/markets/{identifier}", operation="polymarket_get_market")

    def _list_markets_keyset(self, query: MarketQuery) -> List[Market]:
        # Account positions identify Polymarket markets by condition ID.  A
        # bounded catalog page cannot resolve an arbitrary held position (the
        # catalog is ordered by market ID), so honor the shared ticker filter
        # with one filtered Gamma lookup per requested condition.
        requested = query.venue_filters.get("tickers") if query.venue_filters else None
        if requested:
            identifiers = [str(item).strip() for item in str(requested).split(",") if str(item).strip()]
            identifiers = list(dict.fromkeys(identifiers))[:max(0, int(query.max_results))]
            results: list[Market] = []
            for identifier in identifiers:
                payload = self._gamma_market_lookup(identifier)
                if isinstance(payload, dict):
                    nested = payload.get("market") or payload.get("data")
                    payload = nested if isinstance(nested, dict) else payload
                market = self._normalize_market(payload) if isinstance(payload, dict) else None
                if market is not None and (query.status != "open" or market.status == "active"):
                    results.append(market)
            return results
        results: list[Market] = []
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        target = max(0, int(query.max_results))
        while len(results) < target:
            if self._screener_cancel.is_set():
                LOGGER.info("POLYMARKET_SCAN_CANCELLED | markets=%s", len(results))
                break
            if cursor is not None:
                if cursor in seen_cursors:
                    LOGGER.warning("POLYMARKET_PAGINATION_CYCLE | cursor=%s", cursor)
                    break
                seen_cursors.add(cursor)
            limit = min(100, max(1, int(query.page_size)), target - len(results))
            params: dict[str, Any] = {"limit": limit, "closed": query.status in {"closed", "settled"}}
            if cursor:
                params["after_cursor"] = cursor
            response = self._http_call(self.gamma_http, "get", "/markets/keyset", params=params, operation="polymarket_list_markets")
            rows = response.get("data") if isinstance(response.get("data"), list) else response.get("markets")
            if not isinstance(rows, list):
                rows = response if isinstance(response, list) else []
            if not rows:
                break
            for payload in rows:
                market = self._normalize_market(payload)
                if market is not None and (query.status != "open" or market.status == "active"):
                    results.append(market)
                    if len(results) >= target:
                        break
            next_cursor = response.get("next_cursor") or response.get("nextCursor") or response.get("cursor")
            next_cursor_text = str(next_cursor) if next_cursor else ""
            if len(rows) < limit or not next_cursor or next_cursor_text == str(cursor):
                break
            if next_cursor_text in seen_cursors:
                LOGGER.warning("POLYMARKET_PAGINATION_CYCLE | cursor=%s", next_cursor_text)
                break
            cursor = next_cursor_text
        return results

    def _list_markets_bulk_catalog(self, query: MarketQuery) -> List[Market]:
        """Refresh the persistent catalog through parallel offset pages."""
        target = max(0, int(query.max_results))
        page_size = min(int(self.config.catalog_page_size), 10_000)
        workers = max(1, int(self.config.catalog_parallelism))
        if target <= 0:
            target = 100_000

        def fetch(offset: int, requested_page_size: int = page_size) -> tuple[int, list[Any]]:
            for attempt in range(3):
                try:
                    response = self._http_call(self.gamma_http, "get",
                        "/markets",
                        params={
                            "limit": requested_page_size,
                            "offset": offset,
                            "order": "id",
                            "ascending": True,
                            "closed": query.status in {"closed", "settled"},
                        },
                        operation="polymarket_catalog_page",
                    )
                    break
                except HTTPClientError as exc:
                    if exc.status_code not in {429, 500, 502, 503, 504} or attempt >= 2:
                        raise
                    time.sleep(0.25 * (2 ** attempt))
            if isinstance(response, list):
                rows = response
            elif isinstance(response, dict):
                rows = response.get("markets") or response.get("data") or []
            else:
                rows = []
            return offset, rows if isinstance(rows, list) else []

        started = time.perf_counter()
        results: list[Market] = []
        seen: set[str] = set()
        offset = 0
        terminal = False
        raw_rows = 0
        pages = 0
        duplicate_count = 0
        api_errors = 0
        offset_limit_error = False
        # Gamma deployments commonly cap /markets at 100 rows even when a
        # larger limit is requested. Probe once so parallel offsets match the
        # server's actual page width instead of skipping rows or terminating
        # after the first short response.
        try:
            _probe_offset, probe_rows = fetch(0)
        except Exception:
            raise
        if not probe_rows:
            self._catalog_metrics = {
                "catalogMarkets": 0, "rawCatalogRows": 0, "catalogPages": 1,
                "catalogDuplicateCount": 0, "catalogApiErrors": 0,
                "catalogDurationMs": int((time.perf_counter() - started) * 1000),
            }
            return []
        page_size = max(1, min(page_size, len(probe_rows)))
        prefetched: dict[int, list[Any]] = {0: probe_rows}
        while len(results) < target and not terminal:
            if self._screener_cancel.is_set():
                LOGGER.info("POLYMARKET_CATALOG_CANCELLED | markets=%s", len(results))
                break
            offsets = [offset + page_size * index for index in range(workers)]
            page_rows: dict[int, list[Any]] = {}
            pool = self._parallel_executor
            futures = [
                (item, None) if item in prefetched else (item, pool.submit(fetch, item, page_size))
                for item in offsets
            ]
            for item, future in futures:
                if future is None:
                    page_rows[item] = prefetched.pop(item)
                    continue
                try:
                    page_offset, rows = future.result()
                except Exception:
                    api_errors += 1
                    # Gamma rejects deep offset pages (currently offsets
                    # above roughly 2,000). Switch to its cursor endpoint
                    # instead of returning the warm, truncated catalog.
                    error = future.exception()
                    text = str(error).lower()
                    status_code = getattr(error, "status_code", None)
                    if status_code == 422 or "offset" in text:
                        offset_limit_error = True
                    continue
                page_rows[page_offset] = rows
            if offset_limit_error:
                return self._list_markets_keyset_catalog(query)
            if api_errors and not page_rows:
                raise RuntimeError("all Polymarket catalog pages failed")
            for page_offset in offsets:
                if page_offset not in page_rows:
                    continue
                rows = page_rows[page_offset]
                pages += 1
                raw_rows += len(rows)
                if len(rows) < page_size:
                    terminal = True
                for payload in rows:
                    market = self._normalize_market(payload) if isinstance(payload, dict) else None
                    if market is None or (query.status == "open" and market.status != "active"):
                        continue
                    if market.market_id in seen:
                        duplicate_count += 1
                        continue
                    seen.add(market.market_id)
                    results.append(market)
                    if len(results) >= target:
                        break
                if len(results) >= target or terminal:
                    break
            offset += page_size * workers
        if self.catalog_store is not None:
            self.catalog_store.upsert(results)
        self._catalog_metrics = {
            "catalogMarkets": len(results),
            "rawCatalogRows": raw_rows,
            "catalogPages": pages,
            "catalogDuplicateCount": duplicate_count,
            "catalogApiErrors": api_errors,
            "catalogDurationMs": int((time.perf_counter() - started) * 1000),
        }
        return results

    def _list_markets_keyset_catalog(self, query: MarketQuery) -> List[Market]:
        """Deep-pagination fallback for Gamma deployments that cap offsets."""
        started = time.perf_counter()
        results = self._list_markets_keyset(query)
        if self.catalog_store is not None:
            self.catalog_store.upsert(results)
        self._catalog_metrics = {
            "catalogMarkets": len(results),
            "rawCatalogRows": len(results),
            "catalogPages": max(1, (len(results) + 99) // 100),
            "catalogDuplicateCount": 0,
            "catalogApiErrors": 0,
            "catalogPagination": "keyset",
            "catalogDurationMs": int((time.perf_counter() - started) * 1000),
        }
        return results

    def _catalog_markets(self, query: MarketQuery) -> List[Market]:
        assert self.catalog_store is not None
        now = int(time.time() * 1000)
        refresh_ms = 300_000
        with self._catalog_refresh_lock:
            status_key = query.status or "open"
            if (
                self.catalog_store.count() == 0
                or status_key not in self._catalog_loaded_statuses
                or now - self._catalog_last_refresh_ms >= refresh_ms
            ):
                try:
                    self._list_markets_bulk_catalog(query)
                    self._catalog_last_refresh_ms = now
                    self._catalog_loaded_statuses.add(status_key)
                except Exception as exc:
                    self._catalog_metrics = {
                        "catalogMarkets": self.catalog_store.count(),
                        "catalogApiErrors": 1,
                        "catalogRefreshError": str(exc),
                    }
                    LOGGER.warning("POLYMARKET_CATALOG_REFRESH_ERROR | error=%s", exc)
                    # A warm catalog remains usable during a transient Gamma
                    # outage. Cold starts still surface the original error.
                    if self.catalog_store.count() == 0:
                        raise
        status = "active" if query.status == "open" else ("closed" if query.status else "")
        markets = self.catalog_store.list_markets(status=status, limit=max(0, int(query.max_results)))
        self.book_cache.expected_assets = max(self.book_cache.expected_assets, self.catalog_store.count() * 2)
        for market in markets:
            self._market_cache[market.market_id] = market
            if market.yes_token_id:
                self._asset_market[market.yes_token_id] = (market.market_id, "yes")
            if market.no_token_id:
                self._asset_market[market.no_token_id] = (market.market_id, "no")
        return markets

    def list_markets(self, query: MarketQuery) -> List[Market]:
        requested = query.venue_filters.get("tickers") if query.venue_filters else None
        if self.catalog_store is not None and not requested:
            return self._catalog_markets(query)
        markets = self._list_markets_keyset(query)
        self.book_cache.expected_assets = max(self.book_cache.expected_assets, len(markets) * 2)
        return markets

    def _raw_market(self, market_id: str) -> Market:
        cached = self._market_cache.get(market_id)
        if cached is not None:
            return cached
        try:
            payload = self._gamma_market_lookup(market_id)
            if isinstance(payload, dict):
                nested = payload.get("market") or payload.get("data")
                payload = nested if isinstance(nested, dict) else payload
            market = self._normalize_market(payload) if isinstance(payload, dict) else None
            if market is not None and market.market_id == market_id:
                self._market_cache[market_id] = market
                return market
        except Exception:
            # Preserve the catalog fallback for older Gamma deployments and
            # test doubles that do not implement the single-market endpoint.
            pass
        for market in self.list_markets(MarketQuery(max_results=10_000)):
            if market.market_id == market_id:
                return market
        raise KeyError(f"Polymarket market not found: {market_id}")

    def _get_book(self, token_id: str) -> dict[str, dict[int, int]]:
        if not token_id:
            return {"bids": {}, "asks": {}}
        payload = self._http_call(self.http_client, "get", "/book", params={"token_id": token_id}, operation="polymarket_get_order_book")
        if isinstance(payload, Mapping):
            # A worker metadata lookup is also a cache recovery path. Keep the
            # compact top level so the following actor can start from local
            # state instead of repeating the same token request.
            self.book_cache.update_book(
                token_id,
                {**payload, "asset_id": token_id},
                timestamp_ms=_timestamp_ms(payload.get("timestamp")) or 0,
            )
        return {"bids": _book_levels(payload.get("bids")), "asks": _book_levels(payload.get("asks"))}

    def get_market(self, market_id: str) -> Market:
        market = self._raw_market(market_id)
        def cached_levels(token_id: Optional[str]) -> Optional[dict[str, dict[int, int]]]:
            top = self.book_cache.get(token_id or "") if token_id else None
            if top is None or top.bid_units is None or top.ask_units is None:
                return None
            return {
                "bids": {int(top.bid_units): int(top.bid_size_units or 0)},
                "asks": {int(top.ask_units): int(top.ask_size_units or 0)},
            }

        yes = cached_levels(market.yes_token_id) or self._get_book(market.yes_token_id)
        no = cached_levels(market.no_token_id) or self._get_book(market.no_token_id)
        return self._market_with_books(market, yes, no)

    @staticmethod
    def _market_with_books(
        market: Market,
        yes: Mapping[str, dict[int, int]],
        no: Mapping[str, dict[int, int]],
    ) -> Market:
        yes_bid, yes_bid_size = _best(yes["bids"], high=True)
        yes_ask, yes_ask_size = _best(yes["asks"], high=False)
        no_bid, no_bid_size = _best(no["bids"], high=True)
        no_ask, no_ask_size = _best(no["asks"], high=False)
        return Market(**{**market.__dict__, "yes_bid_units": yes_bid, "yes_ask_units": yes_ask, "no_bid_units": no_bid, "no_ask_units": no_ask, "yes_bid_size_units": yes_bid_size, "yes_ask_size_units": yes_ask_size, "no_bid_size_units": no_bid_size, "no_ask_size_units": no_ask_size})

    def _book_payload(self, payload: Any) -> dict[str, dict[int, int]]:
        if not isinstance(payload, Mapping):
            return {"bids": {}, "asks": {}}
        return {"bids": _book_levels(payload.get("bids")), "asks": _book_levels(payload.get("asks"))}

    def hydrate_market_books(self, markets: Sequence[Market], *, allow_rest: bool = True) -> Mapping[str, Market]:
        """Hydrate many markets with bounded POST /books requests.

        The response is a partial mapping: markets missing either token book
        are omitted so callers can safely exclude them for this cycle.
        """
        candidates = [m for m in markets if m.yes_token_id and m.no_token_id]
        if not candidates:
            return {}
        self.book_cache.expected_assets = max(self.book_cache.expected_assets, len(candidates) * 2)
        by_token = {
            token: (market, side)
            for market in candidates
            for token, side in ((market.yes_token_id, "yes"), (market.no_token_id, "no"))
        }
        books: dict[str, dict[str, dict[int, int]]] = {}
        for token in list(by_token):
            cached = self.book_cache.get(token)
            if cached is None:
                continue
            books[token] = {
                "bids": ({cached.bid_units: cached.bid_size_units}
                          if cached.bid_units is not None and cached.bid_size_units is not None else {}),
                "asks": ({cached.ask_units: cached.ask_size_units}
                          if cached.ask_units is not None and cached.ask_size_units is not None else {}),
            }
        by_token = {token: value for token, value in by_token.items() if token not in books}
        if not by_token:
            hydrated = {
                market.market_id: self._market_with_books(
                    market,
                    books[market.yes_token_id],
                    books[market.no_token_id],
                )
                for market in candidates
                if market.yes_token_id in books and market.no_token_id in books
            }
            self._book_metrics = {
                "bookHydrationBatches": 0,
                "restFallbackRequests": 0,
                "bookAssetsExpected": len(candidates) * 2,
                "bookAssetsReady": self.book_cache.ready_count(),
                "bookAssetsMissing": max(0, len(candidates) * 2 - self.book_cache.ready_count()),
            }
            self.book_cache.persist()
            return hydrated
        if not allow_rest:
            hydrated = {
                market.market_id: self._market_with_books(
                    market, books[market.yes_token_id], books[market.no_token_id]
                )
                for market in candidates
                if market.yes_token_id in books and market.no_token_id in books
            }
            self._book_metrics = {
                "bookHydrationBatches": 0,
                "restFallbackRequests": 0,
                "bookAssetsExpected": len(candidates) * 2,
                "bookAssetsReady": self.book_cache.ready_count(),
                "bookAssetsMissing": max(0, len(candidates) * 2 - self.book_cache.ready_count()),
            }
            return hydrated
        batch_size = max(1, int(self.config.book_batch_size))
        token_ids = list(by_token)
        batches = [token_ids[index:index + batch_size] for index in range(0, len(token_ids), batch_size)]
        fallback_requests = 0

        def fetch(batch: list[str]) -> list[Any]:
            for attempt in range(3):
                try:
                    response = self._http_call(self.http_client, "post",
                        "/books",
                        body=[{"token_id": token} for token in batch],
                        operation="polymarket_get_order_books",
                    )
                    break
                except HTTPClientError as exc:
                    if exc.status_code != 429 or attempt >= 2:
                        raise
                    time.sleep(0.25 * (2 ** attempt))
            if isinstance(response, list):
                return response
            if isinstance(response, dict):
                rows = response.get("data") or response.get("books") or []
                return rows if isinstance(rows, list) else []
            return []

        def fetch_with_split(batch: list[str]) -> list[Any]:
            try:
                return fetch(batch)
            except Exception as exc:
                # The provider reports oversized payloads as a 400. Split
                # adaptively; other failures are handled by the individual
                # bounded fallback below.
                text = str(exc).lower()
                status_code = getattr(exc, "status_code", None)
                if len(batch) > 1 and (
                    status_code in {400, 413, 422}
                    or "payload" in text
                    or "too many" in text
                    or "limit" in text
                    or "400" in text
                ):
                    midpoint = max(1, len(batch) // 2)
                    return fetch_with_split(batch[:midpoint]) + fetch_with_split(batch[midpoint:])
                raise

        failures: list[str] = []
        pool = self._parallel_executor
        future_batches = {pool.submit(fetch_with_split, batch): batch for batch in batches}
        for future in as_completed(future_batches):
            batch = future_batches[future]
            try:
                rows = future.result()
            except Exception:
                failures.extend(batch)
                continue
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                asset = str(row.get("asset_id") or row.get("assetId") or "")
                if asset in by_token:
                    books[asset] = self._book_payload(row)
                    self.book_cache.update_book(asset, row, timestamp_ms=_timestamp_ms(row.get("timestamp")) or 0)

        # Retry only failed tokens, with the same bounded executor. This path
        # is intentionally separate from the normal bulk requests so a bad
        # batch cannot turn into 200,000 serial calls.
        if failures:
            fallback_requests = len(failures)
            def fetch_one(token: str) -> tuple[str, Optional[dict[str, dict[int, int]]]]:
                try:
                    response = self._http_call(self.http_client, "get",
                        "/book", params={"token_id": token}, operation="polymarket_get_order_book"
                    )
                    if isinstance(response, Mapping):
                        self.book_cache.update_book(
                            token,
                            {**response, "asset_id": token},
                            timestamp_ms=_timestamp_ms(response.get("timestamp")) or 0,
                        )
                    return token, self._book_payload(response)
                except Exception:
                    return token, None
            for token, payload in pool.map(fetch_one, failures):
                if payload is not None:
                    books[token] = payload

        hydrated: dict[str, Market] = {}
        for market in candidates:
            yes = books.get(market.yes_token_id)
            no = books.get(market.no_token_id)
            if yes is None or no is None:
                continue
            hydrated[market.market_id] = self._market_with_books(market, yes, no)
        self._book_metrics = {
            "bookHydrationBatches": len(batches),
            "restFallbackRequests": fallback_requests,
            "bookAssetsExpected": len(candidates) * 2,
            "bookAssetsReady": self.book_cache.ready_count(),
            "bookAssetsMissing": max(0, len(candidates) * 2 - self.book_cache.ready_count()),
        }
        self.book_cache.persist()
        return hydrated

    def rehydrate_stale_books(self) -> Mapping[str, Market]:
        """Refresh markets whose compact cache no longer has a usable book."""
        stale_markets: dict[str, Market] = {}
        for asset, (market_id, _side) in self._asset_market.items():
            if self.book_cache.get(asset) is not None:
                continue
            market = self._market_cache.get(market_id)
            if market is not None:
                stale_markets[market_id] = market
        if not stale_markets:
            return {}
        return self.hydrate_market_books(tuple(stale_markets.values()), allow_rest=True)

    def get_market_quote(self, market_id: str) -> MarketQuote:
        market = self.get_market(market_id)
        return MarketQuote(market_id, market.yes_bid_units, market.no_bid_units, venue="polymarket")

    def get_account_balance(self) -> AccountBalance:
        sdk = self._require_auth()
        params = _BalanceAllowanceParams("COLLATERAL", None)
        method = getattr(sdk, "get_balance_allowance", None) or getattr(sdk, "getBalanceAllowance", None)
        if method is None:
            raise RuntimeError("Polymarket SDK does not expose balance/allowance")
        payload = self._sdk_call("polymarket_get_balance_allowance", lambda: method(params))
        value = payload.get("balance") if isinstance(payload, Mapping) else getattr(payload, "balance", None)
        if value in (None, ""):
            raise ValueError("Polymarket balance response is missing balance")
        return AccountBalance(
            _collateral_units(value),
            None,
            currency="USDC",
            venue="polymarket",
            allowance_units=_collateral_allowance(payload),
        )

    def get_account_limits(self) -> AccountLimits:
        # The provider does not return Kalshi-style account limits. Return a
        # conservative local pacing budget instead of zeros: the broker uses
        # these buckets to decide whether queued reads may run at all.
        return AccountLimits(
            "polymarket-local",
            RateLimitBucket(LOCAL_READ_RATE, LOCAL_READ_RATE * 2),
            RateLimitBucket(LOCAL_WRITE_RATE, LOCAL_WRITE_RATE * 2),
        )

    def _data_user(self) -> str:
        configured = self.config.funder_address or os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
        if configured:
            return str(configured).strip()
        # EOA accounts can use the signer address directly. Proxy/Safe
        # accounts still need an explicit funder address because their
        # positions are held by the proxy contract.
        if self.config.public_only or not self._private_key_value():
            return ""
        try:
            sdk = self._require_auth()
            address = getattr(sdk, "get_address", None)
            return str(address() if callable(address) else "").strip()
        except Exception:
            return ""

    def list_account_positions(self, query: AccountPositionQuery = AccountPositionQuery()) -> List[AccountPosition]:
        user = self._data_user()
        if not user:
            return []
        payload = self._http_call(self.data_http, "get", "/positions", params={"user": user, "sizeThreshold": 0 if not query.nonzero_only else 0}, operation="polymarket_list_positions")
        rows = payload.get("data", payload) if isinstance(payload, dict) else payload
        result: list[AccountPosition] = []
        for row in rows if isinstance(rows, list) else []:
            size = _size_units(row.get("size", 0))
            if query.nonzero_only and size == 0:
                continue
            condition_id = str(row.get("conditionId") or row.get("condition_id") or row.get("asset") or "")
            outcome = str(row.get("outcome") or "").strip().lower() or None
            signed_size = -size if outcome in {"no", "n"} else size
            current_value = row.get("currentValue")
            current_value_units = _money_units(current_value) if current_value not in (None, "") else None
            mark_price = row.get("curPrice")
            mark_price_units = _price_units(mark_price) if mark_price not in (None, "") else None
            if mark_price_units is None and current_value_units is not None and size:
                # Data API currentValue is dollar notional and size is token
                # count. Derive a normalized price when curPrice is omitted.
                mark_price_units = _price_units(
                    _decimal(current_value) / (_decimal(row.get("size")) or Decimal("1"))
                )
            initial_value = row.get("initialValue")
            if initial_value not in (None, ""):
                exposure_units = _money_units(initial_value)
            else:
                average_price = row.get("avgPrice")
                exposure_units = (
                    _money_units(_decimal(average_price) * _decimal(row.get("size")))
                    if average_price not in (None, "") else None
                )
            realized = row.get("realizedPnl")
            realized_units = _money_units(realized) if realized not in (None, "") else None
            slug = str(row.get("slug") or "").strip()
            result.append(AccountPosition(
                condition_id,
                signed_size,
                market_exposure_units=exposure_units,
                realized_pnl_units=realized_units,
                updated_at_ms=_timestamp_ms(row.get("updatedAt") or row.get("timestamp")),
                venue="polymarket",
                mark_price_units=mark_price_units,
                current_value_units=current_value_units,
                title=str(row.get("title") or "").strip(),
                market_url=f"https://polymarket.com/event/{slug}" if slug else None,
                outcome=outcome,
            ))
        return result

    @staticmethod
    def _rows(payload: Any, *keys: str) -> list[dict]:
        if isinstance(payload, list):
            return [row if isinstance(row, dict) else vars(row) for row in payload if isinstance(row, dict) or hasattr(row, "__dict__")]
        if isinstance(payload, dict):
            for key in keys:
                if isinstance(payload.get(key), list):
                    return [row if isinstance(row, dict) else vars(row) for row in payload[key] if isinstance(row, dict) or hasattr(row, "__dict__")]
        return []

    def list_account_orders(self, query: AccountOrderQuery = AccountOrderQuery()) -> List[AccountOrder]:
        sdk = self._require_auth()
        method = getattr(sdk, "get_open_orders", None) or getattr(sdk, "get_orders", None) or getattr(sdk, "getOrders", None)
        if method is None:
            raise RuntimeError("Polymarket SDK does not expose orders")
        if not query.market_id:
            payload = self._sdk_call("polymarket_list_account_orders", method)
        else:
            try:
                from py_clob_client_v2 import OpenOrderParams  # type: ignore
                payload = self._sdk_call(
                    "polymarket_list_account_orders",
                    lambda: method(OpenOrderParams(market=query.market_id)),
                )
            except ImportError:
                payload = self._sdk_call(
                    "polymarket_list_account_orders",
                    lambda: method({"market": query.market_id}),
                )
        result: list[AccountOrder] = []
        for row in self._rows(payload, "data", "orders"):
            initial = _size_units(row.get("original_size") or row.get("originalSize") or row.get("size"))
            matched = _size_units(row.get("size_matched") or row.get("sizeMatched"))
            action = str(row.get("side") or row.get("action") or "").lower()
            asset = str(row.get("asset_id") or row.get("assetId") or row.get("token_id") or "")
            mapped_market, mapped_side = self._event_for_asset(asset) if asset else ("", "")
            market_id = str(row.get("market") or row.get("condition_id") or mapped_market or "")
            outcome = str(row.get("outcome") or "").lower()
            side = outcome if outcome in {"yes", "no"} else (mapped_side if mapped_side in {"yes", "no"} else None)
            result.append(AccountOrder(
                str(row.get("id") or row.get("order_id") or ""),
                market_id,
                side,
                client_order_id=str(row.get("client_order_id") or row.get("clientOrderId") or ""),
                status=str(row.get("status") or ""),
                price_units=_price_units(row.get("price")) if row.get("price") is not None else None,
                fill_count_units=matched,
                remaining_count_units=max(0, initial - matched),
                initial_count_units=initial,
                created_at_ms=_timestamp_ms(row.get("created_at") or row.get("createdAt")),
                expiration_time_ms=_timestamp_ms(row.get("expiration")),
                venue="polymarket",
                action=action if action in {"buy", "sell"} else None,
            ))
        if query.status:
            wanted = str(query.status).lower()
            result = [item for item in result if str(item.status).lower() == wanted]
        return result[: max(0, int(query.max_results))] if query.max_results > 0 else result

    def list_account_fills(self, query: AccountFillQuery = AccountFillQuery()) -> List[AccountFill]:
        sdk = self._require_auth()
        method = getattr(sdk, "get_trades", None) or getattr(sdk, "getTrades", None)
        if method is None:
            raise RuntimeError("Polymarket SDK does not expose trades")
        try:
            from py_clob_client_v2 import TradeParams  # type: ignore
            payload = self._sdk_call(
                "polymarket_list_account_fills",
                lambda: method(TradeParams(market=query.market_id, after=(query.min_created_at_ms // 1000 if query.min_created_at_ms else None), before=(query.max_created_at_ms // 1000 if query.max_created_at_ms else None)))
                if (query.market_id or query.min_created_at_ms or query.max_created_at_ms) else method(),
            )
        except ImportError:
            payload = self._sdk_call("polymarket_list_account_fills", method)
        result: list[AccountFill] = []
        for row in self._rows(payload, "data", "trades"):
            asset = str(row.get("asset_id") or row.get("assetId") or row.get("token_id") or "")
            mapped_market, mapped_side = self._event_for_asset(asset) if asset else ("", "")
            outcome = str(row.get("outcome") or "").lower()
            side = outcome if outcome in {"yes", "no"} else (mapped_side if mapped_side in {"yes", "no"} else None)
            result.append(AccountFill(
                str(row.get("id") or row.get("trade_id") or ""),
                str(row.get("id") or row.get("trade_id") or ""),
                str(row.get("order_id") or ""),
                str(row.get("market") or row.get("condition_id") or mapped_market or ""),
                side,
                _size_units(row.get("size")),
                _price_units(row.get("price")) if row.get("price") is not None else None,
                _money_units(row.get("fee") or 0),
                _timestamp_ms(row.get("match_time") or row.get("created_at")),
                str(row.get("side") or "").upper() == "TAKER",
                venue="polymarket",
            ))
        return result[: max(0, int(query.max_results))] if query.max_results > 0 else result

    def get_positions(self, market_id: str) -> List[Position]:
        return [Position(item.market_id, item.position_units, venue="polymarket") for item in self.list_account_positions() if item.market_id == market_id]

    def get_resting_orders(self, market_id: str) -> List[Order]:
        return [Order(item.order_id, item.market_id, item.side, item.client_order_id, item.status, item.price_units, item.fill_count_units, item.remaining_count_units, item.expiration_time_ms, venue="polymarket") for item in self.list_account_orders(AccountOrderQuery(market_id=market_id)) if item.status.lower() in {"live", "resting", "open"}]

    def get_order_queue_position(self, order_id: str) -> QueuePosition:
        return QueuePosition(order_id, None)

    def get_series(self, series_id: str) -> Series:
        return Series(series_id, fee_type="none", title=series_id)

    def get_series_fee_changes(self, series_id: str, *, show_historical: bool = False) -> List[SeriesFeeChange]:
        return []

    def get_incentive_programs(self, *, status: str = "active", incentive_type: str = "all", limit: int = 10_000) -> List[IncentiveProgram]:
        return []

    def _order_payload(self, request: CreateOrderRequest) -> dict[str, Any]:
        market = self._raw_market(request.market_id)
        token_id = market.yes_token_id if request.side == "yes" else market.no_token_id
        if not token_id:
            raise ValueError(f"Polymarket market {request.market_id} has no {request.side.upper()} token")
        if not 0 < int(request.price_units) < PRICE_SCALE:
            raise ValueError("Polymarket price must be strictly between 0 and 1")
        tick_units = max(1, int(market.legacy_tick_size_units or 1))
        if int(request.price_units) % tick_units:
            raise ValueError(
                f"Polymarket price must align to the market tick size ({tick_units} units)"
            )
        if int(request.count_units) <= 0 or (market.min_order_size_units and int(request.count_units) < market.min_order_size_units):
            raise ValueError("Polymarket order size is below the market minimum")
        tif = str(request.time_in_force or "GTC").upper()
        if tif not in {"GTC", "GTD", "FOK", "FAK"}:
            raise ValueError("Polymarket time in force must be GTC, GTD, FOK, or FAK")
        if request.post_only and tif in {"FOK", "FAK"}:
            raise ValueError("Polymarket post-only orders cannot use FOK or FAK")
        if tif == "GTD" and not request.expiration_timestamp_seconds:
            raise ValueError("Polymarket GTD orders require an expiration timestamp")
        if request.post_only:
            # Reject an immediately crossing post-only order when a current
            # normalized book is available.  A missing book is left to the
            # venue, since rejecting it locally would make reconnects unsafe.
            best_opposite = (
                market.yes_ask_units if request.action == "buy" and request.side == "yes" else
                market.no_ask_units if request.action == "buy" else
                market.yes_bid_units if request.side == "yes" else market.no_bid_units
            )
            crosses = (
                best_opposite is not None and
                ((request.action == "buy" and request.price_units >= best_opposite) or
                 (request.action == "sell" and request.price_units <= best_opposite))
            )
            if crosses:
                raise PostOnlyCrossError("Polymarket post-only order would cross the book")
        price = Decimal(request.price_units) / PRICE_SCALE
        size = Decimal(request.count_units) / TOKEN_SCALE
        return {"tokenID": token_id, "price": str(price), "size": str(size), "side": request.action.upper(), "orderType": tif, "postOnly": bool(request.post_only), "expiration": request.expiration_timestamp_seconds or 0, "clientOrderId": request.client_order_id}

    def create_order(self, request: CreateOrderRequest) -> Order:
        # Validate the normalized request before either SDK path.  The V2
        # helper below intentionally bypasses the legacy dict payload, so the
        # same tick, size, TIF, and post-only checks must still run here.
        self._order_payload(request)
        if self.dry_run:
            return Order(f"dry-run:{request.client_order_id}", request.market_id, request.side, request.client_order_id, "resting", request.price_units, 0, request.count_units, venue="polymarket")
        sdk = self._require_auth()
        v2_method = getattr(sdk, "create_and_post_order", None)
        method = v2_method or getattr(sdk, "post_order", None) or getattr(sdk, "postOrder", None)
        if method is None:
            raise RuntimeError("Polymarket SDK does not expose order placement")
        if v2_method is not None:
            try:
                from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side  # type: ignore
                market = self._raw_market(request.market_id)
                token_id = market.yes_token_id if request.side == "yes" else market.no_token_id
                order_args = {
                    "token_id": token_id,
                    "price": float(Decimal(request.price_units) / PRICE_SCALE),
                    "size": float(Decimal(request.count_units) / TOKEN_SCALE),
                    "side": Side.BUY if request.action == "buy" else Side.SELL,
                }
                if request.expiration_timestamp_seconds:
                    # V2 includes GTD expiry in the signed OrderArgs rather
                    # than in the post-order options.
                    order_args["expiration"] = int(request.expiration_timestamp_seconds)
                response = self._sdk_call(
                    "polymarket_create_order",
                    lambda: method(
                        OrderArgs(**order_args),
                        options=PartialCreateOrderOptions(tick_size=str(Decimal(market.legacy_tick_size_units) / PRICE_SCALE)),
                        order_type=getattr(OrderType, str(request.time_in_force or "GTC").upper()),
                        post_only=bool(request.post_only),
                    ),
                )
            except ImportError:
                response = self._sdk_call("polymarket_create_order", lambda: method(self._order_payload(request)))
        else:
            response = self._sdk_call("polymarket_create_order", lambda: method(self._order_payload(request)))
        row = response if isinstance(response, dict) else getattr(response, "__dict__", {})
        return Order(str(row.get("orderID") or row.get("id") or request.client_order_id), request.market_id, request.side, request.client_order_id, str(row.get("status") or "live"), request.price_units, 0, request.count_units, venue="polymarket")

    def cancel_order(self, *, order_id: str) -> Order:
        if self.dry_run:
            return Order(order_id, status="canceled", venue="polymarket")
        sdk = self._require_auth()
        # CLOB v2's ``cancel_order`` takes an OrderPayload object, rather than
        # the string order id accepted by the older SDK method names.  Passing
        # the string directly reaches ``payload.orderID`` inside v2 and raises
        # ``AttributeError: 'str' object has no attribute 'orderID'``.
        cancel_order = getattr(sdk, "cancel_order", None)
        cancel = getattr(sdk, "cancel", None)
        cancel_order_legacy = getattr(sdk, "cancelOrder", None)
        method = cancel_order or cancel or cancel_order_legacy
        if method is None:
            raise RuntimeError("Polymarket SDK does not expose order cancellation")
        if cancel_order is not None:
            try:
                from py_clob_client_v2 import OrderPayload  # type: ignore
            except ImportError:
                payload = SimpleNamespace(orderID=str(order_id))
            else:
                payload = OrderPayload(orderID=str(order_id))
            self._sdk_call("polymarket_cancel_order", lambda: method(payload))
        else:
            self._sdk_call("polymarket_cancel_order", lambda: method(order_id))
        return Order(order_id, status="canceled", venue="polymarket")

    def amend_order(self, request: AmendOrderRequest) -> Order:
        self.cancel_order(order_id=request.order_id)
        return self.create_order(CreateOrderRequest(request.market_id, request.side, request.new_price_units, request.new_total_fillable_count_units, request.updated_client_order_id, None, action=request.action, venue="polymarket"))

    def decrease_order_to(self, *, order_id: str, remaining_count_units: int) -> Order:
        if remaining_count_units <= 0:
            return self.cancel_order(order_id=order_id)
        order = next((item for item in self.list_account_orders(AccountOrderQuery()) if item.order_id == order_id), None)
        if order is None or order.price_units is None or order.side not in {"yes", "no"}:
            raise OrderNotFoundError(f"Polymarket order {order_id} cannot be decreased")
        return self.amend_order(AmendOrderRequest(
            order_id=order_id,
            market_id=order.market_id,
            side=order.side,
            new_price_units=order.price_units,
            new_total_fillable_count_units=int(remaining_count_units),
            previous_client_order_id=order.client_order_id,
            updated_client_order_id=f"{order.client_order_id}:decrease:{int(time.time() * 1000)}",
            action=order.action or "buy",
            venue="polymarket",
        ))

    def _event_for_asset(self, asset_id: str) -> tuple[str, str]:
        return self._asset_market.get(str(asset_id), (str(asset_id), "yes"))

    def _parse_ws_message(self, payload: Any, fallback_market_id: str = "") -> list[MarketEvent]:
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                return []
        rows = payload if isinstance(payload, list) else [payload]
        events: list[MarketEvent] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("asset_id") or row.get("assetId") or row.get("token_id") or "")
            market_id, side = self._event_for_asset(asset) if asset else (fallback_market_id, "yes")
            event_type = str(row.get("event_type") or row.get("eventType") or row.get("type") or "")
            if event_type == "price_change" and isinstance(row.get("price_changes"), list):
                for change in row["price_changes"]:
                    if isinstance(change, dict):
                        events.extend(self._parse_ws_message({**change, "event_type": "price_change", "timestamp": row.get("timestamp")}, fallback_market_id=str(row.get("market") or fallback_market_id)))
                continue
            if ("order" in event_type or "trade" in event_type or "fill" in event_type or "match" in event_type) and (row.get("order_id") or row.get("orderId") or row.get("status")):
                events.extend(self._parse_user_ws_message(row))
                continue
            if event_type in {"book", "book_snapshot", "snapshot"} or "bids" in row:
                market = self._market_cache.get(market_id)
                if market is None:
                    continue
                # A market-channel snapshot is already the authoritative
                # book for the asset in the message. Do not issue REST calls
                # while parsing websocket traffic; update the compact cache
                # and compose the actor snapshot from the two cached token
                # tops instead.
                if asset:
                    self.book_cache.update_book(
                        asset,
                        row,
                        timestamp_ms=_timestamp_ms(row.get("timestamp")) or 0,
                    )
                yes_top = self.book_cache.get(market.yes_token_id or "")
                no_top = self.book_cache.get(market.no_token_id or "")
                yes_bids = (
                    {int(yes_top.bid_units): int(yes_top.bid_size_units or 0)}
                    if yes_top is not None and yes_top.bid_units is not None else {}
                )
                no_bids = (
                    {int(no_top.bid_units): int(no_top.bid_size_units or 0)}
                    if no_top is not None and no_top.bid_units is not None else {}
                )
                yes_asks = (
                    {int(yes_top.ask_units): int(yes_top.ask_size_units or 0)}
                    if yes_top is not None and yes_top.ask_units is not None else {}
                )
                no_asks = (
                    {int(no_top.ask_units): int(no_top.ask_size_units or 0)}
                    if no_top is not None and no_top.ask_units is not None else {}
                )
                events.append(OrderBookSnapshot(
                    market_id,
                    row.get("sequence"),
                    yes_bids,
                    no_bids,
                    venue="polymarket",
                    yes_ask_levels=yes_asks,
                    no_ask_levels=no_asks,
                ))
            elif event_type in {"price_change", "book_delta", "delta"}:
                price = row.get("price")
                size = row.get("size") or row.get("delta")
                if price is not None and size is not None:
                    events.append(OrderBookDelta(market_id, row.get("sequence"), side, _price_units(price), _size_units(size), _timestamp_ms(row.get("timestamp") or row.get("ts")) or int(time.time() * 1000), venue="polymarket", is_ask=str(row.get("side") or "").upper() in {"SELL", "ASK"}))
            elif event_type in {"last_trade_price", "trade", "public_trade"}:
                events.append(PublicTrade(market_id, str(row.get("id") or row.get("trade_id") or ""), _timestamp_ms(row.get("timestamp") or row.get("ts")) or int(time.time() * 1000), _price_units(row.get("price")) if side == "yes" else 0, _price_units(row.get("price")) if side == "no" else 0, _size_units(row.get("size")), str(row.get("side") or ""), venue="polymarket"))
        return events

    def _parse_user_ws_message(self, payload: Any) -> list[MarketEvent]:
        """Normalize user-channel order and trade notifications.

        Polymarket has used both snake_case and camelCase fields in the user
        channel.  Keep the raw lifecycle status intact so callers can handle
        ``LIVE``, ``MATCHED``, ``CANCELED``, ``FAILED`` and future statuses.
        """
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                return []
        rows = payload if isinstance(payload, list) else [payload]
        events: list[MarketEvent] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            event_type = str(row.get("event_type") or row.get("eventType") or row.get("type") or "").lower()
            event_id = str(row.get("id") or row.get("order_id") or row.get("orderId") or row.get("trade_id") or "")
            dedupe_key = f"{event_type}:{event_id}:{row.get('status') or row.get('size_matched') or row.get('sizeMatched') or ''}"
            if event_id and dedupe_key in self._seen_user_events:
                continue
            if event_id:
                self._seen_user_events.add(dedupe_key)
            asset = str(row.get("asset_id") or row.get("assetId") or row.get("token_id") or row.get("market") or "")
            market_id, mapped_side = self._event_for_asset(asset)
            side = str(row.get("outcome") or row.get("side_outcome") or mapped_side).lower()
            if side not in {"yes", "no"}:
                side = mapped_side if mapped_side in {"yes", "no"} else "yes"
            if "order" in event_type or any(key in row for key in ("order_id", "orderId", "status")):
                events.append(OrderUpdate(
                    market_id=market_id,
                    side=side,  # type: ignore[arg-type]
                    order_id=event_id,
                    client_order_id=str(row.get("client_order_id") or row.get("clientOrderId") or ""),
                    status=str(row.get("status") or event_type or "unknown"),
                    fill_count_units=_size_units(row.get("size_matched") or row.get("sizeMatched") or 0),
                    remaining_count_units=_size_units(row.get("remaining_size") or row.get("remainingSize") or 0),
                    price_units=_price_units(row.get("price")) if row.get("price") is not None else None,
                    expiration_time_ms=_timestamp_ms(row.get("expiration")),
                    venue="polymarket",
                ))
            elif "trade" in event_type or "fill" in event_type or "match" in event_type:
                events.append(Fill(
                    market_id=market_id,
                    order_id=str(row.get("order_id") or row.get("orderId") or row.get("taker_order_id") or ""),
                    trade_id=event_id,
                    timestamp_ms=_timestamp_ms(row.get("match_time") or row.get("timestamp") or row.get("created_at")) or int(time.time() * 1000),
                    count_units=_size_units(row.get("size") or row.get("matched_size") or 0),
                    yes_price_units=_price_units(row.get("price")) if side == "yes" and row.get("price") is not None else None,
                    no_price_units=_price_units(row.get("price")) if side == "no" and row.get("price") is not None else None,
                    fee_units=_money_units(row.get("fee") or 0),
                    is_taker=str(row.get("taker_side") or row.get("trader_side") or row.get("side") or "").upper() == "TAKER",
                    venue="polymarket",
                ))
        return events

    def _user_subscription(self) -> str:
        creds = self._active_api_creds
        return json.dumps({
            "type": "user",
            "auth": {
                "apiKey": getattr(creds, "api_key", None) or self.config.api_key,
                "secret": getattr(creds, "api_secret", None) or self.config.api_secret,
                "passphrase": getattr(creds, "api_passphrase", None) or self.config.api_passphrase,
            },
        })

    async def stream_user_events(self) -> AsyncIterator[MarketEvent]:
        """Stream authenticated order/trade events with REST reconciliation.

        A reconnect is treated as a consistency boundary: REST snapshots are
        replayed through the same normalizers before the socket subscription
        is attempted again.  Consumers therefore do not need a venue-specific
        reconnect path.
        """
        self._require_auth()
        while True:
            try:
                await self._user_websocket.subscribe([self._user_subscription()])
                async for raw in self._user_websocket:
                    for event in self._parse_user_ws_message(raw):
                        yield event
                raise ConnectionError("Polymarket user stream ended")
            except asyncio.CancelledError:
                raise
            except Exception:
                for order in self.list_account_orders(AccountOrderQuery()):
                    key = f"reconcile:{order.order_id}:{order.status}:{order.fill_count_units}"
                    if key in self._seen_user_events:
                        continue
                    self._seen_user_events.add(key)
                    yield OrderUpdate(order.market_id, order.side or "yes", order.order_id, order.client_order_id, order.status, order.fill_count_units, order.remaining_count_units, order.price_units, order.expiration_time_ms, venue="polymarket")
                for fill in self.list_account_fills(AccountFillQuery()):
                    key = f"reconcile:fill:{fill.fill_id}"
                    if key in self._seen_user_events:
                        continue
                    self._seen_user_events.add(key)
                    yield Fill(fill.market_id, fill.order_id, fill.trade_id, fill.created_at_ms or int(time.time() * 1000), fill.count_units, fill.price_units if fill.side == "yes" else None, fill.price_units if fill.side == "no" else None, fill.fee_units, venue="polymarket")
                await asyncio.sleep(1.0)

    async def stream_events(self, market_id: str, *, include_position_updates: bool = True) -> AsyncIterator[MarketEvent]:
        market = self._raw_market(market_id)
        self._market_stream_assets = tuple(
            token for token in (market.yes_token_id, market.no_token_id) if token
        )
        self.book_cache.expected_assets = len(self._market_stream_assets)
        await self.websocket_client.subscribe([json.dumps({
            "type": "market",
            "assets_ids": [market.yes_token_id, market.no_token_id],
            "initial_dump": True,
        })])
        self._market_stream_started = True
        async for raw in self.websocket_client:
            for event in self._parse_ws_message(raw, market_id):
                yield event

    async def stream_events_many(self, market_ids: Sequence[str], *, include_position_updates: bool = True) -> AsyncIterator[MarketEvent]:
        markets = [self._raw_market(str(item)) for item in dict.fromkeys(market_ids)]
        assets = [token for market in markets for token in (market.yes_token_id, market.no_token_id) if token]
        self._market_stream_assets = tuple(dict.fromkeys(assets))
        self.book_cache.expected_assets = len(self._market_stream_assets)
        await self.websocket_client.subscribe([json.dumps({
            "type": "market",
            "assets_ids": assets,
            "initial_dump": True,
        })])
        self._market_stream_started = True
        async for raw in self.websocket_client:
            for event in self._parse_ws_message(raw):
                yield event

    async def bootstrap_market_books(
        self,
        markets: Sequence[Market],
        *,
        timeout_seconds: float = 45.0,
    ) -> dict[str, BookTop]:
        """Subscribe to all market assets and collect initial book snapshots."""
        assets = [token for market in markets for token in (market.yes_token_id, market.no_token_id) if token]
        expected = set(assets)
        self._market_stream_assets = tuple(dict.fromkeys(assets))
        self.book_cache.expected_assets = len(expected)
        if not expected:
            self._book_metrics.update({
                "bookAssetsExpected": 0,
                "bookAssetsReady": 0,
                "bookAssetsMissing": 0,
                "bookBootstrapDurationMs": 0,
            })
            return {}
        started = time.perf_counter()
        await self.websocket_client.subscribe([
            json.dumps({"type": "market", "assets_ids": assets, "initial_dump": True})
        ])
        deadline = time.monotonic() + max(1.0, float(timeout_seconds))
        # ``async for`` waits forever when a connection succeeds but the
        # provider sends no snapshot. Pull one message at a time with the
        # remaining bootstrap deadline so readiness cannot wedge startup.
        stream = self.websocket_client.__aiter__()
        while time.monotonic() < deadline:
            remaining = max(0.01, deadline - time.monotonic())
            try:
                raw = await asyncio.wait_for(stream.__anext__(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            except StopAsyncIteration:
                break
            value: Any = raw
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except Exception:
                    value = None
            rows = value if isinstance(value, list) else [value]
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                asset = str(row.get("asset_id") or row.get("assetId") or "")
                if asset not in expected or not ("bids" in row or "asks" in row):
                    continue
                self.book_cache.update_book(asset, row, timestamp_ms=_timestamp_ms(row.get("timestamp")) or 0)
            ready = sum(1 for asset in expected if self.book_cache.get(asset) is not None)
            if ready >= len(expected) or time.monotonic() >= deadline:
                break
        ready = sum(1 for asset in expected if self.book_cache.get(asset) is not None)
        self.book_cache.missing_assets = max(0, len(expected) - ready)
        self._book_metrics.update({
            "bookAssetsExpected": len(expected),
            "bookAssetsReady": ready,
            "bookAssetsMissing": max(0, len(expected) - ready),
            "bookAssetsStale": self.book_cache.stale_assets,
            "bookBootstrapDurationMs": int((time.perf_counter() - started) * 1000),
            "websocketMessages": self.book_cache.websocket_messages,
        })
        self.book_cache.persist()
        if self._market_stream_task is None or self._market_stream_task.done():
            self._market_stream_task = asyncio.create_task(self._consume_market_stream())
            self._market_stream_started = True
        return self.book_cache.snapshot()

    async def start_market_book_stream(self, markets: Sequence[Market]) -> None:
        """Subscribe and consume book updates without blocking on snapshots.

        The mirror already has a persisted generation to screen while a new
        catalog is being fetched.  Waiting for every initial dump here would
        delay that stream until the catalog task completes, so the initial
        dump and subsequent deltas are consumed by the background task.
        """

        assets = [
            token for market in markets
            for token in (market.yes_token_id, market.no_token_id)
            if token
        ]
        expected = tuple(dict.fromkeys(assets))
        self._market_stream_assets = expected
        self.book_cache.expected_assets = len(expected)
        if not expected:
            return
        await self.websocket_client.subscribe([
            json.dumps({
                "type": "market",
                "assets_ids": list(expected),
                "initial_dump": True,
            })
        ])
        self._market_stream_started = True
        if self._market_stream_task is None or self._market_stream_task.done():
            self._market_stream_task = asyncio.create_task(self._consume_market_stream())

    async def _consume_market_stream(self) -> None:
        """Keep the market-channel cache current after bootstrap."""
        while True:
            try:
                async for raw in self.websocket_client:
                    value: Any = raw
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except Exception:
                            continue
                    rows = value if isinstance(value, list) else [value]
                    for row in rows:
                        if not isinstance(row, Mapping):
                            continue
                        asset = str(row.get("asset_id") or row.get("assetId") or "")
                        event_type = str(row.get("event_type") or row.get("eventType") or "")
                        if asset and ("bids" in row or "asks" in row):
                            self.book_cache.update_book(asset, row, timestamp_ms=_timestamp_ms(row.get("timestamp")) or 0)
                        elif event_type in {"price_change", "book_delta", "delta"}:
                            if asset:
                                self.book_cache.update_price_change(
                                    asset, row,
                                    timestamp_ms=_timestamp_ms(row.get("timestamp")) or 0,
                                )
                            for change in row.get("price_changes") or ():
                                if isinstance(change, Mapping):
                                    changed_asset = str(change.get("asset_id") or change.get("assetId") or "")
                                    if changed_asset:
                                        self.book_cache.update_price_change(
                                            changed_asset,
                                            change,
                                            timestamp_ms=_timestamp_ms(
                                                change.get("timestamp") or row.get("timestamp")
                                            ) or 0,
                                        )
                    self._book_metrics.update({
                        "bookAssetsReady": self.book_cache.ready_count(),
                        "bookAssetsStale": self.book_cache.stale_assets,
                        "bookAssetsMissing": max(
                            0,
                            self.book_cache.expected_assets - self.book_cache.ready_count(),
                        ),
                        "websocketMessages": self.book_cache.websocket_messages,
                    })
                raise ConnectionError("Polymarket market stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning(
                    "POLYMARKET_MARKET_STREAM_ERROR | error=%s",
                    _redact_proxy_text(exc, self.proxy_url),
                )
                assets = getattr(self, "_market_stream_assets", ())
                if not assets:
                    return
                await asyncio.sleep(1.0)
                try:
                    await self.websocket_client.subscribe([
                        json.dumps({"type": "market", "assets_ids": list(assets), "initial_dump": False})
                    ])
                    # Recover assets that became stale while the connection
                    # was interrupted without blocking normal scans.
                    await asyncio.to_thread(self.rehydrate_stale_books)
                except Exception as reconnect_exc:
                    LOGGER.warning(
                        "POLYMARKET_MARKET_STREAM_RECONNECT_ERROR | error=%s",
                        _redact_proxy_text(reconnect_exc, self.proxy_url),
                    )
                    await asyncio.sleep(2.0)

    async def update_market_subscriptions(self, *, add: Sequence[str], remove: Sequence[str]) -> None:
        current = set(self._market_stream_assets)
        for market_id in add:
            market = self._raw_market(str(market_id))
            current.update(token for token in (market.yes_token_id, market.no_token_id) if token)
        for market_id in remove:
            market = self._market_cache.get(str(market_id))
            if market is None:
                continue
            current.difference_update(token for token in (market.yes_token_id, market.no_token_id) if token)
        self._market_stream_assets = tuple(sorted(current))
        self.book_cache.expected_assets = len(self._market_stream_assets)
        if self._market_stream_started and self.websocket_client is not None and self._market_stream_assets:
            await self.websocket_client.send(json.dumps({
                "type": "market",
                "assets_ids": list(self._market_stream_assets),
                "initial_dump": False,
            }))

    async def close(self) -> None:
        if self._market_stream_task is not None:
            self._market_stream_task.cancel()
            await asyncio.gather(self._market_stream_task, return_exceptions=True)
            self._market_stream_task = None
        await super().close()
        await self._user_websocket.close()
        try:
            for transport in (self.gamma_http, self.data_http):
                closer = getattr(transport, "close", None)
                if callable(closer):
                    closer()
        finally:
            self._parallel_executor.shutdown(wait=False, cancel_futures=True)
