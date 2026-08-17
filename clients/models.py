"""Typed, venue-neutral data exchanged between trading bots and adaptors."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Literal, Mapping, Optional, Tuple, Union


Side = Literal["yes", "no"]
OrderAction = Literal["buy", "sell"]


class ClientError(RuntimeError):
    """Base error raised by a venue adaptor."""


class RateLimitError(ClientError):
    """The venue rejected a request because its rate limit was exceeded."""


class OrderNotFoundError(ClientError):
    """An order no longer exists or is no longer addressable."""


class PostOnlyCrossError(ClientError):
    """A post-only order would execute immediately."""


class AmendTargetUnavailableError(ClientError):
    """An order can no longer be amended because it is not resting."""


@dataclass(frozen=True)
class PriceRangeData:
    start_units: int
    end_units: int
    step_units: int


@dataclass(frozen=True)
class MarketQuery:
    status: str = "open"
    page_size: int = 1_000
    max_results: int = 10_000
    venue_filters: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AccountPositionQuery:
    nonzero_only: bool = True
    page_size: int = 1_000


@dataclass(frozen=True)
class AccountOrderQuery:
    status: str = ""
    market_id: str = ""
    min_created_at_ms: Optional[int] = None
    max_created_at_ms: Optional[int] = None
    page_size: int = 1_000


@dataclass(frozen=True)
class AccountFillQuery:
    market_id: str = ""
    order_id: str = ""
    min_created_at_ms: Optional[int] = None
    max_created_at_ms: Optional[int] = None
    page_size: int = 1_000


@dataclass(frozen=True)
class Market:
    market_id: str
    title: str = ""
    status: str = ""
    series_id: str = ""
    event_id: str = ""
    close_time_ms: Optional[int] = None
    price_level_structure: str = ""
    fractional_trading_enabled: bool = False
    price_ranges: Tuple[PriceRangeData, ...] = ()
    legacy_tick_size_units: int = 100
    yes_bid_units: Optional[int] = None
    yes_ask_units: Optional[int] = None
    no_bid_units: Optional[int] = None
    no_ask_units: Optional[int] = None
    last_price_units: Optional[int] = None
    yes_bid_size_units: Optional[int] = None
    yes_ask_size_units: Optional[int] = None
    no_bid_size_units: Optional[int] = None
    no_ask_size_units: Optional[int] = None
    volume_24h_units: Optional[int] = None
    open_interest_units: Optional[int] = None
    expected_expiration_time_ms: Optional[int] = None
    expiration_time_ms: Optional[int] = None
    market_url: Optional[str] = None


@dataclass(frozen=True)
class MarketQuote:
    market_id: str
    yes_bid_units: Optional[int]
    no_bid_units: Optional[int]


@dataclass(frozen=True)
class Position:
    market_id: str
    position_units: int


@dataclass(frozen=True)
class AccountBalance:
    available_cash_units: int
    portfolio_value_units: int
    updated_at_ms: Optional[int] = None


@dataclass(frozen=True)
class RateLimitBucket:
    refill_rate: int = 0
    bucket_capacity: int = 0


@dataclass(frozen=True)
class AccountLimits:
    usage_tier: str
    read: RateLimitBucket = field(default_factory=RateLimitBucket)
    write: RateLimitBucket = field(default_factory=RateLimitBucket)


@dataclass(frozen=True)
class AccountPosition:
    market_id: str
    position_units: int
    total_traded_units: Optional[int] = None
    market_exposure_units: Optional[int] = None
    realized_pnl_units: Optional[int] = None
    fees_paid_units: Optional[int] = None
    resting_order_count: int = 0
    updated_at_ms: Optional[int] = None


@dataclass(frozen=True)
class Order:
    order_id: str
    market_id: str = ""
    side: Optional[Side] = None
    client_order_id: str = ""
    status: str = ""
    price_units: Optional[int] = None
    fill_count_units: int = 0
    remaining_count_units: int = 0
    expiration_time_ms: Optional[int] = None


@dataclass(frozen=True)
class AccountOrder:
    order_id: str
    market_id: str
    side: Optional[Side] = None
    client_order_id: str = ""
    status: str = ""
    price_units: Optional[int] = None
    fill_count_units: int = 0
    remaining_count_units: int = 0
    initial_count_units: int = 0
    fill_cost_units: int = 0
    fees_units: int = 0
    created_at_ms: Optional[int] = None
    updated_at_ms: Optional[int] = None
    expiration_time_ms: Optional[int] = None


@dataclass(frozen=True)
class AccountFill:
    fill_id: str
    trade_id: str
    order_id: str
    market_id: str
    side: Optional[Side]
    count_units: int
    price_units: Optional[int]
    fee_units: int = 0
    created_at_ms: Optional[int] = None
    is_taker: bool = False


@dataclass(frozen=True)
class QueuePosition:
    order_id: str
    queue_position_units: Optional[int]


@dataclass(frozen=True)
class Series:
    series_id: str
    fee_type: str = ""
    fee_multiplier: float = 1.0
    title: str = ""


@dataclass(frozen=True)
class SeriesFeeChange:
    series_id: str
    fee_type: str = ""
    fee_multiplier: Optional[float] = None
    effective_time_ms: Optional[int] = None


@dataclass(frozen=True)
class IncentiveProgram:
    market_id: str
    incentive_type: str = ""
    discount_factor_bps: float = 0.0
    target_size_units: int = 0


@dataclass(frozen=True)
class CreateOrderRequest:
    market_id: str
    side: Side
    price_units: int
    count_units: int
    client_order_id: str
    expiration_timestamp_seconds: Optional[int]
    action: OrderAction = "buy"
    post_only: Optional[bool] = None
    reduce_only: Optional[bool] = None
    time_in_force: Optional[str] = None
    cancel_order_on_pause: Optional[bool] = None
    self_trade_prevention_type: Optional[str] = None


@dataclass(frozen=True)
class AmendOrderRequest:
    order_id: str
    market_id: str
    side: Side
    new_price_units: int
    new_total_fillable_count_units: int
    previous_client_order_id: str
    updated_client_order_id: str
    action: OrderAction = "buy"


@dataclass(frozen=True)
class StreamReset:
    market_id: str


@dataclass(frozen=True)
class OrderBookSnapshot:
    market_id: str
    sequence: Optional[int]
    yes_levels: Dict[int, int]
    no_levels: Dict[int, int]


@dataclass(frozen=True)
class OrderBookDelta:
    market_id: str
    sequence: Optional[int]
    side: Side
    price_units: int
    delta_count_units: int
    timestamp_ms: int


@dataclass(frozen=True)
class OrderUpdate:
    market_id: str
    side: Side
    order_id: str
    client_order_id: str
    status: str
    fill_count_units: Optional[int] = None
    remaining_count_units: Optional[int] = None
    price_units: Optional[int] = None
    expiration_time_ms: Optional[int] = None


@dataclass(frozen=True)
class Fill:
    market_id: str
    order_id: str
    trade_id: str
    timestamp_ms: int
    count_units: int
    yes_price_units: Optional[int] = None
    no_price_units: Optional[int] = None
    fee_units: Optional[int] = None
    post_position_units: Optional[int] = None
    is_taker: bool = False


@dataclass(frozen=True)
class PublicTrade:
    market_id: str
    trade_id: str
    timestamp_ms: int
    yes_price_units: int
    no_price_units: int
    count_units: int
    taker_side: str = ""


@dataclass(frozen=True)
class TickerUpdate:
    market_id: str
    timestamp_ms: int
    price_units: Optional[int] = None
    yes_bid_units: Optional[int] = None
    yes_ask_units: Optional[int] = None
    volume_units: Optional[int] = None
    open_interest_units: Optional[int] = None
    yes_bid_size_units: Optional[int] = None
    yes_ask_size_units: Optional[int] = None
    last_trade_size_units: Optional[int] = None


@dataclass(frozen=True)
class PositionUpdate:
    market_id: str
    position_units: int


MarketEvent = Union[
    StreamReset,
    OrderBookSnapshot,
    OrderBookDelta,
    OrderUpdate,
    Fill,
    PublicTrade,
    TickerUpdate,
    PositionUpdate,
]
