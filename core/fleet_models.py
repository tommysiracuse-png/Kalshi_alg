"""Typed messages and hard limits shared by the sharded fleet runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Literal, Mapping, Optional, Tuple


DEFAULT_MAX_BOTS = 40
MAX_CONCURRENT_BOTS = 500
DEFAULT_SHARD_SIZE = 25
MAX_WORKERS = 20


QuoteSide = Literal["yes", "no"]


class IntentUrgency(IntEnum):
    """Lower values are dispatched first by the execution broker."""

    EMERGENCY_CANCEL = 0
    RISK_REDUCE = 1
    NORMAL_CANCEL_OR_AMEND = 2
    NORMAL = 3


@dataclass(frozen=True)
class QuoteIntent:
    ticker: str
    side: QuoteSide
    desired_price_units: Optional[int]
    desired_size_units: int
    strategy_generation: int
    urgency: IntentUrgency
    created_at_ms: int
    expires_at_ms: int
    fair_value_units: Optional[int] = None
    risk_justification: str = ""
    exposure_increasing: bool = True

    @property
    def is_cancel(self) -> bool:
        return self.desired_price_units is None or self.desired_size_units <= 0


@dataclass(frozen=True)
class ExecutionResult:
    ticker: str
    side: QuoteSide
    order_id: str
    status: str
    accepted_price_units: Optional[int] = None
    accepted_size_units: int = 0
    exchange_timestamp_ms: Optional[int] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class MarketHealth:
    book_available: bool
    book_age_ms: Optional[int]
    decision_age_ms: Optional[int]
    risk_mode: str
    risk_age_ms: Optional[int]
    resting_order_count: int = 0
    position_units: int = 0
    error: str = ""
    started_at_ms: int = 0
    fill_count: int = 0
    order_activity: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    pnl: Mapping[str, object] = field(default_factory=dict)
    markouts: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    last_quote_at_ms: int = 0
    last_order_create_at_ms: int = 0
    last_fill_at_ms: int = 0
    price_units: Optional[int] = None
    price_source: str = "unavailable"
    price_at_ms: Optional[int] = None
    # Why the worker's risk evaluator chose ``risk_mode`` (e.g.
    # ``elevated_price_move``); surfaced as ``bots[].watchdogReason``.
    risk_reason: str = ""


@dataclass(frozen=True)
class WorkerHeartbeat:
    worker_id: str
    assigned_tickers: Tuple[str, ...]
    market_health: Mapping[str, MarketHealth]
    memory_rss_bytes: int
    queue_depth: int
    event_lag_ms: int
    generated_at_ms: int
    venue: str = "kalshi"
    api_activity: Mapping[str, object] = field(default_factory=dict)
    # Startup fields are optional so heartbeats written by older workers and
    # test fixtures remain readable while a rolling deployment is in flight.
    pending_tickers: Tuple[str, ...] = ()
    startup_attempts: Mapping[str, int] = field(default_factory=dict)
    startup_progress_at_ms: int = 0
    startup_error: str = ""
    worker_error: str = ""


@dataclass(frozen=True)
class WorkerControlAck:
    worker_id: str
    request_id: str
    action: str
    ok: bool
    generated_at_ms: int
    error: str = ""


@dataclass(frozen=True)
class FleetCapacity:
    api_tier: str
    read_refill_rate: int
    write_refill_rate: int
    normal_quote_side_capacity: int
    requested_markets: int
    admitted_markets: int
    admitted_quote_sides: int
    cash_available_units: int
    cash_allocatable_units: int
    cash_committed_units: int
    reserved_cash_units: int
    freshness_seconds: float
    normal_queue_depth: int = 0
    estimated_backlog_seconds: float = 0.0
    gate_open: bool = False
    reduction_only: bool = True
    error: str = ""
    venue: str = "kalshi"
    capacity_market_limit: int = 0
    capacity_limited: bool = False
    omitted_markets: int = 0
    omitted_reason: str = ""
    global_max_bots: Optional[int] = None
    venue_max_bots: Optional[int] = None
    priority: Optional[int] = None
    global_slots_remaining: Optional[int] = None


@dataclass(frozen=True)
class ScreenerPick:
    market_id: str
    title: str
    yes_budget_cents: int
    no_budget_cents: int
    ranking: Mapping[str, object] = field(default_factory=dict)
    selection_reason: str = "screen"
    # Market class assigned by the screener's classifier and the per-class
    # BotSettings overrides resolved for it (``(field, value)`` pairs; tuples
    # only, so the pick stays picklable across the worker queue and hashable).
    market_class: str = "default"
    settings_overrides: Tuple[Tuple[str, Any], ...] = ()
    venue: str = "kalshi"

    @property
    def ticker(self) -> str:
        return self.market_id

    @property
    def market_key(self) -> tuple[str, str]:
        return (str(self.venue or "kalshi").lower(), self.market_id)

    @property
    def raw_row(self) -> Dict[str, str]:
        row = {str(key): "" if value is None else str(value) for key, value in self.ranking.items()}
        row.setdefault("Venue", self.venue)
        return row

    def runtime_key(self) -> Tuple[str, int, int, str, Tuple[Tuple[str, Any], ...]]:
        # Everything that changes the running actor's settings belongs here:
        # the screener diffs runtime keys to decide which retained markets
        # must restart, so overrides left out would never be reconciled.
        return (
            self.venue,
            self.yes_budget_cents,
            self.no_budget_cents,
            self.market_class,
            tuple(tuple(item) for item in self.settings_overrides),
        )


@dataclass(frozen=True)
class ScreenerUpdate:
    generation_id: int
    generated_at_ms: int
    reason: str
    picks: Tuple[ScreenerPick, ...]
    added: Tuple[str, ...]
    kept: Tuple[str, ...]
    changed: Tuple[str, ...]
    removed: Tuple[str, ...]
    inventory_carried: Tuple[str, ...] = ()
    inventory_unknown: Tuple[str, ...] = ()

    @property
    def pick_by_market_id(self) -> Dict[str, ScreenerPick]:
        return {pick.market_id: pick for pick in self.picks}

    @property
    def pick_by_market_key(self) -> Dict[tuple[str, str], ScreenerPick]:
        return {pick.market_key: pick for pick in self.picks}

    @property
    def venue(self) -> str:
        return str(self.picks[0].venue if self.picks else "kalshi").lower()


@dataclass(frozen=True)
class ScreenerEvent:
    ok: bool
    reason: str
    generated_at_ms: int
    update: Optional[ScreenerUpdate] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class BotManagerEvent:
    event_type: str
    market_id: str
    generated_at_ms: int
    detail: Mapping[str, object] = field(default_factory=dict)
    venue: str = "kalshi"
