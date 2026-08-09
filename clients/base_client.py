"""Abstract interface implemented by every trading venue adaptor."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, List

from .http_client import HTTPClient
from .models import (
    AmendOrderRequest,
    CreateOrderRequest,
    IncentiveProgram,
    Market,
    MarketEvent,
    MarketQuery,
    MarketQuote,
    Order,
    Position,
    QueuePosition,
    Series,
    SeriesFeeChange,
)
from .websocket_client import WebsocketClient


class BaseClient(ABC):
    venue_name = "unknown"
    environment_name = "unknown"
    dry_run = False
    rate_limit_backoff_seconds = 1.0

    def __init__(self, *, http_client: HTTPClient, websocket_client: WebsocketClient) -> None:
        self.http_client = http_client
        self.websocket_client = websocket_client

    @abstractmethod
    def get_market(self, market_id: str) -> Market: ...

    @abstractmethod
    def list_markets(self, query: MarketQuery) -> List[Market]: ...

    @abstractmethod
    def get_market_quote(self, market_id: str) -> MarketQuote: ...

    @abstractmethod
    def get_positions(self, market_id: str) -> List[Position]: ...

    @abstractmethod
    def get_resting_orders(self, market_id: str) -> List[Order]: ...

    @abstractmethod
    def get_order_queue_position(self, order_id: str) -> QueuePosition: ...

    @abstractmethod
    def get_series(self, series_id: str) -> Series: ...

    @abstractmethod
    def get_series_fee_changes(self, series_id: str, *, show_historical: bool = False) -> List[SeriesFeeChange]: ...

    @abstractmethod
    def get_incentive_programs(
        self, *, status: str = "active", incentive_type: str = "all", limit: int = 10_000
    ) -> List[IncentiveProgram]: ...

    @abstractmethod
    def create_order(self, request: CreateOrderRequest) -> Order: ...

    @abstractmethod
    def amend_order(self, request: AmendOrderRequest) -> Order: ...

    @abstractmethod
    def decrease_order_to(self, *, order_id: str, remaining_count_units: int) -> Order: ...

    @abstractmethod
    def cancel_order(self, *, order_id: str) -> Order: ...

    @abstractmethod
    def stream_events(self, market_id: str, *, include_position_updates: bool = True) -> AsyncIterator[MarketEvent]: ...

    async def close(self) -> None:
        await self.websocket_client.close()

    def activity_snapshot(self) -> Dict[str, object]:
        http = self.http_client.activity_snapshot() if hasattr(self.http_client, "activity_snapshot") else {"startedAtMs": 0, "rest": {}}
        websocket = self.websocket_client.activity_snapshot() if hasattr(self.websocket_client, "activity_snapshot") else {"startedAtMs": 0, "stream": {}}
        starts = [int(value) for value in (http.get("startedAtMs"), websocket.get("startedAtMs")) if value]
        return {
            "startedAtMs": min(starts) if starts else 0,
            "rest": http.get("rest", {}),
            "stream": websocket.get("stream", {}),
        }
