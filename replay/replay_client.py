"""Duck-typed venue client backed by the paper-fill simulator.

Implements the surface the production MarketActor actually touches (see the
boundary test FakeClient and the ``self.api_client.*`` call sites in
top_of_book_bot.py). Order calls forward to the FillSimulator, which is the
single source of truth for resting-order state.
"""

from __future__ import annotations

from typing import List, Optional

from clients.models import (
    AmendOrderRequest,
    ClientError,
    CreateOrderRequest,
    MarketQuote,
    Order,
    Position,
)

from replay.fill_sim import PRICE_SCALE, FillSimulator


class ReplayClient:
    venue_name = "replay"
    environment_name = "backtest"
    dry_run = False
    rate_limit_backoff_seconds = 0

    def __init__(self, simulator: FillSimulator) -> None:
        self.simulator = simulator
        self.closed = False

    # -- account state -------------------------------------------------
    def get_positions(self, market_id: str) -> List[Position]:
        return []  # replay sessions always start flat

    def get_resting_orders(self, market_id: str) -> List[Order]:
        if market_id != self.simulator.market_id:
            return []
        return self.simulator.resting_orders()

    def get_market_quote(self, market_id: str) -> MarketQuote:
        yes_bid = self.simulator.best_yes_bid_units
        yes_ask = self.simulator.best_yes_ask_units
        no_bid = PRICE_SCALE - yes_ask if yes_ask is not None else None
        return MarketQuote(market_id, yes_bid, no_bid)

    # -- order entry ---------------------------------------------------
    def create_order(self, request: CreateOrderRequest) -> Order:
        return self.simulator.create_order(request)

    def amend_order(self, request: AmendOrderRequest) -> Order:
        return self.simulator.amend_order(request)

    def cancel_order(self, *, order_id: str) -> Order:
        return self.simulator.cancel_order(order_id=order_id)

    def decrease_order_to(self, *, order_id: str, new_total_fillable_count_units: int) -> Order:
        return self.simulator.decrease_order_to(
            order_id=order_id,
            new_total_fillable_count_units=new_total_fillable_count_units,
        )

    # -- unused-but-present surface ------------------------------------
    def get_series(self, series_id: str):
        raise ClientError("replay client has no series endpoint")

    def get_series_fee_changes(self, series_id: str, *, show_historical: bool = False):
        raise ClientError("replay client has no fee-change endpoint")

    def get_incentive_programs(self, **kwargs):
        raise ClientError("replay client has no incentive endpoint")

    def get_order_queue_position(self, order_id: str):
        raise ClientError("replay client has no queue-position endpoint")

    def activity_snapshot(self) -> dict:
        return {"rest": {}, "stream": {}}

    async def stream_events(self, market_id: str, *, include_position_updates: bool = True):
        raise ClientError("replay client does not stream; events are injected")
        yield  # pragma: no cover

    async def close(self) -> None:
        self.closed = True
