import asyncio

from clients.models import (
    MarketQuote,
    Order,
    OrderBookSnapshot,
    Position,
    PositionUpdate,
    StreamReset,
)
from top_of_book_bot import BotSettings, MarketMetadata, PriceGrid, PriceRange, TopOfBookBot


class FakeClient:
    venue_name = "fake"
    environment_name = "test"
    rate_limit_backoff_seconds = 0

    def __init__(self):
        self.canceled = []
        self.closed = False

    def get_positions(self, market_id):
        return [Position(market_id, 250)]

    def get_resting_orders(self, market_id):
        return [Order("owned", market_id, "yes", "mm:owned", "resting"), Order("other", market_id, "yes", "manual", "resting")]

    def cancel_order(self, *, order_id):
        self.canceled.append(order_id)
        return Order(order_id)

    def get_market_quote(self, market_id):
        return MarketQuote(market_id, 4000, 5000)

    async def stream_events(self, market_id, *, include_position_updates=True):
        yield StreamReset(market_id)
        yield OrderBookSnapshot(market_id, 1, {4000: 1000}, {5000: 1200})
        yield PositionUpdate(market_id, 125)

    async def close(self):
        self.closed = True


def make_bot():
    settings = BotSettings(
        market_ticker="MKT",
        enable_sqlite_telemetry=False,
        enable_queue_position_logging=False,
    )
    market = MarketMetadata(
        ticker="MKT",
        title="Market",
        status="open",
        series_ticker="SERIES",
        event_ticker="EVENT",
        close_time_ms=None,
        price_level_structure="linear_cent",
        fractional_trading_enabled=True,
        price_grid=PriceGrid([PriceRange(0, 10_000, 100)]),
    )
    client = FakeClient()
    return TopOfBookBot(settings, client, market), client


def test_startup_state_and_cleanup_use_typed_base_client():
    bot, client = make_bot()
    bot.load_startup_position()
    bot.cancel_owned_resting_quotes_on_startup()
    assert bot.net_position_units == 250
    assert client.canceled == ["owned"]


def test_market_event_loop_has_no_websocket_dependency():
    bot, client = make_bot()
    asyncio.run(bot.market_event_main())
    assert bot.book_ready
    assert bot.book_yes == {4000: 1000}
    assert bot.book_no == {5000: 1200}
    assert bot.net_position_units == 125
    asyncio.run(bot.request_shutdown(0))
    assert client.closed
