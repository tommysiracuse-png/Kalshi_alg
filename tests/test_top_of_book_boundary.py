import asyncio

from clients.models import (
    MarketQuote,
    Order,
    OrderBookSnapshot,
    OrderUpdate,
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
        self.resting = [
            Order("owned", "MKT", "yes", "mm:owned", "resting"),
            Order("other", "MKT", "yes", "manual", "resting"),
        ]

    def get_positions(self, market_id):
        return [Position(market_id, 250)]

    def get_resting_orders(self, market_id):
        return list(self.resting)

    def cancel_order(self, *, order_id):
        self.canceled.append(order_id)
        self.resting = [order for order in self.resting if order.order_id != order_id]
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


def test_shutdown_cancels_and_verifies_only_bot_owned_orders():
    bot, client = make_bot()
    result = asyncio.run(bot.request_shutdown(0))

    assert result["ordersVerifiedAbsent"] is True
    assert result["ordersCanceled"] == 1
    assert client.canceled == ["owned"]
    assert [order.order_id for order in client.resting] == ["other"]
    assert client.closed is True


def test_monitoring_snapshot_tracks_process_session_separately_from_starting_inventory():
    bot, _ = make_bot()
    bot.load_startup_position()
    bot.session_yes_quantity_units = 100
    bot.session_no_quantity_units = 100
    bot.session_yes_cost_units = 4_000 * 100
    bot.session_no_cost_units = 5_000 * 100
    bot.session_fee_units = 10
    bot.session_fill_count = 2
    bot.book_yes = {4_000: 100}
    bot.book_no = {5_000: 100}
    bot.book_ready = True
    bot.last_market_event_timestamp_ms = bot.started_at_ms

    status = bot.status_snapshot()
    monitoring = status["monitoring"]
    assert monitoring["portfolio"]["startingPositionUnits"] == 250
    assert monitoring["pnl"]["sessionPositionUnits"] == 0
    assert monitoring["pnl"]["realizedCents"] == 9.9
    assert monitoring["market"]["priceSource"] == "book_mid"


def test_projected_position_cap_clamps_risk_increasing_quotes():
    bot, _ = make_bot()
    bot.net_position_units = 600  # Long 6 with a hard limit of 10.

    assert bot.projected_position_capacity_units("yes") == 400
    assert bot.desired_remaining_units("yes", 1_000, 0) == 400
    # A NO order reduces the long position and may still use the per-order cap.
    assert bot.desired_remaining_units("no", 1_000, 0) == 500

    bot.net_position_units = 1_000
    assert bot.desired_remaining_units("yes", 1_000, 0) == 0


def test_projected_position_cap_never_rounds_past_limit():
    bot, _ = make_bot()
    bot.net_position_units = 950

    # Whole-contract entry cannot add one contract without projecting to 10.5.
    assert bot.projected_position_capacity_units("yes") == 0
    assert bot.desired_remaining_units("yes", 1_000, 0) == 0

    # Risk-reducing orders remain available even when already beyond the cap.
    bot.net_position_units = -1_100
    assert bot.desired_remaining_units("yes", 1_000, 0) == 500
    assert bot.desired_remaining_units("no", 1_000, 0) == 0


def test_client_order_id_prevents_cross_side_state_corruption():
    bot, _ = make_bot()

    bot.handle_user_order_update(
        OrderUpdate("MKT", "yes", "no-order", "mm:no:123", "resting", remaining_count_units=100)
    )

    assert bot.orders["yes"].order_id is None
    assert bot.orders["no"].order_id == "no-order"
    assert bot.known_strategy_order_sides["no-order"] == "no"
