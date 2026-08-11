import asyncio
import json

import pytest

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from clients.http_client import HTTPClientError
from clients.models import (
    AccountFillQuery,
    AccountOrderQuery,
    CreateOrderRequest,
    OrderBookDelta,
    OrderBookSnapshot,
    OrderUpdate,
    RateLimitError,
    StreamReset,
    MarketQuery,
)


class FakePrivateKey:
    def sign(self, message, *args):
        self.message = message
        return b"signature"


class FakeHTTP:
    def __init__(self):
        self.calls = []
        self.responses = []

    def _next(self, method, path, kwargs):
        self.calls.append((method, path, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get(self, path, **kwargs):
        return self._next("GET", path, kwargs)

    def post(self, path, **kwargs):
        return self._next("POST", path, kwargs)

    def delete(self, path, **kwargs):
        return self._next("DELETE", path, kwargs)


class FakeWebsocket:
    def __init__(self, sessions=None):
        self.sessions = list(sessions or [])
        self.current = []
        self.subscriptions = []
        self.closed = False

    async def subscribe(self, messages, *, headers=None):
        self.subscriptions.append((list(messages), headers))
        self.current = self.sessions.pop(0) if self.sessions else []

    def __aiter__(self):
        async def messages():
            for message in self.current:
                yield message

        return messages()

    async def close(self):
        self.closed = True


def make_client(http=None, websocket=None, **overrides):
    values = {
        "api_key_id": "key-id",
        "private_key_path": "unused",
        "enable_shared_write_rate_limiter": False,
    }
    values.update(overrides)
    return KalshiApiClient(
        KalshiClientConfig(**values),
        http_client=http or FakeHTTP(),
        websocket_client=websocket or FakeWebsocket(),
        private_key=FakePrivateKey(),
    )


def test_market_position_and_order_payloads_are_normalized():
    http = FakeHTTP()
    http.responses = [
        {
            "market": {
                "ticker": "MKT",
                "series_ticker": "SERIES",
                "yes_bid_dollars": "0.4321",
                "price_ranges": [{"start": "0.00", "end": "1.00", "step": "0.01"}],
            }
        },
        {"market_positions": [{"ticker": "MKT", "position_fp": "2.50"}]},
        {
            "order": {
                "order_id": "order-1",
                "client_order_id": "client-1",
                "fill_count": "0.00",
                "remaining_count": "1.50",
                "no_price": "25.00",
                "expiration_time": "2026-01-02T03:04:05Z",
            }
        },
    ]
    client = make_client(http=http)

    market = client.get_market("MKT")
    positions = client.get_positions("MKT")
    order = client.create_order(CreateOrderRequest("MKT", "no", 2500, 150, "client-1", None))

    assert market.market_id == "MKT"
    assert market.series_id == "SERIES"
    assert market.price_ranges[0].step_units == 100
    assert positions[0].position_units == 250
    assert order.order_id == "order-1"
    assert order.fill_count_units == 0
    assert order.remaining_count_units == 150
    assert order.price_units == 2_500
    body = http.calls[2][2]["body"]
    assert body["side"] == "ask"
    assert body["price"] == "0.7500"
    assert body["count"] == "1.50"


def test_http_rate_limit_is_mapped_to_typed_error():
    http = FakeHTTP()
    http.responses = [HTTPClientError(method="GET", path="/x", status_code=429, response_text="too_many_requests")]
    client = make_client(http=http)
    with pytest.raises(RateLimitError):
        client.get_market("MKT")


def test_all_market_stream_messages_are_normalized():
    client = make_client()
    cases = [
        ({"type": "orderbook_snapshot", "seq": 1, "msg": {"yes_dollars_fp": [["0.40", "2.50"]], "no_dollars_fp": [["0.55", "3.00"]]}}, OrderBookSnapshot),
        ({"type": "orderbook_delta", "seq": 2, "msg": {"side": "yes", "price_dollars": "0.40", "delta_fp": "-1.25", "ts": 10}}, OrderBookDelta),
        ({"type": "user_order", "msg": {"ticker": "MKT", "side": "yes", "order_id": "o", "client_order_id": "mm:x", "status": "resting"}}, object),
        ({"type": "fill", "msg": {"ticker": "MKT", "order_id": "o", "trade_id": "t", "count_fp": "1.00", "yes_price_dollars": "0.40"}}, object),
        ({"type": "trade", "msg": {"ticker": "MKT", "trade_id": "t", "count_fp": "1.00", "yes_price_dollars": "0.40"}}, object),
        ({"type": "ticker", "msg": {"ticker": "MKT", "yes_bid_dollars": "0.40"}}, object),
        ({"type": "market_position", "msg": {"ticker": "MKT", "position_fp": "1.25"}}, object),
    ]
    for payload, expected_type in cases:
        event = client._event(payload, "MKT")
        assert event is not None
        if expected_type is not object:
            assert isinstance(event, expected_type)


def test_user_order_side_normalizes_legacy_sell_yes_to_no_outcome():
    client = make_client()

    event = client._event(
        {
            "type": "user_order",
            "msg": {
                "ticker": "MKT",
                "side": "yes",
                "action": "sell",
                "order_id": "no-order",
                "client_order_id": "mm:no:123",
                "status": "resting",
            },
        },
        "MKT",
    )

    assert isinstance(event, OrderUpdate)
    assert event.side == "no"


def test_sequence_gap_reconnects_and_resubscribes():
    first = [
        json.dumps({"type": "orderbook_snapshot", "seq": 5, "msg": {"yes": [], "no": []}}),
        json.dumps({"type": "orderbook_delta", "seq": 7, "msg": {"side": "yes", "price": 40, "delta": 1}}),
    ]
    websocket = FakeWebsocket([first, []])
    client = make_client(websocket=websocket)

    async def scenario():
        stream = client.stream_events("MKT", include_position_updates=False)
        assert isinstance(await anext(stream), StreamReset)
        assert isinstance(await anext(stream), OrderBookSnapshot)
        assert isinstance(await anext(stream), StreamReset)
        await client.close()
        await stream.aclose()

    asyncio.run(scenario())
    assert len(websocket.subscriptions) == 2
    first_payloads = [json.loads(value) for value in websocket.subscriptions[0][0]]
    assert all("market_positions" not in item["params"]["channels"] for item in first_payloads)


def test_list_markets_paginates_and_normalizes_screening_fields():
    http = FakeHTTP()
    http.responses = [
        {
            "markets": [{
                "ticker": "ONE",
                "yes_bid_dollars": "0.4100",
                "yes_bid_size_fp": "2.50",
                "volume_24h_fp": "100.00",
                "open_interest_fp": "40.00",
                "expected_expiration_time": "2026-08-10T12:00:00Z",
            }],
            "cursor": "next",
        },
        {"markets": [{"ticker": "TWO", "last_price": 52}]},
    ]
    client = make_client(http=http)
    markets = client.list_markets(MarketQuery(status="open", page_size=50, max_results=10))
    assert [market.market_id for market in markets] == ["ONE", "TWO"]
    assert markets[0].yes_bid_units == 4100
    assert markets[0].yes_bid_size_units == 250
    assert markets[0].volume_24h_units == 10_000
    assert markets[0].open_interest_units == 4_000
    assert markets[0].expected_expiration_time_ms is not None
    assert http.calls[1][2]["params"]["cursor"] == "next"


def test_public_only_client_can_discover_markets_without_credentials():
    http = FakeHTTP()
    http.responses = [{"markets": [{"ticker": "PUBLIC"}]}]
    client = KalshiApiClient(
        KalshiClientConfig(public_only=True),
        http_client=http,
        websocket_client=FakeWebsocket(),
    )
    markets = client.list_markets(MarketQuery(max_results=1))
    assert markets[0].market_id == "PUBLIC"
    assert "KALSHI-ACCESS-KEY" not in http.calls[0][2]["headers"]


def test_account_endpoints_paginate_and_normalize_fixed_point_fields():
    http = FakeHTTP()
    http.responses = [
        {"balance_dollars": "12.3400", "portfolio_value": 456, "updated_ts": 10},
        {"usage_tier": "expert", "read": {"refill_rate": 30, "bucket_capacity": 60}, "write": {"refill_rate": 10, "bucket_capacity": 20}},
        {"market_positions": [{"ticker": "MKT", "position_fp": "-2.50", "total_traded_dollars": "4.0000", "market_exposure_dollars": "1.5000", "realized_pnl_dollars": "0.2500", "fees_paid_dollars": "0.0100", "resting_orders_count": 2}], "cursor": "p2"},
        {"market_positions": [], "cursor": ""},
        {"orders": [{"order_id": "o1", "ticker": "MKT", "outcome_side": "no", "no_price_dollars": "0.6000", "fill_count_fp": "1.00", "remaining_count_fp": "2.50", "initial_count_fp": "3.50", "maker_fill_cost_dollars": "0.6000", "created_time": "2026-08-09T12:00:00Z"}], "cursor": ""},
        {"fills": [{"fill_id": "f1", "order_id": "o1", "ticker": "MKT", "book_side": "ask", "count_fp": "1.00", "no_price_dollars": "0.6000", "fee_cost": "0.0100", "ts": 20}], "cursor": ""},
    ]
    client = make_client(http=http, subaccount_number=3)
    balance = client.get_account_balance()
    limits = client.get_account_limits()
    positions = client.list_account_positions()
    orders = client.list_account_orders(AccountOrderQuery(status="resting", min_created_at_ms=1_000))
    fills = client.list_account_fills(AccountFillQuery(min_created_at_ms=2_000))

    assert balance.available_cash_units == 123_400
    assert balance.portfolio_value_units == 45_600
    assert limits.usage_tier == "expert"
    assert positions[0].position_units == -250
    assert positions[0].market_exposure_units == 15_000
    assert orders[0].side == "no"
    assert orders[0].price_units == 6_000
    assert orders[0].remaining_count_units == 250
    assert fills[0].side == "no"
    assert fills[0].fee_units == 100
    assert all(call[2].get("params", {}).get("subaccount") == 3 for call in http.calls if "/portfolio/" in call[1])
    assert http.calls[4][2]["params"]["min_ts"] == 1
    assert http.calls[5][2]["params"]["min_ts"] == 2


def test_account_direction_accepts_legacy_action_and_side():
    order = KalshiApiClient._account_order({
        "order_id": "legacy", "action": "sell", "side": "yes", "yes_price_dollars": "0.3000"
    })
    assert order.side == "no"
