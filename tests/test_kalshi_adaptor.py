import asyncio
import json
import logging

import pytest

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from clients.http_client import HTTPClientError
from clients.models import (
    AccountFillQuery,
    AccountOrderQuery,
    AmendOrderRequest,
    AmendTargetUnavailableError,
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
        self.sent = []

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

    async def send(self, message, **_kwargs):
        self.sent.append(message)


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
    assert market.market_url is None
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


def test_reduce_only_create_is_normalized_to_ioc_and_not_post_only():
    http = FakeHTTP()
    http.responses = [{
        "order": {
            "order_id": "reduce-only-1",
            "client_order_id": "wd:yes:1",
            "fill_count": "0.00",
            "remaining_count": "0.00",
            "yes_price": "0.40",
        }
    }]
    client = make_client(http=http)

    client.create_order(CreateOrderRequest(
        "MKT", "yes", 4_000, 100, "wd:yes:1", 1_800_000_000,
        reduce_only=True, post_only=True,
    ))

    body = http.calls[0][2]["body"]
    assert body["reduce_only"] is True
    assert body["time_in_force"] == "immediate_or_cancel"
    assert body["post_only"] is False
    assert "expiration_time" not in body


def test_market_derives_series_from_official_event_ticker_without_guessing_a_url():
    market = KalshiApiClient._market({
        "ticker": "KXDEEPSHARE-DEEP-26",
        "event_ticker": "KXDEEPSHARE-DEEP",
        "title": "DeepSeek market share this week?",
        "series_title": "DeepSeek market share",
    }, "")
    assert market.series_id == "KXDEEPSHARE"
    assert market.event_id == "KXDEEPSHARE-DEEP"
    assert market.series_title == "DeepSeek market share"
    assert market.market_url is None


def test_series_title_is_retained_for_canonical_website_links():
    http = FakeHTTP()
    http.responses = [{"series": {
        "ticker": "KXDEEPSHARE", "title": "DeepSeek market share",
        "fee_type": "quadratic", "fee_multiplier": 1,
    }}]
    series = make_client(http=http).get_series("KXDEEPSHARE")
    assert series.title == "DeepSeek market share"


def test_http_rate_limit_is_mapped_to_typed_error():
    http = FakeHTTP()
    http.responses = [HTTPClientError(method="GET", path="/x", status_code=429, response_text="too_many_requests")]
    client = make_client(http=http)
    with pytest.raises(RateLimitError):
        client.get_market("MKT")


def test_amend_side_mismatch_is_recovered_as_unavailable_target():
    http = FakeHTTP()
    http.responses = [HTTPClientError(
        method="POST",
        path="/portfolio/events/orders/o1/amend",
        status_code=400,
        response_text='{"error":{"code":"order_side_mismatch","message":"order side mismatch"}}',
    )]
    client = make_client(http=http)

    with pytest.raises(AmendTargetUnavailableError):
        client.amend_order(AmendOrderRequest("o1", "MKT", "yes", 4_100, 100, "c1", "c2"))


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


def test_multi_market_stream_uses_subscription_sequence_for_interleaved_events():
    messages = [
        json.dumps({"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 91}}),
        json.dumps({"type": "orderbook_snapshot", "sid": 91, "seq": 5, "msg": {"market_ticker": "A", "yes": [], "no": []}}),
        json.dumps({"type": "orderbook_snapshot", "sid": 91, "seq": 6, "msg": {"market_ticker": "B", "yes": [], "no": []}}),
        json.dumps({"type": "orderbook_delta", "sid": 91, "seq": 7, "msg": {"market_ticker": "A", "side": "yes", "price": 40, "delta": 1}}),
        json.dumps({"type": "orderbook_delta", "sid": 91, "seq": 8, "msg": {"market_ticker": "B", "side": "no", "price": 50, "delta": 1}}),
    ]
    websocket = FakeWebsocket([messages])
    client = make_client(websocket=websocket)

    async def scenario():
        stream = client.stream_events_many(("A", "B"), include_position_updates=False)
        events = [await anext(stream) for _ in range(6)]
        await client.update_market_subscriptions(add=("C",), remove=("B",))
        await client.close()
        await stream.aclose()
        return events

    events = asyncio.run(scenario())
    assert [event.market_id for event in events[:2]] == ["A", "B"]
    assert isinstance(events[2], OrderBookSnapshot) and events[2].market_id == "A"
    assert isinstance(events[3], OrderBookSnapshot) and events[3].market_id == "B"
    assert isinstance(events[4], OrderBookDelta) and events[4].market_id == "A"
    assert isinstance(events[5], OrderBookDelta) and events[5].market_id == "B"
    commands = [json.loads(item) for item in websocket.sent]
    assert not any(item["params"]["action"] == "get_snapshot" for item in commands)
    assert any(item["params"]["action"] == "delete_markets" for item in commands)
    assert any(item["params"]["action"] == "add_markets" for item in commands)


def test_multi_market_stream_recovers_all_books_after_real_subscription_gap():
    messages = [
        json.dumps({"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 91}}),
        json.dumps({"type": "orderbook_snapshot", "sid": 91, "seq": 5, "msg": {"market_ticker": "A", "yes": [], "no": []}}),
        json.dumps({"type": "orderbook_snapshot", "sid": 91, "seq": 6, "msg": {"market_ticker": "B", "yes": [], "no": []}}),
        json.dumps({"type": "orderbook_delta", "sid": 91, "seq": 8, "msg": {"market_ticker": "A", "side": "yes", "price": 40, "delta": 1}}),
    ]
    websocket = FakeWebsocket([messages])
    client = make_client(websocket=websocket)

    async def scenario():
        stream = client.stream_events_many(("A", "B"), include_position_updates=False)
        events = [await anext(stream) for _ in range(6)]
        await client.close()
        await stream.aclose()
        return events

    events = asyncio.run(scenario())
    assert [event.market_id for event in events[-2:]] == ["A", "B"]
    assert all(isinstance(event, StreamReset) for event in events[-2:])
    commands = [json.loads(item) for item in websocket.sent]
    snapshots = [item for item in commands if item["params"]["action"] == "get_snapshot"]
    assert snapshots[-1]["params"]["market_tickers"] == ["A", "B"]


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


def test_account_order_and_fill_pagination_respects_hard_result_caps():
    http = FakeHTTP()
    http.responses = [
        {
            "orders": [
                {"order_id": "o1", "ticker": "MKT"},
                {"order_id": "o2", "ticker": "MKT"},
            ],
            "cursor": "orders-next",
        },
        {
            "orders": [
                {"order_id": "o3", "ticker": "MKT"},
                {"order_id": "o4", "ticker": "MKT"},
            ],
            "cursor": "orders-never-read",
        },
        {
            "fills": [
                {"fill_id": "f1", "ticker": "MKT"},
                {"fill_id": "f2", "ticker": "MKT"},
            ],
            "cursor": "fills-next",
        },
        {
            "fills": [
                {"fill_id": "f3", "ticker": "MKT"},
                {"fill_id": "f4", "ticker": "MKT"},
            ],
            "cursor": "fills-never-read",
        },
    ]
    client = make_client(http=http)

    orders = client.list_account_orders(AccountOrderQuery(page_size=2, max_results=3))
    fills = client.list_account_fills(AccountFillQuery(page_size=2, max_results=3))

    assert [item.order_id for item in orders] == ["o1", "o2", "o3"]
    assert [item.fill_id for item in fills] == ["f1", "f2", "f3"]
    assert http.calls[1][2]["params"]["limit"] == 1
    assert http.calls[1][2]["params"]["cursor"] == "orders-next"
    assert http.calls[3][2]["params"]["limit"] == 1
    assert http.calls[3][2]["params"]["cursor"] == "fills-next"


def test_account_direction_accepts_legacy_action_and_side():
    order = KalshiApiClient._account_order({
        "order_id": "legacy", "action": "sell", "side": "yes", "yes_price_dollars": "0.3000"
    })
    assert order.side == "no"


# --- exchange sharding -------------------------------------------------------


def _create(market_id, client_order_id="c1"):
    return CreateOrderRequest(market_id, "yes", 4_000, 100, client_order_id, None)


def test_market_parses_exchange_index_and_defaults_to_zero():
    assert KalshiApiClient._market({"ticker": "KXATP-X", "exchange_index": 3}, "").exchange_index == 3
    assert KalshiApiClient._market({"ticker": "KXATP-X", "exchange_index": "2"}, "").exchange_index == 2
    assert KalshiApiClient._market({"ticker": "KXATP-X"}, "").exchange_index == 0
    assert KalshiApiClient._market({"ticker": "KXATP-X", "exchange_index": None}, "").exchange_index == 0


def test_balance_breakdown_is_parsed_per_exchange_in_price_units():
    http = FakeHTTP()
    http.responses = [{
        "balance": 71803,
        "balance_dollars": "718.0341",
        "portfolio_value_dollars": "900.0000",
        "updated_ts": 10,
        "balance_breakdown": [
            {"exchange_index": 3, "balance": "0.0000"},
            {"exchange_index": 0, "balance": "718.0341"},
            {"exchange_index": 2, "balance": "0.0000"},
            {"exchange_index": 1, "balance": "0.0000"},
        ],
    }]
    balance = make_client(http=http).get_account_balance()
    assert balance.available_cash_units == 7_180_341
    assert balance.portfolio_value_units == 9_000_000
    assert balance.balance_by_exchange == ((0, 7_180_341), (1, 0), (2, 0), (3, 0))


def test_balance_without_breakdown_leaves_per_exchange_tuple_empty():
    http = FakeHTTP()
    http.responses = [{"balance_dollars": "12.3400", "portfolio_value_dollars": "12.3400"}]
    balance = make_client(http=http).get_account_balance()
    assert balance.available_cash_units == 123_400
    assert balance.balance_by_exchange == ()


def test_create_order_targets_cached_market_shard_else_requires_auto_routing():
    http = FakeHTTP()
    http.responses = [
        {"market": {"ticker": "TENNIS", "exchange_index": 3}},
        {"order": {"order_id": "o-tennis"}},
        {"order": {"order_id": "o-unknown"}},
        {"market": {"ticker": "LEGACY"}},
        {"order": {"order_id": "o-legacy"}},
    ]
    client = make_client(http=http)

    market = client.get_market("TENNIS")
    assert market.exchange_index == 3
    assert client.exchange_index_for_market("TENNIS") == 3
    assert client.exchange_index_for_market("UNKNOWN") is None

    client.create_order(_create("TENNIS"))
    # create-order-v2: exchange_index is a JSON body integer.
    assert http.calls[1][2]["body"]["exchange_index"] == 3
    assert "exchange_index" not in (http.calls[1][2].get("params") or {})

    client.create_order(_create("UNKNOWN"))
    assert http.calls[2][2]["body"]["exchange_index"] == -1

    # A market payload that omits exchange_index normalizes to shard 0 on the
    # model but is NOT trusted for direct routing: the write auto-routes.
    legacy = client.get_market("LEGACY")
    assert legacy.exchange_index == 0
    assert client.exchange_index_for_market("LEGACY") is None
    client.create_order(_create("LEGACY"))
    assert http.calls[4][2]["body"]["exchange_index"] == -1


def test_list_markets_and_quotes_populate_shard_cache():
    http = FakeHTTP()
    http.responses = [
        {"markets": [{"ticker": "MLB-1", "exchange_index": 3}, {"ticker": "BTC-1", "exchange_index": 2}]},
        {"market": {"ticker": "QUOTED", "exchange_index": 1, "yes_bid_dollars": "0.4000"}},
    ]
    client = make_client(http=http)
    markets = client.list_markets(MarketQuery(max_results=2))
    assert [m.exchange_index for m in markets] == [3, 2]
    client.get_market_quote("QUOTED")
    assert client.exchange_index_for_market("MLB-1") == 3
    assert client.exchange_index_for_market("BTC-1") == 2
    assert client.exchange_index_for_market("QUOTED") == 1


def test_cancel_decrease_and_amend_carry_remembered_shard_and_ticker():
    http = FakeHTTP()
    http.responses = [
        {"market": {"ticker": "TENNIS", "exchange_index": 3}},
        {"order": {"order_id": "o1"}},
        {"order_id": "o1", "reduced_by": "1.00"},
        {"order_id": "o1", "remaining_count": "0.50"},
        {"order": {"order_id": "o1"}},
    ]
    client = make_client(http=http, subaccount_number=2)
    client.get_market("TENNIS")
    client.create_order(_create("TENNIS"))

    client.cancel_order(order_id="o1")
    # cancel-order-v2: exchange_index and market_ticker are QUERY parameters.
    method, path, kwargs = http.calls[2]
    assert method == "DELETE" and path.endswith("/portfolio/events/orders/o1")
    assert kwargs["params"] == {"subaccount": 2, "exchange_index": 3, "market_ticker": "TENNIS"}

    client.decrease_order_to(order_id="o1", remaining_count_units=50)
    # decrease-order-v2: exchange_index and market_ticker are JSON BODY fields.
    method, path, kwargs = http.calls[3]
    assert method == "POST" and path.endswith("/portfolio/events/orders/o1/decrease")
    assert kwargs["body"] == {"reduce_to": "0.50", "exchange_index": 3, "market_ticker": "TENNIS"}
    assert kwargs["params"] == {"subaccount": 2}

    client.amend_order(AmendOrderRequest("o1", "TENNIS", "yes", 4_100, 100, "c1", "c2"))
    # amend-order-v2: exchange_index is a JSON body integer alongside ticker.
    method, path, kwargs = http.calls[4]
    assert path.endswith("/portfolio/events/orders/o1/amend")
    assert kwargs["body"]["exchange_index"] == 3
    assert kwargs["body"]["ticker"] == "TENNIS"


def test_cancel_of_auto_routed_order_falls_back_to_ticker_auto_routing():
    http = FakeHTTP()
    http.responses = [
        {"order": {"order_id": "o-auto"}},
        {"order_id": "o-auto"},
        {"order_id": "o-auto"},
        {"market": {"ticker": "LATER", "exchange_index": 2}},
        {"order_id": "o-auto"},
        {"order": {"order_id": "o-resp", "exchange_index": 1}},
        {"order_id": "o-resp"},
    ]
    client = make_client(http=http)

    # Created without a known shard (-1). The shard is still unknown, but the
    # ticker is remembered, so cancel/decrease send -1 + market_ticker.
    client.create_order(_create("LATER"))
    assert http.calls[0][2]["body"]["exchange_index"] == -1
    client.cancel_order(order_id="o-auto")
    assert http.calls[1][2]["params"] == {"subaccount": 0, "exchange_index": -1, "market_ticker": "LATER"}
    client.decrease_order_to(order_id="o-auto", remaining_count_units=100)
    assert http.calls[2][2]["body"] == {"reduce_to": "1.00", "exchange_index": -1, "market_ticker": "LATER"}

    # Once the market's shard is learned, the order resolves to it explicitly.
    client.get_market("LATER")
    client.cancel_order(order_id="o-auto")
    assert http.calls[4][2]["params"] == {"subaccount": 0, "exchange_index": 2, "market_ticker": "LATER"}

    # A create response that reports exchange_index wins over the value sent.
    client.create_order(_create("LATER", "c9"))
    client.cancel_order(order_id="o-resp")
    assert http.calls[6][2]["params"]["exchange_index"] == 1


def test_cancel_of_unknown_order_omits_routing_and_warns_once(caplog):
    http = FakeHTTP()
    http.responses = [{"order_id": "ghost"}, {"order_id": "ghost"}, {"order_id": "ghost"}]
    client = make_client(http=http, subaccount_number=1)

    with caplog.at_level(logging.WARNING, logger="kalshi_top_of_book_bot"):
        client.cancel_order(order_id="ghost")
        client.decrease_order_to(order_id="ghost", remaining_count_units=100)
        client.cancel_order(order_id="ghost")

    # Docs: -1 "require[s] auto-routing by market ticker"; with no ticker the
    # parameter is omitted and the venue defaults to shard 0, warned once.
    assert http.calls[0][2]["params"] == {"subaccount": 1}
    assert http.calls[1][2]["body"] == {"reduce_to": "1.00"}
    assert http.calls[2][2]["params"] == {"subaccount": 1}
    warnings = [r for r in caplog.records if "ORDER_ROUTE_UNKNOWN" in r.getMessage()]
    assert len(warnings) == 1
    assert "ghost" in warnings[0].getMessage()


def test_resting_orders_and_stream_updates_seed_routing_after_restart():
    http = FakeHTTP()
    http.responses = [
        {"orders": [
            {"order_id": "r-shard", "ticker": "TENNIS", "status": "resting", "exchange_index": 3},
            {"order_id": "r-ticker", "ticker": "TENNIS", "status": "resting"},
        ], "cursor": ""},
        {"order_id": "r-shard"},
        {"order_id": "r-ticker"},
        {"order_id": "ws-order"},
    ]
    client = make_client(http=http)
    resting = client.get_resting_orders("TENNIS")
    assert [o.order_id for o in resting] == ["r-shard", "r-ticker"]

    client.cancel_order(order_id="r-shard")
    assert http.calls[1][2]["params"] == {"subaccount": 0, "exchange_index": 3, "market_ticker": "TENNIS"}
    client.cancel_order(order_id="r-ticker")
    assert http.calls[2][2]["params"] == {"subaccount": 0, "exchange_index": -1, "market_ticker": "TENNIS"}

    event = client._event(
        {"type": "user_order", "msg": {"ticker": "MLB-9", "side": "yes", "order_id": "ws-order", "status": "resting"}},
        "MLB-9",
    )
    assert isinstance(event, OrderUpdate)
    client.cancel_order(order_id="ws-order")
    assert http.calls[3][2]["params"] == {"subaccount": 0, "exchange_index": -1, "market_ticker": "MLB-9"}


def test_dry_run_writes_still_short_circuit_with_sharding():
    http = FakeHTTP()
    client = make_client(http=http, dry_run=True)
    order = client.create_order(_create("TENNIS"))
    assert order.order_id.startswith("DRY-")
    assert client.cancel_order(order_id=order.order_id).order_id == order.order_id
    assert client.decrease_order_to(order_id=order.order_id, remaining_count_units=10).order_id == order.order_id
    assert http.calls == []


def test_dry_run_create_reports_the_outcome_side_like_a_live_answer():
    client = make_client(http=FakeHTTP(), dry_run=True)
    sell_yes = CreateOrderRequest("MKT", "yes", 4_000, 100, "wd:yes:1", None, action="sell", reduce_only=True)
    assert client.create_order(sell_yes).side == "no"
    assert client.create_order(CreateOrderRequest("MKT", "no", 4_000, 100, "c2", None, action="sell")).side == "yes"
    assert client.create_order(_create("MKT")).side == "yes"
