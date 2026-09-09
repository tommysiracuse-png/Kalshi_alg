from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import replace
from types import SimpleNamespace

import pytest

from adaptors.polymarket import PolymarketClient, PolymarketClientConfig
from clients.models import AccountPositionQuery, Market, MarketQuery
from fleet_runtime.worker import restore_cached_polymarket_book, restore_market_top_book, seed_risk_from_book
from fleet_runtime.risk import RiskSample
from polymarket_cache import BookTop
from screener import _BaseClientMarketSource
from top_of_book_bot import MarketActor


class _BalanceSdk:
    def __init__(self) -> None:
        self.params = None

    def get_balance_allowance(self, params):
        self.params = params
        # The CLOB returns six-decimal pUSD base units (12.345678 USDC).
        return {"balance": "12345678", "allowance": "99000000"}


def test_polymarket_balance_requests_collateral_allowance():
    client = PolymarketClient(PolymarketClientConfig(private_key_path="unused"))
    sdk = _BalanceSdk()
    client._require_auth = lambda: sdk  # type: ignore[method-assign]

    balance = client.get_account_balance()

    assert sdk.params.asset_type == "COLLATERAL"
    assert sdk.params.token_id is None
    assert balance.available_cash_units == 123_457
    assert balance.currency == "USDC"


def test_polymarket_rejects_negative_collateral_balance():
    client = PolymarketClient(PolymarketClientConfig(private_key_path="unused"))
    sdk = _BalanceSdk()
    sdk.get_balance_allowance = lambda params: {"balance": "-1", "allowance": "0"}
    client._require_auth = lambda: sdk  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="non-negative"):
        client.get_account_balance()


def test_polymarket_accepts_plural_allowances_response():
    client = PolymarketClient(PolymarketClientConfig(private_key_path="unused"))
    sdk = _BalanceSdk()
    sdk.get_balance_allowance = lambda params: {"balance": "12345678", "allowances": {"exchange": "99000000"}}
    client._require_auth = lambda: sdk  # type: ignore[method-assign]
    balance = client.get_account_balance()
    assert balance.available_cash_units == 123_457
    assert balance.allowance_units == 990_000


def test_polymarket_positions_retain_data_api_marks_for_archived_markets():
    client = PolymarketClient(PolymarketClientConfig(funder_address="0x" + "1" * 40))
    client.data_http = SimpleNamespace(get=lambda path, *, params, operation: [{
        "conditionId": "0x" + "2" * 64,
        "asset": "123",
        "size": "8",
        "outcome": "No",
        "curPrice": "0.25",
        "currentValue": "2.00",
        "initialValue": "1.50",
        "realizedPnl": "0.10",
        "title": "Archived market",
        "slug": "archived-market",
    }])

    positions = client.list_account_positions(AccountPositionQuery(nonzero_only=True))

    assert len(positions) == 1
    position = positions[0]
    assert position.position_units == -800
    assert position.mark_price_units == 2_500
    assert position.current_value_units == 20_000
    assert position.market_exposure_units == 15_000
    assert position.realized_pnl_units == 1_000
    assert position.title == "Archived market"
    assert position.market_url == "https://polymarket.com/event/archived-market"


def test_polymarket_market_normalization_keeps_missing_open_interest_optional():
    client = PolymarketClient(PolymarketClientConfig())
    base = {
        "conditionId": "condition-1",
        "question": "Question",
        "active": True,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["yes", "no"],
    }

    missing = client._normalize_market(dict(base))
    supplied = client._normalize_market({**base, "conditionId": "condition-2", "openInterest": "12.34"})
    explicit_zero = client._normalize_market({**base, "conditionId": "condition-3", "openInterest": 0})

    assert missing is not None and missing.open_interest_units is None
    assert supplied is not None and supplied.open_interest_units == 1_234
    assert explicit_zero is not None and explicit_zero.open_interest_units == 0


def test_polymarket_hydrates_open_interest_in_batches_and_preserves_missing_values():
    client = PolymarketClient(PolymarketClientConfig())
    calls = []

    class Data:
        def get(self, path, *, params, operation):
            calls.append((path, dict(params), operation))
            return [
                {"market": "condition-1", "value": "12.34"},
                {"market": "condition-2", "value": 0},
            ]

    client.data_http = Data()
    markets = [
        Market("condition-1", open_interest_units=None),
        Market("condition-2", open_interest_units=None),
        Market("condition-3", open_interest_units=9_999),
    ]

    hydrated, stats = client.hydrate_market_open_interest(markets)

    assert len(calls) == 1
    assert calls[0][0] == "/oi"
    assert calls[0][1]["market"] == "condition-1,condition-2,condition-3"
    assert calls[0][2] == "polymarket_get_open_interest"
    assert [market.open_interest_units for market in hydrated] == [1_234, 0, None]
    assert stats == {
        "batches": 1,
        "marketsRequested": 3,
        "marketsResolved": 2,
        "marketsMissing": 1,
        "apiErrors": 0,
    }


def test_polymarket_open_interest_batch_failure_does_not_abort_catalog_enrichment():
    client = PolymarketClient(PolymarketClientConfig())

    class Data:
        def get(self, path, *, params, operation):
            raise RuntimeError("temporary data api failure")

    client.data_http = Data()
    hydrated, stats = client.hydrate_market_open_interest([Market("condition-1")])

    assert hydrated[0].open_interest_units is None
    assert stats["batches"] == 1
    assert stats["marketsRequested"] == 1
    assert stats["marketsResolved"] == 0
    assert stats["marketsMissing"] == 1
    assert stats["apiErrors"] == 1


def test_polymarket_market_pagination_stops_on_a_cursor_cycle():
    market_payload = {
        "conditionId": "condition-1",
        "question": "Question",
        "active": True,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["yes", "no"],
    }

    class Gamma:
        def __init__(self):
            self.calls = []

        def get(self, path, *, params, operation):
            self.calls.append(dict(params))
            return {"data": [market_payload], "next_cursor": "same-cursor"}

    client = PolymarketClient(PolymarketClientConfig(private_key_path="unused"))
    gamma = Gamma()
    client.gamma_http = gamma

    markets = client.list_markets(MarketQuery(status="open", page_size=1, max_results=10))

    assert len(markets) == 2
    assert len(gamma.calls) == 2


def test_polymarket_market_scan_honors_shutdown_cancellation():
    class Gamma:
        def get(self, path, *, params, operation):
            raise AssertionError("cancelled scan must not start another page")

    client = PolymarketClient(PolymarketClientConfig(private_key_path="unused"))
    client.gamma_http = Gamma()
    client.begin_screener_scan()
    client.cancel_screener_scan()

    assert client.list_markets(MarketQuery(status="open", page_size=100, max_results=100)) == []


def test_polymarket_market_lookup_honors_condition_id_filter():
    payload = {
        "conditionId": "condition-held",
        "question": "Held market",
        "active": True,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["yes-token", "no-token"],
    }

    class Gamma:
        def __init__(self):
            self.calls = []

        def get(self, path, *, params=None, operation):
            self.calls.append((path, params, operation))
            assert path == "/markets/condition-held"
            return payload

    client = PolymarketClient(PolymarketClientConfig())
    gamma = Gamma()
    client.gamma_http = gamma

    markets = client.list_markets(MarketQuery(status="", max_results=1, venue_filters={"tickers": "condition-held"}))

    assert [market.market_id for market in markets] == ["condition-held"]
    assert gamma.calls == [("/markets/condition-held", None, "polymarket_get_market")]


def test_polymarket_hex_condition_id_uses_gamma_condition_filter():
    condition_id = "0x" + "ab" * 32
    market_payload = {
        "conditionId": condition_id,
        "question": "Held market",
        "active": True,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["yes-token", "no-token"],
    }

    class Gamma:
        def __init__(self):
            self.calls = []

        def get(self, path, *, params=None, operation):
            self.calls.append((path, params, operation))
            assert path == "/markets"
            assert params == {"condition_ids": [condition_id], "limit": 1}
            return {"data": [market_payload]}

    client = PolymarketClient(PolymarketClientConfig())
    gamma = Gamma()
    client.gamma_http = gamma

    markets = client.list_markets(
        MarketQuery(status="open", max_results=1, venue_filters={"tickers": condition_id})
    )

    assert [market.market_id for market in markets] == [condition_id]
    assert gamma.calls == [("/markets", {"condition_ids": [condition_id], "limit": 1}, "polymarket_get_market")]


def test_private_key_file_whitespace_is_removed_before_clob_authentication(tmp_path):
    key = "0x" + "a" * 64
    path = tmp_path / "polymarket.key"
    path.write_text(f"\n  {key}  \n")

    client = PolymarketClient(PolymarketClientConfig(private_key_path=str(path)))

    assert client._private_key_value() == key


def test_polymarket_cancel_uses_v2_order_payload():
    class Sdk:
        def __init__(self):
            self.payload = None

        def cancel_order(self, payload):
            assert payload.orderID == "order-1"
            self.payload = payload
            return {"success": True}

    sdk = Sdk()
    client = PolymarketClient(
        PolymarketClientConfig(private_key="0x" + "a" * 64),
        clob_client=sdk,
    )

    canceled = client.cancel_order(order_id="order-1")

    assert sdk.payload.orderID == "order-1"
    assert canceled.order_id == "order-1"
    assert canceled.status == "canceled"


def test_invalid_stored_api_credentials_are_rederived_once():
    class Unauthorized(Exception):
        status_code = 401

    class Sdk:
        def __init__(self):
            self.creds = None
            self.calls = 0

        def create_or_derive_api_key(self):
            return "fresh-creds"

    sdk = Sdk()
    client = PolymarketClient(PolymarketClientConfig(
        private_key="0x" + "a" * 64,
        api_key="stale-key",
        api_secret="stale-secret",
        api_passphrase="stale-passphrase",
    ), clob_client=sdk)

    def operation():
        sdk.calls += 1
        if sdk.calls == 1:
            raise Unauthorized("Unauthorized/Invalid api key")
        return "ok"

    assert client._sdk_call("test_operation", operation) == "ok"
    assert sdk.calls == 2
    assert sdk.creds == "fresh-creds"


def test_polymarket_account_limits_provide_nonzero_local_pacing():
    client = PolymarketClient(PolymarketClientConfig())

    limits = client.get_account_limits()

    assert limits.read.refill_rate > 0
    assert limits.read.bucket_capacity >= limits.read.refill_rate
    assert limits.write.refill_rate > 0
    assert limits.write.bucket_capacity >= limits.write.refill_rate


def test_polymarket_orders_and_fills_normalize_token_side():
    class Sdk:
        def get_open_orders(self, params=None):
            return [{"id": "o1", "market": "condition-1", "asset_id": "yes", "side": "BUY", "size": "2", "price": "0.4", "status": "LIVE"}]

        def get_trades(self, params=None):
            return [{"id": "t1", "order_id": "o1", "market": "condition-1", "asset_id": "yes", "size": "1", "price": "0.4"}]

    client = PolymarketClient(
        PolymarketClientConfig(private_key="0x" + "a" * 64),
        clob_client=Sdk(),
    )
    client._asset_market["yes"] = ("condition-1", "yes")
    assert client.list_account_orders()[0].side == "yes"
    assert client.list_account_orders()[0].action == "buy"
    assert client.list_account_fills()[0].side == "yes"


def test_polymarket_proxy_is_applied_to_all_adaptor_transports():
    client = PolymarketClient(PolymarketClientConfig(proxy_url="socks5h://127.0.0.1:9050"))

    assert client.http_client.proxy_url == "socks5h://127.0.0.1:9050"
    assert client.http_client.session.trust_env is False
    assert client.gamma_http.proxy_url == "socks5h://127.0.0.1:9050"
    assert client.data_http.proxy_url == "socks5h://127.0.0.1:9050"
    assert client.websocket_client.proxy_url == "socks5h://127.0.0.1:9050"
    assert client._user_websocket.proxy_url == "socks5h://127.0.0.1:9050"


def test_polymarket_proxy_scheme_is_validated():
    with pytest.raises(ValueError, match="proxy_url"):
        PolymarketClientConfig(proxy_url="ftp://proxy.test:21")


def test_prime_market_seeds_condition_and_token_ids():
    client = PolymarketClient(PolymarketClientConfig())

    market = client.prime_market({
        "condition_id": "condition-1",
        "question": "Question",
        "active": True,
        "outcomes": ["Yes", "No"],
        "yes_token_id": "yes-token",
        "no_token_id": "no-token",
    })

    assert market is not None
    assert market.market_id == "condition-1"
    assert market.yes_token_id == "yes-token"
    assert market.no_token_id == "no-token"
    assert client._market_cache["condition-1"] is market


def test_polymarket_screen_source_hydrates_clob_books_before_shared_filters():
    market = Market(
        "condition-1", title="Question", status="active",
        close_time_ms=int(time.time() * 1000) + 3_600_000,
        volume_24h_units=100_000, open_interest_units=1_000,
        venue="polymarket", yes_token_id="yes", no_token_id="no",
    )
    hydrated = replace(
        market, yes_bid_units=4_000, yes_ask_units=4_500,
        no_bid_units=5_000, no_ask_units=5_500,
        yes_bid_size_units=100, yes_ask_size_units=100,
        no_bid_size_units=100, no_ask_size_units=100,
    )
    calls = []
    client = SimpleNamespace(
        venue_name="polymarket",
        list_markets=lambda query: [market],
        get_market=lambda market_id: calls.append(market_id) or hydrated,
    )
    source = _BaseClientMarketSource(client, {"min_vol24h": 0, "min_oi": 0, "max_time_to_close_hrs": 10})

    rows = list(source.list_markets(status="open", limit=100, max_total=10, mve_filter=None))

    assert calls == ["condition-1"]
    assert rows[0]["yes_bid_dollars"] == "0.4000"
    assert rows[0]["no_bid_dollars"] == "0.5000"


def test_polymarket_bulk_book_hydration_uses_books_endpoint():
    def book(asset, bid, ask):
        return {"asset_id": asset, "bids": [{"price": str(bid), "size": "2"}], "asks": [{"price": str(ask), "size": "3"}]}

    market = Market("condition-1", title="Question", status="active", venue="polymarket", yes_token_id="yes", no_token_id="no")
    calls = []
    client = PolymarketClient(PolymarketClientConfig(book_batch_size=10))
    client.http_client = SimpleNamespace(post=lambda path, *, body, operation: calls.append((path, body, operation)) or [book("yes", .4, .5), book("no", .6, .7)])

    hydrated = client.hydrate_market_books([market])

    assert list(hydrated) == ["condition-1"]
    assert hydrated["condition-1"].yes_bid_units == 4_000
    assert hydrated["condition-1"].no_bid_units == 6_000
    assert calls == [("/books", [{"token_id": "yes"}, {"token_id": "no"}], "polymarket_get_order_books")]


def test_polymarket_bulk_book_hydration_skips_market_with_missing_token_book():
    market = Market("condition-1", status="active", venue="polymarket", yes_token_id="yes", no_token_id="no")
    client = PolymarketClient(PolymarketClientConfig())
    client.http_client = SimpleNamespace(post=lambda path, *, body, operation: [{"asset_id": "yes", "bids": [], "asks": []}])

    assert client.hydrate_market_books([market]) == {}


def test_polymarket_market_websocket_bootstrap_requests_initial_dump():
    class Socket:
        def __init__(self):
            self.messages = iter([json.dumps({"event_type": "book", "asset_id": "yes", "bids": [{"price": "0.4", "size": "2"}], "asks": [{"price": "0.5", "size": "3"}]}), json.dumps({"event_type": "book", "asset_id": "no", "bids": [{"price": "0.6", "size": "2"}], "asks": [{"price": "0.7", "size": "3"}]})])
            self.sent = []
        async def send(self, message): self.sent.append(message)
        def __aiter__(self):
            async def iterate():
                for message in self.messages:
                    yield message
            return iterate()
        async def close(self): pass

    socket = Socket()
    class Ws:
        async def subscribe(self, messages):
            self.messages = messages
        def __aiter__(self): return socket.__aiter__()
    client = PolymarketClient(PolymarketClientConfig())
    client.websocket_client = Ws()
    markets = [Market("condition-1", venue="polymarket", yes_token_id="yes", no_token_id="no")]

    import asyncio
    result = asyncio.run(client.bootstrap_market_books(markets, timeout_seconds=1))

    assert result["yes"].bid_units == 4_000
    assert result["no"].ask_units == 7_000
    subscription = json.loads(client.websocket_client.messages[0])
    assert subscription["initial_dump"] is True
    assert subscription["assets_ids"] == ["yes", "no"]


def test_polymarket_market_stream_can_start_without_waiting_for_initial_dump():
    class Ws:
        def __init__(self):
            self.messages = []

        async def subscribe(self, messages):
            self.messages.extend(messages)

        async def close(self):
            return None

        def __aiter__(self):
            async def iterate():
                if False:
                    yield None
            return iterate()

    client = PolymarketClient(PolymarketClientConfig())
    client.websocket_client = Ws()
    markets = [Market("condition-1", venue="polymarket", yes_token_id="yes", no_token_id="no")]

    import asyncio
    async def start_and_close():
        await client.start_market_book_stream(markets)
        await asyncio.sleep(0)
        await client.close()

    asyncio.run(start_and_close())
    subscription = json.loads(client.websocket_client.messages[0])
    assert subscription["initial_dump"] is True
    assert subscription["assets_ids"] == ["yes", "no"]


def test_polymarket_market_stream_many_requests_initial_dump():
    class Socket:
        def __aiter__(self):
            async def iterate():
                if False:
                    yield None
            return iterate()

    class Ws:
        def __init__(self):
            self.messages = []

        async def subscribe(self, messages):
            self.messages.extend(messages)

        def __aiter__(self):
            return Socket().__aiter__()

    client = PolymarketClient(PolymarketClientConfig())
    client.websocket_client = Ws()
    client.prime_market({
        "conditionId": "condition-stream",
        "question": "Question",
        "active": True,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["yes", "no"],
    })

    import asyncio
    async def consume():
        async for _event in client.stream_events_many(["condition-stream"]):
            pass

    asyncio.run(consume())
    subscription = json.loads(client.websocket_client.messages[0])
    assert subscription["initial_dump"] is True
    assert subscription["assets_ids"] == ["yes", "no"]


def test_worker_restores_cached_polymarket_book_for_quiet_market():
    events = []
    actor = SimpleNamespace(
        market=SimpleNamespace(venue="polymarket", yes_token_id="yes", no_token_id="no"),
        settings=SimpleNamespace(market_ticker="condition-stream"),
        handle_event=events.append,
    )
    cache = SimpleNamespace(
        get=lambda asset: (
            BookTop(4_000, 200, 5_000, 300, timestamp_ms=123)
            if asset == "yes" else BookTop(5_000, 300, 6_000, 400, timestamp_ms=123)
            if asset == "no" else None
        ),
    )

    assert restore_cached_polymarket_book(actor, SimpleNamespace(book_cache=cache))
    assert len(events) == 1
    snapshot = events[0]
    assert snapshot.yes_levels == {4_000: 200}
    assert snapshot.no_levels == {5_000: 300}
    assert snapshot.yes_ask_levels == {5_000: 300}


def test_worker_restores_polymarket_top_book_from_rest_fallback():
    events = []
    actor = SimpleNamespace(
        market=SimpleNamespace(venue="polymarket"),
        settings=SimpleNamespace(market_ticker="condition-rest"),
        handle_event=events.append,
    )
    market = SimpleNamespace(
        yes_bid_units=4_000, yes_bid_size_units=200,
        no_bid_units=5_000, no_bid_size_units=300,
        yes_ask_units=4_500, yes_ask_size_units=100,
        no_ask_units=5_500, no_ask_size_units=100,
    )

    assert restore_market_top_book(actor, market)
    assert events[0].yes_levels == {4_000: 200}
    assert events[0].no_levels == {5_000: 300}


def test_restored_polymarket_book_seeds_risk_for_immediate_quote():
    risk = {}
    actor = SimpleNamespace(
        book_ready=True,
        book_yes={4_000: 200},
        book_no={5_000: 300},
        settings=SimpleNamespace(market_ticker="condition-risk"),
        set_fleet_risk=lambda mode, *, reason, generated_at_ms: risk.update(
            mode=mode, reason=reason, generated_at_ms=generated_at_ms
        ),
    )
    window = deque()

    assert seed_risk_from_book(actor, window, {}, timestamp_ms=1234)
    assert list(window) == [RiskSample(1234, 4_000, 5_000)]
    assert risk == {"mode": "normal", "reason": "book_bootstrap", "generated_at_ms": 1234}


def test_polymarket_cached_top_book_does_not_fail_second_level_gap_guard():
    actor = MarketActor.__new__(MarketActor)
    actor.market = SimpleNamespace(venue="polymarket")
    actor.settings = SimpleNamespace(
        minimum_best_bid_cents_required_to_quote=0,
        minimum_implied_ask_cents_required_to_quote=0,
        minimum_top_level_depth_contracts=0,
        maximum_top_level_gap_cents=15,
        suppress_same_side_quotes_during_reentry_cooldown=False,
    )
    actor.side_reduces_inventory_risk = lambda _side: False
    actor.inventory_blocked_side = lambda: None
    actor.side_same_side_reentry_cooldown_active = lambda _side: False
    actor.side_queue_abandonment_cooldown_active = lambda _side: False
    actor.side_liquidity_pull_cooldown_active = lambda _side: False
    actor.market_queue_cooldown_active = lambda: False
    actor.market_toxicity_cooldown_active = lambda: False
    actor.second_best_bid = lambda _side: None

    allowed, reason = actor.quote_gate_status(
        side="yes", best_bid_units=4_000, implied_ask_units=6_000
    )

    assert (allowed, reason) == (True, "ok")


def test_kalshi_still_requires_second_level_gap_data():
    actor = MarketActor.__new__(MarketActor)
    actor.market = SimpleNamespace(venue="kalshi")
    actor.settings = SimpleNamespace(
        minimum_best_bid_cents_required_to_quote=0,
        minimum_implied_ask_cents_required_to_quote=0,
        minimum_top_level_depth_contracts=0,
        maximum_top_level_gap_cents=15,
        suppress_same_side_quotes_during_reentry_cooldown=False,
    )
    actor.side_reduces_inventory_risk = lambda _side: False
    actor.inventory_blocked_side = lambda: None
    actor.side_same_side_reentry_cooldown_active = lambda _side: False
    actor.side_queue_abandonment_cooldown_active = lambda _side: False
    actor.side_liquidity_pull_cooldown_active = lambda _side: False
    actor.market_queue_cooldown_active = lambda: False
    actor.market_toxicity_cooldown_active = lambda: False
    actor.second_best_bid = lambda _side: None

    allowed, reason = actor.quote_gate_status(
        side="yes", best_bid_units=4_000, implied_ask_units=6_000
    )

    assert (allowed, reason) == (False, "wide_book_gap")


def test_polymarket_catalog_store_uses_bulk_offset_pages(tmp_path):
    payload = {
        "conditionId": "condition-1", "question": "Question", "active": True,
        "outcomes": ["Yes", "No"], "clobTokenIds": ["yes", "no"],
    }
    class Gamma:
        def __init__(self): self.calls = []
        def get(self, path, *, params, operation):
            self.calls.append((path, dict(params), operation))
            return [payload] if params["offset"] == 0 else []
    gamma = Gamma()
    client = PolymarketClient(PolymarketClientConfig(catalog_path=str(tmp_path / "catalog.sqlite3")))
    client.gamma_http = gamma

    markets = client.list_markets(MarketQuery(status="open", page_size=1000, max_results=10))

    assert [item.market_id for item in markets] == ["condition-1"]
    assert gamma.calls
    assert all(call[0] == "/markets" for call in gamma.calls)
    assert gamma.calls[0][1]["limit"] == 1000
    assert all(call[1]["limit"] >= 1 for call in gamma.calls)


def test_polymarket_catalog_paginates_100k_rows_with_deduplication(tmp_path):
    rows = [
        {
            "conditionId": f"condition-{index}", "question": f"Question {index}",
            "active": True, "outcomes": ["Yes", "No"],
            "clobTokenIds": [f"yes-{index}", f"no-{index}"],
        }
        for index in range(100_000)
    ]
    calls = []

    class Gamma:
        def get(self, path, *, params, operation):
            calls.append(params["offset"])
            start = int(params["offset"])
            page = rows[start:start + int(params["limit"])]
            # Repeat one row on the first page to exercise condition-ID dedupe.
            if start == 0:
                page = page + [rows[0]]
            return page

    client = PolymarketClient(PolymarketClientConfig(
        catalog_path=str(tmp_path / "catalog.sqlite3"), catalog_page_size=1000, catalog_parallelism=4,
    ))
    client.gamma_http = Gamma()

    markets = client.list_markets(MarketQuery(status="open", max_results=100_000))

    assert len(markets) == 100_000
    assert len({market.market_id for market in markets}) == 100_000
    assert client._catalog_metrics["catalogDuplicateCount"] >= 1
    assert client._catalog_metrics["catalogMarkets"] == 100_000


def test_polymarket_catalog_falls_back_to_keyset_after_offset_limit(tmp_path):
    def row(index):
        return {
            "conditionId": f"condition-{index}", "question": f"Question {index}",
            "active": True, "outcomes": ["Yes", "No"],
            "clobTokenIds": [f"yes-{index}", f"no-{index}"],
        }

    class Gamma:
        def get(self, path, *, params, operation):
            if path == "/markets" and int(params["offset"]) > 2000:
                error = RuntimeError("offset too large; use /markets/keyset")
                error.status_code = 422
                raise error
            if path == "/markets":
                start = int(params["offset"])
                return [row(index) for index in range(start, min(start + 100, 2300))]
            assert path == "/markets/keyset"
            cursor = params.get("after_cursor")
            start = int(cursor or 0)
            if start >= 3000:
                return {"data": [], "next_cursor": ""}
            return {"data": [row(index) for index in range(start, start + 100)], "next_cursor": str(start + 100)}

    client = PolymarketClient(PolymarketClientConfig(catalog_path=str(tmp_path / "catalog.sqlite3")))
    client.gamma_http = Gamma()
    markets = client.list_markets(MarketQuery(status="open", max_results=3000))

    assert len(markets) == 3000
    assert client._catalog_metrics["catalogPagination"] == "keyset"
