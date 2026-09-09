from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from adaptors.kalshi import KalshiApiClient, KalshiClientConfig
from adaptors.polymarket import PolymarketClient, PolymarketClientConfig
from clients.http_client import HTTPClient
from clients.websocket_client import WebsocketClient


def test_polymarket_proxy_is_explicit_and_ambient_proxy_is_ignored(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://ambient.invalid:9999")
    monkeypatch.setenv("HTTPS_PROXY", "http://ambient.invalid:9999")

    routed = PolymarketClient(PolymarketClientConfig(proxy_url="http://proxy.test:18080"))
    assert routed.gamma_http.proxy_url == "http://proxy.test:18080"
    assert routed.data_http.proxy_url == "http://proxy.test:18080"
    assert routed.http_client.proxy_url == "http://proxy.test:18080"
    assert routed.websocket_client.proxy_url == "http://proxy.test:18080"
    assert routed._user_websocket.proxy_url == "http://proxy.test:18080"
    assert routed.gamma_http.session.trust_env is False
    assert routed.activity_snapshot()["proxyConfigured"] is True

    direct = PolymarketClient(PolymarketClientConfig())
    assert direct.gamma_http.proxy_url is None
    assert direct.gamma_http.session.trust_env is False
    assert direct.activity_snapshot()["proxyConfigured"] is False


def test_polymarket_websocket_allows_large_initial_book_snapshot():
    client = PolymarketClient(PolymarketClientConfig())

    assert client.websocket_client.max_size == 8 * 1024 * 1024
    assert client.websocket_client.ping_timeout_seconds == 60.0
    assert client._user_websocket.max_size == 8 * 1024 * 1024


def test_polymarket_proxy_can_be_sourced_from_venue_environment(monkeypatch):
    monkeypatch.setenv("POLYMARKET_PROXY_URL", "http://proxy.test:18080")
    sourced = PolymarketClient(PolymarketClientConfig())
    assert sourced.http_client.proxy_url == "http://proxy.test:18080"
    direct = PolymarketClient(PolymarketClientConfig(proxy_url=""))
    assert direct.http_client.proxy_url is None


def test_kalshi_transport_does_not_inherit_polymarket_proxy(monkeypatch):
    monkeypatch.setenv("POLYMARKET_PROXY_URL", "http://proxy.test:18080")
    client = KalshiApiClient(KalshiClientConfig(public_only=True))
    assert client.http_client.proxy_url is None
    assert client.websocket_client.proxy_url is None


def test_injected_polymarket_sdk_receives_proxy_configuration(monkeypatch):
    calls: list[str | None] = []
    monkeypatch.setattr("adaptors.polymarket._configure_sdk_proxy", lambda value: calls.append(value or None))
    sdk = SimpleNamespace()
    client = PolymarketClient(
        PolymarketClientConfig(private_key="0x" + "a" * 64, proxy_url="http://proxy.test:18080"),
        clob_client=sdk,
    )
    assert client._require_auth() is sdk
    assert calls == ["http://proxy.test:18080"]


def test_polymarket_config_repr_redacts_credentials_and_proxy():
    value = repr(PolymarketClientConfig(
        private_key="0x" + "a" * 64,
        api_secret="secret-value",
        proxy_url="http://user:password@proxy.test:18080",
    ))
    assert "secret-value" not in value
    assert "password" not in value
    assert "proxy.test" not in value


def test_polymarket_activity_status_contains_only_redacted_proxy_state():
    client = PolymarketClient(PolymarketClientConfig(
        private_key="0x" + "a" * 64,
        api_secret="secret-value",
        proxy_url="http://user:password@proxy.test:18080",
    ))
    activity = str(client.activity_snapshot())
    assert "proxyConfigured" in activity
    assert "secret-value" not in activity
    assert "password" not in activity
    assert "proxy.test" not in activity


def test_proxy_transport_error_is_redacted_from_activity():
    class Session:
        trust_env = False
        proxies = {}

        def get(self, *_args, **_kwargs):
            raise OSError("could not connect to http://user:password@proxy.test:18080")

    client = HTTPClient(
        "https://gamma-api.polymarket.com",
        session=Session(),
        proxy_url="http://user:password@proxy.test:18080",
        trust_env=False,
    )
    with pytest.raises(OSError) as caught:
        client.get("/markets")
    assert "password" not in str(caught.value)
    assert "password" not in str(client.activity_snapshot())


def test_websocket_proxy_transport_error_is_redacted():
    async def connect(*_args, **_kwargs):
        raise OSError("could not connect to http://user:password@proxy.test:18080")

    client = WebsocketClient(
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        proxy_url="http://user:password@proxy.test:18080",
        connect_factory=connect,
    )
    with pytest.raises(OSError) as caught:
        asyncio.run(client.subscribe([]))
    assert "password" not in str(caught.value)
