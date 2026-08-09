import asyncio

import pytest

from clients.http_client import HTTPClient, HTTPClientError
from clients.websocket_client import WebsocketClient


class FakeResponse:
    def __init__(self, status_code=200, text='{"ok": true}', payload=None):
        self.status_code = status_code
        self.text = text
        self.payload = {"ok": True} if payload is None else payload

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []
        self.response = FakeResponse()

    def _call(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.response

    def get(self, url, **kwargs):
        return self._call("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._call("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._call("DELETE", url, **kwargs)


def test_http_client_supports_all_verbs_and_empty_responses():
    session = FakeSession()
    client = HTTPClient("https://example.test/", timeout_seconds=7, session=session)

    assert client.get("/one", headers={"x": "1"}, params={"a": 2}) == {"ok": True}
    assert client.post("two", body={"value": 3}) == {"ok": True}
    session.response = FakeResponse(text="", payload={})
    assert client.delete("/three") == {}

    assert [call[0] for call in session.calls] == ["GET", "POST", "DELETE"]
    assert session.calls[0][1] == "https://example.test/one"
    assert session.calls[0][2]["timeout"] == 7
    assert session.calls[1][2]["json"] == {"value": 3}


def test_http_client_raises_structured_error():
    session = FakeSession()
    session.response = FakeResponse(status_code=429, text="rate limited")
    client = HTTPClient("https://example.test", session=session)

    with pytest.raises(HTTPClientError) as caught:
        client.get("/orders")

    assert caught.value.method == "GET"
    assert caught.value.path == "/orders"
    assert caught.value.status_code == 429
    assert caught.value.response_text == "rate limited"
    activity = client.activity_snapshot()["rest"]
    assert activity["total"] == 1
    assert activity["errors"] == 1
    assert activity["byStatus"] == {"429": 1}


def test_http_activity_uses_logical_operations_without_request_data():
    client = HTTPClient("https://example.test", session=FakeSession())
    client.get("/markets/SECRET", headers={"Authorization": "secret"}, operation="get_market")
    activity = client.activity_snapshot()
    assert activity["rest"]["byOperation"] == {"get_market": 1}
    assert activity["rest"]["requestsLast60s"] == 1
    assert "SECRET" not in str(activity)
    assert "Authorization" not in str(activity)


class FakeConnection:
    def __init__(self):
        self.sent = []
        self.closed = False
        self.messages = iter(("first", "second"))

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        async def messages():
            for message in self.messages:
                yield message

        return messages()

    async def close(self):
        self.closed = True


class FakeContext:
    def __init__(self, connection):
        self.connection = connection
        self.exited = False

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *args):
        self.exited = True


def test_websocket_subscribe_iterate_and_close():
    async def scenario():
        connection = FakeConnection()
        context = FakeContext(connection)
        calls = []

        def connect(url, **kwargs):
            calls.append((url, kwargs))
            return context

        client = WebsocketClient("wss://example.test/ws", connect_factory=connect)
        await client.subscribe(["sub-1", "sub-2"], headers={"Authorization": "value"})
        received = [message async for message in client]
        await client.close()

        assert connection.sent == ["sub-1", "sub-2"]
        assert received == ["first", "second"]
        assert calls[0][1]["additional_headers"] == {"Authorization": "value"}
        assert connection.closed and context.exited
        activity = client.activity_snapshot()["stream"]
        assert activity["connections"] == 1
        assert activity["subscriptionsSent"] == 2
        assert activity["message"] == 2
        assert activity["messagesLast60s"] == 2

    asyncio.run(scenario())


def test_websocket_falls_back_to_legacy_header_keyword():
    async def scenario():
        connection = FakeConnection()
        context = FakeContext(connection)
        calls = []

        def connect(url, **kwargs):
            calls.append(kwargs)
            if "additional_headers" in kwargs:
                raise TypeError("unsupported")
            return context

        client = WebsocketClient("wss://example.test/ws", connect_factory=connect)
        await client.subscribe([], headers={"x": "y"})
        await client.close()
        assert calls[1]["extra_headers"] == {"x": "y"}

    asyncio.run(scenario())


def test_websocket_falls_back_when_header_error_is_deferred_until_enter():
    async def scenario():
        connection = FakeConnection()
        calls = []

        class DeferredFailure:
            async def __aenter__(self):
                raise TypeError("unsupported at connect time")

        def connect(url, **kwargs):
            calls.append(kwargs)
            if "additional_headers" in kwargs:
                return DeferredFailure()
            return FakeContext(connection)

        client = WebsocketClient("wss://example.test/ws", connect_factory=connect)
        await client.subscribe([], headers={"x": "y"})
        await client.close()
        assert calls[1]["extra_headers"] == {"x": "y"}

    asyncio.run(scenario())
