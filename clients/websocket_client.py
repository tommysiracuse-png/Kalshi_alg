"""Venue-independent websocket transport with raw-message iteration."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Iterable, Mapping, Optional, Union

try:
    import websockets
except ImportError:  # Public REST-only tools may run without websocket extras.
    websockets = None  # type: ignore[assignment]

from .monitoring import ActivityMonitor
from .http_client import _redact_proxy_text


WebsocketPayload = Union[str, bytes]


class WebsocketClient:
    def __init__(
        self,
        url: str,
        *,
        ping_interval_seconds: float = 20,
        ping_timeout_seconds: float = 20,
        proxy_url: Optional[str] = None,
        connect_factory: Optional[Any] = None,
    ) -> None:
        self.url = url
        self.ping_interval_seconds = ping_interval_seconds
        self.ping_timeout_seconds = ping_timeout_seconds
        self.proxy_url = str(proxy_url or "").strip() or None
        if connect_factory is None and websockets is None:
            async def unavailable(*_args: Any, **_kwargs: Any) -> Any:
                raise RuntimeError("websockets dependency is required for streaming")
            connect_factory = unavailable
        self._connect_factory = connect_factory or websockets.connect
        self._connection_context: Optional[Any] = None
        self._connection: Optional[Any] = None
        self._send_lock = asyncio.Lock()
        self.activity = ActivityMonitor()

    async def subscribe(
        self,
        messages: Iterable[WebsocketPayload],
        *,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        await self.close()
        self.activity.record_stream("connectAttempts")
        kwargs = {
            "ping_interval": self.ping_interval_seconds,
            "ping_timeout": self.ping_timeout_seconds,
            "proxy": self.proxy_url,
        }

        async def open_connection(header_keyword: str, *, omit_proxy: bool = False) -> tuple[Any, Any]:
            connect_kwargs = dict(kwargs)
            if omit_proxy:
                connect_kwargs.pop("proxy", None)
            try:
                context = self._connect_factory(
                    self.url,
                    **{header_keyword: dict(headers or {})},
                    **connect_kwargs,
                )
            except TypeError:
                # Let the caller retry the legacy header keyword.  A real
                # proxy configuration must never silently fall back to an
                # ambient process proxy or direct connection.
                if self.proxy_url:
                    raise RuntimeError("installed websockets version does not support explicit proxy routing")
                raise
            if hasattr(context, "__aenter__"):
                return context, await context.__aenter__()
            return context, await context

        try:
            try:
                context, connection = await open_connection("additional_headers")
            except TypeError:
                try:
                    context, connection = await open_connection("extra_headers")
                except TypeError:
                    # Older websockets releases also reject the explicit
                    # ``proxy=None`` keyword. Their connector is direct by
                    # default, so remove only that optional keyword after the
                    # header compatibility retry.
                    if self.proxy_url:
                        raise RuntimeError("installed websockets version does not support explicit proxy routing")
                    context, connection = await open_connection("extra_headers", omit_proxy=True)
        except Exception as exc:
            self.activity.record_stream("connectionErrors")
            # A connector error often includes the complete proxy URL. Keep
            # the original exception type while removing proxy credentials
            # before it reaches a caller's log or status payload.
            safe = _redact_proxy_text(exc, self.proxy_url)
            if safe != str(exc):
                try:
                    exc.args = (safe,)
                except Exception:
                    pass
            raise
        self._connection_context = context
        self._connection = connection
        self.activity.record_stream("connections")
        for message in messages:
            await self.send(message, subscription=True)

    async def send(self, message: WebsocketPayload, *, subscription: bool = False) -> None:
        """Send a command on the current connection without reconnecting it."""

        async with self._send_lock:
            if self._connection is None:
                raise RuntimeError("WebsocketClient is not connected")
            await self._connection.send(message)
            self.activity.record_stream("subscriptionsSent" if subscription else "commandsSent")

    def __aiter__(self) -> AsyncIterator[WebsocketPayload]:
        if self._connection is None:
            raise RuntimeError("WebsocketClient must be subscribed before iteration")
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[WebsocketPayload]:
        assert self._connection is not None
        try:
            async for message in self._connection:
                self.activity.record_stream("message")
                yield message
        except Exception:
            self.activity.record_stream("streamErrors")
            raise

    async def close(self) -> None:
        connection = self._connection
        context = self._connection_context
        self._connection = None
        self._connection_context = None
        if connection is not None:
            self.activity.record_stream("closes")
            try:
                await connection.close()
            except Exception:
                pass
        if context is not None and hasattr(context, "__aexit__"):
            try:
                await context.__aexit__(None, None, None)
            except Exception:
                pass

    def activity_snapshot(self) -> dict:
        return self.activity.snapshot()
