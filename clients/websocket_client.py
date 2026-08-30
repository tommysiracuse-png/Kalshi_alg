"""Venue-independent websocket transport with raw-message iteration."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Iterable, Mapping, Optional, Union

import websockets

from .monitoring import ActivityMonitor


WebsocketPayload = Union[str, bytes]


class WebsocketClient:
    def __init__(
        self,
        url: str,
        *,
        ping_interval_seconds: float = 20,
        ping_timeout_seconds: float = 20,
        connect_factory: Optional[Any] = None,
    ) -> None:
        self.url = url
        self.ping_interval_seconds = ping_interval_seconds
        self.ping_timeout_seconds = ping_timeout_seconds
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
        }

        async def open_connection(header_keyword: str) -> tuple[Any, Any]:
            context = self._connect_factory(
                self.url,
                **{header_keyword: dict(headers or {})},
                **kwargs,
            )
            if hasattr(context, "__aenter__"):
                return context, await context.__aenter__()
            return context, await context

        try:
            try:
                context, connection = await open_connection("additional_headers")
            except TypeError:
                context, connection = await open_connection("extra_headers")
        except Exception:
            self.activity.record_stream("connectionErrors")
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
