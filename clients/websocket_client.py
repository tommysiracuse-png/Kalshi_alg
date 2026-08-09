"""Venue-independent websocket transport with raw-message iteration."""

from __future__ import annotations

from typing import Any, AsyncIterator, Iterable, Mapping, Optional, Union

import websockets


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

    async def subscribe(
        self,
        messages: Iterable[WebsocketPayload],
        *,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        await self.close()
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
            context, connection = await open_connection("additional_headers")
        except TypeError:
            context, connection = await open_connection("extra_headers")
        self._connection_context = context
        self._connection = connection
        for message in messages:
            await self._connection.send(message)

    def __aiter__(self) -> AsyncIterator[WebsocketPayload]:
        if self._connection is None:
            raise RuntimeError("WebsocketClient must be subscribed before iteration")
        return self._connection.__aiter__()

    async def close(self) -> None:
        connection = self._connection
        context = self._connection_context
        self._connection = None
        self._connection_context = None
        if connection is not None:
            try:
                await connection.close()
            except Exception:
                pass
        if context is not None and hasattr(context, "__aexit__"):
            try:
                await context.__aexit__(None, None, None)
            except Exception:
                pass
