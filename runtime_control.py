"""Shared runtime status and local control-socket primitives.

The socket is intentionally Unix-only and permissioned to the service account.
It is not an authentication boundary; callers must already be trusted local
processes (the loopback operations API in production).
"""

from __future__ import annotations

import asyncio
import json
import errno
import os
import queue
import socket
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional


STATUS_SCHEMA_VERSION = 4
ALLOWED_ACTIONS = {"status", "refresh", "disable_ticker", "enable_ticker"}


class BotControlServer:
    """Async Unix socket used by BotManager to inspect and stop one bot."""

    def __init__(
        self,
        socket_path: Path,
        status_provider: Callable[[], Dict[str, Any]],
        shutdown_handler: Callable[[], Awaitable[Optional[Dict[str, Any]]]],
    ) -> None:
        self.socket_path = socket_path
        self.status_provider = status_provider
        self.shutdown_handler = shutdown_handler
        self._server: Optional[asyncio.AbstractServer] = None
        self._shutdown_result: Optional[Dict[str, Any]] = None

    async def start(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass
        self._server = await asyncio.start_unix_server(self._handle, path=str(self.socket_path))
        os.chmod(self.socket_path, 0o600)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5)
            if len(raw) > 65_536:
                raise ValueError("request is too large")
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("request must be a JSON object")
            request_id = str(payload.get("request_id") or "").strip()
            action = str(payload.get("action") or "").strip()
            if not request_id or len(request_id) > 128:
                raise ValueError("request_id is required and must be at most 128 characters")
            if action == "status":
                response = {"ok": True, "request_id": request_id, "result": self.status_provider()}
            elif action == "shutdown":
                if self._shutdown_result is None:
                    result = await self.shutdown_handler()
                    self._shutdown_result = dict(result or {"shutdownRequested": True})
                response = {"ok": True, "request_id": request_id, "result": self._shutdown_result}
            else:
                raise ValueError(f"unsupported action: {action}")
        except Exception as exc:
            response = {"ok": False, "code": "invalid_request", "message": str(exc)}
        writer.write((json.dumps(response, separators=(",", ":")) + "\n").encode("utf-8"))
        try:
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


def send_bot_command(socket_path: Path, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
    return send_control_command(socket_path, payload, timeout=timeout)


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=False)
        handle.write("\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def read_json(path: Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else dict(default or {})
    except (OSError, ValueError, TypeError):
        return dict(default or {})


@dataclass
class ControlRequest:
    request_id: str
    action: str
    ticker: Optional[str] = None
    received_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    done: threading.Event = field(default_factory=threading.Event)
    result: Dict[str, Any] = field(default_factory=dict)


class ControlServer:
    """Small newline-delimited JSON server that hands work to the main loop."""

    def __init__(
        self,
        socket_path: Path,
        requests: "queue.Queue[ControlRequest]",
        status_provider: Callable[[], Dict[str, Any]],
    ) -> None:
        self.socket_path = socket_path
        self.requests = requests
        self.status_provider = status_provider
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._server: Optional[socket.socket] = None
        self._completed: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(self.socket_path))
        except Exception:
            server.close()
            raise
        os.chmod(self.socket_path, 0o600)
        server.listen(8)
        server.settimeout(0.5)
        self._server = server
        self._thread = threading.Thread(target=self._serve, name="launcher-control", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._server is not None:
            try:
                self._server.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2)
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass

    def _serve(self) -> None:
        assert self._server is not None
        while not self._stop.is_set():
            try:
                connection, _ = self._server.accept()
            except (socket.timeout, OSError):
                continue
            with connection:
                connection.settimeout(5)
                try:
                    raw = b""
                    while b"\n" not in raw and len(raw) <= 65536:
                        chunk = connection.recv(4096)
                        if not chunk:
                            break
                        raw += chunk
                    payload = json.loads(raw.split(b"\n", 1)[0].decode("utf-8"))
                    response = self._handle(payload)
                except Exception as exc:
                    response = {"ok": False, "code": "invalid_request", "message": str(exc)}
                connection.sendall((json.dumps(response, separators=(",", ":")) + "\n").encode("utf-8"))

    def _handle(self, payload: object) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("request must be a JSON object")
        request_id = str(payload.get("request_id") or "").strip()
        action = str(payload.get("action") or "").strip()
        ticker_value = payload.get("ticker")
        ticker = str(ticker_value).strip() if ticker_value not in (None, "") else None
        if not request_id or len(request_id) > 128:
            raise ValueError("request_id is required and must be at most 128 characters")
        if action not in ALLOWED_ACTIONS:
            raise ValueError(f"unsupported action: {action}")
        if action in {"disable_ticker", "enable_ticker"} and not ticker:
            raise ValueError("ticker is required for this action")
        if action == "status":
            return {"ok": True, "request_id": request_id, "result": self.status_provider()}

        with self._lock:
            previous = self._completed.get(request_id)
        if previous is not None:
            return previous

        item = ControlRequest(request_id=request_id, action=action, ticker=ticker)
        self.requests.put(item)
        if not item.done.wait(timeout=120):
            return {
                "ok": False,
                "request_id": request_id,
                "code": "control_timeout",
                "message": "launcher did not finish the command within 120 seconds",
            }
        response = {"ok": bool(item.result.get("ok")), "request_id": request_id, **item.result}
        with self._lock:
            self._completed[request_id] = response
            if len(self._completed) > 256:
                self._completed.pop(next(iter(self._completed)))
        return response


def send_control_command(socket_path: Path, payload: Dict[str, Any], timeout: float = 125.0) -> Dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(socket_path))
        client.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
        raw = b""
        while b"\n" not in raw and len(raw) <= 1_048_576:
            chunk = client.recv(65536)
            if not chunk:
                break
            raw += chunk
    if not raw:
        raise RuntimeError("launcher returned an empty response")
    response = json.loads(raw.split(b"\n", 1)[0].decode("utf-8"))
    if not isinstance(response, dict):
        raise RuntimeError("launcher returned a non-object response")
    return response
