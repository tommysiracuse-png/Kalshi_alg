"""Standalone millisecond order-book recorder — entry point and asyncio loops.

    .venv\\Scripts\\python -m recorder.main            # run (env-driven config)
    .venv\\Scripts\\python -m recorder.main --select-only   # print the market set, exit

Loops (one asyncio process, no threads except the REST selection worker):

* ``ConnectionRecorder.run`` x N — one websocket per shard of markets.  Each
  session subscribes to the PUBLIC sids only (1 ``orderbook_delta``,
  4 ``trade``+``ticker`` — the same wire format as ``adaptors/kalshi.py``
  ``_subscriptions_many`` minus the private groups), writes every raw message
  verbatim with its receipt time, and ends the session (close + reconnect) at
  the top of each UTC hour, when its shard changes, or when the socket drops
  (exponential backoff 1 s .. 30 s).  A reconnect always yields fresh
  ``orderbook_snapshot`` messages, which is what makes each hour file
  self-contained.
* ``_refresh_loop`` — re-selects markets every ``refresh_seconds`` and prunes
  day directories older than ``retention_days`` once per day.
* ``_heartbeat_loop`` — writes ``runtime/recorder_status.json`` every
  ``heartbeat_seconds`` and honours ``runtime/recorder.stop``.

The supervisor restarts any loop task that exits unexpectedly.

The recorder holds an *authenticated* client because Kalshi's websocket
requires signed headers even for public channels; it never calls a write
endpoint and never subscribes to ``user_orders`` / ``fill`` /
``market_positions``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import shutil
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recorder import RECORDER_VERSION  # noqa: E402
from recorder.config import RecorderConfig, load_env_files  # noqa: E402
from recorder.market_selector import reshard, select_markets  # noqa: E402
from recorder.writer import (  # noqa: E402
    HourlyGzipWriter,
    bytes_for_day,
    day_key,
    hour_dir,
    hour_key,
    next_hour_start_ms,
    prune_old_days,
    write_meta,
)

LOGGER = logging.getLogger("recorder")

# Exactly the adaptor's public groups (adaptors/kalshi.py::_subscriptions_many):
# sid 1 orderbook_delta, sid 4 trade+ticker.  Sids 2/3/5 are private and must
# never be sent by the recorder.
PUBLIC_CHANNEL_GROUPS: tuple[tuple[int, tuple[str, ...]], ...] = (
    (1, ("orderbook_delta",)),
    (4, ("trade", "ticker")),
)
PRIVATE_CHANNELS = frozenset({"user_orders", "fill", "market_positions"})

_TYPE_RE = re.compile(r'"type"\s*:\s*"([A-Za-z0-9_]+)"')
_MAX_ACKS_PER_CONNECTION = 64


def subscription_messages(markets: Sequence[str]) -> list[str]:
    """Subscribe commands in the adaptor's wire format, public sids only."""
    tickers = list(dict.fromkeys(str(m) for m in markets if str(m)))
    if not tickers:
        raise ValueError("at least one market ticker is required")
    messages: list[str] = []
    for sid, channels in PUBLIC_CHANNEL_GROUPS:
        if set(channels) & PRIVATE_CHANNELS:  # defensive: never ship a private channel
            raise RuntimeError(f"private channel in recorder subscription: {channels}")
        messages.append(
            json.dumps(
                {
                    "id": sid,
                    "cmd": "subscribe",
                    "params": {"channels": list(channels), "market_tickers": tickers},
                }
            )
        )
    return messages


def message_type(raw: str) -> str:
    """Cheap wire-type sniff (``"type"`` is the first key in Kalshi messages)."""
    match = _TYPE_RE.search(raw, 0, 96) or _TYPE_RE.search(raw)
    return match.group(1) if match else "unknown"


async def _wait_first(*awaitables: Awaitable[Any]) -> None:
    tasks = [asyncio.ensure_future(item) for item in awaitables]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


class ConnectionRecorder:
    """One websocket connection recording one shard of markets."""

    def __init__(self, index: int, recorder: "Recorder") -> None:
        self.index = int(index)
        self.recorder = recorder
        self.config = recorder.config
        self.writer = HourlyGzipWriter(
            recorder.config.record_dir, self.index, flush_seconds=recorder.config.flush_seconds
        )
        self.ws: Any = None
        self.markets: tuple[str, ...] = ()
        self._assign_event = asyncio.Event()
        self.connected = False
        self.msg_count = 0
        self.connect_count = 0
        self.session_hour: Optional[tuple[str, str]] = None
        self.last_connect_ms: Optional[int] = None
        self.last_message_ms: Optional[int] = None
        self.last_close_reason: str = ""

    def assign(self, markets: Sequence[str]) -> bool:
        """Set this connection's shard; returns True when it changed (=> resubscribe)."""
        new = tuple(str(m) for m in markets)
        if new == self.markets:
            return False
        self.markets = new
        self._assign_event.set()
        return True

    def snapshot(self) -> dict:
        return {
            "index": self.index,
            "connected": self.connected,
            "markets": len(self.markets),
            "msgs": self.msg_count,
            "connects": self.connect_count,
            "hour": "/".join(self.session_hour) if self.session_hour else None,
            "last_connect_ms": self.last_connect_ms,
            "last_message_ms": self.last_message_ms,
            "last_close_reason": self.last_close_reason,
            "file": str(self.writer.path) if self.writer.path else None,
        }

    async def run(self) -> None:
        cfg = self.config
        stop = self.recorder.stop_event
        backoff = cfg.backoff_min_seconds
        while not stop.is_set():
            markets = self.markets
            if not markets:
                await _wait_first(self._assign_event.wait(), stop.wait())
                self._assign_event.clear()
                continue
            self._assign_event.clear()
            session_start_ms = self.recorder.now_ms()
            self.session_hour = hour_key(session_start_ms)
            self.writer.open_hour(self.session_hour)
            if self.ws is None:
                self.ws = self.recorder.ws_factory(self.index)
            try:
                await self.ws.subscribe(subscription_messages(markets), headers=self.recorder.headers_provider())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.recorder.report_error(f"conn{self.index} connect failed: {exc!r}")
                await self.recorder.sleep(backoff)
                backoff = min(cfg.backoff_max_seconds, backoff * 2)
                continue
            self.connected = True
            self.connect_count += 1
            self.last_connect_ms = self.recorder.now_ms()
            backoff = cfg.backoff_min_seconds
            LOGGER.info(
                "CONN%s_SUBSCRIBED | markets=%s hour=%s/%s file=%s",
                self.index, len(markets), self.session_hour[0], self.session_hour[1], self.writer.path,
            )

            reader = asyncio.create_task(self._read(), name=f"conn{self.index}-reader")
            rotate = asyncio.create_task(
                self._sleep_until(next_hour_start_ms(session_start_ms)), name=f"conn{self.index}-rotate"
            )
            assign = asyncio.create_task(self._assign_event.wait(), name=f"conn{self.index}-assign")
            stopped = asyncio.create_task(stop.wait(), name=f"conn{self.index}-stop")
            try:
                done, _pending = await asyncio.wait(
                    {reader, rotate, assign, stopped}, return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.CancelledError:
                for task in (reader, rotate, assign, stopped):
                    task.cancel()
                await asyncio.gather(reader, rotate, assign, stopped, return_exceptions=True)
                await self._close_socket()
                raise
            if stopped in done:
                reason = "stop"
            elif reader in done:
                reason = "stream_ended"
            elif rotate in done:
                reason = "rotate"
            else:
                reason = "resubscribe"
            self.connected = False
            self.last_close_reason = reason
            for task in (rotate, assign, stopped):
                task.cancel()
            await asyncio.gather(rotate, assign, stopped, return_exceptions=True)
            await self._close_socket()
            stream_error: Optional[BaseException] = None
            try:
                await asyncio.wait_for(reader, timeout=cfg.reader_drain_seconds)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                LOGGER.warning("CONN%s_READER_DRAIN_TIMEOUT", self.index)
            except Exception as exc:  # noqa: BLE001 — surfaced as last_error below
                stream_error = exc
            self.writer.flush()
            if reason == "stream_ended":
                self.recorder.report_error(
                    f"conn{self.index} stream ended: {stream_error!r} (reconnect in {backoff:.0f}s)"
                )
                await self.recorder.sleep(backoff)
                backoff = min(cfg.backoff_max_seconds, backoff * 2)
            else:
                LOGGER.info("CONN%s_CLOSED | reason=%s msgs=%s", self.index, reason, self.msg_count)
        self.writer.close()

    async def _sleep_until(self, target_ms: int) -> None:
        """Return only once the recorder's *wall clock* has reached ``target_ms``.

        asyncio.sleep runs on the loop's monotonic clock, which can come back a
        hair before the wall clock crosses the hour; polling the same clock the
        hour key uses guarantees the next session really opens the new hour.
        Sleeps are capped so a forward clock jump is noticed within a minute.
        """
        while True:
            remaining = (int(target_ms) - self.recorder.now_ms()) / 1000.0
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 60.0))

    async def _close_socket(self) -> None:
        if self.ws is None:
            return
        try:
            await self.ws.close()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CONN%s_CLOSE_ERROR | error=%r", self.index, exc)

    async def _read(self) -> None:
        recorder = self.recorder
        writer = self.writer
        async for raw in self.ws:
            if isinstance(raw, (bytes, bytearray, memoryview)):
                raw = bytes(raw).decode("utf-8")
            recv_ms = recorder.now_ms()
            writer.write(recv_ms, raw)
            self.msg_count += 1
            self.last_message_ms = recv_ms
            recorder.on_message(self.index, recv_ms, raw)


class Recorder:
    def __init__(
        self,
        config: RecorderConfig,
        *,
        ws_factory: Callable[[int], Any],
        headers_provider: Callable[[], dict],
        select_markets: Callable[[], Sequence[str]],
        clock: Callable[[], float] = time.time,
        install_signal_handlers: bool = False,
    ) -> None:
        self.config = config
        self.ws_factory = ws_factory
        self.headers_provider = headers_provider
        self.select_markets = select_markets
        self.clock = clock
        self.install_signal_handlers = install_signal_handlers
        self.stop_event = asyncio.Event()
        self.connections = [ConnectionRecorder(index, self) for index in range(config.connection_count)]
        self.markets: list[str] = []
        self.msgs_total = 0
        self.msgs_by_type: dict[str, int] = {}
        self.msgs_per_sec = 0.0
        self.last_error: Optional[str] = None
        self.last_error_ms: Optional[int] = None
        self.started_at_ms: Optional[int] = None
        self.select_count = 0
        self.last_select_ms: Optional[int] = None
        self._last_prune_day: Optional[str] = None
        self._acks: dict[int, list[dict]] = {}
        self._acks_hour: Optional[tuple[str, str]] = None
        self._meta_write_handle: Optional[asyncio.TimerHandle] = None

    # -- small helpers -------------------------------------------------------------

    def now_ms(self) -> int:
        return int(self.clock() * 1000)

    def request_stop(self) -> None:
        self.stop_event.set()

    async def sleep(self, seconds: float) -> None:
        """Sleep, returning early when a stop is requested."""
        if seconds <= 0 or self.stop_event.is_set():
            return
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def report_error(self, message: str) -> None:
        LOGGER.warning("RECORDER_ERROR | %s", message)
        self.last_error = message[:500]
        self.last_error_ms = self.now_ms()

    # -- message accounting ----------------------------------------------------------

    def on_message(self, index: int, recv_ms: int, raw: str) -> None:
        self.msgs_total += 1
        kind = message_type(raw)
        self.msgs_by_type[kind] = self.msgs_by_type.get(kind, 0) + 1
        if kind == "subscribed":
            self._record_ack(index, recv_ms, raw)
        elif kind == "error":
            self.report_error(f"conn{index} server error: {raw[:300]}")

    def _record_ack(self, index: int, recv_ms: int, raw: str) -> None:
        key = hour_key(recv_ms)
        if key != self._acks_hour:
            self._acks = {}
            self._acks_hour = key
        try:
            data = json.loads(raw)
            payload = data.get("msg") or {}
            entry = {
                "recv_ms": recv_ms,
                "id": data.get("id"),
                "channel": payload.get("channel"),
                "sid": payload.get("sid"),
                "raw": raw,
            }
        except ValueError:
            entry = {"recv_ms": recv_ms, "raw": raw}
        acks = self._acks.setdefault(index, [])
        acks.append(entry)
        del acks[:-_MAX_ACKS_PER_CONNECTION]
        self._schedule_meta_write()

    def _schedule_meta_write(self, delay: float = 0.5) -> None:
        if self._meta_write_handle is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self.write_meta()
            return
        self._meta_write_handle = loop.call_later(delay, self._meta_timer_fired)

    def _meta_timer_fired(self) -> None:
        self._meta_write_handle = None
        try:
            self.write_meta()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("META_WRITE_FAILED | error=%r", exc)

    def write_meta(self) -> Path:
        now_ms = self.now_ms()
        key = hour_key(now_ms)
        if key != self._acks_hour:
            self._acks = {}
            self._acks_hour = key
        payload = {
            "recorder_version": RECORDER_VERSION,
            "hour_utc": f"{key[0]}/{key[1]}",
            "generated_ms": now_ms,
            "started_at_ms": self.started_at_ms,
            "markets": list(self.markets),
            "connections": {f"conn{c.index}": list(c.markets) for c in self.connections},
            "subscribe_acks": {f"conn{index}": acks for index, acks in sorted(self._acks.items())},
            "channels": {str(sid): list(channels) for sid, channels in PUBLIC_CHANNEL_GROUPS},
            "config": self.config.public_summary(),
        }
        return write_meta(hour_dir(self.config.record_dir, key), payload)

    # -- market set ------------------------------------------------------------------

    def apply_market_set(self, markets: Sequence[str]) -> list[int]:
        """Reshard and hand each connection its markets; returns the changed indices."""
        shards = reshard(
            [c.markets for c in self.connections],
            markets,
            per_connection=self.config.markets_per_connection,
            connection_count=len(self.connections),
        )
        changed = [c.index for c, shard in zip(self.connections, shards) if c.assign(shard)]
        self.markets = [ticker for shard in shards for ticker in shard]
        try:
            self.write_meta()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("META_WRITE_FAILED | error=%r", exc)
        return changed

    async def _refresh_loop(self) -> None:
        cfg = self.config
        while not self.stop_event.is_set():
            try:
                selected = await asyncio.to_thread(self.select_markets)
                markets = list(dict.fromkeys(str(m) for m in (selected or []) if str(m)))[: cfg.max_markets]
                self.select_count += 1
                self.last_select_ms = self.now_ms()
                if markets:
                    changed = self.apply_market_set(markets)
                    LOGGER.info(
                        "MARKETS_APPLIED | selected=%s recorded=%s changed_connections=%s",
                        len(markets), len(self.markets), changed,
                    )
                else:
                    self.report_error("market selection returned no markets; keeping the previous set")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.report_error(f"market selection failed: {exc!r}")
            try:
                today = day_key(self.now_ms())
                if today != self._last_prune_day:
                    removed = await asyncio.to_thread(
                        prune_old_days, cfg.record_dir, cfg.retention_days, self.now_ms()
                    )
                    self._last_prune_day = today
                    if removed:
                        LOGGER.info("RETENTION_PRUNED | removed=%s", removed)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.report_error(f"retention prune failed: {exc!r}")
            wait = cfg.refresh_seconds if self.markets else min(cfg.refresh_seconds, cfg.select_retry_seconds)
            await self.sleep(wait)

    # -- heartbeat -------------------------------------------------------------------

    def status_snapshot(self, *, final: bool = False) -> dict:
        cfg = self.config
        now_ms = self.now_ms()
        try:
            disk_free_gb = round(shutil.disk_usage(str(cfg.record_dir)).free / 1e9, 2)
        except OSError:
            disk_free_gb = None
        try:
            bytes_today = bytes_for_day(cfg.record_dir, day_key(now_ms))
        except OSError:
            bytes_today = None
        connections = [c.snapshot() for c in self.connections]
        return {
            "version": RECORDER_VERSION,
            "pid": os.getpid(),
            "state": "stopped" if final else "running",
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": now_ms,
            "hour_utc": "/".join(hour_key(now_ms)),
            "connected_connections": sum(1 for c in self.connections if c.connected),
            "connection_count": len(self.connections),
            "connections": connections,
            "markets": len(self.markets),
            "market_list": list(self.markets),
            "msgs_per_sec": round(self.msgs_per_sec, 2),
            "msgs_total": self.msgs_total,
            "msgs_by_type": dict(sorted(self.msgs_by_type.items())),
            "bytes_today": bytes_today,
            "disk_free_gb": disk_free_gb,
            "last_error": self.last_error,
            "last_error_ms": self.last_error_ms,
            "select_count": self.select_count,
            "last_select_ms": self.last_select_ms,
            "record_dir": str(cfg.record_dir),
            "config": cfg.public_summary(),
        }

    def write_status(self, *, final: bool = False) -> None:
        _atomic_write_json(self.config.status_path, self.status_snapshot(final=final))

    async def _heartbeat_loop(self) -> None:
        cfg = self.config
        previous_total = self.msgs_total
        previous_time = time.monotonic()
        while not self.stop_event.is_set():
            await self.sleep(cfg.heartbeat_seconds)
            now = time.monotonic()
            elapsed = max(1e-6, now - previous_time)
            self.msgs_per_sec = (self.msgs_total - previous_total) / elapsed
            previous_total, previous_time = self.msgs_total, now
            try:
                self.write_status()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("STATUS_WRITE_FAILED | error=%r", exc)
            if cfg.stop_path.exists():
                LOGGER.info("STOP_FILE_DETECTED | path=%s", cfg.stop_path)
                try:
                    cfg.stop_path.unlink()
                except OSError:
                    pass
                self.request_stop()

    # -- supervisor ------------------------------------------------------------------

    def _install_signal_handlers(self) -> Callable[[], None]:
        loop = asyncio.get_running_loop()
        previous: dict[int, Any] = {}

        def handler(signum, _frame):  # noqa: ANN001
            if self.stop_event.is_set():
                raise KeyboardInterrupt  # second signal: hard exit
            LOGGER.info("SIGNAL_RECEIVED | signum=%s (graceful stop)", signum)
            loop.call_soon_threadsafe(self.stop_event.set)

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                previous[signum] = signal.signal(signum, handler)
            except (ValueError, OSError):
                continue

        def restore() -> None:
            for signum, old in previous.items():
                try:
                    signal.signal(signum, old)
                except (ValueError, OSError):
                    pass

        return restore

    async def run(self) -> None:
        cfg = self.config
        cfg.record_dir.mkdir(parents=True, exist_ok=True)
        cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.started_at_ms = self.now_ms()
        restore = self._install_signal_handlers() if self.install_signal_handlers else None
        LOGGER.info("RECORDER_START | version=%s config=%s", RECORDER_VERSION, json.dumps(cfg.public_summary()))

        factories: dict[str, Callable[[], Awaitable[None]]] = {f"conn{c.index}": c.run for c in self.connections}
        factories["refresh"] = self._refresh_loop
        factories["heartbeat"] = self._heartbeat_loop
        tasks = {name: asyncio.create_task(fn(), name=name) for name, fn in factories.items()}
        stop_task = asyncio.create_task(self.stop_event.wait(), name="stop")
        try:
            while not self.stop_event.is_set():
                done, _pending = await asyncio.wait(
                    {*tasks.values(), stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_task in done:
                    break
                for name, task in list(tasks.items()):
                    if task not in done:
                        continue
                    error = None if task.cancelled() else task.exception()
                    self.report_error(f"task {name} exited unexpectedly: {error!r}; restarting")
                    await self.sleep(cfg.task_restart_seconds)
                    if self.stop_event.is_set():
                        break
                    tasks[name] = asyncio.create_task(factories[name](), name=name)
        finally:
            self.stop_event.set()
            stop_task.cancel()
            pending = [task for task in tasks.values() if not task.done()]
            if pending:
                await asyncio.wait(pending, timeout=cfg.shutdown_seconds)
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), stop_task, return_exceptions=True)
            if self._meta_write_handle is not None:
                # A debounced meta write is pending (e.g. acks of a just-started
                # hour): flush it now rather than lose it.
                self._meta_write_handle.cancel()
                self._meta_write_handle = None
                try:
                    self.write_meta()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("META_WRITE_FAILED | error=%r", exc)
            for connection in self.connections:
                connection.writer.close()
            try:
                self.write_status(final=True)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("STATUS_WRITE_FAILED | error=%r", exc)
            if restore is not None:
                restore()
            LOGGER.info("RECORDER_STOP | msgs_total=%s", self.msgs_total)


# -- process entry point -------------------------------------------------------------------


def configure_logging(config: RecorderConfig) -> None:
    config.runtime_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s")
    file_handler = RotatingFileHandler(config.log_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    root.handlers[:] = [file_handler, stream_handler]
    logging.getLogger("websockets").setLevel(logging.WARNING)


def build_client(config: RecorderConfig):
    """Authenticated adaptor client (websocket headers must be signed even for
    public channels — ``public_only`` cannot open the stream)."""
    from adaptors.kalshi import KalshiApiClient, KalshiClientConfig

    load_env_files()
    api_key_id = os.environ.get("KALSHI_API_KEY_ID", "").strip()
    private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "").strip()
    if not api_key_id or not private_key_path or not Path(private_key_path).exists():
        raise SystemExit(
            "recorder needs KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH "
            "(env or ~/.config/kalshi-alg/bot.env): the websocket requires signed headers"
        )
    return KalshiApiClient(
        KalshiClientConfig(
            api_key_id=api_key_id,
            private_key_path=private_key_path,
            enable_shared_write_rate_limiter=False,
            use_demo_environment=config.use_demo,
            websocket_url=config.websocket_url,
        )
    )


def build_recorder(config: RecorderConfig, client) -> Recorder:
    from clients.websocket_client import WebsocketClient

    return Recorder(
        config,
        ws_factory=lambda _index: WebsocketClient(client.websocket_url),
        headers_provider=client.websocket_headers,
        select_markets=lambda: select_markets(client, config),
        install_signal_handlers=True,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Standalone ms order-book recorder (public channels only).")
    parser.add_argument("--select-only", action="store_true", help="print the market selection as JSON and exit")
    args = parser.parse_args(argv)

    config = RecorderConfig.from_env()
    configure_logging(config)
    client = build_client(config)

    if args.select_only:
        markets = select_markets(client, config)
        shards = reshard([], markets, per_connection=config.markets_per_connection, connection_count=config.connection_count)
        print(json.dumps({"markets": markets, "shards": shards, "config": config.public_summary()}, indent=2))
        return 0

    config.pid_path.parent.mkdir(parents=True, exist_ok=True)
    config.pid_path.write_text(str(os.getpid()), encoding="ascii")
    try:
        config.stop_path.unlink()
    except OSError:
        pass
    recorder = build_recorder(config, client)
    exit_code = 0
    try:
        asyncio.run(recorder.run())
    except KeyboardInterrupt:
        LOGGER.info("RECORDER_INTERRUPTED")
        exit_code = 130
    finally:
        try:
            if config.pid_path.read_text(encoding="ascii").strip() == str(os.getpid()):
                config.pid_path.unlink()
        except OSError:
            pass
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
