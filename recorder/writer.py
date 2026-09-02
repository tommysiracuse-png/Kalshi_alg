"""Hourly-rotated gzip JSONL sink for raw websocket messages, plus retention.

Layout (all times UTC):

    <record_dir>/YYYYMMDD/HH/conn<k>.jsonl.gz   one line per message:
        {"recv_ms": <epoch ms at receipt>, "raw": "<verbatim wire JSON string>"}
    <record_dir>/YYYYMMDD/HH/meta.json          market list, connection map, acks

The writer never decides *when* to rotate — the connection loop does, because a
rotation must coincide with a reconnect so every hour file starts with fresh
``orderbook_snapshot`` messages.  ``open_hour`` is that explicit rotation point;
``write`` appends to whatever file is open (opening the message's own hour only
when nothing is open yet).

Files are opened in append mode, so a restart inside an hour adds a second gzip
member to the same file (Python's gzip reader handles multi-member files
transparently).  Data is sync-flushed every ``flush_seconds`` so a hard kill
loses at most that window; ``iter_records`` tolerates the missing trailer.
"""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Iterator, Optional, Tuple

HOUR_MS = 3_600_000
DAY_MS = 86_400_000
DAY_DIR_RE = re.compile(r"^\d{8}$")

HourKey = Tuple[str, str]  # ("YYYYMMDD", "HH") in UTC


def hour_key(ms: int) -> HourKey:
    moment = datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    return moment.strftime("%Y%m%d"), moment.strftime("%H")


def day_key(ms: int) -> str:
    return hour_key(ms)[0]


def hour_start_ms(ms: int) -> int:
    return int(ms) - int(ms) % HOUR_MS


def next_hour_start_ms(ms: int) -> int:
    return hour_start_ms(ms) + HOUR_MS


def hour_dir(root: Path, key: HourKey) -> Path:
    return Path(root) / key[0] / key[1]


def connection_file(root: Path, key: HourKey, connection_index: int) -> Path:
    return hour_dir(root, key) / f"conn{int(connection_index)}.jsonl.gz"


def encode_line(recv_ms: int, raw: str) -> str:
    # json.dumps escapes quotes/backslashes/newlines inside ``raw`` and
    # json.loads restores them exactly, so the round trip is byte-identical.
    return json.dumps({"recv_ms": int(recv_ms), "raw": raw}, separators=(",", ":"), ensure_ascii=False) + "\n"


def iter_records(path: Path) -> Iterator[dict]:
    """Yield the decoded records of one file, tolerating a truncated final member."""
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        try:
            for line in handle:
                line = line.rstrip("\n")
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except ValueError:
                    # A partial trailing line can only exist after a hard kill.
                    continue
        except EOFError:
            # Killed between a sync flush and the gzip trailer: everything up to
            # the last flush was already yielded.
            return


class HourlyGzipWriter:
    def __init__(
        self,
        root: Path,
        connection_index: int,
        *,
        flush_seconds: float = 2.0,
        compresslevel: int = 6,
        monotonic=time.monotonic,
    ) -> None:
        self.root = Path(root)
        self.connection_index = int(connection_index)
        self.flush_seconds = float(flush_seconds)
        self.compresslevel = int(compresslevel)
        self._monotonic = monotonic
        self._handle: Optional[IO[str]] = None
        self._last_flush = self._monotonic()
        self.current_key: Optional[HourKey] = None
        self.path: Optional[Path] = None
        self.lines_written = 0
        self.uncompressed_bytes = 0
        self.rotations = 0

    # -- lifecycle -----------------------------------------------------------------

    def open_hour(self, key: HourKey) -> Path:
        """Make ``key`` the current hour file (no-op when it already is)."""
        key = (str(key[0]), str(key[1]))
        if self._handle is not None and key == self.current_key:
            assert self.path is not None
            return self.path
        self.close()
        path = connection_file(self.root, key, self.connection_index)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = gzip.open(path, "at", encoding="utf-8", compresslevel=self.compresslevel)
        self.current_key = key
        self.path = path
        self.rotations += 1
        self._last_flush = self._monotonic()
        return path

    def write(self, recv_ms: int, raw: str) -> None:
        if self._handle is None:
            self.open_hour(hour_key(recv_ms))
        assert self._handle is not None
        line = encode_line(recv_ms, raw)
        self._handle.write(line)
        self.lines_written += 1
        self.uncompressed_bytes += len(line)
        if self.flush_seconds <= 0 or (self._monotonic() - self._last_flush) >= self.flush_seconds:
            self.flush()

    def flush(self) -> None:
        if self._handle is not None:
            # TextIOWrapper.flush -> GzipFile.flush(Z_SYNC_FLUSH): compressed
            # bytes reach the OS file, readable even if we die right after.
            self._handle.flush()
        self._last_flush = self._monotonic()

    def close(self) -> None:
        handle, self._handle = self._handle, None
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        self.current_key = None
        self.path = None

    @property
    def is_open(self) -> bool:
        return self._handle is not None


# -- per-hour metadata ----------------------------------------------------------------


def write_meta(directory: Path, payload: dict) -> Path:
    """Atomically (re)write ``meta.json`` for one hour directory."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "meta.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)
    return path


# -- retention / accounting ------------------------------------------------------------


def prune_old_days(root: Path, retention_days: int, now_ms: int) -> list[str]:
    """Delete ``YYYYMMDD`` day directories older than ``retention_days``.

    Only directories whose name is exactly eight digits are candidates; anything
    else under the record root is left alone.  Returns the removed names.
    """
    root = Path(root)
    if retention_days <= 0 or not root.exists():
        return []
    cutoff = day_key(int(now_ms) - int(retention_days) * DAY_MS)
    removed: list[str] = []
    for entry in sorted(root.iterdir()):
        if entry.is_dir() and DAY_DIR_RE.match(entry.name) and entry.name < cutoff:
            shutil.rmtree(entry, ignore_errors=True)
            removed.append(entry.name)
    return removed


def bytes_for_day(root: Path, day: str) -> int:
    """Total on-disk bytes under ``<root>/<day>`` (0 when absent)."""
    day_dir = Path(root) / day
    if not day_dir.exists():
        return 0
    total = 0
    for dirpath, _dirnames, filenames in os.walk(day_dir):
        for name in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                continue
    return total
