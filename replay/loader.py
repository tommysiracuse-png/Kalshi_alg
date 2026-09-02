"""Event loaders for the replay driver.

Tier 1 (``load_events``): synthesizes a venue event stream from candles +
trades in ``history.sqlite3``. Each 1-minute candle (ts_ms = period END)
becomes four sub-steps at the period start + :00/:15/:30/:45 walking
Open -> High -> Low -> Close of the recorded yes bid/ask columns (falling
back to price_* +/- one 1-cent tick when the bid/ask columns are NULL).
Every sub-step emits an OrderBookSnapshot (top-of-book only, assumed depth)
plus a TickerUpdate. Recorded trades are emitted as PublicTrade events at
their real timestamps. The stream is time-ordered and always begins with an
OrderBookSnapshot.

Tier 2 (``load_tier2_events``): replays RAW recorded websocket messages from
``record_data/YYYYMMDD/HH/conn<k>.jsonl.gz`` (one ``{"recv_ms", "raw"}``
object per line) through ``adaptors.kalshi.parse_wire_event`` -- the exact
parser the production client uses -- demuxed to one ticker. ``recv_ms`` is
the replay clock; an event's own wire timestamp is kept when present and
sane, otherwise it is stamped with the clock. Kalshi's ``seq`` is per
subscription (shared by every market on the connection), so continuity is
verified per (connection file, sid) over all book messages; a gap emits a
``StreamReset`` for the ticker and suppresses its deltas until the next
``orderbook_snapshot`` re-syncs the book. A window that starts mid-hour is
booted with a synthetic snapshot of the ladder reconstructed up to
``start_ms``, so every segment still begins with a full book.
"""

from __future__ import annotations

import gzip
import json
import re
import sqlite3
from collections import OrderedDict
from dataclasses import dataclass, replace as dataclass_replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from clients.models import (
    OrderBookDelta,
    OrderBookSnapshot,
    PublicTrade,
    StreamReset,
    TickerUpdate,
)

PRICE_SCALE = 10_000
TICK_UNITS = 100  # standard binary market: 1-cent tick
SUB_STEP_OFFSETS_MS = (0, 15_000, 30_000, 45_000)  # O, H, L, C
# Sort priority for same-timestamp events: book first, then ticker, trades last.
PRIORITY_SNAPSHOT = 0
PRIORITY_TICKER = 1
PRIORITY_TRADE = 2
PRIORITY_RESET = 0
PRIORITY_DELTA = 0


def open_history(path: str) -> sqlite3.Connection:
    """Open the history DB read-only; a writer may be appending concurrently."""
    uri = Path(path).resolve().as_uri() + "?mode=ro&immutable=0"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def load_market(connection: sqlite3.Connection, ticker: str) -> Optional[dict]:
    row = connection.execute(
        "SELECT ticker, series, title, status, close_ts_ms, result, "
        "volume_24h_units, oi_units, meta_json FROM markets WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    return dict(row) if row is not None else None


def _clamp_quote(bid: int, ask: int) -> Tuple[int, int]:
    bid = max(TICK_UNITS, min(int(bid), PRICE_SCALE - 2 * TICK_UNITS))
    ask = max(int(ask), bid + TICK_UNITS)
    ask = min(ask, PRICE_SCALE - TICK_UNITS)
    if bid >= ask:
        bid = ask - TICK_UNITS
    return bid, ask


def _phase_values(row: sqlite3.Row, prefix: str) -> Tuple[Optional[int], ...]:
    return tuple(
        row[f"{prefix}_{phase}_units"] for phase in ("open", "high", "low", "close")
    )


def load_events(
    connection: sqlite3.Connection,
    ticker: str,
    start_ms: int,
    end_ms: int,
    assumed_depth_units: int,
) -> List[Tuple[int, int, object]]:
    """Merged, time-ordered (ts_ms, priority, event) list for [start_ms, end_ms)."""
    events: List[Tuple[int, int, int, object]] = []  # + seq for stable sort
    sequence = 0

    candles = connection.execute(
        "SELECT * FROM candles WHERE ticker = ? AND ts_ms > ? AND ts_ms <= ? "
        "ORDER BY ts_ms",
        (ticker, int(start_ms), int(end_ms) + 60_000),
    ).fetchall()

    for candle in candles:
        period_start_ms = int(candle["ts_ms"]) - 60_000
        bid_path = _phase_values(candle, "yes_bid")
        ask_path = _phase_values(candle, "yes_ask")
        price_path = _phase_values(candle, "price")
        volume_units = candle["volume_units"]
        oi_units = candle["oi_units"]

        for offset_ms, bid, ask, price in zip(
            SUB_STEP_OFFSETS_MS, bid_path, ask_path, price_path
        ):
            ts_ms = period_start_ms + offset_ms
            if ts_ms < int(start_ms) or ts_ms >= int(end_ms):
                continue
            if bid is None or ask is None:
                if price is None:
                    continue  # nothing usable for this sub-step
                bid = int(price) - TICK_UNITS
                ask = int(price) + TICK_UNITS
            bid, ask = _clamp_quote(int(bid), int(ask))
            sequence += 1
            # Two levels per side (best + one tick behind) so the actor's
            # wide_book_gap guard sees a populated book, not a one-level shell.
            yes_levels = {bid: int(assumed_depth_units)}
            if bid - TICK_UNITS >= TICK_UNITS:
                yes_levels[bid - TICK_UNITS] = int(assumed_depth_units)
            # NO side stores NO-buy prices: a yes ask A rests as a NO bid
            # at PRICE_SCALE - A (implied_yes_ask == PRICE_SCALE - best_no_bid).
            best_no_bid = PRICE_SCALE - ask
            no_levels = {best_no_bid: int(assumed_depth_units)}
            if best_no_bid - TICK_UNITS >= TICK_UNITS:
                no_levels[best_no_bid - TICK_UNITS] = int(assumed_depth_units)
            snapshot = OrderBookSnapshot(
                market_id=ticker,
                sequence=sequence,
                yes_levels=yes_levels,
                no_levels=no_levels,
            )
            events.append((ts_ms, PRIORITY_SNAPSHOT, sequence, snapshot))
            sequence += 1
            ticker_update = TickerUpdate(
                market_id=ticker,
                timestamp_ms=ts_ms,
                price_units=(int(price) if price is not None else (bid + ask) // 2),
                yes_bid_units=bid,
                yes_ask_units=ask,
                volume_units=(int(volume_units) if volume_units is not None else None),
                open_interest_units=(int(oi_units) if oi_units is not None else None),
                yes_bid_size_units=int(assumed_depth_units),
                yes_ask_size_units=int(assumed_depth_units),
            )
            events.append((ts_ms, PRIORITY_TICKER, sequence, ticker_update))

    trades = connection.execute(
        "SELECT * FROM trades WHERE ticker = ? AND ts_ms >= ? AND ts_ms < ? "
        "ORDER BY ts_ms, trade_id",
        (ticker, int(start_ms), int(end_ms)),
    ).fetchall()
    for trade in trades:
        yes_price_units = trade["yes_price_units"]
        count_units = trade["count_units"]
        if yes_price_units is None or count_units is None or int(count_units) <= 0:
            continue
        no_price_units = trade["no_price_units"]
        if no_price_units is None:
            no_price_units = PRICE_SCALE - int(yes_price_units)
        sequence += 1
        events.append(
            (
                int(trade["ts_ms"]),
                PRIORITY_TRADE,
                sequence,
                PublicTrade(
                    market_id=ticker,
                    trade_id=str(trade["trade_id"]),
                    timestamp_ms=int(trade["ts_ms"]),
                    yes_price_units=int(yes_price_units),
                    no_price_units=int(no_price_units),
                    count_units=int(count_units),
                    taker_side=str(trade["taker_side"] or ""),
                ),
            )
        )

    events.sort(key=lambda item: (item[0], item[1], item[2]))

    # The actor requires a ready book before anything else matters: drop any
    # leading events that precede the first snapshot.
    first_snapshot_index = next(
        (
            index
            for index, item in enumerate(events)
            if isinstance(item[3], OrderBookSnapshot)
        ),
        None,
    )
    if first_snapshot_index is None:
        return []
    return [(ts, priority, event) for ts, priority, _seq, event in events[first_snapshot_index:]]


# ----------------------------------------------------------------------
# Tier 2: raw recorded websocket messages
# ----------------------------------------------------------------------

HOUR_MS = 3_600_000
# Wire timestamps further than this from recv_ms are treated as unusable
# (clock skew / wrong epoch) and replaced by the replay clock.
MAX_WIRE_SKEW_MS = 60_000
TIER2_CACHE_SIZE = 8
BOOK_MESSAGE_TYPES = ("orderbook_snapshot", "orderbook_delta")

_RECORD_FILE_RE = re.compile(r"^conn(\d+)\.jsonl(\.gz)?$")
_RAW_TYPE_RE = re.compile(r'"type"\s*:\s*"([^"]*)"')
_RAW_SID_RE = re.compile(r'"sid"\s*:\s*(-?\d+)')
_RAW_SEQ_RE = re.compile(r'"seq"\s*:\s*(-?\d+)')
_RAW_TICKER_RE = re.compile(r'"(?:market_ticker|ticker)"\s*:\s*"([^"]*)"')
# Fast-path patterns applied to the undecoded record LINE (the wire message
# is a JSON-escaped string there, so quotes may carry a backslash).
_LINE_RECV_RE = re.compile(r'"recv_ms"\s*:\s*(-?\d+)')
_LINE_BOOK_TYPE_RE = re.compile(r'\\?"type\\?"\s*:\s*\\?"(orderbook_snapshot|orderbook_delta)')
_LINE_SID_RE = re.compile(r'\\?"sid\\?"\s*:\s*(-?\d+)')
_LINE_SEQ_RE = re.compile(r'\\?"seq\\?"\s*:\s*(-?\d+)')
_LINE_TICKER_RE = re.compile(r'\\?"(?:market_ticker|ticker)\\?"\s*:\s*\\?"([^"\\]*)')

_TIER2_CACHE: "OrderedDict[Tuple[str, str, int, int], Tuple[list, list]]" = OrderedDict()


@dataclass(frozen=True)
class RecordFile:
    path: Path
    day: str  # YYYYMMDD
    hour: str  # HH
    conn: int
    hour_start_ms: int

    @property
    def hour_end_ms(self) -> int:
        return self.hour_start_ms + HOUR_MS


def _hour_start_ms(day: str, hour: str) -> int:
    moment = datetime(int(day[0:4]), int(day[4:6]), int(day[6:8]), int(hour), tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def list_record_files(record_root: str) -> List[RecordFile]:
    """All ``YYYYMMDD/HH/conn<k>.jsonl[.gz]`` files under the root, in time order."""
    root = Path(record_root)
    if not root.is_dir():
        return []
    files: List[RecordFile] = []
    for day_dir in root.iterdir():
        if not (day_dir.is_dir() and day_dir.name.isdigit() and len(day_dir.name) == 8):
            continue
        for hour_dir in day_dir.iterdir():
            if not (hour_dir.is_dir() and hour_dir.name.isdigit() and len(hour_dir.name) == 2):
                continue
            try:
                hour_start = _hour_start_ms(day_dir.name, hour_dir.name)
            except ValueError:
                continue
            for item in hour_dir.iterdir():
                match = _RECORD_FILE_RE.match(item.name)
                if match is None or not item.is_file():
                    continue
                files.append(RecordFile(item, day_dir.name, hour_dir.name, int(match.group(1)), hour_start))
    files.sort(key=lambda f: (f.day, f.hour, f.conn))
    return files


def _iter_record_lines(path: Path, warnings: List[str], truncated: Optional[set] = None) -> Iterator[Tuple[int, str]]:
    """Yield (line_no, line); tolerates a truncated tail (file still being written)."""
    opener = gzip.open if path.suffix == ".gz" else open
    line_no = 0
    try:
        with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                line_no += 1
                line = line.strip()
                if line:
                    yield line_no, line
    except (EOFError, OSError, gzip.BadGzipFile) as exc:
        warnings.append(f"{path.name}: truncated after line {line_no} ({type(exc).__name__})")
        if truncated is not None:
            truncated.add(path)


# Per-process owner index: (record_root, day, hour) -> {ticker: conn}. Built as
# a side effect of any full scan of a completed hour (coverage discovery or a
# ticker load whose connection was unknown) so later loads read one file.
_OWNER_INDEX: Dict[Tuple[str, str, str], Dict[str, int]] = {}


def _root_key(record_root: str) -> str:
    return str(Path(record_root).resolve())


def _hour_completed(record: RecordFile) -> bool:
    """True once the wall clock is safely past the hour (files no longer growing)."""
    import time

    return time.time() * 1000 > record.hour_end_ms + 120_000


def _remember_owner_index(root_key: str, hour_key: Tuple[str, str], owners: Dict[str, int],
                          hour_files: List[RecordFile], truncated: set) -> None:
    if not hour_files or any(f.path in truncated for f in hour_files):
        return
    if not all(_hour_completed(f) for f in hour_files):
        return
    _OWNER_INDEX[(root_key, hour_key[0], hour_key[1])] = dict(owners)


def _parse_record_line(line: str) -> Tuple[Optional[int], object]:
    """Returns (recv_ms, raw) where raw is the wire message as str or dict."""
    outer = json.loads(line)
    if not isinstance(outer, dict):
        raise ValueError("record line is not an object")
    recv = outer.get("recv_ms")
    raw = outer.get("raw")
    if raw is None:
        raw = outer.get("message")
    if raw is None:
        raise ValueError("record line has no raw message")
    return (int(recv) if recv is not None else None), raw


def _raw_fields(raw: object) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[str]]:
    """(type, sid, seq, ticker) from the raw message without a full parse when possible."""
    if isinstance(raw, dict):
        payload = raw.get("msg") or {}
        ticker = payload.get("market_ticker") or payload.get("ticker")
        seq = raw.get("seq")
        sid = raw.get("sid")
        return (
            raw.get("type"),
            int(sid) if sid is not None else None,
            int(seq) if seq is not None else None,
            str(ticker) if ticker is not None else None,
        )
    text = str(raw)
    type_match = _RAW_TYPE_RE.search(text)
    sid_match = _RAW_SID_RE.search(text)
    seq_match = _RAW_SEQ_RE.search(text)
    ticker_match = _RAW_TICKER_RE.search(text)
    return (
        type_match.group(1) if type_match else None,
        int(sid_match.group(1)) if sid_match else None,
        int(seq_match.group(1)) if seq_match else None,
        ticker_match.group(1) if ticker_match else None,
    )


def _line_book_fields(line: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """(book message type, sid, seq) straight from the undecoded line; type None if not a book message."""
    type_match = _LINE_BOOK_TYPE_RE.search(line)
    if type_match is None:
        return None, None, None
    sid_match = _LINE_SID_RE.search(line)
    seq_match = _LINE_SEQ_RE.search(line)
    return (
        type_match.group(1),
        int(sid_match.group(1)) if sid_match else None,
        int(seq_match.group(1)) if seq_match else None,
    )


def _line_recv_ms(line: str) -> Optional[int]:
    match = _LINE_RECV_RE.search(line)
    return int(match.group(1)) if match else None


_META_CONN_RE = re.compile(r"^conn(\d+)$")


def meta_connection_for(hour_dir: Path, ticker: str) -> Optional[int]:
    """Connection index that carried ``ticker`` this hour per the recorder's
    ``meta.json`` (``{"connections": {"conn0": [tickers...], ...}}``); None
    when the file is missing/malformed or the ticker is not listed exactly once,
    in which case every connection file of the hour is scanned."""
    try:
        with open(hour_dir / "meta.json", "r", encoding="utf-8") as handle:
            meta = json.load(handle)
    except (OSError, ValueError):
        return None
    connections = meta.get("connections") if isinstance(meta, dict) else None
    if not isinstance(connections, dict):
        return None
    owners = []
    for name, tickers in connections.items():
        match = _META_CONN_RE.match(str(name))
        if match and isinstance(tickers, (list, tuple)) and ticker in tickers:
            owners.append(int(match.group(1)))
    return owners[0] if len(owners) == 1 else None


def available_tier2_windows(record_root: str) -> Dict[str, Tuple[int, int, int]]:
    """{ticker: (min_recv_ms, max_recv_ms, message_count)} across the whole recording."""
    out: Dict[str, List[int]] = {}
    warnings: List[str] = []
    truncated: set = set()
    root_key = _root_key(record_root)
    files = list_record_files(record_root)
    hours: Dict[Tuple[str, str], List[RecordFile]] = {}
    for record in files:
        hours.setdefault((record.day, record.hour), []).append(record)
    for hour_key, hour_files in hours.items():
        owners: Dict[str, int] = {}
        for record in hour_files:
            for _line_no, line in _iter_record_lines(record.path, warnings, truncated):
                ticker_match = _LINE_TICKER_RE.search(line)
                if ticker_match is not None:
                    ticker: Optional[str] = ticker_match.group(1)
                    recv = _line_recv_ms(line)
                else:  # unusual layout: decode properly
                    try:
                        recv, raw = _parse_record_line(line)
                    except (ValueError, TypeError):
                        continue
                    _type, _sid, _seq, ticker = _raw_fields(raw)
                if not ticker:
                    continue
                owners.setdefault(ticker, record.conn)
                ts = recv if recv is not None else record.hour_start_ms
                stats = out.get(ticker)
                if stats is None:
                    out[ticker] = [ts, ts, 1]
                else:
                    if ts < stats[0]:
                        stats[0] = ts
                    if ts > stats[1]:
                        stats[1] = ts
                    stats[2] += 1
        _remember_owner_index(root_key, hour_key, owners, hour_files, truncated)
    return {ticker: (stats[0], stats[1], stats[2]) for ticker, stats in sorted(out.items())}


def _payload_has_timestamp(payload: dict) -> bool:
    return payload.get("ts") not in (None, "") or payload.get("time") not in (None, "")


def _normalize_timestamp(event, payload: dict, clock_ms: int, warnings: List[str], state: dict):
    """Keep the wire timestamp when present and within skew; else stamp the clock."""
    if not isinstance(event, (OrderBookDelta, PublicTrade, TickerUpdate)):
        return event
    if _payload_has_timestamp(payload) and abs(int(event.timestamp_ms) - clock_ms) <= MAX_WIRE_SKEW_MS:
        return event
    if _payload_has_timestamp(payload) and not state.get("skew_warned"):
        state["skew_warned"] = True
        warnings.append(
            f"wire timestamp differs from recv_ms by more than {MAX_WIRE_SKEW_MS} ms; using recv_ms"
        )
    return dataclass_replace(event, timestamp_ms=clock_ms)


def clear_tier2_cache() -> None:
    _TIER2_CACHE.clear()
    _OWNER_INDEX.clear()


def load_tier2_events(
    record_root: str,
    ticker: str,
    start_ms: int,
    end_ms: int,
) -> Tuple[List[Tuple[int, int, object]], List[str]]:
    """Time-ordered ``(ts_ms, priority, event)`` list for one ticker plus warnings.

    See the module docstring for the clock, sequence and boot rules. Results
    are cached per process (``TIER2_CACHE_SIZE`` most recent windows) because
    the optimizer replays the same window for many candidates; the returned
    event list must be treated as read-only.
    """
    key = (str(Path(record_root).resolve()), str(ticker), int(start_ms), int(end_ms))
    cached = _TIER2_CACHE.get(key)
    if cached is not None:
        _TIER2_CACHE.move_to_end(key)
        return cached[0], list(cached[1])
    events, warnings = _load_tier2_events_uncached(record_root, ticker, int(start_ms), int(end_ms))
    _TIER2_CACHE[key] = (events, warnings)
    while len(_TIER2_CACHE) > TIER2_CACHE_SIZE:
        _TIER2_CACHE.popitem(last=False)
    return events, list(warnings)


def _load_tier2_events_uncached(
    record_root: str, ticker: str, start_ms: int, end_ms: int
) -> Tuple[List[Tuple[int, int, object]], List[str]]:
    from adaptors.kalshi import parse_wire_event
    from replay.fill_sim import FullBook

    warnings: List[str] = []
    events: List[Tuple[int, int, object]] = []
    files = [
        record for record in list_record_files(record_root)
        if record.hour_start_ms < end_ms and record.hour_end_ms > start_ms
    ]
    if not files:
        warnings.append(f"no record files overlap [{start_ms}, {end_ms})")
        return events, warnings

    book = FullBook()
    awaiting_snapshot = True  # no usable book for the ticker yet (or after a gap)
    booted = False  # first in-window snapshot emitted
    gap_count = 0
    skipped_deltas = 0
    out_of_order = 0
    last_ts: Optional[int] = None
    owner_conn: Dict[Tuple[str, str], int] = {}  # (day, hour) -> conn carrying the ticker
    duplicate_conn_warned: set = set()
    state: dict = {}
    stop = False

    def emit(ts: int, priority: int, event) -> None:
        nonlocal last_ts, out_of_order
        if last_ts is not None and ts < last_ts:
            out_of_order += 1
            ts = last_ts
        last_ts = ts
        events.append((ts, priority, event))

    def check_sequence(seq_state: Dict[int, int], record: RecordFile, hour_key: Tuple[str, str],
                       line_no: int, sid: Optional[int], seq: int, recv: Optional[int]) -> None:
        """Per-(connection, sid) continuity; a gap only matters on the ticker's own connection."""
        nonlocal awaiting_snapshot, gap_count
        sid_key = sid if sid is not None else -1
        previous = seq_state.get(sid_key)
        seq_state[sid_key] = seq
        if previous is None or seq == previous + 1:
            return
        if owner_conn.get(hour_key) != record.conn:
            return  # another connection's reconnect: this ticker's feed is intact
        gap_count += 1
        warnings.append(
            f"{record.day}/{record.hour}/{record.path.name}:{line_no}: sequence gap "
            f"sid={sid} {previous}->{seq}"
        )
        if not awaiting_snapshot:
            awaiting_snapshot = True
            book.clear()
            if booted and recv is not None and start_ms <= recv < end_ms:
                emit(recv, PRIORITY_RESET, StreamReset(ticker))

    # Which connection carried the ticker each hour: the recorder's meta.json
    # (current market set only) or this process's owner index from an earlier
    # full scan. Known -> read that file alone; unknown -> scan every file of
    # the hour and index the tickers seen for later loads.
    root_key = _root_key(record_root)
    hours: Dict[Tuple[str, str], List[RecordFile]] = {}
    for record in files:
        hours.setdefault((record.day, record.hour), []).append(record)
    mapped_conn: Dict[Tuple[str, str], Optional[int]] = {}
    absent_hours: set = set()
    for hour_key, hour_files in hours.items():
        mapped = meta_connection_for(hour_files[0].path.parent, ticker)
        if mapped is None:
            indexed = _OWNER_INDEX.get((root_key, hour_key[0], hour_key[1]))
            if indexed is not None:
                mapped = indexed.get(ticker)
                if mapped is None:
                    absent_hours.add(hour_key)  # fully indexed hour without this ticker
        mapped_conn[hour_key] = mapped
    truncated: set = set()
    hour_owners: Dict[Tuple[str, str], Dict[str, int]] = {}
    hours_fully_scanned: set = set()

    for record in files:
        if stop:
            break
        hour_key = (record.day, record.hour)
        if hour_key in absent_hours:
            continue
        mapped = mapped_conn.get(hour_key)
        if mapped is not None:
            if record.conn != mapped:
                continue
            owner_conn.setdefault(hour_key, mapped)
        owners = hour_owners.setdefault(hour_key, {})
        indexing = mapped is None  # full scan: learn every ticker's connection
        seq_state: Dict[int, int] = {}  # sid -> last seq on this connection file
        parse_failures = 0
        for line_no, line in _iter_record_lines(record.path, warnings, truncated):
            if ticker not in line:
                # Fast path (the vast majority of lines): not ours, so only the
                # book-sequence bookkeeping matters; no JSON decode needed.
                if indexing:
                    ticker_match = _LINE_TICKER_RE.search(line)
                    if ticker_match is not None:
                        owners.setdefault(ticker_match.group(1), record.conn)
                msg_type, sid, seq = _line_book_fields(line)
                if msg_type is not None and seq is not None:
                    check_sequence(seq_state, record, hour_key, line_no, sid, seq, _line_recv_ms(line))
                continue
            try:
                recv, raw = _parse_record_line(line)
            except (ValueError, TypeError):
                parse_failures += 1
                continue
            msg_type, sid, seq, msg_ticker = _raw_fields(raw)
            if msg_ticker:
                owners.setdefault(msg_ticker, record.conn)
            if msg_ticker == ticker:
                owner_conn.setdefault(hour_key, record.conn)
            if msg_type in BOOK_MESSAGE_TYPES and seq is not None:
                check_sequence(seq_state, record, hour_key, line_no, sid, seq, recv)

            if msg_ticker != ticker:
                continue
            owner = owner_conn[hour_key]
            if owner != record.conn:
                if hour_key not in duplicate_conn_warned:
                    duplicate_conn_warned.add(hour_key)
                    warnings.append(
                        f"{record.day}/{record.hour}: {ticker} also on conn{record.conn}; "
                        f"using conn{owner} only"
                    )
                continue

            data = raw if isinstance(raw, dict) else json.loads(raw)
            event = parse_wire_event(data, ticker)
            if event is None or event.market_id != ticker:
                continue
            payload = data.get("msg") or {}
            if recv is not None:
                ts = recv
            elif hasattr(event, "timestamp_ms") and _payload_has_timestamp(payload):
                ts = int(event.timestamp_ms)
            else:
                ts = last_ts if last_ts is not None else record.hour_start_ms
            event = _normalize_timestamp(event, payload, ts, warnings, state)

            if isinstance(event, OrderBookSnapshot):
                book.apply_snapshot(event)
                awaiting_snapshot = False
                if ts >= end_ms:
                    stop = True
                    break
                if ts < start_ms:
                    continue
                booted = True
                emit(ts, PRIORITY_SNAPSHOT, event)
                continue

            if ts >= end_ms:
                stop = True
                break

            if isinstance(event, OrderBookDelta):
                if awaiting_snapshot:
                    skipped_deltas += 1
                    continue
                if ts < start_ms:
                    book.apply_delta(event)
                    continue
                if not booted:
                    # Window starts mid-hour: boot with the reconstructed ladder.
                    emit(
                        start_ms,
                        PRIORITY_SNAPSHOT,
                        OrderBookSnapshot(ticker, book.sequence, dict(book.yes_levels), dict(book.no_levels)),
                    )
                    booted = True
                book.apply_delta(event)
                emit(ts, PRIORITY_DELTA, event)
                continue

            # Trades / tickers: only inside the window and after the book booted.
            if ts < start_ms:
                continue
            if not booted:
                if awaiting_snapshot or not book.ready:
                    continue
                emit(
                    start_ms,
                    PRIORITY_SNAPSHOT,
                    OrderBookSnapshot(ticker, book.sequence, dict(book.yes_levels), dict(book.no_levels)),
                )
                booted = True
            priority = PRIORITY_TRADE if isinstance(event, PublicTrade) else PRIORITY_TICKER
            emit(ts, priority, event)
        if parse_failures:
            warnings.append(f"{record.day}/{record.hour}/{record.path.name}: {parse_failures} unparseable line(s)")
        if indexing and not stop and record is hours[hour_key][-1]:
            hours_fully_scanned.add(hour_key)

    for hour_key in hours_fully_scanned:
        _remember_owner_index(root_key, hour_key, hour_owners.get(hour_key, {}), hours[hour_key], truncated)

    if gap_count:
        warnings.append(f"{gap_count} sequence gap(s); {skipped_deltas} delta(s) dropped awaiting re-sync")
    if out_of_order:
        warnings.append(f"{out_of_order} event(s) arrived out of order; clamped to the clock")
    if not events:
        warnings.append(f"no replayable events for {ticker!r} in [{start_ms}, {end_ms})")
    return events, warnings
