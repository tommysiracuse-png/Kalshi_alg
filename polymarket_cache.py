"""Persistent Polymarket catalog and compact top-of-book cache.

The cache is intentionally independent of the screener so it can be shared by
the launcher, a websocket ingestion task, and deterministic tests.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional

from clients.models import Market


@dataclass(frozen=True)
class BookTop:
    bid_units: Optional[int]
    bid_size_units: Optional[int]
    ask_units: Optional[int]
    ask_size_units: Optional[int]
    timestamp_ms: int = 0
    book_hash: str = ""
    stale: bool = False


class PolymarketCatalogStore:
    """SQLite-backed normalized market catalog.

    One connection is opened per operation. SQLite WAL keeps catalog refreshes
    from blocking readers and also makes this safe across launcher restarts.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS markets (
                    market_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    series_id TEXT NOT NULL DEFAULT '',
                    event_id TEXT NOT NULL DEFAULT '',
                    close_time_ms INTEGER,
                    tick_units INTEGER NOT NULL DEFAULT 100,
                    volume_24h_units INTEGER,
                    open_interest_units INTEGER,
                    yes_token_id TEXT NOT NULL,
                    no_token_id TEXT NOT NULL,
                    min_order_size_units INTEGER NOT NULL DEFAULT 0,
                    market_url TEXT,
                    series_title TEXT NOT NULL DEFAULT '',
                    updated_at_ms INTEGER NOT NULL
                )"""
            )
            db.execute("CREATE INDEX IF NOT EXISTS markets_status_idx ON markets(status)")
            db.execute("CREATE INDEX IF NOT EXISTS markets_updated_idx ON markets(updated_at_ms)")

    def upsert(self, markets: Iterable[Market], *, updated_at_ms: Optional[int] = None) -> int:
        stamp = int(updated_at_ms or time.time() * 1000)
        rows = [
            (
                m.market_id, m.title, m.status, m.series_id, m.event_id,
                m.close_time_ms, m.legacy_tick_size_units,
                m.volume_24h_units, m.open_interest_units,
                m.yes_token_id, m.no_token_id,
                0 if m.min_order_size_units is None else m.min_order_size_units,
                m.market_url, m.series_title, stamp,
            )
            for m in markets
            if m.market_id and m.yes_token_id and m.no_token_id
        ]
        if not rows:
            return 0
        with self._lock, self._connect() as db:
            db.executemany(
                """INSERT INTO markets(
                    market_id,title,status,series_id,event_id,close_time_ms,
                    tick_units,volume_24h_units,open_interest_units,yes_token_id,
                    no_token_id,min_order_size_units,market_url,series_title,updated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(market_id) DO UPDATE SET
                    title=excluded.title,status=excluded.status,series_id=excluded.series_id,
                    event_id=excluded.event_id,close_time_ms=excluded.close_time_ms,
                    tick_units=excluded.tick_units,volume_24h_units=excluded.volume_24h_units,
                    open_interest_units=excluded.open_interest_units,yes_token_id=excluded.yes_token_id,
                    no_token_id=excluded.no_token_id,min_order_size_units=excluded.min_order_size_units,
                    market_url=excluded.market_url,series_title=excluded.series_title,
                    updated_at_ms=excluded.updated_at_ms""",
                rows,
            )
            return len(rows)

    @staticmethod
    def _market(row: sqlite3.Row) -> Market:
        return Market(
            market_id=str(row["market_id"]), title=str(row["title"] or ""),
            status=str(row["status"] or ""), series_id=str(row["series_id"] or ""),
            event_id=str(row["event_id"] or ""), close_time_ms=row["close_time_ms"],
            price_level_structure="decimal", fractional_trading_enabled=True,
            legacy_tick_size_units=int(row["tick_units"] or 100),
            volume_24h_units=row["volume_24h_units"], open_interest_units=row["open_interest_units"],
            market_url=row["market_url"], series_title=str(row["series_title"] or ""),
            venue="polymarket", native_market_id=str(row["market_id"]),
            yes_token_id=str(row["yes_token_id"]), no_token_id=str(row["no_token_id"]),
            min_order_size_units=(
                None if row["min_order_size_units"] in (None, 0) else int(row["min_order_size_units"])
            ),
            market_rules="direct_token_books",
        )

    def list_markets(self, *, status: str = "", limit: int = 0) -> list[Market]:
        sql = "SELECT * FROM markets"
        params: list[object] = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY market_id"
        if limit > 0:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self._lock, self._connect() as db:
            return [self._market(row) for row in db.execute(sql, params).fetchall()]

    def count(self) -> int:
        with self._lock, self._connect() as db:
            return int(db.execute("SELECT COUNT(*) FROM markets").fetchone()[0])


class PolymarketBookCache:
    """Thread-safe compact top-of-book records keyed by CLOB asset ID."""

    def __init__(self, path: str | Path | None = None) -> None:
        self._books: dict[str, BookTop] = {}
        self._lock = threading.RLock()
        self.path = Path(path) if path else None
        self.expected_assets = 0
        self.websocket_messages = 0
        self.stale_assets = 0
        self.missing_assets = 0
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        if self.path is None:
            raise RuntimeError("book cache persistence is disabled")
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS books (
                    asset_id TEXT PRIMARY KEY,
                    bid_units INTEGER,
                    bid_size_units INTEGER,
                    ask_units INTEGER,
                    ask_size_units INTEGER,
                    timestamp_ms INTEGER NOT NULL,
                    book_hash TEXT NOT NULL DEFAULT '',
                    stale INTEGER NOT NULL DEFAULT 0
                )"""
            )
            for row in db.execute("SELECT * FROM books"):
                self._books[str(row["asset_id"])] = BookTop(
                    row["bid_units"], row["bid_size_units"],
                    row["ask_units"], row["ask_size_units"],
                    int(row["timestamp_ms"] or 0), str(row["book_hash"] or ""),
                    bool(row["stale"]),
                )
            self.stale_assets = sum(1 for book in self._books.values() if book.stale)

    def persist(self) -> None:
        """Persist the current compact snapshot in one bounded transaction."""
        if self.path is None:
            return
        with self._lock:
            rows = [
                (asset, book.bid_units, book.bid_size_units, book.ask_units,
                 book.ask_size_units, book.timestamp_ms, book.book_hash, int(book.stale))
                for asset, book in self._books.items()
            ]
        with self._lock, self._connect() as db:
            db.executemany(
                """INSERT INTO books(asset_id,bid_units,bid_size_units,ask_units,ask_size_units,
                    timestamp_ms,book_hash,stale) VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(asset_id) DO UPDATE SET bid_units=excluded.bid_units,
                    bid_size_units=excluded.bid_size_units,ask_units=excluded.ask_units,
                    ask_size_units=excluded.ask_size_units,timestamp_ms=excluded.timestamp_ms,
                    book_hash=excluded.book_hash,stale=excluded.stale""",
                rows,
            )

    def ready_count(self) -> int:
        with self._lock:
            return sum(
                1 for book in self._books.values()
                if not book.stale and book.bid_units is not None and book.ask_units is not None
            )

    @staticmethod
    def _price(value: object) -> Optional[int]:
        try:
            return int(round(float(value) * 10_000))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _size(value: object) -> Optional[int]:
        try:
            return int(round(float(value) * 100))
        except (TypeError, ValueError):
            return None

    def update_book(self, asset_id: str, payload: Mapping[str, object], *, timestamp_ms: int = 0) -> BookTop:
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        bid_rows = [(self._price(x.get("price")), self._size(x.get("size"))) for x in bids if isinstance(x, Mapping)]
        ask_rows = [(self._price(x.get("price")), self._size(x.get("size"))) for x in asks if isinstance(x, Mapping)]
        bid_rows = [(p, s) for p, s in bid_rows if p is not None and s is not None]
        ask_rows = [(p, s) for p, s in ask_rows if p is not None and s is not None]
        top = BookTop(
            max(bid_rows, default=(None, None), key=lambda item: item[0] or -1)[0],
            max(bid_rows, default=(None, None), key=lambda item: item[0] or -1)[1],
            min(ask_rows, default=(None, None), key=lambda item: item[0] or 10**12)[0],
            min(ask_rows, default=(None, None), key=lambda item: item[0] or 10**12)[1],
            int(timestamp_ms or time.time() * 1000), str(payload.get("hash") or ""),
            stale=False,
        )
        with self._lock:
            previous = self._books.get(str(asset_id))
            if previous is not None and previous.stale:
                self.stale_assets = max(0, self.stale_assets - 1)
            self._books[str(asset_id)] = top
            self.websocket_messages += 1
        return top

    def mark_stale(self, asset_id: str) -> None:
        with self._lock:
            previous = self._books.get(str(asset_id))
            if previous is None or not previous.stale:
                self.stale_assets += 1
            if previous is None:
                self._books[str(asset_id)] = BookTop(None, None, None, None, stale=True)
            else:
                self._books[str(asset_id)] = BookTop(**{**previous.__dict__, "stale": True})

    def update_price_change(self, asset_id: str, payload: Mapping[str, object], *, timestamp_ms: int = 0) -> bool:
        """Apply a CLOB price-change event when it includes new best levels."""
        with self._lock:
            previous = self._books.get(str(asset_id))
            if previous is None:
                self.mark_stale(asset_id)
                return False
            bid = self._price(payload.get("best_bid"))
            ask = self._price(payload.get("best_ask"))
            if bid is None and ask is None:
                self.mark_stale(asset_id)
                return False
            price = self._price(payload.get("price"))
            size = self._size(payload.get("size"))
            side = str(payload.get("side") or "").upper()
            bid_size = previous.bid_size_units
            ask_size = previous.ask_size_units
            if side in {"BUY", "BID"} and size is not None and bid is not None and price == bid:
                bid_size = size
            if side in {"SELL", "ASK"} and size is not None and ask is not None and price == ask:
                ask_size = size
            if bid is not None and bid != previous.bid_units and not (
                side in {"BUY", "BID"} and size is not None and price == bid
            ):
                self.mark_stale(asset_id)
                return False
            if ask is not None and ask != previous.ask_units and not (
                side in {"SELL", "ASK"} and size is not None and price == ask
            ):
                self.mark_stale(asset_id)
                return False
            self._books[str(asset_id)] = BookTop(
                bid if bid is not None else previous.bid_units,
                bid_size,
                ask if ask is not None else previous.ask_units,
                ask_size,
                int(timestamp_ms or time.time() * 1000), previous.book_hash, False,
            )
            self.websocket_messages += 1
            return True

    def get(self, asset_id: str) -> Optional[BookTop]:
        with self._lock:
            value = self._books.get(str(asset_id))
            if value is None or value.stale:
                return None
            return value

    def snapshot(self) -> dict[str, BookTop]:
        with self._lock:
            return dict(self._books)

    def close(self) -> None:
        # Connections are opened per bounded persistence operation. This method
        # is provided for lifecycle symmetry and future backends.
        return None
