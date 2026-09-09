"""Continuously refreshed local Polymarket catalog and top-of-book mirror.

The mirror is deliberately independent from the screener.  A child process
owns network I/O and writes complete generations to SQLite; readers only use a
generation after its readiness marker has been published.
"""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence
from urllib.parse import quote

from clients.models import Market, MarketQuery
from clients.factory import build_client
from clients.monitoring import merge_activity_snapshots
from polymarket_cache import BookTop


LOGGER = logging.getLogger(__name__)
MIRROR_SCHEMA_VERSION = 1

# These fields describe the last SQLite publication.  The child status file
# also carries transient lifecycle fields (``reason``, ``pid``, etc.) while a
# catalog refresh is in flight.  A transient heartbeat must not replace a
# usable published snapshot with ``complete=False`` or erase its generation.
MIRROR_SNAPSHOT_FIELDS = frozenset({
    "schemaVersion", "generationId", "capturedAtMs", "catalogCount",
    "bookReadyMarkets", "availableMarketCount", "bookMissingMarkets",
    "bookCoverage", "bookStaleAssets", "bookMaxAgeMs", "bookLatestAgeMs",
    "bookRevision", "screenable", "partial", "complete",
})


def merge_mirror_status(published: Mapping[str, Any], child: Mapping[str, Any]) -> dict[str, Any]:
    """Merge child lifecycle state without clobbering a published snapshot."""

    merged = dict(published)
    for key, value in child.items():
        if key in MIRROR_SNAPSHOT_FIELDS and key in published:
            continue
        merged[key] = value
    return merged


class MirrorNotReadyError(RuntimeError):
    """Raised when a required mirror cannot provide a fresh full snapshot."""


@dataclass(frozen=True)
class MirrorConfig:
    enabled: bool = False
    required_complete_snapshot: bool = False
    snapshot_max_age_seconds: float = 60.0
    book_stale_after_seconds: float = 60.0
    ws_shards: int = 1
    book_recovery_parallelism: int = 16
    catalog_page_size: int = 100
    sync_interval_seconds: float = 30.0
    bootstrap_timeout_seconds: float = 45.0
    max_markets: int = 0


def _now_ms() -> int:
    return int(time.time() * 1000)


class PolymarketMirrorStore:
    """WAL-backed mirror store with an atomic active-generation marker."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        if read_only:
            # Readers must not try to change the connection's journal mode.
            # The mirror publishes books continuously; opening a normal
            # connection and executing PRAGMA journal_mode=WAL can wait behind
            # the writer transaction and leave a screener refresh wedged.
            uri = f"file:{quote(str(self.path.resolve()), safe='/')}?mode=ro"
            db = sqlite3.connect(uri, uri=True, timeout=1.0)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            return db
        db = sqlite3.connect(self.path, timeout=30.0)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    def _initialize(self) -> None:
        # The launcher foreground client opens the store while the mirror
        # child may already be publishing books. A schema read is safe in WAL
        # mode; replaying CREATE/ALTER statements on every client construction
        # needlessly asks SQLite for a writer lock and can stall screening.
        if self.path.exists():
            try:
                with self._connect(read_only=True) as db:
                    tables = {
                        str(row[0]) for row in db.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        ).fetchall()
                    }
                    columns = db.execute("PRAGMA table_info(mirror_markets)").fetchall()
                    indexes = {
                        str(row[0]) for row in db.execute(
                            "SELECT name FROM sqlite_master WHERE type='index'"
                        ).fetchall()
                    }
                    generation_keyed = any(
                        str(row["name"]) == "generation_id" and int(row["pk"] or 0) == 2
                        for row in columns
                    )
                    if (
                        {"mirror_meta", "mirror_markets", "mirror_books"}.issubset(tables)
                        and generation_keyed
                        and "mirror_books_asset_fresh_idx" in indexes
                    ):
                        return
            except (OSError, sqlite3.Error):
                # Fall through to the normal initialization/migration path.
                pass
        initialize_wal = not self.path.exists()
        with self._connect() as db:
            # Set WAL once when creating the store. Repeating this PRAGMA on
            # every reader connection can itself wait for the continuous book
            # writer and is unnecessary once the database is in WAL mode.
            if initialize_wal:
                db.execute("PRAGMA journal_mode=WAL")
            existing = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mirror_markets'").fetchone()
            if existing:
                columns = db.execute("PRAGMA table_info(mirror_markets)").fetchall()
                # Early development snapshots used market_id as the sole key,
                # which could overwrite the last good generation mid-refresh.
                # Move those rows aside before creating the generation-keyed
                # table so upgrades remain safe and idempotent.
                if columns and not any(str(row["name"]) == "generation_id" and int(row["pk"] or 0) == 2 for row in columns):
                    db.execute("ALTER TABLE mirror_markets RENAME TO mirror_markets_legacy")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS mirror_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mirror_markets (
                    market_id TEXT NOT NULL,
                    generation_id TEXT NOT NULL,
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
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(market_id, generation_id)
                );
                CREATE INDEX IF NOT EXISTS mirror_markets_generation_idx
                    ON mirror_markets(generation_id, status, market_id);
                CREATE INDEX IF NOT EXISTS mirror_markets_cutoff_idx
                    ON mirror_markets(status, close_time_ms);
                CREATE TABLE IF NOT EXISTS mirror_books (
                    asset_id TEXT PRIMARY KEY,
                    bid_units INTEGER,
                    bid_size_units INTEGER,
                    ask_units INTEGER,
                    ask_size_units INTEGER,
                    timestamp_ms INTEGER NOT NULL DEFAULT 0,
                    stale INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS mirror_books_fresh_idx
                    ON mirror_books(stale, timestamp_ms);
                CREATE INDEX IF NOT EXISTS mirror_books_asset_fresh_idx
                    ON mirror_books(asset_id, stale, timestamp_ms);
                """
            )
            db.execute(
                "INSERT OR IGNORE INTO mirror_meta(key,value) VALUES('schema_version',?)",
                (str(MIRROR_SCHEMA_VERSION),),
            )
            db.execute(
                "INSERT OR IGNORE INTO mirror_meta(key,value) VALUES('book_revision','0')"
            )
            legacy = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mirror_markets_legacy'").fetchone()
            if legacy:
                db.execute(
                    """INSERT OR IGNORE INTO mirror_markets(
                        market_id,generation_id,title,status,series_id,event_id,close_time_ms,
                        tick_units,volume_24h_units,open_interest_units,yes_token_id,no_token_id,
                        min_order_size_units,market_url,series_title,updated_at_ms
                    ) SELECT market_id,'legacy',title,status,series_id,event_id,close_time_ms,
                        tick_units,volume_24h_units,open_interest_units,yes_token_id,no_token_id,
                        min_order_size_units,market_url,series_title,updated_at_ms
                    FROM mirror_markets_legacy"""
                )
                db.execute("DROP TABLE mirror_markets_legacy")

    def upsert_markets(self, markets: Iterable[Market], generation_id: str) -> int:
        stamp = _now_ms()
        rows = [
            (
                m.market_id, generation_id, m.title, m.status, m.series_id, m.event_id,
                m.close_time_ms, int(m.legacy_tick_size_units or 100),
                m.volume_24h_units, m.open_interest_units, m.yes_token_id or "",
                m.no_token_id or "", int(m.min_order_size_units or 0), m.market_url,
                m.series_title, stamp,
            )
            for m in markets
            if m.market_id and m.yes_token_id and m.no_token_id
        ]
        if not rows:
            return 0
        with self._connect() as db:
            db.executemany(
                """INSERT INTO mirror_markets(
                    market_id,generation_id,title,status,series_id,event_id,close_time_ms,
                    tick_units,volume_24h_units,open_interest_units,yes_token_id,no_token_id,
                    min_order_size_units,market_url,series_title,updated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(market_id,generation_id) DO UPDATE SET
                    title=excluded.title,status=excluded.status,
                    series_id=excluded.series_id,event_id=excluded.event_id,close_time_ms=excluded.close_time_ms,
                    tick_units=excluded.tick_units,volume_24h_units=excluded.volume_24h_units,
                    open_interest_units=excluded.open_interest_units,yes_token_id=excluded.yes_token_id,
                    no_token_id=excluded.no_token_id,min_order_size_units=excluded.min_order_size_units,
                    market_url=excluded.market_url,series_title=excluded.series_title,
                    updated_at_ms=excluded.updated_at_ms""",
                rows,
            )
        return len(rows)

    def active_generation(self) -> str:
        """Return the generation currently exposed to mirror readers."""

        with self._connect(read_only=True) as db:
            row = db.execute(
                "SELECT value FROM mirror_meta WHERE key='active_generation'"
            ).fetchone()
        return str(row[0] or "") if row else ""

    @staticmethod
    def _market_from_row(row: sqlite3.Row) -> Market:
        return Market(
            str(row["market_id"]),
            title=str(row["title"] or ""),
            status=str(row["status"] or ""),
            series_id=str(row["series_id"] or ""),
            event_id=str(row["event_id"] or ""),
            close_time_ms=row["close_time_ms"],
            legacy_tick_size_units=int(row["tick_units"] or 100),
            volume_24h_units=row["volume_24h_units"],
            open_interest_units=row["open_interest_units"],
            market_url=row["market_url"],
            series_title=str(row["series_title"] or ""),
            venue="polymarket",
            native_market_id=str(row["market_id"]),
            yes_token_id=str(row["yes_token_id"] or ""),
            no_token_id=str(row["no_token_id"] or ""),
            min_order_size_units=int(row["min_order_size_units"] or 0),
            market_rules="direct_token_books",
        )

    def active_markets(self, *, limit: int = 0) -> list[Market]:
        """Load the last published active catalog for stream continuity."""

        generation = self.active_generation()
        if not generation:
            return []
        query = """
            SELECT * FROM mirror_markets
            WHERE generation_id=? AND status='active'
            ORDER BY market_id
        """
        params: list[Any] = [generation]
        if int(limit) > 0:
            query += " LIMIT ?"
            params.append(int(limit))
        with self._connect(read_only=True) as db:
            rows = db.execute(query, params).fetchall()
        return [self._market_from_row(row) for row in rows]

    def upsert_books(self, books: Mapping[str, BookTop]) -> int:
        if not books:
            return 0
        stamp = _now_ms()
        rows = [
            (
                str(asset), book.bid_units, book.bid_size_units, book.ask_units,
                book.ask_size_units, int(book.timestamp_ms or stamp), int(book.stale), stamp,
            )
            for asset, book in books.items()
        ]
        with self._connect() as db:
            # The websocket cache contains the whole subscribed universe, but
            # most heartbeats contain only a handful of changed books. Avoid
            # rewriting every row on every heartbeat: long bulk write
            # transactions block the screener's joined book query and make a
            # mirror-ready refresh appear to hang.
            existing: dict[str, tuple[Any, ...]] = {}
            asset_ids = [row[0] for row in rows]
            for offset in range(0, len(asset_ids), 900):
                chunk = asset_ids[offset:offset + 900]
                placeholders = ",".join("?" for _ in chunk)
                for current in db.execute(
                    f"SELECT asset_id,bid_units,bid_size_units,ask_units,ask_size_units,timestamp_ms,stale "
                    f"FROM mirror_books WHERE asset_id IN ({placeholders})",
                    chunk,
                ).fetchall():
                    existing[str(current[0])] = tuple(current[1:])
            changed_rows = [
                row for row in rows
                if existing.get(str(row[0])) != tuple(row[1:7])
            ]
            if not changed_rows:
                return 0
            db.executemany(
                """INSERT INTO mirror_books(
                    asset_id,bid_units,bid_size_units,ask_units,ask_size_units,
                    timestamp_ms,stale,updated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    bid_units=excluded.bid_units,bid_size_units=excluded.bid_size_units,
                    ask_units=excluded.ask_units,ask_size_units=excluded.ask_size_units,
                    timestamp_ms=excluded.timestamp_ms,stale=excluded.stale,
                    updated_at_ms=excluded.updated_at_ms""",
                changed_rows,
            )
            current_revision = db.execute(
                "SELECT value FROM mirror_meta WHERE key='book_revision'"
            ).fetchone()
            revision = int(current_revision[0] or 0) if current_revision else 0
            db.execute(
                "INSERT INTO mirror_meta(key,value) VALUES('book_revision',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(revision + 1),),
            )
        return len(changed_rows)

    def republish_active(self, *, max_age_seconds: float) -> dict[str, Any]:
        """Refresh readiness for the active generation from current book rows.

        Websocket updates can arrive while a replacement catalog is being
        paged.  Republishing the active generation makes those updates visible
        to readers without waiting for the catalog task to finish.
        """

        generation = self.active_generation()
        if not generation:
            return self.status()
        with self._connect() as db:
            row = db.execute(
                "SELECT COUNT(*) FROM mirror_markets WHERE generation_id=? AND status='active'",
                (generation,),
            ).fetchone()
        catalog_count = int(row[0] or 0) if row else 0
        if not catalog_count:
            return self.status()
        snapshot = self.status()
        catalog_complete = bool(
            str(snapshot.get("generationId") or "") == generation
            and snapshot.get("complete")
        )
        return self.publish(
            generation,
            catalog_count=catalog_count,
            max_age_seconds=max_age_seconds,
            catalog_complete=catalog_complete,
        )

    def publish(self, generation_id: str, *, catalog_count: int, max_age_seconds: float, catalog_complete: bool = True) -> dict[str, Any]:
        now = _now_ms()
        with self._connect() as db:
            row = db.execute(
                """SELECT COUNT(*) AS total,
                    SUM(CASE WHEN y.asset_id IS NOT NULL AND n.asset_id IS NOT NULL
                        AND y.stale=0 AND n.stale=0
                        AND y.bid_units IS NOT NULL AND y.ask_units IS NOT NULL
                        AND n.bid_units IS NOT NULL AND n.ask_units IS NOT NULL
                        AND y.timestamp_ms >= ? AND n.timestamp_ms >= ? THEN 1 ELSE 0 END) AS ready
                    FROM mirror_markets m
                    LEFT JOIN mirror_books y ON y.asset_id=m.yes_token_id
                    LEFT JOIN mirror_books n ON n.asset_id=m.no_token_id
                    WHERE m.generation_id=? AND m.status='active'""",
                (now - int(max_age_seconds * 1000), now - int(max_age_seconds * 1000), generation_id),
            ).fetchone()
            total = int(row["total"] or 0)
            ready = int(row["ready"] or 0)
            complete = bool(catalog_complete and total and ready == total and total == int(catalog_count))
            # A generation is useful to the screener as soon as at least one
            # active market has both sides of a fresh top-of-book.  ``complete``
            # remains the stronger all-catalog readiness marker used by
            # operators and cold-start diagnostics.
            screenable = bool(ready)
            book_stats = db.execute(
                """
                SELECT
                    MIN(b.timestamp_ms) AS oldest_timestamp_ms,
                    MAX(b.timestamp_ms) AS newest_timestamp_ms,
                    SUM(CASE WHEN b.stale=1
                              OR b.bid_units IS NULL OR b.ask_units IS NULL
                              OR b.timestamp_ms < ? THEN 1 ELSE 0 END) AS stale_assets
                FROM mirror_books b
                JOIN (
                    SELECT yes_token_id AS asset_id
                    FROM mirror_markets
                    WHERE generation_id=? AND status='active'
                    UNION
                    SELECT no_token_id AS asset_id
                    FROM mirror_markets
                    WHERE generation_id=? AND status='active'
                ) assets ON assets.asset_id=b.asset_id
                """,
                (now - int(max_age_seconds * 1000), generation_id, generation_id),
            ).fetchone()
            oldest_timestamp = int(book_stats["oldest_timestamp_ms"] or 0)
            newest_timestamp = int(book_stats["newest_timestamp_ms"] or 0)
            max_book_age = max(0, now - oldest_timestamp) if oldest_timestamp else 0
            latest_book_age = max(0, now - newest_timestamp) if newest_timestamp else 0
            stale_assets = int(book_stats["stale_assets"] or 0)
            revision_row = db.execute(
                "SELECT value FROM mirror_meta WHERE key='book_revision'"
            ).fetchone()
            book_revision = int(revision_row[0] or 0) if revision_row else 0
            status = {
                "schemaVersion": MIRROR_SCHEMA_VERSION,
                "generationId": generation_id,
                "capturedAtMs": now,
                "catalogCount": int(catalog_count),
                "bookReadyMarkets": ready,
                "availableMarketCount": ready,
                "bookMissingMarkets": max(0, total - ready),
                "bookCoverage": (ready / total) if total else 0.0,
                "bookStaleAssets": stale_assets,
                "bookMaxAgeMs": int(max_book_age or 0),
                "bookLatestAgeMs": int(latest_book_age or 0),
                "bookRevision": book_revision,
                "screenable": screenable,
                "partial": bool(screenable and not complete),
                "complete": complete,
            }
            db.execute(
                "INSERT INTO mirror_meta(key,value) VALUES('active_generation',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (generation_id,),
            )
            db.execute(
                "INSERT INTO mirror_meta(key,value) VALUES('status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (json.dumps(status, separators=(",", ":")),),
            )
            return status

    def status(self) -> dict[str, Any]:
        try:
            with self._connect(read_only=True) as db:
                row = db.execute("SELECT value FROM mirror_meta WHERE key='status'").fetchone()
            return json.loads(row[0]) if row else {"complete": False, "reason": "no_snapshot"}
        except (OSError, sqlite3.Error, json.JSONDecodeError):
            return {"complete": False, "reason": "store_unavailable"}

    @staticmethod
    def _snapshot_is_readable(snapshot: Mapping[str, Any], *, allow_partial: bool) -> bool:
        return bool(snapshot.get("complete") or (allow_partial and snapshot.get("screenable")))

    def iter_payloads(self, *, status: str, limit: int, max_age_seconds: float, allow_partial: bool = False) -> Iterator[dict[str, Any]]:
        snapshot = self.status()
        captured = int(snapshot.get("capturedAtMs") or 0)
        cutoff = _now_ms() - int(max_age_seconds * 1000)
        if not self._snapshot_is_readable(snapshot, allow_partial=allow_partial) or _now_ms() - captured > int(max_age_seconds * 1000):
            raise MirrorNotReadyError(
                f"Polymarket mirror is not ready (complete={snapshot.get('complete')}, "
                f"ageMs={max(0, _now_ms() - captured) if captured else None})"
            )
        with self._connect(read_only=True) as db:
            rows = db.execute(
                """SELECT m.*, y.bid_units AS y_bid, y.bid_size_units AS y_bid_size,
                    y.ask_units AS y_ask, y.ask_size_units AS y_ask_size, y.timestamp_ms AS y_ts,
                    n.bid_units AS n_bid, n.bid_size_units AS n_bid_size,
                    n.ask_units AS n_ask, n.ask_size_units AS n_ask_size, n.timestamp_ms AS n_ts
                    FROM mirror_markets AS m
                    CROSS JOIN mirror_books AS y INDEXED BY mirror_books_asset_fresh_idx
                        ON y.asset_id=m.yes_token_id AND y.stale=0 AND y.timestamp_ms>=?
                    CROSS JOIN mirror_books AS n INDEXED BY mirror_books_asset_fresh_idx
                        ON n.asset_id=m.no_token_id AND n.stale=0 AND n.timestamp_ms>=?
                    WHERE m.generation_id=? AND (?='' OR m.status=?)
                    ORDER BY m.market_id LIMIT ?""",
                (cutoff, cutoff, str(snapshot["generationId"]), status, status, max(0, int(limit)) or -1),
            ).fetchall()
            for row in rows:
                yield self._payload(row)

    def vectorized_rows(self, *, status: str, limit: int, max_age_seconds: float, settings: Mapping[str, Any], toxic_tickers: set[str] = frozenset(), toxic_series: set[str] = frozenset(), allow_partial: bool = False) -> tuple[list[dict[str, Any]], int]:
        """Screen the numeric mirror columns in batch and materialize only top rows."""
        import numpy as np

        snapshot = self.status()
        captured = int(snapshot.get("capturedAtMs") or 0)
        cutoff = _now_ms() - int(max_age_seconds * 1000)
        if not self._snapshot_is_readable(snapshot, allow_partial=allow_partial) or _now_ms() - captured > int(max_age_seconds * 1000):
            raise MirrorNotReadyError(f"Polymarket mirror is not ready: {snapshot}")
        with self._connect(read_only=True) as db:
            rows = db.execute(
                """SELECT m.*, y.bid_units AS y_bid, y.bid_size_units AS y_bid_size,
                    y.ask_units AS y_ask, y.ask_size_units AS y_ask_size, y.timestamp_ms AS y_ts,
                    n.bid_units AS n_bid, n.bid_size_units AS n_bid_size,
                    n.ask_units AS n_ask, n.ask_size_units AS n_ask_size, n.timestamp_ms AS n_ts
                    FROM mirror_markets AS m
                    CROSS JOIN mirror_books AS y INDEXED BY mirror_books_asset_fresh_idx
                        ON y.asset_id=m.yes_token_id AND y.stale=0 AND y.timestamp_ms>=?
                    CROSS JOIN mirror_books AS n INDEXED BY mirror_books_asset_fresh_idx
                        ON n.asset_id=m.no_token_id AND n.stale=0 AND n.timestamp_ms>=?
                    WHERE m.generation_id=? AND (?='' OR m.status=?)
                    ORDER BY m.market_id LIMIT ?""",
                (cutoff, cutoff, str(snapshot["generationId"]), status, status, max(0, int(limit)) or -1),
            ).fetchall()
        scanned = len(rows)
        if not rows:
            return [], 0
        y_bid = np.asarray([int(row["y_bid"] or 0) / 100.0 for row in rows], dtype=np.float64)
        y_ask = np.asarray([int(row["y_ask"] or 0) / 100.0 for row in rows], dtype=np.float64)
        n_bid = np.asarray([int(row["n_bid"] or 0) / 100.0 for row in rows], dtype=np.float64)
        n_ask = np.asarray([int(row["n_ask"] or 0) / 100.0 for row in rows], dtype=np.float64)
        y_bs = np.asarray([int(row["y_bid_size"] or 0) / 100.0 for row in rows], dtype=np.float64)
        y_as = np.asarray([int(row["y_ask_size"] or 0) / 100.0 for row in rows], dtype=np.float64)
        n_bs = np.asarray([int(row["n_bid_size"] or 0) / 100.0 for row in rows], dtype=np.float64)
        n_as = np.asarray([int(row["n_ask_size"] or 0) / 100.0 for row in rows], dtype=np.float64)
        volume = np.asarray([float(row["volume_24h_units"] or 0) / 100.0 for row in rows], dtype=np.float64)
        oi = np.asarray([
            np.nan if row["open_interest_units"] is None
            else float(row["open_interest_units"]) / 100.0
            for row in rows
        ], dtype=np.float64)
        close = np.asarray([float(row["close_time_ms"] or 0) for row in rows], dtype=np.float64)
        tick = np.maximum(1.0, np.asarray([int(row["tick_units"] or 100) / 100.0 for row in rows], dtype=np.float64))
        y_bid = np.floor(y_bid / tick) * tick
        n_bid = np.floor(n_bid / tick) * tick
        y_ask = np.ceil(np.minimum(y_ask, 100.0 - n_bid) / tick) * tick
        n_ask = np.ceil(np.minimum(n_ask, 100.0 - y_bid) / tick) * tick
        spread = 100.0 - y_bid - n_bid
        hours = (close - _now_ms()) / 3_600_000.0
        valid = (
            (y_bid >= float(settings["min_yes_bid_cents"])) &
            (n_bid >= float(settings["min_no_bid_cents"])) &
            (spread >= float(settings["min_spread_cents"])) &
            (volume >= float(settings["min_vol24h"])) &
            (oi >= float(settings["min_oi"])) &
            (hours >= float(settings["min_time_to_close_hrs"]))
        )
        maximum = settings.get("max_time_to_close_hrs")
        if maximum is not None:
            valid &= hours <= float(maximum)
        max_spread = settings.get("max_spread_cents")
        if max_spread is not None:
            valid &= spread <= float(max_spread)
        keywords = tuple(settings.get("_excluded_ticker_keywords_upper") or ())
        excluded_series = {str(item).upper() for item in (settings.get("excluded_series") or ())}
        for index, row in enumerate(rows):
            ticker = str(row["market_id"])
            event = str(row["event_id"] or ticker)
            if ticker in toxic_tickers or (event.split("-")[0] in toxic_series):
                valid[index] = False
            if keywords and any(item in ticker.upper() for item in keywords):
                valid[index] = False
            if excluded_series and event.split("-")[0].upper() in excluded_series:
                valid[index] = False
        imbalance = np.divide(y_bs - y_as, y_bs + y_as, out=np.zeros_like(y_bs), where=(y_bs + y_as) > 0)
        fair = np.floor(np.clip(np.rint((y_bid + y_ask) / 2.0 + imbalance * float(settings["fair_value_max_orderbook_imbalance_adjust_cents"])), tick, 99.0) / tick) * tick
        fair = np.clip(fair, tick, 99.0)
        tox_yes = float(settings["default_toxicity_cents"]) + np.maximum(0.0, -imbalance) * float(settings["imbalance_toxicity_extra_cents"])
        tox_no = float(settings["default_toxicity_cents"]) + np.maximum(0.0, imbalance) * float(settings["imbalance_toxicity_extra_cents"])
        inventory = float(settings["net_position_contracts"])
        inv_yes = np.where(inventory < 0, 0.0, np.minimum(float(settings["maximum_inventory_skew_ticks"]), abs(inventory) / max(1.0, float(settings["inventory_skew_contracts_per_tick"]))) * float(settings["tick_value_cents_for_inventory_penalty"]))
        inv_no = np.where(inventory > 0, 0.0, np.minimum(float(settings["maximum_inventory_skew_ticks"]), abs(inventory) / max(1.0, float(settings["inventory_skew_contracts_per_tick"]))) * float(settings["tick_value_cents_for_inventory_penalty"]))
        def side_ev(best_bid: Any, ask: Any, fair_side: Any, toxicity: Any, inv: Any, size: Any) -> Any:
            result = np.full(len(rows), -1e9, dtype=np.float64)
            candidate = np.minimum(ask - tick, np.floor((best_bid - tick * int(settings["passive_offset_ticks_when_not_improving"])) / tick) * tick)
            for level in range(max(1, int(settings["candidate_price_levels_to_scan"]))):
                price = candidate + level * tick
                usable = price <= ask - tick
                queue = np.where(np.isclose(price, best_bid), size, 0.0)
                fee = 7.0 * (price / 100.0) * (1.0 - price / 100.0) * float(settings["maker_fee_factor"])
                qpen = np.minimum(float(settings["queue_penalty_cap_cents"]), queue / max(1e-9, float(settings["queue_penalty_contracts_per_cent"])))
                ev = fair_side - price - fee - toxicity - inv - qpen
                result = np.where(usable & (ev >= float(settings["minimum_expected_edge_cents_to_quote"])), np.maximum(result, ev), result)
            return result
        yes_ev = side_ev(y_bid, y_ask, fair, tox_yes, inv_yes, y_bs)
        no_ev = side_ev(n_bid, n_ask, 100.0 - fair, tox_no, inv_no, n_bs)
        best_ev = np.maximum(yes_ev, no_ev)
        valid &= best_ev > -1e8
        candidates = np.flatnonzero(valid)
        if not len(candidates):
            return [], scanned
        keep = min(max(1, int(settings.get("top_n") or 1)), len(candidates))
        if len(candidates) > keep:
            candidates = candidates[np.argpartition(best_ev[candidates], -keep)[-keep:]]
        candidates = candidates[np.argsort(-best_ev[candidates], kind="stable")]
        return [self._payload(rows[int(index)]) for index in candidates], scanned

    @staticmethod
    def _payload(row: sqlite3.Row) -> dict[str, Any]:
        def dollars(value: Any) -> Optional[str]:
            return None if value is None else f"{float(value) / 10_000:.4f}"

        def count(value: Any) -> Optional[str]:
            return None if value is None else f"{float(value) / 100:.2f}"

        def iso(value: Any) -> str:
            if value is None:
                return ""
            return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")

        return {
            "venue": "polymarket", "ticker": str(row["market_id"]),
            "native_market_id": str(row["market_id"]), "yes_token_id": str(row["yes_token_id"]),
            "no_token_id": str(row["no_token_id"]), "market_rules": "direct_token_books",
            "title": str(row["title"] or ""), "status": str(row["status"] or ""),
            "series_ticker": str(row["series_id"] or ""), "event_ticker": str(row["event_id"] or ""),
            "close_time_ms": row["close_time_ms"],
            "close_time": iso(row["close_time_ms"]),
            "_cutoff_ms": row["close_time_ms"], "_cutoff_field": "close_time",
            "tick_size": max(1, int(row["tick_units"] or 100) // 100),
            "yes_bid": None if row["y_bid"] is None else int(round(int(row["y_bid"]) / 100.0)),
            "yes_ask": None if row["y_ask"] is None else int(round(int(row["y_ask"]) / 100.0)),
            "no_bid": None if row["n_bid"] is None else int(round(int(row["n_bid"]) / 100.0)),
            "no_ask": None if row["n_ask"] is None else int(round(int(row["n_ask"]) / 100.0)),
            "yes_bid_dollars": dollars(row["y_bid"]), "yes_ask_dollars": dollars(row["y_ask"]),
            "no_bid_dollars": dollars(row["n_bid"]), "no_ask_dollars": dollars(row["n_ask"]),
            "yes_bid_size_fp": count(row["y_bid_size"]), "yes_ask_size_fp": count(row["y_ask_size"]),
            "no_bid_size_fp": count(row["n_bid_size"]), "no_ask_size_fp": count(row["n_ask_size"]),
            "volume_24h_fp": count(row["volume_24h_units"]), "open_interest_fp": count(row["open_interest_units"]),
            "last_book_timestamp_ms": min(int(row["y_ts"] or 0), int(row["n_ts"] or 0)),
            "market_url": row["market_url"], "series_title": row["series_title"],
        }


class PolymarketMirror:
    """Synchronizer used by the launcher child process and unit tests."""

    def __init__(self, client_config: Any, store_path: str | Path, status_path: str | Path, mirror_config: MirrorConfig) -> None:
        self.client_config = client_config
        self.store = PolymarketMirrorStore(store_path)
        self.status_path = Path(status_path)
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = mirror_config
        self._status_lock = threading.Lock()
        self._publication_lock = threading.Lock()

    def _write_status(self, status: Mapping[str, Any]) -> None:
        with self._status_lock:
            temporary = self.status_path.with_suffix(self.status_path.suffix + ".tmp")
            temporary.write_text(json.dumps(dict(status), separators=(",", ":")), encoding="utf-8")
            os.replace(temporary, self.status_path)

    def _publish(self, generation_id: str, *, catalog_count: int, catalog_complete: bool = True) -> dict[str, Any]:
        with self._publication_lock:
            return self.store.publish(
                generation_id,
                catalog_count=catalog_count,
                max_age_seconds=float(self.config.book_stale_after_seconds),
                catalog_complete=catalog_complete,
            )

    def sync_once(self) -> dict[str, Any]:
        started = time.perf_counter()
        client = self._new_client()
        try:
            generation, markets, status = self._sync_incremental(client, started)
            status.update({
                "running": True,
                "catalogDurationMs": int((time.perf_counter() - started) * 1000),
                "bookAssetsExpected": len(markets) * 2,
                "bookAssetsReady": len(client.book_cache.snapshot()),
                "restRecovery": True,
                "apiActivity": client.activity_snapshot(),
            })
            self._write_status(status)
            return status
        except Exception as exc:
            status = {"schemaVersion": MIRROR_SCHEMA_VERSION, "running": True, "complete": False, "reason": str(exc)}
            self._write_status(status)
            raise
        finally:
            try:
                asyncio.run(client.close())
            except Exception:
                pass

    def _new_client(self) -> Any:
        # The mirror owns its catalog/book persistence. It must never trigger
        # the screener's synchronous catalog refresh path.
        config = replace(self.client_config, catalog_path="", book_cache_path="", mirror_enabled=False)
        return build_client("polymarket", config)

    def _sync_incremental(
        self,
        client: Any,
        started: Optional[float] = None,
        progress: Optional[Callable[[Mapping[str, Any]], None]] = None,
    ) -> tuple[str, list[Market], dict[str, Any]]:
        """Publish each hydrated catalog page while the cursor keeps syncing."""
        started = time.perf_counter() if started is None else started
        generation = f"{_now_ms()}-{os.getpid()}"
        target = int(self.config.max_markets or 0) or 1_000_000
        page_size = max(1, min(100, int(self.config.catalog_page_size)))
        markets: list[Market] = []
        seen: set[str] = set()
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        pages = 0
        catalog_started_ms = _now_ms()
        catalog_finished = False
        open_interest_totals = {
            "batches": 0,
            "marketsRequested": 0,
            "marketsResolved": 0,
            "marketsMissing": 0,
            "apiErrors": 0,
            "forbiddenResponses": 0,
            "cloudflare403": 0,
            "retries": 0,
            "retryExhausted": 0,
            "suppressedRequests": 0,
        }
        while len(markets) < target:
            cancel = getattr(client, "_screener_cancel", None)
            if cancel is not None and cancel.is_set():
                break
            if cursor is not None:
                if cursor in seen_cursors:
                    LOGGER.warning("POLYMARKET_MIRROR_PAGINATION_CYCLE | cursor=%s", cursor)
                    break
                seen_cursors.add(cursor)
            limit = min(page_size, target - len(markets))
            params: dict[str, Any] = {"limit": limit, "closed": False}
            if cursor:
                params["after_cursor"] = cursor
            response = client._http_call(
                client.gamma_http, "get", "/markets/keyset", params=params,
                operation="polymarket_list_markets",
            )
            if isinstance(response, dict):
                rows = response.get("data") if isinstance(response.get("data"), list) else response.get("markets")
            else:
                rows = response if isinstance(response, list) else []
            if not isinstance(rows, list) or not rows:
                catalog_finished = True
                break
            page_markets: list[Market] = []
            for payload in rows:
                market = client._normalize_market(payload) if isinstance(payload, dict) else None
                if market is None or market.status != "active" or market.market_id in seen:
                    continue
                seen.add(market.market_id)
                page_markets.append(market)
                if len(markets) + len(page_markets) >= target:
                    break
            pages += 1

            hydrate_oi = getattr(client, "hydrate_market_open_interest", None)
            if page_markets and callable(hydrate_oi):
                try:
                    page_markets, oi_stats = hydrate_oi(page_markets)
                    for key in open_interest_totals:
                        open_interest_totals[key] += int(oi_stats.get(key, 0) or 0)
                except Exception as exc:
                    # OI is an enrichment. Keep the catalog and book stream
                    # moving when the optional Data API lookup fails.
                    LOGGER.warning("POLYMARKET_OPEN_INTEREST_PAGE_ERROR | error=%s", exc)
                    open_interest_totals["marketsRequested"] += len(page_markets)
                    open_interest_totals["marketsMissing"] += len(page_markets)
                    open_interest_totals["apiErrors"] += 1
            markets.extend(page_markets)
            self.store.upsert_markets(page_markets, generation)
            if page_markets:
                client.hydrate_market_books(page_markets, allow_rest=True)
                page_assets = {
                    token
                    for market in page_markets
                    for token in (market.yes_token_id, market.no_token_id)
                    if token
                }
                page_books = {
                    asset: book
                    for asset, book in client.book_cache.snapshot().items()
                    if asset in page_assets
                }
            else:
                page_books = {}
            # Keep the book write and its readiness publication together so a
            # screener never observes a half-published page while the stream
            # publisher is also committing websocket updates.
            with self._publication_lock:
                self.store.upsert_books(page_books)
                status = self.store.publish(
                    generation,
                    catalog_count=len(markets),
                    max_age_seconds=float(self.config.book_stale_after_seconds),
                    catalog_complete=False,
                )
            api_activity = client.activity_snapshot()
            status.update({
                "running": True,
                "reason": "partial_ready" if status.get("screenable") else "syncing_catalog",
                "pid": os.getpid(),
                "startedAtMs": catalog_started_ms,
                "catalogStartedAtMs": catalog_started_ms,
                "catalogPages": pages,
                "catalogCount": len(markets),
                "catalogDurationMs": int((time.perf_counter() - started) * 1000),
                "bookAssetsExpected": len(markets) * 2,
                "bookAssetsReady": len(client.book_cache.snapshot()),
                "restRecovery": True,
                "openInterest": dict(open_interest_totals),
                "openInterestDiagnostics": api_activity.get("openInterestDiagnostics") or {},
                "apiActivity": api_activity,
                "websocketShards": max(1, int(self.config.ws_shards)),
            })
            if progress is not None:
                progress(status)
            if len(markets) >= target:
                catalog_finished = True
                break
            if len(rows) < limit:
                catalog_finished = True
                break
            next_cursor = None
            if isinstance(response, dict):
                next_cursor = response.get("next_cursor") or response.get("nextCursor") or response.get("cursor")
            next_cursor_text = str(next_cursor) if next_cursor else ""
            if not next_cursor_text:
                catalog_finished = True
                break
            if next_cursor_text == str(cursor) or next_cursor_text in seen_cursors:
                break
            cursor = next_cursor_text
        final = self._publish(
            generation,
            catalog_count=len(markets),
            catalog_complete=catalog_finished,
        )
        api_activity = client.activity_snapshot()
        final.update({
            "catalogDurationMs": int((time.perf_counter() - started) * 1000),
            "catalogPages": pages,
            "catalogCount": len(markets),
            "bookAssetsExpected": len(markets) * 2,
            "bookAssetsReady": len(client.book_cache.snapshot()),
            "restRecovery": True,
            "openInterest": dict(open_interest_totals),
            "openInterestDiagnostics": api_activity.get("openInterestDiagnostics") or {},
            "apiActivity": api_activity,
        })
        return generation, markets, final

    def _sync_client(
        self,
        client: Any,
        started: Optional[float] = None,
        progress: Optional[Callable[[Mapping[str, Any]], None]] = None,
    ) -> tuple[str, list[Market], dict[str, Any]]:
        started = time.perf_counter() if started is None else started
        generation = f"{_now_ms()}-{os.getpid()}"
        target = int(self.config.max_markets or 0) or 1_000_000
        markets = client._list_markets_keyset(
            MarketQuery(status="open", page_size=max(1, int(self.config.catalog_page_size)), max_results=target)
        )
        open_interest_stats = {
            "batches": 0,
            "marketsRequested": 0,
            "marketsResolved": 0,
            "marketsMissing": 0,
            "apiErrors": 0,
            "forbiddenResponses": 0,
            "cloudflare403": 0,
            "retries": 0,
            "retryExhausted": 0,
            "suppressedRequests": 0,
        }
        hydrate_oi = getattr(client, "hydrate_market_open_interest", None)
        if markets and callable(hydrate_oi):
            try:
                markets, open_interest_stats = hydrate_oi(markets)
            except Exception as exc:
                LOGGER.warning("POLYMARKET_OPEN_INTEREST_SYNC_ERROR | error=%s", exc)
                open_interest_stats["marketsRequested"] = len(markets)
                open_interest_stats["marketsMissing"] = len(markets)
                open_interest_stats["apiErrors"] = 1
        self.store.upsert_markets(markets, generation)
        api_activity = client.activity_snapshot()
        if progress is not None:
            progress({
                "schemaVersion": MIRROR_SCHEMA_VERSION,
                "running": True,
                "complete": False,
                "reason": "syncing_books",
                "pid": os.getpid(),
                "startedAtMs": _now_ms(),
                "catalogStartedAtMs": _now_ms(),
                "catalogCount": len(markets),
                "openInterest": dict(open_interest_stats),
                "openInterestDiagnostics": api_activity.get("openInterestDiagnostics") or {},
                "apiActivity": api_activity,
                "websocketShards": max(1, int(self.config.ws_shards)),
            })
        client.hydrate_market_books(markets, allow_rest=True)
        self.store.upsert_books(client.book_cache.snapshot())
        status = self._publish(
            generation,
            catalog_count=len(markets),
        )
        status.update({
            "catalogDurationMs": int((time.perf_counter() - started) * 1000),
            "openInterest": dict(open_interest_stats),
        })
        api_activity = client.activity_snapshot()
        status["openInterestDiagnostics"] = api_activity.get("openInterestDiagnostics") or {}
        status["apiActivity"] = api_activity
        return generation, markets, status

    def run(self, stop: Any) -> None:
        try:
            asyncio.run(self._run_async(stop))
        except Exception as exc:
            LOGGER.warning("POLYMARKET_MIRROR_ERROR | %s", exc)

    async def _configure_streams(
        self,
        stream_clients: Sequence[Any],
        markets: Sequence[Market],
        shard_market_ids: list[set[str]],
        bootstrapped: bool,
    ) -> bool:
        """Keep websocket subscriptions aligned with the published catalog."""

        chunks = [markets[index::len(stream_clients)] for index in range(len(stream_clients))]
        if not bootstrapped:
            for index, (stream_client, chunk) in enumerate(zip(stream_clients, chunks)):
                for market in chunk:
                    stream_client._market_cache[market.market_id] = market
                    if market.yes_token_id:
                        stream_client._asset_market[market.yes_token_id] = (market.market_id, "yes")
                    if market.no_token_id:
                        stream_client._asset_market[market.no_token_id] = (market.market_id, "no")
                if chunk:
                    # The non-blocking path lets the stream consume initial
                    # dumps in the background while catalog paging continues.
                    start_stream = getattr(stream_client, "start_market_book_stream", None)
                    if callable(start_stream):
                        await start_stream(chunk)
                    else:
                        # Compatibility fallback for older/lightweight clients.
                        await stream_client.bootstrap_market_books(
                            chunk,
                            timeout_seconds=float(self.config.bootstrap_timeout_seconds),
                        )
                shard_market_ids[index] = {str(item.market_id) for item in chunk}
            return True

        for index, (stream_client, chunk) in enumerate(zip(stream_clients, chunks)):
            next_ids = {str(item.market_id) for item in chunk}
            for market in chunk:
                stream_client._market_cache[market.market_id] = market
                if market.yes_token_id:
                    stream_client._asset_market[market.yes_token_id] = (market.market_id, "yes")
                if market.no_token_id:
                    stream_client._asset_market[market.no_token_id] = (market.market_id, "no")
            added = sorted(next_ids - shard_market_ids[index])
            removed = sorted(shard_market_ids[index] - next_ids)
            if added or removed:
                await stream_client.update_market_subscriptions(add=added, remove=removed)
            shard_market_ids[index] = next_ids
        return True

    def _publish_stream_snapshot(self, stream_clients: Sequence[Any]) -> dict[str, Any]:
        """Persist websocket books and refresh the active generation marker."""

        with self._publication_lock:
            books: dict[str, BookTop] = {}
            for stream_client in stream_clients:
                books.update(stream_client.book_cache.snapshot())
            self.store.upsert_books(books)
            return self.store.republish_active(
                max_age_seconds=float(self.config.book_stale_after_seconds),
            )

    async def _run_async(self, stop: Any) -> None:
        # Publish a status before opening the client or making the first
        # catalog request.  A cold catalog can legitimately take a while, but
        # the launcher must distinguish that from a dead child process.
        self._write_status({
            "schemaVersion": MIRROR_SCHEMA_VERSION,
            "running": True,
            "complete": False,
            "reason": "starting",
            "pid": os.getpid(),
            "startedAtMs": _now_ms(),
            "websocketShards": max(1, int(self.config.ws_shards)),
        })
        try:
            client = self._new_client()
        except Exception as exc:
            self._write_status({
                "schemaVersion": MIRROR_SCHEMA_VERSION,
                "running": False,
                "complete": False,
                "reason": "client_init_failed",
                "error": str(exc),
                "pid": os.getpid(),
            })
            raise
        stream_clients = [client]
        for _ in range(max(0, int(self.config.ws_shards) - 1)):
            stream_clients.append(self._new_client())
        target = int(self.config.max_markets or 0) or 1_000_000
        generation = self.store.active_generation()
        markets = self.store.active_markets(limit=target)
        shard_market_ids: list[set[str]] = [set() for _ in stream_clients]
        bootstrapped = False
        next_catalog = 0.0
        catalog_task: Optional[asyncio.Task[Any]] = None
        catalog_started_ms = 0
        last_sync_error: Optional[str] = None
        try:
            if markets:
                try:
                    # Resume the last good generation immediately.  A new
                    # catalog request must not take the existing book stream
                    # offline while it is waiting on Gamma/proxy I/O.
                    bootstrapped = await self._configure_streams(
                        stream_clients, markets, shard_market_ids, bootstrapped,
                    )
                except Exception as exc:
                    LOGGER.warning("POLYMARKET_MIRROR_STREAM_START_ERROR | %s", exc)
            while not stop.is_set():
                now = time.monotonic()
                if catalog_task is None and now >= next_catalog:
                    catalog_started_ms = _now_ms()
                    last_sync_error = None
                    self._write_status({
                        "schemaVersion": MIRROR_SCHEMA_VERSION,
                        "running": True,
                        "complete": False,
                        "reason": "syncing_catalog",
                        "pid": os.getpid(),
                        "startedAtMs": catalog_started_ms,
                        "catalogStartedAtMs": catalog_started_ms,
                        "websocketShards": len(stream_clients),
                    })
                    catalog_task = asyncio.create_task(asyncio.to_thread(
                        self._sync_incremental,
                        client,
                        None,
                        self._write_status,
                    ))

                if catalog_task is not None and catalog_task.done():
                    task = catalog_task
                    catalog_task = None
                    try:
                        generation, markets, status = await task
                        if markets:
                            bootstrapped = await self._configure_streams(
                                stream_clients, markets, shard_market_ids, bootstrapped,
                            )
                        status.update({
                            "running": True,
                            "websocket": True,
                            "websocketShards": len(stream_clients),
                            "apiActivity": merge_activity_snapshots(
                                tuple(item.activity_snapshot() for item in stream_clients)
                            ),
                        })
                        self._write_status(status)
                    except Exception as exc:
                        # Keep the last generation alive and visible.  The
                        # periodic stream publisher below will continue to
                        # refresh it while this failed catalog task is retried.
                        last_sync_error = str(exc)
                        self._write_status({
                            "schemaVersion": MIRROR_SCHEMA_VERSION,
                            "running": True,
                            "complete": False,
                            "reason": "sync_failed",
                            "error": last_sync_error,
                            "pid": os.getpid(),
                            "websocketShards": len(stream_clients),
                        })
                        LOGGER.warning("POLYMARKET_MIRROR_SYNC_ERROR | %s", exc)
                    next_catalog = time.monotonic() + max(1.0, float(self.config.sync_interval_seconds))

                if generation and markets:
                    # Persist compact stream updates without blocking the
                    # websocket consumer and refresh whichever generation is
                    # currently active.  This runs while catalog_task is
                    # waiting on Gamma, so the screener sees incremental book
                    # changes instead of an old readiness timestamp.
                    status = await asyncio.to_thread(
                        self._publish_stream_snapshot,
                        stream_clients,
                    )
                    status.update({
                        "running": True,
                        "websocket": True,
                        "websocketShards": len(stream_clients),
                        "apiActivity": merge_activity_snapshots(
                            tuple(item.activity_snapshot() for item in stream_clients)
                        ),
                    })
                    if catalog_task is not None:
                        status.update({
                            "reason": "sync_failed" if last_sync_error else (
                                "partial_ready" if status.get("screenable") else "syncing_catalog"
                            ),
                            "catalogSyncRunning": True,
                            "catalogStartedAtMs": catalog_started_ms,
                        })
                    if last_sync_error:
                        status["reason"] = "sync_failed"
                        status["error"] = last_sync_error
                    self._write_status(status)
                try:
                    await asyncio.wait_for(asyncio.to_thread(stop.wait, 1.0), timeout=1.2)
                except asyncio.TimeoutError:
                    pass
        finally:
            if catalog_task is not None:
                cancel_scan = getattr(client, "cancel_screener_scan", None)
                if callable(cancel_scan):
                    cancel_scan()
                catalog_task.cancel()
                await asyncio.gather(catalog_task, return_exceptions=True)
            await asyncio.gather(*(stream_client.close() for stream_client in stream_clients), return_exceptions=True)
            self._write_status({
                "schemaVersion": MIRROR_SCHEMA_VERSION,
                "running": False,
                "complete": False,
                "reason": "stopped",
                "pid": os.getpid(),
            })


def _mirror_entry(client_config: Any, store_path: str, status_path: str, mirror_config: MirrorConfig, stop: Any) -> None:
    logging.basicConfig(level=logging.INFO)
    PolymarketMirror(client_config, store_path, status_path, mirror_config).run(stop)


class PolymarketMirrorProcess:
    """Launcher-owned child process wrapper."""

    def __init__(self, client_config: Any, store_path: str | Path, status_path: str | Path, mirror_config: MirrorConfig) -> None:
        self._status_path = Path(status_path)
        context = multiprocessing.get_context("spawn")
        self._stop = context.Event()
        self._process = context.Process(
            target=_mirror_entry,
            args=(client_config, str(store_path), str(status_path), mirror_config, self._stop),
            name="polymarket-mirror",
            daemon=True,
        )

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid

    def is_alive(self) -> bool:
        return bool(self._process.is_alive())

    def start(self) -> None:
        if not self._process.is_alive():
            self._stop.clear()
            self._process.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._process.is_alive():
            self._process.join(timeout=max(0.1, timeout))
        if self._process.is_alive():
            self._process.terminate()
            self._process.join(2.0)

    def status(self) -> dict[str, Any]:
        try:
            return json.loads(Path(self._status_path).read_text(encoding="utf-8"))
        except Exception:
            return {"running": self._process.is_alive(), "complete": False, "reason": "status_unavailable"}
