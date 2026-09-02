"""SQLite store for backfilled Kalshi historical market data.

Units follow the repo-wide fixed point: price units are cents * 100
(PRICE_SCALE = 10_000 per dollar); count units are contracts * 100
(COUNT_SCALE = 100). All timestamps are epoch milliseconds.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS markets(
    ticker TEXT PRIMARY KEY,
    series TEXT,
    title TEXT,
    status TEXT,
    close_ts_ms INTEGER,
    result TEXT,
    volume_24h_units INTEGER,
    oi_units INTEGER,
    meta_json TEXT
);
CREATE TABLE IF NOT EXISTS candles(
    ticker TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    price_open_units INTEGER, price_high_units INTEGER,
    price_low_units INTEGER, price_close_units INTEGER,
    yes_bid_open_units INTEGER, yes_bid_high_units INTEGER,
    yes_bid_low_units INTEGER, yes_bid_close_units INTEGER,
    yes_ask_open_units INTEGER, yes_ask_high_units INTEGER,
    yes_ask_low_units INTEGER, yes_ask_close_units INTEGER,
    volume_units INTEGER, oi_units INTEGER,
    PRIMARY KEY(ticker, ts_ms)
);
CREATE TABLE IF NOT EXISTS trades(
    ticker TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    yes_price_units INTEGER,
    no_price_units INTEGER,
    count_units INTEGER,
    taker_side TEXT,
    PRIMARY KEY(ticker, trade_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_ticker_ts ON trades(ticker, ts_ms);
CREATE TABLE IF NOT EXISTS backfill_progress(
    ticker TEXT NOT NULL,
    kind TEXT NOT NULL,
    complete INTEGER NOT NULL DEFAULT 0,
    last_ts_ms INTEGER,
    cursor TEXT,
    PRIMARY KEY(ticker, kind)
);
"""

CANDLE_COLUMNS = (
    "price_open_units", "price_high_units", "price_low_units", "price_close_units",
    "yes_bid_open_units", "yes_bid_high_units", "yes_bid_low_units", "yes_bid_close_units",
    "yes_ask_open_units", "yes_ask_high_units", "yes_ask_low_units", "yes_ask_close_units",
    "volume_units", "oi_units",
)


class HistoryStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    def upsert_market(self, *, ticker: str, series: str, title: str, status: str,
                      close_ts_ms: Optional[int], result: str,
                      volume_24h_units: Optional[int], oi_units: Optional[int],
                      meta: Optional[dict] = None) -> None:
        self.conn.execute(
            "INSERT INTO markets(ticker, series, title, status, close_ts_ms, result,"
            " volume_24h_units, oi_units, meta_json) VALUES(?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(ticker) DO UPDATE SET series=excluded.series, title=excluded.title,"
            " status=excluded.status, close_ts_ms=excluded.close_ts_ms, result=excluded.result,"
            " volume_24h_units=excluded.volume_24h_units, oi_units=excluded.oi_units,"
            " meta_json=excluded.meta_json",
            (ticker, series, title, status, close_ts_ms, result,
             volume_24h_units, oi_units, json.dumps(meta or {})),
        )

    def insert_candles(self, ticker: str, rows: Iterable[dict]) -> int:
        payload = [
            tuple([ticker, int(row["ts_ms"])] + [row.get(col) for col in CANDLE_COLUMNS])
            for row in rows
            if row.get("ts_ms")
        ]
        if not payload:
            return 0
        placeholders = ",".join("?" for _ in range(2 + len(CANDLE_COLUMNS)))
        self.conn.executemany(
            f"INSERT OR REPLACE INTO candles(ticker, ts_ms, {', '.join(CANDLE_COLUMNS)})"
            f" VALUES({placeholders})",
            payload,
        )
        return len(payload)

    def insert_trades(self, ticker: str, trades: Iterable) -> int:
        payload = [
            (ticker, trade.trade_id, trade.timestamp_ms, trade.yes_price_units,
             trade.no_price_units, trade.count_units, trade.taker_side)
            for trade in trades
            if trade.trade_id
        ]
        if not payload:
            return 0
        self.conn.executemany(
            "INSERT OR IGNORE INTO trades(ticker, trade_id, ts_ms, yes_price_units,"
            " no_price_units, count_units, taker_side) VALUES(?,?,?,?,?,?,?)",
            payload,
        )
        return len(payload)

    def get_progress(self, ticker: str, kind: str) -> tuple[bool, Optional[int], str]:
        row = self.conn.execute(
            "SELECT complete, last_ts_ms, cursor FROM backfill_progress WHERE ticker=? AND kind=?",
            (ticker, kind),
        ).fetchone()
        if row is None:
            return False, None, ""
        return bool(row[0]), row[1], row[2] or ""

    def set_progress(self, ticker: str, kind: str, *, complete: bool,
                     last_ts_ms: Optional[int] = None, cursor: str = "") -> None:
        self.conn.execute(
            "INSERT INTO backfill_progress(ticker, kind, complete, last_ts_ms, cursor)"
            " VALUES(?,?,?,?,?) ON CONFLICT(ticker, kind) DO UPDATE SET"
            " complete=excluded.complete, last_ts_ms=excluded.last_ts_ms, cursor=excluded.cursor",
            (ticker, kind, int(complete), last_ts_ms, cursor),
        )

    def commit(self) -> None:
        self.conn.commit()

    def summary(self) -> dict:
        markets = self.conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        candles = self.conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
        trades = self.conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        done = self.conn.execute(
            "SELECT COUNT(*) FROM backfill_progress WHERE complete=1"
        ).fetchone()[0]
        return {"markets": markets, "candles": candles, "trades": trades, "complete_tasks": done}
