"""Durable, storage-only analytics for the account portfolio UI."""

from __future__ import annotations

import base64
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional

from clients.models import AccountFill, AccountOrder


PRICE_SCALE = 10_000
COUNT_SCALE = 100
SUMMARY_RETENTION_MS = 35 * 24 * 60 * 60 * 1000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _value_units(count_units: int, price_units: Optional[int]) -> Optional[int]:
    if price_units is None:
        return None
    return int(round(count_units * price_units / COUNT_SCALE))


def _percent_bps(change: Optional[int], baseline: Optional[int]) -> Optional[int]:
    if change is None or baseline in (None, 0):
        return None
    return int(round(change * 10_000 / abs(baseline)))


class PortfolioAnalyticsStore:
    """SQLite writer/reader shared by the launcher and operations API."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=5)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA busy_timeout=5000")
            with db:
                yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS current_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    snapshot_at_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS summary_samples (
                    snapshot_at_ms INTEGER PRIMARY KEY,
                    available_cash_units INTEGER,
                    midpoint_position_value_units INTEGER,
                    total_portfolio_value_units INTEGER,
                    liquidation_value_units INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_summary_samples_time
                    ON summary_samples(snapshot_at_ms);
                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    side TEXT,
                    client_order_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    price_units INTEGER,
                    fill_count_units INTEGER NOT NULL DEFAULT 0,
                    remaining_count_units INTEGER NOT NULL DEFAULT 0,
                    initial_count_units INTEGER NOT NULL DEFAULT 0,
                    fill_cost_units INTEGER NOT NULL DEFAULT 0,
                    fees_units INTEGER NOT NULL DEFAULT 0,
                    created_at_ms INTEGER,
                    updated_at_ms INTEGER,
                    expiration_time_ms INTEGER,
                    is_open INTEGER NOT NULL DEFAULT 0,
                    last_seen_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_orders_market
                    ON orders(market_id, created_at_ms);
                CREATE INDEX IF NOT EXISTS idx_portfolio_orders_open
                    ON orders(is_open, market_id);
                CREATE TABLE IF NOT EXISTS fills (
                    fill_id TEXT PRIMARY KEY,
                    trade_id TEXT NOT NULL DEFAULT '',
                    order_id TEXT NOT NULL DEFAULT '',
                    market_id TEXT NOT NULL,
                    side TEXT,
                    count_units INTEGER NOT NULL DEFAULT 0,
                    price_units INTEGER,
                    fee_units INTEGER NOT NULL DEFAULT 0,
                    created_at_ms INTEGER,
                    is_taker INTEGER NOT NULL DEFAULT 0,
                    last_seen_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_fills_market_time
                    ON fills(market_id, created_at_ms DESC, fill_id DESC);
                CREATE INDEX IF NOT EXISTS idx_portfolio_fills_order
                    ON fills(order_id);
                CREATE TABLE IF NOT EXISTS market_links (
                    market_id TEXT PRIMARY KEY,
                    market_url TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                PRAGMA user_version=1;
                """
            )

    @staticmethod
    def _upsert_order(db: sqlite3.Connection, order: AccountOrder, snapshot_at_ms: int, is_open: bool) -> None:
        if not order.order_id:
            return
        db.execute(
            """
            INSERT INTO orders(
                order_id,market_id,side,client_order_id,status,price_units,
                fill_count_units,remaining_count_units,initial_count_units,
                fill_cost_units,fees_units,created_at_ms,updated_at_ms,
                expiration_time_ms,is_open,last_seen_at_ms
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_id) DO UPDATE SET
                market_id=excluded.market_id,side=excluded.side,
                client_order_id=excluded.client_order_id,status=excluded.status,
                price_units=excluded.price_units,fill_count_units=excluded.fill_count_units,
                remaining_count_units=excluded.remaining_count_units,
                initial_count_units=excluded.initial_count_units,
                fill_cost_units=excluded.fill_cost_units,fees_units=excluded.fees_units,
                created_at_ms=COALESCE(excluded.created_at_ms,orders.created_at_ms),
                updated_at_ms=COALESCE(excluded.updated_at_ms,orders.updated_at_ms),
                expiration_time_ms=excluded.expiration_time_ms,
                is_open=excluded.is_open,last_seen_at_ms=excluded.last_seen_at_ms
            """,
            (
                order.order_id,
                order.market_id,
                order.side,
                order.client_order_id,
                order.status,
                order.price_units,
                order.fill_count_units,
                order.remaining_count_units,
                order.initial_count_units,
                order.fill_cost_units,
                order.fees_units,
                order.created_at_ms,
                order.updated_at_ms,
                order.expiration_time_ms,
                int(is_open),
                snapshot_at_ms,
            ),
        )

    @staticmethod
    def _upsert_fill(db: sqlite3.Connection, fill: AccountFill, snapshot_at_ms: int) -> None:
        if not fill.fill_id:
            return
        db.execute(
            """
            INSERT INTO fills(
                fill_id,trade_id,order_id,market_id,side,count_units,price_units,
                fee_units,created_at_ms,is_taker,last_seen_at_ms
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(fill_id) DO UPDATE SET
                trade_id=excluded.trade_id,order_id=excluded.order_id,
                market_id=excluded.market_id,side=excluded.side,
                count_units=excluded.count_units,price_units=excluded.price_units,
                fee_units=excluded.fee_units,
                created_at_ms=COALESCE(excluded.created_at_ms,fills.created_at_ms),
                is_taker=excluded.is_taker,last_seen_at_ms=excluded.last_seen_at_ms
            """,
            (
                fill.fill_id,
                fill.trade_id,
                fill.order_id,
                fill.market_id,
                fill.side,
                fill.count_units,
                fill.price_units,
                fill.fee_units,
                fill.created_at_ms,
                int(fill.is_taker),
                snapshot_at_ms,
            ),
        )

    def record_refresh(
        self,
        snapshot: Mapping[str, object],
        orders: Iterable[AccountOrder],
        fills: Iterable[AccountFill],
        resting_orders: Iterable[AccountOrder],
    ) -> None:
        snapshot_at_ms = int(snapshot.get("generatedAtMs") or _now_ms())
        summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
        available_cash = summary.get("availableCashUnits")
        midpoint_value = summary.get("midpointPositionValueUnits")
        total_portfolio_value = (
            int(available_cash) + int(midpoint_value)
            if isinstance(available_cash, (int, float)) and isinstance(midpoint_value, (int, float))
            else None
        )
        resting = list(resting_orders)
        resting_ids = {order.order_id for order in resting if order.order_id}
        with self._connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO metadata(key,value) VALUES('coverage_started_at_ms',?)",
                (str(snapshot_at_ms),),
            )
            db.execute(
                """
                INSERT INTO current_state(singleton,snapshot_at_ms,payload_json)
                VALUES(1,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                    snapshot_at_ms=excluded.snapshot_at_ms,payload_json=excluded.payload_json
                """,
                (snapshot_at_ms, json.dumps(snapshot, separators=(",", ":"))),
            )
            db.execute(
                """
                INSERT OR REPLACE INTO summary_samples(
                    snapshot_at_ms,available_cash_units,midpoint_position_value_units,
                    total_portfolio_value_units,liquidation_value_units
                ) VALUES(?,?,?,?,?)
                """,
                (
                    snapshot_at_ms,
                    available_cash,
                    midpoint_value,
                    total_portfolio_value,
                    summary.get("positionsLiquidationValueUnits"),
                ),
            )
            db.execute(
                "DELETE FROM summary_samples WHERE snapshot_at_ms<?",
                (snapshot_at_ms - SUMMARY_RETENTION_MS,),
            )
            db.execute("UPDATE orders SET is_open=0 WHERE is_open=1")
            seen_orders: Dict[str, AccountOrder] = {
                order.order_id: order for order in orders if order.order_id
            }
            seen_orders.update({order.order_id: order for order in resting if order.order_id})
            for order in seen_orders.values():
                self._upsert_order(db, order, snapshot_at_ms, order.order_id in resting_ids)
            for fill in fills:
                self._upsert_fill(db, fill, snapshot_at_ms)

    def _current(self) -> tuple[Optional[int], Dict[str, Any]]:
        with self._connect() as db:
            row = db.execute("SELECT snapshot_at_ms,payload_json FROM current_state WHERE singleton=1").fetchone()
        if row is None:
            return None, {}
        try:
            return int(row["snapshot_at_ms"]), json.loads(row["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return None, {}

    def _coverage_started_at(self) -> Optional[int]:
        with self._connect() as db:
            row = db.execute("SELECT value FROM metadata WHERE key='coverage_started_at_ms'").fetchone()
        try:
            return int(row[0]) if row else None
        except (TypeError, ValueError):
            return None

    def cache_market_links(self, links: Mapping[str, str]) -> None:
        if not links:
            return
        timestamp = _now_ms()
        with self._connect() as db:
            db.executemany(
                """
                INSERT INTO market_links(market_id,market_url,updated_at_ms) VALUES(?,?,?)
                ON CONFLICT(market_id) DO UPDATE SET
                    market_url=excluded.market_url,updated_at_ms=excluded.updated_at_ms
                """,
                [(str(market_id), str(url), timestamp) for market_id, url in links.items()],
            )

    def market_links(self) -> Dict[str, str]:
        with self._connect() as db:
            return {
                str(row["market_id"]): str(row["market_url"])
                for row in db.execute("SELECT market_id,market_url FROM market_links")
            }

    @staticmethod
    def _source(snapshot_at_ms: Optional[int]) -> Dict[str, Any]:
        return {
            "available": snapshot_at_ms is not None,
            "updatedAt": snapshot_at_ms,
            "stale": snapshot_at_ms is None or _now_ms() - snapshot_at_ms > 45_000,
        }

    @staticmethod
    def _points(rows: list[sqlite3.Row], column: str, maximum: int = 240) -> list[Dict[str, int]]:
        values = [
            {"timestampMs": int(row["snapshot_at_ms"]), "valueUnits": int(row[column])}
            for row in rows
            if row[column] is not None
        ]
        if len(values) <= maximum:
            return values
        indexes = sorted({round(index * (len(values) - 1) / (maximum - 1)) for index in range(maximum)})
        return [values[index] for index in indexes]

    def summary(self, *, window_ms: int = 86_400_000) -> Dict[str, Any]:
        generated_at = _now_ms()
        snapshot_at_ms, current = self._current()
        source = self._source(snapshot_at_ms)
        warnings = list(current.get("warnings") or [])
        if snapshot_at_ms is None:
            return {
                "generatedAt": generated_at,
                "snapshotAtMs": None,
                "source": source,
                "coverage": {"startedAtMs": None, "requestedWindowMs": window_ms, "actualWindowMs": 0, "partial": True},
                "summary": {},
                "history": {},
                "warnings": ["portfolio analytics have not recorded a snapshot yet"],
            }
        target = snapshot_at_ms - window_ms
        with self._connect() as db:
            prior = db.execute(
                "SELECT * FROM summary_samples WHERE snapshot_at_ms<=? ORDER BY snapshot_at_ms DESC LIMIT 1",
                (target,),
            ).fetchone()
            rows = list(
                db.execute(
                    "SELECT * FROM summary_samples WHERE snapshot_at_ms>? AND snapshot_at_ms<=? ORDER BY snapshot_at_ms",
                    (target, snapshot_at_ms),
                ).fetchall()
            )
            if prior is not None:
                rows.insert(0, prior)
            elif not rows:
                earliest = db.execute("SELECT * FROM summary_samples ORDER BY snapshot_at_ms LIMIT 1").fetchone()
                if earliest is not None:
                    rows.append(earliest)
        earliest_at = int(rows[0]["snapshot_at_ms"]) if rows else snapshot_at_ms
        partial = prior is None
        actual_window_ms = max(0, snapshot_at_ms - earliest_at)
        if partial:
            warnings.append("Requested portfolio history is still accumulating")
        summary = current.get("summary") if isinstance(current.get("summary"), dict) else {}
        available_cash = summary.get("availableCashUnits")
        midpoint_value = summary.get("midpointPositionValueUnits")
        total_portfolio_value = (
            int(available_cash) + int(midpoint_value)
            if isinstance(available_cash, (int, float)) and isinstance(midpoint_value, (int, float))
            else None
        )
        mapping = {
            "availableCash": ("available_cash_units", available_cash),
            "totalPortfolioValue": ("total_portfolio_value_units", total_portfolio_value),
            "positionsLiquidationValue": ("liquidation_value_units", summary.get("positionsLiquidationValueUnits")),
        }
        history: Dict[str, Any] = {}
        for name, (column, current_value) in mapping.items():
            points = self._points(rows, column)
            baseline = points[0]["valueUnits"] if points else None
            resolved_current = int(current_value) if current_value is not None else None
            change = resolved_current - baseline if resolved_current is not None and baseline is not None else None
            history[name] = {
                "currentUnits": resolved_current,
                "baselineUnits": baseline,
                "changeUnits": change,
                "changeBps": _percent_bps(change, baseline),
                "points": points,
                "partial": partial,
                "actualWindowMs": actual_window_ms,
            }
        return {
            "generatedAt": generated_at,
            "snapshotAtMs": snapshot_at_ms,
            "source": source,
            "coverage": {
                "startedAtMs": self._coverage_started_at(),
                "requestedWindowMs": window_ms,
                "actualWindowMs": actual_window_ms,
                "partial": partial,
            },
            "summary": {
                "availableCashUnits": available_cash,
                "midpointPositionValueUnits": midpoint_value,
                "totalPortfolioValueUnits": total_portfolio_value,
                "positionsLiquidationValueUnits": summary.get("positionsLiquidationValueUnits"),
                "apiTier": summary.get("apiTier"),
                "readRateLimit": summary.get("readRateLimit"),
                "writeRateLimit": summary.get("writeRateLimit"),
                "positionCount": summary.get("positionCount"),
            },
            "history": history,
            "warnings": list(dict.fromkeys(warnings)),
        }

    @staticmethod
    def _active_start(active_run: Optional[Mapping[str, object]]) -> Optional[int]:
        if not active_run:
            return None
        value = active_run.get("startedAt")
        return int(value) if isinstance(value, (int, float)) else None

    def positions(self, *, active_run: Optional[Mapping[str, object]]) -> Dict[str, Any]:
        generated_at = _now_ms()
        snapshot_at_ms, current = self._current()
        active_start = self._active_start(active_run)
        items = [dict(item) for item in current.get("positions", []) if isinstance(item, dict)]
        market_ids = {
            str(item.get("marketId") or item.get("ticker") or "")
            for item in items
            if item.get("marketId") or item.get("ticker")
        }
        order_counts: Dict[str, int] = {}
        fill_activity: Dict[str, Dict[str, Optional[int]]] = {}
        session_markets: set[str] = set()
        with self._connect() as db:
            # The account order archive can contain millions of rows.  The old
            # GROUP BY queries scanned the complete archive even though this
            # response only annotates the handful of positions in the current
            # snapshot.  Point lookups use the existing market indexes and keep
            # request cost proportional to the number of displayed positions.
            for market_id in market_ids:
                order_counts[market_id] = int(db.execute(
                    "SELECT COUNT(*) FROM orders WHERE market_id=?", (market_id,),
                ).fetchone()[0])
                row = db.execute(
                    "SELECT COUNT(*) AS count,MAX(created_at_ms) AS last_trade_at_ms "
                    "FROM fills WHERE market_id=?",
                    (market_id,),
                ).fetchone()
                fill_activity[market_id] = {
                    "count": int(row["count"]),
                    "lastTradeAtMs": int(row["last_trade_at_ms"]) if row["last_trade_at_ms"] is not None else None,
                }
                if active_start is not None and db.execute(
                    "SELECT 1 FROM fills WHERE market_id=? AND created_at_ms>=? LIMIT 1",
                    (market_id, active_start),
                ).fetchone():
                    session_markets.add(market_id)
        for item in items:
            market_id = str(item.get("marketId") or item.get("ticker") or "")
            item["totalOrderCount"] = order_counts.get(market_id, 0)
            item["totalFillCount"] = fill_activity.get(market_id, {}).get("count", 0)
            item["lastTradeAtMs"] = fill_activity.get(market_id, {}).get("lastTradeAtMs")
            item["currentMarketValueUnits"] = item.get("unrealizedValueUnits")
            realized = item.get("realizedPnlUnits")
            fees = item.get("feesUnits")
            net_realized = (
                int(realized) - int(fees)
                if isinstance(realized, (int, float)) and isinstance(fees, (int, float)) else None
            )
            unrealized = item.get("unrealizedPnlUnits")
            item["netRealizedPnlUnits"] = net_realized
            item["totalPnlUnits"] = (
                net_realized + int(unrealized)
                if net_realized is not None and isinstance(unrealized, (int, float)) else None
            )
            item["runningInCurrentSession"] = market_id in session_markets
        return {
            "generatedAt": generated_at,
            "snapshotAtMs": snapshot_at_ms,
            "source": self._source(snapshot_at_ms),
            "coverage": {"startedAtMs": self._coverage_started_at()},
            "items": items,
            "warnings": list(current.get("warnings") or []),
        }

    def bot_traded_markets(self, market_ids: Iterable[str]) -> set[str]:
        """Return candidate markets with fills from a bot-owned order.

        This intentionally performs indexed candidate lookups.  It replaces an
        account-wide walk of every historical shard database that Overview used
        solely to decide whether an unmanaged exchange position merits a warning.
        """
        candidates = {str(market_id) for market_id in market_ids if market_id}
        if not candidates:
            return set()
        result: set[str] = set()
        with self._connect() as db:
            for market_id in candidates:
                row = db.execute(
                    """
                    SELECT 1
                    FROM fills AS f JOIN orders AS o ON o.order_id=f.order_id
                    WHERE f.market_id=? AND (
                        o.client_order_id LIKE 'mm:%' OR
                        o.client_order_id LIKE 'tob:%' OR
                        o.client_order_id LIKE 'wd:%'
                    )
                    LIMIT 1
                    """,
                    (market_id,),
                ).fetchone()
                if row is not None:
                    result.add(market_id)
        return result

    @staticmethod
    def _encode_cursor(created_at_ms: Optional[int], fill_id: str) -> str:
        raw = json.dumps([int(created_at_ms or 0), fill_id], separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str) -> tuple[int, str]:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            value = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
            return int(value[0]), str(value[1])
        except Exception as exc:
            raise ValueError("invalid fill cursor") from exc

    @staticmethod
    def _mark_for_fill(position: Mapping[str, object], side: Optional[str], field: str) -> Optional[int]:
        position_side = position.get("side")
        direct = position.get(f"{field}PriceUnits")
        if not isinstance(direct, (int, float)):
            return None
        if side == position_side:
            return int(direct)
        if side in {"yes", "no"} and position_side in {"yes", "no"}:
            opposite_field = "ask" if field == "bid" else "bid" if field == "ask" else "mid"
            opposite = position.get(f"{opposite_field}PriceUnits")
            return PRICE_SCALE - int(opposite) if isinstance(opposite, (int, float)) else None
        return None

    def has_market(self, ticker: str) -> bool:
        snapshot_at_ms, current = self._current()
        if snapshot_at_ms is not None and any(
            isinstance(item, dict) and str(item.get("ticker") or item.get("marketId") or "") == ticker
            for item in current.get("positions", [])
        ):
            return True
        with self._connect() as db:
            row = db.execute(
                "SELECT 1 FROM fills WHERE market_id=? UNION SELECT 1 FROM orders WHERE market_id=? LIMIT 1",
                (ticker, ticker),
            ).fetchone()
        return row is not None

    def fills(self, ticker: str, *, limit: int = 100, cursor: str = "") -> Dict[str, Any]:
        generated_at = _now_ms()
        snapshot_at_ms, current = self._current()
        positions = {
            str(item.get("ticker") or item.get("marketId") or ""): item
            for item in current.get("positions", [])
            if isinstance(item, dict)
        }
        position = positions.get(ticker, {})
        params: list[object] = [ticker]
        clause = ""
        if cursor:
            cursor_at, cursor_id = self._decode_cursor(cursor)
            clause = " AND (COALESCE(f.created_at_ms,0)<? OR (COALESCE(f.created_at_ms,0)=? AND f.fill_id<?))"
            params.extend([cursor_at, cursor_at, cursor_id])
        params.append(limit + 1)
        with self._connect() as db:
            rows = list(
                db.execute(
                    f"""
                    SELECT f.*,o.created_at_ms AS order_created_at_ms
                    FROM fills f LEFT JOIN orders o ON o.order_id=f.order_id
                    WHERE f.market_id=?{clause}
                    ORDER BY COALESCE(f.created_at_ms,0) DESC,f.fill_id DESC LIMIT ?
                    """,
                    params,
                ).fetchall()
            )
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = []
        for row in rows:
            count = int(row["count_units"] or 0)
            price = int(row["price_units"]) if row["price_units"] is not None else None
            notional = _value_units(count, price)
            total_paid = notional + int(row["fee_units"] or 0) if notional is not None else None
            bid = self._mark_for_fill(position, row["side"], "bid")
            mid = self._mark_for_fill(position, row["side"], "mid")
            liquidation = _value_units(count, bid)
            market_value = _value_units(count, mid)
            created_at = int(row["created_at_ms"]) if row["created_at_ms"] is not None else None
            order_created = int(row["order_created_at_ms"]) if row["order_created_at_ms"] is not None else None
            items.append(
                {
                    "fillId": row["fill_id"],
                    "tradeId": row["trade_id"],
                    "orderId": row["order_id"],
                    "ticker": row["market_id"],
                    "side": row["side"],
                    "filledAtMs": created_at,
                    "timeToFillMs": max(0, created_at - order_created) if created_at is not None and order_created is not None else None,
                    "contractsUnits": count,
                    "costOfContractsUnits": price,
                    "notionalCostUnits": notional,
                    "feeUnits": int(row["fee_units"] or 0),
                    "costInPositionUnits": total_paid,
                    "liquidationValueUnits": liquidation,
                    "unrealizedValueUnits": market_value,
                    "liquidationPnlUnits": liquidation - total_paid if liquidation is not None and total_paid is not None else None,
                    "marketPnlUnits": market_value - total_paid if market_value is not None and total_paid is not None else None,
                    "isTaker": bool(row["is_taker"]),
                }
            )
        next_cursor = None
        if has_more and rows:
            last = rows[-1]
            next_cursor = self._encode_cursor(last["created_at_ms"], str(last["fill_id"]))
        return {
            "generatedAt": generated_at,
            "snapshotAtMs": snapshot_at_ms,
            "source": self._source(snapshot_at_ms),
            "coverage": {"startedAtMs": self._coverage_started_at()},
            "ticker": ticker,
            "items": items,
            "nextCursor": next_cursor,
            "warnings": list(current.get("warnings") or []),
        }

    def orders(
        self,
        *,
        active_run: Optional[Mapping[str, object]],
        placement_attempts: Optional[Mapping[str, int]] = None,
    ) -> Dict[str, Any]:
        generated_at = _now_ms()
        snapshot_at_ms, current = self._current()
        active_start = self._active_start(active_run)
        attempted_by_market = {str(key): int(value) for key, value in (placement_attempts or {}).items()}
        current_order_items = [
            item for item in (current.get("orders") or {}).get("items", []) if isinstance(item, dict)
        ]
        order_marks = {
            (str(item.get("ticker") or item.get("marketId") or ""), str(item.get("side") or "unknown")): item
            for item in current_order_items
        }
        order_details: Dict[str, Mapping[str, object]] = {}
        for item in current_order_items:
            order_details.setdefault(str(item.get("ticker") or item.get("marketId") or ""), item)
        with self._connect() as db:
            open_orders = list(db.execute("SELECT * FROM orders WHERE is_open=1 ORDER BY created_at_ms"))
            fill_rows = list(
                db.execute(
                    """
                    SELECT f.order_id,f.created_at_ms,o.market_id,o.created_at_ms AS order_created_at_ms
                    FROM fills f JOIN orders o ON o.order_id=f.order_id WHERE o.is_open=1
                    """
                )
            )
        fills_by_order: Dict[str, list[sqlite3.Row]] = {}
        for fill in fill_rows:
            fills_by_order.setdefault(str(fill["order_id"]), []).append(fill)
        groups: Dict[str, Dict[str, Any]] = {}
        all_created: list[int] = []
        all_latencies: list[int] = []
        all_fill_times: list[int] = []
        for order in open_orders:
            ticker = str(order["market_id"])
            side = str(order["side"] or "unknown")
            mark = order_marks.get((ticker, side), {})
            details = order_details.get(ticker, {})
            group = groups.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "marketId": ticker,
                    "title": details.get("title") or ticker,
                    "marketUrl": details.get("marketUrl"),
                    "openOrderCount": 0,
                    "ordersAttempted": attempted_by_market.get(ticker, 0),
                    "remainingContractsUnits": 0,
                    "initialContractsUnits": 0,
                    "filledContractsUnits": 0,
                    "totalFillCount": 0,
                    "firstCreatedAtMs": None,
                    "lastUpdatedAtMs": None,
                    "totalTimeOnBookMs": None,
                    "midPriceUnits": None,
                    "totalMarketValueUnits": 0,
                    "sideBreakdown": {},
                    "runningInCurrentSession": False,
                    "_missingMark": False,
                    "_midPrices": set(),
                },
            )
            group["openOrderCount"] += 1
            group["remainingContractsUnits"] += int(order["remaining_count_units"] or 0)
            group["initialContractsUnits"] += int(order["initial_count_units"] or 0)
            group["filledContractsUnits"] += int(order["fill_count_units"] or 0)
            created = int(order["created_at_ms"]) if order["created_at_ms"] is not None else None
            updated = int(order["updated_at_ms"]) if order["updated_at_ms"] is not None else created
            if created is not None:
                all_created.append(created)
                group["firstCreatedAtMs"] = created if group["firstCreatedAtMs"] is None else min(group["firstCreatedAtMs"], created)
                if active_start is not None and created >= active_start:
                    group["runningInCurrentSession"] = True
            if updated is not None:
                group["lastUpdatedAtMs"] = updated if group["lastUpdatedAtMs"] is None else max(group["lastUpdatedAtMs"], updated)
            breakdown = group["sideBreakdown"].setdefault(
                side, {"side": side, "openOrderCount": 0, "remainingContractsUnits": 0, "midPriceUnits": mark.get("midPriceUnits"), "marketValueUnits": 0}
            )
            breakdown["openOrderCount"] += 1
            breakdown["remainingContractsUnits"] += int(order["remaining_count_units"] or 0)
            mid = mark.get("midPriceUnits")
            value = _value_units(int(order["remaining_count_units"] or 0), int(mid) if isinstance(mid, (int, float)) else None)
            if value is None:
                group["_missingMark"] = True
                breakdown["marketValueUnits"] = None
            else:
                group["totalMarketValueUnits"] += value
                if breakdown["marketValueUnits"] is not None:
                    breakdown["marketValueUnits"] += value
            if isinstance(mid, (int, float)):
                group["_midPrices"].add(int(mid))
            for fill in fills_by_order.get(str(order["order_id"]), []):
                group["totalFillCount"] += 1
                fill_at = int(fill["created_at_ms"]) if fill["created_at_ms"] is not None else None
                order_at = int(fill["order_created_at_ms"]) if fill["order_created_at_ms"] is not None else None
                if fill_at is not None:
                    all_fill_times.append(fill_at)
                if fill_at is not None and order_at is not None:
                    all_latencies.append(max(0, fill_at - order_at))
        items = []
        for group in groups.values():
            if group["firstCreatedAtMs"] is not None:
                group["totalTimeOnBookMs"] = max(0, generated_at - group["firstCreatedAtMs"])
            if group.pop("_missingMark"):
                group["totalMarketValueUnits"] = None
            mid_prices = group.pop("_midPrices")
            group["midPriceUnits"] = next(iter(mid_prices)) if len(mid_prices) == 1 else None
            group["sideBreakdown"] = sorted(group["sideBreakdown"].values(), key=lambda item: item["side"])
            items.append(group)
        items.sort(key=lambda item: (item["firstCreatedAtMs"] is None, item["firstCreatedAtMs"] or 0))
        all_created.sort()
        gaps = [later - earlier for earlier, later in zip(all_created, all_created[1:])]
        values = [item["totalMarketValueUnits"] for item in items]
        total_market_value = sum(values) if all(value is not None for value in values) else None
        warnings = list(current.get("warnings") or [])
        if total_market_value is None and items:
            warnings.append("one or more open-order market values are unavailable")
        return {
            "generatedAt": generated_at,
            "snapshotAtMs": snapshot_at_ms,
            "source": self._source(snapshot_at_ms),
            "coverage": {"startedAtMs": self._coverage_started_at()},
            "summary": {
                "totalOpenOrders": len(open_orders),
                "ordersAttempted": sum(attempted_by_market.values()),
                "lastOrderAtMs": max(all_created, default=None),
                "averageTimeBetweenOrdersMs": int(round(sum(gaps) / len(gaps))) if gaps else None,
                "lastFillAtMs": max(all_fill_times, default=None),
                "averageFillTimeMs": int(round(sum(all_latencies) / len(all_latencies))) if all_latencies else None,
                "fillSampleSize": len(all_latencies),
                "totalMarketValueUnits": total_market_value,
            },
            "items": items,
            "warnings": list(dict.fromkeys(warnings)),
        }
