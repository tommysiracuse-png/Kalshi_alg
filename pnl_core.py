"""Reusable P&L loading and aggregation for the CLI and operations API.

Two fill sources feed the same aggregation:

* ``pnl_tracker.jsonl`` written by the legacy single-market V1 bot
  (``load_fills``), one JSON object per fill with cents/contract fields.
* per-shard ``telemetry.sqlite3`` written by the sharded fleet runtime
  (``load_telemetry_fills``), whose ``fills`` rows are fixed-point units and
  are converted here into the exact same record shape.

Telemetry fills carry no explicit buy/sell column: the fleet's only sell path
is the watchdog flatten order (``action="sell"`` on the held side, client id
``wd:<side>:...``), and the fill row records ``side`` as the held side. The
action is therefore inferred from the inventory change around the fill
(``inventory_before_units`` -> ``inventory_after_units``) and, when that is
unavailable, from the ``wd:`` client order id in ``order_revisions``.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

# Fixed-point scales used by the fleet telemetry (see top_of_book_bot.py).
PRICE_SCALE = 10_000          # 1.0000 dollars == 10,000 price units
COUNT_SCALE = 100             # 1.00 contracts == 100 count units
PRICE_UNITS_PER_CENT = PRICE_SCALE // 100
DEFAULT_MARK_HORIZON_MS = 30_000
WATCHDOG_CLIENT_ORDER_PREFIX = "wd:"

_TELEMETRY_FILE = "telemetry.sqlite3"


def load_fills(path: Path, *, since_ms: Optional[int] = None) -> tuple[List[Dict[str, Any]], List[str]]:
    fills: List[Dict[str, Any]] = []
    warnings: List[str] = []
    if not path.exists():
        return fills, [f"fill data not found: {path.name}"]
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                text = line.strip()
                if not text:
                    continue
                try:
                    item = json.loads(text)
                    if not isinstance(item, dict):
                        raise ValueError("record is not an object")
                    ts = item.get("ts_ms", item.get("ts"))
                    if since_ms is not None:
                        timestamp_ms: Optional[int] = None
                        if isinstance(ts, (int, float)):
                            timestamp_ms = int(ts)
                        elif isinstance(ts, str):
                            try:
                                timestamp_ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
                            except ValueError:
                                pass
                        if timestamp_ms is not None and timestamp_ms < since_ms:
                            continue
                    fills.append(item)
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    warnings.append(f"malformed fill line {line_number}: {exc}")
    except OSError as exc:
        warnings.append(f"could not read fill data: {exc}")
    return fills, warnings


def find_telemetry_databases(artifact: Path) -> List[Path]:
    """Return every per-shard (or per-market) telemetry database under a run artifact.

    Shard databases (``shards/<worker-id>/telemetry.sqlite3``) come first in
    worker order; per-market databases are included for runs that predate
    sharding. Paths are returned in a stable order so merged fills are
    deterministic.
    """
    found: List[Path] = []
    for pattern in (f"shards/*/{_TELEMETRY_FILE}", f"markets/*/{_TELEMETRY_FILE}"):
        try:
            found.extend(sorted(path for path in artifact.glob(pattern) if path.is_file()))
        except OSError:
            continue
    return found


def _units_to_cents(value: Any) -> Optional[float]:
    if value is None:
        return None
    return float(value) / PRICE_UNITS_PER_CENT


def _units_to_contracts(value: Any) -> float:
    return float(value or 0) / COUNT_SCALE


def _iso_timestamp(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _mark_for_fill(row: sqlite3.Row) -> tuple[Optional[float], Optional[str]]:
    """Pick the YES mark (cents) used for unrealized P&L, best source first."""
    mid = row["mark_mid_units"]
    if mid is not None:
        return _units_to_cents(mid), f"markout_{int(row['mark_horizon_ms'])}"
    fair = row["fair_yes_before_units"]
    if fair is not None:
        return _units_to_cents(fair), "fair_before_fill"
    yes_bid, no_bid = row["best_yes_bid_units"], row["best_no_bid_units"]
    if yes_bid is not None and no_bid is not None:
        return _units_to_cents((int(yes_bid) + (PRICE_SCALE - int(no_bid))) / 2), "book_mid"
    return None, None


def classify_fill_action(
    side: str,
    size_units: Any,
    inventory_before_units: Any,
    inventory_after_units: Any,
    client_order_id: Any = None,
) -> tuple[str, str]:
    """Return ``(action, source)`` for a telemetry fill: ``buy`` or ``sell``.

    A buy on ``side`` moves the signed YES inventory by ``+size`` (YES) or
    ``-size`` (NO); a watchdog flatten sell on the held side moves it by the
    exact opposite. When the inventory columns are missing or inconsistent the
    ``wd:`` client order id of the originating order decides; failing that the
    fill is assumed to be a buy (the only other order type the fleet places).
    """
    try:
        size = int(size_units or 0)
        before = int(inventory_before_units) if inventory_before_units is not None else None
        after = int(inventory_after_units) if inventory_after_units is not None else None
    except (TypeError, ValueError):
        size, before, after = 0, None, None
    if size > 0 and before is not None and after is not None:
        expected = size if side == "yes" else -size
        delta = after - before
        if delta == expected:
            return "buy", "inventory"
        if delta == -expected:
            return "sell", "inventory"
    if isinstance(client_order_id, str) and client_order_id.startswith(WATCHDOG_CLIENT_ORDER_PREFIX):
        return "sell", "order_revision"
    if client_order_id:
        return "buy", "order_revision"
    return "buy", "assumed"


def _telemetry_fill_record(row: sqlite3.Row, shard: str) -> Dict[str, Any]:
    ts_ms = int(row["ts_ms"])
    fair_c, mark_source = _mark_for_fill(row)
    side = str(row["side"] or "")
    keys = row.keys()
    before_units = row["inventory_before_units"] if "inventory_before_units" in keys else None
    client_order_id = row["watchdog_client_order_id"] if "watchdog_client_order_id" in keys else None
    action, action_source = classify_fill_action(side, row["size_units"], before_units, row["inventory_after_units"], client_order_id)
    return {
        "ts": _iso_timestamp(ts_ms),
        "ts_ms": ts_ms,
        "ticker": str(row["ticker"] or ""),
        "category": "other",
        "side": side,
        "action": action,
        "action_source": action_source,
        "price_c": _units_to_cents(row["price_units"]),
        "qty": _units_to_contracts(row["size_units"]),
        "fee_c": _units_to_cents(row["fee_units"]) if row["fee_units"] is not None else 0.0,
        "net_pos_before": _units_to_contracts(before_units) if before_units is not None else None,
        "net_pos": _units_to_contracts(row["inventory_after_units"]),
        "fair_c": fair_c,
        "fair_before_c": _units_to_cents(row["fair_yes_before_units"]),
        "mark_source": mark_source,
        "fill_key": row["fill_key"],
        "trade_id": row["trade_id"],
        "order_id": row["order_id"],
        "is_taker": bool(row["is_taker"]),
        "shard": shard,
    }


_TELEMETRY_FILLS_SQL = """
    SELECT f.id, f.fill_key, f.ts_ms, f.ticker, f.side, f.trade_id, f.order_id,
           f.price_units, f.size_units, f.fee_units, f.is_taker,
           f.inventory_before_units, f.inventory_after_units, f.fair_yes_before_units,
           f.best_yes_bid_units, f.best_no_bid_units,
           ? AS mark_horizon_ms,
           {mark_column} AS mark_mid_units,
           {client_order_column} AS watchdog_client_order_id
    FROM fills f
    WHERE (? IS NULL OR f.ts_ms >= ?)
    ORDER BY f.ts_ms, f.id
"""
_MARKOUT_SUBQUERY = """(SELECT m.future_mid_yes_units FROM markouts m
        WHERE m.fill_key = f.fill_key AND m.horizon_ms = ? AND m.future_mid_yes_units IS NOT NULL
        ORDER BY m.id DESC LIMIT 1)"""
_CLIENT_ORDER_SUBQUERY = """(SELECT r.client_order_id FROM order_revisions r
        WHERE r.order_id = f.order_id AND r.client_order_id IS NOT NULL
        ORDER BY r.id LIMIT 1)"""


def _shard_label(path: Path) -> str:
    return path.parent.name or str(path)


def _read_telemetry_fills(path: Path, *, since_ms: Optional[int], mark_horizon_ms: int) -> List[Dict[str, Any]]:
    uri = f"file:{path.as_posix()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=0.25)) as db:
        db.row_factory = sqlite3.Row
        tables = {str(row[0]) for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "fills" not in tables:
            return []
        fill_columns = {str(row[1]) for row in db.execute("PRAGMA table_info(fills)")}
        params: List[Any] = [int(mark_horizon_ms)]
        if "markouts" in tables:
            mark_column = _MARKOUT_SUBQUERY
            params.append(int(mark_horizon_ms))
        else:
            mark_column = "NULL"
        client_order_column = _CLIENT_ORDER_SUBQUERY if "order_revisions" in tables else "NULL"
        sql = _TELEMETRY_FILLS_SQL.format(mark_column=mark_column, client_order_column=client_order_column)
        if "inventory_before_units" not in fill_columns:
            sql = sql.replace("f.inventory_before_units,", "NULL AS inventory_before_units,")
        params.extend([since_ms, since_ms])
        shard = _shard_label(path)
        return [_telemetry_fill_record(row, shard) for row in db.execute(sql, tuple(params))]


def load_telemetry_fills(
    source: Union[Path, Sequence[Path]],
    *,
    since_ms: Optional[int] = None,
    mark_horizon_ms: int = DEFAULT_MARK_HORIZON_MS,
) -> tuple[List[Dict[str, Any]], List[str]]:
    """Read fleet telemetry fills and return records shaped like ``load_fills`` output.

    ``source`` is a run artifact directory (its shard databases are discovered)
    or an explicit list of ``telemetry.sqlite3`` paths. Fills are merged across
    shards in timestamp order and de-duplicated by ``fill_key`` so a market that
    moved between workers is not double counted. Units are converted exactly:
    price/fee units -> cents (10,000 units per dollar), size/inventory units ->
    contracts (100 units per contract). ``fair_c`` is the YES mark taken from the
    ``mark_horizon_ms`` markout mid when recorded, else the pre-fill fair value,
    else the book midpoint. Each record also carries ``action`` (``buy`` or
    ``sell``, see ``classify_fill_action``) and ``net_pos_before`` so the
    aggregation can close positions and seed a carried-in cost basis.
    """
    if isinstance(source, Path):
        paths = find_telemetry_databases(source)
        if not paths:
            return [], [f"fill data not found: no {_TELEMETRY_FILE} under {source.name}"]
    else:
        paths = [Path(item) for item in source]
    since = int(since_ms) if since_ms is not None else None
    merged: List[Dict[str, Any]] = []
    warnings: List[str] = []
    for index, path in enumerate(paths):
        try:
            records = _read_telemetry_fills(path, since_ms=since, mark_horizon_ms=mark_horizon_ms)
        except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
            warnings.append(f"could not read telemetry fills from {_shard_label(path)}: {exc}")
            continue
        for position, record in enumerate(records):
            record["_order"] = (record["ts_ms"], index, position)
            merged.append(record)
    merged.sort(key=lambda item: item["_order"])
    fills: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for record in merged:
        record.pop("_order", None)
        key = record.get("fill_key")
        if key:
            if key in seen:
                continue
            seen.add(key)
        fills.append(record)
    return fills, warnings


def _seed_yes_price_c(fill: Dict[str, Any]) -> Optional[float]:
    """YES-equivalent price (cents) used to value inventory carried into scope."""
    fair_before = fill.get("fair_before_c")
    if fair_before is not None:
        return float(fair_before)
    price_c = fill.get("price_c")
    if price_c is None:
        return None
    return float(price_c) if fill.get("side") == "yes" else 100.0 - float(price_c)


def compute_pnl(fills: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Aggregate fills per ticker into matched-pair cost-basis rows.

    Buys add to that side's quantity and cost. A ``sell`` (watchdog flatten of
    the held side) is booked as the economically identical buy of the opposite
    side at ``100 - price``: selling YES at ``p`` closes the pair just like
    buying NO at ``100 - p`` and realizes ``p - avg_yes`` per contract.

    When a fill's ``net_pos_before`` is not the inventory the fills seen so far
    account for -- the first fill in scope starts from non-zero inventory
    (position carried over from an earlier run, or a time window that cuts the
    history), or a later fill reveals an inventory change no fill row recorded
    (position resync) -- the missing contracts are seeded into the basis at the
    fill's pre-fill fair YES value (else the fill's YES-equivalent price) and
    the row is flagged ``basis_estimated`` so the summary can say so. This
    keeps ``yes_qty - no_qty`` equal to the exchange position, which the
    matched-pair realized/unrealized split relies on.
    """
    tickers: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "category": "other", "fills": 0, "sells": 0, "yes_qty": 0.0, "no_qty": 0.0,
        "yes_cost_c": 0.0, "no_cost_c": 0.0, "fees_c": 0.0,
        "last_fair_c": None, "last_net_pos": 0.0, "first_ts": None, "last_ts": None,
        "basis_estimated": False, "basis_seed_qty": 0.0, "basis_seed_c": None, "basis_gaps": 0,
    })
    for fill in fills:
        ticker = str(fill.get("ticker") or "").strip()
        if not ticker:
            continue
        row = tickers[ticker]
        before = fill.get("net_pos_before")
        if before is not None:
            accounted = 0.0 if row["fills"] == 0 else row["last_net_pos"]
            missing = float(before) - accounted
            seed_c = _seed_yes_price_c(fill) if abs(missing) >= 0.005 else None
            if seed_c is not None:
                if missing > 0:
                    row["yes_qty"] += missing
                    row["yes_cost_c"] += missing * seed_c
                else:
                    row["no_qty"] += abs(missing)
                    row["no_cost_c"] += abs(missing) * (100.0 - seed_c)
                row["basis_estimated"] = True
                row["basis_seed_qty"] += missing
                row["basis_seed_c"] = seed_c if row["basis_seed_c"] is None else row["basis_seed_c"]
                row["basis_gaps"] += 1 if row["fills"] else 0
        row["category"] = fill.get("category", "other")
        row["fills"] += 1
        qty = float(fill.get("qty") or 0)
        price_c = fill.get("price_c")
        row["fees_c"] += float(fill.get("fee_c") or 0)
        row["last_net_pos"] = float(fill.get("net_pos") or 0)
        if fill.get("fair_c") is not None:
            row["last_fair_c"] = float(fill["fair_c"])
        row["first_ts"] = row["first_ts"] if row["first_ts"] is not None else fill.get("ts")
        row["last_ts"] = fill.get("ts")
        if price_c is None:
            continue
        side = fill.get("side")
        if side not in {"yes", "no"}:
            continue
        booked_price = float(price_c)
        if fill.get("action") == "sell":
            row["sells"] += 1
            side = "no" if side == "yes" else "yes"
            booked_price = 100.0 - booked_price
        if side == "yes":
            row["yes_qty"] += qty
            row["yes_cost_c"] += booked_price * qty
        else:
            row["no_qty"] += qty
            row["no_cost_c"] += booked_price * qty
    return dict(tickers)


def summarize_pnl(fills: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = compute_pnl(fills)
    totals = {"fills": 0, "sellFills": 0, "basisEstimatedTickers": 0, "feesCents": 0.0, "realizedCents": 0.0, "unrealizedCents": 0.0}
    output: List[Dict[str, Any]] = []
    for ticker, row in rows.items():
        matched = min(row["yes_qty"], row["no_qty"])
        avg_yes = row["yes_cost_c"] / row["yes_qty"] if row["yes_qty"] else 0.0
        avg_no = row["no_cost_c"] / row["no_qty"] if row["no_qty"] else 0.0
        realized = matched * (100.0 - avg_yes - avg_no) - row["fees_c"] if matched else -row["fees_c"]
        unrealized = 0.0
        position = row["last_net_pos"]
        fair = row["last_fair_c"]
        if position > 0 and fair is not None:
            unrealized = position * (fair - avg_yes)
        elif position < 0 and fair is not None:
            unrealized = abs(position) * ((100.0 - fair) - avg_no)
        item = {
            "ticker": ticker, "category": row["category"], "fills": row["fills"], "sellFills": row["sells"],
            "netPosition": position, "feesCents": round(row["fees_c"], 4),
            "realizedCents": round(realized, 4), "unrealizedCents": round(unrealized, 4),
            "totalCents": round(realized + unrealized, 4), "lastFairCents": fair,
            "basisEstimated": bool(row["basis_estimated"]),
            "basisSeedContracts": round(row["basis_seed_qty"], 4) if row["basis_estimated"] else 0.0,
            "basisSeedCents": round(row["basis_seed_c"], 4) if row["basis_estimated"] and row["basis_seed_c"] is not None else None,
            "basisGapFills": int(row["basis_gaps"]),
            "firstTimestamp": row["first_ts"], "lastTimestamp": row["last_ts"],
        }
        output.append(item)
        totals["fills"] += row["fills"]
        totals["sellFills"] += row["sells"]
        totals["basisEstimatedTickers"] += 1 if row["basis_estimated"] else 0
        totals["feesCents"] += row["fees_c"]
        totals["realizedCents"] += realized
        totals["unrealizedCents"] += unrealized
    totals = {key: round(value, 4) if isinstance(value, float) else value for key, value in totals.items()}
    totals["totalCents"] = round(totals["realizedCents"] + totals["unrealizedCents"], 4)
    return {"totals": totals, "tickers": sorted(output, key=lambda item: item["ticker"])}
