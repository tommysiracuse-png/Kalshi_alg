"""Reusable P&L loading and aggregation for the CLI and operations API."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


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


def compute_pnl(fills: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    tickers: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "category": "other", "fills": 0, "yes_qty": 0.0, "no_qty": 0.0,
        "yes_cost_c": 0.0, "no_cost_c": 0.0, "fees_c": 0.0,
        "last_fair_c": None, "last_net_pos": 0.0, "first_ts": None, "last_ts": None,
    })
    for fill in fills:
        ticker = str(fill.get("ticker") or "").strip()
        if not ticker:
            continue
        row = tickers[ticker]
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
        if fill.get("side") == "yes":
            row["yes_qty"] += qty
            row["yes_cost_c"] += float(price_c) * qty
        elif fill.get("side") == "no":
            row["no_qty"] += qty
            row["no_cost_c"] += float(price_c) * qty
    return dict(tickers)


def summarize_pnl(fills: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = compute_pnl(fills)
    totals = {"fills": 0, "feesCents": 0.0, "realizedCents": 0.0, "unrealizedCents": 0.0}
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
            "ticker": ticker, "category": row["category"], "fills": row["fills"],
            "netPosition": position, "feesCents": round(row["fees_c"], 4),
            "realizedCents": round(realized, 4), "unrealizedCents": round(unrealized, 4),
            "totalCents": round(realized + unrealized, 4), "lastFairCents": fair,
            "firstTimestamp": row["first_ts"], "lastTimestamp": row["last_ts"],
        }
        output.append(item)
        totals["fills"] += row["fills"]
        totals["feesCents"] += row["fees_c"]
        totals["realizedCents"] += realized
        totals["unrealizedCents"] += unrealized
    totals = {key: round(value, 4) if isinstance(value, float) else value for key, value in totals.items()}
    totals["totalCents"] = round(totals["realizedCents"] + totals["unrealizedCents"], 4)
    return {"totals": totals, "tickers": sorted(output, key=lambda item: item["ticker"])}
