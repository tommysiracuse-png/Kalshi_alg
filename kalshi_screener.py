#!/usr/bin/env python3
"""Kalshi screener for terminal usage.

This version keeps the script public-only and CSV-based, but changes the
selection logic from "paired quote edge" to a single-market EV screen that is
much closer to the top-of-book bot.

What it does:
- pulls public Kalshi markets
- applies your broad liquidity / time filters
- estimates a fair YES price from public top-of-book + last price
- scores YES and NO candidate bid levels using a simplified EV formula
- keeps only markets where at least one side clears the EV hurdle
- writes a CSV that still preserves the old key columns so downstream tooling
  can ingest it, while adding side-by-side EV detail columns

What it does NOT do:
- no auth
- no order placement
- no approvals
- no private inventory / telemetry unless you pass a net-position override

Examples:
    python kalshi_screener_ev.py
    python kalshi_screener_ev.py --output my_screener.csv
    python kalshi_screener_ev.py --top-n 50
    python kalshi_screener_ev.py --run-forever --cycle-seconds 60
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

import kalshi_screener_config as config

D100 = Decimal("100")
D1 = Decimal("1")


def cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso8601(ts: Any) -> Optional[datetime]:
    if not isinstance(ts, str) or not ts:
        return None
    s = ts.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fmt_utc(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_decimal(val: Any) -> Optional[Decimal]:
    if val is None:
        return None
    if isinstance(val, Decimal):
        return val
    if isinstance(val, (int, float)):
        return Decimal(str(val))
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return Decimal(s)
        except InvalidOperation:
            return None
    return None


def round_cents(d: Decimal) -> int:
    return int(d.to_integral_value(rounding=ROUND_HALF_UP))


def clamp(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))


def floor_to_tick(p: int, tick: int) -> int:
    return (p // tick) * tick


def ceil_to_tick_int(p: int, tick: int) -> int:
    return ((p + tick - 1) // tick) * tick


def valid_price(p: Optional[int]) -> bool:
    return p is not None and 1 <= int(p) <= 99


def get_price_cents(mkt: Dict[str, Any], dollars_field: str, cents_field: str) -> Optional[int]:
    d = parse_decimal(mkt.get(dollars_field))
    if d is not None:
        return round_cents(d * D100)
    c = mkt.get(cents_field)
    if isinstance(c, (int, float)):
        try:
            return int(c)
        except Exception:
            return None
    return None


def get_count_float(mkt: Dict[str, Any], fp_field: str, int_field: str) -> float:
    v = mkt.get(fp_field)
    if v is not None:
        d = parse_decimal(v)
        if d is not None:
            return float(d)
    v2 = mkt.get(int_field)
    if isinstance(v2, (int, float)):
        return float(v2)
    return 0.0


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
    max_retries: int = 4,
    backoff: float = 0.6,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            response = session.request(method, url, params=params, timeout=timeout)
            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * (2 * attempt))
                continue
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_err = exc
            time.sleep(backoff * (2 ** attempt))
    raise RuntimeError(f"HTTP failed after retries: {url}\nLast error: {last_err}")


class KalshiPublicClient:
    def __init__(self, host: str, api_prefix: str):
        self.host = host.rstrip("/")
        self.api_prefix = api_prefix
        self.session = requests.Session()

    def list_markets(
        self,
        *,
        status: str,
        limit: int,
        max_total: int,
        mve_filter: Optional[str],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        cursor = None
        while True:
            params: Dict[str, Any] = {"limit": int(limit)}
            if status:
                params["status"] = status
            if cursor:
                params["cursor"] = cursor
            if mve_filter:
                params["mve_filter"] = mve_filter

            url = f"{self.host}{self.api_prefix}/markets"
            data = request_json(self.session, "GET", url, params=params)
            out.extend(data.get("markets") or [])

            if len(out) >= max_total:
                return out[:max_total]

            cursor = data.get("cursor")
            if not cursor:
                return out


def compute_cutoff_times(mkt: Dict[str, Any]) -> Tuple[Optional[datetime], str]:
    close_dt = parse_iso8601(mkt.get("close_time"))
    expected_exp_dt = parse_iso8601(mkt.get("expected_expiration_time"))
    exp_dt = parse_iso8601(mkt.get("expiration_time"))

    candidates: List[Tuple[str, datetime]] = []
    if close_dt is not None:
        candidates.append(("close_time", close_dt))
    if expected_exp_dt is not None:
        candidates.append(("expected_expiration_time", expected_exp_dt))
    if exp_dt is not None:
        candidates.append(("expiration_time", exp_dt))

    if not candidates:
        return None, ""

    cutoff_field, cutoff_dt = min(candidates, key=lambda x: x[1])
    return cutoff_dt, cutoff_field


def get_tick_cents(mkt: Dict[str, Any], default_tick_cents: int) -> int:
    ts = mkt.get("tick_size")
    if isinstance(ts, (int, float)) and ts > 0:
        return max(1, int(ts))

    price_ranges = mkt.get("price_ranges")
    if isinstance(price_ranges, list) and price_ranges:
        yb_d = (
            parse_decimal(mkt.get("yes_bid_dollars"))
            or parse_decimal(mkt.get("last_price_dollars"))
            or Decimal("0.5000")
        )
        chosen_step = None
        for rng in price_ranges:
            try:
                start = parse_decimal(rng.get("start"))
                end = parse_decimal(rng.get("end"))
                step = parse_decimal(rng.get("step"))
                if start is None or end is None or step is None:
                    continue
                if start <= yb_d <= end:
                    chosen_step = step
                    break
            except Exception:
                continue
        if chosen_step is None:
            for rng in price_ranges:
                step = parse_decimal(rng.get("step"))
                if step is not None:
                    chosen_step = step
                    break
        if chosen_step is not None:
            tick_c = round_cents(chosen_step * D100)
            return max(1, tick_c)

    return max(1, int(default_tick_cents))


def compute_top_of_book_cents(
    mkt: Dict[str, Any],
) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    yes_bid = get_price_cents(mkt, "yes_bid_dollars", "yes_bid")
    no_bid = get_price_cents(mkt, "no_bid_dollars", "no_bid")

    yes_ask_explicit = get_price_cents(mkt, "yes_ask_dollars", "yes_ask")
    no_ask_explicit = get_price_cents(mkt, "no_ask_dollars", "no_ask")

    yes_ask_implied = (100 - no_bid) if isinstance(no_bid, int) else None
    no_ask_implied = (100 - yes_bid) if isinstance(yes_bid, int) else None

    def pick_best_ask(a: Optional[int], b: Optional[int]) -> Optional[int]:
        vals = [x for x in (a, b) if isinstance(x, int)]
        return min(vals) if vals else None

    yes_ask = pick_best_ask(yes_ask_explicit, yes_ask_implied)
    no_ask = pick_best_ask(no_ask_explicit, no_ask_implied)
    return yes_bid, yes_ask, no_bid, no_ask


def extract_book_snapshot(mkt: Dict[str, Any], default_tick_cents: int) -> Optional[Dict[str, Any]]:
    tick_c = get_tick_cents(mkt, default_tick_cents)
    yes_bid, yes_ask, no_bid, no_ask = compute_top_of_book_cents(mkt)

    if not (valid_price(yes_bid) and valid_price(no_bid)):
        return None

    yes_bid = floor_to_tick(int(yes_bid), tick_c)
    no_bid = floor_to_tick(int(no_bid), tick_c)

    if valid_price(yes_ask):
        yes_ask = ceil_to_tick_int(int(yes_ask), tick_c)
    else:
        yes_ask = None

    if valid_price(no_ask):
        no_ask = ceil_to_tick_int(int(no_ask), tick_c)
    else:
        no_ask = None

    if yes_ask is None:
        yes_ask = 100 - no_bid
    if no_ask is None:
        no_ask = 100 - yes_bid

    if not (valid_price(yes_ask) and valid_price(no_ask)):
        return None

    spread_c = 100 - (yes_bid + no_bid)
    if spread_c <= 0:
        return None

    yes_bid_size = get_count_float(mkt, "yes_bid_size_fp", "yes_bid_size")
    no_bid_size = get_count_float(mkt, "no_bid_size_fp", "no_bid_size")
    yes_ask_size = get_count_float(mkt, "yes_ask_size_fp", "yes_ask_size")
    no_ask_size = get_count_float(mkt, "no_ask_size_fp", "no_ask_size")

    if yes_ask_size <= 0 and no_bid_size > 0:
        yes_ask_size = no_bid_size
    if no_ask_size <= 0 and yes_bid_size > 0:
        no_ask_size = yes_bid_size

    return {
        "tick_c": int(tick_c),
        "yes_bid_c": int(yes_bid),
        "yes_ask_c": int(yes_ask),
        "no_bid_c": int(no_bid),
        "no_ask_c": int(no_ask),
        "spread_c": int(spread_c),
        "yes_side_spread_c": int(max(0, int(yes_ask) - int(yes_bid))),
        "no_side_spread_c": int(max(0, int(no_ask) - int(no_bid))),
        "yes_bid_size": float(yes_bid_size),
        "yes_ask_size": float(yes_ask_size),
        "no_bid_size": float(no_bid_size),
        "no_ask_size": float(no_ask_size),
    }


def market_passes_safety_filters(mkt: Dict[str, Any], book: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    spread_c = int(book["spread_c"])
    yes_bid = int(book["yes_bid_c"])
    no_bid = int(book["no_bid_c"])

    if yes_bid < int(settings["min_yes_bid_cents"]):
        return False
    if no_bid < int(settings["min_no_bid_cents"]):
        return False
    if spread_c < int(settings["min_spread_cents"]):
        return False

    max_spread_cents = settings["max_spread_cents"]
    if max_spread_cents is not None and spread_c > int(max_spread_cents):
        return False

    vol24h = get_count_float(mkt, "volume_24h_fp", "volume_24h")
    oi = get_count_float(mkt, "open_interest_fp", "open_interest")
    if vol24h < float(settings["min_vol24h"]) or oi < float(settings["min_oi"]):
        return False

    cutoff_dt, _ = compute_cutoff_times(mkt)
    if cutoff_dt is None:
        return False

    hrs_to_cutoff = (cutoff_dt - now_utc()).total_seconds() / 3600.0
    if hrs_to_cutoff < float(settings["min_time_to_close_hrs"]):
        return False

    max_time_to_close_hrs = settings["max_time_to_close_hrs"]
    if max_time_to_close_hrs is not None and hrs_to_cutoff > float(max_time_to_close_hrs):
        return False

    return True


def estimate_maker_fee_cents(price_c: int, fee_factor: float) -> float:
    p = Decimal(price_c) / D100
    baseline_fee_cents = Decimal("7.0") * p * (D1 - p)
    return float(baseline_fee_cents * Decimal(str(fee_factor)))


def estimate_orderbook_imbalance(book: Dict[str, Any]) -> float:
    yes_bid_size = float(book.get("yes_bid_size") or 0.0)
    yes_ask_size = float(book.get("yes_ask_size") or 0.0)
    denom = yes_bid_size + yes_ask_size
    if denom <= 0:
        return 0.0
    return max(-1.0, min(1.0, (yes_bid_size - yes_ask_size) / denom))


def estimate_fair_yes_cents(
    mkt: Dict[str, Any],
    book: Dict[str, Any],
    settings: Dict[str, Any],
) -> int:
    mid_yes = (int(book["yes_bid_c"]) + int(book["yes_ask_c"])) / 2.0
    last_yes = get_price_cents(mkt, "last_price_dollars", "last_price")

    anchors: List[Tuple[float, float]] = []
    mid_w = float(settings["fair_value_mid_weight"])
    last_w = float(settings["fair_value_last_weight"])

    if mid_w > 0:
        anchors.append((mid_yes, mid_w))
    if last_yes is not None and last_w > 0:
        anchors.append((float(last_yes), last_w))

    if not anchors:
        raw_fair_yes = mid_yes
    else:
        total_w = sum(weight for _, weight in anchors)
        raw_fair_yes = sum(value * weight for value, weight in anchors) / max(1e-9, total_w)

    imbalance = estimate_orderbook_imbalance(book)
    adjust_c = imbalance * float(settings["fair_value_max_orderbook_imbalance_adjust_cents"])
    fair_yes = int(round(raw_fair_yes + adjust_c))

    tick = int(book["tick_c"])
    fair_yes = clamp(fair_yes, tick, 99)
    fair_yes = floor_to_tick(fair_yes, tick)
    fair_yes = clamp(fair_yes, tick, 99)
    return fair_yes


def estimate_inventory_penalty_cents(side: str, settings: Dict[str, Any]) -> float:
    net_contracts = float(settings["net_position_contracts"])
    if abs(net_contracts) <= 0.0:
        return 0.0

    reducing_inventory = (net_contracts > 0 and side == "no") or (net_contracts < 0 and side == "yes")
    if reducing_inventory:
        return 0.0

    contracts_per_tick = max(1.0, float(settings["inventory_skew_contracts_per_tick"]))
    max_ticks = max(0.0, float(settings["maximum_inventory_skew_ticks"]))
    penalty_ticks = min(max_ticks, abs(net_contracts) / contracts_per_tick)
    return penalty_ticks * float(settings["tick_value_cents_for_inventory_penalty"])


def estimate_toxicity_cents(side: str, book: Dict[str, Any], settings: Dict[str, Any]) -> float:
    imbalance = estimate_orderbook_imbalance(book)
    adverse_imbalance = max(0.0, -imbalance) if side == "yes" else max(0.0, imbalance)
    return float(settings["default_toxicity_cents"]) + (adverse_imbalance * float(settings["imbalance_toxicity_extra_cents"]))


def estimate_queue_penalty_cents(queue_ahead_contracts: float, settings: Dict[str, Any]) -> float:
    if queue_ahead_contracts <= 0:
        return 0.0
    penalty = queue_ahead_contracts / max(1e-9, float(settings["queue_penalty_contracts_per_cent"]))
    return min(float(settings["queue_penalty_cap_cents"]), penalty)


def price_after_passive_offset(best_bid_c: int, tick_c: int, passive_offset_ticks: int) -> int:
    return max(tick_c, best_bid_c - (tick_c * max(0, passive_offset_ticks)))


def projected_queue_ahead_contracts(side: str, candidate_price_c: int, book: Dict[str, Any]) -> float:
    best_bid = int(book[f"{side}_bid_c"])
    best_bid_size = float(book.get(f"{side}_bid_size") or 0.0)
    if candidate_price_c < best_bid:
        return 0.0
    if candidate_price_c == best_bid:
        return best_bid_size
    return 0.0


def nominal_ev_dollars(ev_cents: float, nominal_size_contracts: int) -> float:
    return round((ev_cents / 100.0) * float(nominal_size_contracts), 4)


def scan_side_candidates(
    mkt: Dict[str, Any],
    book: Dict[str, Any],
    side: str,
    fair_yes_c: int,
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    tick_c = int(book["tick_c"])
    best_bid_c = int(book[f"{side}_bid_c"])
    ask_c = int(book[f"{side}_ask_c"])
    max_bid_c = ask_c - tick_c

    if max_bid_c < tick_c:
        return {
            "side": side,
            "pass": False,
            "reason": "no_safe_quote",
            "quote_c": None,
            "ev_c": None,
            "fair_side_c": float(fair_yes_c if side == "yes" else 100 - fair_yes_c),
            "fee_c": None,
            "tox_c": None,
            "queue_penalty_c": None,
            "inventory_penalty_c": None,
            "queue_ahead": None,
            "market_target_c": None,
            "candidate_debug": "",
            "nominal_ev_dollars": None,
        }

    market_target_c = price_after_passive_offset(
        best_bid_c=best_bid_c,
        tick_c=tick_c,
        passive_offset_ticks=int(settings["passive_offset_ticks_when_not_improving"]),
    )
    market_target_c = min(max_bid_c, floor_to_tick(market_target_c, tick_c))

    fair_side_c = float(fair_yes_c if side == "yes" else 100 - fair_yes_c)
    tox_c = estimate_toxicity_cents(side=side, book=book, settings=settings)
    inv_penalty_c = estimate_inventory_penalty_cents(side=side, settings=settings)
    min_edge_c = float(settings["minimum_expected_edge_cents_to_quote"])

    candidate_debug: List[str] = []
    best_candidate: Optional[Dict[str, Any]] = None
    candidate_price_c = market_target_c

    for _ in range(max(1, int(settings["candidate_price_levels_to_scan"]))):
        if candidate_price_c > max_bid_c:
            break

        queue_ahead = projected_queue_ahead_contracts(side=side, candidate_price_c=candidate_price_c, book=book)
        qpen_c = estimate_queue_penalty_cents(queue_ahead, settings)
        fee_c = estimate_maker_fee_cents(candidate_price_c, float(settings["maker_fee_factor"]))
        incentive_c = 0.0
        ev_c = fair_side_c - float(candidate_price_c) - fee_c - tox_c - inv_penalty_c - qpen_c + incentive_c
        passes = ev_c >= min_edge_c

        candidate_debug.append(
            "p={:.2f}|edge={:.2f}|fee={:.2f}|tox={:.2f}|inv={:.2f}|qpen={:.2f}|queue={:.2f}|status={}".format(
                candidate_price_c,
                ev_c,
                fee_c,
                tox_c,
                inv_penalty_c,
                qpen_c,
                queue_ahead,
                "ok" if passes else "edge_fail",
            )
        )

        if passes:
            score = ev_c
            candidate = {
                "side": side,
                "pass": True,
                "reason": "ev_quote",
                "quote_c": int(candidate_price_c),
                "ev_c": round(ev_c, 2),
                "fair_side_c": round(fair_side_c, 2),
                "fee_c": round(fee_c, 2),
                "tox_c": round(tox_c, 2),
                "queue_penalty_c": round(qpen_c, 2),
                "inventory_penalty_c": round(inv_penalty_c, 2),
                "queue_ahead": round(queue_ahead, 2),
                "market_target_c": int(market_target_c),
                "candidate_debug": "; ".join(candidate_debug),
                "nominal_ev_dollars": nominal_ev_dollars(ev_c, int(settings["quote_size"])),
                "score": score,
            }
            if best_candidate is None or score > best_candidate["score"]:
                best_candidate = candidate

        candidate_price_c += tick_c

    if best_candidate is not None:
        best_candidate.pop("score", None)
        return best_candidate

    return {
        "side": side,
        "pass": False,
        "reason": "negative_ev",
        "quote_c": None,
        "ev_c": None,
        "fair_side_c": round(fair_side_c, 2),
        "fee_c": None,
        "tox_c": round(tox_c, 2),
        "queue_penalty_c": None,
        "inventory_penalty_c": round(inv_penalty_c, 2),
        "queue_ahead": None,
        "market_target_c": int(market_target_c),
        "candidate_debug": "; ".join(candidate_debug),
        "nominal_ev_dollars": None,
    }


def build_market_row(mkt: Dict[str, Any], book: Dict[str, Any], settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cutoff_dt, cutoff_field = compute_cutoff_times(mkt)
    if cutoff_dt is None:
        return None

    fair_yes_c = estimate_fair_yes_cents(mkt, book, settings)
    fair_no_c = 100 - fair_yes_c

    yes_result = scan_side_candidates(mkt, book, "yes", fair_yes_c, settings)
    no_result = scan_side_candidates(mkt, book, "no", fair_yes_c, settings)

    if not yes_result["pass"] and not no_result["pass"]:
        return None

    best = yes_result
    if (not best["pass"]) or (no_result["pass"] and float(no_result["ev_c"] or -1e9) > float(best["ev_c"] or -1e9)):
        best = no_result

    vol24h = get_count_float(mkt, "volume_24h_fp", "volume_24h")
    oi = get_count_float(mkt, "open_interest_fp", "open_interest")
    hrs_to_cutoff = (cutoff_dt - now_utc()).total_seconds() / 3600.0

    title = str(mkt.get("title") or "")
    event_ticker = str(mkt.get("event_ticker") or "")
    series_guess = event_ticker.split("-")[0] if event_ticker and "-" in event_ticker else event_ticker
    series_url = f"https://kalshi.com/markets/{series_guess.lower()}" if series_guess else ""

    return {
        # keep old-ish ingestion-friendly columns first
        "Quoted edge(c)": best["ev_c"],
        "Quote YES bid(c)": yes_result["quote_c"],
        "Quote NO bid(c)": no_result["quote_c"],
        "Best side": str(best["side"]).upper(),
        "Best EV(c)": best["ev_c"],
        "Best nominal EV($)": best["nominal_ev_dollars"],
        "Best fair side(c)": best["fair_side_c"],
        "Best market target(c)": best["market_target_c"],
        "Best reason": best["reason"],
        "YES EV(c)": yes_result["ev_c"],
        "YES fair(c)": round(fair_yes_c, 2),
        "YES fee(c)": yes_result["fee_c"],
        "YES tox(c)": yes_result["tox_c"],
        "YES inv(c)": yes_result["inventory_penalty_c"],
        "YES qpen(c)": yes_result["queue_penalty_c"],
        "YES queue ahead": yes_result["queue_ahead"],
        "YES market target(c)": yes_result["market_target_c"],
        "YES reason": yes_result["reason"],
        "NO EV(c)": no_result["ev_c"],
        "NO fair(c)": round(fair_no_c, 2),
        "NO fee(c)": no_result["fee_c"],
        "NO tox(c)": no_result["tox_c"],
        "NO inv(c)": no_result["inventory_penalty_c"],
        "NO qpen(c)": no_result["queue_penalty_c"],
        "NO queue ahead": no_result["queue_ahead"],
        "NO market target(c)": no_result["market_target_c"],
        "NO reason": no_result["reason"],
        "Fair YES(c)": round(fair_yes_c, 2),
        "Fair NO(c)": round(fair_no_c, 2),
        "Spread(c)": int(book["spread_c"]),
        "YES bid(c)": int(book["yes_bid_c"]),
        "YES ask(c)": int(book["yes_ask_c"]),
        "NO bid(c)": int(book["no_bid_c"]),
        "NO ask(c)": int(book["no_ask_c"]),
        "YES bid size": round(float(book.get("yes_bid_size") or 0.0), 2),
        "YES ask size": round(float(book.get("yes_ask_size") or 0.0), 2),
        "NO bid size": round(float(book.get("no_bid_size") or 0.0), 2),
        "NO ask size": round(float(book.get("no_ask_size") or 0.0), 2),
        "Tick(c)": int(book["tick_c"]),
        "Vol24h": float(vol24h),
        "OI": float(oi),
        "Hrs->Cutoff": float(hrs_to_cutoff),
        "CutoffField": cutoff_field,
        "CutoffUTC": fmt_utc(cutoff_dt),
        "Ticker": str(mkt.get("ticker") or ""),
        "SeriesURL": series_url,
        "SearchText": title[:120],
        "YES candidate debug": yes_result["candidate_debug"],
        "NO candidate debug": no_result["candidate_debug"],
    }


def screen_markets(pub: KalshiPublicClient, settings: Dict[str, Any]) -> pd.DataFrame:
    markets = pub.list_markets(
        status=settings["status"],
        limit=1000,
        max_total=int(settings["max_markets_to_scan"]),
        mve_filter=settings["mve_filter"],
    )

    rows: List[Dict[str, Any]] = []
    for market in markets:
        ticker = market.get("ticker")
        if not ticker:
            continue

        book = extract_book_snapshot(market, settings["default_tick_cents"])
        if book is None:
            continue

        if not market_passes_safety_filters(market, book, settings):
            continue

        row = build_market_row(market, book, settings)
        if row is None:
            continue
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values(
        by=["Best EV(c)", "Best nominal EV($)", "Vol24h", "OI", "Hrs->Cutoff"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    return df


def build_export_dataframe(screen_df: pd.DataFrame, settings: Dict[str, Any]) -> pd.DataFrame:
    if screen_df.empty:
        return pd.DataFrame()
    return screen_df.head(int(settings["top_n"])).reset_index(drop=True)


def build_settings_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        # old scan filters
        "status": args.status,
        "mve_filter": args.mve_filter,
        "max_markets_to_scan": args.max_markets_to_scan,
        "top_n": args.top_n,
        "min_spread_cents": args.min_spread_cents,
        "max_spread_cents": args.max_spread_cents,
        "min_yes_bid_cents": args.min_yes_bid_cents,
        "min_no_bid_cents": args.min_no_bid_cents,
        "min_vol24h": args.min_vol24h,
        "min_oi": args.min_oi,
        "min_time_to_close_hrs": args.min_time_to_close_hrs,
        "max_time_to_close_hrs": args.max_time_to_close_hrs,
        "default_tick_cents": args.default_tick_cents,
        "quote_size": args.quote_size,
        # EV-specific settings
        "minimum_expected_edge_cents_to_quote": args.minimum_expected_edge_cents_to_quote,
        "default_toxicity_cents": args.default_toxicity_cents,
        "maker_fee_factor": args.maker_fee_factor,
        "fair_value_mid_weight": args.fair_value_mid_weight,
        "fair_value_last_weight": args.fair_value_last_weight,
        "fair_value_max_orderbook_imbalance_adjust_cents": args.fair_value_max_orderbook_imbalance_adjust_cents,
        "passive_offset_ticks_when_not_improving": args.passive_offset_ticks_when_not_improving,
        "candidate_price_levels_to_scan": args.candidate_price_levels_to_scan,
        "queue_penalty_cap_cents": args.queue_penalty_cap_cents,
        "queue_penalty_contracts_per_cent": args.queue_penalty_contracts_per_cent,
        "imbalance_toxicity_extra_cents": args.imbalance_toxicity_extra_cents,
        "net_position_contracts": args.net_position_contracts,
        "inventory_skew_contracts_per_tick": args.inventory_skew_contracts_per_tick,
        "maximum_inventory_skew_ticks": args.maximum_inventory_skew_ticks,
        "tick_value_cents_for_inventory_penalty": args.tick_value_cents_for_inventory_penalty,
    }


def run_once(args: argparse.Namespace) -> None:
    settings = build_settings_from_args(args)
    pub = KalshiPublicClient(host=args.host, api_prefix=args.api_prefix)
    screen_df = screen_markets(pub, settings)
    export_df = build_export_dataframe(screen_df, settings)
    export_df.to_csv(args.output, index=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Kalshi public EV screener and write a CSV file.")

    parser.add_argument("--host", default=config.KALSHI_HOST)
    parser.add_argument("--api-prefix", default=config.API_PREFIX)
    parser.add_argument("--output", default=config.OUTPUT_CSV)

    # broad scan settings kept from your current script / config
    parser.add_argument("--status", default=config.STATUS)
    parser.add_argument("--mve-filter", default=config.MVE_FILTER)
    parser.add_argument("--max-markets-to-scan", type=int, default=config.MAX_MARKETS_TO_SCAN)
    parser.add_argument("--top-n", type=int, default=config.TOP_N)

    parser.add_argument("--min-spread-cents", type=int, default=config.MIN_SPREAD_CENTS)
    parser.add_argument("--max-spread-cents", type=int, default=config.MAX_SPREAD_CENTS)
    parser.add_argument("--min-yes-bid-cents", type=int, default=config.MIN_YES_BID_CENTS)
    parser.add_argument("--min-no-bid-cents", type=int, default=config.MIN_NO_BID_CENTS)
    parser.add_argument("--min-vol24h", type=float, default=config.MIN_VOL24H)
    parser.add_argument("--min-oi", type=float, default=config.MIN_OI)
    parser.add_argument("--min-time-to-close-hrs", type=float, default=config.MIN_TIME_TO_CLOSE_HRS)
    parser.add_argument("--max-time-to-close-hrs", type=float, default=config.MAX_TIME_TO_CLOSE_HRS)
    parser.add_argument("--default-tick-cents", type=int, default=config.DEFAULT_TICK_CENTS)
    parser.add_argument("--quote-size", type=int, default=config.QUOTE_SIZE)

    # EV defaults are self-contained so config does not need to change
    parser.add_argument(
        "--minimum-expected-edge-cents-to-quote",
        type=float,
        default=float(cfg("MIN_EXPECTED_EDGE_CENTS_TO_QUOTE", max(2, cfg("TARGET_EDGE_CENTS", 4) // 2))),
    )
    parser.add_argument(
        "--default-toxicity-cents",
        type=float,
        default=float(cfg("DEFAULT_TOXICITY_CENTS", 3.0)),
    )
    parser.add_argument(
        "--maker-fee-factor",
        type=float,
        default=float(cfg("DEFAULT_FEE_FACTOR_FOR_MAKER_QUOTES", 0.25)),
    )
    parser.add_argument(
        "--fair-value-mid-weight",
        type=float,
        default=float(cfg("FAIR_VALUE_MID_WEIGHT", 0.55)),
    )
    parser.add_argument(
        "--fair-value-last-weight",
        type=float,
        default=float(cfg("FAIR_VALUE_LAST_WEIGHT", 0.45)),
    )
    parser.add_argument(
        "--fair-value-max-orderbook-imbalance-adjust-cents",
        type=float,
        default=float(cfg("FAIR_VALUE_MAX_ORDERBOOK_IMBALANCE_ADJUST_CENTS", 4.0)),
    )
    parser.add_argument(
        "--passive-offset-ticks-when-not-improving",
        type=int,
        default=int(cfg("PASSIVE_OFFSET_TICKS_WHEN_NOT_IMPROVING", 2)),
    )
    parser.add_argument(
        "--candidate-price-levels-to-scan",
        type=int,
        default=int(cfg("CANDIDATE_PRICE_LEVELS_TO_SCAN", 6)),
    )
    parser.add_argument(
        "--queue-penalty-cap-cents",
        type=float,
        default=float(cfg("QUEUE_PENALTY_CAP_CENTS", 6.0)),
    )
    parser.add_argument(
        "--queue-penalty-contracts-per-cent",
        type=float,
        default=float(cfg("QUEUE_PENALTY_CONTRACTS_PER_CENT", 30.0)),
    )
    parser.add_argument(
        "--imbalance-toxicity-extra-cents",
        type=float,
        default=float(cfg("IMBALANCE_TOXICITY_EXTRA_CENTS", 1.0)),
    )
    parser.add_argument(
        "--net-position-contracts",
        type=float,
        default=float(cfg("NET_POSITION_CONTRACTS", 0.0)),
    )
    parser.add_argument(
        "--inventory-skew-contracts-per-tick",
        type=float,
        default=float(cfg("INVENTORY_SKEW_CONTRACTS_PER_TICK", 15.0)),
    )
    parser.add_argument(
        "--maximum-inventory-skew-ticks",
        type=float,
        default=float(cfg("MAXIMUM_INVENTORY_SKEW_TICKS", 5.0)),
    )
    parser.add_argument(
        "--tick-value-cents-for-inventory-penalty",
        type=float,
        default=float(cfg("TICK_VALUE_CENTS_FOR_INVENTORY_PENALTY", 1.0)),
    )

    parser.add_argument("--run-forever", action="store_true", default=config.RUN_FOREVER)
    parser.add_argument("--cycle-seconds", type=float, default=config.CYCLE_SECONDS)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.run_forever:
            while True:
                run_once(args)
                time.sleep(float(args.cycle_seconds))
        else:
            run_once(args)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"kalshi_screener_ev.py failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
