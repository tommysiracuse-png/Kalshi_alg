#!/usr/bin/env python3
"""Kalshi screener for terminal usage.

This is a stripped-down version of your old Colab workflow that keeps only the
public-market screener logic and CSV export.

What it does:
- pulls public Kalshi markets
- applies your screening filters
- computes suggested YES/NO quote levels for the CSV output
- writes screener_export.csv (or a custom output path)

What it does NOT do:
- no auth
- no order placement
- no approvals
- no trading logic
- no log files / order files / event files

Examples:
    python kalshi_screener.py
    python kalshi_screener.py --output my_screener.csv
    python kalshi_screener.py --top-n 25
    python kalshi_screener.py --run-forever --cycle-seconds 60
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

import kalshi_screener_config as config

D100 = Decimal("100")


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
        return int((d * D100).to_integral_value(rounding="ROUND_HALF_UP"))
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
                time.sleep(backoff * (2 ** attempt))
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
            tick_c = int((chosen_step * D100).to_integral_value(rounding="ROUND_HALF_UP"))
            return max(1, tick_c)

    return max(1, int(default_tick_cents))


def compute_top_of_book_cents(mkt: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
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


def extract_book_tick_spread(mkt: Dict[str, Any], default_tick_cents: int) -> Optional[Dict[str, Any]]:
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

    return {
        "tick_c": int(tick_c),
        "yes_bid_c": int(yes_bid),
        "yes_ask_c": int(yes_ask),
        "no_bid_c": int(no_bid),
        "no_ask_c": int(no_ask),
        "spread_c": int(spread_c),
        "yes_side_spread_c": int(max(0, int(yes_ask) - int(yes_bid))),
        "no_side_spread_c": int(max(0, int(no_ask) - int(no_bid))),
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


def compute_mm_quotes_from_top_of_book(
    yes_bid_c: int,
    yes_ask_c: int,
    no_bid_c: int,
    no_ask_c: int,
    tick_cents: int,
    target_edge_cents_total: int,
) -> Optional[Tuple[int, int, int]]:
    tick_cents = max(1, int(tick_cents))
    max_yes_bid = int(yes_ask_c) - tick_cents
    max_no_bid = int(no_ask_c) - tick_cents
    if max_yes_bid < tick_cents or max_no_bid < tick_cents:
        return None

    target_sum = 100 - int(target_edge_cents_total)
    mid_yes = (yes_bid_c + yes_ask_c) / 2.0
    y0 = int(round(mid_yes / tick_cents) * tick_cents)

    max_steps = int(math.ceil(100 / tick_cents)) + 5
    tried = set()

    for k in range(max_steps):
        for sign in (0, -1, +1):
            y = y0 + sign * k * tick_cents
            if y in tried:
                continue
            tried.add(y)

            y = clamp(int(y), tick_cents, max_yes_bid)
            n = target_sum - y
            if n % tick_cents != 0:
                continue
            if not (tick_cents <= n <= max_no_bid):
                continue

            edge = 100 - (y + n)
            if edge < int(target_edge_cents_total):
                continue
            return int(y), int(n), int(edge)

    return None


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

        book = extract_book_tick_spread(market, settings["default_tick_cents"])
        if book is None:
            continue

        if not market_passes_safety_filters(market, book, settings):
            continue

        cutoff_dt, cutoff_field = compute_cutoff_times(market)
        vol24h = get_count_float(market, "volume_24h_fp", "volume_24h")
        oi = get_count_float(market, "open_interest_fp", "open_interest")
        hrs_to_cutoff = (cutoff_dt - now_utc()).total_seconds() / 3600.0 if cutoff_dt else None

        title = str(market.get("title") or "")
        event_ticker = str(market.get("event_ticker") or "")
        series_guess = event_ticker.split("-")[0] if event_ticker and "-" in event_ticker else event_ticker
        series_url = f"https://kalshi.com/markets/{series_guess.lower()}" if series_guess else ""

        rows.append(
            {
                "Spread(c)": int(book["spread_c"]),
                "YES bid(c)": int(book["yes_bid_c"]),
                "YES ask(c)": int(book["yes_ask_c"]),
                "NO bid(c)": int(book["no_bid_c"]),
                "NO ask(c)": int(book["no_ask_c"]),
                "Tick(c)": int(book["tick_c"]),
                "Vol24h": float(vol24h),
                "OI": float(oi),
                "Hrs->Cutoff": float(hrs_to_cutoff) if hrs_to_cutoff is not None else None,
                "CutoffField": cutoff_field,
                "CutoffUTC": fmt_utc(cutoff_dt),
                "Ticker": str(ticker),
                "SeriesURL": series_url,
                "SearchText": title[:120],
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values(
        by=["Hrs->Cutoff", "Vol24h", "Spread(c)", "OI"],
        ascending=[True, False, True, False],
    ).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    return df


def build_export_dataframe(screen_df: pd.DataFrame, settings: Dict[str, Any]) -> pd.DataFrame:
    if screen_df.empty:
        return pd.DataFrame()

    plans: List[Dict[str, Any]] = []
    target_edge_total = int(settings["target_edge_cents"] + settings["fee_buffer_cents"])
    quote_size = int(settings["quote_size"])
    top_n = int(settings["top_n"])

    for _, row in screen_df.head(top_n).iterrows():
        quote = compute_mm_quotes_from_top_of_book(
            yes_bid_c=int(row["YES bid(c)"]),
            yes_ask_c=int(row["YES ask(c)"]),
            no_bid_c=int(row["NO bid(c)"]),
            no_ask_c=int(row["NO ask(c)"]),
            tick_cents=max(1, int(row["Tick(c)"])),
            target_edge_cents_total=target_edge_total,
        )
        if quote is None:
            continue

        q_yes, q_no, q_edge = quote
        theo_gross_usd = (float(q_edge) / 100.0) * float(quote_size)

        plans.append(
            {
                "Quoted edge(c)": int(q_edge),
                "Quote YES bid(c)": int(q_yes),
                "Quote NO bid(c)": int(q_no),
                "Spread(c)": int(row["Spread(c)"]),
                "YES bid(c)": int(row["YES bid(c)"]),
                "YES ask(c)": int(row["YES ask(c)"]),
                "NO bid(c)": int(row["NO bid(c)"]),
                "NO ask(c)": int(row["NO ask(c)"]),
                "Tick(c)": int(row["Tick(c)"]),
                "Vol24h": float(row["Vol24h"]),
                "OI": float(row["OI"]),
                "Hrs->Cutoff": float(row["Hrs->Cutoff"]),
                "CutoffUTC": row["CutoffUTC"],
                "Ticker": row["Ticker"],
                "SeriesURL": row["SeriesURL"],
                "SearchText": row["SearchText"],
                "Theo gross($) if both fill": round(theo_gross_usd, 4),
            }
        )

    plans_df = pd.DataFrame(plans)
    if plans_df.empty:
        return plans_df

    return plans_df.sort_values(
        by=["Hrs->Cutoff", "Vol24h", "Spread(c)", "OI", "Quoted edge(c)"],
        ascending=[True, False, True, False, False],
    ).reset_index(drop=True)


def build_settings_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    return {
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
        "target_edge_cents": args.target_edge_cents,
        "fee_buffer_cents": args.fee_buffer_cents,
        "default_tick_cents": args.default_tick_cents,
        "quote_size": args.quote_size,
    }


def run_once(args: argparse.Namespace) -> None:
    settings = build_settings_from_args(args)
    pub = KalshiPublicClient(host=args.host, api_prefix=args.api_prefix)
    screen_df = screen_markets(pub, settings)
    export_df = build_export_dataframe(screen_df, settings)
    export_df.to_csv(args.output, index=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Kalshi public screener and write a CSV file.")

    parser.add_argument("--host", default=config.KALSHI_HOST)
    parser.add_argument("--api-prefix", default=config.API_PREFIX)
    parser.add_argument("--output", default=config.OUTPUT_CSV)

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

    parser.add_argument("--target-edge-cents", type=int, default=config.TARGET_EDGE_CENTS)
    parser.add_argument("--fee-buffer-cents", type=int, default=config.FEE_BUFFER_CENTS)
    parser.add_argument("--default-tick-cents", type=int, default=config.DEFAULT_TICK_CENTS)
    parser.add_argument("--quote-size", type=int, default=config.QUOTE_SIZE)

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
        print(f"kalshi_screener.py failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
