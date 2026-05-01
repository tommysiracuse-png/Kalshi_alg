#!/usr/bin/env python3
"""
Rank matched Kalshi vs Polymarket markets by *live executable* cross-book edge.

Why this exists
---------------
The matching CSV from `cross_book_matcher.py` is useful for finding the same line
listed on both books, but displayed probabilities can be misleading when the
bid/ask spread is wide.

This script reads a CSV of matched pairs, fetches current best bid/ask data,
and ranks the rows by executable edge instead of headline displayed probability.

Default ranking logic
---------------------
By default the script only scores *inventory-free* lock-in trades:

1. Buy YES on Kalshi + Buy NO on Polymarket
2. Buy NO on Kalshi + Buy YES on Polymarket

Those are the conservative cross-book arbitrage constructions that do not assume
that you already own inventory on either venue.

Optional same-side crosses
--------------------------
If you pass `--include-same-side-crosses`, the script will also score:

- Sell YES on one venue / Buy YES on the other
- Sell NO on one venue / Buy NO on the other

Those can be useful, but they may require existing inventory or a separate
conversion / mint-burn workflow depending on how you trade.

Input
-----
The input CSV must contain at least:

- kalshi_ticker
- polymarket_slug

It will work best on the CSV produced by `cross_book_matcher.py`, but it can
also enrich older CSVs that already contain quote columns.

Examples
--------
python cross_book_arb_ranker.py matches.csv
python cross_book_arb_ranker.py matches.csv --output-csv ranked.csv --top 100
python cross_book_arb_ranker.py matches.csv --min-edge-cents 0
python cross_book_arb_ranker.py matches.csv --include-same-side-crosses --sort profit
python cross_book_arb_ranker.py --self-test
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence, Tuple
from urllib.parse import quote as url_quote

import requests


KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
POLY_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
POLY_CLOB_BASE_URL = "https://clob.polymarket.com"

USER_AGENT = "cross-book-arb-ranker/1.0"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 25
DEFAULT_HTTP_MAX_RETRIES = 8
DEFAULT_HTTP_BACKOFF_SECONDS = 1.0
DEFAULT_HTTP_BACKOFF_MULTIPLIER = 1.8
DEFAULT_HTTP_MAX_BACKOFF_SECONDS = 30.0
DEFAULT_PAGE_PAUSE_SECONDS = 0.08

KALSHI_PAGE_SIZE = 1000
POLY_MARKETS_PAGE_SIZE = 500
POLY_BOOK_BATCH_SIZE = 500


class HttpError(RuntimeError):
    """Raised when an HTTP request fails after retries."""


@dataclass(frozen=True)
class Quote:
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    yes_bid_size: Optional[float] = None
    yes_ask_size: Optional[float] = None
    no_bid_size: Optional[float] = None
    no_ask_size: Optional[float] = None
    source: str = ""
    status: Optional[str] = None
    title: str = ""


@dataclass(frozen=True)
class Candidate:
    name: str
    edge_dollars: Optional[float]
    capacity: Optional[float]
    gross_profit_at_top_size_dollars: Optional[float]
    total_width_dollars: Optional[float]
    requires_inventory: bool = False


@dataclass(frozen=True)
class Settings:
    input_csv: Optional[str]
    output_csv: Optional[str]
    top: int
    verbose: bool
    min_edge_cents: Optional[float]
    include_same_side_crosses: bool
    sort: str
    self_test: bool


def eprint(verbose: bool, *parts: object) -> None:
    if verbose:
        print(*parts, file=sys.stderr)


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return session


def _parse_retry_after_seconds(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None
    text = str(header_value).strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        return None


def _compute_backoff_seconds(*, response: Optional[requests.Response], attempt_index: int) -> float:
    retry_after_seconds = _parse_retry_after_seconds(response.headers.get("Retry-After")) if response is not None else None
    if retry_after_seconds is not None:
        return min(DEFAULT_HTTP_MAX_BACKOFF_SECONDS, retry_after_seconds)
    return min(
        DEFAULT_HTTP_MAX_BACKOFF_SECONDS,
        DEFAULT_HTTP_BACKOFF_SECONDS * (DEFAULT_HTTP_BACKOFF_MULTIPLIER ** max(0, attempt_index - 1)),
    )


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Any] = None,
    verbose: bool = False,
) -> Any:
    last_error: Optional[BaseException] = None
    retriable_statuses = {429, 500, 502, 503, 504}
    for attempt_index in range(1, DEFAULT_HTTP_MAX_RETRIES + 1):
        response: Optional[requests.Response] = None
        try:
            response = session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code < 400:
                try:
                    return response.json()
                except ValueError as exc:
                    raise HttpError(f"{method} {url} returned non-JSON content") from exc

            if response.status_code in retriable_statuses and attempt_index < DEFAULT_HTTP_MAX_RETRIES:
                backoff_seconds = _compute_backoff_seconds(response=response, attempt_index=attempt_index)
                eprint(verbose, f"Retrying {method} {url} after HTTP {response.status_code} (sleep={backoff_seconds:.2f}s)")
                time.sleep(backoff_seconds)
                continue
            raise HttpError(f"{method} {url} failed {response.status_code}: {response.text[:500]}")
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt_index >= DEFAULT_HTTP_MAX_RETRIES:
                break
            backoff_seconds = _compute_backoff_seconds(response=response, attempt_index=attempt_index)
            eprint(verbose, f"Retrying {method} {url} after {type(exc).__name__} (sleep={backoff_seconds:.2f}s)")
            time.sleep(backoff_seconds)

    if last_error is not None:
        raise HttpError(f"{method} {url} failed after retries: {last_error}") from last_error
    raise HttpError(f"{method} {url} failed after retries")


def parse_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_jsonish_list(raw_value: Any) -> List[Any]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return raw_value
    if isinstance(raw_value, tuple):
        return list(raw_value)
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def chunked(items: Sequence[str], chunk_size: int) -> Iterator[List[str]]:
    for start in range(0, len(items), chunk_size):
        yield list(items[start:start + chunk_size])


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_slug_or_ticker(value: str) -> str:
    return url_quote(str(value).strip(), safe="")


def fmt_decimal(value: Optional[float], places: int = 4) -> str:
    if value is None or not math.isfinite(value):
        return ""
    text = f"{value:.{places}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def fmt_money_cents(value_dollars: Optional[float]) -> str:
    if value_dollars is None or not math.isfinite(value_dollars):
        return ""
    return fmt_decimal(value_dollars * 100.0, 2)


def fmt_price(value: Optional[float]) -> str:
    return fmt_decimal(value, 4)


def fmt_size(value: Optional[float]) -> str:
    return fmt_decimal(value, 2)


def fmt_pct_points(value: Optional[float]) -> str:
    return fmt_decimal(value, 2)


def pick_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def quote_from_existing_row(row: Mapping[str, Any], venue_prefix: str) -> Optional[Quote]:
    yes_bid = parse_float(row.get(f"{venue_prefix}_yes_bid"))
    yes_ask = parse_float(row.get(f"{venue_prefix}_yes_ask"))
    no_bid = parse_float(row.get(f"{venue_prefix}_no_bid"))
    no_ask = parse_float(row.get(f"{venue_prefix}_no_ask"))
    if all(value is None for value in (yes_bid, yes_ask, no_bid, no_ask)):
        return None
    return Quote(
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        yes_bid_size=parse_float(row.get(f"{venue_prefix}_yes_bid_size")),
        yes_ask_size=parse_float(row.get(f"{venue_prefix}_yes_ask_size")),
        no_bid_size=parse_float(row.get(f"{venue_prefix}_no_bid_size")),
        no_ask_size=parse_float(row.get(f"{venue_prefix}_no_ask_size")),
        source="csv_fallback",
        status=None,
        title="",
    )


def width_dollars(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    return ask - bid


def side_width(quote: Optional[Quote], side: str) -> Optional[float]:
    if quote is None:
        return None
    if side == "yes":
        return width_dollars(quote.yes_bid, quote.yes_ask)
    if side == "no":
        return width_dollars(quote.no_bid, quote.no_ask)
    raise ValueError(f"Unsupported side: {side}")


def min_non_none(*values: Optional[float]) -> Optional[float]:
    present = [float(value) for value in values if value is not None and math.isfinite(value)]
    if not present:
        return None
    return min(present)


def candidate(
    name: str,
    price_a: Optional[float],
    size_a: Optional[float],
    price_b: Optional[float],
    size_b: Optional[float],
    *,
    payout_dollars: float,
    total_width_dollars: Optional[float],
    requires_inventory: bool = False,
) -> Candidate:
    if price_a is None or price_b is None:
        return Candidate(
            name=name,
            edge_dollars=None,
            capacity=None,
            gross_profit_at_top_size_dollars=None,
            total_width_dollars=total_width_dollars,
            requires_inventory=requires_inventory,
        )
    edge_dollars = payout_dollars - (price_a + price_b)
    capacity = min_non_none(size_a, size_b)
    profit = None if capacity is None else edge_dollars * capacity
    return Candidate(
        name=name,
        edge_dollars=edge_dollars,
        capacity=capacity,
        gross_profit_at_top_size_dollars=profit,
        total_width_dollars=total_width_dollars,
        requires_inventory=requires_inventory,
    )


def select_best_candidate(candidates: Sequence[Candidate], *, sort: str) -> Optional[Candidate]:
    valid = [candidate for candidate in candidates if candidate.edge_dollars is not None]
    if not valid:
        return None

    def key(candidate: Candidate) -> Tuple[float, float, float]:
        edge = float(candidate.edge_dollars if candidate.edge_dollars is not None else float("-inf"))
        capacity = float(candidate.capacity if candidate.capacity is not None else 0.0)
        profit = float(candidate.gross_profit_at_top_size_dollars if candidate.gross_profit_at_top_size_dollars is not None else float("-inf"))
        if sort == "profit":
            return (profit, edge, capacity)
        if sort == "capacity":
            return (capacity, edge, profit)
        return (edge, profit, capacity)

    return max(valid, key=key)


def compute_inventory_free_candidates(kalshi: Optional[Quote], polymarket: Optional[Quote]) -> List[Candidate]:
    if kalshi is None or polymarket is None:
        return []
    return [
        candidate(
            "Buy YES Kalshi / Buy NO Polymarket",
            kalshi.yes_ask,
            kalshi.yes_ask_size,
            polymarket.no_ask,
            polymarket.no_ask_size,
            payout_dollars=1.0,
            total_width_dollars=(side_width(kalshi, "yes") or 0.0) + (side_width(polymarket, "no") or 0.0)
            if side_width(kalshi, "yes") is not None and side_width(polymarket, "no") is not None
            else None,
            requires_inventory=False,
        ),
        candidate(
            "Buy NO Kalshi / Buy YES Polymarket",
            kalshi.no_ask,
            kalshi.no_ask_size,
            polymarket.yes_ask,
            polymarket.yes_ask_size,
            payout_dollars=1.0,
            total_width_dollars=(side_width(kalshi, "no") or 0.0) + (side_width(polymarket, "yes") or 0.0)
            if side_width(kalshi, "no") is not None and side_width(polymarket, "yes") is not None
            else None,
            requires_inventory=False,
        ),
    ]


def compute_same_side_candidates(kalshi: Optional[Quote], polymarket: Optional[Quote]) -> List[Candidate]:
    if kalshi is None or polymarket is None:
        return []
    return [
        candidate(
            "Sell YES Kalshi / Buy YES Polymarket",
            -kalshi.yes_bid if kalshi.yes_bid is not None else None,
            kalshi.yes_bid_size,
            polymarket.yes_ask,
            polymarket.yes_ask_size,
            payout_dollars=0.0,
            total_width_dollars=(side_width(kalshi, "yes") or 0.0) + (side_width(polymarket, "yes") or 0.0)
            if side_width(kalshi, "yes") is not None and side_width(polymarket, "yes") is not None
            else None,
            requires_inventory=True,
        ),
        candidate(
            "Sell YES Polymarket / Buy YES Kalshi",
            -polymarket.yes_bid if polymarket.yes_bid is not None else None,
            polymarket.yes_bid_size,
            kalshi.yes_ask,
            kalshi.yes_ask_size,
            payout_dollars=0.0,
            total_width_dollars=(side_width(polymarket, "yes") or 0.0) + (side_width(kalshi, "yes") or 0.0)
            if side_width(polymarket, "yes") is not None and side_width(kalshi, "yes") is not None
            else None,
            requires_inventory=True,
        ),
        candidate(
            "Sell NO Kalshi / Buy NO Polymarket",
            -kalshi.no_bid if kalshi.no_bid is not None else None,
            kalshi.no_bid_size,
            polymarket.no_ask,
            polymarket.no_ask_size,
            payout_dollars=0.0,
            total_width_dollars=(side_width(kalshi, "no") or 0.0) + (side_width(polymarket, "no") or 0.0)
            if side_width(kalshi, "no") is not None and side_width(polymarket, "no") is not None
            else None,
            requires_inventory=True,
        ),
        candidate(
            "Sell NO Polymarket / Buy NO Kalshi",
            -polymarket.no_bid if polymarket.no_bid is not None else None,
            polymarket.no_bid_size,
            kalshi.no_ask,
            kalshi.no_ask_size,
            payout_dollars=0.0,
            total_width_dollars=(side_width(polymarket, "no") or 0.0) + (side_width(kalshi, "no") or 0.0)
            if side_width(polymarket, "no") is not None and side_width(kalshi, "no") is not None
            else None,
            requires_inventory=True,
        ),
    ]


# Note: same-side candidate() uses payout_dollars=0 and negated sell price to produce:
# edge = 0 - (-sell_bid + buy_ask) = sell_bid - buy_ask


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def read_csv_rows(path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    with open(path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{key: (value or "") for key, value in row.items()} for row in reader]
    return fieldnames, rows


def write_csv(path: str, fieldnames: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Kalshi quote resolution
# ---------------------------------------------------------------------------

def quote_from_kalshi_market(raw_market: Mapping[str, Any], *, source: str) -> Quote:
    yes_bid = parse_float(raw_market.get("yes_bid_dollars"))
    yes_ask = parse_float(raw_market.get("yes_ask_dollars"))
    no_bid = parse_float(raw_market.get("no_bid_dollars"))
    no_ask = parse_float(raw_market.get("no_ask_dollars"))

    yes_bid_size = parse_float(raw_market.get("yes_bid_size_fp"))
    yes_ask_size = parse_float(raw_market.get("yes_ask_size_fp"))
    no_bid_size = parse_float(raw_market.get("no_bid_size_fp"))
    no_ask_size = parse_float(raw_market.get("no_ask_size_fp"))

    # Fallbacks if some ask fields are omitted from a response shape.
    if yes_ask is None and no_bid is not None:
        yes_ask = 1.0 - no_bid
    if no_ask is None and yes_bid is not None:
        no_ask = 1.0 - yes_bid
    if yes_ask_size is None and no_bid_size is not None:
        yes_ask_size = no_bid_size
    if no_ask_size is None and yes_bid_size is not None:
        no_ask_size = yes_bid_size

    return Quote(
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        yes_bid_size=yes_bid_size,
        yes_ask_size=yes_ask_size,
        no_bid_size=no_bid_size,
        no_ask_size=no_ask_size,
        source=source,
        status=str(raw_market.get("status") or "").strip() or None,
        title=pick_text(raw_market.get("title"), raw_market.get("subtitle")),
    )


def best_kalshi_book_level(levels: Any) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(levels, list) or not levels:
        return None, None
    level = levels[0]
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        return parse_float(level[0]), parse_float(level[1])
    return None, None


def quote_from_kalshi_orderbook(raw_orderbook: Mapping[str, Any]) -> Quote:
    orderbook = raw_orderbook.get("orderbook_fp") or {}
    yes_bid, yes_bid_size = best_kalshi_book_level(orderbook.get("yes_dollars"))
    no_bid, no_bid_size = best_kalshi_book_level(orderbook.get("no_dollars"))
    yes_ask = 1.0 - no_bid if no_bid is not None else None
    no_ask = 1.0 - yes_bid if yes_bid is not None else None
    yes_ask_size = no_bid_size
    no_ask_size = yes_bid_size
    return Quote(
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        yes_bid_size=yes_bid_size,
        yes_ask_size=yes_ask_size,
        no_bid_size=no_bid_size,
        no_ask_size=no_ask_size,
        source="kalshi_orderbook",
    )


def merge_quotes(primary: Quote, fallback: Quote) -> Quote:
    return Quote(
        yes_bid=primary.yes_bid if primary.yes_bid is not None else fallback.yes_bid,
        yes_ask=primary.yes_ask if primary.yes_ask is not None else fallback.yes_ask,
        no_bid=primary.no_bid if primary.no_bid is not None else fallback.no_bid,
        no_ask=primary.no_ask if primary.no_ask is not None else fallback.no_ask,
        yes_bid_size=primary.yes_bid_size if primary.yes_bid_size is not None else fallback.yes_bid_size,
        yes_ask_size=primary.yes_ask_size if primary.yes_ask_size is not None else fallback.yes_ask_size,
        no_bid_size=primary.no_bid_size if primary.no_bid_size is not None else fallback.no_bid_size,
        no_ask_size=primary.no_ask_size if primary.no_ask_size is not None else fallback.no_ask_size,
        source=primary.source or fallback.source,
        status=primary.status if primary.status is not None else fallback.status,
        title=primary.title or fallback.title,
    )


def fetch_needed_kalshi_quotes(
    session: requests.Session,
    tickers: Sequence[str],
    *,
    verbose: bool,
) -> Dict[str, Quote]:
    wanted = {ticker.strip() for ticker in tickers if ticker and ticker.strip()}
    found: Dict[str, Quote] = {}
    if not wanted:
        return found

    missing = set(wanted)
    cursor: Optional[str] = None
    while missing:
        params: Dict[str, Any] = {
            "status": "open",
            "limit": KALSHI_PAGE_SIZE,
            "mve_filter": "exclude",
        }
        if cursor:
            params["cursor"] = cursor
        payload = request_json(session, "GET", f"{KALSHI_BASE_URL}/markets", params=params, verbose=verbose)
        raw_markets = payload.get("markets") or []
        eprint(verbose, f"Fetched {len(raw_markets)} Kalshi markets (cursor={cursor or 'start'})")
        if not raw_markets:
            break
        for raw_market in raw_markets:
            ticker = str(raw_market.get("ticker") or "").strip()
            if ticker in missing:
                found[ticker] = quote_from_kalshi_market(raw_market, source="kalshi_market_list")
                missing.remove(ticker)
        cursor = payload.get("cursor") or None
        if not cursor:
            break
        time.sleep(DEFAULT_PAGE_PAUSE_SECONDS)

    for ticker in list(missing):
        safe_ticker = safe_slug_or_ticker(ticker)
        try:
            payload = request_json(session, "GET", f"{KALSHI_BASE_URL}/markets/{safe_ticker}", verbose=verbose)
            raw_market = payload.get("market") or {}
            quote_from_market = quote_from_kalshi_market(raw_market, source="kalshi_market")
            try:
                book_payload = request_json(
                    session,
                    "GET",
                    f"{KALSHI_BASE_URL}/markets/{safe_ticker}/orderbook",
                    params={"depth": 1},
                    verbose=verbose,
                )
                quote_from_book = quote_from_kalshi_orderbook(book_payload)
                found[ticker] = merge_quotes(quote_from_market, quote_from_book)
            except Exception:
                found[ticker] = quote_from_market
            missing.remove(ticker)
        except Exception as exc:
            eprint(verbose, f"Could not resolve Kalshi quote for {ticker}: {exc}")

    return found


# ---------------------------------------------------------------------------
# Polymarket quote resolution
# ---------------------------------------------------------------------------

def fetch_needed_polymarket_markets(
    session: requests.Session,
    slugs: Sequence[str],
    *,
    verbose: bool,
) -> Dict[str, Dict[str, Any]]:
    wanted = {slug.strip() for slug in slugs if slug and slug.strip()}
    found: Dict[str, Dict[str, Any]] = {}
    if not wanted:
        return found

    missing = set(wanted)
    offset = 0
    while missing:
        params = {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": POLY_MARKETS_PAGE_SIZE,
            "offset": offset,
        }
        payload = request_json(session, "GET", f"{POLY_GAMMA_BASE_URL}/markets", params=params, verbose=verbose)
        if not isinstance(payload, list):
            raise HttpError("Unexpected Polymarket /markets response shape; expected a list")
        eprint(verbose, f"Fetched {len(payload)} Polymarket markets (offset={offset})")
        if not payload:
            break
        for raw_market in payload:
            slug = str(raw_market.get("slug") or "").strip()
            if slug in missing:
                found[slug] = dict(raw_market)
                missing.remove(slug)
        offset += len(payload)
        time.sleep(DEFAULT_PAGE_PAUSE_SECONDS)

    # Fallback to direct slug fetch for anything not found in the active list.
    for slug in list(missing):
        safe_slug = safe_slug_or_ticker(slug)
        raw_market: Optional[Dict[str, Any]] = None
        try:
            payload = request_json(session, "GET", f"{POLY_GAMMA_BASE_URL}/markets/slug/{safe_slug}", verbose=verbose)
            if isinstance(payload, dict):
                raw_market = dict(payload)
        except Exception:
            raw_market = None

        if raw_market is None:
            try:
                payload = request_json(
                    session,
                    "GET",
                    f"{POLY_GAMMA_BASE_URL}/markets",
                    params={"slug": slug},
                    verbose=verbose,
                )
                if isinstance(payload, list) and payload:
                    raw_market = dict(payload[0])
            except Exception:
                raw_market = None

        if raw_market is not None:
            found[slug] = raw_market
            missing.remove(slug)
        else:
            eprint(verbose, f"Could not resolve Polymarket market for slug {slug}")

    return found


def fetch_polymarket_books(
    session: requests.Session,
    token_ids: Sequence[str],
    *,
    verbose: bool,
) -> Dict[str, Dict[str, Any]]:
    books_by_token_id: Dict[str, Dict[str, Any]] = {}
    deduped = [token_id for token_id in dict.fromkeys(str(token_id) for token_id in token_ids if str(token_id).strip())]
    if not deduped:
        return books_by_token_id

    for token_batch in chunked(deduped, POLY_BOOK_BATCH_SIZE):
        body = [{"token_id": token_id} for token_id in token_batch]
        response = request_json(session, "POST", f"{POLY_CLOB_BASE_URL}/books", json_body=body, verbose=verbose)
        if not isinstance(response, list):
            raise HttpError("Unexpected Polymarket /books response shape; expected a list")
        for book in response:
            asset_id = str(book.get("asset_id") or "").strip()
            if asset_id:
                books_by_token_id[asset_id] = dict(book)
        eprint(verbose, f"Fetched {len(response)} Polymarket order books")
        time.sleep(DEFAULT_PAGE_PAUSE_SECONDS)

    missing = [token_id for token_id in deduped if token_id not in books_by_token_id]
    for token_id in missing:
        try:
            response = request_json(
                session,
                "GET",
                f"{POLY_CLOB_BASE_URL}/book",
                params={"token_id": token_id},
                verbose=verbose,
            )
            asset_id = str(response.get("asset_id") or "").strip()
            if asset_id:
                books_by_token_id[asset_id] = dict(response)
        except Exception as exc:
            eprint(verbose, f"Could not resolve Polymarket book for token {token_id}: {exc}")

    return books_by_token_id


def best_poly_level(levels: Any, *, descending: bool) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(levels, list) or not levels:
        return None, None
    entries: List[Tuple[float, Optional[float]]] = []
    for entry in levels:
        if not isinstance(entry, Mapping):
            continue
        price = parse_float(entry.get("price"))
        size = parse_float(entry.get("size"))
        if price is None:
            continue
        entries.append((price, size))
    if not entries:
        return None, None
    entries.sort(key=lambda item: item[0], reverse=descending)
    return entries[0]


def quote_from_polymarket_market_and_books(raw_market: Mapping[str, Any], books_by_token_id: Mapping[str, Mapping[str, Any]]) -> Quote:
    token_ids = parse_jsonish_list(raw_market.get("clobTokenIds"))
    if len(token_ids) < 2:
        return Quote(None, None, None, None, source="polymarket_market_missing_tokens", status=None, title=pick_text(raw_market.get("question")))

    yes_token_id = str(token_ids[0])
    no_token_id = str(token_ids[1])
    yes_book = books_by_token_id.get(yes_token_id, {})
    no_book = books_by_token_id.get(no_token_id, {})

    yes_bid, yes_bid_size = best_poly_level(yes_book.get("bids"), descending=True)
    yes_ask, yes_ask_size = best_poly_level(yes_book.get("asks"), descending=False)
    no_bid, no_bid_size = best_poly_level(no_book.get("bids"), descending=True)
    no_ask, no_ask_size = best_poly_level(no_book.get("asks"), descending=False)

    status_parts = []
    active = raw_market.get("active")
    closed = raw_market.get("closed")
    archived = raw_market.get("archived")
    if active is not None:
        status_parts.append(f"active={bool(active)}")
    if closed is not None:
        status_parts.append(f"closed={bool(closed)}")
    if archived is not None:
        status_parts.append(f"archived={bool(archived)}")

    return Quote(
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        yes_bid_size=yes_bid_size,
        yes_ask_size=yes_ask_size,
        no_bid_size=no_bid_size,
        no_ask_size=no_ask_size,
        source="polymarket_books",
        status=", ".join(status_parts) if status_parts else None,
        title=pick_text(raw_market.get("question"), raw_market.get("slug")),
    )


# ---------------------------------------------------------------------------
# Ranking / enrichment
# ---------------------------------------------------------------------------

def compute_display_gap_pct_points(row: Mapping[str, Any]) -> Optional[float]:
    direct = parse_float(row.get("probability_gap_pct_points"))
    if direct is not None:
        return abs(direct)
    kalshi_prob = parse_float(row.get("kalshi_probability_pct"))
    poly_prob = parse_float(row.get("polymarket_probability_pct"))
    if kalshi_prob is not None and poly_prob is not None:
        return abs(kalshi_prob - poly_prob)
    return None


def enrich_row(
    row: Mapping[str, Any],
    *,
    kalshi_quotes: Mapping[str, Quote],
    polymarket_quotes: Mapping[str, Quote],
    settings: Settings,
    snapshot_utc: str,
) -> Dict[str, Any]:
    enriched: Dict[str, Any] = dict(row)

    kalshi_ticker = str(row.get("kalshi_ticker") or "").strip()
    polymarket_slug = str(row.get("polymarket_slug") or "").strip()

    kalshi_quote = kalshi_quotes.get(kalshi_ticker) or quote_from_existing_row(row, "kalshi")
    polymarket_quote = polymarket_quotes.get(polymarket_slug) or quote_from_existing_row(row, "polymarket")

    inventory_free_candidates = compute_inventory_free_candidates(kalshi_quote, polymarket_quote)
    best_inventory_free = select_best_candidate(inventory_free_candidates, sort=settings.sort)

    same_side_candidates = compute_same_side_candidates(kalshi_quote, polymarket_quote) if settings.include_same_side_crosses else []
    best_same_side = select_best_candidate(same_side_candidates, sort=settings.sort) if same_side_candidates else None

    candidates_for_best_any = list(inventory_free_candidates)
    if settings.include_same_side_crosses:
        candidates_for_best_any.extend(same_side_candidates)
    best_any = select_best_candidate(candidates_for_best_any, sort=settings.sort)

    display_gap = compute_display_gap_pct_points(row)
    best_edge_cents = None if best_any is None or best_any.edge_dollars is None else best_any.edge_dollars * 100.0
    spread_leakage_cents = None
    if display_gap is not None and best_edge_cents is not None:
        spread_leakage_cents = display_gap - best_edge_cents

    def q_source(q: Optional[Quote]) -> str:
        return q.source if q is not None else ""

    enriched.update(
        {
            "live_snapshot_utc": snapshot_utc,
            "kalshi_quote_source": q_source(kalshi_quote),
            "kalshi_live_status": kalshi_quote.status if kalshi_quote is not None and kalshi_quote.status is not None else "",
            "kalshi_live_yes_bid": fmt_price(kalshi_quote.yes_bid if kalshi_quote is not None else None),
            "kalshi_live_yes_ask": fmt_price(kalshi_quote.yes_ask if kalshi_quote is not None else None),
            "kalshi_live_no_bid": fmt_price(kalshi_quote.no_bid if kalshi_quote is not None else None),
            "kalshi_live_no_ask": fmt_price(kalshi_quote.no_ask if kalshi_quote is not None else None),
            "kalshi_live_yes_bid_size": fmt_size(kalshi_quote.yes_bid_size if kalshi_quote is not None else None),
            "kalshi_live_yes_ask_size": fmt_size(kalshi_quote.yes_ask_size if kalshi_quote is not None else None),
            "kalshi_live_no_bid_size": fmt_size(kalshi_quote.no_bid_size if kalshi_quote is not None else None),
            "kalshi_live_no_ask_size": fmt_size(kalshi_quote.no_ask_size if kalshi_quote is not None else None),
            "kalshi_live_yes_width_cents": fmt_money_cents(side_width(kalshi_quote, "yes") if kalshi_quote is not None else None),
            "kalshi_live_no_width_cents": fmt_money_cents(side_width(kalshi_quote, "no") if kalshi_quote is not None else None),
            "polymarket_quote_source": q_source(polymarket_quote),
            "polymarket_live_status": polymarket_quote.status if polymarket_quote is not None and polymarket_quote.status is not None else "",
            "polymarket_live_yes_bid": fmt_price(polymarket_quote.yes_bid if polymarket_quote is not None else None),
            "polymarket_live_yes_ask": fmt_price(polymarket_quote.yes_ask if polymarket_quote is not None else None),
            "polymarket_live_no_bid": fmt_price(polymarket_quote.no_bid if polymarket_quote is not None else None),
            "polymarket_live_no_ask": fmt_price(polymarket_quote.no_ask if polymarket_quote is not None else None),
            "polymarket_live_yes_bid_size": fmt_size(polymarket_quote.yes_bid_size if polymarket_quote is not None else None),
            "polymarket_live_yes_ask_size": fmt_size(polymarket_quote.yes_ask_size if polymarket_quote is not None else None),
            "polymarket_live_no_bid_size": fmt_size(polymarket_quote.no_bid_size if polymarket_quote is not None else None),
            "polymarket_live_no_ask_size": fmt_size(polymarket_quote.no_ask_size if polymarket_quote is not None else None),
            "polymarket_live_yes_width_cents": fmt_money_cents(side_width(polymarket_quote, "yes") if polymarket_quote is not None else None),
            "polymarket_live_no_width_cents": fmt_money_cents(side_width(polymarket_quote, "no") if polymarket_quote is not None else None),
            "display_gap_pct_points": fmt_pct_points(display_gap),
            "best_inventory_free_trade": best_inventory_free.name if best_inventory_free is not None else "",
            "best_inventory_free_edge_cents": fmt_money_cents(best_inventory_free.edge_dollars if best_inventory_free is not None else None),
            "best_inventory_free_capacity_contracts": fmt_size(best_inventory_free.capacity if best_inventory_free is not None else None),
            "best_inventory_free_gross_profit_at_top_size_dollars": fmt_decimal(best_inventory_free.gross_profit_at_top_size_dollars if best_inventory_free is not None else None, 2),
            "best_inventory_free_total_width_cents": fmt_money_cents(best_inventory_free.total_width_dollars if best_inventory_free is not None else None),
            "best_same_side_trade": best_same_side.name if best_same_side is not None else "",
            "best_same_side_edge_cents": fmt_money_cents(best_same_side.edge_dollars if best_same_side is not None else None),
            "best_same_side_capacity_contracts": fmt_size(best_same_side.capacity if best_same_side is not None else None),
            "best_same_side_gross_profit_at_top_size_dollars": fmt_decimal(best_same_side.gross_profit_at_top_size_dollars if best_same_side is not None else None, 2),
            "best_same_side_total_width_cents": fmt_money_cents(best_same_side.total_width_dollars if best_same_side is not None else None),
            "best_same_side_requires_inventory": "yes" if best_same_side is not None and best_same_side.requires_inventory else "",
            "best_ranked_trade": best_any.name if best_any is not None else "",
            "best_ranked_edge_cents": fmt_money_cents(best_any.edge_dollars if best_any is not None else None),
            "best_ranked_capacity_contracts": fmt_size(best_any.capacity if best_any is not None else None),
            "best_ranked_gross_profit_at_top_size_dollars": fmt_decimal(best_any.gross_profit_at_top_size_dollars if best_any is not None else None, 2),
            "best_ranked_total_width_cents": fmt_money_cents(best_any.total_width_dollars if best_any is not None else None),
            "best_ranked_requires_inventory": "yes" if best_any is not None and best_any.requires_inventory else "no" if best_any is not None else "",
            "currently_profitable": "yes" if best_edge_cents is not None and best_edge_cents > 0 else "no" if best_edge_cents is not None else "",
            "edge_leakage_cents": fmt_pct_points(spread_leakage_cents),
        }
    )

    return enriched


def sort_enriched_rows(rows: Sequence[Mapping[str, Any]], *, sort: str) -> List[Mapping[str, Any]]:
    def key(row: Mapping[str, Any]) -> Tuple[float, float, float, float]:
        edge = parse_float(row.get("best_ranked_edge_cents"))
        profit = parse_float(row.get("best_ranked_gross_profit_at_top_size_dollars"))
        capacity = parse_float(row.get("best_ranked_capacity_contracts"))
        confidence = parse_float(row.get("match_confidence"))
        edge_key = edge if edge is not None else float("-inf")
        profit_key = profit if profit is not None else float("-inf")
        capacity_key = capacity if capacity is not None else 0.0
        confidence_key = confidence if confidence is not None else 0.0
        if sort == "profit":
            return (profit_key, edge_key, capacity_key, confidence_key)
        if sort == "capacity":
            return (capacity_key, edge_key, profit_key, confidence_key)
        return (edge_key, profit_key, capacity_key, confidence_key)

    return sorted(rows, key=key, reverse=True)


def maybe_filter_rows(rows: Sequence[Mapping[str, Any]], *, min_edge_cents: Optional[float]) -> List[Mapping[str, Any]]:
    if min_edge_cents is None:
        return list(rows)
    filtered: List[Mapping[str, Any]] = []
    for row in rows:
        edge = parse_float(row.get("best_ranked_edge_cents"))
        if edge is None:
            continue
        if edge >= min_edge_cents:
            filtered.append(row)
    return filtered


def build_output_fieldnames(existing_fieldnames: Sequence[str]) -> List[str]:
    extra_fields = [
        "live_snapshot_utc",
        "kalshi_quote_source",
        "kalshi_live_status",
        "kalshi_live_yes_bid",
        "kalshi_live_yes_ask",
        "kalshi_live_no_bid",
        "kalshi_live_no_ask",
        "kalshi_live_yes_bid_size",
        "kalshi_live_yes_ask_size",
        "kalshi_live_no_bid_size",
        "kalshi_live_no_ask_size",
        "kalshi_live_yes_width_cents",
        "kalshi_live_no_width_cents",
        "polymarket_quote_source",
        "polymarket_live_status",
        "polymarket_live_yes_bid",
        "polymarket_live_yes_ask",
        "polymarket_live_no_bid",
        "polymarket_live_no_ask",
        "polymarket_live_yes_bid_size",
        "polymarket_live_yes_ask_size",
        "polymarket_live_no_bid_size",
        "polymarket_live_no_ask_size",
        "polymarket_live_yes_width_cents",
        "polymarket_live_no_width_cents",
        "display_gap_pct_points",
        "best_inventory_free_trade",
        "best_inventory_free_edge_cents",
        "best_inventory_free_capacity_contracts",
        "best_inventory_free_gross_profit_at_top_size_dollars",
        "best_inventory_free_total_width_cents",
        "best_same_side_trade",
        "best_same_side_edge_cents",
        "best_same_side_capacity_contracts",
        "best_same_side_gross_profit_at_top_size_dollars",
        "best_same_side_total_width_cents",
        "best_same_side_requires_inventory",
        "best_ranked_trade",
        "best_ranked_edge_cents",
        "best_ranked_capacity_contracts",
        "best_ranked_gross_profit_at_top_size_dollars",
        "best_ranked_total_width_cents",
        "best_ranked_requires_inventory",
        "currently_profitable",
        "edge_leakage_cents",
    ]
    fieldnames = list(existing_fieldnames)
    for field in extra_fields:
        if field not in fieldnames:
            fieldnames.append(field)
    return fieldnames


def print_table(rows: Sequence[Mapping[str, Any]], *, top: int) -> None:
    subset = list(rows[:top])
    if not subset:
        print("No rows to display.")
        return
    headers = ["rank", "edge(c)", "profit$", "avail", "disp_gap", "leak", "trade", "event", "kalshi", "polymarket"]
    widths = [5, 8, 9, 8, 8, 8, 34, 32, 18, 18]
    print(" ".join(header.ljust(width) for header, width in zip(headers, widths)))
    print("-" * (sum(widths) + len(widths) - 1))
    for row in subset:
        event = pick_text(row.get("event_title"), row.get("kalshi_title"), row.get("polymarket_question"))
        trade = pick_text(row.get("best_ranked_trade"), row.get("best_inventory_free_trade"))
        rendered = [
            pick_text(row.get("rank"), "-"),
            pick_text(row.get("best_ranked_edge_cents"), "-"),
            pick_text(row.get("best_ranked_gross_profit_at_top_size_dollars"), "-"),
            pick_text(row.get("best_ranked_capacity_contracts"), "-"),
            pick_text(row.get("display_gap_pct_points"), "-"),
            pick_text(row.get("edge_leakage_cents"), "-"),
            trade[:34],
            event[:32],
            pick_text(row.get("kalshi_ticker"))[:18],
            pick_text(row.get("polymarket_slug"))[:18],
        ]
        print(" ".join(value.ljust(width) for value, width in zip(rendered, widths)))


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _assert_close(a: Optional[float], b: Optional[float], *, tol: float = 1e-9) -> None:
    if a is None or b is None:
        if a is not b:
            raise AssertionError(f"Expected {a!r} == {b!r}")
        return
    if abs(a - b) > tol:
        raise AssertionError(f"Expected {a!r} ≈ {b!r}")


def run_self_tests() -> None:
    # JSONish parsing.
    if parse_jsonish_list('["1","2"]') != ["1", "2"]:
        raise AssertionError("parse_jsonish_list failed on JSON string")
    if parse_jsonish_list([1, 2]) != [1, 2]:
        raise AssertionError("parse_jsonish_list failed on list")

    # Kalshi orderbook reciprocity.
    k_quote = quote_from_kalshi_orderbook(
        {
            "orderbook_fp": {
                "yes_dollars": [["0.41", "12"]],
                "no_dollars": [["0.53", "7"]],
            }
        }
    )
    _assert_close(k_quote.yes_bid, 0.41)
    _assert_close(k_quote.no_bid, 0.53)
    _assert_close(k_quote.yes_ask, 0.47)
    _assert_close(k_quote.no_ask, 0.59)
    _assert_close(k_quote.yes_ask_size, 7.0)
    _assert_close(k_quote.no_ask_size, 12.0)

    # Polymarket best levels.
    p_quote = quote_from_polymarket_market_and_books(
        {"question": "Will X win?", "clobTokenIds": '["YES","NO"]', "active": True, "closed": False},
        {
            "YES": {"bids": [{"price": "0.61", "size": "4"}], "asks": [{"price": "0.63", "size": "5"}]},
            "NO": {"bids": [{"price": "0.35", "size": "8"}], "asks": [{"price": "0.37", "size": "9"}]},
        },
    )
    _assert_close(p_quote.yes_bid, 0.61)
    _assert_close(p_quote.yes_ask, 0.63)
    _assert_close(p_quote.no_bid, 0.35)
    _assert_close(p_quote.no_ask, 0.37)

    # Inventory-free candidate selection.
    kalshi = Quote(
        yes_bid=0.40,
        yes_ask=0.42,
        no_bid=0.56,
        no_ask=0.58,
        yes_bid_size=100,
        yes_ask_size=50,
        no_bid_size=60,
        no_ask_size=70,
        source="test",
    )
    polymarket = Quote(
        yes_bid=0.50,
        yes_ask=0.52,
        no_bid=0.46,
        no_ask=0.48,
        yes_bid_size=80,
        yes_ask_size=30,
        no_bid_size=90,
        no_ask_size=20,
        source="test",
    )
    inv = compute_inventory_free_candidates(kalshi, polymarket)
    best_inv = select_best_candidate(inv, sort="edge")
    if best_inv is None:
        raise AssertionError("Expected an inventory-free candidate")
    if best_inv.name != "Buy YES Kalshi / Buy NO Polymarket":
        raise AssertionError(f"Unexpected best inventory-free trade: {best_inv.name}")
    _assert_close(best_inv.edge_dollars, 0.10)
    _assert_close(best_inv.capacity, 20.0)
    _assert_close(best_inv.gross_profit_at_top_size_dollars, 2.0)

    # Same-side cross selection.
    same_side = compute_same_side_candidates(kalshi, polymarket)
    best_same = select_best_candidate(same_side, sort="edge")
    if best_same is None:
        raise AssertionError("Expected a same-side candidate")
    if not best_same.requires_inventory:
        raise AssertionError("Expected same-side candidate to require inventory")
    _assert_close(best_same.edge_dollars, 0.08)

    # Enrichment with fallback row quotes.
    row = {
        "kalshi_ticker": "KTEST",
        "polymarket_slug": "ptest",
        "event_title": "Test Event",
        "kalshi_probability_pct": "42",
        "polymarket_probability_pct": "55",
    }
    enriched = enrich_row(
        row,
        kalshi_quotes={"KTEST": kalshi},
        polymarket_quotes={"ptest": polymarket},
        settings=Settings(
            input_csv=None,
            output_csv=None,
            top=10,
            verbose=False,
            min_edge_cents=None,
            include_same_side_crosses=False,
            sort="edge",
            self_test=False,
        ),
        snapshot_utc="2026-03-18T00:00:00+00:00",
    )
    if enriched["best_ranked_trade"] != "Buy YES Kalshi / Buy NO Polymarket":
        raise AssertionError("Unexpected best ranked trade in enriched row")
    if enriched["best_ranked_edge_cents"] != "10":
        raise AssertionError(f"Unexpected best ranked edge string: {enriched['best_ranked_edge_cents']}")
    if enriched["display_gap_pct_points"] != "13":
        raise AssertionError(f"Unexpected display gap: {enriched['display_gap_pct_points']}")
    if enriched["edge_leakage_cents"] != "3":
        raise AssertionError(f"Unexpected edge leakage: {enriched['edge_leakage_cents']}")

    print("Self-test passed.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> Settings:
    parser = argparse.ArgumentParser(description="Rank matched Kalshi/Polymarket pairs by executable cross-book edge.")
    parser.add_argument("input_csv", nargs="?", help="Input CSV from cross_book_matcher.py (must contain kalshi_ticker and polymarket_slug).")
    parser.add_argument("--output-csv", default=None, help="Where to write the ranked CSV. Defaults to <input>_arb_ranked.csv.")
    parser.add_argument("--top", type=int, default=50, help="Rows to print in the terminal preview.")
    parser.add_argument(
        "--min-edge-cents",
        type=float,
        default=None,
        help="Optional filter. Keep only rows whose best ranked edge is at least this many cents.",
    )
    parser.add_argument(
        "--include-same-side-crosses",
        action="store_true",
        help="Also score sell/buy same-side crosses that may require inventory or a conversion workflow.",
    )
    parser.add_argument(
        "--sort",
        choices=("edge", "profit", "capacity"),
        default="edge",
        help="Ranking key for the best trade. Default: edge.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print progress to stderr.")
    parser.add_argument("--self-test", action="store_true", help="Run local tests and exit.")
    args = parser.parse_args(argv)

    return Settings(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        top=max(1, int(args.top)),
        verbose=bool(args.verbose),
        min_edge_cents=(float(args.min_edge_cents) if args.min_edge_cents is not None else None),
        include_same_side_crosses=bool(args.include_same_side_crosses),
        sort=str(args.sort),
        self_test=bool(args.self_test),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    settings = parse_args(argv)

    if settings.self_test:
        run_self_tests()
        return 0

    if not settings.input_csv:
        raise SystemExit("input_csv is required unless --self-test is used")

    input_path = Path(settings.input_csv)
    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path}")

    output_path = Path(settings.output_csv) if settings.output_csv else input_path.with_name(f"{input_path.stem}_arb_ranked.csv")

    fieldnames, rows = read_csv_rows(str(input_path))
    if not rows:
        print("Input CSV is empty.")
        write_csv(str(output_path), build_output_fieldnames(fieldnames), [])
        print(f"Wrote 0 rows to {output_path}")
        return 0

    required_columns = {"kalshi_ticker", "polymarket_slug"}
    missing_columns = sorted(column for column in required_columns if column not in fieldnames)
    if missing_columns:
        raise SystemExit(f"Input CSV is missing required columns: {', '.join(missing_columns)}")

    session = build_http_session()
    snapshot_utc = now_utc_iso()

    kalshi_tickers = [str(row.get("kalshi_ticker") or "").strip() for row in rows]
    poly_slugs = [str(row.get("polymarket_slug") or "").strip() for row in rows]

    try:
        kalshi_quotes = fetch_needed_kalshi_quotes(session, kalshi_tickers, verbose=settings.verbose)
    except Exception as exc:
        eprint(settings.verbose, f"Kalshi quote resolution failed; falling back to any quotes already in the CSV: {exc}")
        kalshi_quotes = {}
    eprint(settings.verbose, f"Resolved {len(kalshi_quotes)} Kalshi quotes")

    try:
        poly_markets = fetch_needed_polymarket_markets(session, poly_slugs, verbose=settings.verbose)
    except Exception as exc:
        eprint(settings.verbose, f"Polymarket market resolution failed; falling back to any quotes already in the CSV: {exc}")
        poly_markets = {}
    eprint(settings.verbose, f"Resolved {len(poly_markets)} Polymarket markets")

    poly_token_ids: List[str] = []
    for raw_market in poly_markets.values():
        tokens = parse_jsonish_list(raw_market.get("clobTokenIds"))
        for token in tokens[:2]:
            text = str(token).strip()
            if text:
                poly_token_ids.append(text)

    try:
        poly_books = fetch_polymarket_books(session, poly_token_ids, verbose=settings.verbose) if poly_token_ids else {}
    except Exception as exc:
        eprint(settings.verbose, f"Polymarket book resolution failed; falling back to any quotes already in the CSV: {exc}")
        poly_books = {}
    eprint(settings.verbose, f"Resolved {len(poly_books)} Polymarket books")

    polymarket_quotes: Dict[str, Quote] = {}
    for slug, raw_market in poly_markets.items():
        polymarket_quotes[slug] = quote_from_polymarket_market_and_books(raw_market, poly_books)

    enriched_rows = [
        enrich_row(
            row,
            kalshi_quotes=kalshi_quotes,
            polymarket_quotes=polymarket_quotes,
            settings=settings,
            snapshot_utc=snapshot_utc,
        )
        for row in rows
    ]

    ranked_rows = sort_enriched_rows(enriched_rows, sort=settings.sort)
    filtered_rows = maybe_filter_rows(ranked_rows, min_edge_cents=settings.min_edge_cents)

    ranked_with_positions: List[Dict[str, Any]] = []
    for idx, row in enumerate(filtered_rows, start=1):
        row_with_rank = dict(row)
        row_with_rank["rank"] = str(idx)
        ranked_with_positions.append(row_with_rank)

    output_fieldnames = build_output_fieldnames(["rank", *fieldnames])
    write_csv(str(output_path), output_fieldnames, ranked_with_positions)

    print_table(ranked_with_positions, top=settings.top)
    print(f"\nWrote {len(filtered_rows)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
