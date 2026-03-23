#!/usr/bin/env python3
"""
Export a clean market catalog from Kalshi and Polymarket for later line-matching.

This tool intentionally does NOT read order books.
It only fetches public market-discovery data plus the venues' listed probability fields
and writes two CSVs:
  - one for Kalshi
  - one for Polymarket

The goal is to create a high-signal dataset with enough identifying fields to later
build a strong cross-book line matcher.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
POLY_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

DEFAULT_USER_AGENT = "cross-book-market-catalog-exporter/1.0"

HTTP_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

SPORT_HINTS = {
    "mlb",
    "nba",
    "wnba",
    "nfl",
    "nhl",
    "ufc",
    "mma",
    "boxing",
    "soccer",
    "football",
    "baseball",
    "basketball",
    "tennis",
    "golf",
    "ncaab",
    "ncaaf",
    "ncaa",
    "march madness",
    "premier league",
    "champions league",
    "serie a",
    "la liga",
    "bundesliga",
    "ligue 1",
    "cricket",
    "f1",
    "formula 1",
    "nascar",
    "horse racing",
}

STOP_WORDS = {
    "the",
    "a",
    "an",
    "of",
    "to",
    "for",
    "at",
    "in",
    "on",
    "by",
    "vs",
    "v",
    "will",
    "be",
    "is",
    "are",
    "with",
    "and",
    "or",
    "game",
    "match",
}

TEAM_SPLIT_PATTERNS = [
    re.compile(r"\b(.+?)\s+vs\.?\s+(.+?)\b", re.IGNORECASE),
    re.compile(r"\b(.+?)\s+v\.?\s+(.+?)\b", re.IGNORECASE),
    re.compile(r"\b(.+?)\s+at\s+(.+?)\b", re.IGNORECASE),
    re.compile(r"\b(.+?)\s+@\s+(.+?)\b", re.IGNORECASE),
]

LINE_REGEX = re.compile(r"(?<!\d)([-+]?\d+(?:\.\d+)?)(?!\d)")


class HttpError(RuntimeError):
    """Raised when a request fails after retries."""


@dataclass(frozen=True)
class ExportSettings:
    output_dir: str
    kalshi_csv_name: str
    polymarket_csv_name: str
    log_file_name: str
    timeout_seconds: float
    max_retries: int
    base_backoff_seconds: float
    max_backoff_seconds: float
    kalshi_limit_per_page: int
    polymarket_limit_per_page: int
    kalshi_pause_seconds: float
    polymarket_pause_seconds: float
    sports_only: bool
    verbose: bool
    kalshi_max_pages: int
    polymarket_max_pages: int


def parse_args(argv: Optional[Sequence[str]] = None) -> ExportSettings:
    parser = argparse.ArgumentParser(
        description=(
            "Export open Kalshi and active Polymarket markets to two CSVs with strong logging "
            "and matching-friendly identifying fields."
        )
    )
    parser.add_argument("--output-dir", default=".", help="Directory to write the CSVs and log file into.")
    parser.add_argument(
        "--kalshi-csv",
        default="kalshi_markets_export.csv",
        help="Kalshi CSV output filename.",
    )
    parser.add_argument(
        "--polymarket-csv",
        default="polymarket_markets_export.csv",
        help="Polymarket CSV output filename.",
    )
    parser.add_argument(
        "--log-file",
        default="cross_book_market_export.log",
        help="Log filename.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=8,
        help="Maximum retries per HTTP request on transient errors.",
    )
    parser.add_argument(
        "--base-backoff-seconds",
        type=float,
        default=0.75,
        help="Base exponential-backoff delay in seconds.",
    )
    parser.add_argument(
        "--max-backoff-seconds",
        type=float,
        default=20.0,
        help="Maximum exponential-backoff delay in seconds.",
    )
    parser.add_argument(
        "--kalshi-limit",
        type=int,
        default=1000,
        help="Kalshi page size. Kalshi docs say GET /markets supports up to 1000.",
    )
    parser.add_argument(
        "--polymarket-limit",
        type=int,
        default=500,
        help="Polymarket page size.",
    )
    parser.add_argument(
        "--kalshi-pause-seconds",
        type=float,
        default=0.15,
        help="Pause between Kalshi pages.",
    )
    parser.add_argument(
        "--polymarket-pause-seconds",
        type=float,
        default=0.10,
        help="Pause between Polymarket pages.",
    )
    parser.add_argument(
        "--all-markets",
        action="store_true",
        help="Include non-sports markets too. By default the exporter keeps only sports-like / line-like markets.",
    )
    parser.add_argument(
        "--kalshi-max-pages",
        type=int,
        default=1000,
        help="Optional Kalshi page cap. 0 means no page cap.",
    )
    parser.add_argument(
        "--polymarket-max-pages",
        type=int,
        default=1000,
        help="Optional Polymarket page cap. 0 means no page cap.",
    )
    parser.add_argument("--verbose", action="store_true", help="Log extra progress details.")
    ns = parser.parse_args(argv)

    return ExportSettings(
        output_dir=ns.output_dir,
        kalshi_csv_name=ns.kalshi_csv,
        polymarket_csv_name=ns.polymarket_csv,
        log_file_name=ns.log_file,
        timeout_seconds=float(ns.timeout_seconds),
        max_retries=int(ns.max_retries),
        base_backoff_seconds=float(ns.base_backoff_seconds),
        max_backoff_seconds=float(ns.max_backoff_seconds),
        kalshi_limit_per_page=int(ns.kalshi_limit),
        polymarket_limit_per_page=int(ns.polymarket_limit),
        kalshi_pause_seconds=float(ns.kalshi_pause_seconds),
        polymarket_pause_seconds=float(ns.polymarket_pause_seconds),
        sports_only=not bool(ns.all_markets),
        verbose=bool(ns.verbose),
        kalshi_max_pages=int(ns.kalshi_max_pages),
        polymarket_max_pages=int(ns.polymarket_max_pages),
    )


def setup_logging(log_path: str, verbose: bool) -> None:
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    return session


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[dict] = None,
    timeout_seconds: float,
    max_retries: int,
    base_backoff_seconds: float,
    max_backoff_seconds: float,
    request_name: str,
) -> Any:
    logger = logging.getLogger(__name__)

    for attempt_index in range(max_retries + 1):
        try:
            response = session.request(method=method, url=url, params=params, timeout=timeout_seconds)
        except requests.RequestException as exc:
            if attempt_index >= max_retries:
                raise HttpError(f"{request_name} failed after retries: {exc}") from exc
            sleep_seconds = min(max_backoff_seconds, base_backoff_seconds * (2 ** attempt_index))
            sleep_seconds += random.uniform(0, 0.25)
            logger.warning(
                "%s request exception on attempt %s/%s: %s; sleeping %.2fs",
                request_name,
                attempt_index + 1,
                max_retries + 1,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
            continue

        if response.status_code < 400:
            try:
                return response.json()
            except ValueError as exc:
                raise HttpError(f"{request_name} returned non-JSON payload") from exc

        if response.status_code not in HTTP_RETRY_STATUS_CODES or attempt_index >= max_retries:
            raise HttpError(f"{request_name} failed {response.status_code}: {response.text[:500]}")

        retry_after_header = response.headers.get("Retry-After")
        if retry_after_header:
            try:
                sleep_seconds = float(retry_after_header)
            except ValueError:
                sleep_seconds = min(max_backoff_seconds, base_backoff_seconds * (2 ** attempt_index))
        else:
            sleep_seconds = min(max_backoff_seconds, base_backoff_seconds * (2 ** attempt_index))
        sleep_seconds += random.uniform(0, 0.25)

        logger.warning(
            "%s got HTTP %s on attempt %s/%s; sleeping %.2fs",
            request_name,
            response.status_code,
            attempt_index + 1,
            max_retries + 1,
            sleep_seconds,
        )
        time.sleep(sleep_seconds)

    raise HttpError(f"{request_name} failed unexpectedly")


def safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def dollars_to_percent(value: Any) -> Optional[float]:
    raw = safe_float(value)
    if raw is None:
        return None
    return raw * 100.0


def parse_jsonish_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return [value]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(value: Any) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9.+\-@/ ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_token_list(value: Any) -> str:
    tokens = [token for token in normalize_text(value).split() if token and token not in STOP_WORDS]
    return " ".join(tokens)


def compact_search_blob(parts: Iterable[Any]) -> str:
    normalized_parts = [normalize_text(part) for part in parts if clean_text(part)]
    return " | ".join(part for part in normalized_parts if part)


def extract_line_value(*texts: Any, explicit_line: Any = None) -> Optional[float]:
    if explicit_line not in (None, ""):
        numeric = safe_float(explicit_line)
        if numeric is not None:
            return numeric

    for text in texts:
        normalized = normalize_text(text)
        if not normalized:
            continue
        for match in LINE_REGEX.finditer(normalized):
            try:
                return float(match.group(1))
            except ValueError:
                continue
    return None


def extract_side_hint(*texts: Any) -> str:
    joined = " ".join(normalize_text(text) for text in texts if clean_text(text))
    if not joined:
        return ""

    if "over" in joined:
        return "over"
    if "under" in joined:
        return "under"
    if "yes" in joined and "no" not in joined:
        return "yes"
    if "no" in joined and "yes" not in joined:
        return "no"
    if "wins" in joined or "winner" in joined or "to win" in joined:
        return "winner"
    return ""


def detect_market_family(*texts: Any, sports_market_type: Any = None) -> str:
    smt = normalize_text(sports_market_type)
    if smt:
        if "total" in smt:
            return "total"
        if "spread" in smt or "handicap" in smt:
            return "spread"
        if "moneyline" in smt or "winner" in smt:
            return "moneyline"
        if "team total" in smt:
            return "team_total"
        return smt

    joined = " ".join(normalize_text(text) for text in texts if clean_text(text))
    if not joined:
        return ""

    if "over" in joined or "under" in joined or "total" in joined:
        return "total"
    if "spread" in joined or "handicap" in joined:
        return "spread"
    if "winner" in joined or "wins" in joined or "to win" in joined or "moneyline" in joined:
        return "moneyline"
    if re.search(r"[-+]\d+(?:\.\d+)?", joined):
        return "spread_or_line"
    return "other"


def parse_team_pair(*texts: Any) -> Tuple[str, str]:
    for text in texts:
        cleaned = clean_text(text)
        if not cleaned:
            continue
        normalized = cleaned
        for pattern in TEAM_SPLIT_PATTERNS:
            match = pattern.search(normalized)
            if match:
                team_a = clean_text(match.group(1))
                team_b = clean_text(match.group(2))
                if team_a and team_b:
                    return team_a, team_b
    return "", ""


def infer_sports_like(*texts: Any, explicit_category: Any = None, explicit_market_type: Any = None, explicit_game_id: Any = None) -> bool:
    category_text = normalize_text(explicit_category)
    market_type_text = normalize_text(explicit_market_type)
    if explicit_game_id not in (None, ""):
        return True
    joined = " ".join(normalize_text(text) for text in texts if clean_text(text))
    haystack = " ".join(part for part in [category_text, market_type_text, joined] if part)
    if not haystack:
        return False
    return any(hint in haystack for hint in SPORT_HINTS)


def format_percent(value: Optional[float]) -> str:
    if value is None or math.isnan(value):
        return ""
    return f"{value:.4f}"


def json_compact(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return clean_text(value)


def parse_outcome_prices(outcomes_value: Any, outcome_prices_value: Any) -> Tuple[Optional[float], Optional[float], str, str]:
    outcomes = parse_jsonish_list(outcomes_value)
    prices = parse_jsonish_list(outcome_prices_value)
    yes_percent: Optional[float] = None
    no_percent: Optional[float] = None

    for outcome, price in zip(outcomes, prices):
        outcome_text = clean_text(outcome).lower()
        numeric_price = safe_float(price)
        if numeric_price is None:
            continue
        percent_value = numeric_price * 100.0
        if outcome_text == "yes":
            yes_percent = percent_value
        elif outcome_text == "no":
            no_percent = percent_value

    return yes_percent, no_percent, json_compact(outcomes), json_compact(prices)


def preferred_kalshi_listed_yes_percent(market_payload: Dict[str, Any]) -> Optional[float]:
    last_trade = dollars_to_percent(market_payload.get("last_price_dollars"))
    if last_trade is not None:
        return last_trade

    yes_bid = dollars_to_percent(market_payload.get("yes_bid_dollars"))
    yes_ask = dollars_to_percent(market_payload.get("yes_ask_dollars"))
    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / 2.0
    return yes_bid if yes_bid is not None else yes_ask


def fetch_all_kalshi_events(session: requests.Session, settings: ExportSettings) -> Dict[str, Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    events_by_ticker: Dict[str, Dict[str, Any]] = {}
    cursor: Optional[str] = None
    page_count = 0

    while True:
        if settings.kalshi_max_pages and page_count >= settings.kalshi_max_pages:
            logger.info("Kalshi event fetch hit max page cap of %s", settings.kalshi_max_pages)
            break

        params = {
            "limit": 200,
            "status": "open",
            "with_nested_markets": "false",
        }
        if cursor:
            params["cursor"] = cursor

        payload = request_json(
            session,
            "GET",
            f"{KALSHI_BASE_URL}/events",
            params=params,
            timeout_seconds=settings.timeout_seconds,
            max_retries=settings.max_retries,
            base_backoff_seconds=settings.base_backoff_seconds,
            max_backoff_seconds=settings.max_backoff_seconds,
            request_name=f"Kalshi events page {page_count + 1}",
        )
        events = payload.get("events") or []
        page_count += 1
        logger.info("Fetched Kalshi events page %s with %s rows", page_count, len(events))

        for event in events:
            event_ticker = clean_text(event.get("event_ticker"))
            if event_ticker:
                events_by_ticker[event_ticker] = event

        cursor = clean_text(payload.get("cursor"))
        if not cursor or not events:
            break
        time.sleep(settings.kalshi_pause_seconds)

    logger.info("Kalshi event catalog complete: %s events", len(events_by_ticker))
    return events_by_ticker


def fetch_all_kalshi_markets(session: requests.Session, settings: ExportSettings) -> List[Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    all_markets: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page_count = 0

    while True:
        if settings.kalshi_max_pages and page_count >= settings.kalshi_max_pages:
            logger.info("Kalshi market fetch hit max page cap of %s", settings.kalshi_max_pages)
            break

        params = {
            "limit": settings.kalshi_limit_per_page,
            "status": "open",
        }
        if cursor:
            params["cursor"] = cursor

        payload = request_json(
            session,
            "GET",
            f"{KALSHI_BASE_URL}/markets",
            params=params,
            timeout_seconds=settings.timeout_seconds,
            max_retries=settings.max_retries,
            base_backoff_seconds=settings.base_backoff_seconds,
            max_backoff_seconds=settings.max_backoff_seconds,
            request_name=f"Kalshi markets page {page_count + 1}",
        )
        markets = payload.get("markets") or []
        page_count += 1
        logger.info(
            "Fetched Kalshi markets page %s with %s rows (cumulative %s)",
            page_count,
            len(markets),
            len(all_markets) + len(markets),
        )
        all_markets.extend(markets)

        cursor = clean_text(payload.get("cursor"))
        if not cursor or not markets:
            break
        time.sleep(settings.kalshi_pause_seconds)

    logger.info("Kalshi market catalog complete: %s markets", len(all_markets))
    return all_markets


def build_kalshi_rows(markets: List[Dict[str, Any]], events_by_ticker: Dict[str, Dict[str, Any]], settings: ExportSettings) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    logger = logging.getLogger(__name__)

    for market in markets:
        event_ticker = clean_text(market.get("event_ticker"))
        event_payload = events_by_ticker.get(event_ticker, {})

        event_title = clean_text(event_payload.get("title"))
        event_sub_title = clean_text(event_payload.get("sub_title"))
        event_category = clean_text(event_payload.get("category"))
        series_ticker = clean_text(event_payload.get("series_ticker"))
        strike_date = clean_text(event_payload.get("strike_date"))

        market_title = clean_text(market.get("title"))
        market_subtitle = clean_text(market.get("subtitle"))
        yes_sub_title = clean_text(market.get("yes_sub_title"))
        no_sub_title = clean_text(market.get("no_sub_title"))

        sports_like = infer_sports_like(
            event_title,
            event_sub_title,
            market_title,
            market_subtitle,
            yes_sub_title,
            no_sub_title,
            explicit_category=event_category,
            explicit_market_type=market.get("market_type"),
        )
        if settings.sports_only and not sports_like:
            continue

        listed_yes_percent = preferred_kalshi_listed_yes_percent(market)
        listed_no_percent = (100.0 - listed_yes_percent) if listed_yes_percent is not None else None
        yes_bid_percent = dollars_to_percent(market.get("yes_bid_dollars"))
        yes_ask_percent = dollars_to_percent(market.get("yes_ask_dollars"))
        no_bid_percent = dollars_to_percent(market.get("no_bid_dollars"))
        no_ask_percent = dollars_to_percent(market.get("no_ask_dollars"))
        last_yes_percent = dollars_to_percent(market.get("last_price_dollars"))

        market_family = detect_market_family(
            event_title,
            event_sub_title,
            market_title,
            market_subtitle,
            yes_sub_title,
            no_sub_title,
        )
        line_value = extract_line_value(
            event_title,
            event_sub_title,
            market_title,
            market_subtitle,
            yes_sub_title,
            no_sub_title,
        )
        side_hint = extract_side_hint(market_title, market_subtitle, yes_sub_title)
        team_a, team_b = parse_team_pair(event_title, event_sub_title, market_title, market_subtitle)

        normalized_event = normalize_token_list(" ".join(part for part in [event_title, event_sub_title] if part))
        normalized_market = normalize_token_list(" ".join(part for part in [market_title, market_subtitle, yes_sub_title, no_sub_title] if part))
        search_blob = compact_search_blob(
            [
                event_title,
                event_sub_title,
                event_category,
                market_title,
                market_subtitle,
                yes_sub_title,
                no_sub_title,
                event_ticker,
                series_ticker,
            ]
        )

        row = {
            "source": "kalshi",
            "sports_like": str(bool(sports_like)).lower(),
            "event_category": event_category,
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "market_ticker": clean_text(market.get("ticker")),
            "market_status": clean_text(market.get("status")),
            "market_type": clean_text(market.get("market_type")),
            "event_title": event_title,
            "event_sub_title": event_sub_title,
            "market_title": market_title,
            "market_subtitle": market_subtitle,
            "yes_sub_title": yes_sub_title,
            "no_sub_title": no_sub_title,
            "open_time": clean_text(market.get("open_time")),
            "close_time": clean_text(market.get("close_time")),
            "strike_date": strike_date,
            "listed_yes_percent": format_percent(listed_yes_percent),
            "listed_no_percent": format_percent(listed_no_percent),
            "last_yes_percent": format_percent(last_yes_percent),
            "yes_bid_percent": format_percent(yes_bid_percent),
            "yes_ask_percent": format_percent(yes_ask_percent),
            "no_bid_percent": format_percent(no_bid_percent),
            "no_ask_percent": format_percent(no_ask_percent),
            "market_family": market_family,
            "line_value": "" if line_value is None else f"{line_value:.4f}",
            "side_hint": side_hint,
            "team_a": team_a,
            "team_b": team_b,
            "normalized_event": normalized_event,
            "normalized_market": normalized_market,
            "search_blob": search_blob,
        }
        rows.append(row)

    logger.info("Kalshi rows built: %s", len(rows))
    return rows


def fetch_all_polymarket_markets(session: requests.Session, settings: ExportSettings) -> List[Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    all_markets: List[Dict[str, Any]] = []
    offset = 0
    page_count = 0

    while True:
        if settings.polymarket_max_pages and page_count >= settings.polymarket_max_pages:
            logger.info("Polymarket fetch hit max page cap of %s", settings.polymarket_max_pages)
            break

        params = {
            "limit": settings.polymarket_limit_per_page,
            "offset": offset,
            "active": "true",
            "closed": "false",
        }
        payload = request_json(
            session,
            "GET",
            f"{POLY_GAMMA_BASE_URL}/markets",
            params=params,
            timeout_seconds=settings.timeout_seconds,
            max_retries=settings.max_retries,
            base_backoff_seconds=settings.base_backoff_seconds,
            max_backoff_seconds=settings.max_backoff_seconds,
            request_name=f"Polymarket markets page {page_count + 1}",
        )

        if not isinstance(payload, list):
            raise HttpError("Polymarket markets endpoint returned an unexpected payload shape")

        page_count += 1
        logger.info(
            "Fetched Polymarket page %s with %s rows (cumulative %s)",
            page_count,
            len(payload),
            len(all_markets) + len(payload),
        )
        all_markets.extend(payload)

        if len(payload) < settings.polymarket_limit_per_page:
            break
        offset += settings.polymarket_limit_per_page
        time.sleep(settings.polymarket_pause_seconds)

    logger.info("Polymarket market catalog complete: %s markets", len(all_markets))
    return all_markets


def first_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, list) and value:
        first_item = value[0]
        if isinstance(first_item, dict):
            return first_item
    return {}


def build_polymarket_rows(markets: List[Dict[str, Any]], settings: ExportSettings) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    logger = logging.getLogger(__name__)

    for market in markets:
        first_event = first_mapping(market.get("events"))
        first_series = first_mapping(first_event.get("series"))
        first_collection = first_mapping(first_event.get("collections"))

        event_title = clean_text(first_event.get("title"))
        event_slug = clean_text(first_event.get("slug"))
        event_ticker = clean_text(first_event.get("ticker"))
        event_subtitle = clean_text(first_event.get("subtitle"))
        event_category = clean_text(first_event.get("category"))

        series_title = clean_text(first_series.get("title"))
        collection_title = clean_text(first_collection.get("title"))

        question = clean_text(market.get("question"))
        slug = clean_text(market.get("slug"))
        group_item_title = clean_text(market.get("groupItemTitle"))
        sports_market_type = clean_text(market.get("sportsMarketType"))
        category = clean_text(market.get("category"))
        game_id = clean_text(market.get("gameId"))

        sports_like = infer_sports_like(
            question,
            event_title,
            event_subtitle,
            group_item_title,
            series_title,
            collection_title,
            explicit_category=(category or event_category),
            explicit_market_type=sports_market_type,
            explicit_game_id=game_id,
        )
        if settings.sports_only and not sports_like:
            continue

        listed_yes_percent, listed_no_percent, outcomes_raw, outcome_prices_raw = parse_outcome_prices(
            market.get("outcomes"),
            market.get("outcomePrices"),
        )

        market_family = detect_market_family(
            question,
            event_title,
            event_subtitle,
            group_item_title,
            sports_market_type=sports_market_type,
        )
        line_value = extract_line_value(
            question,
            event_title,
            event_subtitle,
            group_item_title,
            explicit_line=market.get("line"),
        )
        side_hint = extract_side_hint(question, group_item_title)
        team_a, team_b = parse_team_pair(event_title, question, group_item_title)

        normalized_event = normalize_token_list(" ".join(part for part in [event_title, event_subtitle, series_title, collection_title] if part))
        normalized_market = normalize_token_list(" ".join(part for part in [question, group_item_title, sports_market_type] if part))
        search_blob = compact_search_blob(
            [
                category,
                event_category,
                sports_market_type,
                event_title,
                event_subtitle,
                series_title,
                collection_title,
                question,
                group_item_title,
                slug,
                event_slug,
                event_ticker,
                game_id,
            ]
        )

        row = {
            "source": "polymarket",
            "sports_like": str(bool(sports_like)).lower(),
            "category": category,
            "event_category": event_category,
            "market_id": clean_text(market.get("id")),
            "condition_id": clean_text(market.get("conditionId")),
            "question_id": clean_text(market.get("questionID") or market.get("questionId")),
            "slug": slug,
            "active": str(bool(market.get("active"))).lower(),
            "closed": str(bool(market.get("closed"))).lower(),
            "question": question,
            "group_item_title": group_item_title,
            "sports_market_type": sports_market_type,
            "line": "" if line_value is None else f"{line_value:.4f}",
            "game_id": game_id,
            "event_title": event_title,
            "event_subtitle": event_subtitle,
            "event_slug": event_slug,
            "event_ticker": event_ticker,
            "series_title": series_title,
            "collection_title": collection_title,
            "start_date": clean_text(market.get("startDate") or market.get("startDateIso")),
            "end_date": clean_text(market.get("endDate") or market.get("endDateIso")),
            "event_start_time": clean_text(market.get("eventStartTime") or market.get("gameStartTime")),
            "listed_yes_percent": format_percent(listed_yes_percent),
            "listed_no_percent": format_percent(listed_no_percent),
            "outcomes_raw": outcomes_raw,
            "outcome_prices_raw": outcome_prices_raw,
            "market_family": market_family,
            "side_hint": side_hint,
            "team_a": team_a,
            "team_b": team_b,
            "normalized_event": normalized_event,
            "normalized_market": normalized_market,
            "search_blob": search_blob,
        }
        rows.append(row)

    logger.info("Polymarket rows built: %s", len(rows))
    return rows


def ordered_fieldnames(rows: List[Dict[str, Any]], preferred: Sequence[str]) -> List[str]:
    fieldnames = list(preferred)
    seen = set(fieldnames)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    return fieldnames


def write_csv(path: str, rows: List[Dict[str, Any]], preferred_fieldnames: Sequence[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fieldnames = ordered_fieldnames(rows, preferred_fieldnames)
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def export_kalshi(settings: ExportSettings) -> List[Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    session = build_session()
    try:
        events_by_ticker = fetch_all_kalshi_events(session, settings)
        markets = fetch_all_kalshi_markets(session, settings)
        rows = build_kalshi_rows(markets, events_by_ticker, settings)
        logger.info("Kalshi export ready with %s rows", len(rows))
        return rows
    finally:
        session.close()


def export_polymarket(settings: ExportSettings) -> List[Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    session = build_session()
    try:
        markets = fetch_all_polymarket_markets(session, settings)
        rows = build_polymarket_rows(markets, settings)
        logger.info("Polymarket export ready with %s rows", len(rows))
        return rows
    finally:
        session.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    settings = parse_args(argv)
    log_path = os.path.join(settings.output_dir, settings.log_file_name)
    setup_logging(log_path, settings.verbose)
    logger = logging.getLogger(__name__)

    logger.info("Starting cross-book market catalog export")
    logger.info(
        "Config sports_only=%s kalshi_limit=%s polymarket_limit=%s output_dir=%s",
        settings.sports_only,
        settings.kalshi_limit_per_page,
        settings.polymarket_limit_per_page,
        os.path.abspath(settings.output_dir),
    )

    kalshi_rows: List[Dict[str, Any]] = []
    polymarket_rows: List[Dict[str, Any]] = []
    errors: List[str] = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_map = {
            executor.submit(export_kalshi, settings): "kalshi",
            executor.submit(export_polymarket, settings): "polymarket",
        }
        for future in as_completed(future_map):
            source_name = future_map[future]
            try:
                rows = future.result()
            except Exception as exc:  # noqa: BLE001
                message = f"{source_name} export failed: {exc}"
                logger.exception(message)
                errors.append(message)
                continue

            if source_name == "kalshi":
                kalshi_rows = rows
            else:
                polymarket_rows = rows

    kalshi_csv_path = os.path.join(settings.output_dir, settings.kalshi_csv_name)
    polymarket_csv_path = os.path.join(settings.output_dir, settings.polymarket_csv_name)

    kalshi_preferred = [
        "source",
        "sports_like",
        "event_category",
        "series_ticker",
        "event_ticker",
        "market_ticker",
        "event_title",
        "event_sub_title",
        "market_title",
        "market_subtitle",
        "yes_sub_title",
        "no_sub_title",
        "market_family",
        "line_value",
        "side_hint",
        "team_a",
        "team_b",
        "listed_yes_percent",
        "listed_no_percent",
        "last_yes_percent",
        "yes_bid_percent",
        "yes_ask_percent",
        "no_bid_percent",
        "no_ask_percent",
        "open_time",
        "close_time",
        "strike_date",
        "normalized_event",
        "normalized_market",
        "search_blob",
    ]
    polymarket_preferred = [
        "source",
        "sports_like",
        "category",
        "event_category",
        "market_id",
        "condition_id",
        "question_id",
        "slug",
        "event_title",
        "event_subtitle",
        "event_slug",
        "event_ticker",
        "series_title",
        "collection_title",
        "question",
        "group_item_title",
        "sports_market_type",
        "market_family",
        "line",
        "side_hint",
        "team_a",
        "team_b",
        "listed_yes_percent",
        "listed_no_percent",
        "start_date",
        "end_date",
        "event_start_time",
        "normalized_event",
        "normalized_market",
        "search_blob",
        "outcomes_raw",
        "outcome_prices_raw",
    ]

    if kalshi_rows:
        write_csv(kalshi_csv_path, kalshi_rows, kalshi_preferred)
        logger.info("Wrote Kalshi CSV: %s (%s rows)", kalshi_csv_path, len(kalshi_rows))
    else:
        logger.warning("No Kalshi rows to write")

    if polymarket_rows:
        write_csv(polymarket_csv_path, polymarket_rows, polymarket_preferred)
        logger.info("Wrote Polymarket CSV: %s (%s rows)", polymarket_csv_path, len(polymarket_rows))
    else:
        logger.warning("No Polymarket rows to write")

    logger.info("Export complete")
    if errors:
        logger.error("Completed with %s error(s)", len(errors))
        for error in errors:
            logger.error(error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
