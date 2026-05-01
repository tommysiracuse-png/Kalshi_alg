
#!/usr/bin/env python3
"""
Kalshi vs Polymarket sports market matcher.

Goal
----
Find *matching sports markets* across Kalshi and Polymarket and show the
displayed probability for the YES side on each venue.

What this script does
---------------------
1. Fetches open Kalshi events with nested markets from the public Events API.
2. Fetches active Polymarket events with nested markets from the public Gamma API.
3. Optionally fetches Polymarket's public teams catalog to improve team alias
   matching and side extraction.
4. Builds normalized market identities:
   - teams / event
   - segment (full game / 1H / Q1 / map 2 / set 1 / etc.)
   - market type (moneyline / total / spread)
   - line (for totals/spreads)
   - YES-side proposition
   - displayed YES probability
5. Matches only plausibly identical markets and ranks them by probability gap
   (or confidence).

Important notes
---------------
- This script intentionally does *not* call order-book endpoints.
- On Polymarket, it prefers `outcomePrices`, which are already implied
  probabilities for each outcome.
- On Kalshi, it prefers `last_price_dollars` and only falls back to bid/ask
  fields already present on the market object if needed.
- The defaults are conservative on proposition matching.
- By default, the script returns *all* confident cross-book candidates rather
  than forcing a one-to-one pairing, so you can review more markets. Use
  `--one-to-one` for a cleaner de-duplicated list.
- For broader coverage on ambiguous market wording, use
  `--include-uncertain-sides`.

Examples
--------
python cross_book_matcher.py --top 100 --output-csv matches.csv --verbose
python cross_book_matcher.py --sort confidence --min-confidence 0.90
python cross_book_matcher.py --include-uncertain-sides --top 200
python cross_book_matcher.py --one-to-one --top 100
python cross_book_matcher.py --self-test
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

import requests


# ---------------------------------------------------------------------------
# API configuration
# ---------------------------------------------------------------------------

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
POLY_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

USER_AGENT = "cross-book-market-matcher/3.0"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 25
DEFAULT_HTTP_MAX_RETRIES = 8
DEFAULT_HTTP_BACKOFF_SECONDS = 1.0
DEFAULT_HTTP_BACKOFF_MULTIPLIER = 1.8
DEFAULT_HTTP_MAX_BACKOFF_SECONDS = 30.0
DEFAULT_PAGE_PAUSE_SECONDS = 0.08

KALSHI_EVENTS_PAGE_SIZE = 200   # docs max=200
POLY_EVENTS_PAGE_SIZE = 100     # examples use 100, safe default
POLY_TEAMS_PAGE_SIZE = 500      # docs don't publish a max; 500 is a safe batch

SPORT_CATEGORY_HINTS = {
    "sports",
    "baseball",
    "basketball",
    "boxing",
    "cricket",
    "esports",
    "football",
    "golf",
    "hockey",
    "mma",
    "rugby",
    "soccer",
    "tennis",
    "ncaa",
    "ncaab",
    "ncaaf",
    "mlb",
    "nba",
    "wnba",
    "nfl",
    "nhl",
    "epl",
    "uefa",
    "ufc",
    "wta",
    "atp",
}

GENERIC_TOKENS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "draw",
    "fc",
    "cf",
    "sc",
    "ac",
    "afc",
    "bc",
    "club",
    "team",
    "teams",
    "winner",
    "moneyline",
    "match",
    "game",
    "games",
    "will",
    "win",
    "wins",
    "vs",
    "v",
    "versus",
    "spread",
    "handicap",
    "line",
    "total",
    "totals",
    "over",
    "under",
    "o",
    "u",
    "ou",
    "o/u",
    "points",
    "point",
    "goals",
    "goal",
    "runs",
    "run",
    "map",
    "maps",
    "set",
    "sets",
    "period",
    "quarter",
    "half",
    "first",
    "second",
    "third",
    "fourth",
    "full",
    "time",
    "end",
    "in",
    "the",
    "of",
    "on",
    "to",
}

MONEYLINE_KEYWORDS = (
    "winner",
    "moneyline",
    "will win",
    "to win",
    "draw",
    "tie",
    "match end in a draw",
)
TOTAL_KEYWORDS = (
    "total",
    "totals",
    "o/u",
    "ou",
    "over under",
    "over/under",
)
SPREAD_KEYWORDS = (
    "spread",
    "handicap",
    "wins by",
    "margin",
)

TEAM_SEPARATOR_RE = re.compile(r"\s+(?:vs\.?|v\.?|versus|@|at)\s+", re.IGNORECASE)
QUESTION_CLEAN_RE = re.compile(r"[?!.]+$")
SPACE_RE = re.compile(r"\s+")
TOKEN_SPLIT_RE = re.compile(r"[^a-z0-9+.\-]+")
DRAW_RE = re.compile(r"\b(draw|tie)\b", re.IGNORECASE)
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
SIGNED_FLOAT_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)")

OVER_RE = re.compile(r"\bover\b", re.IGNORECASE)
UNDER_RE = re.compile(r"\bunder\b", re.IGNORECASE)
OU_RE = re.compile(r"\bo/?u\b", re.IGNORECASE)

TOTAL_LINE_RE = re.compile(
    r"(?:o/?u|over/?under|total(?:s)?(?:\s+(?:points|goals|runs))?)\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
SPREAD_LINE_RE = re.compile(
    r"(?:spread|handicap)\s*:?\s*([+-]?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
TEAM_WITH_SIGNED_LINE_RE = re.compile(
    r"(?P<team>.+?)\s+(?P<line>[+-]\d+(?:\.\d+)?)\b",
    re.IGNORECASE,
)
WILL_WIN_RE = re.compile(r"^\s*will\s+(.+?)\s+win(?:\s+on\b|\s+by\b|\?|$)", re.IGNORECASE)
MATCH_DRAW_RE = re.compile(r"match\s+end\s+in\s+a\s+draw", re.IGNORECASE)

SEGMENT_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(?:first|1st)\s+half\b|\b1h\b", re.IGNORECASE), "1H"),
    (re.compile(r"\b(?:second|2nd)\s+half\b|\b2h\b", re.IGNORECASE), "2H"),
    (re.compile(r"\b(?:first|1st)\s+quarter\b|\bq1\b|\b1q\b", re.IGNORECASE), "Q1"),
    (re.compile(r"\b(?:second|2nd)\s+quarter\b|\bq2\b|\b2q\b", re.IGNORECASE), "Q2"),
    (re.compile(r"\b(?:third|3rd)\s+quarter\b|\bq3\b|\b3q\b", re.IGNORECASE), "Q3"),
    (re.compile(r"\b(?:fourth|4th)\s+quarter\b|\bq4\b|\b4q\b", re.IGNORECASE), "Q4"),
    (re.compile(r"\b(?:first|1st)\s+period\b|\bp1\b|\b1p\b", re.IGNORECASE), "P1"),
    (re.compile(r"\b(?:second|2nd)\s+period\b|\bp2\b|\b2p\b", re.IGNORECASE), "P2"),
    (re.compile(r"\b(?:third|3rd)\s+period\b|\bp3\b|\b3p\b", re.IGNORECASE), "P3"),
    (re.compile(r"\bmap\s*(\d+)\b", re.IGNORECASE), "MAP"),
    (re.compile(r"\bset\s*(\d+)\b", re.IGNORECASE), "SET"),
]


class HttpError(RuntimeError):
    """Raised when a request fails after retries."""


@dataclass(frozen=True)
class ProbabilityQuote:
    yes_prob: Optional[float]          # 0..1
    source: str                        # e.g. "last_price", "outcomePrices", "midpoint_fallback"


@dataclass(frozen=True)
class ParsedMarket:
    source: str                        # "kalshi" or "polymarket"
    source_id: str                     # ticker or slug
    event_title: str
    market_title: str
    display_title: str
    event_time: Optional[datetime]
    market_type: str                   # moneyline / total / spread
    segment: str                       # FULL / 1H / Q1 / MAP2 / ...
    line: Optional[float]
    teams: Tuple[str, str]
    yes_side: Optional[str]            # team, draw, over, under, or team|+3.5
    yes_side_confident: bool
    probability: ProbabilityQuote
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class MatchResult:
    match_confidence: float
    team_score: float
    time_score: float
    line_score: float
    side_score: float
    probability_gap_pct_points: Optional[float]
    side_alignment: str
    needs_manual_review: bool
    event_time_utc: Optional[str]
    event_title: str
    market_type: str
    segment: str
    line: Optional[float]
    proposition: str
    kalshi_probability_pct: Optional[float]
    kalshi_probability_source: str
    kalshi_ticker: str
    kalshi_title: str
    kalshi_yes_side: Optional[str]
    polymarket_probability_pct: Optional[float]
    polymarket_probability_source: str
    polymarket_slug: str
    polymarket_question: str
    polymarket_yes_side: Optional[str]


@dataclass(frozen=True)
class Settings:
    top: int
    output_csv: Optional[str]
    verbose: bool
    min_confidence: float
    max_event_gap_hours: float
    kalshi_event_limit: Optional[int]
    polymarket_event_limit: Optional[int]
    include_uncertain_sides: bool
    one_to_one: bool
    skip_poly_teams: bool
    sort: str
    self_test: bool


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def eprint(verbose: bool, *parts: object) -> None:
    if verbose:
        print(*parts, file=sys.stderr)


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }
    )
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
    params: Optional[Mapping[str, Any]] = None,
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
                params=dict(params or {}),
                json=json_body,
                timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code < 400:
                try:
                    return response.json()
                except ValueError as exc:
                    raise HttpError(f"{method} {url} returned non-JSON content") from exc
            if response.status_code in retriable_statuses and attempt_index < DEFAULT_HTTP_MAX_RETRIES:
                sleep_seconds = _compute_backoff_seconds(response=response, attempt_index=attempt_index)
                eprint(verbose, f"Retrying {method} {url} after HTTP {response.status_code} (sleep={sleep_seconds:.2f}s)")
                time.sleep(sleep_seconds)
                continue
            raise HttpError(f"{method} {url} failed {response.status_code}: {response.text[:600]}")
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt_index >= DEFAULT_HTTP_MAX_RETRIES:
                break
            sleep_seconds = _compute_backoff_seconds(response=response, attempt_index=attempt_index)
            eprint(verbose, f"Retrying {method} {url} after {type(exc).__name__} (sleep={sleep_seconds:.2f}s)")
            time.sleep(sleep_seconds)
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


def parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return parsed
        # Fallback for plain comma-separated text
        if "," in text:
            return [item.strip() for item in text.split(",") if item.strip()]
        return [text]
    return []


def strip_accents(text: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))


def normalize_text(text: str) -> str:
    cleaned = strip_accents(text).lower().replace("&", " and ")
    cleaned = cleaned.replace("vs.", " vs ").replace("versus", " vs ").replace("@", " @ ")
    cleaned = cleaned.replace(":", " ")
    cleaned = TOKEN_SPLIT_RE.sub(" ", cleaned)
    cleaned = SPACE_RE.sub(" ", cleaned).strip()
    return cleaned


def team_tokens(text: str) -> Tuple[str, ...]:
    tokens = [
        token
        for token in normalize_text(text).split()
        if token and token not in GENERIC_TOKENS and not token.isdigit()
    ]
    return tuple(tokens)


def team_similarity(a: str, b: str) -> float:
    norm_a = normalize_text(a)
    norm_b = normalize_text(b)
    if not norm_a or not norm_b:
        return 0.0
    if norm_a == norm_b:
        return 1.0

    tokens_a = set(team_tokens(a))
    tokens_b = set(team_tokens(b))
    if not tokens_a or not tokens_b:
        return SequenceMatcher(None, norm_a, norm_b).ratio()

    intersection = tokens_a.intersection(tokens_b)
    if not intersection:
        return 0.0

    jaccard = len(intersection) / len(tokens_a.union(tokens_b))
    containment = len(intersection) / min(len(tokens_a), len(tokens_b))
    sequence = SequenceMatcher(None, " ".join(sorted(tokens_a)), " ".join(sorted(tokens_b))).ratio()

    # Penalize pure city-only overlaps when one side has a distinguishing trailing token.
    if len(intersection) == 2 and len(tokens_a.union(tokens_b)) >= 3 and sequence < 0.85:
        penalty = 0.88
    else:
        penalty = 1.0

    return max(0.0, min(1.0, penalty * (0.45 * containment + 0.35 * jaccard + 0.20 * sequence)))


def canonical_side_label(side_key: Optional[str]) -> str:
    if not side_key:
        return "?"
    if "|" in side_key:
        team, line = side_key.split("|", 1)
        return f"{team} {line}"
    return side_key


def format_line(line: Optional[float]) -> Optional[str]:
    if line is None:
        return None
    if abs(line - round(line)) < 1e-9:
        return f"{int(round(line))}"
    return f"{line:g}"


def line_bucket(line: Optional[float]) -> Optional[int]:
    if line is None:
        return None
    return int(round(line * 1000))


def probability_to_pct(probability: Optional[float]) -> Optional[float]:
    if probability is None:
        return None
    return probability * 100.0


def clamp_probability(probability: Optional[float]) -> Optional[float]:
    if probability is None:
        return None
    if probability < 0:
        return 0.0
    if probability > 1:
        return 1.0
    return probability


def proposition_label(market: ParsedMarket) -> str:
    if market.market_type == "moneyline":
        return canonical_side_label(market.yes_side)
    if market.market_type == "total":
        side = canonical_side_label(market.yes_side)
        line_text = format_line(market.line) or "?"
        return f"{side} {line_text}".strip()
    if market.market_type == "spread":
        return canonical_side_label(market.yes_side)
    return canonical_side_label(market.yes_side)


# ---------------------------------------------------------------------------
# Team catalog based on Polymarket's /teams endpoint
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TeamEntry:
    team_id: str
    name: str
    league: str
    aliases: Tuple[str, ...]


class TeamCatalog:
    def __init__(self, teams: Sequence[TeamEntry]) -> None:
        self.teams: List[TeamEntry] = list(teams)
        self.by_id: Dict[str, TeamEntry] = {}
        self.alias_to_names: Dict[str, Set[str]] = {}
        self.token_to_names: Dict[str, Set[str]] = {}
        self.name_to_entry: Dict[str, TeamEntry] = {}
        for team in self.teams:
            self.by_id[str(team.team_id)] = team
            self.name_to_entry[team.name] = team
            for alias in {team.name, *team.aliases}:
                normalized_alias = normalize_text(alias)
                if not normalized_alias:
                    continue
                self.alias_to_names.setdefault(normalized_alias, set()).add(team.name)
                for token in team_tokens(alias):
                    self.token_to_names.setdefault(token, set()).add(team.name)

    def get_name_by_id(self, team_id: Any) -> Optional[str]:
        if team_id in (None, ""):
            return None
        entry = self.by_id.get(str(team_id))
        return entry.name if entry is not None else None

    @lru_cache(maxsize=50000)
    def resolve(self, text: str) -> str:
        cleaned = normalize_text(text)
        if not cleaned:
            return text.strip()

        exact_names = self.alias_to_names.get(cleaned)
        if exact_names and len(exact_names) == 1:
            return next(iter(exact_names))

        tokens = set(team_tokens(text))
        candidate_names: Set[str] = set()
        for token in tokens:
            candidate_names.update(self.token_to_names.get(token, set()))

        if not candidate_names:
            return text.strip()

        scored: List[Tuple[float, str]] = []
        for team_name in candidate_names:
            entry = self.name_to_entry[team_name]
            best_score = max(team_similarity(text, alias) for alias in {entry.name, *entry.aliases})
            scored.append((best_score, team_name))
        scored.sort(reverse=True)

        best_score, best_name = scored[0]
        second_score = scored[1][0] if len(scored) >= 2 else 0.0

        # Be conservative with ambiguous city-only phrases like "Washington" or "Los Angeles".
        if best_score >= 0.97 and (best_score - second_score >= 0.03):
            return best_name
        if best_score >= 0.92 and second_score <= 0.82 and len(tokens) >= 2:
            return best_name
        return text.strip()


# ---------------------------------------------------------------------------
# Fetching helpers
# ---------------------------------------------------------------------------

def fetch_all_kalshi_open_events(
    session: requests.Session,
    *,
    limit: Optional[int],
    verbose: bool,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        params: Dict[str, Any] = {
            "status": "open",
            "with_nested_markets": "true",
            "limit": KALSHI_EVENTS_PAGE_SIZE,
        }
        if cursor:
            params["cursor"] = cursor
        payload = request_json(session, "GET", f"{KALSHI_BASE_URL}/events", params=params, verbose=verbose)
        raw_events = payload.get("events") or []
        if not isinstance(raw_events, list):
            raise HttpError("Unexpected Kalshi /events response shape; expected 'events' list")
        eprint(verbose, f"Fetched {len(raw_events)} Kalshi events (cursor={cursor or 'start'})")
        if not raw_events:
            break
        events.extend(raw_events)
        if limit is not None and len(events) >= limit:
            return events[:limit]
        cursor = payload.get("cursor") or None
        if not cursor:
            break
        time.sleep(DEFAULT_PAGE_PAUSE_SECONDS)
    return events


def fetch_all_polymarket_active_events(
    session: requests.Session,
    *,
    limit: Optional[int],
    verbose: bool,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    offset = 0
    while True:
        page_limit = POLY_EVENTS_PAGE_SIZE if limit is None else min(POLY_EVENTS_PAGE_SIZE, max(1, limit - len(events)))
        if page_limit <= 0:
            break
        params: Dict[str, Any] = {
            "active": "true",
            "closed": "false",
            "limit": page_limit,
            "offset": offset,
        }
        payload = request_json(session, "GET", f"{POLY_GAMMA_BASE_URL}/events", params=params, verbose=verbose)
        if not isinstance(payload, list):
            raise HttpError("Unexpected Polymarket /events response shape; expected a list")
        eprint(verbose, f"Fetched {len(payload)} Polymarket events (offset={offset})")
        if not payload:
            break
        events.extend(payload)
        if limit is not None and len(events) >= limit:
            return events[:limit]
        offset += len(payload)
        time.sleep(DEFAULT_PAGE_PAUSE_SECONDS)
    return events


def fetch_polymarket_team_catalog(session: requests.Session, *, verbose: bool) -> TeamCatalog:
    teams: List[TeamEntry] = []
    offset = 0
    while True:
        params = {"limit": POLY_TEAMS_PAGE_SIZE, "offset": offset}
        payload = request_json(session, "GET", f"{POLY_GAMMA_BASE_URL}/teams", params=params, verbose=verbose)
        if not isinstance(payload, list):
            raise HttpError("Unexpected Polymarket /teams response shape; expected a list")
        eprint(verbose, f"Fetched {len(payload)} Polymarket teams (offset={offset})")
        if not payload:
            break
        for raw_team in payload:
            if not isinstance(raw_team, dict):
                continue
            name = str(raw_team.get("name") or "").strip()
            if not name:
                continue
            aliases: List[str] = []
            abbreviation = str(raw_team.get("abbreviation") or "").strip()
            if abbreviation:
                aliases.append(abbreviation)
            alias_field = raw_team.get("alias")
            for alias in parse_jsonish_list(alias_field):
                alias_text = str(alias).strip()
                if alias_text:
                    aliases.append(alias_text)
            if isinstance(alias_field, str) and alias_field.strip() and alias_field.strip() not in aliases:
                # Handles non-JSON plain alias strings.
                aliases.extend([item for item in [piece.strip() for piece in alias_field.split(",")] if item])
            deduped_aliases = tuple(sorted({alias for alias in aliases if alias and normalize_text(alias) != normalize_text(name)}))
            teams.append(
                TeamEntry(
                    team_id=str(raw_team.get("id") or ""),
                    name=name,
                    league=str(raw_team.get("league") or "").strip(),
                    aliases=deduped_aliases,
                )
            )
        offset += len(payload)
        time.sleep(DEFAULT_PAGE_PAUSE_SECONDS)
    return TeamCatalog(teams)


# ---------------------------------------------------------------------------
# Market parsing
# ---------------------------------------------------------------------------

def is_probable_sports_event(category: str, title: str, market_type: Optional[str]) -> bool:
    normalized_category = normalize_text(category)
    if normalized_category in SPORT_CATEGORY_HINTS:
        return True
    if market_type is None:
        return False
    return bool(extract_event_teams(title))


def detect_segment(*texts: str) -> str:
    combined = " ".join(text for text in texts if text)
    normalized = normalize_text(combined)
    for pattern, prefix in SEGMENT_PATTERNS:
        match = pattern.search(normalized)
        if not match:
            continue
        if prefix in {"MAP", "SET"} and match.lastindex:
            return f"{prefix}{match.group(1)}"
        return prefix
    return "FULL"


def infer_market_type(*texts: str, explicit_type: Optional[str] = None) -> Optional[str]:
    combined = " ".join(text for text in texts if text)
    raw_joined = f"{explicit_type or ''} {combined}".strip()
    joined = normalize_text(raw_joined)
    if not joined:
        return None

    compact = joined.replace(" ", "")
    raw_lower = raw_joined.lower()

    if any(keyword in joined for keyword in SPREAD_KEYWORDS):
        return "spread"
    if any(keyword in joined for keyword in TOTAL_KEYWORDS):
        return "total"
    if any(keyword in joined for keyword in MONEYLINE_KEYWORDS):
        return "moneyline"

    # Common shorthands that normalization can separate.
    if "o/u" in raw_lower or re.search(r"\bo\s*/\s*u\b", raw_lower):
        return "total"
    if "over/under" in raw_lower or "over under" in joined:
        return "total"
    if compact.startswith("ou") or "ou" in compact:
        return "total"
    if "winner" in compact or "moneyline" in compact:
        return "moneyline"
    if DRAW_RE.search(raw_joined):
        return "moneyline"
    return None


def clean_team_phrase(text: str) -> str:
    cleaned = str(text).strip()
    cleaned = QUESTION_CLEAN_RE.sub("", cleaned)
    cleaned = re.sub(r"\bon\s+\d{4}-\d{2}-\d{2}\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\b(winner|moneyline|spread|handicap|total|totals|o/?u|over/?under|match|game)\b.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = cleaned.strip(" -:;,/")
    return cleaned


def extract_event_teams(text: str, team_catalog: Optional[TeamCatalog] = None) -> Optional[Tuple[str, str]]:
    if not text:
        return None
    normalized_text = QUESTION_CLEAN_RE.sub("", str(text).strip())
    parts = TEAM_SEPARATOR_RE.split(normalized_text, maxsplit=1)
    if len(parts) != 2:
        return None
    left = clean_team_phrase(parts[0])
    right = clean_team_phrase(parts[1])
    if not left or not right:
        return None
    if team_catalog is not None:
        left = team_catalog.resolve(left)
        right = team_catalog.resolve(right)
    if normalize_text(left) == normalize_text(right):
        return None
    return (left, right)


def event_team_match_scores(
    teams_a: Tuple[str, str],
    teams_b: Tuple[str, str],
) -> Tuple[float, Tuple[str, str]]:
    a1, a2 = teams_a
    b1, b2 = teams_b

    direct_scores = (team_similarity(a1, b1), team_similarity(a2, b2))
    cross_scores = (team_similarity(a1, b2), team_similarity(a2, b1))

    # Prefer the orientation with the better worst-side match; break ties by average.
    direct_key = (min(direct_scores), sum(direct_scores) / 2.0)
    cross_key = (min(cross_scores), sum(cross_scores) / 2.0)

    if cross_key > direct_key:
        return cross_key[1], (b2, b1)
    return direct_key[1], (b1, b2)


def min_event_team_score(
    teams_a: Tuple[str, str],
    teams_b: Tuple[str, str],
) -> float:
    a1, a2 = teams_a
    b1, b2 = teams_b
    direct = (team_similarity(a1, b1), team_similarity(a2, b2))
    cross = (team_similarity(a1, b2), team_similarity(a2, b1))
    return max(min(direct), min(cross))


def extract_line_from_text(text: str, market_type: Optional[str]) -> Optional[float]:
    if market_type not in {"total", "spread"}:
        return None
    if not text:
        return None
    text = str(text)

    if market_type == "total":
        match = TOTAL_LINE_RE.search(text)
        if match:
            return parse_float(match.group(1))
        # More lenient fallback: use the last unsigned number only if O/U or total appears.
        if OU_RE.search(text) or "total" in normalize_text(text):
            values = SIGNED_FLOAT_RE.findall(text)
            if values:
                return parse_float(values[-1])

    if market_type == "spread":
        match = SPREAD_LINE_RE.search(text)
        if match:
            return parse_float(match.group(1))
        # Fallback for labels like "Boston -5.5"
        match = TEAM_WITH_SIGNED_LINE_RE.search(text)
        if match:
            return parse_float(match.group("line"))
    return None


def extract_line_from_kalshi_ticker(ticker: str, market_type: Optional[str]) -> Optional[float]:
    if market_type not in {"total", "spread"} or not ticker:
        return None
    match = re.search(r"-([+-]?\d+(?:\.\d+)?)$", str(ticker).strip())
    if match:
        return parse_float(match.group(1))
    return None


def choose_kalshi_event_time(raw_market: Mapping[str, Any]) -> Optional[datetime]:
    # For sports, expected_expiration_time is often closer to the actual scheduled game completion
    # than close_time/expiration_time, which can be pushed out for delayed/postponed events.
    return (
        parse_datetime(raw_market.get("expected_expiration_time"))
        or parse_datetime(raw_market.get("open_time"))
        or parse_datetime(raw_market.get("close_time"))
        or parse_datetime(raw_market.get("expiration_time"))
    )


def choose_polymarket_event_time(raw_event: Mapping[str, Any], raw_market: Mapping[str, Any]) -> Optional[datetime]:
    return (
        parse_datetime(raw_market.get("eventStartTime"))
        or parse_datetime(raw_market.get("gameStartTime"))
        or parse_datetime(raw_event.get("startDate"))
        or parse_datetime(raw_market.get("startDate"))
        or parse_datetime(raw_event.get("endDate"))
    )


def parse_outcome_labels(raw_market: Mapping[str, Any]) -> List[str]:
    labels: List[str] = []
    for field in ("shortOutcomes", "outcomes"):
        for item in parse_jsonish_list(raw_market.get(field)):
            label = str(item).strip()
            if label:
                labels.append(label)
        if labels:
            break
    return labels


def parse_polymarket_probability(raw_market: Mapping[str, Any]) -> ProbabilityQuote:
    prices = [parse_float(item) for item in parse_jsonish_list(raw_market.get("outcomePrices"))]
    if prices and prices[0] is not None:
        return ProbabilityQuote(yes_prob=clamp_probability(prices[0]), source="outcomePrices")

    last_trade_price = parse_float(raw_market.get("lastTradePrice"))
    if last_trade_price is not None:
        return ProbabilityQuote(yes_prob=clamp_probability(last_trade_price), source="lastTradePrice")

    best_bid = parse_float(raw_market.get("bestBid"))
    best_ask = parse_float(raw_market.get("bestAsk"))
    if best_bid is not None and best_ask is not None:
        return ProbabilityQuote(yes_prob=clamp_probability((best_bid + best_ask) / 2.0), source="midpoint_fallback")
    if best_ask is not None:
        return ProbabilityQuote(yes_prob=clamp_probability(best_ask), source="bestAsk_fallback")
    if best_bid is not None:
        return ProbabilityQuote(yes_prob=clamp_probability(best_bid), source="bestBid_fallback")
    return ProbabilityQuote(yes_prob=None, source="missing")


def parse_kalshi_probability(raw_market: Mapping[str, Any]) -> ProbabilityQuote:
    last_price = parse_float(raw_market.get("last_price_dollars"))
    yes_bid = parse_float(raw_market.get("yes_bid_dollars"))
    yes_ask = parse_float(raw_market.get("yes_ask_dollars"))

    # Prefer last trade when it exists and is non-zero. If there have been no trades yet,
    # Kalshi often leaves last_price at 0.0, so use quoted prices already present on the
    # market response as a fallback.
    if last_price is not None and last_price > 0.0:
        return ProbabilityQuote(yes_prob=clamp_probability(last_price), source="last_price")

    if yes_bid is not None and yes_ask is not None and (yes_bid > 0.0 or yes_ask > 0.0):
        return ProbabilityQuote(yes_prob=clamp_probability((yes_bid + yes_ask) / 2.0), source="midpoint_fallback")
    if yes_ask is not None and yes_ask > 0.0:
        return ProbabilityQuote(yes_prob=clamp_probability(yes_ask), source="yes_ask_fallback")
    if yes_bid is not None and yes_bid > 0.0:
        return ProbabilityQuote(yes_prob=clamp_probability(yes_bid), source="yes_bid_fallback")

    return ProbabilityQuote(yes_prob=clamp_probability(last_price), source="last_price")


def best_matching_team(text: str, teams: Tuple[str, str], *, min_score: float = 0.88) -> Optional[str]:
    if not text:
        return None
    scored = sorted(((team_similarity(text, team), team) for team in teams), reverse=True)
    if not scored:
        return None
    best_score, best_team = scored[0]
    return best_team if best_score >= min_score else None


def parse_total_side(text: str) -> Optional[str]:
    if not text:
        return None
    normalized = normalize_text(text)
    has_over = OVER_RE.search(normalized) is not None
    has_under = UNDER_RE.search(normalized) is not None
    if has_over and not has_under:
        return "over"
    if has_under and not has_over:
        return "under"
    return None


def parse_spread_side(
    text: str,
    *,
    teams: Tuple[str, str],
    explicit_line: Optional[float],
) -> Optional[str]:
    if not text:
        return None
    text = str(text).strip()
    match = TEAM_WITH_SIGNED_LINE_RE.search(text)
    signed_line = parse_float(match.group("line")) if match else None
    line_value = signed_line if signed_line is not None else explicit_line
    team_text = match.group("team") if match else text
    team = best_matching_team(team_text, teams)
    if team is None or line_value is None:
        return None
    return f"{team}|{line_value:+g}"


def infer_kalshi_yes_side(
    raw_market: Mapping[str, Any],
    *,
    market_type: str,
    teams: Tuple[str, str],
    line: Optional[float],
    team_catalog: Optional[TeamCatalog],
) -> Tuple[Optional[str], bool]:
    yes_sub_title = str(raw_market.get("yes_sub_title") or "").strip()
    title = str(raw_market.get("title") or "").strip()
    subtitle = str(raw_market.get("subtitle") or "").strip()
    source_candidates = [yes_sub_title, title, subtitle]

    if market_type == "moneyline":
        for candidate in source_candidates:
            if DRAW_RE.search(candidate):
                return "draw", True
            resolved_text = team_catalog.resolve(candidate) if (team_catalog is not None and candidate) else candidate
            team = best_matching_team(resolved_text, teams)
            if team is not None:
                return team, True
        return None, False

    if market_type == "total":
        for candidate in source_candidates:
            side = parse_total_side(candidate)
            if side is not None:
                return side, True
        # On Kalshi total markets the YES side is typically the affirmative threshold side;
        # if titles are generic and yes_sub_title is empty, better to mark unknown than guess.
        return None, False

    if market_type == "spread":
        for candidate in source_candidates:
            side = parse_spread_side(candidate, teams=teams, explicit_line=line)
            if side is not None:
                return side, True
        return None, False

    return None, False


def infer_polymarket_yes_side(
    raw_event: Mapping[str, Any],
    raw_market: Mapping[str, Any],
    *,
    market_type: str,
    teams: Tuple[str, str],
    line: Optional[float],
    team_catalog: Optional[TeamCatalog],
) -> Tuple[Optional[str], bool]:
    question = str(raw_market.get("question") or "").strip()
    slug = str(raw_market.get("slug") or "").strip()
    group_item_title = str(raw_market.get("groupItemTitle") or "").strip()
    outcome_labels = parse_outcome_labels(raw_market)

    candidate_texts: List[str] = []
    if group_item_title:
        candidate_texts.append(group_item_title)
    candidate_texts.extend(outcome_labels)
    if question:
        candidate_texts.append(question)
    if slug:
        candidate_texts.append(slug)

    if market_type == "moneyline":
        for candidate in candidate_texts:
            if DRAW_RE.search(candidate) or MATCH_DRAW_RE.search(candidate):
                return "draw", True
            resolved_candidate = team_catalog.resolve(candidate) if (team_catalog is not None and candidate) else candidate
            team = best_matching_team(resolved_candidate, teams)
            if team is not None:
                return team, True

        # Fallback for direct "Will <team> win?" wording.
        win_match = WILL_WIN_RE.match(question)
        if win_match:
            candidate = win_match.group(1)
            resolved_candidate = team_catalog.resolve(candidate) if (team_catalog is not None and candidate) else candidate
            team = best_matching_team(resolved_candidate, teams)
            if team is not None:
                return team, True
        return None, False

    if market_type == "total":
        for candidate in candidate_texts:
            side = parse_total_side(candidate)
            if side is not None:
                return side, True
        # For Polymarket O/U sports markets, the YES side is generally the first listed
        # outcome / market direction. If we only see an "O/U 2.5" style question and no
        # richer labels, keep it but mark it uncertain unless we can infer "over".
        if question and OU_RE.search(question):
            return "over", False
        return None, False

    if market_type == "spread":
        for candidate in candidate_texts:
            resolved_candidate = team_catalog.resolve(candidate) if (team_catalog is not None and candidate) else candidate
            side = parse_spread_side(resolved_candidate, teams=teams, explicit_line=line)
            if side is not None:
                return side, True
        return None, False

    return None, False


def resolve_polymarket_event_teams(
    raw_event: Mapping[str, Any],
    raw_market: Mapping[str, Any],
    team_catalog: Optional[TeamCatalog],
) -> Optional[Tuple[str, str]]:
    if team_catalog is not None:
        team_a = team_catalog.get_name_by_id(raw_market.get("teamAID"))
        team_b = team_catalog.get_name_by_id(raw_market.get("teamBID"))
        if team_a and team_b and normalize_text(team_a) != normalize_text(team_b):
            return (team_a, team_b)

    for text in (
        str(raw_event.get("title") or "").strip(),
        str(raw_market.get("question") or "").strip(),
        str(raw_event.get("subtitle") or "").strip(),
        str(raw_market.get("slug") or "").replace("-", " "),
    ):
        teams = extract_event_teams(text, team_catalog=team_catalog)
        if teams is not None:
            return teams
    return None


def build_kalshi_markets(
    raw_events: Sequence[Mapping[str, Any]],
    *,
    team_catalog: Optional[TeamCatalog],
    verbose: bool,
) -> List[ParsedMarket]:
    markets: List[ParsedMarket] = []
    for raw_event in raw_events:
        event_title = str(raw_event.get("title") or "").strip()
        event_category = str(raw_event.get("category") or "").strip()
        event_markets = raw_event.get("markets") or []
        if not isinstance(event_markets, list):
            continue
        for raw_market in event_markets:
            if not isinstance(raw_market, dict):
                continue
            title = str(raw_market.get("title") or "").strip()
            subtitle = str(raw_market.get("subtitle") or "").strip()
            yes_sub_title = str(raw_market.get("yes_sub_title") or "").strip()
            no_sub_title = str(raw_market.get("no_sub_title") or "").strip()
            ticker = str(raw_market.get("ticker") or "").strip()

            market_type = infer_market_type(title, subtitle, yes_sub_title, no_sub_title, ticker)
            if market_type not in {"moneyline", "total", "spread"}:
                continue

            primary_title = title or event_title or subtitle
            if not is_probable_sports_event(event_category, primary_title, market_type):
                continue

            teams = (
                extract_event_teams(title, team_catalog=team_catalog)
                or extract_event_teams(event_title, team_catalog=team_catalog)
                or extract_event_teams(subtitle, team_catalog=team_catalog)
            )
            if teams is None:
                continue

            line = extract_line_from_kalshi_ticker(ticker, market_type)
            if line is None:
                line = extract_line_from_text(yes_sub_title or title or subtitle, market_type)
            if market_type in {"total", "spread"} and line is None:
                continue

            segment = detect_segment(ticker, title, subtitle)
            yes_side, yes_side_confident = infer_kalshi_yes_side(
                raw_market,
                market_type=market_type,
                teams=teams,
                line=line,
                team_catalog=team_catalog,
            )

            # For moneyline/spread we need a known YES side to match correctly.
            if market_type in {"moneyline", "spread"} and yes_side is None:
                continue

            probability = parse_kalshi_probability(raw_market)
            markets.append(
                ParsedMarket(
                    source="kalshi",
                    source_id=ticker,
                    event_title=event_title or title,
                    market_title=title,
                    display_title=title or event_title,
                    event_time=choose_kalshi_event_time(raw_market),
                    market_type=market_type,
                    segment=segment,
                    line=line,
                    teams=teams,
                    yes_side=yes_side,
                    yes_side_confident=yes_side_confident,
                    probability=probability,
                    raw=raw_market,
                )
            )
    eprint(verbose, f"Built {len(markets)} Kalshi markets")
    return markets


def build_polymarket_markets(
    raw_events: Sequence[Mapping[str, Any]],
    *,
    team_catalog: Optional[TeamCatalog],
    verbose: bool,
) -> List[ParsedMarket]:
    markets: List[ParsedMarket] = []
    for raw_event in raw_events:
        event_title = str(raw_event.get("title") or "").strip()
        event_category = str(raw_event.get("category") or "").strip()
        raw_markets = raw_event.get("markets") or []
        if not isinstance(raw_markets, list):
            continue

        for raw_market in raw_markets:
            if not isinstance(raw_market, dict):
                continue
            if raw_market.get("active") is False:
                continue
            if raw_market.get("closed") is True:
                continue
            if raw_market.get("archived") is True:
                continue

            question = str(raw_market.get("question") or "").strip()
            slug = str(raw_market.get("slug") or "").strip()
            sports_market_type = str(raw_market.get("sportsMarketType") or "").strip()

            market_type = infer_market_type(question, slug, explicit_type=sports_market_type)
            if market_type not in {"moneyline", "total", "spread"}:
                continue

            if not is_probable_sports_event(event_category or str(raw_market.get("category") or ""), event_title or question, market_type):
                # Keep markets with explicit sports structure, even if event category is missing.
                if not (raw_market.get("teamAID") or raw_market.get("teamBID") or sports_market_type):
                    continue

            teams = resolve_polymarket_event_teams(raw_event, raw_market, team_catalog)
            if teams is None:
                continue

            explicit_line = parse_float(raw_market.get("line"))
            line = explicit_line if explicit_line is not None else extract_line_from_text(question, market_type)
            if market_type in {"total", "spread"} and line is None:
                continue

            segment = detect_segment(question, slug, event_title)
            yes_side, yes_side_confident = infer_polymarket_yes_side(
                raw_event,
                raw_market,
                market_type=market_type,
                teams=teams,
                line=line,
                team_catalog=team_catalog,
            )
            if market_type in {"moneyline", "spread"} and yes_side is None:
                continue

            probability = parse_polymarket_probability(raw_market)
            markets.append(
                ParsedMarket(
                    source="polymarket",
                    source_id=slug or str(raw_market.get("id") or ""),
                    event_title=event_title or question,
                    market_title=question,
                    display_title=question or event_title,
                    event_time=choose_polymarket_event_time(raw_event, raw_market),
                    market_type=market_type,
                    segment=segment,
                    line=line,
                    teams=teams,
                    yes_side=yes_side,
                    yes_side_confident=yes_side_confident,
                    probability=probability,
                    raw=raw_market,
                )
            )
    eprint(verbose, f"Built {len(markets)} Polymarket markets")
    return markets


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def same_day_or_neighbor_keys(event_time: Optional[datetime]) -> List[str]:
    if event_time is None:
        return ["*"]
    date = event_time.date()
    return [
        date.isoformat(),
        (date - timedelta(days=1)).isoformat(),
        (date + timedelta(days=1)).isoformat(),
        "*",
    ]


def build_index(markets: Sequence[ParsedMarket]) -> Dict[Tuple[str, str, Optional[int], str], List[int]]:
    index: Dict[Tuple[str, str, Optional[int], str], List[int]] = {}
    for idx, market in enumerate(markets):
        bucket = line_bucket(market.line)
        for date_key in same_day_or_neighbor_keys(market.event_time)[:1]:  # primary date bucket
            key = (market.market_type, market.segment, bucket, date_key)
            index.setdefault(key, []).append(idx)
        # Universal fallback bucket
        key_any = (market.market_type, market.segment, bucket, "*")
        index.setdefault(key_any, []).append(idx)
    return index


def get_candidate_indices(
    market: ParsedMarket,
    *,
    index: Mapping[Tuple[str, str, Optional[int], str], List[int]],
) -> List[int]:
    bucket = line_bucket(market.line)
    candidates: List[int] = []
    seen: Set[int] = set()
    for date_key in same_day_or_neighbor_keys(market.event_time):
        key = (market.market_type, market.segment, bucket, date_key)
        for idx in index.get(key, []):
            if idx not in seen:
                candidates.append(idx)
                seen.add(idx)
    return candidates


def time_score(a: Optional[datetime], b: Optional[datetime], *, max_gap_hours: float) -> float:
    if a is None or b is None:
        return 0.55
    hours = abs((a - b).total_seconds()) / 3600.0
    if hours > max_gap_hours:
        return 0.0
    if hours <= 3:
        return 1.0
    if hours <= 6:
        return 0.95
    if hours <= 12:
        return 0.88
    return 0.78


def line_score(a: Optional[float], b: Optional[float], market_type: str) -> float:
    if market_type == "moneyline":
        return 1.0
    if a is None or b is None:
        return 0.0
    diff = abs(a - b)
    if diff <= 0.0001:
        return 1.0
    if diff <= 0.01:
        return 0.95
    return 0.0


def side_similarity(a: Optional[str], b: Optional[str], market_type: str) -> float:
    if a is None or b is None:
        return 0.0
    if a == b:
        return 1.0

    if market_type == "moneyline":
        if a == "draw" or b == "draw":
            return 0.0
        return team_similarity(a, b)

    if market_type == "total":
        return 1.0 if a == b else 0.0

    if market_type == "spread":
        if "|" not in a or "|" not in b:
            return 0.0
        team_a, line_a = a.split("|", 1)
        team_b, line_b = b.split("|", 1)
        if team_similarity(team_a, team_b) < 0.95:
            return 0.0
        try:
            fa = float(line_a)
            fb = float(line_b)
        except ValueError:
            return 0.0
        return 1.0 if abs(fa - fb) <= 0.01 else 0.0

    return 0.0


def compute_match_confidence(
    kalshi_market: ParsedMarket,
    poly_market: ParsedMarket,
    *,
    max_event_gap_hours: float,
    include_uncertain_sides: bool,
) -> Tuple[float, float, float, float, float, str, bool]:
    if kalshi_market.market_type != poly_market.market_type:
        return 0.0, 0.0, 0.0, 0.0, 0.0, "none", False

    if kalshi_market.segment != poly_market.segment:
        return 0.0, 0.0, 0.0, 0.0, 0.0, "none", False

    team_floor = min_event_team_score(kalshi_market.teams, poly_market.teams)
    if team_floor < 0.82:
        return 0.0, 0.0, 0.0, 0.0, 0.0, "none", False

    avg_team_score, oriented_poly_teams = event_team_match_scores(kalshi_market.teams, poly_market.teams)
    if avg_team_score < 0.88:
        return 0.0, 0.0, 0.0, 0.0, 0.0, "none", False

    t_score = time_score(kalshi_market.event_time, poly_market.event_time, max_gap_hours=max_event_gap_hours)
    if t_score <= 0.0:
        return 0.0, 0.0, 0.0, 0.0, 0.0, "none", False

    l_score = line_score(kalshi_market.line, poly_market.line, kalshi_market.market_type)
    if kalshi_market.market_type in {"total", "spread"} and l_score <= 0.0:
        return 0.0, 0.0, 0.0, 0.0, 0.0, "none", False

    s_score = side_similarity(kalshi_market.yes_side, poly_market.yes_side, kalshi_market.market_type)
    needs_manual_review = False

    if s_score < 0.88:
        kalshi_side_known = kalshi_market.yes_side is not None and kalshi_market.yes_side_confident
        poly_side_known = poly_market.yes_side is not None and poly_market.yes_side_confident
        if include_uncertain_sides and not (kalshi_side_known and poly_side_known):
            # Keep same-event same-line matches when one side cannot be read confidently,
            # but mark them for manual review and lower the confidence materially.
            s_score = 0.45
            needs_manual_review = True
            side_alignment = "uncertain"
        else:
            return 0.0, 0.0, 0.0, 0.0, 0.0, "none", False
    else:
        side_alignment = "same"

    confidence = (
        0.52 * avg_team_score +
        0.18 * t_score +
        0.16 * l_score +
        0.14 * s_score
    )
    confidence = max(0.0, min(1.0, confidence))
    return confidence, avg_team_score, t_score, l_score, s_score, side_alignment, needs_manual_review


def probability_gap_pct_points(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return abs(a - b) * 100.0


def match_markets(
    kalshi_markets: Sequence[ParsedMarket],
    poly_markets: Sequence[ParsedMarket],
    *,
    min_confidence: float,
    max_event_gap_hours: float,
    include_uncertain_sides: bool,
    one_to_one: bool,
    verbose: bool,
) -> List[MatchResult]:
    poly_index = build_index(poly_markets)

    tentative: List[Tuple[float, Optional[float], int, int, float, float, float, float, str, bool]] = []
    for k_idx, k_market in enumerate(kalshi_markets):
        candidate_indices = get_candidate_indices(k_market, index=poly_index)
        if not candidate_indices:
            continue

        for p_idx in candidate_indices:
            p_market = poly_markets[p_idx]
            confidence, team_s, time_s, line_s, side_s, alignment, needs_manual_review = compute_match_confidence(
                k_market,
                p_market,
                max_event_gap_hours=max_event_gap_hours,
                include_uncertain_sides=include_uncertain_sides,
            )
            if confidence < min_confidence:
                continue
            gap = probability_gap_pct_points(k_market.probability.yes_prob, p_market.probability.yes_prob)
            tentative.append((confidence, gap, k_idx, p_idx, team_s, time_s, line_s, side_s, alignment, needs_manual_review))

    tentative.sort(
        key=lambda item: (
            item[0],                                  # confidence
            item[1] if item[1] is not None else -1.0, # prob gap
        ),
        reverse=True,
    )

    used_kalshi: Set[int] = set()
    used_poly: Set[int] = set()
    results: List[MatchResult] = []
    for confidence, gap, k_idx, p_idx, team_s, time_s, line_s, side_s, alignment, needs_manual_review in tentative:
        if one_to_one and (k_idx in used_kalshi or p_idx in used_poly):
            continue
        k_market = kalshi_markets[k_idx]
        p_market = poly_markets[p_idx]
        results.append(
            MatchResult(
                match_confidence=confidence,
                team_score=team_s,
                time_score=time_s,
                line_score=line_s,
                side_score=side_s,
                probability_gap_pct_points=gap,
                side_alignment=alignment,
                needs_manual_review=needs_manual_review,
                event_time_utc=k_market.event_time.isoformat() if k_market.event_time is not None else None,
                event_title=k_market.event_title,
                market_type=k_market.market_type,
                segment=k_market.segment,
                line=k_market.line,
                proposition=proposition_label(k_market),
                kalshi_probability_pct=probability_to_pct(k_market.probability.yes_prob),
                kalshi_probability_source=k_market.probability.source,
                kalshi_ticker=k_market.source_id,
                kalshi_title=k_market.market_title,
                kalshi_yes_side=k_market.yes_side,
                polymarket_probability_pct=probability_to_pct(p_market.probability.yes_prob),
                polymarket_probability_source=p_market.probability.source,
                polymarket_slug=p_market.source_id,
                polymarket_question=p_market.market_title,
                polymarket_yes_side=p_market.yes_side,
            )
        )
        if one_to_one:
            used_kalshi.add(k_idx)
            used_poly.add(p_idx)

    if one_to_one:
        eprint(verbose, f"Matched {len(results)} cross-book markets (one-to-one)")
    else:
        eprint(verbose, f"Matched {len(results)} cross-book candidate pairs")
    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def sort_results(results: List[MatchResult], sort_key: str) -> List[MatchResult]:
    if sort_key == "confidence":
        return sorted(
            results,
            key=lambda item: (
                item.match_confidence,
                item.probability_gap_pct_points if item.probability_gap_pct_points is not None else -1.0,
            ),
            reverse=True,
        )
    if sort_key == "time":
        return sorted(
            results,
            key=lambda item: (
                item.event_time_utc or "",
                item.match_confidence,
            ),
        )
    # Default: gap
    return sorted(
        results,
        key=lambda item: (
            item.probability_gap_pct_points if item.probability_gap_pct_points is not None else -1.0,
            item.match_confidence,
        ),
        reverse=True,
    )


def print_table(results: Sequence[MatchResult], *, top: int) -> None:
    subset = list(results[:top])
    if not subset:
        print("No confident cross-book matches found.")
        return

    headers = [
        "conf",
        "gap(pp)",
        "type",
        "seg",
        "line",
        "prop",
        "kalshi%",
        "poly%",
        "event",
    ]
    widths = [5, 8, 9, 6, 7, 24, 8, 8, 44]

    print(" ".join(header.ljust(width) for header, width in zip(headers, widths)))
    print("-" * (sum(widths) + len(widths) - 1))

    for item in subset:
        row = [
            f"{item.match_confidence:.2f}",
            (f"{item.probability_gap_pct_points:.2f}" if item.probability_gap_pct_points is not None else "-"),
            item.market_type,
            item.segment,
            format_line(item.line) or "-",
            item.proposition[:24],
            (f"{item.kalshi_probability_pct:.1f}" if item.kalshi_probability_pct is not None else "-"),
            (f"{item.polymarket_probability_pct:.1f}" if item.polymarket_probability_pct is not None else "-"),
            item.event_title[:44],
        ]
        print(" ".join(str(value).ljust(width) for value, width in zip(row, widths)))


def write_csv(path: str, results: Sequence[MatchResult]) -> None:
    field_names = list(asdict(results[0]).keys()) if results else list(MatchResult.__dataclass_fields__.keys())
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=field_names)
        writer.writeheader()
        for item in results:
            writer.writerow(asdict(item))


# ---------------------------------------------------------------------------
# CLI / self-test
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> Settings:
    parser = argparse.ArgumentParser(description="Find matching Kalshi vs Polymarket sports markets and compare probabilities.")
    parser.add_argument("--top", type=int, default=50, help="Rows to print to stdout.")
    parser.add_argument("--output-csv", type=str, default=None, help="Optional CSV path.")
    parser.add_argument("--verbose", action="store_true", help="Print progress to stderr.")
    parser.add_argument("--min-confidence", type=float, default=0.85, help="Only keep matches with confidence >= this value.")
    parser.add_argument("--max-event-gap-hours", type=float, default=18.0, help="Reject events whose scheduled times differ by more than this many hours.")
    parser.add_argument("--kalshi-event-limit", type=int, default=None, help="Optional max number of Kalshi events to fetch.")
    parser.add_argument("--polymarket-event-limit", type=int, default=None, help="Optional max number of Polymarket events to fetch.")
    parser.add_argument("--include-uncertain-sides", action="store_true", help="Include same-event same-line matches even if one venue's YES side is not readable confidently.")
    parser.add_argument("--one-to-one", action="store_true", help="Keep only the best unique Kalshi↔Polymarket pairing for each market instead of returning all confident candidates.")
    parser.add_argument("--skip-poly-teams", action="store_true", help="Skip Polymarket /teams lookup (faster, but weaker alias matching).")
    parser.add_argument("--sort", choices=("gap", "confidence", "time"), default="gap", help="Sort output by probability gap, confidence, or event time.")
    parser.add_argument("--self-test", action="store_true", help="Run internal parser/matcher tests and exit.")
    args = parser.parse_args(argv)
    return Settings(
        top=max(1, args.top),
        output_csv=args.output_csv,
        verbose=bool(args.verbose),
        min_confidence=max(0.0, min(1.0, float(args.min_confidence))),
        max_event_gap_hours=max(0.0, float(args.max_event_gap_hours)),
        kalshi_event_limit=args.kalshi_event_limit,
        polymarket_event_limit=args.polymarket_event_limit,
        include_uncertain_sides=bool(args.include_uncertain_sides),
        one_to_one=bool(args.one_to_one),
        skip_poly_teams=bool(args.skip_poly_teams),
        sort=str(args.sort),
        self_test=bool(args.self_test),
    )


def run_self_test() -> None:
    # Basic parsing sanity checks that target the failure modes in the original screener.
    catalog = TeamCatalog(
        [
            TeamEntry("1", "Los Angeles Lakers", "NBA", ("LA Lakers", "Los Angeles L", "LAL")),
            TeamEntry("2", "Houston Rockets", "NBA", ("Houston", "HOU")),
            TeamEntry("3", "Austin FC", "MLS", ("Austin", "ATX")),
            TeamEntry("4", "Los Angeles FC", "MLS", ("LAFC", "Los Angeles FC")),
            TeamEntry("5", "Boston Celtics", "NBA", ("Boston", "BOS")),
            TeamEntry("6", "Golden State Warriors", "NBA", ("Golden State", "GSW")),
            TeamEntry("7", "Wichita State Shockers", "NCAAB", ("Wichita State", "WSS")),
            TeamEntry("8", "Nebraska Cornhuskers", "NCAAB", ("Nebraska", "NEC")),
            TeamEntry("9", "Club Puebla", "Liga MX", ("Puebla",)),
            TeamEntry("10", "Club Necaxa", "Liga MX", ("Necaxa",)),
        ]
    )

    # Event team extraction
    assert extract_event_teams("Los Angeles L vs Houston: Total Points", team_catalog=catalog) == (
        "Los Angeles Lakers",
        "Houston Rockets",
    )
    assert extract_event_teams("Austin FC vs. Los Angeles FC: O/U 1.5", team_catalog=catalog) == (
        "Austin FC",
        "Los Angeles FC",
    )

    # Segment detection
    assert detect_segment("Golden State vs Boston: First Half Winner?") == "1H"
    assert detect_segment("Will OG win map 2 in the NIP vs. OG match?") == "MAP2"
    assert detect_segment("Nebraska vs Wichita State winner?") == "FULL"

    # Market type + line
    assert infer_market_type("Austin FC vs. Los Angeles FC: O/U 1.5") == "total"
    assert extract_line_from_text("Austin FC vs. Los Angeles FC: O/U 1.5", "total") == 1.5
    assert infer_market_type("Boston Celtics spread -5.5") == "spread"
    assert extract_line_from_text("Boston Celtics spread -5.5", "spread") == -5.5

    # Side inference
    kalshi_draw_raw = {"yes_sub_title": "Tie", "title": "Saint Martin's vs Washington winner?", "subtitle": ""}
    side, confident = infer_kalshi_yes_side(kalshi_draw_raw, market_type="moneyline", teams=("Saint Martin's", "Washington"), line=None, team_catalog=None)
    assert side == "draw" and confident

    poly_draw_raw = {"question": "Will the match end in a draw?", "groupItemTitle": "Draw", "outcomes": '["Yes","No"]'}
    side, confident = infer_polymarket_yes_side({"title": "Saint Martin's vs Washington"}, poly_draw_raw, market_type="moneyline", teams=("Saint Martin's", "Washington"), line=None, team_catalog=None)
    assert side == "draw" and confident

    # Match reject: same line but different event/league
    k_total = ParsedMarket(
        source="kalshi",
        source_id="KXNBATOTAL-26MAR18LALHOU-234",
        event_title="Los Angeles L vs Houston",
        market_title="Los Angeles L at Houston: Total Points",
        display_title="Los Angeles L at Houston: Total Points",
        event_time=parse_datetime("2026-03-18T23:00:00Z"),
        market_type="total",
        segment="FULL",
        line=234.0,
        teams=("Los Angeles Lakers", "Houston Rockets"),
        yes_side="over",
        yes_side_confident=True,
        probability=ProbabilityQuote(0.57, "last_price"),
        raw={},
    )
    p_total_bad = ParsedMarket(
        source="polymarket",
        source_id="mls-aus-laf-2026-03-21-total-1pt5",
        event_title="Austin FC vs Los Angeles FC",
        market_title="Austin FC vs. Los Angeles FC: O/U 1.5",
        display_title="Austin FC vs. Los Angeles FC: O/U 1.5",
        event_time=parse_datetime("2026-03-21T23:00:00Z"),
        market_type="total",
        segment="FULL",
        line=1.5,
        teams=("Austin FC", "Los Angeles FC"),
        yes_side="over",
        yes_side_confident=False,
        probability=ProbabilityQuote(0.79, "outcomePrices"),
        raw={},
    )
    confidence, *_ = compute_match_confidence(k_total, p_total_bad, max_event_gap_hours=18.0, include_uncertain_sides=False)
    assert confidence == 0.0

    # Match accept: same event and side
    k_money = ParsedMarket(
        source="kalshi",
        source_id="KXNCAABBGAME-26MAR171905NECWSS-WSS",
        event_title="Nebraska vs Wichita State",
        market_title="Nebraska vs Wichita State winner?",
        display_title="Nebraska vs Wichita State winner?",
        event_time=parse_datetime("2026-03-17T23:05:00Z"),
        market_type="moneyline",
        segment="FULL",
        line=None,
        teams=("Nebraska Cornhuskers", "Wichita State Shockers"),
        yes_side="Wichita State Shockers",
        yes_side_confident=True,
        probability=ProbabilityQuote(0.52, "last_price"),
        raw={},
    )
    p_money = ParsedMarket(
        source="polymarket",
        source_id="ncaab-neb-wichst-2026-03-17-wichst",
        event_title="Nebraska Cornhuskers vs Wichita State Shockers",
        market_title="Will Wichita State win?",
        display_title="Will Wichita State win?",
        event_time=parse_datetime("2026-03-17T22:55:00Z"),
        market_type="moneyline",
        segment="FULL",
        line=None,
        teams=("Nebraska Cornhuskers", "Wichita State Shockers"),
        yes_side="Wichita State Shockers",
        yes_side_confident=True,
        probability=ProbabilityQuote(0.71, "outcomePrices"),
        raw={},
    )
    confidence, *_ = compute_match_confidence(k_money, p_money, max_event_gap_hours=18.0, include_uncertain_sides=False)
    assert confidence > 0.9

    # Reject first-half vs full-game
    k_1h = ParsedMarket(
        source="kalshi",
        source_id="KXNBA1HWINNER-26MAR18GSWBOS-BOS",
        event_title="Golden State vs Boston",
        market_title="Golden State vs Boston: First Half Winner?",
        display_title="Golden State vs Boston: First Half Winner?",
        event_time=parse_datetime("2026-03-18T23:00:00Z"),
        market_type="moneyline",
        segment="1H",
        line=None,
        teams=("Golden State Warriors", "Boston Celtics"),
        yes_side="Boston Celtics",
        yes_side_confident=True,
        probability=ProbabilityQuote(0.49, "last_price"),
        raw={},
    )
    p_full = ParsedMarket(
        source="polymarket",
        source_id="nba-gsw-bos-2026-03-18-bos",
        event_title="Golden State Warriors vs Boston Celtics",
        market_title="Will Boston Celtics win?",
        display_title="Will Boston Celtics win?",
        event_time=parse_datetime("2026-03-18T23:00:00Z"),
        market_type="moneyline",
        segment="FULL",
        line=None,
        teams=("Golden State Warriors", "Boston Celtics"),
        yes_side="Boston Celtics",
        yes_side_confident=True,
        probability=ProbabilityQuote(0.55, "outcomePrices"),
        raw={},
    )
    confidence, *_ = compute_match_confidence(k_1h, p_full, max_event_gap_hours=18.0, include_uncertain_sides=False)
    assert confidence == 0.0

    print("Self-test passed.")


def main(argv: Optional[Sequence[str]] = None) -> int:
    settings = parse_args(argv)
    if settings.self_test:
        run_self_test()
        return 0

    session = build_http_session()
    team_catalog: Optional[TeamCatalog] = None
    if not settings.skip_poly_teams:
        try:
            team_catalog = fetch_polymarket_team_catalog(session, verbose=settings.verbose)
            eprint(settings.verbose, f"Loaded {len(team_catalog.teams)} Polymarket team aliases")
        except Exception as exc:
            eprint(settings.verbose, f"Warning: could not load Polymarket teams catalog: {exc}. Continuing without it.")
            team_catalog = None

    kalshi_events = fetch_all_kalshi_open_events(
        session,
        limit=settings.kalshi_event_limit,
        verbose=settings.verbose,
    )
    polymarket_events = fetch_all_polymarket_active_events(
        session,
        limit=settings.polymarket_event_limit,
        verbose=settings.verbose,
    )

    kalshi_markets = build_kalshi_markets(
        kalshi_events,
        team_catalog=team_catalog,
        verbose=settings.verbose,
    )
    poly_markets = build_polymarket_markets(
        polymarket_events,
        team_catalog=team_catalog,
        verbose=settings.verbose,
    )

    results = match_markets(
        kalshi_markets,
        poly_markets,
        min_confidence=settings.min_confidence,
        max_event_gap_hours=settings.max_event_gap_hours,
        include_uncertain_sides=settings.include_uncertain_sides,
        one_to_one=settings.one_to_one,
        verbose=settings.verbose,
    )
    results = sort_results(results, settings.sort)

    print_table(results, top=settings.top)
    if settings.output_csv:
        write_csv(settings.output_csv, results)
        print(f"\nWrote {len(results)} rows to {settings.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
