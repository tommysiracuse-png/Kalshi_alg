#!/usr/bin/env python3
"""
Standalone startup-time market risk profiler for Kalshi markets.

This version focuses much more heavily on validating event timing.

What it does
------------
- Fetches market metadata once
- Samples a market for a short startup window
- Infers scheduled start time from the ticker when possible
- Classifies the market family
- Assigns an expected duration by family
- Cross-checks official close against inferred event timing
- Computes a frozen risk profile:
    * official close time
    * inferred scheduled start time
    * expected event duration
    * event-end estimate
    * effective close time
    * soft stop / hard stop / flatten-only times
    * observed micro-volatility
    * observed short-range instability
    * bid-flip rate
    * style scores
    * suggested action

Usage examples
--------------
python3 market_risk_profile.py --ticker KXMLBGAME-26APR011610NYYSEA-NYY --pretty
python3 market_risk_profile.py --ticker KXWTI-26APR01-T98.99 --sample-seconds 6 --poll-interval-seconds 0.25 --pretty
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import logging
import os
import re
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


PRICE_SCALE = 10_000
PRICE_UNITS_PER_CENT = 100



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
LOGGER = logging.getLogger("market_risk_profile")

PROFILE_VERSION = "heuristic-v2"
DEFAULT_CONFIDENCE_REDUCTION_THRESHOLD = 0.70
DEFAULT_CONFIDENCE_FLATTEN_THRESHOLD = 0.55


def log_event(event_name: str, **fields: object) -> None:
    if fields:
        detail = " ".join(f"{key}={value}" for key, value in fields.items())
        LOGGER.info("%s | %s", event_name, detail)
    else:
        LOGGER.info("%s", event_name)

MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_iso_utc_timestamp_to_ms(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except Exception:
        return None


def ms_to_iso_utc(value_ms: Optional[int]) -> Optional[str]:
    if value_ms is None:
        return None
    return datetime.fromtimestamp(value_ms / 1000.0, tz=timezone.utc).isoformat()


def parse_price_units_from_market_fields(payload: Dict[str, Any], dollars_field: str, cents_field: str) -> Optional[int]:
    value = payload.get(dollars_field)
    if value not in (None, ""):
        return int(round(float(value) * PRICE_SCALE))
    value = payload.get(cents_field)
    if value not in (None, ""):
        return int(value) * PRICE_UNITS_PER_CENT
    return None


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class KalshiApiClient:
    def __init__(self, *, api_key_id: str, private_key_path: str, use_demo: bool) -> None:
        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.use_demo = bool(use_demo)
        self.host = "https://demo-api.kalshi.co" if self.use_demo else "https://api.elections.kalshi.com"
        self.api_prefix = "/trade-api/v2"
        self.session = requests.Session()

        with open(self.private_key_path, "rb") as private_key_file:
            self.private_key = serialization.load_pem_private_key(private_key_file.read(), password=None)

    def sign_message(self, message_bytes: bytes) -> str:
        signature = self.private_key.sign(
            message_bytes,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str) -> Dict[str, str]:
        timestamp_ms = str(int(time.time() * 1000))
        path_without_query = path.split("?")[0]
        signed_message = f"{timestamp_ms}{method.upper()}{path_without_query}".encode("utf-8")
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": self.sign_message(signed_message),
        }

    def rest_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        response = self.session.get(
            self.host + path,
            headers=self._headers("GET", path),
            params=params,
            timeout=20,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"GET {path} failed {response.status_code}: {response.text[:500]}")
        if not response.text.strip():
            return {}
        return response.json()

    def get_market(self, ticker: str) -> Dict[str, Any]:
        response = self.rest_get(f"{self.api_prefix}/markets/{ticker}")
        market = response.get("market")
        if not market:
            raise RuntimeError(f"Market payload missing for ticker {ticker}")
        return market


@dataclass
class MarketMetadata:
    ticker: str
    title: str
    subtitle: str
    status: str
    series_ticker: str
    event_ticker: str
    close_time_ms: Optional[int]
    settle_time_ms: Optional[int]
    open_time_ms: Optional[int]
    market_type_hint: str


@dataclass
class MarketSample:
    ts_ms: int
    yes_bid_units: Optional[int]
    no_bid_units: Optional[int]
    yes_ask_units: Optional[int]
    no_ask_units: Optional[int]
    last_price_units: Optional[int]
    volume: Optional[float]
    open_interest: Optional[float]


@dataclass
class StyleScores:
    front_loaded: float
    sports_like: float
    continuous_price_like: float
    weather_like: float
    binary_event_like: float


@dataclass
class MarketRiskProfile:
    ticker: str
    title: str
    generated_at_ms: int
    official_close_time_ms: Optional[int]
    official_close_time_iso: Optional[str]
    inferred_start_time_ms: Optional[int]
    inferred_start_time_iso: Optional[str]
    inferred_start_source: str
    market_family: str
    expected_duration_minutes: Optional[int]
    event_end_estimate_ms: Optional[int]
    event_end_estimate_iso: Optional[str]
    event_live_now_inferred: bool
    effective_close_time_ms: Optional[int]
    effective_close_time_iso: Optional[str]
    soft_stop_time_ms: Optional[int]
    soft_stop_time_iso: Optional[str]
    hard_stop_time_ms: Optional[int]
    hard_stop_time_iso: Optional[str]
    flatten_only_time_ms: Optional[int]
    flatten_only_time_iso: Optional[str]
    observed_volatility_bp_60s_equiv: float
    observed_range_bp: float
    bid_flip_rate_per_minute: float
    style_scores: StyleScores
    suggested_action: str
    confidence: float
    mode: str
    reason: str
    profile_version: str
    notes: List[str]


class MarketRiskProfiler:
    def __init__(
        self,
        *,
        api_client: KalshiApiClient,
        sample_seconds: float,
        poll_interval_seconds: float,
    ) -> None:
        self.api_client = api_client
        self.sample_seconds = float(sample_seconds)
        self.poll_interval_seconds = float(poll_interval_seconds)

    def load_metadata(self, ticker: str) -> MarketMetadata:
        market = self.api_client.get_market(ticker)

        title = str(market.get("title") or market.get("name") or "")
        subtitle = str(market.get("subtitle") or "")
        close_time_ms = (
            parse_iso_utc_timestamp_to_ms(market.get("close_time"))
            or parse_iso_utc_timestamp_to_ms(market.get("expiration_time"))
            or parse_iso_utc_timestamp_to_ms(market.get("expiration_datetime"))
            or parse_iso_utc_timestamp_to_ms(market.get("close_datetime"))
        )
        settle_time_ms = (
            parse_iso_utc_timestamp_to_ms(market.get("settlement_time"))
            or parse_iso_utc_timestamp_to_ms(market.get("settlement_datetime"))
        )
        open_time_ms = (
            parse_iso_utc_timestamp_to_ms(market.get("open_time"))
            or parse_iso_utc_timestamp_to_ms(market.get("open_datetime"))
        )

        market_type_hint = str(
            market.get("market_type")
            or market.get("result_type")
            or market.get("status")
            or ""
        )

        return MarketMetadata(
            ticker=str(market.get("ticker") or ticker),
            title=title,
            subtitle=subtitle,
            status=str(market.get("status") or ""),
            series_ticker=str(market.get("series_ticker") or ""),
            event_ticker=str(market.get("event_ticker") or ""),
            close_time_ms=close_time_ms,
            settle_time_ms=settle_time_ms,
            open_time_ms=open_time_ms,
            market_type_hint=market_type_hint,
        )

    def fetch_sample(self, ticker: str) -> MarketSample:
        market = self.api_client.get_market(ticker)

        yes_bid_units = parse_price_units_from_market_fields(market, "yes_bid_dollars", "yes_bid")
        no_bid_units = parse_price_units_from_market_fields(market, "no_bid_dollars", "no_bid")
        yes_ask_units = parse_price_units_from_market_fields(market, "yes_ask_dollars", "yes_ask")
        no_ask_units = parse_price_units_from_market_fields(market, "no_ask_dollars", "no_ask")
        last_price_units = parse_price_units_from_market_fields(market, "last_price_dollars", "last_price")

        volume = None
        if market.get("volume") not in (None, ""):
            try:
                volume = float(market["volume"])
            except Exception:
                volume = None

        open_interest = None
        if market.get("open_interest") not in (None, ""):
            try:
                open_interest = float(market["open_interest"])
            except Exception:
                open_interest = None

        return MarketSample(
            ts_ms=now_ms(),
            yes_bid_units=yes_bid_units,
            no_bid_units=no_bid_units,
            yes_ask_units=yes_ask_units,
            no_ask_units=no_ask_units,
            last_price_units=last_price_units,
            volume=volume,
            open_interest=open_interest,
        )

    def collect_startup_samples(self, ticker: str) -> List[MarketSample]:
        samples: List[MarketSample] = []
        end_time = time.time() + max(0.5, self.sample_seconds)

        while True:
            samples.append(self.fetch_sample(ticker))
            if time.time() >= end_time:
                break
            time.sleep(max(0.05, self.poll_interval_seconds))

        return samples

    def compute_price_series(self, samples: List[MarketSample]) -> List[float]:
        prices: List[float] = []
        for sample in samples:
            if sample.last_price_units is not None:
                prices.append(sample.last_price_units / PRICE_SCALE)
                continue
            if sample.yes_bid_units is not None and sample.no_bid_units is not None:
                implied_yes_ask = PRICE_SCALE - sample.no_bid_units
                mid_units = int(round((sample.yes_bid_units + implied_yes_ask) / 2.0))
                prices.append(mid_units / PRICE_SCALE)
        return prices

    def compute_observed_volatility_bp_60s_equiv(self, samples: List[MarketSample]) -> float:
        prices = self.compute_price_series(samples)
        if len(prices) < 3:
            return 0.0

        returns_bp: List[float] = []
        for previous, current in zip(prices[:-1], prices[1:]):
            if previous <= 0:
                continue
            returns_bp.append(((current - previous) / previous) * 10_000.0)

        if len(returns_bp) < 2:
            return 0.0

        stdev_bp = statistics.pstdev(returns_bp)
        observed_duration_s = max(1e-6, (samples[-1].ts_ms - samples[0].ts_ms) / 1000.0)
        average_step_s = observed_duration_s / max(1, len(returns_bp))
        scale = math.sqrt(60.0 / max(1e-6, average_step_s))
        return float(stdev_bp * scale)

    def compute_observed_range_bp(self, samples: List[MarketSample]) -> float:
        prices = self.compute_price_series(samples)
        if len(prices) < 2:
            return 0.0
        low = min(prices)
        high = max(prices)
        mid = (low + high) / 2.0
        if mid <= 0:
            return 0.0
        return ((high - low) / mid) * 10_000.0

    def compute_bid_flip_rate_per_minute(self, samples: List[MarketSample]) -> float:
        if len(samples) < 2:
            return 0.0

        flips = 0
        previous_pair: Optional[Tuple[Optional[int], Optional[int]]] = None
        for sample in samples:
            current_pair = (sample.yes_bid_units, sample.no_bid_units)
            if previous_pair is not None and current_pair != previous_pair:
                flips += 1
            previous_pair = current_pair

        duration_min = max(1e-6, (samples[-1].ts_ms - samples[0].ts_ms) / 60000.0)
        return flips / duration_min

    def infer_style_scores(self, metadata: MarketMetadata, samples: List[MarketSample]) -> StyleScores:
        text = f"{metadata.title} {metadata.subtitle} {metadata.ticker} {metadata.series_ticker} {metadata.event_ticker}".lower()

        front_loaded = 0.15
        sports_like = 0.10
        continuous_price_like = 0.10
        weather_like = 0.05
        binary_event_like = 0.25

        speech_terms = [
            "mention", "say", "speech", "press conference", "conference", "debate",
            "state of the union", "hannity", "participant", "interview", "remarks",
        ]
        sports_terms = [
            "winner", "spread", "total", "points", "assists", "rebounds", "map",
            "innings", "strikeouts", "home runs", "match", "game",
        ]
        price_terms = [
            "price", "settle", "open price", "close price", "above", "below",
            "trimmed mean", "net worth", "at 5pm", "at 4pm", "at 2pm",
        ]
        weather_terms = [
            "temperature", "rain", "high temp", "maximum temperature", "minimum temperature",
        ]

        if any(term in text for term in speech_terms):
            front_loaded += 0.55
            binary_event_like += 0.20

        if any(term in text for term in sports_terms):
            sports_like += 0.55
            binary_event_like += 0.20

        if any(term in text for term in price_terms):
            continuous_price_like += 0.60

        if any(term in text for term in weather_terms):
            weather_like += 0.65

        flip_rate = self.compute_bid_flip_rate_per_minute(samples)
        observed_range_bp = self.compute_observed_range_bp(samples)

        if flip_rate >= 10:
            continuous_price_like += 0.10
        if observed_range_bp >= 150:
            front_loaded += 0.10
            continuous_price_like += 0.10

        total = front_loaded + sports_like + continuous_price_like + weather_like + binary_event_like
        if total <= 0:
            total = 1.0

        return StyleScores(
            front_loaded=front_loaded / total,
            sports_like=sports_like / total,
            continuous_price_like=continuous_price_like / total,
            weather_like=weather_like / total,
            binary_event_like=binary_event_like / total,
        )

    def infer_market_family(self, metadata: MarketMetadata) -> str:
        text = f"{metadata.title} {metadata.subtitle} {metadata.ticker} {metadata.series_ticker}".lower()

        if any(term in text for term in ["mlbgame", "baseball", "innings", "home runs", "strikeouts"]):
            return "mlb"
        if any(term in text for term in ["nbagame", "nba", "points", "assists", "rebounds"]):
            return "nba"
        if any(term in text for term in ["nhl", "goals", "puck line"]):
            return "nhl"
        if any(term in text for term in ["soccer", "fifa", "friendly", "laliga", "serie a", "premier league", "winner?"]):
            if "game" in text or "match" in text or "winner" in text:
                return "soccer"
        if any(term in text for term in ["cs2", "valorant", "league of legends", "lol", "esport", "map "]):
            return "esports"
        if any(term in text for term in ["mention", "say", "speech", "press conference", "debate", "interview", "state of the union"]):
            return "speech_event"
        if any(term in text for term in ["temperature", "rain", "high temp", "maximum temperature", "minimum temperature"]):
            return "weather"
        if any(term in text for term in ["price", "settle", "open price", "close price", "net worth", "trimmed mean", "above", "below"]):
            return "continuous_price"
        return "generic_binary"

    def expected_duration_minutes_for_family(self, family: str) -> Optional[int]:
        mapping = {
            "mlb": 240,
            "nba": 180,
            "nhl": 180,
            "soccer": 150,
            "esports": 120,
            "speech_event": 20,
            "weather": None,
            "continuous_price": None,
            "generic_binary": None,
        }
        return mapping.get(family)

    def infer_start_time_from_ticker(self, ticker: str) -> Tuple[Optional[int], str]:
        # Best case: YYMONDDHHMM embedded anywhere after a dash, e.g. 26APR011610
        match = re.search(r"(\d{2})([A-Z]{3})(\d{2})(\d{4})", ticker)
        if match:
            year_two = int(match.group(1))
            month_str = match.group(2)
            day = int(match.group(3))
            hhmm = match.group(4)
            month = MONTH_MAP.get(month_str)
            if month is not None:
                hour = int(hhmm[:2])
                minute = int(hhmm[2:])
                year = 2000 + year_two
                try:
                    dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000), "ticker_datetime_token"
                except Exception:
                    pass

        # Next best: YYMONDD date only; use midnight UTC as a weak anchor.
        match = re.search(r"(\d{2})([A-Z]{3})(\d{2})", ticker)
        if match:
            year_two = int(match.group(1))
            month_str = match.group(2)
            day = int(match.group(3))
            month = MONTH_MAP.get(month_str)
            if month is not None:
                year = 2000 + year_two
                try:
                    dt = datetime(year, month, day, 0, 0, tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000), "ticker_date_only"
                except Exception:
                    pass

        return None, "none"

    def infer_start_time(
        self,
        *,
        metadata: MarketMetadata,
        family: str,
        official_close_ms: Optional[int],
        notes: List[str],
    ) -> Tuple[Optional[int], str]:
        ticker_start_ms, source = self.infer_start_time_from_ticker(metadata.ticker)
        duration_minutes = self.expected_duration_minutes_for_family(family)

        if ticker_start_ms is not None and duration_minutes is not None:
            return ticker_start_ms, source

        if metadata.open_time_ms is not None:
            notes.append("Using market open time as inferred start anchor.")
            return metadata.open_time_ms, "market_open_time"

        if ticker_start_ms is not None:
            notes.append("Ticker encoded a date but not a validated event duration; using weak ticker anchor.")
            return ticker_start_ms, source

        if official_close_ms is not None and duration_minutes is not None:
            inferred_start = official_close_ms - duration_minutes * 60 * 1000
            notes.append("Backed into start time from official close minus expected duration.")
            return inferred_start, "official_close_minus_expected_duration"

        return None, "none"

    def derive_effective_close(
        self,
        *,
        metadata: MarketMetadata,
        family: str,
        style_scores: StyleScores,
        inferred_start_ms: Optional[int],
        expected_duration_minutes: Optional[int],
        observed_vol_bp: float,
        observed_range_bp: float,
        bid_flip_rate_per_minute: float,
        notes: List[str],
    ) -> Tuple[Optional[int], bool, Optional[int]]:
        official_close_ms = metadata.close_time_ms or metadata.settle_time_ms
        current_ms = now_ms()

        event_end_estimate_ms: Optional[int] = None
        if inferred_start_ms is not None and expected_duration_minutes is not None:
            event_end_estimate_ms = inferred_start_ms + expected_duration_minutes * 60 * 1000

        event_live_now_inferred = False
        if inferred_start_ms is not None and event_end_estimate_ms is not None:
            if inferred_start_ms <= current_ms <= event_end_estimate_ms + 30 * 60 * 1000:
                event_live_now_inferred = True

        # First anchor: if we have both inferred start and expected duration, trust that much more.
        if event_end_estimate_ms is not None:
            effective_close_ms = event_end_estimate_ms
            notes.append("Using inferred event end estimate as primary effective-close anchor.")
            if official_close_ms is not None:
                effective_close_ms = min(effective_close_ms, official_close_ms)
                if official_close_ms - event_end_estimate_ms > 6 * 60 * 60 * 1000:
                    notes.append("Official close is far later than plausible event end; capped to inferred event end.")
        else:
            effective_close_ms = official_close_ms
            if effective_close_ms is None:
                notes.append("No validated timing anchor found; effective close unavailable.")
                return None, event_live_now_inferred, event_end_estimate_ms
            notes.append("Fell back to official close because no validated event end estimate was available.")

        # Front-loaded speech/event contracts get compressed further.
        # 25 min: speeches typically run 20-25 min; 15 min was too aggressive.
        if family == "speech_event" and effective_close_ms is not None:
            remaining_ms = max(0, effective_close_ms - current_ms)
            capped_remaining_ms = min(remaining_ms, 25 * 60 * 1000)
            if capped_remaining_ms < remaining_ms:
                effective_close_ms = current_ms + capped_remaining_ms
                notes.append("Speech/event market capped to 25-minute maximum effective remaining window.")

        # Live-game compression: if already live, shorten horizon when instability is high.
        if event_live_now_inferred and effective_close_ms is not None:
            remaining_ms = max(0, effective_close_ms - current_ms)

            if observed_vol_bp >= 300 or observed_range_bp >= 250 or bid_flip_rate_per_minute >= 20:
                compressed_ms = min(remaining_ms, 45 * 60 * 1000)
                effective_close_ms = current_ms + compressed_ms
                notes.append("Live market with high instability; compressed effective close to <=20 minutes remaining.")
            elif family in {"mlb", "nba", "nhl", "soccer", "esports"}:
                # 180 min: esports fills observed as late as 186 min into a session;
                # MLB/NBA sessions in data run only 14-17 min so cap is irrelevant there.
                compressed_ms = min(remaining_ms, 180 * 60 * 1000)
                effective_close_ms = current_ms + compressed_ms
                notes.append("Live sports/esports market; capped effective remaining window to 180 minutes.")

        # General instability shrink.
        if effective_close_ms is not None:
            remaining_ms = max(0, effective_close_ms - current_ms)
            if observed_vol_bp >= 500 or observed_range_bp >= 400:
                # 45 min: tennis/darts sessions hit this threshold 56% of the time
                # and run up to 46 min with fills throughout. 10 min was cutting matches short.
                effective_close_ms = current_ms + min(remaining_ms, 45 * 60 * 1000)
                notes.append("Extreme startup instability; effective close capped to 45 minutes remaining.")
            elif observed_vol_bp >= 250 or observed_range_bp >= 200:
                # 120 min: commodity sessions hit this 36% of the time and run 400+ min
                # with 398 fills after the 240-min mark. 30 min was missing most of them.
                effective_close_ms = current_ms + min(remaining_ms, 120 * 60 * 1000)
                notes.append("Elevated startup instability; effective close capped to 120 minutes remaining.")

        if official_close_ms is not None and effective_close_ms is not None:
            effective_close_ms = min(effective_close_ms, official_close_ms)

        return effective_close_ms, event_live_now_inferred, event_end_estimate_ms

    def build_profile(
        self,
        ticker: str,
        *,
        confidence_reduction_threshold: float = DEFAULT_CONFIDENCE_REDUCTION_THRESHOLD,
        confidence_flatten_threshold: float = DEFAULT_CONFIDENCE_FLATTEN_THRESHOLD,
    ) -> MarketRiskProfile:
        metadata = self.load_metadata(ticker)
        samples = self.collect_startup_samples(ticker)

        observed_vol_bp = self.compute_observed_volatility_bp_60s_equiv(samples)
        observed_range_bp = self.compute_observed_range_bp(samples)
        bid_flip_rate = self.compute_bid_flip_rate_per_minute(samples)
        style_scores = self.infer_style_scores(metadata, samples)

        notes: List[str] = []
        family = self.infer_market_family(metadata)
        expected_duration_minutes = self.expected_duration_minutes_for_family(family)

        official_close_ms = metadata.close_time_ms or metadata.settle_time_ms
        inferred_start_ms, inferred_start_source = self.infer_start_time(
            metadata=metadata,
            family=family,
            official_close_ms=official_close_ms,
            notes=notes,
        )

        effective_close_ms, event_live_now_inferred, event_end_estimate_ms = self.derive_effective_close(
            metadata=metadata,
            family=family,
            style_scores=style_scores,
            inferred_start_ms=inferred_start_ms,
            expected_duration_minutes=expected_duration_minutes,
            observed_vol_bp=observed_vol_bp,
            observed_range_bp=observed_range_bp,
            bid_flip_rate_per_minute=bid_flip_rate,
            notes=notes,
        )

        suggested_action = "trade_normal"
        if observed_vol_bp >= 500 or observed_range_bp >= 400:
            suggested_action = "inventory_reduction_only"
            notes.append("Observed instability exceeds aggressive safety threshold.")

        current_ms = now_ms()
        soft_stop_time_ms = None
        hard_stop_time_ms = None
        flatten_only_time_ms = None
        if effective_close_ms is not None:
            remaining_ms = max(0, effective_close_ms - current_ms)

            if remaining_ms <= 5 * 60 * 1000:
                soft_stop_time_ms = current_ms
                hard_stop_time_ms = current_ms
                flatten_only_time_ms = current_ms
                suggested_action = "flatten_only"
                notes.append("Market is already in the terminal risk window.")
            else:
                soft_stop_time_ms = effective_close_ms - 5 * 60 * 1000
                hard_stop_time_ms = effective_close_ms - 10 * 60 * 1000
                flatten_only_time_ms = effective_close_ms - 15 * 60 * 1000

        confidence = 0.40
        if inferred_start_source == "ticker_datetime_token":
            confidence += 0.30
        elif inferred_start_source != "none":
            confidence += 0.15
        if expected_duration_minutes is not None:
            confidence += 0.10
        if official_close_ms is not None:
            confidence += 0.10
        if len(samples) >= 6:
            confidence += 0.05
        if family in {"mlb", "nba", "nhl", "soccer", "esports", "speech_event"}:
            confidence += 0.05
        if family in {"continuous_price"} and official_close_ms is not None:
            confidence += 0.05
        confidence = clamp(confidence, 0.05, 0.99)

        mode = "normal"
        reason = "normal"

        if effective_close_ms is None:
            mode = "flatten_only"
            suggested_action = "flatten_only"
            reason = "invalid_effective_close"
            notes.append("Effective close is unavailable; fail-safe flatten-only mode selected.")
        elif flatten_only_time_ms is not None and current_ms >= flatten_only_time_ms:
            mode = "flatten_only"
            suggested_action = "flatten_only"
            reason = "effective_close_window"
        elif confidence < float(confidence_flatten_threshold):
            mode = "flatten_only"
            suggested_action = "flatten_only"
            reason = "low_confidence"
            notes.append(
                f"Confidence {confidence:.3f} is below flatten threshold {float(confidence_flatten_threshold):.3f}."
            )
        elif (
            suggested_action == "inventory_reduction_only"
            or (hard_stop_time_ms is not None and current_ms >= hard_stop_time_ms)
            or confidence < float(confidence_reduction_threshold)
        ):
            mode = "reduction_only"
            reason = "risk_elevated" if suggested_action == "inventory_reduction_only" else "confidence_soft"
            if hard_stop_time_ms is not None and current_ms >= hard_stop_time_ms:
                reason = "hard_stop_window"
            if confidence < float(confidence_reduction_threshold):
                notes.append(
                    f"Confidence {confidence:.3f} is below reduction threshold {float(confidence_reduction_threshold):.3f}."
                )
        elif soft_stop_time_ms is not None and current_ms >= soft_stop_time_ms:
            mode = "reduction_only"
            reason = "soft_stop_window"

        log_event(
            "WATCHDOG_PROFILE_RESULT",
            ticker=metadata.ticker,
            family=family,
            official_close=ms_to_iso_utc(official_close_ms),
            inferred_start=ms_to_iso_utc(inferred_start_ms),
            effective_close=ms_to_iso_utc(effective_close_ms),
            hard_stop=ms_to_iso_utc(hard_stop_time_ms),
            flatten_only=ms_to_iso_utc(flatten_only_time_ms),
            observed_vol_bp=round(observed_vol_bp, 2),
            observed_range_bp=round(observed_range_bp, 2),
            bid_flip_rate=round(bid_flip_rate, 2),
            confidence=round(confidence, 3),
            mode=mode,
            reason=reason,
        )

        return MarketRiskProfile(
            ticker=metadata.ticker,
            title=metadata.title,
            generated_at_ms=current_ms,
            official_close_time_ms=official_close_ms,
            official_close_time_iso=ms_to_iso_utc(official_close_ms),
            inferred_start_time_ms=inferred_start_ms,
            inferred_start_time_iso=ms_to_iso_utc(inferred_start_ms),
            inferred_start_source=inferred_start_source,
            market_family=family,
            expected_duration_minutes=expected_duration_minutes,
            event_end_estimate_ms=event_end_estimate_ms,
            event_end_estimate_iso=ms_to_iso_utc(event_end_estimate_ms),
            event_live_now_inferred=event_live_now_inferred,
            effective_close_time_ms=effective_close_ms,
            effective_close_time_iso=ms_to_iso_utc(effective_close_ms),
            soft_stop_time_ms=soft_stop_time_ms,
            soft_stop_time_iso=ms_to_iso_utc(soft_stop_time_ms),
            hard_stop_time_ms=hard_stop_time_ms,
            hard_stop_time_iso=ms_to_iso_utc(hard_stop_time_ms),
            flatten_only_time_ms=flatten_only_time_ms,
            flatten_only_time_iso=ms_to_iso_utc(flatten_only_time_ms),
            observed_volatility_bp_60s_equiv=round(observed_vol_bp, 2),
            observed_range_bp=round(observed_range_bp, 2),
            bid_flip_rate_per_minute=round(bid_flip_rate, 2),
            style_scores=style_scores,
            suggested_action=suggested_action,
            confidence=round(confidence, 3),
            mode=mode,
            reason=reason,
            profile_version=PROFILE_VERSION,
            notes=notes,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a frozen startup-time market risk profile for one Kalshi ticker.")
    parser.add_argument("--ticker", required=True, help="Kalshi market ticker")
    parser.add_argument("--api-key-id", default="2622245b-4c4d-46f4-8a89-2e69069e7c70")
    parser.add_argument("--private-key", default="tizzler2003.txt")
    parser.add_argument("--use-demo", action="store_true", help="Use demo environment")
    parser.add_argument("--sample-seconds", type=float, default=4.0, help="How long to sample startup microstructure")
    parser.add_argument("--poll-interval-seconds", type=float, default=0.35, help="Polling interval during startup sample")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument(
        "--confidence-reduction-threshold",
        type=float,
        default=DEFAULT_CONFIDENCE_REDUCTION_THRESHOLD,
        help="Confidence below this level moves the profile to reduction_only.",
    )
    parser.add_argument(
        "--confidence-flatten-threshold",
        type=float,
        default=DEFAULT_CONFIDENCE_FLATTEN_THRESHOLD,
        help="Confidence below this level moves the profile to flatten_only.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.api_key_id:
        print("ERROR: missing API key ID. Pass --api-key-id or set KALSHI_API_KEY_ID.")
        return 2
    if not args.private_key:
        print("ERROR: missing private key path. Pass --private-key or set KALSHI_PRIVATE_KEY_PATH.")
        return 2
    if not os.path.exists(args.private_key):
        print(f"ERROR: private key file not found: {args.private_key}")
        return 2

    client = KalshiApiClient(
        api_key_id=args.api_key_id,
        private_key_path=args.private_key,
        use_demo=bool(args.use_demo),
    )
    profiler = MarketRiskProfiler(
        api_client=client,
        sample_seconds=float(args.sample_seconds),
        poll_interval_seconds=float(args.poll_interval_seconds),
    )

    log_event(
        "WATCHDOG_PROFILE_START",
        ticker=args.ticker,
        sample_seconds=args.sample_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        confidence_reduction_threshold=args.confidence_reduction_threshold,
        confidence_flatten_threshold=args.confidence_flatten_threshold,
    )

    profile = profiler.build_profile(
        args.ticker,
        confidence_reduction_threshold=float(args.confidence_reduction_threshold),
        confidence_flatten_threshold=float(args.confidence_flatten_threshold),
    )
    payload = asdict(profile)

    if args.pretty:
        print(json.dumps(payload, indent=2, sort_keys=False))
    else:
        print(json.dumps(payload, separators=(",", ":"), sort_keys=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
