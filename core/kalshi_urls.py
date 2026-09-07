"""Canonical Kalshi website links built from metadata already fetched by bots."""

from __future__ import annotations

import re
import unicodedata
from typing import Optional
from urllib.parse import urlparse


def slugify_market_title(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().lower()
    normalized = normalized.replace("&", " and ")
    return re.sub(r"^-+|-+$", "", re.sub(r"[^a-z0-9]+", "-", normalized))


def canonical_market_url(series_ticker: str, event_ticker: str, series_title: str) -> Optional[str]:
    series = series_ticker.strip().lower()
    event = event_ticker.strip().lower()
    slug = slugify_market_title(series_title)
    if not series or not event or not slug:
        return None
    return f"https://kalshi.com/markets/{series}/{slug}/{event}"


def is_canonical_market_url(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    parsed = urlparse(value)
    segments = [segment for segment in parsed.path.split("/") if segment]
    return parsed.scheme == "https" and parsed.netloc == "kalshi.com" and len(segments) == 4 and segments[0] == "markets"
