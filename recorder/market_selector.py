"""Chooses which markets to record and how to shard them across connections.

Market set = (a) the markets the live fleet is trading right now (from
``runtime/launcher_status.json`` -> ``bots[].ticker``) union (b) the most
actively traded markets on the exchange, found by sampling the global recent
public-trades feed exactly like ``history.backfill.select_markets`` does.
Fleet markets take priority, the union is validated against the exchange
(open markets only — a dead ticker in a subscribe command would poison a whole
connection) and capped at ``max_markets``.

Sharding is *stable*: a market already assigned to a connection stays there
across refreshes so only connections whose shard actually changed reconnect.

REST footprint per refresh: ``trade_sample_pages`` (default 12) trade pages
plus one ``GET /markets?tickers=`` per 100 candidates — ~15 calls / 15 min.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Sequence

LOGGER = logging.getLogger("recorder.selector")

OPEN_STATUSES = frozenset({"open", "active"})
TICKER_LOOKUP_BATCH = 100


# -- inputs ----------------------------------------------------------------------------


def read_fleet_tickers(path: Path, *, now_ms: int, max_age_seconds: float = 1800.0) -> list[str]:
    """Tickers of the bots the launcher currently reports (in file order, deduped).

    Returns ``[]`` when the file is missing, unreadable, or its ``generatedAt``
    is older than ``max_age_seconds`` (the fleet is stopped and the file is a
    leftover).  ``max_age_seconds <= 0`` disables the staleness check.
    """
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(data, dict):
        return []
    generated = data.get("generatedAt")
    if max_age_seconds > 0 and isinstance(generated, (int, float)):
        if int(now_ms) - int(generated) > max_age_seconds * 1000:
            return []
    tickers: list[str] = []
    for bot in data.get("bots") or []:
        if not isinstance(bot, dict):
            continue
        ticker = str(bot.get("ticker") or bot.get("marketId") or "").strip()
        if ticker and ticker not in tickers:
            tickers.append(ticker)
    return tickers


def sample_active_tickers(
    client: Any,
    *,
    pages: int = 12,
    page_size: int = 1_000,
    pause: Optional[Callable[[], None]] = None,
) -> list[tuple[str, int]]:
    """Rank tickers by trade count over the most recent ``pages * page_size`` trades."""
    counts: dict[str, int] = {}
    cursor = ""
    for _ in range(max(1, int(pages))):
        if pause is not None:
            pause()
        trades, cursor = client.list_public_trades("", page_size=page_size, max_results=page_size, cursor=cursor)
        for trade in trades:
            ticker = str(getattr(trade, "market_id", "") or "")
            if ticker:
                counts[ticker] = counts.get(ticker, 0) + 1
        if not cursor or not trades:
            break
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def filter_open_markets(
    client: Any,
    tickers: Sequence[str],
    *,
    pause: Optional[Callable[[], None]] = None,
) -> list[str]:
    """Keep only tickers the exchange reports as open, preserving input order."""
    wanted = list(dict.fromkeys(str(t) for t in tickers if str(t)))
    if not wanted:
        return []
    open_set: set[str] = set()
    for start in range(0, len(wanted), TICKER_LOOKUP_BATCH):
        batch = wanted[start:start + TICKER_LOOKUP_BATCH]
        if pause is not None:
            pause()
        rows = client.list_markets_raw(venue_filters={"tickers": ",".join(batch)}, max_results=len(batch))
        for raw in rows or []:
            if not isinstance(raw, dict):
                continue
            if str(raw.get("status") or "").lower() in OPEN_STATUSES:
                ticker = str(raw.get("ticker") or "")
                if ticker:
                    open_set.add(ticker)
    return [t for t in wanted if t in open_set]


# -- pure combination logic (unit-tested) --------------------------------------------------


def build_market_set(fleet: Iterable[str], screener: Iterable[str], *, max_markets: int) -> list[str]:
    """Fleet markets first (they matter most for replaying live behaviour), then the
    screener ranking, deduped and capped."""
    out: list[str] = []
    seen: set[str] = set()
    for source in (fleet, screener):
        for ticker in source:
            ticker = str(ticker)
            if ticker and ticker not in seen:
                seen.add(ticker)
                out.append(ticker)
                if len(out) >= max_markets:
                    return out
    return out


def reshard(
    previous: Sequence[Sequence[str]],
    markets: Sequence[str],
    *,
    per_connection: int,
    connection_count: int,
) -> list[list[str]]:
    """Assign ``markets`` to ``connection_count`` shards of at most ``per_connection``.

    Markets already present in ``previous`` keep their shard (removed ones are
    dropped); new markets fill the lowest-index shard with free capacity, in
    priority order.  Anything beyond total capacity is dropped (the caller caps
    ``max_markets`` at capacity, so that only happens on misconfiguration).
    """
    per_connection = max(1, int(per_connection))
    connection_count = max(1, int(connection_count))
    wanted = list(dict.fromkeys(str(m) for m in markets if str(m)))
    wanted_set = set(wanted)
    shards: list[list[str]] = []
    placed: set[str] = set()
    for index in range(connection_count):
        old = previous[index] if index < len(previous) else ()
        kept = []
        for ticker in old:
            ticker = str(ticker)
            if ticker in wanted_set and ticker not in placed and len(kept) < per_connection:
                kept.append(ticker)
                placed.add(ticker)
        shards.append(kept)
    for ticker in wanted:
        if ticker in placed:
            continue
        for shard in shards:
            if len(shard) < per_connection:
                shard.append(ticker)
                placed.add(ticker)
                break
        else:
            LOGGER.warning("SHARD_CAPACITY_EXCEEDED | dropped=%s", ticker)
    return shards


# -- full pipeline -----------------------------------------------------------------------


class _Pacer:
    """Minimal REST pacing so selection never bursts (mirrors backfill's Throttle)."""

    def __init__(self, min_interval_s: float) -> None:
        self.min_interval = max(0.0, float(min_interval_s))
        self._last = 0.0

    def __call__(self) -> None:
        if self.min_interval <= 0:
            return
        now = time.monotonic()
        wait = self.min_interval - (now - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()


def select_markets(client: Any, config: Any, *, now_ms: Optional[int] = None) -> list[str]:
    """The complete selection: fleet ∪ top-N active, validated open, capped.

    Blocking (synchronous REST); the recorder runs it in a worker thread.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    pause = _Pacer(getattr(config, "rest_pause_seconds", 0.2))
    fleet = read_fleet_tickers(
        config.launcher_status_path,
        now_ms=now_ms,
        max_age_seconds=float(getattr(config, "fleet_status_max_age_seconds", 1800.0)),
    )
    ranked: list[str] = []
    if config.top_n > 0:
        try:
            ranked = [ticker for ticker, _count in sample_active_tickers(
                client, pages=int(getattr(config, "trade_sample_pages", 12)), pause=pause,
            )]
        except Exception as exc:
            LOGGER.warning("ACTIVE_SAMPLE_FAILED | error=%r (fleet markets only this round)", exc)
    # Validate a wider slice than top_n so closed/settled markets in the ranking
    # do not shrink the final set.
    candidates = build_market_set(fleet, ranked[: max(config.top_n * 2, config.top_n)], max_markets=10_000)
    open_tickers = set(filter_open_markets(client, candidates, pause=pause))
    fleet_open = [t for t in fleet if t in open_tickers]
    screener_open = [t for t in ranked if t in open_tickers][: config.top_n]
    dropped_fleet = [t for t in fleet if t not in open_tickers]
    if dropped_fleet:
        LOGGER.info("FLEET_TICKERS_NOT_OPEN | dropped=%s", dropped_fleet)
    selected = build_market_set(fleet_open, screener_open, max_markets=config.max_markets)
    LOGGER.info(
        "MARKET_SELECTION | fleet=%s active_ranked=%s open_candidates=%s selected=%s",
        len(fleet_open), len(ranked), len(open_tickers), len(selected),
    )
    return selected
