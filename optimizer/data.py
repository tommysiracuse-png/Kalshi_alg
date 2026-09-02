"""Read-only access to the market history database.

Also hosts the *historical screener filter*: the live fleet only trades
markets that pass ``kalshi_screener.market_passes_safety_filters`` at a
screener refresh, so the optimizer replays the same universe by re-evaluating
those liquidity / time filters on the backfilled candles and trades at fixed
evaluation times (``screen_history_windows``).
"""

from __future__ import annotations

import sqlite3
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

MarketWindow = Tuple[str, int, int]  # (ticker, start_ms, end_ms)


def load_market_windows(
    history_db_path: str,
    limit: Optional[int] = None,
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
) -> List[MarketWindow]:
    """Tradable window per ticker = [min(candle ts), max(candle ts)].

    Markets are ordered by TRADE count (fill opportunities, not lifetime) then
    ticker so the selection is deterministic; ``limit`` caps how many are used.
    A candle-count ordering picks long-lived sleepy markets where the bot never
    fills and every score degenerates to zero.

    ``from_ms``/``to_ms`` restrict optimization to a date range: each market's
    window is clamped to the range and markets with no overlap are dropped, so a
    run can target "last week" vs "last month" vs a specific volatile period.
    """
    uri = f"file:{history_db_path}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        rows = con.execute(
            "SELECT c.ticker, MIN(c.ts_ms), MAX(c.ts_ms),"
            " (SELECT COUNT(*) FROM trades t WHERE t.ticker = c.ticker) AS n_trades"
            " FROM candles c GROUP BY c.ticker"
        ).fetchall()
    finally:
        con.close()
    usable = []
    for ticker, start, end, n_trades in rows:
        if start is None or end <= start:
            continue
        lo = int(start) if from_ms is None else max(int(start), int(from_ms))
        hi = int(end) if to_ms is None else min(int(end), int(to_ms))
        if hi <= lo:  # no overlap with the requested period
            continue
        usable.append((ticker, lo, hi, int(n_trades)))
    usable.sort(key=lambda r: (-r[3], r[0]))
    if limit is not None and limit > 0:
        usable = usable[:limit]
    return [(t, s, e) for t, s, e, _ in usable]


def data_window(markets: List[MarketWindow]) -> Tuple[int, int]:
    if not markets:
        raise ValueError("no replayable markets found in the history database")
    return min(m[1] for m in markets), max(m[2] for m in markets)


def latest_data_ms(history_db_path: str) -> Optional[int]:
    """Most recent candle timestamp across all markets (for --last-days)."""
    con = sqlite3.connect(f"file:{history_db_path}?mode=ro", uri=True)
    try:
        row = con.execute("SELECT MAX(ts_ms) FROM candles").fetchone()
    finally:
        con.close()
    return int(row[0]) if row and row[0] is not None else None


# ----------------------------------------------------------------------
# Historical screener filter (the live fleet's market universe, replayed)
# ----------------------------------------------------------------------

PRICE_UNITS_PER_CENT = 100          # PRICE_SCALE 10_000 per dollar -> 100 per cent
COUNT_UNITS_PER_CONTRACT = 100      # COUNT_SCALE
MINUTE_MS = 60_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
# The candle whose close is used as "the book at the evaluation time" must lie
# within this distance of it; backfilled 1-minute candles have multi-minute
# gaps when nothing traded, so an exact match is too strict.
SCREENER_CANDLE_TOLERANCE_MS = 60 * MINUTE_MS
# Default re-evaluation cadence across the data window. The fleet refreshes
# its screen every ~20 minutes; most history markets (sports, 15-minute
# crypto) are only live for hours, so evaluating solely at the window start
# and split boundaries misses nearly all of them.
DEFAULT_SCREENER_EVAL_HOURS = 1.0
# Kalshi multivariate-event (combo) markets are backfilled under KXMVE* series;
# the live screener drops them via the API's ``mve_filter=exclude``.
MVE_SERIES_PREFIX = "KXMVE"

# (canonical name, kalshi_screener_config constant, fallback default)
_SCREENER_FIELDS: Tuple[Tuple[str, str, Any], ...] = (
    ("min_spread_cents", "MIN_SPREAD_CENTS", 4),
    ("max_spread_cents", "MAX_SPREAD_CENTS", 35),
    ("min_yes_bid_cents", "MIN_YES_BID_CENTS", 5),
    ("min_no_bid_cents", "MIN_NO_BID_CENTS", 5),
    ("min_vol24h", "MIN_VOL24H", 500),
    ("min_oi", "MIN_OI", 100),
    ("min_time_to_close_hrs", "MIN_TIME_TO_CLOSE_HRS", 3),
    ("max_time_to_close_hrs", "MAX_TIME_TO_CLOSE_HRS", 50),
    ("excluded_ticker_keywords", "EXCLUDED_TICKER_KEYWORDS", []),
    ("excluded_series", "EXCLUDED_SERIES", []),
    ("status", "STATUS", "open"),
    ("mve_filter", "MVE_FILTER", "exclude"),
)
_NUMERIC_FIELDS = {
    "min_spread_cents", "max_spread_cents", "min_yes_bid_cents", "min_no_bid_cents",
    "min_vol24h", "min_oi", "min_time_to_close_hrs", "max_time_to_close_hrs",
}
_OPTIONAL_CAPS = {"max_spread_cents", "max_time_to_close_hrs"}  # None = no cap (live semantics)
_LIST_FIELDS = {"excluded_ticker_keywords", "excluded_series"}

# Filter reasons, in the order the live screener applies them.
REASON_PASS = "pass"
REASON_CLOSED = "closed"                    # close_ts <= evaluation time (status not open)
REASON_NO_CANDLE = "no_candle"              # no candle near the evaluation time (market not live yet / not recorded)
REASON_MVE = "mve"                          # multivariate combo market (mve_filter=exclude)
REASON_KEYWORD = "excluded_keyword"
REASON_SERIES = "excluded_series"
REASON_NO_BOOK = "no_book"                  # bid/ask outside 1..99c or crossed (live: extract_book_snapshot -> None)
REASON_YES_BID = "yes_bid_below_min"
REASON_NO_BID = "no_bid_below_min"
REASON_SPREAD_MIN = "spread_below_min"
REASON_SPREAD_MAX = "spread_above_max"
REASON_VOL24H = "vol24h_below_min"
REASON_OI = "oi_below_min"
REASON_TOO_SOON = "closes_too_soon"
REASON_TOO_LATE = "closes_too_late"
_NOT_LIVE_REASONS = {REASON_CLOSED, REASON_NO_CANDLE}
# Chain position of each reason: a failure summary reports the reason from the
# checkpoint at which the market got furthest through the live filter chain
# (a market rejected for open interest at one refresh and for 24 h volume at
# another is "thin", not "no volume").
_REASON_DEPTH = {
    reason: index for index, reason in enumerate((
        REASON_CLOSED, REASON_NO_CANDLE, REASON_MVE, REASON_KEYWORD, REASON_SERIES, REASON_NO_BOOK,
        REASON_YES_BID, REASON_NO_BID, REASON_SPREAD_MIN, REASON_SPREAD_MAX, REASON_VOL24H, REASON_OI,
        REASON_TOO_SOON, REASON_TOO_LATE, REASON_PASS,
    ))
}


def _norm_key(name: str) -> str:
    return str(name).replace("_", "").replace("-", "").lower()


# Session ``screener`` section spellings (session_config._SCREENER_FIELDS) that
# do not fold onto a canonical name by case/underscore alone.
_SESSION_ALIASES = {
    "minopeninterest": "min_oi",
    "mintimetoclosehours": "min_time_to_close_hrs",
    "maxtimetoclosehours": "max_time_to_close_hrs",
    "excludedtickerkeywords": "excluded_ticker_keywords",
}


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = value.replace(";", ",").replace(" ", ",").split(",")
        return [part.strip() for part in parts if part.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


def normalize_screener_settings(settings: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Canonical snake_case screener settings from any of the accepted spellings.

    Accepts ``kalshi_screener_config`` constant names (``MIN_SPREAD_CENTS``),
    the screener's own settings keys (``min_spread_cents``) and the session
    ``screener`` section's camelCase fields (``minSpreadCents``); keys are
    matched case- and underscore-insensitively, unknown keys are ignored and
    missing ones take the constants' values. ``max_spread_cents`` /
    ``max_time_to_close_hrs`` may be ``None`` (no cap), like the live screener.
    """
    supplied = {}
    for key, value in dict(settings or {}).items():
        folded = _norm_key(key)
        supplied[_norm_key(_SESSION_ALIASES.get(folded, folded))] = value
    try:
        import kalshi_screener_config as config
    except Exception:  # pragma: no cover - the constants module ships with the repo
        config = None
    base: Dict[str, Any] = {}
    for name, constant, fallback in _SCREENER_FIELDS:
        if _norm_key(name) in supplied:
            value = supplied[_norm_key(name)]
        elif config is not None:
            value = getattr(config, constant, fallback)
        else:
            value = fallback
        if name in _LIST_FIELDS:
            base[name] = [item.upper() for item in _as_list(value)]
        elif name in _NUMERIC_FIELDS:
            if value is None or (isinstance(value, str) and not value.strip()):
                base[name] = None if name in _OPTIONAL_CAPS else float(fallback)
            else:
                base[name] = float(value)
        else:
            base[name] = str(value if value is not None else fallback).strip().lower()
    return base


def default_screener_settings() -> Dict[str, Any]:
    """The live defaults: ``kalshi_screener_config`` constants (fallbacks if missing)."""
    return normalize_screener_settings({})


def screener_settings_from_configuration(configuration: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Normalized settings from a session configuration's ``screener`` section.

    Returns ``None`` when the configuration has no such section (older
    schemas), so callers can fall back to the constants.
    """
    if not isinstance(configuration, Mapping):
        return None
    section = configuration.get("screener")
    if not isinstance(section, Mapping) or not section:
        return None
    return normalize_screener_settings(section)


@dataclass(frozen=True)
class ScreenerVerdict:
    """One market's screener evaluation at ``eval_ms``."""

    ticker: str
    eval_ms: int
    passed: bool
    reason: str
    spread_cents: Optional[float] = None
    yes_bid_cents: Optional[float] = None
    no_bid_cents: Optional[float] = None
    vol24h_contracts: float = 0.0
    oi_contracts: float = 0.0
    hours_to_close: Optional[float] = None
    candle_ts_ms: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ScreenerReport:
    """Result of ``screen_history_windows``: pass/fail per market plus summary."""

    settings: Dict[str, Any]
    eval_ms: List[int]
    verdicts: Dict[str, ScreenerVerdict]            # summary verdict per ticker
    per_eval: Dict[int, Dict[str, ScreenerVerdict]] = field(default_factory=dict)

    @property
    def passing(self) -> List[str]:
        return [ticker for ticker, verdict in self.verdicts.items() if verdict.passed]

    @property
    def total(self) -> int:
        return len(self.verdicts)

    def reason_counts(self) -> Counter:
        return Counter(verdict.reason for verdict in self.verdicts.values() if not verdict.passed)

    def top_reasons(self, n: int = 4) -> List[Tuple[str, int]]:
        return self.reason_counts().most_common(n)

    def summary(self) -> Dict[str, Any]:
        """JSON-friendly summary (recorded in the optimizer run's args_json)."""
        return {
            "settings": dict(self.settings),
            "eval_ms": list(self.eval_ms),
            "total": self.total,
            "passed": len(self.passing),
            "reasons": dict(self.reason_counts()),
            "first_pass_ms": {
                ticker: verdict.eval_ms for ticker, verdict in sorted(self.verdicts.items()) if verdict.passed
            },
            "failed": {
                ticker: verdict.reason for ticker, verdict in sorted(self.verdicts.items()) if not verdict.passed
            },
        }


@dataclass
class _TickerBook:
    """One ticker's candles and minute-bucketed prints, sorted by time."""

    candle_ts: List[int] = field(default_factory=list)
    yes_bid_units: List[Any] = field(default_factory=list)
    yes_ask_units: List[Any] = field(default_factory=list)
    oi_units: List[Any] = field(default_factory=list)
    bucket_ts: List[int] = field(default_factory=list)      # minute-bucket starts with prints
    bucket_cum_units: List[float] = field(default_factory=list)  # cumulative count_units through each bucket

    def nearest_candle(self, eval_ms: int, tolerance_ms: int) -> Optional[int]:
        """Index of the candle nearest ``eval_ms`` within the tolerance.

        Ties prefer the candle at or before the evaluation time (the book the
        screener would actually have seen).
        """
        if not self.candle_ts:
            return None
        index = bisect_right(self.candle_ts, eval_ms)
        best = None
        for candidate in (index - 1, index):
            if 0 <= candidate < len(self.candle_ts):
                distance = abs(self.candle_ts[candidate] - eval_ms)
                if distance <= tolerance_ms and (best is None or distance < best[0]):
                    best = (distance, candidate)
        return best[1] if best is not None else None

    def volume_24h_contracts(self, eval_ms: int) -> float:
        """Contracts printed in the 24 h ending at ``eval_ms`` (minute buckets
        whose start lies in ``[eval-24h, eval)``)."""
        if not self.bucket_ts:
            return 0.0
        hi = bisect_left(self.bucket_ts, eval_ms)          # buckets starting before eval
        lo = bisect_left(self.bucket_ts, eval_ms - DAY_MS)  # buckets starting at/after eval-24h
        if hi <= lo:
            return 0.0
        before = self.bucket_cum_units[lo - 1] if lo > 0 else 0.0
        return (self.bucket_cum_units[hi - 1] - before) / COUNT_UNITS_PER_CONTRACT


class HistoryScreenSnapshot:
    """Candles, prints and close times loaded once for a time range so the
    screener can be evaluated at many times without re-querying SQLite."""

    def __init__(
        self,
        con: sqlite3.Connection,
        tickers: Optional[Iterable[str]],
        lo_ms: int,
        hi_ms: int,
        *,
        candle_tolerance_ms: int = SCREENER_CANDLE_TOLERANCE_MS,
    ) -> None:
        self.candle_tolerance_ms = int(candle_tolerance_ms)
        wanted = set(tickers) if tickers is not None else None
        rows = con.execute("SELECT ticker, series, close_ts_ms FROM markets").fetchall()
        self.markets: Dict[str, Tuple[Optional[str], Optional[int]]] = {
            str(ticker): (series, int(close_ts) if close_ts is not None else None)
            for ticker, series, close_ts in rows
            if wanted is None or ticker in wanted
        }
        if wanted is not None:
            # Windows for tickers missing a markets row still get a verdict (closed: unknown close time).
            for ticker in wanted - set(self.markets):
                self.markets[ticker] = (None, None)
        self.books: Dict[str, _TickerBook] = {}
        candle_rows = con.execute(
            "SELECT ticker, ts_ms, yes_bid_close_units, yes_ask_close_units, oi_units FROM candles"
            " WHERE ts_ms BETWEEN ? AND ? ORDER BY ticker, ts_ms",
            (int(lo_ms - self.candle_tolerance_ms), int(hi_ms + self.candle_tolerance_ms)),
        )
        for ticker, ts, bid, ask, oi in candle_rows:
            if wanted is not None and ticker not in wanted:
                continue
            book = self.books.setdefault(str(ticker), _TickerBook())
            book.candle_ts.append(int(ts))
            book.yes_bid_units.append(bid)
            book.yes_ask_units.append(ask)
            book.oi_units.append(oi)
        trade_rows = con.execute(
            "SELECT ticker, (ts_ms / ?) * ?, SUM(count_units) FROM trades"
            " WHERE ts_ms >= ? AND ts_ms < ? GROUP BY 1, 2 ORDER BY 1, 2",
            (MINUTE_MS, MINUTE_MS, int(lo_ms - DAY_MS), int(hi_ms)),
        )
        for ticker, bucket, units in trade_rows:
            if wanted is not None and ticker not in wanted:
                continue
            book = self.books.setdefault(str(ticker), _TickerBook())
            previous = book.bucket_cum_units[-1] if book.bucket_cum_units else 0.0
            book.bucket_ts.append(int(bucket))
            book.bucket_cum_units.append(previous + float(units or 0))

    def evaluate(self, settings: Mapping[str, Any], eval_ms: int) -> Dict[str, ScreenerVerdict]:
        """Verdict per market at ``eval_ms`` (see ``evaluate_screener_at``)."""
        keywords = list(settings.get("excluded_ticker_keywords") or [])
        excluded_series = set(settings.get("excluded_series") or [])
        mve_exclude = str(settings.get("mve_filter") or "").lower() == "exclude"
        min_yes = float(settings["min_yes_bid_cents"])
        min_no = float(settings["min_no_bid_cents"])
        min_spread = float(settings["min_spread_cents"])
        max_spread = settings.get("max_spread_cents")
        min_vol = float(settings["min_vol24h"])
        min_oi = float(settings["min_oi"])
        min_hours = float(settings["min_time_to_close_hrs"])
        max_hours = settings.get("max_time_to_close_hrs")
        eval_ms = int(eval_ms)

        verdicts: Dict[str, ScreenerVerdict] = {}
        for ticker, (series, close_ts) in self.markets.items():
            upper = ticker.upper()
            series_key = str(series or ticker.split("-")[0]).upper()
            hours = (close_ts - eval_ms) / HOUR_MS if close_ts is not None else None
            book = self.books.get(ticker)
            index = book.nearest_candle(eval_ms, self.candle_tolerance_ms) if book is not None else None
            vol24h = book.volume_24h_contracts(eval_ms) if book is not None else 0.0
            candle_ts = yes_bid = yes_ask = no_bid = spread = None
            oi = 0.0
            if index is not None:
                candle_ts = book.candle_ts[index]
                oi = float(book.oi_units[index] or 0) / COUNT_UNITS_PER_CONTRACT
                bid_units, ask_units = book.yes_bid_units[index], book.yes_ask_units[index]
                yes_bid = float(bid_units) / PRICE_UNITS_PER_CENT if bid_units is not None else None
                yes_ask = float(ask_units) / PRICE_UNITS_PER_CENT if ask_units is not None else None
                no_bid = (100.0 - yes_ask) if yes_ask is not None else None
                spread = (yes_ask - yes_bid) if (yes_ask is not None and yes_bid is not None) else None

            def verdict(passed: bool, reason: str) -> ScreenerVerdict:
                return ScreenerVerdict(
                    ticker=ticker, eval_ms=eval_ms, passed=passed, reason=reason,
                    spread_cents=spread, yes_bid_cents=yes_bid, no_bid_cents=no_bid,
                    vol24h_contracts=vol24h, oi_contracts=oi, hours_to_close=hours,
                    candle_ts_ms=candle_ts,
                )

            # status open: the market must still be trading at the evaluation time
            if hours is None or hours <= 0:
                verdicts[ticker] = verdict(False, REASON_CLOSED)
            elif index is None:
                verdicts[ticker] = verdict(False, REASON_NO_CANDLE)
            elif mve_exclude and series_key.startswith(MVE_SERIES_PREFIX):
                verdicts[ticker] = verdict(False, REASON_MVE)
            elif keywords and any(keyword in upper for keyword in keywords):
                verdicts[ticker] = verdict(False, REASON_KEYWORD)
            elif excluded_series and series_key in excluded_series:
                verdicts[ticker] = verdict(False, REASON_SERIES)
            # live extract_book_snapshot(): both bids must be valid 1..99c prices and the spread positive
            elif (yes_bid is None or no_bid is None or not 1 <= yes_bid <= 99 or not 1 <= no_bid <= 99
                    or spread is None or spread <= 0):
                verdicts[ticker] = verdict(False, REASON_NO_BOOK)
            elif yes_bid < min_yes:
                verdicts[ticker] = verdict(False, REASON_YES_BID)
            elif no_bid < min_no:
                verdicts[ticker] = verdict(False, REASON_NO_BID)
            elif spread < min_spread:
                verdicts[ticker] = verdict(False, REASON_SPREAD_MIN)
            elif max_spread is not None and spread > float(max_spread):
                verdicts[ticker] = verdict(False, REASON_SPREAD_MAX)
            elif vol24h < min_vol:
                verdicts[ticker] = verdict(False, REASON_VOL24H)
            elif oi < min_oi:
                verdicts[ticker] = verdict(False, REASON_OI)
            elif hours < min_hours:
                verdicts[ticker] = verdict(False, REASON_TOO_SOON)
            elif max_hours is not None and hours > float(max_hours):
                verdicts[ticker] = verdict(False, REASON_TOO_LATE)
            else:
                verdicts[ticker] = verdict(True, REASON_PASS)
        return verdicts


def evaluate_screener_at(
    history_db_path: str,
    settings: Optional[Mapping[str, Any]],
    eval_ms: int,
    tickers: Optional[Iterable[str]] = None,
    *,
    candle_tolerance_ms: int = SCREENER_CANDLE_TOLERANCE_MS,
) -> Dict[str, ScreenerVerdict]:
    """Would each history market have passed the live screener at ``eval_ms``?

    Mirrors ``kalshi_screener.market_passes_safety_filters`` on backfilled data:

    * book = the candle nearest ``eval_ms`` (within ``candle_tolerance_ms``);
      yes bid = ``yes_bid_close``, no bid = 100c - ``yes_ask_close``,
      spread = ``yes_ask_close - yes_bid_close`` (both in cents, 1c tick);
    * 24 h volume = trades ``count_units`` printed in the minute buckets
      starting in ``[eval-24h, eval)`` (COUNT_SCALE 100 per contract);
    * open interest = the same candle's ``oi_units``;
    * time to close = ``markets.close_ts_ms - eval_ms``; the market must be
      open (``close_ts_ms > eval_ms``) and have a candle near ``eval_ms``;
    * excluded keywords are case-insensitive substrings of the ticker,
      ``excluded_series`` matches the series prefix, ``mve_filter=exclude``
      drops the KXMVE* combo series.

    Not evaluable on history (left out): the markout toxic-series filter
    (needs live fill telemetry) and the EV-hurdle row builder (depends on the
    very bot parameters being optimized).
    """
    normalized = normalize_screener_settings(settings)
    con = sqlite3.connect(f"file:{history_db_path}?mode=ro", uri=True)
    try:
        snapshot = HistoryScreenSnapshot(con, tickers, int(eval_ms), int(eval_ms), candle_tolerance_ms=candle_tolerance_ms)
    finally:
        con.close()
    return snapshot.evaluate(normalized, int(eval_ms))


def screener_checkpoints(
    data_from_ms: int,
    data_to_ms: int,
    *,
    splits: int = 3,
    interval_hours: float = DEFAULT_SCREENER_EVAL_HOURS,
) -> List[int]:
    """Evaluation times: the window start, each walk-forward split boundary
    (train and test starts) and, when ``interval_hours`` > 0, a fixed cadence
    across the window (a coarse stand-in for the fleet's periodic refresh)."""
    from optimizer import walkforward

    points = {int(data_from_ms)}
    if data_to_ms > data_from_ms:
        for split in walkforward.build_splits(int(data_from_ms), int(data_to_ms), k=max(1, int(splits))):
            points.add(int(split.train_start))
            points.add(int(split.test_start))
        if interval_hours and interval_hours > 0:
            step = int(interval_hours * HOUR_MS)
            ts = int(data_from_ms) + step
            while step > 0 and ts < int(data_to_ms):
                points.add(ts)
                ts += step
    return sorted(points)


def screen_history_windows(
    history_db_path: str,
    settings: Optional[Mapping[str, Any]],
    windows: Sequence[MarketWindow],
    eval_ms: Sequence[int],
    *,
    candle_tolerance_ms: int = SCREENER_CANDLE_TOLERANCE_MS,
) -> ScreenerReport:
    """Evaluate the screener at every ``eval_ms`` for the windows' tickers.

    A market passes when it would have passed at ANY evaluation time (the
    fleet screens periodically and picks a market up at its first passing
    refresh). The summary verdict is the first passing one; for failures it is
    the verdict from the evaluation at which the market got furthest through
    the filter chain (latest such evaluation on ties), so ``closed`` /
    ``no_candle`` only remain when the market was never live at a checkpoint.
    """
    normalized = normalize_screener_settings(settings)
    tickers = [window[0] for window in windows]
    checkpoints = sorted({int(t) for t in eval_ms})
    per_eval: Dict[int, Dict[str, ScreenerVerdict]] = {}
    if checkpoints and tickers:
        con = sqlite3.connect(f"file:{history_db_path}?mode=ro", uri=True)
        try:
            snapshot = HistoryScreenSnapshot(
                con, tickers, checkpoints[0], checkpoints[-1], candle_tolerance_ms=candle_tolerance_ms,
            )
        finally:
            con.close()
        for ts in checkpoints:
            per_eval[ts] = snapshot.evaluate(normalized, ts)
    verdicts: Dict[str, ScreenerVerdict] = {}
    for ticker in tickers:
        history = [per_eval[ts][ticker] for ts in checkpoints if ticker in per_eval.get(ts, {})]
        if not history:
            verdicts[ticker] = ScreenerVerdict(ticker=ticker, eval_ms=0, passed=False, reason=REASON_NO_CANDLE)
            continue
        passing = [v for v in history if v.passed]
        if passing:
            verdicts[ticker] = passing[0]
            continue
        # max() keeps the first maximum, so reverse to prefer the latest on ties
        verdicts[ticker] = max(reversed(history), key=lambda v: _REASON_DEPTH.get(v.reason, -1))
    return ScreenerReport(settings=normalized, eval_ms=checkpoints, verdicts=verdicts, per_eval=per_eval)


def passing_windows(report: ScreenerReport, windows: Sequence[MarketWindow]) -> List[MarketWindow]:
    """``windows`` restricted to the report's passing tickers, order preserved
    (so the trade-count ranking survives the filter)."""
    keep = set(report.passing)
    return [window for window in windows if window[0] in keep]


# ----------------------------------------------------------------------
# Tier 2: recorded raw order-book messages (record_data/YYYYMMDD/HH/*.jsonl.gz)
# ----------------------------------------------------------------------


def tier2_coverage(record_root: str) -> Dict[str, Tuple[int, int, int]]:
    """{ticker: (min_ms, max_ms, message_count)} discovered from the recording."""
    from replay.loader import available_tier2_windows

    return available_tier2_windows(record_root)


def load_tier2_market_windows(
    record_root: str,
    limit: Optional[int] = None,
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
    min_messages: int = 1,
) -> List[MarketWindow]:
    """Replayable window per recorded ticker = [first, last recorded message].

    Markets are ordered by recorded message count (activity) then ticker so
    the selection is deterministic; ``limit`` caps how many are used and
    ``from_ms``/``to_ms`` clamp each window (markets without overlap drop).
    """
    usable = []
    for ticker, (start, end, count) in tier2_coverage(record_root).items():
        if count < max(1, int(min_messages)) or end <= start:
            continue
        lo = int(start) if from_ms is None else max(int(start), int(from_ms))
        hi = int(end) if to_ms is None else min(int(end), int(to_ms))
        if hi <= lo:
            continue
        usable.append((ticker, lo, hi, int(count)))
    usable.sort(key=lambda r: (-r[3], r[0]))
    if limit is not None and limit > 0:
        usable = usable[:limit]
    return [(t, s, e) for t, s, e, _ in usable]


def latest_tier2_ms(record_root: str) -> Optional[int]:
    """Most recent recorded message timestamp across all tickers (for --last-days)."""
    coverage = tier2_coverage(record_root)
    if not coverage:
        return None
    return max(end for _start, end, _count in coverage.values())
