"""Per-market-class bot configuration: classification and override resolution.

A market class is one of ``session_config.MARKET_CLASS_NAMES``. The screener
stamps every pick with a class plus that class's ``botClasses`` overrides, and
``session_config.bot_settings_payload(overrides=...)`` layers them over the
session's ``bot`` section when the market's actor is built.

``classify_market`` is pure: it takes a screener export row (columns of
``kalshi_screener.build_export_dataframe``), the market's per-series stats and
the thresholds from ``botClasses.classifier``. At Tier 1 series stats come from
``history_data/history.sqlite3`` (public trades + 1-minute candles) through
``load_series_stats_from_history``: trade frequency, realized spread from the
candle yes_bid/yes_ask closes, volume, and a maker-markout proxy (how far the
mid moves against the passive side of each recorded print at the horizon,
net of the venue maker fee). The same ``SeriesStats`` shape is produced from
live ``markout_history.GroupStats`` via ``SeriesStats.from_group_stats`` once
fill telemetry exists.

Precedence: toxic > thinWide > thickCalm > default.
"""

from __future__ import annotations

import logging
import math
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Union

from watchdogs.markout_history import series_from_ticker
from core.session_config import MARKET_CLASS_NAMES, validate_session_configuration

logger = logging.getLogger(__name__)

MARKET_CLASSES: Tuple[str, ...] = MARKET_CLASS_NAMES
DEFAULT_MARKET_CLASS = "default"
# Horizon of the Tier-1 maker-markout proxy (mirrors the bot's 30 s markout).
DEFAULT_MARKOUT_HORIZON_MS = 30_000
# Maker fee factor netted out of the public-trade markout proxy; mirrors
# replay.scoring.SCORING_FEE_FACTOR on the venue's 7c * p * (1 - p) formula.
HISTORY_MAKER_FEE_FACTOR = 0.55
CANDLE_PERIOD_MS = 60_000
PRICE_UNITS_PER_CENT = 100
COUNT_UNITS_PER_CONTRACT = 100

Overrides = Tuple[Tuple[str, Any], ...]
MarketClassResolver = Callable[[Mapping[str, object]], Tuple[str, Overrides]]


@dataclass(frozen=True)
class ClassifierThresholds:
    """``botClasses.classifier`` in classifier units (contracts / cents)."""

    thin_depth_contracts: float = 30.0
    wide_spread_cents: float = 12.0
    toxic_net_markout_cents_per_contract: float = -1.0
    min_series_fills: int = 10

    @classmethod
    def from_configuration(cls, configuration: Mapping[str, Any]) -> "ClassifierThresholds":
        classifier = dict((configuration.get("botClasses") or {}).get("classifier") or {})
        defaults = cls()
        return cls(
            thin_depth_contracts=float(classifier.get("thinDepthContracts", defaults.thin_depth_contracts)),
            wide_spread_cents=float(classifier.get("wideSpreadCents", defaults.wide_spread_cents)),
            toxic_net_markout_cents_per_contract=float(
                classifier.get("toxicNetMarkoutCentsPerContract", defaults.toxic_net_markout_cents_per_contract)
            ),
            min_series_fills=int(classifier.get("minSeriesFills", defaults.min_series_fills)),
        )


@dataclass(frozen=True)
class SeriesStats:
    """Per-series evidence the classifier consumes (source-agnostic)."""

    series: str
    fills: int = 0
    net_markout_cents_per_contract: Optional[float] = None
    realized_spread_cents: Optional[float] = None
    trades_per_hour: Optional[float] = None
    volume_contracts: float = 0.0

    @classmethod
    def from_group_stats(cls, stats: Any) -> "SeriesStats":
        """Adapt live ``markout_history.GroupStats`` (size-weighted fill economics)."""
        contracts = float(getattr(stats, "total_contracts", 0.0) or 0.0)
        if contracts > 0:
            per_contract = float(stats.total_net_cents) / contracts
        else:
            per_contract = float(stats.avg_net_cents)
        return cls(
            series=str(stats.key),
            fills=int(stats.fills),
            net_markout_cents_per_contract=per_contract,
            volume_contracts=contracts,
        )


@dataclass(frozen=True)
class MarketFeatures:
    ticker: str
    series: str
    spread_cents: Optional[float]
    depth_contracts: Optional[float]


def _number(value: object) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def extract_features(row: Mapping[str, object]) -> MarketFeatures:
    """Pull the classifier's inputs out of a screener export row (raw or typed)."""
    ticker = str(row.get("Ticker") or "").strip()
    sizes = [size for size in (_number(row.get("YES bid size")), _number(row.get("NO bid size"))) if size is not None]
    return MarketFeatures(
        ticker=ticker,
        series=series_from_ticker(ticker),
        spread_cents=_number(row.get("Spread(c)")),
        # The thinner displayed side is the one a fill leaves us exposed on.
        depth_contracts=min(sizes) if sizes else None,
    )


def _is_toxic(stats: Optional[SeriesStats], thresholds: ClassifierThresholds) -> bool:
    return (
        stats is not None
        and stats.net_markout_cents_per_contract is not None
        and stats.fills >= thresholds.min_series_fills
        and stats.net_markout_cents_per_contract <= thresholds.toxic_net_markout_cents_per_contract
    )


def classify_market(
    row: Mapping[str, object],
    series_stats: Optional[SeriesStats],
    thresholds: ClassifierThresholds,
) -> str:
    """Assign one market class; precedence toxic > thinWide > thickCalm > default.

    * ``toxic``: the series has at least ``min_series_fills`` fills and its
      size-weighted net markout per contract is at or below the threshold.
    * ``thinWide``: displayed top-level depth below ``thin_depth_contracts`` on
      either side, or a spread of ``wide_spread_cents`` or more. The row's
      live spread wins; the series' realized candle spread fills in when the
      row has none.
    * ``thickCalm``: neither thin nor wide, with a known spread.
    * ``default``: not enough information (no spread on the row or series).
    """
    features = extract_features(row)
    if _is_toxic(series_stats, thresholds):
        return "toxic"
    spread = features.spread_cents
    if spread is None and series_stats is not None:
        spread = series_stats.realized_spread_cents
    thin = features.depth_contracts is not None and features.depth_contracts < thresholds.thin_depth_contracts
    wide = spread is not None and spread >= thresholds.wide_spread_cents
    if thin or wide:
        return "thinWide"
    if spread is not None:
        return "thickCalm"
    return DEFAULT_MARKET_CLASS


def _freeze(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def resolve_overrides(configuration: Mapping[str, Any], market_class: str) -> Overrides:
    """``(field, value)`` pairs for ``market_class``; empty when classes are disabled.

    Values are frozen (lists become tuples) so the result is hashable and
    survives the multiprocessing queue inside a ``ScreenerPick``.
    """
    if market_class not in MARKET_CLASSES:
        raise ValueError(f"unknown market class: {market_class}")
    config = validate_session_configuration(configuration)
    classes = config["botClasses"]
    if not classes["enabled"]:
        return ()
    return tuple((str(item["field"]), _freeze(item["value"])) for item in classes[market_class]["overrides"])


# --- Tier-1 series stats from history.sqlite3 --------------------------------

_TRADE_AGGREGATE_SQL = """
WITH marked AS (
    SELECT t.ticker, t.ts_ms, t.taker_side, t.yes_price_units, t.count_units,
           (c.yes_bid_close_units + c.yes_ask_close_units) / 2.0 AS future_mid_units
    FROM trades t
    LEFT JOIN candles c
      ON c.ticker = t.ticker
     AND c.ts_ms = ((t.ts_ms + :horizon_ms) / :period_ms + 1) * :period_ms
    WHERE (:from_ms IS NULL OR t.ts_ms >= :from_ms)
      AND (:to_ms IS NULL OR t.ts_ms <= :to_ms)
),
scored AS (
    SELECT ticker, ts_ms, count_units,
           CASE WHEN future_mid_units IS NULL THEN NULL
                WHEN taker_side = 'yes' THEN yes_price_units - future_mid_units
                WHEN taker_side = 'no' THEN future_mid_units - yes_price_units
                ELSE NULL END AS maker_markout_units,
           7.0 * (yes_price_units / 10000.0) * (1.0 - yes_price_units / 10000.0) AS fee_cents
    FROM marked
)
SELECT ticker, COUNT(*), SUM(count_units), MIN(ts_ms), MAX(ts_ms),
       SUM(CASE WHEN maker_markout_units IS NOT NULL THEN count_units END),
       SUM(maker_markout_units * count_units),
       SUM(CASE WHEN maker_markout_units IS NOT NULL THEN fee_cents * count_units END)
FROM scored GROUP BY ticker
"""

_CANDLE_AGGREGATE_SQL = """
SELECT ticker, COUNT(*), AVG(yes_ask_close_units - yes_bid_close_units), SUM(COALESCE(volume_units, 0))
FROM candles
WHERE yes_bid_close_units > 0 AND yes_ask_close_units >= yes_bid_close_units
  AND (:from_ms IS NULL OR ts_ms >= :from_ms)
  AND (:to_ms IS NULL OR ts_ms <= :to_ms)
GROUP BY ticker
"""

_MARKET_ROWS_SQL = "SELECT ticker, volume_24h_units, oi_units FROM markets"


@dataclass(frozen=True)
class _TickerTrades:
    trades: int
    contracts_units: float
    first_ts_ms: int
    last_ts_ms: int
    covered_units: float
    markout_units_x_count: float
    fee_cents_x_count: float


@dataclass(frozen=True)
class _TickerCandles:
    candles: int
    spread_cents: Optional[float]
    volume_units: float


def _history_aggregates(
    path: Union[str, Path],
    *,
    horizon_ms: int,
    from_ms: Optional[int],
    to_ms: Optional[int],
) -> Tuple[Dict[str, _TickerTrades], Dict[str, _TickerCandles], Dict[str, Tuple[Optional[float], Optional[float]]]]:
    conn = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True)
    try:
        params = {"horizon_ms": int(horizon_ms), "period_ms": CANDLE_PERIOD_MS, "from_ms": from_ms, "to_ms": to_ms}
        trades = {
            str(row[0]): _TickerTrades(
                trades=int(row[1] or 0),
                contracts_units=float(row[2] or 0.0),
                first_ts_ms=int(row[3] or 0),
                last_ts_ms=int(row[4] or 0),
                covered_units=float(row[5] or 0.0),
                markout_units_x_count=float(row[6] or 0.0),
                fee_cents_x_count=float(row[7] or 0.0),
            )
            for row in conn.execute(_TRADE_AGGREGATE_SQL, params)
        }
        candles = {
            str(row[0]): _TickerCandles(
                candles=int(row[1] or 0),
                spread_cents=(float(row[2]) / PRICE_UNITS_PER_CENT) if row[2] is not None else None,
                volume_units=float(row[3] or 0.0),
            )
            for row in conn.execute(_CANDLE_AGGREGATE_SQL, {"from_ms": from_ms, "to_ms": to_ms})
        }
        markets = {
            str(row[0]): (
                (float(row[1]) / COUNT_UNITS_PER_CONTRACT) if row[1] is not None else None,
                (float(row[2]) / COUNT_UNITS_PER_CONTRACT) if row[2] is not None else None,
            )
            for row in conn.execute(_MARKET_ROWS_SQL)
        }
    finally:
        conn.close()
    return trades, candles, markets


def _series_stats(
    trades: Mapping[str, _TickerTrades],
    candles: Mapping[str, _TickerCandles],
    *,
    fee_factor: float,
) -> Dict[str, SeriesStats]:
    by_series: Dict[str, Dict[str, float]] = {}
    for ticker, item in trades.items():
        acc = by_series.setdefault(series_from_ticker(ticker), {})
        acc["fills"] = acc.get("fills", 0.0) + item.trades
        acc["contracts_units"] = acc.get("contracts_units", 0.0) + item.contracts_units
        acc["covered_units"] = acc.get("covered_units", 0.0) + item.covered_units
        acc["markout"] = acc.get("markout", 0.0) + item.markout_units_x_count
        acc["fee"] = acc.get("fee", 0.0) + item.fee_cents_x_count
        acc["first"] = min(acc.get("first", item.first_ts_ms), item.first_ts_ms)
        acc["last"] = max(acc.get("last", item.last_ts_ms), item.last_ts_ms)
    for ticker, item in candles.items():
        acc = by_series.setdefault(series_from_ticker(ticker), {})
        if item.spread_cents is not None and item.candles > 0:
            acc["spread_x_candles"] = acc.get("spread_x_candles", 0.0) + item.spread_cents * item.candles
            acc["candles"] = acc.get("candles", 0.0) + item.candles
    result: Dict[str, SeriesStats] = {}
    for series, acc in by_series.items():
        fills = int(acc.get("fills", 0))
        covered = acc.get("covered_units", 0.0)
        net = None
        if covered > 0:
            gross_cents = (acc["markout"] / covered) / PRICE_UNITS_PER_CENT
            fee_cents = (acc["fee"] / covered) * fee_factor
            net = gross_cents - fee_cents
        span_hours = (acc.get("last", 0) - acc.get("first", 0)) / 3_600_000.0
        rate = fills / span_hours if fills and span_hours > 0 else None
        spread = acc["spread_x_candles"] / acc["candles"] if acc.get("candles") else None
        result[series] = SeriesStats(
            series=series,
            fills=fills,
            net_markout_cents_per_contract=net,
            realized_spread_cents=spread,
            trades_per_hour=rate,
            volume_contracts=acc.get("contracts_units", 0.0) / COUNT_UNITS_PER_CONTRACT,
        )
    return result


def load_series_stats_from_history(
    path: Union[str, Path],
    *,
    horizon_ms: int = DEFAULT_MARKOUT_HORIZON_MS,
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
    fee_factor: float = HISTORY_MAKER_FEE_FACTOR,
) -> Dict[str, SeriesStats]:
    """Series stats derived from backfilled public data (missing DB -> ``{}``).

    The markout proxy treats every recorded print as a fill of the passive
    side and marks it against the mid of the (minute-aligned, period-end
    stamped) candle closing after ``horizon_ms``, net of the venue maker fee.
    Prints whose marking candle is missing are left out of the markout but
    still count as fills. It is a comparative adverse-selection measure, not
    our realized P&L.
    """
    if not Path(path).exists():
        return {}
    trades, candles, _ = _history_aggregates(path, horizon_ms=horizon_ms, from_ms=from_ms, to_ms=to_ms)
    return _series_stats(trades, candles, fee_factor=fee_factor)


def classify_history_markets(
    path: Union[str, Path],
    thresholds: ClassifierThresholds,
    *,
    horizon_ms: int = DEFAULT_MARKOUT_HORIZON_MS,
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
    fee_factor: float = HISTORY_MAKER_FEE_FACTOR,
) -> Dict[str, str]:
    """Class per backfilled ticker, using candle spreads as the row features.

    Displayed depth is not recorded at Tier 1, so ``thinWide`` here comes
    from spread alone and ``thickCalm`` means "tight spread"; the live
    screener adds the depth test.
    """
    trades, candles, markets = _history_aggregates(path, horizon_ms=horizon_ms, from_ms=from_ms, to_ms=to_ms)
    stats = _series_stats(trades, candles, fee_factor=fee_factor)
    result: Dict[str, str] = {}
    for ticker in sorted(set(trades) | set(candles) | set(markets)):
        candle = candles.get(ticker)
        volume, oi = markets.get(ticker, (None, None))
        row: Dict[str, object] = {
            "Ticker": ticker,
            "Spread(c)": candle.spread_cents if candle is not None else None,
            "Vol24h": volume,
            "OI": oi,
        }
        result[ticker] = classify_market(row, stats.get(series_from_ticker(ticker)), thresholds)
    return result


class HistorySeriesStatsSource:
    """TTL-cached ``load_series_stats_from_history`` for the live resolver.

    A failed reload keeps the previous stats (or none) and logs a warning, so
    a locked or missing history database never breaks a screener refresh.
    """

    def __init__(
        self,
        path: Union[str, Path],
        *,
        ttl_seconds: float = 3600.0,
        horizon_ms: int = DEFAULT_MARKOUT_HORIZON_MS,
        lookback_ms: Optional[int] = None,
    ) -> None:
        self.path = Path(path)
        self.ttl_seconds = float(ttl_seconds)
        self.horizon_ms = int(horizon_ms)
        self.lookback_ms = int(lookback_ms) if lookback_ms else None
        self._cache: Optional[Dict[str, SeriesStats]] = None
        self._loaded_at = 0.0

    def __call__(self) -> Mapping[str, SeriesStats]:
        now = time.monotonic()
        if self._cache is not None and now - self._loaded_at < self.ttl_seconds:
            return self._cache
        from_ms = int(time.time() * 1000) - self.lookback_ms if self.lookback_ms else None
        try:
            self._cache = load_series_stats_from_history(self.path, horizon_ms=self.horizon_ms, from_ms=from_ms)
        except sqlite3.Error as exc:
            logger.warning("market_classes: series stats unavailable from %s: %s", self.path, exc)
            if self._cache is None:
                self._cache = {}
        self._loaded_at = now
        return self._cache


def build_market_class_resolver(
    configuration: Mapping[str, Any],
    *,
    series_stats: Union[Mapping[str, SeriesStats], Callable[[], Mapping[str, SeriesStats]], None] = None,
) -> MarketClassResolver:
    """Resolver for ``Screener(market_class_resolver=...)``.

    Overrides are resolved once per class up front; each call classifies one
    screener row and returns ``(market_class, settings_overrides)``. With
    ``botClasses.enabled`` false every market is ``default`` with no
    overrides, so runtime keys stay unchanged. ``series_stats`` is a mapping
    or a zero-argument callable (e.g. ``HistorySeriesStatsSource``).
    """
    config = validate_session_configuration(configuration)
    enabled = bool(config["botClasses"]["enabled"])
    thresholds = ClassifierThresholds.from_configuration(config)
    overrides_by_class = {name: resolve_overrides(config, name) for name in MARKET_CLASSES}

    def lookup(series: str) -> Optional[SeriesStats]:
        source = series_stats() if callable(series_stats) else series_stats
        return (source or {}).get(series)

    def resolver(row: Mapping[str, object]) -> Tuple[str, Overrides]:
        if not enabled:
            return DEFAULT_MARKET_CLASS, ()
        series = series_from_ticker(str(row.get("Ticker") or ""))
        market_class = classify_market(row, lookup(series), thresholds)
        return market_class, overrides_by_class[market_class]

    return resolver
