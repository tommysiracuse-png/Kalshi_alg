"""Aggregate post-fill economics across all per-market telemetry databases.

Used by the screener to drop candidate markets that have demonstrated negative
realized P&L from prior trading. The screener calls :func:`load_unprofitable`
once per run; this module reads every ``telemetry_*.sqlite3`` file in the
workspace in read-only mode (WAL-safe; does not interfere with running bots)
and returns the set of tickers / series whose recent size-weighted net P&L
per contract is below threshold. Aggregation is size-weighted so a 100-lot
toxic fill counts 100x more than a 1-lot fill -- this matches actual dollar
risk rather than per-fill counts.

GROUND-TRUTH RULE: All economics here are computed from ``future_mid_yes_units``
(the raw observed market mid at horizon H, recorded model-free in the
``markouts`` table). We do NOT use the bot's own ``fair_yes_before_units`` or
the stored ``adverse_units`` field, because both of those are derived from the
bot's fair-value model -- and a model that is wrong on toxic flow inflates
claimed edge AND understates adverse selection. By marking each fill to the
realized future mid we get an honest, model-free realized P&L.

The "net" we compute per fill (in cents) is:

    For YES buy at price P:  net = future_mid_yes_c - P_c - fee_c
    For NO  buy at price P:  net = (100 - future_mid_yes_c) - P_c - fee_c

where:

* ``P`` = fill price
* ``future_mid_yes_c`` = observed market mid (YES) at horizon ``H`` (5s default)
* ``fee_c`` ~= ``7 * p * (1-p) * fee_factor`` -- the same maker fee
  approximation used by V1.py and the screener.

The derived ``adverse_cents`` field is signed: ``adverse = -(net + fee)``
(positive means price moved against me). It is NOT clamped at zero.

Schema reference (V1.py):
* ``fills(side, price_units, size_units, fair_yes_before_units, ...)``
* ``markouts(fill_key, horizon_ms, future_mid_yes_units, ...)``

Unit convention: 1 cent = 100 ``_units`` for both price and adverse.
"""

from __future__ import annotations

import glob
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

PRICE_UNITS_PER_CENT = 100
DEFAULT_FEE_FACTOR = 0.55  # matches V1.py default_fee_factor_for_maker_quotes


@dataclass
class FillEconomics:
    """One row per fill with ground-truth realized economics in cents.

    All values are computed from the observed future market mid at horizon H,
    NOT from the bot's own fair-value model. This makes them trustworthy even
    when the bot's fair-value estimate was wrong at fill time.

    * ``edge_cents`` is the realized mark-to-mid value at horizon: how much
      the contract was worth at H minus the price paid. It is *not* a
      pre-fill EV claim. ``net_cents = edge_cents - fee_cents``.
    * ``adverse_cents`` is signed: ``-(net + fee)``. Positive means price
      moved against me; negative means it moved in my favor. Not clamped.
    * ``size_contracts`` is the number of contracts on the fill, used to
      size-weight aggregation so a 100-lot fill counts 100x a 1-lot fill.
    """

    ticker: str
    side: str
    edge_cents: float
    adverse_cents: float
    fee_cents: float
    net_cents: float
    size_contracts: float = 1.0


@dataclass
class GroupStats:
    """Aggregated economics for a ticker or series.

    ``total_adverse_cents`` is size-weighted total adverse selection in cents
    (i.e. realized post-fill drift against us, summed across all contracts).
    Unlike ``total_net_cents`` it does NOT subtract the bot's claimed edge --
    so it stays trustworthy even when the bot's own fair-value model is wrong.
    Use this to catch markets that accumulate dollar bleed regardless of how
    profitable the bot believes its fills are.
    """

    key: str
    fills: int
    avg_edge_cents: float
    avg_adverse_cents: float
    avg_fee_cents: float
    avg_net_cents: float
    total_net_cents: float
    total_adverse_cents: float = 0.0
    total_contracts: float = 0.0
    contributing_tickers: Tuple[str, ...] = field(default_factory=tuple)


def series_from_ticker(ticker: str) -> str:
    """``KXBRENTD-26APR2217-T99.50`` -> ``KXBRENTD``."""
    if not ticker:
        return ""
    return ticker.split("-", 1)[0].upper()


def _ticker_from_db_path(path: str) -> str:
    base = os.path.basename(path)
    if base.startswith("telemetry_") and base.endswith(".sqlite3"):
        return base[len("telemetry_") : -len(".sqlite3")]
    return ""


def _open_readonly(path: str) -> Optional[sqlite3.Connection]:
    """Open a SQLite database read-only (WAL-safe; coexists with live writer)."""
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error as exc:
        logger.debug("markout_history: cannot open %s: %s", path, exc)
        return None


def _estimate_maker_fee_cents(price_cents: float, fee_factor: float) -> float:
    p = max(0.0, min(1.0, price_cents / 100.0))
    return 7.0 * p * (1.0 - p) * float(fee_factor)


def _read_fill_economics(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    horizon_ms: int,
    cutoff_ts_ms: int,
    fee_factor: float,
) -> List[FillEconomics]:
    """Join fills with their markout at the chosen horizon and compute net.

    Uses ``markouts.future_mid_yes_units`` -- the raw observed market mid at
    horizon -- as ground truth. The bot's own ``fair_yes_before_units`` and
    the stored (clamped) ``adverse_units`` are intentionally ignored because
    they are derived from the fair-value model and become unreliable on
    toxic flow.
    """
    try:
        cur = conn.execute(
            """
            SELECT f.side, f.price_units, f.size_units,
                   m.future_mid_yes_units
            FROM fills f
            JOIN markouts m
                ON m.fill_key = f.fill_key AND m.horizon_ms = ?
            WHERE f.ts_ms >= ?
            """,
            (int(horizon_ms), int(cutoff_ts_ms)),
        )
        rows = cur.fetchall()
    except sqlite3.Error as exc:
        logger.debug("markout_history: fill query failed for %s: %s", ticker, exc)
        return []

    out: List[FillEconomics] = []
    for side, price_units, size_units, future_mid_yes_units in rows:
        # Ground-truth requires a future market mid. Without it we cannot
        # honestly mark this fill -- skip rather than guess.
        if price_units is None or future_mid_yes_units is None:
            continue
        if side not in ("yes", "no"):
            continue
        price_c = float(price_units) / PRICE_UNITS_PER_CENT
        future_mid_yes_c = float(future_mid_yes_units) / PRICE_UNITS_PER_CENT
        # size_units is contracts*100 by Kalshi convention. Fall back to 1
        # contract if the column is null/zero so the fill still gets equal
        # weight rather than being dropped silently.
        size_contracts = (
            float(size_units) / PRICE_UNITS_PER_CENT
            if size_units is not None and float(size_units) > 0.0
            else 1.0
        )
        # Realized mark-to-mid value at horizon H. Model-free.
        if side == "yes":
            mark_to_mid_c = future_mid_yes_c - price_c
        else:
            mark_to_mid_c = (100.0 - future_mid_yes_c) - price_c
        fee_c = _estimate_maker_fee_cents(price_c, fee_factor)
        net_c = mark_to_mid_c - fee_c
        # Signed adverse: positive = market moved against me after the fill.
        # By construction adverse = -(net + fee) = -mark_to_mid.
        adverse_c = -mark_to_mid_c
        out.append(
            FillEconomics(
                ticker=ticker,
                side=str(side),
                edge_cents=mark_to_mid_c,
                adverse_cents=adverse_c,
                fee_cents=fee_c,
                net_cents=net_c,
                size_contracts=size_contracts,
            )
        )
    return out


def collect_fill_economics(
    *,
    workspace_dir: str,
    horizon_seconds: int = 5,
    lookback_days: float = 14.0,
    fee_factor: float = DEFAULT_FEE_FACTOR,
) -> List[FillEconomics]:
    """Scan every ``telemetry_*.sqlite3`` file and return one record per fill."""
    horizon_ms = int(horizon_seconds) * 1000
    cutoff_ts_ms = int((time.time() - max(0.0, float(lookback_days)) * 86400.0) * 1000.0)

    pattern = os.path.join(workspace_dir, "telemetry_*.sqlite3")
    db_paths = [p for p in glob.glob(pattern) if not p.endswith(("-shm", "-wal"))]

    all_fills: List[FillEconomics] = []
    for path in db_paths:
        ticker = _ticker_from_db_path(path)
        if not ticker:
            continue
        conn = _open_readonly(path)
        if conn is None:
            continue
        try:
            all_fills.extend(
                _read_fill_economics(
                    conn,
                    ticker=ticker,
                    horizon_ms=horizon_ms,
                    cutoff_ts_ms=cutoff_ts_ms,
                    fee_factor=fee_factor,
                )
            )
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass
    return all_fills


def _aggregate(
    fills: Iterable[FillEconomics],
    *,
    by_series: bool,
) -> Dict[str, GroupStats]:
    grouped: Dict[str, List[FillEconomics]] = {}
    for f in fills:
        key = series_from_ticker(f.ticker) if by_series else f.ticker
        if not key:
            continue
        grouped.setdefault(key, []).append(f)

    out: Dict[str, GroupStats] = {}
    for key, lst in grouped.items():
        n = len(lst)
        if n == 0:
            continue
        contributing = tuple(sorted({f.ticker for f in lst}))
        # Size-weighted aggregation: a 100-lot toxic fill counts 100x more
        # than a 1-lot fill. ``avg_*_cents`` is the per-contract average across
        # all contracts traded; ``total_net_cents`` is the actual realized P&L
        # impact in cents (per-contract net * contracts, summed).
        weight_total = sum(max(0.0, f.size_contracts) for f in lst)
        if weight_total <= 0.0:
            # Shouldn't happen (size defaults to 1.0) but stay safe.
            weight_total = float(n)
            weights = [1.0] * n
        else:
            weights = [max(0.0, f.size_contracts) for f in lst]
        avg_edge = sum(f.edge_cents * w for f, w in zip(lst, weights)) / weight_total
        avg_adv = sum(f.adverse_cents * w for f, w in zip(lst, weights)) / weight_total
        avg_fee = sum(f.fee_cents * w for f, w in zip(lst, weights)) / weight_total
        total_net = sum(f.net_cents * w for f, w in zip(lst, weights))
        total_adverse = sum(f.adverse_cents * w for f, w in zip(lst, weights))
        avg_net = total_net / weight_total
        out[key] = GroupStats(
            key=key,
            fills=n,
            avg_edge_cents=round(avg_edge, 4),
            avg_adverse_cents=round(avg_adv, 4),
            avg_fee_cents=round(avg_fee, 4),
            avg_net_cents=round(avg_net, 4),
            total_net_cents=round(total_net, 4),
            total_adverse_cents=round(total_adverse, 4),
            total_contracts=round(weight_total, 4),
            contributing_tickers=contributing,
        )
    return out


def evaluate_live_ticker(
    *,
    workspace_dir: str,
    ticker: str,
    net_threshold_cents: float = -1.0,
    min_fills: int = 3,
    horizon_seconds: int = 5,
    lookback_seconds: float = 3600.0,
    fee_factor: float = DEFAULT_FEE_FACTOR,
) -> Optional[GroupStats]:
    """Score the current SESSION of one running ticker.

    Reads only ``telemetry_<ticker>.sqlite3`` (read-only, WAL-safe) and only
    fills inside the last ``lookback_seconds``. Used by the watchdog runner to
    decide if a live bot is bleeding and should be flatten_only'd.

    Returns ``None`` if no scoreable fills exist yet (insufficient data → no
    opinion → bot keeps trading). Returns a :class:`GroupStats` row otherwise.
    The caller compares ``stats.fills >= min_fills`` and
    ``stats.avg_net_cents < net_threshold_cents`` to decide.
    """
    if not ticker:
        return None
    safe_ticker = ticker.strip()
    if not safe_ticker:
        return None

    db_path = os.path.join(workspace_dir, f"telemetry_{safe_ticker}.sqlite3")
    if not os.path.exists(db_path):
        return None

    horizon_ms = int(horizon_seconds) * 1000
    cutoff_ts_ms = int((time.time() - max(0.0, float(lookback_seconds))) * 1000.0)

    conn = _open_readonly(db_path)
    if conn is None:
        return None
    try:
        fills = _read_fill_economics(
            conn,
            ticker=safe_ticker,
            horizon_ms=horizon_ms,
            cutoff_ts_ms=cutoff_ts_ms,
            fee_factor=fee_factor,
        )
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            pass

    if not fills:
        return None
    stats = _aggregate(fills, by_series=False)
    return stats.get(safe_ticker)


def load_unprofitable(
    *,
    workspace_dir: str,
    net_threshold_cents: float = -1.0,
    ticker_min_fills: int = 3,
    series_min_fills: int = 20,
    horizon_seconds: int = 5,
    lookback_days: float = 14.0,
    fee_factor: float = DEFAULT_FEE_FACTOR,
    total_net_threshold_cents: float = -300.0,
) -> Tuple[Set[str], Set[str], Dict[str, GroupStats], Dict[str, GroupStats]]:
    """Return ``(toxic_series, toxic_tickers, series_stats, ticker_stats)``.

    A TICKER is flagged toxic (aggressive) when it has >= ``ticker_min_fills``
    AND EITHER:
      * size-weighted avg net per contract < ``net_threshold_cents`` (slow
        bleeders -- every contract loses money on average), OR
      * size-weighted total session net < ``total_net_threshold_cents``
        (dollar bleed has crossed a hard stop, regardless of per-contract
        average).

    A SERIES is flagged toxic (conservative) using the same triggers but
    requires the much higher ``series_min_fills`` evidence bar. Series blocks
    are stronger -- they exclude every strike including brand-new ones that
    have never traded -- so they need more evidence before triggering.

    All net values are GROUND-TRUTH realized P&L marked to the observed
    future market mid (``markouts.future_mid_yes_units``). The bot's own
    ``fair_yes_before_units`` is intentionally not used: it represents the
    bot's *claim* of edge at fill time, which becomes corrupt on toxic flow.
    """
    fills = collect_fill_economics(
        workspace_dir=workspace_dir,
        horizon_seconds=horizon_seconds,
        lookback_days=lookback_days,
        fee_factor=fee_factor,
    )
    series_stats = _aggregate(fills, by_series=True)
    ticker_stats = _aggregate(fills, by_series=False)

    def _flag(stats: Dict[str, GroupStats], min_fills: int) -> Set[str]:
        flagged: Set[str] = set()
        for key, s in stats.items():
            if s.fills < int(min_fills):
                continue
            avg_toxic = s.avg_net_cents < float(net_threshold_cents)
            total_toxic = s.total_net_cents < float(total_net_threshold_cents)
            if avg_toxic or total_toxic:
                flagged.add(key)
        return flagged

    return (
        _flag(series_stats, series_min_fills),
        _flag(ticker_stats, ticker_min_fills),
        series_stats,
        ticker_stats,
    )


def format_stats_report(
    series_stats: Dict[str, GroupStats],
    ticker_stats: Dict[str, GroupStats],
    *,
    toxic_series: Iterable[str],
    toxic_tickers: Iterable[str],
    horizon_seconds: int,
) -> str:
    if not series_stats and not ticker_stats:
        return "markout_history: no fills with markout data in lookback window."
    ts = set(toxic_series)
    tt = set(toxic_tickers)

    lines = [
        f"markout_history: scored {len(series_stats)} series / {len(ticker_stats)} tickers "
        f"(toxic: {len(ts)} series, {len(tt)} tickers @ horizon={horizon_seconds}s)"
    ]

    lines.append("  -- per series --")
    for s in sorted(series_stats.values(), key=lambda x: x.avg_net_cents):
        flag = "TOXIC" if s.key in ts else "ok   "
        lines.append(
            f"    {flag} {s.key:<18s} fills={s.fills:>4d} "
            f"edge={s.avg_edge_cents:+5.2f}c adv={s.avg_adverse_cents:+5.2f}c "
            f"fee={s.avg_fee_cents:+4.2f}c net={s.avg_net_cents:+5.2f}c "
            f"total_net={s.total_net_cents:+7.2f}c"
        )

    lines.append("  -- per ticker --")
    for t in sorted(ticker_stats.values(), key=lambda x: x.avg_net_cents):
        flag = "TOXIC" if t.key in tt else "ok   "
        lines.append(
            f"    {flag} {t.key:<46s} fills={t.fills:>4d} "
            f"edge={t.avg_edge_cents:+5.2f}c adv={t.avg_adverse_cents:+5.2f}c "
            f"net={t.avg_net_cents:+5.2f}c total_net={t.total_net_cents:+7.2f}c"
        )
    return "\n".join(lines)
