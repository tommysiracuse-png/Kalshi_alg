"""Markout / settlement scoring for simulated fills.

All monetary amounts stay in the repo's fixed-point conventions:
price units = 1/10,000 dollar, count units = 1/100 contract, and monetary
totals in 1/10,000-dollar "units" via ``markout_metrics.monetary_markout_units``.
"""

from __future__ import annotations

import bisect
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Sequence, Tuple

from core.markout_metrics import COUNT_SCALE, PRICE_SCALE, monetary_markout_units

# Maker fee discount factor applied on top of the venue's quadratic
# 0.07 * p * (1-p) formula (mirrors FeeModel.baseline_fee_units in
# top_of_book_bot.py with fee_multiplier == SCORING_FEE_FACTOR).
SCORING_FEE_FACTOR = 0.55

HORIZONS_MS: Tuple[int, ...] = (1_000, 5_000, 30_000, 120_000)
PRIMARY_HORIZON_MS = 30_000


def maker_fee_units(price_units: int, count_units: int, factor: float = SCORING_FEE_FACTOR) -> int:
    """Quadratic maker fee in 1/10,000-dollar units (ROUND_HALF_UP)."""
    if price_units <= 0 or price_units >= PRICE_SCALE or count_units <= 0:
        return 0
    contracts = Decimal(count_units) / Decimal(COUNT_SCALE)
    price = Decimal(price_units) / Decimal(PRICE_SCALE)
    fee_dollars = Decimal("0.07") * Decimal(str(factor)) * contracts * price * (Decimal("1") - price)
    return int((fee_dollars * PRICE_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


class MidPath:
    """Step-interpolated yes-mid path sampled from the synthesized book."""

    def __init__(self) -> None:
        self._ts: List[int] = []
        self._mid: List[int] = []

    def record(self, ts_ms: int, mid_units: int) -> None:
        ts_ms = int(ts_ms)
        if self._ts and ts_ms <= self._ts[-1]:
            # Same-timestamp update wins; never allow out-of-order samples.
            if ts_ms == self._ts[-1]:
                self._mid[-1] = int(mid_units)
            return
        self._ts.append(ts_ms)
        self._mid.append(int(mid_units))

    def mid_at(self, ts_ms: int) -> Optional[int]:
        """Last known mid at or before ts; first mid before data starts."""
        if not self._ts:
            return None
        index = bisect.bisect_right(self._ts, int(ts_ms)) - 1
        if index < 0:
            index = 0
        return self._mid[index]


def score_fills(
    fills: Sequence[dict],
    mid_path: MidPath,
    horizons_ms: Sequence[int] = HORIZONS_MS,
) -> Dict[str, object]:
    """Net markout totals per horizon plus drawdown on the 30s equity curve.

    Each fill dict carries: ts_ms, side ('yes'/'no'), yes_price_units
    (fill price expressed in YES space), count_units, fee_units.
    """
    by_horizon: Dict[int, int] = {int(h): 0 for h in horizons_ms}
    contracts_units = 0
    fees_units = 0
    equity = 0
    peak = 0
    max_drawdown = 0

    for fill in fills:
        sign = 1 if fill["side"] == "yes" else -1
        fill_yes_units = int(fill["yes_price_units"])
        count_units = int(fill["count_units"])
        fee_units = int(fill["fee_units"])
        contracts_units += count_units
        fees_units += fee_units
        for horizon in by_horizon:
            future_mid = mid_path.mid_at(int(fill["ts_ms"]) + horizon)
            if future_mid is None:
                continue
            signed_price_units = sign * (int(future_mid) - fill_yes_units)
            net = monetary_markout_units(signed_price_units, count_units) - fee_units
            by_horizon[horizon] += net
            if horizon == PRIMARY_HORIZON_MS:
                equity += net
                if equity > peak:
                    peak = equity
                drawdown = peak - equity
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

    return {
        "net_markout_units_by_horizon": by_horizon,
        "total_net_units": by_horizon.get(PRIMARY_HORIZON_MS, 0),
        "max_drawdown_units": int(max_drawdown),
        "contracts_units": int(contracts_units),
        "fees_units": int(fees_units),
    }


def settlement_net_units(
    fills: Sequence[dict],
    result: Optional[str],
    close_ts_ms: Optional[int],
    start_ms: int,
    end_ms: int,
) -> Optional[int]:
    """Realized P&L valuing every fill at settlement, when known in-window."""
    if result not in ("yes", "no"):
        return None
    if close_ts_ms is None or not (start_ms <= int(close_ts_ms) <= end_ms):
        return None
    settle_yes_units = PRICE_SCALE if result == "yes" else 0
    total = 0
    for fill in fills:
        sign = 1 if fill["side"] == "yes" else -1
        signed_price_units = sign * (settle_yes_units - int(fill["yes_price_units"]))
        total += monetary_markout_units(signed_price_units, int(fill["count_units"])) - int(fill["fee_units"])
    return int(total)
