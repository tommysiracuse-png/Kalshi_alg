"""Pure, deterministic worker-local risk evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Optional


RiskMode = Literal["normal", "reduction_only", "flatten_only"]


@dataclass(frozen=True)
class RiskSample:
    timestamp_ms: int
    best_yes_bid_units: Optional[int]
    best_no_bid_units: Optional[int]
    trade_count_units: int = 0


@dataclass(frozen=True)
class RiskDecision:
    mode: RiskMode
    reason: str
    generated_at_ms: int
    confidence: float


_SEVERITY = {"normal": 0, "reduction_only": 1, "flatten_only": 2}


def tighten(current: RiskDecision, proposed: RiskDecision) -> RiskDecision:
    """A failed/stale evaluator is not allowed to make risk less restrictive."""

    return proposed if _SEVERITY[proposed.mode] >= _SEVERITY[current.mode] else current


def evaluate_risk(
    samples: Iterable[RiskSample],
    *,
    now_ms: int,
    position_units: int,
    has_resting_orders: bool,
    stale_after_ms: int = 120_000,
) -> RiskDecision:
    window = tuple(sorted(samples, key=lambda item: item.timestamp_ms))
    if not window:
        return RiskDecision("reduction_only", "no_risk_samples", now_ms, 0.0)
    age = max(0, now_ms - window[-1].timestamp_ms)
    if age > stale_after_ms:
        mode: RiskMode = "flatten_only" if position_units else "reduction_only"
        return RiskDecision(mode, "risk_inputs_stale", now_ms, 0.0)

    mids = []
    crossed = False
    for sample in window:
        if sample.best_yes_bid_units is None or sample.best_no_bid_units is None:
            continue
        if sample.best_yes_bid_units + sample.best_no_bid_units >= 10_000:
            crossed = True
        mids.append((sample.best_yes_bid_units + (10_000 - sample.best_no_bid_units)) / 2.0)
    if crossed:
        return RiskDecision("reduction_only", "crossed_or_locked_book", now_ms, 0.4)
    if not mids:
        return RiskDecision("reduction_only", "book_unavailable", now_ms, 0.2)
    move = max(mids) - min(mids)
    if move >= 2_000:
        mode = "flatten_only" if position_units else "reduction_only"
        return RiskDecision(mode, "extreme_price_move", now_ms, 0.3)
    if move >= 750:
        return RiskDecision("reduction_only", "elevated_price_move", now_ms, 0.6)
    return RiskDecision("normal", "risk_window_healthy", now_ms, 0.95 if has_resting_orders or position_units else 0.9)
