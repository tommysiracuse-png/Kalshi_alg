"""Pure, deterministic worker-local risk evaluation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping, MutableMapping, Optional


LOGGER = logging.getLogger("kalshi_top_of_book_bot.fleet_risk")

RiskMode = Literal["normal", "reduction_only", "flatten_only"]

# Mid prices are in 1/100 cent units (0..10_000 for 0..100c).
UNITS_PER_CENT = 100

DEFAULT_RISK_WINDOW_MS = 120_000
DEFAULT_RISK_STALE_AFTER_MS = 120_000
DEFAULT_ELEVATED_MOVE_UNITS = 750    # 7.5c
DEFAULT_EXTREME_MOVE_UNITS = 2_000   # 20c
# A book sample whose spread (100c - yes bid - no bid) is wider than this is
# not a price: its mid is excluded from the move measurement.  2026-09-02
# KXAAAGASDOH-26SEP03-3.885: the YES ask sat at 71-79c while the YES bid
# flickered between 74c and 3c (a single bid being pulled and re-posted every
# second), so the raw mids swung 40c <-> 75c inside one window - a 35c
# "extreme move" that flattened a position into the vacuum while the traded
# price had moved 13c.  With wide samples excluded the same window measures
# the 7c the reliable quotes actually moved.  Mirrors the flatten exit's own
# spread guard (top_of_book_bot.WATCHDOG_EXIT_MAX_SPREAD_UNITS).
DEFAULT_MAX_RELIABLE_SPREAD_UNITS = 2_000   # 20c

# Once an extreme mid move has put a market that holds inventory into
# ``flatten_only``, the mode stays latched until the position is flat or this
# cooldown has elapsed without a further extreme move.  The move window is
# time-bounded, so without the latch the mode would relax back to ``normal``
# as soon as the move's samples aged out of the window (~window_ms plus one
# risk-loop period) even though the position is still exposed and the single
# watchdog exit order may still be resting unfilled.  After the cooldown a
# market that *still* holds inventory is not released to ``normal``: it is
# downgraded to ``reduction_only`` (adding side suppressed, the reducing side
# may rest passively, no further watchdog IOC exits) until the position is
# flat.  Deliberately a module constant rather than a ``fleetRuntime``
# setting: the session schema is owned by session_config.py / the web
# editor; change the value here if it must become tunable.
RISK_FLATTEN_LATCH_MS = 600_000

_FLATTEN_LATCH_REASON = "extreme_price_move_latched"
_FLATTEN_COOLDOWN_REASON = "extreme_price_move_cooldown"


@dataclass(frozen=True)
class RiskSample:
    """One observation of the best bids.

    ``synthetic`` marks a sample the worker manufactured from its own copy of
    the book while the market was quiet (see ``worker.quiet_market_sample``).
    Synthetic samples take part in the mid-move window exactly like real ones
    but are *never* evidence that the venue feed is alive: staleness is judged
    on real (venue-event) samples only.
    """

    timestamp_ms: int
    best_yes_bid_units: Optional[int]
    best_no_bid_units: Optional[int]
    trade_count_units: int = 0
    synthetic: bool = False


@dataclass(frozen=True)
class RiskDecision:
    mode: RiskMode
    reason: str
    generated_at_ms: int
    confidence: float


@dataclass(frozen=True)
class RiskThresholds:
    """``evaluate_risk`` tunables as read from the session's ``fleetRuntime`` section.

    The defaults reproduce the historical hard-coded behaviour (7.5c / 20c
    mid move, 120 s staleness) with the move window bounded to 120 s.
    """

    window_ms: int = DEFAULT_RISK_WINDOW_MS
    stale_after_ms: int = DEFAULT_RISK_STALE_AFTER_MS
    elevated_move_units: int = DEFAULT_ELEVATED_MOVE_UNITS
    extreme_move_units: int = DEFAULT_EXTREME_MOVE_UNITS
    # Optional ``riskMaxSpreadCents`` (not part of the session schema today;
    # the default applies unless a configuration carries the key).
    max_spread_units: int = DEFAULT_MAX_RELIABLE_SPREAD_UNITS

    @classmethod
    def from_fleet_config(
        cls,
        fleet: Optional[Mapping[str, Any]],
        *,
        log: Optional[logging.Logger] = None,
    ) -> "RiskThresholds":
        """Read ``riskWindowSeconds`` / ``riskStaleSeconds`` / ``risk*MoveCents``.

        Missing keys fall back to the defaults silently so a worker spawned
        from disk by a controller that validated an older configuration keeps
        working.  A key that is *present* but unusable (non-numeric, boolean,
        non-positive) also falls back to the default, but that substitution is
        reported with one WARNING line listing every such key: the worker
        calls this once at start-up, so the operator sees the mismatch between
        the stored session and the thresholds actually in force exactly once
        per worker instead of running silently on values the UI does not show.
        """
        fleet = fleet or {}
        defaults = cls()
        substituted: list[str] = []

        def parse(key: str) -> Optional[float]:
            raw = fleet.get(key)
            if raw is None:
                return None
            if isinstance(raw, bool):
                substituted.append(f"{key}={raw!r}")
                return None
            try:
                value = float(raw)
            except (TypeError, ValueError):
                substituted.append(f"{key}={raw!r}")
                return None
            if not (value > 0):
                substituted.append(f"{key}={raw!r}")
                return None
            return value

        def seconds_to_ms(key: str, fallback_ms: int) -> int:
            value = parse(key)
            return fallback_ms if value is None else int(round(value * 1000))

        def cents_to_units(key: str, fallback_units: int) -> int:
            value = parse(key)
            return fallback_units if value is None else int(round(value * UNITS_PER_CENT))

        thresholds = cls(
            window_ms=seconds_to_ms("riskWindowSeconds", defaults.window_ms),
            stale_after_ms=seconds_to_ms("riskStaleSeconds", defaults.stale_after_ms),
            elevated_move_units=cents_to_units("riskElevatedMoveCents", defaults.elevated_move_units),
            extreme_move_units=cents_to_units("riskExtremeMoveCents", defaults.extreme_move_units),
            max_spread_units=cents_to_units("riskMaxSpreadCents", defaults.max_spread_units),
        )
        if substituted:
            (log or LOGGER).warning(
                "RISK_CONFIG_FALLBACK | invalid fleetRuntime values replaced by defaults: %s -> %s",
                ", ".join(substituted), thresholds,
            )
        return thresholds


def latch_flatten_only(
    decision: RiskDecision,
    latch: MutableMapping[str, int],
    ticker: str,
    *,
    now_ms: int,
    position_units: int,
    latch_ms: int = RISK_FLATTEN_LATCH_MS,
) -> RiskDecision:
    """Keep ``flatten_only`` sticky for a market holding inventory.

    ``latch`` maps ticker -> timestamp of the most recent extreme move that
    was evaluated while the market held a position.  Rules:

    * An ``extreme_price_move`` decision in ``flatten_only`` (re)arms the
      latch at ``now_ms``.
    * While armed and inside ``latch_ms``, a less restrictive evaluation (the
      move aged out of the window, a merely elevated move, a healthy window)
      is overridden by ``flatten_only`` / ``extreme_price_move_latched`` so
      the actor keeps suppressing the adding side and the watchdog keeps
      attempting the exit.
    * Once ``latch_ms`` has elapsed since the last extreme move with the
      position *still held*, the latch is not dropped: the mode is downgraded
      to ``reduction_only`` / ``extreme_price_move_cooldown`` (adding side
      stays suppressed, the reducing side may rest passively, the watchdog
      stops firing IOC exits every visit).  It never returns to ``normal``
      while inventory remains.  A ``reduction_only`` evaluation of its own
      (elevated move, crossed book) passes through unchanged.
    * The latch is released only when the position is flat.
    * ``reduction_only`` from an elevated move is not latched: it relaxes on
      window expiry as designed.  A decision that is already ``flatten_only``
      for another reason (stale inputs, evaluator failure) is left untouched.
    """
    if decision.mode == "flatten_only" and decision.reason == "extreme_price_move" and position_units:
        latch[ticker] = int(now_ms)
        return decision
    armed_at = latch.get(ticker)
    if armed_at is None:
        return decision
    if not position_units:
        latch.pop(ticker, None)
        return decision
    if decision.mode == "flatten_only":
        return decision
    if now_ms - armed_at < max(0, int(latch_ms)):
        return RiskDecision("flatten_only", _FLATTEN_LATCH_REASON, int(now_ms), decision.confidence)
    if decision.mode == "reduction_only":
        return decision
    return RiskDecision("reduction_only", _FLATTEN_COOLDOWN_REASON, int(now_ms), decision.confidence)


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
    stale_after_ms: int = DEFAULT_RISK_STALE_AFTER_MS,
    window_ms: int = DEFAULT_RISK_WINDOW_MS,
    elevated_move_units: int = DEFAULT_ELEVATED_MOVE_UNITS,
    extreme_move_units: int = DEFAULT_EXTREME_MOVE_UNITS,
    max_spread_units: int = DEFAULT_MAX_RELIABLE_SPREAD_UNITS,
) -> RiskDecision:
    """Decide the worker-local risk mode from the recent book samples.

    * A sample whose spread is wider than ``max_spread_units`` (one side of
      the book pulled, a flickering bid, our own deep quote left as the best
      level) has no usable mid: it still counts for liveness and for the
      crossed-book check but is left out of the move measurement.  A window
      with no usable mid at all cannot measure risk: ``reduction_only``
      (``book_too_wide``) while inventory is held, ``normal`` otherwise.

    * Staleness is judged on the newest *real* sample alone (``synthetic``
      samples are ignored here): no venue-event sample within
      ``stale_after_ms`` of ``now_ms`` means the inputs cannot be trusted.
      A window that holds only synthetic samples is stale too.  Because the
      worker appends real samples only on venue market-data events, a feed
      whose last event was at ``T`` is stale at every evaluation after
      ``T + stale_after_ms`` no matter how many synthetic samples were added.
    * The mid-move rule looks at every sample (synthetic included) within
      ``window_ms`` of ``now_ms`` (time-bounded, not count-bounded), so one
      old move stops counting once it ages out instead of lingering until
      the sample deque has rolled over. If every sample is older than the
      window the newest one alone is used, which yields a zero move.
    * ``elevated_move_units`` / ``extreme_move_units`` are the min-to-max mid
      move thresholds in 1/100 cent units.
    """
    window = tuple(sorted(samples, key=lambda item: item.timestamp_ms))
    if not window:
        return RiskDecision("reduction_only", "no_risk_samples", now_ms, 0.0)
    newest_real = next((sample for sample in reversed(window) if not sample.synthetic), None)
    age = max(0, now_ms - newest_real.timestamp_ms) if newest_real is not None else None
    if age is None or age > stale_after_ms:
        mode: RiskMode = "flatten_only" if position_units else "reduction_only"
        return RiskDecision(mode, "risk_inputs_stale", now_ms, 0.0)
    newest = window[-1]

    cutoff_ms = now_ms - max(0, int(window_ms))
    recent = [sample for sample in window if sample.timestamp_ms >= cutoff_ms] or [newest]

    mids = []
    crossed = False
    two_sided = 0
    for sample in recent:
        if sample.best_yes_bid_units is None or sample.best_no_bid_units is None:
            continue
        two_sided += 1
        if sample.best_yes_bid_units + sample.best_no_bid_units >= 10_000:
            crossed = True
            continue
        spread = 10_000 - sample.best_yes_bid_units - sample.best_no_bid_units
        if spread > max(0, int(max_spread_units)):
            continue  # not a price: one side is gone or a quote is flickering
        mids.append((sample.best_yes_bid_units + (10_000 - sample.best_no_bid_units)) / 2.0)
    if crossed:
        return RiskDecision("reduction_only", "crossed_or_locked_book", now_ms, 0.4)
    if not two_sided:
        return RiskDecision("reduction_only", "book_unavailable", now_ms, 0.2)
    if not mids:
        mode = "reduction_only" if position_units else "normal"
        return RiskDecision(mode, "book_too_wide", now_ms, 0.2)
    move = max(mids) - min(mids)
    if move >= extreme_move_units:
        mode = "flatten_only" if position_units else "reduction_only"
        return RiskDecision(mode, "extreme_price_move", now_ms, 0.3)
    if move >= elevated_move_units:
        return RiskDecision("reduction_only", "elevated_price_move", now_ms, 0.6)
    return RiskDecision("normal", "risk_window_healthy", now_ms, 0.95 if has_resting_orders or position_units else 0.9)
