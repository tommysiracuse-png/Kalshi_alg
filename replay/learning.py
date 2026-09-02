"""Virtual-clock model learning for the replay driver.

Live, ``MarketActor`` closes two learning loops that the replay used to leave
open:

1. After every fill, ``schedule_fill_markouts`` spawns ``capture_fill_markouts``,
   which sleeps on the wall clock until ``fill_ts + horizon`` for every horizon in
   ``MARKOUT_HORIZONS_MS`` ∪ ``settings.markout_horizons_seconds`` and then feeds
   ``ToxicityModel.record_markout`` + ``TelemetryStore.record_markout`` with the
   adverse move against the *future* fair value / mid (top_of_book_bot.py,
   ``capture_fill_markouts``).
2. ``model_refresh_worker`` calls ``refresh_external_models`` every
   ``max(60, model_refresh_interval_seconds)`` seconds, which re-reads the
   telemetry DB into the models (``ToxicityModel.bootstrap_from_telemetry(5_000)``
   and ``FillProbabilityModel.bootstrap_from_telemetry(10_000)``).

``ReplayModelLearner`` reproduces both on the replay's virtual clock. Markout
observations are queued as (target_ts, seq) and evaluated -- through the very
same model entry points -- when the clock passes the target time, against the
actor's own book at that moment (Tier 1: synthesized book; Tier 2: recorded
book with our own overlays, exactly what the live actor would see). The
fill-probability model already settles its pending attempts inside the
actor's own quote cycle (``observe_quote_decision`` / ``register_fill``); the
learner adds the periodic telemetry-backed refresh so the in-memory stats are
rebuilt from the candidate's temp DB the way the live refresh does.

Everything is driven by the event stream, so two identical runs schedule,
evaluate and refresh at identical virtual times.
"""

from __future__ import annotations

import heapq
from typing import Dict, List, Optional, Tuple

from markout_metrics import MARKOUT_HORIZONS_MS, add_observation, empty_markout_aggregate

from replay.clock import ReplayClock

ONE_DOLLAR_PRICE_UNITS = 10_000


def markout_horizons_ms(settings) -> List[int]:
    """Same horizon set ``capture_fill_markouts`` iterates live."""
    return sorted(set(MARKOUT_HORIZONS_MS).union(
        int(value) * 1000 for value in settings.markout_horizons_seconds
    ))


def live_model_refresh_interval_ms(settings) -> int:
    """Mirror of ``model_refresh_worker``: ``max(60, model_refresh_interval_seconds)``."""
    return max(60, int(settings.model_refresh_interval_seconds)) * 1000


class ReplayModelLearner:
    """Replay-side replacement for ``schedule_fill_markouts`` + ``model_refresh_worker``.

    Install with ``actor.schedule_fill_markouts = learner.schedule_fill_markouts``
    and route every clock movement through ``advance_to``; call ``finish`` after
    the last event so observations aligned with it are evaluated.

    ``refresh_interval_ms`` defaults to the live worker's cadence; ``0``
    disables the periodic telemetry-backed refresh (markout observations still
    reach the models directly through ``record_markout``, exactly as live).
    """

    def __init__(
        self,
        actor,
        clock: ReplayClock,
        *,
        enabled: bool = True,
        refresh_interval_ms: Optional[int] = None,
    ) -> None:
        self.actor = actor
        self.clock = clock
        self.enabled = bool(enabled)
        settings = actor.settings
        self.horizons_ms = markout_horizons_ms(settings)
        self.refresh_interval_ms = max(0, (
            int(refresh_interval_ms)
            if refresh_interval_ms is not None
            else live_model_refresh_interval_ms(settings)
        ))
        # (target_ts_ms, seq, horizon_ms, payload); seq keeps the heap total-ordered.
        self._pending: List[Tuple[int, int, int, dict]] = []
        self._seq = 0
        self._next_refresh_ms: Optional[int] = None
        self.stats: Dict[str, int] = {
            "fills_scheduled": 0,
            "markouts_scheduled": 0,
            "markouts_observed": 0,
            "markouts_skipped_no_book": 0,
            "markouts_dropped_after_end": 0,
            "model_refreshes": 0,
        }

    # ------------------------------------------------------------------
    # Fill hook (same signature as MarketActor.schedule_fill_markouts)
    # ------------------------------------------------------------------

    def schedule_fill_markouts(
        self,
        *,
        fill_key: str,
        side: str,
        fill_price_units: int,
        fill_size_units: int,
        fill_fee_units: Optional[int],
        fill_timestamp_ms: int,
        fill_context,
    ) -> None:
        if not self.enabled:
            return
        payload = {
            "fill_key": str(fill_key),
            "side": str(side),
            "fill_price_units": int(fill_price_units),
            "fill_size_units": int(fill_size_units),
            "fill_fee_units": (int(fill_fee_units) if fill_fee_units is not None else None),
            "fill_timestamp_ms": int(fill_timestamp_ms),
            "fill_context": fill_context,
        }
        self.stats["fills_scheduled"] += 1
        for horizon_ms in self.horizons_ms:
            self._seq += 1
            heapq.heappush(
                self._pending,
                (int(fill_timestamp_ms) + int(horizon_ms), self._seq, int(horizon_ms), payload),
            )
            self.stats["markouts_scheduled"] += 1

    # ------------------------------------------------------------------
    # Clock driving
    # ------------------------------------------------------------------

    def advance_to(self, ts_ms: int) -> None:
        """Move the clock to ``ts_ms``, passing through every due observation.

        Observations strictly before ``ts_ms`` are evaluated first (the book as
        of the last event before their target, i.e. the same step-interpolated
        semantics ``scoring.MidPath.mid_at`` uses); observations whose target
        equals ``ts_ms`` wait until the events carrying that timestamp have been
        applied and are evaluated on the next advance (or ``finish``).
        """
        ts_ms = int(ts_ms)
        if self.enabled:
            self._drain(lambda target: target < ts_ms)
        self.clock.advance_to(ts_ms)
        if self.enabled:
            self._maybe_refresh()

    def finish(self) -> None:
        """Evaluate observations due at (or before) the final clock time."""
        if not self.enabled:
            return
        now = self.clock.now_ms()
        self._drain(lambda target: target <= now)
        self.stats["markouts_dropped_after_end"] += len(self._pending)
        self._pending.clear()

    def _drain(self, is_due) -> None:
        while self._pending and is_due(self._pending[0][0]):
            target_ms, _seq, horizon_ms, payload = heapq.heappop(self._pending)
            self.clock.advance_to(target_ms)
            self._maybe_refresh()
            self._observe(horizon_ms, payload)

    def _maybe_refresh(self) -> None:
        """Virtual-clock ``model_refresh_worker``.

        Live, the first refresh runs at startup (a no-op against the empty
        per-candidate DB) and then every ``refresh_interval_ms``.
        """
        if self.refresh_interval_ms <= 0:
            return
        now = self.clock.now_ms()
        if self._next_refresh_ms is None:
            self._next_refresh_ms = now + self.refresh_interval_ms
            return
        while now >= self._next_refresh_ms:
            self.refresh_models()
            self._next_refresh_ms += self.refresh_interval_ms

    def refresh_models(self) -> None:
        """The model part of ``MarketActor.refresh_external_models``."""
        self.actor.toxicity_model.bootstrap_from_telemetry(limit=5_000)
        self.actor.fill_probability_model.bootstrap_from_telemetry(limit=10_000)
        self.stats["model_refreshes"] += 1

    # ------------------------------------------------------------------
    # One markout observation (mirrors the per-horizon body of
    # MarketActor.capture_fill_markouts, minus the wall-clock sleep)
    # ------------------------------------------------------------------

    def _observe(self, horizon_ms: int, payload: dict) -> None:
        actor = self.actor
        side = payload["side"]
        fill_price_units = payload["fill_price_units"]
        future_context = actor.build_market_context()
        if future_context is None:
            self.stats["markouts_skipped_no_book"] += 1
            return
        future_fair = actor.fair_value_engine.estimate(future_context, update_state=False)
        if side == "yes":
            adverse_units = max(0, fill_price_units - future_fair.fair_yes_units)
        else:
            adverse_units = max(0, fill_price_units - future_fair.fair_no_units)
        future_mid_yes_units = future_context.mid_yes_units
        if side == "yes":
            signed_drift_units = int(future_mid_yes_units) - int(fill_price_units)
        else:
            signed_drift_units = (int(ONE_DOLLAR_PRICE_UNITS) - int(future_mid_yes_units)) - int(fill_price_units)
        bucket_key = actor.toxicity_model.record_markout(
            side=side,
            context=payload["fill_context"],
            horizon_ms=horizon_ms,
            adverse_units=adverse_units,
            signed_drift_units=signed_drift_units,
        )
        actor.telemetry_store.record_markout(
            fill_key=payload["fill_key"],
            ts_ms=self.clock.now_ms(),
            ticker=actor.market.ticker,
            side=side,
            horizon_ms=horizon_ms,
            fill_price_units=fill_price_units,
            future_mid_yes_units=future_context.mid_yes_units,
            future_fair_yes_units=future_fair.fair_yes_units,
            adverse_units=adverse_units,
            bucket_key=bucket_key,
        )
        fill_fee_units = payload["fill_fee_units"]
        if fill_fee_units is not None:
            key = str(horizon_ms)
            aggregate = actor.session_markouts_by_horizon.setdefault(
                key, empty_markout_aggregate(horizon_ms)
            )
            add_observation(
                aggregate,
                signed_price_units=signed_drift_units,
                size_units=payload["fill_size_units"],
                fee_units=fill_fee_units,
            )
        self.stats["markouts_observed"] += 1

    # ------------------------------------------------------------------
    # Result snapshot
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        """JSON-friendly summary of what the models learned (deterministic order)."""
        toxicity: Dict[str, dict] = {}
        for (horizon_ms, bucket_key), stat in self.actor.toxicity_model.stats.items():
            toxicity[f"{int(horizon_ms)}|{bucket_key}"] = {
                "observations": int(stat.observations),
                "ewma_adverse_units": round(float(stat.ewma_adverse_units), 4),
                "ewma_signed_drift_units": round(float(stat.ewma_signed_drift_units), 4),
                "signed_drift_observations": int(stat.signed_drift_observations),
            }
        fill_prob = {
            bucket_key: {"attempts": int(stat.attempts), "fills_30s": int(stat.fills_30s)}
            for bucket_key, stat in self.actor.fill_probability_model.stats.items()
        }
        attempts_recorded = 0
        try:
            attempts_recorded = len(
                self.actor.telemetry_store.load_recent_fill_prob_attempts(limit=1_000_000)
            )
        except Exception:
            attempts_recorded = 0
        markouts_recorded = 0
        try:
            markouts_recorded = len(
                self.actor.telemetry_store.load_recent_markouts(limit=1_000_000)
            )
        except Exception:
            markouts_recorded = 0
        return {
            "enabled": self.enabled,
            "refresh_interval_ms": int(self.refresh_interval_ms),
            **{key: int(value) for key, value in self.stats.items()},
            "markouts_recorded": int(markouts_recorded),
            "fill_prob_attempts_recorded": int(attempts_recorded),
            "toxicity_stats": toxicity,
            "fill_prob_stats": fill_prob,
        }
