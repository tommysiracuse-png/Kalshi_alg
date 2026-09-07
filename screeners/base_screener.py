"""Venue-neutral screening contracts.

The base layer deliberately knows nothing about exchange status names or
venue-specific API fields.  Adaptors return normalized ``ScreenerPick``
records and the runtime consumes the typed result.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from core.fleet_models import ScreenerPick, ScreenerUpdate


@dataclass(frozen=True)
class ScreeningResult:
    venue: str
    picks: tuple[ScreenerPick, ...] = ()
    scanned_markets: int = 0
    selected_markets: int = 0
    warnings: tuple[str, ...] = ()
    truncated: bool = False
    metadata: Mapping[str, object] = field(default_factory=dict)
    api_activity: Mapping[str, object] = field(default_factory=dict)


class BaseScreener(ABC):
    """Common interface for venue screeners.

    Concrete adapters may provide a richer refresh implementation (the
    existing Kalshi screener does so to retain inventory carryover and CSV
    compatibility).  The default implementation is useful for lightweight
    screeners and tests and performs deterministic pick reconciliation.
    """

    venue = "unknown"

    @abstractmethod
    def screen(self, settings: Mapping[str, object]) -> ScreeningResult:
        """Return normalized picks and scan metadata for one cycle."""

    def get_latest_picks(self) -> tuple[ScreenerPick, ...]:
        update = getattr(self, "_latest_update", None)
        return tuple(getattr(update, "picks", ()) or ())

    def accept_snapshot(self, update: ScreenerUpdate, warnings: Sequence[str] = ()) -> None:
        self._latest_update = update
        self._generation = max(int(getattr(self, "_generation", 0)), int(update.generation_id))
        if warnings:
            self._last_warnings = tuple(str(item) for item in warnings)

    def status_snapshot(self) -> dict[str, object]:
        update = getattr(self, "_latest_update", None)
        return {
            "venue": self.venue,
            "running": bool(getattr(self, "_running", False)),
            "lastError": getattr(self, "_last_error", None),
            "warnings": list(getattr(self, "_last_warnings", ()) or ()),
            "generationId": getattr(update, "generation_id", None),
            "picks": [
                {"venue": pick.venue, "marketId": pick.market_id, "title": pick.title}
                for pick in self.get_latest_picks()
            ],
        }

    def last_run_metrics(self) -> dict[str, object]:
        return dict(getattr(self, "_last_run_metrics", {}) or {})

    async def refresh(
        self,
        current_picks: Mapping[str, ScreenerPick],
        *,
        reason: str = "scheduled",
        exchange_positions: Optional[Sequence[Any]] = None,
        traded_tickers: Optional[set[str]] = None,
    ) -> Optional[ScreenerUpdate]:
        # ``exchange_positions`` and ``traded_tickers`` are accepted by the
        # shared contract so launchers can use one call for every venue.  A
        # venue adapter may use them for inventory carryover.
        del exchange_positions, traded_tickers
        # The concrete Kalshi implementation owns its existing worker-thread
        # scheduling.  The small default path stays synchronous so adapters
        # with an in-memory source have no hidden executor lifecycle.
        result = self.screen(getattr(self, "settings", {}))
        current = dict(current_picks)
        desired = {pick.market_id: pick for pick in result.picks}
        common = set(current) & set(desired)
        changed = tuple(sorted(key for key in common if current[key].runtime_key() != desired[key].runtime_key()))
        kept = tuple(sorted(common - set(changed)))
        generation = int(getattr(self, "_generation", 0)) + 1
        self._generation = generation
        return ScreenerUpdate(
            generation_id=generation,
            generated_at_ms=int(time.time() * 1000),
            reason=reason,
            picks=tuple(result.picks),
            added=tuple(sorted(set(desired) - set(current))),
            kept=kept,
            changed=changed,
            removed=tuple(sorted(set(current) - set(desired))),
        )
