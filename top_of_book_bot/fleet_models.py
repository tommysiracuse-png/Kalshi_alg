"""Typed messages shared by the fleet screener, manager, and launcher."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional, Tuple


# Each managed market uses a bot process and a watchdog process, while watchdog
# profiling temporarily adds another Python process.  Keeping this as a hard
# application limit prevents a saved session or direct CLI invocation from
# exhausting the host simply by requesting an unbounded fleet.
MAX_CONCURRENT_BOTS = 40


@dataclass(frozen=True)
class ScreenerPick:
    market_id: str
    title: str
    yes_budget_cents: int
    no_budget_cents: int
    ranking: Mapping[str, object] = field(default_factory=dict)
    selection_reason: str = "screen"

    @property
    def ticker(self) -> str:
        return self.market_id

    @property
    def raw_row(self) -> Dict[str, str]:
        return {str(key): "" if value is None else str(value) for key, value in self.ranking.items()}

    def runtime_key(self) -> Tuple[int, int]:
        return self.yes_budget_cents, self.no_budget_cents


@dataclass(frozen=True)
class ScreenerUpdate:
    generation_id: int
    generated_at_ms: int
    reason: str
    picks: Tuple[ScreenerPick, ...]
    added: Tuple[str, ...]
    kept: Tuple[str, ...]
    changed: Tuple[str, ...]
    removed: Tuple[str, ...]
    inventory_carried: Tuple[str, ...] = ()
    inventory_unknown: Tuple[str, ...] = ()

    @property
    def pick_by_market_id(self) -> Dict[str, ScreenerPick]:
        return {pick.market_id: pick for pick in self.picks}


@dataclass(frozen=True)
class ScreenerEvent:
    ok: bool
    reason: str
    generated_at_ms: int
    update: Optional[ScreenerUpdate] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class BotManagerEvent:
    event_type: str
    market_id: str
    generated_at_ms: int
    detail: Mapping[str, object] = field(default_factory=dict)
