"""Priority-aware coordinator for independent venue fleet managers.

Each venue still uses the battle-tested :class:`ShardedBotManager`; this class
only owns cross-venue admission and status aggregation.  Keeping the workers
independent is important because a slow or rate-limited provider must not hold
the other provider's refresh loop hostage.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Mapping

from core.fleet_models import BotManagerEvent, ScreenerPick, ScreenerUpdate


class MultiVenueBotManager:
    def __init__(self, managers: Mapping[str, Any], *, global_max_bots: int, venue_configs: Mapping[str, Mapping[str, Any]]):
        self.managers = {str(name).lower(): manager for name, manager in managers.items()}
        self.global_max_bots = int(global_max_bots)
        self.venue_configs = {str(name).lower(): dict(config) for name, config in venue_configs.items()}
        self.events: "asyncio.Queue[BotManagerEvent]" = asyncio.Queue()
        self._last_allocations: dict[str, tuple[tuple[str, str], ...]] = {}

    @property
    def current_picks(self) -> dict[tuple[str, str], ScreenerPick]:
        return {
            (venue, str(market_id)): pick
            for venue, manager in self.managers.items()
            for market_id, pick in manager.current_picks.items()
        }

    @property
    def bots(self) -> dict[tuple[str, str], Any]:
        return {
            (venue, str(market_id)): bot
            for venue, manager in self.managers.items()
            for market_id, bot in manager.bots.items()
        }

    def begin_shutdown(self) -> None:
        for manager in self.managers.values():
            manager.begin_shutdown()

    async def _forward_events(self, venue: str) -> None:
        manager = self.managers[venue]
        while not manager.events.empty():
            try:
                event = manager.events.get_nowait()
            except asyncio.QueueEmpty:
                break
            await self.events.put(event)

    @staticmethod
    def _filtered_update(update: ScreenerUpdate, picks: tuple[ScreenerPick, ...]) -> ScreenerUpdate:
        selected = {pick.market_id for pick in picks}
        previous = set(update.kept) | set(update.added) | set(update.changed)
        return ScreenerUpdate(
            generation_id=update.generation_id,
            generated_at_ms=update.generated_at_ms,
            reason=update.reason,
            picks=picks,
            added=tuple(item for item in update.added if item in selected),
            kept=tuple(item for item in update.kept if item in selected),
            changed=tuple(item for item in update.changed if item in selected),
            removed=tuple(item for item in update.removed if item not in selected),
            inventory_carried=tuple(item for item in update.inventory_carried if item in selected),
            inventory_unknown=tuple(item for item in update.inventory_unknown if item in selected),
        )

    async def apply_updates(self, updates: Mapping[str, ScreenerUpdate]) -> dict[str, Any]:
        """Admit updates in priority order and return allocation metrics.

        ``updates`` may contain only the venue whose scan just completed.  The
        other venues' current allocations are included as candidates so a
        partial result is applied immediately without exceeding the global bot
        ceiling or losing the configured priority ordering.
        """
        remaining = max(0, self.global_max_bots)
        admitted: dict[str, int] = {}
        omitted: dict[str, int] = {}
        details: dict[str, dict[str, Any]] = {}
        ordered = sorted(
            self.managers,
            key=lambda name: (int(self.venue_configs.get(name, {}).get("priority", 100)), name),
        )
        for venue in ordered:
            manager = self.managers[venue]
            update = updates.get(venue)
            candidate = tuple(update.picks) if update is not None else tuple(manager.current_picks.values())
            config = self.venue_configs.get(venue, {})
            venue_limit = max(0, int(config.get("maxBots", remaining)))
            take = min(venue_limit, remaining, len(candidate))
            picks = tuple(candidate[:take])
            had_existing = bool(manager.current_picks)
            current = manager.current_picks
            unchanged = {key: pick.runtime_key() for key, pick in current.items()} == {pick.market_id: pick.runtime_key() for pick in picks}
            if not unchanged:
                if update is None:
                    update = ScreenerUpdate(
                        generation_id=int(time.time() * 1000),
                        generated_at_ms=int(time.time() * 1000),
                        reason="global_allocation",
                        picks=candidate,
                        added=tuple(candidate_item.market_id for candidate_item in candidate),
                        kept=(), changed=(), removed=tuple(current),
                    )
                await manager.apply_update(self._filtered_update(update, picks))
                await self._forward_events(venue)
            # A venue that reports fatal/unknown capacity may keep its
            # last-good allocation, but it must not admit a new set during a
            # cold refresh.  Managers expose the gate in their status payload.
            snapshot = manager.status_snapshot()
            capacity = snapshot.get("capacity") if isinstance(snapshot, Mapping) else None
            fatal_capacity = bool(getattr(self.managers[venue], "_capacity_error", ""))
            if picks and not had_existing and (fatal_capacity or (isinstance(capacity, Mapping) and capacity.get("gate_open") is False)):
                await manager.apply_update(self._filtered_update(update, ()))
                await self._forward_events(venue)
            actual = len(manager.current_picks)
            admitted[venue] = actual
            omitted[venue] = max(0, len(candidate) - actual)
            remaining = max(0, remaining - actual)
            details[venue] = {
                "priority": int(config.get("priority", 100)),
                "venueMaxBots": venue_limit,
                "requestedMarkets": len(candidate),
                "admittedMarkets": actual,
                "omittedMarkets": omitted[venue],
                "omittedReason": "capacity" if omitted[venue] else "",
                "globalSlotsRemaining": remaining,
                "capacity": capacity or {},
            }
        return {"globalMaxBots": self.global_max_bots, "admittedMarkets": sum(admitted.values()), "slotsRemaining": remaining, "venues": details}

    async def apply_update(self, update: ScreenerUpdate) -> None:
        venue = str(update.picks[0].venue if update.picks else "kalshi").lower()
        await self.apply_updates({venue: update})

    async def monitor_once(self) -> None:
        monitors = [
            manager.monitor_once
            for manager in self.managers.values()
            if callable(getattr(manager, "monitor_once", None))
        ]
        if monitors:
            await asyncio.gather(*(monitor() for monitor in monitors))
        for venue in self.managers:
            await self._forward_events(venue)

    async def stop_all(self, *, reason: str = "launcher_shutdown") -> None:
        await asyncio.gather(*(manager.stop_all(reason=reason) for manager in self.managers.values()))

    async def stop_bot(self, market_id: str, *, venue: str | None = None, reason: str = "operator_disabled") -> None:
        if venue:
            await self.managers[str(venue).lower()].stop_bot(market_id, reason=reason)
            return
        for manager in self.managers.values():
            if market_id in manager.current_picks:
                await manager.stop_bot(market_id, reason=reason)

    def status_snapshot(self) -> dict[str, Any]:
        venue_status = {venue: manager.status_snapshot() for venue, manager in self.managers.items()}
        configured = sum(int(item.get("counts", {}).get("configuredBots", 0) or 0) for item in venue_status.values())
        active = sum(int(item.get("counts", {}).get("activeBots", 0) or 0) for item in venue_status.values())
        admitted = sum(len(manager.current_picks) for manager in self.managers.values())
        return {
            "venues": venue_status,
            "counts": {"configuredBots": configured, "activeBots": active},
            "allocation": {"globalMaxBots": self.global_max_bots, "admittedMarkets": admitted, "slotsRemaining": max(0, self.global_max_bots - admitted)},
            "bots": [bot for item in venue_status.values() for bot in item.get("bots", [])],
            "workers": [worker for item in venue_status.values() for worker in item.get("workers", [])],
            "clients": [client for item in venue_status.values() for client in item.get("clients", [])],
            "capacity": {venue: item.get("capacity") for venue, item in venue_status.items()},
            "broker": {venue: item.get("broker") for venue, item in venue_status.items()},
            "manager": {"running": True, "generatedAtMs": int(time.time() * 1000)},
        }
