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

from clients.monitoring import merge_activity_snapshots
from core.fleet_models import BotManagerEvent, ScreenerPick, ScreenerUpdate
from .capacity import SystemCapacity, SystemCapacityController


class MultiVenueBotManager:
    def __init__(self, managers: Mapping[str, Any], *, global_max_bots: int, venue_configs: Mapping[str, Mapping[str, Any]]):
        self.managers = {str(name).lower(): manager for name, manager in managers.items()}
        self.global_max_bots = int(global_max_bots)
        self.venue_configs = {str(name).lower(): dict(config) for name, config in venue_configs.items()}
        self.events: "asyncio.Queue[BotManagerEvent]" = asyncio.Queue()
        self._last_allocations: dict[str, tuple[tuple[str, str], ...]] = {}
        self._candidate_picks: dict[str, tuple[ScreenerPick, ...]] = {}
        sample_manager = next(iter(self.managers.values()), None)
        fleet_config = getattr(sample_manager, "fleet_config", {}) if sample_manager is not None else {}
        shard_size = int(getattr(sample_manager, "shard_size", 25) or 25) if sample_manager is not None else 25
        self._system_capacity = SystemCapacityController(
            self.global_max_bots,
            shard_size=shard_size,
            settings=fleet_config if isinstance(fleet_config, Mapping) else {},
        )
        self._last_system_capacity: SystemCapacity = self._system_capacity.snapshot()

    @staticmethod
    def _system_capacity_payload(snapshot: SystemCapacity) -> dict[str, Any]:
        return {
            "configuredMaxBots": snapshot.configured_max_bots,
            "hardMaxBots": snapshot.hard_max_bots,
            "effectiveMaxBots": snapshot.effective_max_bots,
            "resourceCapacity": snapshot.resource_capacity,
            "healthCapacity": snapshot.health_capacity,
            "reason": snapshot.reason or None,
            "systemCapacityReason": snapshot.reason or None,
            "cpuPercent": snapshot.cpu_percent,
            "memoryPercent": snapshot.memory_percent,
            "workerCpuPercent": snapshot.worker_cpu_percent,
            "workerMemoryRssBytes": snapshot.worker_memory_rss_bytes,
            "maxEventLoopLagMs": snapshot.max_event_loop_lag_ms,
            "maxQueueWaitMs": snapshot.max_queue_wait_ms,
            "starvedWorkers": snapshot.starved_workers,
            "totalWorkers": snapshot.total_workers,
            "unhealthySamples": snapshot.unhealthy_samples,
            "healthySinceMs": snapshot.healthy_since_ms or None,
            "lastReducedAtMs": snapshot.last_reduced_at_ms or None,
            "lastRecoveredAtMs": snapshot.last_recovered_at_ms or None,
            "activeWorkerBudget": snapshot.active_worker_budget,
            "workersRetained": snapshot.workers_retained,
            "workersScaledDown": snapshot.workers_scaled_down,
        }

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
        remaining = max(0, self._system_capacity.effective_max_bots)
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
            if update is not None:
                self._candidate_picks[venue] = tuple(update.picks)
            candidate = (
                tuple(update.picks)
                if update is not None
                else self._candidate_picks.get(venue, tuple(manager.current_picks.values()))
            )
            config = self.venue_configs.get(venue, {})
            venue_limit = max(0, int(config.get("maxBots", remaining)))
            take = min(venue_limit, remaining, len(candidate))
            picks = tuple(candidate[:take])
            set_capacity = getattr(manager, "set_system_capacity_limit", None)
            if callable(set_capacity):
                set_capacity(take)
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
                "systemCapacityLimit": self._system_capacity.effective_max_bots,
            }
        return {
            "globalMaxBots": self.global_max_bots,
            "systemCapacity": self._system_capacity_payload(self._system_capacity.snapshot()),
            "admittedMarkets": sum(admitted.values()),
            "slotsRemaining": remaining,
            "venues": details,
        }

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
        worker_rows = [
            worker
            for manager in self.managers.values()
            for worker in (manager.status_snapshot().get("workers", []) or [])
        ]
        previous_limit = self._system_capacity.effective_max_bots
        self._last_system_capacity = self._system_capacity.evaluate(worker_rows)
        if self._last_system_capacity.effective_max_bots != previous_limit:
            await self._rebalance_system_capacity()
        for venue in self.managers:
            await self._forward_events(venue)

    def _system_candidates(self, venue: str, manager: Any) -> tuple[ScreenerPick, ...]:
        candidates = self._candidate_picks.get(venue, tuple(manager.current_picks.values()))
        # Keep explicit carryover/reducing markets ahead of ordinary screener
        # picks when a host-pressure reduction must remove actors.
        position_units = getattr(manager, "_position_units_by_ticker", {}) or {}
        indexed = list(enumerate(candidates))
        indexed.sort(
            key=lambda item: (
                0 if (
                    str(getattr(item[1], "selection_reason", "")) == "exchange_position"
                    or int(position_units.get(item[1].market_id, 0) or 0) != 0
                ) else 1,
                item[0],
            )
        )
        return tuple(item[1] for item in indexed)

    async def _rebalance_system_capacity(self) -> None:
        """Apply the current fleet-wide cap without calling it a venue failure."""

        remaining = max(0, self._system_capacity.effective_max_bots)
        ordered = sorted(
            self.managers,
            key=lambda name: (int(self.venue_configs.get(name, {}).get("priority", 100)), name),
        )
        now = int(time.time() * 1000)
        for venue in ordered:
            manager = self.managers[venue]
            candidates = self._system_candidates(venue, manager)
            config = self.venue_configs.get(venue, {})
            venue_limit = max(0, int(config.get("maxBots", remaining)))
            selected = tuple(candidates[: min(venue_limit, remaining)])
            set_capacity = getattr(manager, "set_system_capacity_limit", None)
            if callable(set_capacity):
                set_capacity(len(selected))
            current = tuple(manager.current_picks.values())
            current_keys = {pick.market_id for pick in current}
            selected_keys = {pick.market_id for pick in selected}
            if current_keys != selected_keys or any(
                current_pick.runtime_key() != selected_pick.runtime_key()
                for current_pick in current
                for selected_pick in selected
                if current_pick.market_id == selected_pick.market_id
            ):
                update = ScreenerUpdate(
                    generation_id=now,
                    generated_at_ms=now,
                    reason="system_capacity",
                    picks=selected,
                    added=tuple(pick.market_id for pick in selected if pick.market_id not in current_keys),
                    kept=tuple(pick.market_id for pick in selected if pick.market_id in current_keys),
                    changed=(),
                    removed=tuple(pick.market_id for pick in current if pick.market_id not in selected_keys),
                )
                await manager.apply_update(update)
                await self._forward_events(venue)
            # Re-read the manager after its venue-specific admission gate. A
            # venue with insufficient API bandwidth may admit fewer markets;
            # those unused system slots must remain available to the next
            # venue in priority order.
            remaining = max(0, remaining - len(manager.current_picks))

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
        pnl = {"fills": 0, "feesCents": 0.0, "realizedCents": 0.0, "unrealizedCents": 0.0, "totalCents": 0.0}
        portfolio_items = []
        for item in venue_status.values():
            venue_pnl = item.get("pnl") or {}
            for key in pnl:
                value = venue_pnl.get(key)
                if isinstance(value, (int, float)):
                    pnl[key] += value
            portfolio_items.extend((item.get("portfolio") or {}).get("items") or [])
        gross_units = sum(abs(int(item.get("positionUnits") or 0)) for item in portfolio_items)
        net_units = sum(int(item.get("positionUnits") or 0) for item in portfolio_items)
        monitoring = {
            "running": True,
            "activeVenues": list(self.managers),
            "botsRunning": active,
            "configuredBots": configured,
            "portfolio": {
                "items": portfolio_items,
                "grossPositionUnits": gross_units,
                "netPositionUnits": net_units,
                "unknownMarkets": sum(1 for item in portfolio_items if not item.get("available")),
                "staleMarkets": sum(1 for item in portfolio_items if item.get("stale")),
            },
            "pnl": {key: round(value, 4) if isinstance(value, float) else value for key, value in pnl.items()},
            "apiActivity": merge_activity_snapshots(
                item.get("apiActivity") or {} for item in venue_status.values()
            ),
        }
        worker_rows = [worker for item in venue_status.values() for worker in item.get("workers", [])]
        heartbeat_ages = [int(item["heartbeatAgeMs"]) for item in worker_rows if item.get("heartbeatAgeMs") is not None]
        heartbeat_times = [int(item["heartbeatAtMs"]) for item in worker_rows if item.get("heartbeatAtMs")]
        monitoring["shardHealth"] = {
            "activeShards": sum(1 for item in worker_rows if item.get("running") and not item.get("stale")),
            "totalShards": len(worker_rows),
            "activeActors": sum(int(item.get("botsRunning") or 0) for item in worker_rows),
            "totalMemoryRssBytes": sum(int(item.get("memoryRssBytes") or 0) for item in worker_rows),
            "totalCpuPercent": round(sum(float(item.get("cpuPercent") or 0.0) for item in worker_rows), 3),
            "staleShards": sum(1 for item in worker_rows if item.get("stale")),
            "degradedShards": sum(1 for item in worker_rows if item.get("degraded")),
            "starvedShards": sum(1 for item in worker_rows if item.get("starved")),
            "recoveringShards": sum(1 for item in worker_rows if item.get("recovering")),
            "oldestHeartbeatAgeMs": max(heartbeat_ages, default=None),
            "latestHeartbeatAtMs": max(heartbeat_times, default=None),
            "recoveryCount": sum(int(item.get("recoveryCount") or 0) for item in worker_rows),
            "lastRecoveryAtMs": max(
                (int(item["lastRecoveryAtMs"]) for item in worker_rows if item.get("lastRecoveryAtMs")),
                default=None,
            ),
        }
        system_capacity = self._system_capacity_payload(self._system_capacity.snapshot())
        monitoring["systemCapacity"] = system_capacity
        venue_capacity: dict[str, Any] = {}
        for venue, item in venue_status.items():
            capacity = dict(item.get("capacity") or {})
            broker = item.get("broker") or {}
            rate_limit = broker.get("rateLimit") if isinstance(broker, Mapping) else None
            if isinstance(rate_limit, Mapping):
                capacity["rateLimit"] = dict(rate_limit)
            venue_capacity[venue] = capacity
        return {
            "venues": venue_status,
            "counts": {"configuredBots": configured, "activeBots": active},
            "allocation": {
                "globalMaxBots": self.global_max_bots,
                "systemMaxBots": self._system_capacity.effective_max_bots,
                "admittedMarkets": admitted,
                "slotsRemaining": max(0, self._system_capacity.effective_max_bots - admitted),
            },
            "systemCapacity": system_capacity,
            "venueCapacity": venue_capacity,
            "bots": [bot for item in venue_status.values() for bot in item.get("bots", [])],
            "workers": [worker for item in venue_status.values() for worker in item.get("workers", [])],
            "clients": [client for item in venue_status.values() for client in item.get("clients", [])],
            "capacity": {venue: item.get("capacity") for venue, item in venue_status.items()},
            "broker": {venue: item.get("broker") for venue, item in venue_status.items()},
            "monitoring": monitoring,
            "manager": {"running": True, "generatedAtMs": int(time.time() * 1000)},
        }
