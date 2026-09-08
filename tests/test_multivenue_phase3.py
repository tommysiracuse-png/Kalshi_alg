from __future__ import annotations

import asyncio

from core.fleet_models import ScreenerPick, ScreenerUpdate
from core.session_config import default_session_configuration, enabled_venues, effective_screener_configuration, validate_session_configuration
from fleet_runtime.capacity import AllocationRequest, CapitalAllocator, calculate_fleet_capacity
from fleet_runtime.multi_manager import MultiVenueBotManager


def test_schema_v5_partial_overrides_and_legacy_migration():
    config = default_session_configuration()
    config["launcher"]["maxBots"] = 20
    config["venues"]["kalshi"]["maxBots"] = 20
    config["venues"]["polymarket"].update(enabled=True, priority=1, maxBots=10)
    config["screener"]["venues"]["polymarket"] = {"topN": 7}
    normalized = validate_session_configuration(config)
    assert enabled_venues(normalized) == ("kalshi", "polymarket")
    assert effective_screener_configuration(normalized, "polymarket")["topN"] == 7
    legacy = dict(config)
    legacy["schemaVersion"] = 4
    legacy["screener"] = normalized["screener"]["general"]
    legacy.pop("venues")
    assert validate_session_configuration(legacy)["schemaVersion"] == 5


def test_capacity_uses_complete_two_sided_markets():
    capacity = calculate_fleet_capacity(api_tier="standard", read_refill_rate=10, write_refill_rate=30, requested_markets=200, cash_available_units=10**9)
    assert capacity.normal_quote_side_capacity == 51
    assert capacity.capacity_market_limit == 25
    assert capacity.admitted_markets == 25
    assert capacity.gate_open and capacity.capacity_limited


def test_allocator_never_admits_one_sided_normal_market():
    requests = [AllocationRequest(str(i), "series", i, "yes", 100, 100) for i in range(200)]
    result = CapitalAllocator().allocate(requests, available_cash_units=10**9, quote_side_capacity=51)
    assert result.gate_open
    assert len(result.sides_by_ticker) == 25
    assert all(len(sides) == 2 for sides in result.sides_by_ticker.values())
    assert result.omitted_markets == 175


class _FakeManager:
    def __init__(self):
        self.current_picks = {}
        self.bots = {}
        self.events = asyncio.Queue()

    async def apply_update(self, update):
        self.current_picks = {pick.market_id: pick for pick in update.picks}

    def status_snapshot(self):
        count = len(self.current_picks)
        return {"counts": {"configuredBots": count, "activeBots": count}, "bots": [], "workers": [], "clients": []}

    async def monitor_once(self):
        return None

    async def stop_all(self, *, reason=""): return None
    def begin_shutdown(self): return None


def test_priority_allocation_obeys_global_maximum():
    def update(venue):
        picks = tuple(ScreenerPick(str(i), str(i), 1, 1, venue=venue) for i in range(3))
        return ScreenerUpdate(1, 1, "test", picks, tuple(str(i) for i in range(3)), (), (), ())

    async def run():
        manager = MultiVenueBotManager(
            {"polymarket": _FakeManager(), "kalshi": _FakeManager()}, global_max_bots=3,
            venue_configs={"polymarket": {"priority": 1, "maxBots": 3}, "kalshi": {"priority": 2, "maxBots": 3}},
        )
        status = await manager.apply_updates({"kalshi": update("kalshi"), "polymarket": update("polymarket")})
        assert status["admittedMarkets"] == 3
        assert status["venues"]["polymarket"]["admittedMarkets"] == 3
        assert status["venues"]["kalshi"]["admittedMarkets"] == 0

    asyncio.run(run())


def test_status_snapshot_aggregates_all_venue_monitoring():
    class MonitoringManager(_FakeManager):
        def __init__(self, count, pnl, requests):
            super().__init__()
            self.count, self.pnl, self.requests = count, pnl, requests

        def status_snapshot(self):
            return {
                "counts": {"configuredBots": self.count, "activeBots": self.count},
                "bots": [], "workers": [], "clients": [],
                "portfolio": {"items": []},
                "pnl": {"fills": self.count, "feesCents": 0, "realizedCents": self.pnl, "unrealizedCents": 0, "totalCents": self.pnl},
                "apiActivity": {"rest": {"total": self.requests, "successes": self.requests, "errors": 0}},
            }

    pnl = 125.5
    manager = MultiVenueBotManager(
        {"kalshi": MonitoringManager(2, pnl, 7), "polymarket": MonitoringManager(3, -25.5, 11)},
        global_max_bots=10,
        venue_configs={"kalshi": {}, "polymarket": {}},
    )
    snapshot = manager.status_snapshot()
    assert snapshot["monitoring"]["activeVenues"] == ["kalshi", "polymarket"]
    assert snapshot["monitoring"]["botsRunning"] == 5
    assert snapshot["monitoring"]["pnl"]["totalCents"] == 100
    assert snapshot["monitoring"]["apiActivity"]["rest"]["total"] == 18
