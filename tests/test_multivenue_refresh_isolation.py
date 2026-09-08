from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from apps.launcher import Launcher
from core.fleet_models import ScreenerEvent, ScreenerPick, ScreenerUpdate
from fleet_runtime.multi_manager import MultiVenueBotManager


class _FakeVenueManager:
    def __init__(self):
        self.current_picks = {}
        self.bots = {}
        self.events = asyncio.Queue()
        self.applied_at = []

    async def apply_update(self, update):
        self.current_picks = {pick.market_id: pick for pick in update.picks}
        self.applied_at.append(time.monotonic())

    def status_snapshot(self):
        count = len(self.current_picks)
        return {"counts": {"configuredBots": count, "activeBots": count}, "bots": [], "workers": [], "clients": []}


class _FakeScreener:
    def __init__(self, venue: str, delay: float):
        self.venue = venue
        self.delay = delay
        self.settings = {"max_markets_to_scan": 10}
        self.events = asyncio.Queue()
        self.calls = []

    async def refresh(self, current_picks, *, reason, exchange_positions=None, traded_tickers=None):
        del current_picks, exchange_positions, traded_tickers
        self.calls.append(reason)
        await asyncio.sleep(self.delay)
        pick = ScreenerPick(f"{self.venue}-market", self.venue, 1, 1, venue=self.venue)
        update = ScreenerUpdate(1, int(time.time() * 1000), reason, (pick,), (pick.market_id,), (), (), ())
        await self.events.put(ScreenerEvent(True, reason, update.generated_at_ms, update=update))
        return update

    def last_run_metrics(self):
        return {"status": "succeeded", "startedAtMs": int(time.time() * 1000), "durationMs": 1}

    def status_snapshot(self):
        return {"venue": self.venue}


def test_refreshes_are_isolated_and_apply_as_each_venue_finishes():
    async def run():
        launcher = Launcher.__new__(Launcher)
        launcher.enabled_venues = ("kalshi", "polymarket")
        launcher.venue = "kalshi"
        launcher.screeners = {
            "kalshi": _FakeScreener("kalshi", 0.01),
            "polymarket": _FakeScreener("polymarket", 0.08),
        }
        launcher.managers = {name: _FakeVenueManager() for name in launcher.enabled_venues}
        launcher.clients = {name: object() for name in launcher.enabled_venues}
        launcher.client_configs = {name: object() for name in launcher.enabled_venues}
        launcher._pending_updates = {}
        launcher.shutdown_requested = asyncio.Event()
        launcher.session_store = None
        launcher.run_id = None
        launcher.run_artifact_path = None
        launcher.last_error = None
        launcher.next_refresh_at = None
        launcher.arguments = SimpleNamespace(refresh_interval_seconds=60)
        launcher.publish_status = lambda *_args: None
        launcher.manager = MultiVenueBotManager(
            launcher.managers,
            global_max_bots=2,
            venue_configs={
                "kalshi": {"priority": 2, "maxBots": 2},
                "polymarket": {"priority": 1, "maxBots": 2},
            },
        )
        started = time.monotonic()
        assert await launcher.refresh("test") is True

        kalshi_applied = launcher.managers["kalshi"].applied_at
        polymarket_applied = launcher.managers["polymarket"].applied_at
        assert kalshi_applied and polymarket_applied
        assert kalshi_applied[0] - started < 0.06
        assert kalshi_applied[0] < polymarket_applied[0]
        assert set(launcher.manager.current_picks) == {
            ("kalshi", "kalshi-market"), ("polymarket", "polymarket-market"),
        }
        assert launcher.screeners["kalshi"].calls == ["test"]
        assert launcher.screeners["polymarket"].calls == ["test"]

    asyncio.run(run())
