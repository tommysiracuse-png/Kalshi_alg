import asyncio
import time
from dataclasses import replace

from clients.models import (
    AccountBalance,
    AccountFill,
    AccountLimits,
    AccountOrder,
    AccountPosition,
    Market,
    RateLimitBucket,
)
from portfolio_monitor import PortfolioMonitor, PortfolioMonitorConfig


class FakePortfolioClient:
    def __init__(self):
        self.fail = False
        self.limits_calls = 0
        self.now_ms = int(time.time() * 1000)

    def get_account_balance(self):
        if self.fail:
            raise RuntimeError("exchange unavailable")
        return AccountBalance(1_000_000, 50_000, 1_000)

    def get_account_limits(self):
        self.limits_calls += 1
        return AccountLimits("advanced", RateLimitBucket(20, 40), RateLimitBucket(10, 20))

    def list_account_positions(self, query):
        return [
            AccountPosition("YES", 200, 20_000, 8_000, 1_000, 100, 1, 2_000),
            AccountPosition("NO", -100, 10_000, 3_000, 0, 0, 0, 2_000),
        ]

    def list_account_orders(self, query):
        if query.status == "resting":
            return [AccountOrder("o1", "YES", "yes", status="resting", price_units=4_000, fill_count_units=100, remaining_count_units=200, initial_count_units=300, fill_cost_units=4_000, created_at_ms=self.now_ms - 10_000)]
        return [AccountOrder("o1", "YES", "yes", created_at_ms=self.now_ms - 10_000)]

    def list_account_fills(self, query):
        return [AccountFill("f1", "t1", "o1", "YES", "yes", 100, 4_000, created_at_ms=self.now_ms - 7_500)]

    def list_markets(self, query):
        return [
            Market("YES", "Yes market", series_id="SER", yes_bid_units=5_000, yes_ask_units=5_200, no_bid_units=4_800, no_ask_units=5_000, last_price_units=5_100, market_url="https://kalshi.com/markets/ser"),
            Market("NO", "No market", series_id="SER", yes_bid_units=6_800, yes_ask_units=7_000, no_bid_units=3_000, no_ask_units=3_200, last_price_units=6_900),
        ]

    def activity_snapshot(self):
        return {"rest": {"total": 6}, "stream": {}}


def test_portfolio_calculations_and_last_good_failure():
    client = FakePortfolioClient()
    monitor = PortfolioMonitor(client, PortfolioMonitorConfig(subaccount_number=2))
    async def scenario():
        assert await monitor.refresh() is True
        snapshot = monitor.status_snapshot()
        assert snapshot["summary"]["availableCashUnits"] == 1_000_000
        assert snapshot["summary"]["apiTier"] == "advanced"
        positions = {item["marketId"]: item for item in snapshot["positions"]}
        assert positions["YES"]["unrealizedPnlUnits"] == 2_000
        assert positions["YES"]["marketUnrealizedPnlUnits"] == 2_200
        assert positions["YES"]["marketTotalPnlUnits"] == 3_100
        assert positions["NO"]["lastPriceUnits"] == 3_100
        assert positions["NO"]["unrealizedPnlUnits"] == 0
        assert positions["NO"]["marketUnrealizedPnlUnits"] == 100
        assert snapshot["orders"]["summary"]["openMarketValueUnits"] == 8_000
        assert snapshot["orders"]["summary"]["averageFirstFillTimeMs"] == 2_500
        assert snapshot["orders"]["items"][0]["averageFillPriceUnits"] == 4_000

        client.fail = True
        assert await monitor.refresh() is False
        failed = monitor.status_snapshot()
        assert failed["available"] is True
        assert failed["stale"] is True
        assert failed["summary"]["availableCashUnits"] == 1_000_000
        assert "exchange unavailable" in failed["lastError"]

    asyncio.run(scenario())


def test_missing_liquidation_bid_does_not_become_zero():
    client = FakePortfolioClient()
    original = client.list_markets
    client.list_markets = lambda query: [replace(item, yes_bid_units=None, no_ask_units=None) if item.market_id == "YES" else item for item in original(query)]
    monitor = PortfolioMonitor(client, PortfolioMonitorConfig())
    asyncio.run(monitor.refresh())
    snapshot = monitor.status_snapshot()
    yes = next(item for item in snapshot["positions"] if item["marketId"] == "YES")
    assert yes["unrealizedPnlUnits"] is None
    assert snapshot["summary"]["unrealizedPnlUnits"] is None


def test_refresh_due_and_tier_refresh_cadence():
    client = FakePortfolioClient()
    monitor = PortfolioMonitor(
        client,
        PortfolioMonitorConfig(refresh_interval_seconds=15, tier_refresh_seconds=3_600),
    )

    async def scenario():
        assert await monitor.refresh()
        assert client.limits_calls == 1
        assert monitor.due((monitor._completed_at_ms or 0) + 14_999) is False
        assert monitor.due((monitor._completed_at_ms or 0) + 15_000) is True
        assert await monitor.refresh()
        assert client.limits_calls == 1
        monitor._limits_updated_at_ms = (monitor._limits_updated_at_ms or 0) - 3_600_000
        assert await monitor.refresh()
        assert client.limits_calls == 2

    asyncio.run(scenario())
