from __future__ import annotations

import asyncio

from clients.models import AccountPosition, Market, market_key
from screeners.base_screener import BaseScreener, ScreeningResult
from core.fleet_models import ScreenerPick
from portfolio.base_portfolio import BasePortfolio, PortfolioSnapshot


class FakeScreener(BaseScreener):
    venue = "fake"

    def screen(self, settings):
        return ScreeningResult(
            venue=self.venue,
            picks=(ScreenerPick("M-1", "Market", 100, 100, venue=self.venue),),
            scanned_markets=2,
            selected_markets=1,
            metadata={"source": "test"},
        )


class FakePortfolio(BasePortfolio):
    venue = "fake"

    def due(self, now_ms=None):
        return self._typed_latest is None

    async def refresh(self):
        self._typed_latest = PortfolioSnapshot(
            venue=self.venue,
            generated_at_ms=1,
            available_cash_units=10_000,
            portfolio_value_units=10_000,
            midpoint_position_value_units=0,
            liquidation_value_units=0,
            balance_complete=True,
            portfolio_value_complete=True,
            liquidation_value_complete=True,
        )
        return True

    def status_snapshot(self):
        return self._typed_latest.to_dict() if self._typed_latest else {"available": False}


class DummyClient:
    def activity_snapshot(self):
        return {"rest": {}, "stream": {}}


def test_market_identity_includes_venue():
    assert market_key("kalshi", "same") != market_key("fake", "same")
    assert Market("same", venue="fake").normalized_market_key == ("fake", "same")


def test_base_screener_reconciles_typed_result():
    update = asyncio.run(FakeScreener().refresh({}))
    assert update is not None
    assert update.added == ("M-1",)
    assert update.picks[0].venue == "fake"


def test_portfolio_snapshot_is_typed_and_serializable():
    portfolio = FakePortfolio(DummyClient())
    assert asyncio.run(portfolio.refresh())
    snapshot = portfolio.latest_snapshot()
    assert snapshot is not None
    assert snapshot.balance_complete
    assert snapshot.to_dict()["venue"] == "fake"

