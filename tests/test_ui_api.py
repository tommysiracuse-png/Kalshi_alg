import csv
import json
import os
import tempfile
from pathlib import Path

os.environ["KALSHI_UI_INTERNAL_TOKEN"] = "test-token"

import httpx
import pytest

import ui_api.app as app_module
from clients.models import AccountFill, AccountOrder
from ui_api.config import Settings
from ui_api.store import OperationsStore


def fixture_store(root: Path) -> OperationsStore:
    (root / "runtime").mkdir()
    (root / "logs").mkdir()
    (root / "watchdog_state").mkdir()
    with (root / "screener_export.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["Rank", "Ticker", "SearchText", "Best EV(c)"])
        writer.writeheader(); writer.writerow({"Rank": "1", "Ticker": "TEST-1", "SearchText": "Test market", "Best EV(c)": "2.5"})
    (root / "watchdog_state" / "TEST-1.json").write_text(json.dumps({"mode": "normal", "confidence": .9}))
    (root / "runtime" / "launcher_status.json").write_text(json.dumps({
        "schemaVersion": 2,
        "launcher": {"lifecycle": "running", "heartbeatAt": 9999999999999},
        "bots": [], "counts": {},
        "manager": {"running": True, "botsRunning": 1, "pnl": {"totalCents": 12.5}},
        "clients": [{
            "marketId": "TEST-1", "title": "Test market",
            "market": {"marketId": "TEST-1", "marketUrl": "https://kalshi.com/markets/test/test-market/test-event"},
            "apiActivity": {"rest": {"total": 4}},
        }],
        "screener": {"running": False, "generationId": 3, "picks": [{"marketId": "TEST-1"}]},
        "portfolio": {"available": True, "stale": False, "summary": {"availableCashUnits": 125000}, "positions": [], "orders": {"summary": {"openOrderCount": 0}, "items": []}, "warnings": []},
    }))
    return OperationsStore(Settings(root, root / "runtime", root / "logs", root / "watchdog_state", "test.service"))


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_auth_and_market_contract():
    with tempfile.TemporaryDirectory() as temporary:
        app_module.store = fixture_store(Path(temporary))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app), base_url="http://test") as client:
            assert (await client.get("/api/v1/markets")).status_code == 401
            response = await client.get("/api/v1/markets", headers={"x-internal-token": "test-token"})
        assert response.status_code == 200
        body = response.json()
        assert isinstance(body["generatedAt"], int)
        assert body["items"][0]["ticker"] == "TEST-1"
        assert body["items"][0]["expectedEdgeCents"] == 2.5


@pytest.mark.anyio
async def test_invalid_ticker_uses_error_envelope():
    with tempfile.TemporaryDirectory() as temporary:
        app_module.store = fixture_store(Path(temporary))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app, raise_app_exceptions=False), base_url="http://test") as client:
            response = await client.get("/api/v1/markets/not-a-market", headers={"x-internal-token": "test-token"})
        assert response.status_code == 404
        assert response.json()["code"] == "not_found"
        assert response.json()["requestId"]


@pytest.mark.anyio
async def test_monitoring_contract_and_client_detail():
    with tempfile.TemporaryDirectory() as temporary:
        app_module.store = fixture_store(Path(temporary))
        headers = {"x-internal-token": "test-token"}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app), base_url="http://test") as client:
            snapshot = await client.get("/api/v1/monitoring", headers=headers)
            detail = await client.get("/api/v1/monitoring/clients/TEST-1", headers=headers)
        assert snapshot.status_code == 200
        assert snapshot.json()["manager"]["botsRunning"] == 1
        assert snapshot.json()["screener"]["generationId"] == 3
        assert detail.json()["client"]["apiActivity"]["rest"]["total"] == 4


@pytest.mark.anyio
async def test_portfolio_contract_is_authenticated_and_uses_launcher_snapshot():
    with tempfile.TemporaryDirectory() as temporary:
        app_module.store = fixture_store(Path(temporary))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app), base_url="http://test") as client:
            unauthorized = await client.get("/api/v1/portfolio")
            response = await client.get("/api/v1/portfolio", headers={"x-internal-token": "test-token"})
        assert unauthorized.status_code == 401
        assert response.status_code == 200
        assert response.json()["summary"]["availableCashUnits"] == 125000


@pytest.mark.anyio
async def test_portfolio_analytics_endpoints_are_storage_backed():
    with tempfile.TemporaryDirectory() as temporary:
        operations = fixture_store(Path(temporary))
        timestamp = 2_000_000
        order = AccountOrder(
            "order-1", "TEST-1", "yes", status="resting", price_units=4_000,
            fill_count_units=100, remaining_count_units=100, initial_count_units=200,
            created_at_ms=timestamp - 2_000, updated_at_ms=timestamp - 500,
        )
        fill = AccountFill(
            "fill-1", "trade-1", "order-1", "TEST-1", "yes", 100, 4_000,
            fee_units=100, created_at_ms=timestamp - 1_000,
        )
        operations.portfolio_analytics.record_refresh({
            "generatedAtMs": timestamp,
            "warnings": [],
            "summary": {
                "availableCashUnits": 100_000, "midpointPositionValueUnits": 5_100,
                "totalPortfolioValueUnits": 105_100, "positionsLiquidationValueUnits": 5_000,
                "apiTier": "advanced", "positionCount": 1,
            },
            "positions": [{
                "marketId": "TEST-1", "ticker": "TEST-1", "title": "Test market", "side": "yes",
                "contractsUnits": 100, "bidPriceUnits": 5_000, "askPriceUnits": 5_200,
                "midPriceUnits": 5_100, "costBasisUnits": 4_000,
                "averageCostPriceUnits": 4_000, "liquidationValueUnits": 5_000,
                "unrealizedValueUnits": 5_100, "openOrderCount": 1,
            }],
            "orders": {"items": [{
                "ticker": "TEST-1", "marketId": "TEST-1", "title": "Test market", "side": "yes", "midPriceUnits": 5_100,
            }]},
        }, [order], [fill], [order])
        active_run = operations.sessions.claim_run()
        operations.sessions.record_metrics(active_run["id"], {
            "markets": {"TEST-1": {"orderPlacementsAttempted": 9}},
        }, sample=False)
        app_module.store = operations
        headers = {"x-internal-token": "test-token"}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app), base_url="http://test") as client:
            assert (await client.get("/api/v1/portfolio/summary")).status_code == 401
            summary = await client.get("/api/v1/portfolio/summary", headers=headers)
            positions = await client.get("/api/v1/portfolio/positions", headers=headers)
            fills = await client.get("/api/v1/portfolio/positions/TEST-1/fills?limit=1", headers=headers)
            orders = await client.get("/api/v1/portfolio/orders", headers=headers)
        assert summary.json()["summary"]["totalPortfolioValueUnits"] == 105_100
        assert summary.json()["coverage"]["partial"] is True
        assert positions.json()["items"][0]["totalFillCount"] == 1
        assert positions.json()["items"][0]["marketUrl"] == "https://kalshi.com/markets/test/test-market/test-event"
        assert fills.json()["items"][0]["costInPositionUnits"] == 4_100
        assert orders.json()["summary"]["totalOpenOrders"] == 1
        assert orders.json()["summary"]["ordersAttempted"] == 9
        assert orders.json()["items"][0]["ticker"] == "TEST-1"
        assert orders.json()["items"][0]["ordersAttempted"] == 9
        assert orders.json()["items"][0]["marketUrl"] == "https://kalshi.com/markets/test/test-market/test-event"


@pytest.mark.anyio
async def test_session_crud_and_metrics_are_authenticated():
    with tempfile.TemporaryDirectory() as temporary:
        app_module.store = fixture_store(Path(temporary))
        headers = {"x-internal-token": "test-token"}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app), base_url="http://test") as client:
            assert (await client.get("/api/v1/sessions")).status_code == 401
            listing = await client.get("/api/v1/sessions", headers=headers)
            default = listing.json()["items"][0]
            created = await client.post("/api/v1/sessions", headers=headers, json={"name": "Test config", "configuration": default["configuration"]})
            metrics = await client.get("/api/v1/metrics", headers=headers)
        assert created.status_code == 201
        assert created.json()["item"]["name"] == "Test config"
        assert metrics.status_code == 200
        assert metrics.json()["summary"]["timesRun"] == 0
