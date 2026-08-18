import csv
import json
import os
import sqlite3
import tempfile
from pathlib import Path

os.environ["KALSHI_UI_INTERNAL_TOKEN"] = "test-token"

import httpx
import pytest

import ui_api.app as app_module
from clients.models import AccountFill, AccountOrder
from ui_api.config import Settings
from ui_api.store import OperationsStore
from top_of_book_bot import TelemetryStore


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


@pytest.mark.anyio
async def test_run_market_activity_contract_limits_and_path_validation():
    with tempfile.TemporaryDirectory() as temporary:
        operations = fixture_store(Path(temporary))
        run = operations.sessions.prepare_run()
        database = Path(run["artifactPath"]) / "markets" / "TEST-1" / "telemetry.sqlite3"
        telemetry = TelemetryStore(str(database), enabled=True)
        telemetry.record_market_metadata(
            ticker="TEST-1", title="Test market", series_ticker="TEST", event_ticker="TEST-EVENT",
            market_url="https://kalshi.com/markets/test/test-market/test-event",
        )
        telemetry.start_order_revision(
            revision_key="rejected", action="create", side="yes", client_order_id="client",
            order_id=None, placed_at_ms=2_000_000, size_units=100, price_units=4_000,
            book_bid_units=3_900, book_ask_units=4_100, book_mid_units=4_000,
        )
        telemetry.reject_order_revision("rejected", "rejected")
        app_module.store = operations
        headers = {"x-internal-token": "test-token"}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app, raise_app_exceptions=False), base_url="http://test") as client:
            assert (await client.get(f"/api/v1/runs/{run['id']}/markets")).status_code == 401
            markets = await client.get(f"/api/v1/runs/{run['id']}/markets", headers=headers)
            activity = await client.get(f"/api/v1/runs/{run['id']}/markets/TEST-1/activity?fill_limit=25&order_limit=25", headers=headers)
            too_small = await client.get(f"/api/v1/runs/{run['id']}/markets/TEST-1/activity?fill_limit=0", headers=headers)
            too_large = await client.get(f"/api/v1/runs/{run['id']}/markets/TEST-1/activity?order_limit=501", headers=headers)
            traversal = await client.get(f"/api/v1/runs/{run['id']}/markets/BAD%5C..%5CTEST-1/activity", headers=headers)
        assert markets.status_code == 200
        assert markets.json()["items"][0]["ticker"] == "TEST-1"
        assert activity.status_code == 200
        assert activity.json()["orders"]["items"][0]["endedState"] == "Rejected"
        assert too_small.status_code == 422
        assert too_large.status_code == 422
        assert traversal.status_code == 404


@pytest.mark.anyio
async def test_metrics_heartbeat_is_compact_authenticated_and_shared(monkeypatch: pytest.MonkeyPatch):
    with tempfile.TemporaryDirectory() as temporary:
        operations = fixture_store(Path(temporary))
        run = operations.sessions.claim_run()
        operations.sessions.record_metrics(run["id"], {
            "runtimeMs": 12_345, "orders": 2, "fills": 1, "totalCents": 50,
            "apiCalls": 9, "apiErrors": 1,
            "markets": {"TEST-1": {"orders": 2, "orderPlacementsAttempted": 1, "fills": 1}},
        }, sample=False)
        calls = 0
        original = operations.sessions.metrics_heartbeat

        def counted():
            nonlocal calls
            calls += 1
            return original()

        monkeypatch.setattr(operations.sessions, "metrics_heartbeat", counted)
        app_module.store = operations
        headers = {"x-internal-token": "test-token"}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app), base_url="http://test") as client:
            assert (await client.get("/api/v1/metrics/heartbeat")).status_code == 401
            first = await client.get("/api/v1/metrics/heartbeat", headers=headers)
            second = await client.get("/api/v1/metrics/heartbeat", headers=headers)
        payload = first.json()
        assert first.status_code == second.status_code == 200
        assert calls == 1
        assert payload["activeRun"]["summary"]["orders"] == 2
        assert payload["activeRun"]["activityRevision"]
        assert payload["source"]["available"] is True
        assert len(first.content) < 2_000


@pytest.mark.anyio
async def test_sse_source_failure_does_not_end_other_producers(monkeypatch: pytest.MonkeyPatch):
    class ConnectedRequest:
        async def is_disconnected(self):
            return False

    def broken_overview():
        raise sqlite3.OperationalError("overview database unavailable")

    monkeypatch.setattr(app_module.store, "overview", broken_overview)
    monkeypatch.setattr(app_module.store, "monitoring", lambda: {"healthy": True})
    stream = app_module.event_stream(ConnectedRequest())
    first = await anext(stream)
    second = await anext(stream)
    await stream.aclose()
    assert "event: source_error" in first
    assert '"sourceName":"overview"' in first
    assert "event: monitoring" in second
    assert '"healthy":true' in second
