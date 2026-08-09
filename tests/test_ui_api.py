import csv
import json
import os
import tempfile
from pathlib import Path

os.environ["KALSHI_UI_INTERNAL_TOKEN"] = "test-token"

import httpx
import pytest

import ui_api.app as app_module
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
    (root / "runtime" / "launcher_status.json").write_text(json.dumps({"launcher": {"lifecycle": "running", "heartbeatAt": 9999999999999}, "bots": [], "counts": {}}))
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
