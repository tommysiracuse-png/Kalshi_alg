from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

from clients.models import AccountOrder
from fleet_post_stop import main


class CleanupClient:
    def __init__(self) -> None:
        self.canceled: list[str] = []
        self.bot = AccountOrder("bot", "MKT", client_order_id="mm:yes:1", status="resting")
        self.manual = AccountOrder("manual", "MKT", client_order_id="operator", status="resting")

    def list_account_orders(self, _query):
        return ([self.bot] if not self.canceled else []) + [self.manual]

    def cancel_order(self, *, order_id: str):
        self.canceled.append(order_id)

    async def close(self) -> None:
        await asyncio.sleep(0)


def test_post_stop_cleanup_writes_verified_receipt_and_preserves_manual_orders(
    tmp_path: Path, monkeypatch,
):
    runtime = tmp_path / "runtime"
    client = CleanupClient()
    monkeypatch.setenv("KALSHI_RUNTIME_DIR", str(runtime))
    monkeypatch.setenv("KALSHI_SESSION_STORE", str(tmp_path / "sessions"))
    monkeypatch.setenv("KALSHI_API_KEY_ID", "key")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(tmp_path / "key.pem"))

    with patch("fleet_post_stop.KalshiApiClient", return_value=client):
        assert main() == 0

    receipt = json.loads((runtime / "shutdown_cleanup.json").read_text())
    assert receipt["state"] == "verified"
    assert receipt["ordersVerifiedAbsent"] is True
    assert client.canceled == ["bot"]
