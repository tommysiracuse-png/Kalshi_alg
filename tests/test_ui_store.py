import csv
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ui_api.config import Settings
from ui_api.store import OperationsStore


class OperationsStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "runtime").mkdir()
        (self.root / "logs").mkdir()
        (self.root / "watchdog_state").mkdir()
        with (self.root / "screener_export.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["Rank", "Ticker", "SearchText", "Best EV(c)"])
            writer.writeheader(); writer.writerow({"Rank": "1", "Ticker": "TEST-1", "SearchText": "Test market", "Best EV(c)": "4.2"})
        (self.root / "watchdog_state" / "TEST-1.json").write_text(json.dumps({"mode": "normal", "confidence": .8}))
        (self.root / "runtime" / "launcher_status.json").write_text(json.dumps({"launcher": {"lifecycle": "running", "heartbeatAt": 9999999999999}, "bots": [], "counts": {}}))
        self.store = OperationsStore(Settings(self.root, self.root / "runtime", self.root / "logs", self.root / "watchdog_state", "test.service"))

    def tearDown(self):
        self.temp.cleanup()

    def test_market_is_normalized(self):
        market = self.store.markets()[0]
        self.assertEqual(market["ticker"], "TEST-1")
        self.assertEqual(market["expectedEdgeCents"], 4.2)
        self.assertEqual(market["watchdogMode"], "normal")

    def test_unknown_ticker_is_rejected(self):
        with self.assertRaises(KeyError):
            self.store.market_detail("../../secret")

    def test_malformed_watchdog_is_partial_not_fatal(self):
        (self.root / "watchdog_state" / "TEST-1.json").write_text("{")
        market = self.store.markets()[0]
        self.assertEqual(market["watchdogMode"], "unknown")

    def test_start_control_rejects_service_that_exits_immediately(self):
        results = [
            SimpleNamespace(returncode=0, stdout="", stderr=""),
            SimpleNamespace(returncode=3, stdout="failed\n", stderr=""),
        ]
        with patch("ui_api.store.subprocess.run", side_effect=results):
            with self.assertRaisesRegex(RuntimeError, "exited during startup"):
                self.store.systemd("start")

    def test_stop_requires_current_verified_order_cleanup(self):
        results = [
            SimpleNamespace(returncode=0, stdout="active\n", stderr=""),
            SimpleNamespace(returncode=0, stdout="", stderr=""),
            SimpleNamespace(returncode=3, stdout="inactive\n", stderr=""),
        ]
        with patch("ui_api.store.subprocess.run", side_effect=results):
            with self.assertRaisesRegex(RuntimeError, "order cancellation was not verified"):
                self.store.systemd("stop")

    def test_stop_returns_verified_cleanup_result(self):
        completed_at = int(time.time() * 1000)
        (self.root / "runtime" / "launcher_status.json").write_text(json.dumps({
            "launcher": {"lifecycle": "stopped", "heartbeatAt": completed_at},
            "manager": {"shutdownCleanup": {
                "state": "verified",
                "completedAtMs": completed_at,
                "canceledOrders": 4,
                "ordersVerifiedAbsent": True,
                "error": None,
            }},
            "bots": [],
            "counts": {},
        }))
        results = [
            SimpleNamespace(returncode=0, stdout="active\n", stderr=""),
            SimpleNamespace(returncode=0, stdout="", stderr=""),
            SimpleNamespace(returncode=3, stdout="inactive\n", stderr=""),
        ]
        with patch("ui_api.store.subprocess.run", side_effect=results):
            result = self.store.systemd("stop")
        self.assertTrue(result["shutdownCleanup"]["ordersVerifiedAbsent"])
        self.assertEqual(result["shutdownCleanup"]["canceledOrders"], 4)


if __name__ == "__main__":
    unittest.main()
