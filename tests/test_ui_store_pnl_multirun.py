"""P&L windows union the telemetry of every run that overlaps the window, not only the active run."""

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.test_ui_store_pnl_telemetry import WORKER_00_FILLS, WORKER_00_MARKOUTS, WORKER_01_FILLS, write_shard
from ui_api.config import Settings
from ui_api.store import OperationsStore

NOW_MS = int(time.time() * 1000)
HOUR_MS = 3_600_000


class MultiRunPnlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for name in ("runtime", "logs", "watchdog_state"):
            (self.root / name).mkdir()
        self.store = OperationsStore(Settings(self.root, self.root / "runtime", self.root / "logs", self.root / "watchdog_state", "test.service"))
        self.store.PNL_CACHE_TTL_MS = 0
        base = self.root / "session_data" / "artifacts" / "session"
        self.old_run = base / "run-old"      # ended 90 minutes ago, holds today's fills
        self.active_run = base / "run-active"  # started 20 minutes ago, no fills yet
        self.stale_run = base / "run-stale"   # ended 3 days ago
        write_shard(self.old_run / "shards" / "worker-00" / "telemetry.sqlite3", WORKER_00_FILLS, WORKER_00_MARKOUTS)
        write_shard(self.old_run / "shards" / "worker-01" / "telemetry.sqlite3", WORKER_01_FILLS)
        write_shard(self.active_run / "shards" / "worker-00" / "telemetry.sqlite3", [])
        write_shard(self.stale_run / "shards" / "worker-00" / "telemetry.sqlite3", [
            ("s-1", NOW_MS - 3 * 24 * HOUR_MS, "S", "yes", "t9", "o9", 5000, 100, 0, 0, 0, 100, 5000, 4900, 4900, 0),
        ])
        runs = [
            {"id": "run-active", "artifactPath": str(self.active_run), "createdAt": NOW_MS - 20 * 60_000, "startedAt": NOW_MS - 20 * 60_000, "endedAt": None, "status": "running"},
            {"id": "run-old", "artifactPath": str(self.old_run), "createdAt": NOW_MS - 4 * HOUR_MS, "startedAt": NOW_MS - 4 * HOUR_MS, "endedAt": NOW_MS - 90 * 60_000, "status": "stopped"},
            {"id": "run-stale", "artifactPath": str(self.stale_run), "createdAt": NOW_MS - 3 * 24 * HOUR_MS - HOUR_MS, "startedAt": NOW_MS - 3 * 24 * HOUR_MS - HOUR_MS, "endedAt": NOW_MS - 3 * 24 * HOUR_MS, "status": "stopped"},
        ]
        self.patches = [
            patch.object(self.store.sessions, "active_run", return_value=runs[0]),
            patch.object(self.store.sessions, "list_runs", return_value=runs),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in self.patches:
            item.stop()
        self.temp.cleanup()

    def test_default_scope_is_the_current_session_only(self):
        # The Overview reports this session; earlier runs' fills must not leak in.
        result = self.store.pnl("24h")
        self.assertEqual(result["scope"], "session")
        self.assertEqual(result["source"]["runs"], 1)
        self.assertEqual(result["totals"]["fills"], 0)

    def test_all_scope_24h_unions_the_finished_run_with_the_active_one(self):
        result = self.store.pnl("24h", scope="all")
        self.assertEqual(result["source"]["kind"], "shard_telemetry")
        self.assertEqual(result["source"]["runs"], 2)
        self.assertEqual(result["source"]["shards"], 3)
        self.assertEqual(result["totals"]["fills"], 3)
        self.assertEqual({item["ticker"] for item in result["tickers"]}, {"A", "B"})

    def test_all_scope_short_window_skips_runs_that_ended_before_it_opened(self):
        result = self.store.pnl("1h", scope="all")
        self.assertEqual(result["source"]["runs"], 1)  # the finished run ended 90 min ago
        self.assertEqual(result["totals"]["fills"], 0)

    def test_all_scope_all_window_includes_every_run(self):
        result = self.store.pnl("all", scope="all")
        self.assertEqual(result["source"]["runs"], 3)
        self.assertEqual(result["totals"]["fills"], 4)
        self.assertIn("S", {item["ticker"] for item in result["tickers"]})

    def test_invalid_scope_is_rejected(self):
        with self.assertRaises(ValueError):
            self.store.pnl("24h", scope="everything")


if __name__ == "__main__":
    unittest.main()
