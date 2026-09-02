import csv
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from pnl_core import find_telemetry_databases, load_telemetry_fills
from portfolio_analytics import PortfolioAnalyticsStore
from ui_api.config import Settings
from ui_api.store import OperationsStore


FILLS_DDL = """
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT, fill_key TEXT NOT NULL UNIQUE, ts_ms INTEGER NOT NULL,
    ticker TEXT NOT NULL, side TEXT NOT NULL, trade_id TEXT, order_id TEXT, price_units INTEGER,
    size_units INTEGER, fee_units INTEGER, is_taker INTEGER, inventory_before_units INTEGER,
    inventory_after_units INTEGER, fair_yes_before_units INTEGER, best_yes_bid_units INTEGER,
    best_no_bid_units INTEGER, queue_ahead_units INTEGER
);
CREATE TABLE IF NOT EXISTS markouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, fill_key TEXT NOT NULL, ts_ms INTEGER NOT NULL,
    ticker TEXT NOT NULL, side TEXT NOT NULL, horizon_ms INTEGER NOT NULL, fill_price_units INTEGER NOT NULL,
    future_mid_yes_units INTEGER, future_fair_yes_units INTEGER, adverse_units INTEGER, bucket_key TEXT
);
CREATE TABLE IF NOT EXISTS order_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, revision_key TEXT NOT NULL UNIQUE, action TEXT NOT NULL,
    order_id TEXT, client_order_id TEXT, side TEXT NOT NULL, placed_at_ms INTEGER NOT NULL, ended_at_ms INTEGER,
    size_units INTEGER NOT NULL DEFAULT 0, filled_units INTEGER NOT NULL DEFAULT 0, price_units INTEGER,
    book_bid_units INTEGER, book_ask_units INTEGER, book_mid_units INTEGER, ended_state TEXT NOT NULL,
    error TEXT, ticker TEXT NOT NULL DEFAULT ''
);
"""

NOW_MS = int(time.time() * 1000)
HOUR_MS = 3_600_000


def write_shard(path: Path, fills, markouts=(), order_revisions=()):
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript(FILLS_DDL)
        for fill in fills:
            db.execute(
                """INSERT OR IGNORE INTO fills(fill_key, ts_ms, ticker, side, trade_id, order_id, price_units, size_units,
                   fee_units, is_taker, inventory_before_units, inventory_after_units, fair_yes_before_units,
                   best_yes_bid_units, best_no_bid_units, queue_ahead_units) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                fill,
            )
        for markout in markouts:
            db.execute(
                """INSERT INTO markouts(fill_key, ts_ms, ticker, side, horizon_ms, fill_price_units, future_mid_yes_units,
                   future_fair_yes_units, adverse_units, bucket_key) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                markout,
            )
        for revision in order_revisions:
            db.execute(
                """INSERT INTO order_revisions(revision_key, action, order_id, client_order_id, side, placed_at_ms,
                   size_units, price_units, ended_state, ticker) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                revision,
            )


# Ticker A mirrors tests/test_pnl_core.py: buy 2 YES @ 40c (fee 1c), then 1 NO @ 45c (fee 1c), mark 55c.
# Ticker B: 3 NO @ 65c taker fee 4.95c, no markout row -> falls back to the pre-fill fair value (33c).
WORKER_00_FILLS = [
    ("a-1", NOW_MS - 2 * HOUR_MS, "A", "yes", "t1", "o1", 4000, 200, 100, 0, 0, 200, 4800, 3900, 5900, 0),
    ("a-2", NOW_MS - 30 * 60_000, "A", "no", "t2", "o2", 4500, 100, 100, 0, 200, 100, 5000, 5300, 4500, 0),
]
WORKER_00_MARKOUTS = [
    ("a-1", NOW_MS - 2 * HOUR_MS + 30_000, "A", "yes", 30_000, 4000, 5000, 5100, 0, "k"),
    ("a-2", NOW_MS - 30 * 60_000 + 5_000, "A", "no", 5_000, 4500, 9900, 9900, 0, "k"),
    ("a-2", NOW_MS - 30 * 60_000 + 30_000, "A", "no", 30_000, 4500, 5500, 5600, 0, "k"),
]
WORKER_01_FILLS = [
    ("b-1", NOW_MS - 10 * 60_000, "B", "no", "t3", "o3", 6500, 300, 495, 1, 0, -300, 3300, 3300, 6600, 0),
]

# Watchdog flatten exits, exactly as the fleet records them: the fill's side is the
# held side, price is that side's price, and only the inventory columns (and the
# wd: client order id of the originating order) reveal that it was a sell.
# Ticker W (YES holding): buy 4 YES @ 37c, then sell 4 YES @ 12c taker -> -100c - fees.
# Ticker V (NO holding): buy 3 NO @ 26c, then sell 3 NO @ 14c taker -> -36c - fees.
WORKER_02_FILLS = [
    ("w-1", NOW_MS - 50 * 60_000, "W", "yes", "t4", "o-w-buy", 3700, 400, 50, 0, 0, 400, 3800, 3700, 6200, 0),
    ("w-2", NOW_MS - 40 * 60_000, "W", "yes", "t5", "o-w-exit", 1200, 400, 50, 1, 400, 0, 1300, 1200, 8700, 0),
    ("v-1", NOW_MS - 45 * 60_000, "V", "no", "t6", "o-v-buy", 2600, 300, 30, 0, 0, -300, 7500, 7300, 2600, 0),
    ("v-2", NOW_MS - 35 * 60_000, "V", "no", "t7", "o-v-exit", 1400, 300, 30, 1, -300, 0, 8700, 8500, 1400, 0),
]
WORKER_02_MARKOUTS = [
    ("w-2", NOW_MS - 40 * 60_000 + 30_000, "W", "yes", 30_000, 1200, 1250, 1250, 0, "k"),
    ("v-2", NOW_MS - 35 * 60_000 + 30_000, "V", "no", 30_000, 1400, 8650, 8650, 0, "k"),
]
WORKER_02_REVISIONS = [
    ("r1", "create", "o-w-buy", "tob:yes:1", "yes", NOW_MS - 51 * 60_000, 400, 3700, "Filled", "W"),
    ("r2", "create", "o-w-exit", "wd:yes:1", "yes", NOW_MS - 40 * 60_000, 400, 1200, "Unknown", "W"),
    ("r3", "create", "o-v-buy", "tob:no:1", "no", NOW_MS - 46 * 60_000, 300, 2600, "Filled", "V"),
    ("r4", "create", "o-v-exit", "wd:no:1", "no", NOW_MS - 35 * 60_000, 300, 1400, "Unknown", "V"),
]


class TelemetryPnlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for name in ("runtime", "logs", "watchdog_state"):
            (self.root / name).mkdir()
        with (self.root / "screener_export.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["Rank", "Ticker", "SearchText", "Best EV(c)"])
            writer.writeheader()
            writer.writerow({"Rank": "1", "Ticker": "A", "SearchText": "Market A", "Best EV(c)": "1.0"})
        self.write_status([{"ticker": "A", "positionUnits": 100}, {"ticker": "B", "positionUnits": -300}])
        self.artifact = self.root / "session_data" / "artifacts" / "session" / "run"
        self.artifact.mkdir(parents=True)
        self.store = OperationsStore(Settings(self.root, self.root / "runtime", self.root / "logs", self.root / "watchdog_state", "test.service"))
        self.store.PNL_CACHE_TTL_MS = 0
        self.active_patch = patch.object(self.store.sessions, "active_run", return_value={"id": "run", "artifactPath": str(self.artifact), "startedAt": NOW_MS - 3 * HOUR_MS})
        self.active_patch.start()

    def tearDown(self):
        self.active_patch.stop()
        self.temp.cleanup()

    def write_status(self, bots, portfolio=None):
        status = {"launcher": {"lifecycle": "running", "heartbeatAt": NOW_MS + 10 ** 9}, "bots": bots, "counts": {}}
        if portfolio is not None:
            status["portfolio"] = portfolio
        (self.root / "runtime" / "launcher_status.json").write_text(json.dumps(status))

    @staticmethod
    def exchange_snapshot(positions, *, available=True):
        rows = [{"marketId": ticker, "ticker": ticker, "side": side, "contractsUnits": units} for ticker, side, units in positions]
        return {
            "schemaVersion": 1, "available": available, "stale": False, "generatedAtMs": NOW_MS - 5_000,
            "lastSuccessAtMs": NOW_MS - 4_000, "summary": {"availableCashUnits": 1_000_000}, "positions": rows,
            "orders": {"summary": {}, "items": []}, "warnings": [],
        }

    def write_shards(self):
        write_shard(self.artifact / "shards" / "worker-00" / "telemetry.sqlite3", WORKER_00_FILLS, WORKER_00_MARKOUTS)
        write_shard(self.artifact / "shards" / "worker-01" / "telemetry.sqlite3", WORKER_01_FILLS)

    def write_exit_shard(self):
        write_shard(self.artifact / "shards" / "worker-02" / "telemetry.sqlite3", WORKER_02_FILLS, WORKER_02_MARKOUTS, WORKER_02_REVISIONS)

    def overview(self):
        with patch("ui_api.store.subprocess.run", side_effect=FileNotFoundError("systemctl")):
            return self.store.overview()

    def test_telemetry_fills_convert_units_exactly(self):
        self.write_shards()
        fills, warnings = load_telemetry_fills(self.artifact)
        self.assertEqual(warnings, [])
        self.assertEqual([item["ticker"] for item in fills], ["A", "A", "B"])
        first, second, third = fills
        self.assertEqual((first["price_c"], first["qty"], first["fee_c"], first["net_pos"]), (40.0, 2.0, 1.0, 2.0))
        self.assertEqual((first["fair_c"], first["mark_source"]), (50.0, "markout_30000"))
        self.assertEqual((first["action"], first["action_source"], first["net_pos_before"], first["fair_before_c"]), ("buy", "inventory", 0.0, 48.0))
        self.assertEqual((second["fair_c"], second["mark_source"]), (55.0, "markout_30000"))
        self.assertEqual((second["action"], second["net_pos_before"]), ("buy", 2.0))
        self.assertEqual((third["price_c"], third["qty"], third["fee_c"], third["net_pos"]), (65.0, 3.0, 4.95, -3.0))
        self.assertEqual((third["fair_c"], third["mark_source"]), (33.0, "fair_before_fill"))
        self.assertEqual(third["action"], "buy")
        self.assertEqual(third["ts_ms"], NOW_MS - 10 * 60_000)
        self.assertTrue(third["ts"].endswith("Z"))
        self.assertEqual(third["shard"], "worker-01")

    def test_pnl_uses_shard_telemetry_and_reports_dollars(self):
        self.write_shards()
        result = self.store.pnl()
        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["source"]["kind"], "shard_telemetry")
        self.assertEqual(result["source"]["shards"], 2)
        self.assertTrue(result["source"]["available"])
        rows = {item["ticker"]: item for item in result["tickers"]}
        self.assertEqual(set(rows), {"A", "B"})
        self.assertEqual(rows["A"]["fills"], 2)
        self.assertEqual(rows["A"]["sellFills"], 0)
        self.assertFalse(rows["A"]["basisEstimated"])
        self.assertEqual(rows["A"]["realizedCents"], 13.0)
        self.assertEqual(rows["A"]["unrealizedCents"], 15.0)
        self.assertEqual(rows["A"]["netPosition"], 1.0)
        self.assertEqual(rows["A"]["lastFairCents"], 55.0)
        self.assertEqual(rows["B"]["fills"], 1)
        self.assertEqual(rows["B"]["feesCents"], 4.95)
        self.assertEqual(rows["B"]["realizedCents"], -4.95)
        self.assertEqual(rows["B"]["unrealizedCents"], 6.0)
        self.assertEqual(rows["B"]["netPosition"], -3.0)
        totals = result["totals"]
        self.assertEqual(totals["fills"], 3)
        self.assertEqual(totals["sellFills"], 0)
        self.assertEqual(totals["basisEstimatedTickers"], 0)
        self.assertEqual(totals["feesCents"], 6.95)
        self.assertEqual(totals["realizedCents"], 8.05)
        self.assertEqual(totals["unrealizedCents"], 21.0)
        self.assertEqual(totals["totalCents"], 29.05)

    def test_watchdog_exit_fills_are_sells_that_close_the_position(self):
        self.write_exit_shard()
        fills, warnings = load_telemetry_fills(self.artifact)
        self.assertEqual(warnings, [])
        by_key = {item["fill_key"]: item for item in fills}
        self.assertEqual((by_key["w-1"]["action"], by_key["w-1"]["action_source"]), ("buy", "inventory"))
        self.assertEqual((by_key["w-2"]["action"], by_key["w-2"]["action_source"]), ("sell", "inventory"))
        self.assertEqual((by_key["v-1"]["action"], by_key["v-2"]["action"]), ("buy", "sell"))
        self.assertEqual((by_key["w-2"]["side"], by_key["w-2"]["price_c"], by_key["w-2"]["net_pos_before"], by_key["w-2"]["net_pos"]), ("yes", 12.0, 4.0, 0.0))
        self.assertEqual((by_key["v-2"]["side"], by_key["v-2"]["price_c"], by_key["v-2"]["net_pos_before"], by_key["v-2"]["net_pos"]), ("no", 14.0, -3.0, 0.0))
        result = self.store.pnl()
        rows = {item["ticker"]: item for item in result["tickers"]}
        # YES holding: 4 * (12 - 37) = -100c, fees 1c.
        self.assertEqual((rows["W"]["fills"], rows["W"]["sellFills"], rows["W"]["netPosition"]), (2, 1, 0.0))
        self.assertEqual(rows["W"]["realizedCents"], -101.0)
        self.assertEqual(rows["W"]["unrealizedCents"], 0.0)
        # NO holding: 3 * (14 - 26) = -36c, fees 0.6c.
        self.assertEqual((rows["V"]["fills"], rows["V"]["sellFills"], rows["V"]["netPosition"]), (2, 1, 0.0))
        self.assertEqual(rows["V"]["realizedCents"], -36.6)
        self.assertEqual(rows["V"]["unrealizedCents"], 0.0)
        self.assertEqual(result["totals"]["sellFills"], 2)
        self.assertEqual(result["totals"]["totalCents"], -137.6)
        self.assertFalse(any(item["basisEstimated"] for item in result["tickers"]))
        positions = self.overview()["positions"]
        self.assertEqual(positions, [])

    def test_exit_fill_without_inventory_columns_uses_the_watchdog_client_order_id(self):
        fills = [
            ("x-1", NOW_MS - 50 * 60_000, "X", "yes", "t8", "o-x-buy", 3000, 200, 0, 0, None, None, 3000, 2900, 6900, 0),
            ("x-2", NOW_MS - 40 * 60_000, "X", "yes", "t9", "o-x-exit", 2000, 200, 0, 1, None, None, 2000, 2000, 7900, 0),
        ]
        revisions = [
            ("r-x1", "create", "o-x-buy", "tob:yes:2", "yes", NOW_MS - 51 * 60_000, 200, 3000, "Filled", "X"),
            ("r-x2", "create", "o-x-exit", "wd:yes:2", "yes", NOW_MS - 40 * 60_000, 200, 2000, "Unknown", "X"),
        ]
        write_shard(self.artifact / "shards" / "worker-03" / "telemetry.sqlite3", fills, order_revisions=revisions)
        loaded, _ = load_telemetry_fills(self.artifact)
        self.assertEqual([(item["action"], item["action_source"]) for item in loaded], [("buy", "order_revision"), ("sell", "order_revision")])
        self.assertIsNone(loaded[0]["net_pos_before"])
        row = self.store.pnl()["tickers"][0]
        self.assertEqual((row["sellFills"], row["realizedCents"], row["basisEstimated"]), (1, -20.0, False))

    def test_window_cutting_into_a_position_seeds_the_basis_and_flags_it(self):
        self.write_shards()
        one_hour = self.store.pnl("1h")
        rows = {item["ticker"]: item for item in one_hour["tickers"]}
        # Only a-2 (30 minutes ago) is in scope; it starts from 2 YES carried in, seeded at the
        # pre-fill fair value of 50c: 1 matched pair realizes 100 - 50 - 45 - 1c fee = 4c, and the
        # remaining 1 YES marks 55c - 50c = 5c.
        self.assertEqual(rows["A"]["fills"], 1)
        self.assertTrue(rows["A"]["basisEstimated"])
        self.assertEqual((rows["A"]["basisSeedContracts"], rows["A"]["basisSeedCents"]), (2.0, 50.0))
        self.assertEqual((rows["A"]["realizedCents"], rows["A"]["unrealizedCents"], rows["A"]["netPosition"]), (4.0, 5.0, 1.0))
        self.assertFalse(rows["B"]["basisEstimated"])
        self.assertEqual(one_hour["totals"]["basisEstimatedTickers"], 1)
        self.assertEqual(len(one_hour["warnings"]), 1)
        self.assertIn("cost basis estimated for 1 market(s)", one_hour["warnings"][0])
        self.assertIn("window", one_hour["warnings"][0])
        self.assertIn("A", one_hour["warnings"][0])
        # The full history starts flat, so nothing is estimated there.
        everything = self.store.pnl("all")
        self.assertEqual(everything["totals"]["basisEstimatedTickers"], 0)
        self.assertEqual(everything["warnings"], [])

    def test_position_carried_over_from_a_previous_run_is_flagged(self):
        fills = [
            ("c-1", NOW_MS - 20 * 60_000, "C", "yes", "t10", "o-c", 5700, 700, 100, 0, -700, 0, 4800, 5600, 4200, 0),
        ]
        write_shard(self.artifact / "shards" / "worker-00" / "telemetry.sqlite3", fills)
        result = self.store.pnl()
        row = result["tickers"][0]
        self.assertTrue(row["basisEstimated"])
        self.assertEqual((row["basisSeedContracts"], row["basisSeedCents"]), (-7.0, 48.0))
        # 7 NO carried at 52c closed by 7 YES @ 57c: 7 * (100 - 57 - 52) - 1c fee = -64c.
        self.assertEqual(row["realizedCents"], -64.0)
        self.assertEqual(row["netPosition"], 0.0)
        self.assertIn("carried into the run", result["warnings"][0])

    def test_window_filters_on_ts_ms(self):
        self.write_shards()
        one_hour = self.store.pnl("1h")
        self.assertEqual(one_hour["window"], "1h")
        self.assertEqual(one_hour["totals"]["fills"], 2)
        rows = {item["ticker"]: item for item in one_hour["tickers"]}
        self.assertEqual(rows["A"]["fills"], 1)
        self.assertEqual(rows["B"]["fills"], 1)
        self.assertEqual(self.store.pnl("24h")["totals"]["fills"], 3)
        self.assertEqual(self.store.pnl("all")["totals"]["fills"], 3)
        with self.assertRaises(ValueError):
            self.store.pnl("2h")

    def test_duplicate_fill_keys_across_shards_count_once(self):
        self.write_shards()
        write_shard(self.artifact / "shards" / "worker-02" / "telemetry.sqlite3", WORKER_01_FILLS)
        self.assertEqual(len(find_telemetry_databases(self.artifact)), 3)
        result = self.store.pnl()
        self.assertEqual(result["totals"]["fills"], 3)
        self.assertEqual(result["source"]["shards"], 3)

    def test_telemetry_is_preferred_over_artifact_jsonl(self):
        self.write_shards()
        (self.artifact / "pnl_tracker.jsonl").write_text(json.dumps({
            "ts": "2026-09-02T00:00:00.000Z", "ticker": "C", "side": "yes", "qty": 1, "price_c": 30, "fee_c": 0, "net_pos": 1, "fair_c": 40,
        }) + "\n")
        self.assertEqual(self.store.pnl_fill_source()["kind"], "shard_telemetry")
        result = self.store.pnl()
        self.assertEqual(result["source"]["kind"], "shard_telemetry")
        self.assertEqual([item["ticker"] for item in result["tickers"]], ["A", "B"])
        self.assertEqual(result["totals"]["totalCents"], 29.05)

    def test_artifact_jsonl_is_used_when_the_run_has_no_telemetry(self):
        (self.artifact / "pnl_tracker.jsonl").write_text(json.dumps({
            "ts": "2026-09-02T00:00:00.000Z", "ticker": "C", "side": "yes", "qty": 1, "price_c": 30, "fee_c": 0, "net_pos": 1, "fair_c": 40,
        }) + "\n")
        (self.root / "logs" / "pnl_tracker.jsonl").write_text(json.dumps({
            "ts": "2026-09-02T00:00:00.000Z", "ticker": "L", "side": "no", "qty": 2, "price_c": 60, "fee_c": 1, "net_pos": -2, "fair_c": 30,
        }) + "\n")
        source = self.store.pnl_fill_source()
        self.assertEqual((source["kind"], source["path"]), ("pnl_tracker", self.artifact / "pnl_tracker.jsonl"))
        result = self.store.pnl()
        self.assertEqual(result["source"]["kind"], "pnl_tracker")
        self.assertEqual([item["ticker"] for item in result["tickers"]], ["C"])
        self.assertEqual(result["totals"]["totalCents"], 10.0)

    def test_falls_back_to_logs_jsonl_when_no_telemetry(self):
        (self.root / "logs" / "pnl_tracker.jsonl").write_text(json.dumps({
            "ts": "2026-09-02T00:00:00.000Z", "ticker": "L", "side": "no", "qty": 2, "price_c": 60, "fee_c": 1, "net_pos": -2, "fair_c": 30,
        }) + "\n")
        result = self.store.pnl()
        self.assertEqual(result["source"]["kind"], "pnl_tracker")
        self.assertEqual(result["warnings"], [])
        self.assertEqual([item["ticker"] for item in result["tickers"]], ["L"])
        self.assertEqual(result["totals"]["unrealizedCents"], 20.0)

    def test_missing_everything_still_warns_not_found(self):
        result = self.store.pnl()
        self.assertEqual(result["warnings"], ["fill data not found: pnl_tracker.jsonl"])
        self.assertEqual(result["totals"]["fills"], 0)

    def test_newest_run_is_used_when_no_run_is_active(self):
        self.write_shards()
        self.active_patch.stop()
        older = self.root / "older"
        older.mkdir()
        runs = [
            {"id": "run", "artifactPath": str(self.artifact), "createdAt": 2},
            {"id": "old", "artifactPath": str(older), "createdAt": 1},
        ]
        try:
            with patch.object(self.store.sessions, "active_run", return_value=None), \
                    patch.object(self.store.sessions, "list_runs", return_value=runs):
                result = self.store.pnl()
        finally:
            self.active_patch.start()
        self.assertEqual(result["source"]["kind"], "shard_telemetry")
        self.assertEqual(result["totals"]["fills"], 3)
        self.assertEqual(result["warnings"], [])

    def test_overview_positions_come_from_telemetry_and_match_the_exchange(self):
        self.write_shards()
        self.write_status([], portfolio=self.exchange_snapshot([("A", "yes", 100), ("B", "no", 300)]))
        overview = self.overview()
        self.assertFalse(any("fill data not found" in item for item in overview["warnings"]))
        self.assertEqual(overview["pnl"]["totalCents"], 29.05)
        positions = {item["ticker"]: item for item in overview["positions"]}
        self.assertEqual(set(positions), {"A", "B"})
        self.assertEqual(positions["A"]["netPosition"], 1.0)
        self.assertEqual(positions["A"]["exchangePosition"], 1.0)
        self.assertTrue(positions["A"]["positionMatchesExchange"])
        self.assertEqual(positions["B"]["netPosition"], -3.0)
        self.assertEqual(positions["B"]["exchangePosition"], -3.0)
        self.assertTrue(positions["B"]["positionMatchesExchange"])
        summary = overview["positionSummary"]
        self.assertEqual((summary["markets"], summary["grossContracts"], summary["netContracts"]), (2, 4.0, -2.0))
        self.assertEqual(summary["fillSource"], "shard_telemetry")
        check = summary["exchangeCheck"]
        self.assertEqual((check["available"], check["source"], check["checked"], check["mismatches"]), (True, "launcher_portfolio", 2, 0))
        self.assertEqual(check["updatedAt"], NOW_MS - 4_000)
        self.assertFalse(any("differs from the exchange" in item for item in overview["warnings"]))
        pnl_rows = {item["ticker"]: item for item in self.store.pnl()["tickers"]}
        self.assertEqual(pnl_rows["A"]["totalCents"], positions["A"]["totalCents"])
        self.assertEqual(pnl_rows["B"]["totalCents"], positions["B"]["totalCents"])

    def test_overview_flags_positions_the_exchange_no_longer_holds(self):
        self.write_shards()
        # B is absent from the exchange snapshot (settled market); Z is an exchange position
        # in a market the fleet never traded and is not the fleet's to check.
        self.write_status(
            [{"ticker": "A", "positionUnits": 0}, {"ticker": "B", "positionUnits": 0}],
            portfolio=self.exchange_snapshot([("A", "yes", 100), ("Z", "yes", 500)]),
        )
        overview = self.overview()
        positions = {item["ticker"]: item for item in overview["positions"]}
        # The exchange is the truth for what is open: A shows with the exchange's
        # size, the settled market B is not listed as open, and Z (never traded
        # by the fleet) stays on the Portfolio page.
        self.assertEqual(set(positions), {"A"})
        self.assertTrue(positions["A"]["positionMatchesExchange"])
        self.assertEqual(positions["A"]["positionSource"], "exchange")
        self.assertEqual(overview["positionSummary"]["source"], "exchange")
        self.assertEqual(overview["positionSummary"]["exchangeCheck"]["mismatches"], 1)
        mismatch = [item for item in overview["warnings"] if "differs from the exchange" in item]
        self.assertEqual(len(mismatch), 1)
        self.assertIn("B", mismatch[0])
        self.assertNotIn("Z", mismatch[0])

    def test_overview_makes_no_claim_without_an_exchange_snapshot(self):
        self.write_shards()
        # Launcher bots[] positions are not a cross-check: every bot reports flat here.
        self.write_status([{"ticker": "A", "positionUnits": 0}, {"ticker": "B", "positionUnits": 0}])
        overview = self.overview()
        positions = {item["ticker"]: item for item in overview["positions"]}
        self.assertEqual(set(positions), {"A", "B"})
        self.assertIsNone(positions["A"]["positionMatchesExchange"])
        self.assertIsNone(positions["A"]["exchangePosition"])
        check = overview["positionSummary"]["exchangeCheck"]
        self.assertEqual((check["available"], check["source"], check["checked"], check["mismatches"]), (False, None, 0, 0))
        self.assertFalse(any("differs from the exchange" in item for item in overview["warnings"]))

    def test_overview_uses_the_persisted_portfolio_snapshot_when_the_launcher_has_none(self):
        self.write_shards()
        self.write_status([], portfolio={"available": False, "positions": []})
        analytics = PortfolioAnalyticsStore(self.root / "runtime" / "portfolio_analytics.sqlite3")
        analytics.record_refresh(self.exchange_snapshot([("A", "yes", 100), ("B", "no", 200)]), [], [], [])
        overview = self.overview()
        positions = {item["ticker"]: item for item in overview["positions"]}
        self.assertTrue(positions["A"]["positionMatchesExchange"])
        self.assertFalse(positions["B"]["positionMatchesExchange"])
        self.assertEqual(positions["B"]["exchangePosition"], -2.0)
        check = overview["positionSummary"]["exchangeCheck"]
        self.assertEqual((check["available"], check["source"], check["mismatches"]), (True, "portfolio_analytics", 1))
        self.assertEqual(check["updatedAt"], NOW_MS - 5_000)

    def test_markets_breakdown_uses_same_telemetry_rows(self):
        self.write_shards()
        with patch("ui_api.store.subprocess.run", side_effect=FileNotFoundError("systemctl")):
            market = next(item for item in self.store.markets() if item["ticker"] == "A")
        self.assertEqual(market["pnl"]["netPosition"], 1.0)
        self.assertEqual(market["pnl"]["totalCents"], 28.0)

    def test_locked_or_broken_shard_is_a_warning_not_a_failure(self):
        self.write_shards()
        broken = self.artifact / "shards" / "worker-09" / "telemetry.sqlite3"
        broken.parent.mkdir(parents=True)
        broken.write_text("not a database")
        result = self.store.pnl()
        self.assertEqual(result["totals"]["fills"], 3)
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("worker-09", result["warnings"][0])
        self.assertFalse(any("not found" in item for item in result["warnings"]))


if __name__ == "__main__":
    unittest.main()
