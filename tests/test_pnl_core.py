import unittest

from pnl_core import classify_fill_action, compute_pnl, summarize_pnl


class PnlCoreTests(unittest.TestCase):
    def test_round_trip_and_open_position(self):
        result = summarize_pnl([
            {"ticker": "A", "side": "yes", "qty": 2, "price_c": 40, "fee_c": 1, "net_pos": 2, "fair_c": 50},
            {"ticker": "A", "side": "no", "qty": 1, "price_c": 45, "fee_c": 1, "net_pos": 1, "fair_c": 55},
        ])
        row = result["tickers"][0]
        self.assertEqual(row["fills"], 2)
        self.assertEqual(row["sellFills"], 0)
        self.assertFalse(row["basisEstimated"])
        self.assertEqual(row["realizedCents"], 13.0)
        self.assertEqual(row["unrealizedCents"], 15.0)
        self.assertEqual(result["totals"]["totalCents"], 28.0)
        self.assertEqual(result["totals"]["sellFills"], 0)
        self.assertEqual(result["totals"]["basisEstimatedTickers"], 0)

    def test_watchdog_exit_sell_of_yes_holding_closes_position_at_a_loss(self):
        # Buy 4 YES @ 37c, then the watchdog flattens: sell 4 YES @ 12c (taker).
        # The exit must realize 4 * (12 - 37) = -100c, not add 4 more YES.
        result = summarize_pnl([
            {"ticker": "Y", "side": "yes", "action": "buy", "qty": 4, "price_c": 37, "fee_c": 0.5,
             "net_pos_before": 0, "net_pos": 4, "fair_c": 40},
            {"ticker": "Y", "side": "yes", "action": "sell", "qty": 4, "price_c": 12, "fee_c": 0.5,
             "net_pos_before": 4, "net_pos": 0, "fair_c": 12},
        ])
        row = result["tickers"][0]
        self.assertEqual(row["fills"], 2)
        self.assertEqual(row["sellFills"], 1)
        self.assertEqual(row["netPosition"], 0.0)
        self.assertEqual(row["realizedCents"], -101.0)
        self.assertEqual(row["unrealizedCents"], 0.0)
        self.assertEqual(row["totalCents"], -101.0)
        self.assertEqual(result["totals"]["sellFills"], 1)

    def test_watchdog_exit_sell_of_yes_holding_can_be_a_gain(self):
        result = summarize_pnl([
            {"ticker": "Y", "side": "yes", "action": "buy", "qty": 3, "price_c": 30, "fee_c": 0,
             "net_pos_before": 0, "net_pos": 3, "fair_c": 30},
            {"ticker": "Y", "side": "yes", "action": "sell", "qty": 3, "price_c": 55, "fee_c": 0,
             "net_pos_before": 3, "net_pos": 0, "fair_c": 55},
        ])
        row = result["tickers"][0]
        self.assertEqual(row["realizedCents"], 75.0)
        self.assertEqual(row["totalCents"], 75.0)

    def test_watchdog_exit_sell_of_no_holding_closes_position(self):
        # Buy 14 NO @ 26c, then sold in three exits @ 14c / 19c / 15c (4 + 6 + 4).
        # Cash-flow: -14*26 + 4*14 + 6*19 + 4*15 = -364 + 230 = -134c, minus fees.
        result = summarize_pnl([
            {"ticker": "N", "side": "no", "action": "buy", "qty": 14, "price_c": 26, "fee_c": 2,
             "net_pos_before": 0, "net_pos": -14, "fair_c": 74},
            {"ticker": "N", "side": "no", "action": "sell", "qty": 4, "price_c": 14, "fee_c": 1,
             "net_pos_before": -14, "net_pos": -10, "fair_c": 86},
            {"ticker": "N", "side": "no", "action": "sell", "qty": 6, "price_c": 19, "fee_c": 1,
             "net_pos_before": -10, "net_pos": -4, "fair_c": 81},
            {"ticker": "N", "side": "no", "action": "sell", "qty": 4, "price_c": 15, "fee_c": 1,
             "net_pos_before": -4, "net_pos": 0, "fair_c": 85},
        ])
        row = result["tickers"][0]
        self.assertEqual(row["sellFills"], 3)
        self.assertEqual(row["netPosition"], 0.0)
        self.assertEqual(row["realizedCents"], -139.0)
        self.assertEqual(row["unrealizedCents"], 0.0)

    def test_partial_exit_leaves_remaining_position_at_original_basis(self):
        result = summarize_pnl([
            {"ticker": "P", "side": "no", "action": "buy", "qty": 10, "price_c": 30, "fee_c": 0,
             "net_pos_before": 0, "net_pos": -10, "fair_c": 70},
            {"ticker": "P", "side": "no", "action": "sell", "qty": 4, "price_c": 20, "fee_c": 0,
             "net_pos_before": -10, "net_pos": -6, "fair_c": 75},
        ])
        row = result["tickers"][0]
        self.assertEqual(row["netPosition"], -6.0)
        # 4 closed at 20c vs 30c basis = -40c realized; 6 left at 30c marked (100 - 75) = 25c -> -30c.
        self.assertEqual(row["realizedCents"], -40.0)
        self.assertEqual(row["unrealizedCents"], -30.0)
        self.assertEqual(row["totalCents"], -70.0)

    def test_window_starting_mid_position_seeds_yes_basis_and_flags_row(self):
        # 7 YES carried into scope; the first in-scope fill buys 3 more @ 60c with pre-fill fair 55c.
        result = summarize_pnl([
            {"ticker": "S", "side": "yes", "action": "buy", "qty": 3, "price_c": 60, "fee_c": 0,
             "net_pos_before": 7, "net_pos": 10, "fair_c": 62, "fair_before_c": 55},
        ])
        row = result["tickers"][0]
        self.assertTrue(row["basisEstimated"])
        self.assertEqual(row["basisSeedContracts"], 7.0)
        self.assertEqual(row["basisSeedCents"], 55.0)
        self.assertEqual(row["fills"], 1)
        self.assertEqual(row["netPosition"], 10.0)
        # avg YES basis = (7*55 + 3*60) / 10 = 56.5c; marked at 62c on 10 contracts.
        self.assertEqual(row["unrealizedCents"], 55.0)
        self.assertEqual(row["realizedCents"], 0.0)
        self.assertEqual(result["totals"]["basisEstimatedTickers"], 1)

    def test_window_starting_mid_no_position_closed_by_yes_buys_books_the_outlay(self):
        # 7 NO carried over from a previous run (fair 48c => NO basis 52c); the run buys
        # 7 YES @ 57c to close it, ending flat. The round trip is 7 * (100 - 57 - 52) = -63c.
        result = summarize_pnl([
            {"ticker": "R", "side": "yes", "action": "buy", "qty": 7, "price_c": 57, "fee_c": 1,
             "net_pos_before": -7, "net_pos": 0, "fair_c": 50, "fair_before_c": 48},
        ])
        row = result["tickers"][0]
        self.assertTrue(row["basisEstimated"])
        self.assertEqual(row["basisSeedContracts"], -7.0)
        self.assertEqual(row["basisSeedCents"], 48.0)
        self.assertEqual(row["realizedCents"], -64.0)
        self.assertEqual(row["unrealizedCents"], 0.0)

    def test_seed_falls_back_to_fill_price_when_no_pre_fill_fair(self):
        result = summarize_pnl([
            {"ticker": "F", "side": "no", "action": "sell", "qty": 2, "price_c": 30, "fee_c": 0,
             "net_pos_before": -2, "net_pos": 0, "fair_c": 70},
        ])
        row = result["tickers"][0]
        self.assertTrue(row["basisEstimated"])
        # NO fill @ 30c => YES-equivalent 70c seed => NO basis 30c; exit at 30c realizes 0.
        self.assertEqual(row["basisSeedCents"], 70.0)
        self.assertEqual(row["realizedCents"], 0.0)

    def test_inventory_change_between_fills_is_seeded_at_fair_and_flagged(self):
        # Bought 7 NO @ 31c; the next fill starts from -6 (one NO contract vanished via a position
        # resync). The missing contract is treated as closed at the pre-fill fair of 57c, i.e. a
        # 1-contract YES buy @ 57c: realized 100 - 57 - 31 = 12c on that pair.
        result = summarize_pnl([
            {"ticker": "G", "side": "no", "action": "buy", "qty": 7, "price_c": 31, "fee_c": 0,
             "net_pos_before": 0, "net_pos": -7, "fair_c": 56, "fair_before_c": 56},
            {"ticker": "G", "side": "yes", "action": "buy", "qty": 1, "price_c": 46, "fee_c": 0,
             "net_pos_before": -6, "net_pos": -5, "fair_c": 57, "fair_before_c": 57},
        ])
        row = result["tickers"][0]
        self.assertTrue(row["basisEstimated"])
        self.assertEqual(row["basisGapFills"], 1)
        self.assertEqual(row["basisSeedContracts"], 1.0)
        self.assertEqual(row["basisSeedCents"], 57.0)
        self.assertEqual(row["netPosition"], -5.0)
        # yes: 1 @ 57 (gap) + 1 @ 46 = avg 51.5, matched 2: 2 * (100 - 51.5 - 31) = 35c realized;
        # 5 NO left at 31c basis marked (100 - 57) = 43c -> +60c unrealized.
        self.assertEqual(row["realizedCents"], 35.0)
        self.assertEqual(row["unrealizedCents"], 60.0)
        self.assertEqual(result["totals"]["basisEstimatedTickers"], 1)

    def test_no_seeding_without_inventory_before_or_when_flat(self):
        rows = compute_pnl([
            {"ticker": "J", "side": "yes", "qty": 1, "price_c": 30, "fee_c": 0, "net_pos": 1, "fair_c": 40},
            {"ticker": "K", "side": "yes", "qty": 1, "price_c": 30, "fee_c": 0, "net_pos_before": 0, "net_pos": 1, "fair_c": 40},
            {"ticker": "L", "side": "yes", "qty": 1, "price_c": 30, "fee_c": 0, "net_pos_before": 5, "net_pos": 6, "fair_c": 40},
            {"ticker": "L", "side": "yes", "qty": 1, "price_c": 30, "fee_c": 0, "net_pos_before": 6, "net_pos": 7, "fair_c": 40},
        ])
        self.assertFalse(rows["J"]["basis_estimated"])
        self.assertFalse(rows["K"]["basis_estimated"])
        self.assertTrue(rows["L"]["basis_estimated"])
        self.assertEqual(rows["L"]["basis_seed_qty"], 5.0)
        self.assertEqual(rows["L"]["yes_qty"], 7.0)

    def test_classify_fill_action_prefers_inventory_then_client_order_id(self):
        self.assertEqual(classify_fill_action("yes", 400, 0, 400), ("buy", "inventory"))
        self.assertEqual(classify_fill_action("yes", 399, 399, 0), ("sell", "inventory"))
        self.assertEqual(classify_fill_action("no", 300, 0, -300), ("buy", "inventory"))
        self.assertEqual(classify_fill_action("no", 300, -300, 0), ("sell", "inventory"))
        self.assertEqual(classify_fill_action("no", 400, -1400, -1000, "wd:no:abc"), ("sell", "inventory"))
        # Inventory unavailable or inconsistent: the watchdog client id decides.
        self.assertEqual(classify_fill_action("yes", 400, None, 0, "wd:yes:abc"), ("sell", "order_revision"))
        self.assertEqual(classify_fill_action("yes", 400, None, 400, "tob:yes:abc"), ("buy", "order_revision"))
        self.assertEqual(classify_fill_action("yes", 400, 100, 300, "wd:yes:abc"), ("sell", "order_revision"))
        self.assertEqual(classify_fill_action("yes", 400, None, None), ("buy", "assumed"))
        self.assertEqual(classify_fill_action("yes", 0, 0, 0), ("buy", "assumed"))


if __name__ == "__main__":
    unittest.main()
