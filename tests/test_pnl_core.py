import unittest

from pnl_core import summarize_pnl


class PnlCoreTests(unittest.TestCase):
    def test_round_trip_and_open_position(self):
        result = summarize_pnl([
            {"ticker": "A", "side": "yes", "qty": 2, "price_c": 40, "fee_c": 1, "net_pos": 2, "fair_c": 50},
            {"ticker": "A", "side": "no", "qty": 1, "price_c": 45, "fee_c": 1, "net_pos": 1, "fair_c": 55},
        ])
        row = result["tickers"][0]
        self.assertEqual(row["fills"], 2)
        self.assertEqual(row["realizedCents"], 13.0)
        self.assertEqual(row["unrealizedCents"], 15.0)
        self.assertEqual(result["totals"]["totalCents"], 28.0)


if __name__ == "__main__":
    unittest.main()
