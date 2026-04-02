#!/usr/bin/env python3
"""Read logs/pnl_tracker.jsonl and print P&L summary by category and ticker."""

import json
import pathlib
import sys
from collections import defaultdict

PNL_FILE = pathlib.Path(__file__).resolve().parent / "logs" / "pnl_tracker.jsonl"


def load_fills(path: pathlib.Path):
    if not path.exists():
        print(f"No fill data found at {path}")
        sys.exit(0)
    fills = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                fills.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"Warning: skipping malformed line {lineno}")
    return fills


def compute_pnl(fills):
    """Compute realized + unrealized P&L per ticker.

    Cost basis tracking:
    - YES buy  => spend price_c * qty  (long YES)
    - NO  buy  => spend price_c * qty  (short YES, i.e. long NO)

    Realized P&L comes from round-trips (matched YES+NO fills).
    Unrealized = mark-to-market of open position using last fair_c.
    """
    tickers = defaultdict(lambda: {
        "category": "other",
        "fills": 0,
        "yes_qty": 0.0,
        "no_qty": 0.0,
        "yes_cost_c": 0.0,   # total cents spent buying YES
        "no_cost_c": 0.0,    # total cents spent buying NO
        "fees_c": 0.0,
        "last_fair_c": None,
        "last_net_pos": 0.0,
        "first_ts": None,
        "last_ts": None,
    })

    for f in fills:
        ticker = f["ticker"]
        t = tickers[ticker]
        t["category"] = f.get("category", "other")
        t["fills"] += 1
        qty = f.get("qty", 0) or 0
        price_c = f.get("price_c")
        fee_c = f.get("fee_c") or 0
        t["fees_c"] += fee_c
        t["last_net_pos"] = f.get("net_pos", 0)
        if f.get("fair_c") is not None:
            t["last_fair_c"] = f["fair_c"]
        if t["first_ts"] is None:
            t["first_ts"] = f.get("ts")
        t["last_ts"] = f.get("ts")

        if price_c is None:
            continue

        if f["side"] == "yes":
            t["yes_qty"] += qty
            t["yes_cost_c"] += price_c * qty
        else:
            t["no_qty"] += qty
            t["no_cost_c"] += price_c * qty

    return tickers


def print_summary(tickers):
    # Aggregate by category
    categories = defaultdict(lambda: {
        "tickers": 0,
        "fills": 0,
        "fees_c": 0.0,
        "realized_c": 0.0,
        "unrealized_c": 0.0,
    })

    print("=" * 100)
    print(f"{'TICKER':<45} {'CAT':<14} {'FILLS':>5} {'NET':>6} "
          f"{'COST':>8} {'FEES':>7} {'REAL':>8} {'UNREAL':>8}")
    print("-" * 100)

    for ticker in sorted(tickers, key=lambda t: tickers[t]["category"]):
        t = tickers[ticker]
        # matched = min(yes_qty, no_qty) round-trips
        matched = min(t["yes_qty"], t["no_qty"])
        if matched > 0:
            avg_yes_c = t["yes_cost_c"] / t["yes_qty"] if t["yes_qty"] else 0
            avg_no_c = t["no_cost_c"] / t["no_qty"] if t["no_qty"] else 0
            # Each matched contract pair: revenue = 100c, cost = avg_yes + avg_no
            realized_c = matched * (100.0 - avg_yes_c - avg_no_c) - t["fees_c"]
        else:
            realized_c = -t["fees_c"]

        # Unrealized: open position * (fair_value - avg_cost)
        open_qty = t["last_net_pos"]
        unrealized_c = 0.0
        if open_qty != 0 and t["last_fair_c"] is not None:
            if open_qty > 0:  # long YES
                avg_cost = t["yes_cost_c"] / t["yes_qty"] if t["yes_qty"] else 0
                unrealized_c = open_qty * (t["last_fair_c"] - avg_cost)
            else:  # short YES (long NO)
                avg_cost = t["no_cost_c"] / t["no_qty"] if t["no_qty"] else 0
                unrealized_c = abs(open_qty) * ((100.0 - t["last_fair_c"]) - avg_cost)

        total_cost_c = t["yes_cost_c"] + t["no_cost_c"]

        cat = categories[t["category"]]
        cat["tickers"] += 1
        cat["fills"] += t["fills"]
        cat["fees_c"] += t["fees_c"]
        cat["realized_c"] += realized_c
        cat["unrealized_c"] += unrealized_c

        print(f"{ticker:<45} {t['category']:<14} {t['fills']:>5} "
              f"{t['last_net_pos']:>+6.0f} {total_cost_c:>7.1f}c {t['fees_c']:>6.1f}c "
              f"{realized_c:>+7.1f}c {unrealized_c:>+7.1f}c")

    print("=" * 100)
    print(f"\n{'CATEGORY':<14} {'MKTS':>5} {'FILLS':>5} {'FEES':>8} "
          f"{'REALIZED':>10} {'UNREALIZED':>10} {'TOTAL':>10}")
    print("-" * 66)
    totals = {"fills": 0, "fees_c": 0, "realized_c": 0, "unrealized_c": 0}
    for cat_name in sorted(categories):
        c = categories[cat_name]
        total = c["realized_c"] + c["unrealized_c"]
        print(f"{cat_name:<14} {c['tickers']:>5} {c['fills']:>5} {c['fees_c']:>7.1f}c "
              f"{c['realized_c']:>+9.1f}c {c['unrealized_c']:>+9.1f}c {total:>+9.1f}c")
        for k in totals:
            totals[k] += c[k]

    print("-" * 66)
    grand = totals["realized_c"] + totals["unrealized_c"]
    print(f"{'TOTAL':<14} {'':<5} {totals['fills']:>5} {totals['fees_c']:>7.1f}c "
          f"{totals['realized_c']:>+9.1f}c {totals['unrealized_c']:>+9.1f}c {grand:>+9.1f}c")
    print(f"\n  Grand total: {grand/100:>+.2f} USD  (fees: {totals['fees_c']/100:.2f} USD)")


def main():
    fills = load_fills(PNL_FILE)
    print(f"Loaded {len(fills)} fills from {PNL_FILE}\n")
    tickers = compute_pnl(fills)
    print_summary(tickers)


if __name__ == "__main__":
    main()
