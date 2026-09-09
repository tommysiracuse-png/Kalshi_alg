"""Synthetic warm-mirror benchmark.

Usage:
    .venv/bin/python tools/benchmark_polymarket_mirror.py --markets 1000000

The benchmark measures local snapshot reads only. Network synchronization is
intentionally excluded because it runs continuously in the launcher child.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from clients.models import Market
from polymarket_cache import BookTop
from polymarket_mirror import PolymarketMirrorStore
from screeners.kalshi_screener import build_parser, build_settings_from_args, screen_markets
from screeners.screener import _BaseClientMarketSource


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--markets", type=int, default=1_000_000)
    parser.add_argument("--screen", action="store_true", help="also run the vectorized screener")
    args = parser.parse_args()
    count = max(1, int(args.markets))
    with tempfile.TemporaryDirectory(prefix="polymarket-mirror-bench-") as directory:
        store = PolymarketMirrorStore(Path(directory) / "mirror.sqlite3")
        generation = "benchmark"
        started = time.perf_counter()
        for start in range(0, count, 10_000):
            markets = [
                Market(
                    f"condition-{index}", title=f"Market {index}", status="active",
                    close_time_ms=int(time.time() * 1000) + 3_600_000,
                    volume_24h_units=100_000, open_interest_units=10_000,
                    yes_token_id=f"yes-{index}", no_token_id=f"no-{index}",
                    venue="polymarket", native_market_id=f"condition-{index}",
                    market_rules="direct_token_books",
                )
                for index in range(start, min(count, start + 10_000))
            ]
            store.upsert_markets(markets, generation)
        now = int(time.time() * 1000)
        top = BookTop(4_000, 100, 5_000, 100, now)
        for start in range(0, count, 10_000):
            books = {}
            for index in range(start, min(count, start + 10_000)):
                books[f"yes-{index}"] = top
                books[f"no-{index}"] = top
            store.upsert_books(books)
        store.publish(generation, catalog_count=count, max_age_seconds=60)
        if args.screen:
            client = SimpleNamespace(
                mirror_store=store,
                normalized_venue="polymarket",
                config=SimpleNamespace(
                    mirror_enabled=True,
                    mirror_required_complete_snapshot=True,
                    mirror_snapshot_max_age_seconds=60,
                ),
            )
            settings = build_settings_from_args(build_parser().parse_args([]))
            settings.update({"max_markets_to_scan": count, "top_n": 200, "min_vol24h": 0,
                             "min_oi": 0, "min_time_to_close_hrs": 0, "max_time_to_close_hrs": 100})
            screen_started = time.perf_counter()
            frame = screen_markets(_BaseClientMarketSource(client), settings)
            print(f"screen_seconds={time.perf_counter() - screen_started:.3f} selected={len(frame)}")
        rows = sum(1 for _ in store.iter_payloads(status="active", limit=count, max_age_seconds=60))
        elapsed = time.perf_counter() - started
        print(f"markets={count:,} rows={rows:,} elapsed={elapsed:.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
