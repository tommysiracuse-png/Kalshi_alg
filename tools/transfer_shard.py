"""Move cash between Kalshi exchange shards (YOU run this; it moves real money).

Kalshi keeps balances local to each exchange shard (0 default, 2 crypto,
3 tennis/baseball since 2026-08-24). Orders on a shard holding $0 are rejected
with user_not_found. This tool funds a shard from another one.

Usage (from the repo root, credentials come from ~/.config/kalshi-alg/bot.env):

  .venv\\Scripts\\python tools\\transfer_shard.py --show
  .venv\\Scripts\\python tools\\transfer_shard.py --to-shard 3 --dollars 150 --yes
  .venv\\Scripts\\python tools\\transfer_shard.py --to-shard 2 --dollars 100 --from-shard 0 --yes

Nothing moves without --yes. Amount is converted to centicents (1/100 cent):
$1.00 == 10_000 centicents. Kalshi docs: cross-shard transfers run in up to
three non-atomic steps; if one fails "funds may remain in the primary account"
on either side, so the tool prints balances before and after.
"""
from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

SHARD_NAMES = {0: "default (indices/gas/weather/most)", 1: "exotics/combos", 2: "crypto", 3: "tennis + baseball"}
CENTICENTS_PER_DOLLAR = 10_000


def load_env() -> None:
    if os.environ.get("KALSHI_API_KEY_ID"):
        return
    env = Path.home() / ".config" / "kalshi-alg" / "bot.env"
    for line in env.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def client():
    from adaptors.kalshi import KalshiApiClient, KalshiClientConfig

    return KalshiApiClient(KalshiClientConfig(
        api_key_id=os.environ["KALSHI_API_KEY_ID"],
        private_key_path=os.environ["KALSHI_PRIVATE_KEY_PATH"],
        enable_shared_write_rate_limiter=False,
    ))


def show(c) -> dict[int, int]:
    balance = c.get_account_balance()
    by_shard = {int(index): int(units) for index, units in balance.balance_by_exchange}
    print("Cash by exchange shard:")
    for index in sorted(by_shard):
        print(f"  shard {index} ({SHARD_NAMES.get(index, '?'):<34}) ${by_shard[index] / 10_000:,.2f}")
    return by_shard


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--show", action="store_true", help="print per-shard balances and exit")
    parser.add_argument("--to-shard", type=int, help="destination exchange shard (2 crypto, 3 tennis/baseball)")
    parser.add_argument("--from-shard", type=int, default=0, help="source exchange shard (default 0)")
    parser.add_argument("--dollars", type=Decimal, help="amount to move, in dollars (e.g. 150 or 99.50)")
    parser.add_argument("--yes", action="store_true", help="actually execute the transfer")
    args = parser.parse_args()

    load_env()
    c = client()
    before = show(c)
    if args.show or args.to_shard is None or args.dollars is None:
        if not args.show:
            print("\nNothing moved. Provide --to-shard and --dollars (and --yes) to transfer.")
        return 0

    if args.dollars <= 0:
        print("amount must be positive"); return 2
    if args.to_shard == args.from_shard:
        print("source and destination shards are the same"); return 2
    centicents = int((args.dollars * CENTICENTS_PER_DOLLAR).to_integral_value(rounding=ROUND_HALF_UP))
    available = before.get(args.from_shard, 0)  # PRICE_SCALE units == centicents
    if centicents > available:
        print(f"refusing: ${args.dollars} exceeds shard {args.from_shard} balance ${available / 10_000:,.2f}")
        return 2

    body = {
        "source": "event_contract",
        "destination": "event_contract",
        "amount": centicents,
        "source_exchange_shard": int(args.from_shard),
        "destination_exchange_shard": int(args.to_shard),
    }
    print(f"\nPlanned: move ${args.dollars} ({centicents:,} centicents) from shard {args.from_shard}"
          f" -> shard {args.to_shard} ({SHARD_NAMES.get(args.to_shard, '?')})")
    if not args.yes:
        print("Dry run only. Re-run with --yes to execute.")
        return 0

    response = c._post(f"{c.api_prefix}/portfolio/intra_exchange_instance_transfer", body, action="intra_exchange_instance_transfer")
    print("Transfer accepted:", response)
    print()
    after = show(c)
    moved = after.get(args.to_shard, 0) - before.get(args.to_shard, 0)
    print(f"\nshard {args.to_shard} changed by ${moved / 10_000:,.2f}"
          + ("" if moved >= centicents else "  <-- less than requested; check Kalshi (non-atomic transfer)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
