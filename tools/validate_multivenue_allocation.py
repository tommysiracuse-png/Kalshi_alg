#!/usr/bin/env python3
"""Deterministic, network-free preview of schema-v5 venue admission."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.session_config import default_session_configuration, validate_session_configuration, enabled_venues, effective_screener_configuration
from fleet_runtime.capacity import calculate_fleet_capacity


def main() -> int:
    parser = argparse.ArgumentParser(description="Preview multi-venue screener and capacity allocation")
    parser.add_argument("--config", help="schema-v5 session JSON; defaults to built-in Kalshi session")
    parser.add_argument("--candidates", type=int, default=0)
    args = parser.parse_args()
    config = default_session_configuration() if not args.config else json.loads(open(args.config, encoding="utf-8").read())
    config = validate_session_configuration(config)
    print("Enabled venues:", ", ".join(enabled_venues(config)))
    for venue in enabled_venues(config):
        vc = config["venues"][venue]
        settings = effective_screener_configuration(config, venue)
        candidates = args.candidates or int(settings.get("topN", 0))
        cap = calculate_fleet_capacity(api_tier="preview", read_refill_rate=10, write_refill_rate=30, requested_markets=candidates, cash_available_units=10**9)
        print(f"{venue}: priority={vc['priority']} maxBots={vc['maxBots']} candidates={candidates} capacityLimit={cap.capacity_market_limit}")
    print("Global max bots:", config["launcher"]["maxBots"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
