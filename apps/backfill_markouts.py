#!/usr/bin/env python3
"""Materialize saved telemetry markouts into run metrics without rewriting artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.session_store import SessionStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-dir", default="session_data", help="Directory containing sessions.sqlite3")
    parser.add_argument("--include-active", action="store_true", help="Also refresh pending or active runs")
    args = parser.parse_args()
    report = SessionStore(Path(args.session_dir)).backfill_markouts(include_active=args.include_active)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
