#!/usr/bin/env python3
"""Periodic watchdog runner for one Kalshi ticker.

This process stays outside V1's trading loop. It periodically runs the heuristic
market risk profiler, normalizes the resulting state, and atomically rewrites a
single state JSON file for the ticker.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
LOGGER = logging.getLogger("market_watchdog_runner")


def log_event(event_name: str, **fields: object) -> None:
    if fields:
        detail = " ".join(f"{key}={value}" for key, value in fields.items())
        LOGGER.info("%s | %s", event_name, detail)
    else:
        LOGGER.info("%s", event_name)


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=False)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def normalize_state(profile: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticker": profile.get("ticker"),
        "generated_at_ms": int(profile.get("generated_at_ms") or 0),
        "official_close_time_ms": profile.get("official_close_time_ms"),
        "effective_close_time_ms": profile.get("effective_close_time_ms"),
        "soft_stop_time_ms": profile.get("soft_stop_time_ms"),
        "hard_stop_time_ms": profile.get("hard_stop_time_ms"),
        "flatten_only_time_ms": profile.get("flatten_only_time_ms"),
        "observed_volatility_bp_60s_equiv": float(profile.get("observed_volatility_bp_60s_equiv") or 0.0),
        "observed_range_bp": float(profile.get("observed_range_bp") or 0.0),
        "bid_flip_rate_per_minute": float(profile.get("bid_flip_rate_per_minute") or 0.0),
        "suggested_action": str(profile.get("suggested_action") or "trade_normal"),
        "confidence": float(profile.get("confidence") or 0.0),
        "mode": str(profile.get("mode") or "normal"),
        "reason": str(profile.get("reason") or "unknown"),
        "profile_version": str(profile.get("profile_version") or "heuristic-v2"),
        "title": profile.get("title"),
        "market_family": profile.get("market_family"),
        "notes": list(profile.get("notes") or []),
        "runner_updated_at_ms": int(time.time() * 1000),
    }


def build_profiler_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(Path(args.profiler_script).resolve()),
        "--ticker",
        args.ticker,
        "--sample-seconds",
        str(args.sample_seconds),
        "--poll-interval-seconds",
        str(args.poll_interval_seconds),
        "--confidence-reduction-threshold",
        str(args.confidence_reduction_threshold),
        "--confidence-flatten-threshold",
        str(args.confidence_flatten_threshold),
    ]
    if args.use_demo:
        command.append("--use-demo")
    if args.api_key_id:
        command.extend(["--api-key-id", args.api_key_id])
    if args.private_key:
        command.extend(["--private-key", args.private_key])
    return command


def run_profiler_once(args: argparse.Namespace) -> Dict[str, Any]:
    command = build_profiler_command(args)
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        env=dict(os.environ),
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip().replace("\n", " | ")
        stdout = (completed.stdout or "").strip().replace("\n", " | ")
        raise RuntimeError(
            f"profiler failed rc={completed.returncode} stderr={stderr or 'none'} stdout={stdout or 'none'}"
        )
    stdout = (completed.stdout or "").strip()
    if not stdout:
        raise RuntimeError("profiler returned empty stdout")
    try:
        return json.loads(stdout)
    except Exception as exc:
        raise RuntimeError(f"profiler returned non-json payload: {stdout[:500]}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the heuristic market watchdog for one ticker.")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--profiler-script", default=str(Path(__file__).with_name("market_risk_profiler.py")))
    parser.add_argument("--interval-seconds", type=float, default=180.0)
    parser.add_argument("--sample-seconds", type=float, default=4.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.35)
    parser.add_argument("--confidence-reduction-threshold", type=float, default=0.70)
    parser.add_argument("--confidence-flatten-threshold", type=float, default=0.55)
    parser.add_argument("--api-key-id", default=os.getenv("KALSHI_API_KEY_ID") or os.getenv("API_KEY_ID"))
    parser.add_argument("--private-key", default=os.getenv("KALSHI_PRIVATE_KEY_PATH") or os.getenv("PRIVATE_KEY_PATH"))
    parser.add_argument("--use-demo", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state_file = Path(args.state_file).resolve()

    log_event(
        "WATCHDOG_PROFILE_START",
        ticker=args.ticker,
        sample_seconds=args.sample_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )

    while True:
        cycle_started_ms = int(time.time() * 1000)
        try:
            profile = run_profiler_once(args)
            state = normalize_state(profile)
            atomic_write_json(state_file, state)
            log_event(
                "WATCHDOG_PROFILE_RESULT",
                ticker=args.ticker,
                effective_close=state.get("effective_close_time_ms"),
                hard_stop=state.get("hard_stop_time_ms"),
                flatten_only=state.get("flatten_only_time_ms"),
                confidence=state.get("confidence"),
                mode=state.get("mode"),
                reason=state.get("reason"),
            )
        except Exception as exc:
            existing = read_json(state_file)
            age_seconds = None
            if existing and existing.get("generated_at_ms"):
                age_seconds = max(0.0, (cycle_started_ms - int(existing["generated_at_ms"])) / 1000.0)
            log_event(
                "WATCHDOG_PROFILE_ERROR",
                ticker=args.ticker,
                error=str(exc),
            )
            if age_seconds is not None:
                log_event(
                    "WATCHDOG_PROFILE_STALE",
                    ticker=args.ticker,
                    age_seconds=f"{age_seconds:.1f}",
                    action="fail_safe_reduction_only",
                )

        time.sleep(max(1.0, float(args.interval_seconds)))


if __name__ == "__main__":
    raise SystemExit(main())
