import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import market_watchdog_runner


def profiler_args(slot_directory: Path, **overrides: object) -> SimpleNamespace:
    values = {
        "ticker": "TEST-TICKER",
        "profiler_script": "market_risk_profiler.py",
        "sample_seconds": 2.5,
        "poll_interval_seconds": 0.35,
        "confidence_reduction_threshold": 0.70,
        "confidence_flatten_threshold": 0.55,
        "api_key_id": None,
        "private_key": None,
        "use_demo": False,
        "profiler_timeout_seconds": 7.0,
        "profiler_max_concurrency": 1,
        "profiler_slot_wait_seconds": 0.0,
        "profiler_slot_dir": str(slot_directory),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class WatchdogProfilerSafeguardTests(unittest.TestCase):
    def test_stagger_is_stable_and_bounded(self) -> None:
        first = market_watchdog_runner.deterministic_stagger_seconds(
            "TEST-TICKER",
            interval_seconds=60.0,
            maximum_seconds=10.0,
        )
        second = market_watchdog_runner.deterministic_stagger_seconds(
            "TEST-TICKER",
            interval_seconds=60.0,
            maximum_seconds=10.0,
        )

        self.assertEqual(first, second)
        self.assertGreaterEqual(first, 0.0)
        self.assertLessEqual(first, 10.0)
        self.assertLessEqual(
            market_watchdog_runner.deterministic_stagger_seconds(
                "TEST-TICKER",
                interval_seconds=3.0,
                maximum_seconds=10.0,
            ),
            3.0,
        )

    def test_profiler_has_hard_timeout_and_releases_slot_after_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            slot_directory = Path(temporary) / "slots"
            args = profiler_args(slot_directory)
            expired = subprocess.TimeoutExpired(cmd=["profiler"], timeout=7.0)

            with patch("market_watchdog_runner.subprocess.run", side_effect=expired) as run:
                with self.assertRaisesRegex(RuntimeError, "timed out after 7.0s"):
                    market_watchdog_runner.run_profiler_once(args)

            self.assertEqual(run.call_args.kwargs["timeout"], 7.0)
            with market_watchdog_runner.acquire_profiler_slot(
                ticker="SECOND-TICKER",
                slot_directory=slot_directory,
                maximum_concurrency=1,
                wait_timeout_seconds=0.0,
            ):
                pass
            self.assertEqual((slot_directory / "slot-0.lock").read_text(), "{}")

    def test_shared_slot_prevents_a_second_profiler(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            slot_directory = Path(temporary) / "slots"
            with market_watchdog_runner.acquire_profiler_slot(
                ticker="FIRST-TICKER",
                slot_directory=slot_directory,
                maximum_concurrency=1,
                wait_timeout_seconds=0.0,
            ):
                with self.assertRaisesRegex(TimeoutError, "limit=1"):
                    with market_watchdog_runner.acquire_profiler_slot(
                        ticker="SECOND-TICKER",
                        slot_directory=slot_directory,
                        maximum_concurrency=1,
                        wait_timeout_seconds=0.0,
                    ):
                        pass

    def test_successful_profiler_payload_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            args = profiler_args(Path(temporary) / "slots")
            expected = {"ticker": args.ticker, "mode": "normal"}
            completed = subprocess.CompletedProcess(
                args=["profiler"],
                returncode=0,
                stdout=json.dumps(expected),
                stderr="",
            )

            with patch("market_watchdog_runner.subprocess.run", return_value=completed):
                actual = market_watchdog_runner.run_profiler_once(args)

            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
