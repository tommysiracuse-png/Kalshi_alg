#!/usr/bin/env python3
"""
Launch one single-market Kalshi bot per ticker from a screener CSV.

This launcher intentionally does NOT do market selection by itself.
It reads the rows from your screener export, starts one bot per market,
and can periodically refresh the entire bot set by rerunning the screener.

Periodic refresh behavior in this version:
- Optionally rerun the screener before the initial launch.
- Every refresh interval, stop all child bots.
- Rerun the screener.
- Relaunch the new screener markets plus any previously running markets that
  still have non-zero inventory, so you never leave held inventory without a bot.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import json
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, IO, List, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from fleet_models import DEFAULT_MAX_BOTS, MAX_CONCURRENT_BOTS
from runtime_control import ControlRequest, ControlServer, STATUS_SCHEMA_VERSION

import contextlib

try:
    import fcntl
except ImportError:  # pragma: no cover - launcher control is Linux-only in production
    fcntl = None
try:
    import msvcrt
except ImportError:  # non-Windows
    msvcrt = None


# Serializes disable-list read-modify-write across threads in THIS process
# (the launcher's watchdog-exit handlers). fcntl/msvcrt below add best-effort
# cross-process exclusion on top.
_DISABLE_LIST_LOCK = threading.Lock()


@contextlib.contextmanager
def _exclusive_file_lock(lock_path: Path):
    """Cross-platform exclusive lock guarding disable-list read-modify-write.

    The market disable list is a kill-switch: a lost concurrent write could
    leave a market quoting after a watchdog flatten-exit. The in-process
    threading lock is authoritative for same-process writers; POSIX fcntl adds
    cross-process exclusion, and Windows msvcrt is attempted best-effort (its
    failures are swallowed rather than propagated, so a write is never dropped).
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with _DISABLE_LIST_LOCK:
        with lock_path.open("a+", encoding="utf-8") as handle:
            locked_win = False
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            elif msvcrt is not None:
                handle.seek(0)
                if not handle.read(1):
                    try:
                        handle.write("x")
                        handle.flush()
                    except OSError:
                        pass
                handle.seek(0)
                for _ in range(200):
                    try:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        locked_win = True
                        break
                    except OSError:
                        time.sleep(0.02)
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                elif locked_win:
                    handle.seek(0)
                    try:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass


WATCHDOG_EXIT_CODES = {61, 62, 63, 64}


ATOMIC_REPLACE_ATTEMPTS = 6
ATOMIC_REPLACE_RETRY_SECONDS = 0.05


def atomic_write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=False)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    # On Windows a reader (dashboard API, editor, indexer) holding the target
    # open makes the rename fail with EACCES for a few milliseconds; retry so
    # the snapshot keeps updating, and never leave the temp file behind.
    for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
        try:
            os.replace(tmp_path, path)
            return
        except PermissionError:
            if attempt + 1 >= ATOMIC_REPLACE_ATTEMPTS:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
                raise
            time.sleep(ATOMIC_REPLACE_RETRY_SECONDS * (attempt + 1))
        except OSError:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise


def load_json_file(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def safe_ticker_filename(ticker: str) -> str:
    return str(ticker).replace("/", "_").replace("\\", "_")


def load_disable_list(path: Path) -> Dict[str, object]:
    data = load_json_file(path)
    if isinstance(data, dict):
        return data
    return {}


def write_disable_list(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with _exclusive_file_lock(lock_path):
        atomic_write_json(path, payload)


def disable_ticker(path: Path, ticker: str, reason: str, *, exit_code: Optional[int] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with _exclusive_file_lock(lock_path):
        payload = load_disable_list(path)
        payload[str(ticker)] = {
            "ticker": str(ticker),
            "reason": str(reason),
            "disabled_at_epoch_seconds": time.time(),
            "exit_code": exit_code,
        }
        atomic_write_json(path, payload)


def clear_disabled_ticker(path: Path, ticker: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with _exclusive_file_lock(lock_path):
        payload = load_disable_list(path)
        removed = payload.pop(str(ticker), None) is not None
        atomic_write_json(path, payload)
        return removed


def run_profiler_prestart(
    *,
    profiler_script_path: Path,
    ticker: str,
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    use_demo: bool,
    sample_seconds: float,
    poll_interval_seconds: float,
    confidence_reduction_threshold: float,
    confidence_flatten_threshold: float,
) -> Dict[str, object]:
    command = [
        sys.executable,
        str(profiler_script_path),
        "--ticker",
        str(ticker),
        "--sample-seconds",
        str(sample_seconds),
        "--poll-interval-seconds",
        str(poll_interval_seconds),
        "--confidence-reduction-threshold",
        str(confidence_reduction_threshold),
        "--confidence-flatten-threshold",
        str(confidence_flatten_threshold),
    ]
    if use_demo:
        command.append("--use-demo")
    if api_key_id:
        command.extend(["--api-key-id", api_key_id])
    if private_key_path:
        command.extend(["--private-key", private_key_path])

    completed = subprocess.run(command, capture_output=True, text=True, check=False, env=dict(os.environ))
    if completed.returncode != 0:
        raise RuntimeError(
            f"watchdog profiler failed for {ticker}: rc={completed.returncode} stderr={(completed.stderr or '').strip()} stdout={(completed.stdout or '').strip()}"
        )
    try:
        return json.loads((completed.stdout or "").strip())
    except Exception as exc:
        raise RuntimeError(f"watchdog profiler returned non-json payload for {ticker}") from exc


def ticker_position_units(
    *,
    ticker: str,
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    use_demo: bool,
    subaccount: Optional[int],
) -> int:
    if not api_key_id or not private_key_path:
        return 0
    client = KalshiPositionClient(
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        use_demo=use_demo,
        subaccount=(0 if subaccount is None else int(subaccount)),
    )
    return int(client.get_position_count_units(ticker))

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LaunchPick:
    ticker: str
    title: str
    yes_budget_cents: int
    no_budget_cents: int
    raw_row: Dict[str, str]


@dataclass
class ChildProcess:
    ticker: str
    process: subprocess.Popen
    log_path: Path
    log_handle: IO[str]
    slot_index: int
    pick: LaunchPick
    watchdog_process: Optional[subprocess.Popen] = None
    watchdog_log_path: Optional[Path] = None
    watchdog_log_handle: Optional[IO[str]] = None
    watchdog_state_file: Optional[Path] = None
    control_socket: Optional[Path] = None


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch one Kalshi single-market bot per ticker from a screener CSV."
    )
    parser.add_argument(
        "--screen-file",
        required=True,
        help="CSV file exported from your screener notebook.",
    )
    parser.add_argument(
        "--minimum-carryover-value-cents",
        type=float,
        default=100.0,
        help="Only keep a ticker alive across refresh if marked inventory value is at least this many cents.",
    )
    parser.add_argument(
        "--bot-script",
        required=True,
        help="Path to the single-market bot script, for example V1.py.",
    )
    parser.add_argument(
        "--max-bots",
        type=int,
        default=DEFAULT_MAX_BOTS,
        help=(
            "Maximum number of managed markets to launch "
            f"(1-{MAX_CONCURRENT_BOTS}; 25 markets per worker, up to 20 workers)."
        ),
    )
    parser.add_argument(
        "--ticker-column",
        default="Ticker",
        help="Name of the CSV column that holds the market ticker.",
    )
    parser.add_argument(
        "--title-column",
        default="SearchText",
        help="Preferred CSV column to display as the market description.",
    )
    parser.add_argument(
        "--yes-budget-cents",
        type=int,
        default=100,
        help="Default YES-side working budget per market, in cents.",
    )
    parser.add_argument(
        "--no-budget-cents",
        type=int,
        default=100,
        help="Default NO-side working budget per market, in cents.",
    )
    parser.add_argument(
        "--yes-budget-column",
        default="",
        help="Optional CSV column containing per-row YES budget values in cents.",
    )
    parser.add_argument(
        "--no-budget-column",
        default="",
        help="Optional CSV column containing per-row NO budget values in cents.",
    )
    parser.add_argument(
        "--api-key-id",
        default=None,
        help="Kalshi API key ID. If omitted, KALSHI_API_KEY_ID or API_KEY_ID is used.",
    )
    parser.add_argument(
        "--private-key",
        default=None,
        help="Path to the Kalshi private key file. If omitted, KALSHI_PRIVATE_KEY_PATH or PRIVATE_KEY_PATH is used.",
    )
    parser.add_argument(
        "--use-demo",
        action="store_true",
        help="Pass demo mode through to each bot.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would launch, but do not start child bot processes.",
    )
    parser.add_argument(
        "--logs-dir",
        default="",
        help="Optional directory for bot log files. Defaults to <bot_script_dir>/logs.",
    )
    parser.add_argument(
        "--launch-delay-seconds",
        type=float,
        default=0.25,
        help="Delay between starting child bots, in seconds.",
    )
    parser.add_argument(
        "--subaccount",
        type=int,
        default=None,
        help="Optional Kalshi subaccount number to pass through to each bot.",
    )
    parser.add_argument(
        "--pass-credentials-via-cli",
        action="store_true",
        help="Pass credentials to child bots on the command line instead of environment variables. This is less secure and normally not needed.",
    )
    parser.add_argument(
        "--screener-script",
        default="",
        help="Path to the screener script used to refresh markets.",
    )
    parser.add_argument(
        "--screener-output",
        default="",
        help="CSV path the screener writes to. Defaults to --screen-file.",
    )
    parser.add_argument(
        "--run-screener-on-start",
        action="store_true",
        help="Run the screener once before the initial launch.",
    )
    parser.add_argument(
        "--refresh-interval-seconds",
        type=float,
        default=1800.0,
        help="If > 0, rerun the screener and restart bots on this interval.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=2.0,
        help="Polling interval for monitoring child processes and refresh timing.",
    )
    parser.add_argument("--watchdog-runner-script", default="", help="Path to market_watchdog_runner.py")
    parser.add_argument("--watchdog-profiler-script", default="", help="Path to market_risk_rofile.py")
    parser.add_argument("--watchdog-state-dir", default="", help="Directory for watchdog state JSON files.")
    parser.add_argument("--watchdog-disable-file", default="", help="Path to the permanent watchdog disable-list JSON file.")
    parser.add_argument("--watchdog-interval-seconds", type=float, default=180.0, help="How often each watchdog runner recomputes the profile.")
    parser.add_argument("--watchdog-state-refresh-seconds", type=float, default=3.0, help="How often V1 refreshes its cached watchdog state.")
    parser.add_argument("--watchdog-extreme-stale-seconds", type=float, default=30.0, help="Fail-safe flatten if the watchdog state is older than this.")
    parser.add_argument("--watchdog-sample-seconds", type=float, default=4.0, help="Sample window for the heuristic watchdog profiler.")
    parser.add_argument("--watchdog-poll-interval-seconds", type=float, default=0.35, help="Polling interval inside the heuristic watchdog profiler.")
    parser.add_argument("--watchdog-confidence-reduction-threshold", type=float, default=0.70, help="Confidence below this moves the watchdog to reduction_only.")
    parser.add_argument("--watchdog-confidence-flatten-threshold", type=float, default=0.55, help="Confidence below this moves the watchdog to flatten_only.")
    parser.add_argument("--clear-disabled-ticker", default="", help="Remove one ticker from the watchdog disable list and exit.")
    parser.add_argument("--clear-all-disabled-tickers", action="store_true", help="Clear the full watchdog disable list and exit.")
    parser.add_argument("--runtime-dir", default="", help="Directory for launcher status and the local control socket.")
    parser.add_argument("--session-store", default="", help="Directory containing the saved-session SQLite store.")
    parser.add_argument("--run-id", default="", help="Optional pending run prepared by the operations API.")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def clean_field_name(value: str) -> str:
    return str(value or "").strip()


def parse_integer_field(value: Optional[str], default_value: int) -> int:
    if value is None:
        return int(default_value)

    text = str(value).strip()
    if not text:
        return int(default_value)

    try:
        return int(float(text))
    except ValueError as exc:
        raise ValueError(f"Could not parse integer field value: {value!r}") from exc


def read_csv_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header row")

        cleaned_rows: List[Dict[str, str]] = []
        for row in reader:
            cleaned_row = {
                clean_field_name(key): (value.strip() if isinstance(value, str) else value)
                for key, value in row.items()
            }
            cleaned_rows.append(cleaned_row)

    return cleaned_rows


def choose_title(row: Dict[str, str], preferred_title_column: str) -> str:
    for candidate_column in (
        preferred_title_column,
        "name",
        "title",
        "Title",
        "SearchText",
        "subtitle",
        "Subtitle",
    ):
        if candidate_column and row.get(candidate_column):
            return str(row[candidate_column]).strip()
    return ""


def validate_requested_columns(
    first_row: Dict[str, str],
    *,
    ticker_column: str,
    yes_budget_column: str,
    no_budget_column: str,
) -> None:
    available_columns = set(first_row.keys())

    if ticker_column not in available_columns:
        available = ", ".join(sorted(available_columns))
        raise ValueError(
            f"Ticker column {ticker_column!r} was not found in the CSV. Available columns: {available}"
        )

    if yes_budget_column and yes_budget_column not in available_columns:
        raise ValueError(f"YES budget column {yes_budget_column!r} was not found in the CSV")

    if no_budget_column and no_budget_column not in available_columns:
        raise ValueError(f"NO budget column {no_budget_column!r} was not found in the CSV")


def build_launch_picks(
    *,
    rows: Iterable[Dict[str, str]],
    ticker_column: str,
    title_column: str,
    default_yes_budget_cents: int,
    default_no_budget_cents: int,
    yes_budget_column: str,
    no_budget_column: str,
    max_bots: int,
) -> List[LaunchPick]:
    launch_picks: List[LaunchPick] = []
    seen_tickers: set[str] = set()

    for row in rows:
        ticker = str(row.get(ticker_column, "")).strip()
        if not ticker or ticker in seen_tickers:
            continue

        yes_budget_cents = (
            parse_integer_field(row.get(yes_budget_column), default_yes_budget_cents)
            if yes_budget_column
            else int(default_yes_budget_cents)
        )
        no_budget_cents = (
            parse_integer_field(row.get(no_budget_column), default_no_budget_cents)
            if no_budget_column
            else int(default_no_budget_cents)
        )

        if yes_budget_cents < 0 or no_budget_cents < 0:
            raise ValueError(
                f"Ticker {ticker} has a negative budget. YES={yes_budget_cents}, NO={no_budget_cents}"
            )

        launch_picks.append(
            LaunchPick(
                ticker=ticker,
                title=choose_title(row, title_column),
                yes_budget_cents=yes_budget_cents,
                no_budget_cents=no_budget_cents,
                raw_row=row,
            )
        )
        seen_tickers.add(ticker)

        if max_bots > 0 and len(launch_picks) >= max_bots:
            break

    return launch_picks


# ---------------------------------------------------------------------------
# Kalshi position lookup
# ---------------------------------------------------------------------------

class KalshiPositionClient:
    def __init__(self, *, api_key_id: str, private_key_path: str, use_demo: bool, subaccount: int) -> None:
        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.use_demo = bool(use_demo)
        self.subaccount = int(subaccount)
        self.host = "https://external-api.demo.kalshi.co" if self.use_demo else "https://external-api.kalshi.com"
        self.api_prefix = "/trade-api/v2"
        self.session = requests.Session()

        with open(self.private_key_path, "rb") as private_key_file:
            self.private_key = serialization.load_pem_private_key(private_key_file.read(), password=None)

    def sign_message(self, message_bytes: bytes) -> str:
        signature = self.private_key.sign(
            message_bytes,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        timestamp_ms = str(int(time.time() * 1000))
        path_without_query = path.split("?")[0]
        signed_message = f"{timestamp_ms}{method.upper()}{path_without_query}".encode("utf-8")
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": self.sign_message(signed_message),
        }

    def get_position_count_units(self, market_ticker: str) -> int:
        path = f"{self.api_prefix}/portfolio/positions"
        response = self.session.get(
            self.host + path,
            headers=self._headers("GET", path),
            params={
                "ticker": market_ticker,
                "subaccount": self.subaccount,
                "limit": 1,
            },
            timeout=15,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"GET positions failed {response.status_code}: {response.text[:500]}")
        payload = response.json() if response.text.strip() else {}
        market_positions = payload.get("market_positions") or []
        if not market_positions:
            return 0
        position_payload = market_positions[0]
        if position_payload.get("position_fp") not in (None, ""):
            return int(round(float(position_payload["position_fp"]) * 100))
        if position_payload.get("position") not in (None, ""):
            return int(position_payload["position"]) * 100
        return 0

    def get_market_bid_cents(self, market_ticker: str) -> tuple[Optional[int], Optional[int]]:
        path = f"{self.api_prefix}/markets/{market_ticker}"
        response = self.session.get(
            self.host + path,
            headers=self._headers("GET", path),
            timeout=15,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"GET market failed {response.status_code}: {response.text[:500]}")
        payload = response.json() if response.text.strip() else {}
        market = payload.get("market") or {}

        def pick_cents(dollars_field: str, cents_field: str) -> Optional[int]:
            value = market.get(dollars_field)
            if value not in (None, ""):
                return int(round(float(value) * 100))
            value = market.get(cents_field)
            if value not in (None, ""):
                return int(value)
            return None

        yes_bid_cents = pick_cents("yes_bid_dollars", "yes_bid")
        no_bid_cents = pick_cents("no_bid_dollars", "no_bid")
        return yes_bid_cents, no_bid_cents


# ---------------------------------------------------------------------------
# Launch helpers
# ---------------------------------------------------------------------------

def resolve_credentials(arguments: argparse.Namespace) -> tuple[Optional[str], Optional[str]]:
    api_key_id = arguments.api_key_id or os.getenv("KALSHI_API_KEY_ID") or os.getenv("API_KEY_ID")
    private_key_path = arguments.private_key or os.getenv("KALSHI_PRIVATE_KEY_PATH") or os.getenv("PRIVATE_KEY_PATH")
    return api_key_id, private_key_path


def run_screener_once(screener_script: Path, screener_output: Path) -> None:
    subprocess.run(
        [sys.executable, str(screener_script), "--output", str(screener_output)],
        check=True,
    )


def redacted_command(command: List[str]) -> str:
    redacted_parts: List[str] = []
    skip_next = False

    for token in command:
        if skip_next:
            skip_next = False
            continue
        if token in {"--api-key-id", "--private-key"}:
            redacted_parts.append(token)
            redacted_parts.append("<redacted>")
            skip_next = True
        else:
            redacted_parts.append(token)

    return " ".join(redacted_parts)


def print_launch_plan(picks: List[LaunchPick], *, header: str = "=== Launch plan ===") -> None:
    print(f"\n{header}")
    for index, pick in enumerate(picks, start=1):
        extra_parts: List[str] = []

        if "Quote YES bid(c)" in pick.raw_row:
            extra_parts.append(f"quote_yes={pick.raw_row.get('Quote YES bid(c)')}")
        if "Quote NO bid(c)" in pick.raw_row:
            extra_parts.append(f"quote_no={pick.raw_row.get('Quote NO bid(c)')}")
        if "Quoted edge(c)" in pick.raw_row:
            extra_parts.append(f"edge={pick.raw_row.get('Quoted edge(c)')}c")

        extra_text = " | " + " | ".join(extra_parts) if extra_parts else ""
        print(
            f"{index}. {pick.ticker} | YES=${pick.yes_budget_cents / 100:.2f} | "
            f"NO=${pick.no_budget_cents / 100:.2f} | {pick.title}{extra_text}"
        )


def build_child_command(
    *,
    python_executable: str,
    bot_script_path: Path,
    pick: LaunchPick,
    use_demo: bool,
    dry_run: bool,
    subaccount: Optional[int],
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    pass_credentials_via_cli: bool,
    watchdog_state_file: Optional[Path],
    watchdog_state_refresh_seconds: float,
    watchdog_extreme_stale_seconds: float,
    control_socket: Optional[Path] = None,
    settings_file: Optional[Path] = None,
) -> List[str]:
    command: List[str] = [
        python_executable,
        str(bot_script_path),
        "--ticker",
        pick.ticker,
        "--yes-budget-cents",
        str(pick.yes_budget_cents),
        "--no-budget-cents",
        str(pick.no_budget_cents),
    ]
    if settings_file is not None:
        command.extend(["--settings-file", str(settings_file)])

    if use_demo:
        command.append("--use-demo")
    if dry_run:
        command.append("--dry-run")
    if subaccount is not None:
        command.extend(["--subaccount", str(subaccount)])

    if pass_credentials_via_cli:
        if api_key_id:
            command.extend(["--api-key-id", api_key_id])
        if private_key_path:
            command.extend(["--private-key", private_key_path])

    if watchdog_state_file is not None:
        command.extend([
            "--watchdog-state-file",
            str(watchdog_state_file),
            "--watchdog-refresh-seconds",
            str(watchdog_state_refresh_seconds),
            "--watchdog-extreme-stale-seconds",
            str(watchdog_extreme_stale_seconds),
        ])
    if control_socket is not None:
        command.extend(["--control-socket", str(control_socket)])

    return command


def build_watchdog_command(
    *,
    python_executable: str,
    runner_script_path: Path,
    profiler_script_path: Path,
    ticker: str,
    state_file: Path,
    interval_seconds: float,
    sample_seconds: float,
    poll_interval_seconds: float,
    confidence_reduction_threshold: float,
    confidence_flatten_threshold: float,
    use_demo: bool,
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    pass_credentials_via_cli: bool,
) -> List[str]:
    command: List[str] = [
        python_executable,
        str(runner_script_path),
        "--ticker",
        ticker,
        "--state-file",
        str(state_file),
        "--profiler-script",
        str(profiler_script_path),
        "--interval-seconds",
        str(interval_seconds),
        "--sample-seconds",
        str(sample_seconds),
        "--poll-interval-seconds",
        str(poll_interval_seconds),
        "--confidence-reduction-threshold",
        str(confidence_reduction_threshold),
        "--confidence-flatten-threshold",
        str(confidence_flatten_threshold),
    ]
    if use_demo:
        command.append("--use-demo")
    if pass_credentials_via_cli:
        if api_key_id:
            command.extend(["--api-key-id", api_key_id])
        if private_key_path:
            command.extend(["--private-key", private_key_path])
    return command


def spawn_single_bot(
    *,
    pick: LaunchPick,
    slot_index: int,
    bot_script_path: Path,
    logs_directory: Path,
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    use_demo: bool,
    dry_run: bool,
    subaccount: Optional[int],
    pass_credentials_via_cli: bool,
    watchdog_state_file: Optional[Path],
    watchdog_runner_script_path: Optional[Path],
    watchdog_profiler_script_path: Optional[Path],
    watchdog_interval_seconds: float,
    watchdog_state_refresh_seconds: float,
    watchdog_extreme_stale_seconds: float,
    watchdog_sample_seconds: float,
    watchdog_poll_interval_seconds: float,
    watchdog_confidence_reduction_threshold: float,
    watchdog_confidence_flatten_threshold: float,
    control_socket: Optional[Path] = None,
    session_configuration: Optional[Dict[str, object]] = None,
    bot_artifacts_root: Optional[Path] = None,
) -> Optional[ChildProcess]:
    if dry_run:
        return None

    logs_directory.mkdir(parents=True, exist_ok=True)
    bot_working_directory = bot_script_path.parent.resolve()

    settings_file: Optional[Path] = None
    if session_configuration is not None and bot_artifacts_root is not None:
        from session_config import bot_settings_payload
        market_artifacts = bot_artifacts_root / safe_ticker_filename(pick.ticker)
        market_artifacts.mkdir(parents=True, exist_ok=True)
        settings_file = market_artifacts / "settings.json"
        settings_payload = bot_settings_payload(
            session_configuration,
            # Per-market-class overrides (ScreenerPick.settings_overrides);
            # legacy LaunchPick rows carry none.
            overrides=tuple(getattr(pick, "settings_overrides", ()) or ()),
            market_ticker=pick.ticker,
            yes_budget_cents=pick.yes_budget_cents,
            no_budget_cents=pick.no_budget_cents,
            watchdog_state_file=watchdog_state_file,
            telemetry_sqlite_path=market_artifacts / "telemetry.sqlite3",
            pnl_tracker_path=bot_artifacts_root.parent / "pnl_tracker.jsonl",
        )
        settings_file.write_text(json.dumps(settings_payload, indent=2) + "\n", encoding="utf-8")

    child_command = build_child_command(
        python_executable=sys.executable,
        bot_script_path=bot_script_path,
        pick=pick,
        use_demo=use_demo,
        dry_run=dry_run,
        subaccount=subaccount,
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        pass_credentials_via_cli=pass_credentials_via_cli,
        watchdog_state_file=watchdog_state_file,
        watchdog_state_refresh_seconds=watchdog_state_refresh_seconds,
        watchdog_extreme_stale_seconds=watchdog_extreme_stale_seconds,
        control_socket=control_socket,
        settings_file=settings_file,
    )

    child_environment = dict(os.environ)
    child_environment["PYTHONUNBUFFERED"] = "1"

    if not pass_credentials_via_cli:
        if api_key_id:
            child_environment["KALSHI_API_KEY_ID"] = api_key_id
        if private_key_path:
            child_environment["KALSHI_PRIVATE_KEY_PATH"] = private_key_path

    safe_ticker = pick.ticker.replace("/", "_").replace("\\", "_")
    log_path = logs_directory / f"{safe_ticker}.log"
    log_handle = log_path.open("a", encoding="utf-8", buffering=1)
    log_handle.write(f"\n=== START {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
    log_handle.write("CMD: " + redacted_command(child_command) + "\n")
    if subaccount is not None:
        log_handle.write(f"SUBACCOUNT: {subaccount}\n")
    log_handle.flush()

    watchdog_process = None
    watchdog_log_path: Optional[Path] = None
    watchdog_log_handle: Optional[IO[str]] = None
    if watchdog_state_file is not None and watchdog_runner_script_path is not None and watchdog_profiler_script_path is not None:
        watchdog_log_path = logs_directory / f"{safe_ticker}.watchdog.log"
        watchdog_log_handle = watchdog_log_path.open("a", encoding="utf-8", buffering=1)
        watchdog_log_handle.write(f"\n=== START {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        watchdog_command = build_watchdog_command(
            python_executable=sys.executable,
            runner_script_path=watchdog_runner_script_path,
            profiler_script_path=watchdog_profiler_script_path,
            ticker=pick.ticker,
            state_file=watchdog_state_file,
            interval_seconds=watchdog_interval_seconds,
            sample_seconds=watchdog_sample_seconds,
            poll_interval_seconds=watchdog_poll_interval_seconds,
            confidence_reduction_threshold=watchdog_confidence_reduction_threshold,
            confidence_flatten_threshold=watchdog_confidence_flatten_threshold,
            use_demo=use_demo,
            api_key_id=api_key_id,
            private_key_path=private_key_path,
            pass_credentials_via_cli=pass_credentials_via_cli,
        )
        watchdog_log_handle.write("CMD: " + redacted_command(watchdog_command) + "\n")
        watchdog_log_handle.flush()
        watchdog_process = subprocess.Popen(
            watchdog_command,
            cwd=str(bot_working_directory),
            stdout=watchdog_log_handle,
            stderr=subprocess.STDOUT,
            env=child_environment,
        )

    print(f"Spawning {pick.ticker} -> {log_path}")
    process = subprocess.Popen(
        child_command,
        cwd=str(bot_working_directory),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env=child_environment,
    )

    return ChildProcess(
        ticker=pick.ticker,
        process=process,
        log_path=log_path,
        log_handle=log_handle,
        slot_index=slot_index,
        pick=pick,
        watchdog_process=watchdog_process,
        watchdog_log_path=watchdog_log_path,
        watchdog_log_handle=watchdog_log_handle,
        watchdog_state_file=watchdog_state_file,
        control_socket=control_socket,
    )


def spawn_bots(
    *,
    picks: List[LaunchPick],
    bot_script_path: Path,
    logs_directory: Path,
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    use_demo: bool,
    dry_run: bool,
    subaccount: Optional[int],
    launch_delay_seconds: float,
    pass_credentials_via_cli: bool,
    watchdog_runner_script_path: Optional[Path],
    watchdog_profiler_script_path: Optional[Path],
    watchdog_state_dir: Path,
    watchdog_disable_file: Path,
    watchdog_interval_seconds: float,
    watchdog_state_refresh_seconds: float,
    watchdog_extreme_stale_seconds: float,
    watchdog_sample_seconds: float,
    watchdog_poll_interval_seconds: float,
    watchdog_confidence_reduction_threshold: float,
    watchdog_confidence_flatten_threshold: float,
    control_socket_directory: Optional[Path] = None,
    session_configuration: Optional[Dict[str, object]] = None,
    bot_artifacts_root: Optional[Path] = None,
) -> List[ChildProcess]:
    print_launch_plan(picks)

    if dry_run:
        print("\n(DRY RUN) No bot processes were started.")
        return []

    child_processes: List[ChildProcess] = []

    disable_list = load_disable_list(watchdog_disable_file)

    for slot_index, pick in enumerate(picks):
        if pick.ticker in disable_list:
            print(f"[WATCHDOG] skipping disabled ticker {pick.ticker} | reason={disable_list[pick.ticker].get('reason', 'unknown')}")
            continue

        watchdog_state_file = watchdog_state_dir / f"{safe_ticker_filename(pick.ticker)}.json"
        if not dry_run and watchdog_profiler_script_path is not None:
            try:
                profile = run_profiler_prestart(
                    profiler_script_path=watchdog_profiler_script_path,
                    ticker=pick.ticker,
                    api_key_id=api_key_id,
                    private_key_path=private_key_path,
                    use_demo=use_demo,
                    sample_seconds=watchdog_sample_seconds,
                    poll_interval_seconds=watchdog_poll_interval_seconds,
                    confidence_reduction_threshold=watchdog_confidence_reduction_threshold,
                    confidence_flatten_threshold=watchdog_confidence_flatten_threshold,
                )
                atomic_write_json(watchdog_state_file, profile)
                profile_mode = str(profile.get("mode") or "normal")
                effective_close_ms = profile.get("effective_close_time_ms")
                current_ms = int(time.time() * 1000)
                if profile_mode == "flatten_only" or (effective_close_ms not in (None, "") and int(effective_close_ms) <= current_ms):
                    disable_ticker(watchdog_disable_file, pick.ticker, str(profile.get("reason") or "prestart_flatten_only"), exit_code=61)
                    print(f"[WATCHDOG] not starting {pick.ticker}; prestart mode={profile_mode} reason={profile.get('reason')}")
                    continue
                if profile_mode == "reduction_only":
                    current_position_units = ticker_position_units(
                        ticker=pick.ticker,
                        api_key_id=api_key_id,
                        private_key_path=private_key_path,
                        use_demo=use_demo,
                        subaccount=subaccount,
                    )
                    if int(current_position_units) == 0:
                        print(f"[WATCHDOG] not starting {pick.ticker}; reduction_only with no inventory to unwind")
                        continue
            except Exception as exc:
                print(f"[WATCHDOG] failed prestart profile for {pick.ticker}: {exc}")
                continue

        child = spawn_single_bot(
            pick=pick,
            slot_index=slot_index,
            bot_script_path=bot_script_path,
            logs_directory=logs_directory,
            api_key_id=api_key_id,
            private_key_path=private_key_path,
            use_demo=use_demo,
            dry_run=dry_run,
            subaccount=subaccount,
            pass_credentials_via_cli=pass_credentials_via_cli,
            watchdog_state_file=watchdog_state_file,
            watchdog_runner_script_path=watchdog_runner_script_path,
            watchdog_profiler_script_path=watchdog_profiler_script_path,
            watchdog_interval_seconds=watchdog_interval_seconds,
            watchdog_state_refresh_seconds=watchdog_state_refresh_seconds,
            watchdog_extreme_stale_seconds=watchdog_extreme_stale_seconds,
            watchdog_sample_seconds=watchdog_sample_seconds,
            watchdog_poll_interval_seconds=watchdog_poll_interval_seconds,
            watchdog_confidence_reduction_threshold=watchdog_confidence_reduction_threshold,
            watchdog_confidence_flatten_threshold=watchdog_confidence_flatten_threshold,
            control_socket=(
                control_socket_directory / f"{safe_ticker_filename(pick.ticker)}.sock"
                if control_socket_directory is not None
                else None
            ),
            session_configuration=session_configuration,
            bot_artifacts_root=bot_artifacts_root,
        )
        if child is not None:
            child_processes.append(child)

        if launch_delay_seconds > 0:
            time.sleep(launch_delay_seconds)

    return child_processes


def close_child_log(child: ChildProcess) -> None:
    try:
        child.log_handle.flush()
        child.log_handle.close()
    except Exception:
        pass
    if child.watchdog_log_handle is not None:
        try:
            child.watchdog_log_handle.flush()
            child.watchdog_log_handle.close()
        except Exception:
            pass


def stop_children(children: List[ChildProcess]) -> None:
    if not children:
        return

    for child in children:
        try:
            child.process.send_signal(signal.SIGINT)
        except Exception:
            pass
        try:
            if child.watchdog_process is not None:
                child.watchdog_process.send_signal(signal.SIGINT)
        except Exception:
            pass

    time.sleep(5.0)

    for child in children:
        try:
            if child.process.poll() is None:
                child.process.terminate()
        except Exception:
            pass
        try:
            if child.watchdog_process is not None and child.watchdog_process.poll() is None:
                child.watchdog_process.terminate()
        except Exception:
            pass

    time.sleep(0.5)

    for child in children:
        try:
            if child.process.poll() is None:
                child.process.kill()
        except Exception:
            pass
        try:
            if child.watchdog_process is not None and child.watchdog_process.poll() is None:
                child.watchdog_process.kill()
        except Exception:
            pass

    for child in children:
        close_child_log(child)


def load_screen_picks(
    *,
    screen_file_path: Path,
    ticker_column: str,
    title_column: str,
    yes_budget_cents: int,
    no_budget_cents: int,
    yes_budget_column: str,
    no_budget_column: str,
    max_bots: int,
) -> List[LaunchPick]:
    rows = read_csv_rows(screen_file_path)
    if not rows:
        return []

    validate_requested_columns(
        rows[0],
        ticker_column=ticker_column,
        yes_budget_column=yes_budget_column,
        no_budget_column=no_budget_column,
    )

    return build_launch_picks(
        rows=rows,
        ticker_column=ticker_column,
        title_column=title_column,
        default_yes_budget_cents=yes_budget_cents,
        default_no_budget_cents=no_budget_cents,
        yes_budget_column=yes_budget_column,
        no_budget_column=no_budget_column,
        max_bots=max_bots,
    )


def merge_inventory_picks(
    *,
    screener_picks: List[LaunchPick],
    inventory_tickers: List[str],
    previous_pick_by_ticker: Dict[str, LaunchPick],
    default_yes_budget_cents: int,
    default_no_budget_cents: int,
) -> List[LaunchPick]:
    merged: List[LaunchPick] = []
    seen: set[str] = set()

    for pick in screener_picks:
        if pick.ticker in seen:
            continue
        merged.append(pick)
        seen.add(pick.ticker)

    for ticker in inventory_tickers:
        if ticker in seen:
            continue
        previous_pick = previous_pick_by_ticker.get(ticker)
        if previous_pick is not None:
            merged.append(previous_pick)
        else:
            merged.append(
                LaunchPick(
                    ticker=ticker,
                    title="Inventory carryover",
                    yes_budget_cents=int(default_yes_budget_cents),
                    no_budget_cents=int(default_no_budget_cents),
                    raw_row={"Ticker": ticker, "SearchText": "Inventory carryover"},
                )
            )
        seen.add(ticker)

    return merged


def lookup_inventory_tickers(
    *,
    tickers: List[str],
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    use_demo: bool,
    subaccount: Optional[int],
    previous_position_units_by_ticker: Optional[Dict[str, int]] = None,
    minimum_carryover_value_cents: float = 100.0,
) -> tuple[List[str], Dict[str, int], List[str]]:
    if not tickers or not api_key_id or not private_key_path:
        return [], {}, []

    client = KalshiPositionClient(
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        use_demo=use_demo,
        subaccount=(0 if subaccount is None else int(subaccount)),
    )

    previous_position_units_by_ticker = previous_position_units_by_ticker or {}
    inventory_tickers: List[str] = []
    position_units_by_ticker: Dict[str, int] = {}
    unknown_position_tickers: List[str] = []

    for ticker in tickers:
        last_exc: Optional[Exception] = None
        position_units: Optional[int] = None

        for attempt in range(4):
            try:
                position_units = client.get_position_count_units(ticker)
                break
            except Exception as exc:
                last_exc = exc
                message = str(exc).lower()

                retryable = (
                    "failed 429" in message
                    or "too_many_requests" in message
                    or "failed 500" in message
                    or "failed 502" in message
                    or "failed 503" in message
                    or "failed 504" in message
                )

                if retryable and attempt < 3:
                    sleep_seconds = min(8.0, 0.75 * (2 ** attempt))
                    print(f"[WARN] position lookup retry for {ticker} in {sleep_seconds:.2f}s | attempt={attempt + 1} | error={exc}")
                    time.sleep(sleep_seconds)
                    continue

                break

        if position_units is None:
            # Fail-safe: do NOT let a failed lookup cause us to drop / recycle the line.
            unknown_position_tickers.append(ticker)

            if ticker not in inventory_tickers:
                inventory_tickers.append(ticker)

            if ticker in previous_position_units_by_ticker:
                position_units_by_ticker[ticker] = int(previous_position_units_by_ticker[ticker])

            print(f"[WARN] position unknown for {ticker}; treating as carryover | error={last_exc}")
            continue

        position_units_by_ticker[ticker] = int(position_units)
        if int(position_units) == 0:
            continue

        try:
            yes_bid_cents, no_bid_cents = client.get_market_bid_cents(ticker)
        except Exception as exc:
            unknown_position_tickers.append(ticker)
            if ticker not in inventory_tickers:
                inventory_tickers.append(ticker)
            print(f"[WARN] market value unknown for {ticker}; treating as carryover | error={exc}")
            continue

        side_bid_cents = yes_bid_cents if int(position_units) > 0 else no_bid_cents
        if side_bid_cents is None:
            unknown_position_tickers.append(ticker)
            if ticker not in inventory_tickers:
                inventory_tickers.append(ticker)
            print(f"[WARN] side bid missing for {ticker}; treating as carryover")
            continue

        marked_value_cents = abs(int(position_units)) * float(side_bid_cents) / 100.0
        if marked_value_cents >= float(minimum_carryover_value_cents):
            inventory_tickers.append(ticker)

    return inventory_tickers, position_units_by_ticker, unknown_position_tickers

# ---------------------------------------------------------------------------
# Main monitoring / refresh loop
# ---------------------------------------------------------------------------

def monitor_and_refresh(
    children: List[ChildProcess],
    *,
    screen_file_path: Path,
    screener_script_path: Optional[Path],
    screener_output_path: Path,
    minimum_carryover_value_cents: float,
    bot_script_path: Path,
    logs_directory: Path,
    api_key_id: Optional[str],
    private_key_path: Optional[str],
    use_demo: bool,
    dry_run: bool,
    subaccount: Optional[int],
    launch_delay_seconds: float,
    pass_credentials_via_cli: bool,
    ticker_column: str,
    title_column: str,
    yes_budget_cents: int,
    no_budget_cents: int,
    yes_budget_column: str,
    no_budget_column: str,
    max_bots: int,
    refresh_interval_seconds: float,
    poll_seconds: float,
    watchdog_runner_script_path: Optional[Path],
    watchdog_profiler_script_path: Optional[Path],
    watchdog_state_dir: Path,
    watchdog_disable_file: Path,
    watchdog_interval_seconds: float,
    watchdog_state_refresh_seconds: float,
    watchdog_extreme_stale_seconds: float,
    watchdog_sample_seconds: float,
    watchdog_poll_interval_seconds: float,
    watchdog_confidence_reduction_threshold: float,
    watchdog_confidence_flatten_threshold: float,
    runtime_dir: Path,
) -> int:
    print("\nBots running. Press Ctrl+C to stop them all.")
    next_refresh_at = (time.time() + refresh_interval_seconds) if refresh_interval_seconds > 0 else None
    last_known_position_units_by_ticker: Dict[str, int] = {}
    started_at_ms = int(time.time() * 1000)
    control_requests: "queue.Queue[ControlRequest]" = queue.Queue()
    shutdown_requested = threading.Event()
    status_lock = threading.Lock()
    latest_status: Dict[str, object] = {}
    status_path = runtime_dir / "launcher_status.json"
    socket_path = runtime_dir / "launcher.sock"
    pending_action: Optional[Dict[str, object]] = None
    last_error: Optional[str] = None
    refresh_requests: List[ControlRequest] = []

    def build_status(lifecycle: str = "running") -> Dict[str, object]:
        bot_rows: List[Dict[str, object]] = []
        watchdog_modes: Dict[str, int] = {}
        for child in list(children):
            watchdog = load_json_file(child.watchdog_state_file) if child.watchdog_state_file else {}
            mode = str(watchdog.get("mode") or "unknown")
            watchdog_modes[mode] = watchdog_modes.get(mode, 0) + 1
            bot_rows.append({
                "ticker": child.ticker,
                "title": child.pick.title,
                "slotIndex": child.slot_index,
                "pid": child.process.pid,
                "watchdogPid": child.watchdog_process.pid if child.watchdog_process else None,
                "botRunning": child.process.poll() is None,
                "watchdogRunning": child.watchdog_process.poll() is None if child.watchdog_process else False,
                "yesBudgetCents": child.pick.yes_budget_cents,
                "noBudgetCents": child.pick.no_budget_cents,
                "logPath": str(child.log_path),
                "watchdogLogPath": str(child.watchdog_log_path) if child.watchdog_log_path else None,
                "watchdogMode": mode,
                "watchdogConfidence": watchdog.get("confidence"),
                "watchdogReason": watchdog.get("reason"),
                "watchdogUpdatedAtMs": watchdog.get("runner_updated_at_ms") or watchdog.get("generated_at_ms"),
            })
        disabled = load_disable_list(watchdog_disable_file)
        now_ms = int(time.time() * 1000)
        return {
            "schemaVersion": STATUS_SCHEMA_VERSION,
            "generatedAt": now_ms,
            "launcher": {
                "pid": os.getpid(),
                "environment": "demo" if use_demo else "production",
                "lifecycle": lifecycle,
                "startedAt": started_at_ms,
                "heartbeatAt": now_ms,
                "nextRefreshAt": int(next_refresh_at * 1000) if next_refresh_at is not None else None,
                "lastError": last_error,
                "pendingAction": pending_action,
            },
            "counts": {
                "activeBots": sum(1 for item in bot_rows if item["botRunning"]),
                "configuredBots": len(bot_rows),
                "disabledTickers": len(disabled),
                "watchdogModes": watchdog_modes,
            },
            "bots": bot_rows,
            "disabled": disabled,
        }

    def publish_status(lifecycle: str = "running") -> Dict[str, object]:
        nonlocal latest_status
        snapshot = build_status(lifecycle)
        with status_lock:
            latest_status = snapshot
        atomic_write_json(status_path, snapshot)
        return snapshot

    def status_provider() -> Dict[str, object]:
        with status_lock:
            return dict(latest_status)

    def request_shutdown(signum: int, _frame: object) -> None:
        print(f"\nReceived signal {signum}; stopping bots gracefully...")
        shutdown_requested.set()

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    # Windows: the fleet runs in its own process group so a stray console Ctrl+C
    # (e.g. restarting the dashboard that spawned it) cannot kill live trading.
    # CTRL_BREAK is then the only signal that reaches the group, so it must run
    # the same graceful shutdown as SIGINT rather than hard-exiting.
    previous_sigbreak = None
    if hasattr(signal, "SIGBREAK"):
        previous_sigbreak = signal.getsignal(signal.SIGBREAK)
        signal.signal(signal.SIGBREAK, request_shutdown)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    publish_status("starting")
    control_server = ControlServer(socket_path, control_requests, status_provider)
    control_server.start()
    publish_status("running")

    try:
        while not shutdown_requested.is_set():
            while True:
                try:
                    control = control_requests.get_nowait()
                except queue.Empty:
                    break
                pending_action = {
                    "requestId": control.request_id,
                    "action": control.action,
                    "ticker": control.ticker,
                    "receivedAt": control.received_at_ms,
                }
                try:
                    known_tickers = {child.ticker for child in children}
                    try:
                        known_tickers.update(
                            pick.ticker for pick in load_screen_picks(
                                screen_file_path=screener_output_path,
                                ticker_column=ticker_column,
                                title_column=title_column,
                                yes_budget_cents=yes_budget_cents,
                                no_budget_cents=no_budget_cents,
                                yes_budget_column=yes_budget_column,
                                no_budget_column=no_budget_column,
                                max_bots=0,
                            )
                        )
                    except Exception:
                        pass
                    known_tickers.update(load_disable_list(watchdog_disable_file).keys())
                    if control.ticker and control.ticker not in known_tickers:
                        raise ValueError(f"unknown ticker: {control.ticker}")
                    if control.action == "disable_ticker":
                        assert control.ticker is not None
                        disable_ticker(watchdog_disable_file, control.ticker, "operator_ui")
                        matches = [child for child in children if child.ticker == control.ticker]
                        stop_children(matches)
                        for child in matches:
                            if child in children:
                                children.remove(child)
                        control.result = {"ok": True, "result": {"ticker": control.ticker, "disabled": True, "inventoryRetained": True}}
                        control.done.set()
                        pending_action = None
                    elif control.action == "enable_ticker":
                        assert control.ticker is not None
                        removed = clear_disabled_ticker(watchdog_disable_file, control.ticker)
                        next_refresh_at = time.time()
                        refresh_requests.append(control)
                        control.result = {"ok": True, "result": {"ticker": control.ticker, "disabled": False, "wasDisabled": removed}}
                    elif control.action == "refresh":
                        next_refresh_at = time.time()
                        refresh_requests.append(control)
                    elif control.action == "shutdown":
                        control.result = {"ok": True, "result": {"shutdownRequested": True}}
                        control.done.set()
                        pending_action = None
                        shutdown_requested.set()
                    else:
                        raise ValueError(f"unsupported action: {control.action}")
                except Exception as exc:
                    last_error = str(exc)
                    control.result = {"ok": False, "code": "control_failed", "message": str(exc)}
                    control.done.set()
                    pending_action = None
                publish_status()

            # Print and drop dead children between refreshes.
            for child in list(children):
                return_code = child.process.poll()
                if return_code is None:
                    continue
                if child.watchdog_process is not None:
                    try:
                        if child.watchdog_process.poll() is None:
                            child.watchdog_process.terminate()
                    except Exception:
                        pass
                print(f"[WARN] {child.ticker} exited with code {return_code} | log={child.log_path}")
                last_error = f"{child.ticker} exited with code {return_code}"
                if int(return_code) in WATCHDOG_EXIT_CODES:
                    disable_ticker(watchdog_disable_file, child.ticker, f"watchdog_exit_{int(return_code)}", exit_code=int(return_code))
                    print(f"[WATCHDOG] permanently disabled {child.ticker} after exit code {int(return_code)}")
                close_child_log(child)
                children.remove(child)

            if next_refresh_at is not None and time.time() >= next_refresh_at:
                previous_pick_by_ticker = {child.ticker: child.pick for child in children}
                current_tickers = list(previous_pick_by_ticker.keys())
                inventory_tickers, position_units_by_ticker, unknown_position_tickers = lookup_inventory_tickers(
                    minimum_carryover_value_cents=minimum_carryover_value_cents,
                    tickers=current_tickers,
                    api_key_id=api_key_id,
                    private_key_path=private_key_path,
                    use_demo=use_demo,
                    subaccount=subaccount,
                    previous_position_units_by_ticker=last_known_position_units_by_ticker,
                )
                last_known_position_units_by_ticker.update(position_units_by_ticker)

                if inventory_tickers:
                    print("\nInventory carryover tickers: " + ", ".join(sorted(inventory_tickers)))
                else:
                    print("\nInventory carryover tickers: none")

                if unknown_position_tickers:
                    print("[WARN] inventory status unknown; kept alive as carryover: " + ", ".join(sorted(unknown_position_tickers)))
                stop_children(children)
                children.clear()

                if screener_script_path is not None:
                    print("Rerunning screener...")
                    run_screener_once(screener_script_path, screener_output_path)

                screener_picks = load_screen_picks(
                    screen_file_path=screener_output_path,
                    ticker_column=ticker_column,
                    title_column=title_column,
                    yes_budget_cents=yes_budget_cents,
                    no_budget_cents=no_budget_cents,
                    yes_budget_column=yes_budget_column,
                    no_budget_column=no_budget_column,
                    max_bots=max_bots,
                )
                merged_picks = merge_inventory_picks(
                    screener_picks=screener_picks,
                    inventory_tickers=inventory_tickers,
                    previous_pick_by_ticker=previous_pick_by_ticker,
                    default_yes_budget_cents=yes_budget_cents,
                    default_no_budget_cents=no_budget_cents,
                )

                if not merged_picks:
                    print("[WARN] refresh produced no markets to launch")
                else:
                    print_launch_plan(merged_picks, header="=== Refresh launch plan ===")
                    children.extend(
                        spawn_bots(
                            picks=merged_picks,
                            bot_script_path=bot_script_path,
                            logs_directory=logs_directory,
                            api_key_id=api_key_id,
                            private_key_path=private_key_path,
                            use_demo=use_demo,
                            dry_run=dry_run,
                            subaccount=subaccount,
                            launch_delay_seconds=launch_delay_seconds,
                            pass_credentials_via_cli=pass_credentials_via_cli,
                            watchdog_runner_script_path=watchdog_runner_script_path,
                            watchdog_profiler_script_path=watchdog_profiler_script_path,
                            watchdog_state_dir=watchdog_state_dir,
                            watchdog_disable_file=watchdog_disable_file,
                            watchdog_interval_seconds=watchdog_interval_seconds,
                            watchdog_state_refresh_seconds=watchdog_state_refresh_seconds,
                            watchdog_extreme_stale_seconds=watchdog_extreme_stale_seconds,
                            watchdog_sample_seconds=watchdog_sample_seconds,
                            watchdog_poll_interval_seconds=watchdog_poll_interval_seconds,
                            watchdog_confidence_reduction_threshold=watchdog_confidence_reduction_threshold,
                            watchdog_confidence_flatten_threshold=watchdog_confidence_flatten_threshold,
                        )
                    )
                next_refresh_at = (time.time() + refresh_interval_seconds) if refresh_interval_seconds > 0 else None
                for control in refresh_requests:
                    if not control.result:
                        control.result = {"ok": True, "result": {"refreshed": True, "activeBots": len(children)}}
                    else:
                        result = dict(control.result.get("result") or {})
                        result.update({"refreshed": True, "activeBots": len(children)})
                        control.result["result"] = result
                    control.done.set()
                refresh_requests.clear()
                pending_action = None

            if not children and next_refresh_at is None:
                print("All bots exited.")
                return 0

            publish_status()
            shutdown_requested.wait(min(poll_seconds, 2.0))

    except KeyboardInterrupt:
        shutdown_requested.set()
    except Exception as exc:
        last_error = str(exc)
        raise
    finally:
        control_server.stop()
        for control in refresh_requests:
            control.result = {"ok": False, "code": "launcher_stopping", "message": "launcher stopped before the command completed"}
            control.done.set()
        publish_status("stopping")
        stop_children(children)
        children.clear()
        publish_status("stopped")
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        if previous_sigbreak is not None:
            signal.signal(signal.SIGBREAK, previous_sigbreak)
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    arguments = parse_args()

    session_store = None
    session_run = None
    if arguments.session_store:
        from session_config import apply_configuration_to_arguments
        from session_store import SessionStore
        session_store = SessionStore(Path(arguments.session_store))
        session_run = session_store.claim_run(arguments.run_id or None)
        try:
            apply_configuration_to_arguments(arguments, session_run["configuration"])
        except ValueError as exc:
            session_store.finish_run(
                session_run["id"],
                "failed",
                metrics=session_run.get("metrics") or {},
                error=f"invalid session configuration: {exc}",
            )
            print(f"ERROR: invalid session configuration: {exc}")
            return 2

    screen_file_path = Path(arguments.screen_file).expanduser().resolve()
    bot_script_path = Path(arguments.bot_script).expanduser().resolve()
    logs_directory = (
        Path(arguments.logs_dir).expanduser().resolve()
        if arguments.logs_dir
        else bot_script_path.parent.resolve() / "logs"
    )
    screener_script_path = (
        Path(arguments.screener_script).expanduser().resolve()
        if arguments.screener_script
        else None
    )
    screener_output_path = (
        Path(arguments.screener_output).expanduser().resolve()
        if arguments.screener_output
        else screen_file_path
    )
    watchdog_runner_script_path = (
        Path(arguments.watchdog_runner_script).expanduser().resolve()
        if arguments.watchdog_runner_script
        else bot_script_path.parent.resolve() / "market_watchdog_runner.py"
    )
    watchdog_profiler_script_path = (
        Path(arguments.watchdog_profiler_script).expanduser().resolve()
        if arguments.watchdog_profiler_script
        else bot_script_path.parent.resolve() / "market_risk_profiler.py"
    )
    watchdog_state_dir = (
        Path(arguments.watchdog_state_dir).expanduser().resolve()
        if arguments.watchdog_state_dir
        else bot_script_path.parent.resolve() / "watchdog_state"
    )
    watchdog_disable_file = (
        Path(arguments.watchdog_disable_file).expanduser().resolve()
        if arguments.watchdog_disable_file
        else bot_script_path.parent.resolve() / "watchdog_disable_list.json"
    )
    runtime_dir = (
        Path(arguments.runtime_dir).expanduser().resolve()
        if arguments.runtime_dir
        else bot_script_path.parent.resolve() / "runtime"
    )

    if not bot_script_path.exists():
        print(f"ERROR: bot script not found: {bot_script_path}")
        return 2
    if screener_script_path is not None and not screener_script_path.exists():
        print(f"ERROR: screener script not found: {screener_script_path}")
        return 2
    if not watchdog_runner_script_path.exists():
        print(f"ERROR: watchdog runner script not found: {watchdog_runner_script_path}")
        return 2
    if not watchdog_profiler_script_path.exists():
        print(f"ERROR: watchdog profiler script not found: {watchdog_profiler_script_path}")
        return 2

    max_bots = int(arguments.max_bots)
    if not 1 <= max_bots <= MAX_CONCURRENT_BOTS:
        print(
            f"ERROR: --max-bots must be between 1 and {MAX_CONCURRENT_BOTS}; "
            "unbounded fleets can exhaust system memory"
        )
        if session_store is not None and session_run is not None:
            session_store.finish_run(
                session_run["id"],
                "failed",
                metrics=session_run.get("metrics") or {},
                error=f"unsafe max-bots value: {max_bots}",
            )
        return 2

    if arguments.launch_delay_seconds < 0:
        print("ERROR: --launch-delay-seconds must be >= 0")
        return 2
    if arguments.refresh_interval_seconds < 0:
        print("ERROR: --refresh-interval-seconds must be >= 0")
        return 2
    if arguments.poll_seconds <= 0:
        print("ERROR: --poll-seconds must be > 0")
        return 2
    if arguments.subaccount is not None and arguments.subaccount < 0:
        print("ERROR: --subaccount must be >= 0")
        return 2

    if arguments.clear_all_disabled_tickers:
        write_disable_list(watchdog_disable_file, {})
        print(f"Cleared watchdog disable list: {watchdog_disable_file}")
        return 0
    if arguments.clear_disabled_ticker:
        removed = clear_disabled_ticker(watchdog_disable_file, arguments.clear_disabled_ticker)
        if removed:
            print(f"Cleared disabled ticker: {arguments.clear_disabled_ticker}")
        else:
            print(f"Ticker was not disabled: {arguments.clear_disabled_ticker}")
        return 0

    api_key_id, private_key_path = resolve_credentials(arguments)

    if not arguments.dry_run:
        if not api_key_id:
            print("ERROR: no API key ID provided. Use --api-key-id or set KALSHI_API_KEY_ID.")
            return 2
        if not private_key_path:
            print("ERROR: no private key path provided. Use --private-key or set KALSHI_PRIVATE_KEY_PATH.")
            return 2
        if not Path(private_key_path).expanduser().exists():
            print(f"ERROR: private key file not found: {Path(private_key_path).expanduser()}")
            return 2

    from launcher import Launcher

    return asyncio.run(
        Launcher(
            arguments,
            api_key_id=api_key_id,
            private_key_path=private_key_path,
            session_store=session_store,
            session_run=session_run,
        ).run()
    )



if __name__ == "__main__":
    raise SystemExit(main())
