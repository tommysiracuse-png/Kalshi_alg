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
import base64
import csv
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, IO, List, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


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
        "--bot-script",
        required=True,
        help="Path to the single-market bot script, for example V1.py.",
    )
    parser.add_argument(
        "--max-bots",
        type=int,
        default=3,
        help="Maximum number of screener rows / bots to launch. Use 0 or a negative value to launch every screener row.",
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
        self.host = "https://demo-api.kalshi.co" if self.use_demo else "https://api.elections.kalshi.com"
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
) -> Optional[ChildProcess]:
    if dry_run:
        return None

    logs_directory.mkdir(parents=True, exist_ok=True)
    bot_working_directory = bot_script_path.parent.resolve()

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
) -> List[ChildProcess]:
    print_launch_plan(picks)

    if dry_run:
        print("\n(DRY RUN) No bot processes were started.")
        return []

    child_processes: List[ChildProcess] = []

    for slot_index, pick in enumerate(picks):
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


def stop_children(children: List[ChildProcess]) -> None:
    if not children:
        return

    for child in children:
        try:
            child.process.send_signal(signal.SIGINT)
        except Exception:
            pass

    time.sleep(1.0)

    for child in children:
        try:
            if child.process.poll() is None:
                child.process.terminate()
        except Exception:
            pass

    time.sleep(0.5)

    for child in children:
        try:
            if child.process.poll() is None:
                child.process.kill()
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
) -> List[str]:
    if not tickers or not api_key_id or not private_key_path:
        return []

    client = KalshiPositionClient(
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        use_demo=use_demo,
        subaccount=(0 if subaccount is None else int(subaccount)),
    )

    inventory_tickers: List[str] = []
    for ticker in tickers:
        try:
            position_units = client.get_position_count_units(ticker)
        except Exception as exc:
            print(f"[WARN] failed to check position for {ticker}: {exc}")
            continue
        if position_units != 0:
            inventory_tickers.append(ticker)
    return inventory_tickers


# ---------------------------------------------------------------------------
# Main monitoring / refresh loop
# ---------------------------------------------------------------------------

def monitor_and_refresh(
    children: List[ChildProcess],
    *,
    screen_file_path: Path,
    screener_script_path: Optional[Path],
    screener_output_path: Path,
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
) -> int:
    if not children:
        return 0

    print("\nBots running. Press Ctrl+C to stop them all.")
    next_refresh_at = (time.time() + refresh_interval_seconds) if refresh_interval_seconds > 0 else None

    try:
        while True:
            # Print and drop dead children between refreshes.
            for child in list(children):
                return_code = child.process.poll()
                if return_code is None:
                    continue
                print(f"[WARN] {child.ticker} exited with code {return_code} | log={child.log_path}")
                close_child_log(child)
                children.remove(child)

            if next_refresh_at is not None and time.time() >= next_refresh_at:
                previous_pick_by_ticker = {child.ticker: child.pick for child in children}
                current_tickers = list(previous_pick_by_ticker.keys())
                inventory_tickers = lookup_inventory_tickers(
                    tickers=current_tickers,
                    api_key_id=api_key_id,
                    private_key_path=private_key_path,
                    use_demo=use_demo,
                    subaccount=subaccount,
                )

                if inventory_tickers:
                    print("\nInventory carryover tickers: " + ", ".join(sorted(inventory_tickers)))
                else:
                    print("\nInventory carryover tickers: none")

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
                        )
                    )
                next_refresh_at = time.time() + refresh_interval_seconds

            if not children and next_refresh_at is None:
                print("All bots exited.")
                return 0

            time.sleep(poll_seconds)

    except KeyboardInterrupt:
        print("\nStopping bots...")
        stop_children(children)
        return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    arguments = parse_args()

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

    if not bot_script_path.exists():
        print(f"ERROR: bot script not found: {bot_script_path}")
        return 2
    if screener_script_path is not None and not screener_script_path.exists():
        print(f"ERROR: screener script not found: {screener_script_path}")
        return 2

    if arguments.max_bots == 0:
        max_bots = 0
    else:
        max_bots = int(arguments.max_bots)

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

    if arguments.run_screener_on_start:
        if screener_script_path is None:
            print("ERROR: --run-screener-on-start requires --screener-script")
            return 2
        print("Running screener before initial launch...")
        run_screener_once(screener_script_path, screener_output_path)

    if not screener_output_path.exists():
        print(f"ERROR: screen file not found: {screener_output_path}")
        return 2

    try:
        launch_picks = load_screen_picks(
            screen_file_path=screener_output_path,
            ticker_column=arguments.ticker_column,
            title_column=arguments.title_column,
            yes_budget_cents=arguments.yes_budget_cents,
            no_budget_cents=arguments.no_budget_cents,
            yes_budget_column=arguments.yes_budget_column,
            no_budget_column=arguments.no_budget_column,
            max_bots=max_bots,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    if not launch_picks:
        print("No tickers found to launch.")
        return 0

    children = spawn_bots(
        picks=launch_picks,
        bot_script_path=bot_script_path,
        logs_directory=logs_directory,
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        use_demo=arguments.use_demo,
        dry_run=arguments.dry_run,
        subaccount=arguments.subaccount,
        launch_delay_seconds=arguments.launch_delay_seconds,
        pass_credentials_via_cli=arguments.pass_credentials_via_cli,
    )

    return monitor_and_refresh(
        children,
        screen_file_path=screen_file_path,
        screener_script_path=screener_script_path,
        screener_output_path=screener_output_path,
        bot_script_path=bot_script_path,
        logs_directory=logs_directory,
        api_key_id=api_key_id,
        private_key_path=private_key_path,
        use_demo=arguments.use_demo,
        dry_run=arguments.dry_run,
        subaccount=arguments.subaccount,
        launch_delay_seconds=arguments.launch_delay_seconds,
        pass_credentials_via_cli=arguments.pass_credentials_via_cli,
        ticker_column=arguments.ticker_column,
        title_column=arguments.title_column,
        yes_budget_cents=arguments.yes_budget_cents,
        no_budget_cents=arguments.no_budget_cents,
        yes_budget_column=arguments.yes_budget_column,
        no_budget_column=arguments.no_budget_column,
        max_bots=max_bots,
        refresh_interval_seconds=arguments.refresh_interval_seconds,
        poll_seconds=arguments.poll_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
