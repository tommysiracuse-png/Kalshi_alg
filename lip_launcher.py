
#!/usr/bin/env python3
"""
Launch one single-market Kalshi bot per ticker from a screener CSV.

This launcher intentionally does NOT do market selection by itself.
It only reads the rows you already chose in your screener export.

Safety / quality improvements in this version:
- Clear, explicit argument names and validation.
- Conservative default budgets that match the bot defaults.
- Credentials are passed to child bots through environment variables by default,
  not on the command line.
- Log files store a redacted launch command instead of exposing secrets.
- Optional per-row YES / NO budget columns are supported.
"""

from __future__ import annotations

import argparse
import csv
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, IO, List, Optional


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
        help="Maximum number of rows / bots to launch. Use 0 or a negative value to launch every row.",
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
# Launch helpers
# ---------------------------------------------------------------------------

def resolve_credentials(arguments: argparse.Namespace) -> tuple[Optional[str], Optional[str]]:
    api_key_id = arguments.api_key_id or os.getenv("KALSHI_API_KEY_ID") or os.getenv("API_KEY_ID")
    private_key_path = arguments.private_key or os.getenv("KALSHI_PRIVATE_KEY_PATH") or os.getenv("PRIVATE_KEY_PATH")
    return api_key_id, private_key_path


def redacted_command(command: List[str]) -> str:
    redacted_parts: List[str] = []
    skip_next = False

    for index, token in enumerate(command):
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


def print_launch_plan(picks: List[LaunchPick]) -> None:
    print("\n=== Launch plan ===")
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

    logs_directory.mkdir(parents=True, exist_ok=True)
    bot_working_directory = bot_script_path.parent.resolve()

    child_processes: List[ChildProcess] = []

    for pick in picks:
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

        child_processes.append(
            ChildProcess(
                ticker=pick.ticker,
                process=process,
                log_path=log_path,
                log_handle=log_handle,
            )
        )

        if launch_delay_seconds > 0:
            time.sleep(launch_delay_seconds)

    return child_processes


def close_child_log(child: ChildProcess) -> None:
    try:
        child.log_handle.flush()
        child.log_handle.close()
    except Exception:
        pass


def watch_children(children: List[ChildProcess]) -> int:
    if not children:
        return 0

    print("\nBots running. Press Ctrl+C to stop them all.")

    try:
        while True:
            alive_count = 0

            for child in list(children):
                return_code = child.process.poll()
                if return_code is None:
                    alive_count += 1
                    continue

                print(f"[WARN] {child.ticker} exited with code {return_code} | log={child.log_path}")
                close_child_log(child)
                children.remove(child)

            if alive_count == 0:
                print("All bots exited.")
                return 0

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping bots...")
        for child in children:
            try:
                child.process.send_signal(signal.SIGINT)
            except Exception:
                pass

        time.sleep(1)

        for child in children:
            try:
                if child.process.poll() is None:
                    child.process.terminate()
            except Exception:
                pass

        for child in children:
            close_child_log(child)

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

    if not screen_file_path.exists():
        print(f"ERROR: screen file not found: {screen_file_path}")
        return 2
    if not bot_script_path.exists():
        print(f"ERROR: bot script not found: {bot_script_path}")
        return 2

    if arguments.max_bots == 0:
        max_bots = 0
    else:
        max_bots = int(arguments.max_bots)

    if arguments.launch_delay_seconds < 0:
        print("ERROR: --launch-delay-seconds must be >= 0")
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

    rows = read_csv_rows(screen_file_path)
    if not rows:
        print("ERROR: screen file has no data rows")
        return 2

    try:
        validate_requested_columns(
            rows[0],
            ticker_column=arguments.ticker_column,
            yes_budget_column=arguments.yes_budget_column,
            no_budget_column=arguments.no_budget_column,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    try:
        launch_picks = build_launch_picks(
            rows=rows,
            ticker_column=arguments.ticker_column,
            title_column=arguments.title_column,
            default_yes_budget_cents=arguments.yes_budget_cents,
            default_no_budget_cents=arguments.no_budget_cents,
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
    return watch_children(children)


if __name__ == "__main__":
    raise SystemExit(main())
