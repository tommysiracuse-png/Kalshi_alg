#!/usr/bin/env bash
set -euo pipefail

export ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- EDIT THESE VALUES IF NEEDED ----
export KALSHI_API_KEY_ID="2622245b-4c4d-46f4-8a89-2e69069e7c70"
export KALSHI_PRIVATE_KEY_PATH="$ROOT/tizzler2003.txt"
BOT_SCRIPT="$ROOT/V1.py"
SCREENER_SCRIPT="$ROOT/kalshi_screener.py"
SCREEN_FILE="$ROOT/screener_export.csv"
LAUNCHER_SCRIPT="$ROOT/lip_launcher.py"
WATCHDOG_RUNNER_SCRIPT="$ROOT/market_watchdog_runner.py"
WATCHDOG_PROFILER_SCRIPT="$ROOT/market_risk_profiler.py"
WATCHDOG_STATE_DIR="$ROOT/watchdog_state"
WATCHDOG_DISABLE_FILE="$ROOT/watchdog_disable_list.json"
WATCHDOG_INTERVAL_SECONDS=60
WATCHDOG_STATE_REFRESH_SECONDS=3
WATCHDOG_EXTREME_STALE_SECONDS=120
WATCHDOG_SAMPLE_SECONDS=4
WATCHDOG_POLL_INTERVAL_SECONDS=0.35
WATCHDOG_CONFIDENCE_REDUCTION_THRESHOLD=0.68
WATCHDOG_CONFIDENCE_FLATTEN_THRESHOLD=0.55
MAX_BOTS=15
YES_BUDGET_CENTS=200
NO_BUDGET_CENTS=200
USE_DEMO=0
DRY_RUN=0
SUBACCOUNT=""
LAUNCH_DELAY_SECONDS=2
REFRESH_INTERVAL_SECONDS=900
POLL_SECONDS=50
MINIMUM_CARRYOVER_VALUE_CENTS=50
# ------------------------------------

if [[ ! -f "$BOT_SCRIPT" ]]; then
  echo "ERROR: bot script not found: $BOT_SCRIPT"
  exit 2
fi

if [[ ! -f "$SCREENER_SCRIPT" ]]; then
  echo "ERROR: screener script not found: $SCREENER_SCRIPT"
  exit 2
fi

if [[ ! -f "$LAUNCHER_SCRIPT" ]]; then
  echo "ERROR: launcher script not found: $LAUNCHER_SCRIPT"
  exit 2
fi

if [[ ! -f "$WATCHDOG_RUNNER_SCRIPT" ]]; then
  echo "ERROR: watchdog runner script not found: $WATCHDOG_RUNNER_SCRIPT"
  exit 2
fi

if [[ ! -f "$WATCHDOG_PROFILER_SCRIPT" ]]; then
  echo "ERROR: watchdog profiler script not found: $WATCHDOG_PROFILER_SCRIPT"
  exit 2
fi

if [[ "$DRY_RUN" == "0" ]]; then
  if [[ -z "${KALSHI_API_KEY_ID:-}" ]]; then
    echo "ERROR: KALSHI_API_KEY_ID is empty."
    exit 2
  fi
  if [[ ! -f "$KALSHI_PRIVATE_KEY_PATH" ]]; then
    echo "ERROR: private key file not found: $KALSHI_PRIVATE_KEY_PATH"
    exit 2
  fi
fi

ARGS=(
  --screen-file "$SCREEN_FILE"
  --bot-script "$BOT_SCRIPT"
  --max-bots "$MAX_BOTS"
  --yes-budget-cents "$YES_BUDGET_CENTS"
  --no-budget-cents "$NO_BUDGET_CENTS"
  --launch-delay-seconds "$LAUNCH_DELAY_SECONDS"
  --screener-script "$SCREENER_SCRIPT"
  --screener-output "$SCREEN_FILE"
  --run-screener-on-start
  --refresh-interval-seconds "$REFRESH_INTERVAL_SECONDS"
  --poll-seconds "$POLL_SECONDS"
  --minimum-carryover-value-cents "$MINIMUM_CARRYOVER_VALUE_CENTS"
  --watchdog-runner-script "$WATCHDOG_RUNNER_SCRIPT"
  --watchdog-profiler-script "$WATCHDOG_PROFILER_SCRIPT"
  --watchdog-state-dir "$WATCHDOG_STATE_DIR"
  --watchdog-disable-file "$WATCHDOG_DISABLE_FILE"
  --watchdog-interval-seconds "$WATCHDOG_INTERVAL_SECONDS"
  --watchdog-state-refresh-seconds "$WATCHDOG_STATE_REFRESH_SECONDS"
  --watchdog-extreme-stale-seconds "$WATCHDOG_EXTREME_STALE_SECONDS"
  --watchdog-sample-seconds "$WATCHDOG_SAMPLE_SECONDS"
  --watchdog-poll-interval-seconds "$WATCHDOG_POLL_INTERVAL_SECONDS"
  --watchdog-confidence-reduction-threshold "$WATCHDOG_CONFIDENCE_REDUCTION_THRESHOLD"
  --watchdog-confidence-flatten-threshold "$WATCHDOG_CONFIDENCE_FLATTEN_THRESHOLD"
)

if [[ "$USE_DEMO" == "1" ]]; then
  ARGS+=(--use-demo)
fi

if [[ "$DRY_RUN" == "1" ]]; then
  ARGS+=(--dry-run)
fi

if [[ -n "$SUBACCOUNT" ]]; then
  ARGS+=(--subaccount "$SUBACCOUNT")
fi

python3 "$LAUNCHER_SCRIPT" "${ARGS[@]}"
