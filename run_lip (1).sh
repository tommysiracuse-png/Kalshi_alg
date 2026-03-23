#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- EDIT THESE VALUES IF NEEDED ----
export KALSHI_API_KEY_ID="2622245b-4c4d-46f4-8a89-2e69069e7c70"
export KALSHI_PRIVATE_KEY_PATH="$ROOT/tizzler2003.txt"
BOT_SCRIPT="$ROOT/v1_orederbook_replay.py"
SCREEN_FILE="$ROOT/screener_export.csv"
MAX_BOTS=60
YES_BUDGET_CENTS=1600
NO_BUDGET_CENTS=1600
USE_DEMO=0
DRY_RUN=0
SUBACCOUNT=""
LAUNCH_DELAY_SECONDS=8
# ------------------------------------

if [[ ! -f "$BOT_SCRIPT" ]]; then
  echo "ERROR: bot script not found: $BOT_SCRIPT"s
  exit 2
fi

if [[ ! -f "$SCREEN_FILE" ]]; then
  echo "ERROR: screener file not found: $SCREEN_FILE"
  echo "Export your notebook screener as screener_export.csv into this folder."
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

python3 "$ROOT/lip_launcher.py" "${ARGS[@]}"
