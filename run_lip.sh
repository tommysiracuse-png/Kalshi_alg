#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- EDIT THESE VALUES IF NEEDED ----
export KALSHI_API_KEY_ID="${KALSHI_API_KEY_ID:-}"  # set in your environment
export KALSHI_PRIVATE_KEY_PATH="${KALSHI_PRIVATE_KEY_PATH:-$ROOT/privkey.txt}"  # path to your PEM key
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
WATCHDOG_SAMPLE_SECONDS=2.5
WATCHDOG_POLL_INTERVAL_SECONDS=0.35
WATCHDOG_CONFIDENCE_REDUCTION_THRESHOLD=0.68
WATCHDOG_CONFIDENCE_FLATTEN_THRESHOLD=0.55
# Realized-PnL circuit breaker (defaults baked into market_watchdog_runner.py):
#   --realized-pnl-net-threshold-cents -1.0     (size-weighted avg net per contract)
#   --realized-pnl-total-loss-threshold-cents -300.0  ($3 hard stop on total session loss)
#   --realized-pnl-min-fills 3
#   --realized-pnl-horizon-seconds 5
#   --realized-pnl-lookback-seconds 3600
# All values are GROUND-TRUTH realized P&L (marked to observed future market
# mid), not derived from the bot's own fair-value model. Either trigger fires
# flatten_only. Pass --no-realized-pnl through the launcher to disable.
# Tuned 2026-04-29 from telemetry analysis:
# - Inter-fill gaps avg 45-100 min on slow markets (Brent daily) -> refresh 30m so
#   bots can complete >=1 fill cycle before rotation; carryover still protects winners.
# - User confirmed empirical exposure stays <$100 even at 75 bots x $5 budget, so the
#   "5*$10" exposure cap is non-binding in practice. Bottleneck is bot-yield: screener
#   finds 106 candidates but only ~7 produced fills last session. Marginal bots beyond
#   the top ~25 ranked markets just consume CPU/API quota without producing fills.
# - Budgets at 500c/side stay - raising budget without raising fills only amplifies
#   adverse-selection losses (Brent T97.50 markout is 9.7c/5s = toxic).
MAX_BOTS=100
YES_BUDGET_CENTS=500
NO_BUDGET_CENTS=900
USE_DEMO=0
DRY_RUN=0
SUBACCOUNT=""
LAUNCH_DELAY_SECONDS=.5
REFRESH_INTERVAL_SECONDS=1200
POLL_SECONDS=60
MINIMUM_CARRYOVER_VALUE_CENTS=20
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
