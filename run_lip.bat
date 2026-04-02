@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "KALSHI_API_KEY_ID=24d8c39b-59bd-41a4-b522-3a044f7af7bb"
set "KALSHI_PRIVATE_KEY_PATH=%ROOT%\privkey.txt"
set "BOT_SCRIPT=%ROOT%\V1.py"
set "SCREENER_SCRIPT=%ROOT%\kalshi_screener.py"
set "SCREEN_FILE=%ROOT%\screener_export.csv"
set "LAUNCHER_SCRIPT=%ROOT%\lip_launcher.py"
set "WATCHDOG_RUNNER_SCRIPT=%ROOT%\market_watchdog_runner.py"
set "WATCHDOG_PROFILER_SCRIPT=%ROOT%\market_risk_profile.py"
set "WATCHDOG_STATE_DIR=%ROOT%\watchdog_state"
set "WATCHDOG_DISABLE_FILE=%ROOT%\watchdog_disable_list.json"
set "WATCHDOG_INTERVAL_SECONDS=180"
set "WATCHDOG_STATE_REFRESH_SECONDS=3"
set "WATCHDOG_EXTREME_STALE_SECONDS=30"
set "WATCHDOG_SAMPLE_SECONDS=4"
set "WATCHDOG_POLL_INTERVAL_SECONDS=0.35"
set "WATCHDOG_CONFIDENCE_REDUCTION_THRESHOLD=0.90"
set "WATCHDOG_CONFIDENCE_FLATTEN_THRESHOLD=0.80"
set "MAX_BOTS=10"
set "YES_BUDGET_CENTS=500"
set "NO_BUDGET_CENTS=500"
set "USE_DEMO=0"
set "DRY_RUN=0"
set "SUBACCOUNT="
set "LAUNCH_DELAY_SECONDS=8"
set "REFRESH_INTERVAL_SECONDS=300"
set "POLL_SECONDS=2"
set "MINIMUM_CARRYOVER_VALUE_CENTS=100"

if not exist "%BOT_SCRIPT%" (
    echo ERROR: bot script not found: %BOT_SCRIPT%
    exit /b 2
)

if not exist "%SCREENER_SCRIPT%" (
    echo ERROR: screener script not found: %SCREENER_SCRIPT%
    exit /b 2
)

if not exist "%LAUNCHER_SCRIPT%" (
    echo ERROR: launcher script not found: %LAUNCHER_SCRIPT%
    exit /b 2
)

if not exist "%WATCHDOG_RUNNER_SCRIPT%" (
    echo ERROR: watchdog runner script not found: %WATCHDOG_RUNNER_SCRIPT%
    exit /b 2
)

if not exist "%WATCHDOG_PROFILER_SCRIPT%" (
    echo ERROR: watchdog profiler script not found: %WATCHDOG_PROFILER_SCRIPT%
    exit /b 2
)

if "%DRY_RUN%"=="0" (
    if not defined KALSHI_API_KEY_ID (
        echo ERROR: KALSHI_API_KEY_ID is empty.
        exit /b 2
    )
    if not exist "%KALSHI_PRIVATE_KEY_PATH%" (
        echo ERROR: private key file not found: %KALSHI_PRIVATE_KEY_PATH%
        exit /b 2
    )
)

set "ARGS=--screen-file ""%SCREEN_FILE%"" --bot-script ""%BOT_SCRIPT%"" --max-bots %MAX_BOTS% --yes-budget-cents %YES_BUDGET_CENTS% --no-budget-cents %NO_BUDGET_CENTS% --launch-delay-seconds %LAUNCH_DELAY_SECONDS% --screener-script ""%SCREENER_SCRIPT%"" --screener-output ""%SCREEN_FILE%"" --run-screener-on-start --refresh-interval-seconds %REFRESH_INTERVAL_SECONDS% --poll-seconds %POLL_SECONDS% --watchdog-runner-script ""%WATCHDOG_RUNNER_SCRIPT%"" --watchdog-profiler-script ""%WATCHDOG_PROFILER_SCRIPT%"" --watchdog-state-dir ""%WATCHDOG_STATE_DIR%"" --watchdog-disable-file ""%WATCHDOG_DISABLE_FILE%"" --watchdog-interval-seconds %WATCHDOG_INTERVAL_SECONDS% --watchdog-state-refresh-seconds %WATCHDOG_STATE_REFRESH_SECONDS% --watchdog-extreme-stale-seconds %WATCHDOG_EXTREME_STALE_SECONDS% --watchdog-sample-seconds %WATCHDOG_SAMPLE_SECONDS% --watchdog-poll-interval-seconds %WATCHDOG_POLL_INTERVAL_SECONDS% --watchdog-confidence-reduction-threshold %WATCHDOG_CONFIDENCE_REDUCTION_THRESHOLD% --watchdog-confidence-flatten-threshold %WATCHDOG_CONFIDENCE_FLATTEN_THRESHOLD%"

if "%USE_DEMO%"=="1" (
    set "ARGS=!ARGS! --use-demo"
)

if "%DRY_RUN%"=="1" (
    set "ARGS=!ARGS! --dry-run"
)

if not "%SUBACCOUNT%"=="" (
    set "ARGS=!ARGS! --subaccount ""%SUBACCOUNT%"""
)

if not "%MINIMUM_CARRYOVER_VALUE_CENTS%"=="" (
    set "ARGS=!ARGS! --minimum-carryover-value-cents %MINIMUM_CARRYOVER_VALUE_CENTS%"
)

python "%LAUNCHER_SCRIPT%" !ARGS!
exit /b %errorlevel%
