@echo off
setlocal

REM ============================================================
REM run_lip.bat
REM Launch one V1.py bot per ticker found in screener_export.csv
REM Credentials are passed through environment variables so they
REM do not appear on the child process command line by default.
REM ============================================================

set "ROOT=%~dp0"

REM ---- EDIT THESE VALUES IF NEEDED ----
set "KALSHI_API_KEY_ID=2622245b-4c4d-46f4-8a89-2e69069e7c70"
set "KALSHI_PRIVATE_KEY_PATH=%ROOT%tizzler2003.txt"
set "BOT_SCRIPT=%ROOT%V1.py"
set "SCREEN_FILE=%ROOT%screener_export.csv"
set "MAX_BOTS=14"
set "YES_BUDGET_CENTS=500"
set "NO_BUDGET_CENTS=500"
set "USE_DEMO=0"
set "DRY_RUN=0"
set "SUBACCOUNT="
set "LAUNCH_DELAY_SECONDS=0.25"
REM ------------------------------------

if not exist "%BOT_SCRIPT%" (
    echo ERROR: bot script not found: %BOT_SCRIPT%
    exit /b 2
)

if not exist "%SCREEN_FILE%" (
    echo ERROR: screener file not found: %SCREEN_FILE%
    echo Export your notebook screener as screener_export.csv into this folder.
    exit /b 2
)

if "%DRY_RUN%"=="0" (
    if "%KALSHI_API_KEY_ID%"=="" (
        echo ERROR: KALSHI_API_KEY_ID is empty.
        exit /b 2
    )
    if not exist "%KALSHI_PRIVATE_KEY_PATH%" (
        echo ERROR: private key file not found: %KALSHI_PRIVATE_KEY_PATH%
        exit /b 2
    )
)

set "USE_DEMO_FLAG="
if "%USE_DEMO%"=="1" set "USE_DEMO_FLAG=--use-demo"

set "DRY_RUN_FLAG="
if "%DRY_RUN%"=="1" set "DRY_RUN_FLAG=--dry-run"

set "SUBACCOUNT_FLAG="
if not "%SUBACCOUNT%"=="" set "SUBACCOUNT_FLAG=--subaccount %SUBACCOUNT%"

python "%ROOT%lip_launcher.py" ^
  --screen-file "%SCREEN_FILE%" ^
  --bot-script "%BOT_SCRIPT%" ^
  --max-bots %MAX_BOTS% ^
  --yes-budget-cents %YES_BUDGET_CENTS% ^
  --no-budget-cents %NO_BUDGET_CENTS% ^
  --launch-delay-seconds %LAUNCH_DELAY_SECONDS% ^
  %USE_DEMO_FLAG% ^
  %DRY_RUN_FLAG% ^
  %SUBACCOUNT_FLAG%

endlocal
