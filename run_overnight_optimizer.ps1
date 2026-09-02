# Overnight optimization run — scheduled for midnight via Windows Task Scheduler.
# Crash-safe settings: 8 workers (this machine hard-crashes under heavier load).
$repo = "C:\Users\admin\Documents\KalshiAlg\Kalshi_alg-ui"
Set-Location $repo
$log = Join-Path $repo "runtime\optimizer\overnight_console.log"
"----- overnight optimizer started $(Get-Date -Format o) -----" | Out-File -Append -Encoding utf8 $log
& "$repo\.venv\Scripts\python.exe" -m optimizer.main `
    --history history_data\history.sqlite3 `
    --candidates 300 `
    --workers 8 `
    --seed 2 `
    --top-params 25 `
    --budget-minutes 300 `
    --markets 80 `
    --fill-share 0.5 `
    --base-params-json runtime\optimizer\base_params_winner.json `
    --write-back *>> $log
"----- exited $(Get-Date -Format o) code=$LASTEXITCODE -----" | Out-File -Append -Encoding utf8 $log
