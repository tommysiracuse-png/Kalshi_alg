# Starts the standalone millisecond order-book recorder on Windows (Phase 5 / Tier-2 data path).
# Records RAW Kalshi websocket messages (public channels only: orderbook_delta, trade, ticker)
# for the live fleet's markets + the most actively traded markets, into
#   record_data\YYYYMMDD\HH\conn<k>.jsonl.gz   (UTC hours; one line per message)
# Heartbeat: runtime\recorder_status.json   Log: runtime\recorder.log   PID: runtime\recorder.pid
# Stop gracefully:  New-Item runtime\recorder.stop   (or Stop-Process -Id (Get-Content runtime\recorder.pid))
# Credentials come from ~\.config\kalshi-alg\bot.env (KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH);
# the websocket needs signed headers even for public channels. The recorder never places orders.
# Knobs (optional env): RECORD_MAX_MARKETS=150 MARKETS_PER_CONNECTION=50 RECORD_TOP_N=100
#   RECORD_REFRESH_SECONDS=900 RETENTION_DAYS=30 — see recorder\config.py.

$repo = $PSScriptRoot
$configDir = Join-Path $env:USERPROFILE ".config\kalshi-alg"

function Import-EnvFile($path) {
    if (-not (Test-Path $path)) { Write-Warning "Missing env file: $path"; return }
    Get-Content $path | ForEach-Object {
        if ($_ -match '^\s*#' -or $_ -notmatch '=') { return }
        $name, $value = $_ -split '=', 2
        [Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim(), 'Process')
    }
}

Import-EnvFile (Join-Path $configDir "bot.env")

foreach ($d in @("runtime", "record_data")) {
    New-Item -ItemType Directory -Force (Join-Path $repo $d) | Out-Null
}

$python = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { Write-Error "Missing venv python: $python"; exit 1 }

# Refuse to double-record: two recorders would write the same files.
$pidFile = Join-Path $repo "runtime\recorder.pid"
if (Test-Path $pidFile) {
    $oldPid = (Get-Content $pidFile | Select-Object -First 1)
    if ($oldPid -match '^\d+$') {
        $existing = Get-Process -Id ([int]$oldPid) -ErrorAction SilentlyContinue
        if ($existing -and $existing.ProcessName -like 'python*') {
            Write-Host "Recorder already running (PID $oldPid). Stop it first: New-Item runtime\recorder.stop"
            exit 1
        }
    }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
}
$stopFile = Join-Path $repo "runtime\recorder.stop"
if (Test-Path $stopFile) { Remove-Item $stopFile -Force }

# Console output (uncaught tracebacks) goes to runtime\recorder_console*.log; the
# structured log is runtime\recorder.log (rotating, written by the process itself).
$proc = Start-Process -PassThru -WindowStyle Minimized -FilePath $python `
    -WorkingDirectory $repo `
    -ArgumentList "-u", "-m", "recorder.main" `
    -RedirectStandardOutput (Join-Path $repo "runtime\recorder_console.log") `
    -RedirectStandardError (Join-Path $repo "runtime\recorder_console.err.log")

# .venv\Scripts\python.exe is a launcher stub on Windows: it spawns the real
# interpreter as a child. The recorder writes its own (real) PID to runtime\recorder.pid
# at startup; wait for that so the PID we report is the one to stop/inspect.
$recorderPid = $null
for ($i = 0; $i -lt 40; $i++) {
    if (Test-Path $pidFile) {
        $content = (Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
        if ($content -match '^\d+$') { $recorderPid = [int]$content; break }
    }
    if ($proc.HasExited) { break }
    Start-Sleep -Milliseconds 250
}
if ($null -eq $recorderPid) {
    Write-Warning "Recorder did not write runtime\recorder.pid within 10 s; check runtime\recorder_console.err.log"
    Set-Content -Path $pidFile -Value $proc.Id -Encoding ascii
    $recorderPid = $proc.Id
}

Write-Host ""
Write-Host "Recorder PID: $recorderPid   (launcher stub PID: $($proc.Id))"
Write-Host "Status:  $(Join-Path $repo 'runtime\recorder_status.json')  (updates every 10 s)"
Write-Host "Data:    $(Join-Path $repo 'record_data')\YYYYMMDD\HH\conn<k>.jsonl.gz  (UTC)"
Write-Host "Stop:    New-Item $(Join-Path $repo 'runtime\recorder.stop')"
