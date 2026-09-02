# Starts the Kalshi operations dashboard on Windows.
# API:  FastAPI (uvicorn) on http://127.0.0.1:8001
# Web:  Next.js on        http://127.0.0.1:3000  <- open this in your browser
# Credentials for trading live in ~\.config\kalshi-alg\bot.env (not needed just to view the dashboard).

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

Import-EnvFile (Join-Path $configDir "ui.env")
Import-EnvFile (Join-Path $configDir "bot.env")

foreach ($d in @("runtime", "session_data", "logs", "watchdog_state")) {
    New-Item -ItemType Directory -Force (Join-Path $repo $d) | Out-Null
}

# Start the FastAPI operations API (loopback only).
$api = Start-Process -PassThru -WindowStyle Minimized -FilePath (Join-Path $repo ".venv\Scripts\python.exe") `
    -WorkingDirectory $repo `
    -ArgumentList "-m", "uvicorn", "ui_api.app:app", "--host", "127.0.0.1", "--port", "8001"

# Start the Next.js dashboard (loopback only on Windows).
$web = Start-Process -PassThru -WindowStyle Minimized -FilePath "cmd.exe" `
    -WorkingDirectory (Join-Path $repo "web") `
    -ArgumentList "/c", "npx next start --hostname 127.0.0.1 --port 3000"

Write-Host ""
Write-Host "API PID: $($api.Id)   Web PID: $($web.Id)"
Write-Host "Dashboard: http://127.0.0.1:3000  (username: operator)"
