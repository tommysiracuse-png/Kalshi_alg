# Kalshi Operations UI

The operations UI is a loopback-only Next.js application backed by a loopback-only FastAPI service. The API reads existing screener, watchdog, log, P&L, and SQLite telemetry artifacts. Trading credentials stay in the Python process environment and are never sent to the browser.

## Local setup

1. Create a Python environment and install the API dependencies:

   ```bash
   python3 -m venv .venv-ui
   .venv-ui/bin/pip install -r requirements-ui.txt
   ```

2. Install and build the web application:

   ```bash
   cd web
   npm ci
   npm run build
   ```

3. Generate secrets and a password hash. Do not paste the plaintext password into an environment file:

   ```bash
   openssl rand -base64 32
   node -e "require('argon2').hash(process.argv[1], {type: require('argon2').argon2id}).then(console.log)" 'your-password'
   ```

4. Copy the example environment files into `~/.config/kalshi-alg/`, replace every placeholder, and set permissions to `0600`.

5. Copy the service units into `~/.config/systemd/user/`, then run:

   ```bash
   systemctl --user daemon-reload
   systemctl --user enable --now kalshi-ui-api.service kalshi-ui-web.service
   ```

   Enable `kalshi-bot.service` only if the fleet should start automatically. Otherwise start it from the guarded UI control.

6. Open an SSH tunnel from the operator workstation:

   ```bash
   ssh -L 3000:127.0.0.1:3000 trading-host
   ```

   Visit `http://127.0.0.1:3000`. Put an HTTPS reverse proxy in front before exposing the service to a LAN or the internet.

## Runtime and retention

- `runtime/launcher_status.json` is the current atomic fleet snapshot.
- `runtime/launcher.sock` accepts local launcher controls and is mode `0600`.
- `runtime/ui_audit.sqlite3` stores control outcomes. Back it up while the API is stopped or with SQLite's online backup command.
- `session_data/sessions.sqlite3` stores saved session metadata, immutable run records, and metric summaries. Run logs, configuration snapshots, and telemetry are under `session_data/artifacts/`.
- Session configuration edits apply to the next run. The active run always retains the configuration version captured at startup.
- Archived sessions keep their run history. No automatic retention or migration of legacy `logs/` and `telemetry/` data is performed.
- Existing bot and watchdog logs remain under `logs/`. Use `logrotate` with `copytruncate`, daily rotation, 14 retained files, and compression if the host does not already manage them.
- A stop or disable action cancels bot-owned resting quotes but deliberately retains inventory.

## Verification

Run `.venv-ui/bin/python -m pytest`, `npm test`, `npm run lint`, and `npm run build`. Perform the first integrated run in demo mode, then verify a production read-only session before using controls.
