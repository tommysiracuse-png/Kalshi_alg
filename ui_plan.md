# Kalshi Operations UI Implementation Plan

## Architecture

The operations console is a self-hosted, single-operator application made of three independently supervised processes:

1. `kalshi-bot.service` runs the existing screener, launcher, bots, and watchdogs.
2. `kalshi-ui-api.service` runs a loopback-only FastAPI service that normalizes runtime artifacts and mediates controls.
3. `kalshi-ui-web.service` runs an authenticated, loopback-only Next.js App Router application.

The browser communicates only with Next.js. Authenticated same-origin backend-for-frontend routes inject a private internal token when proxying to FastAPI. Kalshi credentials remain in Python service environments and are never exposed to the browser.

## Launcher contract

- Publish an atomic, versioned `runtime/launcher_status.json` snapshot every two seconds with fleet lifecycle, environment, heartbeat, next refresh, errors, bot/watchdog PIDs, budgets, log paths, watchdog modes, and disabled tickers.
- Accept permission-restricted newline-delimited JSON commands through `runtime/launcher.sock`.
- Support idempotent `status`, `refresh`, `disable_ticker`, and `enable_ticker` commands keyed by `request_id`.
- Serialize disable-list mutations with a filesystem lock.
- Validate ticker controls against known screener, active, watchdog, and disabled tickers.
- Handle SIGINT and SIGTERM through graceful child shutdown. Bot-owned resting quotes are canceled and inventory is retained; UI controls never flatten inventory.

## Operations API

- Normalize `screener_export.csv`, launcher status, watchdog JSON, disable state, SQLite telemetry, P&L JSONL, and bounded log tails.
- Keep malformed or stale sources isolated and report explicit freshness instead of substituting plausible values.
- Expose versioned overview, markets, market detail, P&L, activity, system health, SSE, log-stream, and guarded-control endpoints under `/api/v1`.
- Use cents for money, hundredths for fractional contracts, and UTC epoch milliseconds on the wire.
- Validate all ticker-derived paths against a server-side allowlist.
- Record control request IDs, operator, action, target, outcome, and sanitized error in `runtime/ui_audit.sqlite3`.

## Next.js UI

- Protect all operational pages with Auth.js Credentials authentication using an Argon2id password hash.
- Use a 12-hour encrypted HTTP-only strict-same-site session and require authentication within the last 15 minutes for controls.
- Prioritize fleet status, production warnings, watchdog risk, P&L, positions, disabled markets, and recent failures on the overview.
- Provide searchable/sortable market inventory, per-market telemetry and logs, audit activity, and source diagnostics.
- Stream overview and log updates with SSE, bounded client history, reconnection, and a five-second polling fallback.
- Confirm every mutation and display the acknowledged request ID. Never report optimistic success.
- Meet keyboard, focus, contrast, semantic-table, responsive-layout, and reduced-motion requirements.

## Deployment and safety

- Run all three systemd user services under the same unprivileged account.
- Bind FastAPI to `127.0.0.1:8001` and Next.js to `127.0.0.1:3000`.
- Use an SSH tunnel by default. Add an HTTPS reverse proxy before LAN or internet exposure.
- Store service secrets in mode-0600 files outside the repository.
- Back up the audit SQLite database while stopped or through SQLite's online backup mechanism and rotate bot/watchdog logs daily.

## Verification

- Unit-test P&L, source adapters, stale/malformed data, path validation, locking, command idempotency, and error envelopes.
- Integration-test launcher refresh/disable/enable and graceful fake-child shutdown.
- Component-test statuses, filters, confirmations, stale states, and control outcomes.
- Exercise login, protected routes, SSE reconnection, polling fallback, fleet controls, ticker controls, reauthentication, and audit capture end to end.
- Perform a demo-environment soak test and supervised production read-only session before enabling production controls.
