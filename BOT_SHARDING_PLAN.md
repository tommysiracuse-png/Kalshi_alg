# 500-Market Single-Host Fleet Redesign

## Summary

Replace the one-process-per-market runtime with a sharded architecture capable of managing and actively quoting up to 500 markets on one upgraded host.

The production target is:

- 500 live market state machines.
- At least one risk-eligible quote side per market when API and capital gates pass.
- A 20-second quote-decision freshness deadline.
- Advanced API tier support using centralized token accounting.
- 20 worker processes, each managing up to 25 markets.
- No legacy process-per-market fallback.
- Minimum host: 32 GiB RAM, 8 vCPU, SSD with at least 100 GiB free.
- Twenty percent of available cash permanently reserved.

Kalshi currently supports multi-market WebSocket subscriptions and dynamic market updates, making sharding feasible. Its Advanced tier currently supplies 300 read and write tokens per second; default requests cost 10 tokens, while cancellation is cheaper. [Kalshi WebSocket documentation](https://docs.kalshi.com/websockets/websocket-connection), [Kalshi rate-limit documentation](https://docs.kalshi.com/getting_started/rate_limits).

## Architecture and Interfaces

### Process model

Replace `BotManager` child-per-market spawning with:

```text
Fleet controller
    ├── Execution broker
    ├── Worker 00: up to 25 markets
    ├── Worker 01: up to 25 markets
    ├── ...
    └── Worker 19: up to 25 markets
```

- Derive worker count as `ceil(maxBots / 25)`, capped at 20.
- Assign markets with bounded-load rendezvous hashing so refreshes move as few markets as possible while keeping every shard at or below 25.
- Each worker owns one WebSocket, one SQLite connection, one control socket, and independent market actors.
- A worker failure affects at most 25 markets.
- The controller remains responsible for screening, inventory carryover, assignment, health, and fleet lifecycle.
- The execution broker is the only process allowed to place, amend, decrease, or cancel orders.

### Market actor refactor

Extract the per-market strategy state from `TopOfBookBot` into an in-process `MarketActor`:

- Preserve existing fair-value, queue, inventory, quote, fill, and markout behavior.
- Remove assumptions that the actor owns its API client, process, socket, or SQLite connection.
- Accept market events through `handle_event(event)`.
- Emit immutable `QuoteIntent` objects instead of calling the adaptor directly.
- Maintain independent order state, locks, timers, inventory, and risk mode per ticker.
- Keep a single-market CLI wrapper for development and replay testing, but route it through the same worker and actor implementation.

### Multi-market streaming

Extend the venue interface with:

```python
stream_events_many(
    market_ids: Sequence[str],
    include_position_updates: bool = True,
) -> AsyncIterator[MarketEvent]

update_market_subscriptions(
    add: Sequence[str],
    remove: Sequence[str],
) -> None
```

The Kalshi adaptor will:

- Subscribe each worker connection to its assigned tickers.
- Subscribe to `orderbook_delta`, `trade`, `ticker`, `user_orders`, `fill`, and `market_positions`.
- Route messages using the ticker in each payload.
- Maintain an expected order-book sequence per ticker rather than per connection.
- On a sequence gap, mark only that ticker’s book unavailable, request a fresh snapshot for that ticker, and suppress its new orders until restored.
- Apply dynamic add/delete subscriptions during screener reconciliation without reconnecting unaffected markets.
- Reconnect with exponential backoff and reinitialize every assigned book before allowing exposure-increasing orders.
- Keep the current single-market stream method as a wrapper around the multi-market implementation for tests and tools, not as a production fallback.

### Central execution and API broker

Introduce these internal message types:

```text
QuoteIntent
- ticker, side, desired price and size
- strategy generation
- urgency: emergency_cancel | risk_reduce | normal
- created_at and expires_at
- fair value and risk justification

ExecutionResult
- ticker, side, order ID, status
- accepted price and size
- exchange timestamp or error

WorkerHeartbeat
- worker ID, assigned tickers
- per-market book/risk/order health
- memory, queue depth, and event lag

FleetCapacity
- API tier and token budgets
- admitted markets and quote sides
- cash committed and reserved
- freshness and backlog estimates
```

The broker will:

- Own pooled HTTP clients, authentication, account order state, and token buckets.
- Coalesce pending normal intents by market and side so only the newest desired state is executed.
- Prioritize emergency cancels, then risk-reducing actions, then normal cancels/amends, then new exposure.
- Reserve 15% of write capacity for cancellation and risk-reducing work.
- Reserve 20% of read capacity for reconciliation and emergency account queries.
- Use conservative costs of 10 tokens for normal reads/writes and 2 tokens for cancellations unless the endpoint has a verified lower cost.
- Refresh `/account/limits` periodically and immediately after repeated 429 responses.
- Apply exponential backoff to normal traffic without delaying emergency cancellation.
- Reconcile every response and WebSocket order update against the broker’s authoritative order registry.

Admission for 500 markets uses:

```text
normal_write_capacity =
    floor(write_refill_rate × 0.85 × 20 seconds / 10 tokens)
```

At the current Advanced limit of 300 write tokens/second, this admits 510 normal quote sides. Therefore:

- Allocate one quote side to each eligible market first.
- Allocate second sides only from measured surplus capacity.
- Recompute every market’s desired quote at least every 20 seconds, even when no exchange write is necessary.
- Cancel a quote when market data or its decision becomes older than 20 seconds.
- If the API tier is unavailable or cannot admit the requested market count, enter fail-closed mode: cancel exposure-increasing orders, permit only inventory-reducing actions, and publish a capacity error.
- If capacity drops while running, transition the entire fleet to reduction-only until the gate passes again.

### Capital and risk allocation

Create a controller-level allocator before intents reach the broker:

- Allocatable cash equals 80% of current available cash.
- Include existing resting-order notional, positions, and pending execution intents in committed capital.
- Give inventory-reducing sides first priority.
- Give each eligible market one side in screener-rank order.
- Use remaining capital and API capacity for second sides.
- Enforce configured per-market budgets unchanged.
- Limit one series to 10% of allocatable cash unless it is reducing existing inventory.
- If every requested market cannot receive its minimum allocation, fail closed rather than partially presenting the run as a 500-market fleet.
- Account-wide kill switches cancel exposure-increasing orders when cash, position, order, API-limit, or reconciliation data becomes stale or internally inconsistent.

## Runtime, Storage, and Operations

### Watchdog redesign

Remove persistent watchdog and profiler processes from the production runtime.

- Convert profiler calculations into pure `RiskEvaluator` functions operating on each actor’s rolling order-book/trade window.
- Run risk evaluation inside each worker every 60 seconds with deterministic staggering.
- Mark risk state stale after 120 seconds.
- Prioritize markets with positions or resting orders.
- Query realized markouts from the worker’s actual telemetry database, eliminating the current path mismatch.
- Preserve `normal`, `reduction_only`, and `flatten_only` behavior and UI visibility.
- A stale or failed evaluator may only tighten risk; it cannot restore normal quoting.

### Telemetry and logs

Store new-run telemetry by shard:

```text
session_data/artifacts/<session>/<run>/
    fleet_manifest.json
    launcher.log
    shards/<worker-id>/telemetry.sqlite3
    shards/<worker-id>/worker.log
    markets/<ticker>/settings.json
```

- Keep ticker columns on every telemetry table and index `(ticker, timestamp)`.
- Use one WAL-mode writer connection per worker with batched commits.
- Add a `runtime_events` table for structured bot, adaptor, watchdog, and execution events.
- Retain fills, order revisions, markouts, and P&L for the complete run.
- Retain raw quotes, market states, ticker updates, public trades, and runtime events for seven days.
- Retain one-minute aggregates for the complete run.
- Rotate each worker log at 25 MiB with four retained files.
- Update the UI data layer to resolve ticker-to-shard through `fleet_manifest.json`.
- Keep historical per-market telemetry readable; do not rewrite or delete old run artifacts.
- Update the historical toxicity reader to scan both legacy databases and shard databases.

### Controller and UI scaling

Replace per-market socket polling with pushed shard heartbeats:

- Workers publish heartbeats every two seconds.
- A worker is stale after five seconds.
- On stale heartbeat, the broker cancels that shard’s exposure-increasing orders before restart.
- Poll at most 20 worker sockets concurrently instead of 500 market sockets.
- Build per-market UI rows from cached heartbeats.

Bump launcher status to schema version 4:

- Preserve the `bots` market-level view for UI compatibility.
- Add worker, broker, capacity, queue-depth, quote-age, and allocation summaries.
- Remove the full persistent disable dictionary from the heartbeat file; publish its count and serve paginated details from the API.
- Add pagination to market, disabled-market, telemetry, and runtime-event endpoints.
- Keep existing control endpoints, but route market enable/disable actions through shard assignment and subscription updates.

### Configuration

Bump session configuration to schema version 2 and add:

```json
{
  "fleetRuntime": {
    "shardSize": 25,
    "quoteFreshnessSeconds": 20,
    "writeUtilizationLimit": 0.85,
    "readUtilizationLimit": 0.80,
    "cashReserveFraction": 0.20,
    "seriesExposureFraction": 0.10,
    "workerHeartbeatSeconds": 2,
    "workerStaleSeconds": 5,
    "startupTimeoutSeconds": 300
  }
}
```

- Allow `launcher.maxBots` from 1 through 500.
- Keep the default at 40; 500 must be selected explicitly.
- Validate that derived worker capacity covers `maxBots`.
- Automatically migrate schema-v1 saved sessions by adding the defaults above.
- Continue treating run configurations as immutable snapshots.
- Remove the production runtime path that spawns `V1.py` and `market_watchdog_runner.py` per ticker.
- Consolidate shared code under one canonical package; root scripts become thin entrypoints rather than duplicated implementations.

### Service configuration

Update the fleet systemd unit to require and enforce:

```text
MemoryHigh=24G
MemoryMax=28G
TasksMax=256
LimitNOFILE=65536
TimeoutStopSec=300
Restart=on-failure
RestartSec=10
OOMPolicy=stop
```

Startup must:

1. Validate host resources and API capacity.
2. Query positions and resting orders.
3. Cancel or adopt only recognized bot-tagged orders.
4. Start workers in observe-only mode.
5. Wait for complete books and fresh risk states.
6. Enable quoting only after capital and capacity gates pass.

Shutdown must:

1. Stop new intent generation.
2. Freeze workers.
3. Cancel and verify all bot-tagged orders account-wide.
4. Close streams and telemetry.
5. Stop workers.
6. Exit only after a second authoritative account verification.

## Tests and Rollout

### Automated tests

Add unit coverage for:

- Multi-market subscription creation and dynamic add/delete.
- Per-ticker sequence tracking and isolated snapshot recovery.
- Event routing with interleaved messages from 500 markets.
- Strategy decision parity between recorded legacy behavior and `MarketActor`.
- Quote-intent coalescing, expiration, and priority ordering.
- Advanced-tier capacity math and runtime tier downgrade.
- Twenty-percent cash reservation and 10% series limit.
- One-side-first and second-side allocation.
- Fail-closed and reduction-only transitions.
- Worker heartbeat expiry and shard order cleanup.
- Batched telemetry, retention, and legacy/shard history reads.
- Session schema-v1 to schema-v2 migration.
- Status/API pagination with 500 markets.

Add integration simulations for:

- 500 markets across 20 workers.
- Broad simultaneous book movements.
- WebSocket disconnects and sequence gaps.
- HTTP 429 bursts and API-tier changes.
- Worker, broker, and controller crashes.
- SQLite failure, slow disk, and disk-full behavior.
- Stale market data and stale risk calculations.
- Full-fleet refresh with inventory carryovers.
- Emergency shutdown with up to 1,000 resting orders.

### Acceptance criteria

The new runtime must demonstrate:

- A 24-hour 500-market shadow soak with no OOM or uncontrolled swap growth.
- Total fleet RSS below 24 GiB on the required host.
- All workers healthy and subscribed within five minutes.
- Event-to-decision p95 below 250 ms.
- No quote based on market or decision state older than 20 seconds.
- Emergency cancellation p99 below two seconds after reaching the broker, subject to venue availability.
- No normal-operation 429 loop; any 429 reduces normal traffic while preserving cancellation capacity.
- No duplicate live order per ticker/side.
- Worker failure cancels affected exposure-increasing orders within five seconds.
- Verified full-fleet shutdown within 180 seconds.
- Runtime status generation below 500 ms.
- UI market and monitoring pages remain responsive with 500 markets.
- Artifact growth below 5 GiB per day under the load test.

### Rollout

Because the legacy runtime is being replaced, rollback is release-based rather than a runtime mode:

1. Build and test the new runtime without changing the production cap.
2. Run recorded-event parity and 500-market synthetic load tests.
3. Deploy to the upgraded host in live shadow mode with all order writes disabled for 24 hours.
4. Stop the old fleet and verify all bot-tagged orders absent.
5. Enable the new runtime at 50 markets for one trading day.
6. Increase to 100, 250, and 500, requiring all acceptance metrics to pass for one trading day at each stage.
7. At any failed stage, stop, verify order cleanup, and redeploy the previous release; do not run both runtimes simultaneously.
8. Make 500 available in saved sessions only after the final soak and emergency-shutdown test pass.

## Assumptions and Defaults

- “500 active” means 500 live market actors, with at least one quote side per market only when book, risk, capital, and API gates pass.
- Second-side coverage is opportunistic and subordinate to cancellation capacity.
- The production account remains at least Advanced tier with 300-token read/write refill rates; lower observed capacity fails closed.
- The quote freshness contract is 20 seconds.
- The host will be upgraded before enabling more than 40 markets.
- Existing strategy mathematics and per-market budgets remain unchanged unless required for centralized risk enforcement.
- Historical run artifacts remain readable and immutable.
- The process-per-market production runtime is removed rather than maintained as a feature-flag fallback.
