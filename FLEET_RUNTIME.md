# 500-Market Sharded Fleet Runtime

## Runtime boundary

Production uses one controller, one execution broker, and `ceil(maxBots / 25)` workers, capped at 20. The legacy `V1.py` plus watchdog-per-ticker process topology is not invoked by the launcher. `V1.py` remains a development and replay wrapper around the same `MarketActor` used by workers.

Markets are assigned with stable bounded rendezvous hashing. Existing valid assignments survive screener refreshes; a new ticker goes to its highest-scoring shard with remaining capacity. No shard can own more than `fleetRuntime.shardSize` markets.

Each worker owns:

- one authenticated multi-market WebSocket;
- up to 25 independent `MarketActor` state machines;
- one shared WAL telemetry writer connection;
- one controller command queue and pushed heartbeat path;
- deterministic, staggered risk evaluation.

Only the execution broker may create, amend, decrease, or cancel an order. Worker-side client proxies emit immutable `QuoteIntent` messages and block for the matching execution result. The broker coalesces pending normal intent state by ticker and side, prevents duplicate registered orders, and gives priority to emergency cancellation, risk reduction, normal cancel/amend, then new exposure.

## Admission and fail-closed rules

The write-side admission formula is:

```text
floor(write_refill_rate * 0.85 * quote_freshness_seconds / 10)
```

At 300 write tokens per second and a 20-second freshness deadline this yields 510 normal quote sides. Admission assigns one side to every eligible market in screener order before assigning second sides.

The controller reserves 20% of available cash and limits non-reducing exposure in one series to 10% of allocatable cash. Resting-order notional and reported position exposure count as committed capital. If all requested markets cannot receive a first side, the fleet is fail-closed rather than partially represented as fully admitted.

The broker reserves 15% of measured write refill for cancel/risk work and 20% of read refill for reconciliation/account queries. Normal writes cost 10 accounting tokens and cancellations cost 2. A missing limit response, inadequate capacity, repeated rate-limit response, broker restart, stale account data, or inconsistent capital state disables new exposure fleet-wide. Inventory-reducing work remains eligible.

## Startup and shutdown

Startup validates configured shard capacity and, for fleets above 40 markets, the 32 GiB RAM, 8 vCPU, and 100 GiB free-disk host requirements. The broker queries positions and resting orders and clears only recognized `mm:`, `tob:`, or `wd:` orders from an earlier release. Workers start observe-only. An actor cannot quote until its book and risk state are fresh, the controller API/capital gates pass, and it has an allocated side.

Shutdown disables intent generation, freezes workers, cancels and verifies bot-tagged orders through the broker, stops streams and actors, stops workers, then performs a second authoritative verification. The systemd unit allows 300 seconds for this sequence.

## Health and freshness

Workers publish a heartbeat every two seconds. A five-second-old heartbeat is stale. The controller disables the shard, asks the broker to cancel its exposure-increasing orders, and restarts only that worker.

Every actor recomputes desired state at least once per second. A missing book, a market event older than 20 seconds, a quote decision older than 20 seconds, or risk state older than 120 seconds triggers cancellation and suppresses new exposure. A failed risk evaluator may tighten state but cannot restore normal quoting.

Status schema v4 retains the market-level `bots` rows and adds `workers`, `broker`, `capacity`, `allocation`, quote age, event lag, queue depth, and memory. Disabled-market details, market lists, telemetry, runtime events, and historical run markets have paginated API endpoints.

## Artifacts

```text
session_data/artifacts/<session>/<run>/
    configuration.json
    fleet_manifest.json
    launcher.log
    shards/<worker-id>/telemetry.sqlite3
    shards/<worker-id>/worker.log
    markets/<ticker>/settings.json
    screener/
```

Worker logs rotate at 25 MiB with four backups. Shard SQLite files use one shared writer connection, WAL mode, batched commits, and `(ticker, timestamp)` indexes. The UI and history readers resolve a ticker through `fleet_manifest.json`; legacy `markets/<ticker>/telemetry.sqlite3` artifacts remain readable.

## Release gates

Do not expose 500 in saved production sessions until the host upgrade, synthetic 500-market tests, 24-hour write-disabled soak, emergency cancellation test, and staged 50/100/250/500 live rollout have passed. Rollback is release-based: stop and verify cleanup before deploying the prior release, and never run old and new runtimes together.
