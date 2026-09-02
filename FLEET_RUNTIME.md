# 500-Market Sharded Fleet Runtime

## Runtime boundary

Production uses one controller, one execution broker, and `ceil(maxBots / 25)` workers, capped at 20. The legacy `V1.py` plus watchdog-per-ticker process topology is not invoked by the launcher. `V1.py` remains a development and replay wrapper around the same `MarketActor` used by workers.

Markets are assigned with stable bounded rendezvous hashing. Existing valid assignments survive screener refreshes; a new ticker goes to its highest-scoring shard with remaining capacity. No shard can own more than `fleetRuntime.shardSize` markets.

A retained market whose `botClasses` market class or per-class overrides change at a refresh is restarted in place (`RECONCILE_RESTART`): the running actor is removed — its resting quotes are cancelled — and a fresh actor with the new settings is started and re-subscribed. Unchanged markets keep their actors and quotes.

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

### Capital allocation and oversubscription

Kalshi balances are local to an exchange shard (0 default, 2 crypto, 3 tennis/baseball), so admission allocates per shard from `balance_breakdown`. Each granted quote side commits its market's full side budget (`launcher.yesBudgetCents` / `noBudgetCents`). Two caps apply on every shard and fleet-wide:

* **Budget cap** = allocatable shard cash (reserve kept) x `fleetRuntime.allocationOversubscription`. Sides are granted until committed budgets reach it. At the default `1.0` every side needs its whole budget inside un-multiplied cash (the historical rule) and **nothing else below applies**: the allocator is byte-identical to the legacy one (no live-cap withhold, no broker ledger, no screener cap). Kalshi locks collateral only when an order rests and rejects orders beyond the balance, and most quotes are small and never fill, so a multiplier above `1.0` (validated `<= 10`) lets budgets be committed on expected rather than worst-case exposure. The per-series cap (`seriesExposureFraction`) applies to the same oversubscribed pool.
* **Live cap** = allocatable shard cash, never multiplied. Actual exposure (resting-order notional plus position exposure) must stay below it. It is enforced in two places:
  * **Execution broker (hard limit, every order).** The allocator only runs at admission time, so the broker keeps a per-shard live-exposure ledger (`ShardExposureLedger`): position exposure from the admission snapshot it serves, fills of exposure-increasing orders reported since that snapshot, the resting notional (`price x remaining count`) of every order in its registry, and reservations in flight. The controller sends it `shard_exposure_limits` (allocatable cash per shard, shard per desired market, position exposure) whenever it recomputes capacity, before any worker is told to quote. A new non-reducing create, or an amend that raises resting notional, on a shard where `live + this order > allocatable` is refused with `ShardExposureCapError` (a subclass of `InsufficientBalanceError`, so the bot enters the same 30 s per-market create/amend cooldown); cancels, decreases, amend-downs and reduce-only orders always pass. The ledger is disabled at `1.0`.
  * **Allocator (admission, sticky and hysteretic).** A market that already holds a side (inventory, a resting order, or a grant from the previous pass) keeps it. Markets without one get no new side on a shard while its live exposure is at or above 100% of allocatable cash (`exchange shard N live exposure at allocatable cash`, `ShardAllocation.reduction_only`) and are re-admitted only once it has fallen below 80% (`LIVE_CAP_RELEASE_FRACTION`), so a shard hovering at the cap does not cancel and re-grant its quotes every refresh. Reducing sides never need capital. Exposure in markets the screener no longer desires counts fleet-wide only; a shard with no cash is reported as `unfunded`, never as withheld.
  * The un-multiplied exchange balance remains the venue's own limit: an order it rejects for insufficient balance takes the same cooldown path.

Grant order: within un-multiplied cash sides go in screener rank order. Past it (the oversubscribed region) markets already holding a side are served first, then the rest by rank, so a market with risk to manage keeps its side. First sides for every market precede second sides. The allocation result publishes `oversubscription`, `budget_cap_units`, `live_exposure_units`, `withholding` and a per-shard `shards` table (cash, allocatable, budget cap, committed, live exposure, funded/skipped/withheld markets, shortfall, reduction_only) in status; the skip summary (`skipped N of M markets (shard 3: 59 skipped, short $X of budget cap $Y): ...`) is repeated on the dashboard Overview as an `allocation:` warning. The broker heartbeat carries the ledger (`shard_exposure`, published as `broker.shardExposure`) and the controller's last limits appear under `broker.exposureLimits`.

Only when oversubscribed does the screener cap its picks per exchange shard at `floor(allocatable shard cash x oversubscription / first-side budget)` (a market needs one side to quote; the first side is the larger of `yesBudgetCents` / `noBudgetCents`), incumbents first, so fleet slots are not filled with markets that can never quote; the caps and dropped counts are published in the screener status (`shardCaps`, `firstSideBudgetCents`) and logged.

### Restart carryover of exchange positions

Inventory carryover across refreshes only knows the running fleet, so at a fleet Start the launcher reads the account's open positions through its own client and hands them to the screener (`Screener.carry_exchange_positions`, used by `refresh("startup")` and the CSV / fixed-ticker seed paths). Every nonzero position in an open market on a funded shard that is not disabled gets a bot even if it fails the screener filters, marked `selection_reason="exchange_position"`; the manager grants such a market only the side that reduces the position (no capital), so it quotes reduce-only unless the screener also picked it, and it stays reduce-only for its lifetime. Positions in closed/settled markets, on unfunded shards or in disabled markets are skipped with a warning; `max_bots` is honoured by displacing the lowest-ranked screened picks. The screener status reports `Carried N exchange position(s) into the fleet at start` (`exchangeCarryover`).

The broker reserves 15% of measured write refill for cancel/risk work and 20% of read refill for reconciliation/account queries. Normal writes cost 10 accounting tokens and cancellations cost 2. A missing limit response, inadequate capacity, repeated rate-limit response, broker restart, stale account data, or inconsistent capital state disables new exposure fleet-wide. Inventory-reducing work remains eligible.

## Startup and shutdown

Startup validates configured shard capacity and, for fleets above 40 markets, the 32 GiB RAM, 8 vCPU, and 100 GiB free-disk host requirements. The broker queries positions and resting orders and clears only recognized `mm:`, `tob:`, or `wd:` orders from an earlier release. Workers start observe-only. An actor cannot quote until its book and risk state are fresh, the controller API/capital gates pass, and it has an allocated side.

Shutdown first fences every worker channel in the broker so queued create/amend intents cannot execute. Workers then latch into a frozen state, acknowledge that normal quoting and watchdog exits are disabled, and cancel their quotes. Account cleanup retries venue reads with exponential backoff to tolerate read-after-cancel lag, preserves manual orders, and falls back to the controller client if the broker is unavailable. Worker and broker termination always completes before a cleanup failure is reported, followed by a final authoritative account verification. The systemd unit allows 300 seconds for this sequence.

## Health and freshness

Workers publish a heartbeat every two seconds. A five-second-old heartbeat is stale outside an acknowledged reconciliation window. The controller fences the affected broker channel, freezes and stops that worker, and cancels its bot-owned orders. Cleanup failures keep the shard offline and retry every five seconds; a replacement starts observe-only only after cleanup and reconciliation are acknowledged.

The execution broker is single-threaded by design, so its liveness is guarded separately from the workers'. Every venue call it makes has a bounded connect/read timeout and runs under a per-request deadline (20 s; 120 s for account-cleanup verification, which runs detached); a call that overruns is abandoned and the requester receives a `TimeoutError` while the loop moves on. Requests older than the worker RPC timeout are answered without a venue call, except cancellations, which always execute. The broker publishes a heartbeat every two seconds. The controller restarts it when heartbeats stop for 45 s, when it reports three or more consecutive venue timeouts without a success for 45 s, or when two controller RPCs in a row time out; restarts are rate-limited to one per minute, the fleet is reduction-only until the replacement reports capacity, and every healthy worker is sent a fresh reconcile. For 60 s after a broker restart a stale worker heartbeat alone does not trigger worker recovery (its RPCs were waiting on the old broker), so a broker stall cannot cascade into a fleet-wide worker recovery.

A market whose add failed during a reconcile (`RECONCILE_STEP_FAILED`) is retried by its worker with exponential backoff (30 s, 60 s, ... capped at 300 s) while it remains desired, instead of waiting for the next screener refresh; success is logged as `RECONCILE_RETRY_OK`. Until the actor exists the market is reported as `startup` with the retry state as its watchdog reason.

A worker acknowledges a reconcile as soon as removals and the subscription update are applied; additions and restarted markets are started afterwards by the worker's add loop (eight at a time), so the controller's `startupTimeoutSeconds` window no longer has to cover every add's venue round-trips. The controller never recovers a live, heartbeating worker merely because its reconcile is slow: a timed-out reconcile is torn down only when the worker's heartbeat is also stale, or when it has been outstanding for four startup windows. Actor startup and shutdown venue calls run off the worker's event loop, so a slow broker cannot starve the two-second heartbeat.

The broker caches read-only catalog responses so a reconcile burst cannot saturate it: `get_incentive_programs` and `get_series` / `get_series_fee_changes` for 300 s, `get_market` for 5 s. A failed admission snapshot (for example a stalled broker) fails the fleet closed and is retried from the monitor loop every 30 s instead of aborting the screener refresh.

Shutdown never waits on a dead broker: the write fence is given 15 s, a broker flagged as stalled is bypassed for cleanup (the controller's direct client cancels and verifies), workers probe the broker once before stopping their actors and, when it does not answer, verify and cancel their orders through their own direct venue client, and a broker that did not accept `stop` is terminated without a graceful wait.

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
