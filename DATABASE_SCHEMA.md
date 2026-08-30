# Database and Artifact Schema

This document describes current durable storage. Monetary and price values use fixed-point integer units unless a column name explicitly says cents or basis points: 10,000 price units equal one dollar and 100 count units equal one contract.

## Saved sessions — `session_data/sessions.sqlite3`

### `sessions`

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | TEXT PK | UUID |
| `name` | TEXT UNIQUE | Operator-facing name |
| `description` | TEXT | Notes |
| `configuration_json` | TEXT | Validated schema-v2 configuration without credentials |
| `version` | INTEGER | Optimistic-lock version |
| `created_at_ms`, `updated_at_ms` | INTEGER | Epoch milliseconds |
| `archived_at_ms` | INTEGER nullable | Archive timestamp |

### `selection`

Singleton row mapping `singleton=1` to the selected `session_id`.

### `runs`

Immutable run snapshots: run/session IDs and name, configuration version/JSON, lifecycle status, creation/start/end/heartbeat timestamps, artifact path, summarized metrics JSON, and terminal error.

### `metric_samples`

Time-series run summaries keyed by auto-increment `id`, `run_id`, `timestamp_ms`, and lightweight `payload_json`. Indexed by `(run_id, timestamp_ms)`.

## Shard telemetry — `artifacts/<session>/<run>/shards/<worker>/telemetry.sqlite3`

Each worker has one shared WAL writer connection. Event tables contain `ticker` and have a `(ticker, timestamp)` index. Writes are committed in batches and flushed on worker heartbeats.

### `fills`

One row per fill: stable fill key, timestamp, ticker/side, trade and order IDs, price/size/fee, maker/taker flag, inventory before/after, fill-time fair value, best bids, and queue ahead. Fills are retained for the full run.

### `order_revisions`

The create/amend/decrease/cancel lifecycle: revision key, ticker, action, order/client IDs, side, placement/end timestamps, size/fill/price, book bid/ask/mid, ending state, and error. Retained for the full run and finalized to `Unknown` if a run ends with an unresolved resting revision.

### `quotes`

Every strategy decision: timestamp, ticker/side/mode, fair and reservation values, desired quote, expected edge, fee/toxicity/incentive/inventory/queue components, target size, queue ahead, spread, and inventory.

### `markouts`

Post-fill observations keyed by fill and horizon: timestamp, ticker/side, fill price, future market midpoint/fair value, adverse movement, and model bucket. Retained for the full run.

### `market_state`

Raw book-derived state: timestamp/ticker/source, best bids, implied asks, top sizes, ticker prices/sizes, last trade, trade bias, and inventory.

### `ticker_updates`

Raw ticker channel fields: price, bid/ask, volume, open interest, top sizes, and last-trade size.

### `public_trades`

Raw public trades: timestamp/ticker/trade ID, YES/NO prices, count, and taker side.

### `fill_prob_attempts`

Fill-probability calibration attempts: attempt ID, timestamp/ticker/side, bucket, quote price, and whether it filled within 30 seconds.

### `shard_market_metadata`

One row per ticker: title, series/event tickers, series title, canonical venue URL, and update timestamp. The legacy `market_metadata(singleton=1, ...)` table remains readable in old per-market databases.

### `runtime_events`

Structured worker, actor, adaptor, risk/watchdog, and execution events: timestamp, ticker, source, event type, severity, and JSON payload.

### `market_minute_aggregates`

Full-run one-minute counts keyed by `(ticker, minute_ms)`: quote, trade, fill, and runtime-event counts.

Raw `quotes`, `market_state`, `ticker_updates`, `public_trades`, and `runtime_events` rows older than seven days are aggregated and removed. Fills, order revisions, markouts, metadata, and minute aggregates are not removed by runtime retention.

## Portfolio analytics — `runtime/portfolio_analytics.sqlite3`

- `metadata`: singleton schema/retention metadata.
- `current_state`: latest serialized account portfolio snapshot.
- `summary_samples`: time series of cash, midpoint value, liquidation value, total portfolio value, and position count.
- `orders`: normalized current/recent account orders with prices, quantities, fills, fees, and timestamps.
- `fills`: normalized account fills with cost, fees, maker/taker state, and timestamps.
- `market_links`: cached ticker metadata and canonical URLs.

These tables represent account-wide monitoring, not just strategy-tagged activity.

## UI audit — `runtime/ui_audit.sqlite3`

The `audit` table stores request ID, timestamp, operator, action, target, result, and error for guarded UI controls.

## Non-database artifacts

| Path | Purpose |
| --- | --- |
| `configuration.json` | Immutable run configuration snapshot |
| `fleet_manifest.json` | Worker assignments and ticker-to-shard lookup |
| `launcher.log` | Controller lifecycle |
| `shards/<worker>/worker.log[.1-.4]` | Rotating worker/adaptor/actor log |
| `markets/<ticker>/settings.json` | Exact credential-free actor settings |
| `screener/latest.json` and timestamped JSON | Selection generation snapshots |
| `screener_export.csv` | Latest human-readable ranked selection |
| `watchdog_disable_list.json` | Persistent operator/risk disable state; status publishes only its count |

Historical `markets/<ticker>/telemetry.sqlite3`, `telemetry/telemetry_<ticker>.sqlite3`, and root-level `telemetry_<ticker>.sqlite3` databases remain read-only compatible. Readers check legacy and shard layouts; no migration rewrites old run artifacts.
