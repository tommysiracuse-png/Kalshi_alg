# Polymarket mirror monitor

`monitor_polymarket_mirror.py` is a read-only operational monitor for the
Polymarket mirror and its incremental screener updates. It combines the live
status snapshots with the mirror and session SQLite databases so that API
health, book freshness, open-interest hydration, and screening activity can be
checked from one command.

The monitor never writes to the mirror, launcher, or session databases.

## Quick start

Run one report from the repository root:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py
```

Refresh the report every 10 seconds:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py --interval 10
```

The default terminal report is cleared and redrawn on each refresh. Keep the
previous reports visible with `--no-clear`:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py \
  --interval 10 --no-clear
```

## JSON output

Print one machine-readable report for a dashboard, log collector, or shell
script:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py --json
```

The JSON report contains these top-level sections:

| Section | Contents |
| --- | --- |
| `mirror` | Status-file fields and aggregates read from the mirror database |
| `launcher` | Launcher lifecycle and Polymarket bot counts |
| `api` | REST/WebSocket usage, operation counters, rate-limit data, and OI hydration counters |
| `screener` | Recent Polymarket incremental screening aggregates and the latest update |
| `errors` | Read or database errors encountered while collecting the report |

For continuous JSON collection, run the monitor under a supervisor and use
one-shot mode at the supervisor's polling interval. `--json --interval` emits
multiple formatted JSON documents to stdout and is intended for interactive
use, not as one single JSON document.

## What the report shows

### Mirror state

The mirror section reports the status snapshot's:

- catalog count and active generation;
- book-ready market count and book coverage;
- newest-book age and current synchronization reason;
- whether the mirror is running and screenable.

The database section independently counts active markets in the published
generation. This is useful when catalog synchronization is still publishing
partial generations or when the status file and database are captured between
two updates.

### Open interest

The monitor separates:

- missing OI (`NULL`);
- explicit API OI of zero;
- positive OI; and
- OI at or above 100 units, which is the default screener threshold.

Internally, Polymarket count values use fixed-point units where `100` stored
units represent one displayed contract/count unit. Therefore the report's
`openInterestAtLeast100` count checks `open_interest_units >= 10000`.

If the mirror status exposes hydration counters, the report also shows the
number of OI markets requested, resolved, missing, and the number of batches
or API errors.

### Books

Book aggregates use the newest timestamp from either the Yes or No token for
each active market. The default freshness threshold is 60 seconds and can be
changed with `--freshness`.

`marketsWithFreshBooks` is a coverage diagnostic, not an exact screener row
count. The screener requires both outcome books to be present and fresh, while
this monitor uses the newest side to show whether a market has any recently
updated book data. The mirror status's `bookReadyMarkets` is the better value
for the screener's current readiness state.

### API usage

API counters come from the launcher's Polymarket activity snapshot when it is
available. For each REST operation the report shows:

- cumulative requests, successes, and errors;
- requests and errors during the last 60 seconds; and
- average latency.

It also reports WebSocket messages, connections, reconnects, closes, and
stream errors. The `polymarket_get_open_interest` row is the most useful check
for the Data API hydration path. OI output also includes:

- HTTP 403 and Cloudflare-response counts;
- bounded retry and exhausted-retry counts; and
- the latest Cloudflare Ray ID, cache status, batch, and attempt when available.

Cloudflare response bodies are intentionally omitted from activity output. The
structured HTTP error still remains available to the Polymarket adaptor for
classification and retry decisions.

### Screener aggregates

The screener section summarizes Polymarket rows in `screener_runs` during the
last 15 minutes by default:

- number of incremental updates by status;
- total scanned-market observations;
- average and maximum markets scanned per update;
- screener API requests and errors; and
- added, changed, and removed market counts.

The scanned total is the sum of per-update scan counts. It is not a distinct
market count because incremental updates may scan the same market more than
once and the run-history table does not store every scanned market ID.

## Command-line options

```text
--interval SECONDS
    Refresh continuously. Zero, the default, prints one report.

--window SECONDS
    Screener aggregation window. Default: 900 seconds (15 minutes).

--freshness SECONDS
    Book freshness threshold for database aggregates. Default: 60 seconds.

--json
    Print the complete report as JSON.

--no-clear
    Do not clear the terminal between refreshes.

--operation-limit COUNT
    Maximum number of REST operations printed. Default: 15.

--mirror-status PATH
--launcher-status PATH
--mirror-db PATH
--sessions-db PATH
    Override the default runtime and session paths.
```

Show the built-in help:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py --help
```

## Examples

Watch the mirror and retain a one-hour screener window:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py \
  --interval 15 --window 3600
```

Use a five-minute freshness threshold while diagnosing slow book updates:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py \
  --freshness 300
```

Monitor a copied runtime directory:

```bash
.venv/bin/python tools/monitor_polymarket_mirror.py \
  --mirror-status /path/to/runtime/polymarket_mirror_status.json \
  --launcher-status /path/to/runtime/launcher_status.json \
  --mirror-db /path/to/runtime/polymarket_mirror.sqlite3 \
  --sessions-db /path/to/session_data/sessions.sqlite3
```

## Interpreting common states

### API requests succeed but OI is mostly missing

Check `polymarket_get_open_interest` and the OI hydration counters together.
Successful HTTP requests with a low resolved count indicate that the API
returned fewer market records than requested. This is different from a
transport error and should be investigated through the request encoding or
the API response shape.

### Fresh books exist but no markets are added

Compare:

1. `bookReadyMarkets` in the mirror section;
2. `freshBooksWithOpenInterest` in the database section;
3. the screener's scanned and added counts; and
4. the `OI missing`, `OI zero`, and `OI >= 100` counts.

If scanning is occurring and OI is present, the remaining screener filters
(volume, bids, spread, time to close, exclusions, and markout rules) may be
rejecting the candidates. This monitor reports the input coverage and output
counts; it does not attribute every rejection to an individual filter.

### No Polymarket bots

`configured=0` and `running=0` in the Bots line means the launcher currently
has no Polymarket bot configuration. Check the latest screener `added` count
and the selected session configuration before diagnosing worker or execution
problems.

## Default input paths

When run from any directory, the script resolves paths relative to the
repository containing the script:

```text
runtime/polymarket_mirror_status.json
runtime/launcher_status.json
runtime/polymarket_mirror.sqlite3
session_data/sessions.sqlite3
```
