# Kalshi_alg

A multi-market market-making system for [Kalshi](https://kalshi.com), the
regulated US event-contracts exchange. Each market gets its own bot process
plus a watchdog sidecar; a launcher orchestrates screener output, risk
profiling, and bot lifecycle.

This repo is shared as a working reference, not a turnkey product. It runs
real money against a live exchange, so several things you'd want for a public
release (config-as-code, packaging, tests) are intentionally minimal.

## Architecture

```
run_lip.sh
  └─ lip_launcher.py                  (orchestrator)
       ├─ kalshi_screener.py          (market selection → screener_export.csv)
       ├─ market_risk_profiler.py     (pre-trade safety gate per ticker)
       └─ for each selected ticker:
            ├─ V1.py <TICKER>                   (trading bot)
            └─ market_watchdog_runner.py <TICKER> (sidecar risk monitor)
```

## Components

| File | Role |
|---|---|
| `V1.py` | Single-market EV-based market-making bot. WebSocket book/ticker/trade/fill subscriptions, fair-value engine, EV quote selector, toxicity model, queue-position tracking, telemetry. |
| `lip_launcher.py` | Spawns bot + watchdog processes per selected market, handles refresh cycles, carryover, and shutdown. |
| `run_lip.sh` / `run_lip.bat` | Entry point. All tunable parameters (budgets, edge thresholds, watchdog confidence cutoffs, refresh intervals) live here. |
| `market_risk_profiler.py` | Pre-trade confidence scoring (start_source, official_close, sample count, continuous_price). Assigns `normal` / `reduction_only` / `flatten_only`. |
| `market_watchdog_runner.py` | Periodic re-profile sidecar that can downgrade or kill a bot when conditions degrade. |
| `kalshi_screener.py`, `kalshi_screener_config.py` | Liquidity / spread / volume screener. |
| `pnl_summary.py`, `markout_history.py` | Post-trade tooling — P&L roll-up and markout history extraction from telemetry. |
| `analysis/policy_backtest.py` | Quote-policy backtester over recorded telemetry. |
| `arb/` | Cross-book screener and ranker for related-market arbitrage candidates. |
| `helpers/getter.py` | Minimal WebSocket recorder for a single ticker (debugging / replay corpus). |
| `v1_orederbook_replay.py` | Offline orderbook replay harness. |

## Telemetry

Each bot writes to a per-market SQLite database (`telemetry_<TICKER>.sqlite3`)
with the following tables:

- `fills` — every executed fill (side, price, qty, timestamp).
- `quotes` — every place / cancel / replace decision.
- `markouts` — realized 5s and 30s markouts per fill.
- `fill_prob_attempts` — predicted vs. realized fill probability, used for
  calibrating the EV scorer.
- `market_state` — periodic book snapshots (best bid / ask, imbalance,
  inventory).
- `ticker_updates`, `public_trades` — exchange-side prints.

Telemetry databases and runtime logs are git-ignored. Two example logs are
included under `logs/` purely so the format is visible.

## Configuration

All credentials are read from environment variables — none are committed:

```bash
export KALSHI_API_KEY_ID="<your key id>"
export KALSHI_PRIVATE_KEY_PATH="/absolute/path/to/your_private_key.pem"
```

Then edit budgets / thresholds in `run_lip.sh` and run:

```bash
bash run_lip.sh
```

## Status

This is a personal research project. It is shared as evidence of work, not as
a recommendation to run it. Trading on Kalshi with this code (or any code) is
at your own risk.
