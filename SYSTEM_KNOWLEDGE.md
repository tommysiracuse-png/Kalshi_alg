# Kalshi Trading System - Deep Knowledge Reference

## Architecture Overview

The system is a multi-market prediction-market market-making system for Kalshi. It runs multiple bot instances in parallel, each trading a single ticker.

### Process Hierarchy
```
run_lip.sh
  └─ lip_launcher.py (orchestrator)
       ├─ kalshi_screener.py → screener_export.csv (market selection)
       ├─ market_risk_profiler.py (prestart safety gate per ticker)
       └─ For each selected ticker:
            ├─ V1.py <TICKER> (trading bot)
            └─ market_watchdog_runner.py <TICKER> (sidecar risk monitor)
```

### Data Flow
- **Screener** → CSV → **Launcher** reads picks → **Profiler** gates each → **Bot** + **Watchdog** spawned
- **Watchdog** writes JSON state file every 60s → **Bot** reads every 3s → mode changes
- **Bot** writes to: telemetry SQLite, logs/<TICKER>.log, logs/pnl_tracker.jsonl
- **Watchdog** writes to: logs/<TICKER>.watchdog.log

---

## V1.py - Core Trading Bot (~4967 lines)

### Main Loop Architecture
- `asyncio.run(bot.run())` → starts 3-4 background tasks:
  1. `requote_worker()` - main trading loop, triggered by `requote_event`
  2. `queue_position_worker()` - polls queue position every 10s
  3. `model_refresh_worker()` - refreshes toxicity/fill-prob models every 50s
  4. `watchdog_worker()` - reads watchdog JSON state every 3s (if configured)
- `websocket_main()` - subscribes to orderbook_delta, user_orders, fill, trade, ticker, market_positions

### Event-Driven Quoting
- Every orderbook snapshot/delta, trade, ticker update, fill, or market_position update sets `requote_event`
- `requote_worker` → `desired_quote_prices()` → `compute_quote_decisions()` → `ensure_side_quote()` per side
- Minimum 100ms between requotes

### Decision Pipeline (compute_quote_decisions)
1. `build_market_context()` - snapshot of book state: bids, asks, spread, imbalance, trade bias, inventory, queue positions, seconds_to_close
2. `maybe_market_probability_guard_triggered()` - blocks quoting if market is too skewed (YES bid < 8c or NO bid < 8c)
3. `quote_gate_status()` per side - 10+ gate checks:
   - `inventory_block` (one-way guard exceeded)
   - `same_side_reentry_cooldown` (5s after fill)
   - `queue_abandonment_cooldown` (15s)
   - `side_pull_cooldown` (orderbook pull detected, 5s)
   - `market_queue_cooldown` / `market_toxicity_cooldown`
   - `best_bid_floor` (min 8c)
   - `thin_top_level` (min 3 contracts at best bid)
   - `wide_book_gap` (max 15c gap to 2nd level)
   - `market_probability_guard`
   - `no_exit_liquidity` (for inventory reduction only)
4. `FairValueEngine.estimate()` - blended mid/ticker/trade EWMA with time-decay alpha
5. `compute_side_desired_quote_price()` - join/improve BBO logic
6. `build_side_quote_decision()` per side - THE CORE:
   - Computes reservation price (fair_side - toxicity - fee - inv_penalty)
   - Scans up to 22 candidate price levels from market_target outward
   - For each: edge = fair_side - candidate_price - fee - markout - inv_penalty - queue_penalty + incentive
   - Score = fill_probability_30s * edge * (3600 / horizon_seconds)
   - Picks highest score_per_hour candidate
   - Modes: `ev_quote` | `inventory_reduction` | `negative_ev` | `disabled` | `thin_top_level` | `inventory_block` | `inventory_wait`
7. `clamp_pair_to_avoid_self_cross()` + `apply_pair_guard()` (yes + no bids <= 97c)
8. Floor checks, telemetry recording

### FairValueEngine
- Weighted blend: mid (0.55), ticker (0.25), trade (0.20)
- Adjustments: orderbook imbalance (±2c max), trade bias (±4c max)
- Time urgency multiplier: 1.25x (< 15min), 1.0x (15-60min), 0.80x (1-6h), 0.60x (>6h)
- EMA smoothing alpha: 0.90 (<30min), 0.80 (30-60min), 0.60 (>60min) — faster alpha near expiry to reduce stale-fill toxicity

### ToxicityModel
- Bucketed by: side | time_to_close | imbalance | trade_bias | queue_ahead | spread | inventory_regime
- Bootstrap from telemetry markout table
- EWMA: 0.85 * old + 0.15 * new adverse_units
- Horizon blend: 70% weight on 5s markout, 30% on 30s markout
- Additional flow/imbalance adjustments: +0.5c per unit adverse trade flow, +0.3c per unit adverse imbalance
- Default toxicity: 1 cent

### FillProbabilityModel
- Same bucket_key system as toxicity
- Bayesian: (fills + alpha) / (attempts + alpha + beta), alpha=2.0, beta=3.0 (prior)
- Tracks pending attempts per side, settles after 60s horizon
- Used for score_per_hour calculation: pfill * edge * 3600/horizon

### Order Execution
- `ensure_side_quote()`: Creates new or amends existing resting order
- Post-only orders with 500s expiration (refreshed 30s before)
- Up to 3 post_only_reprice attempts (stepping back 1 tick each time)
- Amend-in-place when price changes (preserves queue priority when size changes)
- Fill handling: updates position, records to telemetry + pnl_tracker, schedules markout measurements, triggers requote

### Key Safety Mechanisms
- **Inventory guard**: Max 8 contracts one-sided, quoting stops on accumulating side
- **Pair guard**: YES bid + NO bid ≤ 97c (prevents self-arbitrage + captures profit buffer)
- **Same-side cooldown**: 5s post-fill before re-entering same side (anti-adverse-selection)
- **Orderbook pull detection**: Tracks depth removal >150 contracts or >20% in 1.5s window → 5s cooldown + 7c penalty
- **Queue abandonment**: If >200 contracts ahead for 3 consecutive polls, abandon position
- **Thin top level guard**: Won't quote if <3 contracts at best bid
- **Wide book gap**: Won't quote if gap to 2nd level > 15c

---

## lip_launcher.py - Orchestrator

### Startup Sequence
1. Run screener → CSV
2. Load picks from CSV (top N by liquidity score)
3. `spawn_bots()` iterates picks SERIALLY:
   a. Run prestart profiler (subprocess, blocks ~2-4s per ticker)
   b. **SAFETY GATE**: If profile returns `flatten_only` or `effective_close <= now` → disable ticker permanently
   c. If `reduction_only` with 0 inventory → skip (don't waste a bot slot)
   d. Spawn V1.py + watchdog as subprocess pair
   e. Sleep `launch_delay_seconds` (0.5s)

### CRITICAL: Prestart Profiling is NOT Optional
The prestart profile serves three essential functions:
1. **Seeds initial watchdog state file** so the bot has risk state from second zero
2. **Gates whether to launch at all** — prevents launching into stale/closed/dangerous markets
3. **Detects flatten_only/reduction_only** before committing resources + budget

### Refresh Cycle (monitor_and_refresh)
- Every `refresh_interval_seconds` (1400s = ~23 min):
  1. Detect dead children, disable tickers that died with watchdog exit codes
  2. Lookup inventory via REST API for all running tickers
  3. Keep inventory tickers alive as carryover (net position != 0)
  4. Stop all children
  5. Re-run screener
  6. Merge screener picks + inventory carryovers
  7. Spawn fresh bots for merged set

---

## market_risk_profiler.py - Risk Assessment

### Confidence Scoring
- Base: 0.40
- +0.30 for ticker_datetime_token (can parse date/time from ticker)
- +0.10 for sufficient duration
- +0.10 for official_close present
- +0.10 for price samples collected
- Continuous price boost: +0.05-0.10 when yes_bid ∈ [15, 85] cents

### Market Family Detection
Infers from ticker pattern: mlb, nba, nhl, soccer, esports, speech_event, weather, continuous_price, generic_binary. Each family has specific close-time compression rules.

### Mode Assignment
- **normal**: confidence >= reduction_threshold (0.68), before soft_stop_time
- **reduction_only**: confidence < reduction_threshold OR between soft_stop and flatten_only_time
- **flatten_only**: confidence < flatten_threshold (0.55) OR past flatten_only_time OR effective_close invalid

---

## market_watchdog_runner.py - Sidecar Monitor

- Runs profiler every 60s with `sample_seconds=1.5`
- Writes normalized JSON to watchdog_state/<TICKER>.json
- Bot reads this file every 3s via `maybe_load_watchdog_state()`
- Bot reacts: if mode is `flatten_only` → emergency cancel + exit orders + shutdown
- If watchdog state is stale (>120s old) → bot shuts down

---

## Current Parameters (run_lip.sh)

| Parameter | Value | Notes |
|-----------|-------|-------|
| YES_BUDGET_CENTS | 400 | $4 per market per side |
| NO_BUDGET_CENTS | 400 | $4 per market per side |
| MAX_BOTS | 100 | No bot count limit effectively |
| LAUNCH_DELAY_SECONDS | 0.5 | Between bot spawns |
| REFRESH_INTERVAL_SECONDS | 1400 | ~23 min cycle |
| WATCHDOG_INTERVAL_SECONDS | 60 | |
| WATCHDOG_EXTREME_STALE_SECONDS | 120 | |
| WATCHDOG_CONFIDENCE_REDUCTION_THRESHOLD | 0.68 | |
| WATCHDOG_CONFIDENCE_FLATTEN_THRESHOLD | 0.55 | |
| MINIMUM_CARRYOVER_VALUE_CENTS | 50 | |

## Current Parameters (V1.py BotSettings defaults)

| Parameter | Value | Notes |
|-----------|-------|-------|
| minimum_expected_edge_cents_to_quote | 2 | MIN_EDGE_CENTS |
| minimum_expected_edge_cents_to_keep_quote | 1 | |
| inventory_reduction_max_negative_edge_cents | 6 | |
| default_fee_factor_for_maker_quotes | 0.45 | FEE_FACTOR |
| one_way_inventory_guard_contracts | 8 | |
| inventory_skew_contracts_per_tick | 2 | |
| maximum_inventory_skew_ticks | 6 | |
| same_side_reentry_cooldown_ms | 5000 | |
| post_fill_no_improve_cooldown_ms | 2000 | |
| resting_order_expiration_seconds | 500 | |
| aggressive_improvement_ticks_when_spread_is_wide | 2 | |
| minimum_spread_ticks_required_for_aggressive_improvement | 8 | |
| passive_offset_ticks_when_not_improving | 0 | |
| minimum_top_level_depth_contracts | 3 | |
| maximum_top_level_gap_cents | 15 | |
| candidate_price_levels_to_scan | 22 | |
| orderbook_pull_absolute_threshold_contracts | 150 | |
| orderbook_pull_penalty_cents | 7 | |
| maximum_queue_ahead_contracts_before_abandonment | 200 | |

## Screener Config (kalshi_screener_config.py)

| Parameter | Value |
|-----------|-------|
| MIN_SPREAD_CENTS | 5 |
| MAX_SPREAD_CENTS | 35 |
| MIN_YES_BID_CENTS | 5 |
| MIN_NO_BID_CENTS | 5 |
| MIN_VOL24H | 500 |
| MIN_OI | 100 |
| MIN_TIME_TO_CLOSE_HRS | 2 |
| MAX_TIME_TO_CLOSE_HRS | 50 |
| Excluded keywords | LOWT, HIGH, RAIN |

---

## Performance Observations (Apr 13-15, 2026)

### Fill Statistics
- 1496 total fills across 142 unique tickers over ~36 hours
- Categories: other (670), weather (401), baseball (150), commodities (104), tennis (101), esports (24), crypto (23), basketball (16), hockey (4), politics (3)
- 100/142 tickers have remaining inventory (not flat)
- 42 tickers fully closed (net_pos = 0)
- Grand total P&L: +$111.54 (realized: +$11.58, unrealized: +$99.96)
- Fees: $0.66 total (very low, most fills at 0 fee)

### Known Issues
- **No-side fill prices are null**: 727/772 no-side fills have `price_c = null` in pnl_tracker.jsonl. The `handle_fill_update` derives no_price from `PRICE_SCALE - yes_price_units`, but Kalshi's fill message appears to not include yes_price for many fills. This affects P&L accuracy.
- **Large inventory positions**: Some tickers accumulate 40-80 contract positions (e.g., KXSERIEABTTS +79, KXMLBTB +63). The 8-contract inventory guard should limit this, but it seems some categories allow larger accumulation (sports total markets, weather).
- **Most common quoting blocks**: thin_top_level, negative_ev, side_pull_cooldown, same_side_reentry_cooldown
- **Tennis markets frequently hit reduction_only**: Risk elevated from rapid price moves during matches
- **Fill probability is low**: Actual fill rates are 0-5% across most buckets (markets are thick/competitive)

### Log Patterns to Watch
- `FILL_TELEMETRY` — every fill with side, quantity, fee, new position
- `negative_ev` — all candidates rejected (edge too thin)
- `thin_top_level` — top-of-book too thin to quote
- `side_pull_cooldown` — orderbook pull toxicity detected
- `WATCHDOG_MODE_CHANGE` — mode transitions (normal→reduction_only→flatten_only)
- `QUEUE_AHEAD_BREACH` / `QUEUE_ABANDON_SIDE` — queue position problems
- `POST_ONLY_RETRY` — crossing the spread, stepping back

---

## Critical Lessons Learned

1. **Prestart profiling is a SAFETY GATE, not just bookkeeping.** It prevents launching bots into markets that are closing, stale, or too risky. Never skip or defer it.

2. **The real startup bottleneck is serial execution** of prestart profiles (each blocks on subprocess + network IO). The profiling concept itself is correct; the serialization is the actual problem.

3. **Always read code thoroughly before recommending changes to safety-critical paths.** The profiler's gate-check logic (lines 870-890 of lip_launcher.py) would have been missed in a surface-level read.

4. **Watchdog exit codes matter**: Codes in `WATCHDOG_EXIT_CODES` (61, 62, 64) cause permanent ticker disabling via `watchdog_disable_list.json`. This persists across launcher restarts.

5. **FairValueEngine is time-aware**: EMA alpha changes based on seconds_to_close. Near expiry (< 30 min), alpha is 0.90 (nearly no smoothing) to avoid stale-fill adverse selection. Far from expiry, alpha is 0.60 (heavier smoothing).

6. **Score_per_hour is the key ranking metric** for candidate prices, not raw edge. This means the model prefers lower-edge, higher-fill-probability prices over high-edge prices that never fill.

7. **Inventory reduction mode allows negative edge** up to `inventory_reduction_max_negative_edge_cents` (6c). This is the mechanism for unwinding positions when the market moves against you.
