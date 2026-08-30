# Screener Architecture and Flow

The screener is a market-making opportunity ranker. It does not predict which contracts will resolve YES or NO. It looks for markets where the bot could place a passive bid with positive estimated value after accounting for fees, adverse selection, order-book queue, and inventory risk.

```text
Startup / scheduled / operator refresh
                    |
                    v
       Download open Kalshi markets
                    |
                    v
  Normalize prices, sizes, ticks, and times
                    |
                    v
 Eligibility + historical-toxicity filters
                    |
                    v
 Estimate fair value and simulate YES/NO quotes
                    |
                    v
       Rank passing markets by EV
              +-----+-----+
              |           |
              v           v
     Atomic CSV export   Fleet selection
                            |
                 Disabled-market filtering
                            |
                    Inventory carryover
                            |
                    Compare old/new fleet
                            |
                 Start / stop / keep bots
```

## 1. What Starts the Screener

The launcher can invoke the screener in several ways:

- At session startup when `runScreenerOnStart` is enabled.
- On the configured refresh interval.
- When the operator requests a refresh.
- When a disabled ticker is re-enabled.
- As a standalone one-shot command.
- If startup screening is disabled, the launcher can seed the initial fleet from the existing CSV instead.
- A fixed-ticker session bypasses screening entirely.

The refresh runs in a worker thread so the launcher can continue publishing status while the market scan is in progress. If screening fails, the previous successful fleet remains active; the failed result is not applied.

Relevant code:

- [`launcher.py`](launcher.py)
- [`screener.py`](screener.py)

## 2. Market Retrieval

The screener requests Kalshi's public market endpoint in pages of up to 1,000 markets.

Current defaults are:

| Setting | Default |
|---|---:|
| Market status | `open` |
| Multivariate markets | Excluded |
| Maximum markets scanned | 20,000 |
| CSV rows retained | 200 |

The 20,000-market limit is a hard memory-safety boundary. If a configuration asks for an unlimited or larger scan, it is reduced to 20,000 and a warning is recorded.

Each Kalshi market is converted into a normalized screening payload containing:

- Ticker, title, event, and series identifiers.
- YES and NO bids and asks.
- Bid and ask sizes.
- Last traded price.
- Tick size and price ranges.
- 24-hour volume and open interest.
- Close and expiration timestamps.

Relevant code:

- [`screener.py`](screener.py)
- [`adaptors/kalshi.py`](adaptors/kalshi.py)
- [`kalshi_screener_config.py`](kalshi_screener_config.py)

## 3. Order-Book Normalization

Because a Kalshi contract is binary, one side can imply the other side's ask:

```text
YES ask = 100 - NO bid
NO ask  = 100 - YES bid
```

When both an explicit and implied ask exist, the screener uses the lower ask. It then aligns bids and asks to the market's tick size.

The binary spread is calculated as:

```text
Spread = 100 - YES bid - NO bid
```

A market is discarded if:

- Either bid is missing or invalid.
- The complementary book is invalid.
- The spread is zero or negative.
- A usable cutoff time cannot be calculated.

The cutoff is the earliest available value among `close_time`, `expected_expiration_time`, and `expiration_time`.

Relevant code: [`kalshi_screener.py`](kalshi_screener.py)

## 4. Initial Market Filters

Before doing the more expensive quote calculations, a market must pass these current defaults:

| Filter | Requirement |
|---|---:|
| YES bid | At least 5 cents |
| NO bid | At least 5 cents |
| Binary spread | 4-35 cents |
| 24-hour volume | At least 500 contracts |
| Open interest | At least 100 contracts |
| Time until cutoff | 3-50 hours |

The strategy behind these limits is:

- Avoid almost-certain contracts where one side has little usable liquidity.
- Avoid spreads too narrow to cover trading costs.
- Avoid extremely wide, stale, or malformed markets.
- Require evidence that the market is active.
- Avoid markets that are about to close.
- Avoid tying up capital in markets too far from resolution.

Specific series can also be excluded through configuration. The ticker-keyword exclusion mechanism exists, but its current default list is empty; the temperature and rain examples in the configuration are commented out.

## 5. Historical Toxicity Strategy

The screener attempts to exclude markets that have produced bad fills previously.

For every historical fill with a five-second markout, it calculates approximately:

```text
YES net = future YES midpoint - fill price - estimated fee
NO net  = future NO midpoint  - fill price - estimated fee
```

The results are size-weighted, so a bad 100-contract fill counts much more than a bad one-contract fill.

A ticker is excluded when it has at least three fills and either:

- Average net markout is below -1 cent per contract, or
- Total net markout is below -300 cents.

An entire series can be excluded under the same loss conditions, but it requires at least 20 fills because excluding a series also removes new strikes that have never traded.

The current lookback is 14 days, with a five-second markout horizon. If the history reader fails, the screener fails open and continues without this filter.

### Telemetry Integration Caveat

The history reader currently searches for repository-level files named `telemetry_*.sqlite3`, while newly launched bots write `telemetry.sqlite3` inside per-run market artifact directories. Consequently, new run-artifact telemetry is not automatically included by this historical filter.

Relevant code:

- [`markout_history.py`](markout_history.py)
- [`lip_launcher.py`](lip_launcher.py)

## 6. Fair-Value Strategy

For each remaining market, the screener estimates a heuristic YES fair value from:

- The YES bid/ask midpoint, weighted 55%.
- The last traded YES price, weighted 45%.
- An order-book imbalance adjustment of up to plus or minus 4 cents.

Order-book imbalance is:

```text
YES imbalance =
    (YES bid size - YES ask size)
    / (YES bid size + YES ask size)
```

A bid-heavy book raises the estimated YES value; an ask-heavy book lowers it. The result is rounded to a valid tick. NO fair value is `100 - YES fair`.

This is entirely market-derived. It does not use news, event fundamentals, external probabilities, or a machine-learning resolution forecast.

## 7. Passive-Quote Strategy

YES and NO are evaluated independently.

For each side, the screener:

1. Starts two ticks behind the current best bid.
2. Scans up to six successive price levels.
3. Never crosses the ask; the maximum candidate is one tick below the ask.
4. Estimates the value and costs of placing a passive order at each level.
5. Keeps the highest-EV candidate that clears the minimum edge.

The expected-value calculation is:

```text
EV =
    estimated fair value
  - quote price
  - estimated maker fee
  - toxicity allowance
  - inventory penalty
  - queue penalty
  + incentive
```

The current minimum passing EV is 2 cents per contract.

The cost components are:

- **Maker fee:** Estimated using the contract price and a maker-fee factor.
- **Toxicity:** A base 3-cent allowance, increased when book imbalance is adverse to the side being quoted.
- **Queue penalty:** Based on contracts already ahead at the best bid, capped at 6 cents.
- **Inventory penalty:** Discourages adding to an existing directional position, while quotes that reduce the position receive no penalty.
- **Incentive:** Currently always zero.

The general screener currently uses a zero net-position setting, so its quote-level inventory penalty is normally zero. Actual account inventory is handled later during fleet reconciliation.

If neither YES nor NO passes, the market is removed. If both pass, the side with the higher EV becomes the market's `Best side`.

## 8. Ranking and CSV Generation

Passing markets are sorted by:

1. Highest best-side EV.
2. Highest nominal EV for the configured 50-contract quote size.
3. Highest 24-hour volume.
4. Highest open interest.
5. Shortest time to cutoff.

The screener assigns rank numbers and retains the first 200 rows by default.

The CSV contains:

- Rank and ticker.
- Best side and best EV.
- Suggested YES and NO bid prices.
- YES and NO fair values.
- Fee, toxicity, inventory, and queue deductions.
- Queue size estimates.
- Current book prices and sizes.
- Spread and tick size.
- Volume and open interest.
- Cutoff time.
- Candidate-level debugging information.
- Market title and series URL.

The output is written to a temporary file and atomically renamed to `screener_export.csv`. This prevents another process from reading a partially written CSV.

### CSV Versus Running Fleet

The CSV is the top-ranked screening output before disabled-market filtering and inventory carryover. Therefore:

- A disabled ticker can still appear in the CSV but will not be launched.
- An inventory-carryover market can remain in the bot fleet without appearing in the new CSV.
- The CSV's 200 rows are candidates; they are not necessarily 200 running bots.

## 9. Turning Rankings Into Running Bots

The launcher converts ranked rows into `ScreenerPick` objects and:

- Skips disabled tickers.
- Applies the configured YES and NO budgets.
- Stops after reaching the session's `maxBots` value.
- Enforces an additional hard fleet ceiling of 40 bots.

It then checks markets that were running previously but disappeared from the new screen:

- If there is no position, remove the bot.
- If the position is above the configured carryover value, retain the bot.
- If the position lookup fails, retain it as `inventory_unknown` for safety.
- If carryovers consume all available slots, they displace the lowest-ranked new candidates.

Finally, it compares the desired fleet with the existing fleet:

- `added`: Start a bot.
- `removed`: Stop the bot and clean up its orders.
- `kept`: Leave it running.
- `changed`: Restart it with new budget settings.

Relevant code:

- [`screener.py`](screener.py)
- [`bot_manager.py`](bot_manager.py)
- [`fleet_models.py`](fleet_models.py)

## 10. Connections to Other Processes

The screener connects to the rest of the application in four primary ways.

### Launcher

The launcher schedules refreshes, exposes screener status, and saves `latest.json` plus timestamped screener snapshots in the run artifacts.

### Bot Manager

The bot manager turns the desired-market difference into child-process starts, stops, and restarts. It also enforces the hard concurrent-bot limit and skips disabled markets.

### Trading Bots

Each selected market receives its own bot process with the ticker and configured YES/NO budgets. Once running, each bot performs its own continuous fair-value, quote, order, and risk calculations. It does not blindly reuse the screener's suggested CSV quote.

### Portfolio Monitor

The portfolio monitor runs alongside the screener. It does not determine the initial ranking, although the screener independently queries positions and current quotes while deciding whether to retain inventory carryovers.

## Summary

The complete intent of the screener is to:

1. Scan a bounded universe of active markets.
2. Remove unsafe, illiquid, badly timed, and historically toxic candidates.
3. Estimate market-derived YES and NO fair values.
4. Simulate passive quotes and deduct estimated trading costs.
5. Rank markets by expected market-making value.
6. Produce an auditable CSV of candidate markets.
7. Reconcile the rankings with disabled markets and existing inventory.
8. Tell the bot manager which market processes to start, stop, restart, or retain.

The screener chooses where the system should consider operating. The individual trading bots remain responsible for real-time execution and risk management after launch.
