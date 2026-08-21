# Kalshi API Rate-Limit Top

Run the real-time terminal dashboard:

```bash
./kalshi_api_top.py
```

It refreshes once per second. Press `q` or `Ctrl-C` to exit. To change the
refresh speed:

```bash
./kalshi_api_top.py --interval 0.5
```

The summary uses the account tier, refill rates, and capacities returned by
Kalshi's account-limits endpoint. It separates Read and Write token spend,
shows trailing token usage and budget utilization, maintains a local estimate
of available bucket tokens, and counts HTTP 429 rate-limit responses.

At startup it retrieves Kalshi's public non-default endpoint-cost catalog. If
that lookup fails, it uses the documented default of 10 tokens and the current
2-token single-order cancellation cost. Use `--offline-costs` to deliberately
skip the lookup.

The available-token value is prefixed with `~` because Kalshi does not return
the server's remaining balance in response headers. It starts full and is then
estimated from API calls observed locally. Requests made by another machine or
application cannot be included.

The aggregated REST table remains below the bucket summary and combines all
bots, the screener, and the portfolio monitor without double-counting the
manager's aggregate.
