"""Configuration for the Kalshi screener terminal script.

Edit these values directly, or override some of them with CLI flags in
kalshi_screener.py.
"""

USE_DEMO = False
KALSHI_HOST = "https://demo-api.kalshi.co" if USE_DEMO else "https://api.elections.kalshi.com"
API_PREFIX = "/trade-api/v2"

# Market scan settings
STATUS = "open"                 # open / unopened / paused / closed / settled
MVE_FILTER = "exclude"          # excludes multivariate combo markets
MAX_MARKETS_TO_SCAN = 600000
TOP_N = 200

# Liquidity / time filters
MIN_SPREAD_CENTS = 4
MAX_SPREAD_CENTS = 35
MIN_YES_BID_CENTS = 5
MIN_NO_BID_CENTS = 5
MIN_VOL24H = 500
MIN_OI = 100
MIN_TIME_TO_CLOSE_HRS = 3
MAX_TIME_TO_CLOSE_HRS = 50

# Ticker keywords to permanently exclude from screening.
# Any market whose ticker contains one of these substrings (case-insensitive)
# is silently dropped.  Temperature markets get stuck and are consistent losers.
EXCLUDED_TICKER_KEYWORDS: list[str] = [
    "LOWT",        # low-temperature markets (all cities)
    "HIGH",        # high-temperature markets (all cities — KXHIGHNY, KXHIGHTDC, etc.)
    "RAIN",        # rain markets
]

# Quote-planning logic used only for the screener CSV output
TARGET_EDGE_CENTS = 2
FEE_BUFFER_CENTS = 0
DEFAULT_TICK_CENTS = 1
QUOTE_SIZE = 50

# Markout-based realized-economics filter.
# All numbers are GROUND-TRUTH realized P&L: each fill is marked to the
# observed future market mid (markouts.future_mid_yes_units), NOT to the
# bot's own claimed fair value. This eliminates the bias that occurs when
# the bot's fair-value model is wrong on toxic flow.
#
# Net per contract:
#   YES buy at price P:  net = future_mid_yes - P - estimated_fee
#   NO  buy at price P:  net = (100 - future_mid_yes) - P - estimated_fee
# Aggregation is size-weighted: a 100-lot bad fill dominates a 1-lot bad fill,
# so the threshold reflects actual dollar bleed rather than fill counts.
#
# Calibrated 2026-04-29 from live ground-truth telemetry:
#   - Trump/Putin (toxic):  avg_net=-2.05c/ctr, total_net=-$4.93
#   - ITFW-VEDPAL (toxic):  avg_net=-3.41c/ctr, total_net=-$5.38
#   - Brent T97.50 (good):  avg_net=+0.32c/ctr, total_net=+$0.29
#   - Brent T96.50 (good):  avg_net=+0.33c/ctr, total_net=+$0.28
# Avg threshold -1.0c catches all toxic markets while leaving profitable
# wide-spread markets alone. min_fills=3 matches actual fill volume.
MARKOUT_FILTER_ENABLED = True
MARKOUT_FILTER_NET_THRESHOLD_CENTS = -1.0   # avg net per contract
# Ticker-level filter is AGGRESSIVE: only blocks the specific strike that has
# proven unprofitable. Low evidence bar is fine because the blast radius is
# limited to one ticker.
MARKOUT_FILTER_TICKER_MIN_FILLS = 3
# Series-level filter is CONSERVATIVE: blocks every strike in a product family
# including brand-new ones that have never traded. Requires a much higher
# evidence bar so a few bad fills don't kill an entire viable product line.
MARKOUT_FILTER_SERIES_MIN_FILLS = 20
MARKOUT_FILTER_HORIZON_SECONDS = 5          # 1/5/30/120 recorded; 5 has best signal-to-noise
MARKOUT_FILTER_LOOKBACK_DAYS = 14.0         # ignore markouts older than this
MARKOUT_FILTER_FEE_FACTOR = 0.55            # must match V1.py default_fee_factor_for_maker_quotes
# Total session dollar-bleed hard stop. Drops markets whose size-weighted
# total realized net (in cents) is below this. Default -300c = -$3.
MARKOUT_FILTER_TOTAL_NET_THRESHOLD_CENTS = -300.0

# Output
OUTPUT_CSV = "screener_export.csv"

# Optional loop mode
RUN_FOREVER = False
CYCLE_SECONDS = 30
