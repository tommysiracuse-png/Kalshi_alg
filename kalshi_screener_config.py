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
MAX_MARKETS_TO_SCAN = 20000
TOP_N = 50

# Liquidity / time filters
MIN_SPREAD_CENTS = 4
MAX_SPREAD_CENTS = 40
MIN_YES_BID_CENTS = 20
MIN_NO_BID_CENTS = 20
MIN_VOL24H = 10000
MIN_OI = 5000
MIN_TIME_TO_CLOSE_HRS = .5
MAX_TIME_TO_CLOSE_HRS = 10000

# Quote-planning logic used only for the screener CSV output
TARGET_EDGE_CENTS = 4
FEE_BUFFER_CENTS = 0
DEFAULT_TICK_CENTS = 1
QUOTE_SIZE = 50

# Output
OUTPUT_CSV = "screener_export.csv"

# Optional loop mode
RUN_FOREVER = False
CYCLE_SECONDS = 30
