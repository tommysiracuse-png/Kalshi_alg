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
MAX_MARKETS_TO_SCAN = 500000
TOP_N = 80

# Liquidity / time filters
MIN_SPREAD_CENTS = 5
MAX_SPREAD_CENTS = 35
MIN_YES_BID_CENTS = 5
MIN_NO_BID_CENTS = 5
MIN_VOL24H = 2000   # raised from 500 — kills dead zero-fill slots
MIN_OI = 100
MIN_TIME_TO_CLOSE_HRS = 2
MAX_TIME_TO_CLOSE_HRS = 50

# Series prefixes to exclude entirely from screening.
# These were net money-losers in back-analysis: WTI/XRP spreads are too tight
# relative to their informed taker flow; LowtLAX and SpotstreamGlobal have the
# same problem.  Add a series prefix (uppercase) to keep it out of the bot.
EXCLUDED_SERIES: list[str] = [
    "KXWTI",             # avg -4.1¢/fill after 30 s adverse selection
    "KXXRPD",            # avg -8.9¢/fill
    "KXLOWTLAX",         # avg -4.1¢/fill
    "KXSPOTSTREAMGLOBAL",# avg -20¢/fill
]

# Quote-planning logic used only for the screener CSV output
TARGET_EDGE_CENTS = 2
FEE_BUFFER_CENTS = 0
DEFAULT_TICK_CENTS = 1
QUOTE_SIZE = 50

# Output
OUTPUT_CSV = "screener_export.csv"

# Optional loop mode
RUN_FOREVER = False
CYCLE_SECONDS = 30
