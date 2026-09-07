"""Compatibility import for :mod:`screeners.kalshi_screener_config`."""

import sys
from screeners import kalshi_screener_config as _implementation

sys.modules[__name__] = _implementation
