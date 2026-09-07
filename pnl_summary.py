"""Compatibility import for :mod:`portfolio.pnl_summary`."""

import sys
from portfolio import pnl_summary as _implementation

sys.modules[__name__] = _implementation
