"""Compatibility import for :mod:`portfolio.portfolio_analytics`."""

import sys
from portfolio import portfolio_analytics as _implementation

sys.modules[__name__] = _implementation
