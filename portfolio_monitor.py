"""Compatibility import for :mod:`portfolio.portfolio_monitor`."""

import sys
from portfolio import portfolio_monitor as _implementation

sys.modules[__name__] = _implementation
