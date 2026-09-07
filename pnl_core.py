"""Compatibility import for :mod:`portfolio.pnl_core`."""

import sys
from portfolio import pnl_core as _implementation

sys.modules[__name__] = _implementation
