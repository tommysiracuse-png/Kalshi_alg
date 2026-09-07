"""Compatibility import for :mod:`core.market_classes`."""

import sys
from core import market_classes as _implementation

sys.modules[__name__] = _implementation
