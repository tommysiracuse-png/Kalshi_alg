"""Compatibility import for :mod:`screeners.screener`."""

import sys
from screeners import screener as _implementation

sys.modules[__name__] = _implementation
