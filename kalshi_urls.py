"""Compatibility import for :mod:`core.kalshi_urls`."""

import sys
from core import kalshi_urls as _implementation

sys.modules[__name__] = _implementation
