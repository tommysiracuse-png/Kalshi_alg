"""Compatibility import for :mod:`watchdogs.markout_history`."""

import sys
from watchdogs import markout_history as _implementation

sys.modules[__name__] = _implementation
