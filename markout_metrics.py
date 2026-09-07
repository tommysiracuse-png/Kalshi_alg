"""Compatibility import for :mod:`core.markout_metrics`."""

import sys
from core import markout_metrics as _implementation

sys.modules[__name__] = _implementation
