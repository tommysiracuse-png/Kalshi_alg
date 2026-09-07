"""Compatibility import for :mod:`core.fleet_models`."""

import sys
from core import fleet_models as _implementation

sys.modules[__name__] = _implementation
