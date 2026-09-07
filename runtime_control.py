"""Compatibility import for :mod:`core.runtime_control`."""

import sys
from core import runtime_control as _implementation

sys.modules[__name__] = _implementation
