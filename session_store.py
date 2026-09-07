"""Compatibility import for :mod:`core.session_store`."""

import sys
from core import session_store as _implementation

sys.modules[__name__] = _implementation
