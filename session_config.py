"""Compatibility import for :mod:`core.session_config`."""

import sys
from core import session_config as _implementation

sys.modules[__name__] = _implementation
