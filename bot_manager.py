"""Compatibility import for :mod:`core.bot_manager`."""

import sys
from core import bot_manager as _implementation

sys.modules[__name__] = _implementation
