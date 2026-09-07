"""Compatibility import for :mod:`bots.top_of_book_bot`."""

import sys
from bots import top_of_book_bot as _implementation

sys.modules[__name__] = _implementation
