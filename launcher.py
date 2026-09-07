"""Compatibility import for :mod:`apps.launcher`."""

import sys
from apps import launcher as _implementation

sys.modules[__name__] = _implementation
