"""Compatibility entrypoint for :mod:`apps.V1`."""

import sys
from apps import V1 as _implementation

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
