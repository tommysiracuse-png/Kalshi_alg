"""Compatibility entrypoint for :mod:`apps.v1_orederbook_replay`."""

import sys
from apps import v1_orederbook_replay as _implementation

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
