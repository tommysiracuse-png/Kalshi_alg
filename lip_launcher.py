"""Compatibility entrypoint for :mod:`apps.lip_launcher`."""

import sys
from apps import lip_launcher as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
