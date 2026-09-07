"""Compatibility entrypoint for :mod:`apps.fleet_post_stop`."""

import sys
from apps import fleet_post_stop as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
