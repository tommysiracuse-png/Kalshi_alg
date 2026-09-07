"""Compatibility entrypoint for :mod:`apps.backfill_markouts`."""

import sys
from apps import backfill_markouts as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
