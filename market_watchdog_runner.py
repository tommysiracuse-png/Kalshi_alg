"""Compatibility entrypoint for :mod:`watchdogs.market_watchdog_runner`."""

import sys
from watchdogs import market_watchdog_runner as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
