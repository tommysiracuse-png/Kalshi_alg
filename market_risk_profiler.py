"""Compatibility entrypoint for :mod:`watchdogs.market_risk_profiler`."""

import sys
from watchdogs import market_risk_profiler as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
