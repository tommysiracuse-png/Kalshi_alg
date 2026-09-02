"""Standalone millisecond order-book recorder (Phase 5 / Tier-2 data path).

Records the RAW Kalshi websocket wire messages verbatim (``orderbook_delta``,
``trade`` and ``ticker`` — public channels only) for the markets the live fleet
is trading plus the most actively traded markets on the exchange, so the replay
engine can later rebuild full-depth books and queue/pull events at ms resolution.

The recorder never subscribes to private channels and never places orders.

Run from the repo root:  ``.venv\\Scripts\\python -m recorder.main``
(or ``start_recorder.ps1``).  Configuration is environment driven — see
``recorder/config.py``.
"""

RECORDER_VERSION = "1.0.0"

__all__ = ["RECORDER_VERSION"]
