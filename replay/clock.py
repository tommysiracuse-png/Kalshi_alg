"""Monotonic replay clock used to monkeypatch ``top_of_book_bot.now_ms``."""

from __future__ import annotations


class ReplayClock:
    """Deterministic wall clock driven by the historical event stream."""

    def __init__(self, start_ms: int) -> None:
        self._now_ms = int(start_ms)

    def now_ms(self) -> int:
        return self._now_ms

    def now_seconds(self) -> int:
        return self._now_ms // 1000

    def advance_to(self, ts_ms: int) -> None:
        """Advance the clock (never backwards) to ``ts_ms``."""
        ts_ms = int(ts_ms)
        if ts_ms > self._now_ms:
            self._now_ms = ts_ms
