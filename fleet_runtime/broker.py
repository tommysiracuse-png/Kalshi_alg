"""Central intent scheduling and token accounting primitives."""

from __future__ import annotations

import heapq
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from fleet_models import IntentUrgency, QuoteIntent


class TokenBucket:
    def __init__(self, refill_rate: float, capacity: float, *, clock: Callable[[], float] = time.monotonic) -> None:
        if refill_rate < 0 or capacity < 0:
            raise ValueError("token rates must be non-negative")
        self.refill_rate = float(refill_rate)
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self._clock = clock
        self._updated_at = clock()
        self._lock = threading.Lock()

    def reconfigure(self, refill_rate: float, capacity: float) -> None:
        if refill_rate < 0 or capacity < 0:
            raise ValueError("token rates must be non-negative")
        with self._lock:
            self._refill()
            self.refill_rate = float(refill_rate)
            self.capacity = float(capacity)
            self._tokens = min(self._tokens, self.capacity)

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated_at)
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._updated_at = now

    def consume(self, cost: float) -> bool:
        if cost < 0:
            raise ValueError("token cost must be non-negative")
        with self._lock:
            self._refill()
            if self._tokens < cost:
                return False
            self._tokens -= cost
            return True

    def snapshot(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


@dataclass(order=True)
class _QueuedIntent:
    urgency: int
    sequence: int
    key: tuple[str, str] = field(compare=False)
    intent: QuoteIntent = field(compare=False)


class IntentQueue:
    """Priority queue that coalesces normal desired state by ticker and side."""

    def __init__(self) -> None:
        self._heap: list[_QueuedIntent] = []
        self._latest: dict[tuple[str, str], QuoteIntent] = {}
        self._sequence = 0
        self._lock = threading.Lock()

    def submit(self, intent: QuoteIntent) -> None:
        key = (intent.ticker, intent.side)
        with self._lock:
            current = self._latest.get(key)
            if current and intent.strategy_generation < current.strategy_generation:
                return
            self._latest[key] = intent
            self._sequence += 1
            heapq.heappush(self._heap, _QueuedIntent(int(intent.urgency), self._sequence, key, intent))

    def pop(self, *, now_ms: Optional[int] = None, reduction_only: bool = False) -> Optional[QuoteIntent]:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        with self._lock:
            while self._heap:
                queued = heapq.heappop(self._heap)
                latest = self._latest.get(queued.key)
                if latest is not queued.intent:
                    continue
                del self._latest[queued.key]
                if latest.expires_at_ms <= now_ms:
                    continue
                if reduction_only and latest.exposure_increasing and not latest.is_cancel:
                    continue
                return latest
        return None

    def __len__(self) -> int:
        with self._lock:
            return len(self._latest)
