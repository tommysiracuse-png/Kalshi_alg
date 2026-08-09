"""Thread-safe, venue-neutral transport activity counters."""

from __future__ import annotations

import time
from collections import Counter, deque
from threading import Lock
from typing import Deque, Dict, Optional, TypedDict


class RestActivitySnapshot(TypedDict):
    total: int
    successes: int
    errors: int
    requestsLast60s: int
    averageLatencyMs: float
    totalLatencyMs: float
    lastActivityAtMs: Optional[int]
    byMethod: Dict[str, int]
    byOperation: Dict[str, int]
    byStatus: Dict[str, int]


class StreamActivitySnapshot(TypedDict, total=False):
    messagesLast60s: int
    lastActivityAtMs: Optional[int]
    byEventType: Dict[str, int]


class ClientActivitySnapshot(TypedDict):
    startedAtMs: int
    rest: RestActivitySnapshot
    stream: StreamActivitySnapshot


def now_ms() -> int:
    return int(time.time() * 1000)


class ActivityMonitor:
    """Collect bounded process-local metrics without retaining request data."""

    def __init__(self) -> None:
        self.started_at_ms = now_ms()
        self._lock = Lock()
        self._rest_total = self._rest_successes = self._rest_errors = 0
        self._rest_latency_ms = 0.0
        self._rest_last_at_ms: Optional[int] = None
        self._rest_recent: Deque[int] = deque()
        self._by_method: Counter[str] = Counter()
        self._by_operation: Counter[str] = Counter()
        self._by_status: Counter[str] = Counter()
        self._stream: Counter[str] = Counter()
        self._stream_events: Counter[str] = Counter()
        self._stream_recent: Deque[int] = deque()
        self._stream_last_at_ms: Optional[int] = None

    @staticmethod
    def _trim(values: Deque[int], current_ms: int) -> None:
        cutoff = current_ms - 60_000
        while values and values[0] < cutoff:
            values.popleft()

    def record_rest(
        self,
        *,
        method: str,
        operation: str,
        status_code: Optional[int],
        latency_ms: float,
        error: bool,
    ) -> None:
        timestamp = now_ms()
        with self._lock:
            self._rest_total += 1
            self._rest_errors += int(error)
            self._rest_successes += int(not error)
            self._rest_latency_ms += max(0.0, float(latency_ms))
            self._rest_last_at_ms = timestamp
            self._rest_recent.append(timestamp)
            self._trim(self._rest_recent, timestamp)
            self._by_method[method.upper()] += 1
            self._by_operation[operation or "unknown"] += 1
            self._by_status[str(status_code) if status_code is not None else "transport_error"] += 1

    def record_stream(self, event: str, *, event_type: Optional[str] = None) -> None:
        timestamp = now_ms()
        with self._lock:
            self._stream[event] += 1
            if event_type:
                self._stream_events[event_type] += 1
            self._stream_last_at_ms = timestamp
            if event in {"message", "event"}:
                self._stream_recent.append(timestamp)
                self._trim(self._stream_recent, timestamp)

    def snapshot(self) -> ClientActivitySnapshot:
        timestamp = now_ms()
        with self._lock:
            self._trim(self._rest_recent, timestamp)
            self._trim(self._stream_recent, timestamp)
            average = self._rest_latency_ms / self._rest_total if self._rest_total else 0.0
            return {
                "startedAtMs": self.started_at_ms,
                "rest": {
                    "total": self._rest_total,
                    "successes": self._rest_successes,
                    "errors": self._rest_errors,
                    "requestsLast60s": len(self._rest_recent),
                    "averageLatencyMs": round(average, 3),
                    "totalLatencyMs": round(self._rest_latency_ms, 3),
                    "lastActivityAtMs": self._rest_last_at_ms,
                    "byMethod": dict(self._by_method),
                    "byOperation": dict(self._by_operation),
                    "byStatus": dict(self._by_status),
                },
                "stream": {
                    **dict(self._stream),
                    "messagesLast60s": len(self._stream_recent),
                    "lastActivityAtMs": self._stream_last_at_ms,
                    "byEventType": dict(self._stream_events),
                },
            }
