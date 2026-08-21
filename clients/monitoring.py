"""Thread-safe, venue-neutral transport activity counters."""

from __future__ import annotations

import time
from collections import Counter, deque
from threading import Lock
from typing import Deque, Dict, Optional, TypedDict


class RestErrorSnapshot(TypedDict):
    atMs: int
    method: str
    operation: str
    statusCode: Optional[int]
    message: str


class RestOperationSnapshot(TypedDict):
    total: int
    successes: int
    errors: int
    requestsLast60s: int
    errorsLast60s: int
    averageLatencyMs: float
    totalLatencyMs: float
    lastActivityAtMs: Optional[int]
    lastError: Optional[RestErrorSnapshot]


class RestActivitySnapshot(TypedDict):
    total: int
    successes: int
    errors: int
    requestsLast60s: int
    errorsLast60s: int
    rateLimitErrors: int
    rateLimitErrorsLast60s: int
    averageLatencyMs: float
    totalLatencyMs: float
    lastActivityAtMs: Optional[int]
    lastError: Optional[RestErrorSnapshot]
    lastRateLimitError: Optional[RestErrorSnapshot]
    byMethod: Dict[str, int]
    byOperation: Dict[str, int]
    byStatus: Dict[str, int]
    operations: Dict[str, RestOperationSnapshot]


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
        self._rest_error_recent: Deque[int] = deque()
        self._rest_rate_limit_errors = 0
        self._rest_rate_limit_recent: Deque[int] = deque()
        self._rest_last_rate_limit_error: Optional[RestErrorSnapshot] = None
        self._rest_last_error: Optional[RestErrorSnapshot] = None
        self._by_method: Counter[str] = Counter()
        self._by_operation: Counter[str] = Counter()
        self._by_status: Counter[str] = Counter()
        self._operation_successes: Counter[str] = Counter()
        self._operation_errors: Counter[str] = Counter()
        self._operation_latency_ms: Counter[str] = Counter()
        self._operation_last_at_ms: Dict[str, int] = {}
        self._operation_recent: Dict[str, Deque[int]] = {}
        self._operation_error_recent: Dict[str, Deque[int]] = {}
        self._operation_last_error: Dict[str, RestErrorSnapshot] = {}
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
        error_message: Optional[str] = None,
    ) -> None:
        timestamp = now_ms()
        operation_name = operation or "unknown"
        latency = max(0.0, float(latency_ms))
        with self._lock:
            self._rest_total += 1
            self._rest_errors += int(error)
            self._rest_successes += int(not error)
            self._rest_latency_ms += latency
            self._rest_last_at_ms = timestamp
            self._rest_recent.append(timestamp)
            self._trim(self._rest_recent, timestamp)
            self._by_method[method.upper()] += 1
            self._by_operation[operation_name] += 1
            self._by_status[str(status_code) if status_code is not None else "transport_error"] += 1
            self._operation_successes[operation_name] += int(not error)
            self._operation_errors[operation_name] += int(error)
            self._operation_latency_ms[operation_name] += latency
            self._operation_last_at_ms[operation_name] = timestamp
            operation_recent = self._operation_recent.setdefault(operation_name, deque())
            operation_recent.append(timestamp)
            self._trim(operation_recent, timestamp)
            if error:
                error_snapshot: RestErrorSnapshot = {
                    "atMs": timestamp,
                    "method": method.upper(),
                    "operation": operation_name,
                    "statusCode": status_code,
                    "message": str(error_message or "request failed")[:500],
                }
                self._rest_error_recent.append(timestamp)
                self._trim(self._rest_error_recent, timestamp)
                self._rest_last_error = error_snapshot
                operation_errors = self._operation_error_recent.setdefault(operation_name, deque())
                operation_errors.append(timestamp)
                self._trim(operation_errors, timestamp)
                self._operation_last_error[operation_name] = error_snapshot
                if status_code == 429:
                    self._rest_rate_limit_errors += 1
                    self._rest_rate_limit_recent.append(timestamp)
                    self._trim(self._rest_rate_limit_recent, timestamp)
                    self._rest_last_rate_limit_error = error_snapshot

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
            self._trim(self._rest_error_recent, timestamp)
            self._trim(self._rest_rate_limit_recent, timestamp)
            self._trim(self._stream_recent, timestamp)
            average = self._rest_latency_ms / self._rest_total if self._rest_total else 0.0
            operations: Dict[str, RestOperationSnapshot] = {}
            for operation, total in self._by_operation.items():
                recent = self._operation_recent.setdefault(operation, deque())
                recent_errors = self._operation_error_recent.setdefault(operation, deque())
                self._trim(recent, timestamp)
                self._trim(recent_errors, timestamp)
                latency = float(self._operation_latency_ms[operation])
                operations[operation] = {
                    "total": total,
                    "successes": self._operation_successes[operation],
                    "errors": self._operation_errors[operation],
                    "requestsLast60s": len(recent),
                    "errorsLast60s": len(recent_errors),
                    "averageLatencyMs": round(latency / total if total else 0.0, 3),
                    "totalLatencyMs": round(latency, 3),
                    "lastActivityAtMs": self._operation_last_at_ms.get(operation),
                    "lastError": self._operation_last_error.get(operation),
                }
            return {
                "startedAtMs": self.started_at_ms,
                "rest": {
                    "total": self._rest_total,
                    "successes": self._rest_successes,
                    "errors": self._rest_errors,
                    "requestsLast60s": len(self._rest_recent),
                    "errorsLast60s": len(self._rest_error_recent),
                    "rateLimitErrors": self._rest_rate_limit_errors,
                    "rateLimitErrorsLast60s": len(self._rest_rate_limit_recent),
                    "averageLatencyMs": round(average, 3),
                    "totalLatencyMs": round(self._rest_latency_ms, 3),
                    "lastActivityAtMs": self._rest_last_at_ms,
                    "lastError": self._rest_last_error,
                    "lastRateLimitError": self._rest_last_rate_limit_error,
                    "byMethod": dict(self._by_method),
                    "byOperation": dict(self._by_operation),
                    "byStatus": dict(self._by_status),
                    "operations": operations,
                },
                "stream": {
                    **dict(self._stream),
                    "messagesLast60s": len(self._stream_recent),
                    "lastActivityAtMs": self._stream_last_at_ms,
                    "byEventType": dict(self._stream_events),
                },
            }
