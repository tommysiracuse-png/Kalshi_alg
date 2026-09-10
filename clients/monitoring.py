"""Thread-safe, venue-neutral transport activity counters."""

from __future__ import annotations

import time
from collections import Counter, deque
from threading import Lock
from typing import Any, Deque, Dict, Iterable, Mapping, Optional, TypedDict


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
    disconnects: int
    lastDisconnectAtMs: Optional[int]
    lastError: Optional[RestErrorSnapshot]
    lastRateLimitError: Optional[RestErrorSnapshot]
    byMethod: Dict[str, int]
    byOperation: Dict[str, int]
    byStatus: Dict[str, int]
    operations: Dict[str, RestOperationSnapshot]


class StreamActivitySnapshot(TypedDict, total=False):
    connections: int
    disconnects: int
    connectionErrors: int
    messages: int
    messageSuccesses: int
    messageFailures: int
    totalMessageLatencyMs: float
    averageMessageLatencyMs: float
    lastMessageAtMs: Optional[int]
    lastDisconnectAtMs: Optional[int]
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
        self._rest_disconnects = 0
        self._rest_last_disconnect_at_ms: Optional[int] = None
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
        self._stream_message_successes = 0
        self._stream_message_failures = 0
        self._stream_message_latency_ms = 0.0
        self._stream_last_message_at_ms: Optional[int] = None
        self._stream_last_disconnect_at_ms: Optional[int] = None

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
                if status_code is None:
                    self._rest_disconnects += 1
                    self._rest_last_disconnect_at_ms = timestamp
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
            if event in {"closes", "disconnects"}:
                self._stream["disconnects"] += int(event != "disconnects")
                self._stream_last_disconnect_at_ms = timestamp
            if event in {"message", "event"}:
                self._stream_recent.append(timestamp)
                self._trim(self._stream_recent, timestamp)
                if event == "message":
                    self._stream_last_message_at_ms = timestamp

    def record_stream_message(self, *, latency_ms: float, success: bool, event_type: Optional[str] = None) -> None:
        """Record adapter processing of one received WebSocket message.

        The raw transport records receipt; adapters call this method after
        parsing/dispatch so message failures and processing latency remain
        distinct from provider event age.
        """
        timestamp = now_ms()
        with self._lock:
            self._stream_message_successes += int(bool(success))
            self._stream_message_failures += int(not success)
            self._stream_message_latency_ms += max(0.0, float(latency_ms))
            self._stream_last_message_at_ms = timestamp
            if event_type:
                self._stream_events[event_type] += 1

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
                    "disconnects": self._rest_disconnects,
                    "lastDisconnectAtMs": self._rest_last_disconnect_at_ms,
                    "lastError": self._rest_last_error,
                    "lastRateLimitError": self._rest_last_rate_limit_error,
                    "byMethod": dict(self._by_method),
                    "byOperation": dict(self._by_operation),
                    "byStatus": dict(self._by_status),
                    "operations": operations,
                },
                "stream": {
                    **dict(self._stream),
                    "messages": int(self._stream.get("message", 0)),
                    "messageSuccesses": self._stream_message_successes,
                    "messageFailures": self._stream_message_failures,
                    "totalMessageLatencyMs": round(self._stream_message_latency_ms, 3),
                    "averageMessageLatencyMs": round(
                        self._stream_message_latency_ms
                        / (self._stream_message_successes + self._stream_message_failures)
                        if (self._stream_message_successes + self._stream_message_failures) else 0.0,
                        3,
                    ),
                    "lastMessageAtMs": self._stream_last_message_at_ms,
                    "lastDisconnectAtMs": self._stream_last_disconnect_at_ms,
                    "messagesLast60s": len(self._stream_recent),
                    "lastActivityAtMs": self._stream_last_at_ms,
                    "byEventType": dict(self._stream_events),
                },
            }


def merge_activity_snapshots(snapshots: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Combine activity from several HTTP/WebSocket transports.

    Polymarket has separate Gamma, Data API, CLOB, and authenticated SDK
    transports.  The launcher needs one client-level snapshot, while the API
    dashboard can still inspect the individual component snapshots.  This
    helper deliberately treats missing fields as zero so it can also merge
    snapshots produced by older workers.
    """

    values = [item for item in snapshots if isinstance(item, Mapping)]
    rest: dict[str, Any] = {
        "total": 0, "successes": 0, "errors": 0,
        "requestsLast60s": 0, "errorsLast60s": 0,
        "rateLimitErrors": 0, "rateLimitErrorsLast60s": 0,
        "averageLatencyMs": 0.0, "totalLatencyMs": 0.0,
        "lastActivityAtMs": None, "disconnects": 0, "lastDisconnectAtMs": None,
        "lastError": None,
        "lastRateLimitError": None, "byMethod": {}, "byOperation": {},
        "byStatus": {}, "operations": {},
    }
    stream: dict[str, Any] = {
        "messagesLast60s": 0, "messages": 0, "messageSuccesses": 0,
        "messageFailures": 0, "totalMessageLatencyMs": 0.0,
        "averageMessageLatencyMs": 0.0, "connections": 0, "disconnects": 0,
        "lastMessageAtMs": None, "lastDisconnectAtMs": None,
        "lastActivityAtMs": None, "byEventType": {},
    }

    def add_counter(target: dict[str, int], source: Any) -> None:
        if not isinstance(source, Mapping):
            return
        for key, value in source.items():
            try:
                target[str(key)] = target.get(str(key), 0) + int(value or 0)
            except (TypeError, ValueError):
                continue

    def newer(current: Any, candidate: Any) -> Any:
        if not isinstance(candidate, Mapping):
            return current
        if not isinstance(current, Mapping) or int(candidate.get("atMs") or 0) >= int(current.get("atMs") or 0):
            return dict(candidate)
        return current

    for snapshot in values:
        snapshot_rest = snapshot.get("rest")
        if isinstance(snapshot_rest, Mapping):
            for key in (
                "total", "successes", "errors", "requestsLast60s", "errorsLast60s",
                "rateLimitErrors", "rateLimitErrorsLast60s", "totalLatencyMs",
                "disconnects",
            ):
                value = snapshot_rest.get(key)
                if value is not None:
                    rest[key] = rest.get(key, 0) + (float(value) if key == "totalLatencyMs" else int(value or 0))
            last_at = snapshot_rest.get("lastActivityAtMs")
            if last_at is not None and (rest["lastActivityAtMs"] is None or int(last_at) > int(rest["lastActivityAtMs"])):
                rest["lastActivityAtMs"] = int(last_at)
            disconnect_at = snapshot_rest.get("lastDisconnectAtMs")
            if disconnect_at is not None and (
                rest["lastDisconnectAtMs"] is None or int(disconnect_at) > int(rest["lastDisconnectAtMs"])
            ):
                rest["lastDisconnectAtMs"] = int(disconnect_at)
            rest["lastError"] = newer(rest["lastError"], snapshot_rest.get("lastError"))
            rest["lastRateLimitError"] = newer(rest["lastRateLimitError"], snapshot_rest.get("lastRateLimitError"))
            add_counter(rest["byMethod"], snapshot_rest.get("byMethod"))
            add_counter(rest["byOperation"], snapshot_rest.get("byOperation"))
            add_counter(rest["byStatus"], snapshot_rest.get("byStatus"))
            for operation, metrics in (snapshot_rest.get("operations") or {}).items():
                if not isinstance(metrics, Mapping):
                    continue
                current = rest["operations"].setdefault(str(operation), {
                    "total": 0, "successes": 0, "errors": 0,
                    "requestsLast60s": 0, "errorsLast60s": 0,
                    "totalLatencyMs": 0.0, "lastActivityAtMs": None,
                    "lastError": None,
                })
                for key in ("total", "successes", "errors", "requestsLast60s", "errorsLast60s"):
                    current[key] += int(metrics.get(key) or 0)
                current["totalLatencyMs"] += float(metrics.get("totalLatencyMs") or 0.0)
                metric_at = metrics.get("lastActivityAtMs")
                if metric_at is not None and (current["lastActivityAtMs"] is None or int(metric_at) > int(current["lastActivityAtMs"])):
                    current["lastActivityAtMs"] = int(metric_at)
                current["lastError"] = newer(current["lastError"], metrics.get("lastError"))
        snapshot_stream = snapshot.get("stream")
        if isinstance(snapshot_stream, Mapping):
            stream["messagesLast60s"] += int(snapshot_stream.get("messagesLast60s") or 0)
            for key in ("messages", "messageSuccesses", "messageFailures", "connections", "disconnects"):
                stream[key] += int(snapshot_stream.get(key) or 0)
            stream["totalMessageLatencyMs"] += float(snapshot_stream.get("totalMessageLatencyMs") or 0.0)
            stream["lastActivityAtMs"] = max(
                [value for value in (stream["lastActivityAtMs"], snapshot_stream.get("lastActivityAtMs")) if value is not None],
                default=None,
            )
            for timestamp_key in ("lastMessageAtMs", "lastDisconnectAtMs"):
                candidate = snapshot_stream.get(timestamp_key)
                if candidate is not None and (
                    stream[timestamp_key] is None or int(candidate) > int(stream[timestamp_key])
                ):
                    stream[timestamp_key] = int(candidate)
            add_counter(stream["byEventType"], snapshot_stream.get("byEventType"))
            for key, value in snapshot_stream.items():
                if key not in {
                    "messagesLast60s", "lastActivityAtMs", "byEventType",
                    "messages", "messageSuccesses", "messageFailures",
                    "totalMessageLatencyMs", "averageMessageLatencyMs",
                    "connections", "disconnects", "lastMessageAtMs", "lastDisconnectAtMs",
                }:
                    try:
                        stream[key] = stream.get(key, 0) + int(value or 0)
                    except (TypeError, ValueError):
                        pass

    rest["averageLatencyMs"] = round(rest["totalLatencyMs"] / rest["total"] if rest["total"] else 0.0, 3)
    stream["averageMessageLatencyMs"] = round(
        stream["totalMessageLatencyMs"] / stream["messages"] if stream["messages"] else 0.0,
        3,
    )
    for metrics in rest["operations"].values():
        metrics["averageLatencyMs"] = round(metrics["totalLatencyMs"] / metrics["total"] if metrics["total"] else 0.0, 3)
    starts = [int(item.get("startedAtMs")) for item in values if item.get("startedAtMs")]
    return {
        "startedAtMs": min(starts) if starts else 0,
        "rest": rest,
        "stream": stream,
    }


class SessionActivityAccumulator:
    """Retain process-generation counters while keeping rolling rates live."""

    def __init__(self) -> None:
        self._snapshots: dict[str, dict[str, Mapping[str, Any]]] = {}

    def observe(
        self,
        venue: str,
        sources: Iterable[tuple[str, Mapping[str, Any]]],
    ) -> dict[str, Any]:
        venue_key = str(venue).lower()
        stored = self._snapshots.setdefault(venue_key, {})
        active: list[Mapping[str, Any]] = []
        for source_id, snapshot in sources:
            if not isinstance(snapshot, Mapping):
                continue
            stored[str(source_id)] = dict(snapshot)
            active.append(snapshot)
        cumulative = merge_activity_snapshots(stored.values())
        live = merge_activity_snapshots(active)
        cumulative_rest = cumulative.get("rest") or {}
        live_rest = live.get("rest") or {}
        for key in ("requestsLast60s", "errorsLast60s", "rateLimitErrorsLast60s"):
            cumulative_rest[key] = int(live_rest.get(key) or 0)
        cumulative_stream = cumulative.get("stream") or {}
        live_stream = live.get("stream") or {}
        cumulative_stream["messagesLast60s"] = int(live_stream.get("messagesLast60s") or 0)
        return cumulative
