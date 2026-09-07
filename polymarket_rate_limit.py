"""Local, provider-aware pacing for Polymarket REST and order operations."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass
class _Bucket:
    rate_per_second: float
    capacity: float
    tokens: float
    updated_at: float

    @classmethod
    def create(cls, rate: float, capacity: float) -> "_Bucket":
        now = time.monotonic()
        return cls(float(rate), float(capacity), float(capacity), now)

    def wait_seconds(self, cost: float, now: float) -> float:
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
        self.updated_at = now
        if self.tokens >= cost:
            self.tokens -= cost
            return 0.0
        if self.rate_per_second <= 0:
            return float("inf")
        delay = (cost - self.tokens) / self.rate_per_second
        self.tokens = 0.0
        self.updated_at = now + delay
        return delay


STANDARD_LIMITS: dict[str, tuple[float, float]] = {
    # Published sliding-window limits converted to conservative per-second
    # token buckets.  Capacities preserve the documented ten-second burst.
    "gamma_markets": (30.0, 300.0),
    "gamma_general": (400.0, 4_000.0),
    "data_positions": (15.0, 150.0),
    "data_trades": (20.0, 200.0),
    "clob_book": (150.0, 1_500.0),
    "clob_books": (50.0, 500.0),
    "clob_ledger": (90.0, 900.0),
    "clob_auth": (10.0, 100.0),
    "order": (40.0, 60.0),
    "cancel": (80.0, 120.0),
}

PROFILE_LIMITS: dict[str, dict[str, tuple[float, float]]] = {
    "standard": STANDARD_LIMITS,
    "copper": {**STANDARD_LIMITS, "order": (60.0, 90.0), "cancel": (120.0, 180.0)},
    "bronze": {**STANDARD_LIMITS, "order": (80.0, 120.0), "cancel": (160.0, 240.0)},
    "silver": {**STANDARD_LIMITS, "order": (200.0, 300.0), "cancel": (400.0, 600.0)},
    "gold": {**STANDARD_LIMITS, "order": (400.0, 600.0), "cancel": (800.0, 1_200.0)},
    "platinum": {**STANDARD_LIMITS, "order": (450.0, 675.0), "cancel": (900.0, 1_350.0)},
    "diamond": {**STANDARD_LIMITS, "order": (525.0, 787.0), "cancel": (1_050.0, 1_575.0)},
    "elite": {**STANDARD_LIMITS, "order": (600.0, 900.0), "cancel": (1_200.0, 1_800.0)},
}


class PolymarketRateLimiter:
    """Thread-safe pacing and response metrics for one Polymarket client."""

    def __init__(self, profile: str = "standard", overrides: Optional[Mapping[str, Any]] = None) -> None:
        normalized = str(profile or "standard").strip().lower()
        if normalized not in PROFILE_LIMITS:
            raise ValueError(f"unsupported Polymarket rate-limit profile: {profile}")
        values = dict(PROFILE_LIMITS[normalized])
        for key, value in (overrides or {}).items():
            if key not in values:
                continue
            if isinstance(value, (tuple, list)) and len(value) == 2:
                values[key] = (float(value[0]), float(value[1]))
            else:
                values[key] = (float(value), values[key][1])
        self.profile = normalized
        self._buckets = {key: _Bucket.create(*pair) for key, pair in values.items()}
        self._lock = threading.Lock()
        self._metrics: dict[str, int] = {"throttled": 0, "retries": 0, "rateLimitResponses": 0}
        self._last_headers: dict[str, str] = {}

    def acquire(self, operation: str, cost: float = 1.0) -> None:
        bucket = self._buckets.get(str(operation), self._buckets["gamma_general"])
        amount = max(0.0, float(cost))
        while True:
            with self._lock:
                delay = bucket.wait_seconds(amount, time.monotonic())
                if delay <= 0:
                    return
                self._metrics["throttled"] += 1
            time.sleep(min(delay, 1.0))

    def observe_response(
        self,
        status_code: Optional[int],
        headers: Optional[Mapping[str, Any]] = None,
        *,
        operation: Optional[str] = None,
    ) -> None:
        normalized = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
        with self._lock:
            if status_code == 429:
                self._metrics["rateLimitResponses"] += 1
            if normalized:
                self._last_headers = {
                    key: value
                    for key, value in normalized.items()
                    if key in {
                        "retry-after", "poly-ratelimit-remaining", "poly-ratelimit-reset",
                        "poly-ratelimit-tier", "x-ratelimit-remaining", "x-ratelimit-reset",
                    }
                }
                remaining_value = normalized.get("poly-ratelimit-remaining") or normalized.get("x-ratelimit-remaining")
                if operation and remaining_value is not None:
                    try:
                        bucket = self._buckets.get(str(operation))
                        if bucket is not None:
                            bucket.tokens = min(bucket.tokens, max(0.0, float(remaining_value)))
                    except (TypeError, ValueError):
                        pass

    def record_retry(self) -> None:
        with self._lock:
            self._metrics["retries"] += 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            buckets = {
                key: {
                    "ratePerSecond": round(value.rate_per_second, 3),
                    "capacity": value.capacity,
                    "tokens": round(value.tokens, 3),
                }
                for key, value in self._buckets.items()
            }
            return {
                "profile": self.profile,
                **dict(self._metrics),
                "buckets": buckets,
                "lastHeaders": dict(self._last_headers),
            }
