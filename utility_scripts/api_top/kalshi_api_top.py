#!/usr/bin/env python3
"""A top-like terminal dashboard for Kalshi read/write token-bucket usage."""

from __future__ import annotations

import argparse
import json
import os
import select
import shutil
import sys
import termios
import time
import tty
import urllib.error
import urllib.request
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Iterable


CATEGORY_NAMES = {
    "create_order": "create order",
    "cancel_order": "delete order",
    "amend_order": "amend order",
    "decrease_order": "decrease order",
    "get_market": "pull market",
    "get_market_quote": "pull market",
    "list_markets": "pull market",
    "get_positions": "pull positions",
    "list_account_positions": "pull positions",
    "list_account_orders": "pull orders",
    "get_order_queue_position": "pull queue position",
    "list_account_fills": "pull fills",
    "get_account_balance": "pull account balance",
    "get_account_limits": "pull account limits",
    "get_series": "pull series",
    "get_series_fee_changes": "pull series fees",
    "get_incentive_programs": "pull incentives",
}

ENDPOINT_COSTS_URL = "https://external-api.kalshi.com/trade-api/v2/account/endpoint_costs"

# Logical operation names are intentionally mapped to endpoint templates rather
# than retaining request URLs, which may contain market or order identifiers.
OPERATION_ENDPOINTS = {
    "create_order": ("POST", "/trade-api/v2/portfolio/events/orders"),
    "cancel_order": ("DELETE", "/trade-api/v2/portfolio/events/orders/:order_id"),
    "amend_order": ("POST", "/trade-api/v2/portfolio/events/orders/:order_id/amend"),
    "decrease_order": ("POST", "/trade-api/v2/portfolio/events/orders/:order_id/decrease"),
    "get_market": ("GET", "/trade-api/v2/markets/:ticker"),
    "get_market_quote": ("GET", "/trade-api/v2/markets/:ticker"),
    "list_markets": ("GET", "/trade-api/v2/markets"),
    "get_positions": ("GET", "/trade-api/v2/portfolio/positions"),
    "list_account_positions": ("GET", "/trade-api/v2/portfolio/positions"),
    "list_account_orders": ("GET", "/trade-api/v2/portfolio/orders"),
    "get_order_queue_position": ("GET", "/trade-api/v2/portfolio/orders/:order_id/queue_position"),
    "list_account_fills": ("GET", "/trade-api/v2/portfolio/fills"),
    "get_account_balance": ("GET", "/trade-api/v2/portfolio/balance"),
    "get_account_limits": ("GET", "/trade-api/v2/account/limits"),
    "get_series": ("GET", "/trade-api/v2/series/:series_ticker"),
    "get_series_fee_changes": ("GET", "/trade-api/v2/series/fee_changes"),
    "get_incentive_programs": ("GET", "/trade-api/v2/incentive_programs"),
}

# Current documented fallback. The live public endpoint replaces this catalog
# at startup when reachable.
FALLBACK_NON_DEFAULT_COSTS = {
    ("DELETE", "/trade-api/v2/portfolio/events/orders/:order_id"): 2,
}


def category_name(operation: str) -> str:
    return CATEGORY_NAMES.get(operation, operation.replace("_", " "))


def api_sources(status: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    sources: list[tuple[str, dict[str, Any]]] = []
    for index, client in enumerate(status.get("clients") or []):
        rest = ((client.get("apiActivity") or {}).get("rest") or {})
        if rest:
            identity = f"bot:{client.get('marketId', index)}:{client.get('pid', '')}"
            sources.append((identity, rest))
    for component in ("screener", "portfolio"):
        activity = (status.get(component) or {}).get("apiActivity") or {}
        rest = activity.get("rest") or {}
        if rest:
            identity = f"{component}:{activity.get('startedAtMs', '')}"
            sources.append((identity, rest))
    if not sources:
        rest = (((status.get("manager") or {}).get("apiActivity") or {}).get("rest") or {})
        if rest:
            sources.append(("manager", rest))
    return sources


def aggregate_operations(sources: Iterable[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    categories: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "successes": 0,
            "errors": 0,
            "total": 0,
            "totalLatencyMs": 0.0,
            "lastActivityAtMs": None,
            "complete": True,
        }
    )
    for _, rest in sources:
        detailed = rest.get("operations") or {}
        operation_names = set(rest.get("byOperation") or {}) | set(detailed)
        for operation in operation_names:
            category = categories[category_name(operation)]
            metrics = detailed.get(operation)
            if not metrics:
                category["complete"] = False
                category["total"] += int((rest.get("byOperation") or {}).get(operation) or 0)
                continue
            category["successes"] += int(metrics.get("successes") or 0)
            category["errors"] += int(metrics.get("errors") or 0)
            category["total"] += int(metrics.get("total") or 0)
            category["totalLatencyMs"] += float(metrics.get("totalLatencyMs") or 0.0)
            last_at = metrics.get("lastActivityAtMs")
            if last_at is not None and (
                category["lastActivityAtMs"] is None or last_at > category["lastActivityAtMs"]
            ):
                category["lastActivityAtMs"] = last_at
    rows = []
    for name, metrics in categories.items():
        metrics["name"] = name
        metrics["averageLatencyMs"] = (
            metrics["totalLatencyMs"] / metrics["total"] if metrics["total"] else 0.0
        )
        rows.append(metrics)
    return sorted(rows, key=lambda row: (-row["total"], row["name"]))


def newest_error(status: dict[str, Any], sources: Iterable[tuple[str, dict[str, Any]]]) -> dict[str, Any] | None:
    errors = [rest.get("lastError") for _, rest in sources if rest.get("lastError")]
    for client in status.get("clients") or []:
        for event in (client.get("orderActivity") or {}).get("recent") or []:
            if event.get("outcome") == "error" and event.get("error"):
                errors.append(
                    {
                        "atMs": event.get("timestampMs"),
                        "operation": event.get("action") or "order",
                        "statusCode": None,
                        "message": event.get("error"),
                    }
                )
    return max(errors, key=lambda item: int(item.get("atMs") or 0), default=None)


class EndpointCosts:
    """Resolve logical requests against Kalshi's public endpoint-cost catalog."""

    def __init__(self) -> None:
        self.default_cost = 10
        self.costs = dict(FALLBACK_NON_DEFAULT_COSTS)
        self.source = "documented fallback"
        self.error: str | None = None

    def refresh(self, url: str = ENDPOINT_COSTS_URL) -> None:
        request = urllib.request.Request(url, headers={"User-Agent": "kalshi-api-top/1"})
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                payload = json.load(response)
            default_cost = int(payload["default_cost"])
            costs = {
                (str(item["method"]).upper(), str(item["path"])): int(item["cost"])
                for item in payload.get("endpoint_costs") or []
            }
            if default_cost <= 0 or any(value <= 0 for value in costs.values()):
                raise ValueError("endpoint-cost response contained a non-positive cost")
            self.default_cost = default_cost
            self.costs = costs
            self.source = "live Kalshi catalog"
            self.error = None
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            self.error = str(exc)

    def operation(self, name: str) -> tuple[str, int]:
        endpoint = OPERATION_ENDPOINTS.get(name)
        if endpoint is None:
            method = "GET" if name.startswith(("get", "list")) else "POST"
            return ("read" if method == "GET" else "write", self.default_cost)
        method, path = endpoint
        bucket = "read" if method == "GET" else "write"
        return bucket, self.costs.get((method, path), self.default_cost)


def rate_limits(status: dict[str, Any]) -> tuple[str, dict[str, dict[str, float]]]:
    summary = (status.get("portfolio") or {}).get("summary") or {}
    limits = {}
    for name in ("read", "write"):
        value = summary.get(f"{name}RateLimit") or {}
        limits[name] = {
            "refill": float(value.get("refillRate") or 0),
            "capacity": float(value.get("bucketCapacity") or 0),
        }
    return str(summary.get("apiTier") or "unknown"), limits


class BucketUsageTracker:
    """Track local token spend and estimate bucket balances between snapshots."""

    def __init__(self) -> None:
        self.started_at = time.monotonic()
        self.previous_operations: dict[str, dict[str, int]] = {}
        self.previous_429s: dict[str, int] = {}
        self.events: deque[tuple[float, int, int, int]] = deque()
        self.balance: dict[str, float | None] = {"read": None, "write": None}
        self.capacity: dict[str, float] = {"read": 0, "write": 0}
        self.last_update: float | None = None

    def update(
        self,
        now: float,
        sources: Iterable[tuple[str, dict[str, Any]]],
        costs: EndpointCosts,
        limits: dict[str, dict[str, float]],
    ) -> dict[str, Any]:
        sources = list(sources)
        elapsed = 0.0 if self.last_update is None else max(0.0, now - self.last_update)
        for bucket in ("read", "write"):
            capacity = limits[bucket]["capacity"]
            refill = limits[bucket]["refill"]
            if capacity <= 0:
                self.balance[bucket] = None
            elif self.balance[bucket] is None or capacity != self.capacity[bucket]:
                self.balance[bucket] = capacity
            else:
                self.balance[bucket] = min(capacity, float(self.balance[bucket]) + elapsed * refill)
            self.capacity[bucket] = capacity

        current_operations: dict[str, dict[str, int]] = {}
        current_429s: dict[str, int] = {}
        spent = {"read": 0, "write": 0}
        rate_limit_delta = 0
        for identity, rest in sources:
            operations = {str(key): int(value or 0) for key, value in (rest.get("byOperation") or {}).items()}
            current_operations[identity] = operations
            prior = self.previous_operations.get(identity)
            if prior is not None:
                for operation, total in operations.items():
                    delta = max(0, total - prior.get(operation, 0))
                    bucket, cost = costs.operation(operation)
                    spent[bucket] += delta * cost
            rate_limit_total = int((rest.get("byStatus") or {}).get("429") or 0)
            current_429s[identity] = rate_limit_total
            if identity in self.previous_429s:
                rate_limit_delta += max(0, rate_limit_total - self.previous_429s[identity])

        self.previous_operations = current_operations
        self.previous_429s = current_429s
        self.last_update = now
        for bucket in ("read", "write"):
            if self.balance[bucket] is not None:
                self.balance[bucket] = max(0.0, float(self.balance[bucket]) - spent[bucket])
        if spent["read"] or spent["write"] or rate_limit_delta:
            self.events.append((now, spent["read"], spent["write"], rate_limit_delta))
        while self.events and self.events[0][0] < now - 60:
            self.events.popleft()

        detailed = bool(sources) and all(
            set(rest.get("byOperation") or {}).issubset(set(rest.get("operations") or {}))
            for _, rest in sources
        )
        if detailed:
            window_spend = {"read": 0, "write": 0}
            for _, rest in sources:
                for operation, metrics in (rest.get("operations") or {}).items():
                    bucket, cost = costs.operation(operation)
                    window_spend[bucket] += int(metrics.get("requestsLast60s") or 0) * cost
        else:
            window_spend = {
                "read": sum(event[1] for event in self.events),
                "write": sum(event[2] for event in self.events),
            }
        rate_limit_detailed = bool(sources) and all(
            "rateLimitErrorsLast60s" in rest for _, rest in sources
        )
        if rate_limit_detailed:
            rate_limits_last_60 = sum(int(rest.get("rateLimitErrorsLast60s") or 0) for _, rest in sources)
        else:
            rate_limits_last_60 = sum(event[3] for event in self.events)
        return {
            "tokensLast60s": window_spend,
            "rateLimitErrorsLast60s": rate_limits_last_60,
            "balance": dict(self.balance),
            "fullWindow": detailed or now - self.started_at >= 60,
            "detailed": detailed,
            "rateLimitDetailed": rate_limit_detailed,
        }


def duration(seconds: float | None) -> str:
    if seconds is None:
        return "never"
    seconds = max(0.0, seconds)
    if seconds < 1:
        return f"{seconds:.1f}s"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, second = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {second:02d}s"
    hours, minute = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minute:02d}m"
    days, hour = divmod(hours, 24)
    return f"{days}d {hour:02d}h"


def age(timestamp_ms: Any, now_ms: int) -> str:
    if timestamp_ms is None:
        return "never"
    return duration((now_ms - int(timestamp_ms)) / 1000)


def shortened(value: str, width: int) -> str:
    value = str(value).replace("\r", " ").replace("\n", " ")
    if len(value) <= width:
        return value
    return value[: max(0, width - 1)] + "…"


def render(
    status: dict[str, Any],
    *,
    monitor_seconds: float,
    usage: dict[str, Any],
    costs: EndpointCosts,
    width: int,
    height: int,
) -> str:
    now_ms = int(time.time() * 1000)
    sources = api_sources(status)
    rest_values = [rest for _, rest in sources]
    last_call = max((int(rest.get("lastActivityAtMs") or 0) for rest in rest_values), default=0)
    rate_limit_total = sum(int((rest.get("byStatus") or {}).get("429") or 0) for rest in rest_values)
    snapshot_at = status.get("generatedAt")
    tier, limits = rate_limits(status)
    window_suffix = "" if usage["detailed"] else " (observed since start)"
    rate_limit_suffix = "" if usage["rateLimitDetailed"] else " (observed since start)"

    lines = [
        "KALSHI API RATE-LIMIT TOP  |  q or Ctrl-C to quit",
        (
            f"Monitoring: {duration(monitor_seconds):<10}  "
            f"Snapshot age: {age(snapshot_at, now_ms):<10}  Last API call: {age(last_call or None, now_ms)}"
        ),
        (
            f"Tier: {tier.upper()}  Sources: {len(sources)}  Default cost: {costs.default_cost} tokens  "
            f"Costs: {costs.source}"
        ),
    ]

    for bucket in ("read", "write"):
        refill = limits[bucket]["refill"]
        capacity = limits[bucket]["capacity"]
        tokens = int(usage["tokensLast60s"][bucket])
        spend_rate = tokens / 60
        utilization = spend_rate / refill * 100 if refill else 0.0
        headroom = refill - spend_rate
        balance = usage["balance"][bucket]
        available = "unknown" if balance is None else f"~{balance:,.0f}/{capacity:,.0f}"
        lines.append(
            f"{bucket.upper():5} available {available:<16} refill {refill:,.0f} tok/s  "
            f"spend {spend_rate:,.1f} tok/s ({utilization:,.1f}%)  "
            f"headroom {headroom:+,.1f} tok/s"
        )
        lines.append(f"      token spend/60s: {tokens:,}{window_suffix}")

    lines.append(
        f"HTTP 429 rate limits: {usage['rateLimitErrorsLast60s']:,} in last 60s{rate_limit_suffix}  "
        f"|  {rate_limit_total:,} total"
    )

    latest = newest_error(status, sources)
    if latest:
        status_code = f" HTTP {latest.get('statusCode')}" if latest.get("statusCode") else ""
        prefix = f"Last error ({age(latest.get('atMs'), now_ms)} ago, {latest.get('operation', 'unknown')}{status_code}): "
        lines.append(prefix + shortened(str(latest.get("message") or "request failed"), max(20, width - len(prefix))))
    else:
        lines.append("Last error: none recorded")
    lines.extend(("", "AGGREGATED REST CALLS"))

    category_width = max(18, min(36, width - 51))
    header = (
        f"{'CATEGORY':<{category_width}} {'LAST CALL':>11} "
        f"{'SUCCEEDED':>11} {'ERRORS':>9} {'AVG RTT':>11}"
    )
    lines.extend((header, "-" * min(width, len(header))))
    available_rows = max(1, height - len(lines) - 1)
    rows = aggregate_operations(sources)
    for row in rows[:available_rows]:
        complete = row["complete"]
        succeeded = f"{row['successes']:,}" if complete else "—"
        row_errors = f"{row['errors']:,}" if complete else "—"
        rtt = f"{row['averageLatencyMs']:.2f} ms" if complete else "—"
        lines.append(
            f"{shortened(row['name'], category_width):<{category_width}} "
            f"{age(row['lastActivityAtMs'], now_ms):>11} "
            f"{succeeded:>11} {row_errors:>9} {rtt:>11}"
        )
    if len(rows) > available_rows:
        lines.append(f"… {len(rows) - available_rows} more categories (enlarge the terminal to display them)")
    if any(not row["complete"] for row in rows):
        lines.append("— detailed per-category metrics become available after the launcher is restarted")
    return "\n".join(shortened(line, width) for line in lines)


class Keyboard:
    def __init__(self) -> None:
        self.fd: int | None = None
        self.settings: list[Any] | None = None

    def __enter__(self) -> "Keyboard":
        if sys.stdin.isatty():
            self.fd = sys.stdin.fileno()
            self.settings = termios.tcgetattr(self.fd)
            tty.setcbreak(self.fd)
        return self

    def quit_requested(self) -> bool:
        if self.fd is None:
            return False
        ready, _, _ = select.select([self.fd], [], [], 0)
        return bool(ready and os.read(self.fd, 1).lower() == b"q")

    def __exit__(self, *_: Any) -> None:
        if self.fd is not None and self.settings is not None:
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.settings)


def load_status(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--status-file",
        type=Path,
        default=Path(os.environ.get("KALSHI_LAUNCHER_STATUS", root / "runtime/launcher_status.json")),
    )
    parser.add_argument("--interval", type=float, default=1.0, help="refresh interval in seconds")
    parser.add_argument("--once", action="store_true", help="print one snapshot and exit")
    parser.add_argument("--offline-costs", action="store_true", help="use bundled documented endpoint costs")
    parser.add_argument("--endpoint-costs-url", default=ENDPOINT_COSTS_URL, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")

    started = time.monotonic()
    costs = EndpointCosts()
    if not args.offline_costs:
        costs.refresh(args.endpoint_costs_url)
    tracker = BucketUsageTracker()
    if args.once:
        status = load_status(args.status_file)
        sources = api_sources(status)
        _, limits = rate_limits(status)
        usage = tracker.update(time.monotonic(), sources, costs, limits)
        size = shutil.get_terminal_size((120, 40))
        print(render(status, monitor_seconds=0, usage=usage, costs=costs, width=size.columns, height=size.lines))
        return 0

    with Keyboard() as keyboard:
        sys.stdout.write("\033[?1049h\033[?25l")
        try:
            while True:
                try:
                    status = load_status(args.status_file)
                    sources = api_sources(status)
                    _, limits = rate_limits(status)
                    usage = tracker.update(time.monotonic(), sources, costs, limits)
                    size = shutil.get_terminal_size((120, 40))
                    output = render(
                        status,
                        monitor_seconds=time.monotonic() - started,
                        usage=usage,
                        costs=costs,
                        width=size.columns,
                        height=size.lines,
                    )
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    output = f"KALSHI API TOP\n\nWaiting for telemetry: {exc}"
                sys.stdout.write("\033[H\033[2J" + output + "\n")
                sys.stdout.flush()
                deadline = time.monotonic() + args.interval
                while time.monotonic() < deadline:
                    if keyboard.quit_requested():
                        return 0
                    time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        except KeyboardInterrupt:
            pass
        finally:
            sys.stdout.write("\033[?25h\033[?1049l")
            sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
