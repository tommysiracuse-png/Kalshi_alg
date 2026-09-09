"""Read-only Polymarket mirror and screener monitor.

Usage:
    .venv/bin/python tools/monitor_polymarket_mirror.py
    .venv/bin/python tools/monitor_polymarket_mirror.py --interval 10
    .venv/bin/python tools/monitor_polymarket_mirror.py --window 3600 --json

The monitor reads the launcher's status, the mirror status, and the SQLite
databases. It never writes to the mirror or session database.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MIRROR_STATUS = ROOT / "runtime" / "polymarket_mirror_status.json"
DEFAULT_LAUNCHER_STATUS = ROOT / "runtime" / "launcher_status.json"
DEFAULT_MIRROR_DB = ROOT / "runtime" / "polymarket_mirror.sqlite3"
DEFAULT_SESSIONS_DB = ROOT / "session_data" / "sessions.sqlite3"


def _read_json(path: Path) -> dict[str, Any]:
    """Read a status file while tolerating an atomic-write race."""

    last_error: Optional[Exception] = None
    for _ in range(3):
        try:
            with path.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.05)
    if last_error is not None:
        raise last_error
    return {}


def _connect_read_only(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=1000")
    return connection


def _first_mapping(value: Any, *keys: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        for key in keys:
            candidate = value.get(key)
            if isinstance(candidate, Mapping):
                return candidate
    return {}


def _polymarket_monitoring(launcher: Mapping[str, Any]) -> Mapping[str, Any]:
    monitoring = launcher.get("venueMonitoring")
    if isinstance(monitoring, list):
        for item in monitoring:
            if isinstance(item, Mapping) and str(item.get("venue", "")).lower() == "polymarket":
                return item
    if isinstance(monitoring, Mapping):
        item = monitoring.get("polymarket")
        if isinstance(item, Mapping):
            return item
    return {}


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _timestamp(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except (TypeError, ValueError, OSError, OverflowError):
        return str(value)


def _age_seconds(value: Any, now_ms: int) -> Optional[float]:
    if value in (None, ""):
        return None
    return max(0.0, (now_ms - _number(value)) / 1000.0)


class _BookCoverageEstimator:
    """Estimate time until the current mirror generation reaches 100% books."""

    def __init__(self) -> None:
        self.generation_id: Optional[str] = None
        self.last_at_ms: Optional[int] = None
        self.last_ready_markets: Optional[int] = None
        self.rates_markets_per_second: deque[float] = deque(maxlen=8)

    def estimate(self, status: Mapping[str, Any], *, now_ms: int) -> dict[str, Any]:
        generation_id = str(status.get("generationId") or "") or None
        if generation_id != self.generation_id:
            self.generation_id = generation_id
            self.last_at_ms = None
            self.last_ready_markets = None
            self.rates_markets_per_second.clear()

        total = max(0, _integer(status.get("catalogCount")))
        ready = max(0, min(total, _integer(status.get("bookReadyMarkets")))) if total else 0
        if self.last_at_ms is not None and self.last_ready_markets is not None:
            elapsed_seconds = (now_ms - self.last_at_ms) / 1000.0
            progress = ready - self.last_ready_markets
            if elapsed_seconds > 0 and progress > 0:
                self.rates_markets_per_second.append(progress / elapsed_seconds)
        self.last_at_ms = now_ms
        self.last_ready_markets = ready

        remaining = max(0, total - ready)
        coverage = (ready / total) if total else None
        is_complete = bool(total and ready >= total)
        is_scanning = bool(status.get("running")) and not bool(status.get("complete"))
        rate = (
            sum(self.rates_markets_per_second) / len(self.rates_markets_per_second)
            if self.rates_markets_per_second else None
        )

        # A one-shot invocation has no prior monitor sample. The mirror's
        # catalog duration provides a useful fallback while a generation is
        # actively syncing, but it is intentionally not used after a completed
        # sync because cached books may have been present before this run.
        rate_source = "observed_refreshes"
        if rate is None and is_scanning:
            duration_ms = _integer(status.get("catalogDurationMs"))
            if duration_ms <= 0:
                started_ms = status.get("catalogStartedAtMs") or status.get("startedAtMs")
                duration_ms = max(0, now_ms - _integer(started_ms)) if started_ms else 0
            if duration_ms > 0 and ready > 0:
                rate = ready / (duration_ms / 1000.0)
                rate_source = "mirror_elapsed"

        eta_seconds: Optional[float]
        if is_complete:
            eta_seconds = 0.0
            estimate_status = "complete"
        elif rate is not None and rate > 0 and is_scanning:
            eta_seconds = remaining / rate
            estimate_status = "estimating"
        elif not total:
            eta_seconds = None
            estimate_status = "unavailable"
        elif not is_scanning:
            eta_seconds = None
            estimate_status = "not_scanning"
        else:
            eta_seconds = None
            estimate_status = "no_progress"

        return {
            "generationId": generation_id,
            "targetMarkets": total,
            "readyMarkets": ready,
            "remainingMarkets": remaining,
            "coverage": coverage,
            "rateMarketsPerSecond": rate,
            "rateMarketsPerMinute": rate * 60.0 if rate is not None else None,
            "etaSeconds": eta_seconds,
            "status": estimate_status,
            "rateSource": rate_source if rate is not None else None,
        }


def _operation_rows(activity: Mapping[str, Any]) -> list[dict[str, Any]]:
    rest = _first_mapping(activity, "rest")
    operations = rest.get("operations")
    if not isinstance(operations, Mapping):
        return []
    rows = []
    for operation, raw in operations.items():
        if not isinstance(raw, Mapping):
            continue
        rows.append({
            "operation": str(operation),
            "total": _integer(raw.get("total")),
            "successes": _integer(raw.get("successes")),
            "errors": _integer(raw.get("errors")),
            "requestsLast60s": _integer(raw.get("requestsLast60s")),
            "errorsLast60s": _integer(raw.get("errorsLast60s")),
            "averageLatencyMs": _number(raw.get("averageLatencyMs")),
            "lastActivityAtMs": raw.get("lastActivityAtMs"),
        })
    return sorted(rows, key=lambda row: (-row["total"], row["operation"]))


def _api_summary(activity: Mapping[str, Any]) -> dict[str, Any]:
    rest = _first_mapping(activity, "rest")
    stream = _first_mapping(activity, "stream")
    return {
        "rest": {
            "total": _integer(rest.get("total")),
            "successes": _integer(rest.get("successes")),
            "errors": _integer(rest.get("errors")),
            "requestsLast60s": _integer(rest.get("requestsLast60s")),
            "errorsLast60s": _integer(rest.get("errorsLast60s")),
            "rateLimitErrors": _integer(rest.get("rateLimitErrors")),
            "rateLimitErrorsLast60s": _integer(rest.get("rateLimitErrorsLast60s")),
            "averageLatencyMs": _number(rest.get("averageLatencyMs")),
            "lastActivityAtMs": rest.get("lastActivityAtMs"),
            "lastError": rest.get("lastError"),
            "operations": _operation_rows(activity),
        },
        "stream": {
            "messagesLast60s": _integer(stream.get("messagesLast60s")),
            "messages": _integer(stream.get("message")),
            "connections": _integer(stream.get("connections")),
            "connectAttempts": _integer(stream.get("connectAttempts")),
            "subscriptionsSent": _integer(stream.get("subscriptionsSent")),
            "streamErrors": _integer(stream.get("streamErrors")),
            "closes": _integer(stream.get("closes")),
            "lastActivityAtMs": stream.get("lastActivityAtMs"),
        },
    }


def _query_one(connection: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> dict[str, Any]:
    row = connection.execute(query, tuple(params)).fetchone()
    return dict(row) if row is not None else {}


def _mirror_database_summary(path: Path, *, now_ms: int, freshness_seconds: float) -> dict[str, Any]:
    connection = _connect_read_only(path)
    try:
        meta = {
            str(row["key"]): row["value"]
            for row in connection.execute("SELECT key, value FROM mirror_meta")
        }
        generation = meta.get("active_generation")
        if not generation:
            return {"activeGeneration": None, "error": "active_generation is not published"}

        cutoff_ms = int(now_ms - freshness_seconds * 1000)
        markets = _query_one(
            connection,
            """
            SELECT
                COUNT(*) AS activeMarkets,
                SUM(open_interest_units IS NULL) AS missingOpenInterest,
                SUM(open_interest_units = 0) AS zeroOpenInterest,
                SUM(open_interest_units > 0) AS positiveOpenInterest,
                SUM(open_interest_units >= 10000) AS openInterestAtLeast100,
                SUM(volume_24h_units IS NULL) AS missingVolume
            FROM mirror_markets
            WHERE generation_id = ? AND status = 'active'
            """,
            (generation,),
        )
        books = _query_one(
            connection,
            """
            SELECT
                COUNT(*) AS bookAssets,
                SUM(timestamp_ms > 0) AS assetsWithTimestamp,
                SUM(timestamp_ms >= ?) AS freshBookAssets,
                SUM(stale = 1) AS staleBookAssets,
                MAX(timestamp_ms) AS newestBookTimestampMs,
                MAX(updated_at_ms) AS newestBookUpdateMs
            FROM mirror_books
            """,
            (cutoff_ms,),
        )
        market_books = _query_one(
            connection,
            """
            WITH active AS (
                SELECT market_id, yes_token_id, no_token_id, open_interest_units
                FROM mirror_markets
                WHERE generation_id = ? AND status = 'active'
            ), per_market AS (
                SELECT
                    active.market_id,
                    active.open_interest_units,
                    MAX(COALESCE(yes.timestamp_ms, 0), COALESCE(no.timestamp_ms, 0)) AS latestBookTimestampMs
                FROM active
                LEFT JOIN mirror_books AS yes ON yes.asset_id = active.yes_token_id
                LEFT JOIN mirror_books AS no ON no.asset_id = active.no_token_id
                GROUP BY active.market_id
            )
            SELECT
                COUNT(*) AS activeMarkets,
                SUM(latestBookTimestampMs > 0) AS marketsWithBooks,
                SUM(latestBookTimestampMs >= ?) AS marketsWithFreshBooks,
                SUM(latestBookTimestampMs > 0 AND latestBookTimestampMs < ?) AS marketsWithStaleBooks,
                SUM(latestBookTimestampMs = 0) AS marketsWithoutBooks,
                SUM(open_interest_units IS NOT NULL AND latestBookTimestampMs >= ?) AS freshBooksWithOpenInterest,
                SUM(open_interest_units >= 10000 AND latestBookTimestampMs >= ?) AS freshBooksWithOpenInterestAtLeast100
            FROM per_market
            """,
            (generation, cutoff_ms, cutoff_ms, cutoff_ms, cutoff_ms),
        )
        return {
            "activeGeneration": generation,
            "markets": {key: _integer(value) for key, value in markets.items()},
            "books": {
                key: (_integer(value) if key not in {"newestBookTimestampMs", "newestBookUpdateMs"} else value)
                for key, value in books.items()
            },
            "marketBookCoverage": {key: _integer(value) for key, value in market_books.items()},
            "freshnessSeconds": freshness_seconds,
            "cutoffMs": cutoff_ms,
        }
    finally:
        connection.close()


def _screener_summary(path: Path, *, now_ms: int, window_seconds: float) -> dict[str, Any]:
    connection = _connect_read_only(path)
    try:
        cutoff_ms = int(now_ms - window_seconds * 1000)
        aggregate = _query_one(
            connection,
            """
            SELECT
                COUNT(*) AS updates,
                SUM(status = 'succeeded') AS succeeded,
                SUM(status = 'running') AS running,
                SUM(status = 'failed') AS failed,
                SUM(status = 'interrupted') AS interrupted,
                SUM(COALESCE(scanned_markets, 0)) AS scannedMarketObservations,
                AVG(scanned_markets) AS averageMarketsPerUpdate,
                MAX(scanned_markets) AS maxMarketsPerUpdate,
                SUM(COALESCE(api_requests, 0)) AS apiRequests,
                SUM(COALESCE(api_errors, 0)) AS apiErrors,
                SUM(added_count) AS added,
                SUM(changed_count) AS changed,
                SUM(removed_count) AS removed
            FROM screener_runs
            WHERE venue = 'polymarket' AND started_at_ms >= ?
            """,
            (cutoff_ms,),
        )
        latest = _query_one(
            connection,
            """
            SELECT id, fleet_run_id, status, reason, started_at_ms, ended_at_ms,
                   duration_ms, scanned_markets, api_requests, api_errors,
                   added_count, changed_count, removed_count, error
            FROM screener_runs
            WHERE venue = 'polymarket'
            ORDER BY started_at_ms DESC, id DESC
            LIMIT 1
            """,
        )
        current_run = _query_one(
            connection,
            """
            SELECT id, status, started_at_ms, ended_at_ms
            FROM runs
            ORDER BY started_at_ms DESC, id DESC
            LIMIT 1
            """,
        )
        return {
            "windowSeconds": window_seconds,
            "cutoffMs": cutoff_ms,
            "aggregate": {
                key: (_number(value) if key == "averageMarketsPerUpdate" else _integer(value))
                for key, value in aggregate.items()
            },
            "latest": latest,
            "currentFleetRun": current_run,
        }
    finally:
        connection.close()


def collect_report(args: argparse.Namespace) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    coverage_estimator = getattr(args, "_coverage_estimator", None)
    if not isinstance(coverage_estimator, _BookCoverageEstimator):
        coverage_estimator = _BookCoverageEstimator()
        setattr(args, "_coverage_estimator", coverage_estimator)
    errors: list[str] = []
    try:
        mirror_status = _read_json(args.mirror_status)
    except Exception as exc:
        mirror_status = {}
        errors.append(f"mirror status: {exc}")
    try:
        launcher_status = _read_json(args.launcher_status)
    except Exception as exc:
        launcher_status = {}
        errors.append(f"launcher status: {exc}")

    monitoring = _polymarket_monitoring(launcher_status)
    launcher_activity = monitoring.get("apiActivity") if isinstance(monitoring, Mapping) else {}
    if not isinstance(launcher_activity, Mapping):
        launcher_activity = {}
    mirror_activity = mirror_status.get("apiActivity")
    if not isinstance(mirror_activity, Mapping):
        mirror_activity = {}
    coverage_eta = coverage_estimator.estimate(mirror_status, now_ms=now_ms)

    try:
        database = _mirror_database_summary(
            args.mirror_db,
            now_ms=now_ms,
            freshness_seconds=args.freshness,
        )
    except Exception as exc:
        database = {"error": str(exc)}
        errors.append(f"mirror database: {exc}")
    try:
        screener = _screener_summary(
            args.sessions_db,
            now_ms=now_ms,
            window_seconds=args.window,
        )
    except Exception as exc:
        screener = {"error": str(exc)}
        errors.append(f"sessions database: {exc}")

    return {
        "generatedAtMs": now_ms,
        "generatedAt": _timestamp(now_ms),
        "errors": errors,
        "mirror": {
            "status": mirror_status,
            "database": database,
            "coverageEta": coverage_eta,
        },
        "launcher": {
            "lifecycle": launcher_status.get("launcher", {}).get("lifecycle") if isinstance(launcher_status.get("launcher"), Mapping) else None,
            "pid": launcher_status.get("launcher", {}).get("pid") if isinstance(launcher_status.get("launcher"), Mapping) else None,
            "polymarket": {
                "active": monitoring.get("active"),
                "configuredBots": monitoring.get("configuredBots"),
                "botsRunning": monitoring.get("botsRunning"),
            },
        },
        "api": {
            "launcher": _api_summary(launcher_activity),
            "mirror": _api_summary(mirror_activity),
            "rateLimit": mirror_status.get("rateLimit") or launcher_activity.get("rateLimit") or {},
            "openInterest": mirror_status.get("openInterest") or {},
            "openInterestDiagnostics": (
                mirror_status.get("openInterestDiagnostics")
                or mirror_activity.get("openInterestDiagnostics")
                or launcher_activity.get("openInterestDiagnostics")
                or {}
            ),
        },
        "screener": screener,
    }


def _fmt_number(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{_integer(value):,}"


def _format_duration_seconds(value: Any) -> str:
    if value is None:
        return "unknown"
    seconds = max(0, int(math.ceil(_number(value))))
    if seconds < 60:
        return f"{seconds}s"
    minutes, remainder = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {remainder:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours:02d}h"


def _print_operations(activity: Mapping[str, Any], limit: int) -> None:
    rows = activity.get("rest", {}).get("operations", [])
    if not rows:
        print("  REST operations: unavailable")
        return
    print("  REST operations (cumulative / last 60s):")
    print("    operation                                      total ok err last60 avg-ms")
    for row in rows[:limit]:
        print(
            f"    {str(row['operation']):44.44} "
            f"{row['total']:5d} {row['successes']:5d} {row['errors']:3d} "
            f"{row['requestsLast60s']:6d} {row['averageLatencyMs']:7.1f}"
        )


def _print_text(report: Mapping[str, Any], operation_limit: int) -> None:
    mirror = report.get("mirror", {})
    status = mirror.get("status", {})
    database = mirror.get("database", {})
    launcher = report.get("launcher", {})
    api = report.get("api", {})
    screener = report.get("screener", {})
    rest = api.get("launcher", {}).get("rest", {})
    stream = api.get("launcher", {}).get("stream", {})

    print(f"Polymarket mirror monitor | {report.get('generatedAt', '-')}")
    if report.get("errors"):
        print("Warnings:")
        for error in report["errors"]:
            print(f"  - {error}")

    print("\nMirror")
    print(
        f"  process={status.get('pid', '-')} running={status.get('running', '-') } "
        f"reason={status.get('reason', '-')} screenable={status.get('screenable', '-')} "
        f"generation={status.get('generationId', database.get('activeGeneration', '-'))}"
    )
    print(
        f"  catalog={_fmt_number(status.get('catalogCount'))} "
        f"book-ready={_fmt_number(status.get('bookReadyMarkets'))} "
        f"coverage={_number(status.get('bookCoverage')):.1%} "
        f"latest-book-age={_fmt_number(status.get('bookLatestAgeMs'))} ms"
    )
    coverage_eta = mirror.get("coverageEta", {})
    if isinstance(coverage_eta, Mapping):
        eta_status = str(coverage_eta.get("status") or "unavailable")
        if eta_status == "complete":
            eta_text = "complete"
        elif eta_status == "estimating":
            eta_text = (
                f"{_format_duration_seconds(coverage_eta.get('etaSeconds'))} remaining "
                f"at {_number(coverage_eta.get('rateMarketsPerMinute')):.1f} markets/min"
            )
        elif eta_status == "no_progress":
            eta_text = "unknown (no book progress observed)"
        elif eta_status == "not_scanning":
            eta_text = "unknown (mirror is not actively scanning)"
        else:
            eta_text = "unknown"
        print(
            f"  estimated time to 100% book coverage={eta_text} "
            f"({_fmt_number(coverage_eta.get('readyMarkets'))}/"
            f"{_fmt_number(coverage_eta.get('targetMarkets'))} markets)"
        )
    markets = database.get("markets", {})
    print(
        f"  active DB markets={_fmt_number(markets.get('activeMarkets'))} "
        f"OI missing={_fmt_number(markets.get('missingOpenInterest'))} "
        f"OI zero={_fmt_number(markets.get('zeroOpenInterest'))} "
        f"OI positive={_fmt_number(markets.get('positiveOpenInterest'))} "
        f"OI >= 100={_fmt_number(markets.get('openInterestAtLeast100'))}"
    )
    coverage = database.get("marketBookCoverage", {})
    print(
        f"  market books: with-books={_fmt_number(coverage.get('marketsWithBooks'))} "
        f"fresh={_fmt_number(coverage.get('marketsWithFreshBooks'))} "
        f"stale={_fmt_number(coverage.get('marketsWithStaleBooks'))} "
        f"missing={_fmt_number(coverage.get('marketsWithoutBooks'))} "
        f"fresh+OI={_fmt_number(coverage.get('freshBooksWithOpenInterest'))}"
    )

    print("\nAPI usage (launcher Polymarket activity)")
    print(
        f"  REST total={_fmt_number(rest.get('total'))} ok={_fmt_number(rest.get('successes'))} "
        f"errors={_fmt_number(rest.get('errors'))} last60s={_fmt_number(rest.get('requestsLast60s'))} "
        f"error-last60s={_fmt_number(rest.get('errorsLast60s'))} "
        f"avg-latency={_number(rest.get('averageLatencyMs')):.1f} ms"
    )
    print(
        f"  websocket messages-last60s={_fmt_number(stream.get('messagesLast60s'))} "
        f"connections={_fmt_number(stream.get('connections'))} "
        f"errors={_fmt_number(stream.get('streamErrors'))} closes={_fmt_number(stream.get('closes'))}"
    )
    _print_operations(api.get("launcher", {}), operation_limit)
    open_interest = api.get("openInterest", {})
    if open_interest:
        print(
            f"  OI hydration: requested={_fmt_number(open_interest.get('marketsRequested'))} "
            f"resolved={_fmt_number(open_interest.get('marketsResolved'))} "
            f"missing={_fmt_number(open_interest.get('marketsMissing'))} "
            f"batches={_fmt_number(open_interest.get('batches'))} "
            f"API errors={_fmt_number(open_interest.get('apiErrors'))} "
            f"403={_fmt_number(open_interest.get('forbiddenResponses'))} "
            f"Cloudflare={_fmt_number(open_interest.get('cloudflare403'))} "
            f"retries={_fmt_number(open_interest.get('retries'))} "
            f"exhausted={_fmt_number(open_interest.get('retryExhausted'))} "
            f"suppressed={_fmt_number(open_interest.get('suppressedRequests'))}"
        )
    diagnostics = api.get("openInterestDiagnostics", {})
    last_cloudflare = diagnostics.get("lastCloudflare") if isinstance(diagnostics, Mapping) else None
    if isinstance(last_cloudflare, Mapping):
        print(
            f"  latest Cloudflare response: ray={last_cloudflare.get('cfRay') or '-'} "
            f"cache={last_cloudflare.get('cfCacheStatus') or '-'} "
            f"batch={last_cloudflare.get('batch', '-')} "
            f"attempt={last_cloudflare.get('attempt', '-')}"
        )
    rate_limit = api.get("rateLimit", {})
    buckets = rate_limit.get("buckets") if isinstance(rate_limit, Mapping) else None
    if isinstance(buckets, Mapping) and "data_open_interest" in buckets:
        bucket = buckets["data_open_interest"]
        print(
            f"  data_open_interest bucket: rate={_number(bucket.get('ratePerSecond')):.1f}/s "
            f"capacity={_fmt_number(bucket.get('capacity'))} tokens={_fmt_number(bucket.get('tokens'))}"
        )

    aggregate = screener.get("aggregate", {})
    print(f"\nScreener aggregates (Polymarket, last {_number(screener.get('windowSeconds')) / 60:.0f} min)")
    print(
        f"  updates={_fmt_number(aggregate.get('updates'))} "
        f"succeeded={_fmt_number(aggregate.get('succeeded'))} "
        f"running={_fmt_number(aggregate.get('running'))} "
        f"failed={_fmt_number(aggregate.get('failed'))} "
        f"interrupted={_fmt_number(aggregate.get('interrupted'))}"
    )
    print(
        f"  scanned market observations={_fmt_number(aggregate.get('scannedMarketObservations'))} "
        f"avg/update={_number(aggregate.get('averageMarketsPerUpdate')):.1f} "
        f"max/update={_fmt_number(aggregate.get('maxMarketsPerUpdate'))}"
    )
    print(
        f"  API requests={_fmt_number(aggregate.get('apiRequests'))} "
        f"API errors={_fmt_number(aggregate.get('apiErrors'))} "
        f"added={_fmt_number(aggregate.get('added'))} "
        f"changed={_fmt_number(aggregate.get('changed'))} "
        f"removed={_fmt_number(aggregate.get('removed'))}"
    )
    latest = screener.get("latest", {})
    if latest:
        print(
            f"  latest: {latest.get('status', '-')} reason={latest.get('reason', '-')} "
            f"scanned={_fmt_number(latest.get('scanned_markets'))} "
            f"duration={_fmt_number(latest.get('duration_ms'))} ms "
            f"started={_timestamp(latest.get('started_at_ms'))}"
        )
    polymarket = launcher.get("polymarket", {})
    print(
        f"\nBots: active={polymarket.get('active', '-')} "
        f"configured={polymarket.get('configuredBots', '-')} "
        f"running={polymarket.get('botsRunning', '-')}"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor the Polymarket mirror, API usage, and screener scans.")
    parser.add_argument("--interval", type=float, default=0, help="Refresh every N seconds; 0 prints one snapshot (default).")
    parser.add_argument("--window", type=float, default=900, help="Screener aggregation window in seconds (default: 900).")
    parser.add_argument("--freshness", type=float, default=60, help="Book freshness threshold in seconds (default: 60).")
    parser.add_argument("--json", action="store_true", help="Print the complete report as JSON.")
    parser.add_argument("--no-clear", action="store_true", help="Do not clear the terminal between refreshes.")
    parser.add_argument("--operation-limit", type=int, default=15, help="Maximum API operations to print (default: 15).")
    parser.add_argument("--mirror-status", type=Path, default=DEFAULT_MIRROR_STATUS)
    parser.add_argument("--launcher-status", type=Path, default=DEFAULT_LAUNCHER_STATUS)
    parser.add_argument("--mirror-db", type=Path, default=DEFAULT_MIRROR_DB)
    parser.add_argument("--sessions-db", type=Path, default=DEFAULT_SESSIONS_DB)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _parser().parse_args(argv)
    args.interval = max(0.0, args.interval)
    args.window = max(0.0, args.window)
    args.freshness = max(0.0, args.freshness)
    args.operation_limit = max(0, args.operation_limit)
    first = True
    try:
        while True:
            if not first and args.interval > 0 and not args.json and not args.no_clear:
                print("\033[2J\033[H", end="")
            elif not first and not args.json:
                print("\n" + "=" * 100 + "\n")
            report = collect_report(args)
            if args.json:
                print(json.dumps(report, indent=2, sort_keys=True, default=str))
            else:
                _print_text(report, args.operation_limit)
            sys.stdout.flush()
            first = False
            if args.interval <= 0:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
