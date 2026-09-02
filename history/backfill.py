"""Backfill Kalshi historical 1-minute candlesticks and public trades into SQLite.

Public REST only (no credentials, no websockets); self-throttled and resumable —
re-running tops up incrementally. Selects the most liquid open markets plus
recently settled markets from the same series (complete histories with known
outcomes are the best training data).

Usage (from repo root):
  .venv\\Scripts\\python -m history.backfill --days 14 --markets 40 --rps 5
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adaptors.kalshi import (  # noqa: E402
    KalshiApiClient,
    KalshiClientConfig,
    _optional_count,
    _optional_timestamp_ms,
)
from clients.models import RateLimitError  # noqa: E402
from history.store import HistoryStore  # noqa: E402

CANDLE_CHUNK_MINUTES = 4_900  # Kalshi caps 5000 candlesticks per request
MIN_VOLUME_24H_CONTRACTS = 200
MIN_OPEN_INTEREST_CONTRACTS = 50


class Throttle:
    def __init__(self, rps: float) -> None:
        self.min_interval = 1.0 / max(0.5, rps)
        self._last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        delta = now - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.monotonic()


def call_with_retry(fn, *args, **kwargs):
    delay = 2.0
    for attempt in range(6):
        try:
            return fn(*args, **kwargs)
        except RateLimitError:
            if attempt == 5:
                raise
            print(f"[rate-limit] backing off {delay:.0f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def _load_env_files() -> None:
    """Load Kalshi credentials from ~/.config/kalshi-alg if not already in the env."""
    import os
    if os.environ.get("KALSHI_API_KEY_ID"):
        return
    config_dir = Path.home() / ".config" / "kalshi-alg"
    for name in ("bot.env", "ui.env"):
        path = config_dir / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def _contracts(raw: dict, fixed_field: str, legacy_field: str) -> int:
    units = _optional_count(raw, fixed_field, legacy_field)
    return int((units or 0) / 100)


def _series_of(raw: dict) -> str:
    ticker = str(raw.get("ticker") or "")
    return str(raw.get("series_ticker") or (raw.get("event_ticker") or ticker).split("-")[0])


def _close_ts_ms(raw: dict) -> int | None:
    for field in ("close_time", "close_date", "expiration_time", "expected_expiration_time"):
        value = raw.get(field)
        if value:
            parsed = _optional_timestamp_ms(value)
            if parsed:
                return parsed
    return None


def select_markets(client: KalshiApiClient, *, top_n: int, closed_per_series: int,
                   lookback_ms: int, now_ms: int, throttle: Throttle) -> list[dict]:
    """Find actively trading markets by sampling the global recent-trades feed,
    then add recently settled markets from the same series."""
    trade_counts: dict[str, int] = {}
    cursor = ""
    for _ in range(12):  # ~12k most recent trades across the whole exchange
        throttle.wait()
        trades, cursor = call_with_retry(
            client.list_public_trades, "", page_size=1_000, max_results=1_000, cursor=cursor,
        )
        for trade in trades:
            trade_counts[trade.market_id] = trade_counts.get(trade.market_id, 0) + 1
        if not cursor or not trades:
            break
    active = sorted(trade_counts.items(), key=lambda item: item[1], reverse=True)
    candidate_tickers = [ticker for ticker, _ in active[: top_n * 3]]
    print(f"[select] sampled recent trades: {sum(trade_counts.values())} trades across"
          f" {len(trade_counts)} markets", flush=True)

    picked: list[dict] = []
    for start in range(0, len(candidate_tickers), 100):
        batch = candidate_tickers[start:start + 100]
        throttle.wait()
        rows = call_with_retry(
            client.list_markets_raw,
            venue_filters={"tickers": ",".join(batch)}, max_results=len(batch),
        )
        picked.extend(rows)
    open_picked = [raw for raw in picked if str(raw.get("status") or "") in ("open", "active")]
    open_picked.sort(key=lambda raw: trade_counts.get(str(raw.get("ticker") or ""), 0), reverse=True)
    open_picked = open_picked[:top_n]
    picked_series = {_series_of(raw) for raw in open_picked}
    print(f"[select] picked {len(open_picked)} active markets across {len(picked_series)} series",
          flush=True)

    settled_pick: list[dict] = []
    for series in sorted(picked_series):
        throttle.wait()
        try:
            rows = call_with_retry(
                client.list_markets_raw,
                status="settled", venue_filters={"series_ticker": series}, max_results=200,
            )
        except Exception as exc:
            print(f"[select] settled scan failed for {series}: {exc}", flush=True)
            continue
        recent = [raw for raw in rows
                  if (_close_ts_ms(raw) or 0) >= now_ms - lookback_ms]
        recent.sort(key=lambda raw: (_contracts(raw, "volume_fp", "volume")
                                     or _contracts(raw, "volume_24h_fp", "volume_24h")), reverse=True)
        settled_pick.extend(recent[:closed_per_series])
    print(f"[select] added {len(settled_pick)} recently settled markets from picked series", flush=True)
    seen = set()
    combined = []
    for raw in open_picked + settled_pick:
        ticker = str(raw.get("ticker") or "")
        if ticker and ticker not in seen:
            seen.add(ticker)
            combined.append(raw)
    return combined


def backfill_market(client: KalshiApiClient, store: HistoryStore, throttle: Throttle,
                    raw: dict, *, start_ms: int, end_ms: int) -> None:
    ticker = str(raw.get("ticker") or "")
    series = _series_of(raw)
    close_ms = _close_ts_ms(raw)
    store.upsert_market(
        ticker=ticker,
        series=series,
        title=str(raw.get("title") or ""),
        status=str(raw.get("status") or ""),
        close_ts_ms=close_ms,
        result=str(raw.get("result") or ""),
        volume_24h_units=_optional_count(raw, "volume_24h_fp", "volume_24h"),
        oi_units=_optional_count(raw, "open_interest_fp", "open_interest"),
        meta={"event_ticker": raw.get("event_ticker"), "category": raw.get("category")},
    )

    window_end_ms = min(end_ms, close_ms or end_ms)
    window_start_ms = max(start_ms, (close_ms or end_ms) - (end_ms - start_ms)) if close_ms else start_ms
    open_ms = None
    if raw.get("open_time"):
        open_ms = _optional_timestamp_ms(raw.get("open_time"))
    if open_ms:
        window_start_ms = max(window_start_ms, open_ms)
    if window_end_ms <= window_start_ms:
        store.set_progress(ticker, "candles", complete=True)
        store.set_progress(ticker, "trades", complete=True)
        return

    # Candles: chunked forward passes, resumable from last_ts_ms.
    done, last_ts_ms, _ = store.get_progress(ticker, "candles")
    if not done:
        cursor_ms = max(window_start_ms, (last_ts_ms or 0) + 1)
        total = 0
        while cursor_ms < window_end_ms:
            chunk_end_ms = min(window_end_ms, cursor_ms + CANDLE_CHUNK_MINUTES * 60_000)
            throttle.wait()
            rows = call_with_retry(
                client.get_market_candlesticks,
                series, ticker,
                start_ts_s=cursor_ms // 1000,
                end_ts_s=chunk_end_ms // 1000,
                period_interval_minutes=1,
            )
            total += store.insert_candles(ticker, rows)
            store.set_progress(ticker, "candles", complete=False, last_ts_ms=chunk_end_ms)
            store.commit()
            cursor_ms = chunk_end_ms + 1
        store.set_progress(ticker, "candles", complete=True, last_ts_ms=window_end_ms)
        store.commit()
        print(f"[candles] {ticker}: {total} rows", flush=True)

    # Trades: cursor pagination (newest first), bounded by the window; resumable via cursor.
    done, _, cursor = store.get_progress(ticker, "trades")
    if not done:
        total = 0
        while True:
            throttle.wait()
            trades, cursor = call_with_retry(
                client.list_public_trades,
                ticker,
                min_ts_s=window_start_ms // 1000,
                max_ts_s=window_end_ms // 1000,
                page_size=1_000,
                max_results=1_000,
                cursor=cursor,
            )
            total += store.insert_trades(ticker, trades)
            store.set_progress(ticker, "trades", complete=False, cursor=cursor)
            store.commit()
            if not cursor or not trades:
                break
        store.set_progress(ticker, "trades", complete=True, cursor="")
        store.commit()
        print(f"[trades] {ticker}: {total} rows", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="history_data/history.sqlite3")
    parser.add_argument("--days", type=float, default=14.0)
    parser.add_argument("--markets", type=int, default=40)
    parser.add_argument("--closed-per-series", type=int, default=3)
    parser.add_argument("--rps", type=float, default=5.0)
    args = parser.parse_args()

    import os
    _load_env_files()
    api_key_id = os.environ.get("KALSHI_API_KEY_ID", "")
    private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
    if api_key_id and private_key_path and Path(private_key_path).exists():
        client = KalshiApiClient(KalshiClientConfig(
            api_key_id=api_key_id, private_key_path=private_key_path,
            enable_shared_write_rate_limiter=False,
        ))
        print("[auth] using authenticated client (higher rate tier)", flush=True)
    else:
        client = KalshiApiClient(KalshiClientConfig(public_only=True))
        print("[auth] no credentials found; using public client", flush=True)
    store = HistoryStore(args.output)
    throttle = Throttle(args.rps)
    now_ms = int(time.time() * 1000)
    lookback_ms = int(args.days * 86_400_000)

    markets = select_markets(
        client, top_n=args.markets, closed_per_series=args.closed_per_series,
        lookback_ms=lookback_ms, now_ms=now_ms, throttle=throttle,
    )
    started = time.monotonic()
    for index, raw in enumerate(markets, 1):
        ticker = raw.get("ticker")
        try:
            backfill_market(client, store, throttle, raw,
                            start_ms=now_ms - lookback_ms, end_ms=now_ms)
        except Exception as exc:  # keep going; a single market must not kill the run
            print(f"[error] {ticker}: {exc}", flush=True)
        if index % 5 == 0 or index == len(markets):
            elapsed = time.monotonic() - started
            print(f"[progress] {index}/{len(markets)} markets, {elapsed:.0f}s elapsed,"
                  f" summary={json.dumps(store.summary())}", flush=True)
    print(f"[done] {json.dumps(store.summary())}", flush=True)
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
