"""Worker process hosting up to 25 independently stateful market actors."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import multiprocessing as mp
import os
import queue
import threading
try:
    import resource
except ImportError:  # Windows has no POSIX resource module.
    resource = None  # type: ignore[assignment]
import time
import uuid
from collections import deque
from dataclasses import dataclass
from dataclasses import replace
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from clients.base_client import BaseClient
from clients.factory import build_client
from clients.models import (
    AccountOrder,
    AccountPosition,
    Fill,
    Market,
    OrderBookDelta,
    OrderBookSnapshot,
    OrderUpdate,
    PublicTrade,
    StreamReset,
    StartupAccountSnapshot,
    TickerUpdate,
)
from core.fleet_models import MarketHealth, ScreenerPick, WorkerControlAck, WorkerHeartbeat
from core.session_config import bot_settings_payload
from bots.top_of_book_bot import BotSettings, MarketActor, TelemetryStore, load_market_metadata
from .execution import BrokerRequest, BrokerRpcClient, read_queue_lockfree
from .risk import (
    DEFAULT_RISK_STALE_AFTER_MS,
    RISK_FLATTEN_LATCH_MS,
    RiskDecision,
    RiskSample,
    RiskThresholds,
    evaluate_risk,
    latch_flatten_only,
)


def _rss_bytes() -> int:
    if resource is None:  # Windows: RSS accounting unavailable, non-critical.
        return 0
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value * 1024) if os.name != "darwin" else int(value)


def resolve_event_loop_lag(
    measured_event_loop_lag_ms: Optional[int],
    fallback_event_loop_lag_ms: int,
) -> int:
    """Select the measured heartbeat lag without relying on an undefined alias."""
    return int(
        measured_event_loop_lag_ms
        if measured_event_loop_lag_ms is not None
        else fallback_event_loop_lag_ms
    )


def offer_latest_heartbeat(handoff: queue.Queue[Any], heartbeat: Any) -> bool:
    """Put the newest heartbeat in a bounded handoff, replacing stale data."""
    try:
        handoff.put_nowait(heartbeat)
        return False
    except queue.Full:
        pass
    try:
        handoff.get_nowait()
    except queue.Empty:
        pass
    try:
        handoff.put_nowait(heartbeat)
    except queue.Full:
        return False
    return True


# Markets reconciled concurrently. add_actor/remove_actor each make several
# rate-limited REST round-trips; running a large screener refresh serially kept
# the worker inside one reconcile for minutes, so its new actors were never
# subscribed (no order book -> permanently stuck in `startup`, unable to quote)
# and no further control command could be processed. Bounded so the burst of
# REST calls stays inside the venue's rate limits.
RECONCILE_CONCURRENCY = 8

# A market whose actor could not be started (RECONCILE_STEP_FAILED, typically a
# broker/venue timeout) is retried by the worker itself with exponential
# backoff instead of waiting for the next screener refresh.  Retries continue
# for as long as the controller still desires the market.
ADD_RETRY_BASE_SECONDS = 30.0
ADD_RETRY_MAX_SECONDS = 300.0
ADD_RETRY_POLL_SECONDS = 5.0

# Shutdown: probe the broker once, then bound every remaining broker wait so
# order verification falls back to the direct venue client instead of hanging.
SHUTDOWN_BROKER_PROBE_SECONDS = 3.0
SHUTDOWN_RPC_TIMEOUT_SECONDS = 10.0


def add_retry_delay_seconds(attempts: int) -> float:
    """Backoff before the ``attempts``-th failed add is retried."""
    return min(ADD_RETRY_MAX_SECONDS, ADD_RETRY_BASE_SECONDS * (2 ** max(0, int(attempts) - 1)))


def restore_cached_polymarket_book(actor: MarketActor, client: Any) -> bool:
    """Seed a Polymarket actor from its compact persistent book cache.

    A quiet market may not emit another websocket event after an actor is
    created.  Applying the latest valid cache snapshot here lets the normal
    quote and risk loops start immediately while the websocket continues to
    provide fresh deltas.
    """
    if str(getattr(getattr(actor, "market", None), "venue", "")).lower() != "polymarket":
        return False
    cache = getattr(client, "book_cache", None)
    get_book = getattr(cache, "get", None)
    if not callable(get_book):
        return False
    yes = get_book(str(getattr(actor.market, "yes_token_id", "") or ""))
    no = get_book(str(getattr(actor.market, "no_token_id", "") or ""))
    if yes is None or no is None or yes.bid_units is None or no.bid_units is None:
        return False
    snapshot = OrderBookSnapshot(
        actor.settings.market_ticker,
        None,
        {int(yes.bid_units): int(yes.bid_size_units or 0)},
        {int(no.bid_units): int(no.bid_size_units or 0)},
        venue="polymarket",
        yes_ask_levels=(
            {int(yes.ask_units): int(yes.ask_size_units or 0)}
            if yes.ask_units is not None else {}
        ),
        no_ask_levels=(
            {int(no.ask_units): int(no.ask_size_units or 0)}
            if no.ask_units is not None else {}
        ),
    )
    actor.handle_event(snapshot)
    return True


def restore_market_top_book(actor: MarketActor, market: Any) -> bool:
    """Apply a normalized REST top-of-book response to an actor."""
    yes_bid = getattr(market, "yes_bid_units", None)
    no_bid = getattr(market, "no_bid_units", None)
    if yes_bid is None or no_bid is None:
        return False
    snapshot = OrderBookSnapshot(
        actor.settings.market_ticker,
        None,
        {int(yes_bid): int(getattr(market, "yes_bid_size_units", 0) or 0)},
        {int(no_bid): int(getattr(market, "no_bid_size_units", 0) or 0)},
        venue="polymarket",
        yes_ask_levels=(
            {int(market.yes_ask_units): int(getattr(market, "yes_ask_size_units", 0) or 0)}
            if getattr(market, "yes_ask_units", None) is not None else {}
        ),
        no_ask_levels=(
            {int(market.no_ask_units): int(getattr(market, "no_ask_size_units", 0) or 0)}
            if getattr(market, "no_ask_units", None) is not None else {}
        ),
    )
    actor.handle_event(snapshot)
    return True


def seed_risk_from_book(
    actor: MarketActor,
    window: deque[RiskSample],
    last_event_ms: dict[str, int],
    *,
    timestamp_ms: Optional[int] = None,
) -> bool:
    """Seed risk freshness from a restored snapshot."""
    if not getattr(actor, "book_ready", False):
        return False
    yes = best_bid_units(getattr(actor, "book_yes", None))
    no = best_bid_units(getattr(actor, "book_no", None))
    if yes is None or no is None:
        return False
    stamp = int(timestamp_ms or time.time() * 1000)
    ticker = str(actor.settings.market_ticker)
    window.append(RiskSample(stamp, yes, no))
    last_event_ms[ticker] = stamp
    actor.set_fleet_risk("normal", reason="book_bootstrap", generated_at_ms=stamp)
    return True


@dataclass
class PendingAdd:
    pick: ScreenerPick
    attempts: int = 0
    retry_at_ms: int = 0
    error: str = ""


def queue_pending_adds(
    pending_adds: dict[str, PendingAdd],
    desired: Mapping[str, ScreenerPick],
    actors: Iterable[str],
) -> list[str]:
    """Bring ``pending_adds`` in line with a reconcile's desired set.

    Markets no longer desired are dropped; every desired market without a
    running actor is (re)queued.  A market already waiting keeps its attempt
    count but becomes due immediately: a reconcile re-issued after a broker
    restart is the signal that the venue path is back, so a market sitting
    in a 5-minute backoff must not stay bookless for the rest of it.
    Returns the tickers now due.
    """
    running = set(actors)
    for ticker in list(pending_adds):
        if ticker not in desired:
            pending_adds.pop(ticker, None)
    due: list[str] = []
    for ticker in sorted(set(desired) - running):
        item = pending_adds.get(ticker)
        if item is None:
            pending_adds[ticker] = PendingAdd(desired[ticker])
        else:
            item.pick = desired[ticker]
            item.retry_at_ms = 0
        due.append(ticker)
    return due


def watchdog_exit_allowed(*, mode: str, position_units: int, shutdown_frozen: bool) -> bool:
    return mode == "flatten_only" and bool(position_units) and not shutdown_frozen


def best_bid_units(levels: Mapping[int, int] | None) -> Optional[int]:
    return max(levels) if levels else None


# Venue MARKET-DATA events: the only stream traffic that proves a market's
# own subscription is alive.  OrderUpdate / Fill / PositionUpdate are the
# worker's *own* order traffic (user-level channels): they keep flowing while
# the actor cancels and the watchdog fires IOC exits on a market whose book
# feed has died, so they must never count as liveness evidence or produce a
# book sample.  StreamReset is adaptor-generated.
MARKET_DATA_EVENTS = (OrderBookSnapshot, OrderBookDelta, PublicTrade, TickerUpdate)

# Risk samples are appended at most this often per market. Busy books (tennis
# in play 24-39 events/s, 15-minute crypto ~800/s) would otherwise push 120 s
# of history through a bounded deque in seconds, silently shrinking the
# time-bounded move window to a few seconds of data. At 10 samples/s the
# RISK_WINDOW_MAX_SAMPLES deque holds 300 s, longer than any window in use.
RISK_SAMPLE_MIN_INTERVAL_MS = 100
RISK_WINDOW_MAX_SAMPLES = 3_000


def is_market_data_event(event: Any) -> bool:
    return isinstance(event, MARKET_DATA_EVENTS)


def observe_stream_event(
    event: Any,
    actor: Any,
    window: deque[RiskSample] | list[RiskSample],
    last_event_ms: dict[str, int],
    *,
    now_ms: int,
) -> bool:
    """Per-event liveness / sample bookkeeping for one stream event.

    Only a venue market-data event refreshes ``last_event_ms`` for its
    market and, if the actor's book is ready, appends a *real* ``RiskSample``
    of the current best bids to ``window``.  Every other event (own order
    updates, fills, position updates, stream resets) leaves both untouched.
    Returns True when a sample was appended.
    """
    if not is_market_data_event(event):
        return False
    last_event_ms[event.market_id] = int(now_ms)
    if not getattr(actor, "book_ready", False):
        return False
    if window:
        newest = window[-1]
        if not getattr(newest, "synthetic", False) and int(now_ms) - int(newest.timestamp_ms) < RISK_SAMPLE_MIN_INTERVAL_MS:
            return False  # rate-capped; liveness was already stamped above
    window.append(RiskSample(
        int(now_ms),
        best_bid_units(getattr(actor, "book_yes", None)),
        best_bid_units(getattr(actor, "book_no", None)),
    ))
    return True


def quiet_after_ms_for(stale_after_ms: int) -> int:
    """Silence after which a quiet market is re-sampled from its book.

    Derived from the *staleness* threshold, not the move window.  A synthetic
    sample is only produced while the market's newest real sample is at
    least this old *and* its own last market-data event is within
    ``stale_after_ms``, i.e. inside a re-sample window of
    ``stale_after_ms - quiet_after_ms`` = two thirds of the stale threshold.
    At the default 120 s threshold that window is 80 s, wider than the ~60 s
    risk-loop period plus its per-visit processing, so a healthy quiet market
    is re-sampled at least once per quiet spell instead of racing the loop
    phase (at ``stale_after_ms // 2`` the window equalled the loop period and
    a visit could miss it).  A missed re-sample can no longer demote a
    market anyway - staleness is judged on real samples only - it merely
    leaves the move window without a fresh copy of the book.  Floor of 1 s so
    a tiny threshold cannot turn every visit into a duplicate of the live
    event stream.
    """
    return max(1_000, int(stale_after_ms) // 3)


def quiet_market_sample(
    actor: Any,
    samples: Iterable[RiskSample],
    *,
    now_ms: int,
    quiet_after_ms: int,
    stream_connected: bool = True,
    last_event_ms: Optional[int] = None,
    stale_after_ms: int = DEFAULT_RISK_STALE_AFTER_MS,
) -> Optional[RiskSample]:
    """Synthetic ``RiskSample`` for a healthy market whose stream has merely been quiet.

    Real risk samples are appended per venue market-data event.  For a
    market with a valid book that has had nothing to report for a while the
    current best bids are returned stamped ``now_ms`` and tagged
    ``synthetic=True`` so the mid-move window keeps a current copy of the
    book.  The synthetic sample is **not** liveness evidence: ``evaluate_risk``
    judges staleness on real samples only, so a feed that died at ``T`` is
    ``risk_inputs_stale`` at every visit after ``T + stale_after_ms``
    (worst case one risk-loop period later) whatever was synthesised.
    A sample is produced only when:

    * ``last_event_ms`` - the timestamp of the newest venue *market-data*
      event for this market (book snapshot/delta, public trade, ticker) as
      recorded by the worker; own order updates, fills and position updates
      do not count - is within ``stale_after_ms`` of ``now_ms``.  The venue
      adaptor exposes no per-market heartbeat (websocket ping/pong is handled
      inside the transport and never surfaces), so a dead feed is simply not
      re-sampled.
    * ``stream_connected`` (shard-wide, cleared on generation change and on
      adaptor errors) is only an additional veto, never proof of liveness.
    * the actor's book is ready (no unresolved ``StreamReset``).
    * the newest sample is at least ``quiet_after_ms`` old so live events
      are not duplicated.
    """
    if not stream_connected or not getattr(actor, "book_ready", False):
        return None
    if last_event_ms is None or now_ms - int(last_event_ms) > max(0, int(stale_after_ms)):
        return None
    newest_ms = max((sample.timestamp_ms for sample in samples), default=None)
    if newest_ms is not None and now_ms - newest_ms < max(0, int(quiet_after_ms)):
        return None
    return RiskSample(
        int(now_ms),
        best_bid_units(getattr(actor, "book_yes", None)),
        best_bid_units(getattr(actor, "book_no", None)),
        synthetic=True,
    )


def evaluate_market_risk(
    actor: Any,
    window: deque[RiskSample] | list[RiskSample],
    *,
    ticker: str,
    now_ms: int,
    last_event_ms: Optional[int],
    stream_connected: bool,
    quiet_after_ms: int,
    flatten_latch: dict[str, int],
    risk_kwargs: Mapping[str, int],
    has_resting_orders: bool,
    latch_ms: int = RISK_FLATTEN_LATCH_MS,
) -> RiskDecision:
    """One risk-loop visit for one market: re-sample if quiet, evaluate, latch.

    Mutates ``window`` (synthetic sample appended) and ``flatten_latch``.
    """
    synthetic = quiet_market_sample(
        actor, window, now_ms=now_ms, quiet_after_ms=quiet_after_ms,
        stream_connected=stream_connected, last_event_ms=last_event_ms,
        stale_after_ms=int(risk_kwargs.get("stale_after_ms", DEFAULT_RISK_STALE_AFTER_MS)),
    )
    if synthetic is not None:
        window.append(synthetic)
    position_units = int(getattr(actor, "net_position_units", 0) or 0)
    decision = evaluate_risk(
        window, now_ms=now_ms, position_units=position_units,
        has_resting_orders=has_resting_orders, **risk_kwargs,
    )
    return latch_flatten_only(
        decision, flatten_latch, ticker, now_ms=now_ms, position_units=position_units, latch_ms=latch_ms,
    )


class FleetWorkerProcess(mp.Process):
    def __init__(
        self,
        *,
        worker_id: str,
        venue: str = "kalshi",
        client_config: Any = None,
        session_configuration: Mapping[str, Any],
        artifact_root: Path,
        broker_request_queue: Any,
        broker_response_queue: Any,
        command_queue: Any,
        heartbeat_queue: Any,
        control_ack_queue: Any,
    ) -> None:
        super().__init__(name=f"fleet-{worker_id}", daemon=False)
        self.worker_id = worker_id
        self.venue = str(venue or "kalshi")
        self.client_config = client_config
        self.session_configuration = dict(session_configuration)
        self.artifact_root = Path(artifact_root)
        self.broker_request_queue = broker_request_queue
        self.broker_response_queue = broker_response_queue
        self.command_queue = command_queue
        self.heartbeat_queue = heartbeat_queue
        self.control_ack_queue = control_ack_queue

    def run(self) -> None:  # pragma: no cover - covered by multiprocessing integration simulations
        asyncio.run(self._run())

    async def _run(self) -> None:
        shard_dir = self.artifact_root / "shards" / self.worker_id
        shard_dir.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            shard_dir / "worker.log", maxBytes=25 * 1024 * 1024, backupCount=4, encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logging.getLogger("kalshi_top_of_book_bot").addHandler(handler)
        logging.getLogger("kalshi_top_of_book_bot").setLevel(logging.INFO)
        log = logging.getLogger("kalshi_top_of_book_bot")
        markets_dir = self.artifact_root / "markets"
        venue_markets_dir = markets_dir / self.venue
        venue_markets_dir.mkdir(parents=True, exist_ok=True)
        direct: BaseClient = build_client(self.venue, self.client_config)
        client = BrokerRpcClient(
            direct,
            self.broker_request_queue,
            self.broker_response_queue,
            self.worker_id,
        )
        actors: dict[str, MarketActor] = {}
        actors_lock = threading.Lock()
        picks: dict[str, ScreenerPick] = {}
        desired_picks: dict[str, ScreenerPick] = {}
        pending_adds: dict[str, PendingAdd] = {}
        risk_windows: dict[str, deque[RiskSample]] = {}
        # Per-market liveness evidence: timestamp of the newest venue
        # MARKET-DATA event per ticker (see MARKET_DATA_EVENTS); the worker's
        # own order/fill/position traffic never refreshes it.
        last_event_ms: dict[str, int] = {}
        # Per-market flatten_only latch (see risk.latch_flatten_only).
        flatten_latch: dict[str, int] = {}
        last_allocations: dict[str, Any] = {}
        allocations_known = False
        quoting_enabled = False
        shutdown_frozen = False
        broker_responsive_hint: Optional[bool] = None
        last_retention_ms = 0
        stop_requested = asyncio.Event()
        subscriptions_ready = asyncio.Event()
        add_wakeup = asyncio.Event()
        heartbeat_thread_stop = threading.Event()
        # The async loop only replaces the newest heartbeat in this local
        # one-slot handoff.  The existing fallback thread performs the
        # potentially blocking multiprocessing-queue write.
        heartbeat_handoff: queue.Queue[WorkerHeartbeat] = queue.Queue(maxsize=1)
        heartbeat_signal = {"last_async_ms": 0}
        heartbeat_signal_lock = threading.Lock()
        heartbeat_state_lock = threading.Lock()
        heartbeat_state: dict[str, Any] = {
            "assigned_tickers": (),
            "pending_tickers": (),
            "startup_attempts": {},
            "startup_progress_at_ms": 0,
            "startup_error": "",
            "worker_error": "",
        }
        heartbeat_sequence = 0
        fallback_heartbeat_count = 0
        last_fallback_at_ms = 0
        event_loop_lag_ms = 0
        cpu_percent = 0.0
        last_cpu_process_seconds = time.process_time()
        last_cpu_sample_monotonic = time.monotonic()
        command_state = {"active": False, "queued_at_ms": 0, "wait_ms": 0}
        warning_count = 0
        error_count = 0
        last_warning_seen = ""
        last_error_seen = ""
        recovery_count = 0
        last_recovery_at_ms = 0
        last_recovery_reason = ""
        cached_rich_health: dict[str, MarketHealth] = {}
        cached_diagnostic_revisions: dict[str, tuple[Any, ...]] = {}
        cached_diagnostic_at_ms: dict[str, int] = {}
        cached_api_activity: Mapping[str, object] = {}
        cached_api_errors: Mapping[str, object] = {}
        diagnostics_duration_ms = 0
        telemetry_flush_ms = 0
        heartbeat_build_ms = 0
        heartbeat_publish_wait_ms = 0
        heartbeat_coalesced_count = 0
        last_rich_diagnostics_at_ms = 0
        diagnostics_state_lock = threading.Lock()
        reconcile_lock = asyncio.Lock()
        stream_generation = 0
        # True between the first event of a (re)connected shard stream and
        # its next generation change / adaptor error.  Only a veto for the
        # synthetic quiet-market samples: liveness itself is judged per
        # market from ``last_event_ms`` (the adaptor swallows websocket
        # failures inside its reconnect loop, so this flag alone cannot
        # prove a feed is alive).
        stream_connected = False
        fleet = self.session_configuration["fleetRuntime"]
        worker_io_threads = max(1, int(fleet.get("workerIoThreads", 8)))
        reconcile_concurrency = max(1, min(RECONCILE_CONCURRENCY, worker_io_threads))
        heartbeat_seconds = float(fleet["workerHeartbeatSeconds"])
        freshness_seconds = float(fleet["quoteFreshnessSeconds"])
        startup_attempts: dict[str, int] = {}
        startup_progress_at_ms = 0
        startup_error = ""
        worker_error = ""
        startup_account_snapshot: Optional[StartupAccountSnapshot] = None
        startup_account_read_gate = (
            asyncio.Semaphore(max(1, int(fleet.get("polymarketStartupAccountReadConcurrency", 2))))
            if self.venue.lower() == "polymarket" else None
        )
        io_executor = ThreadPoolExecutor(
            max_workers=worker_io_threads,
            thread_name_prefix=f"{self.worker_id}-io",
        )
        # All blocking actor/venue calls share one bounded pool.  asyncio's
        # default executor otherwise grows independently in every worker.
        asyncio.get_running_loop().set_default_executor(io_executor)
        # Logs one WARNING (into this worker's log) if any stored value had
        # to be replaced by a default.
        risk_thresholds = RiskThresholds.from_fleet_config(fleet, log=log)
        risk_kwargs = {
            "stale_after_ms": risk_thresholds.stale_after_ms,
            "window_ms": risk_thresholds.window_ms,
            "elevated_move_units": risk_thresholds.elevated_move_units,
            "extreme_move_units": risk_thresholds.extreme_move_units,
            "max_spread_units": risk_thresholds.max_spread_units,
        }
        # Quiet-market re-sampling cadence is tied to the stale threshold
        # (see quiet_after_ms_for), not the move window.
        quiet_after_ms = quiet_after_ms_for(risk_thresholds.stale_after_ms)

        def update_heartbeat_state() -> None:
            """Publish a thread-safe liveness snapshot for the fallback thread."""
            with actors_lock:
                assigned_tickers = tuple(sorted(actors))
            with heartbeat_state_lock:
                heartbeat_state.update(
                    {
                        "assigned_tickers": assigned_tickers,
                        "pending_tickers": tuple(sorted(pending_adds)),
                        "startup_attempts": {
                            ticker: int(item.attempts) for ticker, item in pending_adds.items()
                        },
                        "startup_progress_at_ms": int(startup_progress_at_ms),
                        "startup_error": str(startup_error or ""),
                        "worker_error": str(worker_error or ""),
                    }
                )

        def heartbeat_state_snapshot() -> dict[str, Any]:
            with heartbeat_state_lock:
                return {
                    "assigned_tickers": tuple(heartbeat_state["assigned_tickers"]),
                    "pending_tickers": tuple(heartbeat_state["pending_tickers"]),
                    "startup_attempts": dict(heartbeat_state["startup_attempts"]),
                    "startup_progress_at_ms": int(heartbeat_state["startup_progress_at_ms"]),
                    "startup_error": str(heartbeat_state["startup_error"] or ""),
                    "worker_error": str(heartbeat_state["worker_error"] or ""),
                }

        def cached_diagnostics_snapshot(
            *, include_health: bool = True,
        ) -> tuple[dict[str, MarketHealth], Mapping[str, object], Mapping[str, object], int, int, int, int]:
            with diagnostics_state_lock:
                return (
                    dict(cached_rich_health) if include_health else {},
                    dict(cached_api_activity),
                    dict(cached_api_errors),
                    int(diagnostics_duration_ms),
                    int(telemetry_flush_ms),
                    int(last_rich_diagnostics_at_ms),
                    int(heartbeat_publish_wait_ms),
                )

        def enqueue_heartbeat(heartbeat: WorkerHeartbeat) -> None:
            """Replace an older unsent heartbeat without blocking the loop."""
            nonlocal heartbeat_coalesced_count
            if offer_latest_heartbeat(heartbeat_handoff, heartbeat):
                with diagnostics_state_lock:
                    heartbeat_coalesced_count += 1

        def override_key(pick: ScreenerPick | None) -> tuple[str, tuple[tuple[str, Any], ...]]:
            if pick is None:
                return "default", ()
            return (
                str(getattr(pick, "market_class", "default") or "default"),
                tuple(tuple(item) for item in (getattr(pick, "settings_overrides", ()) or ())),
            )

        async def add_actor(pick: ScreenerPick) -> None:
            nonlocal startup_progress_at_ms, startup_error, worker_error
            if pick.market_id in actors:
                picks[pick.market_id] = pick
                return
            market_class, settings_overrides = override_key(pick)
            # Per-market-class overrides land inside the payload (and thus the
            # persisted settings.json) before the launcher-managed fields.
            payload = bot_settings_payload(
                self.session_configuration,
                overrides=settings_overrides,
                market_ticker=pick.market_id,
                yes_budget_cents=pick.yes_budget_cents,
                no_budget_cents=pick.no_budget_cents,
                telemetry_sqlite_path=str(shard_dir / "telemetry.sqlite3"),
                pnl_tracker_path=str(shard_dir / "telemetry.sqlite3"),
            )
            settings = BotSettings(**payload)
            market_dir = venue_markets_dir / pick.market_id.replace("/", "_").replace("\\", "_")
            market_dir.mkdir(parents=True, exist_ok=True)
            # settings.json stays a pure BotSettings payload (the legacy child
            # path rejects unknown keys); the class provenance sits alongside.
            (market_dir / "settings.json").write_text(
                json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8"
            )
            (market_dir / "market_class.json").write_text(
                json.dumps(
                    {
                        "marketClass": market_class,
                        "settingsOverrides": [
                            {"field": name, "value": value} for name, value in settings_overrides
                        ],
                    },
                    indent=2,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
            # The screener carries normalized Polymarket token identity in its
            # ranking payload.  Prime the child client before its metadata
            # lookup so a worker does not repeat a full catalog scan.
            prime_market = getattr(direct, "prime_market", None)
            primed_market: Optional[Market] = None
            if callable(prime_market) and pick.ranking:
                candidate = prime_market(pick.ranking)
                if isinstance(candidate, Market) and str(candidate.market_id) == str(pick.market_id):
                    primed_market = candidate
            log.info(
                "ACTOR_START_ATTEMPT | ticker=%s attempt=%d venue=%s",
                pick.market_id,
                startup_attempts.get(pick.market_id, 0) + 1,
                self.venue,
            )
            if primed_market is not None:
                metadata = await asyncio.to_thread(
                    load_market_metadata, client, pick.market_id, primed_market,
                )
                if metadata.yes_bid_units is None or metadata.no_bid_units is None:
                    # Keep the old REST book recovery behavior for a ranking
                    # that carries identity but not a usable top-of-book. The
                    # direct Polymarket client reuses its book cache before
                    # issuing token-book requests.
                    try:
                        hydrated = await asyncio.to_thread(direct.get_market, pick.market_id)
                        metadata = await asyncio.to_thread(
                            load_market_metadata, client, pick.market_id, hydrated,
                        )
                    except Exception as exc:
                        log.info(
                            "ACTOR_BOOK_RECOVERY_DEFERRED | ticker=%s error=%s",
                            pick.market_id, exc,
                        )
            else:
                metadata = await asyncio.to_thread(load_market_metadata, client, pick.market_id)
            telemetry = TelemetryStore(
                str(shard_dir / "telemetry.sqlite3"),
                enabled=settings.enable_sqlite_telemetry,
                shard_mode=True,
            )
            actor = MarketActor(
                settings,
                client,
                metadata,
                owns_api_client=False,
                quote_freshness_seconds=freshness_seconds,
                quoting_enabled=False,
                telemetry_store=telemetry,
            )
            snapshot = startup_account_snapshot
            snapshot_positions: Optional[tuple[AccountPosition, ...]] = None
            snapshot_orders: Optional[tuple[AccountOrder, ...]] = None
            if snapshot is not None:
                snapshot_age_ms = int(time.time() * 1000) - int(snapshot.captured_at_ms)
                max_age_ms = int(float(fleet.get("startupAccountSnapshotMaxAgeSeconds", 30.0)) * 1000)
                if snapshot_age_ms <= max_age_ms:
                    if snapshot.positions_available:
                        snapshot_positions = tuple(snapshot.positions_by_market.get(pick.market_id, ()))
                    if snapshot.orders_available:
                        snapshot_orders = tuple(snapshot.orders_by_market.get(pick.market_id, ()))
            await actor.start(
                startup_positions=snapshot_positions,
                startup_orders=snapshot_orders,
                startup_read_semaphore=startup_account_read_gate,
                defer_external_models=self.venue.lower() == "polymarket",
            )
            actor.telemetry_store.record_runtime_event(
                ticker=pick.market_id, source="worker", event_type="actor_started",
                payload={"worker_id": self.worker_id},
            )
            with actors_lock:
                actors[pick.market_id] = actor
                picks[pick.market_id] = pick
                risk_windows[pick.market_id] = deque(maxlen=RISK_WINDOW_MAX_SAMPLES)
            startup_progress_at_ms = int(time.time() * 1000)
            startup_attempts.pop(pick.market_id, None)
            startup_error = ""
            log.info("ACTOR_START_OK | ticker=%s venue=%s", pick.market_id, self.venue)
            cached_restored = restore_cached_polymarket_book(actor, direct)
            restored = cached_restored
            if not restored and str(getattr(metadata, "venue", "")).lower() == "polymarket":
                restored = restore_market_top_book(actor, metadata)
            if restored:
                if seed_risk_from_book(actor, risk_windows[pick.market_id], last_event_ms):
                    log.info("RISK_BOOTSTRAPPED | ticker=%s reason=book_bootstrap", pick.market_id)
                log.info("BOOK_BOOTSTRAPPED | ticker=%s source=%s", pick.market_id, "cache" if cached_restored else "rest")
            # An actor started after the controller's allocation arrived (a
            # retried add) must inherit it; otherwise it would quote both sides.
            if allocations_known:
                actor.set_allowed_quote_sides(last_allocations.get(pick.market_id, ()))
            actor.set_quoting_enabled(quoting_enabled)

        async def remove_actor(ticker: str) -> None:
            with actors_lock:
                actor = actors.pop(ticker, None)
                picks.pop(ticker, None)
                risk_windows.pop(ticker, None)
                last_event_ms.pop(ticker, None)
                flatten_latch.pop(ticker, None)
            if actor is not None:
                await actor.stop(verify_orders=True)

        def acknowledge(command: Mapping[str, Any], action: str, *, error: BaseException | None = None) -> None:
            request_id = str(command.get("request_id") or "")
            if not request_id:
                return
            self.control_ack_queue.put(WorkerControlAck(
                self.worker_id,
                request_id,
                action,
                error is None,
                int(time.time() * 1000),
                str(error) if error is not None else "",
            ))

        async def reconcile_step(ticker: str, gate: asyncio.Semaphore, coroutine_factory) -> None:
            # One market must never abort the whole reconcile: the surviving
            # markets still need their subscription update to reach the venue.
            async with gate:
                try:
                    await coroutine_factory()
                except Exception as exc:
                    log.warning("RECONCILE_STEP_FAILED | ticker=%s error=%s", ticker, exc)

        async def attempt_add(ticker: str, gate: asyncio.Semaphore) -> None:
            nonlocal startup_error, worker_error
            item = pending_adds.get(ticker)
            if item is None:
                return
            async with gate:
                try:
                    await add_actor(item.pick)
                except Exception as exc:
                    item.attempts += 1
                    startup_attempts[ticker] = item.attempts
                    item.error = str(exc)
                    startup_error = item.error
                    if "can't start new thread" in item.error.lower() or "cannot start new thread" in item.error.lower():
                        worker_error = item.error
                        log.exception("WORKER_RESOURCE_ERROR | ticker=%s error=%s", ticker, exc)
                    delay = add_retry_delay_seconds(item.attempts)
                    item.retry_at_ms = int(time.time() * 1000 + delay * 1000)
                    log.warning(
                        "RECONCILE_STEP_FAILED | ticker=%s attempt=%d retry_in_s=%.0f error=%s",
                        ticker, item.attempts, delay, exc,
                    )
                    return
            pending_adds.pop(ticker, None)
            if item.attempts:
                log.info("RECONCILE_RETRY_OK | ticker=%s attempts=%d", ticker, item.attempts)

        async def run_pending_adds() -> list[str]:
            """Start every due pending actor; returns the tickers that came up."""
            nonlocal stream_generation
            now = int(time.time() * 1000)
            due = [
                ticker for ticker, item in pending_adds.items()
                if item.retry_at_ms <= now and ticker in desired_picks and ticker not in actors
            ]
            if not due:
                return []
            had_actors = bool(actors)
            gate = asyncio.Semaphore(reconcile_concurrency)
            await asyncio.gather(*(attempt_add(ticker, gate) for ticker in due))
            added = [ticker for ticker in due if ticker in actors]
            if not added:
                return []
            if had_actors:
                try:
                    await direct.update_market_subscriptions(add=added, remove=[])
                except RuntimeError:
                    # Reconnect only when the subscription acknowledgement
                    # has not arrived yet; ordinary refreshes stay dynamic.
                    await direct.close()
            stream_generation += 1
            subscriptions_ready.set()
            return added

        async def command_loop() -> None:
            nonlocal startup_progress_at_ms, startup_error, worker_error, startup_account_snapshot
            nonlocal quoting_enabled, shutdown_frozen, stream_generation
            nonlocal allocations_known, broker_responsive_hint
            while not stop_requested.is_set():
                # Poll the sole command reader directly.  Queue.get through
                # asyncio.to_thread consumed one executor thread before a
                # reconcile could even enqueue its actor starts.
                try:
                    command = read_queue_lockfree(self.command_queue, 0)
                except queue.Empty:
                    await asyncio.sleep(0.05)
                    continue
                except Exception as exc:
                    worker_error = str(exc)
                    log.exception("WORKER_COMMAND_QUEUE_ERROR | error=%s", exc)
                    await asyncio.sleep(1.0)
                    continue
                action = str(command.get("action") or "")
                command_received_at = int(time.time() * 1000)
                with heartbeat_signal_lock:
                    command_state["active"] = True
                    command_state["queued_at_ms"] = int(command.get("queuedAtMs") or command_received_at)
                    command_state["wait_ms"] = max(0, command_received_at - command_state["queued_at_ms"])
                try:
                    if action == "reconcile":
                        if shutdown_frozen:
                            raise RuntimeError("worker is permanently frozen")
                        desired = {item.market_id: item for item in command.get("picks") or ()}
                        supplied_snapshot = command.get("startup_account_snapshot")
                        startup_account_snapshot = (
                            supplied_snapshot if isinstance(supplied_snapshot, StartupAccountSnapshot) else None
                        )
                        async with reconcile_lock:
                            desired_picks.clear()
                            desired_picks.update(desired)
                            had_actors = bool(actors)
                            remove = sorted(set(actors) - set(desired))
                            # Retained markets whose class/overrides changed restart
                            # so the new settings take effect: remove_actor cancels
                            # the resting quotes and the fresh actor re-posts once
                            # its book is ready (same cost as a screener eviction).
                            restart = sorted(
                                ticker
                                for ticker in set(desired) & set(actors)
                                if override_key(picks.get(ticker)) != override_key(desired[ticker])
                            )
                            gate = asyncio.Semaphore(reconcile_concurrency)

                            async def _restart_actor(ticker: str) -> None:
                                market_class, settings_overrides = override_key(desired[ticker])
                                log.info(
                                    "RECONCILE_RESTART | ticker=%s class=%s overrides=%d",
                                    ticker, market_class, len(settings_overrides),
                                )
                                await remove_actor(ticker)

                            if remove:
                                await asyncio.gather(*(
                                    reconcile_step(ticker, gate, lambda t=ticker: remove_actor(t))
                                    for ticker in remove
                                ))
                            if restart:
                                await asyncio.gather(*(
                                    reconcile_step(ticker, gate, lambda t=ticker: _restart_actor(t))
                                    for ticker in restart
                                ))
                            # Unsubscribe only markets whose actor is really gone, so
                            # the stream never delivers events nothing can consume.
                            removed = [ticker for ticker in (*remove, *restart) if ticker not in actors]
                            for ticker in set(desired) & set(actors):
                                picks[ticker] = desired[ticker]
                            if had_actors and removed:
                                try:
                                    await direct.update_market_subscriptions(add=[], remove=removed)
                                except RuntimeError:
                                    await direct.close()
                            # Additions (and restarted markets) are started by the
                            # add loop right after this acknowledgement.  Acking
                            # first keeps the controller's reconcile window short:
                            # a slow venue no longer turns a live, heartbeating
                            # worker into a "timed out" one that gets torn down.
                            # Every waiting add becomes due at once: this
                            # reconcile may be the controller's broker-restart
                            # signal, so a market in a long retry backoff must
                            # not stay bookless for the rest of it.
                            queue_pending_adds(pending_adds, desired, actors)
                            startup_error = ""
                            if desired:
                                stamp = int(time.time() * 1000)
                                startup_progress_at_ms = stamp
                            stream_generation += 1
                            if actors:
                                subscriptions_ready.set()
                            else:
                                subscriptions_ready.clear()
                        add_wakeup.set()
                    elif action == "enable_quoting":
                        quoting_enabled = bool(command.get("enabled")) and not shutdown_frozen
                        allocations = command.get("allocations") or {}
                        last_allocations.clear()
                        last_allocations.update(allocations)
                        allocations_known = True
                        for ticker, actor in actors.items():
                            actor.set_allowed_quote_sides(allocations.get(ticker, ()))
                            actor.set_quoting_enabled(quoting_enabled)
                    elif action == "freeze":
                        shutdown_frozen = True
                        quoting_enabled = False
                        for actor in actors.values():
                            actor.set_quoting_enabled(False)
                        results = await asyncio.gather(
                            *(actor.emergency_cancel_all_quotes(reason="worker_freeze") for actor in actors.values()),
                            return_exceptions=True,
                        )
                        failure = next((item for item in results if isinstance(item, BaseException)), None)
                        if failure is not None:
                            raise failure
                    elif action == "stop":
                        hint = command.get("broker_responsive")
                        if hint is not None:
                            broker_responsive_hint = bool(hint)
                        acknowledge(command, action)
                        stop_requested.set()
                        return
                    else:
                        raise ValueError(f"unsupported worker action: {action}")
                    acknowledge(command, action)
                except Exception as exc:
                    acknowledge(command, action, error=exc)
                    worker_error = str(exc)
                    if action == "reconcile":
                        shutdown_frozen = True
                        quoting_enabled = False
                        for actor in actors.values():
                            actor.set_quoting_enabled(False)
                finally:
                    with heartbeat_signal_lock:
                        command_state["active"] = False
                        command_state["queued_at_ms"] = 0

        async def add_loop() -> None:
            while not stop_requested.is_set():
                try:
                    await asyncio.wait_for(add_wakeup.wait(), timeout=ADD_RETRY_POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass
                add_wakeup.clear()
                if stop_requested.is_set() or shutdown_frozen or not pending_adds:
                    continue
                try:
                    async with reconcile_lock:
                        if shutdown_frozen:
                            continue
                        await run_pending_adds()
                except Exception as exc:
                    log.warning("RECONCILE_ADD_LOOP_ERROR | error=%s", exc)

        async def stream_loop() -> None:
            nonlocal stream_generation, stream_connected
            observed_generation = -1
            while not stop_requested.is_set():
                await subscriptions_ready.wait()
                if stop_requested.is_set():
                    return
                observed_generation = stream_generation
                try:
                    async for event in direct.stream_events_many(tuple(actors)):
                        if stop_requested.is_set():
                            return
                        stream_connected = True
                        actor = actors.get(event.market_id)
                        if actor is None:
                            # A busy venue stream can have another event ready
                            # immediately.  Yield even for an event that no
                            # longer has an actor so the heartbeat and command
                            # tasks cannot be starved by a full socket buffer.
                            # A zero-length sleep can keep re-queueing this
                            # callback ahead of timer callbacks when the
                            # websocket is continuously readable.  Use a
                            # small real delay so heartbeat/risk timers always
                            # get a scheduling turn under a busy stream.
                            await asyncio.sleep(0.001)
                            continue
                        actor.handle_event(event)
                        # Only venue market-data events refresh the market's
                        # liveness evidence and append a real book sample;
                        # the worker's own order traffic does neither.
                        sampled = observe_stream_event(
                            event, actor, risk_windows[event.market_id], last_event_ms,
                            now_ms=int(time.time() * 1000),
                        )
                        actor.telemetry_store.record_runtime_event(
                            ticker=event.market_id,
                            source="execution" if isinstance(event, (OrderUpdate, Fill)) else "adaptor",
                            event_type=type(event).__name__,
                            severity="warning" if isinstance(event, StreamReset) else "info",
                        )
                        if isinstance(event, OrderUpdate):
                            self.broker_request_queue.put(BrokerRequest(
                                uuid.uuid4().hex, "", "order_update", {"event": event}, int(time.time() * 1000)
                            ))
                        if sampled and not actor.fleet_risk_generated_at_ms:
                            decision = evaluate_risk(
                                risk_windows[event.market_id], now_ms=int(time.time() * 1000),
                                position_units=actor.net_position_units, has_resting_orders=False,
                                **risk_kwargs,
                            )
                            actor.set_fleet_risk(decision.mode, reason=decision.reason, generated_at_ms=decision.generated_at_ms)
                        if stream_generation != observed_generation:
                            stream_connected = False
                            break
                        # ``async for`` does not necessarily suspend when the
                        # websocket already has buffered data.  Without an
                        # explicit yield a busy Kalshi stream can monopolize
                        # this worker's event loop, preventing heartbeats and
                        # control acknowledgements; the manager then marks a
                        # healthy quoting worker stale and restarts it.
                        await asyncio.sleep(0.001)
                except Exception:
                    stream_connected = False
                    await asyncio.sleep(1.0)

        async def risk_loop() -> None:
            while not stop_requested.is_set():
                now = int(time.time() * 1000)
                ordered = sorted(
                    actors.values(),
                    key=lambda actor: (not bool(actor.net_position_units), not any(x.has_active_resting_order for x in actor.orders.values()), actor.settings.market_ticker),
                )
                for index, actor in enumerate(ordered):
                    ticker = actor.settings.market_ticker
                    # Visits are spread across the whole 60 s cycle; a per-visit
                    # clock keeps staleness and the flatten latch honest for the
                    # markets evaluated late in the cycle.
                    now = int(time.time() * 1000)
                    try:
                        decision = evaluate_market_risk(
                            actor, risk_windows[ticker], ticker=ticker, now_ms=now,
                            last_event_ms=last_event_ms.get(ticker),
                            stream_connected=stream_connected, quiet_after_ms=quiet_after_ms,
                            flatten_latch=flatten_latch, risk_kwargs=risk_kwargs,
                            has_resting_orders=any(item.has_active_resting_order for item in actor.orders.values()),
                        )
                        actor.set_fleet_risk(decision.mode, reason=decision.reason, generated_at_ms=decision.generated_at_ms)
                        actor.telemetry_store.record_runtime_event(
                            ticker=ticker, source="watchdog", event_type="risk_evaluated",
                            severity="warning" if decision.mode != "normal" else "info",
                            payload={"mode": decision.mode, "reason": decision.reason, "confidence": decision.confidence},
                        )
                        if watchdog_exit_allowed(
                            mode=decision.mode,
                            position_units=actor.net_position_units,
                            shutdown_frozen=shutdown_frozen,
                        ):
                            await actor.emergency_cancel_all_quotes(reason="risk_flatten_only")
                            if not shutdown_frozen:
                                await actor.submit_watchdog_exit_order()
                    except Exception:
                        actor.set_fleet_risk("flatten_only" if actor.net_position_units else "reduction_only", reason="risk_evaluator_failed", generated_at_ms=now)
                    if index + 1 < len(ordered):
                        await asyncio.sleep(60.0 / max(1, len(ordered)))
                await asyncio.sleep(max(0.1, 60.0 / max(1, len(ordered))))

        def market_health(actor: MarketActor, now: int, *, rich: bool) -> MarketHealth:
            price_units, price_source, price_at_ms = actor.current_market_price()
            return MarketHealth(
                book_available=actor.book_ready,
                book_age_ms=(now - actor.last_orderbook_event_timestamp_ms)
                if actor.last_orderbook_event_timestamp_ms else None,
                decision_age_ms=(now - actor.last_quote_decision_at_ms)
                if actor.last_quote_decision_at_ms else None,
                risk_mode=("startup" if not getattr(actor, "models_ready", True) else actor.fleet_risk_mode or "startup"),
                risk_age_ms=(now - actor.fleet_risk_generated_at_ms)
                if actor.fleet_risk_generated_at_ms else None,
                resting_order_count=sum(
                    1 for item in actor.orders.values() if item.has_active_resting_order
                ),
                position_units=actor.net_position_units,
                started_at_ms=actor.started_at_ms,
                fill_count=actor.session_fill_count,
                order_activity=(
                    {
                        action: {name: int(value) for name, value in counters.items()}
                        for action, counters in actor.order_activity.items()
                    }
                    if rich else {}
                ),
                pnl=actor.session_pnl_snapshot() if rich else {},
                markouts=actor.session_markout_snapshot(current_ms=now) if rich else {},
                risk_reason=str(getattr(actor, "fleet_risk_reason", "") or ""),
                last_quote_at_ms=actor.last_quote_decision_at_ms,
                last_order_create_at_ms=int(actor.last_order_create_at_ms or 0),
                last_fill_at_ms=actor.last_fill_timestamp_ms,
                price_units=price_units,
                price_source=price_source,
                price_at_ms=price_at_ms,
                error=str(getattr(actor, "model_refresh_error", "") or ""),
            )

        def merge_cached_market_health(
            lightweight: MarketHealth,
            cached_rich: Optional[MarketHealth],
        ) -> MarketHealth:
            """Keep liveness fields current while reusing rich diagnostics."""
            if cached_rich is None:
                return lightweight
            return replace(
                cached_rich,
                book_available=lightweight.book_available,
                book_age_ms=lightweight.book_age_ms,
                decision_age_ms=lightweight.decision_age_ms,
                risk_mode=lightweight.risk_mode,
                risk_age_ms=lightweight.risk_age_ms,
                resting_order_count=lightweight.resting_order_count,
                position_units=lightweight.position_units,
                error=lightweight.error,
                started_at_ms=lightweight.started_at_ms,
                fill_count=lightweight.fill_count,
                risk_reason=lightweight.risk_reason,
                last_quote_at_ms=lightweight.last_quote_at_ms,
                last_order_create_at_ms=lightweight.last_order_create_at_ms,
                last_fill_at_ms=lightweight.last_fill_at_ms,
                price_units=lightweight.price_units,
                price_source=lightweight.price_source,
                price_at_ms=lightweight.price_at_ms,
            )

        def market_diagnostics_revision(actor: MarketActor) -> tuple[Any, ...]:
            """Return a bounded fingerprint for rich-health cache invalidation."""
            ticker_state = getattr(actor, "ticker_state", None)
            markout_counts = tuple(
                (str(key), int((value or {}).get("count", 0) or 0))
                for key, value in getattr(actor, "session_markouts_by_horizon", {}).items()
            )
            order_activity = tuple(
                (
                    str(action),
                    tuple(sorted((str(name), int(value)) for name, value in counters.items())),
                )
                for action, counters in getattr(actor, "order_activity", {}).items()
            )
            return (
                int(getattr(actor, "session_fill_count", 0) or 0),
                int(getattr(actor, "last_fill_timestamp_ms", 0) or 0),
                int(getattr(actor, "last_orderbook_event_timestamp_ms", 0) or 0),
                int(getattr(actor, "last_quote_decision_at_ms", 0) or 0),
                int(getattr(ticker_state, "ts_ms", 0) or 0),
                len(getattr(actor, "session_fill_timestamps_ms", ()) or ()),
                markout_counts,
                order_activity,
            )

        def build_heartbeat(
            now: int,
            health: Mapping[str, MarketHealth],
            *,
            source: str,
            measured_event_loop_lag_ms: Optional[int] = None,
            state: Optional[Mapping[str, Any]] = None,
        ) -> WorkerHeartbeat:
            nonlocal heartbeat_build_ms
            nonlocal heartbeat_sequence, fallback_heartbeat_count, last_fallback_at_ms
            nonlocal event_loop_lag_ms, cpu_percent, last_cpu_process_seconds
            nonlocal last_cpu_sample_monotonic, warning_count, error_count
            nonlocal last_warning_seen, last_error_seen
            build_started = time.perf_counter()
            heartbeat_state_values = dict(state or heartbeat_state_snapshot())
            assigned_tickers = tuple(heartbeat_state_values.get("assigned_tickers") or ())
            pending_tickers = tuple(heartbeat_state_values.get("pending_tickers") or ())
            startup_attempts_snapshot = dict(heartbeat_state_values.get("startup_attempts") or {})
            current_startup_progress_at_ms = int(heartbeat_state_values.get("startup_progress_at_ms") or 0)
            current_startup_error = str(heartbeat_state_values.get("startup_error") or "")
            current_worker_error = str(heartbeat_state_values.get("worker_error") or "")
            with heartbeat_signal_lock:
                heartbeat_sequence += 1
                if source == "fallback":
                    fallback_heartbeat_count += 1
                    last_fallback_at_ms = now
                if source == "rich":
                    current_process_seconds = time.process_time()
                    current_monotonic = time.monotonic()
                    elapsed = max(0.001, current_monotonic - last_cpu_sample_monotonic)
                    cpu_percent = max(
                        0.0,
                        min(10000.0, (current_process_seconds - last_cpu_process_seconds) / elapsed * 100.0),
                    )
                    last_cpu_process_seconds = current_process_seconds
                    last_cpu_sample_monotonic = current_monotonic
                if current_worker_error and current_worker_error != last_error_seen:
                    error_count += 1
                    last_error_seen = current_worker_error
                if current_startup_error and current_startup_error != last_warning_seen:
                    warning_count += 1
                    last_warning_seen = current_startup_error
                command_active = bool(command_state["active"])
                command_age_ms = (
                    max(0, now - int(command_state["queued_at_ms"]))
                    if command_active and command_state["queued_at_ms"] else 0
                )
                command_wait_ms = int(command_state["wait_ms"])
                current_event_loop_lag = resolve_event_loop_lag(
                    measured_event_loop_lag_ms,
                    event_loop_lag_ms,
                )
                if source == "rich":
                    event_loop_lag_ms = current_event_loop_lag
                current_heartbeat_sequence = int(heartbeat_sequence)
                current_cpu_percent = float(cpu_percent)
                current_fallback_count = int(fallback_heartbeat_count)
                current_last_fallback_at_ms = int(last_fallback_at_ms)
                current_warning_count = int(warning_count)
                current_error_count = int(error_count)
                current_last_warning = str(last_warning_seen or "")
                current_last_error = str(last_error_seen or "")
                current_recovery_count = int(recovery_count)
                current_last_recovery_at_ms = int(last_recovery_at_ms)
                current_last_recovery_reason = str(last_recovery_reason or "")
            (
                _rich_cache,
                api_activity,
                api_errors,
                current_diagnostics_ms,
                current_telemetry_flush_ms,
                current_rich_at_ms,
                current_publish_wait_ms,
            ) = cached_diagnostics_snapshot(include_health=False)
            with diagnostics_state_lock:
                current_build_ms = int(heartbeat_build_ms)
                current_coalesced_count = int(heartbeat_coalesced_count)
            heartbeat = WorkerHeartbeat(
                    self.worker_id,
                    assigned_tickers,
                    health,
                    _rss_bytes(),
                    len(pending_tickers),
                    max((item.book_age_ms or 0 for item in health.values()), default=0),
                    now,
                    self.venue,
                    api_activity,
                    pending_tickers,
                    startup_attempts_snapshot,
                    current_startup_progress_at_ms,
                    current_startup_error,
                    current_worker_error,
                    api_errors=api_errors,
                    heartbeat_sequence=current_heartbeat_sequence,
                    heartbeat_source=source,
                    published_at_ms=now,
                    event_loop_lag_ms=current_event_loop_lag,
                    cpu_percent=current_cpu_percent,
                    thread_count=threading.active_count(),
                    command_queue_depth=int(command_active),
                    command_oldest_age_ms=command_age_ms,
                    command_wait_ms=command_wait_ms,
                    fallback_heartbeat_count=current_fallback_count,
                    last_fallback_at_ms=current_last_fallback_at_ms,
                    warning_count=current_warning_count,
                    error_count=current_error_count,
                    last_warning=current_last_warning,
                    last_error=current_last_error,
                    recovery_count=current_recovery_count,
                    last_recovery_at_ms=current_last_recovery_at_ms,
                    last_recovery_reason=current_last_recovery_reason,
                    heartbeat_build_ms=current_build_ms,
                    diagnostics_duration_ms=current_diagnostics_ms,
                    telemetry_flush_ms=current_telemetry_flush_ms,
                    heartbeat_publish_wait_ms=current_publish_wait_ms,
                    heartbeat_coalesced_count=current_coalesced_count,
                    last_rich_diagnostics_at_ms=current_rich_at_ms,
                )
            with diagnostics_state_lock:
                heartbeat_build_ms = max(
                    0, int((time.perf_counter() - build_started) * 1000)
                )
                current_build_ms = int(heartbeat_build_ms)
            return replace(heartbeat, heartbeat_build_ms=current_build_ms)

        def lightweight_heartbeat(now: int) -> WorkerHeartbeat:
            """Build a watchdog-only heartbeat without touching SQLite."""
            with actors_lock:
                actor_items = list(actors.items())
            rich_cache, *_ = cached_diagnostics_snapshot()
            health = {
                ticker: merge_cached_market_health(
                    market_health(actor, now, rich=False), rich_cache.get(ticker)
                )
                for ticker, actor in actor_items
            }
            return build_heartbeat(
                now,
                health,
                source="fallback",
                state=heartbeat_state_snapshot(),
            )

        def heartbeat_fallback_thread() -> None:
            nonlocal heartbeat_publish_wait_ms
            # Let the async task publish its initial heartbeat first.  If the
            # event loop later becomes busy, this thread publishes before the
            # manager's stale threshold; a third-of-budget wake interval gives
            # it a fallback sample before it can recycle a healthy worker.
            stale_budget_seconds = float(fleet.get("workerStaleSeconds", 5.0))
            interval = max(0.25, min(heartbeat_seconds, stale_budget_seconds / 3.0))
            while not heartbeat_thread_stop.is_set():
                try:
                    heartbeat = heartbeat_handoff.get(timeout=interval)
                    fallback = False
                except queue.Empty:
                    now = int(time.time() * 1000)
                    with heartbeat_signal_lock:
                        last_async_ms = int(heartbeat_signal["last_async_ms"])
                    if last_async_ms and now - last_async_ms <= int(stale_budget_seconds * 500):
                        continue
                    try:
                        heartbeat = lightweight_heartbeat(now)
                        fallback = True
                    except Exception:
                        log.exception("WORKER_HEARTBEAT_FALLBACK_ERROR")
                        continue
                try:
                    publish_started = time.perf_counter()
                    self.heartbeat_queue.put(heartbeat)
                    publish_wait_ms = max(0, int((time.perf_counter() - publish_started) * 1000))
                    with diagnostics_state_lock:
                        heartbeat_publish_wait_ms = publish_wait_ms
                    if fallback:
                        log.info("WORKER_HEARTBEAT_FALLBACK_SENT | actors=%d", len(heartbeat.market_health))
                except Exception:
                    log.exception("WORKER_HEARTBEAT_FALLBACK_ERROR")

        async def heartbeat_loop() -> None:
            nonlocal worker_error
            heartbeat_count = 0
            last_heartbeat_log_ms = 0
            last_heartbeat_monotonic = time.monotonic()
            while not stop_requested.is_set():
                now = int(time.time() * 1000)
                try:
                    current_monotonic = time.monotonic()
                    event_loop_delay = max(
                        0.0,
                        (current_monotonic - last_heartbeat_monotonic - heartbeat_seconds) * 1000.0,
                    )
                    last_heartbeat_monotonic = current_monotonic
                    with actors_lock:
                        actor_items = list(actors.items())
                    rich_cache, *_ = cached_diagnostics_snapshot()
                    health = {
                        ticker: merge_cached_market_health(
                            market_health(actor, now, rich=False), rich_cache.get(ticker)
                        )
                        for ticker, actor in actor_items
                    }
                    # Markets still waiting for their actor stay visible as
                    # ``startup`` with the retry state as the reason.
                    for ticker, item in list(pending_adds.items()):
                        if ticker in health:
                            continue
                        if item.attempts:
                            reason = (
                                f"actor start retry {item.attempts} in "
                                f"{max(0, item.retry_at_ms - now) // 1000}s: {item.error}"
                            )
                        else:
                            reason = "actor start pending"
                        health[ticker] = MarketHealth(
                            book_available=False, book_age_ms=None, decision_age_ms=None,
                            risk_mode="startup", risk_age_ms=None, error=reason,
                        )
                    update_heartbeat_state()
                    heartbeat = build_heartbeat(
                        now,
                        health,
                        source="rich",
                        measured_event_loop_lag_ms=int(event_loop_delay),
                        state=heartbeat_state_snapshot(),
                    )
                    enqueue_heartbeat(heartbeat)
                    with heartbeat_signal_lock:
                        heartbeat_signal["last_async_ms"] = now
                    heartbeat_count += 1
                    if heartbeat_count == 1 or now - last_heartbeat_log_ms >= 30_000:
                        log.info(
                            "WORKER_HEARTBEAT_SENT | count=%d actors=%d pending=%d",
                            heartbeat_count, len(actors), len(pending_adds),
                        )
                        last_heartbeat_log_ms = now
                except Exception as exc:
                    # A telemetry/metrics failure must not silently terminate
                    # the heartbeat task.  Keep the manager informed with a
                    # minimal health payload so a quoting worker is not
                    # repeatedly recycled as stale.
                    log.exception("WORKER_HEARTBEAT_ERROR | error=%s", exc)
                    worker_error = str(exc)
                    with actors_lock:
                        actor_items = list(actors.items())
                    fallback_health = {
                        ticker: market_health(actor, now, rich=False)
                        for ticker, actor in actor_items
                    }
                    update_heartbeat_state()
                    try:
                        enqueue_heartbeat(
                            build_heartbeat(
                                now,
                                fallback_health,
                                source="fallback",
                                state=heartbeat_state_snapshot(),
                            )
                        )
                        with heartbeat_signal_lock:
                            heartbeat_signal["last_async_ms"] = now
                        heartbeat_count += 1
                        if heartbeat_count == 1 or now - last_heartbeat_log_ms >= 30_000:
                            log.info(
                                "WORKER_HEARTBEAT_SENT | count=%d actors=%d pending=%d fallback=true",
                                heartbeat_count, len(actors), len(pending_adds),
                            )
                            last_heartbeat_log_ms = now
                    except Exception:
                        log.exception("WORKER_HEARTBEAT_FALLBACK_ERROR")
                await asyncio.sleep(heartbeat_seconds)

        async def diagnostics_loop() -> None:
            """Refresh rich market diagnostics without blocking liveness."""
            nonlocal cached_rich_health, cached_diagnostic_revisions, cached_diagnostic_at_ms
            nonlocal cached_api_activity, cached_api_errors
            nonlocal diagnostics_duration_ms, last_rich_diagnostics_at_ms, worker_error
            interval_seconds = 5.0
            while not stop_requested.is_set():
                started = time.perf_counter()
                now = int(time.time() * 1000)
                with actors_lock:
                    actor_items = list(actors.items())
                refreshed: dict[str, MarketHealth] = {}
                refreshed_revisions: dict[str, tuple[Any, ...]] = {}
                refreshed_at_ms: dict[str, int] = {}
                with diagnostics_state_lock:
                    previous_health = dict(cached_rich_health)
                    previous_revisions = dict(cached_diagnostic_revisions)
                    previous_at_ms = dict(cached_diagnostic_at_ms)
                for index, (ticker, actor) in enumerate(actor_items):
                    revision = market_diagnostics_revision(actor)
                    if (
                        ticker in previous_health
                        and previous_revisions.get(ticker) == revision
                        and now - int(previous_at_ms.get(ticker, 0) or 0) < int(interval_seconds * 1000)
                    ):
                        refreshed[ticker] = previous_health[ticker]
                        refreshed_revisions[ticker] = revision
                        refreshed_at_ms[ticker] = int(previous_at_ms[ticker])
                        continue
                    try:
                        refreshed[ticker] = market_health(actor, now, rich=True)
                        refreshed_revisions[ticker] = revision
                        refreshed_at_ms[ticker] = now
                    except Exception as exc:
                        worker_error = f"diagnostics: {exc}"
                        log.exception("WORKER_DIAGNOSTICS_ERROR | ticker=%s error=%s", ticker, exc)
                        if ticker in previous_health:
                            refreshed[ticker] = previous_health[ticker]
                            refreshed_revisions[ticker] = previous_revisions.get(ticker, revision)
                            refreshed_at_ms[ticker] = int(previous_at_ms.get(ticker, now))
                    if index % 4 == 3:
                        await asyncio.sleep(0)
                try:
                    activity = await asyncio.to_thread(direct.activity_snapshot)
                except Exception as exc:
                    worker_error = f"diagnostics activity: {exc}"
                    log.exception("WORKER_DIAGNOSTICS_ACTIVITY_ERROR | error=%s", exc)
                    activity = None
                try:
                    api_errors = await asyncio.to_thread(client.api_errors_snapshot)
                except Exception as exc:
                    worker_error = f"diagnostics api errors: {exc}"
                    log.exception("WORKER_DIAGNOSTICS_API_ERROR | error=%s", exc)
                    api_errors = None
                duration_ms = max(0, int((time.perf_counter() - started) * 1000))
                with diagnostics_state_lock:
                    cached_rich_health = refreshed
                    cached_diagnostic_revisions = refreshed_revisions
                    cached_diagnostic_at_ms = refreshed_at_ms
                    if activity is not None:
                        cached_api_activity = dict(activity)
                    if api_errors is not None:
                        cached_api_errors = dict(api_errors)
                    diagnostics_duration_ms = duration_ms
                    last_rich_diagnostics_at_ms = now
                update_heartbeat_state()
                await asyncio.sleep(interval_seconds)

        async def telemetry_persistence_loop() -> None:
            """Flush shard SQLite outside the liveness heartbeat task."""
            nonlocal last_retention_ms, telemetry_flush_ms, worker_error
            while not stop_requested.is_set():
                await asyncio.sleep(5.0)
                with actors_lock:
                    actor_items = list(actors.values())
                if not actor_items:
                    continue
                telemetry = actor_items[0].telemetry_store
                started = time.perf_counter()
                try:
                    await asyncio.to_thread(telemetry.flush)
                    now = int(time.time() * 1000)
                    if now - last_retention_ms >= 86_400_000:
                        await asyncio.to_thread(
                            telemetry.apply_retention,
                            now_timestamp_ms=now,
                            raw_days=7,
                        )
                        last_retention_ms = now
                    flush_duration_ms = max(0, int((time.perf_counter() - started) * 1000))
                    with diagnostics_state_lock:
                        telemetry_flush_ms = flush_duration_ms
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    worker_error = f"telemetry: {exc}"
                    log.exception("WORKER_TELEMETRY_ERROR | error=%s", exc)

        async def supervised(name: str, factory) -> None:
            nonlocal worker_error
            while not stop_requested.is_set():
                try:
                    await factory()
                    if not stop_requested.is_set():
                        raise RuntimeError(f"{name} task exited unexpectedly")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    worker_error = f"{name}: {exc}"
                    log.exception("WORKER_TASK_ERROR | task=%s error=%s", name, exc)
                    await asyncio.sleep(1.0)

        tasks = [
            asyncio.create_task(supervised("command_loop", command_loop)),
            asyncio.create_task(supervised("add_loop", add_loop)),
            asyncio.create_task(supervised("stream_loop", stream_loop)),
            asyncio.create_task(supervised("risk_loop", risk_loop)),
            asyncio.create_task(supervised("diagnostics_loop", diagnostics_loop)),
            asyncio.create_task(supervised("telemetry_persistence_loop", telemetry_persistence_loop)),
            asyncio.create_task(supervised("heartbeat_loop", heartbeat_loop)),
        ]
        heartbeat_thread = threading.Thread(
            target=heartbeat_fallback_thread,
            name=f"{self.worker_id}-heartbeat",
            daemon=True,
        )
        try:
            heartbeat_thread.start()
        except RuntimeError as exc:
            worker_error = str(exc)
            log.exception("WORKER_RESOURCE_ERROR | component=heartbeat_fallback error=%s", exc)
        try:
            await stop_requested.wait()
        finally:
            heartbeat_thread_stop.set()
            for actor in actors.values():
                actor.set_quoting_enabled(False)
            # A dead or wedged broker must not hold the shutdown: bound every
            # remaining broker wait and let reads/cancels go direct so each
            # actor still verifies its orders are absent at the venue.
            try:
                responsive = await asyncio.to_thread(
                    client.prepare_for_shutdown,
                    broker_responsive=broker_responsive_hint,
                    probe_timeout=SHUTDOWN_BROKER_PROBE_SECONDS,
                    rpc_timeout=SHUTDOWN_RPC_TIMEOUT_SECONDS,
                )
                log.info("WORKER_SHUTDOWN_BROKER_PROBE | responsive=%s", responsive)
            except Exception as exc:
                log.warning("WORKER_SHUTDOWN_BROKER_PROBE | error=%s", exc)
            await asyncio.gather(*(actor.stop(verify_orders=True) for actor in actors.values()), return_exceptions=True)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            with actors_lock:
                shutdown_actors = list(actors.values())
            if shutdown_actors:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(shutdown_actors[0].telemetry_store.flush),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    log.warning("WORKER_TELEMETRY_FLUSH_TIMEOUT | phase=shutdown")
                except Exception as exc:
                    log.warning("WORKER_TELEMETRY_FLUSH_ERROR | phase=shutdown error=%s", exc)
            try:
                await direct.close()
                client.close_dispatcher()
            finally:
                io_executor.shutdown(wait=False, cancel_futures=True)
