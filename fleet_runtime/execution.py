"""Dedicated execution-process RPC used by every market worker."""

from __future__ import annotations

import dataclasses
import heapq
import logging
import multiprocessing as mp
import queue
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from multiprocessing.reduction import ForkingPickler
from typing import Any, Callable, Iterable, Mapping, Optional

try:  # The venue client is built on requests; its transport errors are ours to classify.
    import requests as _requests
except Exception:  # pragma: no cover - requests is a hard dependency of the adaptor
    _requests = None  # type: ignore[assignment]

from clients.base_client import BaseClient
from clients.factory import build_client, build_client_config
from clients.models import (
    AmendTargetUnavailableError,
    ClientError,
    InsufficientBalanceError,
    OrderNotFoundError,
    PostOnlyCrossError,
    RateLimitError,
)
from core.fleet_models import IntentUrgency, QuoteIntent
from .broker import TokenBucket


class ShardExposureCapError(InsufficientBalanceError):
    """The broker refused new exposure: the shard's live exposure would pass its allocatable cash.

    A subclass of ``InsufficientBalanceError`` so every worker build maps it
    to the bot's per-market create/amend cooldown (the response is serialised
    under the base name; see ``ExecutionBrokerProcess._respond``).
    """

    def __init__(self, message: str, *, detail: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message)
        self.detail = dict(detail or {})


_ERROR_TYPES = {
    item.__name__: item
    for item in (
        ClientError,
        RateLimitError,
        OrderNotFoundError,
        PostOnlyCrossError,
        AmendTargetUnavailableError,
        InsufficientBalanceError,
        ShardExposureCapError,
        # A broker-side execution timeout surfaces in the worker as the same
        # exception class as an RPC wait timeout, so callers keep one path.
        TimeoutError,
    )
}
BOT_ORDER_PREFIXES = ("mm:", "tob:", "wd:")
# The execution broker runs in its own process; WARNING lines reach the
# console even when nothing configured a handler there (logging's last resort).
LOGGER = logging.getLogger("kalshi_top_of_book_bot.fleet_execution")

# Default wait a worker spends on one broker round-trip.  The broker drops
# non-cancel requests older than this: their waiter is gone, so executing them
# would only burn venue capacity on answers nobody reads (the congestion
# collapse seen when the venue slows down under a reconcile burst).
DEFAULT_RPC_TIMEOUT_SECONDS = 30.0
# Operations whose venue effect is still wanted even after the requester gave
# up waiting: cancellations must land, control ops are cheap and stateful.
_KEEP_WHEN_STALE = {
    "cancel_order", "cancel_market", "cancel_all", "verify_clear", "stop",
    "order_update", "quiesce_all", "quiesce_channel", "resume_channel",
    "shard_exposure_limits",
}
# Account-cleanup operations retry with backoff for tens of seconds.  They run
# off the request loop so one slow cleanup cannot starve every other channel.
_CLEANUP_OPERATIONS = {"cancel_market", "cancel_all", "verify_clear"}
# Broker-local operations: they never reach the venue, so answering them says
# nothing about venue health.  A websocket order stream keeps ``order_update``
# flowing through a REST stall; counting those as successes hid the stall.
_NO_VENUE_OPERATIONS = {
    "ping", "stop", "order_update", "quiesce_all", "quiesce_channel", "resume_channel",
    "shard_exposure_limits",
}
# HTTP gateway statuses that mean the venue front door failed the call, not
# that the call was rejected.
_GATEWAY_FAILURE_STATUSES = {502, 503, 504}


def _transport_error_types() -> tuple[type, ...]:
    types: tuple[type, ...] = (TimeoutError, ConnectionError, FutureTimeoutError)
    if _requests is not None:
        types += (_requests.exceptions.Timeout, _requests.exceptions.ConnectionError)
    return types


_TRANSPORT_ERROR_TYPES = _transport_error_types()


def is_venue_transport_failure(exc: BaseException) -> bool:
    """True when ``exc`` (or its cause chain) says the venue could not be reached.

    The HTTP layer bounds each call at 10 s connect / 15 s read, so during a
    venue outage every broker call fails here, well before the broker's own
    execution deadline.  Those failures must feed the stall signal exactly
    like a deadline overrun, or the most common outage shape never trips it.
    """
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    depth = 0
    while current is not None and id(current) not in seen and depth < 8:
        seen.add(id(current))
        depth += 1
        if isinstance(current, _TRANSPORT_ERROR_TYPES):
            return True
        status = getattr(current, "status_code", None)
        if isinstance(status, int) and status in _GATEWAY_FAILURE_STATUSES:
            return True
        current = current.__cause__ or current.__context__
    return False


def _urgency(operation: str, payload: Mapping[str, Any]) -> IntentUrgency:
    intent = payload.get("intent")
    if isinstance(intent, QuoteIntent):
        return intent.urgency
    if operation in {
        "cancel_all", "cancel_market", "verify_clear",
        "quiesce_all", "quiesce_channel", "resume_channel",
        # The shutdown liveness probe must not queue behind a quote burst: a
        # healthy broker that answers late looks dead and every actor goes
        # direct to the venue for nothing.
        "ping",
        # New exposure limits must land before the quote burst they gate.
        "shard_exposure_limits",
    }:
        return IntentUrgency.EMERGENCY_CANCEL
    if operation == "cancel_order":
        return IntentUrgency.EMERGENCY_CANCEL
    request = payload.get("request")
    if bool(getattr(request, "reduce_only", False)):
        return IntentUrgency.RISK_REDUCE
    if operation in {"amend_order", "decrease_order_to"}:
        return IntentUrgency.NORMAL_CANCEL_OR_AMEND
    return IntentUrgency.NORMAL


@dataclass(frozen=True)
class BrokerRequest:
    request_id: str
    response_channel: str
    operation: str
    payload: Mapping[str, Any]
    submitted_at_ms: int


class BotOrderCleanupError(RuntimeError):
    """Raised after bot-owned resting orders survive every cleanup attempt."""

    def __init__(
        self,
        message: str,
        *,
        remaining_order_ids: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.remaining_order_ids = remaining_order_ids


class BrokerRequestDecodeError(RuntimeError):
    """A queue item could not be unpickled (torn mid-write by a dying producer)."""


class BrokerQueueUnreadable(RuntimeError):
    """The request queue's pipe keeps failing: the broker must exit and be replaced."""


def read_queue_lockfree(source: Any, timeout: float) -> Any:
    """Pop one item from ``source`` without taking its reader lock.

    ``multiprocessing.Queue.get`` holds ``_rlock`` while it polls the pipe; a
    process terminated inside ``get`` (a stalled broker restart, a stale
    worker recovery) dies holding it, and the replacement process that
    inherits the queue would block on the lock forever.  Every queue in the
    fleet has exactly one consumer, so the lock only exists for multi-reader
    safety nobody needs: read the pipe directly (poll -> recv_bytes ->
    release the size semaphore -> unpickle, i.e. ``Queue.get`` minus the
    lock).  Non-multiprocessing queues (tests) fall back to ``get``.

    Raises ``queue.Empty`` when nothing arrived, ``BrokerRequestDecodeError``
    when the bytes do not unpickle, and lets pipe errors propagate.
    """
    reader = getattr(source, "_reader", None)
    if reader is None:
        if timeout > 0:
            return source.get(timeout=timeout)
        return source.get_nowait()
    if not reader.poll(timeout if timeout > 0 else 0):
        raise queue.Empty
    data = reader.recv_bytes()
    semaphore = getattr(source, "_sem", None)
    if semaphore is not None:
        try:
            semaphore.release()
        except Exception:
            pass
    try:
        return ForkingPickler.loads(data)
    except Exception as exc:
        raise BrokerRequestDecodeError(f"undecodable queue item: {exc}") from exc


def release_queue_reader_lock(source: Any) -> bool:
    """Free ``source``'s reader lock if a terminated consumer took it down.

    Only ever called for a queue whose sole consumer is known to be dead (and
    whose replacement reads lock-free anyway), so releasing can only free an
    orphaned lock.  Returns whether a lock was released; ``ValueError`` (not
    held, the normal case) and non-multiprocessing queues yield False.
    """
    lock = getattr(source, "_rlock", None)
    if lock is None:
        return False
    try:
        lock.release()
        return True
    except Exception:
        return False


def is_bot_owned_order(client_order_id: str) -> bool:
    return bool(client_order_id) and client_order_id.startswith(BOT_ORDER_PREFIXES)


def cancel_and_verify_owned_orders(
    client: Any,
    market_id: str = "",
    *,
    attempts: int = 8,
    delay_seconds: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
    deadline: Optional[float] = None,
) -> int:
    """Cancel bot-tagged resting orders, tolerating venue read-after-write lag.

    ``deadline`` (a ``time.monotonic`` instant) bounds the venue traffic once
    it passes: no further attempt starts, the backoff sleeps are clipped, and
    at most ONE more venue read happens - the authoritative verification read
    after a cancel pass, which is skipped when the pass that preceded it did
    not get an answer from the venue at all (a second identical call cannot
    verify anything either).  Past the deadline a silent venue therefore costs
    one read (the HTTP layer's 15 s read timeout), so a shutdown against an
    unreachable venue reports "not verified" inside its budget instead of
    being hard-killed mid-verification.  Cancels of orders the venue did
    report are always issued, deadline or not: leaving a known resting order
    behind is never the cheaper outcome.
    """

    from clients.models import AccountOrderQuery

    def expired() -> bool:
        return deadline is not None and time.monotonic() >= deadline

    canceled_ids: set[str] = set()
    remaining_ids: tuple[str, ...] = ()
    last_error: Optional[BaseException] = None
    # Whether the most recent pass ended in a venue error (nothing to verify).
    last_pass_unanswered = False
    total_attempts = max(1, int(attempts))
    for attempt in range(total_attempts):
        if attempt and expired():
            break
        try:
            orders = client.list_account_orders(
                AccountOrderQuery(status="resting", market_id=market_id, page_size=1_000)
            )
            owned = [
                order for order in orders
                if order.order_id and is_bot_owned_order(order.client_order_id)
            ]
            remaining_ids = tuple(sorted({str(order.order_id) for order in owned}))
            if not owned:
                return len(canceled_ids)
            for order in owned:
                order_id = str(order.order_id)
                try:
                    client.cancel_order(order_id=order_id)
                except OrderNotFoundError:
                    pass
                canceled_ids.add(order_id)
            last_error = None
            last_pass_unanswered = False
        except Exception as exc:
            last_error = exc
            last_pass_unanswered = True
        if attempt + 1 < total_attempts:
            pause = min(4.0, max(0.0, float(delay_seconds)) * (2 ** attempt))
            if deadline is not None:
                pause = min(pause, max(0.0, deadline - time.monotonic()))
            if pause > 0:
                sleep(pause)

    if last_pass_unanswered and expired():
        scope = market_id or "the account"
        ids = f"; remaining order ids: {', '.join(remaining_ids)}" if remaining_ids else ""
        raise BotOrderCleanupError(
            f"could not verify cancellation of all bot-owned orders for {scope}{ids}"
            f"; venue did not answer before the deadline; last error: {last_error}",
            remaining_order_ids=remaining_ids,
        ) from last_error

    # The last cancellation attempt must still be followed by an authoritative
    # read; otherwise a successful final cancel would be reported as failure.
    try:
        orders = client.list_account_orders(
            AccountOrderQuery(status="resting", market_id=market_id, page_size=1_000)
        )
        remaining_ids = tuple(sorted({
            str(order.order_id)
            for order in orders
            if order.order_id and is_bot_owned_order(order.client_order_id)
        }))
        if not remaining_ids:
            return len(canceled_ids)
        last_error = None
    except Exception as exc:
        last_error = exc

    scope = market_id or "the account"
    detail = f"; last error: {last_error}" if last_error is not None else ""
    ids = f"; remaining order ids: {', '.join(remaining_ids)}" if remaining_ids else ""
    raise BotOrderCleanupError(
        f"could not verify cancellation of all bot-owned orders for {scope}{ids}{detail}",
        remaining_order_ids=remaining_ids,
    ) from last_error


class OrderRegistry:
    """Latest bot order per (ticker, side), plus orphans, behind one lock.

    Venue calls run on executor threads while the request loop and later
    calls read the registry, so every access is serialised here.  An
    *orphan* is an order created by a request whose requester had already been
    told ``TimeoutError``: nobody tracks it, so the next write on the same
    (ticker, side) cancels it before placing anything new.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._orders: dict[tuple[str, str], Any] = {}
        self._stamps: dict[tuple[str, str], int] = {}
        self._orphans: dict[tuple[str, str], list[str]] = {}

    @staticmethod
    def _order_id(order: Any) -> str:
        return str(getattr(order, "order_id", "") or "")

    def get(self, key: tuple[str, str]) -> Any:
        with self._lock:
            return self._orders.get(key)

    def set(self, key: tuple[str, str], order: Any, *, stamp: Optional[int] = None) -> bool:
        """Register ``order`` for ``key``.

        With a ``stamp`` (dispatch order of the request that produced the
        order) an older result never overwrites a newer registration: it is
        an untracked order and becomes an orphan instead.  Returns whether the
        order was registered as the live one.
        """
        with self._lock:
            if stamp is not None and key in self._stamps and stamp < self._stamps[key]:
                order_id = self._order_id(order)
                if order_id and order_id != self._order_id(self._orders.get(key)):
                    self._orphans.setdefault(key, [])
                    if order_id not in self._orphans[key]:
                        self._orphans[key].append(order_id)
                return False
            self._orders[key] = order
            if stamp is not None:
                self._stamps[key] = stamp
            order_id = self._order_id(order)
            if order_id and key in self._orphans:
                self._orphans[key] = [item for item in self._orphans[key] if item != order_id]
                if not self._orphans[key]:
                    self._orphans.pop(key, None)
            return True

    def pop(self, key: tuple[str, str]) -> Any:
        with self._lock:
            self._stamps.pop(key, None)
            return self._orders.pop(key, None)

    def add_orphan(self, key: tuple[str, str], order_id: str) -> None:
        if not order_id:
            return
        with self._lock:
            if order_id == self._order_id(self._orders.get(key)):
                return
            bucket = self._orphans.setdefault(key, [])
            if order_id not in bucket:
                bucket.append(order_id)

    def is_orphan(self, key: tuple[str, str], order_id: str) -> bool:
        with self._lock:
            return order_id in self._orphans.get(key, ())

    def take_orphans(self, key: tuple[str, str]) -> list[str]:
        with self._lock:
            return list(self._orphans.pop(key, ()))

    def orphans(self, key: tuple[str, str]) -> list[str]:
        with self._lock:
            return list(self._orphans.get(key, ()))

    def forget_order(self, order_id: str) -> None:
        """Drop ``order_id`` wherever it is registered (cancelled/expired)."""
        if not order_id:
            return
        with self._lock:
            for key in [key for key, order in self._orders.items() if self._order_id(order) == order_id]:
                self._orders.pop(key, None)
                self._stamps.pop(key, None)
            for key in list(self._orphans):
                self._orphans[key] = [item for item in self._orphans[key] if item != order_id]
                if not self._orphans[key]:
                    self._orphans.pop(key, None)

    def replace(self, orders: Mapping[tuple[str, str], Any]) -> None:
        with self._lock:
            self._orders = dict(orders)
            self._stamps = {}
            self._orphans = {}

    def snapshot(self) -> dict[tuple[str, str], Any]:
        with self._lock:
            return dict(self._orders)


def order_notional_units(order: Any) -> int:
    """Cash a resting order locks: price x remaining count, in cash units.

    Prices are 1/10000 dollar per contract and counts are contracts x 100, so
    ``price x count // 100`` is the collateral in cash units (the same
    arithmetic the controller's admission uses for resting orders).
    """
    price = getattr(order, "price_units", None)
    remaining = getattr(order, "remaining_count_units", None)
    if price is None or remaining is None:
        return 0
    return max(0, int(price)) * max(0, int(remaining)) // 100


def _signed_direction(side: str, action: str = "buy") -> int:
    """+1 when a fill on ``(side, action)`` raises the signed YES position, -1 when it lowers it."""
    direction = 1 if str(side or "") == "yes" else -1
    return -direction if str(action or "buy") == "sell" else direction


def order_reduces_position(signed_units: int, side: str, action: str = "buy") -> bool:
    """Whether an order on ``side`` reduces the signed position ``signed_units``.

    The bot's own test (``top_of_book_bot.side_reduces_inventory_risk``: long
    YES -> a NO buy reduces, short -> a YES buy reduces) generalised to sells:
    the first contract filled moves ``|position|`` down.
    """
    return int(signed_units or 0) * _signed_direction(side, action) < 0


@dataclass
class _TrackedOrder:
    """What the ledger remembers about an order it saw placed or filled."""

    ticker: str
    side: str
    action: str
    price_units: int
    # Contracts (x100) of this order that only offset the position it was
    # placed against: fills up to here add no exposure, fills beyond do.
    exempt_units: int
    fill_count_units: int = 0
    reduce_only: bool = False
    done: bool = False


@dataclass(frozen=True)
class OrderAdmission:
    """The ledger's answer to a create/amend: what may go to the venue.

    ``token`` is the reservation to ``release`` once the order is registered
    (None when nothing was reserved).  ``count_units`` is the size admitted;
    it is below ``requested_units`` when the broker trimmed the order to the
    part that reduces the ticker's position (``reason``: ``"shard_cap"`` -
    the remainder did not fit the shard's live cap; ``"carryover"`` - a
    restart carryover may never open the opposite side).  ``carryover`` is
    True for every create on a carryover ticker: the broker stamps those
    venue reduce-only so the venue cancels anything the ledger did not see.
    """

    token: Optional[str] = None
    count_units: int = 0
    requested_units: int = 0
    reason: str = ""
    carryover: bool = False
    detail: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def trimmed(self) -> bool:
        return self.count_units < self.requested_units


class ShardExposureLedger:
    """Per-exchange-shard live exposure the broker enforces between allocations.

    The capital allocator only runs at admission time (a screener refresh or
    its retry); between passes every bot keeps quoting, and with
    ``fleetRuntime.allocationOversubscription`` above 1.0 the committed side
    budgets exceed the shard's cash by design.  This ledger turns the live
    cap - allocatable shard cash, reserve kept, never multiplied - into a
    hard limit at the one place every order passes through: a NEW
    exposure-increasing create, or an amend that raises resting notional, is
    refused with ``ShardExposureCapError`` when

        positions + fills since the position baseline + resting notional
        + reservations in flight + this order's notional
            > allocatable cash of the order's shard.

    Invariant (monotone, snapshot-safe): the ledger's live value for a
    ticker never goes DOWN because of a position snapshot that is older
    than a fill the ledger has already applied.  Exposure is tracked
    continuously from the broker's own event stream - the create/amend
    result (contracts filled at placement count as fills), ``order_update``
    fills, cancels/expiries through the order registry - and a position
    snapshot (the broker's own ``admission_snapshot`` read, or the
    controller's ``shard_exposure_limits`` copy of it, stamped
    ``positions_at_ms``) re-baselines a ticker ONLY when the snapshot was
    taken at or after the last fill the ledger applied for that ticker;
    otherwise the ticker keeps its running value (baseline raised to
    ``max(snapshot, baseline)``) and its fill accumulator.  Snapshots older
    than the ticker's accepted baseline are ignored.  Reducing fills never
    release position exposure (the venue's next accepted snapshot does):
    the ledger only ever over-counts, which can only refuse more.

    Reducing work is never blocked: cancels, decreases, amend-downs and
    venue reduce-only orders pass, and a create/amend whose side reduces
    the ticker's signed position (the bot's ``side_reduces_inventory_risk``
    test) is exempt for the size that merely offsets the position; only the
    remainder is subject to the cap.  When that remainder does not fit, the
    order is NOT refused: ``reserve_order`` admits it trimmed to the exempt
    size (the broker shrinks the request before the venue sees it and logs
    ``SHARD_CAP_TRIMMED``), so a shard at its cap can always be reduced.  A
    refusal - ``ShardExposureCapError``, which the bot maps to its 30 s
    create/amend cooldown - is reserved for orders with no reducing part.
    The signed position per ticker comes from the snapshots
    (``position_units``) plus every fill applied since.

    Sides: the adaptor reports every order and order update on its OUTCOME
    side (a sell of YES comes back as side ``"no"``, see
    ``adaptors.kalshi._outcome_side``), i.e. as the buy-equivalent, so the
    ledger applies the raw request ``action`` only when it has to fall back
    to the request's side.

    Restart carryover tickers (``reduce_only_tickers``: the controller
    granted them only the side that reduces an exchange position) refuse
    any create that does not reduce the position - so once the position is
    flat the bot stops quoting at the broker, whatever the controller last
    told the worker - and a create that does reduce it is capped at the
    position's size and stamped venue reduce-only, so a quote larger than
    the position (the bot lets a reducing quote cross through flat up to
    ``maximum_projected_contracts_per_line``) can flatten the ticker but
    never open the opposite side.  Both guards apply even while the cap is
    disabled.

    The cap itself is disabled - every order passes - until the controller
    enables it, which it does only for an oversubscribed fleet, so the
    default 1.0 behaviour is unchanged.
    """

    TRACKED_ORDER_LIMIT = 8_192

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.enabled = False
        self._allocatable: dict[int, int] = {}
        self._shard_by_ticker: dict[str, int] = {}
        # Baseline position exposure per ticker from the last accepted snapshot.
        self._position_by_ticker: dict[str, int] = {}
        # Signed position (contracts x 100) per ticker: snapshot + fills since.
        self._signed_by_ticker: dict[str, int] = {}
        # Exposure added by fills since the ticker's baseline.
        self._fills_by_ticker: dict[str, int] = {}
        # Per ticker: when its accepted baseline was taken / its last fill applied.
        self._baseline_at_ms: dict[str, int] = {}
        self._last_fill_at_ms: dict[str, int] = {}
        self._orders: dict[str, _TrackedOrder] = {}
        self._reduce_only_tickers: set[str] = set()
        self._reservations: dict[str, tuple[int, int]] = {}
        self.updated_at_ms = 0
        self.positions_at_ms = 0
        self.rejections = 0
        # Orders admitted smaller than requested (cap or carryover trims).
        self.trimmed = 0

    # -- configuration ---------------------------------------------------

    def configure(
        self,
        *,
        enabled: bool,
        allocatable_by_shard: Optional[Mapping[Any, Any]] = None,
        shard_by_ticker: Optional[Mapping[str, Any]] = None,
        position_exposure_by_ticker: Optional[Mapping[str, Any]] = None,
        position_units_by_ticker: Optional[Mapping[str, Any]] = None,
        positions_at_ms: Optional[int] = None,
        reduce_only_tickers: Optional[Iterable[str]] = None,
    ) -> None:
        """Apply the controller's ``shard_exposure_limits`` message.

        ``position_exposure_by_ticker`` / ``position_units_by_ticker`` are the
        controller's copy of an admission snapshot taken at
        ``positions_at_ms``; without a stamp they count as taken at time 0,
        so they can never roll back anything the broker has seen itself.
        """
        with self._lock:
            self.enabled = bool(enabled)
            if allocatable_by_shard is not None:
                self._allocatable = {
                    int(index): max(0, int(units or 0)) for index, units in allocatable_by_shard.items()
                }
            if shard_by_ticker is not None:
                self._shard_by_ticker = {str(ticker): int(index) for ticker, index in shard_by_ticker.items()}
            if reduce_only_tickers is not None:
                self._reduce_only_tickers = {str(ticker) for ticker in reduce_only_tickers if str(ticker)}
            if position_exposure_by_ticker is not None or position_units_by_ticker is not None:
                exposure = (
                    {str(t): max(0, int(u or 0)) for t, u in position_exposure_by_ticker.items()}
                    if position_exposure_by_ticker is not None else None
                )
                signed = (
                    {str(t): int(u or 0) for t, u in position_units_by_ticker.items()}
                    if position_units_by_ticker is not None else None
                )
                self._apply_snapshot_locked(exposure, signed, int(positions_at_ms or 0))
            self.updated_at_ms = int(time.time() * 1000)

    def record_positions(self, positions: Iterable[Any], *, at_ms: Optional[int] = None) -> None:
        """Take the venue's positions (an admission snapshot) taken at ``at_ms``.

        ``at_ms`` should be the instant the venue call STARTED: a fill that
        landed while the call was in flight may or may not be inside the
        answer, and keeping it in the accumulator can only over-count.
        """
        exposure: dict[str, int] = {}
        signed: dict[str, int] = {}
        for item in positions or ():
            ticker = str(getattr(item, "market_id", "") or "")
            if not ticker:
                continue
            exposure[ticker] = exposure.get(ticker, 0) + max(0, int(getattr(item, "market_exposure_units", 0) or 0))
            signed[ticker] = signed.get(ticker, 0) + int(getattr(item, "position_units", 0) or 0)
        stamp = int(time.time() * 1000) if at_ms is None else int(at_ms)
        with self._lock:
            self._apply_snapshot_locked(exposure, signed, stamp)

    def _apply_snapshot_locked(
        self,
        exposure: Optional[dict[str, int]],
        signed: Optional[dict[str, int]],
        at_ms: int,
    ) -> None:
        tickers: set[str] = set(self._position_by_ticker) | set(self._fills_by_ticker) | set(self._signed_by_ticker)
        if exposure is not None:
            tickers |= set(exposure)
        if signed is not None:
            tickers |= set(signed)
        for ticker in tickers:
            if at_ms < self._baseline_at_ms.get(ticker, 0):
                continue  # older than the baseline this ticker already holds
            if at_ms >= self._last_fill_at_ms.get(ticker, 0):
                # Every fill the ledger applied is inside this snapshot.
                if exposure is not None:
                    self._set_or_pop(self._position_by_ticker, ticker, exposure.get(ticker, 0))
                    self._fills_by_ticker.pop(ticker, None)
                if signed is not None:
                    self._set_or_pop(self._signed_by_ticker, ticker, signed.get(ticker, 0))
                self._baseline_at_ms[ticker] = at_ms
                continue
            # Fills newer than the snapshot: keep the running value, never lower it.
            if exposure is not None:
                self._set_or_pop(
                    self._position_by_ticker, ticker,
                    max(exposure.get(ticker, 0), self._position_by_ticker.get(ticker, 0)),
                )
        self.positions_at_ms = max(self.positions_at_ms, at_ms)

    @staticmethod
    def _set_or_pop(target: dict[str, int], ticker: str, value: int) -> None:
        if value:
            target[ticker] = value
        else:
            target.pop(ticker, None)

    # -- order lifecycle -------------------------------------------------

    def note_order_result(self, order: Any, *, reduce_only: bool, request: Any = None) -> None:
        """Track the order a create/amend produced; contracts filled at placement are fills."""
        order_id = str(getattr(order, "order_id", "") or "")
        if not order_id:
            return
        ticker = str(getattr(order, "market_id", "") or getattr(request, "market_id", "") or "")
        side = str(getattr(order, "side", "") or "")
        if side:
            # The adaptor's Order.side is the outcome side - a sell of YES
            # arrives as side "no" - so it already carries the request's
            # action.  Applying "sell" on top of it would invert the fill
            # again (a watchdog exit of a long would ADD to the position).
            action = "buy"
        else:
            side = str(getattr(request, "side", "") or "")
            action = str(getattr(request, "action", "") or "buy")
        price = getattr(order, "price_units", None)
        if price is None:
            price = getattr(request, "price_units", None)
        if price is None:
            price = getattr(request, "new_price_units", 0)
        count = getattr(request, "count_units", None)
        if count is None:
            count = getattr(request, "new_total_fillable_count_units", None)
        if count is None:
            count = int(getattr(order, "fill_count_units", 0) or 0) + int(getattr(order, "remaining_count_units", 0) or 0)
        with self._lock:
            tracked = self._orders.get(order_id)
            if tracked is None:
                tracked = _TrackedOrder(ticker, side, action, max(0, int(price or 0)), 0)
                self._orders[order_id] = tracked
            else:
                tracked.price_units = max(0, int(price or 0))
            if reduce_only:
                tracked.reduce_only = True
            tracked.exempt_units = self._exempt_units_locked(ticker, side, action, int(count or 0), reduce_only=tracked.reduce_only)
            self._apply_fill_locked(order_id, tracked, int(getattr(order, "fill_count_units", 0) or 0))
            self._prune_locked()

    def note_order_update(self, event: Any) -> None:
        """Apply the venue's fills; the signed position and the fill accumulator follow."""
        order_id = str(getattr(event, "order_id", "") or "")
        ticker = str(getattr(event, "market_id", "") or "")
        if not order_id or not ticker:
            return
        fill_count = getattr(event, "fill_count_units", None)
        price = getattr(event, "price_units", None)
        status = str(getattr(event, "status", "") or "")
        with self._lock:
            tracked = self._orders.get(order_id)
            if tracked is None and fill_count is not None and int(fill_count) > 0:
                # An order this broker did not place (previous generation,
                # direct fallback): count its fills as new exposure.  The
                # stream's OrderUpdate.side is the outcome side (a "wd:"
                # sell of YES arrives as side "no"), so it is applied as a
                # buy of that side - never re-inverted by the client tag.
                tracked = _TrackedOrder(
                    ticker, str(getattr(event, "side", "") or ""), "buy",
                    max(0, int(price or 0)), 0,
                )
                self._orders[order_id] = tracked
            if tracked is not None:
                if price is not None:
                    tracked.price_units = max(0, int(price))
                if fill_count is not None:
                    self._apply_fill_locked(order_id, tracked, int(fill_count))
                if status and status != "resting":
                    tracked.done = True
            self._prune_locked()

    def _apply_fill_locked(self, order_id: str, tracked: _TrackedOrder, fill_count: int) -> None:
        fill_count = max(0, int(fill_count))
        delta = fill_count - tracked.fill_count_units
        if delta <= 0:
            return
        if not tracked.reduce_only:
            increasing = max(0, fill_count - max(tracked.fill_count_units, tracked.exempt_units))
            if increasing:
                self._fills_by_ticker[tracked.ticker] = (
                    self._fills_by_ticker.get(tracked.ticker, 0) + tracked.price_units * increasing // 100
                )
        self._set_or_pop(
            self._signed_by_ticker, tracked.ticker,
            self._signed_by_ticker.get(tracked.ticker, 0) + _signed_direction(tracked.side, tracked.action) * delta,
        )
        self._last_fill_at_ms[tracked.ticker] = max(self._last_fill_at_ms.get(tracked.ticker, 0), int(time.time() * 1000))
        tracked.fill_count_units = fill_count

    def _prune_locked(self) -> None:
        if len(self._orders) > self.TRACKED_ORDER_LIMIT:
            for key in list(self._orders)[: self.TRACKED_ORDER_LIMIT // 2]:
                self._orders.pop(key, None)

    # -- accounting ------------------------------------------------------

    def shard_for(self, ticker: str) -> Optional[int]:
        with self._lock:
            return self._shard_by_ticker.get(str(ticker))

    def signed_position_units(self, ticker: str) -> int:
        with self._lock:
            return self._signed_by_ticker.get(str(ticker), 0)

    def _exempt_units_locked(self, ticker: str, side: str, action: str, count: int, *, reduce_only: bool = False) -> int:
        count = max(0, int(count))
        if reduce_only:
            return count
        signed = self._signed_by_ticker.get(ticker, 0)
        if order_reduces_position(signed, side, action):
            return min(count, abs(signed))
        return 0

    def _order_exposure_locked(self, ticker: str, side: str, action: str, price: Any, count: Any, *, reduce_only: bool = False) -> int:
        """Cash an order of ``count`` at ``price`` locks beyond what merely offsets the position."""
        count = max(0, int(count or 0))
        exempt = self._exempt_units_locked(ticker, side, action, count, reduce_only=reduce_only)
        return max(0, int(price or 0)) * (count - exempt) // 100

    def _resting_exposure_locked(self, key: tuple[str, str], order: Any) -> int:
        order_id = str(getattr(order, "order_id", "") or "")
        tracked = self._orders.get(order_id)
        return self._order_exposure_locked(
            str(key[0]), str(getattr(order, "side", "") or key[1]),
            tracked.action if tracked else "buy",
            getattr(order, "price_units", None), getattr(order, "remaining_count_units", None),
            reduce_only=bool(tracked and tracked.reduce_only),
        )

    def _live_locked(
        self,
        shard: int,
        orders: Mapping[tuple[str, str], Any],
        *,
        exclude_key: Optional[tuple[str, str]] = None,
    ) -> dict[str, int]:
        resting = 0
        for key, order in orders.items():
            if key == exclude_key:
                continue
            if self._shard_by_ticker.get(str(key[0])) != shard:
                continue
            resting += self._resting_exposure_locked(key, order)
        positions = sum(
            units for ticker, units in self._position_by_ticker.items()
            if self._shard_by_ticker.get(ticker) == shard
        )
        fills = sum(
            units for ticker, units in self._fills_by_ticker.items()
            if self._shard_by_ticker.get(ticker) == shard
        )
        reserved = sum(units for index, units in self._reservations.values() if index == shard)
        return {
            "resting": resting, "positions": positions, "fills": fills, "reserved": reserved,
            "live": resting + positions + fills + reserved,
        }

    def live_exposure_units(
        self,
        shard: int,
        orders: Mapping[tuple[str, str], Any],
        *,
        exclude_key: Optional[tuple[str, str]] = None,
    ) -> int:
        with self._lock:
            return self._live_locked(int(shard), orders, exclude_key=exclude_key)["live"]

    @staticmethod
    def _orders_locked(orders: Optional[Mapping[tuple[str, str], Any]], registry: Any) -> Mapping[tuple[str, str], Any]:
        # The registry snapshot is taken while the ledger lock is held (lock
        # order ledger -> registry; the registry never calls the ledger), so
        # a peer create that registered its order and released its
        # reservation cannot fall between the snapshot and the admission.
        if orders is not None:
            return orders
        if registry is not None:
            return registry.snapshot()
        return {}

    def _fits_locked(
        self,
        shard: int,
        notional: int,
        orders: Mapping[tuple[str, str], Any],
        exclude_key: Optional[tuple[str, str]],
    ) -> tuple[bool, dict[str, int]]:
        """Whether ``notional`` more exposure fits under the shard's cap, with the live parts."""
        parts = self._live_locked(shard, orders, exclude_key=exclude_key)
        return parts["live"] + notional <= self._allocatable[shard], parts

    def _admit_locked(
        self,
        shard: int,
        ticker: str,
        notional: int,
        orders: Mapping[tuple[str, str], Any],
        exclude_key: Optional[tuple[str, str]],
    ) -> str:
        cap = self._allocatable[shard]
        parts = self._live_locked(shard, orders, exclude_key=exclude_key)
        if parts["live"] + notional > cap:
            self.rejections += 1
            detail = {
                "shard": shard, "ticker": str(ticker), "notional_units": notional,
                "live_units": parts["live"], "allocatable_units": cap, **{
                    f"{name}_units": value for name, value in parts.items() if name != "live"
                },
            }
            raise ShardExposureCapError(
                f"shard {shard} exposure cap: live ${parts['live'] / 10_000:,.2f} + order "
                f"${notional / 10_000:,.2f} would exceed allocatable ${cap / 10_000:,.2f} ({ticker})",
                detail=detail,
            )
        token = uuid.uuid4().hex
        self._reservations[token] = (shard, notional)
        return token

    def reserve(
        self,
        ticker: str,
        notional_units: int,
        *,
        orders: Optional[Mapping[tuple[str, str], Any]] = None,
        registry: Any = None,
        exclude_key: Optional[tuple[str, str]] = None,
    ) -> Optional[str]:
        """Admit ``notional_units`` of new exposure on ``ticker``'s shard.

        Returns a reservation token to ``release`` once the order is
        registered (or failed), so concurrent creates on the same shard
        cannot both pass on the same headroom.  ``None`` means nothing was
        reserved (ledger disabled, shard unknown, or nothing to add).
        Raises ``ShardExposureCapError`` when the cap would be exceeded.
        Pass ``registry`` (preferred) so its snapshot is read under the
        ledger lock, or an ``orders`` snapshot already taken.
        """
        notional = max(0, int(notional_units))
        with self._lock:
            if not self.enabled or notional <= 0:
                return None
            shard = self._shard_by_ticker.get(str(ticker))
            if shard is None or shard not in self._allocatable:
                return None
            return self._admit_locked(shard, ticker, notional, self._orders_locked(orders, registry), exclude_key)

    def reserve_order(
        self,
        ticker: str,
        side: str,
        *,
        price_units: Any,
        count_units: Any,
        action: str = "buy",
        reduce_only: bool = False,
        orders: Optional[Mapping[tuple[str, str], Any]] = None,
        registry: Any = None,
        exclude_key: Optional[tuple[str, str]] = None,
        replaces: Any = None,
    ) -> OrderAdmission:
        """Admit a create (``exclude_key``: the order it replaces) or an amend
        (``replaces``: the tracked order, only the added notional is reserved).

        Returns what may go to the venue (see ``OrderAdmission``: for an amend
        ``count_units`` is the remaining size after the amend).  Raises
        ``ShardExposureCapError`` only for an order with no reducing part.

        Restart carryover tickers (``reduce_only_tickers``), cap enabled or
        not: an order that does not reduce the position is refused
        (``carryover_flat``); one that does is capped at the position's size
        (a ``carryover`` trim) and flagged for the venue reduce-only stamp,
        so the ticker can be flattened but never flipped.  Then the size that
        merely offsets the ticker's signed position is exempt from the cap and
        venue reduce-only orders never reserve.  When the non-exempt remainder
        would pass the shard's cap, an order with a reducing part is admitted
        trimmed to that part (a ``shard_cap`` trim: nothing reserved, an
        amend can shrink its order) instead of refused, so a shard at its cap
        can always be reduced.
        """
        ticker = str(ticker)
        side = str(side or "")
        action = str(action or "buy")
        requested = max(0, int(count_units or 0))
        admitted = requested
        reason = ""
        carryover = False
        with self._lock:
            signed = self._signed_by_ticker.get(ticker, 0)
            if ticker in self._reduce_only_tickers and not reduce_only:
                if not order_reduces_position(signed, side, action):
                    self.rejections += 1
                    raise ShardExposureCapError(
                        f"{ticker}: restart carryover may only reduce its exchange position "
                        f"(signed {signed / 100:+.2f} contracts, {action} {side} refused)",
                        detail={
                            "reason": "carryover_flat", "ticker": ticker, "side": side,
                            "signed_units": signed, "notional_units": 0,
                        },
                    )
                carryover = True
                if requested > abs(signed):
                    admitted, reason = abs(signed), "carryover"
                    self.trimmed += 1
            detail: dict[str, Any] = {
                "ticker": ticker, "side": side, "action": action, "signed_units": signed,
                "requested_units": requested, "count_units": admitted,
            }
            if not self.enabled or reduce_only:
                return OrderAdmission(None, admitted, requested, reason, carryover, detail)
            shard = self._shard_by_ticker.get(ticker)
            if shard is None or shard not in self._allocatable:
                return OrderAdmission(None, admitted, requested, reason, carryover, detail)
            exempt = self._exempt_units_locked(ticker, side, action, admitted)
            notional = max(0, int(price_units or 0)) * (admitted - exempt) // 100
            if replaces is not None:
                notional -= self._resting_exposure_locked((ticker, side), replaces)
            if notional <= 0:
                return OrderAdmission(None, admitted, requested, reason, carryover, detail)
            live_orders = self._orders_locked(orders, registry)
            if exempt > 0:
                fits, parts = self._fits_locked(shard, notional, live_orders, exclude_key)
                if not fits:
                    # The reducing part always goes through: send just that.
                    self.trimmed += 1
                    detail.update({
                        "shard": shard, "count_units": exempt, "notional_units": notional,
                        "live_units": parts["live"], "allocatable_units": self._allocatable[shard],
                    })
                    return OrderAdmission(None, exempt, requested, "shard_cap", carryover, detail)
            token = self._admit_locked(shard, ticker, notional, live_orders, exclude_key)
            return OrderAdmission(token, admitted, requested, reason, carryover, detail)

    def release(self, token: Optional[str]) -> None:
        if not token:
            return
        with self._lock:
            self._reservations.pop(token, None)

    def snapshot(self, orders: Optional[Mapping[tuple[str, str], Any]] = None) -> dict[str, Any]:
        with self._lock:
            shards = {}
            for shard, cap in sorted(self._allocatable.items()):
                parts = self._live_locked(shard, orders or {})
                shards[str(shard)] = {"allocatable_units": cap, **{f"{k}_units": v for k, v in parts.items()}}
            return {
                "enabled": self.enabled, "updated_at_ms": self.updated_at_ms,
                "positions_at_ms": self.positions_at_ms, "rejections": self.rejections,
                "trimmed": self.trimmed,
                "tickers": len(self._shard_by_ticker), "shards": shards,
                "tracked_orders": len(self._orders),
                "reduce_only_tickers": len(self._reduce_only_tickers),
                # Carryover tickers whose position the ledger sees as flat: the
                # controller drops them from the workers' allowed sides.
                "flat_reduce_only": sorted(
                    ticker for ticker in self._reduce_only_tickers if not self._signed_by_ticker.get(ticker, 0)
                ),
            }


class BrokerRpcClient:
    """Read-through venue client whose order writes are broker RPCs."""

    def __init__(
        self,
        delegate: BaseClient,
        request_queue: Any,
        response_queue: Any,
        channel: str,
        *,
        rpc_timeout_seconds: float = DEFAULT_RPC_TIMEOUT_SECONDS,
    ) -> None:
        self._delegate = delegate
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._channel = channel
        self._pending: dict[str, "queue.Queue[Mapping[str, Any]]"] = {}
        self._pending_lock = threading.Lock()
        self._closed = threading.Event()
        self.rpc_timeout_seconds = float(rpc_timeout_seconds)
        # Shutdown-only escape hatches (see prepare_for_shutdown): when the
        # broker is unresponsive, reads and cancels go straight to the venue so
        # order verification cannot hang behind a dead process.
        self._fallback_enabled = False
        self._direct_only = False
        self._dispatcher = threading.Thread(target=self._dispatch, name=f"broker-rpc-{channel}", daemon=True)
        self._dispatcher.start()
        self.venue_name = delegate.venue_name
        self.venue = getattr(delegate, "venue", self.venue_name)
        self.environment_name = delegate.environment_name
        self.dry_run = delegate.dry_run
        self.rate_limit_backoff_seconds = delegate.rate_limit_backoff_seconds

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    @property
    def direct_only(self) -> bool:
        return self._direct_only

    # A broken response pipe makes every read fail instantly; back off so the
    # dispatcher does not spin, and let the RPC waiters time out normally.
    READER_ERROR_BACKOFF_SECONDS = 0.05

    def _read_response(self, timeout: float) -> Any:
        """Pop one response without the queue's reader lock (see read_queue_lockfree).

        This dispatcher is the response queue's only consumer.  A worker
        terminated inside ``Queue.get`` (stale-worker recovery) leaves the
        lock held, and the replacement worker inherits the same queue: read
        lock-free so the replacement's RPCs are answered instead of timing
        out forever.
        """
        return read_queue_lockfree(self._response_queue, timeout)

    def _dispatch(self) -> None:
        while not self._closed.is_set():
            try:
                response = self._read_response(0.25)
            except queue.Empty:
                continue
            except BrokerRequestDecodeError:
                continue
            except Exception:
                time.sleep(self.READER_ERROR_BACKOFF_SECONDS)
                continue
            if not isinstance(response, Mapping):
                continue
            request_id = str(response.get("request_id") or "")
            with self._pending_lock:
                waiter = self._pending.get(request_id)
            if waiter is not None:
                waiter.put(response)

    def _rpc(
        self,
        operation: str,
        payload: Mapping[str, Any],
        *,
        timeout: Optional[float] = None,
        fallback: Optional[Callable[[], Any]] = None,
    ) -> Any:
        if self._direct_only and fallback is not None:
            return fallback()
        wait_seconds = self.rpc_timeout_seconds if timeout is None else float(timeout)
        request_id = uuid.uuid4().hex
        waiter: "queue.Queue[Mapping[str, Any]]" = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = waiter
        self._request_queue.put(BrokerRequest(request_id, self._channel, operation, dict(payload), int(time.time() * 1000)))
        try:
            response = waiter.get(timeout=wait_seconds)
        except queue.Empty as exc:
            if self._fallback_enabled and fallback is not None:
                return fallback()
            raise TimeoutError(f"execution broker timed out during {operation}") from exc
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)
        if not response.get("ok"):
            error_type = _ERROR_TYPES.get(str(response.get("error_type")), ClientError)
            detail = response.get("error_detail")
            if isinstance(detail, Mapping):
                # The broker serialises subclasses under the base name older
                # workers know; a worker that knows the precise class uses it.
                precise = _ERROR_TYPES.get(str(detail.get("error_class") or ""))
                if precise is not None and issubclass(precise, error_type):
                    error_type = precise
            raise error_type(str(response.get("error") or f"broker {operation} failed"))
        return response.get("result")

    def ping(self, *, timeout: float = 3.0) -> Any:
        return self._rpc("ping", {}, timeout=timeout)

    def prepare_for_shutdown(
        self,
        *,
        broker_responsive: Optional[bool] = None,
        probe_timeout: float = 3.0,
        rpc_timeout: float = 10.0,
    ) -> bool:
        """Bound every remaining broker wait and arm the direct-venue fallback.

        Returns whether the broker answered.  When it did not, reads and
        cancels bypass it entirely so the actors' order verification still
        completes against the venue instead of hanging on a dead process.
        """
        responsive = broker_responsive
        if responsive is None:
            try:
                self.ping(timeout=probe_timeout)
                responsive = True
            except TimeoutError:
                responsive = False
            except Exception:
                # An older broker answers ``ping`` with an unsupported-operation
                # error: it is alive, which is all the probe needs to know.
                responsive = True
        self._fallback_enabled = True
        if responsive:
            self.rpc_timeout_seconds = min(self.rpc_timeout_seconds, float(rpc_timeout))
        else:
            self._direct_only = True
        return bool(responsive)

    def create_order(self, request: Any) -> Any:
        now = int(time.time() * 1000)
        intent = QuoteIntent(
            request.market_id, request.side, request.price_units, request.count_units,
            now, IntentUrgency.RISK_REDUCE if request.reduce_only else IntentUrgency.NORMAL,
            now, now + 20_000, exposure_increasing=not bool(request.reduce_only),
            risk_justification="reduce_only" if request.reduce_only else "strategy_quote",
        )
        return self._rpc("quote_intent", {"action": "create_order", "request": request, "intent": intent})

    def amend_order(self, request: Any) -> Any:
        now = int(time.time() * 1000)
        intent = QuoteIntent(
            request.market_id, request.side, request.new_price_units,
            request.new_total_fillable_count_units, now, IntentUrgency.NORMAL_CANCEL_OR_AMEND,
            now, now + 20_000, risk_justification="strategy_amend",
        )
        return self._rpc("quote_intent", {"action": "amend_order", "request": request, "intent": intent})

    def decrease_order_to(self, *, order_id: str, remaining_count_units: int) -> Any:
        return self._rpc("decrease_order_to", {"order_id": order_id, "remaining_count_units": remaining_count_units})

    def cancel_order(self, *, order_id: str) -> Any:
        return self._rpc(
            "cancel_order", {"order_id": order_id},
            fallback=lambda: self._delegate.cancel_order(order_id=order_id),
        )

    def get_market(self, market_id: str) -> Any:
        return self._rpc(
            "get_market", {"market_id": market_id},
            fallback=lambda: self._delegate.get_market(market_id),
        )

    def list_markets(self, query: Any) -> Any:
        return self._rpc("list_markets", {"query": query})

    def get_market_quote(self, market_id: str) -> Any:
        return self._rpc(
            "get_market_quote", {"market_id": market_id},
            fallback=lambda: self._delegate.get_market_quote(market_id),
        )

    def get_account_balance(self) -> Any:
        return self._rpc("balance", {})

    def get_account_limits(self) -> Any:
        return self._rpc("limits", {})

    def list_account_positions(self, query: Any = None) -> Any:
        return self._rpc("list_account_positions", {"query": query})

    def list_account_orders(self, query: Any = None) -> Any:
        return self._rpc(
            "list_account_orders", {"query": query},
            fallback=lambda: (
                self._delegate.list_account_orders(query) if query is not None
                else self._delegate.list_account_orders()
            ),
        )

    def list_account_fills(self, query: Any = None) -> Any:
        return self._rpc("list_account_fills", {"query": query})

    def get_positions(self, market_id: str) -> Any:
        return self._rpc(
            "get_positions", {"market_id": market_id},
            fallback=lambda: self._delegate.get_positions(market_id),
        )

    def get_resting_orders(self, market_id: str) -> Any:
        return self._rpc(
            "get_resting_orders", {"market_id": market_id},
            fallback=lambda: self._delegate.get_resting_orders(market_id),
        )

    def get_order_queue_position(self, order_id: str) -> Any:
        return self._rpc("get_order_queue_position", {"order_id": order_id})

    def get_series(self, series_id: str) -> Any:
        return self._rpc("get_series", {"series_id": series_id})

    def get_series_fee_changes(self, series_id: str, *, show_historical: bool = False) -> Any:
        return self._rpc("get_series_fee_changes", {"series_id": series_id, "show_historical": show_historical})

    def get_incentive_programs(self, *, status: str = "active", incentive_type: str = "all", limit: int = 10_000) -> Any:
        return self._rpc("get_incentive_programs", {"status": status, "incentive_type": incentive_type, "limit": limit})

    async def close(self) -> None:
        # The worker owns and closes the delegate after all actors stop.
        return None

    def close_dispatcher(self) -> None:
        self._closed.set()
        self._dispatcher.join(timeout=1.0)


class ExecutionBrokerProcess(mp.Process):
    BOT_ORDER_PREFIXES = BOT_ORDER_PREFIXES
    # Bounded per-request execution.  A venue call that outlives this is
    # abandoned to its thread (the HTTP layer still bounds it) and the request
    # loop moves on, so one stuck call cannot serialize the whole fleet.
    EXECUTE_TIMEOUT_SECONDS = 20.0
    CLEANUP_TIMEOUT_SECONDS = 120.0
    EXECUTOR_THREADS = 4
    HEARTBEAT_SECONDS = 2.0
    # Read caches: the incentive catalog and series fee schedule change on the
    # order of hours yet every actor re-fetched them once a minute through the
    # broker, which was the dominant load whenever a reconcile burst arrived.
    INCENTIVE_CACHE_SECONDS = 300.0
    SERIES_CACHE_SECONDS = 300.0
    MARKET_CACHE_SECONDS = 5.0
    # Class-level fallbacks for every tunable __init__ stores on the instance.
    # On Windows the parent pickles this object to spawn the child, which
    # unpickles it against the module currently on disk: a parent still
    # running an older build hands over an instance without these attributes,
    # and run() must keep working (with defaults) rather than crash-loop.
    refresh_limits_seconds = 60.0
    cleanup_attempts = 8
    cleanup_delay_seconds = 0.5
    execute_timeout_seconds = EXECUTE_TIMEOUT_SECONDS
    cleanup_timeout_seconds = CLEANUP_TIMEOUT_SECONDS
    request_ttl_seconds = DEFAULT_RPC_TIMEOUT_SECONDS
    heartbeat_seconds = HEARTBEAT_SECONDS

    def __init__(
        self,
        *,
        venue: str = "kalshi",
        client_config: Any = None,
        request_queue: Any,
        response_queues: Mapping[str, Any],
        status_queue: Any,
        write_utilization_limit: float = 0.85,
        refresh_limits_seconds: float = 60.0,
        cleanup_attempts: int = 8,
        cleanup_delay_seconds: float = 0.5,
        execute_timeout_seconds: Optional[float] = None,
        cleanup_timeout_seconds: Optional[float] = None,
        request_ttl_seconds: float = DEFAULT_RPC_TIMEOUT_SECONDS,
        heartbeat_seconds: Optional[float] = None,
    ) -> None:
        super().__init__(name="execution-broker", daemon=False)
        self.venue = str(venue or "kalshi")
        self.client_config = client_config
        self.request_queue = request_queue
        self.response_queues = dict(response_queues)
        self.status_queue = status_queue
        self.write_utilization_limit = write_utilization_limit
        self.refresh_limits_seconds = refresh_limits_seconds
        self.cleanup_attempts = max(1, int(cleanup_attempts))
        self.cleanup_delay_seconds = max(0.0, float(cleanup_delay_seconds))
        self.execute_timeout_seconds = float(
            self.EXECUTE_TIMEOUT_SECONDS if execute_timeout_seconds is None else execute_timeout_seconds
        )
        self.cleanup_timeout_seconds = float(
            self.CLEANUP_TIMEOUT_SECONDS if cleanup_timeout_seconds is None else cleanup_timeout_seconds
        )
        self.request_ttl_seconds = max(0.0, float(request_ttl_seconds))
        self.heartbeat_seconds = float(
            self.HEARTBEAT_SECONDS if heartbeat_seconds is None else heartbeat_seconds
        )
        # Thread-affine helpers (executor, locks) are created inside the
        # child: this object is pickled to spawn the process on Windows.

    @staticmethod
    def _owned(client_order_id: str) -> bool:
        return is_bot_owned_order(client_order_id)

    # ------------------------------------------------------------------
    # Child-process-only state, created lazily so unit tests can call the
    # helpers on an un-started instance.

    def _get_executor(self) -> ThreadPoolExecutor:
        executor = self.__dict__.get("_executor")
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=self.EXECUTOR_THREADS, thread_name_prefix="broker-exec"
            )
            self.__dict__["_executor"] = executor
        return executor

    def _cache_state(self) -> tuple[dict[tuple, tuple[float, Any]], threading.Lock]:
        cache = self.__dict__.get("_read_cache")
        lock = self.__dict__.get("_cache_lock")
        if cache is None or lock is None:
            cache = self.__dict__.setdefault("_read_cache", {})
            lock = self.__dict__.setdefault("_cache_lock", threading.Lock())
        return cache, lock

    def _cached(self, key: tuple, ttl_seconds: float, loader: Callable[[], Any]) -> Any:
        if ttl_seconds <= 0:
            return loader()
        cache, lock = self._cache_state()
        now = time.monotonic()
        with lock:
            hit = cache.get(key)
            if hit is not None and hit[0] > now:
                return hit[1]
        value = loader()
        with lock:
            cache[key] = (time.monotonic() + float(ttl_seconds), value)
            if len(cache) > 4_096:
                expired = [item for item, (expires, _) in cache.items() if expires <= now]
                for item in expired:
                    cache.pop(item, None)
        return value

    @property
    def _registry(self) -> OrderRegistry:
        registry = self.__dict__.get("_order_registry_state")
        if not isinstance(registry, OrderRegistry):
            registry = OrderRegistry()
            if isinstance(self.__dict__.get("_order_registry_state"), dict):
                registry.replace(self.__dict__["_order_registry_state"])
            self.__dict__["_order_registry_state"] = registry
        return registry

    @property
    def _order_registry(self) -> dict[tuple[str, str], Any]:
        """Snapshot of the live order per (ticker, side); see OrderRegistry."""
        return self._registry.snapshot()

    @_order_registry.setter
    def _order_registry(self, value: Mapping[tuple[str, str], Any]) -> None:
        self._registry.replace(value)

    @property
    def _ledger(self) -> ShardExposureLedger:
        ledger = self.__dict__.get("_shard_exposure_ledger")
        if not isinstance(ledger, ShardExposureLedger):
            ledger = ShardExposureLedger()
            self.__dict__["_shard_exposure_ledger"] = ledger
        return ledger

    # ------------------------------------------------------------------

    def _respond(self, request: BrokerRequest, *, result: Any = None, error: Optional[BaseException] = None) -> None:
        target = self.response_queues.get(request.response_channel)
        if target is None:
            return
        detail = None
        error_type = type(error).__name__ if error else None
        if isinstance(error, BotOrderCleanupError):
            detail = {"remaining_order_ids": list(error.remaining_order_ids)}
        elif isinstance(error, ShardExposureCapError):
            # Serialised under the base name so a worker built before this
            # class existed still maps it to the balance cooldown; a current
            # worker restores the precise class from error_detail.
            error_type = InsufficientBalanceError.__name__
            detail = {"error_class": ShardExposureCapError.__name__, **error.detail}
        target.put({
            "request_id": request.request_id,
            "ok": error is None,
            "result": result,
            "error": str(error) if error else None,
            "error_type": error_type,
            "error_detail": detail,
        })

    def _reserve_exposure(
        self,
        action: str,
        request: Any,
        key: Optional[tuple[str, str]],
    ) -> tuple[Optional[str], Any]:
        """Ledger admission for a create/amend; see ShardExposureLedger.reserve_order.

        Returns ``(token, request)``: the reservation to release after the
        venue call, and the request to send.  That is the caller's request
        unless the ledger admitted it smaller (its count is replaced by the
        admitted size - logged as ``SHARD_CAP_TRIMMED`` - so the venue, the
        order registry and the ledger all carry the trimmed size) or the
        ticker is a restart carryover (creates are stamped venue reduce-only,
        so the venue itself cancels anything beyond the position it holds).

        A create reserves the notional beyond what offsets the ticker's
        signed position and excludes the order it will replace on its
        (ticker, side) key; an amend reserves only the notional it adds over
        the tracked order (amend-downs pass).  Reduce-only creates never
        reserve.  The order registry is read inside the ledger lock.
        """
        ledger = self._ledger
        ticker = str(getattr(request, "market_id", "") or (key[0] if key else ""))
        side = str(getattr(request, "side", "") or (key[1] if key else ""))
        registry_key = key if key is not None else (ticker, side)
        order_action = str(getattr(request, "action", "") or "buy")
        changes: dict[str, Any] = {}
        if action == "create_order":
            admission = ledger.reserve_order(
                ticker, side, action=order_action,
                price_units=getattr(request, "price_units", 0), count_units=getattr(request, "count_units", 0),
                reduce_only=bool(getattr(request, "reduce_only", False)),
                registry=self._registry, exclude_key=registry_key,
            )
            if admission.trimmed:
                changes["count_units"] = admission.count_units
            if admission.carryover and not bool(getattr(request, "reduce_only", False)):
                changes["reduce_only"] = True
        else:
            existing = self._registry.get(registry_key)
            filled = max(0, int(getattr(existing, "fill_count_units", 0) or 0))
            remaining_after = max(0, int(getattr(request, "new_total_fillable_count_units", 0) or 0) - filled)
            admission = ledger.reserve_order(
                ticker, side, action=order_action,
                price_units=getattr(request, "new_price_units", 0), count_units=remaining_after,
                registry=self._registry, exclude_key=None, replaces=existing,
            )
            if admission.trimmed:
                changes["new_total_fillable_count_units"] = filled + admission.count_units
        if not changes:
            return admission.token, request
        if not dataclasses.is_dataclass(request) or isinstance(request, type):
            # Cannot rewrite the request: refuse rather than send the full size.
            ledger.release(admission.token)
            raise ShardExposureCapError(
                f"{ticker}: {action} could not be trimmed to {admission.count_units / 100:.2f} contracts",
                detail={"reason": "untrimmable", **admission.detail},
            )
        if admission.trimmed:
            detail = admission.detail
            LOGGER.warning(
                "SHARD_CAP_TRIMMED | op=%s ticker=%s side=%s action=%s reason=%s "
                "requested_contracts=%.2f admitted_contracts=%.2f signed_contracts=%+.2f%s",
                action, ticker, side, order_action, admission.reason,
                admission.requested_units / 100, admission.count_units / 100,
                int(detail.get("signed_units", 0)) / 100,
                (
                    f" shard={detail['shard']} live_dollars={int(detail['live_units']) / 10_000:.2f}"
                    f" allocatable_dollars={int(detail['allocatable_units']) / 10_000:.2f}"
                    if "shard" in detail else ""
                ),
            )
        return admission.token, dataclasses.replace(request, **changes)

    def _cancel_owned(self, client: BaseClient, market_id: str = "") -> int:
        return cancel_and_verify_owned_orders(
            client,
            market_id,
            attempts=self.cleanup_attempts,
            delay_seconds=self.cleanup_delay_seconds,
        )

    def _call_bounded(
        self,
        fn: Callable[[], Any],
        *,
        timeout: float,
        label: str,
        abandoned: Optional[threading.Event] = None,
    ) -> Any:
        """Run ``fn`` on the executor and wait at most ``timeout`` seconds.

        ``abandoned`` is set when the wait expires so the still-running call
        knows its requester has already been answered with ``TimeoutError``.
        """
        future = self._get_executor().submit(fn)
        try:
            return future.result(timeout=max(0.0, float(timeout)))
        except FutureTimeoutError as exc:
            if abandoned is not None:
                abandoned.set()
            future.cancel()
            raise TimeoutError(
                f"execution broker timed out executing {label} after {float(timeout):.0f}s"
            ) from exc

    def _execute_bounded_traced(self, client: BaseClient, request: BrokerRequest) -> tuple[Any, bool]:
        """Bounded execution; also reports whether the venue was really called."""
        abandoned = threading.Event()
        return self._call_bounded(
            lambda: self._execute_traced(client, request, abandoned=abandoned),
            timeout=self.execute_timeout_seconds,
            label=request.operation,
            abandoned=abandoned,
        )

    def _execute_bounded(self, client: BaseClient, request: BrokerRequest) -> Any:
        return self._execute_bounded_traced(client, request)[0]

    # Request-queue reader errors (a broken pipe handle) are not decode
    # errors: each one backs the loop off briefly instead of spinning, and a
    # persistent run ends the broker (run() raises BrokerQueueUnreadable after
    # its cleanup) so the manager's dead-broker restart replaces it within
    # seconds rather than the stall detector after 45 s of silence.
    READER_ERROR_LIMIT = 20
    READER_ERROR_BACKOFF_SECONDS = 0.05
    _reader_errors = 0

    def _next_request(self, timeout: float) -> Any:
        """Pop one request without taking the shared queue's reader lock.

        The broker is the request queue's only consumer, so the lock exists
        purely for multi-reader safety it does not need.  ``Queue.get`` holds
        that lock while it polls; a broker terminated inside ``get`` (the
        manager's stall restart) dies holding it and every replacement broker
        would block forever on the first ``get``.  Reading the pipe directly
        (see read_queue_lockfree) makes a generation change safe.  Raises
        ``queue.Empty``, ``BrokerRequestDecodeError`` for an undecodable item,
        and propagates pipe errors.
        """
        return read_queue_lockfree(self.request_queue, timeout)

    def _read_request(self, timeout: float) -> tuple[Any, str]:
        """One request-queue read: ``(request, outcome)``.

        ``outcome`` is ``"request"`` (a BrokerRequest), ``"empty"``,
        ``"decode_error"`` (an item torn mid-write by a dying producer: its
        requester times out and retries) or ``"reader_error"`` (the pipe
        itself failed; the loop backed off ``READER_ERROR_BACKOFF_SECONDS``).
        Raises ``BrokerQueueUnreadable`` once ``READER_ERROR_LIMIT``
        consecutive reader errors show the pipe is gone for good.
        """
        try:
            item = self._next_request(timeout)
        except queue.Empty:
            self._reader_errors = 0
            return None, "empty"
        except BrokerRequestDecodeError:
            self._reader_errors = 0
            return None, "decode_error"
        except Exception as exc:
            self._reader_errors = int(self._reader_errors) + 1
            if self._reader_errors >= self.READER_ERROR_LIMIT:
                raise BrokerQueueUnreadable(
                    f"request queue unreadable after {self._reader_errors} consecutive read errors: {exc}"
                ) from exc
            time.sleep(self.READER_ERROR_BACKOFF_SECONDS)
            return None, "reader_error"
        self._reader_errors = 0
        if isinstance(item, BrokerRequest):
            return item, "request"
        return None, "decode_error"

    def is_stale(self, request: BrokerRequest, *, now_ms: Optional[int] = None) -> bool:
        """True when the requester has already stopped waiting for this request."""
        if self.request_ttl_seconds <= 0 or request.operation in _KEEP_WHEN_STALE:
            return False
        current = int(time.time() * 1000) if now_ms is None else int(now_ms)
        return current - int(request.submitted_at_ms) > self.request_ttl_seconds * 1000

    def _settle_write(
        self,
        client: BaseClient,
        key: tuple[str, str],
        result: Any,
        *,
        stamp: int,
        abandoned: Optional[threading.Event],
    ) -> None:
        """Register the order a create/amend produced, or reconcile an abandoned one.

        When the requester was already told ``TimeoutError`` nothing tracks
        this order: cancel it now (best effort) and, if that fails, keep its
        id as an orphan so the next write on this side cancels it first.
        """
        registry = self._registry
        order_id = str(getattr(result, "order_id", "") or "")
        if abandoned is not None and abandoned.is_set() and order_id:
            try:
                client.cancel_order(order_id=order_id)
                registry.forget_order(order_id)
                return
            except OrderNotFoundError:
                registry.forget_order(order_id)
                return
            except Exception:
                registry.add_orphan(key, order_id)
                return
        registry.set(key, result, stamp=stamp)

    def _cancel_prior(self, client: BaseClient, key: tuple[str, str]) -> None:
        """Cancel the tracked order and any orphans on ``key`` before a new create.

        Orphans are popped as a batch; if one cancel fails for a transport
        reason (venue unreachable, rate limited) it AND every orphan not yet
        attempted go back into the registry before the error propagates, so
        no orphan is ever silently dropped.  A cancel the venue rejects for
        any other reason (the order is no longer resting: filled, expired,
        already cancelled) has nothing left to cancel, so that orphan is
        dropped instead of wedging every later create on this side.
        """
        registry = self._registry
        existing = registry.get(key)
        existing_id = str(getattr(existing, "order_id", "") or "")
        if existing_id:
            try:
                client.cancel_order(order_id=existing_id)
            except OrderNotFoundError:
                pass
        orphans = registry.take_orphans(key)
        for index, order_id in enumerate(orphans):
            try:
                client.cancel_order(order_id=order_id)
            except OrderNotFoundError:
                continue
            except Exception as exc:
                if isinstance(exc, ClientError) and not isinstance(exc, RateLimitError) \
                        and not is_venue_transport_failure(exc):
                    # Venue rejection: the order is not resting any more.
                    continue
                for untried in orphans[index:]:
                    registry.add_orphan(key, untried)
                raise

    def _execute(
        self,
        client: BaseClient,
        request: BrokerRequest,
        *,
        abandoned: Optional[threading.Event] = None,
    ) -> Any:
        return self._execute_traced(client, request, abandoned=abandoned)[0]

    def _execute_traced(
        self,
        client: BaseClient,
        request: BrokerRequest,
        *,
        abandoned: Optional[threading.Event] = None,
    ) -> tuple[Any, bool]:
        """Execute ``request``; returns ``(result, venue_called)``.

        ``venue_called`` is False for broker-local operations and for reads
        served from the cache, so the caller can keep venue-health accounting
        honest.
        """
        payload = request.payload
        touched = [request.operation not in _NO_VENUE_OPERATIONS]

        def cached(key: tuple, ttl_seconds: float, loader: Callable[[], Any]) -> Any:
            touched[0] = False

            def traced_loader() -> Any:
                touched[0] = True
                return loader()

            return self._cached(key, ttl_seconds, traced_loader)

        def check_not_abandoned(label: str) -> None:
            # The requester was already told TimeoutError (the deadline expired
            # while _cancel_prior ran): placing the order now would only be
            # followed by _settle_write cancelling it - two venue writes for
            # nobody, during the very congestion that caused the timeout.
            if abandoned is not None and abandoned.is_set():
                raise TimeoutError(f"execution broker abandoned {label}: requester timed out")

        if request.operation == "quote_intent":
            action = str(payload.get("action") or "")
            intent = payload.get("intent")
            key = (intent.ticker, intent.side) if isinstance(intent, QuoteIntent) else None
            stamp = time.monotonic_ns()
            if action == "create_order":
                order_request = payload["request"]
                # Live-cap admission BEFORE the prior order is cancelled: a
                # refused create leaves the resting quote in place.  The
                # request the ledger hands back may be trimmed to the size it
                # admitted: that is what the venue, the registry and the
                # ledger's own tracking see from here on.
                token, order_request = self._reserve_exposure("create_order", order_request, key)
                try:
                    if key is not None:
                        self._cancel_prior(client, key)
                    check_not_abandoned("create_order")
                    result = client.create_order(order_request)
                    self._ledger.note_order_result(
                        result, reduce_only=bool(getattr(order_request, "reduce_only", False)), request=order_request,
                    )
                    if key is not None:
                        self._settle_write(client, key, result, stamp=stamp, abandoned=abandoned)
                finally:
                    self._ledger.release(token)
                return result, True
            if action == "amend_order":
                order_request = payload["request"]
                check_not_abandoned("amend_order")
                token, order_request = self._reserve_exposure("amend_order", order_request, key)
                try:
                    result = client.amend_order(order_request)
                    self._ledger.note_order_result(result, reduce_only=False, request=order_request)
                    if key is not None:
                        self._settle_write(client, key, result, stamp=stamp, abandoned=abandoned)
                finally:
                    self._ledger.release(token)
                return result, True
            raise ValueError(f"unsupported quote intent action: {action}")
        if request.operation == "create_order":
            order_request = payload["request"]
            token, order_request = self._reserve_exposure("create_order", order_request, None)
            try:
                result = client.create_order(order_request)
                self._ledger.note_order_result(
                    result, reduce_only=bool(getattr(order_request, "reduce_only", False)), request=order_request,
                )
            finally:
                self._ledger.release(token)
            return result, True
        if request.operation == "amend_order":
            order_request = payload["request"]
            token, order_request = self._reserve_exposure("amend_order", order_request, None)
            try:
                result = client.amend_order(order_request)
                self._ledger.note_order_result(result, reduce_only=False, request=order_request)
            finally:
                self._ledger.release(token)
            return result, True
        if request.operation == "decrease_order_to":
            return client.decrease_order_to(
                order_id=str(payload["order_id"]),
                remaining_count_units=int(payload["remaining_count_units"]),
            ), True
        if request.operation == "cancel_order":
            order_id = str(payload["order_id"])
            result = client.cancel_order(order_id=order_id)
            self._registry.forget_order(order_id)
            return result, True
        if request.operation == "cancel_market":
            return self._cancel_owned(client, str(payload.get("market_id") or "")), True
        if request.operation == "cancel_all":
            return self._cancel_owned(client), True
        if request.operation == "verify_clear":
            return self._cancel_owned(client), True
        if request.operation == "limits":
            return client.get_account_limits(), True
        if request.operation == "balance":
            return client.get_account_balance(), True
        if request.operation == "get_market":
            market_id = str(payload["market_id"])
            return cached(
                ("get_market", market_id), self.MARKET_CACHE_SECONDS,
                lambda: client.get_market(market_id),
            ), touched[0]
        if request.operation == "list_markets":
            return client.list_markets(payload["query"]), True
        if request.operation == "get_market_quote":
            return client.get_market_quote(str(payload["market_id"])), True
        if request.operation == "list_account_positions":
            return (
                client.list_account_positions(payload.get("query")) if payload.get("query") is not None
                else client.list_account_positions()
            ), True
        if request.operation == "list_account_orders":
            return (
                client.list_account_orders(payload.get("query")) if payload.get("query") is not None
                else client.list_account_orders()
            ), True
        if request.operation == "list_account_fills":
            return (
                client.list_account_fills(payload.get("query")) if payload.get("query") is not None
                else client.list_account_fills()
            ), True
        if request.operation == "get_positions":
            return client.get_positions(str(payload["market_id"])), True
        if request.operation == "get_resting_orders":
            return client.get_resting_orders(str(payload["market_id"])), True
        if request.operation == "get_order_queue_position":
            return client.get_order_queue_position(str(payload["order_id"])), True
        if request.operation == "get_series":
            series_id = str(payload["series_id"])
            return cached(
                ("get_series", series_id), self.SERIES_CACHE_SECONDS,
                lambda: client.get_series(series_id),
            ), touched[0]
        if request.operation == "get_series_fee_changes":
            series_id = str(payload["series_id"])
            show_historical = bool(payload.get("show_historical"))
            return cached(
                ("get_series_fee_changes", series_id, show_historical), self.SERIES_CACHE_SECONDS,
                lambda: client.get_series_fee_changes(series_id, show_historical=show_historical),
            ), touched[0]
        if request.operation == "get_incentive_programs":
            status = str(payload.get("status") or "active")
            incentive_type = str(payload.get("incentive_type") or "all")
            limit = int(payload.get("limit") or 10_000)
            return cached(
                ("get_incentive_programs", status, incentive_type, limit), self.INCENTIVE_CACHE_SECONDS,
                lambda: client.get_incentive_programs(status=status, incentive_type=incentive_type, limit=limit),
            ), touched[0]
        if request.operation == "admission_snapshot":
            from clients.models import AccountOrderQuery, AccountPositionQuery
            positions_at_ms = int(time.time() * 1000)
            positions = client.list_account_positions(AccountPositionQuery(nonzero_only=True, page_size=1_000))
            # The same position exposure the controller allocates on seeds
            # the live-cap ledger (see ShardExposureLedger), stamped with the
            # instant the read started so a later copy of it can never roll
            # back fills applied since.
            self._ledger.record_positions(positions, at_ms=positions_at_ms)
            return {
                "limits": client.get_account_limits(),
                "balance": client.get_account_balance(),
                "orders": client.list_account_orders(AccountOrderQuery(status="resting", page_size=1_000)),
                "positions": positions,
                "positions_at_ms": positions_at_ms,
            }, True
        if request.operation == "shard_exposure_limits":
            # Controller-pushed live cap: allocatable cash per exchange shard
            # and the shard of every desired market.  Enabled only for an
            # oversubscribed fleet (the controller sends enabled=False at the
            # default 1.0).
            self._ledger.configure(
                enabled=bool(payload.get("enabled")),
                allocatable_by_shard=payload.get("allocatable_by_shard"),
                shard_by_ticker=payload.get("shard_by_ticker"),
                position_exposure_by_ticker=payload.get("position_exposure_by_ticker"),
                position_units_by_ticker=payload.get("position_units_by_ticker"),
                positions_at_ms=payload.get("positions_at_ms"),
                reduce_only_tickers=payload.get("reduce_only_tickers"),
            )
            return "configured", False
        if request.operation == "order_update":
            event = payload.get("event")
            key = (getattr(event, "market_id", ""), getattr(event, "side", ""))
            order_id = str(getattr(event, "order_id", "") or "")
            registry = self._registry
            self._ledger.note_order_update(event)
            if getattr(event, "status", "") == "resting":
                # An orphan that is confirmed resting stays an orphan: the
                # next create on this side cancels it.  Anything else is the
                # venue's word on the live order for this side.
                if not (order_id and registry.is_orphan(key, order_id)):
                    registry.set(key, event)
            elif order_id:
                registry.forget_order(order_id)
            else:
                registry.pop(key)
            return "reconciled", False
        if request.operation == "ping":
            return "pong", False
        if request.operation == "stop":
            return "stopping", False
        raise ValueError(f"unsupported broker operation: {request.operation}")

    @staticmethod
    def _write_blocked(
        request: BrokerRequest,
        *,
        all_workers_quiesced: bool,
        quiesced_channels: set[str],
    ) -> bool:
        if request.response_channel == "controller":
            return False
        if not all_workers_quiesced and request.response_channel not in quiesced_channels:
            return False
        return request.operation in {"quote_intent", "create_order", "amend_order", "decrease_order_to"}

    def run(self) -> None:  # pragma: no cover - integration exercised with real multiprocessing
        config = build_client_config(
            self.venue,
            self.client_config,
            enable_shared_write_rate_limiter=False,
        )
        client: BaseClient = build_client(self.venue, config)
        self.__dict__["_order_registry_state"] = OrderRegistry()
        self.__dict__["_shard_exposure_ledger"] = ShardExposureLedger()
        self.__dict__["_read_cache"] = {}
        self.__dict__["_cache_lock"] = threading.Lock()
        try:
            # Previous-release bot orders are recognized by their client tag.
            # Start from a verified clear account rather than inheriting an
            # order whose strategy generation is unknown to this broker.
            client.list_account_positions()
            self._cancel_owned(client)
        except Exception as exc:
            self.status_queue.put({"type": "startup_error", "venue": self.venue, "at_ms": int(time.time() * 1000), "error": str(exc)})
            import asyncio
            asyncio.run(client.close())
            return
        normal_bucket = TokenBucket(0, 0)
        reserved_bucket = TokenBucket(0, 0)
        normal_read_bucket = TokenBucket(0, 0)
        reserved_read_bucket = TokenBucket(0, 0)
        last_limits = 0.0
        sequence = 0
        pending: list[tuple[int, int, BrokerRequest]] = []
        stopping = False
        repeated_429 = 0
        superseded: set[str] = set()
        followers: dict[str, list[BrokerRequest]] = {}
        latest_intents: dict[tuple[str, str], BrokerRequest] = {}
        all_workers_quiesced = False
        quiesced_channels: set[str] = set()
        # Liveness accounting reported to the controller.  ``consecutive_timeouts``
        # counts venue failures of every kind - broker deadline overruns and
        # HTTP-layer connect/read timeouts or connection errors alike - and
        # only a call that really reached the venue and succeeded resets it.
        last_heartbeat = 0.0
        last_success_at_ms = int(time.time() * 1000)
        consecutive_timeouts = 0
        dropped_stale = 0
        decode_errors = 0
        reader_errors = 0
        self._reader_errors = 0
        # Cleanup ops run detached from the loop: (future, request, deadline).
        in_flight: list[tuple[Future, BrokerRequest, float]] = []

        def note_success() -> None:
            nonlocal last_success_at_ms, consecutive_timeouts
            last_success_at_ms = int(time.time() * 1000)
            consecutive_timeouts = 0

        def note_failure(exc: BaseException) -> None:
            nonlocal consecutive_timeouts
            if isinstance(exc, TimeoutError) or is_venue_transport_failure(exc):
                consecutive_timeouts += 1

        def respond_all(request: BrokerRequest, *, result: Any = None, error: Optional[BaseException] = None) -> None:
            self._respond(request, result=result, error=error)
            for follower in followers.pop(request.request_id, []):
                self._respond(follower, result=result, error=error)

        def reap_in_flight() -> None:
            nonlocal consecutive_timeouts
            if not in_flight:
                return
            now = time.monotonic()
            remaining: list[tuple[Future, BrokerRequest, float]] = []
            for future, request, deadline in in_flight:
                if future.done():
                    try:
                        respond_all(request, result=future.result())
                        note_success()
                    except Exception as exc:
                        note_failure(exc)
                        respond_all(request, error=exc)
                elif now >= deadline:
                    future.cancel()
                    consecutive_timeouts += 1
                    respond_all(request, error=TimeoutError(
                        f"execution broker timed out executing {request.operation} "
                        f"after {self.cleanup_timeout_seconds:.0f}s"
                    ))
                else:
                    remaining.append((future, request, deadline))
            in_flight[:] = remaining

        def enqueue(request: BrokerRequest) -> None:
            nonlocal sequence
            sequence += 1
            if request.operation == "quote_intent":
                intent = request.payload.get("intent")
                if isinstance(intent, QuoteIntent) and intent.urgency == IntentUrgency.NORMAL:
                    key = (intent.ticker, intent.side)
                    prior = latest_intents.get(key)
                    if prior is not None:
                        superseded.add(prior.request_id)
                        followers.setdefault(request.request_id, []).append(prior)
                        followers[request.request_id].extend(followers.pop(prior.request_id, []))
                    latest_intents[key] = request
            heapq.heappush(pending, (int(_urgency(request.operation, request.payload)), sequence, request))

        def pull_request(timeout: float) -> bool:
            """Move one queued request into the heap; False when none arrived.

            Decode errors (an item torn mid-write by a dying producer) are
            counted and skipped: the requester times out and retries.  Reader
            errors are the pipe itself failing: _read_request backs off so
            this cannot spin, and gives up (BrokerQueueUnreadable) once they
            are persistent so the broker is replaced instead of sitting alive
            and deaf.
            """
            nonlocal decode_errors, reader_errors
            request, outcome = self._read_request(timeout)
            if outcome == "empty":
                return False
            if outcome == "reader_error":
                reader_errors += 1
                return False
            if outcome == "decode_error":
                decode_errors += 1
                return True
            enqueue(request)
            return True

        try:
            while not stopping:
                now = time.monotonic()
                reap_in_flight()
                if now - last_heartbeat >= self.heartbeat_seconds:
                    last_heartbeat = now
                    try:
                        self.status_queue.put({
                            "type": "heartbeat", "at_ms": int(time.time() * 1000),
                            "last_success_at_ms": last_success_at_ms,
                            "consecutive_timeouts": consecutive_timeouts,
                            "pending": len(pending), "in_flight": len(in_flight),
                            "dropped_stale": dropped_stale, "decode_errors": decode_errors,
                            "reader_errors": reader_errors,
                            "shard_exposure": self._ledger.snapshot(self._registry.snapshot()),
                        })
                    except Exception:
                        pass
                if now - last_limits >= self.refresh_limits_seconds:
                    try:
                        limits = self._call_bounded(
                            client.get_account_limits,
                            timeout=self.execute_timeout_seconds, label="limits",
                        )
                        write_rate = max(0, limits.write.refill_rate)
                        normal_rate = write_rate * self.write_utilization_limit
                        reserved_rate = write_rate - normal_rate
                        read_rate = max(0, limits.read.refill_rate)
                        normal_read_rate = read_rate * 0.80
                        reserved_read_rate = read_rate - normal_read_rate
                        normal_bucket.reconfigure(normal_rate, max(10.0, normal_rate))
                        reserved_bucket.reconfigure(reserved_rate, max(2.0, reserved_rate))
                        normal_read_bucket.reconfigure(normal_read_rate, max(10.0, normal_read_rate))
                        reserved_read_bucket.reconfigure(reserved_read_rate, max(10.0, reserved_read_rate))
                        self.status_queue.put({
                            "type": "capacity", "at_ms": int(time.time() * 1000), "limits": limits,
                            "normal_write_rate": normal_rate, "reserved_write_rate": reserved_rate,
                        })
                        last_limits = now
                        repeated_429 = 0
                        note_success()
                    except Exception as exc:
                        note_failure(exc)
                        self.status_queue.put({"type": "capacity_error", "at_ms": int(time.time() * 1000), "error": str(exc)})
                        last_limits = now

                if pull_request(0.05 if not pending else 0.0):
                    while len(pending) < 1_024 and pull_request(0.0):
                        pass
                if not pending:
                    continue
                urgency, _seq, request = heapq.heappop(pending)
                if request.request_id in superseded:
                    continue
                if request.operation == "quote_intent":
                    intent = request.payload.get("intent")
                    if isinstance(intent, QuoteIntent):
                        key = (intent.ticker, intent.side)
                        if latest_intents.get(key) is request:
                            latest_intents.pop(key, None)
                if self.is_stale(request):
                    # The waiter timed out already; answering costs nothing and
                    # executing would spend venue capacity on a discarded result.
                    dropped_stale += 1
                    respond_all(request, error=TimeoutError(
                        f"execution broker dropped stale {request.operation} request"
                    ))
                    continue
                cancel_cost = request.operation in {"cancel_order", "cancel_market", "cancel_all", "verify_clear"}
                cost = 2.0 if cancel_cost else 10.0
                write_operation = request.operation in {
                    "quote_intent", "create_order", "amend_order", "decrease_order_to", "cancel_order",
                    "cancel_market", "cancel_all", "verify_clear",
                }
                no_token_operation = request.operation in _NO_VENUE_OPERATIONS
                if no_token_operation:
                    permitted = True
                elif write_operation:
                    permitted = (
                        (reserved_bucket.consume(cost) or normal_bucket.consume(cost))
                        if cancel_cost or urgency <= int(IntentUrgency.RISK_REDUCE)
                        else normal_bucket.consume(cost)
                    )
                else:
                    reserved_read = request.operation in {"limits", "balance", "admission_snapshot"}
                    permitted = (
                        (reserved_read_bucket.consume(cost) or normal_read_bucket.consume(cost))
                        if reserved_read else normal_read_bucket.consume(cost)
                    )
                if not permitted:
                    heapq.heappush(pending, (urgency, _seq, request))
                    time.sleep(0.005)
                    continue
                try:
                    venue_called = False
                    if request.operation == "quiesce_all":
                        all_workers_quiesced = True
                        result = "quiesced"
                    elif request.operation == "quiesce_channel":
                        channel = str(request.payload.get("channel") or "")
                        if not channel or channel == "controller":
                            raise ValueError("a worker channel is required")
                        quiesced_channels.add(channel)
                        result = channel
                    elif request.operation == "resume_channel":
                        channel = str(request.payload.get("channel") or "")
                        if not channel or channel == "controller":
                            raise ValueError("a worker channel is required")
                        quiesced_channels.discard(channel)
                        # A global fence remains authoritative during shutdown.
                        if all_workers_quiesced:
                            raise RuntimeError("all worker channels are quiesced")
                        result = channel
                    elif self._write_blocked(
                        request,
                        all_workers_quiesced=all_workers_quiesced,
                        quiesced_channels=quiesced_channels,
                    ):
                        raise ClientError(f"execution channel {request.response_channel} is quiesced")
                    elif request.operation in _CLEANUP_OPERATIONS:
                        # Detached: answered by reap_in_flight when it completes.
                        future = self._get_executor().submit(self._execute, client, request)
                        in_flight.append((future, request, time.monotonic() + self.cleanup_timeout_seconds))
                        continue
                    else:
                        result, venue_called = self._execute_bounded_traced(client, request)
                    respond_all(request, result=result)
                    repeated_429 = 0
                    if venue_called:
                        note_success()
                    stopping = request.operation == "stop"
                except RateLimitError as exc:
                    repeated_429 += 1
                    respond_all(request, error=exc)
                    if repeated_429 >= 2:
                        last_limits = 0.0
                    if urgency >= int(IntentUrgency.NORMAL_CANCEL_OR_AMEND):
                        time.sleep(min(5.0, 0.25 * (2 ** min(repeated_429, 4))))
                except Exception as exc:
                    note_failure(exc)
                    respond_all(request, error=exc)
        except BrokerQueueUnreadable as exc:
            # Tell the controller why before exiting (capacity_error fails the
            # quoting gate closed on every manager build); the finally block
            # still runs the cleanup, then the dead broker is restarted.
            try:
                self.status_queue.put({"type": "capacity_error", "at_ms": int(time.time() * 1000), "error": str(exc)})
            except Exception:
                pass
            raise
        finally:
            try:
                for future, request, _deadline in in_flight:
                    try:
                        respond_all(request, result=future.result(timeout=self.cleanup_timeout_seconds))
                    except Exception as exc:
                        respond_all(request, error=exc)
                self._cancel_owned(client)
                self._cancel_owned(client)
            finally:
                try:
                    executor = self.__dict__.get("_executor")
                    if executor is not None:
                        executor.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
                import asyncio
                asyncio.run(client.close())
