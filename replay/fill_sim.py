"""Paper-fill simulators for the replay driver.

Tier 1 -- ``FillSimulator`` (candle-synthesized top of book, no queue model).
Matching rules, all in the venue's fixed-point units:

- ``create_order`` is post-only: an order that would execute against the
  current opposite best is rejected with ``PostOnlyCrossError`` exactly like
  the live adaptor surfaces it. Otherwise the order rests.
- A resting YES buy at yes-price P fills only from recorded trades whose
  taker sold into the bids (``taker_side == 'no'``) at yes-price T:
  trade-through (T < P) fills the full remaining size; at-price (T == P)
  fills are capped at ``fill_share_fraction`` x the trade's count (we were
  not in the historical queue). A NO buy at no-price Q behaves symmetrically
  against ``taker_side == 'yes'`` prints at no-price Tn.
- Queue position resets on amend are irrelevant at this tier -- there is no
  queue model; conservatism comes solely from the at-price share cap.
- Maker fees use the quadratic 0.07 * p * (1-p) formula discounted by
  ``scoring.SCORING_FEE_FACTOR`` (mirrors FeeModel.baseline_fee_units).

Tier 2 -- ``QueueFillSimulator`` (recorded ``orderbook_snapshot`` +
``orderbook_delta`` stream, full-depth book, FIFO queue model). Assumptions:

- ``FullBook`` holds the complete recorded ladder on both sides, keyed by
  side-space buy price (NO levels by NO price, as the wire snapshot does).
  It is the *historical* book: our simulated orders are phantom participants
  that never appear in it (``overlay_snapshot`` adds them for the actor's
  view, and ``take_own_deltas`` yields the +/- deltas our own placements,
  cancels and fills would have produced on the live feed).
- Queue position: on placement ``queue_ahead = recorded depth at our price``
  (everything already resting is ahead of us; later arrivals are behind).
  A price change or size increase on amend resets it (cancel/replace
  semantics); decrease-only keeps it, like the venue.
- Cancels ahead of us: a negative delta at our price that is not explained
  by a trade print advances our position pro-rata --
  ``advance = removed * queue_ahead / depth_before_delta`` -- since we cannot
  tell which resting orders were pulled.
- Trades at our price consume ``queue_ahead`` first; only the remainder of
  the print can fill us, further capped by ``fill_share_fraction`` x print
  size (the Tier-1 knob kept as an extra conservative cap; 1.0 is defensible
  once the queue is modeled). A print through our level (T < P) fills the
  full remaining size: the level was swept, so we were too.
- Trade/delta attribution: the venue publishes both a ``trade`` print and the
  matching negative ``orderbook_delta`` for every execution, in either order.
  Negative deltas at our price are held for ``match_window_ms`` (default
  1500 ms of event time); a print at that price within the window claims
  them (they were fills, not cancels); unclaimed removals flush as cancels.
  Symmetrically, a print records a credit that absorbs its later delta.
- On a fresh snapshot (hour rollover, gap recovery) queue positions are
  clamped to the new depth at our price; nothing better is knowable.
- No market impact: the recorded flow is replayed unchanged, so our fills do
  not alter what later takers do. Scores stay comparative, not absolute.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace as dataclass_replace
from typing import Deque, Dict, List, Optional, Tuple

from clients.models import (
    AmendOrderRequest,
    ClientError,
    CreateOrderRequest,
    Fill,
    Order,
    OrderBookDelta,
    OrderBookSnapshot,
    OrderNotFoundError,
    AmendTargetUnavailableError,
    OrderUpdate,
    PostOnlyCrossError,
    PublicTrade,
    QueuePosition,
)

PRICE_SCALE = 10_000
# Event-time window in which a trade print and its negative book delta are
# treated as the same execution (either arrival order).
TRADE_DELTA_MATCH_WINDOW_MS = 1500


@dataclass
class SimOrder:
    order_id: str
    market_id: str
    side: str  # 'yes' or 'no'
    price_units: int  # side-space buy price
    total_fillable_units: int
    filled_units: int = 0
    client_order_id: str = ""
    expiration_time_ms: Optional[int] = None
    queue_ahead_units: int = 0  # Tier-2 only: resting depth ahead of us at our price

    @property
    def remaining_units(self) -> int:
        return max(0, self.total_fillable_units - self.filled_units)


class FillSimulator:
    def __init__(
        self,
        market_id: str,
        *,
        fill_share_fraction: float = 0.5,
        fee_factor: Optional[float] = None,
    ) -> None:
        from replay.scoring import SCORING_FEE_FACTOR

        self.market_id = market_id
        self.fill_share_fraction = float(fill_share_fraction)
        self.fee_factor = SCORING_FEE_FACTOR if fee_factor is None else float(fee_factor)
        self.best_yes_bid_units: Optional[int] = None
        self.best_yes_ask_units: Optional[int] = None
        self.orders: Dict[str, SimOrder] = {}
        self.orders_placed = 0
        self.fills: List[dict] = []  # scoring records
        self._order_counter = 0
        self._fill_counter = 0

    # ------------------------------------------------------------------
    # Book state
    # ------------------------------------------------------------------

    def update_book(self, best_yes_bid_units: Optional[int], best_yes_ask_units: Optional[int]) -> None:
        self.best_yes_bid_units = best_yes_bid_units
        self.best_yes_ask_units = best_yes_ask_units

    def _yes_space_ask(self) -> Optional[int]:
        return self.best_yes_ask_units

    def _no_space_ask(self) -> Optional[int]:
        if self.best_yes_bid_units is None:
            return None
        return PRICE_SCALE - self.best_yes_bid_units

    def _would_cross(self, side: str, price_units: int) -> bool:
        opposite_ask = self._yes_space_ask() if side == "yes" else self._no_space_ask()
        return opposite_ask is not None and price_units >= opposite_ask

    # ------------------------------------------------------------------
    # Order entry (called by ReplayClient)
    # ------------------------------------------------------------------

    def create_order(self, request: CreateOrderRequest) -> Order:
        if request.market_id != self.market_id:
            raise ClientError(f"unknown market {request.market_id!r}")
        if request.action != "buy":
            raise ClientError("replay fill simulator only supports buy quotes")
        if request.side not in ("yes", "no"):
            raise ClientError(f"unknown side {request.side!r}")
        if request.count_units <= 0:
            raise ClientError("count_units must be positive")
        if self._would_cross(request.side, request.price_units):
            raise PostOnlyCrossError(
                f"post-only {request.side} @ {request.price_units} would cross"
            )

        self._order_counter += 1
        order_id = f"sim-{self._order_counter}"
        expiration_ms = (
            int(request.expiration_timestamp_seconds) * 1000
            if request.expiration_timestamp_seconds is not None
            else None
        )
        order = SimOrder(
            order_id=order_id,
            market_id=request.market_id,
            side=request.side,
            price_units=int(request.price_units),
            total_fillable_units=int(request.count_units),
            client_order_id=request.client_order_id,
            expiration_time_ms=expiration_ms,
        )
        self.orders[order_id] = order
        self.orders_placed += 1
        return Order(
            order_id=order_id,
            market_id=order.market_id,
            side=order.side,  # type: ignore[arg-type]
            client_order_id=order.client_order_id,
            status="resting",
            price_units=order.price_units,
            fill_count_units=0,
            remaining_count_units=order.remaining_units,
            expiration_time_ms=order.expiration_time_ms,
        )

    def amend_order(self, request: AmendOrderRequest) -> Order:
        order = self.orders.get(request.order_id)
        if order is None:
            raise AmendTargetUnavailableError(f"order {request.order_id!r} is not resting")
        if self._would_cross(order.side, request.new_price_units):
            raise PostOnlyCrossError(
                f"amend {order.side} @ {request.new_price_units} would cross"
            )
        order.price_units = int(request.new_price_units)
        order.total_fillable_units = int(request.new_total_fillable_count_units)
        order.client_order_id = request.updated_client_order_id
        if order.remaining_units <= 0:
            self.orders.pop(order.order_id, None)
        return Order(
            order_id=order.order_id,
            market_id=order.market_id,
            side=order.side,  # type: ignore[arg-type]
            client_order_id=order.client_order_id,
            status="resting" if order.remaining_units > 0 else "executed",
            price_units=order.price_units,
            fill_count_units=order.filled_units,
            remaining_count_units=order.remaining_units,
            expiration_time_ms=order.expiration_time_ms,
        )

    def cancel_order(self, *, order_id: str) -> Order:
        order = self.orders.pop(order_id, None)
        if order is None:
            raise OrderNotFoundError(f"order {order_id!r} not found")
        return Order(
            order_id=order_id,
            market_id=order.market_id,
            side=order.side,  # type: ignore[arg-type]
            client_order_id=order.client_order_id,
            status="canceled",
            price_units=order.price_units,
            fill_count_units=order.filled_units,
            remaining_count_units=0,
        )

    def decrease_order_to(self, *, order_id: str, new_total_fillable_count_units: int) -> Order:
        order = self.orders.get(order_id)
        if order is None:
            raise OrderNotFoundError(f"order {order_id!r} not found")
        order.total_fillable_units = min(order.total_fillable_units, int(new_total_fillable_count_units))
        if order.remaining_units <= 0:
            self.orders.pop(order_id, None)
        return Order(
            order_id=order_id,
            market_id=order.market_id,
            side=order.side,  # type: ignore[arg-type]
            client_order_id=order.client_order_id,
            status="resting" if order.remaining_units > 0 else "canceled",
            price_units=order.price_units,
            fill_count_units=order.filled_units,
            remaining_count_units=order.remaining_units,
        )

    def resting_orders(self) -> List[Order]:
        return [
            Order(
                order_id=order.order_id,
                market_id=order.market_id,
                side=order.side,  # type: ignore[arg-type]
                client_order_id=order.client_order_id,
                status="resting",
                price_units=order.price_units,
                fill_count_units=order.filled_units,
                remaining_count_units=order.remaining_units,
                expiration_time_ms=order.expiration_time_ms,
            )
            for order in self.orders.values()
        ]

    # ------------------------------------------------------------------
    # Trade matching
    # ------------------------------------------------------------------

    def on_trade(self, trade: PublicTrade) -> List[Tuple[Fill, OrderUpdate]]:
        """Match a recorded print against resting orders; returns venue events."""
        if trade.market_id != self.market_id:
            return []
        events: List[Tuple[Fill, OrderUpdate]] = []
        trade_yes_units = trade.yes_price_units
        trade_no_units = (
            trade.no_price_units
            if trade.no_price_units is not None
            else PRICE_SCALE - trade_yes_units
        )

        for order in list(self.orders.values()):
            if order.remaining_units <= 0:
                continue
            if order.side == "yes":
                if trade.taker_side != "no":
                    continue  # buyer lifting asks cannot fill a resting bid
                trade_side_units = trade_yes_units
            else:
                if trade.taker_side != "yes":
                    continue
                trade_side_units = trade_no_units

            if trade_side_units > order.price_units:
                continue  # print worse than our level: untouched
            if trade_side_units < order.price_units:
                fill_units = order.remaining_units  # traded through our level
            else:
                capped = int(self.fill_share_fraction * trade.count_units)
                fill_units = min(order.remaining_units, capped)
            if fill_units <= 0:
                continue

            from replay.scoring import maker_fee_units

            fee_units = maker_fee_units(order.price_units, fill_units, self.fee_factor)
            order.filled_units += fill_units
            fully_filled = order.remaining_units <= 0
            if fully_filled:
                self.orders.pop(order.order_id, None)

            if order.side == "yes":
                yes_price_units = order.price_units
            else:
                yes_price_units = PRICE_SCALE - order.price_units
            self._fill_counter += 1
            trade_id = f"simfill-{self._fill_counter}-{trade.trade_id}"
            fill_event = Fill(
                market_id=self.market_id,
                order_id=order.order_id,
                trade_id=trade_id,
                timestamp_ms=trade.timestamp_ms,
                count_units=fill_units,
                yes_price_units=yes_price_units,
                no_price_units=PRICE_SCALE - yes_price_units,
                fee_units=fee_units,
                post_position_units=None,
                is_taker=False,
            )
            order_event = OrderUpdate(
                market_id=self.market_id,
                side=order.side,  # type: ignore[arg-type]
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                status="resting" if not fully_filled else "executed",
                fill_count_units=order.filled_units,
                remaining_count_units=order.remaining_units,
                price_units=order.price_units,
                expiration_time_ms=order.expiration_time_ms,
            )
            events.append((fill_event, order_event))
            self.fills.append(
                {
                    "ts_ms": int(trade.timestamp_ms),
                    "side": order.side,
                    "yes_price_units": int(yes_price_units),
                    "count_units": int(fill_units),
                    "fee_units": int(fee_units),
                }
            )
        return events


# ----------------------------------------------------------------------
# Tier 2: full-depth book + FIFO queue model
# ----------------------------------------------------------------------


class FullBook:
    """Complete recorded ladder rebuilt from a snapshot plus deltas.

    Levels are keyed by side-space buy price in price units (YES levels by
    YES price, NO levels by NO price) with count units as values -- the same
    convention as ``OrderBookSnapshot``. Only positive levels are kept.
    """

    def __init__(self) -> None:
        self.yes_levels: Dict[int, int] = {}
        self.no_levels: Dict[int, int] = {}
        self.ready = False
        self.sequence: Optional[int] = None

    def clear(self) -> None:
        self.yes_levels = {}
        self.no_levels = {}
        self.ready = False
        self.sequence = None

    def levels(self, side: str) -> Dict[int, int]:
        return self.yes_levels if side == "yes" else self.no_levels

    def apply_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        self.yes_levels = {int(p): int(c) for p, c in snapshot.yes_levels.items() if int(c) > 0}
        self.no_levels = {int(p): int(c) for p, c in snapshot.no_levels.items() if int(c) > 0}
        self.ready = True
        self.sequence = snapshot.sequence

    def apply_delta(self, delta: OrderBookDelta) -> int:
        """Apply one delta; returns the level's depth *before* the change."""
        levels = self.levels(delta.side)
        price = int(delta.price_units)
        before = int(levels.get(price, 0))
        after = before + int(delta.delta_count_units)
        if after > 0:
            levels[price] = after
        else:
            levels.pop(price, None)
        self.sequence = delta.sequence
        return before

    def depth_at(self, side: str, price_units: int) -> int:
        return int(self.levels(side).get(int(price_units), 0))

    def best_bid(self, side: str) -> Optional[int]:
        levels = self.levels(side)
        return max(levels) if levels else None

    def best_yes_bid(self) -> Optional[int]:
        return self.best_bid("yes")

    def best_yes_ask(self) -> Optional[int]:
        best_no = self.best_bid("no")
        return PRICE_SCALE - best_no if best_no is not None else None

    def mid_units(self) -> Optional[int]:
        bid = self.best_yes_bid()
        ask = self.best_yes_ask()
        if bid is None or ask is None:
            return None
        return (bid + ask) // 2

    def top_levels(self, side: str, count: int) -> List[Tuple[int, int]]:
        levels = self.levels(side)
        prices = sorted(levels, reverse=True)[: max(0, int(count))]
        return [(p, levels[p]) for p in prices]

    def total_depth(self, side: str) -> int:
        return sum(self.levels(side).values())


class QueueFillSimulator(FillSimulator):
    """Tier-2 simulator: real depth for crossing/depth checks and a FIFO queue.

    Drive it with ``set_clock`` (every event), ``apply_snapshot``,
    ``apply_delta``, ``on_trade`` and ``on_stream_reset``; the venue-facing
    order-entry surface is inherited. ``own_deltas`` accumulate the book
    changes our own orders would have produced so the driver can overlay them
    into the actor's view of the book (see module docstring).
    """

    def __init__(
        self,
        market_id: str,
        *,
        fill_share_fraction: float = 0.5,
        fee_factor: Optional[float] = None,
        match_window_ms: int = TRADE_DELTA_MATCH_WINDOW_MS,
    ) -> None:
        super().__init__(market_id, fill_share_fraction=fill_share_fraction, fee_factor=fee_factor)
        self.book = FullBook()
        self.match_window_ms = max(0, int(match_window_ms))
        self._now_ms = 0
        # (side, price) -> FIFO of (ts_ms, units, depth_before) removals not yet
        # attributed to a print; flushed as cancels after the match window.
        self._pending_removals: Dict[Tuple[str, int], Deque[Tuple[int, int, int]]] = {}
        # (side, price) -> FIFO of (ts_ms, units) print quantities whose book
        # delta has not arrived yet.
        self._trade_credits: Dict[Tuple[str, int], Deque[Tuple[int, int]]] = {}
        self.own_deltas: List[Tuple[str, int, int]] = []  # (side, price_units, delta_units)
        self.stats: Dict[str, int] = {
            "cancel_advance_units": 0,
            "trade_consumed_units": 0,
            "at_price_fills": 0,
            "trade_through_fills": 0,
            "queue_polls": 0,
            "own_delta_events": 0,
        }

    # -- clock / book feed ----------------------------------------------

    def set_clock(self, ts_ms: int) -> None:
        self._now_ms = max(self._now_ms, int(ts_ms))
        self._flush_pending_removals()

    def _sync_best(self) -> None:
        self.best_yes_bid_units = self.book.best_yes_bid()
        self.best_yes_ask_units = self.book.best_yes_ask()

    def on_stream_reset(self) -> None:
        self.book.clear()
        self._pending_removals.clear()
        self._trade_credits.clear()
        self._sync_best()

    def apply_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        self.book.apply_snapshot(snapshot)
        self._pending_removals.clear()
        self._trade_credits.clear()
        for order in self.orders.values():
            order.queue_ahead_units = min(
                int(order.queue_ahead_units), self.book.depth_at(order.side, order.price_units)
            )
        self._sync_best()

    def apply_delta(self, delta: OrderBookDelta) -> None:
        depth_before = self.book.apply_delta(delta)
        self._sync_best()
        removed = -int(delta.delta_count_units)
        if removed <= 0:
            return  # arrivals rest behind us: queue position unchanged
        key = (delta.side, int(delta.price_units))
        removed = self._consume(self._trade_credits, key, removed, delta.timestamp_ms)
        if removed <= 0:
            return  # fully explained by a print already matched
        if not any(o.side == key[0] and o.price_units == key[1] for o in self.orders.values()):
            return  # nobody of ours rests there; no queue to advance
        self._pending_removals.setdefault(key, deque()).append(
            (int(delta.timestamp_ms), int(removed), int(depth_before))
        )

    def _consume(self, store, key, units: int, ts_ms: int) -> int:
        """FIFO-consume up to ``units`` from ``store[key]``; returns the leftover."""
        queue = store.get(key)
        if not queue:
            return units
        cutoff = int(ts_ms) - self.match_window_ms
        while queue and units > 0:
            entry = queue[0]
            if entry[0] < cutoff:
                queue.popleft()  # stale: outside the attribution window
                continue
            take = min(units, entry[1])
            units -= take
            if take >= entry[1]:
                queue.popleft()
            else:
                queue[0] = (entry[0], entry[1] - take) + tuple(entry[2:])
        if not queue:
            store.pop(key, None)
        return units

    def _flush_pending_removals(self) -> None:
        cutoff = self._now_ms - self.match_window_ms
        for key in list(self._pending_removals):
            queue = self._pending_removals[key]
            while queue and queue[0][0] < cutoff:
                _ts, units, depth_before = queue.popleft()
                self._apply_cancel(key, units, depth_before)
            if not queue:
                self._pending_removals.pop(key, None)

    def _apply_cancel(self, key: Tuple[str, int], units: int, depth_before: int) -> None:
        if depth_before <= 0 or units <= 0:
            return
        for order in self.orders.values():
            if order.side != key[0] or order.price_units != key[1] or order.queue_ahead_units <= 0:
                continue
            advance = int(round(units * order.queue_ahead_units / float(depth_before)))
            advance = min(order.queue_ahead_units, max(0, advance))
            order.queue_ahead_units -= advance
            self.stats["cancel_advance_units"] += advance

    # -- order entry (queue bookkeeping + own-delta overlay) --------------

    def create_order(self, request: CreateOrderRequest) -> Order:
        ack = super().create_order(request)
        order = self.orders[ack.order_id]
        order.queue_ahead_units = self.book.depth_at(order.side, order.price_units)
        self._own_delta(order.side, order.price_units, order.remaining_units)
        return ack

    def amend_order(self, request: AmendOrderRequest) -> Order:
        before = self.orders.get(request.order_id)
        old_price = before.price_units if before is not None else None
        old_remaining = before.remaining_units if before is not None else 0
        old_total = before.total_fillable_units if before is not None else 0
        ack = super().amend_order(request)
        if before is None:
            return ack
        moved = old_price != before.price_units
        if moved or before.total_fillable_units > old_total:
            before.queue_ahead_units = self.book.depth_at(before.side, before.price_units)
        self._own_delta(before.side, old_price, -old_remaining)
        self._own_delta(before.side, before.price_units, before.remaining_units)
        return ack

    def cancel_order(self, *, order_id: str) -> Order:
        order = self.orders.get(order_id)
        if order is not None:
            self._own_delta(order.side, order.price_units, -order.remaining_units)
        return super().cancel_order(order_id=order_id)

    def decrease_order_to(self, *, order_id: str, new_total_fillable_count_units: int) -> Order:
        order = self.orders.get(order_id)
        old_remaining = order.remaining_units if order is not None else 0
        ack = super().decrease_order_to(
            order_id=order_id, new_total_fillable_count_units=new_total_fillable_count_units
        )
        if order is not None:
            self._own_delta(order.side, order.price_units, order.remaining_units - old_remaining)
        return ack

    def get_order_queue_position(self, order_id: str) -> QueuePosition:
        order = self.orders.get(order_id)
        if order is None:
            raise OrderNotFoundError(f"order {order_id!r} not found")
        self._flush_pending_removals()
        self.stats["queue_polls"] += 1
        return QueuePosition(order_id, int(order.queue_ahead_units))

    def _own_delta(self, side: str, price_units: Optional[int], delta_units: int) -> None:
        if price_units is None or delta_units == 0:
            return
        self.own_deltas.append((side, int(price_units), int(delta_units)))
        self.stats["own_delta_events"] += 1

    def take_own_deltas(self) -> List[Tuple[str, int, int]]:
        out = self.own_deltas
        self.own_deltas = []
        return out

    def overlay_snapshot(self, snapshot: OrderBookSnapshot) -> OrderBookSnapshot:
        """Recorded snapshot plus our resting orders (what the live feed shows)."""
        if not self.orders:
            return snapshot
        yes_levels = dict(snapshot.yes_levels)
        no_levels = dict(snapshot.no_levels)
        for order in self.orders.values():
            if order.remaining_units <= 0:
                continue
            levels = yes_levels if order.side == "yes" else no_levels
            levels[order.price_units] = levels.get(order.price_units, 0) + order.remaining_units
        return dataclass_replace(snapshot, yes_levels=yes_levels, no_levels=no_levels)

    # -- trade matching with the queue model --------------------------------

    def on_trade(self, trade: PublicTrade) -> List[Tuple[Fill, OrderUpdate]]:
        if trade.market_id != self.market_id:
            return []
        events: List[Tuple[Fill, OrderUpdate]] = []
        trade_yes_units = int(trade.yes_price_units)
        trade_no_units = (
            int(trade.no_price_units)
            if trade.no_price_units is not None
            else PRICE_SCALE - trade_yes_units
        )
        count_units = int(trade.count_units)
        credited: set = set()

        for order in list(self.orders.values()):
            if order.remaining_units <= 0:
                continue
            if order.side == "yes":
                if trade.taker_side != "no":
                    continue
                trade_side_units = trade_yes_units
            else:
                if trade.taker_side != "yes":
                    continue
                trade_side_units = trade_no_units
            if trade_side_units > order.price_units:
                continue

            if trade_side_units < order.price_units:
                # Swept through our level: everything ahead of us is gone.
                order.queue_ahead_units = 0
                fill_units = order.remaining_units
                self.stats["trade_through_fills"] += 1
            else:
                key = (order.side, order.price_units)
                if key not in credited:
                    credited.add(key)
                    leftover = self._consume(self._pending_removals, key, count_units, trade.timestamp_ms)
                    if leftover > 0:
                        self._trade_credits.setdefault(key, deque()).append(
                            (int(trade.timestamp_ms), int(leftover))
                        )
                consumed = min(count_units, order.queue_ahead_units)
                order.queue_ahead_units -= consumed
                self.stats["trade_consumed_units"] += consumed
                available = count_units - consumed
                capped = int(self.fill_share_fraction * count_units)
                fill_units = min(order.remaining_units, available, capped)
                if fill_units > 0:
                    self.stats["at_price_fills"] += 1
            if fill_units <= 0:
                continue

            from replay.scoring import maker_fee_units

            fee_units = maker_fee_units(order.price_units, fill_units, self.fee_factor)
            order.filled_units += fill_units
            fully_filled = order.remaining_units <= 0
            if fully_filled:
                self.orders.pop(order.order_id, None)
            self._own_delta(order.side, order.price_units, -fill_units)

            yes_price_units = order.price_units if order.side == "yes" else PRICE_SCALE - order.price_units
            self._fill_counter += 1
            trade_id = f"simfill-{self._fill_counter}-{trade.trade_id}"
            fill_event = Fill(
                market_id=self.market_id,
                order_id=order.order_id,
                trade_id=trade_id,
                timestamp_ms=trade.timestamp_ms,
                count_units=fill_units,
                yes_price_units=yes_price_units,
                no_price_units=PRICE_SCALE - yes_price_units,
                fee_units=fee_units,
                post_position_units=None,
                is_taker=False,
            )
            order_event = OrderUpdate(
                market_id=self.market_id,
                side=order.side,  # type: ignore[arg-type]
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                status="resting" if not fully_filled else "executed",
                fill_count_units=order.filled_units,
                remaining_count_units=order.remaining_units,
                price_units=order.price_units,
                expiration_time_ms=order.expiration_time_ms,
            )
            events.append((fill_event, order_event))
            self.fills.append(
                {
                    "ts_ms": int(trade.timestamp_ms),
                    "side": order.side,
                    "yes_price_units": int(yes_price_units),
                    "count_units": int(fill_units),
                    "fee_units": int(fee_units),
                }
            )
        return events
