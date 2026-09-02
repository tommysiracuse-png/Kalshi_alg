"""Tier-2 replay: raw recorded order-book messages -> full book + queue model."""

from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pytest

from adaptors.kalshi import parse_wire_event
from clients.models import (
    AmendOrderRequest,
    CreateOrderRequest,
    OrderBookDelta,
    OrderBookSnapshot,
    PublicTrade,
    StreamReset,
    TickerUpdate,
)
from optimizer import data as data_mod
from optimizer import runner
from optimizer.space import TIER2_BOUNDS, build_space, is_tier2_field
from replay import driver, loader
from replay.driver import evaluate_candidate
from replay.fill_sim import FullBook, QueueFillSimulator

TICKER_A = "T2-ALPHA"
TICKER_B = "T2-BETA"
DAY = "20260901"
HOUR = "10"
HOUR_START_MS = int(datetime(2026, 9, 1, 10, tzinfo=timezone.utc).timestamp() * 1000)
HOUR_END_MS = HOUR_START_MS + 3_600_000


# ----------------------------------------------------------------------
# Synthetic recording
# ----------------------------------------------------------------------


class RecordingBuilder:
    """Builds one hour file with two markets on one connection (shared seq)."""

    def __init__(self) -> None:
        self.lines: List[Tuple[int, dict]] = []  # (recv_ms, raw message)
        self.book_seq = 0  # sid 1 (orderbook channel), shared by both markets
        self.feed_seq = 0  # sid 4 (trade + ticker channel)
        self.trade_counter = 0
        self.stats = {"deltas_a": 0, "deltas_b": 0, "trades_a": 0, "trades_b": 0,
                      "tickers_a": 0, "dropped_a_deltas": 0, "lines_a": 0, "lines_b": 0}

    def _add(self, offset_ms: int, raw: dict) -> None:
        ticker = raw["msg"]["market_ticker"]
        self.stats["lines_a" if ticker == TICKER_A else "lines_b"] += 1
        self.lines.append((HOUR_START_MS + offset_ms, raw))

    def snapshot(self, offset_ms: int, ticker: str, yes: List[Tuple[str, str]], no: List[Tuple[str, str]]) -> None:
        self.book_seq += 1
        self._add(offset_ms, {
            "type": "orderbook_snapshot", "sid": 1, "seq": self.book_seq,
            "msg": {"market_ticker": ticker,
                    "yes_dollars_fp": [list(level) for level in yes],
                    "no_dollars_fp": [list(level) for level in no]},
        })

    def delta(self, offset_ms: int, ticker: str, side: str, price: str, delta_fp: str,
              *, skip_seq: bool = False, dropped: bool = False) -> None:
        self.book_seq += 2 if skip_seq else 1
        self.stats["deltas_a" if ticker == TICKER_A else "deltas_b"] += 1
        if dropped:
            self.stats["dropped_a_deltas"] += 1
        self._add(offset_ms, {
            "type": "orderbook_delta", "sid": 1, "seq": self.book_seq,
            "msg": {"market_ticker": ticker, "side": side, "price_dollars": price,
                    "delta_fp": delta_fp, "ts": (HOUR_START_MS + offset_ms) // 1000},
        })

    def trade(self, offset_ms: int, ticker: str, yes_price: str, count_fp: str, taker_side: str) -> None:
        self.feed_seq += 1
        self.trade_counter += 1
        self.stats["trades_a" if ticker == TICKER_A else "trades_b"] += 1
        no_price = f"{1.0 - float(yes_price):.2f}"
        self._add(offset_ms, {
            "type": "trade", "sid": 4, "seq": self.feed_seq,
            "msg": {"market_ticker": ticker, "trade_id": f"tr-{self.trade_counter}",
                    "yes_price_dollars": yes_price, "no_price_dollars": no_price,
                    "count_fp": count_fp, "taker_side": taker_side,
                    "ts": (HOUR_START_MS + offset_ms) // 1000},
        })

    def ticker(self, offset_ms: int, ticker: str, bid: str, ask: str) -> None:
        self.feed_seq += 1
        if ticker == TICKER_A:
            self.stats["tickers_a"] += 1
        self._add(offset_ms, {
            "type": "ticker", "sid": 4, "seq": self.feed_seq,
            "msg": {"market_ticker": ticker, "yes_bid_dollars": bid, "yes_ask_dollars": ask,
                    "price_dollars": bid, "volume_fp": "1000.00", "open_interest_fp": "500.00",
                    "yes_bid_size_fp": "100.00", "yes_ask_size_fp": "100.00",
                    "ts": (HOUR_START_MS + offset_ms) // 1000},
        })

    def write(self, root: str, conn: int = 0, meta: dict | None = None) -> None:
        hour_dir = os.path.join(root, DAY, HOUR)
        os.makedirs(hour_dir, exist_ok=True)
        if meta is None:
            meta = {"markets": [TICKER_A, TICKER_B], "hour_start_ms": HOUR_START_MS}
        with open(os.path.join(hour_dir, "meta.json"), "w", encoding="utf-8") as handle:
            json.dump(meta, handle)
        with gzip.open(os.path.join(hour_dir, f"conn{conn}.jsonl.gz"), "wt", encoding="utf-8") as handle:
            for recv_ms, raw in self.lines:
                handle.write(json.dumps({"recv_ms": recv_ms, "raw": json.dumps(raw, separators=(",", ":"))}) + "\n")


BOOK_A_YES = [("0.45", "120.00"), ("0.44", "80.00"), ("0.43", "200.00"), ("0.40", "500.00")]
BOOK_A_NO = [("0.51", "150.00"), ("0.50", "90.00"), ("0.49", "300.00"), ("0.45", "400.00")]
BOOK_B_YES = [("0.30", "50.00"), ("0.29", "40.00")]
BOOK_B_NO = [("0.65", "60.00"), ("0.64", "30.00")]
PULL_OFFSET_MS = 30_000
GAP_OFFSET_MS = 60_000
RESYNC_OFFSET_MS = 62_000


def build_recording() -> RecordingBuilder:
    b = RecordingBuilder()
    b.snapshot(0, TICKER_A, BOOK_A_YES, BOOK_A_NO)
    b.snapshot(5, TICKER_B, BOOK_B_YES, BOOK_B_NO)

    def activity(start_ms: int, end_ms: int, *, first_index: int) -> None:
        """Interleaved deltas/trades/tickers for both markets, 250 ms cadence."""
        index = first_index
        offset = start_ms
        while offset < end_ms:
            step = index % 8
            if step == 0:
                b.delta(offset, TICKER_A, "yes", "0.44", "+5.00")
            elif step == 1:
                # execution against the A bid: print first, then its delta
                b.trade(offset, TICKER_A, "0.45", "10.00", "no")
                b.delta(offset + 20, TICKER_A, "yes", "0.45", "-10.00")
            elif step == 2:
                b.delta(offset, TICKER_B, "yes", "0.30", "+2.00")
            elif step == 3:
                # execution lifting the A ask (NO bid at 0.51): delta first, then print
                b.delta(offset, TICKER_A, "no", "0.51", "-8.00")
                b.trade(offset + 20, TICKER_A, "0.49", "8.00", "yes")
            elif step == 4:
                b.delta(offset, TICKER_A, "yes", "0.45", "+10.00")  # replenish
            elif step == 5:
                b.trade(offset, TICKER_B, "0.30", "1.00", "no")
                b.delta(offset + 20, TICKER_B, "yes", "0.30", "-1.00")
            elif step == 6:
                b.delta(offset, TICKER_A, "no", "0.51", "+8.00")  # replenish
            else:
                b.delta(offset, TICKER_A, "yes", "0.43", "-3.00")
                if index % 40 == 7:
                    b.ticker(offset + 30, TICKER_A, "0.45", "0.49")
            index += 1
            offset += 250

    activity(1_000, PULL_OFFSET_MS, first_index=0)
    # The large pull: 100 contracts yanked from the A best bid in one delta.
    b.delta(PULL_OFFSET_MS, TICKER_A, "yes", "0.45", "-100.00")
    b.delta(PULL_OFFSET_MS + 2_000, TICKER_A, "yes", "0.45", "+80.00")
    activity(PULL_OFFSET_MS + 3_000, GAP_OFFSET_MS, first_index=1)
    # Deliberate sequence gap on a B message (the connection lost messages):
    # everything on the subscription is suspect until the next snapshot.
    b.delta(GAP_OFFSET_MS, TICKER_B, "no", "0.65", "+1.00", skip_seq=True)
    b.delta(GAP_OFFSET_MS + 250, TICKER_A, "yes", "0.45", "+7.00", dropped=True)
    b.delta(GAP_OFFSET_MS + 500, TICKER_A, "yes", "0.44", "-2.00", dropped=True)
    b.trade(GAP_OFFSET_MS + 750, TICKER_A, "0.45", "4.00", "no")  # trades still flow
    b.snapshot(RESYNC_OFFSET_MS, TICKER_A, BOOK_A_YES, BOOK_A_NO)
    b.snapshot(RESYNC_OFFSET_MS + 5, TICKER_B, BOOK_B_YES, BOOK_B_NO)
    activity(RESYNC_OFFSET_MS + 1_000, 120_000, first_index=2)
    return b


def independent_ladder(builder: RecordingBuilder, ticker: str, before_ms: int) -> Tuple[Dict[int, int], Dict[int, int]]:
    """Plain add/pop accumulation of the raw messages (no replay code)."""
    yes: Dict[int, int] = {}
    no: Dict[int, int] = {}
    for recv_ms, raw in builder.lines:
        if recv_ms >= before_ms or raw["msg"]["market_ticker"] != ticker:
            continue
        msg = raw["msg"]
        if raw["type"] == "orderbook_snapshot":
            yes = {int(round(float(p) * 10_000)): int(round(float(c) * 100)) for p, c in msg["yes_dollars_fp"]}
            no = {int(round(float(p) * 10_000)): int(round(float(c) * 100)) for p, c in msg["no_dollars_fp"]}
        elif raw["type"] == "orderbook_delta":
            levels = yes if msg["side"] == "yes" else no
            price = int(round(float(msg["price_dollars"]) * 10_000))
            levels[price] = levels.get(price, 0) + int(round(float(msg["delta_fp"]) * 100))
            if levels[price] <= 0:
                del levels[price]
    return yes, no


@pytest.fixture(scope="module")
def recording(tmp_path_factory):
    root = tmp_path_factory.mktemp("record_data")
    builder = build_recording()
    builder.write(str(root))
    loader.clear_tier2_cache()
    return str(root), builder


# ----------------------------------------------------------------------
# 1. parse_wire_event extraction is behaviour-neutral
# ----------------------------------------------------------------------


def test_parse_wire_event_matches_client_event_golden():
    from tests.test_kalshi_adaptor import make_client

    client = make_client()
    samples = [
        {"type": "orderbook_snapshot", "seq": 1, "msg": {"market_ticker": "MKT", "yes_dollars_fp": [["0.40", "2.50"], ["0.39", "1.00"]], "no_dollars_fp": [["0.55", "3.00"]]}},
        {"type": "orderbook_snapshot", "seq": 1, "msg": {"market_ticker": "MKT", "yes": [[40, 2]], "no": [[55, 3]]}},
        {"type": "orderbook_delta", "seq": 2, "msg": {"market_ticker": "MKT", "side": "yes", "price_dollars": "0.40", "delta_fp": "-1.25", "ts": 1_700_000_000}},
        {"type": "orderbook_delta", "seq": 3, "msg": {"market_ticker": "MKT", "side": "no", "price": 55, "delta": 1, "ts": 1_700_000_001}},
        {"type": "trade", "msg": {"market_ticker": "MKT", "trade_id": "t1", "count_fp": "1.00", "yes_price_dollars": "0.40", "taker_side": "no", "ts": 1_700_000_002}},
        {"type": "trade", "msg": {"ticker": "MKT", "trade_id": "t2", "count": 3, "no_price": 60, "taker_side": "yes", "ts": 1_700_000_003}},
        {"type": "ticker", "msg": {"market_ticker": "MKT", "yes_bid_dollars": "0.40", "yes_ask_dollars": "0.42", "price_dollars": "0.41", "volume_fp": "100.00", "open_interest_fp": "50.00", "yes_bid_size_fp": "12.00", "yes_ask_size_fp": "7.00", "ts": 1_700_000_004}},
        {"type": "market_position", "msg": {"ticker": "MKT", "position_fp": "1.25"}},
        {"type": "orderbook_delta", "seq": 4, "msg": {"market_ticker": "MKT", "price_dollars": "0.40", "delta_fp": "1.00", "ts": 1}},  # no side -> None
        {"type": "unknown", "msg": {"market_ticker": "MKT"}},
    ]
    seen_types = set()
    for sample in samples:
        via_method = client._event(sample, "MKT")
        via_function = parse_wire_event(sample, "MKT")
        assert via_method == via_function, sample
        if via_function is not None:
            seen_types.add(type(via_function))
    assert {OrderBookSnapshot, OrderBookDelta, PublicTrade, TickerUpdate} <= seen_types
    snapshot = parse_wire_event(samples[0], "MKT")
    assert snapshot.yes_levels == {4_000: 250, 3_900: 100} and snapshot.no_levels == {5_500: 300}
    delta = parse_wire_event(samples[2], "MKT")
    assert (delta.side, delta.price_units, delta.delta_count_units, delta.timestamp_ms) == ("yes", 4_000, -125, 1_700_000_000_000)


# ----------------------------------------------------------------------
# 2. Loader: discovery, ordering, gap handling, mid-window boot
# ----------------------------------------------------------------------


def test_available_tier2_windows(recording):
    root, builder = recording
    windows = loader.available_tier2_windows(root)
    assert set(windows) == {TICKER_A, TICKER_B}
    a_lines = [recv for recv, raw in builder.lines if raw["msg"]["market_ticker"] == TICKER_A]
    assert windows[TICKER_A] == (min(a_lines), max(a_lines), builder.stats["lines_a"])
    assert windows[TICKER_B][2] == builder.stats["lines_b"]
    assert windows[TICKER_A][2] > windows[TICKER_B][2]


def test_loader_ordering_demux_and_gap_handling(recording):
    root, builder = recording
    events, warnings = loader.load_tier2_events(root, TICKER_A, HOUR_START_MS, HOUR_END_MS)
    assert events, warnings
    assert isinstance(events[0][2], OrderBookSnapshot) and events[0][2].sequence == 1
    assert all(event.market_id == TICKER_A for _ts, _p, event in events)
    timestamps = [ts for ts, _p, _e in events]
    assert timestamps == sorted(timestamps)

    resets = [i for i, (_t, _p, e) in enumerate(events) if isinstance(e, StreamReset)]
    assert len(resets) == 1, warnings
    reset_index = resets[0]
    assert events[reset_index][0] == HOUR_START_MS + GAP_OFFSET_MS
    # After the reset no delta reaches the actor until the re-sync snapshot.
    after = [e for _t, _p, e in events[reset_index + 1:]]
    first_book_event = next(e for e in after if isinstance(e, (OrderBookDelta, OrderBookSnapshot)))
    assert isinstance(first_book_event, OrderBookSnapshot)
    assert first_book_event.sequence == builder.lines[[r["type"] for _ts, r in builder.lines].index("orderbook_snapshot", 2)][1]["seq"]

    deltas = [e for _t, _p, e in events if isinstance(e, OrderBookDelta)]
    trades = [e for _t, _p, e in events if isinstance(e, PublicTrade)]
    tickers = [e for _t, _p, e in events if isinstance(e, TickerUpdate)]
    assert len(deltas) == builder.stats["deltas_a"] - builder.stats["dropped_a_deltas"]
    assert len(trades) == builder.stats["trades_a"]
    assert len(tickers) == builder.stats["tickers_a"]
    assert builder.stats["deltas_a"] >= 200 and builder.stats["trades_a"] >= 50
    assert any("sequence gap" in w for w in warnings)
    assert any("1 sequence gap(s)" in w and f"{builder.stats['dropped_a_deltas']} delta(s) dropped" in w for w in warnings)
    # Wire timestamps (seconds) are kept when sane; the pull delta is real.
    pull = next(d for d in deltas if d.delta_count_units == -10_000)
    assert pull.side == "yes" and pull.price_units == 4_500
    assert abs(pull.timestamp_ms - (HOUR_START_MS + PULL_OFFSET_MS)) < 1_000


def test_loader_caches_and_windows_are_independent(recording):
    root, _builder = recording
    first, _ = loader.load_tier2_events(root, TICKER_A, HOUR_START_MS, HOUR_END_MS)
    second, _ = loader.load_tier2_events(root, TICKER_A, HOUR_START_MS, HOUR_END_MS)
    assert first is second  # per-process cache hit
    short, _ = loader.load_tier2_events(root, TICKER_A, HOUR_START_MS, HOUR_START_MS + 10_000)
    assert short and all(ts < HOUR_START_MS + 10_000 for ts, _p, _e in short)
    other, _ = loader.load_tier2_events(root, TICKER_B, HOUR_START_MS, HOUR_END_MS)
    assert other and all(e.market_id == TICKER_B for _t, _p, e in other)


def test_loader_reads_only_the_connection_meta_json_maps(tmp_path):
    """meta.json's connection map routes the ticker to one file; without it every file is scanned."""
    def build(delta_count: int) -> RecordingBuilder:
        b = RecordingBuilder()
        b.snapshot(0, TICKER_A, BOOK_A_YES, BOOK_A_NO)
        for i in range(delta_count):
            b.delta(1_000 + 250 * i, TICKER_A, "yes", "0.44", "+1.00")
        return b

    def deltas(root: str):
        loader.clear_tier2_cache()
        events, warnings = loader.load_tier2_events(root, TICKER_A, HOUR_START_MS, HOUR_END_MS)
        return sum(isinstance(e, OrderBookDelta) for _t, _p, e in events), warnings

    mapped_root = str(tmp_path / "mapped")
    meta = {"connections": {"conn0": [TICKER_B], "conn1": [TICKER_A]}, "markets": [TICKER_A, TICKER_B]}
    build(3).write(mapped_root, conn=0, meta=meta)  # decoy copy of A on the wrong connection
    build(5).write(mapped_root, conn=1, meta=meta)
    count, warnings = deltas(mapped_root)
    assert count == 5 and not any("also on conn" in w for w in warnings)
    assert loader.meta_connection_for(tmp_path / "mapped" / DAY / HOUR, TICKER_A) == 1
    assert loader.meta_connection_for(tmp_path / "mapped" / DAY / HOUR, "UNLISTED") is None

    unmapped_root = str(tmp_path / "unmapped")
    build(3).write(unmapped_root, conn=0)
    build(5).write(unmapped_root, conn=1)
    count, warnings = deltas(unmapped_root)
    assert count == 3  # first connection seen wins; the duplicate is reported
    assert any("also on conn1" in w for w in warnings)
    assert loader.meta_connection_for(tmp_path / "unmapped" / DAY / HOUR, TICKER_A) is None

    # That full scan indexed the (completed) hour: a later load in this process
    # reads conn0 alone, so the duplicate is never even seen.
    index = loader._OWNER_INDEX[(loader._root_key(unmapped_root), DAY, HOUR)]
    assert index == {TICKER_A: 0}
    events, warnings = loader.load_tier2_events(unmapped_root, TICKER_A, HOUR_START_MS, HOUR_START_MS + 1_600)
    assert sum(isinstance(e, OrderBookDelta) for _t, _p, e in events) == 3
    assert not any("also on conn1" in w for w in warnings)
    # Coverage discovery indexes too, and an indexed hour without the ticker is skipped outright.
    loader.clear_tier2_cache()
    loader.available_tier2_windows(unmapped_root)
    assert loader._OWNER_INDEX[(loader._root_key(unmapped_root), DAY, HOUR)] == {TICKER_A: 0}
    events, warnings = loader.load_tier2_events(unmapped_root, "T2-GAMMA", HOUR_START_MS, HOUR_END_MS)
    assert events == [] and any("no replayable events" in w for w in warnings)


def test_loader_mid_window_boot_snapshot_matches_independent_ladder(recording):
    root, builder = recording
    start_ms = HOUR_START_MS + 20_000
    events, _warnings = loader.load_tier2_events(root, TICKER_A, start_ms, HOUR_END_MS)
    ts, _priority, boot = events[0]
    assert ts == start_ms and isinstance(boot, OrderBookSnapshot)
    expected_yes, expected_no = independent_ladder(builder, TICKER_A, start_ms)
    assert boot.yes_levels == expected_yes
    assert boot.no_levels == expected_no
    assert all(ts >= start_ms for ts, _p, _e in events)
    assert not isinstance(events[1][2], OrderBookSnapshot)


def test_full_book_reconstruction_matches_hand_ladder(recording):
    # Literal hand-computed case.
    book = FullBook()
    book.apply_snapshot(OrderBookSnapshot("M", 1, {4_500: 12_000, 4_400: 8_000}, {5_100: 15_000}))
    for side, price, delta in (("yes", 4_500, 2_000), ("yes", 4_400, -8_000), ("yes", 4_500, -5_000), ("yes", 4_600, 1_000), ("no", 5_200, 700)):
        book.apply_delta(OrderBookDelta("M", None, side, price, delta, 0))
    assert book.yes_levels == {4_500: 9_000, 4_600: 1_000}
    assert book.no_levels == {5_100: 15_000, 5_200: 700}
    assert (book.best_yes_bid(), book.best_yes_ask(), book.mid_units()) == (4_600, 4_800, 4_700)
    assert book.top_levels("yes", 1) == [(4_600, 1_000)]

    # Full recorded stream: replaying every loader event reproduces the
    # independently accumulated ladder at the end of the hour.
    root, builder = recording
    events, _ = loader.load_tier2_events(root, TICKER_A, HOUR_START_MS, HOUR_END_MS)
    replayed = FullBook()
    for _ts, _p, event in events:
        if isinstance(event, OrderBookSnapshot):
            replayed.apply_snapshot(event)
        elif isinstance(event, OrderBookDelta):
            replayed.apply_delta(event)
    expected_yes, expected_no = independent_ladder(builder, TICKER_A, HOUR_END_MS)
    assert replayed.yes_levels == expected_yes
    assert replayed.no_levels == expected_no


# ----------------------------------------------------------------------
# 3. Queue model
# ----------------------------------------------------------------------

T0 = 1_760_000_000_000


def make_sim(share: float = 1.0) -> QueueFillSimulator:
    sim = QueueFillSimulator("MKT", fill_share_fraction=share)
    sim.set_clock(T0)
    sim.apply_snapshot(OrderBookSnapshot("MKT", 1, {4_500: 10_000, 4_400: 5_000}, {5_100: 15_000}))
    return sim


def place(sim: QueueFillSimulator, side: str, price: int, count: int):
    return sim.create_order(CreateOrderRequest(
        market_id="MKT", side=side, price_units=price, count_units=count,
        client_order_id=f"mm:{side}:x", expiration_timestamp_seconds=None,
    ))


def test_queue_fill_only_after_queue_ahead_consumed():
    sim = make_sim()
    ack = place(sim, "yes", 4_500, 5_000)
    order = sim.orders[ack.order_id]
    assert order.queue_ahead_units == 10_000  # the whole recorded level is ahead of us
    assert sim.take_own_deltas() == [("yes", 4_500, 5_000)]

    # 60 contracts trade at our price: all of it consumes the queue ahead.
    assert sim.on_trade(PublicTrade("MKT", "p1", T0 + 100, 4_500, 5_500, 6_000, "no")) == []
    assert order.queue_ahead_units == 4_000
    sim.apply_delta(OrderBookDelta("MKT", 2, "yes", 4_500, -6_000, T0 + 150))  # the print's delta
    assert sim.get_order_queue_position(ack.order_id).queue_position_units == 4_000

    # The remaining 40 ahead trade away: still nothing for us.
    assert sim.on_trade(PublicTrade("MKT", "p2", T0 + 1_000, 4_500, 5_500, 4_000, "no")) == []
    sim.apply_delta(OrderBookDelta("MKT", 3, "yes", 4_500, -4_000, T0 + 1_050))
    assert order.queue_ahead_units == 0

    # Arrivals behind us do not matter; the next print is ours.
    sim.apply_delta(OrderBookDelta("MKT", 4, "yes", 4_500, 3_000, T0 + 2_000))
    events = sim.on_trade(PublicTrade("MKT", "p3", T0 + 3_000, 4_500, 5_500, 3_000, "no"))
    assert len(events) == 1
    fill, update = events[0]
    assert fill.count_units == 3_000 and fill.yes_price_units == 4_500 and fill.fee_units > 0
    assert update.status == "resting" and update.remaining_count_units == 2_000
    assert sim.take_own_deltas() == [("yes", 4_500, -3_000)]
    assert sim.stats["trade_consumed_units"] == 10_000 and sim.stats["at_price_fills"] == 1


def test_queue_share_cap_still_applies():
    sim = make_sim(share=0.5)
    ack = place(sim, "yes", 4_500, 5_000)
    sim.orders[ack.order_id].queue_ahead_units = 0
    (fill, _update), = sim.on_trade(PublicTrade("MKT", "p", T0 + 10, 4_500, 5_500, 4_000, "no"))
    assert fill.count_units == 2_000  # min(remaining 5000, available 4000, 0.5 x 4000)


def test_queue_cancels_advance_position_pro_rata():
    sim = make_sim()
    ack = place(sim, "yes", 4_500, 5_000)
    order = sim.orders[ack.order_id]
    sim.apply_delta(OrderBookDelta("MKT", 2, "yes", 4_500, -5_000, T0 + 100))  # no print: a cancel
    assert sim.get_order_queue_position(ack.order_id).queue_position_units == 10_000  # held in window
    sim.set_clock(T0 + 100 + 1_600)  # window elapsed: attributed to cancels
    assert order.queue_ahead_units == 5_000  # 5000 x 10000/10000
    assert sim.stats["cancel_advance_units"] == 5_000

    sim.apply_delta(OrderBookDelta("MKT", 3, "yes", 4_500, 4_000, T0 + 2_000))  # 40 arrive behind us (depth 90)
    sim.apply_delta(OrderBookDelta("MKT", 4, "yes", 4_500, -3_000, T0 + 2_100))
    sim.set_clock(T0 + 2_100 + 1_600)
    assert order.queue_ahead_units == 5_000 - round(3_000 * 5_000 / 9_000)  # 3333


@pytest.mark.parametrize("delta_first", [True, False])
def test_queue_trade_and_delta_are_one_execution(delta_first):
    sim = make_sim()
    ack = place(sim, "yes", 4_500, 5_000)
    order = sim.orders[ack.order_id]
    delta = OrderBookDelta("MKT", 2, "yes", 4_500, -3_000, T0 + 100)
    trade = PublicTrade("MKT", "p", T0 + 300, 4_500, 5_500, 3_000, "no")
    if delta_first:
        sim.apply_delta(delta)
        assert sim.on_trade(trade) == []
    else:
        assert sim.on_trade(trade) == []
        sim.apply_delta(delta)
    sim.set_clock(T0 + 5_000)  # well past the match window
    assert order.queue_ahead_units == 7_000  # consumed once, never double-counted as a cancel
    assert sim.stats["cancel_advance_units"] == 0


def test_queue_trade_through_fills_fully_and_no_side_symmetry():
    sim = make_sim()
    ack = place(sim, "yes", 4_500, 5_000)
    events = sim.on_trade(PublicTrade("MKT", "sweep", T0 + 10, 4_400, 5_600, 100, "no"))
    (fill, update), = events
    assert fill.count_units == 5_000 and update.status == "executed" and ack.order_id not in sim.orders
    assert sim.stats["trade_through_fills"] == 1

    no_ack = place(sim, "no", 5_100, 2_000)
    assert sim.orders[no_ack.order_id].queue_ahead_units == 15_000
    # Seller hitting the YES bids never touches a resting NO quote.
    assert sim.on_trade(PublicTrade("MKT", "w", T0 + 20, 4_500, 5_500, 400, "no")) == []
    # Buyer lifting through the NO level (yes 0.49 == no 0.51): 150 ahead, 10 for us.
    (fill, _u), = sim.on_trade(PublicTrade("MKT", "lift", T0 + 30, 4_900, 5_100, 16_000, "yes"))
    assert fill.count_units == 1_000 and fill.yes_price_units == 4_900


def test_queue_amend_and_overlay_and_snapshot_clamp():
    sim = make_sim()
    ack = place(sim, "yes", 4_500, 5_000)
    sim.take_own_deltas()
    overlay = sim.overlay_snapshot(OrderBookSnapshot("MKT", 9, {4_500: 10_000}, {5_100: 15_000}))
    assert overlay.yes_levels == {4_500: 15_000} and overlay.no_levels == {5_100: 15_000}

    sim.amend_order(AmendOrderRequest(order_id=ack.order_id, market_id="MKT", side="yes",
                                      new_price_units=4_400, new_total_fillable_count_units=5_000,
                                      previous_client_order_id="mm:yes:x", updated_client_order_id="mm:yes:y"))
    order = sim.orders[ack.order_id]
    assert order.queue_ahead_units == 5_000  # re-queued behind the 4400 level
    assert sim.take_own_deltas() == [("yes", 4_500, -5_000), ("yes", 4_400, 5_000)]

    sim.decrease_order_to(order_id=ack.order_id, new_total_fillable_count_units=3_000)
    assert order.queue_ahead_units == 5_000  # decrease keeps priority
    assert sim.take_own_deltas() == [("yes", 4_400, -2_000)]

    # A fresh snapshot with a thinner level clamps what can still be ahead.
    sim.apply_snapshot(OrderBookSnapshot("MKT", 10, {4_400: 1_200}, {5_100: 15_000}))
    assert order.queue_ahead_units == 1_200

    sim.cancel_order(order_id=ack.order_id)
    assert sim.take_own_deltas() == [("yes", 4_400, -3_000)]
    assert sim.orders == {}


# ----------------------------------------------------------------------
# 4. Search space, driver contract, determinism, runner plumbing
# ----------------------------------------------------------------------


def test_driver_queue_poll_feeds_actor_queue_state():
    """The emulated queue_position_worker pass reaches the actor's guard."""
    import asyncio

    sim = make_sim()
    ack = place(sim, "yes", 4_500, 5_000)

    class State:
        def __init__(self, order_id, status):
            self.order_id = order_id
            self.status = status
            self.last_queue_position_units = None
            self.consecutive_queue_ahead_breaches = 2

        @property
        def has_active_resting_order(self):
            return bool(self.order_id) and self.status == "resting"

    class Actor:
        def __init__(self):
            self.orders = {"yes": State(ack.order_id, "resting"), "no": State(None, None)}
            self.updates = []

        async def process_queue_position_update(self, side, units):
            self.updates.append((side, units))

    actor = Actor()
    asyncio.run(driver._queue_poll(actor, sim))
    assert actor.updates == [("yes", 10_000)]
    assert actor.orders["yes"].last_queue_position_units == 10_000
    assert actor.orders["no"].consecutive_queue_ahead_breaches == 0  # idle side reset, as live
    assert sim.stats["queue_polls"] == 1


def test_build_space_tier2_unpins_depth_queue_pull_group():
    tier1 = build_space()
    tier2 = build_space(2)
    assert tier1.tier == 1 and tier2.tier == 2
    group = {name for name in tier1.excluded if "awaiting Tier-2" in tier1.excluded[name]}
    assert group and all(is_tier2_field(name) for name in group)
    assert not any("awaiting Tier-2" in reason for reason in tier2.excluded.values())
    assert group <= set(tier2.dims)
    assert not (group & set(tier1.dims))
    for name, (low, high) in TIER2_BOUNDS.items():
        dim = tier2.dims[name]
        assert dim.low >= low - 1e-9 and dim.high <= high + 1e-9 and dim.low < dim.high, (name, dim)
        assert dim.low <= dim.default <= dim.high
    for name in ("queue_abandonment_side_cooldown_seconds", "maximum_queue_ahead_contracts_before_abandonment",
                 "maximum_top_level_gap_cents", "orderbook_pull_top_levels_to_track"):
        assert name in tier2.dims
    assert tier2.to_json()["tier"] == 2
    assert set(tier2.dims) - set(tier1.dims) == group


def test_evaluate_candidate_tier2_contract_and_determinism(recording):
    root, _builder = recording
    source = {"tier": 2, "record_root": root}
    before = len(driver._TEMP_DIRS_CREATED)
    first = evaluate_candidate("", TICKER_A, {}, HOUR_START_MS, HOUR_END_MS, seed=3, data_source=source)
    second = evaluate_candidate("", TICKER_A, {}, HOUR_START_MS, HOUR_END_MS, seed=3, data_source=source)
    assert first["error"] is None, first["error"]
    assert first == second
    expected_keys = {"ticker", "error", "decisions", "orders_placed", "fills", "contracts_units", "fees_units",
                     "net_markout_units_by_horizon", "total_net_units", "max_drawdown_units", "settlement_net_units"}
    assert expected_keys <= set(first)
    assert first["tier"] == 2 and first["ticker"] == TICKER_A
    assert first["decisions"] > 0
    assert set(first["net_markout_units_by_horizon"]) == {1_000, 5_000, 30_000, 120_000}
    assert first["total_net_units"] == first["net_markout_units_by_horizon"][30_000]
    assert isinstance(first["tier2_stats"], dict) and first["tier2_stats"]["queue_polls"] >= 0
    assert any("sequence gap" in w for w in first["warnings"])
    assert first["settlement_net_units"] is None  # no history DB -> no settlement anchor
    created = driver._TEMP_DIRS_CREATED[before:]
    assert len(created) == 2 and all(not os.path.exists(path) for path in created)

    # A Tier-2-only parameter changes the run (it is measurable now).
    aggressive = evaluate_candidate("", TICKER_A, {"minimum_top_level_depth_contracts": 0,
                                                   "orderbook_pull_absolute_threshold_contracts": 50},
                                    HOUR_START_MS, HOUR_END_MS, seed=3, data_source=source)
    assert aggressive["error"] is None, aggressive["error"]

    # Unknown tier / missing root are reported, never raised.
    bad = evaluate_candidate("", TICKER_A, {}, HOUR_START_MS, HOUR_END_MS, data_source={"tier": 2})
    assert bad["error"] and "record_root" in bad["error"]
    empty = evaluate_candidate("", "NOPE", {}, HOUR_START_MS, HOUR_END_MS, data_source=source)
    assert empty["error"] and "no replayable events" in empty["error"]


def test_evaluate_candidate_tier2_model_learning_block(recording):
    """Tier 2 drives the same virtual-clock model learning as Tier 1."""
    root, _builder = recording
    source = {"tier": 2, "record_root": root}
    on = evaluate_candidate("", TICKER_A, {}, HOUR_START_MS, HOUR_END_MS, seed=3, data_source=source)
    assert on["error"] is None, on["error"]
    learning = on["learning"]
    assert learning["enabled"] is True
    assert learning["fills_scheduled"] == on["fills"]
    assert learning["markouts_scheduled"] == 4 * on["fills"]
    assert learning["markouts_recorded"] == learning["markouts_observed"]
    if on["fills"]:
        assert learning["markouts_observed"] > 0 and learning["toxicity_stats"]
    assert learning["model_refreshes"] > 0  # one-hour tape, 60s live cadence
    assert learning["fill_prob_attempts_recorded"] == sum(
        value["attempts"] for value in learning["fill_prob_stats"].values()
    )

    off = evaluate_candidate("", TICKER_A, {}, HOUR_START_MS, HOUR_END_MS, seed=3,
                             data_source={**source, "model_learning": False})
    assert off["error"] is None, off["error"]
    assert off["learning"]["enabled"] is False
    assert off["learning"]["toxicity_stats"] == {} and off["learning"]["model_refreshes"] == 0
    assert off == evaluate_candidate("", TICKER_A, {}, HOUR_START_MS, HOUR_END_MS, seed=3,
                                     data_source=source, model_learning=False)


def test_evaluate_candidate_tier1_path_unchanged(recording):
    """Tier-1 callers (no data_source) still get the candle/trade replay."""
    root, _builder = recording
    result = evaluate_candidate("", TICKER_A, {}, HOUR_START_MS, HOUR_END_MS)
    assert result["tier"] == 1 and result["error"]  # no history DB at all -> Tier-1 error, not Tier-2 data


def test_runner_passes_data_source_only_when_configured(monkeypatch):
    calls = []

    def fake_evaluator(history_db, ticker, params, start_ms, end_ms, **kwargs):
        calls.append(dict(kwargs))
        return {"ticker": ticker, "error": None, "total_net_units": 1, "fills": 1,
                "contracts_units": 100, "max_drawdown_units": 0}

    monkeypatch.setattr(runner, "EVALUATOR_OVERRIDE", fake_evaluator)
    monkeypatch.delenv(runner.ENV_DATA_SOURCE, raising=False)
    task = ("h.db", "c1", "stage", "split", {}, [("T", 0, 10)], 0, 0.5)
    runner.evaluate_task(task)
    assert "data_source" not in calls[-1]

    monkeypatch.setenv(runner.ENV_DATA_SOURCE, json.dumps({"tier": 2, "record_root": "rd"}))
    runner.evaluate_task(task)
    assert calls[-1]["data_source"] == {"tier": 2, "record_root": "rd"}

    runner.evaluate_task(task + ({"tier": 2, "record_root": "explicit"},))
    assert calls[-1]["data_source"] == {"tier": 2, "record_root": "explicit"}


def test_data_tier2_market_windows(recording):
    root, builder = recording
    windows = data_mod.load_tier2_market_windows(root)
    assert [w[0] for w in windows] == [TICKER_A, TICKER_B]  # most recorded activity first
    coverage = loader.available_tier2_windows(root)
    assert windows[0] == (TICKER_A,) + coverage[TICKER_A][:2]
    assert data_mod.load_tier2_market_windows(root, limit=1) == windows[:1]
    clamped = data_mod.load_tier2_market_windows(root, from_ms=HOUR_START_MS + 30_000, to_ms=HOUR_START_MS + 40_000)
    assert clamped and all(s == HOUR_START_MS + 30_000 and e == HOUR_START_MS + 40_000 for _t, s, e in clamped)
    assert data_mod.load_tier2_market_windows(root, from_ms=HOUR_END_MS) == []
    assert data_mod.latest_tier2_ms(root) == max(v[1] for v in coverage.values())
    assert data_mod.latest_tier2_ms(os.path.join(root, "missing")) is None


def test_main_tier2_arguments_build_data_source():
    from optimizer import main as main_mod

    args = main_mod.build_arg_parser().parse_args(["--tier", "2", "--record-root", "record_data"])
    assert main_mod._tier2_data_source(args) == {"tier": 2, "record_root": "record_data"}
    default = main_mod.build_arg_parser().parse_args([])
    assert default.tier == 1 and main_mod._tier2_data_source(default) is None
