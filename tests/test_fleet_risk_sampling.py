"""Risk samples are rate-capped so the time-bounded move window really spans its window in busy markets."""

from collections import deque
from types import SimpleNamespace

from clients.models import OrderBookDelta, OrderUpdate
from fleet_runtime import worker
from fleet_runtime.risk import RiskSample, evaluate_risk


def _actor(yes_bid_units: int, no_bid_units: int):
    return SimpleNamespace(book_ready=True, book_yes={yes_bid_units: 100}, book_no={no_bid_units: 100})


def _delta(market_id: str = "MKT"):
    return OrderBookDelta(market_id=market_id, sequence=None, side="yes", price_units=5000, delta_count_units=100, timestamp_ms=0)


def test_busy_market_samples_are_capped_to_ten_per_second():
    window: deque = deque(maxlen=worker.RISK_WINDOW_MAX_SAMPLES)
    last_event: dict = {}
    actor = _actor(4000, 5500)
    appended = 0
    for i in range(2_000):  # 2,000 book events in one second (worse than 15-minute crypto)
        appended += worker.observe_stream_event(_delta(), actor, window, last_event, now_ms=1_000_000 + i // 2)
    assert 9 <= appended <= 12
    assert len(window) == appended
    assert last_event["MKT"] == 1_000_000 + 999  # liveness still stamped by every event


def test_capped_window_still_covers_the_full_move_window():
    window: deque = deque(maxlen=worker.RISK_WINDOW_MAX_SAMPLES)
    last_event: dict = {}
    start = 5_000_000
    # 300 s of a market that emits 50 events/s: the old 1,000-sample deque held 20 s.
    for t in range(0, 300_000, 20):
        yes = 4000 if t < 150_000 else 5000  # a 10c jump halfway through
        worker.observe_stream_event(_delta(), _actor(yes, 10_000 - yes - 200), window, last_event, now_ms=start + t)
    assert len(window) <= worker.RISK_WINDOW_MAX_SAMPLES
    oldest = window[0].timestamp_ms
    assert start + 300_000 - oldest >= 250_000  # the deque now spans minutes, not seconds
    decision = evaluate_risk(window, now_ms=start + 300_000, position_units=0, has_resting_orders=False,
                             window_ms=120_000, elevated_move_units=750, extreme_move_units=2_000)
    assert decision.mode == "normal"  # the jump is older than the 120 s window
    decision = evaluate_risk(window, now_ms=start + 160_000, position_units=0, has_resting_orders=False,
                             window_ms=120_000, elevated_move_units=750, extreme_move_units=2_000)
    assert decision.mode == "reduction_only"  # inside the window the 10c jump still counts


def test_own_order_events_never_add_samples_even_when_the_cap_would_allow():
    window: deque = deque(maxlen=worker.RISK_WINDOW_MAX_SAMPLES)
    last_event: dict = {}
    update = OrderUpdate(market_id="MKT", side="yes", order_id="o1", client_order_id="tob:yes:1", status="resting",
                         price_units=4000, remaining_count_units=100)
    assert worker.observe_stream_event(update, _actor(4000, 5500), window, last_event, now_ms=1_000) is False
    assert not window and "MKT" not in last_event
