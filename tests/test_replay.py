import hashlib
import json
import os
import sqlite3
from types import SimpleNamespace

import pytest

from clients.models import CreateOrderRequest, PostOnlyCrossError, PublicTrade
from replay import driver
from replay.clock import ReplayClock
from replay.driver import evaluate_candidate
from replay.fill_sim import FillSimulator
from replay.learning import ReplayModelLearner

TICKER = "RPLY-TEST"
BASE_TS_MS = 1_700_000_000_000
MINUTES = 30
WINDOW_START_MS = BASE_TS_MS
WINDOW_END_MS = BASE_TS_MS + MINUTES * 60_000
CLOSE_TS_MS = WINDOW_END_MS - 30_000

HISTORY_SCHEMA = """
        CREATE TABLE markets(ticker TEXT PRIMARY KEY, series TEXT, title TEXT,
            status TEXT, close_ts_ms INTEGER, result TEXT,
            volume_24h_units INTEGER, oi_units INTEGER, meta_json TEXT);
        CREATE TABLE candles(ticker TEXT, ts_ms INTEGER,
            price_open_units INTEGER, price_high_units INTEGER,
            price_low_units INTEGER, price_close_units INTEGER,
            yes_bid_open_units INTEGER, yes_bid_high_units INTEGER,
            yes_bid_low_units INTEGER, yes_bid_close_units INTEGER,
            yes_ask_open_units INTEGER, yes_ask_high_units INTEGER,
            yes_ask_low_units INTEGER, yes_ask_close_units INTEGER,
            volume_units INTEGER, oi_units INTEGER,
            PRIMARY KEY(ticker, ts_ms));
        CREATE TABLE trades(ticker TEXT, trade_id TEXT, ts_ms INTEGER,
            yes_price_units INTEGER, no_price_units INTEGER,
            count_units INTEGER, taker_side TEXT,
            PRIMARY KEY(ticker, trade_id));
"""


def build_history(path: str) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(HISTORY_SCHEMA)
    connection.execute(
        "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?)",
        (TICKER, "RPLY", "Replay test market", "active", CLOSE_TS_MS, "yes",
         1_000_000, 500_000, "{}"),
    )
    # Stable 2-cent spread: yes bid 49c, yes ask 51c, price 50c.
    for index in range(MINUTES):
        period_end_ms = BASE_TS_MS + (index + 1) * 60_000
        connection.execute(
            "INSERT INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (TICKER, period_end_ms,
             5_000, 5_000, 5_000, 5_000,
             4_900, 4_900, 4_900, 4_900,
             5_100, 5_100, 5_100, 5_100,
             10_000, 500_000),
        )
    # Trades on both sides of the book every 20 seconds.
    trade_index = 0
    ts_ms = BASE_TS_MS + 30_000
    while ts_ms < WINDOW_END_MS - 60_000:
        if trade_index % 2 == 0:
            yes_price, taker = 4_900, "no"    # seller hitting bids
        else:
            yes_price, taker = 5_100, "yes"   # buyer lifting asks
        connection.execute(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
            (TICKER, f"t{trade_index:05d}", ts_ms, yes_price,
             10_000 - yes_price, 400, taker),
        )
        trade_index += 1
        ts_ms += 20_000
    connection.commit()
    connection.close()


@pytest.fixture()
def history_db(tmp_path):
    path = str(tmp_path / "history.sqlite3")
    build_history(path)
    return path


ADVERSE_TICKER = "RPLY-ADVERSE"
ADVERSE_HALF_SPREAD_UNITS = 500   # 45c / 55c book: wide enough that quoting has edge
ADVERSE_DROP_UNITS = 300          # 3c adverse move right after each fill


def build_adverse_history(path: str) -> None:
    """Sawtooth book where every fill is followed by an adverse move.

    Sub-steps of each minute (loader offsets :00/:15/:30/:45) carry the book
    hi, lo, lo, hi. A seller hits the hi bid at :14 (fills our YES bid, then
    the book drops 3c at :15); a buyer lifts the lo ask at :44 (fills our NO
    bid, then the book jumps 3c at :45). So the 1s/5s markouts of every fill
    are adverse and the 30s markout is flat.
    """
    connection = sqlite3.connect(path)
    connection.executescript(HISTORY_SCHEMA)
    connection.execute(
        "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?)",
        (ADVERSE_TICKER, "RPLY", "Adverse replay market", "active", CLOSE_TS_MS, "yes",
         1_000_000, 500_000, "{}"),
    )
    hi = (5_000 - ADVERSE_HALF_SPREAD_UNITS, 5_000 + ADVERSE_HALF_SPREAD_UNITS)
    lo = (hi[0] - ADVERSE_DROP_UNITS, hi[1] - ADVERSE_DROP_UNITS)
    levels = {"hi": hi, "lo": lo}
    phases = ("hi", "lo", "lo", "hi")
    for index in range(MINUTES):
        bids = [levels[phase][0] for phase in phases]
        asks = [levels[phase][1] for phase in phases]
        mids = [(bid + ask) // 2 for bid, ask in zip(bids, asks)]
        connection.execute(
            "INSERT INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ADVERSE_TICKER, BASE_TS_MS + (index + 1) * 60_000,
             *mids, *bids, *asks, 10_000, 500_000),
        )
    trade_index = 0
    for index in range(MINUTES - 1):
        for offset_ms, taker, level in ((14_000, "no", "hi"), (44_000, "yes", "lo")):
            bid, ask = levels[level]
            yes_price = bid if taker == "no" else ask
            connection.execute(
                "INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                (ADVERSE_TICKER, f"a{trade_index:05d}", BASE_TS_MS + index * 60_000 + offset_ms,
                 yes_price, 10_000 - yes_price, 2_000, taker),
            )
            trade_index += 1
    connection.commit()
    connection.close()


@pytest.fixture()
def adverse_history_db(tmp_path):
    path = str(tmp_path / "adverse.sqlite3")
    build_adverse_history(path)
    return path


def _result_hash(result: dict) -> str:
    return hashlib.sha256(json.dumps(result, sort_keys=True).encode("utf-8")).hexdigest()


def test_determinism_identical_results(history_db):
    first = evaluate_candidate(history_db, TICKER, {}, WINDOW_START_MS, WINDOW_END_MS, seed=7)
    second = evaluate_candidate(history_db, TICKER, {}, WINDOW_START_MS, WINDOW_END_MS, seed=7)
    assert first["error"] is None, first["error"]
    assert first == second


def test_fill_sim_post_only_cross_rejected():
    sim = FillSimulator("MKT", fill_share_fraction=0.5)
    sim.update_book(4_900, 5_100)
    with pytest.raises(PostOnlyCrossError):
        sim.create_order(CreateOrderRequest(
            market_id="MKT", side="yes", price_units=5_100, count_units=100,
            client_order_id="mm:yes:x", expiration_timestamp_seconds=None,
        ))
    # NO-space ask is 10000 - best yes bid = 5100.
    with pytest.raises(PostOnlyCrossError):
        sim.create_order(CreateOrderRequest(
            market_id="MKT", side="no", price_units=5_100, count_units=100,
            client_order_id="mm:no:x", expiration_timestamp_seconds=None,
        ))
    assert sim.orders_placed == 0


def test_fill_sim_at_price_cap_trade_through_and_wrong_side():
    sim = FillSimulator("MKT", fill_share_fraction=0.5)
    sim.update_book(4_900, 5_100)
    ack = sim.create_order(CreateOrderRequest(
        market_id="MKT", side="yes", price_units=5_000, count_units=1_000,
        client_order_id="mm:yes:a", expiration_timestamp_seconds=None,
    ))
    assert ack.status == "resting"

    # Wrong-side print (buyer lifting asks) never fills a resting yes bid.
    events = sim.on_trade(PublicTrade("MKT", "w1", 1_000, 5_000, 5_000, 600, "yes"))
    assert events == []

    # At-price print: capped at fill_share_fraction x trade size.
    events = sim.on_trade(PublicTrade("MKT", "a1", 2_000, 5_000, 5_000, 600, "no"))
    assert len(events) == 1
    fill_event, order_event = events[0]
    assert fill_event.count_units == 300  # int(0.5 * 600)
    assert order_event.status == "resting"
    assert order_event.remaining_count_units == 700

    # Trade-through print: fills the full remaining size.
    events = sim.on_trade(PublicTrade("MKT", "b1", 3_000, 4_900, 5_100, 100, "no"))
    assert len(events) == 1
    fill_event, order_event = events[0]
    assert fill_event.count_units == 700
    assert fill_event.yes_price_units == 5_000  # filled at our price
    assert order_event.status == "executed"
    assert sim.orders == {}


def test_fill_sim_no_side_symmetry():
    sim = FillSimulator("MKT", fill_share_fraction=0.5)
    sim.update_book(4_900, 5_100)
    sim.create_order(CreateOrderRequest(
        market_id="MKT", side="no", price_units=5_000, count_units=200,
        client_order_id="mm:no:a", expiration_timestamp_seconds=None,
    ))
    # Seller hitting bids cannot fill a resting NO quote.
    assert sim.on_trade(PublicTrade("MKT", "w2", 1_000, 4_900, 5_100, 400, "no")) == []
    # Buyer lifting asks through our NO level (trade no-price 4900 < 5000).
    events = sim.on_trade(PublicTrade("MKT", "c1", 2_000, 5_100, 4_900, 400, "yes"))
    assert len(events) == 1
    fill_event, _ = events[0]
    assert fill_event.count_units == 200
    assert fill_event.yes_price_units == 5_000  # 10000 - our NO price


def test_sanity_default_settings(history_db):
    result = evaluate_candidate(history_db, TICKER, {}, WINDOW_START_MS, WINDOW_END_MS)
    assert result["error"] is None, result["error"]
    assert result["ticker"] == TICKER
    assert result["decisions"] > 0
    assert set(result["net_markout_units_by_horizon"]) == {1_000, 5_000, 30_000, 120_000}
    assert result["total_net_units"] == result["net_markout_units_by_horizon"][30_000]
    assert result["max_drawdown_units"] >= 0
    if result["fills"] > 0:
        assert result["orders_placed"] > 0
        assert result["contracts_units"] > 0
        assert result["fees_units"] >= 0
        assert result["settlement_net_units"] is not None  # result known, close in window


def test_candidate_isolation_and_temp_cleanup(history_db):
    before = len(driver._TEMP_DIRS_CREATED)
    evaluate_candidate(history_db, TICKER, {"aggressive_improvement_ticks_when_spread_is_wide": 0},
                       WINDOW_START_MS, WINDOW_END_MS)
    evaluate_candidate(history_db, TICKER, {"minimum_milliseconds_between_requotes": 500},
                       WINDOW_START_MS, WINDOW_END_MS)
    created = driver._TEMP_DIRS_CREATED[before:]
    assert len(created) == 2
    assert created[0] != created[1]
    for temp_dir in created:
        assert not os.path.exists(temp_dir)


# ----------------------------------------------------------------------
# Model learning on the virtual clock (replay/learning.py)
# ----------------------------------------------------------------------


def _prior_toxicity_units() -> int:
    import session_config

    return int(session_config.default_session_configuration()["bot"]["default_toxicity_cents"]) * 100


def test_learning_toxicity_rises_above_prior_after_adverse_fills(adverse_history_db):
    result = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS, seed=7)
    assert result["error"] is None, result["error"]
    learning = result["learning"]
    assert learning["enabled"] is True
    assert result["fills"] > 0
    # Every simulated fill re-entered the actor and scheduled its markouts.
    assert learning["fills_scheduled"] == result["fills"]
    assert learning["markouts_scheduled"] == learning["fills_scheduled"] * 4  # 1/5/30/120 s
    assert learning["markouts_observed"] > 0
    assert learning["markouts_skipped_no_book"] == 0
    # Observations reached the per-candidate telemetry DB through the same
    # record_markout call the live bot makes...
    assert learning["markouts_recorded"] == learning["markouts_observed"]
    # ...and the periodic telemetry-backed refresh ran on the virtual clock.
    assert learning["model_refreshes"] > 0

    stats = learning["toxicity_stats"]
    assert stats, "toxicity model never left the prior"
    horizons = {int(key.split("|", 1)[0]) for key in stats}
    assert {1_000, 5_000, 30_000} <= horizons
    yes_5s = {key: value for key, value in stats.items()
              if key.startswith("5000|yes|")}
    assert yes_5s, "no YES bucket observed at the 5s horizon"
    # The 5s markout of every YES fill is adverse (3c drop), so the bucket
    # estimate leaves the unknown/prior state and exceeds the prior.
    prior_units = _prior_toxicity_units()
    assert any(value["observations"] >= 3 and value["ewma_adverse_units"] > prior_units
               for value in yes_5s.values()), yes_5s
    # Signed drift (bucket pessimism input) is negative for those buckets.
    assert any(value["ewma_signed_drift_units"] < 0 for value in yes_5s.values())


def test_learning_fill_probability_attempts_recorded(adverse_history_db):
    result = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS, seed=7)
    assert result["error"] is None, result["error"]
    learning = result["learning"]
    assert learning["fill_prob_attempts_recorded"] > 0
    stats = learning["fill_prob_stats"]
    assert stats
    # In-memory stats are rebuilt from the temp telemetry DB by the emulated
    # refresh (bootstrap_from_telemetry(limit=10_000)): they must agree.
    assert sum(value["attempts"] for value in stats.values()) == learning["fill_prob_attempts_recorded"]
    assert any(value["fills_30s"] > 0 for value in stats.values())
    assert all(0 <= value["fills_30s"] <= value["attempts"] for value in stats.values())


def test_learning_determinism_hash_unchanged(adverse_history_db):
    first = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS, seed=7)
    second = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS, seed=7)
    assert first["error"] is None, first["error"]
    assert first["learning"]["markouts_observed"] > 0
    assert first == second
    assert _result_hash(first) == _result_hash(second)


def test_learning_disabled_reproduces_constant_prior(adverse_history_db):
    off = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS,
                             seed=7, model_learning=False)
    off_again = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS,
                                   seed=7, model_learning=False)
    assert off["error"] is None, off["error"]
    assert off == off_again
    learning = off["learning"]
    assert learning["enabled"] is False
    # Old behaviour: no markout ever reaches the toxicity model, so every
    # decision is priced off default_toxicity_cents.
    assert learning["toxicity_stats"] == {}
    assert learning["markouts_observed"] == 0
    assert learning["markouts_recorded"] == 0
    assert learning["model_refreshes"] == 0
    assert off["fills"] > 0

    on = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS, seed=7)
    assert on["learning"]["toxicity_stats"]
    # Learning changes what the actor does on this tape (the A/B is real).
    assert (on["fills"], on["total_net_units"]) != (off["fills"], off["total_net_units"])

    # The switch is also reachable through data_source (optimizer env path).
    via_source = evaluate_candidate(adverse_history_db, ADVERSE_TICKER, {}, WINDOW_START_MS, WINDOW_END_MS,
                                    seed=7, data_source={"tier": 1, "model_learning": False})
    assert via_source == off
    assert driver.resolve_data_source({"tier": 1, "model_learning": 0, "model_refresh_ms": 0}) == {
        "tier": 1, "model_learning": False, "model_refresh_ms": 0,
    }
    assert driver.resolve_data_source({"tier": 1}) == {"tier": 1}


class _FakeLearningActor:
    """Minimal stand-in exposing the surface ReplayModelLearner touches."""

    def __init__(self, clock: ReplayClock) -> None:
        self.clock = clock
        self.settings = SimpleNamespace(markout_horizons_seconds=(1, 5), model_refresh_interval_seconds=50)
        self.market = SimpleNamespace(ticker="MKT")
        self.mid_yes_units = 5_000
        self.observed = []
        self.recorded = []
        self.refreshes = []
        self.session_markouts_by_horizon = {}
        self.fair_value_engine = SimpleNamespace(
            estimate=lambda context, update_state=True: SimpleNamespace(
                fair_yes_units=self.mid_yes_units, fair_no_units=10_000 - self.mid_yes_units
            )
        )
        self.toxicity_model = SimpleNamespace(
            stats={},
            record_markout=self._record_markout,
            bootstrap_from_telemetry=lambda limit: self.refreshes.append(("toxicity", limit)),
        )
        self.fill_probability_model = SimpleNamespace(
            stats={},
            bootstrap_from_telemetry=lambda limit: self.refreshes.append(("fill_prob", limit)),
        )
        self.telemetry_store = SimpleNamespace(
            record_markout=lambda **kwargs: self.recorded.append(kwargs),
            load_recent_markouts=lambda limit: list(self.recorded),
            load_recent_fill_prob_attempts=lambda limit: [],
        )

    def build_market_context(self):
        return SimpleNamespace(mid_yes_units=self.mid_yes_units)

    def _record_markout(self, *, side, context, horizon_ms, adverse_units, signed_drift_units):
        assert context == "ctx"
        self.observed.append((self.clock.now_ms(), side, horizon_ms, adverse_units, signed_drift_units))
        return "bucket"


def test_learner_evaluates_markouts_on_virtual_clock():
    clock = ReplayClock(BASE_TS_MS)
    actor = _FakeLearningActor(clock)
    learner = ReplayModelLearner(actor, clock)
    assert learner.horizons_ms == [1_000, 5_000, 30_000, 120_000]  # live union with MARKOUT_HORIZONS_MS
    assert learner.refresh_interval_ms == 60_000  # max(60, model_refresh_interval_seconds)

    learner.schedule_fill_markouts(fill_key="f1", side="yes", fill_price_units=4_900, fill_size_units=100,
                                   fill_fee_units=10, fill_timestamp_ms=BASE_TS_MS, fill_context="ctx")
    learner.advance_to(BASE_TS_MS + 500)
    assert actor.observed == [] and clock.now_ms() == BASE_TS_MS + 500

    # An observation whose target equals the incoming event time waits for
    # that event (same inclusive semantics as scoring.MidPath.mid_at).
    actor.mid_yes_units = 4_800
    learner.advance_to(BASE_TS_MS + 1_000)
    assert actor.observed == []
    learner.advance_to(BASE_TS_MS + 3_000)
    assert actor.observed == [(BASE_TS_MS + 1_000, "yes", 1_000, 100, -100)]
    assert actor.recorded[-1]["ts_ms"] == BASE_TS_MS + 1_000  # clock sat at the target while observing
    assert clock.now_ms() == BASE_TS_MS + 3_000

    # Favourable move: adverse is floored at 0, signed drift stays signed.
    actor.mid_yes_units = 5_100
    learner.advance_to(BASE_TS_MS + 70_000)
    assert actor.observed[1] == (BASE_TS_MS + 5_000, "yes", 5_000, 0, 200)
    # The 30s markout fired at exactly +30s on the way, then the emulated
    # model refresh ran once the clock crossed startup + 60s.
    assert [item[0] for item in actor.observed] == [BASE_TS_MS + 1_000, BASE_TS_MS + 5_000, BASE_TS_MS + 30_000]
    assert actor.refreshes == [("toxicity", 5_000), ("fill_prob", 10_000)]
    assert set(actor.session_markouts_by_horizon) == {"1000", "5000", "30000"}
    assert actor.session_markouts_by_horizon["1000"]["coveredFillCount"] == 1

    # Pending observations past the end of the tape are dropped, not evaluated.
    learner.schedule_fill_markouts(fill_key="f2", side="no", fill_price_units=5_200, fill_size_units=100,
                                   fill_fee_units=None, fill_timestamp_ms=BASE_TS_MS + 70_000, fill_context="ctx")
    learner.finish()
    snapshot = learner.snapshot()
    assert snapshot["markouts_observed"] == 3
    assert snapshot["markouts_recorded"] == 3
    assert snapshot["markouts_dropped_after_end"] == 1 + 4  # f1's 120s + all of f2
    assert snapshot["model_refreshes"] == 1
    assert len(actor.observed) == 3


def test_learner_disabled_is_a_clock_passthrough():
    clock = ReplayClock(BASE_TS_MS)
    actor = _FakeLearningActor(clock)
    learner = ReplayModelLearner(actor, clock, enabled=False)
    learner.schedule_fill_markouts(fill_key="f1", side="yes", fill_price_units=4_900, fill_size_units=100,
                                   fill_fee_units=10, fill_timestamp_ms=BASE_TS_MS, fill_context="ctx")
    learner.advance_to(BASE_TS_MS + 200_000)
    learner.finish()
    assert clock.now_ms() == BASE_TS_MS + 200_000
    assert actor.observed == [] and actor.recorded == [] and actor.refreshes == []
    assert learner.snapshot()["enabled"] is False

    # refresh_interval_ms=0 keeps direct record_markout learning but no refresh.
    clock = ReplayClock(BASE_TS_MS)
    actor = _FakeLearningActor(clock)
    learner = ReplayModelLearner(actor, clock, refresh_interval_ms=0)
    learner.schedule_fill_markouts(fill_key="f1", side="yes", fill_price_units=4_900, fill_size_units=100,
                                   fill_fee_units=10, fill_timestamp_ms=BASE_TS_MS, fill_context="ctx")
    learner.advance_to(BASE_TS_MS + 200_000)
    assert len(actor.observed) == 4 and actor.refreshes == []
