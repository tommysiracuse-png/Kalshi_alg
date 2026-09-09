"""Broker live-cap ledger as a hard limit between allocations, and the restart
carryover adoption rule.

(1) monotone, snapshot-safe ledger; (2) position-reducing creates/amends are
exempt for the offsetting size; (3) the order registry is read under the
ledger lock; (4) contracts filled at placement are exposure; (5) restart
carryover adopts only markets the fleet could have traded, displaces seeded
picks, and a reduce-only carryover stops quoting once flat."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

import pytest

from clients.models import AccountPosition, Market, OrderUpdate
from fleet_runtime.execution import (
    OrderRegistry,
    ShardExposureCapError,
    ShardExposureLedger,
    order_reduces_position,
)
from test_fleet_stall import LedgerVenue, _amend, _create, make_broker, request


def _now() -> int:
    return int(time.time() * 1000)


def _fill(ticker, side, order_id, *, filled, remaining, price, status="resting", client_order_id="mm:x"):
    return OrderUpdate(
        ticker, side, order_id, client_order_id, status,
        fill_count_units=filled, remaining_count_units=remaining, price_units=price,
    )


def _parts(broker, shard="3"):
    item = broker._ledger.snapshot(broker._order_registry)["shards"][shard]
    return item["positions_units"], item["resting_units"], item["fills_units"], item["live_units"]


class SnapshotVenue(LedgerVenue):
    """LedgerVenue whose position list the test controls."""

    def __init__(self) -> None:
        super().__init__()
        self.positions = [AccountPosition("TEN-A", 300, market_exposure_units=30_000)]

    def list_account_positions(self, query=None):
        return list(self.positions)


# ---------------------------------------------------------------------------
# (1) monotone, snapshot-safe
# ---------------------------------------------------------------------------

def test_resent_admission_snapshot_never_rolls_back_fills_applied_since():
    broker = make_broker()
    venue = SnapshotVenue()
    snapshot_at = _now() - 60_000                       # the admission snapshot is a minute old
    limits = {
        "enabled": True, "allocatable_by_shard": {3: 100_000},
        "shard_by_ticker": {"TEN-A": 3, "TEN-B": 3},
        "position_exposure_by_ticker": {"TEN-A": 30_000},
        "position_units_by_ticker": {"TEN-A": 300},
        "positions_at_ms": snapshot_at,
    }
    broker._execute(venue, request("shard_exposure_limits", limits))
    broker._execute(venue, _create("TEN-B", "yes", 5_000, 1_000))            # $5.00 resting
    assert _parts(broker) == (30_000, 50_000, 0, 80_000)

    # Half of it fills: resting -> position exposure, live unchanged.
    broker._execute(venue, request("order_update", {"event": _fill("TEN-B", "yes", "o1", filled=500, remaining=500, price=5_000)}))
    assert _parts(broker) == (30_000, 25_000, 25_000, 80_000)
    assert broker._ledger.signed_position_units("TEN-B") == 500

    # The controller re-broadcasts the same (older) snapshot every capacity
    # tick: the fills are newer than it, so nothing is forgotten.
    for _ in range(3):
        broker._execute(venue, request("shard_exposure_limits", limits))
    assert _parts(broker) == (30_000, 25_000, 25_000, 80_000)
    assert broker._ledger.signed_position_units("TEN-B") == 500
    # ... and the headroom the fills would have freed is not there.
    with pytest.raises(ShardExposureCapError):
        broker._execute(venue, _create("TEN-A", "yes", 5_000, 500))         # +$2.50 -> $10.50

    # An unstamped copy (an older controller build) counts as time 0: ignored
    # for every ticker the broker already baselined itself.
    broker._execute(venue, request("shard_exposure_limits", {
        **limits, "position_exposure_by_ticker": {}, "position_units_by_ticker": {}, "positions_at_ms": None,
    }))
    assert _parts(broker) == (30_000, 25_000, 25_000, 80_000)

    # An old snapshot that reports MORE than the baseline raises it (max rule)
    # while the accumulator is kept.
    broker._execute(venue, request("shard_exposure_limits", {
        **limits, "position_exposure_by_ticker": {"TEN-A": 40_000, "TEN-B": 10_000},
    }))
    assert _parts(broker) == (50_000, 25_000, 25_000, 100_000)

    # A snapshot taken AFTER the fill re-baselines: the venue now reports the
    # fill inside the position, so the accumulator is folded in.
    venue.positions = [
        AccountPosition("TEN-A", 300, market_exposure_units=30_000),
        AccountPosition("TEN-B", 500, market_exposure_units=25_000),
    ]
    result = broker._execute(venue, request("admission_snapshot"))
    assert result["positions_at_ms"] >= snapshot_at
    assert _parts(broker) == (55_000, 25_000, 0, 80_000)
    assert broker._ledger.snapshot()["positions_at_ms"] == result["positions_at_ms"]


def test_ledger_snapshot_rules_per_ticker(monkeypatch):
    ledger = ShardExposureLedger()
    ledger.configure(enabled=True, allocatable_by_shard={0: 10**9}, shard_by_ticker={"A": 0, "B": 0})
    ledger.record_positions([AccountPosition("A", 100, market_exposure_units=1_000)], at_ms=1_000)
    # A fill on A at t=5_000 (clock pinned), nothing on B.
    monkeypatch.setattr("fleet_runtime.execution.time.time", lambda: 5.0)
    ledger.note_order_update(_fill("A", "yes", "oa", filled=100, remaining=0, price=5_000, status="executed"))
    ledger.note_order_update(_fill("B", "no", "ob", filled=100, remaining=0, price=4_000, status="executed"))
    monkeypatch.setattr("fleet_runtime.execution.time.time", lambda: 9.0)
    live = lambda: ledger.snapshot()["shards"]["0"]
    assert (live()["positions_units"], live()["fills_units"]) == (1_000, 5_000 + 4_000)
    assert ledger.signed_position_units("A") == 200 and ledger.signed_position_units("B") == -100

    # Older than A's last fill AND B's: nothing re-baselined, baselines only rise.
    ledger.record_positions([AccountPosition("A", 100, market_exposure_units=500)], at_ms=3_000)
    assert (live()["positions_units"], live()["fills_units"]) == (1_000, 9_000)
    assert ledger.signed_position_units("A") == 200
    # Older than the baseline A already holds (1_000): ignored outright.
    ledger.record_positions([AccountPosition("A", 900, market_exposure_units=90_000)], at_ms=500)
    assert live()["positions_units"] == 1_000
    # At/after the fills: both tickers re-baselined; B absent from the venue's
    # list means flat, so its accumulator and signed position clear.
    ledger.record_positions([AccountPosition("A", 200, market_exposure_units=6_000)], at_ms=5_000)
    assert (live()["positions_units"], live()["fills_units"]) == (6_000, 0)
    assert ledger.signed_position_units("A") == 200 and ledger.signed_position_units("B") == 0


# ---------------------------------------------------------------------------
# (2) position-reducing work is exempt for the offsetting size
# ---------------------------------------------------------------------------

def test_reducing_side_is_exempt_up_to_the_position_and_the_rest_is_capped():
    assert order_reduces_position(300, "no") and order_reduces_position(-300, "yes")
    assert not order_reduces_position(300, "yes") and not order_reduces_position(0, "no")
    assert order_reduces_position(300, "yes", "sell") and not order_reduces_position(300, "no", "sell")

    broker = make_broker()
    venue = LedgerVenue()
    # Shard 3 sits exactly at its cap with a long 3 YES position on TEN-A.
    limits = {
        "enabled": True, "allocatable_by_shard": {3: 30_000}, "shard_by_ticker": {"TEN-A": 3, "TEN-B": 3},
        "position_exposure_by_ticker": {"TEN-A": 30_000}, "position_units_by_ticker": {"TEN-A": 300},
        "positions_at_ms": _now(),
    }
    broker._execute(venue, request("shard_exposure_limits", limits))
    # Any new exposure is refused ...
    with pytest.raises(ShardExposureCapError):
        broker._execute(venue, _create("TEN-B", "yes", 5_000, 100))
    with pytest.raises(ShardExposureCapError):
        broker._execute(venue, _create("TEN-A", "yes", 5_000, 100))          # adds to the long
    # ... but the bot's ordinary NO quote against the long passes for the
    # offsetting size (no reduce_only flag needed) and counts no exposure.
    reducing = broker._execute(venue, _create("TEN-A", "no", 5_000, 300))
    assert reducing.order_id == "o1"
    assert _parts(broker) == (30_000, 0, 0, 30_000)
    # A larger NO quote is exempt for 300; the remaining 200 does not fit the
    # cap, so instead of refusing the whole quote (which would park the bot's
    # reducing side in its 30 s insufficient-balance cooldown) the broker
    # sends just the reducing part: the venue sees 3 contracts, nothing is
    # reserved and the shard's live exposure is unchanged.
    trimmed = broker._execute(venue, _create("TEN-A", "no", 5_000, 500))
    assert venue.created[-1].count_units == 300 and not venue.created[-1].reduce_only
    assert broker._ledger.trimmed == 1 and _parts(broker) == (30_000, 0, 0, 30_000)
    resting_id = trimmed.order_id
    # Amend-up of the reducing order: exempt to 300, trimmed beyond.
    broker._execute(venue, _amend("TEN-A", "no", resting_id, 5_000, 400))
    assert venue.amended[-1].new_total_fillable_count_units == 300 and broker._ledger.trimmed == 2
    broker._execute(venue, _amend("TEN-A", "no", resting_id, 5_000, 200))
    assert venue.amended[-1].new_total_fillable_count_units == 200
    assert _parts(broker) == (30_000, 0, 0, 30_000)
    # Once it fills the position is flat: the NO side is no longer reducing and
    # is capped like any other create; the fill itself added no exposure.
    broker._execute(venue, request("order_update", {"event": _fill("TEN-A", "no", resting_id, filled=200, remaining=0, price=5_000, status="executed")}))
    assert broker._ledger.signed_position_units("TEN-A") == 100
    last = broker._execute(venue, _create("TEN-A", "no", 5_000, 100))        # still reducing the last 1 YES
    broker._execute(venue, request("order_update", {"event": _fill("TEN-A", "no", last.order_id, filled=100, remaining=0, price=5_000, status="executed")}))
    assert broker._ledger.signed_position_units("TEN-A") == 0
    assert _parts(broker) == (30_000, 0, 0, 30_000)
    with pytest.raises(ShardExposureCapError):                              # no reducing part: refused
        broker._execute(venue, _create("TEN-A", "no", 5_000, 100))
    assert broker._ledger.rejections == 3
    # Venue reduce-only orders still never reserve.
    broker._execute(venue, _create("TEN-B", "no", 5_000, 2_000, reduce_only=True))


# ---------------------------------------------------------------------------
# (3) registry read under the ledger lock
# ---------------------------------------------------------------------------

class LockCheckingRegistry(OrderRegistry):
    def __init__(self, ledger):
        super().__init__()
        self.ledger = ledger
        self.snapshots_under_lock = 0
        self.snapshots_outside_lock = 0

    def snapshot(self):
        if self.ledger._lock.locked():
            self.snapshots_under_lock += 1
        else:
            self.snapshots_outside_lock += 1
        return super().snapshot()


def test_reserve_reads_the_order_registry_inside_the_ledger_lock():
    broker = make_broker()
    venue = LedgerVenue()
    registry = LockCheckingRegistry(broker._ledger)
    broker.__dict__["_order_registry_state"] = registry
    broker._ledger.configure(enabled=True, allocatable_by_shard={3: 100_000}, shard_by_ticker={"TEN-A": 3})
    broker._execute(venue, _create("TEN-A", "yes", 5_000, 1_000))
    broker._execute(venue, _amend("TEN-A", "yes", "o1", 5_000, 1_200))
    assert registry.snapshots_under_lock == 2 and registry.snapshots_outside_lock == 0
    # The direct API takes the registry object too (the snapshot mapping is
    # still accepted for callers that already hold one).
    assert broker._ledger.reserve("TEN-A", 1_000, registry=registry)
    assert registry.snapshots_under_lock == 3
    assert broker._ledger.reserve("TEN-A", 1_000, orders=registry.snapshot()) and registry.snapshots_outside_lock == 1


# ---------------------------------------------------------------------------
# (4) contracts filled at placement
# ---------------------------------------------------------------------------

class FillingVenue(LedgerVenue):
    """Every create fills 200 contracts on placement."""

    def create_order(self, request):
        from clients.models import Order

        self.sequence += 1
        self.calls.append(("create_order", (request.market_id, request.side, request.price_units, request.count_units)))
        return Order(
            f"o{self.sequence}", request.market_id, request.side, request.client_order_id, "resting",
            request.price_units, 200, request.count_units - 200,
        )


def test_contracts_filled_at_placement_count_as_exposure():
    broker = make_broker()
    venue = FillingVenue()
    broker._execute(venue, request("shard_exposure_limits", {
        "enabled": True, "allocatable_by_shard": {3: 100_000}, "shard_by_ticker": {"TEN-A": 3},
        "position_exposure_by_ticker": {}, "position_units_by_ticker": {}, "positions_at_ms": _now(),
    }))
    broker._execute(venue, _create("TEN-A", "yes", 5_000, 1_000))
    # 800 resting ($4.00) + 200 filled on placement ($1.00): nothing freed.
    assert _parts(broker) == (0, 40_000, 10_000, 50_000)
    assert broker._ledger.signed_position_units("TEN-A") == 200
    # The venue's later update for the same fill is not counted twice.
    broker._execute(venue, request("order_update", {"event": _fill("TEN-A", "yes", "o1", filled=200, remaining=800, price=5_000)}))
    assert _parts(broker) == (0, 40_000, 10_000, 50_000)
    broker._execute(venue, request("order_update", {"event": _fill("TEN-A", "yes", "o1", filled=300, remaining=700, price=5_000)}))
    assert _parts(broker) == (0, 35_000, 15_000, 50_000)


# ---------------------------------------------------------------------------
# (5) restart carryover: adoption rule, seed displacement, flat carryovers
# ---------------------------------------------------------------------------

HOUR_MS = 3_600_000


class HorizonClient:
    """Screen client whose markets carry close times and statuses."""

    def __init__(self, now_ms):
        self.now_ms = now_ms
        self.markets = {
            "SOON": Market("SOON", title="Closes soon", status="active", series_id="SOON", close_time_ms=now_ms + HOUR_MS // 6),
            "INSIDE": Market("INSIDE", title="Inside", status="active", series_id="INS", close_time_ms=now_ms + 5 * HOUR_MS),
            "LONGDATED": Market("LONGDATED", title="Long dated", status="open", series_id="LONG", close_time_ms=now_ms + 30 * 24 * HOUR_MS),
            "TRADED": Market("TRADED", title="Traded before", status="open", series_id="TRD", close_time_ms=now_ms + 30 * 24 * HOUR_MS),
            "UNOPENED": Market("UNOPENED", title="Not open", status="unopened", series_id="UNO", close_time_ms=now_ms + 5 * HOUR_MS),
            "EXPECTED": Market("EXPECTED", title="Expected expiry", status="open", series_id="EXP",
                               close_time_ms=now_ms + 400 * HOUR_MS, expected_expiration_time_ms=now_ms + 10 * HOUR_MS),
        }

    def list_markets(self, query):
        return []

    def get_market(self, market_id):
        return self.markets[market_id]

    def get_positions(self, market_id):
        from clients.models import Position
        return [Position(market_id, 0)]

    def get_market_quote(self, market_id):
        from clients.models import MarketQuote
        return MarketQuote(market_id, 5000, 5000)


HORIZON_SETTINGS = {"status": "open", "min_time_to_close_hrs": 1.0, "max_time_to_close_hrs": 48.0}


def _horizon_screener(tmp_path, monkeypatch, client, *, settings=HORIZON_SETTINGS, max_bots=10, name="screen.csv"):
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener

    frame = pd.DataFrame([{"Rank": 1, "Ticker": "SCREENED", "SearchText": "Screened", "Exchange Index": 0}])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)
    return Screener(
        client=client, settings=settings, output_path=tmp_path / name,
        default_yes_budget_cents=100, default_no_budget_cents=150,
        max_bots=max_bots, minimum_carryover_value_cents=20,
    )


def test_screener_horizon_verdict_uses_the_screener_status_and_time_window():
    from screener import screener_horizon_verdict

    now = _now()
    client = HorizonClient(now)
    verdict = lambda ticker, settings=HORIZON_SETTINGS: screener_horizon_verdict(client.markets[ticker], settings, now_ms=now)
    assert verdict("INSIDE") == "" and verdict("EXPECTED") == ""     # 'active' == screened 'open'; earliest cutoff wins
    assert "under the 1 h minimum" in verdict("SOON")
    assert "over the 48 h maximum" in verdict("LONGDATED")
    assert "status 'unopened' is not the screened 'open'" in verdict("UNOPENED")
    assert verdict("LONGDATED", {"status": "open"}) == ""              # no window configured -> no restriction
    assert verdict("UNOPENED", {"min_time_to_close_hrs": 1}) == ""     # no status configured
    assert verdict("SOON", {}) == ""
    assert "no close time" in screener_horizon_verdict(Market("X", status="open"), HORIZON_SETTINGS, now_ms=now)


def test_carryover_adopts_only_markets_inside_the_horizon_or_with_fleet_fills(tmp_path, monkeypatch):
    now = _now()
    screener = _horizon_screener(tmp_path, monkeypatch, HorizonClient(now))
    positions = [
        AccountPosition(ticker, 100, market_exposure_units=5_000)
        for ticker in ("SOON", "INSIDE", "LONGDATED", "TRADED", "UNOPENED", "EXPECTED")
    ]
    update = asyncio.run(screener.refresh({}, reason="startup", exchange_positions=positions, traded_tickers={"TRADED"}))
    assert set(update.inventory_carried) == {"INSIDE", "EXPECTED", "TRADED"}
    assert set(update.pick_by_market_id) == {"SCREENED", "INSIDE", "EXPECTED", "TRADED"}
    status = screener.status_snapshot()
    assert status["exchangeCarryover"]["leftAlone"] == ["SOON", "LONGDATED", "UNOPENED"]
    assert status["exchangeCarryover"]["carried"] == ["EXPECTED", "INSIDE", "TRADED"]   # value, then ticker
    left = [w for w in status["warnings"] if w.startswith("left 3 exchange position(s) alone (outside the screener's horizon): ")]
    assert left and "SOON, LONGDATED, UNOPENED" in left[0] and "under the 1 h minimum" in left[0]

    # Without fill history the long-dated position is left alone too.
    plain = _horizon_screener(tmp_path, monkeypatch, HorizonClient(now), name="screen2.csv")
    update = asyncio.run(plain.refresh({}, reason="startup", exchange_positions=positions))
    assert set(update.inventory_carried) == {"INSIDE", "EXPECTED"}
    assert plain.status_snapshot()["exchangeCarryover"]["leftAlone"] == ["SOON", "LONGDATED", "TRADED", "UNOPENED"]

    # A session without a window (settings={}) restricts nothing.
    loose = _horizon_screener(tmp_path, monkeypatch, HorizonClient(now), settings={}, name="screen3.csv")
    update = asyncio.run(loose.refresh({}, reason="startup", exchange_positions=positions))
    assert len(update.inventory_carried) == 6


def test_carryover_displaces_csv_and_fixed_ticker_seeds_like_screened_picks(tmp_path, monkeypatch):
    from fleet_models import ScreenerPick

    now = _now()
    screener = _horizon_screener(tmp_path, monkeypatch, HorizonClient(now), max_bots=2)
    seeds = [
        ScreenerPick("SEED-1", "Seed 1", 100, 150, {"Ticker": "SEED-1"}, selection_reason="csv_seed"),
        ScreenerPick("SEED-2", "Seed 2", 100, 150, {"Ticker": "SEED-2"}, selection_reason="csv_seed"),
    ]
    selected, carried, warnings = screener.carry_exchange_positions(
        seeds, [AccountPosition("INSIDE", 100, market_exposure_units=5_000)],
    )
    assert [p.market_id for p in selected] == ["SEED-1", "INSIDE"] and carried == ["INSIDE"]
    assert any("displaced 1 lower-ranked seeded market(s) to honor max_bots=2" in w for w in warnings)
    fixed = [ScreenerPick("FIXED", "Fixed", 100, 150, {"Ticker": "FIXED"}, selection_reason="fixed_session_ticker")]
    one = _horizon_screener(tmp_path, monkeypatch, HorizonClient(now), max_bots=1, name="screen2.csv")
    selected, carried, _ = one.carry_exchange_positions(fixed, [AccountPosition("INSIDE", 100, market_exposure_units=5_000)])
    assert [p.market_id for p in selected] == ["INSIDE"] and carried == ["INSIDE"]
    # Carryovers never displace each other: the lowest-value one is omitted instead.
    selected, carried, warnings = one.carry_exchange_positions(
        [], [AccountPosition("INSIDE", 100, market_exposure_units=5_000), AccountPosition("EXPECTED", 100, market_exposure_units=1_000)],
    )
    assert [p.market_id for p in selected] == ["INSIDE"] and any("omitted 1 lower-value position(s): EXPECTED" in w for w in warnings)


def _write_fills_db(path: Path, tickers) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE fills (id INTEGER PRIMARY KEY, fill_key TEXT, ts_ms INTEGER, ticker TEXT, side TEXT)")
        db.executemany(
            "INSERT INTO fills (fill_key, ts_ms, ticker, side) VALUES (?, ?, ?, ?)",
            [(f"{ticker}-{index}", 1_000 + index, ticker, "yes") for index, ticker in enumerate(tickers)],
        )


def test_launcher_reads_fleet_fill_history_from_the_session_run_artifacts(tmp_path, monkeypatch):
    from launcher import Launcher

    session_root = tmp_path / "artifacts" / "session-1"
    _write_fills_db(session_root / "run-old" / "shards" / "worker-00" / "telemetry.sqlite3", ["TRADED", "OLD"])
    _write_fills_db(session_root / "run-old" / "shards" / "worker-01" / "telemetry.sqlite3", ["OTHER"])
    _write_fills_db(session_root / "run-legacy" / "markets" / "LEGACY" / "telemetry.sqlite3", ["LEGACY"])
    (session_root / "run-empty" / "shards" / "worker-00").mkdir(parents=True)
    (session_root / "run-empty" / "shards" / "worker-00" / "telemetry.sqlite3").write_bytes(b"not a database")
    current = session_root / "run-current"
    current.mkdir()

    launcher = Launcher.__new__(Launcher)
    launcher.run_artifact_path = current
    assert launcher._fleet_traded_tickers_sync() == {"TRADED", "OLD", "OTHER", "LEGACY"}
    assert asyncio.run(launcher._fleet_traded_tickers()) == {"TRADED", "OLD", "OTHER", "LEGACY"}
    launcher.run_artifact_path = None
    assert launcher._fleet_traded_tickers_sync() == set()

    # The direct seed path hands the history to the carryover: the long-dated
    # position the fleet traded before is adopted, the untouched one is not.
    now = _now()
    launcher.run_artifact_path = current
    launcher.api_key_id, launcher.private_key_path, launcher.last_error = "key", "pem", None

    class StartClient:
        def list_account_positions(self, query=None):
            return [
                AccountPosition("TRADED", 100, market_exposure_units=5_000),
                AccountPosition("LONGDATED", 100, market_exposure_units=5_000),
            ]

    launcher.client = StartClient()
    launcher.screener = _horizon_screener(tmp_path, monkeypatch, HorizonClient(now))
    picks, carried, warnings = asyncio.run(launcher._with_startup_carryover(()))
    assert [p.market_id for p in picks] == ["TRADED"] and carried == ("TRADED",)
    assert any(w.startswith("left 1 exchange position(s) alone (outside the screener's horizon): LONGDATED") for w in warnings)


def test_broker_refuses_carryover_creates_that_do_not_reduce_the_position():
    broker = make_broker()
    venue = LedgerVenue()
    # A 1.0 fleet (cap disabled): the guard is independent of the cap.
    limits = {
        "enabled": False, "allocatable_by_shard": {0: 500_000}, "shard_by_ticker": {"CARRY": 0, "IDX": 0},
        "position_exposure_by_ticker": {"CARRY": 15_000}, "position_units_by_ticker": {"CARRY": -300},
        "positions_at_ms": _now(), "reduce_only_tickers": ["CARRY"],
    }
    broker._execute(venue, request("shard_exposure_limits", limits))
    assert broker._ledger.snapshot()["flat_reduce_only"] == [] and broker._ledger.snapshot()["reduce_only_tickers"] == 1
    with pytest.raises(ShardExposureCapError) as refused:      # NO would build the short
        broker._execute(venue, _create("CARRY", "no", 5_000, 100))
    assert refused.value.detail["reason"] == "carryover_flat" and refused.value.detail["signed_units"] == -300
    broker._execute(venue, _create("CARRY", "yes", 5_000, 300))            # reduces the short: passes
    broker._execute(venue, _create("IDX", "yes", 5_000, 90_000))           # other tickers unaffected
    # The reducing quote fills the position flat: the very next create on the
    # (still granted) YES side is refused and the heartbeat reports it flat.
    broker._execute(venue, request("order_update", {"event": _fill("CARRY", "yes", "o1", filled=300, remaining=0, price=5_000, status="executed")}))
    assert broker._ledger.signed_position_units("CARRY") == 0
    assert broker._ledger.snapshot()["flat_reduce_only"] == ["CARRY"]
    with pytest.raises(ShardExposureCapError):
        broker._execute(venue, _create("CARRY", "yes", 5_000, 100))
    assert [name for name, _ in venue.calls if name == "create_order"] == ["create_order", "create_order"]
    # Dropped from the carryover set by the next admission: an ordinary ticker again.
    broker._execute(venue, request("shard_exposure_limits", {**limits, "reduce_only_tickers": []}))
    broker._execute(venue, _create("CARRY", "yes", 5_000, 100))


def test_manager_stamps_the_snapshot_and_drops_flat_carryovers_from_allowed_sides(tmp_path):
    from fleet_models import ScreenerUpdate
    from fleet_runtime.execution import BrokerRequest
    from test_allocation_oversubscription import SHARD0_CASH, SHARD3_CASH, _SnapshotAdmin, _live_manager, _pick

    class StampedAdmin(_SnapshotAdmin):
        async def call(self, operation, payload=None, timeout=60.0):
            return {**(await super().call(operation, payload, timeout)), "positions_at_ms": 1_234_567}

    async def scenario():
        admin = StampedAdmin(
            cash_by_shard={0: SHARD0_CASH, 3: SHARD3_CASH},
            positions=[AccountPosition("KXCS2-BIG", -1_100, market_exposure_units=6_000)],
        )
        manager, commands = _live_manager(tmp_path, oversubscription=3.0, admin=admin)
        picks = (_pick("KXINX-1", 0), _pick("KXCS2-BIG", 0, reason="exchange_position"))
        await manager.apply_update(ScreenerUpdate(1, 1, "test", picks, tuple(p.market_id for p in picks), (), (), ()))
        payload = [item for item in manager._request_queue.items if isinstance(item, BrokerRequest)][-1].payload
        assert payload["positions_at_ms"] == 1_234_567
        assert payload["position_units_by_ticker"] == {"KXCS2-BIG": -1_100}
        assert payload["reduce_only_tickers"] == ["KXCS2-BIG"]
        assert manager.status_snapshot()["broker"]["exposureLimits"]["reduceOnlyTickers"] == 1
        assert commands.items[-1]["allocations"]["KXCS2-BIG"] == ("yes",)
        before = len(commands.items)

        # A heartbeat with nothing flat changes nothing; one that reports the
        # carryover flat triggers a re-broadcast without that side.
        now = _now()
        assert manager._ingest_broker_status({"type": "heartbeat", "at_ms": now, "shard_exposure": {"flat_reduce_only": []}}) is False
        assert manager._ingest_broker_status({"type": "heartbeat", "at_ms": now, "shard_exposure": {"flat_reduce_only": ["KXCS2-BIG"]}}) is True
        gate_open, allocations = manager._quoting_gate()
        assert gate_open and "KXCS2-BIG" not in allocations and allocations["KXINX-1"]
        manager._broadcast_quoting_gate()          # what monitor_once does on a capacity change
        assert len(commands.items) == before + 1
        assert commands.items[-1]["enabled"] is True and "KXCS2-BIG" not in commands.items[-1]["allocations"]
        assert commands.items[-1]["allocations"]["KXINX-1"] == allocations["KXINX-1"]
        # The same report again is not a change; an unrelated flat ticker neither.
        assert manager._ingest_broker_status({"type": "heartbeat", "at_ms": now, "shard_exposure": {"flat_reduce_only": ["KXCS2-BIG"]}}) is False
        assert manager._ingest_broker_status({"type": "heartbeat", "at_ms": now, "shard_exposure": {"flat_reduce_only": ["KXCS2-BIG", "ZZZ"]}}) is False

        # An admission without a stamp falls back to the instant it was asked.
        manager._admin = _SnapshotAdmin(cash_by_shard={0: SHARD0_CASH, 3: SHARD3_CASH})
        asked = _now()
        await manager._admission({"KXINX-1": picks[0]})
        assert manager._position_snapshot_at_ms >= asked and manager._reduce_only_tickers == set()

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# (6) sells arrive on the OUTCOME side; carryover quotes can flatten, never flip
# ---------------------------------------------------------------------------

def _watchdog_exit(ticker, held_side, price, count):
    """The bot's flatten order: sell the held side, IOC, venue reduce-only, client id ``wd:``."""
    from clients.models import CreateOrderRequest
    from fleet_models import IntentUrgency, QuoteIntent

    now = _now()
    return request("quote_intent", {
        "action": "create_order",
        "request": CreateOrderRequest(
            ticker, held_side, price, count, f"wd:{held_side}:x", None,
            action="sell", reduce_only=True, time_in_force="immediate_or_cancel",
        ),
        "intent": QuoteIntent(ticker, held_side, price, count, 1, IntentUrgency.NORMAL, now, now + 20_000),
    })


def test_watchdog_sell_on_the_outcome_side_flattens_the_ledger_position():
    """A sell of YES comes back from the adaptor as side "no" (buy-equivalent).

    Combining that with the request's raw action "sell" inverted the fill a
    second time, so a watchdog exit of a long DOUBLED the ledger's position
    (300 -> 600) instead of flattening it: the carryover never reported flat
    and a NO quote on the really-flat ticker was admitted as "reducing" with
    no reservation.  The venue double answers like the real adaptor here.
    """
    broker = make_broker()
    venue = LedgerVenue(normalize_sides=True)
    limits = {
        "enabled": True, "allocatable_by_shard": {0: 500_000}, "shard_by_ticker": {"CARRY": 0, "CARRY2": 0},
        "position_exposure_by_ticker": {"CARRY": 12_000, "CARRY2": 12_000},
        "position_units_by_ticker": {"CARRY": 300, "CARRY2": 300},
        "positions_at_ms": _now(), "reduce_only_tickers": ["CARRY", "CARRY2"],
    }
    broker._execute(venue, request("shard_exposure_limits", limits))

    exit_order = broker._execute(venue, _watchdog_exit("CARRY", "yes", 4_000, 300))
    assert exit_order.side == "no" and exit_order.fill_count_units == 300     # the adaptor's contract
    assert broker._ledger.signed_position_units("CARRY") == 0                # flat, not +600
    assert broker._ledger.snapshot()["flat_reduce_only"] == ["CARRY"]
    assert _parts(broker, "0")[2] == 0                                        # a reduce-only exit adds no exposure
    with pytest.raises(ShardExposureCapError) as refused:                     # flat: nothing may open the other side
        broker._execute(venue, _create("CARRY", "no", 4_000, 100))
    assert refused.value.detail["reason"] == "carryover_flat"

    # The same for a fill the broker did not place (previous generation): the
    # stream's side is the outcome side and the client tag never re-inverts it.
    broker._execute(venue, request("order_update", {"event": _fill(
        "CARRY2", "no", "wd-legacy", filled=300, remaining=0, price=4_000, status="executed", client_order_id="wd:yes:legacy",
    )}))
    assert broker._ledger.signed_position_units("CARRY2") == 0
    assert broker._ledger.snapshot()["flat_reduce_only"] == ["CARRY", "CARRY2"]


def test_carryover_reducing_quote_is_capped_at_the_position_and_stamped_reduce_only():
    """A reduce-only carryover with |position| < quote size must flatten, never flip.

    The bot sizes a reducing quote from the side budget and lets it cross
    through flat; the broker caps the create at the position's size and
    stamps it venue reduce-only, cap enabled or not.
    """
    broker = make_broker()
    venue = LedgerVenue(normalize_sides=True)
    limits = {
        "enabled": False, "allocatable_by_shard": {0: 500_000}, "shard_by_ticker": {"CARRY": 0},
        "position_exposure_by_ticker": {"CARRY": 15_000}, "position_units_by_ticker": {"CARRY": 300},
        "positions_at_ms": _now(), "reduce_only_tickers": ["CARRY"],
    }
    broker._execute(venue, request("shard_exposure_limits", limits))

    order = broker._execute(venue, _create("CARRY", "no", 5_000, 1_000))      # 10 contracts against a 3-contract long
    sent = venue.created[-1]
    assert sent.count_units == 300 and sent.reduce_only is True
    assert sent.time_in_force == "immediate_or_cancel" and sent.post_only is False
    assert order.fill_count_units == 300 and order.remaining_count_units == 0 and broker._ledger.trimmed == 1
    # IOC fills flatten the carryover immediately; the next NO create is
    # refused because the ticker cannot go short.
    assert broker._ledger.signed_position_units("CARRY") == 0
    assert _parts(broker, "0")[2] == 0                                        # the offsetting fill added no exposure
    with pytest.raises(ShardExposureCapError) as refused:
        broker._execute(venue, _create("CARRY", "no", 5_000, 100))
    assert refused.value.detail["reason"] == "carryover_flat"
