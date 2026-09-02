import asyncio
import io
import signal
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from bot_manager import BotManager, BotManagerConfig, ManagedBot
from clients.models import AccountOrder, MarketQuote, Position
from fleet_models import MAX_CONCURRENT_BOTS, ScreenerPick, ScreenerUpdate
from kalshi_screener import MARKET_SCAN_HARD_LIMIT, screen_markets
from lip_launcher import ChildProcess


class FakeScreenClient:
    def list_markets(self, query):
        return []

    def get_positions(self, market_id):
        if market_id == "UNKNOWN":
            raise RuntimeError("temporary failure")
        return [Position(market_id, 0)]

    def get_market_quote(self, market_id):
        return MarketQuote(market_id, 4000, 5000)


def pick(market_id, yes=100, no=100):
    return ScreenerPick(market_id, market_id, yes, no, {"Ticker": market_id})


def test_screener_diff_inventory_fail_safe_csv_and_last_good(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener

    frame = pd.DataFrame([{"Ticker": "NEW", "SearchText": "New market", "Best EV(c)": 4.0}])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)
    output = tmp_path / "screen.csv"
    screener = Screener(
        client=FakeScreenClient(),
        settings={},
        output_path=output,
        default_yes_budget_cents=100,
        default_no_budget_cents=100,
        max_bots=10,
        minimum_carryover_value_cents=20,
    )

    update = asyncio.run(screener.refresh({"UNKNOWN": pick("UNKNOWN"), "FLAT": pick("FLAT")}, reason="test"))
    assert update is not None
    assert set(update.added) == {"NEW"}
    assert set(update.kept) == {"UNKNOWN"}
    assert set(update.removed) == {"FLAT"}
    assert update.inventory_unknown == ("UNKNOWN",)
    assert output.exists()
    assert {item.market_id for item in screener.get_latest_picks()} == {"NEW", "UNKNOWN"}

    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: (_ for _ in ()).throw(RuntimeError("bad refresh")))
    failed = asyncio.run(screener.refresh(update.pick_by_market_id, reason="failure"))
    assert failed is None
    assert {item.market_id for item in screener.get_latest_picks()} == {"NEW", "UNKNOWN"}
    status = screener.status_snapshot()
    assert status["running"] is False
    assert status["lastDurationMs"] is not None
    assert status["lastError"] == "bad refresh"
    assert {item["marketId"] for item in status["picks"]} == {"NEW", "UNKNOWN"}


def test_screener_inventory_carryover_consumes_cap_and_displaces_lowest_rank(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener

    frame = pd.DataFrame([
        {"Rank": 1, "Ticker": "FIRST", "SearchText": "First"},
        {"Rank": 2, "Ticker": "SECOND", "SearchText": "Second"},
        {"Rank": 3, "Ticker": "THIRD", "SearchText": "Third"},
    ])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)

    class InventoryClient(FakeScreenClient):
        def get_positions(self, market_id):
            return [Position(market_id, 100)]

        def get_market_quote(self, market_id):
            return MarketQuote(market_id, 5000, 5000)

    screener = Screener(
        client=InventoryClient(),
        settings={},
        output_path=tmp_path / "screen.csv",
        default_yes_budget_cents=100,
        default_no_budget_cents=100,
        max_bots=3,
        minimum_carryover_value_cents=20,
    )
    update = asyncio.run(screener.refresh({"HELD": pick("HELD")}, reason="test"))

    assert update is not None
    assert [item.market_id for item in update.picks] == ["FIRST", "SECOND", "HELD"]
    assert update.inventory_carried == ("HELD",)
    assert len(update.picks) == 3
    assert any("displacing 1 lower-ranked" in warning for warning in screener.status_snapshot()["warnings"])


def test_screener_overflow_prioritizes_unknown_then_largest_inventory(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener

    frame = pd.DataFrame([{"Rank": 1, "Ticker": "SCREEN", "SearchText": "Screen"}])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)

    class OverflowClient(FakeScreenClient):
        def get_positions(self, market_id):
            if market_id == "UNKNOWN":
                raise RuntimeError("position temporarily unavailable")
            return [Position(market_id, 100)]

        def get_market_quote(self, market_id):
            prices = {"SMALL": 3000, "LARGE": 7000}
            return MarketQuote(market_id, prices[market_id], 5000)

    screener = Screener(
        client=OverflowClient(),
        settings={},
        output_path=tmp_path / "screen.csv",
        default_yes_budget_cents=100,
        default_no_budget_cents=100,
        max_bots=2,
        minimum_carryover_value_cents=1,
    )
    update = asyncio.run(screener.refresh(
        {key: pick(key) for key in ("SMALL", "LARGE", "UNKNOWN")}, reason="test"
    ))

    assert update is not None
    assert [item.market_id for item in update.picks] == ["UNKNOWN", "LARGE"]
    assert update.inventory_unknown == ("UNKNOWN",)
    assert len(update.picks) == 2
    assert any("omitted 1 lower-priority" in warning for warning in screener.status_snapshot()["warnings"])


def test_market_scan_is_hard_capped_and_reports_truncation():
    class MarketSource:
        max_total = None

        def list_markets(self, *, status, limit, max_total, mve_filter):
            self.max_total = max_total
            return []

    source = MarketSource()
    frame = screen_markets(source, {
        "status": "open",
        "mve_filter": "exclude",
        "max_markets_to_scan": 600_000,
        "markout_filter_enabled": False,
    })

    assert source.max_total == MARKET_SCAN_HARD_LIMIT
    assert frame.attrs["market_scan"] == {
        "requestedLimit": 600_000,
        "effectiveLimit": MARKET_SCAN_HARD_LIMIT,
        "scannedMarkets": 0,
        "truncated": True,
    }
    assert any("capped" in warning for warning in frame.attrs["warnings"])


def manager_config(tmp_path):
    return BotManagerConfig(
        bot_script_path=tmp_path / "V1.py",
        logs_directory=tmp_path / "logs",
        runtime_dir=tmp_path / "runtime",
        watchdog_state_dir=tmp_path / "watchdog",
        watchdog_disable_file=tmp_path / "disabled.json",
        watchdog_runner_script_path=None,
        watchdog_profiler_script_path=None,
        dry_run=True,
    )


def test_manager_applies_incremental_diff(tmp_path):
    async def scenario():
        manager = BotManager(manager_config(tmp_path))
        calls = []

        async def start(value, *, restart=False):
            calls.append(("start", value.market_id, restart))

        async def stop(market_id, *, reason="", remove_desired=True):
            calls.append(("stop", market_id, reason))

        manager.start_bot = start
        manager.stop_bot = stop
        manager.bots["KEEP"] = SimpleNamespace(pick=pick("KEEP"))
        update = ScreenerUpdate(
            2,
            1,
            "test",
            (pick("KEEP"), pick("ADD"), pick("CHANGE", 200)),
            added=("ADD",),
            kept=("KEEP",),
            changed=("CHANGE",),
            removed=("REMOVE",),
        )
        await manager.apply_update(update)
        assert ("stop", "REMOVE", "screen_removed") in calls
        assert ("stop", "CHANGE", "configuration_changed") in calls
        assert ("start", "ADD", False) in calls
        assert ("start", "CHANGE", False) in calls
        assert not any(call[:2] == ("start", "KEEP") for call in calls)

    asyncio.run(scenario())


def test_manager_restart_limit_is_bounded(tmp_path):
    async def scenario():
        manager = BotManager(manager_config(tmp_path))
        manager._desired["MKT"] = pick("MKT")
        manager._restart_history["MKT"] = __import__("collections").deque([time.time()] * 3)
        await manager._restart("MKT")
        event = await manager.events.get()
        assert event.event_type == "restart_exhausted"

    asyncio.run(scenario())


def test_manager_aggregates_client_portfolio_pnl_and_api_activity(tmp_path):
    class RunningProcess:
        pid = 77

        def poll(self):
            return None

    manager = BotManager(manager_config(tmp_path))
    selected = pick("MKT")
    child = ChildProcess(
        ticker="MKT",
        process=RunningProcess(),
        log_path=tmp_path / "bot.log",
        log_handle=io.StringIO(),
        slot_index=0,
        pick=selected,
    )
    managed = ManagedBot(selected, child, socket_healthy=True)
    managed.last_status = {
        "lifecycle": "running",
        "lastMarketEventAtMs": int(time.time() * 1000),
        "monitoring": {
            "portfolio": {"currentPositionUnits": 250, "updatedAtMs": int(time.time() * 1000)},
            "pnl": {"fills": 2, "feesCents": 1.0, "realizedCents": 3.0, "unrealizedCents": 2.0, "totalCents": 5.0},
            "apiActivity": {"rest": {"total": 7, "errors": 1, "byOperation": {"get_positions": 2}}, "stream": {"message": 9}},
        },
    }
    manager.bots["MKT"] = managed
    manager._desired["MKT"] = selected

    snapshot = manager.status_snapshot()
    assert snapshot["monitoring"]["portfolio"]["netPositionUnits"] == 250
    assert snapshot["monitoring"]["portfolio"]["unknownMarkets"] == 0
    assert snapshot["monitoring"]["pnl"]["totalCents"] == 5.0
    assert snapshot["monitoring"]["apiActivity"]["rest"]["total"] == 7
    assert snapshot["clients"][0]["marketId"] == "MKT"


def test_manager_shutdown_escalates_after_socket_is_unavailable(tmp_path):
    class FakeProcess:
        pid = 123

        def __init__(self):
            self.signals = []
            self.terminated = False

        def poll(self):
            return None

        def send_signal(self, value):
            self.signals.append(value)

        def terminate(self):
            self.terminated = True

        def kill(self):
            raise AssertionError("kill should not be needed")

    async def scenario():
        manager = BotManager(manager_config(tmp_path))
        process = FakeProcess()
        selected = pick("MKT")
        child = ChildProcess(
            ticker="MKT",
            process=process,
            log_path=tmp_path / "bot.log",
            log_handle=io.StringIO(),
            slot_index=0,
            pick=selected,
        )
        manager.bots["MKT"] = ManagedBot(selected, child)
        waits = iter((False, False, True))

        async def wait_process(value, timeout):
            return next(waits)

        manager._wait_process = wait_process
        await manager.stop_bot("MKT")
        assert process.signals
        assert process.terminated
        assert "MKT" not in manager.bots

    asyncio.run(scenario())


def test_manager_account_cleanup_cancels_bot_orders_and_preserves_manual_orders(tmp_path):
    class CleanupClient:
        def __init__(self):
            self.orders = [
                AccountOrder("bot-1", "MKT", client_order_id="mm:yes:1", status="resting"),
                AccountOrder("bot-2", "OTHER", client_order_id="tob:no:2", status="resting"),
                AccountOrder("manual", "MKT", client_order_id="operator-order", status="resting"),
            ]
            self.canceled = []

        def list_account_orders(self, query):
            return [
                order for order in self.orders
                if not query.market_id or order.market_id == query.market_id
            ]

        def cancel_order(self, *, order_id):
            self.canceled.append(order_id)
            self.orders = [order for order in self.orders if order.order_id != order_id]

    client = CleanupClient()
    config = replace(manager_config(tmp_path), dry_run=False, shutdown_cleanup_delay_seconds=0)
    manager = BotManager(config, cleanup_client=client)

    canceled = manager._cancel_and_verify_owned_orders_sync()

    assert canceled == 2
    assert client.canceled == ["bot-1", "bot-2"]
    assert [order.order_id for order in client.orders] == ["manual"]


@pytest.mark.skipif(
    not hasattr(signal, "SIGSTOP"),
    reason="legacy per-process BotManager freezes children via POSIX SIGSTOP; "
    "the live fleet uses ShardedBotManager, which freezes via worker control "
    "messages (covered by tests/test_fleet_shutdown.py)",
)
def test_fleet_shutdown_freezes_every_bot_before_account_cleanup(tmp_path):
    timeline = []

    class Process:
        def __init__(self, pid):
            self.pid = pid
            self.running = True

        def poll(self):
            return None if self.running else 0

        def send_signal(self, value):
            timeline.append(("signal", self.pid, value))

        def terminate(self):
            timeline.append(("terminate", self.pid))
            self.running = False

        def kill(self):
            self.running = False

    class CleanupClient:
        def __init__(self):
            self.orders = [
                AccountOrder("one", "ONE", client_order_id="mm:yes:1", status="resting"),
                AccountOrder("two", "TWO", client_order_id="mm:no:2", status="resting"),
            ]

        def list_account_orders(self, query):
            timeline.append(("list",))
            return list(self.orders)

        def cancel_order(self, *, order_id):
            timeline.append(("cancel", order_id))
            self.orders = [order for order in self.orders if order.order_id != order_id]

    async def scenario():
        config = replace(manager_config(tmp_path), dry_run=False, shutdown_cleanup_delay_seconds=0)
        manager = BotManager(config, cleanup_client=CleanupClient())
        for index, market_id in enumerate(("ONE", "TWO"), start=1):
            selected = pick(market_id)
            child = ChildProcess(
                ticker=market_id,
                process=Process(index),
                log_path=tmp_path / f"{market_id}.log",
                log_handle=io.StringIO(),
                slot_index=index,
                pick=selected,
            )
            manager.bots[market_id] = ManagedBot(selected, child)

        async def wait_process(process, timeout):
            return process.poll() is not None

        manager._wait_process = wait_process
        await manager.stop_all()

        first_cancel = next(index for index, item in enumerate(timeline) if item[0] == "cancel")
        frozen_pids = {
            item[1] for item in timeline[:first_cancel]
            if item[0] == "signal" and item[2] == signal.SIGSTOP
        }
        assert frozen_pids == {1, 2}
        assert not manager.bots
        cleanup = manager.status_snapshot()["monitoring"]["shutdownCleanup"]
        assert cleanup["state"] == "verified"
        assert cleanup["ordersVerifiedAbsent"] is True
        assert cleanup["canceledOrders"] == 2

    asyncio.run(scenario())


def test_manager_rejects_new_bots_after_shutdown_begins(tmp_path):
    async def scenario():
        manager = BotManager(manager_config(tmp_path))
        manager.begin_shutdown()
        manager._spawn_sync = lambda *args: (_ for _ in ()).throw(AssertionError("spawned"))
        assert await manager.start_bot(pick("LATE")) is None
        event = await manager.events.get()
        assert event.event_type == "start_skipped_shutdown"

    asyncio.run(scenario())


def test_manager_never_spawns_past_process_safety_cap(tmp_path):
    async def scenario():
        manager = BotManager(manager_config(tmp_path))
        manager.bots.update({f"MKT-{index}": object() for index in range(MAX_CONCURRENT_BOTS)})
        manager._spawn_sync = lambda *args: (_ for _ in ()).throw(AssertionError("spawned"))

        assert await manager.start_bot(pick("ONE-TOO-MANY")) is None
        event = await manager.events.get()
        assert event.event_type == "start_skipped_capacity"
        assert event.detail["maximum_bots"] == MAX_CONCURRENT_BOTS

    asyncio.run(scenario())


def test_running_markets_keep_slots_when_rank_slips(tmp_path, monkeypatch):
    """Incumbents inside the retention band survive a re-rank.

    Evicting a healthy market restarts it: cold models, lost queue position and
    a spell in `startup` where it cannot quote. A one-place rank slip must not
    cost a market its slot.
    """
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener

    # NEWCOMER now outranks both running markets, which slipped to 2nd and 3rd.
    frame = pd.DataFrame([
        {"Rank": 1, "Ticker": "NEWCOMER", "SearchText": "Newcomer"},
        {"Rank": 2, "Ticker": "RUNNING-A", "SearchText": "Running A"},
        {"Rank": 3, "Ticker": "RUNNING-B", "SearchText": "Running B"},
    ])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)

    screener = Screener(
        client=FakeScreenClient(),
        settings={},
        output_path=tmp_path / "screen.csv",
        default_yes_budget_cents=100,
        default_no_budget_cents=100,
        max_bots=2,
        minimum_carryover_value_cents=20,
    )

    current = {"RUNNING-A": pick("RUNNING-A"), "RUNNING-B": pick("RUNNING-B")}
    update = asyncio.run(screener.refresh(current, reason="test"))

    assert update is not None
    assert set(update.kept) == {"RUNNING-A", "RUNNING-B"}
    assert not update.added and not update.removed

    # An incumbent that falls out of the retention band (max_bots * 1.5 = 3)
    # does lose its slot, so genuinely stale markets still rotate out.
    demoted = pd.DataFrame([
        {"Rank": 1, "Ticker": "NEWCOMER", "SearchText": "Newcomer"},
        {"Rank": 2, "Ticker": "OTHER", "SearchText": "Other"},
        {"Rank": 3, "Ticker": "RUNNING-A", "SearchText": "Running A"},
        {"Rank": 4, "Ticker": "RUNNING-B", "SearchText": "Running B"},
    ])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: demoted)
    update = asyncio.run(screener.refresh(current, reason="test"))
    assert update is not None
    assert "RUNNING-B" in set(update.removed)


def test_screener_stamps_picks_from_injected_market_class_resolver(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    import pickle
    import screener as screener_module
    from screener import Screener

    frame = pd.DataFrame([
        {"Rank": 1, "Ticker": "THIN", "SearchText": "Thin", "Spread(c)": 20},
        {"Rank": 2, "Ticker": "CALM", "SearchText": "Calm", "Spread(c)": 2},
        {"Rank": 3, "Ticker": "BROKEN", "SearchText": "Broken", "Spread(c)": 2},
    ])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)
    thin_overrides = (("maximum_contracts_per_order", 2), ("markout_horizons_seconds", (1, 5)))

    def resolver(row):
        if row["Ticker"] == "BROKEN":
            raise RuntimeError("series stats unavailable")
        return ("thinWide", thin_overrides) if float(row["Spread(c)"]) >= 12 else ("thickCalm", ())

    screener = Screener(
        client=FakeScreenClient(), settings={}, output_path=tmp_path / "screen.csv",
        default_yes_budget_cents=100, default_no_budget_cents=100, max_bots=10,
        minimum_carryover_value_cents=20, market_class_resolver=resolver,
    )
    update = asyncio.run(screener.refresh({}, reason="test"))
    assert update is not None
    by_id = update.pick_by_market_id
    assert (by_id["THIN"].market_class, by_id["THIN"].settings_overrides) == ("thinWide", thin_overrides)
    assert (by_id["CALM"].market_class, by_id["CALM"].settings_overrides) == ("thickCalm", ())
    # A failing resolver degrades that market to the default class and warns.
    assert (by_id["BROKEN"].market_class, by_id["BROKEN"].settings_overrides) == ("default", ())
    status = screener.status_snapshot()
    assert any("resolver failed for 1 market" in warning for warning in status["warnings"])
    assert {item["marketId"]: item["marketClass"] for item in status["picks"]} == {
        "THIN": "thinWide", "CALM": "thickCalm", "BROKEN": "default",
    }
    # Picks cross the worker multiprocessing queue and must stay picklable.
    restored = pickle.loads(pickle.dumps(by_id["THIN"]))
    assert restored.runtime_key() == by_id["THIN"].runtime_key()
    assert hash(by_id["THIN"].runtime_key())

    # Without a resolver every pick is "default" with no overrides.
    plain = Screener(
        client=FakeScreenClient(), settings={}, output_path=tmp_path / "screen2.csv",
        default_yes_budget_cents=100, default_no_budget_cents=100, max_bots=10,
        minimum_carryover_value_cents=20,
    )
    plain_update = asyncio.run(plain.refresh({}, reason="test"))
    assert {(item.market_class, item.settings_overrides) for item in plain_update.picks} == {("default", ())}


def test_changed_class_overrides_reconcile_running_market(tmp_path, monkeypatch):
    """runtime_key() must cover class + overrides, or edits never reach a running actor."""
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener

    frame = pd.DataFrame([{"Rank": 1, "Ticker": "THIN", "SearchText": "Thin", "Spread(c)": 20}])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)
    new_overrides = (("maximum_contracts_per_order", 2),)

    class InventoryClient(FakeScreenClient):
        def get_positions(self, market_id):
            return [Position(market_id, 100)]

        def get_market_quote(self, market_id):
            return MarketQuote(market_id, 5000, 5000)

    screener = Screener(
        client=InventoryClient(), settings={}, output_path=tmp_path / "screen.csv",
        default_yes_budget_cents=100, default_no_budget_cents=100, max_bots=10,
        minimum_carryover_value_cents=20, market_class_resolver=lambda row: ("thinWide", new_overrides),
    )

    def running(overrides, market_class="thinWide"):
        return ScreenerPick("THIN", "Thin", 100, 100, {"Ticker": "THIN"},
                            market_class=market_class, settings_overrides=overrides)

    # Same budgets, different override value -> changed (worker restarts the actor).
    update = asyncio.run(screener.refresh({"THIN": running((("maximum_contracts_per_order", 5),))}, reason="test"))
    assert update.changed == ("THIN",) and update.kept == ()
    # Same class and overrides -> kept.
    update = asyncio.run(screener.refresh({"THIN": running(new_overrides)}, reason="test"))
    assert update.kept == ("THIN",) and update.changed == ()
    # Class flip without overrides also changes the runtime key.
    update = asyncio.run(screener.refresh({"THIN": running(new_overrides, market_class="toxic")}, reason="test"))
    assert update.changed == ("THIN",)
    # An inventory carryover keeps the running actor's class/overrides so it is "kept", not restarted.
    held = ScreenerPick("HELD", "Held", 100, 100, {"Ticker": "HELD"},
                        market_class="toxic", settings_overrides=(("minimum_expected_edge_cents_to_quote", 6),))
    update = asyncio.run(screener.refresh({"THIN": running(new_overrides), "HELD": held}, reason="test"))
    carried = update.pick_by_market_id["HELD"]
    assert update.inventory_carried == ("HELD",)
    assert (carried.market_class, carried.settings_overrides) == ("toxic", (("minimum_expected_edge_cents_to_quote", 6),))
    assert set(update.kept) == {"THIN", "HELD"} and update.changed == ()


def test_screener_excludes_markets_on_unfunded_exchange_shards(tmp_path, monkeypatch):
    """A market whose Kalshi exchange shard holds no cash cannot be traded
    (every order is rejected with user_not_found), so it must not consume a
    fleet slot; the exclusion is surfaced as a warning."""
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener
    from clients.models import AccountBalance

    frame = pd.DataFrame([
        {"Rank": 1, "Ticker": "KXITFMATCH-1", "SearchText": "Tennis", "Exchange Index": 3},
        {"Rank": 2, "Ticker": "KXINXU-1", "SearchText": "Index", "Exchange Index": 0},
        {"Rank": 3, "Ticker": "KXBTC15M-1", "SearchText": "Crypto", "Exchange Index": 2},
    ])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)

    class ShardAwareClient(FakeScreenClient):
        def get_account_balance(self):
            return AccountBalance(
                available_cash_units=7_180_341, portfolio_value_units=0,
                balance_by_exchange=((0, 7_180_341), (1, 0), (2, 0), (3, 0)),
            )

    screener = Screener(
        client=ShardAwareClient(), settings={}, output_path=tmp_path / "screen.csv",
        default_yes_budget_cents=100, default_no_budget_cents=100,
        max_bots=10, minimum_carryover_value_cents=20,
    )
    update = asyncio.run(screener.refresh({}, reason="test"))
    assert update is not None
    assert [item.market_id for item in update.picks] == ["KXINXU-1"]
    warnings = screener.status_snapshot()["warnings"]
    assert any("unfunded" in w and "shard 2: 1" in w and "shard 3: 1" in w for w in warnings)

    # A client without balance reporting (no breakdown) must not exclude anything.
    plain = Screener(
        client=FakeScreenClient(), settings={}, output_path=tmp_path / "screen2.csv",
        default_yes_budget_cents=100, default_no_budget_cents=100,
        max_bots=10, minimum_carryover_value_cents=20,
    )
    update = asyncio.run(plain.refresh({}, reason="test"))
    assert len(update.picks) == 3


# ---------------------------------------------------------------------------
# Restart carryover of exchange positions (fleet Start)
# ---------------------------------------------------------------------------

ORPHAN = "KXCS2GAME-26SEP021400BIGPAIN-BIG"


class PositionsClient(FakeScreenClient):
    """Screen client that can describe markets and report shard balances."""

    def __init__(self, *, shard3_cash=0, held=()):
        from clients.models import Market

        self.held = set(held)
        self.shard3_cash = shard3_cash
        self.markets = {
            ORPHAN: Market(ORPHAN, title="CS2: BIG vs PAIN", status="active", series_id="KXCS2GAME", exchange_index=0),
            "SCREENED": Market("SCREENED", title="Screened", status="active", series_id="SCR", exchange_index=0),
            "OTHER": Market("OTHER", title="Other", status="active", series_id="OTH", exchange_index=0),
            "SETTLED": Market("SETTLED", title="Settled", status="settled", series_id="SET", exchange_index=0),
            "CLOSED": Market("CLOSED", title="Closed", status="closed", series_id="CLO", exchange_index=0),
            "TENNIS": Market("TENNIS", title="Tennis", status="active", series_id="KXITF", exchange_index=3),
        }

    def get_market(self, market_id):
        if market_id == "MYSTERY":
            raise RuntimeError("market lookup failed")
        return self.markets[market_id]

    def get_positions(self, market_id):
        return [Position(market_id, 100 if market_id in self.held else 0)]

    def get_market_quote(self, market_id):
        return MarketQuote(market_id, 5000, 5000)

    def get_account_balance(self):
        from clients.models import AccountBalance
        return AccountBalance(
            available_cash_units=7_000_000 + self.shard3_cash, portfolio_value_units=0,
            balance_by_exchange=((0, 7_000_000), (2, 0), (3, self.shard3_cash)),
        )


def positions_fixture():
    from clients.models import AccountPosition
    return [
        AccountPosition(ORPHAN, -1_100, market_exposure_units=6_000),      # 11 NO contracts, unscreened
        AccountPosition("SCREENED", 200, market_exposure_units=1_000),     # also screened -> normal pick
        AccountPosition("SETTLED", 100, market_exposure_units=500),
        AccountPosition("CLOSED", 100, market_exposure_units=500),
        AccountPosition("TENNIS", 100, market_exposure_units=500),         # shard 3 unfunded
        AccountPosition("FLAT", 0, market_exposure_units=0),
    ]


def carry_screener(tmp_path, monkeypatch, client, *, max_bots=10, disabled=None, name="screen.csv"):
    pd = pytest.importorskip("pandas")
    import screener as screener_module
    from screener import Screener

    frame = pd.DataFrame([
        {"Rank": 1, "Ticker": "SCREENED", "SearchText": "Screened", "Exchange Index": 0},
        {"Rank": 2, "Ticker": "OTHER", "SearchText": "Other", "Exchange Index": 0},
    ])
    monkeypatch.setattr(screener_module, "screen_markets", lambda source, settings: frame)
    monkeypatch.setattr(screener_module, "build_export_dataframe", lambda value, settings: value)
    return Screener(
        client=client, settings={}, output_path=tmp_path / name,
        default_yes_budget_cents=100, default_no_budget_cents=150,
        max_bots=max_bots, minimum_carryover_value_cents=20,
        disabled_market_ids=(lambda: set(disabled or ())),
    )


def test_startup_carries_unscreened_exchange_positions_reduce_only(tmp_path, monkeypatch):
    from screener import EXCHANGE_POSITION_REASON

    screener = carry_screener(tmp_path, monkeypatch, PositionsClient())
    update = asyncio.run(screener.refresh({}, reason="startup", exchange_positions=positions_fixture()))
    assert update is not None
    by_id = update.pick_by_market_id
    # The orphan gets a bot although it never passed the screener filters ...
    assert set(by_id) == {"SCREENED", "OTHER", ORPHAN}
    orphan = by_id[ORPHAN]
    assert orphan.selection_reason == EXCHANGE_POSITION_REASON == "exchange_position"
    assert orphan.title == "CS2: BIG vs PAIN"
    assert (orphan.yes_budget_cents, orphan.no_budget_cents) == (100, 150)
    assert orphan.ranking["Exchange Index"] == 0 and orphan.ranking["Series Ticker"] == "KXCS2GAME"
    assert orphan.ranking["Position"] == -11.0
    # ... a screened market with a position is a normal pick ...
    assert by_id["SCREENED"].selection_reason == "screen"
    # ... settled/closed markets, the unfunded shard and flat positions are skipped.
    assert update.inventory_carried == (ORPHAN,)
    assert update.added == tuple(sorted(("OTHER", "SCREENED", ORPHAN)))
    status = screener.status_snapshot()
    warnings = status["warnings"]
    assert any(w.startswith("Carried 1 exchange position(s) into the fleet at start") and ORPHAN in w for w in warnings)
    assert any("Skipped 2 exchange position(s) in closed/settled market(s): SETTLED, CLOSED" in w for w in warnings)
    assert any("Skipped 1 exchange position(s) on unfunded Kalshi exchange shard(s) (shard 3: TENNIS)" in w for w in warnings)
    assert status["exchangeCarryover"] == {
        "carried": [ORPHAN], "displaced": 0, "omitted": [], "skippedClosed": ["SETTLED", "CLOSED"],
        "skippedUnfunded": ["3:TENNIS"], "skippedDisabled": [], "leftAlone": [], "unknownMarket": [],
    }
    assert status["changes"]["inventoryCarried"] == [ORPHAN]
    assert [p["selectionReason"] for p in status["picks"] if p["marketId"] == ORPHAN] == ["exchange_position"]

    # A scheduled refresh (no positions handed in) never carries anything new.
    plain = asyncio.run(screener.refresh({}, reason="scheduled"))
    assert set(plain.pick_by_market_id) == {"SCREENED", "OTHER"} and plain.inventory_carried == ()


def test_exchange_position_carryover_funded_shard_and_disabled_and_unknown_market(tmp_path, monkeypatch):
    from clients.models import AccountPosition

    # Cash moved to shard 3: the tennis position is carried too.
    funded = carry_screener(tmp_path, monkeypatch, PositionsClient(shard3_cash=500_000))
    update = asyncio.run(funded.refresh({}, reason="startup", exchange_positions=positions_fixture()))
    assert set(update.inventory_carried) == {ORPHAN, "TENNIS"}
    assert update.pick_by_market_id["TENNIS"].ranking["Exchange Index"] == 3
    assert not any("unfunded" in w for w in funded.status_snapshot()["warnings"])

    # A disabled market is never carried; a market the venue cannot describe
    # is carried anyway (fail safe) with a warning.
    disabled = carry_screener(
        tmp_path, monkeypatch, PositionsClient(), disabled=[ORPHAN], name="screen2.csv",
    )
    update = asyncio.run(disabled.refresh({}, reason="startup", exchange_positions=[
        *positions_fixture(), AccountPosition("MYSTERY", 100, market_exposure_units=700),
    ]))
    assert update.inventory_carried == ("MYSTERY",)
    assert update.pick_by_market_id["MYSTERY"].selection_reason == "exchange_position"
    warnings = disabled.status_snapshot()["warnings"]
    assert any(f"Skipped 1 exchange position(s) in disabled market(s): {ORPHAN}" in w for w in warnings)
    assert any("could not describe (shard assumed 0): MYSTERY: market lookup failed" in w for w in warnings)


def test_exchange_position_carryover_honours_max_bots(tmp_path, monkeypatch):
    from clients.models import AccountPosition

    # max_bots=2: the carryover displaces the lowest-ranked screened pick.
    tight = carry_screener(tmp_path, monkeypatch, PositionsClient(), max_bots=2)
    update = asyncio.run(tight.refresh({}, reason="startup", exchange_positions=positions_fixture()))
    assert [p.market_id for p in update.picks] == ["SCREENED", ORPHAN]
    assert any("displaced 1 lower-ranked seeded market(s) to honor max_bots=2" in w for w in tight.status_snapshot()["warnings"])

    # max_bots=1 with two carryovers: the lower-value one is omitted.
    one = carry_screener(tmp_path, monkeypatch, PositionsClient(shard3_cash=500_000), max_bots=1, name="screen2.csv")
    update = asyncio.run(one.refresh({}, reason="startup", exchange_positions=[
        AccountPosition(ORPHAN, -1_100, market_exposure_units=6_000),
        AccountPosition("TENNIS", 100, market_exposure_units=500),
    ]))
    assert [p.market_id for p in update.picks] == [ORPHAN]
    status = one.status_snapshot()
    assert status["exchangeCarryover"]["omitted"] == ["TENNIS"] and status["exchangeCarryover"]["displaced"] == 1
    assert any("exceeded max_bots=1; omitted 1 lower-value position(s): TENNIS" in w for w in status["warnings"])


def test_exchange_position_carryover_stays_reduce_only_across_refreshes(tmp_path, monkeypatch):
    from screener import EXCHANGE_POSITION_REASON

    client = PositionsClient(held=[ORPHAN, "HELD"])
    screener = carry_screener(tmp_path, monkeypatch, client)
    startup = asyncio.run(screener.refresh({}, reason="startup", exchange_positions=positions_fixture()))
    orphan = startup.pick_by_market_id[ORPHAN]
    held = ScreenerPick("HELD", "Held", 100, 150, {"Ticker": "HELD"})
    # Next refresh: still holding inventory -> carried again, still reduce-only,
    # and kept (same runtime key) rather than restarted; an ordinary running
    # market with inventory is the usual "inventory" carryover.
    later = asyncio.run(screener.refresh({**startup.pick_by_market_id, "HELD": held}, reason="scheduled"))
    assert set(later.inventory_carried) == {ORPHAN, "HELD"}
    assert later.pick_by_market_id[ORPHAN].selection_reason == EXCHANGE_POSITION_REASON
    assert later.pick_by_market_id["HELD"].selection_reason == "inventory"
    assert ORPHAN in later.kept and later.changed == ()
    # Flat again -> dropped like any carryover.
    client.held.discard(ORPHAN)
    flat = asyncio.run(screener.refresh(later.pick_by_market_id, reason="scheduled"))
    assert ORPHAN in flat.removed


def test_launcher_reads_exchange_positions_at_start_and_seeds_carryovers(tmp_path, monkeypatch):
    from clients.models import AccountPosition, AccountPositionQuery
    from launcher import Launcher

    class StartClient:
        def __init__(self, positions=None, error=None):
            self.positions = positions or []
            self.error = error
            self.queries = []

        def list_account_positions(self, query=None):
            self.queries.append(query)
            if self.error is not None:
                raise self.error
            return list(self.positions)

    launcher = Launcher.__new__(Launcher)
    launcher.api_key_id = "key"
    launcher.private_key_path = "pem"
    launcher.last_error = None
    launcher.client = StartClient([
        AccountPosition(ORPHAN, -1_100, market_exposure_units=6_000),
        AccountPosition("FLAT", 0, market_exposure_units=0),
    ])
    launcher.screener = carry_screener(tmp_path, monkeypatch, PositionsClient())

    positions = asyncio.run(launcher._startup_exchange_positions())
    assert [p.market_id for p in positions] == [ORPHAN]
    assert isinstance(launcher.client.queries[0], AccountPositionQuery) and launcher.client.queries[0].nonzero_only

    # The direct seed paths (CSV / fixed ticker) append the carryover picks.
    seed = (ScreenerPick("SCREENED", "Screened", 100, 150, {"Ticker": "SCREENED"}, selection_reason="csv_seed"),)
    picks, carried, warnings = asyncio.run(launcher._with_startup_carryover(seed))
    assert [p.market_id for p in picks] == ["SCREENED", ORPHAN] and carried == (ORPHAN,)
    assert picks[1].selection_reason == "exchange_position"
    assert any(w.startswith("Carried 1 exchange position(s) into the fleet at start") for w in warnings)

    # No credentials -> no lookup; a venue error -> no carryover, surfaced as last_error.
    launcher.api_key_id = None
    assert asyncio.run(launcher._startup_exchange_positions()) is None
    launcher.api_key_id = "key"
    launcher.client = StartClient(error=RuntimeError("venue down"))
    assert asyncio.run(launcher._startup_exchange_positions()) is None
    assert "restart carryover skipped: venue down" in launcher.last_error
    assert asyncio.run(launcher._with_startup_carryover(seed)) == (seed, (), [])
