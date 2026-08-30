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
