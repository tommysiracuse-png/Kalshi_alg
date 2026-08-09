import asyncio
import io
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from bot_manager import BotManager, BotManagerConfig, ManagedBot
from clients.models import MarketQuote, Position
from fleet_models import ScreenerPick, ScreenerUpdate
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
