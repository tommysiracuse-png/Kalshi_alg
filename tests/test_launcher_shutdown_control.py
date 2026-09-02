"""The launcher's control endpoint accepts a console-independent 'shutdown' action."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

from launcher import Launcher
from runtime_control import ALLOWED_ACTIONS, ControlRequest


def test_shutdown_is_an_allowed_control_action():
    assert "shutdown" in ALLOWED_ACTIONS


def _bare_launcher(tmp_path: Path, calls: list) -> Launcher:
    launcher = Launcher.__new__(Launcher)
    launcher.pending_action = None
    launcher.last_error = None
    launcher.shutdown_requested = asyncio.Event()
    launcher.manager = SimpleNamespace(
        current_picks={}, bots={}, begin_shutdown=lambda: calls.append("begin_shutdown"),
    )
    launcher.watchdog_disable_file = tmp_path / "watchdog_disable_list.json"
    launcher.watchdog_disable_file.write_text("{}", encoding="utf-8")
    launcher.screen_path = tmp_path / "missing_screen.csv"
    launcher.arguments = SimpleNamespace(
        ticker_column="Ticker", title_column="Title", yes_budget_cents=100, no_budget_cents=100,
        yes_budget_column="", no_budget_column="",
    )
    return launcher


def test_launcher_shutdown_control_requests_graceful_stop(tmp_path: Path):
    calls: list = []
    launcher = _bare_launcher(tmp_path, calls)
    request = ControlRequest(request_id="stop-1", action="shutdown", ticker=None)

    asyncio.run(launcher._handle_control(request))

    assert request.result == {"ok": True, "result": {"shutdownRequested": True}}
    assert request.done.is_set()
    assert launcher.shutdown_requested.is_set()
    assert calls == ["begin_shutdown"]
    assert launcher.pending_action is None


def test_launcher_rejects_unknown_control_actions(tmp_path: Path):
    launcher = _bare_launcher(tmp_path, [])
    request = ControlRequest(request_id="x-1", action="explode", ticker=None)

    asyncio.run(launcher._handle_control(request))

    assert request.result["ok"] is False
    assert "unsupported action" in request.result["message"]
    assert not launcher.shutdown_requested.is_set()
