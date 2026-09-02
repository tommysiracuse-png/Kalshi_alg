"""Fleet guardian: keeps the selected session trading unattended.

Every ``CHECK_INTERVAL_S`` it reads the launcher snapshot and restarts the
fleet (Stop, then Start of the selected session) only when the fleet is
provably wedged or dead: no bot has an order book for minutes after startup,
the launcher heartbeat has stalled, or the launcher exited without finalizing
its run. Operator intent wins: a fleet the operator stopped stays stopped, and
every guardian action is written to the audit log under ``fleet-guardian``
and to ``runtime/fleet_guardian.json``.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

GUARDIAN_OPERATOR = "fleet-guardian"
CHECK_INTERVAL_S = 30.0
# A fresh run needs time to subscribe every book before "bookless" means anything.
STARTUP_GRACE_S = 300.0
# No bot has a book (the 23:59 broker-stall signature) for this long -> wedged.
WEDGE_SUSTAIN_S = 180.0
# Fewer than half the bots have a book for this long -> wedged. Long enough
# to ride out a slow reconcile cycle (a 60-market re-add through a busy
# broker takes minutes) without forcing a stop that would itself hang on it.
DEGRADED_SUSTAIN_S = 900.0
# The launcher rewrites its snapshot every ~2 s; this long without one -> hung.
HEARTBEAT_HUNG_S = 180.0
# Minimum gap between consecutive guardian restarts (grows with each one in 2 h).
RESTART_BACKOFF_S: Tuple[float, ...] = (600.0, 1200.0, 2400.0, 3600.0)
# A dead launcher is auto-started only after this long, so an operator who is
# mid-restart is never raced.
CRASH_RESTART_DELAY_S = 60.0


@dataclass
class Observation:
    now_ms: int
    service_active: bool = False
    lifecycle: Optional[str] = None
    heartbeat_ms: Optional[int] = None
    run_started_ms: Optional[int] = None
    active_run: bool = False
    last_run_status: Optional[str] = None
    operator_intends_running: bool = False
    bots: int = 0
    book_ready: int = 0
    watchdog_modes: Dict[str, int] = field(default_factory=dict)
    workers: int = 0
    workers_reconciling: int = 0


@dataclass
class GuardianState:
    wedged_since_ms: Optional[int] = None
    degraded_since_ms: Optional[int] = None
    dead_since_ms: Optional[int] = None
    restarts: List[int] = field(default_factory=list)
    last_check_ms: Optional[int] = None
    last_action: Optional[str] = None
    last_reason: Optional[str] = None
    last_error: Optional[str] = None


def _seconds(now_ms: int, since_ms: int) -> int:
    return int((now_ms - since_ms) / 1000)


def decide(obs: Observation, state: GuardianState) -> Tuple[str, str]:
    """Return (action, reason); action is 'none', 'restart' or 'start'.

    Mutates the sustain timers in ``state`` so a condition must hold across
    consecutive checks before it triggers.
    """
    now = obs.now_ms
    recent = [stamp for stamp in state.restarts if now - stamp < 2 * 3600_000]
    if recent:
        gap_s = RESTART_BACKOFF_S[min(len(recent) - 1, len(RESTART_BACKOFF_S) - 1)]
        if now - recent[-1] < gap_s * 1000:
            return "none", f"backoff: last guardian restart {_seconds(now, recent[-1])}s ago (minimum gap {int(gap_s)}s)"

    if not obs.service_active:
        state.wedged_since_ms = None
        state.degraded_since_ms = None
        crashed = obs.active_run or obs.last_run_status == "interrupted"
        if not crashed or not obs.operator_intends_running:
            state.dead_since_ms = None
            return "none", "launcher not running" + ("" if obs.operator_intends_running else " (operator stopped it)")
        if state.dead_since_ms is None:
            state.dead_since_ms = now
        if now - state.dead_since_ms < CRASH_RESTART_DELAY_S * 1000:
            return "none", f"launcher dead for {_seconds(now, state.dead_since_ms)}s; waiting before auto-start"
        return "start", "launcher exited without finalizing its run"

    state.dead_since_ms = None
    if obs.lifecycle != "running":
        return "none", f"lifecycle {obs.lifecycle}"
    if obs.run_started_ms is None or now - obs.run_started_ms < STARTUP_GRACE_S * 1000:
        state.wedged_since_ms = None
        state.degraded_since_ms = None
        return "none", "startup grace"
    if obs.heartbeat_ms is not None and now - obs.heartbeat_ms > HEARTBEAT_HUNG_S * 1000:
        return "restart", f"launcher heartbeat stale for {_seconds(now, obs.heartbeat_ms)}s"
    if obs.bots > 0 and obs.book_ready == 0:
        if state.wedged_since_ms is None:
            state.wedged_since_ms = now
        held = _seconds(now, state.wedged_since_ms)
        if now - state.wedged_since_ms >= WEDGE_SUSTAIN_S * 1000:
            return "restart", f"{obs.bots} bots without an order book for {held}s (watchdog {obs.watchdog_modes})"
        return "none", f"bookless for {held}s; waiting for sustain"
    state.wedged_since_ms = None
    if obs.bots >= 4 and obs.book_ready * 2 < obs.bots:
        if state.degraded_since_ms is None:
            state.degraded_since_ms = now
        held = _seconds(now, state.degraded_since_ms)
        if now - state.degraded_since_ms >= DEGRADED_SUSTAIN_S * 1000:
            return "restart", f"only {obs.book_ready}/{obs.bots} bots have an order book for {held}s"
        return "none", f"degraded {obs.book_ready}/{obs.bots} for {held}s; waiting for sustain"
    state.degraded_since_ms = None
    return "none", "healthy"


def _pick(mapping: Any, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


class FleetGuardian:
    def __init__(self, store: Any, *, interval_s: float = CHECK_INTERVAL_S) -> None:
        self.store = store
        self.interval_s = float(interval_s)
        self.state = GuardianState()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ observe
    def observe(self, now_ms: Optional[int] = None) -> Observation:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        obs = Observation(now_ms=now)
        try:
            obs.service_active = self.store._service_state() == "active"
        except Exception:
            obs.service_active = False
        try:
            data = (self.store.status() or {}).get("data") or {}
        except Exception:
            data = {}
        launcher = data.get("launcher") if isinstance(data.get("launcher"), dict) else {}
        obs.lifecycle = launcher.get("lifecycle")
        heartbeat = launcher.get("heartbeatAt")
        obs.heartbeat_ms = int(heartbeat) if isinstance(heartbeat, (int, float)) else None
        started = launcher.get("startedAt")
        obs.run_started_ms = int(started) if isinstance(started, (int, float)) else None
        bots = data.get("bots") if isinstance(data.get("bots"), list) else []
        obs.bots = len(bots)
        obs.book_ready = sum(1 for bot in bots if isinstance(bot, dict) and bot.get("bookReady"))
        counts = data.get("counts") if isinstance(data.get("counts"), dict) else {}
        modes = counts.get("watchdogModes")
        obs.watchdog_modes = dict(modes) if isinstance(modes, dict) else {}
        workers = data.get("workers") if isinstance(data.get("workers"), list) else []
        obs.workers = len(workers)
        obs.workers_reconciling = sum(1 for w in workers if isinstance(w, dict) and w.get("phase") == "reconciling")
        try:
            obs.active_run = self.store.sessions.active_run() is not None
        except Exception:
            obs.active_run = False
        try:
            runs = self.store.sessions.list_runs(include_artifact_bytes=False)
            newest = max(runs, key=lambda run: int(_pick(run, "createdAt", "createdAtMs", "created_at_ms") or 0)) if runs else None
            obs.last_run_status = _pick(newest, "status") if newest else None
        except Exception:
            obs.last_run_status = None
        obs.operator_intends_running = self._operator_intends_running(obs.last_run_status is not None or obs.active_run)
        return obs

    def _operator_intends_running(self, default: bool) -> bool:
        """True when the operator's last fleet start came after their last fleet stop."""
        try:
            entries = self.store.audit.list(200)
        except Exception:
            return default
        last_start = last_stop = None
        for entry in entries:
            if entry.get("target") != "fleet" or entry.get("operator") == GUARDIAN_OPERATOR:
                continue
            stamp = int(entry.get("timestamp_ms") or 0)
            if entry.get("action") == "start":
                last_start = max(last_start or 0, stamp)
            elif entry.get("action") == "stop":
                last_stop = max(last_stop or 0, stamp)
        if last_start is None and last_stop is None:
            return default
        return (last_start or 0) > (last_stop or 0)

    # --------------------------------------------------------------------- act
    def _log(self, message: str, now_ms: int) -> None:
        try:
            with Path(self.store.launcher_console_log).open("ab") as console:
                console.write(f"----- fleet guardian: {message} at {now_ms} -----\n".encode("utf-8"))
        except OSError:
            pass

    def _control(self, action: str) -> Dict[str, Any]:
        return self.store.control(action, ticker=None, operator=GUARDIAN_OPERATOR, request_id=str(uuid.uuid4()))

    def act(self, action: str, reason: str, now_ms: int) -> None:
        self._log(f"{action}: {reason}", now_ms)
        if action == "restart":
            try:
                self._control("stop")
            except Exception as exc:  # a forced stop still stops; the start below is what matters
                self._log(f"stop reported: {exc}", now_ms)
        try:
            self.store.reconcile_stale_runs()
        except Exception:
            pass
        self._control("start")
        self.state.restarts.append(now_ms)
        self._log(f"started the selected session after {action}", now_ms)

    def run_once(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        obs = self.observe(now_ms)
        action, reason = decide(obs, self.state)
        self.state.last_check_ms = obs.now_ms
        self.state.last_reason = reason
        if action != "none":
            self.state.last_action = f"{action} @ {obs.now_ms}"
            try:
                self.act(action, reason, obs.now_ms)
                self.state.last_error = None
            except Exception as exc:
                self.state.last_error = f"{action} failed: {exc}"
                self._log(self.state.last_error, obs.now_ms)
                self.state.restarts.append(obs.now_ms)
        self._persist(obs, action)
        return {"action": action, "reason": reason, "observation": asdict(obs)}

    def _persist(self, obs: Observation, action: str) -> None:
        try:
            path = Path(self.store.settings.runtime_dir) / "fleet_guardian.json"
            payload = {
                "enabled": True,
                "intervalSeconds": self.interval_s,
                "action": action,
                "state": asdict(self.state),
                "observation": asdict(obs),
            }
            path.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
        except OSError:
            pass

    # ------------------------------------------------------------------ thread
    def _loop(self) -> None:
        while not self._stop.wait(self.interval_s):
            try:
                self.run_once()
            except Exception as exc:
                self.state.last_error = f"guardian check failed: {exc}"

    def start(self) -> threading.Thread:
        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="fleet-guardian", daemon=True)
            self._thread.start()
        return self._thread

    def stop(self) -> None:
        self._stop.set()
