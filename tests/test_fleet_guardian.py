"""Fleet guardian decisions and its stop/start behaviour against a fake store."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from ui_api import guardian
from ui_api.guardian import FleetGuardian, GuardianState, Observation, decide

MIN = 60_000
T0 = 1_788_300_000_000


def healthy(now_ms, **overrides):
    base = dict(
        now_ms=now_ms, service_active=True, lifecycle="running", heartbeat_ms=now_ms - 1_000,
        run_started_ms=now_ms - 20 * MIN, active_run=True, last_run_status="running",
        operator_intends_running=True, bots=45, book_ready=45,
        watchdog_modes={"normal": 30, "reduction_only": 15}, workers=4, workers_reconciling=0,
    )
    base.update(overrides)
    return Observation(**base)


class DecideTests(unittest.TestCase):
    def test_healthy_fleet_needs_nothing(self):
        state = GuardianState()
        self.assertEqual(decide(healthy(T0), state), ("none", "healthy"))

    def test_bookless_fleet_restarts_only_after_sustain(self):
        state = GuardianState()
        wedged = dict(book_ready=0, watchdog_modes={"startup": 45})
        action, _ = decide(healthy(T0, **wedged), state)
        self.assertEqual(action, "none")
        action, _ = decide(healthy(T0 + 2 * MIN, **wedged), state)
        self.assertEqual(action, "none")
        action, reason = decide(healthy(T0 + 3 * MIN + 1_000, **wedged), state)
        self.assertEqual(action, "restart")
        self.assertIn("45 bots without an order book", reason)

    def test_recovery_resets_the_sustain_timer(self):
        state = GuardianState()
        decide(healthy(T0, book_ready=0), state)
        decide(healthy(T0 + 2 * MIN), state)  # books came back
        action, _ = decide(healthy(T0 + 4 * MIN, book_ready=0), state)
        self.assertEqual(action, "none")

    def test_startup_grace_ignores_bookless_new_runs(self):
        state = GuardianState()
        obs = healthy(T0, book_ready=0, run_started_ms=T0 - 2 * MIN)
        self.assertEqual(decide(obs, state), ("none", "startup grace"))
        self.assertIsNone(state.wedged_since_ms)

    def test_stale_heartbeat_restarts(self):
        state = GuardianState()
        action, reason = decide(healthy(T0, heartbeat_ms=T0 - 4 * MIN), state)
        self.assertEqual(action, "restart")
        self.assertIn("heartbeat stale", reason)

    def test_degraded_fleet_restarts_only_after_the_degraded_window(self):
        state = GuardianState()
        degraded = dict(book_ready=10)
        window_ms = int(guardian.DEGRADED_SUSTAIN_S * 1000)
        self.assertEqual(decide(healthy(T0, **degraded), state)[0], "none")
        self.assertEqual(decide(healthy(T0 + window_ms - MIN, **degraded), state)[0], "none")
        self.assertEqual(decide(healthy(T0 + window_ms + 1_000, **degraded), state)[0], "restart")

    def test_backoff_blocks_a_second_restart_inside_ten_minutes(self):
        state = GuardianState(restarts=[T0 - 5 * MIN])
        action, reason = decide(healthy(T0, book_ready=0, run_started_ms=T0 - 10 * MIN), state)
        self.assertEqual(action, "none")
        self.assertIn("backoff", reason)
        state = GuardianState(restarts=[T0 - 11 * MIN], wedged_since_ms=T0 - 4 * MIN)
        self.assertEqual(decide(healthy(T0, book_ready=0), state)[0], "restart")

    def test_dead_launcher_autostarts_after_delay_when_operator_wanted_it_running(self):
        state = GuardianState()
        dead = dict(service_active=False, lifecycle="stopped", active_run=False, last_run_status="interrupted")
        self.assertEqual(decide(healthy(T0, **dead), state)[0], "none")
        action, reason = decide(healthy(T0 + 61_000, **dead), state)
        self.assertEqual(action, "start")
        self.assertIn("without finalizing", reason)

    def test_dead_launcher_stays_down_after_operator_stop(self):
        state = GuardianState()
        stopped = dict(service_active=False, lifecycle="stopped", active_run=False,
                       last_run_status="interrupted", operator_intends_running=False)
        decide(healthy(T0, **stopped), state)
        action, reason = decide(healthy(T0 + 5 * MIN, **stopped), state)
        self.assertEqual(action, "none")
        self.assertIn("operator stopped it", reason)

    def test_cleanly_finished_run_is_left_alone(self):
        state = GuardianState()
        done = dict(service_active=False, lifecycle="stopped", active_run=False, last_run_status="stopped")
        self.assertEqual(decide(healthy(T0 + 5 * MIN, **done), state)[0], "none")


class FakeSessions:
    def __init__(self, active, runs):
        self._active, self._runs = active, runs

    def active_run(self):
        return self._active

    def list_runs(self, **kwargs):
        return self._runs


class FakeAudit:
    def __init__(self, entries):
        self.entries = entries

    def list(self, limit=100):
        return self.entries


class FakeStore:
    def __init__(self, root, status, service_state, audit_entries, runs, active=None):
        self.settings = SimpleNamespace(runtime_dir=root)
        self.launcher_console_log = root / "launcher_console.log"
        self._status, self._service_state_value = status, service_state
        self.sessions = FakeSessions(active, runs)
        self.audit = FakeAudit(audit_entries)
        self.calls = []
        self.stop_error = None

    def _service_state(self):
        return self._service_state_value

    def status(self):
        return {"data": self._status, "source": {}}

    def reconcile_stale_runs(self):
        self.calls.append("reconcile")

    def control(self, action, *, ticker, operator, request_id):
        self.calls.append(f"{action}:{operator}")
        if action == "stop" and self.stop_error:
            raise RuntimeError(self.stop_error)
        return {"ok": True}


class RunOnceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def _wedged_status(self, now_ms):
        return {
            "launcher": {"lifecycle": "running", "heartbeatAt": now_ms - 500, "startedAt": now_ms - 30 * MIN},
            "counts": {"watchdogModes": {"startup": 3}},
            "bots": [{"ticker": "A", "bookReady": False}, {"ticker": "B", "bookReady": False}, {"ticker": "C", "bookReady": False}],
            "workers": [{"workerId": "w0", "phase": "reconciling"}],
        }

    def test_wedged_fleet_is_stopped_then_started_even_when_stop_is_not_verified(self):
        audit = [{"timestamp_ms": T0 - 40 * MIN, "action": "start", "target": "fleet", "operator": "operator", "result": "success"}]
        store = FakeStore(self.root, self._wedged_status(T0), "active", audit, runs=[{"status": "running", "createdAtMs": T0 - 30 * MIN}], active={"id": "r1"})
        store.stop_error = "bot order cancellation was not verified"
        watch = FleetGuardian(store)
        watch.state.wedged_since_ms = T0 - 4 * MIN
        result = watch.run_once(T0)
        self.assertEqual(result["action"], "restart")
        self.assertEqual(store.calls, [f"stop:{guardian.GUARDIAN_OPERATOR}", "reconcile", f"start:{guardian.GUARDIAN_OPERATOR}"])
        self.assertEqual(watch.state.restarts, [T0])
        self.assertIn("fleet guardian: restart", store.launcher_console_log.read_text(encoding="utf-8"))
        persisted = json.loads((self.root / "fleet_guardian.json").read_text(encoding="utf-8"))
        self.assertEqual(persisted["action"], "restart")

    def test_operator_stop_after_start_means_no_autostart(self):
        audit = [
            {"timestamp_ms": T0 - 10 * MIN, "action": "stop", "target": "fleet", "operator": "operator", "result": "failed"},
            {"timestamp_ms": T0 - 40 * MIN, "action": "start", "target": "fleet", "operator": "operator", "result": "success"},
        ]
        status = {"launcher": {"lifecycle": "stopped"}, "counts": {}, "bots": [], "workers": []}
        store = FakeStore(self.root, status, "inactive", audit, runs=[{"status": "interrupted", "createdAtMs": T0 - 40 * MIN}])
        watch = FleetGuardian(store)
        watch.state.dead_since_ms = T0 - 5 * MIN
        result = watch.run_once(T0)
        self.assertEqual(result["action"], "none")
        self.assertEqual(store.calls, [])

    def test_crashed_launcher_is_started_again_and_guardian_entries_do_not_count_as_intent(self):
        audit = [
            {"timestamp_ms": T0 - 2 * MIN, "action": "stop", "target": "fleet", "operator": guardian.GUARDIAN_OPERATOR, "result": "success"},
            {"timestamp_ms": T0 - 40 * MIN, "action": "start", "target": "fleet", "operator": "operator", "result": "success"},
        ]
        status = {"launcher": {"lifecycle": "running"}, "counts": {}, "bots": [], "workers": []}
        store = FakeStore(self.root, status, "inactive", audit, runs=[{"status": "interrupted", "createdAtMs": T0 - 40 * MIN}])
        watch = FleetGuardian(store)
        watch.state.dead_since_ms = T0 - 2 * MIN
        result = watch.run_once(T0)
        self.assertEqual(result["action"], "start")
        self.assertEqual(store.calls, ["reconcile", f"start:{guardian.GUARDIAN_OPERATOR}"])


if __name__ == "__main__":
    unittest.main()
