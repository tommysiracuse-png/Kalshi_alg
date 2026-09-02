"""Windows-specific fleet runner and TCP control-fallback tests.

These tests are platform independent: the TCP fallback is forced by patching
runtime_control._HAS_AF_UNIX and the Windows process primitives are faked, so
the suite passes on both Windows and Linux hosts.
"""

import json
import queue
import socket as socket_module
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import runtime_control
from runtime_control import ControlServer, control_endpoint_path, send_control_command
from ui_api.config import Settings
from ui_api.store import OperationsStore


class TcpControlFallbackTests(unittest.TestCase):
    def _start_tcp_server(self, temporary: str):
        requests = queue.Queue()
        socket_path = Path(temporary) / "launcher.sock"
        server = ControlServer(socket_path, requests, lambda: {"launcher": {"lifecycle": "running"}})
        server.start()
        return server, socket_path

    def test_round_trip_with_valid_token(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(runtime_control, "_HAS_AF_UNIX", False):
                server, socket_path = self._start_tcp_server(temporary)
                try:
                    descriptor = json.loads(control_endpoint_path(socket_path).read_text(encoding="utf-8"))
                    self.assertIsInstance(descriptor["port"], int)
                    self.assertTrue(descriptor["token"])
                    self.assertFalse(socket_path.exists())
                    # send_control_command must auto-detect the TCP descriptor
                    # and stamp the token itself.
                    result = send_control_command(socket_path, {"request_id": "status-1", "action": "status"})
                    self.assertTrue(result["ok"])
                    self.assertEqual(result["result"]["launcher"]["lifecycle"], "running")
                finally:
                    server.stop()
            self.assertFalse(control_endpoint_path(socket_path).exists())

    def test_rejects_missing_or_bad_token(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(runtime_control, "_HAS_AF_UNIX", False):
                server, socket_path = self._start_tcp_server(temporary)
                try:
                    descriptor = json.loads(control_endpoint_path(socket_path).read_text(encoding="utf-8"))
                    for token_fields in ({}, {"token": "wrong-token"}):
                        payload = {"request_id": "status-2", "action": "status", **token_fields}
                        with socket_module.create_connection(("127.0.0.1", descriptor["port"]), timeout=5) as client:
                            client.settimeout(5)
                            client.sendall((json.dumps(payload) + "\n").encode("utf-8"))
                            raw = b""
                            while b"\n" not in raw:
                                chunk = client.recv(4096)
                                if not chunk:
                                    break
                                raw += chunk
                        response = json.loads(raw.split(b"\n", 1)[0].decode("utf-8"))
                        self.assertFalse(response["ok"])
                        self.assertEqual(response["code"], "unauthorized")
                finally:
                    server.stop()


class WindowsServiceRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for name in ("runtime", "logs", "watchdog_state"):
            (self.root / name).mkdir()
        self.store = OperationsStore(
            Settings(self.root, self.root / "runtime", self.root / "logs", self.root / "watchdog_state", "test.service")
        )

    def tearDown(self):
        self.temp.cleanup()

    def _write_status(self, payload):
        (self.root / "runtime" / "launcher_status.json").write_text(json.dumps(payload), encoding="utf-8")

    def test_systemd_dispatches_to_windows_runner_when_systemctl_is_missing(self):
        sentinel = {"service": "test.service", "action": "is-active", "active": False, "output": "inactive"}
        with patch.object(self.store, "_systemd_linux", side_effect=FileNotFoundError("systemctl not found")), \
                patch.object(self.store, "_windows_service", return_value=sentinel), \
                patch("ui_api.store.sys.platform", "win32"):
            self.assertEqual(self.store.systemd("is-active"), sentinel)

    def test_systemd_reraises_when_systemctl_is_missing_off_windows(self):
        with patch.object(self.store, "_systemd_linux", side_effect=FileNotFoundError("systemctl not found")), \
                patch("ui_api.store.sys.platform", "linux"):
            with self.assertRaises(FileNotFoundError):
                self.store.systemd("is-active")

    def test_windows_start_is_active_and_stop_with_fake_process(self):
        state = SimpleNamespace(alive=True, signalled=False)
        spawned = {}

        def fake_popen(command, **kwargs):
            spawned["command"] = command
            spawned["kwargs"] = kwargs
            return SimpleNamespace(pid=4242, kill=lambda: None)

        def fake_signal(pid):
            state.signalled = True
            state.alive = False
            now = int(time.time() * 1000)
            self._write_status({
                "launcher": {"lifecycle": "stopped", "heartbeatAt": now},
                "manager": {"running": False, "shutdownCleanup": {"completedAtMs": now, "ordersVerifiedAbsent": True}},
                "bots": [], "counts": {},
            })

        # The venv stub (4242) hands off to the real interpreter (5151); both
        # must be recorded so the fleet stays "active" if the stub alone dies.
        listing = [(5151, 4242, "python.exe C:\\ws\\lip_launcher.py --runtime-dir C:\\ws\\runtime")]
        with patch("ui_api.store.subprocess.Popen", side_effect=fake_popen), \
                patch.object(self.store, "_windows_pid_alive", lambda pid: state.alive), \
                patch.object(self.store, "_windows_list_python_processes", lambda: listing), \
                patch.object(self.store, "_windows_signal_launcher", fake_signal):
            started = self.store._windows_service("start")
            self.assertTrue(started["active"])
            self.assertEqual(self.store.launcher_pid_path.read_text(encoding="utf-8").split(), ["4242", "5151"])
            self.assertEqual(self.store._read_launcher_pid(), 4242)
            self.assertIn("interpreter 5151", started["output"])
            self.assertIn("lip_launcher.py", str(spawned["command"][1]))
            self.assertEqual(spawned["kwargs"]["cwd"], str(self.root))
            self.assertTrue(self.store.launcher_console_log.exists())

            active = self.store._windows_service("is-active")
            self.assertTrue(active["active"])

            stopped = self.store._windows_service("stop")
            self.assertTrue(state.signalled)
            self.assertFalse(stopped["active"])
            self.assertFalse(self.store.launcher_pid_path.exists())
            self.assertTrue(stopped["shutdownCleanup"]["ordersVerifiedAbsent"])

            inactive = self.store._windows_service("is-active")
            self.assertFalse(inactive["active"])

    def test_windows_stop_prefers_the_control_endpoint_over_ctrl_break(self):
        state = SimpleNamespace(alive=True, signalled=False, control_actions=[])

        def fake_send(path, payload, timeout=5.0):
            state.control_actions.append(payload["action"])
            state.alive = False
            now = int(time.time() * 1000)
            self._write_status({
                "launcher": {"lifecycle": "stopped", "heartbeatAt": now},
                "manager": {"running": False, "shutdownCleanup": {"completedAtMs": now, "ordersVerifiedAbsent": True}},
                "bots": [], "counts": {},
            })
            return {"ok": True, "request_id": payload["request_id"], "result": {"shutdownRequested": True}}

        self.store.launcher_pid_path.write_text("4242\n5151\n", encoding="utf-8")
        with patch("ui_api.store.control_endpoint_available", lambda path, timeout=1.0: True), \
                patch("ui_api.store.send_control_command", fake_send), \
                patch.object(self.store, "_windows_pid_alive", lambda pid: state.alive), \
                patch.object(self.store, "_windows_signal_launcher", lambda pid: setattr(state, "signalled", True)):
            stopped = self.store._windows_service("stop")
        self.assertEqual(state.control_actions, ["shutdown"])
        self.assertFalse(state.signalled)
        self.assertFalse(stopped["active"])
        self.assertTrue(stopped["shutdownCleanup"]["ordersVerifiedAbsent"])

    def test_windows_stop_falls_back_to_ctrl_break_when_control_endpoint_is_silent(self):
        state = SimpleNamespace(alive=True, signalled=False)

        def fake_signal(pid):
            state.signalled = True
            state.alive = False
            now = int(time.time() * 1000)
            self._write_status({
                "launcher": {"lifecycle": "stopped", "heartbeatAt": now},
                "manager": {"running": False, "shutdownCleanup": {"completedAtMs": now, "ordersVerifiedAbsent": True}},
                "bots": [], "counts": {},
            })

        self.store.launcher_pid_path.write_text("4242\n", encoding="utf-8")
        with patch("ui_api.store.control_endpoint_available", lambda path, timeout=1.0: True), \
                patch("ui_api.store.send_control_command", side_effect=OSError("connection refused")), \
                patch.object(self.store, "_windows_pid_alive", lambda pid: state.alive), \
                patch.object(self.store, "_windows_signal_launcher", fake_signal):
            stopped = self.store._windows_service("stop")
        self.assertTrue(state.signalled)
        self.assertFalse(stopped["active"])

    def test_windows_stop_without_cleanup_verification_raises(self):
        state = SimpleNamespace(alive=True)

        def fake_signal(pid):
            state.alive = False
            self._write_status({
                "launcher": {"lifecycle": "stopped", "heartbeatAt": int(time.time() * 1000)},
                "manager": {"running": False},
                "bots": [], "counts": {},
            })

        self.store.launcher_pid_path.write_text("4242\n", encoding="utf-8")
        with patch.object(self.store, "_windows_pid_alive", lambda pid: state.alive), \
                patch.object(self.store, "_windows_signal_launcher", fake_signal):
            with self.assertRaises(RuntimeError) as raised:
                self.store._windows_service("stop")
        self.assertIn("not verified", str(raised.exception))
        self.assertFalse(self.store.launcher_pid_path.exists())


class OrphanWorkerSweepTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for name in ("runtime", "logs", "watchdog_state"):
            (self.root / name).mkdir()
        self.store = OperationsStore(
            Settings(self.root, self.root / "runtime", self.root / "logs", self.root / "watchdog_state", "test.service")
        )

    def tearDown(self):
        self.temp.cleanup()

    # Real spawn children never carry the workspace path on their command line.
    SPAWN_CHILD = (
        '"C:\\Python312\\python.exe" "-c" "from multiprocessing.spawn import spawn_main; '
        'spawn_main(parent_pid=999, pipe_handle=1234)" "--multiprocessing-fork"'
    )

    def _fake_run(self, commands):
        def fake_run(args, **kwargs):
            commands.append(list(args))
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return fake_run

    def test_sweep_kills_spawn_children_whose_parent_is_gone(self):
        processes = [
            (100, 999, self.SPAWN_CHILD),
            (101, 100, self.SPAWN_CHILD),
            (102, 999, "python.exe C:\\ws\\lip_launcher.py --runtime-dir C:\\ws\\runtime"),
            (103, 777, "python.exe -m optimizer.main --workers 2"),
        ]
        commands = []
        with patch("ui_api.store.subprocess.run", self._fake_run(commands)):
            killed = self.store._windows_sweep_orphaned_workers(processes=processes)

        self.assertEqual(killed, [100])
        self.assertEqual(commands, [["taskkill", "/PID", "100", "/F"]])
        self.assertIn("swept 1 orphaned worker", self.store.launcher_console_log.read_text(encoding="utf-8"))

    def test_sweep_uses_last_published_worker_pids_when_that_launcher_is_dead(self):
        (self.root / "runtime" / "launcher_status.json").write_text(json.dumps({
            "launcher": {"pid": 999, "lifecycle": "running"},
            "workers": [{"workerId": "worker-00", "pid": 200}, {"workerId": "worker-01", "pid": 201}],
        }), encoding="utf-8")
        processes = [
            (200, 150, self.SPAWN_CHILD),
            (150, 999, "python.exe -m something_else"),
            (300, 150, self.SPAWN_CHILD),
        ]
        commands = []
        with patch("ui_api.store.subprocess.run", self._fake_run(commands)):
            killed = self.store._windows_sweep_orphaned_workers(processes=processes)

        # 200: published by the dead launcher (999) -> killed even though its
        # direct parent 150 is alive. 300: live parent, never published -> kept.
        # 201: not running any more -> nothing to do.
        self.assertEqual(killed, [200])

    def test_sweep_leaves_published_workers_alone_while_their_launcher_lives(self):
        (self.root / "runtime" / "launcher_status.json").write_text(json.dumps({
            "launcher": {"pid": 999, "lifecycle": "running"},
            "workers": [{"workerId": "worker-00", "pid": 200}],
        }), encoding="utf-8")
        processes = [(999, 1, "python.exe C:\\ws\\lip_launcher.py"), (200, 999, self.SPAWN_CHILD)]
        commands = []
        with patch("ui_api.store.subprocess.run", self._fake_run(commands)):
            self.assertEqual(self.store._windows_sweep_orphaned_workers(processes=processes), [])
        self.assertEqual(commands, [])

    def test_sweep_is_a_no_op_when_process_listing_fails(self):
        with patch("ui_api.store.subprocess.run", side_effect=TypeError("no popen here")):
            self.assertEqual(self.store._windows_sweep_orphaned_workers(), [])


if __name__ == "__main__":
    unittest.main()
