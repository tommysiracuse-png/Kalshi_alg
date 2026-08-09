import json
import errno
import queue
import tempfile
import threading
import unittest
from pathlib import Path

from runtime_control import ControlServer, send_control_command


class ControlServerTests(unittest.TestCase):
    def test_status_and_idempotent_command(self):
        with tempfile.TemporaryDirectory() as temporary:
            requests = queue.Queue()
            server = ControlServer(Path(temporary) / "launcher.sock", requests, lambda: {"launcher": {"lifecycle": "running"}})
            try:
                server.start()
            except PermissionError as exc:
                if exc.errno == errno.EPERM:
                    self.skipTest("sandbox forbids Unix-domain socket binding")
                raise
            try:
                status = send_control_command(server.socket_path, {"request_id": "status-1", "action": "status"})
                self.assertEqual(status["result"]["launcher"]["lifecycle"], "running")

                def complete():
                    item = requests.get(timeout=2)
                    item.result = {"ok": True, "result": {"refreshed": True}}
                    item.done.set()

                worker = threading.Thread(target=complete)
                worker.start()
                payload = {"request_id": "refresh-1", "action": "refresh"}
                first = send_control_command(server.socket_path, payload)
                worker.join()
                second = send_control_command(server.socket_path, payload)
                self.assertTrue(first["ok"])
                self.assertEqual(first, second)
                self.assertTrue(requests.empty())
            finally:
                server.stop()

    def test_rejects_missing_ticker(self):
        with tempfile.TemporaryDirectory() as temporary:
            requests = queue.Queue()
            server = ControlServer(Path(temporary) / "launcher.sock", requests, lambda: {})
            try:
                server.start()
            except PermissionError as exc:
                if exc.errno == errno.EPERM:
                    self.skipTest("sandbox forbids Unix-domain socket binding")
                raise
            try:
                result = send_control_command(server.socket_path, {"request_id": "disable-1", "action": "disable_ticker"})
                self.assertFalse(result["ok"])
                self.assertEqual(result["code"], "invalid_request")
            finally:
                server.stop()


if __name__ == "__main__":
    unittest.main()
