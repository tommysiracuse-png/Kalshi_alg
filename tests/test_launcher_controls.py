import tempfile
import threading
import unittest
from pathlib import Path

from lip_launcher import LaunchPick, build_child_command, clear_disabled_ticker, disable_ticker, load_disable_list


class DisableListTests(unittest.TestCase):
    def test_concurrent_updates_are_not_lost(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "disabled.json"
            threads = [threading.Thread(target=disable_ticker, args=(path, f"TEST-{index}", "test")) for index in range(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(len(load_disable_list(path)), 10)
            self.assertTrue(clear_disabled_ticker(path, "TEST-3"))
            self.assertNotIn("TEST-3", load_disable_list(path))

    def test_child_command_includes_manager_socket(self):
        command = build_child_command(
            python_executable="python3",
            bot_script_path=Path("V1.py"),
            pick=LaunchPick("MKT", "Market", 100, 200, {"Ticker": "MKT"}),
            use_demo=False,
            dry_run=False,
            subaccount=None,
            api_key_id=None,
            private_key_path=None,
            pass_credentials_via_cli=False,
            watchdog_state_file=None,
            watchdog_state_refresh_seconds=3,
            watchdog_extreme_stale_seconds=30,
            control_socket=Path("runtime/bots/MKT.sock"),
        )
        self.assertIn("--control-socket", command)
        self.assertIn("runtime/bots/MKT.sock", command)


if __name__ == "__main__":
    unittest.main()
