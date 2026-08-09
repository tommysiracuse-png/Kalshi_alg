import asyncio
import errno
import os

import pytest

from runtime_control import BotControlServer, send_bot_command


def test_bot_control_status_shutdown_and_permissions(tmp_path):
    async def scenario():
        shutdown_calls = []

        async def shutdown():
            shutdown_calls.append(True)

        server = BotControlServer(tmp_path / "bot.sock", lambda: {"marketId": "MKT", "bookReady": True}, shutdown)
        try:
            await server.start()
        except PermissionError as exc:
            if exc.errno == errno.EPERM:
                pytest.skip("sandbox forbids Unix-domain socket binding")
            raise
        try:
            assert os.stat(server.socket_path).st_mode & 0o777 == 0o600
            status = await asyncio.to_thread(
                send_bot_command, server.socket_path, {"request_id": "status", "action": "status"}
            )
            first = await asyncio.to_thread(
                send_bot_command, server.socket_path, {"request_id": "stop-1", "action": "shutdown"}
            )
            second = await asyncio.to_thread(
                send_bot_command, server.socket_path, {"request_id": "stop-2", "action": "shutdown"}
            )
            assert status["result"]["marketId"] == "MKT"
            assert first["ok"] and second["ok"]
            assert len(shutdown_calls) == 1
        finally:
            await server.stop()
        assert not server.socket_path.exists()

    asyncio.run(scenario())
