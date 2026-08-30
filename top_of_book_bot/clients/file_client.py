import json
import logging
import os
from time import time

LOGGER = logging.getLogger("kalshi_top_of_book_bot")

class _CrossProcessFileLock:
    def __init__(self, path: str) -> None:
        self.path = path
        self._file = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._file = open(self.path, "a+", encoding="utf-8")
        self._file.seek(0)
        if not self._file.read():
            self._file.seek(0)
            self._file.write("{}")
            self._file.flush()
            os.fsync(self._file.fileno())
        if os.name == "nt":
            import msvcrt

            self._file.seek(0)
            while True:
                try:
                    msvcrt.locking(self._file.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
        else:
            import fcntl

            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
        self._file.seek(0)
        return self._file

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is None:
            return
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
        except Exception:
            pass
        if os.name == "nt":
            import msvcrt

            self._file.seek(0)
            try:
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        self._file.close()
        self._file = None


class _SharedWriteRateLimiter:
    def __init__(self, *, path: str, writes_per_second: float, burst_capacity: int, enabled: bool) -> None:
        self.path = path
        self.writes_per_second = float(writes_per_second)
        self.burst_capacity = int(burst_capacity)
        self.enabled = bool(enabled)

    def _default(self) -> dict:
        return {
            "tokens_available": float(self.burst_capacity),
            "last_refill_epoch_seconds": time.time(),
            "blocked_until_epoch_seconds": 0.0,
        }

    def _load(self, file) -> dict:
        file.seek(0)
        try:
            state = json.loads(file.read().strip() or "{}")
        except json.JSONDecodeError:
            state = {}
        default = self._default()
        return {key: float(state.get(key, value)) for key, value in default.items()}

    @staticmethod
    def _save(file, state: dict) -> None:
        file.seek(0)
        file.truncate(0)
        file.write(json.dumps(state, separators=(",", ":")))
        file.flush()
        os.fsync(file.fileno())

    def acquire(self, action: str) -> None:
        if not self.enabled:
            return
        while True:
            sleep_seconds = 0.0
            with _CrossProcessFileLock(self.path) as file:
                now = time.time()
                state = self._load(file)
                if state["blocked_until_epoch_seconds"] > now:
                    sleep_seconds = max(0.05, state["blocked_until_epoch_seconds"] - now)
                else:
                    elapsed = max(0.0, now - state["last_refill_epoch_seconds"])
                    tokens = min(self.burst_capacity, state["tokens_available"] + elapsed * self.writes_per_second)
                    state["last_refill_epoch_seconds"] = now
                    if tokens >= 1:
                        state["tokens_available"] = tokens - 1
                        state["blocked_until_epoch_seconds"] = 0.0
                        self._save(file, state)
                        return
                    state["tokens_available"] = tokens
                    sleep_seconds = max(0.05, (1.0 - tokens) / self.writes_per_second)
                self._save(file, state)
            LOGGER.info("SHARED_WRITE_LIMIT_WAIT | action=%s seconds=%.3f", action, sleep_seconds)
            time.sleep(sleep_seconds)

    def cooldown(self, seconds: float) -> None:
        if not self.enabled or seconds <= 0:
            return
        with _CrossProcessFileLock(self.path) as file:
            now = time.time()
            state = self._load(file)
            state["blocked_until_epoch_seconds"] = max(state["blocked_until_epoch_seconds"], now + seconds)
            state["last_refill_epoch_seconds"] = now
            self._save(file, state)