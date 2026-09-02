"""Configuration for the standalone millisecond order-book recorder.

Everything is driven by environment variables (credentials are loaded from
``~/.config/kalshi-alg/bot.env`` when not already present, the same way
``history/backfill.py`` does it) so the process starts from
``start_recorder.ps1`` without arguments.  Relative paths resolve against the
repository root, not the current working directory.

Knobs (env var -> default):

    RECORD_DIR                          record_data
    RECORD_RUNTIME_DIR                  runtime
    RECORD_LAUNCHER_STATUS_PATH         runtime/launcher_status.json
    RECORD_MAX_MARKETS                  150   total markets recorded (fleet + screener)
    MARKETS_PER_CONNECTION              50    Kalshi caps markets per subscription
    RECORD_TOP_N                        100   most actively traded markets to add
    RECORD_REFRESH_SECONDS              900   market re-selection period
    RETENTION_DAYS                      30    delete day directories older than this
    RECORD_HEARTBEAT_SECONDS            10    runtime/recorder_status.json period
    RECORD_FLUSH_SECONDS                2     gzip sync-flush period (crash-safe window)
    RECORD_BACKOFF_MIN_SECONDS          1     reconnect backoff (exponential)
    RECORD_BACKOFF_MAX_SECONDS          30
    RECORD_FLEET_STATUS_MAX_AGE_SECONDS 1800  ignore launcher_status.json older than this
    RECORD_TRADE_SAMPLE_PAGES           12    public-trades pages sampled per selection
    RECORD_REST_PAUSE_SECONDS           0.2   pause between REST calls during selection
    RECORD_WEBSOCKET_URL                ""    override (default: adaptor's prod URL)
    RECORD_USE_DEMO                     0     use the demo environment
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_env_files(config_dir: Optional[Path] = None) -> None:
    """Load Kalshi credentials from ~/.config/kalshi-alg if not already in the env.

    Same semantics as ``history.backfill._load_env_files``: existing environment
    values win, ``bot.env`` is read before ``ui.env``.
    """
    if os.environ.get("KALSHI_API_KEY_ID"):
        return
    config_dir = config_dir or (Path.home() / ".config" / "kalshi-alg")
    for name in ("bot.env", "ui.env"):
        path = config_dir / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name, "")
    try:
        return int(raw) if raw.strip() else default
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, "")
    try:
        return float(raw) if raw.strip() else default
    except ValueError:
        raise ValueError(f"{name} must be a number, got {raw!r}") from None


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_path(env: Mapping[str, str], name: str, default: str, root: Path) -> Path:
    value = Path(env.get(name, "").strip() or default)
    return value if value.is_absolute() else (root / value)


@dataclass(frozen=True)
class RecorderConfig:
    record_dir: Path
    runtime_dir: Path
    launcher_status_path: Path
    max_markets: int = 150
    markets_per_connection: int = 50
    top_n: int = 100
    refresh_seconds: float = 900.0
    retention_days: int = 30
    heartbeat_seconds: float = 10.0
    flush_seconds: float = 2.0
    backoff_min_seconds: float = 1.0
    backoff_max_seconds: float = 30.0
    fleet_status_max_age_seconds: float = 1800.0
    trade_sample_pages: int = 12
    rest_pause_seconds: float = 0.2
    websocket_url: str = ""
    use_demo: bool = False
    # Operational timing (not usually tuned).
    select_retry_seconds: float = 30.0   # retry period while no market set exists yet
    reader_drain_seconds: float = 5.0    # wait for the reader task after closing a socket
    task_restart_seconds: float = 5.0    # delay before restarting a crashed loop task
    shutdown_seconds: float = 15.0       # grace period for loops to exit on stop

    def __post_init__(self) -> None:
        if self.max_markets < 1:
            raise ValueError("RECORD_MAX_MARKETS must be >= 1")
        if self.markets_per_connection < 1:
            raise ValueError("MARKETS_PER_CONNECTION must be >= 1")
        if self.top_n < 0:
            raise ValueError("RECORD_TOP_N must be >= 0")
        if self.refresh_seconds <= 0:
            raise ValueError("RECORD_REFRESH_SECONDS must be > 0")
        if self.heartbeat_seconds <= 0:
            raise ValueError("RECORD_HEARTBEAT_SECONDS must be > 0")
        if self.backoff_min_seconds <= 0 or self.backoff_max_seconds < self.backoff_min_seconds:
            raise ValueError("backoff seconds must satisfy 0 < min <= max")
        if self.retention_days < 0:
            raise ValueError("RETENTION_DAYS must be >= 0 (0 disables pruning)")

    @property
    def connection_count(self) -> int:
        return max(1, math.ceil(self.max_markets / self.markets_per_connection))

    @property
    def status_path(self) -> Path:
        return self.runtime_dir / "recorder_status.json"

    @property
    def log_path(self) -> Path:
        return self.runtime_dir / "recorder.log"

    @property
    def pid_path(self) -> Path:
        return self.runtime_dir / "recorder.pid"

    @property
    def stop_path(self) -> Path:
        """Creating this file asks a running recorder to shut down cleanly."""
        return self.runtime_dir / "recorder.stop"

    @classmethod
    def from_env(cls, root: Path = REPO_ROOT, env: Optional[Mapping[str, str]] = None) -> "RecorderConfig":
        env = os.environ if env is None else env
        runtime_dir = _env_path(env, "RECORD_RUNTIME_DIR", "runtime", root)
        return cls(
            record_dir=_env_path(env, "RECORD_DIR", "record_data", root),
            runtime_dir=runtime_dir,
            launcher_status_path=_env_path(
                env, "RECORD_LAUNCHER_STATUS_PATH", str(runtime_dir / "launcher_status.json"), root
            ),
            max_markets=_env_int(env, "RECORD_MAX_MARKETS", 150),
            markets_per_connection=_env_int(env, "MARKETS_PER_CONNECTION", 50),
            top_n=_env_int(env, "RECORD_TOP_N", 100),
            refresh_seconds=_env_float(env, "RECORD_REFRESH_SECONDS", 900.0),
            retention_days=_env_int(env, "RETENTION_DAYS", 30),
            heartbeat_seconds=_env_float(env, "RECORD_HEARTBEAT_SECONDS", 10.0),
            flush_seconds=_env_float(env, "RECORD_FLUSH_SECONDS", 2.0),
            backoff_min_seconds=_env_float(env, "RECORD_BACKOFF_MIN_SECONDS", 1.0),
            backoff_max_seconds=_env_float(env, "RECORD_BACKOFF_MAX_SECONDS", 30.0),
            fleet_status_max_age_seconds=_env_float(env, "RECORD_FLEET_STATUS_MAX_AGE_SECONDS", 1800.0),
            trade_sample_pages=_env_int(env, "RECORD_TRADE_SAMPLE_PAGES", 12),
            rest_pause_seconds=_env_float(env, "RECORD_REST_PAUSE_SECONDS", 0.2),
            websocket_url=env.get("RECORD_WEBSOCKET_URL", "").strip(),
            use_demo=_env_bool(env, "RECORD_USE_DEMO", False),
        )

    def public_summary(self) -> dict:
        return {
            "record_dir": str(self.record_dir),
            "max_markets": self.max_markets,
            "markets_per_connection": self.markets_per_connection,
            "connection_count": self.connection_count,
            "top_n": self.top_n,
            "refresh_seconds": self.refresh_seconds,
            "retention_days": self.retention_days,
            "websocket_url": self.websocket_url or ("demo" if self.use_demo else "prod-default"),
        }
