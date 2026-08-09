from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    workspace: Path
    runtime_dir: Path
    logs_dir: Path
    watchdog_dir: Path
    service_name: str

    @classmethod
    def from_environment(cls) -> "Settings":
        workspace = Path(os.getenv("KALSHI_WORKSPACE", Path(__file__).resolve().parents[1])).resolve()
        return cls(
            workspace=workspace,
            runtime_dir=Path(os.getenv("KALSHI_RUNTIME_DIR", workspace / "runtime")).resolve(),
            logs_dir=Path(os.getenv("KALSHI_LOGS_DIR", workspace / "logs")).resolve(),
            watchdog_dir=Path(os.getenv("KALSHI_WATCHDOG_DIR", workspace / "watchdog_state")).resolve(),
            service_name=os.getenv("KALSHI_BOT_SERVICE", "kalshi-bot.service"),
        )
