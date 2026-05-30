


from pathlib import Path
from typing import Dict

import LauncherConfig

WORKSPACE_ROOT = Path(__file__).parent.parent
CONFIG_DIR = WORKSPACE_ROOT / "frontend" / "launcher_configs"
CONFIG_DIR.mkdir(exist_ok=True)

SESSIONS_DIR = WORKSPACE_ROOT / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)
SESSION_META_FILENAME = "session.json"

def session_dir(session_name: str) -> Path:
    return SESSIONS_DIR / safe_session_name(session_name)

def safe_session_name(value: str) -> str:
    candidate = "".join(
        ch if ch.isalnum() or ch in "-_" else "_"
        for ch in str(value or "").strip()
    )
    return candidate.strip("_") or "session"

class Session:
    name: str
    session_dir: Path
    logs_dir: Path
    watchdog_dir: Path
    telemetry_dir: Path
    session_log_path: Path
    config: LauncherConfig

    def __init__(self, name: str):
        self.name = name
        self.ensure_session_dirs()
        self.create_session_dirs()


    def ensure_session_dirs(self):
        session_directory = session_dir(self.name)
        logs_directory = session_directory / "logs"
        watchdog_directory = session_directory / "watchdog_state"
        telemetry_directory = session_directory / "telemetry"

        self.session_dir = session_directory
        self.logs_dir = logs_directory
        self.watchdog_dir = watchdog_directory
        self.telemetry_dir = telemetry_directory

        print(self.session_dir, self.logs_dir, self.watchdog_dir, self.telemetry_dir)

    def create_session_dirs(self):
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.watchdog_dir.mkdir(parents=True, exist_ok=True)
        self.telemetry_dir.mkdir(parents=True, exist_ok=True)