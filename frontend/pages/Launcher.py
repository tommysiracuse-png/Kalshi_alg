import datetime
from logging import config
import os
from pathlib import Path
import sys
from typing import List, Optional

import streamlit as st
import subprocess

import LauncherConfig
import Sessions

st.title("Launcher")
st.markdown("Launch and manage your Kalshi bots here.")

WORKSPACE_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = WORKSPACE_ROOT / "frontend" / "launcher_configs"
CONFIG_DIR.mkdir(exist_ok=True)

class LipLauncher:
    command: list[str]
    config: LauncherConfig
    session: Sessions.Session
    log_path: str

    def __init__(self, config: LauncherConfig, session: Sessions.Session):
        self.config = config
        self.session = session
        self.log_path = "launcher.log"
        self.command = self.build_launch_command()

    def build_launch_command(self):
        command = ["python", str(WORKSPACE_ROOT / "lip_launcher.py")]
        command.extend(["--screen-file", self.config.screen_file])
        command.extend(["--bot-script", self.config.bot_script])
        command.extend(["--max-bots", self.config.max_bots])
        command.extend(["--yes-budget-cents", self.config.yes_budget_cents])
        command.extend(["--no-budget-cents", self.config.no_budget_cents])
        command.extend(["--launch-delay-seconds", self.config.launch_delay_seconds])
        command.extend(["--refresh-interval-seconds", self.config.refresh_interval_seconds])
        command.extend(["--poll-seconds", self.config.poll_seconds])
        command.extend(["--minimum-carryover-value-cents", self.config.minimum_carryover_value_cents])
        command.extend(["--screener-script", self.config.screener_script])
        command.extend(["--screener-output", self.config.screener_output])
        if self.config.run_screener_on_start:
            command.append("--run-screener-on-start")
        if self.config.use_demo:
            command.append("--use-demo")
        if self.config.dry_run:
            command.append("--dry-run")
        if self.config.subaccount not in (None, ""):
            command.extend(["--subaccount", self.config.subaccount])
        if self.config.api_key_id:
            command.extend(["--api-key-id", self.config.api_key_id])
        if self.config.private_key_path:
            command.extend(["--private-key", self.config.private_key_path])
        if self.session.logs_dir.name:
            command.extend(["--logs-dir", self.session.logs_dir.as_posix()])
        if self.session.watchdog_dir:
            command.extend(["--watchdog-state-dir", self.session.watchdog_dir.as_posix()])
        if self.session.telemetry_dir:
            command.extend(["--telemetry-dir", self.session.telemetry_dir.as_posix()])
        if self.session.screener_output:
            command.extend(["--screener-output", self.session.screener_output.as_posix()])

        return command
    
    def ensure_venv_dependencies(self) -> Optional[str]:

        venv_dir = WORKSPACE_ROOT / ".venv"
        venv_python = venv_dir / "bin" / "python"
        
        if not venv_python.exists():
            st.warning(f"Creating virtual environment in {venv_dir}...")
            try:
                subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                st.error(f"Failed to create venv: {e}")
                return None
        
        # Install/upgrade pip and dependencies
        try:
            subprocess.run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"], check=True, capture_output=True, timeout=60)
            requirements_file = WORKSPACE_ROOT / "requirements.txt"
            if requirements_file.exists():
                subprocess.run([str(venv_python), "-m", "pip", "install", "-r", str(requirements_file)], check=True, capture_output=True, timeout=120)
        except subprocess.CalledProcessError as e:
            st.error(f"Failed to install dependencies: {e}")
            return None
        except subprocess.TimeoutExpired:
            st.error("Dependency installation timed out")
            return None
        
        return str(venv_python)
    
    def launch(self):
        log_handle = self.log_path
        venv_python = self.ensure_venv_dependencies()
        if not venv_python:
            return None, None, "Failed to set up virtual environment"

        launcher_log_path = self.session.session_dir / "launcher.log"
        log_handle = launcher_log_path.open("a", encoding="utf-8", buffering=1)
        log_handle.write(f"\n=== START {datetime.datetime.utcnow().isoformat()} ===\n")
        log_handle.write("CMD: " + " ".join(self.command) + "\n")
        log_handle.flush()

        try:
            process = subprocess.Popen(
                self.command,
                cwd=str(WORKSPACE_ROOT),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=dict(os.environ, PYTHONUNBUFFERED="1"),
            )
            return process, None, None
        except Exception as exc:
            log_handle.close()
            return None, None, str(exc)

def list_configs() -> List[str]:
    return [f.stem for f in CONFIG_DIR.glob("*.json")]

def load_launcher_settings(name: str):
    return LauncherConfig.loadLauncherConfig(name)

def list_sessions() -> List[str]:
    return [d.name for d in Sessions.SESSIONS_DIR.iterdir() if d.is_dir()]

def update_session_selection():
    st.session_state.new_session_name = st.session_state.selected_session_input
    print(f"Updated selected session to {st.session_state.new_session_name}")

def create_session():
    if st.session_state.new_session_name in st.session_state.sessions:
        st.session_state.session_create_error = True
        return
    
    Sessions.Session(st.session_state.new_session_name)
    st.session_state.session_create_error = False
    print(f"Successfully created session {st.session_state.new_session_name}")

if "session_create_error" not in st.session_state:
    st.session_state.session_create_error = False

st.session_state.sessions = list_sessions()
# print(f"Found sessions: {st.session_state.sessions}")

with st.container(horizontal=True):
    st.session_state.selected_session_name = st.selectbox("Select Session", st.session_state.sessions, index=0, help="Select session", on_change=update_session_selection, key="selected_session_input")
    st.text_input("New Session", key="new_session_name")
    create_new_session = st.button("Create", on_click=create_session)

if create_new_session:
    if st.session_state.session_create_error:
        st.error(f"Error: Session {st.session_state.new_session_name} already exists")
    else:
        st.info(f"Created session {st.session_state.new_session_name}")

st.session_state.session = Sessions.Session(st.session_state.selected_session_name)


st.session_state.configs = list_configs()
configs = st.session_state.configs
# print(f"Found configs: {configs}")
st.session_state.selected_config_name = st.selectbox("Load Existing Configuration", st.session_state.configs, index=0, help="Select configuration")
if st.session_state.selected_config_name is not None:
    # TODO - Load Selected Config
    config = load_launcher_settings(st.session_state.selected_config_name)
    st.info(f"Loaded Configuration: {st.session_state.selected_config_name}")
else:
    config = LauncherConfig.LauncherConfig()

st.session_state.config = config

launcher = LipLauncher(config=st.session_state.config, session=st.session_state.session)
st.info(launcher.command)

with st.container():
    launch = st.button("Launch!", key="launch", on_click=launcher.launch)



# session = Sessions.Session("Test3")
