import datetime
from logging import config
import os
from pathlib import Path
import signal
import sys
import time
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
    process: Optional[subprocess.Popen]

    def __init__(self, config: LauncherConfig, session: Sessions.Session):
        self.config = config
        self.session = session
        self.log_path = "launcher.log"
        self.command = self.build_launch_command()
        self.process = None

    def build_launch_command(self):
        command = ["python", str(WORKSPACE_ROOT / "lip_launcher.py")]
        command.extend(["--screen-file", self.config.screen_file])
        command.extend(["--bot-script", self.config.bot_script])
        command.extend(["--max-bots", str(self.config.max_bots)])
        command.extend(["--yes-budget-cents", str(self.config.yes_budget_cents)])
        command.extend(["--no-budget-cents", str(self.config.no_budget_cents)])
        command.extend(["--launch-delay-seconds", str(self.config.launch_delay_seconds)])
        command.extend(["--refresh-interval-seconds", str(self.config.refresh_interval_seconds)])
        command.extend(["--poll-seconds", str(self.config.poll_seconds)])
        command.extend(["--minimum-carryover-value-cents", str(self.config.minimum_carryover_value_cents)])
        command.extend(["--screener-script", self.config.screener_script])
        command.extend(["--screener-output", self.config.screener_output])
        command.extend(["--api-key-id", self.config.api_key_id])
        command.extend(["--private-key", self.config.private_key_path])
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
            popen_kwargs = {
                "cwd": str(WORKSPACE_ROOT),
                "stdout": log_handle,
                "stderr": subprocess.STDOUT,
                "env": dict(os.environ, PYTHONUNBUFFERED="1"),
            }
                
            popen_kwargs["start_new_session"] = True

            process = subprocess.Popen(self.command, **popen_kwargs)
            self.process = process
            return process, None, None
        except Exception as exc:
            log_handle.close()
            return None, None, str(exc)

    def stop(self):
        if self.process is None:
            st.warning("No launcher process is currently running.")
            return None, None, "No process to stop"

        if self.process.poll() is not None:
            st.info("Launcher process has already exited.")
            return None, None, None

        try:
            self.process.send_signal(signal.SIGINT)
            st.info("Sent stop signal to launcher process.")
            return None, None, None
        except Exception as exc:
            return None, None, str(exc)

def list_configs() -> List[str]:
    return [f.stem for f in CONFIG_DIR.glob("*.json")]

def load_launcher_settings(name: str):
    return LauncherConfig.loadLauncherConfig(name)

def list_sessions() -> List[str]:
    return [d.name for d in Sessions.SESSIONS_DIR.iterdir() if d.is_dir()]

def update_session_selection():
    selected = st.session_state.get("selected_session_input")
    if not selected:
        print("No session selected")
        return

    st.session_state.selected_session_name = selected
    st.session_state.new_session_name = selected
    st.session_state.session = Sessions.Session(selected)

    print(f"Updated selected session to {selected}")

    if "launcher" in st.session_state:
        st.session_state.launcher.session = st.session_state.session
        try:
            st.session_state.launcher.command = st.session_state.launcher.build_launch_command()
        except Exception:
            pass

def create_session():
    if st.session_state.new_session_name in st.session_state.sessions:
        st.session_state.session_create_error = True
        return
    
    Sessions.Session(st.session_state.new_session_name)
    st.session_state.session_create_error = False
    print(f"Successfully created session {st.session_state.new_session_name}")

def display_launcher_log(session: Optional[Sessions.Session] = None, target=None):
    if session is None:
        selected = st.session_state.get("selected_session_name") or st.session_state.get("selected_session_input")
        if not selected:
            st.info("No session selected yet.")
            return
        session = Sessions.Session(selected)

    launcher_log_path = session.session_dir / "launcher.log"

    if "log_auto_refresh" not in st.session_state:
        st.session_state.log_auto_refresh = True
    expander = target.expander("Launcher Log", expanded=True) if target is not None else st.expander("Launcher Log", expanded=True)

    with expander:
        col1, col2 = st.columns([1, 4])

        with col1:
            auto_refresh = st.checkbox("Auto-refresh", value=st.session_state.log_auto_refresh, key="log_auto_refresh_checkbox")
            st.session_state.log_auto_refresh = auto_refresh

        with col2:
            if st.button("Refresh Log", key="refresh_log_button"):
                st.rerun()

        code_ph = st.empty()
        caption_ph = st.empty()

        if launcher_log_path.exists():
            log_content = launcher_log_path.read_text(encoding="utf-8")
            log_lines = log_content.split("\n")

            file_stat = launcher_log_path.stat()
            caption_ph.caption(f"Total lines: {len(log_lines)} | Last updated: {datetime.datetime.fromtimestamp(file_stat.st_mtime).isoformat()} | Size: {file_stat.st_size} bytes")

            line_height_px = 16
            max_lines = 50
            display_height = min(len(log_lines), max_lines) * line_height_px

            escaped = (log_content.replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;")
                        .replace('"', "&quot;")
                        .replace("'", "&#39;"))

            html = ("""
            <div id="log-container" style="height:%dpx; overflow:auto; background-color:#0e1117; color:#c9d1d9; padding:12px; border-radius:6px; font-family:monospace; font-size:13px; white-space:pre-wrap; line-height:1.4;">
              <pre style="margin:0">%s</pre>
            </div>
            <script>
              setTimeout(function(){ var el = document.getElementById('log-container'); if(el) { el.scrollTop = el.scrollHeight; } }, 50);
            </script>
            """) % (display_height, escaped)

            code_ph.markdown(html, unsafe_allow_html=True)
        else:
            code_ph.empty()
            caption_ph.empty()
            st.info("No launcher log found. Launch a bot to create one.")

    if st.session_state.log_auto_refresh:
        time.sleep(1)
        st.rerun()

if "session_create_error" not in st.session_state:
    st.session_state.session_create_error = False

st.session_state.sessions = list_sessions()
is_running = False
if "launcher" in st.session_state:
    proc = getattr(st.session_state.launcher, "process", None)
    if proc is not None and proc.poll() is None:
        is_running = True

with st.container(horizontal=True):
    st.session_state.selected_session_name = st.selectbox("Select Session", st.session_state.sessions, index=0, help="Select session", on_change=update_session_selection, key="selected_session_input", disabled=is_running)
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
st.session_state.selected_config_name = st.selectbox("Load Existing Configuration", st.session_state.configs, index=0, help="Select configuration", disabled=is_running)
if st.session_state.selected_config_name is not None:
    config = load_launcher_settings(st.session_state.selected_config_name)
else:
    config = LauncherConfig.LauncherConfig()

st.session_state.config = config

if "launcher" not in st.session_state:
    st.session_state.launcher = LipLauncher(config=st.session_state.config, session=st.session_state.session)
else:
    st.session_state.launcher.config = st.session_state.config
    st.session_state.launcher.session = st.session_state.session
    st.session_state.launcher.command = st.session_state.launcher.build_launch_command()

with st.container():
    col1, col2 = st.columns(2)
    with col1:
        launch = st.button("Launch!", key="launch", on_click=st.session_state.launcher.launch)
    with col2:
        stop = st.button("Stop", key="stop")
    if stop:
        st.session_state.launcher.stop()

if st.session_state.launcher.process is not None and st.session_state.launcher.process.poll() is None:
    stop = False
    st.info(f"Launcher is running with PID: {st.session_state.launcher.process.pid}")
else:    st.info("Launcher is not currently running.")

log_placeholder = st.empty()
display_launcher_log(st.session_state.session, target=log_placeholder)
