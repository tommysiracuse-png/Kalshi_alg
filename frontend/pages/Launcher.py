from logging import config
from pathlib import Path
from typing import List

import streamlit as st

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
    log_path: str

    def __init__(self, config: LauncherConfig, session: str):
        self.config = config
        self.command = self.build_launch_command()
        self.log_path = "launcher.log"

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

    # if session_paths is not None:
    #     if session_paths.get("logs_dir"):
    #         command.extend(["--logs-dir", session_paths["logs_dir"]])
    #     if session_paths.get("watchdog_state_dir"):
    #         command.extend(["--watchdog-state-dir", session_paths["watchdog_state_dir"]])
    #     if session_paths.get("telemetry_dir"):
    #         command.extend(["--telemetry-dir", session_paths["telemetry_dir"]])
    #     if session_paths.get("screener_output"):
    #         command.extend(["--screener-output", session_paths["screener_output"]])

        return command
    
    def launch(self):
        log_handle = self.log_path

def list_configs() -> List[str]:
    return [f.stem for f in CONFIG_DIR.glob("*.json")]

def load_launcher_settings(name: str):
    return LauncherConfig.loadLauncherConfig(name)

st.session_state.configs = list_configs()
configs = st.session_state.configs
print(f"Found configs: {configs}")
selected_config_name = st.selectbox("Load Existing Configuration", configs, index=0, help="Select configuration")
if selected_config_name is not None:
    # TODO - Load Selected Config
    config = load_launcher_settings(selected_config_name)
    st.info(f"Loaded Configuration: {selected_config_name}")
else:
    config = LauncherConfig.LauncherConfig()

launcher = LipLauncher(config=config, session="")

session = Sessions.Session("Test3")
