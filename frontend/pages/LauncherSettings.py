from pathlib import Path
from typing import List

import streamlit as st
import LauncherConfig

WORKSPACE_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = WORKSPACE_ROOT / "frontend" / "launcher_configs"
CONFIG_DIR.mkdir(exist_ok=True)

st.title("Launcher Settings")
st.markdown("Configure your launcher settings here.")

def list_configs() -> List[str]:
    return [f.stem for f in CONFIG_DIR.glob("*.json")]

def render_launcher_settings(config: LauncherConfig) -> LauncherConfig:
    st.markdown("### Launcher configuration")
    col1, col2 = st.columns(2)
    with col1:
        screen_file = st.text_input("Screener CSV file", value=config.screen_file)
        bot_script = st.text_input("Bot script", value=config.bot_script)
        screener_script = st.text_input("Screener script", value=config.screener_script)
        screener_output = st.text_input("Screener output CSV", value=config.screener_output)
        api_key_id = st.text_input("API Key ID", value=config.api_key_id)
    with col2:
        private_key_path = st.text_input("Private key path", value=config.private_key_path)
        subaccount = st.text_input("Subaccount", value=str(config.subaccount))
        max_bots = st.number_input("Max bots", min_value=0, value=int(config.max_bots))
        yes_budget_cents = st.number_input("YES budget (cents)", min_value=0, value=int(config.yes_budget_cents))
        no_budget_cents = st.number_input("NO budget (cents)", min_value=0, value=int(config.no_budget_cents))
    col3, col4 = st.columns(2)
    with col3:
        launch_delay_seconds = st.number_input("Launch delay (seconds)", min_value=0.0, value=float(config.launch_delay_seconds))
        refresh_interval_seconds = st.number_input("Refresh interval (seconds)", min_value=0.0, value=float(config.refresh_interval_seconds))
        poll_seconds = st.number_input("Poll interval (seconds)", min_value=0.0, value=float(config.poll_seconds))
        minimum_carryover_value_cents = st.number_input("Minimum carryover value (cents)", min_value=0.0, value=float(config.minimum_carryover_value_cents))
    with col4:
        run_screener_on_start = st.checkbox("Run screener on start", value=bool(config.run_screener_on_start))
        use_demo = st.checkbox("Use demo mode", value=bool(config.use_demo))
        dry_run = st.checkbox("Dry run", value=bool(config.dry_run))

    return config

def load_launcher_settings(name: str):
    return LauncherConfig.loadLauncherConfig(name)

def validate_launcher_save_input() -> None:
    st.write(f"Got new config name {st.session_state.new_config_name}")

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

def render_launcher_save(configs, selected_config_name, launcherSettings):
    with st.container(horizontal=True):
        if selected_config_name is not None:
            st.session_state.new_config_name = st.text_input("Save to new configuration")
            print(f"Got new config name {st.session_state.new_config_name}")
            error = None
            success = None

            save_btn = st.form_submit_button("Save Settings")
            if save_btn:
                if st.session_state.new_config_name:
                    if st.session_state.new_config_name in configs:
                        error = f"Error: {st.session_state.new_config_name} already exists"
                    else:
                        success = LauncherConfig.saveLauncherConfig(launcherSettings, st.session_state.new_config_name)
                        selected_config_name = st.session_state.new_config_name
                else:
                    success = LauncherConfig.saveLauncherConfig(launcherSettings, selected_config_name)

    if error:
        st.error(error)
    elif success:
        st.success(f"Settings saved to {selected_config_name}")


with st.form("launcher_settings"):
    st.write("### Launcher Settings")
    launcherSettings = render_launcher_settings(config)
    render_launcher_save(configs, selected_config_name, launcherSettings)