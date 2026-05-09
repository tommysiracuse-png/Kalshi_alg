#!/usr/bin/env python3
"""
Streamlit UI for LIP Launcher
Provides a nice interface for running lip_launcher and managing configurations.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

# Set page config
st.set_page_config(
    page_title="LIP Launcher UI",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("🚀 LIP Launcher UI")
st.markdown("Launch and manage Kalshi bots with ease")

# Get workspace root
WORKSPACE_ROOT = Path(__file__).parent
CONFIG_DIR = WORKSPACE_ROOT / "launcher_configs"
CONFIG_DIR.mkdir(exist_ok=True)

DEFAULT_CONFIG: Dict[str, Any] = {
    "screen_file": str(WORKSPACE_ROOT / "screener_export.csv"),
    "bot_script": str(WORKSPACE_ROOT / "V1.py"),
    "screener_script": str(WORKSPACE_ROOT / "kalshi_screener.py"),
    "screener_output": str(WORKSPACE_ROOT / "screener_export.csv"),
    "max_bots": 100,
    "yes_budget_cents": 500,
    "no_budget_cents": 900,
    "launch_delay_seconds": 0.5,
    "refresh_interval_seconds": 1200.0,
    "poll_seconds": 60.0,
    "minimum_carryover_value_cents": 20.0,
    "run_screener_on_start": True,
    "use_demo": False,
    "dry_run": False,
    "subaccount": "",
    "api_key_id": "",
    "private_key_path": str(WORKSPACE_ROOT / "privkey.txt"),
    "host": "https://api.elections.kalshi.com",
    "api_prefix": "/trade-api/v2",
    "status": "open",
    "mve_filter": "exclude",
    "max_markets_to_scan": 600000,
    "top_n": 200,
    "min_spread_cents": 4,
    "max_spread_cents": 35,
    "min_yes_bid_cents": 5,
    "min_no_bid_cents": 5,
    "min_vol24h": 500.0,
    "min_oi": 100.0,
    "min_time_to_close_hrs": 3.0,
    "max_time_to_close_hrs": 50.0,
    "default_tick_cents": 1,
    "quote_size": 50,
    "excluded_series": [],
    "minimum_expected_edge_cents_to_quote": 2.0,
    "default_toxicity_cents": 3.0,
    "maker_fee_factor": 0.25,
    "fair_value_mid_weight": 0.55,
    "fair_value_last_weight": 0.45,
    "fair_value_max_orderbook_imbalance_adjust_cents": 4.0,
    "passive_offset_ticks_when_not_improving": 2,
    "candidate_price_levels_to_scan": 6,
    "queue_penalty_cap_cents": 6.0,
    "queue_penalty_contracts_per_cent": 30.0,
    "imbalance_toxicity_extra_cents": 1.0,
    "net_position_contracts": 0.0,
    "inventory_skew_contracts_per_tick": 15.0,
    "maximum_inventory_skew_ticks": 5.0,
}


def list_configs() -> List[str]:
    return [f.stem for f in CONFIG_DIR.glob("*.json")]


def load_config(config_name: str) -> Dict[str, Any]:
    config_path = CONFIG_DIR / f"{config_name}.json"
    config = DEFAULT_CONFIG.copy()
    if config_path.exists():
        try:
            loaded = json.loads(config_path.read_text())
            if isinstance(loaded, dict):
                config.update(loaded)
        except Exception:
            st.warning(f"Could not read configuration '{config_name}'. Defaults are loaded.")
    return config


def save_config(config_name: str, config: Dict[str, Any]) -> None:
    if not config_name.strip():
        st.error("Configuration name cannot be empty.")
        return
    normalized = config.copy()
    excluded = normalized.get("excluded_series")
    if isinstance(excluded, str):
        normalized["excluded_series"] = [item.strip() for item in excluded.split(",") if item.strip()]
    elif excluded is None:
        normalized["excluded_series"] = []
    config_path = CONFIG_DIR / f"{config_name}.json"
    config_path.write_text(json.dumps(normalized, indent=2))
    st.success(f"Configuration '{config_name}' saved to `{config_path.name}`.")


def ensure_venv_dependencies() -> Optional[str]:
    """Ensure venv exists and has required dependencies. Returns venv python path or None if failed."""
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


def build_launch_command(config: Dict[str, Any], venv_python: str) -> List[str]:
    command = [venv_python, str(WORKSPACE_ROOT / "lip_launcher.py")]
    command.extend(["--screen-file", str(config["screen_file"])])
    command.extend(["--bot-script", str(config["bot_script"])])
    command.extend(["--max-bots", str(config["max_bots"])])
    command.extend(["--yes-budget-cents", str(config["yes_budget_cents"])])
    command.extend(["--no-budget-cents", str(config["no_budget_cents"])])
    command.extend(["--launch-delay-seconds", str(config["launch_delay_seconds"])])
    command.extend(["--refresh-interval-seconds", str(config["refresh_interval_seconds"])])
    command.extend(["--poll-seconds", str(config["poll_seconds"])])
    command.extend(["--minimum-carryover-value-cents", str(config["minimum_carryover_value_cents"])])
    command.extend(["--screener-script", str(config["screener_script"])])
    command.extend(["--screener-output", str(config["screener_output"])])
    if config.get("run_screener_on_start"):
        command.append("--run-screener-on-start")
    if config.get("use_demo"):
        command.append("--use-demo")
    if config.get("dry_run"):
        command.append("--dry-run")
    if config.get("subaccount") not in (None, ""):
        command.extend(["--subaccount", str(config["subaccount"])])
    if config.get("api_key_id"):
        command.extend(["--api-key-id", str(config["api_key_id"])])
    if config.get("private_key_path"):
        command.extend(["--private-key", str(config["private_key_path"])])
    return command


def run_launcher(config: Dict[str, Any]) -> tuple[bool, str, str]:
    venv_python = ensure_venv_dependencies()
    if not venv_python:
        return False, "", "Failed to set up virtual environment"
    
    try:
        command = build_launch_command(config, venv_python)
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300,
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Launcher timeout"
    except Exception as e:
        return False, "", str(e)


def get_log_files() -> List[Path]:
    """Get all log files sorted by modification time (newest first)."""
    logs_dir = WORKSPACE_ROOT / "logs"
    if not logs_dir.exists():
        return []
    log_files = list(logs_dir.glob("*.log"))
    return sorted(log_files, key=lambda x: x.stat().st_mtime, reverse=True)


def parse_log_line(line: str) -> Dict[str, Any]:
    """Parse a log line into timestamp, level, event name, and key-value pairs."""
    line = line.strip()
    if not line or line.startswith("===") or line.startswith("CMD:"):
        return {"raw": line, "is_header": True}
    
    parts = line.split(" | ", 1)
    if len(parts) < 2:
        return {"raw": line, "is_header": False}
    
    # Parse timestamp and level/event
    meta = parts[0].split()
    if len(meta) < 3:
        return {"raw": line, "is_header": False}
    
    timestamp = meta[0]
    level = meta[1]
    event_name = meta[2]
    
    # Parse key-value pairs
    kvp_string = parts[1]
    kvp = {}
    for pair in kvp_string.split():
        if "=" in pair:
            key, value = pair.split("=", 1)
            kvp[key] = value
    
    return {
        "timestamp": timestamp,
        "level": level,
        "event": event_name,
        "data": kvp,
        "is_header": False,
        "raw": line
    }


def format_log_for_table(entry: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Convert a parsed log entry to a table row. Returns None for header entries."""
    if entry["is_header"]:
        return None
    
    if "timestamp" in entry:
        level_emoji = {
            "INFO": "🟢",
            "WARNING": "🟡",
            "ERROR": "🔴",
            "DEBUG": "🔵"
        }.get(entry["level"], "⚪")
        
        # Build a compact data string from key-value pairs
        if entry["data"]:
            data_str = " | ".join([f"{k}={v}" for k, v in entry["data"].items()])
        else:
            data_str = ""
        
        return {
            "Time": entry["timestamp"],
            "Level": f"{level_emoji} {entry['level']}",
            "Event": entry["event"],
            "Data": data_str
        }
    return None


def display_log_entries_table(log_lines: List[str], search_term: str = "") -> None:
    """Display all log entries as a single dataframe table."""
    rows = []
    
    for line in log_lines:
        if search_term and search_term.lower() not in line.lower():
            continue
        
        entry = parse_log_line(line)
        row = format_log_for_table(entry)
        if row:
            rows.append(row)
    
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No matching log entries found.")


def render_screener_section(config: Dict[str, Any]) -> Dict[str, Any]:
    st.markdown("### Screener configuration")
    col1, col2 = st.columns(2)
    with col1:
        host = st.text_input("Kalshi host", value=config.get("host", ""))
        api_prefix = st.text_input("API prefix", value=config.get("api_prefix", ""))
        status = st.selectbox("Market status", ["open", "unopened", "paused", "closed", "settled"], index=["open", "unopened", "paused", "closed", "settled"].index(config.get("status", "open")))
        mve_filter = st.selectbox("MVE filter", ["exclude", "include", "none"], index=["exclude", "include", "none"].index(config.get("mve_filter", "exclude")))
        max_markets_to_scan = st.number_input("Max markets to scan", min_value=0, value=int(config.get("max_markets_to_scan", 600000)))
        top_n = st.number_input("Top N results", min_value=1, value=int(config.get("top_n", 200)))
    with col2:
        min_spread_cents = st.number_input("Min spread (cents)", min_value=0, value=int(config.get("min_spread_cents", 4)))
        max_spread_cents = st.number_input("Max spread (cents)", min_value=0, value=int(config.get("max_spread_cents", 35)))
        min_yes_bid_cents = st.number_input("Min YES bid (cents)", min_value=0, value=int(config.get("min_yes_bid_cents", 5)))
        min_no_bid_cents = st.number_input("Min NO bid (cents)", min_value=0, value=int(config.get("min_no_bid_cents", 5)))
        min_vol24h = st.number_input("Min 24h volume", min_value=0.0, value=float(config.get("min_vol24h", 500.0)))
        min_oi = st.number_input("Min open interest", min_value=0.0, value=float(config.get("min_oi", 100.0)))
    excluded_series = st.text_input(
        "Excluded series (comma-separated)",
        value=", ".join(config.get("excluded_series", [])),
        help="Market series prefixes to exclude from screening."
    )
    col3, col4 = st.columns(2)
    with col3:
        min_time_to_close_hrs = st.number_input("Min time to close (hrs)", min_value=0.0, value=float(config.get("min_time_to_close_hrs", 3.0)))
        max_time_to_close_hrs = st.number_input("Max time to close (hrs)", min_value=0.0, value=float(config.get("max_time_to_close_hrs", 50.0)))
        default_tick_cents = st.number_input("Default tick size (cents)", min_value=1, value=int(config.get("default_tick_cents", 1)))
        quote_size = st.number_input("Quote size", min_value=1, value=int(config.get("quote_size", 50)))
    with col4:
        minimum_expected_edge_cents_to_quote = st.number_input("Minimum edge to quote (cents)", min_value=0.0, value=float(config.get("minimum_expected_edge_cents_to_quote", 2.0)))
        default_toxicity_cents = st.number_input("Default toxicity (cents)", min_value=0.0, value=float(config.get("default_toxicity_cents", 3.0)))
        maker_fee_factor = st.number_input("Maker fee factor", min_value=0.0, value=float(config.get("maker_fee_factor", 0.25)))
        fair_value_mid_weight = st.number_input("Fair value mid weight", min_value=0.0, max_value=1.0, value=float(config.get("fair_value_mid_weight", 0.55)))
        fair_value_last_weight = st.number_input("Fair value last weight", min_value=0.0, max_value=1.0, value=float(config.get("fair_value_last_weight", 0.45)))
    return {
        "host": host,
        "api_prefix": api_prefix,
        "status": status,
        "mve_filter": mve_filter,
        "max_markets_to_scan": max_markets_to_scan,
        "top_n": top_n,
        "min_spread_cents": min_spread_cents,
        "max_spread_cents": max_spread_cents,
        "min_yes_bid_cents": min_yes_bid_cents,
        "min_no_bid_cents": min_no_bid_cents,
        "min_vol24h": min_vol24h,
        "min_oi": min_oi,
        "excluded_series": [item.strip() for item in excluded_series.split(",") if item.strip()],
        "min_time_to_close_hrs": min_time_to_close_hrs,
        "max_time_to_close_hrs": max_time_to_close_hrs,
        "default_tick_cents": default_tick_cents,
        "quote_size": quote_size,
        "minimum_expected_edge_cents_to_quote": minimum_expected_edge_cents_to_quote,
        "default_toxicity_cents": default_toxicity_cents,
        "maker_fee_factor": maker_fee_factor,
        "fair_value_mid_weight": fair_value_mid_weight,
        "fair_value_last_weight": fair_value_last_weight,
        "fair_value_max_orderbook_imbalance_adjust_cents": float(config.get("fair_value_max_orderbook_imbalance_adjust_cents", 4.0)),
        "passive_offset_ticks_when_not_improving": int(config.get("passive_offset_ticks_when_not_improving", 2)),
        "candidate_price_levels_to_scan": int(config.get("candidate_price_levels_to_scan", 6)),
        "queue_penalty_cap_cents": float(config.get("queue_penalty_cap_cents", 6.0)),
        "queue_penalty_contracts_per_cent": float(config.get("queue_penalty_contracts_per_cent", 30.0)),
        "imbalance_toxicity_extra_cents": float(config.get("imbalance_toxicity_extra_cents", 1.0)),
        "net_position_contracts": float(config.get("net_position_contracts", 0.0)),
        "inventory_skew_contracts_per_tick": float(config.get("inventory_skew_contracts_per_tick", 15.0)),
        "maximum_inventory_skew_ticks": float(config.get("maximum_inventory_skew_ticks", 5.0)),
    }


def render_launcher_section(config: Dict[str, Any]) -> Dict[str, Any]:
    st.markdown("### Launcher configuration")
    col1, col2 = st.columns(2)
    with col1:
        screen_file = st.text_input("Screener CSV file", value=config.get("screen_file", ""))
        bot_script = st.text_input("Bot script", value=config.get("bot_script", ""))
        screener_script = st.text_input("Screener script", value=config.get("screener_script", ""))
        screener_output = st.text_input("Screener output CSV", value=config.get("screener_output", ""))
        api_key_id = st.text_input("API Key ID", value=config.get("api_key_id", ""))
    with col2:
        private_key_path = st.text_input("Private key path", value=config.get("private_key_path", ""))
        subaccount = st.text_input("Subaccount", value=str(config.get("subaccount", "")))
        max_bots = st.number_input("Max bots", min_value=0, value=int(config.get("max_bots", 100)))
        yes_budget_cents = st.number_input("YES budget (cents)", min_value=0, value=int(config.get("yes_budget_cents", 500)))
        no_budget_cents = st.number_input("NO budget (cents)", min_value=0, value=int(config.get("no_budget_cents", 900)))
    col3, col4 = st.columns(2)
    with col3:
        launch_delay_seconds = st.number_input("Launch delay (seconds)", min_value=0.0, value=float(config.get("launch_delay_seconds", 0.5)))
        refresh_interval_seconds = st.number_input("Refresh interval (seconds)", min_value=0.0, value=float(config.get("refresh_interval_seconds", 1200.0)))
        poll_seconds = st.number_input("Poll interval (seconds)", min_value=0.0, value=float(config.get("poll_seconds", 60.0)))
        minimum_carryover_value_cents = st.number_input("Minimum carryover value (cents)", min_value=0.0, value=float(config.get("minimum_carryover_value_cents", 20.0)))
    with col4:
        run_screener_on_start = st.checkbox("Run screener on start", value=bool(config.get("run_screener_on_start", True)))
        use_demo = st.checkbox("Use demo mode", value=bool(config.get("use_demo", False)))
        dry_run = st.checkbox("Dry run", value=bool(config.get("dry_run", False)))

    return {
        "screen_file": screen_file,
        "bot_script": bot_script,
        "screener_script": screener_script,
        "screener_output": screener_output,
        "api_key_id": api_key_id,
        "private_key_path": private_key_path,
        "subaccount": subaccount,
        "max_bots": max_bots,
        "yes_budget_cents": yes_budget_cents,
        "no_budget_cents": no_budget_cents,
        "launch_delay_seconds": launch_delay_seconds,
        "refresh_interval_seconds": refresh_interval_seconds,
        "poll_seconds": poll_seconds,
        "minimum_carryover_value_cents": minimum_carryover_value_cents,
        "run_screener_on_start": run_screener_on_start,
        "use_demo": use_demo,
        "dry_run": dry_run,
    }


# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a page:",
    ["Dashboard", "Launch Bot", "Launcher Config", "Logs", "Status", "Settings"]
)

# Initialize session state for selected config
if "selected_config" not in st.session_state:
    configs = list_configs()
    st.session_state.selected_config = configs[0] if configs else "default"

if "console_output" not in st.session_state:
    st.session_state.console_output = ""

if "console_stderr" not in st.session_state:
    st.session_state.console_stderr = ""

if "launch_status" not in st.session_state:
    st.session_state.launch_status = None

config = load_config(st.session_state.selected_config)

if page == "Dashboard":
    st.header("Dashboard")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Status", "Ready", "+1")
    with col2:
        st.metric("Bots Running", "0", "0")
    with col3:
        st.metric("Last Refresh", "Never", "")
    
    st.markdown("---")
    st.subheader("Quick Actions")
    if st.button("🔄 Refresh Status", use_container_width=True):
        st.info("Status refreshed")

elif page == "Launch Bot":
    st.header("Launch Bots")
    st.markdown("Launch the Lip Launcher with the selected configuration.")
    
    configs = list_configs()
    if configs:
        selected_config = st.selectbox("Select configuration", configs, index=configs.index(st.session_state.selected_config) if st.session_state.selected_config in configs else 0)
        st.session_state.selected_config = selected_config
        config = load_config(selected_config)
        
        with st.expander(f"Saved configuration: {selected_config}"):
            st.json(config)
        
        if st.button("🚀 Launch with selected configuration", use_container_width=True, type="primary"):
            with st.spinner("Setting up virtual environment and installing dependencies..."):
                venv_python = ensure_venv_dependencies()
            
            if not venv_python:
                st.error("Failed to set up virtual environment. See messages above.")
                st.session_state.launch_status = "failed"
            else:
                st.info(f"🔄 Launching bots with configuration '{selected_config}'...")
                success, stdout, stderr = run_launcher(config)
                
                st.session_state.console_output = stdout
                st.session_state.console_stderr = stderr
                st.session_state.launch_status = "success" if success else "failed"
                
                if success:
                    st.success("✅ Bots launched successfully!")
                else:
                    st.error("❌ Failed to launch bots")
                    venv_python_path = WORKSPACE_ROOT / ".venv" / "bin" / "python"
                    cmd = build_launch_command(config, str(venv_python_path))
                    st.markdown(f"**Command attempted:** `{' '.join(cmd)}`")
        
        st.markdown("---")
        st.subheader("📋 Console Output")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Clear console", use_container_width=True):
                st.session_state.console_output = ""
                st.session_state.console_stderr = ""
                st.session_state.launch_status = None
                st.rerun()
        
        if st.session_state.console_output or st.session_state.console_stderr:
            if st.session_state.console_output:
                st.markdown("**STDOUT:**")
                st.code(st.session_state.console_output, language=None)
            
            if st.session_state.console_stderr:
                st.markdown("**STDERR:**")
                st.code(st.session_state.console_stderr, language=None)
        else:
            st.info("Console output will appear here after launching.")
    
    else:
        st.warning("No saved configurations found. Please create one on the Launcher Config page.")

elif page == "Launcher Config":
    st.header("Launcher Configuration")
    st.markdown("Set and save launcher, bot, and screener configuration values.")
    
    configs = list_configs()
    selected_config = st.selectbox("Load existing configuration (optional)", [""] + configs, index=0)
    if selected_config:
        config = load_config(selected_config)
        st.info(f"Loaded configuration '{selected_config}'.")
    
    config_name = st.text_input("Configuration name", value=selected_config if selected_config else "")
    
    with st.form("config_form"):
        launcher_updates = render_launcher_section(config)
        st.markdown("---")
        screener_updates = render_screener_section(config)
        
        submitted = st.form_submit_button("💾 Save configuration")
        if submitted:
            full_config = {**launcher_updates, **screener_updates}
            save_config(config_name, full_config)
            if config_name not in configs:
                st.session_state.selected_config = config_name
    
    if configs:
        with st.expander("Saved configurations"):
            for conf in configs:
                config_path = CONFIG_DIR / f"{conf}.json"
                if config_path.exists():
                    st.markdown(f"**{conf}**: {config_path.read_text()[:100]}...")

elif page == "Logs":
    st.header("📋 Log Viewer")
    st.markdown("View and analyze bot log files.")
    
    log_files = get_log_files()
    if not log_files:
        st.info("No log files found in the logs directory.")
    else:
        # Create tabs for each log file
        tabs = st.tabs([f.stem for f in log_files])
        
        for tab, log_file in zip(tabs, log_files):
            with tab:
                st.subheader(log_file.name)
                
                # File info
                file_size = log_file.stat().st_size / 1024  # KB
                st.caption(f"📁 {log_file} | 📦 {file_size:.1f} KB")
                
                # Add refresh and download buttons
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1:
                    if st.button(f"🔄 Refresh {log_file.stem}", key=f"refresh_{log_file.stem}"):
                        st.rerun()
                with col2:
                    log_content = log_file.read_text()
                    st.download_button(
                        label="⬇️ Download",
                        data=log_content,
                        file_name=log_file.name,
                        mime="text/plain",
                        key=f"download_{log_file.stem}"
                    )
                
                # Line count and search
                log_lines = log_file.read_text().split("\n")
                search_term = st.text_input(f"Search in {log_file.stem}", key=f"search_{log_file.stem}")
                
                # Parse and display log entries
                st.markdown("---")
                
                # Show stats
                col_a, col_b = st.columns(2)
                with col_a:
                    st.metric("Total lines", len(log_lines))
                with col_b:
                    if search_term:
                        matching_lines = [l for l in log_lines if search_term.lower() in l.lower()]
                        st.metric("Matching lines", len(matching_lines))
                
                st.markdown("---")
                
                # Display log entries with expander
                with st.expander("📖 View log entries", expanded=True):
                    display_log_entries_table(log_lines, search_term)

elif page == "Status":
    st.header("Status & Monitoring")
    with st.container():
        st.subheader("Running Bots")
        st.info("No bots currently running")
    with st.container():
        st.subheader("Recent Activity")
        st.markdown("*Activity log will appear here*")

elif page == "Settings":
    st.header("Settings & Configuration")
    st.markdown("### Application Settings")
    st.info("Configuration management comes soon")
    with st.expander("ℹ️ About"):
        st.markdown(
            """
            **LIP Launcher UI** provides a user-friendly interface for managing Kalshi bots.

            - **Port**: 6969
            - **Framework**: Streamlit
            - **Version**: 1.0
            """
        )

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #888; font-size: 12px;'>"
    "LIP Launcher UI • Running on port 6969"
    "</div>",
    unsafe_allow_html=True,
)
