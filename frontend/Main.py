import streamlit as st
from pathlib import Path

import LauncherConfig

# Get workspace root (parent of frontend directory)
WORKSPACE_ROOT = Path(__file__).parent.parent

# Title and description
st.title("🚀 Kalshi Bot Launcher")
st.markdown("Launch and manage Kalshi bots with ease")

dashboardPage = st.Page("pages/Dashboard.py", title="Dashboard")
launcherPage = st.Page("pages/Launcher.py", title="Launcher")
botsViewPage = st.Page("pages/Bots.py", title="View Bots")
launcherSettingsPage = st.Page("pages/LauncherSettings.py", title="Launcher Settings")
logsPage = st.Page("pages/Logs.py", title="Logs")

pg = st.navigation({
    "Home": [dashboardPage],
    "Bots": [launcherPage, botsViewPage],
    "Settings": [launcherSettingsPage],
    "Monitoring": [logsPage]
})

pg.run()
