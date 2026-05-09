#!/bin/bash
# Start LIP Launcher UI on port 6969

cd "$(dirname "$0")"

VENV_DIR=".venv"
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"

if [ ! -x "$PYTHON" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
fi

if ! "$PYTHON" -c "import streamlit" 2>/dev/null; then
    echo "Installing streamlit into virtual environment..."
    "$PIP" install --upgrade pip
    "$PIP" install streamlit
fi

echo "Starting LIP Launcher UI on port 6969..."
"$PYTHON" -m streamlit run lip_launcher_ui.py --server.port 6969 --server.address localhost
