#!/bin/bash

cd "$(dirname "$0")"

VENV_DIR=".venv"
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"

if [ ! -x "$PYTHON" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
fi

"$PIP" install -r requirements.txt

echo "Starting LIP Launcher UI on port 6969..."
"$PYTHON" -m streamlit run Main.py --server.port 6969 --server.address localhost
