#!/usr/bin/env bash
# install_deps.sh  – create or activate venv and install Python dependencies

# 1) Detect Python interpreter
if command -v python3 >/dev/null 2>&1; then
    PYTHON=python3
elif command -v python >/dev/null 2>&1; then
    PYTHON=python
elif command -v py >/dev/null 2>&1; then
    PYTHON='py -3'
else
    echo "Error: No Python interpreter (python3, python, or py) found in PATH." >&2
    exit 1
fi

# 2) Create venv if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    $PYTHON -m venv .venv
else
    echo "Virtual environment already exists, skipping creation."
fi

# 3) Activate the venv
if [ -f ".venv/bin/activate" ]; then
    # Linux/macOS
    echo "Activating virtual environment (Unix)..."
    # shellcheck disable=SC1091
    source .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
    # Windows (Git Bash, WSL)
    echo "Activating virtual environment (Windows posix)..."
    # shellcheck disable=SC1091
    source .venv/Scripts/activate
else
    echo "Error: could not locate activate script in .venv" >&2
    exit 1
fi

# 4) Install/update dependencies
echo "Installing dependencies from requirements.txt..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Dependencies installed successfully."
