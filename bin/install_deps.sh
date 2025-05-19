#!/usr/bin/env bash

# 1) Only create if no .venv is not existing
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment…"
  python3 -m venv .venv
else
  echo "Virtual environment already exists, skipping creation."
fi

# 2) Actiave environment
echo "Activating virtual environment…"
source .venv/bin/activate

# 3) Install pip and dependencies
echo "Installing dependencies…"
pip install --upgrade pip
pip install -r requirements.txt