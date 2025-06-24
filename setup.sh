#!/usr/bin/env bash
set -euo pipefail

# setup.sh - Bootstrap environment and initialize the application
# Usage: ./setup.sh
# Creates a virtual environment, installs Python dependencies,
# builds custom components and initializes default databases.

usage() {
  echo "Usage: $0" >&2
  echo "  Creates ./env, installs requirements, builds components" >&2
  echo "  and initializes the application's databases." >&2
}

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_ROOT/env"
PYTHON="$VENV_DIR/bin/python"

echo "[i] Creating virtual environment at $VENV_DIR"
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

echo "[i] Installing Python packages"
"$PYTHON" -m pip install --upgrade pip
"$PYTHON" -m pip install -r "$PROJECT_ROOT/requirements.txt" -r "$PROJECT_ROOT/requirements-dev.txt"

echo "[i] Building custom C++ components"
"$PYTHON" "$PROJECT_ROOT/build_custom_components.py"

echo "[i] Initializing databases"
"$PYTHON" - <<'PY'
from app import initialize_database
initialize_database()
PY

echo "[i] Setup complete. Activate the environment with 'source env/bin/activate'"
