#!/usr/bin/env bash
set -euo pipefail

# run_all.sh - Launch all SpeakQuery services concurrently.
# Usage: ./run_all.sh
# Loads variables from .env and starts the Flask app,
# query engine and scheduled input engine. All processes
# are stopped together when the script exits.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

if [[ -f .env ]]; then
  echo "[i] Loading environment variables from .env"
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

pids=()

echo "[i] Starting Flask app..."
python app.py &
pids+=("$!")

echo "[i] Starting query engine..."
python query_engine/QueryEngine.py &
pids+=("$!")

echo "[i] Starting scheduled input engine..."
python scheduled_input_engine/ScheduledInputEngine.py &
pids+=("$!")

cleanup() {
  echo "[i] Shutting down services..."
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
      wait "$pid" || true
    fi
  done
}

trap cleanup INT TERM EXIT

wait
