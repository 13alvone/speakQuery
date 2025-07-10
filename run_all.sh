#!/usr/bin/env bash
set -euo pipefail

# run_all.sh - Launch all SpeakQuery services concurrently.
# Usage: ./run_all.sh
# Decrypts the repo-specific .env if present and starts
# the Flask app, query engine and scheduled input engine.
# All processes are stopped together when the script exits.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Attempt to decrypt repo-specific environment file
REPO_NAME="$(basename "$PROJECT_ROOT")"
ENV_ENC="$PROJECT_ROOT/input_repos/$REPO_NAME/.env.enc"
if [[ -f "$ENV_ENC" ]]; then
  echo "[i] Decrypting environment variables from $ENV_ENC"
  tmp_env="$(mktemp)"
  python utils/env_crypto.py decrypt "$ENV_ENC" > "$tmp_env"
  chmod 600 "$tmp_env"
  export ENV_PATH="$tmp_env"
  set -o allexport
  # shellcheck disable=SC1090
  source "$tmp_env"
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
  if [[ -n "${tmp_env:-}" && -f "$tmp_env" ]]; then
    rm -f "$tmp_env"
  fi
}

trap cleanup INT TERM EXIT

wait
