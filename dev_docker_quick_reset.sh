#!/usr/bin/env bash
# dev_docker_quick_reset.sh - Rebuilds and reruns dev container from scratch

set -euo pipefail
IFS=$'\n\t'

APP_NAME="speakquery"
PORT="5000"
ENV_FILE=".env"
ENV_EXAMPLE=".env.example"

log() {
  local level="$1"; shift
  echo "[$level] $*"
}

# ─────────────────────────────────────────────────────────────────────────────
# Pre-checks
# ─────────────────────────────────────────────────────────────────────────────

command -v docker >/dev/null 2>&1 || { log "x" "Docker is not installed. Aborting."; exit 1; }

log "i" "Pruning Docker system to reclaim space (including volumes)..."
sudo docker system prune -a --volumes -f

# ─────────────────────────────────────────────────────────────────────────────
# Remove old container/image if they exist
# ─────────────────────────────────────────────────────────────────────────────

if sudo docker ps -a --format '{{.Names}}' | grep -Eq "^${APP_NAME}$"; then
  log "i" "Stopping and removing existing container: $APP_NAME"
  sudo docker stop "$APP_NAME" || true
  sudo docker rm "$APP_NAME" || true
fi

if sudo docker images -q "$APP_NAME" >/dev/null 2>&1; then
  log "i" "Removing existing Docker image: $APP_NAME"
  sudo docker rmi "$APP_NAME" || true
fi

# ─────────────────────────────────────────────────────────────────────────────
# Prepare .env file
# ─────────────────────────────────────────────────────────────────────────────

if [[ ! -f "$ENV_FILE" ]]; then
  log "i" "Copying $ENV_EXAMPLE to $ENV_FILE"
  cp "$ENV_EXAMPLE" "$ENV_FILE"
else
  log "i" "$ENV_FILE already exists. Skipping copy."
fi

# ─────────────────────────────────────────────────────────────────────────────
# Build Docker image
# ─────────────────────────────────────────────────────────────────────────────

log "i" "Building Docker image from scratch..."
sudo docker build --no-cache -t "$APP_NAME" .

# ─────────────────────────────────────────────────────────────────────────────
# Run Docker container
# ─────────────────────────────────────────────────────────────────────────────

log "i" "Starting Docker container: $APP_NAME"
sudo docker run -d \
  --name "$APP_NAME" \
  --env-file "$ENV_FILE" \
  -p "$PORT:$PORT" \
  --restart unless-stopped \
  "$APP_NAME"

log "i" "Container '$APP_NAME' is now running on http://localhost:$PORT"

