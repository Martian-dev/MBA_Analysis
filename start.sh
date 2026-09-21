#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$project_dir"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    echo "$2" >&2
    exit 1
  fi
}

require_command docker "Install Docker Desktop or Docker Engine with the Compose plugin."
require_command uv "Install uv from https://docs.astral.sh/uv/getting-started/installation/"
require_command curl "Install curl so the launcher can verify the local viewer."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is installed but its daemon is not running. Start Docker and rerun ./start.sh." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "The Docker Compose plugin is unavailable. Install it and rerun ./start.sh." >&2
  exit 1
fi

echo "============================================================"
echo " Basket Signal — one-shot local environment"
echo "============================================================"
echo "[1/2] Installing/verifying Python dependencies..."
uv sync
echo "[2/2] Starting a fresh randomized streaming demonstration..."
echo

exec bash scripts/run_live.sh
