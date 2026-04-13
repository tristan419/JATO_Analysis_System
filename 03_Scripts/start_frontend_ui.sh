#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="$ROOT_DIR/06_AppPlatform/frontend"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

if [[ ! -d "$FRONTEND_DIR" ]]; then
  echo "[frontend] missing frontend directory: $FRONTEND_DIR"
  exit 1
fi

echo "[frontend] starting dev server"
echo "[frontend] url: http://$FRONTEND_HOST:$FRONTEND_PORT"
echo "[frontend] API base comes from the frontend .env file unless overridden"

cd "$FRONTEND_DIR"
exec npm run dev -- --host "$FRONTEND_HOST" --port "$FRONTEND_PORT"
