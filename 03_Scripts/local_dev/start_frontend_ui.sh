#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=03_Scripts/local_dev/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

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
