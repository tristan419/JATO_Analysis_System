#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/06_AppPlatform/backend"
FRONTEND_DIR="$ROOT_DIR/06_AppPlatform/frontend"
FULLSTACK_SCRIPT="$ROOT_DIR/03_Scripts/fullstack_dev.sh"
POSTGRES_SCRIPT="$ROOT_DIR/03_Scripts/start_local_postgres.sh"

APP_DATABASE_ENABLED="${APP_DATABASE_ENABLED:-true}"
APP_DATABASE_URL="${APP_DATABASE_URL:-postgresql+psycopg://postgres:postgres@127.0.0.1:5432/jato_app}"
APP_ENGINEERING_IMPORT_ROOT="${APP_ENGINEERING_IMPORT_ROOT:-$ROOT_DIR/01_RAW_DATA}"
APP_AUTH_ENABLED="${APP_AUTH_ENABLED:-true}"
APP_AUTH_TOKEN="${APP_AUTH_TOKEN:-change-me}"
APP_TOKEN_ROLE_MAP="${APP_TOKEN_ROLE_MAP:-$APP_AUTH_TOKEN:admin}"
APP_CORS_ORIGINS="${APP_CORS_ORIGINS:-http://127.0.0.1:5173,http://localhost:5173}"
PYTHON_BIN_DEFAULT="$ROOT_DIR/.venv/bin/python"
if [[ -x "$PYTHON_BIN_DEFAULT" ]]; then
  PYTHON_BIN="$PYTHON_BIN_DEFAULT"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

ensure_backend_dirs() {
  if [[ ! -d "$BACKEND_DIR" || ! -d "$FRONTEND_DIR" ]]; then
    echo "[fullstack] expected backend and frontend directories under $ROOT_DIR"
    exit 1
  fi
}

run_migrations() {
  echo "[fullstack] running alembic upgrade head"
  (
    cd "$BACKEND_DIR"
    export APP_DATABASE_ENABLED
    export APP_DATABASE_URL
    export APP_ENGINEERING_IMPORT_ROOT
    export APP_AUTH_ENABLED
    export APP_AUTH_TOKEN
    export APP_TOKEN_ROLE_MAP
    export APP_CORS_ORIGINS
    export PYTHONPATH="$BACKEND_DIR"
    "$PYTHON_BIN" -m alembic upgrade head
  )
}

start_services() {
  export APP_DATABASE_ENABLED
  export APP_DATABASE_URL
  export APP_ENGINEERING_IMPORT_ROOT
  export APP_AUTH_ENABLED
  export APP_AUTH_TOKEN
  export APP_TOKEN_ROLE_MAP
  export APP_CORS_ORIGINS

  bash "$FULLSTACK_SCRIPT" start
}

case "${1:-start}" in
  start)
    ensure_backend_dirs
    bash "$POSTGRES_SCRIPT"
    run_migrations
    start_services
    ;;
  test)
    ensure_backend_dirs
    bash "$POSTGRES_SCRIPT"
    run_migrations
    start_services
    bash "$FULLSTACK_SCRIPT" test
    ;;
  stop)
    bash "$FULLSTACK_SCRIPT" stop
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      docker stop "${POSTGRES_CONTAINER_NAME:-jato-postgres}" >/dev/null 2>&1 || true
    fi
    ;;
  status)
    bash "$FULLSTACK_SCRIPT" status
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      if docker ps --format '{{.Names}}' | grep -qx "${POSTGRES_CONTAINER_NAME:-jato-postgres}"; then
        echo "[postgres] running"
      else
        echo "[postgres] stopped"
      fi
    else
      echo "[postgres] unavailable"
    fi
    ;;
  *)
    cat <<'EOF'
Usage:
  bash 03_Scripts/start_fullstack_local.sh start
  bash 03_Scripts/start_fullstack_local.sh test
  bash 03_Scripts/start_fullstack_local.sh stop
  bash 03_Scripts/start_fullstack_local.sh status
EOF
    exit 1
    ;;
esac
