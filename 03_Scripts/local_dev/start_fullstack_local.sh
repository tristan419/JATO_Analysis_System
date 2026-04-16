#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=03_Scripts/local_dev/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

FULLSTACK_SCRIPT="$LOCAL_DEV_DIR/fullstack_dev.sh"
POSTGRES_SCRIPT="$LOCAL_DEV_DIR/start_local_postgres.sh"

APP_DATABASE_ENABLED="${APP_DATABASE_ENABLED:-true}"
APP_DATABASE_URL="${APP_DATABASE_URL:-}"
APP_ENGINEERING_IMPORT_ROOT="${APP_ENGINEERING_IMPORT_ROOT:-$REPO_DIR/01_RAW_DATA}"
APP_AUTH_ENABLED="${APP_AUTH_ENABLED:-false}"
APP_AUTH_TOKEN="${APP_AUTH_TOKEN:-change-me}"
APP_TOKEN_ROLE_MAP="${APP_TOKEN_ROLE_MAP:-$APP_AUTH_TOKEN:admin}"
APP_CORS_ORIGINS="${APP_CORS_ORIGINS:-http://127.0.0.1:5173,http://localhost:5173}"
PYTHON_BIN="${PYTHON_BIN:-$(prefer_python_bin)}"

ensure_backend_dirs() {
  ensure_app_dirs
}

hydrate_database_url() {
  if [[ -n "$APP_DATABASE_URL" ]]; then
    return 0
  fi

  if load_postgres_runtime && [[ -n "${APP_DATABASE_URL:-}" ]]; then
    return 0
  fi

  echo "[fullstack] could not resolve APP_DATABASE_URL after PostgreSQL startup" >&2
  return 1
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
    bash "$POSTGRES_SCRIPT" start
    hydrate_database_url
    run_migrations
    start_services
    ;;
  test)
    ensure_backend_dirs
    bash "$POSTGRES_SCRIPT" start
    hydrate_database_url
    run_migrations
    start_services
    bash "$FULLSTACK_SCRIPT" test
    ;;
  stop)
    bash "$FULLSTACK_SCRIPT" stop
    bash "$POSTGRES_SCRIPT" stop
    ;;
  status)
    bash "$FULLSTACK_SCRIPT" status
    bash "$POSTGRES_SCRIPT" status
    ;;
  *)
    cat <<'EOF'
Usage:
  bash 03_Scripts/local_dev/start_fullstack_local.sh start
  bash 03_Scripts/local_dev/start_fullstack_local.sh test
  bash 03_Scripts/local_dev/start_fullstack_local.sh stop
  bash 03_Scripts/local_dev/start_fullstack_local.sh status
EOF
    exit 1
    ;;
esac
