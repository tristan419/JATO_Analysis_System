#!/usr/bin/env bash
set -euo pipefail

LOCAL_DEV_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_DEV_DIR="$(cd "$LOCAL_DEV_LIB_DIR/.." && pwd)"
REPO_DIR="$(cd "$LOCAL_DEV_DIR/../.." && pwd)"
BACKEND_DIR="$REPO_DIR/06_AppPlatform/backend"
FRONTEND_DIR="$REPO_DIR/06_AppPlatform/frontend"
RUNTIME_DIR="$REPO_DIR/06_AppPlatform/.runtime"
APP_RUNTIME_LOG_DIR="$RUNTIME_DIR/logs"
POSTGRES_RUNTIME_FILE="$RUNTIME_DIR/postgres.env"

prefer_python_bin() {
  if [[ -x "$REPO_DIR/.venv/bin/python" ]]; then
    printf '%s\n' "$REPO_DIR/.venv/bin/python"
    return 0
  fi

  if [[ -x "$REPO_DIR/venv/bin/python" ]]; then
    printf '%s\n' "$REPO_DIR/venv/bin/python"
    return 0
  fi

  printf '%s\n' "${PYTHON_BIN:-python3}"
}

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

port_in_use() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 1
}

find_free_port() {
  local start_port="$1"
  local candidate="$start_port"
  while port_in_use "$candidate"; do
    candidate=$((candidate + 1))
  done
  printf '%s\n' "$candidate"
}

wait_for_http() {
  local url="$1"
  local timeout_sec="$2"
  local elapsed=0
  while [[ "$elapsed" -lt "$timeout_sec" ]]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  return 1
}

docker_daemon_available() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

ensure_app_dirs() {
  if [[ ! -d "$BACKEND_DIR" || ! -d "$FRONTEND_DIR" ]]; then
    echo "[local-dev] expected backend and frontend directories under $REPO_DIR" >&2
    exit 1
  fi
}

database_url_connects() {
  local candidate_url="$1"
  local python_bin="${2:-$(prefer_python_bin)}"

  APP_DATABASE_URL="$candidate_url" "$python_bin" - <<'PY' >/dev/null 2>&1
from sqlalchemy import create_engine, text
import os
import sys

url = os.environ["APP_DATABASE_URL"]

try:
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
except Exception:
    sys.exit(1)
PY
}

build_local_database_url() {
  local user="$1"
  local password="$2"
  local port="$3"
  local database="$4"
  printf 'postgresql+psycopg://%s:%s@127.0.0.1:%s/%s\n' "$user" "$password" "$port" "$database"
}

write_shell_assignment() {
  local key="$1"
  local value="${2:-}"
  printf '%s=%q\n' "$key" "$value"
}

save_postgres_runtime() {
  local port="$1"
  local database_url="$2"
  local container_name="${3:-}"
  local managed="${4:-false}"
  local user="${5:-}"
  local database="${6:-}"

  mkdir -p "$RUNTIME_DIR"
  {
    write_shell_assignment POSTGRES_PORT "$port"
    write_shell_assignment APP_DATABASE_URL "$database_url"
    write_shell_assignment POSTGRES_CONTAINER_NAME "$container_name"
    write_shell_assignment POSTGRES_MANAGED "$managed"
    write_shell_assignment POSTGRES_USER "$user"
    write_shell_assignment POSTGRES_DB "$database"
  } >"$POSTGRES_RUNTIME_FILE"
}

clear_postgres_runtime() {
  rm -f "$POSTGRES_RUNTIME_FILE"
}

load_postgres_runtime() {
  if [[ -f "$POSTGRES_RUNTIME_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$POSTGRES_RUNTIME_FILE"
    return 0
  fi
  return 1
}
