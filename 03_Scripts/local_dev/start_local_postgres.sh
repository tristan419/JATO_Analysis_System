#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=03_Scripts/local_dev/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-jato-postgres}"
POSTGRES_VOLUME_NAME="${POSTGRES_VOLUME_NAME:-jato-postgres-data}"
POSTGRES_DB="${POSTGRES_DB:-jato_app}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
PYTHON_BIN="${PYTHON_BIN:-$(prefer_python_bin)}"

wait_for_postgres() {
  local container_name="$1"
  local elapsed=0
  while [[ "$elapsed" -lt 60 ]]; do
    if docker exec "$container_name" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  return 1
}

postgres_container_exists() {
  local container_name="$1"
  docker ps -a --format '{{.Names}}' | grep -qx "$container_name"
}

postgres_container_running() {
  local container_name="$1"
  docker ps --format '{{.Names}}' | grep -qx "$container_name"
}

container_host_port() {
  local container_name="$1"
  docker inspect \
    --format '{{with index (index .HostConfig.PortBindings "5432/tcp") 0}}{{.HostPort}}{{end}}' \
    "$container_name" 2>/dev/null || true
}

runtime_postgres_state() {
  local had_app_database_url=0
  local had_runtime_container_name=0
  local had_runtime_managed=0
  local requested_port="$POSTGRES_PORT"
  local requested_db="$POSTGRES_DB"
  local requested_user="$POSTGRES_USER"
  local requested_password="$POSTGRES_PASSWORD"
  local requested_container="$CONTAINER_NAME"
  local requested_app_database_url="${APP_DATABASE_URL:-}"
  local requested_runtime_container_name="${POSTGRES_CONTAINER_NAME:-}"
  local requested_runtime_managed="${POSTGRES_MANAGED:-}"
  local runtime_port=""
  local runtime_url=""
  local runtime_container=""
  local runtime_managed="false"

  if [[ "${APP_DATABASE_URL+x}" == "x" ]]; then
    had_app_database_url=1
  fi
  if [[ "${POSTGRES_CONTAINER_NAME+x}" == "x" ]]; then
    had_runtime_container_name=1
  fi
  if [[ "${POSTGRES_MANAGED+x}" == "x" ]]; then
    had_runtime_managed=1
  fi

  if load_postgres_runtime; then
    runtime_port="${POSTGRES_PORT:-}"
    runtime_url="${APP_DATABASE_URL:-}"
    runtime_container="${POSTGRES_CONTAINER_NAME:-}"
    runtime_managed="${POSTGRES_MANAGED:-false}"
  fi

  POSTGRES_PORT="$requested_port"
  POSTGRES_DB="$requested_db"
  POSTGRES_USER="$requested_user"
  POSTGRES_PASSWORD="$requested_password"
  CONTAINER_NAME="$requested_container"
  if [[ "$had_app_database_url" -eq 1 ]]; then
    APP_DATABASE_URL="$requested_app_database_url"
  else
    unset APP_DATABASE_URL
  fi
  if [[ "$had_runtime_container_name" -eq 1 ]]; then
    POSTGRES_CONTAINER_NAME="$requested_runtime_container_name"
  else
    unset POSTGRES_CONTAINER_NAME
  fi
  if [[ "$had_runtime_managed" -eq 1 ]]; then
    POSTGRES_MANAGED="$requested_runtime_managed"
  else
    unset POSTGRES_MANAGED
  fi

  printf '%s|%s|%s|%s\n' \
    "$runtime_port" \
    "$runtime_url" \
    "$runtime_container" \
    "$runtime_managed"
}

managed_container_name_for_port() {
  local port="$1"
  if [[ -n "${POSTGRES_CONTAINER_NAME:-}" || "$port" == "$POSTGRES_PORT" ]]; then
    printf '%s\n' "$CONTAINER_NAME"
    return 0
  fi

  printf '%s-%s\n' "$CONTAINER_NAME" "$port"
}

use_existing_database_url() {
  local port="$1"
  local database_url="$2"
  echo "[postgres] compatible PostgreSQL detected on 127.0.0.1:$port"
  save_postgres_runtime \
    "$port" \
    "$database_url" \
    "" \
    "false" \
    "$POSTGRES_USER" \
    "$POSTGRES_DB"
}

ensure_docker_available() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "[postgres] docker is not installed; start PostgreSQL manually or export APP_DATABASE_URL to a compatible local instance"
    return 1
  fi

  if ! docker_daemon_available; then
    echo "[postgres] docker daemon is not available; start Docker or export APP_DATABASE_URL to a compatible local instance"
    return 1
  fi
}

start_postgres() {
  local runtime_state
  runtime_state="$(runtime_postgres_state)"
  local runtime_port="${runtime_state%%|*}"
  local remainder="${runtime_state#*|}"
  local runtime_url="${remainder%%|*}"
  remainder="${remainder#*|}"
  local runtime_container="${remainder%%|*}"
  local runtime_managed="${remainder##*|}"

  if [[ -n "$runtime_url" ]] && database_url_connects "$runtime_url" "$PYTHON_BIN"; then
    echo "[postgres] reusing runtime database on 127.0.0.1:${runtime_port:-$POSTGRES_PORT}"
    save_postgres_runtime \
      "${runtime_port:-$POSTGRES_PORT}" \
      "$runtime_url" \
      "$runtime_container" \
      "$runtime_managed" \
      "$POSTGRES_USER" \
      "$POSTGRES_DB"
    return 0
  fi

  if [[ "$runtime_managed" == "true" && -n "$runtime_container" ]]; then
    if ensure_docker_available && postgres_container_exists "$runtime_container"; then
      if postgres_container_running "$runtime_container"; then
        if wait_for_postgres "$runtime_container"; then
          local resumed_port="${runtime_port:-$(container_host_port "$runtime_container")}"
          local resumed_url
          resumed_url="$(build_local_database_url "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$resumed_port" "$POSTGRES_DB")"
          echo "[postgres] runtime container $runtime_container already running on $resumed_port"
          save_postgres_runtime \
            "$resumed_port" \
            "$resumed_url" \
            "$runtime_container" \
            "true" \
            "$POSTGRES_USER" \
            "$POSTGRES_DB"
          return 0
        fi
      else
        echo "[postgres] starting runtime container $runtime_container"
        docker start "$runtime_container" >/dev/null
        if wait_for_postgres "$runtime_container"; then
          local resumed_port="${runtime_port:-$(container_host_port "$runtime_container")}"
          local resumed_url
          resumed_url="$(build_local_database_url "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$resumed_port" "$POSTGRES_DB")"
          echo "[postgres] ready on 127.0.0.1:$resumed_port"
          save_postgres_runtime \
            "$resumed_port" \
            "$resumed_url" \
            "$runtime_container" \
            "true" \
            "$POSTGRES_USER" \
            "$POSTGRES_DB"
          return 0
        fi
      fi
    fi
  fi

  local target_port="$POSTGRES_PORT"
  local database_url
  database_url="$(build_local_database_url "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$target_port" "$POSTGRES_DB")"

  if port_in_use "$target_port"; then
    if database_url_connects "$database_url" "$PYTHON_BIN"; then
      use_existing_database_url "$target_port" "$database_url"
      return 0
    fi

    target_port="$(find_free_port "$((POSTGRES_PORT + 1))")"
    database_url="$(build_local_database_url "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$target_port" "$POSTGRES_DB")"
    echo "[postgres] port $POSTGRES_PORT is occupied by an incompatible service; starting managed PostgreSQL on $target_port"
  fi

  if ! ensure_docker_available; then
    return 1
  fi

  local target_container
  target_container="$(managed_container_name_for_port "$target_port")"

  if postgres_container_exists "$target_container"; then
    local mapped_port
    mapped_port="$(container_host_port "$target_container")"
    if [[ -n "$mapped_port" && "$mapped_port" != "$target_port" ]]; then
      target_port="$mapped_port"
      database_url="$(build_local_database_url "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$target_port" "$POSTGRES_DB")"
    fi

    if postgres_container_running "$target_container"; then
      echo "[postgres] container $target_container already running"
    else
      echo "[postgres] starting existing container $target_container"
      docker start "$target_container" >/dev/null
    fi
  else
    echo "[postgres] creating container $target_container"
    docker run \
      --name "$target_container" \
      -e POSTGRES_DB="$POSTGRES_DB" \
      -e POSTGRES_USER="$POSTGRES_USER" \
      -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
      -v "$POSTGRES_VOLUME_NAME:/var/lib/postgresql/data" \
      -p "$target_port":5432 \
      -d "$POSTGRES_IMAGE" >/dev/null
  fi

  if wait_for_postgres "$target_container"; then
    echo "[postgres] ready on 127.0.0.1:$target_port"
    save_postgres_runtime \
      "$target_port" \
      "$database_url" \
      "$target_container" \
      "true" \
      "$POSTGRES_USER" \
      "$POSTGRES_DB"
  else
    echo "[postgres] failed to become ready"
    return 1
  fi
}

stop_postgres() {
  local runtime_state
  runtime_state="$(runtime_postgres_state)"
  local runtime_port="${runtime_state%%|*}"
  local remainder="${runtime_state#*|}"
  local runtime_url="${remainder%%|*}"
  remainder="${remainder#*|}"
  local runtime_container="${remainder%%|*}"
  local runtime_managed="${remainder##*|}"

  clear_postgres_runtime

  if [[ "$runtime_managed" != "true" ]]; then
    if [[ -n "$runtime_url" ]]; then
      echo "[postgres] external database on ${runtime_port:-$POSTGRES_PORT} left running"
      return 0
    fi
  fi

  if ! command -v docker >/dev/null 2>&1 || ! docker_daemon_available; then
    echo "[postgres] docker unavailable; cannot stop managed container"
    return 0
  fi

  local target_container="${runtime_container:-$CONTAINER_NAME}"
  if postgres_container_running "$target_container"; then
    echo "[postgres] stopping container $target_container"
    docker stop "$target_container" >/dev/null
  else
    echo "[postgres] managed container not running"
  fi
}

status_postgres() {
  local runtime_state
  runtime_state="$(runtime_postgres_state)"
  local runtime_port="${runtime_state%%|*}"
  local remainder="${runtime_state#*|}"
  local runtime_url="${remainder%%|*}"
  remainder="${remainder#*|}"
  local runtime_container="${remainder%%|*}"
  local runtime_managed="${remainder##*|}"

  if [[ -n "$runtime_url" ]] && database_url_connects "$runtime_url" "$PYTHON_BIN"; then
    if [[ "$runtime_managed" == "true" ]]; then
      echo "[postgres] managed database ready on 127.0.0.1:${runtime_port:-$POSTGRES_PORT}"
    else
      echo "[postgres] external database available on 127.0.0.1:${runtime_port:-$POSTGRES_PORT}"
    fi
    return 0
  fi

  local default_url
  default_url="$(build_local_database_url "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$POSTGRES_PORT" "$POSTGRES_DB")"
  if port_in_use "$POSTGRES_PORT" && database_url_connects "$default_url" "$PYTHON_BIN"; then
    echo "[postgres] compatible database available on 127.0.0.1:$POSTGRES_PORT"
    return 0
  fi

  if port_in_use "$POSTGRES_PORT"; then
    echo "[postgres] port $POSTGRES_PORT is occupied by an incompatible service"
    return 0
  fi

  if command -v docker >/dev/null 2>&1 && docker_daemon_available && [[ -n "$runtime_container" ]] && postgres_container_exists "$runtime_container"; then
    if postgres_container_running "$runtime_container"; then
      echo "[postgres] container $runtime_container running but database probe failed"
    else
      echo "[postgres] container $runtime_container exists but is stopped"
    fi
    return 0
  fi

  echo "[postgres] stopped"
}

case "${1:-start}" in
  start)
    start_postgres
    ;;
  stop)
    stop_postgres
    ;;
  status)
    status_postgres
    ;;
  *)
    cat <<'EOF'
Usage:
  bash 03_Scripts/local_dev/start_local_postgres.sh start
  bash 03_Scripts/local_dev/start_local_postgres.sh stop
  bash 03_Scripts/local_dev/start_local_postgres.sh status
EOF
    exit 1
    ;;
esac
