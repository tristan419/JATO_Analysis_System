#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-jato-postgres}"
POSTGRES_DB="${POSTGRES_DB:-jato_app}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"

port_in_use() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 1
}

wait_for_postgres() {
  local elapsed=0
  while [[ "$elapsed" -lt 60 ]]; do
    if docker exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  return 1
}

if port_in_use "$POSTGRES_PORT"; then
  echo "[postgres] port $POSTGRES_PORT is already in use; assuming PostgreSQL is available"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "[postgres] docker is not installed; start PostgreSQL manually or set POSTGRES_PORT to an active local instance"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[postgres] docker daemon is not available; start Docker or use an existing local PostgreSQL instance"
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "[postgres] container $CONTAINER_NAME already running"
  else
    echo "[postgres] starting existing container $CONTAINER_NAME"
    docker start "$CONTAINER_NAME" >/dev/null
  fi
else
  echo "[postgres] creating container $CONTAINER_NAME"
  docker run \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_DB="$POSTGRES_DB" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -p "$POSTGRES_PORT":5432 \
    -d "$POSTGRES_IMAGE" >/dev/null
fi

if wait_for_postgres; then
  echo "[postgres] ready on 127.0.0.1:$POSTGRES_PORT"
else
  echo "[postgres] failed to become ready"
  exit 1
fi
