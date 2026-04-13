#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/06_AppPlatform/backend"
FRONTEND_DIR="$ROOT_DIR/06_AppPlatform/frontend"
RUNTIME_DIR="$ROOT_DIR/06_AppPlatform/.runtime"
LOG_DIR="$ROOT_DIR/06_AppPlatform/.runtime/logs"

BACKEND_PID_FILE="$RUNTIME_DIR/backend.pid"
FRONTEND_PID_FILE="$RUNTIME_DIR/frontend.pid"
PORTS_FILE="$RUNTIME_DIR/ports.env"

BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"

BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

AUTH_ENABLED="${APP_AUTH_ENABLED:-true}"
AUTH_TOKEN="${APP_AUTH_TOKEN:-change-me}"
USER_ROLE="${APP_USER_ROLE:-admin}"
USER_NAME="${APP_USER_NAME:-local-dev}"

PYTHON_BIN_DEFAULT="$ROOT_DIR/.venv/bin/python"
if [[ -x "$PYTHON_BIN_DEFAULT" ]]; then
  PYTHON_BIN="$PYTHON_BIN_DEFAULT"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

NPM_BIN="${NPM_BIN:-npm}"

mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

if [[ -f "$PORTS_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$PORTS_FILE"
fi

save_ports() {
  cat >"$PORTS_FILE" <<EOF
BACKEND_HOST=$BACKEND_HOST
BACKEND_PORT=$BACKEND_PORT
FRONTEND_HOST=$FRONTEND_HOST
FRONTEND_PORT=$FRONTEND_PORT
EOF
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
  echo "$candidate"
}

is_running() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

read_pid() {
  local file="$1"
  if [[ -f "$file" ]]; then
    cat "$file"
  fi
}

kill_by_port() {
  local port="$1"
  local pids
  pids="$(lsof -ti TCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$pids" ]]; then
    echo "[cleanup] killing processes on port $port: $pids"
    echo "$pids" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
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

start_backend() {
  local force_restart="${1:-false}"

  if [[ "$force_restart" == "true" ]]; then
    stop_service "backend" "$BACKEND_PID_FILE"
    kill_by_port "$BACKEND_PORT"
  fi

  if wait_for_http "http://$BACKEND_HOST:$BACKEND_PORT/healthz" 2; then
    echo "[backend] already reachable on $BACKEND_HOST:$BACKEND_PORT"
    save_ports
    return 0
  fi

  if port_in_use "$BACKEND_PORT"; then
    local old_port="$BACKEND_PORT"
    BACKEND_PORT="$(find_free_port "$BACKEND_PORT")"
    echo "[backend] port $old_port in use, switched to $BACKEND_PORT"
  fi

  local pid
  pid="$(read_pid "$BACKEND_PID_FILE")"
  if is_running "$pid"; then
    echo "[backend] already running (pid=$pid)"
    return 0
  fi

  echo "[backend] starting..."
  (
    cd "$BACKEND_DIR"
    export APP_AUTH_ENABLED="$AUTH_ENABLED"
    export APP_AUTH_TOKEN="$AUTH_TOKEN"
    export PYTHONPATH="$BACKEND_DIR"
    nohup "$PYTHON_BIN" -m uvicorn app.main:app \
      --host "$BACKEND_HOST" \
      --port "$BACKEND_PORT" \
      --reload \
      >>"$BACKEND_LOG" 2>&1 &
    echo $! >"$BACKEND_PID_FILE"
  )

  if wait_for_http "http://$BACKEND_HOST:$BACKEND_PORT/healthz" 30; then
    echo "[backend] ready: http://$BACKEND_HOST:$BACKEND_PORT/healthz"
    save_ports
  else
    echo "[backend] failed to become ready within timeout"
    return 1
  fi
}

start_frontend() {
  local force_restart="${1:-false}"

  if [[ "$force_restart" == "true" ]]; then
    stop_service "frontend" "$FRONTEND_PID_FILE"
    kill_by_port "$FRONTEND_PORT"
  fi

  if wait_for_http "http://$FRONTEND_HOST:$FRONTEND_PORT" 2; then
    echo "[frontend] already reachable on $FRONTEND_HOST:$FRONTEND_PORT"
    save_ports
    return 0
  fi

  if port_in_use "$FRONTEND_PORT"; then
    local old_port="$FRONTEND_PORT"
    FRONTEND_PORT="$(find_free_port "$FRONTEND_PORT")"
    echo "[frontend] port $old_port in use, switched to $FRONTEND_PORT"
  fi

  local pid
  pid="$(read_pid "$FRONTEND_PID_FILE")"
  if is_running "$pid"; then
    echo "[frontend] already running (pid=$pid)"
    return 0
  fi

  echo "[frontend] starting..."
  (
    cd "$FRONTEND_DIR"
    export VITE_API_BASE="http://$BACKEND_HOST:$BACKEND_PORT/v1"
    export VITE_AUTH_TOKEN="$AUTH_TOKEN"
    export VITE_USER_ROLE="$USER_ROLE"
    export VITE_USER_NAME="$USER_NAME"
    nohup "$NPM_BIN" run dev -- \
      --host "$FRONTEND_HOST" \
      --port "$FRONTEND_PORT" \
      >>"$FRONTEND_LOG" 2>&1 &
    echo $! >"$FRONTEND_PID_FILE"
  )

  if wait_for_http "http://$FRONTEND_HOST:$FRONTEND_PORT" 30; then
    echo "[frontend] ready: http://$FRONTEND_HOST:$FRONTEND_PORT"
    save_ports
  else
    echo "[frontend] failed to become ready within timeout"
    return 1
  fi
}

stop_service() {
  local name="$1"
  local file="$2"
  local pid
  pid="$(read_pid "$file")"
  if ! is_running "$pid"; then
    echo "[$name] not running"
    rm -f "$file"
    return 0
  fi

  echo "[$name] stopping pid=$pid"
  kill "$pid" >/dev/null 2>&1 || true
  sleep 1
  if is_running "$pid"; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$file"
}

status_service() {
  local name="$1"
  local file="$2"
  local pid
  pid="$(read_pid "$file")"
  if is_running "$pid"; then
    echo "[$name] running pid=$pid"
  else
    echo "[$name] stopped"
  fi
}

run_smoke_tests() {
  local read_headers=(
    -H "X-Auth-Token: $AUTH_TOKEN"
    -H "X-User-Name: $USER_NAME"
  )
  local json_headers=(
    -H "X-Auth-Token: $AUTH_TOKEN"
    -H "X-User-Name: $USER_NAME"
    -H "Content-Type: application/json"
  )

  echo "[test] health"
  curl -fsS "http://$BACKEND_HOST:$BACKEND_PORT/healthz" | head -c 200 | cat
  echo

  echo "[test] metadata columns"
  curl -fsS "http://$BACKEND_HOST:$BACKEND_PORT/v1/metadata/columns" \
    "${read_headers[@]}" | head -c 300 | cat
  echo

  echo "[test] overview"
  curl -fsS "http://$BACKEND_HOST:$BACKEND_PORT/v1/analysis/overview" \
    "${json_headers[@]}" \
    -d '{"filters":{},"prefer_precomputed":true,"top_n":12}' \
    | head -c 300 | cat
  echo

  echo "[test] crud list"
  curl -fsS "http://$BACKEND_HOST:$BACKEND_PORT/v1/crud/items?page=1&page_size=5" \
    "${read_headers[@]}" | head -c 300 | cat
  echo

  echo "[test] done"
}

usage() {
  cat <<'EOF'
Usage:
  bash 03_Scripts/fullstack_dev.sh start
  bash 03_Scripts/fullstack_dev.sh stop
  bash 03_Scripts/fullstack_dev.sh restart
  bash 03_Scripts/fullstack_dev.sh status
  bash 03_Scripts/fullstack_dev.sh test

Optional env overrides:
  BACKEND_HOST BACKEND_PORT FRONTEND_HOST FRONTEND_PORT
  APP_AUTH_ENABLED APP_AUTH_TOKEN APP_USER_ROLE APP_USER_NAME
EOF
}

cmd="${1:-start}"
case "$cmd" in
  start)
    start_backend true
    start_frontend true
    echo
    echo "Backend:  http://$BACKEND_HOST:$BACKEND_PORT"
    echo "Frontend: http://$FRONTEND_HOST:$FRONTEND_PORT"
    echo "Logs:"
    echo "  $BACKEND_LOG"
    echo "  $FRONTEND_LOG"
    ;;
  stop)
    stop_service "frontend" "$FRONTEND_PID_FILE"
    stop_service "backend" "$BACKEND_PID_FILE"
    rm -f "$PORTS_FILE"
    ;;
  restart)
    "$0" stop
    "$0" start
    ;;
  status)
    status_service "backend" "$BACKEND_PID_FILE"
    status_service "frontend" "$FRONTEND_PID_FILE"
    ;;
  test)
    run_smoke_tests
    ;;
  *)
    usage
    exit 1
    ;;
esac
