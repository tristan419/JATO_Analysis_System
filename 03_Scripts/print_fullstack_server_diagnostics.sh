#!/usr/bin/env bash
set -u

REPO_DIR="${REPO_DIR:-/opt/JATO_Analysis_System}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-jato-fullstack-backend@8000}"
BACKEND_PORT="${BACKEND_PORT:-}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
NGINX_ERROR_LOG="${NGINX_ERROR_LOG:-/var/log/nginx/error.log}"
FRONTEND_DIST_DIR="${FRONTEND_DIST_DIR:-$REPO_DIR/06_AppPlatform/frontend/dist}"

if [[ -z "$BACKEND_PORT" ]]; then
  if [[ "$BACKEND_SERVICE_NAME" =~ @([0-9]+)$ ]]; then
    BACKEND_PORT="${BASH_REMATCH[1]}"
  else
    BACKEND_PORT="8000"
  fi
fi

SUDO=""
if [[ "$(id -u)" -ne 0 ]] && sudo -n true >/dev/null 2>&1; then
  SUDO="sudo -n"
fi

section() {
  printf '\n===== %s =====\n' "$1"
}

run_shell() {
  local command="$1"
  if ! bash -lc "$command" 2>&1; then
    echo "[WARN] command failed: $command"
  fi
}

echo "===== BEGIN JATO FULLSTACK DIAGNOSTICS ====="
section "timestamp"
date '+%Y-%m-%d %H:%M:%S %Z'

section "host"
run_shell "hostname && whoami && pwd"

section "versions"
run_shell "bash --version | head -n 1"
run_shell "git --version"
run_shell "python3 --version"
run_shell "node -v"
run_shell "npm -v"

section "repo"
if [[ -d "$REPO_DIR/.git" ]]; then
  run_shell "git -C '$REPO_DIR' rev-parse --short HEAD"
  run_shell "git -C '$REPO_DIR' status --short"
else
  echo "[WARN] repo not found: $REPO_DIR"
fi

section "backend env file"
if [[ -f "$BACKEND_ENV_FILE" ]]; then
  run_shell "$SUDO ls -l '$BACKEND_ENV_FILE'"
else
  echo "[WARN] backend env file missing: $BACKEND_ENV_FILE"
fi

section "frontend dist"
if [[ -d "$FRONTEND_DIST_DIR" ]]; then
  run_shell "ls -lah '$FRONTEND_DIST_DIR' | sed -n '1,20p'"
else
  echo "[WARN] frontend dist missing: $FRONTEND_DIST_DIR"
fi

section "backend systemd status"
run_shell "systemctl is-enabled '$BACKEND_SERVICE_NAME'"
run_shell "systemctl is-active '$BACKEND_SERVICE_NAME'"
run_shell "$SUDO systemctl --no-pager status '$BACKEND_SERVICE_NAME' | sed -n '1,80p'"

section "backend journal"
run_shell "$SUDO journalctl -u '$BACKEND_SERVICE_NAME' -n 120 --no-pager"

section "nginx status"
run_shell "systemctl is-active nginx"
run_shell "$SUDO systemctl --no-pager status nginx | sed -n '1,80p'"
run_shell "$SUDO nginx -t"

section "nginx error log"
if [[ -f "$NGINX_ERROR_LOG" ]]; then
  run_shell "$SUDO tail -n 80 '$NGINX_ERROR_LOG'"
else
  echo "[WARN] nginx error log missing: $NGINX_ERROR_LOG"
fi

section "health checks"
run_shell "curl -i --max-time 10 'http://127.0.0.1:${BACKEND_PORT}/healthz'"
run_shell "curl -i --max-time 10 'http://127.0.0.1/healthz'"

section "ports"
run_shell "ss -ltnp | grep -E '(:80|:${BACKEND_PORT})\\b' || true"

section "disk and memory"
run_shell "df -h '$REPO_DIR'"
run_shell "free -h"

echo
echo "===== END JATO FULLSTACK DIAGNOSTICS ====="