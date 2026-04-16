#!/bin/bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER_SCRIPT="$REPO_DIR/03_Scripts/local_dev/start_fullstack_local.sh"
PORTS_FILE="$REPO_DIR/06_AppPlatform/.runtime/ports.env"
POSTGRES_FILE="$REPO_DIR/06_AppPlatform/.runtime/postgres.env"

cd "$REPO_DIR"
clear

printf '══════════════════════════════════════════════════\n'
printf '  JATO 本地前后端 + PostgreSQL 双击启动\n'
printf '══════════════════════════════════════════════════\n\n'

/bin/bash "$RUNNER_SCRIPT" start
status=$?

printf '\n'
if [[ $status -eq 0 ]]; then
  if [[ -f "$POSTGRES_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$POSTGRES_FILE"
    printf 'Postgres: 127.0.0.1:%s (%s)\n' "${POSTGRES_PORT:-5432}" "${POSTGRES_CONTAINER_NAME:-external}"
  fi
  if [[ -f "$PORTS_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$PORTS_FILE"
    printf 'Backend : http://%s:%s\n' "${BACKEND_HOST:-127.0.0.1}" "${BACKEND_PORT:-8000}"
    printf 'Frontend: http://%s:%s\n' "${FRONTEND_HOST:-127.0.0.1}" "${FRONTEND_PORT:-5173}"
  fi
  printf '启动完成。\n'
else
  printf '启动失败，退出码: %s\n' "$status"
fi

read -r -p '按回车关闭窗口...'
exit "$status"
