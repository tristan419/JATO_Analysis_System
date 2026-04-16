#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
SSH_ALIAS="${SSH_ALIAS:-tencent-cloud}"
REMOTE_BACKEND_ENV_FILE="${REMOTE_BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
REMOTE_BACKEND_SERVICE="${REMOTE_BACKEND_SERVICE:-jato-fullstack-backend@8000}"
REMOTE_BACKEND_PORT="${REMOTE_BACKEND_PORT:-}"
LOCAL_DATABASE_URL="${LOCAL_DATABASE_URL:-${APP_DATABASE_URL:-postgresql+psycopg://postgres:postgres@127.0.0.1:5432/jato_app}}"
PG_DUMP_BIN="${PG_DUMP_BIN:-pg_dump}"
if [[ -z "${PSQL_BIN:-}" ]]; then
  if [[ "$PG_DUMP_BIN" == */pg_dump ]]; then
    PSQL_BIN="${PG_DUMP_BIN%/pg_dump}/psql"
  else
    PSQL_BIN="psql"
  fi
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

log_step() { printf "\n${CYAN}[STEP]${NC} %s\n" "$1"; }
log_ok() { printf "${GREEN}  ✅ %s${NC}\n" "$1"; }
log_fail() { printf "${RED}  ❌ %s${NC}\n" "$1"; }
log_info() { printf '  %s\n' "$1"; }

normalize_pgtool_url() {
  local value="$1"
  if [[ "$value" == postgresql+*://* ]]; then
    printf 'postgresql://%s\n' "${value#postgresql+*://}"
    return 0
  fi
  printf '%s\n' "$value"
}

mask_db_url_for_log() {
  local value="$1"
  if [[ "$value" =~ ^([^:]+://[^:/?#]+:)[^@]+(@.*)$ ]]; then
    printf '%s***%s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
    return 0
  fi
  printf '%s\n' "$value"
}

pg_bin_major() {
  local bin="$1"
  "$bin" --version 2>/dev/null | awk 'NR==1 {split($NF, parts, "."); print parts[1]}'
}

detect_source_db_major() {
  local server_version_num=""

  if ! command -v "$PSQL_BIN" >/dev/null 2>&1; then
    return 0
  fi

  server_version_num="$("$PSQL_BIN" --dbname="$LOCAL_PGTOOLS_URL" -Atqc 'SHOW server_version_num' 2>/dev/null || true)"
  if [[ "$server_version_num" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "$(( server_version_num / 10000 ))"
  fi
}

select_compatible_pg_dump_bin() {
  local source_major=""
  local dump_major=""
  local candidate=""

  source_major="$(detect_source_db_major)"
  dump_major="$(pg_bin_major "$PG_DUMP_BIN")"

  if [[ -z "$source_major" || -z "$dump_major" || "$dump_major" == "$source_major" ]]; then
    return 0
  fi

  candidate="/opt/homebrew/opt/postgresql@${source_major}/bin/pg_dump"
  if [[ -x "$candidate" ]]; then
    PG_DUMP_BIN="$candidate"
    log_info "检测到本地源库为 PostgreSQL ${source_major}，自动切换 pg_dump: ${PG_DUMP_BIN}"
    return 0
  fi

  if (( dump_major > source_major )); then
    log_fail "当前 pg_dump ${dump_major} 高于源库 PostgreSQL ${source_major}；请安装 postgresql@${source_major} 或设置兼容的 PG_DUMP_BIN。"
    exit 1
  fi
}

if [[ -z "$REMOTE_BACKEND_PORT" && "$REMOTE_BACKEND_SERVICE" =~ @([0-9]+)$ ]]; then
  REMOTE_BACKEND_PORT="${BASH_REMATCH[1]}"
fi
REMOTE_BACKEND_PORT="${REMOTE_BACKEND_PORT:-8000}"
LOCAL_PGTOOLS_URL="$(normalize_pgtool_url "$LOCAL_DATABASE_URL")"

if ! command -v "$PG_DUMP_BIN" >/dev/null 2>&1; then
  log_fail "未找到 pg_dump: $PG_DUMP_BIN"
  exit 1
fi

select_compatible_pg_dump_bin

TMP_DIR="$(mktemp -d)"
ARCHIVE_NAME="jato_msrp_$(date +%Y%m%d-%H%M%S).dump"
LOCAL_DUMP_PATH="$TMP_DIR/$ARCHIVE_NAME"

cleanup() {
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

printf "${CYAN}══════════════════════════════════════════════════${NC}\n"
printf "${CYAN}  MSRP PostgreSQL 一键同步到腾讯云${NC}\n"
printf "${CYAN}══════════════════════════════════════════════════${NC}\n"
log_info "本地数据库: $(mask_db_url_for_log "$LOCAL_DATABASE_URL")"
log_info "远端主机: $SSH_ALIAS"

log_step "1/4 导出本地 PostgreSQL 数据库..."
"$PG_DUMP_BIN" \
  --format=custom \
  --no-owner \
  --no-privileges \
  --dbname="$LOCAL_PGTOOLS_URL" \
  --file="$LOCAL_DUMP_PATH"

dump_size="$(du -h "$LOCAL_DUMP_PATH" | awk '{print $1}')"
log_ok "导出完成: $dump_size"

log_step "2/4 上传 dump 到腾讯云..."
scp -o ConnectTimeout=30 -o ServerAliveInterval=15 "$LOCAL_DUMP_PATH" "$SSH_ALIAS:/tmp/$ARCHIVE_NAME"
log_ok "上传完成"

log_step "3/4 在腾讯云恢复数据库并重启后端..."
ssh -o ConnectTimeout=30 "$SSH_ALIAS" bash -s <<REMOTE_SCRIPT
set -Eeuo pipefail

ARCHIVE_PATH="/tmp/$ARCHIVE_NAME"
BACKEND_ENV_FILE="$REMOTE_BACKEND_ENV_FILE"
BACKEND_SERVICE="$REMOTE_BACKEND_SERVICE"
BACKEND_PORT="$REMOTE_BACKEND_PORT"

normalize_pgtool_url() {
  local value="\$1"
  if [[ "\$value" == postgresql+*://* ]]; then
    printf 'postgresql://%s\n' "\${value#postgresql+*://}"
    return 0
  fi
  printf '%s\n' "\$value"
}

restart_needed=false
ensure_backend_started() {
  if [[ "\$restart_needed" == "true" ]]; then
    sudo systemctl start "\$BACKEND_SERVICE" >/dev/null 2>&1 || true
  fi
}

wait_for_http_ok() {
  local url="\$1"
  shift
  local attempt=0

  while true; do
    if curl -fsS --connect-timeout 5 --max-time 15 "\$@" "\$url" >/dev/null 2>&1; then
      return 0
    fi

    attempt=\$((attempt + 1))
    if (( attempt >= 30 )); then
      return 1
    fi
    sleep 1
  done
}

trap ensure_backend_started EXIT

if [[ ! -f "\$ARCHIVE_PATH" ]]; then
  echo "[ERROR] dump 文件不存在: \$ARCHIVE_PATH" >&2
  exit 1
fi

if ! command -v pg_restore >/dev/null 2>&1; then
  echo "[ERROR] 远端未安装 pg_restore" >&2
  exit 1
fi

REMOTE_DB_URL="\$(sudo awk -F= '/^APP_DATABASE_URL=/{sub(/^[^=]*=/, ""); print; exit}' "\$BACKEND_ENV_FILE")"
REMOTE_DB_ENABLED="\$(sudo awk -F= '/^APP_DATABASE_ENABLED=/{print \$2; exit}' "\$BACKEND_ENV_FILE")"
AUTH_TOKEN="\$(sudo awk -F= '/^APP_AUTH_TOKEN=/{sub(/^[^=]*=/, ""); print; exit}' "\$BACKEND_ENV_FILE")"
REMOTE_PGTOOLS_URL="\$(normalize_pgtool_url "\$REMOTE_DB_URL")"

if [[ -z "\$REMOTE_DB_URL" ]]; then
  echo "[ERROR] 远端 backend.env 未配置 APP_DATABASE_URL" >&2
  exit 1
fi

case "\${REMOTE_DB_ENABLED,,}" in
  1|true|yes|on) ;;
  *)
    echo "[ERROR] 远端 backend.env 未启用 APP_DATABASE_ENABLED=true" >&2
    exit 1
    ;;
esac

sudo systemctl stop "\$BACKEND_SERVICE"
restart_needed=true
pg_restore \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  --dbname="\$REMOTE_PGTOOLS_URL" \
  "\$ARCHIVE_PATH"
rm -f "\$ARCHIVE_PATH"
sudo systemctl start "\$BACKEND_SERVICE"
restart_needed=false
trap - EXIT

if ! wait_for_http_ok "http://127.0.0.1:\$BACKEND_PORT/healthz"; then
  sudo systemctl status --no-pager --lines=40 "\$BACKEND_SERVICE" | cat >&2 || true
  exit 1
fi

if ! wait_for_http_ok "http://127.0.0.1:\$BACKEND_PORT/v1/platform/db/health" -H "X-Auth-Token: \$AUTH_TOKEN"; then
  sudo systemctl status --no-pager --lines=40 "\$BACKEND_SERVICE" | cat >&2 || true
  exit 1
fi

curl -fsS --connect-timeout 5 --max-time 15 \
  -H "X-Auth-Token: \$AUTH_TOKEN" \
  "http://127.0.0.1:\$BACKEND_PORT/v1/platform/db/health"
REMOTE_SCRIPT

log_ok "远端恢复完成"

log_step "4/4 完成验证..."
ssh "$SSH_ALIAS" "curl -fsS --connect-timeout 5 --max-time 15 http://127.0.0.1:$REMOTE_BACKEND_PORT/healthz"
log_ok "MSRP 数据库同步完成"
