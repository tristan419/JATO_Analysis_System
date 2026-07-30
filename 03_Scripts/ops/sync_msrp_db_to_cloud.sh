#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
SSH_ALIAS="${SSH_ALIAS:-tencent-cloud}"
REMOTE_BACKEND_ENV_FILE="${REMOTE_BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
REMOTE_BACKEND_SERVICE="${REMOTE_BACKEND_SERVICE:-}"
REMOTE_BACKEND_PORT="${REMOTE_BACKEND_PORT:-}"
REMOTE_BLUEGREEN_STATE_ROOT="${REMOTE_BLUEGREEN_STATE_ROOT:-/var/lib/jato-release}"
REMOTE_ACTIVE_SLOT_FILE="${REMOTE_ACTIVE_SLOT_FILE:-$REMOTE_BLUEGREEN_STATE_ROOT/active-slot}"
REMOTE_DEPLOYMENT_MARKER="${REMOTE_DEPLOYMENT_MARKER:-$REMOTE_BLUEGREEN_STATE_ROOT/deployment-maintenance}"
REMOTE_ACTIVE_RELEASE_LINK="${REMOTE_ACTIVE_RELEASE_LINK:-/opt/jato/active}"
REMOTE_SLOTS_ROOT="${REMOTE_SLOTS_ROOT:-/opt/jato/slots}"
REMOTE_RELEASES_ROOT="${REMOTE_RELEASES_ROOT:-/opt/jato/releases}"
REMOTE_DEPLOY_STATE_DIR="${REMOTE_DEPLOY_STATE_DIR:-}"
REMOTE_BLUEGREEN_SWITCH_UNIT="${REMOTE_BLUEGREEN_SWITCH_UNIT:-jato-bluegreen-production.service}"
REMOTE_DEPLOY_LOCK_WAIT="${REMOTE_DEPLOY_LOCK_WAIT:-300}"
REMOTE_DEPLOYMENT_MODE=""
LOCAL_POSTGRES_RUNTIME_FILE="${LOCAL_POSTGRES_RUNTIME_FILE:-$REPO_DIR/06_AppPlatform/.runtime/postgres.env}"
LOCAL_DATABASE_URL="${LOCAL_DATABASE_URL:-${APP_DATABASE_URL:-}}"
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

resolve_local_database_url() {
  if [[ -n "$LOCAL_DATABASE_URL" ]]; then
    log_info "本地数据库来源: 显式环境变量"
    return 0
  fi

  if [[ -f "$LOCAL_POSTGRES_RUNTIME_FILE" ]]; then
    local runtime_database_url=""
    runtime_database_url="$(
      (
        # shellcheck disable=SC1090
        source "$LOCAL_POSTGRES_RUNTIME_FILE"
        printf '%s' "${APP_DATABASE_URL:-}"
      )
    )"
    if [[ -n "$runtime_database_url" ]]; then
      LOCAL_DATABASE_URL="$runtime_database_url"
      log_info "本地数据库来源: $LOCAL_POSTGRES_RUNTIME_FILE"
      return 0
    fi
  fi

  LOCAL_DATABASE_URL="postgresql+psycopg://postgres:postgres@127.0.0.1:5432/jato_app"
  log_info "本地数据库来源: fallback 5432 default"
}

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

resolve_remote_backend_target() {
  local requested_service="$REMOTE_BACKEND_SERVICE"
  local requested_port="$REMOTE_BACKEND_PORT"
  local remote_target=""
  local remote_target_file=""
  local detected_mode=""
  local detected_slot=""

  remote_target_file="$(mktemp)"
  chmod 0600 "$remote_target_file"
  if ! ssh -o ConnectTimeout=30 "$SSH_ALIAS" bash -s -- \
    "$REMOTE_ACTIVE_SLOT_FILE" \
    "$REMOTE_DEPLOYMENT_MARKER" \
    "$REMOTE_ACTIVE_RELEASE_LINK" \
    "$REMOTE_SLOTS_ROOT" \
    "$REMOTE_RELEASES_ROOT" > "$remote_target_file" <<'REMOTE_SCRIPT'
set -Eeuo pipefail
active_slot_file="$1"
deployment_marker="$2"
active_release_link="$3"
slots_root="$4"
releases_root="$5"

if [[ ! -e "$active_slot_file" && ! -L "$active_slot_file" \
  && ! -e "$active_release_link" && ! -L "$active_release_link" ]]; then
  printf 'legacy\t-\n'
  exit 0
fi
if [[ -e "$deployment_marker" || -L "$deployment_marker" ]]; then
  echo "[ERROR] deployment maintenance fence is active; refusing MSRP DB sync" >&2
  exit 1
fi
if [[ ! -f "$active_slot_file" || -L "$active_slot_file" ]]; then
  echo "[ERROR] blue/green active-slot is missing or unsafe" >&2
  exit 1
fi
active_slot="$(cat "$active_slot_file")"
case "$active_slot" in
  8000) inactive_slot=8001 ;;
  8001) inactive_slot=8000 ;;
  *)
    echo "[ERROR] blue/green active-slot is malformed" >&2
    exit 1
    ;;
esac
slot_link="$slots_root/$active_slot/current"
if [[ ! -L "$active_release_link" || ! -L "$slot_link" ]]; then
  echo "[ERROR] blue/green active release links are missing" >&2
  exit 1
fi
active_root="$(realpath "$active_release_link")"
slot_root="$(realpath "$slot_link")"
releases_root="$(realpath "$releases_root")"
if [[ "$active_root" != "$slot_root" || "$active_root" != "$releases_root/"* ]]; then
  echo "[ERROR] blue/green active release does not match durable active-slot" >&2
  exit 1
fi
if systemctl is-active --quiet "jato-fullstack-backend@$inactive_slot"; then
  echo "[ERROR] inactive blue/green slot is running; a deployment may be in progress" >&2
  exit 1
fi
if ! systemctl is-active --quiet "jato-fullstack-backend@$active_slot" \
  || ! curl -fsS --connect-timeout 5 --max-time 15 \
    "http://127.0.0.1:$active_slot/healthz" >/dev/null \
  || ! curl -fsS --connect-timeout 5 --max-time 15 \
    "http://127.0.0.1:$active_slot/readyz" >/dev/null; then
  echo "[ERROR] durable active blue/green slot did not pass direct health probes" >&2
  exit 1
fi
printf 'bluegreen\t%s\n' "$active_slot"
REMOTE_SCRIPT
  then
    rm -f "$remote_target_file"
    log_fail "无法验证腾讯云当前部署槽，已在上传数据库 dump 前停止"
    return 1
  fi
  remote_target="$(cat "$remote_target_file")"
  rm -f "$remote_target_file"
  IFS=$'\t' read -r detected_mode detected_slot <<< "$remote_target"
  case "$detected_mode" in
    bluegreen)
      if [[ -n "$requested_service" \
        && "$requested_service" != "jato-fullstack-backend@$detected_slot" ]]; then
        log_fail "REMOTE_BACKEND_SERVICE 指向 inactive/错误槽: $requested_service"
        return 1
      fi
      if [[ -n "$requested_port" && "$requested_port" != "$detected_slot" ]]; then
        log_fail "REMOTE_BACKEND_PORT 指向 inactive/错误槽: $requested_port"
        return 1
      fi
      REMOTE_DEPLOYMENT_MODE=bluegreen
      REMOTE_BACKEND_SERVICE="jato-fullstack-backend@$detected_slot"
      REMOTE_BACKEND_PORT="$detected_slot"
      ;;
    legacy)
      REMOTE_DEPLOYMENT_MODE=legacy
      REMOTE_BACKEND_SERVICE="${requested_service:-jato-fullstack-backend@8000}"
      if [[ -n "$requested_port" ]]; then
        REMOTE_BACKEND_PORT="$requested_port"
      elif [[ "$REMOTE_BACKEND_SERVICE" =~ @([0-9]+)$ ]]; then
        REMOTE_BACKEND_PORT="${BASH_REMATCH[1]}"
      else
        log_fail "legacy REMOTE_BACKEND_SERVICE 无法推导端口，请显式设置 REMOTE_BACKEND_PORT"
        return 1
      fi
      ;;
    *)
      log_fail "远端部署模式返回异常: ${detected_mode:-empty}"
      return 1
      ;;
  esac
  if [[ ! "$REMOTE_BACKEND_PORT" =~ ^[0-9]+$ ]] \
    || (( REMOTE_BACKEND_PORT < 1 || REMOTE_BACKEND_PORT > 65535 )); then
    log_fail "远端后端端口无效: $REMOTE_BACKEND_PORT"
    return 1
  fi
  log_info "远端后端目标: mode=$REMOTE_DEPLOYMENT_MODE service=$REMOTE_BACKEND_SERVICE port=$REMOTE_BACKEND_PORT"
}

resolve_remote_backend_target
resolve_local_database_url
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
DEPLOYMENT_MODE="$REMOTE_DEPLOYMENT_MODE"
ACTIVE_SLOT_FILE="$REMOTE_ACTIVE_SLOT_FILE"
DEPLOYMENT_MARKER="$REMOTE_DEPLOYMENT_MARKER"
ACTIVE_RELEASE_LINK="$REMOTE_ACTIVE_RELEASE_LINK"
SLOTS_ROOT="$REMOTE_SLOTS_ROOT"
RELEASES_ROOT="$REMOTE_RELEASES_ROOT"
DEPLOY_STATE_DIR="$REMOTE_DEPLOY_STATE_DIR"
BLUEGREEN_SWITCH_UNIT="$REMOTE_BLUEGREEN_SWITCH_UNIT"
DEPLOY_LOCK_WAIT="$REMOTE_DEPLOY_LOCK_WAIT"
if [[ -z "\$DEPLOY_STATE_DIR" ]]; then
  DEPLOY_STATE_DIR="\$HOME/.local/state/jato-production-release"
fi
DEPLOY_LOCK_PATH="\${DEPLOY_STATE_DIR%/}/production-deploy.lock"

normalize_pgtool_url() {
  local value="\$1"
  if [[ "\$value" == postgresql+*://* ]]; then
    printf 'postgresql://%s\n' "\${value#postgresql+*://}"
    return 0
  fi
  printf '%s\n' "\$value"
}

assert_bluegreen_switch_quiescent() {
  local active_state=""
  local load_state=""
  local sub_state=""
  if ! load_state="\$(
    systemctl show "\$BLUEGREEN_SWITCH_UNIT" -p LoadState --value 2>/dev/null
  )" \
    || ! active_state="\$(
      systemctl show "\$BLUEGREEN_SWITCH_UNIT" -p ActiveState --value 2>/dev/null
    )" \
    || ! sub_state="\$(
      systemctl show "\$BLUEGREEN_SWITCH_UNIT" -p SubState --value 2>/dev/null
    )"; then
    echo "[ERROR] cannot inspect the blue/green production switch unit" >&2
    return 1
  fi
  if [[ "\$load_state" == "not-found" ]]; then
    if [[ -n "\$active_state" && "\$active_state" != "inactive" ]]; then
      echo "[ERROR] unloaded blue/green switch unit reported state=\$active_state" >&2
      return 1
    fi
    return 0
  fi
  if [[ "\$load_state" != "loaded" ]] \
    || [[ "\$active_state" != "inactive" && "\$active_state" != "failed" ]]; then
    echo "[ERROR] blue/green production switch is not quiescent: load=\$load_state active=\$active_state sub=\$sub_state" >&2
    return 1
  fi
}

acquire_production_mutation_lock() {
  local lock_parent=""
  if ! command -v flock >/dev/null 2>&1 \
    || ! command -v systemctl >/dev/null 2>&1 \
    || ! command -v python3 >/dev/null 2>&1; then
    echo "[ERROR] flock, systemctl, and python3 are required for MSRP DB restore" >&2
    return 1
  fi
  if [[ "\$DEPLOY_STATE_DIR" != /* ]] \
    || [[ ! "\$DEPLOY_LOCK_WAIT" =~ ^[0-9]+\$ ]]; then
    echo "[ERROR] remote production deploy lock configuration is invalid" >&2
    return 1
  fi
  lock_parent="\$(dirname "\$DEPLOY_LOCK_PATH")"
  python3 -B - "\$lock_parent" false <<'PY_LOCK_PATH'
import os
from pathlib import Path
import stat
import sys

path = Path(sys.argv[1])
require_all = sys.argv[2] == "true"
cursor = Path(path.anchor)
for part in path.parts[1:]:
    cursor /= part
    try:
        mode = os.lstat(cursor).st_mode
    except FileNotFoundError:
        if require_all:
            raise SystemExit(
                f"[ERROR] production deploy lock ancestor disappeared: {cursor}"
            )
        continue
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise SystemExit(
            f"[ERROR] production deploy lock ancestor is unsafe: {cursor}"
        )
PY_LOCK_PATH
  mkdir -p "\$lock_parent"
  python3 -B - "\$lock_parent" true <<'PY_LOCK_PATH'
import os
from pathlib import Path
import stat
import sys

path = Path(sys.argv[1])
cursor = Path(path.anchor)
for part in path.parts[1:]:
    cursor /= part
    mode = os.lstat(cursor).st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise SystemExit(
            f"[ERROR] production deploy lock ancestor became unsafe: {cursor}"
        )
PY_LOCK_PATH
  chmod 700 "\$lock_parent"
  if [[ -L "\$DEPLOY_LOCK_PATH" ]] \
    || [[ -e "\$DEPLOY_LOCK_PATH" && ! -f "\$DEPLOY_LOCK_PATH" ]]; then
    echo "[ERROR] production deploy lock is unsafe" >&2
    return 1
  fi
  exec 9>"\$DEPLOY_LOCK_PATH"
  if ! flock -w "\$DEPLOY_LOCK_WAIT" 9; then
    echo "[ERROR] another production mutation holds the server-wide deploy lock" >&2
    return 1
  fi
  assert_bluegreen_switch_quiescent
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

verify_backend_target() {
  local active_slot=""
  local inactive_slot=""
  local active_root=""
  local slot_root=""
  local releases_root=""
  if [[ "\$DEPLOYMENT_MODE" != "bluegreen" ]]; then
    return 0
  fi
  if [[ -e "\$DEPLOYMENT_MARKER" || -L "\$DEPLOYMENT_MARKER" ]] \
    || [[ ! -f "\$ACTIVE_SLOT_FILE" || -L "\$ACTIVE_SLOT_FILE" ]]; then
    echo "[ERROR] blue/green state changed or maintenance began before DB restore" >&2
    return 1
  fi
  active_slot="\$(cat "\$ACTIVE_SLOT_FILE")"
  if [[ "\$active_slot" != "\$BACKEND_PORT" ]] \
    || [[ "\$BACKEND_SERVICE" != "jato-fullstack-backend@\$active_slot" ]]; then
    echo "[ERROR] refusing to stop or restart an inactive blue/green slot" >&2
    return 1
  fi
  if [[ "\$active_slot" == "8000" ]]; then
    inactive_slot=8001
  elif [[ "\$active_slot" == "8001" ]]; then
    inactive_slot=8000
  else
    echo "[ERROR] blue/green active-slot became malformed" >&2
    return 1
  fi
  if [[ ! -L "\$ACTIVE_RELEASE_LINK" \
    || ! -L "\$SLOTS_ROOT/\$active_slot/current" ]]; then
    echo "[ERROR] blue/green active release links disappeared" >&2
    return 1
  fi
  active_root="\$(realpath "\$ACTIVE_RELEASE_LINK")"
  slot_root="\$(realpath "\$SLOTS_ROOT/\$active_slot/current")"
  releases_root="\$(realpath "\$RELEASES_ROOT")"
  if [[ "\$active_root" != "\$slot_root" \
    || "\$active_root" != "\$releases_root/"* ]]; then
    echo "[ERROR] blue/green active release identity changed before DB restore" >&2
    return 1
  fi
  if systemctl is-active --quiet "jato-fullstack-backend@\$inactive_slot"; then
    echo "[ERROR] inactive slot started; refusing DB restore during deployment" >&2
    return 1
  fi
  if ! systemctl is-active --quiet "\$BACKEND_SERVICE" \
    || ! curl -fsS --connect-timeout 5 --max-time 15 \
      "http://127.0.0.1:\$BACKEND_PORT/readyz" >/dev/null; then
    echo "[ERROR] selected active backend is not ready" >&2
    return 1
  fi
}

trap ensure_backend_started EXIT

acquire_production_mutation_lock
verify_backend_target

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
