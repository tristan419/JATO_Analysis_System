#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-jato-fullstack-backend@${BACKEND_PORT}}"
SERVER_NAME="${SERVER_NAME:-_}"
REPO_REMOTE_URL="${REPO_REMOTE_URL:-git@github.com:tristan419/JATO_Analysis_System.git}"
REPO_ARCHIVE_URL="${REPO_ARCHIVE_URL:-https://codeload.github.com/tristan419/JATO_Analysis_System/tar.gz/refs/heads/main}"
ENABLE_HTTPS="${ENABLE_HTTPS:-false}"
CERTBOT_EMAIL="${CERTBOT_EMAIL:-}"
APP_AUTH_ENABLED="${APP_AUTH_ENABLED:-false}"
APP_AUTH_TOKEN="${APP_AUTH_TOKEN:-}"
APP_BACKEND_WORKERS="${APP_BACKEND_WORKERS:-3}"
APP_ENGINEERING_IMPORT_ROOT="${APP_ENGINEERING_IMPORT_ROOT:-$REPO_DIR/01_RAW_DATA}"
APP_DATABASE_ENABLED="${APP_DATABASE_ENABLED:-false}"
APP_DATABASE_URL="${APP_DATABASE_URL:-}"
APP_DATABASE_ECHO="${APP_DATABASE_ECHO:-false}"
RUN_DATABASE_MIGRATIONS="${RUN_DATABASE_MIGRATIONS:-auto}"
EMBED_FRONTEND_TOKEN="${EMBED_FRONTEND_TOKEN:-false}"
VITE_API_BASE="${VITE_API_BASE:-/v1}"
VITE_USER_ROLE="${VITE_USER_ROLE:-viewer}"
VITE_USER_NAME="${VITE_USER_NAME:-anonymous}"

# ── China-friendly mirror defaults (override with env vars if needed) ──
NPM_REGISTRY="${NPM_REGISTRY:-https://mirrors.cloud.tencent.com/npm/}"
PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}"
PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}"
NODESOURCE_MIRROR="${NODESOURCE_MIRROR:-https://deb.nodesource.com/setup_20.x}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
DIAGNOSTIC_SCRIPT="$REPO_DIR/03_Scripts/ops/print_fullstack_server_diagnostics.sh"
SYSTEMD_TEMPLATE="$REPO_DIR/03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
SYSTEMD_TARGET="/etc/systemd/system/jato-fullstack-backend@.service"
NGINX_INSTALL_SCRIPT="$REPO_DIR/03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh"
HTTPS_INSTALL_SCRIPT="$REPO_DIR/03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh"
FRONTEND_ROOT="$REPO_DIR/06_AppPlatform/frontend/dist"
CURRENT_STEP="initialization"

log_section() {
  printf '\n[STEP] %s\n' "$1"
}

is_truthy() {
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

generate_token() {
  python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(24))
PY
}

run_diagnostics() {
  if [[ -x "$DIAGNOSTIC_SCRIPT" ]]; then
    REPO_DIR="$REPO_DIR" \
    BACKEND_SERVICE_NAME="$BACKEND_SERVICE_NAME" \
    BACKEND_PORT="$BACKEND_PORT" \
    BACKEND_ENV_FILE="$BACKEND_ENV_FILE" \
    bash "$DIAGNOSTIC_SCRIPT" || true
  fi
}

on_error() {
  local line_no="$1"
  local command="$2"
  echo
  echo "[ERROR] tencent_fullstack_bootstrap.sh failed"
  echo "[ERROR] step=$CURRENT_STEP"
  echo "[ERROR] line=$line_no"
  echo "[ERROR] command=$command"
  run_diagnostics
}

trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR

node_version_ok() {
  if ! command -v node >/dev/null 2>&1; then
    return 1
  fi
  node - <<'EOF' >/dev/null
const [major, minor, patch] = process.versions.node.split('.').map(Number);
const ok = (major === 20 && (minor > 19 || (minor === 19 && patch >= 0))) || (major === 22 && minor >= 12) || major > 22;
process.exit(ok ? 0 : 1);
EOF
}

CURRENT_STEP="Validate repository"
log_section "$CURRENT_STEP"
if [[ ! -d "$REPO_DIR" ]]; then
  echo "[ERROR] Repository directory not found at $REPO_DIR"
  echo "        Download the codeload archive first or clone the mirror into this path."
  exit 1
fi

if [[ -d "$REPO_DIR/.git" ]]; then
  echo "[INFO] Git repository metadata found"
else
  echo "[INFO] No .git metadata found; continuing with archive-based bootstrap"
fi

CURRENT_STEP="Validate sudo access"
log_section "$CURRENT_STEP"
if [[ "$(id -u)" -ne 0 ]]; then
  sudo -v
fi

CURRENT_STEP="Install base packages"
log_section "$CURRENT_STEP"
sudo apt-get update -y
sudo apt-get install -y git curl ca-certificates gnupg nginx python3 python3-venv python3-pip

CURRENT_STEP="Install or validate Node.js"
log_section "$CURRENT_STEP"
if ! node_version_ok; then
  echo "[INFO] Installing Node.js 20.x via NodeSource …"
  curl -# -L --connect-timeout 15 --max-time 120 \
    "$NODESOURCE_MIRROR" | sudo -E bash - || {
    echo "[WARN] NodeSource script timed out; trying snap fallback …"
    sudo snap install node --classic --channel=20 || true
  }
  sudo apt-get install -y nodejs || true
fi
node -v
npm -v

# ── Configure npm to use China mirror ──
echo "[INFO] npm registry → $NPM_REGISTRY"
npm config set registry "$NPM_REGISTRY"

CURRENT_STEP="Create Python virtualenv"
log_section "$CURRENT_STEP"
if [[ ! -x "$REPO_DIR/.venv/bin/python" ]]; then
  python3 -m venv "$REPO_DIR/.venv"
fi

CURRENT_STEP="Render backend environment file"
log_section "$CURRENT_STEP"
if [[ -z "$APP_AUTH_TOKEN" ]]; then
  APP_AUTH_TOKEN="$(generate_token)"
fi

TMP_ENV_FILE="$(mktemp)"
trap 'rm -f "$TMP_ENV_FILE"' EXIT
cat >"$TMP_ENV_FILE" <<EOF
APP_AUTH_ENABLED=$APP_AUTH_ENABLED
APP_AUTH_TOKEN=$APP_AUTH_TOKEN
APP_BACKEND_WORKERS=$APP_BACKEND_WORKERS
APP_PROJECT_ROOT=$REPO_DIR
JATO_PARQUET_PATH=${JATO_PARQUET_PATH:-$REPO_DIR/04_Processed_data/jato_full_archive.parquet}
JATO_PARTITIONED_PATH=${JATO_PARTITIONED_PATH:-$REPO_DIR/04_Processed_data/partitioned_dataset_v1}
APP_CRUD_DATA_PATH=${APP_CRUD_DATA_PATH:-$REPO_DIR/04_Processed_data/app_entities.json}
APP_ENGINEERING_IMPORT_ROOT=$APP_ENGINEERING_IMPORT_ROOT
APP_DATABASE_ENABLED=$APP_DATABASE_ENABLED
APP_DATABASE_URL=$APP_DATABASE_URL
APP_DATABASE_ECHO=$APP_DATABASE_ECHO
EOF

sudo install -d -m 755 /etc/jato-fullstack
sudo install -m 600 "$TMP_ENV_FILE" "$BACKEND_ENV_FILE"

CURRENT_STEP="Install systemd service template"
log_section "$CURRENT_STEP"
sed "s|/opt/JATO_Analysis_System-main|$REPO_DIR|g" "$SYSTEMD_TEMPLATE" | sudo tee "$SYSTEMD_TARGET" >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable "$BACKEND_SERVICE_NAME"

CURRENT_STEP="Deploy backend and frontend"
log_section "$CURRENT_STEP"
export REPO_DIR
export DEPLOY_BRANCH
export BACKEND_PORT
export BACKEND_SERVICE_NAME
export BACKEND_ENV_FILE
export REPO_REMOTE_URL
export RUN_DATABASE_MIGRATIONS
export SKIP_GIT_SYNC=true
export VITE_API_BASE
export VITE_USER_ROLE
export VITE_USER_NAME
export NPM_REGISTRY
export PIP_INDEX_URL
export PIP_TRUSTED_HOST
if [[ "$EMBED_FRONTEND_TOKEN" == "true" ]]; then
  export VITE_AUTH_TOKEN="$APP_AUTH_TOKEN"
else
  unset VITE_AUTH_TOKEN || true
fi

if [[ "$RUN_DATABASE_MIGRATIONS" == "auto" ]]; then
  if is_truthy "$APP_DATABASE_ENABLED" && [[ -n "$APP_DATABASE_URL" ]]; then
    export RUN_DATABASE_MIGRATIONS=true
  else
    export RUN_DATABASE_MIGRATIONS=false
  fi
fi

bash "$REPO_DIR/03_Scripts/ops/deploy_fullstack_server.sh"

CURRENT_STEP="Install nginx configuration"
log_section "$CURRENT_STEP"
sudo SERVER_NAME="$SERVER_NAME" BACKEND_PORT="$BACKEND_PORT" FRONTEND_ROOT="$FRONTEND_ROOT" \
  bash "$NGINX_INSTALL_SCRIPT"

CURRENT_STEP="Enable HTTPS"
log_section "$CURRENT_STEP"
if is_truthy "$ENABLE_HTTPS"; then
  if [[ "$SERVER_NAME" == "_" ]]; then
    echo "[ERROR] ENABLE_HTTPS=true requires SERVER_NAME to be set to a real domain"
    exit 1
  fi

  sudo SERVER_NAME="$SERVER_NAME" BACKEND_PORT="$BACKEND_PORT" FRONTEND_ROOT="$FRONTEND_ROOT" \
    CERTBOT_EMAIL="$CERTBOT_EMAIL" bash "$HTTPS_INSTALL_SCRIPT"
else
  echo "[INFO] HTTPS bootstrap skipped (ENABLE_HTTPS=$ENABLE_HTTPS)"
fi

CURRENT_STEP="Allow firewall rules if ufw is active"
log_section "$CURRENT_STEP"
if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | grep -qi 'Status: active'; then
  sudo ufw allow OpenSSH >/dev/null 2>&1 || true
  sudo ufw allow 'Nginx Full' >/dev/null 2>&1 || true
fi

CURRENT_STEP="Final verification"
log_section "$CURRENT_STEP"
curl -fsS "http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null
curl -fsS "http://127.0.0.1/healthz" >/dev/null

echo
echo "[INFO] Bootstrap finished successfully"
echo "[INFO] URL: http://$(hostname -I | awk '{print $1}')/"
echo "[INFO] Backend service: $BACKEND_SERVICE_NAME"
echo "[INFO] Backend token: $APP_AUTH_TOKEN"
echo "[INFO] If anything fails later, run: bash 03_Scripts/ops/print_fullstack_server_diagnostics.sh"
