#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-jato-fullstack-backend@${BACKEND_PORT}}"
SERVER_NAME="${SERVER_NAME:-_}"
REPO_REMOTE_URL="${REPO_REMOTE_URL:-https://gitclone.com/github.com/tristan419/JATO_Analysis_System.git}"
APP_AUTH_ENABLED="${APP_AUTH_ENABLED:-true}"
APP_AUTH_TOKEN="${APP_AUTH_TOKEN:-}"
EMBED_FRONTEND_TOKEN="${EMBED_FRONTEND_TOKEN:-false}"
VITE_API_BASE="${VITE_API_BASE:-/v1}"
VITE_USER_ROLE="${VITE_USER_ROLE:-viewer}"
VITE_USER_NAME="${VITE_USER_NAME:-anonymous}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
DIAGNOSTIC_SCRIPT="$REPO_DIR/03_Scripts/print_fullstack_server_diagnostics.sh"
SYSTEMD_TEMPLATE="$REPO_DIR/03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
SYSTEMD_TARGET="/etc/systemd/system/jato-fullstack-backend@.service"
NGINX_INSTALL_SCRIPT="$REPO_DIR/03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh"
FRONTEND_ROOT="$REPO_DIR/06_AppPlatform/frontend/dist"
CURRENT_STEP="initialization"

log_section() {
  printf '\n[STEP] %s\n' "$1"
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
if [[ ! -d "$REPO_DIR/.git" ]]; then
  echo "[ERROR] Git repository not found at $REPO_DIR"
  echo "        Run this script from the checked-out repository root or set REPO_DIR."
  exit 1
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
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi
node -v
npm -v

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
APP_PROJECT_ROOT=$REPO_DIR
JATO_PARQUET_PATH=${JATO_PARQUET_PATH:-$REPO_DIR/04_Processed_data/jato_full_archive.parquet}
JATO_PARTITIONED_PATH=${JATO_PARTITIONED_PATH:-$REPO_DIR/04_Processed_data/partitioned_dataset_v1}
APP_CRUD_DATA_PATH=${APP_CRUD_DATA_PATH:-$REPO_DIR/04_Processed_data/app_entities.json}
EOF

sudo install -d -m 755 /etc/jato-fullstack
sudo install -m 600 "$TMP_ENV_FILE" "$BACKEND_ENV_FILE"

CURRENT_STEP="Install systemd service template"
log_section "$CURRENT_STEP"
sed "s|/opt/JATO_Analysis_System|$REPO_DIR|g" "$SYSTEMD_TEMPLATE" | sudo tee "$SYSTEMD_TARGET" >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable "$BACKEND_SERVICE_NAME"

CURRENT_STEP="Deploy backend and frontend"
log_section "$CURRENT_STEP"
export REPO_DIR
export DEPLOY_BRANCH
export BACKEND_PORT
export BACKEND_SERVICE_NAME
export REPO_REMOTE_URL
export VITE_API_BASE
export VITE_USER_ROLE
export VITE_USER_NAME
if [[ "$EMBED_FRONTEND_TOKEN" == "true" ]]; then
  export VITE_AUTH_TOKEN="$APP_AUTH_TOKEN"
else
  unset VITE_AUTH_TOKEN || true
fi
bash "$REPO_DIR/03_Scripts/deploy_fullstack_server.sh"

CURRENT_STEP="Install nginx configuration"
log_section "$CURRENT_STEP"
sudo SERVER_NAME="$SERVER_NAME" BACKEND_PORT="$BACKEND_PORT" FRONTEND_ROOT="$FRONTEND_ROOT" \
  bash "$NGINX_INSTALL_SCRIPT"

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
echo "[INFO] If anything fails later, run: bash 03_Scripts/print_fullstack_server_diagnostics.sh"