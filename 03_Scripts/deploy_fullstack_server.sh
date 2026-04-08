#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR="${REPO_DIR:-/opt/JATO_Analysis_System}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-jato-fullstack-backend@8000}"
BACKEND_PORT="${BACKEND_PORT:-}"
REMOTE_NAME="${REMOTE_NAME:-}"
REPO_REMOTE_URL="${REPO_REMOTE_URL:-git@github.com:tristan419/JATO_Analysis_System.git}"
SKIP_GIT_SYNC="${SKIP_GIT_SYNC:-false}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIAGNOSTIC_SCRIPT="$SCRIPT_DIR/print_fullstack_server_diagnostics.sh"
CURRENT_STEP="initialization"

BACKEND_DIR="$REPO_DIR/06_AppPlatform/backend"
FRONTEND_DIR="$REPO_DIR/06_AppPlatform/frontend"
BACKEND_REQUIREMENTS="$BACKEND_DIR/requirements.txt"
VENV_DIR="$REPO_DIR/.venv"

VITE_API_BASE="${VITE_API_BASE:-/v1}"
VITE_AUTH_TOKEN="${VITE_AUTH_TOKEN:-}"
VITE_USER_ROLE="${VITE_USER_ROLE:-viewer}"
VITE_USER_NAME="${VITE_USER_NAME:-anonymous}"

# ── China-friendly mirror defaults ──
NPM_REGISTRY="${NPM_REGISTRY:-https://mirrors.cloud.tencent.com/npm/}"
PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}"
PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}"

log_section() {
  printf '\n[STEP] %s\n' "$1"
}

run_diagnostics() {
  if [[ -x "$DIAGNOSTIC_SCRIPT" ]]; then
    REPO_DIR="$REPO_DIR" \
    BACKEND_SERVICE_NAME="$BACKEND_SERVICE_NAME" \
    BACKEND_PORT="$BACKEND_PORT" \
    bash "$DIAGNOSTIC_SCRIPT" || true
  fi
}

on_error() {
  local line_no="$1"
  local command="$2"
  echo
  echo "[ERROR] deploy_fullstack_server.sh failed"
  echo "[ERROR] step=$CURRENT_STEP"
  echo "[ERROR] line=$line_no"
  echo "[ERROR] command=$command"
  run_diagnostics
}

trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR

if [[ -z "$BACKEND_PORT" ]]; then
  if [[ "$BACKEND_SERVICE_NAME" =~ @([0-9]+)$ ]]; then
    BACKEND_PORT="${BASH_REMATCH[1]}"
  else
    BACKEND_PORT="8000"
  fi
fi

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "[ERROR] Missing required command: $name"
    exit 1
  fi
}

require_command git
require_command curl
require_command npm
require_command node

CURRENT_STEP="Validate sudo access"
log_section "$CURRENT_STEP"
if [[ "$(id -u)" -ne 0 ]]; then
  sudo -v
fi

if [[ ! -d "$REPO_DIR/.git" ]]; then
  if [[ ! -d "$REPO_DIR" ]]; then
    echo "[ERROR] Repository directory not found at $REPO_DIR"
    echo "        Download the codeload archive or clone the mirror first."
    exit 1
  fi
  echo "[INFO] No .git metadata found; continuing with local tree only"
fi

if [[ "$SKIP_GIT_SYNC" != "true" && -d "$REPO_DIR/.git" ]]; then
  if [[ -z "$REMOTE_NAME" ]]; then
    if git -C "$REPO_DIR" remote get-url origin >/dev/null 2>&1; then
      REMOTE_NAME="origin"
    else
      REMOTE_NAME="$(git -C "$REPO_DIR" remote | head -n 1)"
    fi
  fi

  if [[ -z "$REMOTE_NAME" ]]; then
    echo "[ERROR] No git remote found in $REPO_DIR"
    exit 1
  fi

  if [[ -n "$REPO_REMOTE_URL" ]]; then
    CURRENT_REMOTE_URL="$(git -C "$REPO_DIR" remote get-url "$REMOTE_NAME" 2>/dev/null || true)"
    if [[ -n "$CURRENT_REMOTE_URL" && "$CURRENT_REMOTE_URL" != "$REPO_REMOTE_URL" ]]; then
      echo "[INFO] Pointing git remote '$REMOTE_NAME' to mirror URL"
      echo "[INFO] old=$CURRENT_REMOTE_URL"
      echo "[INFO] new=$REPO_REMOTE_URL"
      git -C "$REPO_DIR" remote set-url "$REMOTE_NAME" "$REPO_REMOTE_URL"
    fi
  fi
fi

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  echo "[ERROR] Python virtualenv not found: $VENV_DIR"
  echo "        Create it first with: python3 -m venv $VENV_DIR"
  exit 1
fi

echo "[INFO] Validate Node.js version"
CURRENT_STEP="Validate Node.js version"
log_section "$CURRENT_STEP"
node - <<'EOF'
const [major, minor, patch] = process.versions.node.split('.').map(Number);
const ok = (major === 20 && minor >= 10) || (major === 22 && minor >= 0) || major > 22;
if (!ok) {
  console.error(`[ERROR] Node.js ${process.versions.node} detected. Need 20.10+ or 22.x+.`);
  process.exit(1);
}
console.log(`[INFO] Node.js ${process.versions.node}`);
EOF

echo "[INFO] Update repository"
CURRENT_STEP="Update repository"
log_section "$CURRENT_STEP"
if [[ "$SKIP_GIT_SYNC" == "true" ]]; then
  echo "[INFO] SKIP_GIT_SYNC=true; using the local tree without git pull"
elif [[ -d "$REPO_DIR/.git" ]]; then
  cd "$REPO_DIR"
  git fetch "$REMOTE_NAME" "$DEPLOY_BRANCH"
  git checkout "$DEPLOY_BRANCH"
  git pull --ff-only "$REMOTE_NAME" "$DEPLOY_BRANCH"
else
  echo "[INFO] No git repository metadata; skipping sync and using local tree"
fi

echo "[INFO] Install backend dependencies"
CURRENT_STEP="Install backend dependencies"
log_section "$CURRENT_STEP"
. "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip \
  -i "$PIP_INDEX_URL" --trusted-host "$PIP_TRUSTED_HOST"
pip install -r "$BACKEND_REQUIREMENTS" \
  -i "$PIP_INDEX_URL" --trusted-host "$PIP_TRUSTED_HOST"

echo "[INFO] Build frontend"
CURRENT_STEP="Build frontend"
log_section "$CURRENT_STEP"
cd "$FRONTEND_DIR"
npm config set registry "$NPM_REGISTRY"
echo "[INFO] npm registry → $NPM_REGISTRY"
npm ci
export VITE_API_BASE
export VITE_AUTH_TOKEN
export VITE_USER_ROLE
export VITE_USER_NAME
npm run build

if [[ ! -f "$FRONTEND_DIR/dist/index.html" ]]; then
  echo "[ERROR] Frontend build did not produce dist/index.html"
  exit 1
fi

echo "[INFO] Restart backend service"
CURRENT_STEP="Restart backend service"
log_section "$CURRENT_STEP"
if ! sudo -n systemctl cat "$BACKEND_SERVICE_NAME" >/dev/null 2>&1; then
  echo "[ERROR] systemd service not found: $BACKEND_SERVICE_NAME"
  echo "        Run bash 03_Scripts/tencent_fullstack_bootstrap.sh first."
  exit 1
fi
sudo -n systemctl restart "$BACKEND_SERVICE_NAME"
sudo -n systemctl --no-pager status "$BACKEND_SERVICE_NAME" | head -n 30

if systemctl is-active --quiet nginx; then
  echo "[INFO] Reload nginx"
  CURRENT_STEP="Reload nginx"
  log_section "$CURRENT_STEP"
  sudo -n systemctl reload nginx
fi

echo "[INFO] Verify backend health"
CURRENT_STEP="Verify backend health"
log_section "$CURRENT_STEP"
curl -fsS "http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null

echo "[INFO] Current revision"
CURRENT_STEP="Print revision"
log_section "$CURRENT_STEP"
if [[ -d "$REPO_DIR/.git" ]]; then
  git -C "$REPO_DIR" rev-parse --short HEAD
else
  echo "[INFO] No git revision available for archive-based bootstrap"
fi
echo "[INFO] Deployment finished successfully"
