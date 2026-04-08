#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/JATO_Analysis_System}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-jato-fullstack-backend@8000}"
BACKEND_PORT="${BACKEND_PORT:-}"
REMOTE_NAME="${REMOTE_NAME:-}"

BACKEND_DIR="$REPO_DIR/06_AppPlatform/backend"
FRONTEND_DIR="$REPO_DIR/06_AppPlatform/frontend"
BACKEND_REQUIREMENTS="$BACKEND_DIR/requirements.txt"
VENV_DIR="$REPO_DIR/.venv"

VITE_API_BASE="${VITE_API_BASE:-/v1}"
VITE_AUTH_TOKEN="${VITE_AUTH_TOKEN:-}"
VITE_USER_ROLE="${VITE_USER_ROLE:-viewer}"
VITE_USER_NAME="${VITE_USER_NAME:-anonymous}"

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

if [[ ! -d "$REPO_DIR/.git" ]]; then
  echo "[ERROR] Git repository not found at $REPO_DIR"
  exit 1
fi

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

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  echo "[ERROR] Python virtualenv not found: $VENV_DIR"
  echo "        Create it first with: python3 -m venv $VENV_DIR"
  exit 1
fi

echo "[INFO] Validate Node.js version"
node - <<'EOF'
const [major, minor, patch] = process.versions.node.split('.').map(Number);
const ok = (major === 20 && (minor > 19 || (minor === 19 && patch >= 0))) || (major === 22 && minor >= 12) || major > 22;
if (!ok) {
  console.error(`[ERROR] Node.js ${process.versions.node} detected. Need 20.19+ or 22.12+.`);
  process.exit(1);
}
console.log(`[INFO] Node.js ${process.versions.node}`);
EOF

echo "[INFO] Update repository"
cd "$REPO_DIR"
git fetch "$REMOTE_NAME" "$DEPLOY_BRANCH"
git checkout "$DEPLOY_BRANCH"
git pull --ff-only "$REMOTE_NAME" "$DEPLOY_BRANCH"

echo "[INFO] Install backend dependencies"
. "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip
pip install -r "$BACKEND_REQUIREMENTS"

echo "[INFO] Build frontend"
cd "$FRONTEND_DIR"
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
sudo -n systemctl restart "$BACKEND_SERVICE_NAME"
sudo -n systemctl --no-pager status "$BACKEND_SERVICE_NAME" | head -n 30

if systemctl is-active --quiet nginx; then
  echo "[INFO] Reload nginx"
  sudo -n systemctl reload nginx
fi

echo "[INFO] Verify backend health"
curl -fsS "http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null

echo "[INFO] Current revision"
git -C "$REPO_DIR" rev-parse --short HEAD