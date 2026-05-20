#!/usr/bin/env bash
set -Eeuo pipefail

DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-jato-fullstack-backend@8000}"
BACKEND_PORT="${BACKEND_PORT:-}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
RUN_DATABASE_MIGRATIONS="${RUN_DATABASE_MIGRATIONS:-auto}"
REMOTE_NAME="${REMOTE_NAME:-}"
REPO_REMOTE_URL="${REPO_REMOTE_URL:-git@github.com:tristan419/JATO_Analysis_System.git}"
SKIP_GIT_SYNC="${SKIP_GIT_SYNC:-false}"
DEPLOY_PRUNE_UNTRACKED="${DEPLOY_PRUNE_UNTRACKED:-true}"
DEPLOY_UNTRACKED_CLEAN_PATTERNS="${DEPLOY_UNTRACKED_CLEAN_PATTERNS:-04_Processed_data/.refresh_backups/pre-sync-* Markdown_Readme/Fullstack/*.md Markdown_Readme/Streamlit/*.md}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIAGNOSTIC_SCRIPT="$SCRIPT_DIR/print_fullstack_server_diagnostics.sh"
CURRENT_STEP="initialization"

VITE_API_BASE="${VITE_API_BASE:-/v1}"
VITE_AUTH_TOKEN="${VITE_AUTH_TOKEN:-}"
VITE_USER_ROLE="${VITE_USER_ROLE:-viewer}"
VITE_USER_NAME="${VITE_USER_NAME:-anonymous}"

# ── China-friendly mirror defaults ──
NPM_REGISTRY="${NPM_REGISTRY:-https://mirrors.cloud.tencent.com/npm/}"
PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}"
PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}"
LOCAL_NO_PROXY_HOSTS="localhost,127.0.0.1,::1"
export no_proxy="${no_proxy:+$no_proxy,}$LOCAL_NO_PROXY_HOSTS"
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}$LOCAL_NO_PROXY_HOSTS"

resolve_repo_dir() {
  if [[ -n "${REPO_DIR:-}" ]]; then
    printf '%s\n' "$REPO_DIR"
    return
  fi

  local candidate=""
  for candidate in \
    /opt/JATO_Analysis_System-main \
    /opt/JATO_Analysis_System \
    /var/www/JATO_Analysis_System
  do
    if [[ -d "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return
    fi
  done

  printf '%s\n' /opt/JATO_Analysis_System-main
}

REPO_DIR="$(resolve_repo_dir)"

BACKEND_DIR="$REPO_DIR/06_AppPlatform/backend"
FRONTEND_DIR="$REPO_DIR/06_AppPlatform/frontend"
BACKEND_REQUIREMENTS="$BACKEND_DIR/requirements.txt"
VENV_DIR="$REPO_DIR/.venv"
TOOLKIT_DIR="$REPO_DIR/07_ScrapingToolkit"
DEPLOY_RELEASE_FILE="$REPO_DIR/hermes/deploy_release.json"
SYSTEMD_SOURCE_DIR="$REPO_DIR/03_Scripts/deploy/systemd"
SYSTEMD_TARGET_DIR="/etc/systemd/system"
JATO_ETC_DIR="/etc/jato-fullstack"
ENABLE_SCRAPER_SCHEDULERS="${ENABLE_SCRAPER_SCHEDULERS:-true}"

log_section() {
  printf '\n[STEP] %s\n' "$1"
}

is_truthy() {
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

run_privileged_bash() {
  local script="$1"
  shift

  if [[ "$(id -u)" -eq 0 ]]; then
    bash -lc "$script" _ "$@"
  else
    sudo -n bash -lc "$script" _ "$@"
  fi
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

echo "[INFO] Repository directory: $REPO_DIR"
if [[ -f "$DEPLOY_RELEASE_FILE" ]]; then
  echo "[INFO] Deploy release metadata:"
  python - "$DEPLOY_RELEASE_FILE" <<'PY' || true
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text())
print(f"[INFO] release={payload.get('shortSha') or str(payload.get('commitSha', ''))[:8]} run={payload.get('workflowRunId', '')} source={payload.get('source', '')}")
PY
else
  echo "[WARN] Deploy release metadata missing: $DEPLOY_RELEASE_FILE"
fi

cleanup_known_untracked_paths() {
  local raw_pattern=""
  local candidate=""
  local status_output=""
  local removed_count=0

  if ! is_truthy "$DEPLOY_PRUNE_UNTRACKED"; then
    echo "[INFO] Skipping untracked cleanup because DEPLOY_PRUNE_UNTRACKED=$DEPLOY_PRUNE_UNTRACKED"
    return
  fi

  if [[ ! -d "$REPO_DIR/.git" ]]; then
    echo "[INFO] Skipping untracked cleanup because git metadata is unavailable"
    return
  fi

  cd "$REPO_DIR"
  shopt -s nullglob dotglob
  for raw_pattern in $DEPLOY_UNTRACKED_CLEAN_PATTERNS; do
    for candidate in $raw_pattern; do
      [[ -e "$candidate" ]] || continue
      if git ls-files --error-unmatch -- "$candidate" >/dev/null 2>&1; then
        continue
      fi

      status_output="$(git status --short --untracked-files=all -- "$candidate" || true)"
      if [[ -z "$status_output" ]] || ! grep -q '^?? ' <<< "$status_output"; then
        continue
      fi

      echo "[INFO] Pruning known untracked path: $candidate"
      git clean -fd -- "$candidate"
      removed_count=$((removed_count + 1))
    done
  done
  shopt -u nullglob dotglob

  if [[ "$removed_count" -eq 0 ]]; then
    echo "[INFO] No matching known untracked paths found"
  else
    echo "[INFO] Pruned $removed_count known untracked path(s)"
  fi
}

install_systemd_file() {
  local source_path="$1"
  local target_name="${2:-$(basename "$source_path")}"
  local temp_file=""

  if [[ ! -f "$source_path" ]]; then
    echo "[ERROR] Missing systemd source file: $source_path"
    exit 1
  fi

  temp_file="$(mktemp)"
  sed "s|/opt/JATO_Analysis_System-main|$REPO_DIR|g" "$source_path" > "$temp_file"
  sudo -n install -D -m 644 "$temp_file" "$SYSTEMD_TARGET_DIR/$target_name"
  rm -f "$temp_file"
}

install_env_file_if_missing() {
  local source_path="$1"
  local target_path="$2"
  local mode="${3:-600}"
  local temp_file=""

  if [[ ! -f "$source_path" ]]; then
    echo "[ERROR] Missing env template: $source_path"
    exit 1
  fi

  if sudo -n test -e "$target_path"; then
    echo "[INFO] Existing env file preserved: $target_path"
    return 0
  fi

  temp_file="$(mktemp)"
  sed "s|/opt/JATO_Analysis_System-main|$REPO_DIR|g" "$source_path" > "$temp_file"
  sudo -n install -D -m "$mode" "$temp_file" "$target_path"
  rm -f "$temp_file"
  echo "[INFO] Installed default env file: $target_path"
}

restart_timer_unit() {
  local timer_name="$1"

  sudo -n systemctl enable "$timer_name"
  sudo -n systemctl restart "$timer_name"
  sudo -n systemctl --no-pager status "$timer_name" 2>&1 | head -n 12 || true
}

reconcile_scraper_schedulers() {
  if ! is_truthy "$ENABLE_SCRAPER_SCHEDULERS"; then
    echo "[INFO] Skipping scraper scheduler reconciliation because ENABLE_SCRAPER_SCHEDULERS=$ENABLE_SCRAPER_SCHEDULERS"
    return 0
  fi

  install_env_file_if_missing \
    "$SYSTEMD_SOURCE_DIR/jato-country-news.env.example" \
    "$JATO_ETC_DIR/country-news.env"
  install_env_file_if_missing \
    "$SYSTEMD_SOURCE_DIR/jato-msrp.env.example" \
    "$JATO_ETC_DIR/msrp.env"
  install_env_file_if_missing \
    "$SYSTEMD_SOURCE_DIR/jato-voc.env.example" \
    "$JATO_ETC_DIR/voc.env"

  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-country-news-sync.service"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-country-news-sync.timer"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-country-news-sync-b.service"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-country-news-sync-b.timer"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-msrp-sync@.service"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-msrp-dryrun.timer"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-msrp-ingest.timer"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-voc-forum-sync.service"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/jato-voc-forum-sync.timer"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/hermes-source-quality.service"
  install_systemd_file "$SYSTEMD_SOURCE_DIR/hermes-source-quality.timer"

  sudo -n systemctl daemon-reload

  restart_timer_unit jato-country-news-sync.timer
  restart_timer_unit jato-country-news-sync-b.timer
  restart_timer_unit jato-msrp-dryrun.timer
  restart_timer_unit jato-msrp-ingest.timer
  restart_timer_unit jato-voc-forum-sync.timer
  restart_timer_unit hermes-source-quality.timer
}

CURRENT_STEP="Validate sudo access"
log_section "$CURRENT_STEP"
if [[ "$(id -u)" -ne 0 ]]; then
  if ! sudo -n true 2>/dev/null; then
    echo "[WARN] sudo requires a password; skipping sudo -v (CI mode)"
    echo "[WARN] Later sudo -n calls may fail if NOPASSWD is not configured"
  fi
fi

if [[ ! -d "$REPO_DIR/.git" ]]; then
  if [[ ! -d "$REPO_DIR" ]]; then
    echo "[ERROR] Repository directory not found at $REPO_DIR"
    echo "        Download the codeload archive or clone the mirror first."
    exit 1
  fi
  echo "[INFO] No .git metadata found; continuing with local tree only"
fi

CURRENT_STEP="Prune known untracked paths"
log_section "$CURRENT_STEP"
cleanup_known_untracked_paths

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
node -v
npm -v
node - <<'EOF'
const [major, minor, patch] = process.versions.node.split('.').map(Number);
// Must match engines in 06_AppPlatform/frontend/package.json
const ok = (
  (major === 20 && minor >= 19) ||
  (major === 22 && minor >= 12) ||
  major > 22
);
if (!ok) {
  console.error(`[ERROR] Node.js ${process.versions.node} detected.`);
  console.error('[ERROR] Required: >=20.19.0 <21 or >=22.12.0 (see frontend package.json engines)');
  process.exit(1);
}
console.log(`[INFO] Node.js ${process.versions.node} — matches frontend engines requirement`);
EOF

echo "[INFO] Update repository"
CURRENT_STEP="Update repository"
log_section "$CURRENT_STEP"
if [[ "$SKIP_GIT_SYNC" == "true" ]]; then
  echo "[INFO] SKIP_GIT_SYNC=true; using the local tree without git pull"
elif [[ -d "$REPO_DIR/.git" ]]; then
  cd "$REPO_DIR"
  git fetch "$REMOTE_NAME" "$DEPLOY_BRANCH"
  if git rev-parse --verify HEAD >/dev/null 2>&1; then
    git checkout "$DEPLOY_BRANCH"
    git pull --ff-only "$REMOTE_NAME" "$DEPLOY_BRANCH"
  else
    echo "[INFO] Repository has no local commits yet; bootstrapping $DEPLOY_BRANCH from $REMOTE_NAME/$DEPLOY_BRANCH"
    git checkout -f -B "$DEPLOY_BRANCH" "$REMOTE_NAME/$DEPLOY_BRANCH"
  fi
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

echo "[INFO] Install scraping toolkit"
CURRENT_STEP="Install scraping toolkit"
log_section "$CURRENT_STEP"
if [[ ! -d "$TOOLKIT_DIR" ]]; then
  echo "[ERROR] Scraping toolkit directory not found: $TOOLKIT_DIR"
  exit 1
fi
python -m pip install -e "$TOOLKIT_DIR" \
  -i "$PIP_INDEX_URL" --trusted-host "$PIP_TRUSTED_HOST"

echo "[INFO] Install Playwright browsers (headless chromium)"
CURRENT_STEP="Install Playwright browsers"
log_section "$CURRENT_STEP"
if "$VENV_DIR/bin/python" -c "import playwright" 2>/dev/null; then
  (
    export http_proxy="${http_proxy:-http://127.0.0.1:7897}"
    export https_proxy="${https_proxy:-http://127.0.0.1:7897}"
    export no_proxy="${no_proxy:-$LOCAL_NO_PROXY_HOSTS}"
    export NO_PROXY="${NO_PROXY:-$LOCAL_NO_PROXY_HOSTS}"
    "$VENV_DIR/bin/playwright" install chromium 2>&1
  ) || echo "[WARN] playwright install chromium failed — MSRP scraper may not work"
  # Also cache for root (systemd services run as root)
  if [[ -d "$HOME/.cache/ms-playwright" ]]; then
    sudo -n mkdir -p /root/.cache/ms-playwright
    sudo -n cp -a "$HOME/.cache/ms-playwright/." /root/.cache/ms-playwright/ 2>/dev/null || true
    echo "[INFO] Playwright browser cache synced to /root/.cache/ms-playwright"
  fi
else
  echo "[INFO] Playwright not installed; skipping browser install"
fi

echo "[INFO] Reconcile scraper schedulers"
CURRENT_STEP="Reconcile scraper schedulers"
log_section "$CURRENT_STEP"
reconcile_scraper_schedulers

echo "[INFO] Run database migrations when configured"
CURRENT_STEP="Run database migrations"
log_section "$CURRENT_STEP"
if [[ "$RUN_DATABASE_MIGRATIONS" == "auto" ]]; then
  if [[ -f "$BACKEND_ENV_FILE" ]]; then
    if db_state="$(run_privileged_bash 'set -a; . "$1"; set +a; if [[ -n "${APP_DATABASE_URL:-}" ]] && [[ "${APP_DATABASE_ENABLED:-false}" =~ ^(1|true|yes|on)$ ]]; then echo run; else echo skip; fi' "$BACKEND_ENV_FILE" 2>/dev/null)"; then
      RUN_DATABASE_MIGRATIONS="$db_state"
    else
      RUN_DATABASE_MIGRATIONS="skip"
    fi
  else
    RUN_DATABASE_MIGRATIONS="skip"
  fi
fi

if [[ "$RUN_DATABASE_MIGRATIONS" == "true" || "$RUN_DATABASE_MIGRATIONS" == "run" ]]; then
  run_privileged_bash 'set -Eeuo pipefail; set -a; . "$1"; set +a; export PYTHONPATH="$2"; . "$3/bin/activate"; cd "$2"; python -m alembic upgrade head' \
    "$BACKEND_ENV_FILE" "$BACKEND_DIR" "$VENV_DIR"
else
  echo "[INFO] Database migrations skipped (database not configured)"
fi

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
sleep 2
sudo -n systemctl --no-pager status "$BACKEND_SERVICE_NAME" 2>&1 | head -n 30 || true

if systemctl is-active --quiet nginx; then
  echo "[INFO] Reload nginx"
  CURRENT_STEP="Reload nginx"
  log_section "$CURRENT_STEP"
  sudo -n systemctl reload nginx
fi

echo "[INFO] Verify backend health"
CURRENT_STEP="Verify backend health"
log_section "$CURRENT_STEP"
for i in $(seq 1 15); do
  if curl --noproxy '*' -fsS "http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null 2>&1; then
    echo "[INFO] Health check passed on attempt $i"
    break
  fi
  if [[ "$i" -eq 15 ]]; then
    echo "[ERROR] Health check failed after 15 attempts"
    exit 1
  fi
  echo "[INFO] Health check attempt $i failed, retrying in 5s …"
  sleep 5
done

echo "[INFO] Current revision"
CURRENT_STEP="Print revision"
log_section "$CURRENT_STEP"
if [[ -d "$REPO_DIR/.git" ]]; then
  git -C "$REPO_DIR" rev-parse --short HEAD
else
  echo "[INFO] No git revision available for archive-based bootstrap"
fi
echo "[INFO] Deployment finished successfully"
