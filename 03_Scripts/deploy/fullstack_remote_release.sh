#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/opt/JATO_Analysis_System-main"
ENV_FILE="/etc/jato-fullstack/backend.env"
LOCAL_NO_PROXY_HOSTS="localhost,127.0.0.1,::1"
export no_proxy="${no_proxy:+$no_proxy,}$LOCAL_NO_PROXY_HOSTS"
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}$LOCAL_NO_PROXY_HOSTS"

for required_name in \
  DEPLOY_COMMIT_SHA \
  DEPLOY_ARCHIVE_PATH \
  DEPLOY_BRANCH \
  DEPLOY_RUN_ID \
  DEPLOY_RUN_ATTEMPT \
  FRONTEND_ARTIFACT_NAME \
  FRONTEND_ARTIFACT_IDENTITY \
  FRONTEND_ARTIFACT_CHECKSUM \
  FRONTEND_GITHUB_ARTIFACT_ID \
  FRONTEND_GITHUB_ARTIFACT_DIGEST \
  FRONTEND_BUILD_ID \
  FRONTEND_NODE_VERSION
do
  if [ -z "${!required_name:-}" ]; then
    echo "[ERROR] $required_name is required for immutable production release"
    exit 1
  fi
done
if [ "$DEPLOY_BRANCH" != "main" ]; then
  echo "[ERROR] Immutable production release only accepts DEPLOY_BRANCH=main"
  exit 1
fi

RELEASE_WORKTREE="/tmp/JATO_deploy_work_${DEPLOY_COMMIT_SHA}"
RELEASE_ARCHIVE="$DEPLOY_ARCHIVE_PATH"
DEPLOY_SOURCE="github_actions_archive"
PRODUCTION_RELEASE_WORKFLOW="true"
PREBUILT_FRONTEND_DIR="$REPO_DIR/06_AppPlatform/frontend/.dist-${DEPLOY_COMMIT_SHA}.staged"
RUNTIME_PRESERVE_DIR="$(mktemp -d /tmp/JATO_deploy_runtime.XXXXXX)"
RUNTIME_PRESERVE_PATHS="
03_Scripts/diagnostics/artifacts
03_Scripts/logs
06_AppPlatform/frontend/dist
hermes/reports
"

rm -rf "$RELEASE_WORKTREE"
mkdir -p "$RELEASE_WORKTREE"
if [ ! -f "$RELEASE_ARCHIVE" ]; then
  echo "[ERROR] Uploaded production release archive is missing: $RELEASE_ARCHIVE"
  exit 1
fi
if ! tar tzf "$RELEASE_ARCHIVE" >/dev/null 2>&1; then
  echo "[ERROR] Uploaded production release archive is incomplete or invalid"
  exit 1
fi
echo "[INFO] Extracting uploaded production release archive: $RELEASE_ARCHIVE"
tar xzf "$RELEASE_ARCHIVE" -C "$RELEASE_WORKTREE"
ARCHIVE_COMMIT="$(python3 -c 'import json, sys; from pathlib import Path; payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")); print(payload.get("expectedCommitSha") or payload.get("commitSha") or "")' "$RELEASE_WORKTREE/hermes/deploy_release.json")"
if [ "$ARCHIVE_COMMIT" != "$DEPLOY_COMMIT_SHA" ]; then
  echo "[ERROR] Uploaded release commit mismatch: archive=${ARCHIVE_COMMIT:-missing} expected=$DEPLOY_COMMIT_SHA"
  exit 1
fi

FRONTEND_RELEASE_DIR="$RELEASE_WORKTREE/hermes/frontend_release"
FRONTEND_RELEASE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/frontend_release_artifact.py"
if [ ! -f "$FRONTEND_RELEASE_HELPER" ]; then
  echo "[ERROR] Immutable frontend release verifier is missing"
  exit 1
fi
python3 "$FRONTEND_RELEASE_HELPER" verify \
  --release-dir "$FRONTEND_RELEASE_DIR" \
  --expected-github-sha "$DEPLOY_COMMIT_SHA" \
  --expected-artifact-name "$FRONTEND_ARTIFACT_NAME" \
  --expected-artifact-identity "$FRONTEND_ARTIFACT_IDENTITY" \
  --expected-artifact-checksum "$FRONTEND_ARTIFACT_CHECKSUM" \
  --expected-build-id "$FRONTEND_BUILD_ID" \
  --expected-node-version "$FRONTEND_NODE_VERSION" \
  --expected-run-id "$DEPLOY_RUN_ID" \
  --expected-run-attempt "$DEPLOY_RUN_ATTEMPT" \
  --github-artifact-id "$FRONTEND_GITHUB_ARTIFACT_ID" \
  --github-artifact-digest "$FRONTEND_GITHUB_ARTIFACT_DIGEST"

for runtime_path in $RUNTIME_PRESERVE_PATHS; do
  if [ -e "$REPO_DIR/$runtime_path" ]; then
    mkdir -p "$RUNTIME_PRESERVE_DIR/$(dirname "$runtime_path")"
    cp -a "$REPO_DIR/$runtime_path" "$RUNTIME_PRESERVE_DIR/$runtime_path"
    echo "[INFO] Preserved runtime path: $runtime_path"
  fi
done

sudo mkdir -p "$REPO_DIR"
sudo chown -R "$USER":"$USER" "$REPO_DIR"
for release_path in 03_Scripts 06_AppPlatform 07_ScrapingToolkit hermes; do
  if [ -e "$RELEASE_WORKTREE/$release_path" ]; then
    rm -rf "$REPO_DIR/$release_path"
    (cd "$RELEASE_WORKTREE" && tar cf - "$release_path") | (cd "$REPO_DIR" && tar xf -)
  fi
done
if [ ! -f "$RELEASE_WORKTREE/01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx" ]; then
  echo "[ERROR] Production release archive is missing the required workbook"
  exit 1
fi
mkdir -p "$REPO_DIR/01_RAW_DATA"
cp -f "$RELEASE_WORKTREE/01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx" \
  "$REPO_DIR/01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx"
for release_file in .nvmrc requirements.txt CODEX.md; do
  if [ -f "$RELEASE_WORKTREE/$release_file" ]; then
    cp -f "$RELEASE_WORKTREE/$release_file" "$REPO_DIR/$release_file"
  fi
done
for runtime_path in $RUNTIME_PRESERVE_PATHS; do
  if [ -e "$RUNTIME_PRESERVE_DIR/$runtime_path" ]; then
    if [ -d "$RUNTIME_PRESERVE_DIR/$runtime_path" ] && [ -d "$REPO_DIR/$runtime_path" ]; then
      (cd "$RUNTIME_PRESERVE_DIR" && tar cf - "$runtime_path") | (cd "$REPO_DIR" && tar xf -)
      echo "[INFO] Merged runtime path: $runtime_path"
    else
      rm -rf "$REPO_DIR/$runtime_path"
      mkdir -p "$(dirname "$REPO_DIR/$runtime_path")"
      cp -a "$RUNTIME_PRESERVE_DIR/$runtime_path" "$REPO_DIR/$runtime_path"
      echo "[INFO] Restored runtime path: $runtime_path"
    fi
  fi
done
rm -rf "$RUNTIME_PRESERVE_DIR"

rm -rf "$PREBUILT_FRONTEND_DIR"
python3 "$FRONTEND_RELEASE_HELPER" verify \
  --release-dir "$FRONTEND_RELEASE_DIR" \
  --expected-github-sha "$DEPLOY_COMMIT_SHA" \
  --expected-artifact-name "$FRONTEND_ARTIFACT_NAME" \
  --expected-artifact-identity "$FRONTEND_ARTIFACT_IDENTITY" \
  --expected-artifact-checksum "$FRONTEND_ARTIFACT_CHECKSUM" \
  --expected-build-id "$FRONTEND_BUILD_ID" \
  --expected-node-version "$FRONTEND_NODE_VERSION" \
  --expected-run-id "$DEPLOY_RUN_ID" \
  --expected-run-attempt "$DEPLOY_RUN_ATTEMPT" \
  --github-artifact-id "$FRONTEND_GITHUB_ARTIFACT_ID" \
  --github-artifact-digest "$FRONTEND_GITHUB_ARTIFACT_DIGEST" \
  --materialize-dir "$PREBUILT_FRONTEND_DIR"

rm -rf "$RELEASE_WORKTREE"
rm -f "$RELEASE_ARCHIVE"

echo "[INFO] Refreshing local mihomo subscription before backend restart..."
mkdir -p "$REPO_DIR/hermes/reports"
MIHOMO_REFRESH_LOG="$REPO_DIR/hermes/reports/mihomo_refresh_status.txt"
PACKAGED_MIHOMO_CONFIG="$REPO_DIR/hermes/runtime/mihomo/config.yaml"
{
  date -u
  if [ -n "${MIHOMO_SUB_URL:-}" ]; then
    echo "[mihomo-sub] MIHOMO_SUB_URL configured from deploy environment"
  else
    echo "[mihomo-sub] MIHOMO_SUB_URL not configured; trying local protected file and 0dcloud discovery"
  fi
  echo "[mihomo-sub] MIHOMO_SUB_URL_FILE=${MIHOMO_SUB_URL_FILE:-/etc/mihomo/subscription_url}"
  echo "[mihomo-sub] MIHOMO_DB_PATH=${MIHOMO_DB_PATH:-auto}"
  if [ -r "$PACKAGED_MIHOMO_CONFIG" ]; then
    echo "[mihomo-sub] Using packaged mihomo config from GitHub runner"
    MIHOMO_LOCAL=true MIHOMO_SOURCE_CONFIG="$PACKAGED_MIHOMO_CONFIG" \
      bash "$REPO_DIR/03_Scripts/deploy/update_mihomo_subscription.sh"
  else
    MIHOMO_LOCAL=true bash "$REPO_DIR/03_Scripts/deploy/update_mihomo_subscription.sh"
  fi
  rm -f "$PACKAGED_MIHOMO_CONFIG"
} > "$MIHOMO_REFRESH_LOG" 2>&1 \
  || echo "[WARN] Local mihomo refresh failed; continuing with existing proxy config" >> "$MIHOMO_REFRESH_LOG"

export REPO_DIR DEPLOY_SOURCE PREBUILT_FRONTEND_DIR PRODUCTION_RELEASE_WORKFLOW
python3 - <<'PY'
import datetime as _dt
import json
import os
import pathlib

root = pathlib.Path(os.environ["REPO_DIR"])
commit_sha = os.environ.get("DEPLOY_COMMIT_SHA", "")
out = root / "hermes" / "deploy_release.json"
try:
    payload = json.loads(out.read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError, OSError):
    payload = {}
payload.update({
    "releaseId": os.environ.get("DEPLOY_RELEASE_ID", ""),
    "service": "jato-fullstack-backend",
    "environment": "production",
    "expectedCommitSha": commit_sha,
    "commitSha": commit_sha,
    "shortSha": os.environ.get("DEPLOY_SHORT_SHA") or commit_sha[:8],
    "branch": os.environ.get("DEPLOY_BRANCH", "main"),
    "repository": os.environ.get("DEPLOY_REPOSITORY", "tristan419/JATO_Analysis_System"),
    "workflow": os.environ.get("DEPLOY_WORKFLOW", "production-release"),
    "workflowRunId": os.environ.get("DEPLOY_RUN_ID", ""),
    "workflowRunAttempt": os.environ.get("DEPLOY_RUN_ATTEMPT", ""),
    "deployMethod": "github_actions",
    "packagedAt": _dt.datetime.now(_dt.UTC).isoformat(),
    "source": os.environ.get("DEPLOY_SOURCE", "github_actions_archive"),
})
if not isinstance(payload.get("frontendRelease"), dict):
    raise SystemExit("[ERROR] deploy_release.json is missing frontendRelease provenance")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"[INFO] Wrote {out.relative_to(root)} for release {payload['shortSha']}")
PY

if [ -n "${DEEPSEEK_API_KEY:-}" ]; then
  echo "[INFO] Configuring DeepSeek in backend env..."
  sudo -n mkdir -p "$(dirname "$ENV_FILE")" 2>/dev/null || true
  if sudo -n test -f "$ENV_FILE" 2>/dev/null; then
    sudo -n sed -i '/^DEEPSEEK_API_KEY=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^HERMES_SYNC_TOKEN=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_COUNTRY_CHAT_DEEPSEEK_TIMEOUT_SECONDS=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_COUNTRY_CHAT_MODEL_OPTIONS=/d' "$ENV_FILE" 2>/dev/null || true
    {
      echo "DEEPSEEK_API_KEY=$DEEPSEEK_API_KEY"
      echo "HERMES_SYNC_TOKEN=$HERMES_SYNC_TOKEN"
      echo "APP_COUNTRY_CHAT_DEEPSEEK_TIMEOUT_SECONDS=60"
      echo "APP_COUNTRY_CHAT_MODEL_OPTIONS=deepseek:deepseek-chat"
    } | sudo -n tee -a "$ENV_FILE" > /dev/null 2>&1 || true
    echo "[INFO] DeepSeek config updated"
  fi
fi

if [ -n "${GOOGLE_CLIENT_ID:-}" ]; then
  echo "[INFO] Configuring Google OAuth in backend env..."
  sudo -n mkdir -p "$(dirname "$ENV_FILE")" 2>/dev/null || true
  if sudo -n test -f "$ENV_FILE" 2>/dev/null; then
    sudo -n sed -i '/^APP_GOOGLE_CLIENT_ID=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GOOGLE_CLIENT_SECRET=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GOOGLE_REDIRECT_URI=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_FRONTEND_ORIGIN=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_FRONTEND_ORIGINS=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_CORS_ORIGINS=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GOOGLE_OAUTH_PROXY_URL=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GOOGLE_OAUTH_RELAY_URL=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GOOGLE_OAUTH_RELAY_TOKEN=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GOOGLE_OAUTH_TIMEOUT_SECONDS=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GROUPED_TIME_SERIES_PREWARM_GROUP_BY=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GROUPED_TIME_SERIES_PREWARM_GRAINS=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GROUPED_TIME_SERIES_PREWARM_SCOPES=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_ADVANCED_ANALYSIS_WARMUP_COUNTRIES=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_ADVANCED_ANALYSIS_WARMUP_SCOPES=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_ADVANCED_ANALYSIS_WARMUP_SALES_MODES=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_ADVANCED_ANALYSIS_WARMUP_TOP_N=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_ADVANCED_ANALYSIS_WARMUP_PROFILE_OPTIONS=/d' "$ENV_FILE" 2>/dev/null || true
    sudo -n sed -i '/^APP_ADVANCED_ANALYSIS_WARMUP_COMPETITOR_SET=/d' "$ENV_FILE" 2>/dev/null || true
    GOOGLE_OAUTH_PROXY_URL="${GOOGLE_OAUTH_PROXY_URL:-http://127.0.0.1:7897}"
    {
      echo "APP_GOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID"
      echo "APP_GOOGLE_CLIENT_SECRET=$GOOGLE_CLIENT_SECRET"
      echo "APP_GOOGLE_REDIRECT_URI=https://www.ojeur.cloud/v1/auth/google/callback"
      echo "APP_FRONTEND_ORIGIN=https://www.ojeur.cloud"
      echo "APP_FRONTEND_ORIGINS=https://www.ojeur.cloud,https://intl.ojeur.cloud"
      echo "APP_CORS_ORIGINS=https://www.ojeur.cloud,https://intl.ojeur.cloud,http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000"
      echo "APP_GOOGLE_OAUTH_PROXY_URL=$GOOGLE_OAUTH_PROXY_URL"
      if [ -n "${GOOGLE_OAUTH_RELAY_URL:-}" ]; then
        echo "APP_GOOGLE_OAUTH_RELAY_URL=$GOOGLE_OAUTH_RELAY_URL"
      fi
      if [ -n "${GOOGLE_OAUTH_RELAY_TOKEN:-}" ]; then
        echo "APP_GOOGLE_OAUTH_RELAY_TOKEN=$GOOGLE_OAUTH_RELAY_TOKEN"
      fi
      echo "APP_GOOGLE_OAUTH_TIMEOUT_SECONDS=30"
      echo "APP_GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED=true"
      echo "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=true"
      echo "APP_GROUPED_TIME_SERIES_PREWARM_GROUP_BY=动总规整,国家"
      echo "APP_GROUPED_TIME_SERIES_PREWARM_GRAINS=month,year"
      echo "APP_GROUPED_TIME_SERIES_PREWARM_SCOPES=viewer,order_filler,editor,admin"
      echo "APP_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON='[{\"国家\":[\"丹麦\",\"克罗地亚\",\"匈牙利\",\"奥地利\",\"希腊\",\"德国\",\"意大利\",\"挪威\",\"捷克\",\"斯洛伐克\",\"斯洛文尼亚\",\"比利时\",\"法国\",\"波兰\",\"瑞典\",\"瑞士\",\"罗马尼亚\",\"芬兰\",\"荷兰\",\"葡萄牙\",\"西班牙\"],\"动总规整\":[\"ICE\",\"HEV\",\"BEV\",\"MHEV\",\"PHEV\"]}]'"
      echo "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=true"
      echo "APP_ADVANCED_ANALYSIS_WARMUP_COUNTRIES=瑞典"
      echo "APP_ADVANCED_ANALYSIS_WARMUP_SCOPES=viewer,order_filler,editor,admin"
      echo "APP_ADVANCED_ANALYSIS_WARMUP_SALES_MODES=month"
      echo "APP_ADVANCED_ANALYSIS_WARMUP_TOP_N=15"
      echo "APP_ADVANCED_ANALYSIS_WARMUP_PROFILE_OPTIONS=true"
      echo "APP_ADVANCED_ANALYSIS_WARMUP_COMPETITOR_SET=false"
    } | sudo -n tee -a "$ENV_FILE" > /dev/null 2>&1 || true
    echo "[INFO] Google OAuth config updated"
  fi
  if [ -n "${GOOGLE_OAUTH_RELAY_URL:-}" ]; then
    echo "[INFO] Google OAuth relay check via ${GOOGLE_OAUTH_RELAY_URL}"
    curl -fsS --max-time 20 "${GOOGLE_OAUTH_RELAY_URL%/}/healthz" || true
  else
    check_google_oauth_proxy() {
      local label="$1"
      local proxy_url="${GOOGLE_OAUTH_PROXY_URL:-http://127.0.0.1:7897}"
      echo "[INFO] Google OAuth proxy check (${label}) via ${proxy_url}"
      curl -I --max-time 20 --proxy "$proxy_url" https://oauth2.googleapis.com/token
    }
    if ! check_google_oauth_proxy "before-mihomo-restart"; then
      echo "[WARN] Google OAuth proxy check failed; restarting mihomo"
      sudo -n systemctl restart mihomo.service 2>/dev/null \
        || sudo -n systemctl restart mihomo 2>/dev/null \
        || true
      sleep 5
      if ! check_google_oauth_proxy "after-mihomo-restart"; then
        echo "[WARN] Google OAuth proxy still failed; selecting a working mihomo node"
        bash "$REPO_DIR/03_Scripts/deploy/select_mihomo_google_proxy.sh" || true
        check_google_oauth_proxy "after-mihomo-select" || true
      fi
    fi
  fi
fi

cd "$REPO_DIR"
export REPO_DIR SKIP_GIT_SYNC=true
export BACKEND_SERVICE_NAME="jato-fullstack-backend@8000"
set +e
bash 03_Scripts/deploy_fullstack_server.sh 2>&1
DEPLOY_RC=$?
set -e

FRONTEND_ROOT="$REPO_DIR/06_AppPlatform/frontend/dist"

if [ "$DEPLOY_RC" -eq 0 ] && [ -n "$DEPLOY_SERVER_NAME" ] && [ "$DEPLOY_SERVER_NAME" != "_" ]; then
  if [ "${DEPLOY_ENABLE_HTTPS,,}" = "true" ]; then
    sudo SERVER_NAME="$DEPLOY_SERVER_NAME" BACKEND_PORT=8000 FRONTEND_ROOT="$FRONTEND_ROOT" \
      CERTBOT_EMAIL="$DEPLOY_CERTBOT_EMAIL" CERTBOT_RENEW_DRY_RUN=false \
      bash 03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh
  else
    sudo SERVER_NAME="$DEPLOY_SERVER_NAME" BACKEND_PORT=8000 FRONTEND_ROOT="$FRONTEND_ROOT" \
      bash 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
  fi
  NGINX_CONF=$(sudo -n grep -l 'proxy_pass http://jato_fullstack_api' /etc/nginx/sites-enabled/* /etc/nginx/conf.d/* 2>/dev/null | sed -n '1p' || echo "")
  if [ -n "$NGINX_CONF" ] && sudo -n test -f "$NGINX_CONF"; then
    sudo -n sed -i 's/proxy_buffering on;/proxy_buffering off;/g' "$NGINX_CONF" 2>/dev/null || true
    sudo -n nginx -t 2>/dev/null && sudo -n systemctl reload nginx 2>/dev/null || true
    echo "[INFO] nginx proxy_buffering disabled for SSE streaming"
  fi
fi

DIST="$REPO_DIR/06_AppPlatform/frontend/dist"
mkdir -p "$DIST"
{
  echo "deploy_exit_code=$DEPLOY_RC"
  echo "timestamp=$(date -u)"
  echo "---systemctl---"
  sudo -n systemctl status jato-fullstack-backend@8000 --no-pager 2>&1 || true
  echo "---healthz---"
  curl --noproxy '*' -fsS http://127.0.0.1:8000/healthz 2>&1 || echo "HEALTHZ_FAILED"
  echo "---google oauth proxy---"
  sudo -n awk -F= '/^(APP_GOOGLE_OAUTH_PROXY_URL|APP_GOOGLE_OAUTH_RELAY_URL|APP_GOOGLE_OAUTH_TIMEOUT_SECONDS|APP_GOOGLE_REDIRECT_URI|APP_FRONTEND_ORIGIN|APP_FRONTEND_ORIGINS|APP_CORS_ORIGINS|APP_GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED|APP_GROUPED_TIME_SERIES_PREWARM_ENABLED|APP_GROUPED_TIME_SERIES_PREWARM_GROUP_BY|APP_GROUPED_TIME_SERIES_PREWARM_GRAINS|APP_GROUPED_TIME_SERIES_PREWARM_SCOPES|APP_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON|APP_ADVANCED_ANALYSIS_WARMUP_[A-Z_]+)=/ {print}' "$ENV_FILE" 2>&1 || true
  sudo -n awk -F= '/^APP_GOOGLE_OAUTH_RELAY_TOKEN=/ {print "APP_GOOGLE_OAUTH_RELAY_TOKEN=configured"}' "$ENV_FILE" 2>&1 || true
  if [ -n "${GOOGLE_OAUTH_RELAY_URL:-}" ]; then
    curl -fsS --max-time 20 "${GOOGLE_OAUTH_RELAY_URL%/}/healthz" 2>&1 || true
  fi
  systemctl is-active mihomo 2>&1 || true
  ss -ltnp | grep -E '(:7897)\b' 2>&1 || true
  curl -I --max-time 20 --proxy "${GOOGLE_OAUTH_PROXY_URL:-http://127.0.0.1:7897}" https://oauth2.googleapis.com/token 2>&1 || true
  echo "---mihomo refresh---"
  cat "$REPO_DIR/hermes/reports/mihomo_refresh_status.txt" 2>&1 || echo "MIHOMO_REFRESH_STATUS_MISSING"
  echo "---release---"
  cat "$REPO_DIR/hermes/deploy_release.json" 2>&1 || echo "RELEASE_METADATA_MISSING"
  echo "---deploy failure context---"
  cat "$REPO_DIR/hermes/deploy_failure_context.txt" 2>&1 || echo "DEPLOY_FAILURE_CONTEXT_MISSING"
  echo "---nginx---"
  sudo -n systemctl status nginx --no-pager 2>&1 | sed -n '1,5p' || true
  echo "---msrp scheduler---"
  sudo -n systemctl status jato-msrp-dryrun.timer --no-pager 2>&1 || true
  sudo -n systemctl status jato-msrp-sync@dryrun.service --no-pager 2>&1 || true
  echo "---msrp timers---"
  sudo -n systemctl list-timers --all 'jato-msrp*' --no-pager 2>&1 || true
  echo "---msrp env---"
  bash "$REPO_DIR/03_Scripts/ops/print_msrp_env_status.sh" 2>&1 || true
  echo "---msrp artifacts---"
  for artifact_path in \
    03_Scripts/diagnostics/artifacts/dryrun_report.json \
    03_Scripts/diagnostics/artifacts/dryrun_runs_index.json \
    hermes/reports/msrp_country_progress.json \
    hermes/reports/pipeline_status/msrp_dryrun.json
  do
    if [ -e "$REPO_DIR/$artifact_path" ]; then
      stat -c "$artifact_path size=%s mtime=%y" "$REPO_DIR/$artifact_path" 2>&1 || true
    else
      echo "missing $artifact_path"
    fi
  done
  echo "---index.html---"
  head -1 "$DIST/index.html" 2>&1 || echo "INDEX_MISSING"
} > "$DIST/_deploy_status.txt" 2>&1

if [ "$DEPLOY_RC" -ne 0 ]; then
  exit "$DEPLOY_RC"
fi
curl --noproxy '*' -fsS http://127.0.0.1:8000/healthz >/dev/null
