#!/usr/bin/env bash
set -Eeuo pipefail

ACTION="${1:-}"
RELEASES_ROOT="${RELEASES_ROOT:-/opt/jato/releases}"
SLOTS_ROOT="${SLOTS_ROOT:-/opt/jato/slots}"
LEGACY_ROOT="${LEGACY_ROOT:-/opt/JATO_Analysis_System-main}"
REPORTS_ROOT="${REPORTS_ROOT:-/opt/jato/operation-reports}"
ACTIVE_SLOT_FILE="${ACTIVE_SLOT_FILE:-/var/lib/jato-release/active-slot}"
DEPLOYMENT_MARKER="${DEPLOYMENT_MARKER:-/var/lib/jato-release/deployment-maintenance}"
JATO_JOB_ROOT="${JATO_JOB_ROOT:-/opt/jato/shared/04_Processed_data/ops/jato_monthly_update_jobs}"
PRODUCTION_LOCK_PATH="${PRODUCTION_LOCK_PATH:-}"
V2_CONTROLLER_PATH="${V2_CONTROLLER_PATH:-}"
V2_ARCHIVE_CACHE_ROOT="${V2_ARCHIVE_CACHE_ROOT:-}"

die() {
  printf '[ERROR] %s\n' "$1" >&2
  exit 1
}

require_identity() {
  [[ "${DEPLOY_COMMIT_SHA:-}" =~ ^[0-9a-f]{40}$ ]] \
    || die "DEPLOY_COMMIT_SHA must be one full lowercase Git SHA"
  [[ "${DEPLOY_ARCHIVE_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] \
    || die "DEPLOY_ARCHIVE_SHA256 must be one lowercase SHA-256"
}

require_production_lock() {
  [[ "$PRODUCTION_LOCK_PATH" == /*/production-deploy.lock ]] \
    || die "PRODUCTION_LOCK_PATH must be one absolute production-deploy.lock path"
}

require_archive_cache_root() {
  [[ "$V2_ARCHIVE_CACHE_ROOT" == /* ]] \
    || die "V2_ARCHIVE_CACHE_ROOT must be one absolute path"
  [[ "$V2_ARCHIVE_CACHE_ROOT" != *".."* ]] \
    || die "V2_ARCHIVE_CACHE_ROOT must not contain parent traversal"
}

controller_for_release() {
  local controller="$V2_CONTROLLER_PATH"
  [[ -n "$controller" ]] \
    || die "V2_CONTROLLER_PATH is required for fixed V2 control actions"
  [[ -f "$controller" && ! -L "$controller" ]] \
    || die "trusted fixed V2 controller is unavailable"
  printf '%s\n' "$controller"
}

release_root_for() {
  local pointer="$1"
  [[ -L "$pointer" ]] || die "release pointer is missing: $pointer"
  local target relative commit archive
  target="$(readlink -f -- "$pointer")"
  relative="${target#"$RELEASES_ROOT"/}"
  commit="${relative%%/*}"
  archive="${relative#*/}"
  [[ "$target" == "$RELEASES_ROOT/$commit/$archive" ]] \
    || die "release pointer escaped the V2 store: $pointer"
  [[ "$commit" =~ ^[0-9a-f]{40}$ && "$archive" =~ ^[0-9a-f]{64}$ ]] \
    || die "release pointer identity is malformed: $pointer"
  [[ -f "$target/release-v2-manifest.json" \
    && ! -L "$target/release-v2-manifest.json" ]] \
    || die "release pointer lacks a V2 manifest: $pointer"
  printf '%s\n' "$target"
}

verify_pointer_identity() {
  local root="$1"
  [[ "$root" == "$RELEASES_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256" ]] \
    || die "reviewed Candidate identity differs from the live pointer"
}

link_durable_path() {
  local root="$1"
  local relative="$2"
  local durable="$3"
  [[ -d "$durable" && ! -L "$durable" ]] \
    || die "durable runtime directory is missing or unsafe: $durable"
  rm -rf -- "$root/$relative"
  mkdir -p -- "$(dirname "$root/$relative")"
  ln -s -- "$durable" "$root/$relative"
}

build_candidate_runtime() {
  local root="$1"
  local deploy_uid deploy_gid unit
  deploy_uid="${SUDO_UID:-$(id -u)}"
  deploy_gid="${SUDO_GID:-$(id -g)}"
  unit="jato-v2-candidate-build-${DEPLOY_COMMIT_SHA:0:8}-$$"
  python3 -B - <<'PY'
from pathlib import Path

available_kib = None
for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
    if line.startswith("MemAvailable:"):
        available_kib = int(line.split()[1])
        break
if available_kib is None or available_kib < 3 * 1024 * 1024:
    raise SystemExit("[ERROR] less than 3 GiB memory is available for Candidate build")
PY
  rm -rf -- "$root/.venv"
  systemd-run \
    --quiet \
    --wait \
    --collect \
    --service-type=exec \
    --unit="$unit" \
    --uid="$deploy_uid" \
    --gid="$deploy_gid" \
    --working-directory="$root" \
    --property='MemoryHigh=3G' \
    --property='MemoryMax=4G' \
    --property='CPUQuota=100%' \
    --property='TasksMax=512' \
    --property='RuntimeMaxSec=1200' \
    /bin/bash -c '
      set -Eeuo pipefail
      root="$1"
      python3 -m venv --copies "$root/.venv"
      "$root/.venv/bin/python" -m pip install --upgrade pip \
        -i https://pypi.tuna.tsinghua.edu.cn/simple \
        --trusted-host pypi.tuna.tsinghua.edu.cn
      "$root/.venv/bin/pip" install \
        -r "$root/06_AppPlatform/backend/requirements.txt" \
        -i https://pypi.tuna.tsinghua.edu.cn/simple \
        --trusted-host pypi.tuna.tsinghua.edu.cn
      "$root/.venv/bin/pip" install "$root/07_ScrapingToolkit"
    ' _ "$root"
  [[ -x "$root/.venv/bin/python" ]] \
    || die "Candidate virtualenv build did not produce Python"
}

prepare_candidate() {
  require_identity
  [[ -n "${FRONTEND_ARTIFACT_IDENTITY:-}" ]] \
    || die "FRONTEND_ARTIFACT_IDENTITY is required"
  [[ "${FRONTEND_ARTIFACT_CHECKSUM:-}" =~ ^[0-9a-f]{64}$ ]] \
    || die "FRONTEND_ARTIFACT_CHECKSUM is invalid"
  [[ "${RELEASE_V2_MANIFEST_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] \
    || die "RELEASE_V2_MANIFEST_SHA256 is invalid"
  [[ -n "${RELEASE_V2_MANIFEST_B64:-}" ]] \
    || die "RELEASE_V2_MANIFEST_B64 is required"
  local root="${RELEASE_WORKTREE:-}"
  local frontend="${PREBUILT_FRONTEND_DIR:-}"
  [[ "$root" == "$RELEASES_ROOT" ]] \
    && die "release staging must be outside the immutable store"
  [[ "$root" == /opt/jato/staging/* && -d "$root" && ! -L "$root" ]] \
    || die "validated release worktree is unavailable"
  [[ "$(stat -c '%d' "$root")" == "$(stat -c '%d' "$RELEASES_ROOT")" ]] \
    || die "release staging and immutable store must share one filesystem"
  [[ -d "$frontend" && ! -L "$frontend" ]] \
    || die "verified frontend staging is unavailable"
  rm -rf -- "$root/06_AppPlatform/frontend/dist"
  mkdir -p -- "$root/06_AppPlatform/frontend"
  mv -- "$frontend" "$root/06_AppPlatform/frontend/dist"
  printf '%s' "$RELEASE_V2_MANIFEST_B64" \
    | base64 --decode > "$root/release-v2-manifest.json"
  chmod 0600 "$root/release-v2-manifest.json"
  [[ "$(sha256sum "$root/release-v2-manifest.json" | awk '{print $1}')" == \
    "$RELEASE_V2_MANIFEST_SHA256" ]] \
    || die "V2 release manifest SHA-256 mismatch"

  link_durable_path "$root" 01_RAW_DATA "$LEGACY_ROOT/01_RAW_DATA"
  link_durable_path "$root" 04_Processed_data "$LEGACY_ROOT/04_Processed_data"
  link_durable_path \
    "$root" 03_Scripts/diagnostics/artifacts \
    "$LEGACY_ROOT/03_Scripts/diagnostics/artifacts"
  link_durable_path "$root" 03_Scripts/logs "$LEGACY_ROOT/03_Scripts/logs"
  link_durable_path "$root" hermes/reports "$LEGACY_ROOT/hermes/reports"
  local seal_helper="$root/03_Scripts/deploy/verify_release_source_seal.py"
  local final_root="$RELEASES_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256"
  build_candidate_runtime "$root"

  chown -R root:root "$root"
  chmod -R a+rX,go-w "$root"
  chmod 0444 "$root/release-v2-manifest.json"
  python3 -B "$seal_helper" build \
    --root "$root" \
    --output "$root/.jato-source-seal.json"
  python3 -B "$seal_helper" build \
    --profile runtime \
    --root "$root" \
    --output "$root/.jato-runtime-seal.json" \
    --commit "$DEPLOY_COMMIT_SHA" \
    --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
    --frontend-identity "$FRONTEND_ARTIFACT_IDENTITY" \
    --frontend-checksum "$FRONTEND_ARTIFACT_CHECKSUM" \
    --recorded-runtime-root "$final_root"
  chmod 0444 "$root/.jato-source-seal.json" "$root/.jato-runtime-seal.json"
  python3 -B "$seal_helper" verify \
    --root "$root" \
    --manifest "$root/.jato-source-seal.json"
  python3 -B "$seal_helper" verify \
    --profile runtime \
    --root "$root" \
    --manifest "$root/.jato-runtime-seal.json" \
    --commit "$DEPLOY_COMMIT_SHA" \
    --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
    --frontend-identity "$FRONTEND_ARTIFACT_IDENTITY" \
    --frontend-checksum "$FRONTEND_ARTIFACT_CHECKSUM" \
    --recorded-runtime-root "$final_root"

  python3 -B "$root/03_Scripts/deploy/fixed_release_v2.py" \
    --release-root "$RELEASES_ROOT" \
    --slots-root "$SLOTS_ROOT" \
    --reports-root "$REPORTS_ROOT" \
    --archive-cache-root "$V2_ARCHIVE_CACHE_ROOT" \
    --production-lock "$PRODUCTION_LOCK_PATH" \
    prepare-candidate \
    --commit "$DEPLOY_COMMIT_SHA" \
    --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
    --manifest-sha256 "$RELEASE_V2_MANIFEST_SHA256" \
    --staging-root "$root"
}

verify_active_after_action() {
  [[ ! -e "$DEPLOYMENT_MARKER" && ! -L "$DEPLOYMENT_MARKER" ]] \
    || die "JATO deployment marker remained after the Active operation"
  [[ -f "$ACTIVE_SLOT_FILE" && ! -L "$ACTIVE_SLOT_FILE" ]] \
    || die "fixed Active ownership file is unavailable"
  [[ "$(tr -d '[:space:]' < "$ACTIVE_SLOT_FILE")" == 8000 ]] \
    || die "fixed Active ownership is not 8000"
  curl --noproxy '*' --fail --silent --show-error --max-time 20 \
    http://127.0.0.1:8000/healthz >/dev/null
  curl --noproxy '*' --fail --silent --show-error --max-time 20 \
    https://www.ojeur.cloud/healthz >/dev/null
}

run_active_action() {
  local controller="$1"
  shift
  python3 -B "$controller" \
    --release-root "$RELEASES_ROOT" \
    --slots-root "$SLOTS_ROOT" \
    --reports-root "$REPORTS_ROOT" \
    --archive-cache-root "$V2_ARCHIVE_CACHE_ROOT" \
    --production-lock "$PRODUCTION_LOCK_PATH" \
    "$@"
  verify_active_after_action
}

require_production_lock
require_archive_cache_root
case "$ACTION" in
  prepare-candidate)
    [[ "$(id -u)" -eq 0 ]] || die "prepare-candidate must run as root"
    prepare_candidate
    ;;
  discard-candidate)
    [[ "$(id -u)" -eq 0 ]] || die "discard-candidate must run as root"
    if [[ -L "$SLOTS_ROOT/8001/current" ]]; then
      root="$(release_root_for "$SLOTS_ROOT/8001/current")"
    else
      root="$LEGACY_ROOT"
    fi
    controller="$(controller_for_release "$root")"
    python3 -B "$controller" \
      --release-root "$RELEASES_ROOT" \
      --slots-root "$SLOTS_ROOT" \
      --reports-root "$REPORTS_ROOT" \
      --archive-cache-root "$V2_ARCHIVE_CACHE_ROOT" \
      --production-lock "$PRODUCTION_LOCK_PATH" \
      discard-candidate
    ;;
  update-active)
    [[ "$(id -u)" -eq 0 ]] || die "update-active must run as root"
    require_identity
    [[ "${RELEASE_V2_MANIFEST_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] \
      || die "RELEASE_V2_MANIFEST_SHA256 is invalid"
    root="$(release_root_for "$SLOTS_ROOT/8001/current")"
    verify_pointer_identity "$root"
    controller="$(controller_for_release "$root")"
    run_active_action \
      "$controller" \
      update-active \
      --commit "$DEPLOY_COMMIT_SHA" \
      --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
      --manifest-sha256 "$RELEASE_V2_MANIFEST_SHA256"
    ;;
  rollback-active)
    [[ "$(id -u)" -eq 0 ]] || die "rollback-active must run as root"
    require_identity
    [[ "${RELEASE_V2_MANIFEST_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] \
      || die "RELEASE_V2_MANIFEST_SHA256 is invalid"
    root="$RELEASES_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256"
    [[ -d "$root" && ! -L "$root" ]] \
      || die "reviewed rollback release is unavailable"
    controller="$(controller_for_release "$root")"
    run_active_action \
      "$controller" \
      rollback-active \
      --commit "$DEPLOY_COMMIT_SHA" \
      --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
      --manifest-sha256 "$RELEASE_V2_MANIFEST_SHA256"
    ;;
  *)
    die "action must be prepare-candidate, discard-candidate, update-active, or rollback-active"
    ;;
esac
