#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR="/opt/JATO_Analysis_System-main"
ENV_FILE="/etc/jato-fullstack/backend.env"
RELEASE_BACKUP_ROOT="${BACKUP_ROOT:-/opt/backups/jato}"
BLUEGREEN_STATE_ROOT="${BLUEGREEN_STATE_ROOT:-/var/lib/jato-release}"
BLUEGREEN_RELEASES_ROOT="${BLUEGREEN_RELEASES_ROOT:-/opt/jato/releases}"
ACTIVE_SLOT_FILE="${ACTIVE_SLOT_FILE:-$BLUEGREEN_STATE_ROOT/active-slot}"
DEPLOYMENT_MARKER="${DEPLOYMENT_MARKER:-$BLUEGREEN_STATE_ROOT/deployment-maintenance}"
SCHEDULER_STATE_FILE="${SCHEDULER_STATE_FILE:-$BLUEGREEN_STATE_ROOT/scheduler-state.tsv}"
BLUEGREEN_SWITCH_UNIT="jato-bluegreen-production.service"
LOCAL_NO_PROXY_HOSTS="localhost,127.0.0.1,::1"
export no_proxy="${no_proxy:+$no_proxy,}$LOCAL_NO_PROXY_HOSTS"
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}$LOCAL_NO_PROXY_HOSTS"

for required_name in \
  DEPLOY_COMMIT_SHA \
  DEPLOY_ARCHIVE_PATH \
  DEPLOY_ARCHIVE_BYTES \
  DEPLOY_ARCHIVE_SHA256 \
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

if [[ ! "$DEPLOY_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "[ERROR] DEPLOY_COMMIT_SHA must be a full lowercase git SHA"
  exit 1
fi
if [[ ! "$DEPLOY_ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "[ERROR] DEPLOY_ARCHIVE_SHA256 must be a lowercase SHA-256"
  exit 1
fi
if [[ ! "$DEPLOY_ARCHIVE_BYTES" =~ ^[1-9][0-9]*$ ]]; then
  echo "[ERROR] DEPLOY_ARCHIVE_BYTES must be a positive integer"
  exit 1
fi
if [[ ! "$DEPLOY_RUN_ID" =~ ^[1-9][0-9]*$ ]] || [[ ! "$DEPLOY_RUN_ATTEMPT" =~ ^[1-9][0-9]*$ ]]; then
  echo "[ERROR] DEPLOY_RUN_ID and DEPLOY_RUN_ATTEMPT must be positive integers"
  exit 1
fi

DEPLOY_REPOSITORY="${DEPLOY_REPOSITORY:-tristan419/JATO_Analysis_System}"
ARCHIVE_ROOT="$HOME/.cache/jato-releases/archives"
EXPECTED_ARCHIVE_RELATIVE=".cache/jato-releases/archives/${DEPLOY_COMMIT_SHA}/${DEPLOY_ARCHIVE_SHA256}.tar.gz"
EXPECTED_ARCHIVE_PATH="$HOME/$EXPECTED_ARCHIVE_RELATIVE"
case "$DEPLOY_ARCHIVE_PATH" in
  "$EXPECTED_ARCHIVE_RELATIVE"|"$EXPECTED_ARCHIVE_PATH") ;;
  *)
    echo "[ERROR] DEPLOY_ARCHIVE_PATH is not the expected content-addressed release path"
    exit 1
    ;;
esac
if [[ "$DEPLOY_ARCHIVE_PATH" == *".."* ]]; then
  echo "[ERROR] DEPLOY_ARCHIVE_PATH must not contain parent traversal"
  exit 1
fi

for required_command in flock realpath sha256sum stat tar python3 systemd-run; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "[ERROR] Missing required deployment command: $required_command"
    exit 1
  fi
done

RELEASE_ARCHIVE="$(realpath -m "$EXPECTED_ARCHIVE_PATH")"
ARCHIVE_ROOT_REAL="$(realpath -m "$ARCHIVE_ROOT")"
if [[ "$RELEASE_ARCHIVE" != "$ARCHIVE_ROOT_REAL/${DEPLOY_COMMIT_SHA}/${DEPLOY_ARCHIVE_SHA256}.tar.gz" ]]; then
  echo "[ERROR] Release archive realpath escaped the private content-addressed root"
  exit 1
fi
python3 - "$HOME" "$EXPECTED_ARCHIVE_PATH" <<'PY'
import pathlib
import sys

home = pathlib.Path(sys.argv[1]).expanduser()
archive = pathlib.Path(sys.argv[2]).expanduser()
relative = archive.relative_to(home)
for depth in range(1, len(relative.parts) + 1):
    candidate = home.joinpath(*relative.parts[:depth])
    if candidate.is_symlink():
        raise SystemExit(f"[ERROR] Release archive path must not contain symlinks: {candidate}")
PY

DEPLOY_STATE_DIR="${DEPLOY_STATE_DIR:-$HOME/.local/state/jato-production-release}"
if [[ "$DEPLOY_STATE_DIR" != /* ]] \
  || [[ -L "$DEPLOY_STATE_DIR" ]] \
  || [[ -e "$DEPLOY_STATE_DIR" && ! -d "$DEPLOY_STATE_DIR" ]]; then
  echo "[ERROR] DEPLOY_STATE_DIR must be an absolute, non-symlink directory"
  exit 1
fi
python3 -B - "$DEPLOY_STATE_DIR" <<'PY'
import os
from pathlib import Path
import stat
import sys

path = Path(sys.argv[1])
cursor = Path(path.anchor)
for part in path.parts[1:]:
    cursor /= part
    try:
        mode = os.lstat(cursor).st_mode
    except FileNotFoundError:
        continue
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise SystemExit(
            f"[ERROR] DEPLOY_STATE_DIR ancestor is unsafe: {cursor}"
        )
PY
mkdir -p "$DEPLOY_STATE_DIR/checkpoints/$DEPLOY_COMMIT_SHA" "$DEPLOY_STATE_DIR/journals/$DEPLOY_COMMIT_SHA"
chmod 700 "$DEPLOY_STATE_DIR" "$DEPLOY_STATE_DIR/checkpoints" "$DEPLOY_STATE_DIR/journals" \
  "$DEPLOY_STATE_DIR/checkpoints/$DEPLOY_COMMIT_SHA" "$DEPLOY_STATE_DIR/journals/$DEPLOY_COMMIT_SHA"
DEPLOY_LOCK_PATH="$DEPLOY_STATE_DIR/production-deploy.lock"
if [[ -L "$DEPLOY_LOCK_PATH" ]] \
  || [[ -e "$DEPLOY_LOCK_PATH" && ! -f "$DEPLOY_LOCK_PATH" ]]; then
  echo "[ERROR] Production deploy lock must be a regular non-symlink file"
  exit 1
fi
exec 9>"$DEPLOY_LOCK_PATH"
if ! flock -w 300 9; then
  echo "[ERROR] Another production deployment holds the global server-side lock"
  exit 1
fi
DEPLOY_LOCK_HELD=1
DEPLOY_LOCK_HOLDER_PID="$$"
DEPLOY_LOCK_FD=9

CHECKPOINT_FILE="$DEPLOY_STATE_DIR/checkpoints/$DEPLOY_COMMIT_SHA/${DEPLOY_ARCHIVE_SHA256}.json"
CHECKPOINT_JOURNAL="$DEPLOY_STATE_DIR/journals/$DEPLOY_COMMIT_SHA/${DEPLOY_ARCHIVE_SHA256}.jsonl"
RELEASE_WORKTREE="$(mktemp -d "/tmp/JATO_deploy_work_${DEPLOY_COMMIT_SHA}.XXXXXX")"
PREBUILT_FRONTEND_DIR="$REPO_DIR/.release-staging/frontend_${DEPLOY_COMMIT_SHA}_${DEPLOY_ARCHIVE_SHA256}.staged"
RELEASE_REPLACEMENT_PATHS="03_Scripts 06_AppPlatform 07_ScrapingToolkit hermes"

remove_transient_release_paths() {
  local transient_path=""

  for transient_path in "$RELEASE_WORKTREE" "$PREBUILT_FRONTEND_DIR"; do
    if [[ -z "$transient_path" || ! -e "$transient_path" ]]; then
      continue
    fi
    if ! rm -rf -- "$transient_path"; then
      echo "[WARN] Failed to remove transient deployment path: $transient_path" >&2
    fi
  done
  return 0
}

cleanup_release_staging() {
  remove_transient_release_paths
  # The verified content-addressed archive is intentionally retained.  A later
  # workflow checkpoint owns cleanup after www/intl parity reaches complete.
}
trap cleanup_release_staging EXIT

verify_release_archive_identity() {
  local archive_path="$1"
  local actual_bytes=""
  local actual_sha256=""

  if [[ ! -f "$archive_path" || -L "$archive_path" ]]; then
    echo "[ERROR] Uploaded production release archive is missing, non-regular, or a symlink: $archive_path"
    return 1
  fi
  actual_bytes="$(stat -c '%s' "$archive_path")"
  if [[ "$actual_bytes" != "$DEPLOY_ARCHIVE_BYTES" ]]; then
    echo "[ERROR] Release archive size mismatch: actual=$actual_bytes expected=$DEPLOY_ARCHIVE_BYTES"
    return 1
  fi
  actual_sha256="$(sha256sum "$archive_path" | awk '{print $1}')"
  if [[ "$actual_sha256" != "$DEPLOY_ARCHIVE_SHA256" ]]; then
    echo "[ERROR] Release archive SHA-256 mismatch"
    return 1
  fi
}

verify_release_archive_identity "$RELEASE_ARCHIVE"

if ! tar tzf "$RELEASE_ARCHIVE" >/dev/null 2>&1; then
  echo "[ERROR] Uploaded production release archive is incomplete or invalid"
  exit 1
fi
python3 - "$RELEASE_ARCHIVE" <<'PY_VALIDATE_RELEASE_ARCHIVE'
import pathlib
import sys
import tarfile

with tarfile.open(sys.argv[1], mode="r:gz") as archive:
    members = archive.getmembers()
    if not members:
        raise SystemExit("[ERROR] Production release archive is empty")
    seen: set[str] = set()
    root_directory_seen = False
    for member in members:
        if member.name in {".", "./"}:
            if not member.isdir() or root_directory_seen:
                raise SystemExit(
                    "[ERROR] Release archive root member must be one directory"
                )
            root_directory_seen = True
            continue
        name = pathlib.PurePosixPath(member.name)
        normalized = name.as_posix()
        if name.is_absolute() or ".." in name.parts or not name.parts:
            raise SystemExit(f"[ERROR] Unsafe release archive path: {member.name}")
        if normalized in seen:
            raise SystemExit(f"[ERROR] Duplicate release archive path: {member.name}")
        seen.add(normalized)
        if member.issym() or member.islnk() or member.isdev():
            raise SystemExit(f"[ERROR] Unsupported release archive member: {member.name}")
        if not (member.isfile() or member.isdir()):
            raise SystemExit(f"[ERROR] Unsupported release archive entry type: {member.name}")
print("[INFO] Release archive members passed fail-closed validation")
PY_VALIDATE_RELEASE_ARCHIVE

echo "[INFO] Extracting verified production release archive: $RELEASE_ARCHIVE"
tar xzf "$RELEASE_ARCHIVE" -C "$RELEASE_WORKTREE"

required_release_files=(
  hermes/deploy_release.json
  hermes/frontend_release/frontend-release.json
  hermes/frontend_release/frontend-dist.tar.gz
  01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx
  03_Scripts/deploy/frontend_release_artifact.py
  03_Scripts/deploy/release_checkpoint.py
  03_Scripts/deploy/release_evidence.py
  03_Scripts/deploy/prepare_backend_release.py
  03_Scripts/deploy/verify_backend_readiness.py
  03_Scripts/deploy/jato_quiescence_gate.py
  03_Scripts/deploy/jato_release_storage_guard.py
  03_Scripts/deploy/tencent_bluegreen_release.sh
  03_Scripts/deploy/verify_release_source_seal.py
  03_Scripts/deploy/lib/production_mutation_lock.sh
  03_Scripts/deploy/lib/release_paths.sh
  03_Scripts/deploy_fullstack_server.sh
  03_Scripts/ops/deploy_fullstack_server.sh
  03_Scripts/ops/backup_production_data.sh
  03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh
  03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
  03_Scripts/deploy/systemd/jato-country-news.env.example
  03_Scripts/deploy/systemd/jato-msrp.env.example
  03_Scripts/deploy/systemd/jato-voc.env.example
  03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example
  03_Scripts/deploy/systemd/jato-fullstack-backend@.service
  03_Scripts/deploy/systemd/jato-country-news-sync.service
  03_Scripts/deploy/systemd/jato-country-news-sync.timer
  03_Scripts/deploy/systemd/jato-country-news-sync-b.service
  03_Scripts/deploy/systemd/jato-country-news-sync-b.timer
  03_Scripts/deploy/systemd/jato-msrp-sync@.service
  03_Scripts/deploy/systemd/jato-msrp-dryrun.timer
  03_Scripts/deploy/systemd/jato-msrp-ingest.timer
  03_Scripts/deploy/systemd/jato-voc-forum-sync.service
  03_Scripts/deploy/systemd/jato-voc-forum-sync.timer
  03_Scripts/deploy/systemd/hermes-source-quality.service
  03_Scripts/deploy/systemd/hermes-source-quality.timer
  06_AppPlatform/backend/requirements.txt
  06_AppPlatform/backend/alembic.ini
  06_AppPlatform/backend/alembic/env.py
  06_AppPlatform/backend/app/main.py
  07_ScrapingToolkit/pyproject.toml
  03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_swiss_top30_suv/top30_suv_price_movement_candidates.json
  03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_swiss_top30_suv/official_evidence_leads.json
)
for release_file in "${required_release_files[@]}"; do
  if [[ ! -f "$RELEASE_WORKTREE/$release_file" ]]; then
    echo "[ERROR] Production release archive is missing required file: $release_file"
    exit 1
  fi
done

required_release_directories=(
  03_Scripts/deploy/systemd
  06_AppPlatform/backend/app
  06_AppPlatform/frontend
  07_ScrapingToolkit/jato_scraper
  hermes/frontend_release
)
for release_directory in "${required_release_directories[@]}"; do
  if [[ ! -d "$RELEASE_WORKTREE/$release_directory" ]]; then
    echo "[ERROR] Production release archive is missing required directory: $release_directory"
    exit 1
  fi
done

RELEASE_PATHS_LIB="$RELEASE_WORKTREE/03_Scripts/deploy/lib/release_paths.sh"
# shellcheck disable=SC1090
source "$RELEASE_PATHS_LIB"
MSRP_PROJECT_ROOT_OVERRIDE="${APP_PROJECT_ROOT:-}"
MSRP_EVIDENCE_ROOT_OVERRIDE="${MSRP_GOVERNANCE_EVIDENCE_ROOT:-}"
if sudo -n test -f "$ENV_FILE" 2>/dev/null; then
  if [[ -z "$MSRP_PROJECT_ROOT_OVERRIDE" ]]; then
    MSRP_PROJECT_ROOT_OVERRIDE="$(
      sudo -n bash -c 'set -a; . "$1"; set +a; printf "%s" "${APP_PROJECT_ROOT:-}"' _ "$ENV_FILE"
    )"
  fi
  if [[ -z "$MSRP_EVIDENCE_ROOT_OVERRIDE" ]]; then
    MSRP_EVIDENCE_ROOT_OVERRIDE="$(
      sudo -n bash -c 'set -a; . "$1"; set +a; printf "%s" "${MSRP_GOVERNANCE_EVIDENCE_ROOT:-}"' _ "$ENV_FILE"
    )"
  fi
fi
MSRP_EVIDENCE_ROOT="$(
  resolve_msrp_evidence_root \
    "${MSRP_PROJECT_ROOT_OVERRIDE:-$REPO_DIR}" \
    "$MSRP_EVIDENCE_ROOT_OVERRIDE"
)"
assert_path_outside_release_roots \
  "$REPO_DIR" \
  "$MSRP_EVIDENCE_ROOT" \
  $RELEASE_REPLACEMENT_PATHS
echo "[INFO] Durable MSRP evidence root is outside release replacement paths: $MSRP_EVIDENCE_ROOT"

ARCHIVE_COMMIT="$(python3 -c 'import json, sys; from pathlib import Path; payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")); print(payload.get("expectedCommitSha") or payload.get("commitSha") or "")' "$RELEASE_WORKTREE/hermes/deploy_release.json")"
if [[ "$ARCHIVE_COMMIT" != "$DEPLOY_COMMIT_SHA" ]]; then
  echo "[ERROR] Uploaded release commit mismatch: archive=${ARCHIVE_COMMIT:-missing} expected=$DEPLOY_COMMIT_SHA"
  exit 1
fi

FRONTEND_RELEASE_DIR="$RELEASE_WORKTREE/hermes/frontend_release"
FRONTEND_RELEASE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/frontend_release_artifact.py"
CHECKPOINT_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/release_checkpoint.py"
EVIDENCE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/release_evidence.py"
BACKEND_READINESS_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/verify_backend_readiness.py"
rm -rf "$PREBUILT_FRONTEND_DIR"
mkdir -p "$(dirname "$PREBUILT_FRONTEND_DIR")"
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
for frontend_file in index.html build-meta.json release-provenance.json; do
  if [[ ! -f "$PREBUILT_FRONTEND_DIR/$frontend_file" ]]; then
    echo "[ERROR] Verified frontend staging is missing required file: $frontend_file"
    exit 1
  fi
done

staging_device="$(stat -c '%d' "$PREBUILT_FRONTEND_DIR")"
production_device="$(stat -c '%d' "$REPO_DIR")"
if [[ "$staging_device" != "$production_device" ]]; then
  echo "[ERROR] Frontend staging and production directories must share a filesystem for atomic install"
  exit 1
fi

checkpoint_identity_args=(
  --repository "$DEPLOY_REPOSITORY"
  --commit "$DEPLOY_COMMIT_SHA"
  --archive-sha256 "$DEPLOY_ARCHIVE_SHA256"
  --archive-bytes "$DEPLOY_ARCHIVE_BYTES"
  --run-id "$DEPLOY_RUN_ID"
  --run-attempt "$DEPLOY_RUN_ATTEMPT"
  --frontend-identity "$FRONTEND_ARTIFACT_IDENTITY"
  --frontend-checksum "$FRONTEND_ARTIFACT_CHECKSUM"
)
CHECKPOINT_PHASE=""
CHECKPOINT_STATUS=""
CHECKPOINT_DECISION="new"
if [[ -e "$CHECKPOINT_FILE" ]]; then
  RESUME_JSON="$(python3 "$CHECKPOINT_HELPER" assert-resumable \
    --checkpoint "$CHECKPOINT_FILE" "${checkpoint_identity_args[@]}")"
  CHECKPOINT_DECISION="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["decision"])' <<< "$RESUME_JSON")"
  CHECKPOINT_STATE="$(python3 "$CHECKPOINT_HELPER" show --checkpoint "$CHECKPOINT_FILE")"
  read -r CHECKPOINT_PHASE CHECKPOINT_STATUS < <(
    python3 -c 'import json,sys; p=json.load(sys.stdin); print(p["phase"], p["status"])' <<< "$CHECKPOINT_STATE"
  )
fi

CROSS_RELEASE_STATE="$(python3 "$CHECKPOINT_HELPER" assert-cross-release-safe \
  --checkpoints-root "$DEPLOY_STATE_DIR/checkpoints" \
  --current-checkpoint "$CHECKPOINT_FILE" \
  "${checkpoint_identity_args[@]}")"
echo "[INFO] Cross-release production checkpoint gate passed: $CROSS_RELEASE_STATE"

release_evidence_matches() {
  local evidence_file="${CHECKPOINT_FILE%.json}.evidence.json"
  local verifier=(
    python3 -B "$EVIDENCE_HELPER" verify
    "$CHECKPOINT_FILE" "$evidence_file"
    --backup-root "$RELEASE_BACKUP_ROOT"
    "${checkpoint_identity_args[@]}"
  )

  if [[ "$(id -u)" -eq 0 ]]; then
    "${verifier[@]}" >/dev/null
  else
    sudo -n "${verifier[@]}" >/dev/null
  fi
}

verify_backend_readiness() {
  local timeout_seconds="${1:-10}"
  local backend_port="${2:-8000}"

  python3 -B "$BACKEND_READINESS_HELPER" \
    --url "http://127.0.0.1:${backend_port}/readyz" \
    --expected-commit "$DEPLOY_COMMIT_SHA" \
    --timeout-seconds "$timeout_seconds"
}

local_release_matches() {
  local active_port="8000"
  local active_root="$REPO_DIR"
  local expected_root="$BLUEGREEN_RELEASES_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256"
  if sudo -n test -f "$ACTIVE_SLOT_FILE" 2>/dev/null; then
    active_port="$(sudo -n cat "$ACTIVE_SLOT_FILE")"
  fi
  if sudo -n test -L /opt/jato/active 2>/dev/null; then
    active_root="$(sudo -n realpath /opt/jato/active)"
  fi
  [[ "$active_root" == "$expected_root" ]] \
    && verified_active_source_seal_matches "$active_root" \
    && verified_active_runtime_seal_matches "$active_root" \
    && [[ "$active_port" == "8000" || "$active_port" == "8001" ]] \
    && curl --noproxy '*' -fsS --max-time 10 \
      "http://127.0.0.1:${active_port}/healthz" >/dev/null 2>&1 \
    && verify_backend_readiness 10 "$active_port" \
    && grep -Fxq 'deploy_exit_code=0' \
      "$active_root/06_AppPlatform/frontend/dist/_deploy_status.txt" \
    && release_evidence_matches \
    && python3 - "$active_root/hermes/deploy_release.json" "$DEPLOY_COMMIT_SHA" \
      "$FRONTEND_ARTIFACT_IDENTITY" "$FRONTEND_ARTIFACT_CHECKSUM" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(1)
payload = json.loads(path.read_text(encoding="utf-8"))
frontend = payload.get("frontendRelease") or {}
artifact = frontend.get("artifact") or {}
commit = payload.get("actualCommitSha") or payload.get("commitSha") or ""
if commit != sys.argv[2] or artifact.get("id") != sys.argv[3] or artifact.get("checksum") != sys.argv[4]:
    raise SystemExit(1)
PY
}

verified_active_source_seal_matches() {
  local active_root="$1"
  local expected_seal=""
  local helper="$RELEASE_WORKTREE/03_Scripts/deploy/verify_release_source_seal.py"
  local stored_seal="$active_root/.jato-source-seal.json"
  expected_seal="$(mktemp)"
  if ! python3 -B "$helper" build \
    --root "$RELEASE_WORKTREE" \
    --output "$expected_seal" \
    || sudo -n test -L "$stored_seal" \
    || ! sudo -n test -f "$stored_seal" \
    || ! sudo -n cmp -s "$expected_seal" "$stored_seal" \
    || ! python3 -B "$helper" verify \
      --root "$active_root" \
      --manifest "$expected_seal"; then
    rm -f "$expected_seal"
    return 1
  fi
  rm -f "$expected_seal"
}

verified_active_runtime_seal_matches() {
  local active_root="$1"
  local helper="$RELEASE_WORKTREE/03_Scripts/deploy/verify_release_source_seal.py"
  local stored_seal="$active_root/.jato-runtime-seal.json"
  if sudo -n test -L "$stored_seal" \
    || ! sudo -n test -f "$stored_seal" \
    || ! python3 -B "$helper" verify \
      --profile runtime \
      --root "$active_root" \
      --manifest "$stored_seal" \
      --commit "$DEPLOY_COMMIT_SHA" \
      --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
      --frontend-identity "$FRONTEND_ARTIFACT_IDENTITY" \
      --frontend-checksum "$FRONTEND_ARTIFACT_CHECKSUM"; then
    return 1
  fi
}

bluegreen_reconciliation_pending() {
  local active_state=""
  local load_state=""
  if sudo -n test -e "$DEPLOYMENT_MARKER" \
    || sudo -n test -L "$DEPLOYMENT_MARKER" \
    || sudo -n test -e "$SCHEDULER_STATE_FILE" \
    || sudo -n test -L "$SCHEDULER_STATE_FILE"; then
    return 0
  fi
  if ! active_state="$(
    systemctl show "$BLUEGREEN_SWITCH_UNIT" -p ActiveState --value 2>/dev/null
  )" \
    || ! load_state="$(
      systemctl show "$BLUEGREEN_SWITCH_UNIT" -p LoadState --value 2>/dev/null
    )"; then
    return 0
  fi
  if [[ "$load_state" != "not-found" ]] \
    || [[ -n "$active_state" && "$active_state" != "inactive" ]]; then
    return 0
  fi
  return 1
}

bluegreen_local_noop_allowed() {
  local_release_matches && ! bluegreen_reconciliation_pending
}

if [[ "$CHECKPOINT_DECISION" == "already-complete" ]]; then
  if bluegreen_local_noop_allowed; then
    echo "[INFO] Exact completed release is already healthy; remote deploy is a no-op"
    exit 0
  fi
  if local_release_matches; then
    echo "[WARN] Exact completed release is healthy but durable blue/green reconciliation is pending"
  else
    echo "[ERROR] Complete checkpoint exists but local health/provenance does not match; refusing mutation"
    exit 1
  fi
fi
if [[ "$CHECKPOINT_DECISION" == "already-rolled-back" ]]; then
  if bluegreen_reconciliation_pending; then
    echo "[WARN] Rolled-back release still has durable blue/green state to reconcile"
  else
    echo "[ERROR] This exact release was already rolled back; create a new reviewed release instead of replaying it"
    exit 1
  fi
fi
if [[ "$CHECKPOINT_PHASE" == "backend_healthy" && "$CHECKPOINT_STATUS" == "completed" ]] \
  && bluegreen_local_noop_allowed; then
  echo "[INFO] Exact server release is already healthy; remote deploy is a no-op"
  exit 0
fi
if [[ "$CHECKPOINT_PHASE" == "backend_healthy" && "$CHECKPOINT_STATUS" == "completed" ]] \
  && local_release_matches; then
  echo "[WARN] Exact server release is healthy but durable blue/green reconciliation is pending"
fi

if [[ -z "$CHECKPOINT_PHASE" || "$CHECKPOINT_PHASE" == "prepared" ]]; then
  python3 "$CHECKPOINT_HELPER" write \
    --checkpoint "$CHECKPOINT_FILE" \
    --journal "$CHECKPOINT_JOURNAL" \
    "${checkpoint_identity_args[@]}" \
    --phase prepared \
    --status completed \
    --retry-class automatic \
    --message "archive, provenance, required assets, and frontend staging verified"
  CHECKPOINT_PHASE="prepared"
  CHECKPOINT_STATUS="completed"
fi

echo "[INFO] Release archive and materialized frontend passed all pre-mutation checks"

# Production releases use the independent 8000/8001 Tencent blue/green
# controller.  It owns all source materialization, candidate verification,
# JATO quiescence, Nginx switching, rollback, and final slot promotion.  This
# outer verifier never mutates the live source tree or publishes target health.
export \
  RELEASE_WORKTREE PREBUILT_FRONTEND_DIR \
  CHECKPOINT_FILE CHECKPOINT_JOURNAL \
  DEPLOY_STATE_DIR DEPLOY_LOCK_PATH DEPLOY_LOCK_HELD \
  DEPLOY_LOCK_HOLDER_PID DEPLOY_LOCK_FD \
  BLUEGREEN_STATE_ROOT ACTIVE_SLOT_FILE DEPLOYMENT_MARKER SCHEDULER_STATE_FILE \
  DEPLOY_REPOSITORY DEPLOY_COMMIT_SHA DEPLOY_ARCHIVE_SHA256 \
  DEPLOY_ARCHIVE_BYTES DEPLOY_RUN_ID DEPLOY_RUN_ATTEMPT DEPLOY_BRANCH \
  FRONTEND_ARTIFACT_IDENTITY FRONTEND_ARTIFACT_CHECKSUM \
  DEPLOY_SERVER_NAME="${DEPLOY_SERVER_NAME:-_}"
set +e
bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh" \
  prepare-and-switch
BLUEGREEN_RC=$?
set -e
exit "$BLUEGREEN_RC"
