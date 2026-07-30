#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR="/opt/JATO_Analysis_System-main"
ENV_FILE="/etc/jato-fullstack/backend.env"
RELEASE_BACKUP_ROOT="${BACKUP_ROOT:-/opt/backups/jato}"
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

for required_command in flock realpath sha256sum stat tar python3; do
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
mkdir -p "$DEPLOY_STATE_DIR/checkpoints/$DEPLOY_COMMIT_SHA" "$DEPLOY_STATE_DIR/journals/$DEPLOY_COMMIT_SHA"
chmod 700 "$DEPLOY_STATE_DIR" "$DEPLOY_STATE_DIR/checkpoints" "$DEPLOY_STATE_DIR/journals" \
  "$DEPLOY_STATE_DIR/checkpoints/$DEPLOY_COMMIT_SHA" "$DEPLOY_STATE_DIR/journals/$DEPLOY_COMMIT_SHA"
DEPLOY_LOCK_PATH="$DEPLOY_STATE_DIR/production-deploy.lock"
exec 9>"$DEPLOY_LOCK_PATH"
if ! flock -w 300 9; then
  echo "[ERROR] Another production deployment holds the global server-side lock"
  exit 1
fi

CHECKPOINT_FILE="$DEPLOY_STATE_DIR/checkpoints/$DEPLOY_COMMIT_SHA/${DEPLOY_ARCHIVE_SHA256}.json"
CHECKPOINT_JOURNAL="$DEPLOY_STATE_DIR/journals/$DEPLOY_COMMIT_SHA/${DEPLOY_ARCHIVE_SHA256}.jsonl"
PREVIOUS_RELEASE_METADATA_PATH="${CHECKPOINT_FILE%.json}.previous-release.json"
RELEASE_WORKTREE="$(mktemp -d "/tmp/JATO_deploy_work_${DEPLOY_COMMIT_SHA}.XXXXXX")"
DEPLOY_SOURCE="github_actions_resumable_ssh_archive"
PRODUCTION_RELEASE_WORKFLOW="true"
PREBUILT_FRONTEND_DIR="$REPO_DIR/.release-staging/frontend_${DEPLOY_COMMIT_SHA}_${DEPLOY_ARCHIVE_SHA256}.staged"
PREVIOUS_DEPLOY_RELEASE_FILE=""
RUNTIME_PRESERVE_DIR=""
PRODUCTION_MUTATION_STARTED="false"
REMOTE_DEPLOY_SUCCEEDED="false"
RUNTIME_PRESERVE_PATHS="
03_Scripts/diagnostics/artifacts
03_Scripts/logs
06_AppPlatform/frontend/dist
hermes/reports
"
RELEASE_REPLACEMENT_PATHS="03_Scripts 06_AppPlatform 07_ScrapingToolkit hermes"

remove_transient_release_paths() {
  local transient_path=""

  for transient_path in "$RELEASE_WORKTREE" "$RUNTIME_PRESERVE_DIR" "$PREBUILT_FRONTEND_DIR"; do
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
  if [[ "$REMOTE_DEPLOY_SUCCEEDED" == "true" ]]; then
    remove_transient_release_paths
    return
  fi
  if [[ "$PRODUCTION_MUTATION_STARTED" == "false" ]]; then
    remove_transient_release_paths
  else
    for recovery_path in "$RELEASE_WORKTREE" "$RUNTIME_PRESERVE_DIR" "$PREBUILT_FRONTEND_DIR"; do
      if [[ -n "$recovery_path" && -e "$recovery_path" ]]; then
        echo "[WARN] Retained deployment recovery path: $recovery_path" >&2
      fi
    done
  fi
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
  03_Scripts/deploy/lib/release_paths.sh
  03_Scripts/deploy_fullstack_server.sh
  03_Scripts/ops/deploy_fullstack_server.sh
  03_Scripts/ops/backup_production_data.sh
  03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh
  03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
  03_Scripts/deploy/systemd/jato-country-news.env.example
  03_Scripts/deploy/systemd/jato-msrp.env.example
  03_Scripts/deploy/systemd/jato-voc.env.example
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
BACKEND_RELEASE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/prepare_backend_release.py"
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

  python3 -B "$BACKEND_READINESS_HELPER" \
    --url "http://127.0.0.1:8000/readyz" \
    --expected-commit "$DEPLOY_COMMIT_SHA" \
    --timeout-seconds "$timeout_seconds"
}

local_release_matches() {
  curl --noproxy '*' -fsS --max-time 10 http://127.0.0.1:8000/healthz >/dev/null 2>&1 \
    && verify_backend_readiness 10 \
    && grep -Fxq 'deploy_exit_code=0' "$REPO_DIR/06_AppPlatform/frontend/dist/_deploy_status.txt" \
    && release_evidence_matches \
    && python3 - "$REPO_DIR/hermes/deploy_release.json" "$DEPLOY_COMMIT_SHA" \
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

if [[ "$CHECKPOINT_DECISION" == "already-complete" ]]; then
  if local_release_matches; then
    echo "[INFO] Exact completed release is already healthy; remote deploy is a no-op"
    REMOTE_DEPLOY_SUCCEEDED="true"
    exit 0
  fi
  echo "[ERROR] Complete checkpoint exists but local health/provenance does not match; refusing mutation"
  exit 1
fi
if [[ "$CHECKPOINT_PHASE" == "backend_healthy" && "$CHECKPOINT_STATUS" == "completed" ]] \
  && local_release_matches; then
  echo "[INFO] Exact server release is already healthy; remote deploy is a no-op"
  REMOTE_DEPLOY_SUCCEEDED="true"
  exit 0
fi

remote_checkpoint_phase_rank() {
  case "$1" in
    packaged) echo 0 ;;
    transport_verified) echo 1 ;;
    prepared) echo 2 ;;
    source_install_started) echo 3 ;;
    source_installed) echo 4 ;;
    backup_verified) echo 5 ;;
    migration_started) echo 6 ;;
    migrated) echo 7 ;;
    switch_started) echo 8 ;;
    switched) echo 9 ;;
    backend_healthy) echo 10 ;;
    www_verified) echo 11 ;;
    intl_deploy_started) echo 12 ;;
    intl_verified) echo 13 ;;
    parity_verified) echo 14 ;;
    complete) echo 15 ;;
    *) echo -1 ;;
  esac
}

remote_checkpoint_at_least() {
  [[ "$(remote_checkpoint_phase_rank "$CHECKPOINT_PHASE")" -ge "$(remote_checkpoint_phase_rank "$1")" ]]
}

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

if [[ -e "$PREVIOUS_RELEASE_METADATA_PATH" ]]; then
  if [[ ! -f "$PREVIOUS_RELEASE_METADATA_PATH" || -L "$PREVIOUS_RELEASE_METADATA_PATH" ]]; then
    echo "[ERROR] Durable previous release metadata is unsafe"
    exit 1
  fi
  PREVIOUS_DEPLOY_RELEASE_FILE="$PREVIOUS_RELEASE_METADATA_PATH"
fi

if remote_checkpoint_at_least source_installed; then
  echo "[INFO] Exact release source is already installed; skipping live tree replacement"
else
  RUNTIME_PRESERVE_DIR="$(mktemp -d /tmp/JATO_deploy_runtime.XXXXXX)"
  if [[ -z "$PREVIOUS_DEPLOY_RELEASE_FILE" && -e "$REPO_DIR/hermes/deploy_release.json" ]]; then
    if [[ ! -f "$REPO_DIR/hermes/deploy_release.json" || -L "$REPO_DIR/hermes/deploy_release.json" ]]; then
      echo "[ERROR] Existing deploy release metadata is unsafe"
      exit 1
    fi
    previous_metadata_temp="${PREVIOUS_RELEASE_METADATA_PATH}.tmp.$$"
    cp "$REPO_DIR/hermes/deploy_release.json" "$previous_metadata_temp"
    chmod 600 "$previous_metadata_temp"
    mv -f "$previous_metadata_temp" "$PREVIOUS_RELEASE_METADATA_PATH"
    PREVIOUS_DEPLOY_RELEASE_FILE="$PREVIOUS_RELEASE_METADATA_PATH"
    echo "[INFO] Preserved previous actual release identity"
  fi
  for runtime_path in $RUNTIME_PRESERVE_PATHS; do
    if [[ -e "$REPO_DIR/$runtime_path" ]]; then
      mkdir -p "$RUNTIME_PRESERVE_DIR/$(dirname "$runtime_path")"
      cp -a "$REPO_DIR/$runtime_path" "$RUNTIME_PRESERVE_DIR/$runtime_path"
      echo "[INFO] Preserved runtime path: $runtime_path"
    fi
  done

  # Production mutation boundary: every archive, path, helper, provenance,
  # workbook/evidence, and materialized frontend check above passed first.
  python3 "$CHECKPOINT_HELPER" write \
    --checkpoint "$CHECKPOINT_FILE" --journal "$CHECKPOINT_JOURNAL" \
    "${checkpoint_identity_args[@]}" \
    --phase source_install_started --status in_progress --retry-class rollback_required \
    --message "live source installation started; interruption requires rollback inspection"
  CHECKPOINT_PHASE="source_install_started"
  CHECKPOINT_STATUS="in_progress"
  PRODUCTION_MUTATION_STARTED="true"
  sudo mkdir -p "$REPO_DIR"
  sudo chown -R "$USER":"$USER" "$REPO_DIR"
  for release_path in 03_Scripts 06_AppPlatform 07_ScrapingToolkit hermes; do
    if [[ -e "$RELEASE_WORKTREE/$release_path" ]]; then
      rm -rf "$REPO_DIR/$release_path"
      (cd "$RELEASE_WORKTREE" && tar cf - "$release_path") | (cd "$REPO_DIR" && tar xf -)
    fi
  done
  mkdir -p "$REPO_DIR/01_RAW_DATA"
  cp -f "$RELEASE_WORKTREE/01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx" \
    "$REPO_DIR/01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx"
  for release_file in .nvmrc requirements.txt CODEX.md; do
    if [[ -f "$RELEASE_WORKTREE/$release_file" ]]; then
      cp -f "$RELEASE_WORKTREE/$release_file" "$REPO_DIR/$release_file"
    fi
  done
  for runtime_path in $RUNTIME_PRESERVE_PATHS; do
    if [[ -e "$RUNTIME_PRESERVE_DIR/$runtime_path" ]]; then
      if [[ -d "$RUNTIME_PRESERVE_DIR/$runtime_path" && -d "$REPO_DIR/$runtime_path" ]]; then
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
  for public_parent in "$REPO_DIR" "$REPO_DIR/06_AppPlatform" "$REPO_DIR/06_AppPlatform/frontend"; do
    if [[ ! -d "$public_parent" ]]; then
      echo "[ERROR] Frontend parent directory is missing after source installation: $public_parent"
      exit 1
    fi
    chmod a+x "$public_parent"
  done
  python3 "$REPO_DIR/03_Scripts/deploy/release_checkpoint.py" write \
    --checkpoint "$CHECKPOINT_FILE" --journal "$CHECKPOINT_JOURNAL" \
    "${checkpoint_identity_args[@]}" \
    --phase source_installed --status completed --retry-class automatic \
    --message "live source tree and preserved runtime paths installed"
  CHECKPOINT_PHASE="source_installed"
  CHECKPOINT_STATUS="completed"
fi

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

export \
  REPO_DIR DEPLOY_SOURCE PREBUILT_FRONTEND_DIR PRODUCTION_RELEASE_WORKFLOW \
  DEPLOY_REPOSITORY DEPLOY_ARCHIVE_PATH DEPLOY_ARCHIVE_BYTES DEPLOY_ARCHIVE_SHA256 \
  CHECKPOINT_FILE CHECKPOINT_JOURNAL
python3 - <<'PY'
import datetime as _dt
import json
import os
import pathlib
import tempfile

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
    "expectedShortSha": os.environ.get("DEPLOY_SHORT_SHA") or commit_sha[:8],
    "actualCommitSha": "",
    "actualShortSha": "",
    "commitSha": "",
    "shortSha": "",
    "branch": os.environ.get("DEPLOY_BRANCH", "main"),
    "repository": os.environ.get("DEPLOY_REPOSITORY", "tristan419/JATO_Analysis_System"),
    "workflow": os.environ.get("DEPLOY_WORKFLOW", "production-release"),
    "workflowRunId": os.environ.get("DEPLOY_RUN_ID", ""),
    "workflowRunAttempt": os.environ.get("DEPLOY_RUN_ATTEMPT", ""),
    "deployMethod": "github_actions",
    "packagedAt": _dt.datetime.now(_dt.UTC).isoformat(),
    "source": os.environ.get("DEPLOY_SOURCE", "github_actions_archive"),
    "releaseTransport": {
        "kind": "resumable-ssh",
        "archivePath": os.environ.get("DEPLOY_ARCHIVE_PATH", ""),
        "archiveBytes": int(os.environ.get("DEPLOY_ARCHIVE_BYTES", "0")),
        "archiveSha256": os.environ.get("DEPLOY_ARCHIVE_SHA256", ""),
    },
})
if not isinstance(payload.get("frontendRelease"), dict):
    raise SystemExit("[ERROR] deploy_release.json is missing frontendRelease provenance")
out.parent.mkdir(parents=True, exist_ok=True)
descriptor, temporary_name = tempfile.mkstemp(
    prefix=f".{out.name}.",
    suffix=".tmp",
    dir=out.parent,
)
try:
    os.fchmod(descriptor, out.stat().st_mode & 0o777 if out.exists() else 0o644)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_name, out)
finally:
    try:
        os.unlink(temporary_name)
    except FileNotFoundError:
        pass
print(
    f"[INFO] Prepared {out.relative_to(root)} "
    f"for expected release {payload['expectedShortSha']}"
)
PY

install_backend_env_atomically() {
  local candidate=""
  local privileged_candidate=""

  if ! sudo -n test -f "$ENV_FILE"; then
    echo "[ERROR] Backend env file is required before managed settings can be updated: $ENV_FILE"
    return 1
  fi
  if [[ -n "${GOOGLE_CLIENT_ID:-}" && -z "${GOOGLE_CLIENT_SECRET:-}" ]]; then
    echo "[ERROR] GOOGLE_CLIENT_SECRET is required when GOOGLE_CLIENT_ID is configured"
    return 1
  fi

  GOOGLE_OAUTH_PROXY_URL="${GOOGLE_OAUTH_PROXY_URL:-http://127.0.0.1:7897}"
  export GOOGLE_OAUTH_PROXY_URL
  candidate="$(mktemp /tmp/jato-backend-env.XXXXXX)"
  privileged_candidate="${ENV_FILE}.candidate.${DEPLOY_RUN_ID}.${DEPLOY_RUN_ATTEMPT}"
  sudo -n cat "$ENV_FILE" > "$candidate"
  python3 - "$candidate" <<'PY'
import os
from pathlib import Path
import re
import shlex
import sys

path = Path(sys.argv[1])
deepseek = bool(os.environ.get("DEEPSEEK_API_KEY"))
google = bool(os.environ.get("GOOGLE_CLIENT_ID"))
managed = set()
if deepseek:
    managed.update({
        "DEEPSEEK_API_KEY", "HERMES_SYNC_TOKEN",
        "APP_COUNTRY_CHAT_DEEPSEEK_TIMEOUT_SECONDS",
        "APP_COUNTRY_CHAT_MODEL_OPTIONS",
    })
if google:
    managed.update({
        "APP_GOOGLE_CLIENT_ID", "APP_GOOGLE_CLIENT_SECRET",
        "APP_GOOGLE_REDIRECT_URI", "APP_FRONTEND_ORIGIN",
        "APP_FRONTEND_ORIGINS", "APP_CORS_ORIGINS",
        "APP_GOOGLE_OAUTH_PROXY_URL", "APP_GOOGLE_OAUTH_RELAY_URL",
        "APP_GOOGLE_OAUTH_RELAY_TOKEN", "APP_GOOGLE_OAUTH_TIMEOUT_SECONDS",
        "APP_GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED",
        "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED",
        "APP_GROUPED_TIME_SERIES_PREWARM_GROUP_BY",
        "APP_GROUPED_TIME_SERIES_PREWARM_GRAINS",
        "APP_GROUPED_TIME_SERIES_PREWARM_SCOPES",
        "APP_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON",
        "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED",
        "APP_ADVANCED_ANALYSIS_WARMUP_COUNTRIES",
        "APP_ADVANCED_ANALYSIS_WARMUP_SCOPES",
        "APP_ADVANCED_ANALYSIS_WARMUP_SALES_MODES",
        "APP_ADVANCED_ANALYSIS_WARMUP_TOP_N",
        "APP_ADVANCED_ANALYSIS_WARMUP_PROFILE_OPTIONS",
        "APP_ADVANCED_ANALYSIS_WARMUP_COMPETITOR_SET",
    })
lines = []
for line in path.read_text(encoding="utf-8").splitlines():
    match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line)
    if not match or match.group(1) not in managed:
        lines.append(line)

def add(key: str, value: str) -> None:
    lines.append(f"{key}={shlex.quote(value)}")

if deepseek:
    add("DEEPSEEK_API_KEY", os.environ["DEEPSEEK_API_KEY"])
    add("HERMES_SYNC_TOKEN", os.environ.get("HERMES_SYNC_TOKEN", ""))
    add("APP_COUNTRY_CHAT_DEEPSEEK_TIMEOUT_SECONDS", "60")
    add("APP_COUNTRY_CHAT_MODEL_OPTIONS", "deepseek:deepseek-chat")
if google:
    values = {
        "APP_GOOGLE_CLIENT_ID": os.environ["GOOGLE_CLIENT_ID"],
        "APP_GOOGLE_CLIENT_SECRET": os.environ["GOOGLE_CLIENT_SECRET"],
        "APP_GOOGLE_REDIRECT_URI": "https://www.ojeur.cloud/v1/auth/google/callback",
        "APP_FRONTEND_ORIGIN": "https://www.ojeur.cloud",
        "APP_FRONTEND_ORIGINS": "https://www.ojeur.cloud,https://intl.ojeur.cloud",
        "APP_CORS_ORIGINS": "https://www.ojeur.cloud,https://intl.ojeur.cloud,http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000",
        "APP_GOOGLE_OAUTH_PROXY_URL": os.environ["GOOGLE_OAUTH_PROXY_URL"],
        "APP_GOOGLE_OAUTH_TIMEOUT_SECONDS": "30",
        "APP_GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED": "true",
        "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED": "true",
        "APP_GROUPED_TIME_SERIES_PREWARM_GROUP_BY": "动总规整,国家",
        "APP_GROUPED_TIME_SERIES_PREWARM_GRAINS": "month,year",
        "APP_GROUPED_TIME_SERIES_PREWARM_SCOPES": "viewer,order_filler,editor,admin",
        "APP_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON": '[{"国家":["丹麦","克罗地亚","匈牙利","奥地利","希腊","德国","意大利","挪威","捷克","斯洛伐克","斯洛文尼亚","比利时","法国","波兰","瑞典","瑞士","罗马尼亚","芬兰","荷兰","葡萄牙","西班牙"],"动总规整":["ICE","HEV","BEV","MHEV","PHEV"]}]',
        "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED": "true",
        "APP_ADVANCED_ANALYSIS_WARMUP_COUNTRIES": "瑞典",
        "APP_ADVANCED_ANALYSIS_WARMUP_SCOPES": "viewer,order_filler,editor,admin",
        "APP_ADVANCED_ANALYSIS_WARMUP_SALES_MODES": "month",
        "APP_ADVANCED_ANALYSIS_WARMUP_TOP_N": "15",
        "APP_ADVANCED_ANALYSIS_WARMUP_PROFILE_OPTIONS": "true",
        "APP_ADVANCED_ANALYSIS_WARMUP_COMPETITOR_SET": "false",
    }
    if os.environ.get("GOOGLE_OAUTH_RELAY_URL"):
        values["APP_GOOGLE_OAUTH_RELAY_URL"] = os.environ["GOOGLE_OAUTH_RELAY_URL"]
    if os.environ.get("GOOGLE_OAUTH_RELAY_TOKEN"):
        values["APP_GOOGLE_OAUTH_RELAY_TOKEN"] = os.environ["GOOGLE_OAUTH_RELAY_TOKEN"]
    for key, value in values.items():
        add(key, value)
path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
PY
  python3 -B "$BACKEND_RELEASE_HELPER" update-env \
    --path "$candidate" \
    --commit "$DEPLOY_COMMIT_SHA"
  bash -n "$candidate"
  sudo -n install -D -m 600 "$candidate" "$privileged_candidate"
  sudo -n bash -n "$privileged_candidate"
  sudo -n mv -f "$privileged_candidate" "$ENV_FILE"
  rm -f "$candidate"
  echo "[INFO] Backend env validated and atomically installed"
}

install_backend_env_atomically

if [[ -n "${GOOGLE_CLIENT_ID:-}" ]]; then
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
export \
  REPO_DIR SKIP_GIT_SYNC=true \
  PREVIOUS_DEPLOY_RELEASE_FILE \
  BACKEND_SERVICE_NAME="jato-fullstack-backend@8000" \
  RELEASE_CHECKPOINT_FILE="$CHECKPOINT_FILE" \
  RELEASE_CHECKPOINT_JOURNAL="$CHECKPOINT_JOURNAL" \
  RELEASE_CHECKPOINT_REPOSITORY="$DEPLOY_REPOSITORY" \
  RELEASE_CHECKPOINT_COMMIT="$DEPLOY_COMMIT_SHA" \
  RELEASE_CHECKPOINT_ARCHIVE_SHA256="$DEPLOY_ARCHIVE_SHA256" \
  RELEASE_CHECKPOINT_ARCHIVE_BYTES="$DEPLOY_ARCHIVE_BYTES" \
  RELEASE_CHECKPOINT_RUN_ID="$DEPLOY_RUN_ID" \
  RELEASE_CHECKPOINT_RUN_ATTEMPT="$DEPLOY_RUN_ATTEMPT" \
  RELEASE_CHECKPOINT_FRONTEND_IDENTITY="$FRONTEND_ARTIFACT_IDENTITY" \
  RELEASE_CHECKPOINT_FRONTEND_CHECKSUM="$FRONTEND_ARTIFACT_CHECKSUM"
set +e
bash 03_Scripts/deploy_fullstack_server.sh 2>&1
DEPLOY_RC=$?
set -e

FRONTEND_ROOT="$REPO_DIR/06_AppPlatform/frontend/dist"
OUTER_EVIDENCE_FILE="${CHECKPOINT_FILE%.json}.evidence.json"
OUTER_EVIDENCE_SHA256=""
if [[ "$DEPLOY_RC" -eq 0 ]]; then
  if [[ ! -f "$OUTER_EVIDENCE_FILE" || -L "$OUTER_EVIDENCE_FILE" ]]; then
    echo "[ERROR] Durable release evidence is missing before outer verification"
    DEPLOY_RC=1
  elif ! release_evidence_matches; then
    echo "[ERROR] Privileged durable release evidence verification failed"
    DEPLOY_RC=1
  else
    OUTER_EVIDENCE_SHA256="$(sha256sum "$OUTER_EVIDENCE_FILE" | awk '{print $1}')"
  fi
fi
OUTER_EVIDENCE_BINDING="evidence_path=$OUTER_EVIDENCE_FILE evidence_sha256=$OUTER_EVIDENCE_SHA256"
NGINX_RC=0

if [[ "$DEPLOY_RC" -eq 0 && -n "${DEPLOY_SERVER_NAME:-}" && "$DEPLOY_SERVER_NAME" != "_" ]]; then
  set +e
  if [ "${DEPLOY_ENABLE_HTTPS,,}" = "true" ]; then
    sudo SERVER_NAME="$DEPLOY_SERVER_NAME" BACKEND_PORT=8000 FRONTEND_ROOT="$FRONTEND_ROOT" \
      CERTBOT_EMAIL="$DEPLOY_CERTBOT_EMAIL" CERTBOT_RENEW_DRY_RUN=false \
      bash 03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh
  else
    sudo SERVER_NAME="$DEPLOY_SERVER_NAME" BACKEND_PORT=8000 FRONTEND_ROOT="$FRONTEND_ROOT" \
      bash 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
  fi
  NGINX_RC=$?
  set -e
  if [[ "$NGINX_RC" -eq 0 ]]; then
    NGINX_CONF="$(sudo -n grep -l 'proxy_pass http://jato_fullstack_api' /etc/nginx/sites-enabled/* /etc/nginx/conf.d/* 2>/dev/null | sed -n '1p' || true)"
    if [[ -n "$NGINX_CONF" ]] && sudo -n test -f "$NGINX_CONF"; then
      set +e
      sudo -n sed -i 's/proxy_buffering on;/proxy_buffering off;/g' "$NGINX_CONF" \
        && sudo -n nginx -t \
        && sudo -n systemctl reload nginx
      NGINX_RC=$?
      set -e
      if [[ "$NGINX_RC" -eq 0 ]]; then
        echo "[INFO] nginx proxy_buffering disabled for SSE streaming"
      fi
    fi
  fi
fi

FINAL_HEALTH_RC=1
FINAL_READINESS_RC=1
if [[ "$DEPLOY_RC" -eq 0 && "$NGINX_RC" -eq 0 ]]; then
  if curl --noproxy '*' -fsS --max-time 20 \
    http://127.0.0.1:8000/healthz >/dev/null 2>&1; then
    FINAL_HEALTH_RC=0
  fi
  if verify_backend_readiness 20; then
    FINAL_READINESS_RC=0
  fi
fi
FINAL_RC="$DEPLOY_RC"
if [[ "$FINAL_RC" -eq 0 && "$NGINX_RC" -ne 0 ]]; then
  FINAL_RC="$NGINX_RC"
elif [[ "$FINAL_RC" -eq 0 && "$FINAL_HEALTH_RC" -ne 0 ]]; then
  FINAL_RC="$FINAL_HEALTH_RC"
elif [[ "$FINAL_RC" -eq 0 && "$FINAL_READINESS_RC" -ne 0 ]]; then
  FINAL_RC="$FINAL_READINESS_RC"
fi

DIST="$REPO_DIR/06_AppPlatform/frontend/dist"
mkdir -p "$DIST"
STATUS_TEMP="$(mktemp "$DIST/.deploy_status.XXXXXX.tmp")"
{
  echo "deploy_exit_code=$FINAL_RC"
  echo "inner_deploy_exit_code=$DEPLOY_RC"
  echo "nginx_exit_code=$NGINX_RC"
  echo "final_health_exit_code=$FINAL_HEALTH_RC"
  echo "final_readiness_exit_code=$FINAL_READINESS_RC"
  echo "timestamp=$(date -u)"
  echo "---systemctl---"
  sudo -n systemctl status jato-fullstack-backend@8000 --no-pager 2>&1 || true
  echo "---healthz---"
  curl --noproxy '*' -fsS http://127.0.0.1:8000/healthz 2>&1 || echo "HEALTHZ_FAILED"
  echo "---readyz---"
  verify_backend_readiness 20 2>&1 || echo "READYZ_FAILED"
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
  echo "---release checkpoint---"
  python3 "$REPO_DIR/03_Scripts/deploy/release_checkpoint.py" show \
    --checkpoint "$CHECKPOINT_FILE" 2>&1 || echo "RELEASE_CHECKPOINT_MISSING_OR_INVALID"
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
} > "$STATUS_TEMP" 2>&1
if ! mv -f "$STATUS_TEMP" "$DIST/_deploy_status.txt"; then
  if [[ "$DEPLOY_RC" -eq 0 ]]; then
    python3 "$REPO_DIR/03_Scripts/deploy/release_checkpoint.py" write \
      --checkpoint "$CHECKPOINT_FILE" --journal "$CHECKPOINT_JOURNAL" \
      "${checkpoint_identity_args[@]}" \
      --phase backend_healthy --status failed --retry-class automatic \
      --message "atomic deploy status publication failed; $OUTER_EVIDENCE_BINDING" >/dev/null || true
  fi
  exit 1
fi

if [[ "$FINAL_RC" -ne 0 ]]; then
  if [[ "$DEPLOY_RC" -eq 0 ]]; then
    python3 "$REPO_DIR/03_Scripts/deploy/release_checkpoint.py" write \
      --checkpoint "$CHECKPOINT_FILE" --journal "$CHECKPOINT_JOURNAL" \
      "${checkpoint_identity_args[@]}" \
      --phase backend_healthy --status failed --retry-class automatic \
      --message "outer release verification failed: nginx_rc=$NGINX_RC health_rc=$FINAL_HEALTH_RC readiness_rc=$FINAL_READINESS_RC; $OUTER_EVIDENCE_BINDING" >/dev/null || true
  fi
  exit "$FINAL_RC"
fi
python3 "$REPO_DIR/03_Scripts/deploy/release_checkpoint.py" write \
  --checkpoint "$CHECKPOINT_FILE" --journal "$CHECKPOINT_JOURNAL" \
  "${checkpoint_identity_args[@]}" \
  --phase backend_healthy --status completed --retry-class automatic \
  --message "nginx, final local liveness, release readiness, and atomic deploy status completed; $OUTER_EVIDENCE_BINDING" >/dev/null
REMOTE_DEPLOY_SUCCEEDED="true"
