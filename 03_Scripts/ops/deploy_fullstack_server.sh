#!/usr/bin/env bash
set -Eeuo pipefail

DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-jato-fullstack-backend@8000}"
BACKEND_PORT="${BACKEND_PORT:-}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
RUN_DATABASE_MIGRATIONS="${RUN_DATABASE_MIGRATIONS:-auto}"
PRODUCTION_RELEASE_WORKFLOW="${PRODUCTION_RELEASE_WORKFLOW:-false}"
RUN_PRE_DEPLOY_BACKUP="${RUN_PRE_DEPLOY_BACKUP:-auto}"
RUN_GROUPED_TIME_SERIES_PREWARM="${RUN_GROUPED_TIME_SERIES_PREWARM:-auto}"
BLUEGREEN_PREPARE_ONLY="${BLUEGREEN_PREPARE_ONLY:-false}"
BLUEGREEN_POST_ACTIVATION_ONLY="${BLUEGREEN_POST_ACTIVATION_ONLY:-false}"
BLUEGREEN_GLOBAL_RECONCILE_ONLY="${BLUEGREEN_GLOBAL_RECONCILE_ONLY:-false}"
REMOTE_NAME="${REMOTE_NAME:-}"
REPO_REMOTE_URL="${REPO_REMOTE_URL:-git@github.com:tristan419/JATO_Analysis_System.git}"
SKIP_GIT_SYNC="${SKIP_GIT_SYNC:-false}"
DEPLOY_PRUNE_UNTRACKED="${DEPLOY_PRUNE_UNTRACKED:-true}"
DEPLOY_UNTRACKED_CLEAN_PATTERNS="${DEPLOY_UNTRACKED_CLEAN_PATTERNS:-04_Processed_data/.refresh_backups/pre-sync-* Markdown_Readme/Fullstack/*.md Markdown_Readme/Streamlit/*.md}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIAGNOSTIC_SCRIPT="$SCRIPT_DIR/print_fullstack_server_diagnostics.sh"
BACKUP_SCRIPT="$SCRIPT_DIR/backup_production_data.sh"
RELEASE_BACKUP_ROOT="${BACKUP_ROOT:-/opt/backups/jato}"
CURRENT_STEP="initialization"

# ── China-friendly mirror defaults ──
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
PREBUILT_FRONTEND_DIR="${PREBUILT_FRONTEND_DIR:-}"
BACKEND_REQUIREMENTS="$BACKEND_DIR/requirements.txt"
VENV_DIR="$REPO_DIR/.venv"
TOOLKIT_DIR="$REPO_DIR/07_ScrapingToolkit"
TOOLKIT_EGG_INFO_HELPER="$REPO_DIR/03_Scripts/deploy/cleanup_toolkit_egg_info.py"
DEPLOY_RELEASE_FILE="$REPO_DIR/hermes/deploy_release.json"
PREVIOUS_DEPLOY_RELEASE_FILE="${PREVIOUS_DEPLOY_RELEASE_FILE:-}"
DEPLOY_FAILURE_FILE="${DEPLOY_FAILURE_FILE:-$REPO_DIR/hermes/deploy_failure_context.txt}"
SYSTEMD_SOURCE_DIR="$REPO_DIR/03_Scripts/deploy/systemd"
SYSTEMD_TARGET_DIR="/etc/systemd/system"
SYSTEMD_RUNTIME_ROOT="${SYSTEMD_RUNTIME_ROOT:-$REPO_DIR}"
JATO_ETC_DIR="/etc/jato-fullstack"
ENABLE_SCRAPER_SCHEDULERS="${ENABLE_SCRAPER_SCHEDULERS:-true}"
RECONCILE_SCRAPER_TIMER_STATE="${RECONCILE_SCRAPER_TIMER_STATE:-true}"
BOOTSTRAP_MSRP_DRYRUN_IF_MISSING="${BOOTSTRAP_MSRP_DRYRUN_IF_MISSING:-true}"
RELEASE_CHECKPOINT_FILE="${RELEASE_CHECKPOINT_FILE:-}"
RELEASE_CHECKPOINT_JOURNAL="${RELEASE_CHECKPOINT_JOURNAL:-}"
RELEASE_CHECKPOINT_REPOSITORY="${RELEASE_CHECKPOINT_REPOSITORY:-}"
RELEASE_CHECKPOINT_COMMIT="${RELEASE_CHECKPOINT_COMMIT:-${DEPLOY_COMMIT_SHA:-}}"
RELEASE_CHECKPOINT_ARCHIVE_SHA256="${RELEASE_CHECKPOINT_ARCHIVE_SHA256:-}"
RELEASE_CHECKPOINT_ARCHIVE_BYTES="${RELEASE_CHECKPOINT_ARCHIVE_BYTES:-}"
RELEASE_CHECKPOINT_RUN_ID="${RELEASE_CHECKPOINT_RUN_ID:-}"
RELEASE_CHECKPOINT_RUN_ATTEMPT="${RELEASE_CHECKPOINT_RUN_ATTEMPT:-}"
RELEASE_CHECKPOINT_FRONTEND_IDENTITY="${RELEASE_CHECKPOINT_FRONTEND_IDENTITY:-}"
RELEASE_CHECKPOINT_FRONTEND_CHECKSUM="${RELEASE_CHECKPOINT_FRONTEND_CHECKSUM:-}"
CHECKPOINT_HELPER="$REPO_DIR/03_Scripts/deploy/release_checkpoint.py"
RELEASE_EVIDENCE_HELPER="${RELEASE_EVIDENCE_HELPER:-$REPO_DIR/03_Scripts/deploy/release_evidence.py}"
BACKEND_RELEASE_HELPER="${BACKEND_RELEASE_HELPER:-$REPO_DIR/03_Scripts/deploy/prepare_backend_release.py}"
BACKEND_READINESS_HELPER="${BACKEND_READINESS_HELPER:-$REPO_DIR/03_Scripts/deploy/verify_backend_readiness.py}"
TARGET_BACKEND_COMMIT=""
CURRENT_CHECKPOINT_PHASE=""
CURRENT_CHECKPOINT_STATUS=""
CHECKPOINT_WRITING_FAILURE="false"
CHECKPOINT_ALREADY_COMPLETE="false"
RELEASE_EVIDENCE_FILE="${RELEASE_CHECKPOINT_FILE%.json}.evidence.json"
LAST_BACKUP_MANIFEST_PATH=""
LAST_BACKUP_MANIFEST_BYTES="0"
LAST_BACKUP_MANIFEST_SHA256=""
MIGRATION_PRE_REVISION=""
MIGRATION_TARGET_REVISION=""
MIGRATION_RESULT_REVISION=""
DATABASE_ENABLED="false"
DATABASE_BACKUP_REQUIRED="false"
DATABASE_MIGRATION_REQUIRED="false"
DATABASE_MIGRATION_VERIFY_ONLY="false"
DATABASE_READ_ONLY_GATE_FAILED="false"
RELEASE_EVIDENCE_SHA256=""

checkpoint_enabled() {
  [[ -n "$RELEASE_CHECKPOINT_FILE" ]]
}

checkpoint_phase_rank() {
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
    rollback_started) echo 10 ;;
    rollback_completed) echo 11 ;;
    pre_switch_aborted) echo 12 ;;
    backend_healthy) echo 13 ;;
    www_verified) echo 14 ;;
    intl_deploy_started) echo 15 ;;
    intl_verified) echo 16 ;;
    parity_verified) echo 17 ;;
    complete) echo 18 ;;
    *) echo -1 ;;
  esac
}

checkpoint_at_least() {
  local wanted="$1"
  [[ "$(checkpoint_phase_rank "$CURRENT_CHECKPOINT_PHASE")" -ge "$(checkpoint_phase_rank "$wanted")" ]]
}

checkpoint_completed_or_past() {
  local wanted="$1"
  local current_rank="$(checkpoint_phase_rank "$CURRENT_CHECKPOINT_PHASE")"
  local wanted_rank="$(checkpoint_phase_rank "$wanted")"
  [[ "$current_rank" -gt "$wanted_rank" ]] \
    || [[ "$current_rank" -eq "$wanted_rank" && "$CURRENT_CHECKPOINT_STATUS" == "completed" ]]
}

checkpoint_identity_args() {
  printf '%s\0' \
    --repository "$RELEASE_CHECKPOINT_REPOSITORY" \
    --commit "$RELEASE_CHECKPOINT_COMMIT" \
    --archive-sha256 "$RELEASE_CHECKPOINT_ARCHIVE_SHA256" \
    --archive-bytes "$RELEASE_CHECKPOINT_ARCHIVE_BYTES" \
    --run-id "$RELEASE_CHECKPOINT_RUN_ID" \
    --run-attempt "$RELEASE_CHECKPOINT_RUN_ATTEMPT" \
    --frontend-identity "$RELEASE_CHECKPOINT_FRONTEND_IDENTITY" \
    --frontend-checksum "$RELEASE_CHECKPOINT_FRONTEND_CHECKSUM"
}

write_release_checkpoint() {
  local phase="$1"
  local status="$2"
  local retry_class="$3"
  local message="$4"
  local identity_args=()

  if ! checkpoint_enabled; then
    return 0
  fi
  if [[ -f "$RELEASE_EVIDENCE_FILE" && "$message" != *"evidence_sha256="* ]]; then
    RELEASE_EVIDENCE_SHA256="$(sha256sum "$RELEASE_EVIDENCE_FILE" | awk '{print $1}')"
    message="$message; evidence_path=$RELEASE_EVIDENCE_FILE evidence_sha256=$RELEASE_EVIDENCE_SHA256"
  fi
  mapfile -d '' -t identity_args < <(checkpoint_identity_args)
  python3 "$CHECKPOINT_HELPER" write \
    --checkpoint "$RELEASE_CHECKPOINT_FILE" \
    --journal "$RELEASE_CHECKPOINT_JOURNAL" \
    "${identity_args[@]}" \
    --phase "$phase" \
    --status "$status" \
    --retry-class "$retry_class" \
    --message "$message" >/dev/null
  CURRENT_CHECKPOINT_PHASE="$phase"
  CURRENT_CHECKPOINT_STATUS="$status"
}

write_release_evidence() {
  local migration_status="$1"

  if ! checkpoint_enabled; then
    return 0
  fi
  RELEASE_EVIDENCE_SHA256="$(python3 - "$RELEASE_EVIDENCE_FILE" \
    "$LAST_BACKUP_MANIFEST_PATH" "$LAST_BACKUP_MANIFEST_BYTES" "$LAST_BACKUP_MANIFEST_SHA256" \
    "$MIGRATION_PRE_REVISION" "$MIGRATION_TARGET_REVISION" "$MIGRATION_RESULT_REVISION" \
    "$migration_status" "$RELEASE_CHECKPOINT_REPOSITORY" "$RELEASE_CHECKPOINT_COMMIT" \
    "$RELEASE_CHECKPOINT_ARCHIVE_SHA256" "$RELEASE_CHECKPOINT_ARCHIVE_BYTES" \
    "$RELEASE_CHECKPOINT_RUN_ID" "$RELEASE_CHECKPOINT_RUN_ATTEMPT" \
    "$RELEASE_CHECKPOINT_FRONTEND_IDENTITY" "$RELEASE_CHECKPOINT_FRONTEND_CHECKSUM" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

(
    evidence_path,
    backup_path,
    backup_bytes,
    backup_sha256,
    pre_revision,
    target_revision,
    result_revision,
    migration_status,
    repository,
    commit,
    archive_sha256,
    archive_bytes,
    run_id,
    run_attempt,
    frontend_identity,
    frontend_checksum,
) = sys.argv[1:]
payload = {
    "identity": {
        "repository": repository,
        "commit": commit,
        "archiveSha256": archive_sha256,
        "archiveBytes": int(archive_bytes),
        "runId": int(run_id),
        "runAttempt": int(run_attempt),
        "frontendIdentity": frontend_identity,
        "frontendChecksum": frontend_checksum,
    },
    "backup": {
        "manifestPath": backup_path or None,
        "manifestBytes": int(backup_bytes),
        "manifestSha256": backup_sha256 or None,
    },
    "migration": {
        "status": migration_status,
        "preRevision": pre_revision or None,
        "targetRevision": target_revision or None,
        "resultRevision": result_revision or None,
    },
}
path = Path(evidence_path)
path.parent.mkdir(parents=True, exist_ok=True)
fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_name, path)
    os.chmod(path, 0o600)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(path.parent, directory_flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
finally:
    try:
        os.close(fd)
    except OSError:
        pass
    try:
        Path(temporary_name).unlink()
    except FileNotFoundError:
        pass
digest = hashlib.sha256(path.read_bytes()).hexdigest()
print(digest)
PY
)"
}

verify_release_evidence() {
  local identity_args=()
  mapfile -d '' -t identity_args < <(checkpoint_identity_args)
  if [[ "$(id -u)" -eq 0 ]]; then
    python3 -B "$RELEASE_EVIDENCE_HELPER" verify \
      "$RELEASE_CHECKPOINT_FILE" "$RELEASE_EVIDENCE_FILE" \
      --backup-root "$RELEASE_BACKUP_ROOT" "${identity_args[@]}"
  else
    sudo -n python3 -B "$RELEASE_EVIDENCE_HELPER" verify \
      "$RELEASE_CHECKPOINT_FILE" "$RELEASE_EVIDENCE_FILE" \
      --backup-root "$RELEASE_BACKUP_ROOT" "${identity_args[@]}"
  fi
}

verify_live_migration_revision_if_available() {
  local evidence_status=""
  local evidence_result=""
  local live_result=""

  if ! checkpoint_at_least migrated || [[ ! -f "$RELEASE_EVIDENCE_FILE" ]]; then
    return 0
  fi
  evidence_status="$(python3 -c 'import json,sys; print((json.load(open(sys.argv[1], encoding="utf-8")).get("migration") or {}).get("status") or "")' "$RELEASE_EVIDENCE_FILE")"
  if [[ "$evidence_status" == "not_required" ]]; then
    if [[ "${DATABASE_ENABLED:-false}" == "true" ]]; then
      echo "[ERROR] Live database is enabled but release evidence says migration was not required"
      return 1
    fi
    return 0
  fi
  if [[ "$evidence_status" != "completed" ]]; then
    echo "[ERROR] Migrated checkpoint lacks completed migration evidence"
    return 1
  fi
  if [[ "${DATABASE_ENABLED:-false}" != "true" ]]; then
    echo "[WARN] Database is not safely readable; relying on bound migration evidence"
    return 0
  fi
  evidence_result="$(python3 -c 'import json,sys; print((json.load(open(sys.argv[1], encoding="utf-8")).get("migration") or {}).get("resultRevision") or "")' "$RELEASE_EVIDENCE_FILE")"
  live_result="$(read_database_current_revision)"
  assert_alembic_revision_sets_equal \
    "$evidence_result" "evidence" "$live_result" "live" \
    "Live Alembic revision does not match bound migration evidence"
}

initialize_release_checkpoint() {
  local state=""
  local resume_state=""
  local identity_args=()
  local evidence_fields=()
  local required_value=""

  if ! checkpoint_enabled; then
    if [[ "$PRODUCTION_RELEASE_WORKFLOW" == "true" ]]; then
      echo "[ERROR] Production release requires a persistent checkpoint path"
      return 1
    fi
    return 0
  fi
  for required_value in \
    "$RELEASE_CHECKPOINT_FILE" "$RELEASE_CHECKPOINT_JOURNAL" \
    "$RELEASE_CHECKPOINT_REPOSITORY" "$RELEASE_CHECKPOINT_COMMIT" \
    "$RELEASE_CHECKPOINT_ARCHIVE_SHA256" "$RELEASE_CHECKPOINT_ARCHIVE_BYTES" \
    "$RELEASE_CHECKPOINT_RUN_ID" "$RELEASE_CHECKPOINT_RUN_ATTEMPT" \
    "$RELEASE_CHECKPOINT_FRONTEND_IDENTITY" "$RELEASE_CHECKPOINT_FRONTEND_CHECKSUM"
  do
    if [[ -z "$required_value" ]]; then
      echo "[ERROR] Production release checkpoint identity is incomplete"
      return 1
    fi
  done
  if [[ ! -f "$CHECKPOINT_HELPER" || -L "$CHECKPOINT_HELPER" ]]; then
    echo "[ERROR] Release checkpoint helper is missing or unsafe: $CHECKPOINT_HELPER"
    return 1
  fi
  if [[ ! -f "$RELEASE_EVIDENCE_HELPER" || -L "$RELEASE_EVIDENCE_HELPER" ]]; then
    echo "[ERROR] Release evidence helper is missing or unsafe"
    return 1
  fi
  mapfile -d '' -t identity_args < <(checkpoint_identity_args)
  resume_state="$(python3 "$CHECKPOINT_HELPER" assert-resumable \
    --checkpoint "$RELEASE_CHECKPOINT_FILE" "${identity_args[@]}")"
  local resume_decision=""
  resume_decision="$(
    python3 -c 'import json,sys; print(json.load(sys.stdin)["decision"])' \
      <<< "$resume_state"
  )"
  if [[ "$resume_decision" == "already-pre-switch-aborted" ]]; then
    echo "[ERROR] This exact release was abandoned before Candidate start; refusing replay"
    return 1
  fi
  if [[ "$resume_decision" == "already-complete" ]]; then
    CHECKPOINT_ALREADY_COMPLETE="true"
  fi
  state="$(python3 "$CHECKPOINT_HELPER" show --checkpoint "$RELEASE_CHECKPOINT_FILE")"
  read -r CURRENT_CHECKPOINT_PHASE CURRENT_CHECKPOINT_STATUS < <(
    python3 -c 'import json,sys; p=json.load(sys.stdin); print(p["phase"], p["status"])' <<< "$state"
  )
  if checkpoint_completed_or_past backup_verified; then
    verify_release_evidence
  fi
  if [[ -f "$RELEASE_EVIDENCE_FILE" ]]; then
    mapfile -d '' -t evidence_fields < <(python3 - "$RELEASE_EVIDENCE_FILE" <<'PY'
import json
from pathlib import Path
import sys

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
backup = payload.get("backup") or {}
migration = payload.get("migration") or {}
for value in (
    backup.get("manifestPath") or "",
    str(backup.get("manifestBytes") or 0),
    backup.get("manifestSha256") or "",
    migration.get("preRevision") or "",
    migration.get("targetRevision") or "",
    migration.get("resultRevision") or "",
):
    sys.stdout.write(str(value) + "\0")
PY
    )
    LAST_BACKUP_MANIFEST_PATH="${evidence_fields[0]:-}"
    LAST_BACKUP_MANIFEST_BYTES="${evidence_fields[1]:-0}"
    LAST_BACKUP_MANIFEST_SHA256="${evidence_fields[2]:-}"
    MIGRATION_PRE_REVISION="${evidence_fields[3]:-}"
    MIGRATION_TARGET_REVISION="${evidence_fields[4]:-}"
    MIGRATION_RESULT_REVISION="${evidence_fields[5]:-}"
  fi
}

completed_checkpoint_matches_local() {
  verify_release_evidence \
    && curl --noproxy '*' -fsS --max-time 10 "http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null 2>&1 \
    && verify_backend_readiness 10 \
    && python3 - "$DEPLOY_RELEASE_FILE" "$RELEASE_CHECKPOINT_COMMIT" \
      "$RELEASE_CHECKPOINT_FRONTEND_IDENTITY" "$RELEASE_CHECKPOINT_FRONTEND_CHECKSUM" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
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

target_backend_commit() {
  local git_commit=""

  if [[ -n "${TARGET_BACKEND_COMMIT:-}" ]]; then
    printf '%s\n' "$TARGET_BACKEND_COMMIT"
    return 0
  fi
  if [[ -n "${DEPLOY_COMMIT_SHA:-}" ]]; then
    printf '%s\n' "$DEPLOY_COMMIT_SHA"
    return 0
  fi
  if [[ -n "$RELEASE_CHECKPOINT_COMMIT" ]]; then
    printf '%s\n' "$RELEASE_CHECKPOINT_COMMIT"
    return 0
  fi
  if git_commit="$(git -C "$REPO_DIR" rev-parse --verify HEAD 2>/dev/null)" \
    && [[ -n "$git_commit" ]]; then
    printf '%s\n' "$git_commit"
    return 0
  fi
  return 1
}

git_repository_available() {
  git -C "$REPO_DIR" rev-parse --git-dir >/dev/null 2>&1
}

resolve_target_backend_commit() {
  local candidate="${DEPLOY_COMMIT_SHA:-}"
  local git_commit=""

  if checkpoint_enabled && [[ -z "$candidate" ]]; then
    fail_deploy \
      "Checkpointed production release requires explicit DEPLOY_COMMIT_SHA" \
      "$LINENO"
  fi
  if git_repository_available; then
    git_commit="$(git -C "$REPO_DIR" rev-parse --verify HEAD 2>/dev/null || true)"
  fi
  if [[ -z "$candidate" ]]; then
    candidate="$git_commit"
  fi
  if [[ ! "$candidate" =~ ^[0-9a-f]{40}$ ]]; then
    fail_deploy \
      "Target backend release requires a full lowercase DEPLOY_COMMIT_SHA or Git HEAD" \
      "$LINENO"
  fi
  if ! checkpoint_enabled \
    && [[ -n "$git_commit" ]] \
    && [[ "$git_commit" != "$candidate" ]]; then
    fail_deploy \
      "Target backend commit does not match the checked-out Git HEAD" \
      "$LINENO"
  fi
  if checkpoint_enabled \
    && [[ -n "$RELEASE_CHECKPOINT_COMMIT" ]] \
    && [[ "$RELEASE_CHECKPOINT_COMMIT" != "$candidate" ]]; then
    fail_deploy \
      "Target backend commit does not match the production release checkpoint" \
      "$LINENO"
  fi

  TARGET_BACKEND_COMMIT="$candidate"
  DEPLOY_COMMIT_SHA="$candidate"
  export DEPLOY_COMMIT_SHA
  echo "[INFO] Target backend release commit: $TARGET_BACKEND_COMMIT"
}

install_target_release_sha_atomically() {
  local candidate=""
  local privileged_candidate=""

  if [[ ! -f "$BACKEND_RELEASE_HELPER" ]]; then
    echo "[ERROR] Backend release preparation helper is missing: $BACKEND_RELEASE_HELPER"
    return 1
  fi
  if ! sudo -n test -f "$BACKEND_ENV_FILE"; then
    echo "[ERROR] Backend env file is required to freeze the target release SHA"
    return 1
  fi

  candidate="$(mktemp /tmp/jato-backend-release-env.XXXXXX)"
  privileged_candidate="${BACKEND_ENV_FILE}.candidate.release.$$"
  if ! {
    sudo -n cat "$BACKEND_ENV_FILE" > "$candidate" \
      && python3 -B "$BACKEND_RELEASE_HELPER" update-env \
        --path "$candidate" \
        --commit "$TARGET_BACKEND_COMMIT" \
      && bash -n "$candidate" \
      && sudo -n install -D -m 600 "$candidate" "$privileged_candidate" \
      && sudo -n bash -n "$privileged_candidate" \
      && sudo -n mv -f "$privileged_candidate" "$BACKEND_ENV_FILE"
  }; then
    rm -f "$candidate"
    sudo -n rm -f "$privileged_candidate" 2>/dev/null || true
    return 1
  fi
  rm -f "$candidate"
  echo "[INFO] Backend process release SHA prepared atomically"
}

prepare_target_release_metadata() {
  local arguments=(
    prepare-metadata
    --path "$DEPLOY_RELEASE_FILE"
    --commit "$TARGET_BACKEND_COMMIT"
    --branch "$DEPLOY_BRANCH"
    --source "${DEPLOY_SOURCE:-direct_server_deploy}"
  )

  if checkpoint_enabled; then
    arguments+=(--require-existing)
  fi
  if [[ -n "$PREVIOUS_DEPLOY_RELEASE_FILE" ]]; then
    if [[ ! -f "$PREVIOUS_DEPLOY_RELEASE_FILE" || -L "$PREVIOUS_DEPLOY_RELEASE_FILE" ]]; then
      echo "[ERROR] Previous deploy release metadata is missing or unsafe"
      return 1
    fi
    arguments+=(--previous-metadata "$PREVIOUS_DEPLOY_RELEASE_FILE")
  fi
  python3 -B "$BACKEND_RELEASE_HELPER" "${arguments[@]}"
}

prepare_target_backend_identity() {
  if [[ "$BLUEGREEN_PREPARE_ONLY" == "true" ]]; then
    echo "[INFO] Blue/green candidate SHA is isolated in its slot env; common backend.env remains unchanged"
  else
    install_target_release_sha_atomically
  fi
  prepare_target_release_metadata
}

verify_backend_readiness() {
  local timeout_seconds="${1:-10}"
  local expected_commit=""

  if [[ ! -f "$BACKEND_READINESS_HELPER" ]]; then
    echo '{"check":"backend_readyz","ok":false,"error":{"code":"helper_missing","message":"backend readiness helper is missing"}}' >&2
    return 1
  fi
  if ! expected_commit="$(target_backend_commit)" || [[ -z "$expected_commit" ]]; then
    echo '{"check":"backend_readyz","ok":false,"error":{"code":"target_commit_missing","message":"target backend commit cannot be resolved"}}' >&2
    return 1
  fi
  python3 -B "$BACKEND_READINESS_HELPER" \
    --url "http://127.0.0.1:${BACKEND_PORT}/readyz" \
    --expected-commit "$expected_commit" \
    --timeout-seconds "$timeout_seconds"
}

record_failure_checkpoint() {
  local message="$1"
  local phase="${CURRENT_CHECKPOINT_PHASE:-prepared}"
  local retry_class="inspect_then_resume"

  if ! checkpoint_enabled || [[ "$CHECKPOINT_WRITING_FAILURE" == "true" ]]; then
    return 0
  fi
  if [[ "${DATABASE_READ_ONLY_GATE_FAILED:-false}" == "true" ]]; then
    echo "[INFO] Preserving the resumable backup checkpoint because the database gate was read-only"
    return 0
  fi
  CHECKPOINT_WRITING_FAILURE="true"
  case "$phase" in
    migration_started) retry_class="manual_db_recovery" ;;
    switch_started|switched) retry_class="rollback_required" ;;
    backend_healthy) retry_class="automatic" ;;
    prepared|backup_verified|migrated) retry_class="inspect_then_resume" ;;
    *) retry_class="inspect_then_resume" ;;
  esac
  write_release_checkpoint "$phase" failed "$retry_class" "$message" || true
  CHECKPOINT_WRITING_FAILURE="false"
}

log_section() {
  printf '\n[STEP] %s\n' "$1"
}

is_truthy() {
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_database_migration_policy() {
  local database_state="disabled"
  local requested_mode=""

  requested_mode="$(
    printf '%s' "$RUN_DATABASE_MIGRATIONS" | tr '[:upper:]' '[:lower:]'
  )"

  DATABASE_ENABLED="false"
  DATABASE_BACKUP_REQUIRED="false"
  DATABASE_MIGRATION_REQUIRED="false"
  DATABASE_MIGRATION_VERIFY_ONLY="false"

  case "$requested_mode" in
    auto|true|run|verify_only|false|skip) ;;
    *)
      fail_deploy \
        "Unsupported database migration policy: $RUN_DATABASE_MIGRATIONS" \
        "$LINENO"
      ;;
  esac

  if [[ -f "$BACKEND_ENV_FILE" ]]; then
    if ! database_state="$(run_privileged_bash 'set -Eeuo pipefail; set -a; . "$1"; set +a; enabled="$(printf "%s" "${APP_DATABASE_ENABLED:-false}" | tr "[:upper:]" "[:lower:]")"; case "$enabled" in 1|true|yes|on) [[ -n "${APP_DATABASE_URL:-${DATABASE_URL:-}}" ]] || exit 2; echo enabled ;; *) echo disabled ;; esac' "$BACKEND_ENV_FILE" 2>/dev/null)"; then
      fail_deploy "Cannot resolve database migration policy from backend env" "$LINENO"
    fi
  elif checkpoint_enabled; then
    fail_deploy \
      "Cannot resolve database migration policy because backend env is missing" \
      "$LINENO"
  fi

  if [[ "$database_state" == "enabled" ]]; then
    DATABASE_ENABLED="true"
  fi

  case "$requested_mode" in
    auto)
      if [[ "$DATABASE_ENABLED" == "true" ]]; then
        RUN_DATABASE_MIGRATIONS="run"
        DATABASE_BACKUP_REQUIRED="true"
        DATABASE_MIGRATION_REQUIRED="true"
      else
        RUN_DATABASE_MIGRATIONS="skip"
      fi
      ;;
    true|run)
      if [[ "$DATABASE_ENABLED" != "true" ]]; then
        fail_deploy \
          "Database migrations were requested but the database is not enabled" \
          "$LINENO"
      fi
      RUN_DATABASE_MIGRATIONS="run"
      DATABASE_BACKUP_REQUIRED="true"
      DATABASE_MIGRATION_REQUIRED="true"
      ;;
    verify_only)
      if [[ "$BLUEGREEN_PREPARE_ONLY" != "true" ]] \
        || [[ "$DEPLOY_BRANCH" != "main" ]] \
        || [[ "$PRODUCTION_RELEASE_WORKFLOW" != "true" ]]; then
        fail_deploy \
          "Read-only database verification is restricted to blue/green main production preparation" \
          "$LINENO"
      fi
      if [[ "$DATABASE_ENABLED" == "true" ]]; then
        RUN_DATABASE_MIGRATIONS="verify_only"
        DATABASE_BACKUP_REQUIRED="true"
        DATABASE_MIGRATION_VERIFY_ONLY="true"
      else
        RUN_DATABASE_MIGRATIONS="skip"
      fi
      ;;
    false|skip)
      if [[ "$DATABASE_ENABLED" == "true" ]]; then
        fail_deploy \
          "Cannot skip migration evidence while the database is enabled; use verify_only for blue/green preparation" \
          "$LINENO"
      fi
      RUN_DATABASE_MIGRATIONS="skip"
      ;;
  esac

  if [[ "$DATABASE_MIGRATION_REQUIRED" == "true" ]] \
    && {
      [[ "$DEPLOY_BRANCH" != "main" ]] \
        || [[ "$PRODUCTION_RELEASE_WORKFLOW" != "true" ]];
    }; then
    fail_deploy "Database migrations require the main production release workflow" "$LINENO"
  fi
}

read_database_current_revision() {
  run_privileged_bash \
    'set -Eeuo pipefail; set -a; . "$1"; set +a; export PYTHONPATH="$2"; export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c default_transaction_read_only=on"; . "$3/bin/activate"; cd "$2"; python -m alembic current' \
    "$BACKEND_ENV_FILE" "$BACKEND_DIR" "$VENV_DIR"
}

read_candidate_migration_heads() {
  run_privileged_bash \
    'set -Eeuo pipefail; export PYTHONPATH="$1"; . "$2/bin/activate"; cd "$1"; python -m alembic heads' \
    "$BACKEND_DIR" "$VENV_DIR"
}

assert_alembic_revision_sets_equal() {
  local left_output="$1"
  local left_label="$2"
  local right_output="$3"
  local right_label="$4"
  local message="$5"

  python3 - "$left_output" "$left_label" "$right_output" "$right_label" "$message" <<'PY'
import re
import sys

left_output, left_label, right_output, right_label, message = sys.argv[1:]
pattern = re.compile(r"(?m)^([0-9]{8}_[0-9]{4})\b")
left = set(pattern.findall(left_output))
right = set(pattern.findall(right_output))
if not left or left != right:
    raise SystemExit(
        f"[ERROR] {message}: "
        f"{left_label}={sorted(left)} {right_label}={sorted(right)}"
    )
PY
}

verify_database_schema_without_migration() {
  echo "[INFO] Verify database schema without running migrations"
  CURRENT_STEP="Verify database migration compatibility"
  log_section "$CURRENT_STEP"
  DATABASE_READ_ONLY_GATE_FAILED="true"
  MIGRATION_PRE_REVISION="$(read_database_current_revision)"
  MIGRATION_TARGET_REVISION="$(read_candidate_migration_heads)"
  if ! assert_alembic_revision_sets_equal \
    "$MIGRATION_PRE_REVISION" "current" \
    "$MIGRATION_TARGET_REVISION" "heads" \
    "Blue/green release forbids database migrations and current does not match heads"; then
    fail_deploy \
      "Read-only database compatibility verification failed; no migration was executed" \
      "$LINENO"
  fi
  MIGRATION_RESULT_REVISION="$MIGRATION_PRE_REVISION"
  DATABASE_READ_ONLY_GATE_FAILED="false"
  echo "[INFO] Database current revision already matches candidate Alembic heads"
}

should_run_grouped_time_series_prewarm() {
  local mode="${RUN_GROUPED_TIME_SERIES_PREWARM,,}"
  local enabled="false"

  case "$mode" in
    1|true|yes|on|run|strict) return 0 ;;
    0|false|no|off|skip) return 1 ;;
  esac

  if [[ ! -f "$BACKEND_ENV_FILE" ]]; then
    return 1
  fi
  enabled="$(run_privileged_bash 'set -a; . "$1"; set +a; printf "%s\n" "${APP_GROUPED_TIME_SERIES_PREWARM_ENABLED:-false}"' "$BACKEND_ENV_FILE" 2>/dev/null || true)"
  is_truthy "$enabled"
}

backend_prewarm_identity() {
  if [[ ! -f "$BACKEND_ENV_FILE" ]]; then
    printf '\tprewarm\tviewer\n'
    return
  fi

  run_privileged_bash 'set -a; . "$1"; set +a; printf "%s\t%s\t%s\n" "${APP_AUTH_TOKEN:-${VITE_AUTH_TOKEN:-}}" "${VITE_USER_NAME:-prewarm}" "${APP_GROUPED_TIME_SERIES_PREWARM_SCOPES:-${VITE_USER_ROLE:-viewer}}"' "$BACKEND_ENV_FILE" 2>/dev/null \
    || printf '\tprewarm\tviewer\n'
}

run_grouped_time_series_prewarm() {
  local script="$REPO_DIR/03_Scripts/diagnostics/prewarm_grouped_time_series.py"
  local token=""
  local user_name=""
  local user_roles=""
  local identity=""
  local args=()

  if ! should_run_grouped_time_series_prewarm; then
    echo "[INFO] Grouped time-series prewarm skipped"
    return
  fi
  if [[ ! -f "$script" ]]; then
    echo "[WARN] Grouped time-series prewarm script missing: $script"
    return
  fi

  identity="$(backend_prewarm_identity)"
  IFS=$'\t' read -r token user_name user_roles <<< "$identity"
  args=(
    "$script"
    --origin "http://127.0.0.1:${BACKEND_PORT}"
    --user-name "${user_name:-prewarm}"
    --user-roles "${user_roles:-viewer}"
    --require-server-cache
    --require-repeat-hit
  )
  if [[ -n "$token" ]]; then
    args+=(--token "$token")
  fi

  if "$VENV_DIR/bin/python" "${args[@]}"; then
    echo "[INFO] Grouped time-series prewarm passed"
    return
  fi

  if [[ "${RUN_GROUPED_TIME_SERIES_PREWARM,,}" == "strict" ]]; then
    fail_deploy "Grouped time-series prewarm failed" "$LINENO"
  fi
  echo "[WARN] Grouped time-series prewarm failed; continuing deploy"
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

write_deploy_failure_context() {
  local line_no="$1"
  local command="$2"
  local rc="$3"
  local failure_dir=""

  failure_dir="$(dirname "$DEPLOY_FAILURE_FILE")"
  mkdir -p "$failure_dir" 2>/dev/null || true
  {
    echo "deploy_exit_code=$rc"
    echo "failed_step=$CURRENT_STEP"
    echo "failed_line=$line_no"
    echo "failed_command=$command"
    echo "timestamp=$(date -u)"
  } > "$DEPLOY_FAILURE_FILE" 2>&1 || true
}

fail_deploy() {
  local message="$1"
  local line_no="${2:-$LINENO}"

  echo "[ERROR] $message"
  record_failure_checkpoint "$message"
  write_deploy_failure_context "$line_no" "$message" 1
  run_diagnostics
  trap - ERR
  exit 1
}

on_error() {
  local line_no="$1"
  local command="$2"
  local rc="$3"
  echo
  echo "[ERROR] deploy_fullstack_server.sh failed"
  echo "[ERROR] exit_code=$rc"
  echo "[ERROR] step=$CURRENT_STEP"
  echo "[ERROR] line=$line_no"
  echo "[ERROR] command=$command"
  record_failure_checkpoint "step=$CURRENT_STEP line=$line_no command=$command rc=$rc"
  write_deploy_failure_context "$line_no" "$command" "$rc"
  run_diagnostics
}

trap 'on_error "$LINENO" "$BASH_COMMAND" "$?"' ERR

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
    fail_deploy "Missing required command: $name" "$LINENO"
  fi
}

require_command git
require_command curl
require_command python3
require_command sha256sum

cleanup_scraping_toolkit_egg_info() {
  if [[ ! -f "$TOOLKIT_EGG_INFO_HELPER" ]] \
    || [[ -L "$TOOLKIT_EGG_INFO_HELPER" ]]; then
    fail_deploy "Toolkit egg-info cleanup helper is missing or unsafe"
  fi
  python3 -B "$TOOLKIT_EGG_INFO_HELPER" --toolkit-root "$TOOLKIT_DIR"
}

cleanup_toolkit_on_exit() {
  local original_status="$?"
  trap - EXIT
  if ! cleanup_scraping_toolkit_egg_info; then
    exit 1
  fi
  exit "$original_status"
}

install_scraping_toolkit_editable() {
  trap cleanup_toolkit_on_exit EXIT

  if [[ ! -d "$TOOLKIT_DIR" ]] || [[ -L "$TOOLKIT_DIR" ]]; then
    fail_deploy "Scraping toolkit source must be a real directory: $TOOLKIT_DIR"
  fi

  cleanup_scraping_toolkit_egg_info
  python -m pip install -e "$TOOLKIT_DIR" \
    -i "$PIP_INDEX_URL" \
    --trusted-host "$PIP_TRUSTED_HOST"
  cleanup_scraping_toolkit_egg_info
  trap - EXIT
}

CURRENT_STEP="Validate persistent release checkpoint"
initialize_release_checkpoint
if [[ "$CHECKPOINT_ALREADY_COMPLETE" == "true" ]]; then
  if completed_checkpoint_matches_local; then
    echo "[INFO] Exact completed release is already healthy; direct server deploy is a no-op"
    exit 0
  fi
  echo "[ERROR] Complete checkpoint is immutable but local health/provenance does not match"
  exit 1
fi

echo "[INFO] Repository directory: $REPO_DIR"
rm -f "$DEPLOY_FAILURE_FILE" 2>/dev/null || true
if [[ -f "$DEPLOY_RELEASE_FILE" ]]; then
  echo "[INFO] Deploy release metadata:"
  python3 - "$DEPLOY_RELEASE_FILE" <<'PY' || true
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text())
print(f"[INFO] release={payload.get('shortSha') or str(payload.get('commitSha', ''))[:8]} run={payload.get('workflowRunId', '')} source={payload.get('source', '')}")
PY
else
  echo "[WARN] Deploy release metadata missing: $DEPLOY_RELEASE_FILE"
fi

mark_release_deployed() {
  local actual_commit=""

  if [[ ! -f "$DEPLOY_RELEASE_FILE" ]]; then
    if checkpoint_enabled; then
      echo "[ERROR] Cannot update missing deploy release metadata: $DEPLOY_RELEASE_FILE"
      return 1
    fi
    echo "[WARN] Cannot update missing deploy release metadata: $DEPLOY_RELEASE_FILE"
    return 0
  fi

  actual_commit="$(target_backend_commit 2>/dev/null || true)"
  if [[ ! "$actual_commit" =~ ^[0-9a-f]{40}$ ]]; then
    echo "[ERROR] Cannot confirm deployed release without a full target commit"
    return 1
  fi
  python3 -B "$BACKEND_RELEASE_HELPER" confirm-metadata \
    --path "$DEPLOY_RELEASE_FILE" \
    --commit "$actual_commit" \
    --service "$BACKEND_SERVICE_NAME"
}

cleanup_known_untracked_paths() {
  local raw_pattern=""
  local candidate=""
  local status_output=""
  local removed_count=0

  if ! is_truthy "$DEPLOY_PRUNE_UNTRACKED"; then
    echo "[INFO] Skipping untracked cleanup because DEPLOY_PRUNE_UNTRACKED=$DEPLOY_PRUNE_UNTRACKED"
    return
  fi

  if ! git_repository_available; then
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
    fail_deploy "Missing systemd source file: $source_path" "$LINENO"
  fi

  temp_file="$(mktemp)"
  sed "s|/opt/JATO_Analysis_System-main|$SYSTEMD_RUNTIME_ROOT|g" "$source_path" > "$temp_file"
  sudo -n install -D -m 644 "$temp_file" "$SYSTEMD_TARGET_DIR/$target_name"
  rm -f "$temp_file"
}

install_env_file_if_missing() {
  local source_path="$1"
  local target_path="$2"
  local mode="${3:-600}"
  local temp_file=""

  if [[ ! -f "$source_path" ]]; then
    fail_deploy "Missing env template: $source_path" "$LINENO"
  fi

  if sudo -n test -e "$target_path"; then
    echo "[INFO] Existing env file preserved: $target_path"
    return 0
  fi

  temp_file="$(mktemp)"
  sed "s|/opt/JATO_Analysis_System-main|$SYSTEMD_RUNTIME_ROOT|g" "$source_path" > "$temp_file"
  sudo -n install -D -m "$mode" "$temp_file" "$target_path"
  rm -f "$temp_file"
  echo "[INFO] Installed default env file: $target_path"
}

upsert_managed_env_value() {
  local target_path="$1"
  local key="$2"
  local value="$3"
  local local_candidate=""
  local remote_candidate="${target_path}.new.$$"

  if sudo -n test -L "$target_path" \
    || ! sudo -n test -f "$target_path"; then
    fail_deploy "Managed env target must be a regular non-symlink file: $target_path" "$LINENO"
  fi
  local_candidate="$(mktemp)"
  sudo -n cat "$target_path" > "$local_candidate"
  python3 - "$local_candidate" "$key" "$value" <<'PY'
from pathlib import Path
import re
import shlex
import sys

path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
    raise SystemExit("[ERROR] invalid managed env key")
if "\n" in value or "\r" in value:
    raise SystemExit("[ERROR] invalid managed env value")
pattern = re.compile(rf"^[ \t]*{re.escape(key)}=")
lines = [
    line
    for line in path.read_text(encoding="utf-8").splitlines()
    if not pattern.match(line)
]
lines.append(f"{key}={shlex.quote(value)}")
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
  if ! bash -n "$local_candidate" \
    || ! sudo -n install -m 0600 "$local_candidate" "$remote_candidate" \
    || ! sudo -n bash -n "$remote_candidate" \
    || ! sudo -n mv -f "$remote_candidate" "$target_path"; then
    rm -f "$local_candidate"
    sudo -n rm -f "$remote_candidate" >/dev/null 2>&1 || true
    fail_deploy "Failed to atomically update managed env value $key in $target_path" "$LINENO"
  fi
  rm -f "$local_candidate"
  echo "[INFO] Reconciled managed env value $key in $target_path"
}

run_pre_deploy_backup() {
  local database_required="${1:-false}"
  local backup_output=""

  if [[ "$RUN_PRE_DEPLOY_BACKUP" == "false" || "$RUN_PRE_DEPLOY_BACKUP" == "0" ]]; then
    if [[ "$database_required" == "true" ]] || checkpoint_enabled; then
      echo "[ERROR] Checkpointed production releases require a verified pre-deploy backup manifest"
      return 1
    fi
    echo "[INFO] Skipping pre-deploy backup because RUN_PRE_DEPLOY_BACKUP=$RUN_PRE_DEPLOY_BACKUP"
    return 0
  fi

  if [[ ! -f "$BACKUP_SCRIPT" ]]; then
    if [[ "$database_required" == "true" ]] || checkpoint_enabled; then
      echo "[ERROR] Required pre-deploy backup script missing: $BACKUP_SCRIPT"
      return 1
    fi
    echo "[WARN] Optional pre-deploy backup script missing: $BACKUP_SCRIPT"
    return 0
  fi

  echo "[INFO] Running pre-deploy production data backup"
  if backup_output="$(run_privileged_bash 'REPO_DIR="$1" BACKEND_ENV_FILE="$2" REQUIRE_DATABASE_BACKUP="$4" BACKUP_ROOT="$5" bash "$3"' \
    "$REPO_DIR" "$BACKEND_ENV_FILE" "$BACKUP_SCRIPT" "$database_required" "$RELEASE_BACKUP_ROOT")"; then
    printf '%s\n' "$backup_output"
    LAST_BACKUP_MANIFEST_PATH="$(sed -n 's/.*manifest=\([^ ]*\).*/\1/p' <<< "$backup_output" | tail -1)"
    LAST_BACKUP_MANIFEST_BYTES="$(sed -n 's/.*manifestBytes=\([0-9][0-9]*\).*/\1/p' <<< "$backup_output" | tail -1)"
    LAST_BACKUP_MANIFEST_SHA256="$(sed -n 's/.*manifestSha256=\([0-9a-f]\{64\}\).*/\1/p' <<< "$backup_output" | tail -1)"
    if checkpoint_enabled && {
      [[ -z "$LAST_BACKUP_MANIFEST_PATH" ]] \
        || [[ ! "$LAST_BACKUP_MANIFEST_BYTES" =~ ^[1-9][0-9]*$ ]] \
        || [[ ! "$LAST_BACKUP_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]];
    }; then
      echo "[ERROR] Backup completed without durable manifest identity"
      return 1
    fi
    echo "[INFO] Pre-deploy backup completed"
    return 0
  fi

  if checkpoint_enabled \
    || [[ "$database_required" == "true" || "$RUN_PRE_DEPLOY_BACKUP" == "true" || "$RUN_PRE_DEPLOY_BACKUP" == "1" ]]; then
    echo "[ERROR] Pre-deploy backup failed"
    return 1
  fi

  echo "[WARN] Pre-deploy backup failed; continuing because RUN_PRE_DEPLOY_BACKUP=$RUN_PRE_DEPLOY_BACKUP"
  return 0
}

precompress_frontend_assets() {
  local dist_dir="$1"
  local asset=""
  local gzip_count=0
  local brotli_count=0
  local has_brotli=false

  if [[ ! -d "$dist_dir" ]]; then
    echo "[ERROR] Frontend dist directory missing: $dist_dir"
    return 1
  fi

  if command -v brotli >/dev/null 2>&1; then
    has_brotli=true
  fi

  while IFS= read -r -d '' asset; do
    gzip -kf -9 "$asset"
    gzip_count=$((gzip_count + 1))
    if [[ "$has_brotli" == "true" ]]; then
      brotli -f -q 6 "$asset"
      brotli_count=$((brotli_count + 1))
    fi
  done < <(
    find "$dist_dir" -type f \
      \( -name '*.js' -o -name '*.css' -o -name '*.html' -o -name '*.svg' -o -name '*.json' -o -name '*.wasm' \) \
      -size +1024c -print0
  )

  echo "[INFO] Precompressed frontend assets: gzip=$gzip_count brotli=$brotli_count"
  if [[ "$has_brotli" != "true" ]]; then
    echo "[INFO] brotli command not found; generated gzip assets only"
  fi
}

normalize_frontend_public_permissions() {
  local dist_dir="$1"
  local parent_dir=""
  local unsafe_link=""

  if [[ ! -d "$dist_dir" || -L "$dist_dir" ]]; then
    echo "[ERROR] Frontend dist directory is missing or unsafe: $dist_dir"
    return 1
  fi
  unsafe_link="$(find "$dist_dir" -type l -print -quit)"
  if [[ -n "$unsafe_link" ]]; then
    echo "[ERROR] Frontend dist must not contain symlinks: $unsafe_link"
    return 1
  fi
  for parent_dir in "$REPO_DIR" "$REPO_DIR/06_AppPlatform" "$FRONTEND_DIR"; do
    if ! python3 -B - "$parent_dir" <<'PY'
from pathlib import Path
import stat
import sys

path = Path(sys.argv[1])
metadata = path.lstat()
if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
    raise SystemExit(1)
if stat.S_IMODE(metadata.st_mode) not in {0o711, 0o755}:
    raise SystemExit(1)
PY
    then
      echo "[ERROR] Sealed frontend parent must already be a real safe traversable directory: $parent_dir"
      return 1
    fi
  done
  find "$dist_dir" -type d -exec chmod 755 {} +
  find "$dist_dir" -type f -exec chmod 644 {} +
  echo "[INFO] Normalized public frontend permissions"
}

install_prebuilt_frontend() {
  local target_dir="$FRONTEND_DIR/dist"
  local backup_dir="$FRONTEND_DIR/.dist-previous"

  if [[ -z "$PREBUILT_FRONTEND_DIR" || ! -d "$PREBUILT_FRONTEND_DIR" ]]; then
    fail_deploy "Verified prebuilt frontend directory is missing" "$LINENO"
  fi
  for required_file in index.html build-meta.json release-provenance.json; do
    if [[ ! -f "$PREBUILT_FRONTEND_DIR/$required_file" ]]; then
      fail_deploy "Verified prebuilt frontend is missing $required_file" "$LINENO"
    fi
  done

  if python3 - "$target_dir/build-meta.json" "$RELEASE_CHECKPOINT_COMMIT" \
    "$RELEASE_CHECKPOINT_FRONTEND_IDENTITY" "$RELEASE_CHECKPOINT_FRONTEND_CHECKSUM" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(1)
payload = json.loads(path.read_text(encoding="utf-8"))
if (
    payload.get("deployCommit") != sys.argv[2]
    or payload.get("artifactId") != sys.argv[3]
    or payload.get("artifactChecksum") != sys.argv[4]
):
    raise SystemExit(1)
PY
  then
    normalize_frontend_public_permissions "$target_dir"
    echo "[INFO] Verified frontend is already active; preserving existing rollback directory"
    return 0
  fi

  precompress_frontend_assets "$PREBUILT_FRONTEND_DIR"
  normalize_frontend_public_permissions "$PREBUILT_FRONTEND_DIR"
  if [[ -e "$backup_dir" ]]; then
    fail_deploy "Existing frontend rollback directory requires inspection: $backup_dir" "$LINENO"
  fi
  if [[ -e "$target_dir" ]]; then
    mv "$target_dir" "$backup_dir"
  fi
  if mv "$PREBUILT_FRONTEND_DIR" "$target_dir"; then
    normalize_frontend_public_permissions "$target_dir"
    echo "[INFO] Atomically installed verified prebuilt frontend dist; previous dist retained until backend health"
    return 0
  fi

  echo "[ERROR] Atomic frontend install failed; restoring previous dist"
  if [[ -e "$backup_dir" && ! -e "$target_dir" ]]; then
    mv "$backup_dir" "$target_dir"
  fi
  return 1
}

cleanup_previous_frontend_after_health() {
  local backup_dir="$FRONTEND_DIR/.dist-previous"
  if [[ -e "$backup_dir" ]]; then
    rm -rf "$backup_dir"
    echo "[INFO] Removed previous frontend only after backend health succeeded"
  fi
}

restart_timer_unit() {
  local timer_name="$1"

  sudo -n systemctl enable "$timer_name"
  sudo -n systemctl restart "$timer_name"
  sudo -n systemctl --no-pager status "$timer_name" 2>&1 | sed -n '1,12p' || true
}

_write_msrp_status() {
  local pipeline="$1"
  local status="$2"
  local reason="$3"
  local status_path="$REPO_DIR/03_Scripts/logs/scheduled_fetch_status.json"

  mkdir -p "$(dirname "$status_path")" "$REPO_DIR/hermes/reports/pipeline_status" 2>/dev/null || true
  python3 - "$status_path" "$pipeline" "$status" "$reason" <<'PY' 2>/dev/null || true
import json
import os
import sys
from datetime import datetime, timezone

status_path, pipeline, status, reason = sys.argv[1:5]
payload = {}
if os.path.exists(status_path):
    try:
        with open(status_path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (json.JSONDecodeError, OSError):
        payload = {}
payload[pipeline] = {
    "lastRunAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "status": status,
    "reason": reason,
}
with open(status_path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps(payload, indent=2) + "\n")
PY
  python3 "$REPO_DIR/03_Scripts/hermes/pipeline_status_writer.py" "$pipeline" \
    --status "$status" \
    --source "03_Scripts/ops/deploy_fullstack_server.sh" \
    --message "$reason" \
    --artifact-ref "03_Scripts/logs/scheduled_fetch_status.json" \
    --repo-root "$REPO_DIR" 2>/dev/null || true
  echo "[status] $pipeline=$status written to $status_path"
}

bootstrap_msrp_dryrun_if_missing() {
  local latest_report="$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_report.json"
  local runs_index="$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_runs_index.json"
  local service_name="jato-msrp-sync@dryrun.service"
  local gate_state="missing"

  if ! is_truthy "$BOOTSTRAP_MSRP_DRYRUN_IF_MISSING"; then
    echo "[INFO] Skipping MSRP dryrun bootstrap because BOOTSTRAP_MSRP_DRYRUN_IF_MISSING=$BOOTSTRAP_MSRP_DRYRUN_IF_MISSING"
    return 0
  fi

  if [[ -s "$latest_report" && -s "$runs_index" ]]; then
    gate_state="$(
      python3 - "$latest_report" <<'PY' 2>/dev/null || true
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except Exception:
    print("missing")
    raise SystemExit(0)
status = str(payload.get("gateStatus") or payload.get("gate_status") or "").strip().lower()
try:
    pass_pct = float(payload.get("overallPassRate") or payload.get("overallPassPct") or 0)
except (TypeError, ValueError):
    pass_pct = 0.0
try:
    threshold = float(payload.get("gateThreshold") or 70)
except (TypeError, ValueError):
    threshold = 70.0
if status == "allowed" or pass_pct >= threshold:
    print("allowed")
else:
    print(status or "blocked")
PY
    )"
    if [[ "$gate_state" == "allowed" ]]; then
      echo "[INFO] MSRP dryrun artifacts already exist and gate is allowed; not bootstrapping $service_name"
      return 0
    fi
    echo "[INFO] MSRP dryrun artifacts exist but gate is $gate_state; queueing a fresh $service_name"
  fi

  if sudo -n systemctl is-active --quiet "$service_name"; then
    echo "[INFO] $service_name already running; not starting another dryrun"
    _write_msrp_status "msrp_dryrun" "running" "$service_name active; waiting for dryrun artifacts"
    return 0
  fi

  echo "[INFO] MSRP dryrun artifacts missing; starting $service_name asynchronously"
  if sudo -n systemctl start --no-block "$service_name"; then
    echo "[INFO] $service_name queued; dryrun artifacts will appear after the run finishes"
    _write_msrp_status "msrp_dryrun" "running" "$service_name queued; dryrun artifacts pending"
  else
    echo "[WARN] Failed to queue $service_name; dryrun artifacts remain missing"
  fi
}

record_active_msrp_dryrun_status() {
  local runs_index="$REPO_DIR/03_Scripts/diagnostics/artifacts/dryrun_runs_index.json"
  local service_name="jato-msrp-sync@dryrun.service"

  if [[ -s "$runs_index" ]]; then
    return 0
  fi

  if sudo -n systemctl is-active --quiet "$service_name"; then
    echo "[INFO] $service_name active; recording running MSRP dryrun status"
    _write_msrp_status "msrp_dryrun" "running" "$service_name active; waiting for dryrun artifacts"
  fi
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
  upsert_managed_env_value \
    "$JATO_ETC_DIR/msrp.env" \
    JATO_API_BASE \
    "http://127.0.0.1:18000/v1"
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

  if ! is_truthy "$RECONCILE_SCRAPER_TIMER_STATE"; then
    echo "[INFO] Scheduler definitions refreshed without changing timer enabled/active state"
    return 0
  fi

  restart_timer_unit jato-country-news-sync.timer
  restart_timer_unit jato-country-news-sync-b.timer
  restart_timer_unit jato-msrp-dryrun.timer
  restart_timer_unit jato-msrp-ingest.timer
  restart_timer_unit jato-voc-forum-sync.timer
  restart_timer_unit hermes-source-quality.timer
}

run_post_deploy_readiness_audits() {
  local reports_dir="$REPO_DIR/hermes/reports"
  local api_base="http://127.0.0.1:${BACKEND_PORT}/v1"
  local script=""
  local rc=0

  mkdir -p "$reports_dir"

  script="$REPO_DIR/03_Scripts/diagnostics/msrp_readiness_audit.py"
  if [[ -f "$script" ]]; then
    echo "[INFO] Refreshing MSRP readiness status"
    "$VENV_DIR/bin/python" "$script" \
      --api-base "$api_base" \
      --timeout-seconds 20 \
      --out-dir "$reports_dir" \
      --write-status \
      || rc=$?
    if [[ "$rc" -ne 0 ]]; then
      echo "[WARN] MSRP readiness audit failed with rc=$rc"
      rc=0
    fi
  else
    echo "[WARN] MSRP readiness audit script missing: $script"
  fi

  script="$REPO_DIR/03_Scripts/diagnostics/ai_intelligence_enrichment_smoke.py"
  if [[ -f "$script" ]]; then
    echo "[INFO] Refreshing AI intelligence smoke status"
    "$VENV_DIR/bin/python" "$script" \
      --out-dir "$reports_dir" \
      --write-status \
      || rc=$?
    if [[ "$rc" -ne 0 ]]; then
      echo "[WARN] AI intelligence smoke audit failed with rc=$rc"
      rc=0
    fi
  else
    echo "[WARN] AI intelligence smoke script missing: $script"
  fi

  script="$REPO_DIR/03_Scripts/diagnostics/unified_scraping_readiness_audit.py"
  if [[ -f "$script" ]]; then
    echo "[INFO] Refreshing unified scraping readiness status"
    "$VENV_DIR/bin/python" "$script" \
      --repo-root "$REPO_DIR" \
      --out-dir "$reports_dir" \
      --write-status \
      || rc=$?
    if [[ "$rc" -ne 0 ]]; then
      echo "[WARN] Unified scraping readiness audit failed with rc=$rc"
    fi
  else
    echo "[WARN] Unified scraping readiness script missing: $script"
  fi
}

if [[ "$BLUEGREEN_GLOBAL_RECONCILE_ONLY" == "true" ]]; then
  if [[ "$BLUEGREEN_PREPARE_ONLY" == "true" ]] \
    || [[ "$BLUEGREEN_POST_ACTIVATION_ONLY" == "true" ]]; then
    fail_deploy "blue/green global reconcile mode is mutually exclusive" "$LINENO"
  fi
  echo "[INFO] Blue/green committed global scheduler definition reconciliation"
  if ! checkpoint_enabled || ! checkpoint_at_least backend_healthy; then
    fail_deploy "global reconciliation requires backend_healthy checkpoint" "$LINENO"
  fi
  resolve_target_backend_commit
  RECONCILE_SCRAPER_TIMER_STATE=false
  reconcile_scraper_schedulers
  echo "[INFO] Blue/green committed global scheduler definitions reconciled"
  exit 0
fi

if [[ "$BLUEGREEN_POST_ACTIVATION_ONLY" == "true" ]]; then
  if [[ "$BLUEGREEN_PREPARE_ONLY" == "true" ]]; then
    fail_deploy "blue/green prepare-only and post-activation-only are mutually exclusive" "$LINENO"
  fi
  echo "[INFO] Blue/green post-activation reconciliation"
  if ! checkpoint_enabled || ! checkpoint_at_least switched; then
    fail_deploy "blue/green post-activation requires a switched release checkpoint" "$LINENO"
  fi
  resolve_target_backend_commit
  if ! curl --noproxy '*' -fsS --max-time 10 \
    "http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null; then
    fail_deploy "blue/green active backend liveness failed" "$LINENO"
  fi
  verify_backend_readiness 10
  mark_release_deployed
  verify_release_evidence
  record_active_msrp_dryrun_status
  run_grouped_time_series_prewarm
  bootstrap_msrp_dryrun_if_missing
  run_post_deploy_readiness_audits
  echo "[INFO] Blue/green post-activation reconciliation completed"
  exit 0
fi

CURRENT_STEP="Validate sudo access"
log_section "$CURRENT_STEP"
if [[ "$(id -u)" -ne 0 ]]; then
  if ! sudo -n true 2>/dev/null; then
    echo "[WARN] sudo requires a password; skipping sudo -v (CI mode)"
    echo "[WARN] Later sudo -n calls may fail if NOPASSWD is not configured"
  fi
fi

if ! git_repository_available; then
  if [[ ! -d "$REPO_DIR" ]]; then
    fail_deploy "Repository directory not found at $REPO_DIR" "$LINENO"
  fi
  echo "[INFO] No .git metadata found; continuing with local tree only"
fi

CURRENT_STEP="Prune known untracked paths"
log_section "$CURRENT_STEP"
cleanup_known_untracked_paths

if [[ "$SKIP_GIT_SYNC" != "true" ]] && git_repository_available; then
  if [[ -z "$REMOTE_NAME" ]]; then
    if git -C "$REPO_DIR" remote get-url origin >/dev/null 2>&1; then
      REMOTE_NAME="origin"
    else
      REMOTE_NAME="$(git -C "$REPO_DIR" remote | sed -n '1p')"
    fi
  fi

  if [[ -z "$REMOTE_NAME" ]]; then
    fail_deploy "No git remote found in $REPO_DIR" "$LINENO"
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
  fail_deploy "Python virtualenv not found: $VENV_DIR" "$LINENO"
fi

echo "[INFO] Validate immutable frontend staging directory"
CURRENT_STEP="Validate immutable frontend staging directory"
log_section "$CURRENT_STEP"
if [[ -z "$PREBUILT_FRONTEND_DIR" || ! -d "$PREBUILT_FRONTEND_DIR" ]]; then
  fail_deploy "PREBUILT_FRONTEND_DIR must reference the verified release artifact" "$LINENO"
fi

echo "[INFO] Update repository"
CURRENT_STEP="Update repository"
log_section "$CURRENT_STEP"
if [[ "$SKIP_GIT_SYNC" == "true" ]]; then
  echo "[INFO] SKIP_GIT_SYNC=true; using the local tree without git pull"
elif git_repository_available; then
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

echo "[INFO] Resolve target backend release identity"
CURRENT_STEP="Resolve target backend release identity"
log_section "$CURRENT_STEP"
resolve_target_backend_commit

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
install_scraping_toolkit_editable

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

echo "[INFO] Resolve database migration policy"
CURRENT_STEP="Resolve database migration policy"
log_section "$CURRENT_STEP"
resolve_database_migration_policy

verify_live_migration_revision_if_available

if checkpoint_completed_or_past backup_verified; then
  echo "[INFO] Exact release checkpoint already has a verified backup"
else
  echo "[INFO] Run pre-deploy backup when configured"
  CURRENT_STEP="Run pre-deploy backup"
  log_section "$CURRENT_STEP"
  write_release_checkpoint backup_verified in_progress automatic "pre-deploy backup started"
  run_pre_deploy_backup "$DATABASE_BACKUP_REQUIRED"
  write_release_evidence "not_started"
  if [[ "$DATABASE_MIGRATION_VERIFY_ONLY" == "true" ]]; then
    verify_database_schema_without_migration
  fi
  write_release_checkpoint backup_verified completed automatic \
    "pre-deploy backup verified; evidence=$RELEASE_EVIDENCE_FILE sha256=$RELEASE_EVIDENCE_SHA256"
  verify_release_evidence
fi

if checkpoint_at_least migrated; then
  echo "[INFO] Exact release checkpoint already passed database migration"
elif [[ "$DATABASE_MIGRATION_REQUIRED" == "true" ]]; then
  echo "[INFO] Run database migrations when configured"
  CURRENT_STEP="Run database migrations"
  log_section "$CURRENT_STEP"
  MIGRATION_PRE_REVISION="$(read_database_current_revision)"
  MIGRATION_TARGET_REVISION="$(read_candidate_migration_heads)"
  write_release_evidence "in_progress"
  write_release_checkpoint migration_started in_progress manual_db_recovery \
    "database migration started; evidence=$RELEASE_EVIDENCE_FILE sha256=$RELEASE_EVIDENCE_SHA256; interruption requires manual database inspection"
  verify_release_evidence
  run_privileged_bash 'set -Eeuo pipefail; set -a; . "$1"; set +a; export PYTHONPATH="$2"; . "$3/bin/activate"; cd "$2"; python -m alembic upgrade head' \
    "$BACKEND_ENV_FILE" "$BACKEND_DIR" "$VENV_DIR"
  MIGRATION_RESULT_REVISION="$(read_database_current_revision)"
  assert_alembic_revision_sets_equal \
    "$MIGRATION_TARGET_REVISION" "target" \
    "$MIGRATION_RESULT_REVISION" "result" \
    "Database revision after migration does not match Alembic heads"
  write_release_evidence "completed"
  write_release_checkpoint migrated completed automatic \
    "database migration completed; evidence=$RELEASE_EVIDENCE_FILE sha256=$RELEASE_EVIDENCE_SHA256"
  verify_release_evidence
elif [[ "$DATABASE_MIGRATION_VERIFY_ONLY" == "true" ]]; then
  if [[ -z "$MIGRATION_PRE_REVISION" ]] \
    || [[ -z "$MIGRATION_TARGET_REVISION" ]] \
    || [[ -z "$MIGRATION_RESULT_REVISION" ]]; then
    verify_database_schema_without_migration
  fi
  write_release_evidence "completed"
  write_release_checkpoint migrated completed automatic \
    "database schema already matches candidate heads; no migration executed; evidence=$RELEASE_EVIDENCE_FILE sha256=$RELEASE_EVIDENCE_SHA256"
  verify_release_evidence
else
  echo "[INFO] Database migration evidence is not required because the database is disabled"
  write_release_evidence "not_required"
  write_release_checkpoint migrated completed automatic \
    "database migration not required; evidence=$RELEASE_EVIDENCE_FILE sha256=$RELEASE_EVIDENCE_SHA256"
  verify_release_evidence
fi

if [[ "$BLUEGREEN_PREPARE_ONLY" == "true" ]]; then
  echo "[INFO] Prepare backend release identity without switching production"
  CURRENT_STEP="Prepare blue/green backend release identity"
  prepare_target_backend_identity
  echo "[INFO] Install verified frontend inside the immutable candidate release"
  CURRENT_STEP="Install blue/green candidate frontend"
  install_prebuilt_frontend
  echo "[INFO] Blue/green candidate preparation completed before service start"
  exit 0
fi

if ! checkpoint_at_least switched; then
  write_release_checkpoint switch_started in_progress rollback_required \
    "frontend/backend switch started; interruption requires rollback inspection"

  echo "[INFO] Prepare backend release identity atomically"
  CURRENT_STEP="Prepare backend release identity"
  log_section "$CURRENT_STEP"
  prepare_target_backend_identity

  echo "[INFO] Install verified prebuilt frontend atomically"
  CURRENT_STEP="Install verified prebuilt frontend atomically"
  log_section "$CURRENT_STEP"
  install_prebuilt_frontend

  echo "[INFO] Restart backend service"
  CURRENT_STEP="Restart backend service"
  log_section "$CURRENT_STEP"
  if ! sudo -n systemctl cat "$BACKEND_SERVICE_NAME" >/dev/null 2>&1; then
    fail_deploy "systemd service not found: $BACKEND_SERVICE_NAME" "$LINENO"
  fi
  sudo -n systemctl restart "$BACKEND_SERVICE_NAME"
  sleep 2
  sudo -n systemctl --no-pager status "$BACKEND_SERVICE_NAME" 2>&1 | sed -n '1,30p' || true

  if systemctl is-active --quiet nginx; then
    echo "[INFO] Reload nginx"
    CURRENT_STEP="Reload nginx"
    log_section "$CURRENT_STEP"
    sudo -n systemctl reload nginx
  fi
  write_release_checkpoint switched completed automatic "frontend and backend switch completed"
else
  echo "[INFO] Exact release checkpoint already switched; restarting backend for recovery verification"
  CURRENT_STEP="Restart backend service for checkpoint recovery"
  prepare_target_backend_identity
  normalize_frontend_public_permissions "$FRONTEND_DIR/dist"
  if ! sudo -n systemctl cat "$BACKEND_SERVICE_NAME" >/dev/null 2>&1; then
    fail_deploy "systemd service not found: $BACKEND_SERVICE_NAME" "$LINENO"
  fi
  sudo -n systemctl restart "$BACKEND_SERVICE_NAME"
  sleep 2
fi

echo "[INFO] Verify backend health"
CURRENT_STEP="Verify backend health"
log_section "$CURRENT_STEP"
for i in $(seq 1 15); do
  health_ok="false"
  readiness_ok="false"
  if curl --noproxy '*' -fsS --max-time 10 \
    "http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null 2>&1; then
    health_ok="true"
  fi
  if verify_backend_readiness 10; then
    readiness_ok="true"
  fi
  if [[ "$health_ok" == "true" && "$readiness_ok" == "true" ]]; then
    echo "[INFO] Backend liveness and release readiness passed on attempt $i"
    break
  fi
  if [[ "$i" -eq 15 ]]; then
    fail_deploy \
      "Backend liveness/readiness failed after 15 attempts (healthz=$health_ok readyz=$readiness_ok)" \
      "$LINENO"
  fi
  echo "[INFO] Backend check attempt $i failed (healthz=$health_ok readyz=$readiness_ok), retrying in 5s …"
  sleep 5
done

mark_release_deployed
cleanup_previous_frontend_after_health
verify_release_evidence
write_release_checkpoint backend_healthy in_progress automatic \
  "backend liveness, release readiness, and provenance verified; post-health reconciliation pending"

echo "[INFO] Reconcile scraper schedulers after backend health"
CURRENT_STEP="Reconcile scraper schedulers"
log_section "$CURRENT_STEP"
reconcile_scraper_schedulers
record_active_msrp_dryrun_status

echo "[INFO] Prewarm grouped time-series cache"
CURRENT_STEP="Prewarm grouped time-series cache"
log_section "$CURRENT_STEP"
run_grouped_time_series_prewarm

echo "[INFO] Bootstrap MSRP dryrun after backend health"
CURRENT_STEP="Bootstrap MSRP dryrun"
log_section "$CURRENT_STEP"
bootstrap_msrp_dryrun_if_missing

echo "[INFO] Run post-deploy readiness audits"
CURRENT_STEP="Run post-deploy readiness audits"
log_section "$CURRENT_STEP"
run_post_deploy_readiness_audits

echo "[INFO] Server-side post-health work completed; outer release controller owns backend_healthy completion"

echo "[INFO] Current revision"
CURRENT_STEP="Print revision"
log_section "$CURRENT_STEP"
if git_repository_available; then
  git -C "$REPO_DIR" rev-parse --short HEAD
else
  echo "[INFO] No git revision available for archive-based bootstrap"
fi
echo "[INFO] Deployment finished successfully"
