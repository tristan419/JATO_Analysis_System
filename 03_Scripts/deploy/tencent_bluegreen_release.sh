#!/usr/bin/env bash
set -Eeuo pipefail

# Tencent CVM blue/green controller.
#
# The outer release script has already authenticated the content-addressed
# archive and frontend artifact.  This controller prepares an independent
# release/venv, validates the inactive 8000/8001 slot, and only then executes
# the Nginx handover under the JATO quiescence supervisor.

BLUEGREEN_ROOT="${BLUEGREEN_ROOT:-/opt/jato}"
RELEASES_ROOT="${RELEASES_ROOT:-$BLUEGREEN_ROOT/releases}"
SLOTS_ROOT="${SLOTS_ROOT:-$BLUEGREEN_ROOT/slots}"
SHARED_ROOT="${SHARED_ROOT:-$BLUEGREEN_ROOT/shared}"
ACTIVE_RELEASE_LINK="${ACTIVE_RELEASE_LINK:-$BLUEGREEN_ROOT/active}"
BLUEGREEN_STATE_ROOT="${BLUEGREEN_STATE_ROOT:-/var/lib/jato-release}"
ACTIVE_SLOT_FILE="${ACTIVE_SLOT_FILE:-$BLUEGREEN_STATE_ROOT/active-slot}"
DEPLOYMENT_MARKER="${DEPLOYMENT_MARKER:-$BLUEGREEN_STATE_ROOT/deployment-maintenance}"
BLUEGREEN_CONTROLLER_TIMEOUT="${BLUEGREEN_CONTROLLER_TIMEOUT:-2700}"
NGINX_ACTIVE_RELEASE_CONF="${NGINX_ACTIVE_RELEASE_CONF:-/etc/jato-fullstack/nginx/active-release.conf}"
NGINX_PREIMAGE_DIR="${NGINX_PREIMAGE_DIR:-$BLUEGREEN_STATE_ROOT/nginx-preimage-${DEPLOY_COMMIT_SHA:-unknown}}"
SLOT_ENV_ROOT="${SLOT_ENV_ROOT:-/etc/jato-fullstack/slots}"
BACKEND_ENV_FILE="${BACKEND_ENV_FILE:-/etc/jato-fullstack/backend.env}"
LEGACY_ROOT="${LEGACY_ROOT:-/opt/JATO_Analysis_System-main}"
JATO_JOB_ROOT="${JATO_JOB_ROOT:-$LEGACY_ROOT/04_Processed_data/ops/jato_monthly_update_jobs}"
BLUEGREEN_QUIESCENCE_TIMEOUT="${BLUEGREEN_QUIESCENCE_TIMEOUT:-1800}"
BLUEGREEN_DRAIN_SECONDS="${BLUEGREEN_DRAIN_SECONDS:-30}"
BLUEGREEN_CANDIDATE_MEMORY_HIGH="${BLUEGREEN_CANDIDATE_MEMORY_HIGH:-3G}"
BLUEGREEN_CANDIDATE_MEMORY_MAX="${BLUEGREEN_CANDIDATE_MEMORY_MAX:-4G}"
BLUEGREEN_CANDIDATE_BUILD_TIMEOUT="${BLUEGREEN_CANDIDATE_BUILD_TIMEOUT:-1800}"
BLUEGREEN_ACTIVE_MEMORY_HIGH="${BLUEGREEN_ACTIVE_MEMORY_HIGH:-6G}"
BLUEGREEN_ACTIVE_MEMORY_MAX="${BLUEGREEN_ACTIVE_MEMORY_MAX:-8G}"
BLUEGREEN_MIN_TOTAL_MEMORY_BYTES=$((14 * 1024 * 1024 * 1024))
BLUEGREEN_MIN_AVAILABLE_MEMORY_BYTES=$((5 * 1024 * 1024 * 1024))
BLUEGREEN_CANDIDATE_MAX_MEMORY_BYTES=$((4 * 1024 * 1024 * 1024))
BLUEGREEN_OS_MEMORY_RESERVE_BYTES=$((2 * 1024 * 1024 * 1024))
BLUEGREEN_ACTIVE_MEMORY_HIGH_BYTES=$((6 * 1024 * 1024 * 1024))
BLUEGREEN_ACTIVE_MEMORY_MAX_BYTES=$((8 * 1024 * 1024 * 1024))
BLUEGREEN_PREPARE_DISK_RESERVE_BYTES=$((15 * 1024 * 1024 * 1024))
BLUEGREEN_PREPARE_DISK_RESERVE_PERCENT=8
BLUEGREEN_RUNTIME_DISK_RESERVE_BYTES=$((10 * 1024 * 1024 * 1024))
BLUEGREEN_RUNTIME_DISK_RESERVE_PERCENT=5
BLUEGREEN_RELEASE_KEEP_UNREFERENCED=3
BLUEGREEN_RELEASE_NORMAL_GC_AGE_SECONDS=$((14 * 24 * 60 * 60))
BLUEGREEN_RELEASE_EMERGENCY_GC_AGE_SECONDS=$((24 * 60 * 60))
BLUEGREEN_FAULT="${BLUEGREEN_FAULT:-}"
BLUEGREEN_MODE="${1:-prepare-and-switch}"
SERVICE_PREFIX="jato-fullstack-backend@"
SCHEDULER_STATE_FILE="${SCHEDULER_STATE_FILE:-$BLUEGREEN_STATE_ROOT/scheduler-state.tsv}"
EXIT_COMMAND_FAILED_MARKER_RETAINED=81
SCHEDULER_TIMERS=(
  jato-country-news-sync.timer
  jato-country-news-sync-b.timer
  jato-msrp-dryrun.timer
  jato-msrp-ingest.timer
  jato-voc-forum-sync.timer
  hermes-source-quality.timer
)
SCHEDULER_SERVICES=(
  jato-country-news-sync.service
  jato-country-news-sync-b.service
  jato-msrp-sync@dryrun.service
  jato-msrp-sync@ingest.service
  jato-voc-forum-sync.service
  hermes-source-quality.service
)

required_environment=(
  DEPLOY_COMMIT_SHA
  DEPLOY_ARCHIVE_SHA256
  DEPLOY_ARCHIVE_BYTES
  DEPLOY_REPOSITORY
  DEPLOY_RUN_ID
  DEPLOY_RUN_ATTEMPT
  DEPLOY_BRANCH
  FRONTEND_ARTIFACT_IDENTITY
  FRONTEND_ARTIFACT_CHECKSUM
  RELEASE_WORKTREE
  PREBUILT_FRONTEND_DIR
  CHECKPOINT_FILE
  CHECKPOINT_JOURNAL
)

fail() {
  echo "[ERROR] $*" >&2
  return 1
}

is_truthy() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

require_environment() {
  local name=""
  for name in "${required_environment[@]}"; do
    if [[ -z "${!name:-}" ]]; then
      fail "$name is required by the Tencent blue/green controller"
    fi
  done
  if [[ "$DEPLOY_BRANCH" != "main" ]]; then
    fail "Tencent blue/green production release only accepts main"
  fi
  if [[ ! "$DEPLOY_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]] \
    || [[ ! "$DEPLOY_ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    fail "release identity is malformed"
  fi
  if [[ "$BLUEGREEN_ROOT" != "/opt/jato" ]] \
    || [[ "$RELEASES_ROOT" != "/opt/jato/releases" ]] \
    || [[ "$SLOTS_ROOT" != "/opt/jato/slots" ]] \
    || [[ "$ACTIVE_RELEASE_LINK" != "/opt/jato/active" ]] \
    || [[ "$BLUEGREEN_STATE_ROOT" != "/var/lib/jato-release" ]]; then
    fail "production blue/green storage roots must use the reviewed /opt/jato layout"
  fi
  if [[ -n "${BLUEGREEN_CHECKPOINT_HELPER_OVERRIDE:-}" ]] \
    || [[ -n "${BLUEGREEN_STORAGE_GUARD_OVERRIDE:-}" ]]; then
    fail "production blue/green release does not accept helper overrides"
  fi
}

ensure_bluegreen_state_root() {
  if sudo -n test -L "$BLUEGREEN_STATE_ROOT" \
    || {
      sudo -n test -e "$BLUEGREEN_STATE_ROOT" \
        && ! sudo -n test -d "$BLUEGREEN_STATE_ROOT";
    }; then
    fail "blue/green state root must be a real directory"
    return 1
  fi
  sudo -n install -d -m 0755 "$BLUEGREEN_STATE_ROOT"
}

assert_inherited_production_lock() {
  local expected_lock=""
  local mutation_lock_helper="$RELEASE_WORKTREE/03_Scripts/deploy/lib/production_mutation_lock.sh"
  if [[ ! -f "$mutation_lock_helper" ]] || [[ -L "$mutation_lock_helper" ]]; then
    fail "production mutation lock helper is missing or unsafe"
    return 1
  fi
  if [[ -z "${DEPLOY_STATE_DIR:-}" || "$DEPLOY_STATE_DIR" != /* ]] \
    || [[ -z "${DEPLOY_LOCK_PATH:-}" || "$DEPLOY_LOCK_PATH" != /* ]]; then
    fail "production deployment lock paths are missing or relative"
    return 1
  fi
  expected_lock="${DEPLOY_STATE_DIR%/}/production-deploy.lock"
  if [[ "$DEPLOY_LOCK_PATH" != "$expected_lock" ]]; then
    fail "production deployment lock path differs from its state namespace"
    return 1
  fi
  # shellcheck source=03_Scripts/deploy/lib/production_mutation_lock.sh
  source "$mutation_lock_helper"
  jato_validate_inherited_production_lock "$DEPLOY_LOCK_PATH"
}

ensure_bluegreen_runtime_roots() {
  local path=""
  for path in \
    "$BLUEGREEN_ROOT" \
    "$RELEASES_ROOT" \
    "$RELEASES_ROOT/$DEPLOY_COMMIT_SHA" \
    "$SLOTS_ROOT" \
    "$SLOTS_ROOT/8000" \
    "$SLOTS_ROOT/8001" \
    "$SHARED_ROOT"; do
    if sudo -n test -L "$path" \
      || {
        sudo -n test -e "$path" \
          && ! sudo -n test -d "$path";
      }; then
      fail "blue/green runtime directory is unsafe: $path"
      return 1
    fi
    sudo -n install -d -m 0755 "$path" || return 1
  done
}

checkpoint_identity_args=(
  --repository "${DEPLOY_REPOSITORY:-}"
  --commit "${DEPLOY_COMMIT_SHA:-}"
  --archive-sha256 "${DEPLOY_ARCHIVE_SHA256:-}"
  --archive-bytes "${DEPLOY_ARCHIVE_BYTES:-0}"
  --run-id "${DEPLOY_RUN_ID:-0}"
  --run-attempt "${DEPLOY_RUN_ATTEMPT:-0}"
  --frontend-identity "${FRONTEND_ARTIFACT_IDENTITY:-}"
  --frontend-checksum "${FRONTEND_ARTIFACT_CHECKSUM:-}"
)
runtime_seal_identity_args=(
  --commit "${DEPLOY_COMMIT_SHA:-}"
  --archive-sha256 "${DEPLOY_ARCHIVE_SHA256:-}"
  --frontend-identity "${FRONTEND_ARTIFACT_IDENTITY:-}"
  --frontend-checksum "${FRONTEND_ARTIFACT_CHECKSUM:-}"
)

RELEASE_DIR="$RELEASES_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256"
RELEASE_IDENTITY_FILE="$RELEASE_DIR/.jato-release-identity"
RELEASE_SOURCE_SEAL_FILE="$RELEASE_DIR/.jato-source-seal.json"
RELEASE_RUNTIME_SEAL_FILE="$RELEASE_DIR/.jato-runtime-seal.json"
CHECKPOINT_HELPER="${BLUEGREEN_CHECKPOINT_HELPER_OVERRIDE:-$RELEASE_WORKTREE/03_Scripts/deploy/release_checkpoint.py}"
RELEASE_STORAGE_GUARD="${BLUEGREEN_STORAGE_GUARD_OVERRIDE:-$RELEASE_WORKTREE/03_Scripts/deploy/jato_release_storage_guard.py}"
READINESS_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/verify_backend_readiness.py"
QUIESCENCE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/jato_quiescence_gate.py"
SYSTEMD_TEMPLATE="$RELEASE_WORKTREE/03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
SHARED_BACKEND_TEMPLATE="/etc/systemd/system/jato-fullstack-backend@.service"
BACKEND_TEMPLATE_PREIMAGE="$BLUEGREEN_STATE_ROOT/backend-template.pre-${DEPLOY_COMMIT_SHA:-unknown}.service"
BACKEND_TEMPLATE_PREIMAGE_STATE="${BACKEND_TEMPLATE_PREIMAGE}.state"
SLOT_ENV_TEMPLATE="$RELEASE_WORKTREE/03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example"
NGINX_INSTALLER="$RELEASE_WORKTREE/03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh"
SOURCE_SEAL_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/verify_release_source_seal.py"
BOOT_RECONCILE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/jato_bluegreen_boot_reconcile.py"
BOOT_RECONCILE_HELPER_TARGET="/usr/local/libexec/jato-bluegreen-boot-reconcile.py"
BOOT_RECONCILE_UNIT_TEMPLATE="$RELEASE_WORKTREE/03_Scripts/deploy/systemd/jato-bluegreen-boot-reconcile.service"
BOOT_RECONCILE_UNIT_TARGET="/etc/systemd/system/jato-bluegreen-boot-reconcile.service"
NGINX_BOOT_DROPIN_TEMPLATE="$RELEASE_WORKTREE/03_Scripts/deploy/systemd/nginx-jato-bluegreen-boot-reconcile.conf"
NGINX_BOOT_DROPIN_TARGET="/etc/systemd/system/nginx.service.d/20-jato-bluegreen-boot-reconcile.conf"
BOOT_RECONCILE_UNIT="jato-bluegreen-boot-reconcile.service"
INNER_DEPLOY="$RELEASE_DIR/03_Scripts/ops/deploy_fullstack_server.sh"
EVIDENCE_FILE="${CHECKPOINT_FILE%.json}.evidence.json"
CHECKPOINTS_ROOT="${CHECKPOINTS_ROOT:-$(dirname "$(dirname "$CHECKPOINT_FILE")")}"
PREVIOUS_RELEASE_METADATA_PATH="$BLUEGREEN_STATE_ROOT/previous-metadata/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256.json"
CURRENT_ACTIVE_SLOT="${CURRENT_ACTIVE_SLOT:-}"
CANDIDATE_SLOT="${CANDIDATE_SLOT:-}"
CURRENT_FRONTEND_ROOT=""
PREVIOUS_RELEASE_ROOT=""
PREVIOUS_RELEASE_SHA=""
CHECKPOINT_PHASE=""
CHECKPOINT_STATUS=""
SWITCH_BACKUP=""
SCHEDULERS_PAUSED=false
SWITCH_COMPLETED=false
SWITCH_RECONCILED=false
SWITCH_HANDLER_ACTIVE=false
RELEASE_ROLLED_BACK=false
PRE_SUPERVISOR_CANDIDATE_ARMED=false
RUNTIME_ALREADY_SEALED=false

checkpoint_write() {
  local phase="$1"
  local status="$2"
  local retry_class="$3"
  local message="$4"
  python3 -B "$CHECKPOINT_HELPER" write \
    --checkpoint "$CHECKPOINT_FILE" \
    --journal "$CHECKPOINT_JOURNAL" \
    "${checkpoint_identity_args[@]}" \
    --phase "$phase" \
    --status "$status" \
    --retry-class "$retry_class" \
    --message "$message" >/dev/null
}

evidence_binding() {
  local digest=""
  if [[ ! -f "$EVIDENCE_FILE" || -L "$EVIDENCE_FILE" ]]; then
    fail "release evidence is unavailable before the switch"
  fi
  digest="$(sha256sum "$EVIDENCE_FILE" | awk '{print $1}')"
  if [[ ! "$digest" =~ ^[0-9a-f]{64}$ ]]; then
    fail "release evidence digest is invalid"
  fi
  printf 'evidence_path=%s evidence_sha256=%s' "$EVIDENCE_FILE" "$digest"
}

durable_install_file() {
  local source="$1"
  local target="$2"
  local mode="${3:-0644}"
  sudo -n mkdir -p "$(dirname "$target")"
  sudo -n python3 -B - "$source" "$target" "$mode" <<'PY'
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile

source = Path(sys.argv[1])
target = Path(sys.argv[2])
mode = int(sys.argv[3], 8)
source_stat = source.lstat()
if not stat.S_ISREG(source_stat.st_mode) or source.is_symlink():
    raise SystemExit(f"[ERROR] durable source must be a regular non-symlink file: {source}")
target.parent.mkdir(parents=True, exist_ok=True)
descriptor, temporary_name = tempfile.mkstemp(
    prefix=f".{target.name}.",
    suffix=".new",
    dir=target.parent,
)
temporary = Path(temporary_name)
try:
    source_flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        source_flags |= os.O_NOFOLLOW
    source_descriptor = os.open(source, source_flags)
    try:
        with os.fdopen(source_descriptor, "rb") as reader, os.fdopen(
            descriptor,
            "wb",
        ) as writer:
            descriptor = -1
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fchmod(writer.fileno(), mode)
            os.fsync(writer.fileno())
    finally:
        if source_descriptor >= 0:
            try:
                os.close(source_descriptor)
            except OSError:
                pass
    os.replace(temporary, target)
    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    directory_descriptor = os.open(target.parent, directory_flags)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
finally:
    if descriptor >= 0:
        try:
            os.close(descriptor)
        except OSError:
            pass
    try:
        temporary.unlink()
    except FileNotFoundError:
        pass
PY
}

durable_remove_path() {
  local target="$1"
  sudo -n python3 -B - "$target" <<'PY'
import os
from pathlib import Path
import stat
import sys

target = Path(sys.argv[1])
try:
    target_stat = target.lstat()
except FileNotFoundError:
    raise SystemExit(0)
if stat.S_ISDIR(target_stat.st_mode):
    raise SystemExit(f"[ERROR] durable unlink refuses a directory: {target}")
target.unlink()
flags = os.O_RDONLY
if hasattr(os, "O_DIRECTORY"):
    flags |= os.O_DIRECTORY
descriptor = os.open(target.parent, flags)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
}

durable_remove_tree() {
  local target="$1"
  if [[ "$target" != "$BLUEGREEN_STATE_ROOT/"* ]]; then
    fail "refusing to remove a blue/green state tree outside $BLUEGREEN_STATE_ROOT"
    return 1
  fi
  sudo -n python3 -B - "$target" <<'PY'
import os
from pathlib import Path
import shutil
import stat
import sys

target = Path(sys.argv[1])
try:
    target_stat = target.lstat()
except FileNotFoundError:
    raise SystemExit(0)
if not stat.S_ISDIR(target_stat.st_mode) or target.is_symlink():
    raise SystemExit(f"[ERROR] durable tree must be a real directory: {target}")
shutil.rmtree(target)
flags = os.O_RDONLY
if hasattr(os, "O_DIRECTORY"):
    flags |= os.O_DIRECTORY
descriptor = os.open(target.parent, flags)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
}

atomic_text() {
  local target="$1"
  local value="$2"
  local temporary=""
  temporary="$(mktemp)"
  printf '%s\n' "$value" > "$temporary"
  if ! durable_install_file "$temporary" "$target" 0644; then
    rm -f "$temporary"
    return 1
  fi
  rm -f "$temporary"
}

atomic_symlink() {
  local target="$1"
  local link_path="$2"
  sudo -n mkdir -p "$(dirname "$link_path")"
  sudo -n python3 -B - "$target" "$link_path" <<'PY'
import os
from pathlib import Path
import secrets
import sys

target = sys.argv[1]
link = Path(sys.argv[2])
link.parent.mkdir(parents=True, exist_ok=True)
for _attempt in range(32):
    temporary = link.parent / f".{link.name}.{secrets.token_hex(8)}.new"
    try:
        os.symlink(target, temporary)
        break
    except FileExistsError:
        continue
else:
    raise SystemExit("[ERROR] cannot allocate a same-filesystem symlink temporary")
try:
    os.replace(temporary, link)
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(link.parent, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
finally:
    try:
        temporary.unlink()
    except FileNotFoundError:
        pass
PY
}

resolve_active_slot() {
  local value=""
  local port8000=false
  local port8001=false
  if sudo -n test -L "$ACTIVE_SLOT_FILE"; then
    fail "durable active slot file must not be a symlink"
  fi
  if sudo -n test -f "$ACTIVE_SLOT_FILE"; then
    value="$(sudo -n cat "$ACTIVE_SLOT_FILE")"
    if [[ "$value" != "8000" && "$value" != "8001" ]]; then
      fail "durable active slot is invalid: $value"
    fi
    CURRENT_ACTIVE_SLOT="$value"
  else
    systemctl is-active --quiet "${SERVICE_PREFIX}8000" && port8000=true
    systemctl is-active --quiet "${SERVICE_PREFIX}8001" && port8001=true
    if [[ "$port8000" == "true" && "$port8001" == "false" ]]; then
      CURRENT_ACTIVE_SLOT="8000"
    elif [[ "$port8001" == "true" && "$port8000" == "false" ]]; then
      CURRENT_ACTIVE_SLOT="8001"
    else
      fail "cannot prove one legacy active backend slot"
    fi
    atomic_text "$ACTIVE_SLOT_FILE" "$CURRENT_ACTIVE_SLOT"
  fi
  if [[ "$CURRENT_ACTIVE_SLOT" == "8000" ]]; then
    CANDIDATE_SLOT="8001"
  else
    CANDIDATE_SLOT="8000"
  fi
}

resolve_current_frontend_root() {
  local slot_link="$SLOTS_ROOT/$CURRENT_ACTIVE_SLOT/current"
  if sudo -n test -L "$slot_link"; then
    CURRENT_FRONTEND_ROOT="$(
      sudo -n realpath "$slot_link"
    )/06_AppPlatform/frontend/dist"
  else
    CURRENT_FRONTEND_ROOT="$LEGACY_ROOT/06_AppPlatform/frontend/dist"
  fi
  if ! sudo -n test -f "$CURRENT_FRONTEND_ROOT/index.html"; then
    fail "current frontend root cannot be proven: $CURRENT_FRONTEND_ROOT"
  fi
}

ensure_shared_link() {
  local stable_path="$1"
  local live_path="$2"
  if ! sudo -n test -d "$live_path"; then
    fail "required durable runtime path is missing: $live_path"
  fi
  if sudo -n test -L "$stable_path"; then
    if [[ "$(sudo -n realpath "$stable_path")" != "$(sudo -n realpath "$live_path")" ]]; then
      fail "durable shared link points to an unexpected path: $stable_path"
    fi
    return
  fi
  if sudo -n test -e "$stable_path"; then
    fail "durable shared path already exists and is not a symlink: $stable_path"
  fi
  atomic_symlink "$live_path" "$stable_path"
}

prepare_shared_runtime() {
  sudo -n mkdir -p "$SHARED_ROOT"
  ensure_shared_link "$SHARED_ROOT/01_RAW_DATA" "$LEGACY_ROOT/01_RAW_DATA"
  ensure_shared_link "$SHARED_ROOT/04_Processed_data" "$LEGACY_ROOT/04_Processed_data"
  if [[ ! -d "$JATO_JOB_ROOT" || -L "$JATO_JOB_ROOT" ]]; then
    fail "JATO job root must be one durable real directory: $JATO_JOB_ROOT"
  fi
}

release_identity_matches() {
  sudo -n test -f "$RELEASE_IDENTITY_FILE" \
    && [[ "$(sudo -n cat "$RELEASE_IDENTITY_FILE")" == \
      "commit=$DEPLOY_COMMIT_SHA archive=$DEPLOY_ARCHIVE_SHA256" ]]
}

link_release_runtime_path() {
  local relative_path="$1"
  local durable_path="$2"
  sudo -n rm -rf "$RELEASE_DIR/$relative_path"
  sudo -n mkdir -p "$(dirname "$RELEASE_DIR/$relative_path")"
  sudo -n ln -s "$durable_path" "$RELEASE_DIR/$relative_path"
}

materialize_release_source() {
  local release_parent="$RELEASES_ROOT/$DEPLOY_COMMIT_SHA"
  local checkpoint_phase=""
  local expected_seal=""
  expected_seal="$(mktemp)"
  if ! python3 -B "$SOURCE_SEAL_HELPER" build \
    --root "$RELEASE_WORKTREE" \
    --output "$expected_seal"; then
    rm -f "$expected_seal"
    return 1
  fi
  if sudo -n test -L "$RELEASE_DIR"; then
    rm -f "$expected_seal"
    fail "immutable release path must not be a symlink: $RELEASE_DIR"
  fi
  if sudo -n test -e "$RELEASE_DIR" && ! release_identity_matches; then
    sudo -n rm -rf "$RELEASE_DIR"
  fi
  if ! sudo -n test -d "$RELEASE_DIR"; then
    checkpoint_write source_install_started in_progress automatic \
      "immutable blue/green release materialization started"
    sudo -n mkdir -p "$release_parent"
    sudo -n install -d -m 0755 "$RELEASE_DIR"
    (
      cd "$RELEASE_WORKTREE"
      tar cf - .
    ) | sudo -n tar xf - -C "$RELEASE_DIR"
    sudo -n chown -R "$(id -u):$(id -g)" "$RELEASE_DIR"
    link_release_runtime_path "01_RAW_DATA" "$SHARED_ROOT/01_RAW_DATA"
    link_release_runtime_path "04_Processed_data" "$SHARED_ROOT/04_Processed_data"
    link_release_runtime_path \
      "03_Scripts/diagnostics/artifacts" \
      "$LEGACY_ROOT/03_Scripts/diagnostics/artifacts"
    link_release_runtime_path \
      "03_Scripts/logs" \
      "$LEGACY_ROOT/03_Scripts/logs"
    link_release_runtime_path "hermes/reports" "$LEGACY_ROOT/hermes/reports"
    printf 'commit=%s archive=%s\n' \
      "$DEPLOY_COMMIT_SHA" "$DEPLOY_ARCHIVE_SHA256" > /tmp/jato-release-identity.$$
    sudo -n install -m 0444 /tmp/jato-release-identity.$$ "$RELEASE_IDENTITY_FILE"
    rm -f /tmp/jato-release-identity.$$
    if ! python3 -B "$SOURCE_SEAL_HELPER" verify \
      --root "$RELEASE_DIR" \
      --manifest "$expected_seal" \
      || ! durable_install_file \
        "$expected_seal" "$RELEASE_SOURCE_SEAL_FILE" 0444; then
      rm -f "$expected_seal"
      return 1
    fi
    checkpoint_write source_installed completed automatic \
      "immutable release source and durable runtime links installed"
  else
    if sudo -n test -L "$RELEASE_SOURCE_SEAL_FILE" \
      || ! sudo -n test -f "$RELEASE_SOURCE_SEAL_FILE" \
      || ! sudo -n cmp -s "$expected_seal" "$RELEASE_SOURCE_SEAL_FILE" \
      || ! python3 -B "$SOURCE_SEAL_HELPER" verify \
        --root "$RELEASE_DIR" \
        --manifest "$expected_seal"; then
      rm -f "$expected_seal"
      fail "persistent release source failed verified archive seal reuse"
      return 1
    fi
  fi
  rm -f "$expected_seal"
  checkpoint_phase="$(
    python3 -B "$CHECKPOINT_HELPER" show --checkpoint "$CHECKPOINT_FILE" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["phase"])'
  )"
  if [[ "$checkpoint_phase" == "source_install_started" ]]; then
    checkpoint_write source_installed completed automatic \
      "verified an existing immutable release source and durable runtime links"
  fi
}

verify_materialized_release_source() {
  local expected_seal=""
  expected_seal="$(mktemp)"
  if ! python3 -B "$SOURCE_SEAL_HELPER" build \
    --root "$RELEASE_WORKTREE" \
    --output "$expected_seal" \
    || sudo -n test -L "$RELEASE_SOURCE_SEAL_FILE" \
    || ! sudo -n test -f "$RELEASE_SOURCE_SEAL_FILE" \
    || ! sudo -n cmp -s "$expected_seal" "$RELEASE_SOURCE_SEAL_FILE" \
    || ! python3 -B "$SOURCE_SEAL_HELPER" verify \
      --root "$RELEASE_DIR" \
      --manifest "$expected_seal"; then
    rm -f "$expected_seal"
    fail "materialized release source changed after archive verification"
    return 1
  fi
  rm -f "$expected_seal"
}

write_candidate_deploy_status() {
  local dist="$RELEASE_DIR/06_AppPlatform/frontend/dist"
  local temp=""
  temp="$(mktemp "$dist/.deploy_status.XXXXXX")"
  {
    echo "deploy_exit_code=0"
    echo "release_sha=$DEPLOY_COMMIT_SHA"
    echo "active_slot=$CANDIDATE_SLOT"
    echo "deployment_mode=tencent_bluegreen"
    echo "candidate_memory_high=$BLUEGREEN_CANDIDATE_MEMORY_HIGH"
    echo "candidate_memory_max=$BLUEGREEN_CANDIDATE_MEMORY_MAX"
    echo "timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$temp"
  chmod 0644 "$temp"
  mv -f "$temp" "$dist/_deploy_status.txt"
}

finalize_runtime_seal() {
  local expected_seal=""
  if sudo -n test -e "$RELEASE_RUNTIME_SEAL_FILE" \
    || sudo -n test -L "$RELEASE_RUNTIME_SEAL_FILE"; then
    fail "refusing to overwrite an existing final runtime seal"
    return 1
  fi
  expected_seal="$(mktemp)"
  if ! python3 -B "$SOURCE_SEAL_HELPER" build \
    --profile runtime \
    --root "$RELEASE_DIR" \
    --output "$expected_seal" \
    "${runtime_seal_identity_args[@]}" \
    || ! durable_install_file \
      "$expected_seal" "$RELEASE_RUNTIME_SEAL_FILE" 0444; then
    rm -f "$expected_seal"
    return 1
  fi
  rm -f "$expected_seal"
  verify_final_runtime_seal
}

prepare_candidate_runtime() {
  if sudo -n test -e "$RELEASE_RUNTIME_SEAL_FILE" \
    || sudo -n test -L "$RELEASE_RUNTIME_SEAL_FILE"; then
    verify_final_runtime_seal || return 1
    RUNTIME_ALREADY_SEALED=true
    echo "[INFO] Reusing the exact previously sealed candidate runtime"
    return 0
  fi
  sudo -n python3 -B - "$RELEASE_DIR" <<'PY'
import os
from pathlib import Path
import shutil
import stat
import sys

root = Path(sys.argv[1])
if root.is_symlink() or not root.is_dir():
    raise SystemExit("[ERROR] candidate release root must be a real directory")
for relative in (".venv", "06_AppPlatform/frontend/dist"):
    target = root / relative
    try:
        metadata = target.lstat()
    except FileNotFoundError:
        continue
    if stat.S_ISDIR(metadata.st_mode) and not target.is_symlink():
        shutil.rmtree(target)
    else:
        target.unlink()
    descriptor = os.open(
        target.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
PY
  python3 -m venv --copies "$RELEASE_DIR/.venv"
  if [[ ! -x "$RELEASE_DIR/.venv/bin/python" ]]; then
    fail "trusted candidate virtualenv rebuild did not produce Python"
    return 1
  fi
}

verify_final_runtime_seal() {
  if sudo -n test -L "$RELEASE_RUNTIME_SEAL_FILE" \
    || ! sudo -n test -f "$RELEASE_RUNTIME_SEAL_FILE" \
    || ! python3 -B "$SOURCE_SEAL_HELPER" verify \
      --profile runtime \
      --root "$RELEASE_DIR" \
      --manifest "$RELEASE_RUNTIME_SEAL_FILE" \
      "${runtime_seal_identity_args[@]}"; then
    fail "candidate final runtime differs from its sealed venv/frontend payload"
    return 1
  fi
}

read_release_commit() {
  local metadata_path="$1"
  python3 - "$metadata_path" <<'PY'
import json
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
if not path.is_file() or path.is_symlink():
    raise SystemExit(1)
payload = json.loads(path.read_text(encoding="utf-8"))
value = str(payload.get("actualCommitSha") or payload.get("commitSha") or "")
if not re.fullmatch(r"[0-9a-f]{40}", value):
    raise SystemExit(1)
print(value)
PY
}

checkpoint_phase_status() {
  python3 -B "$CHECKPOINT_HELPER" show --checkpoint "$CHECKPOINT_FILE" \
    | python3 -c \
      'import json,sys; payload=json.load(sys.stdin); print(payload["phase"], payload["status"])'
}

read_checkpoint_phase_status() {
  local state=""
  state="$(checkpoint_phase_status)" || return 1
  if ! read -r CHECKPOINT_PHASE CHECKPOINT_STATUS <<< "$state" \
    || [[ -z "$CHECKPOINT_PHASE" || -z "$CHECKPOINT_STATUS" ]]; then
    fail "release checkpoint phase/status is unavailable"
    return 1
  fi
}

checkpoint_commits_candidate() {
  local phase="$1"
  local status="$2"
  case "$phase" in
    backend_healthy)
      [[ "$status" == "completed" ]]
      ;;
    www_verified|intl_deploy_started|intl_verified|parity_verified|complete)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

slot_release_root() {
  local slot="$1"
  local slot_link="$SLOTS_ROOT/$slot/current"
  if sudo -n test -L "$slot_link"; then
    sudo -n realpath "$slot_link"
    return
  fi
  if [[ "$slot" == "$CURRENT_ACTIVE_SLOT" ]] \
    && sudo -n test -d "$LEGACY_ROOT"; then
    printf '%s\n' "$LEGACY_ROOT"
    return
  fi
  return 1
}

resolve_previous_release_identity() {
  local metadata=""
  PREVIOUS_RELEASE_ROOT="$(slot_release_root "$CURRENT_ACTIVE_SLOT")" || return 1
  metadata="$PREVIOUS_RELEASE_ROOT/hermes/deploy_release.json"
  PREVIOUS_RELEASE_SHA="$(read_release_commit "$metadata")" || return 1
  if [[ "$PREVIOUS_RELEASE_SHA" == "$DEPLOY_COMMIT_SHA" ]]; then
    fail "previous slot unexpectedly reports the candidate SHA"
    return 1
  fi
}

verify_slot_release_exact() {
  local slot="$1"
  local expected_sha="$2"
  curl --noproxy '*' -fsS --max-time 20 \
    "http://127.0.0.1:${slot}/healthz" >/dev/null 2>&1 \
    && python3 -B "$READINESS_HELPER" \
      --url "http://127.0.0.1:${slot}/readyz" \
      --expected-commit "$expected_sha" \
      --timeout-seconds 20 >/dev/null
}

mark_maintenance_required() {
  atomic_text \
    "$DEPLOYMENT_MARKER" \
    "release=$DEPLOY_COMMIT_SHA status=reconciliation_required"
}

clear_maintenance_marker() {
  durable_remove_path "$DEPLOYMENT_MARKER"
}

resolve_existing_candidate_slot() {
  local matches=()
  local root=""
  local slot=""
  local env_path=""
  for slot in 8000 8001; do
    root="$(slot_release_root "$slot" 2>/dev/null || true)"
    env_path="$SLOT_ENV_ROOT/$slot.env"
    if [[ "$root" == "$RELEASE_DIR" ]] \
      && sudo -n test -f "$env_path" \
      && ! sudo -n test -L "$env_path" \
      && sudo -n grep -Fxq "APP_RELEASE_SHA=$DEPLOY_COMMIT_SHA" "$env_path"; then
      matches+=("$slot")
    fi
  done
  if [[ "${#matches[@]}" -ne 1 ]]; then
    fail "cannot prove exactly one slot belongs to the interrupted candidate"
    return 1
  fi
  CANDIDATE_SLOT="${matches[0]}"
  if [[ "$CANDIDATE_SLOT" == "8000" ]]; then
    CURRENT_ACTIVE_SLOT="8001"
  else
    CURRENT_ACTIVE_SLOT="8000"
  fi
}

ensure_current_slot_restartable() {
  local slot_link="$SLOTS_ROOT/$CURRENT_ACTIVE_SLOT/current"
  local env_target="$SLOT_ENV_ROOT/$CURRENT_ACTIVE_SLOT.env"
  local current_root="$LEGACY_ROOT"
  local current_sha=""
  local env_temp=""
  if sudo -n test -L "$slot_link"; then
    current_root="$(sudo -n realpath "$slot_link")"
  else
    atomic_symlink "$LEGACY_ROOT" "$slot_link"
  fi
  if sudo -n test -L "$env_target"; then
    fail "current slot env must not be a symlink: $env_target"
  fi
  if sudo -n test -f "$env_target"; then
    if ! sudo -n grep -Fxq "APP_RELEASE_SLOT=$CURRENT_ACTIVE_SLOT" "$env_target" \
      || ! sudo -n grep -Fxq "APP_JATO_MONTHLY_ENABLED=true" "$env_target" \
      || ! sudo -n grep -Fxq \
        "APP_JATO_MONTHLY_ACTIVE_SLOT_FILE=$ACTIVE_SLOT_FILE" "$env_target" \
      || ! sudo -n grep -Fxq \
        "APP_JATO_MONTHLY_DEPLOYMENT_MARKER=$DEPLOYMENT_MARKER" "$env_target"; then
      fail "current slot env does not satisfy the rollback contract: $env_target"
    fi
    return
  fi
  current_sha="$(read_release_commit "$current_root/hermes/deploy_release.json")" \
    || fail "current slot release SHA cannot be proven for rollback restart"
  env_temp="$(mktemp)"
  sed \
    -e "s|__SLOT__|$CURRENT_ACTIVE_SLOT|g" \
    -e "s|__RELEASE_SHA__|$current_sha|g" \
    -e "s|/opt/jato/slots/$CURRENT_ACTIVE_SLOT/current|$current_root|g" \
    "$SLOT_ENV_TEMPLATE" > "$env_temp"
  {
    printf '\nAPP_JATO_MONTHLY_ENABLED=true\n'
    printf '\nAPP_JATO_MONTHLY_UPDATE_JOB_ROOT=%s\n' "$JATO_JOB_ROOT"
    printf 'APP_JATO_MONTHLY_ACTIVE_SLOT_FILE=%s\n' "$ACTIVE_SLOT_FILE"
    printf 'APP_JATO_MONTHLY_DEPLOYMENT_MARKER=%s\n' "$DEPLOYMENT_MARKER"
    printf 'APP_JATO_MONTHLY_EXECUTION_MODE=subprocess\n'
  } >> "$env_temp"
  if ! durable_install_file "$env_temp" "$env_target" 0600; then
    rm -f "$env_temp"
    return 1
  fi
  rm -f "$env_temp"
}

preserve_previous_release_metadata() {
  if [[ -n "${PREVIOUS_DEPLOY_RELEASE_FILE:-}" ]]; then
    if [[ "$PREVIOUS_DEPLOY_RELEASE_FILE" != "$PREVIOUS_RELEASE_METADATA_PATH" ]] \
      || [[ ! -f "$PREVIOUS_DEPLOY_RELEASE_FILE" ]] \
      || [[ -L "$PREVIOUS_DEPLOY_RELEASE_FILE" ]]; then
      fail "previous release metadata override is not the candidate-scoped sidecar"
      return 1
    fi
    return 0
  fi
  local current_root=""
  local current_metadata=""
  current_root="$(slot_release_root "$CURRENT_ACTIVE_SLOT")" \
    || fail "current active release root is unavailable for metadata preservation" \
    || return 1
  current_metadata="$current_root/hermes/deploy_release.json"
  sudo -n python3 -B "$CHECKPOINT_HELPER" preserve-previous-metadata \
    --state-root "$BLUEGREEN_STATE_ROOT" \
    --source "$current_metadata" \
    --candidate-commit "$DEPLOY_COMMIT_SHA" \
    --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
    --owner-uid "$(id -u)" \
    --owner-gid "$(id -g)" >/dev/null \
    || return 1
  if [[ ! -f "$PREVIOUS_RELEASE_METADATA_PATH" ]] \
    || [[ -L "$PREVIOUS_RELEASE_METADATA_PATH" ]]; then
    fail "candidate-scoped previous release metadata was not durably created"
    return 1
  fi
  PREVIOUS_DEPLOY_RELEASE_FILE="$PREVIOUS_RELEASE_METADATA_PATH"
  export PREVIOUS_DEPLOY_RELEASE_FILE
}

run_inner_prepare() {
  if [[ ! -x "$INNER_DEPLOY" ]]; then
    fail "candidate inner deploy script is missing: $INNER_DEPLOY"
  fi
  REPO_DIR="$RELEASE_DIR" \
  SKIP_GIT_SYNC=true \
  DEPLOY_PRUNE_UNTRACKED=false \
  BLUEGREEN_PREPARE_ONLY=true \
  RUN_DATABASE_MIGRATIONS=false \
  RUN_GROUPED_TIME_SERIES_PREWARM=false \
  PRODUCTION_RELEASE_WORKFLOW=true \
  PREBUILT_FRONTEND_DIR="$PREBUILT_FRONTEND_DIR" \
  PREVIOUS_DEPLOY_RELEASE_FILE="${PREVIOUS_DEPLOY_RELEASE_FILE:-}" \
  BACKEND_SERVICE_NAME="${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
  BACKEND_PORT="$CANDIDATE_SLOT" \
  RELEASE_CHECKPOINT_FILE="$CHECKPOINT_FILE" \
  RELEASE_CHECKPOINT_JOURNAL="$CHECKPOINT_JOURNAL" \
  RELEASE_CHECKPOINT_REPOSITORY="$DEPLOY_REPOSITORY" \
  RELEASE_CHECKPOINT_COMMIT="$DEPLOY_COMMIT_SHA" \
  RELEASE_CHECKPOINT_ARCHIVE_SHA256="$DEPLOY_ARCHIVE_SHA256" \
  RELEASE_CHECKPOINT_ARCHIVE_BYTES="$DEPLOY_ARCHIVE_BYTES" \
  RELEASE_CHECKPOINT_RUN_ID="$DEPLOY_RUN_ID" \
  RELEASE_CHECKPOINT_RUN_ATTEMPT="$DEPLOY_RUN_ATTEMPT" \
  RELEASE_CHECKPOINT_FRONTEND_IDENTITY="$FRONTEND_ARTIFACT_IDENTITY" \
  RELEASE_CHECKPOINT_FRONTEND_CHECKSUM="$FRONTEND_ARTIFACT_CHECKSUM" \
    bash "$INNER_DEPLOY"
}

candidate_build_scope_unit_name() {
  printf 'jato-bluegreen-candidate-build.scope\n'
}

assert_candidate_build_scope() {
  local actual_group=""
  local actual_high=""
  local actual_max=""
  local actual_tasks=""
  local expected_high=$((3 * 1024 * 1024 * 1024))
  local expected_max=$((4 * 1024 * 1024 * 1024))
  local unit=""
  unit="$(candidate_build_scope_unit_name)"
  if [[ ! "${BLUEGREEN_CANDIDATE_BUILD_UID:-}" =~ ^[0-9]+$ ]] \
    || [[ ! "${BLUEGREEN_CANDIDATE_BUILD_GID:-}" =~ ^[0-9]+$ ]] \
    || [[ "$(id -u)" != "$BLUEGREEN_CANDIDATE_BUILD_UID" ]] \
    || [[ "$(id -g)" != "$BLUEGREEN_CANDIDATE_BUILD_GID" ]]; then
    fail "candidate build scope did not retain the deploy user identity"
    return 1
  fi
  actual_high="$(systemctl show "$unit" -p MemoryHigh --value)" || return 1
  actual_max="$(systemctl show "$unit" -p MemoryMax --value)" || return 1
  actual_tasks="$(systemctl show "$unit" -p TasksMax --value)" || return 1
  actual_group="$(systemctl show "$unit" -p ControlGroup --value)" || return 1
  if [[ "$BLUEGREEN_CANDIDATE_MEMORY_HIGH" != "3G" ]] \
    || [[ "$actual_high" != "$expected_high" ]] \
    || [[ "$BLUEGREEN_CANDIDATE_MEMORY_MAX" != "4G" ]] \
    || [[ "$actual_max" != "$expected_max" ]] \
    || [[ "$actual_tasks" != "512" ]] \
    || [[ -z "$actual_group" || "$actual_group" == "/" ]]; then
    fail "candidate build scope resource limits are not the reviewed 3G/4G/512 contract"
    return 1
  fi
  python3 -B - "$actual_group" "$$" <<'PY'
from pathlib import Path
import sys

group = sys.argv[1]
pid = sys.argv[2]
if ".." in Path(group).parts:
    raise SystemExit("[ERROR] candidate build scope control group is unsafe")
members = Path("/sys/fs/cgroup") / group.lstrip("/") / "cgroup.procs"
try:
    processes = set(members.read_text(encoding="utf-8").splitlines())
except OSError as exc:
    raise SystemExit(
        f"[ERROR] cannot verify candidate build scope membership: {exc}"
    ) from exc
if pid not in processes:
    raise SystemExit("[ERROR] candidate build shell is outside its resource scope")
PY
}

build_candidate_runtime_locked() {
  require_environment
  assert_candidate_build_scope
  assert_inherited_production_lock
  if [[ "$CURRENT_ACTIVE_SLOT" != "8000" && "$CURRENT_ACTIVE_SLOT" != "8001" ]] \
    || [[ "$CANDIDATE_SLOT" != "8000" && "$CANDIDATE_SLOT" != "8001" ]] \
    || [[ "$CURRENT_ACTIVE_SLOT" == "$CANDIDATE_SLOT" ]]; then
    fail "candidate build slot identity is invalid"
    return 1
  fi
  if [[ -n "${PREVIOUS_DEPLOY_RELEASE_FILE:-}" ]] \
    && {
      [[ ! -f "$PREVIOUS_DEPLOY_RELEASE_FILE" ]] \
        || [[ -L "$PREVIOUS_DEPLOY_RELEASE_FILE" ]];
    }; then
    fail "candidate build previous release metadata is missing or unsafe"
    return 1
  fi
  prepare_candidate_runtime
  if [[ "$RUNTIME_ALREADY_SEALED" != "true" ]]; then
    run_inner_prepare
    verify_materialized_release_source
    assert_no_database_migration_delta
    write_candidate_deploy_status
    finalize_runtime_seal
  else
    assert_no_database_migration_delta
  fi
  verify_final_runtime_seal
}

run_candidate_build_scope() {
  local bash_bin=""
  local controller="$RELEASE_DIR/03_Scripts/deploy/tencent_bluegreen_release.sh"
  local unit=""
  if ! [[ "$BLUEGREEN_CANDIDATE_BUILD_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    fail "candidate build timeout must be a positive integer"
    return 1
  fi
  if [[ -z "${HOME:-}" || "$HOME" != /* ]] \
    || [[ -z "${PATH:-}" ]]; then
    fail "candidate build requires an absolute HOME and non-empty PATH"
    return 1
  fi
  if [[ ! -f "$controller" || -L "$controller" ]]; then
    fail "sealed candidate build controller is missing or unsafe"
    return 1
  fi
  bash_bin="$(command -v bash)" || return 1
  unit="$(candidate_build_scope_unit_name)"
  sudo -n systemd-run \
    --quiet \
    --collect \
    --scope \
    --unit="$unit" \
    --uid="$(id -u)" \
    --gid="$(id -g)" \
    --working-directory="$RELEASE_DIR" \
    --property="RuntimeMaxSec=${BLUEGREEN_CANDIDATE_BUILD_TIMEOUT}s" \
    --property="MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH" \
    --property="MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX" \
    --property="CPUQuota=100%" \
    --property="TasksMax=512" \
    --setenv="HOME=$HOME" \
    --setenv="PATH=$PATH" \
    --setenv="BLUEGREEN_MODE=build-candidate-runtime" \
    --setenv="BLUEGREEN_CANDIDATE_BUILD_UID=$(id -u)" \
    --setenv="BLUEGREEN_CANDIDATE_BUILD_GID=$(id -g)" \
    --setenv="BLUEGREEN_ROOT=$BLUEGREEN_ROOT" \
    --setenv="RELEASES_ROOT=$RELEASES_ROOT" \
    --setenv="SLOTS_ROOT=$SLOTS_ROOT" \
    --setenv="SHARED_ROOT=$SHARED_ROOT" \
    --setenv="ACTIVE_RELEASE_LINK=$ACTIVE_RELEASE_LINK" \
    --setenv="BLUEGREEN_STATE_ROOT=$BLUEGREEN_STATE_ROOT" \
    --setenv="ACTIVE_SLOT_FILE=$ACTIVE_SLOT_FILE" \
    --setenv="DEPLOYMENT_MARKER=$DEPLOYMENT_MARKER" \
    --setenv="NGINX_ACTIVE_RELEASE_CONF=$NGINX_ACTIVE_RELEASE_CONF" \
    --setenv="SLOT_ENV_ROOT=$SLOT_ENV_ROOT" \
    --setenv="BACKEND_ENV_FILE=$BACKEND_ENV_FILE" \
    --setenv="LEGACY_ROOT=$LEGACY_ROOT" \
    --setenv="JATO_JOB_ROOT=$JATO_JOB_ROOT" \
    --setenv="BLUEGREEN_CANDIDATE_MEMORY_HIGH=$BLUEGREEN_CANDIDATE_MEMORY_HIGH" \
    --setenv="BLUEGREEN_CANDIDATE_MEMORY_MAX=$BLUEGREEN_CANDIDATE_MEMORY_MAX" \
    --setenv="BLUEGREEN_CANDIDATE_BUILD_TIMEOUT=$BLUEGREEN_CANDIDATE_BUILD_TIMEOUT" \
    --setenv="BLUEGREEN_ACTIVE_MEMORY_HIGH=$BLUEGREEN_ACTIVE_MEMORY_HIGH" \
    --setenv="BLUEGREEN_ACTIVE_MEMORY_MAX=$BLUEGREEN_ACTIVE_MEMORY_MAX" \
    --setenv="BLUEGREEN_FAULT=$BLUEGREEN_FAULT" \
    --setenv="DEPLOY_COMMIT_SHA=$DEPLOY_COMMIT_SHA" \
    --setenv="DEPLOY_ARCHIVE_SHA256=$DEPLOY_ARCHIVE_SHA256" \
    --setenv="DEPLOY_ARCHIVE_BYTES=$DEPLOY_ARCHIVE_BYTES" \
    --setenv="DEPLOY_REPOSITORY=$DEPLOY_REPOSITORY" \
    --setenv="DEPLOY_RUN_ID=$DEPLOY_RUN_ID" \
    --setenv="DEPLOY_RUN_ATTEMPT=$DEPLOY_RUN_ATTEMPT" \
    --setenv="DEPLOY_BRANCH=$DEPLOY_BRANCH" \
    --setenv="DEPLOY_SERVER_NAME=${DEPLOY_SERVER_NAME:-_}" \
    --setenv="FRONTEND_ARTIFACT_IDENTITY=$FRONTEND_ARTIFACT_IDENTITY" \
    --setenv="FRONTEND_ARTIFACT_CHECKSUM=$FRONTEND_ARTIFACT_CHECKSUM" \
    --setenv="RELEASE_WORKTREE=$RELEASE_WORKTREE" \
    --setenv="PREBUILT_FRONTEND_DIR=$PREBUILT_FRONTEND_DIR" \
    --setenv="CHECKPOINT_FILE=$CHECKPOINT_FILE" \
    --setenv="CHECKPOINT_JOURNAL=$CHECKPOINT_JOURNAL" \
    --setenv="CURRENT_ACTIVE_SLOT=$CURRENT_ACTIVE_SLOT" \
    --setenv="CANDIDATE_SLOT=$CANDIDATE_SLOT" \
    --setenv="PREVIOUS_DEPLOY_RELEASE_FILE=${PREVIOUS_DEPLOY_RELEASE_FILE:-}" \
    --setenv="DEPLOY_STATE_DIR=${DEPLOY_STATE_DIR:-}" \
    --setenv="DEPLOY_LOCK_PATH=${DEPLOY_LOCK_PATH:-}" \
    --setenv="DEPLOY_LOCK_HELD=${DEPLOY_LOCK_HELD:-}" \
    --setenv="DEPLOY_LOCK_HOLDER_PID=${DEPLOY_LOCK_HOLDER_PID:-}" \
    --setenv="DEPLOY_LOCK_FD=${DEPLOY_LOCK_FD:-}" \
    "$bash_bin" "$controller" build-candidate-runtime
}

assert_no_database_migration_delta() {
  local enabled=""
  local current=""
  local heads=""
  if ! sudo -n test -f "$BACKEND_ENV_FILE"; then
    fail "backend env is required for a production blue/green release"
  fi
  enabled="$(
    sudo -n bash -c \
      'set -a; . "$1"; set +a; printf "%s" "${APP_DATABASE_ENABLED:-false}"' \
      _ "$BACKEND_ENV_FILE"
  )"
  if ! is_truthy "$enabled"; then
    echo "[INFO] Database is disabled; no Alembic compatibility gate is needed"
    return
  fi
  current="$(
    sudo -n bash -c \
      'set -Eeuo pipefail; set -a; . "$1"; set +a; export PYTHONPATH="$2"; cd "$2"; "$3" -m alembic current' \
      _ "$BACKEND_ENV_FILE" "$RELEASE_DIR/06_AppPlatform/backend" "$RELEASE_DIR/.venv/bin/python"
  )"
  heads="$(
    sudo -n bash -c \
      'set -Eeuo pipefail; export PYTHONPATH="$1"; cd "$1"; "$2" -m alembic heads' \
      _ "$RELEASE_DIR/06_AppPlatform/backend" "$RELEASE_DIR/.venv/bin/python"
  )"
  python3 - "$current" "$heads" <<'PY'
import re
import sys

pattern = re.compile(r"(?m)^([0-9]{8}_[0-9]{4})\b")
current = set(pattern.findall(sys.argv[1]))
heads = set(pattern.findall(sys.argv[2]))
if not current or current != heads:
    raise SystemExit(
        "[ERROR] Blue/green v1 forbids Alembic changes; use an expand/contract "
        f"migration release first (current={sorted(current)} heads={sorted(heads)})"
    )
PY
}

install_slot_runtime() {
  local service_target="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"
  local env_target="$SLOT_ENV_ROOT/$CANDIDATE_SLOT.env"
  local sandbox_cache="/var/cache/jato-candidate-$CANDIDATE_SLOT"
  local sandbox_dropin="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.d/10-candidate-sandbox.conf"
  local sandbox_temp=""
  local env_temp=""
  durable_install_file "$SYSTEMD_TEMPLATE" "$service_target" 0644
  env_temp="$(mktemp)"
  sed \
    -e "s|__SLOT__|$CANDIDATE_SLOT|g" \
    -e "s|__RELEASE_SHA__|$DEPLOY_COMMIT_SHA|g" \
    "$SLOT_ENV_TEMPLATE" > "$env_temp"
  {
    printf '\nAPP_JATO_MONTHLY_ENABLED=true\n'
    printf 'APP_JATO_MONTHLY_UPDATE_JOB_ROOT=%s\n' "$JATO_JOB_ROOT"
    printf 'APP_JATO_MONTHLY_ACTIVE_SLOT_FILE=%s\n' "$ACTIVE_SLOT_FILE"
    printf 'APP_JATO_MONTHLY_DEPLOYMENT_MARKER=%s\n' "$DEPLOYMENT_MARKER"
    printf 'APP_JATO_MONTHLY_EXECUTION_MODE=subprocess\n'
  } >> "$env_temp"
  if ! durable_install_file "$env_temp" "$env_target" 0600; then
    rm -f "$env_temp"
    return 1
  fi
  rm -f "$env_temp"
  sandbox_temp="$(mktemp)"
  {
    echo "[Service]"
    echo "DynamicUser=yes"
    echo "ProtectSystem=strict"
    echo "ProtectHome=true"
    echo "PrivateTmp=true"
    echo "PrivateDevices=true"
    echo "NoNewPrivileges=true"
    echo "CapabilityBoundingSet="
    echo "AmbientCapabilities="
    echo "RestrictNamespaces=true"
    echo "ProtectKernelTunables=true"
    echo "ProtectKernelModules=true"
    echo "ProtectKernelLogs=true"
    echo "ProtectControlGroups=true"
    echo "LockPersonality=true"
    echo "RestrictRealtime=true"
    echo "RestrictSUIDSGID=true"
    printf 'CacheDirectory=jato-candidate-%s\n' "$CANDIDATE_SLOT"
    printf 'Environment=HOME=%s\n' "$sandbox_cache"
    printf 'Environment=XDG_CACHE_HOME=%s\n' "$sandbox_cache"
    echo 'Environment="PGOPTIONS=-c default_transaction_read_only=on"'
    echo "Environment=APP_REDIS_ENABLED=false"
    printf 'ReadOnlyPaths=%s %s\n' "$SHARED_ROOT" "$BLUEGREEN_STATE_ROOT"
    printf 'ReadWritePaths=%s\n' "$sandbox_cache"
  } > "$sandbox_temp"
  if ! durable_install_file "$sandbox_temp" "$sandbox_dropin" 0644; then
    rm -f "$sandbox_temp"
    return 1
  fi
  rm -f "$sandbox_temp"
  PRE_SUPERVISOR_CANDIDATE_ARMED=true
  sudo -n systemctl stop "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 || true
  atomic_symlink "$RELEASE_DIR" "$SLOTS_ROOT/$CANDIDATE_SLOT/current"
  sudo -n systemctl daemon-reload
  sudo -n systemctl disable "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState inactive
  sudo -n systemctl set-property "${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
    "MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH" \
    "MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX" \
    "CPUQuota=100%"
}

unit_property_equals() {
  local unit="$1"
  local property="$2"
  local expected="$3"
  local actual=""
  actual="$(systemctl show "$unit" -p "$property" --value)" || return 1
  if [[ "$actual" != "$expected" ]]; then
    fail "$unit $property mismatch: expected=$expected actual=${actual:-empty}"
    return 1
  fi
}

systemd_property_contains_unit() {
  local dependency="$3"
  local property="$2"
  local unit="$1"
  local value=""
  local token=""
  value="$(systemctl show "$unit" -p "$property" --value)" || return 1
  for token in $value; do
    if [[ "$token" == "$dependency" ]]; then
      return 0
    fi
  done
  fail "$unit $property does not contain required unit $dependency"
}

verify_boot_reconciler_installation() {
  sudo -n cmp -s "$BOOT_RECONCILE_HELPER" "$BOOT_RECONCILE_HELPER_TARGET" \
    || fail "boot reconciler helper failed durable read-back" \
    || return 1
  sudo -n cmp -s \
    "$BOOT_RECONCILE_UNIT_TEMPLATE" "$BOOT_RECONCILE_UNIT_TARGET" \
    || fail "boot reconciler unit failed durable read-back" \
    || return 1
  sudo -n cmp -s \
    "$NGINX_BOOT_DROPIN_TEMPLATE" "$NGINX_BOOT_DROPIN_TARGET" \
    || fail "Nginx boot dependency failed durable read-back" \
    || return 1
  unit_property_equals \
    "$BOOT_RECONCILE_UNIT" FragmentPath "$BOOT_RECONCILE_UNIT_TARGET" \
    || return 1
  unit_property_equals "$BOOT_RECONCILE_UNIT" UnitFileState enabled || return 1
  systemd_property_contains_unit \
    nginx.service Requires "$BOOT_RECONCILE_UNIT" || return 1
  systemd_property_contains_unit \
    nginx.service After "$BOOT_RECONCILE_UNIT" || return 1
}

install_boot_reconciler() {
  if [[ ! -f "$BOOT_RECONCILE_HELPER" || -L "$BOOT_RECONCILE_HELPER" ]] \
    || [[ ! -f "$BOOT_RECONCILE_UNIT_TEMPLATE" \
      || -L "$BOOT_RECONCILE_UNIT_TEMPLATE" ]] \
    || [[ ! -f "$NGINX_BOOT_DROPIN_TEMPLATE" \
      || -L "$NGINX_BOOT_DROPIN_TEMPLATE" ]]; then
    fail "blue/green boot reconciler source files are missing or unsafe"
    return 1
  fi
  durable_install_file \
    "$BOOT_RECONCILE_HELPER" "$BOOT_RECONCILE_HELPER_TARGET" 0755 \
    || return 1
  durable_install_file \
    "$BOOT_RECONCILE_UNIT_TEMPLATE" "$BOOT_RECONCILE_UNIT_TARGET" 0644 \
    || return 1
  durable_install_file \
    "$NGINX_BOOT_DROPIN_TEMPLATE" "$NGINX_BOOT_DROPIN_TARGET" 0644 \
    || return 1
  sudo -n systemctl daemon-reload || return 1
  sudo -n systemctl enable "$BOOT_RECONCILE_UNIT" >/dev/null || return 1
  verify_boot_reconciler_installation
}

verify_stable_current_nginx_route() {
  local expected_conf=""
  resolve_previous_release_identity \
    || fail "previous release metadata is unavailable for the stable Nginx route" \
    || return 1
  verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA" \
    || fail "current slot does not serve the exact previous release" \
    || return 1
  if sudo -n test -L "$NGINX_ACTIVE_RELEASE_CONF" \
    || ! sudo -n test -f "$NGINX_ACTIVE_RELEASE_CONF"; then
    fail "stable Nginx active release include is missing or unsafe"
    return 1
  fi
  expected_conf="$(mktemp)" || return 1
  if ! render_active_release \
    "$expected_conf" "$CURRENT_ACTIVE_SLOT" "$CURRENT_FRONTEND_ROOT"; then
    rm -f "$expected_conf"
    return 1
  fi
  if ! sudo -n cmp -s "$expected_conf" "$NGINX_ACTIVE_RELEASE_CONF"; then
    rm -f "$expected_conf"
    fail "stable Nginx route does not bind the current backend and frontend"
    return 1
  fi
  rm -f "$expected_conf"
  verify_public_release_exact "$PREVIOUS_RELEASE_SHA" \
    || fail "stable public route does not serve the exact previous release"
}

run_stable_nginx_installer() {
  sudo -n DEPLOY_STATE_DIR="${DEPLOY_STATE_DIR:-}" \
    JATO_PRODUCTION_DEPLOY_LOCK_PATH="${DEPLOY_LOCK_PATH:-}" \
    DEPLOY_LOCK_HELD="${DEPLOY_LOCK_HELD:-}" \
    DEPLOY_LOCK_HOLDER_PID="${DEPLOY_LOCK_HOLDER_PID:-}" \
    DEPLOY_LOCK_FD="${DEPLOY_LOCK_FD:-}" \
    NGINX_PREIMAGE_DIR="$NGINX_PREIMAGE_DIR" \
    SERVER_NAME="${DEPLOY_SERVER_NAME:-_}" \
    BACKEND_PORT="$CURRENT_ACTIVE_SLOT" \
    FRONTEND_ROOT="$CURRENT_FRONTEND_ROOT" \
    SKIP_HEALTH_CHECK=true \
    bash "$NGINX_INSTALLER"
}

prepare_stable_nginx_boot_infrastructure() {
  if [[ -e "$NGINX_PREIMAGE_DIR" || -L "$NGINX_PREIMAGE_DIR" ]]; then
    if [[ ! -d "$NGINX_PREIMAGE_DIR" || -L "$NGINX_PREIMAGE_DIR" ]]; then
      fail "durable Nginx preimage is unsafe: $NGINX_PREIMAGE_DIR"
      return 1
    fi
    # active-release.conf is written before the stable site configuration.
    # Its presence alone therefore cannot prove that an interrupted installer
    # completed.  Always restore the exact preimage and replay the full
    # installer before committing the boot dependency.
    restore_nginx_preimage || return 1
    remove_nginx_preimage || return 1
  fi
  run_stable_nginx_installer || return 1
  verify_stable_current_nginx_route || return 1

  # Once the exact old release is durably routed, this migration is committed
  # infrastructure.  Candidate failures must retain it: removing the preimage
  # before adding Nginx's boot dependency makes every interruption window safe.
  remove_nginx_preimage || return 1
  install_boot_reconciler || return 1
  verify_boot_reconciler_installation || return 1
  verify_stable_current_nginx_route
}

arm_pre_switch_static_boot_safety() {
  verify_boot_reconciler_installation || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState active || return 1
  sudo -n systemctl disable \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" >/dev/null || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState disabled || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState active || return 1
}

restore_old_static_boot_owner() {
  sudo -n systemctl enable \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" >/dev/null || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState enabled || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState active
}

capture_backend_template_preimage() {
  local state=""
  local state_temp=""
  if sudo -n test -e "$BACKEND_TEMPLATE_PREIMAGE_STATE" \
    || sudo -n test -L "$BACKEND_TEMPLATE_PREIMAGE_STATE"; then
    if sudo -n test -L "$BACKEND_TEMPLATE_PREIMAGE_STATE" \
      || ! sudo -n test -f "$BACKEND_TEMPLATE_PREIMAGE_STATE"; then
      fail "backend template preimage state is unsafe"
      return 1
    fi
    state="$(sudo -n cat "$BACKEND_TEMPLATE_PREIMAGE_STATE")"
    if [[ "$state" == "present" ]]; then
      sudo -n test -f "$BACKEND_TEMPLATE_PREIMAGE" \
        && ! sudo -n test -L "$BACKEND_TEMPLATE_PREIMAGE"
      return
    fi
    [[ "$state" == "absent" ]]
    return
  fi
  state_temp="$(mktemp)"
  if sudo -n test -L "$SHARED_BACKEND_TEMPLATE"; then
    rm -f "$state_temp"
    fail "shared backend template must not be a symlink"
    return 1
  fi
  if sudo -n test -f "$SHARED_BACKEND_TEMPLATE"; then
    durable_install_file \
      "$SHARED_BACKEND_TEMPLATE" "$BACKEND_TEMPLATE_PREIMAGE" 0600 \
      || {
        rm -f "$state_temp"
        return 1
      }
    printf 'present\n' > "$state_temp"
  else
    printf 'absent\n' > "$state_temp"
  fi
  if ! durable_install_file \
    "$state_temp" "$BACKEND_TEMPLATE_PREIMAGE_STATE" 0600; then
    rm -f "$state_temp"
    return 1
  fi
  rm -f "$state_temp"
}

restore_backend_template_preimage() {
  local state=""
  if ! sudo -n test -e "$BACKEND_TEMPLATE_PREIMAGE_STATE" \
    && ! sudo -n test -L "$BACKEND_TEMPLATE_PREIMAGE_STATE"; then
    return 0
  fi
  if sudo -n test -L "$BACKEND_TEMPLATE_PREIMAGE_STATE" \
    || ! sudo -n test -f "$BACKEND_TEMPLATE_PREIMAGE_STATE"; then
    fail "backend template preimage state is unsafe"
    return 1
  fi
  state="$(sudo -n cat "$BACKEND_TEMPLATE_PREIMAGE_STATE")"
  case "$state" in
    present)
      if sudo -n test -L "$BACKEND_TEMPLATE_PREIMAGE" \
        || ! sudo -n test -f "$BACKEND_TEMPLATE_PREIMAGE"; then
        fail "backend template preimage is missing or unsafe"
        return 1
      fi
      durable_install_file \
        "$BACKEND_TEMPLATE_PREIMAGE" "$SHARED_BACKEND_TEMPLATE" 0644 \
        || return 1
      ;;
    absent)
      durable_remove_path "$SHARED_BACKEND_TEMPLATE" || return 1
      ;;
    *)
      fail "backend template preimage state is invalid"
      return 1
      ;;
  esac
  sudo -n systemctl daemon-reload
}

remove_backend_template_preimage() {
  durable_remove_path "$BACKEND_TEMPLATE_PREIMAGE" || return 1
  durable_remove_path "$BACKEND_TEMPLATE_PREIMAGE_STATE"
}

verify_candidate_reboot_gate() {
  local candidate_unit="${SERVICE_PREFIX}${CANDIDATE_SLOT}"
  unit_property_equals "$candidate_unit" ActiveState active || return 1
  unit_property_equals "$candidate_unit" UnitFileState enabled || return 1
  unit_property_equals \
    "$candidate_unit" FragmentPath "$SHARED_BACKEND_TEMPLATE" || return 1
  sudo -n cmp -s "$SYSTEMD_TEMPLATE" "$SHARED_BACKEND_TEMPLATE" \
    || fail "candidate reboot template differs from the target release" \
    || return 1
}

commit_backend_unit_template() {
  local explicit_candidate="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"
  local candidate_unit="${SERVICE_PREFIX}${CANDIDATE_SLOT}"
  capture_backend_template_preimage || return 1
  durable_install_file "$SYSTEMD_TEMPLATE" "$SHARED_BACKEND_TEMPLATE" 0644 \
    || return 1
  unit_property_equals "$candidate_unit" ActiveState active || return 1
  if [[ "$(
    systemctl show "$candidate_unit" -p FragmentPath --value
  )" == "$SHARED_BACKEND_TEMPLATE" ]]; then
    sudo -n systemctl enable "$candidate_unit" >/dev/null || return 1
    verify_candidate_reboot_gate
    return
  fi
  unit_property_equals \
    "$candidate_unit" FragmentPath "$explicit_candidate" || return 1
  sudo -n systemctl disable "$candidate_unit" >/dev/null || return 1
  unit_property_equals "$candidate_unit" UnitFileState disabled || return 1
  unit_property_equals "$candidate_unit" ActiveState active || return 1
  durable_remove_path "$explicit_candidate" || return 1
  sudo -n systemctl daemon-reload || return 1
  unit_property_equals \
    "$candidate_unit" FragmentPath "$SHARED_BACKEND_TEMPLATE" || return 1
  sudo -n systemctl enable "$candidate_unit" >/dev/null || return 1
  verify_candidate_reboot_gate
}

verify_candidate_sandbox() {
  local dropin="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.d/10-candidate-sandbox.conf"
  local ambient_capabilities=""
  local capability_bounding_set=""
  local dynamic_user=""
  local environment=""
  local main_pid=""
  local no_new_privileges=""
  local private_devices=""
  local private_tmp=""
  local protect_system=""
  local read_only_paths=""
  local read_write_paths=""
  local restrict_namespaces=""
  if sudo -n test -L "$dropin" || ! sudo -n test -f "$dropin"; then
    fail "candidate sandbox drop-in is missing or unsafe"
    return 1
  fi
  protect_system="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p ProtectSystem --value
  )"
  dynamic_user="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p DynamicUser --value
  )"
  private_tmp="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p PrivateTmp --value
  )"
  private_devices="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p PrivateDevices --value
  )"
  no_new_privileges="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p NoNewPrivileges --value
  )"
  capability_bounding_set="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p CapabilityBoundingSet --value
  )"
  ambient_capabilities="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p AmbientCapabilities --value
  )"
  restrict_namespaces="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p RestrictNamespaces --value
  )"
  environment="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p Environment --value
  )"
  read_only_paths="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p ReadOnlyPaths --value
  )"
  read_write_paths="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p ReadWritePaths --value
  )"
  main_pid="$(
    systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p MainPID --value
  )"
  if [[ "$dynamic_user" != "yes" ]] \
    || [[ "$protect_system" != "strict" ]] \
    || [[ "$private_tmp" != "yes" ]] \
    || [[ "$private_devices" != "yes" ]] \
    || [[ "$no_new_privileges" != "yes" ]] \
    || [[ -n "$capability_bounding_set" ]] \
    || [[ -n "$ambient_capabilities" ]] \
    || [[ "$restrict_namespaces" != "yes" ]] \
    || [[ "$environment" != *"PGOPTIONS=-c default_transaction_read_only=on"* ]] \
    || [[ "$environment" != *"APP_REDIS_ENABLED=false"* ]] \
    || [[ "$read_only_paths" != *"$SHARED_ROOT"* ]] \
    || [[ "$read_only_paths" != *"$BLUEGREEN_STATE_ROOT"* ]] \
    || [[ "$read_write_paths" != *"/var/cache/jato-candidate-$CANDIDATE_SLOT"* ]]; then
    fail "candidate systemd sandbox does not match the read-only data/state contract"
    return 1
  fi
  if [[ ! "$main_pid" =~ ^[1-9][0-9]*$ ]] \
    || ! sudo -n python3 -B - \
      "$main_pid" "/var/cache/jato-candidate-$CANDIDATE_SLOT" <<'PY'
import os
from pathlib import Path
import sys

status: dict[str, str] = {}
for line in (Path("/proc") / sys.argv[1] / "status").read_text(
    encoding="utf-8"
).splitlines():
    key, separator, value = line.partition(":")
    if separator:
        status[key] = value.strip()
uids = status.get("Uid", "").split()
if len(uids) != 4 or any(uid == "0" for uid in uids):
    raise SystemExit("[ERROR] candidate backend did not run as a non-root dynamic user")
if int(status.get("CapEff", "-1"), 16) != 0:
    raise SystemExit("[ERROR] candidate backend retained effective Linux capabilities")
if int(status.get("CapBnd", "-1"), 16) != 0:
    raise SystemExit("[ERROR] candidate backend capability bounding set is not empty")
if status.get("NoNewPrivs") != "1":
    raise SystemExit("[ERROR] candidate backend lacks no-new-privileges enforcement")
environment = (
    Path("/proc") / sys.argv[1] / "environ"
).read_bytes().split(b"\0")
if b"PGOPTIONS=-c default_transaction_read_only=on" not in environment:
    raise SystemExit("[ERROR] candidate backend lacks PostgreSQL read-only PGOPTIONS")
if b"APP_REDIS_ENABLED=false" not in environment:
    raise SystemExit("[ERROR] candidate backend did not disable shared Redis writes")
gids = status.get("Gid", "").split()
if len(gids) != 4 or any(gid == "0" for gid in gids):
    raise SystemExit("[ERROR] candidate backend did not run with a non-root group")
cache = Path(sys.argv[2])
probe = cache / ".jato-candidate-write-probe"
os.setgroups([])
os.setgid(int(gids[1]))
os.setuid(int(uids[1]))
probe.write_text("candidate-cache-write-ok\n", encoding="utf-8")
probe.unlink()
PY
  then
    return 1
  fi
}

remove_candidate_sandbox_before_switch() {
  local dropin="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.d/10-candidate-sandbox.conf"
  verify_final_runtime_seal || return 1
  durable_remove_path "$dropin" || return 1
  sudo -n systemctl daemon-reload || return 1
  sudo -n systemctl restart "${SERVICE_PREFIX}${CANDIDATE_SLOT}" || return 1
  sudo -n systemctl set-property "${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
    "MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH" \
    "MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX" \
    "CPUQuota=100%" || return 1
  if sudo -n test -e "$dropin" || sudo -n test -L "$dropin"; then
    fail "candidate sandbox drop-in remained after activation restart"
    return 1
  fi
  verify_candidate || return 1
}

release_storage_reference_args() {
  local current_root=""
  local path=""
  current_root="$(slot_release_root "$CURRENT_ACTIVE_SLOT")" \
    || fail "current active release root is unavailable for storage protection" \
    || return 1
  if sudo -n test -e "$ACTIVE_RELEASE_LINK" \
    || sudo -n test -L "$ACTIVE_RELEASE_LINK"; then
    if ! sudo -n test -L "$ACTIVE_RELEASE_LINK" \
      || [[ "$(sudo -n realpath "$ACTIVE_RELEASE_LINK")" != "$current_root" ]]; then
      fail "active release link differs from the controller active slot"
      return 1
    fi
  fi
  for path in \
    "$current_root" \
    "$CURRENT_FRONTEND_ROOT" \
    "$ACTIVE_RELEASE_LINK" \
    "$SLOTS_ROOT/8000/current" \
    "$SLOTS_ROOT/8001/current"; do
    if sudo -n test -e "$path" || sudo -n test -L "$path"; then
      printf '%s\n' "$path"
    fi
  done
}

run_release_storage_guard() {
  local minimum_bytes="$1"
  local minimum_percent="$2"
  local check_only="${3:-false}"
  local current_root=""
  local legacy_option=()
  local protected_args=()
  local protected_output=""
  local protected_root=""
  if [[ ! -f "$RELEASE_STORAGE_GUARD" ]] \
    || [[ -L "$RELEASE_STORAGE_GUARD" ]]; then
    fail "release storage guard is missing or unsafe"
    return 1
  fi
  current_root="$(slot_release_root "$CURRENT_ACTIVE_SLOT")" \
    || fail "current active release root is unavailable for storage protection" \
    || return 1
  if sudo -n test -L "$NGINX_ACTIVE_RELEASE_CONF"; then
    fail "Nginx active release include must not be a symlink"
    return 1
  fi
  if ! sudo -n test -f "$NGINX_ACTIVE_RELEASE_CONF"; then
    if [[ "$current_root" != "$LEGACY_ROOT" ]]; then
      fail "Nginx release include is absent after immutable releases became active"
      return 1
    fi
    legacy_option+=(--allow-missing-nginx-legacy)
  fi
  protected_output="$(release_storage_reference_args)" || return 1
  while IFS= read -r protected_root; do
    if [[ -n "$protected_root" ]]; then
      protected_args+=(--protected-root "$protected_root")
    fi
  done <<< "$protected_output"

  local check_only_args=()
  if [[ "$check_only" == "true" ]]; then
    check_only_args+=(--check-only)
  fi
  sudo -n python3 -B "$RELEASE_STORAGE_GUARD" storage \
    --releases-root "$RELEASES_ROOT" \
    --target-root "$RELEASE_DIR" \
    "${protected_args[@]}" \
    --checkpoints-root "$CHECKPOINTS_ROOT" \
    --current-checkpoint "$CHECKPOINT_FILE" \
    --expected-repository "$DEPLOY_REPOSITORY" \
    --nginx-active-release-conf "$NGINX_ACTIVE_RELEASE_CONF" \
    --expected-active-slot "$CURRENT_ACTIVE_SLOT" \
    --expected-active-root "$current_root" \
    --minimum-available-bytes "$minimum_bytes" \
    --minimum-available-percent "$minimum_percent" \
    --keep-unreferenced "$BLUEGREEN_RELEASE_KEEP_UNREFERENCED" \
    --normal-min-age-seconds "$BLUEGREEN_RELEASE_NORMAL_GC_AGE_SECONDS" \
    --emergency-min-age-seconds "$BLUEGREEN_RELEASE_EMERGENCY_GC_AGE_SECONDS" \
    "${legacy_option[@]}" \
    "${check_only_args[@]}"
}

guard_release_storage() {
  run_release_storage_guard \
    "$BLUEGREEN_PREPARE_DISK_RESERVE_BYTES" \
    "$BLUEGREEN_PREPARE_DISK_RESERVE_PERCENT" \
    false
}

assert_runtime_storage_reserve() {
  run_release_storage_guard \
    "$BLUEGREEN_RUNTIME_DISK_RESERVE_BYTES" \
    "$BLUEGREEN_RUNTIME_DISK_RESERVE_PERCENT" \
    true
}

assert_host_memory_budget() {
  if [[ "$BLUEGREEN_CANDIDATE_MEMORY_HIGH" != "3G" ]] \
    || [[ "$BLUEGREEN_CANDIDATE_MEMORY_MAX" != "4G" ]] \
    || [[ "$BLUEGREEN_ACTIVE_MEMORY_HIGH" != "6G" ]] \
    || [[ "$BLUEGREEN_ACTIVE_MEMORY_MAX" != "8G" ]]; then
    fail "non-default blue/green memory limits require a reviewed controller update"
  fi
  python3 -B "$RELEASE_STORAGE_GUARD" memory \
    --active-service "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" \
    --expected-active-memory-high-bytes \
      "$BLUEGREEN_ACTIVE_MEMORY_HIGH_BYTES" \
    --expected-active-memory-max-bytes \
      "$BLUEGREEN_ACTIVE_MEMORY_MAX_BYTES" \
    --minimum-total-bytes "$BLUEGREEN_MIN_TOTAL_MEMORY_BYTES" \
    --minimum-available-bytes "$BLUEGREEN_MIN_AVAILABLE_MEMORY_BYTES" \
    --candidate-max-bytes "$BLUEGREEN_CANDIDATE_MAX_MEMORY_BYTES" \
    --os-reserve-bytes "$BLUEGREEN_OS_MEMORY_RESERVE_BYTES"
}

verify_candidate_cgroup() {
  local expected_high=$((3 * 1024 * 1024 * 1024))
  local expected_max=$((4 * 1024 * 1024 * 1024))
  local actual_high=""
  local actual_max=""
  actual_high="$(systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p MemoryHigh --value)"
  actual_max="$(systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p MemoryMax --value)"
  if [[ "$BLUEGREEN_CANDIDATE_MEMORY_HIGH" == "3G" && "$actual_high" != "$expected_high" ]]; then
    fail "candidate MemoryHigh is not 3G: $actual_high"
  fi
  if [[ "$BLUEGREEN_CANDIDATE_MEMORY_MAX" == "4G" && "$actual_max" != "$expected_max" ]]; then
    fail "candidate MemoryMax is not 4G: $actual_max"
  fi
  verify_candidate_cgroup_processes_only
}

verify_active_cgroup() {
  local expected_high=$((6 * 1024 * 1024 * 1024))
  local expected_max=$((8 * 1024 * 1024 * 1024))
  local actual_high=""
  local actual_max=""
  actual_high="$(systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p MemoryHigh --value)"
  actual_max="$(systemctl show "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p MemoryMax --value)"
  if [[ "$BLUEGREEN_ACTIVE_MEMORY_HIGH" == "6G" && "$actual_high" != "$expected_high" ]]; then
    fail "active MemoryHigh is not 6G: $actual_high"
  fi
  if [[ "$BLUEGREEN_ACTIVE_MEMORY_MAX" == "8G" && "$actual_max" != "$expected_max" ]]; then
    fail "active MemoryMax is not 8G: $actual_max"
  fi
  sudo -n grep -Fxq "APP_BACKEND_WORKERS=2" "$SLOT_ENV_ROOT/$CANDIDATE_SLOT.env" \
    || fail "active slot must retain exactly two configured Uvicorn workers"
  verify_candidate_cgroup_processes_only
}

verify_candidate_cgroup_processes_only() {
  python3 - "${SERVICE_PREFIX}${CANDIDATE_SLOT}" <<'PY'
import pathlib
import subprocess
import sys

unit = sys.argv[1]
group = subprocess.check_output(
    ["systemctl", "show", unit, "-p", "ControlGroup", "--value"],
    text=True,
).strip()
if not group or ".." in pathlib.PurePosixPath(group).parts:
    raise SystemExit("[ERROR] Backend cgroup cannot be resolved")
procs = pathlib.Path("/sys/fs/cgroup") / group.lstrip("/") / "cgroup.procs"
for raw_pid in procs.read_text(encoding="utf-8").splitlines():
    cmdline = pathlib.Path("/proc") / raw_pid / "cmdline"
    try:
        command = cmdline.read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
    except OSError:
        continue
    if "jato_monthly_worker.py" in command:
        raise SystemExit("[ERROR] Backend cgroup started a JATO monthly worker during deployment")
PY
}

verify_candidate_monthly_gate() {
  local body=""
  local status=""
  body="$(mktemp)"
  if ! status="$(
    curl --noproxy '*' --silent --show-error --output "$body" \
      --write-out '%{http_code}' --max-time 10 \
      "http://127.0.0.1:${CANDIDATE_SLOT}/v1/msrp/monthly-update-jobs"
  )"; then
    rm -f "$body"
    return 1
  fi
  if ! python3 -B - \
    "$body" "$status" "$CANDIDATE_SLOT" "$CURRENT_ACTIVE_SLOT" <<'PY'
import json
from pathlib import Path
import sys

body = Path(sys.argv[1])
status = sys.argv[2]
candidate_slot = sys.argv[3]
active_slot = sys.argv[4]
try:
    payload = json.loads(body.read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as exc:
    raise SystemExit(f"[ERROR] candidate monthly gate returned invalid JSON: {exc}")
detail = payload.get("detail") if isinstance(payload, dict) else None
if status != "423" or not isinstance(detail, dict):
    raise SystemExit(
        "[ERROR] candidate monthly gate did not return structured HTTP 423"
    )
if (
    detail.get("code") != "JATO_MONTHLY_DISABLED"
    or detail.get("enabled") is not False
    or detail.get("releaseSlot") != candidate_slot
):
    raise SystemExit(
        "[ERROR] candidate monthly gate identity/disabled contract is invalid"
    )
reason = detail.get("reason")
if reason == "inactive_release_slot":
    if detail.get("activeSlot") != active_slot:
        raise SystemExit(
            "[ERROR] inactive candidate monthly gate reports the wrong active slot"
        )
elif reason == "deployment_in_progress":
    if detail.get("activeSlot") is not None:
        raise SystemExit(
            "[ERROR] deployment maintenance gate must not claim an active owner"
        )
else:
    raise SystemExit(
        f"[ERROR] candidate monthly gate used unsafe reason {reason!r}; "
        "explicit disablement or invalid configuration cannot prove slot isolation"
    )
PY
  then
    rm -f "$body"
    return 1
  fi
  rm -f "$body"
}

verify_active_monthly_gate_released() {
  local active_slot="${1:-$CANDIDATE_SLOT}"
  local body=""
  local status=""
  body="$(mktemp)"
  if ! status="$(
    curl --noproxy '*' --silent --show-error --output "$body" \
      --write-out '%{http_code}' --max-time 10 \
      "http://127.0.0.1:${active_slot}/v1/msrp/monthly-update-jobs"
  )"; then
    rm -f "$body"
    return 1
  fi
  if ! python3 -B - "$body" "$status" <<'PY'
import json
from pathlib import Path
import sys

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as exc:
    raise SystemExit(f"[ERROR] active monthly endpoint returned invalid JSON: {exc}")
status = sys.argv[2]
if status == "423":
    detail = payload.get("detail") if isinstance(payload, dict) else None
    reason = detail.get("reason") if isinstance(detail, dict) else "unstructured"
    raise SystemExit(
        f"[ERROR] active slot remains blocked by the release gate: reason={reason}"
    )
if status in {"401", "403"}:
    detail = payload.get("detail") if isinstance(payload, dict) else None
    if detail in (None, "", {}):
        raise SystemExit(
            "[ERROR] active monthly endpoint returned an unstructured auth denial"
        )
    print(
        f"[INFO] Active monthly release gate is open; unauthenticated probe "
        f"reached the HTTP {status} authentication boundary"
    )
elif status == "200":
    print("[INFO] Active monthly release gate is open and returned HTTP 200")
else:
    raise SystemExit(
        f"[ERROR] active monthly endpoint returned unexpected HTTP {status}"
    )
PY
  then
    rm -f "$body"
    return 1
  fi
  rm -f "$body"
}

verify_candidate() {
  verify_final_runtime_seal || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled || return 1
  if [[ "$BLUEGREEN_FAULT" == "candidate_start" ]]; then
    fail "fault injection: candidate_start"
  fi
  sudo -n systemctl start "${SERVICE_PREFIX}${CANDIDATE_SLOT}"
  for attempt in $(seq 1 20); do
    if curl --noproxy '*' -fsS --max-time 10 \
      "http://127.0.0.1:${CANDIDATE_SLOT}/healthz" >/dev/null \
      && python3 -B "$READINESS_HELPER" \
        --url "http://127.0.0.1:${CANDIDATE_SLOT}/readyz" \
        --expected-commit "$DEPLOY_COMMIT_SHA" \
        --timeout-seconds 10 >/dev/null; then
      break
    fi
    if [[ "$attempt" -eq 20 ]]; then
      fail "candidate did not pass liveness and exact release readiness"
    fi
    sleep 3
  done
  verify_candidate_monthly_gate || return 1
  verify_candidate_cgroup || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled || return 1
  verify_final_runtime_seal || return 1
}

verify_switch_prerequisites() {
  verify_boot_reconciler_installation \
    || fail "boot route reconciler changed before the atomic route switch" \
    || return 1
  resolve_previous_release_identity \
    || fail "previous release metadata is unavailable before the route switch" \
    || return 1
  verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA" \
    || fail "previous slot no longer serves its exact rollback SHA" \
    || return 1
  verify_public_release_exact "$PREVIOUS_RELEASE_SHA" \
    || fail "public route no longer serves the exact previous rollback SHA" \
    || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState disabled \
    || fail "old slot retained an unsafe static boot owner before route switch" \
    || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState active \
    || fail "old routed slot stopped before the atomic route switch" \
    || return 1
  verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
    || fail "candidate no longer serves the exact target SHA before switch" \
    || return 1
  verify_candidate_cgroup \
    || fail "candidate cgroup isolation changed before switch" \
    || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled \
    || fail "candidate became boot-enabled before the atomic route switch" \
    || return 1
  verify_candidate_monthly_gate \
    || fail "candidate monthly ownership gate changed before switch" \
    || return 1
  verify_final_runtime_seal \
    || fail "candidate venv/frontend runtime changed before switch" \
    || return 1
}

render_active_release() {
  local output="$1"
  local port="$2"
  local frontend_root="$3"
  python3 - "$output" "$port" "$frontend_root" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
port = int(sys.argv[2])
root = sys.argv[3]
if port not in {8000, 8001} or not root.startswith("/opt/"):
    raise SystemExit("[ERROR] active release route is invalid")
path.write_text(
    "# Managed by the JATO blue/green release controller.\n"
    "# Backend and frontend must always move together in this one file.\n"
    "upstream jato_fullstack_api {\n"
    f"    server 127.0.0.1:{port} max_fails=3 fail_timeout=30s;\n"
    "    keepalive 32;\n"
    "}\n\n"
    "map $host $jato_frontend_root {\n"
    f'    default "{root}";\n'
    "}\n\n"
    "# Stable loopback entry for host-side consumers such as MSRP schedulers.\n"
    "# It follows the same upstream switch as the public site and is never exposed externally.\n"
    "server {\n"
    "    listen 127.0.0.1:18000;\n"
    "    server_name _;\n\n"
    "    location ^~ /v1/msrp/monthly-update {\n"
    "        if (-f /var/lib/jato-release/deployment-maintenance) {\n"
    "            return 423;\n"
    "        }\n"
    "        proxy_pass http://jato_fullstack_api;\n"
    "        proxy_http_version 1.1;\n"
    "        proxy_buffering off;\n"
    "        proxy_read_timeout 3600s;\n"
    "        proxy_send_timeout 3600s;\n"
    "        add_header Cache-Control \"no-store\" always;\n"
    "    }\n\n"
    "    location / {\n"
    "        proxy_pass http://jato_fullstack_api;\n"
    "        proxy_http_version 1.1;\n"
    "        proxy_set_header Host $host;\n"
    "        proxy_set_header X-Real-IP $remote_addr;\n"
    "        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n"
    "        proxy_set_header X-Forwarded-Proto http;\n"
    "        proxy_connect_timeout 10s;\n"
    "        proxy_send_timeout 600s;\n"
    "        proxy_read_timeout 600s;\n"
    "        proxy_buffering off;\n"
    "    }\n"
    "}\n",
    encoding="utf-8",
)
PY
}

verify_durable_route_ownership() {
  local slot="$1"
  local release_root="$2"
  local expected_sha="$3"
  local frontend_root="$4"
  local expected_conf=""
  if sudo -n test -L "$ACTIVE_SLOT_FILE" \
    || ! sudo -n test -f "$ACTIVE_SLOT_FILE" \
    || [[ "$(sudo -n cat "$ACTIVE_SLOT_FILE")" != "$slot" ]]; then
    fail "durable active-slot owner does not match slot $slot"
    return 1
  fi
  if ! sudo -n test -L "$ACTIVE_RELEASE_LINK" \
    || [[ "$(sudo -n realpath "$ACTIVE_RELEASE_LINK")" != "$release_root" ]]; then
    fail "durable active release link does not match $release_root"
    return 1
  fi
  if sudo -n test -L "$NGINX_ACTIVE_RELEASE_CONF" \
    || ! sudo -n test -f "$NGINX_ACTIVE_RELEASE_CONF"; then
    fail "durable Nginx active release include is missing or unsafe"
    return 1
  fi
  expected_conf="$(mktemp)"
  render_active_release "$expected_conf" "$slot" "$frontend_root"
  if ! sudo -n cmp -s "$expected_conf" "$NGINX_ACTIVE_RELEASE_CONF"; then
    rm -f "$expected_conf"
    fail "durable Nginx active release include does not match slot $slot"
    return 1
  fi
  rm -f "$expected_conf"
  verify_slot_release_exact "$slot" "$expected_sha" \
    && verify_public_release_exact "$expected_sha"
}

is_known_scheduler_timer() {
  local expected=""
  local candidate="$1"
  for expected in "${SCHEDULER_TIMERS[@]}"; do
    if [[ "$candidate" == "$expected" ]]; then
      return 0
    fi
  done
  return 1
}

snapshot_scheduler_state() {
  local timer=""
  local enabled_state=""
  local active_state=""
  local temp=""
  if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
    fail "scheduler state snapshot already exists and requires inspection: $SCHEDULER_STATE_FILE"
  fi
  temp="$(mktemp "${SCHEDULER_STATE_FILE}.tmp.XXXXXX")"
  for timer in "${SCHEDULER_TIMERS[@]}"; do
    if ! sudo -n systemctl cat "$timer" >/dev/null 2>&1; then
      printf '%s\tabsent\tfalse\n' "$timer" >> "$temp"
      continue
    fi
    enabled_state="$(systemctl is-enabled "$timer" 2>/dev/null || true)"
    case "$enabled_state" in
      enabled|enabled-runtime|disabled|masked|masked-runtime|static|indirect|generated|transient|alias|linked|linked-runtime) ;;
      *)
        rm -f "$temp"
        fail "unsupported enablement state for $timer: ${enabled_state:-empty}"
        return
        ;;
    esac
    active_state=false
    if systemctl is-active --quiet "$timer"; then
      active_state=true
    fi
    printf '%s\t%s\t%s\n' "$timer" "$enabled_state" "$active_state" >> "$temp"
  done
  chmod 0600 "$temp"
  if ! durable_install_file "$temp" "$SCHEDULER_STATE_FILE" 0600; then
    rm -f "$temp"
    return 1
  fi
  rm -f "$temp"
}

restore_scheduler_enablement() {
  local timer="$1"
  local expected="$2"
  local actual=""
  case "$expected" in
    enabled)
      sudo -n systemctl unmask "$timer" >/dev/null
      sudo -n systemctl enable "$timer" >/dev/null
      ;;
    enabled-runtime)
      sudo -n systemctl unmask "$timer" >/dev/null
      sudo -n systemctl disable "$timer" >/dev/null
      sudo -n systemctl enable --runtime "$timer" >/dev/null
      ;;
    disabled)
      sudo -n systemctl unmask "$timer" >/dev/null
      sudo -n systemctl disable "$timer" >/dev/null
      ;;
    masked)
      sudo -n systemctl mask "$timer" >/dev/null
      ;;
    masked-runtime)
      sudo -n systemctl mask --runtime "$timer" >/dev/null
      ;;
    static|indirect|generated|transient|alias|linked|linked-runtime)
      # These states cannot be manufactured safely by enable/disable. The
      # post-activation path must leave them untouched, and verification below
      # fails closed if that contract is broken.
      ;;
    *)
      fail "refusing unknown scheduler enablement state: $expected"
      return
      ;;
  esac
  actual="$(systemctl is-enabled "$timer" 2>/dev/null || true)"
  if [[ "$actual" != "$expected" ]]; then
    fail "scheduler enablement restore mismatch for $timer: expected=$expected actual=${actual:-empty}"
  fi
}

pause_schedulers() {
  local timer=""
  local service=""
  snapshot_scheduler_state
  for timer in "${SCHEDULER_TIMERS[@]}"; do
    if sudo -n systemctl cat "$timer" >/dev/null 2>&1; then
      sudo -n systemctl stop "$timer"
    fi
  done
  if (( ${#SCHEDULER_SERVICES[@]} == 0 )); then
    SCHEDULERS_PAUSED=true
    return
  fi
  for attempt in $(seq 1 60); do
    local busy=false
    for service in "${SCHEDULER_SERVICES[@]}"; do
      if systemctl is-active --quiet "$service"; then
        busy=true
      fi
    done
    if [[ "$busy" == "false" ]]; then
      SCHEDULERS_PAUSED=true
      return
    fi
    sleep 2
  done
  fail "scheduled old-code services did not quiesce naturally"
}

resume_schedulers() {
  local timer=""
  local enabled_state=""
  local active_state=""
  local extra=""
  local restored_timer=""
  local already_restored=false
  local restored_timers=()
  if [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]]; then
    echo "[INFO] No scheduler state snapshot exists; scheduler restore is a no-op"
    SCHEDULERS_PAUSED=false
    return 0
  fi
  if [[ ! -f "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
    fail "scheduler state snapshot must be a regular non-symlink file: $SCHEDULER_STATE_FILE"
  fi
  while IFS=$'\t' read -r timer enabled_state active_state extra; do
    if [[ -z "$timer" ]]; then
      continue
    fi
    already_restored=false
    if (( ${#restored_timers[@]} > 0 )); then
      for restored_timer in "${restored_timers[@]}"; do
        if [[ "$timer" == "$restored_timer" ]]; then
          already_restored=true
          break
        fi
      done
    fi
    if [[ -n "$extra" ]] \
      || ! is_known_scheduler_timer "$timer" \
      || [[ "$already_restored" == "true" ]] \
      || [[ "$active_state" != "true" && "$active_state" != "false" ]]; then
      fail "scheduler state snapshot is malformed or contains an unexpected timer"
      return
    fi
    if [[ "$enabled_state" == "absent" ]]; then
      if sudo -n systemctl cat "$timer" >/dev/null 2>&1; then
        fail "scheduler appeared after an absent-state snapshot: $timer"
        return
      fi
      restored_timers+=("$timer")
      continue
    fi
    if ! sudo -n systemctl cat "$timer" >/dev/null 2>&1; then
      fail "scheduler disappeared before state restore: $timer"
      return
    fi
    restored_timers+=("$timer")
    restore_scheduler_enablement "$timer" "$enabled_state"
    if [[ "$active_state" == "true" ]]; then
      sudo -n systemctl start "$timer"
      if ! systemctl is-active --quiet "$timer"; then
        fail "scheduler active-state restore failed for $timer"
        return
      fi
    else
      sudo -n systemctl stop "$timer"
      if systemctl is-active --quiet "$timer"; then
        fail "scheduler inactive-state restore failed for $timer"
        return
      fi
    fi
  done < "$SCHEDULER_STATE_FILE"
  if [[ "${#restored_timers[@]}" -ne "${#SCHEDULER_TIMERS[@]}" ]]; then
    fail "scheduler state snapshot is incomplete"
    return
  fi
  for timer in "${SCHEDULER_TIMERS[@]}"; do
    already_restored=false
    for restored_timer in "${restored_timers[@]}"; do
      if [[ "$timer" == "$restored_timer" ]]; then
        already_restored=true
        break
      fi
    done
    if [[ "$already_restored" != "true" ]]; then
      fail "scheduler state snapshot omitted expected timer: $timer"
      return
    fi
  done
  durable_remove_path "$SCHEDULER_STATE_FILE"
  SCHEDULERS_PAUSED=false
}

normalized_deploy_server_names() {
  python3 - "${1:-}" <<'PY'
import re
import sys

raw = sys.argv[1]
names = raw.split() or ["_"]
if "_" in names:
    if names != ["_"]:
        raise SystemExit("[ERROR] DEPLOY_SERVER_NAME '_' cannot be mixed with public hostnames")
    print("_")
    raise SystemExit(0)

label_pattern = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
seen: set[str] = set()
for raw_name in names:
    name = raw_name.lower()
    labels = name.split(".")
    if (
        len(name) > 253
        or len(labels) < 2
        or any(not label_pattern.fullmatch(label) for label in labels)
    ):
        raise SystemExit(f"[ERROR] invalid DEPLOY_SERVER_NAME hostname: {raw_name}")
    if name not in seen:
        seen.add(name)
        print(name)
PY
}

verify_nginx_payloads() {
  local ready_payload="$1"
  local build_payload="$2"
  local verified_host="$3"
  python3 - "$ready_payload" "$build_payload" \
    "$RELEASE_DIR/06_AppPlatform/frontend/dist" "$DEPLOY_COMMIT_SHA" \
    "$verified_host" <<'PY'
import json
from pathlib import Path
import sys

ready = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
build = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
root = Path(sys.argv[3])
commit = sys.argv[4]
verified_host = sys.argv[5]
if ready.get("status") != "ready":
    raise SystemExit(f"[ERROR] Nginx /readyz did not report ready for {verified_host}")
release = ready.get("release")
if not isinstance(release, dict) or release.get("commitSha") != commit:
    raise SystemExit(f"[ERROR] Nginx /readyz does not bind the target SHA for {verified_host}")
for name in ("index.html", "build-meta.json", "release-provenance.json"):
    if not (root / name).is_file():
        raise SystemExit(f"[ERROR] Candidate frontend is missing {name}")
values = {
    str(build.get("githubSha") or ""),
    str(build.get("commitSha") or ""),
    str(build.get("sha") or ""),
}
if commit not in values:
    raise SystemExit(
        f"[ERROR] Candidate frontend build metadata does not bind the target SHA for {verified_host}"
    )
PY
}

verify_nginx_candidate() {
  local ready_payload=""
  local build_payload=""
  local normalized_names=""
  local server_name=""
  local server_names=()
  ready_payload="$(mktemp)"
  build_payload="$(mktemp)"
  if ! normalized_names="$(normalized_deploy_server_names "${DEPLOY_SERVER_NAME:-}")"; then
    rm -f "$ready_payload" "$build_payload"
    return 1
  fi
  mapfile -t server_names <<< "$normalized_names"
  if [[ "${server_names[*]}" == "_" ]]; then
    if ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
      -H 'Host: localhost' http://127.0.0.1/readyz > "$ready_payload" \
      || ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
        -H 'Host: localhost' http://127.0.0.1/build-meta.json > "$build_payload" \
      || ! verify_nginx_payloads "$ready_payload" "$build_payload" "localhost"; then
      rm -f "$ready_payload" "$build_payload"
      return 1
    fi
  else
    for server_name in "${server_names[@]}"; do
      if ! curl --noproxy '*' --fail --silent --show-error --location \
        --proto '=https' --proto-redir '=https' --max-time 20 \
        --resolve "${server_name}:443:127.0.0.1" \
        "https://${server_name}/readyz" > "$ready_payload" \
        || ! curl --noproxy '*' --fail --silent --show-error --location \
          --proto '=https' --proto-redir '=https' --max-time 20 \
          --resolve "${server_name}:443:127.0.0.1" \
          "https://${server_name}/build-meta.json" > "$build_payload" \
        || ! verify_nginx_payloads "$ready_payload" "$build_payload" "$server_name"; then
        rm -f "$ready_payload" "$build_payload"
        return 1
      fi
    done
  fi
  rm -f "$ready_payload" "$build_payload"
}

verify_public_release_exact() {
  local expected_sha="$1"
  local normalized_names=""
  local ready_payload=""
  local build_payload=""
  local server_name=""
  local server_names=()
  ready_payload="$(mktemp)"
  build_payload="$(mktemp)"
  if ! normalized_names="$(normalized_deploy_server_names "${DEPLOY_SERVER_NAME:-}")"; then
    rm -f "$ready_payload" "$build_payload"
    return 1
  fi
  mapfile -t server_names <<< "$normalized_names"
  for server_name in "${server_names[@]}"; do
    if [[ "$server_name" == "_" ]]; then
      if ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
        -H 'Host: localhost' http://127.0.0.1/readyz > "$ready_payload" \
        || ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
          -H 'Host: localhost' http://127.0.0.1/build-meta.json > "$build_payload"; then
        rm -f "$ready_payload" "$build_payload"
        return 1
      fi
    else
      if ! curl --noproxy '*' --fail --silent --show-error --location \
        --proto '=https' --proto-redir '=https' --max-time 20 \
        --resolve "${server_name}:443:127.0.0.1" \
        "https://${server_name}/readyz" > "$ready_payload" \
        || ! curl --noproxy '*' --fail --silent --show-error --location \
          --proto '=https' --proto-redir '=https' --max-time 20 \
          --resolve "${server_name}:443:127.0.0.1" \
          "https://${server_name}/build-meta.json" > "$build_payload"; then
        rm -f "$ready_payload" "$build_payload"
        return 1
      fi
    fi
    if ! python3 - "$ready_payload" "$build_payload" \
      "$expected_sha" "$server_name" <<'PY'
import json
from pathlib import Path
import sys

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
build = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
release = payload.get("release")
if (
    payload.get("status") != "ready"
    or not isinstance(release, dict)
    or release.get("commitSha") != sys.argv[3]
):
    raise SystemExit(
        f"[ERROR] public /readyz does not bind exact release {sys.argv[3]} "
        f"for {sys.argv[4]}"
    )
build_commits = {
    str(build.get(name) or "")
    for name in ("deployCommit", "githubSha", "commitSha", "sha")
}
if sys.argv[3] not in build_commits:
    raise SystemExit(
        f"[ERROR] public frontend build-meta does not bind exact release "
        f"{sys.argv[3]} for {sys.argv[4]}"
    )
PY
    then
      rm -f "$ready_payload" "$build_payload"
      return 1
    fi
  done
  rm -f "$ready_payload" "$build_payload"
}

keep_candidate_route_healthy() {
  local candidate_conf=""
  verify_final_runtime_seal || return 1
  sudo -n systemctl enable "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null || return 1
  sudo -n systemctl set-property "${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
    "MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH" \
    "MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX" \
    "CPUQuota=100%" || return 1
  sudo -n systemctl start "${SERVICE_PREFIX}${CANDIDATE_SLOT}" || return 1
  verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" || return 1
  candidate_conf="$(mktemp)" || return 1
  if ! render_active_release \
    "$candidate_conf" \
    "$CANDIDATE_SLOT" \
    "$RELEASE_DIR/06_AppPlatform/frontend/dist"; then
    rm -f "$candidate_conf"
    return 1
  fi
  durable_install_file \
    "$candidate_conf" \
    "$NGINX_ACTIVE_RELEASE_CONF" 0644 || {
      rm -f "$candidate_conf"
      return 1
    }
  rm -f "$candidate_conf"
  sudo -n nginx -t || return 1
  sudo -n systemctl reload nginx || return 1
  verify_public_release_exact "$DEPLOY_COMMIT_SHA" || return 1
  atomic_text "$ACTIVE_SLOT_FILE" "$CANDIDATE_SLOT" || return 1
  atomic_symlink "$RELEASE_DIR" "$ACTIVE_RELEASE_LINK" || return 1
  mark_maintenance_required || return 1
  resume_schedulers || return 1
  echo "[WARN] Candidate remains the healthy public route under the durable maintenance fence" >&2
}

restore_previous_route() {
  local binding=""
  SWITCH_BACKUP="${SWITCH_BACKUP:-$BLUEGREEN_STATE_ROOT/active-release.pre-${DEPLOY_COMMIT_SHA}.conf}"
  binding="$(evidence_binding 2>/dev/null || true)"
  if ! read_checkpoint_phase_status; then
    mark_maintenance_required || true
    keep_candidate_route_healthy || true
    return 1
  fi
  if [[ "$CHECKPOINT_PHASE" == "rollback_completed" ]]; then
    SWITCH_RECONCILED=true
    RELEASE_ROLLED_BACK=true
    return 0
  fi
  if [[ "$CHECKPOINT_PHASE" != "rollback_started" ]]; then
    if ! checkpoint_write rollback_started in_progress rollback_required \
      "blue/green route rollback started; ${binding:-evidence_unavailable}"; then
      mark_maintenance_required || true
      keep_candidate_route_healthy || true
      return 1
    fi
  fi
  if ! resolve_previous_release_identity \
    || ! restore_backend_template_preimage \
    || ! sudo -n systemctl enable "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" >/dev/null \
    || ! sudo -n systemctl start "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" \
    || ! unit_property_equals \
      "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState enabled \
    || ! unit_property_equals \
      "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState active \
    || ! verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA"; then
    mark_maintenance_required || true
    keep_candidate_route_healthy || true
    return 1
  fi
  if [[ ! -f "$SWITCH_BACKUP" || -L "$SWITCH_BACKUP" ]]; then
    mark_maintenance_required || true
    keep_candidate_route_healthy || true
    return 1
  fi
  if ! durable_install_file \
    "$SWITCH_BACKUP" \
    "$NGINX_ACTIVE_RELEASE_CONF" 0644 \
    || ! sudo -n nginx -t \
    || ! sudo -n systemctl reload nginx \
    || ! verify_public_release_exact "$PREVIOUS_RELEASE_SHA" \
    || ! atomic_text "$ACTIVE_SLOT_FILE" "$CURRENT_ACTIVE_SLOT" \
    || ! atomic_symlink "$PREVIOUS_RELEASE_ROOT" "$ACTIVE_RELEASE_LINK" \
    || ! verify_durable_route_ownership \
      "$CURRENT_ACTIVE_SLOT" \
      "$PREVIOUS_RELEASE_ROOT" \
      "$PREVIOUS_RELEASE_SHA" \
      "$PREVIOUS_RELEASE_ROOT/06_AppPlatform/frontend/dist"; then
    mark_maintenance_required || true
    keep_candidate_route_healthy || true
    return 1
  fi
  if ! resume_schedulers; then
    mark_maintenance_required || true
    return 1
  fi
  if ! sudo -n systemctl disable --now \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 \
    || ! unit_property_equals \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled \
    || ! unit_property_equals \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState inactive \
    || ! remove_candidate_explicit_unit \
    || ! remove_backend_template_preimage; then
    mark_maintenance_required || true
    return 1
  fi
  if ! checkpoint_write rollback_completed completed automatic \
    "old exact-SHA slot and public Nginx route verified after candidate failure; ${binding:-evidence_unavailable}"; then
    mark_maintenance_required || true
    return 1
  fi
  SWITCH_RECONCILED=true
  RELEASE_ROLLED_BACK=true
}

restore_nginx_preimage() {
  if [[ ! -e "$NGINX_PREIMAGE_DIR" && ! -L "$NGINX_PREIMAGE_DIR" ]]; then
    return 0
  fi
  if [[ ! -d "$NGINX_PREIMAGE_DIR" || -L "$NGINX_PREIMAGE_DIR" ]]; then
    fail "durable Nginx preimage is unsafe: $NGINX_PREIMAGE_DIR"
    return 1
  fi
  sudo -n DEPLOY_STATE_DIR="${DEPLOY_STATE_DIR:-}" \
    JATO_PRODUCTION_DEPLOY_LOCK_PATH="${DEPLOY_LOCK_PATH:-}" \
    DEPLOY_LOCK_HELD="${DEPLOY_LOCK_HELD:-}" \
    DEPLOY_LOCK_HOLDER_PID="${DEPLOY_LOCK_HOLDER_PID:-}" \
    DEPLOY_LOCK_FD="${DEPLOY_LOCK_FD:-}" \
    NGINX_PREIMAGE_DIR="$NGINX_PREIMAGE_DIR" \
    bash "$NGINX_INSTALLER" restore-preimage
}

remove_nginx_preimage() {
  if [[ -e "$NGINX_PREIMAGE_DIR" || -L "$NGINX_PREIMAGE_DIR" ]]; then
    durable_remove_tree "$NGINX_PREIMAGE_DIR"
  fi
}

remove_candidate_explicit_unit() {
  local explicit_candidate="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"
  durable_remove_path "$explicit_candidate" || return 1
  sudo -n systemctl daemon-reload
}

candidate_cleanup_is_complete() {
  local active_state=""
  local explicit_candidate="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"
  local load_state=""
  local unit_file_state=""
  if sudo -n test -e "$explicit_candidate" \
    || sudo -n test -L "$explicit_candidate"; then
    return 1
  fi
  load_state="$(
    systemctl show \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p LoadState --value 2>/dev/null \
      || true
  )"
  active_state="$(
    systemctl show \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p ActiveState --value 2>/dev/null \
      || true
  )"
  unit_file_state="$(
    systemctl show \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" -p UnitFileState --value 2>/dev/null \
      || true
  )"
  [[ "$active_state" == "inactive" ]] \
    && {
      [[ "$load_state" == "not-found" ]] \
        || [[ "$unit_file_state" == "disabled" ]]
    }
}

cleanup_pre_switch_candidate() {
  if ! resolve_previous_release_identity \
    || ! verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA"; then
    mark_maintenance_required || true
    return 1
  fi
  if [[ "$BLUEGREEN_MODE" == "switch-locked" ]]; then
    if ! verify_public_release_exact "$PREVIOUS_RELEASE_SHA"; then
      mark_maintenance_required || true
      return 1
    fi
    if ! restore_old_static_boot_owner; then
      mark_maintenance_required || true
      return 1
    fi
    if ! candidate_cleanup_is_complete \
      && {
        ! sudo -n systemctl disable --now \
          "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 \
          || ! unit_property_equals \
            "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled \
          || ! unit_property_equals \
            "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState inactive \
          || ! remove_candidate_explicit_unit
      }; then
      mark_maintenance_required || true
      return 1
    fi
    if ! candidate_cleanup_is_complete; then
      mark_maintenance_required || true
      return 1
    fi
    if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]] \
      && ! resume_schedulers; then
      mark_maintenance_required || true
      return 1
    fi
    mark_maintenance_required || true
    echo "[WARN] Persistent supervisor stopped the candidate and restored schedulers without mutating the durable stable Nginx route" >&2
    return 1
  fi
  if ! restore_nginx_preimage \
    || ! verify_public_release_exact "$PREVIOUS_RELEASE_SHA"; then
    mark_maintenance_required || true
    return 1
  fi
  if ! restore_old_static_boot_owner; then
    mark_maintenance_required || true
    return 1
  fi
  if ! candidate_cleanup_is_complete \
    && {
      ! sudo -n systemctl disable --now \
        "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 \
        || ! unit_property_equals \
          "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled \
        || ! unit_property_equals \
          "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState inactive \
        || ! remove_candidate_explicit_unit
    }; then
    mark_maintenance_required || true
    return 1
  fi
  if ! candidate_cleanup_is_complete; then
    mark_maintenance_required || true
    return 1
  fi
  if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
    resume_schedulers || {
      mark_maintenance_required || true
      return 1
    }
  fi
  remove_nginx_preimage || {
    mark_maintenance_required || true
    return 1
  }
}

prepare_exit_handler() {
  local rc="$?"
  trap - EXIT TERM INT HUP
  if [[ "$PRE_SUPERVISOR_CANDIDATE_ARMED" == "true" ]]; then
    PRE_SUPERVISOR_CANDIDATE_ARMED=false
    if ! cleanup_pre_switch_candidate; then
      rc="$EXIT_COMMAND_FAILED_MARKER_RETAINED"
    fi
  fi
  exit "$rc"
}

reconcile_pre_switch_state() {
  if ! resolve_existing_candidate_slot \
    || ! resolve_previous_release_identity; then
    mark_maintenance_required || true
    return 1
  fi
  if verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
    && verify_public_release_exact "$DEPLOY_COMMIT_SHA"; then
    mark_maintenance_required || true
    keep_candidate_route_healthy || true
    return 1
  fi
  if verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA" \
    && verify_public_release_exact "$PREVIOUS_RELEASE_SHA"; then
    if ! cleanup_pre_switch_candidate; then
      mark_maintenance_required || true
      return 1
    fi
    if [[ "$BLUEGREEN_MODE" != "switch-locked" ]]; then
      clear_maintenance_marker || return 1
    fi
    SWITCH_RECONCILED=true
    return 0
  fi
  mark_maintenance_required || true
  if verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA"; then
    keep_candidate_route_healthy || true
  fi
  return 1
}

reconcile_incomplete_switch() {
  if ! read_checkpoint_phase_status; then
    mark_maintenance_required || true
    return 1
  fi
  if checkpoint_commits_candidate "$CHECKPOINT_PHASE" "$CHECKPOINT_STATUS"; then
    if verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
      && verify_public_release_exact "$DEPLOY_COMMIT_SHA"; then
      SWITCH_COMPLETED=true
      SWITCH_RECONCILED=true
      return 0
    fi
    mark_maintenance_required || true
    return 1
  fi
  case "$CHECKPOINT_PHASE" in
    switch_started|switched|rollback_started)
      restore_previous_route
      ;;
    rollback_completed)
      SWITCH_RECONCILED=true
      RELEASE_ROLLED_BACK=true
      ;;
    *)
      reconcile_pre_switch_state
      ;;
  esac
}

switch_exit_handler() {
  local rc="$?"
  trap - EXIT TERM INT HUP
  if [[ "$BLUEGREEN_MODE" == "switch-locked" ]] \
    && [[ "$SWITCH_COMPLETED" != "true" ]] \
    && [[ "$SWITCH_HANDLER_ACTIVE" != "true" ]]; then
    SWITCH_HANDLER_ACTIVE=true
    if ! reconcile_incomplete_switch; then
      rc="$EXIT_COMMAND_FAILED_MARKER_RETAINED"
    fi
  fi
  exit "$rc"
}

switch_signal_handler() {
  local signal_number="$1"
  exit "$((128 + signal_number))"
}

run_post_activation() {
  SYSTEMD_RUNTIME_ROOT="$ACTIVE_RELEASE_LINK" \
  REPO_DIR="$RELEASE_DIR" \
  SKIP_GIT_SYNC=true \
  DEPLOY_PRUNE_UNTRACKED=false \
  BLUEGREEN_POST_ACTIVATION_ONLY=true \
  RUN_DATABASE_MIGRATIONS=false \
  RUN_GROUPED_TIME_SERIES_PREWARM=false \
  RECONCILE_SCRAPER_TIMER_STATE=false \
  BOOTSTRAP_MSRP_DRYRUN_IF_MISSING=false \
  PRODUCTION_RELEASE_WORKFLOW=true \
  PREBUILT_FRONTEND_DIR="$RELEASE_DIR/06_AppPlatform/frontend/dist" \
  BACKEND_SERVICE_NAME="${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
  BACKEND_PORT="$CANDIDATE_SLOT" \
  RELEASE_CHECKPOINT_FILE="$CHECKPOINT_FILE" \
  RELEASE_CHECKPOINT_JOURNAL="$CHECKPOINT_JOURNAL" \
  RELEASE_CHECKPOINT_REPOSITORY="$DEPLOY_REPOSITORY" \
  RELEASE_CHECKPOINT_COMMIT="$DEPLOY_COMMIT_SHA" \
  RELEASE_CHECKPOINT_ARCHIVE_SHA256="$DEPLOY_ARCHIVE_SHA256" \
  RELEASE_CHECKPOINT_ARCHIVE_BYTES="$DEPLOY_ARCHIVE_BYTES" \
  RELEASE_CHECKPOINT_RUN_ID="$DEPLOY_RUN_ID" \
  RELEASE_CHECKPOINT_RUN_ATTEMPT="$DEPLOY_RUN_ATTEMPT" \
  RELEASE_CHECKPOINT_FRONTEND_IDENTITY="$FRONTEND_ARTIFACT_IDENTITY" \
  RELEASE_CHECKPOINT_FRONTEND_CHECKSUM="$FRONTEND_ARTIFACT_CHECKSUM" \
    bash "$INNER_DEPLOY"
}

run_post_commit_global_reconciliation() {
  SYSTEMD_RUNTIME_ROOT="$ACTIVE_RELEASE_LINK" \
  REPO_DIR="$RELEASE_DIR" \
  SKIP_GIT_SYNC=true \
  DEPLOY_PRUNE_UNTRACKED=false \
  BLUEGREEN_GLOBAL_RECONCILE_ONLY=true \
  RUN_DATABASE_MIGRATIONS=false \
  RUN_GROUPED_TIME_SERIES_PREWARM=false \
  RECONCILE_SCRAPER_TIMER_STATE=false \
  BOOTSTRAP_MSRP_DRYRUN_IF_MISSING=false \
  PRODUCTION_RELEASE_WORKFLOW=true \
  PREBUILT_FRONTEND_DIR="$RELEASE_DIR/06_AppPlatform/frontend/dist" \
  BACKEND_SERVICE_NAME="${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
  BACKEND_PORT="$CANDIDATE_SLOT" \
  RELEASE_CHECKPOINT_FILE="$CHECKPOINT_FILE" \
  RELEASE_CHECKPOINT_JOURNAL="$CHECKPOINT_JOURNAL" \
  RELEASE_CHECKPOINT_REPOSITORY="$DEPLOY_REPOSITORY" \
  RELEASE_CHECKPOINT_COMMIT="$DEPLOY_COMMIT_SHA" \
  RELEASE_CHECKPOINT_ARCHIVE_SHA256="$DEPLOY_ARCHIVE_SHA256" \
  RELEASE_CHECKPOINT_ARCHIVE_BYTES="$DEPLOY_ARCHIVE_BYTES" \
  RELEASE_CHECKPOINT_RUN_ID="$DEPLOY_RUN_ID" \
  RELEASE_CHECKPOINT_RUN_ATTEMPT="$DEPLOY_RUN_ATTEMPT" \
  RELEASE_CHECKPOINT_FRONTEND_IDENTITY="$FRONTEND_ARTIFACT_IDENTITY" \
  RELEASE_CHECKPOINT_FRONTEND_CHECKSUM="$FRONTEND_ARTIFACT_CHECKSUM" \
    bash "$INNER_DEPLOY"
}

complete_candidate_activation() {
  local binding="$1"
  verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" || return 1
  verify_public_release_exact "$DEPLOY_COMMIT_SHA" || return 1
  atomic_text "$ACTIVE_SLOT_FILE" "$CANDIDATE_SLOT" || return 1
  atomic_symlink "$RELEASE_DIR" "$ACTIVE_RELEASE_LINK" || return 1
  sudo -n systemctl enable "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null \
    || return 1
  run_post_activation || return 1
  sleep "$BLUEGREEN_DRAIN_SECONDS" || return 1
  sudo -n systemctl disable \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" >/dev/null || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState disabled || return 1
  sudo -n systemctl stop "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState inactive || return 1
  sudo -n systemctl set-property "${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
    "MemoryHigh=$BLUEGREEN_ACTIVE_MEMORY_HIGH" \
    "MemoryMax=$BLUEGREEN_ACTIVE_MEMORY_MAX" \
    "CPUQuota=200%" || return 1
  verify_active_cgroup || return 1
  resume_schedulers || return 1
  verify_durable_route_ownership \
    "$CANDIDATE_SLOT" \
    "$RELEASE_DIR" \
    "$DEPLOY_COMMIT_SHA" \
    "$RELEASE_DIR/06_AppPlatform/frontend/dist" || return 1
  commit_backend_unit_template || return 1
  verify_candidate_reboot_gate || return 1
  verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" || return 1
  verify_public_release_exact "$DEPLOY_COMMIT_SHA" || return 1
  checkpoint_write backend_healthy completed automatic \
    "Tencent blue/green release is healthy with one active slot, 6G/8G cgroup, and JATO ownership prepared behind the deployment marker; $binding" \
    || return 1
  SWITCH_COMPLETED=true
  SWITCH_RECONCILED=true
  if ! run_post_commit_global_reconciliation; then
    mark_maintenance_required || true
    return "$EXIT_COMMAND_FAILED_MARKER_RETAINED"
  fi
  remove_backend_template_preimage \
    || return "$EXIT_COMMAND_FAILED_MARKER_RETAINED"
  remove_nginx_preimage || return "$EXIT_COMMAND_FAILED_MARKER_RETAINED"
}

reconcile_existing_switch() {
  local binding=""
  if ! read_checkpoint_phase_status; then
    mark_maintenance_required || true
    return 1
  fi
  if ! resolve_existing_candidate_slot; then
    mark_maintenance_required || true
    return 1
  fi
  SWITCH_BACKUP="$BLUEGREEN_STATE_ROOT/active-release.pre-${DEPLOY_COMMIT_SHA}.conf"
  if checkpoint_commits_candidate "$CHECKPOINT_PHASE" "$CHECKPOINT_STATUS"; then
    if verify_final_runtime_seal \
      && sudo -n systemctl enable --now \
        "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null \
      && verify_candidate_reboot_gate \
      && verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
      && verify_public_release_exact "$DEPLOY_COMMIT_SHA" \
      && atomic_text "$ACTIVE_SLOT_FILE" "$CANDIDATE_SLOT" \
      && atomic_symlink "$RELEASE_DIR" "$ACTIVE_RELEASE_LINK" \
      && sudo -n systemctl set-property "${SERVICE_PREFIX}${CANDIDATE_SLOT}" \
        "MemoryHigh=$BLUEGREEN_ACTIVE_MEMORY_HIGH" \
        "MemoryMax=$BLUEGREEN_ACTIVE_MEMORY_MAX" \
        "CPUQuota=200%" \
      && verify_active_cgroup \
      && verify_durable_route_ownership \
        "$CANDIDATE_SLOT" \
        "$RELEASE_DIR" \
        "$DEPLOY_COMMIT_SHA" \
        "$RELEASE_DIR/06_AppPlatform/frontend/dist" \
      && sudo -n systemctl disable --now \
        "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" >/dev/null 2>&1 \
      && unit_property_equals \
        "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState disabled \
      && unit_property_equals \
        "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState inactive \
      && run_post_commit_global_reconciliation \
      && remove_backend_template_preimage \
      && remove_nginx_preimage; then
      SWITCH_COMPLETED=true
      SWITCH_RECONCILED=true
      clear_maintenance_marker || return 1
      if ! verify_active_monthly_gate_released "$CANDIDATE_SLOT"; then
        mark_maintenance_required || true
        return 1
      fi
      return 0
    fi
    mark_maintenance_required || true
    return 1
  fi
  if [[ "$CHECKPOINT_PHASE" == "rollback_completed" ]]; then
    if resolve_previous_release_identity \
      && restore_backend_template_preimage \
      && sudo -n systemctl enable --now \
        "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" >/dev/null \
      && unit_property_equals \
        "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState enabled \
      && unit_property_equals \
        "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState active \
      && verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA" \
      && verify_public_release_exact "$PREVIOUS_RELEASE_SHA" \
      && atomic_text "$ACTIVE_SLOT_FILE" "$CURRENT_ACTIVE_SLOT" \
      && atomic_symlink "$PREVIOUS_RELEASE_ROOT" "$ACTIVE_RELEASE_LINK" \
      && verify_durable_route_ownership \
        "$CURRENT_ACTIVE_SLOT" \
        "$PREVIOUS_RELEASE_ROOT" \
        "$PREVIOUS_RELEASE_SHA" \
        "$PREVIOUS_RELEASE_ROOT/06_AppPlatform/frontend/dist" \
      && sudo -n systemctl disable --now \
        "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 \
      && unit_property_equals \
        "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled \
      && unit_property_equals \
        "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState inactive \
      && remove_candidate_explicit_unit \
      && remove_backend_template_preimage; then
      SWITCH_RECONCILED=true
      RELEASE_ROLLED_BACK=true
      clear_maintenance_marker || return 1
      if ! verify_active_monthly_gate_released "$CURRENT_ACTIVE_SLOT"; then
        mark_maintenance_required || true
        return 1
      fi
      return 0
    fi
    mark_maintenance_required || true
    return 1
  fi
  if [[ "$CHECKPOINT_PHASE" == "switch_started" || "$CHECKPOINT_PHASE" == "switched" ]]; then
    if verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
      && verify_public_release_exact "$DEPLOY_COMMIT_SHA"; then
      binding="$(evidence_binding 2>/dev/null || true)"
      if [[ "$CHECKPOINT_PHASE" != "switch_started" ]] \
        || checkpoint_write switched completed automatic \
          "reconciled exact candidate route after controller interruption; ${binding:-evidence_unavailable}"; then
        if complete_candidate_activation "${binding:-evidence_unavailable}"; then
          clear_maintenance_marker || return 1
          return 0
        fi
      fi
    fi
  fi
  if restore_previous_route \
    && [[ "$BLUEGREEN_MODE" != "switch-locked" ]]; then
    clear_maintenance_marker || return 1
  fi
  return 1
}

reconcile_supervisor_result() {
  local supervisor_rc="$1"
  if ! read_checkpoint_phase_status; then
    mark_maintenance_required || true
    return 1
  fi
  case "$CHECKPOINT_PHASE" in
    backend_healthy|www_verified|intl_deploy_started|intl_verified|parity_verified|complete|switch_started|switched|rollback_started|rollback_completed)
      reconcile_existing_switch
      return
      ;;
    *)
      if reconcile_pre_switch_state; then
        echo "[ERROR] Persistent blue/green supervisor exited before the switch boundary (rc=$supervisor_rc)" >&2
      fi
      return 1
      ;;
  esac
}

bluegreen_switch_unit_name() {
  printf 'jato-bluegreen-production.service\n'
}

assert_no_active_switch_unit() {
  local active_state=""
  local load_state=""
  local unit=""
  unit="$(bluegreen_switch_unit_name)"
  active_state="$(systemctl show "$unit" -p ActiveState --value 2>/dev/null || true)"
  load_state="$(systemctl show "$unit" -p LoadState --value 2>/dev/null || true)"
  case "$active_state" in
    active|activating|reloading|deactivating)
      fail "another persistent production blue/green controller is $active_state"
      return 1
      ;;
  esac
  if [[ -n "$load_state" && "$load_state" != "not-found" ]] \
    || [[ -n "$active_state" && "$active_state" != "inactive" && "$active_state" != "failed" ]]; then
    fail "global production blue/green unit is not safely reusable: load=${load_state:-unknown} active=${active_state:-unknown}"
    return 1
  fi
}

run_switch_supervisor() {
  local active_bundle_lock=""
  local active_main_pid=""
  local active_project_root=""
  local bash_bin=""
  local controller="$RELEASE_DIR/03_Scripts/deploy/tencent_bluegreen_release.sh"
  local helper="$RELEASE_DIR/03_Scripts/deploy/jato_quiescence_gate.py"
  local minimum_timeout=0
  local python_bin=""
  local quiescence_evidence="$BLUEGREEN_STATE_ROOT/quiescence-${DEPLOY_COMMIT_SHA}.json"
  local unit=""
  if ! [[ "$BLUEGREEN_CONTROLLER_TIMEOUT" =~ ^[1-9][0-9]*$ ]] \
    || ! [[ "$BLUEGREEN_QUIESCENCE_TIMEOUT" =~ ^[1-9][0-9]*$ ]] \
    || ! [[ "$BLUEGREEN_DRAIN_SECONDS" =~ ^[0-9]+$ ]]; then
    fail "blue/green controller, quiescence, and drain timeouts are malformed"
    return 1
  fi
  minimum_timeout=$((BLUEGREEN_QUIESCENCE_TIMEOUT + BLUEGREEN_DRAIN_SECONDS + 300))
  if [[ "$BLUEGREEN_CONTROLLER_TIMEOUT" -le "$minimum_timeout" ]]; then
    fail "persistent controller timeout must exceed quiescence plus switch recovery budget"
    return 1
  fi
  python_bin="$(command -v python3)" || return 1
  bash_bin="$(command -v bash)" || return 1
  active_main_pid="$(
    systemctl show "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" -p MainPID --value
  )" || return 1
  if ! [[ "$active_main_pid" =~ ^[0-9]+$ ]] \
    || [[ "$active_main_pid" -le 1 ]]; then
    fail "active backend MainPID cannot be proven before quiescence"
    return 1
  fi
  active_project_root="$(slot_release_root "$CURRENT_ACTIVE_SLOT")" || return 1
  active_bundle_lock="$active_project_root/04_Processed_data/active-bundle.lock"
  unit="$(bluegreen_switch_unit_name)"
  assert_no_active_switch_unit || return 1
  sudo -n systemd-run \
    --quiet \
    --wait \
    --collect \
    --service-type=exec \
    --unit="$unit" \
    --uid="$(id -u)" \
    --gid="$(id -g)" \
    --working-directory="$RELEASE_DIR" \
    --property="RuntimeMaxSec=${BLUEGREEN_CONTROLLER_TIMEOUT}s" \
    --property="TimeoutStopSec=120s" \
    --property="KillMode=control-group" \
    --setenv="BLUEGREEN_MODE=switch-locked" \
    --setenv="BLUEGREEN_ROOT=$BLUEGREEN_ROOT" \
    --setenv="RELEASES_ROOT=$RELEASES_ROOT" \
    --setenv="SLOTS_ROOT=$SLOTS_ROOT" \
    --setenv="SHARED_ROOT=$SHARED_ROOT" \
    --setenv="ACTIVE_RELEASE_LINK=$ACTIVE_RELEASE_LINK" \
    --setenv="BLUEGREEN_STATE_ROOT=$BLUEGREEN_STATE_ROOT" \
    --setenv="ACTIVE_SLOT_FILE=$ACTIVE_SLOT_FILE" \
    --setenv="DEPLOYMENT_MARKER=$DEPLOYMENT_MARKER" \
    --setenv="NGINX_ACTIVE_RELEASE_CONF=$NGINX_ACTIVE_RELEASE_CONF" \
    --setenv="SLOT_ENV_ROOT=$SLOT_ENV_ROOT" \
    --setenv="BACKEND_ENV_FILE=$BACKEND_ENV_FILE" \
    --setenv="LEGACY_ROOT=$LEGACY_ROOT" \
    --setenv="JATO_JOB_ROOT=$JATO_JOB_ROOT" \
    --setenv="BLUEGREEN_CANDIDATE_MEMORY_HIGH=$BLUEGREEN_CANDIDATE_MEMORY_HIGH" \
    --setenv="BLUEGREEN_CANDIDATE_MEMORY_MAX=$BLUEGREEN_CANDIDATE_MEMORY_MAX" \
    --setenv="BLUEGREEN_ACTIVE_MEMORY_HIGH=$BLUEGREEN_ACTIVE_MEMORY_HIGH" \
    --setenv="BLUEGREEN_ACTIVE_MEMORY_MAX=$BLUEGREEN_ACTIVE_MEMORY_MAX" \
    --setenv="BLUEGREEN_DRAIN_SECONDS=$BLUEGREEN_DRAIN_SECONDS" \
    --setenv="BLUEGREEN_CONTROLLER_TIMEOUT=$BLUEGREEN_CONTROLLER_TIMEOUT" \
    --setenv="BLUEGREEN_FAULT=$BLUEGREEN_FAULT" \
    --setenv="DEPLOY_COMMIT_SHA=$DEPLOY_COMMIT_SHA" \
    --setenv="DEPLOY_ARCHIVE_SHA256=$DEPLOY_ARCHIVE_SHA256" \
    --setenv="DEPLOY_ARCHIVE_BYTES=$DEPLOY_ARCHIVE_BYTES" \
    --setenv="DEPLOY_REPOSITORY=$DEPLOY_REPOSITORY" \
    --setenv="DEPLOY_RUN_ID=$DEPLOY_RUN_ID" \
    --setenv="DEPLOY_RUN_ATTEMPT=$DEPLOY_RUN_ATTEMPT" \
    --setenv="DEPLOY_BRANCH=$DEPLOY_BRANCH" \
    --setenv="DEPLOY_SERVER_NAME=${DEPLOY_SERVER_NAME:-_}" \
    --setenv="FRONTEND_ARTIFACT_IDENTITY=$FRONTEND_ARTIFACT_IDENTITY" \
    --setenv="FRONTEND_ARTIFACT_CHECKSUM=$FRONTEND_ARTIFACT_CHECKSUM" \
    --setenv="RELEASE_WORKTREE=$RELEASE_DIR" \
    --setenv="PREBUILT_FRONTEND_DIR=$RELEASE_DIR/06_AppPlatform/frontend/dist" \
    --setenv="CHECKPOINT_FILE=$CHECKPOINT_FILE" \
    --setenv="CHECKPOINT_JOURNAL=$CHECKPOINT_JOURNAL" \
    --setenv="SCHEDULER_STATE_FILE=$SCHEDULER_STATE_FILE" \
    --setenv="CURRENT_ACTIVE_SLOT=$CURRENT_ACTIVE_SLOT" \
    --setenv="CANDIDATE_SLOT=$CANDIDATE_SLOT" \
    --setenv="PREVIOUS_DEPLOY_RELEASE_FILE=${PREVIOUS_DEPLOY_RELEASE_FILE:-}" \
    "$python_bin" -B "$helper" hold \
      --job-root "$JATO_JOB_ROOT" \
      --active-main-pid "$active_main_pid" \
      --expected-project-root "$active_project_root" \
      --active-bundle-lock "$active_bundle_lock" \
      --marker "$DEPLOYMENT_MARKER" \
      --timeout "$BLUEGREEN_QUIESCENCE_TIMEOUT" \
      --evidence "$quiescence_evidence" \
      -- "$bash_bin" "$controller" switch-locked
}

switch_locked() {
  local candidate_conf=""
  local binding=""
  local dist="$RELEASE_DIR/06_AppPlatform/frontend/dist"
  if [[ "${JATO_QUIESCENCE_LOCK_HELD:-}" != "1" ]] \
    || [[ ! -f "${JATO_DEPLOYMENT_MARKER:-/nonexistent}" ]]; then
    fail "switch-locked requires the JATO quiescence supervisor"
  fi
  remove_candidate_sandbox_before_switch
  verify_switch_prerequisites
  SWITCH_BACKUP="$BLUEGREEN_STATE_ROOT/active-release.pre-${DEPLOY_COMMIT_SHA}.conf"
  durable_install_file "$NGINX_ACTIVE_RELEASE_CONF" "$SWITCH_BACKUP" 0600
  pause_schedulers
  binding="$(evidence_binding)"
  checkpoint_write switch_started in_progress rollback_required \
    "atomic Nginx blue/green switch started; $binding"
  candidate_conf="$(mktemp)"
  render_active_release "$candidate_conf" "$CANDIDATE_SLOT" "$dist"
  if [[ "$BLUEGREEN_FAULT" == "nginx_test" ]]; then
    printf '\ninvalid_directive_for_fault_injection;\n' >> "$candidate_conf"
  fi
  if ! durable_install_file "$candidate_conf" "$NGINX_ACTIVE_RELEASE_CONF" 0644; then
    rm -f "$candidate_conf"
    return 1
  fi
  rm -f "$candidate_conf"
  if ! sudo -n nginx -t; then
    return 1
  fi
  if [[ "$BLUEGREEN_FAULT" == "nginx_reload" ]]; then
    return 1
  fi
  sudo -n systemctl reload nginx
  if [[ "$BLUEGREEN_FAULT" == "post_switch_readiness" ]] \
    || ! verify_nginx_candidate; then
    return 1
  fi
  checkpoint_write switched completed automatic \
    "Nginx serves the exact candidate while the old slot remains available for rollback; $binding"
  complete_candidate_activation "$binding"
}

prepare_and_switch() {
  local orphaned_scheduler_snapshot=false
  local supervisor_rc=0
  require_environment
  assert_inherited_production_lock
  ensure_bluegreen_state_root
  ensure_bluegreen_runtime_roots
  assert_no_active_switch_unit
  if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
    orphaned_scheduler_snapshot=true
    if ! resume_schedulers; then
      mark_maintenance_required || true
      return 1
    fi
  fi
  if ! read_checkpoint_phase_status; then
    mark_maintenance_required || true
    return 1
  fi
  case "$CHECKPOINT_PHASE" in
    switch_started|switched|rollback_started|backend_healthy|www_verified|intl_deploy_started|intl_verified|parity_verified|complete)
      if ! reconcile_existing_switch; then
        return 1
      fi
      if [[ "$orphaned_scheduler_snapshot" == "true" ]]; then
        echo "[ERROR] Restored an orphaned scheduler snapshot and reconciled the route; retry this release from a new attempt" >&2
        return 1
      fi
      return
      ;;
    rollback_completed)
      reconcile_existing_switch || true
      return 1
      ;;
  esac
  if [[ "$orphaned_scheduler_snapshot" == "true" ]]; then
    if [[ -e "$DEPLOYMENT_MARKER" || -L "$DEPLOYMENT_MARKER" ]]; then
      reconcile_pre_switch_state || true
    fi
    echo "[ERROR] Restored an orphaned scheduler snapshot; retry this release from a new attempt" >&2
    return 1
  fi
  if [[ -e "$DEPLOYMENT_MARKER" || -L "$DEPLOYMENT_MARKER" ]]; then
    reconcile_pre_switch_state || true
    echo "[ERROR] Reconciled a retained deployment marker; retry this release from a new attempt" >&2
    return 1
  fi
  resolve_active_slot
  resolve_current_frontend_root
  prepare_shared_runtime
  ensure_current_slot_restartable
  preserve_previous_release_metadata
  guard_release_storage
  assert_host_memory_budget
  materialize_release_source
  run_candidate_build_scope
  verify_final_runtime_seal
  assert_no_database_migration_delta
  assert_runtime_storage_reserve
  assert_host_memory_budget
  install_slot_runtime
  prepare_stable_nginx_boot_infrastructure
  verify_candidate
  verify_candidate_sandbox
  if [[ "$BLUEGREEN_FAULT" == "candidate_ready" ]]; then
    fail "fault injection: candidate_ready"
  fi
  arm_pre_switch_static_boot_safety

  sudo -n mkdir -p "$BLUEGREEN_STATE_ROOT"
  sudo -n chown "$(id -u):$(id -g)" "$BLUEGREEN_STATE_ROOT"
  sudo -n install -d -m 0755 -o "$(id -u)" -g "$(id -g)" \
    "$(dirname "$DEPLOYMENT_MARKER")"
  PRE_SUPERVISOR_CANDIDATE_ARMED=false
  set +e
  run_switch_supervisor
  supervisor_rc=$?
  set -e
  if ! reconcile_supervisor_result "$supervisor_rc"; then
    if [[ "$RELEASE_ROLLED_BACK" == "true" ]]; then
      clear_maintenance_marker || true
      echo "[ERROR] Candidate release failed and the exact previous release was restored" >&2
      if [[ "$supervisor_rc" -eq 0 ]]; then
        supervisor_rc=1
      fi
      return "$supervisor_rc"
    fi
    if [[ "$supervisor_rc" -eq 0 ]]; then
      supervisor_rc=1
    fi
    if [[ "$supervisor_rc" -eq "$EXIT_COMMAND_FAILED_MARKER_RETAINED" ]] \
      || [[ "$SWITCH_RECONCILED" != "true" ]]; then
      echo "[ERROR] Persistent blue/green supervisor failed; durable route reconciliation retained a healthy route behind the maintenance fence" >&2
      mark_maintenance_required || true
    else
      echo "[ERROR] Persistent blue/green supervisor failed before publishing the candidate" >&2
    fi
    return "$supervisor_rc"
  fi
  if [[ "$RELEASE_ROLLED_BACK" == "true" ]]; then
    clear_maintenance_marker || true
    echo "[ERROR] Candidate release failed and the exact previous release was restored" >&2
    if [[ "$supervisor_rc" -eq 0 ]]; then
      supervisor_rc=1
    fi
    return "$supervisor_rc"
  fi
  if [[ "$supervisor_rc" -ne 0 ]]; then
    echo "[WARN] Supervisor transport/evidence failed after the exact candidate reached durable healthy state" >&2
  fi
  clear_maintenance_marker
  python3 -B "$CHECKPOINT_HELPER" show --checkpoint "$CHECKPOINT_FILE" \
    | python3 -c \
      'import json,sys; p=json.load(sys.stdin); assert p["phase"] == "backend_healthy" and p["status"] == "completed"'
  if ! verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
    || ! verify_public_release_exact "$DEPLOY_COMMIT_SHA"; then
    mark_maintenance_required || true
    return 1
  fi
  if ! verify_active_monthly_gate_released "$CANDIDATE_SLOT"; then
    mark_maintenance_required || true
    return 1
  fi
  echo "[INFO] Tencent blue/green release completed: active=$CANDIDATE_SLOT old=$CURRENT_ACTIVE_SLOT"
}

case "$BLUEGREEN_MODE" in
  prepare-and-switch)
    trap prepare_exit_handler EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    prepare_and_switch
    trap - EXIT HUP INT TERM
    ;;
  switch-locked)
    # Values are exported explicitly by the quiescence supervisor command.
    require_environment
    if [[ "$CURRENT_ACTIVE_SLOT" != "8000" && "$CURRENT_ACTIVE_SLOT" != "8001" ]] \
      || [[ "$CANDIDATE_SLOT" != "8000" && "$CANDIDATE_SLOT" != "8001" ]] \
      || [[ "$CURRENT_ACTIVE_SLOT" == "$CANDIDATE_SLOT" ]]; then
      fail "locked switch slot identity is invalid"
    fi
    trap switch_exit_handler EXIT
    trap 'switch_signal_handler 1' HUP
    trap 'switch_signal_handler 2' INT
    trap 'switch_signal_handler 15' TERM
    switch_locked
    trap - EXIT HUP INT TERM
    ;;
  build-candidate-runtime)
    build_candidate_runtime_locked
    ;;
  *)
    fail "unknown Tencent blue/green mode: $BLUEGREEN_MODE"
    ;;
esac
