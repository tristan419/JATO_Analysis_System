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
BLUEGREEN_CANDIDATE_PREVIEW_PORT=18002
BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_HIGH=128M
BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_MAX=256M
BLUEGREEN_CANDIDATE_PREVIEW_TASKS_MAX=64
BLUEGREEN_CANDIDATE_BUILD_TIMEOUT="${BLUEGREEN_CANDIDATE_BUILD_TIMEOUT:-1800}"
BLUEGREEN_ACTIVE_MEMORY_HIGH="${BLUEGREEN_ACTIVE_MEMORY_HIGH:-6G}"
BLUEGREEN_ACTIVE_MEMORY_MAX="${BLUEGREEN_ACTIVE_MEMORY_MAX:-8G}"
BLUEGREEN_MIN_TOTAL_MEMORY_BYTES=$((14 * 1024 * 1024 * 1024))
BLUEGREEN_MIN_AVAILABLE_MEMORY_BYTES=$((5 * 1024 * 1024 * 1024))
BLUEGREEN_CANDIDATE_MAX_MEMORY_BYTES=$((4 * 1024 * 1024 * 1024))
BLUEGREEN_OS_MEMORY_RESERVE_BYTES=$((2 * 1024 * 1024 * 1024))
BLUEGREEN_ACTIVE_MEMORY_HIGH_BYTES=$((6 * 1024 * 1024 * 1024))
BLUEGREEN_ACTIVE_MEMORY_MAX_BYTES=$((8 * 1024 * 1024 * 1024))
BLUEGREEN_CANDIDATE_MEMORY_HIGH_BYTES=$((3 * 1024 * 1024 * 1024))
BLUEGREEN_CANDIDATE_MEMORY_MAX_BYTES=$((4 * 1024 * 1024 * 1024))
BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_HIGH_BYTES=$((128 * 1024 * 1024))
BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_MAX_BYTES=$((256 * 1024 * 1024))
BLUEGREEN_CANDIDATE_PREIMAGE_CACHE_MAX_BYTES=$((256 * 1024 * 1024))
BLUEGREEN_PREPARE_DISK_RESERVE_BYTES=$((15 * 1024 * 1024 * 1024))
BLUEGREEN_PREPARE_DISK_RESERVE_PERCENT=8
BLUEGREEN_RUNTIME_DISK_RESERVE_BYTES=$((10 * 1024 * 1024 * 1024))
BLUEGREEN_RUNTIME_DISK_RESERVE_PERCENT=5
BLUEGREEN_RELEASE_KEEP_UNREFERENCED=3
BLUEGREEN_RELEASE_NORMAL_GC_AGE_SECONDS=$((14 * 24 * 60 * 60))
BLUEGREEN_RELEASE_EMERGENCY_GC_AGE_SECONDS=$((24 * 60 * 60))
BLUEGREEN_FAULT="${BLUEGREEN_FAULT:-}"
BLUEGREEN_MODE="${1:-}"
SERVICE_PREFIX="jato-fullstack-backend@"
# One production release predates the immutable slot readiness contract.  The
# bridge is deliberately bound to the byte-for-byte server baseline collected
# on 2026-08-05.  It is only used to prove that this exact legacy Active stayed
# unchanged while a Candidate is prepared or discarded; it never authorizes an
# Active update or weakens readiness for an immutable release.
LEGACY_ACTIVE_BRIDGE_COMMIT_SHA="cd4557cb932374a0fefb6c80a5fac9fb75a67d62"
LEGACY_ACTIVE_BRIDGE_SLOT="8000"
LEGACY_ACTIVE_BRIDGE_ROOT="/opt/JATO_Analysis_System-main"
LEGACY_ACTIVE_BRIDGE_UNIT_FILE="/etc/systemd/system/jato-fullstack-backend@.service"
LEGACY_ACTIVE_BRIDGE_UNIT_SHA256="5318e4c5e55e9e8b586adf5f2bcb72581fb41514fca3f4c153ba7f7ba265618d"
LEGACY_ACTIVE_BRIDGE_MEMORY_DROPIN="/etc/systemd/system/jato-fullstack-backend@8000.service.d/20-memory-guard.conf"
LEGACY_ACTIVE_BRIDGE_MEMORY_DROPIN_SHA256="626b15c662a830df52d138895243cb5b3346834aefeb613fd7429ba2bf50a256"
LEGACY_ACTIVE_BRIDGE_SLOT_ENV="/etc/jato-fullstack/slots/8000.env"
LEGACY_ACTIVE_BRIDGE_SLOT_ENV_SHA256="6572ed1f04eedae85705eb386ce6229b325debe6389c14c1334a6684077391b8"
LEGACY_ACTIVE_BRIDGE_NGINX_SITE="/etc/nginx/sites-enabled/jato_fullstack.conf"
LEGACY_ACTIVE_BRIDGE_NGINX_SITE_SHA256="6f5e26e9e293d8024af90ed8446a1f8f1c5072567c38f050f86ec38524e2880d"
LEGACY_ACTIVE_BRIDGE_RELEASE_METADATA_SHA256="43a908bd071dcd9e9761d44435e6744580abd7729c14a4ea6f19071656b2714f"
LEGACY_ACTIVE_BRIDGE_BUILD_META_SHA256="3c1112aa4c2c6cdc6134f88618633fd13d840f4ec3a6d4a71dc164e6b96ee880"
LEGACY_ACTIVE_BRIDGE_PROVENANCE_SHA256="15ef7a2a571cbc9a4e1855e45d44a891f0cb609f0c5a5509cfd5d66e27b1f1ca"
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
    || [[ "$BLUEGREEN_STATE_ROOT" != "/var/lib/jato-release" ]] \
    || [[ "$CANDIDATE_PREIMAGE_ROOT" != "/var/lib/jato-release-preimages" ]] \
    || [[ "$SLOT_ENV_ROOT" != "/etc/jato-fullstack/slots" ]] \
    || [[ "$BLUEGREEN_CANDIDATE_MEMORY_HIGH" != "3G" ]] \
    || [[ "$BLUEGREEN_CANDIDATE_MEMORY_MAX" != "4G" ]] \
    || [[ "$BLUEGREEN_ACTIVE_MEMORY_HIGH" != "6G" ]] \
    || [[ "$BLUEGREEN_ACTIVE_MEMORY_MAX" != "8G" ]]; then
    fail "production blue/green paths and resource limits must use the reviewed canonical values"
  fi
  if [[ -n "${BLUEGREEN_CHECKPOINT_HELPER_OVERRIDE:-}" ]] \
    || [[ -n "${BLUEGREEN_STORAGE_GUARD_OVERRIDE:-}" ]] \
    || [[ -n "${BLUEGREEN_CANDIDATE_PREIMAGE_HELPER_OVERRIDE:-}" ]]; then
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
CANDIDATE_PREIMAGE_HELPER="${BLUEGREEN_CANDIDATE_PREIMAGE_HELPER_OVERRIDE:-$RELEASE_DIR/03_Scripts/deploy/candidate_runtime_preimage.py}"
CANDIDATE_PREIMAGE_ROOT="${CANDIDATE_PREIMAGE_ROOT:-/var/lib/jato-release-preimages}"
CANDIDATE_PREIMAGE_DIR="$CANDIDATE_PREIMAGE_ROOT/candidate-preimages/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256"
FIXED_ACTIVE_PREIMAGE_HELPER="$RELEASE_DIR/03_Scripts/deploy/fixed_active_preimage.py"
FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT="$BLUEGREEN_STATE_ROOT/fixed-active-legacy-bootstrap.completed"
READINESS_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/verify_backend_readiness.py"
QUIESCENCE_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/jato_quiescence_gate.py"
SYSTEMD_TEMPLATE="$RELEASE_WORKTREE/03_Scripts/deploy/systemd/jato-fullstack-backend@.service"
SHARED_BACKEND_TEMPLATE="/etc/systemd/system/jato-fullstack-backend@.service"
BACKEND_TEMPLATE_PREIMAGE="$BLUEGREEN_STATE_ROOT/backend-template.pre-${DEPLOY_COMMIT_SHA:-unknown}.service"
BACKEND_TEMPLATE_PREIMAGE_STATE="${BACKEND_TEMPLATE_PREIMAGE}.state"
SLOT_ENV_TEMPLATE="$RELEASE_WORKTREE/03_Scripts/deploy/systemd/jato-fullstack-backend-slot.env.example"
NGINX_INSTALLER="$RELEASE_WORKTREE/03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh"
CANDIDATE_PREVIEW_TEMPLATE="$RELEASE_DIR/03_Scripts/deploy/nginx/jato_candidate_preview.conf.example"
CANDIDATE_PREVIEW_STATE_DIR="$BLUEGREEN_STATE_ROOT/candidate-preview/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256"
CANDIDATE_PREVIEW_CONFIG="$CANDIDATE_PREVIEW_STATE_DIR/nginx.conf"
SOURCE_SEAL_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/verify_release_source_seal.py"
TOOLKIT_EGG_INFO_HELPER="$RELEASE_WORKTREE/03_Scripts/deploy/cleanup_toolkit_egg_info.py"
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
ACTIVE_UPDATE_HANDLER_ARMED=false
ACTIVE_UPDATE_TARGET_ENV=""
ACTIVE_UPDATE_TARGET_NGINX=""
FIXED_ACTIVE_SLOT_DIGEST=""
PREVIOUS_ACTIVE_RESTORE_ARMED=false

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
    return 1
  fi
  if ! digest="$(sha256sum "$EVIDENCE_FILE" | awk '{print $1}')"; then
    fail "release evidence digest cannot be calculated"
    return 1
  fi
  if [[ ! "$digest" =~ ^[0-9a-f]{64}$ ]]; then
    fail "release evidence digest is invalid"
    return 1
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

require_existing_active_slot_anchor() {
  if sudo -n test -L "$ACTIVE_SLOT_FILE" \
    || ! sudo -n test -f "$ACTIVE_SLOT_FILE"; then
    fail "failed Candidate discard requires an existing regular active-slot anchor"
    return 1
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
    sudo -n install -d -m 0711 "$RELEASE_DIR"
    (
      cd "$RELEASE_WORKTREE"
      tar cf - .
    ) | sudo -n tar --same-permissions --no-overwrite-dir \
      -xf - -C "$RELEASE_DIR"
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
    if ! python3 -B "$TOOLKIT_EGG_INFO_HELPER" \
      --toolkit-root "$RELEASE_DIR/07_ScrapingToolkit" \
      || sudo -n test -L "$RELEASE_SOURCE_SEAL_FILE" \
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
  umask 0022
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
      'import json,sys; payload=json.load(sys.stdin); print(payload["phase"], payload["status"], payload["retryClass"])'
}

read_checkpoint_phase_status() {
  local state=""
  state="$(checkpoint_phase_status)" || return 1
  if ! read -r CHECKPOINT_PHASE CHECKPOINT_STATUS CHECKPOINT_RETRY_CLASS \
    <<< "$state" \
    || [[ -z "$CHECKPOINT_PHASE" || -z "$CHECKPOINT_STATUS" \
      || -z "$CHECKPOINT_RETRY_CLASS" ]]; then
    fail "release checkpoint phase/status/retry class is unavailable"
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

wait_for_slot_release_exact() {
  local slot="$1"
  local expected_sha="$2"
  local attempt=0
  for attempt in $(seq 1 20); do
    if verify_slot_release_exact "$slot" "$expected_sha"; then
      return 0
    fi
    if [[ "$attempt" -lt 20 ]]; then
      sleep 3
    fi
  done
  fail "slot $slot did not become ready with exact release $expected_sha"
}

legacy_active_bridge_check_file_digest() {
  local field="$1"
  local path="$2"
  local expected="$3"
  local actual=""
  if sudo -n test -L "$path" || ! sudo -n test -f "$path"; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=%s expected=regular-file actual=missing-or-unsafe path=%s\n' \
      "$field" "$path" >&2
    return 1
  fi
  actual="$(sudo -n sha256sum -- "$path" | awk '{print $1}')" || return 1
  if [[ "$actual" != "$expected" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=%s expected=%s actual=%s path=%s\n' \
      "$field" "$expected" "${actual:-unavailable}" "$path" >&2
    return 1
  fi
}

legacy_active_bridge_check_unit() {
  local expected_unit="${SERVICE_PREFIX}${LEGACY_ACTIVE_BRIDGE_SLOT}"
  local properties=""
  properties="$(
    systemctl show "$expected_unit" \
      -p FragmentPath -p UnitFileState -p ActiveState -p SubState \
      -p MainPID -p ExecStart -p WorkingDirectory -p Environment \
      -p EnvironmentFiles 2>/dev/null
  )" || return 1
  python3 -B - \
    "$properties" \
    "$LEGACY_ACTIVE_BRIDGE_UNIT_FILE" \
    "$LEGACY_ROOT" \
    "$LEGACY_ACTIVE_BRIDGE_SLOT" <<'PY'
import json
import re
import shlex
import sys

raw, expected_fragment, legacy_root, slot = sys.argv[1:]
properties = {}
for line in raw.splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value

expected = {
    "FragmentPath": expected_fragment,
    "UnitFileState": "enabled",
    "ActiveState": "active",
    "SubState": "running",
    "WorkingDirectory": f"{legacy_root}/06_AppPlatform/backend",
}
mismatches = []
for field, value in expected.items():
    if properties.get(field) != value:
        mismatches.append(
            {
                "field": f"systemd.{field}",
                "expected": value,
                "actual": properties.get(field),
            }
        )

try:
    environment_tokens = shlex.split(properties.get("Environment", ""))
except ValueError:
    environment_tokens = []
environment = {}
for token in environment_tokens:
    key, separator, value = token.partition("=")
    if separator:
        environment[key] = value
expected_environment = {
    "APP_BACKEND_WORKERS": "2",
    "APP_PROJECT_ROOT": legacy_root,
    "PYTHONPATH": f"{legacy_root}/06_AppPlatform/backend",
}
for field, value in expected_environment.items():
    if environment.get(field) != value:
        mismatches.append(
            {
                "field": f"systemd.Environment.{field}",
                "expected": value,
                "actual": environment.get(field),
            }
        )

exec_start = properties.get("ExecStart", "")
required_exec_tokens = (
    f"path={legacy_root}/.venv/bin/python",
    f"argv[]={legacy_root}/.venv/bin/python -m uvicorn app.main:app",
    f"--host 127.0.0.1 --port {slot}",
    "--workers ${APP_BACKEND_WORKERS}",
)
for token in required_exec_tokens:
    if token not in exec_start:
        mismatches.append(
            {
                "field": "systemd.ExecStart",
                "expected": token,
                "actual": "redacted-argv-mismatch",
            }
        )

if re.fullmatch(r"[1-9][0-9]*", properties.get("MainPID", "")) is None:
    mismatches.append(
        {
            "field": "systemd.MainPID",
            "expected": "live-positive-pid",
            "actual": properties.get("MainPID"),
        }
    )
if properties.get("EnvironmentFiles") != (
    "/etc/jato-fullstack/backend.env (ignore_errors=yes)"
):
    mismatches.append(
        {
            "field": "systemd.EnvironmentFiles",
            "expected": "/etc/jato-fullstack/backend.env (ignore_errors=yes)",
            "actual": properties.get("EnvironmentFiles"),
        }
    )

if mismatches:
    print(
        json.dumps(
            {
                "decision": "legacy-active-bridge-rejected",
                "category": "legacy-active-systemd-drift",
                "fieldDiffs": mismatches,
                "mutationPerformed": False,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    raise SystemExit(1)
PY
}

legacy_active_bridge_check_readyz_absent() {
  local body=""
  local status=""
  body="$(mktemp)" || return 1
  if ! status="$(
    curl --noproxy '*' --silent --show-error --output "$body" \
      --write-out '%{http_code}' --max-time 10 \
      "http://127.0.0.1:${LEGACY_ACTIVE_BRIDGE_SLOT}/readyz"
  )"; then
    rm -f "$body"
    fail "legacy Active /readyz probe failed instead of returning exact HTTP 404"
    return 1
  fi
  if ! python3 -B - "$body" "$status" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
status = sys.argv[2]
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError):
    payload = None
if status != "404" or payload != {"detail": "Not Found"}:
    print(
        json.dumps(
            {
                "decision": "legacy-active-bridge-rejected",
                "category": "legacy-readyz-contract-drift",
                "fieldDiffs": [
                    {
                        "field": "direct.readyz",
                        "expected": {
                            "status": 404,
                            "body": {"detail": "Not Found"},
                        },
                        "actual": {
                            "status": status,
                            "body": payload,
                        },
                    }
                ],
                "mutationPerformed": False,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    raise SystemExit(1)
PY
  then
    rm -f "$body"
    return 1
  fi
  rm -f "$body"
}

verify_legacy_public_release_exact() {
  local build_payload=""
  local health_payload=""
  local normalized_names=""
  local provenance_payload=""
  local server_name=""
  local server_names=()
  health_payload="$(mktemp)" || return 1
  build_payload="$(mktemp)" || {
    rm -f "$health_payload"
    return 1
  }
  provenance_payload="$(mktemp)" || {
    rm -f "$health_payload" "$build_payload"
    return 1
  }
  if ! normalized_names="$(normalized_deploy_server_names "${DEPLOY_SERVER_NAME:-}")"; then
    rm -f "$health_payload" "$build_payload" "$provenance_payload"
    return 1
  fi
  mapfile -t server_names <<< "$normalized_names"
  for server_name in "${server_names[@]}"; do
    if [[ "$server_name" == "_" ]]; then
      if ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
        -H 'Host: localhost' http://127.0.0.1/healthz > "$health_payload" \
        || ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
          -H 'Host: localhost' http://127.0.0.1/build-meta.json \
          > "$build_payload" \
        || ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
          -H 'Host: localhost' http://127.0.0.1/release-provenance.json \
          > "$provenance_payload"; then
        rm -f "$health_payload" "$build_payload" "$provenance_payload"
        return 1
      fi
    else
      if ! curl --noproxy '*' --fail --silent --show-error --location \
        --proto '=https' --proto-redir '=https' --max-time 20 \
        --resolve "${server_name}:443:127.0.0.1" \
        "https://${server_name}/healthz" > "$health_payload" \
        || ! curl --noproxy '*' --fail --silent --show-error --location \
          --proto '=https' --proto-redir '=https' --max-time 20 \
          --resolve "${server_name}:443:127.0.0.1" \
          "https://${server_name}/build-meta.json" > "$build_payload" \
        || ! curl --noproxy '*' --fail --silent --show-error --location \
          --proto '=https' --proto-redir '=https' --max-time 20 \
          --resolve "${server_name}:443:127.0.0.1" \
          "https://${server_name}/release-provenance.json" \
          > "$provenance_payload"; then
        rm -f "$health_payload" "$build_payload" "$provenance_payload"
        return 1
      fi
    fi
    if ! python3 -B - \
      "$health_payload" \
      "$build_payload" \
      "$provenance_payload" \
      "$LEGACY_ACTIVE_BRIDGE_COMMIT_SHA" \
      "$server_name" <<'PY'
import json
from pathlib import Path
import sys

health = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
build = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
provenance = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
expected_sha = sys.argv[4]
server_name = sys.argv[5]
build_commits = {
    str(build.get(name) or "")
    for name in ("deployCommit", "githubSha", "commitSha", "sha")
}
source = provenance.get("source")
if health != {"status": "ok"}:
    raise SystemExit(
        f"[ERROR] legacy public /healthz changed for {server_name}"
    )
if expected_sha not in build_commits:
    raise SystemExit(
        f"[ERROR] legacy public build-meta does not bind {expected_sha} "
        f"for {server_name}"
    )
if not isinstance(source, dict) or any(
    source.get(field) != expected_sha
    for field in ("appCommit", "deployCommit", "githubSha")
):
    raise SystemExit(
        f"[ERROR] legacy public provenance does not bind {expected_sha} "
        f"for {server_name}"
    )
PY
    then
      rm -f "$health_payload" "$build_payload" "$provenance_payload"
      return 1
    fi
  done
  rm -f "$health_payload" "$build_payload" "$provenance_payload"
}

legacy_active_bridge_check_identity() {
  local failure=0
  local main_pid=""
  local process_cwd=""
  local slot_link="$SLOTS_ROOT/$LEGACY_ACTIVE_BRIDGE_SLOT/current"
  if [[ "$CURRENT_ACTIVE_SLOT" != "$LEGACY_ACTIVE_BRIDGE_SLOT" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=activeSlot expected=%s actual=%s\n' \
      "$LEGACY_ACTIVE_BRIDGE_SLOT" "${CURRENT_ACTIVE_SLOT:-unset}" >&2
    failure=1
  fi
  if [[ "$PREVIOUS_RELEASE_ROOT" != "$LEGACY_ROOT" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=releaseRoot expected=%s actual=%s\n' \
      "$LEGACY_ROOT" "${PREVIOUS_RELEASE_ROOT:-unset}" >&2
    failure=1
  fi
  if [[ "$LEGACY_ROOT" != "$LEGACY_ACTIVE_BRIDGE_ROOT" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=canonicalLegacyRoot expected=%s actual=%s\n' \
      "$LEGACY_ACTIVE_BRIDGE_ROOT" "$LEGACY_ROOT" >&2
    failure=1
  fi
  if [[ "$PREVIOUS_RELEASE_SHA" != "$LEGACY_ACTIVE_BRIDGE_COMMIT_SHA" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=commitSha expected=%s actual=%s\n' \
      "$LEGACY_ACTIVE_BRIDGE_COMMIT_SHA" "${PREVIOUS_RELEASE_SHA:-unset}" >&2
    failure=1
  fi
  if sudo -n test -L "$ACTIVE_SLOT_FILE" \
    || ! sudo -n test -f "$ACTIVE_SLOT_FILE" \
    || [[ "$(sudo -n cat "$ACTIVE_SLOT_FILE" 2>/dev/null || true)" \
      != "$LEGACY_ACTIVE_BRIDGE_SLOT" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=activeSlotAnchor expected=regular-8000 actual=drifted\n' \
      >&2
    failure=1
  fi
  if ! sudo -n test -L "$slot_link" \
    || [[ "$(sudo -n realpath "$slot_link" 2>/dev/null || true)" != "$LEGACY_ROOT" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=slotLink expected=%s actual=drifted\n' \
      "$LEGACY_ROOT" >&2
    failure=1
  fi
  if sudo -n test -e "$ACTIVE_RELEASE_LINK" \
    || sudo -n test -L "$ACTIVE_RELEASE_LINK"; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=activeReleaseLink expected=absent actual=present\n' \
      >&2
    failure=1
  fi
  if sudo -n test -e "$NGINX_ACTIVE_RELEASE_CONF" \
    || sudo -n test -L "$NGINX_ACTIVE_RELEASE_CONF"; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=activeReleaseConf expected=absent actual=present\n' \
      >&2
    failure=1
  fi
  if sudo -n test -e "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT" \
    || sudo -n test -L "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT"; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=bootstrapReceipt expected=absent actual=present\n' \
      >&2
    failure=1
  fi
  if ! legacy_active_bridge_check_file_digest \
    systemd.unit \
    "$LEGACY_ACTIVE_BRIDGE_UNIT_FILE" \
    "$LEGACY_ACTIVE_BRIDGE_UNIT_SHA256"; then
    failure=1
  fi
  if ! legacy_active_bridge_check_file_digest \
    systemd.memoryDropIn \
    "$LEGACY_ACTIVE_BRIDGE_MEMORY_DROPIN" \
    "$LEGACY_ACTIVE_BRIDGE_MEMORY_DROPIN_SHA256"; then
    failure=1
  fi
  if ! legacy_active_bridge_check_file_digest \
    active.slotEnv \
    "$LEGACY_ACTIVE_BRIDGE_SLOT_ENV" \
    "$LEGACY_ACTIVE_BRIDGE_SLOT_ENV_SHA256"; then
    failure=1
  fi
  if ! legacy_active_bridge_check_file_digest \
    nginx.enabledSite \
    "$LEGACY_ACTIVE_BRIDGE_NGINX_SITE" \
    "$LEGACY_ACTIVE_BRIDGE_NGINX_SITE_SHA256"; then
    failure=1
  fi
  if ! legacy_active_bridge_check_file_digest \
    release.metadata \
    "$LEGACY_ROOT/hermes/deploy_release.json" \
    "$LEGACY_ACTIVE_BRIDGE_RELEASE_METADATA_SHA256"; then
    failure=1
  fi
  if ! legacy_active_bridge_check_file_digest \
    frontend.buildMeta \
    "$LEGACY_ROOT/06_AppPlatform/frontend/dist/build-meta.json" \
    "$LEGACY_ACTIVE_BRIDGE_BUILD_META_SHA256"; then
    failure=1
  fi
  if ! legacy_active_bridge_check_file_digest \
    frontend.provenance \
    "$LEGACY_ROOT/06_AppPlatform/frontend/dist/release-provenance.json" \
    "$LEGACY_ACTIVE_BRIDGE_PROVENANCE_SHA256"; then
    failure=1
  fi
  if ! legacy_active_bridge_check_unit; then
    failure=1
  fi
  main_pid="$(
    systemctl show \
      "${SERVICE_PREFIX}${LEGACY_ACTIVE_BRIDGE_SLOT}" \
      -p MainPID --value 2>/dev/null || true
  )"
  if [[ "$main_pid" =~ ^[1-9][0-9]*$ ]]; then
    process_cwd="$(sudo -n readlink -f "/proc/$main_pid/cwd" 2>/dev/null || true)"
  fi
  if [[ "$process_cwd" != "$LEGACY_ROOT/06_AppPlatform/backend" ]]; then
    printf \
      '[ERROR] legacy Active bridge mismatch: field=process.cwd expected=%s actual=%s\n' \
      "$LEGACY_ROOT/06_AppPlatform/backend" "${process_cwd:-unavailable}" >&2
    failure=1
  fi
  if ! curl --noproxy '*' -fsS --max-time 10 \
    "http://127.0.0.1:${LEGACY_ACTIVE_BRIDGE_SLOT}/healthz" >/dev/null; then
    fail "legacy Active direct /healthz is not healthy"
    failure=1
  fi
  if ! legacy_active_bridge_check_readyz_absent; then
    failure=1
  fi
  if ! verify_legacy_public_release_exact; then
    failure=1
  fi
  if ! verify_active_cgroup "$LEGACY_ACTIVE_BRIDGE_SLOT"; then
    failure=1
  fi
  if ! verify_active_monthly_gate_released "$LEGACY_ACTIVE_BRIDGE_SLOT"; then
    failure=1
  fi
  if [[ "$failure" -ne 0 ]]; then
    fail "legacy Active bridge preflight rejected; no Candidate mutation is authorized"
    return 1
  fi
  printf \
    '[INFO] Exact legacy Active bridge verified: sha=%s slot=%s mutationPerformed=false\n' \
    "$LEGACY_ACTIVE_BRIDGE_COMMIT_SHA" "$LEGACY_ACTIVE_BRIDGE_SLOT"
}

verify_previous_active_runtime_exact() {
  if [[ "$PREVIOUS_RELEASE_ROOT" == "$LEGACY_ROOT" ]]; then
    legacy_active_bridge_check_identity
    return
  fi
  verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA" \
    && verify_public_release_exact "$PREVIOUS_RELEASE_SHA" \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState enabled \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState active \
    && verify_active_cgroup "$CURRENT_ACTIVE_SLOT" \
    && verify_durable_route_ownership \
      "$CURRENT_ACTIVE_SLOT" \
      "$PREVIOUS_RELEASE_ROOT" \
      "$PREVIOUS_RELEASE_SHA" \
      "$PREVIOUS_RELEASE_ROOT/06_AppPlatform/frontend/dist" \
    && verify_active_monthly_gate_released "$CURRENT_ACTIVE_SLOT"
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
  RUN_DATABASE_MIGRATIONS=verify_only \
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
  umask 0022
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
      'set -Eeuo pipefail; set -a; . "$1"; set +a; export PYTHONPATH="$2"; export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c default_transaction_read_only=on"; cd "$2"; "$3" -m alembic current' \
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

ensure_candidate_preimage_root() {
  if sudo -n test -L "$CANDIDATE_PREIMAGE_ROOT" \
    || {
      sudo -n test -e "$CANDIDATE_PREIMAGE_ROOT" \
        && ! sudo -n test -d "$CANDIDATE_PREIMAGE_ROOT";
    }; then
    fail "Candidate preimage root must be a real directory"
    return 1
  fi
  if ! sudo -n test -e "$CANDIDATE_PREIMAGE_ROOT"; then
    sudo -n install -d -m 0700 -o root -g root "$CANDIDATE_PREIMAGE_ROOT" \
      || return 1
  fi
  sudo -n python3 -B - "$CANDIDATE_PREIMAGE_ROOT" <<'PY'
from pathlib import Path
import stat
import sys

path = Path(sys.argv[1])
metadata = path.lstat()
if (
    path.is_symlink()
    or not stat.S_ISDIR(metadata.st_mode)
    or metadata.st_uid != 0
    or metadata.st_gid != 0
    or stat.S_IMODE(metadata.st_mode) & 0o077
):
    raise SystemExit("[ERROR] Candidate preimage root must remain root:root and 0700")
PY
}

candidate_runtime_preimage_command() {
  local command="$1"
  shift
  if [[ ! -f "$CANDIDATE_PREIMAGE_HELPER" ]] \
    || [[ -L "$CANDIDATE_PREIMAGE_HELPER" ]]; then
    fail "Candidate runtime preimage helper is missing or unsafe"
    return 1
  fi
  sudo -n python3 -B "$CANDIDATE_PREIMAGE_HELPER" "$command" \
    --preimage "$CANDIDATE_PREIMAGE_DIR" \
    --commit "$DEPLOY_COMMIT_SHA" \
    --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
    --candidate-slot "$CANDIDATE_SLOT" \
    --slot-link "$SLOTS_ROOT/$CANDIDATE_SLOT/current" \
    --slot-link-stage "$SLOTS_ROOT/$CANDIDATE_SLOT/.current.jato-candidate-installing" \
    --slot-env "$SLOT_ENV_ROOT/$CANDIDATE_SLOT.env" \
    --slot-env-stage "$SLOT_ENV_ROOT/.$CANDIDATE_SLOT.env.jato-candidate-installing" \
    --explicit-unit "/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service" \
    --explicit-unit-stage "/etc/systemd/system/.${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.jato-candidate-installing" \
    --instance-dropins "/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.d" \
    --persistent-control-dropins "/etc/systemd/system.control/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.d" \
    --runtime-control-dropins "/run/systemd/system.control/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.d" \
    --candidate-cache-link "/var/cache/jato-candidate-$CANDIDATE_SLOT" \
    --candidate-cache-private "/var/cache/private/jato-candidate-$CANDIDATE_SLOT" \
    "$@"
}

candidate_runtime_is_quiescent() {
  local active_state=""
  local load_state=""
  local main_pid=""
  local listeners=""
  local unit="${SERVICE_PREFIX}${CANDIDATE_SLOT}"
  local unit_file_state=""
  local unit_state_is_safe=false
  load_state="$(systemctl show "$unit" -p LoadState --value 2>/dev/null || true)"
  active_state="$(systemctl show "$unit" -p ActiveState --value 2>/dev/null || true)"
  unit_file_state="$(systemctl show "$unit" -p UnitFileState --value 2>/dev/null || true)"
  main_pid="$(systemctl show "$unit" -p MainPID --value 2>/dev/null || true)"
  if ! listeners="$(ss -H -ltn "sport = :$CANDIDATE_SLOT" 2>/dev/null)"; then
    fail "cannot prove the Candidate listener state"
    return 1
  fi
  if [[ "$load_state" == "loaded" ]] \
    && [[ "$unit_file_state" == "disabled" ]]; then
    unit_state_is_safe=true
  elif [[ "$load_state" == "not-found" ]] \
    && {
      [[ -z "$unit_file_state" ]] \
        || [[ "$unit_file_state" == "not-found" ]];
    }; then
    unit_state_is_safe=true
  fi
  if [[ "$active_state" != "inactive" ]] \
    || [[ "$main_pid" != "0" ]] \
    || [[ "$unit_state_is_safe" != "true" ]] \
    || [[ -e "/etc/systemd/system/multi-user.target.wants/$unit" ]] \
    || [[ -L "/etc/systemd/system/multi-user.target.wants/$unit" ]] \
    || [[ -n "$listeners" ]]; then
    fail "Candidate runtime is not inactive, disabled/not-found, PID 0, and listener-free"
    return 1
  fi
}

prepare_candidate_runtime_preimage() {
  local env_source="$1"
  local sandbox_source="$2"
  ensure_candidate_preimage_root || return 1
  if sudo -n test -e "$CANDIDATE_PREIMAGE_DIR" \
    || sudo -n test -L "$CANDIDATE_PREIMAGE_DIR"; then
    restore_candidate_runtime_preimage || return 1
  else
    candidate_runtime_is_quiescent || return 1
  fi
  candidate_runtime_preimage_command capture \
    --post-slot-link-target "$RELEASE_DIR" \
    --post-env-source "$env_source" \
    --post-unit-source "$SYSTEMD_TEMPLATE" \
    --post-sandbox-source "$sandbox_source" \
    --post-memory-high-bytes "$BLUEGREEN_CANDIDATE_MEMORY_HIGH_BYTES" \
    --post-memory-max-bytes "$BLUEGREEN_CANDIDATE_MEMORY_MAX_BYTES" \
    --post-cpu-quota-percent 100 \
    --post-active-memory-high-bytes "$BLUEGREEN_ACTIVE_MEMORY_HIGH_BYTES" \
    --post-active-memory-max-bytes "$BLUEGREEN_ACTIVE_MEMORY_MAX_BYTES" \
    --post-active-cpu-quota-percent 200 \
    --candidate-cache-max-bytes "$BLUEGREEN_CANDIDATE_PREIMAGE_CACHE_MAX_BYTES" \
    || return 1
  candidate_runtime_preimage_command verify-live
}

restore_candidate_runtime_preimage() {
  if ! sudo -n test -d "$CANDIDATE_PREIMAGE_DIR" \
    || sudo -n test -L "$CANDIDATE_PREIMAGE_DIR"; then
    fail "Candidate runtime preimage is unavailable for exact cleanup"
    return 1
  fi
  sudo -n systemctl disable --now \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 || true
  sudo -n systemctl reset-failed \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 || true
  candidate_runtime_is_quiescent || return 1
  candidate_runtime_preimage_command restore || return 1
  sudo -n systemctl daemon-reload || return 1
  candidate_runtime_is_quiescent || return 1
  candidate_runtime_preimage_command verify-live
}

discard_candidate_runtime_preimage() {
  candidate_runtime_preimage_command discard
}

candidate_durable_install_file() {
  local source="$1"
  local target="$2"
  local mode="$3"
  local stage="$4"
  local create_target_parent="${5:-false}"
  sudo -n python3 -B - \
    "$source" "$target" "$mode" "$stage" "$create_target_parent" <<'PY'
import os
from pathlib import Path
import shutil
import stat
import sys

source = Path(sys.argv[1])
target = Path(sys.argv[2])
mode = int(sys.argv[3], 8)
stage = Path(sys.argv[4])
create_target_parent = sys.argv[5] == "true"

source_metadata = source.lstat()
if (
    not stat.S_ISREG(source_metadata.st_mode)
    or source.is_symlink()
    or source_metadata.st_nlink != 1
):
    raise SystemExit("[ERROR] Candidate source must be one regular file")
for parent, may_create in (
    (stage.parent, False),
    (target.parent, create_target_parent),
):
    try:
        parent_metadata = parent.lstat()
    except FileNotFoundError:
        if not may_create:
            raise SystemExit(f"[ERROR] Candidate target parent is absent: {parent}")
        parent.mkdir(mode=0o755)
        parent_metadata = parent.lstat()
    if stat.S_ISLNK(parent_metadata.st_mode) or not stat.S_ISDIR(
        parent_metadata.st_mode
    ):
        raise SystemExit(f"[ERROR] Candidate target parent is unsafe: {parent}")
if stage.exists() or stage.is_symlink():
    raise SystemExit(f"[ERROR] Candidate staging already exists: {stage}")

flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(stage, flags, 0o600)
try:
    source_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    source_flags |= getattr(os, "O_NOFOLLOW", 0)
    source_descriptor = os.open(source, source_flags)
    try:
        with os.fdopen(source_descriptor, "rb") as reader, os.fdopen(
            descriptor,
            "wb",
        ) as writer:
            source_descriptor = -1
            descriptor = -1
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fchmod(writer.fileno(), mode)
            os.fsync(writer.fileno())
    finally:
        if source_descriptor >= 0:
            os.close(source_descriptor)
    after = source.lstat()
    if (
        source_metadata.st_dev,
        source_metadata.st_ino,
        source_metadata.st_size,
        source_metadata.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise SystemExit("[ERROR] Candidate source changed during installation")
    os.replace(stage, target)
    for parent in {stage.parent, target.parent}:
        directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        directory_flags |= getattr(os, "O_DIRECTORY", 0)
        directory_descriptor = os.open(parent, directory_flags)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
finally:
    if descriptor >= 0:
        os.close(descriptor)
    try:
        stage_metadata = stage.lstat()
    except FileNotFoundError:
        pass
    else:
        if (
            not stat.S_ISREG(stage_metadata.st_mode)
            or stage_metadata.st_uid != os.geteuid()
            or stage_metadata.st_nlink != 1
        ):
            raise SystemExit("[ERROR] Candidate staging changed during cleanup")
        stage.unlink()
        directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        directory_flags |= getattr(os, "O_DIRECTORY", 0)
        directory_descriptor = os.open(stage.parent, directory_flags)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
PY
}

candidate_atomic_symlink() {
  local target="$1"
  local link_path="$2"
  local stage="$3"
  sudo -n python3 -B - "$target" "$link_path" "$stage" <<'PY'
import os
from pathlib import Path
import stat
import sys

target = sys.argv[1]
link = Path(sys.argv[2])
stage = Path(sys.argv[3])
parent_metadata = link.parent.lstat()
if stat.S_ISLNK(parent_metadata.st_mode) or not stat.S_ISDIR(
    parent_metadata.st_mode
):
    raise SystemExit("[ERROR] Candidate slot parent is unsafe")
if stage.parent != link.parent:
    raise SystemExit("[ERROR] Candidate symlink staging must be same-directory")
if stage.exists() or stage.is_symlink():
    raise SystemExit(f"[ERROR] Candidate symlink staging already exists: {stage}")
os.symlink(target, stage)
directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
directory_flags |= getattr(os, "O_DIRECTORY", 0)
try:
    directory_descriptor = os.open(link.parent, directory_flags)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    os.replace(stage, link)
    directory_descriptor = os.open(link.parent, directory_flags)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
finally:
    try:
        stage_metadata = stage.lstat()
    except FileNotFoundError:
        pass
    else:
        if not stat.S_ISLNK(stage_metadata.st_mode) or stage_metadata.st_uid != os.geteuid():
            raise SystemExit("[ERROR] Candidate symlink staging changed during cleanup")
        stage.unlink()
        directory_descriptor = os.open(link.parent, directory_flags)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
PY
}

install_slot_runtime() {
  local service_target="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service"
  local service_stage="/etc/systemd/system/.${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.jato-candidate-installing"
  local env_target="$SLOT_ENV_ROOT/$CANDIDATE_SLOT.env"
  local env_stage="$SLOT_ENV_ROOT/.$CANDIDATE_SLOT.env.jato-candidate-installing"
  local sandbox_dropin="/etc/systemd/system/${SERVICE_PREFIX}${CANDIDATE_SLOT}.service.d/10-candidate-sandbox.conf"
  local slot_link="$SLOTS_ROOT/$CANDIDATE_SLOT/current"
  local slot_link_stage="$SLOTS_ROOT/$CANDIDATE_SLOT/.current.jato-candidate-installing"
  local sandbox_temp=""
  local env_temp=""
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
  sandbox_temp="$(mktemp)"
  {
    echo "[Service]"
    echo "# Deliberately empty: Candidate uses the same data access as Active."
  } > "$sandbox_temp"
  if ! prepare_candidate_runtime_preimage "$env_temp" "$sandbox_temp"; then
    rm -f "$env_temp" "$sandbox_temp"
    return 1
  fi
  PRE_SUPERVISOR_CANDIDATE_ARMED=true
  if ! candidate_durable_install_file \
    "$SYSTEMD_TEMPLATE" "$service_target" 0644 "$service_stage"; then
    rm -f "$env_temp" "$sandbox_temp"
    return 1
  fi
  if ! candidate_durable_install_file \
    "$env_temp" "$env_target" 0600 "$env_stage"; then
    rm -f "$env_temp" "$sandbox_temp"
    return 1
  fi
  rm -f "$env_temp"
  durable_remove_path "$sandbox_dropin" || return 1
  rm -f "$sandbox_temp"
  sudo -n systemctl stop "${SERVICE_PREFIX}${CANDIDATE_SLOT}" >/dev/null 2>&1 || true
  candidate_atomic_symlink "$RELEASE_DIR" "$slot_link" "$slot_link_stage"
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

verify_candidate_data_access_contract() {
  local dropin="/etc/systemd/system/$SERVICE_PREFIX$CANDIDATE_SLOT.service.d/10-candidate-sandbox.conf"
  local active_main_pid=""
  local active_property=""
  local active_unit="$SERVICE_PREFIX$CURRENT_ACTIVE_SLOT"
  local candidate_main_pid=""
  local candidate_property=""
  local candidate_unit="$SERVICE_PREFIX$CANDIDATE_SLOT"
  local property=""
  if sudo -n test -e "$dropin" || sudo -n test -L "$dropin"; then
    fail "Candidate retained a data-access override drop-in"
    return 1
  fi
  for property in \
    DynamicUser ProtectSystem ProtectHome PrivateTmp PrivateDevices \
    NoNewPrivileges RestrictNamespaces ReadOnlyPaths ReadWritePaths; do
    candidate_property="$(
      systemctl show "$candidate_unit" -p "$property" --value
    )" || return 1
    active_property="$(
      systemctl show "$active_unit" -p "$property" --value
    )" || return 1
    if [[ "$candidate_property" != "$active_property" ]]; then
      fail "Candidate $property differs from Active data-access isolation"
      return 1
    fi
  done
  candidate_main_pid="$(
    systemctl show "$candidate_unit" -p MainPID --value
  )" || return 1
  active_main_pid="$(
    systemctl show "$active_unit" -p MainPID --value
  )" || return 1
  if [[ ! "$candidate_main_pid" =~ ^[1-9][0-9]*$ ]] \
    || [[ ! "$active_main_pid" =~ ^[1-9][0-9]*$ ]] \
    || ! sudo -n python3 -B - \
      "$candidate_main_pid" "$active_main_pid" <<'PY'
import os
from pathlib import Path
import sys


def process_environment(pid: str) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for token in (Path("/proc") / pid / "environ").read_bytes().split(b"\0"):
        key, separator, value = token.partition(b"=")
        if separator:
            result[key.decode("ascii", "strict")] = value
    return result


def effective_path(environment: dict[str, bytes], key: str) -> bytes | None:
    value = environment.get(key)
    if value is not None:
        return value
    project_root = environment.get("APP_PROJECT_ROOT")
    if project_root is None:
        return None
    defaults = {
        "JATO_PARQUET_PATH": "04_Processed_data/jato_full_archive.parquet",
        "JATO_PARTITIONED_PATH": "04_Processed_data/partitioned_dataset_v1",
        "APP_CRUD_DATA_PATH": "04_Processed_data/app_entities.json",
        "APP_ENGINEERING_IMPORT_ROOT": "01_RAW_DATA",
        "MSRP_GOVERNANCE_EVIDENCE_ROOT": (
            "04_Processed_data/ops/msrp_source_evidence"
        ),
        "APP_LOCAL_WIKI_DB_PATH": "04_Processed_data/chroma_db",
    }
    root = Path(os.fsdecode(project_root))
    if not root.is_absolute():
        return None
    return os.fsencode(root / defaults[key])


def same_runtime_path(candidate_value: bytes | None, active_value: bytes | None) -> bool:
    if candidate_value == active_value:
        return candidate_value is not None
    if candidate_value is None or active_value is None:
        return False
    candidate_path = Path(os.fsdecode(candidate_value))
    active_path = Path(os.fsdecode(active_value))
    if not candidate_path.is_absolute() or not active_path.is_absolute():
        return False
    candidate_exists = candidate_path.exists()
    active_exists = active_path.exists()
    if candidate_exists != active_exists:
        return False
    if candidate_exists:
        try:
            return os.path.samefile(candidate_path, active_path)
        except OSError:
            return False
    return candidate_path.resolve(strict=False) == active_path.resolve(strict=False)


candidate = process_environment(sys.argv[1])
active = process_environment(sys.argv[2])
exact_keys = (
    "APP_DATABASE_ENABLED",
    "APP_DATABASE_URL",
    "APP_REDIS_ENABLED",
    "APP_REDIS_URL",
    "PGOPTIONS",
)
path_keys = (
    "JATO_PARQUET_PATH",
    "JATO_PARTITIONED_PATH",
    "APP_CRUD_DATA_PATH",
    "APP_ENGINEERING_IMPORT_ROOT",
    "MSRP_GOVERNANCE_EVIDENCE_ROOT",
    "APP_LOCAL_WIKI_DB_PATH",
)
for key in exact_keys:
    if candidate.get(key) != active.get(key):
        raise SystemExit(
            f"[ERROR] Candidate and Active differ for data connection key {key}"
        )
for key in path_keys:
    if not same_runtime_path(
        effective_path(candidate, key),
        effective_path(active, key),
    ):
        raise SystemExit(
            f"[ERROR] Candidate and Active resolve to different runtime paths for {key}"
        )
singleton_expectations = {
    "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED": b"false",
    "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED": b"false",
    "APP_METADATA_PREWARM_ENABLED": b"false",
    "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED": b"false",
    "HERMES_RUN_ENABLED": b"false",
}
for key, expected in singleton_expectations.items():
    if candidate.get(key, b"false").lower() != expected:
        raise SystemExit(f"[ERROR] Candidate singleton task gate is open: {key}")
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
  verify_backend_cgroup_processes_only "$CANDIDATE_SLOT"
}

verify_active_cgroup() {
  local active_slot="${1:-}"
  local expected_high=$((6 * 1024 * 1024 * 1024))
  local expected_max=$((8 * 1024 * 1024 * 1024))
  local actual_high=""
  local actual_max=""
  if [[ "$active_slot" != "8000" && "$active_slot" != "8001" ]]; then
    fail "active cgroup verification requires an explicit 8000 or 8001 slot"
    return 1
  fi
  actual_high="$(systemctl show "${SERVICE_PREFIX}${active_slot}" -p MemoryHigh --value)"
  actual_max="$(systemctl show "${SERVICE_PREFIX}${active_slot}" -p MemoryMax --value)"
  if [[ "$BLUEGREEN_ACTIVE_MEMORY_HIGH" == "6G" && "$actual_high" != "$expected_high" ]]; then
    fail "active MemoryHigh is not 6G: $actual_high"
  fi
  if [[ "$BLUEGREEN_ACTIVE_MEMORY_MAX" == "8G" && "$actual_max" != "$expected_max" ]]; then
    fail "active MemoryMax is not 8G: $actual_max"
  fi
  sudo -n grep -Fxq "APP_BACKEND_WORKERS=2" "$SLOT_ENV_ROOT/$active_slot.env" \
    || fail "active slot must retain exactly two configured Uvicorn workers"
  verify_backend_cgroup_processes_only "$active_slot"
}

verify_backend_cgroup_processes_only() {
  local slot="${1:-}"
  if [[ "$slot" != "8000" && "$slot" != "8001" ]]; then
    fail "backend cgroup process verification requires an explicit 8000 or 8001 slot"
    return 1
  fi
  python3 - "${SERVICE_PREFIX}${slot}" <<'PY'
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

verify_active_monthly_gate_held() {
  local active_slot="$1"
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
  if ! python3 -B - "$body" "$status" "$active_slot" <<'PY'
import json
from pathlib import Path
import sys

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as exc:
    raise SystemExit(f"[ERROR] held monthly gate returned invalid JSON: {exc}")
detail = payload.get("detail") if isinstance(payload, dict) else None
if (
    sys.argv[2] != "423"
    or not isinstance(detail, dict)
    or detail.get("code") != "JATO_MONTHLY_DISABLED"
    or detail.get("enabled") is not False
    or detail.get("releaseSlot") != sys.argv[3]
    or detail.get("reason") != "deployment_in_progress"
    or detail.get("activeSlot") is not None
):
    raise SystemExit("[ERROR] Active monthly gate is not held by deployment")
PY
  then
    rm -f "$body"
    return 1
  fi
  rm -f "$body"
}

verify_quiescence_hold_context() {
  if [[ "${JATO_QUIESCENCE_LOCK_HELD:-}" != "1" ]] \
    || [[ "${JATO_DEPLOYMENT_MARKER:-}" != "$DEPLOYMENT_MARKER" ]] \
    || [[ ! -f "$DEPLOYMENT_MARKER" ]] \
    || [[ -L "$DEPLOYMENT_MARKER" ]]; then
    fail "fixed Active mutation requires the persistent JATO quiescence hold"
    return 1
  fi
}

verify_candidate() {
  verify_final_runtime_seal || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled || return 1
  if [[ "$BLUEGREEN_FAULT" == "candidate_start" ]]; then
    fail "fault injection: candidate_start"
  fi
  sudo -n systemctl start "${SERVICE_PREFIX}${CANDIDATE_SLOT}"
  wait_for_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA"
  verify_candidate_monthly_gate || return 1
  verify_candidate_cgroup || return 1
  unit_property_equals \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled || return 1
  verify_final_runtime_seal || return 1
}

candidate_preview_unit_name() {
  printf 'jato-candidate-preview-%s-%s.service\n' \
    "${DEPLOY_COMMIT_SHA:0:12}" "${DEPLOY_ARCHIVE_SHA256:0:12}"
}

candidate_preview_runtime_directory() {
  printf 'jato-candidate-preview-%s-%s\n' \
    "${DEPLOY_COMMIT_SHA:0:12}" "${DEPLOY_ARCHIVE_SHA256:0:12}"
}

candidate_preview_runtime_root() {
  printf '/run/%s\n' "$(candidate_preview_runtime_directory)"
}

candidate_preview_identity() {
  printf '%s:%s:%s:%s\n' \
    "$DEPLOY_COMMIT_SHA" \
    "$DEPLOY_ARCHIVE_SHA256" \
    "$CANDIDATE_SLOT" \
    "$BLUEGREEN_CANDIDATE_PREVIEW_PORT"
}

candidate_preview_nginx_bin() {
  printf '/usr/sbin/nginx\n'
}

ensure_candidate_preview_state_dir() {
  local component=""
  local current="$BLUEGREEN_STATE_ROOT"
  for component in \
    candidate-preview \
    "$DEPLOY_COMMIT_SHA" \
    "$DEPLOY_ARCHIVE_SHA256"; do
    current="$current/$component"
    if sudo -n test -L "$current" \
      || {
        sudo -n test -e "$current" \
          && ! sudo -n test -d "$current";
      }; then
      fail "Candidate preview state path is unsafe: $current"
      return 1
    fi
    sudo -n install -d -m 0755 -o "$(id -u)" -g "$(id -g)" "$current" \
      || return 1
  done
}

render_candidate_preview_config() {
  local output="$1"
  local frontend_root="$RELEASE_DIR/06_AppPlatform/frontend/dist"
  local runtime_root=""
  runtime_root="$(candidate_preview_runtime_root)" || return 1
  python3 -B - \
    "$CANDIDATE_PREVIEW_TEMPLATE" \
    "$output" \
    "$frontend_root" \
    "$runtime_root" \
    "$DEPLOY_COMMIT_SHA" \
    "$DEPLOY_ARCHIVE_SHA256" \
    "$CANDIDATE_SLOT" \
    "$BLUEGREEN_CANDIDATE_PREVIEW_PORT" <<'PY'
import json
import os
from pathlib import Path
import re
import stat
import sys

(
    template_name,
    output_name,
    frontend_root,
    runtime_root,
    commit,
    archive_sha256,
    slot_raw,
    port_raw,
) = sys.argv[1:]
template = Path(template_name)
output = Path(output_name)
try:
    template_metadata = template.lstat()
except FileNotFoundError as exc:
    raise SystemExit("[ERROR] Candidate preview Nginx template is missing") from exc
if template.is_symlink() or not stat.S_ISREG(template_metadata.st_mode):
    raise SystemExit("[ERROR] Candidate preview Nginx template is unsafe")
if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
    raise SystemExit("[ERROR] Candidate preview commit SHA is malformed")
if re.fullmatch(r"[0-9a-f]{64}", archive_sha256) is None:
    raise SystemExit("[ERROR] Candidate preview archive SHA-256 is malformed")
if slot_raw not in {"8000", "8001"}:
    raise SystemExit("[ERROR] Candidate preview slot is malformed")
if port_raw != "18002":
    raise SystemExit("[ERROR] Candidate preview port must remain 18002")
expected_frontend = re.fullmatch(
    r"/opt/jato/releases/[0-9a-f]{40}/[0-9a-f]{64}"
    r"/06_AppPlatform/frontend/dist",
    frontend_root,
)
if expected_frontend is None or not (Path(frontend_root) / "index.html").is_file():
    raise SystemExit("[ERROR] Candidate preview frontend root is invalid")
if re.fullmatch(
    r"/run/jato-candidate-preview-[0-9a-f]{12}-[0-9a-f]{12}",
    runtime_root,
) is None:
    raise SystemExit("[ERROR] Candidate preview runtime root is invalid")
if output.is_symlink():
    raise SystemExit("[ERROR] Candidate preview render target must not be a symlink")
payload = json.dumps(
    {
        "role": "candidate",
        "commitSha": commit,
        "archiveSha256": archive_sha256,
        "candidateSlot": int(slot_raw),
        "previewPort": int(port_raw),
    },
    separators=(",", ":"),
    sort_keys=True,
)
replacements = {
    "__CANDIDATE_PREVIEW_RUNTIME_ROOT__": runtime_root,
    "__CANDIDATE_PREVIEW_PORT__": port_raw,
    "__CANDIDATE_FRONTEND_ROOT__": frontend_root,
    "__CANDIDATE_PREVIEW_JSON__": payload,
    "__CANDIDATE_SLOT__": slot_raw,
}
rendered = template.read_text(encoding="utf-8")
placeholders = set(re.findall(r"__[A-Z0-9_]+__", rendered))
if placeholders != set(replacements):
    raise SystemExit("[ERROR] Candidate preview template placeholders are invalid")
for placeholder, value in replacements.items():
    if not value or placeholder not in rendered:
        raise SystemExit("[ERROR] Candidate preview template replacement is empty")
    rendered = rendered.replace(placeholder, value)
if re.search(r"__[A-Z0-9_]+__", rendered):
    raise SystemExit("[ERROR] Candidate preview template retained a placeholder")
flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
if hasattr(os, "O_NOFOLLOW"):
    flags |= os.O_NOFOLLOW
descriptor = os.open(output, flags, 0o600)
with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
    handle.write(rendered)
    handle.flush()
    os.fsync(handle.fileno())
PY
}

candidate_preview_port_is_unused() {
  local listeners=""
  if ! listeners="$(
    ss -H -ltn "sport = :$BLUEGREEN_CANDIDATE_PREVIEW_PORT" 2>/dev/null
  )"; then
    fail "cannot inspect the Candidate preview listener"
    return 1
  fi
  if [[ -n "$listeners" ]]; then
    fail "Candidate preview port is already in use"
    return 1
  fi
}

candidate_preview_listener_is_loopback_only() {
  local listeners=""
  if ! listeners="$(
    ss -H -ltn "sport = :$BLUEGREEN_CANDIDATE_PREVIEW_PORT" 2>/dev/null
  )"; then
    fail "cannot inspect the Candidate preview listener"
    return 1
  fi
  python3 -B - "$listeners" "$BLUEGREEN_CANDIDATE_PREVIEW_PORT" <<'PY'
import sys

lines = [line.split() for line in sys.argv[1].splitlines() if line.strip()]
expected = f"127.0.0.1:{sys.argv[2]}"
if len(lines) != 1 or len(lines[0]) < 4 or lines[0][3] != expected:
    raise SystemExit(
        "[ERROR] Candidate preview must own exactly one IPv4 loopback listener"
    )
PY
}

verify_candidate_preview_unit() {
  local expected_argv=()
  local identity=""
  local nginx_bin=""
  local properties=""
  local runtime_root=""
  local unit=""
  identity="$(candidate_preview_identity)" || return 1
  nginx_bin="$(candidate_preview_nginx_bin)" || return 1
  runtime_root="$(candidate_preview_runtime_root)" || return 1
  unit="$(candidate_preview_unit_name)" || return 1
  expected_argv=(
    "$nginx_bin"
    -c "$CANDIDATE_PREVIEW_CONFIG"
    -p "$runtime_root/"
  )
  properties="$(
    systemctl show "$unit" \
      -p LoadState -p ActiveState -p UnitFileState -p FragmentPath \
      -p MainPID -p InvocationID -p ExecStart -p Environment \
      -p DynamicUser -p ProtectSystem -p ProtectHome -p NoNewPrivileges \
      -p Restart -p MemoryHigh -p MemoryMax -p MemorySwapMax -p TasksMax \
      -p ControlGroup -p ReadOnlyPaths -p ReadWritePaths -p BindsTo -p After
  )" || return 1
  python3 -B - \
    "$properties" \
    "$unit" \
    "$identity" \
    "$RELEASE_DIR" \
    "$CANDIDATE_PREVIEW_CONFIG" \
    "$runtime_root" \
    "$BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_HIGH_BYTES" \
    "$BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_MAX_BYTES" \
    "$BLUEGREEN_CANDIDATE_PREVIEW_TASKS_MAX" \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}.service" \
    "${#expected_argv[@]}" \
    "${expected_argv[@]}" <<'PY'
import re
import shlex
import sys

(
    raw,
    unit,
    identity,
    release_dir,
    config,
    runtime_root,
    memory_high,
    memory_max,
    tasks_max,
    candidate_unit,
) = sys.argv[1:11]
argv_count = int(sys.argv[11])
expected_argv = sys.argv[12 : 12 + argv_count]
properties = {}
for line in raw.splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value
exec_start = properties.get("ExecStart", "")
if exec_start.count("argv[]=") != 1:
    raise SystemExit("[ERROR] Candidate preview ExecStart is ambiguous")
try:
    actual_argv = shlex.split(
        exec_start.split("argv[]=", 1)[1].split(" ; ", 1)[0]
    )
    environment_tokens = shlex.split(properties.get("Environment", ""))
except ValueError as exc:
    raise SystemExit("[ERROR] Candidate preview unit metadata is malformed") from exc
environment = {}
for token in environment_tokens:
    key, separator, value = token.partition("=")
    if not separator or not key or key in environment:
        raise SystemExit("[ERROR] Candidate preview environment is malformed")
    environment[key] = value
required = {
    "LoadState": "loaded",
    "ActiveState": "active",
    "UnitFileState": "transient",
    "FragmentPath": f"/run/systemd/transient/{unit}",
    "DynamicUser": "yes",
    "ProtectSystem": "strict",
    "ProtectHome": "yes",
    "NoNewPrivileges": "yes",
    "Restart": "no",
    "MemoryHigh": memory_high,
    "MemoryMax": memory_max,
    "MemorySwapMax": "0",
    "TasksMax": tasks_max,
    "ControlGroup": f"/system.slice/{unit}",
}
for key, expected in required.items():
    if properties.get(key) != expected:
        raise SystemExit(f"[ERROR] Candidate preview property {key} is not exact")
if actual_argv != expected_argv:
    raise SystemExit("[ERROR] Candidate preview ExecStart argv is not exact")
if environment.get("JATO_CANDIDATE_PREVIEW_ID") != identity:
    raise SystemExit("[ERROR] Candidate preview environment identity is not exact")
if candidate_unit not in properties.get("BindsTo", "").split():
    raise SystemExit("[ERROR] Candidate preview BindsTo identity is not exact")
if candidate_unit not in properties.get("After", "").split():
    raise SystemExit("[ERROR] Candidate preview After identity is not exact")
if release_dir not in properties.get("ReadOnlyPaths", ""):
    raise SystemExit("[ERROR] Candidate preview release path is not read-only")
if config not in properties.get("ReadOnlyPaths", ""):
    raise SystemExit("[ERROR] Candidate preview config path is not read-only")
if runtime_root not in properties.get("ReadWritePaths", ""):
    raise SystemExit("[ERROR] Candidate preview runtime path is not writable")
if re.fullmatch(r"[1-9][0-9]*", properties.get("MainPID", "")) is None:
    raise SystemExit("[ERROR] Candidate preview MainPID is not live")
invocation_id = properties.get("InvocationID", "")
if (
    re.fullmatch(r"[0-9A-Fa-f]{32}", invocation_id) is None
    or invocation_id.lower() == "0" * 32
):
    raise SystemExit("[ERROR] Candidate preview InvocationID is not live")
PY
}

verify_candidate_preview_http() {
  local base="http://127.0.0.1:$BLUEGREEN_CANDIDATE_PREVIEW_PORT"
  local build_payload=""
  local headers=""
  local preview_payload=""
  local ready_payload=""
  build_payload="$(mktemp)"
  headers="$(mktemp)"
  preview_payload="$(mktemp)"
  ready_payload="$(mktemp)"
  if ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
    --dump-header "$headers" \
    --output "$preview_payload" \
    "$base/candidate-preview.json" \
    || ! python3 -B - \
      "$preview_payload" \
      "$headers" \
      "$DEPLOY_COMMIT_SHA" \
      "$DEPLOY_ARCHIVE_SHA256" \
      "$CANDIDATE_SLOT" \
      "$BLUEGREEN_CANDIDATE_PREVIEW_PORT" <<'PY'
import json
from pathlib import Path
import sys

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
headers = Path(sys.argv[2]).read_text(encoding="iso-8859-1").lower().splitlines()
expected = {
    "role": "candidate",
    "commitSha": sys.argv[3],
    "archiveSha256": sys.argv[4],
    "candidateSlot": int(sys.argv[5]),
    "previewPort": int(sys.argv[6]),
}
if payload != expected:
    raise SystemExit("[ERROR] Candidate preview metadata is not exact")
if not any(line.startswith("content-type: application/json") for line in headers):
    raise SystemExit("[ERROR] Candidate preview metadata lacks JSON content type")
if not any(
    line.startswith("cache-control:") and "no-store" in line for line in headers
):
    raise SystemExit("[ERROR] Candidate preview metadata is cacheable")
PY
  then
    rm -f "$build_payload" "$headers" "$preview_payload" "$ready_payload"
    return 1
  fi
  if ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
    "$base/readyz" > "$ready_payload" \
    || ! curl --noproxy '*' --fail --silent --show-error --max-time 20 \
      "$base/build-meta.json" > "$build_payload" \
    || ! verify_nginx_payloads \
      "$ready_payload" "$build_payload" "candidate-preview"; then
    rm -f "$build_payload" "$headers" "$preview_payload" "$ready_payload"
    return 1
  fi
  rm -f "$build_payload" "$headers" "$preview_payload" "$ready_payload"
}

verify_candidate_preview() {
  verify_candidate_preview_unit \
    && candidate_preview_listener_is_loopback_only \
    && verify_candidate_preview_http
}

start_candidate_preview() {
  local config_temp=""
  local identity=""
  local load_state=""
  local nginx_bin=""
  local runtime_directory=""
  local runtime_root=""
  local unit=""
  unit="$(candidate_preview_unit_name)" || return 1
  runtime_directory="$(candidate_preview_runtime_directory)" || return 1
  runtime_root="$(candidate_preview_runtime_root)" || return 1
  identity="$(candidate_preview_identity)" || return 1
  nginx_bin="$(candidate_preview_nginx_bin)" || return 1
  if [[ ! -x "$nginx_bin" ]] \
    || [[ "$(realpath "$(command -v nginx)")" != "$nginx_bin" ]]; then
    fail "Candidate preview requires the canonical /usr/sbin/nginx executable"
    return 1
  fi
  load_state="$(
    systemctl show "$unit" -p LoadState --value 2>/dev/null || true
  )"
  if [[ "$load_state" != "not-found" ]] \
    || systemctl is-active --quiet "$unit"; then
    fail "Candidate preview transient unit name is already in use"
    return 1
  fi
  candidate_preview_port_is_unused || return 1
  ensure_candidate_preview_state_dir || return 1
  config_temp="$(mktemp)"
  if ! render_candidate_preview_config "$config_temp" \
    || ! durable_install_file "$config_temp" "$CANDIDATE_PREVIEW_CONFIG" 0644; then
    rm -f "$config_temp"
    return 1
  fi
  rm -f "$config_temp"
  sudo -n systemd-run \
    --quiet \
    --collect \
    --unit="$unit" \
    --service-type=exec \
    --working-directory="$RELEASE_DIR" \
    --property="BindsTo=${SERVICE_PREFIX}${CANDIDATE_SLOT}.service" \
    --property="After=${SERVICE_PREFIX}${CANDIDATE_SLOT}.service" \
    --property="Restart=no" \
    --property="DynamicUser=yes" \
    --property="ProtectSystem=strict" \
    --property="ProtectHome=yes" \
    --property="PrivateTmp=yes" \
    --property="PrivateDevices=yes" \
    --property="NoNewPrivileges=yes" \
    --property="CapabilityBoundingSet=" \
    --property="AmbientCapabilities=" \
    --property="RestrictNamespaces=yes" \
    --property="ProtectKernelTunables=yes" \
    --property="ProtectKernelModules=yes" \
    --property="ProtectKernelLogs=yes" \
    --property="ProtectControlGroups=yes" \
    --property="LockPersonality=yes" \
    --property="RestrictRealtime=yes" \
    --property="RestrictSUIDSGID=yes" \
    --property="MemoryHigh=$BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_HIGH" \
    --property="MemoryMax=$BLUEGREEN_CANDIDATE_PREVIEW_MEMORY_MAX" \
    --property="MemorySwapMax=0" \
    --property="CPUQuota=25%" \
    --property="TasksMax=$BLUEGREEN_CANDIDATE_PREVIEW_TASKS_MAX" \
    --property="RuntimeDirectory=$runtime_directory" \
    --property="ReadOnlyPaths=$RELEASE_DIR $CANDIDATE_PREVIEW_CONFIG" \
    --property="ReadWritePaths=$runtime_root" \
    --setenv="JATO_CANDIDATE_PREVIEW_ID=$identity" \
    "$nginx_bin" \
      -c "$CANDIDATE_PREVIEW_CONFIG" \
      -p "$runtime_root/" || return 1
  for _attempt in $(seq 1 20); do
    if verify_candidate_preview; then
      return 0
    fi
    sleep 1
  done
  fail "Candidate preview did not become healthy on 127.0.0.1:18002"
}

stop_candidate_preview() {
  local current_load_state=""
  local identity=""
  local legacy_preview_argv=false
  local nginx_bin=""
  local properties=""
  local runtime_root=""
  local unit=""
  identity="$(candidate_preview_identity)" || return 1
  nginx_bin="$(candidate_preview_nginx_bin)" || return 1
  runtime_root="$(candidate_preview_runtime_root)" || return 1
  unit="$(candidate_preview_unit_name)" || return 1
  current_load_state="$(
    systemctl show "$unit" -p LoadState --value 2>/dev/null || true
  )"
  if [[ "$current_load_state" == "not-found" ]]; then
    candidate_preview_port_is_unused || return 1
    durable_remove_tree "$CANDIDATE_PREVIEW_STATE_DIR"
    return
  fi
  if [[ "$BLUEGREEN_MODE" == "discard-failed-candidate" ]]; then
    legacy_preview_argv=true
  fi
  properties="$(
    systemctl show "$unit" \
      -p LoadState -p UnitFileState -p FragmentPath -p ExecStart \
      -p Environment -p BindsTo -p After 2>/dev/null || true
  )"
  python3 -B - \
    "$properties" \
    "$unit" \
    "$identity" \
    "${SERVICE_PREFIX}${CANDIDATE_SLOT}.service" \
    "$nginx_bin" \
    "$CANDIDATE_PREVIEW_CONFIG" \
    "$runtime_root/" \
    "$legacy_preview_argv" <<'PY'
import shlex
import sys

(
    raw,
    unit,
    identity,
    candidate_unit,
    nginx,
    config,
    runtime_root,
    legacy_preview_argv,
) = sys.argv[1:]
properties = {}
for line in raw.splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value
exec_start = properties.get("ExecStart", "")
if exec_start.count("argv[]=") != 1:
    raise SystemExit("[ERROR] refusing to stop an ambiguous preview unit")
try:
    environment_tokens = shlex.split(properties.get("Environment", ""))
except ValueError as exc:
    raise SystemExit("[ERROR] refusing to stop a malformed preview unit") from exc
marker = " ; ignore_errors="
serialized = exec_start.split("argv[]=", 1)[1]
if marker not in serialized:
    raise SystemExit("[ERROR] refusing to stop a malformed preview unit")
try:
    actual_argv = shlex.split(serialized.split(marker, 1)[0])
except ValueError as exc:
    raise SystemExit("[ERROR] refusing to stop a malformed preview unit") from exc
environment = {}
for token in environment_tokens:
    key, separator, value = token.partition("=")
    if not separator or not key or key in environment:
        raise SystemExit("[ERROR] refusing to stop a malformed preview unit")
    environment[key] = value
expected_argv = [nginx, "-c", config, "-p", runtime_root]
allowed_argv = [expected_argv]
if legacy_preview_argv == "true":
    allowed_argv.append(expected_argv + ["-g", "daemon off;"])
required = {
    "LoadState": "loaded",
    "UnitFileState": "transient",
    "FragmentPath": f"/run/systemd/transient/{unit}",
}
for key, expected in required.items():
    if properties.get(key) != expected:
        raise SystemExit(
            f"[ERROR] refusing to stop a preview with unexpected {key}"
        )
if actual_argv not in allowed_argv:
    raise SystemExit("[ERROR] refusing to stop a preview with unexpected ExecStart")
if environment.get("JATO_CANDIDATE_PREVIEW_ID") != identity:
    raise SystemExit("[ERROR] refusing to stop a preview with unexpected identity")
if candidate_unit not in properties.get("BindsTo", "").split():
    raise SystemExit("[ERROR] refusing to stop a preview with unexpected BindsTo")
if candidate_unit not in properties.get("After", "").split():
    raise SystemExit("[ERROR] refusing to stop a preview with unexpected After")
PY
  sudo -n systemctl stop "$unit" >/dev/null || return 1
  sudo -n systemctl reset-failed "$unit" >/dev/null 2>&1 || true
  for _attempt in $(seq 1 30); do
    current_load_state="$(
      systemctl show "$unit" -p LoadState --value 2>/dev/null || true
    )"
    if [[ "$current_load_state" == "not-found" ]]; then
      candidate_preview_port_is_unused || return 1
      durable_remove_tree "$CANDIDATE_PREVIEW_STATE_DIR"
      return
    fi
    sleep 1
  done
  fail "Candidate preview transient unit was not collected"
}

candidate_ready_runtime_is_exact() {
  if [[ "$CHECKPOINT_PHASE" != "candidate_ready" ]] \
    || [[ "$CHECKPOINT_STATUS" != "completed" ]]; then
    return 1
  fi
  if [[ "$CANDIDATE_SLOT" != "8000" && "$CANDIDATE_SLOT" != "8001" ]] \
    && ! resolve_existing_candidate_slot; then
    return 1
  fi
  if [[ "$CURRENT_ACTIVE_SLOT" != "8000" && "$CURRENT_ACTIVE_SLOT" != "8001" ]]; then
    if [[ "$CANDIDATE_SLOT" == "8000" ]]; then
      CURRENT_ACTIVE_SLOT=8001
    else
      CURRENT_ACTIVE_SLOT=8000
    fi
  fi
  resolve_previous_release_identity \
    && verify_previous_active_runtime_exact \
    && verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState active \
    && verify_candidate_cgroup \
    && verify_candidate_monthly_gate \
    && verify_candidate_data_access_contract \
    && verify_final_runtime_seal \
    && verify_candidate_preview
}

candidate_ready_state_is_legal() {
  [[ ! -e "$DEPLOYMENT_MARKER" && ! -L "$DEPLOYMENT_MARKER" ]] \
    && [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && candidate_ready_runtime_is_exact \
    && verify_active_monthly_gate_released "$CURRENT_ACTIVE_SLOT"
}

candidate_ready_state_is_legal_under_hold() {
  verify_quiescence_hold_context \
    && [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && candidate_ready_runtime_is_exact \
    && verify_active_monthly_gate_held "$CURRENT_ACTIVE_SLOT"
}

validate_fixed_active_approval_identity() {
  if [[ ! "${DEPLOY_APPROVAL_RUN_ID:-}" =~ ^[1-9][0-9]*$ ]] \
    || [[ ! "${DEPLOY_APPROVAL_RUN_ATTEMPT:-}" =~ ^[1-9][0-9]*$ ]] \
    || [[ ! "${DEPLOY_CANDIDATE_ATTESTATION_SHA256:-}" =~ ^[0-9a-f]{64}$ ]]; then
    fail "fixed Active approval run identity and Candidate attestation are required"
    return 1
  fi
}

fixed_active_approval_binding() {
  validate_fixed_active_approval_identity || return 1
  printf 'candidate_run=%s/%s approval_run=%s/%s candidate_attestation_sha256=%s' \
    "$DEPLOY_RUN_ID" \
    "$DEPLOY_RUN_ATTEMPT" \
    "$DEPLOY_APPROVAL_RUN_ID" \
    "$DEPLOY_APPROVAL_RUN_ATTEMPT" \
    "$DEPLOY_CANDIDATE_ATTESTATION_SHA256"
}

capture_fixed_active_slot_anchor() {
  if sudo -n test -L "$ACTIVE_SLOT_FILE" \
    || ! sudo -n test -f "$ACTIVE_SLOT_FILE" \
    || [[ "$(sudo -n cat "$ACTIVE_SLOT_FILE")" != "$CURRENT_ACTIVE_SLOT" ]]; then
    fail "fixed Active approval cannot prove the active-slot anchor"
    return 1
  fi
  FIXED_ACTIVE_SLOT_DIGEST="$(sudo -n sha256sum "$ACTIVE_SLOT_FILE" | awk '{print $1}')" \
    || return 1
  if [[ ! "$FIXED_ACTIVE_SLOT_DIGEST" =~ ^[0-9a-f]{64}$ ]]; then
    fail "fixed Active active-slot anchor digest is malformed"
    return 1
  fi
}

verify_fixed_active_slot_anchor() {
  local actual_digest=""
  if [[ -z "$FIXED_ACTIVE_SLOT_DIGEST" ]]; then
    fail "fixed Active active-slot preimage was not captured"
    return 1
  fi
  if sudo -n test -L "$ACTIVE_SLOT_FILE" \
    || ! sudo -n test -f "$ACTIVE_SLOT_FILE" \
    || [[ "$(sudo -n cat "$ACTIVE_SLOT_FILE")" != "$CURRENT_ACTIVE_SLOT" ]]; then
    fail "fixed Active active-slot owner changed"
    return 1
  fi
  actual_digest="$(sudo -n sha256sum "$ACTIVE_SLOT_FILE" | awk '{print $1}')" \
    || return 1
  if [[ "$actual_digest" != "$FIXED_ACTIVE_SLOT_DIGEST" ]]; then
    fail "fixed Active active-slot contents changed"
    return 1
  fi
}

render_fixed_active_env() {
  local output="$1"
  if [[ ! -f "$SLOT_ENV_TEMPLATE" || -L "$SLOT_ENV_TEMPLATE" ]]; then
    fail "fixed Active slot env template is missing or unsafe"
    return 1
  fi
  sed \
    -e "s|__SLOT__|$CURRENT_ACTIVE_SLOT|g" \
    -e "s|__RELEASE_SHA__|$DEPLOY_COMMIT_SHA|g" \
    "$SLOT_ENV_TEMPLATE" > "$output"
  {
    printf '\nAPP_JATO_MONTHLY_ENABLED=true\n'
    printf 'APP_JATO_MONTHLY_UPDATE_JOB_ROOT=%s\n' "$JATO_JOB_ROOT"
    printf 'APP_JATO_MONTHLY_ACTIVE_SLOT_FILE=%s\n' "$ACTIVE_SLOT_FILE"
    printf 'APP_JATO_MONTHLY_DEPLOYMENT_MARKER=%s\n' "$DEPLOYMENT_MARKER"
    printf 'APP_JATO_MONTHLY_EXECUTION_MODE=subprocess\n'
  } >> "$output"
  chmod 0600 "$output"
}

verify_fixed_active_unit_compatibility() {
  local active_unit="${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}"
  local fragment=""
  local sandbox_dropin="/etc/systemd/system/${active_unit}.service.d/10-candidate-sandbox.conf"
  fragment="$(systemctl show "$active_unit" -p FragmentPath --value)" \
    || return 1
  case "$fragment" in
    "$SHARED_BACKEND_TEMPLATE"|"/etc/systemd/system/${active_unit}.service") ;;
    *)
      fail "fixed Active unit fragment is outside the reviewed template paths: $fragment"
      return 1
      ;;
  esac
  if sudo -n test -L "$fragment" \
    || ! sudo -n test -f "$fragment" \
    || ! sudo -n cmp -s "$SYSTEMD_TEMPLATE" "$fragment"; then
    fail "fixed Active approval refuses an untested systemd template change"
    return 1
  fi
  if sudo -n test -e "$sandbox_dropin" \
    || sudo -n test -L "$sandbox_dropin"; then
    fail "fixed Active slot unexpectedly retains a Candidate sandbox"
    return 1
  fi
}

fixed_active_preimage_path() {
  printf '%s/active-update-preimages/%s/%s\n' \
    "$BLUEGREEN_STATE_ROOT" "$DEPLOY_COMMIT_SHA" "$DEPLOY_ARCHIVE_SHA256"
}

fixed_active_previous_release_proof() {
  local archive=""
  local frontend_checksum=""
  local frontend_identity=""
  local frontend_identity_b64=""
  local relative=""
  local receipt_state=""
  local runtime_digest=""
  local runtime_seal=""
  local source_digest=""
  local source_seal=""
  local values=""
  if [[ "$PREVIOUS_RELEASE_ROOT" == "$RELEASES_ROOT/"* ]]; then
    relative="${PREVIOUS_RELEASE_ROOT#"$RELEASES_ROOT/"}"
    archive="${relative#*/}"
    if [[ "${relative%%/*}" != "$PREVIOUS_RELEASE_SHA" ]] \
      || [[ ! "$archive" =~ ^[0-9a-f]{64}$ ]] \
      || [[ "$archive" == */* ]]; then
      fail "fixed Active previous content-addressed release identity is invalid"
      return 1
    fi
    source_seal="$PREVIOUS_RELEASE_ROOT/.jato-source-seal.json"
    runtime_seal="$PREVIOUS_RELEASE_ROOT/.jato-runtime-seal.json"
    values="$({
      sudo -n python3 -B - \
        "$source_seal" "$runtime_seal" \
        "$PREVIOUS_RELEASE_SHA" "$archive" <<'PY'
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

source = Path(sys.argv[1])
runtime = Path(sys.argv[2])
digests = []
for path in (source, runtime):
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or metadata.st_nlink != 1
        or metadata.st_size <= 0
        or metadata.st_size > 64 * 1024 * 1024
    ):
        raise SystemExit("[ERROR] previous release seal is unsafe")
    digests.append(hashlib.sha256(path.read_bytes()).hexdigest())
payload = json.loads(runtime.read_text(encoding="utf-8"))
identity = payload.get("releaseIdentity")
if not isinstance(identity, dict):
    raise SystemExit("[ERROR] previous runtime seal identity is missing")
frontend_identity = str(identity.get("frontendIdentity") or "")
frontend_checksum = str(identity.get("frontendChecksum") or "")
if (
    identity.get("commit") != sys.argv[3]
    or identity.get("archiveSha256") != sys.argv[4]
    or not frontend_identity
    or len(frontend_identity) > 2048
    or re.fullmatch(r"[0-9a-f]{64}", frontend_checksum) is None
    or payload.get("sourceSealSha256") != digests[0]
):
    raise SystemExit("[ERROR] previous runtime seal identity is invalid")
print(digests[0])
print(digests[1])
print(base64.urlsafe_b64encode(frontend_identity.encode()).decode())
print(frontend_checksum)
PY
    })" || return 1
    source_digest="$(printf '%s\n' "$values" | sed -n '1p')"
    runtime_digest="$(printf '%s\n' "$values" | sed -n '2p')"
    frontend_identity_b64="$(printf '%s\n' "$values" | sed -n '3p')"
    frontend_checksum="$(printf '%s\n' "$values" | sed -n '4p')"
    frontend_identity="$(
      python3 -B -c \
        'import base64,sys; print(base64.urlsafe_b64decode(sys.argv[1]).decode())' \
        "$frontend_identity_b64"
    )" || return 1
    sudo -n python3 -B "$SOURCE_SEAL_HELPER" verify \
      --root "$PREVIOUS_RELEASE_ROOT" \
      --manifest "$source_seal" || return 1
    sudo -n python3 -B "$SOURCE_SEAL_HELPER" verify \
      --profile runtime \
      --root "$PREVIOUS_RELEASE_ROOT" \
      --manifest "$runtime_seal" \
      --commit "$PREVIOUS_RELEASE_SHA" \
      --archive-sha256 "$archive" \
      --frontend-identity "$frontend_identity" \
      --frontend-checksum "$frontend_checksum" || return 1
    printf 'content-addressed:%s:%s:%s:%s\n' \
      "$PREVIOUS_RELEASE_SHA" "$archive" "$source_digest" "$runtime_digest"
    return 0
  fi
  if [[ "$PREVIOUS_RELEASE_ROOT" != "$LEGACY_ROOT" ]]; then
    fail "fixed Active previous release is outside immutable or bootstrap roots"
    return 1
  fi
  receipt_state="$(fixed_active_legacy_bootstrap_receipt_state)" || return 1
  case "$receipt_state" in
    absent|current) ;;
    consumed-other)
      fail "legacy fixed Active bootstrap was already consumed by another immutable release"
      return 1
      ;;
    *)
      fail "legacy fixed Active bootstrap receipt state is invalid"
      return 1
      ;;
  esac
  printf 'legacy-private-fingerprint:%s\n' "$PREVIOUS_RELEASE_SHA"
}

fixed_active_legacy_bootstrap_receipt_payload() {
  printf 'previous=%s target=%s archive=%s\n' \
    "$PREVIOUS_RELEASE_SHA" "$DEPLOY_COMMIT_SHA" "$DEPLOY_ARCHIVE_SHA256"
}

fixed_active_legacy_bootstrap_receipt_state() {
  local actual=""
  local expected=""
  expected="$(fixed_active_legacy_bootstrap_receipt_payload)" || return 1
  if ! sudo -n test -e "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT" \
    && ! sudo -n test -L "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT"; then
    printf 'absent\n'
    return 0
  fi
  if sudo -n test -L "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT" \
    || ! sudo -n test -f "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT" \
    || [[ "$(sudo -n stat -c '%a:%h' "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT")" != "600:1" ]]; then
    fail "legacy fixed Active bootstrap receipt is unsafe"
    return 1
  fi
  actual="$(sudo -n cat "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT")" \
    || return 1
  if [[ "$actual" == "$expected" ]]; then
    printf 'current\n'
  else
    printf 'consumed-other\n'
  fi
}

mark_fixed_active_legacy_bootstrap_complete() {
  local state=""
  local temporary=""
  if [[ "$PREVIOUS_RELEASE_ROOT" != "$LEGACY_ROOT" ]]; then
    return 0
  fi
  state="$(fixed_active_legacy_bootstrap_receipt_state)" || return 1
  if [[ "$state" == "current" ]]; then
    return 0
  fi
  if [[ "$state" != "absent" ]]; then
    fail "legacy fixed Active bootstrap was already consumed"
    return 1
  fi
  temporary="$(mktemp)" || return 1
  fixed_active_legacy_bootstrap_receipt_payload > "$temporary"
  if ! durable_install_file \
    "$temporary" "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT" 0600; then
    rm -f "$temporary"
    return 1
  fi
  rm -f "$temporary"
  [[ "$(fixed_active_legacy_bootstrap_receipt_state)" == "current" ]]
}

clear_fixed_active_legacy_bootstrap_after_restore() {
  local state=""
  if [[ "$PREVIOUS_RELEASE_ROOT" != "$LEGACY_ROOT" ]]; then
    return 0
  fi
  state="$(fixed_active_legacy_bootstrap_receipt_state)" || return 1
  case "$state" in
    absent) return 0 ;;
    current)
      durable_remove_path "$FIXED_ACTIVE_LEGACY_BOOTSTRAP_RECEIPT"
      ;;
    *)
      fail "refusing to remove another release's legacy bootstrap receipt"
      return 1
      ;;
  esac
}

verify_fixed_active_previous_release_source() {
  fixed_active_preimage_command verify >/dev/null
}

fixed_active_preimage_command() {
  local command="$1"
  local preimage_helper="$FIXED_ACTIVE_PREIMAGE_HELPER"
  local previous_proof=""
  if [[ ! -f "$preimage_helper" || -L "$preimage_helper" ]]; then
    fail "fixed Active preimage helper is missing or unsafe"
    return 1
  fi
  if [[ -z "$ACTIVE_UPDATE_TARGET_ENV" ]] \
    || [[ -z "$ACTIVE_UPDATE_TARGET_NGINX" ]] \
    || [[ -z "$PREVIOUS_RELEASE_ROOT" ]] \
    || [[ -z "$PREVIOUS_RELEASE_SHA" ]]; then
    fail "fixed Active preimage inputs are incomplete"
    return 1
  fi
  previous_proof="$(fixed_active_previous_release_proof)" || return 1
  sudo -n python3 -B "$preimage_helper" "$command" \
    --state-root "$BLUEGREEN_STATE_ROOT" \
    --slots-root "$SLOTS_ROOT" \
    --slot-env-root "$SLOT_ENV_ROOT" \
    --slot-link "$SLOTS_ROOT/$CURRENT_ACTIVE_SLOT/current" \
    --slot-env "$SLOT_ENV_ROOT/$CURRENT_ACTIVE_SLOT.env" \
    --active-release-link "$ACTIVE_RELEASE_LINK" \
    --nginx-conf "$NGINX_ACTIVE_RELEASE_CONF" \
    --previous-release-root "$PREVIOUS_RELEASE_ROOT" \
    --legacy-root "$LEGACY_ROOT" \
    --target-release-root "$RELEASE_DIR" \
    --target-env "$ACTIVE_UPDATE_TARGET_ENV" \
    --target-nginx "$ACTIVE_UPDATE_TARGET_NGINX" \
    --commit "$DEPLOY_COMMIT_SHA" \
    --archive-sha256 "$DEPLOY_ARCHIVE_SHA256" \
    --active-slot "$CURRENT_ACTIVE_SLOT" \
    --previous-release-sha "$PREVIOUS_RELEASE_SHA" \
    --previous-release-proof "$previous_proof"
}

load_fixed_active_previous_identity() {
  local manifest=""
  local values=""
  manifest="$(fixed_active_preimage_path)/manifest.json"
  values="$({
    sudo -n python3 -B - \
      "$manifest" \
      "$DEPLOY_COMMIT_SHA" \
      "$DEPLOY_ARCHIVE_SHA256" \
      "$CURRENT_ACTIVE_SLOT" \
      "$RELEASES_ROOT" \
      "$LEGACY_ROOT" <<'PY'
import json
from pathlib import Path
import re
import stat
import sys

manifest_path = Path(sys.argv[1])
metadata = manifest_path.lstat()
if (
    manifest_path.is_symlink()
    or not stat.S_ISREG(metadata.st_mode)
    or metadata.st_uid != 0
    or stat.S_IMODE(metadata.st_mode) & 0o077
    or metadata.st_size <= 0
    or metadata.st_size > 64 * 1024
):
    raise SystemExit("[ERROR] fixed Active preimage manifest is unsafe")
payload = json.loads(manifest_path.read_text(encoding="utf-8"))
identity = payload.get("identity")
expected = {
    "commit": sys.argv[2],
    "archiveSha256": sys.argv[3],
    "activeSlot": sys.argv[4],
}
if not isinstance(identity, dict) or any(
    identity.get(key) != value for key, value in expected.items()
):
    raise SystemExit("[ERROR] fixed Active preimage identity changed")
previous_sha = str(identity.get("previousReleaseSha") or "")
previous_root = str(payload.get("paths", {}).get("previousReleaseRoot") or "")
if re.fullmatch(r"[0-9a-f]{40}", previous_sha) is None:
    raise SystemExit("[ERROR] previous release SHA is malformed")
release_pattern = re.compile(
    re.escape(sys.argv[5]) + r"/[0-9a-f]{40}/[0-9a-f]{64}"
)
if previous_root != sys.argv[6] and release_pattern.fullmatch(previous_root) is None:
    raise SystemExit("[ERROR] previous release root is outside reviewed paths")
print(previous_sha)
print(previous_root)
PY
  })" || return 1
  PREVIOUS_RELEASE_SHA="$(printf '%s\n' "$values" | sed -n '1p')"
  PREVIOUS_RELEASE_ROOT="$(printf '%s\n' "$values" | sed -n '2p')"
  if [[ -z "$PREVIOUS_RELEASE_SHA" || -z "$PREVIOUS_RELEASE_ROOT" ]]; then
    fail "fixed Active previous release identity is unavailable"
    return 1
  fi
}

prepare_fixed_active_targets() {
  ACTIVE_UPDATE_TARGET_ENV="$(mktemp)" || return 1
  ACTIVE_UPDATE_TARGET_NGINX="$(mktemp)" || return 1
  render_fixed_active_env "$ACTIVE_UPDATE_TARGET_ENV" || return 1
  render_active_release \
    "$ACTIVE_UPDATE_TARGET_NGINX" \
    "$CURRENT_ACTIVE_SLOT" \
    "$RELEASE_DIR/06_AppPlatform/frontend/dist" || return 1
  chmod 0644 "$ACTIVE_UPDATE_TARGET_NGINX"
}

remove_fixed_active_target_temporaries() {
  if [[ -n "$ACTIVE_UPDATE_TARGET_ENV" ]]; then
    rm -f "$ACTIVE_UPDATE_TARGET_ENV"
    ACTIVE_UPDATE_TARGET_ENV=""
  fi
  if [[ -n "$ACTIVE_UPDATE_TARGET_NGINX" ]]; then
    rm -f "$ACTIVE_UPDATE_TARGET_NGINX"
    ACTIVE_UPDATE_TARGET_NGINX=""
  fi
}

verify_fixed_active_candidate_retained() {
  verify_slot_release_exact "$CANDIDATE_SLOT" "$DEPLOY_COMMIT_SHA" \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" UnitFileState disabled \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CANDIDATE_SLOT}" ActiveState active \
    && verify_candidate_cgroup \
    && verify_candidate_monthly_gate \
    && verify_candidate_data_access_contract \
    && verify_final_runtime_seal \
    && verify_candidate_preview
}

fixed_active_runtime_is_exact_base() {
  verify_fixed_active_slot_anchor \
    && verify_fixed_active_previous_release_source \
    && verify_active_cgroup "$CURRENT_ACTIVE_SLOT" \
    && verify_durable_route_ownership \
      "$CURRENT_ACTIVE_SLOT" \
      "$RELEASE_DIR" \
      "$DEPLOY_COMMIT_SHA" \
      "$RELEASE_DIR/06_AppPlatform/frontend/dist"
}

fixed_active_runtime_is_exact() {
  [[ ! -e "$DEPLOYMENT_MARKER" && ! -L "$DEPLOYMENT_MARKER" ]] \
    && [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && fixed_active_runtime_is_exact_base \
    && verify_active_monthly_gate_released "$CURRENT_ACTIVE_SLOT"
}

fixed_active_runtime_is_exact_under_hold() {
  verify_quiescence_hold_context \
    && [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && fixed_active_runtime_is_exact_base \
    && verify_active_monthly_gate_held "$CURRENT_ACTIVE_SLOT"
}

fixed_active_runtime_is_verified_under_hold() {
  verify_quiescence_hold_context \
    && [[ -f "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && fixed_active_runtime_is_exact_base \
    && verify_active_monthly_gate_held "$CURRENT_ACTIVE_SLOT"
}

fixed_active_update_is_committed() {
  fixed_active_runtime_is_exact \
    && verify_fixed_active_candidate_retained
}

fixed_active_update_is_committed_under_hold() {
  fixed_active_runtime_is_exact_under_hold \
    && verify_fixed_active_candidate_retained
}

fixed_active_update_is_verified_under_hold() {
  fixed_active_runtime_is_verified_under_hold \
    && verify_fixed_active_candidate_retained
}

previous_fixed_active_runtime_is_exact_base() {
  verify_fixed_active_slot_anchor \
    && verify_fixed_active_previous_release_source \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" UnitFileState enabled \
    && unit_property_equals \
      "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" ActiveState active \
    && verify_active_cgroup "$CURRENT_ACTIVE_SLOT" \
    && verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA" \
    && verify_public_release_exact "$PREVIOUS_RELEASE_SHA" \
    && verify_durable_route_ownership \
      "$CURRENT_ACTIVE_SLOT" \
      "$PREVIOUS_RELEASE_ROOT" \
      "$PREVIOUS_RELEASE_SHA" \
      "$PREVIOUS_RELEASE_ROOT/06_AppPlatform/frontend/dist"
}

previous_fixed_active_runtime_is_committed() {
  [[ ! -e "$DEPLOYMENT_MARKER" && ! -L "$DEPLOYMENT_MARKER" ]] \
    && [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && previous_fixed_active_runtime_is_exact_base \
    && verify_active_monthly_gate_released "$CURRENT_ACTIVE_SLOT"
}

previous_fixed_active_runtime_is_exact_under_hold() {
  verify_quiescence_hold_context \
    && [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && previous_fixed_active_runtime_is_exact_base \
    && verify_active_monthly_gate_held "$CURRENT_ACTIVE_SLOT"
}

previous_fixed_active_restore_is_committed() {
  previous_fixed_active_runtime_is_committed \
    && verify_fixed_active_candidate_retained
}

previous_fixed_active_restore_is_committed_under_hold() {
  previous_fixed_active_runtime_is_exact_under_hold \
    && verify_fixed_active_candidate_retained
}

retain_fixed_active_maintenance_fence() {
  if verify_quiescence_hold_context; then
    return 0
  fi
  mark_maintenance_required
}

install_release_on_fixed_active() {
  local active_env="$SLOT_ENV_ROOT/$CURRENT_ACTIVE_SLOT.env"
  local active_env_stage="$SLOT_ENV_ROOT/.$CURRENT_ACTIVE_SLOT.env.jato-active-updating"
  if ! candidate_durable_install_file \
    "$ACTIVE_UPDATE_TARGET_ENV" \
    "$active_env" 0600 "$active_env_stage"; then
    return 1
  fi
  atomic_symlink "$RELEASE_DIR" "$SLOTS_ROOT/$CURRENT_ACTIVE_SLOT/current" \
    || return 1
  sudo -n systemctl daemon-reload || return 1
  sudo -n systemctl set-property "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" \
    "MemoryHigh=$BLUEGREEN_ACTIVE_MEMORY_HIGH" \
    "MemoryMax=$BLUEGREEN_ACTIVE_MEMORY_MAX" \
    "CPUQuota=200%" || return 1
  sudo -n systemctl restart "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" \
    || return 1
  verify_active_cgroup "$CURRENT_ACTIVE_SLOT" || return 1
  wait_for_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$DEPLOY_COMMIT_SHA" \
    || return 1
  durable_install_file \
    "$ACTIVE_UPDATE_TARGET_NGINX" \
    "$NGINX_ACTIVE_RELEASE_CONF" 0644 || return 1
  sudo -n nginx -t || return 1
  sudo -n systemctl reload nginx || return 1
  verify_public_release_exact "$DEPLOY_COMMIT_SHA" || return 1
  atomic_symlink "$RELEASE_DIR" "$ACTIVE_RELEASE_LINK" || return 1
  verify_durable_route_ownership \
    "$CURRENT_ACTIVE_SLOT" \
    "$RELEASE_DIR" \
    "$DEPLOY_COMMIT_SHA" \
    "$RELEASE_DIR/06_AppPlatform/frontend/dist" || return 1
  verify_fixed_active_slot_anchor \
    && verify_fixed_active_candidate_retained
}

rollback_fixed_active_update() {
  local binding="${1:-}"
  local checkpoint_before=""
  local source_phase="${2:-}"
  local under_hold=false
  if ! read_checkpoint_phase_status; then
    retain_fixed_active_maintenance_fence || true
    return 1
  fi
  checkpoint_before="$CHECKPOINT_PHASE"
  if [[ -z "$source_phase" ]]; then
    source_phase="$checkpoint_before"
  fi
  if [[ -z "$binding" ]]; then
    binding="$(evidence_binding 2>/dev/null || true)"
  fi
  case "$CHECKPOINT_PHASE" in
    rollback_completed)
      return 0
      ;;
    active_update_started|active_update_verified)
      checkpoint_write rollback_started in_progress rollback_required \
        "fixed Active update failed; exact Active preimage restoration started; "\
"${binding:-evidence_unavailable}" \
        || return 1
      ;;
    rollback_started) ;;
    *)
      fail "fixed Active rollback refuses checkpoint phase $CHECKPOINT_PHASE"
      return 1
      ;;
  esac
  if [[ "${JATO_QUIESCENCE_LOCK_HELD:-}" == "1" ]]; then
    verify_quiescence_hold_context || return 1
    under_hold=true
  else
    mark_maintenance_required || return 1
  fi
  if [[ -z "$PREVIOUS_RELEASE_ROOT" || -z "$PREVIOUS_RELEASE_SHA" ]]; then
    load_fixed_active_previous_identity || return 1
  fi
  fixed_active_preimage_command restore || return 1
  clear_fixed_active_legacy_bootstrap_after_restore || return 1
  sudo -n systemctl daemon-reload || return 1
  sudo -n systemctl set-property "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" \
    "MemoryHigh=$BLUEGREEN_ACTIVE_MEMORY_HIGH" \
    "MemoryMax=$BLUEGREEN_ACTIVE_MEMORY_MAX" \
    "CPUQuota=200%" || return 1
  sudo -n systemctl restart "${SERVICE_PREFIX}${CURRENT_ACTIVE_SLOT}" \
    || return 1
  verify_active_cgroup "$CURRENT_ACTIVE_SLOT" || return 1
  wait_for_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA" \
    || return 1
  sudo -n nginx -t || return 1
  sudo -n systemctl reload nginx || return 1
  verify_public_release_exact "$PREVIOUS_RELEASE_SHA" || return 1
  verify_fixed_active_slot_anchor || return 1
  verify_durable_route_ownership \
    "$CURRENT_ACTIVE_SLOT" \
    "$PREVIOUS_RELEASE_ROOT" \
    "$PREVIOUS_RELEASE_SHA" \
    "$PREVIOUS_RELEASE_ROOT/06_AppPlatform/frontend/dist" || return 1
  verify_fixed_active_candidate_retained || return 1
  if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
    resume_schedulers || return 1
  fi
  if [[ "$under_hold" == "true" ]]; then
    previous_fixed_active_restore_is_committed_under_hold || return 1
  else
    clear_maintenance_marker || return 1
    previous_fixed_active_restore_is_committed || return 1
  fi
  checkpoint_write rollback_completed completed automatic \
    "exact previous fixed Active restored; tested Candidate remains isolated; "\
"source_phase=$source_phase; ${binding:-evidence_unavailable}" \
    || return 1
  RELEASE_ROLLED_BACK=true
}

fixed_active_update_exit_handler() {
  local rc="$?"
  trap - EXIT TERM INT HUP
  if [[ "$ACTIVE_UPDATE_HANDLER_ARMED" == "true" ]]; then
    ACTIVE_UPDATE_HANDLER_ARMED=false
    if read_checkpoint_phase_status \
      && [[ "$CHECKPOINT_PHASE" == "active_updated" ]]; then
      if [[ "${JATO_QUIESCENCE_LOCK_HELD:-}" == "1" ]]; then
        fixed_active_update_is_committed_under_hold || {
          retain_fixed_active_maintenance_fence || true
          rc="$EXIT_COMMAND_FAILED_MARKER_RETAINED"
        }
      elif ! fixed_active_update_is_committed; then
        retain_fixed_active_maintenance_fence || true
        rc="$EXIT_COMMAND_FAILED_MARKER_RETAINED"
      fi
    elif ! rollback_fixed_active_update; then
      retain_fixed_active_maintenance_fence || true
      rc="$EXIT_COMMAND_FAILED_MARKER_RETAINED"
    elif [[ "$rc" -eq 0 ]]; then
      rc=1
    fi
  fi
  remove_fixed_active_target_temporaries
  exit "$rc"
}

active_update_locked() {
  local approval_binding=""
  local current_evidence_binding=""
  local release_evidence_binding=""
  require_environment
  approval_binding="$(fixed_active_approval_binding)" || return 1
  verify_quiescence_hold_context
  resolve_active_slot
  capture_fixed_active_slot_anchor
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  case "$CHECKPOINT_PHASE:$CHECKPOINT_STATUS" in
    active_update_started:in_progress|rollback_started:in_progress)
      prepare_fixed_active_targets || return 1
      load_fixed_active_previous_identity || return 1
      ACTIVE_UPDATE_HANDLER_ARMED=true
      if ! rollback_fixed_active_update "" "$CHECKPOINT_PHASE"; then
        return 1
      fi
      ACTIVE_UPDATE_HANDLER_ARMED=false
      fail "interrupted fixed Active update was restored exactly; Candidate still requires explicit discard"
      return 1
      ;;
    active_update_verified:completed)
      prepare_fixed_active_targets || return 1
      load_fixed_active_previous_identity || return 1
      ACTIVE_UPDATE_HANDLER_ARMED=true
      if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
        fixed_active_update_is_verified_under_hold \
          || fail "verified fixed Active state changed before scheduler restoration" \
          || return 1
        resume_schedulers || return 1
      fi
      fixed_active_update_is_committed_under_hold \
        || fail "fixed Active did not remain exact after scheduler restoration" \
        || return 1
      release_evidence_binding="$(evidence_binding)" || return 1
      checkpoint_write active_updated completed inspect_then_resume \
        "fixed Active now runs the exact manually tested artifact without changing "\
"active-slot or public upstream; Candidate remains available for explicit cleanup; "\
"$approval_binding; $release_evidence_binding"
      ACTIVE_UPDATE_HANDLER_ARMED=false
      remove_fixed_active_target_temporaries
      printf \
        '[INFO] Interrupted fixed Active finalization completed: sha=%s active=%s candidate=%s\n' \
        "$DEPLOY_COMMIT_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
      return 0
      ;;
    candidate_ready:completed) ;;
    *)
      fail "locked fixed Active update cannot reconcile $CHECKPOINT_PHASE/$CHECKPOINT_STATUS"
      return 1
      ;;
  esac
  candidate_ready_state_is_legal_under_hold \
    || fail "Candidate failed exact revalidation before fixed Active approval" \
    || return 1
  verify_durable_route_ownership \
    "$CURRENT_ACTIVE_SLOT" \
    "$PREVIOUS_RELEASE_ROOT" \
    "$PREVIOUS_RELEASE_SHA" \
    "$PREVIOUS_RELEASE_ROOT/06_AppPlatform/frontend/dist" \
    || fail "previous successful Active is not an exact rollback source" \
    || return 1
  verify_fixed_active_unit_compatibility
  prepare_fixed_active_targets
  fixed_active_preimage_command capture >/dev/null
  verify_fixed_active_previous_release_source
  release_evidence_binding="$(evidence_binding)" || return 1
  checkpoint_write active_update_started in_progress rollback_required \
    "approved Candidate artifact is being installed on the same fixed Active slot; "\
"active_slot=$CURRENT_ACTIVE_SLOT candidate_slot=$CANDIDATE_SLOT; "\
"$approval_binding; $release_evidence_binding"
  ACTIVE_UPDATE_HANDLER_ARMED=true
  pause_schedulers
  install_release_on_fixed_active
  mark_fixed_active_legacy_bootstrap_complete
  fixed_active_update_is_verified_under_hold
  current_evidence_binding="$(evidence_binding)" || return 1
  if [[ "$current_evidence_binding" != "$release_evidence_binding" ]]; then
    fail "release evidence changed during the fixed Active update"
    return 1
  fi
  checkpoint_write active_update_verified completed inspect_then_resume \
    "fixed Active and public route run the exact tested artifact while singleton "\
"schedulers remain paused under maintenance hold; $approval_binding; "\
"$release_evidence_binding"
  resume_schedulers
  fixed_active_update_is_committed_under_hold
  checkpoint_write active_updated completed inspect_then_resume \
    "fixed Active now runs the exact manually tested artifact without changing "\
"active-slot or public upstream; Candidate remains available for explicit cleanup; "\
"$approval_binding; $release_evidence_binding"
  ACTIVE_UPDATE_HANDLER_ARMED=false
  remove_fixed_active_target_temporaries
  printf \
    '[INFO] Fixed Active updated under quiescence: sha=%s active=%s candidate=%s\n' \
    "$DEPLOY_COMMIT_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
}

approve_candidate_to_active() {
  local supervisor_rc=0
  require_environment
  fixed_active_approval_binding >/dev/null || return 1
  assert_inherited_production_lock
  require_existing_active_slot_anchor
  assert_no_active_switch_unit
  resolve_active_slot
  resolve_previous_release_identity \
    || fail "previous release metadata is unavailable before fixed Active approval" \
    || return 1
  if [[ "$PREVIOUS_RELEASE_ROOT" == "$LEGACY_ROOT" ]]; then
    legacy_active_bridge_check_identity || return 1
    fail \
      "legacy_active_bootstrap_required: Candidate remains test-only until the "\
"legacy Active systemd and Nginx preimage migration is reviewed"
    return 1
  fi
  ensure_bluegreen_state_root
  ensure_bluegreen_runtime_roots
  capture_fixed_active_slot_anchor
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  if [[ "$CHECKPOINT_PHASE" == "active_updated" ]] \
    && [[ "$CHECKPOINT_STATUS" == "completed" ]]; then
    prepare_fixed_active_targets || return 1
    load_fixed_active_previous_identity || return 1
    fixed_active_update_is_committed \
      || fail "fixed Active committed state failed exact revalidation" \
      || return 1
    remove_fixed_active_target_temporaries
    return 0
  fi
  case "$CHECKPOINT_PHASE:$CHECKPOINT_STATUS" in
    candidate_ready:completed)
      candidate_ready_state_is_legal \
        || fail "Candidate failed exact revalidation before quiescence" \
        || return 1
      ;;
    active_update_started:in_progress|\
    active_update_verified:completed|\
    rollback_started:in_progress)
      echo "[WARN] Reconciling interrupted fixed Active phase: $CHECKPOINT_PHASE"
      ;;
    *)
      fail "checkpoint phase $CHECKPOINT_PHASE cannot approve or reconcile a fixed Active update"
      return 1
      ;;
  esac
  set +e
  run_quiescence_supervisor active-update-locked
  supervisor_rc=$?
  set -e
  resolve_active_slot
  capture_fixed_active_slot_anchor
  if read_checkpoint_phase_status \
    && [[ "$CHECKPOINT_PHASE" == "active_updated" ]] \
    && [[ "$CHECKPOINT_STATUS" == "completed" ]]; then
    prepare_fixed_active_targets || return 1
    load_fixed_active_previous_identity || return 1
    if fixed_active_update_is_committed; then
      remove_fixed_active_target_temporaries
      printf \
        '[INFO] Fixed Active update settled after quiescence: sha=%s active=%s candidate=%s\n' \
        "$DEPLOY_COMMIT_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
      return 0
    fi
  fi
  if [[ "$CHECKPOINT_PHASE" == "rollback_completed" ]] \
    && [[ "$CHECKPOINT_STATUS" == "completed" ]]; then
    fail "fixed Active update failed and exact previous Active was restored; Candidate remains for explicit discard"
  fi
  if [[ "$supervisor_rc" -eq 0 ]]; then
    supervisor_rc=1
  fi
  return "$supervisor_rc"
}

restore_previous_active_exit_handler() {
  local rc="$?"
  trap - EXIT TERM INT HUP
  if [[ "$PREVIOUS_ACTIVE_RESTORE_ARMED" == "true" ]]; then
    if [[ "${JATO_QUIESCENCE_LOCK_HELD:-}" == "1" ]] \
      && read_checkpoint_phase_status \
      && [[ "$CHECKPOINT_PHASE" == "rollback_completed" ]] \
      && [[ "$CHECKPOINT_STATUS" == "completed" ]] \
      && previous_fixed_active_restore_is_committed_under_hold; then
      PREVIOUS_ACTIVE_RESTORE_ARMED=false
    elif read_checkpoint_phase_status \
      && [[ "$CHECKPOINT_PHASE" == "rollback_completed" ]] \
      && [[ "$CHECKPOINT_STATUS" == "completed" ]] \
      && previous_fixed_active_restore_is_committed; then
      PREVIOUS_ACTIVE_RESTORE_ARMED=false
    else
      retain_fixed_active_maintenance_fence || true
      rc="$EXIT_COMMAND_FAILED_MARKER_RETAINED"
    fi
  fi
  remove_fixed_active_target_temporaries
  exit "$rc"
}

restore_previous_active_locked() {
  local approval_binding=""
  local release_evidence_binding=""
  require_environment
  approval_binding="$(fixed_active_approval_binding)" || return 1
  verify_quiescence_hold_context
  resolve_active_slot
  capture_fixed_active_slot_anchor
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  if [[ "$CHECKPOINT_PHASE" != "active_updated" ]] \
    || [[ "$CHECKPOINT_STATUS" != "completed" ]]; then
    fail "previous Active restore requires active_updated/completed"
    return 1
  fi
  prepare_fixed_active_targets || return 1
  load_fixed_active_previous_identity || return 1
  fixed_active_update_is_committed_under_hold \
    || fail "updated Active, exact preimage, or retained Candidate changed" \
    || return 1
  release_evidence_binding="$(evidence_binding)" || return 1
  PREVIOUS_ACTIVE_RESTORE_ARMED=true
  pause_schedulers
  checkpoint_write rollback_started in_progress rollback_required \
    "post-update public audit rejected the fixed Active artifact; "\
"exact previous Active restoration started; $approval_binding; "\
"$release_evidence_binding"
  rollback_fixed_active_update "$release_evidence_binding" active_updated
  PREVIOUS_ACTIVE_RESTORE_ARMED=false
  remove_fixed_active_target_temporaries
  printf \
    '[INFO] Previous fixed Active restored under quiescence: sha=%s active=%s; Candidate retained on %s\n' \
    "$PREVIOUS_RELEASE_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
}

restore_previous_active() {
  local supervisor_rc=0
  require_environment
  fixed_active_approval_binding >/dev/null || return 1
  assert_inherited_production_lock
  ensure_bluegreen_state_root
  ensure_bluegreen_runtime_roots
  assert_no_active_switch_unit
  resolve_active_slot
  capture_fixed_active_slot_anchor
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  if [[ "$CHECKPOINT_PHASE" != "active_updated" ]] \
    || [[ "$CHECKPOINT_STATUS" != "completed" ]]; then
    fail "previous Active restore requires active_updated/completed"
    return 1
  fi
  prepare_fixed_active_targets || return 1
  load_fixed_active_previous_identity || return 1
  fixed_active_update_is_committed \
    || fail "updated Active, exact preimage, or retained Candidate changed" \
    || return 1
  remove_fixed_active_target_temporaries
  set +e
  run_quiescence_supervisor restore-previous-active-locked
  supervisor_rc=$?
  set -e
  resolve_active_slot
  capture_fixed_active_slot_anchor
  if read_checkpoint_phase_status \
    && [[ "$CHECKPOINT_PHASE" == "rollback_completed" ]] \
    && [[ "$CHECKPOINT_STATUS" == "completed" ]]; then
    prepare_fixed_active_targets || return 1
    load_fixed_active_previous_identity || return 1
    if previous_fixed_active_restore_is_committed; then
      remove_fixed_active_target_temporaries
      printf \
        '[INFO] Previous fixed Active restore settled: sha=%s active=%s candidate=%s\n' \
        "$PREVIOUS_RELEASE_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
      return 0
    fi
  fi
  if [[ "$supervisor_rc" -eq 0 ]]; then
    supervisor_rc=1
  fi
  return "$supervisor_rc"
}

candidate_preview_is_released() {
  local active_state=""
  local load_state=""
  local unit=""
  unit="$(candidate_preview_unit_name)" || return 1
  load_state="$(
    systemctl show "$unit" -p LoadState --value 2>/dev/null || true
  )"
  active_state="$(
    systemctl show "$unit" -p ActiveState --value 2>/dev/null || true
  )"
  if [[ "$load_state" != "not-found" ]] \
    || [[ "$active_state" == "active" || "$active_state" == "activating" ]] \
    || [[ -e "$CANDIDATE_PREVIEW_STATE_DIR" ]] \
    || [[ -L "$CANDIDATE_PREVIEW_STATE_DIR" ]]; then
    fail "Candidate preview is not fully released"
    return 1
  fi
  candidate_preview_port_is_unused
}

candidate_release_is_complete() {
  candidate_cleanup_is_complete \
    && candidate_preview_is_released
}

previous_active_is_exact_for_candidate_discard() {
  resolve_previous_release_identity \
    && [[ ! -e "$DEPLOYMENT_MARKER" && ! -L "$DEPLOYMENT_MARKER" ]] \
    && [[ ! -e "$SCHEDULER_STATE_FILE" && ! -L "$SCHEDULER_STATE_FILE" ]] \
    && verify_previous_active_runtime_exact
}

fixed_active_preimage_exists() {
  local manifest=""
  manifest="$(fixed_active_preimage_path)/manifest.json"
  sudo -n test -f "$manifest" \
    && ! sudo -n test -L "$manifest"
}

verify_discarded_active_is_exact() {
  if fixed_active_preimage_exists; then
    prepare_fixed_active_targets || return 1
    load_fixed_active_previous_identity || return 1
    previous_fixed_active_runtime_is_committed
  else
    previous_active_is_exact_for_candidate_discard
  fi
}

discard_candidate_exit_handler() {
  local rc="$?"
  trap - EXIT TERM INT HUP
  remove_fixed_active_target_temporaries
  exit "$rc"
}

discard_failed_candidate() {
  local binding=""
  local expected_active_slot=""
  local expected_candidate_slot=""
  require_environment
  assert_inherited_production_lock
  require_existing_active_slot_anchor
  assert_no_active_switch_unit
  if [[ -e "$DEPLOYMENT_MARKER" || -L "$DEPLOYMENT_MARKER" ]] \
    || [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]] \
    || [[ -e "$NGINX_PREIMAGE_DIR" || -L "$NGINX_PREIMAGE_DIR" ]]; then
    fail "failed Candidate discard refuses Active, scheduler, or Nginx reconciliation state"
    return 1
  fi
  resolve_active_slot
  expected_active_slot="$CURRENT_ACTIVE_SLOT"
  expected_candidate_slot="$CANDIDATE_SLOT"
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  case "$CHECKPOINT_PHASE:$CHECKPOINT_STATUS:$CHECKPOINT_RETRY_CLASS" in
    migrated:completed:automatic)
      if resolve_existing_candidate_slot 2>/dev/null; then
        if [[ "$CURRENT_ACTIVE_SLOT" != "$expected_active_slot" \
          || "$CANDIDATE_SLOT" != "$expected_candidate_slot" ]]; then
          fail "failed Candidate slot identity differs from the durable Active anchor"
          return 1
        fi
      else
        CURRENT_ACTIVE_SLOT="$expected_active_slot"
        CANDIDATE_SLOT="$expected_candidate_slot"
        if ! candidate_cleanup_is_complete; then
          fail "failed Candidate is neither exact live runtime nor exact restored preimage"
          return 1
        fi
      fi
      binding="$(evidence_binding)" || return 1
      previous_active_is_exact_for_candidate_discard \
        || fail "previous successful Active is not exact before failed Candidate discard" \
        || return 1
      if ! candidate_preview_is_released; then
        stop_candidate_preview || return 1
      fi
      if ! candidate_cleanup_is_complete; then
        restore_candidate_runtime_preimage || return 1
      fi
      candidate_release_is_complete \
        || fail "failed Candidate cleanup did not reach exact quiescent state" \
        || return 1
      previous_active_is_exact_for_candidate_discard \
        || fail "previous successful Active changed during failed Candidate discard" \
        || return 1
      settle_candidate_checkpoint_after_cleanup || return 1
      ;;
    candidate_prepare_aborted:completed:automatic)
      previous_active_is_exact_for_candidate_discard \
        || fail "aborted Candidate checkpoint no longer has the exact previous Active" \
        || return 1
      candidate_release_is_complete \
        || fail "aborted Candidate checkpoint no longer has exact quiescent runtime" \
        || return 1
      ;;
    *)
      fail \
        "checkpoint $CHECKPOINT_PHASE/$CHECKPOINT_STATUS/$CHECKPOINT_RETRY_CLASS cannot discard a failed Candidate"
      return 1
      ;;
  esac
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  if [[ "$CHECKPOINT_PHASE:$CHECKPOINT_STATUS:$CHECKPOINT_RETRY_CLASS" \
    != "candidate_prepare_aborted:completed:automatic" ]]; then
    fail "failed Candidate discard did not reach candidate_prepare_aborted/completed/automatic"
    return 1
  fi
  printf \
    '[INFO] Failed Candidate discarded: sha=%s active=%s candidate=%s; public Active was unchanged; %s\n' \
    "$DEPLOY_COMMIT_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT" \
    "${binding:-already-settled}"
}

discard_candidate() {
  local binding=""
  require_environment
  assert_inherited_production_lock
  require_existing_active_slot_anchor
  assert_no_active_switch_unit
  resolve_active_slot
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  case "$CHECKPOINT_PHASE" in
    candidate_ready)
      if [[ "$CHECKPOINT_STATUS" != "completed" ]]; then
        fail "Candidate discard requires candidate_ready/completed"
        return 1
      fi
      binding="$(evidence_binding)" || return 1
      previous_active_is_exact_for_candidate_discard \
        || fail "previous successful Active is not exact before Candidate discard" \
        || return 1
      stop_candidate_preview || return 1
      if ! candidate_cleanup_is_complete; then
        restore_candidate_runtime_preimage || return 1
      fi
      candidate_release_is_complete \
        || fail "Candidate discard did not reach exact quiescent state" \
        || return 1
      previous_active_is_exact_for_candidate_discard \
        || fail "previous successful Active changed during Candidate discard" \
        || return 1
      checkpoint_write candidate_discarded completed automatic \
        "manually rejected Candidate preview and runtime were exactly removed; "\
"active_slot=$CURRENT_ACTIVE_SLOT candidate_slot=$CANDIDATE_SLOT; $binding"
      ;;
    rollback_completed)
      if [[ "$CHECKPOINT_STATUS" != "completed" ]]; then
        fail "Candidate discard requires rollback_completed/completed"
        return 1
      fi
      binding="$(evidence_binding)" || return 1
      prepare_fixed_active_targets || return 1
      load_fixed_active_previous_identity || return 1
      previous_fixed_active_restore_is_committed \
        || fail "restored previous Active or retained Candidate changed" \
        || return 1
      stop_candidate_preview || return 1
      if ! candidate_cleanup_is_complete; then
        restore_candidate_runtime_preimage || return 1
      fi
      candidate_release_is_complete \
        || fail "restored Candidate cleanup did not reach exact quiescence" \
        || return 1
      previous_fixed_active_runtime_is_committed \
        || fail "previous Active changed while discarding retained Candidate" \
        || return 1
      checkpoint_write candidate_discarded completed automatic \
        "Candidate retained after exact fixed Active restoration was explicitly removed; "\
"active_slot=$CURRENT_ACTIVE_SLOT candidate_slot=$CANDIDATE_SLOT; $binding"
      ;;
    candidate_discarded)
      if [[ "$CHECKPOINT_STATUS" != "completed" ]]; then
        fail "Candidate discarded checkpoint is not completed"
        return 1
      fi
      verify_discarded_active_is_exact \
        || fail "discarded Candidate checkpoint no longer has the exact previous Active" \
        || return 1
      candidate_release_is_complete \
        || fail "discarded Candidate checkpoint no longer has an exact quiescent Candidate" \
        || return 1
      ;;
    *)
      fail "checkpoint phase $CHECKPOINT_PHASE cannot discard Candidate"
      return 1
      ;;
  esac
  remove_fixed_active_target_temporaries
  printf \
    '[INFO] Candidate discarded: sha=%s active=%s candidate=%s; public Active was unchanged\n' \
    "$DEPLOY_COMMIT_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
}

release_candidate_exit_handler() {
  local rc="$?"
  trap - EXIT TERM INT HUP
  remove_fixed_active_target_temporaries
  exit "$rc"
}

release_candidate() {
  local release_binding=""
  require_environment
  release_binding="$(fixed_active_approval_binding)" || return 1
  assert_inherited_production_lock
  ensure_bluegreen_state_root
  ensure_bluegreen_runtime_roots
  assert_no_active_switch_unit
  resolve_active_slot
  capture_fixed_active_slot_anchor
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  case "$CHECKPOINT_PHASE" in
    active_updated)
      prepare_fixed_active_targets || return 1
      load_fixed_active_previous_identity || return 1
      fixed_active_runtime_is_exact \
        || fail "fixed Active changed before Candidate release" \
        || return 1
      stop_candidate_preview || return 1
      restore_candidate_runtime_preimage || return 1
      candidate_release_is_complete \
        || fail "Candidate release did not reach exact quiescent state" \
        || return 1
      fixed_active_runtime_is_exact \
        || fail "fixed Active changed during Candidate release" \
        || return 1
      checkpoint_write candidate_released completed automatic \
        "Candidate preview and runtime were exactly released without changing fixed Active; "\
"active_slot=$CURRENT_ACTIVE_SLOT candidate_slot=$CANDIDATE_SLOT $release_binding"
      ;;
    candidate_released)
      prepare_fixed_active_targets || return 1
      load_fixed_active_previous_identity || return 1
      fixed_active_runtime_is_exact \
        || fail "released Candidate checkpoint no longer has an exact fixed Active" \
        || return 1
      candidate_release_is_complete \
        || fail "released Candidate checkpoint no longer has an exact quiescent Candidate slot" \
        || return 1
      ;;
    *)
      fail "checkpoint phase $CHECKPOINT_PHASE cannot release Candidate"
      return 1
      ;;
  esac
  remove_fixed_active_target_temporaries
  printf \
    '[INFO] Candidate released: sha=%s active=%s candidate=%s; fixed Active and rollback artifacts were retained\n' \
    "$DEPLOY_COMMIT_SHA" "$CURRENT_ACTIVE_SLOT" "$CANDIDATE_SLOT"
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
  if ! restore_candidate_runtime_preimage \
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

candidate_cleanup_is_complete() {
  if ! sudo -n test -d "$CANDIDATE_PREIMAGE_DIR" \
    || sudo -n test -L "$CANDIDATE_PREIMAGE_DIR"; then
    return 1
  fi
  candidate_runtime_is_quiescent \
    && candidate_runtime_preimage_command verify-live
}

mark_pre_switch_cleanup_required() {
  if [[ "$BLUEGREEN_MODE" == "prepare-candidate" \
    || "$BLUEGREEN_MODE" == "discard-failed-candidate" ]]; then
    echo \
      "[ERROR] Candidate cleanup needs inspection; no production maintenance marker was written" \
      >&2
    return 0
  fi
  mark_maintenance_required
}

settle_candidate_checkpoint_after_cleanup() {
  if [[ ! -e "$CHECKPOINT_FILE" && ! -L "$CHECKPOINT_FILE" ]]; then
    return 0
  fi
  read_checkpoint_phase_status || return 1
  case "$CHECKPOINT_PHASE:$CHECKPOINT_STATUS" in
    migrated:completed)
      checkpoint_write candidate_prepare_aborted completed automatic \
        "Candidate preparation failed, and its runtime and preview were exactly removed before readiness"
      ;;
    candidate_ready:completed)
      checkpoint_write candidate_discarded completed automatic \
        "invalid or interrupted Candidate was exactly removed before any public route switch"
      ;;
    candidate_prepare_aborted:completed|candidate_discarded:completed) ;;
    *)
      fail "Candidate cleanup cannot settle checkpoint $CHECKPOINT_PHASE/$CHECKPOINT_STATUS"
      return 1
      ;;
  esac
  read_checkpoint_phase_status \
    && [[ "$CHECKPOINT_STATUS" == "completed" ]] \
    && { [[ "$CHECKPOINT_PHASE" == "candidate_prepare_aborted" ]] \
      || [[ "$CHECKPOINT_PHASE" == "candidate_discarded" ]]; }
}

cleanup_pre_switch_candidate() {
  if ! stop_candidate_preview; then
    fail "Candidate cleanup failed while stopping the preview"
    mark_pre_switch_cleanup_required || true
    return 1
  fi
  if ! resolve_previous_release_identity \
    || ! verify_slot_release_exact "$CURRENT_ACTIVE_SLOT" "$PREVIOUS_RELEASE_SHA"; then
    fail "Candidate cleanup could not prove the previous Active slot"
    mark_pre_switch_cleanup_required || true
    return 1
  fi
  if [[ "$BLUEGREEN_MODE" == "switch-locked" ]]; then
    if ! verify_public_release_exact "$PREVIOUS_RELEASE_SHA"; then
      mark_pre_switch_cleanup_required || true
      return 1
    fi
    if ! restore_old_static_boot_owner; then
      mark_pre_switch_cleanup_required || true
      return 1
    fi
    if ! candidate_cleanup_is_complete \
      && ! restore_candidate_runtime_preimage; then
      mark_pre_switch_cleanup_required || true
      return 1
    fi
    if ! candidate_cleanup_is_complete; then
      mark_pre_switch_cleanup_required || true
      return 1
    fi
    if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]] \
      && ! resume_schedulers; then
      mark_pre_switch_cleanup_required || true
      return 1
    fi
    mark_maintenance_required || true
    echo "[WARN] Persistent supervisor stopped the candidate and restored schedulers without mutating the durable stable Nginx route" >&2
    return 1
  fi
  if ! restore_nginx_preimage \
    || ! verify_public_release_exact "$PREVIOUS_RELEASE_SHA"; then
    fail "Candidate cleanup could not restore and verify the previous public route"
    mark_pre_switch_cleanup_required || true
    return 1
  fi
  if ! restore_old_static_boot_owner; then
    fail "Candidate cleanup could not restore the previous boot owner"
    mark_pre_switch_cleanup_required || true
    return 1
  fi
  if ! candidate_cleanup_is_complete \
    && ! restore_candidate_runtime_preimage; then
    fail "Candidate cleanup could not restore the captured runtime preimage"
    mark_pre_switch_cleanup_required || true
    return 1
  fi
  if ! candidate_cleanup_is_complete; then
    fail "Candidate cleanup runtime differs from the captured preimage"
    mark_pre_switch_cleanup_required || true
    return 1
  fi
  if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
    resume_schedulers || {
      mark_pre_switch_cleanup_required || true
      return 1
    }
  fi
  remove_nginx_preimage || {
    fail "Candidate cleanup could not remove the settled Nginx preimage"
    mark_pre_switch_cleanup_required || true
    return 1
  }
  if ! settle_candidate_checkpoint_after_cleanup; then
    fail "Candidate cleanup could not settle the release checkpoint"
    mark_pre_switch_cleanup_required || true
    return 1
  fi
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
    mark_pre_switch_cleanup_required || true
    return 1
  fi
  if read_checkpoint_phase_status \
    && candidate_ready_state_is_legal; then
    SWITCH_RECONCILED=true
    echo "[INFO] Preserved exact candidate_ready state during reconciliation" >&2
    return 0
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
      mark_pre_switch_cleanup_required || true
      return 1
    fi
    if [[ "$BLUEGREEN_MODE" != "switch-locked" ]] \
      && [[ "$BLUEGREEN_MODE" != "prepare-candidate" ]]; then
      clear_maintenance_marker || return 1
    fi
    SWITCH_RECONCILED=true
    return 0
  fi
  mark_pre_switch_cleanup_required || true
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
  verify_active_cgroup "$CANDIDATE_SLOT" || return 1
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
  discard_candidate_runtime_preimage \
    || return "$EXIT_COMMAND_FAILED_MARKER_RETAINED"
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
      && verify_active_cgroup "$CANDIDATE_SLOT" \
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
      && discard_candidate_runtime_preimage \
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
      && restore_candidate_runtime_preimage \
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

run_quiescence_supervisor() {
  local active_bundle_lock=""
  local active_main_pid=""
  local active_project_root=""
  local bash_bin=""
  local controller="$RELEASE_DIR/03_Scripts/deploy/tencent_bluegreen_release.sh"
  local helper="$RELEASE_DIR/03_Scripts/deploy/jato_quiescence_gate.py"
  local locked_mode="${1:-}"
  local minimum_timeout=0
  local python_bin=""
  local quiescence_evidence=""
  local unit=""
  case "$locked_mode" in
    switch-locked|active-update-locked|restore-previous-active-locked) ;;
    *)
      fail "unsupported quiescence child mode: ${locked_mode:-missing}"
      return 1
      ;;
  esac
  quiescence_evidence="$BLUEGREEN_STATE_ROOT/quiescence-${DEPLOY_COMMIT_SHA}-${locked_mode}.json"
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
    --setenv="BLUEGREEN_MODE=$locked_mode" \
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
    --setenv="CANDIDATE_PREIMAGE_ROOT=$CANDIDATE_PREIMAGE_ROOT" \
    --setenv="BLUEGREEN_DRAIN_SECONDS=$BLUEGREEN_DRAIN_SECONDS" \
    --setenv="BLUEGREEN_CONTROLLER_TIMEOUT=$BLUEGREEN_CONTROLLER_TIMEOUT" \
    --setenv="BLUEGREEN_FAULT=$BLUEGREEN_FAULT" \
    --setenv="DEPLOY_COMMIT_SHA=$DEPLOY_COMMIT_SHA" \
    --setenv="DEPLOY_ARCHIVE_SHA256=$DEPLOY_ARCHIVE_SHA256" \
    --setenv="DEPLOY_ARCHIVE_BYTES=$DEPLOY_ARCHIVE_BYTES" \
    --setenv="DEPLOY_REPOSITORY=$DEPLOY_REPOSITORY" \
    --setenv="DEPLOY_RUN_ID=$DEPLOY_RUN_ID" \
    --setenv="DEPLOY_RUN_ATTEMPT=$DEPLOY_RUN_ATTEMPT" \
    --setenv="DEPLOY_APPROVAL_RUN_ID=${DEPLOY_APPROVAL_RUN_ID:-}" \
    --setenv="DEPLOY_APPROVAL_RUN_ATTEMPT=${DEPLOY_APPROVAL_RUN_ATTEMPT:-}" \
    --setenv="DEPLOY_CANDIDATE_ATTESTATION_SHA256=${DEPLOY_CANDIDATE_ATTESTATION_SHA256:-}" \
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
      -- "$bash_bin" "$controller" "$locked_mode"
}

run_switch_supervisor() {
  run_quiescence_supervisor switch-locked
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

prepare_candidate() {
  local binding=""
  require_environment
  assert_inherited_production_lock
  require_existing_active_slot_anchor
  assert_no_active_switch_unit
  if [[ -e "$SCHEDULER_STATE_FILE" || -L "$SCHEDULER_STATE_FILE" ]]; then
    fail "Candidate preparation refuses an existing scheduler state snapshot"
    return 1
  fi
  if [[ -e "$DEPLOYMENT_MARKER" || -L "$DEPLOYMENT_MARKER" ]]; then
    fail "Candidate preparation refuses an existing deployment maintenance marker"
    return 1
  fi
  resolve_active_slot
  resolve_current_frontend_root
  resolve_previous_release_identity \
    || fail "previous Active identity is unavailable before Candidate preparation" \
    || return 1
  verify_previous_active_runtime_exact \
    || fail "previous Active failed the complete pre-mutation Candidate preflight" \
    || return 1
  ensure_bluegreen_state_root
  ensure_bluegreen_runtime_roots
  if ! read_checkpoint_phase_status; then
    return 1
  fi
  case "$CHECKPOINT_PHASE" in
    candidate_ready)
      if ! resolve_existing_candidate_slot; then
        fail "candidate_ready checkpoint no longer has one exact Candidate slot"
        return 1
      fi
      PRE_SUPERVISOR_CANDIDATE_ARMED=true
      if candidate_ready_state_is_legal; then
        PRE_SUPERVISOR_CANDIDATE_ARMED=false
        printf \
          '[INFO] Candidate remains ready: sha=%s slot=%s preview=http://127.0.0.1:%s\n' \
          "$DEPLOY_COMMIT_SHA" \
          "$CANDIDATE_SLOT" \
          "$BLUEGREEN_CANDIDATE_PREVIEW_PORT"
        return 0
      fi
      fail "candidate_ready checkpoint failed exact live-state revalidation"
      return 1
      ;;
    candidate_prepare_aborted|candidate_discarded|switch_started|switched|rollback_started|rollback_completed)
      fail "checkpoint phase $CHECKPOINT_PHASE cannot prepare a new Candidate"
      return 1
      ;;
    backend_healthy|www_verified|intl_deploy_started|intl_verified|parity_verified)
      fail "checkpoint phase $CHECKPOINT_PHASE cannot prepare a new Candidate"
      return 1
      ;;
    complete|pre_switch_aborted)
      fail "checkpoint phase $CHECKPOINT_PHASE cannot prepare a new Candidate"
      return 1
      ;;
  esac
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
  verify_candidate
  verify_candidate_data_access_contract
  start_candidate_preview
  verify_candidate_preview
  if ! read_checkpoint_phase_status \
    || [[ "$CHECKPOINT_PHASE" != "migrated" ]] \
    || [[ "$CHECKPOINT_STATUS" != "completed" ]]; then
    fail "Candidate runtime verification did not finish from migrated/completed"
    return 1
  fi
  binding="$(evidence_binding)" || return 1
  checkpoint_write candidate_ready completed inspect_then_resume \
    "exact Candidate and loopback preview are ready for manual inspection; "\
"slot=$CANDIDATE_SLOT port=$BLUEGREEN_CANDIDATE_PREVIEW_PORT; $binding"
  read_checkpoint_phase_status
  if [[ "$BLUEGREEN_FAULT" == "candidate_ready" ]]; then
    fail "fault injection: candidate_ready"
  fi
  candidate_ready_state_is_legal
  PRE_SUPERVISOR_CANDIDATE_ARMED=false
  printf \
    '[INFO] Candidate ready: sha=%s slot=%s preview=http://127.0.0.1:%s\n' \
    "$DEPLOY_COMMIT_SHA" \
    "$CANDIDATE_SLOT" \
    "$BLUEGREEN_CANDIDATE_PREVIEW_PORT"
}

prepare_and_switch() {
  local binding=""
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
    candidate_ready)
      if ! resolve_existing_candidate_slot; then
        fail "candidate_ready checkpoint no longer has one exact Candidate slot"
        return 1
      fi
      PRE_SUPERVISOR_CANDIDATE_ARMED=true
      if candidate_ready_state_is_legal; then
        PRE_SUPERVISOR_CANDIDATE_ARMED=false
        fail "candidate_ready requires a separately approved switch mode"
        return 1
      fi
      fail "candidate_ready checkpoint failed exact live-state revalidation"
      return 1
      ;;
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
  verify_candidate_data_access_contract
  binding="$(evidence_binding)" || return 1
  checkpoint_write candidate_ready completed inspect_then_resume \
    "exact Candidate backend passed automatic verification before the production switch; $binding"
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
  restore-previous-active)
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    restore_previous_active
    trap - HUP INT TERM
    ;;
  restore-previous-active-locked)
    trap restore_previous_active_exit_handler EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    restore_previous_active_locked
    trap - EXIT HUP INT TERM
    ;;
  discard-candidate)
    trap discard_candidate_exit_handler EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    discard_candidate
    trap - EXIT HUP INT TERM
    ;;
  discard-failed-candidate)
    trap discard_candidate_exit_handler EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    discard_failed_candidate
    trap - EXIT HUP INT TERM
    ;;
  release-candidate)
    trap release_candidate_exit_handler EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    release_candidate
    trap - EXIT HUP INT TERM
    ;;
  approve-candidate-to-active)
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    approve_candidate_to_active
    trap - HUP INT TERM
    ;;
  active-update-locked)
    trap fixed_active_update_exit_handler EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    active_update_locked
    trap - EXIT HUP INT TERM
    ;;
  prepare-candidate)
    trap prepare_exit_handler EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    prepare_candidate
    trap - EXIT HUP INT TERM
    ;;
  prepare-and-switch|switch-locked)
    fail "legacy physical slot switching is retired; use prepare-candidate and approve-candidate-to-active"
    ;;
  build-candidate-runtime)
    build_candidate_runtime_locked
    ;;
  *)
    fail "unknown Tencent blue/green mode: $BLUEGREEN_MODE"
    ;;
esac
