#!/usr/bin/env bash
set -Eeuo pipefail

# Non-routing feature canary for Tencent CVM.
#
# This controller consumes an already-uploaded immutable feature archive.  It
# never invokes the production release controller, installs a unit, edits an
# active-slot marker, or reloads Nginx.  Build and runtime are separate
# transient systemd units on a loopback-only non-production port.

CANARY_MODE="${1:-launch}"
CANARY_ROOT="${CANARY_ROOT:-/opt/jato-canary}"
CANARY_STATE_ROOT="${CANARY_STATE_ROOT:-/var/lib/jato-canary}"
CANARY_PORT="${CANARY_PORT:-18001}"
CANARY_MEMORY_HIGH="${CANARY_MEMORY_HIGH:-3G}"
CANARY_MEMORY_MAX="${CANARY_MEMORY_MAX:-4G}"
CANARY_TASKS_MAX="${CANARY_TASKS_MAX:-512}"
CANARY_BUILD_TIMEOUT="${CANARY_BUILD_TIMEOUT:-1800}"
CANARY_RUNTIME_TIMEOUT="${CANARY_RUNTIME_TIMEOUT:-300}"
CANARY_PUBLIC_ORIGIN="${CANARY_PUBLIC_ORIGIN:-https://www.ojeur.cloud}"
CANARY_FAULT="${CANARY_FAULT:-}"
LEGACY_ROOT="${LEGACY_ROOT:-/opt/JATO_Analysis_System-main}"
CANARY_INITIAL_LOCK_PATH="${CANARY_INITIAL_LOCK_PATH-${JATO_PRODUCTION_DEPLOY_LOCK_PATH-}}"
CANARY_MAX_SOURCE_BYTES=$((256 * 1024 * 1024))
CANARY_PORT_RELEASE_TIMEOUT_SECONDS=75
CANARY_CONTROLLER_RECOVERY_TIMEOUT_SECONDS=900
CANARY_SUPERVISOR_STOP_TIMEOUT_SECONDS=1200
CANARY_SUPERVISOR_RESTART_SECONDS=5
CANARY_SUPERVISOR_MEMORY_HIGH=256M
CANARY_SUPERVISOR_MEMORY_MAX=512M
CANARY_SUPERVISOR_TASKS_MAX=64
CANARY_CONTROLLER_MEMORY_HIGH=256M
CANARY_CONTROLLER_MEMORY_MAX=512M
CANARY_CONTROLLER_TASKS_MAX=64
CANARY_SUPERVISOR_INVOCATION_ID="${CANARY_SUPERVISOR_INVOCATION_ID:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANARY_GUARD="$SCRIPT_DIR/jato_feature_canary_guard.py"
MUTATION_LOCK_HELPER="$SCRIPT_DIR/lib/production_mutation_lock.sh"
READINESS_VERIFIER="$SCRIPT_DIR/verify_backend_readiness.py"

CANARY_COMMIT_SHA="${CANARY_COMMIT_SHA:-}"
CANARY_BRANCH="${CANARY_BRANCH:-}"
CANARY_REPOSITORY="${CANARY_REPOSITORY:-}"
CANARY_SOURCE_ARCHIVE="${CANARY_SOURCE_ARCHIVE:-}"
CANARY_SOURCE_SHA256="${CANARY_SOURCE_SHA256:-}"
CANARY_SOURCE_BYTES="${CANARY_SOURCE_BYTES:-}"
CANARY_RUN_ID="${CANARY_RUN_ID:-}"

RUN_KEY=""
RUNTIME_ROOT=""
CHECKPOINT_FILE=""
RECEIPT_FILE=""
EVIDENCE_FILE=""
BEFORE_SNAPSHOT=""
AFTER_SNAPSHOT=""
ACTIVE_UNIT=""
SUPERVISOR_UNIT=""
CONTROLLER_UNIT=""
BUILD_UNIT=""
SERVICE_UNIT=""
SERVICE_RUNTIME_DIRECTORY=""
CONTROL_ROOT=""
CONTROL_SCRIPT=""
STAGED_SOURCE_ARCHIVE=""

CANARY_RUNTIME_CREATED=false
CANARY_FINALIZING=false
CANARY_EXPECTED_FAILURE_OBSERVED=false
CANARY_SUPERVISOR_STOP_REQUESTED=false
CANARY_ERROR=""

fail() {
  CANARY_ERROR="$*"
  echo "[ERROR] $*" >&2
  return 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 \
    || fail "required command is unavailable: $1"
}

record_checkpoint() {
  local phase="$1"
  local status="$2"
  local message="$3"
  local identity=(
    --repository "$CANARY_REPOSITORY"
    --branch "$CANARY_BRANCH"
    --commit "$CANARY_COMMIT_SHA"
    --archive-sha256 "$CANARY_SOURCE_SHA256"
    --archive-bytes "$CANARY_SOURCE_BYTES"
    --run-id "$CANARY_RUN_ID"
    --port "$CANARY_PORT"
  )
  python3 -B "$CANARY_GUARD" record \
    --path "$CHECKPOINT_FILE" \
    "${identity[@]}" \
    --phase "$phase" \
    --status "$status" \
    --message "$message"
}

verify_checkpoint_marker() {
  local phase="$1"
  local status="${2:-completed}"
  python3 -B "$CANARY_GUARD" verify-marker \
    --checkpoint "$CHECKPOINT_FILE" \
    --repository "$CANARY_REPOSITORY" \
    --branch "$CANARY_BRANCH" \
    --commit "$CANARY_COMMIT_SHA" \
    --archive-sha256 "$CANARY_SOURCE_SHA256" \
    --archive-bytes "$CANARY_SOURCE_BYTES" \
    --run-id "$CANARY_RUN_ID" \
    --port "$CANARY_PORT" \
    --phase "$phase" \
    --status "$status"
}

ensure_checkpoint_marker() {
  local phase="$1"
  local status="${2:-completed}"
  local message="$3"
  python3 -B "$CANARY_GUARD" ensure-marker \
    --checkpoint "$CHECKPOINT_FILE" \
    --repository "$CANARY_REPOSITORY" \
    --branch "$CANARY_BRANCH" \
    --commit "$CANARY_COMMIT_SHA" \
    --archive-sha256 "$CANARY_SOURCE_SHA256" \
    --archive-bytes "$CANARY_SOURCE_BYTES" \
    --run-id "$CANARY_RUN_ID" \
    --port "$CANARY_PORT" \
    --phase "$phase" \
    --status "$status" \
    --message "$message"
}

verify_existing_receipt() {
  python3 -B "$CANARY_GUARD" verify-receipt \
    --path "$RECEIPT_FILE" \
    --repository "$CANARY_REPOSITORY" \
    --branch "$CANARY_BRANCH" \
    --commit "$CANARY_COMMIT_SHA" \
    --archive-sha256 "$CANARY_SOURCE_SHA256" \
    --archive-bytes "$CANARY_SOURCE_BYTES" \
    --run-id "$CANARY_RUN_ID" \
    --port "$CANARY_PORT"
}

validate_feature_identity() {
  python3 -B - \
    "$CANARY_BRANCH" "$CANARY_COMMIT_SHA" "$CANARY_SOURCE_SHA256" \
    "$CANARY_SOURCE_BYTES" "$CANARY_RUN_ID" "$CANARY_REPOSITORY" <<'PY'
import re
import sys

branch, commit, archive, archive_bytes, run_id, repository = sys.argv[1:]
if (
    not branch
    or branch == "main"
    or branch.startswith("/")
    or branch.endswith("/")
    or ".." in branch.split("/")
    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,254}", branch)
):
    raise SystemExit("[ERROR] canary requires a real, safe, non-main feature branch")
if not re.fullmatch(r"[0-9a-f]{40}", commit):
    raise SystemExit("[ERROR] canary commit must be a full lowercase git SHA")
if not re.fullmatch(r"[0-9a-f]{64}", archive):
    raise SystemExit("[ERROR] canary archive SHA-256 is invalid")
if not re.fullmatch(r"[1-9][0-9]*", archive_bytes):
    raise SystemExit("[ERROR] canary archive bytes must be a positive integer")
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}", run_id):
    raise SystemExit("[ERROR] canary run id is malformed")
if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
    raise SystemExit("[ERROR] canary repository identity is malformed")
PY
}

validate_static_contract() {
  local actual_bytes=""
  local actual_sha=""
  local account_home=""
  local canonical_deploy_state=""
  for command_name in \
    awk curl flock getent python3 readlink realpath sha256sum stat systemctl \
    systemd-run tar timeout; do
    require_command "$command_name"
  done
  if [[ "$CANARY_ROOT" != "/opt/jato-canary" ]] \
    || [[ "$CANARY_STATE_ROOT" != "/var/lib/jato-canary" ]] \
    || [[ "$CANARY_PORT" != "18001" ]] \
    || [[ "$CANARY_MEMORY_HIGH" != "3G" ]] \
    || [[ "$CANARY_MEMORY_MAX" != "4G" ]] \
    || [[ "$CANARY_TASKS_MAX" != "512" ]]; then
    fail "canary root, port, and 3G/4G/512 resource contract are immutable"
    return 1
  fi
  if [[ "$CANARY_MODE" == "launch" \
    || "$CANARY_MODE" == "supervisor" \
    || "$CANARY_MODE" == "controller" ]]; then
    account_home="$(
      getent passwd "$(id -u)" | awk -F: 'NR == 1 {print $6}'
    )"
    if [[ -z "$account_home" || "$account_home" != /* ]] \
      || [[ "${HOME:-}" != "$account_home" ]]; then
      fail "HOME must match the deploy account for the canonical production lock"
      return 1
    fi
    canonical_deploy_state="${account_home%/}/.local/state/jato-production-release"
    if [[ "${DEPLOY_STATE_DIR:-}" != "$canonical_deploy_state" ]]; then
      fail "canary requires the canonical production deploy state directory"
      return 1
    fi
    if [[ -n "${JATO_PRODUCTION_DEPLOY_LOCK_PATH:-}" ]] \
      && [[ "$JATO_PRODUCTION_DEPLOY_LOCK_PATH" != \
        "$canonical_deploy_state/production-deploy.lock" ]]; then
      fail "canary production lock override is not canonical"
      return 1
    fi
    if [[ -n "$CANARY_INITIAL_LOCK_PATH" ]] \
      && [[ "$CANARY_INITIAL_LOCK_PATH" != \
        "$canonical_deploy_state/production-deploy.lock" ]]; then
      fail "canary initial production lock identity is not canonical"
      return 1
    fi
  fi
  case "$CANARY_FAULT" in
    ""|after_candidate_start) ;;
    *)
      fail "unsupported canary fault injection: $CANARY_FAULT"
      return 1
      ;;
  esac
  validate_feature_identity
  if [[ "$CANARY_MODE" != "launch" ]]; then
    verify_canary_parent_roots
  fi
  for control_file in \
    "$SCRIPT_DIR/tencent_feature_candidate_canary.sh" \
    "$CANARY_GUARD" \
    "$MUTATION_LOCK_HELPER" \
    "$READINESS_VERIFIER"; do
    if [[ ! -f "$control_file" || -L "$control_file" ]]; then
      fail "canary control-plane file is missing or unsafe: $control_file"
      return 1
    fi
  done
  if [[ "$CANARY_SOURCE_ARCHIVE" != /* ]] \
    || [[ ! -f "$CANARY_SOURCE_ARCHIVE" ]] \
    || [[ -L "$CANARY_SOURCE_ARCHIVE" ]]; then
    fail "canary source archive must be an absolute regular non-symlink file"
    return 1
  fi
  actual_bytes="$(stat -c '%s' "$CANARY_SOURCE_ARCHIVE")"
  actual_sha="$(sha256sum "$CANARY_SOURCE_ARCHIVE" | awk '{print $1}')"
  if [[ "$actual_bytes" != "$CANARY_SOURCE_BYTES" ]] \
    || [[ "$actual_sha" != "$CANARY_SOURCE_SHA256" ]]; then
    fail "canary source archive size or SHA-256 differs from its immutable identity"
    return 1
  fi
  if (( actual_bytes > CANARY_MAX_SOURCE_BYTES )); then
    fail "canary source archive exceeds the 256 MiB safety limit"
    return 1
  fi
  if [[ ! "$CANARY_BUILD_TIMEOUT" =~ ^[1-9][0-9]*$ ]] \
    || [[ ! "$CANARY_RUNTIME_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    fail "canary build/runtime timeout must be a positive integer"
    return 1
  fi
  timeout 120 python3 -B - "$CANARY_SOURCE_ARCHIVE" "$SCRIPT_DIR" <<'PY'
from pathlib import Path, PurePosixPath
import hashlib
import sys
import tarfile

archive_path = Path(sys.argv[1])
script_dir = Path(sys.argv[2])
relative_paths = (
    "03_Scripts/deploy/tencent_feature_candidate_canary.sh",
    "03_Scripts/deploy/jato_feature_canary_guard.py",
    "03_Scripts/deploy/lib/production_mutation_lock.sh",
    "03_Scripts/deploy/verify_backend_readiness.py",
)
max_members = 50_000
max_expanded_bytes = 2 * 1024 * 1024 * 1024
max_member_bytes = 512 * 1024 * 1024
with tarfile.open(archive_path, mode="r:gz") as archive:
    indexed = {}
    member_count = 0
    expanded_bytes = 0
    for member in archive:
        member_count += 1
        if member_count > max_members:
            raise SystemExit("[ERROR] feature archive contains too many members")
        if member.size < 0 or member.size > max_member_bytes:
            raise SystemExit("[ERROR] feature archive member size is unsafe")
        expanded_bytes += member.size
        if expanded_bytes > max_expanded_bytes:
            raise SystemExit("[ERROR] feature archive expands beyond 2 GiB")
        normalized = PurePosixPath(*(
            part for part in PurePosixPath(member.name).parts if part != "."
        )).as_posix()
        if normalized in relative_paths:
            if normalized in indexed or not member.isfile():
                raise SystemExit(
                    f"[ERROR] archive control-plane member is duplicate/unsafe: "
                    f"{normalized}"
                )
            source = archive.extractfile(member)
            if source is None:
                raise SystemExit(
                    f"[ERROR] archive control-plane member is unreadable: {normalized}"
                )
            with source:
                indexed[normalized] = hashlib.sha256(source.read()).hexdigest()
for relative in relative_paths:
    local = script_dir.parent.parent / relative
    if relative not in indexed or not local.is_file() or local.is_symlink():
        raise SystemExit(
            f"[ERROR] control-plane provenance is incomplete: {relative}"
        )
    if hashlib.sha256(local.read_bytes()).hexdigest() != indexed[relative]:
        raise SystemExit(
            f"[ERROR] control-plane file differs from immutable archive: {relative}"
        )
PY
}

pin_canary_production_lock_path() {
  local canonical_lock_path="${DEPLOY_STATE_DIR%/}/production-deploy.lock"
  CANARY_INITIAL_LOCK_PATH="$canonical_lock_path"
  JATO_PRODUCTION_DEPLOY_LOCK_PATH="$canonical_lock_path"
  export CANARY_INITIAL_LOCK_PATH JATO_PRODUCTION_DEPLOY_LOCK_PATH
}

initialize_paths() {
  RUN_KEY="${CANARY_COMMIT_SHA:0:12}-${CANARY_RUN_ID}"
  if [[ ! "$RUN_KEY" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
    fail "canary run key is malformed"
    return 1
  fi
  RUNTIME_ROOT="$CANARY_ROOT/runtime/$RUN_KEY"
  CHECKPOINT_FILE="$CANARY_STATE_ROOT/checkpoints/$RUN_KEY.json"
  RECEIPT_FILE="$CANARY_STATE_ROOT/receipts/$RUN_KEY.json"
  EVIDENCE_FILE="$CANARY_STATE_ROOT/evidence/$RUN_KEY.json"
  BEFORE_SNAPSHOT="$CANARY_STATE_ROOT/snapshots/$RUN_KEY.before.json"
  AFTER_SNAPSHOT="$CANARY_STATE_ROOT/snapshots/$RUN_KEY.after.json"
  SUPERVISOR_UNIT="jato-feature-canary-supervisor-$RUN_KEY.service"
  CONTROLLER_UNIT="jato-feature-canary-controller-$RUN_KEY.service"
  BUILD_UNIT="jato-feature-canary-build-$RUN_KEY.service"
  SERVICE_UNIT="jato-feature-canary-$RUN_KEY.service"
  SERVICE_RUNTIME_DIRECTORY="jato-feature-canary-$RUN_KEY"
  CONTROL_ROOT="$CANARY_ROOT/control/$RUN_KEY"
  CONTROL_SCRIPT="$CONTROL_ROOT/03_Scripts/deploy/tencent_feature_candidate_canary.sh"
  STAGED_SOURCE_ARCHIVE="$CANARY_ROOT/sources/$RUN_KEY.tar.gz"
  export \
    RUN_KEY RUNTIME_ROOT CHECKPOINT_FILE RECEIPT_FILE EVIDENCE_FILE \
    BEFORE_SNAPSHOT AFTER_SNAPSHOT SUPERVISOR_UNIT CONTROLLER_UNIT \
    BUILD_UNIT SERVICE_UNIT \
    SERVICE_RUNTIME_DIRECTORY CONTROL_ROOT CONTROL_SCRIPT \
    STAGED_SOURCE_ARCHIVE
}

verify_canary_parent_roots() {
  python3 -B - \
    "$CANARY_ROOT" \
    "$CANARY_ROOT/runtime" \
    "$CANARY_ROOT/control" \
    "$CANARY_ROOT/sources" <<'PY'
from pathlib import Path
import os
import stat
import sys

for raw in sys.argv[1:]:
    path = Path(raw)
    metadata = os.lstat(path)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755
    ):
        raise SystemExit(
            f"[ERROR] canary immutable parent is mutable/unsafe: {path}"
        )
PY
}

ensure_canary_roots() {
  local path=""
  for path in \
    "$CANARY_ROOT" \
    "$CANARY_ROOT/runtime" \
    "$CANARY_ROOT/control" \
    "$CANARY_ROOT/sources"; do
    if sudo -n test -L "$path" \
      || {
        sudo -n test -e "$path" \
          && ! sudo -n test -d "$path";
      }; then
      fail "canary-owned path is unsafe: $path"
      return 1
    fi
    sudo -n install -d -m 0755 -o root -g root "$path"
  done
  verify_canary_parent_roots
  for path in \
    "$CANARY_STATE_ROOT" \
    "$CANARY_STATE_ROOT/checkpoints" \
    "$CANARY_STATE_ROOT/receipts" \
    "$CANARY_STATE_ROOT/evidence" \
    "$CANARY_STATE_ROOT/snapshots"; do
    if sudo -n test -L "$path" \
      || {
        sudo -n test -e "$path" \
          && ! sudo -n test -d "$path";
      }; then
      fail "canary-owned path is unsafe: $path"
      return 1
    fi
    sudo -n install -d -m 0750 -o "$(id -u)" -g "$(id -g)" "$path"
  done
  for path in "$RUNTIME_ROOT" "$CONTROL_ROOT" "$STAGED_SOURCE_ARCHIVE"; do
    if sudo -n test -e "$path" || sudo -n test -L "$path"; then
      fail "canary ephemeral path already exists and requires inspection: $path"
      return 1
    fi
  done
  for path in \
    "$CHECKPOINT_FILE" \
    "$RECEIPT_FILE" \
    "$EVIDENCE_FILE" \
    "$BEFORE_SNAPSHOT" \
    "$AFTER_SNAPSHOT"; do
    if sudo -n test -e "$path" || sudo -n test -L "$path"; then
      fail "canary run id already has durable state and cannot be reused: $path"
      return 1
    fi
  done
}

stage_canary_inputs() {
  local staged_bytes=""
  local staged_sha=""
  sudo -n install -d -m 0755 -o "$(id -u)" -g "$(id -g)" "$RUNTIME_ROOT"
  sudo -n install -d -m 0555 -o root -g root \
    "$CONTROL_ROOT/03_Scripts/deploy/lib"
  sudo -n install -m 0555 -o root -g root \
    "$SCRIPT_DIR/tencent_feature_candidate_canary.sh" \
    "$CONTROL_SCRIPT"
  sudo -n install -m 0444 -o root -g root \
    "$CANARY_GUARD" \
    "$CONTROL_ROOT/03_Scripts/deploy/jato_feature_canary_guard.py"
  sudo -n install -m 0444 -o root -g root \
    "$MUTATION_LOCK_HELPER" \
    "$CONTROL_ROOT/03_Scripts/deploy/lib/production_mutation_lock.sh"
  sudo -n install -m 0444 -o root -g root \
    "$READINESS_VERIFIER" \
    "$CONTROL_ROOT/03_Scripts/deploy/verify_backend_readiness.py"
  sudo -n install -m 0444 -o root -g root \
    "$CANARY_SOURCE_ARCHIVE" "$STAGED_SOURCE_ARCHIVE"
  staged_bytes="$(stat -c '%s' "$STAGED_SOURCE_ARCHIVE")"
  staged_sha="$(sha256sum "$STAGED_SOURCE_ARCHIVE" | awk '{print $1}')"
  if [[ "$staged_bytes" != "$CANARY_SOURCE_BYTES" ]] \
    || [[ "$staged_sha" != "$CANARY_SOURCE_SHA256" ]]; then
    fail "staged canary archive differs from its immutable source identity"
    return 1
  fi
  CANARY_SOURCE_ARCHIVE="$STAGED_SOURCE_ARCHIVE"
  export CANARY_SOURCE_ARCHIVE
}

build_canary_control_environment() {
  local mode="$1"
  CANARY_CONTROL_ENVIRONMENT=(
    "PATH=$PATH"
    "HOME=$HOME"
    "DEPLOY_STATE_DIR=$DEPLOY_STATE_DIR"
    "JATO_PRODUCTION_DEPLOY_LOCK_PATH=$CANARY_INITIAL_LOCK_PATH"
    "CANARY_INITIAL_LOCK_PATH=$CANARY_INITIAL_LOCK_PATH"
    "CANARY_MODE=$mode"
    "CANARY_ROOT=$CANARY_ROOT"
    "CANARY_STATE_ROOT=$CANARY_STATE_ROOT"
    "CANARY_PORT=$CANARY_PORT"
    "CANARY_MEMORY_HIGH=$CANARY_MEMORY_HIGH"
    "CANARY_MEMORY_MAX=$CANARY_MEMORY_MAX"
    "CANARY_TASKS_MAX=$CANARY_TASKS_MAX"
    "CANARY_BUILD_TIMEOUT=$CANARY_BUILD_TIMEOUT"
    "CANARY_RUNTIME_TIMEOUT=$CANARY_RUNTIME_TIMEOUT"
    "CANARY_PUBLIC_ORIGIN=$CANARY_PUBLIC_ORIGIN"
    "CANARY_FAULT=$CANARY_FAULT"
    "LEGACY_ROOT=$LEGACY_ROOT"
    "CANARY_COMMIT_SHA=$CANARY_COMMIT_SHA"
    "CANARY_BRANCH=$CANARY_BRANCH"
    "CANARY_REPOSITORY=$CANARY_REPOSITORY"
    "CANARY_SOURCE_ARCHIVE=$STAGED_SOURCE_ARCHIVE"
    "CANARY_SOURCE_SHA256=$CANARY_SOURCE_SHA256"
    "CANARY_SOURCE_BYTES=$CANARY_SOURCE_BYTES"
    "CANARY_RUN_ID=$CANARY_RUN_ID"
    "RUN_KEY=$RUN_KEY"
    "SUPERVISOR_UNIT=$SUPERVISOR_UNIT"
    "CONTROLLER_UNIT=$CONTROLLER_UNIT"
    "RUNTIME_ROOT=$RUNTIME_ROOT"
    "CHECKPOINT_FILE=$CHECKPOINT_FILE"
    "RECEIPT_FILE=$RECEIPT_FILE"
    "EVIDENCE_FILE=$EVIDENCE_FILE"
    "BEFORE_SNAPSHOT=$BEFORE_SNAPSHOT"
    "AFTER_SNAPSHOT=$AFTER_SNAPSHOT"
    "BUILD_UNIT=$BUILD_UNIT"
    "SERVICE_UNIT=$SERVICE_UNIT"
    "SERVICE_RUNTIME_DIRECTORY=$SERVICE_RUNTIME_DIRECTORY"
    "CONTROL_ROOT=$CONTROL_ROOT"
    "CONTROL_SCRIPT=$CONTROL_SCRIPT"
    "STAGED_SOURCE_ARCHIVE=$STAGED_SOURCE_ARCHIVE"
  )
  if [[ "$mode" != "supervisor" ]]; then
    CANARY_CONTROL_ENVIRONMENT+=(
      "CANARY_SUPERVISOR_INVOCATION_ID=$CANARY_SUPERVISOR_INVOCATION_ID"
    )
  fi
}

assert_supervisor_scope() {
  local bash_bin=""
  local expected_active_states=""
  local properties=""
  bash_bin="$(command -v bash)"
  case "$CANARY_MODE" in
    supervisor|controller)
      expected_active_states=active
      ;;
    reconcile)
      expected_active_states=active,deactivating
      ;;
    *)
      fail "supervisor scope may only be asserted by supervisor, controller, or reconcile"
      return 1
      ;;
  esac
  build_canary_control_environment supervisor
  properties="$(
    systemctl show "$SUPERVISOR_UNIT" \
      -p LoadState -p ActiveState -p UnitFileState -p FragmentPath \
      -p ExecStart -p Environment -p ControlGroup -p MainPID -p Restart \
      -p MemoryHigh -p MemoryMax -p MemorySwapMax -p TasksMax -p KillMode
  )"
  python3 -B - \
    "$properties" "$SUPERVISOR_UNIT" "$bash_bin" "$CONTROL_SCRIPT" \
    "$CANARY_MODE" "$expected_active_states" "$$" \
    "${CANARY_CONTROL_ENVIRONMENT[@]}" <<'PY'
from pathlib import Path
import shlex
import sys

(
    raw,
    unit,
    bash_bin,
    control_script,
    mode,
    expected_active_states_raw,
    pid,
) = sys.argv[1:8]
expected_active_states = set(expected_active_states_raw.split(","))
expected_environment_tokens = sys.argv[8:]
properties = {}
for line in raw.splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value
expected_fragment = f"/run/systemd/transient/{unit}"
expected_group = f"/system.slice/{unit}"
required = {
    "LoadState": "loaded",
    "UnitFileState": "transient",
    "FragmentPath": expected_fragment,
    "ControlGroup": expected_group,
    "MemoryHigh": str(256 * 1024 * 1024),
    "MemoryMax": str(512 * 1024 * 1024),
    "MemorySwapMax": "0",
    "TasksMax": "64",
    "KillMode": "control-group",
    "Restart": "on-failure",
}
if properties.get("ActiveState") not in expected_active_states:
    raise SystemExit(
        "[ERROR] canary supervisor property ActiveState is not exact"
    )
for key, expected in required.items():
    if properties.get(key) != expected:
        raise SystemExit(
            f"[ERROR] canary supervisor property {key} is not exact"
        )


def command_argv(property_name: str) -> list[str]:
    raw_value = properties.get(property_name, "")
    if raw_value.count("argv[]=") != 1:
        raise SystemExit(
            f"[ERROR] canary supervisor {property_name} argv is ambiguous"
        )
    argv_raw = raw_value.split("argv[]=", 1)[1].split(" ; ", 1)[0]
    try:
        return shlex.split(argv_raw)
    except ValueError as exc:
        raise SystemExit(
            f"[ERROR] canary supervisor {property_name} argv is malformed"
        ) from exc


if command_argv("ExecStart") != [bash_bin, control_script, "supervisor"]:
    raise SystemExit("[ERROR] canary supervisor ExecStart argv is not exact")

try:
    environment_tokens = shlex.split(properties.get("Environment", ""))
except ValueError as exc:
    raise SystemExit("[ERROR] canary supervisor Environment is malformed") from exc
environment = {}
for token in environment_tokens:
    key, separator, value = token.partition("=")
    if not separator or not key or key in environment:
        raise SystemExit(
            "[ERROR] canary supervisor Environment has malformed or duplicate keys"
        )
    environment[key] = value
expected_environment = {}
for token in expected_environment_tokens:
    key, separator, expected = token.partition("=")
    if not separator or not key or key in expected_environment:
        raise SystemExit(
            "[ERROR] canary supervisor expected Environment is malformed"
        )
    expected_environment[key] = expected
if environment != expected_environment:
    changed = sorted(
        key
        for key in set(environment) | set(expected_environment)
        if environment.get(key) != expected_environment.get(key)
    )
    if changed:
        raise SystemExit(
            "[ERROR] canary supervisor Environment is not exact: "
            + ", ".join(changed)
        )
main_pid = properties.get("MainPID", "")
if not main_pid.isdigit() or main_pid == "0":
    raise SystemExit("[ERROR] canary supervisor MainPID is not live")
members = (
    Path("/sys/fs/cgroup") / expected_group.lstrip("/") / "cgroup.procs"
).read_text(encoding="utf-8").splitlines()
if main_pid not in members:
    raise SystemExit("[ERROR] canary supervisor MainPID escaped its exact cgroup")
if mode != "controller" and pid not in members:
    raise SystemExit("[ERROR] canary supervisor process escaped its exact cgroup")
PY
}

read_live_supervisor_invocation_id() {
  local allowed_active_states="${1:-active}"
  local properties=""
  properties="$(
    systemctl show "$SUPERVISOR_UNIT" \
      -p LoadState -p ActiveState -p MainPID -p InvocationID
  )"
  python3 -B - "$properties" "$allowed_active_states" <<'PY'
import re
import sys

properties = {}
for line in sys.argv[1].splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value
invocation_id = properties.get("InvocationID", "")
main_pid = properties.get("MainPID", "")
allowed_active_states = set(sys.argv[2].split(","))
if (
    properties.get("LoadState") != "loaded"
    or properties.get("ActiveState") not in allowed_active_states
    or not main_pid.isdigit()
    or main_pid == "0"
    or re.fullmatch(r"[0-9A-Fa-f]{32}", invocation_id) is None
    or invocation_id == "0" * 32
):
    raise SystemExit("[ERROR] durable supervisor generation is not live and exact")
print(invocation_id.lower())
PY
}

capture_supervisor_invocation_id() {
  if [[ "$CANARY_MODE" != "supervisor" ]]; then
    fail "only the durable supervisor may capture its invocation generation"
    return 1
  fi
  CANARY_SUPERVISOR_INVOCATION_ID="$(
    read_live_supervisor_invocation_id
  )"
  export CANARY_SUPERVISOR_INVOCATION_ID
}

assert_supervisor_generation() {
  local current_invocation_id=""
  if [[ ! "$CANARY_SUPERVISOR_INVOCATION_ID" =~ ^[0-9a-f]{32}$ ]] \
    || [[ "$CANARY_SUPERVISOR_INVOCATION_ID" == \
      "00000000000000000000000000000000" ]]; then
    fail "child unit lacks its original supervisor generation fence"
    return 1
  fi
  current_invocation_id="$(read_live_supervisor_invocation_id)"
  if [[ "$current_invocation_id" != "$CANARY_SUPERVISOR_INVOCATION_ID" ]]; then
    fail "child unit belongs to a stale supervisor generation"
    return 1
  fi
}

assert_reconcile_supervisor_generation() {
  local current_invocation_id=""
  if [[ "$CANARY_MODE" != "reconcile" ]]; then
    fail "deactivating supervisor generations are only valid during reconciliation"
    return 1
  fi
  if [[ ! "$CANARY_SUPERVISOR_INVOCATION_ID" =~ ^[0-9a-f]{32}$ ]] \
    || [[ "$CANARY_SUPERVISOR_INVOCATION_ID" == \
      "00000000000000000000000000000000" ]]; then
    fail "reconcile lacks its original supervisor generation fence"
    return 1
  fi
  current_invocation_id="$(
    read_live_supervisor_invocation_id active,deactivating
  )"
  if [[ "$current_invocation_id" != "$CANARY_SUPERVISOR_INVOCATION_ID" ]]; then
    fail "reconcile belongs to a stale supervisor generation"
    return 1
  fi
}

assert_controller_scope() {
  local bash_bin=""
  local controller_timeout=0
  local properties=""
  bash_bin="$(command -v bash)"
  controller_timeout=$((CANARY_BUILD_TIMEOUT + CANARY_RUNTIME_TIMEOUT + 300))
  build_canary_control_environment controller
  properties="$(
    systemctl show "$CONTROLLER_UNIT" \
      -p LoadState -p ActiveState -p UnitFileState -p FragmentPath \
      -p ExecStart -p Environment -p ControlGroup -p MainPID -p Restart \
      -p MemoryHigh -p MemoryMax -p MemorySwapMax -p TasksMax -p KillMode \
      -p SendSIGKILL \
      -p BindsTo -p PartOf -p After -p StopPropagatedFrom \
      -p RuntimeMaxUSec -p TimeoutStopUSec
  )"
  python3 -B - \
    "$properties" "$CONTROLLER_UNIT" "$SUPERVISOR_UNIT" \
    "$bash_bin" "$CONTROL_SCRIPT" "$$" \
    "$controller_timeout" "$CANARY_CONTROLLER_RECOVERY_TIMEOUT_SECONDS" \
    "${CANARY_CONTROL_ENVIRONMENT[@]}" <<'PY'
from decimal import Decimal
from pathlib import Path
import re
import shlex
import sys

(
    raw,
    unit,
    supervisor_unit,
    bash_bin,
    control_script,
    pid,
    runtime_seconds,
    stop_seconds,
) = sys.argv[1:9]
expected_environment_tokens = sys.argv[9:]
properties = {}
for line in raw.splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value
expected_group = f"/system.slice/{unit}"
required = {
    "LoadState": "loaded",
    "ActiveState": "active",
    "UnitFileState": "transient",
    "FragmentPath": f"/run/systemd/transient/{unit}",
    "ControlGroup": expected_group,
    "MainPID": pid,
    "Restart": "no",
    "MemoryHigh": str(256 * 1024 * 1024),
    "MemoryMax": str(512 * 1024 * 1024),
    "MemorySwapMax": "0",
    "TasksMax": "64",
    "KillMode": "control-group",
    "SendSIGKILL": "yes",
}
for key, expected in required.items():
    if properties.get(key) != expected:
        raise SystemExit(
            f"[ERROR] canary controller property {key} is not exact"
        )


def parse_systemd_usec(value: str) -> int:
    if value.isdigit():
        return int(value)
    unit_usec = {
        "us": Decimal(1),
        "µs": Decimal(1),
        "ms": Decimal(1_000),
        "s": Decimal(1_000_000),
        "min": Decimal(60_000_000),
        "h": Decimal(3_600_000_000),
        "d": Decimal(86_400_000_000),
        "w": Decimal(604_800_000_000),
    }
    total = Decimal(0)
    position = 0
    for match in re.finditer(
        r"\s*([0-9]+(?:\.[0-9]+)?)\s*(us|µs|ms|s|min|h|d|w)",
        value,
    ):
        if match.start() != position:
            raise ValueError(value)
        total += Decimal(match.group(1)) * unit_usec[match.group(2)]
        position = match.end()
    if position != len(value) or position == 0 or total != total.to_integral_value():
        raise ValueError(value)
    return int(total)


for property_name, seconds in (
    ("RuntimeMaxUSec", runtime_seconds),
    ("TimeoutStopUSec", stop_seconds),
):
    try:
        actual_usec = parse_systemd_usec(properties.get(property_name, ""))
    except ValueError as exc:
        raise SystemExit(
            f"[ERROR] canary controller property {property_name} is malformed"
        ) from exc
    if actual_usec != int(seconds) * 1_000_000:
        raise SystemExit(
            f"[ERROR] canary controller property {property_name} is not exact"
        )
if supervisor_unit not in properties.get("After", "").split():
    raise SystemExit(
        "[ERROR] canary controller omitted exact supervisor ordering"
    )
if set(properties.get("StopPropagatedFrom", "").split()) != {supervisor_unit}:
    raise SystemExit(
        "[ERROR] canary controller omitted exact stop-only supervisor propagation"
    )
if properties.get("BindsTo", "").split() or properties.get("PartOf", "").split():
    raise SystemExit(
        "[ERROR] canary controller gained a restart-propagating supervisor dependency"
    )


def command_argv() -> list[str]:
    raw_value = properties.get("ExecStart", "")
    if raw_value.count("argv[]=") != 1:
        raise SystemExit("[ERROR] canary controller ExecStart argv is ambiguous")
    argv_raw = raw_value.split("argv[]=", 1)[1].split(" ; ", 1)[0]
    try:
        return shlex.split(argv_raw)
    except ValueError as exc:
        raise SystemExit(
            "[ERROR] canary controller ExecStart argv is malformed"
        ) from exc


if command_argv() != [bash_bin, control_script, "controller"]:
    raise SystemExit("[ERROR] canary controller ExecStart argv is not exact")
try:
    environment_tokens = shlex.split(properties.get("Environment", ""))
except ValueError as exc:
    raise SystemExit("[ERROR] canary controller Environment is malformed") from exc
environment = {}
for token in environment_tokens:
    key, separator, value = token.partition("=")
    if not separator or not key or key in environment:
        raise SystemExit(
            "[ERROR] canary controller Environment has malformed or duplicate keys"
        )
    environment[key] = value
expected_environment = {}
for token in expected_environment_tokens:
    key, separator, expected = token.partition("=")
    if not separator or not key or key in expected_environment:
        raise SystemExit(
            "[ERROR] canary controller expected Environment is malformed"
        )
    expected_environment[key] = expected
if environment != expected_environment:
    changed = sorted(
        key
        for key in set(environment) | set(expected_environment)
        if environment.get(key) != expected_environment.get(key)
    )
    raise SystemExit(
        "[ERROR] canary controller Environment is not exact: "
        + ", ".join(changed)
    )
members = (
    Path("/sys/fs/cgroup") / expected_group.lstrip("/") / "cgroup.procs"
).read_text(encoding="utf-8").splitlines()
if pid not in members:
    raise SystemExit("[ERROR] canary controller escaped its exact cgroup")
PY
}

assert_supervisor_production_lock() {
  local expected_lock_path="${DEPLOY_STATE_DIR%/}/production-deploy.lock"
  local flock_rc=0
  local holder_pid=""
  local holder_target=""
  local expected_target=""
  if [[ "$CANARY_INITIAL_LOCK_PATH" != "$expected_lock_path" ]] \
    || [[ "$JATO_PRODUCTION_DEPLOY_LOCK_PATH" != "$expected_lock_path" ]] \
    || [[ ! -f "$expected_lock_path" ]] \
    || [[ -L "$expected_lock_path" ]]; then
    fail "controller production lock identity is not canonical and safe"
    return 1
  fi
  holder_pid="$(
    systemctl show "$SUPERVISOR_UNIT" -p MainPID --value
  )"
  if [[ ! "$holder_pid" =~ ^[1-9][0-9]*$ ]] \
    || [[ ! -e "/proc/$holder_pid/fd/9" ]]; then
    fail "controller cannot observe the live supervisor lock fd"
    return 1
  fi
  holder_target="$(readlink "/proc/$holder_pid/fd/9")" || return 1
  expected_target="$(realpath -m "$expected_lock_path")" || return 1
  if [[ "$holder_target" != "$expected_target" ]]; then
    fail "supervisor fd 9 references a different production lock"
    return 1
  fi
  python3 -B - "$holder_pid" "$expected_lock_path" <<'PY'
from pathlib import Path
import os
import stat
import sys

holder_pid, expected_name = sys.argv[1:]
expected = Path(expected_name)
expected_stat = os.stat(expected, follow_symlinks=False)
fd_path = Path(f"/proc/{holder_pid}/fd/9")
fd_stat = os.stat(fd_path)
if (
    not stat.S_ISREG(expected_stat.st_mode)
    or expected_stat.st_dev != fd_stat.st_dev
    or expected_stat.st_ino != fd_stat.st_ino
):
    raise SystemExit("[ERROR] supervisor fd 9 inode differs from canonical lock")
lock_lines = [
    line.split()
    for line in Path(f"/proc/{holder_pid}/fdinfo/9")
    .read_text(encoding="utf-8")
    .splitlines()
    if line.startswith("lock:")
]
matching = []
for fields in lock_lines:
    if len(fields) != 9:
        continue
    device_parts = fields[6].rsplit(":", 2)
    if len(device_parts) != 3:
        continue
    try:
        int(fields[5])
        major = int(device_parts[0], 16)
        minor = int(device_parts[1], 16)
        inode = int(device_parts[2])
    except ValueError:
        continue
    if (
        fields[2:5] == ["FLOCK", "ADVISORY", "WRITE"]
        and major == os.major(expected_stat.st_dev)
        and minor == os.minor(expected_stat.st_dev)
        and inode == expected_stat.st_ino
        and fields[7:] == ["0", "EOF"]
    ):
        matching.append(fields)
if len(matching) != 1:
    raise SystemExit(
        "[ERROR] supervisor fd 9 lacks one exact owned FLOCK write record"
    )
PY
  exec 8>"$expected_lock_path"
  if flock -n 8; then
    flock_rc=0
  else
    flock_rc=$?
  fi
  if [[ "$flock_rc" -eq 0 ]]; then
    flock -u 8 || true
    exec 8>&-
    fail "supervisor fd 9 does not hold the production mutation flock"
    return 1
  fi
  exec 8>&-
  if [[ "$flock_rc" -ne 1 ]]; then
    fail "controller could not verify the supervisor production flock"
    return 1
  fi
}

resolve_active_unit() {
  local active_units=()
  local unit=""
  for unit in \
    jato-fullstack-backend@8000.service \
    jato-fullstack-backend@8001.service; do
    if systemctl is-active --quiet "$unit"; then
      active_units+=("$unit")
    fi
  done
  if [[ "${#active_units[@]}" -ne 1 ]]; then
    fail "canary requires exactly one active production backend slot"
    return 1
  fi
  ACTIVE_UNIT="${active_units[0]}"
  export ACTIVE_UNIT
}

capture_snapshot() {
  local output="$1"
  python3 -B "$CANARY_GUARD" observe \
    --output "$output" \
    --public-origin "$CANARY_PUBLIC_ORIGIN" \
    --active-unit "$ACTIVE_UNIT" \
    --candidate-port "$CANARY_PORT"
}

assert_build_scope() {
  local expected_high=$((3 * 1024 * 1024 * 1024))
  local expected_max=$((4 * 1024 * 1024 * 1024))
  local actual_high=""
  local actual_max=""
  local actual_tasks=""
  local actual_swap=""
  local actual_protect_home=""
  local actual_protect_system=""
  local actual_no_new_privileges=""
  local actual_restart=""
  local actual_unit_file_state=""
  local actual_fragment=""
  local actual_inaccessible_paths=""
  local actual_write_paths=""
  local actual_after=""
  local actual_binds_to=""
  local actual_part_of=""
  local actual_stop_propagated_from=""
  local group=""
  actual_high="$(systemctl show "$BUILD_UNIT" -p MemoryHigh --value)"
  actual_max="$(systemctl show "$BUILD_UNIT" -p MemoryMax --value)"
  actual_tasks="$(systemctl show "$BUILD_UNIT" -p TasksMax --value)"
  actual_swap="$(systemctl show "$BUILD_UNIT" -p MemorySwapMax --value)"
  actual_protect_home="$(systemctl show "$BUILD_UNIT" -p ProtectHome --value)"
  actual_protect_system="$(systemctl show "$BUILD_UNIT" -p ProtectSystem --value)"
  actual_no_new_privileges="$(
    systemctl show "$BUILD_UNIT" -p NoNewPrivileges --value
  )"
  actual_restart="$(systemctl show "$BUILD_UNIT" -p Restart --value)"
  actual_unit_file_state="$(
    systemctl show "$BUILD_UNIT" -p UnitFileState --value
  )"
  actual_fragment="$(systemctl show "$BUILD_UNIT" -p FragmentPath --value)"
  actual_inaccessible_paths="$(
    systemctl show "$BUILD_UNIT" -p InaccessiblePaths --value
  )"
  actual_write_paths="$(systemctl show "$BUILD_UNIT" -p ReadWritePaths --value)"
  actual_binds_to="$(systemctl show "$BUILD_UNIT" -p BindsTo --value)"
  actual_part_of="$(systemctl show "$BUILD_UNIT" -p PartOf --value)"
  actual_after="$(systemctl show "$BUILD_UNIT" -p After --value)"
  actual_stop_propagated_from="$(
    systemctl show "$BUILD_UNIT" -p StopPropagatedFrom --value
  )"
  group="$(systemctl show "$BUILD_UNIT" -p ControlGroup --value)"
  if [[ "$actual_high" != "$expected_high" ]] \
    || [[ "$actual_max" != "$expected_max" ]] \
    || [[ "$actual_tasks" != "$CANARY_TASKS_MAX" ]] \
    || [[ "$actual_swap" != "0" ]] \
    || [[ "$actual_protect_home" != "yes" ]] \
    || [[ "$actual_protect_system" != "strict" ]] \
    || [[ "$actual_no_new_privileges" != "yes" ]] \
    || [[ "$actual_restart" != "no" ]] \
    || [[ "$actual_unit_file_state" != "transient" ]] \
    || [[ "$actual_fragment" != "/run/systemd/transient/$BUILD_UNIT" ]] \
    || [[ "$actual_write_paths" != *"$RUNTIME_ROOT"* ]] \
    || [[ "$actual_inaccessible_paths" != *"$LEGACY_ROOT/01_RAW_DATA"* ]] \
    || [[ "$actual_inaccessible_paths" != *"$LEGACY_ROOT/04_Processed_data"* ]] \
    || [[ "$actual_inaccessible_paths" != *"/etc/jato-fullstack"* ]] \
    || [[ -n "$actual_binds_to" ]] \
    || [[ -n "$actual_part_of" ]] \
    || [[ " $actual_after " != *" $SUPERVISOR_UNIT "* ]] \
    || [[ "$actual_stop_propagated_from" != "$SUPERVISOR_UNIT" ]] \
    || [[ -z "$group" || "$group" == "/" ]]; then
    fail "candidate build service lacks its reviewed sandbox/cgroup contract"
    return 1
  fi
  python3 -B - "$group" "$$" <<'PY'
from pathlib import Path
import sys

group, pid = sys.argv[1:]
members = Path("/sys/fs/cgroup") / group.lstrip("/") / "cgroup.procs"
if pid not in members.read_text(encoding="utf-8").splitlines():
    raise SystemExit("[ERROR] candidate build process escaped its cgroup")
PY
}

safe_extract_source_archive() {
  python3 -B - "$CANARY_SOURCE_ARCHIVE" "$RUNTIME_ROOT" <<'PY'
from pathlib import Path, PurePosixPath
import os
import shutil
import stat
import sys
import tarfile

archive_path = Path(sys.argv[1])
destination = Path(sys.argv[2])
if (
    destination.is_symlink()
    or not destination.is_dir()
    or any(destination.iterdir())
):
    raise SystemExit("[ERROR] candidate runtime destination is not an empty directory")
max_members = 50_000
max_expanded_bytes = 2 * 1024 * 1024 * 1024
max_member_bytes = 512 * 1024 * 1024
member_count = 0
expanded_bytes = 0
with tarfile.open(archive_path, mode="r:gz") as archive:
    for member in archive:
        member_count += 1
        if member_count > max_members:
            raise SystemExit("[ERROR] feature source archive contains too many members")
        if member.size < 0 or member.size > max_member_bytes:
            raise SystemExit("[ERROR] feature source archive member size is unsafe")
        expanded_bytes += member.size
        if expanded_bytes > max_expanded_bytes:
            raise SystemExit("[ERROR] feature source archive expands beyond 2 GiB")
if shutil.disk_usage(destination).free < expanded_bytes + 512 * 1024 * 1024:
    raise SystemExit("[ERROR] insufficient disk headroom for candidate extraction")
seen: set[str] = set()
with tarfile.open(archive_path, mode="r:gz") as archive:
    extracted = 0
    for member in archive:
        extracted += 1
        pure = PurePosixPath(member.name)
        parts = tuple(part for part in pure.parts if part != ".")
        if pure.is_absolute() or ".." in parts or not parts:
            if member.isdir() and member.name in {".", "./"}:
                continue
            raise SystemExit(f"[ERROR] unsafe feature source path: {member.name!r}")
        normalized = PurePosixPath(*parts).as_posix()
        if normalized in seen:
            raise SystemExit(f"[ERROR] duplicate feature source path: {normalized!r}")
        seen.add(normalized)
        target = destination.joinpath(*parts)
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
            target.chmod(0o755)
            continue
        if not member.isfile():
            raise SystemExit(
                f"[ERROR] feature source archive contains a link/device: {member.name!r}"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        source = archive.extractfile(member)
        if source is None:
            raise SystemExit(f"[ERROR] cannot read feature source member: {member.name!r}")
        with source, target.open("wb") as output:
            shutil.copyfileobj(source, output)
        mode = 0o755 if member.mode & stat.S_IXUSR else 0o644
        target.chmod(mode)
if extracted == 0:
    raise SystemExit("[ERROR] feature source archive is empty")
descriptor = os.open(destination, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
  CANARY_RUNTIME_CREATED=true
}

verify_packaged_feature_identity() {
  python3 -B - \
    "$RUNTIME_ROOT/hermes/deploy_release.json" \
    "$CANARY_REPOSITORY" "$CANARY_BRANCH" "$CANARY_COMMIT_SHA" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
if path.is_symlink() or not path.is_file():
    raise SystemExit("[ERROR] packaged feature release metadata is missing or unsafe")
payload = json.loads(path.read_text(encoding="utf-8"))
expected = {
    "repository": sys.argv[2],
    "branch": sys.argv[3],
    "expectedCommitSha": sys.argv[4],
}
for key, value in expected.items():
    if payload.get(key) != value:
        raise SystemExit(
            f"[ERROR] packaged feature metadata mismatch for {key}: "
            f"{payload.get(key)!r} != {value!r}"
        )
PY
}

build_candidate_runtime() {
  assert_build_scope
  safe_extract_source_archive
  verify_packaged_feature_identity

  python3 -m venv --copies "$RUNTIME_ROOT/.venv"
  "$RUNTIME_ROOT/.venv/bin/python" -m pip install --upgrade pip \
    -i "${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}" \
    --trusted-host "${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}"
  "$RUNTIME_ROOT/.venv/bin/pip" install \
    -r "$RUNTIME_ROOT/06_AppPlatform/backend/requirements.txt" \
    -i "${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}" \
    --trusted-host "${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}"
  "$RUNTIME_ROOT/.venv/bin/pip" install \
    -e "$RUNTIME_ROOT/07_ScrapingToolkit" \
    -i "${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}" \
    --trusted-host "${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}"

  "$RUNTIME_ROOT/.venv/bin/python" -B \
    "$RUNTIME_ROOT/03_Scripts/deploy/prepare_backend_release.py" \
    confirm-metadata \
    --path "$RUNTIME_ROOT/hermes/deploy_release.json" \
    --commit "$CANARY_COMMIT_SHA" \
    --service "jato-feature-candidate-canary"
  chmod -R a-w "$RUNTIME_ROOT"
}

run_build_scope() {
  local bash_bin=""
  local load_state=""
  bash_bin="$(command -v bash)"
  load_state="$(
    systemctl show "$BUILD_UNIT" -p LoadState --value 2>/dev/null || true
  )"
  if [[ "$load_state" != "not-found" ]] \
    || systemctl is-active --quiet "$BUILD_UNIT"; then
    fail "derived candidate build service name is already in use"
    return 1
  fi
  sudo -n systemd-run \
    --quiet \
    --wait \
    --pipe \
    --collect \
    --unit="$BUILD_UNIT" \
    --service-type=exec \
    --uid="$(id -u)" \
    --gid="$(id -g)" \
    --working-directory="$CANARY_ROOT" \
    --property="StopPropagatedFrom=$SUPERVISOR_UNIT" \
    --property="After=$SUPERVISOR_UNIT" \
    --property="RuntimeMaxSec=${CANARY_BUILD_TIMEOUT}s" \
    --property="Restart=no" \
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
    --property="InaccessiblePaths=$LEGACY_ROOT/01_RAW_DATA $LEGACY_ROOT/04_Processed_data /etc/jato-fullstack" \
    --property="ReadWritePaths=$RUNTIME_ROOT" \
    --property="MemoryHigh=$CANARY_MEMORY_HIGH" \
    --property="MemoryMax=$CANARY_MEMORY_MAX" \
    --property="MemorySwapMax=0" \
    --property="CPUQuota=100%" \
    --property="TasksMax=$CANARY_TASKS_MAX" \
    --setenv="HOME=/tmp" \
    --setenv="XDG_CACHE_HOME=/tmp/cache" \
    --setenv="PATH=$PATH" \
    --setenv="CANARY_MODE=build" \
    --setenv="CANARY_ROOT=$CANARY_ROOT" \
    --setenv="CANARY_STATE_ROOT=$CANARY_STATE_ROOT" \
    --setenv="CANARY_PORT=$CANARY_PORT" \
    --setenv="CANARY_MEMORY_HIGH=$CANARY_MEMORY_HIGH" \
    --setenv="CANARY_MEMORY_MAX=$CANARY_MEMORY_MAX" \
    --setenv="CANARY_TASKS_MAX=$CANARY_TASKS_MAX" \
    --setenv="CANARY_BUILD_TIMEOUT=$CANARY_BUILD_TIMEOUT" \
    --setenv="CANARY_RUNTIME_TIMEOUT=$CANARY_RUNTIME_TIMEOUT" \
    --setenv="CANARY_PUBLIC_ORIGIN=$CANARY_PUBLIC_ORIGIN" \
    --setenv="CANARY_FAULT=$CANARY_FAULT" \
    --setenv="LEGACY_ROOT=$LEGACY_ROOT" \
    --setenv="CANARY_COMMIT_SHA=$CANARY_COMMIT_SHA" \
    --setenv="CANARY_BRANCH=$CANARY_BRANCH" \
    --setenv="CANARY_REPOSITORY=$CANARY_REPOSITORY" \
    --setenv="CANARY_SOURCE_ARCHIVE=$CANARY_SOURCE_ARCHIVE" \
    --setenv="CANARY_SOURCE_SHA256=$CANARY_SOURCE_SHA256" \
    --setenv="CANARY_SOURCE_BYTES=$CANARY_SOURCE_BYTES" \
    --setenv="CANARY_RUN_ID=$CANARY_RUN_ID" \
    --setenv="RUN_KEY=$RUN_KEY" \
    --setenv="RUNTIME_ROOT=$RUNTIME_ROOT" \
    --setenv="CHECKPOINT_FILE=$CHECKPOINT_FILE" \
    --setenv="RECEIPT_FILE=$RECEIPT_FILE" \
    --setenv="EVIDENCE_FILE=$EVIDENCE_FILE" \
    --setenv="BEFORE_SNAPSHOT=$BEFORE_SNAPSHOT" \
    --setenv="AFTER_SNAPSHOT=$AFTER_SNAPSHOT" \
    --setenv="SUPERVISOR_UNIT=$SUPERVISOR_UNIT" \
    --setenv="CANARY_SUPERVISOR_INVOCATION_ID=$CANARY_SUPERVISOR_INVOCATION_ID" \
    --setenv="BUILD_UNIT=$BUILD_UNIT" \
    --setenv="SERVICE_UNIT=$SERVICE_UNIT" \
    --setenv="SERVICE_RUNTIME_DIRECTORY=$SERVICE_RUNTIME_DIRECTORY" \
    --setenv="PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}" \
    --setenv="PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST:-pypi.tuna.tsinghua.edu.cn}" \
    "$bash_bin" "$CONTROL_SCRIPT" build
}

start_candidate_service() {
  local backend="$RUNTIME_ROOT/06_AppPlatform/backend"
  local bash_bin=""
  local load_state=""
  local runtime_home="/run/$SERVICE_RUNTIME_DIRECTORY"
  bash_bin="$(command -v bash)"
  load_state="$(
    systemctl show "$SERVICE_UNIT" -p LoadState --value 2>/dev/null || true
  )"
  if [[ "$load_state" != "not-found" ]] \
    || systemctl is-active --quiet "$SERVICE_UNIT"; then
    fail "derived candidate transient service name is already in use"
    return 1
  fi
  sudo -n systemd-run \
    --quiet \
    --collect \
    --unit="$SERVICE_UNIT" \
    --service-type=exec \
    --working-directory="$backend" \
    --property="StopPropagatedFrom=$SUPERVISOR_UNIT" \
    --property="After=$SUPERVISOR_UNIT" \
    --property="RuntimeMaxSec=${CANARY_RUNTIME_TIMEOUT}s" \
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
    --property="MemoryHigh=$CANARY_MEMORY_HIGH" \
    --property="MemoryMax=$CANARY_MEMORY_MAX" \
    --property="MemorySwapMax=0" \
    --property="CPUQuota=100%" \
    --property="TasksMax=$CANARY_TASKS_MAX" \
    --property="RuntimeDirectory=$SERVICE_RUNTIME_DIRECTORY" \
    --property="ReadOnlyPaths=$LEGACY_ROOT/01_RAW_DATA $LEGACY_ROOT/04_Processed_data" \
    --property="ReadWritePaths=$runtime_home" \
    --setenv="HOME=$runtime_home" \
    --setenv="XDG_CACHE_HOME=$runtime_home/cache" \
    --setenv="PYTHONPATH=$backend" \
    --setenv="PYTHONDONTWRITEBYTECODE=1" \
    --setenv="PYTHONUNBUFFERED=1" \
    --setenv="CANARY_MODE=runtime" \
    --setenv="APP_PROJECT_ROOT=$RUNTIME_ROOT" \
    --setenv="APP_RELEASE_SHA=$CANARY_COMMIT_SHA" \
    --setenv="APP_RELEASE_SLOT=$CANARY_PORT" \
    --setenv="APP_BACKEND_WORKERS=2" \
    --setenv="APP_DATABASE_ENABLED=false" \
    --setenv="APP_REDIS_ENABLED=false" \
    --setenv="APP_JATO_MONTHLY_ENABLED=false" \
    --setenv="APP_JATO_MONTHLY_EXECUTION_MODE=disabled" \
    --setenv="APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=false" \
    --setenv="APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED=false" \
    --setenv="APP_METADATA_PREWARM_ENABLED=false" \
    --setenv="APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=false" \
    --setenv="HERMES_RUN_ENABLED=false" \
    --setenv="PGOPTIONS=-c default_transaction_read_only=on" \
    --setenv="JATO_PARQUET_PATH=$LEGACY_ROOT/04_Processed_data/jato_full_archive.parquet" \
    --setenv="JATO_PARTITIONED_PATH=$LEGACY_ROOT/04_Processed_data/partitioned_dataset_v1" \
    --setenv="APP_CRUD_DATA_PATH=$LEGACY_ROOT/04_Processed_data/app_entities.json" \
    --setenv="APP_ENGINEERING_IMPORT_ROOT=$LEGACY_ROOT/01_RAW_DATA" \
    --setenv="CANARY_ROOT=$CANARY_ROOT" \
    --setenv="CANARY_STATE_ROOT=$CANARY_STATE_ROOT" \
    --setenv="CANARY_BRANCH=$CANARY_BRANCH" \
    --setenv="CANARY_COMMIT_SHA=$CANARY_COMMIT_SHA" \
    --setenv="CANARY_PORT=$CANARY_PORT" \
    --setenv="CANARY_REPOSITORY=$CANARY_REPOSITORY" \
    --setenv="CANARY_SOURCE_SHA256=$CANARY_SOURCE_SHA256" \
    --setenv="CANARY_SOURCE_BYTES=$CANARY_SOURCE_BYTES" \
    --setenv="CANARY_RUN_ID=$CANARY_RUN_ID" \
    --setenv="CANARY_SUPERVISOR_INVOCATION_ID=$CANARY_SUPERVISOR_INVOCATION_ID" \
    --setenv="LEGACY_ROOT=$LEGACY_ROOT" \
    "$bash_bin" "$CONTROL_SCRIPT" runtime \
    "$RUNTIME_ROOT/.venv/bin/python" -m uvicorn app.main:app \
      --host 127.0.0.1 --port "$CANARY_PORT" --workers 2
}

verify_candidate_service() {
  local health=""
  local monthly_body=""
  local monthly_status=""
  local properties=""
  local readyz_evidence=""
  local expected_high=$((3 * 1024 * 1024 * 1024))
  local expected_max=$((4 * 1024 * 1024 * 1024))
  for attempt in $(seq 1 30); do
    if health="$(curl --noproxy '*' -fsS --max-time 10 \
      "http://127.0.0.1:${CANARY_PORT}/healthz")" \
      && readyz_evidence="$(python3 -B "$READINESS_VERIFIER" \
        --url "http://127.0.0.1:${CANARY_PORT}/readyz" \
        --expected-commit "$CANARY_COMMIT_SHA" \
        --timeout-seconds 10)"; then
      break
    fi
    if [[ "$attempt" -eq 30 ]]; then
      fail "candidate did not pass healthz and exact feature-SHA readyz"
      return 1
    fi
    sleep 2
  done

  monthly_body="$(mktemp)"
  if ! monthly_status="$(
    curl --noproxy '*' --silent --show-error --output "$monthly_body" \
      --write-out '%{http_code}' --max-time 10 \
      "http://127.0.0.1:${CANARY_PORT}/v1/msrp/monthly-update-jobs"
  )"; then
    rm -f "$monthly_body"
    return 1
  fi
  python3 -B - "$monthly_body" "$monthly_status" <<'PY'
import json
from pathlib import Path
import sys

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
detail = payload.get("detail") if isinstance(payload, dict) else None
if (
    sys.argv[2] != "423"
    or not isinstance(detail, dict)
    or detail.get("code") != "JATO_MONTHLY_DISABLED"
    or detail.get("reason") != "explicitly_disabled"
    or detail.get("enabled") is not False
):
    raise SystemExit("[ERROR] candidate monthly endpoint lacks structured explicit HTTP 423")
PY

  properties="$(systemctl show "$SERVICE_UNIT" \
    -p ActiveState -p UnitFileState -p DynamicUser -p ProtectSystem \
    -p ProtectHome -p NoNewPrivileges \
    -p Restart \
    -p MemoryHigh -p MemoryMax -p MemorySwapMax -p TasksMax \
    -p ExecStart -p Environment \
    -p ReadOnlyPaths -p MainPID -p ControlGroup \
    -p BindsTo -p PartOf -p After -p StopPropagatedFrom)"
  python3 -B - \
    "$EVIDENCE_FILE" "$properties" "$health" "$monthly_body" \
    "$monthly_status" "$expected_high" "$expected_max" \
    "$CANARY_COMMIT_SHA" "$CANARY_PORT" "$LEGACY_ROOT" "$SUPERVISOR_UNIT" \
    "$readyz_evidence" <<'PY'
import json
import os
from pathlib import Path
import re
import sys
import tempfile

(
    output_name,
    properties_raw,
    health_raw,
    monthly_name,
    monthly_status,
    expected_high,
    expected_max,
    commit,
    port,
    legacy_root,
    supervisor_unit,
    readyz_raw,
) = sys.argv[1:]
properties = {}
for line in properties_raw.splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value
required = {
    "ActiveState": "active",
    "UnitFileState": "transient",
    "DynamicUser": "yes",
    "ProtectSystem": "strict",
    "ProtectHome": "yes",
    "NoNewPrivileges": "yes",
    "Restart": "no",
    "MemoryHigh": expected_high,
    "MemoryMax": expected_max,
    "MemorySwapMax": "0",
    "TasksMax": "512",
}
for key, expected in required.items():
    if properties.get(key) != expected:
        raise SystemExit(
            f"[ERROR] candidate property {key} mismatch: "
            f"{properties.get(key)!r} != {expected!r}"
        )
health_payload = json.loads(health_raw)
if not isinstance(health_payload, dict) or health_payload.get("status") != "ok":
    raise SystemExit("[ERROR] candidate healthz is not status=ok")
readyz_payload = json.loads(readyz_raw)
readyz_observed = readyz_payload.get("observed")
readyz_release = (
    readyz_observed.get("release")
    if isinstance(readyz_observed, dict)
    else None
)
if (
    readyz_payload.get("ok") is not True
    or not isinstance(readyz_observed, dict)
    or readyz_observed.get("status") != "ready"
    or not isinstance(readyz_release, dict)
    or readyz_release.get("commitSha") != commit
):
    raise SystemExit("[ERROR] candidate readyz evidence lacks the exact feature SHA")
if "--workers 2" not in properties.get("ExecStart", ""):
    raise SystemExit("[ERROR] candidate ExecStart does not use exactly two workers")
environment = properties.get("Environment", "")
for value in (
    "APP_DATABASE_ENABLED=false",
    "APP_REDIS_ENABLED=false",
    "APP_JATO_MONTHLY_ENABLED=false",
    "APP_JATO_MONTHLY_EXECUTION_MODE=disabled",
    "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=false",
    "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED=false",
    "APP_METADATA_PREWARM_ENABLED=false",
    "APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=false",
    "HERMES_RUN_ENABLED=false",
):
    if value not in environment:
        raise SystemExit(f"[ERROR] candidate environment omitted {value}")
if (
    f"{legacy_root}/01_RAW_DATA" not in properties.get("ReadOnlyPaths", "")
    or f"{legacy_root}/04_Processed_data" not in properties.get("ReadOnlyPaths", "")
):
    raise SystemExit("[ERROR] candidate data paths are not read-only")
if supervisor_unit not in properties.get("After", "").split():
    raise SystemExit("[ERROR] candidate omits durable supervisor ordering")
if set(properties.get("StopPropagatedFrom", "").split()) != {supervisor_unit}:
    raise SystemExit(
        "[ERROR] candidate omits exact stop-only supervisor propagation"
    )
if properties.get("BindsTo", "").split() or properties.get("PartOf", "").split():
    raise SystemExit(
        "[ERROR] candidate gained a restart-propagating supervisor dependency"
    )
group = properties.get("ControlGroup", "")
if not group or ".." in Path(group).parts:
    raise SystemExit("[ERROR] candidate cgroup is unsafe")
processes = (
    Path("/sys/fs/cgroup") / group.lstrip("/") / "cgroup.procs"
).read_text(encoding="utf-8").splitlines()
if not processes:
    raise SystemExit("[ERROR] candidate cgroup has no processes")
main_pid = properties.get("MainPID", "")
if not main_pid.isdigit() or int(main_pid) <= 0:
    raise SystemExit("[ERROR] candidate MainPID is invalid")
worker_pids = []
for pid in processes:
    try:
        command = (
            Path("/proc") / pid / "cmdline"
        ).read_bytes().replace(b"\0", b" ")
        status = (Path("/proc") / pid / "status").read_text(
            encoding="utf-8"
        )
    except OSError:
        continue
    if b"jato_monthly_worker.py" in command:
        raise SystemExit("[ERROR] candidate cgroup started a monthly worker")
    parent_pid = ""
    for line in status.splitlines():
        if line.startswith("PPid:"):
            parent_pid = line.partition(":")[2].strip()
            break
    if (
        parent_pid == main_pid
        and b"multiprocessing.spawn" in command
        and b"spawn_main" in command
    ):
        worker_pids.append(int(pid))
if len(worker_pids) != 2:
    raise SystemExit(
        "[ERROR] candidate must have exactly two live Uvicorn workers; "
        f"found {len(worker_pids)}"
    )
payload = {
    "status": "verified",
    "featureCommit": commit,
    "port": int(port),
    "healthz": health_payload,
    "readyz": readyz_payload,
    "monthlyStatus": int(monthly_status),
    "monthlyResponse": json.loads(Path(monthly_name).read_text(encoding="utf-8")),
    "liveBackendWorkerCount": len(worker_pids),
    "liveBackendWorkerPids": sorted(worker_pids),
    "systemd": properties,
}
output = Path(output_name)
output.parent.mkdir(parents=True, exist_ok=True)
descriptor, temporary_name = tempfile.mkstemp(
    prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
)
with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
os.replace(temporary_name, output)
PY
  rm -f "$monthly_body"
  record_checkpoint candidate_verified completed \
    "loopback candidate passed health, exact SHA, sandbox, cgroup and monthly 423"
}

stop_verified_transient_unit() {
  local unit="$1"
  local expected_environment="$2"
  shift 2
  local expected_argv=("$@")
  local current_load_state=""
  local current_properties=""
  local current_invocation_id=""
  local properties=""
  local verified_invocation_id=""
  properties="$(
    systemctl show "$unit" \
      -p LoadState -p ActiveState -p UnitFileState -p FragmentPath -p ExecStart \
      -p Environment -p ControlGroup -p BindsTo -p PartOf -p After \
      -p StopPropagatedFrom \
      -p InvocationID \
      2>/dev/null || true
  )"
  if [[ "$properties" == *$'LoadState=not-found\n'* ]] \
    || [[ "$properties" == "LoadState=not-found" ]]; then
    return 0
  fi
  if ! verified_invocation_id="$(
    python3 -B - \
    "$properties" "$unit" "$expected_environment" "$SUPERVISOR_UNIT" \
    "${#expected_argv[@]}" "${expected_argv[@]}" <<'PY'
import re
import shlex
import sys

(
    raw,
    unit,
    expected_environment,
    supervisor_unit,
) = sys.argv[1:5]
argv_count = int(sys.argv[5])
expected_argv = sys.argv[6 : 6 + argv_count]
properties = {}
for line in raw.splitlines():
    key, separator, value = line.partition("=")
    if separator:
        properties[key] = value
expected_fragment = f"/run/systemd/transient/{unit}"
active_state = properties.get("ActiveState", "")
control_group = properties.get("ControlGroup", "")
exec_start = properties.get("ExecStart", "")
invocation_id = properties.get("InvocationID", "")
if exec_start.count("argv[]=") != 1:
    raise SystemExit("[ERROR] refusing to stop a unit with ambiguous ExecStart")
argv_raw = exec_start.split("argv[]=", 1)[1].split(" ; ", 1)[0]
try:
    actual_argv = shlex.split(argv_raw)
    environment_tokens = shlex.split(properties.get("Environment", ""))
except ValueError as exc:
    raise SystemExit(
        "[ERROR] refusing to stop a unit with malformed argv/environment"
    ) from exc
environment = {}
for token in environment_tokens:
    key, separator, value = token.partition("=")
    if not separator or not key or key in environment:
        raise SystemExit(
            "[ERROR] refusing to stop a unit with duplicate/malformed environment"
        )
    environment[key] = value
expected_key, separator, expected_value = expected_environment.partition("=")
if (
    properties.get("LoadState") != "loaded"
    or properties.get("UnitFileState") != "transient"
    or properties.get("FragmentPath") != expected_fragment
    or re.fullmatch(r"[0-9A-Fa-f]{32}", invocation_id) is None
    or invocation_id == "0" * 32
    or actual_argv != expected_argv
    or not separator
    or environment.get(expected_key) != expected_value
    or properties.get("BindsTo", "").split()
    or properties.get("PartOf", "").split()
    or supervisor_unit not in properties.get("After", "").split()
    or set(properties.get("StopPropagatedFrom", "").split()) != {supervisor_unit}
    or (
        active_state in {"active", "activating", "deactivating"}
        and control_group != f"/system.slice/{unit}"
    )
):
    raise SystemExit("[ERROR] refusing to stop a unit without exact canary identity")
print(invocation_id.lower())
PY
  )"; then
    current_load_state="$(
      systemctl show "$unit" -p LoadState --value 2>/dev/null || true
    )"
    if [[ "$current_load_state" == "not-found" ]]; then
      return 0
    fi
    return 1
  fi
  if ! sudo -n systemctl stop "$unit" >/dev/null 2>&1; then
    current_load_state="$(
      systemctl show "$unit" -p LoadState --value 2>/dev/null || true
    )"
    if [[ "$current_load_state" == "not-found" ]]; then
      return 0
    fi
    return 1
  fi
  sudo -n systemctl reset-failed "$unit" >/dev/null 2>&1 || true
  for _attempt in $(seq 1 30); do
    current_properties="$(
      systemctl show "$unit" \
        -p LoadState -p ActiveState -p InvocationID \
        2>/dev/null || true
    )"
    current_load_state=""
    current_invocation_id=""
    while IFS="=" read -r property_name property_value; do
      case "$property_name" in
        LoadState)
          current_load_state="$property_value"
          ;;
        InvocationID)
          current_invocation_id="${property_value,,}"
          ;;
      esac
    done <<<"$current_properties"
    if [[ "$current_load_state" == "not-found" ]]; then
      return 0
    fi
    if [[ "$current_load_state" != "loaded" ]] \
      || [[ "$current_invocation_id" != "$verified_invocation_id" ]]; then
      echo \
        "[ERROR] transient canary unit identity changed while waiting for collect: $unit" \
        >&2
      return 1
    fi
    sleep 1
  done
  echo "[ERROR] transient canary unit was not collected: $unit" >&2
  return 1
}

cleanup_candidate() {
  local bash_bin=""
  local cleanup_rc=0
  bash_bin="$(command -v bash)"
  # Durable reconciliation cannot trust in-memory "attempted" flags.  Derived
  # child names plus exact transient-unit identity are the sole stop authority.
  stop_verified_transient_unit \
    "$SERVICE_UNIT" \
    "APP_RELEASE_SHA=$CANARY_COMMIT_SHA" \
    "$bash_bin" "$CONTROL_SCRIPT" runtime \
    "$RUNTIME_ROOT/.venv/bin/python" \
    -m uvicorn app.main:app \
    --host 127.0.0.1 --port "$CANARY_PORT" --workers 2 \
    || cleanup_rc=1
  stop_verified_transient_unit \
    "$BUILD_UNIT" \
    "RUN_KEY=$RUN_KEY" \
    "$bash_bin" "$CONTROL_SCRIPT" build \
    || cleanup_rc=1
  if [[ "$cleanup_rc" -eq 0 ]]; then
    case "$RUNTIME_ROOT" in
      /opt/jato-canary/runtime/*)
        if sudo -n test -e "$RUNTIME_ROOT" \
          || sudo -n test -L "$RUNTIME_ROOT"; then
          sudo -n chmod -R u+w "$RUNTIME_ROOT" >/dev/null 2>&1 || true
          sudo -n rm -rf --one-file-system "$RUNTIME_ROOT" || cleanup_rc=1
        fi
        ;;
      *)
        echo "[ERROR] refusing to clean runtime outside canary root" >&2
        cleanup_rc=1
        ;;
    esac
    case "$STAGED_SOURCE_ARCHIVE" in
      /opt/jato-canary/sources/*.tar.gz)
        if sudo -n test -e "$STAGED_SOURCE_ARCHIVE" \
          || sudo -n test -L "$STAGED_SOURCE_ARCHIVE"; then
          sudo -n rm -f "$STAGED_SOURCE_ARCHIVE" || cleanup_rc=1
        fi
        ;;
      *)
        echo "[ERROR] refusing to clean source archive outside canary root" >&2
        cleanup_rc=1
        ;;
    esac
  fi
  if [[ "$cleanup_rc" -eq 0 ]]; then
    for ephemeral in "$RUNTIME_ROOT" "$STAGED_SOURCE_ARCHIVE"; do
      if sudo -n test -e "$ephemeral" || sudo -n test -L "$ephemeral"; then
        echo "[ERROR] canary ephemeral path remained after cleanup: $ephemeral" >&2
        cleanup_rc=1
      fi
    done
  fi
  return "$cleanup_rc"
}

verify_retained_control_bundle() {
  case "$CONTROL_ROOT" in
    /opt/jato-canary/control/*)
      ;;
    *)
      echo "[ERROR] refusing a retained control path outside canary root" >&2
      return 1
      ;;
  esac
  verify_canary_parent_roots
  python3 -B - "$CONTROL_ROOT" <<'PY'
from pathlib import Path
import os
import stat
import sys

root = Path(sys.argv[1])
directories = (
    root,
    root / "03_Scripts",
    root / "03_Scripts/deploy",
    root / "03_Scripts/deploy/lib",
)
files = {
    root / "03_Scripts/deploy/tencent_feature_candidate_canary.sh": 0o555,
    root / "03_Scripts/deploy/jato_feature_canary_guard.py": 0o444,
    root / "03_Scripts/deploy/lib/production_mutation_lock.sh": 0o444,
    root / "03_Scripts/deploy/verify_backend_readiness.py": 0o444,
}
expected_members = set(directories[1:]) | set(files)
observed_members = set(root.rglob("*"))
if observed_members != expected_members:
    raise SystemExit("[ERROR] retained canary control bundle has unexpected members")
for path in directories:
    metadata = os.lstat(path)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise SystemExit(
            f"[ERROR] retained canary control directory is mutable/unsafe: {path}"
        )
for path, expected_mode in files.items():
    metadata = os.lstat(path)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise SystemExit(
            f"[ERROR] retained canary control file is mutable/unsafe: {path}"
        )
PY
  if [[ "$?" -ne 0 ]]; then
    return 1
  fi
  echo \
    "[INFO] Root-owned control evidence retained until the supervisor is collected: $CONTROL_ROOT"
}

wait_for_candidate_port_release() {
  local attempt=0
  for attempt in $(seq 0 "$CANARY_PORT_RELEASE_TIMEOUT_SECONDS"); do
    if python3 -B "$CANARY_GUARD" verify-port-free \
      --port "$CANARY_PORT" >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$attempt" -lt "$CANARY_PORT_RELEASE_TIMEOUT_SECONDS" ]]; then
      sleep 1
    fi
  done
  echo \
    "[ERROR] candidate loopback port did not quiesce within ${CANARY_PORT_RELEASE_TIMEOUT_SECONDS}s" \
    >&2
  return 1
}

finalize_canary() {
  local original_rc="$?"
  local final_rc="$original_rc"
  local comparison_rc=0
  local cleanup_rc=0
  local port_release_rc=0
  local checkpoint_rc=0
  if [[ "$CANARY_FINALIZING" == "true" ]]; then
    return "$original_rc"
  fi
  CANARY_FINALIZING=true
  trap - EXIT ERR HUP INT TERM
  set +e
  if [[ "$CANARY_FAULT" == "after_candidate_start" ]] \
    && [[ "$CANARY_EXPECTED_FAILURE_OBSERVED" == "true" ]] \
    && [[ "$original_rc" -eq 97 ]]; then
    if verify_checkpoint_marker fault_observed; then
      final_rc=0
    else
      CANARY_ERROR="${CANARY_ERROR:+$CANARY_ERROR; }durable fault marker missing"
      final_rc=1
    fi
  elif [[ "$original_rc" -eq 0 ]]; then
    if ! verify_checkpoint_marker controller_completed; then
      CANARY_ERROR="${CANARY_ERROR:+$CANARY_ERROR; }durable controller completion marker missing"
      final_rc=1
    fi
  fi

  cleanup_candidate
  cleanup_rc=$?
  if [[ "$cleanup_rc" -ne 0 ]]; then
    CANARY_ERROR="${CANARY_ERROR:+$CANARY_ERROR; }candidate cleanup failed"
    final_rc=1
  else
    wait_for_candidate_port_release
    port_release_rc=$?
    if [[ "$port_release_rc" -ne 0 ]]; then
      CANARY_ERROR="${CANARY_ERROR:+$CANARY_ERROR; }candidate loopback port did not quiesce after cleanup"
      final_rc=1
    fi
  fi

  if [[ -n "$ACTIVE_UNIT" && -f "$BEFORE_SNAPSHOT" ]]; then
    capture_snapshot "$AFTER_SNAPSHOT"
    comparison_rc=$?
    if [[ "$comparison_rc" -eq 0 ]]; then
      python3 -B "$CANARY_GUARD" verify-baseline \
        --snapshot "$AFTER_SNAPSHOT"
      comparison_rc=$?
    fi
    if [[ "$comparison_rc" -eq 0 ]]; then
      python3 -B "$CANARY_GUARD" compare \
        --before "$BEFORE_SNAPSHOT" \
        --after "$AFTER_SNAPSHOT"
      comparison_rc=$?
    fi
    if [[ "$comparison_rc" -ne 0 ]]; then
      CANARY_ERROR="${CANARY_ERROR:+$CANARY_ERROR; }production invariants changed"
      final_rc=1
    fi
  else
    comparison_rc=1
    final_rc=1
  fi

  if [[ -f "$CHECKPOINT_FILE" ]]; then
    if [[ "$final_rc" -eq 0 ]]; then
      if [[ "$CANARY_EXPECTED_FAILURE_OBSERVED" == "true" ]]; then
        record_checkpoint expected_failure_verified completed \
          "injected candidate failure cleaned up; old production remained exact"
        checkpoint_rc=$?
      else
        record_checkpoint cleanup_verified completed \
          "transient service/runtime removed; production snapshot unchanged"
        checkpoint_rc=$?
      fi
      if [[ "$checkpoint_rc" -ne 0 ]]; then
        CANARY_ERROR="${CANARY_ERROR:+$CANARY_ERROR; }terminal checkpoint failed"
        final_rc=1
      fi
    else
      record_checkpoint cleanup_verified failed \
        "${CANARY_ERROR:-canary failed}; old production comparison rc=$comparison_rc"
      checkpoint_rc=$?
      if [[ "$checkpoint_rc" -ne 0 ]]; then
        CANARY_ERROR="${CANARY_ERROR:+$CANARY_ERROR; }failure checkpoint failed"
        final_rc=1
      fi
    fi
  else
    final_rc=1
  fi
  echo \
    "[INFO] Controller evidence finalized; durable supervisor reconciliation owns the terminal receipt"
  exit "$final_rc"
}

acquire_canary_production_lock() {
  export JATO_BLUEGREEN_SWITCH_UNIT=jato-bluegreen-production.service
  # shellcheck source=03_Scripts/deploy/lib/production_mutation_lock.sh
  source "$MUTATION_LOCK_HELPER"
  jato_acquire_production_mutation_lock
  if [[ "$JATO_PRODUCTION_DEPLOY_LOCK_PATH" != \
    "${DEPLOY_STATE_DIR%/}/production-deploy.lock" ]]; then
    fail "canary did not acquire the existing production mutation lock"
    return 1
  fi
}

assert_supervisor_unit_available() {
  local active_state=""
  local load_state=""
  load_state="$(
    systemctl show "$SUPERVISOR_UNIT" -p LoadState --value 2>/dev/null || true
  )"
  active_state="$(
    systemctl show "$SUPERVISOR_UNIT" -p ActiveState --value 2>/dev/null || true
  )"
  if [[ "$load_state" != "not-found" ]] \
    || [[ -n "$active_state" && "$active_state" != "inactive" ]]; then
    fail "derived durable canary supervisor name is already in use"
    return 1
  fi
}

start_canary_supervisor() {
  local bash_bin=""
  local environment_args=()
  local environment_entry=""
  bash_bin="$(command -v bash)"
  assert_supervisor_unit_available
  build_canary_control_environment supervisor
  for environment_entry in "${CANARY_CONTROL_ENVIRONMENT[@]}"; do
    environment_args+=("--setenv=$environment_entry")
  done
  sudo -n systemd-run \
    --quiet \
    --collect \
    --unit="$SUPERVISOR_UNIT" \
    --service-type=exec \
    --uid="$(id -u)" \
    --gid="$(id -g)" \
    --working-directory="$CONTROL_ROOT" \
    --property="TimeoutStopSec=${CANARY_SUPERVISOR_STOP_TIMEOUT_SECONDS}s" \
    --property="KillMode=control-group" \
    --property="Restart=on-failure" \
    --property="RestartSec=${CANARY_SUPERVISOR_RESTART_SECONDS}s" \
    --property="StartLimitIntervalSec=0" \
    --property="MemoryHigh=$CANARY_SUPERVISOR_MEMORY_HIGH" \
    --property="MemoryMax=$CANARY_SUPERVISOR_MEMORY_MAX" \
    --property="MemorySwapMax=0" \
    --property="TasksMax=$CANARY_SUPERVISOR_TASKS_MAX" \
    "${environment_args[@]}" \
    "$bash_bin" "$CONTROL_SCRIPT" supervisor
}

assert_controller_unit_available() {
  local active_state=""
  local load_state=""
  load_state="$(
    systemctl show "$CONTROLLER_UNIT" -p LoadState --value 2>/dev/null || true
  )"
  active_state="$(
    systemctl show "$CONTROLLER_UNIT" -p ActiveState --value 2>/dev/null || true
  )"
  if [[ "$load_state" != "not-found" ]] \
    || [[ -n "$active_state" && "$active_state" != "inactive" ]]; then
    fail "derived durable canary controller name is already in use"
    return 1
  fi
}

run_canary_controller_unit() {
  local bash_bin=""
  local controller_timeout=0
  local environment_args=()
  local environment_entry=""
  bash_bin="$(command -v bash)"
  controller_timeout=$((CANARY_BUILD_TIMEOUT + CANARY_RUNTIME_TIMEOUT + 300))
  assert_controller_unit_available
  build_canary_control_environment controller
  for environment_entry in "${CANARY_CONTROL_ENVIRONMENT[@]}"; do
    environment_args+=("--setenv=$environment_entry")
  done
  sudo -n systemd-run \
    --quiet \
    --wait \
    --pipe \
    --collect \
    --unit="$CONTROLLER_UNIT" \
    --service-type=exec \
    --uid="$(id -u)" \
    --gid="$(id -g)" \
    --working-directory="$CONTROL_ROOT" \
    --property="StopPropagatedFrom=$SUPERVISOR_UNIT" \
    --property="After=$SUPERVISOR_UNIT" \
    --property="RuntimeMaxSec=${controller_timeout}s" \
    --property="TimeoutStopSec=${CANARY_CONTROLLER_RECOVERY_TIMEOUT_SECONDS}s" \
    --property="KillMode=control-group" \
    --property="SendSIGKILL=yes" \
    --property="Restart=no" \
    --property="MemoryHigh=$CANARY_CONTROLLER_MEMORY_HIGH" \
    --property="MemoryMax=$CANARY_CONTROLLER_MEMORY_MAX" \
    --property="MemorySwapMax=0" \
    --property="TasksMax=$CANARY_CONTROLLER_TASKS_MAX" \
    "${environment_args[@]}" \
    "$bash_bin" "$CONTROL_SCRIPT" controller
}

quiesce_canary_controller_unit() {
  local active_state=""
  local bash_bin=""
  local control_group=""
  local load_state=""
  local members_path=""
  bash_bin="$(command -v bash)"
  stop_verified_transient_unit \
    "$CONTROLLER_UNIT" \
    "RUN_KEY=$RUN_KEY" \
    "$bash_bin" "$CONTROL_SCRIPT" controller \
    || return 1
  for _attempt in $(seq 1 30); do
    load_state="$(
      systemctl show "$CONTROLLER_UNIT" -p LoadState --value 2>/dev/null || true
    )"
    if [[ "$load_state" == "not-found" ]]; then
      return 0
    fi
    active_state="$(
      systemctl show "$CONTROLLER_UNIT" -p ActiveState --value 2>/dev/null || true
    )"
    control_group="$(
      systemctl show "$CONTROLLER_UNIT" -p ControlGroup --value 2>/dev/null || true
    )"
    members_path="/sys/fs/cgroup/${control_group#/}/cgroup.procs"
    if [[ "$active_state" != "active" ]] \
      && [[ "$active_state" != "activating" ]] \
      && [[ "$active_state" != "deactivating" ]] \
      && {
        [[ -z "$control_group" ]] \
          || [[ ! -e "$members_path" ]] \
          || [[ ! -s "$members_path" ]];
      }; then
      sudo -n systemctl reset-failed "$CONTROLLER_UNIT" >/dev/null 2>&1 || true
      return 0
    fi
    sleep 1
  done
  fail "durable canary controller cgroup did not become quiescent"
}

launch_canary() {
  validate_static_contract
  pin_canary_production_lock_path
  initialize_paths
  ensure_canary_roots
  stage_canary_inputs
  record_checkpoint initialized in_progress \
    "feature canary inputs staged; durable supervisor launch requested"
  if ! start_canary_supervisor; then
    record_checkpoint supervisor_launch_failed failed \
      "systemd rejected or could not start the durable canary supervisor" \
      || true
    echo \
      "[ERROR] Durable supervisor launch failed; staged control bundle retained for inspection" \
      >&2
    return 1
  fi
  echo "[INFO] Durable feature canary supervisor started: $SUPERVISOR_UNIT"
  echo "[INFO] Poll durable receipt: $RECEIPT_FILE"
}

checkpoint_marker_present() {
  verify_checkpoint_marker "$1" "${2:-completed}" >/dev/null 2>&1
}

run_canary_supervisor() {
  local controller_rc=0
  local reconcile_rc=0
  initialize_paths
  if [[ -f "$RECEIPT_FILE" ]] \
    || checkpoint_marker_present controller_unit_started in_progress; then
    # A restarted supervisor must be able to recover after normal cleanup has
    # already removed the staged source archive.  Recovery validates the
    # root-owned control plane and durable identity instead of reopening input.
    validate_reconcile_contract
  else
    validate_static_contract
    assert_supervisor_scope
  fi
  verify_checkpoint_marker initialized in_progress

  # The supervisor is the sole durable owner of fd 9.  The business controller
  # runs in its own transient systemd cgroup, so systemd can prove that the
  # complete controller process tree is dead before reconciliation begins.
  acquire_canary_production_lock
  capture_supervisor_invocation_id
  if ! checkpoint_marker_present supervisor_started in_progress; then
    record_checkpoint supervisor_started in_progress \
      "durable supervisor acquired the canonical production mutation lock"
  fi

  if [[ ! -f "$RECEIPT_FILE" ]] \
    && ! checkpoint_marker_present controller_unit_started in_progress; then
    record_checkpoint controller_unit_started in_progress \
      "isolated controller unit launch committed; later attempts are recovery-only"
    set +e
    run_canary_controller_unit
    controller_rc=$?
    set -e
  else
    controller_rc=125
    echo \
      "[WARN] Durable supervisor restart detected; business canary will not be rerun" \
      >&2
  fi
  export CANARY_CONTROLLER_RC="$controller_rc"
  export CANARY_SUPERVISOR_STOP_REQUESTED

  if ! quiesce_canary_controller_unit; then
    echo \
      "[ERROR] Durable canary controller tree is not quiescent; refusing concurrent reconciliation" \
      >&2
    return 1
  fi

  set +e
  bash "$CONTROL_SCRIPT" reconcile
  reconcile_rc=$?
  set -e
  if [[ "$reconcile_rc" -ne 0 ]]; then
    echo \
      "[ERROR] Durable canary reconciliation is incomplete; systemd will retry the supervisor" \
      >&2
    return 1
  fi
  return 0
}

run_canary_controller() {
  initialize_paths
  assert_supervisor_generation
  validate_static_contract
  assert_supervisor_scope
  assert_controller_scope
  assert_supervisor_production_lock
  verify_checkpoint_marker initialized in_progress
  verify_checkpoint_marker supervisor_started in_progress
  record_checkpoint controller_started in_progress \
    "isolated controller verified the supervisor's durable production lock"

  # The controller is a separate transient cgroup and never owns or reacquires
  # fd 9.  The exact supervisor identity, its live fd 9 target, and a failed
  # nonblocking second flock prove the production fence before any snapshot.
  resolve_active_unit
  capture_snapshot "$BEFORE_SNAPSHOT"
  python3 -B "$CANARY_GUARD" verify-baseline \
    --snapshot "$BEFORE_SNAPSHOT"
  record_checkpoint baseline_verified completed \
    "public health/build SHA and active 6G/8G/2-worker baseline verified"

  # Feature and dependency code runs only inside the isolated transient build
  # service. The parent retains the production lock and durable evidence.
  run_build_scope
  record_checkpoint source_verified completed \
    "staged archive identity and control-plane provenance verified"
  record_checkpoint runtime_built completed \
    "candidate dependencies built inside transient 3G/4G/0-swap sandbox"

  start_candidate_service
  verify_candidate_service
  if [[ "$CANARY_FAULT" == "after_candidate_start" ]]; then
    CANARY_EXPECTED_FAILURE_OBSERVED=true
    CANARY_ERROR="expected fault injection: after_candidate_start"
    record_checkpoint fault_observed completed \
      "expected after_candidate_start fault reached its durable boundary"
    return 97
  fi
  record_checkpoint controller_completed completed \
    "candidate verification completed under the durable controller"
}

run_candidate_runtime() {
  local actual_argv=("$@")
  local expected_argv=()
  initialize_paths
  assert_supervisor_generation
  verify_canary_parent_roots
  validate_feature_identity
  expected_argv=(
    "$RUNTIME_ROOT/.venv/bin/python"
    -m uvicorn app.main:app
    --host 127.0.0.1
    --port "$CANARY_PORT"
    --workers 2
  )
  if [[ "$#" -ne "${#expected_argv[@]}" ]]; then
    fail "candidate runtime wrapper received an unexpected argv length"
    return 1
  fi
  for index in "${!expected_argv[@]}"; do
    if [[ "${actual_argv[$index]}" != "${expected_argv[$index]}" ]]; then
      fail "candidate runtime wrapper received unexpected argv"
      return 1
    fi
  done
  exec "${expected_argv[@]}"
}

validate_reconcile_contract() {
  local account_home=""
  local canonical_deploy_state=""
  for command_name in \
    curl flock getent python3 readlink realpath sha256sum stat systemctl timeout; do
    require_command "$command_name"
  done
  if [[ "$CANARY_ROOT" != "/opt/jato-canary" ]] \
    || [[ "$CANARY_STATE_ROOT" != "/var/lib/jato-canary" ]] \
    || [[ "$CANARY_PORT" != "18001" ]] \
    || [[ "$CANARY_MEMORY_HIGH" != "3G" ]] \
    || [[ "$CANARY_MEMORY_MAX" != "4G" ]] \
    || [[ "$CANARY_TASKS_MAX" != "512" ]]; then
    fail "reconcile canary root, port, and resource identity are not exact"
    return 1
  fi
  validate_feature_identity
  verify_canary_parent_roots
  account_home="$(
    getent passwd "$(id -u)" | awk -F: 'NR == 1 {print $6}'
  )"
  canonical_deploy_state="${account_home%/}/.local/state/jato-production-release"
  if [[ -z "$account_home" || "$account_home" != /* ]] \
    || [[ "${HOME:-}" != "$account_home" ]] \
    || [[ "${DEPLOY_STATE_DIR:-}" != "$canonical_deploy_state" ]]; then
    fail "reconcile requires the canonical deploy account and state directory"
    return 1
  fi
  if [[ -n "${JATO_PRODUCTION_DEPLOY_LOCK_PATH:-}" ]] \
    && [[ "$JATO_PRODUCTION_DEPLOY_LOCK_PATH" != \
      "$canonical_deploy_state/production-deploy.lock" ]]; then
    fail "reconcile production lock override is not canonical"
    return 1
  fi
  if [[ "$CANARY_INITIAL_LOCK_PATH" != \
    "$canonical_deploy_state/production-deploy.lock" ]]; then
    fail "reconcile initial production lock identity is not canonical"
    return 1
  fi
  if [[ "$SCRIPT_DIR/tencent_feature_candidate_canary.sh" != \
    "$CONTROL_SCRIPT" ]]; then
    fail "reconcile is not executing from the exact staged control bundle"
    return 1
  fi
  for control_file in \
    "$CONTROL_SCRIPT" "$CANARY_GUARD" "$MUTATION_LOCK_HELPER" \
    "$READINESS_VERIFIER"; do
    if [[ ! -f "$control_file" || -L "$control_file" ]] \
      || [[ "$(stat -c '%u' "$control_file")" != "0" ]]; then
      fail "reconcile control-plane file is missing, mutable, or unsafe"
      return 1
    fi
  done
  assert_supervisor_scope
}

write_terminal_receipt() {
  local outcome="$1"
  local error="$2"
  local identity=(
    --repository "$CANARY_REPOSITORY"
    --branch "$CANARY_BRANCH"
    --commit "$CANARY_COMMIT_SHA"
    --archive-sha256 "$CANARY_SOURCE_SHA256"
    --archive-bytes "$CANARY_SOURCE_BYTES"
    --run-id "$CANARY_RUN_ID"
    --port "$CANARY_PORT"
  )
  if [[ "$CANARY_MODE" != "reconcile" ]]; then
    fail "only supervisor reconciliation may write a terminal canary receipt"
    return 1
  fi
  if [[ ! "$CANARY_SUPERVISOR_INVOCATION_ID" =~ ^[0-9a-f]{32}$ ]] \
    || [[ "$CANARY_SUPERVISOR_INVOCATION_ID" == \
      "00000000000000000000000000000000" ]]; then
    fail "terminal receipt writer lacks its supervisor generation"
    return 1
  fi
  python3 -B "$CANARY_GUARD" finalize \
    --path "$RECEIPT_FILE" \
    "${identity[@]}" \
    --outcome "$outcome" \
    --fault "$CANARY_FAULT" \
    --error "$error" \
    --before "$BEFORE_SNAPSHOT" \
    --after "$AFTER_SNAPSHOT" \
    --candidate "$EVIDENCE_FILE" \
    --checkpoint "$CHECKPOINT_FILE" \
    --terminal-writer supervisor_reconcile \
    --writer-invocation-id "$CANARY_SUPERVISOR_INVOCATION_ID"
}

reconcile_canary_controller() {
  local cleanup_rc=0
  local comparison_rc=0
  local controller_completed=false
  local controller_cleanup_completed=false
  local fault_observed=false
  local fault_cleanup_completed=false
  local outcome="failed"
  local receipt_error=""
  initialize_paths
  validate_reconcile_contract
  assert_reconcile_supervisor_generation
  # Reconcile is always a supervisor descendant. On the first attempt it
  # validates the still-open inherited fd 9; after a supervisor restart it
  # validates the freshly reacquired canonical lock before touching evidence.
  acquire_canary_production_lock

  if [[ -f "$RECEIPT_FILE" && ! -L "$RECEIPT_FILE" ]]; then
    verify_existing_receipt
    cleanup_candidate
    wait_for_candidate_port_release
    verify_retained_control_bundle
    return 0
  elif [[ -e "$RECEIPT_FILE" || -L "$RECEIPT_FILE" ]]; then
    fail "reconcile receipt path is unsafe"
    return 1
  fi

  cleanup_candidate || cleanup_rc=1
  if [[ "$cleanup_rc" -eq 0 ]]; then
    wait_for_candidate_port_release || cleanup_rc=1
  fi
  if [[ "$cleanup_rc" -ne 0 ]]; then
    echo \
      "[ERROR] Reconcile cleanup or candidate port release is incomplete; refusing a terminal receipt" \
      >&2
    return 1
  fi

  resolve_active_unit || comparison_rc=1
  if [[ "$comparison_rc" -eq 0 ]]; then
    capture_snapshot "$AFTER_SNAPSHOT" || comparison_rc=1
  fi
  if [[ "$comparison_rc" -eq 0 ]]; then
    python3 -B "$CANARY_GUARD" verify-baseline \
      --snapshot "$AFTER_SNAPSHOT" || comparison_rc=1
  fi
  if [[ "$comparison_rc" -eq 0 && -f "$BEFORE_SNAPSHOT" ]]; then
    python3 -B "$CANARY_GUARD" compare \
      --before "$BEFORE_SNAPSHOT" \
      --after "$AFTER_SNAPSHOT" || comparison_rc=1
  elif [[ "$comparison_rc" -eq 0 ]]; then
    comparison_rc=1
  fi
  if [[ "$comparison_rc" -ne 0 ]]; then
    receipt_error="${CANARY_ERROR:+$CANARY_ERROR; }exact production before/after comparison unavailable or changed"
  fi

  if ! verify_retained_control_bundle; then
    echo \
      "[ERROR] Retained root-owned control evidence is unsafe; refusing a terminal receipt" \
      >&2
    return 1
  fi

  if checkpoint_marker_present controller_completed completed; then
    controller_completed=true
  fi
  if checkpoint_marker_present cleanup_verified completed; then
    controller_cleanup_completed=true
  fi
  if checkpoint_marker_present fault_observed completed; then
    fault_observed=true
  fi
  if checkpoint_marker_present expected_failure_verified completed; then
    fault_cleanup_completed=true
  fi

  if [[ "$comparison_rc" -eq 0 ]] \
    && [[ "${CANARY_SUPERVISOR_STOP_REQUESTED:-false}" != "true" ]] \
    && [[ "$CANARY_FAULT" == "" ]] \
    && [[ "$controller_completed" == "true" ]] \
    && [[ "$controller_cleanup_completed" == "true" ]] \
    && [[ "$fault_observed" == "false" ]] \
    && [[ "$fault_cleanup_completed" == "false" ]]; then
    outcome="passed"
    receipt_error=""
  elif [[ "$comparison_rc" -eq 0 ]] \
    && [[ "${CANARY_SUPERVISOR_STOP_REQUESTED:-false}" != "true" ]] \
    && [[ "$CANARY_FAULT" == "after_candidate_start" ]] \
    && [[ "$controller_completed" == "false" ]] \
    && [[ "$controller_cleanup_completed" == "false" ]] \
    && [[ "$fault_observed" == "true" ]] \
    && [[ "$fault_cleanup_completed" == "true" ]]; then
    outcome="expected_failure_verified"
    receipt_error="expected fault injection: after_candidate_start"
  else
    outcome="failed"
    if [[ "${CANARY_SUPERVISOR_STOP_REQUESTED:-false}" == "true" ]]; then
      receipt_error="${receipt_error:+$receipt_error; }durable supervisor stop was requested"
    elif [[ "$comparison_rc" -eq 0 ]]; then
      receipt_error="${receipt_error:+$receipt_error; }durable controller outcome markers are missing, duplicate, or contradictory"
    fi
    receipt_error="${receipt_error:+$receipt_error; }controller rc=${CANARY_CONTROLLER_RC:-unknown}"
  fi

  ensure_checkpoint_marker supervisor_reconciled completed \
    "supervisor held the production lock, quiesced all child units, and freshly reconciled outcome=$outcome"

  if ! write_terminal_receipt "$outcome" "$receipt_error"; then
    if [[ "$outcome" == "failed" ]]; then
      return 1
    fi
    receipt_error="supervisor rejected incomplete ${outcome} evidence, including candidate/writer generation binding"
    if ! write_terminal_receipt failed "$receipt_error"; then
      return 1
    fi
  fi
  verify_existing_receipt
}

main() {
  case "$CANARY_MODE" in
    launch)
      launch_canary
      ;;
    supervisor)
      trap \
        'CANARY_SUPERVISOR_STOP_REQUESTED=true; CANARY_ERROR="supervisor signal HUP"' \
        HUP
      trap \
        'CANARY_SUPERVISOR_STOP_REQUESTED=true; CANARY_ERROR="supervisor signal INT"' \
        INT
      trap \
        'CANARY_SUPERVISOR_STOP_REQUESTED=true; CANARY_ERROR="supervisor signal TERM"' \
        TERM
      run_canary_supervisor
      ;;
    controller)
      trap finalize_canary EXIT
      trap 'CANARY_ERROR="${CANARY_ERROR:-unexpected command failure}"' ERR
      trap 'CANARY_ERROR="signal HUP"; exit 129' HUP
      trap 'CANARY_ERROR="signal INT"; exit 130' INT
      trap 'CANARY_ERROR="signal TERM"; exit 143' TERM
      run_canary_controller
      ;;
    build)
      initialize_paths
      assert_supervisor_generation
      validate_static_contract
      build_candidate_runtime
      ;;
    runtime)
      shift
      run_candidate_runtime "$@"
      ;;
    reconcile)
      trap - EXIT ERR HUP INT TERM
      reconcile_canary_controller
      ;;
    *)
      fail "unknown feature candidate canary mode: $CANARY_MODE"
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
