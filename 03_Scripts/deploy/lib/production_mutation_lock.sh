#!/usr/bin/env bash

# Shared lock contract for host-side production mutations.
#
# The immutable release verifier owns fd 9 while it prepares a release.  A
# privileged child may reuse that lock only when it can prove that the declared
# holder is itself or an ancestor and that the holder's fd 9 still references
# the exact, non-symlink production lock file.  An arbitrary environment flag
# is therefore insufficient to bypass flock.

JATO_PRODUCTION_DEPLOY_LOCK_FD=9
JATO_BLUEGREEN_SWITCH_UNIT="${JATO_BLUEGREEN_SWITCH_UNIT:-jato-bluegreen-production.service}"
JATO_PRODUCTION_DEPLOY_LOCK_WAIT="${JATO_PRODUCTION_DEPLOY_LOCK_WAIT:-300}"

jato_fail_lock_contract() {
  echo "[ERROR] $*" >&2
  return 1
}

jato_deploy_owner_home() {
  local owner_home=""
  if [[ -n "${DEPLOY_STATE_DIR:-}" ]]; then
    dirname "$(dirname "$DEPLOY_STATE_DIR")"
    return 0
  fi
  if [[ "$(id -u)" -eq 0 && -n "${SUDO_USER:-}" && "$SUDO_USER" != "root" ]]; then
    owner_home="$(getent passwd "$SUDO_USER" 2>/dev/null | awk -F: 'NR == 1 {print $6}')"
    if [[ -z "$owner_home" || "$owner_home" != /* ]]; then
      jato_fail_lock_contract "cannot resolve the production deploy user's home"
      return 1
    fi
    printf '%s\n' "$owner_home"
    return 0
  fi
  if [[ -z "${HOME:-}" || "$HOME" != /* ]]; then
    jato_fail_lock_contract "HOME must be an absolute path for the production deploy lock"
    return 1
  fi
  printf '%s\n' "$HOME"
}

jato_resolve_production_deploy_lock_path() {
  local owner_home=""
  if [[ -n "${JATO_PRODUCTION_DEPLOY_LOCK_PATH:-}" ]]; then
    if [[ "$JATO_PRODUCTION_DEPLOY_LOCK_PATH" != /* ]]; then
      jato_fail_lock_contract "JATO_PRODUCTION_DEPLOY_LOCK_PATH must be absolute"
      return 1
    fi
    printf '%s\n' "$JATO_PRODUCTION_DEPLOY_LOCK_PATH"
    return 0
  fi
  if [[ -n "${DEPLOY_STATE_DIR:-}" ]]; then
    if [[ "$DEPLOY_STATE_DIR" != /* ]]; then
      jato_fail_lock_contract "DEPLOY_STATE_DIR must be absolute"
      return 1
    fi
    printf '%s/production-deploy.lock\n' "${DEPLOY_STATE_DIR%/}"
    return 0
  fi
  owner_home="$(jato_deploy_owner_home)" || return 1
  printf '%s/.local/state/jato-production-release/production-deploy.lock\n' \
    "${owner_home%/}"
}

jato_assert_safe_lock_parent_components() {
  local lock_parent="$1"
  local require_all="${2:-false}"
  python3 -B - "$lock_parent" "$require_all" <<'PY'
import os
from pathlib import Path
import stat
import sys

path = Path(sys.argv[1])
require_all = sys.argv[2] == "true"
if not path.is_absolute():
    raise SystemExit("[ERROR] production deploy lock parent must be absolute")
cursor = Path(path.anchor)
for part in path.parts[1:]:
    cursor /= part
    try:
        mode = os.lstat(cursor).st_mode
    except FileNotFoundError:
        if require_all:
            raise SystemExit(
                f"[ERROR] production deploy lock ancestor disappeared: {cursor}"
            )
        continue
    if stat.S_ISLNK(mode):
        raise SystemExit(
            f"[ERROR] production deploy lock ancestor must not be a symlink: {cursor}"
        )
    if not stat.S_ISDIR(mode):
        raise SystemExit(
            f"[ERROR] production deploy lock ancestor must be a directory: {cursor}"
        )
PY
}

jato_validate_lock_path() {
  local lock_path="$1"
  local lock_parent=""
  lock_parent="$(dirname "$lock_path")"
  jato_assert_safe_lock_parent_components "$lock_parent" false || return 1
  mkdir -p "$lock_parent"
  jato_assert_safe_lock_parent_components "$lock_parent" true || return 1
  chmod 700 "$lock_parent"
  if [[ -L "$lock_path" ]] \
    || [[ -e "$lock_path" && ! -f "$lock_path" ]]; then
    jato_fail_lock_contract "production deploy lock is unsafe: $lock_path"
    return 1
  fi
}

jato_pid_is_self_or_ancestor() {
  local wanted_pid="$1"
  local current_pid="$$"
  local parent_pid=""
  while [[ "$current_pid" =~ ^[1-9][0-9]*$ ]]; do
    if [[ "$current_pid" == "$wanted_pid" ]]; then
      return 0
    fi
    if [[ ! -r "/proc/$current_pid/stat" ]]; then
      return 1
    fi
    parent_pid="$(
      python3 -B - "$current_pid" <<'PY'
from pathlib import Path
import sys

payload = Path(f"/proc/{sys.argv[1]}/stat").read_text(encoding="utf-8")
closing = payload.rfind(")")
if closing < 0:
    raise SystemExit(1)
fields = payload[closing + 2 :].split()
if len(fields) < 2:
    raise SystemExit(1)
print(fields[1])
PY
    )" || return 1
    if [[ "$parent_pid" == "$current_pid" || "$parent_pid" == "0" ]]; then
      return 1
    fi
    current_pid="$parent_pid"
  done
  return 1
}

jato_validate_inherited_production_lock() {
  local lock_path="$1"
  local holder_pid="${DEPLOY_LOCK_HOLDER_PID:-}"
  local inherited_fd="${DEPLOY_LOCK_FD:-}"
  local holder_fd_path=""
  local holder_target=""
  local expected_target=""

  if [[ "${DEPLOY_LOCK_HELD:-}" != "1" ]] \
    || [[ "$inherited_fd" != "$JATO_PRODUCTION_DEPLOY_LOCK_FD" ]] \
    || [[ ! "$holder_pid" =~ ^[1-9][0-9]*$ ]]; then
    jato_fail_lock_contract "nested production mutation lock claim is malformed"
    return 1
  fi
  if [[ ! -d /proc ]] || ! jato_pid_is_self_or_ancestor "$holder_pid"; then
    jato_fail_lock_contract "declared production lock holder is not a live ancestor"
    return 1
  fi
  holder_fd_path="/proc/$holder_pid/fd/$JATO_PRODUCTION_DEPLOY_LOCK_FD"
  if [[ ! -e "$holder_fd_path" ]]; then
    jato_fail_lock_contract "declared production lock fd is not open"
    return 1
  fi
  holder_target="$(readlink "$holder_fd_path")" || return 1
  expected_target="$(realpath -m "$lock_path")" || return 1
  if [[ "$holder_target" != "$expected_target" ]]; then
    jato_fail_lock_contract "declared production lock fd references a different file"
    return 1
  fi

  exec 8>"$lock_path"
  if flock -n 8; then
    flock -u 8 || true
    exec 8>&-
    jato_fail_lock_contract "declared production lock fd does not hold flock"
    return 1
  fi
  exec 8>&-
}

jato_assert_no_active_bluegreen_switch() {
  local active_state=""
  local load_state=""
  local sub_state=""
  if ! command -v systemctl >/dev/null 2>&1; then
    jato_fail_lock_contract "systemctl is required to fence blue/green mutation"
    return 1
  fi
  if ! load_state="$(
    systemctl show "$JATO_BLUEGREEN_SWITCH_UNIT" -p LoadState --value 2>/dev/null
  )" \
    || ! active_state="$(
      systemctl show "$JATO_BLUEGREEN_SWITCH_UNIT" -p ActiveState --value 2>/dev/null
    )" \
    || ! sub_state="$(
      systemctl show "$JATO_BLUEGREEN_SWITCH_UNIT" -p SubState --value 2>/dev/null
    )"; then
    jato_fail_lock_contract "cannot inspect the blue/green production switch unit"
    return 1
  fi
  if [[ "$load_state" == "not-found" ]]; then
    if [[ -n "$active_state" && "$active_state" != "inactive" ]]; then
      jato_fail_lock_contract "unloaded blue/green switch unit reported state=$active_state"
      return 1
    fi
    return 0
  fi
  if [[ "$load_state" != "loaded" ]] \
    || [[ "$active_state" != "inactive" && "$active_state" != "failed" ]]; then
    jato_fail_lock_contract \
      "blue/green production switch is not quiescent: load=$load_state active=$active_state sub=$sub_state"
    return 1
  fi
}

jato_acquire_production_mutation_lock() {
  local lock_path=""
  if ! command -v flock >/dev/null 2>&1 \
    || ! command -v realpath >/dev/null 2>&1; then
    jato_fail_lock_contract "flock and realpath are required for production mutation"
    return 1
  fi
  if [[ ! "$JATO_PRODUCTION_DEPLOY_LOCK_WAIT" =~ ^[0-9]+$ ]]; then
    jato_fail_lock_contract "JATO_PRODUCTION_DEPLOY_LOCK_WAIT must be an integer"
    return 1
  fi
  lock_path="$(jato_resolve_production_deploy_lock_path)" || return 1
  jato_validate_lock_path "$lock_path" || return 1

  if [[ -n "${DEPLOY_LOCK_HELD:-}" \
    || -n "${DEPLOY_LOCK_HOLDER_PID:-}" \
    || -n "${DEPLOY_LOCK_FD:-}" ]]; then
    jato_validate_inherited_production_lock "$lock_path" || return 1
  else
    exec 9>"$lock_path"
    if ! flock -w "$JATO_PRODUCTION_DEPLOY_LOCK_WAIT" 9; then
      jato_fail_lock_contract "another production mutation holds the server-wide deploy lock"
      return 1
    fi
    DEPLOY_LOCK_HELD=1
    DEPLOY_LOCK_HOLDER_PID="$$"
    DEPLOY_LOCK_FD="$JATO_PRODUCTION_DEPLOY_LOCK_FD"
    export DEPLOY_LOCK_HELD DEPLOY_LOCK_HOLDER_PID DEPLOY_LOCK_FD
  fi
  JATO_PRODUCTION_DEPLOY_LOCK_PATH="$lock_path"
  export JATO_PRODUCTION_DEPLOY_LOCK_PATH
  jato_assert_no_active_bluegreen_switch
}
