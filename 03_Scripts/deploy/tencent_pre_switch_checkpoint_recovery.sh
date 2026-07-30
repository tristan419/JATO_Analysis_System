#!/usr/bin/env bash
set -Eeuo pipefail

# Main-only, approval-gated controller for one reviewed pre-switch checkpoint.
# It never starts a Candidate, changes Nginx, restarts services, or runs an
# Alembic migration.  The canonical production lock is held for both dry-run
# inspection and the append-only settlement.

RECOVERY_MODE="${RECOVERY_MODE:-dry-run}"
RECOVERY_PLAN_SHA256="${RECOVERY_PLAN_SHA256:-}"
RECOVERY_IMPLEMENTATION_COMMIT="${RECOVERY_IMPLEMENTATION_COMMIT:-}"
RECOVERY_BUNDLE_ROOT="${RECOVERY_BUNDLE_ROOT:-}"
RECOVERY_CONFIRMATION="${RECOVERY_CONFIRMATION:-}"
RECOVERY_APPLY_CONFIRMATION="ABORT 2026-07-30-ce5 PRE-SWITCH"

fail() {
  echo "[ERROR] $*" >&2
  exit 1
}

if [[ "$RECOVERY_MODE" != "dry-run" && "$RECOVERY_MODE" != "apply" ]]; then
  fail "RECOVERY_MODE must be dry-run or apply"
fi
if [[ ! "$RECOVERY_PLAN_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  fail "RECOVERY_PLAN_SHA256 must be a lowercase SHA-256"
fi
if [[ ! "$RECOVERY_IMPLEMENTATION_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
  fail "RECOVERY_IMPLEMENTATION_COMMIT must be a full lowercase git SHA"
fi
if [[ "$RECOVERY_BUNDLE_ROOT" != /* ]] \
  || [[ -L "$RECOVERY_BUNDLE_ROOT" ]] \
  || [[ ! -d "$RECOVERY_BUNDLE_ROOT" ]]; then
  fail "RECOVERY_BUNDLE_ROOT must be an absolute real directory"
fi
if [[ "$RECOVERY_MODE" == "apply" ]] \
  && [[ "$RECOVERY_CONFIRMATION" != "$RECOVERY_APPLY_CONFIRMATION" ]]; then
  fail "apply mode requires the incident-specific confirmation phrase"
fi
if [[ "$RECOVERY_MODE" == "dry-run" && -n "$RECOVERY_CONFIRMATION" ]]; then
  fail "dry-run mode must not carry an apply confirmation"
fi

LOCK_LIBRARY="$RECOVERY_BUNDLE_ROOT/03_Scripts/deploy/lib/production_mutation_lock.sh"
RECOVERY_HELPER="$RECOVERY_BUNDLE_ROOT/03_Scripts/deploy/pre_switch_checkpoint_recovery.py"
CHECKPOINT_HELPER="$RECOVERY_BUNDLE_ROOT/03_Scripts/deploy/release_checkpoint.py"
PLAN_ROOT="$RECOVERY_BUNDLE_ROOT/.github/recovery-plans"
for required_file in "$LOCK_LIBRARY" "$RECOVERY_HELPER" "$CHECKPOINT_HELPER"; do
  if [[ ! -f "$required_file" || -L "$required_file" ]]; then
    fail "recovery bundle file is missing or unsafe: $required_file"
  fi
done
if [[ ! -d "$PLAN_ROOT" || -L "$PLAN_ROOT" ]]; then
  fail "recovery plan directory is missing or unsafe"
fi

mapfile -t matching_plans < <(
  find "$PLAN_ROOT" -maxdepth 1 -type f -name '*.json' -print0 \
    | xargs -0 -r sha256sum \
    | awk -v wanted="$RECOVERY_PLAN_SHA256" '$1 == wanted {print $2}'
)
if [[ "${#matching_plans[@]}" -ne 1 ]]; then
  fail "recovery bundle must contain exactly one plan matching the approved SHA-256"
fi
RECOVERY_PLAN="${matching_plans[0]}"
if [[ "$RECOVERY_PLAN" != "$PLAN_ROOT/"* ]] || [[ -L "$RECOVERY_PLAN" ]]; then
  fail "resolved recovery plan path is unsafe"
fi

# shellcheck disable=SC1090
source "$LOCK_LIBRARY"
jato_acquire_production_mutation_lock

if [[ "${DEPLOY_LOCK_HELD:-}" != "1" ]] \
  || [[ "${DEPLOY_LOCK_FD:-}" != "9" ]] \
  || [[ ! "${DEPLOY_LOCK_HOLDER_PID:-}" =~ ^[1-9][0-9]*$ ]]; then
  fail "canonical production lock contract was not established"
fi

sudo -n env \
  "PYTHONPATH=$RECOVERY_BUNDLE_ROOT/03_Scripts/deploy" \
  python3 -B "$RECOVERY_HELPER" \
    --plan "$RECOVERY_PLAN" \
    --expected-plan-sha256 "$RECOVERY_PLAN_SHA256" \
    --bundle-root "$RECOVERY_BUNDLE_ROOT" \
    --implementation-commit "$RECOVERY_IMPLEMENTATION_COMMIT" \
    --lock-path "$JATO_PRODUCTION_DEPLOY_LOCK_PATH" \
    --lock-holder-pid "$DEPLOY_LOCK_HOLDER_PID" \
    --mode "$RECOVERY_MODE"
