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
RECOVERY_86CE_APPLY_CONFIRMATION="ABORT 2026-07-30-86ce PRE-SWITCH"
RECOVERY_29DF_APPLY_CONFIRMATION="QUARANTINE 29df5e6e667351f09305783932b34e5438d6a9d5 RESIDUE AND ABORT PRE-SWITCH"

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

# Resolve the apply contract from the plan itself.  Schema v2 keeps the
# historical 86ce phrase unchanged.  Schema v3 accepts the distinct 29df
# quarantine phrase and derives its reviewed dry-run authorization only from
# the already content-addressed control bundle.
mapfile -t recovery_contract < <(
  python3 - "$RECOVERY_PLAN" "$RECOVERY_BUNDLE_ROOT" \
    "$RECOVERY_IMPLEMENTATION_COMMIT" "$RECOVERY_PLAN_SHA256" \
    "$RECOVERY_MODE" <<'PY'
import hashlib
import json
from pathlib import Path
import stat
import sys


def reject_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SystemExit(f"duplicate JSON key: {key}")
        result[key] = value
    return result


plan_path = Path(sys.argv[1])
bundle_root = Path(sys.argv[2]).resolve(strict=True)
implementation_commit = sys.argv[3]
plan_sha256 = sys.argv[4]
mode = sys.argv[5]
plan = json.loads(
    plan_path.read_text(encoding="utf-8"),
    object_pairs_hook=reject_duplicates,
)
schema_version = plan.get("schemaVersion")
incident_id = plan.get("incidentId")
confirmation = ""
authorization_path = ""
authorization_sha256 = ""
if (
    schema_version == 2
    and incident_id == "2026-07-30-86ce-pre-switch-db-evidence"
):
    confirmation = "ABORT 2026-07-30-86ce PRE-SWITCH"
elif (
    schema_version == 3
    and incident_id == "2026-08-03-29df-pre-switch-candidate-residue"
):
    confirmation = (
        "QUARANTINE 29df5e6e667351f09305783932b34e5438d6a9d5 "
        "RESIDUE AND ABORT PRE-SWITCH"
    )
    if mode == "apply":
        manifest_path = bundle_root / "recovery-control-manifest.json"
        authorization = bundle_root / "reviewed-dry-run-authorization.json"
        for path, label in (
            (manifest_path, "control manifest"),
            (authorization, "reviewed dry-run authorization"),
        ):
            if path.is_symlink():
                raise SystemExit(f"{label} must not be a symlink")
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise SystemExit(f"{label} must be one regular file")
            if path.resolve(strict=True).parent != bundle_root:
                raise SystemExit(f"{label} escaped the immutable bundle")
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicates,
        )
        files = manifest.get("files")
        relative = "reviewed-dry-run-authorization.json"
        if (
            manifest.get("schemaVersion") != 1
            or manifest.get("commit") != implementation_commit
            or manifest.get("planSha256") != plan_sha256
            or not isinstance(files, dict)
            or not isinstance(files.get(relative), str)
        ):
            raise SystemExit("immutable control manifest authorization binding changed")
        raw = authorization.read_bytes()
        authorization_sha256 = hashlib.sha256(raw).hexdigest()
        if authorization_sha256 != files[relative]:
            raise SystemExit("reviewed dry-run authorization digest changed")
        authorization_path = str(authorization)
elif mode == "apply":
    raise SystemExit("unsupported apply recovery contract")

print(schema_version)
print(incident_id)
print(confirmation)
print(authorization_path)
print(authorization_sha256)
PY
)
if [[ "${#recovery_contract[@]}" -ne 5 ]]; then
  fail "recovery plan did not resolve to one supported controller contract"
fi
RECOVERY_PLAN_SCHEMA="${recovery_contract[0]}"
RECOVERY_INCIDENT_ID="${recovery_contract[1]}"
RECOVERY_EXPECTED_CONFIRMATION="${recovery_contract[2]}"
RECOVERY_DRY_RUN_AUTHORIZATION_PATH="${recovery_contract[3]}"
RECOVERY_DRY_RUN_AUTHORIZATION_SHA256="${recovery_contract[4]}"
if [[ "$RECOVERY_MODE" == "apply" ]] \
  && [[ "$RECOVERY_CONFIRMATION" != "$RECOVERY_EXPECTED_CONFIRMATION" ]]; then
  fail "apply mode requires the incident-specific confirmation phrase"
fi
if [[ "$RECOVERY_PLAN_SCHEMA" == "2" ]] \
  && [[ "$RECOVERY_EXPECTED_CONFIRMATION" != "$RECOVERY_86CE_APPLY_CONFIRMATION" ]]; then
  fail "schema v2 controller confirmation contract changed"
fi
if [[ "$RECOVERY_PLAN_SCHEMA" == "3" ]] \
  && [[ "$RECOVERY_EXPECTED_CONFIRMATION" != "$RECOVERY_29DF_APPLY_CONFIRMATION" ]]; then
  fail "schema v3 controller confirmation contract changed"
fi

# shellcheck disable=SC1090
source "$LOCK_LIBRARY"
jato_acquire_production_mutation_lock

if [[ "${DEPLOY_LOCK_HELD:-}" != "1" ]] \
  || [[ "${DEPLOY_LOCK_FD:-}" != "9" ]] \
  || [[ ! "${DEPLOY_LOCK_HOLDER_PID:-}" =~ ^[1-9][0-9]*$ ]]; then
  fail "canonical production lock contract was not established"
fi

recovery_arguments=(
  --plan "$RECOVERY_PLAN"
  --expected-plan-sha256 "$RECOVERY_PLAN_SHA256"
  --bundle-root "$RECOVERY_BUNDLE_ROOT"
  --implementation-commit "$RECOVERY_IMPLEMENTATION_COMMIT"
  --lock-path "$JATO_PRODUCTION_DEPLOY_LOCK_PATH"
  --lock-holder-pid "$DEPLOY_LOCK_HOLDER_PID"
  --mode "$RECOVERY_MODE"
)
if [[ "$RECOVERY_MODE" == "apply" && "$RECOVERY_PLAN_SCHEMA" == "3" ]]; then
  recovery_arguments+=(
    --dry-run-authorization "$RECOVERY_DRY_RUN_AUTHORIZATION_PATH"
    --dry-run-authorization-sha256 "$RECOVERY_DRY_RUN_AUTHORIZATION_SHA256"
  )
fi

sudo -n env \
  "PYTHONPATH=$RECOVERY_BUNDLE_ROOT/03_Scripts/deploy" \
  python3 -B "$RECOVERY_HELPER" "${recovery_arguments[@]}"
