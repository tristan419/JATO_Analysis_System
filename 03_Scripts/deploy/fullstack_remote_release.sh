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
  RELEASE_ARCHIVE_VALIDATOR_B64 \
  RELEASE_ARCHIVE_VALIDATOR_SHA256 \
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

DEPLOY_BLUEGREEN_MODE="${DEPLOY_BLUEGREEN_MODE:-prepare-candidate}"
case "$DEPLOY_BLUEGREEN_MODE" in
  prepare-candidate|approve-candidate-to-active|discard-candidate|release-candidate|restore-previous-active) ;;
  *)
    echo "[ERROR] DEPLOY_BLUEGREEN_MODE must prepare, approve, or clean one exact Candidate"
    exit 1
    ;;
esac

if [[ "$DEPLOY_BLUEGREEN_MODE" != "prepare-candidate" ]]; then
  for approval_name in \
    DEPLOY_APPROVAL_RUN_ID \
    DEPLOY_APPROVAL_RUN_ATTEMPT \
    DEPLOY_CANDIDATE_ATTESTATION_SHA256 \
    DEPLOY_CANDIDATE_SERVER_CHECKPOINT_PATH \
    DEPLOY_CANDIDATE_SERVER_CHECKPOINT_SHA256 \
    DEPLOY_CANDIDATE_SERVER_EVIDENCE_PATH \
    DEPLOY_CANDIDATE_SERVER_EVIDENCE_SHA256
  do
    if [ -z "${!approval_name:-}" ]; then
      echo "[ERROR] $approval_name is required for Candidate approval"
      exit 1
    fi
  done
  if [[ ! "$DEPLOY_APPROVAL_RUN_ID" =~ ^[1-9][0-9]*$ ]] \
    || [[ ! "$DEPLOY_APPROVAL_RUN_ATTEMPT" =~ ^[1-9][0-9]*$ ]]; then
    echo "[ERROR] Candidate approval run identity must use positive integers"
    exit 1
  fi
  if [[ ! "$DEPLOY_CANDIDATE_ATTESTATION_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[ERROR] Candidate approval attestation SHA-256 is invalid"
    exit 1
  fi
  if [[ ! "$DEPLOY_CANDIDATE_SERVER_CHECKPOINT_SHA256" =~ ^[0-9a-f]{64}$ ]] \
    || [[ ! "$DEPLOY_CANDIDATE_SERVER_EVIDENCE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[ERROR] Candidate server handoff SHA-256 is invalid"
    exit 1
  fi
fi

if [[ ! "$DEPLOY_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "[ERROR] DEPLOY_COMMIT_SHA must be a full lowercase git SHA"
  exit 1
fi
if [[ ! "$DEPLOY_ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "[ERROR] DEPLOY_ARCHIVE_SHA256 must be a lowercase SHA-256"
  exit 1
fi
if [[ ! "$RELEASE_ARCHIVE_VALIDATOR_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "[ERROR] RELEASE_ARCHIVE_VALIDATOR_SHA256 must be a lowercase SHA-256"
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

for required_command in base64 flock realpath sha256sum stat tar python3 systemd-run; do
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
CHECKPOINT_EVIDENCE_FILE="${CHECKPOINT_FILE%.json}.evidence.json"

verify_attested_candidate_paths_and_evidence() {
  local actual_evidence_sha256=""

  if [[ "$DEPLOY_BLUEGREEN_MODE" == "prepare-candidate" ]]; then
    return 0
  fi
  if [[ "$DEPLOY_CANDIDATE_SERVER_CHECKPOINT_PATH" != "$CHECKPOINT_FILE" ]] \
    || [[ "$DEPLOY_CANDIDATE_SERVER_EVIDENCE_PATH" != "$CHECKPOINT_EVIDENCE_FILE" ]]; then
    echo "[ERROR] Attested Candidate server paths do not match the locked release state"
    return 1
  fi
  if [[ -L "$CHECKPOINT_FILE" || ! -f "$CHECKPOINT_FILE" ]] \
    || [[ -L "$CHECKPOINT_EVIDENCE_FILE" || ! -f "$CHECKPOINT_EVIDENCE_FILE" ]]; then
    echo "[ERROR] Attested Candidate server files are missing or unsafe"
    return 1
  fi
  actual_evidence_sha256="$(sha256sum "$CHECKPOINT_EVIDENCE_FILE" | awk '{print $1}')"
  if [[ "$actual_evidence_sha256" != "$DEPLOY_CANDIDATE_SERVER_EVIDENCE_SHA256" ]]; then
    echo "[ERROR] Candidate server evidence changed after the reviewed handoff"
    return 1
  fi
}

verify_attested_candidate_checkpoint_for_mode() {
  local actual_checkpoint_sha256=""
  local require_original_checkpoint=false

  case "$DEPLOY_BLUEGREEN_MODE:$CHECKPOINT_PHASE" in
    approve-candidate-to-active:candidate_ready) require_original_checkpoint=true ;;
    discard-candidate:candidate_ready) require_original_checkpoint=true ;;
  esac
  if [[ "${DEPLOY_CANDIDATE_HANDOFF_SOURCE:-}" == "canonical-server" ]]; then
    require_original_checkpoint=true
  fi
  if [[ "$require_original_checkpoint" != "true" ]]; then
    return 0
  fi
  actual_checkpoint_sha256="$(sha256sum "$CHECKPOINT_FILE" | awk '{print $1}')"
  if [[ "$actual_checkpoint_sha256" != "$DEPLOY_CANDIDATE_SERVER_CHECKPOINT_SHA256" ]]; then
    echo "[ERROR] Candidate server checkpoint changed after the reviewed handoff"
    return 1
  fi
}

verify_attested_candidate_paths_and_evidence
RELEASE_WORKTREE=""
TRUSTED_ARCHIVE_VALIDATOR_TEMP=""
SEALED_TRUST_ROOT="/var/lib/jato-sealed-inputs"
SEALED_ARCHIVE_ROOT="$SEALED_TRUST_ROOT/inputs"
SEALED_ARCHIVE_DIR="$SEALED_ARCHIVE_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256/${DEPLOY_RUN_ID}-${DEPLOY_RUN_ATTEMPT}"
SEALED_RELEASE_ARCHIVE="$SEALED_ARCHIVE_DIR/release.tar.gz"
SEALED_ARCHIVE_VALIDATOR="$SEALED_ARCHIVE_DIR/validate_release_archive.py"
ARCHIVE_VALIDATION_RECEIPT_ROOT="$SEALED_TRUST_ROOT/receipts"
ARCHIVE_VALIDATION_RECEIPT="$ARCHIVE_VALIDATION_RECEIPT_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256/${DEPLOY_RUN_ID}-${DEPLOY_RUN_ATTEMPT}.json"
PRODUCTION_EXTRACTION_RESERVE_BYTES=$((15 * 1024 * 1024 * 1024))
BLUEGREEN_HEADROOM_TARGET=""
PREBUILT_FRONTEND_DIR="$REPO_DIR/.release-staging/frontend_${DEPLOY_COMMIT_SHA}_${DEPLOY_ARCHIVE_SHA256}.staged"
RELEASE_REPLACEMENT_PATHS="03_Scripts 06_AppPlatform 07_ScrapingToolkit hermes"
DEPLOY_GID="$(id -g)"

remove_transient_release_paths() {
  local transient_path=""

  for transient_path in \
    "$RELEASE_WORKTREE" \
    "$TRUSTED_ARCHIVE_VALIDATOR_TEMP" \
    "$PREBUILT_FRONTEND_DIR"; do
    if [[ -z "$transient_path" || ! -e "$transient_path" ]]; then
      continue
    fi
    if ! rm -rf -- "$transient_path"; then
      echo "[WARN] Failed to remove transient deployment path: $transient_path" >&2
    fi
  done
  case "$SEALED_ARCHIVE_DIR" in
    /var/lib/jato-sealed-inputs/inputs/*/*/*)
      if sudo -n test -e "$SEALED_ARCHIVE_DIR" \
        || sudo -n test -L "$SEALED_ARCHIVE_DIR"; then
        if ! sudo -n rm -rf --one-file-system "$SEALED_ARCHIVE_DIR"; then
          echo \
            "[WARN] Failed to remove sealed release input: $SEALED_ARCHIVE_DIR" \
            >&2
        fi
      fi
      ;;
    *)
      echo "[WARN] Refusing unexpected sealed archive cleanup path" >&2
      ;;
  esac
  return 0
}

cleanup_release_staging() {
  remove_transient_release_paths
  # The verified content-addressed archive is intentionally retained.  A later
  # workflow checkpoint owns cleanup after www/intl parity reaches complete.
}
trap cleanup_release_staging EXIT

RELEASE_WORKTREE="$(mktemp -d "/tmp/JATO_deploy_work_${DEPLOY_COMMIT_SHA}.XXXXXX")"
TRUSTED_ARCHIVE_VALIDATOR_TEMP="$(
  mktemp "$DEPLOY_STATE_DIR/.archive-validator-${DEPLOY_COMMIT_SHA}.XXXXXX.py"
)"

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

ensure_sealed_trust_roots() {
  sudo -n python3 -B - \
    "$SEALED_TRUST_ROOT" "$SEALED_ARCHIVE_ROOT" \
    "$SEALED_ARCHIVE_ROOT/$DEPLOY_COMMIT_SHA" \
    "$SEALED_ARCHIVE_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256" \
    "$ARCHIVE_VALIDATION_RECEIPT_ROOT" \
    "$ARCHIVE_VALIDATION_RECEIPT_ROOT/$DEPLOY_COMMIT_SHA" \
    "$ARCHIVE_VALIDATION_RECEIPT_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256" \
    <<'PY'
from pathlib import Path
import os
import stat
import sys

anchor = Path("/var/lib")
paths = [Path(value) for value in sys.argv[1:]]
if paths[0] != Path("/var/lib/jato-sealed-inputs"):
    raise SystemExit("[ERROR] sealed release trust root is not reviewed")
for path in (anchor, *paths):
    if path != anchor:
        try:
            path.relative_to(anchor)
        except ValueError as exc:
            raise SystemExit(
                f"[ERROR] sealed release directory escaped /var/lib: {path}"
            ) from exc
        try:
            os.mkdir(path, 0o755)
        except FileExistsError:
            pass
        else:
            os.chown(path, 0, 0)
            os.chmod(path, 0o755)
    metadata = os.lstat(path)
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or mode & 0o022
    ):
        raise SystemExit(
            f"[ERROR] sealed release directory is mutable or unsafe: {path}"
        )
PY
}

select_bluegreen_release_headroom_target() {
  if [[ "$BLUEGREEN_RELEASES_ROOT" != "/opt/jato/releases" ]]; then
    echo "[ERROR] Blue/green release root is not the reviewed /opt path" >&2
    return 1
  fi
  BLUEGREEN_HEADROOM_TARGET="$(
    python3 -B - "$BLUEGREEN_RELEASES_ROOT" "$(id -u)" <<'PY'
from pathlib import Path
import os
import stat
import sys

target = Path(sys.argv[1])
deploy_uid = int(sys.argv[2])
if target != Path("/opt/jato/releases"):
    raise SystemExit("[ERROR] blue/green release headroom target is unreviewed")
deepest = None
for path in (Path("/opt"), Path("/opt/jato"), target):
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        break
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid not in {0, deploy_uid}
        or mode & 0o022
    ):
        raise SystemExit(
            f"[ERROR] blue/green headroom ancestor is mutable or unsafe: {path}"
        )
    if path == Path("/opt") and metadata.st_uid != 0:
        raise SystemExit("[ERROR] /opt is not root-owned")
    deepest = path
if deepest is None:
    raise SystemExit("[ERROR] no safe blue/green headroom ancestor exists")
print(deepest)
PY
  )" || return 1
  if [[ "$BLUEGREEN_HEADROOM_TARGET" != "/opt" \
    && "$BLUEGREEN_HEADROOM_TARGET" != "/opt/jato" \
    && "$BLUEGREEN_HEADROOM_TARGET" != "/opt/jato/releases" ]]; then
    echo "[ERROR] Blue/green headroom ancestor selection is invalid" >&2
    return 1
  fi
  export BLUEGREEN_HEADROOM_TARGET
}

seal_release_archive_input() {
  ensure_sealed_trust_roots
  if sudo -n test -e "$SEALED_ARCHIVE_DIR" \
    || sudo -n test -L "$SEALED_ARCHIVE_DIR"; then
    echo "[ERROR] Sealed archive run directory already exists" >&2
    return 1
  fi
  sudo -n install -d -m 0750 -o root -g "$DEPLOY_GID" \
    "$SEALED_ARCHIVE_DIR"
  sudo -n install -m 0440 -o root -g "$DEPLOY_GID" \
    "$RELEASE_ARCHIVE" "$SEALED_RELEASE_ARCHIVE"
  if [[ "$(stat -c '%u:%g:%a' "$SEALED_ARCHIVE_DIR")" != \
    "0:${DEPLOY_GID}:750" ]] \
    || [[ "$(stat -c '%u:%g:%a' "$SEALED_RELEASE_ARCHIVE")" != \
      "0:${DEPLOY_GID}:440" ]]; then
    echo "[ERROR] Sealed archive input lost its root-owned identity" >&2
    return 1
  fi
  verify_release_archive_identity "$SEALED_RELEASE_ARCHIVE"
}

verify_sealed_release_bundle() {
  sudo -n python3 -B - \
    "$SEALED_TRUST_ROOT" "$SEALED_ARCHIVE_DIR" \
    "$SEALED_RELEASE_ARCHIVE" "$SEALED_ARCHIVE_VALIDATOR" \
    "$DEPLOY_ARCHIVE_SHA256" "$DEPLOY_ARCHIVE_BYTES" \
    "$RELEASE_ARCHIVE_VALIDATOR_SHA256" "$DEPLOY_GID" \
    "$DEPLOY_COMMIT_SHA" "$DEPLOY_RUN_ID" "$DEPLOY_RUN_ATTEMPT" <<'PY'
from pathlib import Path
import hashlib
import os
import stat
import sys

(
    trust_root_name,
    run_dir_name,
    archive_name,
    helper_name,
    archive_sha256,
    archive_bytes,
    helper_sha256,
    deploy_gid,
    commit,
    run_id,
    run_attempt,
) = sys.argv[1:]
trust_root = Path(trust_root_name)
run_dir = Path(run_dir_name)
archive = Path(archive_name)
helper = Path(helper_name)
expected_run_dir = (
    trust_root
    / "inputs"
    / commit
    / archive_sha256
    / f"{run_id}-{run_attempt}"
)
if (
    trust_root != Path("/var/lib/jato-sealed-inputs")
    or run_dir != expected_run_dir
    or archive.parent != run_dir
    or helper.parent != run_dir
):
    raise SystemExit("[ERROR] sealed release bundle path is not canonical")

chain = []
cursor = run_dir
while True:
    chain.append(cursor)
    if cursor == Path("/var/lib"):
        break
    if cursor.parent == cursor:
        raise SystemExit("[ERROR] sealed release bundle escaped /var/lib")
    cursor = cursor.parent
for path in reversed(chain):
    metadata = os.lstat(path)
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or mode & 0o022
    ):
        raise SystemExit(
            f"[ERROR] sealed release directory is mutable or unsafe: {path}"
        )
run_metadata = os.lstat(run_dir)
if (
    run_metadata.st_gid != int(deploy_gid)
    or stat.S_IMODE(run_metadata.st_mode) != 0o750
):
    raise SystemExit("[ERROR] sealed release run directory identity changed")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


expected = (
    (archive, 0o440, archive_sha256, int(archive_bytes)),
    (helper, 0o550, helper_sha256, None),
)
for path, expected_mode, expected_sha, expected_bytes in expected:
    metadata = os.lstat(path)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != int(deploy_gid)
        or stat.S_IMODE(metadata.st_mode) != expected_mode
        or (expected_bytes is not None and metadata.st_size != expected_bytes)
        or sha256_file(path) != expected_sha
    ):
        raise SystemExit(
            f"[ERROR] sealed release file identity changed: {path}"
        )
PY
}

verify_archive_validation_receipt() {
  sudo -n python3 -B - \
    "$ARCHIVE_VALIDATION_RECEIPT" "$SEALED_TRUST_ROOT" \
    "$SEALED_ARCHIVE_DIR" "$SEALED_RELEASE_ARCHIVE" \
    "$SEALED_ARCHIVE_VALIDATOR" "$RELEASE_WORKTREE" \
    "$BLUEGREEN_HEADROOM_TARGET" "$DEPLOY_COMMIT_SHA" \
    "$DEPLOY_ARCHIVE_SHA256" "$DEPLOY_ARCHIVE_BYTES" \
    "$RELEASE_ARCHIVE_VALIDATOR_SHA256" "$DEPLOY_RUN_ID" \
    "$DEPLOY_RUN_ATTEMPT" "$DEPLOY_GID" \
    "$PRODUCTION_EXTRACTION_RESERVE_BYTES" <<'PY'
from pathlib import Path
import hashlib
import json
import os
import stat
import sys

(
    receipt_name,
    trust_root_name,
    run_dir_name,
    archive_name,
    helper_name,
    worktree_name,
    release_headroom_name,
    commit,
    archive_sha256,
    archive_bytes,
    helper_sha256,
    run_id,
    run_attempt,
    deploy_gid,
    reserve_bytes,
) = sys.argv[1:]
receipt_path = Path(receipt_name)
trust_root = Path(trust_root_name)
run_dir = Path(run_dir_name)
archive_path = Path(archive_name)
helper_path = Path(helper_name)
expected_targets = {
    str(Path(worktree_name)),
    str(Path(release_headroom_name)),
}
expected_receipt = (
    trust_root
    / "receipts"
    / commit
    / archive_sha256
    / f"{run_id}-{run_attempt}.json"
)
expected_run_dir = (
    trust_root
    / "inputs"
    / commit
    / archive_sha256
    / f"{run_id}-{run_attempt}"
)
if receipt_path != expected_receipt or run_dir != expected_run_dir:
    raise SystemExit("[ERROR] archive validation attempt path is not canonical")
metadata = os.lstat(receipt_path)
if (
    not stat.S_ISREG(metadata.st_mode)
    or stat.S_ISLNK(metadata.st_mode)
    or metadata.st_uid != 0
    or metadata.st_gid != 0
    or stat.S_IMODE(metadata.st_mode) != 0o444
):
    raise SystemExit("[ERROR] archive validation receipt is mutable or unsafe")

cursor = receipt_path.parent
while True:
    parent_metadata = os.lstat(cursor)
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or stat.S_ISLNK(parent_metadata.st_mode)
        or parent_metadata.st_uid != 0
        or stat.S_IMODE(parent_metadata.st_mode) & 0o022
    ):
        raise SystemExit(
            f"[ERROR] archive receipt parent is mutable or unsafe: {cursor}"
        )
    if cursor == Path("/var/lib"):
        break
    if cursor.parent == cursor:
        raise SystemExit("[ERROR] archive receipt escaped /var/lib")
    cursor = cursor.parent

payload = json.loads(receipt_path.read_text(encoding="utf-8"))
if (
    payload.get("schemaVersion") != 2
    or payload.get("status") != "validated"
    or payload.get("archiveSha256") != archive_sha256
    or payload.get("archiveBytes") != int(archive_bytes)
    or payload.get("validationAttempt")
    != {"runId": run_id, "runAttempt": int(run_attempt)}
):
    raise SystemExit("[ERROR] archive validation receipt identity is invalid")
trusted_controls = payload.get("trustedControls")
expected_control = "03_Scripts/deploy/validate_release_archive.py"
if (
    not isinstance(trusted_controls, dict)
    or trusted_controls != {expected_control: helper_sha256}
):
    raise SystemExit("[ERROR] archive receipt lacks exact helper provenance")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


sealed = payload.get("sealedInput")
if (
    not isinstance(sealed, dict)
    or sealed.get("root") != str(trust_root)
    or sealed.get("anchor") != "/var/lib"
):
    raise SystemExit("[ERROR] archive receipt lacks its root seal")
expected_file_evidence = {
    "archive": (
        archive_path,
        "0440",
        archive_sha256,
        int(archive_bytes),
    ),
    "helper": (
        helper_path,
        "0550",
        helper_sha256,
        None,
    ),
}
for label, (path, mode, digest, expected_bytes) in expected_file_evidence.items():
    evidence = sealed.get(label)
    current = os.lstat(path)
    if (
        not isinstance(evidence, dict)
        or evidence.get("path") != str(path)
        or evidence.get("device") != current.st_dev
        or evidence.get("inode") != current.st_ino
        or evidence.get("uid") != 0
        or evidence.get("gid") != int(deploy_gid)
        or evidence.get("mode") != mode
        or evidence.get("sha256") != digest
        or evidence.get("bytes") != current.st_size
        or (expected_bytes is not None and current.st_size != expected_bytes)
        or not stat.S_ISREG(current.st_mode)
        or stat.S_ISLNK(current.st_mode)
        or current.st_uid != 0
        or current.st_gid != int(deploy_gid)
        or f"{stat.S_IMODE(current.st_mode):04o}" != mode
        or sha256_file(path) != digest
    ):
        raise SystemExit(
            f"[ERROR] root-sealed {label} differs from its receipt"
        )
directories = sealed.get("directories")
if not isinstance(directories, list) or not directories:
    raise SystemExit("[ERROR] archive receipt lacks sealed parent identities")
observed_directory_paths = []
for evidence in directories:
    if not isinstance(evidence, dict) or not isinstance(evidence.get("path"), str):
        raise SystemExit("[ERROR] sealed directory receipt is malformed")
    path = Path(evidence["path"])
    current = os.lstat(path)
    actual = {
        "path": str(path),
        "device": current.st_dev,
        "inode": current.st_ino,
        "uid": current.st_uid,
        "gid": current.st_gid,
        "mode": f"{stat.S_IMODE(current.st_mode):04o}",
    }
    if (
        evidence != actual
        or not stat.S_ISDIR(current.st_mode)
        or stat.S_ISLNK(current.st_mode)
        or current.st_uid != 0
        or stat.S_IMODE(current.st_mode) & 0o022
    ):
        raise SystemExit(
            f"[ERROR] sealed parent identity changed after validation: {path}"
        )
    observed_directory_paths.append(path)
if (
    observed_directory_paths[0] != Path("/var/lib")
    or trust_root not in observed_directory_paths
    or observed_directory_paths[-1] != run_dir
):
    raise SystemExit("[ERROR] sealed parent chain is incomplete")

expanded = payload.get("expandedBytes")
checks = payload.get("headroomChecks")
if (
    isinstance(expanded, bool)
    or not isinstance(expanded, int)
    or expanded <= 0
    or not isinstance(checks, list)
    or not checks
):
    raise SystemExit("[ERROR] archive receipt lacks headroom checks")
observed_targets = set()
observed_devices = set()
for check in checks:
    targets = check.get("targets") if isinstance(check, dict) else None
    if not isinstance(targets, list) or not targets:
        raise SystemExit("[ERROR] archive headroom receipt is malformed")
    device = check.get("device")
    copies = check.get("materializationCopies")
    available = check.get("availableBytes")
    required = check.get("requiredBytes")
    reserve = check.get("reserveBytes")
    if (
        isinstance(device, bool)
        or not isinstance(device, int)
        or device in observed_devices
        or copies != len(targets)
        or reserve != int(reserve_bytes)
        or required != expanded * copies + reserve
        or isinstance(available, bool)
        or not isinstance(available, int)
        or available < required
        or check.get("target") != sorted(targets)[0]
    ):
        raise SystemExit("[ERROR] archive headroom receipt is inconsistent")
    for target_name in targets:
        target = Path(target_name)
        target_metadata = os.lstat(target)
        if (
            target_name not in expected_targets
            or not stat.S_ISDIR(target_metadata.st_mode)
            or stat.S_ISLNK(target_metadata.st_mode)
            or target_metadata.st_dev != device
        ):
            raise SystemExit("[ERROR] archive headroom target changed")
        observed_targets.add(target_name)
    observed_devices.add(device)
if observed_targets != expected_targets:
    raise SystemExit("[ERROR] archive headroom targets are incomplete")
PY
}

seal_release_archive_input

if ! printf '%s' "$RELEASE_ARCHIVE_VALIDATOR_B64" \
  | base64 --decode >"$TRUSTED_ARCHIVE_VALIDATOR_TEMP"; then
  echo "[ERROR] Trusted release archive validator payload is malformed"
  exit 1
fi
chmod 500 "$TRUSTED_ARCHIVE_VALIDATOR_TEMP"
if [[ "$(sha256sum "$TRUSTED_ARCHIVE_VALIDATOR_TEMP" | awk '{print $1}')" != \
  "$RELEASE_ARCHIVE_VALIDATOR_SHA256" ]]; then
  echo "[ERROR] Trusted release archive validator SHA-256 mismatch"
  exit 1
fi
sudo -n install -m 0550 -o root -g "$DEPLOY_GID" \
  "$TRUSTED_ARCHIVE_VALIDATOR_TEMP" "$SEALED_ARCHIVE_VALIDATOR"
if ! rm -f -- "$TRUSTED_ARCHIVE_VALIDATOR_TEMP"; then
  echo "[ERROR] Could not remove transient release archive validator" >&2
  exit 1
fi
TRUSTED_ARCHIVE_VALIDATOR_TEMP=""
verify_sealed_release_bundle
select_bluegreen_release_headroom_target
if sudo -n test -e "$ARCHIVE_VALIDATION_RECEIPT" \
  || sudo -n test -L "$ARCHIVE_VALIDATION_RECEIPT"; then
  echo "[ERROR] Archive validation receipt already exists for this attempt" >&2
  exit 1
fi

# A single root-executed copy validates the immutable archive, binds its own
# provenance, and atomically writes one attempt-scoped receipt containing both
# extraction targets.  There is no second pass that can overwrite evidence.
if ! sudo -n python3 -B "$SEALED_ARCHIVE_VALIDATOR" \
  --archive "$SEALED_RELEASE_ARCHIVE" \
  --expected-sha256 "$DEPLOY_ARCHIVE_SHA256" \
  --expected-bytes "$DEPLOY_ARCHIVE_BYTES" \
  --trusted-control \
    "03_Scripts/deploy/validate_release_archive.py=$SEALED_ARCHIVE_VALIDATOR" \
  --output "$ARCHIVE_VALIDATION_RECEIPT" \
  --validation-run-id "$DEPLOY_RUN_ID" \
  --validation-run-attempt "$DEPLOY_RUN_ATTEMPT" \
  --sealed-root "$SEALED_TRUST_ROOT" \
  --sealed-helper "$SEALED_ARCHIVE_VALIDATOR" \
  --expected-helper-sha256 "$RELEASE_ARCHIVE_VALIDATOR_SHA256" \
  --expected-sealed-group "$DEPLOY_GID" \
  --headroom-target "$RELEASE_WORKTREE" 1 \
    "$PRODUCTION_EXTRACTION_RESERVE_BYTES" \
  --headroom-target "$BLUEGREEN_HEADROOM_TARGET" 1 \
    "$PRODUCTION_EXTRACTION_RESERVE_BYTES" \
  >/dev/null; then
  echo "[ERROR] Production release archive validation failed" >&2
  exit 1
fi
sudo -n chmod 0444 "$ARCHIVE_VALIDATION_RECEIPT"
verify_sealed_release_bundle
verify_archive_validation_receipt

echo "[INFO] Extracting sealed production release archive"
verify_sealed_release_bundle
verify_archive_validation_receipt
tar --same-permissions --no-overwrite-dir \
  -xzf "$SEALED_RELEASE_ARCHIVE" -C "$RELEASE_WORKTREE"
verify_sealed_release_bundle
verify_archive_validation_receipt

required_release_files=(
  hermes/deploy_release.json
  hermes/frontend_release/frontend-release.json
  hermes/frontend_release/frontend-dist.tar.gz
  01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx
  03_Scripts/deploy/frontend_release_artifact.py
  03_Scripts/deploy/cleanup_toolkit_egg_info.py
  03_Scripts/deploy/fixed_active_preimage.py
  03_Scripts/deploy/release_checkpoint.py
  03_Scripts/deploy/release_evidence.py
  03_Scripts/deploy/prepare_backend_release.py
  03_Scripts/deploy/verify_backend_readiness.py
  03_Scripts/deploy/jato_quiescence_gate.py
  03_Scripts/deploy/jato_release_storage_guard.py
  03_Scripts/deploy/tencent_bluegreen_release.sh
  03_Scripts/deploy/validate_release_archive.py
  03_Scripts/deploy/verify_release_source_seal.py
  03_Scripts/deploy/lib/production_mutation_lock.sh
  03_Scripts/deploy/lib/release_paths.sh
  03_Scripts/deploy_fullstack_server.sh
  03_Scripts/ops/deploy_fullstack_server.sh
  03_Scripts/ops/backup_production_data.sh
  03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh
  03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
  03_Scripts/deploy/nginx/jato_candidate_preview.conf.example
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
CHECKPOINT_ACTION="none"
if [[ -e "$CHECKPOINT_FILE" ]]; then
  RESUME_JSON="$(python3 "$CHECKPOINT_HELPER" assert-resumable \
    --checkpoint "$CHECKPOINT_FILE" "${checkpoint_identity_args[@]}")"
  CHECKPOINT_DECISION="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["decision"])' <<< "$RESUME_JSON")"
  CHECKPOINT_ACTION="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("action", "none"))' <<< "$RESUME_JSON")"
  CHECKPOINT_STATE="$(python3 "$CHECKPOINT_HELPER" show --checkpoint "$CHECKPOINT_FILE")"
  read -r CHECKPOINT_PHASE CHECKPOINT_STATUS < <(
    python3 -c 'import json,sys; p=json.load(sys.stdin); print(p["phase"], p["status"])' <<< "$CHECKPOINT_STATE"
  )
fi
verify_attested_candidate_checkpoint_for_mode

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
if [[ "$CHECKPOINT_DECISION" == "candidate-cleanup-required" ]]; then
  case "$CHECKPOINT_PHASE:$DEPLOY_BLUEGREEN_MODE" in
    rollback_completed:discard-candidate)
      echo "[INFO] Rolled-back fixed Active is retained; continuing only to discard its isolated Candidate"
      ;;
    active_updated:release-candidate)
      echo "[INFO] Fixed Active is committed; continuing only to release its isolated Candidate"
      ;;
    rollback_completed:*)
      echo "[ERROR] This exact release restored the previous Active; explicit Candidate discard is required"
      exit 1
      ;;
    active_updated:*)
      echo "[ERROR] This exact release updated fixed Active; explicit Candidate release is required"
      exit 1
      ;;
    *)
      echo "[ERROR] Unsupported Candidate cleanup checkpoint phase: $CHECKPOINT_PHASE"
      exit 1
      ;;
  esac
fi
if [[ "$CHECKPOINT_DECISION" == "reconcile-required" ]]; then
  case "$DEPLOY_BLUEGREEN_MODE:$CHECKPOINT_ACTION" in
    approve-candidate-to-active:restore-previous-active|\
    approve-candidate-to-active:finalize-active-update|\
    approve-candidate-to-active:resume-rollback)
      echo "[WARN] Resuming exact fixed Active reconciliation: action=$CHECKPOINT_ACTION phase=$CHECKPOINT_PHASE"
      ;;
    *)
      echo "[ERROR] Interrupted release requires an exact supported reconciliation action before any other mode: ${CHECKPOINT_ACTION:-missing}"
      exit 1
      ;;
  esac
fi
if [[ "$CHECKPOINT_DECISION" == "already-candidate-prepare-aborted" ]]; then
  echo "[ERROR] This exact Candidate preparation was cleaned and sealed as aborted; create a new reviewed release"
  exit 1
fi
if [[ "$CHECKPOINT_DECISION" == "already-pre-switch-aborted" ]]; then
  echo "[ERROR] This exact release was abandoned before Candidate start; create a new reviewed release instead of replaying it"
  exit 1
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
if [[ "$DEPLOY_BLUEGREEN_MODE" != "prepare-candidate" ]]; then
  export \
    DEPLOY_APPROVAL_RUN_ID \
    DEPLOY_APPROVAL_RUN_ATTEMPT \
    DEPLOY_CANDIDATE_ATTESTATION_SHA256 \
    DEPLOY_CANDIDATE_SERVER_CHECKPOINT_PATH \
    DEPLOY_CANDIDATE_SERVER_CHECKPOINT_SHA256 \
    DEPLOY_CANDIDATE_SERVER_EVIDENCE_PATH \
    DEPLOY_CANDIDATE_SERVER_EVIDENCE_SHA256
fi
set +e
bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh" \
  "$DEPLOY_BLUEGREEN_MODE"
BLUEGREEN_RC=$?
set -e
exit "$BLUEGREEN_RC"
