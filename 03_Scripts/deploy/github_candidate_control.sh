#!/usr/bin/env bash
set -Eeuo pipefail

CONTROL_MODE="${1:-}"
case "$CONTROL_MODE" in
  approve-candidate-to-active|discard-candidate|discard-failed-candidate|release-candidate|restore-previous-active) ;;
  capture-canonical-cleanup) ;;
  *)
    echo "[ERROR] Unsupported Candidate control mode: $CONTROL_MODE" >&2
    exit 1
    ;;
esac

for required_name in \
  AUTH_MODE \
  GITHUB_REPOSITORY \
  GITHUB_RUN_ATTEMPT \
  GITHUB_RUN_ID \
  GITHUB_WORKFLOW \
  SSH_HOST \
  SSH_USER
do
  if [[ -z "${!required_name:-}" ]]; then
    echo "[ERROR] $required_name is required for Candidate control" >&2
    exit 1
  fi
done

if [[ "$CONTROL_MODE" == "capture-canonical-cleanup" ]]; then
  for canonical_name in \
    CANDIDATE_ARCHIVE_SHA256 \
    CANDIDATE_CANONICAL_BUNDLE_OUTPUT \
    CANDIDATE_CLEANUP_MODE \
    CANDIDATE_COMMIT_SHA \
    CANDIDATE_REQUESTED_ATTESTATION_SHA256 \
    CANDIDATE_RUN_ATTEMPT \
    CANDIDATE_RUN_ID
  do
    if [[ -z "${!canonical_name:-}" ]]; then
      echo "[ERROR] $canonical_name is required for canonical cleanup capture" >&2
      exit 1
    fi
  done
  [[ "$CANDIDATE_CLEANUP_MODE" == "discard-candidate" \
    || "$CANDIDATE_CLEANUP_MODE" == "release-candidate" ]]
  [[ "$CANDIDATE_RUN_ID" =~ ^[1-9][0-9]*$ ]]
  [[ "$CANDIDATE_RUN_ATTEMPT" =~ ^[1-9][0-9]*$ ]]
  [[ "$CANDIDATE_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]]
  [[ "$CANDIDATE_ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  [[ "$CANDIDATE_REQUESTED_ATTESTATION_SHA256" =~ ^[0-9a-f]{64}$ ]]
  if [[ -L "$CANDIDATE_CANONICAL_BUNDLE_OUTPUT" ]]; then
    echo "[ERROR] Canonical Candidate bundle output is unsafe" >&2
    exit 1
  fi
else
  : "${CANDIDATE_VERIFIED_ENV:?Candidate verified environment is required}"
  if [[ -L "$CANDIDATE_VERIFIED_ENV" ]] || [[ ! -f "$CANDIDATE_VERIFIED_ENV" ]]; then
    echo "[ERROR] Candidate verified environment is missing or unsafe" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$CANDIDATE_VERIFIED_ENV"
fi

if [[ "$CONTROL_MODE" != "capture-canonical-cleanup" ]]; then
  for candidate_name in \
    CANDIDATE_ARCHIVE_BYTES \
    CANDIDATE_ARCHIVE_SHA256 \
    CANDIDATE_COMMIT_SHA \
    CANDIDATE_FRONTEND_ARTIFACT_CHECKSUM \
    CANDIDATE_FRONTEND_ARTIFACT_IDENTITY \
    CANDIDATE_FRONTEND_ARTIFACT_NAME \
    CANDIDATE_FRONTEND_BUILD_ID \
    CANDIDATE_FRONTEND_NODE_VERSION \
    CANDIDATE_GITHUB_ARTIFACT_DIGEST \
    CANDIDATE_GITHUB_ARTIFACT_ID \
    CANDIDATE_HANDOFF_SOURCE \
    CANDIDATE_RELEASE_ID \
    CANDIDATE_REMOTE_ARCHIVE_PATH \
    CANDIDATE_RUN_ATTEMPT \
    CANDIDATE_RUN_ID \
    CANDIDATE_SERVER_CHECKPOINT_PATH \
    CANDIDATE_SERVER_CHECKPOINT_SHA256 \
    CANDIDATE_SERVER_EVIDENCE_PATH \
    CANDIDATE_SERVER_EVIDENCE_SHA256
  do
    if [[ -z "${!candidate_name:-}" ]]; then
      echo "[ERROR] $candidate_name is missing from the verified Candidate handoff" >&2
      exit 1
    fi
  done
  if [[ "$CONTROL_MODE" == "discard-failed-candidate" ]]; then
    : "${CANDIDATE_SERVER_REVIEWED_CHECKPOINT_B64:?Reviewed failed Candidate checkpoint is required}"
  else
    for reviewed_candidate_name in \
      CANDIDATE_ATTESTATION_SHA256 \
      CANDIDATE_PREVIEW_PORT \
      CANDIDATE_SLOT
    do
      if [[ -z "${!reviewed_candidate_name:-}" ]]; then
        echo "[ERROR] $reviewed_candidate_name is missing from the verified Candidate handoff" >&2
        exit 1
      fi
    done
  fi
fi

[[ "$AUTH_MODE" == "key" || "$AUTH_MODE" == "password" ]]
[[ "$GITHUB_RUN_ID" =~ ^[1-9][0-9]*$ ]]
[[ "$GITHUB_RUN_ATTEMPT" =~ ^[1-9][0-9]*$ ]]
if [[ "$CONTROL_MODE" != "capture-canonical-cleanup" ]]; then
  [[ "$CANDIDATE_HANDOFF_SOURCE" == "github-artifact" \
    || "$CANDIDATE_HANDOFF_SOURCE" == "canonical-server" \
    || "$CANDIDATE_HANDOFF_SOURCE" == "failed-github-artifact" ]]
  [[ "$CANDIDATE_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]]
  [[ "$CANDIDATE_ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  [[ "$CANDIDATE_SERVER_CHECKPOINT_SHA256" =~ ^[0-9a-f]{64}$ ]]
  [[ "$CANDIDATE_SERVER_EVIDENCE_SHA256" =~ ^[0-9a-f]{64}$ ]]
  if [[ "$CONTROL_MODE" == "discard-failed-candidate" ]]; then
    [[ "$CANDIDATE_HANDOFF_SOURCE" == "failed-github-artifact" ]]
  else
    [[ "$CANDIDATE_ATTESTATION_SHA256" =~ ^[0-9a-f]{64}$ ]]
    [[ "$CANDIDATE_SLOT" == "8000" || "$CANDIDATE_SLOT" == "8001" ]]
    [[ "$CANDIDATE_PREVIEW_PORT" == "18002" ]]
  fi
fi

SSH_PORT="${SSH_PORT:-22}"
DEPLOY_SERVER_NAME="${DEPLOY_SERVER_NAME:-ojeur.cloud www.ojeur.cloud}"
DEPLOY_ENABLE_HTTPS="${DEPLOY_ENABLE_HTTPS:-true}"
ssh_options=(
  -T -p "$SSH_PORT"
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$HOME/.ssh/known_hosts"
  -o ConnectTimeout=20
  -o ConnectionAttempts=2
  -o ServerAliveInterval=10
  -o ServerAliveCountMax=2
)
if [[ "$AUTH_MODE" == "key" ]]; then
  : "${SSH_PRIVATE_KEY:?Missing SSH_PRIVATE_KEY}"
  install -d -m 700 ~/.ssh
  printf '%s\n' "$SSH_PRIVATE_KEY" > ~/.ssh/github_actions_tencent_key
  chmod 600 ~/.ssh/github_actions_tencent_key
  ssh_command=(ssh -o BatchMode=yes -i ~/.ssh/github_actions_tencent_key)
else
  : "${SSH_PASSWORD:?Missing SSH_PASSWORD}"
  command -v sshpass >/dev/null
  export SSHPASS="$SSH_PASSWORD"
  ssh_command=(
    sshpass -e ssh
    -o PreferredAuthentications=password
    -o PubkeyAuthentication=no
  )
fi

if [[ "$CONTROL_MODE" == "capture-canonical-cleanup" ]]; then
  : "${RUNNER_TEMP:?RUNNER_TEMP is required for canonical cleanup capture}"
  case "$CANDIDATE_CANONICAL_BUNDLE_OUTPUT" in
    "$RUNNER_TEMP"/*) ;;
    *)
      echo "[ERROR] Canonical Candidate bundle must stay below RUNNER_TEMP" >&2
      exit 1
      ;;
  esac
  capture_payload="$(mktemp "$RUNNER_TEMP/candidate-canonical-capture.XXXXXX")"
  capture_output="$(mktemp "$RUNNER_TEMP/candidate-canonical-bundle.XXXXXX")"
  umask 077
  chmod 600 "$capture_payload" "$capture_output"
  trap 'rm -f "$capture_payload" "$capture_output"' EXIT
  write_capture_export() {
    local name="$1"
    local value="$2"
    printf 'export %s=%q\n' "$name" "$value" >> "$capture_payload"
  }
  write_capture_export CAPTURE_REPOSITORY "$GITHUB_REPOSITORY"
  write_capture_export CAPTURE_COMMIT_SHA "$CANDIDATE_COMMIT_SHA"
  write_capture_export CAPTURE_ARCHIVE_SHA256 "$CANDIDATE_ARCHIVE_SHA256"
  write_capture_export CAPTURE_RUN_ID "$CANDIDATE_RUN_ID"
  write_capture_export CAPTURE_RUN_ATTEMPT "$CANDIDATE_RUN_ATTEMPT"
  write_capture_export CAPTURE_CLEANUP_MODE "$CANDIDATE_CLEANUP_MODE"
  write_capture_export CAPTURE_REQUESTED_ATTESTATION_SHA256 \
    "$CANDIDATE_REQUESTED_ATTESTATION_SHA256"
  cat >> "$capture_payload" <<'REMOTE_CAPTURE'
set -Eeuo pipefail
python3 -B - <<'PY'
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.request import build_opener, ProxyHandler, Request


def fail(message: str) -> None:
    raise SystemExit(f"[ERROR] {message}")


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        fail(f"{name} is required")
    return value


def assert_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        fail(f"{label} is missing")
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        fail(f"{label} is unsafe")


def read_regular(path: Path, label: str, maximum: int = 1_048_576) -> bytes:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        fail(f"{label} is unavailable: {exc}")
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
            or metadata.st_size > maximum
        ):
            fail(f"{label} is unsafe")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(maximum + 1)
        after = os.fstat(descriptor)
        if (
            len(raw) != metadata.st_size
            or after.st_size != metadata.st_size
            or after.st_mtime_ns != metadata.st_mtime_ns
            or after.st_ino != metadata.st_ino
            or after.st_dev != metadata.st_dev
        ):
            fail(f"{label} changed while it was read")
        return raw
    finally:
        os.close(descriptor)


def read_json(path: Path, label: str) -> tuple[dict[str, object], bytes]:
    raw = read_regular(path, label)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        fail(f"{label} is invalid JSON: {exc}")
    if not isinstance(payload, dict):
        fail(f"{label} root must be an object")
    return payload, raw


def hash_regular(path: Path, label: str, maximum: int) -> tuple[int, str]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        fail(f"{label} is unavailable: {exc}")
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
            or metadata.st_size > maximum
        ):
            fail(f"{label} is unsafe")
        digest = hashlib.sha256()
        total = 0
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            digest.update(block)
            total += len(block)
        after = os.fstat(descriptor)
        if (
            total != metadata.st_size
            or after.st_size != metadata.st_size
            or after.st_mtime_ns != metadata.st_mtime_ns
            or after.st_ino != metadata.st_ino
            or after.st_dev != metadata.st_dev
        ):
            fail(f"{label} changed while it was hashed")
        return total, digest.hexdigest()
    finally:
        os.close(descriptor)


repository = required("CAPTURE_REPOSITORY")
commit = required("CAPTURE_COMMIT_SHA")
archive_sha256 = required("CAPTURE_ARCHIVE_SHA256")
run_id_raw = required("CAPTURE_RUN_ID")
run_attempt_raw = required("CAPTURE_RUN_ATTEMPT")
cleanup_mode = required("CAPTURE_CLEANUP_MODE")
requested_attestation = required("CAPTURE_REQUESTED_ATTESTATION_SHA256")
if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
    fail("canonical cleanup commit SHA is invalid")
if re.fullmatch(r"[0-9a-f]{64}", archive_sha256) is None:
    fail("canonical cleanup archive SHA-256 is invalid")
if re.fullmatch(r"[0-9a-f]{64}", requested_attestation) is None:
    fail("canonical cleanup requested attestation SHA-256 is invalid")
if re.fullmatch(r"[1-9][0-9]*", run_id_raw) is None or re.fullmatch(
    r"[1-9][0-9]*", run_attempt_raw
) is None:
    fail("canonical cleanup run identity is invalid")
if cleanup_mode not in {"discard-candidate", "release-candidate"}:
    fail("canonical cleanup mode is invalid")
run_id = int(run_id_raw)
run_attempt = int(run_attempt_raw)

home = Path.home()
state_dir = home / ".local/state/jato-production-release"
checkpoint_dir = state_dir / "checkpoints" / commit
checkpoint_path = checkpoint_dir / f"{archive_sha256}.json"
evidence_path = checkpoint_dir / f"{archive_sha256}.evidence.json"
journal_path = state_dir / "journals" / commit / f"{archive_sha256}.jsonl"
lock_path = state_dir / "production-deploy.lock"
for directory, label in (
    (home, "remote home"),
    (state_dir, "production release state directory"),
    (state_dir / "checkpoints", "production checkpoint directory"),
    (checkpoint_dir, "Candidate checkpoint directory"),
    (state_dir / "journals", "production journal directory"),
    (journal_path.parent, "Candidate journal directory"),
):
    assert_directory(directory, label)

lock_flags = os.O_RDONLY
if hasattr(os, "O_NOFOLLOW"):
    lock_flags |= os.O_NOFOLLOW
try:
    lock_descriptor = os.open(lock_path, lock_flags)
except OSError as exc:
    fail(f"production lock is missing or unsafe: {exc}")
try:
    lock_metadata = os.fstat(lock_descriptor)
    if not stat.S_ISREG(lock_metadata.st_mode) or lock_metadata.st_nlink != 1:
        fail("production lock is not a regular single-link file")
    deadline = time.monotonic() + 300
    while True:
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if time.monotonic() >= deadline:
                fail("another production deployment holds the server-side lock")
            time.sleep(0.25)

    checkpoint, checkpoint_raw = read_json(checkpoint_path, "Candidate checkpoint")
    evidence, evidence_raw = read_json(evidence_path, "Candidate evidence")
    journal_raw = read_regular(
        journal_path,
        "Candidate checkpoint journal",
        maximum=8 * 1_048_576,
    )
    identity = checkpoint.get("identity")
    expected_identity = {
        "repository": repository,
        "commit": commit,
        "archiveSha256": archive_sha256,
        "runId": run_id,
        "runAttempt": run_attempt,
    }
    if not isinstance(identity, dict) or any(
        identity.get(key) != value for key, value in expected_identity.items()
    ):
        fail("canonical Candidate checkpoint identity mismatch")
    allowed_states = {
        "discard-candidate": {
            ("candidate_ready", "inspect_then_resume"),
            ("rollback_completed", "automatic"),
        },
        "release-candidate": {("active_updated", "inspect_then_resume")},
    }[cleanup_mode]
    checkpoint_state = (checkpoint.get("phase"), checkpoint.get("retryClass"))
    if checkpoint.get("status") != "completed" or checkpoint_state not in allowed_states:
        fail("canonical Candidate checkpoint is not eligible for cleanup")
    if evidence.get("identity") != identity:
        fail("canonical Candidate checkpoint/evidence identity mismatch")
    try:
        journal_lines = journal_raw.decode("utf-8").splitlines()
        journal_events = [json.loads(line) for line in journal_lines]
    except (UnicodeError, json.JSONDecodeError) as exc:
        fail(f"Candidate checkpoint journal is invalid: {exc}")
    if not journal_events:
        fail("Candidate checkpoint journal is empty")
    candidate_ready_events = []
    for expected_sequence, event in enumerate(journal_events, start=1):
        if (
            not isinstance(event, dict)
            or event.get("event") != "checkpoint_transition"
            or event.get("sequence") != expected_sequence
            or event.get("identity") != identity
        ):
            fail("Candidate checkpoint journal sequence/identity is invalid")
        if (
            event.get("phase") == "candidate_ready"
            and event.get("status") == "completed"
            and event.get("retryClass") == "inspect_then_resume"
        ):
            candidate_ready_events.append(event)
    journal_tail = dict(journal_events[-1])
    journal_tail.pop("event", None)
    if journal_tail != checkpoint:
        fail("Candidate checkpoint journal tail differs from checkpoint")
    if len(candidate_ready_events) != 1:
        fail("Candidate checkpoint journal has no unique ready attestation source")
    ready_checkpoint = dict(candidate_ready_events[0])
    ready_checkpoint.pop("event", None)

    archive_relative = (
        f".cache/jato-releases/archives/{commit}/{archive_sha256}.tar.gz"
    )
    archive_path = home / archive_relative
    archive_size, archive_digest = hash_regular(
        archive_path,
        "content-addressed Candidate archive",
        maximum=512 * 1024 * 1024,
    )
    if archive_digest != archive_sha256:
        fail("content-addressed Candidate archive SHA-256 mismatch")
    archive_bytes = identity.get("archiveBytes")
    if (
        isinstance(archive_bytes, bool)
        or not isinstance(archive_bytes, int)
        or archive_bytes != archive_size
    ):
        fail("content-addressed Candidate archive byte count mismatch")

    release_root = Path("/opt/jato/releases") / commit / archive_sha256
    for directory, label in (
        (Path("/opt/jato/releases"), "release storage root"),
        (release_root.parent, "release commit root"),
        (release_root, "Candidate release root"),
    ):
        assert_directory(directory, label)
    frontend_manifest, _ = read_json(
        release_root / "hermes/frontend_release/frontend-release.json",
        "Candidate frontend manifest",
    )
    build_meta, _ = read_json(
        release_root / "06_AppPlatform/frontend/dist/build-meta.json",
        "Candidate frontend build metadata",
    )
    artifact_name = f"frontend-dist-{commit}"
    frontend_identity = identity.get("frontendIdentity")
    frontend_checksum = identity.get("frontendChecksum")
    expected_frontend_identity = (
        f"gha://{repository}/actions/runs/{run_id}/attempts/{run_attempt}"
        f"/artifacts/{artifact_name}"
    )
    release = frontend_manifest.get("release")
    source = frontend_manifest.get("source")
    artifact = frontend_manifest.get("artifact")
    frontend = frontend_manifest.get("frontend")
    if not all(isinstance(item, dict) for item in (release, source, artifact, frontend)):
        fail("Candidate frontend manifest is incomplete")
    if (
        release.get("repository") != repository
        or release.get("workflow") != "production-release"
        or str(release.get("workflowRunId")) != run_id_raw
        or str(release.get("workflowRunAttempt")) != run_attempt_raw
        or source.get("githubSha") != commit
        or artifact.get("name") != artifact_name
        or artifact.get("id") != expected_frontend_identity
        or artifact.get("checksum") != frontend_checksum
        or frontend_identity != expected_frontend_identity
    ):
        fail("Candidate frontend manifest identity mismatch")
    github_artifact_id_raw = build_meta.get("githubArtifactId")
    github_artifact_digest = build_meta.get("githubArtifactDigest")
    frontend_build_id = frontend.get("buildId")
    node_version = frontend.get("nodeVersion")
    if (
        build_meta.get("githubSha") != commit
        or build_meta.get("artifactName") != artifact_name
        or build_meta.get("artifactId") != frontend_identity
        or build_meta.get("artifactChecksum") != frontend_checksum
        or str(build_meta.get("workflowRunId")) != run_id_raw
        or str(build_meta.get("workflowRunAttempt")) != run_attempt_raw
        or build_meta.get("frontendBuildId") != frontend_build_id
        or build_meta.get("nodeVersion") != node_version
        or not isinstance(github_artifact_id_raw, (str, int))
        or isinstance(github_artifact_id_raw, bool)
        or re.fullmatch(r"[1-9][0-9]*", str(github_artifact_id_raw)) is None
        or not isinstance(github_artifact_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", github_artifact_digest) is None
        or not isinstance(frontend_build_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", frontend_build_id) is None
        or not isinstance(node_version, str)
        or re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", node_version) is None
    ):
        fail("Candidate frontend build identity mismatch")

    evidence_sha256 = hashlib.sha256(evidence_raw).hexdigest()
    ready_message = ready_checkpoint.get("message")
    ready_binding = re.search(
        r"(?:^|[; ])evidence_path=(\S+) "
        r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])",
        ready_message if isinstance(ready_message, str) else "",
    )
    if (
        ready_binding is None
        or ready_binding.group(1) != str(evidence_path)
        or ready_binding.group(2) != evidence_sha256
    ):
        fail("original Candidate-ready checkpoint evidence binding mismatch")
    backup = evidence.get("backup")
    migration = evidence.get("migration")
    if not isinstance(backup, dict) or not isinstance(migration, dict):
        fail("Candidate evidence final outcome is incomplete")
    manifest_path = backup.get("manifestPath")
    manifest_bytes = backup.get("manifestBytes")
    manifest_sha256 = backup.get("manifestSha256")
    migration_status = migration.get("status")
    if (
        not isinstance(manifest_path, str)
        or not manifest_path.startswith("/")
        or isinstance(manifest_bytes, bool)
        or not isinstance(manifest_bytes, int)
        or manifest_bytes <= 0
        or not isinstance(manifest_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
        or migration_status not in {"completed", "not_required"}
    ):
        fail("Candidate evidence backup/migration outcome is invalid")
    github_artifact_id = int(github_artifact_id_raw)

    opener = build_opener(ProxyHandler({}))
    try:
        response = opener.open(
            Request(
                "http://127.0.0.1:18002/candidate-preview.json",
                headers={"Cache-Control": "no-cache"},
            ),
            timeout=20,
        )
        preview_raw = response.read(65_537)
    except OSError as exc:
        fail(f"Candidate preview proof is unavailable: {exc}")
    if len(preview_raw) > 65_536:
        fail("Candidate preview proof is too large")
    try:
        preview = json.loads(preview_raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        fail(f"Candidate preview proof is invalid JSON: {exc}")
    if (
        not isinstance(preview, dict)
        or preview.get("role") != "candidate"
        or preview.get("commitSha") != commit
        or preview.get("archiveSha256") != archive_sha256
        or preview.get("candidateSlot") not in {8000, 8001}
        or preview.get("previewPort") != 18002
    ):
        fail("Candidate preview proof identity mismatch")

    ready_checkpoint_raw = (
        json.dumps(ready_checkpoint, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    attestation = {
        "schemaVersion": 1,
        "releaseId": f"{run_id}-{run_attempt}",
        "releaseMode": "prepare-candidate",
        "identity": identity,
        "approvalHandoff": {
            "workflow": "production-release",
            "remoteArchivePath": archive_relative,
            "frontendArtifactName": artifact_name,
            "frontendGithubArtifactId": github_artifact_id,
            "frontendGithubArtifactDigest": github_artifact_digest,
            "frontendBuildId": frontend_build_id,
            "frontendNodeVersion": node_version,
        },
        "serverCheckpoint": {
            "remotePath": str(checkpoint_path),
            "sha256": hashlib.sha256(ready_checkpoint_raw).hexdigest(),
            "phase": "candidate_ready",
            "status": "completed",
        },
        "serverEvidence": {
            "remotePath": str(evidence_path),
            "sha256": evidence_sha256,
            "backupManifestBytes": manifest_bytes,
            "backupManifestSha256": manifest_sha256,
            "migrationStatus": migration_status,
        },
        "candidatePreview": preview,
    }
    reconstructed_attestation_raw = (
        json.dumps(attestation, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    reconstructed_attestation_sha256 = hashlib.sha256(
        reconstructed_attestation_raw
    ).hexdigest()
    if reconstructed_attestation_sha256 != requested_attestation:
        fail("canonical Candidate does not match the reviewed attestation SHA-256")

    bundle = {
        "schemaVersion": 1,
        "source": "canonical-server",
        "request": {
            "repository": repository,
            "commit": commit,
            "archiveSha256": archive_sha256,
            "runId": run_id,
            "runAttempt": run_attempt,
            "cleanupMode": cleanup_mode,
            "requestedAttestationSha256": requested_attestation,
        },
        "productionLock": {
            "path": str(lock_path),
            "mode": "exclusive",
            "held": True,
        },
        "reviewedAttestation": {
            "sha256": reconstructed_attestation_sha256,
            "source": "reconstructed-from-canonical-journal",
        },
        "checkpoint": {
            "remotePath": str(checkpoint_path),
            "sha256": hashlib.sha256(checkpoint_raw).hexdigest(),
            "contentBase64": base64.b64encode(checkpoint_raw).decode("ascii"),
        },
        "evidence": {
            "remotePath": str(evidence_path),
            "sha256": hashlib.sha256(evidence_raw).hexdigest(),
            "contentBase64": base64.b64encode(evidence_raw).decode("ascii"),
        },
        "candidatePreview": preview,
        "archive": {
            "remotePath": archive_relative,
            "bytes": archive_bytes,
            "sha256": archive_sha256,
        },
        "frontend": {
            "artifactName": artifact_name,
            "artifactIdentity": frontend_identity,
            "artifactChecksum": frontend_checksum,
            "githubArtifactId": github_artifact_id,
            "githubArtifactDigest": github_artifact_digest,
            "buildId": frontend_build_id,
            "nodeVersion": node_version,
        },
    }
    json.dump(bundle, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
finally:
    os.close(lock_descriptor)
PY
REMOTE_CAPTURE
  timeout "${CANDIDATE_CONTROL_TIMEOUT:-3600}s" \
    "${ssh_command[@]}" "${ssh_options[@]}" \
    "$SSH_USER@$SSH_HOST" "umask 077; exec bash -s" \
    < "$capture_payload" > "$capture_output"
  python3 - "$capture_output" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
if not isinstance(payload, dict) or payload.get("source") != "canonical-server":
    raise SystemExit("canonical Candidate capture did not return one exact bundle")
PY
  install -m 600 "$capture_output" "$CANDIDATE_CANONICAL_BUNDLE_OUTPUT"
  exit 0
fi

archive_validator="03_Scripts/deploy/validate_release_archive.py"
outer_release="03_Scripts/deploy/fullstack_remote_release.sh"
failed_candidate_controller="03_Scripts/deploy/tencent_bluegreen_release.sh"
local_control_files=("$archive_validator" "$outer_release")
if [[ "$CONTROL_MODE" == "discard-failed-candidate" ]]; then
  local_control_files+=("$failed_candidate_controller")
fi
for local_control_file in "${local_control_files[@]}"; do
  if [[ -L "$local_control_file" ]] || [[ ! -f "$local_control_file" ]]; then
    echo "[ERROR] Candidate control source is missing or unsafe: $local_control_file" >&2
    exit 1
  fi
done
archive_validator_sha256="$(sha256sum "$archive_validator" | awk '{print $1}')"
archive_validator_b64="$(base64 -w 0 "$archive_validator")"
failed_candidate_controller_sha256=""
failed_candidate_controller_b64=""
if [[ "$CONTROL_MODE" == "discard-failed-candidate" ]]; then
  failed_candidate_controller_sha256="$(
    sha256sum "$failed_candidate_controller" | awk '{print $1}'
  )"
  failed_candidate_controller_b64="$(base64 -w 0 "$failed_candidate_controller")"
fi
control_payload="$(mktemp "${RUNNER_TEMP:-/tmp}/candidate-control.XXXXXX")"
umask 077
chmod 600 "$control_payload"
trap 'rm -f "$control_payload"' EXIT

write_remote_export() {
  local name="$1"
  local value="$2"
  printf 'export %s=%q\n' "$name" "$value" >> "$control_payload"
}

write_remote_assignment() {
  local name="$1"
  local value="$2"
  printf '%s=%q\n' "$name" "$value" >> "$control_payload"
}

write_remote_export DEPLOY_SERVER_NAME "$DEPLOY_SERVER_NAME"
write_remote_export DEPLOY_ENABLE_HTTPS "$DEPLOY_ENABLE_HTTPS"
write_remote_export DEPLOY_CERTBOT_EMAIL "${DEPLOY_CERTBOT_EMAIL:-}"
write_remote_export DEPLOY_BRANCH main
write_remote_export DEPLOY_BLUEGREEN_MODE "$CONTROL_MODE"
write_remote_export DEPLOY_COMMIT_SHA "$CANDIDATE_COMMIT_SHA"
write_remote_export DEPLOY_SHORT_SHA "${CANDIDATE_COMMIT_SHA:0:8}"
write_remote_export DEPLOY_RELEASE_ID "$CANDIDATE_RELEASE_ID"
write_remote_export DEPLOY_RUN_ID "$CANDIDATE_RUN_ID"
write_remote_export DEPLOY_RUN_ATTEMPT "$CANDIDATE_RUN_ATTEMPT"
write_remote_export DEPLOY_APPROVAL_RUN_ID "$GITHUB_RUN_ID"
write_remote_export DEPLOY_APPROVAL_RUN_ATTEMPT "$GITHUB_RUN_ATTEMPT"
write_remote_export DEPLOY_CANDIDATE_HANDOFF_SOURCE "$CANDIDATE_HANDOFF_SOURCE"
if [[ "$CONTROL_MODE" == "discard-failed-candidate" ]]; then
  write_remote_export DEPLOY_CANDIDATE_REVIEWED_CHECKPOINT_B64 \
    "$CANDIDATE_SERVER_REVIEWED_CHECKPOINT_B64"
  # Keep the large controller payload out of the environment inherited by
  # external commands. Bash receives it over stdin, verifies it, materializes
  # one transient file, and unsets it before handing off to the controller.
  write_remote_assignment DISCARD_FAILED_CANDIDATE_CONTROLLER_B64 \
    "$failed_candidate_controller_b64"
  write_remote_export DISCARD_FAILED_CANDIDATE_CONTROLLER_SHA256 \
    "$failed_candidate_controller_sha256"
else
  write_remote_export DEPLOY_CANDIDATE_ATTESTATION_SHA256 \
    "$CANDIDATE_ATTESTATION_SHA256"
  write_remote_export DEPLOY_CANDIDATE_SLOT "$CANDIDATE_SLOT"
  write_remote_export DEPLOY_CANDIDATE_PREVIEW_PORT "$CANDIDATE_PREVIEW_PORT"
fi
write_remote_export DEPLOY_CANDIDATE_SERVER_CHECKPOINT_PATH \
  "$CANDIDATE_SERVER_CHECKPOINT_PATH"
write_remote_export DEPLOY_CANDIDATE_SERVER_CHECKPOINT_SHA256 \
  "$CANDIDATE_SERVER_CHECKPOINT_SHA256"
write_remote_export DEPLOY_CANDIDATE_SERVER_EVIDENCE_PATH \
  "$CANDIDATE_SERVER_EVIDENCE_PATH"
write_remote_export DEPLOY_CANDIDATE_SERVER_EVIDENCE_SHA256 \
  "$CANDIDATE_SERVER_EVIDENCE_SHA256"
write_remote_export DEPLOY_ARCHIVE_PATH "$CANDIDATE_REMOTE_ARCHIVE_PATH"
write_remote_export DEPLOY_ARCHIVE_BYTES "$CANDIDATE_ARCHIVE_BYTES"
write_remote_export DEPLOY_ARCHIVE_SHA256 "$CANDIDATE_ARCHIVE_SHA256"
write_remote_export RELEASE_ARCHIVE_VALIDATOR_B64 "$archive_validator_b64"
write_remote_export RELEASE_ARCHIVE_VALIDATOR_SHA256 "$archive_validator_sha256"
write_remote_export DEPLOY_REPOSITORY "$GITHUB_REPOSITORY"
write_remote_export DEPLOY_WORKFLOW "$GITHUB_WORKFLOW"
write_remote_export FRONTEND_ARTIFACT_NAME "$CANDIDATE_FRONTEND_ARTIFACT_NAME"
write_remote_export FRONTEND_ARTIFACT_IDENTITY "$CANDIDATE_FRONTEND_ARTIFACT_IDENTITY"
write_remote_export FRONTEND_ARTIFACT_CHECKSUM "$CANDIDATE_FRONTEND_ARTIFACT_CHECKSUM"
write_remote_export FRONTEND_GITHUB_ARTIFACT_ID "$CANDIDATE_GITHUB_ARTIFACT_ID"
write_remote_export FRONTEND_GITHUB_ARTIFACT_DIGEST "$CANDIDATE_GITHUB_ARTIFACT_DIGEST"
write_remote_export FRONTEND_BUILD_ID "$CANDIDATE_FRONTEND_BUILD_ID"
write_remote_export FRONTEND_NODE_VERSION "$CANDIDATE_FRONTEND_NODE_VERSION"
for secret_name in \
  DEEPSEEK_API_KEY HERMES_SYNC_TOKEN GOOGLE_CLIENT_ID \
  GOOGLE_CLIENT_SECRET GOOGLE_OAUTH_PROXY_URL GOOGLE_OAUTH_RELAY_URL \
  GOOGLE_OAUTH_RELAY_TOKEN MIHOMO_SUB_URL MIHOMO_DB_PATH
do
  write_remote_export "$secret_name" "${!secret_name:-}"
done
cat "$outer_release" >> "$control_payload"

timeout "${CANDIDATE_CONTROL_TIMEOUT:-3600}s" \
  "${ssh_command[@]}" "${ssh_options[@]}" \
  "$SSH_USER@$SSH_HOST" "umask 077; exec bash -s" < "$control_payload"
