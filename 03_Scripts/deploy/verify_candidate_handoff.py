#!/usr/bin/env python3
"""Verify an immutable prepare-candidate handoff and emit a private env file."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
from typing import Any


SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
CANONICAL_CLEANUP_STATES = {
    "discard-candidate": frozenset(
        {
            ("candidate_ready", "inspect_then_resume"),
            ("rollback_completed", "automatic"),
        }
    ),
    "release-candidate": frozenset(
        {("active_updated", "inspect_then_resume")}
    ),
}


def _private_server_path(value: Any, suffix: str, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("/") or any(
        character.isspace() for character in value
    ):
        raise ValueError(f"{label} is unsafe")
    path = PurePosixPath(value)
    if path.as_posix() != value or ".." in path.parts or not value.endswith(suffix):
        raise ValueError(f"{label} is unsafe")
    return value


def _read_regular_json(
    path: Path,
    label: str,
    *,
    maximum_bytes: int = 1_048_576,
) -> tuple[dict[str, Any], bytes]:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular non-symlink file")
    raw = path.read_bytes()
    if not raw or len(raw) > maximum_bytes:
        raise ValueError(f"{label} has an invalid size")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} root must be an object")
    return payload, raw


def _load_artifacts(path: Path) -> list[dict[str, Any]]:
    pages = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(pages, dict):
        pages = [pages]
    if not isinstance(pages, list):
        raise ValueError("prepare-run artifact response is invalid")
    artifacts: list[dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict) or not isinstance(page.get("artifacts"), list):
            raise ValueError("prepare-run artifact page is invalid")
        for artifact in page["artifacts"]:
            if not isinstance(artifact, dict):
                raise ValueError("prepare-run artifact entry is invalid")
            artifacts.append(artifact)
    return artifacts


def _read_regular_jsonl(
    path: Path,
    label: str,
    *,
    maximum_bytes: int = 1_048_576,
) -> list[dict[str, Any]]:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular non-symlink file")
    raw = path.read_bytes()
    if not raw or len(raw) > maximum_bytes:
        raise ValueError(f"{label} has an invalid size")
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
        if not line:
            raise ValueError(f"{label} contains an empty line")
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ValueError(f"{label} line {line_number} must be an object")
        records.append(record)
    return records


def _require_empty_regular_file(path: Path, label: str) -> None:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular non-symlink file")
    if metadata.st_size != 0:
        raise ValueError(f"{label} must be empty")


def _write_private_file(path: Path, raw: bytes, label: str) -> None:
    if path.is_symlink():
        raise ValueError(f"{label} path is unsafe")
    path.write_bytes(raw)
    os.chmod(path, 0o600)


def _write_verified_environment(
    args: argparse.Namespace,
    values: dict[str, str],
    checkpoint_raw: bytes,
) -> None:
    if args.env_output.is_symlink():
        raise ValueError("verified Candidate environment path is unsafe")
    args.env_output.write_text(
        "".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items()),
        encoding="utf-8",
    )
    os.chmod(args.env_output, 0o600)
    reviewed_checkpoint_output = getattr(args, "reviewed_checkpoint_output", None)
    if reviewed_checkpoint_output is not None:
        _write_private_file(
            reviewed_checkpoint_output,
            checkpoint_raw,
            "reviewed Candidate checkpoint",
        )


def _canonical_content(
    record: Any,
    *,
    label: str,
) -> tuple[dict[str, Any], bytes, str, str]:
    if not isinstance(record, dict):
        raise ValueError(f"canonical {label} record is missing")
    remote_path = _private_server_path(
        record.get("remotePath"),
        ".evidence.json" if label == "evidence" else ".json",
        f"canonical {label} path",
    )
    expected_sha256 = record.get("sha256")
    encoded = record.get("contentBase64")
    if (
        not isinstance(expected_sha256, str)
        or not SHA256_PATTERN.fullmatch(expected_sha256)
        or not isinstance(encoded, str)
    ):
        raise ValueError(f"canonical {label} content identity is invalid")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"canonical {label} content is not valid base64") from exc
    if not raw or len(raw) > 1_048_576:
        raise ValueError(f"canonical {label} content has an invalid size")
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError(f"canonical {label} SHA-256 mismatch")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"canonical {label} content is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"canonical {label} root must be an object")
    return payload, raw, remote_path, expected_sha256


def verify(args: argparse.Namespace) -> None:
    checkpoint, checkpoint_raw = _read_regular_json(
        args.checkpoint,
        "Candidate checkpoint",
    )
    attestation, attestation_raw = _read_regular_json(
        args.attestation,
        "Candidate attestation",
    )
    actual_attestation_sha256 = hashlib.sha256(attestation_raw).hexdigest()
    if actual_attestation_sha256 != args.attestation_sha256:
        raise ValueError("Candidate attestation SHA-256 mismatch")

    identity = checkpoint.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("Candidate checkpoint identity is missing")
    if (
        checkpoint.get("phase") != "candidate_ready"
        or checkpoint.get("status") != "completed"
        or checkpoint.get("retryClass") != "inspect_then_resume"
    ):
        raise ValueError("Candidate checkpoint is not candidate_ready/completed")
    expected_scalars = {
        "repository": args.repository,
        "commit": args.commit,
        "archiveSha256": args.archive_sha256,
        "runId": args.run_id,
        "runAttempt": args.run_attempt,
    }
    for key, expected in expected_scalars.items():
        if identity.get(key) != expected:
            raise ValueError(f"Candidate identity mismatch for {key}")
    archive_bytes = identity.get("archiveBytes")
    if (
        isinstance(archive_bytes, bool)
        or not isinstance(archive_bytes, int)
        or archive_bytes <= 0
    ):
        raise ValueError("Candidate archive byte count is invalid")
    frontend_identity = identity.get("frontendIdentity")
    frontend_checksum = identity.get("frontendChecksum")
    if not isinstance(frontend_identity, str) or not frontend_identity:
        raise ValueError("Candidate frontend identity is invalid")
    if not isinstance(frontend_checksum, str) or not SHA256_PATTERN.fullmatch(
        frontend_checksum
    ):
        raise ValueError("Candidate frontend checksum is invalid")

    if attestation.get("identity") != identity:
        raise ValueError("Candidate checkpoint/attestation identity mismatch")
    if attestation.get("releaseId") != f"{args.run_id}-{args.run_attempt}":
        raise ValueError("Candidate release id mismatch")
    if attestation.get("releaseMode") != "prepare-candidate":
        raise ValueError("Candidate was not produced by prepare-candidate mode")
    server_checkpoint = attestation.get("serverCheckpoint")
    if not isinstance(server_checkpoint, dict):
        raise ValueError("Candidate server checkpoint attestation is missing")
    checkpoint_suffix = (
        "/.local/state/jato-production-release/checkpoints/"
        f"{args.commit}/{args.archive_sha256}.json"
    )
    expected_checkpoint_path = _private_server_path(
        server_checkpoint.get("remotePath"),
        checkpoint_suffix,
        "Candidate server checkpoint path",
    )
    if (
        server_checkpoint.get("sha256")
        != hashlib.sha256(checkpoint_raw).hexdigest()
        or server_checkpoint.get("phase") != "candidate_ready"
        or server_checkpoint.get("status") != "completed"
    ):
        raise ValueError("Candidate server checkpoint attestation mismatch")
    server_evidence = attestation.get("serverEvidence")
    if not isinstance(server_evidence, dict):
        raise ValueError("Candidate server evidence attestation is missing")
    expected_evidence_path = _private_server_path(
        server_evidence.get("remotePath"),
        checkpoint_suffix.removesuffix(".json") + ".evidence.json",
        "Candidate server evidence path",
    )
    evidence_sha256 = server_evidence.get("sha256")
    if (
        not isinstance(evidence_sha256, str)
        or not SHA256_PATTERN.fullmatch(evidence_sha256)
        or expected_checkpoint_path
        != expected_evidence_path.removesuffix(".evidence.json") + ".json"
    ):
        raise ValueError("Candidate server evidence attestation mismatch")
    message = checkpoint.get("message")
    binding = re.search(
        r"(?:^|[; ])evidence_path=(\S+) "
        r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])",
        message if isinstance(message, str) else "",
    )
    if (
        binding is None
        or binding.group(1) != expected_evidence_path
        or binding.group(2) != evidence_sha256
    ):
        raise ValueError("Candidate checkpoint evidence binding mismatch")
    preview = attestation.get("candidatePreview")
    if (
        not isinstance(preview, dict)
        or preview.get("role") != "candidate"
        or preview.get("commitSha") != args.commit
        or preview.get("archiveSha256") != args.archive_sha256
        or preview.get("candidateSlot") not in {8000, 8001}
        or preview.get("previewPort") != 18002
    ):
        raise ValueError("Candidate preview attestation mismatch")
    candidate_slot = preview["candidateSlot"]
    preview_port = preview["previewPort"]

    handoff = attestation.get("approvalHandoff")
    if not isinstance(handoff, dict):
        raise ValueError("Candidate approval handoff is missing")
    expected_archive_path = (
        f".cache/jato-releases/archives/{args.commit}/{args.archive_sha256}.tar.gz"
    )
    artifact_name = f"frontend-dist-{args.commit}"
    if handoff.get("workflow") != "production-release":
        raise ValueError("Candidate workflow handoff mismatch")
    if handoff.get("remoteArchivePath") != expected_archive_path:
        raise ValueError("Candidate remote archive handoff mismatch")
    if handoff.get("frontendArtifactName") != artifact_name:
        raise ValueError("Candidate frontend artifact name mismatch")
    github_artifact_id = handoff.get("frontendGithubArtifactId")
    github_artifact_digest = handoff.get("frontendGithubArtifactDigest")
    frontend_build_id = handoff.get("frontendBuildId")
    node_version = handoff.get("frontendNodeVersion")
    if (
        isinstance(github_artifact_id, bool)
        or not isinstance(github_artifact_id, int)
        or github_artifact_id <= 0
    ):
        raise ValueError("Candidate frontend GitHub artifact id is invalid")
    if not isinstance(github_artifact_digest, str) or re.fullmatch(
        r"(?:sha256:)?[0-9a-f]{64}",
        github_artifact_digest,
    ) is None:
        raise ValueError("Candidate frontend GitHub artifact digest is invalid")
    if not isinstance(frontend_build_id, str) or not SHA256_PATTERN.fullmatch(
        frontend_build_id
    ):
        raise ValueError("Candidate frontend build id is invalid")
    if not isinstance(node_version, str) or re.fullmatch(
        r"v[0-9]+\.[0-9]+\.[0-9]+",
        node_version,
    ) is None:
        raise ValueError("Candidate frontend Node version is invalid")

    artifacts = _load_artifacts(args.artifacts)
    candidate_name = f"release-candidate-{args.commit}-{args.run_attempt}"
    selected = {
        name: [item for item in artifacts if item.get("name") == name]
        for name in {candidate_name, artifact_name}
    }
    for name, matches in selected.items():
        if len(matches) != 1 or matches[0].get("expired") is not False:
            raise ValueError(f"exact non-expired artifact is unavailable: {name}")
    frontend_artifact = selected[artifact_name][0]
    if frontend_artifact.get("id") != github_artifact_id:
        raise ValueError("Candidate frontend GitHub artifact id/API mismatch")
    normalized_digest = (
        github_artifact_digest
        if github_artifact_digest.startswith("sha256:")
        else f"sha256:{github_artifact_digest}"
    )
    if frontend_artifact.get("digest") != normalized_digest:
        raise ValueError("Candidate frontend GitHub artifact digest/API mismatch")

    values = {
        "CANDIDATE_HANDOFF_SOURCE": "github-artifact",
        "CANDIDATE_REPOSITORY": args.repository,
        "CANDIDATE_COMMIT_SHA": args.commit,
        "CANDIDATE_ARCHIVE_SHA256": args.archive_sha256,
        "CANDIDATE_ARCHIVE_BYTES": str(archive_bytes),
        "CANDIDATE_RUN_ID": str(args.run_id),
        "CANDIDATE_RUN_ATTEMPT": str(args.run_attempt),
        "CANDIDATE_RELEASE_ID": f"{args.run_id}-{args.run_attempt}",
        "CANDIDATE_REMOTE_ARCHIVE_PATH": expected_archive_path,
        "CANDIDATE_FRONTEND_ARTIFACT_NAME": artifact_name,
        "CANDIDATE_FRONTEND_ARTIFACT_IDENTITY": frontend_identity,
        "CANDIDATE_FRONTEND_ARTIFACT_CHECKSUM": frontend_checksum,
        "CANDIDATE_GITHUB_ARTIFACT_ID": str(github_artifact_id),
        "CANDIDATE_GITHUB_ARTIFACT_DIGEST": github_artifact_digest,
        "CANDIDATE_FRONTEND_BUILD_ID": frontend_build_id,
        "CANDIDATE_FRONTEND_NODE_VERSION": node_version,
        "CANDIDATE_ATTESTATION_SHA256": actual_attestation_sha256,
        "CANDIDATE_SERVER_CHECKPOINT_PATH": expected_checkpoint_path,
        "CANDIDATE_SERVER_CHECKPOINT_SHA256": server_checkpoint["sha256"],
        "CANDIDATE_SERVER_EVIDENCE_PATH": expected_evidence_path,
        "CANDIDATE_SERVER_EVIDENCE_SHA256": evidence_sha256,
        "CANDIDATE_SLOT": str(candidate_slot),
        "CANDIDATE_PREVIEW_PORT": str(preview_port),
    }
    _write_verified_environment(args, values, checkpoint_raw)


def verify_failed_handoff(args: argparse.Namespace) -> None:
    transport, _transport_raw = _read_regular_json(
        args.failed_transport_checkpoint,
        "failed Candidate transport checkpoint",
    )
    transport_journal = _read_regular_jsonl(
        args.failed_transport_journal,
        "failed Candidate transport journal",
    )
    expected_transport_event = dict(transport)
    expected_transport_event["event"] = "checkpoint_transition"
    if len(transport_journal) != 1 or transport_journal[0] != expected_transport_event:
        raise ValueError(
            "failed Candidate transport journal does not exactly bind its checkpoint"
        )

    transport_identity = transport.get("identity")
    if not isinstance(transport_identity, dict):
        raise ValueError("failed Candidate transport identity is missing")
    archive_bytes = transport_identity.get("archiveBytes")
    if (
        isinstance(archive_bytes, bool)
        or not isinstance(archive_bytes, int)
        or archive_bytes <= 0
    ):
        raise ValueError("failed Candidate archive byte count is invalid")
    frontend_identity = transport_identity.get("frontendIdentity")
    frontend_checksum = transport_identity.get("frontendChecksum")
    expected_frontend_identity = (
        f"gha://{args.repository}/actions/runs/{args.run_id}/attempts/"
        f"{args.run_attempt}/artifacts/frontend-dist-{args.commit}"
    )
    if frontend_identity != expected_frontend_identity:
        raise ValueError("failed Candidate frontend identity is invalid")
    if not isinstance(frontend_checksum, str) or not SHA256_PATTERN.fullmatch(
        frontend_checksum
    ):
        raise ValueError("failed Candidate frontend checksum is invalid")
    expected_identity = {
        "repository": args.repository,
        "commit": args.commit,
        "archiveSha256": args.archive_sha256,
        "archiveBytes": archive_bytes,
        "runId": args.run_id,
        "runAttempt": args.run_attempt,
        "frontendIdentity": frontend_identity,
        "frontendChecksum": frontend_checksum,
    }
    if transport_identity != expected_identity:
        raise ValueError("failed Candidate transport identity mismatch")
    if (
        transport.get("schemaVersion") != 1
        or transport.get("sequence") != 1
        or transport.get("phase") != "transport_verified"
        or transport.get("status") != "completed"
        or transport.get("retryClass") != "inspect_then_resume"
    ):
        raise ValueError("failed Candidate transport checkpoint is ineligible")

    server_checkpoint, server_checkpoint_raw = _read_regular_json(
        args.failed_server_checkpoint,
        "failed Candidate server checkpoint",
    )
    if server_checkpoint.get("identity") != expected_identity:
        raise ValueError("failed Candidate server checkpoint identity mismatch")
    server_sequence = server_checkpoint.get("sequence")
    if (
        server_checkpoint.get("schemaVersion") != 1
        or isinstance(server_sequence, bool)
        or not isinstance(server_sequence, int)
        or server_sequence <= 1
        or server_checkpoint.get("phase") != "migrated"
        or server_checkpoint.get("status") != "completed"
        or server_checkpoint.get("retryClass") != "automatic"
    ):
        raise ValueError(
            "failed Candidate server checkpoint is not migrated/completed/automatic"
        )

    evidence, evidence_raw = _read_regular_json(
        args.failed_server_evidence,
        "failed Candidate server evidence",
    )
    evidence_sha256 = hashlib.sha256(evidence_raw).hexdigest()
    message = server_checkpoint.get("message")
    binding = re.search(
        r"(?:^|[; ])evidence_path=(\S+) "
        r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])",
        message if isinstance(message, str) else "",
    )
    if binding is None or binding.group(2) != evidence_sha256:
        raise ValueError("failed Candidate server checkpoint evidence binding mismatch")
    checkpoint_suffix = (
        "/.local/state/jato-production-release/checkpoints/"
        f"{args.commit}/{args.archive_sha256}.json"
    )
    evidence_path = _private_server_path(
        binding.group(1),
        checkpoint_suffix.removesuffix(".json") + ".evidence.json",
        "failed Candidate server evidence path",
    )
    checkpoint_path = evidence_path.removesuffix(".evidence.json") + ".json"
    if evidence.get("identity") != expected_identity:
        raise ValueError("failed Candidate checkpoint/evidence identity mismatch")

    backup = evidence.get("backup")
    if not isinstance(backup, dict):
        raise ValueError("failed Candidate server evidence backup is missing")
    backup_path = backup.get("manifestPath")
    backup_bytes = backup.get("manifestBytes")
    backup_sha256 = backup.get("manifestSha256")
    if (
        not isinstance(backup_path, str)
        or isinstance(backup_bytes, bool)
        or not isinstance(backup_bytes, int)
        or backup_bytes <= 0
        or not isinstance(backup_sha256, str)
        or not SHA256_PATTERN.fullmatch(backup_sha256)
    ):
        raise ValueError("failed Candidate backup manifest identity is incomplete")
    _private_server_path(
        backup_path,
        ".json",
        "failed Candidate backup manifest path",
    )

    migration = evidence.get("migration")
    if not isinstance(migration, dict) or migration.get("status") != "completed":
        raise ValueError("failed Candidate migration evidence is not completed")
    revisions = [
        migration.get("preRevision"),
        migration.get("targetRevision"),
        migration.get("resultRevision"),
    ]
    if (
        any(not isinstance(revision, str) or not revision for revision in revisions)
        or len(set(revisions)) != 1
    ):
        raise ValueError("failed Candidate migration was not a no-op")

    fetch_status, _fetch_status_raw = _read_regular_json(
        args.failed_fetch_status,
        "failed Candidate fetch status",
    )
    fetch_exit_code = fetch_status.get("exitCode")
    if (
        set(fetch_status) != {"schemaVersion", "exitCode", "backendHealthyAttested"}
        or fetch_status.get("schemaVersion") != 1
        or isinstance(fetch_exit_code, bool)
        or not isinstance(fetch_exit_code, int)
        or not 1 <= fetch_exit_code <= 255
        or fetch_status.get("backendHealthyAttested") is not False
    ):
        raise ValueError("failed Candidate fetch status is not an unattested failure")
    _require_empty_regular_file(
        args.failed_preview,
        "failed Candidate preview metadata",
    )

    frontend_dir_metadata = args.failed_frontend_release_dir.lstat()
    if (
        args.failed_frontend_release_dir.is_symlink()
        or not stat.S_ISDIR(frontend_dir_metadata.st_mode)
    ):
        raise ValueError("failed Candidate frontend release directory is unsafe")
    manifest, _manifest_raw = _read_regular_json(
        args.failed_frontend_release_dir / "frontend-release.json",
        "failed Candidate frontend manifest",
    )
    release = manifest.get("release")
    source = manifest.get("source")
    artifact = manifest.get("artifact")
    frontend = manifest.get("frontend")
    if not all(isinstance(value, dict) for value in (release, source, artifact, frontend)):
        raise ValueError("failed Candidate frontend manifest is incomplete")
    assert isinstance(release, dict)
    assert isinstance(source, dict)
    assert isinstance(artifact, dict)
    assert isinstance(frontend, dict)
    if (
        manifest.get("schemaVersion") != 2
        or release.get("releaseId") != f"{args.run_id}-{args.run_attempt}"
        or release.get("environment") != "production"
        or release.get("repository") != args.repository
        or release.get("workflow") != "production-release"
        or release.get("workflowRunId") != str(args.run_id)
        or release.get("workflowRunAttempt") != str(args.run_attempt)
        or source.get("githubSha") != args.commit
        or source.get("deployCommit") != args.commit
    ):
        raise ValueError("failed Candidate frontend manifest release identity mismatch")
    artifact_name = f"frontend-dist-{args.commit}"
    payload_name = artifact.get("payload")
    payload_bytes = artifact.get("payloadBytes")
    if (
        artifact.get("name") != artifact_name
        or artifact.get("id") != frontend_identity
        or artifact.get("checksumAlgorithm") != "sha256"
        or artifact.get("checksum") != frontend_checksum
        or payload_name != "frontend-dist.tar.gz"
        or isinstance(payload_bytes, bool)
        or not isinstance(payload_bytes, int)
        or payload_bytes <= 0
    ):
        raise ValueError("failed Candidate frontend artifact identity mismatch")
    payload_path = args.failed_frontend_release_dir / payload_name
    payload_metadata = payload_path.lstat()
    if payload_path.is_symlink() or not stat.S_ISREG(payload_metadata.st_mode):
        raise ValueError("failed Candidate frontend payload must be a regular file")
    if (
        payload_metadata.st_size != payload_bytes
        or hashlib.sha256(payload_path.read_bytes()).hexdigest() != frontend_checksum
    ):
        raise ValueError("failed Candidate frontend payload identity mismatch")
    frontend_build_id = frontend.get("buildId")
    node_version = frontend.get("nodeVersion")
    if (
        not isinstance(frontend_build_id, str)
        or not SHA256_PATTERN.fullmatch(frontend_build_id)
        or not isinstance(node_version, str)
        or re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", node_version) is None
    ):
        raise ValueError("failed Candidate frontend build identity is invalid")

    artifacts = _load_artifacts(args.artifacts)
    candidate_artifact_name = (
        f"release-candidate-{args.commit}-{args.run_attempt}"
    )
    selected = {
        name: [item for item in artifacts if item.get("name") == name]
        for name in {candidate_artifact_name, artifact_name}
    }
    for name, matches in selected.items():
        if len(matches) != 1 or matches[0].get("expired") is not False:
            raise ValueError(f"exact non-expired artifact is unavailable: {name}")
    frontend_artifact = selected[artifact_name][0]
    github_artifact_id = frontend_artifact.get("id")
    github_artifact_digest = frontend_artifact.get("digest")
    if (
        isinstance(github_artifact_id, bool)
        or not isinstance(github_artifact_id, int)
        or github_artifact_id <= 0
        or not isinstance(github_artifact_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", github_artifact_digest) is None
    ):
        raise ValueError("failed Candidate frontend GitHub artifact identity is invalid")

    values = {
        "CANDIDATE_HANDOFF_SOURCE": "failed-github-artifact",
        "CANDIDATE_REPOSITORY": args.repository,
        "CANDIDATE_COMMIT_SHA": args.commit,
        "CANDIDATE_ARCHIVE_SHA256": args.archive_sha256,
        "CANDIDATE_ARCHIVE_BYTES": str(archive_bytes),
        "CANDIDATE_RUN_ID": str(args.run_id),
        "CANDIDATE_RUN_ATTEMPT": str(args.run_attempt),
        "CANDIDATE_RELEASE_ID": f"{args.run_id}-{args.run_attempt}",
        "CANDIDATE_REMOTE_ARCHIVE_PATH": (
            f".cache/jato-releases/archives/{args.commit}/"
            f"{args.archive_sha256}.tar.gz"
        ),
        "CANDIDATE_FRONTEND_ARTIFACT_NAME": artifact_name,
        "CANDIDATE_FRONTEND_ARTIFACT_IDENTITY": frontend_identity,
        "CANDIDATE_FRONTEND_ARTIFACT_CHECKSUM": frontend_checksum,
        "CANDIDATE_GITHUB_ARTIFACT_ID": str(github_artifact_id),
        "CANDIDATE_GITHUB_ARTIFACT_DIGEST": github_artifact_digest,
        "CANDIDATE_FRONTEND_BUILD_ID": frontend_build_id,
        "CANDIDATE_FRONTEND_NODE_VERSION": node_version,
        "CANDIDATE_SERVER_CHECKPOINT_PATH": checkpoint_path,
        "CANDIDATE_SERVER_CHECKPOINT_SHA256": hashlib.sha256(
            server_checkpoint_raw
        ).hexdigest(),
        "CANDIDATE_SERVER_EVIDENCE_PATH": evidence_path,
        "CANDIDATE_SERVER_EVIDENCE_SHA256": evidence_sha256,
        "CANDIDATE_SERVER_REVIEWED_CHECKPOINT_B64": base64.b64encode(
            server_checkpoint_raw
        ).decode("ascii"),
        "CANDIDATE_FAILED_FETCH_EXIT_CODE": str(fetch_exit_code),
    }
    _write_verified_environment(args, values, server_checkpoint_raw)


def verify_canonical_cleanup(args: argparse.Namespace) -> None:
    bundle, bundle_raw = _read_regular_json(
        args.canonical_server_bundle,
        "canonical Candidate cleanup bundle",
        maximum_bytes=3 * 1_048_576,
    )
    if bundle.get("schemaVersion") != 1 or bundle.get("source") != "canonical-server":
        raise ValueError("canonical Candidate cleanup bundle schema/source is invalid")
    request = bundle.get("request")
    if not isinstance(request, dict):
        raise ValueError("canonical Candidate cleanup request is missing")
    expected_request = {
        "repository": args.repository,
        "commit": args.commit,
        "archiveSha256": args.archive_sha256,
        "runId": args.run_id,
        "runAttempt": args.run_attempt,
        "cleanupMode": args.cleanup_mode,
        "requestedAttestationSha256": args.attestation_sha256,
    }
    if request != expected_request:
        raise ValueError("canonical Candidate cleanup request identity mismatch")
    reviewed_attestation = bundle.get("reviewedAttestation")
    if reviewed_attestation != {
        "sha256": args.attestation_sha256,
        "source": "reconstructed-from-canonical-journal",
    }:
        raise ValueError("canonical Candidate reviewed attestation proof is invalid")
    allowed_states = CANONICAL_CLEANUP_STATES.get(args.cleanup_mode)
    if allowed_states is None:
        raise ValueError("canonical Candidate cleanup mode is unsupported")

    checkpoint, checkpoint_raw, checkpoint_path, checkpoint_sha256 = (
        _canonical_content(bundle.get("checkpoint"), label="checkpoint")
    )
    evidence, _evidence_raw, evidence_path, evidence_sha256 = _canonical_content(
        bundle.get("evidence"),
        label="evidence",
    )
    checkpoint_suffix = (
        "/.local/state/jato-production-release/checkpoints/"
        f"{args.commit}/{args.archive_sha256}.json"
    )
    if not checkpoint_path.endswith(checkpoint_suffix):
        raise ValueError("canonical Candidate checkpoint path identity mismatch")
    if evidence_path != checkpoint_path.removesuffix(".json") + ".evidence.json":
        raise ValueError("canonical Candidate evidence path identity mismatch")

    identity = checkpoint.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("canonical Candidate checkpoint identity is missing")
    expected_identity_scalars = {
        "repository": args.repository,
        "commit": args.commit,
        "archiveSha256": args.archive_sha256,
        "runId": args.run_id,
        "runAttempt": args.run_attempt,
    }
    for key, expected in expected_identity_scalars.items():
        if identity.get(key) != expected:
            raise ValueError(f"canonical Candidate identity mismatch for {key}")
    archive_bytes = identity.get("archiveBytes")
    frontend_identity = identity.get("frontendIdentity")
    frontend_checksum = identity.get("frontendChecksum")
    if (
        isinstance(archive_bytes, bool)
        or not isinstance(archive_bytes, int)
        or archive_bytes <= 0
    ):
        raise ValueError("canonical Candidate archive byte count is invalid")
    if (
        not isinstance(frontend_identity, str)
        or frontend_identity
        != (
            f"gha://{args.repository}/actions/runs/{args.run_id}/attempts/"
            f"{args.run_attempt}/artifacts/frontend-dist-{args.commit}"
        )
    ):
        raise ValueError("canonical Candidate frontend identity is invalid")
    if not isinstance(frontend_checksum, str) or not SHA256_PATTERN.fullmatch(
        frontend_checksum
    ):
        raise ValueError("canonical Candidate frontend checksum is invalid")
    checkpoint_state = (checkpoint.get("phase"), checkpoint.get("retryClass"))
    if checkpoint.get("status") != "completed" or checkpoint_state not in allowed_states:
        raise ValueError("canonical Candidate checkpoint phase/status is ineligible")
    if evidence.get("identity") != identity:
        raise ValueError("canonical Candidate checkpoint/evidence identity mismatch")
    message = checkpoint.get("message")
    binding = re.search(
        r"(?:^|[; ])evidence_path=(\S+) "
        r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])",
        message if isinstance(message, str) else "",
    )
    if (
        binding is None
        or binding.group(1) != evidence_path
        or binding.group(2) != evidence_sha256
    ):
        raise ValueError("canonical Candidate checkpoint evidence binding mismatch")

    preview = bundle.get("candidatePreview")
    if (
        not isinstance(preview, dict)
        or preview.get("role") != "candidate"
        or preview.get("commitSha") != args.commit
        or preview.get("archiveSha256") != args.archive_sha256
        or preview.get("candidateSlot") not in {8000, 8001}
        or preview.get("previewPort") != 18002
    ):
        raise ValueError("canonical Candidate preview identity mismatch")
    candidate_slot = preview["candidateSlot"]
    preview_port = preview["previewPort"]
    if checkpoint.get("phase") == "candidate_ready":
        slot_port = re.search(r"(?:^|[; ])slot=(8000|8001) port=(18002)(?:$|[; ])", message)
        if (
            slot_port is None
            or int(slot_port.group(1)) != candidate_slot
            or int(slot_port.group(2)) != preview_port
        ):
            raise ValueError("canonical Candidate checkpoint slot/port binding mismatch")
    elif checkpoint.get("phase") == "active_updated":
        candidate_slot_binding = re.search(
            r"(?:^|[; ])candidate_slot=(8000|8001)(?:$|[; ])",
            message,
        )
        if (
            candidate_slot_binding is None
            or int(candidate_slot_binding.group(1)) != candidate_slot
        ):
            raise ValueError("canonical Candidate checkpoint slot binding mismatch")

    archive = bundle.get("archive")
    expected_archive_path = (
        f".cache/jato-releases/archives/{args.commit}/{args.archive_sha256}.tar.gz"
    )
    if (
        not isinstance(archive, dict)
        or archive.get("remotePath") != expected_archive_path
        or archive.get("sha256") != args.archive_sha256
        or archive.get("bytes") != archive_bytes
    ):
        raise ValueError("canonical Candidate archive identity mismatch")
    lock = bundle.get("productionLock")
    if (
        not isinstance(lock, dict)
        or not isinstance(lock.get("path"), str)
        or not lock["path"].endswith(
            "/.local/state/jato-production-release/production-deploy.lock"
        )
        or lock.get("mode") != "exclusive"
        or lock.get("held") is not True
    ):
        raise ValueError("canonical Candidate production lock proof is invalid")

    frontend = bundle.get("frontend")
    artifact_name = f"frontend-dist-{args.commit}"
    if not isinstance(frontend, dict):
        raise ValueError("canonical Candidate frontend proof is missing")
    github_artifact_id = frontend.get("githubArtifactId")
    github_artifact_digest = frontend.get("githubArtifactDigest")
    frontend_build_id = frontend.get("buildId")
    node_version = frontend.get("nodeVersion")
    if (
        frontend.get("artifactName") != artifact_name
        or frontend.get("artifactIdentity") != frontend_identity
        or frontend.get("artifactChecksum") != frontend_checksum
        or isinstance(github_artifact_id, bool)
        or not isinstance(github_artifact_id, int)
        or github_artifact_id <= 0
        or not isinstance(github_artifact_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", github_artifact_digest) is None
        or not isinstance(frontend_build_id, str)
        or not SHA256_PATTERN.fullmatch(frontend_build_id)
        or not isinstance(node_version, str)
        or re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", node_version) is None
    ):
        raise ValueError("canonical Candidate frontend identity is incomplete")

    canonical_proof_sha256 = hashlib.sha256(bundle_raw).hexdigest()
    values = {
        "CANDIDATE_HANDOFF_SOURCE": "canonical-server",
        "CANDIDATE_REPOSITORY": args.repository,
        "CANDIDATE_COMMIT_SHA": args.commit,
        "CANDIDATE_ARCHIVE_SHA256": args.archive_sha256,
        "CANDIDATE_ARCHIVE_BYTES": str(archive_bytes),
        "CANDIDATE_RUN_ID": str(args.run_id),
        "CANDIDATE_RUN_ATTEMPT": str(args.run_attempt),
        "CANDIDATE_RELEASE_ID": f"{args.run_id}-{args.run_attempt}",
        "CANDIDATE_REMOTE_ARCHIVE_PATH": expected_archive_path,
        "CANDIDATE_FRONTEND_ARTIFACT_NAME": artifact_name,
        "CANDIDATE_FRONTEND_ARTIFACT_IDENTITY": frontend_identity,
        "CANDIDATE_FRONTEND_ARTIFACT_CHECKSUM": frontend_checksum,
        "CANDIDATE_GITHUB_ARTIFACT_ID": str(github_artifact_id),
        "CANDIDATE_GITHUB_ARTIFACT_DIGEST": github_artifact_digest,
        "CANDIDATE_FRONTEND_BUILD_ID": frontend_build_id,
        "CANDIDATE_FRONTEND_NODE_VERSION": node_version,
        "CANDIDATE_ATTESTATION_SHA256": args.attestation_sha256,
        "CANDIDATE_CANONICAL_PROOF_SHA256": canonical_proof_sha256,
        "CANDIDATE_SERVER_CHECKPOINT_PATH": checkpoint_path,
        "CANDIDATE_SERVER_CHECKPOINT_SHA256": checkpoint_sha256,
        "CANDIDATE_SERVER_EVIDENCE_PATH": evidence_path,
        "CANDIDATE_SERVER_EVIDENCE_SHA256": evidence_sha256,
        "CANDIDATE_SLOT": str(candidate_slot),
        "CANDIDATE_PREVIEW_PORT": str(preview_port),
    }
    _write_verified_environment(args, values, checkpoint_raw)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--checkpoint", type=Path)
    result.add_argument("--attestation", type=Path)
    result.add_argument("--artifacts", type=Path)
    result.add_argument("--canonical-server-bundle", type=Path)
    result.add_argument("--failed-transport-checkpoint", type=Path)
    result.add_argument("--failed-transport-journal", type=Path)
    result.add_argument("--failed-server-checkpoint", type=Path)
    result.add_argument("--failed-server-evidence", type=Path)
    result.add_argument("--failed-fetch-status", type=Path)
    result.add_argument("--failed-preview", type=Path)
    result.add_argument("--failed-frontend-release-dir", type=Path)
    result.add_argument("--cleanup-mode", choices=tuple(CANONICAL_CLEANUP_STATES))
    result.add_argument("--env-output", type=Path, required=True)
    result.add_argument("--reviewed-checkpoint-output", type=Path)
    result.add_argument("--repository", required=True)
    result.add_argument("--commit", required=True)
    result.add_argument("--archive-sha256", required=True)
    result.add_argument("--run-id", type=int, required=True)
    result.add_argument("--run-attempt", type=int, required=True)
    result.add_argument("--attestation-sha256")
    return result


def main() -> int:
    args = parser().parse_args()
    if COMMIT_PATTERN.fullmatch(args.commit) is None:
        raise SystemExit("Candidate commit SHA is invalid")
    if SHA256_PATTERN.fullmatch(args.archive_sha256) is None:
        raise SystemExit("Candidate archive SHA-256 is invalid")
    if args.run_id <= 0 or args.run_attempt <= 0:
        raise SystemExit("Candidate run identity must use positive integers")
    failed_inputs = (
        args.failed_transport_checkpoint,
        args.failed_transport_journal,
        args.failed_server_checkpoint,
        args.failed_server_evidence,
        args.failed_fetch_status,
        args.failed_preview,
        args.failed_frontend_release_dir,
    )
    failed_mode = any(path is not None for path in failed_inputs)
    try:
        if failed_mode:
            if any(path is None for path in failed_inputs) or args.artifacts is None:
                raise ValueError("failed Candidate handoff inputs are incomplete")
            if any(
                value is not None
                for value in (
                    args.checkpoint,
                    args.attestation,
                    args.canonical_server_bundle,
                    args.cleanup_mode,
                    args.attestation_sha256,
                )
            ):
                raise ValueError(
                    "failed Candidate handoff cannot mix attestation or canonical inputs"
                )
            if args.reviewed_checkpoint_output is None:
                raise ValueError(
                    "failed Candidate handoff requires a reviewed checkpoint output"
                )
            verify_failed_handoff(args)
        elif args.canonical_server_bundle is not None:
            if (
                args.attestation_sha256 is None
                or SHA256_PATTERN.fullmatch(args.attestation_sha256) is None
            ):
                raise ValueError("Candidate attestation SHA-256 is invalid")
            if args.cleanup_mode is None:
                raise ValueError("canonical Candidate cleanup mode is required")
            if any(
                path is not None
                for path in (args.checkpoint, args.attestation, args.artifacts)
            ):
                raise ValueError(
                    "canonical Candidate cleanup cannot mix GitHub artifact inputs"
                )
            if args.reviewed_checkpoint_output is None:
                raise ValueError(
                    "canonical Candidate cleanup requires a reviewed checkpoint output"
                )
            verify_canonical_cleanup(args)
        else:
            if (
                args.attestation_sha256 is None
                or SHA256_PATTERN.fullmatch(args.attestation_sha256) is None
            ):
                raise ValueError("Candidate attestation SHA-256 is invalid")
            if args.cleanup_mode is not None:
                raise ValueError(
                    "cleanup mode is only valid with a canonical server bundle"
                )
            if any(
                path is None
                for path in (args.checkpoint, args.attestation, args.artifacts)
            ):
                raise ValueError("GitHub Candidate handoff inputs are incomplete")
            verify(args)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
