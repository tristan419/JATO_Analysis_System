from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import stat
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER_PATH = REPO_ROOT / "03_Scripts/deploy/verify_candidate_handoff.py"


def _load_helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("verify_candidate_handoff", HELPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load verify_candidate_handoff helper")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HELPER = _load_helper()


def _write_json(path: Path, payload: Any) -> bytes:
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode()
    path.write_bytes(raw)
    return raw


def _fixture(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
) -> argparse.Namespace:
    commit = "a" * 40
    archive_sha256 = "b" * 64
    evidence_sha256 = "c" * 64
    checkpoint_path = (
        "/home/deploy/.local/state/jato-production-release/checkpoints/"
        f"{commit}/{archive_sha256}.json"
    )
    evidence_path = checkpoint_path.removesuffix(".json") + ".evidence.json"
    identity = {
        "repository": "tristan419/JATO_Analysis_System",
        "commit": commit,
        "archiveSha256": archive_sha256,
        "archiveBytes": 123,
        "runId": 42,
        "runAttempt": 3,
        "frontendIdentity": "frontend-identity",
        "frontendChecksum": "d" * 64,
    }
    checkpoint = {
        "identity": identity,
        "phase": "candidate_ready",
        "status": "completed",
        "retryClass": "inspect_then_resume",
        "message": (
            "Candidate ready; "
            f"evidence_path={evidence_path} evidence_sha256={evidence_sha256}"
        ),
    }
    attestation: dict[str, Any] = {
        "identity": identity,
        "releaseId": "42-3",
        "releaseMode": "prepare-candidate",
        "serverCheckpoint": {
            "remotePath": checkpoint_path,
            "phase": "candidate_ready",
            "status": "completed",
        },
        "serverEvidence": {
            "remotePath": evidence_path,
            "sha256": evidence_sha256,
        },
        "candidatePreview": {
            "role": "candidate",
            "commitSha": commit,
            "archiveSha256": archive_sha256,
            "candidateSlot": 8001,
            "previewPort": 18002,
        },
        "approvalHandoff": {
            "workflow": "production-release",
            "remoteArchivePath": (
                f".cache/jato-releases/archives/{commit}/{archive_sha256}.tar.gz"
            ),
            "frontendArtifactName": f"frontend-dist-{commit}",
            "frontendGithubArtifactId": 77,
            "frontendGithubArtifactDigest": "sha256:" + "e" * 64,
            "frontendBuildId": "f" * 64,
            "frontendNodeVersion": "v20.19.0",
        },
    }
    if mutate is not None:
        mutate(checkpoint, attestation)
    checkpoint_file = tmp_path / "candidate.json"
    checkpoint_raw = _write_json(checkpoint_file, checkpoint)
    attestation["serverCheckpoint"]["sha256"] = hashlib.sha256(
        checkpoint_raw
    ).hexdigest()
    attestation_file = tmp_path / "attestation.json"
    attestation_raw = _write_json(attestation_file, attestation)
    artifacts_file = tmp_path / "artifacts.json"
    _write_json(
        artifacts_file,
        [
            {
                "artifacts": [
                    {
                        "name": f"release-candidate-{commit}-3",
                        "expired": False,
                    },
                    {
                        "name": f"frontend-dist-{commit}",
                        "expired": False,
                        "id": 77,
                        "digest": "sha256:" + "e" * 64,
                    },
                ]
            }
        ],
    )
    return argparse.Namespace(
        checkpoint=checkpoint_file,
        attestation=attestation_file,
        artifacts=artifacts_file,
        env_output=tmp_path / "verified.env",
        repository="tristan419/JATO_Analysis_System",
        commit=commit,
        archive_sha256=archive_sha256,
        run_id=42,
        run_attempt=3,
        attestation_sha256=hashlib.sha256(attestation_raw).hexdigest(),
        reviewed_checkpoint_output=None,
    )


def test_verified_env_binds_expected_server_evidence(tmp_path: Path) -> None:
    args = _fixture(tmp_path)

    HELPER.verify(args)

    env = args.env_output.read_text(encoding="utf-8")
    checkpoint_path = (
        "/home/deploy/.local/state/jato-production-release/checkpoints/"
        f"{'a' * 40}/{'b' * 64}.json"
    )
    assert f"CANDIDATE_SERVER_CHECKPOINT_PATH={checkpoint_path}\n" in env
    assert "CANDIDATE_SERVER_CHECKPOINT_SHA256=" in env
    assert f"CANDIDATE_SERVER_EVIDENCE_PATH={checkpoint_path[:-5]}.evidence.json\n" in env
    assert f"CANDIDATE_SERVER_EVIDENCE_SHA256={'c' * 64}\n" in env
    assert "CANDIDATE_HANDOFF_SOURCE=github-artifact\n" in env
    assert "CANDIDATE_SLOT=8001\n" in env
    assert "CANDIDATE_PREVIEW_PORT=18002\n" in env
    assert stat.S_IMODE(args.env_output.stat().st_mode) == 0o600


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda _checkpoint, attestation: attestation["serverEvidence"].__setitem__(
                "remotePath", ".local/state/jato-production-release/checkpoints/wrong"
            ),
            "Candidate server evidence path is unsafe",
        ),
        (
            lambda _checkpoint, attestation: attestation["serverEvidence"].__setitem__(
                "sha256", "not-a-digest"
            ),
            "Candidate server evidence attestation mismatch",
        ),
        (
            lambda checkpoint, _attestation: checkpoint.__setitem__(
                "message",
                checkpoint["message"].replace("c" * 64, "0" * 64),
            ),
            "Candidate checkpoint evidence binding mismatch",
        ),
    ],
)
def test_rejects_invalid_server_evidence_binding(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any], dict[str, Any]], None],
    message: str,
) -> None:
    args = _fixture(tmp_path, mutate)

    with pytest.raises(ValueError, match=message):
        HELPER.verify(args)


def _failed_fixture(tmp_path: Path) -> tuple[argparse.Namespace, dict[str, Path]]:
    commit = "a" * 40
    archive_sha256 = "b" * 64
    repository = "tristan419/JATO_Analysis_System"
    run_id = 42
    run_attempt = 3
    frontend_artifact_name = f"frontend-dist-{commit}"
    frontend_identity = (
        f"gha://{repository}/actions/runs/{run_id}/attempts/{run_attempt}/"
        f"artifacts/{frontend_artifact_name}"
    )
    frontend_dir = tmp_path / "frontend-release"
    frontend_dir.mkdir()
    frontend_payload = b"immutable frontend payload\n"
    frontend_checksum = hashlib.sha256(frontend_payload).hexdigest()
    frontend_payload_path = frontend_dir / "frontend-dist.tar.gz"
    frontend_payload_path.write_bytes(frontend_payload)
    frontend_manifest = {
        "schemaVersion": 2,
        "release": {
            "releaseId": f"{run_id}-{run_attempt}",
            "environment": "production",
            "repository": repository,
            "workflow": "production-release",
            "workflowRunId": str(run_id),
            "workflowRunAttempt": str(run_attempt),
            "buildTimestamp": "2026-08-05T01:02:03Z",
        },
        "source": {
            "githubSha": commit,
            "appCommit": commit,
            "deployCommit": commit,
            "deployCommitSemantics": "test",
        },
        "artifact": {
            "name": frontend_artifact_name,
            "id": frontend_identity,
            "payload": "frontend-dist.tar.gz",
            "payloadBytes": len(frontend_payload),
            "checksumAlgorithm": "sha256",
            "checksum": frontend_checksum,
        },
        "frontend": {
            "buildMetaPath": "build-meta.json",
            "buildId": "d" * 64,
            "buildIdSemantics": "test",
            "nodeVersion": "v20.19.0",
        },
        "edgeFunctions": {},
    }
    frontend_manifest_path = frontend_dir / "frontend-release.json"
    _write_json(frontend_manifest_path, frontend_manifest)

    identity = {
        "repository": repository,
        "commit": commit,
        "archiveSha256": archive_sha256,
        "archiveBytes": 123,
        "runId": run_id,
        "runAttempt": run_attempt,
        "frontendIdentity": frontend_identity,
        "frontendChecksum": frontend_checksum,
    }
    transport = {
        "schemaVersion": 1,
        "identity": identity,
        "phase": "transport_verified",
        "status": "completed",
        "retryClass": "inspect_then_resume",
        "sequence": 1,
        "message": "Immutable SSH archive size and SHA-256 verified on Tencent",
        "updatedAt": "2026-08-05T01:03:00Z",
    }
    transport_path = tmp_path / "candidate.json"
    _write_json(transport_path, transport)
    journal_path = tmp_path / "journal.jsonl"
    transport_event = dict(transport)
    transport_event["event"] = "checkpoint_transition"
    journal_path.write_text(
        json.dumps(transport_event, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    checkpoint_path = (
        "/home/deploy/.local/state/jato-production-release/checkpoints/"
        f"{commit}/{archive_sha256}.json"
    )
    evidence_path = checkpoint_path.removesuffix(".json") + ".evidence.json"
    revision = "20260715_0046 (head)"
    evidence = {
        "identity": identity,
        "backup": {
            "manifestPath": "/opt/backups/jato/manifests/backup-20260805.json",
            "manifestBytes": 1707,
            "manifestSha256": "e" * 64,
        },
        "migration": {
            "preRevision": revision,
            "targetRevision": revision,
            "resultRevision": revision,
            "status": "completed",
        },
    }
    evidence_file = tmp_path / "backend-healthy.evidence.json"
    evidence_raw = _write_json(evidence_file, evidence)
    evidence_sha256 = hashlib.sha256(evidence_raw).hexdigest()
    server_checkpoint = {
        "schemaVersion": 1,
        "identity": identity,
        "phase": "migrated",
        "status": "completed",
        "retryClass": "automatic",
        "sequence": 6,
        "message": (
            "database schema already matches candidate heads; "
            f"evidence_path={evidence_path} evidence_sha256={evidence_sha256}"
        ),
        "updatedAt": "2026-08-05T01:04:00Z",
    }
    server_checkpoint_path = tmp_path / "backend-healthy.json"
    _write_json(server_checkpoint_path, server_checkpoint)
    fetch_status_path = tmp_path / "fetch-status.json"
    _write_json(
        fetch_status_path,
        {
            "schemaVersion": 1,
            "exitCode": 7,
            "backendHealthyAttested": False,
        },
    )
    preview_path = tmp_path / "candidate-preview.json"
    preview_path.write_bytes(b"")
    artifacts_path = tmp_path / "artifacts.json"
    _write_json(
        artifacts_path,
        {
            "artifacts": [
                {
                    "name": f"release-candidate-{commit}-{run_attempt}",
                    "expired": False,
                    "id": 76,
                    "digest": "sha256:" + "f" * 64,
                },
                {
                    "name": frontend_artifact_name,
                    "expired": False,
                    "id": 77,
                    "digest": "sha256:" + "0" * 64,
                },
            ]
        },
    )
    paths = {
        "transport": transport_path,
        "journal": journal_path,
        "server_checkpoint": server_checkpoint_path,
        "evidence": evidence_file,
        "fetch_status": fetch_status_path,
        "preview": preview_path,
        "frontend_dir": frontend_dir,
        "frontend_manifest": frontend_manifest_path,
        "artifacts": artifacts_path,
    }
    args = argparse.Namespace(
        failed_transport_checkpoint=transport_path,
        failed_transport_journal=journal_path,
        failed_server_checkpoint=server_checkpoint_path,
        failed_server_evidence=evidence_file,
        failed_fetch_status=fetch_status_path,
        failed_preview=preview_path,
        failed_frontend_release_dir=frontend_dir,
        artifacts=artifacts_path,
        env_output=tmp_path / "failed-verified.env",
        reviewed_checkpoint_output=tmp_path / "reviewed-failed-candidate.json",
        repository=repository,
        commit=commit,
        archive_sha256=archive_sha256,
        run_id=run_id,
        run_attempt=run_attempt,
    )
    return args, paths


def test_failed_handoff_binds_migrated_failure_without_attestation_or_slot(
    tmp_path: Path,
) -> None:
    args, _paths = _failed_fixture(tmp_path)

    HELPER.verify_failed_handoff(args)

    env = args.env_output.read_text(encoding="utf-8")
    assert "CANDIDATE_HANDOFF_SOURCE=failed-github-artifact\n" in env
    assert "CANDIDATE_FAILED_FETCH_EXIT_CODE=7\n" in env
    assert "CANDIDATE_SERVER_CHECKPOINT_SHA256=" in env
    assert "CANDIDATE_SERVER_EVIDENCE_SHA256=" in env
    assert "CANDIDATE_SERVER_REVIEWED_CHECKPOINT_B64=" in env
    assert "CANDIDATE_ATTESTATION_SHA256" not in env
    assert "CANDIDATE_SLOT" not in env
    reviewed = args.reviewed_checkpoint_output.read_bytes()
    server_checkpoint = args.failed_server_checkpoint.read_bytes()
    assert reviewed == server_checkpoint
    encoded = next(
        line.removeprefix("CANDIDATE_SERVER_REVIEWED_CHECKPOINT_B64=").strip()
        for line in env.splitlines()
        if line.startswith("CANDIDATE_SERVER_REVIEWED_CHECKPOINT_B64=")
    )
    assert base64.b64decode(encoded) == server_checkpoint
    assert stat.S_IMODE(args.env_output.stat().st_mode) == 0o600
    assert stat.S_IMODE(args.reviewed_checkpoint_output.stat().st_mode) == 0o600


def _rewrite_failed_evidence(
    paths: dict[str, Path],
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    evidence = json.loads(paths["evidence"].read_text(encoding="utf-8"))
    mutate(evidence)
    evidence_raw = _write_json(paths["evidence"], evidence)
    server = json.loads(paths["server_checkpoint"].read_text(encoding="utf-8"))
    server["message"] = server["message"].replace(
        server["message"].rsplit("=", 1)[1],
        hashlib.sha256(evidence_raw).hexdigest(),
    )
    _write_json(paths["server_checkpoint"], server)


def test_failed_handoff_rejects_non_noop_migration(tmp_path: Path) -> None:
    args, paths = _failed_fixture(tmp_path)
    _rewrite_failed_evidence(
        paths,
        lambda evidence: evidence["migration"].__setitem__(
            "resultRevision",
            "unexpected-head",
        ),
    )

    with pytest.raises(ValueError, match="migration was not a no-op"):
        HELPER.verify_failed_handoff(args)


def test_failed_handoff_rejects_incomplete_backup_identity(tmp_path: Path) -> None:
    args, paths = _failed_fixture(tmp_path)
    _rewrite_failed_evidence(
        paths,
        lambda evidence: evidence["backup"].__setitem__("manifestBytes", 0),
    )

    with pytest.raises(ValueError, match="backup manifest identity is incomplete"):
        HELPER.verify_failed_handoff(args)


def test_failed_handoff_rejects_non_migrated_server_checkpoint(tmp_path: Path) -> None:
    args, paths = _failed_fixture(tmp_path)
    checkpoint = json.loads(
        paths["server_checkpoint"].read_text(encoding="utf-8")
    )
    checkpoint["phase"] = "candidate_ready"
    _write_json(paths["server_checkpoint"], checkpoint)

    with pytest.raises(ValueError, match="not migrated/completed/automatic"):
        HELPER.verify_failed_handoff(args)


@pytest.mark.parametrize(
    ("fetch_status", "message"),
    [
        (
            {
                "schemaVersion": 1,
                "exitCode": 0,
                "backendHealthyAttested": False,
            },
            "fetch status is not an unattested failure",
        ),
        (
            {
                "schemaVersion": 1,
                "exitCode": 7,
                "backendHealthyAttested": True,
            },
            "fetch status is not an unattested failure",
        ),
    ],
)
def test_failed_handoff_rejects_attested_or_successful_fetch(
    tmp_path: Path,
    fetch_status: dict[str, Any],
    message: str,
) -> None:
    args, paths = _failed_fixture(tmp_path)
    _write_json(paths["fetch_status"], fetch_status)

    with pytest.raises(ValueError, match=message):
        HELPER.verify_failed_handoff(args)


def test_failed_handoff_rejects_nonempty_preview(tmp_path: Path) -> None:
    args, paths = _failed_fixture(tmp_path)
    paths["preview"].write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="preview metadata must be empty"):
        HELPER.verify_failed_handoff(args)


def test_failed_handoff_rejects_duplicate_release_artifact(tmp_path: Path) -> None:
    args, paths = _failed_fixture(tmp_path)
    artifacts = json.loads(paths["artifacts"].read_text(encoding="utf-8"))
    artifacts["artifacts"].append(dict(artifacts["artifacts"][0]))
    _write_json(paths["artifacts"], artifacts)

    with pytest.raises(ValueError, match="exact non-expired artifact is unavailable"):
        HELPER.verify_failed_handoff(args)


def test_failed_handoff_rejects_frontend_run_identity_mismatch(tmp_path: Path) -> None:
    args, paths = _failed_fixture(tmp_path)
    manifest = json.loads(paths["frontend_manifest"].read_text(encoding="utf-8"))
    manifest["release"]["workflowRunId"] = "999"
    _write_json(paths["frontend_manifest"], manifest)

    with pytest.raises(ValueError, match="frontend manifest release identity mismatch"):
        HELPER.verify_failed_handoff(args)


def test_failed_handoff_rejects_transport_journal_mismatch(tmp_path: Path) -> None:
    args, paths = _failed_fixture(tmp_path)
    paths["journal"].write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="transport journal does not exactly bind"):
        HELPER.verify_failed_handoff(args)


def test_failed_handoff_cli_rejects_attestation_mixing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args, _paths = _failed_fixture(tmp_path)
    argv = [
        str(HELPER_PATH),
        "--failed-transport-checkpoint",
        str(args.failed_transport_checkpoint),
        "--failed-transport-journal",
        str(args.failed_transport_journal),
        "--failed-server-checkpoint",
        str(args.failed_server_checkpoint),
        "--failed-server-evidence",
        str(args.failed_server_evidence),
        "--failed-fetch-status",
        str(args.failed_fetch_status),
        "--failed-preview",
        str(args.failed_preview),
        "--failed-frontend-release-dir",
        str(args.failed_frontend_release_dir),
        "--artifacts",
        str(args.artifacts),
        "--env-output",
        str(args.env_output),
        "--reviewed-checkpoint-output",
        str(args.reviewed_checkpoint_output),
        "--repository",
        args.repository,
        "--commit",
        args.commit,
        "--archive-sha256",
        args.archive_sha256,
        "--run-id",
        str(args.run_id),
        "--run-attempt",
        str(args.run_attempt),
        "--attestation-sha256",
        "9" * 64,
    ]
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit, match="cannot mix attestation or canonical"):
        HELPER.main()


def _canonical_fixture(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None] | None = None,
    *,
    cleanup_mode: str = "discard-candidate",
    checkpoint_phase: str | None = None,
) -> argparse.Namespace:
    commit = "a" * 40
    archive_sha256 = "b" * 64
    requested_attestation = "d" * 64
    checkpoint_path = (
        "/home/deploy/.local/state/jato-production-release/checkpoints/"
        f"{commit}/{archive_sha256}.json"
    )
    evidence_path = checkpoint_path.removesuffix(".json") + ".evidence.json"
    frontend_identity = (
        "gha://tristan419/JATO_Analysis_System/actions/runs/42/attempts/3/"
        f"artifacts/frontend-dist-{commit}"
    )
    identity = {
        "repository": "tristan419/JATO_Analysis_System",
        "commit": commit,
        "archiveSha256": archive_sha256,
        "archiveBytes": 123,
        "runId": 42,
        "runAttempt": 3,
        "frontendIdentity": frontend_identity,
        "frontendChecksum": "e" * 64,
    }
    evidence = {"identity": identity, "migration": {"status": "not_required"}}
    evidence_raw = (json.dumps(evidence, sort_keys=True) + "\n").encode()
    evidence_sha256 = hashlib.sha256(evidence_raw).hexdigest()
    phase = checkpoint_phase or "candidate_ready"
    retry_class = "inspect_then_resume"
    message = (
        "Candidate ready; slot=8001 port=18002; "
        f"evidence_path={evidence_path} evidence_sha256={evidence_sha256}"
    )
    if phase == "rollback_completed":
        retry_class = "automatic"
        message = (
            "Previous Active restored; "
            f"evidence_path={evidence_path} evidence_sha256={evidence_sha256}"
        )
    elif cleanup_mode == "release-candidate":
        phase = "active_updated"
        message = (
            "Active updated; active_slot=8000 candidate_slot=8001; "
            f"evidence_path={evidence_path} evidence_sha256={evidence_sha256}"
        )
    checkpoint = {
        "identity": identity,
        "phase": phase,
        "status": "completed",
        "retryClass": retry_class,
        "message": message,
    }
    checkpoint_raw = (json.dumps(checkpoint, sort_keys=True) + "\n").encode()
    bundle: dict[str, Any] = {
        "schemaVersion": 1,
        "source": "canonical-server",
        "request": {
            "repository": "tristan419/JATO_Analysis_System",
            "commit": commit,
            "archiveSha256": archive_sha256,
            "runId": 42,
            "runAttempt": 3,
            "cleanupMode": cleanup_mode,
            "requestedAttestationSha256": requested_attestation,
        },
        "reviewedAttestation": {
            "sha256": requested_attestation,
            "source": "reconstructed-from-canonical-journal",
        },
        "productionLock": {
            "path": (
                "/home/deploy/.local/state/jato-production-release/"
                "production-deploy.lock"
            ),
            "mode": "exclusive",
            "held": True,
        },
        "checkpoint": {
            "remotePath": checkpoint_path,
            "sha256": hashlib.sha256(checkpoint_raw).hexdigest(),
            "contentBase64": base64.b64encode(checkpoint_raw).decode(),
        },
        "evidence": {
            "remotePath": evidence_path,
            "sha256": evidence_sha256,
            "contentBase64": base64.b64encode(evidence_raw).decode(),
        },
        "candidatePreview": {
            "role": "candidate",
            "commitSha": commit,
            "archiveSha256": archive_sha256,
            "candidateSlot": 8001,
            "previewPort": 18002,
        },
        "archive": {
            "remotePath": (
                f".cache/jato-releases/archives/{commit}/{archive_sha256}.tar.gz"
            ),
            "bytes": 123,
            "sha256": archive_sha256,
        },
        "frontend": {
            "artifactName": f"frontend-dist-{commit}",
            "artifactIdentity": frontend_identity,
            "artifactChecksum": "e" * 64,
            "githubArtifactId": 77,
            "githubArtifactDigest": "sha256:" + "f" * 64,
            "buildId": "0" * 64,
            "nodeVersion": "v20.19.0",
        },
    }
    if mutate is not None:
        mutate(bundle)
    bundle_file = tmp_path / "canonical-server.json"
    _write_json(bundle_file, bundle)
    return argparse.Namespace(
        canonical_server_bundle=bundle_file,
        cleanup_mode=cleanup_mode,
        env_output=tmp_path / "verified.env",
        reviewed_checkpoint_output=tmp_path / "reviewed-candidate.json",
        repository="tristan419/JATO_Analysis_System",
        commit=commit,
        archive_sha256=archive_sha256,
        run_id=42,
        run_attempt=3,
        attestation_sha256=requested_attestation,
    )


@pytest.mark.parametrize("cleanup_mode", ["discard-candidate", "release-candidate"])
def test_canonical_server_cleanup_binds_exact_live_candidate(
    tmp_path: Path,
    cleanup_mode: str,
) -> None:
    args = _canonical_fixture(tmp_path, cleanup_mode=cleanup_mode)

    HELPER.verify_canonical_cleanup(args)

    env = args.env_output.read_text(encoding="utf-8")
    assert "CANDIDATE_HANDOFF_SOURCE=canonical-server\n" in env
    assert "CANDIDATE_SLOT=8001\n" in env
    assert "CANDIDATE_PREVIEW_PORT=18002\n" in env
    assert f"CANDIDATE_ATTESTATION_SHA256={args.attestation_sha256}\n" in env
    assert "CANDIDATE_CANONICAL_PROOF_SHA256=" in env
    assert args.reviewed_checkpoint_output.read_text(encoding="utf-8")
    assert stat.S_IMODE(args.env_output.stat().st_mode) == 0o600
    assert stat.S_IMODE(args.reviewed_checkpoint_output.stat().st_mode) == 0o600


def test_canonical_server_cleanup_accepts_exact_rollback_retained_candidate(
    tmp_path: Path,
) -> None:
    args = _canonical_fixture(tmp_path, checkpoint_phase="rollback_completed")

    HELPER.verify_canonical_cleanup(args)

    env = args.env_output.read_text(encoding="utf-8")
    assert "CANDIDATE_HANDOFF_SOURCE=canonical-server\n" in env
    assert "CANDIDATE_SLOT=8001\n" in env


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda bundle: bundle["candidatePreview"].__setitem__(
                "candidateSlot", 8000
            ),
            "checkpoint slot/port binding mismatch",
        ),
        (
            lambda bundle: bundle["archive"].__setitem__("bytes", 124),
            "archive identity mismatch",
        ),
        (
            lambda bundle: bundle["productionLock"].__setitem__("held", False),
            "production lock proof is invalid",
        ),
        (
            lambda bundle: bundle["reviewedAttestation"].__setitem__(
                "sha256", "9" * 64
            ),
            "reviewed attestation proof is invalid",
        ),
    ],
)
def test_canonical_server_cleanup_rejects_unbound_state(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
    message: str,
) -> None:
    args = _canonical_fixture(tmp_path, mutate)

    with pytest.raises(ValueError, match=message):
        HELPER.verify_canonical_cleanup(args)
