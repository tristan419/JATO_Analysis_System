from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import stat
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
