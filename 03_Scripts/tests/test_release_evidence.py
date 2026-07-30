from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "03_Scripts" / "deploy"))

import release_checkpoint as checkpoint  # noqa: E402
import release_evidence as evidence  # noqa: E402


def _private_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)


def _fixture(
    tmp_path: Path,
    *,
    phase: str = "migrated",
    migration_status: str = "completed",
    database_enabled: bool = True,
    database_required: bool = True,
    pre_revision: str = "20260707_0042",
) -> dict[str, object]:
    identity = checkpoint.ReleaseIdentity.create(
        repository="example/JATO_Analysis_System",
        commit="a" * 40,
        archive_sha256="b" * 64,
        archive_bytes=22_000_000,
        run_id=123456,
        run_attempt=2,
        frontend_identity="gha://example/JATO_Analysis_System/frontend",
        frontend_checksum="c" * 64,
    )
    backup_root = tmp_path / "backups"
    manifests_root = backup_root / "manifests"
    dumps_root = backup_root / "pg"
    for directory in (backup_root, manifests_root, dumps_root):
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)

    dump = dumps_root / "jato-20260722-120000.dump"
    _private_write(dump, b"private verified database dump")
    manifest = manifests_root / "backup-20260722-120000.json"
    manifest_payload = {
        "createdAt": "2026-07-22T04:00:00+00:00",
        "database": {
            "enabled": database_enabled,
            "required": database_required,
            "status": (
                "completed"
                if database_enabled or database_required
                else "skipped"
            ),
            "dumpPath": (
                str(dump)
                if database_enabled or database_required
                else None
            ),
            "dumpBytes": (
                dump.stat().st_size
                if database_enabled or database_required
                else 0
            ),
            "dumpSha256": (
                hashlib.sha256(dump.read_bytes()).hexdigest()
                if database_enabled or database_required
                else None
            ),
        },
    }
    _private_write(
        manifest,
        (json.dumps(manifest_payload, sort_keys=True) + "\n").encode(),
    )

    checkpoint_path = (
        tmp_path
        / "checkpoints"
        / identity.commit
        / f"{identity.archiveSha256}.json"
    )
    evidence_path = checkpoint_path.with_name(
        f"{identity.archiveSha256}.evidence.json"
    )
    evidence_payload = {
        "identity": identity.to_dict(),
        "backup": {
            "manifestPath": str(manifest),
            "manifestBytes": manifest.stat().st_size,
            "manifestSha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        },
        "migration": {
            "status": migration_status,
            "preRevision": pre_revision if migration_status != "not_required" else None,
            "targetRevision": "20260709_0043 (head)" if migration_status != "not_required" else None,
            "resultRevision": (
                "20260709_0043 (head)" if migration_status == "completed" else None
            ),
        },
    }
    _private_write(
        evidence_path,
        (json.dumps(evidence_payload, sort_keys=True) + "\n").encode(),
    )
    evidence_sha256 = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    checkpoint.write_checkpoint(
        checkpoint_path=checkpoint_path,
        journal_path=tmp_path / "journals" / "release.jsonl",
        identity=identity,
        phase=phase,
        status="completed",
        retry_class="automatic",
        message=(
            f"evidence_path={evidence_path} "
            f"evidence_sha256={evidence_sha256}"
        ),
        now="2026-07-22T04:00:00.000Z",
    )
    return {
        "identity": identity,
        "backup_root": backup_root,
        "manifest": manifest,
        "dump": dump,
        "checkpoint": checkpoint_path,
        "evidence": evidence_path,
    }


def _verify(fixture: dict[str, object]) -> dict[str, str]:
    return evidence.verify_release_evidence(
        checkpoint_path=fixture["checkpoint"],
        evidence_path=fixture["evidence"],
        backup_root=fixture["backup_root"],
        expected_identity=fixture["identity"],
        backup_owner_uid=os.getuid(),
    )


def test_private_evidence_chain_verifies_without_exposing_paths(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = _verify(fixture)

    assert result["status"] == "verified"
    assert result["databaseBackup"] == "completed"
    assert result["migrationStatus"] == "completed"
    assert not any("/" in value for value in result.values())
    assert fixture["manifest"].stat().st_mode & 0o777 == 0o600
    assert fixture["dump"].stat().st_mode & 0o777 == 0o600


def test_enabled_database_accepts_completed_read_only_verification(
    tmp_path: Path,
) -> None:
    fixture = _fixture(
        tmp_path,
        database_required=False,
        pre_revision="20260709_0043 (head)",
    )

    result = _verify(fixture)

    assert result["databaseBackup"] == "completed"
    assert result["migrationStatus"] == "completed"


def test_disabled_database_accepts_not_required_migration_evidence(
    tmp_path: Path,
) -> None:
    fixture = _fixture(
        tmp_path,
        migration_status="not_required",
        database_enabled=False,
        database_required=False,
    )

    result = _verify(fixture)

    assert result["databaseBackup"] == "skipped"
    assert result["migrationStatus"] == "not_required"


def test_enabled_database_rejects_not_required_migration_evidence(
    tmp_path: Path,
) -> None:
    fixture = _fixture(
        tmp_path,
        migration_status="not_required",
        database_required=False,
    )

    with pytest.raises(
        evidence.EvidenceVerificationError,
        match="migration_invalid",
    ):
        _verify(fixture)


def test_production_verifier_requires_root_owned_backup_chain(tmp_path: Path) -> None:
    if os.getuid() == 0:
        pytest.skip("fixture is already root-owned")
    fixture = _fixture(tmp_path)

    with pytest.raises(
        evidence.EvidenceVerificationError,
        match="backup_root_invalid",
    ):
        evidence.verify_release_evidence(
            checkpoint_path=fixture["checkpoint"],
            evidence_path=fixture["evidence"],
            backup_root=fixture["backup_root"],
            expected_identity=fixture["identity"],
        )


def test_backup_boundary_accepts_not_started_migration(tmp_path: Path) -> None:
    fixture = _fixture(
        tmp_path,
        phase="backup_verified",
        migration_status="not_started",
    )

    result = _verify(fixture)

    assert result["checkpointPhase"] == "backup_verified"
    assert result["migrationStatus"] == "not_started"


def test_unprivileged_read_failure_is_fail_closed_but_privileged_path_can_verify(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    manifest = fixture["manifest"]
    original_open = evidence.os.open

    def deny_manifest(path: object, flags: int, *args: object) -> int:
        if Path(path) == manifest:
            raise PermissionError("simulated root-owned 0600 manifest")
        return original_open(path, flags, *args)

    with mock.patch.object(evidence.os, "open", side_effect=deny_manifest):
        with pytest.raises(
            evidence.EvidenceVerificationError,
            match="backup_manifest_invalid",
        ):
            _verify(fixture)

    assert _verify(fixture)["status"] == "verified"


@pytest.mark.parametrize("failure", ["dump_digest", "dump_symlink", "identity"])
def test_tampering_and_symlinks_fail_closed(tmp_path: Path, failure: str) -> None:
    fixture = _fixture(tmp_path)
    if failure == "dump_digest":
        fixture["dump"].write_bytes(b"tampered database dump content")
        fixture["dump"].chmod(0o600)
    elif failure == "dump_symlink":
        dump = fixture["dump"]
        target = tmp_path / "outside.dump"
        _private_write(target, b"private verified database dump")
        dump.unlink()
        dump.symlink_to(target)
    else:
        fixture["identity"] = checkpoint.ReleaseIdentity.create(
            repository="example/JATO_Analysis_System",
            commit="d" * 40,
            archive_sha256="b" * 64,
            archive_bytes=22_000_000,
            run_id=123456,
            run_attempt=2,
            frontend_identity="gha://example/JATO_Analysis_System/frontend",
            frontend_checksum="c" * 64,
        )

    with pytest.raises(evidence.EvidenceVerificationError):
        _verify(fixture)


def test_migration_target_and_result_must_match_exactly(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    evidence_path = fixture["evidence"]
    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    payload["migration"]["resultRevision"] = "20260707_0042 (head)"
    _private_write(
        evidence_path,
        (json.dumps(payload, sort_keys=True) + "\n").encode(),
    )

    checkpoint_path = fixture["checkpoint"]
    checkpoint_payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint_payload["message"] = (
        f"evidence_path={evidence_path} "
        f"evidence_sha256={hashlib.sha256(evidence_path.read_bytes()).hexdigest()}"
    )
    _private_write(
        checkpoint_path,
        (json.dumps(checkpoint_payload, sort_keys=True) + "\n").encode(),
    )

    with pytest.raises(
        evidence.EvidenceVerificationError,
        match="migration_invalid",
    ):
        _verify(fixture)
