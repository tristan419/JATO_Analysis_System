from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import sys
from unittest import mock

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "03_Scripts" / "deploy"))

import pre_switch_checkpoint_recovery as recovery  # noqa: E402
import release_checkpoint as checkpoint  # noqa: E402


ACTIVE_COMMIT = "a" * 40
IMPLEMENTATION_COMMIT = "b" * 40
FRONTEND_CHECKSUM = "c" * 64
REVISION = "20260715_0046"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_private(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    os.chmod(path, 0o600)


def _build_fence_publish_fixture(tmp_path: Path) -> dict[str, object]:
    root = tmp_path / "quarantine"
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    marker = tmp_path / "deployment-maintenance"
    marker.write_text("legacy marker\n", encoding="utf-8")
    marker_identity = dict(recovery._stable_path_identity(marker, "test"))
    destination = root / "legacy-maintenance-marker"
    manifest = root / "quarantine-contract.json"
    manifest.write_text('{"contract":"exact"}\n', encoding="utf-8")
    os.chmod(manifest, 0o600)
    uid = os.getuid()
    gid = os.getgid()
    plan = {
        "incidentId": "incident",
        "checkpoint": {
            "identity": {
                "repository": "example/JATO_Analysis_System",
                "commit": "d" * 40,
                "archiveSha256": "e" * 64,
                "archiveBytes": 1,
                "runId": 1,
                "runAttempt": 1,
                "frontendIdentity": "gha://example/artifact",
                "frontendChecksum": "f" * 64,
            }
        },
        "residue": {
            "profile": "materialized_never_started",
            "candidateUnit": {},
            "quarantineRoot": str(root),
            "quarantineDevice": root.stat().st_dev,
            "quarantineOwnerUid": uid,
            "quarantineOwnerGid": gid,
            "quarantineMode": "0700",
            "manifestName": manifest.name,
            "finalFenceName": "recovery-fence-final",
            "fenceContent": "release=test status=recovery_in_progress incident=incident",
            "items": [
                {
                    "id": "maintenance_marker",
                    "path": str(marker),
                    "quarantineName": destination.name,
                    **marker_identity,
                }
            ],
            "retainedEvidence": [],
            "requiredAbsentPaths": [],
        },
    }
    return {
        "plan": plan,
        "root": root,
        "marker": marker,
        "markerIdentity": marker_identity,
        "destination": destination,
        "temp": recovery._recovery_fence_temp_path(destination),
        "manifest": manifest,
        "manifestSha256": _sha256(manifest),
        "candidateSlot": tmp_path / "candidate-current",
    }


def _rename_no_replace_for_test(
    source: Path,
    destination: Path,
    flags: int,
) -> None:
    assert flags == recovery.RENAME_NOREPLACE
    assert not destination.exists() and not destination.is_symlink()
    os.rename(source, destination)


def _valid_observation(plan: dict[str, object]) -> dict[str, object]:
    expected = plan["expected"]
    runtime = plan["runtime"]
    assert isinstance(expected, dict)
    assert isinstance(runtime, dict)
    public = {
        name: {
            route: {
                "healthStatus": "ok",
                "frontendBuildCommit": ACTIVE_COMMIT,
                "frontendBuildMetaSha256": expected[
                    "activeBuildMetaSha256"
                ],
            }
            for route in ("origin", "external")
        }
        for name in runtime["serverNames"]
    }
    return {
        "active": {
            "frontendCommit": ACTIVE_COMMIT,
            "releaseRoot": expected["activeReleaseRoot"],
            "slot": "8000",
            "slotLink": "/opt/jato/slots/8000/current",
            "activeReleaseLinkTarget": "",
            "activeSlotFileSha256": expected["activeSlotFileSha256"],
            "slotEnvReleaseSha": ACTIVE_COMMIT,
            "slotEnvReleaseSlot": "8000",
            "backendInvocationId": expected["activeBackendInvocationId"],
            "backendExecMainStartTimestampMonotonic": expected[
                "activeBackendExecMainStartTimestampMonotonic"
            ],
            "backendActiveEnterTimestampMonotonic": expected[
                "activeBackendActiveEnterTimestampMonotonic"
            ],
            "backendNRestarts": 0,
            "backendProcesses": {
                "mainPid": 1234,
                "masterWorkersArgument": 2,
                "workerProcesses": 2,
                "resourceTrackerProcesses": 1,
            },
            "workers": 2,
            "memoryHighBytes": 6 * 1024**3,
            "memoryMaxBytes": 8 * 1024**3,
            "health": {
                "backendHealthStatus": "ok",
                "frontendBuildCommit": ACTIVE_COMMIT,
                "frontendBuildMetaSha256": expected[
                    "activeBuildMetaSha256"
                ],
            },
        },
        "public": public,
        "candidate": {
            "slot": "8001",
            "unitActive": False,
            "unitEnabled": False,
            "listener": False,
            "slotLinkExists": False,
            "targetReleaseActive": False,
            "nginxReferencesTarget": False,
        },
        "database": {
            "enabled": True,
            "mode": "read_only",
            "pgoptions": "default_transaction_read_only=on",
            "transactionReadOnly": "on",
            "currentRevisions": [REVISION],
            "oldHeadRevisions": [REVISION],
            "newHeadRevisions": [REVISION],
            "backupRevisions": [REVISION],
            "equal": True,
        },
        "runtime": {
            "nginxBackendSlot": "8000",
            "nginxConfigSha256": runtime["nginxConfigSha256"],
            "nginxFrontendRoot": expected["activeFrontendRoot"],
            "nginxMode": "legacy_pre_candidate",
            "monthlyWorkerActive": False,
            "monthlyWorkerEnabled": False,
            "switchUnitLoadState": "not-found",
            "switchUnitActiveState": "inactive",
            "maintenanceMarkerPresent": False,
            "schedulerSnapshotPresent": False,
        },
    }


def _build_fixture(
    tmp_path: Path,
    *,
    plan_schema_version: int = 1,
    evidence_migration: dict[str, object] | None = None,
) -> dict[str, object]:
    state_root = tmp_path / "home/.local/state/jato-production-release"
    checkpoints_root = state_root / "checkpoints"
    journals_root = state_root / "journals"
    state_root.mkdir(parents=True)
    checkpoints_root.mkdir()
    journals_root.mkdir()
    for path in (state_root, checkpoints_root, journals_root):
        os.chmod(path, 0o700)

    archive_content = b"reviewed legacy release archive\n"
    archive_sha = hashlib.sha256(archive_content).hexdigest()
    old_commit = "d" * 40
    archive_path = (
        tmp_path
        / "home/.cache/jato-releases/archives"
        / old_commit
        / f"{archive_sha}.tar.gz"
    )
    _write_private(archive_path, archive_content)
    release_root = tmp_path / "opt/jato/releases" / old_commit / archive_sha
    release_root.mkdir(parents=True)
    identity_marker = f"commit={old_commit} archive={archive_sha}\n"
    _write_private(
        release_root / ".jato-release-identity",
        identity_marker.encode(),
    )
    source_file = release_root / "03_Scripts/deploy/legacy-controller.sh"
    _write_private(source_file, b"RUN_DATABASE_MIGRATIONS=false\n")

    backup_root = tmp_path / "opt/backups/jato"
    manifests_root = backup_root / "manifests"
    dumps_root = backup_root / "pg"
    for path in (backup_root, manifests_root, dumps_root):
        path.mkdir(parents=True, exist_ok=True)
        os.chmod(path, 0o700)
    dump_path = dumps_root / "jato-20260730-233545.dump"
    _write_private(dump_path, f"COPY\n{REVISION}\n".encode())
    manifest_path = manifests_root / "backup-20260730-233545.json"
    manifest = {
        "database": {
            "enabled": True,
            "required": True,
            "status": "completed",
            "format": "postgresql_custom",
            "dumpPath": str(dump_path),
            "dumpBytes": dump_path.stat().st_size,
            "dumpSha256": _sha256(dump_path),
        }
    }
    _write_private(
        manifest_path,
        (json.dumps(manifest, sort_keys=True) + "\n").encode(),
    )

    identity = checkpoint.ReleaseIdentity.create(
        repository="example/JATO_Analysis_System",
        commit=old_commit,
        archive_sha256=archive_sha,
        archive_bytes=len(archive_content),
        run_id=30554067463,
        run_attempt=1,
        frontend_identity=(
            "gha://example/JATO_Analysis_System/actions/runs/30554067463/"
            f"attempts/1/artifacts/frontend-dist-{old_commit}"
        ),
        frontend_checksum=FRONTEND_CHECKSUM,
    )
    checkpoint_path = checkpoints_root / old_commit / f"{archive_sha}.json"
    journal_path = journals_root / old_commit / f"{archive_sha}.jsonl"
    evidence_path = checkpoint_path.with_name(f"{archive_sha}.evidence.json")
    if evidence_migration is None:
        if plan_schema_version == 1:
            evidence_migration = {
                "status": "not_required",
                "preRevision": None,
                "targetRevision": None,
                "resultRevision": None,
            }
        else:
            revision_output = f"{REVISION} (head)"
            evidence_migration = {
                "status": "completed",
                "preRevision": revision_output,
                "targetRevision": revision_output,
                "resultRevision": revision_output,
            }
    evidence = {
        "identity": identity.to_dict(),
        "backup": {
            "manifestPath": str(manifest_path),
            "manifestBytes": manifest_path.stat().st_size,
            "manifestSha256": _sha256(manifest_path),
        },
        "migration": evidence_migration,
    }
    _write_private(
        evidence_path,
        (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode(),
    )

    def write(
        phase: str,
        status: str = "completed",
        retry_class: str = "automatic",
        message: str | None = None,
    ) -> None:
        checkpoint.write_checkpoint(
            checkpoint_path=checkpoint_path,
            journal_path=journal_path,
            identity=identity,
            phase=phase,
            status=status,
            retry_class=retry_class,
            message=message,
            now="2026-07-30T15:35:51.793Z",
        )

    write("prepared")
    write(
        "source_install_started",
        status="in_progress",
        retry_class="rollback_required",
    )
    write("source_installed")
    write("backup_verified", status="in_progress")
    write("backup_verified")
    write(
        "migrated",
        message=(
            "database migration not required; "
            f"evidence_path={evidence_path} "
            f"evidence_sha256={_sha256(evidence_path)}"
        ),
    )

    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    active_root = tmp_path / "opt/JATO_Analysis_System-main"
    active_frontend = active_root / "06_AppPlatform/frontend/dist"
    active_frontend.mkdir(parents=True)
    bundle_root = tmp_path / "bundle"
    (bundle_root / "06_AppPlatform/backend").mkdir(parents=True)
    plan = {
        "schemaVersion": plan_schema_version,
        "incidentId": "test-pre-switch-db-evidence",
        "repository": identity.repository,
        "checkpoint": {
            "path": str(checkpoint_path),
            "bytes": checkpoint_path.stat().st_size,
            "sha256": _sha256(checkpoint_path),
            "sequence": 6,
            "journalPath": str(journal_path),
            "journalBytes": journal_path.stat().st_size,
            "journalLines": 6,
            "journalSha256": _sha256(journal_path),
            "evidencePath": str(evidence_path),
            "evidenceBytes": evidence_path.stat().st_size,
            "evidenceSha256": _sha256(evidence_path),
            "identity": identity.to_dict(),
        },
        "archive": {
            "path": str(archive_path),
            "bytes": archive_path.stat().st_size,
            "sha256": archive_sha,
            "releaseRoot": str(release_root),
            "releaseIdentity": identity_marker.strip(),
        },
        "backup": {
            "root": str(backup_root),
            "manifestPath": str(manifest_path),
            "manifestBytes": manifest_path.stat().st_size,
            "manifestSha256": _sha256(manifest_path),
            "dumpPath": str(dump_path),
            "dumpBytes": dump_path.stat().st_size,
            "dumpSha256": _sha256(dump_path),
        },
        "expected": {
            "activeCommit": ACTIVE_COMMIT,
            "activeBackendActiveEnterTimestampMonotonic": 1002,
            "activeBackendControlGroup": (
                "/system.slice/jato-fullstack-backend@8000.service"
            ),
            "activeBackendExecMainStartTimestampMonotonic": 1001,
            "activeBackendInvocationId": "f" * 32,
            "activeBackendNRestarts": 0,
            "activeBuildMetaBytes": 100,
            "activeBuildMetaSha256": "9" * 64,
            "activeReleaseRoot": str(active_root),
            "activeFrontendRoot": str(active_frontend),
            "activeSlot": "8000",
            "activeSlotFileBytes": 5,
            "activeSlotFileSha256": "8" * 64,
            "candidateSlot": "8001",
            "workers": 2,
            "memoryHighBytes": 6 * 1024**3,
            "memoryMaxBytes": 8 * 1024**3,
            "revisions": [REVISION],
        },
        "runtime": {
            "activeSlotFile": str(runtime_root / "active-slot"),
            "activeReleaseLink": str(runtime_root / "active"),
            "candidateSlotLink": str(runtime_root / "candidate"),
            "bluegreenStateRoot": str(runtime_root),
            "deploymentMarker": str(runtime_root / "maintenance"),
            "schedulerState": str(runtime_root / "scheduler.tsv"),
            "nginxConfig": str(runtime_root / "jato_fullstack.conf"),
            "nginxConfigBytes": 100,
            "nginxConfigSha256": "7" * 64,
            "nginxMode": "legacy_pre_candidate",
            "backendEnv": str(runtime_root / "backend.env"),
            "slotEnv": str(runtime_root / "8000.env"),
            "venv": str(runtime_root / ".venv"),
            "backendServicePrefix": "jato-fullstack-backend@",
            "monthlyWorkerUnit": "jato-monthly-worker.service",
            "switchUnit": "jato-bluegreen-production.service",
            "serverNames": ["ojeur.cloud", "www.ojeur.cloud"],
        },
        "sourceProofs": {
            "03_Scripts/deploy/legacy-controller.sh": _sha256(source_file),
        },
    }
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "plan": plan,
        "plan_path": plan_path,
        "plan_sha": _sha256(plan_path),
        "bundle_root": bundle_root,
        "checkpoint_path": checkpoint_path,
        "journal_path": journal_path,
        "evidence_path": evidence_path,
        "identity": identity,
        "state_root": state_root,
        "observation": _valid_observation(plan),
    }


def _recover(
    fixture: dict[str, object],
    mode: str,
    observation: dict[str, object] | None = None,
) -> dict[str, object]:
    plan_path = fixture["plan_path"]
    state_root = fixture["state_root"]
    assert isinstance(plan_path, Path)
    assert isinstance(state_root, Path)
    with (
        mock.patch.object(
            recovery,
            "assert_production_lock",
            return_value={
                "path": str(state_root / "production-deploy.lock"),
                "device": 1,
                "inode": 2,
                "holderPid": 123,
            },
        ),
        mock.patch.object(
            recovery,
            "collect_observation",
            return_value=observation or fixture["observation"],
        ),
        mock.patch.object(
            recovery,
            "TRUSTED_SYSTEM_UID",
            os.getuid(),
        ),
    ):
        return dict(
            recovery.recover(
                plan_path=plan_path,
                expected_plan_sha256=str(fixture["plan_sha"]),
                bundle_root=fixture["bundle_root"],
                implementation_commit=IMPLEMENTATION_COMMIT,
                lock_path=state_root / "production-deploy.lock",
                lock_holder_pid=123,
                mode=mode,
            )
        )


def test_dry_run_proves_eligibility_without_writing_state(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    checkpoint_before = fixture["checkpoint_path"].read_bytes()
    journal_before = fixture["journal_path"].read_bytes()
    evidence_before = fixture["evidence_path"].read_bytes()

    result = _recover(fixture, "dry-run")

    assert result["decision"] == "dry-run-eligible"
    assert fixture["checkpoint_path"].read_bytes() == checkpoint_before
    assert fixture["journal_path"].read_bytes() == journal_before
    assert fixture["evidence_path"].read_bytes() == evidence_before
    assert not (fixture["state_root"] / "recoveries").exists()


def test_apply_preserves_legacy_evidence_and_seals_distinct_terminal(
    tmp_path: Path,
) -> None:
    fixture = _build_fixture(tmp_path)
    evidence_before = fixture["evidence_path"].read_bytes()

    result = _recover(fixture, "apply")
    persisted = checkpoint.load_checkpoint(fixture["checkpoint_path"])

    assert result["decision"] == "pre-switch-aborted"
    assert persisted["phase"] == checkpoint.PRE_SWITCH_ABORT_PHASE
    assert persisted["status"] == "completed"
    assert persisted["retryClass"] == "automatic"
    assert persisted["sequence"] == 7
    assert fixture["evidence_path"].read_bytes() == evidence_before
    assert len(fixture["journal_path"].read_text().splitlines()) == 7
    resumable = checkpoint.assert_resumable(
        checkpoint_path=fixture["checkpoint_path"],
        expected_identity=fixture["identity"],
    )
    assert resumable["decision"] == "already-pre-switch-aborted"
    with pytest.raises(checkpoint.CheckpointError, match="immutable"):
        checkpoint.write_checkpoint(
            checkpoint_path=fixture["checkpoint_path"],
            journal_path=fixture["journal_path"],
            identity=fixture["identity"],
            phase="backend_healthy",
            status="completed",
            retry_class="automatic",
        )
    receipt_path = Path(result["receiptPath"])
    assert receipt_path.is_file()
    assert stat_mode(receipt_path) == 0o600

    second = _recover(fixture, "apply")
    assert second["decision"] == "already-pre-switch-aborted"
    assert len(fixture["journal_path"].read_text().splitlines()) == 7


@pytest.mark.parametrize("plan_schema_version", [1, 2])
def test_terminal_receipt_is_required_by_cross_release_gate(
    tmp_path: Path,
    plan_schema_version: int,
) -> None:
    fixture = _build_fixture(
        tmp_path,
        plan_schema_version=plan_schema_version,
    )
    result = _recover(fixture, "apply")
    identity = fixture["identity"]
    assert isinstance(identity, checkpoint.ReleaseIdentity)
    next_identity = checkpoint.ReleaseIdentity.create(
        repository=identity.repository,
        commit="e" * 40,
        archive_sha256="f" * 64,
        archive_bytes=123,
        run_id=987654,
        run_attempt=1,
        frontend_identity="gha://example/JATO_Analysis_System/next",
        frontend_checksum="1" * 64,
    )
    checkpoints_root = fixture["state_root"] / "checkpoints"
    current = (
        checkpoints_root
        / next_identity.commit
        / f"{next_identity.archiveSha256}.json"
    )
    current_journal = (
        fixture["state_root"]
        / "journals"
        / next_identity.commit
        / f"{next_identity.archiveSha256}.jsonl"
    )
    checkpoint.write_checkpoint(
        checkpoint_path=current,
        journal_path=current_journal,
        identity=next_identity,
        phase="prepared",
        status="completed",
        retry_class="automatic",
    )

    gate = checkpoint.assert_cross_release_safe(
        checkpoints_root=checkpoints_root,
        current_checkpoint=current,
        expected_identity=next_identity,
    )
    assert gate["decision"] == "cross-release-safe"

    receipt_path = Path(result["receiptPath"])
    receipt_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(
        checkpoint.CheckpointError,
        match="receipt SHA256 mismatch",
    ):
        checkpoint.assert_cross_release_safe(
            checkpoints_root=checkpoints_root,
            current_checkpoint=current,
            expected_identity=next_identity,
        )


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def test_observation_rejects_each_unsafe_corner_case(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    plan = fixture["plan"]
    assert isinstance(plan, dict)
    scenarios: dict[str, tuple[tuple[str, ...], object]] = {
        "db_mismatch": (
            ("database", "currentRevisions"),
            ["20260715_0045"],
        ),
        "candidate_active": (("candidate", "unitActive"), True),
        "candidate_link": (("candidate", "slotLinkExists"), True),
        "public_wrong_sha": (
            (
                "public",
                "ojeur.cloud",
                "external",
                "frontendBuildCommit",
            ),
            "e" * 40,
        ),
        "monthly_enabled": (("runtime", "monthlyWorkerEnabled"), True),
        "maintenance_marker": (
            ("runtime", "maintenanceMarkerPresent"),
            True,
        ),
        "switch_active": (
            ("runtime", "switchUnitActiveState"),
            "active",
        ),
    }
    for parts, value in scenarios.values():
        with pytest.raises(recovery.RecoveryError):
            observation = copy.deepcopy(fixture["observation"])
            cursor = observation
            for part in parts[:-1]:
                cursor = cursor[part]
            cursor[parts[-1]] = value
            recovery.validate_observation(plan, observation)


def test_legacy_evidence_signature_is_exact(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    evidence_path = fixture["evidence_path"]
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    evidence["migration"]["status"] = "completed"
    evidence_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(recovery.RecoveryError, match="legacy_evidence_invalid"):
        _recover(fixture, "dry-run")


def test_schema_v2_completed_evidence_dry_run_and_apply_are_exact(
    tmp_path: Path,
) -> None:
    fixture = _build_fixture(tmp_path, plan_schema_version=2)
    checkpoint_before = fixture["checkpoint_path"].read_bytes()
    journal_before = fixture["journal_path"].read_bytes()
    evidence_before = fixture["evidence_path"].read_bytes()

    dry_run = _recover(fixture, "dry-run")

    assert dry_run["decision"] == "dry-run-eligible"
    assert fixture["checkpoint_path"].read_bytes() == checkpoint_before
    assert fixture["journal_path"].read_bytes() == journal_before
    assert fixture["evidence_path"].read_bytes() == evidence_before
    assert not (fixture["state_root"] / "recoveries").exists()

    applied = _recover(fixture, "apply")
    receipt = json.loads(Path(applied["receiptPath"]).read_text(encoding="utf-8"))
    assert applied["decision"] == "pre-switch-aborted"
    assert receipt["schemaVersion"] == 2
    assert receipt["legacyEvidence"]["migrationStatus"] == "completed"
    checkpoint.validate_pre_switch_abort_settlement(
        checkpoint_path=fixture["checkpoint_path"],
        checkpoints_root=fixture["state_root"] / "checkpoints",
    )
    replay = _recover(fixture, "apply")
    assert replay["decision"] == "already-pre-switch-aborted"
    assert len(fixture["journal_path"].read_text().splitlines()) == 7


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "not_required"),
        ("preRevision", "20260715_0045 (head)"),
        ("targetRevision", ""),
        ("resultRevision", None),
    ],
)
def test_schema_v2_rejects_profile_or_revision_mismatch(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    revision_output = f"{REVISION} (head)"
    migration: dict[str, object] = {
        "status": "completed",
        "preRevision": revision_output,
        "targetRevision": revision_output,
        "resultRevision": revision_output,
    }
    migration[field] = value
    fixture = _build_fixture(
        tmp_path,
        plan_schema_version=2,
        evidence_migration=migration,
    )

    with pytest.raises(recovery.RecoveryError, match="legacy_evidence_invalid"):
        _recover(fixture, "dry-run")


@pytest.mark.parametrize(
    ("plan_schema_version", "migration_status"),
    [(1, "completed"), (2, "not_required")],
)
def test_settlement_rejects_receipt_profile_swap(
    tmp_path: Path,
    plan_schema_version: int,
    migration_status: str,
) -> None:
    fixture = _build_fixture(
        tmp_path,
        plan_schema_version=plan_schema_version,
    )
    applied = _recover(fixture, "apply")
    receipt = json.loads(
        Path(applied["receiptPath"]).read_text(encoding="utf-8")
    )
    receipt["legacyEvidence"]["migrationStatus"] = migration_status

    with pytest.raises(
        checkpoint.CheckpointError,
        match="legacy evidence proof is invalid",
    ):
        checkpoint._validate_recovery_legacy_evidence(
            receipt=receipt,
            evidence_bindings=[
                (str(fixture["evidence_path"]), _sha256(fixture["evidence_path"]))
            ],
        )


def test_schema_v2_settlement_rejects_receipt_revision_mismatch(
    tmp_path: Path,
) -> None:
    fixture = _build_fixture(tmp_path, plan_schema_version=2)
    applied = _recover(fixture, "apply")
    receipt = json.loads(
        Path(applied["receiptPath"]).read_text(encoding="utf-8")
    )
    receipt["database"]["currentRevisions"] = ["20260715_0045"]

    with pytest.raises(
        checkpoint.CheckpointError,
        match="schema v2 recovery evidence revisions differ from receipt",
    ):
        checkpoint._validate_recovery_legacy_evidence(
            receipt=receipt,
            evidence_bindings=[
                (str(fixture["evidence_path"]), _sha256(fixture["evidence_path"]))
            ],
        )


@pytest.mark.parametrize("schema_version", [True, 0, 4])
def test_receipt_schema_version_is_explicit_and_not_boolean(
    schema_version: object,
) -> None:
    with pytest.raises(
        checkpoint.CheckpointError,
        match="recovery receipt schemaVersion is invalid",
    ):
        checkpoint._recovery_migration_status(
            {"schemaVersion": schema_version}
        )


@pytest.mark.parametrize("schema_version", [True, 0, 4])
def test_plan_schema_version_is_explicit_and_not_boolean(
    tmp_path: Path,
    schema_version: object,
) -> None:
    fixture = _build_fixture(tmp_path)
    plan = fixture["plan"]
    assert isinstance(plan, dict)
    plan["schemaVersion"] = schema_version
    path = fixture["plan_path"]
    assert isinstance(path, Path)
    path.write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(recovery.RecoveryError, match="unsupported schemaVersion"):
        recovery.load_recovery_plan(path, _sha256(path))


def test_generic_checkpoint_write_cannot_forge_abort(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    with pytest.raises(
        checkpoint.CheckpointError,
        match="audited recovery helper",
    ):
        checkpoint.write_checkpoint(
            checkpoint_path=fixture["checkpoint_path"],
            journal_path=fixture["journal_path"],
            identity=fixture["identity"],
            phase=checkpoint.PRE_SWITCH_ABORT_PHASE,
            status="completed",
            retry_class="automatic",
        )


def test_journal_ahead_crash_reuses_receipt_and_finishes_checkpoint(
    tmp_path: Path,
) -> None:
    fixture = _build_fixture(tmp_path)
    real_atomic_write = checkpoint.atomic_write_json
    failed_once = False

    def fail_checkpoint_replace(
        path: Path,
        payload: dict[str, object],
        **kwargs: object,
    ) -> None:
        nonlocal failed_once
        if (
            path == fixture["checkpoint_path"]
            and payload.get("phase") == checkpoint.PRE_SWITCH_ABORT_PHASE
            and not failed_once
        ):
            failed_once = True
            raise OSError("simulated power loss")
        real_atomic_write(path, payload, **kwargs)

    with mock.patch.object(
        checkpoint,
        "atomic_write_json",
        side_effect=fail_checkpoint_replace,
    ):
        with pytest.raises(OSError, match="simulated power loss"):
            _recover(fixture, "apply")

    assert checkpoint.load_checkpoint(fixture["checkpoint_path"])["phase"] == "migrated"
    assert len(fixture["journal_path"].read_text().splitlines()) == 7

    result = _recover(fixture, "apply")
    assert result["decision"] == "pre-switch-aborted"
    assert checkpoint.load_checkpoint(fixture["checkpoint_path"])["phase"] == (
        checkpoint.PRE_SWITCH_ABORT_PHASE
    )
    assert len(fixture["journal_path"].read_text().splitlines()) == 7


def test_apply_never_seals_before_final_runtime_proof(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    unsafe = copy.deepcopy(fixture["observation"])
    unsafe["candidate"]["listener"] = True
    with (
        mock.patch.object(
            recovery,
            "assert_production_lock",
            return_value={
                "path": str(fixture["state_root"] / "production-deploy.lock"),
                "device": 1,
                "inode": 2,
                "holderPid": 123,
            },
        ),
        mock.patch.object(
            recovery,
            "collect_observation",
            side_effect=[
                fixture["observation"],
                fixture["observation"],
                unsafe,
            ],
        ),
        mock.patch.object(
            recovery,
            "TRUSTED_SYSTEM_UID",
            os.getuid(),
        ),
    ):
        with pytest.raises(recovery.RecoveryError, match="candidate_present"):
            recovery.recover(
                plan_path=fixture["plan_path"],
                expected_plan_sha256=fixture["plan_sha"],
                bundle_root=fixture["bundle_root"],
                implementation_commit=IMPLEMENTATION_COMMIT,
                lock_path=fixture["state_root"] / "production-deploy.lock",
                lock_holder_pid=123,
                mode="apply",
            )

    persisted = checkpoint.load_checkpoint(fixture["checkpoint_path"])
    assert persisted["phase"] == "migrated"
    assert len(fixture["journal_path"].read_text().splitlines()) == 6
    assert (fixture["state_root"] / "recoveries").is_dir()


def test_receipt_parent_symlink_is_rejected(tmp_path: Path) -> None:
    fixture = _build_fixture(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (fixture["state_root"] / "recoveries").symlink_to(
        outside,
        target_is_directory=True,
    )

    with pytest.raises(
        checkpoint.CheckpointError,
        match="private state",
    ):
        _recover(fixture, "apply")
    assert checkpoint.load_checkpoint(fixture["checkpoint_path"])["phase"] == (
        "migrated"
    )
    assert list(outside.iterdir()) == []


def test_database_probe_is_read_only_and_compares_all_revision_sets(
    tmp_path: Path,
) -> None:
    fixture = _build_fixture(tmp_path)
    calls: list[tuple[str, str, list[str]]] = []

    def bash_probe(
        script: str,
        arguments: list[str],
        category: str,
    ) -> str:
        calls.append((category, script, arguments))
        if category == "database_not_enabled":
            return "enabled"
        if category == "database_read_only_failed":
            return "on"
        return REVISION

    with (
        mock.patch.object(recovery, "_read_secure_text", return_value=""),
        mock.patch.object(recovery, "_bash_probe", side_effect=bash_probe),
        mock.patch.object(
            recovery,
            "_run_text",
            return_value=f"COPY public.alembic_version\n{REVISION}\n",
        ) as run_text,
    ):
        proof = recovery._collect_database_proof(
            fixture["plan"],
            fixture["bundle_root"],
        )

    assert proof["transactionReadOnly"] == "on"
    by_category = {category: (script, args) for category, script, args in calls}
    read_only_script = by_category["database_read_only_failed"][0]
    current_script = by_category["database_current_failed"][0]
    assert "default_transaction_read_only=on" in read_only_script
    assert "SHOW transaction_read_only" in by_category[
        "database_read_only_failed"
    ][1][3]
    assert "default_transaction_read_only=on" in current_script
    assert "-m alembic current" in current_script
    assert "-m alembic heads" in by_category["old_heads_failed"][0]
    assert "-m alembic heads" in by_category["new_heads_failed"][0]
    backup_command = run_text.call_args.args[0]
    assert backup_command[:3] == [
        "pg_restore",
        "--data-only",
        "--table=alembic_version",
    ]

    def mismatched_probe(
        script: str,
        arguments: list[str],
        category: str,
    ) -> str:
        if category == "database_not_enabled":
            return "enabled"
        if category == "database_read_only_failed":
            return "on"
        if category == "new_heads_failed":
            return "20260715_0045"
        return REVISION

    with (
        mock.patch.object(recovery, "_read_secure_text", return_value=""),
        mock.patch.object(
            recovery,
            "_bash_probe",
            side_effect=mismatched_probe,
        ),
        mock.patch.object(recovery, "_run_text", return_value=REVISION),
        pytest.raises(
            recovery.RecoveryError,
            match="database_revision_mismatch",
        ),
    ):
        recovery._collect_database_proof(
            fixture["plan"],
            fixture["bundle_root"],
        )


def test_public_build_probe_verifies_raw_byte_identity() -> None:
    expected_build = {"commit": ACTIVE_COMMIT, "marker": 1}
    expected_raw = json.dumps(
        expected_build,
        separators=(",", ":"),
    ).encode("utf-8")
    same_json_different_bytes = (
        f'{{"marker":1,"commit":"{ACTIVE_COMMIT}"}}'.encode("utf-8")
    )
    assert len(same_json_different_bytes) == len(expected_raw)
    assert json.loads(same_json_different_bytes) == expected_build

    with (
        mock.patch.object(
            recovery,
            "_run_text",
            return_value='{"status":"ok"}',
        ),
        mock.patch.object(recovery, "_run_bytes", return_value=expected_raw),
    ):
        proof = recovery._probe_legacy_health_and_build(
            health_url="https://example.test/healthz",
            build_url="https://example.test/build-meta.json",
            expected_build=expected_build,
            expected_build_bytes=len(expected_raw),
            expected_build_sha256=hashlib.sha256(expected_raw).hexdigest(),
            expected_commit=ACTIVE_COMMIT,
        )
    assert proof["frontendBuildMetaSha256"] == hashlib.sha256(
        expected_raw
    ).hexdigest()

    with (
        mock.patch.object(
            recovery,
            "_run_text",
            return_value='{"status":"ok"}',
        ),
        mock.patch.object(
            recovery,
            "_run_bytes",
            return_value=same_json_different_bytes,
        ),
        pytest.raises(
            recovery.RecoveryError,
            match="public build metadata byte identity",
        ),
    ):
        recovery._probe_legacy_health_and_build(
            health_url="https://example.test/healthz",
            build_url="https://example.test/build-meta.json",
            expected_build=expected_build,
            expected_build_bytes=len(expected_raw),
            expected_build_sha256=hashlib.sha256(expected_raw).hexdigest(),
            expected_commit=ACTIVE_COMMIT,
        )


def test_ce5_versioned_incident_plan_remains_valid_and_exact() -> None:
    path = (
        REPO_ROOT
        / ".github/recovery-plans/"
        "2026-07-30-ce5-pre-switch-db-evidence.json"
    )
    plan, digest = recovery.load_recovery_plan(path, _sha256(path))

    assert digest == (
        "ee8157e6f55f31890579b2bffa3106cfde4ff35d683c17476e6a47e364e81322"
    )
    assert plan["schemaVersion"] == 1
    assert plan["incidentId"] == "2026-07-30-ce5-pre-switch-db-evidence"
    assert plan["checkpoint"]["sequence"] == 6
    assert plan["checkpoint"]["sha256"] == (
        "b6ab32cc272bc6cdb94533c7bb66f2a8a003baeb4befe518038bcacb4251fb18"
    )
    assert plan["expected"]["revisions"] == [REVISION]
    assert plan["runtime"]["nginxMode"] == "legacy_pre_candidate"


def test_86ce_versioned_incident_plan_is_valid_and_exact() -> None:
    path = (
        REPO_ROOT
        / ".github/recovery-plans/"
        "2026-07-30-86ce-pre-switch-db-evidence.json"
    )
    plan, digest = recovery.load_recovery_plan(path, _sha256(path))

    assert digest == (
        "6ba2251e187bfbf027ce4629d050f61a0212fbad6441ca7bbc787e49b2a2e797"
    )
    assert plan["schemaVersion"] == 2
    assert plan["incidentId"] == "2026-07-30-86ce-pre-switch-db-evidence"
    assert plan["checkpoint"]["identity"]["commit"] == (
        "86ce149ea9db84a6125cdb3a99d38ba794ce7edf"
    )
    assert plan["checkpoint"]["sha256"] == (
        "528abbeeaab1aea242407dede492a5f4b1df4cf01c8e820eb45dddee662f65ee"
    )
    assert plan["checkpoint"]["evidenceSha256"] == (
        "4805110c6f4be1cf8175b556c1ff6eb309cda27ad3b8c676dd903af92b4a268d"
    )
    assert plan["checkpoint"]["journalSha256"] == (
        "b86bd3400099350e00cc1975dae3d119eb5a4296c53568d915a6467879b4578b"
    )
    assert plan["archive"]["sha256"] == (
        "8e95818d8e8702d8de422131502fcf54b77b97ad08f17a8ca937358d69d672fb"
    )
    assert plan["backup"]["manifestSha256"] == (
        "42587d69d468bf79262aa22d9173a27933adefd402ffaf792852dfe73b00f341"
    )
    assert plan["backup"]["dumpSha256"] == (
        "1845312b259bc62ed8275155d767ba14aa3c01f3d47b8640dd83b4bd366a66ef"
    )
    assert plan["expected"]["revisions"] == [REVISION]
    assert plan["runtime"]["nginxMode"] == "legacy_pre_candidate"


def test_29df_schema_v3_plan_is_exact_and_does_not_change_historical_plans() -> None:
    path = (
        REPO_ROOT
        / ".github/recovery-plans/"
        "2026-08-03-29df-pre-switch-candidate-residue.json"
    )
    plan, digest = recovery.load_recovery_plan(path, _sha256(path))

    assert digest == (
        "ae4d3d5eb76695e29c2eeb947b7783c42960a266c27abaa3f7b6a2faa51fd0f2"
    )
    assert plan["schemaVersion"] == 3
    assert plan["incidentId"] == recovery.RESIDUE_INCIDENT_ID
    assert plan["checkpoint"]["identity"]["commit"] == recovery.RESIDUE_TARGET_COMMIT
    assert plan["checkpoint"]["sequence"] == 6
    assert plan["checkpoint"]["sha256"] == (
        "21137d89a177ab0892ab3bf1f00abf0cc6ca4fababefaf8c5a9db79e62c89ffd"
    )
    assert {item["id"] for item in plan["residue"]["items"]} == set(
        recovery.RESIDUE_PATHS
    )
    assert all(item["nlink"] == 1 for item in plan["residue"]["items"])
    assert plan["residue"]["candidateUnit"]["mainPid"] == 0
    assert plan["residue"]["candidateUnit"]["invocationId"] == ""
    assert plan["runtime"]["nginxConfigSha256"] == (
        "6f5e26e9e293d8024af90ed8446a1f8f1c5072567c38f050f86ec38524e2880d"
    )
    assert plan["runtime"]["nginxCanonicalConfig"] == (
        "/etc/nginx/sites-available/jato_fullstack.conf"
    )
    retained = {
        item["id"]: item for item in plan["residue"]["retainedEvidence"]
    }
    assert retained["canonical_nginx_config"] == {
        "bytes": 3795,
        "device": 64770,
        "gid": 0,
        "id": "canonical_nginx_config",
        "inode": 789351,
        "kind": "file",
        "mode": "0644",
        "mtimeNs": 1783042607281907362,
        "nlink": 1,
        "path": "/etc/nginx/sites-available/jato_fullstack.conf",
        "sha256": (
            "964c351bbed725a36da517c06ce7ef82ff9d11046e8329f02a118f638a32aec4"
        ),
        "target": None,
        "targetSha256": None,
        "uid": 0,
    }
    assert set(plan["residue"]["requiredAbsentPaths"]) == {
        str(path) for path in recovery.RESIDUE_REQUIRED_ABSENT_PATHS
    }


def test_29df_plan_rejects_canonical_and_absence_contract_drift(
    tmp_path: Path,
) -> None:
    source = (
        REPO_ROOT
        / ".github/recovery-plans/"
        "2026-08-03-29df-pre-switch-candidate-residue.json"
    )
    original = json.loads(source.read_text(encoding="utf-8"))
    cases = []
    wrong_canonical = copy.deepcopy(original)
    wrong_canonical["residue"]["retainedEvidence"][0]["inode"] += 1
    cases.append(wrong_canonical)
    wrong_path = copy.deepcopy(original)
    wrong_path["runtime"]["nginxCanonicalConfig"] = "/tmp/nginx.conf"
    cases.append(wrong_path)
    duplicate_absence = copy.deepcopy(original)
    duplicate_absence["residue"]["requiredAbsentPaths"][-1] = (
        duplicate_absence["residue"]["requiredAbsentPaths"][0]
    )
    cases.append(duplicate_absence)

    for index, payload in enumerate(cases):
        path = tmp_path / f"changed-{index}.json"
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(recovery.RecoveryError, match="plan_invalid"):
            recovery.load_recovery_plan(path, _sha256(path))


def test_schema_v3_allows_only_reviewed_cached_sources_before_daemon_reload() -> None:
    path = (
        REPO_ROOT
        / ".github/recovery-plans/"
        "2026-08-03-29df-pre-switch-candidate-residue.json"
    )
    plan, _ = recovery.load_recovery_plan(path, _sha256(path))
    expected = plan["residue"]["candidateUnit"]
    unit = {
        "LoadState": expected["loadState"],
        "ActiveState": expected["activeState"],
        "SubState": expected["subState"],
        "UnitFileState": expected["unitFileState"],
        "MainPID": str(expected["mainPid"]),
        "Result": expected["result"],
        "NRestarts": str(expected["nRestarts"]),
        "ExecMainStartTimestampMonotonic": str(
            expected["execMainStartTimestampMonotonic"]
        ),
        "ActiveEnterTimestampMonotonic": str(
            expected["activeEnterTimestampMonotonic"]
        ),
        "InactiveEnterTimestampMonotonic": str(
            expected["inactiveEnterTimestampMonotonic"]
        ),
        "InvocationID": expected["invocationId"],
        "FragmentPath": expected["fragmentPath"],
        "DropInPaths": " ".join(expected["dropInPaths"]),
        "MemoryHigh": str(expected["memoryHighBytes"]),
        "MemoryMax": str(expected["memoryMaxBytes"]),
    }

    cached = recovery._candidate_never_started_proof(
        plan,
        unit,
        stage="quarantined_fenced",
        listener=False,
    )
    assert cached["ownedSourceReferences"] == sorted(
        {expected["fragmentPath"], *expected["dropInPaths"]}
    )
    with pytest.raises(recovery.RecoveryError, match="finalized Candidate residue"):
        recovery._candidate_never_started_proof(
            plan,
            unit,
            stage="finalized",
            listener=False,
        )

    unit["FragmentPath"] = "/etc/systemd/system/jato-fullstack-backend@.service"
    unit["DropInPaths"] = ""
    detached = recovery._candidate_never_started_proof(
        plan,
        unit,
        stage="quarantined_fenced",
        listener=False,
    )
    assert detached["ownedSourceReferences"] == []

    unit["DropInPaths"] = "/tmp/unreviewed.conf"
    with pytest.raises(recovery.RecoveryError, match="unknown unit sources"):
        recovery._candidate_never_started_proof(
            plan,
            unit,
            stage="quarantined_fenced",
            listener=False,
        )


def test_schema_v3_apply_authorization_is_loaded_only_from_bound_bundle(
    tmp_path: Path,
) -> None:
    plan_path = (
        REPO_ROOT
        / ".github/recovery-plans/"
        "2026-08-03-29df-pre-switch-candidate-residue.json"
    )
    plan, plan_sha = recovery.load_recovery_plan(plan_path, _sha256(plan_path))
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    authorization = {
        "schemaVersion": 1,
        "kind": "checkpoint_recovery_dry_run_authorization",
        "repository": plan["repository"],
        "workflowPath": ".github/workflows/production-checkpoint-recovery.yml",
        "runId": 123456,
        "runAttempt": 2,
        "mainSha": IMPLEMENTATION_COMMIT,
        "planSha256": plan_sha,
        "resultSha256": "a" * 64,
        "incidentId": plan["incidentId"],
        "inventoryDigest": recovery._residue_inventory_digest(plan),
        "decision": "candidate-residue-dry-run-eligible",
    }
    authorization_path = bundle / "reviewed-dry-run-authorization.json"
    authorization_path.write_text(
        json.dumps(authorization, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    authorization_sha = _sha256(authorization_path)
    (bundle / "recovery-control-manifest.json").write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "commit": IMPLEMENTATION_COMMIT,
                "planSha256": plan_sha,
                "files": {
                    "reviewed-dry-run-authorization.json": authorization_sha,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    bound_path, bound_sha = recovery._bundled_dry_run_authorization(
        bundle_root=bundle,
        plan_sha256=plan_sha,
        implementation_commit=IMPLEMENTATION_COMMIT,
    )
    loaded, loaded_sha = recovery._load_dry_run_authorization(
        path=bound_path,
        expected_sha256=bound_sha,
        bundle_root=bundle,
        plan=plan,
        plan_sha256=plan_sha,
        implementation_commit=IMPLEMENTATION_COMMIT,
    )
    assert loaded == authorization
    assert loaded_sha == authorization_sha

    authorization_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(recovery.RecoveryError, match="authorization SHA-256 changed"):
        recovery._load_dry_run_authorization(
            path=bound_path,
            expected_sha256=bound_sha,
            bundle_root=bundle,
            plan=plan,
            plan_sha256=plan_sha,
            implementation_commit=IMPLEMENTATION_COMMIT,
        )


@pytest.mark.parametrize(
    "failure_point",
    [
        "after_open",
        "after_chmod",
        "partial_write",
        "file_fsync",
        "rename",
        "dir_fsync",
    ],
)
def test_recovery_fence_publish_is_replayable_at_every_crash_boundary(
    tmp_path: Path,
    failure_point: str,
) -> None:
    fixture = _build_fence_publish_fixture(tmp_path)
    plan = fixture["plan"]
    marker = fixture["marker"]
    destination = fixture["destination"]
    temp = fixture["temp"]
    manifest = fixture["manifest"]
    candidate_slot = fixture["candidateSlot"]
    assert isinstance(plan, dict)
    assert isinstance(marker, Path)
    assert isinstance(destination, Path)
    assert isinstance(temp, Path)
    assert isinstance(manifest, Path)
    assert isinstance(candidate_slot, Path)
    manifest_sha256 = str(fixture["manifestSha256"])

    real_fchmod = os.fchmod
    real_write = os.write
    partial_write_calls = 0

    def chmod_then_fail(descriptor: int, mode: int) -> None:
        real_fchmod(descriptor, mode)
        raise OSError("crash after chmod")

    def partial_write_then_fail(descriptor: int, raw: bytes) -> int:
        nonlocal partial_write_calls
        partial_write_calls += 1
        if partial_write_calls == 1:
            return real_write(descriptor, raw[: max(1, len(raw) // 2)])
        raise OSError("crash after partial write")

    def rename_then_fail(source: Path, target: Path, flags: int) -> None:
        assert flags == recovery.RENAME_NOREPLACE
        raise recovery.RecoveryError(
            "quarantine_rename_failed",
            "crash before rename",
        )

    def rename_then_dir_fsync_failure(
        source: Path,
        target: Path,
        flags: int,
    ) -> None:
        _rename_no_replace_for_test(source, target, flags)
        raise OSError("crash during parent fsync")

    if failure_point == "after_open":
        fault = mock.patch.object(
            recovery.os,
            "fchown",
            side_effect=OSError("crash after open"),
        )
    elif failure_point == "after_chmod":
        fault = mock.patch.object(
            recovery.os,
            "fchmod",
            side_effect=chmod_then_fail,
        )
    elif failure_point == "partial_write":
        fault = mock.patch.object(
            recovery.os,
            "write",
            side_effect=partial_write_then_fail,
        )
    elif failure_point == "file_fsync":
        fault = mock.patch.object(
            recovery.os,
            "fsync",
            side_effect=OSError("crash during file fsync"),
        )
    elif failure_point == "rename":
        fault = mock.patch.object(
            recovery,
            "_renameat2",
            side_effect=rename_then_fail,
        )
    else:
        fault = mock.patch.object(
            recovery,
            "_renameat2",
            side_effect=rename_then_dir_fsync_failure,
        )

    with fault, pytest.raises((OSError, recovery.RecoveryError)):
        recovery._create_recovery_fence(
            destination,
            plan,
            manifest_path=manifest,
            manifest_sha256=manifest_sha256,
        )

    assert recovery._stable_path_identity(marker, "test") == fixture["markerIdentity"]
    with mock.patch.object(
        recovery,
        "RESIDUE_PATHS",
        {
            "maintenance_marker": marker,
            "candidate_slot_link": candidate_slot,
        },
    ):
        state = recovery._collect_residue_state(plan, "c" * 64)
    assert state["stage"] == "partial"

    with mock.patch.object(
        recovery,
        "_renameat2",
        side_effect=_rename_no_replace_for_test,
    ):
        recovery._create_recovery_fence(
            destination,
            plan,
            manifest_path=manifest,
            manifest_sha256=manifest_sha256,
        )

    recovery._verify_fence_identity(destination, plan)
    assert not temp.exists() and not temp.is_symlink()
    assert recovery._stable_path_identity(marker, "test") == fixture["markerIdentity"]


@pytest.mark.parametrize("temp_state", ["content", "mode", "hardlink"])
def test_recovery_fence_rejects_foreign_or_tampered_known_temp(
    tmp_path: Path,
    temp_state: str,
) -> None:
    fixture = _build_fence_publish_fixture(tmp_path)
    plan = fixture["plan"]
    destination = fixture["destination"]
    temp = fixture["temp"]
    manifest = fixture["manifest"]
    assert isinstance(plan, dict)
    assert isinstance(destination, Path)
    assert isinstance(temp, Path)
    assert isinstance(manifest, Path)
    residue = plan["residue"]
    assert isinstance(residue, dict)
    expected = (str(residue["fenceContent"]) + "\n").encode("utf-8")
    if temp_state == "content":
        temp.write_bytes(b"not an expected prefix")
        os.chmod(temp, 0o600)
    elif temp_state == "mode":
        temp.write_bytes(expected[:4])
        os.chmod(temp, 0o666)
    else:
        foreign = temp.with_name("foreign-hardlink-source")
        foreign.write_bytes(expected[:4])
        os.chmod(foreign, 0o600)
        os.link(foreign, temp)

    before = temp.read_bytes()
    with pytest.raises(recovery.RecoveryError, match="recovery_fence_invalid"):
        recovery._create_recovery_fence(
            destination,
            plan,
            manifest_path=manifest,
            manifest_sha256=str(fixture["manifestSha256"]),
        )
    assert temp.read_bytes() == before
    assert not destination.exists() and not destination.is_symlink()


def test_recovery_fence_rejects_unknown_temp_entry(tmp_path: Path) -> None:
    fixture = _build_fence_publish_fixture(tmp_path)
    plan = fixture["plan"]
    root = fixture["root"]
    marker = fixture["marker"]
    candidate_slot = fixture["candidateSlot"]
    assert isinstance(plan, dict)
    assert isinstance(root, Path)
    assert isinstance(marker, Path)
    assert isinstance(candidate_slot, Path)
    (root / ".unknown-fence-temp").write_text("unknown\n", encoding="utf-8")

    with (
        mock.patch.object(
            recovery,
            "RESIDUE_PATHS",
            {
                "maintenance_marker": marker,
                "candidate_slot_link": candidate_slot,
            },
        ),
        pytest.raises(recovery.RecoveryError, match="unreviewed entries"),
    ):
        recovery._collect_residue_state(plan, "c" * 64)


def test_recovery_fence_temp_requires_exact_manifest_and_legacy_marker(
    tmp_path: Path,
) -> None:
    fixture = _build_fence_publish_fixture(tmp_path)
    plan = fixture["plan"]
    marker = fixture["marker"]
    destination = fixture["destination"]
    temp = fixture["temp"]
    manifest = fixture["manifest"]
    assert isinstance(plan, dict)
    assert isinstance(marker, Path)
    assert isinstance(destination, Path)
    assert isinstance(temp, Path)
    assert isinstance(manifest, Path)
    residue = plan["residue"]
    assert isinstance(residue, dict)
    expected = (str(residue["fenceContent"]) + "\n").encode("utf-8")
    temp.write_bytes(expected[:7])
    os.chmod(temp, 0o600)
    temp_before = temp.read_bytes()
    manifest_sha256 = str(fixture["manifestSha256"])

    manifest.write_text('{"contract":"tampered"}\n', encoding="utf-8")
    with pytest.raises(recovery.RecoveryError, match="manifest binding changed"):
        recovery._create_recovery_fence(
            destination,
            plan,
            manifest_path=manifest,
            manifest_sha256=manifest_sha256,
        )
    assert temp.read_bytes() == temp_before

    manifest.write_text('{"contract":"exact"}\n', encoding="utf-8")
    marker.write_text("changed legacy marker\n", encoding="utf-8")
    with pytest.raises(recovery.RecoveryError, match="residue identity changed"):
        recovery._create_recovery_fence(
            destination,
            plan,
            manifest_path=manifest,
            manifest_sha256=manifest_sha256,
        )
    assert temp.read_bytes() == temp_before


def test_schema_v3_quarantines_exact_inodes_and_finalizes_fence_last(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    sources = runtime / "sources"
    sources.mkdir(parents=True)
    marker = sources / "deployment-maintenance"
    slot_link = sources / "candidate-current"
    unit = sources / "candidate-unit"
    marker.write_text("legacy marker\n", encoding="utf-8")
    slot_link.symlink_to(tmp_path / "release-target")
    unit.write_text("[Service]\n", encoding="utf-8")
    retained = runtime / "previous-metadata.json"
    retained.write_text('{"commit":"old"}\n', encoding="utf-8")
    canonical = runtime / "canonical-nginx.conf"
    canonical.write_text("incident canonical\n", encoding="utf-8")
    quarantine_root = runtime / "quarantine" / "incident"
    absent = runtime / "scheduler.tsv"
    item_paths = {
        "maintenance_marker": marker,
        "candidate_slot_link": slot_link,
        "candidate_explicit_unit": unit,
    }
    quarantine_names = {
        "maintenance_marker": "legacy-maintenance-marker",
        "candidate_slot_link": "candidate-slot-link",
        "candidate_explicit_unit": "candidate-explicit-unit",
    }
    items = []
    original_inodes = {}
    for item_id, path in item_paths.items():
        identity = dict(recovery._stable_path_identity(path, "test"))
        original_inodes[item_id] = identity["inode"]
        items.append(
            {
                "id": item_id,
                "path": str(path),
                "quarantineName": quarantine_names[item_id],
                **identity,
            }
        )
    retained_identity = dict(recovery._stable_path_identity(retained, "test"))
    canonical_identity = dict(recovery._stable_path_identity(canonical, "test"))
    uid = os.getuid()
    gid = os.getgid()
    plan = {
        "schemaVersion": 3,
        "incidentId": "incident",
        "repository": "example/JATO_Analysis_System",
        "checkpoint": {
            "identity": {
                "repository": "example/JATO_Analysis_System",
                "commit": "d" * 40,
                "archiveSha256": "e" * 64,
                "archiveBytes": 1,
                "runId": 1,
                "runAttempt": 1,
                "frontendIdentity": "gha://example/artifact",
                "frontendChecksum": "f" * 64,
            }
        },
        "runtime": {"deploymentMarker": str(marker)},
        "residue": {
            "profile": "materialized_never_started",
            "quarantineRoot": str(quarantine_root),
            "quarantineDevice": runtime.stat().st_dev,
            "quarantineOwnerUid": uid,
            "quarantineOwnerGid": gid,
            "quarantineMode": "0700",
            "manifestName": "quarantine-contract.json",
            "finalFenceName": "recovery-fence-final",
            "fenceContent": (
                f"release={'d' * 40} status=recovery_in_progress incident=incident"
            ),
            "candidateUnit": {},
            "items": items,
            "retainedEvidence": [
                {"id": "previous_metadata", "path": str(retained), **retained_identity},
                {
                    "id": "canonical_nginx_config",
                    "path": str(canonical),
                    **canonical_identity,
                },
            ],
            "requiredAbsentPaths": [str(absent)],
        },
    }
    authorization = {
        "schemaVersion": 1,
        "kind": "checkpoint_recovery_dry_run_authorization",
        "repository": plan["repository"],
        "workflowPath": ".github/workflows/production-checkpoint-recovery.yml",
        "runId": 10,
        "runAttempt": 1,
        "mainSha": "b" * 40,
        "planSha256": "c" * 64,
        "resultSha256": "a" * 64,
        "incidentId": "incident",
        "inventoryDigest": recovery._residue_inventory_digest(plan),
        "decision": "candidate-residue-dry-run-eligible",
    }

    def rename_for_test(source: Path, destination: Path, flags: int) -> None:
        if flags == recovery.RENAME_EXCHANGE:
            temporary = source.parent / ".exchange"
            os.rename(source, temporary)
            os.rename(destination, source)
            os.rename(temporary, destination)
        else:
            assert not destination.exists() and not destination.is_symlink()
            os.rename(source, destination)

    with (
        mock.patch.object(recovery, "RESIDUE_PATHS", item_paths),
        mock.patch.object(recovery, "RESIDUE_QUARANTINE_NAMES", quarantine_names),
        mock.patch.object(checkpoint, "RESIDUE_MAINTENANCE_MARKER", marker),
        mock.patch.object(checkpoint, "RESIDUE_INCIDENT_ID", "incident"),
        mock.patch.object(checkpoint, "RESIDUE_TARGET_COMMIT", "d" * 40),
        mock.patch.object(checkpoint, "RESIDUE_DEVICE", runtime.stat().st_dev),
        mock.patch.object(checkpoint, "RESIDUE_OWNER_UID", uid),
        mock.patch.object(checkpoint, "RESIDUE_OWNER_GID", gid),
        mock.patch.object(checkpoint, "RESIDUE_RETAINED_OWNER_UID", uid),
        mock.patch.object(checkpoint, "RESIDUE_RETAINED_OWNER_GID", gid),
        mock.patch.object(checkpoint.os, "geteuid", return_value=0),
        mock.patch.object(recovery, "_renameat2", side_effect=rename_for_test),
        mock.patch.object(recovery, "_run_text", return_value=""),
        mock.patch.object(
            recovery,
            "_verify_candidate_detached_after_reload",
            return_value={"ownedSourceReferences": []},
        ),
    ):
        state = recovery._quarantine_candidate_residue(
            plan=plan,
            plan_sha256="c" * 64,
            implementation_commit="b" * 40,
            authorization=authorization,
            authorization_sha256="9" * 64,
        )
        assert state["stage"] == "quarantined_fenced"
        assert marker.read_text(encoding="utf-8") == (
            plan["residue"]["fenceContent"] + "\n"
        )
        for item in items:
            destination = quarantine_root / item["quarantineName"]
            assert destination.lstat().st_ino == original_inodes[item["id"]]
            if item["id"] != "maintenance_marker":
                assert not Path(item["path"]).exists()
                assert not Path(item["path"]).is_symlink()

        finalization = recovery._build_finalization_receipt(
            plan=plan,
            plan_sha256="c" * 64,
            implementation_commit="b" * 40,
            quarantine_state=state,
        )
        fence_inode = marker.stat().st_ino
        recovery._finalize_recovery_fence(plan, finalization)
        final_fence = quarantine_root / "recovery-fence-final"
        assert not marker.exists()
        assert final_fence.stat().st_ino == fence_inode

        receipt_root = runtime / "receipts"
        finalization_path = (
            receipt_root / "incident" / f"{'c' * 64}.finalization.json"
        )
        checkpoint.atomic_write_json(
            finalization_path,
            finalization,
            owner_uid=uid,
            owner_gid=gid,
        )
        operation_receipt = {
            "incidentId": plan["incidentId"],
            "identity": plan["checkpoint"]["identity"],
            "implementation": finalization["implementation"],
            "authorization": {
                **authorization,
                "authorizationSha256": "9" * 64,
            },
            "residue": {
                "quarantineRoot": str(quarantine_root),
                "manifestPath": state["manifestPath"],
                "manifestSha256": state["manifestSha256"],
                "items": [
                    {
                        "id": item["id"],
                        "sourcePath": item["path"],
                        "quarantinePath": str(
                            quarantine_root / item["quarantineName"]
                        ),
                        "identity": recovery._expected_identity(item),
                    }
                    for item in items
                ],
                "retainedEvidence": [
                    {
                        "id": "previous_metadata",
                        "path": str(retained),
                        "identity": recovery._expected_identity(
                            plan["residue"]["retainedEvidence"][0]
                        ),
                    },
                    {
                        "id": "canonical_nginx_config",
                        "path": str(canonical),
                        "identity": recovery._expected_identity(
                            plan["residue"]["retainedEvidence"][1]
                        ),
                    },
                ],
                "requiredAbsentPaths": [str(absent)],
            },
            "finalizationReceipt": {
                "path": str(finalization_path),
                "sha256": _sha256(finalization_path),
            },
        }
        checkpoint._validate_schema_v3_finalization(
            operation_receipt=operation_receipt,
            receipt_root=receipt_root,
            owner_uid=uid,
            owner_gid=gid,
            enforce_incident_runtime=True,
        )
        moved_item = next(item for item in items if item["id"] == "candidate_explicit_unit")
        moved_source = Path(moved_item["path"])
        moved_destination = quarantine_root / moved_item["quarantineName"]
        os.rename(moved_destination, moved_source)
        with pytest.raises(checkpoint.CheckpointError, match="quarantine is not settled"):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=True,
            )
        os.rename(moved_source, moved_destination)

        real_live_identity = checkpoint._live_identity

        def reject_private_traversal(path: Path, *, label: str):
            if path.is_relative_to(quarantine_root):
                raise AssertionError("unprivileged gate traversed private quarantine")
            return real_live_identity(path, label=label)

        with (
            mock.patch.object(checkpoint.os, "geteuid", return_value=1000),
            mock.patch.object(
                checkpoint,
                "_live_identity",
                side_effect=reject_private_traversal,
            ),
        ):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=True,
            )
            absent.write_text("unexpected\n", encoding="utf-8")
            with pytest.raises(
                checkpoint.CheckpointError,
                match="required recovery-absence invariant",
            ):
                checkpoint._validate_schema_v3_finalization(
                    operation_receipt=operation_receipt,
                    receipt_root=receipt_root,
                    owner_uid=uid,
                    owner_gid=gid,
                    enforce_incident_runtime=True,
                )
            absent.unlink()

        # Subsequent blue/green releases may legitimately recreate the shared
        # Candidate slot, service unit, active/runtime paths and canonical Nginx
        # file.  Historical validation must continue to bind the immutable
        # settlement evidence without pinning those mutable paths forever.
        slot_link.symlink_to(tmp_path / "successor-release-one")
        unit.write_text("[Service]\nEnvironment=SUCCESSOR_ONE=1\n", encoding="utf-8")
        absent.write_text("successor one runtime\n", encoding="utf-8")
        canonical.write_text("successor one canonical\n", encoding="utf-8")
        with (
            mock.patch.object(checkpoint.os, "geteuid", return_value=1000),
            mock.patch.object(
                checkpoint,
                "_live_identity",
                side_effect=reject_private_traversal,
            ),
        ):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=False,
            )

        slot_link.unlink()
        slot_link.symlink_to(tmp_path / "successor-release-two")
        unit.write_text("[Service]\nEnvironment=SUCCESSOR_TWO=1\n", encoding="utf-8")
        absent.write_text("successor two runtime\n", encoding="utf-8")
        canonical.write_text("successor two canonical\n", encoding="utf-8")
        with (
            mock.patch.object(checkpoint.os, "geteuid", return_value=1000),
            mock.patch.object(
                checkpoint,
                "_live_identity",
                side_effect=reject_private_traversal,
            ),
        ):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=False,
            )

        # Root may still audit the permanent private quarantine after mutable
        # successor paths have been rebuilt.
        with mock.patch.object(checkpoint.os, "geteuid", return_value=0):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=False,
            )

        marker.write_text("new release marker\n", encoding="utf-8")
        with pytest.raises(checkpoint.CheckpointError, match="not settled"):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=False,
            )
        marker.unlink()

        finalization_raw = finalization_path.read_bytes()
        finalization_path.write_bytes(finalization_raw + b" ")
        with pytest.raises(checkpoint.CheckpointError, match="SHA256 mismatch"):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=False,
            )
        finalization_path.write_bytes(finalization_raw)

        os.rename(moved_destination, moved_source.with_suffix(".quarantine-tamper"))
        with (
            mock.patch.object(checkpoint.os, "geteuid", return_value=0),
            pytest.raises(
                checkpoint.CheckpointError,
                match="quarantine is not settled",
            ),
        ):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=False,
            )
        os.rename(moved_source.with_suffix(".quarantine-tamper"), moved_destination)

        retained.write_text('{"commit":"changed"}\n', encoding="utf-8")
        with pytest.raises(checkpoint.CheckpointError, match="retained recovery evidence"):
            checkpoint._validate_schema_v3_finalization(
                operation_receipt=operation_receipt,
                receipt_root=receipt_root,
                owner_uid=uid,
                owner_gid=gid,
                enforce_incident_runtime=False,
            )
