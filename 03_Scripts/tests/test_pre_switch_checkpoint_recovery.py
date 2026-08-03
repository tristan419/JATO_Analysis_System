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


@pytest.mark.parametrize("schema_version", [True, 0, 3])
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


@pytest.mark.parametrize("schema_version", [True, 0, 3])
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
