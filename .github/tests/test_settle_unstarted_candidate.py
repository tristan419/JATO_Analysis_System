from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / ".github/scripts/settle_unstarted_candidate.py"
WORKFLOW = REPO_ROOT / ".github/workflows/settle-unstarted-candidate.yml"
WRAPPER = REPO_ROOT / ".github/scripts/github_settle_unstarted_candidate.sh"
REVIEWED_PLAN = (
    REPO_ROOT
    / ".github/recovery-plans/2026-08-04-aa1d-unstarted-candidate.json"
)


def _module():
    specification = importlib.util.spec_from_file_location(
        "settle_unstarted_candidate_tested",
        HELPER,
    )
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _identity() -> dict[str, object]:
    return {
        "repository": "tristan419/JATO_Analysis_System",
        "commit": "a" * 40,
        "archiveSha256": "b" * 64,
        "archiveBytes": 1234,
        "runId": 123,
        "runAttempt": 1,
        "frontendIdentity": "gha://example/frontend",
        "frontendChecksum": "c" * 64,
    }


def _write_checkpoint_chain(module, root: Path) -> tuple[Path, Path]:
    checkpoint = root / "checkpoints/candidate.json"
    journal = root / "journals/candidate.jsonl"
    identity = module.ReleaseIdentity.from_mapping(_identity())
    for phase in (
        "packaged",
        "transport_verified",
        "prepared",
        "backup_verified",
        "migrated",
    ):
        module.write_checkpoint(
            checkpoint_path=checkpoint,
            journal_path=journal,
            identity=identity,
            phase=phase,
            status="completed",
            retry_class="automatic",
            message=f"test transition to {phase}",
        )
    return checkpoint, journal


def _plan(module, root: Path) -> dict[str, object]:
    checkpoint, journal = _write_checkpoint_chain(module, root)
    evidence = root / "checkpoints/candidate.evidence.json"
    evidence_payload = {
        "identity": _identity(),
        "backup": {
            "manifestPath": "/private/backup.json",
            "manifestBytes": 123,
            "manifestSha256": "d" * 64,
        },
        "migration": {
            "status": "completed",
            "preRevision": "20260715_0046 (head)",
            "targetRevision": "20260715_0046 (head)",
            "resultRevision": "20260715_0046 (head)",
        },
    }
    evidence_raw = (
        json.dumps(evidence_payload, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_bytes(evidence_raw)
    os.chmod(evidence, 0o600)
    lock = root / "production-deploy.lock"
    lock.touch()
    paths = {
        "stateRoot": str(root / "state"),
        "checkpoint": str(checkpoint),
        "journal": str(journal),
        "evidence": str(evidence),
        "releaseRoot": str(root / "release"),
        "activeSlotFile": str(root / "active-slot"),
        "slotsRoot": str(root / "slots"),
        "slotEnvRoot": str(root / "slot-env"),
        "backendEnvFile": str(root / "backend.env"),
        "candidatePreimageRoot": str(root / "preimages"),
        "candidatePreviewStateRoot": str(root / "preview"),
        "deploymentMarker": str(root / "deployment-maintenance"),
        "schedulerStateFile": str(root / "scheduler-state.tsv"),
        "productionLock": str(lock),
    }
    return {
        "schemaVersion": 1,
        "kind": "unstarted_candidate_settlement",
        "incidentId": "test-unstarted-candidate",
        "identity": _identity(),
        "checkpointSha256": hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
        "evidenceSha256": hashlib.sha256(evidence_raw).hexdigest(),
        "candidatePreimageHelperSha256": "e" * 64,
        "expectedActiveCommit": "f" * 40,
        "expectedDatabaseRevision": "20260715_0046",
        "paths": paths,
    }


def test_reviewed_plan_is_exact_and_loadable() -> None:
    module = _module()
    plan = module._load_plan(REVIEWED_PLAN)

    assert plan["incidentId"] == "2026-08-04-aa1d-procfs-boot-id-preimage"
    assert plan["identity"]["commit"] == (
        "aa1d424b284497497973dd2e57dc6059b3d1ab8b"
    )
    assert plan["expectedActiveCommit"] == (
        "cd4557cb932374a0fefb6c80a5fac9fb75a67d62"
    )


def test_checkpoint_chain_accepts_only_reviewed_migrated_tail(tmp_path: Path) -> None:
    module = _module()
    plan = _plan(module, tmp_path)

    identity, evidence, *_ = module._verify_checkpoint_chain(plan)

    assert identity.to_dict() == _identity()
    assert evidence["migration"]["preRevision"] == "20260715_0046 (head)"

    plan["checkpointSha256"] = "0" * 64
    with pytest.raises(module.SettlementError, match="checkpoint SHA-256"):
        module._verify_checkpoint_chain(plan)


def test_apply_records_terminal_without_touching_active_or_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    plan = _plan(module, tmp_path)
    active_root = tmp_path / "active-release"
    active_root.mkdir()
    calls: list[str] = []

    monkeypatch.setattr(module.os, "geteuid", lambda: 0)
    monkeypatch.setattr(module.os, "seteuid", lambda value: calls.append(f"uid:{value}"))
    monkeypatch.setattr(module.os, "setegid", lambda value: calls.append(f"gid:{value}"))
    monkeypatch.setattr(module, "_verify_old_helper", lambda value: calls.append("helper"))
    monkeypatch.setattr(
        module,
        "_verify_database",
        lambda value, evidence: calls.append("database"),
    )
    monkeypatch.setattr(
        module,
        "_verify_previous_metadata",
        lambda value: calls.append("previous"),
    )
    monkeypatch.setattr(
        module,
        "_verify_active",
        lambda value: (calls.append("active") or ("8000", active_root)),
    )
    monkeypatch.setattr(
        module,
        "_verify_candidate_absent",
        lambda value, slot: (calls.append("candidate") or "8001"),
    )

    result = module.settle(plan, apply=True)
    checkpoint = module.load_checkpoint(Path(plan["paths"]["checkpoint"]))

    assert result["decision"] == "candidate_prepare_aborted"
    assert result["active"]["changed"] is False
    assert result["databaseChanged"] is False
    assert result["trafficChanged"] is False
    assert result["jatoDataChanged"] is False
    assert checkpoint["phase"] == "candidate_prepare_aborted"
    assert checkpoint["status"] == "completed"
    assert checkpoint["retryClass"] == "automatic"
    assert calls.count("active") == 2
    assert calls.count("candidate") == 2
    receipt = Path(result["receipt"]["path"])
    assert receipt.is_file()
    assert hashlib.sha256(receipt.read_bytes()).hexdigest() == (
        result["receipt"]["sha256"]
    )


def test_old_helper_proof_requires_boot_id_before_capture(tmp_path: Path) -> None:
    module = _module()
    release_root = tmp_path / "release"
    helper = release_root / "03_Scripts/deploy/candidate_runtime_preimage.py"
    helper.parent.mkdir(parents=True)
    source = b"""\
def main():
    boot_id = _boot_id(arguments)
    roles = _role_paths(arguments)
    result = capture(preimage, identity, roles, authorization, boot_id)
"""
    helper.write_bytes(source)
    plan = {
        "paths": {"releaseRoot": str(release_root)},
        "candidatePreimageHelperSha256": hashlib.sha256(source).hexdigest(),
    }

    module._verify_old_helper(plan)

    helper.write_bytes(source.replace(b"roles =", b"result = capture()\n    roles ="))
    plan["candidatePreimageHelperSha256"] = hashlib.sha256(
        helper.read_bytes()
    ).hexdigest()
    with pytest.raises(module.SettlementError, match="before capture writes"):
        module._verify_old_helper(plan)


def test_workflow_is_main_only_serialized_and_never_deploys_active() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    wrapper = WRAPPER.read_text(encoding="utf-8")

    assert "github.ref == 'refs/heads/main'" in workflow
    assert "environment: production" in workflow
    assert "group: production-release-main" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "confirm_settlement" in workflow
    assert "30905118347" in workflow
    assert "2026-08-04-aa1d-unstarted-candidate.json" in workflow
    assert "production-release.yml" not in wrapper
    assert "nginx" not in wrapper.lower()
    assert "systemctl start" not in wrapper
    assert "systemctl restart" not in wrapper
    assert "StrictHostKeyChecking=yes" in wrapper
    assert "sudo -n env PYTHONPATH" in wrapper
