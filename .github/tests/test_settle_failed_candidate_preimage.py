from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = REPO_ROOT / ".github/scripts"
DEPLOY_DIR = REPO_ROOT / "03_Scripts/deploy"
PLAN = REPO_ROOT / (
    ".github/recovery-plans/2026-08-04-73980-candidate-preimage.json"
)
WORKFLOW = REPO_ROOT / ".github/workflows/settle-failed-candidate-preimage.yml"
TRANSPORT = REPO_ROOT / ".github/scripts/github_settle_failed_candidate_preimage.sh"

for directory in (SCRIPT_DIR, DEPLOY_DIR):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

spec = importlib.util.spec_from_file_location(
    "settle_failed_candidate_preimage",
    SCRIPT_DIR / "settle_failed_candidate_preimage.py",
)
assert spec and spec.loader
settlement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(settlement)


def test_reviewed_plan_binds_exact_failed_candidate() -> None:
    plan = settlement._load_plan(PLAN)

    assert plan["kind"] == "failed_candidate_preimage_settlement"
    assert plan["identity"]["commit"] == (
        "73980b57176301e1fb2f59663f42c292d9eb9e7e"
    )
    assert plan["identity"]["runId"] == 30915744371
    assert plan["expectedActiveCommit"] == (
        "cd4557cb932374a0fefb6c80a5fac9fb75a67d62"
    )


def test_candidate_role_paths_are_confined_to_inactive_slot() -> None:
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    roles = settlement._role_paths(plan, "8001")

    assert roles["slot_link"] == Path("/opt/jato/slots/8001/current")
    assert roles["slot_env"] == Path("/etc/jato-fullstack/slots/8001.env")
    assert roles["explicit_unit"] == Path(
        "/etc/systemd/system/jato-fullstack-backend@8001.service"
    )
    assert all("8000" not in str(path) for path in roles.values())


def test_check_mode_is_read_only_after_exact_preflight(tmp_path: Path) -> None:
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    lock = tmp_path / "production.lock"
    lock.write_text("lock\n", encoding="utf-8")
    plan["paths"]["productionLock"] = str(lock)
    identity = SimpleNamespace(to_dict=lambda: plan["identity"])
    checkpoint = tmp_path / "checkpoint.json"
    journal = tmp_path / "journal.jsonl"
    evidence_path = tmp_path / "evidence.json"
    checkpoint.write_text("{}\n", encoding="utf-8")
    journal.write_text("{}\n", encoding="utf-8")
    evidence_path.write_text("{}\n", encoding="utf-8")
    evidence = {"migration": {"status": "completed"}}
    preimage = tmp_path / "preimage"
    roles = {"slot_link": tmp_path / "slot"}

    with (
        mock.patch.object(settlement.os, "geteuid", return_value=0),
        mock.patch.object(
            settlement,
            "_verify_checkpoint_chain",
            return_value=(
                identity,
                evidence,
                checkpoint,
                journal,
                evidence_path,
                b"checkpoint",
                b"evidence",
            ),
        ),
        mock.patch.object(settlement, "_verify_database"),
        mock.patch.object(settlement, "_verify_previous_metadata"),
        mock.patch.object(settlement, "_require_absent"),
        mock.patch.object(
            settlement,
            "_verify_active",
            return_value=("8000", Path("/opt/JATO_Analysis_System-main")),
        ),
        mock.patch.object(settlement, "_candidate_slot", return_value="8001"),
        mock.patch.object(
            settlement,
            "_preimage_state",
            return_value=(preimage, roles, {}, "a" * 64),
        ),
        mock.patch.object(settlement.subprocess, "run") as run,
        mock.patch.object(settlement, "_invoke_preimage_helper") as invoke,
    ):
        result = settlement.settle(plan, apply=False)

    assert result["decision"] == "eligible"
    assert result["mutationPerformed"] is False
    run.assert_not_called()
    invoke.assert_not_called()


def test_workflow_is_main_only_production_approved_and_exact() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "github.ref == 'refs/heads/main'" in workflow
    assert "environment: production" in workflow
    assert "confirm_settlement" in workflow
    assert "30915744371" in workflow
    assert "production-release-main" in workflow
    assert "settle_failed_candidate_preimage.py" in workflow


def test_transport_keeps_large_payload_out_of_exported_environment() -> None:
    transport = TRANSPORT.read_text(encoding="utf-8")

    assert 'write_payload SETTLEMENT_HELPER "$helper"' in transport
    assert "export SETTLEMENT_HELPER_B64" not in transport
    assert '"umask 077; exec bash -s"' in transport
    assert "--mode check" in transport
    assert "--mode apply" in transport
