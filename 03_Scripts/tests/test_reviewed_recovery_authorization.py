from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "03_Scripts" / "deploy"))

import reviewed_recovery_authorization as reviewed  # noqa: E402


PLAN_PATH = (
    REPO_ROOT
    / ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue.json"
)
MAIN_SHA = "a" * 40
REPOSITORY = "tristan419/JATO_Analysis_System"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _valid_result() -> dict[str, object]:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    return {
        "decision": "candidate-residue-dry-run-eligible",
        "incidentId": plan["incidentId"],
        "implementationCommit": MAIN_SHA,
        "planSha256": _sha256(PLAN_PATH),
        "checkpointPhase": "migrated",
        "databaseRevisions": plan["expected"]["revisions"],
        "activeCommit": plan["expected"]["activeCommit"],
        "candidatePresent": False,
        "candidateStarted": False,
        "trafficChanged": False,
        "databaseChanged": False,
        "checkpointChanged": False,
        "mutationPerformed": False,
        "mode": "dry-run",
        "otherReleaseGate": "cross-release-safe",
        "targetIdentity": plan["checkpoint"]["identity"],
        "inventoryDigest": reviewed._inventory_digest(plan),
        "candidateResiduePresent": True,
    }


def _write_result(path: Path, payload: dict[str, object]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_freeze_and_revalidate_share_one_exact_contract(tmp_path: Path) -> None:
    result_path = tmp_path / "checkpoint-recovery-result.json"
    _write_result(result_path, _valid_result())
    output = tmp_path / "frozen"
    reviewed.freeze_reviewed_dry_run(
        argparse.Namespace(
            result=result_path,
            plan=PLAN_PATH,
            expected_result_sha256=_sha256(result_path),
            expected_main_sha=MAIN_SHA,
            expected_plan_sha256=_sha256(PLAN_PATH),
            repository=REPOSITORY,
            run_id=12345,
            run_attempt=2,
            output_dir=output,
        )
    )

    assert {path.name for path in output.iterdir()} == {
        "checkpoint-recovery-result.json",
        "reviewed-dry-run-authorization.json",
    }
    assert stat_mode(output) == 0o700
    assert all(stat_mode(path) == 0o600 for path in output.iterdir())
    reviewed.verify_frozen_review(
        argparse.Namespace(
            result=output / "checkpoint-recovery-result.json",
            authorization=output / "reviewed-dry-run-authorization.json",
            plan=PLAN_PATH,
            expected_result_sha256=_sha256(result_path),
            expected_main_sha=MAIN_SHA,
            expected_plan_sha256=_sha256(PLAN_PATH),
            repository=REPOSITORY,
            run_id=12345,
        )
    )


def stat_mode(path: Path) -> int:
    return os.stat(path).st_mode & 0o777


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("trafficChanged", True),
        ("candidateResiduePresent", False),
        ("inventoryDigest", "0" * 64),
    ],
)
def test_freeze_rejects_review_result_drift(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    result = _valid_result()
    result[field] = value
    result_path = tmp_path / "checkpoint-recovery-result.json"
    _write_result(result_path, result)

    with pytest.raises(
        reviewed.AuthorizationError,
        match="result contract",
    ):
        reviewed.validate_reviewed_dry_run(
            result_path=result_path,
            plan_path=PLAN_PATH,
            expected_result_sha256=_sha256(result_path),
            expected_main_sha=MAIN_SHA,
            expected_plan_sha256=_sha256(PLAN_PATH),
        )


def test_revalidate_rejects_authorization_tamper(tmp_path: Path) -> None:
    result_path = tmp_path / "checkpoint-recovery-result.json"
    _write_result(result_path, _valid_result())
    output = tmp_path / "frozen"
    arguments = argparse.Namespace(
        result=result_path,
        plan=PLAN_PATH,
        expected_result_sha256=_sha256(result_path),
        expected_main_sha=MAIN_SHA,
        expected_plan_sha256=_sha256(PLAN_PATH),
        repository=REPOSITORY,
        run_id=12345,
        run_attempt=2,
        output_dir=output,
    )
    reviewed.freeze_reviewed_dry_run(arguments)
    authorization_path = output / "reviewed-dry-run-authorization.json"
    authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    authorization["runId"] = 999
    authorization_path.write_text(
        json.dumps(authorization, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(reviewed.AuthorizationError, match="binding changed"):
        reviewed.validate_authorization(
            authorization_path=authorization_path,
            repository=REPOSITORY,
            run_id=12345,
            main_sha=MAIN_SHA,
            plan_sha256=_sha256(PLAN_PATH),
            result_sha256=_sha256(result_path),
            inventory_digest=_valid_result()["inventoryDigest"],
        )


def test_reviewed_document_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"a": 1, "a": 2}\n', encoding="utf-8")

    with pytest.raises(reviewed.AuthorizationError, match="duplicate JSON key"):
        reviewed._read_document(duplicate, "test document")
