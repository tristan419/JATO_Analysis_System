from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / ".github" / "scripts"))

import present_checkpoint_recovery_result as presenter  # noqa: E402


MAIN_SHA = "a" * 40
PLAN_PATH = (
    REPO_ROOT
    / ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue.json"
)
PLAN_SHA256 = hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> bytes:
    raw = json.dumps(payload, indent=1, ensure_ascii=False).encode("utf-8") + b"\n"
    path.write_bytes(raw)
    return raw


def _success_result() -> dict[str, object]:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    return {
        "decision": "candidate-residue-dry-run-eligible",
        "incidentId": plan["incidentId"],
        "mode": "dry-run",
        "implementationCommit": MAIN_SHA,
        "planSha256": PLAN_SHA256,
        "checkpointPhase": "migrated",
        "databaseRevisions": plan["expected"]["revisions"],
        "activeCommit": plan["expected"]["activeCommit"],
        "candidatePresent": False,
        "candidateStarted": False,
        "trafficChanged": False,
        "databaseChanged": False,
        "checkpointChanged": False,
        "mutationPerformed": False,
        "otherReleaseGate": "cross-release-safe",
        "targetIdentity": plan["checkpoint"]["identity"],
        "inventoryDigest": presenter.reviewed._inventory_digest(plan),
        "candidateResiduePresent": True,
    }


def _failure_result(*, mode: str = "dry-run") -> dict[str, object]:
    dry_run = mode == "dry-run"
    change_status: bool | None = False if dry_run else None
    return {
        "schemaVersion": 1,
        "kind": "checkpoint_recovery_failure",
        "decision": "dry-run-rejected" if dry_run else "apply-failed",
        "mode": mode,
        "stage": "initial_candidate_proof",
        "category": "candidate_runtime_invalid",
        "detail": "Candidate proof differs from the reviewed plan.",
        "fieldDiffs": [
            {
                "field": "name",
                "expected": "jato-fullstack-backend@8001.service",
                "actual": None,
            }
        ],
        "passed": [
            "active_backend_2_workers_6g_8g",
            "candidate_never_started",
            "nginx_unchanged",
            "database_read_only",
        ],
        "notReached": [
            "monthly_worker_disabled",
            "switch_unit_quiescent",
        ],
        "implementationCommit": MAIN_SHA,
        "planSha256": PLAN_SHA256,
        "trafficChanged": change_status,
        "databaseChanged": change_status,
        "jatoDataChanged": change_status,
        "checkpointChanged": change_status,
        "mutationPerformed": change_status,
    }


def _apply_success_result() -> dict[str, object]:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    payload = _success_result()
    payload.update(
        {
            "decision": "pre-switch-residue-quarantined-and-aborted",
            "mode": "apply",
            "checkpointPhase": "pre_switch_aborted",
            "checkpointSequence": plan["checkpoint"]["sequence"] + 1,
            "candidateResiduePresent": False,
            "checkpointChanged": True,
            "mutationPerformed": True,
            "receiptPath": "/state/recovery-receipt.json",
            "receiptSha256": "c" * 64,
            "receiptReused": False,
            "finalizationReceiptPath": "/state/finalization-receipt.json",
            "finalizationReceiptSha256": "d" * 64,
            "finalizationReceiptReused": False,
        }
    )
    return payload


def _terminal_success_result(*, mutation_performed: bool) -> dict[str, object]:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    return {
        "decision": "already-pre-switch-aborted",
        "incidentId": plan["incidentId"],
        "implementationCommit": MAIN_SHA,
        "planSha256": PLAN_SHA256,
        "checkpointPhase": "pre_switch_aborted",
        "receiptCreatedAt": "2026-08-04T00:00:00.000Z",
        "activeCommit": plan["expected"]["activeCommit"],
        "candidatePresent": False,
        "candidateStarted": False,
        "trafficChanged": False,
        "databaseChanged": False,
        "checkpointChanged": False,
        "mutationPerformed": mutation_performed,
        "mode": "apply",
    }


def test_valid_success_result_is_preserved_and_summarized(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    original = _write_json(result_path, _success_result())
    summary = tmp_path / "summary.md"

    assert presenter.present_result(
        result_path=result_path,
        summary_path=summary,
        plan_path=PLAN_PATH,
        step_outcome="success",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0

    assert result_path.read_bytes() == original
    summary_text = summary.read_text(encoding="utf-8")
    assert "candidate-residue-dry-run-eligible" in summary_text
    assert "Public traffic | No change" in summary_text


def test_valid_apply_success_result_is_preserved(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    original = _write_json(result_path, _apply_success_result())
    summary = tmp_path / "summary.md"

    assert presenter.present_result(
        result_path=result_path,
        summary_path=summary,
        plan_path=PLAN_PATH,
        step_outcome="success",
        mode="apply",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0

    assert result_path.read_bytes() == original
    assert "Recovery mutation | Changed" in summary.read_text(encoding="utf-8")


@pytest.mark.parametrize("mutation_performed", [False, True])
def test_terminal_apply_success_preserves_exact_mutation_fact(
    tmp_path: Path,
    mutation_performed: bool,
) -> None:
    result_path = tmp_path / "result.json"
    original = _write_json(
        result_path,
        _terminal_success_result(mutation_performed=mutation_performed),
    )
    summary = tmp_path / "summary.md"

    assert presenter.present_result(
        result_path=result_path,
        summary_path=summary,
        plan_path=PLAN_PATH,
        step_outcome="success",
        mode="apply",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0

    assert result_path.read_bytes() == original
    expected = "Changed" if mutation_performed else "No change"
    assert f"Recovery mutation | {expected}" in summary.read_text(
        encoding="utf-8"
    )


def test_structured_failure_is_preserved_and_summarized(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    original = _write_json(result_path, _failure_result())
    summary = tmp_path / "summary.md"

    assert presenter.present_result(
        result_path=result_path,
        summary_path=summary,
        plan_path=PLAN_PATH,
        step_outcome="failure",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0

    assert result_path.read_bytes() == original
    summary_text = summary.read_text(encoding="utf-8")
    assert "candidate_runtime_invalid" in summary_text
    assert "jato-fullstack-backend@8001.service" in summary_text
    assert "monthly_worker_disabled" in summary_text
    assert "failed result cannot be used" in summary_text


def test_structured_dry_run_failure_accepts_unknown_change_status(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    payload = _failure_result()
    for field in presenter.CHANGE_FIELDS:
        payload[field] = None
    original = _write_json(result_path, payload)

    assert presenter.present_result(
        result_path=result_path,
        summary_path=tmp_path / "summary.md",
        plan_path=PLAN_PATH,
        step_outcome="failure",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0
    assert result_path.read_bytes() == original


@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"",
        b"not-json\n",
        b'{"decision":"x","decision":"y"}\n',
    ],
)
def test_failed_step_without_valid_json_gets_safe_generic_report(
    tmp_path: Path,
    raw: bytes | None,
) -> None:
    result_path = tmp_path / "result.json"
    if raw is not None:
        result_path.write_bytes(raw)
    summary = tmp_path / "summary.md"

    assert presenter.present_result(
        result_path=result_path,
        summary_path=summary,
        plan_path=PLAN_PATH,
        step_outcome="failure",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0

    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["category"] == "unstructured_recovery_failure"
    assert all(payload[field] is None for field in presenter.CHANGE_FIELDS)
    assert "Unknown" in summary.read_text(encoding="utf-8")


def test_oversized_failed_result_gets_safe_generic_report(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    result_path.write_bytes(b"x" * (presenter.MAX_RESULT_BYTES + 1))

    assert presenter.present_result(
        result_path=result_path,
        summary_path=tmp_path / "summary.md",
        plan_path=PLAN_PATH,
        step_outcome="failure",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0

    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["category"] == "unstructured_recovery_failure"


def test_invalid_success_keeps_gate_failed_and_replaces_unsafe_result(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result.json"
    payload = _success_result()
    payload["trafficChanged"] = True
    _write_json(result_path, payload)

    assert presenter.present_result(
        result_path=result_path,
        summary_path=tmp_path / "summary.md",
        plan_path=PLAN_PATH,
        step_outcome="success",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 2

    replacement = json.loads(result_path.read_text(encoding="utf-8"))
    assert replacement["category"] == "result_validation_failed"
    assert all(replacement[field] is None for field in presenter.CHANGE_FIELDS)


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_success_requires_the_exact_reviewed_authorization_contract(
    tmp_path: Path,
    mutation: str,
) -> None:
    result_path = tmp_path / "result.json"
    payload = _success_result()
    if mutation == "missing":
        payload.pop("databaseRevisions")
    else:
        payload["unreviewedField"] = "must-fail"
    _write_json(result_path, payload)

    assert presenter.present_result(
        result_path=result_path,
        summary_path=tmp_path / "summary.md",
        plan_path=PLAN_PATH,
        step_outcome="success",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 2

    replacement = json.loads(result_path.read_text(encoding="utf-8"))
    assert replacement["category"] == "result_validation_failed"


def test_summary_escapes_diagnostic_markdown_and_html(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    payload = _failure_result()
    payload["detail"] = "bad | value <script>alert(1)</script> `code`"
    payload["fieldDiffs"] = [
        {
            "field": "dropInPaths",
            "expected": ["/safe/a|b"],
            "actual": ["<unsafe>"],
        }
    ]
    _write_json(result_path, payload)
    summary = tmp_path / "summary.md"

    assert presenter.present_result(
        result_path=result_path,
        summary_path=summary,
        plan_path=PLAN_PATH,
        step_outcome="failure",
        mode="dry-run",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0

    summary_text = summary.read_text(encoding="utf-8")
    assert "<script>" not in summary_text
    assert "&lt;script&gt;" in summary_text
    assert "&#124;" in summary_text


def test_apply_failure_requires_unknown_change_status(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    original = _write_json(result_path, _failure_result(mode="apply"))

    assert presenter.present_result(
        result_path=result_path,
        summary_path=tmp_path / "summary.md",
        plan_path=PLAN_PATH,
        step_outcome="failure",
        mode="apply",
        main_sha=MAIN_SHA,
        plan_sha256=PLAN_SHA256,
    ) == 0
    assert result_path.read_bytes() == original
