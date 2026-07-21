from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.api.routes import msrp_monthly_update
from app.core import security
from app.main import app
from app.services import jato_monthly_update_service


SOURCE_CANDIDATE_FINGERPRINT = "a" * 64
ACTIVE_FINGERPRINT = "b" * 64
REPORT_FINGERPRINT = "c" * 64
RESOLVED_CANDIDATE_FINGERPRINT = "d" * 64
OTHER_FINGERPRINT = "e" * 64
REQUEST_ID = "smart-merge-resume-request-001"


def _configure_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    project_root = tmp_path / "project"
    job_root = (
        project_root
        / "04_Processed_data"
        / "ops"
        / "jato_monthly_update_jobs"
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "PROJECT_ROOT",
        project_root,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "MONTHLY_UPDATE_JOB_ROOT",
        job_root,
    )
    return project_root, job_root


def _relative(project_root: Path, path: Path) -> str:
    return str(path.relative_to(project_root))


def _write_candidate_artifacts(
    *,
    project_root: Path,
    job_id: str,
    candidate_scope: str,
) -> dict[str, Any]:
    staging_root = (
        project_root
        / "04_Processed_data"
        / "staging"
        / f"{job_id}-candidate"
    )
    paths = {
        "stagingOutputPath": staging_root / "jato_full_archive.parquet",
        "manifestPath": staging_root / "manifest.json",
        "partitionOutputPath": staging_root / "partitioned_dataset_v1",
        "fingerprintPath": staging_root / "dataset_fingerprint.json",
        "refreshReportPath": staging_root / "refresh_job_report.json",
        "summariesOutputPath": staging_root / "summaries",
    }
    required_fields = {
        "stagingOutputPath",
        "manifestPath",
        "refreshReportPath",
    }
    if candidate_scope == "full_smart_merge":
        required_fields = set(paths)
    for field, path in paths.items():
        if field not in required_fields:
            continue
        if field in {"partitionOutputPath", "summariesOutputPath"}:
            path.mkdir(parents=True, exist_ok=True)
            (path / "marker.txt").write_text(field, encoding="utf-8")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(field, encoding="utf-8")
    return {
        "candidateScope": candidate_scope,
        **{
            field: _relative(project_root, path)
            for field, path in paths.items()
        },
    }


def _prepare_failed_smart_merge_job(
    *,
    project_root: Path,
    job_root: Path,
    job_id: str,
    candidate_scope: str = "target_country_partitions_only",
) -> tuple[dict[str, Any], dict[str, str]]:
    upload_path = job_root / job_id / "uploads" / "washed.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"retained washed source")
    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=job_id,
        month="2026-06",
        triggered_by="tester",
        upload_filename=upload_path.name,
        stored_upload_path=upload_path,
    )
    resolution = {
        "status": (
            "resolved" if candidate_scope == "full_smart_merge" else "failed"
        ),
        "activeBaseFingerprint": ACTIVE_FINGERPRINT,
        "sourceCandidateFingerprint": SOURCE_CANDIDATE_FINGERPRINT,
        "reportFingerprint": REPORT_FINGERPRINT,
        "decisions": [{"country": "德国", "decision": "keep_active"}],
    }
    if candidate_scope == "full_smart_merge":
        resolution["resolvedCandidateFingerprint"] = (
            RESOLVED_CANDIDATE_FINGERPRINT
        )
    state.update(
        {
            "status": "failed",
            "phase": "smart_merge_failed",
            "operation": "smart_merge",
            "error": "simulated Smart Merge memory failure",
            "activeBaseFingerprint": ACTIVE_FINGERPRINT,
            "historicalReclassificationResolution": resolution,
            "reviewApproval": None,
            "publication": None,
        }
    )
    state["artifacts"].update(
        _write_candidate_artifacts(
            project_root=project_root,
            job_id=job_id,
            candidate_scope=candidate_scope,
        )
    )
    jato_monthly_update_service._persist_job_state(state)
    seals = {
        "expected_source_candidate_fingerprint": (
            SOURCE_CANDIDATE_FINGERPRINT
        ),
        "expected_active_fingerprint": ACTIVE_FINGERPRINT,
        "expected_report_fingerprint": REPORT_FINGERPRINT,
        "expected_resolution_fingerprint": (
            jato_monthly_update_service
            ._historical_reclassification_resolution_fingerprint(resolution)
        ),
    }
    return state, seals


def _patch_resume_queue_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    launches: list[str],
) -> None:
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_monthly_update_worker_start_window",
        lambda **_kwargs: nullcontext(),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: ACTIVE_FINGERPRINT,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_validated_historical_reclassification_resolution",
        lambda _resolution: {"德国": "keep_active"},
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        launches.append,
    )


def _resume(
    *,
    job_id: str,
    request_id: str,
    seals: dict[str, str],
) -> dict[str, Any]:
    return jato_monthly_update_service.resume_failed_jato_smart_merge(
        job_id=job_id,
        triggered_by="admin-user",
        request_id=request_id,
        **seals,
    )


def test_resume_queues_original_job_without_new_job_or_etl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-smart-merge-resume-original"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
    )
    launches: list[str] = []
    etl_calls: list[str] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_job",
        lambda run_job_id: etl_calls.append(run_job_id),
    )
    before_jobs = sorted(
        path.parent.name for path in job_root.glob("*/job_state.json")
    )

    queued = _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)

    assert queued["jobId"] == job_id
    assert queued["pendingOperation"]["type"] == "smart_merge_resume"
    assert queued["pendingOperation"]["status"] == "queued"
    assert queued["pendingOperation"]["requestId"] == REQUEST_ID
    assert queued["pendingOperation"]["resumeMode"] == "merge_and_review"
    assert launches == [job_id]
    assert etl_calls == []
    assert sorted(
        path.parent.name for path in job_root.glob("*/job_state.json")
    ) == before_jobs == [job_id]
    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["upload"]["originalFilename"] == "washed.xlsx"
    assert persisted["historicalReclassificationResolution"]["decisions"] == [
        {"country": "德国", "decision": "keep_active"}
    ]


@pytest.mark.parametrize(
    "seal_field",
    [
        "expected_source_candidate_fingerprint",
        "expected_active_fingerprint",
        "expected_report_fingerprint",
        "expected_resolution_fingerprint",
    ],
)
def test_resume_rejects_each_changed_recovery_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    seal_field: str,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = f"jato-smart-merge-seal-{seal_field}"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
    )
    launches: list[str] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    changed_seals = {**seals, seal_field: OTHER_FINGERPRINT}

    with pytest.raises(HTTPException) as exc_info:
        _resume(
            job_id=job_id,
            request_id=REQUEST_ID,
            seals=changed_seals,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "SMART_MERGE_RESUME_SEAL_CHANGED"
    assert exc_info.value.detail["blockerType"] == (
        "smart_merge_resume_seal_changed"
    )
    assert launches == []
    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert "pendingOperation" not in persisted
    assert persisted["status"] == "failed"


@pytest.mark.parametrize("operation_status", ["queued", "running", "success", "failed"])
def test_same_request_id_replays_queued_running_and_terminal_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation_status: str,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = f"jato-smart-merge-replay-{operation_status}"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
    )
    launches: list[str] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    first = _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)
    operation_id = first["pendingOperation"]["operationId"]
    state = jato_monthly_update_service._load_job_state(job_id)
    state["pendingOperation"]["status"] = operation_status
    jato_monthly_update_service._persist_job_state(state)
    launches.clear()

    def forbidden_worker_gate(**_kwargs: object) -> nullcontext:
        raise AssertionError("idempotent replay entered the worker start gate")

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_monthly_update_worker_start_window",
        forbidden_worker_gate,
    )
    replay = _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)

    assert replay["pendingOperation"]["operationId"] == operation_id
    assert replay["pendingOperation"]["status"] == operation_status
    assert launches == ([job_id] if operation_status == "queued" else [])


@pytest.mark.parametrize("operation_status", ["queued", "running"])
def test_different_request_id_conflicts_with_active_resume_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation_status: str,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = f"jato-smart-merge-conflict-{operation_status}"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
    )
    launches: list[str] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    first = _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)
    state = jato_monthly_update_service._load_job_state(job_id)
    state["pendingOperation"]["status"] = operation_status
    jato_monthly_update_service._persist_job_state(state)
    launches.clear()

    with pytest.raises(HTTPException) as exc_info:
        _resume(
            job_id=job_id,
            request_id="smart-merge-resume-request-002",
            seals=seals,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == (
        "SMART_MERGE_RESUME_OPERATION_IN_PROGRESS"
    )
    assert launches == []
    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["pendingOperation"]["operationId"] == (
        first["pendingOperation"]["operationId"]
    )


def test_resume_worker_rejects_source_candidate_content_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-smart-merge-worker-source-drift"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
    )
    launches: list[str] = []
    merge_calls: list[str] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_candidate_fingerprint_id",
        lambda _artifacts: OTHER_FINGERPRINT,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_smart_merge",
        lambda run_job_id, **_kwargs: merge_calls.append(run_job_id),
    )

    jato_monthly_update_service._run_active_bundle_operation(
        job_id=job_id,
        operation_type="smart_merge_resume",
    )

    persisted = jato_monthly_update_service._load_job_state(job_id)
    operation = persisted["pendingOperation"]
    assert operation["status"] == "failed"
    assert operation["failureDigest"]["technicalDetail"]["blockerType"] == (
        "candidate_content_drift"
    )
    assert operation["failureDigest"]["technicalDetail"][
        "expectedCandidateFingerprint"
    ] == SOURCE_CANDIDATE_FINGERPRINT
    assert operation["failureDigest"]["technicalDetail"][
        "actualCandidateFingerprint"
    ] == OTHER_FINGERPRINT
    assert merge_calls == []


def test_resume_worker_does_not_mark_success_when_smart_merge_swallows_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-smart-merge-worker-swallowed-failure"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
    )
    launches: list[str] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_candidate_fingerprint_id",
        lambda _artifacts: SOURCE_CANDIDATE_FINGERPRINT,
    )

    swallowed_failure_digest = {
        "code": "SMART_MERGE_RESOURCE_FAILURE",
        "category": "resource",
        "phase": "smart_merge",
        "retryable": True,
        "message": "swallowed merge failure",
        "sourceFeedback": None,
        "technicalDetail": None,
        "nextAction": "resume_smart_merge",
    }

    def swallow_failure(
        run_job_id: str,
        **_kwargs: object,
    ) -> None:
        failed = jato_monthly_update_service._load_job_state(run_job_id)
        failed["status"] = "failed"
        failed["phase"] = "smart_merge_failed"
        failed["error"] = "swallowed merge failure"
        failed["failureDigest"] = swallowed_failure_digest
        jato_monthly_update_service._persist_job_state(failed)

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_smart_merge",
        swallow_failure,
    )

    jato_monthly_update_service._run_active_bundle_operation(
        job_id=job_id,
        operation_type="smart_merge_resume",
    )

    persisted = jato_monthly_update_service._load_job_state(job_id)
    operation = persisted["pendingOperation"]
    assert persisted["status"] == "failed"
    assert persisted["phase"] == "smart_merge_failed"
    assert operation["status"] == "failed"
    assert operation["error"] == "swallowed merge failure"
    assert operation["failureDigest"] == swallowed_failure_digest


def test_committed_full_bundle_resume_only_rebuilds_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-smart-merge-review-only-resume"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
        candidate_scope="full_smart_merge",
    )
    active_parquet = (
        project_root / "04_Processed_data" / "jato_full_archive.parquet"
    )
    active_parquet.parent.mkdir(parents=True, exist_ok=True)
    active_parquet.write_text("active", encoding="utf-8")
    launches: list[str] = []
    validation_calls: list[dict[str, Path]] = []
    review_calls: list[tuple[str, str | None, str | None]] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    queued = _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)
    operation_id = queued["pendingOperation"]["operationId"]
    assert queued["pendingOperation"]["resumeMode"] == "review_only"
    queued_state = jato_monthly_update_service._load_job_state(job_id)
    queued_state["historicalReclassificationResolution"][
        "reviewBuildError"
    ] = "old review failure"
    queued_state["historicalReclassificationResolution"][
        "reviewBuildFailedAt"
    ] = "2026-07-21T00:00:00+00:00"
    jato_monthly_update_service._persist_job_state(queued_state)

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_validate_candidate_full_bundle",
        lambda **kwargs: validation_calls.append(kwargs) or {},
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_cache_jato_monthly_update_review",
        lambda review_job_id, **kwargs: review_calls.append(
            (
                review_job_id,
                kwargs.get("review_generation_id"),
                kwargs.get("expected_active_fingerprint"),
            )
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_smart_merge_parquet_streaming",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("committed bundle must not be merged again")
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_logged_command",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("committed bundle must not rebuild artifacts")
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_smart_merge_resume_bundle_is_durable",
        lambda **_kwargs: True,
    )

    jato_monthly_update_service._run_active_bundle_operation(
        job_id=job_id,
        operation_type="smart_merge_resume",
    )

    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["status"] == "success"
    assert persisted["phase"] == "completed"
    assert persisted["pendingOperation"]["status"] == "success"
    assert persisted["pendingOperation"]["phase"] == "completed"
    assert persisted["artifacts"]["candidateScope"] == "full_smart_merge"
    assert persisted["historicalReclassificationResolution"][
        "resolvedCandidateFingerprint"
    ] == RESOLVED_CANDIDATE_FINGERPRINT
    assert "reviewBuildError" not in (
        persisted["historicalReclassificationResolution"]
    )
    assert "reviewBuildFailedAt" not in (
        persisted["historicalReclassificationResolution"]
    )
    assert len(validation_calls) == 1
    assert review_calls == [(job_id, operation_id, ACTIVE_FINGERPRINT)]


def test_review_memory_failure_preserves_committed_bundle_and_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-smart-merge-review-memory-retry"
    _state, seals = _prepare_failed_smart_merge_job(
        project_root=project_root,
        job_root=job_root,
        job_id=job_id,
        candidate_scope="full_smart_merge",
    )
    active_parquet = (
        project_root / "04_Processed_data" / "jato_full_archive.parquet"
    )
    active_parquet.parent.mkdir(parents=True, exist_ok=True)
    active_parquet.write_text("active", encoding="utf-8")
    launches: list[str] = []
    _patch_resume_queue_runtime(monkeypatch, launches=launches)
    queued = _resume(job_id=job_id, request_id=REQUEST_ID, seals=seals)
    assert queued["pendingOperation"]["resumeMode"] == "review_only"

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_validate_candidate_full_bundle",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_release_worker_memory_before_review",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_cache_jato_monthly_update_review",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            MemoryError("Unable to allocate 14.8 MiB")
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_smart_merge_parquet_streaming",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("committed bundle must not be merged again")
        ),
    )

    jato_monthly_update_service._run_active_bundle_operation(
        job_id=job_id,
        operation_type="smart_merge_resume",
    )

    persisted = jato_monthly_update_service._load_job_state(job_id)
    resolution = persisted["historicalReclassificationResolution"]
    assert persisted["status"] == "failed"
    assert persisted["phase"] == "smart_merge_failed"
    assert persisted["artifacts"]["candidateScope"] == "full_smart_merge"
    assert resolution["status"] == "resolved"
    assert resolution["resolvedCandidateFingerprint"] == (
        RESOLVED_CANDIDATE_FINGERPRINT
    )
    assert resolution["decisions"] == [
        {"country": "德国", "decision": "keep_active"}
    ]
    digest = persisted["failureDigest"]
    assert digest["code"] == "MEMORY_LIMIT_EXCEEDED"
    assert digest["phase"] == "building_review"
    assert digest["nextAction"] == "resume_smart_merge"
    assert persisted["pendingOperation"]["status"] == "failed"
    assert persisted["pendingOperation"]["failureDigest"] == digest
    assert (
        jato_monthly_update_service._smart_merge_recovery_view(persisted)[
            "canResume"
        ]
        is True
    )


def test_smart_merge_resume_route_is_admin_only_and_forwards_all_seals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    editor_token = "smart-merge-resume-editor-token"
    admin_token = "smart-merge-resume-admin-token"
    monkeypatch.setitem(security.TOKEN_ROLE_MAP, editor_token, "editor")
    monkeypatch.setitem(security.TOKEN_ROLE_MAP, admin_token, "admin")
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        msrp_monthly_update,
        "resume_failed_jato_smart_merge",
        lambda **kwargs: captured.append(kwargs)
        or {
            "jobId": kwargs["job_id"],
            "pendingOperation": {
                "type": "smart_merge_resume",
                "status": "queued",
            },
        },
    )
    client = TestClient(app)
    body = {
        "requestId": REQUEST_ID,
        "expectedSourceCandidateFingerprint": (
            SOURCE_CANDIDATE_FINGERPRINT
        ),
        "expectedActiveFingerprint": ACTIVE_FINGERPRINT,
        "expectedReportFingerprint": REPORT_FINGERPRINT,
        "expectedResolutionFingerprint": OTHER_FINGERPRINT,
    }
    url = (
        "/v1/msrp/monthly-update-jobs/"
        "jato-smart-merge-route/smart-merge-resume"
    )

    forbidden = client.post(
        url,
        headers={
            "X-Auth-Token": editor_token,
            "X-User-Name": "editor-user",
        },
        json=body,
    )
    allowed = client.post(
        url,
        headers={
            "X-Auth-Token": admin_token,
            "X-User-Name": "admin-user",
        },
        json=body,
    )

    assert forbidden.status_code == 403
    assert allowed.status_code == 200
    assert captured == [
        {
            "job_id": "jato-smart-merge-route",
            "triggered_by": "admin-user",
            "request_id": REQUEST_ID,
            "expected_source_candidate_fingerprint": (
                SOURCE_CANDIDATE_FINGERPRINT
            ),
            "expected_active_fingerprint": ACTIVE_FINGERPRINT,
            "expected_report_fingerprint": REPORT_FINGERPRINT,
            "expected_resolution_fingerprint": OTHER_FINGERPRINT,
        }
    ]
