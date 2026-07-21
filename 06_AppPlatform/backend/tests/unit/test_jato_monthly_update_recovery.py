from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


def _configure_failed_partial_recovery_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    project_root = tmp_path / "project"
    job_root = project_root / "04_Processed_data" / "ops" / "jobs"
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
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-recovery",
    )

    active_fingerprint = "a" * 64
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: active_fingerprint,
    )

    source_job_id = "jato-update-source"
    upload_id = "jato-upload-source"
    filename = "JATO-2026.06-16-countries.xlsx"
    content = b"retained-washed-jato-file"
    file_sha256 = hashlib.sha256(content).hexdigest()
    source_upload_path = jato_monthly_update_service._job_upload_storage_path(
        source_job_id,
        filename,
    )
    source_upload_path.parent.mkdir(parents=True, exist_ok=True)
    source_upload_path.write_bytes(content)
    digest = {
        "schemaVersion": 1,
        "status": "ready",
        "fileSha256": file_sha256,
        "sizeBytes": len(content),
        "sheetName": "Data Export",
        "route": "partial_country",
        "candidateScope": "target_country_partitions_only",
        "countries": ["捷克", "丹麦"],
        "countryLatestMonths": {
            "捷克": "2026-06",
            "丹麦": "2026-06",
        },
        "activeLatestMonths": {
            "捷克": "2026-05",
            "丹麦": "2026-05",
        },
        "latestMonth": "2026-06",
        "dataRowCount": 2,
        "advancedCountries": ["捷克", "丹麦"],
        "unchangedCountries": [],
        "regressedCountries": [],
        "activeDatasetVersion": active_fingerprint,
        "blockers": [],
        "warnings": [],
    }
    ingestion_key = jato_monthly_update_service._build_ingestion_key(digest)
    source_state = {
        "jobId": source_job_id,
        "month": "2026-06",
        "batchId": "2026-06-partial-2c",
        "status": "failed",
        "phase": "worker_lost",
        "jobType": "partial_country",
        "country": None,
        "countryScope": ["捷克", "丹麦"],
        "triggeredBy": "uploader",
        "createdAt": "2026-07-21T00:00:00+00:00",
        "updatedAt": "2026-07-21T00:01:00+00:00",
        "startedAt": "2026-07-21T00:00:30+00:00",
        "finishedAt": "2026-07-21T00:01:00+00:00",
        "error": "worker lost",
        "ingestionKey": ingestion_key,
        "ingestDigest": digest,
        "failureDigest": {
            "code": "WORKER_LOST",
            "category": "resource",
        },
        "activeBaseFingerprint": active_fingerprint,
        "upload": {
            "originalFilename": filename,
            "storedPath": jato_monthly_update_service._relative_to_project(
                source_upload_path
            ),
            "sizeBytes": len(content),
            "sha256": file_sha256,
        },
        "plan": None,
        "artifacts": {
            "jobDir": jato_monthly_update_service._relative_to_project(
                jato_monthly_update_service._job_dir(source_job_id)
            ),
            "logPath": jato_monthly_update_service._relative_to_project(
                jato_monthly_update_service._job_log_path(source_job_id)
            ),
        },
        "summaries": {},
        "logPath": jato_monthly_update_service._relative_to_project(
            jato_monthly_update_service._job_log_path(source_job_id)
        ),
    }
    jato_monthly_update_service._persist_job_state(source_state)

    upload_state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename=filename,
        size_bytes=len(content),
        resume_key="retained-upload",
        triggered_by="uploader",
    )
    upload_state.update(
        {
            "status": "consumed",
            "uploadedBytes": len(content),
            "fileSha256": file_sha256,
            "ingestDigest": digest,
            "ingestionKey": ingestion_key,
            "consumedJobId": source_job_id,
        }
    )
    jato_monthly_update_service._persist_upload_session(upload_state)
    return {
        "jobRoot": job_root,
        "sourceJobId": source_job_id,
        "uploadId": upload_id,
        "filename": filename,
        "content": content,
        "fileSha256": file_sha256,
        "sourceUploadPath": source_upload_path,
        "digest": digest,
        "ingestionKey": ingestion_key,
        "activeFingerprint": active_fingerprint,
    }


def test_recover_failed_job_creates_one_idempotent_immutable_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    source_state_path = jato_monthly_update_service._job_state_path(
        context["sourceJobId"]
    )
    source_upload_state_path = jato_monthly_update_service._upload_session_state_path(
        context["uploadId"]
    )
    source_state_before = source_state_path.read_bytes()
    source_upload_state_before = source_upload_state_path.read_bytes()
    source_file_before = context["sourceUploadPath"].read_bytes()
    launched_states: list[dict[str, Any]] = []

    def capture_launch(job_id: str) -> None:
        launched_states.append(jato_monthly_update_service._load_job_state(job_id))

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        capture_launch,
    )
    recovery_key = "recovery-20260721-fixed"

    created = jato_monthly_update_service.recover_failed_jato_monthly_update_job(
        source_job_id=context["sourceJobId"],
        recovery_key=recovery_key,
        triggered_by="admin-user",
    )
    replay = jato_monthly_update_service.recover_failed_jato_monthly_update_job(
        source_job_id=context["sourceJobId"],
        recovery_key=recovery_key,
        triggered_by="admin-user",
    )

    assert replay["jobId"] == created["jobId"]
    assert created["jobId"] != context["sourceJobId"]
    assert created["status"] == "queued"
    assert created["recoveryOfJobId"] == context["sourceJobId"]
    assert created["recoveryKey"] == recovery_key
    assert created["ingestionKey"] == context["ingestionKey"]
    assert created["ingestDigest"] == context["digest"]
    assert created["activeBaseFingerprint"] == context["activeFingerprint"]
    assert created["recoverySource"] == {
        "uploadId": context["uploadId"],
        "validatedAt": created["recoverySource"]["validatedAt"],
        "sizeBytes": len(context["content"]),
        "sha256": context["fileSha256"],
        "activeBaseFingerprint": context["activeFingerprint"],
        "storageMethod": "hardlink",
    }
    assert len(launched_states) == 1
    assert launched_states[0]["recoveryOfJobId"] == context["sourceJobId"]
    assert launched_states[0]["recoveryKey"] == recovery_key

    recovery_path = jato_monthly_update_service._project_path(
        created["upload"]["storedPath"]
    )
    assert recovery_path is not None
    assert recovery_path.read_bytes() == context["content"]
    assert recovery_path.stat().st_ino == context["sourceUploadPath"].stat().st_ino
    assert source_state_path.read_bytes() == source_state_before
    assert source_upload_state_path.read_bytes() == source_upload_state_before
    assert context["sourceUploadPath"].read_bytes() == source_file_before
    recovery_states = list(context["jobRoot"].glob("jato-update-*/job_state.json"))
    assert len(recovery_states) == 2


def test_concurrent_recovery_replay_launches_one_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        launches.append,
    )
    barrier = threading.Barrier(3)
    results: list[dict[str, Any]] = []
    errors: list[BaseException] = []

    def recover() -> None:
        barrier.wait(timeout=3)
        try:
            results.append(
                jato_monthly_update_service.recover_failed_jato_monthly_update_job(
                    source_job_id=context["sourceJobId"],
                    recovery_key="recovery-concurrent-key",
                    triggered_by="admin-user",
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    workers = [threading.Thread(target=recover) for _ in range(2)]
    for worker in workers:
        worker.start()
    barrier.wait(timeout=3)
    for worker in workers:
        worker.join(timeout=5)

    assert all(not worker.is_alive() for worker in workers)
    assert errors == []
    assert len(results) == 2
    assert results[0]["jobId"] == results[1]["jobId"]
    assert launches == [results[0]["jobId"]]


def test_recover_failed_job_uses_copy_fallback_and_verifies_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda _job_id: None,
    )
    monkeypatch.setattr(
        jato_monthly_update_service.os,
        "link",
        lambda _source, _target: (_ for _ in ()).throw(OSError("cross-device")),
    )

    created = jato_monthly_update_service.recover_failed_jato_monthly_update_job(
        source_job_id=context["sourceJobId"],
        recovery_key="recovery-copy-fallback",
        triggered_by="admin-user",
    )

    recovery_path = jato_monthly_update_service._project_path(
        created["upload"]["storedPath"]
    )
    assert recovery_path is not None
    assert recovery_path.read_bytes() == context["content"]
    assert created["recoverySource"]["storageMethod"] == "copy"


def test_recover_failed_job_rejects_changed_source_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    context["sourceUploadPath"].write_bytes(b"tampered")
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        launches.append,
    )

    with pytest.raises(HTTPException) as blocked:
        jato_monthly_update_service.recover_failed_jato_monthly_update_job(
            source_job_id=context["sourceJobId"],
            recovery_key="recovery-source-tampered",
            triggered_by="admin-user",
        )

    assert blocked.value.status_code == 409
    assert blocked.value.detail["code"] == "RECOVERY_SOURCE_FILE_MISMATCH"
    assert launches == []


def test_recover_failed_job_rejects_changed_active_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda _job_id: pytest.fail("stale recovery must not launch"),
    )

    with pytest.raises(HTTPException) as blocked:
        jato_monthly_update_service.recover_failed_jato_monthly_update_job(
            source_job_id=context["sourceJobId"],
            recovery_key="recovery-stale-active",
            triggered_by="admin-user",
        )

    assert blocked.value.status_code == 409
    assert blocked.value.detail["code"] == "RECOVERY_ACTIVE_LINEAGE_CHANGED"


@pytest.mark.parametrize("failure_category", ["data", "processing", "input_validation"])
def test_recover_failed_job_rejects_non_platform_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_category: str,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    source_state = jato_monthly_update_service._load_job_state(context["sourceJobId"])
    source_state["failureDigest"] = {
        "code": "SOURCE_DATA_FAILED",
        "category": failure_category,
    }
    jato_monthly_update_service._persist_job_state(source_state)
    source_state_before = jato_monthly_update_service._job_state_path(
        context["sourceJobId"]
    ).read_bytes()
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda _job_id: pytest.fail("ineligible recovery must not launch"),
    )

    with pytest.raises(HTTPException) as blocked:
        jato_monthly_update_service.recover_failed_jato_monthly_update_job(
            source_job_id=context["sourceJobId"],
            recovery_key="recovery-data-failure",
            triggered_by="admin-user",
        )

    assert blocked.value.status_code == 409
    assert blocked.value.detail["code"] == "RECOVERY_FAILURE_NOT_ELIGIBLE"
    assert (
        jato_monthly_update_service._job_state_path(context["sourceJobId"]).read_bytes()
        == source_state_before
    )


@pytest.mark.parametrize(
    ("guard_field", "guard_value", "expected_code"),
    [
        (
            "publication",
            {"publishedAt": "2026-07-21T00:00:00+00:00"},
            "RECOVERY_SOURCE_HAS_PUBLICATION",
        ),
        (
            "reviewApproval",
            {"decision": "approved"},
            "RECOVERY_SOURCE_HAS_APPROVAL",
        ),
        (
            "pendingOperation",
            {"type": "publish", "status": "failed"},
            "RECOVERY_SOURCE_HAS_PENDING_OPERATION",
        ),
    ],
)
def test_recover_failed_job_rejects_publish_review_or_operation_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_field: str,
    guard_value: dict[str, str],
    expected_code: str,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    source_state = jato_monthly_update_service._load_job_state(context["sourceJobId"])
    source_state[guard_field] = guard_value
    jato_monthly_update_service._persist_job_state(source_state)

    with pytest.raises(HTTPException) as blocked:
        jato_monthly_update_service.recover_failed_jato_monthly_update_job(
            source_job_id=context["sourceJobId"],
            recovery_key="recovery-guard-check",
            triggered_by="admin-user",
        )

    assert blocked.value.status_code == 409
    assert blocked.value.detail["code"] == expected_code


def test_recover_failed_job_blocks_second_key_for_same_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _configure_failed_partial_recovery_source(tmp_path, monkeypatch)
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        launches.append,
    )
    first = jato_monthly_update_service.recover_failed_jato_monthly_update_job(
        source_job_id=context["sourceJobId"],
        recovery_key="recovery-first-key",
        triggered_by="admin-user",
    )

    with pytest.raises(HTTPException) as blocked:
        jato_monthly_update_service.recover_failed_jato_monthly_update_job(
            source_job_id=context["sourceJobId"],
            recovery_key="recovery-second-key",
            triggered_by="admin-user",
        )

    assert blocked.value.status_code == 409
    assert blocked.value.detail["code"] == "RECOVERY_ALREADY_CREATED"
    assert blocked.value.detail["jobIds"] == [first["jobId"]]
    assert launches == [first["jobId"]]
