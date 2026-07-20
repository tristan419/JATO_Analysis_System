from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


DIRECTORY_BUNDLE_KEYS = {"partition", "summaries"}


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


def _write_bundle_node(path: Path, key: str, marker: str) -> None:
    if key in DIRECTORY_BUNDLE_KEYS:
        path.mkdir(parents=True, exist_ok=True)
        (path / "marker.txt").write_text(marker, encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(marker, encoding="utf-8")


def _read_bundle_node(path: Path, key: str) -> str:
    if key in DIRECTORY_BUNDLE_KEYS:
        return (path / "marker.txt").read_text(encoding="utf-8")
    return path.read_text(encoding="utf-8")


def _prepare_switched_transaction(
    *,
    tmp_path: Path,
    job_id: str,
    transaction_id: str,
) -> tuple[dict[str, Path], Path]:
    active_paths = jato_monthly_update_service._active_data_paths()
    staged_root = tmp_path / "staged"
    staged_paths: dict[str, Path | None] = {}
    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS:
        active_path = active_paths[key]
        staged_path = staged_root / active_path.name
        _write_bundle_node(active_path, key, f"old-{key}")
        _write_bundle_node(staged_path, key, f"new-{key}")
        staged_paths[key] = staged_path

    backup_dir = active_paths["backupRoot"] / transaction_id
    jato_monthly_update_service._swap_staged_active_bundle(
        staged_paths=staged_paths,
        active_paths=active_paths,
        backup_dir=backup_dir,
        transaction_metadata={
            "transactionId": transaction_id,
            "jobId": job_id,
            "operationType": "publish",
        },
    )
    return active_paths, backup_dir


def _ready_upload_state(
    *,
    upload_id: str,
    filename: str,
    assembled_path: Path,
    active_dataset_version: str,
) -> dict[str, Any]:
    file_sha256 = hashlib.sha256(assembled_path.read_bytes()).hexdigest()
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename=filename,
        size_bytes=assembled_path.stat().st_size,
        resume_key=f"resume-{upload_id}",
        triggered_by="tester",
    )
    state.update(
        {
            "status": "ready",
            "completedAt": "2026-07-20T00:00:00+00:00",
            "assembledPath": jato_monthly_update_service._relative_to_project(
                assembled_path
            ),
            "fileSha256": file_sha256,
            "ingestDigest": {
                "fileSha256": file_sha256,
                "sizeBytes": assembled_path.stat().st_size,
                "route": "single_country",
                "activeDatasetVersion": active_dataset_version,
                "countries": ["匈牙利"],
                "countryLatestMonths": {"匈牙利": "2026-06"},
                "latestMonth": "2026-06",
                "blockers": [],
                "warnings": [],
            },
        }
    )
    return state


def test_switched_transaction_with_durable_publication_commits_without_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_project(tmp_path, monkeypatch)
    job_id = "jato-durable-publish"
    transaction_id = "publish-durable-transaction"
    active_paths, backup_dir = _prepare_switched_transaction(
        tmp_path=tmp_path,
        job_id=job_id,
        transaction_id=transaction_id,
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "publication": {
                "publishedAt": "2026-07-20T01:00:00+00:00",
                "activeTransactionId": transaction_id,
            },
        }
    )

    recovered = (
        jato_monthly_update_service._recover_incomplete_active_transactions(
            active_paths
        )
    )

    assert recovered == []
    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS:
        assert _read_bundle_node(active_paths[key], key) == f"new-{key}"
        assert (
            _read_bundle_node(backup_dir / active_paths[key].name, key)
            == f"old-{key}"
        )
    journal = jato_monthly_update_service._read_json(
        backup_dir / jato_monthly_update_service.ACTIVE_TRANSACTION_FILENAME
    )
    assert journal["status"] == "committed"
    assert journal["committedByRecovery"] is True


def test_switched_transaction_without_durable_publication_restores_old_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_project(tmp_path, monkeypatch)
    active_paths, backup_dir = _prepare_switched_transaction(
        tmp_path=tmp_path,
        job_id="jato-missing-publication",
        transaction_id="publish-uncommitted-transaction",
    )

    recovered = (
        jato_monthly_update_service._recover_incomplete_active_transactions(
            active_paths
        )
    )

    assert recovered == [str(backup_dir)]
    for key in jato_monthly_update_service.ACTIVE_BUNDLE_KEYS:
        assert _read_bundle_node(active_paths[key], key) == f"old-{key}"
    journal = jato_monthly_update_service._read_json(
        backup_dir / jato_monthly_update_service.ACTIVE_TRANSACTION_FILENAME
    )
    assert journal["status"] == "recovered"


def test_cross_type_operation_conflicts_and_stale_completion_is_ignored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-operation-cas"
    old_operation = {
        "operationId": "publish-old",
        "type": "publish",
        "status": "queued",
        "requestedAt": "2026-07-20T00:00:00+00:00",
        "requestedBy": "publisher",
        "startedAt": None,
        "finishedAt": None,
        "error": None,
        "failureDigest": None,
    }
    state = {
        "jobId": job_id,
        "status": "success",
        "phase": "completed",
        "pendingOperation": old_operation,
    }
    jato_monthly_update_service._persist_job_state(state)

    with pytest.raises(HTTPException) as conflict:
        jato_monthly_update_service._queue_active_bundle_operation(
            payload=jato_monthly_update_service._load_job_state(job_id),
            operation_type="rollback",
            triggered_by="operator",
        )

    assert conflict.value.status_code == 409
    assert (
        jato_monthly_update_service._load_job_state(job_id)[
            "pendingOperation"
        ]["operationId"]
        == "publish-old"
    )

    replacement_operation = {
        "operationId": "rollback-new",
        "type": "rollback",
        "status": "queued",
        "requestedAt": "2026-07-20T00:01:00+00:00",
        "requestedBy": "operator",
        "startedAt": None,
        "finishedAt": None,
        "error": None,
        "failureDigest": None,
    }

    def replace_operation_during_publish(**_kwargs: Any) -> dict[str, str]:
        latest = jato_monthly_update_service._load_job_state(job_id)
        latest["pendingOperation"] = dict(replacement_operation)
        jato_monthly_update_service._persist_job_state(latest)
        return {"jobId": job_id}

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_execute_publish_jato_monthly_update_job",
        replace_operation_during_publish,
    )

    jato_monthly_update_service._run_active_bundle_operation(
        job_id=job_id,
        operation_type="publish",
    )

    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["pendingOperation"] == replacement_operation


def test_summaries_only_change_updates_active_dataset_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_project(tmp_path, monkeypatch)
    summaries_root = (
        jato_monthly_update_service._active_data_paths()["summaries"]
    )
    summaries_root.mkdir(parents=True)
    summary_path = summaries_root / "country_summary.json"
    summary_path.write_text('{"Hungary": "2026-05"}', encoding="utf-8")
    before = jato_monthly_update_service._active_dataset_version()

    summary_path.write_text('{"Hungary": "2026-06"}', encoding="utf-8")
    after = jato_monthly_update_service._active_dataset_version()

    assert before != after


def test_ingestion_key_includes_active_dataset_version() -> None:
    common_digest = {
        "fileSha256": "b" * 64,
        "route": "single_country",
        "countryLatestMonths": {"匈牙利": "2026-06"},
    }

    first = jato_monthly_update_service._build_ingestion_key(
        {**common_digest, "activeDatasetVersion": "active-v1"}
    )
    second = jato_monthly_update_service._build_ingestion_key(
        {**common_digest, "activeDatasetVersion": "active-v2"}
    )

    assert first != second


def test_ready_stale_digest_requeues_same_assembled_file_without_reupload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_project(tmp_path, monkeypatch)
    upload_id = "upload-ready-stale"
    filename = "JATO-2026.06-Hungary.xlsx"
    assembled_path = (
        jato_monthly_update_service._upload_session_assembled_path(
            upload_id,
            filename,
        )
    )
    assembled_path.parent.mkdir(parents=True)
    assembled_path.write_bytes(b"same-assembled-upload")
    state = _ready_upload_state(
        upload_id=upload_id,
        filename=filename,
        assembled_path=assembled_path,
        active_dataset_version="active-v1",
    )
    jato_monthly_update_service._persist_upload_session(state)
    original_inode = assembled_path.stat().st_ino
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-v2",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        lambda launched_upload_id: (
            launches.append(launched_upload_id) or 4242
        ),
    )

    with pytest.raises(HTTPException) as refreshing:
        jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=upload_id,
            triggered_by="tester",
        )

    assert refreshing.value.status_code == 409
    assert refreshing.value.detail["code"] == (
        "STALE_UPLOAD_DIGEST_REFRESHING"
    )
    assert refreshing.value.detail["startNewUploadRequired"] is False
    assert launches == [upload_id]
    persisted = jato_monthly_update_service._load_upload_session(upload_id)
    assert persisted["status"] == "digesting"
    assert persisted["ingestDigest"] is None
    assert persisted["digestPid"] == 4242
    assert persisted["digestAttempts"] == 1
    assert persisted["assembledPath"] == state["assembledPath"]
    assert assembled_path.read_bytes() == b"same-assembled-upload"
    assert assembled_path.stat().st_ino == original_inode
    assert not jato_monthly_update_service._upload_session_chunk_dir(
        upload_id
    ).exists()


def test_consumed_stale_digest_requires_new_upload_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_project(tmp_path, monkeypatch)
    upload_id = "upload-consumed-stale"
    filename = "JATO-2026.06-Hungary.xlsx"
    assembled_path = (
        jato_monthly_update_service._upload_session_assembled_path(
            upload_id,
            filename,
        )
    )
    assembled_path.parent.mkdir(parents=True)
    assembled_path.write_bytes(b"consumed-upload")
    state = _ready_upload_state(
        upload_id=upload_id,
        filename=filename,
        assembled_path=assembled_path,
        active_dataset_version="active-v1",
    )
    state["status"] = "consumed"
    state["consumedJobId"] = "jato-old-active-job"
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-v2",
    )

    with pytest.raises(HTTPException) as stale:
        jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=upload_id,
            triggered_by="tester",
        )

    assert stale.value.status_code == 409
    assert stale.value.detail["code"] == "STALE_UPLOAD_DIGEST"
    assert stale.value.detail["startNewUploadRequired"] is True


def test_full_candidate_exact_static_duplicates_fail_closed(
    tmp_path: Path,
) -> None:
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Hungary",
                "Make": "DFSK",
                "Model": "T5 EVO",
                "Powertrain type": "ICE",
                "2026 Apr": 0,
                "2026 May": 10,
            },
            {
                "Country": "Hungary",
                "Make": "DFSK",
                "Model": "T5 EVO",
                "Powertrain type": "ICE",
                "2026 Apr": 0,
                "2026 May": 12,
            },
        ]
    ).to_parquet(candidate_path, index=False)

    result = (
        jato_monthly_update_service
        ._find_candidate_duplicate_configurations(candidate_path)
    )

    assert result[0]["country"] == "Hungary"
    assert result[0]["duplicateRows"] == 2
    assert result[0]["duplicateGroupCount"] == 1


def test_historical_analysis_dimension_redistribution_is_blocked() -> None:
    active = pd.DataFrame(
        [
            {
                "Country": "Hungary",
                "Make": "DFSK",
                "Model": "T5 EVO",
                "Powertrain type": "ICE",
                "2026 Apr": 10,
            },
            {
                "Country": "Hungary",
                "Make": "DFSK",
                "Model": "T5 EVO",
                "Powertrain type": "BEV",
                "2026 Apr": 20,
            },
        ]
    )
    candidate = active.copy()
    candidate.loc[0, "2026 Apr"] = 5
    candidate.loc[1, "2026 Apr"] = 25

    result = (
        jato_monthly_update_service
        ._single_country_historical_sales_stability(
            country="Hungary",
            active_frame=active,
            candidate_frame=candidate,
            active_latest_month="2026 Apr",
        )
    )

    assert result["status"] == "fail"
    assert result["reason"] == (
        "historical_analysis_dimension_reclassification"
    )
    assert result["makeModelMismatchCount"] == 0
    assert result["analysisDimensionMismatchCount"] == 2


def test_abandon_upload_releases_maintenance_gate_without_touching_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    upload_id = "upload-abandon"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="resume",
        triggered_by="tester",
    )
    state["status"] = "ready"
    jato_monthly_update_service._persist_upload_session(state)

    abandoned = (
        jato_monthly_update_service
        .abandon_jato_monthly_update_upload(
            upload_id=upload_id,
            triggered_by="tester",
        )
    )

    assert abandoned["status"] == "abandoned"
    assert (
        jato_monthly_update_service._active_upload_session_payloads()
        == []
    )
    assert abandoned["failureDigest"]["code"] == (
        "UPLOAD_SESSION_ABANDONED"
    )


def test_terminate_process_group_refuses_pid_identity_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda _pid: True,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_read_process_identity",
        lambda _pid: {
            "startTimeTicks": "new-process",
            "cmdlineSha256": "b" * 64,
            "arguments": ["python", "other-service.py"],
        },
    )
    monkeypatch.setattr(
        jato_monthly_update_service.os,
        "killpg",
        lambda pid, sig: signals.append((pid, sig)),
    )

    result = jato_monthly_update_service._terminate_process_group(
        1234,
        expected_identity={
            "startTimeTicks": "old-process",
            "cmdlineSha256": "a" * 64,
        },
    )

    assert result["identityVerified"] is False
    assert result["sigtermSent"] is False
    assert signals == []
