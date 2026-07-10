import hashlib

from fastapi.testclient import TestClient

from app.api.routes import msrp_monthly_update
from app.main import app
from app.services import jato_monthly_update_service


def _configure_baseline_dirs(
    tmp_path,
    monkeypatch,
    *,
    active_baseline_name: str | None = None,
    archived_baseline_name: str | None = None,
) -> None:
    project_root = tmp_path / "project"
    raw_root = project_root / "01_RAW_DATA"
    baseline_root = raw_root / "baseline"
    history_root = raw_root / "historyDataArchive"

    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "RAW_DATA_ROOT", raw_root)
    monkeypatch.setattr(jato_monthly_update_service, "BASELINE_ROOT", baseline_root)
    monkeypatch.setattr(jato_monthly_update_service, "PATCHES_ROOT", raw_root / "patches")
    monkeypatch.setattr(
        jato_monthly_update_service, "HISTORY_ARCHIVE_ROOT", history_root
    )

    baseline_root.mkdir(parents=True, exist_ok=True)
    (history_root / "baseline").mkdir(parents=True, exist_ok=True)

    if active_baseline_name is not None:
        (baseline_root / active_baseline_name).write_bytes(b"active-baseline")
    if archived_baseline_name is not None:
        ((history_root / "baseline") / archived_baseline_name).write_bytes(
            b"archived-baseline"
        )


def _headers() -> dict[str, str]:
    return {
        # Unknown dev token resolves to the permissive local admin context.
        "X-Auth-Token": "monthly-update-route-test-admin",
        "X-User-Name": "tester",
    }


def test_create_monthly_update_job_route_persists_job(
    tmp_path, monkeypatch
) -> None:
    _configure_baseline_dirs(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.2-full-baseline.xlsx",
    )
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(
        jato_monthly_update_service, "_launch_job_thread", lambda job_id: None
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_detect_latest_month_from_upload",
        lambda _path: "2026-03",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-r1",
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs",
        headers=_headers(),
        files={
            "file": (
                "patch.xlsx",
                b"not-a-real-xlsx-but-nonempty",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["month"] is None
    assert payload["batchId"] is None
    assert payload["status"] == "queued"
    assert payload["phase"] == "queued"
    assert payload["triggeredBy"] == "tester"
    assert payload["upload"]["originalFilename"] == "patch.xlsx"
    assert len(list(job_root.glob("*/job_state.json"))) == 1


def test_list_and_detail_monthly_update_job_routes_return_saved_state(
    tmp_path, monkeypatch
) -> None:
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )

    job_id = "jato-update-1234abcd"
    upload_path = job_root / job_id / "uploads" / "patch.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"fake")
    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=job_id,
        month="2026-03",
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=upload_path,
    )
    state["status"] = "success"
    state["phase"] = "completed"
    state["summaries"] = {
        "refresh": {
            "jobStatus": "success",
            "rowCount": 123,
        }
    }
    jato_monthly_update_service._persist_job_state(state)
    (job_root / job_id / "job.log").write_text(
        "line1\nline2\nline3\n", encoding="utf-8"
    )

    client = TestClient(app)
    list_response = client.get(
        "/v1/msrp/monthly-update-jobs?limit=10", headers=_headers()
    )
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert list_payload["rows"] == 1
    assert list_payload["items"][0]["jobId"] == job_id
    assert "logTail" not in list_payload["items"][0]

    detail_response = client.get(
        f"/v1/msrp/monthly-update-jobs/{job_id}", headers=_headers()
    )
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()["item"]
    assert detail_payload["jobId"] == job_id
    assert detail_payload["status"] == "success"
    assert detail_payload["logTail"] == "line1\nline2\nline3"


def test_monthly_update_cleanup_route_returns_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "run_jato_monthly_update_cleanup",
        lambda *, triggered_by, cleanup_tier: {
            "cleanedAt": "2026-04-16T00:00:00+00:00",
            "triggeredBy": triggered_by,
            "cleanupTier": cleanup_tier,
            "activeBaselinePath": "01_RAW_DATA/baseline/JATO-2026.3-full-baseline.xlsx",
            "activePatchMonth": "2026-03",
            "freedBytes": 12345,
            "archivedBaselineCount": 1,
            "archivedBaselines": [
                "01_RAW_DATA/historyDataArchive/baseline/JATO-2026.2-full-baseline.xlsx"
            ],
            "archivedPatchDirCount": 1,
            "archivedPatchDirs": ["01_RAW_DATA/historyDataArchive/patches/2026-02"],
            "removedUploadSessionDirCount": 1,
            "removedUploadSessionDirs": [
                "04_Processed_data/ops/jato_monthly_update_jobs/_upload_sessions/session-a"
            ],
            "removedJobUploadDirCount": 2,
            "removedJobUploadDirs": [
                "04_Processed_data/ops/jato_monthly_update_jobs/job-a/uploads",
                "04_Processed_data/ops/jato_monthly_update_jobs/job-b/uploads",
            ],
            "deletedReviewDirCount": 1,
            "deletedReviewDirs": ["04_Processed_data/reviews/raw_compare/2026-01_vs_2026-03"],
            "deletedStagingDirCount": 1,
            "deletedStagingDirs": ["04_Processed_data/staging/2026-03-r1-mixed"],
            "deletedRefreshBackupDirCount": 1,
            "deletedRefreshBackupDirs": ["04_Processed_data/.refresh_backups/manual-promote-1"],
            "deletedArchivedBaselineCount": 1,
            "deletedArchivedBaselines": ["01_RAW_DATA/historyDataArchive/baseline/old.xlsx"],
            "deletedArchivedPatchDirCount": 1,
            "deletedArchivedPatchDirs": ["01_RAW_DATA/historyDataArchive/patches/2026-01"],
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-maintenance/cleanup",
        json={"cleanupTier": "cautious"},
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["triggeredBy"] == "tester"
    assert payload["cleanupTier"] == "cautious"
    assert payload["archivedBaselineCount"] == 1
    assert payload["removedUploadSessionDirCount"] == 1
    assert payload["removedJobUploadDirCount"] == 2


def test_monthly_update_maintenance_status_route_returns_storage_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "get_jato_monthly_update_maintenance_status",
        lambda: {
            "checkedAt": "2026-04-20T12:00:00+00:00",
            "activeBaselinePath": "01_RAW_DATA/baseline/JATO-2026.3-full-baseline.xlsx",
            "activeBaselineSource": "active",
            "latestPatchBatch": "2026-03-r1",
            "jobCount": 5,
            "uploadSessionCount": 2,
            "trackedStorageBytes": 123456,
            "storageMetrics": [
                {
                    "key": "job-upload-copies",
                    "label": "Job upload copies",
                    "bytes": 2048,
                    "fileCount": 2,
                    "dirCount": 1,
                    "paths": ["04_Processed_data/ops/jato_monthly_update_jobs/job-a/uploads"],
                }
            ],
        },
    )

    client = TestClient(app)
    response = client.get(
        "/v1/msrp/monthly-update-maintenance/status",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["activeBaselineSource"] == "active"
    assert payload["latestPatchBatch"] == "2026-03-r1"
    assert payload["trackedStorageBytes"] == 123456


def test_monthly_update_promote_baseline_route_returns_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "promote_current_active_to_baseline",
        lambda *, triggered_by: {
            "promotedAt": "2026-04-20T12:30:00+00:00",
            "triggeredBy": triggered_by,
            "sourceParquetPath": "04_Processed_data/jato_full_archive.parquet",
            "baselinePath": "01_RAW_DATA/baseline/JATO-2026.3-full-21countries-baseline.xlsx",
            "detectedLatestMonth": "2026-03",
            "countryCount": 21,
            "rowCount": 1272500,
            "archivedBaselineCount": 1,
            "archivedBaselines": [
                "01_RAW_DATA/historyDataArchive/baseline/JATO-2026.2-full-21countries-baseline.xlsx"
            ],
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-maintenance/promote-baseline",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["triggeredBy"] == "tester"
    assert payload["detectedLatestMonth"] == "2026-03"
    assert payload["archivedBaselineCount"] == 1


def test_chunked_monthly_update_upload_routes_create_job(
    tmp_path, monkeypatch
) -> None:
    _configure_baseline_dirs(
        tmp_path,
        monkeypatch,
        archived_baseline_name="JATO-2026.1-full-baseline.xlsx",
    )
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(jato_monthly_update_service, "UPLOAD_CHUNK_SIZE_BYTES", 4)
    monkeypatch.setattr(
        jato_monthly_update_service, "_launch_job_thread", lambda job_id: None
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_detect_latest_month_from_upload",
        lambda _path: "2026-03",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-r1",
    )

    client = TestClient(app)
    initiate_response = client.post(
        "/v1/msrp/monthly-update-uploads/initiate",
        headers=_headers(),
        json={"filename": "JATO-2026.03-patch.xlsx", "sizeBytes": 10, "resumeKey": "resume-key-1"},
    )
    assert initiate_response.status_code == 200
    upload_id = initiate_response.json()["item"]["uploadId"]
    assert initiate_response.json()["item"]["totalChunks"] == 3
    reinitiate_response = client.post(
        "/v1/msrp/monthly-update-uploads/initiate",
        headers=_headers(),
        json={"filename": "JATO-2026.03-patch.xlsx", "sizeBytes": 10, "resumeKey": "resume-key-1"},
    )
    assert reinitiate_response.status_code == 200
    assert reinitiate_response.json()["item"]["uploadId"] == upload_id

    chunk_one = client.put(
        f"/v1/msrp/monthly-update-uploads/{upload_id}/parts/1",
        headers={
            **_headers(),
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": hashlib.sha256(b"abcd").hexdigest(),
        },
        content=b"abcd",
    )
    assert chunk_one.status_code == 200

    status_response = client.get(
        f"/v1/msrp/monthly-update-uploads/{upload_id}",
        headers=_headers(),
    )
    assert status_response.status_code == 200
    assert status_response.json()["item"]["receivedChunks"] == [1]

    chunk_two = client.put(
        f"/v1/msrp/monthly-update-uploads/{upload_id}/parts/2",
        headers={
            **_headers(),
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": hashlib.sha256(b"efgh").hexdigest(),
        },
        content=b"efgh",
    )
    assert chunk_two.status_code == 200

    chunk_three = client.put(
        f"/v1/msrp/monthly-update-uploads/{upload_id}/parts/3",
        headers={
            **_headers(),
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": hashlib.sha256(b"ij").hexdigest(),
        },
        content=b"ij",
    )
    assert chunk_three.status_code == 200

    complete_response = client.post(
        f"/v1/msrp/monthly-update-uploads/{upload_id}/complete",
        headers=_headers(),
    )
    assert complete_response.status_code == 200
    assert complete_response.json()["item"]["status"] == "completed"
    assert complete_response.json()["item"]["fileSha256"] == hashlib.sha256(b"abcdefghij").hexdigest()

    create_response = client.post(
        "/v1/msrp/monthly-update-jobs/from-upload",
        headers=_headers(),
        json={"uploadId": upload_id},
    )
    assert create_response.status_code == 200
    payload = create_response.json()["item"]
    assert payload["status"] == "queued"
    assert payload["month"] is None
    assert payload["requestedMonth"] == "2026-03"
    assert payload["batchId"] is None
    assert payload["phase"] == "queued"
    assert payload["upload"]["sizeBytes"] == 10
    assert payload["upload"]["sha256"] == hashlib.sha256(b"abcdefghij").hexdigest()


def test_retry_failed_monthly_update_job_route_requeues_existing_upload(
    tmp_path, monkeypatch
) -> None:
    _configure_baseline_dirs(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.2-full-baseline.xlsx",
    )
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(
        jato_monthly_update_service, "_launch_job_thread", lambda job_id: None
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_detect_latest_month_from_upload",
        lambda _path: "2026-03",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-r2",
    )

    source_job_id = "jato-update-failed"
    source_upload = job_root / source_job_id / "uploads" / "patch.xlsx"
    source_upload.parent.mkdir(parents=True, exist_ok=True)
    source_upload.write_bytes(b"retry-me")
    failed_state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=source_job_id,
        month="2026-03",
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=source_upload,
        file_sha256=hashlib.sha256(b"retry-me").hexdigest(),
    )
    failed_state["status"] = "failed"
    failed_state["phase"] = "failed"
    failed_state["error"] = "prepare exploded"
    jato_monthly_update_service._persist_job_state(failed_state)

    client = TestClient(app)
    response = client.post(
        f"/v1/msrp/monthly-update-jobs/{source_job_id}/retry",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] != source_job_id
    assert payload["status"] == "queued"
    assert payload["month"] is None
    assert payload["requestedMonth"] == "2026-03"
    assert payload["batchId"] is None
    assert payload["phase"] == "queued"
    assert payload["triggeredBy"] == "tester"
    assert payload["upload"]["sha256"] == hashlib.sha256(b"retry-me").hexdigest()
    assert payload["artifacts"]["retriedFromJobId"] == source_job_id


def test_recheck_monthly_update_job_route_returns_updated_job(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "recheck_jato_monthly_update_job",
        lambda *, job_id, triggered_by: {
            "jobId": job_id,
            "status": "failed",
            "phase": "stale_failed",
            "triggeredBy": triggered_by,
            "runtimeCheck": {"resolvedAs": "stale_failed"},
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/recheck",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] == "jato-update-1234abcd"
    assert payload["phase"] == "stale_failed"
    assert payload["runtimeCheck"]["resolvedAs"] == "stale_failed"
    assert payload["triggeredBy"] == "tester"


def test_cancel_monthly_update_job_route_returns_cancelled_job(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "cancel_jato_monthly_update_job",
        lambda *, job_id, triggered_by: {
            "jobId": job_id,
            "status": "cancelled",
            "phase": "cancelled",
            "error": f"Cancelled by {triggered_by} during raw_compare",
            "cancellation": {"cancelledBy": triggered_by},
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/cancel",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] == "jato-update-1234abcd"
    assert payload["status"] == "cancelled"
    assert payload["cancellation"]["cancelledBy"] == "tester"


def test_get_monthly_update_review_route_returns_review_bundle(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "get_jato_monthly_update_review",
        lambda job_id: {
            "jobId": job_id,
            "compareId": "2026-02_vs_2026-03",
            "decisionSuggestion": "manual_review_required",
            "compareKeyColumns": ["国家", "MakeModel"],
            "checklistMarkdown": "## checklist",
            "reviewFindings": [],
            "sampledCountries": ["DE"],
            "conflictSampleCount": 1,
            "countryFreshnessSummary": [
                {
                    "country": "DE",
                    "oldLatestMonth": "2026 Jan",
                    "newLatestMonth": "2026 Mar",
                    "freshnessStatus": "advanced",
                    "rowDelta": 12,
                }
            ],
            "countryCoverageSummary": [
                {
                    "country": "DE",
                    "oldMonths": ["2026 Jan"],
                    "newMonths": ["2026 Jan", "2026 Feb", "2026 Mar"],
                    "addedMonths": ["2026 Feb", "2026 Mar"],
                    "removedMonths": [],
                    "overlappingMonths": ["2026 Jan"],
                    "coverageStatus": "expanded",
                }
            ],
            "countrySalesReferenceLabel": "网站当前 active",
            "countryMonthlySalesSummary": [
                {
                    "country": "DE",
                    "rows": [
                        {
                            "month": "2026 Jan",
                            "referenceSales": 100,
                            "candidateSales": 100,
                            "deltaSales": 0,
                            "changeStatus": "unchanged",
                        },
                        {
                            "month": "2026 Feb",
                            "referenceSales": None,
                            "candidateSales": 120,
                            "deltaSales": None,
                            "changeStatus": "added",
                        },
                    ],
                }
            ],
            "countryMonthlySalesError": None,
            "timeAxisCheck": {},
            "countryScopeSummary": {},
            "refreshSummary": {"jobStatus": "success"},
        },
    )

    client = TestClient(app)
    response = client.get(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/review",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] == "jato-update-1234abcd"
    assert payload["decisionSuggestion"] == "manual_review_required"
    assert payload["countryFreshnessSummary"][0]["newLatestMonth"] == "2026 Mar"
    assert payload["countryCoverageSummary"][0]["addedMonths"] == ["2026 Feb", "2026 Mar"]
    assert payload["countryMonthlySalesSummary"][0]["rows"][1]["candidateSales"] == 120


def test_publish_monthly_update_job_route_returns_published_job(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "publish_jato_monthly_update_job",
        lambda *, job_id, triggered_by: {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "triggeredBy": "builder",
            "publication": {
                "publishedAt": "2026-04-20T16:00:00+00:00",
                "publishedBy": triggered_by,
                "backupDir": "04_Processed_data/.refresh_backups/manual-promote-test",
            },
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/publish",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] == "jato-update-1234abcd"
    assert payload["publication"]["publishedBy"] == "tester"


def test_rollback_monthly_update_job_route_returns_rolled_back_job(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "rollback_jato_monthly_update_job",
        lambda *, job_id, triggered_by: {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "publication": {
                "publishedAt": "2026-04-20T16:00:00+00:00",
                "publishedBy": "tester",
                "backupDir": "04_Processed_data/.refresh_backups/manual-promote-test",
                "rolledBackAt": "2026-04-20T16:10:00+00:00",
                "rolledBackBy": triggered_by,
            },
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/rollback",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] == "jato-update-1234abcd"
    assert payload["publication"]["rolledBackBy"] == "tester"


def test_review_approval_and_worker_status_routes_use_service_guards(monkeypatch) -> None:
    observed: dict[str, object] = {}
    monkeypatch.setattr(
        msrp_monthly_update,
        "approve_jato_monthly_update_review",
        lambda *, job_id, triggered_by, decision, note: observed.update({
            "jobId": job_id,
            "triggeredBy": triggered_by,
            "decision": decision,
            "note": note,
        }) or {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "reviewApproval": {"decision": "approved", "reviewedBy": triggered_by},
        },
    )
    monkeypatch.setattr(
        msrp_monthly_update,
        "get_jato_monthly_update_worker_status_service",
        lambda: {
            "state": "idle",
            "healthy": True,
            "queuedJobCount": 0,
            "queuedJobIds": [],
        },
    )

    client = TestClient(app)
    approval_response = client.post(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/review-approval",
        headers=_headers(),
        json={"decision": "approve", "note": "checked HU May"},
    )
    worker_response = client.get("/v1/msrp/monthly-update-worker/status", headers=_headers())

    assert approval_response.status_code == 200
    assert observed == {
        "jobId": "jato-update-1234abcd",
        "triggeredBy": "tester",
        "decision": "approve",
        "note": "checked HU May",
    }
    assert worker_response.status_code == 200
    assert worker_response.json()["item"]["healthy"] is True
