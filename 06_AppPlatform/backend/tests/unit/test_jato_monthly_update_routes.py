import hashlib

from fastapi.testclient import TestClient

from app.api.routes import msrp_monthly_update
from app.core import security
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
        "X-Auth-Token": "change-me",
        "X-User-Name": "tester",
    }


def _admin_headers(monkeypatch) -> dict[str, str]:
    monkeypatch.setitem(
        security.TOKEN_ROLE_MAP,
        "jato-monthly-admin-test-token",
        "admin",
    )
    return {
        "X-Auth-Token": "jato-monthly-admin-test-token",
        "X-User-Name": "tester",
    }


def _ready_upload_digest(
    *,
    file_sha256: str,
    size_bytes: int,
) -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "status": "ready",
        "fileSha256": file_sha256,
        "sizeBytes": size_bytes,
        "sheetName": "Data Export",
        "route": "full_batch",
        "candidateScope": "full_candidate",
        "countries": ["瑞典"],
        "countryLatestMonths": {"瑞典": "2026-03"},
        "activeLatestMonths": {"瑞典": "2026-02"},
        "latestMonth": "2026-03",
        "dataRowCount": 1,
        "advancedCountries": ["瑞典"],
        "unchangedCountries": [],
        "regressedCountries": [],
        "activeDatasetVersion": "active-test-version",
        "blockers": [],
        "warnings": [],
    }


def test_abandon_monthly_update_upload_route(
    monkeypatch,
) -> None:
    calls: list[dict[str, str]] = []
    monkeypatch.setattr(
        msrp_monthly_update,
        "abandon_jato_monthly_update_upload",
        lambda **kwargs: (
            calls.append(kwargs)
            or {
                "uploadId": kwargs["upload_id"],
                "status": "abandoned",
            }
        ),
    )
    client = TestClient(app)

    response = client.post(
        "/v1/msrp/monthly-update-uploads/upload-123/abandon",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["item"]["status"] == "abandoned"
    assert calls == [
            {
                "upload_id": "upload-123",
                "triggered_by": "tester",
                "triggered_role": "editor",
            }
        ]


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
    assert payload["month"] is not None  # from filename parse or mock
    assert payload["batchId"] is not None
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
        headers=_admin_headers(monkeypatch),
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
        headers=_admin_headers(monkeypatch),
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
        "_launch_upload_digest_process",
        lambda _upload_id: 4242,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_build_upload_ingest_digest",
        lambda *, path, file_sha256, size_bytes: _ready_upload_digest(
            file_sha256=file_sha256,
            size_bytes=size_bytes,
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-r1",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-test-version",
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
    assert complete_response.json()["item"]["status"] == "assembling"
    monkeypatch.setenv(
        "APP_JATO_DIGEST_ATTEMPT_ID",
        jato_monthly_update_service._load_upload_session(upload_id)[
            "digestAttempt"
        ]["attemptId"],
    )

    digested = jato_monthly_update_service.run_jato_monthly_update_upload_digest(
        upload_id
    )
    assert digested["status"] == "ready"
    assert digested["fileSha256"] == hashlib.sha256(b"abcdefghij").hexdigest()
    digest_status_response = client.get(
        f"/v1/msrp/monthly-update-uploads/{upload_id}",
        headers=_headers(),
    )
    assert digest_status_response.status_code == 200
    assert digest_status_response.json()["item"]["status"] == "ready"
    assert digest_status_response.json()["item"]["ingestDigest"]["route"] == "full_batch"

    create_response = client.post(
        "/v1/msrp/monthly-update-jobs/from-upload",
        headers=_headers(),
        json={"uploadId": upload_id},
    )
    assert create_response.status_code == 200
    payload = create_response.json()["item"]
    assert payload["status"] == "queued"
    assert payload["batchId"] is not None  # from filename parse "JATO-2026.03-patch"
    assert payload["phase"] == "queued"
    assert payload["upload"]["sizeBytes"] == 10
    assert payload["upload"]["sha256"] == hashlib.sha256(b"abcdefghij").hexdigest()
    consumed_response = client.get(
        f"/v1/msrp/monthly-update-uploads/{upload_id}",
        headers=_headers(),
    )
    assert consumed_response.json()["item"]["status"] == "consumed"
    assert consumed_response.json()["item"]["consumedJobId"] == payload["jobId"]


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
    assert payload["batchId"] is not None  # from source job state month
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
        headers=_admin_headers(monkeypatch),
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


def test_historical_reclassification_resolution_route_passes_decisions(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def resolve(*, job_id, triggered_by, decisions):
        captured.update(
            {
                "jobId": job_id,
                "triggeredBy": triggered_by,
                "decisions": decisions,
            }
        )
        return {"jobId": job_id, "status": "queued"}

    monkeypatch.setattr(
        msrp_monthly_update,
        "resolve_jato_historical_reclassification",
        resolve,
    )
    client = TestClient(app)
    response = client.post(
        (
            "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/"
            "historical-reclassification-resolution"
        ),
        headers=_admin_headers(monkeypatch),
        json={
            "decisions": [
                {"country": "捷克", "decision": "use_latest"},
                {"country": "丹麦", "decision": "keep_active"},
            ]
        },
    )

    assert response.status_code == 200
    assert response.json()["item"]["status"] == "queued"
    assert captured == {
        "jobId": "jato-update-1234abcd",
        "triggeredBy": "tester",
        "decisions": [
            {"country": "捷克", "decision": "use_latest"},
            {"country": "丹麦", "decision": "keep_active"},
        ],
    }


def test_publish_monthly_update_job_route_returns_queued_operation(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monthly_update,
        "publish_jato_monthly_update_job",
        lambda *, job_id, triggered_by: {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "triggeredBy": "builder",
            "publication": None,
            "pendingOperation": {
                "operationId": "jato-publish-test",
                "type": "publish",
                "status": "queued",
                "requestedBy": triggered_by,
            },
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/publish",
        headers=_admin_headers(monkeypatch),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] == "jato-update-1234abcd"
    assert payload["publication"] is None
    assert payload["pendingOperation"]["type"] == "publish"
    assert payload["pendingOperation"]["status"] == "queued"
    assert payload["pendingOperation"]["requestedBy"] == "tester"


def test_rollback_monthly_update_job_route_returns_queued_operation(monkeypatch) -> None:
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
            },
            "pendingOperation": {
                "operationId": "jato-rollback-test",
                "type": "rollback",
                "status": "queued",
                "requestedBy": triggered_by,
            },
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs/jato-update-1234abcd/rollback",
        headers=_admin_headers(monkeypatch),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["jobId"] == "jato-update-1234abcd"
    assert "rolledBackAt" not in payload["publication"]
    assert payload["pendingOperation"]["type"] == "rollback"
    assert payload["pendingOperation"]["status"] == "queued"
    assert payload["pendingOperation"]["requestedBy"] == "tester"
