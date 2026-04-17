import hashlib

from fastapi.testclient import TestClient

from app.api.routes import msrp_monthly_update
from app.main import app
from app.services import jato_monthly_update_service


def _headers() -> dict[str, str]:
    return {
        "X-Auth-Token": "change-me",
        "X-User-Name": "tester",
    }


def test_create_monthly_update_job_route_persists_job(
    tmp_path, monkeypatch
) -> None:
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(
        jato_monthly_update_service, "_launch_job_thread", lambda job_id: None
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-jobs",
        headers=_headers(),
        data={"month": "2026-03"},
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
    assert payload["month"] == "2026-03"
    assert payload["status"] == "queued"
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
        lambda *, triggered_by: {
            "cleanedAt": "2026-04-16T00:00:00+00:00",
            "triggeredBy": triggered_by,
            "activeBaselinePath": "01_RAW_DATA/baseline/JATO-2026.3-full-baseline.xlsx",
            "activePatchMonth": "2026-03",
            "archivedBaselineCount": 1,
            "archivedBaselines": [
                "01_RAW_DATA/historyDataArchive/baseline/JATO-2026.2-full-baseline.xlsx"
            ],
            "archivedPatchDirCount": 1,
            "archivedPatchDirs": ["01_RAW_DATA/historyDataArchive/patches/2026-02"],
            "removedJobUploadDirCount": 2,
            "removedJobUploadDirs": [
                "04_Processed_data/ops/jato_monthly_update_jobs/job-a/uploads",
                "04_Processed_data/ops/jato_monthly_update_jobs/job-b/uploads",
            ],
        },
    )

    client = TestClient(app)
    response = client.post(
        "/v1/msrp/monthly-update-maintenance/cleanup",
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["triggeredBy"] == "tester"
    assert payload["archivedBaselineCount"] == 1
    assert payload["removedJobUploadDirCount"] == 2


def test_chunked_monthly_update_upload_routes_create_job(
    tmp_path, monkeypatch
) -> None:
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(jato_monthly_update_service, "UPLOAD_CHUNK_SIZE_BYTES", 4)
    monkeypatch.setattr(
        jato_monthly_update_service, "_launch_job_thread", lambda job_id: None
    )

    client = TestClient(app)
    initiate_response = client.post(
        "/v1/msrp/monthly-update-uploads/initiate",
        headers=_headers(),
        json={"filename": "patch.xlsx", "sizeBytes": 10, "resumeKey": "resume-key-1"},
    )
    assert initiate_response.status_code == 200
    upload_id = initiate_response.json()["item"]["uploadId"]
    assert initiate_response.json()["item"]["totalChunks"] == 3
    reinitiate_response = client.post(
        "/v1/msrp/monthly-update-uploads/initiate",
        headers=_headers(),
        json={"filename": "patch.xlsx", "sizeBytes": 10, "resumeKey": "resume-key-1"},
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
        json={"month": "2026-03", "uploadId": upload_id},
    )
    assert create_response.status_code == 200
    payload = create_response.json()["item"]
    assert payload["status"] == "queued"
    assert payload["upload"]["sizeBytes"] == 10
    assert payload["upload"]["sha256"] == hashlib.sha256(b"abcdefghij").hexdigest()
