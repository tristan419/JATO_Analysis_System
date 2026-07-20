from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.core import security
from app.main import app
from app.services import jato_monthly_update_service


def _configure_upload_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service,
        "MONTHLY_UPDATE_JOB_ROOT",
        job_root,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "UPLOAD_CHUNK_SIZE_BYTES",
        4,
    )
    return job_root


def _headers(
    monkeypatch: pytest.MonkeyPatch,
    *,
    name: str,
    role: str,
) -> dict[str, str]:
    token = f"jato-upload-{name}-{role}"
    monkeypatch.setitem(security.TOKEN_ROLE_MAP, token, role)
    return {
        "X-Auth-Token": token,
        "X-User-Name": name,
    }


def test_initiate_reuses_only_owner_resume_and_allows_one_active_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)

    first = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="alice-resume",
        triggered_by="alice",
    )
    resumed = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="alice-resume",
        triggered_by="alice",
    )
    assert resumed["uploadId"] == first["uploadId"]

    with pytest.raises(HTTPException) as same_owner_conflict:
        jato_monthly_update_service.initiate_jato_monthly_update_upload(
            filename="JATO-other.xlsx",
            size_bytes=4,
            resume_key="alice-other",
            triggered_by="alice",
        )
    assert same_owner_conflict.value.status_code == 409

    with pytest.raises(HTTPException) as other_owner_conflict:
        jato_monthly_update_service.initiate_jato_monthly_update_upload(
            filename="JATO-2026.06.xlsx",
            size_bytes=4,
            resume_key="alice-resume",
            triggered_by="bob",
        )
    assert other_owner_conflict.value.status_code == 409

    jato_monthly_update_service.abandon_jato_monthly_update_upload(
        upload_id=first["uploadId"],
        triggered_by="alice",
        triggered_role="editor",
    )
    second = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="bob-resume",
        triggered_by="bob",
    )
    assert second["uploadId"] != first["uploadId"]


def test_editor_cannot_read_write_end_or_consume_another_owner_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    initiated = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="alice-resume",
        triggered_by="alice",
    )
    upload_id = initiated["uploadId"]

    denied_calls = (
        lambda: jato_monthly_update_service.get_jato_monthly_update_upload(
            upload_id,
            requested_by="bob",
            requested_role="editor",
        ),
        lambda: jato_monthly_update_service.get_jato_monthly_update_expected_chunk_size(
            upload_id=upload_id,
            part_number=1,
            requested_by="bob",
            requested_role="editor",
        ),
        lambda: jato_monthly_update_service.upload_jato_monthly_update_chunk(
            upload_id=upload_id,
            part_number=1,
            content=b"data",
            chunk_sha256=hashlib.sha256(b"data").hexdigest(),
            requested_by="bob",
            requested_role="editor",
        ),
        lambda: jato_monthly_update_service.complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by="bob",
            requested_role="editor",
        ),
        lambda: jato_monthly_update_service.abandon_jato_monthly_update_upload(
            upload_id=upload_id,
            triggered_by="bob",
            triggered_role="editor",
        ),
        lambda: jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=upload_id,
            triggered_by="bob",
            triggered_role="editor",
        ),
    )
    for denied_call in denied_calls:
        with pytest.raises(HTTPException) as denied:
            denied_call()
        assert denied.value.status_code == 403

    uploaded = jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=1,
        content=b"data",
        chunk_sha256=hashlib.sha256(b"data").hexdigest(),
        requested_by="ops-admin",
        requested_role="admin",
    )
    assert uploaded["receivedChunks"] == [1]
    abandoned = jato_monthly_update_service.abandon_jato_monthly_update_upload(
        upload_id=upload_id,
        triggered_by="ops-admin",
        triggered_role="admin",
    )
    assert abandoned["status"] == "abandoned"
    assert abandoned["failureDigest"]["code"] == "UPLOAD_SESSION_ABANDONED"


def test_upload_declared_size_is_positive_integral_and_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    monkeypatch.setattr(jato_monthly_update_service, "UPLOAD_MAX_BYTES", 8)

    for invalid_size in (None, 0, -1, True, 1.5, "4.0"):
        with pytest.raises(HTTPException) as invalid:
            jato_monthly_update_service.initiate_jato_monthly_update_upload(
                filename="JATO-2026.06.xlsx",
                size_bytes=invalid_size,
                resume_key=f"invalid-{invalid_size}",
                triggered_by="alice",
            )
        assert invalid.value.status_code == 400

    with pytest.raises(HTTPException) as oversized:
        jato_monthly_update_service.initiate_jato_monthly_update_upload(
            filename="JATO-2026.06.xlsx",
            size_bytes=9,
            resume_key="oversized",
            triggered_by="alice",
        )
    assert oversized.value.status_code == 413

    accepted = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=8,
        resume_key="accepted",
        triggered_by="alice",
    )
    assert accepted["sizeBytes"] == 8


def test_complete_rejects_chunk_changed_after_verified_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    initiated = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="tampered",
        triggered_by="alice",
    )
    upload_id = initiated["uploadId"]
    jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=1,
        content=b"data",
        chunk_sha256=hashlib.sha256(b"data").hexdigest(),
        requested_by="alice",
        requested_role="editor",
    )
    chunk_path = (
        jato_monthly_update_service._upload_session_chunk_dir(upload_id)
        / jato_monthly_update_service._chunk_file_name(1)
    )
    chunk_path.write_bytes(b"extra")

    with pytest.raises(HTTPException) as changed:
        jato_monthly_update_service.complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by="alice",
            requested_role="editor",
        )
    assert changed.value.status_code == 409
    assert "大小已变化" in str(changed.value.detail)


def test_upload_routes_enforce_owner_and_explicit_admin_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    client = TestClient(app)
    alice_headers = _headers(
        monkeypatch,
        name="alice",
        role="editor",
    )
    bob_headers = _headers(
        monkeypatch,
        name="bob",
        role="editor",
    )
    admin_headers = _headers(
        monkeypatch,
        name="ops-admin",
        role="admin",
    )
    initiated = client.post(
        "/v1/msrp/monthly-update-uploads/initiate",
        headers=alice_headers,
        json={
            "filename": "JATO-2026.06.xlsx",
            "sizeBytes": 4,
            "resumeKey": "alice-route-resume",
        },
    )
    assert initiated.status_code == 200
    upload_id = initiated.json()["item"]["uploadId"]

    denied_responses = (
        client.get(
            f"/v1/msrp/monthly-update-uploads/{upload_id}",
            headers=bob_headers,
        ),
        client.put(
            f"/v1/msrp/monthly-update-uploads/{upload_id}/parts/1",
            headers={
                **bob_headers,
                "Content-Type": "application/octet-stream",
                "X-Chunk-SHA256": hashlib.sha256(b"data").hexdigest(),
            },
            content=b"data",
        ),
        client.post(
            f"/v1/msrp/monthly-update-uploads/{upload_id}/complete",
            headers=bob_headers,
        ),
        client.post(
            f"/v1/msrp/monthly-update-uploads/{upload_id}/abandon",
            headers=bob_headers,
        ),
        client.post(
            "/v1/msrp/monthly-update-jobs/from-upload",
            headers=bob_headers,
            json={"uploadId": upload_id},
        ),
    )
    assert [response.status_code for response in denied_responses] == [
        403,
        403,
        403,
        403,
        403,
    ]

    admin_get = client.get(
        f"/v1/msrp/monthly-update-uploads/{upload_id}",
        headers=admin_headers,
    )
    assert admin_get.status_code == 200
    admin_abandon = client.post(
        f"/v1/msrp/monthly-update-uploads/{upload_id}/abandon",
        headers=admin_headers,
    )
    assert admin_abandon.status_code == 200
    assert admin_abandon.json()["item"]["status"] == "abandoned"
