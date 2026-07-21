from __future__ import annotations

import hashlib
import threading
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


def _persist_retryable_digest_session(
    upload_id: str,
    *,
    technical_detail: dict[str, object] | None = None,
) -> Path:
    content = b"verified-assembly"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=len(content),
        resume_key=f"resume-{upload_id}",
        triggered_by="alice",
    )
    assembled_path = jato_monthly_update_service._upload_session_assembled_path(
        upload_id,
        state["filename"],
    )
    assembled_path.parent.mkdir(parents=True, exist_ok=True)
    assembled_path.write_bytes(content)
    state["status"] = "invalid"
    state["assembledPath"] = str(assembled_path)
    state["fileSha256"] = hashlib.sha256(content).hexdigest()
    state["failureDigest"] = {
        "code": "DIGEST_TIMEOUT",
        "retryable": True,
        "technicalDetail": technical_detail,
    }
    jato_monthly_update_service._persist_upload_session(state)
    return assembled_path


def _persist_ready_upload_session(
    upload_id: str,
    *,
    active_version: str = "active-race-version",
) -> None:
    content = b"ready-upload-race"
    file_sha256 = hashlib.sha256(content).hexdigest()
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=len(content),
        resume_key=f"resume-{upload_id}",
        triggered_by="alice",
    )
    assembled_path = jato_monthly_update_service._upload_session_assembled_path(
        upload_id,
        state["filename"],
    )
    assembled_path.parent.mkdir(parents=True, exist_ok=True)
    assembled_path.write_bytes(content)
    state["status"] = "ready"
    state["assembledPath"] = str(assembled_path)
    state["fileSha256"] = file_sha256
    state["uploadedBytes"] = len(content)
    state["ingestDigest"] = {
        "status": "ready",
        "route": "full_batch",
        "fileSha256": file_sha256,
        "sizeBytes": len(content),
        "latestMonth": "2026-06",
        "activeDatasetVersion": active_version,
        "countries": ["匈牙利"],
        "countryLatestMonths": {"匈牙利": "2026-06"},
        "blockers": [],
    }
    jato_monthly_update_service._persist_upload_session(state)


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
        lambda: jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
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


@pytest.mark.parametrize(
    "entrypoint",
    ["direct_batch", "single_country", "failed_retry"],
)
def test_legacy_job_entrypoints_share_active_upload_resource_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entrypoint: str,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    active_upload_id = "jato-upload-resource-quarantine"
    active_state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=active_upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="resource-quarantine",
        triggered_by="alice",
    )
    active_state["status"] = "digesting"
    active_state["failureDigest"] = {
        "code": "RESOURCE_QUARANTINED",
        "retryable": False,
    }
    jato_monthly_update_service._persist_upload_session(active_state)

    reached_implementation: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_create_jato_monthly_update_job_in_start_window",
        lambda **_kwargs: reached_implementation.append("direct_batch"),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_create_single_country_job_in_start_window",
        lambda **_kwargs: reached_implementation.append("single_country"),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_retry_failed_jato_monthly_update_job_in_start_window",
        lambda **_kwargs: reached_implementation.append("failed_retry"),
    )

    with pytest.raises(HTTPException) as rejected:
        if entrypoint == "direct_batch":
            jato_monthly_update_service.create_jato_monthly_update_job(
                file=object(),  # type: ignore[arg-type]
                triggered_by="alice",
            )
        elif entrypoint == "single_country":
            jato_monthly_update_service.create_single_country_job(
                country="匈牙利",
                month="2026-06",
                file=object(),  # type: ignore[arg-type]
                triggered_by="alice",
            )
        else:
            jato_monthly_update_service.retry_failed_jato_monthly_update_job(
                source_job_id="failed-job",
                triggered_by="alice",
            )

    assert rejected.value.status_code == 409
    assert active_upload_id in str(rejected.value.detail)
    assert reached_implementation == []


@pytest.mark.parametrize(
    "upload_condition",
    ["assembling", "digesting", "quarantined"],
)
@pytest.mark.parametrize(
    "entrypoint",
    ["publish", "rollback", "smart_merge", "historical", "recheck"],
)
def test_existing_job_worker_entrypoints_do_not_queue_during_active_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    upload_condition: str,
    entrypoint: str,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = f"jato-upload-{upload_condition}"
    upload_state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key=f"resource-{upload_condition}",
        triggered_by="alice",
    )
    upload_state["status"] = (
        "digesting" if upload_condition == "quarantined" else upload_condition
    )
    if upload_condition == "quarantined":
        upload_state["failureDigest"] = {
            "code": "RESOURCE_QUARANTINED",
            "retryable": False,
        }
    jato_monthly_update_service._persist_upload_session(upload_state)

    job_id = f"jato-update-gated-{entrypoint}"
    if entrypoint == "recheck":
        jato_monthly_update_service._persist_job_state(
            {
                "jobId": job_id,
                "status": "queued",
                "phase": "queued",
            }
        )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_recover_incomplete_active_transactions_if_possible",
        lambda: [],
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_reconcile_stale_monthly_update_jobs",
        lambda: [],
    )
    inner_calls: list[str] = []
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_publish_jato_monthly_update_job_with_job_lock",
        lambda **_kwargs: inner_calls.append("publish"),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_rollback_jato_monthly_update_job_with_job_lock",
        lambda **_kwargs: inner_calls.append("rollback"),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_create_smart_merge_candidate_with_job_lock",
        lambda **_kwargs: inner_calls.append("smart_merge"),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_resolve_jato_historical_reclassification_with_job_lock",
        lambda **_kwargs: inner_calls.append("historical"),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda launched_job_id: launches.append(launched_job_id),
    )

    with pytest.raises(HTTPException) as rejected:
        if entrypoint == "publish":
            jato_monthly_update_service.publish_jato_monthly_update_job(
                job_id=job_id,
                triggered_by="alice",
            )
        elif entrypoint == "rollback":
            jato_monthly_update_service.rollback_jato_monthly_update_job(
                job_id=job_id,
                triggered_by="alice",
            )
        elif entrypoint == "smart_merge":
            jato_monthly_update_service.create_smart_merge_candidate(
                job_id=job_id,
                triggered_by="alice",
            )
        elif entrypoint == "historical":
            jato_monthly_update_service.resolve_jato_historical_reclassification(
                job_id=job_id,
                triggered_by="alice",
                decisions=[],
            )
        else:
            jato_monthly_update_service.recheck_jato_monthly_update_job(
                job_id=job_id,
                triggered_by="alice",
            )

    assert rejected.value.status_code == 409
    assert upload_id in str(rejected.value.detail)
    assert inner_calls == []
    assert launches == []
    if entrypoint == "recheck":
        persisted = jato_monthly_update_service._load_job_state(job_id)
        assert persisted["status"] == "queued"


def test_from_upload_excludes_only_own_ready_session_from_resource_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    own_upload_id = "jato-upload-own-ready"
    own_state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=own_upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="own-ready",
        triggered_by="alice",
    )
    own_state["status"] = "ready"
    jato_monthly_update_service._persist_upload_session(own_state)
    calls: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_create_jato_monthly_update_job_from_upload_in_start_window",
        lambda **_kwargs: calls.append(own_upload_id) or {"status": "queued"},
    )

    accepted = jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
        upload_id=own_upload_id,
        triggered_by="alice",
        triggered_role="editor",
    )

    assert accepted == {"status": "queued"}
    assert calls == [own_upload_id]

    other_upload_id = "jato-upload-other-digesting"
    other_state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=other_upload_id,
        filename="JATO-other.xlsx",
        size_bytes=4,
        resume_key="other-digesting",
        triggered_by="bob",
    )
    other_state["status"] = "digesting"
    jato_monthly_update_service._persist_upload_session(other_state)

    with pytest.raises(HTTPException) as rejected:
        jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=own_upload_id,
            triggered_by="alice",
            triggered_role="editor",
        )

    assert rejected.value.status_code == 409
    assert other_upload_id in str(rejected.value.detail)
    assert calls == [own_upload_id]

    other_state["status"] = "invalid"
    jato_monthly_update_service._persist_upload_session(other_state)
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": "jato-update-other-running",
            "status": "running",
        }
    )
    with pytest.raises(HTTPException) as running_rejected:
        jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=own_upload_id,
            triggered_by="alice",
            triggered_role="editor",
        )

    assert running_rejected.value.status_code == 409
    assert "运行中的月更任务" in str(running_rejected.value.detail)
    assert calls == [own_upload_id]


def test_create_from_ready_holds_state_lock_until_consumed_before_abandon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-create-wins-race"
    _persist_ready_upload_session(upload_id)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-race-version",
    )
    queue_entered = threading.Barrier(2)
    queue_release = threading.Barrier(2)
    queue_calls: list[str] = []

    def queue_job(**kwargs):
        queue_calls.append(str(kwargs["job_id"]))
        queue_entered.wait(timeout=3)
        queue_release.wait(timeout=3)
        return {"jobId": str(kwargs["job_id"]), "status": "queued"}

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_queue_monthly_update_job_from_stored_upload",
        queue_job,
    )
    create_results: list[dict[str, object]] = []
    create_errors: list[BaseException] = []
    abandon_errors: list[BaseException] = []
    abandon_started = threading.Event()
    abandon_done = threading.Event()

    def run_create() -> None:
        try:
            create_results.append(
                jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
                    upload_id=upload_id,
                    triggered_by="alice",
                    triggered_role="editor",
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            create_errors.append(exc)

    def run_abandon() -> None:
        abandon_started.set()
        try:
            jato_monthly_update_service.abandon_jato_monthly_update_upload(
                upload_id=upload_id,
                triggered_by="alice",
                triggered_role="editor",
            )
        except BaseException as exc:
            abandon_errors.append(exc)
        finally:
            abandon_done.set()

    creator = threading.Thread(target=run_create)
    abandoner = threading.Thread(target=run_abandon)
    creator.start()
    queue_entered.wait(timeout=3)
    abandoner.start()
    assert abandon_started.wait(timeout=3)
    assert not abandon_done.wait(timeout=0.1)
    queue_release.wait(timeout=3)
    creator.join(timeout=3)
    abandoner.join(timeout=3)

    assert not creator.is_alive()
    assert not abandoner.is_alive()
    assert create_errors == []
    assert create_results[0]["status"] == "queued"
    assert len(queue_calls) == 1
    assert len(abandon_errors) == 1
    assert isinstance(abandon_errors[0], HTTPException)
    assert abandon_errors[0].status_code == 409
    persisted = jato_monthly_update_service._load_upload_session(upload_id)
    assert persisted["status"] == "consumed"
    assert persisted["consumedJobId"] == create_results[0]["jobId"]


def test_abandon_from_ready_prevents_concurrent_create_from_queueing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-abandon-wins-race"
    _persist_ready_upload_session(upload_id)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-race-version",
    )
    abandon_commit_entered = threading.Barrier(2)
    abandon_commit_release = threading.Barrier(2)
    original_persist = jato_monthly_update_service._persist_upload_session

    def pause_abandoned_commit(payload: dict[str, object]) -> None:
        if (
            str(payload.get("uploadId") or "") == upload_id
            and str(payload.get("status") or "") == "abandoned"
        ):
            abandon_commit_entered.wait(timeout=3)
            abandon_commit_release.wait(timeout=3)
        original_persist(payload)

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_persist_upload_session",
        pause_abandoned_commit,
    )
    queue_calls: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_queue_monthly_update_job_from_stored_upload",
        lambda **kwargs: queue_calls.append(str(kwargs["job_id"])),
    )
    abandon_results: list[dict[str, object]] = []
    abandon_errors: list[BaseException] = []
    create_errors: list[BaseException] = []
    create_started = threading.Event()
    create_done = threading.Event()

    def run_abandon() -> None:
        try:
            abandon_results.append(
                jato_monthly_update_service.abandon_jato_monthly_update_upload(
                    upload_id=upload_id,
                    triggered_by="alice",
                    triggered_role="editor",
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            abandon_errors.append(exc)

    def run_create() -> None:
        create_started.set()
        try:
            jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
                upload_id=upload_id,
                triggered_by="alice",
                triggered_role="editor",
            )
        except BaseException as exc:
            create_errors.append(exc)
        finally:
            create_done.set()

    abandoner = threading.Thread(target=run_abandon)
    creator = threading.Thread(target=run_create)
    abandoner.start()
    abandon_commit_entered.wait(timeout=3)
    creator.start()
    assert create_started.wait(timeout=3)
    assert not create_done.wait(timeout=0.1)
    abandon_commit_release.wait(timeout=3)
    abandoner.join(timeout=3)
    creator.join(timeout=3)

    assert not abandoner.is_alive()
    assert not creator.is_alive()
    assert abandon_errors == []
    assert abandon_results[0]["status"] == "abandoned"
    assert len(create_errors) == 1
    assert isinstance(create_errors[0], HTTPException)
    assert create_errors[0].status_code == 409
    assert queue_calls == []
    persisted = jato_monthly_update_service._load_upload_session(upload_id)
    assert persisted["status"] == "abandoned"
    assert persisted["consumedJobId"] is None


def test_long_cjk_display_filename_uses_bounded_internal_assembly_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    filename = (
        "jato【已规整】202606 16国：比利时、波兰、丹麦、德国、法国、荷兰、"
        "捷克、克罗地亚、罗马尼亚、挪威、葡萄牙、瑞典、瑞士、西班牙、希腊、"
        "意大利.xlsx"
    )
    legacy_temp_name = f".{filename}.1497283.{'a' * 32}.assembling"
    assert len(legacy_temp_name.encode("utf-8")) > 255

    initiated = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename=filename,
        size_bytes=8,
        resume_key="long-cjk-filename",
        triggered_by="alice",
    )
    upload_id = initiated["uploadId"]
    assert initiated["filename"] == filename
    for part_number, content in ((1, b"data"), (2, b"more")):
        jato_monthly_update_service.upload_jato_monthly_update_chunk(
            upload_id=upload_id,
            part_number=part_number,
            content=content,
            chunk_sha256=hashlib.sha256(content).hexdigest(),
            requested_by="alice",
            requested_role="editor",
        )

    assembled_path, file_sha256 = (
        jato_monthly_update_service._assemble_monthly_update_upload(
            jato_monthly_update_service._load_upload_session(upload_id)
        )
    )

    assert assembled_path.read_bytes() == b"datamore"
    assert file_sha256 == hashlib.sha256(b"datamore").hexdigest()
    assert assembled_path.parent == (
        jato_monthly_update_service._upload_session_dir(upload_id)
        / "assembled"
    )
    assert assembled_path.suffix == ".xlsx"
    assert assembled_path.name.isascii()
    assert len(assembled_path.name.encode("utf-8")) < 100
    assert filename not in str(assembled_path)
    assert not list(assembled_path.parent.glob(".assemble-*.tmp"))


def test_upload_filename_sanitizes_both_path_separator_styles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)

    normalized = jato_monthly_update_service._validate_upload_filename(
        "../../outside\\nested\\报告.xlsx"
    )
    assembled_path = jato_monthly_update_service._upload_session_assembled_path(
        "jato-upload-safe",
        normalized,
    )

    assert normalized == "报告.xlsx"
    assert assembled_path.parent == (
        jato_monthly_update_service._upload_session_dir("jato-upload-safe")
        / "assembled"
    )
    assert assembled_path.name.isascii()
    assert len(assembled_path.name.encode("utf-8")) < 100


def test_job_metadata_keeps_display_filename_with_safe_storage_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    filename = "月度数据" * 100 + ".xlsx"
    job_id = "jato-update-safe-name"
    stored_path = jato_monthly_update_service._job_upload_storage_path(
        job_id,
        filename,
    )
    stored_path.parent.mkdir(parents=True, exist_ok=True)
    stored_path.write_bytes(b"workbook")
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda _job_id: None,
    )

    queued = jato_monthly_update_service._queue_monthly_update_job_from_stored_upload(
        job_id=job_id,
        triggered_by="alice",
        upload_filename=filename,
        stored_upload_path=stored_path,
        month="2026-06",
    )

    assert queued["upload"]["originalFilename"] == filename
    assert Path(queued["upload"]["storedPath"]).name == stored_path.name
    assert stored_path.name.isascii()
    assert len(stored_path.name.encode("utf-8")) < 100


def test_retry_digest_reuses_verified_assembly_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-retry-digest"
    content = b"verified-assembly"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=len(content),
        resume_key="retry-digest",
        triggered_by="alice",
    )
    assembled_path = jato_monthly_update_service._upload_session_assembled_path(
        upload_id,
        state["filename"],
    )
    assembled_path.parent.mkdir(parents=True, exist_ok=True)
    assembled_path.write_bytes(content)
    state["status"] = "invalid"
    state["assembledPath"] = str(assembled_path)
    state["fileSha256"] = hashlib.sha256(content).hexdigest()
    state["failureDigest"] = {
        "code": "DIGEST_TIMEOUT",
        "retryable": True,
    }
    jato_monthly_update_service._persist_upload_session(state)
    chunk_dir = jato_monthly_update_service._upload_session_chunk_dir(upload_id)
    chunk_dir.mkdir(parents=True)
    (chunk_dir / "part-000001.chunk").write_bytes(b"old-chunk-evidence")
    launched: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        lambda current_upload_id: launched.append(current_upload_id) or 24680,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_read_process_identity",
        lambda _pid: {"startTimeTicks": "123", "cmdlineSha256": "a" * 64},
    )

    restarted = jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
        upload_id=upload_id,
        requested_by="alice",
        requested_role="editor",
    )
    replayed = jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
        upload_id=upload_id,
        requested_by="alice",
        requested_role="editor",
    )

    assert launched == [upload_id]
    assert restarted["status"] == "assembling"
    assert replayed["status"] == "assembling"
    assert restarted["digestAttempts"] == 1
    assert restarted["failureDigest"] is None

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_assemble_monthly_update_upload",
        lambda _state: (_ for _ in ()).throw(
            AssertionError("digest-only retry must not reassemble chunks")
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_build_upload_ingest_digest",
        lambda **_kwargs: {"status": "ready"},
    )
    digested = jato_monthly_update_service.run_jato_monthly_update_upload_digest(
        upload_id
    )

    assert digested["status"] == "ready"
    assert assembled_path.read_bytes() == content
    assert not chunk_dir.exists()


def test_retry_digest_rejects_input_validation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-invalid-source"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="invalid-source",
        triggered_by="alice",
    )
    state["status"] = "invalid"
    state["failureDigest"] = {
        "code": "UPLOAD_DIGEST_FAILED",
        "retryable": False,
    }
    jato_monthly_update_service._persist_upload_session(state)

    with pytest.raises(HTTPException) as rejected:
        jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
            upload_id=upload_id,
            requested_by="alice",
            requested_role="editor",
        )

    assert rejected.value.status_code == 409
    assert rejected.value.detail["code"] == "DIGEST_RETRY_NOT_ALLOWED"


@pytest.mark.parametrize("resource_kind", ["active_upload", "running_job", "baseline"])
def test_retry_digest_rejects_competing_global_resource_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resource_kind: str,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-resource-guard"
    _persist_retryable_digest_session(upload_id)
    if resource_kind == "active_upload":
        other = jato_monthly_update_service._prepare_upload_session_state(
            upload_id="jato-upload-other-active",
            filename="other.xlsx",
            size_bytes=4,
            resume_key="other-active",
            triggered_by="bob",
        )
        other["status"] = "digesting"
        jato_monthly_update_service._persist_upload_session(other)
    elif resource_kind == "running_job":
        jato_monthly_update_service._persist_job_state(
            {"jobId": "jato-update-running", "status": "running"}
        )
    else:
        jato_monthly_update_service._write_json(
            jato_monthly_update_service._baseline_promotion_state_path(),
            {"status": "running"},
        )
    launched: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        lambda current_upload_id: launched.append(current_upload_id) or 24680,
    )

    with pytest.raises(HTTPException) as rejected:
        jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
            upload_id=upload_id,
            requested_by="alice",
            requested_role="editor",
        )

    assert rejected.value.status_code == 409
    assert launched == []
    persisted = jato_monthly_update_service._load_upload_session(upload_id)
    assert persisted["status"] == "invalid"
    assert persisted["digestAttempts"] == 0


def test_retry_digest_rejects_while_maintenance_coordination_lock_is_held(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-maintenance-guard"
    _persist_retryable_digest_session(upload_id)
    lock_ready = threading.Event()
    release_lock = threading.Event()

    def hold_maintenance_lock() -> None:
        with jato_monthly_update_service._exclusive_file_lock(
            jato_monthly_update_service._maintenance_coordination_lock_path()
        ) as acquired:
            assert acquired
            lock_ready.set()
            assert release_lock.wait(timeout=3)

    holder = threading.Thread(target=hold_maintenance_lock)
    holder.start()
    assert lock_ready.wait(timeout=3)
    try:
        with pytest.raises(HTTPException) as rejected:
            jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
                upload_id=upload_id,
                requested_by="alice",
                requested_role="editor",
            )
        assert rejected.value.status_code == 409
    finally:
        release_lock.set()
        holder.join(timeout=3)
    assert not holder.is_alive()


def test_retry_digest_rejects_when_old_worker_is_still_alive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-old-worker-alive"
    _persist_retryable_digest_session(
        upload_id,
        technical_detail={
            "digestPid": 97531,
            "digestProcessIdentity": {
                "startTimeTicks": "123",
                "cmdlineSha256": "a" * 64,
            },
        },
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda pid: pid == 97531,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_identity_matches",
        lambda **_kwargs: (
            True,
            {"startTimeTicks": "123", "cmdlineSha256": "a" * 64},
        ),
    )
    launched: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        lambda current_upload_id: launched.append(current_upload_id) or 24680,
    )

    with pytest.raises(HTTPException) as rejected:
        jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
            upload_id=upload_id,
            requested_by="alice",
            requested_role="editor",
        )

    assert rejected.value.status_code == 409
    assert rejected.value.detail["code"] == "DIGEST_RETRY_WORKER_STILL_RUNNING"
    assert rejected.value.detail["digestPid"] == 97531
    assert launched == []


@pytest.mark.parametrize("identity_state", ["missing", "unreadable"])
def test_retry_digest_blocks_when_live_old_pid_identity_cannot_be_confirmed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    identity_state: str,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = f"jato-upload-old-worker-{identity_state}"
    expected_identity = {
        "startTimeTicks": "123",
        "cmdlineSha256": "a" * 64,
    }
    technical_detail: dict[str, object] = {
        "digestPid": 97531,
        "termination": {
            "processAliveAfter": True,
            "error": "SIGKILL failed",
        },
    }
    if identity_state == "unreadable":
        technical_detail["digestProcessIdentity"] = expected_identity
    _persist_retryable_digest_session(
        upload_id,
        technical_detail=technical_detail,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda pid: pid == 97531,
    )
    identity_checks: list[int] = []

    def unreadable_identity(**kwargs):
        identity_checks.append(int(kwargs["pid"]))
        return False, None

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_identity_matches",
        unreadable_identity,
    )

    with pytest.raises(HTTPException) as rejected:
        jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
            upload_id=upload_id,
            requested_by="alice",
            requested_role="editor",
        )

    assert rejected.value.status_code == 409
    assert rejected.value.detail["code"] == "DIGEST_RETRY_WORKER_IDENTITY_UNKNOWN"
    assert identity_checks == ([] if identity_state == "missing" else [97531])


def test_retry_digest_allows_confirmed_pid_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-confirmed-pid-reuse"
    _persist_retryable_digest_session(
        upload_id,
        technical_detail={
            "digestPid": 97531,
            "digestProcessIdentity": {
                "startTimeTicks": "old-start",
                "cmdlineSha256": "a" * 64,
            },
        },
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda pid: pid == 97531,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_identity_matches",
        lambda **_kwargs: (
            False,
            {"startTimeTicks": "new-start", "cmdlineSha256": "b" * 64},
        ),
    )
    launched: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        lambda current_upload_id: launched.append(current_upload_id) or 24680,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_read_process_identity",
        lambda _pid: {"startTimeTicks": "456", "cmdlineSha256": "c" * 64},
    )

    restarted = jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
        upload_id=upload_id,
        requested_by="alice",
        requested_role="editor",
    )

    assert restarted["status"] == "assembling"
    assert restarted["digestPid"] == 24680
    assert launched == [upload_id]


def test_concurrent_retry_digest_requests_never_launch_two_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-concurrent-retry"
    _persist_retryable_digest_session(upload_id)
    launch_started = threading.Event()
    release_launch = threading.Event()
    launched: list[str] = []

    def launch_once(current_upload_id: str) -> int:
        launched.append(current_upload_id)
        launch_started.set()
        assert release_launch.wait(timeout=3)
        return 24680

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        launch_once,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_read_process_identity",
        lambda _pid: {"startTimeTicks": "123", "cmdlineSha256": "a" * 64},
    )
    first_result: list[dict[str, object]] = []

    def first_retry() -> None:
        first_result.append(
            jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
                upload_id=upload_id,
                requested_by="alice",
                requested_role="editor",
            )
        )

    first = threading.Thread(target=first_retry)
    first.start()
    assert launch_started.wait(timeout=3)
    try:
        with pytest.raises(HTTPException) as rejected:
            jato_monthly_update_service.retry_jato_monthly_update_upload_digest(
                upload_id=upload_id,
                requested_by="alice",
                requested_role="editor",
            )
        assert rejected.value.status_code == 409
    finally:
        release_launch.set()
        first.join(timeout=3)

    assert not first.is_alive()
    assert launched == [upload_id]
    assert first_result[0]["status"] == "assembling"


def test_abandon_keeps_live_unconfirmed_digest_in_active_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-abandon-quarantine"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="abandon-quarantine",
        triggered_by="alice",
    )
    state["status"] = "digesting"
    state["digestPid"] = 86420
    state["digestProcessIdentity"] = {
        "startTimeTicks": "123",
        "cmdlineSha256": "a" * 64,
    }
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda pid: pid == 86420,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_terminate_process_group",
        lambda pid, **_kwargs: {
            "pid": pid,
            "processAliveBefore": True,
            "processAliveAfter": True,
            "identityVerified": False,
            "error": "process identity could not be verified",
        },
    )

    with pytest.raises(HTTPException) as rejected:
        jato_monthly_update_service.abandon_jato_monthly_update_upload(
            upload_id=upload_id,
            triggered_by="alice",
            triggered_role="editor",
        )

    assert rejected.value.status_code == 409
    assert rejected.value.detail["code"] == "RESOURCE_QUARANTINED"
    persisted = jato_monthly_update_service._load_upload_session(upload_id)
    assert persisted["status"] == "digesting"
    assert persisted["digestPid"] == 86420
    assert persisted["completedAt"] is None
    assert persisted["failureDigest"]["code"] == "RESOURCE_QUARANTINED"
    assert [
        payload["uploadId"]
        for payload in jato_monthly_update_service._active_upload_session_payloads()
    ] == [upload_id]


def test_abandon_pidless_digest_requires_free_digest_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-abandon-pidless"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="abandon-pidless",
        triggered_by="alice",
    )
    state["status"] = "assembling"
    state["digestPid"] = None
    jato_monthly_update_service._persist_upload_session(state)

    lock_ready = threading.Event()
    release_lock = threading.Event()

    def hold_digest_lock() -> None:
        with jato_monthly_update_service._exclusive_file_lock(
            jato_monthly_update_service._upload_digest_lock_path(upload_id)
        ) as acquired:
            assert acquired
            lock_ready.set()
            assert release_lock.wait(timeout=3)

    holder = threading.Thread(target=hold_digest_lock)
    holder.start()
    assert lock_ready.wait(timeout=3)
    try:
        with pytest.raises(HTTPException) as rejected:
            jato_monthly_update_service.abandon_jato_monthly_update_upload(
                upload_id=upload_id,
                triggered_by="alice",
                triggered_role="editor",
            )
        assert rejected.value.status_code == 409
        assert rejected.value.detail["code"] == "RESOURCE_QUARANTINED"
        assert rejected.value.detail["technicalDetail"][
            "digestLockHeld"
        ] is True
        persisted = jato_monthly_update_service._load_upload_session(upload_id)
        assert persisted["status"] == "assembling"
    finally:
        release_lock.set()
        holder.join(timeout=3)
    assert not holder.is_alive()

    abandoned = jato_monthly_update_service.abandon_jato_monthly_update_upload(
        upload_id=upload_id,
        triggered_by="alice",
        triggered_role="editor",
    )

    assert abandoned["status"] == "abandoned"
    assert abandoned["failureDigest"]["code"] == "UPLOAD_SESSION_ABANDONED"
    assert abandoned["failureDigest"]["technicalDetail"][
        "digestLockHeld"
    ] is False
    assert jato_monthly_update_service._active_upload_session_payloads() == []


def test_abandon_becomes_terminal_only_after_digest_is_confirmed_dead(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "jato-upload-abandon-confirmed-dead"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="abandon-confirmed-dead",
        triggered_by="alice",
    )
    state["status"] = "digesting"
    state["digestPid"] = 86421
    state["digestProcessIdentity"] = {
        "startTimeTicks": "123",
        "cmdlineSha256": "a" * 64,
    }
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda _pid: True,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_terminate_process_group",
        lambda pid, **_kwargs: {
            "pid": pid,
            "processAliveBefore": True,
            "processAliveAfter": False,
            "identityVerified": True,
        },
    )

    abandoned = jato_monthly_update_service.abandon_jato_monthly_update_upload(
        upload_id=upload_id,
        triggered_by="alice",
        triggered_role="editor",
    )

    assert abandoned["status"] == "abandoned"
    assert abandoned["digestPid"] is None
    assert abandoned["failureDigest"]["code"] == "UPLOAD_SESSION_ABANDONED"
    assert abandoned["failureDigest"]["technicalDetail"]["termination"][
        "processAliveAfter"
    ] is False
    assert jato_monthly_update_service._active_upload_session_payloads() == []


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
            f"/v1/msrp/monthly-update-uploads/{upload_id}/retry-digest",
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
