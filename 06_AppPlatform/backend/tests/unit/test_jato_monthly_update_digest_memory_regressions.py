from __future__ import annotations

import importlib.util
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import pytest
from fastapi import HTTPException
from openpyxl import Workbook

from app.services import jato_monthly_update_service


REPO_ROOT = Path(__file__).resolve().parents[4]
PRECOMPUTE_PATH = (
    REPO_ROOT / "03_Scripts" / "data_pipeline" / "precompute_summaries.py"
)
WORKER_PATH = REPO_ROOT / "03_Scripts" / "jato_monthly_worker.py"


def _load_script(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


precompute_summaries = _load_script(
    "jato_monthly_precompute_memory_regression",
    PRECOMPUTE_PATH,
)
jato_monthly_worker = _load_script(
    "jato_monthly_worker_drain_regression",
    WORKER_PATH,
)


def _configure_upload_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    project_root = tmp_path / "project"
    job_root = (
        project_root
        / "04_Processed_data"
        / "ops"
        / "jato_monthly_update_jobs"
    )
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "MONTHLY_UPDATE_JOB_ROOT",
        job_root,
    )
    return job_root


def test_full_precompute_uses_bounded_aggregator_and_metadata_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parquet_path = str(tmp_path / "candidate.parquet")
    load_calls: list[str] = []
    bounded_calls: list[tuple[str, Path]] = []
    metadata_calls: list[str] = []
    full_frame = pd.DataFrame(
        {
            "国家": ["匈牙利", "捷克"],
            "2026 Jun": [10, 20],
        }
    )

    def fake_count(path: str) -> int:
        metadata_calls.append(path)
        return 321

    def fake_load(path: str) -> pd.DataFrame:
        load_calls.append(path)
        return full_frame.copy()

    def fake_bounded(
        path: str,
        *,
        scratch_parent: Path,
    ) -> dict[str, pd.DataFrame]:
        bounded_calls.append((path, scratch_parent))
        return {
            "country": pd.DataFrame({"country": ["all"]}),
            "yearMonth": pd.DataFrame({"month": ["2026-06"]}),
            "powertrain": pd.DataFrame({"powertrain": ["all"]}),
            "segment": pd.DataFrame({"segment": ["all"]}),
            "topMakes": pd.DataFrame({"make": ["top-20"]}),
        }

    monkeypatch.setattr(precompute_summaries, "count_analysis_rows", fake_count)
    monkeypatch.setattr(precompute_summaries, "load_analysis_data", fake_load)
    monkeypatch.setattr(
        precompute_summaries,
        "compute_all_summaries_bounded",
        fake_bounded,
    )

    manifest = precompute_summaries.precompute_all_summaries(
        parquet_path,
        output_dir=str(tmp_path / "summaries"),
    )

    assert metadata_calls == [parquet_path]
    assert load_calls == []
    assert bounded_calls == [
        (parquet_path, tmp_path),
    ]
    assert manifest["originalRowCount"] == 321
    assert manifest["totalSummaryRows"] == 5


def test_review_monthly_sales_uses_bounded_parquet_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parquet_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {"Country": "Hungary", "2026 May": 1, "2026 Jun": None},
            {"Country": "Czechia", "2026 May": 100, "2026 Jun": 200},
            {"Country": " Hungary ", "2026 May": 2, "2026 Jun": 3},
            {"Country": "Denmark", "2026 May": None, "2026 Jun": None},
            {"Country": "Hungary", "2026 May": 4, "2026 Jun": 5},
        ]
    ).to_parquet(parquet_path, index=False)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "SMART_MERGE_SCAN_BATCH_ROWS",
        2,
    )
    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda *_args, **_kwargs: pytest.fail(
            "bounded Review aggregation must not call pd.read_parquet"
        ),
    )

    result = (
        jato_monthly_update_service._collect_country_monthly_sales_from_path(
            parquet_path,
            countries=["Hungary", "Denmark"],
            path_label="candidate 数据集",
        )
    )

    assert result == {
        "Hungary": {"2026 May": 7, "2026 Jun": 8},
        "Denmark": {},
    }


def test_review_monthly_sales_detects_ambiguous_country_across_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parquet_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {"Country": "Hungary", "2026 Jun": 1},
            {"Country": "Czechia", "2026 Jun": 2},
            {"Country": "hungary", "2026 Jun": 3},
        ]
    ).to_parquet(parquet_path, index=False)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "SMART_MERGE_SCAN_BATCH_ROWS",
        2,
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service._collect_country_monthly_sales_from_path(
            parquet_path,
            countries=["Hungary"],
            path_label="candidate 数据集",
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "ambiguous_logical_country"


def test_publish_sales_gates_reuse_bounded_parquet_aggregation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {"Country": "Hungary", "2026 Jan": 1_000, "2026 Feb": 2_000},
            {"Country": "Czechia", "2026 Jan": 500, "2026 Feb": 600},
        ]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(
        [
            {"Country": "Hungary", "2026 Jan": 2_000, "2026 Feb": 4_000},
            {"Country": "Czechia", "2026 Jan": 500, "2026 Feb": 600},
        ]
    ).to_parquet(candidate_path, index=False)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "SMART_MERGE_SCAN_BATCH_ROWS",
        1,
    )
    monkeypatch.setattr(
        pd,
        "read_parquet",
        lambda *_args, **_kwargs: pytest.fail(
            "publish sales gates must not call pd.read_parquet"
        ),
    )

    changes = (
        jato_monthly_update_service._find_publish_historical_sales_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
    )
    anomalies = (
        jato_monthly_update_service._find_publish_sales_doubling_anomalies(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
    )

    assert [item["country"] for item in changes] == ["Hungary"]
    assert changes[0]["changedMonthCount"] == 2
    assert [item["country"] for item in anomalies] == ["Hungary"]
    assert anomalies[0]["suspiciousMonthCount"] == 2


def test_smart_merge_country_scan_rejects_blank_country_rows(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {"Country": "Hungary", "2026 Jun": 1},
            {"Country": " ", "2026 Jun": 2},
        ]
    ).to_parquet(parquet_path, index=False)

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service._smart_merge_country_value_map(
            parquet_path,
            country_column="Country",
            path_label="candidate 数据集",
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "missing_country_rows"
    assert exc_info.value.detail["rowCount"] == 1


@pytest.mark.parametrize("upload_status", ["assembling", "digesting"])
def test_get_upload_marks_dead_digest_worker_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    upload_status: str,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = f"upload-dead-{upload_status}"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key=f"resume-{upload_status}",
        triggered_by="tester",
    )
    state["status"] = upload_status
    state["digestPid"] = 987654
    state["digestLaunchedAt"] = (
        jato_monthly_update_service._utc_now().isoformat()
    )
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda _pid: False,
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        upload_id,
        requested_by="tester",
        requested_role="editor",
    )

    assert result["status"] == "invalid"
    assert result["digestPid"] is None
    assert result["failureDigest"]["code"] == "DIGEST_WORKER_LOST"
    assert result["failureDigest"]["phase"] == "digesting"
    assert result["failureDigest"]["retryable"] is True


def test_launch_stale_without_pid_quarantines_while_digest_lock_is_held(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "upload-launch-stale-lock-held"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="resume-launch-stale-lock-held",
        triggered_by="tester",
    )
    state["status"] = "assembling"
    state["digestPid"] = None
    state["digestLaunchedAt"] = (
        jato_monthly_update_service._utc_now()
        - timedelta(
            seconds=(
                jato_monthly_update_service.DIGEST_WORKER_STALE_GRACE_SECONDS
                + 1
            )
        )
    ).isoformat()
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
        result = jato_monthly_update_service.get_jato_monthly_update_upload(
            upload_id,
            requested_by="tester",
            requested_role="editor",
        )

        assert result["status"] == "assembling"
        assert result["failureDigest"]["code"] == "RESOURCE_QUARANTINED"
        assert result["failureDigest"]["retryable"] is False
        assert result["failureDigest"]["technicalDetail"][
            "digestLockHeld"
        ] is True
        assert [
            payload["uploadId"]
            for payload in jato_monthly_update_service._active_upload_session_payloads()
        ] == [upload_id]
        with pytest.raises(HTTPException) as initiate_blocked:
            jato_monthly_update_service.initiate_jato_monthly_update_upload(
                filename="other.xlsx",
                size_bytes=4,
                resume_key="other-launch-stale",
                triggered_by="other-user",
            )
        assert initiate_blocked.value.status_code == 409
        with pytest.raises(HTTPException) as cleanup_blocked:
            jato_monthly_update_service.run_jato_monthly_update_cleanup(
                triggered_by="admin",
            )
        assert cleanup_blocked.value.status_code == 409
    finally:
        release_lock.set()
        holder.join(timeout=3)
    assert not holder.is_alive()


def test_launch_stale_without_pid_becomes_worker_lost_when_digest_lock_is_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "upload-launch-stale-lock-free"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="resume-launch-stale-lock-free",
        triggered_by="tester",
    )
    state["status"] = "assembling"
    state["digestPid"] = None
    state["digestLaunchedAt"] = (
        jato_monthly_update_service._utc_now()
        - timedelta(
            seconds=(
                jato_monthly_update_service.DIGEST_WORKER_STALE_GRACE_SECONDS
                + 1
            )
        )
    ).isoformat()
    jato_monthly_update_service._persist_upload_session(state)

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        upload_id,
        requested_by="tester",
        requested_role="editor",
    )

    assert result["status"] == "invalid"
    assert result["failureDigest"]["code"] == "DIGEST_WORKER_LOST"
    assert result["failureDigest"]["retryable"] is True
    assert result["failureDigest"]["technicalDetail"][
        "digestLockHeld"
    ] is False
    assert jato_monthly_update_service._active_upload_session_payloads() == []


def test_get_upload_terminates_verified_digest_worker_after_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "upload-timeout"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="resume-timeout",
        triggered_by="tester",
    )
    state["status"] = "digesting"
    state["digestPid"] = 24680
    state["digestProcessIdentity"] = {
        "startTimeTicks": "123",
        "cmdlineSha256": "a" * 64,
    }
    state["digestLaunchedAt"] = (
        jato_monthly_update_service._utc_now()
        - timedelta(
            seconds=(
                jato_monthly_update_service._digest_worker_timeout_seconds(
                    state["sizeBytes"]
                )
                + 1
            )
        )
    ).isoformat()
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda _pid: True,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_is_digest_worker_for_upload",
        lambda pid, checked_upload_id: (
            pid == 24680 and checked_upload_id == upload_id
        ),
    )
    terminated: list[int] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_terminate_process_group",
        lambda pid, **_kwargs: (
            terminated.append(pid)
            or {
                "pid": pid,
                "sigtermSent": True,
                "sigkillSent": False,
                "processAliveBefore": True,
                "processAliveAfter": False,
            }
        ),
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        upload_id,
        requested_by="tester",
        requested_role="editor",
    )

    assert terminated == [24680]
    assert result["status"] == "invalid"
    assert result["failureDigest"]["code"] == "DIGEST_TIMEOUT"
    assert result["failureDigest"]["technicalDetail"]["termination"][
        "processAliveAfter"
    ] is False
    technical_detail = result["failureDigest"]["technicalDetail"]
    assert technical_detail["timeoutSeconds"] == 10 * 60
    assert technical_detail["elapsedSeconds"] > technical_detail["timeoutSeconds"]
    assert technical_detail["fileSizeBytes"] == 4
    assert technical_detail["digestProcessIdentity"] == {
        "startTimeTicks": "123",
        "cmdlineSha256": "a" * 64,
    }
    assert result["digestPid"] is None
    assert jato_monthly_update_service._active_upload_session_payloads() == []


@pytest.mark.parametrize(
    "termination_error",
    [
        "SIGKILL failed",
        "process identity could not be verified",
    ],
)
def test_timeout_quarantines_live_worker_and_keeps_resource_gates_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    termination_error: str,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "upload-timeout-quarantined"
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="resume-timeout-quarantined",
        triggered_by="tester",
    )
    state["status"] = "digesting"
    state["digestPid"] = 24680
    state["digestProcessIdentity"] = {
        "startTimeTicks": "123",
        "cmdlineSha256": "a" * 64,
    }
    state["digestLaunchedAt"] = (
        jato_monthly_update_service._utc_now()
        - timedelta(
            seconds=(
                jato_monthly_update_service._digest_worker_timeout_seconds(4)
                + 1
            )
        )
    ).isoformat()
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda pid: pid == 24680,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_terminate_process_group",
        lambda pid, **_kwargs: {
            "pid": pid,
            "processAliveBefore": True,
            "processAliveAfter": True,
            "identityVerified": "identity" not in termination_error,
            "error": termination_error,
        },
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        upload_id,
        requested_by="tester",
        requested_role="editor",
    )

    assert result["status"] == "digesting"
    assert result["digestPid"] == 24680
    assert result["failureDigest"]["code"] == "RESOURCE_QUARANTINED"
    assert result["failureDigest"]["retryable"] is False
    assert result["failureDigest"]["technicalDetail"]["termination"][
        "processAliveAfter"
    ] is True
    assert [
        payload["uploadId"]
        for payload in jato_monthly_update_service._active_upload_session_payloads()
    ] == [upload_id]
    with pytest.raises(HTTPException) as initiate_blocked:
        jato_monthly_update_service.initiate_jato_monthly_update_upload(
            filename="other.xlsx",
            size_bytes=4,
            resume_key="other",
            triggered_by="other-user",
        )
    assert initiate_blocked.value.status_code == 409
    with pytest.raises(HTTPException) as cleanup_blocked:
        jato_monthly_update_service.run_jato_monthly_update_cleanup(
            triggered_by="admin",
        )
    assert cleanup_blocked.value.status_code == 409


def test_digest_timeout_is_size_aware_and_bounded() -> None:
    small_timeout = jato_monthly_update_service._digest_worker_timeout_seconds(
        4 * 1024 * 1024
    )
    production_sample_timeout = (
        jato_monthly_update_service._digest_worker_timeout_seconds(339_874_111)
    )
    huge_timeout = jato_monthly_update_service._digest_worker_timeout_seconds(
        10 * 1024 * 1024 * 1024
    )

    assert small_timeout == 10 * 60
    assert 30 * 60 <= production_sample_timeout <= 32 * 60
    assert huge_timeout == 45 * 60


def test_large_upload_digest_is_not_killed_at_ten_minutes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    upload_id = "upload-large-still-digesting"
    file_size_bytes = 339_874_111
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06-16-countries.xlsx",
        size_bytes=file_size_bytes,
        resume_key="resume-large",
        triggered_by="tester",
    )
    state["status"] = "digesting"
    state["digestPid"] = 24681
    state["digestLaunchedAt"] = (
        jato_monthly_update_service._utc_now()
        - timedelta(minutes=10, seconds=5)
    ).isoformat()
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda _pid: True,
    )
    terminated: list[int] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_terminate_process_group",
        lambda pid, **_kwargs: terminated.append(pid),
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        upload_id,
        requested_by="tester",
        requested_role="editor",
    )

    assert terminated == []
    assert result["status"] == "digesting"
    assert result["failureDigest"] is None
    assert (
        jato_monthly_update_service._digest_worker_timeout_seconds(
            file_size_bytes
        )
        > 10 * 60 + 5
    )


def test_invalid_upload_session_is_not_reused_by_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    first = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="same-resume-key",
        triggered_by="tester",
    )
    first_state = jato_monthly_update_service._load_upload_session(
        first["uploadId"]
    )
    first_state["status"] = "invalid"
    first_state["failureDigest"] = {
        "code": "UPLOAD_DIGEST_FAILED",
        "category": "input_validation",
        "phase": "digesting",
        "retryable": False,
        "message": "bad workbook",
        "sourceFeedback": "fix workbook",
        "technicalDetail": None,
        "nextAction": "fix_source",
    }
    jato_monthly_update_service._persist_upload_session(first_state)

    second = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="same-resume-key",
        triggered_by="tester",
    )

    assert second["uploadId"] != first["uploadId"]
    assert second["status"] == "pending"
    assert (
        jato_monthly_update_service.get_jato_monthly_update_upload(
            first["uploadId"],
            requested_by="tester",
            requested_role="editor",
        )["status"]
        == "invalid"
    )


def test_concurrent_complete_replay_launches_digest_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_upload_root(tmp_path, monkeypatch)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "UPLOAD_CHUNK_SIZE_BYTES",
        4,
    )
    initiated = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key="concurrent-complete",
        triggered_by="tester",
    )
    upload_id = initiated["uploadId"]
    jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=1,
        content=b"data",
        chunk_sha256=(
            "3a6eb0790f39ac87c94f3856b2dd2c5d"
            "110e6811602261a9a923d3bb23adc8b7"
        ),
        requested_by="tester",
        requested_role="editor",
    )

    # Completion now acquires the canonical maintenance, global-upload and
    # per-session locks in one thread.  Model those distinct file locks with a
    # re-entrant test lock while still serializing both concurrent replays.
    completion_lock = threading.RLock()

    @contextmanager
    def locked(
        _path: Path,
        *,
        blocking: bool = True,
    ) -> Iterator[bool]:
        del blocking
        with completion_lock:
            yield True

    launch_count = 0
    launch_count_lock = threading.Lock()

    def launch_once(_upload_id: str) -> int:
        nonlocal launch_count
        assert _upload_id == upload_id
        with launch_count_lock:
            launch_count += 1
        time.sleep(0.05)
        return 424242

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_exclusive_file_lock",
        locked,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        launch_once,
    )
    start = threading.Barrier(2)

    def complete() -> dict[str, Any]:
        start.wait(timeout=2)
        return jato_monthly_update_service.complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by="tester",
            requested_role="editor",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: complete(), range(2)))

    assert launch_count == 1
    assert {result["status"] for result in results} == {"assembling"}
    persisted = jato_monthly_update_service._load_upload_session(upload_id)
    assert persisted["digestAttempts"] == 1
    assert persisted["digestPid"] == 424242


def _persist_supervised_digest_attempt(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    upload_id: str,
    status: str = "digesting",
) -> tuple[dict[str, Any], Path]:
    _configure_upload_root(tmp_path, monkeypatch)
    state = jato_monthly_update_service._prepare_upload_session_state(
        upload_id=upload_id,
        filename="JATO-2026.06.xlsx",
        size_bytes=4,
        resume_key=f"resume-{upload_id}",
        triggered_by="tester",
    )
    state["status"] = status
    state["digestPid"] = 424242
    state["digestProcessIdentity"] = {
        "startTimeTicks": "123",
        "cmdlineSha256": "a" * 64,
    }
    state["digestLaunchedAt"] = (
        jato_monthly_update_service._utc_now().isoformat()
    )
    state["digestAttempts"] = 1
    state["digestAttempt"] = (
        jato_monthly_update_service._new_upload_digest_attempt(
            upload_id=upload_id,
            attempt_number=1,
        )
    )
    state["digestAttempt"]["status"] = "running"
    state["digestAttempt"]["supervisorPid"] = 424242
    state["digestAttempt"]["supervisorIdentity"] = {
        "arguments": ["/private/server/path/jato_monthly_worker.py"],
    }
    jato_monthly_update_service._persist_upload_session(state)
    receipt_path = (
        jato_monthly_update_service._upload_digest_attempt_artifact_path(
            state,
            "receiptPath",
        )
    )
    assert receipt_path is not None
    return state, receipt_path


def _write_finished_digest_receipt(
    *,
    state: dict[str, Any],
    receipt_path: Path,
    return_code: int,
    termination_reason: str | None = None,
    peak_rss_bytes: int = 128 * 1024 * 1024,
) -> None:
    attempt = state["digestAttempt"]
    jato_monthly_update_service._write_json(
        receipt_path,
        {
            "schemaVersion": 1,
            "status": "finished",
            "uploadId": state["uploadId"],
            "attemptId": attempt["attemptId"],
            "supervisorPid": state["digestPid"],
            "workerPid": 434343,
            "returnCode": return_code,
            "signalNumber": abs(return_code) if return_code < 0 else None,
            "signalName": "SIGKILL" if return_code == -9 else None,
            "terminationReason": termination_reason,
            "elapsedSeconds": 3.5,
            "peakRssBytes": peak_rss_bytes,
            "rssWarningBytes": 1024 * 1024 * 1024,
            "rssLimitBytes": 1536 * 1024 * 1024,
            "oomKillDelta": 0,
            "cgroupEventDelta": {"oom": 0, "oom_kill": 0},
            "supervisorError": None,
            "logPath": "/must/not/leak/absolute/path.log",
        },
    )


def test_get_upload_reconciles_sigkill_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state, receipt_path = _persist_supervised_digest_attempt(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        upload_id="upload-receipt-sigkill",
    )
    _write_finished_digest_receipt(
        state=state,
        receipt_path=receipt_path,
        return_code=-9,
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        state["uploadId"],
        requested_by="tester",
        requested_role="editor",
    )

    assert result["status"] == "invalid"
    assert result["failureDigest"]["code"] == "DIGEST_WORKER_SIGNALLED"
    assert result["failureDigest"]["phase"] == "digesting"
    detail = result["failureDigest"]["technicalDetail"]
    assert detail["exitReceipt"]["returnCode"] == -9
    assert detail["exitReceipt"]["signalName"] == "SIGKILL"
    assert detail["exitReceipt"]["logPath"] == state["digestAttempt"]["logPath"]
    assert detail["digestProcessIdentity"] == state["digestProcessIdentity"]
    assert result["digestPid"] is None
    assert "supervisorIdentity" not in result["digestAttempt"]
    assert "logPath" not in result["digestAttempt"]
    assert "receiptPath" not in result["digestAttempt"]


def test_get_upload_reconciles_actual_rss_limit_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state, receipt_path = _persist_supervised_digest_attempt(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        upload_id="upload-receipt-memory-limit",
    )
    _write_finished_digest_receipt(
        state=state,
        receipt_path=receipt_path,
        return_code=-15,
        termination_reason="rss_limit",
        peak_rss_bytes=1600 * 1024 * 1024,
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        state["uploadId"],
        requested_by="tester",
        requested_role="editor",
    )

    assert result["status"] == "invalid"
    assert result["failureDigest"]["code"] == "DIGEST_MEMORY_LIMIT"
    assert result["failureDigest"]["retryable"] is False
    receipt = result["failureDigest"]["technicalDetail"]["exitReceipt"]
    assert receipt["peakRssBytes"] == 1600 * 1024 * 1024
    assert receipt["rssLimitBytes"] == 1536 * 1024 * 1024


def test_get_upload_reconciles_zero_exit_without_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state, receipt_path = _persist_supervised_digest_attempt(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        upload_id="upload-receipt-no-result",
    )
    _write_finished_digest_receipt(
        state=state,
        receipt_path=receipt_path,
        return_code=0,
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        state["uploadId"],
        requested_by="tester",
        requested_role="editor",
    )

    assert result["status"] == "invalid"
    assert result["failureDigest"]["code"] == "DIGEST_RESULT_MISSING"
    assert result["failureDigest"]["retryable"] is True


def test_finished_receipt_does_not_misclassify_ready_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state, receipt_path = _persist_supervised_digest_attempt(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        upload_id="upload-receipt-ready",
        status="ready",
    )
    state["completedAt"] = jato_monthly_update_service._utc_now().isoformat()
    state["ingestDigest"] = {"status": "ready", "blockers": []}
    jato_monthly_update_service._persist_upload_session(state)
    _write_finished_digest_receipt(
        state=state,
        receipt_path=receipt_path,
        return_code=0,
    )

    result = jato_monthly_update_service.get_jato_monthly_update_upload(
        state["uploadId"],
        requested_by="tester",
        requested_role="editor",
    )

    assert result["status"] == "ready"
    assert result["failureDigest"] is None
    assert result["ingestDigest"] == {"status": "ready", "blockers": []}
    assert result["digestAttempt"]["exit"]["returnCode"] == 0
    assert result["digestPid"] is None


def test_mismatched_attempt_receipt_is_ignored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state, receipt_path = _persist_supervised_digest_attempt(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        upload_id="upload-receipt-attempt-mismatch",
    )
    _write_finished_digest_receipt(
        state=state,
        receipt_path=receipt_path,
        return_code=-9,
    )
    receipt = jato_monthly_update_service._read_json(receipt_path)
    receipt["attemptId"] = "old-attempt"
    jato_monthly_update_service._write_json(receipt_path, receipt)

    with jato_monthly_update_service._exclusive_file_lock(
        jato_monthly_update_service._upload_state_lock_path(state["uploadId"])
    ) as acquired:
        assert acquired
        reconciled = (
            jato_monthly_update_service._reconcile_digest_attempt_receipt_locked(
                jato_monthly_update_service._load_upload_session(
                    state["uploadId"]
                )
            )
        )

    assert reconciled["status"] == "digesting"
    assert reconciled["digestPid"] == 424242
    assert reconciled["failureDigest"] is None
    assert reconciled["digestAttempt"]["exit"] is None


def test_missing_supervisor_with_live_child_keeps_resource_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state, _receipt_path = _persist_supervised_digest_attempt(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        upload_id="upload-supervisor-missing-child-live",
    )
    state["digestAttempt"]["supervisorMissingAt"] = (
        jato_monthly_update_service._utc_now()
        - timedelta(
            seconds=(
                jato_monthly_update_service.DIGEST_EXIT_RECEIPT_GRACE_SECONDS
                + 1
            )
        )
    ).isoformat()
    jato_monthly_update_service._persist_upload_session(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_process_exists",
        lambda _pid: False,
    )
    lock_ready = threading.Event()
    release_lock = threading.Event()

    def hold_digest_lock() -> None:
        with jato_monthly_update_service._exclusive_file_lock(
            jato_monthly_update_service._upload_digest_lock_path(
                state["uploadId"]
            )
        ) as acquired:
            assert acquired
            lock_ready.set()
            assert release_lock.wait(timeout=3)

    holder = threading.Thread(target=hold_digest_lock)
    holder.start()
    assert lock_ready.wait(timeout=3)
    try:
        result = jato_monthly_update_service.get_jato_monthly_update_upload(
            state["uploadId"],
            requested_by="tester",
            requested_role="editor",
        )
    finally:
        release_lock.set()
        holder.join(timeout=3)

    assert result["status"] == "digesting"
    assert result["failureDigest"]["code"] == "RESOURCE_QUARANTINED"
    assert result["failureDigest"]["retryable"] is False
    assert result["failureDigest"]["technicalDetail"][
        "digestLockHeld"
    ] is True


def test_digest_child_refuses_stale_attempt_before_any_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state, _receipt_path = _persist_supervised_digest_attempt(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        upload_id="upload-stale-child-attempt",
    )
    monkeypatch.setenv("APP_JATO_DIGEST_ATTEMPT_ID", "old-attempt")

    result = jato_monthly_update_service.run_jato_monthly_update_upload_digest(
        state["uploadId"]
    )

    assert result["status"] == "digesting"
    persisted = jato_monthly_update_service._load_upload_session(
        state["uploadId"]
    )
    assert persisted["digestPid"] == 424242
    assert persisted["digestWorkerPid"] is None
    assert persisted["digestAttempt"]["attemptId"] == state["digestAttempt"][
        "attemptId"
    ]


def test_worker_drain_waits_when_cycle_lock_is_held(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results = [
        {
            "processedJobId": None,
            "reconciledJobIds": [],
            "skipped": "worker_lock_held",
        },
        {
            "processedJobId": None,
            "reconciledJobIds": [],
        },
    ]
    calls = 0

    def worker_once() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return results.pop(0)

    sleeps: list[float] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "run_jato_monthly_update_worker_once",
        worker_once,
    )
    monkeypatch.setattr(jato_monthly_worker, "_apply_resource_limits", lambda: None)
    monkeypatch.setattr(
        jato_monthly_worker.time,
        "sleep",
        lambda seconds: sleeps.append(seconds),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(WORKER_PATH),
            "--drain",
            "--poll-seconds",
            "0",
        ],
    )

    assert jato_monthly_worker.main() == 0
    assert calls == 2
    assert sleeps == [0.5]


def test_digest_reports_lightweight_washed_source_issues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = tmp_path / "JATO-Hungary-2026-06.xlsx"
    workbook = Workbook(write_only=True)
    worksheet = workbook.create_sheet("Data Export")
    worksheet.append(
        [
            "国家",
            "Model",
            "Model",
            "2026 Apr",
            "2026 May",
            "2026 Jun",
        ]
    )
    worksheet.append(["匈牙利", "T5", "T5 EVO", "N/A", -1, 10])
    worksheet.append(["", "orphan", "orphan", 1, 2, 3])
    workbook.save(workbook_path)

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_partition_country_names",
        lambda: ["匈牙利", "捷克"],
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_country_latest_month",
        lambda country: "2026 May" if country == "匈牙利" else "2026 May",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "active-v1",
    )

    digest = jato_monthly_update_service._build_upload_ingest_digest(
        path=workbook_path,
        file_sha256="test-sha",
        size_bytes=workbook_path.stat().st_size,
    )

    blockers = {
        blocker["code"]: blocker
        for blocker in digest["blockers"]
    }
    assert digest["status"] == "invalid"
    assert digest["route"] == "single_country"
    assert digest["candidateScope"] == "target_country_partition_only"
    assert digest["advancedCountries"] == ["匈牙利"]
    assert {
        "DUPLICATE_COLUMNS",
        "NON_NUMERIC_MONTHLY_SALES",
        "NEGATIVE_MONTHLY_SALES",
        "SALES_WITHOUT_COUNTRY",
    }.issubset(blockers)
    assert blockers["DUPLICATE_COLUMNS"]["fields"] == ["Model"]
    assert blockers["NON_NUMERIC_MONTHLY_SALES"]["fields"] == ["2026 Apr"]
    assert blockers["NEGATIVE_MONTHLY_SALES"]["fields"] == ["2026 May"]
    assert blockers["SALES_WITHOUT_COUNTRY"]["fields"] == ["国家"]
    assert all(
        blocker["sourceFeedback"]
        for blocker in blockers.values()
    )
