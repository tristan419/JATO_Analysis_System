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


def test_full_precompute_loads_archive_once_and_counts_from_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parquet_path = str(tmp_path / "candidate.parquet")
    load_calls: list[str] = []
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

    monkeypatch.setattr(precompute_summaries, "count_analysis_rows", fake_count)
    monkeypatch.setattr(precompute_summaries, "load_analysis_data", fake_load)
    monkeypatch.setattr(
        precompute_summaries,
        "load_existing_summary",
        lambda _output_dir, _summary_name: pd.DataFrame(),
    )
    monkeypatch.setattr(
        precompute_summaries,
        "compute_country_summary",
        lambda _df: pd.DataFrame({"country": ["all"]}),
    )
    monkeypatch.setattr(
        precompute_summaries,
        "compute_year_month_summary",
        lambda _df: pd.DataFrame({"month": ["2026-06"]}),
    )
    monkeypatch.setattr(
        precompute_summaries,
        "compute_powertrain_summary",
        lambda _df: pd.DataFrame({"powertrain": ["all"]}),
    )
    monkeypatch.setattr(
        precompute_summaries,
        "compute_segment_summary",
        lambda _df: pd.DataFrame({"segment": ["all"]}),
    )
    monkeypatch.setattr(
        precompute_summaries,
        "compute_top_makes_summary",
        lambda _df, top_n: pd.DataFrame(
            {"make": [f"top-{top_n}"]}
        ),
    )

    manifest = precompute_summaries.precompute_all_summaries(
        parquet_path,
        output_dir=str(tmp_path / "summaries"),
    )

    assert metadata_calls == [parquet_path]
    assert load_calls == [parquet_path]
    assert manifest["originalRowCount"] == 321
    assert manifest["totalSummaryRows"] == 5


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
                jato_monthly_update_service.DIGEST_WORKER_MAX_SECONDS
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

    completion_lock = threading.Lock()

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
