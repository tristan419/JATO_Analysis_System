from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_generic_retry_requires_dedicated_smart_merge_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_root, job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-smart-merge-failed"
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
    state.update(
        {
            "status": "failed",
            "phase": "smart_merge_failed",
            "operation": "smart_merge",
            "error": "simulated Smart Merge memory failure",
            "activeBaseFingerprint": "a" * 64,
            "historicalReclassificationResolution": {
                "status": "failed",
                "sourceCandidateFingerprint": "b" * 64,
                "reportFingerprint": "c" * 64,
                "decisions": [
                    {"country": "德国", "decision": "keep_active"}
                ],
            },
        }
    )
    jato_monthly_update_service._persist_job_state(state)
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        launches.append,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-unexpected-retry",
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.retry_failed_jato_monthly_update_job(
            source_job_id=job_id,
            triggered_by="tester",
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == {
        "code": "SMART_MERGE_RESUME_REQUIRED",
        "blockerType": "smart_merge_resume_required",
        "message": exc_info.value.detail["message"],
        "jobId": job_id,
        "nextAction": "resume_smart_merge",
    }
    assert "Smart Merge" in exc_info.value.detail["message"]
    assert launches == []
    assert sorted(
        path.parent.name
        for path in job_root.glob("jato-*/job_state.json")
    ) == [job_id]
    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["status"] == "failed"
    assert persisted["phase"] == "smart_merge_failed"
    assert persisted["historicalReclassificationResolution"]["decisions"] == [
        {"country": "德国", "decision": "keep_active"}
    ]


def test_streaming_smart_merge_never_reads_source_parquet_with_pandas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    output_path = tmp_path / "merged.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "Old",
                "2026 May": 10,
            },
            {
                "Country": "Germany",
                "Make": "OTHER",
                "Model": "STABLE",
                "Version name": "Stable",
                "2026 May": 20,
            },
        ]
    ).to_parquet(active_path, index=False, row_group_size=1)
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "New",
                "2026 May": 999,
                "2026 Jun": 12,
            }
        ]
    ).to_parquet(candidate_path, index=False, row_group_size=1)
    source_paths = {active_path.resolve(), candidate_path.resolve()}
    real_read_parquet = pd.read_parquet

    def guarded_read_parquet(path: object, *args: object, **kwargs: object):
        try:
            resolved = Path(path).resolve()  # type: ignore[arg-type]
        except TypeError:
            resolved = None
        if resolved in source_paths:
            raise AssertionError(
                f"Smart Merge materialized source parquet with pandas: {resolved}"
            )
        return real_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", guarded_read_parquet)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "SMART_MERGE_SCAN_BATCH_ROWS",
        1,
    )

    row_count, summary = (
        jato_monthly_update_service._smart_merge_parquet_streaming(
            active_path=active_path,
            candidate_path=candidate_path,
            output_path=output_path,
            regressed_countries=[],
            historical_reclassification_decisions={
                "Czechia": "keep_active"
            },
        )
    )

    merged = pq.read_table(output_path).to_pandas()
    czechia = merged.loc[merged["Country"] == "Czechia"]
    germany = merged.loc[merged["Country"] == "Germany"]
    assert row_count == len(merged) == 3
    assert czechia["2026 May"].fillna(0).sum() == 10
    assert czechia["2026 Jun"].fillna(0).sum() == 12
    assert germany["2026 May"].fillna(0).sum() == 20
    assert germany["2026 Jun"].fillna(0).sum() == 0
    assert summary["resourceProfile"]["scanBatchRows"] == 1
    assert summary["resourceProfile"]["maxInputBatchRows"] <= 1
    assert summary["resourceProfile"]["spilledRows"] > 0


def test_cross_batch_normalized_duplicate_blocks_without_replacing_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Germany",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "NEW",
                "2026 May": 10,
            }
        ]
    ).to_parquet(active_path, index=False, row_group_size=1)
    pd.DataFrame(
        [
            {
                "Country": "Germany",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "NEW",
                "2026 May": 10,
                "2026 Jun": 3,
            },
            {
                "Country": "Germany",
                "Make": " brand ",
                "Model": " model ",
                "Version name": " new ",
                "2026 May": 0,
                "2026 Jun": 4,
            },
        ]
    ).to_parquet(candidate_path, index=False, row_group_size=1)
    candidate_sha_before = _sha256(candidate_path)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "SMART_MERGE_SCAN_BATCH_ROWS",
        1,
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service._smart_merge_parquet_streaming(
            active_path=active_path,
            candidate_path=candidate_path,
            regressed_countries=[],
            historical_reclassification_decisions={
                "Germany": "keep_active"
            },
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "duplicate_configurations"
    assert exc_info.value.detail["source"] == "candidate_future"
    assert exc_info.value.detail["duplicateRows"] == 2
    assert _sha256(candidate_path) == candidate_sha_before
    assert list(
        tmp_path.glob(f".{candidate_path.name}.*.smart-merge*")
    ) == []
