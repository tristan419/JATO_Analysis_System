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


@pytest.mark.parametrize("decision", [None, "keep_active", "use_latest"])
def test_smart_merge_advances_months_without_losing_old_history(
    tmp_path: Path, decision: str | None,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    output_path = tmp_path / "merged.parquet"
    pd.DataFrame([
        {"Country": "Finland", "Make": "BRAND", "Model": "MODEL",
         "Version name": "Old", "2023 Jan": 7, "2026 Aug": 10},
        {"Country": "Germany", "Make": "OTHER", "Model": "STABLE",
         "Version name": "Stable", "2023 Jan": 20, "2026 Aug": 30},
    ]).to_parquet(active_path, index=False)
    pd.DataFrame([
        {"Country": "Finland", "Make": "BRAND", "Model": "MODEL",
         "Version name": "New", "2026 Aug": 999, "2026 Sep": 12,
         "YTD 2026 (Sep)": 1011},
    ]).to_parquet(candidate_path, index=False)
    original_hashes = (_sha256(active_path), _sha256(candidate_path))

    _, summary = jato_monthly_update_service._smart_merge_parquet_streaming(
        active_path=active_path, candidate_path=candidate_path, output_path=output_path,
        regressed_countries=[],
        historical_reclassification_decisions={"Finland": decision} if decision else None,
    )

    merged = pq.read_table(output_path).to_pandas()
    finland = merged.loc[merged["Country"] == "Finland"]
    germany = merged.loc[merged["Country"] == "Germany"]
    assert finland["2023 Jan"].sum() == 7
    assert finland["2026 Aug"].sum() == (999 if decision == "use_latest" else 10)
    assert finland["2026 Sep"].sum() == 12
    assert finland["YTD 2026 (Sep)"].sum() == (1011 if decision == "use_latest" else 22)
    assert germany["2023 Jan"].sum() == 20
    assert germany["2026 Aug"].sum() == 30
    assert germany["2026 Sep"].fillna(0).sum() == 0
    policy = summary["historicalReclassificationPolicies"]["Finland"]
    assert policy["policy"] == (decision or "keep_active")
    assert policy["monthBoundaryCheck"] == "pass"
    assert original_hashes == (_sha256(active_path), _sha256(candidate_path))
    assert list(tmp_path.glob(".*.smart-merge*")) == []


def test_four_country_september_patch_passes_complete_review_after_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    countries = ["奥地利", "比利时", "丹麦", "芬兰"]
    old_months = [f"2023 {month}" for month in (
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    )]
    active_rows = [{
        "国家": country, "Make": "BRAND", "Model": "MODEL",
        "Version name": "ONE", **{month: index for month in old_months},
        "2026 Aug": index * 10,
    } for index, country in enumerate([*countries, "德国"], start=1)]
    candidate_rows = [{
        key: value for key, value in row.items() if key not in old_months
    } | {"2026 Sep": index * 11} for index, row in enumerate(active_rows[:4], start=1)]
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    output_path = tmp_path / "merged.parquet"
    pd.DataFrame(active_rows).to_parquet(active_path, index=False)
    pd.DataFrame(candidate_rows).to_parquet(candidate_path, index=False)
    active_hash = _sha256(active_path)
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(jato_monthly_update_service, "_active_data_paths", lambda: {
        "parquet": active_path, "partition": tmp_path / "no-partitions",
    })
    payload = {
        "jobId": "september-four-countries", "countryScope": countries,
        "artifacts": {
            "candidateScope": "target_country_partitions_only",
            "stagingOutputPath": str(candidate_path),
            "untouchedPartitionCheck": {"status": "pass"},
        },
    }
    raw_review = jato_monthly_update_service._build_partial_country_review(
        payload, candidate_fingerprint="a" * 64,
    )
    assert raw_review["decisionSuggestion"] == "manual_review_required"
    assert len([f for f in raw_review["reviewFindings"] if f["ruleId"] == "SC015"]) == 4

    row_count, _summary = jato_monthly_update_service._smart_merge_parquet_streaming(
        active_path=active_path, candidate_path=candidate_path, output_path=output_path,
        regressed_countries=[],
    )
    assert row_count == 5  # exact matching configurations merge once, never add overlap
    payload["artifacts"]["candidateScope"] = "full_smart_merge"
    payload["artifacts"]["stagingOutputPath"] = str(output_path)
    full_review = jato_monthly_update_service._build_partial_country_review(
        payload, candidate_fingerprint="b" * 64,
    )
    assert full_review["decisionSuggestion"] == "manual_review_required"
    assert not any(f["severity"] == "blocker" for f in full_review["reviewFindings"])
    merged = pq.read_table(output_path).to_pandas().set_index("国家")
    for index, country in enumerate(countries, start=1):
        assert merged.loc[country, old_months].sum() == 12 * index
        assert merged.loc[country, "2026 Aug"] == index * 10
        assert merged.loc[country, "2026 Sep"] == index * 11
    assert merged.loc["德国", old_months].sum() == 60
    assert merged.loc["德国", "2026 Aug"] == 50
    assert pd.isna(merged.loc["德国", "2026 Sep"])
    assert _sha256(active_path) == active_hash


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
    assert exc_info.value.detail["country"] == "Germany"
    assert exc_info.value.detail["samples"][0]["2026 Jun"] == 3
    assert exc_info.value.detail["samples"][1]["2026 Jun"] == 4
    assert "Germany" in exc_info.value.detail["sourceFeedback"]
    assert _sha256(candidate_path) == candidate_sha_before
    assert list(
        tmp_path.glob(f".{candidate_path.name}.*.smart-merge*")
    ) == []


def test_streaming_merge_preserves_uploaded_null_and_zero_price_rows(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    output_path = tmp_path / "merged.parquet"
    identity = {
        "Country": "Finland", "Make": "KIA", "Model": "PICANTO",
        "Version name": "1.0 LX 4P",
    }
    pd.DataFrame([{
        **identity, "MSRP including delivery charge": 0.0,
        "2023 Jan": 7, "2026 Jun": 5,
    }]).to_parquet(active_path, index=False)
    pd.DataFrame([
        {**identity, "MSRP including delivery charge": None,
         "2026 Jun": 0, "2026 Jul": 2, "2026 Aug": 0, "2026 Sep": 0},
        {**identity, "MSRP including delivery charge": 0.0,
         "2026 Jun": 999, "2026 Jul": 0, "2026 Aug": 3, "2026 Sep": 4},
    ]).to_parquet(candidate_path, index=False, row_group_size=1)
    original_hashes = (_sha256(active_path), _sha256(candidate_path))

    row_count, _summary = (
        jato_monthly_update_service._smart_merge_parquet_streaming(
            active_path=active_path, candidate_path=candidate_path,
            output_path=output_path, regressed_countries=[],
        )
    )

    merged = pq.read_table(output_path).to_pandas()
    assert row_count == 2
    assert merged["MSRP including delivery charge"].isna().sum() == 1
    assert merged["MSRP including delivery charge"].eq(0).sum() == 1
    assert merged[["2023 Jan", "2026 Jun", "2026 Jul", "2026 Aug", "2026 Sep"]].sum().tolist() == [7, 5, 2, 3, 4]
    assert original_hashes == (_sha256(active_path), _sha256(candidate_path))
    assert not jato_monthly_update_service._find_candidate_duplicate_configurations(output_path)
