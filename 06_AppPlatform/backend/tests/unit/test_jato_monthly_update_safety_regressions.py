import hashlib
import importlib.util
import os
import shutil
import sys
import warnings
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPTS_ROOT = REPO_ROOT / "03_Scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

ELT_WORKER_PATH = SCRIPTS_ROOT / "elt_worker.py"
ELT_WORKER_SPEC = importlib.util.spec_from_file_location(
    "jato_monthly_safety_elt_worker",
    ELT_WORKER_PATH,
)
assert ELT_WORKER_SPEC is not None
assert ELT_WORKER_SPEC.loader is not None
elt_worker = importlib.util.module_from_spec(ELT_WORKER_SPEC)
ELT_WORKER_SPEC.loader.exec_module(elt_worker)


def _configure_project(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
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
    return project_root, job_root


def _write_review_refresh_candidate(
    project_root: Path,
) -> tuple[dict[str, str], Path]:
    staging_dir = project_root / "04_Processed_data" / "staging" / "review"
    staging_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = staging_dir / "candidate.parquet"
    manifest_path = staging_dir / "manifest.json"
    report_path = staging_dir / "refresh.json"
    candidate_path.write_bytes(b"candidate-v1")
    manifest_path.write_text('{"version":1}', encoding="utf-8")
    report_path.write_text('{"status":"success"}', encoding="utf-8")
    return (
        {
            "candidateScope": "target_country_partition_only",
            "stagingOutputPath": (
                "04_Processed_data/staging/review/candidate.parquet"
            ),
            "manifestPath": "04_Processed_data/staging/review/manifest.json",
            "refreshReportPath": (
                "04_Processed_data/staging/review/refresh.json"
            ),
        },
        candidate_path,
    )


def _write_country_partition(
    project_root: Path,
    *,
    country: str,
    latest_month: str,
    sales: int,
) -> None:
    partition_dir = (
        project_root
        / "04_Processed_data"
        / "partitioned_dataset_v1"
        / f"国家={quote(country, safe='')}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({latest_month: [sales]}).to_parquet(
        partition_dir / "part-0.parquet",
        index=False,
    )


def test_empty_artifact_path_never_resolves_to_project_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)

    assert jato_monthly_update_service._project_path(None) is None
    assert jato_monthly_update_service._project_path("") is None
    assert jato_monthly_update_service._project_path("   ") is None
    assert (
        jato_monthly_update_service._project_path("candidate.parquet")
        == project_root / "candidate.parquet"
    )


def test_baseline_patch_replaces_country_without_active_supplement() -> None:
    source_index_column = elt_worker.SOURCE_TRACK_COLUMNS[1]
    combined = pd.DataFrame(
        {
            "国家": ["匈牙利 ", "匈牙利", "捷克", "匈牙利 "],
            "Model": ["T5", "T5 EVO", "Enyaq", "T5 EVO"],
            "2026 Apr": [10, 20, 30, 7],
            "2026 May": [None, None, None, 9],
            source_index_column: [0, 0, 0, 1],
        }
    )

    replaced, replacement_summary = (
        elt_worker.replace_baseline_countries_with_patch_rows(
            combined,
            patch_source_indices={1},
        )
    )
    result, supplement_summary = elt_worker.supplement_missing_countries_from_parquet(
        replaced,
        None,
        source_index=2,
        patch_source_indices={1},
    )

    hungary = result.loc[result["国家"] == "匈牙利"]
    assert len(hungary) == 1
    assert hungary.iloc[0]["Model"] == "T5 EVO"
    assert hungary["2026 Apr"].sum() == 7
    assert hungary["2026 May"].sum() == 9
    assert result.loc[result["国家"] == "捷克", "2026 Apr"].sum() == 30
    assert replacement_summary == {
        "enabled": True,
        "patchSourceIndices": [1],
        "replacedCountryCount": 1,
        "replacedCountries": ["匈牙利"],
        "removedBaselineRowCount": 2,
    }
    assert supplement_summary["enabled"] is False
    assert supplement_summary["supplementedRowCount"] == 0


def test_same_country_in_multiple_patch_sources_is_rejected() -> None:
    source_index_column = elt_worker.SOURCE_TRACK_COLUMNS[1]
    combined = pd.DataFrame(
        {
            "国家": ["匈牙利", "匈牙利", "匈牙利"],
            "Model": ["T5", "T5 EVO", "T5 EVO"],
            "2026 May": [0, 9, 9],
            source_index_column: [0, 1, 2],
        }
    )

    with pytest.raises(ValueError, match="washed 快照累加.*匈牙利"):
        elt_worker.replace_baseline_countries_with_patch_rows(
            combined,
            patch_source_indices={1, 2},
        )


@pytest.mark.parametrize(
    ("candidate_rows", "expected_active", "expected_candidate"),
    [
        (
            [
                {
                    "国家": "匈牙利",
                    "2026 Jan": 101,
                    "2026 Feb": 200,
                    "2026 Mar": 300,
                }
            ],
            100,
            101,
        ),
        (
            [
                {
                    "国家": "匈牙利",
                    "2026 Jan": 99,
                    "2026 Feb": 200,
                    "2026 Mar": 300,
                }
            ],
            100,
            99,
        ),
        (
            [
                {
                    "国家": "匈牙利",
                    "2026 Feb": 200,
                    "2026 Mar": 300,
                }
            ],
            100,
            None,
        ),
    ],
    ids=["historical-plus-one", "historical-minus-one", "historical-month-missing"],
)
def test_publish_history_gate_detects_exact_historical_month_changes(
    tmp_path: Path,
    candidate_rows: list[dict[str, object]],
    expected_active: int,
    expected_candidate: int | None,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "国家": "匈牙利",
                "2026 Jan": 100,
                "2026 Feb": 200,
            }
        ]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(candidate_rows).to_parquet(candidate_path, index=False)

    changes = jato_monthly_update_service._find_publish_historical_sales_changes(
        active_parquet_path=active_path,
        candidate_parquet_path=candidate_path,
    )

    assert len(changes) == 1
    assert changes[0]["country"] == "匈牙利"
    assert changes[0]["changedMonthCount"] == 1
    assert changes[0]["sampleMonths"] == [
        {
            "month": "2026 Jan",
            "activeSales": expected_active,
            "candidateSales": expected_candidate,
            "deltaSales": (
                expected_candidate - expected_active
                if expected_candidate is not None
                else None
            ),
        }
    ]


def test_publish_history_gate_uses_logical_country_key_across_case(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [{"Country": "Hungary", "Make": "BMW", "Model": "iX1", "2026 Apr": 10}]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(
        [{"Country": " hungary ", "Make": "BMW", "Model": "iX1", "2026 Apr": 10}]
    ).to_parquet(candidate_path, index=False)

    assert (
        jato_monthly_update_service._find_publish_historical_sales_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
        == []
    )
    assert (
        jato_monthly_update_service._find_publish_country_regressions(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
        == []
    )


def test_publish_gate_rejects_multiple_displays_for_one_logical_country(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [{"Country": "Hungary", "2026 Apr": 10}]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(
        [
            {"Country": "Hungary", "2026 Apr": 10},
            {"Country": "hungary", "2026 May": 12},
        ]
    ).to_parquet(candidate_path, index=False)

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service._find_publish_historical_sales_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "ambiguous_logical_country"
    assert exc_info.value.detail["countries"] == [
        {
            "logicalKey": "hungary",
            "displayValues": ["Hungary", "hungary"],
        }
    ]


def test_publish_gate_rejects_equal_total_make_model_history_rewrite(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Hungary",
                "Make": "BMW",
                "Model": "iX1",
                "2026 Apr": 10,
            },
            {
                "Country": "Hungary",
                "Make": "AUDI",
                "Model": "Q4",
                "2026 Apr": 20,
            },
        ]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(
        [
            {
                "Country": "Hungary",
                "Make": "BMW",
                "Model": "iX1",
                "2026 Apr": 0,
                "2026 May": 5,
            },
            {
                "Country": "Hungary",
                "Make": "AUDI",
                "Model": "Q4",
                "2026 Apr": 30,
                "2026 May": 7,
            },
        ]
    ).to_parquet(candidate_path, index=False)

    assert (
        jato_monthly_update_service._find_publish_historical_sales_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
        == []
    )
    changes = (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
    )

    assert len(changes) == 1
    assert changes[0]["country"] == "Hungary"
    assert changes[0]["reason"] == "unconfirmed_make_model_reclassification"
    assert changes[0]["makeModelMismatchCount"] == 2
    assert {
        (item["Make"], item["Model"])
        for item in changes[0]["impactedMakeModels"]
    } == {("AUDI", "Q4"), ("BMW", "iX1")}


def test_publish_rejects_when_active_changes_after_approval(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-update-active-drift"
    candidate_path = (
        project_root
        / "04_Processed_data"
        / "staging"
        / "2026-05-r1"
        / "candidate.parquet"
    )
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_path.write_bytes(b"reviewed-candidate")
    manifest_path = candidate_path.parent / "manifest.json"
    refresh_report_path = candidate_path.parent / "refresh_job_report.json"
    partition_path = candidate_path.parent / "partitioned_dataset_v1"
    fingerprint_path = candidate_path.parent / "dataset_fingerprint.json"
    summaries_path = candidate_path.parent / "summaries"
    manifest_path.write_text("{}", encoding="utf-8")
    refresh_report_path.write_text("{}", encoding="utf-8")
    partition_path.mkdir(parents=True, exist_ok=True)
    (partition_path / "marker.txt").write_text("partition", encoding="utf-8")
    fingerprint_path.write_text("{}", encoding="utf-8")
    summaries_path.mkdir(parents=True, exist_ok=True)
    (summaries_path / "marker.txt").write_text("summaries", encoding="utf-8")
    upload_path = (
        project_root
        / "04_Processed_data"
        / "ops"
        / "jato_monthly_update_jobs"
        / job_id
        / "uploads"
        / "hungary.xlsx"
    )
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"upload")
    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=job_id,
        month="2026-05",
        triggered_by="tester",
        upload_filename=upload_path.name,
        stored_upload_path=upload_path,
    )
    state["status"] = "success"
    state["phase"] = "completed"
    state["summaries"] = {"refresh": {"jobStatus": "success"}}
    state["artifacts"].update(
        {
            "stagingOutputPath": (
                "04_Processed_data/staging/2026-05-r1/candidate.parquet"
            ),
            "manifestPath": "04_Processed_data/staging/2026-05-r1/manifest.json",
                "refreshReportPath": (
                    "04_Processed_data/staging/2026-05-r1/refresh_job_report.json"
                ),
                "partitionOutputPath": (
                    "04_Processed_data/staging/2026-05-r1/partitioned_dataset_v1"
                ),
                "fingerprintPath": (
                    "04_Processed_data/staging/2026-05-r1/dataset_fingerprint.json"
                ),
                "summariesOutputPath": (
                    "04_Processed_data/staging/2026-05-r1/summaries"
                ),
        }
    )
    jato_monthly_update_service._persist_job_state(state)

    active_version = {"value": "a" * 64}
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: active_version["value"],
    )
    state["activeBaseFingerprint"] = active_version["value"]
    jato_monthly_update_service._persist_job_state(state)
    candidate_fingerprint = (
        jato_monthly_update_service._candidate_fingerprint_id(
            state["artifacts"]
        )
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "reviewFindings": [],
            "candidateFingerprint": candidate_fingerprint,
        },
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_require_no_running_monthly_update_jobs",
        lambda **_kwargs: None,
    )

    approved = jato_monthly_update_service.approve_jato_monthly_update_review(
        job_id=job_id,
        triggered_by="reviewer",
        decision="approve",
    )
    assert approved["reviewApproval"]["activeBaseFingerprint"] == ("a" * 64)

    active_version["value"] = "b" * 64
    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.publish_jato_monthly_update_job(
            job_id=job_id,
            triggered_by="publisher",
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == {
        "blockerType": "stale_candidate",
        "message": "审批后 active 数据已变化，旧 candidate 不得覆盖新 active；请重新生成 Review。",
        "approvedActiveFingerprint": "a" * 64,
        "currentActiveFingerprint": "b" * 64,
    }


def test_cached_review_and_approval_do_not_hash_large_candidate_in_web(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-update-cached-review"
    candidate_path = (
        project_root
        / "04_Processed_data"
        / "staging"
        / "2026-06-r1"
        / "candidate.parquet"
    )
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_path.write_bytes(b"worker-reviewed-candidate")
    upload_path = (
        project_root
        / "04_Processed_data"
        / "ops"
        / "jato_monthly_update_jobs"
        / job_id
        / "uploads"
        / "batch.xlsx"
    )
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"upload")
    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=job_id,
        month="2026-06",
        triggered_by="tester",
        upload_filename=upload_path.name,
        stored_upload_path=upload_path,
    )
    state["status"] = "success"
    state["phase"] = "completed"
    state["activeBaseFingerprint"] = "b" * 64
    state["artifacts"]["stagingOutputPath"] = (
        "04_Processed_data/staging/2026-06-r1/candidate.parquet"
    )
    signature = jato_monthly_update_service._candidate_artifact_stat_signature(
        state["artifacts"]
    )
    review_path = jato_monthly_update_service._job_review_bundle_path(job_id)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    jato_monthly_update_service._write_json(
        review_path,
        {
            "jobId": job_id,
            "reviewFindings": [],
            "candidateFingerprint": "a" * 64,
            "reviewBundleSchemaVersion": (
                jato_monthly_update_service.REVIEW_BUNDLE_SCHEMA_VERSION
            ),
            "candidateArtifactStatSignatureVersion": 2,
            "candidateArtifactStatSignature": signature,
        },
    )
    state["artifacts"]["reviewBundlePath"] = (
        jato_monthly_update_service._relative_to_project(review_path)
    )
    jato_monthly_update_service._persist_job_state(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_candidate_fingerprint_id",
        lambda _artifacts: (_ for _ in ()).throw(
            AssertionError("web request must not content-hash candidate")
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )

    review = jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    approved = jato_monthly_update_service.approve_jato_monthly_update_review(
        job_id=job_id,
        triggered_by="reviewer",
        decision="approve",
    )

    assert review["candidateFingerprint"] == "a" * 64
    assert approved["reviewApproval"]["candidateFingerprint"] == "a" * 64

    cached_bundle = jato_monthly_update_service._read_json(review_path)
    cached_bundle.pop("candidateFingerprint")
    jato_monthly_update_service._write_json(review_path, cached_bundle)
    with pytest.raises(HTTPException) as missing_fingerprint:
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    assert missing_fingerprint.value.detail["reason"] == (
        "candidate_fingerprint_unavailable"
    )
    assert missing_fingerprint.value.detail["canRebuild"] is False
    cached_bundle["candidateFingerprint"] = "a" * 64
    jato_monthly_update_service._write_json(review_path, cached_bundle)

    candidate_path.write_bytes(b"candidate-drift-with-different-size")
    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    assert exc_info.value.detail["blockerType"] == "review_bundle_stale"
    assert exc_info.value.detail["reason"] == "candidate_metadata_changed"


def test_candidate_stat_signature_v2_ignores_inode_and_ctime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    artifacts, candidate_path = _write_review_refresh_candidate(project_root)
    original_stat = candidate_path.stat()
    original_signature = (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
    )

    replacement = candidate_path.with_suffix(".replacement")
    replacement.write_bytes(candidate_path.read_bytes())
    os.utime(
        replacement,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    os.replace(replacement, candidate_path)

    replaced_stat = candidate_path.stat()
    assert (
        replaced_stat.st_ino != original_stat.st_ino
        or replaced_stat.st_ctime_ns != original_stat.st_ctime_ns
    )
    assert (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
        == original_signature
    )
    assert original_signature.startswith("v2:")

    candidate_path.write_bytes(b"candidate-v2")
    os.utime(
        candidate_path,
        ns=(replaced_stat.st_atime_ns, replaced_stat.st_mtime_ns),
    )
    assert (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
        == original_signature
    )
    candidate_path.write_bytes(b"candidate-with-a-new-size")
    os.utime(
        candidate_path,
        ns=(replaced_stat.st_atime_ns, replaced_stat.st_mtime_ns),
    )
    assert (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
        != original_signature
    )


def test_legacy_review_bundle_fails_closed_without_web_content_hash(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    artifacts, _candidate_path = _write_review_refresh_candidate(project_root)
    job_id = "jato-review-legacy-signature"
    candidate_fingerprint = (
        jato_monthly_update_service._candidate_fingerprint_id(artifacts)
    )
    review_path = jato_monthly_update_service._job_review_bundle_path(job_id)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    jato_monthly_update_service._write_json(
        review_path,
        {
            "jobId": job_id,
            "candidateFingerprint": candidate_fingerprint,
            "candidateArtifactStatSignature": "1" * 64,
        },
    )
    artifacts["reviewBundlePath"] = (
        jato_monthly_update_service._relative_to_project(review_path)
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "activeBaseFingerprint": "b" * 64,
            "artifacts": artifacts,
        }
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_candidate_fingerprint_id",
        lambda _artifacts: (_ for _ in ()).throw(
            AssertionError("GET Review must not content-hash candidate")
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)

    assert exc_info.value.detail["blockerType"] == "review_bundle_stale"
    assert exc_info.value.detail["reason"] == "legacy_review_bundle_schema"
    assert exc_info.value.detail["canRebuild"] is True
    assert exc_info.value.detail["candidateFingerprint"] == (
        candidate_fingerprint
    )

    legacy_bundle = jato_monthly_update_service._read_json(review_path)
    legacy_bundle.update(
        {
            "reviewBundleSchemaVersion": (
                jato_monthly_update_service.REVIEW_BUNDLE_SCHEMA_VERSION
            ),
            "candidateArtifactStatSignatureVersion": 1,
            "candidateArtifactStatSignature": (
                jato_monthly_update_service
                ._candidate_artifact_stat_signature(artifacts)
            ),
        }
    )
    jato_monthly_update_service._write_json(review_path, legacy_bundle)
    with pytest.raises(HTTPException) as wrong_metadata_version:
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    assert wrong_metadata_version.value.detail["reason"] == (
        "legacy_stat_signature_metadata"
    )

    legacy_bundle.pop("candidateFingerprint")
    jato_monthly_update_service._write_json(review_path, legacy_bundle)
    with pytest.raises(HTTPException) as missing_fingerprint:
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    assert missing_fingerprint.value.detail["reason"] == (
        "legacy_stat_signature_metadata"
    )
    assert missing_fingerprint.value.detail["canRebuild"] is False
    assert missing_fingerprint.value.detail["rebuildBlockerReason"] == (
        "candidate_fingerprint_unavailable"
    )
    with pytest.raises(HTTPException) as queue_error:
        (
            jato_monthly_update_service
            ._queue_jato_monthly_update_review_refresh_locked(
                job_id=job_id,
                triggered_by="tester",
                request_id="review-request-missing-fingerprint",
                expected_candidate_fingerprint=None,
            )
        )
    assert queue_error.value.detail["reason"] == (
        "candidate_fingerprint_unavailable"
    )


@pytest.mark.parametrize("bundle_text", ["{not-json", "[]"])
def test_corrupt_review_bundle_is_structured_and_cannot_rebuild(
    tmp_path: Path,
    monkeypatch,
    bundle_text: str,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    artifacts, _candidate_path = _write_review_refresh_candidate(project_root)
    job_id = "jato-review-corrupt"
    review_path = jato_monthly_update_service._job_review_bundle_path(job_id)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(bundle_text, encoding="utf-8")
    artifacts["reviewBundlePath"] = (
        jato_monthly_update_service._relative_to_project(review_path)
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "activeBaseFingerprint": "b" * 64,
            "artifacts": artifacts,
        }
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_candidate_fingerprint_id",
        lambda _artifacts: (_ for _ in ()).throw(
            AssertionError("corrupt bundle must not trigger a Web content hash")
        ),
    )

    with pytest.raises(HTTPException) as review_error:
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    assert review_error.value.detail["blockerType"] == "review_bundle_stale"
    assert review_error.value.detail["reason"] == "review_bundle_corrupt"
    assert review_error.value.detail["canRebuild"] is False
    assert review_error.value.detail["rebuildBlockerReason"] == (
        "candidate_fingerprint_unavailable"
    )

    with pytest.raises(HTTPException) as queue_error:
        (
            jato_monthly_update_service
            ._queue_jato_monthly_update_review_refresh_locked(
                job_id=job_id,
                triggered_by="tester",
                request_id="review-request-corrupt",
                expected_candidate_fingerprint=None,
            )
        )
    assert queue_error.value.detail["reason"] == "review_bundle_corrupt"
    assert queue_error.value.detail["canRebuild"] is False


def test_review_refresh_worker_rejects_same_stat_content_drift_and_keeps_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    artifacts, candidate_path = _write_review_refresh_candidate(project_root)
    job_id = "jato-review-content-drift"
    candidate_fingerprint = (
        jato_monthly_update_service._candidate_fingerprint_id(artifacts)
    )
    original_stat = candidate_path.stat()
    original_signature = (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
    )
    review_path = jato_monthly_update_service._job_review_bundle_path(job_id)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    jato_monthly_update_service._write_json(
        review_path,
        {"sentinel": "old-review", "candidateFingerprint": candidate_fingerprint},
    )
    artifacts["reviewBundlePath"] = (
        jato_monthly_update_service._relative_to_project(review_path)
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "activeBaseFingerprint": "b" * 64,
            "artifacts": artifacts,
        }
    )
    candidate_path.write_bytes(b"candidate-v2")
    os.utime(
        candidate_path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    assert (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
        == original_signature
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service._cache_jato_monthly_update_review(
            job_id,
            expected_candidate_fingerprint=candidate_fingerprint,
            expected_active_fingerprint="b" * 64,
            review_generation_id="review-generation-1",
        )

    assert exc_info.value.detail["blockerType"] == "candidate_content_drift"
    assert jato_monthly_update_service._read_json(review_path)["sentinel"] == (
        "old-review"
    )


def test_review_refresh_queue_reuses_one_operation_and_clears_approval(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    artifacts, candidate_path = _write_review_refresh_candidate(project_root)
    job_id = "jato-review-double-click"
    candidate_fingerprint = (
        jato_monthly_update_service._candidate_fingerprint_id(artifacts)
    )
    review_path = jato_monthly_update_service._job_review_bundle_path(job_id)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    jato_monthly_update_service._write_json(
        review_path,
        {
            "candidateFingerprint": candidate_fingerprint,
            "reviewBundleSchemaVersion": 2,
            "candidateArtifactStatSignatureVersion": 1,
            "candidateArtifactStatSignature": (
                jato_monthly_update_service
                ._candidate_artifact_stat_signature(artifacts)
            ),
        },
    )
    artifacts["reviewBundlePath"] = (
        jato_monthly_update_service._relative_to_project(review_path)
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "activeBaseFingerprint": "b" * 64,
            "artifacts": artifacts,
            "reviewApproval": {
                "decision": "approved",
                "candidateFingerprint": candidate_fingerprint,
            },
        }
    )
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda launched_job_id: launches.append(launched_job_id),
    )
    candidate_before = candidate_path.read_bytes()

    first = (
        jato_monthly_update_service
        ._queue_jato_monthly_update_review_refresh_locked(
            job_id=job_id,
            triggered_by="tester",
            request_id="review-request-1",
            expected_candidate_fingerprint=candidate_fingerprint,
        )
    )
    replay = (
        jato_monthly_update_service
        ._queue_jato_monthly_update_review_refresh_locked(
            job_id=job_id,
            triggered_by="tester",
            request_id="review-request-1",
            expected_candidate_fingerprint=candidate_fingerprint,
        )
    )

    assert replay["pendingOperation"]["operationId"] == (
        first["pendingOperation"]["operationId"]
    )
    assert replay["pendingOperation"]["type"] == "review_refresh"
    assert replay["pendingOperation"]["status"] == "queued"
    assert launches == [job_id, job_id]
    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["status"] == "success"
    assert persisted["phase"] == "completed"
    assert persisted["reviewApproval"] is None
    assert candidate_path.read_bytes() == candidate_before

    with pytest.raises(HTTPException, match="另一个 candidate"):
        (
            jato_monthly_update_service
            ._queue_jato_monthly_update_review_refresh_locked(
                job_id=job_id,
                triggered_by="tester",
                request_id="review-request-2",
                expected_candidate_fingerprint="c" * 64,
            )
        )


def test_review_refresh_worker_and_reconcile_require_durable_generation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    artifacts, candidate_path = _write_review_refresh_candidate(project_root)
    job_id = "jato-review-durable-worker"
    candidate_fingerprint = (
        jato_monthly_update_service._candidate_fingerprint_id(artifacts)
    )
    operation_id = "jato-review_refresh-test"
    operation = {
        "operationId": operation_id,
        "type": "review_refresh",
        "status": "queued",
        "phase": "queued",
        "requestId": "review-request-durable",
        "requestedAt": "2026-07-21T00:00:00+00:00",
        "requestedBy": "tester",
        "expectedCandidateFingerprint": candidate_fingerprint,
        "expectedActiveFingerprint": "b" * 64,
        "candidateArtifactStatSignature": (
            jato_monthly_update_service._candidate_artifact_stat_signature(
                artifacts
            )
        ),
    }
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "activeBaseFingerprint": "b" * 64,
            "artifacts": artifacts,
            "pendingOperation": operation,
        }
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )
    candidate_before = candidate_path.read_bytes()

    def cache_review(
        requested_job_id: str,
        *,
        expected_candidate_fingerprint: str | None = None,
        expected_active_fingerprint: str | None = None,
        review_generation_id: str | None = None,
    ) -> Path:
        assert requested_job_id == job_id
        assert expected_candidate_fingerprint == candidate_fingerprint
        assert expected_active_fingerprint == "b" * 64
        latest = jato_monthly_update_service._load_job_state(job_id)
        latest_artifacts = latest["artifacts"]
        bundle_path = jato_monthly_update_service._job_review_bundle_path(
            job_id
        )
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        jato_monthly_update_service._write_json(
            bundle_path,
            {
                "reviewGenerationId": review_generation_id,
                "candidateFingerprint": candidate_fingerprint,
                "reviewBundleSchemaVersion": (
                    jato_monthly_update_service.REVIEW_BUNDLE_SCHEMA_VERSION
                ),
                "candidateArtifactStatSignatureVersion": (
                    jato_monthly_update_service
                    .CANDIDATE_ARTIFACT_STAT_SIGNATURE_VERSION
                ),
                "candidateArtifactStatSignature": (
                    jato_monthly_update_service
                    ._candidate_artifact_stat_signature(latest_artifacts)
                ),
            },
        )
        latest_artifacts["reviewBundlePath"] = (
            jato_monthly_update_service._relative_to_project(bundle_path)
        )
        latest["artifacts"] = latest_artifacts
        jato_monthly_update_service._persist_job_state(latest)
        return bundle_path

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_cache_jato_monthly_update_review",
        cache_review,
    )

    jato_monthly_update_service._run_active_bundle_operation(
        job_id=job_id,
        operation_type="review_refresh",
    )

    completed = jato_monthly_update_service._load_job_state(job_id)
    assert completed["status"] == "success"
    assert completed["phase"] == "completed"
    assert completed["pendingOperation"]["status"] == "success"
    assert completed["pendingOperation"]["phase"] == "completed"
    assert candidate_path.read_bytes() == candidate_before

    completed["artifacts"]["reviewBundlePath"] = "legacy/review_bundle.json"
    completed["pendingOperation"]["status"] = "running"
    completed["workerPid"] = 2_000_000_000
    jato_monthly_update_service._persist_job_state(completed)
    reconciled = (
        jato_monthly_update_service._reconcile_stale_monthly_update_jobs()
    )
    recovered = jato_monthly_update_service._load_job_state(job_id)
    assert reconciled == [job_id]
    assert recovered["pendingOperation"]["status"] == "success"
    assert recovered["pendingOperation"]["recoveredAfterWorkerLoss"] is True
    assert recovered["artifacts"]["reviewBundlePath"] == (
        jato_monthly_update_service._relative_to_project(
            jato_monthly_update_service._job_review_bundle_path(job_id)
        )
    )

    bundle_path = jato_monthly_update_service._job_review_bundle_path(job_id)
    bundle = jato_monthly_update_service._read_json(bundle_path)
    bundle["reviewGenerationId"] = "different-generation"
    jato_monthly_update_service._write_json(bundle_path, bundle)
    recovered["pendingOperation"]["status"] = "running"
    recovered["workerPid"] = 2_000_000_000
    jato_monthly_update_service._persist_job_state(recovered)
    jato_monthly_update_service._reconcile_stale_monthly_update_jobs()
    failed = jato_monthly_update_service._load_job_state(job_id)
    assert failed["status"] == "success"
    assert failed["phase"] == "completed"
    assert failed["pendingOperation"]["status"] == "failed"
    assert failed["pendingOperation"]["failureDigest"]["code"] == (
        "REVIEW_REFRESH_WORKER_LOST"
    )
    assert failed["pendingOperation"]["failureDigest"]["retryable"] is True


def test_review_refresh_worker_rejects_queue_snapshot_drift_before_hashing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    artifacts, candidate_path = _write_review_refresh_candidate(project_root)
    job_id = "jato-review-queued-stat-drift"
    queued_signature = (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "activeBaseFingerprint": "b" * 64,
            "artifacts": artifacts,
            "pendingOperation": {
                "operationId": "jato-review_refresh-stat-drift",
                "type": "review_refresh",
                "status": "queued",
                "phase": "queued",
                "requestedAt": "2026-07-21T00:00:00+00:00",
                "requestedBy": "tester",
                "expectedCandidateFingerprint": None,
                "expectedActiveFingerprint": "b" * 64,
                "candidateArtifactStatSignature": queued_signature,
            },
        }
    )
    candidate_path.write_bytes(b"candidate-drift-after-queue")
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_candidate_fingerprint_id",
        lambda _artifacts: (_ for _ in ()).throw(
            AssertionError("metadata drift must fail before content hashing")
        ),
    )

    jato_monthly_update_service._run_active_bundle_operation(
        job_id=job_id,
        operation_type="review_refresh",
    )

    failed = jato_monthly_update_service._load_job_state(job_id)
    assert failed["status"] == "success"
    assert failed["phase"] == "completed"
    assert failed["pendingOperation"]["status"] == "failed"
    assert failed["pendingOperation"]["failureDigest"]["technicalDetail"][
        "blockerType"
    ] == "candidate_metadata_changed"


def test_cached_legacy_review_exposes_server_normalized_allowed_decisions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-update-cached-legacy-history"
    legacy_countries = [
        {
            "country": "荷兰",
            "monthlyTotalsStable": False,
            "decisionRequired": False,
        }
    ]
    legacy_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            legacy_countries
        )
    )
    review_path = jato_monthly_update_service._job_review_bundle_path(
        job_id
    )
    review_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_path = project_root / "candidate.parquet"
    candidate_path.write_bytes(b"candidate")
    artifacts = {
        "stagingOutputPath": "candidate.parquet",
        "reviewBundlePath": (
            jato_monthly_update_service._relative_to_project(review_path)
        ),
    }
    jato_monthly_update_service._write_json(
        review_path,
        {
            "jobId": job_id,
            "reviewFindings": [],
            "candidateFingerprint": "a" * 64,
            "reviewBundleSchemaVersion": (
                jato_monthly_update_service.REVIEW_BUNDLE_SCHEMA_VERSION
            ),
            "candidateArtifactStatSignatureVersion": 2,
            "candidateArtifactStatSignature": (
                jato_monthly_update_service
                ._candidate_artifact_stat_signature(artifacts)
            ),
            "historicalReclassificationReport": {
                "status": "not_required",
                "countries": legacy_countries,
                "reportFingerprint": legacy_fingerprint,
            },
        },
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "artifacts": artifacts,
        }
    )

    review = jato_monthly_update_service.get_jato_monthly_update_review(
        job_id
    )

    normalized_report = review["historicalReclassificationReport"]
    assert normalized_report["status"] == "decision_required"
    assert normalized_report["countries"] == [
        {
            "country": "荷兰",
            "monthlyTotalsStable": False,
            "decisionRequired": True,
            "allowedDecisions": ["use_latest", "keep_active"],
        }
    ]
    assert normalized_report["reportFingerprint"] != legacy_fingerprint

    empty_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint([])
    )
    cached_bundle = jato_monthly_update_service._read_json(review_path)
    cached_bundle["historicalReclassificationReport"] = {
        "status": "not_required",
        "countries": [],
        "reportFingerprint": empty_fingerprint,
    }
    jato_monthly_update_service._write_json(review_path, cached_bundle)
    empty_review = (
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    )
    assert empty_review["historicalReclassificationReport"] == {
        "status": "not_required",
        "countries": [],
        "reportFingerprint": empty_fingerprint,
        "truncation": {},
    }

    cached_bundle["historicalReclassificationReport"][
        "reportFingerprint"
    ] = "invalid"
    jato_monthly_update_service._write_json(review_path, cached_bundle)
    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)
    assert (
        exc_info.value.detail["blockerType"]
        == "historical_reclassification_resolution_invalid"
    )


def test_upload_complete_digest_and_create_are_idempotent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    _write_country_partition(
        project_root,
        country="匈牙利",
        latest_month="2026 Apr",
        sales=10,
    )
    _write_country_partition(
        project_root,
        country="捷克",
        latest_month="2026 Apr",
        sales=20,
    )
    source_path = tmp_path / "JATO-Hungary-2026-05.xlsx"
    pd.DataFrame(
        {
            "国家": ["匈牙利"],
            "Model": ["T5 EVO"],
            "2026 Apr": [7],
            "2026 May": [9],
        }
    ).to_excel(source_path, index=False, sheet_name="Data Export")
    upload_bytes = source_path.read_bytes()
    upload_sha = hashlib.sha256(upload_bytes).hexdigest()
    monkeypatch.setattr(
        jato_monthly_update_service,
        "UPLOAD_CHUNK_SIZE_BYTES",
        len(upload_bytes),
    )
    launched_uploads: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_upload_digest_process",
        lambda upload_id: launched_uploads.append(upload_id),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        lambda _job_id: None,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-r1",
    )

    initiated = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename=source_path.name,
        size_bytes=len(upload_bytes),
        resume_key="hungary-2026-05",
        triggered_by="tester",
    )
    upload_id = initiated["uploadId"]
    uploaded = jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=1,
        content=upload_bytes,
        chunk_sha256=upload_sha,
        requested_by="tester",
        requested_role="editor",
    )
    assert uploaded["receivedChunkCount"] == 1

    completing_once = (
        jato_monthly_update_service.complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by="tester",
            requested_role="editor",
        )
    )
    completing_replay = (
        jato_monthly_update_service.complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by="tester",
            requested_role="editor",
        )
    )
    assert completing_once["status"] == "assembling"
    assert completing_replay["status"] == "assembling"
    assert launched_uploads == [upload_id]
    monkeypatch.setenv(
        "APP_JATO_DIGEST_ATTEMPT_ID",
        jato_monthly_update_service._load_upload_session(upload_id)[
            "digestAttempt"
        ]["attemptId"],
    )

    digested_once = (
        jato_monthly_update_service.run_jato_monthly_update_upload_digest(
            upload_id
        )
    )
    digested_replay = (
        jato_monthly_update_service.run_jato_monthly_update_upload_digest(
            upload_id
        )
    )
    assert digested_once["status"] == "ready"
    assert digested_replay["status"] == "ready"
    assert digested_replay["fileSha256"] == upload_sha
    assert digested_replay["ingestDigest"]["route"] == "single_country"
    assert digested_replay["ingestDigest"]["activeLatestMonths"] == {
        "匈牙利": "2026-04"
    }
    assert digested_replay["ingestDigest"]["countryLatestMonths"] == {
        "匈牙利": "2026-05"
    }
    assert (
        jato_monthly_update_service.complete_jato_monthly_update_upload(
            upload_id=upload_id,
            requested_by="tester",
            requested_role="editor",
        )["status"]
        == "ready"
    )

    created_once = (
        jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=upload_id,
            triggered_by="tester",
        )
    )
    created_replay = (
        jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=upload_id,
            triggered_by="tester",
        )
    )
    assert created_replay["jobId"] == created_once["jobId"]
    assert created_replay["ingestionKey"] == created_once["ingestionKey"]
    assert created_once["month"] == "2026-05"
    assert created_once["countryScope"] == ["匈牙利"]
    assert (
        jato_monthly_update_service.get_jato_monthly_update_upload(
            upload_id,
            requested_by="tester",
            requested_role="editor",
        )[
            "consumedJobId"
        ]
        == created_once["jobId"]
    )
    persisted_jobs = list(job_root.glob("jato-update-*/job_state.json"))
    assert persisted_jobs == [
        job_root / created_once["jobId"] / "job_state.json"
    ]


def test_single_country_smart_merge_builds_full_canonical_candidate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    processed_root = project_root / "04_Processed_data"
    active_path = processed_root / "jato_full_archive.parquet"
    active_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Country": "Hungary",
                "Make": "BMW",
                "Model": "iX1",
                "2026 Apr": 10,
            },
            {
                "Country": "Germany",
                "Make": "VW",
                "Model": "ID.4",
                "2026 Apr": 20,
            },
        ]
    ).to_parquet(active_path, index=False)

    job_id = "jato-smart-merge-single-country"
    candidate_path = (
        processed_root
        / "staging"
        / "2026-05-r1"
        / "jato_full_archive.parquet"
    )
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Country": " hungary ",
                "Make": "BMW",
                "Model": "iX1",
                "2026 Apr": 10,
                "2026 May": 12,
            }
        ]
    ).to_parquet(candidate_path, index=False)
    upload_path = job_root / job_id / "uploads" / "hungary.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"upload")
    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=job_id,
        month="2026-05",
        triggered_by="tester",
        upload_filename=upload_path.name,
        stored_upload_path=upload_path,
    )
    state["status"] = "success"
    state["phase"] = "completed"
    state["activeBaseFingerprint"] = (
        jato_monthly_update_service._active_dataset_version()
    )
    state["reviewApproval"] = {
        "decision": "approved",
        "candidateFingerprint": "old",
    }
    state["artifacts"].update(
        {
            "stagingOutputPath": (
                "04_Processed_data/staging/2026-05-r1/"
                "jato_full_archive.parquet"
            ),
            "candidateScope": "target_country_partition_only",
        }
    )
    jato_monthly_update_service._persist_job_state(state)
    old_review_path = (
        jato_monthly_update_service._job_review_bundle_path(job_id)
    )
    old_review_path.write_text("old review", encoding="utf-8")
    source_candidate_sha = (
        jato_monthly_update_service._sha256_hex_for_path(candidate_path)
    )

    commands: list[str] = []

    def fake_command(*, label, args, log_path, job_id=None):
        del log_path, job_id
        commands.append(label)
        if label == "Smart Merge rebuild":
            partition_output = Path(
                args[args.index("--partition-output") + 1]
            )
            partition_output.mkdir(parents=True, exist_ok=True)
            (partition_output / "manifest.json").write_text(
                '{"parquetFileCount": 2, "partitionDirectoryCount": 2}',
                encoding="utf-8",
            )
        if label == "Smart Merge summaries":
            output_dir = Path(args[args.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)

    validation_calls: list[dict[str, Path]] = []
    cached_jobs: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_logged_command",
        fake_command,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_validate_candidate_full_bundle",
        lambda **paths: validation_calls.append(paths),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_cache_jato_monthly_update_review",
        lambda cached_job_id, **_kwargs: (
            cached_jobs.append(cached_job_id)
            or jato_monthly_update_service._job_review_bundle_path(
                cached_job_id
            )
        ),
    )

    jato_monthly_update_service._run_smart_merge(job_id)

    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert persisted["status"] == "success"
    assert persisted["phase"] == "completed"
    assert persisted["reviewApproval"] is None
    assert persisted["artifacts"]["candidateScope"] == "full_smart_merge"
    assert commands == ["Smart Merge rebuild", "Smart Merge summaries"]
    assert len(validation_calls) == 1
    assert cached_jobs == [job_id]
    assert (
        jato_monthly_update_service._sha256_hex_for_path(candidate_path)
        == source_candidate_sha
    )
    merged_path = project_root / persisted["artifacts"][
        "stagingOutputPath"
    ]
    assert merged_path != candidate_path
    merged = pd.read_parquet(merged_path)
    assert sorted(merged["Country"].tolist()) == ["Germany", "Hungary"]
    assert len(merged.loc[merged["Country"] == "Hungary"]) == 1
    assert (
        merged.loc[
            merged["Country"] == "Hungary",
            "2026 May",
        ].iloc[0]
        == 12
    )


def test_full_candidate_resolution_forces_smart_merge_without_regression(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    processed_root = project_root / "04_Processed_data"
    active_path = processed_root / "jato_full_archive.parquet"
    active_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "2026 Jan": 10,
            },
            {
                "Country": "Germany",
                "Make": "OTHER",
                "Model": "STABLE",
                "2026 Jan": 20,
            },
        ]
    ).to_parquet(active_path, index=False)

    job_id = "jato-full-resolution-no-regression"
    staging_dir = processed_root / "staging" / "2026-02-r1"
    staging_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = staging_dir / "jato_full_archive.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "2026 Jan": 10,
                "2026 Feb": 12,
            },
            {
                "Country": "Germany",
                "Make": "OTHER",
                "Model": "STABLE",
                "2026 Jan": 20,
                "2026 Feb": 0,
            },
        ]
    ).to_parquet(candidate_path, index=False)
    country_reports = [
        {
            "country": "Czechia",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    candidate_fingerprint = "b" * 64
    state = {
        "jobId": job_id,
        "status": "queued",
        "phase": "queued",
        "activeBaseFingerprint": (
            jato_monthly_update_service._active_dataset_version()
        ),
        "artifacts": {
            "stagingOutputPath": (
                "04_Processed_data/staging/2026-02-r1/"
                "jato_full_archive.parquet"
            ),
            "candidateScope": "full_candidate",
        },
        "historicalReclassificationResolution": {
            "status": "queued",
            "sourceCandidateFingerprint": candidate_fingerprint,
            "reportFingerprint": report_fingerprint,
            "decisions": [
                {"country": "Czechia", "decision": "keep_active"}
            ],
            "report": {
                "status": "decision_required",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
            },
        },
    }
    jato_monthly_update_service._persist_job_state(state)

    merge_calls: list[dict[str, str]] = []
    commands: list[str] = []

    def fake_merge(**kwargs):
        merge_calls.append(
            kwargs["historical_reclassification_decisions"]
        )
        shutil.copy2(kwargs["candidate_path"], kwargs["output_path"])
        return 2, {"enabled": False, "columnResults": {}}

    def fake_command(*, label, args, log_path, job_id=None):
        del log_path, job_id
        commands.append(label)
        if label == "Smart Merge rebuild":
            partition_output = Path(
                args[args.index("--partition-output") + 1]
            )
            partition_output.mkdir(parents=True, exist_ok=True)
            (partition_output / "manifest.json").write_text(
                '{"parquetFileCount": 2, "partitionDirectoryCount": 2}',
                encoding="utf-8",
            )
        elif label == "Smart Merge summaries":
            output_dir = Path(args[args.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_candidate_fingerprint_id",
        lambda _artifacts: candidate_fingerprint,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_smart_merge_parquet_streaming",
        fake_merge,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_logged_command",
        fake_command,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_validate_candidate_full_bundle",
        lambda **_paths: None,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_cache_jato_monthly_update_review",
        lambda cached_job_id, **_kwargs: (
            jato_monthly_update_service._job_review_bundle_path(
                cached_job_id
            )
        ),
    )

    jato_monthly_update_service._run_smart_merge(job_id)

    persisted = jato_monthly_update_service._load_job_state(job_id)
    assert merge_calls == [{"czechia": "keep_active"}]
    assert commands == ["Smart Merge rebuild", "Smart Merge summaries"]
    assert persisted["status"] == "success"
    assert persisted["phase"] == "completed"
    assert persisted["artifacts"]["candidateScope"] == "full_smart_merge"
    assert (
        persisted["historicalReclassificationResolution"]["status"]
        == "resolved"
    )
    assert (
        persisted["historicalReclassificationResolution"][
            "resolvedCandidateFingerprint"
        ]
        == candidate_fingerprint
    )


def test_historical_reclassification_report_uses_single_dimension_totals() -> None:
    active = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL A",
                "Body type": "Sedan",
                "Version name": "Old",
                "2026 Jan": 10,
            },
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "OLD MODEL",
                "Body type": "Hatchback",
                "Version name": "Stable",
                "2026 Jan": 5,
            },
        ]
    )
    candidate = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL A",
                "Body type": "SUV",
                "Version name": "New",
                "2026 Jan": 10,
                "2026 Feb": 2,
            },
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "NEW MODEL",
                "Body type": "Hatchback",
                "Version name": "Stable",
                "2026 Jan": 5,
                "2026 Feb": 0,
            },
        ]
    )

    stability = (
        jato_monthly_update_service._single_country_historical_sales_stability(
            country="捷克",
            active_frame=active,
            candidate_frame=candidate,
            active_latest_month="2026 Jan",
        )
    )
    report = stability["historicalReclassification"]
    assert report["monthlyTotalsStable"] is True
    assert report["decisionRequired"] is True
    body = next(
        item
        for item in report["dimensionSummaries"]
        if item["dimension"] == "Body type"
    )
    assert body["mismatchCellCount"] == 2
    assert body["movedSales"] == 10
    assert body["oldValues"] == [
        {"value": "Sedan", "sales": 10, "monthCount": 1}
    ]
    assert body["newValues"] == [
        {"value": "SUV", "sales": 10, "monthCount": 1}
    ]
    assert report["exactChanges"][0]["transferredSales"] == 10


def test_historical_sales_change_requires_explicit_history_decision() -> None:
    active = pd.DataFrame(
        [
            {
                "国家": "荷兰",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "Sedan",
                "2026 Jan": 10,
            }
        ]
    )
    candidate = pd.DataFrame(
        [
            {
                "国家": "荷兰",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "SUV",
                "2026 Jan": 11,
                "2026 Feb": 2,
            }
        ]
    )

    stability = (
        jato_monthly_update_service
        ._single_country_historical_sales_stability(
            country="荷兰",
            active_frame=active,
            candidate_frame=candidate,
            active_latest_month="2026 Jan",
        )
    )

    assert stability["status"] == "fail"
    assert stability["reason"] == "historical_sales_changed"
    report = stability["historicalReclassification"]
    assert report["monthlyTotalsStable"] is False
    assert report["decisionRequired"] is True
    assert report["allowedDecisions"] == ["use_latest", "keep_active"]


def test_keep_active_history_slices_months_without_accumulation() -> None:
    active = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "Old label",
                "2026 Jan": 10,
            }
        ]
    )
    candidate = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "New label",
                "2026 Jan": 10,
                "2026 Feb": 12,
            }
        ]
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        merged, summary = (
            jato_monthly_update_service._keep_active_history_country_frame(
                active_frame=active,
                candidate_frame=candidate,
            )
        )

    assert summary["monthBoundaryCheck"] == "pass"
    assert not any(
        issubclass(item.category, FutureWarning)
        for item in caught
    )
    assert merged["2026 Jan"].sum() == 10
    assert merged["2026 Feb"].sum() == 12
    assert (
        merged.loc[
            merged["Version name"] == "Old label",
            "2026 Feb",
        ].sum()
        == 0
    )
    assert (
        merged.loc[
            merged["Version name"] == "New label",
            "2026 Jan",
        ].sum()
        == 0
    )


def test_keep_active_rejects_normalized_duplicates_before_sum() -> None:
    active = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "Old",
                "2026 Jan": 10,
            }
        ]
    )
    candidate = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "New",
                "2026 Jan": 10,
                "2026 Feb": 5,
            },
            {
                "国家": " 捷克 ",
                "Make": " brand ",
                "Model": "model",
                "Version name": " new ",
                "2026 Jan": 0,
                "2026 Feb": 7,
            },
        ]
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service._keep_active_history_country_frame(
            active_frame=active,
            candidate_frame=candidate,
        )

    assert exc_info.value.detail["blockerType"] == "duplicate_configurations"
    assert exc_info.value.detail["source"] == "candidate_future"
    assert exc_info.value.detail["duplicateRows"] == 2


def test_keep_active_rejects_duplicate_active_source() -> None:
    active = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "Old",
                "2026 Jan": 6,
            },
            {
                "国家": " 捷克 ",
                "Make": "brand",
                "Model": " model ",
                "Version name": " old ",
                "2026 Jan": 4,
            },
        ]
    )
    candidate = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "New",
                "2026 Jan": 10,
                "2026 Feb": 12,
            }
        ]
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service._keep_active_history_country_frame(
            active_frame=active,
            candidate_frame=candidate,
        )

    assert exc_info.value.detail["source"] == "active"
    assert exc_info.value.detail["duplicateGroupCount"] == 1


def test_historical_reclassification_resolution_rejects_other_blockers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-blocked"
    state = {
        "jobId": job_id,
        "artifacts": {},
        "activeBaseFingerprint": "a" * 64,
    }
    jato_monthly_update_service._persist_job_state(state)
    country_reports = [
        {
            "country": "捷克",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "candidateFingerprint": "b" * 64,
            "reviewFindings": [
                {"severity": "blocker", "ruleId": "SC005"}
            ],
            "historicalReclassificationReport": {
                "status": "decision_required",
                "reportFingerprint": report_fingerprint,
                "countries": country_reports,
            },
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.resolve_jato_historical_reclassification(
            job_id=job_id,
            triggered_by="admin",
            decisions=[
                {"country": "捷克", "decision": "use_latest"}
            ],
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "review_blockers_present"
    assert exc_info.value.detail["rules"] == ["SC005"]


def test_historical_reclassification_resolution_requires_exact_country_scope(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-scope"
    state = {
        "jobId": job_id,
        "artifacts": {},
        "activeBaseFingerprint": "a" * 64,
    }
    jato_monthly_update_service._persist_job_state(state)
    country_reports = [
        {
            "country": "捷克",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        },
        {
            "country": "丹麦",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        },
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "candidateFingerprint": "b" * 64,
            "reviewFindings": [],
            "historicalReclassificationReport": {
                "status": "decision_required",
                "reportFingerprint": report_fingerprint,
                "countries": country_reports,
            },
        },
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "a" * 64,
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.resolve_jato_historical_reclassification(
            job_id=job_id,
            triggered_by="admin",
            decisions=[
                {"country": " 捷克 ", "decision": "use_latest"}
            ],
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["missingCountries"] == ["丹麦"]


def test_legacy_historical_sales_blocker_normalizes_and_queues_keep_active(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-legacy-sales-change"
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "activeBaseFingerprint": "a" * 64,
            "artifacts": {},
        }
    )
    legacy_countries = [
        {
            "country": "捷克",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        },
        {
            "country": "荷兰",
            "monthlyTotalsStable": False,
            "decisionRequired": False,
            "jointMismatchCellCount": 1,
        },
    ]
    legacy_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            legacy_countries
        )
    )
    review = {
        "candidateFingerprint": "b" * 64,
        "reviewFindings": [
            {
                "severity": "blocker",
                "scope": "country",
                "ruleId": "SC011",
                "target": " 荷兰 ",
                "metrics": {
                    "reason": "historical_sales_changed",
                    "countryMismatchCount": 1,
                },
            }
        ],
        "historicalReclassificationReport": {
            "status": "decision_required",
            "countries": legacy_countries,
            "reportFingerprint": legacy_fingerprint,
            "truncation": {"truncated": False},
        },
    }
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: review,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "a" * 64,
    )
    queued: dict[str, object] = {}

    def fake_queue(*, job_id: str, triggered_by: str) -> dict[str, object]:
        queued["jobId"] = job_id
        queued["triggeredBy"] = triggered_by
        return {"jobId": job_id, "status": "queued"}

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_create_smart_merge_candidate_locked",
        fake_queue,
    )

    result = (
        jato_monthly_update_service
        ._resolve_jato_historical_reclassification_with_job_lock(
            job_id=job_id,
            triggered_by="admin",
            decisions=[
                {"country": "捷克", "decision": "keep_active"},
                {"country": "荷兰", "decision": "keep_active"},
            ],
        )
    )

    assert result["status"] == "queued"
    assert queued == {"jobId": job_id, "triggeredBy": "admin"}
    resolution = (
        jato_monthly_update_service._load_job_state(job_id)[
            "historicalReclassificationResolution"
        ]
    )
    normalized_countries = resolution["report"]["countries"]
    netherlands = next(
        item
        for item in normalized_countries
        if item["country"] == "荷兰"
    )
    assert netherlands["decisionRequired"] is True
    assert netherlands["allowedDecisions"] == [
        "use_latest",
        "keep_active",
    ]
    assert resolution["decisions"] == [
        {"country": "捷克", "decision": "keep_active"},
        {"country": "荷兰", "decision": "keep_active"},
    ]
    assert resolution["reportFingerprint"] == (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            normalized_countries
        )
    )
    assert (
        resolution["report"]["reportFingerprint"]
        == resolution["reportFingerprint"]
    )


def test_historical_sales_change_use_latest_is_queued_at_endpoint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-sales-change-use-latest"
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "activeBaseFingerprint": "a" * 64,
            "artifacts": {},
        }
    )
    legacy_countries = [
        {
            "country": "瑞士",
            "monthlyTotalsStable": False,
            "decisionRequired": False,
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            legacy_countries
        )
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "candidateFingerprint": "b" * 64,
            "reviewFindings": [
                {
                    "severity": "blocker",
                    "scope": "country",
                    "ruleId": "SC011",
                    "target": "瑞士",
                    "metrics": {
                        "blockerType": "historical_sales_changed",
                        "countryMismatchCount": 2,
                    },
                }
            ],
            "historicalReclassificationReport": {
                "status": "not_required",
                "countries": legacy_countries,
                "reportFingerprint": report_fingerprint,
            },
        },
    )

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "a" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_create_smart_merge_candidate_locked",
        lambda *, job_id, triggered_by: {
            "jobId": job_id,
            "triggeredBy": triggered_by,
            "status": "queued",
        },
    )

    result = (
        jato_monthly_update_service
        ._resolve_jato_historical_reclassification_with_job_lock(
            job_id=job_id,
            triggered_by="admin",
            decisions=[
                {"country": "瑞士", "decision": "use_latest"}
            ],
        )
    )

    assert result["status"] == "queued"
    resolution = (
        jato_monthly_update_service
        ._historical_reclassification_resolution(
            jato_monthly_update_service._load_job_state(job_id)
        )
    )
    assert resolution is not None
    assert resolution["activeBaseFingerprint"] == "a" * 64
    assert resolution["sourceCandidateFingerprint"] == "b" * 64
    assert resolution["reportFingerprint"] == (
        resolution["report"]["reportFingerprint"]
    )
    assert resolution["decisions"] == [
        {"country": "瑞士", "decision": "use_latest"}
    ]


def test_persisted_historical_sales_change_use_latest_is_validated() -> None:
    country_reports = [
        {
            "country": "瑞士",
            "monthlyTotalsStable": False,
            "decisionRequired": True,
            "allowedDecisions": ["use_latest", "keep_active"],
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    resolution = {
        "status": "queued",
        "reportFingerprint": report_fingerprint,
        "decisions": [
            {"country": "瑞士", "decision": "use_latest"}
        ],
        "report": {
            "status": "decision_required",
            "countries": country_reports,
            "reportFingerprint": report_fingerprint,
        },
    }

    assert (
        jato_monthly_update_service
        ._validated_historical_reclassification_resolution(resolution)
        == {"瑞士".casefold(): "use_latest"}
    )


def test_sc011_non_sales_blocker_is_not_resolved_by_keep_active(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-other-sc011"
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "activeBaseFingerprint": "a" * 64,
            "artifacts": {},
        }
    )
    country_reports = [
        {
            "country": "荷兰",
            "monthlyTotalsStable": False,
            "decisionRequired": True,
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "candidateFingerprint": "b" * 64,
            "reviewFindings": [
                {
                    "severity": "blocker",
                    "scope": "country",
                    "ruleId": "SC011",
                    "target": "荷兰",
                    "metrics": {
                        "reason": (
                            "historical_configuration_guard_unavailable"
                        ),
                        "countryMismatchCount": 1,
                    },
                }
            ],
            "historicalReclassificationReport": {
                "status": "decision_required",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
            },
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        (
            jato_monthly_update_service
            ._resolve_jato_historical_reclassification_with_job_lock(
                job_id=job_id,
                triggered_by="admin",
                decisions=[
                    {"country": "荷兰", "decision": "keep_active"}
                ],
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "review_blockers_present"
    assert exc_info.value.detail["rules"] == ["SC011"]


def test_historical_sales_blocker_requires_country_scope() -> None:
    finding = {
        "severity": "blocker",
        "scope": "dataset",
        "ruleId": "SC011",
        "target": "荷兰",
        "metrics": {
            "reason": "historical_sales_changed",
            "countryMismatchCount": 1,
        },
    }

    assert (
        jato_monthly_update_service
        ._historical_sales_changed_blocker_country_key(finding)
        is None
    )


def test_resolved_report_preserves_affected_country_and_rejects_tampering() -> None:
    country_report = {
        "country": "捷克",
        "monthlyTotalsStable": True,
        "decisionRequired": True,
        "jointMismatchCellCount": 2,
    }
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            [country_report]
        )
    )
    resolution = {
        "status": "resolved",
        "reportFingerprint": report_fingerprint,
        "decisions": [
            {"country": "捷克", "decision": "use_latest"}
        ],
        "report": {
            "status": "decision_required",
            "countries": [country_report],
            "reportFingerprint": report_fingerprint,
        },
    }
    report = (
        jato_monthly_update_service
        ._build_historical_reclassification_report(
            payload={
                "historicalReclassificationResolution": resolution
            },
            current_countries=[],
        )
    )
    assert report["status"] == "resolved"
    assert report["countries"][0]["decisionRequired"] is True
    assert report["countries"][0]["decision"] == "use_latest"

    tampered_resolution = {
        **resolution,
        "report": {
            **resolution["report"],
            "countries": [
                {**country_report, "jointMismatchCellCount": 999}
            ],
        },
    }
    with pytest.raises(HTTPException) as exc_info:
        (
            jato_monthly_update_service
            ._build_historical_reclassification_report(
                payload={
                    "historicalReclassificationResolution": (
                        tampered_resolution
                    )
                },
                current_countries=[],
            )
        )
    assert (
        exc_info.value.detail["blockerType"]
        == "historical_reclassification_resolution_invalid"
    )


def test_approval_rejects_resolution_decisions_missing_affected_country(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-missing-decision"
    country_reports = [
        {
            "country": "捷克",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        },
        {
            "country": "丹麦",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        },
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    state = {
        "jobId": job_id,
        "status": "success",
        "phase": "completed",
        "artifacts": {},
        "activeBaseFingerprint": "a" * 64,
        "historicalReclassificationResolution": {
            "status": "resolved",
            "reportFingerprint": report_fingerprint,
            "resolvedCandidateFingerprint": "b" * 64,
            "decisions": [
                {"country": "捷克", "decision": "use_latest"}
            ],
            "report": {
                "status": "decision_required",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
            },
        },
    }
    jato_monthly_update_service._persist_job_state(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "a" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "candidateFingerprint": "b" * 64,
            "reviewFindings": [],
            "historicalReclassificationReport": {
                "status": "resolved",
                "reportFingerprint": report_fingerprint,
                "countries": country_reports,
            },
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.approve_jato_monthly_update_review(
            job_id=job_id,
            triggered_by="admin",
            decision="approve",
        )

    assert (
        exc_info.value.detail["blockerType"]
        == "historical_reclassification_resolution_invalid"
    )
    assert exc_info.value.detail["missingCountries"] == ["丹麦"]


def test_publish_configuration_guard_only_allows_bound_use_latest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    project_root.mkdir(parents=True, exist_ok=True)
    active_path = project_root / "active.parquet"
    candidate_path = project_root / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "Sedan",
                "2026 Jan": 10,
            }
        ]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "SUV",
                "2026 Jan": 10,
                "2026 Feb": 2,
            }
        ]
    ).to_parquet(candidate_path, index=False)

    blocked = (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
    )
    allowed = (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            approved_reclassification_decisions={
                " cZeChIa ": "use_latest"
            },
        )
    )

    assert len(blocked) == 1
    assert allowed == []
    assert (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            approved_reclassification_decisions={
                "Czechia": "keep_active"
            },
        )
        == blocked
    )

    changed_total = pd.read_parquet(candidate_path)
    changed_total.loc[:, "2026 Jan"] = 11
    changed_total.to_parquet(candidate_path, index=False)
    historical_sales_changes = (
        jato_monthly_update_service._find_publish_historical_sales_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
    )
    assert historical_sales_changes != []
    assert historical_sales_changes[0]["monthChanges"] == [
        {
            "month": "2026 Jan",
            "activeSales": 10.0,
            "candidateSales": 11.0,
            "deltaSales": 1,
        }
    ]
    blocking, authorized = (
        jato_monthly_update_service
        ._partition_publish_historical_sales_changes(
            changes=historical_sales_changes,
            approved_sales_overwrite_countries={"czechia"},
        )
    )
    assert blocking == []
    assert authorized == historical_sales_changes
    assert (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            approved_reclassification_decisions={
                "Czechia": "use_latest"
            },
        )
        != []
    )
    assert (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            approved_reclassification_decisions={
                "Czechia": "use_latest"
            },
            approved_sales_overwrite_countries={"czechia"},
        )
        == []
    )

    changed_total.drop(columns=["2026 Jan"]).to_parquet(
        candidate_path,
        index=False,
    )
    missing_month_changes = (
        jato_monthly_update_service._find_publish_historical_sales_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
    )
    blocking, authorized = (
        jato_monthly_update_service
        ._partition_publish_historical_sales_changes(
            changes=missing_month_changes,
            approved_sales_overwrite_countries={"czechia"},
        )
    )
    assert authorized == []
    assert blocking[0]["missingCandidateMonths"] == ["2026 Jan"]


def test_publish_context_only_authorizes_unstable_use_latest_sales() -> None:
    candidate_fingerprint = "b" * 64
    decisions, audit, sales_overwrite_countries = (
        jato_monthly_update_service
        ._approved_historical_reclassification_publish_context(
            {
                "candidateFingerprint": candidate_fingerprint,
                "historicalReclassification": {
                    "resolvedCandidateFingerprint": candidate_fingerprint,
                    "decisions": [
                        {
                            "country": "Czechia",
                            "decision": "use_latest",
                            "monthlyTotalsStable": True,
                        },
                        {
                            "country": "Denmark",
                            "decision": "use_latest",
                            "monthlyTotalsStable": False,
                        },
                        {
                            "country": "Austria",
                            "decision": "keep_active",
                            "monthlyTotalsStable": False,
                        },
                    ],
                },
            }
        )
    )

    assert decisions == {
        "czechia": "use_latest",
        "denmark": "use_latest",
        "austria": "keep_active",
    }
    assert [item["country"] for item in audit] == [
        "Czechia",
        "Denmark",
        "Austria",
    ]
    assert sales_overwrite_countries == {"denmark"}
    assert (
        jato_monthly_update_service
        ._approved_historical_reclassification_publish_context(
            {
                "candidateFingerprint": candidate_fingerprint,
                "historicalReclassification": {
                    "resolvedCandidateFingerprint": "c" * 64,
                    "decisions": [
                        {
                            "country": "Denmark",
                            "decision": "use_latest",
                            "monthlyTotalsStable": False,
                        }
                    ],
                },
            }
        )
        == ({}, [], set())
    )


def test_confirmed_reclassification_requires_bound_use_latest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_collect_dataset_country_latest_months",
        lambda _path: {"Czechia": "2026 Jan"},
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_load_country_configuration_history_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            [{"Country": "Czechia", "2026 Jan": 10}]
        ),
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_single_country_historical_sales_stability",
        lambda **_kwargs: {
            "status": "confirmed",
            "reason": "confirmed_make_model_reclassification",
            "comparedThrough": "2026 Jan",
            "mismatchCount": 2,
            "countryMismatchCount": 0,
            "makeModelMismatchCount": 2,
            "analysisDimensionMismatchCount": 0,
            "comparedAnalysisDimensions": [],
            "impactedMakeModels": [],
            "mismatchSamples": [],
            "unconfirmedReclassificationCandidates": [],
        },
    )

    blocked = (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
    )
    allowed = (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            approved_reclassification_decisions={
                " cZeChIa ": "use_latest"
            },
        )
    )

    assert len(blocked) == 1
    assert (
        blocked[0]["reason"]
        == "confirmed_make_model_reclassification"
    )
    assert allowed == []


def test_partial_candidate_scope_must_exactly_match_bound_countries(
    tmp_path: Path,
) -> None:
    candidate_path = tmp_path / "partial.parquet"
    pd.DataFrame(
        [
            {"Country": "Czechia", "2026 Jun": 1},
            {"Country": "Denmark", "2026 Jun": 2},
        ]
    ).to_parquet(candidate_path, index=False)
    assert (
        jato_monthly_update_service
        ._validate_candidate_logical_country_scope(
            candidate_path=candidate_path,
            expected_countries=[" cZeChIa ", "DENMARK"],
        )
        == ["Czechia", "Denmark"]
    )

    pd.DataFrame(
        [
            {"Country": "Czechia", "2026 Jun": 1},
            {"Country": "Denmark", "2026 Jun": 2},
            {"Country": "Germany", "2026 Jun": 3},
        ]
    ).to_parquet(candidate_path, index=False)
    with pytest.raises(RuntimeError, match="extra=Germany"):
        (
            jato_monthly_update_service
            ._validate_candidate_logical_country_scope(
                candidate_path=candidate_path,
                expected_countries=["Czechia", "Denmark"],
            )
        )

    pd.DataFrame(
        [{"Country": "Czechia", "2026 Jun": 1}]
    ).to_parquet(candidate_path, index=False)
    with pytest.raises(RuntimeError, match="missing=Denmark"):
        (
            jato_monthly_update_service
            ._validate_candidate_logical_country_scope(
                candidate_path=candidate_path,
                expected_countries=["Czechia", "Denmark"],
            )
        )


def test_approval_binds_resolved_decisions_and_rejects_stale_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-approval"
    country_reports = [
        {
            "country": "捷克",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    state = {
        "jobId": job_id,
        "status": "success",
        "phase": "completed",
        "artifacts": {},
        "activeBaseFingerprint": "a" * 64,
        "historicalReclassificationResolution": {
            "status": "resolved",
            "reportFingerprint": report_fingerprint,
            "resolvedCandidateFingerprint": "stale",
            "decisions": [
                {"country": "捷克", "decision": "use_latest"}
            ],
            "report": {
                "status": "decision_required",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
            },
        },
    }
    jato_monthly_update_service._persist_job_state(state)
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "a" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "candidateFingerprint": "b" * 64,
            "reviewFindings": [],
            "historicalReclassificationReport": {
                "status": "resolved",
                "reportFingerprint": report_fingerprint,
                "countries": country_reports,
            },
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.approve_jato_monthly_update_review(
            job_id=job_id,
            triggered_by="admin",
            decision="approve",
        )
    assert (
        exc_info.value.detail["blockerType"]
        == "historical_reclassification_resolution_stale"
    )

    state = jato_monthly_update_service._load_job_state(job_id)
    state["historicalReclassificationResolution"][
        "resolvedCandidateFingerprint"
    ] = "b" * 64
    jato_monthly_update_service._persist_job_state(state)
    approved = (
        jato_monthly_update_service.approve_jato_monthly_update_review(
            job_id=job_id,
            triggered_by="admin",
            decision="approve",
        )
    )
    bound = approved["reviewApproval"]["historicalReclassification"]
    assert bound["reportFingerprint"] == report_fingerprint
    assert bound["resolvedCandidateFingerprint"] == "b" * 64
    assert bound["decisions"] == [
        {
            "country": "捷克",
            "decision": "use_latest",
            "monthlyTotalsStable": True,
        }
    ]


def test_approval_requires_exact_passed_keep_active_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-resolution-keep-active-approval"
    country_reports = [
        {
            "country": "荷兰",
            "monthlyTotalsStable": False,
            "decisionRequired": True,
            "allowedDecisions": ["keep_active"],
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "artifacts": {},
            "activeBaseFingerprint": "a" * 64,
            "historicalReclassificationResolution": {
                "status": "resolved",
                "reportFingerprint": report_fingerprint,
                "resolvedCandidateFingerprint": "b" * 64,
                "decisions": [
                    {"country": "荷兰", "decision": "keep_active"}
                ],
                "report": {
                    "status": "decision_required",
                    "countries": country_reports,
                    "reportFingerprint": report_fingerprint,
                },
            },
        }
    )
    review_report: dict[str, object] = {
        "status": "resolved",
        "reportFingerprint": report_fingerprint,
        "countries": country_reports,
    }
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "a" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "get_jato_monthly_update_review",
        lambda _job_id: {
            "candidateFingerprint": "b" * 64,
            "reviewFindings": [],
            "historicalReclassificationReport": review_report,
        },
    )
    invalid_validations: list[object] = [
        None,
        [
            {
                "country": "荷兰",
                "decision": "keep_active",
                "status": "fail",
            }
        ],
        [
            {
                "country": "荷兰",
                "decision": "keep_active",
                "status": "pass",
            },
            {
                "country": "荷兰",
                "decision": "keep_active",
                "status": "pass",
            },
        ],
        [
            {
                "country": "荷兰",
                "decision": "keep_active",
                "status": "pass",
            },
            {
                "country": "德国",
                "decision": "keep_active",
                "status": "pass",
            },
        ],
    ]
    for validation in invalid_validations:
        if validation is None:
            review_report.pop("resolutionValidation", None)
        else:
            review_report["resolutionValidation"] = validation
        with pytest.raises(HTTPException) as exc_info:
            jato_monthly_update_service.approve_jato_monthly_update_review(
                job_id=job_id,
                triggered_by="admin",
                decision="approve",
            )
        assert (
            exc_info.value.detail["blockerType"]
            == "historical_keep_active_validation_failed"
        )

    review_report["resolutionValidation"] = [
        {
            "country": "荷兰",
            "decision": "keep_active",
            "status": "pass",
        }
    ]
    approved = jato_monthly_update_service.approve_jato_monthly_update_review(
        job_id=job_id,
        triggered_by="admin",
        decision="approve",
    )
    assert (
        approved["reviewApproval"]["historicalReclassification"][
            "decisions"
        ]
        == [
            {
                "country": "荷兰",
                "decision": "keep_active",
                "monthlyTotalsStable": False,
            }
        ]
    )


def test_streaming_smart_merge_applies_keep_active_and_proves_untouched(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "Old",
                "2026 Jan": 10,
            },
            {
                "Country": "Germany",
                "Make": "OTHER",
                "Model": "UNCHANGED",
                "Version name": "Stable",
                "2026 Jan": 20,
            },
        ]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "New",
                "New dimension": "latest",
                "2026 Jan": 99,
                "2026 Feb": 12,
            }
        ]
    ).to_parquet(candidate_path, index=False)

    _rows, summary = (
        jato_monthly_update_service._smart_merge_parquet_streaming(
            active_path=active_path,
            candidate_path=candidate_path,
            regressed_countries=[],
            historical_reclassification_decisions={
                "Czechia": "keep_active"
            },
        )
    )
    merged = pd.read_parquet(candidate_path)
    czechia = merged.loc[merged["Country"] == "Czechia"]
    germany = merged.loc[merged["Country"] == "Germany"]
    assert czechia["2026 Jan"].sum() == 10
    assert czechia["2026 Feb"].sum() == 12
    assert (
        czechia.loc[
            czechia["Version name"] == "Old",
            "2026 Jan",
        ].sum()
        == 10
    )
    assert (
        czechia.loc[
            czechia["Version name"] == "New",
            "2026 Feb",
        ].sum()
        == 12
    )
    assert germany["2026 Jan"].sum() == 20
    assert germany["New dimension"].isna().all()
    assert (
        summary["historicalReclassificationPolicies"]["Czechia"][
            "monthBoundaryCheck"
        ]
        == "pass"
    )
    assert (
        summary["untouchedCountryChecks"]["Germany"][
            "candidateOnlyColumnsNull"
        ]
        is True
    )
    assert (
        jato_monthly_update_service
        ._find_publish_historical_sales_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
        == []
    )
    assert (
        jato_monthly_update_service
        ._find_publish_historical_configuration_changes(
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
        )
        == []
    )


def test_streaming_smart_merge_use_latest_replaces_without_accumulation(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "Old",
                "2026 Jan": 10,
            },
            {
                "Country": "Germany",
                "Make": "OTHER",
                "Model": "UNCHANGED",
                "Version name": "Stable",
                "2026 Jan": 20,
            },
        ]
    ).to_parquet(active_path, index=False)
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Version name": "Washed",
                "New dimension": "latest",
                "2026 Jan": 11,
                "2026 Feb": 12,
            }
        ]
    ).to_parquet(candidate_path, index=False)

    _rows, summary = (
        jato_monthly_update_service._smart_merge_parquet_streaming(
            active_path=active_path,
            candidate_path=candidate_path,
            regressed_countries=[],
            historical_reclassification_decisions={
                "Czechia": "use_latest"
            },
        )
    )

    merged = pd.read_parquet(candidate_path)
    czechia = merged.loc[merged["Country"] == "Czechia"]
    germany = merged.loc[merged["Country"] == "Germany"]
    assert czechia["2026 Jan"].sum() == 11
    assert czechia["2026 Jan"].sum() != 21
    assert czechia["2026 Feb"].sum() == 12
    assert czechia["Version name"].tolist() == ["Washed"]
    assert germany["2026 Jan"].sum() == 20
    assert germany["Version name"].tolist() == ["Stable"]
    assert germany["New dimension"].isna().all()
    assert summary["historicalReclassificationPolicies"]["Czechia"] == {
        "policy": "use_latest",
        "historicalMonthsFrom": "candidate",
        "monthBoundaryCheck": "not_applicable",
    }
    assert summary["untouchedCountryChecks"]["Germany"]["status"] == (
        "pass"
    )


def test_resolved_keep_active_review_blocks_remaining_dimension_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    active_path = (
        project_root / "04_Processed_data" / "jato_full_archive.parquet"
    )
    active_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "Sedan",
                "2026 Jan": 10,
            }
        ]
    ).to_parquet(active_path, index=False)
    candidate_path = project_root / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "SUV",
                "2026 Jan": 10,
                "2026 Feb": 2,
            }
        ]
    ).to_parquet(candidate_path, index=False)
    (project_root / "manifest.json").write_text("{}", encoding="utf-8")
    (project_root / "refresh.json").write_text("{}", encoding="utf-8")
    country_reports = [
        {
            "country": "Czechia",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
            "allowedDecisions": ["use_latest", "keep_active"],
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    payload = {
        "jobId": "jato-keep-active-validation",
        "country": "Czechia",
        "artifacts": {
            "stagingOutputPath": "candidate.parquet",
            "manifestPath": "manifest.json",
            "refreshReportPath": "refresh.json",
            "candidateScope": "target_country_partition_only",
            "untouchedPartitionCheck": {"status": "pass"},
        },
        "historicalReclassificationResolution": {
            "status": "resolved",
            "reportFingerprint": report_fingerprint,
            "decisions": [
                {"country": "Czechia", "decision": "keep_active"}
            ],
            "report": {
                "status": "decision_required",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
            },
        },
    }

    review = jato_monthly_update_service._build_single_country_review(
        payload
    )

    blocker = next(
        finding
        for finding in review["reviewFindings"]
        if finding["ruleId"] == "SC011"
    )
    assert blocker["severity"] == "blocker"
    assert (
        blocker["metrics"]["blockerType"]
        == "historical_keep_active_validation_failed"
    )


def test_full_review_drops_legacy_sales_blocker_only_after_keep_active_pass(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    processed_root = project_root / "04_Processed_data"
    active_path = processed_root / "jato_full_archive.parquet"
    active_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Country": "Netherlands",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "Sedan",
                "2026 Jan": 10,
            }
        ]
    ).to_parquet(active_path, index=False)
    staging = processed_root / "staging" / "review"
    staging.mkdir(parents=True, exist_ok=True)
    candidate_path = staging / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Netherlands",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "Sedan",
                "2026 Jan": 10,
                "2026 Feb": 0,
            },
            {
                "Country": "Netherlands",
                "Make": "BRAND",
                "Model": "MODEL",
                "Body type": "SUV",
                "2026 Jan": 0,
                "2026 Feb": 2,
            },
        ]
    ).to_parquet(candidate_path, index=False)
    raw_report_path = staging / "raw.json"
    jato_monthly_update_service._write_json(
        raw_report_path,
        {
            "decisionSuggestion": "reject_input_batch",
            "reviewFindings": [
                {
                    "severity": "blocker",
                    "scope": "country",
                    "target": "Netherlands",
                    "ruleId": "SC011",
                    "message": "legacy historical sales blocker",
                    "metrics": {
                        "reason": "historical_sales_changed",
                        "countryMismatchCount": 1,
                    },
                    "suggestedAction": "reject_input_batch",
                }
            ],
        },
    )
    artifact_paths = {
        "manifestPath": staging / "manifest.json",
        "fingerprintPath": staging / "fingerprint.json",
        "refreshReportPath": staging / "refresh.json",
    }
    for path in artifact_paths.values():
        path.write_text("{}", encoding="utf-8")
    partition_path = staging / "partition"
    summaries_path = staging / "summaries"
    partition_path.mkdir()
    summaries_path.mkdir()
    (partition_path / "part.txt").write_text("partition", encoding="utf-8")
    (summaries_path / "summary.txt").write_text("summary", encoding="utf-8")
    country_reports = [
        {
            "country": "Netherlands",
            "monthlyTotalsStable": False,
            "decisionRequired": True,
            "allowedDecisions": ["keep_active"],
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    job_id = "jato-full-review-legacy-blocker"
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "status": "success",
            "phase": "completed",
            "artifacts": {
                "stagingOutputPath": (
                    jato_monthly_update_service._relative_to_project(
                        candidate_path
                    )
                ),
                "rawCompareReportPath": (
                    jato_monthly_update_service._relative_to_project(
                        raw_report_path
                    )
                ),
                "candidateScope": "full_smart_merge",
                "manifestPath": (
                    jato_monthly_update_service._relative_to_project(
                        artifact_paths["manifestPath"]
                    )
                ),
                "partitionOutputPath": (
                    jato_monthly_update_service._relative_to_project(
                        partition_path
                    )
                ),
                "fingerprintPath": (
                    jato_monthly_update_service._relative_to_project(
                        artifact_paths["fingerprintPath"]
                    )
                ),
                "refreshReportPath": (
                    jato_monthly_update_service._relative_to_project(
                        artifact_paths["refreshReportPath"]
                    )
                ),
                "summariesOutputPath": (
                    jato_monthly_update_service._relative_to_project(
                        summaries_path
                    )
                ),
            },
            "historicalReclassificationResolution": {
                "status": "resolved",
                "reportFingerprint": report_fingerprint,
                "decisions": [
                    {
                        "country": "Netherlands",
                        "decision": "keep_active",
                    }
                ],
                "report": {
                    "status": "decision_required",
                    "countries": country_reports,
                    "reportFingerprint": report_fingerprint,
                },
            },
        }
    )

    review = jato_monthly_update_service.get_jato_monthly_update_review(
        job_id,
        allow_build=True,
    )

    assert review["historicalReclassificationReport"][
        "resolutionValidation"
    ] == [
        {
            "country": "Netherlands",
            "decision": "keep_active",
            "status": "pass",
            "currentStabilityStatus": "pass",
            "reason": None,
        }
    ]
    assert not any(
        finding["severity"] == "blocker"
        for finding in review["reviewFindings"]
    )


def test_smart_merge_worker_rejects_candidate_drift_after_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root, job_root = _configure_project(tmp_path, monkeypatch)
    processed_root = project_root / "04_Processed_data"
    active_path = processed_root / "jato_full_archive.parquet"
    active_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"Country": "Czechia", "Make": "A", "Model": "M", "2026 Jan": 1}]
    ).to_parquet(active_path, index=False)
    job_id = "jato-resolution-drift"
    staging_dir = processed_root / "staging" / "2026-02-r1"
    staging_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = staging_dir / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "A",
                "Model": "M",
                "2026 Jan": 1,
                "2026 Feb": 1,
            }
        ]
    ).to_parquet(candidate_path, index=False)
    (staging_dir / "manifest.json").write_text("{}", encoding="utf-8")
    (staging_dir / "refresh.json").write_text("{}", encoding="utf-8")
    country_reports = [
        {
            "country": "Czechia",
            "monthlyTotalsStable": True,
            "decisionRequired": True,
        }
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(
            country_reports
        )
    )
    state = {
        "jobId": job_id,
        "status": "queued",
        "phase": "queued",
        "activeBaseFingerprint": (
            jato_monthly_update_service._active_dataset_version()
        ),
        "artifacts": {
            "stagingOutputPath": (
                "04_Processed_data/staging/2026-02-r1/candidate.parquet"
            ),
            "manifestPath": (
                "04_Processed_data/staging/2026-02-r1/manifest.json"
            ),
            "refreshReportPath": (
                "04_Processed_data/staging/2026-02-r1/refresh.json"
            ),
            "candidateScope": "target_country_partition_only",
        },
        "historicalReclassificationResolution": {
            "status": "queued",
            "sourceCandidateFingerprint": "0" * 64,
            "reportFingerprint": report_fingerprint,
            "decisions": [
                {"country": "Czechia", "decision": "use_latest"}
            ],
            "report": {
                "status": "decision_required",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
            },
        },
    }
    (job_root / job_id).mkdir(parents=True, exist_ok=True)
    jato_monthly_update_service._persist_job_state(state)

    jato_monthly_update_service._run_smart_merge(job_id)

    failed = jato_monthly_update_service._load_job_state(job_id)
    assert failed["status"] == "failed"
    assert failed["phase"] == "smart_merge_failed"
    assert "candidate 内容已变化" in failed["error"]
    assert (
        failed["historicalReclassificationResolution"]["status"]
        == "failed"
    )


def test_web_review_cache_miss_never_builds_heavy_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _project_root, _job_root = _configure_project(tmp_path, monkeypatch)
    job_id = "jato-review-not-ready"
    jato_monthly_update_service._persist_job_state(
        {
            "jobId": job_id,
            "jobType": "partial_country",
            "countryScope": ["捷克", "丹麦"],
            "artifacts": {"candidateScope": "target_country_partitions_only"},
        }
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_build_partial_country_review",
        lambda _payload: (_ for _ in ()).throw(
            AssertionError("web request must not build Review")
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.get_jato_monthly_update_review(
            job_id
        )

    assert exc_info.value.status_code == 409
    assert (
        exc_info.value.detail["blockerType"]
        == "review_bundle_not_ready"
    )
