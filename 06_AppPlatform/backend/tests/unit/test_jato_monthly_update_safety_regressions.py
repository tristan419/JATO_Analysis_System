import hashlib
import importlib.util
import sys
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

    candidate_path.write_bytes(b"candidate-drift-with-different-size")
    with pytest.raises(HTTPException, match="candidate 在 Review bundle 生成后已变化"):
        jato_monthly_update_service.get_jato_monthly_update_review(job_id)


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
        lambda cached_job_id: (
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
    merged = pd.read_parquet(candidate_path)
    assert sorted(merged["Country"].tolist()) == ["Germany", "Hungary"]
    assert len(merged.loc[merged["Country"] == "Hungary"]) == 1
    assert (
        merged.loc[
            merged["Country"] == "Hungary",
            "2026 May",
        ].iloc[0]
        == 12
    )
