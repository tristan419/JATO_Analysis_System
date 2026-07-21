from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


@pytest.mark.parametrize(
    "exc",
    [
        MemoryError(
            "Unable to allocate 14.8 MiB for an array with shape (25, 77480)"
        ),
        RuntimeError("numpy._ArrayMemoryError: unable to allocate 14.8 MiB"),
        RuntimeError("pyarrow.lib.ArrowMemoryError: realloc of size 33554432 failed"),
    ],
)
def test_review_memory_failures_are_retryable_platform_failures(
    exc: BaseException,
) -> None:
    digest = jato_monthly_update_service._failure_digest_from_exception(
        phase="building_review",
        exc=exc,
    )

    assert digest["code"] == "MEMORY_LIMIT_EXCEEDED"
    assert digest["category"] == "resource"
    assert digest["phase"] == "building_review"
    assert digest["retryable"] is True
    assert digest["nextAction"] == "resume_smart_merge"
    assert "无需重新洗数或重新上传" in digest["sourceFeedback"]


@pytest.mark.parametrize(
    ("configured_limit", "expected_limit"),
    [
        (
            None,
            jato_monthly_update_service
            .MONTHLY_WORKER_DEFAULT_MEMORY_LIMIT_BYTES,
        ),
        (str(5 * 1024 * 1024 * 1024), 5 * 1024 * 1024 * 1024),
    ],
)
def test_launch_job_thread_uses_six_gib_default_and_keeps_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    configured_limit: str | None,
    expected_limit: int,
) -> None:
    worker_script = tmp_path / "jato_monthly_worker.py"
    worker_script.write_text("# test worker\n", encoding="utf-8")
    captured: dict[str, Any] = {}

    def fake_popen(args: list[str], **kwargs: Any) -> object:
        captured["args"] = args
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(
        jato_monthly_update_service,
        "MONTHLY_WORKER_SCRIPT_PATH",
        worker_script,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "PROJECT_ROOT",
        tmp_path,
    )
    monkeypatch.setattr(
        jato_monthly_update_service.subprocess,
        "Popen",
        fake_popen,
    )
    monkeypatch.delenv("APP_JATO_MONTHLY_EXECUTION_MODE", raising=False)
    if configured_limit is None:
        monkeypatch.delenv(
            "APP_JATO_MONTHLY_WORKER_MEMORY_LIMIT_BYTES",
            raising=False,
        )
    else:
        monkeypatch.setenv(
            "APP_JATO_MONTHLY_WORKER_MEMORY_LIMIT_BYTES",
            configured_limit,
        )
    monkeypatch.setenv("MALLOC_ARENA_MAX", "99")
    monkeypatch.setenv("OMP_NUM_THREADS", "99")
    monkeypatch.setenv("OPENBLAS_NUM_THREADS", "99")
    monkeypatch.setenv("MKL_NUM_THREADS", "99")
    monkeypatch.setenv("NUMEXPR_NUM_THREADS", "99")

    jato_monthly_update_service._launch_job_thread("jato-memory-headroom")

    child_env = captured["env"]
    assert int(
        child_env["APP_JATO_MONTHLY_WORKER_MEMORY_LIMIT_BYTES"]
    ) == expected_limit
    assert child_env["MALLOC_ARENA_MAX"] == "2"
    assert child_env["OMP_NUM_THREADS"] == "1"
    assert child_env["OPENBLAS_NUM_THREADS"] == "1"
    assert child_env["MKL_NUM_THREADS"] == "1"
    assert child_env["NUMEXPR_NUM_THREADS"] == "1"
    assert captured["args"][-1] == "--drain"


def test_review_refresh_memory_failure_is_retryable_without_reupload() -> None:
    digest = jato_monthly_update_service._review_refresh_failure_digest(
        MemoryError("Unable to allocate 32 MiB")
    )

    assert digest["code"] == "MEMORY_LIMIT_EXCEEDED"
    assert digest["category"] == "resource"
    assert digest["phase"] == "review_refresh"
    assert digest["retryable"] is True
    assert digest["nextAction"] == "retry_review_refresh"
    assert "无需重新洗数或重新上传" in digest["sourceFeedback"]


@pytest.mark.parametrize(
    ("bundle_schema_version", "queues_refresh"),
    [(2, True), (3, False)],
)
def test_review_refresh_rebuilds_old_schema_but_rejects_current_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bundle_schema_version: int,
    queues_refresh: bool,
) -> None:
    project_root = tmp_path / "project"
    job_root = (
        project_root
        / "04_Processed_data"
        / "ops"
        / "jato_monthly_update_jobs"
    )
    staging_dir = project_root / "04_Processed_data" / "staging" / "review"
    staging_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = staging_dir / "candidate.parquet"
    manifest_path = staging_dir / "manifest.json"
    refresh_path = staging_dir / "refresh.json"
    partition_path = staging_dir / "partitioned_dataset_v1"
    fingerprint_path = staging_dir / "dataset_fingerprint.json"
    summaries_path = staging_dir / "summaries"
    candidate_path.write_bytes(b"candidate-v1")
    manifest_path.write_text('{"version":1}', encoding="utf-8")
    refresh_path.write_text('{"status":"success"}', encoding="utf-8")
    fingerprint_path.write_text('{"fingerprint":"v1"}', encoding="utf-8")
    partition_path.mkdir()
    (partition_path / "part.parquet").write_bytes(b"partition-v1")
    summaries_path.mkdir()
    (summaries_path / "summary.json").write_text("{}", encoding="utf-8")
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
    artifacts = {
        "candidateScope": "full_smart_merge",
        "stagingOutputPath": (
            "04_Processed_data/staging/review/candidate.parquet"
        ),
        "manifestPath": "04_Processed_data/staging/review/manifest.json",
        "partitionOutputPath": (
            "04_Processed_data/staging/review/partitioned_dataset_v1"
        ),
        "fingerprintPath": (
            "04_Processed_data/staging/review/dataset_fingerprint.json"
        ),
        "refreshReportPath": "04_Processed_data/staging/review/refresh.json",
        "summariesOutputPath": (
            "04_Processed_data/staging/review/summaries"
        ),
    }
    candidate_fingerprint = (
        jato_monthly_update_service._candidate_fingerprint_id(artifacts)
    )
    signature = (
        jato_monthly_update_service._candidate_artifact_stat_signature(
            artifacts
        )
    )
    job_id = f"jato-review-schema-{bundle_schema_version}"
    review_path = jato_monthly_update_service._job_review_bundle_path(job_id)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    jato_monthly_update_service._write_json(
        review_path,
        {
            "candidateFingerprint": candidate_fingerprint,
            "reviewBundleSchemaVersion": bundle_schema_version,
            "candidateArtifactStatSignatureVersion": (
                jato_monthly_update_service
                .CANDIDATE_ARTIFACT_STAT_SIGNATURE_VERSION
            ),
            "candidateArtifactStatSignature": signature,
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
    launches: list[str] = []
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_active_dataset_version",
        lambda: "b" * 64,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_launch_job_thread",
        launches.append,
    )

    if queues_refresh:
        queued = (
            jato_monthly_update_service
            ._queue_jato_monthly_update_review_refresh_locked(
                job_id=job_id,
                triggered_by="tester",
                request_id=f"review-schema-{bundle_schema_version}",
                expected_candidate_fingerprint=candidate_fingerprint,
            )
        )
        assert queued["pendingOperation"]["status"] == "queued"
        assert launches == [job_id]
    else:
        with pytest.raises(HTTPException, match="当前 Review bundle 已是最新"):
            (
                jato_monthly_update_service
                ._queue_jato_monthly_update_review_refresh_locked(
                    job_id=job_id,
                    triggered_by="tester",
                    request_id=f"review-schema-{bundle_schema_version}",
                    expected_candidate_fingerprint=candidate_fingerprint,
                )
            )
        assert launches == []


def test_historical_sales_frame_matches_existing_numeric_semantics() -> None:
    frame = pd.DataFrame(
        {
            "2026 Jan": [1, "2", None, "bad"],
            "2026 Feb": [3.5, 0, float("nan"), "4.5"],
        },
        index=[10, 11, 12, 13],
    )
    months = ["2026 Jan", "2026 Feb"]
    original = frame.copy(deep=True)
    expected = (
        frame[months]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
    )

    actual = jato_monthly_update_service._historical_sales_frame(
        frame,
        months,
    )

    pd.testing.assert_frame_equal(actual, expected)
    pd.testing.assert_frame_equal(frame, original)


def test_historical_sales_frame_reuses_clean_numeric_buffers() -> None:
    frame = pd.DataFrame(
        {
            "2026 Jan": [1, 2, 3],
            "2026 Feb": [4.0, 5.0, 6.0],
        }
    )

    actual = jato_monthly_update_service._historical_sales_frame(
        frame,
        ["2026 Jan", "2026 Feb"],
    )

    assert np.shares_memory(
        actual["2026 Jan"].to_numpy(),
        frame["2026 Jan"].to_numpy(),
    )
    assert np.shares_memory(
        actual["2026 Feb"].to_numpy(),
        frame["2026 Feb"].to_numpy(),
    )


def test_full_candidate_single_review_reuses_stability_for_keep_active_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path / "project"
    active_path = (
        project_root / "04_Processed_data" / "jato_full_archive.parquet"
    )
    active_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "SKODA",
                "Model": "T5",
                "Body type": "SUV",
                "2026 Jan": 10,
            }
        ]
    ).to_parquet(active_path, index=False)
    candidate_path = project_root / "candidate.parquet"
    pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "SKODA",
                "Model": "T5",
                "Body type": "SUV",
                "2026 Jan": 10,
                "2026 Feb": 12,
            }
        ]
    ).to_parquet(candidate_path, index=False)
    (project_root / "manifest.json").write_text("{}", encoding="utf-8")
    (project_root / "refresh.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        jato_monthly_update_service,
        "PROJECT_ROOT",
        project_root,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_build_historical_reclassification_report_from_paths",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("single-country Review must reuse stability")
        ),
    )
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
        ._historical_reclassification_report_fingerprint(country_reports)
    )

    review = jato_monthly_update_service._build_single_country_review(
        {
            "jobId": "jato-single-keep-active-proof",
            "country": "Czechia",
            "artifacts": {
                "stagingOutputPath": "candidate.parquet",
                "manifestPath": "manifest.json",
                "refreshReportPath": "refresh.json",
                "candidateScope": "full_smart_merge",
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
        },
        candidate_fingerprint="f" * 64,
    )

    assert review["historicalReclassificationReport"][
        "resolutionValidation"
    ] == [
        {
            "country": "Czechia",
            "decision": "keep_active",
            "status": "pass",
            "currentStabilityStatus": "pass",
            "reason": None,
        }
    ]


def test_partition_candidate_path_review_does_not_issue_keep_active_proof(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "active.parquet"
    candidate_path = tmp_path / "candidate.parquet"
    frame = pd.DataFrame(
        [
            {
                "Country": "Czechia",
                "Make": "SKODA",
                "Model": "T5",
                "2026 Jan": 10,
            }
        ]
    )
    frame.to_parquet(active_path, index=False)
    frame.to_parquet(candidate_path, index=False)
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
        ._historical_reclassification_report_fingerprint(country_reports)
    )

    report = (
        jato_monthly_update_service
        ._build_historical_reclassification_report_from_paths(
            payload={
                "artifacts": {
                    "candidateScope": "target_country_partition_only"
                },
                "historicalReclassificationResolution": {
                    "status": "resolved",
                    "reportFingerprint": report_fingerprint,
                    "decisions": [
                        {
                            "country": "Czechia",
                            "decision": "keep_active",
                        }
                    ],
                    "report": {
                        "status": "decision_required",
                        "countries": country_reports,
                        "reportFingerprint": report_fingerprint,
                    },
                },
            },
            countries=["Czechia"],
            active_path=active_path,
            candidate_path=candidate_path,
        )
    )

    assert report["status"] == "resolved"
    assert "resolutionValidation" not in report


def test_partial_review_aggregates_exact_keep_active_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    countries = ["捷克", "丹麦"]
    country_reports = [
        {
            "country": country,
            "monthlyTotalsStable": True,
            "decisionRequired": True,
            "allowedDecisions": ["use_latest", "keep_active"],
        }
        for country in countries
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(country_reports)
    )

    def fake_country_review(
        payload: dict[str, object],
        *,
        candidate_fingerprint: str | None = None,
    ) -> dict[str, object]:
        _ = candidate_fingerprint
        country = str(payload["country"])
        return {
            "reviewFindings": [],
            "compareKeyColumns": ["Country", "Model"],
            "checklistMarkdown": f"- {country}",
            "overlapChangeSummary": [],
            "countryFreshnessSummary": [{"country": country}],
            "countryCoverageSummary": [{"country": country}],
            "countryMonthlySalesSummary": [
                {"country": country, "rows": []}
            ],
            "timeAxisCheck": {"targetCountry": country},
            "refreshSummary": {"jobStatus": "success"},
            "candidateFingerprint": "f" * 64,
            "historicalReclassificationReport": {
                "status": "resolved",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
                "resolutionValidation": [
                    {
                        "country": country,
                        "decision": "keep_active",
                        "status": "pass",
                        "currentStabilityStatus": "pass",
                        "reason": None,
                    }
                ],
            },
        }

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_build_single_country_review",
        fake_country_review,
    )
    review = jato_monthly_update_service._build_partial_country_review(
        {
            "jobId": "jato-partial-keep-active-validation",
            "countryScope": countries,
            "artifacts": {
                "candidateScope": "full_smart_merge",
                "untouchedPartitionCheck": {"status": "pass"},
            },
            "historicalReclassificationResolution": {
                "status": "resolved",
                "reportFingerprint": report_fingerprint,
                "decisions": [
                    {"country": country, "decision": "keep_active"}
                    for country in countries
                ],
                "report": {
                    "status": "decision_required",
                    "countries": country_reports,
                    "reportFingerprint": report_fingerprint,
                },
            },
        }
    )

    assert review["historicalReclassificationReport"][
        "resolutionValidation"
    ] == [
        {
            "country": country,
            "decision": "keep_active",
            "status": "pass",
            "currentStabilityStatus": "pass",
            "reason": None,
        }
        for country in countries
    ]


@pytest.mark.parametrize(
    "raw_validation",
    [
        [
            {
                "country": "捷克",
                "decision": "keep_active",
                "status": "pass",
            }
        ],
        [
            {
                "country": "捷克",
                "decision": "keep_active",
                "status": "pass",
            },
            {
                "country": "捷克",
                "decision": "keep_active",
                "status": "pass",
            },
            {
                "country": "丹麦",
                "decision": "keep_active",
                "status": "pass",
            },
        ],
        [
            {
                "country": "捷克",
                "decision": "keep_active",
                "status": "pass",
            },
            {
                "country": "丹麦",
                "decision": "keep_active",
                "status": "pass",
            },
            {
                "country": "德国",
                "decision": "keep_active",
                "status": "pass",
            },
        ],
    ],
)
def test_partial_keep_active_proof_rejects_missing_duplicate_or_extra(
    raw_validation: list[dict[str, object]],
) -> None:
    country_reports = [
        {
            "country": country,
            "monthlyTotalsStable": True,
            "decisionRequired": True,
            "allowedDecisions": ["use_latest", "keep_active"],
        }
        for country in ["捷克", "丹麦"]
    ]
    report_fingerprint = (
        jato_monthly_update_service
        ._historical_reclassification_report_fingerprint(country_reports)
    )
    payload = {
        "historicalReclassificationResolution": {
            "status": "resolved",
            "reportFingerprint": report_fingerprint,
            "decisions": [
                {"country": country, "decision": "keep_active"}
                for country in ["捷克", "丹麦"]
            ],
            "report": {
                "status": "decision_required",
                "countries": country_reports,
                "reportFingerprint": report_fingerprint,
            },
        }
    }

    with pytest.raises(HTTPException) as exc_info:
        (
            jato_monthly_update_service
            ._exact_partial_keep_active_resolution_validation(
                payload=payload,
                raw_validation=raw_validation,
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == (
        "historical_keep_active_validation_failed"
    )
