import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


def _configure_raw_roots(
    tmp_path: Path,
    monkeypatch,
    *,
    active_baseline_name: str | None = None,
    archived_baseline_name: str | None = None,
) -> tuple[Path, Path, Path]:
    project_root = tmp_path / "project"
    raw_root = project_root / "01_RAW_DATA"
    baseline_root = raw_root / "baseline"
    history_root = raw_root / "historyDataArchive"

    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "RAW_DATA_ROOT", raw_root)
    monkeypatch.setattr(jato_monthly_update_service, "BASELINE_ROOT", baseline_root)
    monkeypatch.setattr(jato_monthly_update_service, "PATCHES_ROOT", raw_root / "patches")
    monkeypatch.setattr(
        jato_monthly_update_service, "HISTORY_ARCHIVE_ROOT", history_root
    )

    baseline_root.mkdir(parents=True, exist_ok=True)
    (history_root / "baseline").mkdir(parents=True, exist_ok=True)

    baseline_path = baseline_root / (
        active_baseline_name or "JATO-2026.2-full-baseline.xlsx"
    )
    archive_path = (history_root / "baseline") / (
        archived_baseline_name or "JATO-2026.1-full-baseline.xlsx"
    )

    if active_baseline_name is not None:
        baseline_path.write_bytes(b"active-baseline")
    if archived_baseline_name is not None:
        archive_path.write_bytes(b"archived-baseline")

    return project_root, baseline_path, archive_path


def test_detect_latest_month_from_upload_uses_last_non_empty_month(tmp_path: Path) -> None:
    upload_path = tmp_path / "patch.xlsx"
    pd.DataFrame(
        {
            "国家": ["德国", "波兰"],
            "2026 Jan": [1, 2],
            "2026 Feb": [0, None],
            "2026 Mar": [None, 3],
        }
    ).to_excel(upload_path, index=False, sheet_name="Data Export")

    detected = jato_monthly_update_service._detect_latest_month_from_upload(upload_path)

    assert detected == "2026-03"


def test_allocate_batch_id_increments_existing_revisions(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    patch_root = project_root / "01_RAW_DATA" / "patches"
    job_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "PATCHES_ROOT", patch_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    (patch_root / "2026-03-r1").mkdir(parents=True, exist_ok=True)
    stored_path = job_root / "jato-update-existing" / "uploads" / "patch.xlsx"
    stored_path.parent.mkdir(parents=True, exist_ok=True)
    stored_path.write_bytes(b"fake")
    existing_state = jato_monthly_update_service._prepare_initial_job_state(
        job_id="jato-update-existing",
        month="2026-03",
        batch_id="2026-03-r2",
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=stored_path,
    )
    jato_monthly_update_service._persist_job_state(existing_state)

    allocated = jato_monthly_update_service._allocate_batch_id("2026-03")

    assert allocated == "2026-03-r3"


def test_parse_plan_markdown_extracts_commands_and_artifacts(tmp_path: Path) -> None:
    plan_path = tmp_path / "monthly_update_plan.md"
    plan_path.write_text(
        "\n".join(
            [
                "# 2026-03 月度更新计划",
                "",
                "- 数据月: 2026-03",
                "- 批次: 2026-03-r1",
                "- 对比: 2026-02_vs_2026-03-r1",
                "- baseline: 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx",
                "- patch: 01_RAW_DATA/patches/2026-03-r1/JATO-2026.3-partial.xlsx",
                "",
                "## 步骤 1 · Raw Compare",
                "",
                "```bash",
                "python 03_Scripts/raw_compare_review.py --old 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx --new 01_RAW_DATA/patches/2026-03-r1/JATO-2026.3-partial.xlsx --allow-missing-countries --output-dir 04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03-r1",
                "```",
                "",
                "## 步骤 2 · Candidate Refresh",
                "",
                "```bash",
                "python 03_Scripts/data_pipeline/run_data_refresh_job.py --baseline-input 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx --patch-input-files 01_RAW_DATA/patches/2026-03-r1/JATO-2026.3-partial.xlsx --output 04_Processed_data/staging/2026-03-r1-mixed/jato_full_archive.parquet --manifest 04_Processed_data/staging/2026-03-r1-mixed/manifest.json --partition-output 04_Processed_data/staging/2026-03-r1-mixed/partitioned_dataset_v1 --report 04_Processed_data/staging/2026-03-r1-mixed/refresh_job_report.json --fingerprint 04_Processed_data/staging/2026-03-r1-mixed/dataset_fingerprint.json --incremental --skip-benchmark",
                "```",
            ]
        ),
        encoding="utf-8",
    )

    parsed = jato_monthly_update_service._parse_plan_markdown(plan_path)

    assert parsed["month"] == "2026-03"
    assert parsed["batchId"] == "2026-03-r1"
    assert parsed["compareId"] == "2026-02_vs_2026-03-r1"
    assert parsed["baselinePath"] == "01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx"
    assert parsed["patchPath"] == "01_RAW_DATA/patches/2026-03-r1/JATO-2026.3-partial.xlsx"
    assert parsed["reviewDir"] == "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03-r1"
    assert parsed["rawCompareReportPath"] == "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03-r1/raw_compare_report.json"
    assert parsed["refreshReportPath"] == "04_Processed_data/staging/2026-03-r1-mixed/refresh_job_report.json"
    assert parsed["manifestPath"] == "04_Processed_data/staging/2026-03-r1-mixed/manifest.json"
    assert parsed["fingerprintPath"] == "04_Processed_data/staging/2026-03-r1-mixed/dataset_fingerprint.json"


def test_summarize_raw_compare_report_counts_findings() -> None:
    summary = jato_monthly_update_service._summarize_raw_compare_report(
        {
            "compareId": "2026-02_vs_2026-03",
            "decisionSuggestion": "manual_review_required",
            "compareKeyMode": "auto",
            "compareKeyColumns": ["国家", "MakeModel"],
            "reviewFindings": [
                {"severity": "blocker"},
                {"severity": "review"},
                {"severity": "review"},
                {"severity": "info"},
            ],
            "countryFreshnessSummary": [
                {"freshnessStatus": "advanced"},
                {"freshnessStatus": "advanced"},
                {"freshnessStatus": "regressed"},
                {"freshnessStatus": "new_country"},
                {"freshnessStatus": "missing_in_candidate"},
            ],
            "countryScopeSummary": {
                "addedCountries": ["挪威", "丹麦"],
                "removedCountries": ["芬兰"],
            },
        }
    )

    assert summary == {
        "compareId": "2026-02_vs_2026-03",
        "decisionSuggestion": "manual_review_required",
        "compareKeyMode": "auto",
        "compareKeyColumns": ["国家", "MakeModel"],
        "blockerCount": 1,
        "reviewCount": 2,
        "infoCount": 1,
        "advancedCountryCount": 2,
        "regressedCountryCount": 1,
        "newCountryCount": 1,
        "missingCountryCount": 1,
        "addedCountryCount": 2,
        "removedCountryCount": 1,
    }


def test_serialize_job_state_includes_log_tail(tmp_path: Path, monkeypatch) -> None:
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service,
        "MONTHLY_UPDATE_JOB_ROOT",
        job_root,
    )

    job_id = "jato-update-1234abcd"
    log_path = job_root / job_id / "job.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("line1\nline2\nline3\n", encoding="utf-8")

    payload = {
        "jobId": job_id,
        "month": "2026-03",
        "status": "running",
        "phase": "refresh",
        "triggeredBy": "analyst",
        "createdAt": "2026-04-13T00:00:00+00:00",
        "updatedAt": "2026-04-13T00:01:00+00:00",
        "startedAt": "2026-04-13T00:00:10+00:00",
        "finishedAt": None,
        "error": None,
        "upload": {"originalFilename": "input.xlsx"},
        "plan": {"path": "01_RAW_DATA/patches/2026-03/monthly_update_plan.md"},
        "artifacts": {"refreshReportPath": "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json"},
        "summaries": {"refresh": {"jobStatus": "running"}},
        "logPath": "04_Processed_data/ops/jato_monthly_update_jobs/jato-update-1234abcd/job.log",
    }

    item = jato_monthly_update_service._serialize_job_state(
        payload,
        include_log_tail=True,
    )

    assert item["jobId"] == job_id
    assert item["logTail"] == "line1\nline2\nline3"
    assert item["artifacts"] == {
        "refreshReportPath": "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json"
    }


def _write_plan(project_root: Path, month: str, compare_id: str) -> None:
    plan_path = (
        project_root / "01_RAW_DATA" / "patches" / month / "monthly_update_plan.md"
    )
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(
        "\n".join(
            [
                f"# {month} 月度更新计划",
                "",
                f"- 数据月: {month}",
                f"- 批次: {month}",
                f"- 对比: {compare_id}",
                "- baseline: 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx",
                f"- patch: 01_RAW_DATA/patches/{month}/JATO-2026.3-partial.xlsx",
                "",
                "## 步骤 1 · Raw Compare",
                "",
                "```bash",
                f"python 03_Scripts/raw_compare_review.py --old 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx --new 01_RAW_DATA/patches/{month}/JATO-2026.3-partial.xlsx --allow-missing-countries --output-dir 04_Processed_data/reviews/raw_compare/{compare_id}",
                "```",
                "",
                "## 步骤 2 · Candidate Refresh",
                "",
                "```bash",
                f"python 03_Scripts/data_pipeline/run_data_refresh_job.py --baseline-input 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx --patch-input-files 01_RAW_DATA/patches/{month}/JATO-2026.3-partial.xlsx --output 04_Processed_data/staging/{month}-mixed/jato_full_archive.parquet --manifest 04_Processed_data/staging/{month}-mixed/manifest.json --partition-output 04_Processed_data/staging/{month}-mixed/partitioned_dataset_v1 --report 04_Processed_data/staging/{month}-mixed/refresh_job_report.json --fingerprint 04_Processed_data/staging/{month}-mixed/dataset_fingerprint.json --incremental --skip-benchmark",
                "```",
            ]
        ),
        encoding="utf-8",
    )


def _write_dataset_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_run_job_marks_success_and_collects_summaries(
    tmp_path: Path, monkeypatch
) -> None:
    project_root, baseline_path, _archive_path = _configure_raw_roots(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.2-full-baseline.xlsx",
    )
    month = "2026-03"
    compare_id = "2026-02_vs_2026-03"
    job_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    upload_path = job_root / "jato-update-1234abcd" / "uploads" / "patch.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"fake-xlsx")

    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    prepare_script = (
        project_root
        / "03_Scripts"
        / "data_pipeline"
        / "prepare_monthly_raw_update.py"
    )
    prepare_script.parent.mkdir(parents=True, exist_ok=True)
    prepare_script.write_text("# fake", encoding="utf-8")
    monkeypatch.setattr(
        jato_monthly_update_service, "PREPARE_SCRIPT_PATH", prepare_script
    )

    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id="jato-update-1234abcd",
        month=month,
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=upload_path,
        baseline_path=baseline_path,
        baseline_source="active",
    )
    jato_monthly_update_service._persist_job_state(state)

    def fake_run_logged_command(
        *, label: str, args: list[str], log_path: Path
    ) -> None:
        jato_monthly_update_service._append_log(
            log_path, f"{label}: {' '.join(args)}"
        )
        if label == "Prepare monthly update":
            assert "--batch-id" in args
            assert args[args.index("--batch-id") + 1] == month
            assert "--baseline" in args
            assert args[args.index("--baseline") + 1] == str(baseline_path)
            _write_plan(project_root, month, compare_id)
        elif label == "Raw compare review":
            report_path = (
                project_root
                / "04_Processed_data"
                / "reviews"
                / "raw_compare"
                / compare_id
                / "raw_compare_report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(
                    {
                        "compareId": compare_id,
                        "decisionSuggestion": "manual_review_required",
                        "compareKeyMode": "auto",
                        "compareKeyColumns": ["国家", "MakeModel"],
                        "reviewFindings": [
                            {"severity": "blocker"},
                            {"severity": "review"},
                            {"severity": "info"},
                        ],
                        "countryFreshnessSummary": [
                            {"freshnessStatus": "advanced"},
                            {"freshnessStatus": "regressed"},
                        ],
                        "countryScopeSummary": {
                            "addedCountries": ["挪威"],
                            "removedCountries": [],
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        elif label == "Candidate refresh":
            refresh_path = (
                project_root
                / "04_Processed_data"
                / "staging"
                / f"{month}-mixed"
                / "refresh_job_report.json"
            )
            refresh_path.parent.mkdir(parents=True, exist_ok=True)
            refresh_path.write_text(
                json.dumps(
                    {
                        "jobStatus": "success",
                        "jobElapsedSeconds": 12.8,
                        "fullManifest": {"rows": 12345, "columns": 96},
                        "partitionManifest": {"parquetFileCount": 21},
                        "incremental": {
                            "fingerprintMatched": False,
                            "fingerprintUpdated": True,
                            "regression": {
                                "changedRows": 321,
                                "changedCountryCount": 4,
                                "mergeKeyRegression": {
                                    "conflictGroupCount": 7,
                                    "conflictRowCount": 18,
                                },
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_logged_command",
        fake_run_logged_command,
    )

    jato_monthly_update_service._run_job("jato-update-1234abcd")
    payload = jato_monthly_update_service._load_job_state("jato-update-1234abcd")

    assert payload["status"] == "success"
    assert payload["phase"] == "completed"
    assert payload["error"] is None
    assert payload["plan"]["compareId"] == compare_id
    assert payload["summaries"]["rawCompare"]["blockerCount"] == 1
    assert payload["summaries"]["refresh"]["rowCount"] == 12345
    assert payload["artifacts"]["rawCompareReportPath"] == (
        f"04_Processed_data/reviews/raw_compare/{compare_id}/raw_compare_report.json"
    )


def test_run_job_injects_active_parquet_supplement_into_refresh(
    tmp_path: Path, monkeypatch
) -> None:
    project_root, baseline_path, _archive_path = _configure_raw_roots(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.2-full-baseline.xlsx",
    )
    month = "2026-03"
    compare_id = "2026-02_vs_2026-03"
    processed_root = project_root / "04_Processed_data"
    _write_dataset_parquet(
        processed_root / "jato_full_archive.parquet",
        [{"国家": "瑞典", "Model": "EX30", "2026 Jan": 10}],
    )
    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    upload_path = job_root / "jato-update-merge" / "uploads" / "patch.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"fake-xlsx")

    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    prepare_script = (
        project_root
        / "03_Scripts"
        / "data_pipeline"
        / "prepare_monthly_raw_update.py"
    )
    prepare_script.parent.mkdir(parents=True, exist_ok=True)
    prepare_script.write_text("# fake", encoding="utf-8")
    monkeypatch.setattr(
        jato_monthly_update_service, "PREPARE_SCRIPT_PATH", prepare_script
    )

    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id="jato-update-merge",
        month=month,
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=upload_path,
        baseline_path=baseline_path,
        baseline_source="active",
    )
    jato_monthly_update_service._persist_job_state(state)

    def fake_run_logged_command(
        *, label: str, args: list[str], log_path: Path
    ) -> None:
        jato_monthly_update_service._append_log(
            log_path, f"{label}: {' '.join(args)}"
        )
        if label == "Prepare monthly update":
            assert "--batch-id" in args
            assert args[args.index("--batch-id") + 1] == month
            _write_plan(project_root, month, compare_id)
        elif label == "Raw compare review":
            report_path = (
                project_root
                / "04_Processed_data"
                / "reviews"
                / "raw_compare"
                / compare_id
                / "raw_compare_report.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(
                    {
                        "compareId": compare_id,
                        "decisionSuggestion": "manual_review_required",
                        "compareKeyMode": "auto",
                        "compareKeyColumns": ["国家", "MakeModel"],
                        "reviewFindings": [],
                        "countryFreshnessSummary": [],
                        "countryScopeSummary": {
                            "addedCountries": [],
                            "removedCountries": [],
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        elif label == "Candidate refresh":
            assert "--supplement-missing-countries-from-parquet" in args
            assert args[args.index("--supplement-missing-countries-from-parquet") + 1] == str(
                processed_root / "jato_full_archive.parquet"
            )
            refresh_path = (
                project_root
                / "04_Processed_data"
                / "staging"
                / f"{month}-mixed"
                / "refresh_job_report.json"
            )
            refresh_path.parent.mkdir(parents=True, exist_ok=True)
            refresh_path.write_text(
                json.dumps(
                    {
                        "jobStatus": "success",
                        "jobElapsedSeconds": 10.2,
                        "fullManifest": {"rows": 123, "columns": 96},
                        "partitionManifest": {"parquetFileCount": 2},
                        "incremental": {
                            "fingerprintMatched": False,
                            "fingerprintUpdated": True,
                            "regression": {
                                "changedRows": 12,
                                "changedCountryCount": 1,
                                "mergeKeyRegression": {
                                    "conflictGroupCount": 0,
                                    "conflictRowCount": 0,
                                },
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_logged_command",
        fake_run_logged_command,
    )

    jato_monthly_update_service._run_job("jato-update-merge")
    payload = jato_monthly_update_service._load_job_state("jato-update-merge")

    assert payload["artifacts"]["supplementParquetPath"] == (
        "04_Processed_data/jato_full_archive.parquet"
    )


def test_run_job_marks_failed_when_stage_raises(
    tmp_path: Path, monkeypatch
) -> None:
    project_root, baseline_path, _archive_path = _configure_raw_roots(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.2-full-baseline.xlsx",
    )
    month = "2026-03"
    compare_id = "2026-02_vs_2026-03"
    job_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    upload_path = job_root / "jato-update-failed" / "uploads" / "patch.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"fake-xlsx")

    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    prepare_script = (
        project_root
        / "03_Scripts"
        / "data_pipeline"
        / "prepare_monthly_raw_update.py"
    )
    prepare_script.parent.mkdir(parents=True, exist_ok=True)
    prepare_script.write_text("# fake", encoding="utf-8")
    monkeypatch.setattr(
        jato_monthly_update_service, "PREPARE_SCRIPT_PATH", prepare_script
    )

    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id="jato-update-failed",
        month=month,
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=upload_path,
        baseline_path=baseline_path,
        baseline_source="active",
    )
    jato_monthly_update_service._persist_job_state(state)

    def fake_run_logged_command(
        *, label: str, args: list[str], log_path: Path
    ) -> None:
        jato_monthly_update_service._append_log(
            log_path, f"{label}: {' '.join(args)}"
        )
        if label == "Prepare monthly update":
            assert "--batch-id" in args
            assert args[args.index("--batch-id") + 1] == month
            assert "--baseline" in args
            assert args[args.index("--baseline") + 1] == str(baseline_path)
            _write_plan(project_root, month, compare_id)
            return
        if label == "Raw compare review":
            raise RuntimeError("compare step exploded")

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_run_logged_command",
        fake_run_logged_command,
    )

    jato_monthly_update_service._run_job("jato-update-failed")
    payload = jato_monthly_update_service._load_job_state("jato-update-failed")

    assert payload["status"] == "failed"
    assert payload["phase"] == "failed"
    assert payload["error"] == "compare step exploded"
    assert "compare step exploded" in (
        (job_root / "jato-update-failed" / "job.log").read_text(encoding="utf-8")
    )


def test_run_cleanup_archives_old_raw_data_and_removes_job_upload_copies(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    raw_root = project_root / "01_RAW_DATA"
    baseline_root = raw_root / "baseline"
    patch_root = raw_root / "patches"
    history_root = raw_root / "historyDataArchive"
    job_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"

    baseline_root.mkdir(parents=True, exist_ok=True)
    patch_root.mkdir(parents=True, exist_ok=True)
    history_root.mkdir(parents=True, exist_ok=True)

    old_baseline = baseline_root / "JATO-2026.1-full-baseline.xlsx"
    new_baseline = baseline_root / "JATO-2026.3-full-baseline.xlsx"
    old_baseline.write_bytes(b"old")
    new_baseline.write_bytes(b"new")

    old_patch_dir = patch_root / "2026-02"
    new_patch_dir = patch_root / "2026-03"
    old_patch_dir.mkdir()
    new_patch_dir.mkdir()
    (old_patch_dir / "old.xlsx").write_bytes(b"old-patch")
    (new_patch_dir / "new.xlsx").write_bytes(b"new-patch")

    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "RAW_DATA_ROOT", raw_root)
    monkeypatch.setattr(jato_monthly_update_service, "BASELINE_ROOT", baseline_root)
    monkeypatch.setattr(jato_monthly_update_service, "PATCHES_ROOT", patch_root)
    monkeypatch.setattr(
        jato_monthly_update_service, "HISTORY_ARCHIVE_ROOT", history_root
    )
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )

    success_upload = job_root / "job-success" / "uploads" / "patch.xlsx"
    success_upload.parent.mkdir(parents=True, exist_ok=True)
    success_upload.write_bytes(b"upload")
    success_state = jato_monthly_update_service._prepare_initial_job_state(
        job_id="job-success",
        month="2026-03",
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=success_upload,
    )
    success_state["status"] = "success"
    success_state["phase"] = "completed"
    jato_monthly_update_service._persist_job_state(success_state)

    failed_upload = job_root / "job-failed" / "uploads" / "patch.xlsx"
    failed_upload.parent.mkdir(parents=True, exist_ok=True)
    failed_upload.write_bytes(b"failed-upload")
    failed_state = jato_monthly_update_service._prepare_initial_job_state(
        job_id="job-failed",
        month="2026-03",
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=failed_upload,
    )
    failed_state["status"] = "failed"
    failed_state["phase"] = "failed"
    failed_state["error"] = "prepare exploded"
    jato_monthly_update_service._persist_job_state(failed_state)

    result = jato_monthly_update_service.run_jato_monthly_update_cleanup(
        triggered_by="tester"
    )

    assert result["triggeredBy"] == "tester"
    assert result["cleanupTier"] == "safe"
    assert result["freedBytes"] > 0
    assert (
        result["activeBaselinePath"]
        == "01_RAW_DATA/baseline/JATO-2026.3-full-baseline.xlsx"
    )
    assert result["activePatchMonth"] == "2026-03"
    assert result["archivedBaselineCount"] == 1
    assert result["archivedBaselines"] == [
        "01_RAW_DATA/historyDataArchive/baseline/JATO-2026.1-full-baseline.xlsx"
    ]
    assert result["archivedPatchDirCount"] == 1
    assert result["archivedPatchDirs"] == [
        "01_RAW_DATA/historyDataArchive/patches/2026-02"
    ]
    assert result["removedUploadSessionDirCount"] == 0
    assert result["removedUploadSessionDirs"] == []
    assert result["removedJobUploadDirCount"] == 2
    assert sorted(result["removedJobUploadDirs"]) == sorted(
        [
            "04_Processed_data/ops/jato_monthly_update_jobs/job-success/uploads",
            "04_Processed_data/ops/jato_monthly_update_jobs/job-failed/uploads",
        ]
    )
    assert result["deletedReviewDirCount"] == 0
    assert result["deletedStagingDirCount"] == 0
    assert result["deletedRefreshBackupDirCount"] == 0
    assert result["deletedArchivedBaselineCount"] == 0
    assert result["deletedArchivedPatchDirCount"] == 0
    assert not old_baseline.exists()
    assert new_baseline.exists()
    assert not old_patch_dir.exists()
    assert new_patch_dir.exists()
    assert (history_root / "baseline" / old_baseline.name).exists()
    assert (history_root / "patches" / old_patch_dir.name).exists()
    assert not success_upload.parent.exists()
    assert not failed_upload.parent.exists()

    payload = jato_monthly_update_service._load_job_state("job-success")
    assert payload["upload"]["storedPath"] is None
    failed_payload = jato_monthly_update_service._load_job_state("job-failed")
    assert failed_payload["upload"]["storedPath"] is None


def test_cautious_cleanup_removes_regenerable_artifacts_and_archives(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    raw_root = project_root / "01_RAW_DATA"
    baseline_root = raw_root / "baseline"
    patch_root = raw_root / "patches"
    history_root = raw_root / "historyDataArchive"
    job_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    processed_root = project_root / "04_Processed_data"

    baseline_root.mkdir(parents=True, exist_ok=True)
    patch_root.mkdir(parents=True, exist_ok=True)
    (history_root / "baseline").mkdir(parents=True, exist_ok=True)
    (history_root / "patches").mkdir(parents=True, exist_ok=True)

    (baseline_root / "JATO-2026.3-full-baseline.xlsx").write_bytes(b"new")
    (patch_root / "2026-03").mkdir()
    ((patch_root / "2026-03") / "new.xlsx").write_bytes(b"new-patch")
    (history_root / "baseline" / "old-archive.xlsx").write_bytes(b"archived-baseline")
    (history_root / "patches" / "2026-01").mkdir()
    ((history_root / "patches" / "2026-01") / "patch.xlsx").write_bytes(b"archived-patch")

    review_dir = processed_root / "reviews" / "raw_compare" / "2026-01_vs_2026-03"
    review_dir.mkdir(parents=True, exist_ok=True)
    (review_dir / "raw_compare_report.json").write_text("{}", encoding="utf-8")

    staging_dir = processed_root / "staging" / "2026-03-r1-mixed"
    staging_dir.mkdir(parents=True, exist_ok=True)
    (staging_dir / "manifest.json").write_text("{}", encoding="utf-8")

    backup_dir = processed_root / ".refresh_backups" / "manual-promote-1"
    backup_dir.mkdir(parents=True, exist_ok=True)
    (backup_dir / "manifest.json").write_text("{}", encoding="utf-8")

    upload_session_dir = job_root / "_upload_sessions" / "session-1"
    upload_session_dir.mkdir(parents=True, exist_ok=True)
    (upload_session_dir / "upload_state.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "RAW_DATA_ROOT", raw_root)
    monkeypatch.setattr(jato_monthly_update_service, "BASELINE_ROOT", baseline_root)
    monkeypatch.setattr(jato_monthly_update_service, "PATCHES_ROOT", patch_root)
    monkeypatch.setattr(jato_monthly_update_service, "HISTORY_ARCHIVE_ROOT", history_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    result = jato_monthly_update_service.run_jato_monthly_update_cleanup(
        triggered_by="tester",
        cleanup_tier="cautious",
    )

    assert result["cleanupTier"] == "cautious"
    assert result["removedUploadSessionDirCount"] == 1
    assert result["deletedReviewDirCount"] == 1
    assert result["deletedStagingDirCount"] == 1
    assert result["deletedRefreshBackupDirCount"] == 1
    assert result["deletedArchivedBaselineCount"] == 1
    assert result["deletedArchivedPatchDirCount"] == 1
    assert result["freedBytes"] > 0
    assert not review_dir.exists()
    assert not staging_dir.exists()
    assert not backup_dir.exists()
    assert not upload_session_dir.exists()
    assert not (history_root / "baseline" / "old-archive.xlsx").exists()
    assert not (history_root / "patches" / "2026-01").exists()


def test_promote_current_active_to_baseline_exports_snapshot_and_archives_old(
    tmp_path: Path, monkeypatch
) -> None:
    project_root, _baseline_path, _archive_path = _configure_raw_roots(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.2-full-21countries-baseline.xlsx",
    )
    processed_root = project_root / "04_Processed_data"
    processed_root.mkdir(parents=True, exist_ok=True)
    active_parquet = processed_root / "jato_full_archive.parquet"
    pd.DataFrame(
        {
            "国家": ["德国", "波兰"],
            "Make": ["BMW", "Tesla"],
            "2026 Jan": [10, 12],
            "2026 Feb": [11, None],
            "2026 Mar": [None, 14],
        }
    ).to_parquet(active_parquet, index=False)

    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )

    result = jato_monthly_update_service.promote_current_active_to_baseline(
        triggered_by="tester"
    )

    assert result["triggeredBy"] == "tester"
    assert result["detectedLatestMonth"] == "2026-03"
    assert result["countryCount"] == 2
    assert result["rowCount"] == 2
    assert result["archivedBaselineCount"] == 1
    assert result["archivedBaselines"] == [
        "01_RAW_DATA/historyDataArchive/baseline/JATO-2026.2-full-21countries-baseline.xlsx"
    ]

    promoted_path = project_root / str(result["baselinePath"])
    assert promoted_path.exists()
    exported = pd.read_excel(promoted_path, sheet_name="Data Export")
    assert exported.shape == (2, 5)
    assert list(exported.columns) == ["国家", "Make", "2026 Jan", "2026 Feb", "2026 Mar"]
    assert (
        project_root
        / "01_RAW_DATA"
        / "historyDataArchive"
        / "baseline"
        / "JATO-2026.2-full-21countries-baseline.xlsx"
    ).exists()


def test_get_monthly_update_maintenance_status_summarizes_storage(
    tmp_path: Path, monkeypatch
) -> None:
    project_root, _baseline_path, _archive_path = _configure_raw_roots(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.3-full-21countries-baseline.xlsx",
        archived_baseline_name="JATO-2026.2-full-21countries-baseline.xlsx",
    )
    processed_root = project_root / "04_Processed_data"
    patch_dir = project_root / "01_RAW_DATA" / "patches" / "2026-03-r1"
    patch_dir.mkdir(parents=True, exist_ok=True)
    (patch_dir / "patch.xlsx").write_bytes(b"patch")

    active_parquet = processed_root / "jato_full_archive.parquet"
    active_parquet.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"国家": ["德国"], "2026 Mar": [1]}).to_parquet(active_parquet, index=False)
    (processed_root / "manifest.json").write_text("{}", encoding="utf-8")
    (processed_root / "dataset_fingerprint.json").write_text("{}", encoding="utf-8")
    (processed_root / "refresh_job_report.json").write_text("{}", encoding="utf-8")
    (processed_root / "partitioned_dataset_v1" / "国家=德国").mkdir(parents=True, exist_ok=True)
    ((processed_root / "partitioned_dataset_v1" / "国家=德国") / "part-0.parquet").write_bytes(b"x")
    ((processed_root / "reviews" / "raw_compare" / "2026-03-r1") / "raw_compare_report.json").parent.mkdir(parents=True, exist_ok=True)
    ((processed_root / "reviews" / "raw_compare" / "2026-03-r1") / "raw_compare_report.json").write_text("{}", encoding="utf-8")
    ((processed_root / "staging" / "2026-03-r1-mixed") / "refresh_job_report.json").parent.mkdir(parents=True, exist_ok=True)
    ((processed_root / "staging" / "2026-03-r1-mixed") / "refresh_job_report.json").write_text("{}", encoding="utf-8")
    ((processed_root / ".refresh_backups" / "backup-a") / "manifest.json").parent.mkdir(parents=True, exist_ok=True)
    ((processed_root / ".refresh_backups" / "backup-a") / "manifest.json").write_text("{}", encoding="utf-8")

    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    upload_dir = job_root / "job-a" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    (upload_dir / "patch.xlsx").write_bytes(b"upload")
    upload_session_chunks = job_root / "_upload_sessions" / "upload-a" / "chunks"
    upload_session_chunks.mkdir(parents=True, exist_ok=True)
    (upload_session_chunks / "00001.part").write_bytes(b"chunk")

    status = jato_monthly_update_service.get_jato_monthly_update_maintenance_status()

    assert status["activeBaselinePath"] == "01_RAW_DATA/baseline/JATO-2026.3-full-21countries-baseline.xlsx"
    assert status["activeBaselineSource"] == "active"
    assert status["latestPatchBatch"] == "2026-03-r1"
    assert status["trackedStorageBytes"] > 0
    metrics = {item["key"]: item for item in status["storageMetrics"]}
    assert metrics["upload-session-cache"]["bytes"] > 0
    assert metrics["job-upload-copies"]["bytes"] > 0
    assert metrics["active-dataset"]["bytes"] > 0
    assert metrics["patch-batches"]["bytes"] > 0


def test_chunked_upload_session_can_be_completed_and_queued(
    tmp_path: Path, monkeypatch
) -> None:
    _project_root, _baseline_path, _archive_path = _configure_raw_roots(
        tmp_path,
        monkeypatch,
        archived_baseline_name="JATO-2026.1-full-baseline.xlsx",
    )
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(jato_monthly_update_service, "UPLOAD_CHUNK_SIZE_BYTES", 4)
    monkeypatch.setattr(
        jato_monthly_update_service, "_launch_job_thread", lambda job_id: None
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_detect_latest_month_from_upload",
        lambda _path: "2026-03",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-r1",
    )

    initiated = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="patch.xlsx",
        size_bytes=10,
        resume_key="resume-key-1",
        triggered_by="tester",
    )

    assert initiated["totalChunks"] == 3
    upload_id = initiated["uploadId"]
    resumed = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="patch.xlsx",
        size_bytes=10,
        resume_key="resume-key-1",
        triggered_by="tester",
    )
    assert resumed["uploadId"] == upload_id

    status = jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=1,
        content=b"abcd",
        chunk_sha256=hashlib.sha256(b"abcd").hexdigest(),
    )
    assert status["receivedChunks"] == [1]
    status = jato_monthly_update_service.get_jato_monthly_update_upload(upload_id)
    assert status["uploadedBytes"] == 4
    jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=2,
        content=b"efgh",
        chunk_sha256=hashlib.sha256(b"efgh").hexdigest(),
    )
    completed = jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=3,
        content=b"ij",
        chunk_sha256=hashlib.sha256(b"ij").hexdigest(),
    )
    assert completed["receivedChunkCount"] == 3

    assembled = jato_monthly_update_service.complete_jato_monthly_update_upload(
        upload_id=upload_id
    )
    assert assembled["status"] == "completed"
    assert assembled["uploadedBytes"] == 10
    assert assembled["fileSha256"] == hashlib.sha256(b"abcdefghij").hexdigest()

    job = jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
        upload_id=upload_id,
        triggered_by="tester",
    )

    assert job["status"] == "queued"
    assert job["month"] == "2026-03"
    assert job["batchId"] == "2026-03-r1"
    assert job["upload"]["sizeBytes"] == 10
    assert job["upload"]["sha256"] == hashlib.sha256(b"abcdefghij").hexdigest()
    assert job["artifacts"]["baselinePath"] == "01_RAW_DATA/historyDataArchive/baseline/JATO-2026.1-full-baseline.xlsx"
    assert job["artifacts"]["baselineSource"] == "archive"
    stored_path = job_root / job["jobId"] / "uploads" / "patch.xlsx"
    assert stored_path.read_bytes() == b"abcdefghij"
    assert not (job_root / "_upload_sessions" / upload_id).exists()


def test_retry_failed_job_reuses_stored_upload_copy(
    tmp_path: Path, monkeypatch
) -> None:
    project_root, baseline_path, _archive_path = _configure_raw_roots(
        tmp_path,
        monkeypatch,
        active_baseline_name="JATO-2026.2-full-baseline.xlsx",
    )
    job_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(
        jato_monthly_update_service, "_launch_job_thread", lambda job_id: None
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_detect_latest_month_from_upload",
        lambda _path: "2026-03",
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_allocate_batch_id",
        lambda month: f"{month}-r2",
    )

    source_upload = job_root / "jato-update-failed" / "uploads" / "patch.xlsx"
    source_upload.parent.mkdir(parents=True, exist_ok=True)
    source_upload.write_bytes(b"retry-me")
    failed_state = jato_monthly_update_service._prepare_initial_job_state(
        job_id="jato-update-failed",
        month="2026-03",
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=source_upload,
        file_sha256=hashlib.sha256(b"retry-me").hexdigest(),
        baseline_path=baseline_path,
        baseline_source="active",
    )
    failed_state["status"] = "failed"
    failed_state["phase"] = "failed"
    failed_state["error"] = "prepare exploded"
    jato_monthly_update_service._persist_job_state(failed_state)

    retried = jato_monthly_update_service.retry_failed_jato_monthly_update_job(
        source_job_id="jato-update-failed",
        triggered_by="retry-user",
    )

    assert retried["jobId"] != "jato-update-failed"
    assert retried["status"] == "queued"
    assert retried["month"] == "2026-03"
    assert retried["batchId"] == "2026-03-r2"
    assert retried["triggeredBy"] == "retry-user"
    assert retried["upload"]["originalFilename"] == "patch.xlsx"
    assert retried["upload"]["sha256"] == hashlib.sha256(b"retry-me").hexdigest()
    assert retried["artifacts"]["baselinePath"] == (
        "01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx"
    )
    assert retried["artifacts"]["retriedFromJobId"] == "jato-update-failed"

    retried_upload = job_root / retried["jobId"] / "uploads" / "patch.xlsx"
    assert retried_upload.exists()
    assert retried_upload.read_bytes() == b"retry-me"
    assert source_upload.read_bytes() == b"retry-me"


def test_create_job_from_upload_restores_assembled_file_when_queue_fails(
    tmp_path: Path, monkeypatch
) -> None:
    job_root = tmp_path / "jobs"
    monkeypatch.setattr(
        jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root
    )
    monkeypatch.setattr(jato_monthly_update_service, "UPLOAD_CHUNK_SIZE_BYTES", 4)

    initiated = jato_monthly_update_service.initiate_jato_monthly_update_upload(
        filename="patch.xlsx",
        size_bytes=10,
        resume_key="resume-key-fail",
        triggered_by="tester",
    )
    upload_id = initiated["uploadId"]
    jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=1,
        content=b"abcd",
        chunk_sha256=hashlib.sha256(b"abcd").hexdigest(),
    )
    jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=2,
        content=b"efgh",
        chunk_sha256=hashlib.sha256(b"efgh").hexdigest(),
    )
    jato_monthly_update_service.upload_jato_monthly_update_chunk(
        upload_id=upload_id,
        part_number=3,
        content=b"ij",
        chunk_sha256=hashlib.sha256(b"ij").hexdigest(),
    )
    assembled = jato_monthly_update_service.complete_jato_monthly_update_upload(
        upload_id=upload_id
    )
    assembled_path = jato_monthly_update_service._upload_session_assembled_path(
        upload_id,
        "patch.xlsx",
    )

    def _raise_queue_failure(**kwargs):
        raise RuntimeError("queue failed")

    monkeypatch.setattr(
        jato_monthly_update_service,
        "_queue_monthly_update_job_from_stored_upload",
        _raise_queue_failure,
    )

    try:
        jato_monthly_update_service.create_jato_monthly_update_job_from_upload(
            upload_id=upload_id,
            triggered_by="tester",
        )
    except RuntimeError as exc:
        assert str(exc) == "queue failed"
    else:
        raise AssertionError("Expected queue failure")

    assert assembled_path.exists()
    assert assembled_path.read_bytes() == b"abcdefghij"
    assert not any(path.name.startswith("jato-update-") for path in job_root.iterdir() if path.is_dir())
    upload_state = jato_monthly_update_service.get_jato_monthly_update_upload(upload_id)
    assert upload_state["status"] == "completed"


def test_get_review_bundle_reads_compare_outputs(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    job_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    job_id = "jato-update-review"
    upload_path = job_root / job_id / "uploads" / "patch.xlsx"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    upload_path.write_bytes(b"fake")

    review_dir = project_root / "04_Processed_data" / "reviews" / "raw_compare" / "2026-02_vs_2026-03"
    review_dir.mkdir(parents=True, exist_ok=True)
    (review_dir / "review_checklist.md").write_text("## checklist\n- check rows\n", encoding="utf-8")
    (review_dir / "conflict_samples.json").write_text(
        json.dumps(
            {
                "sampledCountries": ["DE", "PL"],
                "samples": [
                    {
                        "country": "DE",
                        "businessKey": {"国家": "DE", "Make": "VW", "Model": "ID.4"},
                        "oldValueDigest": "old-digest",
                        "newValueDigest": "new-digest",
                        "changedFields": ["2026 Jan", "2026 Feb"],
                    },
                    {
                        "country": "PL",
                        "businessKey": {"国家": "PL", "Make": "Skoda", "Model": "Enyaq"},
                        "oldValueDigest": "old-digest-2",
                        "newValueDigest": "new-digest-2",
                        "changedFields": ["2026 Mar"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    (review_dir / "raw_compare_report.json").write_text(
        json.dumps(
            {
                "compareId": "2026-02_vs_2026-03",
                "decisionSuggestion": "manual_review_required",
                "compareKeyColumns": ["国家", "MakeModel"],
                "reviewFindings": [
                    {
                        "severity": "review",
                        "scope": "country",
                        "target": "DE",
                        "ruleId": "price-jump",
                        "message": "Unexpected jump",
                        "metrics": {"delta": 15},
                        "suggestedAction": "inspect",
                    }
                ],
                "overlapChangeSummary": [
                    {
                        "country": "DE",
                        "compareMonths": ["2026 Jan"],
                        "compareKeyColumns": ["国家", "MakeModel"],
                        "addedRecordCount": 1,
                        "removedRecordCount": 0,
                        "changedRecordCount": 12,
                        "unchangedRecordCount": 88,
                        "changeRate": 0.12,
                        "sampleAddedKeys": [{"国家": "DE", "MakeModel": "VW ID.4"}],
                        "sampleRemovedKeys": [],
                        "sampleChangedKeys": [{"国家": "DE", "MakeModel": "VW ID.3"}],
                    }
                ],
                "countryFreshnessSummary": [
                    {
                        "country": "DE",
                        "oldLatestMonth": "2026 Jan",
                        "newLatestMonth": "2026 Mar",
                        "freshnessStatus": "advanced",
                        "rowDelta": 12,
                    }
                ],
                "countryCoverageSummary": [
                    {
                        "country": "DE",
                        "oldMonths": ["2026 Jan"],
                        "newMonths": ["2026 Jan", "2026 Feb", "2026 Mar"],
                        "addedMonths": ["2026 Feb", "2026 Mar"],
                        "removedMonths": [],
                        "overlappingMonths": ["2026 Jan"],
                        "coverageStatus": "expanded",
                    }
                ],
                "timeAxisCheck": {"hasOverlap": True},
                "countryScopeSummary": {"addedCountries": ["DE"]},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    refresh_report = project_root / "04_Processed_data" / "staging" / "2026-03-mixed" / "refresh_job_report.json"
    refresh_report.parent.mkdir(parents=True, exist_ok=True)
    refresh_report.write_text(
        json.dumps(
            {
                "jobStatus": "success",
                "jobElapsedSeconds": 22.5,
                "fullManifest": {"rows": 123, "columns": 96},
                "partitionManifest": {"parquetFileCount": 21},
                "incremental": {"fingerprintMatched": False, "fingerprintUpdated": True},
            }
        ),
        encoding="utf-8",
    )
    candidate_parquet = (
        project_root
        / "04_Processed_data"
        / "staging"
        / "2026-03-mixed"
        / "jato_full_archive.parquet"
    )
    _write_dataset_parquet(
        candidate_parquet,
        [
            {"国家": "DE", "2026 Jan": 182526, "2026 Feb": 195317, "2026 Mar": 275452},
            {"国家": "PL", "2026 Jan": 12345},
        ],
    )
    active_parquet = project_root / "04_Processed_data" / "jato_full_archive.parquet"
    _write_dataset_parquet(
        active_parquet,
        [
            {"国家": "DE", "2026 Jan": 182526, "2026 Feb": 190000, "2026 Mar": None},
            {"国家": "PL", "2026 Jan": 12345},
        ],
    )

    state = jato_monthly_update_service._prepare_initial_job_state(
        job_id=job_id,
        month="2026-03",
        triggered_by="tester",
        upload_filename="patch.xlsx",
        stored_upload_path=upload_path,
    )
    state["artifacts"] = {
        "reviewDir": "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03",
        "rawCompareReportPath": "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03/raw_compare_report.json",
        "refreshReportPath": "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json",
        "stagingOutputPath": "04_Processed_data/staging/2026-03-mixed/jato_full_archive.parquet",
    }
    jato_monthly_update_service._persist_job_state(state)

    review = jato_monthly_update_service.get_jato_monthly_update_review(job_id)

    assert review["jobId"] == job_id
    assert review["compareId"] == "2026-02_vs_2026-03"
    assert review["decisionSuggestion"] == "manual_review_required"
    assert review["compareKeyColumns"] == ["国家", "MakeModel"]
    assert review["checklistMarkdown"] == "## checklist\n- check rows\n"
    assert review["sampledCountries"] == ["DE", "PL"]
    assert review["conflictSampleCount"] == 2
    assert review["conflictSamples"][0]["country"] == "DE"
    assert review["conflictSamples"][0]["businessKey"]["Model"] == "ID.4"
    assert review["conflictSamples"][0]["changedFields"] == ["2026 Jan", "2026 Feb"]
    assert review["overlapChangeSummary"][0]["country"] == "DE"
    assert review["overlapChangeSummary"][0]["sampleChangedKeys"][0]["MakeModel"] == "VW ID.3"
    assert review["countryFreshnessSummary"][0]["newLatestMonth"] == "2026 Mar"
    assert review["countryCoverageSummary"][0]["addedMonths"] == ["2026 Feb", "2026 Mar"]
    assert review["reviewFindings"][0]["ruleId"] == "price-jump"
    assert review["refreshSummary"]["jobStatus"] == "success"
    assert review["refreshSummary"]["rowCount"] == 123
    assert review["countrySalesReferenceLabel"] == "网站当前 active"
    assert review["countryMonthlySalesError"] is None
    assert review["countryMonthlySalesSummary"][0]["country"] == "DE"
    assert review["countryMonthlySalesSummary"][0]["rows"] == [
        {
            "month": "2026 Jan",
            "referenceSales": 182526,
            "candidateSales": 182526,
            "deltaSales": 0,
            "changeStatus": "unchanged",
        },
        {
            "month": "2026 Feb",
            "referenceSales": 190000,
            "candidateSales": 195317,
            "deltaSales": 5317,
            "changeStatus": "changed",
        },
        {
            "month": "2026 Mar",
            "referenceSales": None,
            "candidateSales": 275452,
            "deltaSales": None,
            "changeStatus": "added",
        },
    ]


def test_publish_monthly_update_job_promotes_staging_outputs(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    processed_root = project_root / "04_Processed_data"
    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    active_partition = processed_root / "partitioned_dataset_v1"
    active_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        processed_root / "jato_full_archive.parquet",
        [{"国家": "瑞典", "Model": "EX30", "2026 Jan": 10}],
    )
    (processed_root / "manifest.json").write_text('{"version":"old"}', encoding="utf-8")
    (processed_root / "dataset_fingerprint.json").write_text('{"hash":"old"}', encoding="utf-8")
    (processed_root / "refresh_job_report.json").write_text('{"jobStatus":"old"}', encoding="utf-8")
    (active_partition / "part-000.parquet").write_text("old-partition", encoding="utf-8")

    staging_root = processed_root / "staging" / "2026-03-mixed"
    staging_partition = staging_root / "partitioned_dataset_v1"
    staging_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        staging_root / "jato_full_archive.parquet",
        [{"国家": "瑞典", "Model": "EX30", "2026 Jan": 10, "2026 Mar": 12}],
    )
    (staging_root / "manifest.json").write_text('{"version":"new"}', encoding="utf-8")
    (staging_root / "dataset_fingerprint.json").write_text('{"hash":"new"}', encoding="utf-8")
    (staging_root / "refresh_job_report.json").write_text('{"jobStatus":"success"}', encoding="utf-8")
    (staging_partition / "part-000.parquet").write_text("new-partition", encoding="utf-8")

    job_id = "jato-update-publish"
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
    state["artifacts"] = {
        "stagingOutputPath": "04_Processed_data/staging/2026-03-mixed/jato_full_archive.parquet",
        "manifestPath": "04_Processed_data/staging/2026-03-mixed/manifest.json",
        "partitionOutputPath": "04_Processed_data/staging/2026-03-mixed/partitioned_dataset_v1",
        "fingerprintPath": "04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json",
        "refreshReportPath": "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json",
    }
    state["summaries"] = {"refresh": {"jobStatus": "success"}}
    jato_monthly_update_service._persist_job_state(state)
    evidence_calls = []
    cache_invalidation = {
        "marketScanDeckLocal": {"enabled": True, "clearedCount": 1},
        "marketScanDeckRedis": {"enabled": True, "deletedCount": 2},
    }
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_invalidate_jato_publish_runtime_caches",
        lambda: cache_invalidation,
    )
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_write_jato_publish_cache_invalidation_evidence",
        lambda **kwargs: evidence_calls.append(kwargs),
    )

    published = jato_monthly_update_service.publish_jato_monthly_update_job(
        job_id=job_id,
        triggered_by="publisher",
    )

    assert published["publication"]["publishedBy"] == "publisher"
    assert published["publication"]["backupDir"].startswith("04_Processed_data/.refresh_backups/manual-promote-")
    assert published["publication"]["cacheInvalidation"] == cache_invalidation
    assert evidence_calls[0]["job_id"] == job_id
    assert evidence_calls[0]["cache_invalidation"] == cache_invalidation
    assert pd.read_parquet(processed_root / "jato_full_archive.parquet").to_dict("records") == [
        {"国家": "瑞典", "Model": "EX30", "2026 Jan": 10, "2026 Mar": 12}
    ]
    assert (processed_root / "manifest.json").read_text(encoding="utf-8") == '{"version":"new"}'
    assert (processed_root / "dataset_fingerprint.json").read_text(encoding="utf-8") == '{"hash":"new"}'
    assert (processed_root / "refresh_job_report.json").read_text(encoding="utf-8") == '{"jobStatus":"success"}'
    assert (active_partition / "part-000.parquet").read_text(encoding="utf-8") == "new-partition"

    backup_dir = project_root / published["publication"]["backupDir"]
    assert pd.read_parquet(backup_dir / "jato_full_archive.parquet").to_dict("records") == [
        {"国家": "瑞典", "Model": "EX30", "2026 Jan": 10}
    ]
    assert (backup_dir / "manifest.json").read_text(encoding="utf-8") == '{"version":"old"}'
    assert (backup_dir / "dataset_fingerprint.json").read_text(encoding="utf-8") == '{"hash":"old"}'
    assert (backup_dir / "refresh_job_report.json").read_text(encoding="utf-8") == '{"jobStatus":"old"}'
    assert (backup_dir / "partitioned_dataset_v1" / "part-000.parquet").read_text(encoding="utf-8") == "old-partition"


def test_publish_monthly_update_job_blocks_country_regression(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    processed_root = project_root / "04_Processed_data"
    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    active_partition = processed_root / "partitioned_dataset_v1"
    active_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        processed_root / "jato_full_archive.parquet",
        [
            {"国家": "瑞典", "Model": "EX30", "2026 Jan": 10, "2026 Mar": 20},
            {"国家": "德国", "Model": "ID.4", "2026 Jan": 11, "2026 Mar": 21},
        ],
    )
    (processed_root / "manifest.json").write_text(
        '{"version":"active"}',
        encoding="utf-8",
    )
    (processed_root / "dataset_fingerprint.json").write_text(
        '{"hash":"active"}',
        encoding="utf-8",
    )
    (processed_root / "refresh_job_report.json").write_text(
        '{"jobStatus":"active"}',
        encoding="utf-8",
    )
    (active_partition / "part-000.parquet").write_text(
        "active-partition",
        encoding="utf-8",
    )

    staging_root = processed_root / "staging" / "2026-03-mixed"
    staging_partition = staging_root / "partitioned_dataset_v1"
    staging_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        staging_root / "jato_full_archive.parquet",
        [
            {"国家": "瑞典", "Model": "EX30", "2026 Jan": 10},
            {"国家": "德国", "Model": "ID.4", "2026 Jan": 11, "2026 Mar": 21},
        ],
    )
    (staging_root / "manifest.json").write_text(
        '{"version":"candidate"}',
        encoding="utf-8",
    )
    (staging_root / "dataset_fingerprint.json").write_text(
        '{"hash":"candidate"}',
        encoding="utf-8",
    )
    (staging_root / "refresh_job_report.json").write_text(
        '{"jobStatus":"success"}',
        encoding="utf-8",
    )
    (staging_partition / "part-000.parquet").write_text(
        "candidate-partition",
        encoding="utf-8",
    )

    job_id = "jato-update-regression"
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
    state["artifacts"] = {
        "stagingOutputPath": "04_Processed_data/staging/2026-03-mixed/jato_full_archive.parquet",
        "manifestPath": "04_Processed_data/staging/2026-03-mixed/manifest.json",
        "partitionOutputPath": "04_Processed_data/staging/2026-03-mixed/partitioned_dataset_v1",
        "fingerprintPath": "04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json",
        "refreshReportPath": "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json",
    }
    state["summaries"] = {"refresh": {"jobStatus": "success"}}
    jato_monthly_update_service._persist_job_state(state)

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.publish_jato_monthly_update_job(
            job_id=job_id,
            triggered_by="publisher",
        )

    assert exc_info.value.status_code == 409
    assert "瑞典" in str(exc_info.value.detail)
    assert "2026 Mar -> 2026 Jan" in str(exc_info.value.detail)


def test_publish_monthly_update_job_blocks_likely_doubled_sales(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    processed_root = project_root / "04_Processed_data"
    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    active_partition = processed_root / "partitioned_dataset_v1"
    active_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        processed_root / "jato_full_archive.parquet",
        [
            {
                "国家": "瑞典",
                "Model": "EX30",
                "2026 Jan": 16041,
                "2026 Feb": 19341,
                "2026 Mar": 26576,
            }
        ],
    )
    (processed_root / "manifest.json").write_text('{"version":"active"}', encoding="utf-8")
    (processed_root / "dataset_fingerprint.json").write_text('{"hash":"active"}', encoding="utf-8")
    (processed_root / "refresh_job_report.json").write_text('{"jobStatus":"active"}', encoding="utf-8")
    (active_partition / "part-000.parquet").write_text("active-partition", encoding="utf-8")

    staging_root = processed_root / "staging" / "2026-03-mixed"
    staging_partition = staging_root / "partitioned_dataset_v1"
    staging_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        staging_root / "jato_full_archive.parquet",
        [
            {
                "国家": "瑞典",
                "Model": "EX30",
                "2026 Jan": 32082,
                "2026 Feb": 38682,
                "2026 Mar": 53152,
            }
        ],
    )
    (staging_root / "manifest.json").write_text('{"version":"candidate"}', encoding="utf-8")
    (staging_root / "dataset_fingerprint.json").write_text('{"hash":"candidate"}', encoding="utf-8")
    (staging_root / "refresh_job_report.json").write_text('{"jobStatus":"success"}', encoding="utf-8")
    (staging_partition / "part-000.parquet").write_text("candidate-partition", encoding="utf-8")

    job_id = "jato-update-doubled"
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
    state["artifacts"] = {
        "stagingOutputPath": "04_Processed_data/staging/2026-03-mixed/jato_full_archive.parquet",
        "manifestPath": "04_Processed_data/staging/2026-03-mixed/manifest.json",
        "partitionOutputPath": "04_Processed_data/staging/2026-03-mixed/partitioned_dataset_v1",
        "fingerprintPath": "04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json",
        "refreshReportPath": "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json",
    }
    state["summaries"] = {"refresh": {"jobStatus": "success"}}
    jato_monthly_update_service._persist_job_state(state)

    with pytest.raises(HTTPException) as exc_info:
        jato_monthly_update_service.publish_jato_monthly_update_job(
            job_id=job_id,
            triggered_by="publisher",
        )

    assert exc_info.value.status_code == 409
    assert "重复合并" in str(exc_info.value.detail)
    assert "瑞典" in str(exc_info.value.detail)
    active_records = pd.read_parquet(
        processed_root / "jato_full_archive.parquet"
    ).to_dict("records")
    assert active_records == [
        {
            "国家": "瑞典",
            "Model": "EX30",
            "2026 Jan": 16041,
            "2026 Feb": 19341,
            "2026 Mar": 26576,
        }
    ]


def test_rollback_monthly_update_job_restores_publish_backup(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    processed_root = project_root / "04_Processed_data"
    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    active_partition = processed_root / "partitioned_dataset_v1"
    active_partition.mkdir(parents=True, exist_ok=True)
    (processed_root / "jato_full_archive.parquet").write_text("bad-parquet", encoding="utf-8")
    (processed_root / "manifest.json").write_text('{"version":"bad"}', encoding="utf-8")
    (processed_root / "dataset_fingerprint.json").write_text('{"hash":"bad"}', encoding="utf-8")
    (processed_root / "refresh_job_report.json").write_text('{"jobStatus":"bad"}', encoding="utf-8")
    (active_partition / "part-000.parquet").write_text("bad-partition", encoding="utf-8")

    backup_dir = processed_root / ".refresh_backups" / "manual-promote-jato-update-publish-20260420-120233"
    (backup_dir / "partitioned_dataset_v1").mkdir(parents=True, exist_ok=True)
    (backup_dir / "jato_full_archive.parquet").write_text("good-parquet", encoding="utf-8")
    (backup_dir / "manifest.json").write_text('{"version":"good"}', encoding="utf-8")
    (backup_dir / "dataset_fingerprint.json").write_text('{"hash":"good"}', encoding="utf-8")
    (backup_dir / "refresh_job_report.json").write_text('{"jobStatus":"good"}', encoding="utf-8")
    (backup_dir / "partitioned_dataset_v1" / "part-000.parquet").write_text("good-partition", encoding="utf-8")

    job_id = "jato-update-publish"
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
    state["publication"] = {
        "publishedAt": "2026-04-20T12:02:33+00:00",
        "publishedBy": "tester",
        "backupDir": "04_Processed_data/.refresh_backups/manual-promote-jato-update-publish-20260420-120233",
    }
    jato_monthly_update_service._persist_job_state(state)

    rolled_back = jato_monthly_update_service.rollback_jato_monthly_update_job(
        job_id=job_id,
        triggered_by="operator",
    )

    assert (processed_root / "jato_full_archive.parquet").read_text(encoding="utf-8") == "good-parquet"
    assert (processed_root / "manifest.json").read_text(encoding="utf-8") == '{"version":"good"}'
    assert (processed_root / "dataset_fingerprint.json").read_text(encoding="utf-8") == '{"hash":"good"}'
    assert (processed_root / "refresh_job_report.json").read_text(encoding="utf-8") == '{"jobStatus":"good"}'
    assert (active_partition / "part-000.parquet").read_text(encoding="utf-8") == "good-partition"
    assert rolled_back["publication"]["rolledBackBy"] == "operator"
    assert rolled_back["publication"]["rollbackBackupDir"].startswith(
        "04_Processed_data/.refresh_backups/restore-pre-"
    )
    restore_backup_dir = project_root / rolled_back["publication"]["rollbackBackupDir"]
    assert (restore_backup_dir / "jato_full_archive.parquet").read_text(encoding="utf-8") == "bad-parquet"


def test_publish_monthly_update_job_allows_republish_after_rollback(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    processed_root = project_root / "04_Processed_data"
    job_root = processed_root / "ops" / "jato_monthly_update_jobs"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(jato_monthly_update_service, "MONTHLY_UPDATE_JOB_ROOT", job_root)

    active_partition = processed_root / "partitioned_dataset_v1"
    active_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        processed_root / "jato_full_archive.parquet",
        [{"国家": "瑞典", "Model": "EX30", "2026 Jan": 10}],
    )
    (processed_root / "manifest.json").write_text('{"version":"old"}', encoding="utf-8")
    (processed_root / "dataset_fingerprint.json").write_text('{"hash":"old"}', encoding="utf-8")
    (processed_root / "refresh_job_report.json").write_text('{"jobStatus":"old"}', encoding="utf-8")
    (active_partition / "part-000.parquet").write_text("old-partition", encoding="utf-8")

    staging_root = processed_root / "staging" / "2026-03-mixed"
    staging_partition = staging_root / "partitioned_dataset_v1"
    staging_partition.mkdir(parents=True, exist_ok=True)
    _write_dataset_parquet(
        staging_root / "jato_full_archive.parquet",
        [{"国家": "瑞典", "Model": "EX30", "2026 Jan": 10, "2026 Mar": 12}],
    )
    (staging_root / "manifest.json").write_text('{"version":"new"}', encoding="utf-8")
    (staging_root / "dataset_fingerprint.json").write_text('{"hash":"new"}', encoding="utf-8")
    (staging_root / "refresh_job_report.json").write_text('{"jobStatus":"success"}', encoding="utf-8")
    (staging_partition / "part-000.parquet").write_text("new-partition", encoding="utf-8")

    job_id = "jato-update-republish"
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
    state["artifacts"] = {
        "stagingOutputPath": "04_Processed_data/staging/2026-03-mixed/jato_full_archive.parquet",
        "manifestPath": "04_Processed_data/staging/2026-03-mixed/manifest.json",
        "partitionOutputPath": "04_Processed_data/staging/2026-03-mixed/partitioned_dataset_v1",
        "fingerprintPath": "04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json",
        "refreshReportPath": "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json",
    }
    state["summaries"] = {"refresh": {"jobStatus": "success"}}
    state["publication"] = {
        "publishedAt": "2026-04-20T12:02:33+00:00",
        "publishedBy": "tester",
        "backupDir": "04_Processed_data/.refresh_backups/manual-promote-jato-update-republish-20260420-120233",
        "rolledBackAt": "2026-04-20T12:18:45+00:00",
        "rolledBackBy": "tester",
        "rollbackBackupDir": "04_Processed_data/.refresh_backups/restore-pre-jato-update-republish-20260420-121845",
    }
    jato_monthly_update_service._persist_job_state(state)

    published = jato_monthly_update_service.publish_jato_monthly_update_job(
        job_id=job_id,
        triggered_by="publisher",
    )

    assert published["publication"]["publishedBy"] == "publisher"
    assert published["publication"]["backupDir"].startswith(
        "04_Processed_data/.refresh_backups/manual-promote-"
    )
    assert "rolledBackAt" not in published["publication"]
