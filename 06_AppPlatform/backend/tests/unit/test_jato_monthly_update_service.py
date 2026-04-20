import hashlib
import json
from pathlib import Path

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


def test_parse_plan_markdown_extracts_commands_and_artifacts(tmp_path: Path) -> None:
    plan_path = tmp_path / "monthly_update_plan.md"
    plan_path.write_text(
        "\n".join(
            [
                "# 2026-03 月度更新计划",
                "",
                "- 对比: 2026-02_vs_2026-03",
                "- baseline: 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx",
                "- patch: 01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx",
                "",
                "## 步骤 1 · Raw Compare",
                "",
                "```bash",
                "python 03_Scripts/raw_compare_review.py --old 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx --new 01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx --output-dir 04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03",
                "```",
                "",
                "## 步骤 2 · Candidate Refresh",
                "",
                "```bash",
                "python 03_Scripts/data_pipeline/run_data_refresh_job.py --baseline-input 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx --patch-input-files 01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx --output 04_Processed_data/staging/2026-03-mixed/jato_full_archive.parquet --manifest 04_Processed_data/staging/2026-03-mixed/manifest.json --partition-output 04_Processed_data/staging/2026-03-mixed/partitioned_dataset_v1 --report 04_Processed_data/staging/2026-03-mixed/refresh_job_report.json --fingerprint 04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json --incremental --skip-benchmark",
                "```",
            ]
        ),
        encoding="utf-8",
    )

    parsed = jato_monthly_update_service._parse_plan_markdown(plan_path)

    assert parsed["compareId"] == "2026-02_vs_2026-03"
    assert parsed["baselinePath"] == "01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx"
    assert parsed["patchPath"] == "01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx"
    assert parsed["reviewDir"] == "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03"
    assert parsed["rawCompareReportPath"] == "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03/raw_compare_report.json"
    assert parsed["refreshReportPath"] == "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json"
    assert parsed["manifestPath"] == "04_Processed_data/staging/2026-03-mixed/manifest.json"
    assert parsed["fingerprintPath"] == "04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json"


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
                f"- 对比: {compare_id}",
                "- baseline: 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx",
                f"- patch: 01_RAW_DATA/patches/{month}/JATO-2026.3-partial.xlsx",
                "",
                "## 步骤 1 · Raw Compare",
                "",
                "```bash",
                f"python 03_Scripts/raw_compare_review.py --old 01_RAW_DATA/baseline/JATO-2026.2-full-baseline.xlsx --new 01_RAW_DATA/patches/{month}/JATO-2026.3-partial.xlsx --output-dir 04_Processed_data/reviews/raw_compare/{compare_id}",
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

    result = jato_monthly_update_service.run_jato_monthly_update_cleanup(
        triggered_by="tester"
    )

    assert result["triggeredBy"] == "tester"
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
    assert result["removedJobUploadDirCount"] == 1
    assert result["removedJobUploadDirs"] == [
        "04_Processed_data/ops/jato_monthly_update_jobs/job-success/uploads"
    ]
    assert not old_baseline.exists()
    assert new_baseline.exists()
    assert not old_patch_dir.exists()
    assert new_patch_dir.exists()
    assert (history_root / "baseline" / old_baseline.name).exists()
    assert (history_root / "patches" / old_patch_dir.name).exists()
    assert not success_upload.parent.exists()

    payload = jato_monthly_update_service._load_job_state("job-success")
    assert payload["upload"]["storedPath"] is None


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
        month="2026-03",
        upload_id=upload_id,
        triggered_by="tester",
    )

    assert job["status"] == "queued"
    assert job["month"] == "2026-03"
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
            month="2026-03",
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
