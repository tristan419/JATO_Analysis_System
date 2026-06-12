import json
from pathlib import Path

from app.services import msrp_dryrun_progress as progress


def test_dashboard_reads_v3_report_from_artifacts(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    artifacts.mkdir()
    logs.mkdir()

    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260611-120000",
        "batch": "batch_a",
        "expectedCountries": ["se"],
        "observedCountries": ["se"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "total": 1,
            "pass": 1,
            "empty": 0,
            "fail": 0,
            "errors": 0,
            "passPct": 100.0,
            "status": "success",
            "gateThreshold": 70,
            "gateStatus": "allowed",
        },
        "countriesDetail": [
            {
                "countryCode": "se",
                "total": 1,
                "pass": 1,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "passPct": 100.0,
                "status": "success",
                "failureBreakdown": {},
                "strategyRecommendations": {},
                "sources": [
                    {
                        "country": "se",
                        "sourceCode": "volvo_xc60_se_draft_scrapling",
                        "status": "pass",
                        "valid": 1,
                        "extracted": 1,
                        "rejected": 0,
                        "elapsedSeconds": 1.0,
                        "extractorError": "Page.goto: net::ERR_INTERNET_DISCONNECTED",
                        "sourceUrl": "https://www.volvocars.com/se/build/xc60-hybrid/",
                        "finalUrl": "https://www.volvocars.com/se/build/xc60-hybrid/",
                        "httpStatus": 0,
                    }
                ],
            }
        ],
        "results": [],
        "generatedAt": "2026-06-11T12:00:00Z",
    }
    index = {
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "latestRunId": "msrp-dryrun-20260611-120000",
        "runs": [
            {
                "runId": "msrp-dryrun-20260611-120000",
                "batch": "batch_a",
                "finishedAt": "2026-06-11T12:00:00Z",
                "status": "success",
                "gateStatus": "allowed",
                "passPct": 100.0,
                "total": 1,
                "pass": 1,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "artifactPath": str(artifacts / "dryrun_report_msrp-dryrun-20260611-120000.json"),
            }
        ],
    }

    (artifacts / "dryrun_report.json").write_text(json.dumps(report))
    (artifacts / "dryrun_report_msrp-dryrun-20260611-120000.json").write_text(json.dumps(report))
    (artifacts / "dryrun_runs_index.json").write_text(json.dumps(index))

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)

    dashboard = progress.get_dryrun_dashboard()

    assert dashboard["current"]["available"] is True
    assert dashboard["current"]["runId"] == "msrp-dryrun-20260611-120000"
    assert dashboard["current"]["countries"][0]["countryCode"] == "se"
    assert dashboard["current"]["countries"][0]["sources"][0]["sourceCode"] == "volvo_xc60_se_draft_scrapling"
    assert dashboard["current"]["countries"][0]["sources"][0]["extractorError"] == "Page.goto: net::ERR_INTERNET_DISCONNECTED"
    assert dashboard["current"]["countries"][0]["sources"][0]["sourceUrl"] == "https://www.volvocars.com/se/build/xc60-hybrid/"
    assert dashboard["current"]["countries"][0]["sources"][0]["finalUrl"] == "https://www.volvocars.com/se/build/xc60-hybrid/"
    assert dashboard["current"]["countries"][0]["sources"][0]["httpStatus"] == 0
    assert dashboard["history"][0]["runId"] == "msrp-dryrun-20260611-120000"


def test_dashboard_prefers_active_partial_run_over_stale_latest_report(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    active_run = logs / "msrp-dryrun-20260612-125301"
    country_dir = active_run / "countries"
    artifacts.mkdir()
    country_dir.mkdir(parents=True)

    old_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260612-070207",
        "batch": "old_batch",
        "expectedCountries": ["se"],
        "observedCountries": ["se"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "total": 1,
            "pass": 0,
            "empty": 1,
            "fail": 0,
            "errors": 0,
            "passPct": 0.0,
            "status": "failure",
            "gateThreshold": 70,
            "gateStatus": "blocked",
        },
        "countriesDetail": [],
        "generatedAt": "2026-06-12T09:31:08Z",
    }
    index = {
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "latestRunId": old_report["runId"],
        "runs": [
            {
                "runId": old_report["runId"],
                "batch": "old_batch",
                "finishedAt": "2026-06-12T09:31:08Z",
                "status": "failure",
                "gateStatus": "blocked",
                "passPct": 0.0,
                "total": 1,
                "pass": 0,
                "empty": 1,
                "fail": 0,
                "errors": 0,
                "artifactPath": str(artifacts / f"dryrun_report_{old_report['runId']}.json"),
            }
        ],
    }
    (artifacts / "dryrun_report.json").write_text(json.dumps(old_report), encoding="utf-8")
    (artifacts / f"dryrun_report_{old_report['runId']}.json").write_text(json.dumps(old_report), encoding="utf-8")
    (artifacts / "dryrun_runs_index.json").write_text(json.dumps(index), encoding="utf-8")
    (active_run / "run.log").write_text(
        "\n".join([
            "[RUN] 1/2 country=se mode=dryrun (parallel slot 1/2)",
            "[RUN] 2/2 country=fi mode=dryrun (parallel slot 2/2)",
        ]),
        encoding="utf-8",
    )
    (country_dir / "se.json").write_text(json.dumps({
        "schemaVersion": "msrp_dryrun_country_v1",
        "runId": "msrp-dryrun-20260612-125301",
        "country": "se",
        "total": 1,
        "pass": 1,
        "empty": 0,
        "fail": 0,
        "errors": 0,
        "passPct": 100.0,
        "status": "success",
        "failureBreakdown": {},
        "strategyRecommendations": {},
        "results": [{
            "country": "se",
            "sourceCode": "volvo_xc60_se_draft_scrapling",
            "status": "success",
            "valid": 1,
            "extracted": 1,
            "rejected": 0,
        }],
    }), encoding="utf-8")
    lock_file = tmp_path / "jato-msrp-low-concurrency.lock"
    lock_file.write_text("locked", encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", lock_file)

    dashboard = progress.get_dryrun_dashboard()
    current = dashboard["current"]

    assert current["partial"] is True
    assert current["running"] is True
    assert current["runId"] == "msrp-dryrun-20260612-125301"
    assert [country["countryCode"] for country in current["countries"]] == ["se", "fi"]
    assert dashboard["history"][0]["runId"] == "msrp-dryrun-20260612-070207"


def test_dashboard_uses_running_pipeline_status_when_partial_artifacts_are_pending(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    status_dir = tmp_path / "pipeline_status"
    artifacts.mkdir()
    logs.mkdir()
    status_dir.mkdir()

    old_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260612-070207",
        "batch": "old_batch",
        "expectedCountries": ["se"],
        "observedCountries": ["se"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "total": 1,
            "pass": 0,
            "empty": 1,
            "fail": 0,
            "errors": 0,
            "passPct": 0.0,
            "status": "failure",
            "gateThreshold": 70,
            "gateStatus": "blocked",
        },
        "countriesDetail": [],
        "generatedAt": "2026-06-12T09:31:08Z",
    }
    (artifacts / "dryrun_report.json").write_text(json.dumps(old_report), encoding="utf-8")
    pipeline_status_path = status_dir / "msrp_dryrun.json"
    pipeline_status_path.write_text(json.dumps({
        "pipelineId": "msrp_dryrun",
        "status": "running",
        "lastRunAt": "2026-06-12T12:53:01Z",
        "finishedAt": "2026-06-12T12:53:01Z",
        "message": "run msrp-dryrun-20260612-125301 started countries=2 concurrency=2 requested_concurrency=3",
        "metadata": {
            "countries": ["se", "fi"],
            "effectiveConcurrency": 2,
        },
    }), encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "PIPELINE_STATUS_PATH", pipeline_status_path)
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", tmp_path / "missing.lock")

    dashboard = progress.get_dryrun_dashboard()
    current = dashboard["current"]

    assert current["partial"] is True
    assert current["running"] is True
    assert current["runId"] == "msrp-dryrun-20260612-125301"
    assert current["expectedCountries"] == ["se", "fi"]
    assert current["missingCountries"] == ["se", "fi"]
    assert [country["status"] for country in current["countries"]] == ["running", "running"]


def test_dashboard_uses_pending_run_id_for_queued_pipeline_status(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    status_dir = tmp_path / "pipeline_status"
    artifacts.mkdir()
    logs.mkdir()
    status_dir.mkdir()

    old_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260612-070207",
        "summary": {
            "total": 1,
            "pass": 0,
            "empty": 1,
            "fail": 0,
            "errors": 0,
            "passPct": 0.0,
            "gateStatus": "blocked",
        },
        "countriesDetail": [],
    }
    (artifacts / "dryrun_report.json").write_text(json.dumps(old_report), encoding="utf-8")
    pipeline_status_path = status_dir / "msrp_dryrun.json"
    pipeline_status_path.write_text(json.dumps({
        "pipelineId": "msrp_dryrun",
        "status": "running",
        "lastRunAt": "2026-06-12T13:10:17Z",
        "message": "jato-msrp-sync@dryrun.service queued; dryrun artifacts pending",
    }), encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "PIPELINE_STATUS_PATH", pipeline_status_path)
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", tmp_path / "missing.lock")

    current = progress.get_dryrun_dashboard()["current"]

    assert current["partial"] is True
    assert current["running"] is True
    assert current["runId"] == "msrp-dryrun-pending"
    assert current["expectedCountries"] == []
    assert current["pipelineMessage"] == "jato-msrp-sync@dryrun.service queued; dryrun artifacts pending"


def test_dashboard_reads_partial_run_dir_country_artifacts(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    run_dir = logs / "msrp-dryrun-20260612-070207"
    country_dir = run_dir / "countries"
    artifacts.mkdir()
    country_dir.mkdir(parents=True)
    (run_dir / "run.log").write_text(
        "\n".join([
            "[RUN] 1/2 country=se mode=dryrun (parallel slot 1/2)",
            "[RUN] 2/2 country=fi mode=dryrun (parallel slot 2/2)",
        ]),
        encoding="utf-8",
    )
    (run_dir / "fi.log").write_text(
        "[ 1/2] ✅ volvo_xc60_fi_draft_scrapling valid=2 extracted=2 rejected=0 (4.0s)\n",
        encoding="utf-8",
    )
    (country_dir / "se.json").write_text(json.dumps({
        "schemaVersion": "msrp_dryrun_country_v1",
        "runId": "msrp-dryrun-20260612-070207",
        "country": "se",
        "total": 1,
        "pass": 1,
        "empty": 0,
        "fail": 0,
        "errors": 0,
        "passPct": 100.0,
        "status": "success",
        "failureBreakdown": {},
        "strategyRecommendations": {},
        "results": [{
            "country": "se",
            "sourceCode": "volvo_xc60_se_draft_scrapling",
            "status": "pass",
            "valid": 1,
            "extracted": 1,
            "rejected": 0,
            "elapsedSeconds": 2.5,
            "sourceUrl": "https://www.volvocars.com/se/",
        }],
    }), encoding="utf-8")
    lock_file = tmp_path / "jato-msrp-low-concurrency.lock"
    lock_file.write_text("locked", encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", lock_file)

    dashboard = progress.get_dryrun_dashboard()
    current = dashboard["current"]

    assert current["available"] is True
    assert current["partial"] is True
    assert current["running"] is True
    assert current["runId"] == "msrp-dryrun-20260612-070207"
    assert current["totalSources"] == 3
    assert current["totalPass"] == 2
    assert [country["countryCode"] for country in current["countries"]] == ["se", "fi"]
    assert current["countries"][0]["completed"] is True
    assert current["countries"][0]["sources"][0]["sourceUrl"] == "https://www.volvocars.com/se/"
    assert current["countries"][1]["completed"] is False
    assert current["countries"][1]["sources"][0]["sourceCode"] == "volvo_xc60_fi_draft_scrapling"


def test_partial_artifact_recomputes_valid_success_status_as_pass(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    run_dir = logs / "msrp-dryrun-20260612-090000"
    country_dir = run_dir / "countries"
    artifacts.mkdir()
    country_dir.mkdir(parents=True)
    (run_dir / "run.log").write_text(
        "[RUN] 1/1 country=se mode=dryrun (parallel slot 1/1)\n",
        encoding="utf-8",
    )
    (country_dir / "se.json").write_text(json.dumps({
        "schemaVersion": "msrp_dryrun_country_v1",
        "runId": "msrp-dryrun-20260612-090000",
        "country": "se",
        "total": 1,
        "pass": 0,
        "empty": 0,
        "fail": 1,
        "errors": 0,
        "passPct": 0.0,
        "status": "failure",
        "failureBreakdown": {},
        "strategyRecommendations": {},
        "results": [{
            "country": "se",
            "sourceCode": "polestar_4_se_draft_scrapling",
            "status": "success",
            "valid": 1,
            "extracted": 1,
            "rejected": 0,
            "elapsedSeconds": 2.5,
        }],
    }), encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", tmp_path / "missing.lock")

    dashboard = progress.get_dryrun_dashboard()
    current = dashboard["current"]
    country = current["countries"][0]
    source = country["sources"][0]

    assert current["totalPass"] == 1
    assert current["totalFail"] == 0
    assert current["overallPassRate"] == 100.0
    assert country["pass"] == 1
    assert country["fail"] == 0
    assert country["passRate"] == 100.0
    assert country["status"] == "success"
    assert source["status"] == "pass"
    assert source["rawStatus"] == "success"


def test_partial_dashboard_aggregates_expected_countries_and_failures(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    run_dir = logs / "msrp-dryrun-20260612-100000"
    country_dir = run_dir / "countries"
    artifacts.mkdir()
    country_dir.mkdir(parents=True)
    (run_dir / "run.log").write_text(
        "[RUN] 1/2 country=se mode=dryrun (parallel slot 1/2)\n",
        encoding="utf-8",
    )
    for country in ("se", "dk"):
        (country_dir / f"{country}.json").write_text(json.dumps({
            "schemaVersion": "msrp_dryrun_country_v1",
            "runId": "msrp-dryrun-20260612-100000",
            "country": country,
            "total": 1,
            "pass": 0,
            "empty": 1,
            "fail": 0,
            "errors": 0,
            "passPct": 0.0,
            "status": "failure",
            "failureBreakdown": {"http_timeout": 1},
            "strategyRecommendations": {"retry_or_reduce_concurrency": 1},
            "results": [{
                "country": country,
                "sourceCode": f"source_{country}",
                "status": "empty",
                "valid": 0,
                "extracted": 0,
                "rejected": 0,
                "failureReason": "http_timeout",
                "recommendedStrategy": "retry_or_reduce_concurrency",
            }],
        }), encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", tmp_path / "missing.lock")

    dashboard = progress.get_dryrun_dashboard()
    current = dashboard["current"]

    assert current["expectedCountries"] == ["se", "dk"]
    assert current["observedCountries"] == ["se", "dk"]
    assert current["missingCountries"] == []
    assert current["failureBreakdown"] == {"http_timeout": 2}
    assert current["strategyRecommendations"] == {"retry_or_reduce_concurrency": 2}
