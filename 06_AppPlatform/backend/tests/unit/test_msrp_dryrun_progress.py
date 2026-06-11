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
    assert dashboard["history"][0]["runId"] == "msrp-dryrun-20260611-120000"
