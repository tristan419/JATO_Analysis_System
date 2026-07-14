import fcntl
import json
from pathlib import Path

from app.services import msrp_dryrun_progress as progress


def test_existing_unlocked_lock_file_is_not_running(tmp_path, monkeypatch):
    lock_file = tmp_path / "jato-msrp-low-concurrency.lock"
    lock_file.touch()
    monkeypatch.setattr(progress, "LOCK_FILE", lock_file)

    assert progress._is_running() is False


def test_held_flock_is_running(tmp_path, monkeypatch):
    lock_file = tmp_path / "jato-msrp-low-concurrency.lock"
    lock_file.touch()
    monkeypatch.setattr(progress, "LOCK_FILE", lock_file)

    with lock_file.open("rb") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            assert progress._is_running() is True
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


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
            "financeObservationCandidates": 1,
            "financeMonthlyPaymentCount": 1,
            "financeSemanticsCounts": {"lease_monthly": 1},
            "financeTypeCounts": {"private_lease": 1},
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
                "financeObservationCandidates": 1,
                "financeMonthlyPaymentCount": 1,
                "financeSemanticsCounts": {"lease_monthly": 1},
                "financeTypeCounts": {"private_lease": 1},
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
                        "financeObservationCandidates": 1,
                        "financeMonthlyPaymentCount": 1,
                        "financeSemanticsCounts": {"lease_monthly": 1},
                        "financeTypeCounts": {"private_lease": 1},
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
                "financeObservationCandidates": 1,
                "financeMonthlyPaymentCount": 1,
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
    assert dashboard["current"]["financeObservationCandidates"] == 1
    assert dashboard["current"]["financeMonthlyPaymentCount"] == 1
    assert dashboard["current"]["countries"][0]["financeSemanticsCounts"] == {"lease_monthly": 1}
    assert dashboard["current"]["countries"][0]["sources"][0]["financeMonthlyPaymentCount"] == 1
    assert dashboard["history"][0]["runId"] == "msrp-dryrun-20260611-120000"
    assert dashboard["history"][0]["financeObservationCandidates"] == 1
    assert dashboard["history"][0]["financeMonthlyPaymentCount"] == 1
    assert [country["countryCode"] for country in dashboard["allCountries"]] == ["se"]
    assert dashboard["allCountries"][0]["financeMonthlyPaymentCount"] == 1
    assert dashboard["stableCoverage"]["countryCount"] == 1
    assert dashboard["stableCoverage"]["readyCountryCount"] == 1
    assert dashboard["stableCoverage"]["latestRunId"] == "msrp-dryrun-20260611-120000"
    assert dashboard["stableCoverage"]["sourceRowsObserved"] == 1
    assert dashboard["stableCoverage"]["sourceCount"] == 1
    assert dashboard["stableCoverage"]["readySourceCount"] == 1
    assert dashboard["stableCoverage"]["sourcePassRate"] == 100.0
    assert dashboard["stableCoverage"]["financeObservationCandidates"] == 1
    assert dashboard["stableCoverage"]["financeMonthlyPaymentCount"] == 1


def test_dashboard_exposes_latest_progress_for_all_historical_countries(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    artifacts.mkdir()
    logs.mkdir()

    latest_run_id = "msrp-dryrun-20260615-064326"
    older_run_id = "msrp-dryrun-20260614-024553"

    latest_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": latest_run_id,
        "batch": "se,fi",
        "expectedCountries": ["fi", "se"],
        "observedCountries": ["fi", "se"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "total": 2,
            "pass": 2,
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
                "sources": [],
            },
            {
                "countryCode": "fi",
                "total": 1,
                "pass": 1,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "passPct": 100.0,
                "status": "success",
                "sources": [],
            },
        ],
        "generatedAt": "2026-06-15T07:08:47Z",
    }
    older_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": older_run_id,
        "batch": "dk,no",
        "expectedCountries": ["dk", "no", "--help"],
        "observedCountries": ["dk", "no"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "total": 2,
            "pass": 1,
            "empty": 1,
            "fail": 0,
            "errors": 0,
            "passPct": 50.0,
            "status": "degraded",
            "gateThreshold": 70,
            "gateStatus": "blocked",
        },
        "countriesDetail": [
            {
                "countryCode": "dk",
                "total": 1,
                "pass": 1,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "passPct": 100.0,
                "status": "success",
                "sources": [],
            },
            {
                "countryCode": "no",
                "total": 1,
                "pass": 0,
                "empty": 1,
                "fail": 0,
                "errors": 0,
                "passPct": 0.0,
                "status": "degraded",
                "sources": [],
            },
            {
                "countryCode": "--help",
                "total": 1,
                "pass": 0,
                "empty": 1,
                "fail": 0,
                "errors": 0,
                "passPct": 0.0,
                "status": "degraded",
                "sources": [],
            },
        ],
        "generatedAt": "2026-06-14T02:45:53Z",
    }
    index = {
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "latestRunId": latest_run_id,
        "runs": [
            {
                "runId": latest_run_id,
                "batch": "se,fi",
                "finishedAt": "2026-06-15T07:08:47Z",
                "status": "success",
                "gateStatus": "allowed",
                "passPct": 100.0,
                "total": 2,
                "pass": 2,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "artifactPath": str(artifacts / f"dryrun_report_{latest_run_id}.json"),
            },
            {
                "runId": older_run_id,
                "batch": "dk,no",
                "finishedAt": "2026-06-14T02:45:53Z",
                "status": "degraded",
                "gateStatus": "blocked",
                "passPct": 50.0,
                "total": 2,
                "pass": 1,
                "empty": 1,
                "fail": 0,
                "errors": 0,
                "artifactPath": str(artifacts / f"dryrun_report_{older_run_id}.json"),
            },
        ],
    }

    (artifacts / "dryrun_report.json").write_text(json.dumps(latest_report))
    (artifacts / f"dryrun_report_{latest_run_id}.json").write_text(json.dumps(latest_report))
    (artifacts / f"dryrun_report_{older_run_id}.json").write_text(json.dumps(older_report))
    (artifacts / "dryrun_runs_index.json").write_text(json.dumps(index))

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)

    dashboard = progress.get_dryrun_dashboard()

    assert {country["countryCode"] for country in dashboard["current"]["countries"]} == {"fi", "se"}
    all_countries = {country["countryCode"]: country for country in dashboard["allCountries"]}
    assert set(all_countries) == {"dk", "fi", "no", "se"}
    assert all_countries["se"]["isLatestRun"] is True
    assert all_countries["dk"]["runId"] == older_run_id
    assert all_countries["no"]["gateStatus"] == "blocked"
    assert dashboard["stableCoverage"]["countryCount"] == 4
    assert dashboard["stableCoverage"]["readyCountryCount"] == 3
    assert dashboard["stableCoverage"]["blockedCountryCount"] == 1
    assert dashboard["stableCoverage"]["stablePassRate"] == 75.0
    assert dashboard["stableCoverage"]["readyCountries"] == ["fi", "se", "dk"]
    assert dashboard["stableCoverage"]["blockedCountries"] == ["no"]
    assert dashboard["stableCoverage"]["activeRunId"] == latest_run_id
    assert dashboard["stableCoverage"]["latestRunId"] == latest_run_id
    assert dashboard["stableCoverage"]["probeDiffersFromStableRun"] is False
    assert dashboard["stableCoverage"]["sourceRowsObserved"] == 0
    assert dashboard["stableCoverage"]["sourceCount"] == 4
    assert dashboard["stableCoverage"]["readySourceCount"] == 3
    assert dashboard["stableCoverage"]["sourcePassRate"] == 75.0


def test_dashboard_sorts_unsorted_runs_index_before_selecting_latest_stable_country(
    tmp_path,
    monkeypatch,
):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    artifacts.mkdir()
    logs.mkdir()

    older_run_id = "msrp-dryrun-20260616-010000"
    newer_run_id = "msrp-dryrun-20260618-010000"

    def make_report(run_id: str, valid: int, generated_at: str) -> dict:
        return {
            "schemaVersion": "msrp_dryrun_report_v3",
            "runId": run_id,
            "batch": "es",
            "expectedCountries": ["es"],
            "observedCountries": ["es"],
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
                    "countryCode": "es",
                    "total": 1,
                    "pass": 1,
                    "empty": 0,
                    "fail": 0,
                    "errors": 0,
                    "passPct": 100.0,
                    "status": "success",
                    "sources": [
                        {
                            "sourceCode": "seat_arona_es_draft_scrapling",
                            "status": "pass",
                            "valid": valid,
                        },
                    ],
                },
            ],
            "generatedAt": generated_at,
        }

    older_report = make_report(older_run_id, 1, "2026-06-16T01:00:00Z")
    newer_report = make_report(newer_run_id, 7, "2026-06-18T01:00:00Z")
    index = {
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "latestRunId": newer_run_id,
        "runs": [
            {
                "runId": older_run_id,
                "batch": "es",
                "finishedAt": "2026-06-16T01:00:00Z",
                "status": "success",
                "gateStatus": "allowed",
                "gateThreshold": 70,
                "artifactPath": str(artifacts / f"dryrun_report_{older_run_id}.json"),
            },
            {
                "runId": newer_run_id,
                "batch": "es",
                "finishedAt": "2026-06-18T01:00:00Z",
                "status": "success",
                "gateStatus": "allowed",
                "gateThreshold": 70,
                "artifactPath": str(artifacts / f"dryrun_report_{newer_run_id}.json"),
            },
        ],
    }
    (artifacts / "dryrun_report.json").write_text(json.dumps(newer_report))
    (artifacts / f"dryrun_report_{older_run_id}.json").write_text(
        json.dumps(older_report),
    )
    (artifacts / f"dryrun_report_{newer_run_id}.json").write_text(
        json.dumps(newer_report),
    )
    (artifacts / "dryrun_runs_index.json").write_text(json.dumps(index))

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)

    dashboard = progress.get_dryrun_dashboard()

    assert dashboard["allCountries"][0]["countryCode"] == "es"
    assert dashboard["allCountries"][0]["countryLabel"] == "Spain"
    assert dashboard["allCountries"][0]["runId"] == newer_run_id
    assert dashboard["allCountries"][0]["sources"][0]["valid"] == 7
    assert dashboard["history"][0]["runId"] == newer_run_id
    assert dashboard["stableCoverage"]["latestRunId"] == newer_run_id


def test_dashboard_keeps_latest_stable_country_when_new_probe_regresses(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    artifacts.mkdir()
    logs.mkdir()

    latest_run_id = "msrp-dryrun-20260618-110029"
    stable_run_id = "msrp-dryrun-20260618-085225"
    stable_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": stable_run_id,
        "batch": "at",
        "expectedCountries": ["at"],
        "observedCountries": ["at"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "total": 2,
            "pass": 2,
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
                "countryCode": "at",
                "total": 2,
                "pass": 2,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "passPct": 100.0,
                "status": "success",
                "sources": [
                    {
                        "sourceCode": "audi_q8_at_draft_scrapling",
                        "status": "pass",
                        "valid": 4,
                    },
                    {
                        "sourceCode": "skoda_karoq_at_draft_scrapling",
                        "status": "pass",
                        "valid": 6,
                    },
                ],
            }
        ],
    }
    latest_report = {
        **stable_report,
        "runId": latest_run_id,
        "summary": {
            **stable_report["summary"],
            "pass": 1,
            "empty": 1,
            "passPct": 50.0,
            "status": "degraded",
            "gateStatus": "blocked",
        },
        "countriesDetail": [
            {
                "countryCode": "at",
                "total": 2,
                "pass": 1,
                "empty": 1,
                "fail": 0,
                "errors": 0,
                "passPct": 50.0,
                "status": "degraded",
                "failureBreakdown": {"network_unavailable": 1},
                "sources": [
                    {
                        "sourceCode": "audi_q8_at_draft_scrapling",
                        "status": "pass",
                        "valid": 4,
                    },
                    {
                        "sourceCode": "skoda_karoq_at_draft_scrapling",
                        "status": "empty",
                        "valid": 0,
                        "failureReason": "network_unavailable",
                        "recommendedStrategy": "retry_network_or_proxy",
                    },
                ],
            }
        ],
    }
    index = {
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "latestRunId": latest_run_id,
        "runs": [
            {
                "runId": latest_run_id,
                "batch": "at",
                "finishedAt": "2026-06-18T11:00:29Z",
                "status": "degraded",
                "gateStatus": "blocked",
                "gateThreshold": 70,
                "passPct": 50.0,
                "total": 2,
                "pass": 1,
                "empty": 1,
                "fail": 0,
                "errors": 0,
                "artifactPath": str(artifacts / f"dryrun_report_{latest_run_id}.json"),
            },
            {
                "runId": stable_run_id,
                "batch": "at",
                "finishedAt": "2026-06-18T08:52:25Z",
                "status": "success",
                "gateStatus": "allowed",
                "gateThreshold": 70,
                "passPct": 100.0,
                "total": 2,
                "pass": 2,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "artifactPath": str(artifacts / f"dryrun_report_{stable_run_id}.json"),
            },
        ],
    }

    (artifacts / "dryrun_report.json").write_text(json.dumps(latest_report))
    (artifacts / f"dryrun_report_{latest_run_id}.json").write_text(json.dumps(latest_report))
    (artifacts / f"dryrun_report_{stable_run_id}.json").write_text(json.dumps(stable_report))
    (artifacts / "dryrun_runs_index.json").write_text(json.dumps(index))

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)

    dashboard = progress.get_dryrun_dashboard()

    assert dashboard["current"]["runId"] == latest_run_id
    assert dashboard["current"]["overallPassRate"] == 50.0
    assert dashboard["allCountries"][0]["countryCode"] == "at"
    assert dashboard["allCountries"][0]["runId"] == stable_run_id
    assert dashboard["allCountries"][0]["passRate"] == 100.0
    assert dashboard["allCountries"][0]["isLatestRun"] is False
    assert dashboard["stableCoverage"]["latestRunId"] == stable_run_id
    assert dashboard["stableCoverage"]["activeRunId"] == latest_run_id
    assert dashboard["stableCoverage"]["probeDiffersFromStableRun"] is True
    assert dashboard["stableCoverage"]["probeRegressionCount"] == 1
    assert dashboard["stableCoverage"]["probeRegressionSamples"][0]["sourceCode"] == (
        "skoda_karoq_at_draft_scrapling"
    )


def test_dashboard_uses_runs_index_when_latest_shortcut_is_stale_partial(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    latest_run_id = "msrp-dryrun-20260612-070207"
    run_dir = logs / latest_run_id
    artifacts.mkdir()
    run_dir.mkdir(parents=True)

    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": latest_run_id,
        "batch": "batch_a",
        "expectedCountries": ["fi"],
        "observedCountries": ["fi"],
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
                "countryCode": "fi",
                "total": 1,
                "pass": 1,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "passPct": 100.0,
                "status": "success",
                "failureBreakdown": {},
                "strategyRecommendations": {},
                "sources": [],
            }
        ],
        "results": [],
        "generatedAt": "2026-06-12T09:31:08Z",
    }
    index = {
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "latestRunId": latest_run_id,
        "runs": [
            {
                "runId": latest_run_id,
                "batch": "batch_a",
                "finishedAt": "2026-06-12T09:31:08Z",
                "status": "success",
                "gateStatus": "allowed",
                "passPct": 100.0,
                "total": 1,
                "pass": 1,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "artifactPath": str(artifacts / f"dryrun_report_{latest_run_id}.json"),
            }
        ],
    }

    (artifacts / "dryrun_report.json").write_text(json.dumps({
        "schemaVersion": "msrp_dryrun_partial_v1",
        "runId": latest_run_id,
        "running": True,
        "partial": True,
    }), encoding="utf-8")
    (artifacts / f"dryrun_report_{latest_run_id}.json").write_text(json.dumps(report), encoding="utf-8")
    (artifacts / "dryrun_runs_index.json").write_text(json.dumps(index), encoding="utf-8")
    (run_dir / "run.log").write_text("[INFO] Countries: fi no\n", encoding="utf-8")
    lock_file = tmp_path / "jato-msrp-low-concurrency.lock"
    lock_file.touch()

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", lock_file)

    dashboard = progress.get_dryrun_dashboard()

    assert dashboard["current"]["schemaVersion"] == "msrp_dryrun_report_v3"
    assert dashboard["current"]["runId"] == latest_run_id
    assert dashboard["current"]["countries"][0]["countryCode"] == "fi"
    assert dashboard["current"]["gateStatus"] == "allowed"
    assert dashboard["current"]["running"] is False
    assert dashboard["current"]["partial"] is False
    assert dashboard["current"]["finishedAt"] is not None


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
                "sources": [
                    {
                        "country": "se",
                        "sourceCode": "volvo_xc60_se_draft_scrapling",
                        "status": "pass",
                        "valid": 1,
                        "extracted": 1,
                        "rejected": 0,
                    }
                ],
            }
        ],
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
                "status": "success",
                "gateStatus": "allowed",
                "passPct": 100.0,
                "total": 1,
                "pass": 1,
                "empty": 0,
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
        "pass": 0,
        "empty": 1,
        "fail": 0,
        "errors": 0,
        "passPct": 0.0,
        "status": "failure",
        "failureBreakdown": {},
        "strategyRecommendations": {},
        "results": [{
            "country": "se",
            "sourceCode": "volvo_xc60_se_draft_scrapling",
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "rejected": 0,
            "failureReason": "http_timeout",
        }],
    }), encoding="utf-8")
    lock_file = tmp_path / "jato-msrp-low-concurrency.lock"
    lock_file.write_text("locked", encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", lock_file)

    with lock_file.open("rb") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            dashboard = progress.get_dryrun_dashboard()
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    current = dashboard["current"]

    assert current["partial"] is True
    assert current["running"] is True
    assert current["runId"] == "msrp-dryrun-20260612-125301"
    assert [country["countryCode"] for country in current["countries"]] == ["se", "fi"]
    assert dashboard["history"][0]["runId"] == "msrp-dryrun-20260612-070207"
    assert dashboard["stableCoverage"]["latestRunId"] == "msrp-dryrun-20260612-070207"
    assert dashboard["stableCoverage"]["activeRunId"] == "msrp-dryrun-20260612-125301"
    assert dashboard["stableCoverage"]["probeDiffersFromStableRun"] is True
    assert dashboard["stableCoverage"]["activeRunPartial"] is True
    assert dashboard["stableCoverage"]["probeRegressionCount"] == 1
    assert dashboard["stableCoverage"]["probeRegressionSamples"][0]["countryCode"] == "se"
    assert dashboard["stableCoverage"]["probeRegressionSamples"][0]["sourceCode"] == "volvo_xc60_se_draft_scrapling"
    assert dashboard["stableCoverage"]["probeRegressionSamples"][0]["activeStatus"] == "empty"
    assert dashboard["stableCoverage"]["probeRegressionSamples"][0]["stableRunId"] == "msrp-dryrun-20260612-070207"


def test_partial_run_dir_uses_logged_country_plan_before_country_starts(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    active_run = logs / "msrp-dryrun-20260612-125301"
    country_dir = active_run / "countries"
    artifacts.mkdir()
    country_dir.mkdir(parents=True)

    (active_run / "run.log").write_text(
        "\n".join([
            "[INFO] Countries: se fi no dk",
            "[RUN] 1/4 country=se mode=dryrun (parallel slot 1/2)",
            "[RUN] 2/4 country=fi mode=dryrun (parallel slot 2/2)",
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
            "status": "pass",
            "valid": 1,
            "extracted": 1,
            "rejected": 0,
        }],
    }), encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", tmp_path / "jato-msrp-low-concurrency.lock")

    current = progress.get_dryrun_dashboard()["current"]

    assert current["expectedCountries"] == ["se", "fi", "no", "dk"]
    assert current["observedCountries"] == ["se"]
    assert current["missingCountries"] == ["fi", "no", "dk"]
    assert [country["countryCode"] for country in current["countries"]] == ["se", "fi", "no", "dk"]
    assert [country["status"] for country in current["countries"]] == ["success", "running", "running", "running"]


def test_partial_run_dir_without_lock_is_not_marked_running(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    logs = tmp_path / "logs"
    partial_run = logs / "msrp-dryrun-20260612-125301"
    country_dir = partial_run / "countries"
    artifacts.mkdir()
    country_dir.mkdir(parents=True)

    (partial_run / "run.log").write_text(
        "\n".join([
            "[INFO] Countries: se fi",
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
            "status": "pass",
            "valid": 1,
            "extracted": 1,
            "rejected": 0,
        }],
    }), encoding="utf-8")

    monkeypatch.setattr(progress, "ARTIFACT_DIR", artifacts)
    monkeypatch.setattr(progress, "LATEST_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(progress, "RUNS_INDEX_PATH", artifacts / "dryrun_runs_index.json")
    monkeypatch.setattr(progress, "LOG_DIR", logs)
    monkeypatch.setattr(progress, "LOCK_FILE", tmp_path / "missing.lock")

    current = progress.get_dryrun_dashboard()["current"]

    assert current["partial"] is True
    assert current["running"] is False
    assert current["gateStatus"] is None
    assert current["missingCountries"] == ["fi"]


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

    with lock_file.open("rb") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            dashboard = progress.get_dryrun_dashboard()
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
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
