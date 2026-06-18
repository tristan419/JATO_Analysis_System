import importlib.util
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "03_Scripts" / "hermes" / "hermes_msrp_country_progress.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "hermes_msrp_country_progress_test_module",
        SCRIPT_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_country_progress_preserves_finance_dryrun_counts(tmp_path, monkeypatch):
    module = _load_module()
    report_path = tmp_path / "dryrun_report.json"
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260617-012812",
        "expectedCountries": ["se"],
        "observedCountries": ["se"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "passPct": 96.6,
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
                "total": 29,
                "pass": 28,
                "empty": 1,
                "fail": 0,
                "errors": 0,
                "passPct": 96.6,
                "status": "success",
                "failureBreakdown": {},
                "strategyRecommendations": {},
                "financeObservationCandidates": 1,
                "financeMonthlyPaymentCount": 1,
                "financeSemanticsCounts": {"lease_monthly": 1},
                "financeTypeCounts": {"private_lease": 1},
                "sources": [],
            },
        ],
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(module, "STATUS_FILE_PATH", tmp_path / "missing_status.json")
    monkeypatch.setattr(module, "FALLBACK_REPORT_PATH", report_path)
    monkeypatch.setattr(module, "RUNS_INDEX_PATH", tmp_path / "missing_index.json")
    monkeypatch.setattr(
        module,
        "SOURCE_REPAIR_BACKLOG_PATH",
        tmp_path / "missing_backlog.json",
    )
    reference_path = tmp_path / "msrp_source_reference_evidence.json"
    reference_path.write_text(
        json.dumps({
            "schemaVersion": "msrp_source_reference_evidence_v1",
            "generatedAt": "2026-06-17T02:16:49Z",
            "backlogRunId": "msrp-dryrun-20260617-012812",
            "referenceSource": "EVKX",
            "referencePolicy": "reference_only_review_required",
            "officialSourceRequiredForIngest": True,
            "officialIngestEligible": False,
            "summary": {
                "evidenceItemCount": 1,
                "localReferenceCount": 5,
                "missingLocalReferenceCount": 0,
                "officialIngestEligibleCount": 0,
            },
            "items": [],
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "SOURCE_REFERENCE_EVIDENCE_PATH", reference_path)

    result = module.run(str(tmp_path / "reports"))

    assert result["status"]["financeObservationCandidates"] == 1
    assert result["status"]["financeMonthlyPaymentCount"] == 1
    assert result["status"]["financeSemanticsCounts"] == {"lease_monthly": 1}
    assert result["countries"][0]["financeObservationCandidates"] == 1
    assert result["countries"][0]["financeMonthlyPaymentCount"] == 1
    markdown = (tmp_path / "reports" / "msrp_country_progress.md").read_text(
        encoding="utf-8",
    )
    assert "| Finance candidates | 1 |" in markdown
    assert "| Monthly offers | 1 |" in markdown
    assert "| se | success | 96.6% | 28 | 1 | 0 | 1 | 1 |" in markdown
    assert result["sourceReferenceEvidence"]["summary"]["localReferenceCount"] == 5
    assert "| Local references | 5 |" in markdown


def test_country_progress_keeps_stable_latest_when_active_run_regresses(tmp_path, monkeypatch):
    module = _load_module()
    latest_run_id = "msrp-dryrun-20260618-110029"
    stable_run_id = "msrp-dryrun-20260618-085225"
    artifacts = tmp_path / "artifacts"
    reports = tmp_path / "reports"
    artifacts.mkdir()

    stable_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": stable_run_id,
        "batch": "at",
        "expectedCountries": ["at"],
        "observedCountries": ["at"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
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
            },
        ],
    }
    latest_report = {
        **stable_report,
        "runId": latest_run_id,
        "summary": {
            "passPct": 50.0,
            "status": "degraded",
            "gateThreshold": 70,
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
                "strategyRecommendations": {"retry_network_or_proxy": 1},
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
            },
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
                "artifactPath": str(artifacts / f"dryrun_report_{latest_run_id}.json"),
            },
            {
                "runId": stable_run_id,
                "batch": "at",
                "finishedAt": "2026-06-18T08:52:25Z",
                "status": "success",
                "gateStatus": "allowed",
                "gateThreshold": 70,
                "artifactPath": str(artifacts / f"dryrun_report_{stable_run_id}.json"),
            },
        ],
    }

    latest_path = artifacts / f"dryrun_report_{latest_run_id}.json"
    stable_path = artifacts / f"dryrun_report_{stable_run_id}.json"
    latest_path.write_text(json.dumps(latest_report), encoding="utf-8")
    stable_path.write_text(json.dumps(stable_report), encoding="utf-8")
    (artifacts / "dryrun_report.json").write_text(
        json.dumps(latest_report),
        encoding="utf-8",
    )
    index_path = artifacts / "dryrun_runs_index.json"
    index_path.write_text(json.dumps(index), encoding="utf-8")

    monkeypatch.setattr(module, "STATUS_FILE_PATH", tmp_path / "missing_status.json")
    monkeypatch.setattr(module, "FALLBACK_REPORT_PATH", artifacts / "dryrun_report.json")
    monkeypatch.setattr(module, "RUNS_INDEX_PATH", index_path)
    monkeypatch.setattr(
        module,
        "SOURCE_REPAIR_BACKLOG_PATH",
        tmp_path / "missing_backlog.json",
    )
    monkeypatch.setattr(
        module,
        "SOURCE_REFERENCE_EVIDENCE_PATH",
        tmp_path / "missing_reference.json",
    )

    result = module.run(str(reports))

    assert result["status"]["runId"] == latest_run_id
    assert result["status"]["stableLatestRunId"] == stable_run_id
    assert result["status"]["activeRunId"] == latest_run_id
    assert result["allCountriesLatest"][0]["countryCode"] == "at"
    assert result["allCountriesLatest"][0]["runId"] == stable_run_id
    assert result["allCountriesLatest"][0]["passPct"] == 100.0
    assert result["stableCoverage"]["latestRunId"] == stable_run_id
    assert result["stableCoverage"]["activeRunId"] == latest_run_id
    assert result["stableCoverage"]["probeDiffersFromStableRun"] is True
    assert result["stableCoverage"]["probeRegressionCount"] == 1
    assert result["stableCoverage"]["probeRegressionSamples"][0]["sourceCode"] == (
        "skoda_karoq_at_draft_scrapling"
    )

    markdown = (reports / "msrp_country_progress.md").read_text(encoding="utf-8")
    assert "| Latest stable run | msrp-dryrun-20260618-085225 |" in markdown
    assert "| Probe regressions | 1 |" in markdown
