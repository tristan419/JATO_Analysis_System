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
    accessibility_path = tmp_path / "msrp_source_accessibility_audit.json"
    accessibility_path.write_text(
        json.dumps({
            "schemaVersion": "msrp_source_accessibility_audit_v1",
            "generatedAt": "2026-06-18T17:38:42Z",
            "backlogRunId": "msrp-dryrun-20260617-012812",
            "summary": {
                "sourceRepairIssueCount": 2,
                "transientRegressionCount": 0,
                "probedSourceCount": 2,
                "probeStatusCounts": {"anti_bot_blocked": 1, "network_unreachable": 1},
                "recommendedActionCounts": {
                    "official_proxy_or_configurator_api": 1,
                    "retry_network_or_proxy": 1,
                },
                "retryableNetworkCount": 1,
                "officialProxyRequiredCount": 1,
                "tlsHandshakeFailedCount": 1,
                "dnsUnresolvedCount": 0,
            },
            "items": [],
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "SOURCE_ACCESSIBILITY_AUDIT_PATH", accessibility_path)

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
    assert result["sourceAccessibilityAudit"]["summary"]["officialProxyRequiredCount"] == 1
    assert result["sourceAccessibilityAudit"]["summary"]["tlsHandshakeFailedCount"] == 1
    assert "| Local references | 5 |" in markdown
    assert "| Official proxy required | 1 |" in markdown
    assert "| TLS handshake failed | 1 |" in markdown


def test_source_repair_backlog_preserves_rejection_diagnostics():
    module = _load_module()
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260624-083348",
        "countriesDetail": [
            {
                "countryCode": "fr",
                "sources": [
                    {
                        "sourceCode": "nissan_qashqai_fr_draft_scrapling",
                        "brand": "NISSAN",
                        "sourceUrl": "https://www.nissan.fr/vehicules/neufs/qashqai.html",
                        "status": "fail",
                        "rawStatus": "dry_run",
                        "valid": 0,
                        "extracted": 1,
                        "rejected": 1,
                        "failureReason": "price_out_of_range",
                        "recommendedStrategy": "check_currency_and_price_semantics",
                        "rejectedReasons": [
                            "msrp_value=229.0 < 5000.0 for base_msrp",
                        ],
                        "rejectedRules": ["price_range"],
                        "rejectionRuleCounts": {"price_range": 1},
                        "sampleRejectedObservations": [
                            {
                                "officialModel": "QASHQAI",
                                "officialTrim": "Personnalisation et style",
                                "msrpValue": 229,
                            },
                        ],
                    }
                ],
            }
        ],
    }

    backlog = module._source_repair_backlog_from_report(
        report,
        "2026-06-24T08:33:48Z",
    )

    issue = backlog["sourceIssues"][0]
    assert issue["sourceCode"] == "nissan_qashqai_fr_draft_scrapling"
    assert issue["rejectedRules"] == ["price_range"]
    assert issue["rejectionRuleCounts"] == {"price_range": 1}
    assert (
        issue["sampleRejectedObservations"][0]["officialTrim"]
        == "Personnalisation et style"
    )
    assert backlog["groups"][0]["sourceRepairIssues"][0] == issue


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


def test_country_progress_sorts_unsorted_runs_index_for_latest_stable_country(
    tmp_path,
    monkeypatch,
):
    module = _load_module()
    artifacts = tmp_path / "artifacts"
    reports = tmp_path / "reports"
    artifacts.mkdir()

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
    (artifacts / "dryrun_report.json").write_text(
        json.dumps(newer_report),
        encoding="utf-8",
    )
    (artifacts / f"dryrun_report_{older_run_id}.json").write_text(
        json.dumps(older_report),
        encoding="utf-8",
    )
    (artifacts / f"dryrun_report_{newer_run_id}.json").write_text(
        json.dumps(newer_report),
        encoding="utf-8",
    )
    index_path = artifacts / "dryrun_runs_index.json"
    index_path.write_text(
        json.dumps({
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
        }),
        encoding="utf-8",
    )

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

    assert result["allCountriesLatest"][0]["countryCode"] == "es"
    assert result["allCountriesLatest"][0]["countryLabel"] == "Spain"
    assert result["allCountriesLatest"][0]["runId"] == newer_run_id
    assert result["stableCoverage"]["latestRunId"] == newer_run_id
