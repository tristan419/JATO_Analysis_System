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
