"""Tests for 03_Scripts/hermes/hermes_source_quality.py — dryrun artifact integration."""

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT_PATH = REPO_ROOT / "03_Scripts" / "hermes" / "hermes_source_quality.py"
SCRIPT_DIR = SCRIPT_PATH.parent

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

spec = importlib.util.spec_from_file_location("hermes_source_quality_test", SCRIPT_PATH)
sq = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(sq)


# ── _load_dryrun_artifact tests ────────────────────────────────────


def test_load_dryrun_artifact_file_not_found(tmp_path):
    """Returns None when no artifact file exists."""
    with patch.object(sq, "REPO_ROOT", tmp_path):
        result = sq._load_dryrun_artifact()
        assert result is None


def test_load_dryrun_artifact_valid_json(tmp_path):
    """Returns parsed JSON when artifact file exists with valid content."""
    artifacts_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
    artifacts_dir.mkdir(parents=True)
    artifact_path = artifacts_dir / "dryrun_report.json"
    payload = {
        "schemaVersion": "msrp_dryrun_report_v2",
        "results": [
            {"country": "se", "code": "kia_sportage_se", "failureReason": "selector_empty",
             "recommendedStrategy": "try_scrapling_dynamic_or_playwright"},
        ]
    }
    artifact_path.write_text(json.dumps(payload))

    with patch.object(sq, "REPO_ROOT", tmp_path):
        result = sq._load_dryrun_artifact()
        assert result is not None
        assert result["schemaVersion"] == "msrp_dryrun_report_v2"
        assert len(result["results"]) == 1


def test_load_dryrun_artifact_corrupted_json(tmp_path):
    """Returns None when artifact file is corrupted."""
    artifacts_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
    artifacts_dir.mkdir(parents=True)
    artifact_path = artifacts_dir / "dryrun_report.json"
    artifact_path.write_text("not valid json")

    with patch.object(sq, "REPO_ROOT", tmp_path):
        result = sq._load_dryrun_artifact()
        assert result is None


# ── _add_dryrun_source_findings tests ──────────────────────────────


def test_add_dryrun_findings_no_artifact(tmp_path):
    """Returns empty list when no dryrun artifact exists."""
    report = {"unstructuredFailures": []}
    with patch.object(sq, "_load_dryrun_artifact", return_value=None):
        findings = sq._add_dryrun_source_findings(report)
        assert findings == []
        assert "sourceLevelFindings" not in report


def test_add_dryrun_findings_empty_results(tmp_path):
    """Returns empty list when artifact has no results."""
    report = {"unstructuredFailures": []}
    artifact = {"schemaVersion": "msrp_dryrun_report_v2", "results": []}
    with patch.object(sq, "_load_dryrun_artifact", return_value=artifact):
        findings = sq._add_dryrun_source_findings(report)
        assert findings == []


def test_add_dryrun_findings_classifies_failures():
    """Adds source-level findings for failed sources, skips passing ones."""
    report = {"unstructuredFailures": []}
    artifact = {
        "results": [
            {"country": "se", "code": "kia_sportage_se", "status": "empty",
             "valid": 0, "extracted": 0, "failureReason": "selector_empty",
             "recommendedStrategy": "try_scrapling_dynamic_or_playwright", "elapsed": 5.2},
            {"country": "se", "code": "volvo_xc60_se", "status": "dry_run",
             "valid": 3, "extracted": 3, "failureReason": None,
             "recommendedStrategy": None, "elapsed": 1.0},
            {"country": "no", "code": "vw_id4_no", "status": "exception",
             "valid": 0, "extracted": 0, "failureReason": "http_timeout",
             "recommendedStrategy": "retry_or_reduce_concurrency", "elapsed": 30.5},
        ]
    }
    with patch.object(sq, "_load_dryrun_artifact", return_value=artifact):
        findings = sq._add_dryrun_source_findings(report)

    # Only failed sources should appear
    assert len(findings) == 2
    reasons = {f["sourceCode"]: f["failureReason"] for f in findings}
    assert reasons["kia_sportage_se"] == "selector_empty"
    assert reasons["vw_id4_no"] == "http_timeout"
    assert "volvo_xc60_se" not in reasons

    # Check failure breakdown was added to report
    assert "failureBreakdown" in report
    assert report["failureBreakdown"]["selector_empty"] == 1
    assert report["failureBreakdown"]["http_timeout"] == 1

    # Check unstructured failure was appended
    assert len(report["unstructuredFailures"]) == 1


def test_add_dryrun_findings_deduplicates_codes():
    """Duplicate source codes in results are skipped."""
    report = {"unstructuredFailures": []}
    artifact = {
        "results": [
            {"country": "se", "code": "kia_sportage_se", "failureReason": "selector_empty",
             "recommendedStrategy": "scrapling_dynamic", "elapsed": 5.0},
            {"country": "se", "code": "kia_sportage_se", "failureReason": "selector_empty",
             "recommendedStrategy": "scrapling_dynamic", "elapsed": 5.0},
        ]
    }
    with patch.object(sq, "_load_dryrun_artifact", return_value=artifact):
        findings = sq._add_dryrun_source_findings(report)
    assert len(findings) == 1


# ── _generate_dryrun_source_findings_table tests ───────────────────


def test_generate_table_empty():
    """Empty findings returns empty string."""
    result = sq._generate_dryrun_source_findings_table([])
    assert result == ""


def test_generate_table_has_headers():
    """Table includes expected column headers."""
    findings = [
        {"country": "se", "sourceCode": "kia_sportage_se", "status": "empty",
         "valid": 0, "extracted": 0, "failureReason": "selector_empty",
         "recommendedStrategy": "try_scrapling_dynamic", "elapsed": 5.2},
    ]
    result = sq._generate_dryrun_source_findings_table(findings)
    assert "## MSRP Source-Level Failure Breakdown" in result
    assert "Failure Reason" in result
    assert "Strategy" in result
    assert "kia_sportage_se" in result
    assert "selector_empty" in result
    assert "try_scrapling_dynamic" in result
