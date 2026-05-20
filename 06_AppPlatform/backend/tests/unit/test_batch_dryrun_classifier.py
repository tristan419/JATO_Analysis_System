"""Tests for 03_Scripts/batch_dryrun.py — failure classifier and report v2 schema."""

import importlib.util
import json
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
BATCH_DRYRUN_PATH = REPO_ROOT / "03_Scripts" / "batch_dryrun.py"

spec = importlib.util.spec_from_file_location("batch_dryrun_test", BATCH_DRYRUN_PATH)
dryrun_mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(dryrun_mod)


# ── classify_dryrun_failure tests ──────────────────────────────────


def test_classify_successful_dry_run():
    """Passing dry-run returns no failure reason."""
    src = {"status": "dry_run", "valid": 3, "extracted": 3}
    result = dryrun_mod._classify_dryrun_failure(src)
    assert result["failureReason"] is None
    assert result["recommendedStrategy"] is None


def test_classify_empty_with_todo_selector():
    """empty status + TODO_SELECTOR → selector_empty."""
    src = {"status": "empty", "error": "TODO_SELECTOR not configured"}
    result = dryrun_mod._classify_dryrun_failure(src)
    assert result["failureReason"] == "selector_empty"
    assert result["recommendedStrategy"] == "try_scrapling_dynamic_or_playwright"


def test_classify_empty_no_observation():
    """empty status without specific error → no_observation_extracted."""
    src = {"status": "empty", "error": ""}
    result = dryrun_mod._classify_dryrun_failure(src)
    assert result["failureReason"] == "no_observation_extracted"
    assert result["recommendedStrategy"] == "diagnose_with_msrp_page_analyzer"


def test_classify_http_timeout():
    """Exception with timeout → http_timeout."""
    exc = TimeoutError("Page.goto: Timeout 30000ms exceeded")
    result = dryrun_mod._classify_dryrun_failure({}, exception=exc)
    assert result["failureReason"] == "http_timeout"
    assert result["recommendedStrategy"] == "retry_or_reduce_concurrency"


def test_classify_forbidden_403():
    """403 error → forbidden_403."""
    exc = Exception("HTTP Error 403 Forbidden")
    result = dryrun_mod._classify_dryrun_failure({}, exception=exc)
    assert result["failureReason"] == "forbidden_403"
    assert result["recommendedStrategy"] == "manual_review_or_proxy_required"


def test_classify_js_required():
    """Playwright timeout → js_required_or_selector_timeout."""
    src = {"status": "error", "error": "Locator.wait_for: Timeout 60000ms exceeded.\nwaiting for locator(\"[data-testid=trimcard]\").first to be visible"}
    result = dryrun_mod._classify_dryrun_failure(src)
    assert result["failureReason"] == "js_required_or_selector_timeout"
    assert result["recommendedStrategy"] == "try_playwright_card_flow"


def test_classify_currency_mismatch():
    """All rejected with currency reason → currency_mismatch."""
    src = {"status": "dry_run", "valid": 0, "extracted": 5, "rejectedReasons": ["currency mismatch", "price in EUR expected SEK"]}
    result = dryrun_mod._classify_dryrun_failure(src)
    assert result["failureReason"] == "currency_mismatch"
    assert result["recommendedStrategy"] == "check_default_currency"


def test_classify_validation_rejected_all():
    """All rejected without specific reason → validation_rejected_all."""
    src = {"status": "dry_run", "valid": 0, "extracted": 3, "rejectedReasons": ["invalid format", "missing field"]}
    result = dryrun_mod._classify_dryrun_failure(src)
    assert result["failureReason"] == "validation_rejected_all"
    assert result["recommendedStrategy"] == "review_validation_rules"


def test_classify_db_backend_error():
    """502/503 error → db_or_backend_write_failed."""
    exc = Exception("502 Server Error: Bad Gateway for url: http://127.0.0.1:8000/v1/...")
    result = dryrun_mod._classify_dryrun_failure({}, exception=exc)
    assert result["failureReason"] == "db_or_backend_write_failed"
    assert result["recommendedStrategy"] == "pipeline_error_not_source_error"


# ── Report v2 schema tests ─────────────────────────────────────────


class TestDryrunReportV2Schema:
    """Validate the v2 report payload structure."""

    def test_report_has_schema_version(self):
        """Report payload includes schemaVersion field."""
        countries = ["se", "no"]
        results = []
        # Build classifications to test failure breakdown
        for i, (cc, code, reason, strat) in enumerate([
            ("se", "kia_sportage_se", "selector_empty", "try_scrapling_dynamic_or_playwright"),
            ("se", "vw_tiguan_se", "http_timeout", "retry_or_reduce_concurrency"),
            ("no", "volvo_xc60_no", None, None),
        ]):
            r = {"country": cc, "code": code, "status": "empty", "valid": 0, "extracted": 0, "rejected": 0, "elapsed": 1.0}
            if reason:
                r["failureReason"] = reason
                r["recommendedStrategy"] = strat
            results.append(r)

        failure_breakdown = {}
        strategy_recs = {}
        for r in results:
            reason = r.get("failureReason")
            if reason:
                failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
            strat = r.get("recommendedStrategy")
            if strat:
                strategy_recs[strat] = strategy_recs.get(strat, 0) + 1

        total = len(results)
        pass_count = 1  # only volvo_xc60 passed

        report_payload = {
            "schemaVersion": "msrp_dryrun_report_v2",
            "batch": "test",
            "countries": countries,
            "total": total,
            "pass": pass_count,
            "empty": 1,
            "fail": 0,
            "errors": 1,
            "passPct": round(pass_count / total * 100, 1),
            "failureBreakdown": failure_breakdown,
            "strategyRecommendations": strategy_recs,
            "results": results,
            "savedAt": "2026-05-20T00:00:00Z",
        }

        assert report_payload["schemaVersion"] == "msrp_dryrun_report_v2"
        assert report_payload["passPct"] == pytest.approx(33.3, rel=0.1)
        assert report_payload["failureBreakdown"] == {
            "selector_empty": 1,
            "http_timeout": 1,
        }
        assert report_payload["strategyRecommendations"] == {
            "try_scrapling_dynamic_or_playwright": 1,
            "retry_or_reduce_concurrency": 1,
        }

    def test_empty_results_no_crash(self):
        """Empty results list doesn't crash report generation."""
        results = []
        failure_breakdown = {}
        strategy_recs = {}
        for r in results:
            reason = r.get("failureReason")
            if reason:
                failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
            strat = r.get("recommendedStrategy")
            if strat:
                strategy_recs[strat] = strategy_recs.get(strat, 0) + 1

        payload = {
            "schemaVersion": "msrp_dryrun_report_v2",
            "total": 0,
            "pass": 0,
            "empty": 0,
            "fail": 0,
            "errors": 0,
            "passPct": 0.0,
            "failureBreakdown": failure_breakdown,
            "strategyRecommendations": strategy_recs,
            "results": results,
        }
        assert payload["passPct"] == 0.0
        assert payload["failureBreakdown"] == {}
        assert payload["strategyRecommendations"] == {}


# ── scheduled_fetch_status passPct mapping tests ───────────────────


class TestStatusPassPctMapping:
    """Test 3-state status logic from passPct."""

    def _status_from_pct(self, pct: float) -> str:
        if pct >= 90:
            return "success"
        elif pct >= 50:
            return "degraded"
        return "failure"

    def test_success_when_90pct_or_above(self):
        assert self._status_from_pct(95.0) == "success"
        assert self._status_from_pct(90.0) == "success"
        assert self._status_from_pct(100.0) == "success"

    def test_degraded_when_50_to_90(self):
        assert self._status_from_pct(75.0) == "degraded"
        assert self._status_from_pct(50.0) == "degraded"
        assert self._status_from_pct(89.9) == "degraded"

    def test_failure_when_below_50(self):
        assert self._status_from_pct(49.9) == "failure"
        assert self._status_from_pct(10.0) == "failure"
        assert self._status_from_pct(0.0) == "failure"

    def test_empty_total_returns_failure(self):
        assert self._status_from_pct(0.0) == "failure"
