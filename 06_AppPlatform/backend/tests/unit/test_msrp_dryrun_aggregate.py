"""Tests for 03_Scripts/msrp_dryrun_aggregate.py — v3 aggregation."""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT_PATH = REPO_ROOT / "03_Scripts" / "msrp_dryrun_aggregate.py"

import importlib.util
spec = importlib.util.spec_from_file_location("msrp_dryrun_aggregate_test", SCRIPT_PATH)
agg_mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(agg_mod)


def _make_country_artifact(country: str, pass_count: int, total: int, fail_reasons: dict | None = None) -> dict:
    empty = total - pass_count
    fail = 0
    errors = 0
    pass_pct = round(pass_count / total * 100, 1) if total > 0 else 0.0
    status = "success" if pass_pct >= 90 else ("degraded" if pass_pct >= 50 else "failure")
    results = [{"country": country, "code": f"source_{country}_{i}",
                "status": "empty" if i >= pass_count else "dry_run",
                "valid": 0 if i >= pass_count else 1, "extracted": 0 if i >= pass_count else 1,
                "failureReason": None if i < pass_count else "no_observation_extracted",
                "recommendedStrategy": None if i < pass_count else "diagnose",
                "elapsed": 1.0} for i in range(total)]
    return {
        "schemaVersion": "msrp_dryrun_country_v1",
        "runId": "test-run",
        "country": country,
        "total": total, "pass": pass_count, "empty": empty,
        "fail": fail, "errors": errors,
        "passPct": pass_pct, "status": status,
        "failureBreakdown": {"no_observation_extracted": empty},
        "strategyRecommendations": {"diagnose": empty},
        "results": results,
    }


def test_aggregate_missing_country_detection(tmp_path):
    """Aggregator detects missing countries."""
    run_dir = tmp_path / "run"
    countries_dir = run_dir / "countries"
    countries_dir.mkdir(parents=True)
    (countries_dir / "se.json").write_text(json.dumps(_make_country_artifact("se", 2, 5)))

    result = agg_mod.run(str(run_dir), ["se", "no"], out_latest=None)
    assert "no" in result["missingCountries"]
    assert len(result["missingCountries"]) == 1
    # Missing country should appear in countriesDetail
    no_detail = [c for c in result["countriesDetail"] if c["countryCode"] == "no"]
    assert len(no_detail) == 1
    assert no_detail[0]["status"] == "missing"


def test_aggregate_gate_status_blocked():
    """Gate status is blocked when passPct < threshold."""
    with patch.dict("os.environ", {"JATO_MSRP_MIN_DRYRUN_PASS_PCT": "70"}):
        se = _make_country_artifact("se", 2, 10)  # 20%
        run_dir = Path("/tmp/test_aggregate_gate_blocked")
        countries_dir = run_dir / "countries"
        countries_dir.mkdir(parents=True)
        (countries_dir / "se.json").write_text(json.dumps(se))

        try:
            result = agg_mod.run(str(run_dir), ["se"], out_latest=None)
            assert result["summary"]["gateStatus"] == "blocked"
            assert result["summary"]["passPct"] < 70
        finally:
            import shutil
            shutil.rmtree(run_dir, ignore_errors=True)


def test_aggregate_counts_countries():
    """Aggregator counts observed vs expected countries correctly."""
    se = _make_country_artifact("se", 5, 10)
    fi = _make_country_artifact("fi", 8, 10)
    run_dir = Path("/tmp/test_aggregate_counts")
    countries_dir = run_dir / "countries"
    countries_dir.mkdir(parents=True)
    (countries_dir / "se.json").write_text(json.dumps(se))
    (countries_dir / "fi.json").write_text(json.dumps(fi))

    try:
        result = agg_mod.run(str(run_dir), ["se", "fi", "no"], out_latest=None)
        assert len(result["observedCountries"]) == 2
        assert len(result["expectedCountries"]) == 3
        assert len(result["missingCountries"]) == 1
    finally:
        import shutil
        shutil.rmtree(run_dir, ignore_errors=True)


def test_aggregate_status_mapping():
    """Status mapping: >=90 success, >=50 degraded, <50 failure."""
    # success
    se = _make_country_artifact("se", 9, 10)  # 90%
    # degraded
    fi = _make_country_artifact("fi", 7, 10)  # 70%
    # failure
    no = _make_country_artifact("no", 4, 10)  # 40%

    run_dir = Path("/tmp/test_aggregate_status_map")
    countries_dir = run_dir / "countries"
    countries_dir.mkdir(parents=True)
    (countries_dir / "se.json").write_text(json.dumps(se))
    (countries_dir / "fi.json").write_text(json.dumps(fi))
    (countries_dir / "no.json").write_text(json.dumps(no))

    try:
        result = agg_mod.run(str(run_dir), ["se", "fi", "no"], out_latest=None)
        # Check per-country status
        for c in result["countriesDetail"]:
            if c["countryCode"] == "se":
                assert c["status"] == "success"
            elif c["countryCode"] == "fi":
                assert c["status"] == "degraded"
            elif c["countryCode"] == "no":
                assert c["status"] == "failure"
    finally:
        import shutil
        shutil.rmtree(run_dir, ignore_errors=True)
