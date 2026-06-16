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


def test_aggregate_writes_history_index_and_source_repair_backlog(tmp_path):
    """Aggregator writes latest, historical, runs index, and repair backlog artifacts."""
    run_dir = tmp_path / "msrp-dryrun-20260521-033000"
    countries_dir = run_dir / "countries"
    countries_dir.mkdir(parents=True)
    artifact = _make_country_artifact("se", 2, 5)
    for result in artifact["results"][2:]:
        result["sourceUrl"] = "https://www.volvocars.com/se/build/xc60-hybrid/"
    (countries_dir / "se.json").write_text(json.dumps(artifact))

    out_latest = tmp_path / "artifacts" / "dryrun_report.json"
    result = agg_mod.run(str(run_dir), ["se"], out_latest=str(out_latest))

    assert result["schemaVersion"] == "msrp_dryrun_report_v3"
    assert out_latest.is_file()
    assert (out_latest.parent / "dryrun_report_msrp-dryrun-20260521-033000.json").is_file()

    index = json.loads((out_latest.parent / "dryrun_runs_index.json").read_text())
    assert index["latestRunId"] == "msrp-dryrun-20260521-033000"
    assert index["runs"][0]["runId"] == "msrp-dryrun-20260521-033000"
    assert index["runs"][0]["runDir"].endswith("msrp-dryrun-20260521-033000")

    backlog = json.loads((out_latest.parent / "msrp_source_repair_backlog.json").read_text())
    assert backlog["runId"] == "msrp-dryrun-20260521-033000"
    assert backlog["groups"][0]["failureReason"] == "no_observation_extracted"
    assert backlog["groups"][0]["affectedCountries"] == ["se"]
    assert backlog["topSourceHosts"][0]["host"] == "volvocars.com"
    assert backlog["topSourceHosts"][0]["count"] == 3
    assert backlog["groups"][0]["topSourceHosts"][0]["host"] == "volvocars.com"


def test_source_repair_backlog_marks_historical_pass_as_transient(tmp_path):
    """A source that passed in a prior v3 run is marked for recheck first."""
    previous_run_id = "msrp-dryrun-20260521-020000"
    current_run_dir = tmp_path / "msrp-dryrun-20260521-033000"
    countries_dir = current_run_dir / "countries"
    countries_dir.mkdir(parents=True)
    source_code = "volvo_xc40_se_draft_scrapling"
    out_dir = tmp_path / "artifacts"
    out_dir.mkdir()

    previous_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": previous_run_id,
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
                        "sourceCode": source_code,
                        "status": "pass",
                        "valid": 1,
                        "failureReason": None,
                    }
                ],
            }
        ],
        "generatedAt": "2026-05-21T02:00:00Z",
    }
    previous_path = out_dir / f"dryrun_report_{previous_run_id}.json"
    previous_path.write_text(json.dumps(previous_report))
    (out_dir / "dryrun_runs_index.json").write_text(json.dumps({
        "schemaVersion": "msrp_dryrun_runs_index_v1",
        "latestRunId": previous_run_id,
        "runs": [
            {
                "runId": previous_run_id,
                "finishedAt": "2026-05-21T02:05:00Z",
                "artifactPath": str(previous_path),
            }
        ],
    }))

    artifact = _make_country_artifact("se", 0, 1)
    artifact["results"][0].update({
        "code": source_code,
        "failureReason": "http_timeout",
        "recommendedStrategy": "retry_or_reduce_concurrency",
        "sourceUrl": "https://www.volvocars.com/se/cars/xc40-electric/",
    })
    (countries_dir / "se.json").write_text(json.dumps(artifact))

    agg_mod.run(str(current_run_dir), ["se"], out_latest=str(out_dir / "dryrun_report.json"))

    backlog = json.loads((out_dir / "msrp_source_repair_backlog.json").read_text())
    assert backlog["totalIssueCount"] == 1
    assert backlog["transientRegressionCount"] == 1
    assert backlog["sourceRepairIssueCount"] == 0
    group = backlog["groups"][0]
    assert group["recommendedAction"] == "recheck_before_source_repair"
    assert group["sampleTransientRegressions"][0]["sourceCode"] == source_code
    assert group["sampleTransientRegressions"][0]["lastKnownGoodRunId"] == previous_run_id


def test_aggregate_preserves_source_diagnostics(tmp_path):
    """Country source rows preserve diagnostics used by dashboards."""
    run_dir = tmp_path / "msrp-dryrun-20260521-040000"
    countries_dir = run_dir / "countries"
    countries_dir.mkdir(parents=True)
    artifact = _make_country_artifact("fi", 0, 1)
    artifact["results"][0].update({
        "failureReason": "dns_resolution_failed",
        "recommendedStrategy": "retry_or_check_dns",
        "extractorError": "curl: (6) Could not resolve host: www.audi.fi",
        "sourceUrl": "https://www.audi.fi/fi/web/fi/models/q4-e-tron.html",
        "httpStatus": 0,
        "finalUrl": "https://www.audi.fi/fi/web/fi/models/q4-e-tron.html",
    })
    (countries_dir / "fi.json").write_text(json.dumps(artifact))

    result = agg_mod.run(str(run_dir), ["fi"], out_latest=None)

    source = result["countriesDetail"][0]["sources"][0]
    assert source["failureReason"] == "dns_resolution_failed"
    assert source["extractorError"] == "curl: (6) Could not resolve host: www.audi.fi"
    assert source["sourceUrl"] == "https://www.audi.fi/fi/web/fi/models/q4-e-tron.html"
    assert source["finalUrl"] == "https://www.audi.fi/fi/web/fi/models/q4-e-tron.html"
    assert source["httpStatus"] == 0


def test_aggregate_recomputes_valid_success_status_as_pass(tmp_path):
    """Old country artifacts with valid non-dry_run status are normalized as pass."""
    run_dir = tmp_path / "msrp-dryrun-20260612-090000"
    countries_dir = run_dir / "countries"
    countries_dir.mkdir(parents=True)
    artifact = _make_country_artifact("se", 0, 1)
    artifact.update({
        "pass": 0,
        "empty": 0,
        "fail": 1,
        "passPct": 0.0,
        "status": "failure",
        "failureBreakdown": {},
        "strategyRecommendations": {},
    })
    artifact["results"] = [{
        "country": "se",
        "code": "polestar_4_se_draft_scrapling",
        "status": "success",
        "valid": 1,
        "extracted": 1,
        "rejected": 0,
        "elapsed": 1.0,
    }]
    (countries_dir / "se.json").write_text(json.dumps(artifact))

    result = agg_mod.run(str(run_dir), ["se"], out_latest=None)

    country = result["countriesDetail"][0]
    source = country["sources"][0]
    assert result["summary"]["pass"] == 1
    assert result["summary"]["fail"] == 0
    assert result["summary"]["passPct"] == 100.0
    assert country["pass"] == 1
    assert country["status"] == "success"
    assert source["status"] == "pass"
    assert source["rawStatus"] == "success"


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
