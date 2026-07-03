from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from typing import Any

import pytest


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "msrp_readiness_audit.py"
)


def load_module():
    module_name = "msrp_readiness_audit_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


audit_module = load_module()


@pytest.fixture(autouse=True)
def isolate_dryrun_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        audit_module,
        "DRYRUN_RUNS_INDEX_PATH",
        tmp_path / "missing_dryrun_runs_index.json",
    )
    monkeypatch.setattr(
        audit_module,
        "DRYRUN_REPORT_PATH",
        tmp_path / "missing_dryrun_report.json",
    )
    monkeypatch.setattr(
        audit_module,
        "SOURCE_REPAIR_BACKLOG_PATH",
        tmp_path / "missing_msrp_source_repair_backlog.json",
    )


class FakeReadinessClient:
    api_base = "https://example.test/v1"

    def __init__(
        self,
        *,
        missing_snapshot: bool = False,
        auth_role: str = "editor",
        missing_all_country_latest: bool = False,
        missing_monitoring: bool = False,
    ) -> None:
        self.missing_snapshot = missing_snapshot
        self.auth_role = auth_role
        self.missing_all_country_latest = missing_all_country_latest
        self.missing_monitoring = missing_monitoring

    def request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        assert method == "GET"
        assert payload is None
        if path == "/msrp/sources":
            return {"rows": 3, "items": [{}, {}, {}]}
        if path == "/msrp/sources/observations":
            return {"rows": 5, "total": 5, "items": [{}] * 5}
        if path == "/msrp/current-prices":
            return {"rows": 2, "total": 2, "items": [{}, {}]}
        if path == "/msrp/price-history":
            return {"rows": 4, "items": [{}] * 4}
        if path == "/msrp/current-prices/alerts":
            return {"rows": 1, "total": 1, "items": [{"severity": "critical"}]}
        if path == "/msrp/monitoring/events":
            if self.missing_monitoring:
                raise audit_module.SmokeFailure("monitoring 404")
            return {
                "schemaVersion": "msrp_monitoring_events_v1",
                "summary": {
                    "eventCount": 1,
                    "timelineEventCount": 2,
                    "affectedCountryCount": 2,
                    "sourceRiskCount": 1,
                    "reviewRequiredCount": 1,
                },
                "events": [{}],
                "warnings": ["unit_warning"],
            }
        if path == "/msrp/current-prices/snapshot":
            if self.missing_snapshot:
                raise audit_module.SmokeFailure("snapshot 404")
            return {
                "schemaVersion": "msrp_current_price_snapshot_v1",
                "snapshotWeek": "2026-W24",
                "summary": {
                    "currentPriceCount": 2,
                    "priceAlertCount": 1,
                },
            }
        if path == "/msrp/finance-observations":
            return {
                "rows": 1,
                "total": 1,
                "summary": {
                    "monthlyPaymentCount": 1,
                    "subsidyObservationCount": 1,
                    "netPriceAfterSubsidyCount": 1,
                },
                "items": [{}],
            }
        if path == "/auth/me":
            return {
                "username": "msrp-cron",
                "role": self.auth_role,
            }
        if path == "/msrp/reconciliation":
            return {
                "schemaVersion": "msrp_reconciliation_v1",
                "summary": {
                    "totalGroups": 2,
                    "statusCounts": {"conflict": 1, "single_source": 1},
                },
                "items": [{}, {}],
            }
        if path == "/msrp/effectiveness":
            return {
                "schemaVersion": "msrp_price_sales_effectiveness_v1",
                "summary": {
                    "analyzedEventCount": 1,
                    "labelCounts": {"positive": 1},
                },
                "items": [{}],
            }
        if path == "/hermes/msrp-country-progress":
            payload = {
                "overall": "ok",
                "status": {
                    "runId": "msrp-dryrun-test",
                    "activeRunId": "msrp-dryrun-active",
                    "stableLatestRunId": "msrp-dryrun-stable",
                    "gateStatus": "allowed",
                    "overallPassPct": 96.4,
                },
                "stableCoverage": {
                    "latestRunId": "msrp-dryrun-stable",
                    "activeRunId": "msrp-dryrun-active",
                    "countryCount": 2,
                    "probeDiffersFromStableRun": True,
                    "probeRegressionCount": 1,
                },
                "sourceRepairBacklog": {
                    "sourceRepairIssueCount": 2,
                    "transientRegressionCount": 1,
                },
            }
            if not self.missing_all_country_latest:
                payload["allCountriesLatest"] = [
                    {
                        "countryCode": "se",
                        "runId": "msrp-dryrun-stable",
                        "passPct": 100.0,
                    },
                    {
                        "countryCode": "fi",
                        "runId": "msrp-dryrun-stable",
                        "passPct": 92.8,
                    },
                ]
            return payload
        if path == "/hermes/msrp-dryrun-history":
            return {
                "latestRunId": "msrp-dryrun-test",
                "runs": [{"runId": "msrp-dryrun-test"}],
            }
        raise AssertionError(f"Unexpected request: {path}")


class ZeroSourceRepairReadinessClient(FakeReadinessClient):
    def request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = super().request_json(method, path, payload, query)
        if path == "/hermes/msrp-country-progress":
            response["sourceRepairBacklog"] = {
                "totalIssueCount": 8,
                "sourceRepairIssueCount": 0,
                "transientRegressionCount": 7,
                "externalAccessIssueCount": 1,
            }
        return response


def test_build_readiness_report_marks_complete_contract_passed() -> None:
    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(),
        filters={"country": "dk", "brand": "CODEX", "jato_model": "SMOKE"},
    )

    assert report["schemaVersion"] == audit_module.SCHEMA_VERSION
    assert report["status"] == "passed"
    assert report["summary"]["statusCounts"] == {"passed": 17}
    requirements = {
        item["key"]: item
        for item in report["requirements"]
    }
    assert requirements["official_msrp_ingest_auth"]["runtime"]["role"] == "editor"
    assert requirements["weekly_snapshot"]["runtime"]["snapshotWeek"] == "2026-W24"
    assert requirements["weekly_snapshot"]["runtime"]["scriptCovered"] is True
    assert (
        requirements["weekly_snapshot"]["runtime"][
            "archiveIncludesEffectivenessReconciliationFinance"
        ]
        is True
    )
    assert requirements["sales_effectiveness"]["runtime"]["labelCounts"] == {"positive": 1}
    monitoring_runtime = requirements["monitoring_events"]["runtime"]
    assert monitoring_runtime["schemaVersion"] == "msrp_monitoring_events_v1"
    assert monitoring_runtime["timelineEventCount"] == 2
    assert monitoring_runtime["sourceRiskCount"] == 1
    assert monitoring_runtime["warningCount"] == 1
    assert (
        audit_module.TEST_EVIDENCE["monitoringService"]
        in requirements["monitoring_events"]["evidence"]
    )
    auto_review_runtime = requirements["auto_review_scoring"]["runtime"]
    finance_runtime = requirements["finance_monthly_lease_subsidy_net"]["runtime"]
    assert auto_review_runtime["schemaVersion"] == "msrp_auto_review_score_v1"
    assert finance_runtime["financeObservationCount"] == 1
    assert (
        audit_module.TEST_EVIDENCE["snapshotScript"]
        in requirements["finance_monthly_lease_subsidy_net"]["evidence"]
    )
    config_runtime = requirements["official_config_table_pipeline"]["runtime"]
    assert config_runtime["sourceSyncStatus"] == "passed"
    assert config_runtime["schemaRefs"] == {"SpecFeatureObservation": 4}
    assert "engineering_config.vehicle_trims" in config_runtime["warehouseTables"]
    assert (
        config_runtime["landingAdapter"]
        == "spec_feature_observation_to_engineering_config_landing_v1"
    )
    assert config_runtime["landingSummary"]["vehicleTrimRows"] == 4
    dryrun_runtime = requirements["dryrun_governance"]["runtime"]
    assert dryrun_runtime["activeRunId"] == "msrp-dryrun-active"
    assert dryrun_runtime["stableLatestRunId"] == "msrp-dryrun-stable"
    assert dryrun_runtime["allCountryLatestCount"] == 2
    assert dryrun_runtime["stableCoverage"]["probeRegressionCount"] == 1
    assert dryrun_runtime["sourceRepairIssueCount"] == 2
    assert dryrun_runtime["transientRecheckCount"] == 1
    assert report["summary"]["runtimeCounts"]["dryrunAllCountryLatestCount"] == 2
    assert report["summary"]["runtimeCounts"]["dryrunSourceRepairIssueCount"] == 2
    assert report["summary"]["runtimeCounts"]["dryrunTransientRecheckCount"] == 1
    assert requirements["multi_source_reconciliation"]["runtime"]["statusCounts"]["conflict"] == 1
    assert (
        audit_module.TEST_EVIDENCE["snapshotScript"]
        in requirements["multi_source_reconciliation"]["evidence"]
    )
    pipeline_runtime = requirements["pipeline_orchestration"]["runtime"]
    assert pipeline_runtime["statusPipelineId"] == "msrp_pipeline"
    assert "unified_readiness" in pipeline_runtime["phases"]
    assert "goal_completion_audit" in pipeline_runtime["phases"]


def test_build_readiness_report_preserves_zero_source_repair_count() -> None:
    report = audit_module.build_readiness_report(
        client=ZeroSourceRepairReadinessClient(),
        filters={},
    )

    requirements = {
        item["key"]: item
        for item in report["requirements"]
    }
    dryrun_runtime = requirements["dryrun_governance"]["runtime"]
    assert dryrun_runtime["sourceRepairIssueCount"] == 0
    assert dryrun_runtime["transientRecheckCount"] == 7
    assert report["summary"]["runtimeCounts"]["dryrunSourceRepairIssueCount"] == 0
    assert report["summary"]["runtimeCounts"]["dryrunTransientRecheckCount"] == 7


def test_build_readiness_report_marks_missing_when_monitoring_events_are_unavailable() -> None:
    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(missing_monitoring=True),
        filters={},
    )

    requirements = {
        item["key"]: item
        for item in report["requirements"]
    }
    assert report["status"] == "missing"
    assert requirements["monitoring_events"]["status"] == "missing"
    assert requirements["monitoring_events"]["runtime"]["error"] == "monitoring 404"


def test_build_readiness_report_degrades_when_snapshot_is_missing() -> None:
    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(missing_snapshot=True),
        filters={},
    )

    requirements = {
        item["key"]: item
        for item in report["requirements"]
    }
    assert report["status"] == "missing"
    assert requirements["weekly_snapshot"]["status"] == "missing"
    assert requirements["weekly_snapshot"]["runtime"]["error"] == "snapshot 404"


def test_build_readiness_report_marks_viewer_token_missing_for_ingest() -> None:
    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(auth_role="viewer"),
        filters={},
    )

    requirements = {
        item["key"]: item
        for item in report["requirements"]
    }
    auth_requirement = requirements["official_msrp_ingest_auth"]
    assert report["status"] == "missing"
    assert auth_requirement["status"] == "missing"
    assert auth_requirement["runtime"]["role"] == "viewer"
    assert auth_requirement["runtime"]["requiredRole"] == "editor"
    assert auth_requirement["runtime"]["reason"] == "write_role_required"


def test_build_readiness_report_degrades_without_all_country_latest_progress() -> None:
    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(missing_all_country_latest=True),
        filters={},
    )

    requirements = {
        item["key"]: item
        for item in report["requirements"]
    }
    dryrun_requirement = requirements["dryrun_governance"]
    assert report["status"] == "degraded"
    assert dryrun_requirement["status"] == "degraded"
    assert dryrun_requirement["runtime"]["allCountryLatestCount"] == 0
    assert dryrun_requirement["runtime"]["stableLatestRunId"] == "msrp-dryrun-stable"


def test_build_readiness_report_uses_latest_dryrun_artifact_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runs_index = tmp_path / "dryrun_runs_index.json"
    dryrun_report = tmp_path / "dryrun_report.json"
    backlog = tmp_path / "msrp_source_repair_backlog.json"
    runs_index.write_text(
        json.dumps({
            "schemaVersion": "msrp_dryrun_runs_index_v1",
            "latestRunId": "msrp-dryrun-artifact-latest",
            "runs": [
                {
                    "runId": "msrp-dryrun-artifact-latest",
                    "batch": "fr",
                    "status": "success",
                    "gateStatus": "allowed",
                    "gateThreshold": 70,
                    "passPct": 96.3,
                    "total": 27,
                    "pass": 26,
                    "empty": 1,
                    "fail": 0,
                    "errors": 0,
                    "finishedAt": "2026-07-01T02:23:02Z",
                    "artifactPath": "03_Scripts/diagnostics/artifacts/dryrun_report_artifact.json",
                },
                {
                    "runId": "msrp-dryrun-se-stable",
                    "batch": "se",
                    "status": "success",
                    "gateStatus": "allowed",
                    "gateThreshold": 70,
                    "passPct": 90.0,
                    "total": 30,
                    "pass": 27,
                    "empty": 3,
                    "fail": 0,
                    "errors": 0,
                    "finishedAt": "2026-06-23T05:19:53Z",
                },
                {
                    "runId": "msrp-dryrun-batch-a-summary",
                    "batch": "batch_a",
                    "status": "success",
                    "gateStatus": "allowed",
                    "gateThreshold": 70,
                    "passPct": 80.0,
                    "total": 60,
                    "pass": 48,
                    "empty": 12,
                    "fail": 0,
                    "errors": 0,
                    "finishedAt": "2026-06-22T05:19:53Z",
                },
            ],
        }),
        encoding="utf-8",
    )
    dryrun_report.write_text(
        json.dumps({
            "schemaVersion": "msrp_dryrun_report_v3",
            "runId": "msrp-dryrun-artifact-latest",
            "summary": {
                "passPct": 96.3,
                "gateStatus": "allowed",
                "gateThreshold": 70,
            },
            "expectedCountries": ["fr"],
            "observedCountries": ["fr"],
            "missingCountries": [],
            "duplicateCountries": [],
        }),
        encoding="utf-8",
    )
    backlog.write_text(
        json.dumps({
            "schemaVersion": "msrp_source_repair_backlog_v1",
            "sourceRepairIssueCount": 1,
            "transientRegressionCount": 0,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit_module, "DRYRUN_RUNS_INDEX_PATH", runs_index)
    monkeypatch.setattr(audit_module, "DRYRUN_REPORT_PATH", dryrun_report)
    monkeypatch.setattr(audit_module, "SOURCE_REPAIR_BACKLOG_PATH", backlog)

    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(),
        filters={},
    )

    requirements = {
        item["key"]: item
        for item in report["requirements"]
    }
    dryrun_runtime = requirements["dryrun_governance"]["runtime"]
    assert dryrun_runtime["latestRunId"] == "msrp-dryrun-artifact-latest"
    assert dryrun_runtime["activeRunId"] == "msrp-dryrun-artifact-latest"
    assert dryrun_runtime["artifactFallbackUsed"] is True
    assert dryrun_runtime["allCountryLatestCount"] == 2
    assert dryrun_runtime["stableCoverage"]["readyCountries"] == ["fr", "se"]
    assert dryrun_runtime["passPct"] == 96.3
    assert report["summary"]["runtimeCounts"]["dryrunRunCount"] == 3
    assert report["summary"]["runtimeCounts"]["dryrunAllCountryLatestCount"] == 2


def test_main_prints_json_report(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        audit_module,
        "ApiClient",
        lambda **_: FakeReadinessClient(),
    )

    exit_code = audit_module.main([
        "--api-base",
        "https://example.test/v1",
        "--strict",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["summary"]["requirementCount"] == 17


def test_write_outputs_creates_latest_and_historical_artifacts(tmp_path: Path) -> None:
    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(),
        filters={},
    )

    artifacts = audit_module.write_outputs(report, tmp_path)

    assert artifacts["latestJson"] == str(tmp_path / "msrp_readiness_audit.json")
    assert artifacts["latestMarkdown"] == str(tmp_path / "msrp_readiness_audit.md")
    assert (tmp_path / "msrp_readiness_audit.json").exists()
    assert (tmp_path / "msrp_readiness_audit.md").exists()
    assert len(list(tmp_path.glob("msrp_readiness_audit_*.json"))) == 1
    assert len(list(tmp_path.glob("msrp_readiness_audit_*.md"))) == 1
    markdown = (tmp_path / "msrp_readiness_audit.md").read_text(encoding="utf-8")
    assert "# MSRP Official Price Readiness" in markdown
    assert "| weekly_snapshot | passed |" in markdown


def test_write_status_record_maps_readiness_to_pipeline_status(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    report = audit_module.build_readiness_report(
        client=FakeReadinessClient(missing_snapshot=True),
        filters={},
    )
    monkeypatch.setattr(
        audit_module,
        "write_pipeline_status",
        lambda **kwargs: calls.append(kwargs) or kwargs,
    )

    result = audit_module.write_status_record(
        report,
        started_at="2026-06-12T00:00:00Z",
        artifact_refs=["hermes/reports/msrp_readiness_audit.json"],
    )

    assert result["pipeline_id"] == "msrp_readiness_audit" or result["pipelineId"] == "msrp_readiness_audit"
    assert calls[0]["status"] == "failed"
    assert calls[0]["failed_count"] >= 1
    assert calls[0]["artifact_refs"] == ["hermes/reports/msrp_readiness_audit.json"]
