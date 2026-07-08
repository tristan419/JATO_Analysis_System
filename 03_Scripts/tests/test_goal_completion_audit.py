from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "goal_completion_audit.py"
)


def load_module():
    module_name = "goal_completion_audit_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


audit = load_module()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_statuses(repo_root: Path) -> None:
    status_dir = repo_root / "hermes" / "reports" / "pipeline_status"
    msrp_report_path = repo_root / "hermes" / "reports" / "msrp_readiness_audit.json"
    requirements = []
    for key in audit.REQUIRED_MSRP_REQUIREMENT_KEYS:
        runtime = {}
        if key == "review_queue":
            runtime = {
                "priceAlertReviewQueueSchemaVersion": (
                    audit.PRICE_ALERT_REVIEW_QUEUE_SCHEMA_VERSION
                ),
                "priceAlertReviewQueuePath": (
                    "03_Scripts/diagnostics/artifacts/msrp_price_alert_review_queue.json"
                ),
                "priceAlertReviewCaseCount": 2,
                "priceAlertReviewEffectivenessFollowUpCount": 1,
                "priceAlertReviewEffectivenessLinkedCount": 1,
                "priceAlertReviewEffectivenessMissingCount": 0,
                "priceAlertReviewQueueCovered": True,
            }
        requirements.append({
            "key": key,
            "title": key.replace("_", " ").title(),
            "status": "passed",
            "runtime": runtime,
            "evidence": ["unit"],
            "note": "unit",
        })
    _write_json(
        msrp_report_path,
        {
            "schemaVersion": "msrp_official_price_readiness_v1",
            "status": "passed",
            "requirements": requirements,
        },
    )
    _write_json(
        status_dir / "msrp_readiness_audit.json",
        {
            "pipelineId": "msrp_readiness_audit",
            "status": "success",
            "readinessStatus": "passed",
            "statusCounts": {"passed": len(audit.REQUIRED_MSRP_REQUIREMENT_KEYS)},
            "artifactRefs": ["hermes/reports/msrp_readiness_audit.json"],
        },
    )
    _write_json(
        status_dir / "unified_scraping_readiness.json",
        {
            "pipelineId": "unified_scraping_readiness",
            "status": "success",
            "readinessStatus": "passed",
            "contractStatus": "ok",
            "stageStatus": "ok",
            "jobsByKind": {"msrp": 1, "news": 1, "voc": 1},
        },
    )


def _write_source_drafts(
    source_root: Path,
    countries: tuple[str, ...] = ("se", "fi"),
    *,
    with_todo: bool = False,
) -> None:
    for country in countries:
        path = source_root / country / f"01_codex_{country}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "\n".join(
                [
                    f"country: {country.upper()}",
                    "source_code: codex",
                    "profile:",
                    "  css:",
                    "    vehicle_container: .vehicle",
                    "keywords:",
                    "  - TODO_BEV_KEYWORD" if with_todo and country == countries[0] else "  - electric",
                ]
            ),
            encoding="utf-8",
        )


def _remote_price_alert_review_queue_payload(
    *,
    follow_up_count: int = 1,
    linked_count: int = 1,
    missing_count: int = 0,
) -> dict:
    return {
        "schemaVersion": audit.PRICE_ALERT_REVIEW_QUEUE_SCHEMA_VERSION,
        "snapshotWeek": "2026-W24",
        "summary": {
            "totalCases": 2,
            "highPriorityAlertCount": 1,
            "missingEvidenceCount": 0,
            "effectivenessFollowUpCount": follow_up_count,
            "effectivenessLinkedCount": linked_count,
            "effectivenessMissingCount": missing_count,
        },
        "items": [],
    }


def test_build_report_separates_local_p0_from_unchecked_production(tmp_path: Path) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
    )

    assert report["status"] == "in_progress"
    assert report["summary"]["localP0Ready"] is True
    assert report["summary"]["msrpReady"] is True
    assert report["summary"]["unifiedReady"] is True
    by_key = {item["key"]: item for item in report["requirements"]}
    assert by_key["msrp_official_price_p0"]["status"] == "passed"
    assert by_key["msrp_finance_monthly_lease_subsidy_net"]["status"] == "passed"
    assert by_key["msrp_official_config_table_pipeline"]["status"] == "passed"
    assert by_key["msrp_auto_review_scoring"]["status"] == "passed"
    assert by_key["msrp_monitoring_events"]["status"] == "passed"
    assert by_key["msrp_pipeline_orchestration"]["status"] == "passed"
    assert by_key["msrp_price_alert_review_effectiveness_closure"]["status"] == "passed"
    assert "ai_news_voc_15_country_smoke" not in by_key
    assert by_key["production_deployment_state"]["status"] == "not_checked"
    assert report["summary"]["msrpDetailedPassedCount"] == 16
    assert report["summary"]["msrpCompletionPassedCount"] == 17
    assert report["summary"]["priceAlertReviewCaseCount"] == 2
    assert report["summary"]["priceAlertReviewEffectivenessLinkedCount"] == 1
    assert report["summary"]["priceAlertReviewEffectivenessMissingCount"] == 0


def test_source_todo_placeholders_degrade_full_goal(tmp_path: Path) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"), with_todo=True)

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
    )
    by_key = {item["key"]: item for item in report["requirements"]}

    assert by_key["msrp_21_country_source_draft_coverage"]["status"] == "degraded"
    assert by_key["msrp_21_country_source_draft_coverage"]["runtime"]["todoPlaceholderCount"] == 1


def test_remote_checks_can_mark_production_passed(monkeypatch, tmp_path: Path) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))

    def fake_fetch_json(
        url: str,
        timeout_seconds: int,
        *,
        resolve_ip: str | None = None,
    ):
        assert resolve_ip is None
        if url.endswith("/msrp/current-prices/snapshot"):
            return {
                "schemaVersion": "msrp_current_price_snapshot_v1",
                "snapshotWeek": "2026-W24",
            }, None, 200
        if url.endswith("/hermes/msrp-country-progress"):
            return {
                "status": {
                    "runId": "msrp-dryrun-20260612-013223",
                    "gateStatus": "allowed",
                    "overallPassPct": 96.4,
                },
                "priceAlertReviewQueue": _remote_price_alert_review_queue_payload(),
            }, None, 200
        if url.endswith("/hermes/pipeline/status/unified_scraping_readiness"):
            return {
                "pipelineId": "unified_scraping_readiness",
                "status": "success",
                "readinessStatus": "passed",
            }, None, 200
        if url.endswith("/msrp/monitoring/events"):
            return {
                "schemaVersion": "msrp_monitoring_events_v1",
                "summary": {"eventCount": 1, "timelineEventCount": 1, "sourceRiskCount": 0},
                "warnings": [],
            }, None, 200
        raise AssertionError(url)

    monkeypatch.setattr(audit, "_fetch_json", fake_fetch_json)

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
        remote_api_base="https://example.test/v1",
    )
    by_key = {item["key"]: item for item in report["requirements"]}

    assert by_key["production_deployment_state"]["status"] == "passed"
    assert by_key["production_deployment_state"]["runtime"]["priceAlertReviewQueue"] == {
        "schemaVersion": audit.PRICE_ALERT_REVIEW_QUEUE_SCHEMA_VERSION,
        "schemaOk": True,
        "snapshotWeek": "2026-W24",
        "totalCases": 2,
        "highPriorityAlertCount": 1,
        "missingEvidenceCount": 0,
        "effectivenessFollowUpCount": 1,
        "effectivenessLinkedCount": 1,
        "effectivenessMissingCount": 0,
        "effectivenessLinkageStatus": "ok",
    }
    assert report["status"] == "complete"


def test_remote_checks_rejects_missing_price_alert_review_effectiveness_linkage(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))

    def fake_fetch_json(
        url: str,
        timeout_seconds: int,
        *,
        resolve_ip: str | None = None,
    ):
        if url.endswith("/msrp/current-prices/snapshot"):
            return {"schemaVersion": "msrp_current_price_snapshot_v1"}, None, 200
        if url.endswith("/hermes/msrp-country-progress"):
            return {
                "status": {"gateStatus": "allowed", "overallPassPct": 96.4},
                "priceAlertReviewQueue": _remote_price_alert_review_queue_payload(
                    follow_up_count=1,
                    linked_count=0,
                    missing_count=1,
                ),
            }, None, 200
        if url.endswith("/hermes/pipeline/status/unified_scraping_readiness"):
            return {"status": "success", "readinessStatus": "passed"}, None, 200
        if url.endswith("/msrp/monitoring/events"):
            return {"schemaVersion": "msrp_monitoring_events_v1", "summary": {}}, None, 200
        raise AssertionError(url)

    monkeypatch.setattr(audit, "_fetch_json", fake_fetch_json)

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
        remote_api_base="https://example.test/v1",
    )
    production = {
        item["key"]: item for item in report["requirements"]
    }["production_deployment_state"]

    assert production["status"] == "missing"
    assert report["status"] == "in_progress"
    assert (
        production["runtime"]["priceAlertReviewQueue"][
            "effectivenessLinkageStatus"
        ]
        == "missing_linkage"
    )


def test_remote_checks_can_use_stable_progress_when_active_probe_regresses(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))

    def fake_fetch_json(
        url: str,
        timeout_seconds: int,
        *,
        resolve_ip: str | None = None,
    ):
        assert resolve_ip is None
        if url.endswith("/msrp/current-prices/snapshot"):
            return {
                "schemaVersion": "msrp_current_price_snapshot_v1",
                "snapshotWeek": "2026-W25",
            }, None, 200
        if url.endswith("/hermes/msrp-country-progress"):
            return {
                "status": {
                    "runId": "msrp-dryrun-active",
                    "gateStatus": "blocked",
                    "gateThreshold": 70,
                    "overallPassPct": 51.6,
                },
                "stableCoverage": {
                    "gateThreshold": 70,
                    "stablePassRate": 94.4,
                    "sourcePassRate": 94.4,
                    "readyCountryCount": 3,
                    "blockedCountryCount": 0,
                    "probeRegressionCount": 13,
                    "latestRunId": "msrp-dryrun-stable",
                    "activeRunId": "msrp-dryrun-active",
                    "activeRunRunning": False,
                    "activeRunPartial": False,
                },
                "priceAlertReviewQueue": _remote_price_alert_review_queue_payload(),
            }, None, 200
        if url.endswith("/hermes/pipeline/status/unified_scraping_readiness"):
            return {
                "pipelineId": "unified_scraping_readiness",
                "status": "success",
                "readinessStatus": "passed",
            }, None, 200
        if url.endswith("/msrp/monitoring/events"):
            return {"schemaVersion": "msrp_monitoring_events_v1", "summary": {}}, None, 200
        raise AssertionError(url)

    monkeypatch.setattr(audit, "_fetch_json", fake_fetch_json)

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
        remote_api_base="https://example.test/v1",
    )
    production = {
        item["key"]: item for item in report["requirements"]
    }["production_deployment_state"]
    progress = production["runtime"]["msrpCountryProgress"]

    assert production["status"] == "passed"
    assert report["status"] == "complete"
    assert progress["gateStatus"] == "blocked"
    assert progress["effectiveGateStatus"] == "allowed"
    assert progress["effectiveGateBasis"] == "stable"
    assert progress["stableCoverage"]["sourcePassRate"] == 94.4
    assert progress["stableCoverage"]["readyCountryCount"] == 3
    assert progress["stableCoverage"]["blockedCountryCount"] == 0
    assert progress["stableCoverage"]["probeRegressionCount"] == 13


def test_remote_checks_rejects_stable_progress_with_blocked_country(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))

    def fake_fetch_json(
        url: str,
        timeout_seconds: int,
        *,
        resolve_ip: str | None = None,
    ):
        if url.endswith("/msrp/current-prices/snapshot"):
            return {"schemaVersion": "msrp_current_price_snapshot_v1"}, None, 200
        if url.endswith("/hermes/msrp-country-progress"):
            return {
                "status": {
                    "gateStatus": "blocked",
                    "gateThreshold": 70,
                    "overallPassPct": 51.6,
                },
                "stableCoverage": {
                    "gateThreshold": 70,
                    "sourcePassRate": 94.4,
                    "readyCountryCount": 2,
                    "blockedCountryCount": 1,
                    "activeRunRunning": False,
                    "activeRunPartial": False,
                },
                "priceAlertReviewQueue": _remote_price_alert_review_queue_payload(),
            }, None, 200
        if url.endswith("/hermes/pipeline/status/unified_scraping_readiness"):
            return {"status": "success", "readinessStatus": "passed"}, None, 200
        if url.endswith("/msrp/monitoring/events"):
            return {"schemaVersion": "msrp_monitoring_events_v1", "summary": {}}, None, 200
        raise AssertionError(url)

    monkeypatch.setattr(audit, "_fetch_json", fake_fetch_json)

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
        remote_api_base="https://example.test/v1",
    )
    production = {
        item["key"]: item for item in report["requirements"]
    }["production_deployment_state"]
    progress = production["runtime"]["msrpCountryProgress"]

    assert production["status"] == "missing"
    assert report["status"] == "in_progress"
    assert progress["effectiveGateStatus"] == "blocked"
    assert progress["effectiveGateBasis"] == "active"


def test_remote_checks_passes_resolve_ip_to_fetcher(monkeypatch, tmp_path: Path) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))
    seen: list[tuple[str, str | None]] = []

    def fake_fetch_json(
        url: str,
        timeout_seconds: int,
        *,
        resolve_ip: str | None = None,
    ):
        seen.append((url, resolve_ip))
        if url.endswith("/msrp/current-prices/snapshot"):
            return {"schemaVersion": "msrp_current_price_snapshot_v1"}, None, 200
        if url.endswith("/hermes/msrp-country-progress"):
            return {
                "status": {"gateStatus": "allowed", "overallPassPct": 96.4},
                "priceAlertReviewQueue": _remote_price_alert_review_queue_payload(),
            }, None, 200
        if url.endswith("/hermes/pipeline/status/unified_scraping_readiness"):
            return {"status": "success", "readinessStatus": "passed"}, None, 200
        if url.endswith("/msrp/monitoring/events"):
            return {"schemaVersion": "msrp_monitoring_events_v1", "summary": {}}, None, 200
        raise AssertionError(url)

    monkeypatch.setattr(audit, "_fetch_json", fake_fetch_json)

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
        remote_api_base="https://example.test/v1",
        remote_resolve_ip="203.0.113.10",
    )
    production = {
        item["key"]: item for item in report["requirements"]
    }["production_deployment_state"]

    assert production["status"] == "passed"
    assert production["runtime"]["resolveIp"] == "203.0.113.10"
    assert production["runtime"]["msrpMonitoringEvents"]["schemaVersion"] == "msrp_monitoring_events_v1"
    assert {item[1] for item in seen} == {"203.0.113.10"}


def test_missing_msrp_detail_blocks_local_p0(tmp_path: Path) -> None:
    _write_statuses(tmp_path)
    report_path = tmp_path / "hermes" / "reports" / "msrp_readiness_audit.json"
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    report_payload["requirements"] = [
        item
        for item in report_payload["requirements"]
        if item["key"] != "finance_monthly_lease_subsidy_net"
    ]
    _write_json(report_path, report_payload)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
    )
    by_key = {item["key"]: item for item in report["requirements"]}

    assert report["summary"]["localP0Ready"] is False
    assert by_key["msrp_official_price_p0"]["status"] == "missing"
    assert by_key["msrp_finance_monthly_lease_subsidy_net"]["status"] == "missing"
    assert report["summary"]["msrpMissingRequirementKeys"] == [
        "msrp_finance_monthly_lease_subsidy_net"
    ]


def test_missing_price_alert_review_effectiveness_link_blocks_local_p0(tmp_path: Path) -> None:
    _write_statuses(tmp_path)
    report_path = tmp_path / "hermes" / "reports" / "msrp_readiness_audit.json"
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    for item in report_payload["requirements"]:
        if item["key"] != "review_queue":
            continue
        item["runtime"]["priceAlertReviewEffectivenessFollowUpCount"] = 1
        item["runtime"]["priceAlertReviewEffectivenessLinkedCount"] = 0
        item["runtime"]["priceAlertReviewEffectivenessMissingCount"] = 1
    _write_json(report_path, report_payload)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))

    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
    )
    by_key = {item["key"]: item for item in report["requirements"]}

    assert report["summary"]["localP0Ready"] is False
    assert by_key["msrp_official_price_p0"]["status"] == "missing"
    closure = by_key["msrp_price_alert_review_effectiveness_closure"]
    assert closure["status"] == "degraded"
    assert closure["runtime"]["effectivenessLinkageStatus"] == "missing_linkage"
    assert (
        "msrp_price_alert_review_effectiveness_closure"
        in report["summary"]["msrpMissingRequirementKeys"]
    )


def test_write_outputs_and_status_record(monkeypatch, tmp_path: Path) -> None:
    _write_statuses(tmp_path)
    source_root = tmp_path / "source_drafts"
    _write_source_drafts(source_root, ("se", "fi"))
    report = audit.build_goal_completion_report(
        repo_root=tmp_path,
        source_draft_dir=source_root,
        required_source_countries=("se", "fi"),
    )
    artifacts = audit.write_outputs(report, tmp_path / "reports")
    captured: dict[str, object] = {}

    def fake_write_pipeline_status(**kwargs):
        captured.update(kwargs)
        return {"pipelineId": kwargs["pipeline_id"], "status": kwargs["status"]}

    monkeypatch.setattr(audit, "write_pipeline_status", fake_write_pipeline_status)
    status_record = audit.write_status_record(
        report,
        artifact_refs=list(artifacts.values()),
    )

    assert Path(artifacts["latestJson"]).exists()
    assert Path(artifacts["latestMarkdown"]).exists()
    assert status_record == {"pipelineId": audit.PIPELINE_ID, "status": "failed"}
    assert captured["records_processed"] == 21
    assert captured["failed_count"] == 1
    assert captured["extra"]["priceAlertReviewCaseCount"] == 2
    assert captured["extra"]["priceAlertReviewEffectivenessLinkedCount"] == 1
    assert captured["extra"]["priceAlertReviewEffectivenessMissingCount"] == 0
