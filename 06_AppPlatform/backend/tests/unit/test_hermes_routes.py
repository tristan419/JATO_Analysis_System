"""Unit tests for Hermes governance API endpoints.

Tests the three new/updated endpoints:
  - GET /hermes/gaps
  - GET /hermes/markdown-diagrams
  - GET /hermes/evidence-ledger
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from fastapi.testclient import TestClient

from app.api.routes.hermes import (
    MERMAID_BLOCK_RE,
    _md_diagrams_cache,
    _msrp_progress_from_partial_current,
    router,
)


@pytest.fixture
def client():
    """Return a TestClient with only the hermes router mounted."""
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


# ── helpers ──────────────────────────────────────────────────────────

def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data))


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def _make_msrp_v3_report(run_id: str = "msrp-dryrun-20260611-120000") -> dict:
    return {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": run_id,
        "batch": "batch_a",
        "expectedCountries": ["fi"],
        "observedCountries": ["fi"],
        "missingCountries": [],
        "duplicateCountries": [],
        "summary": {
            "total": 30,
            "pass": 27,
            "empty": 3,
            "fail": 0,
            "errors": 0,
            "passPct": 90.0,
            "status": "success",
            "gateThreshold": 70,
            "gateStatus": "allowed",
        },
        "countriesDetail": [
            {
                "countryCode": "fi",
                "total": 30,
                "pass": 27,
                "empty": 3,
                "fail": 0,
                "errors": 0,
                "passPct": 90.0,
                "status": "success",
                "failureBreakdown": {
                    "no_observation_extracted": 2,
                    "forbidden_403": 1,
                },
                "strategyRecommendations": {
                    "diagnose_with_msrp_page_analyzer": 2,
                    "manual_review_or_proxy_required": 1,
                },
                "sources": [],
            }
        ],
        "results": [],
        "generatedAt": "2026-06-11T12:00:00Z",
    }


# ── /hermes/sentinel + deploy status ─────────────────────────────────

class TestSentinelAndDeploy:
    def test_run_route_respects_runner_disabled(self, client, monkeypatch):
        monkeypatch.setenv("HERMES_RUN_ENABLED", "false")

        resp = client.post("/hermes/run/cost-report")

        assert resp.status_code == 403
        assert "disabled" in resp.json()["detail"]

    def test_set_notification_status_route(self, client, tmp_path, monkeypatch):
        from app.services import hermes_sentinel_service as sentinel

        monkeypatch.setattr(sentinel, "_project_root", tmp_path)
        created = sentinel._emit("devsync", "medium", "Missing Docs", "Body")
        assert created is not None

        resp = client.post(
            f"/hermes/sentinel/notifications/{created['id']}/status",
            json={"status": "archived"},
        )

        assert resp.status_code == 200
        assert resp.json()["status"] == "archived"

    def test_deploy_status_endpoint_reports_drift(self, client, tmp_path, monkeypatch):
        from app.services import hermes_deploy_status_service as deploy_status

        monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
        _write_json(tmp_path / "hermes" / "deploy_release.json", {
            "commitSha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "shortSha": "aaaaaaaa",
            "source": "github_actions_archive",
        })
        _write_json(tmp_path / "hermes" / "deploy_expected.json", {
            "commitSha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "shortSha": "bbbbbbbb",
        })

        resp = client.get("/hermes/deploy/status")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "critical"
        assert data["drift"]["isDrift"] is True

    def test_full_design_document_endpoint(self, client, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir(parents=True)
        (reports_dir / "HERMES_FULL_DESIGN_DOCUMENT.md").write_text("# Hermes\n\nDesign", encoding="utf-8")

        with patch("app.api.routes.hermes.HERMES_DIR", tmp_path), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path.parent):
            resp = client.get("/hermes/reports/full-design-document")

        assert resp.status_code == 200
        data = resp.json()
        assert data["exists"] is True
        assert "# Hermes" in data["content"]

    def test_pipeline_status_list_endpoint(self, client, tmp_path, monkeypatch):
        from app.services import hermes_pipeline_status_service as pipeline_status

        monkeypatch.setattr(pipeline_status, "_project_root", tmp_path)
        pipeline_status.write_pipeline_status({
            "pipelineId": "msrp_dryrun",
            "status": "success",
            "lastRunAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "recordsProcessed": 12,
        })

        resp = client.get("/hermes/pipeline/status")

        assert resp.status_code == 200
        data = resp.json()
        dryrun = [item for item in data if item["pipelineId"] == "msrp_dryrun"][0]
        assert dryrun["status"] == "success"
        assert dryrun["recordsProcessed"] == 12

    def test_pipeline_status_detail_endpoint(self, client, tmp_path, monkeypatch):
        from app.services import hermes_pipeline_status_service as pipeline_status

        monkeypatch.setattr(pipeline_status, "_project_root", tmp_path)

        resp = client.get("/hermes/pipeline/status/msrp_ingest")

        assert resp.status_code == 200
        data = resp.json()
        assert data["pipelineId"] == "msrp_ingest"
        assert data["status"] == "missing"

    def test_msrp_country_progress_latest_missing_returns_empty_state(self, client, tmp_path):
        reports_dir = tmp_path / "hermes" / "reports"
        reports_dir.mkdir(parents=True)

        with (
            patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path),
            patch("app.api.routes.hermes.REPORTS_DIR", reports_dir),
            patch("app.api.routes.hermes._partial_msrp_progress", return_value=None),
        ):
            resp = client.get("/hermes/msrp-country-progress")

        assert resp.status_code == 200
        data = resp.json()
        assert data["probe"] == "pipeline.msrp_country_progress"
        assert data["countries"] == []
        assert data["findings"][0]["type"] == "no_dryrun_report"
        assert data["sourceRepairBacklog"]["groups"] == []

    def test_msrp_country_progress_uses_partial_running_state(self, client, tmp_path):
        reports_dir = tmp_path / "hermes" / "reports"
        reports_dir.mkdir(parents=True)
        _write_json(reports_dir / "msrp_country_progress.json", {
            "probe": "pipeline.msrp_country_progress",
            "overall": "critical",
            "status": {},
            "countries": [],
            "findings": [{"type": "no_dryrun_report", "severity": "critical"}],
        })
        partial_progress = {
            "probe": "pipeline.msrp_country_progress",
            "overall": "warning",
            "generatedAt": "2026-06-12T08:00:00Z",
            "status": {
                "runId": "msrp-dryrun-20260612-070207",
                "schemaVersion": "msrp_dryrun_partial_v1",
                "running": True,
                "partial": True,
                "overallPassPct": 66.7,
                "gateStatus": "pending",
                "expectedCountries": ["se", "fi"],
                "observedCountries": ["se"],
                "missingCountries": ["fi"],
                "duplicateCountries": [],
            },
            "countries": [{"countryCode": "se", "passPct": 100.0}],
            "topBlockingCountries": [],
            "topFailureReasons": [],
            "sourceRepairBacklog": {"schemaVersion": "msrp_source_repair_backlog_v1", "groups": []},
            "findings": [{"type": "dryrun_running_without_aggregate", "severity": "warning"}],
        }

        with (
            patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path),
            patch("app.api.routes.hermes.REPORTS_DIR", reports_dir),
            patch("app.api.routes.hermes._partial_msrp_progress", return_value=partial_progress),
        ):
            resp = client.get("/hermes/msrp-country-progress")

        assert resp.status_code == 200
        data = resp.json()
        assert data["overall"] == "warning"
        assert data["status"]["partial"] is True
        assert data["status"]["runId"] == "msrp-dryrun-20260612-070207"
        assert data["findings"][0]["type"] == "dryrun_running_without_aggregate"

    def test_msrp_country_progress_prefers_new_active_partial_over_stale_complete_run(
        self,
        client,
        tmp_path,
    ):
        reports_dir = tmp_path / "hermes" / "reports"
        artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
        reports_dir.mkdir(parents=True)
        old_report = _make_msrp_v3_report("msrp-dryrun-20260612-070207")
        old_report["summary"].update({
            "pass": 0,
            "empty": 30,
            "passPct": 0.0,
            "status": "failure",
            "gateStatus": "blocked",
        })
        _write_json(artifact_dir / "dryrun_report.json", old_report)
        _write_json(reports_dir / "msrp_country_progress.json", {
            "probe": "pipeline.msrp_country_progress",
            "overall": "critical",
            "status": {"runId": old_report["runId"], "gateStatus": "blocked"},
            "countries": [],
            "topBlockingCountries": [],
            "topFailureReasons": [],
            "sourceRepairBacklog": {"schemaVersion": "msrp_source_repair_backlog_v1", "groups": []},
            "findings": [{"type": "ingest_gate_blocked", "severity": "critical"}],
        })
        partial_progress = {
            "probe": "pipeline.msrp_country_progress",
            "overall": "warning",
            "generatedAt": "2026-06-12T12:53:01Z",
            "status": {
                "runId": "msrp-dryrun-20260612-125301",
                "schemaVersion": "msrp_dryrun_partial_v1",
                "running": True,
                "partial": True,
                "overallPassPct": 100.0,
                "gateStatus": "pending",
                "expectedCountries": ["se", "fi"],
                "observedCountries": ["se"],
                "missingCountries": ["fi"],
                "duplicateCountries": [],
            },
            "countries": [{"countryCode": "se", "passPct": 100.0}],
            "topBlockingCountries": [],
            "topFailureReasons": [],
            "sourceRepairBacklog": {"schemaVersion": "msrp_source_repair_backlog_v1", "groups": []},
            "findings": [{"type": "dryrun_running_without_aggregate", "severity": "warning"}],
        }

        with (
            patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path),
            patch("app.api.routes.hermes.REPORTS_DIR", reports_dir),
            patch("app.api.routes.hermes._partial_msrp_progress", return_value=partial_progress),
        ):
            resp = client.get("/hermes/msrp-country-progress")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"]["runId"] == "msrp-dryrun-20260612-125301"
        assert data["status"]["partial"] is True
        assert data["status"]["gateStatus"] == "pending"
        assert data["findings"][0]["type"] == "dryrun_running_without_aggregate"

    def test_partial_msrp_progress_builds_repair_backlog(self):
        current = {
            "available": True,
            "partial": True,
            "running": True,
            "runId": "msrp-dryrun-20260612-070207",
            "schemaVersion": "msrp_dryrun_partial_v1",
            "overallPassRate": 10.0,
            "gateStatus": "pending",
            "expectedCountries": ["se", "dk"],
            "observedCountries": ["se", "dk"],
            "missingCountries": [],
            "countries": [
                {
                    "countryCode": "se",
                    "completed": True,
                    "total": 2,
                    "pass": 1,
                    "empty": 1,
                    "fail": 0,
                    "errors": 0,
                    "passRate": 50.0,
                    "status": "degraded",
                    "topFailureReason": "http_timeout",
                    "failureBreakdown": {"http_timeout": 1},
                    "strategyRecommendations": {"retry_or_reduce_concurrency": 1},
                    "sources": [
                        {
                            "sourceCode": "audi_q4_e_tron_se_draft_scrapling",
                            "status": "empty",
                            "failureReason": "http_timeout",
                            "recommendedStrategy": "retry_or_reduce_concurrency",
                            "sourceUrl": "https://www.audi.se/se/web/sv/models/q4-e-tron.html",
                        }
                    ],
                },
                {
                    "countryCode": "dk",
                    "completed": True,
                    "total": 1,
                    "pass": 0,
                    "empty": 1,
                    "fail": 0,
                    "errors": 0,
                    "passRate": 0.0,
                    "status": "failure",
                    "topFailureReason": "json_ld_empty",
                    "failureBreakdown": {"json_ld_empty": 1},
                    "strategyRecommendations": {"try_css_or_attr_json": 1},
                    "sources": [
                        {
                            "sourceCode": "source_dk",
                            "status": "empty",
                            "failureReason": "json_ld_empty",
                            "recommendedStrategy": "try_css_or_attr_json",
                            "sourceUrl": "https://www.citroen.dk/modeller/c3-aircross.html",
                        }
                    ],
                },
            ],
        }

        data = _msrp_progress_from_partial_current(current)

        assert data is not None
        assert data["overall"] == "critical"
        assert data["topFailureReasons"] == [
            {"reason": "http_timeout", "count": 1},
            {"reason": "json_ld_empty", "count": 1},
        ]
        assert data["topBlockingCountries"][0]["countryCode"] == "dk"
        backlog = data["sourceRepairBacklog"]
        assert backlog["partial"] is True
        assert backlog["runId"] == "msrp-dryrun-20260612-070207"
        assert backlog["totalIssueCount"] == 2
        assert backlog["groups"][0]["failureReason"] == "http_timeout"
        assert backlog["groups"][0]["priorityScore"] > 0
        assert backlog["groups"][0]["priorityBand"] in {"medium", "high", "critical"}
        assert backlog["groups"][0]["reviewAssist"]["preferred"] == "rule_based"
        assert backlog["groups"][0]["sampleSources"] == ["audi_q4_e_tron_se_draft_scrapling"]
        assert backlog["groups"][0]["topSourceHosts"] == [
            {
                "host": "audi.se",
                "count": 1,
                "affectedCountries": ["se"],
                "affectedCountryCount": 1,
                "sampleSources": ["audi_q4_e_tron_se_draft_scrapling"],
                "sampleUrls": ["https://www.audi.se/se/web/sv/models/q4-e-tron.html"],
            }
        ]
        assert backlog["topSourceHosts"][0]["host"] == "audi.se"

    def test_partial_msrp_progress_marks_probe_regressions_for_recheck(self):
        current = {
            "available": True,
            "partial": True,
            "running": True,
            "runId": "msrp-dryrun-20260612-125301",
            "schemaVersion": "msrp_dryrun_partial_v1",
            "overallPassRate": 0.0,
            "gateStatus": None,
            "expectedCountries": ["se"],
            "observedCountries": ["se"],
            "missingCountries": [],
            "countries": [
                {
                    "countryCode": "se",
                    "completed": True,
                    "total": 1,
                    "pass": 0,
                    "empty": 1,
                    "fail": 0,
                    "errors": 0,
                    "passRate": 0.0,
                    "status": "failure",
                    "failureBreakdown": {"http_timeout": 1},
                    "strategyRecommendations": {"retry_or_reduce_concurrency": 1},
                    "sources": [
                        {
                            "sourceCode": "audi_q4_e_tron_se_draft_scrapling",
                            "status": "empty",
                            "failureReason": "http_timeout",
                            "recommendedStrategy": "retry_or_reduce_concurrency",
                            "sourceUrl": "https://www.audi.se/se/web/sv/models/q4-e-tron.html",
                        }
                    ],
                }
            ],
        }
        stable_coverage = {
            "probeRegressionSamples": [
                {
                    "countryCode": "se",
                    "sourceCode": "audi_q4_e_tron_se_draft_scrapling",
                    "stableRunId": "msrp-dryrun-20260612-070207",
                    "activeRunId": "msrp-dryrun-20260612-125301",
                    "activeStatus": "empty",
                    "failureReason": "http_timeout",
                }
            ]
        }

        all_countries_latest = [
            {
                "countryCode": "se",
                "countryLabel": "Sweden",
                "total": 1,
                "pass": 1,
                "empty": 0,
                "fail": 0,
                "errors": 0,
                "passRate": 100.0,
                "status": "success",
                "runId": "msrp-dryrun-20260612-070207",
                "isLatestRun": True,
            }
        ]

        data = _msrp_progress_from_partial_current(
            current,
            stable_coverage,
            all_countries_latest,
        )

        assert data is not None
        assert data["status"]["stableLatestRunId"] == "msrp-dryrun-20260612-070207"
        assert data["allCountriesLatest"][0]["countryCode"] == "se"
        assert data["allCountriesLatest"][0]["passPct"] == 100.0
        assert data["allCountriesLatest"][0]["runId"] == "msrp-dryrun-20260612-070207"
        assert data["stableCoverage"]["probeRegressionSamples"][0]["stableRunId"] == (
            "msrp-dryrun-20260612-070207"
        )
        backlog = data["sourceRepairBacklog"]
        assert backlog["totalIssueCount"] == 1
        assert backlog["transientRegressionCount"] == 1
        assert backlog["sourceRepairIssueCount"] == 0
        assert backlog["groups"][0]["recommendedAction"] == "recheck_before_source_repair"
        assert backlog["groups"][0]["priorityBand"] == "recheck"
        assert backlog["groups"][0]["reviewAssist"]["preferred"] == "rule_based_recheck"
        assert backlog["groups"][0]["sampleTransientRegressions"][0]["lastKnownGoodRunId"] == (
            "msrp-dryrun-20260612-070207"
        )

    def test_partial_msrp_progress_marks_stopped_partial_without_aggregate(self):
        current = {
            "available": True,
            "partial": True,
            "running": False,
            "runId": "msrp-dryrun-20260612-125301",
            "schemaVersion": "msrp_dryrun_partial_v1",
            "overallPassRate": 50.0,
            "gateStatus": None,
            "expectedCountries": ["se", "fi"],
            "observedCountries": ["se"],
            "missingCountries": ["fi"],
            "countries": [
                {
                    "countryCode": "se",
                    "completed": True,
                    "total": 1,
                    "pass": 1,
                    "empty": 0,
                    "fail": 0,
                    "errors": 0,
                    "passRate": 100.0,
                    "status": "success",
                    "failureBreakdown": {},
                    "strategyRecommendations": {},
                    "sources": [],
                },
                {
                    "countryCode": "fi",
                    "completed": False,
                    "total": 0,
                    "pass": 0,
                    "empty": 0,
                    "fail": 0,
                    "errors": 0,
                    "passRate": 0.0,
                    "status": "running",
                    "failureBreakdown": {},
                    "strategyRecommendations": {},
                    "sources": [],
                },
            ],
        }

        data = _msrp_progress_from_partial_current(current)

        assert data is not None
        assert data["status"]["running"] is False
        assert data["findings"][0]["type"] == "dryrun_partial_without_aggregate"
        assert "no active run" in data["findings"][0]["message"]

    def test_msrp_country_progress_missing_specific_run_remains_404(self, client, tmp_path):
        reports_dir = tmp_path / "hermes" / "reports"
        reports_dir.mkdir(parents=True)

        with (
            patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path),
            patch("app.api.routes.hermes.REPORTS_DIR", reports_dir),
        ):
            resp = client.get("/hermes/msrp-country-progress?run_id=missing-run")

        assert resp.status_code == 404

    def test_msrp_country_progress_falls_back_to_latest_dryrun_artifact(self, client, tmp_path):
        reports_dir = tmp_path / "hermes" / "reports"
        artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
        stale_progress = {
            "probe": "pipeline.msrp_country_progress",
            "overall": "critical",
            "status": {},
            "countries": [],
            "topBlockingCountries": [],
            "topFailureReasons": [],
            "findings": [{"type": "no_dryrun_report", "severity": "critical"}],
        }
        _write_json(reports_dir / "msrp_country_progress.json", stale_progress)
        _write_json(artifact_dir / "dryrun_report.json", _make_msrp_v3_report())
        _write_json(artifact_dir / "msrp_source_repair_backlog.json", {
            "schemaVersion": "msrp_source_repair_backlog_v1",
            "runId": "msrp-dryrun-20260611-120000",
            "totalIssueCount": 3,
            "groups": [],
        })

        with patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path), patch(
            "app.api.routes.hermes.REPORTS_DIR",
            reports_dir,
        ):
            resp = client.get("/hermes/msrp-country-progress")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"]["runId"] == "msrp-dryrun-20260611-120000"
        assert data["overall"] == "ok"
        assert data["countries"][0]["countryCode"] == "fi"
        assert data["countries"][0]["passPct"] == 90.0
        assert data["topFailureReasons"][0] == {
            "reason": "no_observation_extracted",
            "count": 2,
        }
        assert data["sourceRepairBacklog"]["totalIssueCount"] == 3

    def test_msrp_country_progress_enriches_latest_report_with_all_country_latest(
        self,
        client,
        tmp_path,
    ):
        reports_dir = tmp_path / "hermes" / "reports"
        artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
        report = _make_msrp_v3_report("msrp-dryrun-20260613-110334")
        report["expectedCountries"] = ["hu"]
        report["observedCountries"] = ["hu"]
        report["countriesDetail"][0]["countryCode"] = "hu"
        _write_json(artifact_dir / "dryrun_report.json", report)

        dashboard_context = {
            "dashboard": {},
            "stableLatestRunId": "msrp-dryrun-20260615-193249",
            "stableCoverage": {
                "countryCount": 2,
                "readyCountryCount": 1,
                "latestRunId": "msrp-dryrun-20260615-193249",
                "activeRunId": "msrp-dryrun-20260613-110334",
            },
            "allCountriesLatest": [
                {
                    "countryCode": "fi",
                    "total": 30,
                    "pass": 27,
                    "empty": 3,
                    "fail": 0,
                    "errors": 0,
                    "passPct": 90.0,
                    "status": "success",
                    "runId": "msrp-dryrun-20260615-193249",
                    "isLatestRun": True,
                },
                {
                    "countryCode": "hu",
                    "total": 31,
                    "pass": 15,
                    "empty": 13,
                    "fail": 3,
                    "errors": 0,
                    "passPct": 48.4,
                    "status": "failure",
                    "runId": "msrp-dryrun-20260613-110334",
                    "isLatestRun": False,
                },
            ],
        }

        with (
            patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path),
            patch("app.api.routes.hermes.REPORTS_DIR", reports_dir),
            patch("app.api.routes.hermes._msrp_dashboard_context", return_value=dashboard_context),
            patch("app.api.routes.hermes._partial_msrp_progress", return_value=None),
        ):
            resp = client.get("/hermes/msrp-country-progress")

        assert resp.status_code == 200
        data = resp.json()
        assert [country["countryCode"] for country in data["countries"]] == ["hu"]
        assert [country["countryCode"] for country in data["allCountriesLatest"]] == ["fi", "hu"]
        assert data["stableCoverage"]["countryCount"] == 2
        assert data["status"]["stableLatestRunId"] == "msrp-dryrun-20260615-193249"
        assert data["status"]["activeRunId"] == "msrp-dryrun-20260613-110334"

    def test_msrp_country_progress_uses_runs_index_when_latest_shortcut_is_partial(
        self,
        client,
        tmp_path,
    ):
        reports_dir = tmp_path / "hermes" / "reports"
        artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
        run_id = "msrp-dryrun-20260612-070207"
        partial_run_id = "msrp-dryrun-20260612-125301"
        _write_json(reports_dir / "msrp_country_progress.json", {
            "probe": "pipeline.msrp_country_progress",
            "overall": "critical",
            "status": {
                "runId": partial_run_id,
                "schemaVersion": "msrp_dryrun_partial_v1",
                "running": True,
                "partial": True,
            },
            "countries": [{"countryCode": "se", "passPct": 7.5}],
            "topBlockingCountries": [],
            "topFailureReasons": [],
            "sourceRepairBacklog": {
                "schemaVersion": "msrp_source_repair_backlog_v1",
                "totalIssueCount": 0,
                "groups": [],
            },
            "findings": [],
        })
        _write_json(artifact_dir / "dryrun_report.json", {
            "schemaVersion": "msrp_dryrun_partial_v1",
            "runId": partial_run_id,
            "running": True,
            "partial": True,
        })
        _write_json(artifact_dir / f"dryrun_report_{run_id}.json", _make_msrp_v3_report(run_id))
        _write_json(artifact_dir / "dryrun_runs_index.json", {
            "schemaVersion": "msrp_dryrun_runs_index_v1",
            "latestRunId": run_id,
            "runs": [
                {
                    "runId": run_id,
                    "artifactPath": str(artifact_dir / f"dryrun_report_{run_id}.json"),
                }
            ],
        })

        with (
            patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path),
            patch("app.api.routes.hermes.REPORTS_DIR", reports_dir),
            patch("app.api.routes.hermes._partial_msrp_progress", return_value={
                "status": {
                    "runId": partial_run_id,
                    "running": True,
                    "partial": True,
                }
            }),
        ):
            resp = client.get("/hermes/msrp-country-progress")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"]["schemaVersion"] == "msrp_dryrun_report_v3"
        assert data["status"]["runId"] == run_id
        assert data["countries"][0]["countryCode"] == "fi"
        assert data["status"]["gateStatus"] == "allowed"

    def test_msrp_country_progress_run_id_falls_back_to_historical_dryrun_artifact(
        self,
        client,
        tmp_path,
    ):
        reports_dir = tmp_path / "hermes" / "reports"
        artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
        run_id = "msrp-dryrun-20260611-130000"
        _write_json(artifact_dir / f"dryrun_report_{run_id}.json", _make_msrp_v3_report(run_id))

        with patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path), patch(
            "app.api.routes.hermes.REPORTS_DIR",
            reports_dir,
        ):
            resp = client.get(f"/hermes/msrp-country-progress?run_id={run_id}")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"]["runId"] == run_id
        assert data["countries"][0]["countryCode"] == "fi"

    def test_msrp_country_progress_derives_host_backlog_from_v3_sources(
        self,
        client,
        tmp_path,
    ):
        reports_dir = tmp_path / "hermes" / "reports"
        artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
        report = _make_msrp_v3_report()
        report["summary"].update({
            "pass": 0,
            "empty": 3,
            "passPct": 0.0,
            "status": "failure",
            "gateStatus": "blocked",
        })
        report["countriesDetail"][0].update({
            "pass": 0,
            "empty": 3,
            "passPct": 0.0,
            "status": "failure",
            "failureBreakdown": {"http_timeout": 2, "forbidden_403": 1},
            "strategyRecommendations": {
                "retry_or_reduce_concurrency": 2,
                "manual_review_or_proxy_required": 1,
            },
            "sources": [
                {
                    "sourceCode": "audi_q4_e_tron_fi_draft_scrapling",
                    "status": "empty",
                    "failureReason": "http_timeout",
                    "recommendedStrategy": "retry_or_reduce_concurrency",
                    "extractorError": (
                        "Page.goto: Timeout 30000ms exceeded while navigating to "
                        "\"https://www.audi.fi/fi/web/fi/models/q4-e-tron.html\""
                    ),
                },
                {
                    "sourceCode": "audi_q6_e_tron_fi_draft_scrapling",
                    "status": "empty",
                    "failureReason": "http_timeout",
                    "recommendedStrategy": "retry_or_reduce_concurrency",
                    "finalUrl": "https://www.audi.fi/fi/web/fi/models/q6-e-tron.html",
                },
                {
                    "sourceCode": "cupra_formentor_fi_draft_scrapling",
                    "status": "empty",
                    "failureReason": "forbidden_403",
                    "recommendedStrategy": "manual_review_or_proxy_required",
                    "sourceUrl": "https://www.cupraofficial.fi/autot/formentor",
                },
            ],
        })
        _write_json(artifact_dir / "dryrun_report.json", report)
        _write_json(reports_dir / "msrp_country_progress.json", {
            "probe": "pipeline.msrp_country_progress",
            "overall": "critical",
            "status": {"runId": report["runId"]},
            "countries": [],
            "topBlockingCountries": [],
            "topFailureReasons": [],
            "sourceRepairBacklog": {
                "schemaVersion": "msrp_source_repair_backlog_v1",
                "runId": report["runId"],
                "totalIssueCount": 3,
                "groups": [{"failureReason": "http_timeout", "count": 2}],
            },
            "findings": [],
        })

        with patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path), patch(
            "app.api.routes.hermes.REPORTS_DIR",
            reports_dir,
        ):
            resp = client.get("/hermes/msrp-country-progress")

        assert resp.status_code == 200
        backlog = resp.json()["sourceRepairBacklog"]
        assert backlog["totalIssueCount"] == 3
        assert backlog["topSourceHosts"][0]["host"] == "audi.fi"
        assert backlog["topSourceHosts"][0]["count"] == 2
        assert backlog["groups"][0]["failureReason"] == "http_timeout"
        assert backlog["groups"][0]["priorityScore"] > 0
        assert backlog["groups"][0]["topSourceHosts"][0]["host"] == "audi.fi"
        assert backlog["groups"][0]["topSourceHosts"][0]["sampleSources"] == [
            "audi_q4_e_tron_fi_draft_scrapling",
            "audi_q6_e_tron_fi_draft_scrapling",
        ]

    def test_history_clusters_endpoint(self, client, monkeypatch):
        monkeypatch.setattr(
            "app.services.hermes_history_service.list_history_clusters",
            lambda level, y_axis, workstream, limit: {
                "summary": {"level": level, "yAxis": y_axis, "clusterCount": 1},
                "clusters": [{"clusterId": "cluster_1", "title": "Hermes"}],
            },
        )

        resp = client.get("/hermes/history/clusters?level=feature&yAxis=workstream")

        assert resp.status_code == 200
        assert resp.json()["summary"]["clusterCount"] == 1

    def test_progress_swimlanes_endpoint(self, client, monkeypatch):
        monkeypatch.setattr(
            "app.services.hermes_history_service.get_progress_swimlanes",
            lambda: {
                "summary": {"total": 1, "blocking": 0},
                "phases": ["PRD", "Implemented"],
                "lanes": [{"workstream": "Hermes", "features": []}],
            },
        )

        resp = client.get("/hermes/progress/swimlanes")

        assert resp.status_code == 200
        assert resp.json()["summary"]["total"] == 1


# ── /hermes/gaps ──────────────────────────────────────────────────────

class TestGaps:
    def test_returns_all_gaps_when_no_filters(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test", "severity": "high"},
                {"gapId": "g2", "status": "resolved", "category": "docs", "severity": "low"},
                {"gapId": "g3", "status": "in_progress", "category": "pipeline", "severity": "medium"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3

    def test_filters_by_status(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
                {"gapId": "g2", "status": "resolved", "category": "docs"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?status=open")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["gapId"] == "g1"

    def test_filters_by_category(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
                {"gapId": "g2", "status": "open", "category": "docs"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?category=test")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["gapId"] == "g1"

    def test_filters_combined_and_logic(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
                {"gapId": "g2", "status": "resolved", "category": "test"},
                {"gapId": "g3", "status": "open", "category": "docs"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?status=open&category=test")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["gapId"] == "g1"

    def test_unknown_status_returns_empty(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?status=nonexistent")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_unknown_category_returns_empty(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?category=nonexistent")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_file_missing_returns_empty(self, client, tmp_path):
        nonexistent = tmp_path / "nonexistent"
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", nonexistent):
            resp = client.get("/hermes/gaps")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_malformed_yaml_returns_empty(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        gaps_yaml.parent.mkdir(parents=True, exist_ok=True)
        gaps_yaml.write_text(":!!:bad yaml: - [")
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps")
        # Malformed YAML is caught and returns empty list
        assert resp.status_code == 200
        assert resp.json() == []

    def test_gaps_without_status_field_not_filtered_out(self, client, tmp_path):
        """Gaps missing 'status' key should be left in when no filter is active."""
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "category": "test"},
                {"gapId": "g2", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_gaps_without_category_field_not_filtered_out(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open"},
                {"gapId": "g2", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?category=test")
        assert resp.status_code == 200
        assert len(resp.json()) == 1


# ── /hermes/evidence-ledger ──────────────────────────────────────────

class TestEvidenceLedger:
    def _make_entry(self, created_at: str, etype: str = "fact") -> dict:
        return {"createdAt": created_at, "type": etype, "fact": "test fact"}

    def test_returns_empty_when_file_missing(self, client, tmp_path):
        nonexistent = tmp_path / "nonexistent_ledger.jsonl"
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", nonexistent.parent):
            with patch("app.api.routes.hermes.HERMES_DIR", nonexistent.parent):
                resp = client.get("/hermes/evidence-ledger")
        # file not found → empty return, 200
        assert resp.status_code == 200
        data = resp.json()
        assert data["totalCount"] == 0
        assert data["records"] == []
        assert data["byType"] == {}
        assert data["rangeStart"] == ""
        assert data["rangeEnd"] == ""

    def test_returns_records_with_type_breakdown(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(days=i)).isoformat(), "fact" if i % 2 == 0 else "event")
            for i in range(10)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        _write_json(ledger, entries[0])  # won't write array, need one per line
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert data["totalCount"] == 10
        assert len(data["records"]) == 10
        assert "fact" in data["byType"]
        assert "event" in data["byType"]
        assert data["rangeStart"] != ""
        assert data["rangeEnd"] != ""

    def test_days_filter_narrows_results(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(days=i)).isoformat()) for i in range(20)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp_all = client.get("/hermes/evidence-ledger?days=90&limit=100")
            resp_7 = client.get("/hermes/evidence-ledger?days=7")
        all_data = resp_all.json()
        day7_data = resp_7.json()
        # totalCount is all-time regardless of days filter
        assert all_data["totalCount"] == 20
        assert day7_data["totalCount"] == 20
        # But records within the 7-day window should be fewer
        assert len(day7_data["records"]) <= len(all_data["records"])

    def test_limit_caps_records(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(hours=i)).isoformat()) for i in range(50)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=90&limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["records"]) == 5
        assert data["totalCount"] == 50  # all-time

    def test_default_days_is_7(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(days=i)).isoformat()) for i in range(14)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["records"]) <= 7 + 1  # ~7 days of unique dates

    def test_entries_without_created_at_are_handled(self, client, tmp_path):
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text(json.dumps({"type": "fact", "event": "no date"}) + "\n")

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger")
        assert resp.status_code == 200
        data = resp.json()
        # No crash, total counts
        assert data["totalCount"] == 1

    def test_corrupt_lines_are_skipped(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        ledger = tmp_path / "evidence_ledger.jsonl"
        lines = [
            "not json",
            json.dumps(self._make_entry(now.isoformat(), "fact")),
            "",
            "{broken",
        ]
        ledger.write_text("\n".join(lines))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert data["totalCount"] == 1  # only the valid JSON line

    def test_by_type_is_always_present(self, client, tmp_path):
        """byType must be present as {} even when records is empty."""
        entries = [
            self._make_entry((datetime.now(timezone.utc) - timedelta(days=60)).isoformat()),
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["records"] == []
        assert data["byType"] == {}
        assert isinstance(data["byType"], dict)


# ── /hermes/markdown-diagrams ─────────────────────────────────────────

MD_SINGLE_FLOWCHART = """# Test Doc

## Pipeline Overview

```mermaid
flowchart TD
    A --> B
    B --> C
```

Some text after.
"""

MD_TWO_DIAGRAMS = """# Two Diagrams

## First Diagram

```mermaid
flowchart LR
    X --> Y
```

## Second Diagram

```mermaid
sequenceDiagram
    Alice->>Bob: Hello
```

End.
"""

MD_NO_MERMAID = """# No Diagrams Here

Just some text and a code block:

```
not mermaid
```
"""

MD_BROKEN_MERMAID = """# Broken Mermaid

```mermaid
this is not valid mermaid at all
```
"""


class TestMarkdownDiagrams:
    def test_extracts_single_flowchart(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "test.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_SINGLE_FLOWCHART)
        # Invalidate cache
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        d = data[0]
        assert d["type"] == "flowchart"
        assert "A --> B" in d["raw"]
        assert d["diagramIndex"] == 0
        assert "test.md" in d["file"]

    def test_extracts_multiple_diagrams(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "multi.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_TWO_DIAGRAMS)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["type"] == "flowchart"
        assert data[1]["type"] == "sequenceDiagram"

    def test_returns_empty_when_no_diagrams(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "noop.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_NO_MERMAID)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_handles_broken_mermaid_gracefully(self, client, tmp_path):
        """A syntactically broken mermaid block is still extracted — rendering
        failure is the frontend's responsibility."""
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "broken.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_BROKEN_MERMAID)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert "not valid" in data[0]["raw"]

    def test_file_filter_filters_by_substring(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        (md_dir / "sub").mkdir(parents=True)
        (md_dir / "a.md").write_text(MD_SINGLE_FLOWCHART)
        (md_dir / "sub" / "b.md").write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams?file_filter=sub")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert "sub" in data[0]["file"]

    def test_file_filter_case_insensitive(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "WORKFLOWS").mkdir(parents=True, exist_ok=True)
        (md_dir / "WORKFLOWS" / "test.md").write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams?file_filter=workflows")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_cache_invalidates_on_mtime_change(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        md_file = md_dir / "test.md"
        md_file.write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp1 = client.get("/hermes/markdown-diagrams")
        assert resp1.status_code == 200
        cached_data = resp1.json()
        assert len(cached_data) == 1

        # Modify the file
        time.sleep(0.1)
        md_file.write_text(MD_TWO_DIAGRAMS)

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp2 = client.get("/hermes/markdown-diagrams")
        assert resp2.status_code == 200
        # Cache should have been invalidated, now returns 2 diagrams
        assert len(resp2.json()) == 2

    def test_cache_invalidates_when_file_deleted(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        f1 = md_dir / "a.md"
        f2 = md_dir / "b.md"
        f1.write_text(MD_SINGLE_FLOWCHART)
        f2.write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp1 = client.get("/hermes/markdown-diagrams")
        assert len(resp1.json()) == 2

        # Delete one file
        f1.unlink()

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp2 = client.get("/hermes/markdown-diagrams")
        assert len(resp2.json()) == 1

    def test_empty_md_dir_returns_empty(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_missing_md_dir_returns_empty(self, client, tmp_path):
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path / "no_such_dir"):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_heading_extraction_from_before_block(self, client, tmp_path):
        content = """# Top Level

## My Flowchart

Some description text here.

```mermaid
flowchart TD
    A --> B
```
"""
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "heading.md").write_text(content)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["title"] == "My Flowchart"

    def test_non_utf8_file_is_skipped(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "bad.md").write_bytes(b"\xff\xfe\x00\x01")
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_unknown_diagram_type_defaults_to_flowchart(self, client, tmp_path):
        content = """```mermaid
erDiagram
    CUSTOMER ||--o{ ORDER : places
```
"""
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "erd.md").write_text(content)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["type"] == "flowchart"  # default for unknown


# ── Regex unit tests (no server needed) ──────────────────────────────

# ── Workspace Health route ─────────────────────────────────────────


class TestWorkspaceHealth:
    def test_returns_expected_structure(self, client, monkeypatch):
        monkeypatch.setattr(
            "app.services.hermes_workspace_health_service.get_workspace_health",
            lambda: {
                "changedFiles": ["a.py"],
                "stagedFiles": [],
                "committedUnpushed": [],
                "unlinkedChanges": 1,
                "riskLevel": "low",
                "warnings": ["Some code changes not in dev events"],
                "gitAvailable": True,
            },
        )
        resp = client.get("/hermes/dev/workspace-health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["changedFiles"] == ["a.py"]
        assert data["unlinkedChanges"] == 1
        assert data["riskLevel"] == "low"
        assert data["pushedUnsyncedEvents"] == 0

    def test_preserves_pushed_unsynced_events(self, client, monkeypatch):
        monkeypatch.setattr(
            "app.services.hermes_workspace_health_service.get_workspace_health",
            lambda: {
                "changedFiles": [],
                "stagedFiles": [],
                "committedUnpushed": [],
                "unlinkedChanges": 0,
                "riskLevel": "low",
                "warnings": [],
                "gitAvailable": True,
            },
        )
        data = client.get("/hermes/dev/workspace-health").json()
        assert "pushedUnsyncedEvents" in data


# ── DevSync auth fail-closed ────────────────────────────────────────


class TestDevSyncAuthFailClosed:
    def test_github_actions_without_sync_token_returns_401(self, client, monkeypatch):
        monkeypatch.setenv("HERMES_SYNC_TOKEN", "")
        resp = client.post("/hermes/dev/sync?source=github_actions", json={})
        assert resp.status_code == 401
        assert "HERMES_SYNC_TOKEN" in resp.json()["detail"]

    def test_github_actions_invalid_token_returns_401(self, client, monkeypatch):
        monkeypatch.setenv("HERMES_SYNC_TOKEN", "valid-token")
        resp = client.post(
            "/hermes/dev/sync?source=github_actions",
            json={},
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp.status_code == 401

    def test_github_actions_sync_token_works_when_auth_enabled(self, client, monkeypatch, tmp_path):
        monkeypatch.setattr("app.core.security.AUTH_ENABLED", True)
        monkeypatch.setenv("HERMES_SYNC_TOKEN", "valid-token")
        monkeypatch.setattr("app.api.routes.hermes.HERMES_DIR", tmp_path)
        monkeypatch.setattr(
            "app.services.hermes_devsync_service.sync_dev_events",
            lambda: {"status": "ok", "synced": 0},
        )

        resp = client.post(
            "/hermes/dev/sync?source=github_actions",
            json={},
            headers={"Authorization": "Bearer valid-token"},
        )

        assert resp.status_code == 200

    def test_non_github_actions_uses_local_admin_when_auth_disabled(self, client, monkeypatch, tmp_path):
        monkeypatch.setenv("HERMES_SYNC_TOKEN", "")
        monkeypatch.setattr("app.api.routes.hermes.HERMES_DIR", tmp_path)
        monkeypatch.setattr(
            "app.services.hermes_devsync_service.sync_dev_events",
            lambda: {"status": "ok", "synced": 0},
        )
        resp = client.post("/hermes/dev/sync?source=claude_code", json={})
        assert resp.status_code == 200

    def test_non_github_actions_requires_developer_role_when_auth_enabled(self, client, monkeypatch):
        monkeypatch.setattr("app.core.security.AUTH_ENABLED", True)

        resp = client.post(
            "/hermes/dev/sync?source=claude_code",
            json={},
            headers={"Authorization": "Bearer wrong-token"},
        )

        assert resp.status_code == 403

    def test_non_github_actions_allows_developer_token_when_auth_enabled(self, client, monkeypatch, tmp_path):
        from app.core import security

        monkeypatch.setattr("app.core.security.AUTH_ENABLED", True)
        monkeypatch.setitem(security.TOKEN_ROLE_MAP, "dev-token", "developer")
        monkeypatch.setenv("HERMES_SYNC_TOKEN", "")
        monkeypatch.setattr("app.api.routes.hermes.HERMES_DIR", tmp_path)
        monkeypatch.setattr(
            "app.services.hermes_devsync_service.sync_dev_events",
            lambda: {"status": "ok", "synced": 0},
        )

        resp = client.post(
            "/hermes/dev/sync?source=claude_code",
            json={},
            headers={"X-Auth-Token": "dev-token"},
        )

        assert resp.status_code == 200


# ── Command execution route-consolidation tests ──────────────────────


class TestCommandExecution:
    def test_delegates_to_service(self, client, monkeypatch):
        monkeypatch.setenv("HERMES_RUN_ENABLED", "true")
        expected = {
            "commandId": "source-quality",
            "runId": "run_test",
            "status": "success",
            "exitCode": 0,
        }
        monkeypatch.setattr(
            "app.api.routes.hermes.execute_hermes_command",
            lambda command_id, *, parameters, actor, session_id: expected,
        )
        resp = client.post("/hermes/commands/execute", json={"commandId": "source-quality"})
        assert resp.status_code == 200
        assert resp.json()["commandId"] == "source-quality"

    def test_missing_command_id_returns_400(self, client):
        resp = client.post("/hermes/commands/execute", json={})
        assert resp.status_code == 400

    def test_runner_disabled_returns_403(self, client, monkeypatch):
        monkeypatch.setenv("HERMES_RUN_ENABLED", "false")
        resp = client.post("/hermes/commands/execute", json={"commandId": "source-quality"})
        assert resp.status_code == 403

    def test_passes_parameters_to_service(self, client, monkeypatch):
        monkeypatch.setenv("HERMES_RUN_ENABLED", "true")
        captured = {}

        def _capture(command_id, *, parameters, actor, session_id):
            captured.update({
                "command_id": command_id,
                "parameters": parameters,
                "session_id": session_id,
                "actor": actor,
            })
            return {"commandId": command_id, "runId": "r", "status": "success", "exitCode": 0}

        monkeypatch.setattr("app.api.routes.hermes.execute_hermes_command", _capture)
        client.post(
            "/hermes/commands/execute",
            json={
                "commandId": "code-audit",
                "parameters": {"base": "main", "head": "HEAD"},
                "sessionId": "sess-123",
            },
        )
        assert captured["command_id"] == "code-audit"
        assert captured["parameters"] == {"base": "main", "head": "HEAD"}
        assert captured["session_id"] == "sess-123"


# ── Per-command role enforcement ────────────────────────────────────


class TestRunEndpointRoleEnforcement:
    def test_run_endpoint_returns_200_when_role_ok(self, client, monkeypatch):
        monkeypatch.setenv("HERMES_RUN_ENABLED", "true")
        monkeypatch.setattr(
            "app.api.routes.hermes.execute_hermes_command",
            lambda *a, **kw: {"commandId": "source-quality", "runId": "r", "status": "success", "exitCode": 0},
        )
        resp = client.post("/hermes/run/source-quality")
        # AUTH_ENABLED=False → all users are admin → should pass
        assert resp.status_code == 200

    def test_commands_list_returns_required_role(self, client):
        resp = client.get("/hermes/commands")
        assert resp.status_code == 200
        for cmd in resp.json():
            assert "requiredRole" in cmd

    def test_commands_code_audit_requires_developer(self, client):
        resp = client.get("/hermes/commands")
        code_audit = [c for c in resp.json() if c["commandId"] == "code-audit"][0]
        assert code_audit["requiredRole"] == "developer"


class TestMermaidRegex:
    def test_finds_single_block(self):
        blocks = MERMAID_BLOCK_RE.findall(MD_SINGLE_FLOWCHART)
        assert len(blocks) == 1
        assert "flowchart TD" in blocks[0]

    def test_finds_multiple_blocks(self):
        blocks = MERMAID_BLOCK_RE.findall(MD_TWO_DIAGRAMS)
        assert len(blocks) == 2

    def test_no_match_on_no_mermaid(self):
        blocks = MERMAID_BLOCK_RE.findall(MD_NO_MERMAID)
        assert blocks == []

    def test_mermaid_with_trailing_whitespace(self):
        src = "```mermaid  \nflowchart TD\n    A\n```"
        blocks = MERMAID_BLOCK_RE.findall(src)
        assert len(blocks) == 1

    def test_empty_mermaid_block_is_extracted(self):
        src = "```mermaid\n```"
        blocks = MERMAID_BLOCK_RE.findall(src)
        assert len(blocks) == 1
        assert blocks[0].strip() == ""
