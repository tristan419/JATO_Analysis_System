import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.services import hermes_sentinel_service as sentinel


def _ok_probe(name: str) -> dict:
    return {"probe": name, "overall": "ok", "findings": []}


def test_run_all_probes_updates_existing_notification_without_reemitting(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    monkeypatch.setattr(sentinel, "probe_devsync", lambda: {
        "probe": "devsync",
        "overall": "warning",
        "findings": [{
            "type": "missing_docs",
            "severity": "medium",
            "message": "1 feature has no documentation.",
            "count": 1,
            "features": ["feature.test"],
        }],
    })
    monkeypatch.setattr(sentinel, "probe_workspace", lambda: _ok_probe("workspace"))
    monkeypatch.setattr(sentinel, "probe_gaps", lambda: _ok_probe("gaps"))
    monkeypatch.setattr(sentinel, "probe_evidence", lambda: _ok_probe("evidence"))
    monkeypatch.setattr(sentinel, "probe_gha", lambda: _ok_probe("gha"))
    monkeypatch.setattr(sentinel, "probe_deploy", lambda: _ok_probe("deploy"))
    monkeypatch.setattr(sentinel, "probe_pipeline_failures", lambda: _ok_probe("pipeline_failures"))

    first = sentinel.run_all_probes()
    assert first["overall"] == "warning"
    assert len(first["emittedNotifications"]) == 1
    assert len(first["notifications"]) == 1
    assert first["notifications"][0]["actionLevel"] == "needs_review"
    assert first["notifications"][0]["blocking"] is False
    assert first["notifications"][0]["recommendedAction"]

    second = sentinel.run_all_probes()
    assert second["emittedNotifications"] == []
    assert len(second["notifications"]) == 1
    assert second["unreadCount"] == 1
    assert second["notifications"][0]["id"] == first["notifications"][0]["id"]
    assert second["notifications"][0]["occurrenceCount"] == 2
    assert second["notifications"][0]["lastSeenAt"]


def test_set_notification_status_supports_mailbox_states(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    created = sentinel._emit(
        "devsync",
        "medium",
        "Missing Docs",
        "1 feature has no documentation.",
        context={"actionLevel": "needs_review", "blocking": False},
    )
    assert created is not None

    acked = sentinel.set_notification_status(created["id"], "acked")
    assert acked is not None
    assert acked["status"] == "acked"

    archived = sentinel.set_notification_status(created["id"], "archived")
    assert archived is not None
    assert archived["status"] == "archived"
    assert (tmp_path / "hermes" / "sentinel_notification_state.json").is_file()
    assert sentinel.get_notifications(status="new") == []
    assert sentinel.get_notifications(status="archived")[0]["id"] == created["id"]


def test_repeated_finding_preserves_read_state(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    monkeypatch.setattr(sentinel, "probe_devsync", lambda: {
        "probe": "devsync",
        "overall": "warning",
        "findings": [{
            "type": "missing_docs",
            "severity": "medium",
            "message": "1 feature has no documentation.",
            "count": 1,
        }],
    })
    monkeypatch.setattr(sentinel, "probe_workspace", lambda: _ok_probe("workspace"))
    monkeypatch.setattr(sentinel, "probe_gaps", lambda: _ok_probe("gaps"))
    monkeypatch.setattr(sentinel, "probe_evidence", lambda: _ok_probe("evidence"))
    monkeypatch.setattr(sentinel, "probe_gha", lambda: _ok_probe("gha"))
    monkeypatch.setattr(sentinel, "probe_deploy", lambda: _ok_probe("deploy"))
    monkeypatch.setattr(sentinel, "probe_pipeline_failures", lambda: _ok_probe("pipeline_failures"))

    first = sentinel.run_all_probes()
    notification_id = first["notifications"][0]["id"]
    read = sentinel.set_notification_status(notification_id, "read")
    assert read is not None
    assert read["status"] == "read"

    second = sentinel.run_all_probes()

    assert second["emittedNotifications"] == []
    assert second["notifications"][0]["id"] == notification_id
    assert second["notifications"][0]["status"] == "read"
    assert second["notifications"][0]["occurrenceCount"] == 2
    assert second["unreadCount"] == 0


def test_deploy_pipeline_fingerprint_ignores_exit_code():
    base = {
        "findingType": "last_deploy_failed",
        "conditionId": "deploy_pipeline",
        "resourceId": "deploy-fullstack-tencent",
        "environment": "production",
        "workflow": "deploy-fullstack-tencent",
        "stage": "ssh",
        "conclusion": "failure",
        "statusPath": "06_AppPlatform/frontend/dist/_deploy_status.txt",
        "deployExitCode": "1",
    }
    changed_exit_code = {**base, "deployExitCode": "255"}

    assert (
        sentinel._notification_fingerprint("deploy", "Last Deploy Failed", base)
        == sentinel._notification_fingerprint("deploy", "Last Deploy Failed", changed_exit_code)
    )


def test_probe_deploy_reports_commit_drift(monkeypatch):
    monkeypatch.setattr(
        "app.services.hermes_deploy_status_service.get_deploy_status",
        lambda: {
            "status": "critical",
            "release": {
                "actualCommitSha": "1111111111111111",
                "commitSha": "1111111111111111",
                "shortSha": "11111111",
                "metadataPath": "hermes/deploy_release.json",
                "environment": "production",
            },
            "expected": {
                "expectedCommitSha": "2222222222222222",
                "commitSha": "2222222222222222",
                "shortSha": "22222222",
                "metadataPath": "hermes/deploy_expected.json",
            },
            "drift": {
                "isDrift": True,
                "releaseUnknown": False,
                "releaseCommitSha": "1111111111111111",
                "actualCommitSha": "1111111111111111",
                "expectedCommitSha": "2222222222222222",
                "commitsBehind": 40,
            },
            "lastDeploy": {"available": False},
            "conditions": {
                "productionRevision": {
                    "id": "production_revision",
                    "status": "critical",
                    "type": "production_commit_drift",
                },
                "deployPipeline": {
                    "id": "deploy_pipeline",
                    "status": "unknown",
                    "type": "last_deploy_status_missing",
                },
            },
        },
    )

    result = sentinel.probe_deploy()

    assert result["overall"] == "critical"
    assert result["findings"][0]["type"] == "production_commit_drift"
    assert result["findings"][0]["conditionId"] == "production_revision"
    assert result["findings"][0]["conditionStatus"] == "critical"
    assert result["findings"][0]["releaseMetadataPath"] == "hermes/deploy_release.json"
    assert "11111111" in result["findings"][0]["message"]
    assert "22222222" in result["findings"][0]["message"]


def test_probe_deploy_reports_pipeline_failure_separately_from_revision(monkeypatch):
    monkeypatch.setattr(
        "app.services.hermes_deploy_status_service.get_deploy_status",
        lambda: {
            "status": "critical",
            "release": {
                "actualCommitSha": "2222222222222222",
                "commitSha": "2222222222222222",
                "shortSha": "22222222",
                "environment": "production",
            },
            "expected": {
                "expectedCommitSha": "2222222222222222",
                "commitSha": "2222222222222222",
                "shortSha": "22222222",
            },
            "drift": {
                "isDrift": False,
                "releaseUnknown": False,
                "releaseCommitSha": "2222222222222222",
                "actualCommitSha": "2222222222222222",
                "expectedCommitSha": "2222222222222222",
                "commitsBehind": 0,
            },
            "lastDeploy": {
                "available": True,
                "deployExitCode": "255",
                "path": "06_AppPlatform/frontend/dist/_deploy_status.txt",
                "timestamp": "Thu Jun 11 10:00:00 UTC 2026",
            },
            "conditions": {
                "productionRevision": {
                    "id": "production_revision",
                    "status": "ok",
                    "type": "production_revision_ok",
                },
                "deployPipeline": {
                    "id": "deploy_pipeline",
                    "status": "critical",
                    "type": "last_deploy_failed",
                },
            },
        },
    )

    result = sentinel.probe_deploy()

    assert result["overall"] == "critical"
    assert len(result["findings"]) == 1
    assert result["findings"][0]["type"] == "last_deploy_failed"
    assert result["findings"][0]["conditionId"] == "deploy_pipeline"
    assert result["findings"][0]["deployExitCode"] == "255"
    assert result["findings"][0]["statusPath"] == "06_AppPlatform/frontend/dist/_deploy_status.txt"


def test_probe_workspace_delegates_to_service(monkeypatch):
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

    result = sentinel.probe_workspace()
    assert result["probe"] == "workspace"
    assert result["overall"] == "warning"
    assert len(result["findings"]) == 1
    assert result["findings"][0]["type"] == "unlinked_changes"
    assert result["findings"][0]["count"] == 1


def test_probe_workspace_no_findings_when_clean(monkeypatch):
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

    result = sentinel.probe_workspace()
    assert result["overall"] == "ok"
    assert result["findings"] == []


def _write_pipeline_inputs(
    tmp_path: Path,
    status: dict[str, dict],
    *,
    source_quality_generated_at: str | None = None,
) -> None:
    status_path = tmp_path / "03_Scripts" / "logs" / "scheduled_fetch_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status), encoding="utf-8")

    last_run_at = source_quality_generated_at
    if last_run_at is None:
        for entry in status.values():
            if entry.get("lastRunAt"):
                last_run_at = entry["lastRunAt"]
                break
    if last_run_at:
        status_dir = tmp_path / "hermes" / "reports" / "pipeline_status"
        status_dir.mkdir(parents=True, exist_ok=True)
        for pipeline_id in (
            "msrp_current_price_snapshot",
            "msrp_readiness_audit",
            "ai_intelligence_enrichment_smoke",
            "unified_scraping_readiness",
        ):
            (status_dir / f"{pipeline_id}.json").write_text(
                json.dumps({
                    "pipelineId": pipeline_id,
                    "status": "success",
                    "lastRunAt": last_run_at,
                    "finishedAt": last_run_at,
                }),
                encoding="utf-8",
            )

    if source_quality_generated_at is not None:
        report_path = tmp_path / "hermes" / "reports" / "source_quality_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps({"generatedAt": source_quality_generated_at}),
            encoding="utf-8",
        )


def test_probe_pipeline_failures_reports_missing_status_file(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)

    result = sentinel.probe_pipeline_failures()
    finding_types = {finding["type"] for finding in result["findings"]}

    assert result["probe"] == "pipeline_failures"
    assert result["overall"] == "warning"
    assert "pipeline_jato_etl_missing" in finding_types
    assert "pipeline_msrp_dryrun_missing" in finding_types
    assert result["findings"][0]["statusRecord"]["status"] == "missing"


def test_probe_pipeline_failures_ok_when_all_statuses_are_fresh(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_pipeline_inputs(
        tmp_path,
        {
            "news": {"status": "success", "lastRunAt": now},
            "voc": {"status": "success", "lastRunAt": now},
            "msrp_dryrun": {"status": "success", "lastRunAt": now},
            "msrp_ingest": {"status": "success", "lastRunAt": now},
            "jato_etl": {"status": "success", "lastRunAt": now},
        },
        source_quality_generated_at=now,
    )

    result = sentinel.probe_pipeline_failures()

    assert result["overall"] == "ok"
    assert result["findings"] == []


def test_probe_pipeline_failures_flags_failed_ingest(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_pipeline_inputs(
        tmp_path,
        {
            "news": {"status": "success", "lastRunAt": now},
            "voc": {"status": "success", "lastRunAt": now},
            "msrp_dryrun": {"status": "success", "lastRunAt": now},
            "msrp_ingest": {"status": "failure", "lastRunAt": now},
            "jato_etl": {"status": "success", "lastRunAt": now},
        },
        source_quality_generated_at=now,
    )

    result = sentinel.probe_pipeline_failures()

    assert result["overall"] == "critical"
    assert result["findings"][0]["type"] == "pipeline_msrp_ingest_failed"
    assert result["findings"][0]["severity"] == "critical"


def test_probe_pipeline_failures_accepts_failed_count_alias(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_pipeline_inputs(
        tmp_path,
        {
            "news": {"status": "success", "lastRunAt": now},
            "voc": {
                "status": "degraded",
                "lastRunAt": now,
                "successCount": 2,
                "failedCount": 3,
            },
            "msrp_dryrun": {"status": "success", "lastRunAt": now},
            "msrp_ingest": {"status": "success", "lastRunAt": now},
            "jato_etl": {"status": "success", "lastRunAt": now},
        },
        source_quality_generated_at=now,
    )

    result = sentinel.probe_pipeline_failures()

    assert result["overall"] == "warning"
    assert result["findings"][0]["type"] == "pipeline_voc_forum_sync_degraded"
    assert "3 failed" in result["findings"][0]["message"]


def test_probe_pipeline_failures_treats_partial_success_as_degraded(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_pipeline_inputs(
        tmp_path,
        {
            "news": {"status": "success", "lastRunAt": now},
            "voc": {
                "status": "partial_success",
                "lastRunAt": now,
                "successCount": 40,
                "failedCount": 2,
            },
            "msrp_dryrun": {"status": "success", "lastRunAt": now},
            "msrp_ingest": {"status": "success", "lastRunAt": now},
            "jato_etl": {"status": "success", "lastRunAt": now},
        },
        source_quality_generated_at=now,
    )

    result = sentinel.probe_pipeline_failures()

    assert result["overall"] == "warning"
    assert result["findings"][0]["type"] == "pipeline_voc_forum_sync_degraded"
    assert "2 failed" in result["findings"][0]["message"]


def test_probe_pipeline_failures_flags_missing_and_stale_inputs(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sentinel, "_project_root", tmp_path)
    stale = (datetime.now(timezone.utc) - timedelta(hours=40)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_pipeline_inputs(
        tmp_path,
        {
            "news": {"status": "success", "lastRunAt": stale},
            "voc": {"status": "success", "lastRunAt": stale},
        },
        source_quality_generated_at=stale,
    )

    result = sentinel.probe_pipeline_failures()
    finding_types = {finding["type"] for finding in result["findings"]}

    assert result["overall"] == "warning"
    assert "pipeline_msrp_dryrun_missing" in finding_types
    assert "pipeline_country_news_sync_stale" in finding_types
    assert "pipeline_source_quality_stale" in finding_types
