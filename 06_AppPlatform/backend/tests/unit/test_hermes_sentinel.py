import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.services import hermes_sentinel_service as sentinel


def _ok_probe(name: str) -> dict:
    return {"probe": name, "overall": "ok", "findings": []}


def test_run_all_probes_returns_active_notifications_during_cooldown(monkeypatch, tmp_path: Path):
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
    assert sentinel.get_notifications(status="new") == []
    assert sentinel.get_notifications(status="archived")[0]["id"] == created["id"]


def test_probe_deploy_reports_commit_drift(monkeypatch):
    monkeypatch.setattr(
        "app.services.hermes_deploy_status_service.get_deploy_status",
        lambda: {
            "status": "critical",
            "release": {"commitSha": "1111111111111111", "shortSha": "11111111"},
            "expected": {"commitSha": "2222222222222222", "shortSha": "22222222"},
            "drift": {
                "isDrift": True,
                "releaseUnknown": False,
                "releaseCommitSha": "1111111111111111",
                "expectedCommitSha": "2222222222222222",
                "commitsBehind": 40,
            },
            "lastDeploy": {"available": False},
        },
    )

    result = sentinel.probe_deploy()

    assert result["overall"] == "critical"
    assert result["findings"][0]["type"] == "production_commit_drift"
    assert "11111111" in result["findings"][0]["message"]
    assert "22222222" in result["findings"][0]["message"]


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

    assert result["probe"] == "pipeline_failures"
    assert result["overall"] == "critical"
    assert result["findings"][0]["type"] == "missing_status_file"


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
    assert result["findings"][0]["type"] == "pipeline_msrp_ingest_failure"
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
    assert result["findings"][0]["type"] == "pipeline_voc_degraded"
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
    assert result["findings"][0]["type"] == "pipeline_voc_degraded"
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
    assert "missing_pipeline_status" in finding_types
    assert "pipeline_news_stale" in finding_types
    assert "stale_source_quality_report" in finding_types
