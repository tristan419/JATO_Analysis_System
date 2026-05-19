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
