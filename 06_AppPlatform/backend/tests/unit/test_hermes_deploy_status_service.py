import json
from pathlib import Path

from app.services import hermes_deploy_status_service as deploy_status


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_record_expected_deploy_writes_latest_commit(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)

    record = deploy_status.record_expected_deploy(
        {
            "commitSha": "abcdef1234567890",
            "branch": "main",
            "workflowRunId": "123",
            "workflowRunAttempt": "2",
            "repository": "owner/repo",
        },
        source="github_actions",
    )

    assert record is not None
    assert record["commitSha"] == "abcdef1234567890"
    assert record["shortSha"] == "abcdef12"
    assert record["source"] == "github_actions"
    written = json.loads((tmp_path / "hermes" / "deploy_expected.json").read_text())
    assert written["workflowRunId"] == "123"


def test_get_deploy_status_reports_no_drift_when_release_matches_expected(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
    sha = "abcdef1234567890abcdef1234567890abcdef12"
    _write_json(tmp_path / "hermes" / "deploy_release.json", {
        "commitSha": sha,
        "shortSha": "abcdef12",
        "source": "github_actions_archive",
    })
    _write_json(tmp_path / "hermes" / "deploy_expected.json", {
        "commitSha": sha[:12],
        "branch": "main",
    })

    status = deploy_status.get_deploy_status()

    assert status["status"] == "ok"
    assert status["drift"]["isDrift"] is False
    assert status["release"]["confidence"] == "high"


def test_get_deploy_status_reports_drift(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
    _write_json(tmp_path / "hermes" / "deploy_release.json", {
        "commitSha": "1111111111111111111111111111111111111111",
        "shortSha": "11111111",
        "source": "github_actions_archive",
    })
    _write_json(tmp_path / "hermes" / "deploy_expected.json", {
        "commitSha": "2222222222222222222222222222222222222222",
        "shortSha": "22222222",
        "branch": "main",
    })

    status = deploy_status.get_deploy_status()

    assert status["status"] == "critical"
    assert status["drift"]["isDrift"] is True
    assert status["drift"]["releaseCommitSha"].startswith("11111111")
    assert status["drift"]["expectedCommitSha"].startswith("22222222")


def test_get_deploy_status_reports_failed_last_deploy(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
    dist = tmp_path / "06_AppPlatform" / "frontend" / "dist"
    dist.mkdir(parents=True)
    (dist / "_deploy_status.txt").write_text(
        "deploy_exit_code=1\ntimestamp=Mon May 18 00:00:00 UTC 2026\n",
        encoding="utf-8",
    )

    status = deploy_status.get_deploy_status()

    assert status["status"] == "critical"
    assert status["lastDeploy"]["deployExitCode"] == "1"
    assert any("non-zero" in warning for warning in status["warnings"])
