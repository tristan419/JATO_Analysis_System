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
    assert record["expectedCommitSha"] == "abcdef1234567890"
    assert record["shortSha"] == "abcdef12"
    assert record["environment"] == "production"
    assert record["source"] == "github_actions"
    written = json.loads((tmp_path / "hermes" / "deploy_expected.json").read_text())
    assert written["workflowRunId"] == "123"


def test_get_deploy_status_reports_no_drift_when_release_matches_expected(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
    sha = "abcdef1234567890abcdef1234567890abcdef12"
    _write_json(tmp_path / "hermes" / "deploy_release.json", {
        "expectedCommitSha": sha,
        "actualCommitSha": sha,
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
    assert status["release"]["actualCommitSha"] == sha
    assert status["conditions"]["productionRevision"]["status"] == "ok"


def test_get_deploy_status_reports_drift(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
    _write_json(tmp_path / "hermes" / "deploy_release.json", {
        "expectedCommitSha": "2222222222222222222222222222222222222222",
        "actualCommitSha": "1111111111111111111111111111111111111111",
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
    assert status["drift"]["actualCommitSha"].startswith("11111111")
    assert status["drift"]["expectedCommitSha"].startswith("22222222")
    assert status["conditions"]["productionRevision"]["status"] == "critical"
    assert status["conditions"]["productionRevision"]["type"] == "production_commit_drift"
    assert status["conditions"]["deployPipeline"]["status"] == "unknown"


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
    assert status["drift"]["isDrift"] is False
    assert status["lastDeploy"]["deployExitCode"] == "1"
    assert status["conditions"]["productionRevision"]["status"] == "unknown"
    assert status["conditions"]["deployPipeline"]["status"] == "critical"
    assert status["conditions"]["deployPipeline"]["type"] == "last_deploy_failed"
    assert any("non-zero" in warning for warning in status["warnings"])


def test_deploy_pipeline_failure_is_separate_from_matching_production_revision(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
    sha = "abcdef1234567890abcdef1234567890abcdef12"
    _write_json(tmp_path / "hermes" / "deploy_release.json", {
        "expectedCommitSha": sha,
        "actualCommitSha": sha,
        "shortSha": "abcdef12",
        "source": "github_actions_archive",
    })
    _write_json(tmp_path / "hermes" / "deploy_expected.json", {
        "commitSha": sha,
        "branch": "main",
    })
    dist = tmp_path / "06_AppPlatform" / "frontend" / "dist"
    dist.mkdir(parents=True)
    (dist / "_deploy_status.txt").write_text(
        "deploy_exit_code=255\ntimestamp=Thu Jun 11 10:00:00 UTC 2026\n",
        encoding="utf-8",
    )

    status = deploy_status.get_deploy_status()

    assert status["status"] == "critical"
    assert status["drift"]["isDrift"] is False
    assert status["conditions"]["productionRevision"]["status"] == "ok"
    assert status["conditions"]["deployPipeline"]["status"] == "critical"
    assert status["conditions"]["deployPipeline"]["deployExitCode"] == "255"


def test_release_metadata_accepts_legacy_commit_field(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
    sha = "abcdef1234567890abcdef1234567890abcdef12"
    _write_json(tmp_path / "hermes" / "deploy_release.json", {
        "commit": sha,
        "source": "manual_script",
    })

    release = deploy_status.get_release_metadata()

    assert release["expectedCommitSha"] == sha
    assert release["actualCommitSha"] == sha
    assert release["commitSha"] == sha
