from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_tencent_deploy_workflow_excludes_local_tooling_and_temp_artifacts() -> None:
    workflow = (REPO_ROOT / ".github/workflows/deploy-fullstack-tencent.yml").read_text(
        encoding="utf-8",
    )

    assert "--exclude='.claude'" in workflow
    assert "--exclude='tmp'" in workflow
    assert "--exclude='*.pyc'" in workflow


def test_tencent_deploy_workflow_preserves_runtime_artifacts() -> None:
    workflow = (REPO_ROOT / ".github/workflows/deploy-fullstack-tencent.yml").read_text(
        encoding="utf-8",
    )

    assert "03_Scripts/diagnostics/artifacts" in workflow
    assert "03_Scripts/logs" in workflow
    assert "hermes/reports" in workflow
    assert "Preserved runtime path" in workflow
    assert "Restored runtime path" in workflow


def test_archive_deploy_reports_expected_commit_when_git_sync_is_skipped() -> None:
    script = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")

    assert '[[ "$SKIP_GIT_SYNC" == "true" && -n "${DEPLOY_COMMIT_SHA:-}" ]]' in script
    assert 'actual_commit="$DEPLOY_COMMIT_SHA"' in script


def test_gitignore_covers_local_tooling_and_temp_artifacts() -> None:
    lines = {
        line.strip()
        for line in (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    }

    assert ".claude/" in lines
    assert "tmp/" in lines
