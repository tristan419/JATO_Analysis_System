from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_production_release_excludes_local_tooling_and_temp_artifacts() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "--exclude='.claude'" in workflow
    assert "--exclude='tmp'" in workflow
    assert "--exclude='*.pyc'" in workflow


def test_tencent_remote_release_preserves_runtime_artifacts() -> None:
    script = (REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh").read_text(
        encoding="utf-8",
    )

    assert "03_Scripts/diagnostics/artifacts" in script
    assert "03_Scripts/logs" in script
    assert "06_AppPlatform/frontend/dist" in script
    assert "hermes/reports" in script
    assert "Preserved runtime path" in script
    assert "Restored runtime path" in script


def test_intl_edge_prewarm_verifies_completed_release_provenance_first() -> None:
    workflow = (REPO_ROOT / ".github/workflows/intl-edge-prewarm.yml").read_text(
        encoding="utf-8",
    )

    assert "Verify completed immutable release and intl provenance" in workflow
    assert "Prewarm intl edge cache" in workflow
    assert workflow.index("Verify completed immutable release and intl provenance") < workflow.index(
        "Prewarm intl edge cache",
    )
    assert "npm run perf:prewarm-edge" in workflow
    assert "JATO_PREWARM_ROLES" in workflow
    assert "viewer,order_filler" in workflow
    assert "JATO_PREWARM_GROUP_BY" in workflow
    assert "JATO_PREWARM_FAIL_ON_ERROR" in workflow


def test_tencent_release_upload_retries_in_chunks_but_never_falls_back() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "split -b 8M" in workflow
    assert "Upload complete release archive without fallback" in workflow
    assert "fallback to sparse" not in workflow


def test_tencent_uploads_verified_archive_before_deploy_step() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    verify_index = workflow.index("Verify frontend artifact before Tencent deployment")
    upload_index = workflow.index("Upload complete release archive without fallback")
    deploy_index = workflow.index("Deploy verified release on Tencent")
    assert verify_index < upload_index < deploy_index
    assert "frontend-release.json" in workflow
    assert "frontend-dist.tar.gz" in workflow


def test_tencent_archive_checks_initial_tar_members_with_dot_prefix() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert (
        'tar tzf "$RUNNER_TEMP/JATO_deploy.tar.gz" '
        "./hermes/frontend_release/frontend-release.json"
    ) in workflow
    assert (
        'tar tzf "$RUNNER_TEMP/JATO_deploy.tar.gz" '
        "./hermes/frontend_release/frontend-dist.tar.gz"
    ) in workflow


def test_archive_deploy_reports_expected_commit_when_git_sync_is_skipped() -> None:
    script = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")

    assert '[[ "$SKIP_GIT_SYNC" == "true" && -n "${DEPLOY_COMMIT_SHA:-}" ]]' in script
    assert 'actual_commit="$DEPLOY_COMMIT_SHA"' in script


def test_database_migration_requires_main_production_release_workflow() -> None:
    script = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")

    assert 'DEPLOY_BRANCH" != "main"' in script
    assert 'PRODUCTION_RELEASE_WORKFLOW" != "true"' in script
    assert "Database migrations require the main production release workflow" in script
    assert script.index("Database migrations require the main production release workflow") < script.index(
        "python -m alembic upgrade head"
    )


def test_msrp_systemd_service_does_not_force_local_proxy() -> None:
    service = (
        REPO_ROOT / "03_Scripts/deploy/systemd/jato-msrp-sync@.service"
    ).read_text(encoding="utf-8")
    env_example = (
        REPO_ROOT / "03_Scripts/deploy/systemd/jato-msrp.env.example"
    ).read_text(encoding="utf-8")

    assert "Environment=http_proxy=http://127.0.0.1:7897" not in service
    assert "Environment=https_proxy=http://127.0.0.1:7897" not in service
    assert "Leave unset for direct official-site access" in env_example
    assert "JATO_MSRP_CONCURRENCY=2" in env_example
    assert "JATO_MSRP_MAX_DRYRUN_CONCURRENCY=2" in env_example
    assert "JATO_MSRP_ALLOW_HIGH_CONCURRENCY=false" in env_example


def test_deploy_bootstraps_missing_msrp_dryrun_artifacts_async() -> None:
    script = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")

    assert "BOOTSTRAP_MSRP_DRYRUN_IF_MISSING" in script
    assert "03_Scripts/diagnostics/artifacts/dryrun_report.json" in script
    assert "03_Scripts/diagnostics/artifacts/dryrun_runs_index.json" in script
    assert "_write_msrp_status() {" in script
    assert "pipeline_status_writer.py" in script
    assert '2>/dev/null || true' in script
    assert "systemctl start --no-block \"$service_name\"" in script
    assert "bootstrap_msrp_dryrun_if_missing" in script
    assert '_write_msrp_status "msrp_dryrun" "running"' in script
    assert "record_active_msrp_dryrun_status" in script
    assert "recording running MSRP dryrun status" in script


def test_msrp_runner_writes_running_status_and_caps_country_runtime() -> None:
    script = (REPO_ROOT / "03_Scripts/run_msrp_low_concurrency.sh").read_text(
        encoding="utf-8",
    )

    assert "JATO_MSRP_COUNTRY_TIMEOUT_SECONDS" in script
    assert "JATO_MSRP_MAX_DRYRUN_CONCURRENCY" in script
    assert "JATO_MSRP_ALLOW_HIGH_CONCURRENCY" in script
    assert "Dryrun concurrency requested=" in script
    assert 'PIPELINE_ID="msrp_${MODE}"' in script
    assert '_write_msrp_status "$PIPELINE_ID" "running"' in script
    assert "MSRP_RUNTIME_METADATA" in script
    assert '"effectiveConcurrency"' in script
    assert "'metadata': runtime_metadata" in script
    assert "requested_concurrency=$REQUESTED_CONCURRENCY" in script
    assert 'run_cmd=("$TIMEOUT_BIN" "${COUNTRY_TIMEOUT_SECONDS}s" "${cmd[@]}")' in script
    assert "[TIMEOUT] country=$country exceeded" in script
    assert 'COUNTRY_DONE_DIR="$RUN_DIR/.country_done"' in script
    assert "wait_for_finished_country() {" in script
    assert "remove_country_pid_at() {" in script
    assert 'finished_index="$(wait_for_finished_country)"' in script
    assert 'pid="${pids[0]}"' not in script


def test_public_deploy_status_reports_msrp_scheduler_and_artifacts() -> None:
    script = (REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh").read_text(
        encoding="utf-8",
    )

    assert "---msrp scheduler---" in script
    assert "systemctl status jato-msrp-dryrun.timer" in script
    assert "systemctl status jato-msrp-sync@dryrun.service" in script
    assert "systemctl list-timers --all 'jato-msrp*'" in script
    assert "---msrp env---" in script
    assert "03_Scripts/ops/print_msrp_env_status.sh" in script
    assert "---msrp artifacts---" in script
    assert "03_Scripts/diagnostics/artifacts/dryrun_report.json" in script
    assert "03_Scripts/diagnostics/artifacts/dryrun_runs_index.json" in script
    assert "hermes/reports/msrp_country_progress.json" in script
    assert "hermes/reports/pipeline_status/msrp_dryrun.json" in script

    script = (REPO_ROOT / "03_Scripts/ops/print_msrp_env_status.sh").read_text(
        encoding="utf-8",
    )
    assert "JATO_MSRP_CONCURRENCY" in script
    assert "JATO_MSRP_MAX_DRYRUN_CONCURRENCY" in script
    assert "JATO_MSRP_ALLOW_HIGH_CONCURRENCY" in script
    assert "proxy_configured=" in script
    assert "DEEPSEEK_API_KEY" not in script


def test_gitignore_covers_local_tooling_and_temp_artifacts() -> None:
    lines = {
        line.strip()
        for line in (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    }

    assert ".claude/" in lines
    assert "tmp/" in lines
