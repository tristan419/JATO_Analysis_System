import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[4]
RELEASE_PREPARATION_HELPER = (
    REPO_ROOT / "03_Scripts/deploy/prepare_backend_release.py"
)
READINESS_HELPER = REPO_ROOT / "03_Scripts/deploy/verify_backend_readiness.py"


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


def test_tencent_release_upload_resumes_and_never_falls_back() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "Upload complete release archive without fallback" in workflow
    assert "fallback to sparse" not in workflow
    assert "timeout-minutes: 240" in workflow
    assert "cancel-in-progress: false" in workflow
    assert 'remote_archive="${remote_dir}/${archive_sha256}.tar.gz"' in workflow
    assert 'remote_temp="${remote_archive}.partial"' in workflow
    assert 'remote_lock="${remote_archive}.lock"' in workflow
    assert "--partial" in workflow
    assert "--append-verify" in workflow
    assert "flock -w 870" in workflow
    assert "df -Pk" in workflow
    assert "cat >> '$remote_temp'" not in workflow
    assert "local remote_output" in workflow
    assert 'printf \'%s\' "$remote_output"' in workflow
    assert "for upload_attempt in 1 2 3 4 5 6" in workflow
    assert "split -b 8M" not in workflow
    assert "sha256sum '$remote_temp'" in workflow
    assert workflow.index("sha256sum '$remote_temp'") < workflow.rindex(
        'echo "remote-archive=$remote_archive"',
    )
    assert "StrictHostKeyChecking=no" not in workflow
    assert "UserKnownHostsFile=/dev/null" not in workflow
    assert "--compress" not in workflow


def test_tencent_release_upload_replaces_unsafe_scratch_without_overwriting_final() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "reset_bad_partial()" in workflow
    assert "partial_reset_used=0" in workflow
    assert 'if [ "$partial_reset_used" -ne 0 ]; then' in workflow
    assert "rm -f '$remote_temp' '$remote_checksum'" in workflow
    assert "rm -f '$remote_temp' '$remote_archive'" not in workflow
    assert "Remote immutable archive exists but size or SHA-256 is wrong" in workflow
    assert "FINAL_BAD" in workflow
    assert "if [ -e '$remote_archive' ]; then" in workflow
    assert "test ! -e '$remote_archive'" in workflow
    assert workflow.index("test ! -e '$remote_archive'") < workflow.index(
        "ln '$remote_temp' '$remote_archive'",
    )


def test_tencent_release_requires_host_pin_and_hides_secrets_from_remote_argv() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "SSH_KNOWN_HOSTS" in workflow
    assert "StrictHostKeyChecking=yes" in workflow
    assert 'UserKnownHostsFile="$HOME/.ssh/known_hosts"' in workflow
    assert "missing_packages+=(rsync)" in workflow
    assert "missing_packages+=(sshpass)" in workflow
    assert 'sudo apt-get install -y "${missing_packages[@]}"' in workflow
    assert "remote_exports" not in workflow
    assert 'chmod 600 "$control_payload"' in workflow
    assert '"umask 077; exec bash -s" < "$control_payload"' in workflow


def test_backend_release_is_deterministic_and_closes_msrp_evidence_references() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert '"releaseId": release["releaseId"]' in workflow
    assert '"workflowRunAttempt": release["workflowRunAttempt"]' in workflow
    assert '"packagedAt": release["buildTimestamp"]' in workflow
    assert "--sort=name" in workflow
    assert "--owner=0" in workflow
    assert "--group=0" in workflow
    assert '--mtime="@$source_date_epoch"' in workflow
    assert 'gzip -n -f "$RUNNER_TEMP/JATO_deploy.tar"' in workflow
    assert 'value.get("localPath")' in workflow
    assert "missing MSRP localPath evidence" in workflow
    assert 'tar tzf "$RUNNER_TEMP/JATO_deploy.tar.gz" "$evidence_path"' in workflow
    assert "update_mihomo_subscription.sh" not in workflow
    assert '"expectedCommitSha": sha' in workflow
    assert '"actualCommitSha": ""' in workflow
    assert '"commitSha": ""' in workflow
    assert '"commitSha": sha' not in workflow


def test_release_checkpoints_cover_transport_ambiguity_and_final_parity() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    for phase in (
        "transport_verified",
        "www_verified",
        "intl_deploy_started",
        "intl_verified",
        "parity_verified",
        "complete",
    ):
        assert f"--phase {phase}" in workflow
    assert "retention-days: 7" in workflow
    assert "retention-days: 30" in workflow
    assert "steps.intl_current.outputs.current != 'true'" in workflow
    assert "for deploy_attempt in 1 2 3" in workflow
    assert workflow.index("Verify Tencent public release provenance") < workflow.index(
        "Verify www runtime API contract",
    ) < workflow.index("Record www-verified candidate checkpoint")
    assert "--profile www" in workflow


def test_server_checkpoint_and_evidence_are_bound_into_final_receipt() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "Fetch and attest server release checkpoint" in workflow
    assert "backend-healthy.json" in workflow
    assert "backend-healthy.evidence.json" in workflow
    assert "attestation_complete=false" in workflow
    assert 'rm -rf "$server_dir"' in workflow
    assert 'checkpoint.get("phase") != "backend_healthy"' in workflow
    assert 'checkpoint.get("status") != "completed"' in workflow
    assert "server checkpoint evidence binding mismatch" in workflow
    assert 'evidence.get("identity") != expected_identity' in workflow
    assert (
        'echo "Server checkpoint/evidence attestation SHA-256: '
        '$attestation_sha256"'
    ) in workflow
    assert 'echo "attestation-sha256=$attestation_sha256"' not in workflow
    assert "needs.deploy_tencent.outputs.server_attestation_sha256" not in workflow
    assert 'actual_attestation_sha256="$(sha256sum "$attestation"' in workflow
    assert 'candidate_identity != attestation.get("identity")' in workflow
    assert 'candidate_identity.get("archiveBytes")' in workflow
    assert 'candidate_identity.get("archiveSha256")' in workflow
    assert "needs.deploy_tencent.outputs.archive_bytes" not in workflow
    assert "needs.deploy_tencent.outputs.archive_sha256" not in workflow
    assert '--message "server_attestation_sha256=$actual_attestation_sha256"' in workflow
    assert workflow.index("Retain candidate release checkpoint") < workflow.index(
        "Download candidate release checkpoint"
    ) < workflow.index("Seal verified production release checkpoint")
    assert workflow.index("Deploy verified release on Tencent") < workflow.index(
        "Fetch and attest server release checkpoint",
    ) < workflow.index("Verify Tencent public release provenance")


def test_www_and_intl_switch_the_shared_artifact_in_one_protected_job() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "\n  deploy_intl:" not in workflow
    assert workflow.index("Validate Cloudflare deploy configuration") < workflow.index(
        "Upload complete release archive without fallback",
    )
    assert workflow.index("Deploy verified release on Tencent") < workflow.index(
        "Deploy downloaded dist to Cloudflare Pages",
    )
    assert "needs: [build_frontend, deploy_tencent]" in workflow


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

    assert 'if [[ -n "${DEPLOY_COMMIT_SHA:-}" ]]; then' in script
    assert 'actual_commit="$(target_backend_commit 2>/dev/null || true)"' in script


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


def _load_readiness_helper():
    spec = importlib.util.spec_from_file_location(
        "verify_backend_readiness_test_module",
        READINESS_HELPER,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _shell_function(script: str, name: str) -> str:
    start = script.index(f"{name}() {{")
    end = script.index("\n}\n", start) + len("\n}\n")
    return script[start:end]


def test_backend_readiness_helper_requires_ready_status_and_target_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper = _load_readiness_helper()
    target_commit = "a" * 40

    monkeypatch.setattr(
        helper,
        "_read_payload",
        lambda _url, _timeout: {
            "status": "ready",
            "release": {"commitSha": target_commit},
        },
    )
    observed = helper.verify_backend_readiness(
        url="http://127.0.0.1:8000/readyz",
        expected_commit=target_commit,
        timeout_seconds=10,
    )
    assert observed == {
        "status": "ready",
        "release": {"commitSha": target_commit},
    }

    monkeypatch.setattr(
        helper,
        "_read_payload",
        lambda _url, _timeout: {
            "status": "ready",
            "release": {"commitSha": "b" * 40},
        },
    )
    with pytest.raises(helper.ReadinessError, match="does not match") as error:
        helper.verify_backend_readiness(
            url="http://127.0.0.1:8000/readyz",
            expected_commit=target_commit,
            timeout_seconds=10,
        )
    assert error.value.code == "commit_mismatch"

    monkeypatch.setattr(
        helper,
        "_read_payload",
        lambda _url, _timeout: {
            "status": "degraded",
            "release": {"commitSha": target_commit},
        },
    )
    with pytest.raises(helper.ReadinessError, match="not ready") as error:
        helper.verify_backend_readiness(
            url="http://127.0.0.1:8000/readyz",
            expected_commit=target_commit,
            timeout_seconds=10,
        )
    assert error.value.code == "status_not_ready"


def test_backend_readiness_helper_emits_structured_failure() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(READINESS_HELPER),
            "--url",
            "http://127.0.0.1:8000/readyz",
            "--expected-commit",
            "not-a-commit",
        ],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stderr)
    assert payload["check"] == "backend_readyz"
    assert payload["ok"] is False
    assert payload["error"]["code"] == "invalid_expected_commit"


def test_backend_release_preparation_replaces_env_sha_and_creates_metadata(
    tmp_path: Path,
) -> None:
    target_commit = "a" * 40
    env_path = tmp_path / "backend.env"
    env_path.write_text(
        "APP_AUTH_ENABLED=false\n"
        "APP_RELEASE_SHA=old\n"
        "export APP_RELEASE_SHA=duplicate\n",
        encoding="utf-8",
    )
    env_result = subprocess.run(
        [
            sys.executable,
            str(RELEASE_PREPARATION_HELPER),
            "update-env",
            "--path",
            str(env_path),
            "--commit",
            target_commit,
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    assert env_result.returncode == 0, env_result.stderr
    env_lines = env_path.read_text(encoding="utf-8").splitlines()
    assert "APP_AUTH_ENABLED=false" in env_lines
    assert env_lines.count(f"APP_RELEASE_SHA={target_commit}") == 1
    assert sum("APP_RELEASE_SHA" in line for line in env_lines) == 1

    metadata_path = tmp_path / "hermes/deploy_release.json"
    metadata_result = subprocess.run(
        [
            sys.executable,
            str(RELEASE_PREPARATION_HELPER),
            "prepare-metadata",
            "--path",
            str(metadata_path),
            "--commit",
            target_commit,
            "--branch",
            "main",
            "--source",
            "direct_git_sync",
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    assert metadata_result.returncode == 0, metadata_result.stderr
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["expectedCommitSha"] == target_commit
    assert metadata["actualCommitSha"] == ""
    assert metadata["commitSha"] == ""
    assert metadata["source"] == "direct_git_sync"
    assert not list(metadata_path.parent.glob(f".{metadata_path.name}.*.tmp"))

    previous_commit = "b" * 40
    metadata["expectedCommitSha"] = previous_commit
    metadata["expectedShortSha"] = previous_commit[:8]
    metadata["actualCommitSha"] = previous_commit
    metadata["actualShortSha"] = previous_commit[:8]
    metadata["commitSha"] = previous_commit
    metadata["shortSha"] = previous_commit[:8]
    metadata["source"] = "stale_github_actions_archive"
    metadata_path.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
    direct_recovery_result = subprocess.run(
        [
            sys.executable,
            str(RELEASE_PREPARATION_HELPER),
            "prepare-metadata",
            "--path",
            str(metadata_path),
            "--commit",
            target_commit,
            "--branch",
            "main",
            "--source",
            "direct_git_recovery",
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    assert direct_recovery_result.returncode == 0, direct_recovery_result.stderr
    recovered_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert recovered_metadata["expectedCommitSha"] == target_commit
    assert recovered_metadata["actualCommitSha"] == previous_commit
    assert recovered_metadata["commitSha"] == previous_commit
    assert recovered_metadata["source"] == "direct_git_recovery"

    confirm_result = subprocess.run(
        [
            sys.executable,
            str(RELEASE_PREPARATION_HELPER),
            "confirm-metadata",
            "--path",
            str(metadata_path),
            "--commit",
            target_commit,
            "--service",
            "jato-fullstack-backend@8000",
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    assert confirm_result.returncode == 0, confirm_result.stderr
    confirmed_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert confirmed_metadata["actualCommitSha"] == target_commit
    assert confirmed_metadata["commitSha"] == target_commit
    assert confirmed_metadata["readyz"] == "ready"


def test_checkpoint_release_preparation_rejects_missing_or_mismatched_metadata(
    tmp_path: Path,
) -> None:
    metadata_path = tmp_path / "deploy_release.json"
    target_commit = "a" * 40
    command = [
        sys.executable,
        str(RELEASE_PREPARATION_HELPER),
        "prepare-metadata",
        "--path",
        str(metadata_path),
        "--commit",
        target_commit,
        "--branch",
        "main",
        "--source",
        "production_release",
        "--require-existing",
    ]

    missing_result = subprocess.run(
        command,
        capture_output=True,
        check=False,
        text=True,
    )
    assert missing_result.returncode == 1
    assert "checkpoint release metadata is missing" in missing_result.stderr

    original_payload = {
        "expectedCommitSha": "b" * 40,
        "commitSha": "b" * 40,
    }
    metadata_path.write_text(
        json.dumps(original_payload) + "\n",
        encoding="utf-8",
    )
    mismatch_result = subprocess.run(
        command,
        capture_output=True,
        check=False,
        text=True,
    )
    assert mismatch_result.returncode == 1
    assert "does not match the target commit" in mismatch_result.stderr
    assert json.loads(metadata_path.read_text(encoding="utf-8")) == original_payload

    packaged_payload = {
        "expectedCommitSha": target_commit,
        "actualCommitSha": "",
        "commitSha": "",
        "source": "github_actions_archive",
    }
    metadata_path.write_text(
        json.dumps(packaged_payload) + "\n",
        encoding="utf-8",
    )
    valid_result = subprocess.run(
        command,
        capture_output=True,
        check=False,
        text=True,
    )
    assert valid_result.returncode == 0, valid_result.stderr
    assert (
        json.loads(metadata_path.read_text(encoding="utf-8"))["source"]
        == "github_actions_archive"
    )
    prepared_packaged = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert prepared_packaged["actualCommitSha"] == ""
    assert prepared_packaged["commitSha"] == ""

    previous_commit = "c" * 40
    previous_path = tmp_path / "previous_deploy_release.json"
    previous_path.write_text(
        json.dumps(
            {
                "expectedCommitSha": previous_commit,
                "actualCommitSha": previous_commit,
                "commitSha": previous_commit,
            },
        )
        + "\n",
        encoding="utf-8",
    )
    preserve_result = subprocess.run(
        command + ["--previous-metadata", str(previous_path)],
        capture_output=True,
        check=False,
        text=True,
    )
    assert preserve_result.returncode == 0, preserve_result.stderr
    preserved_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert preserved_metadata["expectedCommitSha"] == target_commit
    assert preserved_metadata["actualCommitSha"] == previous_commit
    assert preserved_metadata["commitSha"] == previous_commit


def test_target_backend_commit_supports_git_worktree_metadata_file(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    worktree = tmp_path / "worktree"
    repository.mkdir()
    subprocess.run(
        ["git", "-C", str(repository), "init", "--quiet"],
        check=True,
        capture_output=True,
        text=True,
    )
    (repository / "seed.txt").write_text("seed\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(repository), "add", "seed.txt"],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repository),
            "-c",
            "user.name=Readiness Test",
            "-c",
            "user.email=readiness@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "seed",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "-C", str(repository), "worktree", "add", "--quiet", "--detach", str(worktree)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert (worktree / ".git").is_file()
    expected_commit = subprocess.run(
        ["git", "-C", str(worktree), "rev-parse", "--verify", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    server_script = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")
    result = subprocess.run(
        [
            "bash",
            "-c",
            "\n".join(
                (
                    "set -Eeuo pipefail",
                    'DEPLOY_COMMIT_SHA=""',
                    'RELEASE_CHECKPOINT_COMMIT=""',
                    f"REPO_DIR={str(worktree)!r}",
                    _shell_function(server_script, "target_backend_commit"),
                    "target_backend_commit",
                )
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected_commit


def test_deploy_gates_completed_and_new_releases_on_liveness_and_readiness() -> None:
    inner = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")
    outer = (
        REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
    ).read_text(encoding="utf-8")

    assert 'BACKEND_READINESS_HELPER="${BACKEND_READINESS_HELPER:-' in inner
    assert "completed_checkpoint_matches_local() {" in inner
    assert (
        '"http://127.0.0.1:${BACKEND_PORT}/healthz" >/dev/null 2>&1 \\\n'
        "    && verify_backend_readiness 10"
    ) in inner
    assert "Backend liveness and release readiness passed" in inner
    assert 'healthz=$health_ok readyz=$readiness_ok' in inner

    assert "03_Scripts/deploy/verify_backend_readiness.py" in outer
    assert "local_release_matches() {" in outer
    assert "http://127.0.0.1:8000/healthz" in outer
    assert 'http://127.0.0.1:8000/readyz' in outer
    assert 'final_readiness_exit_code=$FINAL_READINESS_RC' in outer
    assert "---readyz---" in outer
    assert "readiness_rc=$FINAL_READINESS_RC" in outer


def test_deploy_prepares_frozen_release_identity_before_backend_restart() -> None:
    inner = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")
    outer = (
        REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
    ).read_text(encoding="utf-8")
    bootstrap = (
        REPO_ROOT / "03_Scripts/ops/tencent_fullstack_bootstrap.sh"
    ).read_text(encoding="utf-8")

    assert "03_Scripts/deploy/prepare_backend_release.py" in outer
    env_function = _shell_function(outer, "install_backend_env_atomically")
    assert "update-env" in env_function
    assert '--commit "$DEPLOY_COMMIT_SHA"' in env_function
    assert 'sudo -n mv -f "$privileged_candidate" "$ENV_FILE"' in env_function
    env_install = outer.index("install_backend_env_atomically\n")
    inner_deploy = outer.index("bash 03_Scripts/deploy_fullstack_server.sh")
    assert env_install < inner_deploy

    update_repository = inner.index('CURRENT_STEP="Update repository"')
    resolve_identity = inner.index(
        'CURRENT_STEP="Resolve target backend release identity"',
    )
    switch_started = inner.index("switch_started in_progress rollback_required")
    prepare_identity = inner.index(
        'CURRENT_STEP="Prepare backend release identity"',
    )
    backend_restart = inner.index(
        'sudo -n systemctl restart "$BACKEND_SERVICE_NAME"',
        prepare_identity,
    )
    assert update_repository < resolve_identity < switch_started < prepare_identity
    assert prepare_identity < backend_restart
    assert "arguments+=(--require-existing)" in inner
    assert "Checkpointed production release requires explicit DEPLOY_COMMIT_SHA" in inner
    assert (
        'if ! checkpoint_enabled \\\n'
        '    && [[ -n "$git_commit" ]] \\\n'
        '    && [[ "$git_commit" != "$candidate" ]]'
    ) in inner
    health_loop = inner.index('echo "[INFO] Verify backend health"')
    readiness_call = inner.index("if verify_backend_readiness 10; then", health_loop)
    mark_call = inner.index("\nmark_release_deployed\n", readiness_call)
    assert readiness_call < mark_call
    assert "confirm-metadata" in _shell_function(inner, "mark_release_deployed")

    assert "APP_RELEASE_SHA=$DEPLOY_COMMIT_SHA" in bootstrap
    assert "Archive bootstrap requires explicit full lowercase DEPLOY_COMMIT_SHA" in bootstrap
    assert "DEPLOY_SOURCE=tencent_fullstack_bootstrap" in bootstrap
    assert '"http://127.0.0.1:${BACKEND_PORT}/readyz"' in bootstrap
    assert '[[ -d "$REPO_DIR/.git" ]]' not in bootstrap
    assert '[[ -d "$REPO_DIR/.git" ]]' not in inner
    assert '"actualCommitSha": ""' in outer
    assert '"commitSha": ""' in outer


def test_bootstrap_docs_bind_archive_and_command_to_one_full_sha() -> None:
    tencent_guide = (
        REPO_ROOT
        / "Markdown_Readme/Fullstack/04_DevOps/TENCENT_CLOUD_DEPLOY.md"
    ).read_text(encoding="utf-8")
    beginner_guide = (
        REPO_ROOT
        / "Markdown_Readme/Fullstack/04_DevOps/"
        "FULLSTACK_DEPLOY_BEGINNER_GUIDE_2026-04-14.md"
    ).read_text(encoding="utf-8")

    assert "commits/main" in tencent_guide
    assert "tar.gz/${DEPLOY_COMMIT_SHA}" in tencent_guide
    assert ".bootstrap-commit-sha" in tencent_guide
    assert 'DEPLOY_COMMIT_SHA="$DEPLOY_COMMIT_SHA" \\\n' in tencent_guide
    assert "\nbash 03_Scripts/tencent_fullstack_bootstrap.sh" not in tencent_guide
    assert "refs/heads/main -o JATO_Analysis_System-main.tar.gz" not in tencent_guide
    assert "DEPLOY_COMMIT_SHA=<full-sha>" in beginner_guide


def test_gitignore_covers_local_tooling_and_temp_artifacts() -> None:
    lines = {
        line.strip()
        for line in (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    }

    assert ".claude/" in lines
    assert "tmp/" in lines
