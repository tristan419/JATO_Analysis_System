import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[4]
RELEASE_PREPARATION_HELPER = (
    REPO_ROOT / "03_Scripts/deploy/prepare_backend_release.py"
)
READINESS_HELPER = REPO_ROOT / "03_Scripts/deploy/verify_backend_readiness.py"
CHECKPOINT_RECOVERY_WORKFLOW = (
    REPO_ROOT / ".github/workflows/production-checkpoint-recovery.yml"
)


def test_production_release_excludes_local_tooling_and_temp_artifacts() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "--exclude='.claude'" in workflow
    assert "--exclude='tmp'" in workflow
    assert "--exclude='*.pyc'" in workflow


def test_checkpoint_recovery_workflow_is_exactly_gated_and_read_only() -> None:
    workflow_text = CHECKPOINT_RECOVERY_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(workflow_text, Loader=yaml.BaseLoader)
    assert isinstance(workflow, dict)
    assert workflow["name"] == "production-checkpoint-recovery"
    assert set(workflow["on"]) == {"workflow_dispatch"}
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"mode", "confirmation"}
    assert inputs["mode"]["required"] == "true"
    assert inputs["mode"]["default"] == "dry-run"
    assert inputs["mode"]["type"] == "choice"
    assert inputs["mode"]["options"] == ["dry-run", "apply"]
    assert {
        "required": inputs["confirmation"]["required"],
        "default": inputs["confirmation"]["default"],
        "type": inputs["confirmation"]["type"],
    } == {
        "required": "false",
        "default": "",
        "type": "string",
    }
    assert workflow["concurrency"] == {
        "group": "production-release-main",
        "cancel-in-progress": "false",
    }
    assert set(workflow["jobs"]) == {
        "recovery_coordination_guard",
        "recover_checkpoint",
    }

    guard = workflow["jobs"]["recovery_coordination_guard"]
    recovery = workflow["jobs"]["recover_checkpoint"]
    main_guard = "${{ github.ref == 'refs/heads/main' }}"
    assert guard["if"] == main_guard
    assert "environment" not in guard
    assert recovery["if"] == main_guard
    assert recovery["needs"] == "recovery_coordination_guard"
    assert recovery["environment"] == "production"

    guard_steps = guard["steps"]
    assert [step["name"] for step in guard_steps] == [
        "Checkout recovery source",
        "Validate recovery dispatch intent",
        "Validate unpublished release coordination",
        "Freeze recovery coordination plan",
    ]
    assert "secrets." not in str(guard_steps)
    frozen_artifact = guard_steps[3]
    assert frozen_artifact["uses"] == "actions/upload-artifact@v4"
    assert frozen_artifact["with"] == {
        "name": (
            "checkpoint-recovery-plan-${{ github.sha }}-"
            "${{ github.run_attempt }}"
        ),
        "path": "${{ runner.temp }}/recovery-coordination-plan.json",
        "if-no-files-found": "error",
        "compression-level": "0",
        "overwrite": "false",
        "retention-days": "7",
    }

    recovery_steps = recovery["steps"]
    assert [step["name"] for step in recovery_steps] == [
        "Checkout recovery source",
        "Download frozen recovery coordination plan",
        "Revalidate frozen coordination plan after approval",
        "Revalidate recovery dispatch intent after approval",
        "Build immutable recovery control bundle",
        "Validate Tencent recovery credentials",
        "Reconfirm current main before recovery transport",
        "Run reviewed checkpoint recovery on Tencent",
        "Upload checkpoint recovery result",
    ]
    assert "secrets." not in str(recovery_steps[:4])
    assert recovery_steps[1]["with"] == {
        "name": (
            "checkpoint-recovery-plan-${{ github.sha }}-"
            "${{ github.run_attempt }}"
        ),
        "path": "${{ runner.temp }}/recovery-coordination-plan",
    }
    assert "verify-plan" in recovery_steps[2]["run"]
    assert "ABORT 2026-07-30-86ce PRE-SWITCH" in recovery_steps[3]["run"]
    assert "recovery-control-manifest.json" in recovery_steps[4]["run"]
    assert (
        'plan=".github/recovery-plans/'
        '2026-07-30-86ce-pre-switch-db-evidence.json"'
        in recovery_steps[4]["run"]
    )
    assert "SSH_KNOWN_HOSTS" in str(recovery_steps[5]["env"])
    assert "verify-plan" in recovery_steps[6]["run"]
    assert "StrictHostKeyChecking=yes" in recovery_steps[7]["run"]
    result_artifact = recovery_steps[8]
    assert result_artifact["if"] == "${{ always() }}"
    assert result_artifact["uses"] == "actions/upload-artifact@v4"
    assert result_artifact["with"] == {
        "name": (
            "checkpoint-recovery-result-${{ github.sha }}-"
            "${{ github.run_attempt }}"
        ),
        "path": "${{ runner.temp }}/checkpoint-recovery-result.json",
        "if-no-files-found": "warn",
        "compression-level": "0",
        "overwrite": "false",
        "retention-days": "30",
    }

    controller = (
        REPO_ROOT
        / "03_Scripts/deploy/tencent_pre_switch_checkpoint_recovery.sh"
    ).read_text(encoding="utf-8")
    helper = (
        REPO_ROOT / "03_Scripts/deploy/pre_switch_checkpoint_recovery.py"
    ).read_text(encoding="utf-8")
    lock_library = (
        REPO_ROOT
        / "03_Scripts/deploy/lib/production_mutation_lock.sh"
    ).read_text(encoding="utf-8")
    recovery_sources = "\n".join(
        (workflow_text, controller, helper, lock_library)
    )
    systemctl_verbs = set(
        re.findall(
            r"(?m)^[ \t]*systemctl\s+([a-z-]+)",
            recovery_sources,
        )
    )
    systemctl_verbs.update(
        re.findall(
            r"""["']systemctl["']\s*,\s*["']([a-z-]+)["']""",
            recovery_sources,
        )
    )
    assert systemctl_verbs == {"show"}
    assert set(
        re.findall(r"-m\s+alembic\s+([a-z-]+)", recovery_sources)
    ) == {"current", "heads"}
    for forbidden in (
        "systemctl start",
        "systemctl stop",
        "systemctl restart",
        "systemctl reload",
        "systemctl enable",
        "systemctl disable",
        "alembic upgrade",
        "alembic downgrade",
        "alembic stamp",
        "nginx -s",
        "fullstack_remote_release.sh",
        "tencent_bluegreen_release.sh",
    ):
        assert forbidden not in recovery_sources


def test_tencent_bluegreen_release_links_durable_runtime_artifacts() -> None:
    outer = (REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh").read_text(
        encoding="utf-8",
    )
    controller = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    ).read_text(encoding="utf-8")

    assert "03_Scripts/deploy/tencent_bluegreen_release.sh" in outer
    assert 'ensure_shared_link "$SHARED_ROOT/01_RAW_DATA"' in controller
    assert 'ensure_shared_link "$SHARED_ROOT/04_Processed_data"' in controller
    assert '"03_Scripts/diagnostics/artifacts"' in controller
    assert '"03_Scripts/logs"' in controller
    assert '"hermes/reports"' in controller
    assert 'link_release_runtime_path "01_RAW_DATA"' in controller
    assert 'link_release_runtime_path "04_Processed_data"' in controller
    assert "Preserved runtime path" not in outer
    assert "Restored runtime path" not in outer


def test_bluegreen_storage_guard_is_packaged_and_runs_before_candidate_runtime() -> None:
    outer = (REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh").read_text(
        encoding="utf-8",
    )
    controller = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    ).read_text(encoding="utf-8")
    boot_reconcile = (
        REPO_ROOT / "03_Scripts/deploy/jato_bluegreen_boot_reconcile.py"
    ).read_text(encoding="utf-8")
    storage_guard = (
        REPO_ROOT / "03_Scripts/deploy/jato_release_storage_guard.py"
    ).read_text(encoding="utf-8")

    assert "03_Scripts/deploy/jato_release_storage_guard.py" in outer
    for required in (
        "BLUEGREEN_MIN_TOTAL_MEMORY_BYTES=$((14 * 1024 * 1024 * 1024))",
        "BLUEGREEN_MIN_AVAILABLE_MEMORY_BYTES=$((5 * 1024 * 1024 * 1024))",
        "BLUEGREEN_CANDIDATE_MAX_MEMORY_BYTES=$((4 * 1024 * 1024 * 1024))",
        "BLUEGREEN_OS_MEMORY_RESERVE_BYTES=$((2 * 1024 * 1024 * 1024))",
        "BLUEGREEN_PREPARE_DISK_RESERVE_BYTES=$((15 * 1024 * 1024 * 1024))",
        "BLUEGREEN_PREPARE_DISK_RESERVE_PERCENT=8",
        "BLUEGREEN_RUNTIME_DISK_RESERVE_BYTES=$((10 * 1024 * 1024 * 1024))",
        "BLUEGREEN_RUNTIME_DISK_RESERVE_PERCENT=5",
        "BLUEGREEN_RELEASE_KEEP_UNREFERENCED=3",
        "BLUEGREEN_RELEASE_NORMAL_GC_AGE_SECONDS=$((14 * 24 * 60 * 60))",
        "BLUEGREEN_RELEASE_EMERGENCY_GC_AGE_SECONDS=$((24 * 60 * 60))",
    ):
        assert required in controller

    prepare = controller[controller.index("prepare_and_switch()") :]
    inherited_lock = prepare.index("\n  assert_inherited_production_lock\n")
    state_root = prepare.index("\n  ensure_bluegreen_state_root\n")
    runtime_roots = prepare.index("\n  ensure_bluegreen_runtime_roots\n")
    preflight = prepare.index("\n  guard_release_storage\n")
    preflight_memory = prepare.index("\n  assert_host_memory_budget\n", preflight)
    materialize = prepare.index("\n  materialize_release_source\n")
    build_scope = prepare.index("\n  run_candidate_build_scope\n")
    final_seal = prepare.index("\n  verify_final_runtime_seal\n")
    database_gate = prepare.index(
        "\n  assert_no_database_migration_delta\n",
        final_seal,
    )
    post_seal = prepare.index("\n  assert_runtime_storage_reserve\n")
    runtime_memory = prepare.index("\n  assert_host_memory_budget\n", post_seal)
    install = prepare.index("\n  install_slot_runtime\n")
    assert (
        inherited_lock
        < state_root
        < runtime_roots
        < preflight
        < preflight_memory
        < materialize
        < build_scope
        < final_seal
        < database_gate
        < post_seal
        < runtime_memory
        < install
    )
    constrained_build = controller[
        controller.index("run_candidate_build_scope()"):
        controller.index("\n}\n", controller.index("run_candidate_build_scope()"))
    ]
    assert "--scope" in constrained_build
    assert "--wait" not in constrained_build
    assert '--property="MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH"' in constrained_build
    assert '--property="MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX"' in constrained_build
    assert '--property="TasksMax=512"' in constrained_build

    assert "jato_release_storage_guard" not in boot_reconcile
    assert "guard_release_storage" not in boot_reconcile
    assert "shutil.rmtree" not in boot_reconcile
    assert "rm -rf" not in boot_reconcile

    for forbidden in (
        "rm -rf",
        "shutil.rmtree(releases_root)",
        "shutil.rmtree(args.releases_root)",
        "shutil.rmtree(jato_root)",
        'glob("*")',
        'rglob("*")',
    ):
        assert forbidden not in storage_guard


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


def test_remote_release_preserves_normalized_archive_permissions() -> None:
    outer = (
        REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
    ).read_text(encoding="utf-8")
    controller = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    ).read_text(encoding="utf-8")

    assert "tar --same-permissions --no-overwrite-dir" in outer
    assert '-xzf "$SEALED_RELEASE_ARCHIVE" -C "$RELEASE_WORKTREE"' in outer
    assert (
        ") | sudo -n tar --same-permissions --no-overwrite-dir"
        in controller
    )
    assert "03_Scripts/deploy/cleanup_toolkit_egg_info.py" in outer


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
    assert "--mode='u=rwX,go=rX'" in workflow
    assert "--mode='u=rwX,go=X'" in workflow
    assert 'tar "${tar_private_normalize[@]}" --no-recursion' in workflow
    assert '"${private_dirs[@]}"' in workflow
    assert '"${private_files[@]}"' in workflow
    assert "--exclude='06_AppPlatform/backend/*.parquet'" in workflow
    assert "--no-acls" in workflow
    assert "--no-xattrs" in workflow
    assert "--no-selinux" in workflow
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
    assert 'timeout 5400s "${ssh_command[@]}"' in workflow
    assert 'timeout 1800s "${ssh_command[@]}"' not in workflow
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
    assert "record_checkpoint_fetch_exit" in workflow
    assert 'trap record_checkpoint_fetch_exit EXIT' in workflow
    assert '"$server_dir/fetch-status.json"' in workflow
    assert '"backendHealthyAttested"' in workflow
    assert 'rm -rf "$server_dir"' not in workflow
    assert (
        "if: ${{ always() && steps.upload_release.outcome == 'success' }}"
        in workflow
    )
    assert 'checkpoint.get("phase") != "backend_healthy"' in workflow
    assert 'checkpoint.get("status") != "completed"' in workflow
    assert "server checkpoint evidence binding mismatch" in workflow
    assert 'evidence.get("identity") != expected_identity' in workflow
    assert (
        'install -m 600 \\\n'
        '            "$server_dir/backend-healthy.json" \\\n'
        '            "$RUNNER_TEMP/release-checkpoint/candidate.json"'
    ) in workflow
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


def test_bluegreen_public_deploy_status_is_atomic_and_binds_active_slot() -> None:
    script = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    ).read_text(encoding="utf-8")
    status = _shell_function(script, "write_candidate_deploy_status")

    assert 'echo "deploy_exit_code=0"' in status
    assert 'echo "release_sha=$DEPLOY_COMMIT_SHA"' in status
    assert 'echo "active_slot=$CANDIDATE_SLOT"' in status
    assert 'echo "deployment_mode=tencent_bluegreen"' in status
    assert 'echo "candidate_memory_high=$BLUEGREEN_CANDIDATE_MEMORY_HIGH"' in status
    assert 'echo "candidate_memory_max=$BLUEGREEN_CANDIDATE_MEMORY_MAX"' in status
    assert 'mv -f "$temp" "$dist/_deploy_status.txt"' in status


def test_msrp_env_status_reports_controls_without_printing_secrets() -> None:
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
    controller = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
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
    assert "http://127.0.0.1:${active_port}/healthz" in outer
    assert '--url "http://127.0.0.1:${backend_port}/readyz"' in outer
    assert 'ACTIVE_SLOT_FILE="${ACTIVE_SLOT_FILE:-$BLUEGREEN_STATE_ROOT/active-slot}"' in outer
    assert "/opt/jato/active" in outer
    handoff = outer.index(
        'bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh"',
    )
    assert outer.rstrip().endswith('exit "$BLUEGREEN_RC"')
    assert handoff < outer.index('exit "$BLUEGREEN_RC"', handoff)
    candidate = controller.index("verify_candidate()")
    assert "http://127.0.0.1:${CANDIDATE_SLOT}/healthz" in controller[candidate:]
    assert 'expected-commit "$DEPLOY_COMMIT_SHA"' in controller[candidate:]
    switch = _shell_function(controller, "switch_locked")
    activation = _shell_function(controller, "complete_candidate_activation")
    nginx_verify = switch.index(
        "\n  if [[ \"$BLUEGREEN_FAULT\" == \"post_switch_readiness\" ]]",
    )
    activation_call = switch.index(
        "\n  complete_candidate_activation",
        nginx_verify,
    )
    healthy_checkpoint = activation.index(
        "\n  checkpoint_write backend_healthy completed automatic",
    )
    assert nginx_verify < activation_call
    assert activation.index("verify_active_cgroup") < healthy_checkpoint


def test_deploy_prepares_frozen_release_identity_before_backend_restart() -> None:
    inner = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")
    outer = (
        REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
    ).read_text(encoding="utf-8")
    controller = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    ).read_text(encoding="utf-8")
    bootstrap = (
        REPO_ROOT / "03_Scripts/ops/tencent_fullstack_bootstrap.sh"
    ).read_text(encoding="utf-8")

    assert "03_Scripts/deploy/prepare_backend_release.py" in outer
    assert "install_backend_env_atomically" not in outer
    prepare = controller[controller.index("prepare_and_switch()"):]
    materialize = prepare.index("\n  materialize_release_source\n")
    build_scope = prepare.index("\n  run_candidate_build_scope\n", materialize)
    frozen_seal = prepare.index("\n  verify_final_runtime_seal\n", build_scope)
    candidate = prepare.index("\n  verify_candidate\n", frozen_seal)
    assert materialize < build_scope < frozen_seal < candidate
    build = _shell_function(controller, "build_candidate_runtime_locked")
    inner_prepare = build.index("\n    run_inner_prepare\n")
    source_verify = build.index(
        "\n    verify_materialized_release_source\n",
        inner_prepare,
    )
    database_gate = build.index(
        "\n    assert_no_database_migration_delta\n",
        source_verify,
    )
    status_write = build.index(
        "\n    write_candidate_deploy_status\n",
        database_gate,
    )
    finalize = build.index("\n    finalize_runtime_seal\n", status_write)
    final_verify = build.index("\n  verify_final_runtime_seal", finalize)
    assert (
        inner_prepare
        < source_verify
        < database_gate
        < status_write
        < finalize
        < final_verify
    )
    run_inner = _shell_function(controller, "run_inner_prepare")
    assert "BLUEGREEN_PREPARE_ONLY=true" in run_inner
    assert "RUN_DATABASE_MIGRATIONS=verify_only" in run_inner
    assert 'RELEASE_CHECKPOINT_COMMIT="$DEPLOY_COMMIT_SHA"' in run_inner
    assert 'PREBUILT_FRONTEND_DIR="$PREBUILT_FRONTEND_DIR"' in run_inner

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
    assert "preserve_previous_release_metadata" in controller
    assert 'RELEASE_CHECKPOINT_COMMIT="$DEPLOY_COMMIT_SHA"' in controller


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
