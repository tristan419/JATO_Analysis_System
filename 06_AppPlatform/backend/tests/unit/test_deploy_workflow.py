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
PRODUCTION_RELEASE_WORKFLOW = REPO_ROOT / ".github/workflows/production-release.yml"
INTL_SYNC_WORKFLOW = REPO_ROOT / ".github/workflows/sync-www-active-to-intl.yml"
WORKFLOW_GUARD_VALIDATOR = (
    REPO_ROOT / ".github/scripts/validate_production_workflow_guards.py"
)


def test_production_release_excludes_local_tooling_and_temp_artifacts() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "--exclude='.claude'" in workflow
    assert "--exclude='tmp'" in workflow
    assert "--exclude='*.pyc'" in workflow


def test_fixed_v2_release_has_only_four_explicit_jobs() -> None:
    workflow = yaml.load(
        PRODUCTION_RELEASE_WORKFLOW.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    assert isinstance(workflow, dict)
    assert set(workflow["on"]) == {"workflow_dispatch", "workflow_run"}
    assert workflow["on"]["workflow_run"] == {
        "workflows": ["ci"],
        "types": ["completed"],
        "branches": ["main"],
    }
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {
        "release_mode",
        "target_commit_sha",
        "target_archive_sha256",
        "target_manifest_sha256",
        "confirm_control_operation",
        "bootstrap_full_upload",
    }
    release_mode = inputs["release_mode"]
    assert release_mode == {
        "description": "Fixed Active/Candidate V2 operation",
        "required": "true",
        "type": "choice",
        "default": "prepare-candidate",
        "options": [
            "prepare-candidate",
            "discard-candidate",
            "update-active",
            "rollback-active",
        ],
    }
    for name in (
        "target_commit_sha",
        "target_archive_sha256",
        "target_manifest_sha256",
    ):
        assert inputs[name]["required"] == "false"
        assert inputs[name]["type"] == "string"
    for name in ("confirm_control_operation", "bootstrap_full_upload"):
        assert inputs[name]["required"] == "true"
        assert inputs[name]["type"] == "boolean"
        assert inputs[name]["default"] == "false"
    assert workflow["env"]["RELEASE_MODE"] == (
        "${{ github.event_name == 'workflow_run' && 'prepare-candidate' || "
        "inputs.release_mode }}"
    )
    assert workflow["concurrency"] == {
        "group": "production-release-main",
        "cancel-in-progress": "false",
    }

    jobs = workflow["jobs"]
    assert set(jobs) == {
        "release_coordination_guard",
        "build_frontend",
        "deploy_tencent",
        "control_fixed_release_v2",
    }
    guard = jobs["release_coordination_guard"]
    assert "environment" not in guard
    assert [step["name"] for step in guard["steps"]] == [
        "Checkout release coordination guard",
        "Revalidate automatic CI source",
        "Validate unpublished release coordination",
        "Freeze release coordination plan",
    ]
    auto_source = guard["steps"][1]
    assert auto_source["if"] == "${{ github.event_name == 'workflow_run' }}"
    assert auto_source["env"] == {
        "GH_TOKEN": "${{ github.token }}",
        "SOURCE_CI_RUN_ID": "${{ github.event.workflow_run.id }}",
        "SOURCE_CI_RUN_ATTEMPT": "${{ github.event.workflow_run.run_attempt }}",
        "SOURCE_CI_HEAD_SHA": "${{ github.event.workflow_run.head_sha }}",
    }
    for token in (
        'test "$SOURCE_CI_HEAD_SHA" = "$GITHUB_SHA"',
        'repos/$GITHUB_REPOSITORY/branches/main',
        'repos/$GITHUB_REPOSITORY/actions/runs/$SOURCE_CI_RUN_ID',
        '.run_attempt == $run_attempt',
        '.path == ".github/workflows/ci.yml"',
        '.event == "push"',
        '.conclusion == "success"',
        '.head_repository.full_name == $repository',
    ):
        assert token in auto_source["run"]
    guard_validate = guard["steps"][2]
    expected_target_env = {
        "TARGET_COMMIT_SHA": "${{ inputs.target_commit_sha }}",
        "TARGET_ARCHIVE_SHA256": "${{ inputs.target_archive_sha256 }}",
        "TARGET_MANIFEST_SHA256": "${{ inputs.target_manifest_sha256 }}",
    }
    for name, expected in expected_target_env.items():
        assert guard_validate["env"][name] == expected
    for token in (
        '--operation "$RELEASE_MODE"',
        '--target-sha "$TARGET_COMMIT_SHA"',
        '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
        '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
    ):
        assert token in guard_validate["run"]
    auto_source_condition = (
        "github.event_name == 'workflow_run' && "
        "github.ref == 'refs/heads/main' && "
        "github.event.workflow_run.conclusion == 'success' && "
        "github.event.workflow_run.event == 'push' && "
        "github.event.workflow_run.name == 'ci' && "
        "github.event.workflow_run.path == '.github/workflows/ci.yml' && "
        "github.event.workflow_run.head_branch == 'main' && "
        "github.event.workflow_run.head_repository.full_name == github.repository && "
        "github.event.workflow_run.head_sha == github.sha"
    )
    prepare_condition = " ".join(
        (
            "${{ (github.event_name == 'workflow_dispatch' && "
            "github.ref == 'refs/heads/main' && "
            "inputs.release_mode == 'prepare-candidate') || ("
            + auto_source_condition
            + ") }}"
        ).split()
    )
    assert " ".join(jobs["build_frontend"]["if"].split()) == prepare_condition
    assert jobs["build_frontend"]["needs"] == "release_coordination_guard"
    assert " ".join(jobs["deploy_tencent"]["if"].split()) == prepare_condition
    assert jobs["deploy_tencent"]["needs"] == [
        "release_coordination_guard",
        "build_frontend",
    ]
    assert jobs["deploy_tencent"]["environment"] == "candidate-preview"
    control_condition = (
        "${{ github.event_name == 'workflow_dispatch' && "
        "github.ref == 'refs/heads/main' && "
        "inputs.release_mode != 'prepare-candidate' }}"
    )
    control = jobs["control_fixed_release_v2"]
    assert control["if"] == control_condition
    assert control["needs"] == "release_coordination_guard"
    assert control["environment"] == "production"
    for job_name in ("deploy_tencent", "control_fixed_release_v2"):
        revalidate = next(
            step
            for step in jobs[job_name]["steps"]
            if step["name"]
            == "Revalidate frozen coordination plan after approval"
        )
        for name, expected in expected_target_env.items():
            assert revalidate["env"][name] == expected
        for token in (
            '--operation "$RELEASE_MODE"',
            '--target-sha "$TARGET_COMMIT_SHA"',
            '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
            '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
        ):
            assert token in revalidate["run"]
    current_main = next(
        step
        for step in jobs["deploy_tencent"]["steps"]
        if step["name"] == "Reconfirm current main before first server mutation"
    )
    assert current_main["env"] == {"GH_TOKEN": "${{ github.token }}"}
    assert 'gh api "repos/$GITHUB_REPOSITORY/branches/main"' in current_main["run"]
    assert 'if [ "$current_main" != "$GITHUB_SHA" ]' in current_main["run"]
    reconfirm = next(
        step
        for step in jobs["deploy_tencent"]["steps"]
        if step["name"] == "Revalidate frozen plan before Candidate mutation"
    )
    for name, expected in expected_target_env.items():
        assert reconfirm["env"][name] == expected
    assert reconfirm["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert reconfirm["env"]["GITHUB_TOKEN"] == "${{ github.token }}"
    assert 'gh api "repos/$GITHUB_REPOSITORY/branches/main"' in reconfirm["run"]
    assert 'if [ "$current_main" != "$GITHUB_SHA" ]' in reconfirm["run"]
    assert '--operation "$RELEASE_MODE"' in reconfirm["run"]
    assert '--target-sha "$TARGET_COMMIT_SHA"' in reconfirm["run"]
    assert '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"' in reconfirm["run"]
    assert '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"' in reconfirm["run"]
    deploy_release = next(
        step
        for step in jobs["deploy_tencent"]["steps"]
        if step["name"] == "Deploy verified release to fixed Candidate on Tencent"
    )
    assert deploy_release["env"]["CANDIDATE_REPLACE_POLICY"] == (
        "${{ github.event_name == 'workflow_run' && "
        "'reuse-verified-same-release' || 'replace' }}"
    )
    assert "write_remote_export CANDIDATE_REPLACE_POLICY" in deploy_release["run"]


@pytest.mark.skipif(
    not CHECKPOINT_RECOVERY_WORKFLOW.exists(),
    reason="incident-only checkpoint recovery workflow has been retired",
)
def test_checkpoint_recovery_workflow_is_exactly_gated_and_read_only() -> None:
    workflow_text = CHECKPOINT_RECOVERY_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(workflow_text, Loader=yaml.BaseLoader)
    assert isinstance(workflow, dict)
    assert workflow["name"] == "production-checkpoint-recovery"
    assert set(workflow["on"]) == {"workflow_dispatch"}
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {
        "mode",
        "confirmation",
        "reviewed_dry_run_run_id",
        "reviewed_dry_run_result_sha256",
        "reviewed_main_sha",
        "reviewed_plan_sha256",
    }
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
    for name in set(inputs) - {"mode", "confirmation"}:
        assert {
            "required": inputs[name]["required"],
            "default": inputs[name]["default"],
            "type": inputs[name]["type"],
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
        "Fetch reviewed dry-run metadata",
        "Resolve reviewed dry-run artifact",
        "Download reviewed dry-run result",
        "Verify and freeze reviewed dry-run authorization",
        "Validate active recovery-only production hold",
        "Validate unpublished release coordination",
        "Freeze recovery coordination plan",
        "Freeze reviewed dry-run evidence",
    ]
    assert "secrets." not in str(guard_steps)
    assert "reviewed_recovery_authorization.py freeze" in guard_steps[5]["run"]
    assert "production_release_hold.py" in guard_steps[6]["run"]
    assert "require-active" in guard_steps[6]["run"]
    assert "PYTHONPATH=03_Scripts/deploy" in guard_steps[6]["run"]
    assert "load_recovery_plan" in guard_steps[6]["run"]
    assert "hashlib.sha256(plan.read_bytes()).hexdigest()" in guard_steps[6]["run"]
    frozen_artifact = guard_steps[8]
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
        "Revalidate active recovery-only production hold after approval",
        "Download frozen recovery coordination plan",
        "Revalidate frozen coordination plan after approval",
        "Revalidate recovery dispatch intent after approval",
        "Download frozen reviewed dry-run evidence",
        "Revalidate frozen reviewed dry-run evidence",
        "Build immutable recovery control bundle",
        "Validate Tencent recovery credentials",
        "Reconfirm current main before recovery transport",
        "Run reviewed checkpoint recovery on Tencent",
        "Prepare structured checkpoint recovery result and summary",
        "Upload checkpoint recovery result",
    ]
    assert "secrets." not in str(recovery_steps[:8])
    assert "production_release_hold.py" in recovery_steps[1]["run"]
    assert "require-active" in recovery_steps[1]["run"]
    assert recovery_steps[2]["with"] == {
        "name": (
            "checkpoint-recovery-plan-${{ github.sha }}-"
            "${{ github.run_attempt }}"
        ),
        "path": "${{ runner.temp }}/recovery-coordination-plan",
    }
    assert "verify-plan" in recovery_steps[3]["run"]
    assert (
        "QUARANTINE 29df5e6e667351f09305783932b34e5438d6a9d5 "
        "RESIDUE AND ABORT PRE-SWITCH"
        in recovery_steps[4]["run"]
    )
    assert "reviewed_recovery_authorization.py verify" in recovery_steps[6]["run"]
    assert "recovery-control-manifest.json" in recovery_steps[7]["run"]
    assert (
        'plan=".github/recovery-plans/'
        '2026-08-03-29df-pre-switch-candidate-residue.json"'
        in recovery_steps[7]["run"]
    )
    assert "SSH_KNOWN_HOSTS" in str(recovery_steps[8]["env"])
    assert "verify-plan" in recovery_steps[9]["run"]
    assert "StrictHostKeyChecking=yes" in recovery_steps[10]["run"]
    assert "ABORT 2026-07-30-86ce PRE-SWITCH" not in workflow_text
    assert recovery_steps[10]["id"] == "recovery"
    assert "if" not in recovery_steps[10]
    assert "continue-on-error" not in recovery_steps[10]
    result_presentation = recovery_steps[11]
    assert result_presentation["if"] == "${{ always() }}"
    assert "secrets." not in str(result_presentation)
    assert result_presentation["env"] == {
        "RECOVERY_MAIN_SHA": "${{ github.sha }}",
        "RECOVERY_MODE": "${{ inputs.mode }}",
        "RECOVERY_RESULT": (
            "${{ runner.temp }}/checkpoint-recovery-result.json"
        ),
        "RECOVERY_STEP_OUTCOME": "${{ steps.recovery.outcome }}",
    }
    assert "GITHUB_STEP_SUMMARY" in result_presentation["run"]
    assert (
        ".github/scripts/present_checkpoint_recovery_result.py"
        in result_presentation["run"]
    )
    for option in (
        "--result",
        "--summary",
        "--plan",
        "--step-outcome",
        "--mode",
        "--main-sha",
        "--plan-sha256",
    ):
        assert option in result_presentation["run"]
    result_artifact = recovery_steps[12]
    assert result_artifact["if"] == "${{ always() }}"
    assert result_artifact["uses"] == "actions/upload-artifact@v4"
    assert result_artifact["with"] == {
        "name": (
            "checkpoint-recovery-result-${{ github.sha }}-"
            "${{ github.run_id }}-${{ github.run_attempt }}"
        ),
        "path": "${{ runner.temp }}/checkpoint-recovery-result.json",
        "if-no-files-found": "error",
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
    assert systemctl_verbs == {"show", "daemon-reload"}
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


def test_production_workflow_loader_rejects_duplicate_yaml_keys() -> None:
    spec = importlib.util.spec_from_file_location(
        "production_workflow_guard_test_module",
        WORKFLOW_GUARD_VALIDATOR,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with pytest.raises(AssertionError, match="duplicate YAML key: path"):
        yaml.load(
            "with:\n  path: first\n  path: second\n",
            Loader=module.UniqueKeyLoader,
        )


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


def test_candidate_prepare_has_no_automatic_intl_follower() -> None:
    prewarm = REPO_ROOT / ".github/workflows/intl-edge-prewarm.yml"
    assert not prewarm.exists()

    sync = yaml.load(
        INTL_SYNC_WORKFLOW.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    assert isinstance(sync, dict)
    assert set(sync["on"]) == {"workflow_dispatch"}
    assert sync["jobs"]["sync_intl"]["environment"] == "production"


def test_tencent_release_upload_requires_verified_basis_or_explicit_bootstrap() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    assert "Upload complete release archive with incremental rsync" in workflow
    assert "fallback to sparse" not in workflow
    assert "timeout-minutes: 240" in workflow
    assert "cancel-in-progress: false" in workflow
    assert 'remote_archive="${remote_dir}/${archive_sha256}.tar.gz"' in workflow
    assert 'remote_temp="${remote_archive}.partial"' in workflow
    assert 'remote_lock="${remote_archive}.lock"' in workflow
    assert "--partial" in workflow
    assert "--append-verify" not in workflow
    assert "gzip -n --rsyncable" in workflow
    assert "sudo -n realpath /opt/jato/slots/8000/current" in workflow
    assert "bootstrap_full_upload:" in workflow
    assert "ALLOW_FULL_UPLOAD_BOOTSTRAP" in workflow
    assert 'GITHUB_EVENT_NAME" != "workflow_dispatch' in workflow
    assert 'RELEASE_MODE" != "prepare-candidate' in workflow
    assert "basis_kind='retained'" in workflow
    assert "basis_kind='bootstrap'" in workflow
    assert "Explicit full-upload bootstrap authorized" in workflow
    assert "NO_BASIS" in workflow
    assert "refusing full upload" in workflow
    assert "--checksum" in workflow
    assert "--stats" in workflow
    assert 'echo "literal-bytes=$literal_bytes"' in workflow
    assert 'echo "bootstrap-used=$bootstrap_used"' in workflow
    assert "Bootstrap transfer did not report the exact full archive byte count" in workflow
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
    assert '"umask 077; exec bash -s"' in workflow
    assert '< "$control_payload" 2>&1 | tee "$operation_log"' in workflow


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
    assert "03_Scripts/deploy/fixed_active_preimage.py" in outer
    assert "03_Scripts/deploy/nginx/jato_candidate_preview.conf.example" in outer
    assert (
        "prepare-candidate|approve-candidate-to-active|"
        "discard-candidate|discard-failed-candidate|release-candidate|"
        "restore-previous-active"
    ) in outer
    assert 'if [[ "$DEPLOY_BLUEGREEN_MODE" != "prepare-candidate" ]]; then' in outer
    assert "DEPLOY_CANDIDATE_ATTESTATION_SHA256" in outer


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
    assert 'gzip -n --rsyncable -f "$RUNNER_TEMP/JATO_deploy.tar"' in workflow
    assert 'value.get("localPath")' in workflow
    assert "missing MSRP localPath evidence" in workflow
    assert 'tar tzf "$RUNNER_TEMP/JATO_deploy.tar.gz" "$evidence_path"' in workflow
    assert "update_mihomo_subscription.sh" not in workflow
    assert '"expectedCommitSha": sha' in workflow
    assert '"actualCommitSha": ""' in workflow
    assert '"commitSha": ""' in workflow
    assert '"commitSha": sha' not in workflow


def test_fixed_v2_prepare_has_diagnostics_without_cross_service_checkpoints() -> None:
    workflow = yaml.load(
        PRODUCTION_RELEASE_WORKFLOW.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    steps = workflow["jobs"]["deploy_tencent"]["steps"]
    active_steps = [
        step
        for step in steps
        if step.get("if") in (None, "${{ always() }}")
    ]
    names = [step["name"] for step in active_steps]
    required_order = [
        "Reconfirm current main before first server mutation",
        "Upload complete release archive with incremental rsync",
        "Generate canonical V2 release manifest",
        "Revalidate frozen plan before Candidate mutation",
        "Deploy verified release to fixed Candidate on Tencent",
        "Retain V2 operation diagnostics",
    ]
    indexes = [names.index(name) for name in required_order]
    assert indexes == sorted(indexes)
    commands = "\n".join(str(step.get("run") or "") for step in active_steps)
    assert 'timeout 5400s "${ssh_command[@]}"' in commands
    assert "V2_OPERATION_REPORT_PATH=" in commands
    for legacy_phase in (
        "--phase transport_verified",
        "--phase www_verified",
        "--phase intl_deploy_started",
        "--phase intl_verified",
        "--phase parity_verified",
        "--phase complete",
    ):
        assert legacy_phase not in commands
    retain_index = next(
        index
        for index, step in enumerate(steps)
        if step["name"] == "Retain V2 operation diagnostics"
    )
    assert retain_index == len(steps) - 1


def test_manual_review_artifacts_share_thirty_day_retention() -> None:
    workflow = yaml.load(
        PRODUCTION_RELEASE_WORKFLOW.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    build_steps = workflow["jobs"]["build_frontend"]["steps"]
    deploy_steps = workflow["jobs"]["deploy_tencent"]["steps"]
    frontend_upload = next(
        step
        for step in build_steps
        if step.get("name") == "Upload the only frontend artifact"
    )
    prepare_diagnostics = next(
        step
        for step in deploy_steps
        if step.get("name") == "Retain V2 operation diagnostics"
    )
    assert frontend_upload["with"]["retention-days"] == "30"
    assert prepare_diagnostics["with"]["retention-days"] == "30"


def test_fixed_v2_operation_reports_are_retained_without_checkpoint_handoff() -> None:
    workflow = yaml.load(
        PRODUCTION_RELEASE_WORKFLOW.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    jobs = workflow["jobs"]
    for job_name, run_name, retain_name in (
        (
            "deploy_tencent",
            "Deploy verified release to fixed Candidate on Tencent",
            "Retain V2 operation diagnostics",
        ),
        (
            "control_fixed_release_v2",
            "Run fixed V2 control operation on Tencent",
            "Retain fixed V2 control diagnostics",
        ),
    ):
        steps = jobs[job_name]["steps"]
        run_step = next(step for step in steps if step["name"] == run_name)
        retain = next(step for step in steps if step["name"] == retain_name)
        command = str(run_step["run"])
        assert "V2_OPERATION_REPORT_PATH=" in command
        assert "operation-report-path=" in command
        assert retain["if"] == "${{ always() }}"
        assert retain["uses"] == "actions/upload-artifact@v4"
        assert retain["with"]["overwrite"] == "false"
        assert retain["with"]["retention-days"] == "30"
    workflow_text = PRODUCTION_RELEASE_WORKFLOW.read_text(encoding="utf-8")
    for legacy_token in (
        "Fetch and attest server release checkpoint",
        "approve_candidate_to_active:",
        "cleanup_candidate:",
        "audit_frontend_parity:",
        "prepare-and-switch",
    ):
        assert legacy_token not in workflow_text


def test_update_active_uses_reviewed_candidate_identity_without_rebuild() -> None:
    workflow = PRODUCTION_RELEASE_WORKFLOW.read_text(encoding="utf-8")
    control = workflow[workflow.index("\n  control_fixed_release_v2:") :]

    for token in (
        "update-active|rollback-active)",
        'write_remote_export DEPLOY_COMMIT_SHA "$TARGET_COMMIT_SHA"',
        'write_remote_export DEPLOY_ARCHIVE_SHA256 "$TARGET_ARCHIVE_SHA256"',
        'write_remote_export RELEASE_V2_MANIFEST_SHA256',
        "fixed_release_v2_remote.sh",
        'PRODUCTION_LOCK_PATH="$production_lock"',
        "bash -s %q",
    ):
        assert token in control
    for forbidden in (
        "npm run build",
        "Package backend release",
        "Upload complete release archive",
        "pages deploy",
        "sync-www-active-to-intl",
    ):
        assert forbidden not in control


def test_discard_candidate_is_the_same_control_only_path() -> None:
    workflow = PRODUCTION_RELEASE_WORKFLOW.read_text(encoding="utf-8")
    control = workflow[workflow.index("\n  control_fixed_release_v2:") :]

    assert "discard-candidate)" in control
    assert '[ -z "$TARGET_COMMIT_SHA" ]' in control
    assert '[ -z "$TARGET_ARCHIVE_SHA256" ]' in control
    assert '[ -z "$TARGET_MANIFEST_SHA256" ]' in control
    assert "github_candidate_control.sh" not in control
    assert "release_checkpoint.py" not in control
    assert "approve_candidate_to_active:" not in workflow
    assert "cleanup_candidate:" not in workflow


def test_candidate_control_shell_is_control_only_and_syntax_valid() -> None:
    helper = REPO_ROOT / "03_Scripts/deploy/github_candidate_control.sh"
    script = helper.read_text(encoding="utf-8")
    assert (
        "approve-candidate-to-active|discard-candidate|"
        "discard-failed-candidate|release-candidate|restore-previous-active"
    ) in script
    assert "capture-canonical-cleanup" in script
    assert "production-deploy.lock" in script
    assert "fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)" in script
    assert '"http://127.0.0.1:18002/candidate-preview.json"' in script
    assert "hash_regular(" in script
    assert "archive_raw" not in script
    assert 'source "$CANDIDATE_VERIFIED_ENV"' in script
    assert 'write_remote_export DEPLOY_RUN_ID "$CANDIDATE_RUN_ID"' in script
    assert (
        'write_remote_export DEPLOY_RUN_ATTEMPT "$CANDIDATE_RUN_ATTEMPT"'
        in script
    )
    assert 'write_remote_export DEPLOY_APPROVAL_RUN_ID "$GITHUB_RUN_ID"' in script
    assert (
        'write_remote_export DEPLOY_APPROVAL_RUN_ATTEMPT "$GITHUB_RUN_ATTEMPT"'
        in script
    )
    assert "DEPLOY_CANDIDATE_ATTESTATION_SHA256" in script
    for name in (
        "CANDIDATE_SERVER_CHECKPOINT_PATH",
        "CANDIDATE_SERVER_CHECKPOINT_SHA256",
        "CANDIDATE_SERVER_EVIDENCE_PATH",
        "CANDIDATE_SERVER_EVIDENCE_SHA256",
    ):
        assert name in script
        assert f"DEPLOY_{name}" in script
    assert "rsync" not in script
    assert "pages deploy" not in script
    result = subprocess.run(
        ["bash", "-n", str(helper)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_current_www_active_intl_sync_is_independent_and_no_build() -> None:
    workflow = yaml.load(
        INTL_SYNC_WORKFLOW.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    assert isinstance(workflow, dict)
    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert set(workflow["on"]["workflow_dispatch"]["inputs"]) == {
        "confirm_sync_current_www_active"
    }
    assert workflow["concurrency"] == {
        "group": "production-release-main",
        "cancel-in-progress": "false",
    }
    sync = workflow["jobs"]["sync_intl"]
    assert sync["if"] == "${{ github.ref == 'refs/heads/main' }}"
    assert sync["environment"] == "production"
    assert "needs" not in sync
    commands = "\n".join(str(step.get("run") or "") for step in sync["steps"])
    for token in (
        "confirm_sync_current_www_active must be explicitly checked",
        "export_active_frontend_release.py",
        "verify_release_source_seal.py",
        "verify-download",
        "frontend_release_artifact.py audit-public",
        "pages deploy",
        '--commit-hash "$ACTIVE_COMMIT_SHA"',
        "verify_intl_runtime_contract.py",
        '"jatoDataChanged": False',
    ):
        assert token in commands
    for forbidden in (
        "npm ci",
        "npm install",
        "npm run build",
        "vite build",
        "github_candidate_control.sh",
        "01_RAW_DATA",
        "04_Processed_data",
    ):
        assert forbidden not in commands


def test_tencent_uploads_verified_archive_before_deploy_step() -> None:
    workflow = (REPO_ROOT / ".github/workflows/production-release.yml").read_text(
        encoding="utf-8",
    )

    verify_index = workflow.index("Verify frontend artifact before Tencent deployment")
    current_main_index = workflow.index(
        "Reconfirm current main before first server mutation"
    )
    upload_index = workflow.index("Upload complete release archive with incremental rsync")
    manifest_index = workflow.index("Generate canonical V2 release manifest")
    deploy_index = workflow.index("Deploy verified release to fixed Candidate on Tencent")
    assert verify_index < current_main_index < upload_index < manifest_index < deploy_index
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
    handoff = outer.index('bash "$BLUEGREEN_CONTROLLER" "$DEPLOY_BLUEGREEN_MODE"')
    assert outer.rstrip().endswith('exit "$BLUEGREEN_RC"')
    assert handoff < outer.index('exit "$BLUEGREEN_RC"', handoff)
    candidate = controller.index("verify_candidate()")
    candidate_body = controller[candidate:]
    readiness = controller.index("verify_slot_release_exact()")
    readiness_body = controller[readiness:candidate]
    assert 'wait_for_slot_release_exact "$CANDIDATE_SLOT"' in candidate_body
    assert "http://127.0.0.1:${slot}/healthz" in readiness_body
    assert 'expected-commit "$expected_sha"' in readiness_body
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
