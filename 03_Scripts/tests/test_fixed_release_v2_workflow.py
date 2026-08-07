from __future__ import annotations

import copy
import os
from pathlib import Path
import subprocess
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / ".github" / "scripts"))

import validate_frontend_release_workflow as validator  # noqa: E402


class FixedReleaseV2WorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = validator.load_workflow(
            validator.PRODUCTION_WORKFLOW_PATH
        )

    def _step(self, workflow: dict, job_name: str, step_name: str) -> dict:
        for step in workflow["jobs"][job_name]["steps"]:
            if step.get("name") == step_name:
                return step
        self.fail(f"missing step {step_name!r} in {job_name}")

    def test_current_fixed_v2_workflow_is_accepted(self) -> None:
        validator.assert_fixed_v2_production_workflow(self.workflow)
        validator.assert_single_build_and_strict_outputs(self.workflow)
        validator.assert_fixed_v2_prepare_and_control_contract(self.workflow)
        validator.assert_prewarm_contract("production-release")

    def test_push_trigger_is_rejected(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["on"]["push"] = {"branches": ["main"]}

        with self.assertRaisesRegex(
            AssertionError,
            "workflow_dispatch only",
        ):
            validator.assert_fixed_v2_production_workflow(workflow)

    def test_fifth_release_action_is_rejected(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["on"]["workflow_dispatch"]["inputs"]["release_mode"][
            "options"
        ].append("one-off-recovery")

        with self.assertRaisesRegex(AssertionError, "action contract"):
            validator.assert_fixed_v2_production_workflow(workflow)

    def test_control_job_requires_production_environment(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        del workflow["jobs"]["control_fixed_release_v2"]["environment"]

        with self.assertRaisesRegex(AssertionError, "production environment"):
            validator.assert_fixed_v2_production_workflow(workflow)

    def test_fifth_job_cannot_be_added(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["one_off_recovery"] = {
            "runs-on": "ubuntu-latest",
            "steps": [],
        }

        with self.assertRaisesRegex(AssertionError, "job set changed"):
            validator.assert_fixed_v2_production_workflow(workflow)

    def test_coordination_plan_must_bind_the_selected_operation(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        step = self._step(
            workflow,
            "release_coordination_guard",
            "Validate unpublished release coordination",
        )
        step["run"] = step["run"].replace(
            '--operation "$RELEASE_MODE"',
            "",
        )

        with self.assertRaisesRegex(AssertionError, "validation is missing"):
            validator.assert_fixed_v2_production_workflow(workflow)

    def test_coordination_revalidation_must_bind_the_target_sha(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        step = self._step(
            workflow,
            "control_fixed_release_v2",
            "Revalidate frozen coordination plan after approval",
        )
        step["run"] = step["run"].replace(
            '--target-sha "$TARGET_COMMIT_SHA"',
            "",
        )

        with self.assertRaisesRegex(AssertionError, "revalidation is missing"):
            validator.assert_fixed_v2_production_workflow(workflow)

    def test_prepare_reconfirm_must_bind_the_target_input(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        step = self._step(
            workflow,
            "deploy_tencent",
            "Reconfirm current main before first production mutation",
        )
        step["env"]["TARGET_COMMIT_SHA"] = "hard-coded"

        with self.assertRaisesRegex(AssertionError, "target input binding"):
            validator.assert_fixed_v2_prepare_and_control_contract(workflow)

    def test_frontend_release_binds_run_attempt_once(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        step = self._step(
            workflow,
            "build_frontend",
            "Create immutable frontend release",
        )
        step["run"] += '\n--run-attempt "$GITHUB_RUN_ATTEMPT"\n'

        with self.assertRaisesRegex(AssertionError, "run-attempt exactly once"):
            validator.assert_single_build_and_strict_outputs(workflow)

    def test_canonical_manifest_step_cannot_be_removed(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        steps = workflow["jobs"]["deploy_tencent"]["steps"]
        steps[:] = [
            step
            for step in steps
            if step.get("name") != "Generate canonical V2 release manifest"
        ]

        with self.assertRaisesRegex(AssertionError, "missing steps"):
            validator.assert_fixed_v2_prepare_and_control_contract(workflow)

    def test_active_pointer_must_be_the_rsync_basis(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        upload = self._step(
            workflow,
            "deploy_tencent",
            "Upload complete release archive with incremental rsync",
        )
        upload["run"] = upload["run"].replace(
            "/opt/jato/slots/8000/current",
            "/opt/jato/active",
        )

        with self.assertRaisesRegex(AssertionError, "incremental upload"):
            validator.assert_fixed_v2_prepare_and_control_contract(workflow)

    def test_control_job_cannot_build_or_upload_a_release(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        control = self._step(
            workflow,
            "control_fixed_release_v2",
            "Run fixed V2 control operation on Tencent",
        )
        control["run"] += "\nnpm run build\n"

        with self.assertRaisesRegex(AssertionError, "forbidden"):
            validator.assert_fixed_v2_prepare_and_control_contract(workflow)

    def test_control_job_uses_hash_bound_current_main_controller_bundle(self) -> None:
        control = self._step(
            self.workflow,
            "control_fixed_release_v2",
            "Run fixed V2 control operation on Tencent",
        )["run"]
        bundle_start = control.index("control_bundle_files=(")
        bundle_end = control.index("\n)", bundle_start)
        bundle_members = {
            line.strip()
            for line in control[bundle_start:bundle_end].splitlines()[1:]
            if line.strip()
        }
        self.assertEqual(
            bundle_members,
            {
                "03_Scripts/deploy/fixed_release_v2.py",
                "03_Scripts/deploy/fixed_release_v2_remote.sh",
                "03_Scripts/deploy/jato_quiescence_gate.py",
                "03_Scripts/deploy/nginx/jato_active_release_v2.conf",
                "03_Scripts/deploy/nginx/jato_candidate_preview_v2.conf",
                "03_Scripts/deploy/release_v2_admission.py",
                "03_Scripts/deploy/release_v2_store.py",
                "03_Scripts/deploy/systemd/jato-candidate-preview.service",
                "03_Scripts/deploy/systemd/jato-fullstack-backend@.service",
                "03_Scripts/deploy/systemd/"
                "jato-fullstack-backend@8001.service.d/"
                "20-candidate-readonly.conf",
                "03_Scripts/deploy/validate_release_archive.py",
            },
        )
        self.assertIn("V2_CONTROL_BUNDLE_SHA256", control)
        self.assertIn('sha256sum "$v2_control_archive"', control)
        self.assertIn('tar -tzf "$v2_control_archive"', control)
        self.assertIn('export V2_CONTROLLER_PATH="${v2_control_root}', control)
        self.assertIn(
            'v2_remote_entry="${v2_control_root}/03_Scripts/deploy/'
            'fixed_release_v2_remote.sh"',
            control,
        )
        self.assertIn('bash "$v2_remote_entry" "$@"', control)
        self.assertNotIn(
            "cat 03_Scripts/deploy/fixed_release_v2_remote.sh",
            control,
        )

    def test_rollback_requires_and_forwards_the_reviewed_identity(self) -> None:
        workflow = self.workflow
        control = self._step(
            workflow,
            "control_fixed_release_v2",
            "Validate fixed V2 operation intent",
        )["run"]
        self.assertIn("update-active|rollback-active)", control)
        remote = validator.FIXED_RELEASE_V2_REMOTE_PATH.read_text(encoding="utf-8")
        rollback = remote[remote.index("  rollback-active)") :]
        for token in (
            "require_identity",
            "RELEASE_V2_MANIFEST_SHA256",
            'rollback-active \\\n',
            '--commit "$DEPLOY_COMMIT_SHA"',
            '--archive-sha256 "$DEPLOY_ARCHIVE_SHA256"',
            '--manifest-sha256 "$RELEASE_V2_MANIFEST_SHA256"',
        ):
            self.assertIn(token, rollback)

    def test_control_job_cannot_drop_current_main_controller_binding(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        control = self._step(
            workflow,
            "control_fixed_release_v2",
            "Run fixed V2 control operation on Tencent",
        )
        control["run"] = control["run"].replace(
            "export V2_CONTROLLER_PATH",
            "export UNTRUSTED_CONTROLLER_PATH",
        )

        with self.assertRaisesRegex(AssertionError, "handoff is missing"):
            validator.assert_fixed_v2_prepare_and_control_contract(workflow)

    def test_archive_cache_root_is_bound_before_sudo_for_all_operations(self) -> None:
        control = self._step(
            self.workflow,
            "control_fixed_release_v2",
            "Run fixed V2 control operation on Tencent",
        )["run"]
        for token in (
            'ssh_home="$HOME"',
            'archive_cache_root="$ssh_home/.cache/jato-releases/archives"',
            'V2_ARCHIVE_CACHE_ROOT="$archive_cache_root"',
        ):
            self.assertIn(token, control)

        outer = (
            validator.REPO_ROOT
            / "03_Scripts/deploy/fullstack_remote_release.sh"
        ).read_text(encoding="utf-8")
        self.assertIn('V2_ARCHIVE_CACHE_ROOT="$ARCHIVE_ROOT_REAL"', outer)

        remote = validator.FIXED_RELEASE_V2_REMOTE_PATH.read_text(encoding="utf-8")
        self.assertIn('[[ "$V2_ARCHIVE_CACHE_ROOT" == /* ]]', remote)
        self.assertIn("require_archive_cache_root", remote)
        self.assertEqual(
            remote.count('--archive-cache-root "$V2_ARCHIVE_CACHE_ROOT"'),
            3,
        )

    def test_control_archive_cache_handoff_is_fail_closed(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        control = self._step(
            workflow,
            "control_fixed_release_v2",
            "Run fixed V2 control operation on Tencent",
        )
        control["run"] = control["run"].replace(
            'V2_ARCHIVE_CACHE_ROOT="$archive_cache_root"',
            'IGNORED_ARCHIVE_CACHE_ROOT="$archive_cache_root"',
        )

        with self.assertRaisesRegex(AssertionError, "handoff is missing"):
            validator.assert_fixed_v2_prepare_and_control_contract(workflow)

    def test_remote_rejects_missing_or_relative_archive_cache_root(self) -> None:
        remote = validator.FIXED_RELEASE_V2_REMOTE_PATH
        base_env = {
            **os.environ,
            "PRODUCTION_LOCK_PATH": "/tmp/production-deploy.lock",
        }
        for value in (None, ".cache/jato-releases/archives"):
            env = dict(base_env)
            if value is None:
                env.pop("V2_ARCHIVE_CACHE_ROOT", None)
            else:
                env["V2_ARCHIVE_CACHE_ROOT"] = value
            result = subprocess.run(
                ["bash", str(remote), "discard-candidate"],
                check=False,
                capture_output=True,
                env=env,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(
                "V2_ARCHIVE_CACHE_ROOT must be one absolute path",
                result.stderr,
            )

    def test_prepare_builds_relocatable_runtime_seal_before_promotion(self) -> None:
        outer = (
            validator.REPO_ROOT
            / "03_Scripts/deploy/fullstack_remote_release.sh"
        ).read_text(encoding="utf-8")
        remote = validator.FIXED_RELEASE_V2_REMOTE_PATH.read_text(encoding="utf-8")
        for token in (
            'FRONTEND_ARTIFACT_IDENTITY="$FRONTEND_ARTIFACT_IDENTITY"',
            'FRONTEND_ARTIFACT_CHECKSUM="$FRONTEND_ARTIFACT_CHECKSUM"',
        ):
            self.assertIn(token, outer)
        source_build = 'python3 -B "$seal_helper" build'
        self.assertIn(source_build, remote)
        self.assertNotIn('"$root/.venv/bin/python" -B "$seal_helper" build', remote)
        self.assertLess(
            remote.index('  build_candidate_runtime "$root"'),
            remote.index(source_build),
        )
        self.assertIn('chmod 0444 "$root/.jato-source-seal.json"', remote)
        self.assertIn(
            'final_root="$RELEASES_ROOT/$DEPLOY_COMMIT_SHA/$DEPLOY_ARCHIVE_SHA256"',
            remote,
        )
        self.assertEqual(remote.count('--recorded-runtime-root "$final_root"'), 2)
        controller = (
            validator.REPO_ROOT / "03_Scripts/deploy/fixed_release_v2.py"
        ).read_text(encoding="utf-8")
        self.assertIn('operation="runtime seal verification"', controller)

    def test_prepare_cleanup_is_bounded_to_exact_stale_v2_staging(self) -> None:
        outer = (
            validator.REPO_ROOT
            / "03_Scripts/deploy/fullstack_remote_release.sh"
        ).read_text(encoding="utf-8")
        cleanup = outer[
            outer.index("prune_stale_v2_staging() {") :
            outer.index(
                '\nif [[ "$DEPLOY_RELEASE_SYSTEM" == "fixed-v2" ]]; then',
                outer.index("prune_stale_v2_staging() {"),
            )
        ]
        for token in (
            "/opt/jato/staging/$stale_name",
            "^prepare-[0-9a-f]{40}-[0-9a-f]{64}",
            "-mindepth 1",
            "-maxdepth 1",
            "-type d",
            "-mmin +1440",
            "-xdev",
            "rm -rf --one-file-system --",
        ):
            self.assertIn(token, cleanup)
        self.assertNotIn("/opt/jato/releases", cleanup)
        self.assertLess(
            outer.index("prune_stale_v2_staging\n", outer.index("fixed-v2")),
            outer.index('sudo -n mktemp -d \\\n'),
        )

    def test_direct_legacy_v1_entry_is_disabled(self) -> None:
        outer = (
            validator.REPO_ROOT
            / "03_Scripts/deploy/fullstack_remote_release.sh"
        ).read_text(encoding="utf-8")
        self.assertIn(
            'DEPLOY_RELEASE_SYSTEM="${DEPLOY_RELEASE_SYSTEM:-fixed-v2}"',
            outer,
        )
        self.assertIn(
            'if [[ "$DEPLOY_RELEASE_SYSTEM" != "fixed-v2" ]]; then',
            outer,
        )
        self.assertIn("Direct legacy-v1 release entry is disabled", outer)
        self.assertIn('if [[ "$DEPLOY_RELEASE_SYSTEM" == "legacy-v1" ]]; then', outer)

    def test_update_active_gate_accepts_only_exact_legacy_or_v2_active(self) -> None:
        remote = validator.FIXED_RELEASE_V2_REMOTE_PATH.read_text(encoding="utf-8")
        controller = (
            validator.REPO_ROOT / "03_Scripts/deploy/fixed_release_v2.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            'local controller="$V2_CONTROLLER_PATH"',
            remote,
        )
        self.assertIn(
            "V2_CONTROLLER_PATH is required for fixed V2 control actions",
            remote,
        )
        self.assertIn(
            "Active current is neither a V2 release nor the exact legacy root",
            controller,
        )
        self.assertIn("legacy_root.is_symlink()", controller)
        self.assertIn(
            "Path(current_anchor[-1]) != expected_root",
            controller,
        )
        self.assertIn("read_pointer(self.config.layout, ACTIVE_SLOT", controller)
        self.assertIn("with self.jato_lock_holder(", controller)

if __name__ == "__main__":
    unittest.main()
