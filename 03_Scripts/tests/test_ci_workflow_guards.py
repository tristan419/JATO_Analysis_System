from __future__ import annotations

import copy
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / ".github" / "scripts"))

import validate_frontend_release_workflow as workflow_validator  # noqa: E402


class CIWorkflowGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = workflow_validator.load_workflow(
            workflow_validator.CI_WORKFLOW_PATH
        )

    def test_current_required_jobs_are_fail_closed_and_use_merge_result(self) -> None:
        workflow_validator.assert_required_ci_jobs_fail_closed(self.workflow)
        workflow_validator.assert_pull_request_merge_result_checkout(self.workflow)

    def test_each_required_job_rejects_continue_on_error(self) -> None:
        for job_name in workflow_validator.REQUIRED_CI_JOBS:
            with self.subTest(job=job_name):
                workflow = copy.deepcopy(self.workflow)
                workflow["jobs"][job_name]["continue-on-error"] = "true"

                with self.assertRaisesRegex(
                    AssertionError,
                    rf"CI job {job_name} must fail closed",
                ):
                    workflow_validator.assert_required_ci_jobs_fail_closed(workflow)

    def test_each_required_context_rejects_display_name_override(self) -> None:
        for job_name in workflow_validator.REQUIRED_CI_JOBS:
            with self.subTest(job=job_name):
                workflow = copy.deepcopy(self.workflow)
                workflow["jobs"][job_name]["name"] = f"renamed-{job_name}"

                with self.assertRaisesRegex(
                    AssertionError,
                    rf"CI job {job_name} must retain its required context name",
                ):
                    workflow_validator.assert_required_ci_jobs_fail_closed(workflow)

    def test_each_required_job_rejects_job_level_condition(self) -> None:
        for job_name in workflow_validator.REQUIRED_CI_JOBS:
            with self.subTest(job=job_name):
                workflow = copy.deepcopy(self.workflow)
                workflow["jobs"][job_name]["if"] = "${{ false }}"

                with self.assertRaisesRegex(
                    AssertionError,
                    rf"CI job {job_name} must not be conditional",
                ):
                    workflow_validator.assert_required_ci_jobs_fail_closed(workflow)

    def test_missing_required_context_is_rejected(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        del workflow["jobs"]["smoke"]

        with self.assertRaisesRegex(
            AssertionError,
            r"job smoke must be a mapping",
        ):
            workflow_validator.assert_required_ci_jobs_fail_closed(workflow)

    def test_required_job_rejects_conditional_continue_on_error(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["smoke"]["continue-on-error"] = (
            "${{ github.event_name == 'pull_request' }}"
        )

        with self.assertRaisesRegex(
            AssertionError,
            r"CI job smoke must fail closed",
        ):
            workflow_validator.assert_required_ci_jobs_fail_closed(workflow)

    def test_required_step_rejects_continue_on_error(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["fullstack-frontend"]["steps"][-1][
            "continue-on-error"
        ] = "true"

        with self.assertRaisesRegex(
            AssertionError,
            r"CI job fullstack-frontend step \d+ must fail closed",
        ):
            workflow_validator.assert_required_ci_jobs_fail_closed(workflow)

    def test_every_required_step_rejects_step_level_condition(self) -> None:
        for job_name in workflow_validator.REQUIRED_CI_JOBS:
            job_steps = self.workflow["jobs"][job_name]["steps"]
            for step_index in range(len(job_steps)):
                with self.subTest(job=job_name, step=step_index + 1):
                    workflow = copy.deepcopy(self.workflow)
                    workflow["jobs"][job_name]["steps"][step_index][
                        "if"
                    ] = "${{ false }}"

                    with self.assertRaisesRegex(
                        AssertionError,
                        rf"CI job {job_name} step {step_index + 1} "
                        r"must not be conditional",
                    ):
                        workflow_validator.assert_required_ci_jobs_fail_closed(
                            workflow
                        )

    def test_required_job_allows_explicit_false(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["smoke"]["continue-on-error"] = "false"

        workflow_validator.assert_required_ci_jobs_fail_closed(workflow)

    def test_each_required_job_rejects_checkout_ref_override(self) -> None:
        for job_name in workflow_validator.REQUIRED_CI_JOBS:
            with self.subTest(job=job_name):
                workflow = copy.deepcopy(self.workflow)
                checkout = next(
                    step
                    for step in workflow["jobs"][job_name]["steps"]
                    if step.get("uses") == "actions/checkout@v4"
                )
                checkout["with"] = {"ref": "${{ github.event.pull_request.head.sha }}"}

                with self.assertRaisesRegex(
                    AssertionError,
                    rf"CI job {job_name} must checkout the pull_request merge result",
                ):
                    workflow_validator.assert_pull_request_merge_result_checkout(
                        workflow
                    )

    def test_fullstack_backend_requires_readiness_regression_test(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        api_contract_step = next(
            step
            for step in workflow["jobs"]["fullstack-backend"]["steps"]
            if step.get("name") == "API contract tests"
        )
        api_contract_step["run"] = api_contract_step["run"].replace(
            "tests/unit/test_readiness_service.py",
            "",
        )

        with self.assertRaisesRegex(
            AssertionError,
            r"required backend readiness CI is incomplete",
        ):
            workflow_validator.assert_backend_readiness_ci_contract(workflow)

    def test_readiness_cannot_move_to_a_separate_unbounded_pytest(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        api_contract_step = next(
            step
            for step in workflow["jobs"]["fullstack-backend"]["steps"]
            if step.get("name") == "API contract tests"
        )
        api_contract_step["run"] = api_contract_step["run"].replace(
            "tests/unit/test_readiness_service.py",
            "",
        )
        api_contract_step["run"] += (
            "\npython -m pytest tests/unit/test_readiness_service.py -q\n"
        )

        with self.assertRaisesRegex(
            AssertionError,
            r"must share one pytest invocation",
        ):
            workflow_validator.assert_backend_readiness_ci_contract(workflow)

    def test_readiness_pytest_step_cannot_be_conditional(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        api_contract_step = next(
            step
            for step in workflow["jobs"]["fullstack-backend"]["steps"]
            if step.get("name") == "API contract tests"
        )
        api_contract_step["if"] = "${{ false }}"

        with self.assertRaisesRegex(
            AssertionError,
            r"backend API and readiness test step must not be conditional",
        ):
            workflow_validator.assert_backend_readiness_ci_contract(workflow)

    def test_readiness_pytest_step_cannot_be_removed(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["fullstack-backend"]["steps"] = [
            step
            for step in workflow["jobs"]["fullstack-backend"]["steps"]
            if step.get("name") != "API contract tests"
        ]

        with self.assertRaisesRegex(
            AssertionError,
            r"expected one step named 'API contract tests'; found 0",
        ):
            workflow_validator.assert_backend_readiness_ci_contract(workflow)

    def test_bluegreen_storage_guard_text_contract_rejects_unsafe_mutations(self) -> None:
        remote = "03_Scripts/deploy/jato_release_storage_guard.py"
        controller = "\n".join(
            (
                "03_Scripts/deploy/jato_release_storage_guard.py",
                "BLUEGREEN_MIN_TOTAL_MEMORY_BYTES=$((14 * 1024 * 1024 * 1024))",
                "BLUEGREEN_MIN_AVAILABLE_MEMORY_BYTES=$((5 * 1024 * 1024 * 1024))",
                "BLUEGREEN_CANDIDATE_MAX_MEMORY_BYTES=$((4 * 1024 * 1024 * 1024))",
                "BLUEGREEN_OS_MEMORY_RESERVE_BYTES=$((2 * 1024 * 1024 * 1024))",
                "BLUEGREEN_ACTIVE_MEMORY_HIGH_BYTES=$((6 * 1024 * 1024 * 1024))",
                "BLUEGREEN_ACTIVE_MEMORY_MAX_BYTES=$((8 * 1024 * 1024 * 1024))",
                "BLUEGREEN_PREPARE_DISK_RESERVE_BYTES=$((15 * 1024 * 1024 * 1024))",
                "BLUEGREEN_PREPARE_DISK_RESERVE_PERCENT=8",
                "BLUEGREEN_RUNTIME_DISK_RESERVE_BYTES=$((10 * 1024 * 1024 * 1024))",
                "BLUEGREEN_RUNTIME_DISK_RESERVE_PERCENT=5",
                "BLUEGREEN_RELEASE_KEEP_UNREFERENCED=3",
                "BLUEGREEN_RELEASE_NORMAL_GC_AGE_SECONDS=$((14 * 24 * 60 * 60))",
                "BLUEGREEN_RELEASE_EMERGENCY_GC_AGE_SECONDS=$((24 * 60 * 60))",
                "guard_release_storage() { :; }",
                "--expected-active-memory-high-bytes",
                "--expected-active-memory-max-bytes",
                "assert_runtime_storage_reserve() { :; }",
                "run_candidate_build_scope() {",
                "  systemd-run --scope",
                '  --property="MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH"',
                '  --property="MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX"',
                '  --property="TasksMax=512"',
                "}",
                "build_candidate_runtime_locked() {",
                "  assert_candidate_build_scope",
                "  assert_inherited_production_lock",
                "  prepare_candidate_runtime",
                "  if true; then",
                "    run_inner_prepare",
                "    finalize_runtime_seal",
                "  fi",
                "}",
                "prepare_and_switch() {",
                "  require_environment",
                "  assert_inherited_production_lock",
                "  ensure_bluegreen_state_root",
                "  ensure_bluegreen_runtime_roots",
                "  guard_release_storage",
                "  assert_host_memory_budget",
                "  materialize_release_source",
                "  run_candidate_build_scope",
                "  verify_final_runtime_seal",
                "  assert_no_database_migration_delta",
                "  assert_runtime_storage_reserve",
                "  assert_host_memory_budget",
                "  install_slot_runtime",
                "}",
            )
        )
        guard = "\n".join(
            (
                ".jato-release-identity",
                "cross_release_is_settled",
                "DANGEROUS_RETRY_CLASSES",
                "expected_repository",
                "read_active_frontend_root",
                "/proc/self/mountinfo",
                "os.replace",
                "os.fsync",
                ".gc-",
                "shutil.rmtree(entry.path)",
            )
        )
        boot = "read_active_slot()"

        workflow_validator.assert_bluegreen_storage_guard_text_contract(
            remote,
            controller,
            guard,
            boot,
        )

        mutations = (
            ("remote", remote.replace("jato_release_storage_guard.py", "missing.py")),
            ("controller", controller.replace("5 * 1024 * 1024 * 1024", "2 * 1024")),
            ("controller", controller.replace("6 * 1024 * 1024 * 1024", "7 * 1024")),
            ("controller", controller.replace("8 * 1024 * 1024 * 1024", "9 * 1024")),
            ("controller", controller.replace("15 * 1024 * 1024 * 1024", "14 * 1024")),
            ("controller", controller.replace("DISK_RESERVE_PERCENT=8", "DISK_RESERVE_PERCENT=7")),
            ("controller", controller.replace("RELEASE_KEEP_UNREFERENCED=3", "RELEASE_KEEP_UNREFERENCED=2")),
            ("controller", controller.replace("14 * 24 * 60 * 60", "7 * 24 * 60 * 60")),
            ("controller", controller.replace("24 * 60 * 60", "0")),
            (
                "controller",
                controller.replace(
                    "systemd-run --scope",
                    "systemd-run --scope --wait",
                ),
            ),
            (
                "controller",
                controller.replace(
                    "  assert_candidate_build_scope\n",
                    "",
                ),
            ),
            (
                "controller",
                controller.replace(
                    "--expected-active-memory-high-bytes\n",
                    "",
                ),
            ),
            (
                "controller",
                controller.replace(
                    "--expected-active-memory-max-bytes\n",
                    "",
                ),
            ),
            (
                "controller",
                controller.replace(
                    "  guard_release_storage\n"
                    "  assert_host_memory_budget\n"
                    "  materialize_release_source",
                    "  materialize_release_source\n"
                    "  assert_host_memory_budget\n"
                    "  guard_release_storage",
                ),
            ),
            (
                "controller",
                controller.replace(
                    "  assert_inherited_production_lock\n"
                    "  ensure_bluegreen_state_root",
                    "  ensure_bluegreen_state_root",
                ),
            ),
            (
                "controller",
                controller.replace(
                    "  ensure_bluegreen_runtime_roots\n"
                    "  guard_release_storage",
                    "  guard_release_storage\n"
                    "  ensure_bluegreen_runtime_roots",
                ),
            ),
            ("guard", guard + "\nshutil.rmtree(releases_root)"),
            ("boot", boot + "\nshutil.rmtree(release_root)"),
        )
        for target, mutated in mutations:
            with self.subTest(target=target):
                values = {
                    "remote": remote,
                    "controller": controller,
                    "guard": guard,
                    "boot": boot,
                }
                values[target] = mutated
                with self.assertRaises(AssertionError):
                    workflow_validator.assert_bluegreen_storage_guard_text_contract(
                        values["remote"],
                        values["controller"],
                        values["guard"],
                        values["boot"],
                    )


if __name__ == "__main__":
    unittest.main()
