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


if __name__ == "__main__":
    unittest.main()
