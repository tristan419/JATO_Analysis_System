#!/usr/bin/env python3
"""Validate that production-capable GitHub Actions jobs are main-only."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MAIN_REF = "refs/heads/main"
MAIN_REF_CONDITION = f"github.ref == '{MAIN_REF}'"
PRODUCTION_ENVIRONMENT = "production"
PRODUCTION_RELEASE_WORKFLOW = ".github/workflows/production-release.yml"
RELEASE_COORDINATION_WORKFLOW = (
    ".github/workflows/release-coordination-guard.yml"
)
RELEASE_COORDINATION_SCRIPT = (
    ".github/scripts/release_coordination_guard.py"
)
RELEASE_COORDINATION_CONTEXT = "release-coordination-guard"
RELEASE_COORDINATION_EVALUATOR = "release-coordination-evaluator"
PINNED_CHECKOUT_V5 = (
    "actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09"
)
PRODUCTION_COORDINATION_JOB = "release_coordination_guard"

PRODUCTION_JOBS = {
    PRODUCTION_RELEASE_WORKFLOW: ("deploy_tencent",),
    ".github/workflows/deploy-aws-ecs.yml": ("deploy",),
    ".github/workflows/deploy-ec2-auto-update.yml": ("deploy",),
    ".github/workflows/hermes-devsync.yml": ("devsync",),
}

MANUAL_DEPLOY_WORKFLOWS = {
    ".github/workflows/deploy-aws-ecs.yml",
    ".github/workflows/deploy-ec2-auto-update.yml",
}
PRODUCTION_RELEASE_MAIN_ONLY_JOBS = (
    PRODUCTION_COORDINATION_JOB,
    "build_frontend",
    "audit_frontend_parity",
)

COUNTRY_NEWS_WORKFLOW = ".github/workflows/country-news-sync.yml"
COUNTRY_NEWS_JOB_CONDITION = (
    "github.event.inputs.dry_run == 'true' || " + MAIN_REF_CONDITION
)
COUNTRY_NEWS_ENVIRONMENT = (
    "github.event.inputs.dry_run == 'true' && 'preview' || 'production'"
)
COUNTRY_NEWS_DATABASE_SECRET = (
    MAIN_REF_CONDITION
    + " && github.event.inputs.dry_run != 'true' "
    + "&& secrets.APP_DATABASE_URL || ''"
)


def load_workflow(relative_path: str) -> Mapping[str, Any]:
    workflow_path = REPO_ROOT / relative_path
    payload = yaml.load(workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    if not isinstance(payload, Mapping):
        raise AssertionError(f"{relative_path}: workflow root must be a mapping")
    return payload


def get_job(workflow: Mapping[str, Any], relative_path: str, job_name: str) -> Mapping[str, Any]:
    jobs = workflow.get("jobs")
    if not isinstance(jobs, Mapping):
        raise AssertionError(f"{relative_path}: jobs must be a mapping")
    job = jobs.get(job_name)
    if not isinstance(job, Mapping):
        raise AssertionError(f"{relative_path}: missing job {job_name!r}")
    return job


def get_steps(
    job: Mapping[str, Any],
    relative_path: str,
    job_name: str,
) -> list[Mapping[str, Any]]:
    steps = job.get("steps")
    if not isinstance(steps, list):
        raise AssertionError(f"{relative_path}:{job_name} steps must be a list")
    result: list[Mapping[str, Any]] = []
    for index, step in enumerate(steps):
        if not isinstance(step, Mapping):
            raise AssertionError(
                f"{relative_path}:{job_name} step {index} must be a mapping"
            )
        result.append(step)
    return result


def unwrap_expression(value: Any) -> str:
    expression = str(value or "").strip()
    if expression.startswith("${{") and expression.endswith("}}"):
        return expression[3:-2].strip()
    return expression


def get_environment_name(job: Mapping[str, Any]) -> str:
    environment = job.get("environment")
    if isinstance(environment, Mapping):
        environment = environment.get("name")
    return unwrap_expression(environment)


def assert_main_only_job(relative_path: str, job_name: str) -> Mapping[str, Any]:
    workflow = load_workflow(relative_path)
    job = get_job(workflow, relative_path, job_name)

    condition = unwrap_expression(job.get("if"))
    if condition != MAIN_REF_CONDITION:
        raise AssertionError(
            f"{relative_path}:{job_name} must use the exact job-level main guard; "
            f"found {condition!r}"
        )
    return job


def assert_main_only_production_job(relative_path: str, job_name: str) -> None:
    job = assert_main_only_job(relative_path, job_name)

    environment = get_environment_name(job)
    if environment != PRODUCTION_ENVIRONMENT:
        raise AssertionError(
            f"{relative_path}:{job_name} must use the production environment; "
            f"found {environment!r}"
        )


def assert_manual_deploy_is_skipped_for_non_main(
    relative_path: str,
    job_name: str,
) -> None:
    workflow = load_workflow(relative_path)
    triggers = workflow.get("on")
    if not isinstance(triggers, Mapping) or "workflow_dispatch" not in triggers:
        raise AssertionError(f"{relative_path}: expected a workflow_dispatch trigger")

    job = get_job(workflow, relative_path, job_name)
    condition = unwrap_expression(job.get("if"))
    simulated_feature_ref = "refs/heads/codex/static-contract-probe"
    can_run = condition == MAIN_REF_CONDITION and simulated_feature_ref == MAIN_REF
    if can_run:
        raise AssertionError(
            f"{relative_path}:{job_name} could run for workflow_dispatch on a feature ref"
        )


def assert_all_deploy_workflows_are_registered() -> None:
    discovered_workflows = {
        str(path.relative_to(REPO_ROOT))
        for path in (REPO_ROOT / ".github/workflows").glob("deploy-*.yml")
    }
    if discovered_workflows != MANUAL_DEPLOY_WORKFLOWS:
        missing_contracts = sorted(discovered_workflows - MANUAL_DEPLOY_WORKFLOWS)
        stale_contracts = sorted(MANUAL_DEPLOY_WORKFLOWS - discovered_workflows)
        raise AssertionError(
            "deploy workflow registry is out of date; "
            f"missing contracts={missing_contracts}, stale contracts={stale_contracts}"
        )

    for relative_path in sorted(discovered_workflows):
        workflow = load_workflow(relative_path)
        jobs = workflow.get("jobs")
        if not isinstance(jobs, Mapping):
            raise AssertionError(f"{relative_path}: jobs must be a mapping")
        expected_jobs = set(PRODUCTION_JOBS[relative_path])
        discovered_jobs = {str(job_name) for job_name in jobs}
        if discovered_jobs != expected_jobs:
            raise AssertionError(
                f"{relative_path}: every job in a deploy workflow needs an explicit "
                "production contract; "
                f"registered={sorted(expected_jobs)}, discovered={sorted(discovered_jobs)}"
            )


def assert_all_static_production_jobs_are_registered() -> None:
    registered_jobs = {
        (path, job_name)
        for path, job_names in PRODUCTION_JOBS.items()
        for job_name in job_names
    }
    for workflow_path in sorted((REPO_ROOT / ".github/workflows").glob("*.yml")):
        relative_path = str(workflow_path.relative_to(REPO_ROOT))
        workflow = load_workflow(relative_path)
        jobs = workflow.get("jobs")
        if not isinstance(jobs, Mapping):
            continue
        for job_name, job in jobs.items():
            if not isinstance(job, Mapping):
                continue
            if get_environment_name(job) != PRODUCTION_ENVIRONMENT:
                continue
            if (relative_path, str(job_name)) not in registered_jobs:
                raise AssertionError(
                    f"{relative_path}:{job_name} uses the production environment "
                    "without a registered main-only contract"
                )


def assert_production_release_main_guards() -> None:
    for job_name in PRODUCTION_RELEASE_MAIN_ONLY_JOBS:
        assert_main_only_job(PRODUCTION_RELEASE_WORKFLOW, job_name)


def assert_pull_request_release_coordination_guard() -> None:
    workflow = load_workflow(RELEASE_COORDINATION_WORKFLOW)
    if workflow.get("name") != "release-coordination":
        raise AssertionError("release coordination workflow name changed")
    triggers = workflow.get("on")
    if not isinstance(triggers, Mapping):
        raise AssertionError("release coordination triggers must be a mapping")
    if set(triggers) != {"pull_request_target", "issues", "workflow_dispatch"}:
        raise AssertionError(
            "release coordination may only use pull_request_target, issues, "
            "and workflow_dispatch"
        )
    pull_request_target = triggers.get("pull_request_target")
    if not isinstance(pull_request_target, Mapping):
        raise AssertionError("pull_request_target trigger must be a mapping")
    if pull_request_target.get("branches") != ["main"]:
        raise AssertionError("pull_request_target must be limited to main")
    expected_pr_events = {
        "opened",
        "reopened",
        "synchronize",
        "edited",
        "labeled",
        "unlabeled",
        "ready_for_review",
        "converted_to_draft",
        "closed",
    }
    if set(pull_request_target.get("types") or []) != expected_pr_events:
        raise AssertionError("pull_request_target event coverage is incomplete")
    issues = triggers.get("issues")
    if not isinstance(issues, Mapping):
        raise AssertionError("issues trigger must be a mapping")
    if set(issues.get("types") or []) != {
        "opened",
        "edited",
        "closed",
        "reopened",
        "labeled",
        "unlabeled",
    }:
        raise AssertionError("release-group issue event coverage is incomplete")

    permissions = workflow.get("permissions")
    if permissions != {
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
        "statuses": "write",
    }:
        raise AssertionError(
            "release coordination must retain exact least-privilege permissions"
        )
    concurrency = workflow.get("concurrency")
    if concurrency != {
        "group": "release-coordination-status-sweeper",
        "cancel-in-progress": "false",
    }:
        raise AssertionError(
            "release coordination sweeps must be serialized without cancellation"
        )
    jobs = workflow.get("jobs")
    if not isinstance(jobs, Mapping) or set(jobs) != {"evaluate"}:
        raise AssertionError("release coordination must have one evaluator job")
    evaluator = get_job(workflow, RELEASE_COORDINATION_WORKFLOW, "evaluate")
    if evaluator.get("name") != RELEASE_COORDINATION_EVALUATOR:
        raise AssertionError("evaluator job name must not collide with required context")
    if evaluator.get("environment"):
        raise AssertionError("PR evaluator must never enter an environment")
    steps = get_steps(evaluator, RELEASE_COORDINATION_WORKFLOW, "evaluate")
    if [step.get("name") for step in steps] != [
        "Checkout trusted main guard",
        "Revoke stale PR success before evaluation",
        "Refresh release coordination status on open PR heads",
    ]:
        raise AssertionError("release coordination evaluator steps changed")
    checkout = steps[0]
    if checkout.get("uses") != PINNED_CHECKOUT_V5:
        raise AssertionError("trusted guard checkout must be pinned to an immutable SHA")
    checkout_with = checkout.get("with")
    if not isinstance(checkout_with, Mapping):
        raise AssertionError("trusted guard checkout configuration is missing")
    if checkout_with.get("ref") != "refs/heads/main":
        raise AssertionError("pull_request_target must checkout trusted main")
    if checkout_with.get("persist-credentials") != "false":
        raise AssertionError("trusted guard checkout must not persist credentials")
    pending_command = str(steps[1].get("run") or "")
    if (
        "mark-pending" not in pending_command
        or '--pull-request "$TARGET_PULL_REQUEST"' not in pending_command
        or steps[1].get("if")
        != (
            "${{ github.event_name == 'pull_request_target' "
            "&& github.event.action != 'closed' }}"
        )
    ):
        raise AssertionError("PR events must revoke stale success before evaluation")
    command = str(steps[2].get("run") or "")
    if (
        f"python3 {RELEASE_COORDINATION_SCRIPT} sweep" not in command
        or RELEASE_COORDINATION_CONTEXT not in (
            REPO_ROOT / RELEASE_COORDINATION_SCRIPT
        ).read_text(encoding="utf-8")
    ):
        raise AssertionError("PR evaluator must run the trusted status sweeper")
    workflow_text = (
        REPO_ROOT / RELEASE_COORDINATION_WORKFLOW
    ).read_text(encoding="utf-8")
    for forbidden in (
        "github.event.pull_request.head.sha",
        "github.event.pull_request.head.ref",
        "github.head_ref",
        "github.event.pull_request.body",
        "github.event.pull_request.title",
        "actions/download-artifact",
        "actions/cache",
        "npm install",
        "npm ci",
        "pip install",
        "requirements.txt",
    ):
        if forbidden in workflow_text:
            raise AssertionError(
                f"pull_request_target workflow references untrusted {forbidden}"
            )


def assert_production_release_coordination_guard() -> None:
    workflow = load_workflow(PRODUCTION_RELEASE_WORKFLOW)
    permissions = workflow.get("permissions")
    if not isinstance(permissions, Mapping):
        raise AssertionError("production permissions must be a mapping")
    for permission, expected in {
        "actions": "read",
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
    }.items():
        if permissions.get(permission) != expected:
            raise AssertionError(
                f"production permission {permission} must be {expected}"
            )

    jobs = workflow.get("jobs")
    if not isinstance(jobs, Mapping):
        raise AssertionError("production jobs must be a mapping")
    if next(iter(jobs), None) != PRODUCTION_COORDINATION_JOB:
        raise AssertionError("release coordination must be the first production job")
    preflight = get_job(
        workflow,
        PRODUCTION_RELEASE_WORKFLOW,
        PRODUCTION_COORDINATION_JOB,
    )
    if preflight.get("needs") is not None:
        raise AssertionError("production coordination preflight cannot have dependencies")
    if preflight.get("environment") is not None:
        raise AssertionError("production coordination preflight cannot enter an environment")
    preflight_steps = get_steps(
        preflight,
        PRODUCTION_RELEASE_WORKFLOW,
        PRODUCTION_COORDINATION_JOB,
    )
    if [step.get("name") for step in preflight_steps] != [
        "Checkout release coordination guard",
        "Validate unpublished release coordination",
        "Freeze release coordination plan",
    ]:
        raise AssertionError("production coordination preflight steps changed")
    checkout_with = preflight_steps[0].get("with")
    if not isinstance(checkout_with, Mapping):
        raise AssertionError("production coordination checkout is missing configuration")
    if checkout_with.get("ref") != "${{ github.sha }}":
        raise AssertionError(
            "production coordination must checkout the exact target main SHA"
        )
    if checkout_with.get("persist-credentials") != "false":
        raise AssertionError(
            "production coordination checkout must not persist credentials"
        )
    preflight_command = str(preflight_steps[1].get("run") or "")
    for required in (
        RELEASE_COORDINATION_SCRIPT,
        "production",
        '--main-sha "$GITHUB_SHA"',
        '--plan-output "$RUNNER_TEMP/release-coordination-plan.json"',
    ):
        if required not in preflight_command:
            raise AssertionError(
                f"production coordination preflight is missing {required}"
            )
    if "secrets." in str(preflight):
        raise AssertionError("coordination preflight must not consume production secrets")
    freeze = preflight_steps[2]
    if freeze.get("uses") != "actions/upload-artifact@v4":
        raise AssertionError("coordination plan must use upload-artifact@v4")
    freeze_with = freeze.get("with")
    if not isinstance(freeze_with, Mapping):
        raise AssertionError("coordination plan upload configuration is missing")
    expected_artifact = {
        "name": "release-coordination-plan-${{ github.sha }}-${{ github.run_attempt }}",
        "path": "${{ runner.temp }}/release-coordination-plan.json",
        "if-no-files-found": "error",
        "compression-level": "0",
        "overwrite": "false",
        "retention-days": "7",
    }
    for key, expected in expected_artifact.items():
        if freeze_with.get(key) != expected:
            raise AssertionError(
                f"coordination plan artifact {key} must be {expected!r}"
            )

    build = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, "build_frontend")
    if build.get("needs") != PRODUCTION_COORDINATION_JOB:
        raise AssertionError("frontend build must wait for release coordination")

    deploy = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, "deploy_tencent")
    deploy_steps = get_steps(deploy, PRODUCTION_RELEASE_WORKFLOW, "deploy_tencent")
    expected_first_steps = [
        "Checkout release source",
        "Download frozen release coordination plan",
        "Revalidate frozen coordination plan after approval",
    ]
    if [step.get("name") for step in deploy_steps[:3]] != expected_first_steps:
        raise AssertionError(
            "production approval must be followed immediately by frozen-plan validation"
        )
    if "secrets." in str(deploy_steps[:3]):
        raise AssertionError(
            "frozen-plan validation must run before production secrets are consumed"
        )
    download_with = deploy_steps[1].get("with")
    if not isinstance(download_with, Mapping):
        raise AssertionError("coordination plan download configuration is missing")
    if download_with.get("name") != expected_artifact["name"]:
        raise AssertionError("deploy must download the exact same-run coordination plan")
    verify_command = str(deploy_steps[2].get("run") or "")
    for required in (
        RELEASE_COORDINATION_SCRIPT,
        "verify-plan",
        '--main-sha "$GITHUB_SHA"',
        "--plan "
        '"$RUNNER_TEMP/release-coordination-plan/release-coordination-plan.json"',
    ):
        if required not in verify_command:
            raise AssertionError(
                f"post-approval plan verification is missing {required}"
            )
    step_names = [str(step.get("name") or "") for step in deploy_steps]
    if step_names.index("Revalidate frozen coordination plan after approval") > step_names.index(
        "Validate Tencent deploy credentials"
    ):
        raise AssertionError("coordination plan must pass before deployment credentials")
    mutation_recheck = "Reconfirm current main before first production mutation"
    if mutation_recheck not in step_names:
        raise AssertionError("production must recheck current main before the first mutation")
    if not (
        step_names.index("Record transport-verified candidate checkpoint")
        < step_names.index(mutation_recheck)
        < step_names.index("Deploy verified release on Tencent")
    ):
        raise AssertionError(
            "final stale-main check must run after transport and before deployment"
        )
    mutation_command = str(
        deploy_steps[step_names.index(mutation_recheck)].get("run") or ""
    )
    if "verify-plan" not in mutation_command:
        raise AssertionError("pre-mutation stale-main check must consume the frozen plan")


def assert_country_news_production_write_is_main_only() -> None:
    workflow = load_workflow(COUNTRY_NEWS_WORKFLOW)
    job = get_job(workflow, COUNTRY_NEWS_WORKFLOW, "sync")

    condition = unwrap_expression(job.get("if"))
    if condition != COUNTRY_NEWS_JOB_CONDITION:
        raise AssertionError(
            f"{COUNTRY_NEWS_WORKFLOW}:sync must allow feature refs only for dry-run; "
            f"found {condition!r}"
        )

    environment = get_environment_name(job)
    if environment != COUNTRY_NEWS_ENVIRONMENT:
        raise AssertionError(
            f"{COUNTRY_NEWS_WORKFLOW}:sync must select production only for non-dry-run; "
            f"found {environment!r}"
        )

    steps = job.get("steps")
    if not isinstance(steps, list):
        raise AssertionError(f"{COUNTRY_NEWS_WORKFLOW}:sync steps must be a list")
    sync_step = next(
        (
            step
            for step in steps
            if isinstance(step, Mapping) and step.get("name") == "Country news sync"
        ),
        None,
    )
    if not isinstance(sync_step, Mapping):
        raise AssertionError(f"{COUNTRY_NEWS_WORKFLOW}: missing Country news sync step")
    environment_values = sync_step.get("env")
    if not isinstance(environment_values, Mapping):
        raise AssertionError(f"{COUNTRY_NEWS_WORKFLOW}: Country news sync env must be a mapping")
    for secret_name in ("APP_DATABASE_URL", "GEMINI_API_KEY"):
        secret_expression = unwrap_expression(environment_values.get(secret_name))
        expected_expression = COUNTRY_NEWS_DATABASE_SECRET.replace(
            "secrets.APP_DATABASE_URL",
            f"secrets.{secret_name}",
        )
        if secret_expression != expected_expression:
            raise AssertionError(
                f"{COUNTRY_NEWS_WORKFLOW}: {secret_name} must be blank for preview; "
                f"found {secret_expression!r}"
            )


def assert_database_migration_is_behind_main_release_gate() -> None:
    production_workflow = (
        REPO_ROOT / PRODUCTION_RELEASE_WORKFLOW
    ).read_text(encoding="utf-8")
    remote_release = (
        REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
    ).read_text(encoding="utf-8")
    bluegreen_release = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    ).read_text(encoding="utf-8")
    server_deploy = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")

    if "03_Scripts/deploy/fullstack_remote_release.sh" not in production_workflow:
        raise AssertionError("production release no longer invokes Tencent remote release")
    if "export DEPLOY_BRANCH=main" not in production_workflow:
        raise AssertionError("production release must pin Tencent DEPLOY_BRANCH=main")
    if 'bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh"' not in remote_release:
        raise AssertionError("remote release no longer invokes the blue/green controller")
    if 'bash "$INNER_DEPLOY"' not in bluegreen_release:
        raise AssertionError("blue/green controller no longer invokes the guarded server deploy")
    if "PRODUCTION_RELEASE_WORKFLOW=true" not in bluegreen_release:
        raise AssertionError("blue/green controller must identify the production release workflow")
    if "RUN_DATABASE_MIGRATIONS=false" not in bluegreen_release:
        raise AssertionError(
            "blue/green v1 must keep schema mutation outside the slot switch",
        )
    if "python -m alembic upgrade head" not in server_deploy:
        raise AssertionError("expected Alembic production migration command was not found")
    if 'DEPLOY_BRANCH" != "main"' not in server_deploy:
        raise AssertionError("database migration must retain the main branch gate")
    if 'PRODUCTION_RELEASE_WORKFLOW" != "true"' not in server_deploy:
        raise AssertionError("database migration must require the production release workflow")


def main() -> None:
    assert_all_deploy_workflows_are_registered()
    assert_pull_request_release_coordination_guard()
    assert_production_release_coordination_guard()

    for relative_path, job_names in PRODUCTION_JOBS.items():
        for job_name in job_names:
            assert_main_only_production_job(relative_path, job_name)

    assert_all_static_production_jobs_are_registered()
    assert_production_release_main_guards()

    for relative_path in MANUAL_DEPLOY_WORKFLOWS:
        for job_name in PRODUCTION_JOBS[relative_path]:
            assert_manual_deploy_is_skipped_for_non_main(relative_path, job_name)

    assert_country_news_production_write_is_main_only()
    assert_database_migration_is_behind_main_release_gate()
    print(
        "Validated release coordination and main-only production gates for "
        f"{sum(len(job_names) for job_names in PRODUCTION_JOBS.values())} "
        "production jobs and "
        f"{len(MANUAL_DEPLOY_WORKFLOWS)} manual deploy workflows."
    )


if __name__ == "__main__":
    main()
