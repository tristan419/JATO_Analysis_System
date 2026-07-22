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
PRODUCTION_RELEASE_MAIN_ONLY_JOBS = ("build_frontend", "audit_frontend_parity")

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
    server_deploy = (
        REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
    ).read_text(encoding="utf-8")

    if "03_Scripts/deploy/fullstack_remote_release.sh" not in production_workflow:
        raise AssertionError("production release no longer invokes Tencent remote release")
    if "export DEPLOY_BRANCH=main" not in production_workflow:
        raise AssertionError("production release must pin Tencent DEPLOY_BRANCH=main")
    if "bash 03_Scripts/deploy_fullstack_server.sh" not in remote_release:
        raise AssertionError("remote release no longer invokes the guarded server deploy")
    if 'PRODUCTION_RELEASE_WORKFLOW="true"' not in remote_release:
        raise AssertionError("remote release must identify the production release workflow")
    if "python -m alembic upgrade head" not in server_deploy:
        raise AssertionError("expected Alembic production migration command was not found")
    if 'DEPLOY_BRANCH" != "main"' not in server_deploy:
        raise AssertionError("database migration must retain the main branch gate")
    if 'PRODUCTION_RELEASE_WORKFLOW" != "true"' not in server_deploy:
        raise AssertionError("database migration must require the production release workflow")


def main() -> None:
    assert_all_deploy_workflows_are_registered()

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
        "Validated main-only production gates for "
        f"{sum(len(job_names) for job_names in PRODUCTION_JOBS.values())} "
        "production jobs and "
        f"{len(MANUAL_DEPLOY_WORKFLOWS)} manual deploy workflows."
    )


if __name__ == "__main__":
    main()
