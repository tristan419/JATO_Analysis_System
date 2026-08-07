#!/usr/bin/env python3
"""Validate that production-capable GitHub Actions jobs are main-only."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import re
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MAIN_REF = "refs/heads/main"
MAIN_REF_CONDITION = f"github.ref == '{MAIN_REF}'"
PRODUCTION_ENVIRONMENT = "production"
PRODUCTION_RELEASE_WORKFLOW = ".github/workflows/production-release.yml"
INTL_SYNC_WORKFLOW = ".github/workflows/sync-www-active-to-intl.yml"
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
PRODUCTION_RELEASE_DEPLOY_CONDITION = (
    MAIN_REF_CONDITION
)
PRODUCTION_PREPARE_CONDITION = (
    PRODUCTION_RELEASE_DEPLOY_CONDITION
    + " && inputs.release_mode == 'prepare-candidate'"
)
PRODUCTION_CONTROL_CONDITION = (
    MAIN_REF_CONDITION
    + " && inputs.release_mode != 'prepare-candidate'"
)
FORBIDDEN_CONTROL_BUILD_TOKENS = (
    "npm ci",
    "npm install",
    "npm run build",
    "Package backend release",
    "Upload complete release archive",
    "rsync ",
)

PRODUCTION_JOBS = {
    PRODUCTION_RELEASE_WORKFLOW: (
        "deploy_tencent",
        "control_fixed_release_v2",
    ),
    INTL_SYNC_WORKFLOW: ("sync_intl",),
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
)
PRODUCTION_RELEASE_MODE_GATED_JOBS = (
    "build_frontend",
    "deploy_tencent",
    "control_fixed_release_v2",
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


class UniqueKeyLoader(yaml.BaseLoader):
    """Keep BaseLoader string semantics while rejecting duplicate YAML keys."""


def _construct_unique_mapping(
    loader: UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise AssertionError(f"duplicate YAML key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_workflow(relative_path: str) -> Mapping[str, Any]:
    workflow_path = REPO_ROOT / relative_path
    payload = yaml.load(
        workflow_path.read_text(encoding="utf-8"),
        Loader=UniqueKeyLoader,
    )
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
    if relative_path == PRODUCTION_RELEASE_WORKFLOW:
        workflow = load_workflow(relative_path)
        job = get_job(workflow, relative_path, job_name)
        condition = unwrap_expression(job.get("if"))
        expected_conditions = {
            "deploy_tencent": PRODUCTION_PREPARE_CONDITION,
            "control_fixed_release_v2": PRODUCTION_CONTROL_CONDITION,
        }
        expected_condition = expected_conditions[job_name]
        if condition != expected_condition:
            raise AssertionError(
                f"{relative_path}:{job_name} must use the exact main-and-mode "
                f"gate; found {condition!r}"
            )
    else:
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
            if unwrap_expression(job.get("if")) == "false":
                continue
            if (relative_path, str(job_name)) not in registered_jobs:
                raise AssertionError(
                    f"{relative_path}:{job_name} uses the production environment "
                    "without a registered main-only contract"
                )


def assert_production_release_main_guards() -> None:
    for job_name in PRODUCTION_RELEASE_MAIN_ONLY_JOBS:
        assert_main_only_job(PRODUCTION_RELEASE_WORKFLOW, job_name)
    workflow = load_workflow(PRODUCTION_RELEASE_WORKFLOW)
    for job_name in PRODUCTION_RELEASE_MODE_GATED_JOBS:
        job = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, job_name)
        condition = unwrap_expression(job.get("if"))
        expected_conditions = {
            "build_frontend": PRODUCTION_PREPARE_CONDITION,
            "deploy_tencent": PRODUCTION_PREPARE_CONDITION,
            "control_fixed_release_v2": PRODUCTION_CONTROL_CONDITION,
        }
        expected_condition = expected_conditions[job_name]
        if condition != expected_condition:
            raise AssertionError(
                f"{PRODUCTION_RELEASE_WORKFLOW}:{job_name} must use the exact "
                f"main-and-mode gate; found {condition!r}"
            )
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


def assert_fixed_release_v2_workflow_contract() -> None:
    """Validate the small fixed Active/Candidate V2 production surface."""

    workflow = load_workflow(PRODUCTION_RELEASE_WORKFLOW)
    triggers = workflow.get("on")
    if not isinstance(triggers, Mapping) or set(triggers) != {"workflow_dispatch"}:
        raise AssertionError(
            "fixed V2 production release must only support workflow_dispatch"
        )
    dispatch = triggers.get("workflow_dispatch")
    inputs = dispatch.get("inputs") if isinstance(dispatch, Mapping) else None
    expected_inputs = {
        "release_mode",
        "target_commit_sha",
        "target_archive_sha256",
        "target_manifest_sha256",
        "confirm_control_operation",
        "bootstrap_full_upload",
    }
    if not isinstance(inputs, Mapping) or set(inputs) != expected_inputs:
        raise AssertionError("fixed V2 dispatch input set changed")
    release_mode = inputs.get("release_mode")
    if not isinstance(release_mode, Mapping) or {
        "required": release_mode.get("required"),
        "type": release_mode.get("type"),
        "default": release_mode.get("default"),
        "options": release_mode.get("options"),
    } != {
        "required": "true",
        "type": "choice",
        "default": "prepare-candidate",
        "options": [
            "prepare-candidate",
            "discard-candidate",
            "update-active",
            "rollback-active",
        ],
    }:
        raise AssertionError("fixed V2 release_mode choices changed")
    for input_name in (
        "target_commit_sha",
        "target_archive_sha256",
        "target_manifest_sha256",
    ):
        value = inputs.get(input_name)
        if not isinstance(value, Mapping) or {
            "required": value.get("required"),
            "type": value.get("type"),
        } != {"required": "false", "type": "string"}:
            raise AssertionError(f"fixed V2 {input_name} contract changed")
    for input_name in ("confirm_control_operation", "bootstrap_full_upload"):
        value = inputs.get(input_name)
        if not isinstance(value, Mapping) or {
            "required": value.get("required"),
            "type": value.get("type"),
            "default": value.get("default"),
        } != {"required": "true", "type": "boolean", "default": "false"}:
            raise AssertionError(f"fixed V2 {input_name} contract changed")

    if workflow.get("env", {}).get("RELEASE_MODE") != "${{ inputs.release_mode }}":
        raise AssertionError("fixed V2 RELEASE_MODE must come from the dispatch choice")
    permissions = workflow.get("permissions")
    if not isinstance(permissions, Mapping) or dict(permissions) != {
        "actions": "read",
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
    }:
        raise AssertionError("fixed V2 production permissions changed")
    concurrency = workflow.get("concurrency")
    if not isinstance(concurrency, Mapping) or dict(concurrency) != {
        "group": "production-release-main",
        "cancel-in-progress": "false",
    }:
        raise AssertionError("fixed V2 production lock contract changed")

    jobs = workflow.get("jobs")
    expected_jobs = {
        PRODUCTION_COORDINATION_JOB,
        "build_frontend",
        "deploy_tencent",
        "control_fixed_release_v2",
    }
    if not isinstance(jobs, Mapping) or set(jobs) != expected_jobs:
        raise AssertionError("fixed V2 production job surface changed")
    if next(iter(jobs), None) != PRODUCTION_COORDINATION_JOB:
        raise AssertionError("release coordination must remain the first job")

    guard = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, PRODUCTION_COORDINATION_JOB)
    if guard.get("needs") is not None or guard.get("environment") is not None:
        raise AssertionError("release coordination must run before production access")
    if unwrap_expression(guard.get("if")) != MAIN_REF_CONDITION:
        raise AssertionError("release coordination must be main-only")
    guard_steps = get_steps(
        guard,
        PRODUCTION_RELEASE_WORKFLOW,
        PRODUCTION_COORDINATION_JOB,
    )
    if [step.get("name") for step in guard_steps] != [
        "Checkout release coordination guard",
        "Validate unpublished release coordination",
        "Freeze release coordination plan",
    ]:
        raise AssertionError("fixed V2 release coordination steps changed")
    checkout_options = guard_steps[0].get("with")
    if not isinstance(checkout_options, Mapping) or {
        "ref": checkout_options.get("ref"),
        "persist-credentials": checkout_options.get("persist-credentials"),
    } != {"ref": "${{ github.sha }}", "persist-credentials": "false"}:
        raise AssertionError("release coordination must checkout the exact main SHA")
    guard_command = str(guard_steps[1].get("run") or "")
    for token in (
        RELEASE_COORDINATION_SCRIPT,
        "production",
        '--main-sha "$GITHUB_SHA"',
        '--operation "$RELEASE_MODE"',
        '--target-sha "$TARGET_COMMIT_SHA"',
        '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
        '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
        '--plan-output "$RUNNER_TEMP/release-coordination-plan.json"',
    ):
        if token not in guard_command:
            raise AssertionError(f"release coordination lost {token!r}")
    guard_env = guard_steps[1].get("env")
    expected_target_env = {
        "TARGET_COMMIT_SHA": "${{ inputs.target_commit_sha }}",
        "TARGET_ARCHIVE_SHA256": "${{ inputs.target_archive_sha256 }}",
        "TARGET_MANIFEST_SHA256": "${{ inputs.target_manifest_sha256 }}",
    }
    if not isinstance(guard_env, Mapping):
        raise AssertionError("release coordination target env is missing")
    for env_name, expected in expected_target_env.items():
        if guard_env.get(env_name) != expected:
            raise AssertionError(
                f"release coordination {env_name} input binding changed"
            )
    if "secrets." in str(guard):
        raise AssertionError("release coordination cannot consume production secrets")
    freeze_options = guard_steps[2].get("with")
    if guard_steps[2].get("uses") != "actions/upload-artifact@v4" or not isinstance(
        freeze_options,
        Mapping,
    ):
        raise AssertionError("release coordination plan must be immutable artifact v4")
    for key, expected in {
        "name": "release-coordination-plan-${{ github.sha }}-${{ github.run_attempt }}",
        "path": "${{ runner.temp }}/release-coordination-plan.json",
        "if-no-files-found": "error",
        "compression-level": "0",
        "overwrite": "false",
        "retention-days": "7",
    }.items():
        if freeze_options.get(key) != expected:
            raise AssertionError(f"coordination artifact {key} changed")

    build = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, "build_frontend")
    deploy = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, "deploy_tencent")
    control = get_job(
        workflow,
        PRODUCTION_RELEASE_WORKFLOW,
        "control_fixed_release_v2",
    )
    if build.get("needs") != PRODUCTION_COORDINATION_JOB:
        raise AssertionError("frontend build must wait for release coordination")
    if unwrap_expression(build.get("if")) != PRODUCTION_PREPARE_CONDITION:
        raise AssertionError("frontend build must be prepare-candidate only")
    if deploy.get("needs") != [PRODUCTION_COORDINATION_JOB, "build_frontend"]:
        raise AssertionError("Candidate preparation must use the one frontend build")
    if unwrap_expression(deploy.get("if")) != PRODUCTION_PREPARE_CONDITION:
        raise AssertionError("Tencent Candidate preparation gate changed")
    if get_environment_name(deploy) != PRODUCTION_ENVIRONMENT:
        raise AssertionError("Tencent Candidate preparation must use production")
    if control.get("needs") != PRODUCTION_COORDINATION_JOB:
        raise AssertionError("fixed V2 control must wait for release coordination")
    if unwrap_expression(control.get("if")) != PRODUCTION_CONTROL_CONDITION:
        raise AssertionError("fixed V2 control main/mode gate changed")
    if get_environment_name(control) != PRODUCTION_ENVIRONMENT:
        raise AssertionError("fixed V2 control must use production")
    expected_opening_steps = [
        "Checkout release source",
        "Download frozen release coordination plan",
        "Revalidate frozen coordination plan after approval",
    ]
    deploy_steps = get_steps(deploy, PRODUCTION_RELEASE_WORKFLOW, "deploy_tencent")
    if [step.get("name") for step in deploy_steps[:3]] != expected_opening_steps:
        raise AssertionError("Candidate preparation must revalidate before secrets")
    control_steps = get_steps(
        control,
        PRODUCTION_RELEASE_WORKFLOW,
        "control_fixed_release_v2",
    )
    if [step.get("name") for step in control_steps[:3]] != [
        "Checkout V2 control source",
        *expected_opening_steps[1:],
    ]:
        raise AssertionError("fixed V2 control must revalidate before secrets")
    for label, guarded_steps in (
        ("prepare", deploy_steps),
        ("control", control_steps),
    ):
        checkout = guarded_steps[0].get("with")
        if not isinstance(checkout, Mapping) or {
            "ref": checkout.get("ref"),
            "persist-credentials": checkout.get("persist-credentials"),
        } != {"ref": "${{ github.sha }}", "persist-credentials": "false"}:
            raise AssertionError(f"{label} must checkout the exact approved SHA")
        verify = str(guarded_steps[2].get("run") or "")
        for token in (
            RELEASE_COORDINATION_SCRIPT,
            "verify-plan",
            '--main-sha "$GITHUB_SHA"',
            '--operation "$RELEASE_MODE"',
            '--target-sha "$TARGET_COMMIT_SHA"',
            '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
            '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
        ):
            if token not in verify:
                raise AssertionError(f"{label} lost frozen plan {token!r}")
        verify_env = guarded_steps[2].get("env")
        if not isinstance(verify_env, Mapping):
            raise AssertionError(f"{label} target env is missing")
        for env_name, expected in expected_target_env.items():
            if verify_env.get(env_name) != expected:
                raise AssertionError(f"{label} {env_name} input binding changed")
        if "secrets." in str(guarded_steps[:3]):
            raise AssertionError(f"{label} consumes secrets before frozen-plan check")

    prepare_names = [str(step.get("name") or "") for step in deploy_steps]
    required_prepare_steps = [
        "Package backend release with verified frontend artifact",
        "Upload complete release archive with incremental rsync",
        "Generate canonical V2 release manifest",
        "Reconfirm current main before first production mutation",
        "Deploy verified release to fixed Candidate on Tencent",
        "Retain V2 operation diagnostics",
    ]
    positions = [prepare_names.index(name) for name in required_prepare_steps]
    if positions != sorted(positions):
        raise AssertionError("fixed V2 Candidate preparation steps are out of order")
    reconfirm_step = deploy_steps[prepare_names.index(required_prepare_steps[3])]
    reconfirm_command = str(reconfirm_step.get("run") or "")
    for token in (
        RELEASE_COORDINATION_SCRIPT,
        "verify-plan",
        '--main-sha "$GITHUB_SHA"',
        '--operation "$RELEASE_MODE"',
        '--target-sha "$TARGET_COMMIT_SHA"',
        '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
        '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
    ):
        if token not in reconfirm_command:
            raise AssertionError(f"prepare reconfirm lost {token!r}")
    reconfirm_env = reconfirm_step.get("env")
    if not isinstance(reconfirm_env, Mapping):
        raise AssertionError("prepare reconfirm target env is missing")
    for env_name, expected in expected_target_env.items():
        if reconfirm_env.get(env_name) != expected:
            raise AssertionError(f"prepare reconfirm {env_name} binding changed")
    manifest_step = deploy_steps[prepare_names.index(required_prepare_steps[2])]
    manifest_command = str(manifest_step.get("run") or "")
    for token in (
        "ReleaseIdentity",
        "ReleaseManifest",
        "canonical_manifest_bytes",
        "manifest_sha256",
        "release-v2-manifest.json",
        "manifest-sha256",
        "manifest-b64",
        "Candidate release identity",
        "Commit SHA",
        "Archive SHA-256",
        "Manifest SHA-256",
    ):
        if token not in manifest_command:
            raise AssertionError(f"canonical V2 manifest lost {token!r}")
    prepare_step = deploy_steps[prepare_names.index(required_prepare_steps[4])]
    prepare_command = str(prepare_step.get("run") or "")
    for token in (
        "write_remote_export DEPLOY_BRANCH main",
        "write_remote_export DEPLOY_RELEASE_SYSTEM fixed-v2",
        "RELEASE_V2_MANIFEST_B64",
        "RELEASE_V2_MANIFEST_SHA256",
        "03_Scripts/deploy/fullstack_remote_release.sh",
        "V2_OPERATION_REPORT_PATH=",
    ):
        if token not in prepare_command:
            raise AssertionError(f"fixed V2 Candidate handoff lost {token!r}")
    for forbidden in (
        "DEEPSEEK_API_KEY",
        "HERMES_SYNC_TOKEN",
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_OAUTH_PROXY_URL",
        "GOOGLE_OAUTH_RELAY_URL",
        "GOOGLE_OAUTH_RELAY_TOKEN",
        "MIHOMO_SUB_URL",
        "MIHOMO_DB_PATH",
        "DEPLOY_CERTBOT_EMAIL",
    ):
        if forbidden in str(deploy):
            raise AssertionError(
                f"fixed V2 prepare forwards an unused app setting: {forbidden!r}"
            )
    diagnostics = deploy_steps[prepare_names.index(required_prepare_steps[5])]
    if unwrap_expression(diagnostics.get("if")) != "always()":
        raise AssertionError("V2 prepare diagnostics must be retained on failure")
    if positions[-1] != len(deploy_steps) - 1:
        raise AssertionError("V2 prepare diagnostics must be the final step")

    control_names = [str(step.get("name") or "") for step in control_steps]
    intent = control_steps[control_names.index("Validate fixed V2 operation intent")]
    intent_command = str(intent.get("run") or "")
    for token in (
        '"$CONFIRM_CONTROL_OPERATION" = "true"',
        "update-active|rollback-active)",
        "discard-candidate)",
        '"$TARGET_COMMIT_SHA" =~ ^[0-9a-f]{40}$',
        '"$TARGET_ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$',
        '"$TARGET_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$',
    ):
        if token not in intent_command:
            raise AssertionError(f"fixed V2 control intent lost {token!r}")
    control_run = control_steps[
        control_names.index("Run fixed V2 control operation on Tencent")
    ]
    control_command = str(control_run.get("run") or "")
    for token in (
        "03_Scripts/deploy/fixed_release_v2_remote.sh",
        "control_bundle_files=(",
        "03_Scripts/deploy/fixed_release_v2.py",
        "03_Scripts/deploy/fixed_release_v2_remote.sh",
        "03_Scripts/deploy/release_v2_store.py",
        "03_Scripts/deploy/release_v2_admission.py",
        "03_Scripts/deploy/jato_quiescence_gate.py",
        "03_Scripts/deploy/validate_release_archive.py",
        "03_Scripts/deploy/nginx/jato_active_release_v2.conf",
        "03_Scripts/deploy/nginx/jato_candidate_preview_v2.conf",
        "03_Scripts/deploy/systemd/jato-candidate-preview.service",
        "03_Scripts/deploy/systemd/jato-fullstack-backend@.service",
        "20-candidate-readonly.conf",
        "--sort=name",
        "gzip -n -f",
        "V2_CONTROL_BUNDLE_SHA256",
        "V2_CONTROL_BUNDLE_B64",
        "V2_CONTROL_BUNDLE_MEMBERS_B64",
        "mktemp -d /run/jato-v2-control.XXXXXX",
        'sha256sum "$v2_control_archive"',
        'tar -tzf "$v2_control_archive"',
        'cmp -s "$v2_expected_members" "$v2_actual_members"',
        "tar --same-permissions --no-overwrite-dir",
        'export V2_CONTROL_ROOT="$v2_control_root"',
        'export V2_CONTROLLER_PATH="${v2_control_root}/03_Scripts/deploy/'
        'fixed_release_v2.py"',
        'v2_remote_entry="${v2_control_root}/03_Scripts/deploy/'
        'fixed_release_v2_remote.sh"',
        'bash "$v2_remote_entry" "$@"',
        "trap v2_control_cleanup EXIT",
        "Fixed V2 control identity",
        "Current main SHA",
        "Control bundle SHA-256",
        'ssh_home="$HOME"',
        'archive_cache_root="$ssh_home/.cache/jato-releases/archives"',
        'production_lock="$ssh_home/.local/state/jato-production-release/'
        'production-deploy.lock"',
        'PRODUCTION_LOCK_PATH="$production_lock"',
        'V2_ARCHIVE_CACHE_ROOT="$archive_cache_root"',
        'bash -s %q',
        "V2_OPERATION_REPORT_PATH=",
    ):
        if token not in control_command:
            raise AssertionError(f"fixed V2 control handoff lost {token!r}")
    for forbidden in FORBIDDEN_CONTROL_BUILD_TOKENS:
        if forbidden in str(control):
            raise AssertionError(f"fixed V2 control must not rebuild: {forbidden!r}")
    if "cat 03_Scripts/deploy/fixed_release_v2_remote.sh" in control_command:
        raise AssertionError("fixed V2 remote entry must come from the hashed bundle")
    control_diagnostics = control_steps[
        control_names.index("Retain fixed V2 control diagnostics")
    ]
    if unwrap_expression(control_diagnostics.get("if")) != "always()":
        raise AssertionError("V2 control diagnostics must be retained on failure")

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


def assert_fixed_release_v2_database_gate_is_read_only() -> None:
    workflow = (REPO_ROOT / PRODUCTION_RELEASE_WORKFLOW).read_text(encoding="utf-8")
    remote_release = (
        REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
    ).read_text(encoding="utf-8")
    controller = (
        REPO_ROOT / "03_Scripts/deploy/fixed_release_v2.py"
    ).read_text(encoding="utf-8")
    admission = (
        REPO_ROOT / "03_Scripts/deploy/release_v2_admission.py"
    ).read_text(encoding="utf-8")

    for token in (
        "03_Scripts/deploy/fullstack_remote_release.sh",
        "write_remote_export DEPLOY_BRANCH main",
        "write_remote_export DEPLOY_RELEASE_SYSTEM fixed-v2",
    ):
        if token not in workflow:
            raise AssertionError(f"fixed V2 workflow lost {token!r}")
    for token in (
        'DEPLOY_RELEASE_SYSTEM="${DEPLOY_RELEASE_SYSTEM:-fixed-v2}"',
        'if [[ "$DEPLOY_RELEASE_SYSTEM" != "fixed-v2" ]]; then',
        "Direct legacy-v1 release entry is disabled",
    ):
        if token not in remote_release:
            raise AssertionError(f"direct legacy-v1 disablement lost {token!r}")
    fixed_function_start = remote_release.index("prepare_fixed_v2_release() {")
    fixed_branch = remote_release.index(
        'if [[ "$DEPLOY_RELEASE_SYSTEM" == "fixed-v2" ]]; then',
        fixed_function_start,
    )
    legacy_seal = remote_release.index("\nseal_release_archive_input\n")
    legacy_checkpoint = remote_release.index("checkpoint_identity_args=(")
    if not fixed_function_start < fixed_branch < legacy_seal < legacy_checkpoint:
        raise AssertionError(
            "fixed V2 must exit before legacy sealed inputs and checkpoints"
        )
    fixed_handoff = remote_release[fixed_function_start:fixed_branch]
    for token in (
        "install_trusted_archive_validator",
        "validate_release_archive.py=$TRUSTED_ARCHIVE_VALIDATOR_TEMP",
        "--headroom-target \"$RELEASE_WORKTREE\" 1",
        "tar --same-permissions --no-overwrite-dir",
        "validate_required_release_content",
        "materialize_verified_frontend",
        "RELEASE_V2_MANIFEST_B64",
        "RELEASE_V2_MANIFEST_SHA256",
        "fixed_release_v2_remote.sh",
        'PRODUCTION_LOCK_PATH="$DEPLOY_LOCK_PATH"',
        'V2_ARCHIVE_CACHE_ROOT="$ARCHIVE_ROOT_REAL"',
        "bash \"$v2_remote_entry\" prepare-candidate",
    ):
        if token not in fixed_handoff:
            raise AssertionError(f"fixed V2 remote handoff lost {token!r}")
    for forbidden in (
        "release_checkpoint.py",
        "release_evidence.py",
        "tencent_bluegreen_release.sh",
        "jato_release_storage_guard.py",
        "MSRP_EVIDENCE_ROOT",
        "PRODUCTION_EXTRACTION_RESERVE_BYTES",
        "SEALED_ARCHIVE",
        "exec 9>&-",
    ):
        if forbidden in fixed_handoff:
            raise AssertionError(
                f"fixed V2 handoff retained a legacy dependency: {forbidden!r}"
            )
    fixed_remote = (
        REPO_ROOT / "03_Scripts/deploy/fixed_release_v2_remote.sh"
    ).read_text(encoding="utf-8")
    for token in (
        '[[ "$V2_ARCHIVE_CACHE_ROOT" == /* ]]',
        "require_archive_cache_root",
        '--archive-cache-root "$V2_ARCHIVE_CACHE_ROOT"',
    ):
        if token not in fixed_remote:
            raise AssertionError(
                f"fixed V2 archive-cache handoff lost {token!r}"
            )
    if fixed_remote.count('--archive-cache-root "$V2_ARCHIVE_CACHE_ROOT"') != 3:
        raise AssertionError(
            "all four fixed V2 operations must receive the archive cache root"
        )
    required_content_start = remote_release.index(
        'if [[ "$DEPLOY_RELEASE_SYSTEM" == "fixed-v2" ]]; then',
        remote_release.index("validate_required_release_content() {"),
    )
    required_content_end = remote_release.index(
        "\n  else\n",
        required_content_start,
    )
    fixed_required_content = remote_release[
        required_content_start:required_content_end
    ]
    for forbidden in (
        "release_checkpoint.py",
        "release_evidence.py",
        "tencent_bluegreen_release.sh",
        "jato_release_storage_guard.py",
        "VOC_Nordic_SUV_Users_100.xlsx",
        "msrp_backfill",
    ):
        if forbidden in fixed_required_content:
            raise AssertionError(
                f"fixed V2 required-file list retained {forbidden!r}"
            )

    for token in (
        '"-m", "alembic", "current"',
        '"-m", "alembic", "heads"',
        '"PGOPTIONS": "-c default_transaction_read_only=on"',
        '"readOnly": True',
    ):
        if token not in admission:
            raise AssertionError(f"fixed V2 database admission lost {token!r}")
    for forbidden in (
        "alembic upgrade",
        "alembic downgrade",
        '"upgrade"',
        '"downgrade"',
    ):
        if forbidden in admission:
            raise AssertionError(
                f"fixed V2 database admission gained mutation {forbidden!r}"
            )

    update_start = controller.index("    def update_active(")
    rollback_start = controller.index("    def rollback_active(")
    update_body = controller[update_start:rollback_start]
    switch_position = update_body.index("self._switch_active(")
    if update_body.index("self._database_gate(") >= switch_position:
        raise AssertionError("database compatibility must pass before Active update")
    rollback_body = controller[rollback_start:]
    if rollback_body.index("self._database_gate(") >= rollback_body.index(
        "self._switch_active("
    ):
        raise AssertionError("database compatibility must pass before rollback")


def assert_feature_canary_cannot_route_or_mutate_production() -> None:
    controller_path = (
        REPO_ROOT / "03_Scripts/deploy/tencent_feature_candidate_canary.sh"
    )
    guard_path = (
        REPO_ROOT / "03_Scripts/deploy/jato_feature_canary_guard.py"
    )
    archive_validator_path = (
        REPO_ROOT / "03_Scripts/deploy/validate_release_archive.py"
    )
    if (
        not controller_path.is_file()
        or not guard_path.is_file()
        or not archive_validator_path.is_file()
    ):
        raise AssertionError("Tencent feature canary controller/guard is missing")
    controller = controller_path.read_text(encoding="utf-8")
    guard = guard_path.read_text(encoding="utf-8")
    archive_validator = archive_validator_path.read_text(encoding="utf-8")

    def shell_function(name: str) -> str:
        match = re.search(
            rf"(?ms)^{re.escape(name)}\(\) \{{\n"
            rf"(.*?)(?=^[a-zA-Z_][a-zA-Z0-9_]*\(\) \{{|\Z)",
            controller,
        )
        if match is None:
            raise AssertionError(
                f"feature canary function {name!r} is missing",
            )
        return match.group(0)

    required = (
        'CANARY_MODE="${1:-launch}"',
        'CANARY_ROOT="${CANARY_ROOT:-/opt/jato-canary}"',
        'CANARY_STATE_ROOT="${CANARY_STATE_ROOT:-/var/lib/jato-canary}"',
        'CANARY_PORT="${CANARY_PORT:-18001}"',
        'CANARY_MEMORY_HIGH="${CANARY_MEMORY_HIGH:-3G}"',
        'CANARY_MEMORY_MAX="${CANARY_MEMORY_MAX:-4G}"',
        'CANARY_TASKS_MAX="${CANARY_TASKS_MAX:-512}"',
        'CANARY_DEPLOY_UID="${CANARY_DEPLOY_UID:-}"',
        'CANARY_DEPLOY_GID="${CANARY_DEPLOY_GID:-}"',
        "pin_canary_deploy_identity",
        "validate_canary_deploy_identity",
        "jato_acquire_production_mutation_lock",
        "canary requires the canonical production deploy state directory",
        'RUN_KEY="${CANARY_COMMIT_SHA:0:12}-${CANARY_RUN_ID}"',
        'RUNTIME_ROOT="$CANARY_ROOT/runtime/$RUN_KEY"',
        'SUPERVISOR_UNIT="jato-feature-canary-supervisor-$RUN_KEY.service"',
        'CONTROLLER_UNIT="jato-feature-canary-controller-$RUN_KEY.service"',
        'SERVICE_UNIT="jato-feature-canary-$RUN_KEY.service"',
        "canary run id already has durable state and cannot be reused",
        '--service-type=exec',
        '--property="Restart=on-failure"',
        '--property="StartLimitIntervalSec=0"',
        '--property="MemoryHigh=$CANARY_SUPERVISOR_MEMORY_HIGH"',
        '--property="MemoryMax=$CANARY_SUPERVISOR_MEMORY_MAX"',
        '--property="MemoryHigh=$CANARY_CONTROLLER_MEMORY_HIGH"',
        '--property="MemoryMax=$CANARY_CONTROLLER_MEMORY_MAX"',
        '--property="SendSIGKILL=yes"',
        '--property="DynamicUser=yes"',
        '--property="ProtectSystem=strict"',
        '--property="ProtectHome=yes"',
        '--property="MemorySwapMax=0"',
        (
            '--property="InaccessiblePaths=$REFERENCE_ROOT '
            '$LEGACY_ROOT/01_RAW_DATA $LEGACY_ROOT/04_Processed_data '
            '/etc/jato-fullstack"'
        ),
        '--property="ReadWritePaths=$RUNTIME_ROOT"',
        '--setenv="APP_REDIS_ENABLED=false"',
        '--setenv="APP_JATO_MONTHLY_ENABLED=false"',
        '--setenv="APP_JATO_MONTHLY_EXECUTION_MODE=disabled"',
        '--setenv="APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=false"',
        '--setenv="APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED=false"',
        '--setenv="APP_METADATA_PREWARM_ENABLED=false"',
        '--setenv="APP_ADVANCED_ANALYSIS_WARMUP_ENABLED=false"',
        'sudo -n systemctl stop "$unit"',
        "refusing to stop a unit without exact canary identity",
        "InvocationID",
        "len(worker_pids) != 2",
        "only supervisor reconciliation may write a terminal canary receipt",
        "--terminal-writer supervisor_reconcile",
        "--writer-invocation-id",
        "ensure_checkpoint_marker",
        "controller_completed",
        "controller_unit_started",
        "fault_observed",
        "supervisor_started",
        "verify_existing_receipt",
        "run_canary_supervisor",
        "run_canary_controller_unit",
        "quiesce_canary_controller_unit",
        "assert_controller_scope",
        "assert_supervisor_generation",
        "assert_staged_supervisor_generation",
        "assert_reconcile_supervisor_generation",
        "capture_supervisor_invocation_id",
        "authorize_candidate_runtime",
        "wait_for_candidate_start_permit",
        "persist_candidate_start_permit",
        "candidateInvocationId",
        "startPermit",
        "assert_supervisor_production_lock",
        "StopPropagatedFrom",
        "verify_retained_control_bundle",
        "verify_canary_parent_roots",
        "reconcile_canary_controller",
        'REFERENCE_ROOT="$CANARY_ROOT/runtime/$RUN_KEY.reference"',
        "validate_canary_archive",
        "validate_release_archive.py",
        "cleanup_toolkit_egg_info.py",
        "verify_release_source_seal.py",
        "tar --same-permissions --no-overwrite-dir",
        '--property="UMask=0022"',
        'sudo -n install -d -m 0700 -o root -g root "$REFERENCE_ROOT"',
        '-o "$CANARY_DEPLOY_UID" -g "$CANARY_DEPLOY_GID" "$RUNTIME_ROOT"',
        'install -m 0440 -o root -g "$CANARY_DEPLOY_GID"',
        "prepare_trusted_materialization",
        "verify_trusted_candidate_integrity",
        "REFERENCE_INTEGRITY_ANCHOR_FILE",
        "cleanup_pre_supervisor_launch",
        "privateMaterialization",
        "sourceSealRuntimeVerification",
        "evidenceSchemaVersion",
    )
    missing = [token for token in required if token not in controller]
    if missing:
        raise AssertionError(
            f"feature canary safety contract is incomplete: {missing}",
        )
    forbidden = (
        "install_jato_fullstack_nginx.sh",
        "enable_jato_fullstack_https.sh",
        "systemctl reload nginx",
        "systemctl restart nginx",
        "systemctl daemon-reload",
        "tencent_bluegreen_release.sh",
        "jato_release_storage_guard.py",
        "release_checkpoint.py",
        'sudo -n systemctl stop "$ACTIVE_UNIT"',
        "pause_schedulers",
    )
    present = [token for token in forbidden if token in controller]
    if present:
        raise AssertionError(
            f"feature canary gained a production mutation primitive: {present}",
        )
    if "\n    --scope \\" in controller:
        raise AssertionError(
            "feature canary build must use a filesystem-sandboxed service, not a scope",
        )
    if "chmod -R a-w" in controller:
        raise AssertionError(
            "feature canary must not mutate source-sealed archive modes",
        )
    for validator_token in (
        "SCHEMA_VERSION = 2",
        "member.uid != 0",
        "member.gid != 0",
        "member.pax_headers",
        "release archive member lacks an explicit directory",
        '"privateEntries"',
        "privateModeEvidence",
        "final_sha256 != actual_sha256",
    ):
        if validator_token not in archive_validator:
            raise AssertionError(
                "shared release archive validator lost a fail-closed guard: "
                f"{validator_token}",
            )

    pin_identity_body = shell_function("pin_canary_deploy_identity")
    for token in (
        '[[ "$CANARY_MODE" != "launch" ]]',
        'current_uid="$(id -u)"',
        'current_gid="$(id -g)"',
        '[[ ! "$current_uid" =~ ^[1-9][0-9]*$ ]]',
        '[[ -n "$CANARY_DEPLOY_UID" && "$CANARY_DEPLOY_UID" != "$current_uid" ]]',
        'CANARY_DEPLOY_UID="$current_uid"',
        'CANARY_DEPLOY_GID="$current_gid"',
        "export CANARY_DEPLOY_UID CANARY_DEPLOY_GID",
    ):
        if token not in pin_identity_body:
            raise AssertionError(
                "feature canary launch lost its one-time non-root deploy "
                f"identity pin: {token}",
            )
    validate_identity_body = shell_function("validate_canary_deploy_identity")
    for token in (
        '[[ ! "$CANARY_DEPLOY_UID" =~ ^[1-9][0-9]*$ ]]',
        'current_uid="$(id -u)"',
        'current_gid="$(id -g)"',
        "runtime)",
        '[[ "$current_uid" == "$CANARY_DEPLOY_UID" ]]',
        '[[ "$current_gid" == "$CANARY_DEPLOY_GID" ]]',
        "launch|supervisor|controller|build|reconcile)",
        '[[ "$current_uid" != "$CANARY_DEPLOY_UID" ]]',
        '[[ "$current_gid" != "$CANARY_DEPLOY_GID" ]]',
    ):
        if token not in validate_identity_body:
            raise AssertionError(
                "feature canary deploy identity validation lost its "
                f"control/DynamicUser split: {token}",
            )
    identity_oracle_bodies = pin_identity_body + validate_identity_body
    for token in ("$(id -u)", "$(id -g)"):
        if (
            controller.count(token) != identity_oracle_bodies.count(token)
            or identity_oracle_bodies.count(token) != 2
        ):
            raise AssertionError(
                "feature canary may consult process-local uid/gid only while "
                f"pinning or validating the deploy identity: {token}",
            )
    for contract_name in (
        "validate_static_contract",
        "validate_reconcile_contract",
    ):
        if (
            "validate_canary_deploy_identity"
            not in shell_function(contract_name)
        ):
            raise AssertionError(
                f"{contract_name} lost its pinned deploy identity gate",
            )

    launcher_body = shell_function("start_canary_supervisor")
    for forbidden_launcher_token in ("--wait", "--pipe", "--scope"):
        if forbidden_launcher_token in launcher_body:
            raise AssertionError(
                "durable feature canary launcher must return after systemd "
                f"acceptance; found {forbidden_launcher_token}",
            )
    if '"$bash_bin" "$CONTROL_SCRIPT" supervisor' not in launcher_body:
        raise AssertionError(
            "durable feature canary launcher does not start the exact supervisor",
        )
    launch_body = shell_function("launch_canary")
    if launch_body.count("start_canary_supervisor") != 1:
        raise AssertionError(
            "feature canary launch path must invoke the detached supervisor exactly once",
        )
    launch_order = (
        "pin_canary_deploy_identity",
        "validate_static_contract",
        "verify_fresh_launch_namespace",
        "ensure_canary_roots",
        "stage_canary_inputs",
        "record_checkpoint initialized in_progress",
        "start_canary_supervisor",
    )
    launch_positions = [launch_body.index(token) for token in launch_order]
    if (
        launch_positions != sorted(launch_positions)
        or launch_body.count("cleanup_pre_supervisor_launch") != 4
        or launch_body.rindex("verify_supervisor_unit_absent")
        > launch_body.rindex("cleanup_pre_supervisor_launch")
    ):
        raise AssertionError(
            "feature canary launch must clean every unambiguous "
            "pre-supervisor failure without deleting an accepted supervisor",
        )
    for forbidden_launch_token in (
        "--wait",
        "--pipe",
        "acquire_canary_production_lock",
        "run_canary_controller",
        "run_build_scope",
        "start_candidate_service",
    ):
        if forbidden_launch_token in launch_body:
            raise AssertionError(
                "feature canary launch path became synchronous or entered business work: "
                f"{forbidden_launch_token}",
            )
    main_body = shell_function("main")
    for dispatch_token in (
        "launch)\n      launch_canary",
        "supervisor)",
        "run_canary_supervisor",
        "controller)",
        "run_canary_controller",
        "runtime)",
        'run_candidate_runtime "$@"',
        "reconcile)",
        "reconcile_canary_controller",
    ):
        if dispatch_token not in main_body:
            raise AssertionError(
                "feature canary main dispatch lost its durable mode routing: "
                f"{dispatch_token}",
            )

    build_body = shell_function("run_build_scope")
    if "--wait" not in build_body or "--pipe" not in build_body:
        raise AssertionError(
            "isolated build unit must remain synchronously supervised by the controller",
        )
    required_build_sandbox = (
        '--property="ProtectSystem=strict"',
        '--property="InaccessiblePaths=$REFERENCE_ROOT '
        '$LEGACY_ROOT/01_RAW_DATA $LEGACY_ROOT/04_Processed_data '
        '/etc/jato-fullstack"',
        '--property="ReadWritePaths=$RUNTIME_ROOT"',
    )
    missing_build_sandbox = [
        token for token in required_build_sandbox if token not in build_body
    ]
    if (
        missing_build_sandbox
        or build_body.count('--property="ReadWritePaths=') != 1
        or '--property="ReadWritePaths=$REFERENCE_ROOT' in build_body
    ):
        raise AssertionError(
            "candidate build must hide the root-owned reference and expose "
            "only the candidate runtime as writable: "
            f"{missing_build_sandbox}",
        )
    build_scope_assertion = shell_function("assert_build_scope")
    for token in (
        '[[ "$actual_write_paths" == *"$REFERENCE_ROOT"* ]]',
        '[[ "$actual_write_paths" != "$RUNTIME_ROOT" ]]',
        '[[ "$actual_inaccessible_paths" != *"$REFERENCE_ROOT"* ]]',
    ):
        if token not in build_scope_assertion:
            raise AssertionError(
                "candidate build scope no longer verifies its reference/"
                f"runtime mount policy: {token}",
            )
    runtime_body = shell_function("start_candidate_service")
    for child_body, child_name in (
        (build_body, "build"),
        (runtime_body, "runtime"),
    ):
        if '--property="Restart=no"' not in child_body:
            raise AssertionError(
                f"feature canary {child_name} must explicitly disable restart",
            )
        for relationship in ("StopPropagatedFrom", "After"):
            token = f'--property="{relationship}=$SUPERVISOR_UNIT"'
            if token not in child_body:
                raise AssertionError(
                    f"feature canary {child_name} omitted {relationship} "
                    "stop-only supervisor contract",
                )
        for forbidden_relationship in ("BindsTo", "PartOf"):
            token = f'--property="{forbidden_relationship}=$SUPERVISOR_UNIT"'
            if token in child_body:
                raise AssertionError(
                    f"feature canary {child_name} gained restart-propagating "
                    f"{forbidden_relationship}",
                )
    for child_body, expected_mode, forbidden_mode in (
        (build_body, "build", "runtime"),
        (runtime_body, "runtime", "build"),
    ):
        expected_token = f'--setenv="CANARY_MODE={expected_mode}"'
        forbidden_token = f'--setenv="CANARY_MODE={forbidden_mode}"'
        if child_body.count(expected_token) != 1 or forbidden_token in child_body:
            raise AssertionError(
                "feature canary child unit has ambiguous execution mode: "
                f"{expected_mode}",
            )

    controller_unit_body = shell_function("run_canary_controller_unit")
    for token in (
        "--wait",
        "--pipe",
        "--collect",
        '--property="RuntimeMaxSec=${controller_timeout}s"',
        '--property="TimeoutStopSec=${CANARY_CONTROLLER_RECOVERY_TIMEOUT_SECONDS}s"',
        '--property="KillMode=control-group"',
        '--property="SendSIGKILL=yes"',
        '--property="Restart=no"',
        '"$bash_bin" "$CONTROL_SCRIPT" controller',
    ):
        if token not in controller_unit_body:
            raise AssertionError(
                "durable controller unit lost its bounded systemd contract: "
                f"{token}",
            )
    for relationship in ("StopPropagatedFrom", "After"):
        token = f'--property="{relationship}=$SUPERVISOR_UNIT"'
        if token not in controller_unit_body:
            raise AssertionError(
                "durable controller unit omitted exact stop-only supervisor contract: "
                f"{relationship}",
            )
    for forbidden_relationship in ("BindsTo", "PartOf"):
        token = f'--property="{forbidden_relationship}=$SUPERVISOR_UNIT"'
        if token in controller_unit_body:
            raise AssertionError(
                "durable controller unit gained restart-propagating dependency: "
                f"{forbidden_relationship}",
            )

    control_environment_body = shell_function("build_canary_control_environment")
    for token in (
        '"CANARY_DEPLOY_UID=$CANARY_DEPLOY_UID"',
        '"CANARY_DEPLOY_GID=$CANARY_DEPLOY_GID"',
    ):
        if control_environment_body.count(token) != 1:
            raise AssertionError(
                "durable canary control environment must carry one exact "
                f"pinned identity value: {token}",
            )
    for unit_body, mode in (
        (launcher_body, "supervisor"),
        (controller_unit_body, "controller"),
    ):
        for token in (
            f"build_canary_control_environment {mode}",
            '--uid="$CANARY_DEPLOY_UID"',
            '--gid="$CANARY_DEPLOY_GID"',
            '"${environment_args[@]}"',
        ):
            if token not in unit_body:
                raise AssertionError(
                    f"durable {mode} unit lost pinned deploy identity "
                    f"propagation: {token}",
                )
    for child_body, child_name in (
        (build_body, "build"),
        (runtime_body, "runtime"),
    ):
        for token in (
            '--setenv="CANARY_DEPLOY_UID=$CANARY_DEPLOY_UID"',
            '--setenv="CANARY_DEPLOY_GID=$CANARY_DEPLOY_GID"',
        ):
            if child_body.count(token) != 1:
                raise AssertionError(
                    f"feature canary {child_name} unit must propagate one "
                    f"exact pinned identity value: {token}",
                )
    for token in (
        '--uid="$CANARY_DEPLOY_UID"',
        '--gid="$CANARY_DEPLOY_GID"',
    ):
        if token not in build_body:
            raise AssertionError(
                "feature canary build unit lost its pinned deploy execution "
                f"identity: {token}",
            )
    if (
        runtime_body.count('--property="DynamicUser=yes"') != 1
        or "--uid=" in runtime_body
        or "--gid=" in runtime_body
        or "SupplementaryGroups" in runtime_body
    ):
        raise AssertionError(
            "candidate runtime must remain a DynamicUser without a fixed uid, "
            "gid, or supplementary deploy group",
        )

    supervisor_body = shell_function("run_canary_supervisor")
    supervisor_order = (
        "acquire_canary_production_lock",
        "capture_supervisor_invocation_id",
        "record_checkpoint controller_unit_started in_progress",
        "run_canary_controller_unit",
        "quiesce_canary_controller_unit",
        'bash "$CONTROL_SCRIPT" reconcile',
    )
    supervisor_positions = [
        supervisor_body.index(token) for token in supervisor_order
    ]
    if supervisor_positions != sorted(supervisor_positions):
        raise AssertionError(
            "durable supervisor no longer holds the lock across controller and reconcile",
        )
    for token in (
        "business canary will not be rerun",
        "refusing concurrent reconciliation",
    ):
        if token not in supervisor_body:
            raise AssertionError(
                f"durable supervisor lost its recovery-only/quiescence contract: {token}",
            )

    run_body = shell_function("run_canary_controller")
    controller_order = (
        "assert_supervisor_generation",
        "assert_supervisor_scope",
        "assert_controller_scope",
        "assert_supervisor_production_lock",
        'capture_snapshot "$BEFORE_SNAPSHOT"',
        "run_build_scope",
    )
    controller_positions = [run_body.index(token) for token in controller_order]
    if (
        controller_positions != sorted(controller_positions)
        or "acquire_canary_production_lock" in run_body
    ):
        raise AssertionError(
            "isolated controller must prove the supervisor lock and snapshot "
            "production before its build",
        )
    candidate_order = (
        "prepare_trusted_materialization",
        "run_build_scope",
        "verify_trusted_candidate_integrity",
        "start_candidate_service",
        "authorize_candidate_runtime",
        "verify_candidate_service",
        'verify_trusted_candidate_integrity "$BUILD_INTEGRITY_EVIDENCE_FILE"',
        "persist_candidate_integrity_evidence",
        "record_checkpoint controller_completed completed",
    )
    candidate_positions = [run_body.index(token) for token in candidate_order]
    if (
        candidate_positions != sorted(candidate_positions)
        or run_body.count("verify_trusted_candidate_integrity") != 2
        or run_body.count("assert_supervisor_generation") < 3
    ):
        raise AssertionError(
            "isolated controller must create trusted materializations, verify "
            "them after build and runtime, and fence the exact waiting candidate",
        )
    candidate_service_body = shell_function("verify_candidate_service")
    if (
        "record_checkpoint candidate_verified completed"
        not in candidate_service_body
        or 'environment.get("CANARY_DEPLOY_UID") != deploy_uid'
        not in candidate_service_body
        or 'environment.get("CANARY_DEPLOY_GID") != deploy_gid'
        not in candidate_service_body
    ):
        raise AssertionError(
            "candidate verification must bind the pinned deploy identity and "
            "persist its terminal validation marker",
        )
    for token in (
        "def verify_ordered_checkpoint_markers(",
        '"source_anchored"',
        '"source_verified"',
        '"candidate_verified"',
        '"supervisor_reconciled"',
        "durable canary source, candidate and terminal markers",
    ):
        if token not in guard:
            raise AssertionError(
                "canary terminal receipt lost ordered source/candidate phases: "
                f"{token}",
            )

    trusted_materialization = shell_function("prepare_trusted_materialization")
    trusted_order = (
        'validate_canary_archive "$validation_receipt"',
        'sudo -n tar --same-owner --same-permissions --no-overwrite-dir',
        '-xzf "$CANARY_SOURCE_ARCHIVE" -C "$REFERENCE_ROOT"',
        'tar --same-permissions --no-overwrite-dir',
        '-xzf "$CANARY_SOURCE_ARCHIVE" -C "$RUNTIME_ROOT"',
        'sudo -n install -m 0444 -o root -g root "$validation_receipt"',
        'sudo -n python3 -B "$SOURCE_SEAL_HELPER" build',
        'sudo -n python3 -B "$SOURCE_SEAL_HELPER" verify',
        '--root "$RUNTIME_ROOT"',
        'sudo -n install -m 0444 -o root -g root "$anchor_temp"',
    )
    trusted_positions = [
        trusted_materialization.index(token)
        for token in trusted_order
    ]
    if trusted_positions != sorted(trusted_positions):
        raise AssertionError(
            "trusted canary materialization must validate, independently "
            "extract, seal and root-anchor its reference before feature code",
        )
    for token in (
        '"schemaVersion": 1',
        '"archiveValidationSha256"',
        '"sourceSealSha256"',
        '"0:0:444"',
    ):
        if token not in trusted_materialization:
            raise AssertionError(
                "trusted canary reference anchor lost root-owned evidence: "
                f"{token}",
            )
    for token in (
        "def _verify_candidate_build_evidence_v3(",
        ") -> tuple[int, int]:",
        'build.get("schemaVersion") != 3',
        'archive.get("schemaVersion") != 2',
        'reference_root != {"uid": 0, "gid": 0, "mode": "0700"}',
        'candidate_root["gid"] <= 0',
        'return candidate_root["uid"], candidate_root["gid"]',
        "deploy_uid, deploy_gid = _verify_candidate_build_evidence_v3(",
        'runtime_deploy_uid = environment.get("CANARY_DEPLOY_UID")',
        'runtime_deploy_gid = environment.get("CANARY_DEPLOY_GID")',
        're.fullmatch(r"[1-9][0-9]*", runtime_deploy_uid)',
        "int(runtime_deploy_uid) != deploy_uid",
        "int(runtime_deploy_gid) != deploy_gid",
        "candidate evidence has an invalid root-owned reference anchor",
    ):
        if token not in guard:
            raise AssertionError(
                "canary guard lost schema-v3 root-owned reference "
                f"attestation: {token}",
            )

    authorize_body = shell_function("authorize_candidate_runtime")
    for token in (
        'systemctl show "$SERVICE_UNIT"',
        'Path("/proc")',
        "/ main_pid",
        '.read_bytes().split(b"\\0")',
        "pre-permit candidate wrapper already changed or escaped",
        "StopPropagatedFrom",
        "BindsTo",
        "PartOf",
        '"CANARY_DEPLOY_UID": deploy_uid',
        '"CANARY_DEPLOY_GID": deploy_gid',
        "assert_supervisor_generation",
        'persist_candidate_start_permit "$candidate_invocation_id"',
    ):
        if token not in authorize_body:
            raise AssertionError(
                "candidate pre-permit authorization lost an exact identity "
                f"check: {token}",
            )

    permit_body = shell_function("persist_candidate_start_permit")
    install_position = permit_body.index('install -m 0444 -o root -g root')
    publish_position = permit_body.index('mv -T')
    verify_position = permit_body.index(
        'assert_candidate_start_permit "$candidate_invocation_id"',
    )
    live_positions = [
        match.start()
        for match in re.finditer(
            r"assert_supervisor_generation",
            permit_body,
        )
    ]
    if (
        len(live_positions) < 3
        or not (
            live_positions[0]
            < install_position
            < live_positions[1]
            < publish_position
            < verify_position
            < live_positions[2]
        )
        or "/dev/stdin" in permit_body
    ):
        raise AssertionError(
            "candidate start permit must use a regular source, repeated live "
            "generation checks and same-directory atomic publication",
        )

    candidate_runtime_body = shell_function("run_candidate_runtime")
    runtime_order = (
        "validate_canary_deploy_identity",
        "verify_canary_parent_roots",
        "validate_feature_identity",
        "assert_staged_supervisor_generation",
        "\n  expected_argv=(\n",
        "wait_for_candidate_start_permit",
        'exec "${expected_argv[@]}"',
    )
    runtime_positions = [
        candidate_runtime_body.index(token) for token in runtime_order
    ]
    if runtime_positions != sorted(runtime_positions):
        raise AssertionError(
            "candidate runtime must verify immutable identity, exact argv and "
            "its root-owned start permit before executing Uvicorn",
        )
    for forbidden_runtime_token in (
        "assert_supervisor_generation",
        "read_live_supervisor_invocation_id",
        "systemctl",
    ):
        if forbidden_runtime_token in candidate_runtime_body:
            raise AssertionError(
                "DynamicUser candidate runtime regained a host D-Bus "
                f"dependency: {forbidden_runtime_token}",
            )
    if "assert_supervisor_generation" not in main_body.split(
        "build)", 1
    )[1].split(";;", 1)[0]:
        raise AssertionError(
            "candidate build must prove its original supervisor generation "
            "before archive extraction or dependency installation",
        )

    cleanup_body = shell_function("cleanup_candidate")
    if (
        "CANARY_SERVICE_START_ATTEMPTED" in cleanup_body
        or "CANARY_BUILD_START_ATTEMPTED" in cleanup_body
        or "verify_retained_control_bundle" in cleanup_body
    ):
        raise AssertionError(
            "durable cleanup must discover exact child units and retain its "
            "control bundle until after the receipt",
        )
    stop_positions = [
        match.start()
        for match in re.finditer(
            r"stop_verified_transient_unit",
            cleanup_body,
        )
    ]
    first_remove = cleanup_body.find("sudo -n rm")
    if (
        len(stop_positions) != 2
        or first_remove < 0
        or any(position > first_remove for position in stop_positions)
    ):
        raise AssertionError(
            "candidate cleanup must stop both exact child units before deleting runtime/source",
        )
    pre_supervisor_cleanup = shell_function("cleanup_pre_supervisor_launch")
    for token in (
        "verify_canary_parent_roots",
        '"$REFERENCE_ROOT" "$RUNTIME_ROOT" "$CONTROL_ROOT"',
        'sudo -n rm -rf --one-file-system -- "$path"',
        '"$CANARY_ROOT/runtime/"*|"$CANARY_ROOT/control/"*',
        '"$CANARY_ROOT/sources/"*.tar.gz',
        "cleanup_canary_state_run",
        "pre-supervisor canary residue remained after cleanup",
    ):
        if token not in pre_supervisor_cleanup:
            raise AssertionError(
                "pre-supervisor cleanup lost its exact namespace or "
                f"postcondition: {token}",
            )
    if 'sudo -n rm -f -- "$path"' in pre_supervisor_cleanup:
        raise AssertionError(
            "pre-supervisor cleanup must not traverse deploy-owned state "
            "parents with path-based sudo rm",
        )
    state_root_verifier = shell_function("verify_canary_state_roots")
    for token in (
        'state_root != Path("/var/lib/jato-canary")',
        'Path("/var/lib"): (0, 0, None)',
        'state_root / "checkpoints"',
        'state_root / "receipts"',
        'state_root / "evidence"',
        'state_root / "snapshots"',
        "stat.S_ISLNK",
        "mode & 0o022",
    ):
        if token not in state_root_verifier:
            raise AssertionError(
                "canary state root verification lost a parent-chain guard: "
                f"{token}",
            )
    state_cleanup = shell_function("cleanup_canary_state_run")
    for token in (
        'sudo -n python3 -B "$CANARY_GUARD" cleanup-launch-state',
        '--state-root "$CANARY_STATE_ROOT"',
        '--run-key "$RUN_KEY"',
        '--expected-uid "$CANARY_DEPLOY_UID"',
        '--expected-gid "$CANARY_DEPLOY_GID"',
    ):
        if token not in state_cleanup:
            raise AssertionError(
                "canary state cleanup lost its dirfd/no-follow boundary: "
                f"{token}",
            )
    for token in (
        "def cleanup_launch_state(",
        'state_root.name != "jato-canary"',
        'getattr(os, "O_NOFOLLOW", 0)',
        'anchor_fd = open_directory(',
        'root_fd = open_directory(',
        '"checkpoints": open_directory(',
        '"receipts": open_directory(',
        '"evidence": open_directory(',
        '"snapshots": open_directory(',
        "follow_symlinks=False",
        "os.unlink(entry_name, dir_fd=directory_fd)",
        'arguments.state_root != Path("/var/lib/jato-canary")',
    ):
        if token not in guard:
            raise AssertionError(
                "canary state guard lost its dirfd/no-follow boundary: "
                f"{token}",
            )
    stop_body = shell_function("stop_verified_transient_unit")
    for token in (
        "-p InvocationID",
        "-p StopPropagatedFrom",
        "LoadState=not-found",
        "transient canary unit identity changed while waiting for collect",
        "transient canary unit was not collected",
    ):
        if token not in stop_body:
            raise AssertionError(
                "transient child cleanup lost its GC/name-reuse identity guard: "
                f"{token}",
            )
    if "ExecMainStartTimestampMonotonic" in stop_body:
        raise AssertionError(
            "transient child cleanup must use systemd InvocationID, not a "
            "potentially colliding start timestamp, for name-reuse detection",
        )
    reconcile_body = shell_function("reconcile_canary_controller")
    ordered_reconcile_tokens = (
        "assert_reconcile_supervisor_generation",
        "acquire_canary_production_lock",
        "cleanup_candidate",
        "wait_for_candidate_port_release",
        'capture_snapshot "$AFTER_SNAPSHOT"',
        "verify_retained_control_bundle",
        "ensure_checkpoint_marker supervisor_reconciled completed",
        "write_terminal_receipt",
        "verify_existing_receipt",
    )
    positions = [
        reconcile_body.rindex(token)
        for token in ordered_reconcile_tokens
    ]
    if positions != sorted(positions):
        raise AssertionError(
            "durable canary recovery no longer locks, cleans, proves production, "
            "verifies retained immutable control, then writes and verifies its "
            "terminal receipt in that order",
        )
    finalizer_body = shell_function("finalize_canary")
    terminal_writer_body = shell_function("write_terminal_receipt")
    if (
        '"$CANARY_GUARD" finalize' in finalizer_body
        or "write_terminal_receipt" in finalizer_body
        or controller.count('"$CANARY_GUARD" finalize') != 1
        or '"$CANARY_GUARD" finalize' not in terminal_writer_body
        or '[[ "$CANARY_MODE" != "reconcile" ]]' not in terminal_writer_body
        or "--terminal-writer supervisor_reconcile" not in terminal_writer_body
        or "--writer-invocation-id" not in terminal_writer_body
    ):
        raise AssertionError(
            "terminal canary receipts must be written exactly once and only by "
            "supervisor reconciliation",
        )
    retained_control_body = shell_function("verify_retained_control_bundle")
    if (
        "sudo -n rm" in retained_control_body
        or "rm -rf" in retained_control_body
        or "verify_canary_parent_roots" not in retained_control_body
    ):
        raise AssertionError(
            "restartable supervisor must retain its control bundle and verify "
            "the immutable parent chain",
        )
    ensure_roots_body = shell_function("ensure_canary_roots")
    if (
        'install -d -m 0755 -o root -g root "$path"' not in ensure_roots_body
        or "verify_canary_parent_roots" not in ensure_roots_body
    ):
        raise AssertionError(
            "canary executable/source parent roots must be root-owned and "
            "verified before detached launch",
        )

    for forbidden_override in (
        'RUNTIME_ROOT="${RUNTIME_ROOT:-',
        'CHECKPOINT_FILE="${CHECKPOINT_FILE:-',
        'RECEIPT_FILE="${RECEIPT_FILE:-',
        'SUPERVISOR_UNIT="${SUPERVISOR_UNIT:-',
        'CONTROLLER_UNIT="${CONTROLLER_UNIT:-',
        'BUILD_UNIT="${BUILD_UNIT:-',
        'SERVICE_UNIT="${SERVICE_UNIT:-',
        'CONTROL_ROOT="${CONTROL_ROOT:-',
        'CONTROL_SCRIPT="${CONTROL_SCRIPT:-',
        'STAGED_SOURCE_ARCHIVE="${STAGED_SOURCE_ARCHIVE:-',
    ):
        if forbidden_override in controller:
            raise AssertionError(
                "feature canary derived path/unit became environment-overridable: "
                f"{forbidden_override}",
            )
    for required_guard_token in (
        "/var/lib/jato-release/active-slot",
        "/opt/jato/active",
        "/etc/jato-fullstack/nginx/active-release.conf",
        "jato-monthly-worker.service",
        "EXPECTED_ACTIVE_MEMORY_HIGH",
        "EXPECTED_ACTIVE_MEMORY_MAX",
        "candidatePortFree",
        "candidatePortReferenced",
        "compare_snapshots",
        "verify_checkpoint_marker",
        "verify_receipt_payload",
        "verify-marker",
        "ensure-marker",
        "verify-receipt",
        "terminalWriter",
        "writerInvocationId",
        "different supervisor generations",
    ):
        if required_guard_token not in guard:
            raise AssertionError(
                "feature canary no longer records production invariant "
                f"{required_guard_token!r}",
            )


def main() -> None:
    assert_all_deploy_workflows_are_registered()
    assert_pull_request_release_coordination_guard()
    assert_fixed_release_v2_workflow_contract()

    for relative_path, job_names in PRODUCTION_JOBS.items():
        for job_name in job_names:
            assert_main_only_production_job(relative_path, job_name)

    assert_all_static_production_jobs_are_registered()
    assert_production_release_main_guards()

    for relative_path in MANUAL_DEPLOY_WORKFLOWS:
        for job_name in PRODUCTION_JOBS[relative_path]:
            assert_manual_deploy_is_skipped_for_non_main(relative_path, job_name)

    assert_country_news_production_write_is_main_only()
    assert_fixed_release_v2_database_gate_is_read_only()
    assert_feature_canary_cannot_route_or_mutate_production()
    print(
        "Validated fixed Active/Candidate V2 and main-only production gates for "
        f"{sum(len(job_names) for job_names in PRODUCTION_JOBS.values())} "
        "production jobs and "
        f"{len(MANUAL_DEPLOY_WORKFLOWS)} manual deploy workflows."
    )


if __name__ == "__main__":
    main()
