#!/usr/bin/env python3
"""Validate that production-capable GitHub Actions jobs are main-only."""

from __future__ import annotations

from collections.abc import Mapping
import importlib.util
from pathlib import Path
import re
import sys
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MAIN_REF = "refs/heads/main"
MAIN_REF_CONDITION = f"github.ref == '{MAIN_REF}'"
PRODUCTION_ENVIRONMENT = "production"
PRODUCTION_RELEASE_WORKFLOW = ".github/workflows/production-release.yml"
INTL_SYNC_WORKFLOW = ".github/workflows/sync-www-active-to-intl.yml"
CHECKPOINT_RECOVERY_WORKFLOW = (
    ".github/workflows/production-checkpoint-recovery.yml"
)
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
PRODUCTION_RELEASE_HOLD_SCRIPT = (
    ".github/scripts/production_release_hold.py"
)
PRODUCTION_RELEASE_HOLD_PATH = (
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue-production-hold.v1.json"
)
PRODUCTION_RELEASE_HOLD_RETIREMENT_PATH = (
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue-"
    "production-hold-retirement.v1.json"
)
PRODUCTION_RELEASE_RECOVERY_PLAN = (
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue.json"
)
PRODUCTION_RELEASE_DEPLOY_CONDITION = (
    MAIN_REF_CONDITION
    + " && needs.release_coordination_guard.outputs.release-action == 'deploy'"
)
PRODUCTION_PREPARE_CONDITION = (
    PRODUCTION_RELEASE_DEPLOY_CONDITION
    + " && (github.event_name != 'workflow_dispatch' || "
    + "inputs.release_mode == 'prepare-candidate')"
)
PRODUCTION_APPROVAL_CONDITION = (
    MAIN_REF_CONDITION
    + " && github.event_name == 'workflow_dispatch'"
    + " && inputs.release_mode == 'approve-candidate-to-active'"
    + " && needs.release_coordination_guard.outputs.release-action == 'deploy'"
)
PRODUCTION_CLEANUP_CONDITION = (
    MAIN_REF_CONDITION
    + " && github.event_name == 'workflow_dispatch'"
    + " && (inputs.release_mode == 'discard-candidate' || "
    + "inputs.release_mode == 'release-candidate')"
    + " && needs.release_coordination_guard.outputs.release-action == 'deploy'"
)
PRODUCTION_LEGACY_INTL_CONDITION = (
    PRODUCTION_RELEASE_DEPLOY_CONDITION
    + " && github.event_name == 'workflow_dispatch'"
    + " && inputs.release_mode == 'prepare-and-switch'"
)

PRODUCTION_JOBS = {
    PRODUCTION_RELEASE_WORKFLOW: (
        "deploy_tencent",
        "approve_candidate_to_active",
        "cleanup_candidate",
    ),
    CHECKPOINT_RECOVERY_WORKFLOW: ("recover_checkpoint",),
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
PRODUCTION_RELEASE_HOLD_GATED_JOBS = (
    "build_frontend",
    "deploy_tencent",
    "approve_candidate_to_active",
    "cleanup_candidate",
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
            "approve_candidate_to_active": PRODUCTION_APPROVAL_CONDITION,
            "cleanup_candidate": PRODUCTION_CLEANUP_CONDITION,
        }
        expected_condition = expected_conditions[job_name]
        if condition != expected_condition:
            raise AssertionError(
                f"{relative_path}:{job_name} must use the exact main-and-hold "
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
            if (relative_path, str(job_name)) not in registered_jobs:
                raise AssertionError(
                    f"{relative_path}:{job_name} uses the production environment "
                    "without a registered main-only contract"
                )


def assert_production_release_main_guards() -> None:
    for job_name in PRODUCTION_RELEASE_MAIN_ONLY_JOBS:
        assert_main_only_job(PRODUCTION_RELEASE_WORKFLOW, job_name)
    workflow = load_workflow(PRODUCTION_RELEASE_WORKFLOW)
    for job_name in PRODUCTION_RELEASE_HOLD_GATED_JOBS:
        job = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, job_name)
        condition = unwrap_expression(job.get("if"))
        expected_conditions = {
            "build_frontend": PRODUCTION_PREPARE_CONDITION,
            "deploy_tencent": PRODUCTION_PREPARE_CONDITION,
            "approve_candidate_to_active": PRODUCTION_APPROVAL_CONDITION,
            "cleanup_candidate": PRODUCTION_CLEANUP_CONDITION,
            "audit_frontend_parity": PRODUCTION_LEGACY_INTL_CONDITION,
        }
        expected_condition = expected_conditions[job_name]
        if condition != expected_condition:
            raise AssertionError(
                f"{PRODUCTION_RELEASE_WORKFLOW}:{job_name} must use the exact "
                f"main-and-hold gate; found {condition!r}"
            )


def assert_checkpoint_recovery_contract() -> None:
    workflow = load_workflow(CHECKPOINT_RECOVERY_WORKFLOW)
    if workflow.get("name") != "production-checkpoint-recovery":
        raise AssertionError("checkpoint recovery workflow name changed")
    workflow_jobs = workflow.get("jobs")
    expected_jobs = {
        "recovery_coordination_guard",
        "recover_checkpoint",
    }
    if not isinstance(workflow_jobs, Mapping) or set(workflow_jobs) != expected_jobs:
        raise AssertionError(
            "checkpoint recovery must contain exactly its guard and recovery jobs"
        )
    triggers = workflow.get("on")
    if not isinstance(triggers, Mapping) or set(triggers) != {"workflow_dispatch"}:
        raise AssertionError(
            "checkpoint recovery must only support an explicit workflow_dispatch"
        )
    dispatch = triggers.get("workflow_dispatch")
    if not isinstance(dispatch, Mapping):
        raise AssertionError("checkpoint recovery dispatch inputs are missing")
    inputs = dispatch.get("inputs")
    expected_inputs = {
        "mode",
        "confirmation",
        "reviewed_dry_run_run_id",
        "reviewed_dry_run_result_sha256",
        "reviewed_main_sha",
        "reviewed_plan_sha256",
    }
    if not isinstance(inputs, Mapping) or set(inputs) != expected_inputs:
        raise AssertionError(
            "checkpoint recovery dispatch input contract changed"
        )
    mode = inputs.get("mode")
    if not isinstance(mode, Mapping) or mode.get("default") != "dry-run":
        raise AssertionError("checkpoint recovery must default to dry-run")
    if mode.get("type") != "choice" or mode.get("options") != [
        "dry-run",
        "apply",
    ]:
        raise AssertionError("checkpoint recovery mode choices changed")
    if mode.get("required") != "true":
        raise AssertionError("checkpoint recovery mode must be required")
    confirmation = inputs.get("confirmation")
    if not isinstance(confirmation, Mapping) or {
        "required": confirmation.get("required"),
        "default": confirmation.get("default"),
        "type": confirmation.get("type"),
    } != {
        "required": "false",
        "default": "",
        "type": "string",
    }:
        raise AssertionError(
            "checkpoint recovery confirmation must be an optional empty string"
        )
    for name in sorted(expected_inputs - {"mode", "confirmation"}):
        reviewed_input = inputs.get(name)
        if not isinstance(reviewed_input, Mapping) or {
            "required": reviewed_input.get("required"),
            "default": reviewed_input.get("default"),
            "type": reviewed_input.get("type"),
        } != {
            "required": "false",
            "default": "",
            "type": "string",
        }:
            raise AssertionError(
                f"checkpoint recovery {name} must be an optional empty string"
            )

    concurrency = workflow.get("concurrency")
    if not isinstance(concurrency, Mapping) or concurrency != {
        "group": "production-release-main",
        "cancel-in-progress": "false",
    }:
        raise AssertionError(
            "checkpoint recovery must serialize with the main production release"
        )
    permissions = workflow.get("permissions")
    if not isinstance(permissions, Mapping) or permissions != {
        "actions": "read",
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
    }:
        raise AssertionError("checkpoint recovery permissions are not least-privilege")

    guard = assert_main_only_job(
        CHECKPOINT_RECOVERY_WORKFLOW,
        "recovery_coordination_guard",
    )
    if get_environment_name(guard):
        raise AssertionError("checkpoint recovery preflight must not use production")
    guard_steps = get_steps(
        guard,
        CHECKPOINT_RECOVERY_WORKFLOW,
        "recovery_coordination_guard",
    )
    expected_guard_steps = [
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
    if [str(step.get("name") or "") for step in guard_steps] != expected_guard_steps:
        raise AssertionError(
            "checkpoint recovery guard steps or ordering changed"
        )
    if "secrets." in str(guard_steps):
        raise AssertionError(
            "checkpoint recovery guard must not consume production secrets"
        )
    guard_checkout = guard_steps[0]
    checkout_with = guard_checkout.get("with")
    if (
        guard_checkout.get("uses") != "actions/checkout@v5"
        or not isinstance(checkout_with, Mapping)
        or checkout_with.get("ref") != "${{ github.sha }}"
        or checkout_with.get("persist-credentials") != "false"
    ):
        raise AssertionError(
            "checkpoint recovery guard must checkout the exact non-credentialed SHA"
        )
    guard_intent = str(guard_steps[1].get("run") or "")
    for required in (
        "case \"$RECOVERY_MODE\" in",
        "dry-run)",
        "apply)",
        (
            "QUARANTINE 29df5e6e667351f09305783932b34e5438d6a9d5 "
            "RESIDUE AND ABORT PRE-SWITCH"
        ),
        "REVIEWED_DRY_RUN_RUN_ID",
        "REVIEWED_DRY_RUN_RESULT_SHA256",
        "REVIEWED_MAIN_SHA",
        "REVIEWED_PLAN_SHA256",
        "test \"$REVIEWED_MAIN_SHA\" = \"$GITHUB_SHA\"",
        "2026-08-03-29df-pre-switch-candidate-residue.json",
    ):
        if required not in guard_intent:
            raise AssertionError(
                f"checkpoint recovery guard intent lost {required!r}"
            )
    apply_only_steps = guard_steps[2:6] + [guard_steps[9]]
    if any(step.get("if") != "${{ inputs.mode == 'apply' }}" for step in apply_only_steps):
        raise AssertionError(
            "reviewed dry-run fetch, validation, and freeze must be apply-only"
        )
    fetch_review = guard_steps[2]
    fetch_environment = fetch_review.get("env")
    fetch_command = str(fetch_review.get("run") or "")
    if (
        not isinstance(fetch_environment, Mapping)
        or fetch_environment.get("GH_TOKEN") != "${{ github.token }}"
        or "actions/runs/$REVIEWED_DRY_RUN_RUN_ID" not in fetch_command
        or "actions/runs/$REVIEWED_DRY_RUN_RUN_ID/artifacts?per_page=100"
        not in fetch_command
    ):
        raise AssertionError(
            "checkpoint recovery must fetch the exact reviewed run and artifacts"
        )
    resolve_review = guard_steps[3]
    if resolve_review.get("id") != "reviewed_dry_run":
        raise AssertionError("reviewed dry-run artifact resolver ID changed")
    resolve_command = str(resolve_review.get("run") or "")
    for required in (
        ".github/workflows/production-checkpoint-recovery.yml",
        'run.get("event") != "workflow_dispatch"',
        'run.get("head_branch") != "main"',
        'run.get("head_sha") != main_sha',
        'run.get("conclusion") != "success"',
        "checkpoint-recovery-result-{main_sha}-{run_id}-{run_attempt}",
        "artifact-id=",
        "run-attempt=",
        're.fullmatch(r"sha256:[0-9a-f]{64}", digest)',
    ):
        if required not in resolve_command:
            raise AssertionError(
                f"reviewed dry-run artifact resolver lost {required!r}"
            )
    reviewed_download = guard_steps[4]
    reviewed_download_with = reviewed_download.get("with")
    if (
        reviewed_download.get("uses") != "actions/download-artifact@v5"
        or not isinstance(reviewed_download_with, Mapping)
        or reviewed_download_with.get("artifact-ids")
        != "${{ steps.reviewed_dry_run.outputs.artifact-id }}"
        or reviewed_download_with.get("github-token") != "${{ github.token }}"
        or reviewed_download_with.get("repository")
        != "${{ github.repository }}"
        or reviewed_download_with.get("run-id")
        != "${{ inputs.reviewed_dry_run_run_id }}"
    ):
        raise AssertionError(
            "checkpoint recovery must download the reviewed artifact by immutable ID"
        )
    review_validation = str(guard_steps[5].get("run") or "")
    for required in (
        "reviewed_recovery_authorization.py freeze",
        "checkpoint-recovery-result.json",
        "--expected-result-sha256",
        "--expected-main-sha",
        "--expected-plan-sha256",
        "--repository",
        "--run-id",
        "--run-attempt",
        "--output-dir",
    ):
        if required not in review_validation:
            raise AssertionError(
                f"reviewed dry-run result validation lost {required!r}"
            )

    hold_validation = str(guard_steps[6].get("run") or "")
    for required in (
        PRODUCTION_RELEASE_HOLD_SCRIPT,
        "require-active",
        "PYTHONPATH=03_Scripts/deploy",
        "from pre_switch_checkpoint_recovery import load_recovery_plan",
        "hashlib.sha256(plan.read_bytes()).hexdigest()",
    ):
        if required not in hold_validation:
            raise AssertionError(
                f"checkpoint recovery preflight hold check lost {required!r}"
            )

    guard_coordination = str(guard_steps[7].get("run") or "")
    for required in (
        RELEASE_COORDINATION_SCRIPT,
        "production",
        '--main-sha "$GITHUB_SHA"',
        '--plan-output "$RUNNER_TEMP/recovery-coordination-plan.json"',
    ):
        if required not in guard_coordination:
            raise AssertionError(
                f"checkpoint recovery guard coordination lost {required!r}"
            )
    freeze = guard_steps[8]
    freeze_with = freeze.get("with")
    expected_frozen_artifact = {
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
    if (
        freeze.get("uses") != "actions/upload-artifact@v4"
        or not isinstance(freeze_with, Mapping)
        or dict(freeze_with) != expected_frozen_artifact
    ):
        raise AssertionError(
            "checkpoint recovery frozen coordination artifact contract changed"
        )
    evidence_freeze = guard_steps[9]
    evidence_freeze_with = evidence_freeze.get("with")
    if (
        evidence_freeze.get("uses") != "actions/upload-artifact@v4"
        or not isinstance(evidence_freeze_with, Mapping)
        or dict(evidence_freeze_with)
        != {
            "name": (
                "checkpoint-recovery-reviewed-dry-run-${{ github.sha }}-"
                "${{ github.run_id }}-${{ github.run_attempt }}"
            ),
            "path": "${{ runner.temp }}/reviewed-dry-run-frozen",
            "if-no-files-found": "error",
            "compression-level": "0",
            "overwrite": "false",
            "retention-days": "30",
        }
    ):
        raise AssertionError(
            "checkpoint recovery reviewed dry-run evidence artifact changed"
        )

    recovery = assert_main_only_job(
        CHECKPOINT_RECOVERY_WORKFLOW,
        "recover_checkpoint",
    )
    if get_environment_name(recovery) != PRODUCTION_ENVIRONMENT:
        raise AssertionError("checkpoint recovery apply job needs production approval")
    if recovery.get("needs") != "recovery_coordination_guard":
        raise AssertionError("checkpoint recovery must wait for its frozen plan")

    steps = get_steps(
        recovery,
        CHECKPOINT_RECOVERY_WORKFLOW,
        "recover_checkpoint",
    )
    step_names = [str(step.get("name") or "") for step in steps]
    expected_steps = [
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
    if step_names != expected_steps:
        raise AssertionError(
            "checkpoint recovery steps or ordering changed"
        )
    if "secrets." in str(steps[:8]):
        raise AssertionError(
            "checkpoint recovery must validate approved intent before reading secrets"
        )
    recovery_checkout = steps[0]
    recovery_checkout_with = recovery_checkout.get("with")
    if (
        recovery_checkout.get("uses") != "actions/checkout@v5"
        or not isinstance(recovery_checkout_with, Mapping)
        or recovery_checkout_with.get("ref") != "${{ github.sha }}"
        or recovery_checkout_with.get("persist-credentials") != "false"
    ):
        raise AssertionError(
            "checkpoint recovery must checkout the exact non-credentialed SHA"
        )
    post_approval_hold = str(steps[1].get("run") or "")
    for required in (
        PRODUCTION_RELEASE_HOLD_SCRIPT,
        "require-active",
    ):
        if required not in post_approval_hold:
            raise AssertionError(
                f"checkpoint recovery post-approval hold check lost {required!r}"
            )

    download = steps[2]
    download_with = download.get("with")
    if (
        download.get("uses") != "actions/download-artifact@v5"
        or not isinstance(download_with, Mapping)
        or dict(download_with)
        != {
            "name": expected_frozen_artifact["name"],
            "path": "${{ runner.temp }}/recovery-coordination-plan",
        }
    ):
        raise AssertionError(
            "checkpoint recovery must download its exact same-run frozen plan"
        )
    approved_plan_check = str(steps[3].get("run") or "")
    final_main_check = str(steps[9].get("run") or "")
    for command, label in (
        (approved_plan_check, "post-approval"),
        (final_main_check, "pre-transport"),
    ):
        for required in (
            RELEASE_COORDINATION_SCRIPT,
            "verify-plan",
            '--main-sha "$GITHUB_SHA"',
            (
                "--plan "
                '"$RUNNER_TEMP/recovery-coordination-plan/'
                'recovery-coordination-plan.json"'
            ),
        ):
            if required not in command:
                raise AssertionError(
                    f"checkpoint recovery {label} plan check lost {required!r}"
                )
    approved_intent = str(steps[4].get("run") or "")
    for required in (
        "case \"$RECOVERY_MODE\" in",
        "dry-run)",
        "apply)",
        (
            "QUARANTINE 29df5e6e667351f09305783932b34e5438d6a9d5 "
            "RESIDUE AND ABORT PRE-SWITCH"
        ),
        "REVIEWED_DRY_RUN_RUN_ID",
        "REVIEWED_DRY_RUN_RESULT_SHA256",
        "REVIEWED_MAIN_SHA",
        "REVIEWED_PLAN_SHA256",
    ):
        if required not in approved_intent:
            raise AssertionError(
                f"checkpoint recovery approved intent lost {required!r}"
            )
    frozen_review_download = steps[5]
    frozen_review_download_with = frozen_review_download.get("with")
    if (
        frozen_review_download.get("if") != "${{ inputs.mode == 'apply' }}"
        or frozen_review_download.get("uses") != "actions/download-artifact@v5"
        or not isinstance(frozen_review_download_with, Mapping)
        or dict(frozen_review_download_with)
        != {
            "name": (
                "checkpoint-recovery-reviewed-dry-run-${{ github.sha }}-"
                "${{ github.run_id }}-${{ github.run_attempt }}"
            ),
            "path": "${{ runner.temp }}/reviewed-dry-run-frozen",
        }
    ):
        raise AssertionError(
            "checkpoint recovery must download its same-run frozen dry-run proof"
        )
    frozen_review_validation = steps[6]
    if frozen_review_validation.get("if") != "${{ inputs.mode == 'apply' }}":
        raise AssertionError("frozen dry-run revalidation must be apply-only")
    frozen_review_command = str(frozen_review_validation.get("run") or "")
    for required in (
        "reviewed_recovery_authorization.py verify",
        "checkpoint-recovery-result.json",
        "reviewed-dry-run-authorization.json",
        "--expected-result-sha256",
        "--expected-main-sha",
        "--expected-plan-sha256",
        "--repository",
        "--run-id",
    ):
        if required not in frozen_review_command:
            raise AssertionError(
                f"post-approval dry-run revalidation lost {required!r}"
            )
    bundle_command = str(steps[7].get("run") or "")
    for required in (
        "2026-08-03-29df-pre-switch-candidate-residue.json",
        "reviewed-dry-run-authorization.json",
        "recovery-control-manifest.json",
        '"files": files',
    ):
        if required not in bundle_command:
            raise AssertionError(
                f"recovery control bundle lost {required!r}"
            )
    recovery_execution = steps[10]
    if (
        recovery_execution.get("id") != "recovery"
        or "if" in recovery_execution
        or "continue-on-error" in recovery_execution
    ):
        raise AssertionError(
            "checkpoint recovery execution must remain a fail-closed named step"
        )
    result_presentation = steps[11]
    presentation_command = str(result_presentation.get("run") or "")
    presentation_env = result_presentation.get("env")
    expected_presentation_env = {
        "RECOVERY_MAIN_SHA": "${{ github.sha }}",
        "RECOVERY_MODE": "${{ inputs.mode }}",
        "RECOVERY_RESULT": (
            "${{ runner.temp }}/checkpoint-recovery-result.json"
        ),
        "RECOVERY_STEP_OUTCOME": "${{ steps.recovery.outcome }}",
    }
    if (
        result_presentation.get("if") != "${{ always() }}"
        or not isinstance(presentation_env, Mapping)
        or dict(presentation_env) != expected_presentation_env
        or "secrets." in str(result_presentation)
        or "present_checkpoint_recovery_result.py" not in presentation_command
        or "GITHUB_STEP_SUMMARY" not in presentation_command
        or any(
            option not in presentation_command
            for option in (
                "--result",
                "--summary",
                "--plan",
                "--step-outcome",
                "--mode",
                "--main-sha",
                "--plan-sha256",
            )
        )
    ):
        raise AssertionError(
            "checkpoint recovery result presentation contract changed"
        )
    result_upload = steps[12]
    result_with = result_upload.get("with")
    if (
        result_upload.get("if") != "${{ always() }}"
        or result_upload.get("uses") != "actions/upload-artifact@v4"
        or not isinstance(result_with, Mapping)
        or dict(result_with)
        != {
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
    ):
        raise AssertionError(
            "checkpoint recovery result artifact contract changed"
        )

    workflow_text = (
        REPO_ROOT / CHECKPOINT_RECOVERY_WORKFLOW
    ).read_text(encoding="utf-8")
    for required in (
        (
            "QUARANTINE 29df5e6e667351f09305783932b34e5438d6a9d5 "
            "RESIDUE AND ABORT PRE-SWITCH"
        ),
        "2026-08-03-29df-pre-switch-candidate-residue.json",
        "reviewed_dry_run_run_id",
        "reviewed_dry_run_result_sha256",
        "reviewed_main_sha",
        "reviewed_plan_sha256",
        "checkpoint-recovery-reviewed-dry-run-",
        PRODUCTION_RELEASE_HOLD_SCRIPT,
        "require-active",
        "artifact-ids:",
        "pre_switch_checkpoint_recovery.py",
        "reviewed_recovery_authorization.py",
        "tencent_pre_switch_checkpoint_recovery.sh",
        "production_mutation_lock.sh",
        "present_checkpoint_recovery_result.py",
        "recovery-control-manifest.json",
        "StrictHostKeyChecking=yes",
        "SSH_KNOWN_HOSTS",
        "checkpoint-recovery-result",
        "GITHUB_STEP_SUMMARY",
        "steps.recovery.outcome",
    ):
        if required not in workflow_text:
            raise AssertionError(
                f"checkpoint recovery lost required contract token {required!r}"
            )
    for forbidden in (
        "ABORT 2026-07-30-ce5 PRE-SWITCH",
        "2026-07-30-ce5-pre-switch-db-evidence.json",
        "2026-07-30-86ce-pre-switch-db-evidence.json",
        "fullstack_remote_release.sh",
        "tencent_bluegreen_release.sh",
        "needs.recovery_coordination_guard.outputs",
    ):
        if forbidden in workflow_text:
            raise AssertionError(
                f"checkpoint recovery contains forbidden mutation {forbidden!r}"
            )
    if workflow_jobs["recovery_coordination_guard"].get("outputs") is not None:
        raise AssertionError(
            "checkpoint recovery must not export mutable cross-job outputs"
        )
    legacy_controller_phrase = "ABORT 2026-07-30-86ce PRE-SWITCH"
    if legacy_controller_phrase in workflow_text:
        raise AssertionError(
            "workflow must pass the reviewed phrase without a legacy controller shim"
        )

    controller = (
        REPO_ROOT
        / "03_Scripts/deploy/tencent_pre_switch_checkpoint_recovery.sh"
    ).read_text(encoding="utf-8")
    helper = (
        REPO_ROOT
        / "03_Scripts/deploy/pre_switch_checkpoint_recovery.py"
    ).read_text(encoding="utf-8")
    lock_library = (
        REPO_ROOT
        / "03_Scripts/deploy/lib/production_mutation_lock.sh"
    ).read_text(encoding="utf-8")
    for required in (
        "jato_acquire_production_mutation_lock",
        'RECOVERY_86CE_APPLY_CONFIRMATION="ABORT 2026-07-30-86ce PRE-SWITCH"',
        (
            'RECOVERY_29DF_APPLY_CONFIRMATION="QUARANTINE '
            "29df5e6e667351f09305783932b34e5438d6a9d5 RESIDUE AND ABORT "
            'PRE-SWITCH"'
        ),
        'schema_version == 2',
        'schema_version == 3',
        'reviewed-dry-run-authorization.json',
        '--dry-run-authorization',
        '--dry-run-authorization-sha256',
        "--lock-holder-pid",
        "--expected-plan-sha256",
    ):
        if required not in controller:
            raise AssertionError(
                f"checkpoint recovery controller lost {required!r}"
            )
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
    if systemctl_verbs != {"show", "daemon-reload"}:
        raise AssertionError(
            "checkpoint recovery may only inspect units and reload after quarantine; "
            f"found {sorted(systemctl_verbs)}"
        )
    if helper.count('["systemctl", "daemon-reload"]') != 1:
        raise AssertionError(
            "checkpoint recovery must issue exactly one bounded daemon-reload"
        )
    alembic_verbs = set(
        re.findall(r"-m\s+alembic\s+([a-z-]+)", recovery_sources)
    )
    if alembic_verbs != {"current", "heads"}:
        raise AssertionError(
            "checkpoint recovery may only use alembic current/heads; "
            f"found {sorted(alembic_verbs)}"
        )
    for pattern, label in (
        (
            r"\bsystemctl(?:\s+|[\"']\s*,\s*[\"'])"
            r"(?:start|stop|restart|reload|enable|disable|mask|unmask|"
            r"kill|reset-failed)\b",
            "systemd mutation",
        ),
        (
            r"\balembic(?:\s+|[\"']\s*,\s*[\"'])"
            r"(?:upgrade|downgrade|stamp|revision|merge|edit)\b",
            "Alembic mutation",
        ),
        (
            r"\bnginx(?:\s+|[\"']\s*,\s*[\"'])(?:-s|reload|restart)\b",
            "Nginx mutation",
        ),
    ):
        if re.search(pattern, recovery_sources):
            raise AssertionError(
                f"checkpoint recovery contains forbidden {label}"
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


def assert_reviewed_production_release_hold() -> None:
    helper_path = REPO_ROOT / PRODUCTION_RELEASE_HOLD_SCRIPT
    if not helper_path.is_file() or helper_path.is_symlink():
        raise AssertionError("production release hold helper must be a regular file")
    spec = importlib.util.spec_from_file_location(
        "production_release_hold_static_validator",
        helper_path,
    )
    if spec is None or spec.loader is None:
        raise AssertionError("cannot load production release hold helper")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    if module.HOLD_PATH.as_posix() != PRODUCTION_RELEASE_HOLD_PATH:
        raise AssertionError("production release hold path changed")
    if (
        module.RETIREMENT_PATH.as_posix()
        != PRODUCTION_RELEASE_HOLD_RETIREMENT_PATH
    ):
        raise AssertionError("production release hold retirement path changed")
    if module.RECOVERY_PLAN_PATH.as_posix() != PRODUCTION_RELEASE_RECOVERY_PLAN:
        raise AssertionError("production release recovery plan path changed")
    if module.RECOVERY_PLAN_SHA256 != (
        "61045c5b1f39516f910ab89cf80fdd97796920e7e3bdb479f52e741b73f2f144"
    ):
        raise AssertionError("production release recovery plan SHA-256 changed")
    if module.HOLD_SHA256 != (
        "92bae4fc1febed5b15aba2c5fab7fb941dcb52d091e3a285f4af3d442f9a6371"
    ):
        raise AssertionError("production release active hold SHA-256 changed")
    retirement = module.EXPECTED_RETIREMENT_DOCUMENT
    if (
        retirement.get("incidentId")
        != "2026-08-03-29df-pre-switch-candidate-residue"
        or retirement.get("status") != "retired"
        or retirement.get("recoveryPlan", {}).get("path")
        != PRODUCTION_RELEASE_RECOVERY_PLAN
        or retirement.get("retiredHold", {}).get("path")
        != PRODUCTION_RELEASE_HOLD_PATH
        or retirement.get("retiredHold", {}).get("sha256")
        != module.HOLD_SHA256
    ):
        raise AssertionError("production release hold retirement contract changed")
    try:
        action = module.resolve_release_action(REPO_ROOT)
    except Exception as exc:
        raise AssertionError(
            f"reviewed production release hold is invalid: {exc}"
        ) from exc
    if action not in {"hold", "deploy"}:
        raise AssertionError(f"invalid production release hold action: {action!r}")


def assert_production_release_coordination_guard() -> None:
    workflow = load_workflow(PRODUCTION_RELEASE_WORKFLOW)
    triggers = workflow.get("on")
    push = triggers.get("push") if isinstance(triggers, Mapping) else None
    paths = push.get("paths") if isinstance(push, Mapping) else None
    required_hold_paths = {
        PRODUCTION_RELEASE_HOLD_SCRIPT,
        PRODUCTION_RELEASE_HOLD_PATH,
        PRODUCTION_RELEASE_HOLD_RETIREMENT_PATH,
        PRODUCTION_RELEASE_RECOVERY_PLAN,
    }
    if not isinstance(paths, list) or not required_hold_paths.issubset(set(paths)):
        raise AssertionError(
            "production push trigger must include the hold helper, document, and plan"
        )
    dispatch = triggers.get("workflow_dispatch") if isinstance(triggers, Mapping) else None
    dispatch_inputs = dispatch.get("inputs") if isinstance(dispatch, Mapping) else None
    release_mode = (
        dispatch_inputs.get("release_mode")
        if isinstance(dispatch_inputs, Mapping)
        else None
    )
    if not isinstance(release_mode, Mapping) or release_mode != {
        "description": (
            "Prepare, approve, discard, or release one exact reviewed Candidate"
        ),
        "required": "true",
        "type": "choice",
        "default": "prepare-candidate",
        "options": [
            "prepare-candidate",
            "approve-candidate-to-active",
            "discard-candidate",
            "release-candidate",
        ],
    }:
        raise AssertionError("production Candidate release_mode input changed")
    expected_approval_inputs = {
        "candidate_prepare_run_id",
        "candidate_prepare_run_attempt",
        "candidate_commit_sha",
        "candidate_archive_sha256",
        "candidate_attestation_sha256",
        "confirm_www_activation",
        "confirm_candidate_cleanup",
    }
    if not isinstance(dispatch_inputs, Mapping) or not expected_approval_inputs.issubset(
        dispatch_inputs
    ):
        raise AssertionError("production Candidate approval inputs changed")
    confirmation = dispatch_inputs.get("confirm_www_activation")
    if not isinstance(confirmation, Mapping) or confirmation != {
        "description": "I confirm this exact Candidate may replace www Active",
        "required": "true",
        "type": "boolean",
        "default": "false",
    }:
        raise AssertionError("production Candidate approval confirmation changed")
    cleanup_confirmation = dispatch_inputs.get("confirm_candidate_cleanup")
    if not isinstance(cleanup_confirmation, Mapping) or cleanup_confirmation != {
        "description": "I confirm the exact reviewed Candidate may be cleaned up",
        "required": "true",
        "type": "boolean",
        "default": "false",
    }:
        raise AssertionError("production Candidate cleanup confirmation changed")
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
    outputs = preflight.get("outputs")
    if not isinstance(outputs, Mapping) or outputs != {
        "release-action": "${{ steps.production_hold.outputs.release-action }}",
    }:
        raise AssertionError(
            "production coordination must expose only the exact hold/deploy action"
        )
    preflight_steps = get_steps(
        preflight,
        PRODUCTION_RELEASE_WORKFLOW,
        PRODUCTION_COORDINATION_JOB,
    )
    if [step.get("name") for step in preflight_steps] != [
        "Checkout release coordination guard",
        "Validate unpublished release coordination",
        "Resolve reviewed production release hold",
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
    hold_step = preflight_steps[2]
    if hold_step.get("id") != "production_hold":
        raise AssertionError("production hold resolver step ID changed")
    hold_command = str(hold_step.get("run") or "")
    for required in (
        PRODUCTION_RELEASE_HOLD_SCRIPT,
        "resolve",
        '--github-output "$GITHUB_OUTPUT"',
    ):
        if required not in hold_command:
            raise AssertionError(
                f"production hold resolver is missing {required!r}"
            )
    if "secrets." in str(hold_step):
        raise AssertionError("production hold resolver must not consume secrets")

    freeze = preflight_steps[3]
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
    if unwrap_expression(build.get("if")) != PRODUCTION_PREPARE_CONDITION:
        raise AssertionError("frontend build must require Candidate preparation mode")

    deploy = get_job(workflow, PRODUCTION_RELEASE_WORKFLOW, "deploy_tencent")
    if deploy.get("needs") != [PRODUCTION_COORDINATION_JOB, "build_frontend"]:
        raise AssertionError(
            "Tencent deploy must directly retain the hold guard dependency"
        )
    if unwrap_expression(deploy.get("if")) != PRODUCTION_PREPARE_CONDITION:
        raise AssertionError("Tencent deploy must require Candidate preparation mode")
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

    approval = get_job(
        workflow,
        PRODUCTION_RELEASE_WORKFLOW,
        "approve_candidate_to_active",
    )
    if approval.get("needs") != PRODUCTION_COORDINATION_JOB:
        raise AssertionError("Candidate approval must retain the hold guard dependency")
    if unwrap_expression(approval.get("if")) != PRODUCTION_APPROVAL_CONDITION:
        raise AssertionError("Candidate approval must use the exact manual approval gate")
    approval_steps = get_steps(
        approval,
        PRODUCTION_RELEASE_WORKFLOW,
        "approve_candidate_to_active",
    )
    approval_names = [str(step.get("name") or "") for step in approval_steps]
    required_approval_steps = [
        "Validate exact Candidate approval request",
        "Verify immutable Candidate handoff",
        "Validate Tencent approval credentials",
        "Revalidate frozen coordination plan immediately before Active mutation",
        "Approve exact Candidate on Tencent",
        "Record failed approval restore boundary",
        "Verify www serves the exact approved Candidate",
        "Restore previous Active after failed www audit",
        "Keep failed www audit red after automatic restore",
        "Fetch and attest Active update checkpoint",
        "Seal www approval receipt",
    ]
    positions = [approval_names.index(name) for name in required_approval_steps]
    if positions != sorted(positions):
        raise AssertionError("Candidate approval guard steps are out of order")
    approval_text = str(approval)
    for required in (
        "approve-candidate-to-active",
        "candidate_attestation_sha256",
        "verify_candidate_handoff.py",
        "CANDIDATE_VERIFIED_ENV",
        "github_candidate_control.sh approve-candidate-to-active",
        "CANDIDATE_SERVER_EVIDENCE_PATH",
        "binding.group(1) != expected_evidence_path",
        "automatic restore previous successful Active",
        "no success receipt is emitted",
        "restore-previous-active",
        "steps.verify_www.outcome == 'failure'",
        "active_updated",
        "active-updated.journal.jsonl",
        "canonical Active update journal tail/checkpoint mismatch",
        "intl unchanged",
    ):
        if required not in approval_text:
            raise AssertionError(
                f"Candidate approval lost fail-closed binding {required!r}"
            )
    for forbidden in ("prepare-and-switch", "pages deploy", "npm run build"):
        if forbidden in approval_text:
            raise AssertionError(
                f"Candidate approval must not rebuild or publish intl: {forbidden!r}"
            )
    if "release-candidate/journal.jsonl" in approval_text:
        raise AssertionError("www receipt must use the canonical server journal")
    approve_step = next(
        step
        for step in approval_steps
        if step.get("name") == "Approve exact Candidate on Tencent"
    )
    if approve_step.get("id") != "approve_active" or approve_step.get(
        "continue-on-error"
    ) not in (None, False, "false"):
        raise AssertionError("Candidate approval failure must not be masked")
    mutation_step_name = (
        "Revalidate frozen coordination plan immediately before Active mutation"
    )
    if approval_names.index(mutation_step_name) + 1 != approval_names.index(
        "Approve exact Candidate on Tencent"
    ):
        raise AssertionError("frozen plan must be rechecked immediately before Active mutation")
    mutation_step = approval_steps[approval_names.index(mutation_step_name)]
    mutation_command = str(mutation_step.get("run") or "")
    for required in (
        RELEASE_COORDINATION_SCRIPT,
        "verify-plan",
        '--main-sha "$GITHUB_SHA"',
        '"$RUNNER_TEMP/release-coordination-plan/release-coordination-plan.json"',
    ):
        if required not in mutation_command:
            raise AssertionError(
                f"pre-Active-mutation coordination check is missing {required}"
            )

    cleanup = get_job(
        workflow,
        PRODUCTION_RELEASE_WORKFLOW,
        "cleanup_candidate",
    )
    if cleanup.get("needs") != PRODUCTION_COORDINATION_JOB:
        raise AssertionError("Candidate cleanup must retain the hold guard dependency")
    if unwrap_expression(cleanup.get("if")) != PRODUCTION_CLEANUP_CONDITION:
        raise AssertionError("Candidate cleanup must use the exact manual cleanup gate")
    cleanup_steps = get_steps(
        cleanup,
        PRODUCTION_RELEASE_WORKFLOW,
        "cleanup_candidate",
    )
    cleanup_names = [str(step.get("name") or "") for step in cleanup_steps]
    required_cleanup_steps = [
        "Validate exact Candidate cleanup request",
        "Resolve Candidate cleanup handoff source",
        "Validate Tencent cleanup credentials",
        "Verify immutable Candidate cleanup handoff",
        "Capture canonical Candidate cleanup handoff",
        "Verify canonical Candidate cleanup handoff",
        "Capture unchanged Active identity before cleanup",
        "Clean exact Candidate on Tencent",
        "Fetch canonical Candidate cleanup receipt",
        "Verify Active identity and health remained unchanged",
        "Retain immutable Candidate cleanup receipt",
    ]
    cleanup_positions = [cleanup_names.index(name) for name in required_cleanup_steps]
    if cleanup_positions != sorted(cleanup_positions):
        raise AssertionError("Candidate cleanup guard steps are out of order")
    cleanup_text = str(cleanup)
    for required in (
        "confirm_candidate_cleanup",
        "canonical-server",
        "capture-canonical-cleanup",
        "CANDIDATE_CANONICAL_BUNDLE_OUTPUT",
        "reviewed-candidate.json",
        "verify_candidate_handoff.py",
        "release-candidate",
        "discard-candidate",
        'mode == "discard-candidate"',
        '{"success", "failure"}',
        'else {"success"}',
        'run.get("conclusion") not in allowed_conclusions',
        "Candidate cleanup journal identity/sequence mismatch",
        "Candidate cleanup journal tail/checkpoint mismatch",
        "Active identity and health remained unchanged",
        "overwrite': 'false'",
        "retention-days': '30'",
    ):
        if required not in cleanup_text:
            raise AssertionError(
                f"Candidate cleanup lost fail-closed binding {required!r}"
            )
    if "Require exact intl artifact before releasing Candidate" in cleanup_names:
        raise AssertionError("intl synchronization must not block Candidate cleanup")
    for forbidden in (
        "prepare-and-switch",
        "pages deploy",
        "npm run build",
        "Package backend release",
        "Upload complete release archive",
    ):
        if forbidden in cleanup_text:
            raise AssertionError(
                f"Candidate cleanup must not build or publish intl: {forbidden!r}"
            )

    audit = get_job(
        workflow,
        PRODUCTION_RELEASE_WORKFLOW,
        "audit_frontend_parity",
    )
    if audit.get("needs") != [
        PRODUCTION_COORDINATION_JOB,
        "build_frontend",
        "deploy_tencent",
    ]:
        raise AssertionError(
            "frontend parity audit must directly retain the hold guard dependency"
        )
    if unwrap_expression(audit.get("if")) != PRODUCTION_LEGACY_INTL_CONDITION:
        raise AssertionError(
            "frontend parity audit must require the full-release mode"
        )


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
    inner_start = bluegreen_release.index("run_inner_prepare() {")
    inner_end = bluegreen_release.index("\n}\n", inner_start)
    inner_prepare = bluegreen_release[inner_start:inner_end]
    if "RUN_DATABASE_MIGRATIONS=verify_only" not in inner_prepare:
        raise AssertionError(
            "blue/green prepare must use read-only database verification",
        )
    if "RUN_DATABASE_MIGRATIONS=false" in inner_prepare:
        raise AssertionError(
            "blue/green prepare must not confuse disabled DB with no-migration mode",
        )
    for function_name in (
        "run_post_activation",
        "run_post_commit_global_reconciliation",
    ):
        function_start = bluegreen_release.index(f"{function_name}() {{")
        function_end = bluegreen_release.index("\n}\n", function_start)
        function_body = bluegreen_release[function_start:function_end]
        if "RUN_DATABASE_MIGRATIONS=false" not in function_body:
            raise AssertionError(
                f"{function_name} must retain its non-preparation no-migration mode",
            )
    verify_start = server_deploy.index(
        'elif [[ "$DATABASE_MIGRATION_VERIFY_ONLY" == "true" ]]',
    )
    verify_end = server_deploy.index("\nelse\n", verify_start)
    verify_only = server_deploy[verify_start:verify_end]
    if "python -m alembic upgrade head" in verify_only:
        raise AssertionError("read-only database verification must not run migrations")
    if 'write_release_evidence "completed"' not in verify_only:
        raise AssertionError(
            "read-only database verification must write completed evidence",
        )
    if "python -m alembic upgrade head" not in server_deploy:
        raise AssertionError("expected Alembic production migration command was not found")
    if 'DEPLOY_BRANCH" != "main"' not in server_deploy:
        raise AssertionError("database migration must retain the main branch gate")
    if 'PRODUCTION_RELEASE_WORKFLOW" != "true"' not in server_deploy:
        raise AssertionError("database migration must require the production release workflow")


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
    assert_reviewed_production_release_hold()
    assert_production_release_coordination_guard()

    for relative_path, job_names in PRODUCTION_JOBS.items():
        for job_name in job_names:
            assert_main_only_production_job(relative_path, job_name)

    assert_all_static_production_jobs_are_registered()
    assert_production_release_main_guards()
    assert_checkpoint_recovery_contract()

    for relative_path in MANUAL_DEPLOY_WORKFLOWS:
        for job_name in PRODUCTION_JOBS[relative_path]:
            assert_manual_deploy_is_skipped_for_non_main(relative_path, job_name)

    assert_country_news_production_write_is_main_only()
    assert_database_migration_is_behind_main_release_gate()
    assert_feature_canary_cannot_route_or_mutate_production()
    print(
        "Validated release coordination and main-only production gates for "
        f"{sum(len(job_names) for job_names in PRODUCTION_JOBS.values())} "
        "production jobs and "
        f"{len(MANUAL_DEPLOY_WORKFLOWS)} manual deploy workflows."
    )


if __name__ == "__main__":
    main()
