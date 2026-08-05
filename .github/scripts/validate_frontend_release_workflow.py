#!/usr/bin/env python3
"""Validate immutable production frontend release workflow contracts."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import re
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/production-release.yml"
PREWARM_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/intl-edge-prewarm.yml"
INTL_SYNC_WORKFLOW_PATH = (
    REPO_ROOT / ".github/workflows/sync-www-active-to-intl.yml"
)
CI_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/ci.yml"
REMOTE_RELEASE_PATH = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
CANDIDATE_CONTROL_PATH = (
    REPO_ROOT / "03_Scripts/deploy/github_candidate_control.sh"
)
ARCHIVE_VALIDATOR_PATH = (
    REPO_ROOT / "03_Scripts/deploy/validate_release_archive.py"
)
SERVER_RELEASE_PATH = REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
BLUEGREEN_RELEASE_PATH = (
    REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
)
BLUEGREEN_BOOT_RECONCILE_PATH = (
    REPO_ROOT / "03_Scripts/deploy/jato_bluegreen_boot_reconcile.py"
)
RELEASE_STORAGE_GUARD_PATH = (
    REPO_ROOT / "03_Scripts/deploy/jato_release_storage_guard.py"
)
MAIN_CONDITION = "github.ref == 'refs/heads/main'"
RELEASE_DEPLOY_CONDITION = (
    MAIN_CONDITION
    + " && needs.release_coordination_guard.outputs.release-action == 'deploy'"
)
PREPARE_RELEASE_CONDITION = (
    RELEASE_DEPLOY_CONDITION
    + " && (github.event_name != 'workflow_dispatch' || "
    + "inputs.release_mode == 'prepare-candidate')"
)
APPROVE_RELEASE_CONDITION = (
    MAIN_CONDITION
    + " && github.event_name == 'workflow_dispatch'"
    + " && inputs.release_mode == 'approve-candidate-to-active'"
    + " && needs.release_coordination_guard.outputs.release-action == 'deploy'"
)
CLEANUP_RELEASE_CONDITION = (
    MAIN_CONDITION
    + " && github.event_name == 'workflow_dispatch'"
    + " && (inputs.release_mode == 'discard-candidate' || "
    + "inputs.release_mode == 'release-candidate')"
    + " && needs.release_coordination_guard.outputs.release-action == 'deploy'"
)
LEGACY_INTL_CONDITION = (
    RELEASE_DEPLOY_CONDITION
    + " && github.event_name == 'workflow_dispatch'"
    + " && inputs.release_mode == 'prepare-and-switch'"
)
PRODUCTION_HOLD_SCRIPT = ".github/scripts/production_release_hold.py"
PRODUCTION_HOLD_PATH = (
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue-production-hold.v1.json"
)
PRODUCTION_HOLD_RETIREMENT_PATH = (
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue-"
    "production-hold-retirement.v1.json"
)
PRODUCTION_HOLD_PLAN_PATH = (
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue.json"
)
PREWARM_CONDITION = " ".join(
    (
        "github.event.workflow_run.conclusion == 'success' &&",
        "github.event.workflow_run.head_branch == 'main' &&",
        "github.event.workflow_run.head_repository.full_name == github.repository",
    )
)
PREWARM_DEPLOY_CONDITION = (
    "steps.production_hold.outputs.release-action == 'deploy'"
)
BUILD_JOB = "build_frontend"
COORDINATION_JOB = "release_coordination_guard"
DEPLOY_JOBS = ("deploy_tencent",)
PRODUCTION_ENVIRONMENT_JOBS = (
    "deploy_tencent",
    "approve_candidate_to_active",
    "cleanup_candidate",
)
REQUIRED_BUILD_OUTPUTS = {
    "artifact_name",
    "artifact_identity",
    "artifact_checksum",
    "github_artifact_id",
    "github_artifact_digest",
    "frontend_build_id",
    "node_version",
    "app_commit",
    "release_id",
    "workflow_run_attempt",
    "build_timestamp",
}
CLOUDFLARE_PROJECT_DIRECTORY = "${{ runner.temp }}/cloudflare-pages-project"
PINNED_WRANGLER_VERSION = "4.86.0"
FORBIDDEN_BUILD_COMMANDS = (
    "npm ci",
    "npm install",
    "npm run build",
    "vite build",
    "yarn build",
    "pnpm build",
)
REQUIRED_CI_JOBS = (
    "production-deployment-guard",
    "frontend-release-contract",
    "smoke",
    "fullstack-backend",
    "fullstack-frontend",
)


def load_workflow(path: Path) -> Mapping[str, Any]:
    payload = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    if not isinstance(payload, Mapping):
        raise AssertionError(f"{path}: workflow root must be a mapping")
    return payload


def mapping(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AssertionError(f"{context} must be a mapping")
    return value


def sequence(value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise AssertionError(f"{context} must be a sequence")
    return value


def unwrap_expression(value: Any) -> str:
    expression = " ".join(str(value or "").split())
    if expression.startswith("${{") and expression.endswith("}}"):
        expression = expression[3:-2].strip()
    return expression


def job(workflow: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    jobs = mapping(workflow.get("jobs"), "jobs")
    return mapping(jobs.get(name), f"job {name}")


def steps(job_payload: Mapping[str, Any], context: str) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    for value in sequence(job_payload.get("steps"), f"{context}.steps"):
        result.append(mapping(value, f"{context}.step"))
    return result


def step_by_name(
    job_steps: list[Mapping[str, Any]],
    name: str,
) -> Mapping[str, Any]:
    matches = [step for step in job_steps if step.get("name") == name]
    if len(matches) != 1:
        raise AssertionError(f"expected one step named {name!r}; found {len(matches)}")
    return matches[0]


def combined_run(job_payload: Mapping[str, Any], context: str) -> str:
    return "\n".join(str(step.get("run") or "") for step in steps(job_payload, context))


def assert_continue_on_error_disabled(
    payload: Mapping[str, Any],
    context: str,
) -> None:
    value = payload.get("continue-on-error")
    if value is None or value is False or value == "false":
        return
    raise AssertionError(
        f"{context} must fail closed; continue-on-error cannot be {value!r}"
    )


def assert_required_ci_jobs_fail_closed(ci: Mapping[str, Any]) -> None:
    for job_name in REQUIRED_CI_JOBS:
        ci_job = job(ci, job_name)
        display_name = ci_job.get("name")
        if display_name not in (None, job_name):
            raise AssertionError(
                f"CI job {job_name} must retain its required context name"
            )
        if "if" in ci_job:
            raise AssertionError(
                f"CI job {job_name} must not be conditional"
            )
        assert_continue_on_error_disabled(ci_job, f"CI job {job_name}")
        for index, ci_step in enumerate(steps(ci_job, f"CI job {job_name}")):
            if "if" in ci_step:
                raise AssertionError(
                    f"CI job {job_name} step {index + 1} must not be conditional"
                )
            assert_continue_on_error_disabled(
                ci_step,
                f"CI job {job_name} step {index + 1}",
            )


def assert_pull_request_merge_result_checkout(ci: Mapping[str, Any]) -> None:
    triggers = mapping(ci.get("on"), "CI workflow on")
    if "pull_request" not in triggers:
        raise AssertionError("CI must retain the pull_request merge-result trigger")

    for job_name in REQUIRED_CI_JOBS:
        checkouts = [
            ci_step
            for ci_step in steps(job(ci, job_name), f"CI job {job_name}")
            if ci_step.get("uses") == "actions/checkout@v4"
        ]
        if len(checkouts) != 1:
            raise AssertionError(
                f"CI job {job_name} must use exactly one actions/checkout@v4 step"
            )
        checkout_with = checkouts[0].get("with")
        if checkout_with is None:
            continue
        checkout_options = mapping(
            checkout_with,
            f"CI job {job_name} checkout with",
        )
        forbidden_options = {"ref", "repository"} & set(checkout_options)
        if forbidden_options:
            raise AssertionError(
                f"CI job {job_name} must checkout the pull_request merge result; "
                f"remove overrides {sorted(forbidden_options)}"
            )


def assert_main_only_production_workflow(workflow: Mapping[str, Any]) -> None:
    triggers = mapping(workflow.get("on"), "production workflow on")
    push = mapping(triggers.get("push"), "production workflow push")
    branches = sequence(push.get("branches"), "production workflow push.branches")
    if branches != ["main"]:
        raise AssertionError("production push trigger must be main-only")
    if set(triggers) != {"push", "workflow_dispatch"}:
        raise AssertionError("production release may only use push and workflow_dispatch")
    dispatch = mapping(
        triggers.get("workflow_dispatch"),
        "production workflow workflow_dispatch",
    )
    dispatch_inputs = mapping(
        dispatch.get("inputs"),
        "production workflow workflow_dispatch.inputs",
    )
    release_mode = mapping(
        dispatch_inputs.get("release_mode"),
        "production workflow release_mode input",
    )
    if (
        release_mode.get("required") != "true"
        or release_mode.get("type") != "choice"
        or release_mode.get("default") != "prepare-candidate"
        or sequence(release_mode.get("options"), "release_mode.options")
        != [
            "prepare-candidate",
            "approve-candidate-to-active",
            "discard-candidate",
            "release-candidate",
        ]
    ):
        raise AssertionError("production release_mode input contract changed")
    expected_approval_inputs = {
        "candidate_prepare_run_id",
        "candidate_prepare_run_attempt",
        "candidate_commit_sha",
        "candidate_archive_sha256",
        "candidate_attestation_sha256",
    }
    if not expected_approval_inputs.issubset(dispatch_inputs):
        raise AssertionError("production Candidate approval identity inputs changed")
    confirmation = mapping(
        dispatch_inputs.get("confirm_www_activation"),
        "production workflow Candidate approval confirmation input",
    )
    if (
        confirmation.get("required") != "true"
        or confirmation.get("type") != "boolean"
        or confirmation.get("default") != "false"
    ):
        raise AssertionError("production Candidate approval confirmation changed")
    cleanup_confirmation = mapping(
        dispatch_inputs.get("confirm_candidate_cleanup"),
        "production workflow Candidate cleanup confirmation input",
    )
    if (
        cleanup_confirmation.get("required") != "true"
        or cleanup_confirmation.get("type") != "boolean"
        or cleanup_confirmation.get("default") != "false"
    ):
        raise AssertionError("production Candidate cleanup confirmation changed")
    bootstrap_confirmation = mapping(
        dispatch_inputs.get("bootstrap_full_upload"),
        "production workflow full-upload bootstrap input",
    )
    if (
        bootstrap_confirmation.get("required") != "true"
        or bootstrap_confirmation.get("type") != "boolean"
        or bootstrap_confirmation.get("default") != "false"
    ):
        raise AssertionError("production full-upload bootstrap confirmation changed")

    workflow_env = mapping(workflow.get("env"), "production workflow env")
    expected_mode = (
        "${{ github.event_name == 'workflow_dispatch' && inputs.release_mode "
        "|| 'prepare-candidate' }}"
    )
    if workflow_env.get("RELEASE_MODE") != expected_mode:
        raise AssertionError("production RELEASE_MODE dispatch binding changed")

    jobs = mapping(workflow.get("jobs"), "production workflow jobs")
    expected_jobs = {
        COORDINATION_JOB,
        BUILD_JOB,
        "deploy_tencent",
        "approve_candidate_to_active",
        "cleanup_candidate",
        "audit_frontend_parity",
    }
    if set(jobs) != expected_jobs:
        raise AssertionError(
            f"unexpected production release jobs: {sorted(set(jobs) - expected_jobs)}"
        )
    for name in expected_jobs:
        condition = unwrap_expression(job(workflow, name).get("if"))
        expected_conditions = {
            COORDINATION_JOB: MAIN_CONDITION,
            BUILD_JOB: PREPARE_RELEASE_CONDITION,
            "deploy_tencent": PREPARE_RELEASE_CONDITION,
            "approve_candidate_to_active": APPROVE_RELEASE_CONDITION,
            "cleanup_candidate": CLEANUP_RELEASE_CONDITION,
            "audit_frontend_parity": LEGACY_INTL_CONDITION,
        }
        expected_condition = expected_conditions[name]
        if condition != expected_condition:
            raise AssertionError(
                f"{name} must use the exact main-and-release-action condition"
            )
    for name in PRODUCTION_ENVIRONMENT_JOBS:
        deploy_job = job(workflow, name)
        if deploy_job.get("environment") != "production":
            raise AssertionError(f"{name} must use the production environment")


def assert_production_hold_gate_contract(workflow: Mapping[str, Any]) -> None:
    triggers = mapping(workflow.get("on"), "production workflow on")
    push = mapping(triggers.get("push"), "production workflow push")
    push_paths = set(sequence(push.get("paths"), "production workflow push.paths"))
    required_paths = {
        PRODUCTION_HOLD_SCRIPT,
        PRODUCTION_HOLD_PATH,
        PRODUCTION_HOLD_RETIREMENT_PATH,
        PRODUCTION_HOLD_PLAN_PATH,
    }
    if not required_paths.issubset(push_paths):
        raise AssertionError(
            "production push paths must trigger on hold helper, document, and plan"
        )

    guard = job(workflow, COORDINATION_JOB)
    guard_outputs = mapping(guard.get("outputs"), "release coordination outputs")
    if dict(guard_outputs) != {
        "release-action": "${{ steps.production_hold.outputs.release-action }}",
    }:
        raise AssertionError("release coordination output must be exact hold or deploy")
    guard_steps = steps(guard, COORDINATION_JOB)
    hold_step = step_by_name(guard_steps, "Resolve reviewed production release hold")
    if hold_step.get("id") != "production_hold":
        raise AssertionError("production hold resolver step ID changed")
    hold_command = str(hold_step.get("run") or "")
    for required in (
        PRODUCTION_HOLD_SCRIPT,
        "resolve",
        '--github-output "$GITHUB_OUTPUT"',
    ):
        if required not in hold_command:
            raise AssertionError(f"production hold resolver is missing {required!r}")
    if "secrets." in str(guard):
        raise AssertionError("production release hold guard must not consume secrets")

    expected_needs = {
        BUILD_JOB: COORDINATION_JOB,
        "deploy_tencent": [COORDINATION_JOB, BUILD_JOB],
        "approve_candidate_to_active": COORDINATION_JOB,
        "cleanup_candidate": COORDINATION_JOB,
        "audit_frontend_parity": [
            COORDINATION_JOB,
            BUILD_JOB,
            "deploy_tencent",
        ],
    }
    for name, needs in expected_needs.items():
        downstream = job(workflow, name)
        if downstream.get("needs") != needs:
            raise AssertionError(f"{name} lost its direct production hold dependency")
        expected_conditions = {
            BUILD_JOB: PREPARE_RELEASE_CONDITION,
            "deploy_tencent": PREPARE_RELEASE_CONDITION,
            "approve_candidate_to_active": APPROVE_RELEASE_CONDITION,
            "cleanup_candidate": CLEANUP_RELEASE_CONDITION,
            "audit_frontend_parity": LEGACY_INTL_CONDITION,
        }
        expected_condition = expected_conditions[name]
        if unwrap_expression(downstream.get("if")) != expected_condition:
            raise AssertionError(f"{name} lost the exact hold/deploy output gate")


def assert_single_build_and_strict_outputs(workflow: Mapping[str, Any]) -> None:
    build = job(workflow, BUILD_JOB)
    if build.get("needs") != COORDINATION_JOB:
        raise AssertionError(
            "build_frontend must wait for the no-environment coordination preflight"
        )
    outputs = mapping(build.get("outputs"), "build_frontend.outputs")
    if set(outputs) != REQUIRED_BUILD_OUTPUTS:
        raise AssertionError(
            "build outputs mismatch: "
            f"missing={sorted(REQUIRED_BUILD_OUTPUTS - set(outputs))}, "
            f"extra={sorted(set(outputs) - REQUIRED_BUILD_OUTPUTS)}"
        )
    build_steps = steps(build, BUILD_JOB)
    setup = next(
        (step for step in build_steps if step.get("uses") == "actions/setup-node@v4"),
        None,
    )
    if setup is None:
        raise AssertionError("build_frontend must use actions/setup-node@v4")
    setup_with = mapping(setup.get("with"), "build setup-node with")
    if setup_with.get("node-version") != "${{ env.FRONTEND_NODE_VERSION }}":
        raise AssertionError("build_frontend must use the workflow's fixed Node version")
    if setup_with.get("cache-dependency-path") != "06_AppPlatform/frontend/package-lock.json":
        raise AssertionError("build_frontend must cache against the frontend lockfile")

    all_commands = combined_run(build, BUILD_JOB)
    if len(re.findall(r"(?m)^\s*npm ci\s*$", all_commands)) != 1:
        raise AssertionError("build_frontend must install the lockfile exactly once")
    if len(re.findall(r"(?m)^\s*(?:run:\s*)?npm run build\s*$", all_commands)) != 1:
        raise AssertionError("build_frontend must build the frontend exactly once")

    upload_steps = [
        step for step in build_steps if step.get("uses") == "actions/upload-artifact@v4"
    ]
    if len(upload_steps) != 1:
        raise AssertionError("build_frontend must upload exactly one v4 artifact")
    upload_with = mapping(upload_steps[0].get("with"), "upload-artifact with")
    if upload_with.get("name") != "${{ steps.release.outputs.artifact-name }}":
        raise AssertionError("upload name must come from the immutable release manifest")
    if upload_with.get("if-no-files-found") != "error":
        raise AssertionError("missing artifact files must fail closed")
    if upload_with.get("overwrite") != "false":
        raise AssertionError("immutable artifact overwrite must be disabled")
    if upload_with.get("retention-days") != "30":
        raise AssertionError("frontend artifact must cover the manual review window")

    create_step = step_by_name(build_steps, "Create immutable frontend release")
    create_command = str(create_step.get("run") or "")
    if "--functions-dir 06_AppPlatform/frontend/functions" not in create_command:
        raise AssertionError(
            "the immutable frontend release must include Cloudflare Pages Functions"
        )


def assert_deploy_jobs_share_one_artifact(workflow: Mapping[str, Any]) -> None:
    for name in DEPLOY_JOBS:
        deploy_job = job(workflow, name)
        expected_needs: Any = [COORDINATION_JOB, BUILD_JOB]
        if deploy_job.get("needs") != expected_needs:
            raise AssertionError(
                f"{name} needs mismatch: expected {expected_needs!r}, "
                f"found {deploy_job.get('needs')!r}"
            )
        deploy_steps = steps(deploy_job, name)
        downloads = [
            step
            for step in deploy_steps
            if step.get("uses") == "actions/download-artifact@v5"
        ]
        if len(downloads) != 2:
            raise AssertionError(
                f"{name} must download the coordination plan and one frontend artifact"
            )
        frontend_downloads = [
            step
            for step in downloads
            if "artifact-ids"
            in mapping(step.get("with"), f"{name} download with")
        ]
        if len(frontend_downloads) != 1:
            raise AssertionError(
                f"{name} must select exactly one frontend artifact by numeric id"
            )
        download_with = mapping(
            frontend_downloads[0].get("with"),
            f"{name} frontend download with",
        )
        if (
            download_with.get("artifact-ids")
            != "${{ needs.build_frontend.outputs.github_artifact_id }}"
        ):
            raise AssertionError(f"{name} must download the build's exact artifact id")
        if "name" in download_with or "pattern" in download_with:
            raise AssertionError(f"{name} must not use a name/pattern fallback download")

        commands = combined_run(deploy_job, name)
        for forbidden in FORBIDDEN_BUILD_COMMANDS:
            if forbidden in commands:
                raise AssertionError(f"{name} contains forbidden build command {forbidden!r}")
        for output_name in (
            "artifact_name",
            "artifact_identity",
            "artifact_checksum",
            "github_artifact_id",
            "github_artifact_digest",
            "frontend_build_id",
            "node_version",
            "release_id",
            "workflow_run_attempt",
            "build_timestamp",
        ):
            expression = f"${{{{ needs.build_frontend.outputs.{output_name} }}}}"
            if expression not in str(deploy_job):
                raise AssertionError(f"{name} does not consume build output {output_name}")

    tencent_steps = steps(job(workflow, "deploy_tencent"), "deploy_tencent")
    tencent_names = [str(step.get("name") or "") for step in tencent_steps]
    cloudflare_node_step = step_by_name(tencent_steps, "Setup fixed Cloudflare Node")
    if cloudflare_node_step.get("uses") != "actions/setup-node@v4":
        raise AssertionError("Cloudflare deployment must use actions/setup-node@v4")
    if unwrap_expression(cloudflare_node_step.get("if")) != (
        "env.RELEASE_MODE == 'prepare-and-switch'"
    ):
        raise AssertionError("Candidate preparation must skip Cloudflare setup")
    cloudflare_node_with = mapping(
        cloudflare_node_step.get("with"),
        "Cloudflare setup-node with",
    )
    if cloudflare_node_with.get("node-version") != "${{ env.FRONTEND_NODE_VERSION }}":
        raise AssertionError("Cloudflare deployment must use the fixed frontend Node version")
    cloudflare_preflight = tencent_names.index("Validate Cloudflare deploy configuration")
    artifact_verify_index = tencent_names.index(
        "Verify frontend artifact before Tencent deployment"
    )
    if artifact_verify_index > cloudflare_preflight:
        raise AssertionError(
            "the shared artifact must be verified and materialized before Cloudflare preflight"
        )
    if cloudflare_preflight > tencent_names.index(
        "Upload complete release archive with incremental rsync"
    ):
        raise AssertionError("Cloudflare configuration must fail before any production mutation")
    if tencent_names.index("Verify frontend artifact before Tencent deployment") > tencent_names.index(
        "Deploy verified release on Tencent"
    ):
        raise AssertionError("Tencent must verify the artifact before deployment")
    verify_step = step_by_name(
        tencent_steps,
        "Verify frontend artifact before Tencent deployment",
    )
    if '--materialize-dir "$RUNNER_TEMP/frontend-dist"' not in str(
        verify_step.get("run") or ""
    ):
        raise AssertionError("the shared artifact must be materialized before either deployment")
    if (
        '--materialize-functions-dir '
        '"$RUNNER_TEMP/cloudflare-pages-project/functions"'
        not in str(verify_step.get("run") or "")
    ):
        raise AssertionError(
            "Cloudflare Pages Functions must be materialized from the shared artifact"
        )

    workflow_env = mapping(workflow.get("env"), "production workflow env")
    if workflow_env.get("WRANGLER_VERSION") != PINNED_WRANGLER_VERSION:
        raise AssertionError(
            f"Wrangler must be pinned to {PINNED_WRANGLER_VERSION}"
        )
    preflight_step = step_by_name(
        tencent_steps,
        "Validate Cloudflare deploy configuration",
    )
    if unwrap_expression(preflight_step.get("if")) != (
        "env.RELEASE_MODE == 'prepare-and-switch'"
    ):
        raise AssertionError("Candidate preparation must skip Cloudflare credentials")
    if preflight_step.get("working-directory") != CLOUDFLARE_PROJECT_DIRECTORY:
        raise AssertionError(
            "Cloudflare preflight must run from the materialized Pages project"
        )
    preflight_command = str(preflight_step.get("run") or "")
    required_preflight_tokens = (
        'npx --yes "wrangler@$WRANGLER_VERSION" pages functions build functions',
        '--project-directory .',
        '--output-routes-path "$routes_manifest"',
        'required = {"/healthz", "/oauth-relay/*", "/v1/*"}',
    )
    missing_preflight = [
        token for token in required_preflight_tokens if token not in preflight_command
    ]
    if missing_preflight:
        raise AssertionError(
            f"Cloudflare Pages Functions preflight is incomplete: {missing_preflight}"
        )

    cloudflare_index = tencent_names.index("Deploy downloaded dist to Cloudflare Pages")
    cloudflare_step = step_by_name(
        tencent_steps,
        "Deploy downloaded dist to Cloudflare Pages",
    )
    if cloudflare_step.get("working-directory") != CLOUDFLARE_PROJECT_DIRECTORY:
        raise AssertionError(
            "Cloudflare deployment must run from the materialized Pages project"
        )
    cloudflare_command = str(cloudflare_step.get("run") or "")
    if (
        'npx --yes "wrangler@$WRANGLER_VERSION" pages deploy '
        '"$RUNNER_TEMP/frontend-dist"'
        not in cloudflare_command
    ):
        raise AssertionError(
            "Cloudflare must deploy the verified dist with the pinned Wrangler"
        )
    if "wrangler@latest" in cloudflare_command:
        raise AssertionError("Cloudflare deployment must not use a floating Wrangler version")
    if tencent_names.index("Deploy verified release on Tencent") > cloudflare_index:
        raise AssertionError("Tencent must succeed before Cloudflare switches the shared artifact")
    if cloudflare_index > tencent_names.index("Verify intl public release provenance"):
        raise AssertionError("Cloudflare deployment must be verified before release completion")
    runtime_verify_index = tencent_names.index("Verify intl API routing contract")
    if runtime_verify_index < tencent_names.index("Verify intl public release provenance"):
        raise AssertionError(
            "intl API routing must be checked after release provenance converges"
        )
    runtime_verify_step = step_by_name(
        tencent_steps,
        "Verify intl API routing contract",
    )
    runtime_verify_command = str(runtime_verify_step.get("run") or "")
    if "verify_intl_runtime_contract.py" not in runtime_verify_command:
        raise AssertionError("intl runtime verification must use the fail-closed helper")
    www_runtime_step = step_by_name(
        tencent_steps,
        "Verify www runtime API contract",
    )
    www_runtime_command = str(www_runtime_step.get("run") or "")
    if (
        "verify_intl_runtime_contract.py" not in www_runtime_command
        or "--profile www" not in www_runtime_command
    ):
        raise AssertionError("www runtime verification must use the compatible JSON profile")
    if tencent_names.index("Verify www runtime API contract") > tencent_names.index(
        "Record www-verified candidate checkpoint"
    ):
        raise AssertionError("www runtime APIs must pass before www_verified is recorded")

    audit_needs = job(workflow, "audit_frontend_parity").get("needs")
    if audit_needs != [COORDINATION_JOB, BUILD_JOB, "deploy_tencent"]:
        raise AssertionError("parity audit must wait for the single production deployment job")


def assert_tencent_resumable_upload_contract(workflow: Mapping[str, Any]) -> None:
    concurrency = mapping(workflow.get("concurrency"), "production concurrency")
    if concurrency.get("group") != "production-release-main":
        raise AssertionError("production releases must share one serialization group")
    if concurrency.get("cancel-in-progress") != "false":
        raise AssertionError("an active production release must not be cancelled by a newer push")

    tencent = job(workflow, "deploy_tencent")
    try:
        timeout_minutes = int(str(tencent.get("timeout-minutes") or "0"))
    except ValueError as exc:
        raise AssertionError("deploy_tencent timeout-minutes must be numeric") from exc
    if timeout_minutes < 120:
        raise AssertionError("deploy_tencent needs at least 120 minutes for a slow resumable upload")

    upload_steps = [
        step
        for step in steps(tencent, "deploy_tencent")
        if step.get("name") == "Upload complete release archive with incremental rsync"
    ]
    if len(upload_steps) != 1:
        raise AssertionError("Tencent must have exactly one fail-closed archive upload step")
    upload = str(upload_steps[0].get("run") or "")
    required_tokens = (
        'remote_dir=".cache/jato-releases/archives/${GITHUB_SHA}"',
        'remote_archive="${remote_dir}/${archive_sha256}.tar.gz"',
        'remote_temp="${remote_archive}.partial"',
        'remote_lock="${remote_archive}.lock"',
        "missing_packages+=(rsync)",
        "missing_packages+=(sshpass)",
        'sudo apt-get install -y "${missing_packages[@]}"',
        "require_rsync_3",
        "Remote rsync >= 3.0 is required",
        "--partial",
        "gzip -n --rsyncable",
        "sudo -n realpath /opt/jato/active",
        "ALLOW_FULL_UPLOAD_BOOTSTRAP",
        'GITHUB_EVENT_NAME" != "workflow_dispatch',
        'RELEASE_MODE" != "prepare-candidate',
        "basis_kind='retained'",
        "basis_kind='bootstrap'",
        "Explicit full-upload bootstrap authorized",
        "NO_BASIS",
        "refusing full upload",
        "--checksum",
        "--stats",
        "--protect-args",
        "--rsync-path=",
        "flock -w 870",
        "df -Pk",
        "required_bytes",
        "command -v flock",
        "local remote_output",
        'printf \'%s\' "$remote_output"',
        "reset_bad_partial()",
        "partial_reset_used",
        "rm -f '$remote_temp' '$remote_checksum'",
        "Remote immutable archive exists but size or SHA-256 is wrong",
        "FINAL_BAD",
        "PARTIAL_BAD",
        "REUSE",
        "SEALED",
        "test ! -e '$remote_archive'",
        "sha256sum '$remote_temp'",
        "ln '$remote_temp' '$remote_archive'",
        'echo "archive-bytes=$archive_bytes"',
        'echo "archive-sha256=$archive_sha256"',
        'echo "literal-bytes=$literal_bytes"',
        'echo "bootstrap-used=$bootstrap_used"',
        "Bootstrap transfer did not report the exact full archive byte count",
    )
    missing = [token for token in required_tokens if token not in upload]
    if missing:
        raise AssertionError(f"Tencent resumable upload contract is incomplete: {missing}")
    forbidden_tokens = (
        "StrictHostKeyChecking=no",
        "UserKnownHostsFile=/dev/null",
        "tail -c",
        "head -c",
        "oflag=seek_bytes",
        "cat >> '$remote_temp'",
        "fallback to sparse",
        "split -b 8M",
        "--compress",
        "--append-verify",
        " -z",
    )
    forbidden = [token for token in forbidden_tokens if token in upload]
    if forbidden:
        raise AssertionError(f"Tencent rsync upload retains unsafe tokens: {forbidden}")

    size_check = upload.rfind(r"[ \"\$partial_size\" != '$archive_bytes' ]")
    sha_check = upload.rfind(r"[ \"\$partial_sha\" != '$archive_sha256' ]")
    no_overwrite = upload.rfind("test ! -e '$remote_archive'")
    atomic_seal = upload.rfind("ln '$remote_temp' '$remote_archive'")
    if not (0 <= size_check <= sha_check < no_overwrite < atomic_seal):
        raise AssertionError(
            "Tencent finalization must verify exact size and SHA before no-clobber sealing"
        )

    tencent_steps = steps(tencent, "deploy_tencent")
    auth = step_by_name(tencent_steps, "Validate Tencent deploy credentials")
    auth_text = str(auth)
    for token in (
        "SSH_KNOWN_HOSTS",
        "StrictHostKeyChecking=yes",
    ):
        if token not in auth_text and token not in upload:
            raise AssertionError(f"Tencent SSH host pinning is missing {token}")
    deploy = str(step_by_name(tencent_steps, "Deploy verified release on Tencent").get("run") or "")
    if "StrictHostKeyChecking=yes" not in deploy or "UserKnownHostsFile=" not in deploy:
        raise AssertionError("Tencent deployment must enforce the pinned known_hosts file")
    if "remote_exports" in deploy or '"${remote_exports}bash -s"' in deploy:
        raise AssertionError("deployment secrets must not be exposed in remote command argv")
    if '"umask 077; exec bash -s" < "$control_payload"' not in deploy:
        raise AssertionError("deployment must send a mode-0600 control payload over SSH stdin")
    remote_timeout = re.search(
        r'timeout\s+([0-9]+)s\s+"\$\{ssh_command\[@\]\}"',
        deploy,
    )
    controller_text = BLUEGREEN_RELEASE_PATH.read_text(encoding="utf-8")
    controller_timeout = re.search(
        r'BLUEGREEN_CONTROLLER_TIMEOUT="\$\{BLUEGREEN_CONTROLLER_TIMEOUT:-([0-9]+)\}"',
        controller_text,
    )
    if remote_timeout is None or controller_timeout is None:
        raise AssertionError(
            "Tencent SSH/controller timeout budgets must be explicit and numeric"
        )
    remote_timeout_seconds = int(remote_timeout.group(1))
    controller_timeout_seconds = int(controller_timeout.group(1))
    if remote_timeout_seconds < controller_timeout_seconds + 600:
        raise AssertionError(
            "Tencent SSH timeout must exceed the persistent controller budget "
            "by at least 600 seconds"
        )


def assert_deterministic_backend_package(workflow: Mapping[str, Any]) -> None:
    tencent_steps = steps(job(workflow, "deploy_tencent"), "deploy_tencent")
    package = str(
        step_by_name(
            tencent_steps,
            "Package backend release with verified frontend artifact",
        ).get("run")
        or ""
    )
    required_tokens = (
        '"releaseId": release["releaseId"]',
        '"workflowRunAttempt": release["workflowRunAttempt"]',
        '"packagedAt": release["buildTimestamp"]',
        'release-source-date-epoch',
        "--sort=name",
        "--owner=0",
        "--group=0",
        "--numeric-owner",
        '--mtime="@$source_date_epoch"',
        "--mode='u=rwX,go=rX'",
        "--mode='u=rwX,go=X'",
        "--exclude='06_AppPlatform/backend/*.parquet'",
        "--no-acls",
        "--no-xattrs",
        "--no-selinux",
        'gzip -n --rsyncable -f "$RUNNER_TEMP/JATO_deploy.tar"',
        'value.get("localPath")',
        "missing MSRP localPath evidence",
        'tar tzf "$RUNNER_TEMP/JATO_deploy.tar.gz" "$evidence_path"',
    )
    missing = [token for token in required_tokens if token not in package]
    if missing:
        raise AssertionError(f"deterministic backend package is incomplete: {missing}")
    forbidden = (
        "GITHUB_RUN_ATTEMPT']}",
        "dt.datetime.now",
        "update_mihomo_subscription.sh",
    )
    found = [token for token in forbidden if token in package]
    if found:
        raise AssertionError(f"backend package retains rerun-varying input: {found}")
    normalized_package = " ".join(package.replace("\\\n", " ").split())
    private_contract_tokens = (
        'private_dirs_file="$RUNNER_TEMP/release-private-dirs.bin"',
        'private_files_file="$RUNNER_TEMP/release-private-files.bin"',
        'PRIVATE_DIRS_FILE="$private_dirs_file"',
        'PRIVATE_FILES_FILE="$private_files_file"',
        'Path(os.environ["PRIVATE_DIRS_FILE"]).write_bytes(',
        'Path(os.environ["PRIVATE_FILES_FILE"]).write_bytes(',
        'str(path).encode("utf-8") + b"\\0"',
        "cursor = relative.parent",
        'while cursor != PurePosixPath("."):',
        "if cursor == prefix or prefix in cursor.parents:",
        "directories.add(cursor)",
        "if cursor == prefix:",
        "cursor = cursor.parent",
        'mapfile -d \'\' -t private_dirs < "$private_dirs_file"',
        'mapfile -d \'\' -t private_files < "$private_files_file"',
        'tar "${tar_private_normalize[@]}" --no-recursion -rf '
        '"$RUNNER_TEMP/JATO_deploy.tar" -C "$GITHUB_WORKSPACE" '
        '"${private_dirs[@]}"',
        'tar "${tar_private_normalize[@]}" -rf '
        '"$RUNNER_TEMP/JATO_deploy.tar" -C "$GITHUB_WORKSPACE" '
        '"${private_files[@]}"',
    )
    missing_private_contract = [
        token
        for token in private_contract_tokens
        if token not in normalized_package
    ]
    if missing_private_contract:
        raise AssertionError(
            "private release asset manifest is incomplete: "
            f"{missing_private_contract}",
        )
    parent_chain_order = (
        "cursor = relative.parent",
        'while cursor != PurePosixPath("."):',
        "if cursor == prefix or prefix in cursor.parents:",
        "directories.add(cursor)",
        "if cursor == prefix:",
        "break",
        "cursor = cursor.parent",
        'Path(os.environ["PRIVATE_DIRS_FILE"]).write_bytes(',
    )
    parent_chain_positions = [
        normalized_package.index(token)
        for token in parent_chain_order
    ]
    if parent_chain_positions != sorted(parent_chain_positions):
        raise AssertionError(
            "private release files must enumerate every reviewed-prefix "
            "parent before publishing the directory manifest",
        )
    private_order = (
        'private_dirs_file="$RUNNER_TEMP/release-private-dirs.bin"',
        'Path(os.environ["PRIVATE_DIRS_FILE"]).write_bytes(',
        'Path(os.environ["PRIVATE_FILES_FILE"]).write_bytes(',
        'mapfile -d \'\' -t private_dirs < "$private_dirs_file"',
        'mapfile -d \'\' -t private_files < "$private_files_file"',
        'tar "${tar_private_normalize[@]}" --no-recursion -rf '
        '"$RUNNER_TEMP/JATO_deploy.tar"',
        'tar "${tar_private_normalize[@]}" -rf '
        '"$RUNNER_TEMP/JATO_deploy.tar"',
        'gzip -n --rsyncable -f "$RUNNER_TEMP/JATO_deploy.tar"',
    )
    private_positions = [
        normalized_package.index(token)
        for token in private_order
    ]
    if private_positions != sorted(private_positions):
        raise AssertionError(
            "private NUL manifests, explicit parent directories and files "
            "must be appended before deterministic compression",
        )
    if normalized_package.count(
        'str(path).encode("utf-8") + b"\\0"',
    ) < 2:
        raise AssertionError(
            "private directory and file manifests must both remain "
            "NUL-delimited",
        )


def assert_release_checkpoint_contract(workflow: Mapping[str, Any]) -> None:
    tencent_steps = steps(job(workflow, "deploy_tencent"), "deploy_tencent")
    names = [str(step.get("name") or "") for step in tencent_steps]
    required_order = (
        "Record transport-verified candidate checkpoint",
        "Deploy verified release on Tencent",
        "Fetch and attest server release checkpoint",
        "Verify Tencent public release provenance",
        "Verify www runtime API contract",
        "Record www-verified candidate checkpoint",
        "Check whether intl already serves the immutable release",
        "Record intl deployment ambiguity boundary",
        "Deploy downloaded dist to Cloudflare Pages",
        "Verify intl public release provenance",
        "Verify intl API routing contract",
        "Record intl-verified candidate checkpoint",
        "Retain candidate release checkpoint",
    )
    indexes = [names.index(name) for name in required_order]
    if indexes != sorted(indexes):
        raise AssertionError("release checkpoint and deploy steps are out of order")
    commands = combined_run(job(workflow, "deploy_tencent"), "deploy_tencent")
    for phase in ("transport_verified", "www_verified", "intl_deploy_started", "intl_verified"):
        if f"--phase {phase}" not in commands:
            raise AssertionError(f"candidate checkpoint is missing phase {phase}")
    if "release-candidate-${{ github.sha }}-${{ github.run_attempt }}" not in str(
        job(workflow, "deploy_tencent")
    ):
        raise AssertionError("candidate checkpoint must be retained per deploy attempt")
    candidate_upload = step_by_name(tencent_steps, "Retain candidate release checkpoint")
    candidate_with = mapping(candidate_upload.get("with"), "candidate receipt upload with")
    expected_candidate_artifact = {
        "name": "release-candidate-${{ github.sha }}-${{ github.run_attempt }}",
        "path": "${{ runner.temp }}/release-checkpoint",
        "if-no-files-found": "error",
        "compression-level": "0",
        "overwrite": "false",
        "retention-days": "30",
    }
    for key, expected in expected_candidate_artifact.items():
        if candidate_with.get(key) != expected:
            raise AssertionError(
                f"candidate checkpoint artifact {key} must be {expected!r}"
            )
    server_attestation = step_by_name(
        tencent_steps,
        "Fetch and attest server release checkpoint",
    )
    server_attestation_run = str(server_attestation.get("run") or "")
    required_attestation_tokens = (
        "StrictHostKeyChecking=yes",
        'UserKnownHostsFile="$HOME/.ssh/known_hosts"',
        "backend-healthy.json",
        "backend-healthy.evidence.json",
        "attestation_complete=false",
        "record_checkpoint_fetch_exit",
        'trap record_checkpoint_fetch_exit EXIT',
        '"$server_dir/fetch-status.json"',
        '"backendHealthyAttested"',
        '"candidate_ready"',
        '"backend_healthy"',
        '"inspect_then_resume"',
        '"$RELEASE_MODE"',
        'http://127.0.0.1:18002/candidate-preview.json',
        'preview.get("archiveSha256") != archive_sha256',
        '"candidateSlot") not in {8000, 8001}',
        '"approvalHandoff"',
        '"remoteArchivePath"',
        '"frontendGithubArtifactId"',
        '"frontendGithubArtifactDigest"',
        'checkpoint.get("status") != "completed"',
        "server checkpoint evidence binding mismatch",
        'evidence.get("identity") != expected_identity',
        'echo "Server checkpoint/evidence attestation SHA-256: $attestation_sha256"',
        '"$server_dir/backend-healthy.json"',
        '"$RUNNER_TEMP/release-checkpoint/candidate.json"',
    )
    missing_attestation = [
        token for token in required_attestation_tokens if token not in server_attestation_run
    ]
    if missing_attestation:
        raise AssertionError(
            f"server checkpoint attestation is incomplete: {missing_attestation}"
        )
    if server_attestation.get("if") != (
        "${{ always() && steps.upload_release.outcome == 'success' }}"
    ):
        raise AssertionError(
            "server checkpoint evidence must be fetched even when deployment fails",
        )
    if 'rm -rf "$server_dir"' in server_attestation_run:
        raise AssertionError(
            "failed server attestation must retain raw checkpoint evidence",
        )
    if 'echo "attestation-sha256=$attestation_sha256"' in server_attestation_run:
        raise AssertionError(
            "server attestation SHA-256 must not cross jobs through GITHUB_OUTPUT"
        )
    deploy_outputs = job(workflow, "deploy_tencent").get("outputs")
    if (
        isinstance(deploy_outputs, dict)
        and "server_attestation_sha256" in deploy_outputs
    ):
        raise AssertionError(
            "server attestation SHA-256 must stay inside the immutable candidate artifact"
        )
    cloudflare = step_by_name(tencent_steps, "Deploy downloaded dist to Cloudflare Pages")
    cloudflare_condition = unwrap_expression(cloudflare.get("if"))
    if cloudflare_condition != (
        "env.RELEASE_MODE == 'prepare-and-switch' && "
        "steps.intl_current.outputs.current != 'true'"
    ):
        raise AssertionError("an already-current intl release must skip redeployment")
    cloudflare_run = str(cloudflare.get("run") or "")
    if "for deploy_attempt in 1 2 3" not in cloudflare_run:
        raise AssertionError("an outdated intl release must have three bounded deploy attempts")

    full_release_steps = (
        "Verify Tencent public release provenance",
        "Verify www runtime API contract",
        "Record www-verified candidate checkpoint",
        "Check whether intl already serves the immutable release",
        "Record intl deployment ambiguity boundary",
        "Verify intl public release provenance",
        "Verify intl API routing contract",
        "Record intl-verified candidate checkpoint",
    )
    for step_name in full_release_steps:
        release_step = step_by_name(tencent_steps, step_name)
        if unwrap_expression(release_step.get("if")) != (
            "env.RELEASE_MODE == 'prepare-and-switch'"
        ):
            raise AssertionError(
                f"{step_name} must be skipped for Candidate-only review"
            )

    approval = job(workflow, "approve_candidate_to_active")
    approval_steps = steps(approval, "approve_candidate_to_active")
    approval_names = [str(step.get("name") or "") for step in approval_steps]
    required_approval_order = (
        "Validate exact Candidate approval request",
        "Download exact Candidate handoff artifact",
        "Download exact reviewed frontend artifact",
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
        "Retain immutable www approval receipt",
    )
    approval_indexes = [approval_names.index(name) for name in required_approval_order]
    if approval_indexes != sorted(approval_indexes):
        raise AssertionError("Candidate approval steps are out of order")
    approval_text = str(approval)
    approval_commands = combined_run(approval, "approve_candidate_to_active")
    required_approval_tokens = (
        'test "$CONFIRM_WWW_ACTIVATION" = "true"',
        'test "$CANDIDATE_COMMIT_SHA" = "$GITHUB_SHA"',
        '"status": "completed"',
        '"conclusion": "success"',
        '"head_branch": "main"',
        "verify_candidate_handoff.py",
        "github_candidate_control.sh approve-candidate-to-active",
        'remote_evidence="$CANDIDATE_SERVER_EVIDENCE_PATH"',
        'binding.group(1) != expected_evidence_path',
        "automatic restore previous successful Active",
        "no success receipt is emitted",
        "github_candidate_control.sh restore-previous-active",
        'checkpoint.get("phase") != "active_updated"',
        'remote_journal_dir=".local/state/jato-production-release/journals/',
        '"$server_dir/active-updated.journal.jsonl"',
        'if len(events) < 3:',
        'started, verified, updated = events[-3:]',
        'started.get("phase") != "active_update_started"',
        'verified.get("phase") != "active_update_verified"',
        'verified.get("status") != "completed"',
        'verified.get("retryClass") != "inspect_then_resume"',
        'updated.get("phase") != "active_updated"',
        "canonical Active update journal tail/checkpoint mismatch",
        '--phase www_verified',
        'intl unchanged',
    )
    missing_approval = [
        token for token in required_approval_tokens if token not in approval_commands
    ]
    if missing_approval:
        raise AssertionError(
            f"Candidate approval contract is incomplete: {missing_approval}"
        )
    if (
        'release-candidate/journal.jsonl"' in approval_commands
        or 'release-candidate/journal.jsonl\'' in approval_commands
    ):
        raise AssertionError(
            "www receipt must not reuse the stale prepare-run journal"
        )
    failed_boundary = step_by_name(
        approval_steps,
        "Record failed approval restore boundary",
    )
    if unwrap_expression(failed_boundary.get("if")) != (
        "failure() && steps.approve_active.outcome == 'failure'"
    ):
        raise AssertionError("failed Candidate approval must retain a red restore boundary")
    approve_step = step_by_name(approval_steps, "Approve exact Candidate on Tencent")
    if approve_step.get("id") != "approve_active" or approve_step.get(
        "continue-on-error"
    ) not in (None, False, "false"):
        raise AssertionError("Candidate approval failure must never be masked as success")
    approve_env = mapping(approve_step.get("env"), "Candidate approval environment")
    if approve_env.get("CANDIDATE_VERIFIED_ENV") != (
        "${{ runner.temp }}/candidate-approval/verified.env"
    ):
        raise AssertionError("Candidate approval must consume the verified handoff env")
    mutation_recheck = step_by_name(
        approval_steps,
        "Revalidate frozen coordination plan immediately before Active mutation",
    )
    if approval_names.index(mutation_recheck["name"]) + 1 != approval_names.index(
        "Approve exact Candidate on Tencent"
    ):
        raise AssertionError("frozen plan must be rechecked immediately before Active mutation")
    mutation_command = str(mutation_recheck.get("run") or "")
    for required in (
        "release_coordination_guard.py",
        "verify-plan",
        '--main-sha "$GITHUB_SHA"',
        '"$RUNNER_TEMP/release-coordination-plan/release-coordination-plan.json"',
    ):
        if required not in mutation_command:
            raise AssertionError(
                f"pre-Active-mutation coordination check is missing {required}"
            )
    www_verify = step_by_name(
        approval_steps,
        "Verify www serves the exact approved Candidate",
    )
    if (
        www_verify.get("id") != "verify_www"
        or www_verify.get("continue-on-error") != "true"
    ):
        raise AssertionError(
            "www audit must expose its failure only to the automatic restore hook"
        )
    restore = step_by_name(
        approval_steps,
        "Restore previous Active after failed www audit",
    )
    if (
        restore.get("id") != "restore_previous_active"
        or unwrap_expression(restore.get("if"))
        != "steps.verify_www.outcome == 'failure'"
    ):
        raise AssertionError("failed www audit must automatically restore previous Active")
    fetch_active = step_by_name(
        approval_steps,
        "Fetch and attest Active update checkpoint",
    )
    if unwrap_expression(fetch_active.get("if")) != (
        "steps.verify_www.outcome == 'success'"
    ):
        raise AssertionError("Active update receipt must be fetched only after www audit")
    for forbidden in (
        "Package backend release",
        "Upload release to Tencent",
        "npm run build",
        "wrangler@",
        "pages deploy",
    ):
        if forbidden in approval_text:
            raise AssertionError(
                f"Candidate approval must not rebuild, reupload or publish intl: {forbidden}"
            )
    for download_name, expected_name in (
        (
            "Download exact Candidate handoff artifact",
            "release-candidate-${{ inputs.candidate_commit_sha }}-"
            "${{ inputs.candidate_prepare_run_attempt }}",
        ),
        (
            "Download exact reviewed frontend artifact",
            "frontend-dist-${{ inputs.candidate_commit_sha }}",
        ),
    ):
        download = step_by_name(approval_steps, download_name)
        download_with = mapping(download.get("with"), f"{download_name} with")
        if (
            download_with.get("name") != expected_name
            or download_with.get("run-id")
            != "${{ inputs.candidate_prepare_run_id }}"
            or download_with.get("github-token") != "${{ github.token }}"
        ):
            raise AssertionError(f"{download_name} lost exact prior-run binding")
    receipt = step_by_name(
        approval_steps,
        "Retain immutable www approval receipt",
    )
    receipt_with = mapping(receipt.get("with"), "www approval receipt with")
    if (
        receipt_with.get("overwrite") != "false"
        or receipt_with.get("retention-days") != "30"
    ):
        raise AssertionError("www approval receipt must be immutable for thirty days")

    cleanup = job(workflow, "cleanup_candidate")
    cleanup_steps = steps(cleanup, "cleanup_candidate")
    cleanup_names = [str(step.get("name") or "") for step in cleanup_steps]
    required_cleanup_order = (
        "Validate exact Candidate cleanup request",
        "Resolve Candidate cleanup handoff source",
        "Validate Tencent cleanup credentials",
        "Download exact Candidate cleanup handoff",
        "Download exact Candidate cleanup frontend",
        "Verify immutable Candidate cleanup handoff",
        "Capture canonical Candidate cleanup handoff",
        "Verify canonical Candidate cleanup handoff",
        "Capture unchanged Active identity before cleanup",
        "Clean exact Candidate on Tencent",
        "Fetch canonical Candidate cleanup receipt",
        "Verify Active identity and health remained unchanged",
        "Retain immutable Candidate cleanup receipt",
    )
    cleanup_indexes = [cleanup_names.index(name) for name in required_cleanup_order]
    if cleanup_indexes != sorted(cleanup_indexes):
        raise AssertionError("Candidate cleanup steps are out of order")
    cleanup_commands = combined_run(cleanup, "cleanup_candidate")
    required_cleanup_tokens = (
        'test "$CONFIRM_CANDIDATE_CLEANUP" = "true"',
        "Cleaning an exact reviewed prior-main Candidate",
        '"status": "completed"',
        'mode == "discard-candidate"',
        '{"success", "failure"}',
        'else {"success"}',
        'run.get("conclusion") not in allowed_conclusions',
        '"head_branch": "main"',
        'source = "canonical-server"',
        "capture-canonical-cleanup",
        "--canonical-server-bundle",
        "--cleanup-mode \"$RELEASE_MODE\"",
        "reviewed-candidate.json",
        "verify_candidate_handoff.py",
        'github_candidate_control.sh "$RELEASE_MODE"',
        '{"candidate_ready", "rollback_completed"}',
        '"release-candidate": ({"active_updated"}, "candidate_released")',
        'previous.get("phase") not in predecessors',
        "Candidate cleanup journal identity/sequence mismatch",
        "Candidate cleanup journal tail/checkpoint mismatch",
        'before_source = before.get("source")',
        'before_source != after_source',
        "Active identity and health remained unchanged",
        "intl was not modified",
    )
    missing_cleanup = [
        token for token in required_cleanup_tokens if token not in cleanup_commands
    ]
    if 'test "$CANDIDATE_COMMIT_SHA" = "$GITHUB_SHA"' in cleanup_commands:
        raise AssertionError(
            "Candidate cleanup must remain possible after main advances",
        )
    if missing_cleanup:
        raise AssertionError(
            f"Candidate cleanup contract is incomplete: {missing_cleanup}"
        )
    if "Require exact intl artifact before releasing Candidate" in cleanup_names:
        raise AssertionError("intl synchronization must not block Candidate cleanup")
    for forbidden in (
        "npm run build",
        "Package backend release",
        "Upload complete release archive",
        "pages deploy",
    ):
        if forbidden in str(cleanup):
            raise AssertionError(
                f"Candidate cleanup must not build, upload, or publish intl: {forbidden}"
            )
    for download_name, expected_name in (
        (
            "Download exact Candidate cleanup handoff",
            "release-candidate-${{ inputs.candidate_commit_sha }}-"
            "${{ inputs.candidate_prepare_run_attempt }}",
        ),
        (
            "Download exact Candidate cleanup frontend",
            "frontend-dist-${{ inputs.candidate_commit_sha }}",
        ),
    ):
        download = step_by_name(cleanup_steps, download_name)
        download_with = mapping(download.get("with"), f"{download_name} with")
        if (
            download_with.get("name") != expected_name
            or download_with.get("run-id")
            != "${{ inputs.candidate_prepare_run_id }}"
            or download_with.get("github-token") != "${{ github.token }}"
        ):
            raise AssertionError(f"{download_name} lost exact prior-run binding")
        if download.get("if") != (
            "${{ steps.cleanup_handoff.outputs.source == 'github-artifact' }}"
        ):
            raise AssertionError(
                f"{download_name} must only consume a live GitHub handoff"
            )
    normal_verify = step_by_name(
        cleanup_steps,
        "Verify immutable Candidate cleanup handoff",
    )
    canonical_capture = step_by_name(
        cleanup_steps,
        "Capture canonical Candidate cleanup handoff",
    )
    canonical_verify = step_by_name(
        cleanup_steps,
        "Verify canonical Candidate cleanup handoff",
    )
    if normal_verify.get("if") != (
        "${{ steps.cleanup_handoff.outputs.source == 'github-artifact' }}"
    ):
        raise AssertionError("normal Candidate cleanup must retain the artifact path")
    for canonical_step in (canonical_capture, canonical_verify):
        if canonical_step.get("if") != (
            "${{ steps.cleanup_handoff.outputs.source == 'canonical-server' }}"
        ):
            raise AssertionError(
                "canonical Candidate cleanup must require the server handoff source"
            )
    cleanup_receipt = step_by_name(
        cleanup_steps,
        "Retain immutable Candidate cleanup receipt",
    )
    cleanup_receipt_with = mapping(
        cleanup_receipt.get("with"),
        "Candidate cleanup receipt with",
    )
    if (
        cleanup_receipt_with.get("overwrite") != "false"
        or cleanup_receipt_with.get("retention-days") != "30"
    ):
        raise AssertionError(
            "Candidate cleanup receipt must be immutable for thirty days"
        )

    audit = job(workflow, "audit_frontend_parity")
    audit_steps = steps(audit, "audit_frontend_parity")
    audit_names = [str(step.get("name") or "") for step in audit_steps]
    candidate_download = step_by_name(
        audit_steps,
        "Download candidate release checkpoint",
    )
    candidate_download_with = mapping(
        candidate_download.get("with"),
        "candidate receipt download with",
    )
    for key in ("name", "path"):
        if candidate_download_with.get(key) != expected_candidate_artifact[key]:
            raise AssertionError(
                f"candidate checkpoint download {key} must match its upload"
            )
    if audit_names.index("Download candidate release checkpoint") >= audit_names.index(
        "Seal verified production release checkpoint"
    ):
        raise AssertionError("candidate checkpoint must be downloaded before sealing")
    audit_commands = combined_run(audit, "audit_frontend_parity")
    for phase in ("parity_verified", "complete"):
        if f"--phase {phase}" not in audit_commands:
            raise AssertionError(f"verified receipt is missing phase {phase}")
    if (
        'actual_attestation_sha256="$(sha256sum "$attestation"' not in audit_commands
        or '--message "server_attestation_sha256=$actual_attestation_sha256"'
        not in audit_commands
    ):
        raise AssertionError(
            "complete receipt must hash and bind the artifact-contained server attestation"
        )
    audit_text = str(audit)
    required_receipt_identity_tokens = (
        "release_checkpoint.py show",
        'candidate_identity != attestation.get("identity")',
        'candidate_identity.get("archiveBytes")',
        'candidate_identity.get("archiveSha256")',
        '--archive-bytes "$archive_bytes"',
        '--archive-sha256 "$archive_sha256"',
    )
    missing_receipt_identity = [
        token for token in required_receipt_identity_tokens if token not in audit_commands
    ]
    if missing_receipt_identity:
        raise AssertionError(
            "verified receipt must reuse the attested candidate identity: "
            f"{missing_receipt_identity}"
        )
    for masked_output in (
        "needs.deploy_tencent.outputs.archive_bytes",
        "needs.deploy_tencent.outputs.archive_sha256",
        "needs.deploy_tencent.outputs.server_attestation_sha256",
    ):
        if masked_output in audit_text:
            raise AssertionError(
                f"verified receipt must not depend on secret-maskable output {masked_output}"
            )
    if "release-verified-${{ github.sha }}-${{ github.run_attempt }}" not in audit_text:
        raise AssertionError("final parity must retain a verified production receipt")
    verified_upload = step_by_name(
        steps(audit, "audit_frontend_parity"),
        "Retain verified production release receipt",
    )
    verified_with = mapping(verified_upload.get("with"), "verified receipt upload with")
    if verified_with.get("retention-days") != "30":
        raise AssertionError("verified receipt must be retained for thirty days")

def assert_server_consumes_only_prebuilt_dist() -> None:
    remote_release = REMOTE_RELEASE_PATH.read_text(encoding="utf-8")
    candidate_control = CANDIDATE_CONTROL_PATH.read_text(encoding="utf-8")
    archive_validator = ARCHIVE_VALIDATOR_PATH.read_text(encoding="utf-8")
    production_workflow = PRODUCTION_WORKFLOW_PATH.read_text(encoding="utf-8")
    server_release = SERVER_RELEASE_PATH.read_text(encoding="utf-8")
    bluegreen_release_path = (
        REPO_ROOT / "03_Scripts/deploy/tencent_bluegreen_release.sh"
    )
    if not bluegreen_release_path.is_file():
        raise AssertionError("Tencent blue/green release controller is missing")
    bluegreen_release = bluegreen_release_path.read_text(encoding="utf-8")
    for path, script in (
        (REMOTE_RELEASE_PATH, remote_release),
        (SERVER_RELEASE_PATH, server_release),
    ):
        for forbidden in FORBIDDEN_BUILD_COMMANDS:
            if forbidden in script:
                raise AssertionError(f"{path} contains forbidden build command {forbidden!r}")
    for required in (
        "approve-candidate-to-active|discard-candidate|release-candidate|"
        "restore-previous-active",
        "capture-canonical-cleanup",
        'source "$CANDIDATE_VERIFIED_ENV"',
        'write_remote_export DEPLOY_APPROVAL_RUN_ID "$GITHUB_RUN_ID"',
        "DEPLOY_CANDIDATE_ATTESTATION_SHA256",
        "CANDIDATE_SERVER_CHECKPOINT_PATH",
        "CANDIDATE_SERVER_CHECKPOINT_SHA256",
        "CANDIDATE_SERVER_EVIDENCE_PATH",
        "CANDIDATE_SERVER_EVIDENCE_SHA256",
        "DEPLOY_CANDIDATE_SERVER_CHECKPOINT_PATH",
        "DEPLOY_CANDIDATE_SERVER_CHECKPOINT_SHA256",
        "DEPLOY_CANDIDATE_SERVER_EVIDENCE_PATH",
        "DEPLOY_CANDIDATE_SERVER_EVIDENCE_SHA256",
        'write_remote_export DEPLOY_CANDIDATE_HANDOFF_SOURCE',
        'write_remote_export DEPLOY_CANDIDATE_SLOT',
        'write_remote_export DEPLOY_CANDIDATE_PREVIEW_PORT',
        "fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)",
        '"http://127.0.0.1:18002/candidate-preview.json"',
        "hash_regular(",
        "Candidate checkpoint journal",
        "reconstructed_attestation_sha256 != requested_attestation",
        '"source": "reconstructed-from-canonical-journal"',
        'cat "$outer_release" >> "$control_payload"',
    ):
        if required not in candidate_control:
            raise AssertionError(
                f"Candidate SSH control lost exact cached-artifact binding: {required}"
            )
    for forbidden in FORBIDDEN_BUILD_COMMANDS + ("rsync", "pages deploy"):
        if forbidden in candidate_control:
            raise AssertionError(
                f"Candidate control must not build, upload or publish intl: {forbidden}"
            )
    attested_server_state_tokens = (
        "verify_attested_candidate_paths_and_evidence",
        "verify_attested_candidate_checkpoint_for_mode",
        'DEPLOY_CANDIDATE_SERVER_CHECKPOINT_PATH" != "$CHECKPOINT_FILE',
        'DEPLOY_CANDIDATE_SERVER_EVIDENCE_PATH" != "$CHECKPOINT_EVIDENCE_FILE',
        'actual_evidence_sha256" != "$DEPLOY_CANDIDATE_SERVER_EVIDENCE_SHA256',
        "approve-candidate-to-active:candidate_ready) "
        "require_original_checkpoint=true",
        "discard-candidate:candidate_ready) require_original_checkpoint=true",
        'actual_checkpoint_sha256" != "$DEPLOY_CANDIDATE_SERVER_CHECKPOINT_SHA256',
    )
    missing_attested_server_state = [
        token for token in attested_server_state_tokens if token not in remote_release
    ]
    if missing_attested_server_state:
        raise AssertionError(
            "Candidate mutation lost exact attested server state binding: "
            f"{missing_attested_server_state}"
        )
    lock_index = remote_release.index("flock -w 300 9")
    checkpoint_path_index = remote_release.index('CHECKPOINT_FILE="$DEPLOY_STATE_DIR')
    evidence_binding_index = remote_release.index(
        "verify_attested_candidate_paths_and_evidence\n",
        checkpoint_path_index,
    )
    checkpoint_state_index = remote_release.index(
        'CHECKPOINT_STATE="$(python3 "$CHECKPOINT_HELPER" show',
        evidence_binding_index,
    )
    checkpoint_binding_index = remote_release.index(
        "verify_attested_candidate_checkpoint_for_mode\n",
        checkpoint_state_index,
    )
    bluegreen_handoff_index = remote_release.index(
        'bash "$RELEASE_WORKTREE/03_Scripts/deploy/tencent_bluegreen_release.sh"',
        checkpoint_binding_index,
    )
    if not (
        lock_index
        < checkpoint_path_index
        < evidence_binding_index
        < checkpoint_state_index
        < checkpoint_binding_index
        < bluegreen_handoff_index
    ):
        raise AssertionError(
            "Candidate server evidence/checkpoint binding must run under the "
            "production lock before mutation"
        )


    for forbidden in ("fallback to sparse", "github_sparse_checkout", "sparse Git fetch"):
        if forbidden in remote_release:
            raise AssertionError(f"remote release retains forbidden fallback: {forbidden}")
    if 'python3 "$FRONTEND_RELEASE_HELPER" verify' not in remote_release:
        raise AssertionError("remote release must invoke the shared artifact verifier")
    if (
        "tar --same-permissions --no-overwrite-dir" not in remote_release
        or '-xzf "$SEALED_RELEASE_ARCHIVE" -C "$RELEASE_WORKTREE"'
        not in remote_release
    ):
        raise AssertionError(
            "remote release must restore normalized archive permissions while "
            "preserving the private extraction root",
        )
    for mode_guard in (
        "stat.S_IMODE(member.mode)",
        "unsafe release archive mode",
        "PRIVATE_PREFIXES",
        "trusted control provenance differs from immutable archive",
        "SCHEMA_VERSION = 2",
        "entry_types",
        "release archive member lacks an explicit directory",
        '"privateEntries"',
        "private_file_entries",
        "private_directory_entries",
    ):
        if mode_guard not in archive_validator:
            raise AssertionError(
                "same-permissions extraction requires schema-v2 archive "
                "mode, explicit-parent and private-entry checks",
            )
    sealed_archive_tokens = (
        'SEALED_TRUST_ROOT="/var/lib/jato-sealed-inputs"',
        'SEALED_ARCHIVE_ROOT="$SEALED_TRUST_ROOT/inputs"',
        'SEALED_RELEASE_ARCHIVE="$SEALED_ARCHIVE_DIR/release.tar.gz"',
        'SEALED_ARCHIVE_VALIDATOR="$SEALED_ARCHIVE_DIR/validate_release_archive.py"',
        'ARCHIVE_VALIDATION_RECEIPT_ROOT="$SEALED_TRUST_ROOT/receipts"',
        '${DEPLOY_RUN_ID}-${DEPLOY_RUN_ATTEMPT}.json',
        "seal_release_archive_input() {",
        "verify_sealed_release_bundle() {",
        "verify_archive_validation_receipt() {",
        "select_bluegreen_release_headroom_target() {",
        'sudo -n install -d -m 0750 -o root -g "$DEPLOY_GID"',
        'sudo -n install -m 0440 -o root -g "$DEPLOY_GID"',
        'sudo -n install -m 0550 -o root -g "$DEPLOY_GID"',
        '"0:${DEPLOY_GID}:750"',
        '"0:${DEPLOY_GID}:440"',
        'sudo -n python3 -B "$SEALED_ARCHIVE_VALIDATOR"',
        '--archive "$SEALED_RELEASE_ARCHIVE"',
        '--output "$ARCHIVE_VALIDATION_RECEIPT"',
        '--validation-run-id "$DEPLOY_RUN_ID"',
        '--validation-run-attempt "$DEPLOY_RUN_ATTEMPT"',
        '--sealed-root "$SEALED_TRUST_ROOT"',
        '--sealed-helper "$SEALED_ARCHIVE_VALIDATOR"',
        '--headroom-target "$RELEASE_WORKTREE" 1',
        '--headroom-target "$BLUEGREEN_HEADROOM_TARGET" 1',
        'sudo -n chmod 0444 "$ARCHIVE_VALIDATION_RECEIPT"',
    )
    missing_sealed_archive = [
        token for token in sealed_archive_tokens if token not in remote_release
    ]
    if missing_sealed_archive:
        raise AssertionError(
            "production release lost sealed archive, receipt or /tmp-/opt "
            f"headroom controls: {missing_sealed_archive}",
        )
    execution_start = remote_release.index(
        'verify_release_archive_identity "$RELEASE_ARCHIVE"',
    )
    seal_archive = remote_release.index(
        "\nseal_release_archive_input\n",
        execution_start,
    )
    validator_materialized = remote_release.index(
        'base64 --decode >"$TRUSTED_ARCHIVE_VALIDATOR_TEMP"',
        seal_archive,
    )
    helper_installed = remote_release.index(
        "sudo -n install -m 0550 -o root",
        validator_materialized,
    )
    bundle_verified = remote_release.index(
        "\nverify_sealed_release_bundle\n",
        helper_installed,
    )
    headroom_root = remote_release.index(
        "\nselect_bluegreen_release_headroom_target\n",
        bundle_verified,
    )
    single_validation = remote_release.index(
        'if ! sudo -n python3 -B "$SEALED_ARCHIVE_VALIDATOR"',
        headroom_root,
    )
    receipt_check = remote_release.index(
        "\nverify_archive_validation_receipt\n",
        single_validation,
    )
    extraction = remote_release.index(
        'tar --same-permissions --no-overwrite-dir',
        receipt_check,
    )
    post_extraction_receipt_check = remote_release.index(
        "\nverify_archive_validation_receipt\n",
        extraction,
    )
    if not (
        execution_start
        < seal_archive
        < validator_materialized
        < helper_installed
        < bundle_verified
        < headroom_root
        < single_validation
        < receipt_check
        < extraction
        < post_extraction_receipt_check
    ):
        raise AssertionError(
            "production must root-seal the archive/helper, create /opt safely, "
            "write one /tmp-/opt receipt, and reverify it around extraction",
        )
    if (
        remote_release.count(
            'sudo -n python3 -B "$SEALED_ARCHIVE_VALIDATOR"',
        )
        != 1
        or remote_release.count('--output "$ARCHIVE_VALIDATION_RECEIPT"') != 1
        or remote_release.count("--headroom-target") != 2
    ):
        raise AssertionError(
            "production archive validation must use one root-owned helper run "
            "and one immutable attempt receipt",
        )
    for shared_validator_token in (
        'RELEASE_ARCHIVE_VALIDATOR_B64',
        'RELEASE_ARCHIVE_VALIDATOR_SHA256',
        'base64 --decode >"$TRUSTED_ARCHIVE_VALIDATOR_TEMP"',
        '"03_Scripts/deploy/validate_release_archive.py=$SEALED_ARCHIVE_VALIDATOR"',
        'sudo -n python3 -B "$SEALED_ARCHIVE_VALIDATOR"',
    ):
        if shared_validator_token not in remote_release:
            raise AssertionError(
                "production outer release lost its pre-extraction trusted "
                f"archive validator: {shared_validator_token}",
            )
    for payload_token in (
        'archive_validator="03_Scripts/deploy/validate_release_archive.py"',
        'archive_validator_sha256="$(sha256sum "$archive_validator"',
        'archive_validator_b64="$(base64 -w 0 "$archive_validator")"',
        'write_remote_export RELEASE_ARCHIVE_VALIDATOR_B64',
        'write_remote_export RELEASE_ARCHIVE_VALIDATOR_SHA256',
    ):
        if payload_token not in production_workflow:
            raise AssertionError(
                "production workflow lost its private validator control payload: "
                f"{payload_token}",
            )
    if "03_Scripts/deploy/cleanup_toolkit_egg_info.py" not in remote_release:
        raise AssertionError(
            "remote release must carry the fail-closed toolkit metadata cleaner",
        )
    for fixed_active_file in (
        "03_Scripts/deploy/fixed_active_preimage.py",
        "03_Scripts/deploy/nginx/jato_candidate_preview.conf.example",
    ):
        if fixed_active_file not in remote_release:
            raise AssertionError(
                "remote release must require the fixed-Active approval asset: "
                f"{fixed_active_file}",
            )
    if '03_Scripts/deploy/release_evidence.py' not in remote_release:
        raise AssertionError("remote release must require the shared evidence verifier")
    if 'sudo -n "${verifier[@]}"' not in remote_release:
        raise AssertionError("remote release evidence must be verified with private-file access")
    if 'sudo -n python3 -B "$RELEASE_EVIDENCE_HELPER" verify' not in server_release:
        raise AssertionError("server recovery must reuse the privileged evidence verifier")
    for toolkit_guard in (
        "03_Scripts/deploy/cleanup_toolkit_egg_info.py",
        "cleanup_scraping_toolkit_egg_info",
        'python -m pip install -e "$TOOLKIT_DIR"',
    ):
        if toolkit_guard not in server_release:
            raise AssertionError(
                "server release must preserve editable toolkit behavior while "
                "cleaning generated source metadata",
            )
    if "python -m pip wheel" in server_release:
        raise AssertionError(
            "server release must not build the currently incomplete toolkit wheel",
        )
    if "--materialize-dir \"$PREBUILT_FRONTEND_DIR\"" not in remote_release:
        raise AssertionError("remote release must materialize only the verified artifact")
    handoff = (
        'bash "$RELEASE_WORKTREE/03_Scripts/deploy/'
        'tencent_bluegreen_release.sh"'
    )
    if handoff not in remote_release:
        raise AssertionError("production remote release must use Tencent blue/green")
    for candidate_mode_token in (
        'write_remote_export DEPLOY_BLUEGREEN_MODE "$RELEASE_MODE"',
        'http://127.0.0.1:18002/candidate-preview.json',
        "approve-candidate-to-active",
        "verify_candidate_handoff.py",
    ):
        if candidate_mode_token not in production_workflow:
            raise AssertionError(
                "production workflow lost Candidate review mode: "
                f"{candidate_mode_token!r}",
            )
    for fixed_role_controller_token in (
        'BLUEGREEN_MODE="${1:-}"',
        'prepare-and-switch|switch-locked)',
        'legacy physical slot switching is retired',
        'wait_for_slot_release_exact()',
    ):
        if fixed_role_controller_token not in bluegreen_release:
            raise AssertionError(
                "Tencent fixed-role controller lost its fail-closed mode contract: "
                f"{fixed_role_controller_token!r}",
            )
    for candidate_mode_token in (
        'DEPLOY_BLUEGREEN_MODE="${DEPLOY_BLUEGREEN_MODE:-prepare-candidate}"',
        (
            "prepare-candidate|approve-candidate-to-active|"
            "discard-candidate|release-candidate|restore-previous-active"
        ),
        'if [[ "$DEPLOY_BLUEGREEN_MODE" != "prepare-candidate" ]]; then',
        "DEPLOY_CANDIDATE_ATTESTATION_SHA256",
        'DEPLOY_CANDIDATE_HANDOFF_SOURCE:-}" == "canonical-server"',
        '"$DEPLOY_BLUEGREEN_MODE"',
    ):
        if candidate_mode_token not in remote_release:
            raise AssertionError(
                "production outer release lost Candidate mode validation: "
                f"{candidate_mode_token!r}",
            )
    if 'rm -rf "$REPO_DIR/$release_path"' in remote_release:
        raise AssertionError(
            "production remote release must not retain legacy live-tree mutation",
        )
    for forbidden in (
        "merge_previous_frontend_assets",
        'cp -p "$source" "$target"',
    ):
        if forbidden in bluegreen_release:
            raise AssertionError(
                "Tencent blue/green must not mutate the verified frontend "
                f"artifact with old-slot assets: {forbidden!r}",
            )
    handoff_exit = 'exit "$BLUEGREEN_RC"'
    if handoff_exit not in remote_release[remote_release.index(handoff) :]:
        raise AssertionError("Tencent blue/green handoff must terminate the outer verifier")
    if remote_release[remote_release.index(handoff_exit) + len(handoff_exit) :].strip():
        raise AssertionError(
            "production remote release must not retain a legacy deployment tail",
        )
    for token in (
        '"$python_bin" -B "$helper" hold',
        "--active-main-pid",
        "--expected-project-root",
        "--active-bundle-lock",
        "verify_candidate",
        "restore_previous_route",
        "rollback_completed",
        "BLUEGREEN_CANDIDATE_MEMORY_HIGH:-3G",
        "BLUEGREEN_CANDIDATE_MEMORY_MAX:-4G",
        "BLUEGREEN_ACTIVE_MEMORY_HIGH:-6G",
        "BLUEGREEN_ACTIVE_MEMORY_MAX:-8G",
        "Blue/green v1 forbids Alembic changes",
    ):
        if token not in bluegreen_release:
            raise AssertionError(
                f"Tencent blue/green release contract is missing {token!r}",
            )
    if "install_prebuilt_frontend" not in server_release:
        raise AssertionError("server release must atomically install the prebuilt dist")
    if 'mv "$PREBUILT_FRONTEND_DIR" "$target_dir"' not in server_release:
        raise AssertionError("server release is missing the same-filesystem atomic move")
    if "python -m alembic upgrade head" not in server_release:
        raise AssertionError("existing Alembic migration semantics must be preserved")
    if 'DEPLOY_BRANCH" != "main"' not in server_release:
        raise AssertionError("database migration must retain the main branch gate")
    if 'PRODUCTION_RELEASE_WORKFLOW" != "true"' not in server_release:
        raise AssertionError("database migration must require the production release workflow")
    inner_start = bluegreen_release.index("run_inner_prepare() {")
    inner_end = bluegreen_release.index("\n}\n", inner_start)
    inner_prepare = bluegreen_release[inner_start:inner_end]
    if "RUN_DATABASE_MIGRATIONS=verify_only" not in inner_prepare:
        raise AssertionError(
            "blue/green candidate preparation must use read-only DB verification",
        )
    if "default_transaction_read_only=on" not in server_release:
        raise AssertionError(
            "Alembic current must be protected by a database-level read-only setting",
        )


def assert_independent_current_active_intl_sync() -> None:
    workflow = load_workflow(INTL_SYNC_WORKFLOW_PATH)
    if workflow.get("name") != "sync-www-active-to-intl":
        raise AssertionError("independent intl sync workflow name changed")
    triggers = mapping(workflow.get("on"), "intl sync on")
    if set(triggers) != {"workflow_dispatch"}:
        raise AssertionError("intl sync must remain explicit workflow_dispatch only")
    dispatch = mapping(triggers.get("workflow_dispatch"), "intl sync dispatch")
    inputs = mapping(dispatch.get("inputs"), "intl sync inputs")
    if set(inputs) != {"confirm_sync_current_www_active"}:
        raise AssertionError("intl sync must not accept an arbitrary release SHA")
    confirmation = mapping(
        inputs.get("confirm_sync_current_www_active"),
        "intl sync confirmation",
    )
    if {
        "required": confirmation.get("required"),
        "default": confirmation.get("default"),
        "type": confirmation.get("type"),
    } != {"required": "true", "default": "false", "type": "boolean"}:
        raise AssertionError("intl sync confirmation contract changed")
    if workflow.get("permissions") != {"contents": "read"}:
        raise AssertionError("intl sync permissions are not least privilege")
    if workflow.get("concurrency") != {
        "group": "production-release-main",
        "cancel-in-progress": "false",
    }:
        raise AssertionError("intl sync must serialize with production release")
    jobs = mapping(workflow.get("jobs"), "intl sync jobs")
    if set(jobs) != {"sync_intl"}:
        raise AssertionError("intl sync must contain one independent job")
    sync_job = job(workflow, "sync_intl")
    if unwrap_expression(sync_job.get("if")) != MAIN_CONDITION:
        raise AssertionError("intl sync must use the exact main-only guard")
    if sync_job.get("environment") != "production" or "needs" in sync_job:
        raise AssertionError("intl sync must be production and Candidate-independent")
    sync_steps = steps(sync_job, "sync_intl")
    names = [str(step.get("name") or "") for step in sync_steps]
    required_order = (
        "Validate explicit current Active sync request",
        "Prove current www Active and download its embedded frontend artifact",
        "Verify and materialize the original Active frontend artifact",
        "Verify current public www exactly matches the Active artifact",
        "Check whether intl already serves the exact Active artifact",
        "Reconfirm current main and unchanged www Active before intl mutation",
        "Deploy original Active dist to Cloudflare Pages",
        "Audit exact intl provenance and API contract",
        "Create immutable intl sync receipt",
        "Retain immutable intl sync receipt",
    )
    indexes = [names.index(name) for name in required_order]
    if indexes != sorted(indexes):
        raise AssertionError("intl sync verification/deployment order changed")
    commands = combined_run(sync_job, "sync_intl")
    required_tokens = (
        'test "$CONFIRM_SYNC_CURRENT_WWW_ACTIVE" = "true"',
        'test "$current_main" = "$GITHUB_SHA"',
        "export_active_frontend_release.py",
        "verify_release_source_seal.py",
        "active-proof-before.json",
        'sudo -n cat -- %q',
        "active-proof-after-download.json",
        "verify-download",
        "--materialize-functions-dir",
        "pages functions build functions",
        "--profile www",
        "current=true",
        "current=false",
        "active-proof-before-deploy.json",
        "pages deploy",
        '--commit-hash "$ACTIVE_COMMIT_SHA"',
        '"jatoDataChanged": False',
    )
    missing = [
        token
        for token in required_tokens
        if token not in commands and token not in str(sync_job)
    ]
    if missing:
        raise AssertionError(f"intl sync contract is incomplete: {missing}")
    for forbidden in FORBIDDEN_BUILD_COMMANDS + (
        "release-candidate-",
        "candidate_prepare_run_id",
        "github_candidate_control.sh",
        "01_RAW_DATA",
        "04_Processed_data",
        "jato-monthly-worker",
    ):
        if forbidden in commands:
            raise AssertionError(f"intl sync contains forbidden behavior: {forbidden}")
    helper = (
        REPO_ROOT / "03_Scripts/deploy/export_active_frontend_release.py"
    ).read_text(encoding="utf-8")
    for required in (
        "legacy/non-content-addressed",
        'getattr(os, "O_NOFOLLOW", None)',
        "_stat_identity(before) != _stat_identity(after)",
        "return root, commit, archive_sha256",
        "Active runtime seal verification failed",
    ):
        if required not in helper:
            raise AssertionError(
                f"current Active export helper lost {required!r}"
            )
    receipt = step_by_name(sync_steps, "Retain immutable intl sync receipt")
    receipt_with = mapping(receipt.get("with"), "intl sync receipt with")
    if (
        receipt.get("uses") != "actions/upload-artifact@v4"
        or receipt_with.get("overwrite") != "false"
        or receipt_with.get("retention-days") != "30"
    ):
        raise AssertionError("intl sync receipt must be immutable for thirty days")


def assert_bluegreen_storage_guard_text_contract(
    remote_release: str,
    bluegreen_release: str,
    storage_guard: str,
    boot_reconcile: str,
) -> None:
    helper_path = "03_Scripts/deploy/jato_release_storage_guard.py"
    if helper_path not in remote_release:
        raise AssertionError(
            "production release archive must include the release storage guard",
        )

    required_controller_tokens = (
        helper_path,
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
        "guard_release_storage",
        "--expected-active-memory-high-bytes",
        "--expected-active-memory-max-bytes",
        "assert_runtime_storage_reserve",
        "materialize_release_source",
        "TOOLKIT_EGG_INFO_HELPER",
        ") | sudo -n tar --same-permissions --no-overwrite-dir",
        "run_candidate_build_scope",
        "build_candidate_runtime_locked",
        "--scope",
        '--property="MemoryHigh=$BLUEGREEN_CANDIDATE_MEMORY_HIGH"',
        '--property="MemoryMax=$BLUEGREEN_CANDIDATE_MEMORY_MAX"',
        '--property="TasksMax=512"',
        "install_slot_runtime",
    )
    missing_controller = [
        token for token in required_controller_tokens if token not in bluegreen_release
    ]
    if missing_controller:
        raise AssertionError(
            "Tencent blue/green storage contract is incomplete: "
            f"{missing_controller}",
        )
    materialize_start = bluegreen_release.index("materialize_release_source() {")
    materialize_end = bluegreen_release.index("\n}\n", materialize_start)
    materialize_source = bluegreen_release[materialize_start:materialize_end]
    try:
        cleanup_metadata = materialize_source.index(
            'python3 -B "$TOOLKIT_EGG_INFO_HELPER"',
        )
        stored_source_seal = materialize_source.index(
            'sudo -n test -L "$RELEASE_SOURCE_SEAL_FILE"',
            cleanup_metadata,
        )
        verify_stored_source = materialize_source.index(
            'python3 -B "$SOURCE_SEAL_HELPER" verify',
            stored_source_seal,
        )
    except ValueError as error:
        raise AssertionError(
            "persistent release retry must safely clean editable metadata "
            "before source-seal verification",
        ) from error
    if not cleanup_metadata < stored_source_seal < verify_stored_source:
        raise AssertionError(
            "toolkit metadata cleanup must precede persistent source-seal reuse",
        )
    build_start = bluegreen_release.index("run_candidate_build_scope() {")
    build_end = bluegreen_release.index("\n}\n", build_start)
    constrained_build = bluegreen_release[build_start:build_end]
    if "--wait" in constrained_build:
        raise AssertionError(
            "systemd-run scope is already synchronous and must not use --wait",
        )
    locked_build_start = bluegreen_release.index(
        "build_candidate_runtime_locked() {",
    )
    locked_build_end = bluegreen_release.index("\n}\n", locked_build_start)
    locked_build = bluegreen_release[locked_build_start:locked_build_end]
    try:
        scope_proof = locked_build.index("\n  assert_candidate_build_scope\n")
        lock_proof = locked_build.index(
            "\n  assert_inherited_production_lock\n",
        )
        runtime_prepare = locked_build.index("\n  prepare_candidate_runtime\n")
        inner_prepare = locked_build.index("\n    run_inner_prepare\n")
        source_verify = locked_build.index(
            "\n    verify_materialized_release_source\n",
        )
        database_gate = locked_build.index(
            "\n    assert_no_database_migration_delta\n",
        )
        status_write = locked_build.index(
            "\n    write_candidate_deploy_status\n",
        )
        final_runtime_seal = locked_build.index("\n    finalize_runtime_seal\n")
        final_runtime_verify = locked_build.index(
            "\n  verify_final_runtime_seal",
        )
    except ValueError as error:
        raise AssertionError(
            "candidate build scope proof and sealed build sequence are incomplete",
        ) from error
    if not (
        scope_proof
        < lock_proof
        < runtime_prepare
        < inner_prepare
        < source_verify
        < database_gate
        < status_write
        < final_runtime_seal
        < final_runtime_verify
    ):
        raise AssertionError(
            "candidate build must prove its cgroup and inherited lock before "
            "building, verifying the source and database, writing status, "
            "and sealing the runtime",
        )

    prepare = bluegreen_release[bluegreen_release.index("prepare_and_switch()") :]
    try:
        environment = prepare.index("\n  require_environment\n")
        inherited_lock = prepare.index(
            "\n  assert_inherited_production_lock\n",
        )
        state_root = prepare.index("\n  ensure_bluegreen_state_root\n")
        runtime_roots = prepare.index("\n  ensure_bluegreen_runtime_roots\n")
        preflight = prepare.index("\n  guard_release_storage\n")
        preflight_memory = prepare.index(
            "\n  assert_host_memory_budget\n",
            preflight,
        )
        materialize = prepare.index("\n  materialize_release_source\n")
        build_scope = prepare.index("\n  run_candidate_build_scope\n")
        final_seal = prepare.index("\n  verify_final_runtime_seal\n")
        database_gate = prepare.index(
            "\n  assert_no_database_migration_delta\n",
            final_seal,
        )
        post_seal = prepare.index("\n  assert_runtime_storage_reserve\n")
        runtime_memory = prepare.index(
            "\n  assert_host_memory_budget\n",
            post_seal,
        )
        install = prepare.index("\n  install_slot_runtime\n")
    except ValueError as error:
        raise AssertionError(
            "release storage and memory preflight calls are incomplete",
        ) from error
    if not (
        environment
        < inherited_lock
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
    ):
        raise AssertionError(
            "the inherited lock and 0755 roots must precede storage enforcement; "
            "the 5 GiB memory checks and constrained build scope must bracket "
            "materialization, follow the final seal, and precede candidate runtime "
            "installation",
        )

    forbidden_boot_tokens = (
        "jato_release_storage_guard",
        "guard_release_storage",
        "release_gc",
        "shutil.rmtree",
        "rm -rf",
    )
    retained_boot_tokens = [
        token for token in forbidden_boot_tokens if token in boot_reconcile
    ]
    if retained_boot_tokens:
        raise AssertionError(
            "boot reconciliation must remain non-GC and non-destructive: "
            f"{retained_boot_tokens}",
        )

    required_guard_tokens = (
        ".jato-release-identity",
        "cross_release_is_settled",
        "DANGEROUS_RETRY_CLASSES",
        "expected_repository",
        "read_active_frontend_root",
        "/proc/self/mountinfo",
        "os.replace",
        "os.fsync",
        ".gc-",
    )
    missing_guard = [
        token for token in required_guard_tokens if token not in storage_guard
    ]
    if missing_guard:
        raise AssertionError(
            "release storage guard is missing fail-closed safety primitives: "
            f"{missing_guard}",
        )

    forbidden_guard_tokens = (
        "rm -rf",
        "shutil.rmtree(releases_root)",
        "shutil.rmtree(args.releases_root)",
        "shutil.rmtree(jato_root)",
        'glob("*")',
        'rglob("*")',
    )
    retained_guard_tokens = [
        token for token in forbidden_guard_tokens if token in storage_guard
    ]
    if retained_guard_tokens:
        raise AssertionError(
            "release storage guard retains a broad deletion primitive: "
            f"{retained_guard_tokens}",
        )


def assert_bluegreen_storage_guard_contract() -> None:
    for path in (
        BLUEGREEN_RELEASE_PATH,
        BLUEGREEN_BOOT_RECONCILE_PATH,
        RELEASE_STORAGE_GUARD_PATH,
    ):
        if not path.is_file():
            raise AssertionError(f"required blue/green safety helper is missing: {path}")
    assert_bluegreen_storage_guard_text_contract(
        REMOTE_RELEASE_PATH.read_text(encoding="utf-8"),
        BLUEGREEN_RELEASE_PATH.read_text(encoding="utf-8"),
        RELEASE_STORAGE_GUARD_PATH.read_text(encoding="utf-8"),
        BLUEGREEN_BOOT_RECONCILE_PATH.read_text(encoding="utf-8"),
    )


def assert_prewarm_contract(production_name: str) -> None:
    prewarm = load_workflow(PREWARM_WORKFLOW_PATH)
    triggers = mapping(prewarm.get("on"), "prewarm on")
    if set(triggers) != {"workflow_run"}:
        raise AssertionError("prewarm must only be triggered by workflow_run")
    workflow_run = mapping(triggers.get("workflow_run"), "prewarm workflow_run")
    if sequence(workflow_run.get("workflows"), "prewarm workflows") != [production_name]:
        raise AssertionError("prewarm workflow_run name must match production workflow name")
    if sequence(workflow_run.get("types"), "prewarm types") != ["completed"]:
        raise AssertionError("prewarm must wait for completed production release")
    if sequence(workflow_run.get("branches"), "prewarm branches") != ["main"]:
        raise AssertionError("prewarm workflow_run must be main-only")

    prewarm_job = job(prewarm, "prewarm")
    if unwrap_expression(prewarm_job.get("if")) != PREWARM_CONDITION:
        raise AssertionError("prewarm must require completed success from main repository")
    prewarm_steps = steps(prewarm_job, "prewarm")
    names = [str(step.get("name") or "") for step in prewarm_steps]
    if names[:2] != [
        "Checkout completed production release",
        "Resolve completed production release action",
    ]:
        raise AssertionError(
            "prewarm must resolve the exact completed SHA's release action first"
        )
    hold_step = prewarm_steps[1]
    if hold_step.get("id") != "production_hold":
        raise AssertionError("prewarm hold resolver step ID changed")
    hold_command = str(hold_step.get("run") or "")
    for required in (
        PRODUCTION_HOLD_SCRIPT,
        "resolve",
        '--github-output "$GITHUB_OUTPUT"',
    ):
        if required not in hold_command:
            raise AssertionError(f"prewarm hold resolver is missing {required!r}")
    for step in prewarm_steps[2:]:
        if unwrap_expression(step.get("if")) != PREWARM_DEPLOY_CONDITION:
            raise AssertionError(
                "every prewarm artifact/provenance/cache step must use the exact "
                "release-action gate"
            )
    verify_name = "Verify completed immutable release and intl provenance"
    if names.index(verify_name) > names.index("Prewarm intl edge cache"):
        raise AssertionError("prewarm must verify release provenance before warming")
    commands = combined_run(prewarm_job, "prewarm")
    for forbidden in ("wrangler", "pages deploy", "scp ", "ssh "):
        if forbidden in commands:
            raise AssertionError(f"prewarm contains a deployment path: {forbidden!r}")
    if "actions/download-artifact@v5" not in str(prewarm_job):
        raise AssertionError("prewarm must consume the completed immutable artifact")
    if "frontend_release_artifact.py audit-public" not in commands:
        raise AssertionError("prewarm must verify public provenance")


def assert_backend_readiness_ci_contract(
    ci: Mapping[str, Any],
) -> None:
    backend_job = job(ci, "fullstack-backend")
    backend_steps = steps(backend_job, "fullstack-backend")
    api_contract_step = step_by_name(backend_steps, "API contract tests")
    if "if" in api_contract_step:
        raise AssertionError(
            "backend API and readiness test step must not be conditional"
        )
    if api_contract_step.get("working-directory") != "06_AppPlatform/backend":
        raise AssertionError(
            "backend readiness tests must run from the backend working directory"
        )

    command = str(api_contract_step.get("run") or "")
    pytest_invocations = [
        line
        for line in command.splitlines()
        if "python -m pytest" in line
    ]
    if len(pytest_invocations) != 1:
        raise AssertionError(
            "backend API and readiness tests must share one pytest invocation"
        )
    pytest_command = " ".join(command.split())
    pytest_match = re.search(
        (
            r'timeout 180s python -m pytest .*?'
            r'2>&1 \| tee "\$pytest_log" \|\| status=\$\?'
        ),
        pytest_command,
    )
    if pytest_match is None:
        raise AssertionError(
            "backend API and readiness tests must share the bounded pytest command"
        )
    bounded_pytest_command = pytest_match.group(0)
    required_tokens = (
        "tests/integration/test_api_contracts.py",
        "tests/unit/test_jato_monthly_update_enabled_gate.py",
        "tests/unit/test_readiness_service.py",
        '|| status=$?',
    )
    missing = [
        token
        for token in required_tokens
        if token not in bounded_pytest_command
    ]
    if 'exit "$status"' not in pytest_command:
        missing.append('exit "$status"')
    if missing:
        raise AssertionError(
            f"required backend readiness CI is incomplete: {missing}"
        )


def assert_required_ci_contract() -> None:
    ci = load_workflow(CI_WORKFLOW_PATH)
    assert_required_ci_jobs_fail_closed(ci)
    assert_pull_request_merge_result_checkout(ci)
    assert_backend_readiness_ci_contract(ci)

    contract_job = job(ci, "frontend-release-contract")
    contract_steps = steps(contract_job, "frontend-release-contract")
    setup_node = step_by_name(contract_steps, "Setup fixed edge contract Node")
    if setup_node.get("uses") != "actions/setup-node@v4":
        raise AssertionError("edge contract CI must use actions/setup-node@v4")
    setup_node_with = mapping(setup_node.get("with"), "edge contract setup-node with")
    if setup_node_with.get("node-version") != "20.19.0":
        raise AssertionError("edge contract CI must use the production Node version")

    commands = combined_run(contract_job, "frontend-release-contract")
    required_tokens = (
        "python -m pip install",
        '"PyYAML==6.0.2"',
        '"pytest<9"',
        '"pydantic==2.11.7"',
        '"setuptools==81.0.0"',
        '"wheel==0.46.3"',
        "npm ci",
        "test_frontend_release_artifact.py",
        "test_verify_intl_runtime_contract.py",
        "bash -n",
        "fullstack_remote_release.sh",
        "tencent_pre_switch_checkpoint_recovery.sh",
        "tencent_feature_candidate_canary.sh",
        "tencent_bluegreen_release.sh",
        "production_mutation_lock.sh",
        "enable_jato_fullstack_https.sh",
        "install_jato_fullstack_nginx.sh",
        "deploy_fullstack_server.sh",
        "backup_production_data.sh",
        "sync_data_to_cloud.sh",
        "sync_msrp_db_to_cloud.sh",
        "test_release_checkpoint.py",
        "test_release_evidence.py",
        "test_bluegreen_systemd_nginx_contract.py",
        "test_jato_bluegreen_boot_reconcile.py",
        "test_jato_quiescence_gate.py",
        "test_jato_release_storage_guard.py",
        "test_tencent_feature_candidate_canary.py",
        "test_tencent_bluegreen_release.py",
        "test_validate_release_archive.py",
        "test_cleanup_toolkit_egg_info.py",
        "test_release_source_seal.py",
        "test_pre_switch_checkpoint_recovery.py",
        "test_present_checkpoint_recovery_result.py",
        "test_release_shell_hardening.py",
        "test_deploy_workflow.py",
        "npx vitest run",
        "edgeCacheFunction.test.ts",
        "healthzEdgeFunction.test.ts",
    )
    missing = [token for token in required_tokens if token not in commands]
    if missing:
        raise AssertionError(f"required edge contract CI is incomplete: {missing}")

    production_guard = job(ci, "production-deployment-guard")
    production_guard_commands = combined_run(
        production_guard,
        "production-deployment-guard",
    )
    for required in (
        "validate_production_workflow_guards.py",
        "test_release_coordination_guard.py",
        "test_production_release_hold.py",
    ):
        if required not in production_guard_commands:
            raise AssertionError(
                f"required production guard CI is missing {required}"
            )


def main() -> None:
    if (REPO_ROOT / ".github/workflows/deploy-fullstack-tencent.yml").exists():
        raise AssertionError("legacy Tencent workflow must not coexist with production release")
    if (REPO_ROOT / ".github/workflows/deploy-cloudflare-pages-intl.yml").exists():
        raise AssertionError("legacy Cloudflare workflow must not coexist with production release")

    production = load_workflow(PRODUCTION_WORKFLOW_PATH)
    production_name = str(production.get("name") or "").strip()
    if production_name != "production-release":
        raise AssertionError("production workflow name must be production-release")
    assert_main_only_production_workflow(production)
    assert_production_hold_gate_contract(production)
    assert_single_build_and_strict_outputs(production)
    assert_deploy_jobs_share_one_artifact(production)
    assert_tencent_resumable_upload_contract(production)
    assert_deterministic_backend_package(production)
    assert_release_checkpoint_contract(production)
    assert_server_consumes_only_prebuilt_dist()
    assert_independent_current_active_intl_sync()
    assert_bluegreen_storage_guard_contract()
    assert_prewarm_contract(production_name)
    assert_required_ci_contract()
    print(
        "Validated immutable production release, shared artifact parity, "
        "artifact-bound edge functions, intl runtime routing, server no-build "
        "semantics, and main-only prewarm provenance."
    )


if __name__ == "__main__":
    main()
