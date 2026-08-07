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
CI_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/ci.yml"
INTL_SYNC_WORKFLOW_PATH = (
    REPO_ROOT / ".github/workflows/sync-www-active-to-intl.yml"
)
PREWARM_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/intl-edge-prewarm.yml"
FIXED_RELEASE_V2_REMOTE_PATH = (
    REPO_ROOT / "03_Scripts/deploy/fixed_release_v2_remote.sh"
)
RELEASE_V2_STORE_PATH = REPO_ROOT / "03_Scripts/deploy/release_v2_store.py"
RELEASE_V2_ADMISSION_PATH = (
    REPO_ROOT / "03_Scripts/deploy/release_v2_admission.py"
)
MAIN_CONDITION = "github.ref == 'refs/heads/main'"
PREPARE_RELEASE_CONDITION = (
    MAIN_CONDITION
    + " && inputs.release_mode == 'prepare-candidate'"
)
CONTROL_RELEASE_CONDITION = (
    MAIN_CONDITION
    + " && inputs.release_mode != 'prepare-candidate'"
)
PREWARM_CONDITION = (
    "github.event.workflow_run.conclusion == 'success' && "
    "github.event.workflow_run.head_branch == 'main' && "
    "github.event.workflow_run.head_repository.full_name == github.repository"
)
BUILD_JOB = "build_frontend"
COORDINATION_JOB = "release_coordination_guard"
PRODUCTION_ENVIRONMENT_JOBS = (
    "deploy_tencent",
    "control_fixed_release_v2",
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


def assert_fixed_v2_production_workflow(workflow: Mapping[str, Any]) -> None:
    """Validate the explicit Fixed V2 prepare/control workflow boundary."""
    triggers = mapping(workflow.get("on"), "production workflow on")
    if set(triggers) != {"workflow_dispatch"}:
        raise AssertionError(
            "production release must be explicit workflow_dispatch only"
        )
    dispatch = mapping(
        triggers.get("workflow_dispatch"),
        "production workflow workflow_dispatch",
    )
    dispatch_inputs = mapping(
        dispatch.get("inputs"),
        "production workflow workflow_dispatch.inputs",
    )
    expected_inputs = {
        "release_mode",
        "target_commit_sha",
        "target_archive_sha256",
        "target_manifest_sha256",
        "confirm_control_operation",
        "bootstrap_full_upload",
    }
    if set(dispatch_inputs) != expected_inputs:
        raise AssertionError("production Fixed V2 dispatch inputs changed")

    release_mode = mapping(
        dispatch_inputs.get("release_mode"),
        "production workflow release_mode input",
    )
    expected_actions = [
        "prepare-candidate",
        "discard-candidate",
        "update-active",
        "rollback-active",
    ]
    if (
        release_mode.get("required") != "true"
        or release_mode.get("type") != "choice"
        or release_mode.get("default") != "prepare-candidate"
        or sequence(release_mode.get("options"), "release_mode.options")
        != expected_actions
    ):
        raise AssertionError("production Fixed V2 action contract changed")
    for input_name in (
        "target_commit_sha",
        "target_archive_sha256",
        "target_manifest_sha256",
    ):
        value = mapping(
            dispatch_inputs.get(input_name),
            f"production workflow {input_name} input",
        )
        if value.get("required") != "false" or value.get("type") != "string":
            raise AssertionError(f"production {input_name} input contract changed")
    for input_name in ("confirm_control_operation", "bootstrap_full_upload"):
        value = mapping(
            dispatch_inputs.get(input_name),
            f"production workflow {input_name} input",
        )
        if {
            "required": value.get("required"),
            "type": value.get("type"),
            "default": value.get("default"),
        } != {"required": "true", "type": "boolean", "default": "false"}:
            raise AssertionError(f"production {input_name} input contract changed")

    workflow_env = mapping(workflow.get("env"), "production workflow env")
    if workflow_env.get("RELEASE_MODE") != "${{ inputs.release_mode }}":
        raise AssertionError("production RELEASE_MODE dispatch binding changed")
    if workflow.get("concurrency") != {
        "group": "production-release-main",
        "cancel-in-progress": "false",
    }:
        raise AssertionError("production release must retain one fail-closed lock")

    jobs = mapping(workflow.get("jobs"), "production workflow jobs")
    expected_jobs = {
        COORDINATION_JOB,
        BUILD_JOB,
        "deploy_tencent",
        "control_fixed_release_v2",
    }
    if set(jobs) != expected_jobs:
        raise AssertionError("production Fixed V2 job set changed")

    expected_conditions = {
        COORDINATION_JOB: MAIN_CONDITION,
        BUILD_JOB: PREPARE_RELEASE_CONDITION,
        "deploy_tencent": PREPARE_RELEASE_CONDITION,
        "control_fixed_release_v2": CONTROL_RELEASE_CONDITION,
    }
    for name, expected in expected_conditions.items():
        if unwrap_expression(job(workflow, name).get("if")) != expected:
            raise AssertionError(f"{name} lost its exact Fixed V2 main guard")
    if job(workflow, BUILD_JOB).get("needs") != COORDINATION_JOB:
        raise AssertionError("build_frontend must wait for coordination validation")
    if job(workflow, "deploy_tencent").get("needs") != [
        COORDINATION_JOB,
        BUILD_JOB,
    ]:
        raise AssertionError("deploy_tencent lost its guard/build dependency")
    if job(workflow, "control_fixed_release_v2").get("needs") != COORDINATION_JOB:
        raise AssertionError("Fixed V2 control lost its coordination dependency")
    for name in PRODUCTION_ENVIRONMENT_JOBS:
        if job(workflow, name).get("environment") != "production":
            raise AssertionError(f"{name} must retain the production environment")

    guard = job(workflow, COORDINATION_JOB)
    if "environment" in guard or "needs" in guard or "secrets." in str(guard):
        raise AssertionError(
            "coordination guard must run before approval without production secrets"
        )
    guard_steps = steps(guard, COORDINATION_JOB)
    if [step.get("name") for step in guard_steps] != [
        "Checkout release coordination guard",
        "Validate unpublished release coordination",
        "Freeze release coordination plan",
    ]:
        raise AssertionError("release coordination guard step contract changed")
    guard_checkout = guard_steps[0]
    guard_checkout_with = mapping(guard_checkout.get("with"), "guard checkout with")
    if (
        guard_checkout.get("uses") != "actions/checkout@v5"
        or guard_checkout_with.get("ref") != "${{ github.sha }}"
        or guard_checkout_with.get("persist-credentials") != "false"
    ):
        raise AssertionError("release coordination checkout must pin the dispatch SHA")
    guard_validate = guard_steps[1]
    guard_validate_env = mapping(
        guard_validate.get("env"),
        "release coordination validation env",
    )
    expected_target_env = {
        "TARGET_COMMIT_SHA": "${{ inputs.target_commit_sha }}",
        "TARGET_ARCHIVE_SHA256": "${{ inputs.target_archive_sha256 }}",
        "TARGET_MANIFEST_SHA256": "${{ inputs.target_manifest_sha256 }}",
    }
    for env_name, expected in expected_target_env.items():
        if guard_validate_env.get(env_name) != expected:
            raise AssertionError(
                f"release coordination {env_name} input binding changed"
            )
    guard_command = str(guard_validate.get("run") or "")
    for token in (
        "release_coordination_guard.py",
        "production",
        '--main-sha "$GITHUB_SHA"',
        '--operation "$RELEASE_MODE"',
        '--target-sha "$TARGET_COMMIT_SHA"',
        '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
        '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
    ):
        if token not in guard_command:
            raise AssertionError(
                f"release coordination validation is missing {token!r}"
            )

    for name in ("deploy_tencent", "control_fixed_release_v2"):
        deploy_steps = steps(job(workflow, name), name)
        checkout = deploy_steps[0]
        checkout_with = mapping(checkout.get("with"), f"{name} checkout with")
        if (
            checkout.get("uses") != "actions/checkout@v5"
            or checkout_with.get("ref") != "${{ github.sha }}"
            or checkout_with.get("persist-credentials") != "false"
        ):
            raise AssertionError(f"{name} must checkout the exact immutable SHA")
        revalidate_step = step_by_name(
            deploy_steps,
            "Revalidate frozen coordination plan after approval",
        )
        revalidate_env = mapping(
            revalidate_step.get("env"),
            f"{name} coordination revalidation env",
        )
        for env_name, expected in expected_target_env.items():
            if revalidate_env.get(env_name) != expected:
                raise AssertionError(f"{name} {env_name} input binding changed")
        revalidate = str(revalidate_step.get("run") or "")
        for token in (
            "release_coordination_guard.py",
            "verify-plan",
            '--main-sha "$GITHUB_SHA"',
            '--operation "$RELEASE_MODE"',
            '--target-sha "$TARGET_COMMIT_SHA"',
            '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
            '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
        ):
            if token not in revalidate:
                raise AssertionError(
                    f"{name} coordination revalidation is missing {token!r}"
                )


def assert_fixed_v2_prepare_and_control_contract(
    workflow: Mapping[str, Any],
) -> None:
    """Validate one immutable prepare path and the separate control path."""
    for path in (
        FIXED_RELEASE_V2_REMOTE_PATH,
        RELEASE_V2_STORE_PATH,
        RELEASE_V2_ADMISSION_PATH,
    ):
        if not path.is_file():
            raise AssertionError(f"required Fixed V2 helper is missing: {path}")
    deploy = job(workflow, "deploy_tencent")
    deploy_steps = steps(deploy, "deploy_tencent")
    names = [str(step.get("name") or "") for step in deploy_steps]
    required_order = (
        "Checkout release source",
        "Download frozen release coordination plan",
        "Revalidate frozen coordination plan after approval",
        "Download immutable frontend artifact by id",
        "Verify frontend artifact before Tencent deployment",
        "Validate Tencent deploy credentials",
        "Package backend release with verified frontend artifact",
        "Upload complete release archive with incremental rsync",
        "Generate canonical V2 release manifest",
        "Reconfirm current main before first production mutation",
        "Deploy verified release to fixed Candidate on Tencent",
        "Retain V2 operation diagnostics",
    )
    missing_steps = [name for name in required_order if name not in names]
    if missing_steps:
        raise AssertionError(f"Fixed V2 prepare is missing steps: {missing_steps}")
    if tuple(names) != required_order:
        raise AssertionError("Fixed V2 prepare step surface or order changed")
    reconfirm = step_by_name(
        deploy_steps,
        "Reconfirm current main before first production mutation",
    )
    reconfirm_env = mapping(reconfirm.get("env"), "prepare reconfirm env")
    expected_target_env = {
        "TARGET_COMMIT_SHA": "${{ inputs.target_commit_sha }}",
        "TARGET_ARCHIVE_SHA256": "${{ inputs.target_archive_sha256 }}",
        "TARGET_MANIFEST_SHA256": "${{ inputs.target_manifest_sha256 }}",
    }
    for env_name, expected in expected_target_env.items():
        if reconfirm_env.get(env_name) != expected:
            raise AssertionError(
                f"prepare reconfirm target input binding changed: {env_name}"
            )
    reconfirm_command = str(reconfirm.get("run") or "")
    for token in (
        "release_coordination_guard.py",
        "verify-plan",
        '--main-sha "$GITHUB_SHA"',
        '--operation "$RELEASE_MODE"',
        '--target-sha "$TARGET_COMMIT_SHA"',
        '--target-archive-sha256 "$TARGET_ARCHIVE_SHA256"',
        '--target-manifest-sha256 "$TARGET_MANIFEST_SHA256"',
    ):
        if token not in reconfirm_command:
            raise AssertionError(f"prepare reconfirm is missing {token!r}")

    downloads = [
        step
        for step in deploy_steps
        if step.get("uses") == "actions/download-artifact@v5"
    ]
    frontend_downloads = [
        step
        for step in downloads
        if "artifact-ids" in mapping(step.get("with"), "artifact download with")
    ]
    if len(frontend_downloads) != 1:
        raise AssertionError("prepare must download one frontend artifact by ID")
    frontend_download_with = mapping(
        frontend_downloads[0].get("with"),
        "frontend artifact download with",
    )
    if frontend_download_with.get("artifact-ids") != (
        "${{ needs.build_frontend.outputs.github_artifact_id }}"
    ):
        raise AssertionError("prepare must consume the exact frontend artifact ID")
    if "name" in frontend_download_with or "pattern" in frontend_download_with:
        raise AssertionError("prepare may not fall back to artifact name/pattern")
    for forbidden in FORBIDDEN_BUILD_COMMANDS:
        if forbidden in combined_run(deploy, "deploy_tencent"):
            raise AssertionError(f"prepare contains forbidden build command {forbidden!r}")

    verify = str(
        step_by_name(
            deploy_steps,
            "Verify frontend artifact before Tencent deployment",
        ).get("run")
        or ""
    )
    for token in (
        "frontend_release_artifact.py verify",
        '--materialize-dir "$RUNNER_TEMP/frontend-dist"',
    ):
        if token not in verify:
            raise AssertionError(f"frontend verification is missing {token!r}")

    upload = str(
        step_by_name(
            deploy_steps,
            "Upload complete release archive with incremental rsync",
        ).get("run")
        or ""
    )
    required_upload_tokens = (
        'archive="$RUNNER_TEMP/JATO_deploy.tar.gz"',
        "gzip -n --rsyncable",
        "sudo -n realpath /opt/jato/slots/8000/current",
        "release-v2-manifest.json",
        "basis_kind='active'",
        "basis_kind='retained'",
        "basis_kind='bootstrap'",
        "ALLOW_FULL_UPLOAD_BOOTSTRAP",
        "--partial",
        "--checksum",
        "--stats",
        "sha256sum '$remote_temp'",
        "test ! -e '$remote_archive'",
        "ln '$remote_temp' '$remote_archive'",
        'echo "archive-sha256=$archive_sha256"',
    )
    missing_upload = [token for token in required_upload_tokens if token not in upload]
    if missing_upload:
        raise AssertionError(
            f"Fixed V2 incremental upload is incomplete: {missing_upload}"
        )
    for forbidden in ("StrictHostKeyChecking=no", "--append-verify", "--compress"):
        if forbidden in upload:
            raise AssertionError(f"Fixed V2 upload retains unsafe token {forbidden!r}")

    manifest = str(
        step_by_name(
            deploy_steps,
            "Generate canonical V2 release manifest",
        ).get("run")
        or ""
    )
    for token in (
        "from release_v2_store import",
        "ReleaseIdentity",
        "ReleaseManifest",
        "canonical_manifest_bytes",
        "manifest_sha256",
        "release-v2-manifest.json",
        "manifest-sha256=",
        "manifest-b64=",
        "Candidate release identity",
        "Commit SHA",
        "Archive SHA-256",
        "Manifest SHA-256",
    ):
        if token not in manifest:
            raise AssertionError(f"canonical V2 manifest is missing {token!r}")

    handoff = str(
        step_by_name(
            deploy_steps,
            "Deploy verified release to fixed Candidate on Tencent",
        ).get("run")
        or ""
    )
    for token in (
        "DEPLOY_RELEASE_SYSTEM fixed-v2",
        "RELEASE_V2_MANIFEST_B64",
        "RELEASE_V2_MANIFEST_SHA256",
        "fullstack_remote_release.sh",
        "V2_OPERATION_REPORT_PATH=",
        '"umask 077; exec bash -s"',
    ):
        if token not in handoff:
            raise AssertionError(f"Fixed V2 Candidate handoff is missing {token!r}")
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
                f"Fixed V2 prepare forwards unused app setting {forbidden!r}"
            )
    diagnostics = step_by_name(deploy_steps, "Retain V2 operation diagnostics")
    if unwrap_expression(diagnostics.get("if")) != "always()":
        raise AssertionError("Fixed V2 prepare diagnostics must run on failure")

    control = job(workflow, "control_fixed_release_v2")
    control_steps = steps(control, "control_fixed_release_v2")
    expected_control_steps = (
        "Checkout V2 control source",
        "Download frozen release coordination plan",
        "Revalidate frozen coordination plan after approval",
        "Validate fixed V2 operation intent",
        "Validate Tencent control credentials",
        "Run fixed V2 control operation on Tencent",
        "Retain fixed V2 control diagnostics",
    )
    if tuple(str(step.get("name") or "") for step in control_steps) != (
        expected_control_steps
    ):
        raise AssertionError("Fixed V2 control step surface or order changed")
    intent = str(
        step_by_name(control_steps, "Validate fixed V2 operation intent").get("run")
        or ""
    )
    for token in (
        'CONFIRM_CONTROL_OPERATION" = "true"',
        "update-active|rollback-active)",
        "discard-candidate)",
        "TARGET_COMMIT_SHA",
        "TARGET_ARCHIVE_SHA256",
        "TARGET_MANIFEST_SHA256",
    ):
        if token not in intent:
            raise AssertionError(f"Fixed V2 control intent is missing {token!r}")
    control_run = str(
        step_by_name(
            control_steps,
            "Run fixed V2 control operation on Tencent",
        ).get("run")
        or ""
    )
    for token in (
        "fixed_release_v2_remote.sh",
        "V2_OPERATION_REPORT_PATH=",
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
        "bash -s %q",
    ):
        if token not in control_run:
            raise AssertionError(f"Fixed V2 control handoff is missing {token!r}")
    for forbidden in FORBIDDEN_BUILD_COMMANDS + ("rsync ",):
        if forbidden in combined_run(control, "control_fixed_release_v2"):
            raise AssertionError(f"Fixed V2 control contains forbidden {forbidden!r}")
    if "cat 03_Scripts/deploy/fixed_release_v2_remote.sh" in control_run:
        raise AssertionError("Fixed V2 remote entry must come from the hashed bundle")
    remote_release = (
        REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
    ).read_text(encoding="utf-8")
    fixed_remote = FIXED_RELEASE_V2_REMOTE_PATH.read_text(encoding="utf-8")
    if 'V2_ARCHIVE_CACHE_ROOT="$ARCHIVE_ROOT_REAL"' not in remote_release:
        raise AssertionError(
            "Fixed V2 prepare must preserve the SSH user's archive cache root"
        )
    for token in (
        '[[ "$V2_ARCHIVE_CACHE_ROOT" == /* ]]',
        "require_archive_cache_root",
        '--archive-cache-root "$V2_ARCHIVE_CACHE_ROOT"',
    ):
        if token not in fixed_remote:
            raise AssertionError(
                f"Fixed V2 archive-cache handoff is missing {token!r}"
            )
    if fixed_remote.count('--archive-cache-root "$V2_ARCHIVE_CACHE_ROOT"') != 3:
        raise AssertionError(
            "All four Fixed V2 operations must receive the archive cache root"
        )
    control_diagnostics = step_by_name(
        control_steps,
        "Retain fixed V2 control diagnostics",
    )
    if unwrap_expression(control_diagnostics.get("if")) != "always()":
        raise AssertionError("Fixed V2 control diagnostics must run on failure")

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
    if create_command.count('--run-attempt "$GITHUB_RUN_ATTEMPT"') != 1:
        raise AssertionError(
            "the immutable frontend release must bind run-attempt exactly once"
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


def assert_prewarm_contract(production_name: str) -> None:
    """Preserve the unchanged, independently triggered intl prewarm contract."""

    prewarm = load_workflow(PREWARM_WORKFLOW_PATH)
    triggers = mapping(prewarm.get("on"), "prewarm on")
    if set(triggers) != {"workflow_run"}:
        raise AssertionError("prewarm must only be triggered by workflow_run")
    workflow_run = mapping(triggers.get("workflow_run"), "prewarm workflow_run")
    if sequence(workflow_run.get("workflows"), "prewarm workflows") != [
        production_name
    ]:
        raise AssertionError("prewarm workflow_run name must match production workflow")
    if sequence(workflow_run.get("types"), "prewarm types") != ["completed"]:
        raise AssertionError("prewarm must wait for completed production release")
    if sequence(workflow_run.get("branches"), "prewarm branches") != ["main"]:
        raise AssertionError("prewarm workflow_run must be main-only")

    prewarm_job = job(prewarm, "prewarm")
    if unwrap_expression(prewarm_job.get("if")) != PREWARM_CONDITION:
        raise AssertionError("prewarm must require completed success from main repository")
    prewarm_steps = steps(prewarm_job, "prewarm")
    names = [str(step.get("name") or "") for step in prewarm_steps]
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
        "validate_frontend_release_workflow.py",
        "test_frontend_release_artifact.py",
        "test_verify_intl_runtime_contract.py",
        "bash -n",
        "fullstack_remote_release.sh",
        "fixed_release_v2_remote.sh",
        "test_fixed_release_v2.py",
        "test_fixed_release_v2_workflow.py",
        "test_release_v2_store.py",
        "test_release_v2_admission.py",
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
    if str(production.get("name") or "").strip() != "production-release":
        raise AssertionError("production workflow name must be production-release")
    assert_fixed_v2_production_workflow(production)
    assert_single_build_and_strict_outputs(production)
    assert_fixed_v2_prepare_and_control_contract(production)
    assert_deterministic_backend_package(production)
    assert_independent_current_active_intl_sync()
    assert_prewarm_contract("production-release")
    assert_required_ci_contract()
    print(
        "Validated explicit Fixed V2 release actions, one immutable prepare "
        "artifact, incremental transport, canonical manifest, separate control "
        "job, and fail-closed CI coverage."
    )


if __name__ == "__main__":
    main()
