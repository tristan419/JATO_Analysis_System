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
CI_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/ci.yml"
REMOTE_RELEASE_PATH = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
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
PREWARM_CONDITION = " ".join(
    (
        "github.event.workflow_run.conclusion == 'success' &&",
        "github.event.workflow_run.head_branch == 'main' &&",
        "github.event.workflow_run.head_repository.full_name == github.repository",
    )
)
BUILD_JOB = "build_frontend"
COORDINATION_JOB = "release_coordination_guard"
DEPLOY_JOBS = ("deploy_tencent",)
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

    jobs = mapping(workflow.get("jobs"), "production workflow jobs")
    expected_jobs = {
        COORDINATION_JOB,
        BUILD_JOB,
        "deploy_tencent",
        "audit_frontend_parity",
    }
    if set(jobs) != expected_jobs:
        raise AssertionError(
            f"unexpected production release jobs: {sorted(set(jobs) - expected_jobs)}"
        )
    for name in expected_jobs:
        condition = unwrap_expression(job(workflow, name).get("if"))
        if condition != MAIN_CONDITION:
            raise AssertionError(f"{name} must use the exact main-only condition")
    for name in DEPLOY_JOBS:
        deploy_job = job(workflow, name)
        if deploy_job.get("environment") != "production":
            raise AssertionError(f"{name} must use the production environment")


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

    create_step = step_by_name(build_steps, "Create immutable frontend release")
    create_command = str(create_step.get("run") or "")
    if "--functions-dir 06_AppPlatform/frontend/functions" not in create_command:
        raise AssertionError(
            "the immutable frontend release must include Cloudflare Pages Functions"
        )


def assert_deploy_jobs_share_one_artifact(workflow: Mapping[str, Any]) -> None:
    for name in DEPLOY_JOBS:
        deploy_job = job(workflow, name)
        expected_needs: Any = BUILD_JOB
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
        "Upload complete release archive without fallback"
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
    if audit_needs != [BUILD_JOB, "deploy_tencent"]:
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
        if step.get("name") == "Upload complete release archive without fallback"
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
        "--append-verify",
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
        'tar "${tar_private_normalize[@]}" -rf "$RUNNER_TEMP/JATO_deploy.tar"',
        "--exclude='06_AppPlatform/backend/*.parquet'",
        "--no-acls",
        "--no-xattrs",
        "--no-selinux",
        'gzip -n -f "$RUNNER_TEMP/JATO_deploy.tar"',
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
    private_append = (
        'tar "${tar_private_normalize[@]}" -rf '
        '"$RUNNER_TEMP/JATO_deploy.tar" -C "$GITHUB_WORKSPACE" '
    )
    for private_path in ('"$required_workbook"', '"$msrp_pack"', '"$evidence_path"'):
        if f"{private_append}{private_path}" not in normalized_package:
            raise AssertionError(
                f"sensitive release asset is not privately archived: {private_path}"
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
        "retention-days": "7",
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
        'checkpoint.get("phase") != "backend_healthy"',
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
    if cloudflare.get("if") != "${{ steps.intl_current.outputs.current != 'true' }}":
        raise AssertionError("an already-current intl release must skip redeployment")
    cloudflare_run = str(cloudflare.get("run") or "")
    if "for deploy_attempt in 1 2 3" not in cloudflare_run:
        raise AssertionError("an outdated intl release must have three bounded deploy attempts")

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
    for forbidden in ("fallback to sparse", "github_sparse_checkout", "sparse Git fetch"):
        if forbidden in remote_release:
            raise AssertionError(f"remote release retains forbidden fallback: {forbidden}")
    if 'python3 "$FRONTEND_RELEASE_HELPER" verify' not in remote_release:
        raise AssertionError("remote release must invoke the shared artifact verifier")
    if (
        "tar --same-permissions --no-overwrite-dir" not in remote_release
        or '-xzf "$RELEASE_ARCHIVE" -C "$RELEASE_WORKTREE"' not in remote_release
    ):
        raise AssertionError(
            "remote release must restore normalized archive permissions while "
            "preserving the private extraction root",
        )
    for mode_guard in ("stat.S_IMODE(member.mode)", "Unsafe release archive mode"):
        if mode_guard not in remote_release:
            raise AssertionError(
                "same-permissions extraction requires fail-closed archive mode checks",
            )
    if "03_Scripts/deploy/cleanup_toolkit_egg_info.py" not in remote_release:
        raise AssertionError(
            "remote release must carry the fail-closed toolkit metadata cleaner",
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
        'python -m pip install "PyYAML==6.0.2" "pytest<9"',
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
        "test_cleanup_toolkit_egg_info.py",
        "test_release_source_seal.py",
        "test_pre_switch_checkpoint_recovery.py",
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
    assert_single_build_and_strict_outputs(production)
    assert_deploy_jobs_share_one_artifact(production)
    assert_tencent_resumable_upload_contract(production)
    assert_deterministic_backend_package(production)
    assert_release_checkpoint_contract(production)
    assert_server_consumes_only_prebuilt_dist()
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
