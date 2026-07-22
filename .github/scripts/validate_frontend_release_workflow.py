#!/usr/bin/env python3
"""Validate immutable production frontend release workflow contracts."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
import re
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/production-release.yml"
PREWARM_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/intl-edge-prewarm.yml"
CI_WORKFLOW_PATH = REPO_ROOT / ".github/workflows/ci.yml"
REMOTE_RELEASE_PATH = REPO_ROOT / "03_Scripts/deploy/fullstack_remote_release.sh"
COS_REQUIREMENTS_PATH = REPO_ROOT / "03_Scripts/deploy/requirements-cos-release.txt"
COSCLI_INSTALLER_PATH = REPO_ROOT / "03_Scripts/deploy/install_cos_release_transport.sh"
COS_UPLOAD_POLICY_PATH = (
    REPO_ROOT / "03_Scripts/deploy/cos/github-upload-policy.template.json"
)
COS_CVM_POLICY_PATH = REPO_ROOT / "03_Scripts/deploy/cos/cvm-read-policy.template.json"
SERVER_RELEASE_PATH = REPO_ROOT / "03_Scripts/ops/deploy_fullstack_server.sh"
MAIN_CONDITION = "github.ref == 'refs/heads/main'"
PREWARM_CONDITION = " ".join(
    (
        "github.event.workflow_run.conclusion == 'success' &&",
        "github.event.workflow_run.head_branch == 'main' &&",
        "github.event.workflow_run.head_repository.full_name == github.repository",
    )
)
BUILD_JOB = "build_frontend"
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
}
CLOUDFLARE_PROJECT_DIRECTORY = "${{ runner.temp }}/cloudflare-pages-project"
PINNED_WRANGLER_VERSION = "4.86.0"
COS_LOCKED_PACKAGES = {
    "certifi",
    "charset-normalizer",
    "cos-python-sdk-v5",
    "crcmod",
    "idna",
    "pycryptodome",
    "requests",
    "six",
    "urllib3",
    "xmltodict",
}
FORBIDDEN_BUILD_COMMANDS = (
    "npm ci",
    "npm install",
    "npm run build",
    "vite build",
    "yarn build",
    "pnpm build",
)


def assert_cos_policy_templates() -> None:
    object_resource = "qcs::cos:<REGION>:uid/<APPID>:<BUCKET-APPID>/releases/*"
    rollback_resource = "qcs::cos:<REGION>:uid/<APPID>:<BUCKET-APPID>/rollback/*"
    secure = {"bool_equal": {"cos:secure-transport": "true"}}
    insecure = {"bool_equal": {"cos:secure-transport": "false"}}
    boundary_actions = [
        "name/cos:InitiateMultipartUpload",
        "name/cos:CompleteMultipartUpload",
    ]
    transport_actions = [
        "name/cos:HeadObject",
        "name/cos:UploadPart",
        "name/cos:AbortMultipartUpload",
    ]
    expected_upload = {
        "version": "2.0",
        "statement": [
            {
                "effect": "allow",
                "action": transport_actions,
                "resource": [object_resource],
                "condition": secure,
            },
            {
                "effect": "allow",
                "action": boundary_actions,
                "resource": [object_resource],
                "condition": {
                    **secure,
                    "string_equal": {"cos:x-cos-forbid-overwrite": "true"},
                },
            },
            {
                "effect": "deny",
                "action": boundary_actions,
                "resource": [object_resource],
                "condition": {
                    "string_not_equal_if_exist": {
                        "cos:x-cos-forbid-overwrite": "true"
                    }
                },
            },
            {
                "effect": "deny",
                "action": [
                    "name/cos:HeadObject",
                    "name/cos:InitiateMultipartUpload",
                    "name/cos:UploadPart",
                    "name/cos:CompleteMultipartUpload",
                    "name/cos:AbortMultipartUpload",
                ],
                "resource": [object_resource],
                "condition": insecure,
            },
        ],
    }
    expected_cvm = {
        "version": "2.0",
        "statement": [
            {
                "effect": "allow",
                "action": ["name/cos:HeadObject", "name/cos:GetObject"],
                "resource": [object_resource, rollback_resource],
                "condition": secure,
            },
            {
                "effect": "deny",
                "action": ["name/cos:HeadObject", "name/cos:GetObject"],
                "resource": [object_resource, rollback_resource],
                "condition": insecure,
            },
        ],
    }
    for path, expected in (
        (COS_UPLOAD_POLICY_PATH, expected_upload),
        (COS_CVM_POLICY_PATH, expected_cvm),
    ):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload != expected:
            raise AssertionError(f"least-privilege COS policy template drifted: {path}")


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
        if len(downloads) != 1:
            raise AssertionError(f"{name} must download exactly one v5 artifact")
        download_with = mapping(downloads[0].get("with"), f"{name} download with")
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
        "Upload and HEAD-verify immutable release in COS"
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

    audit_needs = job(workflow, "audit_frontend_parity").get("needs")
    if audit_needs != [BUILD_JOB, "deploy_tencent"]:
        raise AssertionError("parity audit must wait for the single production deployment job")


def assert_tencent_cos_transport_contract(workflow: Mapping[str, Any]) -> None:
    workflow_source = PRODUCTION_WORKFLOW_PATH.read_text(encoding="utf-8")
    for forbidden in (
        "secrets.COS_SECRET_ID",
        "secrets.COS_SECRET_KEY",
        "secrets.COS_BOOTSTRAP_SECRET_ID",
        "secrets.COS_BOOTSTRAP_SECRET_KEY",
    ):
        if forbidden in workflow_source:
            raise AssertionError("COS production upload must not use long-lived credentials")

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
    if timeout_minutes < 60:
        raise AssertionError("deploy_tencent needs enough time for verified multi-platform release")

    permissions = mapping(tencent.get("permissions"), "deploy_tencent permissions")
    if permissions != {"contents": "read", "id-token": "write"}:
        raise AssertionError(
            "Tencent deploy must grant only contents:read and OIDC id-token:write"
        )

    tencent_steps = steps(tencent, "deploy_tencent")
    install_step = step_by_name(tencent_steps, "Install pinned Tencent COS upload SDK")
    install_command = " ".join(str(install_step.get("run") or "").split())
    required_install_tokens = (
        "python3 -m pip install",
        "--require-hashes",
        "--requirement 03_Scripts/deploy/requirements-cos-release.txt",
    )
    if any(token not in install_command for token in required_install_tokens):
        raise AssertionError("COS SDK install must use the checked-in hash-locked requirements")

    requirements = COS_REQUIREMENTS_PATH.read_text(encoding="utf-8")
    locked_packages = set(
        re.findall(r"(?m)^([a-z0-9][a-z0-9-]*)==[^\\\s]+ \\$", requirements)
    )
    if locked_packages != COS_LOCKED_PACKAGES:
        raise AssertionError(
            "COS requirements package set drifted: "
            f"missing={sorted(COS_LOCKED_PACKAGES - locked_packages)}, "
            f"extra={sorted(locked_packages - COS_LOCKED_PACKAGES)}"
        )
    requirement_blocks = re.split(r"(?m)(?=^[a-z0-9][a-z0-9-]*==)", requirements)
    for block in requirement_blocks[1:]:
        package = block.split("==", 1)[0]
        if "--hash=sha256:" not in block.split("# via", 1)[0]:
            raise AssertionError(f"COS requirement {package} is missing SHA-256 hashes")

    installer = COSCLI_INSTALLER_PATH.read_text(encoding="utf-8")
    required_installer_tokens = (
        'COSCLI_VERSION="v1.0.8"',
        "https://cosbrowser.cloud.tencent.com/software/coscli",
        "https://github.com/tencentyun/coscli/releases/download/${COSCLI_VERSION}",
        "7165f2ae16c5f7ac495864c963ca574a76e04ec72680d7bc8a8eee3234d8cf91",
        "0404b4da5b1d0c230c7d7522cb3bbec2909e314ab998889a0aeb8dc6094a2d21",
        "--proto '=https' --tlsv1.2",
    )
    if any(token not in installer for token in required_installer_tokens):
        raise AssertionError("COSCLI installer version, sources, or hashes have drifted")
    if "http://" in installer or "curl -k" in installer:
        raise AssertionError("COSCLI installer must use verified HTTPS only")

    upload_steps = [
        step
        for step in steps(tencent, "deploy_tencent")
        if step.get("name") == "Upload and HEAD-verify immutable release in COS"
    ]
    if len(upload_steps) != 1:
        raise AssertionError("Tencent must have exactly one fail-closed COS archive upload step")
    upload_step = upload_steps[0]
    if upload_step.get("id") != "cos_release":
        raise AssertionError("COS upload step must expose the immutable receipt outputs")
    upload = str(upload_step.get("run") or "")
    required_tokens = (
        "cos_release_transport.py upload",
        '--archive "$RUNNER_TEMP/JATO_deploy.tar.gz"',
        '--github-sha "$GITHUB_SHA"',
        "--prefix releases",
        '--role-arn "$COS_RELEASE_UPLOAD_ROLE_ARN"',
        '--provider-id "$COS_RELEASE_OIDC_PROVIDER_ID"',
        '--audience "$COS_RELEASE_OIDC_AUDIENCE"',
        "--environment production",
        '--github-output "$GITHUB_OUTPUT"',
        "--part-size-mib 8",
        "--threads 4",
    )
    missing = [token for token in required_tokens if token not in upload]
    if missing:
        raise AssertionError(f"Tencent COS upload contract is incomplete: {missing}")
    upload_env = mapping(upload_step.get("env"), "COS upload env")
    required_variables = {
        "COS_RELEASE_BUCKET",
        "COS_RELEASE_REGION",
        "COS_RELEASE_UPLOAD_ROLE_ARN",
        "COS_RELEASE_OIDC_PROVIDER_ID",
        "COS_RELEASE_OIDC_AUDIENCE",
    }
    if set(upload_env) != required_variables:
        raise AssertionError("COS upload environment variables have drifted")
    if any("secrets." in str(value) for value in upload_env.values()):
        raise AssertionError("COS upload must use OIDC, not long-lived GitHub secrets")
    forbidden_upload_tokens = (
        "ssh ",
        "sshpass",
        "tail -c",
        "head -c",
        "DEPLOY_ARCHIVE_PATH",
        "presigned",
        "signurl",
    )
    if any(token in upload for token in forbidden_upload_tokens):
        raise AssertionError("COS upload step retains a forbidden SSH or signed-URL path")

    deploy = step_by_name(tencent_steps, "Deploy verified release on Tencent")
    deploy_env = mapping(deploy.get("env"), "Tencent deploy env")
    expected_outputs = {
        "DEPLOY_COS_BUCKET": "bucket",
        "DEPLOY_COS_REGION": "region",
        "DEPLOY_COS_OBJECT_KEY": "object-key",
        "DEPLOY_ARCHIVE_BYTES": "archive-bytes",
        "DEPLOY_ARCHIVE_SHA256": "archive-sha256",
        "DEPLOY_ARCHIVE_CRC64": "archive-crc64",
    }
    for env_name, output_name in expected_outputs.items():
        expected = f"${{{{ steps.cos_release.outputs.{output_name} }}}}"
        if deploy_env.get(env_name) != expected:
            raise AssertionError(f"{env_name} must come from the verified COS receipt")
    deploy_command = str(deploy.get("run") or "")
    if "DEPLOY_ARCHIVE_PATH" in deploy_command:
        raise AssertionError("Tencent control command must not reference an SSH-uploaded archive")
    for name in (*expected_outputs, "DEPLOY_COS_CVM_ROLE_NAME"):
        if f"export {name}=" not in deploy_command:
            if f"emit_export {name} " not in deploy_command:
                raise AssertionError(f"Tencent control command does not pass {name}")

    auth_step = step_by_name(tencent_steps, "Validate Tencent deploy credentials")
    auth_env = mapping(auth_step.get("env"), "Tencent SSH auth env")
    if auth_env.get("SSH_KNOWN_HOSTS") != "${{ secrets.SSH_KNOWN_HOSTS }}":
        raise AssertionError("Tencent SSH host identity must come from SSH_KNOWN_HOSTS")
    auth_command = str(auth_step.get("run") or "")
    if "ssh-keygen -F" not in auth_command:
        raise AssertionError("Tencent SSH preflight must verify the pinned host and port")
    forbidden_ssh_tokens = (
        "StrictHostKeyChecking=no",
        "UserKnownHostsFile=/dev/null",
        "remote_exports=",
        '"${remote_exports}bash -s"',
    )
    if any(token in deploy_command for token in forbidden_ssh_tokens):
        raise AssertionError("Tencent control channel retains an insecure SSH path")
    required_ssh_tokens = (
        "StrictHostKeyChecking=yes",
        'UserKnownHostsFile="$HOME/.ssh/known_hosts"',
        'remote_payload="$(mktemp',
        "'exec bash -s' < \"$remote_payload\"",
    )
    if any(token not in deploy_command for token in required_ssh_tokens):
        raise AssertionError("Tencent control channel must pin SSH and keep secrets out of argv")

    names = [str(step.get("name") or "") for step in tencent_steps]
    if "Seal verified production COS receipt" in names:
        raise AssertionError("COS receipt must not be sealed before the final parity job")
    candidate = step_by_name(
        tencent_steps,
        "Retain candidate COS receipt for final parity",
    )
    candidate_with = mapping(candidate.get("with"), "candidate COS receipt artifact")
    if candidate.get("uses") != "actions/upload-artifact@v4":
        raise AssertionError("candidate COS receipt must be retained for the final parity job")
    if candidate_with.get("retention-days") != "7":
        raise AssertionError("candidate COS receipt retention must remain bounded to 7 days")

    audit_steps = steps(job(workflow, "audit_frontend_parity"), "audit_frontend_parity")
    audit_names = [str(step.get("name") or "") for step in audit_steps]
    parity_index = audit_names.index("Require www and intl immutable release parity")
    candidate_download_index = audit_names.index(
        "Download candidate COS receipt after final parity"
    )
    seal_index = audit_names.index("Seal verified production COS receipt")
    retain_index = audit_names.index("Retain verified production COS receipt")
    if not parity_index < candidate_download_index < seal_index < retain_index:
        raise AssertionError("COS receipt may only be sealed after final www/intl parity")
    seal_step = step_by_name(audit_steps, "Seal verified production COS receipt")
    seal_command = str(seal_step.get("run") or "")
    required_seal_tokens = (
        "cos_release_transport.py seal",
        '--github-sha "$GITHUB_SHA"',
        '--repository "$GITHUB_REPOSITORY"',
        "cos-release-candidate/cos-release-candidate.json",
    )
    if any(token not in seal_command for token in required_seal_tokens):
        raise AssertionError("final COS receipt seal is not bound to this main workflow run")
    retain = step_by_name(audit_steps, "Retain verified production COS receipt")
    retain_with = mapping(retain.get("with"), "verified COS receipt artifact")
    if retain.get("uses") != "actions/upload-artifact@v4":
        raise AssertionError("verified COS receipt must be retained as immutable run evidence")
    if retain_with.get("retention-days") != "30":
        raise AssertionError("verified receipt retention must match the 30-day release lifecycle")

    remote_release = REMOTE_RELEASE_PATH.read_text(encoding="utf-8")
    required_remote_tokens = (
        'EXPECTED_COS_OBJECT_KEY="releases/${DEPLOY_COMMIT_SHA}/${DEPLOY_ARCHIVE_SHA256}.tar.gz"',
        'COS_INTERNAL_ENDPOINT="cos-internal.${DEPLOY_COS_REGION}.tencentcos.cn"',
        'COS_NO_PROXY_HOSTS="metadata.tencentyun.com,169.254.0.23,${COS_INTERNAL_HOST}"',
        "mode: CvmRole",
        "--disable-checksum=false",
        'verify_release_archive_identity "$COS_DOWNLOAD_TEMP"',
        'mv "$COS_DOWNLOAD_TEMP" "$RELEASE_ARCHIVE"',
        'verify_release_archive_identity "$RELEASE_ARCHIVE"',
        "Release archive member paths passed fail-closed validation",
        "01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx",
        "top30_suv_price_movement_candidates.json",
        "official_evidence_leads.json",
        "06_AppPlatform/backend/alembic/env.py",
    )
    missing_remote = [token for token in required_remote_tokens if token not in remote_release]
    if missing_remote:
        raise AssertionError(f"Tencent COS download contract is incomplete: {missing_remote}")
    if "cos.accelerate" in remote_release or "presigned" in remote_release:
        raise AssertionError("CVM download must use the derived regional internal endpoint")
    verify_index = remote_release.index('verify_release_archive_identity "$COS_DOWNLOAD_TEMP"')
    safe_paths_index = remote_release.index(
        "Release archive member paths passed fail-closed validation"
    )
    extract_index = remote_release.index('tar xzf "$RELEASE_ARCHIVE"')
    required_files_index = remote_release.index("required_release_files=(")
    required_directories_index = remote_release.index("required_release_directories=(")
    materialize_index = remote_release.index(
        '--materialize-dir "$PREBUILT_FRONTEND_DIR"'
    )
    preflight_complete_index = remote_release.index(
        "Release archive and materialized frontend passed all pre-mutation checks"
    )
    mutate_index = remote_release.index('sudo mkdir -p "$REPO_DIR"')
    if not (
        verify_index
        < safe_paths_index
        < extract_index
        < required_files_index
        < required_directories_index
        < materialize_index
        < preflight_complete_index
        < mutate_index
    ):
        raise AssertionError(
            "CVM must verify COS bytes, archive paths, required members, and frontend "
            "materialization before production mutation"
        )


def assert_server_consumes_only_prebuilt_dist() -> None:
    remote_release = REMOTE_RELEASE_PATH.read_text(encoding="utf-8")
    server_release = SERVER_RELEASE_PATH.read_text(encoding="utf-8")
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
    if "--materialize-dir \"$PREBUILT_FRONTEND_DIR\"" not in remote_release:
        raise AssertionError("remote release must materialize only the verified artifact")
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


def assert_required_ci_contract() -> None:
    ci = load_workflow(CI_WORKFLOW_PATH)
    contract_job = job(ci, "frontend-release-contract")
    if contract_job.get("continue-on-error") == "true":
        raise AssertionError("frontend-release-contract must be a required CI job")
    contract_steps = steps(contract_job, "frontend-release-contract")
    setup_node = step_by_name(contract_steps, "Setup fixed edge contract Node")
    if setup_node.get("uses") != "actions/setup-node@v4":
        raise AssertionError("edge contract CI must use actions/setup-node@v4")
    setup_node_with = mapping(setup_node.get("with"), "edge contract setup-node with")
    if setup_node_with.get("node-version") != "20.19.0":
        raise AssertionError("edge contract CI must use the production Node version")

    cos_install = step_by_name(
        contract_steps,
        "Install hash-locked COS transport dependencies",
    )
    cos_install_command = " ".join(str(cos_install.get("run") or "").split())
    for token in (
        "python -m pip install",
        "--require-hashes",
        "--requirement 03_Scripts/deploy/requirements-cos-release.txt",
    ):
        if token not in cos_install_command:
            raise AssertionError("required CI must install the hash-locked COS SDK")

    commands = combined_run(contract_job, "frontend-release-contract")
    required_tokens = (
        "npm ci",
        "test_frontend_release_artifact.py",
        "test_cos_release_transport.py",
        "test_verify_intl_runtime_contract.py",
        "npx vitest run",
        "edgeCacheFunction.test.ts",
        "healthzEdgeFunction.test.ts",
    )
    missing = [token for token in required_tokens if token not in commands]
    if missing:
        raise AssertionError(f"required edge contract CI is incomplete: {missing}")


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
    assert_cos_policy_templates()
    assert_tencent_cos_transport_contract(production)
    assert_server_consumes_only_prebuilt_dist()
    assert_prewarm_contract(production_name)
    assert_required_ci_contract()
    print(
        "Validated immutable production release, shared artifact parity, "
        "artifact-bound edge functions, intl runtime routing, server no-build "
        "semantics, and main-only prewarm provenance."
    )


if __name__ == "__main__":
    main()
