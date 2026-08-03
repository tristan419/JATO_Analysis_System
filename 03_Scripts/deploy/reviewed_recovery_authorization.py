#!/usr/bin/env python3
"""Verify and freeze one immutable reviewed checkpoint-recovery dry-run."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Mapping, Sequence


MAX_DOCUMENT_BYTES = 1024 * 1024
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
INCIDENT_ID = "2026-08-03-29df-pre-switch-candidate-residue"
TARGET_COMMIT = "29df5e6e667351f09305783932b34e5438d6a9d5"
WORKFLOW_PATH = ".github/workflows/production-checkpoint-recovery.yml"
RESULT_FIELDS = {
    "decision",
    "incidentId",
    "implementationCommit",
    "planSha256",
    "checkpointPhase",
    "databaseRevisions",
    "activeCommit",
    "candidatePresent",
    "candidateStarted",
    "trafficChanged",
    "databaseChanged",
    "checkpointChanged",
    "mutationPerformed",
    "mode",
    "otherReleaseGate",
    "targetIdentity",
    "inventoryDigest",
    "candidateResiduePresent",
}
AUTHORIZATION_FIELDS = {
    "schemaVersion",
    "kind",
    "repository",
    "workflowPath",
    "runId",
    "runAttempt",
    "mainSha",
    "planSha256",
    "resultSha256",
    "incidentId",
    "inventoryDigest",
    "decision",
}


class AuthorizationError(ValueError):
    """The reviewed dry-run or its immutable binding is invalid."""


def _reject_duplicates(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AuthorizationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _require_sha256(value: str, label: str) -> str:
    if not SHA256_PATTERN.fullmatch(value):
        raise AuthorizationError(f"{label} must be a lowercase SHA-256")
    return value


def _require_git_sha(value: str, label: str) -> str:
    if not GIT_SHA_PATTERN.fullmatch(value):
        raise AuthorizationError(f"{label} must be a full lowercase git SHA")
    return value


def _read_document(path: Path, label: str) -> tuple[Mapping[str, Any], bytes]:
    try:
        before = path.lstat()
    except OSError as exc:
        raise AuthorizationError(f"{label} cannot be inspected") from exc
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_size <= 0
        or before.st_size > MAX_DOCUMENT_BYTES
    ):
        raise AuthorizationError(f"{label} must be one bounded regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise AuthorizationError(f"{label} changed while opening")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    if (
        len(raw) != before.st_size
        or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    ):
        raise AuthorizationError(f"{label} changed while reading")
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicates,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AuthorizationError(f"{label} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise AuthorizationError(f"{label} must be one JSON object")
    return payload, raw


def _inventory_digest(plan: Mapping[str, Any]) -> str:
    try:
        payload = {
            "incidentId": plan["incidentId"],
            "identity": plan["checkpoint"]["identity"],
            "profile": plan["residue"]["profile"],
            "candidateUnit": plan["residue"]["candidateUnit"],
            "items": plan["residue"]["items"],
            "retainedEvidence": plan["residue"]["retainedEvidence"],
            "requiredAbsentPaths": plan["residue"]["requiredAbsentPaths"],
        }
    except (KeyError, TypeError) as exc:
        raise AuthorizationError("recovery plan inventory is incomplete") from exc
    raw = (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        + b"\n"
    )
    return hashlib.sha256(raw).hexdigest()


def validate_reviewed_dry_run(
    *,
    result_path: Path,
    plan_path: Path,
    expected_result_sha256: str,
    expected_main_sha: str,
    expected_plan_sha256: str,
) -> tuple[Mapping[str, Any], bytes, Mapping[str, Any], str]:
    expected_result_sha256 = _require_sha256(
        expected_result_sha256,
        "expected result digest",
    )
    expected_main_sha = _require_git_sha(expected_main_sha, "expected main SHA")
    expected_plan_sha256 = _require_sha256(
        expected_plan_sha256,
        "expected plan digest",
    )
    result, result_raw = _read_document(result_path, "reviewed dry-run result")
    plan, plan_raw = _read_document(plan_path, "recovery plan")
    if hashlib.sha256(result_raw).hexdigest() != expected_result_sha256:
        raise AuthorizationError("reviewed dry-run result SHA-256 mismatch")
    if hashlib.sha256(plan_raw).hexdigest() != expected_plan_sha256:
        raise AuthorizationError("recovery plan SHA-256 mismatch")
    inventory_digest = _inventory_digest(plan)
    try:
        identity = plan["checkpoint"]["identity"]
        expected_revisions = plan["expected"]["revisions"]
        active_commit = plan["expected"]["activeCommit"]
    except (KeyError, TypeError) as exc:
        raise AuthorizationError("recovery plan identity is incomplete") from exc
    if (
        plan.get("schemaVersion") != 3
        or plan.get("incidentId") != INCIDENT_ID
        or not isinstance(identity, dict)
        or identity.get("commit") != TARGET_COMMIT
        or set(result) != RESULT_FIELDS
        or result.get("decision") != "candidate-residue-dry-run-eligible"
        or result.get("incidentId") != INCIDENT_ID
        or result.get("implementationCommit") != expected_main_sha
        or result.get("planSha256") != expected_plan_sha256
        or result.get("targetIdentity") != identity
        or result.get("checkpointPhase") != "migrated"
        or result.get("databaseRevisions") != expected_revisions
        or result.get("activeCommit") != active_commit
        or result.get("otherReleaseGate") != "cross-release-safe"
        or result.get("mode") != "dry-run"
        or result.get("candidateResiduePresent") is not True
        or result.get("inventoryDigest") != inventory_digest
        or any(
            result.get(field) is not False
            for field in (
                "trafficChanged",
                "checkpointChanged",
                "databaseChanged",
                "candidateStarted",
                "mutationPerformed",
                "candidatePresent",
            )
        )
    ):
        raise AuthorizationError("reviewed dry-run result contract is invalid")
    return result, result_raw, plan, inventory_digest


def build_authorization(
    *,
    repository: str,
    run_id: int,
    run_attempt: int,
    main_sha: str,
    plan_sha256: str,
    result_sha256: str,
    inventory_digest: str,
) -> Mapping[str, Any]:
    if not repository or repository.startswith("/") or repository.endswith("/"):
        raise AuthorizationError("repository identity is invalid")
    if isinstance(run_id, bool) or run_id <= 0:
        raise AuthorizationError("run ID must be positive")
    if isinstance(run_attempt, bool) or run_attempt <= 0:
        raise AuthorizationError("run attempt must be positive")
    return {
        "schemaVersion": 1,
        "kind": "checkpoint_recovery_dry_run_authorization",
        "repository": repository,
        "workflowPath": WORKFLOW_PATH,
        "runId": run_id,
        "runAttempt": run_attempt,
        "mainSha": _require_git_sha(main_sha, "main SHA"),
        "planSha256": _require_sha256(plan_sha256, "plan digest"),
        "resultSha256": _require_sha256(result_sha256, "result digest"),
        "incidentId": INCIDENT_ID,
        "inventoryDigest": _require_sha256(
            inventory_digest,
            "inventory digest",
        ),
        "decision": "candidate-residue-dry-run-eligible",
    }


def validate_authorization(
    *,
    authorization_path: Path,
    repository: str,
    run_id: int,
    main_sha: str,
    plan_sha256: str,
    result_sha256: str,
    inventory_digest: str,
) -> Mapping[str, Any]:
    authorization, _ = _read_document(
        authorization_path,
        "reviewed dry-run authorization",
    )
    if (
        set(authorization) != AUTHORIZATION_FIELDS
        or authorization.get("schemaVersion") != 1
        or authorization.get("kind")
        != "checkpoint_recovery_dry_run_authorization"
        or authorization.get("repository") != repository
        or authorization.get("workflowPath") != WORKFLOW_PATH
        or authorization.get("runId") != run_id
        or isinstance(authorization.get("runAttempt"), bool)
        or not isinstance(authorization.get("runAttempt"), int)
        or authorization.get("runAttempt", 0) <= 0
        or authorization.get("mainSha") != main_sha
        or authorization.get("planSha256") != plan_sha256
        or authorization.get("resultSha256") != result_sha256
        or authorization.get("incidentId") != INCIDENT_ID
        or authorization.get("inventoryDigest") != inventory_digest
        or authorization.get("decision")
        != "candidate-residue-dry-run-eligible"
    ):
        raise AuthorizationError("reviewed dry-run authorization binding changed")
    return authorization


def _write_new(path: Path, raw: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise OSError("authorization write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def freeze_reviewed_dry_run(arguments: argparse.Namespace) -> None:
    result, result_raw, _, inventory_digest = validate_reviewed_dry_run(
        result_path=arguments.result,
        plan_path=arguments.plan,
        expected_result_sha256=arguments.expected_result_sha256,
        expected_main_sha=arguments.expected_main_sha,
        expected_plan_sha256=arguments.expected_plan_sha256,
    )
    authorization = build_authorization(
        repository=arguments.repository,
        run_id=arguments.run_id,
        run_attempt=arguments.run_attempt,
        main_sha=arguments.expected_main_sha,
        plan_sha256=arguments.expected_plan_sha256,
        result_sha256=arguments.expected_result_sha256,
        inventory_digest=inventory_digest,
    )
    if result["inventoryDigest"] != authorization["inventoryDigest"]:
        raise AuthorizationError("authorization inventory binding changed")
    output = arguments.output_dir
    if output.is_symlink() or output.exists():
        raise AuthorizationError("freeze output directory must not exist")
    output.mkdir(mode=0o700)
    os.chmod(output, 0o700)
    _write_new(output / "checkpoint-recovery-result.json", result_raw)
    authorization_raw = (
        json.dumps(
            authorization,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        + b"\n"
    )
    _write_new(output / "reviewed-dry-run-authorization.json", authorization_raw)


def verify_frozen_review(arguments: argparse.Namespace) -> None:
    _, _, _, inventory_digest = validate_reviewed_dry_run(
        result_path=arguments.result,
        plan_path=arguments.plan,
        expected_result_sha256=arguments.expected_result_sha256,
        expected_main_sha=arguments.expected_main_sha,
        expected_plan_sha256=arguments.expected_plan_sha256,
    )
    validate_authorization(
        authorization_path=arguments.authorization,
        repository=arguments.repository,
        run_id=arguments.run_id,
        main_sha=arguments.expected_main_sha,
        plan_sha256=arguments.expected_plan_sha256,
        result_sha256=arguments.expected_result_sha256,
        inventory_digest=inventory_digest,
    )


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--expected-result-sha256", required=True)
    parser.add_argument("--expected-main-sha", required=True)
    parser.add_argument("--expected-plan-sha256", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True, type=int)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    _common_arguments(freeze)
    freeze.add_argument("--run-attempt", required=True, type=int)
    freeze.add_argument("--output-dir", required=True, type=Path)
    verify = commands.add_parser("verify")
    _common_arguments(verify)
    verify.add_argument("--authorization", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "freeze":
            freeze_reviewed_dry_run(arguments)
        else:
            verify_frozen_review(arguments)
    except (AuthorizationError, OSError) as exc:
        print(f"reviewed recovery authorization error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
