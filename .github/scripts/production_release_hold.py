#!/usr/bin/env python3
"""Resolve the one reviewed recovery-only production release hold.

The hold is intentionally incident-specific.  Exactly one reviewed state must
exist: the active hold pauses production, while the fixed retirement record
resumes it.  Missing, simultaneous, malformed, or plan-drifted states fail
closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
INCIDENT_ID = "2026-08-03-29df-pre-switch-candidate-residue"
HOLD_PATH = Path(
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue-production-hold.v1.json"
)
RETIREMENT_PATH = Path(
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue-"
    "production-hold-retirement.v1.json"
)
HOLD_SHA256 = (
    "b4841970c047ff8fe14db7e97d9d81326e67ac116da67f9f1236160edd11a71b"
)
RECOVERY_PLAN_PATH = Path(
    ".github/recovery-plans/"
    "2026-08-03-29df-pre-switch-candidate-residue.json"
)
RECOVERY_PLAN_SHA256 = (
    "ae4d3d5eb76695e29c2eeb947b7783c42960a266c27abaa3f7b6a2faa51fd0f2"
)
RELEASE_ACTION_OUTPUT = "release-action"
HOLD_ACTION = "hold"
DEPLOY_ACTION = "deploy"
MAX_HOLD_BYTES = 16 * 1024
MAX_RECOVERY_PLAN_BYTES = 2 * 1024 * 1024

EXPECTED_HOLD_DOCUMENT: dict[str, Any] = {
    "authorization": (
        "This hold does not authorize recovery apply, deployment, traffic "
        "switch, Nginx mutation, or JATO data mutation."
    ),
    "effect": {
        "productionReleaseAction": HOLD_ACTION,
        "sharedConcurrency": "production-release-main",
        "skipBefore": "build_frontend",
    },
    "incidentId": INCIDENT_ID,
    "kind": "jato-production-release-hold",
    "lifecycle": {
        "allowedWhileActive": [
            "checkpoint recovery dry-run",
            "separately authorized checkpoint recovery apply",
            "Nginx reconciliation and no-traffic Candidate canary",
        ],
        "removal": (
            "Delete this file only in the same explicitly reviewed final "
            "production release pull request that adds the exact canonical "
            "retirement record, after checkpoint recovery and reconciliation "
            "evidence are complete; deletion alone fails closed and only the "
            "paired change resumes production deployment."
        ),
    },
    "recoveryPlan": {
        "path": RECOVERY_PLAN_PATH.as_posix(),
        "sha256": RECOVERY_PLAN_SHA256,
    },
    "schemaVersion": 1,
    "status": "active",
}

EXPECTED_RETIREMENT_DOCUMENT: dict[str, Any] = {
    "authorization": (
        "Adding this exact record in the same explicitly reviewed final "
        "production release pull request that removes the active hold "
        "authorizes production deployment only after checkpoint recovery, "
        "Nginx reconciliation, and no-traffic Candidate canary evidence are "
        "complete."
    ),
    "effect": {
        "checkpointRecoveryAction": "reject-retired",
        "productionReleaseAction": DEPLOY_ACTION,
    },
    "incidentId": INCIDENT_ID,
    "kind": "jato-production-release-hold-retirement",
    "lifecycle": {
        "requiredChange": (
            "Delete the active hold and add this exact retirement record in "
            "one explicitly reviewed final production release pull request."
        ),
        "retention": (
            "Retain this retirement record on main as the durable authorization "
            "for subsequent production releases."
        ),
    },
    "recoveryPlan": {
        "path": RECOVERY_PLAN_PATH.as_posix(),
        "sha256": RECOVERY_PLAN_SHA256,
    },
    "retiredHold": {
        "path": HOLD_PATH.as_posix(),
        "sha256": HOLD_SHA256,
    },
    "schemaVersion": 1,
    "status": "retired",
}


class HoldContractError(RuntimeError):
    """The checked-in production hold is malformed or no longer reviewed."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise HoldContractError(f"duplicate JSON key in production hold: {key}")
        result[key] = value
    return result


def _file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_stable_regular_file(path: Path, label: str, max_bytes: int) -> bytes:
    try:
        before = path.lstat()
    except FileNotFoundError as exc:
        raise HoldContractError(f"{label} is missing: {path}") from exc
    if not stat.S_ISREG(before.st_mode):
        raise HoldContractError(f"{label} must be a regular file: {path}")
    if before.st_nlink != 1:
        raise HoldContractError(f"{label} must have exactly one hard link: {path}")
    if before.st_size <= 0 or before.st_size > max_bytes:
        raise HoldContractError(
            f"{label} size must be between 1 and {max_bytes} bytes: {path}"
        )

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise HoldContractError(f"cannot safely open {label}: {path}") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or _file_identity(opened) != _file_identity(before)
        ):
            raise HoldContractError(f"{label} changed before it was opened: {path}")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise HoldContractError(f"{label} exceeds {max_bytes} bytes: {path}")
        after_read = os.fstat(descriptor)
        if _file_identity(after_read) != _file_identity(opened):
            raise HoldContractError(f"{label} changed while it was read: {path}")
    finally:
        os.close(descriptor)
    try:
        after_close = path.lstat()
    except FileNotFoundError as exc:
        raise HoldContractError(f"{label} disappeared after it was read: {path}") from exc
    if _file_identity(after_close) != _file_identity(before):
        raise HoldContractError(f"{label} changed after it was read: {path}")
    data = b"".join(chunks)
    if len(data) != before.st_size:
        raise HoldContractError(f"{label} size changed while it was read: {path}")
    return data


def _canonical_document_bytes(document: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            document,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _path_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _validate_document(
    raw_document: bytes,
    expected: dict[str, Any],
    label: str,
) -> None:
    try:
        document = json.loads(
            raw_document.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HoldContractError(f"{label} is not strict UTF-8 JSON") from exc
    if document != expected:
        raise HoldContractError(
            f"{label} does not match the reviewed incident contract"
        )
    if raw_document != _canonical_document_bytes(expected):
        raise HoldContractError(
            f"{label} is not the reviewed canonical serialization"
        )


def _validate_plan(raw_plan: bytes, context: str) -> None:
    actual_plan_sha256 = hashlib.sha256(raw_plan).hexdigest()
    if actual_plan_sha256 != RECOVERY_PLAN_SHA256:
        raise HoldContractError(
            f"recovery plan SHA-256 does not match the {context}: "
            f"expected {RECOVERY_PLAN_SHA256}, found {actual_plan_sha256}"
        )


def _validate_retired_hold_digest(raw_hold: bytes | None = None) -> None:
    canonical_sha256 = hashlib.sha256(
        _canonical_document_bytes(EXPECTED_HOLD_DOCUMENT)
    ).hexdigest()
    if canonical_sha256 != HOLD_SHA256:
        raise HoldContractError(
            "reviewed active hold digest does not match its canonical contract"
        )
    if raw_hold is not None and hashlib.sha256(raw_hold).hexdigest() != HOLD_SHA256:
        raise HoldContractError(
            "production release hold SHA-256 does not match the reviewed digest"
        )


def validate_active_hold(repo_root: Path = REPO_ROOT) -> None:
    """Require the exact reviewed hold and its immutable recovery plan."""

    hold_path = repo_root / HOLD_PATH
    retirement_path = repo_root / RETIREMENT_PATH
    plan_path = repo_root / RECOVERY_PLAN_PATH
    if _path_exists(retirement_path):
        if not _path_exists(hold_path):
            raise HoldContractError(
                "checkpoint recovery requires the active hold; its retirement "
                "record is already present"
            )
        raise HoldContractError(
            "active hold and retirement record must not exist simultaneously"
        )
    raw_hold = _read_stable_regular_file(
        hold_path,
        "production release hold",
        MAX_HOLD_BYTES,
    )
    raw_plan = _read_stable_regular_file(
        plan_path,
        "recovery plan",
        MAX_RECOVERY_PLAN_BYTES,
    )
    _validate_document(
        raw_hold,
        EXPECTED_HOLD_DOCUMENT,
        "production release hold",
    )
    _validate_retired_hold_digest(raw_hold)
    _validate_plan(raw_plan, "production hold")


def validate_retirement(repo_root: Path = REPO_ROOT) -> None:
    """Require the exact reviewed retirement and absence of the active hold."""

    hold_path = repo_root / HOLD_PATH
    retirement_path = repo_root / RETIREMENT_PATH
    plan_path = repo_root / RECOVERY_PLAN_PATH
    if _path_exists(hold_path):
        raise HoldContractError(
            "active hold and retirement record must not exist simultaneously"
        )
    raw_retirement = _read_stable_regular_file(
        retirement_path,
        "production release hold retirement record",
        MAX_HOLD_BYTES,
    )
    raw_plan = _read_stable_regular_file(
        plan_path,
        "recovery plan",
        MAX_RECOVERY_PLAN_BYTES,
    )
    _validate_document(
        raw_retirement,
        EXPECTED_RETIREMENT_DOCUMENT,
        "production release hold retirement record",
    )
    _validate_retired_hold_digest()
    _validate_plan(raw_plan, "production hold retirement record")


def resolve_release_action(repo_root: Path = REPO_ROOT) -> str:
    """Resolve only an exact active hold or exact reviewed retirement."""

    hold_path = repo_root / HOLD_PATH
    retirement_path = repo_root / RETIREMENT_PATH
    hold_exists = _path_exists(hold_path)
    retirement_exists = _path_exists(retirement_path)
    if hold_exists and retirement_exists:
        raise HoldContractError(
            "active hold and retirement record must not exist simultaneously"
        )
    if not hold_exists and not retirement_exists:
        raise HoldContractError(
            "production release requires either the active hold or its reviewed "
            "retirement record"
        )
    if hold_exists:
        validate_active_hold(repo_root)
        return HOLD_ACTION
    validate_retirement(repo_root)
    return DEPLOY_ACTION


def _write_github_output(path: Path, action: str) -> None:
    if action not in {HOLD_ACTION, DEPLOY_ACTION}:
        raise HoldContractError(f"unsupported production release action: {action}")
    with path.open("a", encoding="utf-8") as output:
        output.write(f"{RELEASE_ACTION_OUTPUT}={action}\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    resolve = subparsers.add_parser(
        "resolve",
        help="Emit hold or deploy for the production release guard.",
    )
    resolve.add_argument("--github-output", type=Path, required=True)
    subparsers.add_parser(
        "require-active",
        help="Require the exact hold before checkpoint recovery may access production.",
    )
    return parser


def main(
    argv: list[str] | None = None,
    *,
    repo_root: Path = REPO_ROOT,
) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "resolve":
            action = resolve_release_action(repo_root)
            _write_github_output(args.github_output, action)
            print(f"production release action: {action}")
            return 0
        validate_active_hold(repo_root)
        print(f"active production release hold verified for {INCIDENT_ID}")
        return 0
    except (HoldContractError, OSError) as exc:
        print(f"production release hold validation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
