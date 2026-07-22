#!/usr/bin/env python3
"""Privileged, fail-closed verification for private release evidence.

Production database manifests and dumps are intentionally owned by root and
kept below mode-0700 directories.  Deploy scripts invoke this helper through
``sudo -n`` so they can validate that private chain without copying its content
or relaxing permissions.  Successful output is a small, non-sensitive JSON
summary; failures expose only a stable category.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, BinaryIO, Mapping, Sequence

from release_checkpoint import PHASE_INDEX, ReleaseIdentity, validate_checkpoint


JSON_SIZE_LIMIT = 1024 * 1024
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
EVIDENCE_BINDING_PATTERN = re.compile(
    r"(?:^|[; ])evidence_path=(\S+) "
    r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])"
)
REVISION_PATTERN = re.compile(r"(?m)^([0-9]{8}_[0-9]{4})\b")
EVIDENCE_FIELDS = {"identity", "backup", "migration"}
BACKUP_FIELDS = {"manifestPath", "manifestBytes", "manifestSha256"}
MIGRATION_FIELDS = {
    "status",
    "preRevision",
    "targetRevision",
    "resultRevision",
}


class EvidenceVerificationError(ValueError):
    """A release evidence chain is invalid or cannot be safely read."""

    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def _fail(category: str) -> None:
    raise EvidenceVerificationError(category)


def _require_exact_fields(
    payload: object,
    fields: set[str],
    category: str,
) -> Mapping[str, Any]:
    if not isinstance(payload, dict) or set(payload) != fields:
        _fail(category)
    return payload


def _require_positive_int(value: object, category: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _fail(category)
    return value


def _require_sha256(value: object, category: str) -> str:
    if not isinstance(value, str) or not SHA256_PATTERN.fullmatch(value):
        _fail(category)
    return value


def _require_private(mode: int, category: str) -> None:
    if stat.S_IMODE(mode) & 0o077:
        _fail(category)


def _open_regular(
    path: Path,
    category: str,
    *,
    expected_owner_uid: int | None = None,
) -> tuple[BinaryIO, os.stat_result]:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        _fail(category)
    try:
        current = os.fstat(descriptor)
        if not stat.S_ISREG(current.st_mode):
            _fail(category)
        if expected_owner_uid is not None and current.st_uid != expected_owner_uid:
            _fail(category)
        _require_private(current.st_mode, category)
        return os.fdopen(descriptor, "rb", closefd=True), current
    except BaseException:
        os.close(descriptor)
        raise


def _read_regular_json(
    path: Path,
    category: str,
    *,
    expected_owner_uid: int | None = None,
) -> tuple[Mapping[str, Any], bytes]:
    handle, before = _open_regular(
        path,
        category,
        expected_owner_uid=expected_owner_uid,
    )
    with handle:
        if before.st_size <= 0 or before.st_size > JSON_SIZE_LIMIT:
            _fail(category)
        raw = handle.read(JSON_SIZE_LIMIT + 1)
        after = os.fstat(handle.fileno())
    if (
        len(raw) != before.st_size
        or len(raw) > JSON_SIZE_LIMIT
        or after.st_dev != before.st_dev
        or after.st_ino != before.st_ino
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
    ):
        _fail(category)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        _fail(category)
    if not isinstance(payload, dict):
        _fail(category)
    return payload, raw


def _verify_regular_digest(
    path: Path,
    expected_bytes: int,
    expected_sha256: str,
    category: str,
    *,
    expected_owner_uid: int | None = None,
) -> None:
    handle, before = _open_regular(
        path,
        category,
        expected_owner_uid=expected_owner_uid,
    )
    with handle:
        if before.st_size != expected_bytes:
            _fail(category)
        digest = hashlib.sha256()
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(handle.fileno())
    if (
        after.st_dev != before.st_dev
        or after.st_ino != before.st_ino
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or digest.hexdigest() != expected_sha256
    ):
        _fail(category)


def _require_private_directory(
    path: Path,
    category: str,
    *,
    expected_owner_uid: int | None = None,
) -> None:
    try:
        current = path.lstat()
    except OSError:
        _fail(category)
    if not stat.S_ISDIR(current.st_mode) or stat.S_ISLNK(current.st_mode):
        _fail(category)
    if expected_owner_uid is not None and current.st_uid != expected_owner_uid:
        _fail(category)
    _require_private(current.st_mode, category)


def _require_child(
    path_value: object,
    expected_parent: Path,
    name_pattern: re.Pattern[str],
    category: str,
) -> Path:
    if not isinstance(path_value, str) or not path_value.startswith("/"):
        _fail(category)
    path = Path(os.path.abspath(path_value))
    if path.parent != expected_parent or not name_pattern.fullmatch(path.name):
        _fail(category)
    return path


def _validate_migration(
    migration_value: object,
    checkpoint_phase: str,
    database_required: bool,
) -> str:
    migration = _require_exact_fields(
        migration_value,
        MIGRATION_FIELDS,
        "migration_invalid",
    )
    status_value = migration.get("status")
    if status_value not in {
        "not_started",
        "in_progress",
        "completed",
        "not_required",
    }:
        _fail("migration_invalid")
    status = str(status_value)
    phase_rank = PHASE_INDEX[checkpoint_phase]
    if phase_rank < PHASE_INDEX["migration_started"]:
        if status != "not_started":
            _fail("migration_invalid")
    elif phase_rank < PHASE_INDEX["migrated"]:
        if status != "in_progress":
            _fail("migration_invalid")
    elif status not in {"completed", "not_required"}:
        _fail("migration_invalid")

    for field in ("preRevision", "targetRevision", "resultRevision"):
        value = migration.get(field)
        if value is not None and not isinstance(value, str):
            _fail("migration_invalid")

    if status in {"in_progress", "completed"}:
        target = set(REVISION_PATTERN.findall(migration.get("targetRevision") or ""))
        if not target:
            _fail("migration_invalid")
        if status == "completed":
            result = set(
                REVISION_PATTERN.findall(migration.get("resultRevision") or "")
            )
            if result != target:
                _fail("migration_invalid")
        elif migration.get("resultRevision"):
            _fail("migration_invalid")
    elif status == "not_required":
        if database_required or any(
            migration.get(field)
            for field in ("preRevision", "targetRevision", "resultRevision")
        ):
            _fail("migration_invalid")
    return status


def verify_release_evidence(
    *,
    checkpoint_path: Path,
    evidence_path: Path,
    backup_root: Path,
    expected_identity: ReleaseIdentity,
    backup_owner_uid: int = 0,
) -> dict[str, str]:
    """Verify the complete private evidence chain and return a safe summary."""

    expected_evidence_path = checkpoint_path.with_name(
        f"{expected_identity.archiveSha256}.evidence.json"
    )
    if evidence_path != expected_evidence_path:
        _fail("evidence_path_invalid")

    checkpoint_value, _ = _read_regular_json(
        checkpoint_path,
        "checkpoint_invalid",
    )
    try:
        checkpoint = validate_checkpoint(checkpoint_value)
    except ValueError:
        _fail("checkpoint_invalid")
    if checkpoint.get("identity") != expected_identity.to_dict():
        _fail("identity_mismatch")

    message = checkpoint.get("message") or ""
    bindings = EVIDENCE_BINDING_PATTERN.findall(message)
    if len(bindings) != 1:
        _fail("evidence_binding_invalid")
    bound_path, bound_sha256 = bindings[0]
    if Path(bound_path) != evidence_path:
        _fail("evidence_binding_invalid")

    evidence_value, evidence_raw = _read_regular_json(
        evidence_path,
        "evidence_invalid",
    )
    if hashlib.sha256(evidence_raw).hexdigest() != bound_sha256:
        _fail("evidence_digest_mismatch")
    evidence = _require_exact_fields(
        evidence_value,
        EVIDENCE_FIELDS,
        "evidence_invalid",
    )
    if evidence.get("identity") != expected_identity.to_dict():
        _fail("identity_mismatch")

    backup_root = Path(os.path.abspath(backup_root))
    manifests_root = backup_root / "manifests"
    dumps_root = backup_root / "pg"
    _require_private_directory(
        backup_root,
        "backup_root_invalid",
        expected_owner_uid=backup_owner_uid,
    )
    _require_private_directory(
        manifests_root,
        "backup_root_invalid",
        expected_owner_uid=backup_owner_uid,
    )
    _require_private_directory(
        dumps_root,
        "backup_root_invalid",
        expected_owner_uid=backup_owner_uid,
    )

    backup = _require_exact_fields(
        evidence.get("backup"),
        BACKUP_FIELDS,
        "backup_manifest_invalid",
    )
    manifest_bytes = _require_positive_int(
        backup.get("manifestBytes"),
        "backup_manifest_invalid",
    )
    manifest_sha256 = _require_sha256(
        backup.get("manifestSha256"),
        "backup_manifest_invalid",
    )
    manifest_path = _require_child(
        backup.get("manifestPath"),
        manifests_root,
        re.compile(r"backup-[0-9]{8}-[0-9]{6}\.json"),
        "backup_manifest_invalid",
    )
    manifest_value, manifest_raw = _read_regular_json(
        manifest_path,
        "backup_manifest_invalid",
        expected_owner_uid=backup_owner_uid,
    )
    if (
        len(manifest_raw) != manifest_bytes
        or hashlib.sha256(manifest_raw).hexdigest() != manifest_sha256
    ):
        _fail("backup_manifest_invalid")

    database_value = manifest_value.get("database")
    if not isinstance(database_value, dict):
        _fail("database_backup_invalid")
    database = database_value
    enabled = database.get("enabled")
    required = database.get("required")
    if not isinstance(enabled, bool) or not isinstance(required, bool):
        _fail("database_backup_invalid")
    database_status = database.get("status")
    if database_status not in {"completed", "skipped"}:
        _fail("database_backup_invalid")
    if (enabled or required) and database_status != "completed":
        _fail("database_backup_invalid")

    if database_status == "completed":
        dump_bytes = _require_positive_int(
            database.get("dumpBytes"),
            "database_dump_invalid",
        )
        dump_sha256 = _require_sha256(
            database.get("dumpSha256"),
            "database_dump_invalid",
        )
        dump_path = _require_child(
            database.get("dumpPath"),
            dumps_root,
            re.compile(r"jato-[0-9]{8}-[0-9]{6}\.dump"),
            "database_dump_invalid",
        )
        _verify_regular_digest(
            dump_path,
            dump_bytes,
            dump_sha256,
            "database_dump_invalid",
            expected_owner_uid=backup_owner_uid,
        )
    elif any(
        (
            database.get("dumpPath"),
            database.get("dumpBytes"),
            database.get("dumpSha256"),
        )
    ):
        _fail("database_backup_invalid")

    migration_status = _validate_migration(
        evidence.get("migration"),
        str(checkpoint["phase"]),
        enabled or required,
    )
    return {
        "checkpointPhase": str(checkpoint["phase"]),
        "databaseBackup": str(database_status),
        "evidenceSha256": bound_sha256,
        "migrationStatus": migration_status,
        "status": "verified",
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("checkpoint", type=Path)
    verify.add_argument("evidence", type=Path)
    verify.add_argument("--backup-root", required=True, type=Path)
    verify.add_argument("--repository", required=True)
    verify.add_argument("--commit", required=True)
    verify.add_argument("--archive-sha256", required=True)
    verify.add_argument("--archive-bytes", required=True, type=int)
    verify.add_argument("--run-id", required=True, type=int)
    verify.add_argument("--run-attempt", required=True, type=int)
    verify.add_argument("--frontend-identity", required=True)
    verify.add_argument("--frontend-checksum", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        expected_identity = ReleaseIdentity.create(
            repository=args.repository,
            commit=args.commit,
            archive_sha256=args.archive_sha256,
            archive_bytes=args.archive_bytes,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
            frontend_identity=args.frontend_identity,
            frontend_checksum=args.frontend_checksum,
        )
        summary = verify_release_evidence(
            checkpoint_path=args.checkpoint,
            evidence_path=args.evidence,
            backup_root=args.backup_root,
            expected_identity=expected_identity,
        )
    except EvidenceVerificationError as exc:
        print(
            "[ERROR] release evidence verification failed "
            f"({exc.category})",
            file=sys.stderr,
        )
        return 1
    except (OSError, ValueError, TypeError):
        print(
            "[ERROR] release evidence verification failed (invalid_input)",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
