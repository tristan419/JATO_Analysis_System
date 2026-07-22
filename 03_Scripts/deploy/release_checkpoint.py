#!/usr/bin/env python3
"""Persist and validate production release checkpoints without dependencies.

The checkpoint is the authoritative current state.  The JSONL journal is an
append-only transition history that can be used during incident review.  Both
files are deliberately private (mode 0600) and are fsynced before returning.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import datetime as dt
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = 1
PHASES = (
    "packaged",
    "transport_verified",
    "prepared",
    "source_install_started",
    "source_installed",
    "backup_verified",
    "migration_started",
    "migrated",
    "switch_started",
    "switched",
    "backend_healthy",
    "www_verified",
    "intl_deploy_started",
    "intl_verified",
    "parity_verified",
    "complete",
)
PHASE_INDEX = {phase: index for index, phase in enumerate(PHASES)}
STATUSES = ("in_progress", "completed", "failed")
RETRY_CLASSES = (
    "automatic",
    "inspect_then_resume",
    "manual_db_recovery",
    "rollback_required",
    "complete",
)
DANGEROUS_RETRY_CLASSES = frozenset(
    {"manual_db_recovery", "rollback_required"}
)

REPOSITORY_PATTERN = re.compile(
    r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"
)
GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
IDENTITY_FIELDS = {
    "repository",
    "commit",
    "archiveSha256",
    "archiveBytes",
    "runId",
    "runAttempt",
    "frontendIdentity",
    "frontendChecksum",
}
CHECKPOINT_FIELDS = {
    "schemaVersion",
    "sequence",
    "identity",
    "phase",
    "status",
    "retryClass",
    "updatedAt",
}
OPTIONAL_CHECKPOINT_FIELDS = {"message"}


class CheckpointError(ValueError):
    """Raised when checkpoint state is invalid or unsafe to resume."""


@dataclass(frozen=True)
class ReleaseIdentity:
    repository: str
    commit: str
    archiveSha256: str
    archiveBytes: int
    runId: int
    runAttempt: int
    frontendIdentity: str
    frontendChecksum: str

    @classmethod
    def create(
        cls,
        *,
        repository: str,
        commit: str,
        archive_sha256: str,
        archive_bytes: int,
        run_id: int,
        run_attempt: int,
        frontend_identity: str,
        frontend_checksum: str,
    ) -> "ReleaseIdentity":
        return cls(
            repository=_require_repository(repository),
            commit=_require_hash(commit, GIT_SHA_PATTERN, "commit", 40),
            archiveSha256=_require_hash(
                archive_sha256,
                SHA256_PATTERN,
                "archive SHA256",
                64,
            ),
            archiveBytes=_require_positive_int(archive_bytes, "archive bytes"),
            runId=_require_positive_int(run_id, "run id"),
            runAttempt=_require_positive_int(run_attempt, "run attempt"),
            frontendIdentity=_require_frontend_identity(frontend_identity),
            frontendChecksum=_require_hash(
                frontend_checksum,
                SHA256_PATTERN,
                "frontend checksum",
                64,
            ),
        )

    @classmethod
    def from_mapping(cls, value: object) -> "ReleaseIdentity":
        if not isinstance(value, dict):
            raise CheckpointError("checkpoint identity must be a JSON object")
        unexpected = set(value) - IDENTITY_FIELDS
        missing = IDENTITY_FIELDS - set(value)
        if missing or unexpected:
            raise CheckpointError(
                "checkpoint identity fields are invalid: "
                f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
            )
        return cls.create(
            repository=value["repository"],
            commit=value["commit"],
            archive_sha256=value["archiveSha256"],
            archive_bytes=value["archiveBytes"],
            run_id=value["runId"],
            run_attempt=value["runAttempt"],
            frontend_identity=value["frontendIdentity"],
            frontend_checksum=value["frontendChecksum"],
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _require_repository(value: object) -> str:
    if not isinstance(value, str):
        raise CheckpointError("repository must be a string in owner/name form")
    normalized = value.strip()
    if not REPOSITORY_PATTERN.fullmatch(normalized):
        raise CheckpointError("repository must be in owner/name form")
    return normalized


def _require_hash(
    value: object,
    pattern: re.Pattern[str],
    field: str,
    length: int,
) -> str:
    if not isinstance(value, str):
        raise CheckpointError(f"{field} must be a {length}-character lowercase hex value")
    normalized = value.strip().lower()
    if not pattern.fullmatch(normalized):
        raise CheckpointError(f"{field} must be a {length}-character lowercase hex value")
    return normalized


def _require_positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CheckpointError(f"{field} must be a positive integer")
    return value


def _require_frontend_identity(value: object) -> str:
    if not isinstance(value, str):
        raise CheckpointError("frontend identity must be a non-empty string")
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > 2048
        or any(character.isspace() or ord(character) < 32 for character in normalized)
    ):
        raise CheckpointError(
            "frontend identity must be a non-empty, whitespace-free string"
        )
    return normalized


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00",
        "Z",
    )


def _require_timestamp(value: object) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise CheckpointError("updatedAt must be a UTC ISO8601 timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CheckpointError("updatedAt must be a UTC ISO8601 timestamp") from exc
    if parsed.tzinfo != dt.timezone.utc:
        raise CheckpointError("updatedAt must be a UTC ISO8601 timestamp")
    return value


def _require_choice(value: object, allowed: Sequence[str], field: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise CheckpointError(f"{field} must be one of: {', '.join(allowed)}")
    return value


def _require_message(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CheckpointError("message must be a non-empty string when provided")
    normalized = value.strip()
    if len(normalized) > 4096:
        raise CheckpointError("message must not exceed 4096 characters")
    return normalized


def _validate_terminal_contract(
    *,
    phase: str,
    status: str,
    retry_class: str,
) -> None:
    if phase == "complete":
        if status != "completed" or retry_class != "complete":
            raise CheckpointError(
                "complete phase requires status=completed and retryClass=complete"
            )
    elif retry_class == "complete":
        raise CheckpointError("retryClass=complete is only valid for complete phase")


def validate_checkpoint(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise CheckpointError("checkpoint root must be a JSON object")
    keys = set(payload)
    missing = CHECKPOINT_FIELDS - keys
    unexpected = keys - CHECKPOINT_FIELDS - OPTIONAL_CHECKPOINT_FIELDS
    if missing or unexpected:
        raise CheckpointError(
            "checkpoint fields are invalid: "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )
    if (
        isinstance(payload["schemaVersion"], bool)
        or payload["schemaVersion"] != SCHEMA_VERSION
    ):
        raise CheckpointError(
            f"unsupported checkpoint schemaVersion: {payload['schemaVersion']!r}"
        )
    sequence = _require_positive_int(payload["sequence"], "sequence")
    identity = ReleaseIdentity.from_mapping(payload["identity"])
    phase = _require_choice(payload["phase"], PHASES, "phase")
    status = _require_choice(payload["status"], STATUSES, "status")
    retry_class = _require_choice(
        payload["retryClass"],
        RETRY_CLASSES,
        "retryClass",
    )
    _validate_terminal_contract(
        phase=phase,
        status=status,
        retry_class=retry_class,
    )
    validated: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "sequence": sequence,
        "identity": identity.to_dict(),
        "phase": phase,
        "status": status,
        "retryClass": retry_class,
        "updatedAt": _require_timestamp(payload["updatedAt"]),
    }
    if "message" in payload:
        validated["message"] = _require_message(payload["message"])
    return validated


def _reject_symlink(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if stat.S_ISLNK(mode):
        raise CheckpointError(f"{label} must not be a symlink: {path}")


def load_checkpoint(path: Path) -> dict[str, Any]:
    _reject_symlink(path, "checkpoint")
    if not path.is_file():
        raise CheckpointError(f"checkpoint does not exist: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"invalid checkpoint JSON in {path}: {exc}") from exc
    return validate_checkpoint(raw)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically replace *path* with a private, durable JSON document."""

    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink(path, "checkpoint")
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(file_descriptor, 0o600)
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            os.close(file_descriptor)
        except OSError:
            pass
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


def append_journal(path: Path, checkpoint: Mapping[str, Any]) -> None:
    """Append one fsynced checkpoint transition to a private JSONL journal."""

    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink(path, "journal")
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        line = (
            json.dumps(
                {"event": "checkpoint_transition", **checkpoint},
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        offset = 0
        while offset < len(line):
            written = os.write(descriptor, line[offset:])
            if written <= 0:
                raise OSError("journal append made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _assert_same_identity(
    existing: Mapping[str, Any],
    expected: ReleaseIdentity,
) -> None:
    actual = ReleaseIdentity.from_mapping(existing.get("identity"))
    if actual != expected:
        mismatches = [
            field
            for field in sorted(IDENTITY_FIELDS)
            if actual.to_dict()[field] != expected.to_dict()[field]
        ]
        raise CheckpointError(
            "checkpoint identity mismatch; refusing to mix releases: "
            + ", ".join(mismatches)
        )


def write_checkpoint(
    *,
    checkpoint_path: Path,
    journal_path: Path,
    identity: ReleaseIdentity,
    phase: str,
    status: str,
    retry_class: str,
    message: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    if checkpoint_path.resolve() == journal_path.resolve():
        raise CheckpointError("checkpoint and journal paths must be different")
    phase = _require_choice(phase, PHASES, "phase")
    status = _require_choice(status, STATUSES, "status")
    retry_class = _require_choice(retry_class, RETRY_CLASSES, "retryClass")
    _validate_terminal_contract(
        phase=phase,
        status=status,
        retry_class=retry_class,
    )

    sequence = 1
    if checkpoint_path.exists() or checkpoint_path.is_symlink():
        existing = load_checkpoint(checkpoint_path)
        _assert_same_identity(existing, identity)
        existing_phase = existing["phase"]
        if PHASE_INDEX[phase] < PHASE_INDEX[existing_phase]:
            raise CheckpointError(
                f"phase regression is forbidden: {existing_phase} -> {phase}"
            )
        if existing_phase == "complete":
            raise CheckpointError("complete checkpoint is immutable")
        sequence = existing["sequence"] + 1

    checkpoint: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "sequence": sequence,
        "identity": identity.to_dict(),
        "phase": phase,
        "status": status,
        "retryClass": retry_class,
        "updatedAt": _require_timestamp(now) if now else _utc_now(),
    }
    if message is not None:
        checkpoint["message"] = _require_message(message)
    checkpoint = validate_checkpoint(checkpoint)

    # The journal is written first.  If power is lost between these two fsyncs,
    # it contains a safe superset of attempted transitions and the checkpoint
    # remains the authoritative state.
    append_journal(journal_path, checkpoint)
    atomic_write_json(checkpoint_path, checkpoint)
    return checkpoint


def assert_resumable(
    *,
    checkpoint_path: Path,
    expected_identity: ReleaseIdentity,
) -> dict[str, Any]:
    checkpoint = load_checkpoint(checkpoint_path)
    _assert_same_identity(checkpoint, expected_identity)
    phase = checkpoint["phase"]
    status = checkpoint["status"]
    retry_class = checkpoint["retryClass"]

    if phase == "complete":
        return {
            "decision": "already-complete",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    if phase == "migration_started" and status in {"in_progress", "failed"}:
        raise CheckpointError(
            "database migration outcome is unknown; manual database recovery is "
            "required before resume"
        )
    if retry_class == "manual_db_recovery":
        raise CheckpointError("checkpoint requires manual database recovery")
    if retry_class == "rollback_required":
        raise CheckpointError("checkpoint requires rollback before another release")

    return {
        "decision": "resumable",
        "phase": phase,
        "status": status,
        "retryClass": retry_class,
        "sequence": checkpoint["sequence"],
    }


def _require_real_directory(path: Path, label: str) -> None:
    """Require a real directory without accepting a symlink at *path*."""

    _reject_symlink(path, label)
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as exc:
        raise CheckpointError(f"{label} does not exist: {path}") from exc
    if not stat.S_ISDIR(mode):
        raise CheckpointError(f"{label} must be a directory: {path}")


def _absolute_without_resolution(path: Path) -> Path:
    """Normalize an absolute path without following any symlink."""

    return Path(os.path.abspath(os.fspath(path)))


def _cross_release_is_settled(checkpoint: Mapping[str, Any]) -> bool:
    """Return whether an older release has crossed the safe health boundary."""

    phase = checkpoint["phase"]
    if phase == "complete":
        return True
    return (
        PHASE_INDEX[phase] >= PHASE_INDEX["backend_healthy"]
        and checkpoint["status"] == "completed"
    )


def assert_cross_release_safe(
    *,
    checkpoints_root: Path,
    current_checkpoint: Path,
    expected_identity: ReleaseIdentity,
) -> dict[str, Any]:
    """Fail closed if another release left production in an ambiguous state.

    The caller must hold the server-wide deployment lock.  This function
    validates the complete two-level checkpoint namespace instead of trusting
    filenames alone.  Evidence documents are deliberately not interpreted as
    checkpoints, but their path and file type are still constrained so a
    symlink or unexpected entry cannot hide state from the scan.
    """

    checkpoints_root = _absolute_without_resolution(checkpoints_root)
    current_checkpoint = _absolute_without_resolution(current_checkpoint)
    expected_current = checkpoints_root / expected_identity.commit / (
        f"{expected_identity.archiveSha256}.json"
    )
    if current_checkpoint != expected_current:
        raise CheckpointError(
            "current checkpoint path does not match the expected release identity"
        )

    _require_real_directory(checkpoints_root, "checkpoints root")
    checkpoint_count = 0
    evidence_count = 0
    current_present = False

    for commit_entry in sorted(checkpoints_root.iterdir(), key=lambda item: item.name):
        _require_real_directory(commit_entry, "checkpoint commit directory")
        if not GIT_SHA_PATTERN.fullmatch(commit_entry.name):
            raise CheckpointError(
                f"invalid checkpoint commit directory name: {commit_entry}"
            )

        for entry in sorted(commit_entry.iterdir(), key=lambda item: item.name):
            _reject_symlink(entry, "checkpoint namespace entry")
            try:
                entry_mode = entry.lstat().st_mode
            except FileNotFoundError as exc:
                raise CheckpointError(
                    f"checkpoint namespace changed during validation: {entry}"
                ) from exc
            if not stat.S_ISREG(entry_mode):
                raise CheckpointError(
                    f"checkpoint namespace entry must be a regular file: {entry}"
                )

            if entry.name.endswith(".evidence.json"):
                archive_name = entry.name[: -len(".evidence.json")]
                if not SHA256_PATTERN.fullmatch(archive_name):
                    raise CheckpointError(
                        f"invalid release evidence filename: {entry}"
                    )
                evidence_count += 1
                continue

            if not entry.name.endswith(".json"):
                raise CheckpointError(
                    f"unexpected checkpoint namespace entry: {entry}"
                )
            archive_name = entry.name[: -len(".json")]
            if not SHA256_PATTERN.fullmatch(archive_name):
                raise CheckpointError(f"invalid checkpoint filename: {entry}")

            payload = load_checkpoint(entry)
            identity = ReleaseIdentity.from_mapping(payload["identity"])
            if identity.repository != expected_identity.repository:
                raise CheckpointError(
                    f"checkpoint repository does not match state namespace: {entry}"
                )
            if identity.commit != commit_entry.name:
                raise CheckpointError(
                    f"checkpoint commit identity does not match its path: {entry}"
                )
            if identity.archiveSha256 != archive_name:
                raise CheckpointError(
                    f"checkpoint archive identity does not match its path: {entry}"
                )

            checkpoint_count += 1
            if entry == current_checkpoint:
                _assert_same_identity(payload, expected_identity)
                current_present = True
                continue

            retry_class = payload["retryClass"]
            phase = payload["phase"]
            status = payload["status"]
            if retry_class in DANGEROUS_RETRY_CLASSES:
                raise CheckpointError(
                    "another release requires operator recovery before a new "
                    f"artifact can deploy: path={entry}, phase={phase}, "
                    f"status={status}, retryClass={retry_class}"
                )
            if (
                PHASE_INDEX[phase] >= PHASE_INDEX["source_install_started"]
                and not _cross_release_is_settled(payload)
            ):
                raise CheckpointError(
                    "another release may have mutated production without reaching "
                    "a completed backend health boundary; resume that exact release "
                    f"first: path={entry}, phase={phase}, status={status}, "
                    f"retryClass={retry_class}"
                )

    return {
        "decision": "cross-release-safe",
        "checkpointsScanned": checkpoint_count,
        "evidenceFilesExcluded": evidence_count,
        "currentCheckpointPresent": current_present,
    }


def _positive_int_argument(value: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return int(value)


def _add_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--archive-bytes", required=True, type=_positive_int_argument)
    parser.add_argument("--run-id", required=True, type=_positive_int_argument)
    parser.add_argument("--run-attempt", required=True, type=_positive_int_argument)
    parser.add_argument("--frontend-identity", required=True)
    parser.add_argument("--frontend-checksum", required=True)


def _identity_from_arguments(arguments: argparse.Namespace) -> ReleaseIdentity:
    return ReleaseIdentity.create(
        repository=arguments.repository,
        commit=arguments.commit,
        archive_sha256=arguments.archive_sha256,
        archive_bytes=arguments.archive_bytes,
        run_id=arguments.run_id,
        run_attempt=arguments.run_attempt,
        frontend_identity=arguments.frontend_identity,
        frontend_checksum=arguments.frontend_checksum,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write and validate durable production release checkpoints.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    write_parser = subparsers.add_parser("write", help="write one transition")
    write_parser.add_argument("--checkpoint", required=True, type=Path)
    write_parser.add_argument("--journal", required=True, type=Path)
    _add_identity_arguments(write_parser)
    write_parser.add_argument("--phase", required=True, choices=PHASES)
    write_parser.add_argument("--status", required=True, choices=STATUSES)
    write_parser.add_argument(
        "--retry-class",
        required=True,
        choices=RETRY_CLASSES,
    )
    write_parser.add_argument("--message")

    show_parser = subparsers.add_parser("show", help="show a validated checkpoint")
    show_parser.add_argument("--checkpoint", required=True, type=Path)

    resume_parser = subparsers.add_parser(
        "assert-resumable",
        help="fail closed unless an exact release can be resumed safely",
    )
    resume_parser.add_argument("--checkpoint", required=True, type=Path)
    _add_identity_arguments(resume_parser)

    cross_release_parser = subparsers.add_parser(
        "assert-cross-release-safe",
        help=(
            "fail closed if any non-current checkpoint leaves production in an "
            "ambiguous state"
        ),
    )
    cross_release_parser.add_argument(
        "--checkpoints-root",
        required=True,
        type=Path,
    )
    cross_release_parser.add_argument(
        "--current-checkpoint",
        required=True,
        type=Path,
    )
    _add_identity_arguments(cross_release_parser)
    return parser


def _print_json(payload: Mapping[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "write":
            result = write_checkpoint(
                checkpoint_path=arguments.checkpoint,
                journal_path=arguments.journal,
                identity=_identity_from_arguments(arguments),
                phase=arguments.phase,
                status=arguments.status,
                retry_class=arguments.retry_class,
                message=arguments.message,
            )
        elif arguments.command == "show":
            result = load_checkpoint(arguments.checkpoint)
        elif arguments.command == "assert-resumable":
            result = assert_resumable(
                checkpoint_path=arguments.checkpoint,
                expected_identity=_identity_from_arguments(arguments),
            )
        elif arguments.command == "assert-cross-release-safe":
            result = assert_cross_release_safe(
                checkpoints_root=arguments.checkpoints_root,
                current_checkpoint=arguments.current_checkpoint,
                expected_identity=_identity_from_arguments(arguments),
            )
        else:  # pragma: no cover - argparse enforces the command set.
            raise CheckpointError(f"unsupported command: {arguments.command}")
    except (CheckpointError, OSError) as exc:
        print(f"release checkpoint error: {exc}", file=sys.stderr)
        return 2
    _print_json(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
