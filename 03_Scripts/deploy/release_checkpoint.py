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
import hashlib
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
    "candidate_prepare_aborted",
    "candidate_ready",
    "candidate_discarded",
    "active_update_started",
    "active_update_verified",
    "active_updated",
    "candidate_released",
    "switch_started",
    "switched",
    "rollback_started",
    "rollback_completed",
    "pre_switch_aborted",
    "backend_healthy",
    "www_verified",
    "intl_deploy_started",
    "intl_verified",
    "parity_verified",
    "complete",
)
PHASE_INDEX = {phase: index for index, phase in enumerate(PHASES)}
ALLOWED_PHASE_TRANSITIONS = {
    "packaged": frozenset({"transport_verified"}),
    "transport_verified": frozenset({"prepared"}),
    "prepared": frozenset({"source_install_started", "backup_verified"}),
    "source_install_started": frozenset({"source_installed"}),
    "source_installed": frozenset({"backup_verified"}),
    "backup_verified": frozenset({"migration_started", "migrated"}),
    "migration_started": frozenset({"migrated"}),
    "migrated": frozenset({"candidate_prepare_aborted", "candidate_ready"}),
    "candidate_prepare_aborted": frozenset(),
    "candidate_ready": frozenset(
        {"candidate_discarded", "active_update_started", "switch_started"}
    ),
    "candidate_discarded": frozenset(),
    "active_update_started": frozenset(
        {"active_update_verified", "rollback_started"}
    ),
    "active_update_verified": frozenset(
        {"active_updated", "rollback_started"}
    ),
    "active_updated": frozenset(
        {"candidate_released", "www_verified", "rollback_started"}
    ),
    "candidate_released": frozenset(),
    "switch_started": frozenset({"switched", "rollback_started"}),
    "switched": frozenset({"backend_healthy", "rollback_started"}),
    "rollback_started": frozenset({"rollback_completed"}),
    "rollback_completed": frozenset({"candidate_discarded"}),
    "pre_switch_aborted": frozenset(),
    "backend_healthy": frozenset({"www_verified"}),
    "www_verified": frozenset({"intl_deploy_started"}),
    "intl_deploy_started": frozenset({"intl_verified"}),
    "intl_verified": frozenset({"parity_verified"}),
    "parity_verified": frozenset({"complete"}),
    "complete": frozenset(),
}
ALLOWED_SAME_PHASE_STATUS_TRANSITIONS = {
    "in_progress": frozenset({"completed", "failed"}),
    "failed": frozenset({"completed"}),
    "completed": frozenset(),
}
REQUIRED_PREDECESSOR_STATUSES = {
    ("migrated", "candidate_ready"): frozenset({"completed"}),
    ("migrated", "candidate_prepare_aborted"): frozenset({"completed"}),
    ("candidate_ready", "candidate_discarded"): frozenset({"completed"}),
    ("candidate_ready", "active_update_started"): frozenset({"completed"}),
    ("candidate_ready", "switch_started"): frozenset({"completed"}),
    ("active_update_started", "active_update_verified"): frozenset(
        {"in_progress"}
    ),
    ("active_update_started", "rollback_started"): frozenset(
        {"in_progress", "failed"}
    ),
    ("active_update_verified", "active_updated"): frozenset({"completed"}),
    ("active_update_verified", "rollback_started"): frozenset({"completed"}),
    ("active_updated", "candidate_released"): frozenset({"completed"}),
    ("active_updated", "www_verified"): frozenset({"completed"}),
    ("active_updated", "rollback_started"): frozenset({"completed"}),
    ("switched", "backend_healthy"): frozenset({"completed"}),
    ("rollback_started", "rollback_completed"): frozenset(
        {"in_progress", "failed"}
    ),
    ("rollback_completed", "candidate_discarded"): frozenset({"completed"}),
    ("parity_verified", "complete"): frozenset({"completed"}),
}
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
SCHEMA_V3_AUTHORIZATION_FIELDS = {
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
    "authorizationSha256",
}
REVISION_PATTERN = re.compile(r"(?m)^([0-9]{8}_[0-9]{4})\b")
RECOVERY_RECEIPT_SCHEMA_VERSIONS = frozenset({1, 2, 3})
RECOVERY_MIGRATION_STATUS_BY_SCHEMA = {
    1: "not_required",
    2: "completed",
    3: "completed",
}
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
MAX_RELEASE_METADATA_BYTES = 64 * 1024
MAX_CHECKPOINT_JOURNAL_BYTES = 16 * 1024 * 1024
PRE_SWITCH_ABORT_PHASE = "pre_switch_aborted"
RESIDUE_INCIDENT_ID = "2026-08-03-29df-pre-switch-candidate-residue"
RESIDUE_TARGET_COMMIT = "29df5e6e667351f09305783932b34e5438d6a9d5"
RESIDUE_DEVICE = 64770
RESIDUE_OWNER_UID = 0
RESIDUE_OWNER_GID = 0
RESIDUE_RETAINED_OWNER_UID = 1000
RESIDUE_RETAINED_OWNER_GID = 1001
RESIDUE_PREVIOUS_METADATA_ROOT = Path("/var/lib/jato-release/previous-metadata")
RESIDUE_CANONICAL_NGINX = Path(
    "/etc/nginx/sites-available/jato_fullstack.conf"
)
RESIDUE_CANONICAL_NGINX_IDENTITY = {
    "kind": "file",
    "device": RESIDUE_DEVICE,
    "inode": 789351,
    "uid": RESIDUE_OWNER_UID,
    "gid": RESIDUE_OWNER_GID,
    "mode": "0644",
    "nlink": 1,
    "bytes": 3795,
    "mtimeNs": 1783042607281907362,
    "sha256": "964c351bbed725a36da517c06ce7ef82ff9d11046e8329f02a118f638a32aec4",
    "target": None,
    "targetSha256": None,
}
RESIDUE_QUARANTINE_ROOT = Path(
    "/var/lib/jato-release/recovery-quarantine"
) / RESIDUE_INCIDENT_ID
RESIDUE_MAINTENANCE_MARKER = Path("/var/lib/jato-release/deployment-maintenance")
RESIDUE_REQUIRED_ABSENT_PATHS = frozenset(
    {
        Path("/etc/jato-fullstack/nginx/active-release.conf"),
        Path("/opt/jato/active"),
        Path(
            "/run/systemd/system.control/"
            "jato-fullstack-backend@8001.service.d"
        ),
        Path("/var/cache/jato-candidate-8001"),
        Path("/var/cache/private/jato-candidate-8001"),
        Path(
            "/var/lib/jato-release/backend-template.pre-"
            f"{RESIDUE_TARGET_COMMIT}.service"
        ),
        Path(
            "/var/lib/jato-release/backend-template.pre-"
            f"{RESIDUE_TARGET_COMMIT}.service.state"
        ),
        Path(f"/var/lib/jato-release/nginx-preimage-{RESIDUE_TARGET_COMMIT}"),
        Path("/var/lib/jato-release/scheduler-state.tsv"),
    }
)
RESIDUE_ROOT_ATTESTED_ABSENT_PATHS = frozenset(
    {Path("/var/cache/private/jato-candidate-8001")}
)
RESIDUE_SOURCE_PATHS = {
    "maintenance_marker": RESIDUE_MAINTENANCE_MARKER,
    "candidate_slot_link": Path("/opt/jato/slots/8001/current"),
    "candidate_slot_env": Path("/etc/jato-fullstack/slots/8001.env"),
    "candidate_explicit_unit": Path(
        "/etc/systemd/system/jato-fullstack-backend@8001.service"
    ),
    "candidate_sandbox_dropin": Path(
        "/etc/systemd/system/jato-fullstack-backend@8001.service.d/"
        "10-candidate-sandbox.conf"
    ),
    "candidate_cpu_quota_dropin": Path(
        "/etc/systemd/system.control/jato-fullstack-backend@8001.service.d/"
        "50-CPUQuota.conf"
    ),
    "candidate_memory_high_dropin": Path(
        "/etc/systemd/system.control/jato-fullstack-backend@8001.service.d/"
        "50-MemoryHigh.conf"
    ),
    "candidate_memory_max_dropin": Path(
        "/etc/systemd/system.control/jato-fullstack-backend@8001.service.d/"
        "50-MemoryMax.conf"
    ),
}
RESIDUE_QUARANTINE_NAMES = {
    "maintenance_marker": "legacy-maintenance-marker",
    "candidate_slot_link": "candidate-slot-link",
    "candidate_slot_env": "candidate-slot-env",
    "candidate_explicit_unit": "candidate-explicit-unit",
    "candidate_sandbox_dropin": "candidate-sandbox-dropin",
    "candidate_cpu_quota_dropin": "candidate-cpu-quota-dropin",
    "candidate_memory_high_dropin": "candidate-memory-high-dropin",
    "candidate_memory_max_dropin": "candidate-memory-max-dropin",
}
PRE_SWITCH_RECOVERY_BINDING_PATTERN = re.compile(
    r"(?:^|[; ])recovery_path=(\S+) "
    r"recovery_sha256=([0-9a-f]{64})(?:$|[; ])"
)


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
    if phase == "candidate_prepare_aborted":
        if status != "completed" or retry_class != "automatic":
            raise CheckpointError(
                "candidate_prepare_aborted phase requires status=completed and "
                "retryClass=automatic"
            )
    elif phase == "candidate_ready":
        if status != "completed" or retry_class != "inspect_then_resume":
            raise CheckpointError(
                "candidate_ready phase requires status=completed and "
                "retryClass=inspect_then_resume"
            )
    elif phase == "candidate_discarded":
        if status != "completed" or retry_class != "automatic":
            raise CheckpointError(
                "candidate_discarded phase requires status=completed and "
                "retryClass=automatic"
            )
    elif phase == "active_update_started":
        if status != "in_progress" or retry_class != "rollback_required":
            raise CheckpointError(
                "active_update_started phase requires status=in_progress and "
                "retryClass=rollback_required"
            )
    elif phase == "active_update_verified":
        if status != "completed" or retry_class != "inspect_then_resume":
            raise CheckpointError(
                "active_update_verified phase requires status=completed and "
                "retryClass=inspect_then_resume"
            )
    elif phase == "active_updated":
        if status != "completed" or retry_class != "inspect_then_resume":
            raise CheckpointError(
                "active_updated phase requires status=completed and "
                "retryClass=inspect_then_resume"
            )
    elif phase == "candidate_released":
        if status != "completed" or retry_class != "automatic":
            raise CheckpointError(
                "candidate_released phase requires status=completed and "
                "retryClass=automatic"
            )
    elif phase == "rollback_completed":
        if status != "completed" or retry_class != "automatic":
            raise CheckpointError(
                "rollback_completed phase requires status=completed and "
                "retryClass=automatic"
            )
    elif phase == PRE_SWITCH_ABORT_PHASE:
        if status != "completed" or retry_class != "automatic":
            raise CheckpointError(
                "pre_switch_aborted phase requires status=completed and "
                "retryClass=automatic"
            )
    elif phase == "complete":
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


def _ensure_private_real_directory(
    path: Path,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> Path:
    """Create a private state directory without accepting symlink components."""

    path = _absolute_without_resolution(path)
    if path == Path("/"):
        raise CheckpointError("private state directory must not be the root")
    missing: list[Path] = []
    current = path
    while True:
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            missing.append(current)
            if current.parent == current:
                raise CheckpointError(
                    f"cannot find an existing parent for private state: {path}"
                )
            current = current.parent
            continue
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise CheckpointError(
                f"private state ancestor must be a real directory: {current}"
            )
        break
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            pass
        metadata = directory.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise CheckpointError(
                f"private state directory is unsafe: {directory}"
            )
        os.chmod(directory, 0o700)
        if owner_uid is not None and owner_gid is not None:
            os.chown(directory, owner_uid, owner_gid)
        _fsync_directory(directory.parent)
    if path.resolve(strict=True) != path:
        raise CheckpointError(
            f"private state directory traverses a symlink: {path}"
        )
    if owner_uid is not None and owner_gid is not None:
        os.chown(path, owner_uid, owner_gid)
        os.chmod(path, 0o700)
    return path


def ensure_private_state_directory(
    path: Path,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> Path:
    """Create a private state directory without following symlink components."""

    return _ensure_private_real_directory(
        path,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )


def _read_small_regular_file(path: Path, *, label: str) -> bytes:
    _reject_symlink(path, label)
    try:
        before = path.lstat()
    except FileNotFoundError as exc:
        raise CheckpointError(f"{label} does not exist: {path}") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_size > MAX_RELEASE_METADATA_BYTES
    ):
        raise CheckpointError(
            f"{label} must be a regular file no larger than "
            f"{MAX_RELEASE_METADATA_BYTES} bytes: {path}"
        )
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise CheckpointError(f"{label} changed while being opened: {path}")
        chunks: list[bytes] = []
        remaining = MAX_RELEASE_METADATA_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
    finally:
        os.close(descriptor)
    if len(raw) > MAX_RELEASE_METADATA_BYTES:
        raise CheckpointError(f"{label} is oversized: {path}")
    return raw


def _release_metadata_commit(raw: bytes, *, label: str) -> str:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"{label} is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise CheckpointError(f"{label} must contain one JSON object")
    commits = [
        _require_hash(
            payload[field],
            GIT_SHA_PATTERN,
            f"{label} {field}",
            40,
        )
        for field in ("actualCommitSha", "commitSha")
        if field in payload and payload[field] not in (None, "")
    ]
    if not commits or len(set(commits)) != 1:
        raise CheckpointError(
            f"{label} must contain one unambiguous full release commit"
        )
    return commits[0]


def preserve_previous_release_metadata(
    *,
    state_root: Path,
    source: Path,
    candidate_commit: str,
    archive_sha256: str,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> dict[str, object]:
    """Durably bind the exact previous release metadata to one candidate.

    The sidecar intentionally lives outside ``checkpoints/`` so it cannot be
    misinterpreted as another release checkpoint during the next deployment.
    Exact retries may reuse it only when the source bytes are unchanged.
    """

    candidate_commit = _require_hash(
        candidate_commit,
        GIT_SHA_PATTERN,
        "candidate commit",
        40,
    )
    archive_sha256 = _require_hash(
        archive_sha256,
        SHA256_PATTERN,
        "archive SHA256",
        64,
    )
    if (owner_uid is None) != (owner_gid is None):
        raise CheckpointError("metadata owner UID and GID must be provided together")
    for value, label in ((owner_uid, "owner UID"), (owner_gid, "owner GID")):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
        ):
            raise CheckpointError(f"{label} must be a non-negative integer")
    source = _absolute_without_resolution(source)
    raw = _read_small_regular_file(source, label="previous release metadata")
    previous_commit = _release_metadata_commit(
        raw,
        label="previous release metadata",
    )
    if previous_commit == candidate_commit:
        raise CheckpointError(
            "previous release metadata unexpectedly identifies the candidate"
        )

    state_root = _ensure_private_real_directory(state_root)
    metadata_root = _ensure_private_real_directory(
        state_root / "previous-metadata",
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    candidate_root = _ensure_private_real_directory(
        metadata_root / candidate_commit,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    target = candidate_root / f"{archive_sha256}.json"
    _reject_symlink(target, "previous release metadata sidecar")
    if target.exists():
        existing = _read_small_regular_file(
            target,
            label="previous release metadata sidecar",
        )
        existing_commit = _release_metadata_commit(
            existing,
            label="previous release metadata sidecar",
        )
        if existing != raw or existing_commit != previous_commit:
            raise CheckpointError(
                "previous release metadata changed during an exact release retry"
            )
        if owner_uid is not None and owner_gid is not None:
            os.chown(target, owner_uid, owner_gid)
            os.chmod(target, 0o600)
        return {
            "path": str(target),
            "previousCommit": previous_commit,
            "reused": True,
        }

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=candidate_root,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        if owner_uid is not None and owner_gid is not None:
            os.fchown(descriptor, owner_uid, owner_gid)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        if target.exists() or target.is_symlink():
            raise CheckpointError(
                f"previous release metadata sidecar appeared concurrently: {target}"
            )
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        _fsync_directory(candidate_root)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return {
        "path": str(target),
        "previousCommit": previous_commit,
        "reused": False,
    }


def atomic_write_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> None:
    """Atomically replace *path* with a private, durable JSON document."""

    if (owner_uid is None) != (owner_gid is None):
        raise CheckpointError("owner UID and GID must be provided together")
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink(path, "checkpoint")
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        if owner_uid is not None and owner_gid is not None:
            os.fchown(file_descriptor, owner_uid, owner_gid)
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


def _load_checkpoint_journal(
    path: Path,
    *,
    identity: ReleaseIdentity,
) -> list[dict[str, Any]]:
    """Load one canonical journal without accepting partial or duplicate events."""

    if not path.exists() and not path.is_symlink():
        return []
    _reject_symlink(path, "journal")
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_size <= 0
        or before.st_size > MAX_CHECKPOINT_JOURNAL_BYTES
    ):
        raise CheckpointError("checkpoint journal has an invalid file type or size")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise CheckpointError("checkpoint journal changed while being opened")
        chunks: list[bytes] = []
        remaining = MAX_CHECKPOINT_JOURNAL_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    if len(raw) > MAX_CHECKPOINT_JOURNAL_BYTES:
        raise CheckpointError("checkpoint journal exceeds its size limit")
    if (
        (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        or len(raw) != before.st_size
    ):
        raise CheckpointError("checkpoint journal changed while being read")
    if not raw.endswith(b"\n"):
        raise CheckpointError("checkpoint journal ends in a partial event")
    try:
        raw_events = [json.loads(line) for line in raw.splitlines()]
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError("checkpoint journal is not valid JSONL") from exc
    events: list[dict[str, Any]] = []
    for expected_sequence, raw_event in enumerate(raw_events, start=1):
        if not isinstance(raw_event, dict):
            raise CheckpointError("checkpoint journal event must be an object")
        event = dict(raw_event)
        if event.pop("event", None) != "checkpoint_transition":
            raise CheckpointError("checkpoint journal event kind is invalid")
        validated = validate_checkpoint(event)
        if validated["sequence"] != expected_sequence:
            raise CheckpointError("checkpoint journal sequence is not contiguous")
        _assert_same_identity(validated, identity)
        events.append(validated)
    return events


def _pending_transition_matches(
    pending: Mapping[str, Any],
    *,
    identity: ReleaseIdentity,
    phase: str,
    status: str,
    retry_class: str,
    message: str | None,
) -> bool:
    expected_message = _require_message(message) if message is not None else None
    return (
        pending.get("identity") == identity.to_dict()
        and pending.get("phase") == phase
        and pending.get("status") == status
        and pending.get("retryClass") == retry_class
        and pending.get("message") == expected_message
    )


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
    if phase == PRE_SWITCH_ABORT_PHASE:
        raise CheckpointError(
            "pre_switch_aborted may only be written by the audited recovery helper"
        )
    _validate_terminal_contract(
        phase=phase,
        status=status,
        retry_class=retry_class,
    )

    existing: dict[str, Any] | None = None
    pending: dict[str, Any] | None = None
    sequence = 1
    if checkpoint_path.exists() or checkpoint_path.is_symlink():
        existing = load_checkpoint(checkpoint_path)
        _assert_same_identity(existing, identity)
    journal_events = _load_checkpoint_journal(journal_path, identity=identity)
    if existing is None:
        if len(journal_events) > 1:
            raise CheckpointError(
                "checkpoint journal is more than one transition ahead of an absent checkpoint"
            )
        if journal_events:
            pending = journal_events[0]
    else:
        existing_sequence = existing["sequence"]
        if len(journal_events) not in {existing_sequence, existing_sequence + 1}:
            raise CheckpointError(
                "checkpoint journal is not aligned with the authoritative checkpoint"
            )
        journal_checkpoint = journal_events[existing_sequence - 1]
        if journal_checkpoint != existing:
            raise CheckpointError("checkpoint journal tail differs from checkpoint")
        if len(journal_events) == existing_sequence + 1:
            pending = journal_events[-1]

    if existing is not None:
        existing_phase = existing["phase"]
        existing_status = existing["status"]
        rollback_candidate_cleanup = (
            existing_phase == "rollback_completed"
            and phase == "candidate_discarded"
        )
        if existing_phase in {
            "candidate_prepare_aborted",
            "candidate_discarded",
            "candidate_released",
            "rollback_completed",
            PRE_SWITCH_ABORT_PHASE,
            "complete",
        } and not rollback_candidate_cleanup:
            raise CheckpointError(
                f"{existing_phase} checkpoint is immutable",
            )
        if phase == existing_phase and status == existing_status:
            if retry_class != existing["retryClass"]:
                raise CheckpointError(
                    "an idempotent checkpoint write cannot change retryClass"
                )
            if pending is not None:
                raise CheckpointError(
                    "checkpoint journal has a pending transition that must be reconciled"
                )
            return existing
        if phase == existing_phase:
            if status not in ALLOWED_SAME_PHASE_STATUS_TRANSITIONS[existing_status]:
                raise CheckpointError(
                    "same-phase status regression is forbidden: "
                    f"{existing_phase}/{existing_status} -> {phase}/{status}"
                )
        elif phase not in ALLOWED_PHASE_TRANSITIONS[existing_phase]:
            if PHASE_INDEX[phase] < PHASE_INDEX[existing_phase]:
                raise CheckpointError(
                    f"phase regression is forbidden: {existing_phase} -> {phase}"
                )
            raise CheckpointError(
                "illegal checkpoint phase transition: "
                f"{existing_phase}/{existing_status} -> {phase}/{status}"
            )
        else:
            allowed_source_statuses = REQUIRED_PREDECESSOR_STATUSES.get(
                (existing_phase, phase)
            )
            if (
                allowed_source_statuses is not None
                and existing_status not in allowed_source_statuses
            ):
                raise CheckpointError(
                    "checkpoint predecessor status is invalid: "
                    f"{existing_phase}/{existing_status} -> {phase}/{status}"
                )
        sequence = existing["sequence"] + 1

    if pending is not None:
        if pending["sequence"] != sequence or not _pending_transition_matches(
            pending,
            identity=identity,
            phase=phase,
            status=status,
            retry_class=retry_class,
            message=message,
        ):
            raise CheckpointError(
                "checkpoint journal pending transition differs from the requested write"
            )
        atomic_write_json(checkpoint_path, pending)
        return pending

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
    # the next exact write adopts the single validated journal-ahead event into
    # the authoritative checkpoint instead of appending a duplicate sequence.
    append_journal(journal_path, checkpoint)
    atomic_write_json(checkpoint_path, checkpoint)
    return checkpoint


def _require_recovery_receipt(
    *,
    receipt_path: Path,
    receipt_sha256: str,
    receipt_root: Path,
    identity: ReleaseIdentity,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> tuple[Path, str, Mapping[str, Any]]:
    """Validate the immutable receipt bound to a pre-switch abort."""

    receipt_sha256 = _require_hash(
        receipt_sha256,
        SHA256_PATTERN,
        "recovery receipt SHA256",
        64,
    )
    receipt_path = _absolute_without_resolution(receipt_path)
    receipt_root = _absolute_without_resolution(receipt_root)
    _require_real_directory(receipt_root, "recovery receipt root")
    _reject_symlink(receipt_path, "recovery receipt")
    try:
        resolved_receipt = receipt_path.resolve(strict=True)
        resolved_root = receipt_root.resolve(strict=True)
    except OSError as exc:
        raise CheckpointError("recovery receipt cannot be resolved safely") from exc
    try:
        relative = resolved_receipt.relative_to(resolved_root)
    except ValueError as exc:
        raise CheckpointError(
            "recovery receipt must remain below the recovery receipt root"
        ) from exc
    if len(relative.parts) != 2 or not relative.name.endswith(".json"):
        raise CheckpointError(
            "recovery receipt must use <incident>/<plan-digest>.json layout"
        )
    metadata = receipt_path.stat()
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise CheckpointError("recovery receipt must be private")
    if (
        owner_uid is not None
        and owner_gid is not None
        and (metadata.st_uid, metadata.st_gid) != (owner_uid, owner_gid)
    ):
        raise CheckpointError("recovery receipt owner differs from checkpoint owner")
    raw = _read_small_regular_file(receipt_path, label="recovery receipt")
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    if actual_sha256 != receipt_sha256:
        raise CheckpointError("recovery receipt SHA256 mismatch")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError("recovery receipt must contain valid JSON") from exc
    implementation = payload.get("implementation") if isinstance(payload, dict) else None
    incident = payload.get("incidentId") if isinstance(payload, dict) else None
    receipt_schema = payload.get("schemaVersion") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or isinstance(receipt_schema, bool)
        or not isinstance(receipt_schema, int)
        or receipt_schema not in RECOVERY_RECEIPT_SCHEMA_VERSIONS
        or payload.get("kind") != "pre_switch_abort"
        or payload.get("decision") != "pre_switch_abort_verified"
        or payload.get("identity") != identity.to_dict()
        or not isinstance(incident, str)
        or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,127}", incident)
        or relative.parts[0] != incident
        or not isinstance(implementation, dict)
        or set(implementation) != {"commit", "planSha256"}
        or not GIT_SHA_PATTERN.fullmatch(str(implementation.get("commit") or ""))
        or not SHA256_PATTERN.fullmatch(
            str(implementation.get("planSha256") or "")
        )
        or relative.stem != implementation.get("planSha256")
    ):
        raise CheckpointError("recovery receipt decision contract is invalid")
    if receipt_schema == 3:
        _validate_schema_v3_receipt_contract(
            payload,
            receipt_root=receipt_root,
        )
    return receipt_path, receipt_sha256, payload


def _recovery_migration_status(receipt: Mapping[str, Any]) -> str:
    schema_version = receipt.get("schemaVersion")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version not in RECOVERY_RECEIPT_SCHEMA_VERSIONS
    ):
        raise CheckpointError("recovery receipt schemaVersion is invalid")
    return RECOVERY_MIGRATION_STATUS_BY_SCHEMA[schema_version]


def _validate_schema_v3_authorization_proof(
    authorization: object,
    *,
    incident_id: object,
    identity: object,
    implementation: object,
    inventory_digest: object,
    label: str,
) -> None:
    if (
        not isinstance(authorization, dict)
        or set(authorization) != SCHEMA_V3_AUTHORIZATION_FIELDS
        or not isinstance(identity, dict)
        or not isinstance(implementation, dict)
        or set(implementation) != {"commit", "planSha256"}
        or not GIT_SHA_PATTERN.fullmatch(
            str(implementation.get("commit") or "")
        )
        or not SHA256_PATTERN.fullmatch(
            str(implementation.get("planSha256") or "")
        )
        or authorization.get("schemaVersion") != 1
        or authorization.get("kind")
        != "checkpoint_recovery_dry_run_authorization"
        or authorization.get("workflowPath")
        != ".github/workflows/production-checkpoint-recovery.yml"
        or authorization.get("decision")
        != "candidate-residue-dry-run-eligible"
        or authorization.get("incidentId") != incident_id
        or authorization.get("repository") != identity.get("repository")
        or authorization.get("mainSha") != implementation.get("commit")
        or authorization.get("planSha256") != implementation.get("planSha256")
        or authorization.get("inventoryDigest") != inventory_digest
        or any(
            isinstance(authorization.get(field), bool)
            or not isinstance(authorization.get(field), int)
            or authorization.get(field, 0) <= 0
            for field in ("runId", "runAttempt")
        )
        or any(
            not SHA256_PATTERN.fullmatch(str(authorization.get(field) or ""))
            for field in (
                "resultSha256",
                "planSha256",
                "inventoryDigest",
                "authorizationSha256",
            )
        )
    ):
        raise CheckpointError(f"{label} proof is invalid")
    authorization_document = dict(authorization)
    authorization_sha256 = authorization_document.pop("authorizationSha256")
    authorization_raw = (
        json.dumps(
            authorization_document,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
        + b"\n"
    )
    if hashlib.sha256(authorization_raw).hexdigest() != authorization_sha256:
        raise CheckpointError(f"{label} digest is invalid")


def _validate_schema_v3_receipt_contract(
    receipt: Mapping[str, Any],
    *,
    receipt_root: Path,
) -> None:
    if set(receipt) != {
        "schemaVersion",
        "kind",
        "decision",
        "incidentId",
        "identity",
        "implementation",
        "sourceCheckpoint",
        "legacyEvidence",
        "journal",
        "backup",
        "database",
        "production",
        "candidate",
        "lock",
        "createdAt",
        "authorization",
        "residue",
        "finalizationReceipt",
    }:
        raise CheckpointError("schema v3 recovery receipt fields are invalid")
    authorization = receipt.get("authorization")
    residue = receipt.get("residue")
    finalization = receipt.get("finalizationReceipt")
    implementation = receipt.get("implementation")
    production = receipt.get("production")
    if not all(
        isinstance(value, dict)
        for value in (authorization, residue, finalization, implementation, production)
    ):
        raise CheckpointError("schema v3 recovery proof objects are invalid")
    if (
        receipt.get("incidentId") != RESIDUE_INCIDENT_ID
        or receipt.get("identity", {}).get("commit") != RESIDUE_TARGET_COMMIT
    ):
        raise CheckpointError("schema v3 dry-run authorization proof is invalid")
    _validate_schema_v3_authorization_proof(
        authorization,
        incident_id=receipt.get("incidentId"),
        identity=receipt.get("identity"),
        implementation=implementation,
        inventory_digest=residue.get("inventoryDigest"),
        label="schema v3 dry-run authorization",
    )
    if set(residue) != {
        "profile",
        "inventoryDigest",
        "quarantineRoot",
        "manifestPath",
        "manifestSha256",
        "items",
        "retainedEvidence",
        "requiredAbsentPaths",
        "recoveryFencePresentAtSeal",
    }:
        raise CheckpointError("schema v3 residue proof fields are invalid")
    quarantine_root = _absolute_without_resolution(
        Path(str(residue.get("quarantineRoot") or ""))
    )
    manifest_path = _absolute_without_resolution(
        Path(str(residue.get("manifestPath") or ""))
    )
    items = residue.get("items")
    retained_evidence = residue.get("retainedEvidence")
    required_absent = residue.get("requiredAbsentPaths")
    if (
        residue.get("profile") != "materialized_never_started"
        or residue.get("inventoryDigest") != authorization.get("inventoryDigest")
        or quarantine_root
        != RESIDUE_QUARANTINE_ROOT
        or manifest_path != quarantine_root / "quarantine-contract.json"
        or not SHA256_PATTERN.fullmatch(str(residue.get("manifestSha256") or ""))
        or residue.get("recoveryFencePresentAtSeal") is not True
        or not isinstance(items, list)
        or len(items) != 8
        or not isinstance(retained_evidence, list)
        or len(retained_evidence) != 2
        or not isinstance(required_absent, list)
        or len(required_absent) != len(RESIDUE_REQUIRED_ABSENT_PATHS)
        or {
            _absolute_without_resolution(Path(str(path)))
            for path in required_absent
        }
        != RESIDUE_REQUIRED_ABSENT_PATHS
    ):
        raise CheckpointError("schema v3 residue proof is invalid")
    item_ids: set[str] = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {
            "id",
            "sourcePath",
            "quarantinePath",
            "identity",
        }:
            raise CheckpointError("schema v3 residue item proof is invalid")
        item_id = item.get("id")
        identity = item.get("identity")
        source = _absolute_without_resolution(Path(str(item.get("sourcePath") or "")))
        destination = _absolute_without_resolution(
            Path(str(item.get("quarantinePath") or ""))
        )
        expected_kind = "symlink" if item_id == "candidate_slot_link" else "file"
        if (
            not isinstance(item_id, str)
            or not item_id
            or item_id in item_ids
            or not isinstance(identity, dict)
            or RESIDUE_SOURCE_PATHS.get(item_id) != source
            or destination
            != quarantine_root / str(RESIDUE_QUARANTINE_NAMES.get(item_id) or "")
            or set(identity)
            != {
                "kind",
                "device",
                "inode",
                "uid",
                "gid",
                "mode",
                "nlink",
                "bytes",
                "mtimeNs",
                "sha256",
                "target",
                "targetSha256",
            }
            or identity.get("kind") != expected_kind
            or identity.get("device") != RESIDUE_DEVICE
            or identity.get("uid") != RESIDUE_OWNER_UID
            or identity.get("gid") != RESIDUE_OWNER_GID
            or identity.get("nlink") != 1
            or isinstance(identity.get("inode"), bool)
            or not isinstance(identity.get("inode"), int)
            or identity.get("inode", 0) <= 0
            or isinstance(identity.get("bytes"), bool)
            or not isinstance(identity.get("bytes"), int)
            or identity.get("bytes", 0) <= 0
            or isinstance(identity.get("mtimeNs"), bool)
            or not isinstance(identity.get("mtimeNs"), int)
            or identity.get("mtimeNs", 0) <= 0
            or not re.fullmatch(r"0[0-7]{3}", str(identity.get("mode") or ""))
        ):
            raise CheckpointError("schema v3 residue item identity is invalid")
        if expected_kind == "file":
            if (
                not SHA256_PATTERN.fullmatch(str(identity.get("sha256") or ""))
                or identity.get("target") is not None
                or identity.get("targetSha256") is not None
            ):
                raise CheckpointError("schema v3 regular residue identity is invalid")
        elif (
            identity.get("sha256") is not None
            or identity.get("target")
            != str(
                Path("/opt/jato/releases")
                / RESIDUE_TARGET_COMMIT
                / str(receipt["identity"]["archiveSha256"])
            )
            or not SHA256_PATTERN.fullmatch(
                str(identity.get("targetSha256") or "")
            )
        ):
            raise CheckpointError("schema v3 Candidate slot identity is invalid")
        item_ids.add(item_id)
    if item_ids != set(RESIDUE_SOURCE_PATHS):
        raise CheckpointError("schema v3 residue item set is invalid")
    retained_by_id: dict[str, Mapping[str, Any]] = {}
    for retained in retained_evidence:
        if (
            not isinstance(retained, dict)
            or set(retained) != {"id", "path", "identity"}
            or not isinstance(retained.get("id"), str)
            or retained["id"] in retained_by_id
        ):
            raise CheckpointError("schema v3 retained evidence proof is invalid")
        retained_by_id[retained["id"]] = retained
    expected_retained_path = (
        RESIDUE_PREVIOUS_METADATA_ROOT
        / RESIDUE_TARGET_COMMIT
        / f"{receipt['identity']['archiveSha256']}.json"
    )
    retained = retained_by_id.get("previous_metadata")
    retained_identity = (
        retained.get("identity") if isinstance(retained, dict) else None
    )
    if (
        set(retained_by_id) != {"previous_metadata", "canonical_nginx_config"}
        or not isinstance(retained, dict)
        or retained.get("id") != "previous_metadata"
        or _absolute_without_resolution(Path(str(retained.get("path") or "")))
        != expected_retained_path
        or not isinstance(retained_identity, dict)
        or set(retained_identity)
        != {
            "kind",
            "device",
            "inode",
            "uid",
            "gid",
            "mode",
            "nlink",
            "bytes",
            "mtimeNs",
            "sha256",
            "target",
            "targetSha256",
        }
        or retained_identity.get("kind") != "file"
        or retained_identity.get("device") != RESIDUE_DEVICE
        or retained_identity.get("uid") != RESIDUE_RETAINED_OWNER_UID
        or retained_identity.get("gid") != RESIDUE_RETAINED_OWNER_GID
        or retained_identity.get("mode") != "0600"
        or retained_identity.get("nlink") != 1
        or any(
            isinstance(retained_identity.get(field), bool)
            or not isinstance(retained_identity.get(field), int)
            or retained_identity.get(field, 0) <= 0
            for field in ("inode", "bytes", "mtimeNs")
        )
        or not SHA256_PATTERN.fullmatch(
            str(retained_identity.get("sha256") or "")
        )
        or retained_identity.get("target") is not None
        or retained_identity.get("targetSha256") is not None
    ):
        raise CheckpointError("schema v3 retained evidence proof is invalid")
    canonical = retained_by_id["canonical_nginx_config"]
    if (
        _absolute_without_resolution(Path(str(canonical.get("path") or "")))
        != RESIDUE_CANONICAL_NGINX
        or canonical.get("identity") != RESIDUE_CANONICAL_NGINX_IDENTITY
    ):
        raise CheckpointError("schema v3 canonical Nginx proof is invalid")
    if set(finalization) != {"path", "sha256"}:
        raise CheckpointError("schema v3 finalization binding is invalid")
    finalization_path = _absolute_without_resolution(
        Path(str(finalization.get("path") or ""))
    )
    expected_plan_sha256 = str(
        receipt.get("implementation", {}).get("planSha256") or ""
    )
    expected_finalization_path = (
        _absolute_without_resolution(receipt_root)
        / RESIDUE_INCIDENT_ID
        / f"{expected_plan_sha256}.finalization.json"
    )
    if (
        finalization_path != expected_finalization_path
        or not SHA256_PATTERN.fullmatch(str(finalization.get("sha256") or ""))
    ):
        raise CheckpointError("schema v3 finalization receipt path is invalid")
    runtime = production.get("runtime")
    if (
        not isinstance(runtime, dict)
        or runtime.get("maintenanceMarkerPresent") is not True
        or runtime.get("nginxCanonicalConfig") != str(RESIDUE_CANONICAL_NGINX)
        or runtime.get("nginxCanonicalConfigIdentity")
        != RESIDUE_CANONICAL_NGINX_IDENTITY
    ):
        raise CheckpointError("schema v3 operation receipt did not retain its fence")


def _recovery_revision_set(raw: object) -> list[str]:
    if not isinstance(raw, str) or not raw:
        raise CheckpointError("recovery evidence revision output is invalid")
    revisions = sorted(set(REVISION_PATTERN.findall(raw)))
    if not revisions:
        raise CheckpointError("recovery evidence revision output is empty")
    return revisions


def _validate_recovery_legacy_evidence(
    *,
    receipt: Mapping[str, Any],
    evidence_bindings: Sequence[tuple[str, str]],
) -> None:
    legacy = receipt.get("legacyEvidence")
    database = receipt.get("database")
    if not isinstance(legacy, dict) or not isinstance(database, dict):
        raise CheckpointError("recovery receipt legacy evidence proof is invalid")
    expected_status = _recovery_migration_status(receipt)
    if (
        len(evidence_bindings) != 1
        or legacy.get("path") != evidence_bindings[0][0]
        or legacy.get("sha256") != evidence_bindings[0][1]
        or legacy.get("migrationStatus") != expected_status
    ):
        raise CheckpointError("recovery receipt legacy evidence proof is invalid")

    evidence_path = Path(evidence_bindings[0][0])
    evidence_raw = _read_small_regular_file(
        evidence_path,
        label="pre-switch abort legacy evidence",
    )
    if hashlib.sha256(evidence_raw).hexdigest() != evidence_bindings[0][1]:
        raise CheckpointError("pre-switch abort legacy evidence changed")
    try:
        evidence = json.loads(evidence_raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(
            "pre-switch abort legacy evidence is not valid JSON"
        ) from exc
    if not isinstance(evidence, dict) or set(evidence) != {
        "identity",
        "backup",
        "migration",
    }:
        raise CheckpointError("recovery evidence object is invalid")
    evidence_backup = evidence.get("backup")
    receipt_backup = receipt.get("backup")
    if (
        evidence.get("identity") != receipt.get("identity")
        or not isinstance(evidence_backup, dict)
        or not isinstance(receipt_backup, dict)
        or evidence_backup.get("manifestPath")
        != receipt_backup.get("manifestPath")
        or evidence_backup.get("manifestSha256")
        != receipt_backup.get("manifestSha256")
        or isinstance(evidence_backup.get("manifestBytes"), bool)
        or not isinstance(evidence_backup.get("manifestBytes"), int)
        or evidence_backup.get("manifestBytes", 0) <= 0
    ):
        raise CheckpointError("recovery evidence backup/identity proof is invalid")
    migration = evidence.get("migration")
    if not isinstance(migration, dict) or set(migration) != {
        "status",
        "preRevision",
        "targetRevision",
        "resultRevision",
    }:
        raise CheckpointError("recovery evidence migration proof is invalid")
    if receipt["schemaVersion"] == 1:
        if migration != {
            "status": "not_required",
            "preRevision": None,
            "targetRevision": None,
            "resultRevision": None,
        }:
            raise CheckpointError(
                "schema v1 recovery evidence migration proof is invalid"
            )
        return

    expected_revisions = database.get("currentRevisions")
    if (
        migration.get("status") != "completed"
        or not isinstance(expected_revisions, list)
        or not expected_revisions
        or any(
            _recovery_revision_set(migration.get(field)) != expected_revisions
            for field in ("preRevision", "targetRevision", "resultRevision")
        )
    ):
        if receipt["schemaVersion"] == 2:
            raise CheckpointError(
                "schema v2 recovery evidence revisions differ from receipt"
            )
        raise CheckpointError(
            "schema v3 recovery evidence revisions differ from receipt"
        )


def _validate_recovery_receipt_safety_facts(
    receipt: Mapping[str, Any],
) -> None:
    database = receipt.get("database")
    candidate = receipt.get("candidate")
    backup = receipt.get("backup")
    production = receipt.get("production")
    if not all(
        isinstance(value, dict)
        for value in (database, candidate, backup, production)
    ):
        raise CheckpointError("recovery receipt safety proof objects are invalid")
    revision_fields = (
        "currentRevisions",
        "oldHeadRevisions",
        "newHeadRevisions",
        "backupRevisions",
    )
    revision_sets = [database.get(field) for field in revision_fields]
    if (
        database.get("enabled") is not True
        or database.get("mode") != "read_only"
        or database.get("transactionReadOnly") != "on"
        or database.get("equal") is not True
        or any(
            not isinstance(revisions, list) or not revisions
            for revisions in revision_sets
        )
        or any(revisions != revision_sets[0] for revisions in revision_sets[1:])
    ):
        raise CheckpointError("recovery receipt database proof is invalid")
    if any(
        candidate.get(field) is not False
        for field in (
            "unitActive",
            "unitEnabled",
            "listener",
            "slotLinkExists",
            "targetReleaseActive",
            "nginxReferencesTarget",
        )
    ):
        raise CheckpointError("recovery receipt Candidate proof is invalid")
    if (
        backup.get("databaseEnabled") is not True
        or backup.get("status") != "completed"
        or not SHA256_PATTERN.fullmatch(
            str(backup.get("manifestSha256") or "")
        )
        or not SHA256_PATTERN.fullmatch(str(backup.get("dumpSha256") or ""))
    ):
        raise CheckpointError("recovery receipt backup proof is invalid")


def _path_lexists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _require_path_absent(
    path: Path,
    *,
    label: str,
    allow_permission_denied: bool = False,
) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    except PermissionError as exc:
        if allow_permission_denied:
            return
        raise CheckpointError(f"{label} cannot be inspected: {path}") from exc
    except OSError as exc:
        raise CheckpointError(f"{label} cannot be inspected: {path}") from exc
    raise CheckpointError(f"{label} must remain absent: {path}")


def _live_identity(path: Path, *, label: str) -> Mapping[str, Any]:
    try:
        before = path.lstat()
    except OSError as exc:
        raise CheckpointError(f"{label} cannot be inspected: {path}") from exc
    if stat.S_ISLNK(before.st_mode):
        target = os.readlink(path)
        after = path.lstat()
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise CheckpointError(f"{label} changed while reading: {path}")
        return {
            "kind": "symlink",
            "device": after.st_dev,
            "inode": after.st_ino,
            "uid": after.st_uid,
            "gid": after.st_gid,
            "mode": f"0{stat.S_IMODE(after.st_mode):03o}",
            "nlink": after.st_nlink,
            "bytes": after.st_size,
            "mtimeNs": after.st_mtime_ns,
            "sha256": None,
            "target": target,
            "targetSha256": hashlib.sha256(target.encode("utf-8")).hexdigest(),
        }
    if not stat.S_ISREG(before.st_mode):
        raise CheckpointError(f"{label} has an unsupported type: {path}")
    raw = _read_small_regular_file(path, label=label)
    after = path.lstat()
    if (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) != (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ):
        raise CheckpointError(f"{label} changed while reading: {path}")
    return {
        "kind": "file",
        "device": after.st_dev,
        "inode": after.st_ino,
        "uid": after.st_uid,
        "gid": after.st_gid,
        "mode": f"0{stat.S_IMODE(after.st_mode):03o}",
        "nlink": after.st_nlink,
        "bytes": after.st_size,
        "mtimeNs": after.st_mtime_ns,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "target": None,
        "targetSha256": None,
    }


def _validate_schema_v3_finalization(
    *,
    operation_receipt: Mapping[str, Any],
    receipt_root: Path,
    owner_uid: int,
    owner_gid: int,
    enforce_incident_runtime: bool,
) -> None:
    """Validate the immutable settlement chain and its lifecycle invariants.

    ``enforce_incident_runtime`` is true only while recovery apply seals the
    incident.  Those checks prove that mutable Candidate/runtime paths have
    reached the reviewed incident-time state.  A later release may legitimately
    reuse those paths, so cross-release validation passes false while continuing
    to verify the immutable receipts, the absent recovery marker, permanent
    audit evidence and (when running as root) the private quarantine inodes.
    """

    if not isinstance(enforce_incident_runtime, bool):
        raise CheckpointError("schema v3 recovery lifecycle mode is invalid")

    binding = operation_receipt["finalizationReceipt"]
    finalization_path = _absolute_without_resolution(Path(binding["path"]))
    _reject_symlink(finalization_path, "recovery finalization receipt")
    try:
        relative = finalization_path.resolve(strict=True).relative_to(
            receipt_root.resolve(strict=True)
        )
    except (OSError, ValueError) as exc:
        raise CheckpointError(
            "recovery finalization receipt escaped its receipt root"
        ) from exc
    if len(relative.parts) != 2:
        raise CheckpointError("recovery finalization receipt layout is invalid")
    metadata = finalization_path.stat()
    if (
        (metadata.st_uid, metadata.st_gid) != (owner_uid, owner_gid)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise CheckpointError("recovery finalization receipt owner/mode is invalid")
    raw = _read_small_regular_file(
        finalization_path,
        label="recovery finalization receipt",
    )
    if hashlib.sha256(raw).hexdigest() != binding["sha256"]:
        raise CheckpointError("recovery finalization receipt SHA256 mismatch")
    try:
        finalization = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError("recovery finalization receipt is invalid JSON") from exc
    if (
        not isinstance(finalization, dict)
        or set(finalization)
        != {
            "schemaVersion",
            "kind",
            "decision",
            "incidentId",
            "identity",
            "implementation",
            "quarantineManifest",
            "fence",
            "createdAt",
        }
        or finalization.get("schemaVersion") != 1
        or finalization.get("kind") != "pre_switch_abort_fence_finalization"
        or finalization.get("decision") != "fence_finalization_authorized"
        or finalization.get("incidentId") != operation_receipt.get("incidentId")
        or finalization.get("identity") != operation_receipt.get("identity")
        or finalization.get("implementation")
        != operation_receipt.get("implementation")
    ):
        raise CheckpointError("recovery finalization receipt contract is invalid")
    residue = operation_receipt["residue"]
    manifest = finalization.get("quarantineManifest")
    if manifest != {
        "path": residue["manifestPath"],
        "sha256": residue["manifestSha256"],
    }:
        raise CheckpointError("recovery finalization manifest binding is invalid")
    manifest_path = Path(manifest["path"])
    quarantine_root = Path(residue["quarantineRoot"])
    expected_fence_content = (
        f"release={RESIDUE_TARGET_COMMIT} status=recovery_in_progress "
        f"incident={RESIDUE_INCIDENT_ID}\n"
    )
    expected_fence = {
        "content": expected_fence_content,
        "sha256": hashlib.sha256(expected_fence_content.encode("utf-8")).hexdigest(),
        "livePath": str(RESIDUE_MAINTENANCE_MARKER),
        "finalPath": str(quarantine_root / "recovery-fence-final"),
    }
    fence = finalization.get("fence")
    if not isinstance(fence, dict) or set(fence) != {
        "livePath",
        "finalPath",
        "identity",
    }:
        raise CheckpointError("recovery finalization fence proof is invalid")
    live_path = _absolute_without_resolution(Path(str(fence.get("livePath") or "")))
    final_path = _absolute_without_resolution(Path(str(fence.get("finalPath") or "")))
    fence_identity = fence.get("identity")
    if (
        live_path != RESIDUE_MAINTENANCE_MARKER
        or final_path != quarantine_root / "recovery-fence-final"
        or _path_lexists(live_path)
        or not isinstance(fence_identity, dict)
        or set(fence_identity)
        != {
            "kind",
            "device",
            "inode",
            "uid",
            "gid",
            "mode",
            "nlink",
            "bytes",
            "mtimeNs",
            "sha256",
            "target",
            "targetSha256",
        }
        or fence_identity.get("kind") != "file"
        or fence_identity.get("device") != RESIDUE_DEVICE
        or (fence_identity.get("uid"), fence_identity.get("gid"))
        != (RESIDUE_OWNER_UID, RESIDUE_OWNER_GID)
        or fence_identity.get("mode") != "0644"
        or fence_identity.get("nlink") != 1
        or fence_identity.get("bytes") != len(expected_fence_content.encode("utf-8"))
        or any(
            isinstance(fence_identity.get(field), bool)
            or not isinstance(fence_identity.get(field), int)
            or fence_identity.get(field, 0) <= 0
            for field in ("inode", "mtimeNs")
        )
        or fence_identity.get("sha256") != expected_fence["sha256"]
        or fence_identity.get("target") is not None
        or fence_identity.get("targetSha256") is not None
    ):
        raise CheckpointError("recovery fence finalization is not settled")

    # Recovery apply runs this module as root and proves every inode inside the
    # private 0700 quarantine.  A later root audit repeats that permanent proof.
    # Normal releases run the cross-release gate as the unprivileged SSH account
    # and intentionally cannot traverse the directory; their fail-closed proof
    # is the checkpoint-bound pair of 0600 receipts, the absent recovery marker,
    # and permanent metadata below.  Successor-owned runtime paths are checked
    # by the successor checkpoint rather than frozen to the incident snapshot.
    # A root attacker able to alter both production and the private quarantine
    # remains outside this deployment-integrity threat model.
    if os.geteuid() == 0:
        try:
            quarantine_metadata = quarantine_root.lstat()
            manifest_metadata = manifest_path.lstat()
        except OSError as exc:
            raise CheckpointError(
                "recovery quarantine root/manifest cannot be inspected"
            ) from exc
        if (
            not stat.S_ISDIR(quarantine_metadata.st_mode)
            or stat.S_ISLNK(quarantine_metadata.st_mode)
            or quarantine_metadata.st_dev != RESIDUE_DEVICE
            or (quarantine_metadata.st_uid, quarantine_metadata.st_gid)
            != (RESIDUE_OWNER_UID, RESIDUE_OWNER_GID)
            or stat.S_IMODE(quarantine_metadata.st_mode) != 0o700
            or not stat.S_ISREG(manifest_metadata.st_mode)
            or stat.S_ISLNK(manifest_metadata.st_mode)
            or manifest_metadata.st_dev != RESIDUE_DEVICE
            or (manifest_metadata.st_uid, manifest_metadata.st_gid)
            != (RESIDUE_OWNER_UID, RESIDUE_OWNER_GID)
            or stat.S_IMODE(manifest_metadata.st_mode) != 0o600
            or manifest_metadata.st_nlink != 1
        ):
            raise CheckpointError(
                "recovery quarantine root/manifest identity is invalid"
            )
        manifest_raw = _read_small_regular_file(
            manifest_path,
            label="recovery quarantine manifest",
        )
        if hashlib.sha256(manifest_raw).hexdigest() != manifest["sha256"]:
            raise CheckpointError("recovery quarantine manifest changed")
        try:
            manifest_payload = json.loads(manifest_raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CheckpointError(
                "recovery quarantine manifest is invalid JSON"
            ) from exc
        expected_manifest_items = [
            {
                "id": item["id"],
                "path": item["sourcePath"],
                "quarantineName": Path(item["quarantinePath"]).name,
                **item["identity"],
            }
            for item in residue["items"]
        ]
        expected_retained_evidence = [
            {
                "id": item["id"],
                "path": item["path"],
                **item["identity"],
            }
            for item in residue["retainedEvidence"]
        ]
        expected_manifest_stable = {
            "schemaVersion": 1,
            "kind": "candidate_residue_quarantine_contract",
            "incidentId": operation_receipt["incidentId"],
            "identity": operation_receipt["identity"],
            "quarantineRoot": str(quarantine_root),
            "items": expected_manifest_items,
            "retainedEvidence": expected_retained_evidence,
            "requiredAbsentPaths": residue["requiredAbsentPaths"],
            "fence": expected_fence,
        }
        if not isinstance(manifest_payload, dict) or set(manifest_payload) != {
            *expected_manifest_stable,
            "implementation",
            "authorization",
        }:
            raise CheckpointError("recovery quarantine manifest contract changed")
        manifest_stable = {
            key: value
            for key, value in manifest_payload.items()
            if key not in {"implementation", "authorization"}
        }
        manifest_implementation = manifest_payload["implementation"]
        operation_implementation = operation_receipt["implementation"]
        if (
            manifest_stable != expected_manifest_stable
            or not isinstance(manifest_implementation, dict)
            or not isinstance(operation_implementation, dict)
            or manifest_implementation.get("planSha256")
            != operation_implementation.get("planSha256")
        ):
            raise CheckpointError("recovery quarantine manifest contract changed")
        _validate_schema_v3_authorization_proof(
            manifest_payload["authorization"],
            incident_id=manifest_payload["incidentId"],
            identity=manifest_payload["identity"],
            implementation=manifest_implementation,
            inventory_digest=residue.get("inventoryDigest"),
            label="recovery quarantine manifest authorization",
        )
        if not _path_lexists(final_path):
            raise CheckpointError("final recovery fence is absent")
        final_fence_identity = _live_identity(
            final_path,
            label="final recovery fence",
        )
        if final_fence_identity != fence_identity:
            raise CheckpointError("recovery fence identity is invalid")
        for item in residue["items"]:
            destination = Path(item["quarantinePath"])
            if (
                not _path_lexists(destination)
                or _live_identity(
                    destination,
                    label="quarantined Candidate residue",
                )
                != item["identity"]
            ):
                raise CheckpointError("Candidate residue quarantine is not settled")

    if enforce_incident_runtime:
        for item in residue["items"]:
            source = Path(item["sourcePath"])
            _require_path_absent(
                source,
                label="Candidate residue quarantine is not settled",
            )
        for raw_path in residue["requiredAbsentPaths"]:
            required_absent = Path(raw_path)
            _require_path_absent(
                required_absent,
                label="required recovery-absence invariant",
                allow_permission_denied=(
                    os.geteuid() != 0
                    and required_absent in RESIDUE_ROOT_ATTESTED_ABSENT_PATHS
                ),
            )
    for item in residue["retainedEvidence"]:
        if (
            not enforce_incident_runtime
            and item.get("id") != "previous_metadata"
        ):
            continue
        path = Path(item["path"])
        if (
            not _path_lexists(path)
            or _live_identity(path, label="retained recovery evidence")
            != item["identity"]
        ):
            raise CheckpointError("retained recovery evidence changed")


def _validate_pre_switch_abort_source(
    *,
    checkpoint_path: Path,
    journal_path: Path,
    checkpoint: Mapping[str, Any],
    identity: ReleaseIdentity,
    receipt: Mapping[str, Any],
    binding: str,
) -> tuple[dict[str, Any], bool]:
    """Validate source checkpoint/journal and detect a journal-ahead retry."""

    source = receipt.get("sourceCheckpoint")
    journal_receipt = receipt.get("journal")
    legacy_evidence = receipt.get("legacyEvidence")
    if not all(
        isinstance(value, dict)
        for value in (
            source,
            journal_receipt,
            legacy_evidence,
        )
    ):
        raise CheckpointError("recovery receipt proof objects are invalid")
    _validate_recovery_receipt_safety_facts(receipt)

    checkpoint_raw = _read_small_regular_file(
        checkpoint_path,
        label="source checkpoint",
    )
    if (
        source.get("path") != str(checkpoint_path)
        or source.get("sha256") != hashlib.sha256(checkpoint_raw).hexdigest()
        or source.get("sequence") != checkpoint["sequence"]
        or source.get("phase") != "migrated"
        or source.get("status") != "completed"
        or source.get("retryClass") != "automatic"
    ):
        raise CheckpointError("recovery receipt source checkpoint proof is invalid")

    evidence_bindings = re.findall(
        r"(?:^|[; ])evidence_path=(\S+) "
        r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])",
        str(checkpoint.get("message") or ""),
    )
    _validate_recovery_legacy_evidence(
        receipt=receipt,
        evidence_bindings=evidence_bindings,
    )

    journal_raw = _read_small_regular_file(journal_path, label="checkpoint journal")
    lines = journal_raw.splitlines(keepends=True)
    try:
        events = [json.loads(line) for line in lines]
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError("checkpoint journal is not valid JSONL") from exc
    if not events:
        raise CheckpointError("checkpoint journal is empty")
    for index, event in enumerate(events, start=1):
        if (
            not isinstance(event, dict)
            or event.get("event") != "checkpoint_transition"
            or event.get("sequence") != index
            or event.get("identity") != identity.to_dict()
        ):
            raise CheckpointError("checkpoint journal sequence/identity is invalid")

    appended_checkpoint: dict[str, Any] | None = None
    journal_prefix = journal_raw
    if (
        events[-1].get("phase") == PRE_SWITCH_ABORT_PHASE
        and events[-1].get("sequence") == checkpoint["sequence"] + 1
    ):
        appended_checkpoint = dict(events[-1])
        appended_checkpoint.pop("event", None)
        journal_prefix = b"".join(lines[:-1])
        if (
            appended_checkpoint.get("status") != "completed"
            or appended_checkpoint.get("retryClass") != "automatic"
            or PRE_SWITCH_RECOVERY_BINDING_PATTERN.findall(
                str(appended_checkpoint.get("message") or "")
            )
            != PRE_SWITCH_RECOVERY_BINDING_PATTERN.findall(binding)
        ):
            raise CheckpointError(
                "journal-ahead pre-switch abort does not bind this receipt"
            )
        events = events[:-1]
    if len(events) != checkpoint["sequence"]:
        raise CheckpointError("checkpoint journal has unexpected trailing events")
    journal_tail = dict(events[-1])
    journal_tail.pop("event", None)
    if journal_tail != checkpoint:
        raise CheckpointError("checkpoint journal tail differs from checkpoint")
    if (
        journal_receipt.get("path") != str(journal_path)
        or journal_receipt.get("sha256")
        != hashlib.sha256(journal_prefix).hexdigest()
        or journal_receipt.get("lastSequence") != checkpoint["sequence"]
        or journal_receipt.get("switchPhaseSeen") is not False
    ):
        raise CheckpointError("recovery receipt journal proof is invalid")
    if appended_checkpoint is not None:
        return validate_checkpoint(appended_checkpoint), True
    return {}, False


def seal_pre_switch_abort(
    *,
    checkpoint_path: Path,
    journal_path: Path,
    identity: ReleaseIdentity,
    receipt_path: Path,
    receipt_sha256: str,
    receipt_root: Path,
    now: str | None = None,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> dict[str, Any]:
    """Seal one fully verified pre-switch release as aborted.

    The recovery helper owns all live-system and database proofs.  This state
    transition only accepts its immutable receipt and deliberately bypasses the
    normal transition graph; regular ``write`` calls can never enter this
    terminal phase.
    """

    if checkpoint_path.resolve() == journal_path.resolve():
        raise CheckpointError("checkpoint and journal paths must be different")
    if (owner_uid is None) != (owner_gid is None):
        raise CheckpointError("checkpoint owner UID and GID must be provided together")
    for value, label in ((owner_uid, "owner UID"), (owner_gid, "owner GID")):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
        ):
            raise CheckpointError(f"{label} must be a non-negative integer")

    receipt_path, receipt_sha256, receipt = _require_recovery_receipt(
        receipt_path=receipt_path,
        receipt_sha256=receipt_sha256,
        receipt_root=receipt_root,
        identity=identity,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    existing = load_checkpoint(checkpoint_path)
    _assert_same_identity(existing, identity)
    binding = f"recovery_path={receipt_path} recovery_sha256={receipt_sha256}"

    if existing["phase"] == PRE_SWITCH_ABORT_PHASE:
        bindings = PRE_SWITCH_RECOVERY_BINDING_PATTERN.findall(
            str(existing.get("message") or "")
        )
        if bindings != [(str(receipt_path), receipt_sha256)]:
            raise CheckpointError(
                "pre_switch_aborted checkpoint binds a different recovery receipt"
            )
        return existing
    if (
        existing["phase"] != "migrated"
        or existing["status"] != "completed"
        or existing["retryClass"] != "automatic"
    ):
        raise CheckpointError(
            "pre-switch abort requires migrated/completed/automatic checkpoint"
        )

    checkpoint: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "sequence": existing["sequence"] + 1,
        "identity": identity.to_dict(),
        "phase": PRE_SWITCH_ABORT_PHASE,
        "status": "completed",
        "retryClass": "automatic",
        "updatedAt": _require_timestamp(now) if now else _utc_now(),
        "message": _require_message(
            "release abandoned before Candidate start or traffic switch; "
            f"{binding}"
        ),
    }
    checkpoint = validate_checkpoint(checkpoint)
    journal_ahead, journal_already_appended = _validate_pre_switch_abort_source(
        checkpoint_path=checkpoint_path,
        journal_path=journal_path,
        checkpoint=existing,
        identity=identity,
        receipt=receipt,
        binding=binding,
    )
    if journal_already_appended:
        checkpoint = journal_ahead
    else:
        append_journal(journal_path, checkpoint)
    atomic_write_json(
        checkpoint_path,
        checkpoint,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    if owner_uid is not None and owner_gid is not None:
        os.chown(journal_path, owner_uid, owner_gid)
        os.chmod(journal_path, 0o600)
        _fsync_directory(checkpoint_path.parent)
        _fsync_directory(journal_path.parent)
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
    if phase == "rollback_completed":
        return {
            "decision": "candidate-cleanup-required",
            "action": "discard-candidate",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    if phase == "candidate_prepare_aborted":
        return {
            "decision": "already-candidate-prepare-aborted",
            "action": "none",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    if phase == "candidate_ready":
        return {
            "decision": "approval-required",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    if phase == "candidate_discarded":
        return {
            "decision": "already-candidate-discarded",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    if phase == "active_updated":
        return {
            "decision": "candidate-cleanup-required",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    if phase == "candidate_released":
        return {
            "decision": "already-candidate-released",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    if phase == PRE_SWITCH_ABORT_PHASE:
        return {
            "decision": "already-pre-switch-aborted",
            "phase": phase,
            "status": status,
            "retryClass": retry_class,
            "sequence": checkpoint["sequence"],
        }
    reconcile_actions = {
        "active_update_started": "restore-previous-active",
        "active_update_verified": "finalize-active-update",
        "switch_started": "reconcile-public-route",
        "switched": "reconcile-public-route",
        "rollback_started": "resume-rollback",
    }
    if phase in reconcile_actions:
        return {
            "decision": "reconcile-required",
            "action": reconcile_actions[phase],
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


def cross_release_is_settled(checkpoint: Mapping[str, Any]) -> bool:
    """Return whether an older release has crossed the safe health boundary."""

    phase = checkpoint["phase"]
    if phase == "complete":
        return True
    if phase == "rollback_completed":
        # A fixed-Active rollback deliberately retains the tested Candidate.
        # Only the following candidate_discarded transition proves cleanup.
        return False
    if phase == "candidate_prepare_aborted":
        return checkpoint["status"] == "completed"
    if phase == "candidate_discarded":
        return checkpoint["status"] == "completed"
    if phase == "candidate_released":
        return checkpoint["status"] == "completed"
    if phase == PRE_SWITCH_ABORT_PHASE:
        # This terminal is settled only after its receipt/journal binding is
        # validated by ``assert_cross_release_safe`` with namespace context.
        return False
    if phase == "rollback_started":
        return False
    return (
        PHASE_INDEX[phase] >= PHASE_INDEX["backend_healthy"]
        and checkpoint["status"] == "completed"
    )


def _validate_pre_switch_abort_settlement(
    *,
    checkpoint_path: Path,
    checkpoint: Mapping[str, Any],
    checkpoints_root: Path,
    enforce_incident_runtime: bool,
) -> None:
    identity = ReleaseIdentity.from_mapping(checkpoint["identity"])
    bindings = PRE_SWITCH_RECOVERY_BINDING_PATTERN.findall(
        str(checkpoint.get("message") or "")
    )
    if len(bindings) != 1:
        raise CheckpointError(
            "pre_switch_aborted checkpoint has no unique recovery receipt binding"
        )
    checkpoint_metadata = checkpoint_path.stat()
    receipt_root = checkpoints_root.parent / "recoveries"
    receipt_path, _, receipt = _require_recovery_receipt(
        receipt_path=Path(bindings[0][0]),
        receipt_sha256=bindings[0][1],
        receipt_root=receipt_root,
        identity=identity,
        owner_uid=checkpoint_metadata.st_uid,
        owner_gid=checkpoint_metadata.st_gid,
    )
    _validate_recovery_receipt_safety_facts(receipt)
    if receipt.get("schemaVersion") == 3:
        _validate_schema_v3_finalization(
            operation_receipt=receipt,
            receipt_root=receipt_root,
            owner_uid=checkpoint_metadata.st_uid,
            owner_gid=checkpoint_metadata.st_gid,
            enforce_incident_runtime=enforce_incident_runtime,
        )

    journal_path = (
        checkpoints_root.parent
        / "journals"
        / identity.commit
        / f"{identity.archiveSha256}.jsonl"
    )
    journal_raw = _read_small_regular_file(
        journal_path,
        label="pre-switch abort journal",
    )
    lines = journal_raw.splitlines(keepends=True)
    try:
        events = [json.loads(line) for line in lines]
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(
            "pre-switch abort journal is not valid JSONL"
        ) from exc
    if len(events) != checkpoint["sequence"] or len(events) < 2:
        raise CheckpointError("pre-switch abort journal length is invalid")
    validated: list[dict[str, Any]] = []
    for sequence, event in enumerate(events, start=1):
        if (
            not isinstance(event, dict)
            or event.get("event") != "checkpoint_transition"
            or event.get("sequence") != sequence
            or event.get("identity") != identity.to_dict()
        ):
            raise CheckpointError(
                "pre-switch abort journal sequence/identity is invalid"
            )
        payload = dict(event)
        payload.pop("event", None)
        validated.append(validate_checkpoint(payload))
    if validated[-1] != checkpoint:
        raise CheckpointError(
            "pre-switch abort journal tail differs from checkpoint"
        )
    source_checkpoint = validated[-2]
    source = receipt.get("sourceCheckpoint")
    journal = receipt.get("journal")
    legacy = receipt.get("legacyEvidence")
    if not all(
        isinstance(value, dict)
        for value in (source, journal, legacy)
    ):
        raise CheckpointError("pre-switch abort receipt chain is invalid")
    source_raw = (
        json.dumps(source_checkpoint, indent=2, sort_keys=True).encode("utf-8")
        + b"\n"
    )
    if (
        source_checkpoint.get("phase") != "migrated"
        or source_checkpoint.get("status") != "completed"
        or source_checkpoint.get("retryClass") != "automatic"
        or source.get("path") != str(checkpoint_path)
        or source.get("sha256") != hashlib.sha256(source_raw).hexdigest()
        or source.get("sequence") != source_checkpoint["sequence"]
        or source.get("phase") != source_checkpoint["phase"]
        or source.get("status") != source_checkpoint["status"]
        or source.get("retryClass") != source_checkpoint["retryClass"]
    ):
        raise CheckpointError("pre-switch abort source checkpoint proof is invalid")
    journal_prefix = b"".join(lines[:-1])
    if (
        journal.get("path") != str(journal_path)
        or journal.get("sha256") != hashlib.sha256(journal_prefix).hexdigest()
        or journal.get("lastSequence") != source_checkpoint["sequence"]
        or journal.get("switchPhaseSeen") is not False
    ):
        raise CheckpointError("pre-switch abort journal proof is invalid")

    evidence_bindings = re.findall(
        r"(?:^|[; ])evidence_path=(\S+) "
        r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])",
        str(source_checkpoint.get("message") or ""),
    )
    _validate_recovery_legacy_evidence(
        receipt=receipt,
        evidence_bindings=evidence_bindings,
    )
    if not receipt_path.is_file():  # pragma: no cover - validated above.
        raise CheckpointError("pre-switch abort recovery receipt disappeared")


def validate_pre_switch_abort_settlement(
    *,
    checkpoint_path: Path,
    checkpoints_root: Path,
) -> dict[str, Any]:
    """Validate one terminal abort against its private receipt and journal."""

    checkpoint = load_checkpoint(checkpoint_path)
    if checkpoint["phase"] != PRE_SWITCH_ABORT_PHASE:
        raise CheckpointError(
            "checkpoint is not a pre_switch_aborted terminal"
        )
    _validate_pre_switch_abort_settlement(
        checkpoint_path=_absolute_without_resolution(checkpoint_path),
        checkpoint=checkpoint,
        checkpoints_root=_absolute_without_resolution(checkpoints_root),
        enforce_incident_runtime=True,
    )
    return checkpoint


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
            if phase == PRE_SWITCH_ABORT_PHASE:
                _validate_pre_switch_abort_settlement(
                    checkpoint_path=entry,
                    checkpoint=payload,
                    checkpoints_root=checkpoints_root,
                    enforce_incident_runtime=False,
                )
                continue
            if retry_class in DANGEROUS_RETRY_CLASSES:
                raise CheckpointError(
                    "another release requires operator recovery before a new "
                    f"artifact can deploy: path={entry}, phase={phase}, "
                    f"status={status}, retryClass={retry_class}"
                )
            if (
                PHASE_INDEX[phase] >= PHASE_INDEX["source_install_started"]
                and not cross_release_is_settled(payload)
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

    previous_metadata_parser = subparsers.add_parser(
        "preserve-previous-metadata",
        help=(
            "copy the exact previous release metadata into a candidate-scoped "
            "state namespace outside checkpoints"
        ),
    )
    previous_metadata_parser.add_argument(
        "--state-root",
        required=True,
        type=Path,
    )
    previous_metadata_parser.add_argument(
        "--source",
        required=True,
        type=Path,
    )
    previous_metadata_parser.add_argument(
        "--candidate-commit",
        required=True,
    )
    previous_metadata_parser.add_argument(
        "--archive-sha256",
        required=True,
    )
    previous_metadata_parser.add_argument(
        "--owner-uid",
        type=int,
    )
    previous_metadata_parser.add_argument(
        "--owner-gid",
        type=int,
    )
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
        elif arguments.command == "preserve-previous-metadata":
            result = preserve_previous_release_metadata(
                state_root=arguments.state_root,
                source=arguments.source,
                candidate_commit=arguments.candidate_commit,
                archive_sha256=arguments.archive_sha256,
                owner_uid=arguments.owner_uid,
                owner_gid=arguments.owner_gid,
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
