#!/usr/bin/env python3
"""Audit and settle one reviewed production checkpoint before traffic switch.

This helper is intentionally incident-driven.  It accepts only a versioned
recovery plan from an immutable main-branch control bundle, proves that the
recorded release never started a Candidate or changed public traffic, verifies
the private backup chain, and compares read-only Alembic revisions.  The legacy
evidence remains untouched.  Apply mode writes a separate recovery receipt and
then seals the checkpoint as ``pre_switch_aborted``.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any, Mapping, Sequence

from release_checkpoint import (
    PRE_SWITCH_ABORT_PHASE,
    PRE_SWITCH_RECOVERY_BINDING_PATTERN,
    ReleaseIdentity,
    assert_cross_release_safe,
    atomic_write_json,
    ensure_private_state_directory,
    load_checkpoint,
    seal_pre_switch_abort,
    validate_checkpoint,
    validate_pre_switch_abort_settlement,
)


SUPPORTED_PLAN_SCHEMA_VERSIONS = frozenset({1, 2, 3})
RECEIPT_SCHEMA_VERSION_BY_PLAN = {1: 1, 2: 2, 3: 3}
MIGRATION_STATUS_BY_PLAN = {1: "not_required", 2: "completed", 3: "completed"}
MAX_JSON_BYTES = 1024 * 1024
TRUSTED_SYSTEM_UID = 0
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
REVISION_PATTERN = re.compile(r"(?m)^([0-9]{8}_[0-9]{4})\b")
SAFE_INCIDENT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{0,127}$")
SWITCH_OR_LATER_PHASES = frozenset(
    {
        "switch_started",
        "switched",
        "rollback_started",
        "rollback_completed",
        PRE_SWITCH_ABORT_PHASE,
        "backend_healthy",
        "www_verified",
        "intl_deploy_started",
        "intl_verified",
        "parity_verified",
        "complete",
    }
)

LEGACY_PLAN_FIELDS = {
    "schemaVersion",
    "incidentId",
    "repository",
    "checkpoint",
    "archive",
    "backup",
    "expected",
    "runtime",
    "sourceProofs",
}
RESIDUE_PLAN_FIELDS = LEGACY_PLAN_FIELDS | {"residue"}
CHECKPOINT_PLAN_FIELDS = {
    "path",
    "bytes",
    "sha256",
    "sequence",
    "journalPath",
    "journalBytes",
    "journalLines",
    "journalSha256",
    "evidencePath",
    "evidenceBytes",
    "evidenceSha256",
    "identity",
}
ARCHIVE_PLAN_FIELDS = {
    "path",
    "bytes",
    "sha256",
    "releaseRoot",
    "releaseIdentity",
}
BACKUP_PLAN_FIELDS = {
    "root",
    "manifestPath",
    "manifestBytes",
    "manifestSha256",
    "dumpPath",
    "dumpBytes",
    "dumpSha256",
}
EXPECTED_PLAN_FIELDS = {
    "activeCommit",
    "activeBackendActiveEnterTimestampMonotonic",
    "activeBackendControlGroup",
    "activeBackendExecMainStartTimestampMonotonic",
    "activeBackendInvocationId",
    "activeBackendNRestarts",
    "activeBuildMetaBytes",
    "activeBuildMetaSha256",
    "activeReleaseRoot",
    "activeFrontendRoot",
    "activeSlot",
    "activeSlotFileBytes",
    "activeSlotFileSha256",
    "candidateSlot",
    "workers",
    "memoryHighBytes",
    "memoryMaxBytes",
    "revisions",
}
RUNTIME_PLAN_FIELDS = {
    "activeSlotFile",
    "activeReleaseLink",
    "candidateSlotLink",
    "bluegreenStateRoot",
    "deploymentMarker",
    "schedulerState",
    "nginxConfig",
    "nginxConfigBytes",
    "nginxConfigSha256",
    "nginxMode",
    "backendEnv",
    "slotEnv",
    "venv",
    "backendServicePrefix",
    "monthlyWorkerUnit",
    "switchUnit",
    "serverNames",
}
RESIDUE_RUNTIME_PLAN_FIELDS = RUNTIME_PLAN_FIELDS | {"nginxCanonicalConfig"}
EVIDENCE_FIELDS = {"identity", "backup", "migration"}
EVIDENCE_BACKUP_FIELDS = {"manifestPath", "manifestBytes", "manifestSha256"}
EVIDENCE_MIGRATION_FIELDS = {
    "status",
    "preRevision",
    "targetRevision",
    "resultRevision",
}
RESIDUE_FIELDS = {
    "profile",
    "quarantineRoot",
    "quarantineDevice",
    "quarantineOwnerUid",
    "quarantineOwnerGid",
    "quarantineMode",
    "manifestName",
    "finalFenceName",
    "fenceContent",
    "candidateUnit",
    "items",
    "retainedEvidence",
    "requiredAbsentPaths",
}
RESIDUE_ITEM_FIELDS = {
    "id",
    "path",
    "quarantineName",
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
RETAINED_EVIDENCE_FIELDS = RESIDUE_ITEM_FIELDS - {"quarantineName"}
CANDIDATE_UNIT_FIELDS = {
    "name",
    "loadState",
    "activeState",
    "subState",
    "unitFileState",
    "mainPid",
    "result",
    "nRestarts",
    "execMainStartTimestampMonotonic",
    "activeEnterTimestampMonotonic",
    "inactiveEnterTimestampMonotonic",
    "invocationId",
    "fragmentPath",
    "dropInPaths",
    "memoryHighBytes",
    "memoryMaxBytes",
}
DRY_RUN_AUTHORIZATION_FIELDS = {
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
RENAME_NOREPLACE = 1
RENAME_EXCHANGE = 2
RECOVERY_FENCE_TEMP_SUFFIX = ".publish-tmp"
RESIDUE_INCIDENT_ID = "2026-08-03-29df-pre-switch-candidate-residue"
RESIDUE_TARGET_COMMIT = "29df5e6e667351f09305783932b34e5438d6a9d5"
RESIDUE_QUARANTINE_DEVICE = 64770
RESIDUE_QUARANTINE_ROOT = (
    Path("/var/lib/jato-release/recovery-quarantine") / RESIDUE_INCIDENT_ID
)
RESIDUE_PREVIOUS_METADATA_ROOT = Path("/var/lib/jato-release/previous-metadata")
RESIDUE_PATHS = {
    "maintenance_marker": Path("/var/lib/jato-release/deployment-maintenance"),
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
RESIDUE_REQUIRED_ABSENT_PATHS = {
    Path("/etc/jato-fullstack/nginx/active-release.conf"),
    Path("/var/lib/jato-release/scheduler-state.tsv"),
    Path("/var/cache/jato-candidate-8001"),
    Path("/var/cache/private/jato-candidate-8001"),
    Path("/run/systemd/system.control/jato-fullstack-backend@8001.service.d"),
    Path("/opt/jato/active"),
    Path(f"/var/lib/jato-release/nginx-preimage-{RESIDUE_TARGET_COMMIT}"),
    Path(
        f"/var/lib/jato-release/backend-template.pre-{RESIDUE_TARGET_COMMIT}.service"
    ),
    Path(
        f"/var/lib/jato-release/backend-template.pre-{RESIDUE_TARGET_COMMIT}.service.state"
    ),
}


class RecoveryError(ValueError):
    """A recovery precondition or evidence contract failed."""

    def __init__(self, category: str, detail: str) -> None:
        super().__init__(f"{category}: {detail}")
        self.category = category
        self.detail = detail


def _fail(category: str, detail: str) -> None:
    raise RecoveryError(category, detail)


def _plan_schema_version(plan: Mapping[str, Any]) -> int:
    version = plan.get("schemaVersion")
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version not in SUPPORTED_PLAN_SCHEMA_VERSIONS
    ):
        _fail("plan_invalid", "unsupported schemaVersion")
    return version


def _migration_status_for_plan(plan: Mapping[str, Any]) -> str:
    return MIGRATION_STATUS_BY_PLAN[_plan_schema_version(plan)]


def _receipt_schema_version_for_plan(plan: Mapping[str, Any]) -> int:
    return RECEIPT_SCHEMA_VERSION_BY_PLAN[_plan_schema_version(plan)]


def _require_mapping(
    value: object,
    fields: set[str],
    category: str,
) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        _fail(category, "JSON object fields are not exact")
    return value


def _require_string(value: object, category: str) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        _fail(category, "expected a non-empty single-line string")
    return value


def _require_positive_int(value: object, category: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _fail(category, "expected a positive integer")
    return value


def _require_non_negative_int(value: object, category: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(category, "expected a non-negative integer")
    return value


def _require_sha256(value: object, category: str) -> str:
    value = _require_string(value, category)
    if not SHA256_PATTERN.fullmatch(value):
        _fail(category, "expected a lowercase SHA-256")
    return value


def _require_git_sha(value: object, category: str) -> str:
    value = _require_string(value, category)
    if not GIT_SHA_PATTERN.fullmatch(value):
        _fail(category, "expected a full lowercase git SHA")
    return value


def _require_absolute_path(value: object, category: str) -> Path:
    raw = _require_string(value, category)
    path = Path(os.path.abspath(raw))
    if raw != str(path) or ".." in Path(raw).parts:
        _fail(category, "expected a normalized absolute path")
    return path


def _reject_symlink(path: Path, category: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RecoveryError(category, f"cannot stat {path}") from exc
    if stat.S_ISLNK(metadata.st_mode):
        _fail(category, f"symlink is forbidden: {path}")
    return metadata


def _read_regular_bytes(
    path: Path,
    category: str,
    *,
    maximum_bytes: int = MAX_JSON_BYTES,
) -> tuple[bytes, os.stat_result]:
    before = _reject_symlink(path, category)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_size <= 0
        or before.st_size > maximum_bytes
    ):
        _fail(category, f"file size/type is invalid: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RecoveryError(category, f"cannot open {path}") from exc
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            _fail(category, f"file changed while opening: {path}")
        chunks: list[bytes] = []
        remaining = maximum_bytes + 1
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
    if (
        len(raw) != before.st_size
        or len(raw) > maximum_bytes
        or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    ):
        _fail(category, f"file changed while reading: {path}")
    return raw, before


def _read_json(
    path: Path,
    category: str,
    *,
    maximum_bytes: int = MAX_JSON_BYTES,
) -> tuple[Mapping[str, Any], bytes, os.stat_result]:
    raw, metadata = _read_regular_bytes(
        path,
        category,
        maximum_bytes=maximum_bytes,
    )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RecoveryError(category, f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        _fail(category, f"expected one JSON object: {path}")
    return payload, raw, metadata


def _verify_file_identity(
    path: Path,
    *,
    expected_bytes: int,
    expected_sha256: str,
    category: str,
    maximum_bytes: int | None = None,
    required_uid: int | None = None,
    forbid_group_world_write: bool = False,
) -> bytes:
    before = _reject_symlink(path, category)
    if not stat.S_ISREG(before.st_mode) or before.st_size != expected_bytes:
        _fail(category, f"byte length mismatch: {path}")
    if required_uid is not None and before.st_uid != required_uid:
        _fail(category, f"file owner is not trusted: {path}")
    if forbid_group_world_write and stat.S_IMODE(before.st_mode) & 0o022:
        _fail(category, f"file is group/world writable: {path}")
    if maximum_bytes is not None and before.st_size > maximum_bytes:
        _fail(category, f"file exceeds recovery limit: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RecoveryError(category, f"cannot open {path}") from exc
    digest = hashlib.sha256()
    chunks: list[bytes] = []
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            _fail(category, f"file changed while opening: {path}")
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            if maximum_bytes is not None:
                chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        digest.hexdigest() != expected_sha256
        or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    ):
        _fail(category, f"SHA-256 or stable-read mismatch: {path}")
    return b"".join(chunks)


def _read_secure_text(path: Path, category: str) -> str:
    raw, metadata = _read_regular_bytes(
        path,
        category,
        maximum_bytes=MAX_JSON_BYTES,
    )
    if (
        metadata.st_uid != TRUSTED_SYSTEM_UID
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        _fail(category, f"configuration owner/mode is unsafe: {path}")
    try:
        return raw.decode("utf-8")
    except UnicodeError as exc:
        raise RecoveryError(category, f"configuration is not UTF-8: {path}") from exc


def _json_sha256(payload: Mapping[str, Any]) -> tuple[bytes, str]:
    raw = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False).encode(
            "utf-8"
        )
        + b"\n"
    )
    return raw, hashlib.sha256(raw).hexdigest()


def _require_mode(value: object, category: str) -> int:
    value = _require_string(value, category)
    if not re.fullmatch(r"0[0-7]{3}", value):
        _fail(category, "expected a four-digit octal mode")
    return int(value, 8)


def _validate_residue_identity(
    value: object,
    *,
    fields: set[str],
    category: str,
) -> Mapping[str, Any]:
    identity = _require_mapping(value, fields, category)
    kind = identity.get("kind")
    if kind not in {"file", "symlink"}:
        _fail(category, "residue kind must be file or symlink")
    _require_absolute_path(identity.get("path"), category)
    for field in ("device", "inode", "nlink", "bytes", "mtimeNs"):
        _require_positive_int(identity.get(field), category)
    for field in ("uid", "gid"):
        _require_non_negative_int(identity.get(field), category)
    _require_mode(identity.get("mode"), category)
    if identity.get("nlink") != 1:
        _fail(category, "residue identity must have exactly one hard link")
    if kind == "file":
        _require_sha256(identity.get("sha256"), category)
        if identity.get("target") is not None or identity.get("targetSha256") is not None:
            _fail(category, "regular residue cannot carry a symlink target")
    else:
        target = _require_absolute_path(identity.get("target"), category)
        if not target.is_relative_to(Path("/opt/jato/releases")):
            _fail(category, "residue symlink target is outside the release root")
        _require_sha256(identity.get("targetSha256"), category)
        if identity.get("sha256") is not None:
            _fail(category, "symlink residue cannot carry a file SHA-256")
    return identity


def _validate_residue_plan(plan: Mapping[str, Any]) -> None:
    residue = _require_mapping(plan.get("residue"), RESIDUE_FIELDS, "plan_invalid")
    if (
        plan.get("incidentId") != RESIDUE_INCIDENT_ID
        or plan.get("checkpoint", {}).get("identity", {}).get("commit")
        != RESIDUE_TARGET_COMMIT
        or residue.get("profile") != "materialized_never_started"
    ):
        _fail("plan_invalid", "schema v3 is restricted to the reviewed 29df incident")
    quarantine_root = _require_absolute_path(
        residue.get("quarantineRoot"),
        "plan_invalid",
    )
    if quarantine_root != RESIDUE_QUARANTINE_ROOT:
        _fail("plan_invalid", "schema v3 quarantine root changed")
    if (
        residue.get("quarantineDevice") != RESIDUE_QUARANTINE_DEVICE
        or residue.get("quarantineOwnerUid") != TRUSTED_SYSTEM_UID
        or residue.get("quarantineOwnerGid") != TRUSTED_SYSTEM_UID
        or _require_mode(residue.get("quarantineMode"), "plan_invalid") != 0o700
        or residue.get("manifestName") != "quarantine-contract.json"
        or residue.get("finalFenceName") != "recovery-fence-final"
    ):
        _fail("plan_invalid", "schema v3 quarantine contract changed")
    fence_content = _require_string(residue.get("fenceContent"), "plan_invalid")
    if fence_content != (
        f"release={RESIDUE_TARGET_COMMIT} "
        f"status=recovery_in_progress incident={RESIDUE_INCIDENT_ID}"
    ):
        _fail("plan_invalid", "schema v3 recovery fence content changed")

    candidate_unit = _require_mapping(
        residue.get("candidateUnit"),
        CANDIDATE_UNIT_FIELDS,
        "plan_invalid",
    )
    if candidate_unit != {
        "name": "jato-fullstack-backend@8001.service",
        "loadState": "loaded",
        "activeState": "inactive",
        "subState": "dead",
        "unitFileState": "disabled",
        "mainPid": 0,
        "result": "success",
        "nRestarts": 0,
        "execMainStartTimestampMonotonic": 0,
        "activeEnterTimestampMonotonic": 0,
        "inactiveEnterTimestampMonotonic": 0,
        "invocationId": "",
        "fragmentPath": str(RESIDUE_PATHS["candidate_explicit_unit"]),
        "dropInPaths": [
            str(RESIDUE_PATHS["candidate_sandbox_dropin"]),
            str(RESIDUE_PATHS["candidate_cpu_quota_dropin"]),
            str(RESIDUE_PATHS["candidate_memory_high_dropin"]),
            str(RESIDUE_PATHS["candidate_memory_max_dropin"]),
        ],
        "memoryHighBytes": 3 * 1024**3,
        "memoryMaxBytes": 4 * 1024**3,
    }:
        _fail("plan_invalid", "schema v3 Candidate unit proof changed")

    items = residue.get("items")
    if not isinstance(items, list) or len(items) != len(RESIDUE_PATHS):
        _fail("plan_invalid", "schema v3 residue item set is incomplete")
    seen: set[str] = set()
    for raw_item in items:
        item = _validate_residue_identity(
            raw_item,
            fields=RESIDUE_ITEM_FIELDS,
            category="plan_invalid",
        )
        item_id = _require_string(item.get("id"), "plan_invalid")
        quarantine_name = _require_string(
            item.get("quarantineName"),
            "plan_invalid",
        )
        if (
            item_id in seen
            or RESIDUE_PATHS.get(item_id) != Path(item["path"])
            or RESIDUE_QUARANTINE_NAMES.get(item_id) != quarantine_name
            or Path(quarantine_name).name != quarantine_name
            or item.get("device") != residue["quarantineDevice"]
            or item.get("uid") != TRUSTED_SYSTEM_UID
            or item.get("gid") != TRUSTED_SYSTEM_UID
        ):
            _fail("plan_invalid", "schema v3 residue path/name whitelist changed")
        if item_id == "candidate_slot_link":
            if (
                item.get("kind") != "symlink"
                or Path(str(item.get("target") or ""))
                != Path(plan["archive"]["releaseRoot"])
            ):
                _fail("plan_invalid", "schema v3 Candidate slot target changed")
        elif item.get("kind") != "file":
            _fail("plan_invalid", "schema v3 regular residue type changed")
        seen.add(item_id)
    if seen != set(RESIDUE_PATHS):
        _fail("plan_invalid", "schema v3 residue IDs changed")

    retained = residue.get("retainedEvidence")
    if not isinstance(retained, list) or len(retained) != 2:
        _fail("plan_invalid", "schema v3 retained evidence set changed")
    retained_by_id: dict[str, Mapping[str, Any]] = {}
    for value in retained:
        retained_item = _validate_residue_identity(
            value,
            fields=RETAINED_EVIDENCE_FIELDS,
            category="plan_invalid",
        )
        retained_id = _require_string(retained_item.get("id"), "plan_invalid")
        if retained_id in retained_by_id:
            _fail("plan_invalid", "schema v3 retained evidence IDs changed")
        retained_by_id[retained_id] = retained_item
    expected_previous_metadata = (
        RESIDUE_PREVIOUS_METADATA_ROOT
        / RESIDUE_TARGET_COMMIT
        / f"{plan['checkpoint']['identity']['archiveSha256']}.json"
    )
    previous_metadata = retained_by_id.get("previous_metadata")
    canonical_nginx = retained_by_id.get("canonical_nginx_config")
    if (
        set(retained_by_id) != {"previous_metadata", "canonical_nginx_config"}
        or previous_metadata is None
        or Path(previous_metadata["path"]) != expected_previous_metadata
        or previous_metadata.get("kind") != "file"
        or canonical_nginx is None
        or canonical_nginx
        != {
            "bytes": 3795,
            "device": RESIDUE_QUARANTINE_DEVICE,
            "gid": TRUSTED_SYSTEM_UID,
            "id": "canonical_nginx_config",
            "inode": 789351,
            "kind": "file",
            "mode": "0644",
            "mtimeNs": 1783042607281907362,
            "nlink": 1,
            "path": "/etc/nginx/sites-available/jato_fullstack.conf",
            "sha256": (
                "964c351bbed725a36da517c06ce7ef82ff9d11046e8329f02a118f638a32aec4"
            ),
            "target": None,
            "targetSha256": None,
            "uid": TRUSTED_SYSTEM_UID,
        }
    ):
        _fail("plan_invalid", "schema v3 retained evidence proof changed")

    absent_paths = residue.get("requiredAbsentPaths")
    if (
        not isinstance(absent_paths, list)
        or {Path(_require_absolute_path(path, "plan_invalid")) for path in absent_paths}
        != RESIDUE_REQUIRED_ABSENT_PATHS
        or len(absent_paths) != len(RESIDUE_REQUIRED_ABSENT_PATHS)
    ):
        _fail("plan_invalid", "schema v3 required-absence set changed")


def load_recovery_plan(
    path: Path,
    expected_sha256: str,
) -> tuple[Mapping[str, Any], str]:
    expected_sha256 = _require_sha256(expected_sha256, "plan_digest_invalid")
    payload, raw, _ = _read_json(path, "plan_invalid")
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    if actual_sha256 != expected_sha256:
        _fail("plan_digest_mismatch", "recovery plan SHA-256 changed")
    version = _plan_schema_version(payload)
    plan = _require_mapping(
        payload,
        RESIDUE_PLAN_FIELDS if version == 3 else LEGACY_PLAN_FIELDS,
        "plan_invalid",
    )
    incident = _require_string(plan.get("incidentId"), "plan_invalid")
    if not SAFE_INCIDENT_PATTERN.fullmatch(incident):
        _fail("plan_invalid", "incidentId is unsafe")
    repository = _require_string(plan.get("repository"), "plan_invalid")

    checkpoint_plan = _require_mapping(
        plan.get("checkpoint"),
        CHECKPOINT_PLAN_FIELDS,
        "plan_invalid",
    )
    identity = ReleaseIdentity.from_mapping(checkpoint_plan.get("identity"))
    if identity.repository != repository:
        _fail("plan_invalid", "checkpoint repository differs from plan")
    archive_plan = _require_mapping(
        plan.get("archive"),
        ARCHIVE_PLAN_FIELDS,
        "plan_invalid",
    )
    backup_plan = _require_mapping(
        plan.get("backup"),
        BACKUP_PLAN_FIELDS,
        "plan_invalid",
    )
    expected = _require_mapping(
        plan.get("expected"),
        EXPECTED_PLAN_FIELDS,
        "plan_invalid",
    )
    runtime = _require_mapping(
        plan.get("runtime"),
        RESIDUE_RUNTIME_PLAN_FIELDS if version == 3 else RUNTIME_PLAN_FIELDS,
        "plan_invalid",
    )
    source_proofs = plan.get("sourceProofs")
    if not isinstance(source_proofs, dict) or not source_proofs:
        _fail("plan_invalid", "sourceProofs must be a non-empty object")
    for relative, digest in source_proofs.items():
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or ".." in Path(relative).parts
        ):
            _fail("plan_invalid", "sourceProofs contains an unsafe path")
        _require_sha256(digest, "plan_invalid")

    checkpoint_path = _require_absolute_path(
        checkpoint_plan.get("path"),
        "plan_invalid",
    )
    journal_path = _require_absolute_path(
        checkpoint_plan.get("journalPath"),
        "plan_invalid",
    )
    evidence_path = _require_absolute_path(
        checkpoint_plan.get("evidencePath"),
        "plan_invalid",
    )
    state_root = checkpoint_path.parents[2]
    expected_checkpoint = (
        state_root
        / "checkpoints"
        / identity.commit
        / f"{identity.archiveSha256}.json"
    )
    expected_journal = (
        state_root
        / "journals"
        / identity.commit
        / f"{identity.archiveSha256}.jsonl"
    )
    expected_evidence = expected_checkpoint.with_name(
        f"{identity.archiveSha256}.evidence.json"
    )
    if (
        checkpoint_path != expected_checkpoint
        or journal_path != expected_journal
        or evidence_path != expected_evidence
    ):
        _fail("plan_invalid", "checkpoint namespace paths do not match identity")

    archive_path = _require_absolute_path(
        archive_plan.get("path"),
        "plan_invalid",
    )
    if (
        archive_plan.get("sha256") != identity.archiveSha256
        or archive_plan.get("bytes") != identity.archiveBytes
        or archive_path.name != f"{identity.archiveSha256}.tar.gz"
    ):
        _fail("plan_invalid", "archive plan differs from checkpoint identity")
    for field in (
        "root",
        "manifestPath",
        "dumpPath",
    ):
        _require_absolute_path(backup_plan.get(field), "plan_invalid")
    for field in (
        "activeReleaseRoot",
        "activeFrontendRoot",
    ):
        _require_absolute_path(expected.get(field), "plan_invalid")
    for field in (
        "activeSlotFile",
        "activeReleaseLink",
        "candidateSlotLink",
        "bluegreenStateRoot",
        "deploymentMarker",
        "schedulerState",
        "nginxConfig",
        "backendEnv",
        "slotEnv",
        "venv",
    ):
        _require_absolute_path(runtime.get(field), "plan_invalid")
    if version == 3:
        canonical_nginx = _require_absolute_path(
            runtime.get("nginxCanonicalConfig"),
            "plan_invalid",
        )
        if canonical_nginx != Path(
            "/etc/nginx/sites-available/jato_fullstack.conf"
        ):
            _fail("plan_invalid", "schema v3 canonical Nginx path changed")

    active_slot = _require_string(expected.get("activeSlot"), "plan_invalid")
    candidate_slot = _require_string(expected.get("candidateSlot"), "plan_invalid")
    if {active_slot, candidate_slot} != {"8000", "8001"}:
        _fail("plan_invalid", "active/candidate slots must be 8000 and 8001")
    _require_git_sha(expected.get("activeCommit"), "plan_invalid")
    for field in (
        "workers",
        "memoryHighBytes",
        "memoryMaxBytes",
        "activeBuildMetaBytes",
        "activeSlotFileBytes",
        "activeBackendActiveEnterTimestampMonotonic",
        "activeBackendExecMainStartTimestampMonotonic",
    ):
        _require_positive_int(expected.get(field), "plan_invalid")
    _require_non_negative_int(
        expected.get("activeBackendNRestarts"),
        "plan_invalid",
    )
    for field in ("activeBuildMetaSha256", "activeSlotFileSha256"):
        _require_sha256(expected.get(field), "plan_invalid")
    _require_positive_int(runtime.get("nginxConfigBytes"), "plan_invalid")
    _require_sha256(runtime.get("nginxConfigSha256"), "plan_invalid")
    if not re.fullmatch(
        r"[0-9a-f]{32}",
        str(expected.get("activeBackendInvocationId") or ""),
    ):
        _fail("plan_invalid", "active backend InvocationID is invalid")
    control_group = _require_string(
        expected.get("activeBackendControlGroup"),
        "plan_invalid",
    )
    if not control_group.startswith("/system.slice/") or ".." in Path(
        control_group
    ).parts:
        _fail("plan_invalid", "active backend cgroup is invalid")
    if runtime.get("nginxMode") != "legacy_pre_candidate":
        _fail(
            "plan_invalid",
            "only the reviewed legacy_pre_candidate Nginx mode is valid",
        )
    revisions = expected.get("revisions")
    if (
        not isinstance(revisions, list)
        or not revisions
        or any(not REVISION_PATTERN.fullmatch(str(item)) for item in revisions)
        or len(set(revisions)) != len(revisions)
    ):
        _fail("plan_invalid", "expected revisions are invalid")
    server_names = runtime.get("serverNames")
    if (
        not isinstance(server_names, list)
        or not server_names
        or any(
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9.-]+", name)
            for name in server_names
        )
    ):
        _fail("plan_invalid", "serverNames are invalid")
    if version == 3:
        _validate_residue_plan(plan)
    return plan, actual_sha256


def _read_checkpoint_state(
    plan: Mapping[str, Any],
) -> tuple[Mapping[str, Any], bytes, os.stat_result]:
    checkpoint_plan = plan["checkpoint"]
    checkpoint_path = Path(checkpoint_plan["path"])
    payload, raw, metadata = _read_json(
        checkpoint_path,
        "checkpoint_invalid",
    )
    try:
        checkpoint = validate_checkpoint(payload)
    except ValueError as exc:
        raise RecoveryError("checkpoint_invalid", "schema validation failed") from exc
    identity = ReleaseIdentity.from_mapping(checkpoint_plan["identity"])
    if checkpoint.get("identity") != identity.to_dict():
        _fail("identity_mismatch", "checkpoint identity changed")
    return checkpoint, raw, metadata


def _validate_evidence_migration(
    plan: Mapping[str, Any],
    evidence_migration: Mapping[str, Any],
) -> None:
    version = _plan_schema_version(plan)
    if version == 1:
        if evidence_migration != {
            "status": "not_required",
            "preRevision": None,
            "targetRevision": None,
            "resultRevision": None,
        }:
            _fail(
                "legacy_evidence_invalid",
                "expected the exact enabled-DB not_required legacy signature",
            )
        return

    if evidence_migration.get("status") != "completed":
        _fail(
            "legacy_evidence_invalid",
            "schema v2 requires completed migration evidence",
        )
    expected_revisions = sorted(plan["expected"]["revisions"])
    for field in ("preRevision", "targetRevision", "resultRevision"):
        value = evidence_migration.get(field)
        if not isinstance(value, str) or not value:
            _fail(
                "legacy_evidence_invalid",
                f"schema v2 {field} must be non-empty revision output",
            )
        if _revision_set(value, "legacy_evidence_invalid") != expected_revisions:
            _fail(
                "legacy_evidence_invalid",
                f"schema v2 {field} differs from reviewed revisions",
            )


def _validate_legacy_checkpoint_chain(
    plan: Mapping[str, Any],
) -> tuple[ReleaseIdentity, os.stat_result]:
    checkpoint_plan = plan["checkpoint"]
    identity = ReleaseIdentity.from_mapping(checkpoint_plan["identity"])
    checkpoint, checkpoint_raw, checkpoint_metadata = _read_checkpoint_state(plan)
    if checkpoint["phase"] == PRE_SWITCH_ABORT_PHASE:
        return identity, checkpoint_metadata
    if (
        checkpoint["phase"] != "migrated"
        or checkpoint["status"] != "completed"
        or checkpoint["retryClass"] != "automatic"
        or checkpoint["sequence"] != checkpoint_plan["sequence"]
        or len(checkpoint_raw) != checkpoint_plan["bytes"]
        or hashlib.sha256(checkpoint_raw).hexdigest() != checkpoint_plan["sha256"]
    ):
        _fail(
            "checkpoint_state_mismatch",
            "legacy checkpoint is not the reviewed migrated terminal",
        )

    journal_raw, _ = _read_regular_bytes(
        Path(checkpoint_plan["journalPath"]),
        "journal_invalid",
        maximum_bytes=MAX_JSON_BYTES,
    )
    expected_journal_bytes = _require_positive_int(
        checkpoint_plan["journalBytes"],
        "plan_invalid",
    )
    expected_journal_sha256 = _require_sha256(
        checkpoint_plan["journalSha256"],
        "plan_invalid",
    )
    journal_lines = journal_raw.splitlines(keepends=True)
    base_journal = journal_raw
    journal_ahead = False
    if (
        len(journal_raw) != expected_journal_bytes
        or hashlib.sha256(journal_raw).hexdigest() != expected_journal_sha256
    ):
        if len(journal_lines) != checkpoint_plan["journalLines"] + 1:
            _fail("journal_invalid", "journal digest differs from reviewed history")
        base_journal = b"".join(journal_lines[:-1])
        if (
            len(base_journal) != expected_journal_bytes
            or hashlib.sha256(base_journal).hexdigest()
            != expected_journal_sha256
        ):
            _fail("journal_invalid", "journal prefix differs from reviewed history")
        journal_ahead = True
    try:
        events = [
            json.loads(line)
            for line in base_journal.decode("utf-8").splitlines()
            if line
        ]
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RecoveryError("journal_invalid", "journal is not JSONL") from exc
    if (
        len(events) != checkpoint_plan["journalLines"]
        or len(events) != checkpoint["sequence"]
    ):
        _fail("journal_invalid", "journal length differs from checkpoint sequence")
    for index, event in enumerate(events, start=1):
        if not isinstance(event, dict) or event.get("event") != "checkpoint_transition":
            _fail("journal_invalid", "journal event type is invalid")
        if event.get("sequence") != index:
            _fail("journal_invalid", "journal sequence is not contiguous")
        if event.get("identity") != identity.to_dict():
            _fail("journal_invalid", "journal identity changed")
        if event.get("phase") in SWITCH_OR_LATER_PHASES:
            _fail("candidate_or_switch_seen", "journal crossed the Candidate boundary")
    last = dict(events[-1])
    last.pop("event", None)
    if last != checkpoint:
        _fail("journal_invalid", "journal tail differs from checkpoint")
    if journal_ahead:
        try:
            ahead = json.loads(journal_lines[-1])
        except json.JSONDecodeError as exc:
            raise RecoveryError(
                "journal_invalid",
                "journal-ahead event is invalid JSON",
            ) from exc
        if (
            not isinstance(ahead, dict)
            or ahead.get("event") != "checkpoint_transition"
            or ahead.get("sequence") != checkpoint["sequence"] + 1
            or ahead.get("identity") != identity.to_dict()
            or ahead.get("phase") != PRE_SWITCH_ABORT_PHASE
            or ahead.get("status") != "completed"
            or ahead.get("retryClass") != "automatic"
        ):
            _fail("journal_invalid", "journal-ahead event is not a recovery retry")

    evidence_path = Path(checkpoint_plan["evidencePath"])
    evidence_raw = _verify_file_identity(
        evidence_path,
        expected_bytes=_require_positive_int(
            checkpoint_plan["evidenceBytes"],
            "plan_invalid",
        ),
        expected_sha256=_require_sha256(
            checkpoint_plan["evidenceSha256"],
            "plan_invalid",
        ),
        category="legacy_evidence_invalid",
        maximum_bytes=MAX_JSON_BYTES,
    )
    bindings = re.findall(
        r"(?:^|[; ])evidence_path=(\S+) "
        r"evidence_sha256=([0-9a-f]{64})(?:$|[; ])",
        str(checkpoint.get("message") or ""),
    )
    if bindings != [
        (str(evidence_path), str(checkpoint_plan["evidenceSha256"]))
    ]:
        _fail("legacy_evidence_invalid", "checkpoint evidence binding changed")
    try:
        evidence_value = json.loads(evidence_raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RecoveryError("legacy_evidence_invalid", "invalid JSON") from exc
    evidence = _require_mapping(
        evidence_value,
        EVIDENCE_FIELDS,
        "legacy_evidence_invalid",
    )
    if evidence.get("identity") != identity.to_dict():
        _fail("legacy_evidence_invalid", "evidence identity changed")
    evidence_backup = _require_mapping(
        evidence.get("backup"),
        EVIDENCE_BACKUP_FIELDS,
        "legacy_evidence_invalid",
    )
    evidence_migration = _require_mapping(
        evidence.get("migration"),
        EVIDENCE_MIGRATION_FIELDS,
        "legacy_evidence_invalid",
    )
    if evidence_backup != {
        "manifestPath": plan["backup"]["manifestPath"],
        "manifestBytes": plan["backup"]["manifestBytes"],
        "manifestSha256": plan["backup"]["manifestSha256"],
    }:
        _fail("legacy_evidence_invalid", "backup binding differs from plan")
    _validate_evidence_migration(plan, evidence_migration)
    return identity, checkpoint_metadata


def _validate_backup_chain(plan: Mapping[str, Any]) -> None:
    backup = plan["backup"]
    root = Path(backup["root"])
    manifest_path = Path(backup["manifestPath"])
    dump_path = Path(backup["dumpPath"])
    for directory in (root, manifest_path.parent, dump_path.parent):
        metadata = _reject_symlink(directory, "backup_invalid")
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != TRUSTED_SYSTEM_UID
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            _fail("backup_invalid", f"backup directory is not private: {directory}")
    manifest_raw = _verify_file_identity(
        manifest_path,
        expected_bytes=_require_positive_int(
            backup["manifestBytes"],
            "plan_invalid",
        ),
        expected_sha256=_require_sha256(
            backup["manifestSha256"],
            "plan_invalid",
        ),
        category="backup_manifest_invalid",
        maximum_bytes=MAX_JSON_BYTES,
        required_uid=TRUSTED_SYSTEM_UID,
        forbid_group_world_write=True,
    )
    try:
        manifest = json.loads(manifest_raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RecoveryError("backup_manifest_invalid", "invalid JSON") from exc
    database = manifest.get("database") if isinstance(manifest, dict) else None
    if not isinstance(database, dict):
        _fail("backup_manifest_invalid", "database object is missing")
    if (
        database.get("enabled") is not True
        or database.get("required") is not True
        or database.get("status") != "completed"
        or database.get("format") != "postgresql_custom"
        or database.get("dumpPath") != str(dump_path)
        or database.get("dumpBytes") != backup["dumpBytes"]
        or database.get("dumpSha256") != backup["dumpSha256"]
    ):
        _fail("backup_manifest_invalid", "database backup contract changed")
    _verify_file_identity(
        dump_path,
        expected_bytes=_require_positive_int(backup["dumpBytes"], "plan_invalid"),
        expected_sha256=_require_sha256(
            backup["dumpSha256"],
            "plan_invalid",
        ),
        category="backup_dump_invalid",
        required_uid=TRUSTED_SYSTEM_UID,
        forbid_group_world_write=True,
    )


def _validate_legacy_archive_and_source(plan: Mapping[str, Any]) -> None:
    archive = plan["archive"]
    _verify_file_identity(
        Path(archive["path"]),
        expected_bytes=_require_positive_int(archive["bytes"], "plan_invalid"),
        expected_sha256=_require_sha256(archive["sha256"], "plan_invalid"),
        category="legacy_archive_invalid",
    )
    release_root = _require_absolute_path(
        archive["releaseRoot"],
        "plan_invalid",
    )
    metadata = _reject_symlink(release_root, "legacy_release_invalid")
    if not stat.S_ISDIR(metadata.st_mode):
        _fail("legacy_release_invalid", "release root is not a directory")
    identity_path = release_root / ".jato-release-identity"
    identity_raw, _ = _read_regular_bytes(
        identity_path,
        "legacy_release_invalid",
        maximum_bytes=4096,
    )
    if identity_raw.decode("utf-8").strip() != archive["releaseIdentity"]:
        _fail("legacy_release_invalid", "release identity marker changed")
    for relative, expected_sha256 in plan["sourceProofs"].items():
        source_path = release_root / relative
        raw, _ = _read_regular_bytes(
            source_path,
            "legacy_source_invalid",
            maximum_bytes=4 * 1024 * 1024,
        )
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            _fail("legacy_source_invalid", f"source proof changed: {relative}")


def _run_text(
    command: Sequence[str],
    *,
    category: str,
    timeout: int = 60,
    env: Mapping[str, str] | None = None,
) -> str:
    try:
        result = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=dict(env) if env is not None else None,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RecoveryError(category, "command could not be executed") from exc
    if result.returncode != 0:
        _fail(category, f"command failed with exit code {result.returncode}")
    return result.stdout.strip()


def _run_bytes(
    command: Sequence[str],
    *,
    category: str,
    timeout: int = 60,
) -> bytes:
    try:
        result = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RecoveryError(category, "command could not be executed") from exc
    if result.returncode != 0:
        _fail(category, f"command failed with exit code {result.returncode}")
    return result.stdout


def _systemd_properties(unit: str) -> dict[str, str]:
    output = _run_text(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState",
            "--property=ActiveState",
            "--property=SubState",
            "--property=UnitFileState",
            "--property=MemoryHigh",
            "--property=MemoryMax",
            "--property=MainPID",
            "--property=InvocationID",
            "--property=NRestarts",
            "--property=ExecMainStartTimestampMonotonic",
            "--property=ActiveEnterTimestampMonotonic",
            "--property=ControlGroup",
            "--property=Result",
            "--property=InactiveEnterTimestampMonotonic",
            "--property=FragmentPath",
            "--property=DropInPaths",
            "--no-pager",
        ],
        category="systemd_probe_failed",
    )
    values: dict[str, str] = {}
    for line in output.splitlines():
        name, separator, value = line.partition("=")
        if separator:
            values[name] = value
    required = {
        "LoadState",
        "ActiveState",
        "SubState",
        "UnitFileState",
        "MemoryHigh",
        "MemoryMax",
        "MainPID",
        "InvocationID",
        "NRestarts",
        "ExecMainStartTimestampMonotonic",
        "ActiveEnterTimestampMonotonic",
        "ControlGroup",
        "Result",
        "InactiveEnterTimestampMonotonic",
        "FragmentPath",
        "DropInPaths",
    }
    if set(values) != required:
        _fail("systemd_probe_failed", f"incomplete properties for {unit}")
    return values


def _path_lexists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _stable_path_identity(path: Path, category: str) -> Mapping[str, Any]:
    try:
        before = path.lstat()
    except OSError as exc:
        raise RecoveryError(category, f"cannot stat {path}") from exc
    kind = "symlink" if stat.S_ISLNK(before.st_mode) else "file"
    if kind == "file":
        if not stat.S_ISREG(before.st_mode):
            _fail(category, f"unsupported residue type: {path}")
        raw, after = _read_regular_bytes(path, category, maximum_bytes=MAX_JSON_BYTES)
        identity: dict[str, Any] = {
            "kind": kind,
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
    else:
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
            _fail(category, f"symlink changed while reading: {path}")
        identity = {
            "kind": kind,
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
    return identity


def _expected_identity(item: Mapping[str, Any]) -> Mapping[str, Any]:
    return {field: item[field] for field in RESIDUE_ITEM_FIELDS if field not in {
        "id",
        "path",
        "quarantineName",
    }}


def _verify_residue_identity(
    path: Path,
    item: Mapping[str, Any],
    category: str = "residue_identity_mismatch",
) -> Mapping[str, Any]:
    if not _path_lexists(path):
        _fail(category, f"reviewed residue disappeared: {path}")
    actual = _stable_path_identity(path, category)
    if actual != _expected_identity(item):
        _fail(category, f"reviewed residue identity changed: {path}")
    return actual


def _verify_fence_identity(path: Path, plan: Mapping[str, Any]) -> Mapping[str, Any]:
    if not _path_lexists(path):
        _fail("recovery_fence_invalid", f"recovery fence is absent: {path}")
    identity = _stable_path_identity(path, "recovery_fence_invalid")
    expected_raw = (plan["residue"]["fenceContent"] + "\n").encode("utf-8")
    if (
        identity.get("kind") != "file"
        or identity.get("uid") != plan["residue"]["quarantineOwnerUid"]
        or identity.get("gid") != plan["residue"]["quarantineOwnerGid"]
        or identity.get("mode") != "0644"
        or identity.get("nlink") != 1
        or identity.get("bytes") != len(expected_raw)
        or identity.get("sha256") != hashlib.sha256(expected_raw).hexdigest()
    ):
        _fail("recovery_fence_invalid", f"recovery fence identity changed: {path}")
    return identity


def _recovery_fence_temp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}{RECOVERY_FENCE_TEMP_SUFFIX}")


def _verify_replayable_fence_temp(
    path: Path,
    plan: Mapping[str, Any],
) -> None:
    """Accept only a crash fragment created by this exact fence publisher."""

    expected_raw = (plan["residue"]["fenceContent"] + "\n").encode("utf-8")
    before = _reject_symlink(path, "recovery_fence_invalid")
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_dev != plan["residue"]["quarantineDevice"]
        or before.st_uid != plan["residue"]["quarantineOwnerUid"]
        or before.st_gid != plan["residue"]["quarantineOwnerGid"]
        or stat.S_IMODE(before.st_mode) not in {0o600, 0o644}
        or before.st_nlink != 1
        or before.st_size > len(expected_raw)
    ):
        _fail(
            "recovery_fence_invalid",
            f"recovery fence temp identity is unsafe: {path}",
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RecoveryError(
            "recovery_fence_invalid",
            f"cannot open recovery fence temp: {path}",
        ) from exc
    try:
        opened = os.fstat(descriptor)
        raw = b""
        while len(raw) <= len(expected_raw):
            chunk = os.read(descriptor, len(expected_raw) + 1 - len(raw))
            if not chunk:
                break
            raw += chunk
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        or (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        or len(raw) != before.st_size
        or not expected_raw.startswith(raw)
    ):
        _fail(
            "recovery_fence_invalid",
            f"recovery fence temp is foreign or changed: {path}",
        )


def _residue_items(plan: Mapping[str, Any]) -> Mapping[str, Mapping[str, Any]]:
    return {item["id"]: item for item in plan["residue"]["items"]}


def _retained_evidence_items(
    plan: Mapping[str, Any],
) -> Mapping[str, Mapping[str, Any]]:
    return {item["id"]: item for item in plan["residue"]["retainedEvidence"]}


def _verify_quarantine_root(path: Path, plan: Mapping[str, Any]) -> None:
    metadata = _reject_symlink(path, "quarantine_invalid")
    residue = plan["residue"]
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_dev != residue["quarantineDevice"]
        or metadata.st_uid != residue["quarantineOwnerUid"]
        or metadata.st_gid != residue["quarantineOwnerGid"]
        or stat.S_IMODE(metadata.st_mode) != _require_mode(
            residue["quarantineMode"],
            "quarantine_invalid",
        )
    ):
        _fail("quarantine_invalid", "quarantine root identity is unsafe")


def _residue_inventory_digest(plan: Mapping[str, Any]) -> str:
    payload = {
        "incidentId": plan["incidentId"],
        "identity": plan["checkpoint"]["identity"],
        "profile": plan["residue"]["profile"],
        "candidateUnit": plan["residue"]["candidateUnit"],
        "items": plan["residue"]["items"],
        "retainedEvidence": plan["residue"]["retainedEvidence"],
        "requiredAbsentPaths": plan["residue"]["requiredAbsentPaths"],
    }
    return _json_sha256(payload)[1]


def _verify_retained_and_absent_paths(plan: Mapping[str, Any]) -> None:
    for retained in plan["residue"]["retainedEvidence"]:
        _verify_residue_identity(
            Path(retained["path"]),
            retained,
            "retained_evidence_changed",
        )
    for raw_path in plan["residue"]["requiredAbsentPaths"]:
        path = Path(raw_path)
        if _path_lexists(path):
            _fail("unexpected_runtime_residue", f"path must remain absent: {path}")


def _collect_residue_state(
    plan: Mapping[str, Any],
    plan_sha256: str,
) -> Mapping[str, Any]:
    _verify_retained_and_absent_paths(plan)
    residue = plan["residue"]
    quarantine_root = Path(residue["quarantineRoot"])
    manifest_path = quarantine_root / residue["manifestName"]
    final_fence_path = quarantine_root / residue["finalFenceName"]
    marker_item = _residue_items(plan)["maintenance_marker"]
    marker_path = Path(marker_item["path"])
    marker_destination = quarantine_root / marker_item["quarantineName"]
    fence_temp_path = _recovery_fence_temp_path(marker_destination)

    if not _path_lexists(quarantine_root):
        for item in residue["items"]:
            _verify_residue_identity(Path(item["path"]), item)
        return {
            "stage": "initial",
            "inventoryDigest": _residue_inventory_digest(plan),
            "quarantineRoot": str(quarantine_root),
            "manifestPath": str(manifest_path),
            "manifestSha256": None,
            "maintenanceMarkerPresent": True,
            "candidateSlotLinkExists": True,
            "finalFencePath": str(final_fence_path),
            "finalFenceIdentity": None,
        }

    _verify_quarantine_root(quarantine_root, plan)
    manifest_sha256: str | None = None
    if _path_lexists(manifest_path):
        manifest_raw, manifest_metadata = _read_regular_bytes(
            manifest_path,
            "quarantine_manifest_invalid",
        )
        if (
            manifest_metadata.st_uid != residue["quarantineOwnerUid"]
            or manifest_metadata.st_gid != residue["quarantineOwnerGid"]
            or stat.S_IMODE(manifest_metadata.st_mode) != 0o600
        ):
            _fail("quarantine_manifest_invalid", "quarantine manifest is not private")
        manifest_sha256 = hashlib.sha256(manifest_raw).hexdigest()
        allowed_entries = {
            residue["manifestName"],
            residue["finalFenceName"],
            fence_temp_path.name,
            *(item["quarantineName"] for item in residue["items"]),
        }
        unexpected_entries = {
            entry.name for entry in quarantine_root.iterdir()
        } - allowed_entries
        if unexpected_entries:
            _fail(
                "quarantine_state_invalid",
                "quarantine contains unreviewed entries",
            )
        if _path_lexists(fence_temp_path):
            if _path_lexists(marker_destination) or _path_lexists(final_fence_path):
                _fail(
                    "quarantine_state_invalid",
                    "recovery fence temp conflicts with a published fence",
                )
            _verify_replayable_fence_temp(fence_temp_path, plan)
    else:
        entries = [entry.name for entry in quarantine_root.iterdir()]
        if entries:
            _fail(
                "quarantine_manifest_invalid",
                "quarantine contains entries without its immutable manifest",
            )
        return {
            "stage": "quarantine_root_created",
            "inventoryDigest": _residue_inventory_digest(plan),
            "quarantineRoot": str(quarantine_root),
            "manifestPath": str(manifest_path),
            "manifestSha256": None,
            "maintenanceMarkerPresent": True,
            "candidateSlotLinkExists": True,
            "finalFencePath": str(final_fence_path),
            "finalFenceIdentity": None,
        }

    item_states: dict[str, str] = {}
    for item_id, item in _residue_items(plan).items():
        source = Path(item["path"])
        destination = quarantine_root / item["quarantineName"]
        source_exists = _path_lexists(source)
        destination_exists = _path_lexists(destination)
        if item_id == "maintenance_marker":
            if source_exists and not destination_exists and not _path_lexists(
                final_fence_path
            ):
                _verify_residue_identity(source, item)
                item_states[item_id] = "fence_not_staged"
            elif source_exists and destination_exists:
                source_identity = _stable_path_identity(
                    source,
                    "recovery_fence_invalid",
                )
                if source_identity == _expected_identity(item):
                    _verify_fence_identity(destination, plan)
                    item_states[item_id] = "exchange_pending"
                else:
                    _verify_fence_identity(source, plan)
                    _verify_residue_identity(destination, item)
                    item_states[item_id] = "quarantined_fenced"
            elif not source_exists and destination_exists and _path_lexists(final_fence_path):
                _verify_residue_identity(destination, item)
                item_states[item_id] = "finalized"
            else:
                _fail("quarantine_state_invalid", "maintenance marker state is ambiguous")
            continue
        if source_exists == destination_exists:
            _fail(
                "quarantine_state_invalid",
                f"residue must exist at exactly one location: {item_id}",
            )
        if source_exists:
            _verify_residue_identity(source, item)
            item_states[item_id] = "pending"
        else:
            _verify_residue_identity(destination, item)
            item_states[item_id] = "quarantined"

    marker_state = item_states["maintenance_marker"]
    non_marker_states = {
        state for item_id, state in item_states.items() if item_id != "maintenance_marker"
    }
    final_fence_identity = None
    if marker_state == "finalized":
        final_fence_identity = _verify_fence_identity(final_fence_path, plan)
        stage = "finalized" if non_marker_states == {"quarantined"} else "partial"
    elif marker_state == "quarantined_fenced" and non_marker_states == {"quarantined"}:
        if _path_lexists(final_fence_path):
            _fail("quarantine_state_invalid", "final fence exists before finalization")
        stage = "quarantined_fenced"
    else:
        if _path_lexists(final_fence_path):
            _fail("quarantine_state_invalid", "unexpected final fence exists")
        stage = "partial"
    return {
        "stage": stage,
        "inventoryDigest": _residue_inventory_digest(plan),
        "quarantineRoot": str(quarantine_root),
        "manifestPath": str(manifest_path),
        "manifestSha256": manifest_sha256,
        "maintenanceMarkerPresent": _path_lexists(marker_path),
        "candidateSlotLinkExists": _path_lexists(
            RESIDUE_PATHS["candidate_slot_link"]
        ),
        "itemStates": item_states,
        "finalFencePath": str(final_fence_path),
        "finalFenceIdentity": final_fence_identity,
        "planSha256": plan_sha256,
    }


def _read_virtual_file(path: Path, category: str) -> bytes:
    metadata = _reject_symlink(path, category)
    if not stat.S_ISREG(metadata.st_mode):
        _fail(category, f"virtual proof is not a regular file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RecoveryError(category, f"cannot open {path}") from exc
    try:
        chunks: list[bytes] = []
        total = 0
        while total <= 1024 * 1024:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    if not raw or len(raw) > 1024 * 1024:
        _fail(category, f"virtual proof is empty or oversized: {path}")
    return raw


def _process_cmdline(pid: int, category: str) -> str:
    raw = _read_virtual_file(Path(f"/proc/{pid}/cmdline"), category)
    return raw.replace(b"\0", b" ").decode("utf-8", errors="strict").strip()


def _active_worker_proof(
    unit: Mapping[str, str],
    *,
    expected_workers: int,
    expected_slot: str,
) -> Mapping[str, Any]:
    try:
        main_pid = int(unit["MainPID"])
    except (KeyError, ValueError) as exc:
        raise RecoveryError(
            "active_runtime_invalid",
            "active backend MainPID is invalid",
        ) from exc
    if main_pid <= 0:
        _fail("active_runtime_invalid", "active backend MainPID is not live")
    master_cmdline = _process_cmdline(main_pid, "active_runtime_invalid")
    if (
        f"--port {expected_slot}" not in master_cmdline
        or f"--workers {expected_workers}" not in master_cmdline
    ):
        _fail(
            "active_runtime_invalid",
            "active backend master command differs from worker contract",
        )
    cgroup = unit.get("ControlGroup") or ""
    cgroup_procs = Path("/sys/fs/cgroup") / cgroup.lstrip("/") / "cgroup.procs"
    raw_pids = _read_virtual_file(cgroup_procs, "active_runtime_invalid")
    try:
        pids = {
            int(line)
            for line in raw_pids.decode("ascii").splitlines()
            if line.strip()
        }
    except (UnicodeError, ValueError) as exc:
        raise RecoveryError(
            "active_runtime_invalid",
            "active backend cgroup PID list is invalid",
        ) from exc
    if main_pid not in pids:
        _fail("active_runtime_invalid", "active master is outside its cgroup")
    spawn_workers = 0
    resource_trackers = 0
    for pid in sorted(pids - {main_pid}):
        try:
            cmdline = _process_cmdline(pid, "active_runtime_invalid")
        except RecoveryError:
            _fail(
                "active_runtime_invalid",
                "active backend process disappeared during inspection",
            )
        if "multiprocessing.spawn" in cmdline and "spawn_main" in cmdline:
            spawn_workers += 1
        elif "multiprocessing.resource_tracker" in cmdline:
            resource_trackers += 1
        else:
            _fail(
                "active_runtime_invalid",
                "active backend cgroup contains an unexpected process",
            )
    if spawn_workers != expected_workers or resource_trackers != 1:
        _fail(
            "active_runtime_invalid",
            "active backend worker process count changed",
        )
    return {
        "mainPid": main_pid,
        "masterWorkersArgument": expected_workers,
        "workerProcesses": spawn_workers,
        "resourceTrackerProcesses": resource_trackers,
    }


def _parse_json_output(raw: str, category: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RecoveryError(category, "response is not JSON") from exc
    if not isinstance(payload, dict):
        _fail(category, "response is not a JSON object")
    return payload


def _validate_build_meta(
    payload: Mapping[str, Any],
    *,
    expected_commit: str,
    category: str,
) -> None:
    commit_fields = (
        "appCommit",
        "commit",
        "commitSha",
        "deployCommit",
        "githubSha",
        "sha",
    )
    commits = {
        str(payload.get(field))
        for field in commit_fields
        if payload.get(field) not in (None, "")
    }
    if commits != {expected_commit}:
        _fail(category, "build metadata does not bind one exact active SHA")


def _probe_legacy_health_and_build(
    *,
    health_url: str,
    build_url: str,
    expected_build: Mapping[str, Any],
    expected_build_bytes: int,
    expected_build_sha256: str,
    expected_commit: str,
    resolve_host: str | None = None,
) -> dict[str, Any]:
    curl = [
        "curl",
        "--noproxy",
        "*",
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--max-time",
        "20",
    ]
    if resolve_host is not None:
        curl.extend(
            [
                "--proto",
                "=https",
                "--proto-redir",
                "=https",
                "--resolve",
                f"{resolve_host}:443:127.0.0.1",
            ]
        )
    health = _parse_json_output(
        _run_text([*curl, health_url], category="health_probe_failed"),
        "health_probe_failed",
    )
    build_raw = _run_bytes(
        [*curl, build_url],
        category="health_probe_failed",
    )
    if (
        len(build_raw) != expected_build_bytes
        or hashlib.sha256(build_raw).hexdigest() != expected_build_sha256
    ):
        _fail(
            "health_probe_failed",
            "public build metadata byte identity differs from active file",
        )
    try:
        build_text = build_raw.decode("utf-8")
    except UnicodeError as exc:
        raise RecoveryError(
            "health_probe_failed",
            "public build metadata is not UTF-8",
        ) from exc
    build = _parse_json_output(build_text, "health_probe_failed")
    if health != {"status": "ok"}:
        _fail("health_probe_failed", "legacy healthz payload changed")
    _validate_build_meta(
        build,
        expected_commit=expected_commit,
        category="health_probe_failed",
    )
    if build != expected_build:
        _fail("health_probe_failed", "public build metadata differs from active file")
    return {
        "healthStatus": "ok",
        "frontendBuildCommit": expected_commit,
        "frontendBuildMetaSha256": expected_build_sha256,
    }


def _revision_set(raw: str, category: str) -> list[str]:
    revisions = sorted(set(REVISION_PATTERN.findall(raw)))
    if not revisions:
        _fail(category, "revision output is empty")
    return revisions


def _bash_probe(script: str, arguments: Sequence[str], category: str) -> str:
    return _run_text(
        ["bash", "-c", script, "jato-recovery", *arguments],
        category=category,
        timeout=120,
    )


def _collect_database_proof(
    plan: Mapping[str, Any],
    bundle_root: Path,
) -> Mapping[str, Any]:
    expected = plan["expected"]
    runtime = plan["runtime"]
    active_backend = Path(expected["activeReleaseRoot"]) / "06_AppPlatform/backend"
    old_backend = Path(plan["archive"]["releaseRoot"]) / "06_AppPlatform/backend"
    candidate_backend = bundle_root / "06_AppPlatform/backend"
    venv = Path(runtime["venv"])
    _read_secure_text(
        Path(runtime["backendEnv"]),
        "database_environment_invalid",
    )
    database_state = _bash_probe(
        'set -Eeuo pipefail; set -a; . "$1"; set +a; '
        'case "${APP_DATABASE_ENABLED:-false}" in '
        "1|true|TRUE|yes|YES|on|ON) : ;; *) exit 2 ;; esac; "
        'test -n "${APP_DATABASE_URL:-${DATABASE_URL:-}}"; printf enabled',
        [runtime["backendEnv"]],
        "database_not_enabled",
    )
    if database_state != "enabled":
        _fail("database_not_enabled", "database state is not enabled")
    read_only_output = _bash_probe(
        'set -Eeuo pipefail; set -a; . "$1"; set +a; '
        'export PYTHONPATH="$2"; '
        'export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }'
        '-c default_transaction_read_only=on"; '
        'cd "$2"; exec "$3/bin/python" -c "$4"',
        [
            runtime["backendEnv"],
            str(active_backend),
            str(venv),
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import DATABASE_URL; "
                "url=DATABASE_URL.replace('+asyncpg','+psycopg2')"
                ".replace('+aiopg','+psycopg2'); "
                "engine=create_engine(url, pool_pre_ping=True); "
                "connection=engine.connect(); "
                "value=str(connection.execute("
                "text('SHOW transaction_read_only')).scalar()).lower(); "
                "connection.close(); engine.dispose(); "
                "assert value in {'on','true'}; print('on')"
            ),
        ],
        "database_read_only_failed",
    )
    if read_only_output != "on":
        _fail(
            "database_read_only_failed",
            "database did not report transaction_read_only=on",
        )
    current_output = _bash_probe(
        'set -Eeuo pipefail; set -a; . "$1"; set +a; '
        'export PYTHONPATH="$2"; '
        'export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }'
        '-c default_transaction_read_only=on"; '
        'cd "$2"; exec "$3/bin/python" -m alembic current',
        [runtime["backendEnv"], str(active_backend), str(venv)],
        "database_current_failed",
    )
    old_heads_output = _bash_probe(
        'set -Eeuo pipefail; export PYTHONPATH="$1"; '
        'cd "$1"; exec "$2/bin/python" -m alembic heads',
        [str(old_backend), str(venv)],
        "old_heads_failed",
    )
    new_heads_output = _bash_probe(
        'set -Eeuo pipefail; export PYTHONPATH="$1"; '
        'cd "$1"; exec "$2/bin/python" -m alembic heads',
        [str(candidate_backend), str(venv)],
        "new_heads_failed",
    )
    backup_output = _run_text(
        [
            "pg_restore",
            "--data-only",
            "--table=alembic_version",
            "--file=-",
            plan["backup"]["dumpPath"],
        ],
        category="backup_revision_failed",
        timeout=120,
    )
    revisions = {
        "current": _revision_set(current_output, "database_current_failed"),
        "oldHeads": _revision_set(old_heads_output, "old_heads_failed"),
        "newHeads": _revision_set(new_heads_output, "new_heads_failed"),
        "backup": _revision_set(backup_output, "backup_revision_failed"),
    }
    expected_revisions = sorted(expected["revisions"])
    if any(value != expected_revisions for value in revisions.values()):
        _fail(
            "database_revision_mismatch",
            "backup/current/old heads/new heads are not the same reviewed set",
        )
    return {
        "enabled": True,
        "mode": "read_only",
        "pgoptions": "default_transaction_read_only=on",
        "transactionReadOnly": read_only_output,
        "currentRevisions": revisions["current"],
        "oldHeadRevisions": revisions["oldHeads"],
        "newHeadRevisions": revisions["newHeads"],
        "backupRevisions": revisions["backup"],
        "equal": True,
    }


def _candidate_never_started_proof(
    plan: Mapping[str, Any],
    unit: Mapping[str, str],
    *,
    stage: str,
    listener: bool,
) -> Mapping[str, Any]:
    try:
        proof = {
            "loadState": unit["LoadState"],
            "activeState": unit["ActiveState"],
            "subState": unit["SubState"],
            "unitFileState": unit["UnitFileState"],
            "mainPid": int(unit["MainPID"]),
            "result": unit["Result"],
            "nRestarts": int(unit["NRestarts"]),
            "execMainStartTimestampMonotonic": int(
                unit["ExecMainStartTimestampMonotonic"]
            ),
            "activeEnterTimestampMonotonic": int(
                unit["ActiveEnterTimestampMonotonic"]
            ),
            "inactiveEnterTimestampMonotonic": int(
                unit["InactiveEnterTimestampMonotonic"]
            ),
            "invocationId": unit["InvocationID"],
            "fragmentPath": unit["FragmentPath"],
            "dropInPaths": unit["DropInPaths"].split(),
            "memoryHighBytes": int(unit["MemoryHigh"]),
            "memoryMaxBytes": int(unit["MemoryMax"]),
            "listener": listener,
        }
    except (KeyError, ValueError) as exc:
        raise RecoveryError(
            "candidate_runtime_invalid",
            "Candidate lifetime proof is malformed",
        ) from exc
    never_started = (
        proof["activeState"] == "inactive"
        and proof["subState"] == "dead"
        and proof["unitFileState"] in {"", "disabled"}
        and proof["mainPid"] == 0
        and proof["nRestarts"] == 0
        and proof["execMainStartTimestampMonotonic"] == 0
        and proof["activeEnterTimestampMonotonic"] == 0
        and proof["inactiveEnterTimestampMonotonic"] == 0
        and proof["invocationId"] == ""
        and listener is False
    )
    if not never_started:
        _fail("candidate_started", "Candidate has runtime or lifetime evidence")
    expected = plan["residue"]["candidateUnit"]
    if stage in {"initial", "quarantine_root_created"} and proof != {
        **expected,
        "listener": False,
    }:
        _fail("candidate_runtime_invalid", "initial Candidate proof differs from plan")
    owned_paths = {str(path) for path in RESIDUE_PATHS.values()}
    referenced_paths = {proof["fragmentPath"], *proof["dropInPaths"]} - {""}
    allowed_template_path = "/etc/systemd/system/jato-fullstack-backend@.service"
    if referenced_paths - owned_paths - {allowed_template_path}:
        _fail(
            "candidate_runtime_invalid",
            "Candidate has unknown unit sources",
        )
    owned_references = sorted(referenced_paths & owned_paths)
    if stage == "finalized" and owned_references:
        _fail(
            "candidate_runtime_invalid",
            "systemd still references finalized Candidate residue",
        )
    proof["ownedSourceReferences"] = owned_references
    return proof


def _verify_candidate_detached_after_reload(
    plan: Mapping[str, Any],
) -> Mapping[str, Any]:
    candidate_slot = str(plan["expected"]["candidateSlot"])
    candidate_unit = _systemd_properties(
        f"{plan['runtime']['backendServicePrefix']}{candidate_slot}"
    )
    listener = bool(
        _run_text(
            ["ss", "-H", "-ltn", f"sport = :{candidate_slot}"],
            category="candidate_probe_failed",
        )
    )
    proof = _candidate_never_started_proof(
        plan,
        candidate_unit,
        stage="quarantined_fenced",
        listener=listener,
    )
    if proof["ownedSourceReferences"]:
        _fail(
            "candidate_daemon_reload_failed",
            "systemd retained quarantined Candidate sources after daemon-reload",
        )
    return proof


def collect_observation(
    plan: Mapping[str, Any],
    bundle_root: Path,
    plan_sha256: str = "",
) -> Mapping[str, Any]:
    expected = plan["expected"]
    runtime = plan["runtime"]
    archive = plan["archive"]
    active_slot = expected["activeSlot"]
    candidate_slot = expected["candidateSlot"]
    active_commit = expected["activeCommit"]
    active_root = Path(expected["activeReleaseRoot"])
    old_release_root = Path(archive["releaseRoot"])
    candidate_backend = bundle_root / "06_AppPlatform/backend"

    if not candidate_backend.is_dir() or candidate_backend.is_symlink():
        _fail("bundle_invalid", "candidate Alembic source is missing")
    active_slot_raw = _verify_file_identity(
        Path(runtime["activeSlotFile"]),
        expected_bytes=expected["activeSlotFileBytes"],
        expected_sha256=expected["activeSlotFileSha256"],
        category="production_state_changed",
        maximum_bytes=128,
        required_uid=TRUSTED_SYSTEM_UID,
        forbid_group_world_write=True,
    )
    active_slot_value = active_slot_raw.decode("utf-8").strip()
    if active_slot_value != active_slot:
        _fail("production_state_changed", "active slot differs from plan")

    active_slot_link = Path(f"/opt/jato/slots/{active_slot}/current")
    if (
        not active_slot_link.is_symlink()
        or active_slot_link.resolve(strict=True) != active_root
    ):
        _fail("production_state_changed", "active slot link differs from active root")
    plan_version = _plan_schema_version(plan)
    residue_state = (
        _collect_residue_state(plan, plan_sha256) if plan_version == 3 else None
    )
    candidate_slot_link = Path(runtime["candidateSlotLink"])
    candidate_slot_link_exists = (
        residue_state["candidateSlotLinkExists"]
        if residue_state is not None
        else candidate_slot_link.exists() or candidate_slot_link.is_symlink()
    )
    active_release_link = Path(runtime["activeReleaseLink"])
    active_link_target = ""
    if active_release_link.exists() or active_release_link.is_symlink():
        if not active_release_link.is_symlink():
            _fail("production_state_changed", "active release link is not a symlink")
        active_link_target = str(active_release_link.resolve(strict=True))
        if Path(active_link_target) != active_root:
            _fail("production_state_changed", "active release link changed")

    nginx_path = Path(runtime["nginxConfig"])
    nginx_raw = _verify_file_identity(
        nginx_path,
        expected_bytes=runtime["nginxConfigBytes"],
        expected_sha256=runtime["nginxConfigSha256"],
        category="nginx_route_invalid",
        maximum_bytes=128 * 1024,
        required_uid=TRUSTED_SYSTEM_UID,
        forbid_group_world_write=True,
    )
    nginx = nginx_raw.decode("utf-8")
    backend_matches = re.findall(
        r"server 127\.0\.0\.1:(8000|8001)\b",
        nginx,
    )
    frontend_matches = re.findall(
        r"(?m)^[ \t]*root[ \t]+([^;]+);",
        nginx,
    )
    if backend_matches != [active_slot] or frontend_matches != [
        expected["activeFrontendRoot"],
        expected["activeFrontendRoot"],
    ]:
        _fail("nginx_route_invalid", "Nginx route is not the reviewed active route")
    if identity_commit := plan["checkpoint"]["identity"]["commit"]:
        if identity_commit in nginx or str(old_release_root) in nginx:
            _fail("nginx_route_invalid", "failed release appears in active Nginx route")
    canonical_nginx_identity: Mapping[str, Any] | None = None
    if plan_version == 3:
        canonical_nginx = _retained_evidence_items(plan)[
            "canonical_nginx_config"
        ]
        if Path(runtime["nginxCanonicalConfig"]) != Path(canonical_nginx["path"]):
            _fail("nginx_route_invalid", "canonical Nginx path differs from plan")
        canonical_nginx_identity = _verify_residue_identity(
            Path(runtime["nginxCanonicalConfig"]),
            canonical_nginx,
            "retained_evidence_changed",
        )

    active_unit = _systemd_properties(
        f"{runtime['backendServicePrefix']}{active_slot}"
    )
    candidate_unit = _systemd_properties(
        f"{runtime['backendServicePrefix']}{candidate_slot}"
    )
    monthly_unit = _systemd_properties(runtime["monthlyWorkerUnit"])
    switch_unit = _systemd_properties(runtime["switchUnit"])
    listener_output = _run_text(
        ["ss", "-H", "-ltn", f"sport = :{candidate_slot}"],
        category="candidate_probe_failed",
    )
    candidate_listener = bool(listener_output)
    try:
        active_restarts = int(active_unit["NRestarts"])
        active_exec_start = int(
            active_unit["ExecMainStartTimestampMonotonic"]
        )
        active_enter = int(active_unit["ActiveEnterTimestampMonotonic"])
    except (KeyError, ValueError) as exc:
        raise RecoveryError(
            "active_runtime_invalid",
            "active backend lifetime proof is invalid",
        ) from exc
    if (
        active_unit["InvocationID"] != expected["activeBackendInvocationId"]
        or active_unit["ControlGroup"] != expected["activeBackendControlGroup"]
        or active_restarts != expected["activeBackendNRestarts"]
        or active_exec_start
        != expected["activeBackendExecMainStartTimestampMonotonic"]
        or active_enter
        != expected["activeBackendActiveEnterTimestampMonotonic"]
    ):
        _fail(
            "active_runtime_invalid",
            "active backend invocation differs from the reviewed legacy process",
        )
    active_processes = _active_worker_proof(
        active_unit,
        expected_workers=expected["workers"],
        expected_slot=active_slot,
    )

    slot_env = _read_secure_text(
        Path(runtime["slotEnv"]),
        "active_runtime_invalid",
    )
    expected_slot_values = {
        "APP_BACKEND_WORKERS": str(expected["workers"]),
        "APP_RELEASE_SHA": active_commit,
        "APP_RELEASE_SLOT": active_slot,
    }
    for key, expected_value in expected_slot_values.items():
        values = [
            line.split("=", 1)[1]
            for line in slot_env.splitlines()
            if line.startswith(f"{key}=")
        ]
        if values != [expected_value]:
            _fail("active_runtime_invalid", f"{key} configuration changed")

    build_meta_path = Path(expected["activeFrontendRoot"]) / "build-meta.json"
    build_meta_raw = _verify_file_identity(
        build_meta_path,
        expected_bytes=expected["activeBuildMetaBytes"],
        expected_sha256=expected["activeBuildMetaSha256"],
        category="active_build_meta_invalid",
        maximum_bytes=MAX_JSON_BYTES,
    )
    try:
        build_meta = json.loads(build_meta_raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RecoveryError(
            "active_build_meta_invalid",
            "active build metadata is invalid JSON",
        ) from exc
    if not isinstance(build_meta, dict):
        _fail("active_build_meta_invalid", "active build metadata is not an object")
    _validate_build_meta(
        build_meta,
        expected_commit=active_commit,
        category="active_build_meta_invalid",
    )

    local_health_payload = _parse_json_output(
        _run_text(
            [
                "curl",
                "--noproxy",
                "*",
                "--fail",
                "--silent",
                "--show-error",
                "--max-time",
                "20",
                f"http://127.0.0.1:{active_slot}/healthz",
            ],
            category="health_probe_failed",
        ),
        "health_probe_failed",
    )
    if local_health_payload != {"status": "ok"}:
        _fail("health_probe_failed", "local legacy healthz payload changed")
    local_health = {
        "backendHealthStatus": "ok",
        "frontendBuildCommit": active_commit,
        "frontendBuildMetaSha256": expected["activeBuildMetaSha256"],
    }
    public_health: dict[str, Any] = {}
    for server_name in runtime["serverNames"]:
        public_health[server_name] = {
            "origin": _probe_legacy_health_and_build(
                health_url=f"https://{server_name}/healthz",
                build_url=f"https://{server_name}/build-meta.json",
                expected_build=build_meta,
                expected_build_bytes=expected["activeBuildMetaBytes"],
                expected_build_sha256=expected["activeBuildMetaSha256"],
                expected_commit=active_commit,
                resolve_host=server_name,
            ),
            "external": _probe_legacy_health_and_build(
                health_url=f"https://{server_name}/healthz",
                build_url=f"https://{server_name}/build-meta.json",
                expected_build=build_meta,
                expected_build_bytes=expected["activeBuildMetaBytes"],
                expected_build_sha256=expected["activeBuildMetaSha256"],
                expected_commit=active_commit,
            ),
        }

    database_proof = _collect_database_proof(plan, bundle_root)

    marker_exists = Path(runtime["deploymentMarker"]).exists() or Path(
        runtime["deploymentMarker"]
    ).is_symlink()
    scheduler_exists = Path(runtime["schedulerState"]).exists() or Path(
        runtime["schedulerState"]
    ).is_symlink()
    if scheduler_exists or (plan_version != 3 and marker_exists):
        _fail("production_state_changed", "deployment marker/scheduler state is invalid")
    if (
        active_unit["LoadState"] != "loaded"
        or active_unit["ActiveState"] != "active"
        or active_unit["MemoryHigh"] != str(expected["memoryHighBytes"])
        or active_unit["MemoryMax"] != str(expected["memoryMaxBytes"])
    ):
        _fail("active_runtime_invalid", "active backend runtime changed")
    candidate_lifetime = None
    if plan_version == 3:
        assert residue_state is not None
        candidate_lifetime = _candidate_never_started_proof(
            plan,
            candidate_unit,
            stage=str(residue_state["stage"]),
            listener=candidate_listener,
        )
    elif (
        candidate_unit["LoadState"] != "loaded"
        or candidate_unit["ActiveState"] != "inactive"
        or candidate_unit["UnitFileState"] != "disabled"
        or candidate_listener
        or candidate_slot_link_exists
    ):
        _fail("candidate_present", "Candidate slot is active or materialized")
    if (
        monthly_unit["LoadState"] != "loaded"
        or monthly_unit["ActiveState"] != "inactive"
        or monthly_unit["UnitFileState"] != "disabled"
    ):
        _fail("monthly_worker_enabled", "JATO monthly worker is not disabled")
    if (
        switch_unit["LoadState"] != "not-found"
        or switch_unit["ActiveState"] != "inactive"
        or switch_unit["UnitFileState"] != ""
    ):
        _fail("switch_in_progress", "blue/green switch unit is not quiescent")

    observation = {
        "active": {
            "frontendCommit": active_commit,
            "releaseRoot": str(active_root),
            "slot": active_slot,
            "slotLink": str(active_slot_link),
            "activeReleaseLinkTarget": active_link_target,
            "activeSlotFileSha256": expected["activeSlotFileSha256"],
            "slotEnvReleaseSha": active_commit,
            "slotEnvReleaseSlot": active_slot,
            "backendInvocationId": active_unit["InvocationID"],
            "backendExecMainStartTimestampMonotonic": active_exec_start,
            "backendActiveEnterTimestampMonotonic": active_enter,
            "backendNRestarts": active_restarts,
            "backendProcesses": active_processes,
            "workers": active_processes["workerProcesses"],
            "memoryHighBytes": int(active_unit["MemoryHigh"]),
            "memoryMaxBytes": int(active_unit["MemoryMax"]),
            "health": local_health,
        },
        "public": public_health,
        "candidate": {
            "slot": candidate_slot,
            "unitActive": False,
            "unitEnabled": False,
            "listener": False,
            "slotLinkExists": candidate_slot_link_exists,
            "targetReleaseActive": False,
            "nginxReferencesTarget": False,
            **({"lifetime": candidate_lifetime} if candidate_lifetime else {}),
        },
        "database": database_proof,
        "runtime": {
            "nginxBackendSlot": active_slot,
            "nginxConfigSha256": runtime["nginxConfigSha256"],
            "nginxFrontendRoot": expected["activeFrontendRoot"],
            "nginxMode": runtime["nginxMode"],
            **(
                {
                    "nginxCanonicalConfig": runtime["nginxCanonicalConfig"],
                    "nginxCanonicalConfigIdentity": canonical_nginx_identity,
                }
                if canonical_nginx_identity is not None
                else {}
            ),
            "monthlyWorkerActive": False,
            "monthlyWorkerEnabled": False,
            "switchUnitLoadState": switch_unit["LoadState"],
            "switchUnitActiveState": switch_unit["ActiveState"],
            "maintenanceMarkerPresent": marker_exists,
            "schedulerSnapshotPresent": False,
        },
        **({"residue": residue_state} if residue_state is not None else {}),
    }
    validate_observation(plan, observation)
    return observation


def validate_observation(
    plan: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> None:
    expected = plan["expected"]
    active = observation.get("active")
    public = observation.get("public")
    candidate = observation.get("candidate")
    database = observation.get("database")
    runtime = observation.get("runtime")
    plan_version = _plan_schema_version(plan)
    residue = observation.get("residue")
    if not all(
        isinstance(value, dict)
        for value in (active, public, candidate, database, runtime)
    ):
        _fail("observation_invalid", "runtime proof objects are missing")
    if plan_version == 3 and not isinstance(residue, dict):
        _fail("observation_invalid", "schema v3 residue proof is missing")
    backend_processes = active.get("backendProcesses")
    if (
        active.get("frontendCommit") != expected["activeCommit"]
        or active.get("releaseRoot") != expected["activeReleaseRoot"]
        or active.get("slot") != expected["activeSlot"]
        or active.get("activeSlotFileSha256")
        != expected["activeSlotFileSha256"]
        or active.get("slotEnvReleaseSha") != expected["activeCommit"]
        or active.get("slotEnvReleaseSlot") != expected["activeSlot"]
        or active.get("backendInvocationId")
        != expected["activeBackendInvocationId"]
        or active.get("backendExecMainStartTimestampMonotonic")
        != expected["activeBackendExecMainStartTimestampMonotonic"]
        or active.get("backendActiveEnterTimestampMonotonic")
        != expected["activeBackendActiveEnterTimestampMonotonic"]
        or active.get("backendNRestarts")
        != expected["activeBackendNRestarts"]
        or not isinstance(backend_processes, dict)
        or not isinstance(backend_processes.get("mainPid"), int)
        or backend_processes.get("mainPid", 0) <= 0
        or backend_processes.get("masterWorkersArgument")
        != expected["workers"]
        or backend_processes.get("workerProcesses") != expected["workers"]
        or backend_processes.get("resourceTrackerProcesses") != 1
        or active.get("workers") != expected["workers"]
        or active.get("memoryHighBytes") != expected["memoryHighBytes"]
        or active.get("memoryMaxBytes") != expected["memoryMaxBytes"]
        or active.get("health")
        != {
            "backendHealthStatus": "ok",
            "frontendBuildCommit": expected["activeCommit"],
            "frontendBuildMetaSha256": expected["activeBuildMetaSha256"],
        }
    ):
        _fail("active_runtime_invalid", "active proof differs from plan")
    if set(public) != set(plan["runtime"]["serverNames"]):
        _fail("public_identity_invalid", "public host set differs from plan")
    for proof in public.values():
        if not isinstance(proof, dict) or set(proof) != {"origin", "external"}:
            _fail("public_identity_invalid", "public host proof is incomplete")
        for route_proof in proof.values():
            if (
                not isinstance(route_proof, dict)
                or route_proof.get("healthStatus") != "ok"
                or route_proof.get("frontendBuildCommit")
                != expected["activeCommit"]
                or route_proof.get("frontendBuildMetaSha256")
                != expected["activeBuildMetaSha256"]
            ):
                _fail(
                    "public_identity_invalid",
                    "public host does not bind active frontend SHA",
                )
    candidate_false_fields = [
        "unitActive",
        "unitEnabled",
        "listener",
        "targetReleaseActive",
        "nginxReferencesTarget",
    ]
    if plan_version != 3:
        candidate_false_fields.append("slotLinkExists")
    if any(
        candidate.get(field) is not False
        for field in candidate_false_fields
    ):
        _fail("candidate_present", "Candidate proof is not fully inactive")
    if plan_version == 3:
        assert isinstance(residue, dict)
        stage = residue.get("stage")
        if (
            stage
            not in {
                "initial",
                "quarantine_root_created",
                "partial",
                "quarantined_fenced",
                "finalized",
            }
            or residue.get("inventoryDigest") != _residue_inventory_digest(plan)
            or candidate.get("slotLinkExists")
            is not residue.get("candidateSlotLinkExists")
            or runtime.get("maintenanceMarkerPresent")
            is not residue.get("maintenanceMarkerPresent")
            or not isinstance(candidate.get("lifetime"), dict)
        ):
            _fail("residue_observation_invalid", "schema v3 residue proof changed")
    expected_revisions = sorted(expected["revisions"])
    if (
        database.get("enabled") is not True
        or database.get("mode") != "read_only"
        or database.get("pgoptions") != "default_transaction_read_only=on"
        or database.get("transactionReadOnly") != "on"
        or database.get("equal") is not True
        or any(
            database.get(field) != expected_revisions
            for field in (
                "currentRevisions",
                "oldHeadRevisions",
                "newHeadRevisions",
                "backupRevisions",
            )
        )
    ):
        _fail("database_revision_mismatch", "database proof is not an equal read-only set")
    if (
        runtime.get("nginxBackendSlot") != expected["activeSlot"]
        or runtime.get("nginxConfigSha256")
        != plan["runtime"]["nginxConfigSha256"]
        or runtime.get("nginxFrontendRoot") != expected["activeFrontendRoot"]
        or runtime.get("nginxMode") != "legacy_pre_candidate"
        or runtime.get("monthlyWorkerActive") is not False
        or runtime.get("monthlyWorkerEnabled") is not False
        or runtime.get("switchUnitLoadState") != "not-found"
        or runtime.get("switchUnitActiveState") != "inactive"
        or (
            plan_version != 3
            and runtime.get("maintenanceMarkerPresent") is not False
        )
        or runtime.get("schedulerSnapshotPresent") is not False
    ):
        _fail("production_state_changed", "runtime invariant proof changed")
    if plan_version == 3:
        canonical_nginx = _retained_evidence_items(plan)[
            "canonical_nginx_config"
        ]
        if (
            runtime.get("nginxCanonicalConfig")
            != plan["runtime"]["nginxCanonicalConfig"]
            or runtime.get("nginxCanonicalConfigIdentity")
            != _expected_identity(canonical_nginx)
        ):
            _fail(
                "production_state_changed",
                "canonical Nginx invariant proof changed",
            )


def _parent_pid(pid: int) -> int:
    raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    closing = raw.rfind(")")
    if closing < 0:
        _fail("lock_invalid", "cannot parse process ancestry")
    fields = raw[closing + 2 :].split()
    if len(fields) < 2:
        _fail("lock_invalid", "cannot parse parent PID")
    return int(fields[1])


def assert_production_lock(
    *,
    lock_path: Path,
    holder_pid: int,
    expected_lock_path: Path,
) -> Mapping[str, Any]:
    if lock_path != expected_lock_path:
        _fail("lock_invalid", "lock path is not canonical")
    if holder_pid <= 0:
        _fail("lock_invalid", "holder PID is invalid")
    current = os.getpid()
    ancestors: set[int] = set()
    while current > 0 and current not in ancestors:
        ancestors.add(current)
        if current == holder_pid:
            break
        current = _parent_pid(current)
    else:
        _fail("lock_invalid", "holder PID is not an ancestor")
    holder_fd = Path(f"/proc/{holder_pid}/fd/9")
    try:
        holder_target = Path(os.readlink(holder_fd))
    except OSError as exc:
        raise RecoveryError("lock_invalid", "holder fd 9 is unavailable") from exc
    if holder_target != lock_path.resolve(strict=False):
        _fail("lock_invalid", "holder fd 9 references a different file")
    descriptor = os.open(
        lock_path,
        os.O_WRONLY | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            _fail("lock_invalid", "canonical lock is not held")
        metadata = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    return {
        "path": str(lock_path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "holderPid": holder_pid,
    }


def _load_dry_run_authorization(
    *,
    path: Path,
    expected_sha256: str,
    bundle_root: Path,
    plan: Mapping[str, Any],
    plan_sha256: str,
    implementation_commit: str,
) -> tuple[Mapping[str, Any], str]:
    expected_sha256 = _require_sha256(
        expected_sha256,
        "dry_run_authorization_invalid",
    )
    absolute = Path(os.path.abspath(path))
    root = Path(os.path.abspath(bundle_root))
    try:
        absolute.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise RecoveryError(
            "dry_run_authorization_invalid",
            "authorization must remain inside the immutable control bundle",
        ) from exc
    payload, raw, _ = _read_json(
        absolute,
        "dry_run_authorization_invalid",
    )
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    if actual_sha256 != expected_sha256:
        _fail("dry_run_authorization_invalid", "authorization SHA-256 changed")
    authorization = _require_mapping(
        payload,
        DRY_RUN_AUTHORIZATION_FIELDS,
        "dry_run_authorization_invalid",
    )
    if (
        authorization.get("schemaVersion") != 1
        or authorization.get("kind") != "checkpoint_recovery_dry_run_authorization"
        or authorization.get("repository") != plan["repository"]
        or authorization.get("workflowPath")
        != ".github/workflows/production-checkpoint-recovery.yml"
        or authorization.get("mainSha") != implementation_commit
        or authorization.get("planSha256") != plan_sha256
        or authorization.get("incidentId") != plan["incidentId"]
        or authorization.get("inventoryDigest") != _residue_inventory_digest(plan)
        or authorization.get("decision")
        != "candidate-residue-dry-run-eligible"
    ):
        _fail("dry_run_authorization_invalid", "authorization identity changed")
    for field in ("runId", "runAttempt"):
        _require_positive_int(authorization.get(field), "dry_run_authorization_invalid")
    _require_sha256(
        authorization.get("resultSha256"),
        "dry_run_authorization_invalid",
    )
    return authorization, actual_sha256


def _bundled_dry_run_authorization(
    *,
    bundle_root: Path,
    plan_sha256: str,
    implementation_commit: str,
) -> tuple[Path, str]:
    manifest, _, _ = _read_json(
        bundle_root / "recovery-control-manifest.json",
        "dry_run_authorization_invalid",
    )
    files = manifest.get("files")
    relative = "reviewed-dry-run-authorization.json"
    if (
        manifest.get("schemaVersion") != 1
        or manifest.get("commit") != implementation_commit
        or manifest.get("planSha256") != plan_sha256
        or not isinstance(files, dict)
        or set(path for path in files if path == relative) != {relative}
        or not SHA256_PATTERN.fullmatch(str(files.get(relative) or ""))
    ):
        _fail(
            "dry_run_authorization_invalid",
            "immutable control manifest does not bind the authorization",
        )
    return bundle_root / relative, str(files[relative])


def _quarantine_contract_payload(
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    implementation_commit: str,
    authorization: Mapping[str, Any],
    authorization_sha256: str,
) -> Mapping[str, Any]:
    residue = plan["residue"]
    return {
        "schemaVersion": 1,
        "kind": "candidate_residue_quarantine_contract",
        "incidentId": plan["incidentId"],
        "identity": plan["checkpoint"]["identity"],
        "implementation": {
            "commit": implementation_commit,
            "planSha256": plan_sha256,
        },
        "authorization": {
            **authorization,
            "authorizationSha256": authorization_sha256,
        },
        "quarantineRoot": residue["quarantineRoot"],
        "items": residue["items"],
        "retainedEvidence": residue["retainedEvidence"],
        "requiredAbsentPaths": residue["requiredAbsentPaths"],
        "fence": {
            "content": residue["fenceContent"] + "\n",
            "sha256": hashlib.sha256(
                (residue["fenceContent"] + "\n").encode("utf-8")
            ).hexdigest(),
            "livePath": str(RESIDUE_PATHS["maintenance_marker"]),
            "finalPath": str(
                Path(residue["quarantineRoot"]) / residue["finalFenceName"]
            ),
        },
    }


def _write_immutable_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    owner_uid: int,
    owner_gid: int,
    category: str,
    ignore_created_at: bool = False,
) -> tuple[str, bool]:
    raw, digest = _json_sha256(payload)
    if _path_lexists(path):
        existing, metadata = _read_regular_bytes(path, category)
        if (
            (metadata.st_uid, metadata.st_gid) != (owner_uid, owner_gid)
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            _fail(category, "existing immutable document owner/mode changed")
        try:
            existing_payload = json.loads(existing.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise RecoveryError(category, "existing document is invalid JSON") from exc
        expected_compare = dict(payload)
        existing_compare = (
            dict(existing_payload) if isinstance(existing_payload, dict) else {}
        )
        if ignore_created_at:
            expected_compare.pop("createdAt", None)
            existing_compare.pop("createdAt", None)
        if existing_compare != expected_compare:
            _fail(category, "existing immutable document has different facts")
        return hashlib.sha256(existing).hexdigest(), True
    atomic_write_json(
        path,
        payload,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    return digest, False


def _ensure_quarantine_contract(
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    implementation_commit: str,
    authorization: Mapping[str, Any],
    authorization_sha256: str,
) -> tuple[Path, str]:
    residue = plan["residue"]
    root = Path(residue["quarantineRoot"])
    if _path_lexists(root):
        _verify_quarantine_root(root, plan)
    else:
        ensure_private_state_directory(
            root,
            owner_uid=residue["quarantineOwnerUid"],
            owner_gid=residue["quarantineOwnerGid"],
        )
    _verify_quarantine_root(root, plan)
    manifest_path = root / residue["manifestName"]
    payload = _quarantine_contract_payload(
        plan=plan,
        plan_sha256=plan_sha256,
        implementation_commit=implementation_commit,
        authorization=authorization,
        authorization_sha256=authorization_sha256,
    )
    manifest_sha256, _ = _write_immutable_json(
        manifest_path,
        payload,
        owner_uid=residue["quarantineOwnerUid"],
        owner_gid=residue["quarantineOwnerGid"],
        category="quarantine_manifest_invalid",
    )
    return manifest_path, manifest_sha256


def _trusted_directory_fd(path: Path, category: str) -> int:
    if path.resolve(strict=True) != path:
        _fail(category, f"directory traverses a symlink: {path}")
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        _fail(category, f"directory is unsafe: {path}")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    opened = os.fstat(descriptor)
    if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
        os.close(descriptor)
        _fail(category, f"directory changed while opening: {path}")
    return descriptor


def _renameat2(source: Path, destination: Path, flags: int) -> None:
    source_parent_fd = _trusted_directory_fd(source.parent, "quarantine_rename_failed")
    destination_parent_fd = _trusted_directory_fd(
        destination.parent,
        "quarantine_rename_failed",
    )
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            _fail("quarantine_rename_failed", "renameat2 is unavailable")
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        result = rename(
            source_parent_fd,
            os.fsencode(source.name),
            destination_parent_fd,
            os.fsencode(destination.name),
            flags,
        )
        if result != 0:
            error = ctypes.get_errno()
            raise RecoveryError(
                "quarantine_rename_failed",
                f"renameat2 failed with errno {error}",
            )
        os.fsync(source_parent_fd)
        if destination_parent_fd != source_parent_fd:
            os.fsync(destination_parent_fd)
    finally:
        os.close(source_parent_fd)
        os.close(destination_parent_fd)


def _verify_fence_publish_preconditions(
    *,
    path: Path,
    plan: Mapping[str, Any],
    manifest_path: Path,
    manifest_sha256: str,
) -> None:
    residue = plan["residue"]
    root = Path(residue["quarantineRoot"])
    marker = _residue_items(plan)["maintenance_marker"]
    expected_path = root / marker["quarantineName"]
    expected_manifest_path = root / residue["manifestName"]
    if path != expected_path or manifest_path != expected_manifest_path:
        _fail("recovery_fence_invalid", "recovery fence publish path changed")
    _verify_quarantine_root(root, plan)
    manifest_raw, manifest_metadata = _read_regular_bytes(
        manifest_path,
        "quarantine_manifest_invalid",
    )
    if (
        manifest_metadata.st_uid != residue["quarantineOwnerUid"]
        or manifest_metadata.st_gid != residue["quarantineOwnerGid"]
        or stat.S_IMODE(manifest_metadata.st_mode) != 0o600
        or manifest_metadata.st_nlink != 1
        or hashlib.sha256(manifest_raw).hexdigest() != manifest_sha256
    ):
        _fail(
            "quarantine_manifest_invalid",
            "recovery fence publish manifest binding changed",
        )
    _verify_residue_identity(
        Path(marker["path"]),
        marker,
        "recovery_fence_invalid",
    )
    final_fence_path = root / residue["finalFenceName"]
    if _path_lexists(path) or _path_lexists(final_fence_path):
        _fail(
            "recovery_fence_invalid",
            "legacy marker was exchanged before fence publication",
        )


def _create_recovery_fence(
    path: Path,
    plan: Mapping[str, Any],
    *,
    manifest_path: Path,
    manifest_sha256: str,
) -> None:
    temp_path = _recovery_fence_temp_path(path)
    if _path_lexists(path):
        if _path_lexists(temp_path):
            _fail(
                "recovery_fence_invalid",
                "published recovery fence has an unexpected temp sibling",
            )
        _verify_fence_identity(path, plan)
        return
    _verify_fence_publish_preconditions(
        path=path,
        plan=plan,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
    )
    parent_fd = _trusted_directory_fd(path.parent, "recovery_fence_invalid")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    raw = (plan["residue"]["fenceContent"] + "\n").encode("utf-8")
    descriptor = -1
    try:
        if _path_lexists(temp_path):
            _verify_replayable_fence_temp(temp_path, plan)
            os.unlink(temp_path.name, dir_fd=parent_fd)
            os.fsync(parent_fd)
        descriptor = os.open(temp_path.name, flags, 0o600, dir_fd=parent_fd)
        os.fchown(
            descriptor,
            plan["residue"]["quarantineOwnerUid"],
            plan["residue"]["quarantineOwnerGid"],
        )
        os.fchmod(descriptor, 0o644)
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise OSError("recovery fence write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_fd)
    _verify_replayable_fence_temp(temp_path, plan)
    _verify_fence_publish_preconditions(
        path=path,
        plan=plan,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
    )
    _renameat2(temp_path, path, RENAME_NOREPLACE)
    _verify_fence_identity(path, plan)


def _quarantine_candidate_residue(
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    implementation_commit: str,
    authorization: Mapping[str, Any],
    authorization_sha256: str,
) -> Mapping[str, Any]:
    manifest_path, manifest_sha256 = _ensure_quarantine_contract(
        plan=plan,
        plan_sha256=plan_sha256,
        implementation_commit=implementation_commit,
        authorization=authorization,
        authorization_sha256=authorization_sha256,
    )
    root = Path(plan["residue"]["quarantineRoot"])
    items = _residue_items(plan)
    marker = items["maintenance_marker"]
    marker_source = Path(marker["path"])
    marker_destination = root / marker["quarantineName"]
    if not _path_lexists(marker_destination):
        _verify_residue_identity(marker_source, marker)
        _create_recovery_fence(
            marker_destination,
            plan,
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha256,
        )
    source_identity = _stable_path_identity(marker_source, "recovery_fence_invalid")
    if source_identity == _expected_identity(marker):
        _verify_fence_identity(marker_destination, plan)
        _renameat2(marker_source, marker_destination, RENAME_EXCHANGE)
    _verify_fence_identity(marker_source, plan)
    _verify_residue_identity(marker_destination, marker)

    for item_id, item in items.items():
        if item_id == "maintenance_marker":
            continue
        source = Path(item["path"])
        destination = root / item["quarantineName"]
        if _path_lexists(source):
            if _path_lexists(destination):
                _fail(
                    "quarantine_state_invalid",
                    f"both live and quarantine paths exist: {item_id}",
                )
            _verify_residue_identity(source, item)
            _renameat2(source, destination, RENAME_NOREPLACE)
        _verify_residue_identity(destination, item)

    _run_text(
        ["systemctl", "daemon-reload"],
        category="candidate_daemon_reload_failed",
    )
    _verify_candidate_detached_after_reload(plan)
    state = _collect_residue_state(plan, plan_sha256)
    if (
        state.get("stage") != "quarantined_fenced"
        or state.get("manifestSha256") != manifest_sha256
        or state.get("manifestPath") != str(manifest_path)
    ):
        _fail("quarantine_incomplete", "Candidate residue quarantine is incomplete")
    return state


def _finalization_receipt_path(
    plan: Mapping[str, Any],
    plan_sha256: str,
) -> Path:
    receipt_root, operation_path = _receipt_path(plan, plan_sha256)
    return receipt_root / operation_path.parent.name / f"{plan_sha256}.finalization.json"


def _build_finalization_receipt(
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    implementation_commit: str,
    quarantine_state: Mapping[str, Any],
) -> Mapping[str, Any]:
    fence_path = Path(plan["runtime"]["deploymentMarker"])
    fence_identity = _verify_fence_identity(fence_path, plan)
    return {
        "schemaVersion": 1,
        "kind": "pre_switch_abort_fence_finalization",
        "decision": "fence_finalization_authorized",
        "incidentId": plan["incidentId"],
        "identity": plan["checkpoint"]["identity"],
        "implementation": {
            "commit": implementation_commit,
            "planSha256": plan_sha256,
        },
        "quarantineManifest": {
            "path": quarantine_state["manifestPath"],
            "sha256": quarantine_state["manifestSha256"],
        },
        "fence": {
            "livePath": str(fence_path),
            "finalPath": quarantine_state["finalFencePath"],
            "identity": fence_identity,
        },
        "createdAt": dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    }


def _finalize_recovery_fence(
    plan: Mapping[str, Any],
    finalization_receipt: Mapping[str, Any],
) -> Mapping[str, Any]:
    fence = finalization_receipt.get("fence")
    if not isinstance(fence, dict):
        _fail("finalization_receipt_invalid", "finalization fence proof is missing")
    live_path = Path(str(fence.get("livePath") or ""))
    final_path = Path(str(fence.get("finalPath") or ""))
    expected_live = Path(plan["runtime"]["deploymentMarker"])
    expected_final = (
        Path(plan["residue"]["quarantineRoot"])
        / plan["residue"]["finalFenceName"]
    )
    if live_path != expected_live or final_path != expected_final:
        _fail("finalization_receipt_invalid", "finalization paths changed")
    if _path_lexists(live_path):
        if _path_lexists(final_path):
            _fail("finalization_state_invalid", "both live and final fences exist")
        actual = _verify_fence_identity(live_path, plan)
        if actual != fence.get("identity"):
            _fail("finalization_state_invalid", "live recovery fence identity changed")
        _renameat2(live_path, final_path, RENAME_NOREPLACE)
    final_identity = _verify_fence_identity(final_path, plan)
    if final_identity != fence.get("identity") or _path_lexists(live_path):
        _fail("finalization_state_invalid", "recovery fence finalization is incomplete")
    return final_identity


def _receipt_path(plan: Mapping[str, Any], plan_sha256: str) -> tuple[Path, Path]:
    checkpoint_path = Path(plan["checkpoint"]["path"])
    state_root = checkpoint_path.parents[2]
    receipt_root = state_root / "recoveries"
    receipt_path = receipt_root / plan["incidentId"] / f"{plan_sha256}.json"
    return receipt_root, receipt_path


def _build_receipt(
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    implementation_commit: str,
    observation: Mapping[str, Any],
    lock: Mapping[str, Any],
    authorization: Mapping[str, Any] | None = None,
    authorization_sha256: str | None = None,
    quarantine_state: Mapping[str, Any] | None = None,
    finalization_path: Path | None = None,
    finalization_sha256: str | None = None,
) -> Mapping[str, Any]:
    checkpoint, checkpoint_raw, _ = _read_checkpoint_state(plan)
    receipt: dict[str, Any] = {
        "schemaVersion": _receipt_schema_version_for_plan(plan),
        "kind": "pre_switch_abort",
        "decision": "pre_switch_abort_verified",
        "incidentId": plan["incidentId"],
        "identity": plan["checkpoint"]["identity"],
        "implementation": {
            "commit": implementation_commit,
            "planSha256": plan_sha256,
        },
        "sourceCheckpoint": {
            "path": plan["checkpoint"]["path"],
            "sha256": hashlib.sha256(checkpoint_raw).hexdigest(),
            "sequence": checkpoint["sequence"],
            "phase": checkpoint["phase"],
            "status": checkpoint["status"],
            "retryClass": checkpoint["retryClass"],
        },
        "legacyEvidence": {
            "path": plan["checkpoint"]["evidencePath"],
            "sha256": plan["checkpoint"]["evidenceSha256"],
            "migrationStatus": _migration_status_for_plan(plan),
        },
        "journal": {
            "path": plan["checkpoint"]["journalPath"],
            "sha256": plan["checkpoint"]["journalSha256"],
            "lastSequence": checkpoint["sequence"],
            "switchPhaseSeen": False,
        },
        "backup": {
            "manifestPath": plan["backup"]["manifestPath"],
            "manifestSha256": plan["backup"]["manifestSha256"],
            "dumpPath": plan["backup"]["dumpPath"],
            "dumpSha256": plan["backup"]["dumpSha256"],
            "databaseEnabled": True,
            "status": "completed",
        },
        "database": observation["database"],
        "production": {
            "active": observation["active"],
            "public": observation["public"],
            "runtime": observation["runtime"],
        },
        "candidate": observation["candidate"],
        "lock": lock,
        "createdAt": dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    }
    if _plan_schema_version(plan) == 3:
        if (
            authorization is None
            or authorization_sha256 is None
            or quarantine_state is None
            or finalization_path is None
            or finalization_sha256 is None
        ):
            _fail("recovery_receipt_invalid", "schema v3 receipt proof is incomplete")
        root = Path(plan["residue"]["quarantineRoot"])
        receipt["authorization"] = {
            **authorization,
            "authorizationSha256": authorization_sha256,
        }
        receipt["residue"] = {
            "profile": plan["residue"]["profile"],
            "inventoryDigest": quarantine_state["inventoryDigest"],
            "quarantineRoot": str(root),
            "manifestPath": quarantine_state["manifestPath"],
            "manifestSha256": quarantine_state["manifestSha256"],
            "items": [
                {
                    "id": item["id"],
                    "sourcePath": item["path"],
                    "quarantinePath": str(root / item["quarantineName"]),
                    "identity": _expected_identity(item),
                }
                for item in plan["residue"]["items"]
            ],
            "retainedEvidence": [
                {
                    "id": item["id"],
                    "path": item["path"],
                    "identity": _expected_identity(item),
                }
                for item in plan["residue"]["retainedEvidence"]
            ],
            "requiredAbsentPaths": plan["residue"]["requiredAbsentPaths"],
            "recoveryFencePresentAtSeal": True,
        }
        receipt["finalizationReceipt"] = {
            "path": str(finalization_path),
            "sha256": finalization_sha256,
        }
    return receipt


def _write_receipt(
    path: Path,
    payload: Mapping[str, Any],
    *,
    owner_uid: int,
    owner_gid: int,
) -> tuple[str, bool]:
    raw, digest = _json_sha256(payload)
    receipt_root = path.parents[1]
    ensure_private_state_directory(
        receipt_root,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    ensure_private_state_directory(
        path.parent,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    if path.exists() or path.is_symlink():
        existing, metadata = _read_regular_bytes(
            path,
            "recovery_receipt_invalid",
            maximum_bytes=MAX_JSON_BYTES,
        )
        if (
            (metadata.st_uid, metadata.st_gid) != (owner_uid, owner_gid)
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            _fail(
                "recovery_receipt_invalid",
                "existing receipt owner/mode differs from checkpoint",
            )
        try:
            existing_payload = json.loads(existing.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise RecoveryError(
                "recovery_receipt_invalid",
                "existing receipt is not valid JSON",
            ) from exc
        expected_without_time = dict(payload)
        existing_without_time = (
            dict(existing_payload) if isinstance(existing_payload, dict) else {}
        )
        expected_without_time.pop("createdAt", None)
        existing_without_time.pop("createdAt", None)
        for candidate in (expected_without_time, existing_without_time):
            lock = candidate.get("lock")
            if isinstance(lock, dict):
                candidate["lock"] = {
                    key: value
                    for key, value in lock.items()
                    if key != "holderPid"
                }
        if existing_without_time != expected_without_time:
            _fail(
                "recovery_receipt_conflict",
                "an existing receipt has different verified facts",
            )
        return hashlib.sha256(existing).hexdigest(), True
    atomic_write_json(
        path,
        payload,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    return digest, False


def _load_bound_terminal_receipt(
    plan: Mapping[str, Any],
    plan_sha256: str,
    checkpoint: Mapping[str, Any],
) -> Mapping[str, Any]:
    bindings = PRE_SWITCH_RECOVERY_BINDING_PATTERN.findall(
        str(checkpoint.get("message") or "")
    )
    if len(bindings) != 1:
        _fail("recovery_receipt_invalid", "terminal checkpoint binding is invalid")
    receipt_path = Path(bindings[0][0])
    _, expected_path = _receipt_path(plan, plan_sha256)
    if receipt_path != expected_path:
        _fail(
            "recovery_receipt_invalid",
            "terminal receipt path differs from the reviewed incident plan",
        )
    expected_digest = bindings[0][1]
    payload, raw, _ = _read_json(receipt_path, "recovery_receipt_invalid")
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        _fail("recovery_receipt_invalid", "terminal receipt digest changed")
    implementation = payload.get("implementation")
    legacy_evidence = payload.get("legacyEvidence")
    if (
        payload.get("schemaVersion")
        != _receipt_schema_version_for_plan(plan)
        or payload.get("decision") != "pre_switch_abort_verified"
        or payload.get("identity") != plan["checkpoint"]["identity"]
        or payload.get("incidentId") != plan["incidentId"]
        or not isinstance(legacy_evidence, dict)
        or legacy_evidence.get("migrationStatus")
        != _migration_status_for_plan(plan)
        or not isinstance(implementation, dict)
        or set(implementation) != {"commit", "planSha256"}
        or implementation.get("planSha256") != plan_sha256
        or not GIT_SHA_PATTERN.fullmatch(
            str(implementation.get("commit") or "")
        )
    ):
        _fail("recovery_receipt_invalid", "terminal receipt identity changed")
    return payload


def _load_finalization_receipt(
    plan: Mapping[str, Any],
    plan_sha256: str,
    operation_receipt: Mapping[str, Any],
) -> Mapping[str, Any]:
    binding = operation_receipt.get("finalizationReceipt")
    if not isinstance(binding, dict) or set(binding) != {"path", "sha256"}:
        _fail("finalization_receipt_invalid", "finalization binding is missing")
    expected_path = _finalization_receipt_path(plan, plan_sha256)
    path = Path(str(binding.get("path") or ""))
    digest = _require_sha256(
        binding.get("sha256"),
        "finalization_receipt_invalid",
    )
    if path != expected_path:
        _fail("finalization_receipt_invalid", "finalization receipt path changed")
    payload, raw, _ = _read_json(path, "finalization_receipt_invalid")
    implementation = payload.get("implementation")
    if (
        hashlib.sha256(raw).hexdigest() != digest
        or payload.get("schemaVersion") != 1
        or payload.get("kind") != "pre_switch_abort_fence_finalization"
        or payload.get("decision") != "fence_finalization_authorized"
        or payload.get("incidentId") != plan["incidentId"]
        or payload.get("identity") != plan["checkpoint"]["identity"]
        or not isinstance(implementation, dict)
        or implementation.get("planSha256") != plan_sha256
        or not GIT_SHA_PATTERN.fullmatch(str(implementation.get("commit") or ""))
    ):
        _fail("finalization_receipt_invalid", "finalization receipt identity changed")
    return payload


def _production_invariant_snapshot(observation: Mapping[str, Any]) -> Mapping[str, Any]:
    runtime = dict(observation["runtime"])
    runtime.pop("maintenanceMarkerPresent", None)
    return {
        "active": observation["active"],
        "public": observation["public"],
        "database": observation["database"],
        "runtime": runtime,
    }


def recover(
    *,
    plan_path: Path,
    expected_plan_sha256: str,
    bundle_root: Path,
    implementation_commit: str,
    lock_path: Path,
    lock_holder_pid: int,
    mode: str,
    dry_run_authorization_path: Path | None = None,
    dry_run_authorization_sha256: str | None = None,
) -> Mapping[str, Any]:
    if mode not in {"dry-run", "apply"}:
        _fail("mode_invalid", "mode must be dry-run or apply")
    implementation_commit = _require_git_sha(
        implementation_commit,
        "implementation_invalid",
    )
    plan, plan_sha256 = load_recovery_plan(
        plan_path,
        expected_plan_sha256,
    )
    plan_version = _plan_schema_version(plan)
    authorization: Mapping[str, Any] | None = None
    authorization_sha256: str | None = None
    if plan_version == 3 and mode == "apply":
        if dry_run_authorization_path is None or dry_run_authorization_sha256 is None:
            if (
                dry_run_authorization_path is not None
                or dry_run_authorization_sha256 is not None
            ):
                _fail(
                    "dry_run_authorization_invalid",
                    "authorization path and SHA-256 must be supplied together",
                )
            (
                dry_run_authorization_path,
                dry_run_authorization_sha256,
            ) = _bundled_dry_run_authorization(
                bundle_root=bundle_root,
                plan_sha256=plan_sha256,
                implementation_commit=implementation_commit,
            )
        authorization, authorization_sha256 = _load_dry_run_authorization(
            path=dry_run_authorization_path,
            expected_sha256=dry_run_authorization_sha256,
            bundle_root=bundle_root,
            plan=plan,
            plan_sha256=plan_sha256,
            implementation_commit=implementation_commit,
        )
    elif dry_run_authorization_path is not None or dry_run_authorization_sha256 is not None:
        _fail(
            "dry_run_authorization_invalid",
            "reviewed dry-run authorization is accepted only by schema v3 apply",
        )
    checkpoint_path = Path(plan["checkpoint"]["path"])
    state_root = checkpoint_path.parents[2]
    expected_lock_path = state_root / "production-deploy.lock"
    lock = assert_production_lock(
        lock_path=lock_path,
        holder_pid=lock_holder_pid,
        expected_lock_path=expected_lock_path,
    )
    identity, checkpoint_metadata = _validate_legacy_checkpoint_chain(plan)
    checkpoint = load_checkpoint(checkpoint_path)
    if checkpoint["phase"] == PRE_SWITCH_ABORT_PHASE:
        _validate_backup_chain(plan)
        _validate_legacy_archive_and_source(plan)
        receipt = _load_bound_terminal_receipt(
            plan,
            plan_sha256,
            checkpoint,
        )
        if plan_version == 3:
            receipt_authorization = receipt.get("authorization")
            if (
                mode == "apply"
                and (
                    not isinstance(receipt_authorization, dict)
                    or authorization is None
                    or authorization_sha256 is None
                    or receipt_authorization
                    != {**authorization, "authorizationSha256": authorization_sha256}
                )
            ):
                _fail(
                    "dry_run_authorization_invalid",
                    "terminal recovery binds a different dry-run authorization",
                )
            finalization = _load_finalization_receipt(plan, plan_sha256, receipt)
            if _path_lexists(Path(plan["runtime"]["deploymentMarker"])):
                if mode != "apply":
                    _fail(
                        "fence_finalization_required",
                        "terminal checkpoint still has its recovery fence",
                    )
                _finalize_recovery_fence(plan, finalization)
        validate_pre_switch_abort_settlement(
            checkpoint_path=checkpoint_path,
            checkpoints_root=state_root / "checkpoints",
        )
        observation = collect_observation(plan, bundle_root, plan_sha256)
        validate_observation(plan, observation)
        return {
            "decision": "already-pre-switch-aborted",
            "incidentId": plan["incidentId"],
            "implementationCommit": implementation_commit,
            "planSha256": plan_sha256,
            "checkpointPhase": PRE_SWITCH_ABORT_PHASE,
            "receiptCreatedAt": receipt.get("createdAt"),
            "activeCommit": observation["active"]["frontendCommit"],
            "candidatePresent": False,
            "candidateStarted": False,
            "trafficChanged": False,
            "databaseChanged": False,
            "checkpointChanged": False,
            "mutationPerformed": False,
            "mode": mode,
        }

    _validate_backup_chain(plan)
    _validate_legacy_archive_and_source(plan)
    cross_release_state = assert_cross_release_safe(
        checkpoints_root=state_root / "checkpoints",
        current_checkpoint=checkpoint_path,
        expected_identity=identity,
    )
    observation = collect_observation(plan, bundle_root, plan_sha256)
    validate_observation(plan, observation)
    checkpoint_after, raw_after, metadata_after = _read_checkpoint_state(plan)
    if (
        checkpoint_after != checkpoint
        or hashlib.sha256(raw_after).hexdigest() != plan["checkpoint"]["sha256"]
        or (
            metadata_after.st_dev,
            metadata_after.st_ino,
            metadata_after.st_size,
            metadata_after.st_mtime_ns,
        )
        != (
            checkpoint_metadata.st_dev,
            checkpoint_metadata.st_ino,
            checkpoint_metadata.st_size,
            checkpoint_metadata.st_mtime_ns,
        )
    ):
        _fail("checkpoint_changed", "checkpoint changed during recovery inspection")

    result: dict[str, Any] = {
        "decision": "eligible",
        "incidentId": plan["incidentId"],
        "implementationCommit": implementation_commit,
        "planSha256": plan_sha256,
        "checkpointPhase": checkpoint["phase"],
        "databaseRevisions": observation["database"]["currentRevisions"],
        "activeCommit": observation["active"]["frontendCommit"],
        "candidatePresent": False,
        "candidateStarted": False,
        "trafficChanged": False,
        "databaseChanged": False,
        "checkpointChanged": False,
        "mutationPerformed": False,
        "mode": mode,
        "otherReleaseGate": cross_release_state["decision"],
    }
    if plan_version == 3:
        residue_state = observation["residue"]
        result.update(
            {
                "targetIdentity": plan["checkpoint"]["identity"],
                "inventoryDigest": residue_state["inventoryDigest"],
                "candidateResiduePresent": residue_state["stage"] != "finalized",
            }
        )
    if mode == "dry-run":
        if plan_version == 3:
            if observation["residue"]["stage"] != "initial":
                _fail(
                    "dry_run_not_eligible",
                    "schema v3 dry-run requires the exact untouched residue preimage",
                )
            result["decision"] = "candidate-residue-dry-run-eligible"
        else:
            result["decision"] = "dry-run-eligible"
        return result
    if plan_version == 3:
        assert authorization is not None
        assert authorization_sha256 is not None
        quarantine_state = _quarantine_candidate_residue(
            plan=plan,
            plan_sha256=plan_sha256,
            implementation_commit=implementation_commit,
            authorization=authorization,
            authorization_sha256=authorization_sha256,
        )
    else:
        quarantine_state = None
    final_observation = collect_observation(plan, bundle_root, plan_sha256)
    validate_observation(plan, final_observation)
    if (
        plan_version == 3
        and final_observation["candidate"]["lifetime"]["ownedSourceReferences"]
    ):
        _fail(
            "candidate_daemon_reload_failed",
            "Candidate sources were not detached before recovery settlement",
        )
    if (
        plan_version == 3
        and _production_invariant_snapshot(final_observation)
        != _production_invariant_snapshot(observation)
    ) or (plan_version != 3 and final_observation != observation):
        _fail(
            "production_changed_during_settlement",
            "runtime or database state changed before recovery settlement",
        )
    checkpoint_before_receipt, raw_before_receipt, metadata_before_receipt = (
        _read_checkpoint_state(plan)
    )
    if (
        checkpoint_before_receipt != checkpoint
        or hashlib.sha256(raw_before_receipt).hexdigest()
        != plan["checkpoint"]["sha256"]
        or (
            metadata_before_receipt.st_dev,
            metadata_before_receipt.st_ino,
            metadata_before_receipt.st_size,
            metadata_before_receipt.st_mtime_ns,
        )
        != (
            checkpoint_metadata.st_dev,
            checkpoint_metadata.st_ino,
            checkpoint_metadata.st_size,
            checkpoint_metadata.st_mtime_ns,
        )
    ):
        _fail("checkpoint_changed", "checkpoint changed before receipt write")

    receipt_root, receipt_path = _receipt_path(plan, plan_sha256)
    finalization_path: Path | None = None
    finalization_sha256: str | None = None
    finalization_reused = False
    if plan_version == 3:
        assert quarantine_state is not None
        finalization_path = _finalization_receipt_path(plan, plan_sha256)
        ensure_private_state_directory(
            finalization_path.parent,
            owner_uid=checkpoint_metadata.st_uid,
            owner_gid=checkpoint_metadata.st_gid,
        )
        finalization_payload = _build_finalization_receipt(
            plan=plan,
            plan_sha256=plan_sha256,
            implementation_commit=implementation_commit,
            quarantine_state=quarantine_state,
        )
        finalization_sha256, finalization_reused = _write_immutable_json(
            finalization_path,
            finalization_payload,
            owner_uid=checkpoint_metadata.st_uid,
            owner_gid=checkpoint_metadata.st_gid,
            category="finalization_receipt_invalid",
            ignore_created_at=True,
        )
    receipt = _build_receipt(
        plan=plan,
        plan_sha256=plan_sha256,
        implementation_commit=implementation_commit,
        observation=final_observation,
        lock=lock,
        authorization=authorization,
        authorization_sha256=authorization_sha256,
        quarantine_state=quarantine_state,
        finalization_path=finalization_path,
        finalization_sha256=finalization_sha256,
    )
    receipt_sha256, reused = _write_receipt(
        receipt_path,
        receipt,
        owner_uid=checkpoint_metadata.st_uid,
        owner_gid=checkpoint_metadata.st_gid,
    )
    seal_observation = collect_observation(plan, bundle_root, plan_sha256)
    validate_observation(plan, seal_observation)
    if seal_observation != final_observation:
        _fail(
            "production_changed_during_settlement",
            "runtime or database state changed before checkpoint seal",
        )
    checkpoint_before_seal, raw_before_seal, metadata_before_seal = (
        _read_checkpoint_state(plan)
    )
    if (
        checkpoint_before_seal != checkpoint
        or hashlib.sha256(raw_before_seal).hexdigest()
        != plan["checkpoint"]["sha256"]
        or (
            metadata_before_seal.st_dev,
            metadata_before_seal.st_ino,
            metadata_before_seal.st_size,
            metadata_before_seal.st_mtime_ns,
        )
        != (
            checkpoint_metadata.st_dev,
            checkpoint_metadata.st_ino,
            checkpoint_metadata.st_size,
            checkpoint_metadata.st_mtime_ns,
        )
    ):
        _fail("checkpoint_changed", "checkpoint changed before final seal")
    settled = seal_pre_switch_abort(
        checkpoint_path=checkpoint_path,
        journal_path=Path(plan["checkpoint"]["journalPath"]),
        identity=identity,
        receipt_path=receipt_path,
        receipt_sha256=receipt_sha256,
        receipt_root=receipt_root,
        owner_uid=checkpoint_metadata.st_uid,
        owner_gid=checkpoint_metadata.st_gid,
    )
    if plan_version == 3:
        finalization = _load_finalization_receipt(plan, plan_sha256, receipt)
        _finalize_recovery_fence(plan, finalization)
        finalized_observation = collect_observation(plan, bundle_root, plan_sha256)
        validate_observation(plan, finalized_observation)
        if (
            finalized_observation["residue"]["stage"] != "finalized"
            or _production_invariant_snapshot(finalized_observation)
            != _production_invariant_snapshot(observation)
        ):
            _fail(
                "fence_finalization_incomplete",
                "finalized recovery changed production invariants",
            )
        validate_pre_switch_abort_settlement(
            checkpoint_path=checkpoint_path,
            checkpoints_root=state_root / "checkpoints",
        )
    result.update(
        {
            "decision": (
                "pre-switch-residue-quarantined-and-aborted"
                if plan_version == 3
                else "pre-switch-aborted"
            ),
            "checkpointPhase": settled["phase"],
            "checkpointSequence": settled["sequence"],
            "receiptPath": str(receipt_path),
            "receiptSha256": receipt_sha256,
            "receiptReused": reused,
            "checkpointChanged": True,
            "mutationPerformed": True,
            **(
                {
                    "finalizationReceiptPath": str(finalization_path),
                    "finalizationReceiptSha256": finalization_sha256,
                    "finalizationReceiptReused": finalization_reused,
                    "candidateResiduePresent": False,
                }
                if plan_version == 3
                else {}
            ),
        }
    )
    return result


def _positive_int(value: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return int(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--expected-plan-sha256", required=True)
    parser.add_argument("--bundle-root", required=True, type=Path)
    parser.add_argument("--implementation-commit", required=True)
    parser.add_argument("--lock-path", required=True, type=Path)
    parser.add_argument("--lock-holder-pid", required=True, type=_positive_int)
    parser.add_argument("--mode", choices=("dry-run", "apply"), required=True)
    parser.add_argument(
        "--dry-run-authorization",
        type=Path,
        default=(
            Path(os.environ["RECOVERY_DRY_RUN_AUTHORIZATION_PATH"])
            if os.environ.get("RECOVERY_DRY_RUN_AUTHORIZATION_PATH")
            else None
        ),
    )
    parser.add_argument(
        "--dry-run-authorization-sha256",
        default=os.environ.get("RECOVERY_DRY_RUN_AUTHORIZATION_SHA256"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _build_parser().parse_args(argv)
    try:
        result = recover(
            plan_path=arguments.plan,
            expected_plan_sha256=arguments.expected_plan_sha256,
            bundle_root=arguments.bundle_root,
            implementation_commit=arguments.implementation_commit,
            lock_path=arguments.lock_path,
            lock_holder_pid=arguments.lock_holder_pid,
            mode=arguments.mode,
            dry_run_authorization_path=arguments.dry_run_authorization,
            dry_run_authorization_sha256=(
                arguments.dry_run_authorization_sha256
            ),
        )
    except RecoveryError as exc:
        print(
            f"pre-switch recovery error: {exc.category}: {exc.detail}",
            file=sys.stderr,
        )
        return 2
    except (OSError, ValueError, TypeError) as exc:
        print(
            f"pre-switch recovery error: invalid_input: {type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
