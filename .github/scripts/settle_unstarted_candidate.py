#!/usr/bin/env python3
"""Settle one failed Candidate that provably stopped before preimage capture.

This helper is intentionally narrower than the reviewed residue recovery tool.
It accepts only a ``migrated/completed/automatic`` checkpoint whose database
revision did not change, whose Candidate preimage was never published, and
whose Candidate service and preview never started.  It then records the
existing ``candidate_prepare_aborted`` terminal without changing Active.
"""

from __future__ import annotations

import argparse
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
from urllib.request import build_opener, ProxyHandler, Request

REPOSITORY_DEPLOY_DIR = (
    Path(__file__).resolve().parents[2] / "03_Scripts/deploy"
)
if (REPOSITORY_DEPLOY_DIR / "release_checkpoint.py").is_file():
    sys.path.insert(0, str(REPOSITORY_DEPLOY_DIR))

from release_checkpoint import (
    CheckpointError,
    ReleaseIdentity,
    _load_checkpoint_journal,
    _read_small_regular_file,
    _release_metadata_commit,
    load_checkpoint,
    write_checkpoint,
)


SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
REVISION_PATTERN = re.compile(r"(?m)^([0-9]{8}_[0-9]{4})\b")
PLAN_FIELDS = {
    "schemaVersion",
    "kind",
    "incidentId",
    "identity",
    "checkpointSha256",
    "evidenceSha256",
    "candidatePreimageHelperSha256",
    "expectedActiveCommit",
    "expectedDatabaseRevision",
    "paths",
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
PATH_FIELDS = {
    "stateRoot",
    "checkpoint",
    "journal",
    "evidence",
    "releaseRoot",
    "activeSlotFile",
    "slotsRoot",
    "slotEnvRoot",
    "backendEnvFile",
    "candidatePreimageRoot",
    "candidatePreviewStateRoot",
    "deploymentMarker",
    "schedulerStateFile",
    "productionLock",
}
MAX_JSON_BYTES = 1024 * 1024
MEMORY_HIGH_BYTES = 6 * 1024**3
MEMORY_MAX_BYTES = 8 * 1024**3


class SettlementError(ValueError):
    """A failed Candidate cannot be proven safe to settle."""


def _fail(message: str) -> None:
    raise SettlementError(message)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_regular(path: Path, label: str, maximum: int) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise SettlementError(f"{label} is unavailable") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_size <= 0
        or before.st_size > maximum
    ):
        _fail(f"{label} is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            _fail(f"{label} changed while opening")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
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
        or len(raw) > maximum
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
    ):
        _fail(f"{label} changed while reading")
    return raw


def _read_json(path: Path, label: str) -> tuple[Mapping[str, Any], bytes]:
    raw = _read_regular(path, label, MAX_JSON_BYTES)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SettlementError(f"{label} is invalid JSON") from exc
    if not isinstance(payload, dict):
        _fail(f"{label} root must be an object")
    return payload, raw


def _absolute(value: object, label: str) -> Path:
    if not isinstance(value, str) or not value.startswith("/"):
        _fail(f"{label} must be absolute")
    path = Path(value)
    if str(path) != value or ".." in path.parts:
        _fail(f"{label} is not canonical")
    return path


def _load_plan(path: Path) -> Mapping[str, Any]:
    plan, _ = _read_json(path, "settlement plan")
    if (
        set(plan) != PLAN_FIELDS
        or plan.get("schemaVersion") != 1
        or plan.get("kind") != "unstarted_candidate_settlement"
        or not isinstance(plan.get("incidentId"), str)
    ):
        _fail("settlement plan contract is invalid")
    identity = plan.get("identity")
    paths = plan.get("paths")
    if not isinstance(identity, dict) or set(identity) != IDENTITY_FIELDS:
        _fail("settlement identity contract is invalid")
    if not isinstance(paths, dict) or set(paths) != PATH_FIELDS:
        _fail("settlement path contract is invalid")
    ReleaseIdentity.from_mapping(identity)
    for field in (
        "checkpointSha256",
        "evidenceSha256",
        "candidatePreimageHelperSha256",
    ):
        if not SHA256_PATTERN.fullmatch(str(plan.get(field) or "")):
            _fail(f"settlement plan {field} is invalid")
    if not GIT_SHA_PATTERN.fullmatch(str(plan.get("expectedActiveCommit") or "")):
        _fail("settlement plan expected Active commit is invalid")
    if not REVISION_PATTERN.fullmatch(
        str(plan.get("expectedDatabaseRevision") or "")
    ):
        _fail("settlement plan expected database revision is invalid")
    for field, value in paths.items():
        _absolute(value, f"settlement path {field}")
    return plan


def _path_lexists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _require_absent(path: Path, label: str) -> None:
    if _path_lexists(path):
        _fail(f"{label} must remain absent")


def _command(arguments: Sequence[str], label: str) -> str:
    result = subprocess.run(
        list(arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        _fail(f"{label} failed")
    return result.stdout.strip()


def _unit_properties(unit: str) -> Mapping[str, str]:
    output = _command(
        (
            "systemctl",
            "show",
            unit,
            "--property=LoadState",
            "--property=UnitFileState",
            "--property=ActiveState",
            "--property=MainPID",
            "--property=MemoryHigh",
            "--property=MemoryMax",
        ),
        f"systemd inspection for {unit}",
    )
    values: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            key, value = line.split("=", maxsplit=1)
            values[key] = value
    return values


def _listeners(port: int) -> str:
    return _command(
        ("ss", "-H", "-ltn", f"sport = :{port}"),
        f"listener inspection for {port}",
    )


def _public_json(url: str, label: str) -> Mapping[str, Any]:
    opener = build_opener(ProxyHandler({}))
    request = Request(url, headers={"Cache-Control": "no-cache"})
    try:
        with opener.open(request, timeout=30) as response:
            raw = response.read(256 * 1024 + 1)
    except OSError as exc:
        raise SettlementError(f"{label} is unavailable") from exc
    if not raw or len(raw) > 256 * 1024:
        _fail(f"{label} has an invalid size")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SettlementError(f"{label} is invalid JSON") from exc
    if not isinstance(payload, dict):
        _fail(f"{label} root must be an object")
    return payload


def _database_current(plan: Mapping[str, Any]) -> set[str]:
    paths = plan["paths"]
    release_root = _absolute(paths["releaseRoot"], "release root")
    env_file = _absolute(paths["backendEnvFile"], "backend env")
    backend = release_root / "06_AppPlatform/backend"
    python = release_root / ".venv/bin/python"
    script = (
        "set -Eeuo pipefail; set -a; . \"$1\"; set +a; "
        "export PYTHONPATH=\"$2\"; "
        "export PGOPTIONS=\"${PGOPTIONS:+$PGOPTIONS }"
        "-c default_transaction_read_only=on\"; "
        "cd \"$2\"; \"$3\" -m alembic current"
    )
    output = _command(
        ("bash", "-c", script, "_", str(env_file), str(backend), str(python)),
        "read-only database revision inspection",
    )
    return set(REVISION_PATTERN.findall(output))


def _verify_database(plan: Mapping[str, Any], evidence: Mapping[str, Any]) -> None:
    migration = evidence.get("migration")
    if not isinstance(migration, dict) or migration.get("status") != "completed":
        _fail("failed Candidate migration evidence is not completed")
    expected = str(plan["expectedDatabaseRevision"])
    recorded = {
        field: set(REVISION_PATTERN.findall(str(migration.get(field) or "")))
        for field in ("preRevision", "targetRevision", "resultRevision")
    }
    if any(revisions != {expected} for revisions in recorded.values()):
        _fail("failed Candidate database revisions are not an exact no-op")
    if _database_current(plan) != {expected}:
        _fail("live database revision differs from the reviewed no-op revision")


def _release_commit(root: Path, label: str) -> str:
    metadata = root / "hermes/deploy_release.json"
    raw = _read_small_regular_file(metadata, label=label)
    return _release_metadata_commit(raw, label=label)


def _verify_active(plan: Mapping[str, Any]) -> tuple[str, Path]:
    paths = plan["paths"]
    active_slot_file = _absolute(paths["activeSlotFile"], "active slot file")
    slot_raw = _read_regular(active_slot_file, "active slot file", 32)
    try:
        active_slot = slot_raw.decode("ascii").strip()
    except UnicodeError as exc:
        raise SettlementError("active slot file is not ASCII") from exc
    if active_slot not in {"8000", "8001"}:
        _fail("active slot is invalid")
    slots_root = _absolute(paths["slotsRoot"], "slots root")
    active_link = slots_root / active_slot / "current"
    if not active_link.is_symlink():
        _fail("Active release link is not a symlink")
    active_root = active_link.resolve(strict=True)
    expected_commit = str(plan["expectedActiveCommit"])
    if _release_commit(active_root, "Active release metadata") != expected_commit:
        _fail("Active release commit changed")
    unit = f"jato-fullstack-backend@{active_slot}.service"
    properties = _unit_properties(unit)
    if (
        properties.get("LoadState") != "loaded"
        or properties.get("UnitFileState") != "enabled"
        or properties.get("ActiveState") != "active"
        or not str(properties.get("MainPID") or "").isdigit()
        or int(properties["MainPID"]) <= 0
        or properties.get("MemoryHigh") != str(MEMORY_HIGH_BYTES)
        or properties.get("MemoryMax") != str(MEMORY_MAX_BYTES)
    ):
        _fail("Active systemd runtime contract changed")
    env_path = _absolute(paths["slotEnvRoot"], "slot env root") / f"{active_slot}.env"
    env_raw = _read_regular(env_path, "Active slot env", 256 * 1024)
    required_lines = {
        b"APP_JATO_MONTHLY_ENABLED=true",
        f"APP_RELEASE_SLOT={active_slot}".encode("ascii"),
        f"APP_RELEASE_SHA={expected_commit}".encode("ascii"),
    }
    if not required_lines.issubset(set(env_raw.splitlines())):
        _fail("Active slot env no longer owns the monthly worker contract")
    health = _public_json("https://www.ojeur.cloud/healthz", "public healthz")
    if health.get("status") != "ok":
        _fail("public healthz is not healthy")
    provenance = _public_json(
        "https://www.ojeur.cloud/release-provenance.json",
        "public release provenance",
    )
    source = provenance.get("source")
    if not isinstance(source, dict) or source.get("githubSha") != expected_commit:
        _fail("public release provenance changed")
    return active_slot, active_root


def _verify_candidate_absent(
    plan: Mapping[str, Any],
    active_slot: str,
) -> str:
    paths = plan["paths"]
    identity = plan["identity"]
    candidate_slot = "8001" if active_slot == "8000" else "8000"
    unit = f"jato-fullstack-backend@{candidate_slot}.service"
    properties = _unit_properties(unit)
    safe_unit_file = properties.get("UnitFileState") in {"disabled", "not-found", ""}
    if (
        properties.get("ActiveState") not in {"inactive", "failed"}
        or properties.get("MainPID") not in {"0", ""}
        or not safe_unit_file
        or _listeners(int(candidate_slot))
    ):
        _fail("Candidate backend is not inactive, disabled, PID-free, and listener-free")
    if _listeners(18002):
        _fail("Candidate preview port is still listening")
    wants = Path(
        "/etc/systemd/system/multi-user.target.wants/"
        f"jato-fullstack-backend@{candidate_slot}.service"
    )
    _require_absent(wants, "Candidate enablement link")
    preview_root = _absolute(
        paths["candidatePreviewStateRoot"],
        "Candidate preview state root",
    )
    _require_absent(preview_root, "Candidate preview state")
    preimage_root = _absolute(
        paths["candidatePreimageRoot"],
        "Candidate preimage root",
    )
    commit = str(identity["commit"])
    archive = str(identity["archiveSha256"])
    preimage = preimage_root / "candidate-preimages" / commit / archive
    for target, label in (
        (preimage, "Candidate preimage"),
        (preimage.parent / f".{archive}.capture.new", "Candidate capture temporary"),
        (
            preimage.parent / f".{archive}.restore-intent.json",
            "Candidate restore intent",
        ),
        (
            preimage.parent / f".{archive}.restore-intent.json.new",
            "Candidate restore intent temporary",
        ),
        (preimage.parent / f".{archive}.discarding", "Candidate discard tombstone"),
        (
            preimage_root / "slot-owners" / f"{candidate_slot}.json",
            "Candidate slot owner",
        ),
    ):
        _require_absent(target, label)
    slot_link = _absolute(paths["slotsRoot"], "slots root") / candidate_slot / "current"
    if slot_link.is_symlink():
        resolved = slot_link.resolve(strict=True)
        failed_root = _absolute(paths["releaseRoot"], "failed release root")
        if resolved == failed_root:
            _fail("Candidate slot points at the failed release")
    slot_env_root = _absolute(paths["slotEnvRoot"], "slot env root")
    slot_env = slot_env_root / f"{candidate_slot}.env"
    if _path_lexists(slot_env):
        env_raw = _read_regular(slot_env, "Candidate slot env", 256 * 1024)
        if str(identity["commit"]).encode("ascii") in env_raw:
            _fail("Candidate slot env contains the failed release")
    for stage in (
        slot_link.parent / ".current.jato-candidate-installing",
        slot_env_root / f".{candidate_slot}.env.jato-candidate-installing",
        Path(
            "/etc/systemd/system/"
            f".jato-fullstack-backend@{candidate_slot}.service.jato-candidate-installing"
        ),
    ):
        _require_absent(stage, "Candidate transaction staging")
    return candidate_slot


def _verify_checkpoint_chain(
    plan: Mapping[str, Any],
) -> tuple[ReleaseIdentity, Mapping[str, Any], Path, Path, Path, bytes, bytes]:
    paths = plan["paths"]
    checkpoint_path = _absolute(paths["checkpoint"], "checkpoint")
    journal_path = _absolute(paths["journal"], "journal")
    evidence_path = _absolute(paths["evidence"], "evidence")
    checkpoint_raw = _read_regular(checkpoint_path, "checkpoint", MAX_JSON_BYTES)
    evidence, evidence_raw = _read_json(evidence_path, "release evidence")
    if _sha256(checkpoint_raw) != plan["checkpointSha256"]:
        _fail("checkpoint SHA-256 differs from the reviewed failure artifact")
    if _sha256(evidence_raw) != plan["evidenceSha256"]:
        _fail("release evidence SHA-256 differs from the reviewed failure artifact")
    checkpoint = load_checkpoint(checkpoint_path)
    identity = ReleaseIdentity.from_mapping(plan["identity"])
    if checkpoint.get("identity") != identity.to_dict():
        _fail("checkpoint identity differs from the settlement plan")
    if (
        checkpoint.get("phase") != "migrated"
        or checkpoint.get("status") != "completed"
        or checkpoint.get("retryClass") != "automatic"
    ):
        _fail("checkpoint is not migrated/completed/automatic")
    if evidence.get("identity") != identity.to_dict():
        _fail("release evidence identity differs from the checkpoint")
    events = _load_checkpoint_journal(journal_path, identity=identity)
    if len(events) != checkpoint.get("sequence") or events[-1] != checkpoint:
        _fail("checkpoint journal does not end at the reviewed checkpoint")
    forbidden = {
        "candidate_ready",
        "switch_started",
        "switched",
        "rollback_started",
        "rollback_completed",
        "backend_healthy",
        "www_verified",
        "active_update_started",
        "active_updated",
        "complete",
    }
    if any(event.get("phase") in forbidden for event in events):
        _fail("checkpoint journal reached Candidate start or a later boundary")
    return (
        identity,
        evidence,
        checkpoint_path,
        journal_path,
        evidence_path,
        checkpoint_raw,
        evidence_raw,
    )


def _verify_old_helper(plan: Mapping[str, Any]) -> None:
    release_root = _absolute(plan["paths"]["releaseRoot"], "failed release root")
    helper = release_root / "03_Scripts/deploy/candidate_runtime_preimage.py"
    raw = _read_regular(helper, "failed Candidate preimage helper", 1024 * 1024)
    if _sha256(raw) != plan["candidatePreimageHelperSha256"]:
        _fail("failed Candidate preimage helper identity changed")
    text = raw.decode("utf-8")
    main_index = text.find("def main(")
    boot_index = text.find("boot_id = _boot_id(arguments)", main_index)
    role_index = text.find("roles = _role_paths(arguments)", main_index)
    capture_index = text.find("result = capture(", main_index)
    if min(main_index, boot_index, role_index, capture_index) < 0:
        _fail("failed Candidate preimage helper control flow is unrecognized")
    if not (main_index < boot_index < role_index < capture_index):
        _fail("failed Candidate preimage helper did not fail before capture writes")


def _verify_previous_metadata(plan: Mapping[str, Any]) -> None:
    identity = plan["identity"]
    state_root = _absolute(plan["paths"]["stateRoot"], "state root")
    path = (
        state_root
        / "previous-metadata"
        / str(identity["commit"])
        / f"{identity['archiveSha256']}.json"
    )
    raw = _read_small_regular_file(path, label="previous release metadata sidecar")
    commit = _release_metadata_commit(
        raw,
        label="previous release metadata sidecar",
    )
    if commit != plan["expectedActiveCommit"]:
        _fail("previous release metadata no longer identifies Active")


def _write_receipt(
    plan: Mapping[str, Any],
    *,
    checkpoint_before_sha256: str,
    journal_before_sha256: str,
    active_slot: str,
    active_root: Path,
    candidate_slot: str,
) -> tuple[Path, str]:
    state_root = _absolute(plan["paths"]["stateRoot"], "state root")
    identity = plan["identity"]
    receipt_root = state_root / "unstarted-candidate-receipts" / str(identity["commit"])
    receipt_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(receipt_root, 0o700)
    receipt = receipt_root / f"{identity['archiveSha256']}.json"
    payload = {
        "schemaVersion": 1,
        "kind": "unstarted_candidate_settlement_receipt",
        "decision": "candidate_prepare_aborted_authorized",
        "incidentId": plan["incidentId"],
        "identity": identity,
        "checkpointBeforeSha256": checkpoint_before_sha256,
        "journalBeforeSha256": journal_before_sha256,
        "evidenceSha256": plan["evidenceSha256"],
        "failedHelperSha256": plan["candidatePreimageHelperSha256"],
        "active": {
            "slot": active_slot,
            "root": str(active_root),
            "commit": plan["expectedActiveCommit"],
        },
        "candidate": {
            "slot": candidate_slot,
            "serviceStarted": False,
            "listenerPresent": False,
            "previewPresent": False,
            "preimagePublished": False,
        },
        "database": {
            "changed": False,
            "revision": plan["expectedDatabaseRevision"],
        },
        "trafficChanged": False,
        "activeChanged": False,
        "jatoDataChanged": False,
        "createdAt": dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    }
    raw = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if _path_lexists(receipt):
        existing = _read_regular(receipt, "settlement receipt", MAX_JSON_BYTES)
        existing_payload = json.loads(existing.decode("utf-8"))
        comparable = dict(payload)
        comparable["createdAt"] = existing_payload.get("createdAt")
        expected = (json.dumps(comparable, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        if existing != expected:
            _fail("existing settlement receipt records different facts")
        return receipt, _sha256(existing)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(receipt, flags, 0o600)
    try:
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise OSError("settlement receipt write made no progress")
            offset += written
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(receipt_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return receipt, _sha256(raw)


def settle(plan: Mapping[str, Any], *, apply: bool) -> Mapping[str, Any]:
    if os.geteuid() != 0:
        _fail("settlement helper must run through non-interactive sudo")
    paths = plan["paths"]
    lock_path = _absolute(paths["productionLock"], "production lock")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    lock_descriptor = os.open(lock_path, flags)
    try:
        lock_metadata = os.fstat(lock_descriptor)
        if not stat.S_ISREG(lock_metadata.st_mode) or lock_metadata.st_nlink != 1:
            _fail("production lock is unsafe")
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        (
            identity,
            evidence,
            checkpoint_path,
            journal_path,
            evidence_path,
            checkpoint_raw,
            _evidence_raw,
        ) = _verify_checkpoint_chain(plan)
        _verify_old_helper(plan)
        _verify_database(plan, evidence)
        _verify_previous_metadata(plan)
        _require_absent(
            _absolute(paths["deploymentMarker"], "deployment marker"),
            "deployment maintenance marker",
        )
        _require_absent(
            _absolute(paths["schedulerStateFile"], "scheduler state file"),
            "scheduler state snapshot",
        )
        active_slot, active_root = _verify_active(plan)
        candidate_slot = _verify_candidate_absent(plan, active_slot)
        journal_before = _read_regular(
            journal_path,
            "checkpoint journal",
            8 * MAX_JSON_BYTES,
        )
        result: dict[str, Any] = {
            "schemaVersion": 1,
            "incidentId": plan["incidentId"],
            "decision": "eligible" if not apply else "candidate_prepare_aborted",
            "identity": identity.to_dict(),
            "active": {
                "slot": active_slot,
                "root": str(active_root),
                "commit": plan["expectedActiveCommit"],
                "changed": False,
            },
            "candidate": {
                "slot": candidate_slot,
                "started": False,
                "previewPresent": False,
                "preimagePublished": False,
            },
            "databaseChanged": False,
            "trafficChanged": False,
            "jatoDataChanged": False,
            "mutationPerformed": apply,
        }
        if not apply:
            return result
        receipt_path, receipt_sha256 = _write_receipt(
            plan,
            checkpoint_before_sha256=_sha256(checkpoint_raw),
            journal_before_sha256=_sha256(journal_before),
            active_slot=active_slot,
            active_root=active_root,
            candidate_slot=candidate_slot,
        )
        evidence_sha256 = str(plan["evidenceSha256"])
        message = (
            "Candidate preimage capture failed before any Candidate runtime write; "
            f"active_slot={active_slot} candidate_slot={candidate_slot}; "
            f"evidence_path={evidence_path} evidence_sha256={evidence_sha256}; "
            f"settlement_receipt_path={receipt_path} "
            f"settlement_receipt_sha256={receipt_sha256}"
        )
        checkpoint_metadata = checkpoint_path.lstat()
        journal_metadata = journal_path.lstat()
        if (
            checkpoint_metadata.st_uid != journal_metadata.st_uid
            or checkpoint_metadata.st_gid != journal_metadata.st_gid
        ):
            _fail("checkpoint and journal owners differ")
        owner_uid = checkpoint_metadata.st_uid
        owner_gid = checkpoint_metadata.st_gid
        terminal: Mapping[str, Any]
        try:
            os.setegid(owner_gid)
            os.seteuid(owner_uid)
            terminal = write_checkpoint(
                checkpoint_path=checkpoint_path,
                journal_path=journal_path,
                identity=identity,
                phase="candidate_prepare_aborted",
                status="completed",
                retry_class="automatic",
                message=message,
            )
        finally:
            os.seteuid(0)
            os.setegid(0)
        if (
            terminal.get("phase") != "candidate_prepare_aborted"
            or terminal.get("status") != "completed"
            or terminal.get("retryClass") != "automatic"
        ):
            _fail("terminal checkpoint was not persisted")
        _verify_active(plan)
        _verify_candidate_absent(plan, active_slot)
        result["receipt"] = {
            "path": str(receipt_path),
            "sha256": receipt_sha256,
        }
        result["terminalCheckpointSha256"] = _sha256(
            _read_regular(checkpoint_path, "terminal checkpoint", MAX_JSON_BYTES)
        )
        return result
    finally:
        os.close(lock_descriptor)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--plan", type=Path, required=True)
    result.add_argument("--mode", choices=("check", "apply"), default="check")
    result.add_argument("--output", type=Path)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        plan = _load_plan(arguments.plan)
        result = settle(plan, apply=arguments.mode == "apply")
        raw = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if arguments.output is not None:
            if arguments.output.is_symlink():
                _fail("settlement output path is unsafe")
            arguments.output.write_text(raw, encoding="utf-8")
            os.chmod(arguments.output, 0o600)
        print(raw, end="")
        return 0
    except (
        CheckpointError,
        OSError,
        SettlementError,
        subprocess.SubprocessError,
        UnicodeError,
        ValueError,
    ) as exc:
        print(f"unstarted Candidate settlement error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
