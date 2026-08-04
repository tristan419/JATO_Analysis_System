#!/usr/bin/env python3
"""Restore and settle one exact failed pre-switch Candidate runtime."""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from typing import Any, Mapping, Sequence

from candidate_runtime_preimage import (
    PROCFS_BOOT_ID_FILE,
    _boot_id,
    _load_preimage,
    _load_restore_intent,
    _load_slot_owner,
    _manifest_sha256,
    _preflight_restore,
    verify_live,
)
from release_checkpoint import write_checkpoint
from settle_unstarted_candidate import (
    IDENTITY_FIELDS,
    MAX_JSON_BYTES,
    PATH_FIELDS,
    REVISION_PATTERN,
    SHA256_PATTERN,
    SettlementError,
    _absolute,
    _command,
    _database_current,
    _fail,
    _path_lexists,
    _read_json,
    _read_regular,
    _require_absent,
    _sha256,
    _unit_properties,
    _verify_active,
    _verify_checkpoint_chain,
    _verify_database,
    _verify_previous_metadata,
)

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


def _load_plan(path: Path) -> Mapping[str, Any]:
    plan, _ = _read_json(path, "settlement plan")
    if (
        set(plan) != PLAN_FIELDS
        or plan.get("schemaVersion") != 1
        or plan.get("kind") != "failed_candidate_preimage_settlement"
        or not isinstance(plan.get("incidentId"), str)
    ):
        _fail("failed Candidate settlement plan contract is invalid")
    identity = plan.get("identity")
    paths = plan.get("paths")
    if not isinstance(identity, dict) or set(identity) != IDENTITY_FIELDS:
        _fail("failed Candidate settlement identity is invalid")
    if not isinstance(paths, dict) or set(paths) != PATH_FIELDS:
        _fail("failed Candidate settlement paths are invalid")
    for field in (
        "checkpointSha256",
        "evidenceSha256",
        "candidatePreimageHelperSha256",
    ):
        if not SHA256_PATTERN.fullmatch(str(plan.get(field) or "")):
            _fail(f"failed Candidate settlement {field} is invalid")
    if not REVISION_PATTERN.fullmatch(
        str(plan.get("expectedDatabaseRevision") or "")
    ):
        _fail("failed Candidate database revision is invalid")
    for field, value in paths.items():
        _absolute(value, f"failed Candidate settlement path {field}")
    return plan


def _role_paths(plan: Mapping[str, Any], candidate_slot: str) -> Mapping[str, Path]:
    paths = plan["paths"]
    slots_root = _absolute(paths["slotsRoot"], "slots root")
    slot_env_root = _absolute(paths["slotEnvRoot"], "slot env root")
    unit = f"jato-fullstack-backend@{candidate_slot}.service"
    return {
        "slot_link": slots_root / candidate_slot / "current",
        "slot_link_stage": (
            slots_root / candidate_slot / ".current.jato-candidate-installing"
        ),
        "slot_env": slot_env_root / f"{candidate_slot}.env",
        "slot_env_stage": (
            slot_env_root / f".{candidate_slot}.env.jato-candidate-installing"
        ),
        "explicit_unit": Path("/etc/systemd/system") / unit,
        "explicit_unit_stage": (
            Path("/etc/systemd/system") / f".{unit}.jato-candidate-installing"
        ),
        "instance_dropins": Path("/etc/systemd/system") / f"{unit}.d",
        "persistent_control_dropins": (
            Path("/etc/systemd/system.control") / f"{unit}.d"
        ),
        "runtime_control_dropins": (
            Path("/run/systemd/system.control") / f"{unit}.d"
        ),
        "candidate_cache_link": Path(f"/var/cache/jato-candidate-{candidate_slot}"),
        "candidate_cache_private": Path(
            f"/var/cache/private/jato-candidate-{candidate_slot}"
        ),
    }


def _preimage_state(
    plan: Mapping[str, Any],
    candidate_slot: str,
) -> tuple[Path, Mapping[str, Path], Mapping[str, Any], str]:
    identity = plan["identity"]
    preimage = (
        _absolute(plan["paths"]["candidatePreimageRoot"], "preimage root")
        / "candidate-preimages"
        / str(identity["commit"])
        / str(identity["archiveSha256"])
    )
    role_paths = _role_paths(plan, candidate_slot)
    helper = _absolute(plan["paths"]["releaseRoot"], "release root") / (
        "03_Scripts/deploy/candidate_runtime_preimage.py"
    )
    helper_raw = _read_regular(helper, "failed Candidate preimage helper", 1024 * 1024)
    if _sha256(helper_raw) != plan["candidatePreimageHelperSha256"]:
        _fail("failed Candidate preimage helper identity changed")
    preimage_identity = {
        "commit": str(identity["commit"]),
        "archiveSha256": str(identity["archiveSha256"]),
        "candidateSlot": candidate_slot,
    }
    manifest = _load_preimage(preimage, preimage_identity, role_paths)
    owner, _ = _load_slot_owner(preimage, candidate_slot, required=False)
    if owner is not None and owner.get("identity") != preimage_identity:
        _fail("Candidate preimage owner differs from the failed release")
    boot_id = _boot_id(argparse.Namespace(boot_id_file=PROCFS_BOOT_ID_FILE))
    intent_armed = _load_restore_intent(preimage, manifest) is not None
    if owner is None:
        verify_live(manifest, role_paths, boot_id)
    else:
        _preflight_restore(
            preimage,
            manifest,
            role_paths,
            boot_id,
            intent_armed=intent_armed,
        )
    return preimage, role_paths, manifest, _manifest_sha256(preimage)


def _candidate_slot(plan: Mapping[str, Any], active_slot: str) -> str:
    candidate_slot = "8001" if active_slot == "8000" else "8000"
    unit = f"jato-fullstack-backend@{candidate_slot}.service"
    properties = _unit_properties(unit)
    if properties.get("UnitFileState") not in {"disabled", "not-found", ""}:
        _fail("failed Candidate unexpectedly owns boot enablement")
    if _path_lexists(
        Path(f"/etc/systemd/system/multi-user.target.wants/{unit}")
    ):
        _fail("failed Candidate enablement link exists")
    _require_absent(
        _absolute(
            plan["paths"]["candidatePreviewStateRoot"],
            "Candidate preview state",
        ),
        "Candidate preview state",
    )
    return candidate_slot


def _invoke_preimage_helper(
    plan: Mapping[str, Any],
    command: str,
    preimage: Path,
    role_paths: Mapping[str, Path],
    candidate_slot: str,
) -> str:
    identity = plan["identity"]
    helper = _absolute(plan["paths"]["releaseRoot"], "release root") / (
        "03_Scripts/deploy/candidate_runtime_preimage.py"
    )
    arguments = [
        sys.executable,
        "-B",
        str(helper),
        command,
        "--preimage",
        str(preimage),
        "--commit",
        str(identity["commit"]),
        "--archive-sha256",
        str(identity["archiveSha256"]),
        "--candidate-slot",
        candidate_slot,
    ]
    for role, argument in (
        ("slot_link", "slot-link"),
        ("slot_link_stage", "slot-link-stage"),
        ("slot_env", "slot-env"),
        ("slot_env_stage", "slot-env-stage"),
        ("explicit_unit", "explicit-unit"),
        ("explicit_unit_stage", "explicit-unit-stage"),
        ("instance_dropins", "instance-dropins"),
        ("persistent_control_dropins", "persistent-control-dropins"),
        ("runtime_control_dropins", "runtime-control-dropins"),
        ("candidate_cache_link", "candidate-cache-link"),
        ("candidate_cache_private", "candidate-cache-private"),
    ):
        arguments.extend((f"--{argument}", str(role_paths[role])))
    return _command(arguments, f"Candidate preimage {command}")


def _candidate_is_quiescent(candidate_slot: str) -> None:
    unit = f"jato-fullstack-backend@{candidate_slot}.service"
    properties = _unit_properties(unit)
    if (
        properties.get("ActiveState") not in {"inactive", "failed"}
        or properties.get("MainPID") not in {"0", ""}
        or properties.get("UnitFileState") not in {"disabled", "not-found", ""}
    ):
        _fail("Candidate runtime is not quiescent after restore")
    listener = _command(
        ("ss", "-H", "-ltn", f"sport = :{candidate_slot}"),
        "Candidate listener inspection",
    )
    if listener:
        _fail("Candidate listener remains after restore")


def _write_receipt(
    plan: Mapping[str, Any],
    *,
    active_slot: str,
    candidate_slot: str,
    preimage: Path,
    manifest_sha256: str,
    checkpoint_before_sha256: str,
) -> tuple[Path, str]:
    identity = plan["identity"]
    root = (
        _absolute(plan["paths"]["stateRoot"], "state root")
        / "failed-candidate-receipts"
        / str(identity["commit"])
    )
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    path = root / f"{identity['archiveSha256']}.json"
    payload = {
        "schemaVersion": 1,
        "kind": "failed_candidate_preimage_settlement_receipt",
        "decision": "candidate_prepare_aborted",
        "incidentId": plan["incidentId"],
        "identity": identity,
        "checkpointBeforeSha256": checkpoint_before_sha256,
        "evidenceSha256": plan["evidenceSha256"],
        "preimage": str(preimage),
        "preimageManifestSha256": manifest_sha256,
        "active": {"slot": active_slot, "commit": plan["expectedActiveCommit"]},
        "candidate": {"slot": candidate_slot, "restored": True},
        "databaseChanged": False,
        "trafficChanged": False,
        "activeChanged": False,
        "jatoDataChanged": False,
        "createdAt": dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    }
    raw = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    if _path_lexists(path):
        existing = _read_regular(path, "failed Candidate receipt", MAX_JSON_BYTES)
        replay = dict(payload)
        replay["createdAt"] = json.loads(existing)["createdAt"]
        if existing != (json.dumps(replay, indent=2, sort_keys=True) + "\n").encode():
            _fail("existing failed Candidate receipt records different facts")
        return path, _sha256(existing)
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise OSError("failed Candidate receipt write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path, _sha256(raw)


def settle(plan: Mapping[str, Any], *, apply: bool) -> Mapping[str, Any]:
    if os.geteuid() != 0:
        _fail("failed Candidate settlement must run through non-interactive sudo")
    lock_path = _absolute(plan["paths"]["productionLock"], "production lock")
    descriptor = os.open(
        lock_path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            _fail("production lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        (
            identity,
            evidence,
            checkpoint_path,
            journal_path,
            evidence_path,
            checkpoint_raw,
            _evidence_raw,
        ) = _verify_checkpoint_chain(plan)
        _verify_database(plan, evidence)
        _verify_previous_metadata(plan)
        _require_absent(
            _absolute(plan["paths"]["deploymentMarker"], "deployment marker"),
            "deployment maintenance marker",
        )
        _require_absent(
            _absolute(plan["paths"]["schedulerStateFile"], "scheduler state"),
            "scheduler state snapshot",
        )
        active_slot, active_root = _verify_active(plan)
        candidate_slot = _candidate_slot(plan, active_slot)
        preimage, role_paths, _manifest, manifest_sha256 = _preimage_state(
            plan,
            candidate_slot,
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
                "preimage": str(preimage),
                "preimageManifestSha256": manifest_sha256,
                "restored": apply,
            },
            "databaseChanged": False,
            "trafficChanged": False,
            "jatoDataChanged": False,
            "mutationPerformed": apply,
        }
        if not apply:
            return result
        unit = f"jato-fullstack-backend@{candidate_slot}.service"
        subprocess.run(("systemctl", "disable", "--now", unit), check=False)
        subprocess.run(("systemctl", "reset-failed", unit), check=False)
        _candidate_is_quiescent(candidate_slot)
        _invoke_preimage_helper(
            plan,
            "restore",
            preimage,
            role_paths,
            candidate_slot,
        )
        _command(("systemctl", "daemon-reload"), "systemd daemon reload")
        _candidate_is_quiescent(candidate_slot)
        _invoke_preimage_helper(
            plan,
            "verify-live",
            preimage,
            role_paths,
            candidate_slot,
        )
        _verify_active(plan)
        _verify_database(plan, evidence)
        receipt_path, receipt_sha256 = _write_receipt(
            plan,
            active_slot=active_slot,
            candidate_slot=candidate_slot,
            preimage=preimage,
            manifest_sha256=manifest_sha256,
            checkpoint_before_sha256=_sha256(checkpoint_raw),
        )
        checkpoint_metadata = checkpoint_path.lstat()
        journal_metadata = journal_path.lstat()
        if (
            checkpoint_metadata.st_uid != journal_metadata.st_uid
            or checkpoint_metadata.st_gid != journal_metadata.st_gid
        ):
            _fail("checkpoint and journal owners differ")
        try:
            os.setegid(checkpoint_metadata.st_gid)
            os.seteuid(checkpoint_metadata.st_uid)
            terminal = write_checkpoint(
                checkpoint_path=checkpoint_path,
                journal_path=journal_path,
                identity=identity,
                phase="candidate_prepare_aborted",
                status="completed",
                retry_class="automatic",
                message=(
                    "Failed pre-switch Candidate runtime was restored from its exact "
                    f"preimage; active_slot={active_slot} candidate_slot={candidate_slot}; "
                    f"evidence_path={evidence_path}; receipt={receipt_path}; "
                    f"receipt_sha256={receipt_sha256}"
                ),
            )
        finally:
            os.seteuid(0)
            os.setegid(0)
        if terminal.get("phase") != "candidate_prepare_aborted":
            _fail("failed Candidate terminal checkpoint was not persisted")
        _verify_active(plan)
        if _database_current(plan) != {str(plan["expectedDatabaseRevision"])}:
            _fail("database revision changed during Candidate settlement")
        result["receipt"] = {"path": str(receipt_path), "sha256": receipt_sha256}
        result["terminalCheckpointSha256"] = hashlib.sha256(
            _read_regular(checkpoint_path, "terminal checkpoint", MAX_JSON_BYTES)
        ).hexdigest()
        return result
    finally:
        os.close(descriptor)


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
            arguments.output.write_text(raw, encoding="utf-8")
            os.chmod(arguments.output, 0o600)
        print(raw, end="")
        return 0
    except (OSError, SettlementError, subprocess.SubprocessError, ValueError) as exc:
        print(f"failed Candidate settlement error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
