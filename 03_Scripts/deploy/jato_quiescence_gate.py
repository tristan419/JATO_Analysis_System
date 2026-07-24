#!/usr/bin/env python3
"""Hold a fail-closed JATO maintenance fence around a deployment command.

The monthly-update service stores its durable coordination state below one job
root.  This helper deliberately uses the same flock files as the application:
the maintenance lock is the admission fence, while the remaining non-blocking
locks close the final check-to-switch race.  It never edits a job or upload
state file and never signals a worker.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import dataclass
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any, BinaryIO, Sequence


MAX_STATE_BYTES = 1024 * 1024
MAX_PROC_ENVIRON_BYTES = 2 * 1024 * 1024
BUSY_JOB_STATUSES = frozenset({"queued", "running"})
BUSY_OPERATION_STATUSES = frozenset({"queued", "running"})
BUSY_UPLOAD_STATUSES = frozenset({"assembling", "digesting"})
BUSY_DIGEST_ATTEMPT_STATUSES = frozenset(
    {"launching", "running", "digesting"},
)
BUSY_BASELINE_STATUSES = frozenset({"queued", "running"})
TERMINAL_JOB_STATUSES = frozenset({"success", "failed", "cancelled"})
TERMINAL_OPERATION_STATUSES = frozenset({"success", "failed"})
TERMINAL_DIGEST_ATTEMPT_STATUSES = frozenset(
    {"worker_finished", "finished"},
)
IDLE_UPLOAD_STATUSES = frozenset(
    {"pending", "uploading", "ready", "consumed", "invalid", "abandoned", "expired"},
)
TERMINAL_BASELINE_STATUSES = frozenset({"success", "failed"})
LOCK_RETRY_SECONDS = 0.25
EXIT_BUSY_TIMEOUT = 75
EXIT_INVALID_STATE = 78
EXIT_POST_COMMAND_EVIDENCE_FAILED = 79
EXIT_MARKER_CLEANUP_FAILED = 80
EXIT_COMMAND_FAILED_MARKER_RETAINED = 81
EXIT_COMMAND_START_FAILED = 82


class GateError(RuntimeError):
    """A durable state or coordination invariant could not be proven."""


@dataclass(frozen=True)
class ActiveRuntimeContract:
    """Expected identity of the currently active backend and its JATO locks."""

    main_pid: int
    expected_project_root: Path
    active_bundle_lock_path: Path
    proc_root: Path = Path("/proc")


@dataclass(frozen=True)
class MarkerIdentity:
    device: int
    inode: int


class PostCommandEvidenceError(GateError):
    """The command ended, but its durable helper evidence was not committed."""

    def __init__(self, message: str, *, command_exit_code: int) -> None:
        super().__init__(message)
        self.command_exit_code = command_exit_code
        self.marker_retained = True


class MarkerCleanupError(GateError):
    """The owned maintenance marker could not be durably removed."""

    def __init__(
        self,
        message: str,
        *,
        command_exit_code: int | None,
        marker_retained: bool,
    ) -> None:
        super().__init__(message)
        self.command_exit_code = command_exit_code
        self.marker_retained = marker_retained


class CommandFailedMarkerRetainedError(GateError):
    """The child reported that route/rollback safety is not yet proven."""

    def __init__(self, message: str, *, command_exit_code: int) -> None:
        super().__init__(message)
        self.command_exit_code = command_exit_code
        self.marker_retained = True


class CommandStartError(GateError):
    """The deployment child could not be started, so switching never began."""

    def __init__(self, message: str, *, evidence_committed: bool) -> None:
        super().__init__(message)
        self.evidence_committed = evidence_committed
        self.marker_retained = False


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00",
        "Z",
    )


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, 0o600)
        os.replace(temp_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _read_state(path: Path) -> dict[str, Any]:
    try:
        current = path.lstat()
    except OSError as exc:
        raise GateError(f"cannot stat state file: {path}") from exc
    if path.is_symlink() or not path.is_file():
        raise GateError(f"state path is not a regular file: {path}")
    if current.st_size <= 0 or current.st_size > MAX_STATE_BYTES:
        raise GateError(f"state file size is invalid: {path}")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise GateError(f"state file is unreadable: {path}") from exc
    try:
        after = path.stat()
    except OSError as exc:
        raise GateError(f"state file changed while reading: {path}") from exc
    if (
        len(raw) != current.st_size
        or after.st_dev != current.st_dev
        or after.st_ino != current.st_ino
        or after.st_size != current.st_size
        or after.st_mtime_ns != current.st_mtime_ns
    ):
        raise GateError(f"state file changed while reading: {path}")
    if not isinstance(payload, dict):
        raise GateError(f"state root must be an object: {path}")
    return payload


def _required_status(
    payload: dict[str, Any],
    *,
    context: str,
) -> str:
    value = payload.get("status")
    if not isinstance(value, str) or not value.strip():
        raise GateError(f"{context} status must be one non-empty string")
    return value.strip().lower()


def _optional_pid_is_alive(
    payload: dict[str, Any],
    key: str,
    *,
    context: str,
) -> bool:
    if key not in payload or payload[key] is None:
        return False
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 1:
        raise GateError(f"{context} {key} must be one PID integer greater than 1")
    try:
        os.kill(value, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _job_is_busy(payload: dict[str, Any]) -> bool:
    status = _required_status(payload, context="JATO job")
    if status not in BUSY_JOB_STATUSES and status not in TERMINAL_JOB_STATUSES:
        raise GateError(f"unknown JATO job status: {status or '<empty>'}")

    operation_busy = False
    if "pendingOperation" in payload and payload["pendingOperation"] is not None:
        operation = payload["pendingOperation"]
        if not isinstance(operation, dict):
            raise GateError("JATO pendingOperation must be an object or null")
        operation_status = _required_status(
            operation,
            context="JATO pending operation",
        )
        if operation_status in BUSY_OPERATION_STATUSES:
            operation_busy = True
        elif operation_status not in TERMINAL_OPERATION_STATUSES:
            raise GateError(
                f"unknown JATO pending operation status: {operation_status}",
            )

    current_process_alive = False
    if "currentProcess" in payload and payload["currentProcess"] is not None:
        current_process = payload["currentProcess"]
        if not isinstance(current_process, dict):
            raise GateError("JATO currentProcess must be an object or null")
        if "pid" not in current_process or current_process["pid"] is None:
            raise GateError("JATO currentProcess pid is required")
        current_process_alive = _optional_pid_is_alive(
            current_process,
            "pid",
            context="JATO currentProcess",
        )

    worker_alive = _optional_pid_is_alive(
        payload,
        "workerPid",
        context="JATO job",
    )
    return (
        status in BUSY_JOB_STATUSES
        or operation_busy
        or worker_alive
        or current_process_alive
    )


def _upload_is_busy(payload: dict[str, Any]) -> bool:
    status = _required_status(payload, context="JATO upload")
    if status not in BUSY_UPLOAD_STATUSES and status not in IDLE_UPLOAD_STATUSES:
        raise GateError(f"unknown JATO upload status: {status or '<empty>'}")

    digest_pid_alive = _optional_pid_is_alive(
        payload,
        "digestPid",
        context="JATO upload",
    )
    worker_pid_alive = _optional_pid_is_alive(
        payload,
        "digestWorkerPid",
        context="JATO upload",
    )

    attempt_busy = False
    attempt_supervisor_alive = False
    attempt_worker_alive = False
    if "digestAttempt" in payload and payload["digestAttempt"] is not None:
        attempt = payload["digestAttempt"]
        if not isinstance(attempt, dict):
            raise GateError("JATO digestAttempt must be an object or null")
        attempt_status = _required_status(
            attempt,
            context="JATO digestAttempt",
        )
        attempt_supervisor_alive = _optional_pid_is_alive(
            attempt,
            "supervisorPid",
            context="JATO digestAttempt",
        )
        attempt_worker_alive = _optional_pid_is_alive(
            attempt,
            "workerPid",
            context="JATO digestAttempt",
        )
        if attempt_status in BUSY_DIGEST_ATTEMPT_STATUSES:
            attempt_busy = True
        elif attempt_status not in TERMINAL_DIGEST_ATTEMPT_STATUSES:
            raise GateError(
                f"unknown JATO digest attempt status: {attempt_status}",
            )

    return (
        status in BUSY_UPLOAD_STATUSES
        or digest_pid_alive
        or worker_pid_alive
        or attempt_busy
        or attempt_supervisor_alive
        or attempt_worker_alive
    )


def _state_paths(job_root: Path) -> tuple[list[Path], list[Path], Path]:
    job_states = sorted(
        path
        for path in job_root.glob("*/job_state.json")
        if not path.parent.name.startswith("_")
    )
    upload_root = job_root / "_upload_sessions"
    upload_states = sorted(upload_root.glob("*/upload_state.json"))
    baseline_state = (
        job_root
        / "_maintenance"
        / "baseline_promotion_state.json"
    )
    return job_states, upload_states, baseline_state


def inspect_state(job_root: Path) -> dict[str, object]:
    job_states, upload_states, baseline_state = _state_paths(job_root)
    busy_jobs: list[str] = []
    busy_uploads: list[str] = []
    for path in job_states:
        if _job_is_busy(_read_state(path)):
            busy_jobs.append(path.parent.name)
    for path in upload_states:
        if _upload_is_busy(_read_state(path)):
            busy_uploads.append(path.parent.name)
    baseline_status = ""
    if baseline_state.exists():
        baseline_status = _required_status(
            _read_state(baseline_state),
            context="JATO baseline promotion",
        )
        if (
            baseline_status not in BUSY_BASELINE_STATUSES
            and baseline_status not in TERMINAL_BASELINE_STATUSES
        ):
            raise GateError(
                f"unknown JATO baseline promotion status: {baseline_status or '<empty>'}",
            )
    return {
        "jobStateCount": len(job_states),
        "uploadStateCount": len(upload_states),
        "busyJobs": busy_jobs,
        "busyUploads": busy_uploads,
        "baselineStatus": baseline_status,
        "busy": bool(
            busy_jobs
            or busy_uploads
            or baseline_status in BUSY_BASELINE_STATUSES
        ),
    }


def _proc_stat_start_time(path: Path, expected_pid: int) -> int:
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise GateError(f"cannot read active backend process identity: {path}") from exc
    closing_parenthesis = raw.rfind(")")
    if (
        closing_parenthesis <= 0
        or not raw.startswith(f"{expected_pid} (")
    ):
        raise GateError(f"active backend process stat is malformed: {path}")
    fields = raw[closing_parenthesis + 1 :].strip().split()
    if len(fields) <= 19:
        raise GateError(f"active backend process stat is incomplete: {path}")
    try:
        start_time = int(fields[19])
    except ValueError as exc:
        raise GateError(f"active backend process start time is malformed: {path}") from exc
    if start_time <= 0:
        raise GateError(f"active backend process start time is invalid: {path}")
    return start_time


def _read_proc_environ(path: Path, *, allow_sudo_fallback: bool) -> bytes:
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_PROC_ENVIRON_BYTES + 1)
    except PermissionError as direct_exc:
        if not allow_sudo_fallback:
            raise GateError(f"cannot read active backend environment: {path}") from direct_exc
        try:
            result = subprocess.run(
                ["sudo", "-n", "cat", "--", str(path)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            raise GateError(
                f"cannot invoke privileged active backend environment read: {path}",
            ) from exc
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise GateError(
                "cannot read active backend environment"
                f" with sudo: {path}: {detail or 'permission denied'}",
            )
        raw = result.stdout
    except OSError as exc:
        raise GateError(f"cannot read active backend environment: {path}") from exc
    if not raw or len(raw) > MAX_PROC_ENVIRON_BYTES:
        raise GateError(f"active backend environment size is invalid: {path}")
    return raw


def _parse_proc_environ(raw: bytes, *, path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        key_bytes, separator, value_bytes = entry.partition(b"=")
        if not separator or not key_bytes:
            raise GateError(f"active backend environment is malformed: {path}")
        try:
            key = key_bytes.decode("utf-8")
            value = value_bytes.decode("utf-8")
        except UnicodeError as exc:
            raise GateError(f"active backend environment is not UTF-8: {path}") from exc
        if key in result:
            raise GateError(
                f"active backend environment contains duplicate key {key}: {path}",
            )
        result[key] = value
    return result


def _directory_proof(path: Path, *, context: str) -> dict[str, object]:
    if not path.is_absolute():
        raise GateError(f"{context} must be an absolute path: {path}")
    try:
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
    except OSError as exc:
        raise GateError(f"{context} is unavailable: {path}") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise GateError(f"{context} is not a directory: {path}")
    return {
        "configuredPath": str(path),
        "resolvedPath": str(resolved),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
    }


def _same_directory(
    left: dict[str, object],
    right: dict[str, object],
) -> bool:
    return (
        left["device"] == right["device"]
        and left["inode"] == right["inode"]
    )


def _lock_location_proof(
    path: Path,
    *,
    context: str,
) -> dict[str, object]:
    if path.name != "active-bundle.lock":
        raise GateError(
            f"{context} filename must be active-bundle.lock: {path}",
        )
    parent = _directory_proof(path.parent, context=f"{context} parent")
    return {
        "configuredPath": str(path),
        "parentResolvedPath": parent["resolvedPath"],
        "parentDevice": parent["device"],
        "parentInode": parent["inode"],
        "filename": path.name,
    }


def verify_active_runtime(
    *,
    job_root: Path,
    contract: ActiveRuntimeContract,
) -> dict[str, object]:
    """Prove the lock namespace belongs to the live active service MainPID."""

    if (
        isinstance(contract.main_pid, bool)
        or not isinstance(contract.main_pid, int)
        or contract.main_pid <= 1
    ):
        raise GateError("active backend MainPID must be an integer greater than 1")
    if not contract.proc_root.is_absolute():
        raise GateError("proc root must be an absolute path")

    process_root = contract.proc_root / str(contract.main_pid)
    stat_path = process_root / "stat"
    environ_path = process_root / "environ"
    start_time_before = _proc_stat_start_time(stat_path, contract.main_pid)
    raw_environment = _read_proc_environ(
        environ_path,
        allow_sudo_fallback=contract.proc_root == Path("/proc"),
    )
    start_time_after = _proc_stat_start_time(stat_path, contract.main_pid)
    if start_time_before != start_time_after:
        raise GateError("active backend MainPID changed identity during validation")
    environment = _parse_proc_environ(raw_environment, path=environ_path)

    project_value = environment.get("APP_PROJECT_ROOT", "").strip()
    job_root_value = environment.get(
        "APP_JATO_MONTHLY_UPDATE_JOB_ROOT",
        "",
    ).strip()
    if not project_value:
        raise GateError("active backend APP_PROJECT_ROOT is missing")
    job_root_source = "explicit_environment"
    if not job_root_value:
        job_root_source = "config_default_from_project_root"
        job_root_value = str(
            Path(project_value)
            / "04_Processed_data"
            / "ops"
            / "jato_monthly_update_jobs",
        )

    live_project = _directory_proof(
        Path(project_value),
        context="active backend APP_PROJECT_ROOT",
    )
    expected_project = _directory_proof(
        contract.expected_project_root,
        context="expected active project root",
    )
    if not _same_directory(live_project, expected_project):
        raise GateError(
            "active backend APP_PROJECT_ROOT does not match the expected active bundle",
        )

    live_job_root = _directory_proof(
        Path(job_root_value),
        context="active backend APP_JATO_MONTHLY_UPDATE_JOB_ROOT",
    )
    expected_job_root = _directory_proof(
        job_root,
        context="expected JATO job root",
    )
    if not _same_directory(live_job_root, expected_job_root):
        raise GateError(
            "active backend JATO job root does not match the gate lock namespace",
        )

    live_active_lock = _lock_location_proof(
        Path(project_value) / "04_Processed_data" / "active-bundle.lock",
        context="active backend active bundle lock",
    )
    expected_active_lock = _lock_location_proof(
        contract.active_bundle_lock_path,
        context="expected active bundle lock",
    )
    if (
        live_active_lock["parentDevice"]
        != expected_active_lock["parentDevice"]
        or live_active_lock["parentInode"]
        != expected_active_lock["parentInode"]
        or live_active_lock["filename"] != expected_active_lock["filename"]
    ):
        raise GateError(
            "active backend active-bundle.lock does not match the gate lock namespace",
        )

    return {
        "mainPid": contract.main_pid,
        "processStartTime": start_time_before,
        "projectRoot": live_project,
        "jobRoot": live_job_root,
        "jobRootSource": job_root_source,
        "activeBundleLock": live_active_lock,
    }


def _open_lock(path: Path) -> BinaryIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    return os.fdopen(descriptor, "a+b", closefd=True)


def _try_lock(stack: ExitStack, path: Path) -> BinaryIO | None:
    handle = stack.enter_context(_open_lock(path))
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return None
    os.set_inheritable(handle.fileno(), True)
    return handle


def _final_lock_paths(
    job_root: Path,
    active_bundle_lock_path: Path,
) -> list[Path]:
    job_states, upload_states, _ = _state_paths(job_root)
    upload_root = job_root / "_upload_sessions"
    paths = [
        job_root / "worker.lock",
        active_bundle_lock_path,
        upload_root / "upload-initiate.lock",
        job_root / "_maintenance" / "baseline-promotion.lock",
    ]
    paths.extend(path.parent / "state.lock" for path in job_states)
    for state_path in upload_states:
        paths.extend(
            (
                state_path.parent / "digest.lock",
                state_path.parent / "state.lock",
            )
        )
    return paths


def _create_marker(path: Path) -> MarkerIdentity:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise GateError(f"deployment marker already exists: {path}") from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(f"startedAt={_utc_now()}\npid={os.getpid()}\n")
            handle.flush()
            os.fsync(handle.fileno())
            metadata = os.fstat(handle.fileno())
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        raise GateError(f"deployment marker could not be committed: {path}") from exc
    return MarkerIdentity(device=metadata.st_dev, inode=metadata.st_ino)


def _remove_marker(path: Path, identity: MarkerIdentity) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise OSError(f"deployment marker disappeared before cleanup: {path}") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_dev != identity.device
        or metadata.st_ino != identity.inode
    ):
        raise OSError(f"deployment marker identity changed before cleanup: {path}")
    path.unlink()
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def hold_gate(
    *,
    job_root: Path,
    marker_path: Path,
    timeout_seconds: float,
    evidence_path: Path,
    runtime_contract: ActiveRuntimeContract,
    command: Sequence[str],
) -> int:
    if not job_root.is_dir() or job_root.is_symlink():
        raise GateError("JATO job root must be one existing non-symlink directory")
    if timeout_seconds < 0:
        raise GateError("timeout must not be negative")
    if not command:
        raise GateError("a deployment command is required")

    runtime_proof = verify_active_runtime(
        job_root=job_root,
        contract=runtime_contract,
    )
    started = time.monotonic()
    deadline = started + timeout_seconds
    maintenance_path = (
        job_root
        / "_maintenance"
        / "maintenance-coordination.lock"
    )
    evidence: dict[str, object] = {
        "schemaVersion": 2,
        "startedAt": _utc_now(),
        "jobRoot": str(job_root),
        "jobRootDevice": job_root.stat().st_dev,
        "jobRootInode": job_root.stat().st_ino,
        "runtimeProof": runtime_proof,
        "markerPath": str(marker_path),
        "command": list(command),
        "status": "starting",
    }
    _atomic_write_json(evidence_path, evidence)
    marker_identity = _create_marker(marker_path)
    evidence["markerIdentity"] = {
        "device": marker_identity.device,
        "inode": marker_identity.inode,
    }
    retain_marker = False
    command_exit_code: int | None = None
    try:
        with _open_lock(maintenance_path) as maintenance:
            while True:
                try:
                    fcntl.flock(
                        maintenance.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        evidence.update(
                            status="busy_timeout",
                            finishedAt=_utc_now(),
                            failure="maintenance_lock_busy",
                        )
                        _atomic_write_json(evidence_path, evidence)
                        return EXIT_BUSY_TIMEOUT
                    time.sleep(LOCK_RETRY_SECONDS)
            os.set_inheritable(maintenance.fileno(), True)

            while True:
                first = inspect_state(job_root)
                if not bool(first["busy"]):
                    with ExitStack() as final_stack:
                        locked: list[BinaryIO] = []
                        lock_failed = False
                        for lock_path in _final_lock_paths(
                            job_root,
                            runtime_contract.active_bundle_lock_path,
                        ):
                            handle = _try_lock(final_stack, lock_path)
                            if handle is None:
                                lock_failed = True
                                break
                            locked.append(handle)
                        if not lock_failed:
                            second = inspect_state(job_root)
                            if not bool(second["busy"]) and first == second:
                                final_runtime_proof = verify_active_runtime(
                                    job_root=job_root,
                                    contract=runtime_contract,
                                )
                                if final_runtime_proof != runtime_proof:
                                    raise GateError(
                                        "active backend runtime identity changed "
                                        "while acquiring the JATO fence",
                                    )
                                evidence.update(
                                    status="command_running",
                                    quiescentAt=_utc_now(),
                                    state=second,
                                    runtimeProof=final_runtime_proof,
                                    finalLockCount=len(locked),
                                )
                                _atomic_write_json(evidence_path, evidence)
                                environment = dict(os.environ)
                                environment["JATO_QUIESCENCE_LOCK_HELD"] = "1"
                                environment["JATO_DEPLOYMENT_MARKER"] = str(marker_path)
                                pass_fds = [maintenance.fileno()]
                                pass_fds.extend(handle.fileno() for handle in locked)
                                retain_marker = True
                                try:
                                    result = subprocess.run(
                                        list(command),
                                        check=False,
                                        env=environment,
                                        pass_fds=tuple(pass_fds),
                                    )
                                except OSError as exc:
                                    retain_marker = False
                                    evidence.update(
                                        status="command_start_failed",
                                        failure=str(exc),
                                        finishedAt=_utc_now(),
                                    )
                                    evidence_committed = False
                                    try:
                                        _atomic_write_json(
                                            evidence_path,
                                            evidence,
                                        )
                                    except Exception:
                                        pass
                                    else:
                                        evidence_committed = True
                                    raise CommandStartError(
                                        "deployment command could not be started",
                                        evidence_committed=evidence_committed,
                                    ) from exc
                                command_exit_code = result.returncode
                                evidence.update(
                                    status=(
                                        "completed"
                                        if result.returncode == 0
                                        else (
                                            "command_failed_marker_retained"
                                            if result.returncode
                                            == EXIT_COMMAND_FAILED_MARKER_RETAINED
                                            else "command_failed"
                                        )
                                    ),
                                    commandExitCode=result.returncode,
                                    finishedAt=_utc_now(),
                                )
                                try:
                                    _atomic_write_json(evidence_path, evidence)
                                except Exception as exc:
                                    raise PostCommandEvidenceError(
                                        "deployment command ended but final "
                                        "quiescence evidence was not committed",
                                        command_exit_code=result.returncode,
                                    ) from exc
                                if (
                                    result.returncode
                                    == EXIT_COMMAND_FAILED_MARKER_RETAINED
                                ):
                                    raise CommandFailedMarkerRetainedError(
                                        "deployment command could not prove a safe "
                                        "route or rollback; marker was retained",
                                        command_exit_code=result.returncode,
                                    )
                                retain_marker = False
                                return result.returncode
                if time.monotonic() >= deadline:
                    evidence.update(
                        status="busy_timeout",
                        finishedAt=_utc_now(),
                        state=first,
                    )
                    _atomic_write_json(evidence_path, evidence)
                    return EXIT_BUSY_TIMEOUT
                time.sleep(LOCK_RETRY_SECONDS)
    finally:
        if not retain_marker:
            try:
                _remove_marker(marker_path, marker_identity)
            except OSError as exc:
                try:
                    marker_retained = marker_path.exists()
                except OSError:
                    marker_retained = True
                raise MarkerCleanupError(
                    "deployment marker cleanup could not be proven",
                    command_exit_code=command_exit_code,
                    marker_retained=marker_retained,
                ) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command_name", required=True)
    hold = subparsers.add_parser("hold")
    hold.add_argument("--job-root", type=Path, required=True)
    hold.add_argument("--active-main-pid", type=int, required=True)
    hold.add_argument("--expected-project-root", type=Path, required=True)
    hold.add_argument("--active-bundle-lock", type=Path, required=True)
    hold.add_argument("--marker", type=Path, required=True)
    hold.add_argument("--timeout", type=float, default=1800.0)
    hold.add_argument("--evidence", type=Path, required=True)
    hold.add_argument(
        "--proc-root",
        type=Path,
        default=Path("/proc"),
        help=argparse.SUPPRESS,
    )
    hold.add_argument("deployment_command", nargs=argparse.REMAINDER)
    return parser


def _emit_error(
    *,
    code: str,
    message: str,
    details: dict[str, object] | None = None,
) -> None:
    error: dict[str, object] = {
        "code": code,
        "message": message,
    }
    if details:
        error.update(details)
    print(
        json.dumps(
            {
                "check": "jato_quiescence",
                "ok": False,
                "error": error,
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = list(args.deployment_command)
    if command and command[0] == "--":
        command = command[1:]
    try:
        return hold_gate(
            job_root=args.job_root.resolve(),
            marker_path=args.marker.resolve(),
            timeout_seconds=args.timeout,
            evidence_path=args.evidence.resolve(),
            runtime_contract=ActiveRuntimeContract(
                main_pid=args.active_main_pid,
                expected_project_root=args.expected_project_root.resolve(),
                active_bundle_lock_path=args.active_bundle_lock.resolve(),
                proc_root=args.proc_root.resolve(),
            ),
            command=command,
        )
    except PostCommandEvidenceError as exc:
        _emit_error(
            code="post_command_evidence_failed",
            message=str(exc),
            details={
                "commandExitCode": exc.command_exit_code,
                "markerRetained": exc.marker_retained,
                "checkpointDecisionRequired": True,
            },
        )
        return EXIT_POST_COMMAND_EVIDENCE_FAILED
    except MarkerCleanupError as exc:
        _emit_error(
            code="marker_cleanup_failed",
            message=str(exc),
            details={
                "commandExitCode": exc.command_exit_code,
                "markerRetained": exc.marker_retained,
                "checkpointDecisionRequired": True,
            },
        )
        return EXIT_MARKER_CLEANUP_FAILED
    except CommandFailedMarkerRetainedError as exc:
        _emit_error(
            code="command_failed_marker_retained",
            message=str(exc),
            details={
                "commandExitCode": exc.command_exit_code,
                "markerRetained": exc.marker_retained,
                "checkpointDecisionRequired": True,
            },
        )
        return EXIT_COMMAND_FAILED_MARKER_RETAINED
    except CommandStartError as exc:
        _emit_error(
            code="command_start_failed",
            message=str(exc),
            details={
                "evidenceCommitted": exc.evidence_committed,
                "markerRetained": exc.marker_retained,
                "checkpointDecisionRequired": False,
            },
        )
        return EXIT_COMMAND_START_FAILED
    except GateError as exc:
        _emit_error(
            code="invalid_state",
            message=str(exc),
        )
        return EXIT_INVALID_STATE


if __name__ == "__main__":
    raise SystemExit(main())
