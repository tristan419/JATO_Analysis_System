#!/usr/bin/env python3
"""Isolated runner for JATO upload digests and queued monthly-update jobs."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = PROJECT_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


DIGEST_RSS_WARNING_BYTES = 1024 * 1024 * 1024
DIGEST_RSS_LIMIT_BYTES = 1536 * 1024 * 1024
DIGEST_RSS_LIMIT_CONSECUTIVE_SAMPLES = 2
DIGEST_RSS_SAMPLE_SECONDS = 1.0
DIGEST_TERMINATE_GRACE_SECONDS = 5.0


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def _append_attempt_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{datetime.now(UTC).isoformat()}] {message}\n")
        handle.flush()


def _read_process_rss_bytes(pid: int) -> int | None:
    """Return actual resident bytes on Linux without adding a dependency."""
    status_path = Path(f"/proc/{pid}/status")
    try:
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                fields = line.split()
                return int(fields[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _cgroup_memory_snapshot() -> dict[str, Any] | None:
    """Read cgroup-v2 memory evidence inherited by the digest worker."""
    try:
        cgroup_line = next(
            line
            for line in Path("/proc/self/cgroup")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.startswith("0::")
        )
        relative_path = cgroup_line.split("::", 1)[1].lstrip("/")
        cgroup_path = Path("/sys/fs/cgroup") / relative_path

        def read_value(name: str) -> str | None:
            try:
                return (cgroup_path / name).read_text(encoding="utf-8").strip()
            except OSError:
                return None

        events: dict[str, int] = {}
        raw_events = read_value("memory.events")
        if raw_events:
            for line in raw_events.splitlines():
                key, value = line.split(maxsplit=1)
                events[key] = int(value)
        return {
            "path": f"/{relative_path}" if relative_path else "/",
            "current": read_value("memory.current"),
            "high": read_value("memory.high"),
            "max": read_value("memory.max"),
            "events": events,
        }
    except (OSError, StopIteration, ValueError):
        return None


def _cgroup_event_delta(
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
) -> dict[str, int]:
    before_events = before.get("events", {}) if isinstance(before, dict) else {}
    after_events = after.get("events", {}) if isinstance(after, dict) else {}
    if not isinstance(before_events, dict) or not isinstance(after_events, dict):
        return {}
    return {
        str(key): max(
            int(after_events.get(key, 0) or 0)
            - int(before_events.get(key, 0) or 0),
            0,
        )
        for key in sorted(set(before_events) | set(after_events))
    }


def _signal_name(return_code: int | None) -> str | None:
    if return_code is None or return_code >= 0:
        return None
    try:
        return signal.Signals(abs(return_code)).name
    except ValueError:
        return f"SIGNAL_{abs(return_code)}"


def _terminate_child(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    deadline = time.monotonic() + DIGEST_TERMINATE_GRACE_SECONDS
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    if process.poll() is None:
        process.kill()


def _supervise_digest_upload(
    *,
    upload_id: str,
    attempt_id: str,
    log_path: Path,
    receipt_path: Path,
) -> int:
    """Wait for one detached digest and leave durable exit/resource evidence."""
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    warning_bytes = int(
        os.getenv(
            "APP_JATO_DIGEST_RSS_WARNING_BYTES",
            str(DIGEST_RSS_WARNING_BYTES),
        )
        or 0
    )
    limit_bytes = int(
        os.getenv(
            "APP_JATO_DIGEST_RSS_LIMIT_BYTES",
            str(DIGEST_RSS_LIMIT_BYTES),
        )
        or 0
    )
    consecutive_limit_samples = max(
        int(
            os.getenv(
                "APP_JATO_DIGEST_RSS_LIMIT_CONSECUTIVE_SAMPLES",
                str(DIGEST_RSS_LIMIT_CONSECUTIVE_SAMPLES),
            )
            or DIGEST_RSS_LIMIT_CONSECUTIVE_SAMPLES
        ),
        1,
    )
    sample_seconds = max(
        float(
            os.getenv(
                "APP_JATO_DIGEST_RSS_SAMPLE_SECONDS",
                str(DIGEST_RSS_SAMPLE_SECONDS),
            )
            or DIGEST_RSS_SAMPLE_SECONDS
        ),
        0.05,
    )
    child: subprocess.Popen[Any] | None = None
    return_code: int | None = None
    peak_rss_bytes = 0
    over_limit_samples = 0
    warning_logged = False
    termination_reason: str | None = None
    supervisor_error: str | None = None
    requested_signal: int | None = None
    cgroup_before = _cgroup_memory_snapshot()

    def request_stop(signum: int, _frame: Any) -> None:
        nonlocal requested_signal
        requested_signal = signum

    previous_handlers: dict[int, Any] = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)

    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        _append_attempt_log(
            log_path,
            f"digest supervisor started upload={upload_id} attempt={attempt_id}",
        )
        child_env = dict(os.environ)
        # RLIMIT_AS measures virtual address reservations rather than resident
        # memory. The supervisor enforces actual RSS while the inherited cgroup
        # remains the host-level hard stop.
        child_env["APP_JATO_MONTHLY_WORKER_MEMORY_LIMIT_BYTES"] = "0"
        child_env["APP_JATO_DIGEST_ATTEMPT_ID"] = attempt_id
        child_env["APP_JATO_DIGEST_SUPERVISOR_PID"] = str(os.getpid())
        child_env["OMP_NUM_THREADS"] = "1"
        child_env["OPENBLAS_NUM_THREADS"] = "1"
        child_env["MKL_NUM_THREADS"] = "1"
        child_env["NUMEXPR_NUM_THREADS"] = "1"
        child_env["MALLOC_ARENA_MAX"] = "2"
        child_env["PYTHONUNBUFFERED"] = "1"
        with log_path.open("ab", buffering=0) as output:
            child = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--digest-upload",
                    upload_id,
                ],
                cwd=PROJECT_ROOT,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                env=child_env,
                close_fds=True,
            )
            _atomic_write_json(
                receipt_path,
                {
                    "schemaVersion": 1,
                    "status": "running",
                    "uploadId": upload_id,
                    "attemptId": attempt_id,
                    "supervisorPid": os.getpid(),
                    "workerPid": child.pid,
                    "startedAt": started_at.isoformat(),
                    "logPath": str(log_path),
                    "rssWarningBytes": warning_bytes,
                    "rssLimitBytes": limit_bytes,
                    "cgroupBefore": cgroup_before,
                },
            )
            while True:
                polled = child.poll()
                rss_bytes = _read_process_rss_bytes(child.pid)
                if rss_bytes is not None:
                    peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
                    if warning_bytes > 0 and rss_bytes >= warning_bytes:
                        if not warning_logged:
                            _append_attempt_log(
                                log_path,
                                "digest RSS warning: "
                                f"rss={rss_bytes} threshold={warning_bytes}",
                            )
                            warning_logged = True
                    if limit_bytes > 0 and rss_bytes >= limit_bytes:
                        over_limit_samples += 1
                    else:
                        over_limit_samples = 0
                if requested_signal is not None and polled is None:
                    termination_reason = (
                        f"supervisor_{signal.Signals(requested_signal).name.lower()}"
                    )
                    _terminate_child(child)
                    polled = child.poll()
                elif (
                    polled is None
                    and limit_bytes > 0
                    and over_limit_samples >= consecutive_limit_samples
                ):
                    termination_reason = "rss_limit"
                    _append_attempt_log(
                        log_path,
                        "digest RSS limit reached: "
                        f"rss={rss_bytes} limit={limit_bytes} "
                        f"samples={over_limit_samples}",
                    )
                    _terminate_child(child)
                    polled = child.poll()
                if polled is not None:
                    return_code = child.wait()
                    break
                time.sleep(sample_seconds)
    except BaseException:
        supervisor_error = traceback.format_exc(limit=12)
        _append_attempt_log(log_path, supervisor_error.rstrip())
        if child is not None:
            _terminate_child(child)
            return_code = child.wait()
    finally:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)

    finished_at = datetime.now(UTC)
    cgroup_after = _cgroup_memory_snapshot()
    event_delta = _cgroup_event_delta(cgroup_before, cgroup_after)
    receipt = {
        "schemaVersion": 1,
        "status": "finished",
        "uploadId": upload_id,
        "attemptId": attempt_id,
        "supervisorPid": os.getpid(),
        "workerPid": child.pid if child is not None else None,
        "startedAt": started_at.isoformat(),
        "finishedAt": finished_at.isoformat(),
        "elapsedSeconds": round(time.monotonic() - started_monotonic, 3),
        "returnCode": return_code,
        "signalNumber": abs(return_code) if return_code is not None and return_code < 0 else None,
        "signalName": _signal_name(return_code),
        "terminationReason": termination_reason,
        "peakRssBytes": peak_rss_bytes,
        "rssWarningBytes": warning_bytes,
        "rssLimitBytes": limit_bytes,
        "rssWarningExceeded": warning_logged,
        "cgroupBefore": cgroup_before,
        "cgroupAfter": cgroup_after,
        "cgroupEventDelta": event_delta,
        "oomKillDelta": int(event_delta.get("oom_kill", 0) or 0),
        "supervisorError": supervisor_error,
        "logPath": str(log_path),
    }
    _atomic_write_json(receipt_path, receipt)
    _append_attempt_log(
        log_path,
        "digest supervisor finished "
        f"returnCode={return_code} signal={receipt['signalName']} "
        f"peakRssBytes={peak_rss_bytes}",
    )
    return 0 if supervisor_error is None else 70


def _apply_resource_limits() -> None:
    try:
        import resource
    except ImportError:
        return
    memory_limit = int(
        os.getenv("APP_JATO_MONTHLY_WORKER_MEMORY_LIMIT_BYTES", "0") or 0
    )
    if memory_limit > 0:
        if sys.platform == "darwin":
            # macOS exposes RLIMIT_AS but rejects lowering it. Production is
            # Linux/cgroup-backed; local workers must still be able to run.
            pass
        else:
            _set_required_resource_limit(
                resource,
                resource.RLIMIT_AS,
                memory_limit,
                label="address space",
            )
    cpu_seconds = int(
        os.getenv("APP_JATO_MONTHLY_WORKER_CPU_LIMIT_SECONDS", "0") or 0
    )
    if cpu_seconds > 0:
        _set_required_resource_limit(
            resource,
            resource.RLIMIT_CPU,
            cpu_seconds,
            label="CPU seconds",
        )


def _set_required_resource_limit(
    resource_module: object,
    resource_kind: int,
    requested_limit: int,
    *,
    label: str,
) -> None:
    """Apply a worker limit or stop before any mutable task work begins."""
    current_soft, current_hard = resource_module.getrlimit(resource_kind)
    infinity = resource_module.RLIM_INFINITY
    effective_limit = (
        min(requested_limit, int(current_hard))
        if current_hard != infinity
        else requested_limit
    )
    if effective_limit <= 0:
        raise RuntimeError(f"JATO monthly worker {label} limit is unavailable.")
    try:
        resource_module.setrlimit(
            resource_kind,
            (effective_limit, effective_limit),
        )
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"Unable to enforce JATO monthly worker {label} limit."
        ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run isolated JATO monthly-update work."
    )
    parser.add_argument(
        "--digest-upload",
        default=None,
        help="Assemble and inspect exactly one upload session, then exit.",
    )
    parser.add_argument(
        "--supervise-digest-upload",
        default=None,
        help="Supervise exactly one detached digest and persist its exit receipt.",
    )
    parser.add_argument("--attempt-id", default=None)
    parser.add_argument("--attempt-log", default=None)
    parser.add_argument("--attempt-receipt", default=None)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process at most one queued monthly-update job and exit.",
    )
    parser.add_argument(
        "--drain",
        action="store_true",
        help="Process queued jobs until the queue is empty, then exit.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=3.0,
        help="Idle poll interval for service mode.",
    )
    args = parser.parse_args()
    if args.supervise_digest_upload:
        if not args.attempt_id or not args.attempt_log or not args.attempt_receipt:
            parser.error(
                "digest supervisor requires --attempt-id, --attempt-log, "
                "and --attempt-receipt"
            )
        return _supervise_digest_upload(
            upload_id=str(args.supervise_digest_upload),
            attempt_id=str(args.attempt_id),
            log_path=Path(args.attempt_log),
            receipt_path=Path(args.attempt_receipt),
        )
    _apply_resource_limits()

    from app.services.jato_monthly_update_service import (
        run_jato_monthly_update_upload_digest,
        run_jato_monthly_update_worker_once,
    )

    if args.digest_upload:
        run_jato_monthly_update_upload_digest(str(args.digest_upload))
        return 0

    while True:
        result = run_jato_monthly_update_worker_once()
        if args.once:
            return 0
        if (
            args.drain
            and result.get("processedJobId") is None
            and result.get("skipped") != "worker_lock_held"
        ):
            return 0
        if result.get("processedJobId") is None:
            time.sleep(max(args.poll_seconds, 0.5))


if __name__ == "__main__":
    try:
        exit_code = main()
    except MemoryError:
        traceback.print_exc()
        exit_code = 72
    except Exception:
        traceback.print_exc()
        exit_code = 70
    raise SystemExit(exit_code)
