#!/usr/bin/env python3
"""Isolated runner for JATO upload digests and queued monthly-update jobs."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = PROJECT_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


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
    raise SystemExit(main())
