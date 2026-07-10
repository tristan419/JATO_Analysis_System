#!/usr/bin/env python3
"""Isolated runner for queued JATO monthly-update jobs.

This process is intentionally separate from FastAPI. The web application only
persists queued jobs; systemd runs this worker with lower CPU/IO priority and
an operator-configured memory ceiling.
"""

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
    """Apply only explicitly configured process limits; zero means unset."""
    try:
        import resource
    except ImportError:
        return

    memory_limit = int(os.getenv("APP_JATO_MONTHLY_WORKER_MEMORY_LIMIT_BYTES", "0") or 0)
    if memory_limit > 0:
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
    cpu_seconds = int(os.getenv("APP_JATO_MONTHLY_WORKER_CPU_LIMIT_SECONDS", "0") or 0)
    if cpu_seconds > 0:
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run isolated JATO monthly-update jobs.")
    parser.add_argument("--once", action="store_true", help="Process at most one queued job and exit.")
    parser.add_argument("--poll-seconds", type=float, default=3.0, help="Idle poll interval.")
    args = parser.parse_args()
    _apply_resource_limits()

    from app.services.jato_monthly_update_service import run_jato_monthly_update_worker_once

    while True:
        result = run_jato_monthly_update_worker_once()
        if args.once:
            return 0
        if result.get("processedJobId") is None:
            time.sleep(max(args.poll_seconds, 0.5))


if __name__ == "__main__":
    raise SystemExit(main())
