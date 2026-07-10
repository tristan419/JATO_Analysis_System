#!/usr/bin/env python3
"""Server-local break-glass operations for JATO monthly update jobs.

Run this script only through an approved SSH/bastion session on the production
host. It imports the same service functions as FastAPI and cannot write active
data unless the caller gives both --execute and an operation-specific confirm.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = PROJECT_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def _actor() -> str:
    return f"ops:{os.getenv('USER') or 'unknown'}"


def _require_execute(args: argparse.Namespace) -> None:
    if not args.execute:
        raise SystemExit("This mutation requires --execute. Dry-run made no changes.")


def _load_service():
    from app.services import jato_monthly_update_service as service

    return service


def _require_expected_job(service: Any, args: argparse.Namespace) -> dict[str, Any]:
    if args.confirm_job != args.job_id:
        raise SystemExit("--confirm-job must exactly match --job-id.")
    job = service.get_jato_monthly_update_job(args.job_id)
    if args.expected_country and str(job.get("country") or "") != args.expected_country:
        raise SystemExit("Expected country does not match the queued job.")
    if args.expected_month and str(job.get("month") or "") != args.expected_month:
        raise SystemExit("Expected month does not match the queued job.")
    upload = job.get("upload") if isinstance(job.get("upload"), dict) else {}
    if args.expected_sha256 and str(upload.get("sha256") or "") != args.expected_sha256.lower():
        raise SystemExit("Expected SHA-256 does not match the queued job.")
    return job


def main() -> int:
    parser = argparse.ArgumentParser(description="Approved server-local JATO monthly update operations.")
    parser.add_argument("--execute", action="store_true", help="Permit a mutating operation.")
    parser.add_argument("--job-id")
    parser.add_argument("--confirm-job")
    parser.add_argument("--expected-country")
    parser.add_argument("--expected-month")
    parser.add_argument("--expected-sha256")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor")
    find_parser = subparsers.add_parser("find-upload")
    find_parser.add_argument("--sha256", required=True)
    subparsers.add_parser("status")
    subparsers.add_parser("review")
    approve_parser = subparsers.add_parser("approve")
    approve_parser.add_argument("--decision", choices=("approve", "reject"), required=True)
    approve_parser.add_argument("--note", default="")
    subparsers.add_parser("retry")
    subparsers.add_parser("publish")
    subparsers.add_parser("verify")
    subparsers.add_parser("rollback")
    args = parser.parse_args()
    service = _load_service()

    if args.command == "doctor":
        _print({"worker": service.get_jato_monthly_update_worker_status(), "maintenance": service.get_jato_monthly_update_maintenance_status()})
        return 0
    if args.command == "find-upload":
        sha = args.sha256.strip().lower()
        rows = [
            item
            for item in service.list_jato_monthly_update_jobs(limit=50)["items"]
            if isinstance(item.get("upload"), dict) and str(item["upload"].get("sha256") or "").lower() == sha
        ]
        _print({"items": rows})
        return 0
    if not args.job_id:
        raise SystemExit("--job-id is required for this command.")
    if args.command == "status":
        _print(service.get_jato_monthly_update_job(args.job_id))
        return 0
    if args.command == "review":
        _print(service.get_jato_monthly_update_review(args.job_id))
        return 0
    if args.command == "verify":
        job = service.get_jato_monthly_update_job(args.job_id)
        review = service.get_jato_monthly_update_review(args.job_id)
        _print({"job": job, "review": review, "worker": service.get_jato_monthly_update_worker_status()})
        return 0

    _require_execute(args)
    _require_expected_job(service, args)
    if args.command == "approve":
        _print(service.approve_jato_monthly_update_review(job_id=args.job_id, triggered_by=_actor(), decision=args.decision, note=args.note))
        return 0
    if args.command == "retry":
        _print(service.retry_failed_jato_monthly_update_job(source_job_id=args.job_id, triggered_by=_actor()))
        return 0
    if args.command == "publish":
        _print(service.publish_jato_monthly_update_job(job_id=args.job_id, triggered_by=_actor()))
        return 0
    if args.command == "rollback":
        _print(service.rollback_jato_monthly_update_job(job_id=args.job_id, triggered_by=_actor()))
        return 0
    raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
