#!/usr/bin/env python3
"""Write standard Hermes pipeline status JSON records."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_status(value: Any) -> str:
    status = str(value or "unknown").strip().lower()
    if status in {"failure", "failed", "error"}:
        return "failed"
    if status in {"degraded", "partial_success"}:
        return "degraded"
    if status in {"success", "missing", "skipped", "unknown"}:
        return status
    return "unknown"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_pipeline_status(
    *,
    pipeline_id: str,
    status: str,
    last_run_at: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    exit_code: int | None = None,
    duration_seconds: int = 0,
    records_processed: int = 0,
    failed_count: int = 0,
    warning_count: int = 0,
    artifact_refs: list[str] | None = None,
    source: str = "",
    message: str = "",
    extra: dict[str, Any] | None = None,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    pipeline_id = str(pipeline_id).strip()
    if not pipeline_id:
        raise ValueError("pipeline_id is required")
    finished_at = finished_at or _utc_now()
    record = {
        "pipelineId": pipeline_id,
        "status": _normalize_status(status),
        "lastRunAt": last_run_at or finished_at,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "exitCode": exit_code,
        "durationSeconds": _coerce_int(duration_seconds),
        "recordsProcessed": _coerce_int(records_processed),
        "failedCount": _coerce_int(failed_count),
        "warningCount": _coerce_int(warning_count),
        "artifactRefs": artifact_refs or [],
        "source": source,
        "message": message,
    }
    if extra:
        record.update(extra)

    status_dir = repo_root / "hermes" / "reports" / "pipeline_status"
    status_dir.mkdir(parents=True, exist_ok=True)
    path = status_dir / f"{pipeline_id}.json"
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n")
    tmp_path.replace(path)
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write Hermes pipeline status JSON.")
    parser.add_argument("pipeline_id")
    parser.add_argument("--status", required=True)
    parser.add_argument("--last-run-at")
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--records-processed", type=int, default=0)
    parser.add_argument("--failed-count", type=int, default=0)
    parser.add_argument("--warning-count", type=int, default=0)
    parser.add_argument("--artifact-ref", action="append", default=[])
    parser.add_argument("--source", default="")
    parser.add_argument("--message", default="")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    args = parser.parse_args(argv)

    write_pipeline_status(
        pipeline_id=args.pipeline_id,
        status=args.status,
        last_run_at=args.last_run_at,
        started_at=args.started_at,
        finished_at=args.finished_at,
        exit_code=args.exit_code,
        duration_seconds=args.duration_seconds,
        records_processed=args.records_processed,
        failed_count=args.failed_count,
        warning_count=args.warning_count,
        artifact_refs=args.artifact_ref,
        source=args.source,
        message=args.message,
        repo_root=Path(args.repo_root).expanduser().resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
