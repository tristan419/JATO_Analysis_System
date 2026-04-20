from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, UploadFile

from app.core.config import (
    JATO_MONTHLY_UPDATE_JOB_ROOT,
    JATO_MONTHLY_UPDATE_UPLOAD_CHUNK_SIZE_BYTES,
    PROJECT_ROOT,
)

MONTHLY_UPDATE_JOB_ROOT = JATO_MONTHLY_UPDATE_JOB_ROOT
RAW_DATA_ROOT = PROJECT_ROOT / "01_RAW_DATA"
BASELINE_ROOT = RAW_DATA_ROOT / "baseline"
PATCHES_ROOT = RAW_DATA_ROOT / "patches"
HISTORY_ARCHIVE_ROOT = RAW_DATA_ROOT / "historyDataArchive"
PREPARE_SCRIPT_PATH = (
    PROJECT_ROOT / "03_Scripts" / "data_pipeline" / "prepare_monthly_raw_update.py"
)
STATE_FILENAME = "job_state.json"
LOG_FILENAME = "job.log"
UPLOAD_STATE_FILENAME = "upload_state.json"
MONTH_PATTERN = re.compile(r"(20\d{2})[-./]?(0?[1-9]|1[0-2])")
CODE_BLOCK_PATTERN = re.compile(r"```bash\s*(.*?)\s*```", re.DOTALL)
ALLOWED_UPLOAD_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
UPLOAD_CHUNK_SIZE_BYTES = JATO_MONTHLY_UPDATE_UPLOAD_CHUNK_SIZE_BYTES
_WRITE_LOCK = threading.Lock()
_RUNNING_THREADS: dict[str, threading.Thread] = {}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _relative_to_project(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def _project_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    candidate = Path(value)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _job_dir(job_id: str) -> Path:
    return MONTHLY_UPDATE_JOB_ROOT / job_id


def _job_state_path(job_id: str) -> Path:
    return _job_dir(job_id) / STATE_FILENAME


def _job_log_path(job_id: str) -> Path:
    return _job_dir(job_id) / LOG_FILENAME


def _processed_data_root() -> Path:
    return PROJECT_ROOT / "04_Processed_data"


def _active_data_paths() -> dict[str, Path]:
    processed_root = _processed_data_root()
    return {
        "parquet": processed_root / "jato_full_archive.parquet",
        "manifest": processed_root / "manifest.json",
        "partition": processed_root / "partitioned_dataset_v1",
        "fingerprint": processed_root / "dataset_fingerprint.json",
        "refreshReport": processed_root / "refresh_job_report.json",
        "backupRoot": processed_root / ".refresh_backups",
    }


def _upload_session_root() -> Path:
    return MONTHLY_UPDATE_JOB_ROOT / "_upload_sessions"


def _upload_session_dir(upload_id: str) -> Path:
    return _upload_session_root() / upload_id


def _upload_session_state_path(upload_id: str) -> Path:
    return _upload_session_dir(upload_id) / UPLOAD_STATE_FILENAME


def _upload_session_chunk_dir(upload_id: str) -> Path:
    return _upload_session_dir(upload_id) / "chunks"


def _upload_session_assembled_path(upload_id: str, filename: str) -> Path:
    return _upload_session_dir(upload_id) / "assembled" / filename


def _iter_upload_session_payloads() -> list[dict[str, Any]]:
    _upload_session_root().mkdir(parents=True, exist_ok=True)
    payloads: list[dict[str, Any]] = []
    for state_path in _upload_session_root().glob(f"*/{UPLOAD_STATE_FILENAME}"):
        try:
            payloads.append(_read_json(state_path))
        except Exception:
            continue
    return payloads


def _list_job_state_payloads() -> list[dict[str, Any]]:
    MONTHLY_UPDATE_JOB_ROOT.mkdir(parents=True, exist_ok=True)
    payloads: list[dict[str, Any]] = []
    for state_path in MONTHLY_UPDATE_JOB_ROOT.glob(f"*/{STATE_FILENAME}"):
        try:
            payloads.append(_read_json(state_path))
        except Exception:
            continue
    return payloads


def _load_upload_session(upload_id: str) -> dict[str, Any]:
    path = _upload_session_state_path(upload_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="上传会话不存在或已失效。")
    return _read_json(path)


def _persist_upload_session(payload: dict[str, Any]) -> None:
    upload_id = str(payload["uploadId"])
    payload["updatedAt"] = _utc_now().isoformat()
    with _WRITE_LOCK:
        _write_json(_upload_session_state_path(upload_id), payload)


def _find_upload_session_by_resume_key(
    *,
    resume_key: str,
    filename: str,
    size_bytes: int,
) -> dict[str, Any] | None:
    if not resume_key:
        return None
    for payload in _iter_upload_session_payloads():
        if str(payload.get("resumeKey", "")) != resume_key:
            continue
        if str(payload.get("filename", "")) != filename:
            continue
        if int(payload.get("sizeBytes", 0) or 0) != size_bytes:
            continue
        if str(payload.get("status", "")) not in {"pending", "uploading", "completed"}:
            continue
        return payload
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object: {path}")
    return payload


def _load_job_state(job_id: str) -> dict[str, Any]:
    path = _job_state_path(job_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="JATO monthly update job not found")
    return _read_json(path)


def _persist_job_state(payload: dict[str, Any]) -> None:
    job_id = str(payload["jobId"])
    payload["updatedAt"] = _utc_now().isoformat()
    with _WRITE_LOCK:
        _write_json(_job_state_path(job_id), payload)


def _normalize_month(value: str) -> str:
    match = MONTH_PATTERN.fullmatch(value.strip())
    if match is None:
        raise HTTPException(status_code=400, detail="月份格式无效，需要 YYYY-MM，例如 2026-02")
    return f"{match.group(1)}-{int(match.group(2)):02d}"


def _infer_month_token(value: str) -> str | None:
    dotted_match = re.findall(
        r"(?<!\d)(20\d{2})[._-](0?[1-9]|1[0-2])(?!\d)", value
    )
    if dotted_match:
        year, month = max(dotted_match)
        return f"{year}-{int(month):02d}"
    compact_match = re.findall(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?!\d)", value)
    if compact_match:
        year, month = max(compact_match)
        return f"{year}-{int(month):02d}"
    return None


def _normalize_filename(value: str | None) -> str:
    candidate = Path(value or "jato-update.xlsx").name.strip()
    if not candidate or candidate in {".", ".."}:
        return "jato-update.xlsx"
    return candidate


def _validate_upload(file: UploadFile) -> str:
    return _validate_upload_filename(file.filename or "jato-update.xlsx")


def _validate_upload_filename(filename: str) -> str:
    normalized = _normalize_filename(filename)
    suffix = Path(normalized).suffix.lower()
    if suffix not in ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail="JATO monthly update 仅支持 Excel 文件（.xlsx/.xlsm/.xls）。",
        )
    return normalized


def _normalize_size_bytes(value: Any) -> int:
    try:
        size_bytes = int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="上传文件大小无效。") from None
    if size_bytes <= 0:
        raise HTTPException(status_code=400, detail="上传文件为空，无法启动月更任务。")
    return size_bytes


def _normalize_sha256(value: Any, *, detail: str) -> str:
    candidate = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", candidate):
        raise HTTPException(status_code=400, detail=detail)
    return candidate


def _sha256_hex_for_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_hex_for_path(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _chunk_file_name(part_number: int) -> str:
    return f"{part_number:05d}.part"


def _expected_chunk_size(*, size_bytes: int, chunk_size: int, total_chunks: int, part_number: int) -> int:
    if part_number < total_chunks:
        return chunk_size
    consumed = chunk_size * (total_chunks - 1)
    return size_bytes - consumed


def _collect_uploaded_chunk_numbers(upload_id: str) -> list[int]:
    chunk_dir = _upload_session_chunk_dir(upload_id)
    if not chunk_dir.exists():
        return []
    chunk_numbers: list[int] = []
    for path in chunk_dir.glob("*.part"):
        try:
            chunk_numbers.append(int(path.stem))
        except ValueError:
            continue
    chunk_numbers.sort()
    return chunk_numbers


def _uploaded_chunk_bytes(upload_id: str) -> int:
    chunk_dir = _upload_session_chunk_dir(upload_id)
    if not chunk_dir.exists():
        return 0
    return sum(
        path.stat().st_size
        for path in chunk_dir.glob("*.part")
        if path.is_file()
    )


def _serialize_upload_session(payload: dict[str, Any]) -> dict[str, Any]:
    upload_id = str(payload["uploadId"])
    persisted_chunks = payload.get("receivedChunks")
    if isinstance(persisted_chunks, list) and persisted_chunks:
        received_chunks = [int(item) for item in persisted_chunks]
    else:
        received_chunks = _collect_uploaded_chunk_numbers(upload_id)
    persisted_uploaded_bytes = payload.get("uploadedBytes")
    uploaded_bytes = (
        int(persisted_uploaded_bytes)
        if persisted_uploaded_bytes is not None
        else _uploaded_chunk_bytes(upload_id)
    )
    return {
        "uploadId": upload_id,
        "filename": str(payload.get("filename", "")),
        "sizeBytes": int(payload.get("sizeBytes", 0)),
        "chunkSize": int(payload.get("chunkSize", UPLOAD_CHUNK_SIZE_BYTES)),
        "totalChunks": int(payload.get("totalChunks", 0)),
        "receivedChunkCount": len(received_chunks),
        "receivedChunks": received_chunks,
        "uploadedBytes": uploaded_bytes,
        "status": str(payload.get("status", "pending")),
        "createdAt": payload.get("createdAt"),
        "updatedAt": payload.get("updatedAt"),
        "completedAt": payload.get("completedAt"),
        "assembledPath": payload.get("assembledPath"),
        "resumeKey": payload.get("resumeKey"),
        "fileSha256": payload.get("fileSha256"),
        "triggeredBy": payload.get("triggeredBy"),
    }


def _ensure_unique_archive_path(target: Path) -> Path:
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        candidate = target.with_name(f"{stem}-{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _move_to_archive(path: Path, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = _ensure_unique_archive_path(archive_dir / path.name)
    shutil.move(str(path), str(target))
    return target


def _latest_baseline_file(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return max(
        paths,
        key=lambda item: (
            _infer_month_token(item.stem) or "0000-00",
            item.stat().st_mtime,
            item.name,
        ),
    )


def _list_supported_excel_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.glob("*")
        if path.is_file()
        and path.suffix.lower() in ALLOWED_UPLOAD_EXTENSIONS
        and not path.name.startswith("~$")
    )


def _resolve_latest_baseline() -> tuple[Path | None, str | None]:
    active_baseline = _latest_baseline_file(_list_supported_excel_files(BASELINE_ROOT))
    if active_baseline is not None:
        return active_baseline, "active"

    archived_root = HISTORY_ARCHIVE_ROOT / "baseline"
    archived_baseline = _latest_baseline_file(_list_supported_excel_files(archived_root))
    if archived_baseline is not None:
        return archived_baseline, "archive"

    return None, None


def _require_latest_baseline() -> tuple[Path, str]:
    baseline_path, baseline_source = _resolve_latest_baseline()
    if baseline_path is None or baseline_source is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "未找到可用 baseline。请先在 01_RAW_DATA/baseline/ 放入当前激活 baseline；"
                "如误清理，可从 01_RAW_DATA/historyDataArchive/baseline/ 恢复后重试。"
            ),
        )
    return baseline_path, baseline_source


def _latest_patch_dir(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return max(
        paths,
        key=lambda item: (
            _infer_month_token(item.name) or "0000-00",
            item.stat().st_mtime,
            item.name,
        ),
    )


def _append_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")


def _tail_text(path: Path, *, max_lines: int = 160, max_chars: int = 20000) -> str | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    tail = "\n".join(lines[-max_lines:])
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail


def _read_text_if_exists(project_relative_path: str | None) -> str | None:
    if not project_relative_path:
        return None
    path = PROJECT_ROOT / project_relative_path
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8", errors="replace")


def _extract_flag_value(command_args: list[str], flag: str) -> str | None:
    try:
        index = command_args.index(flag)
    except ValueError:
        return None
    next_index = index + 1
    if next_index >= len(command_args):
        return None
    return command_args[next_index]


def _parse_plan_markdown(plan_path: Path) -> dict[str, Any]:
    text = plan_path.read_text(encoding="utf-8")
    commands = [
        block.strip()
        for block in CODE_BLOCK_PATTERN.findall(text)
        if block.strip()
    ]
    if len(commands) < 2:
        raise RuntimeError("monthly_update_plan.md 缺少 raw compare / refresh 命令")

    metadata: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- 对比:"):
            metadata["compareId"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- baseline:"):
            metadata["baselinePath"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- patch:"):
            metadata["patchPath"] = stripped.split(":", 1)[1].strip()

    compare_command = commands[0]
    refresh_command = commands[1]
    compare_args = shlex.split(compare_command)
    refresh_args = shlex.split(refresh_command)

    review_dir = _extract_flag_value(compare_args, "--output-dir")
    refresh_report = _extract_flag_value(refresh_args, "--report")
    staging_output = _extract_flag_value(refresh_args, "--output")
    manifest_path = _extract_flag_value(refresh_args, "--manifest")
    partition_output = _extract_flag_value(refresh_args, "--partition-output")
    fingerprint_path = _extract_flag_value(refresh_args, "--fingerprint")

    return {
        "compareId": metadata.get("compareId", ""),
        "baselinePath": metadata.get("baselinePath"),
        "patchPath": metadata.get("patchPath"),
        "compareCommand": compare_command,
        "refreshCommand": refresh_command,
        "reviewDir": review_dir,
        "rawCompareReportPath": (
            f"{review_dir}/raw_compare_report.json" if review_dir else None
        ),
        "stagingOutputPath": staging_output,
        "manifestPath": manifest_path,
        "partitionOutputPath": partition_output,
        "refreshReportPath": refresh_report,
        "fingerprintPath": fingerprint_path,
    }


def _command_to_args(command: str) -> list[str]:
    args = shlex.split(command)
    if not args:
        raise RuntimeError("命令为空")
    if args[0] == "python":
        args[0] = sys.executable
    return args


def _read_json_if_exists(project_relative_path: str | None) -> dict[str, Any] | None:
    if not project_relative_path:
        return None
    path = PROJECT_ROOT / project_relative_path
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _summarize_raw_compare_report(report: dict[str, Any]) -> dict[str, Any]:
    findings = report.get("reviewFindings")
    finding_list = findings if isinstance(findings, list) else []
    freshness_entries = report.get("countryFreshnessSummary")
    freshness_list = freshness_entries if isinstance(freshness_entries, list) else []
    scope_summary = report.get("countryScopeSummary")
    scope = scope_summary if isinstance(scope_summary, dict) else {}

    blocker_count = 0
    review_count = 0
    info_count = 0
    for item in finding_list:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity", "")).lower()
        if severity == "blocker":
            blocker_count += 1
        elif severity == "review":
            review_count += 1
        elif severity == "info":
            info_count += 1

    advanced_country_count = 0
    regressed_country_count = 0
    new_country_count = 0
    missing_country_count = 0
    for item in freshness_list:
        if not isinstance(item, dict):
            continue
        status = str(item.get("freshnessStatus", ""))
        if status == "advanced":
            advanced_country_count += 1
        elif status == "regressed":
            regressed_country_count += 1
        elif status == "new_country":
            new_country_count += 1
        elif status == "missing_in_candidate":
            missing_country_count += 1

    added_countries = scope.get("addedCountries")
    removed_countries = scope.get("removedCountries")

    return {
        "compareId": str(report.get("compareId", "")),
        "decisionSuggestion": str(report.get("decisionSuggestion", "")),
        "compareKeyMode": str(report.get("compareKeyMode", "")),
        "compareKeyColumns": (
            [str(item) for item in report.get("compareKeyColumns", [])]
            if isinstance(report.get("compareKeyColumns"), list)
            else []
        ),
        "blockerCount": blocker_count,
        "reviewCount": review_count,
        "infoCount": info_count,
        "advancedCountryCount": advanced_country_count,
        "regressedCountryCount": regressed_country_count,
        "newCountryCount": new_country_count,
        "missingCountryCount": missing_country_count,
        "addedCountryCount": len(added_countries) if isinstance(added_countries, list) else 0,
        "removedCountryCount": (
            len(removed_countries) if isinstance(removed_countries, list) else 0
        ),
    }


def _summarize_refresh_report(report: dict[str, Any]) -> dict[str, Any]:
    full_manifest = report.get("fullManifest")
    full = full_manifest if isinstance(full_manifest, dict) else {}
    partition_manifest = report.get("partitionManifest")
    partition = partition_manifest if isinstance(partition_manifest, dict) else {}
    incremental_payload = report.get("incremental")
    incremental = incremental_payload if isinstance(incremental_payload, dict) else {}
    regression_payload = incremental.get("regression")
    regression = regression_payload if isinstance(regression_payload, dict) else {}
    merge_payload = regression.get("mergeKeyRegression")
    merge = merge_payload if isinstance(merge_payload, dict) else {}

    return {
        "jobStatus": str(report.get("jobStatus", "")),
        "jobElapsedSeconds": float(report.get("jobElapsedSeconds", 0) or 0),
        "rowCount": int(full.get("rows", 0) or 0),
        "columnCount": int(full.get("columns", 0) or 0),
        "partitionCount": int(partition.get("parquetFileCount", 0) or 0),
        "changedRows": int(regression.get("changedRows", 0) or 0),
        "changedCountryCount": int(regression.get("changedCountryCount", 0) or 0),
        "fingerprintMatched": bool(incremental.get("fingerprintMatched")),
        "fingerprintUpdated": bool(incremental.get("fingerprintUpdated")),
        "conflictGroupCount": int(merge.get("conflictGroupCount", 0) or 0),
        "conflictRowCount": int(merge.get("conflictRowCount", 0) or 0),
    }


def _serialize_job_state(
    payload: dict[str, Any],
    *,
    include_log_tail: bool,
) -> dict[str, Any]:
    item = {
        "jobId": str(payload.get("jobId", "")),
        "month": str(payload.get("month", "")),
        "status": str(payload.get("status", "")),
        "phase": str(payload.get("phase", "")),
        "triggeredBy": str(payload.get("triggeredBy", "")),
        "createdAt": str(payload.get("createdAt", "")),
        "updatedAt": str(payload.get("updatedAt", "")),
        "startedAt": payload.get("startedAt"),
        "finishedAt": payload.get("finishedAt"),
        "error": payload.get("error"),
        "upload": payload.get("upload") if isinstance(payload.get("upload"), dict) else None,
        "plan": payload.get("plan") if isinstance(payload.get("plan"), dict) else None,
        "artifacts": (
            payload.get("artifacts")
            if isinstance(payload.get("artifacts"), dict)
            else None
        ),
        "summaries": (
            payload.get("summaries")
            if isinstance(payload.get("summaries"), dict)
            else None
        ),
        "logPath": payload.get("logPath"),
        "publication": (
            payload.get("publication")
            if isinstance(payload.get("publication"), dict)
            else None
        ),
    }
    if include_log_tail:
        log_path = _job_log_path(item["jobId"])
        item["logTail"] = _tail_text(log_path)
    return item


def _sanitize_review_finding(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    metrics = item.get("metrics")
    return {
        "severity": str(item.get("severity", "")),
        "scope": str(item.get("scope", "")),
        "target": str(item.get("target", "")),
        "ruleId": str(item.get("ruleId", "")),
        "message": str(item.get("message", "")),
        "metrics": metrics if isinstance(metrics, dict) else {},
        "suggestedAction": str(item.get("suggestedAction", "")),
    }


def _sanitize_conflict_sample(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    business_key = item.get("businessKey")
    changed_fields = item.get("changedFields")
    return {
        "country": str(item.get("country", "")),
        "businessKey": business_key if isinstance(business_key, dict) else {},
        "oldValueDigest": (
            None
            if item.get("oldValueDigest") in {None, ""}
            else str(item.get("oldValueDigest"))
        ),
        "newValueDigest": (
            None
            if item.get("newValueDigest") in {None, ""}
            else str(item.get("newValueDigest"))
        ),
        "changedFields": (
            [str(field) for field in changed_fields]
            if isinstance(changed_fields, list)
            else []
        ),
    }


def _sanitize_overlap_change_summary(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    return {
        "country": str(item.get("country", "")),
        "compareMonths": (
            [str(month) for month in item.get("compareMonths", [])]
            if isinstance(item.get("compareMonths"), list)
            else []
        ),
        "compareKeyColumns": (
            [str(column) for column in item.get("compareKeyColumns", [])]
            if isinstance(item.get("compareKeyColumns"), list)
            else []
        ),
        "addedRecordCount": int(item.get("addedRecordCount", 0) or 0),
        "removedRecordCount": int(item.get("removedRecordCount", 0) or 0),
        "changedRecordCount": int(item.get("changedRecordCount", 0) or 0),
        "unchangedRecordCount": int(item.get("unchangedRecordCount", 0) or 0),
        "changeRate": float(item.get("changeRate", 0) or 0),
        "sampleAddedKeys": (
            [entry for entry in item.get("sampleAddedKeys", []) if isinstance(entry, dict)]
            if isinstance(item.get("sampleAddedKeys"), list)
            else []
        ),
        "sampleRemovedKeys": (
            [entry for entry in item.get("sampleRemovedKeys", []) if isinstance(entry, dict)]
            if isinstance(item.get("sampleRemovedKeys"), list)
            else []
        ),
        "sampleChangedKeys": (
            [entry for entry in item.get("sampleChangedKeys", []) if isinstance(entry, dict)]
            if isinstance(item.get("sampleChangedKeys"), list)
            else []
        ),
    }


def _require_no_running_monthly_update_jobs(*, excluding_job_id: str | None = None) -> None:
    running_jobs = [
        str(payload.get("jobId", ""))
        for payload in _list_job_state_payloads()
        if str(payload.get("status", "")) in {"queued", "running"}
        and str(payload.get("jobId", "")) != str(excluding_job_id or "")
    ]
    if running_jobs:
        raise HTTPException(
            status_code=409,
            detail="存在运行中的月更任务，请等待完成后再执行 review / publish。",
        )


def get_jato_monthly_update_review(job_id: str) -> dict[str, Any]:
    payload = _load_job_state(job_id)
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise HTTPException(status_code=409, detail="当前任务暂无可 review 的 compare 产物。")

    raw_compare_report_path = str(artifacts.get("rawCompareReportPath") or "").strip()
    review_dir = str(artifacts.get("reviewDir") or "").strip()
    raw_compare_report = _read_json_if_exists(raw_compare_report_path)
    if raw_compare_report is None:
        raise HTTPException(status_code=409, detail="当前任务暂无可 review 的 compare 报告。")

    checklist_markdown = _read_text_if_exists(
        f"{review_dir}/review_checklist.md" if review_dir else None
    )
    conflict_payload = _read_json_if_exists(
        f"{review_dir}/conflict_samples.json" if review_dir else None
    )
    refresh_report = _read_json_if_exists(str(artifacts.get("refreshReportPath") or "").strip())

    findings = [
        sanitized
        for sanitized in (
            _sanitize_review_finding(item)
            for item in raw_compare_report.get("reviewFindings", [])
        )
        if sanitized is not None
    ]
    sampled_countries: list[str] = []
    sample_count = 0
    conflict_samples: list[dict[str, Any]] = []
    overlap_change_summary = [
        sanitized
        for sanitized in (
            _sanitize_overlap_change_summary(item)
            for item in raw_compare_report.get("overlapChangeSummary", [])
        )
        if sanitized is not None
    ]
    if isinstance(conflict_payload, dict):
        sampled = conflict_payload.get("sampledCountries")
        samples = conflict_payload.get("samples")
        if isinstance(sampled, list):
            sampled_countries = [str(item) for item in sampled]
        if isinstance(samples, list):
            sample_count = len(samples)
            conflict_samples = [
                sanitized
                for sanitized in (
                    _sanitize_conflict_sample(item) for item in samples
                )
                if sanitized is not None
            ]

    return {
        "jobId": job_id,
        "reviewDir": review_dir or None,
        "compareId": str(raw_compare_report.get("compareId", "")),
        "decisionSuggestion": str(raw_compare_report.get("decisionSuggestion", "")),
        "compareKeyColumns": (
            [str(item) for item in raw_compare_report.get("compareKeyColumns", [])]
            if isinstance(raw_compare_report.get("compareKeyColumns"), list)
            else []
        ),
        "checklistMarkdown": checklist_markdown,
        "reviewFindings": findings,
        "sampledCountries": sampled_countries,
        "conflictSampleCount": sample_count,
        "conflictSamples": conflict_samples,
        "overlapChangeSummary": overlap_change_summary,
        "timeAxisCheck": (
            raw_compare_report.get("timeAxisCheck")
            if isinstance(raw_compare_report.get("timeAxisCheck"), dict)
            else {}
        ),
        "countryScopeSummary": (
            raw_compare_report.get("countryScopeSummary")
            if isinstance(raw_compare_report.get("countryScopeSummary"), dict)
            else {}
        ),
        "refreshSummary": (
            _summarize_refresh_report(refresh_report)
            if isinstance(refresh_report, dict)
            else None
        ),
    }


def publish_jato_monthly_update_job(
    *,
    job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    _require_no_running_monthly_update_jobs(excluding_job_id=job_id)
    payload = _load_job_state(job_id)
    if str(payload.get("status", "")) != "success" or str(payload.get("phase", "")) != "completed":
        raise HTTPException(
            status_code=409,
            detail="只有 success/completed 的月更任务才能执行 publish。",
        )

    publication = payload.get("publication")
    if isinstance(publication, dict) and publication.get("publishedAt"):
        raise HTTPException(status_code=409, detail="该月更任务已经 publish 过。")

    summaries = payload.get("summaries")
    refresh_summary = summaries.get("refresh") if isinstance(summaries, dict) else None
    if not isinstance(refresh_summary, dict) or str(refresh_summary.get("jobStatus", "")) != "success":
        raise HTTPException(status_code=409, detail="当前任务 refresh 尚未成功，不能 publish。")

    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise HTTPException(status_code=409, detail="当前任务缺少 staging 产物信息，不能 publish。")

    source_paths = {
        "parquet": _project_path(str(artifacts.get("stagingOutputPath") or "").strip()),
        "manifest": _project_path(str(artifacts.get("manifestPath") or "").strip()),
        "partition": _project_path(str(artifacts.get("partitionOutputPath") or "").strip()),
        "fingerprint": _project_path(str(artifacts.get("fingerprintPath") or "").strip()),
        "refreshReport": _project_path(str(artifacts.get("refreshReportPath") or "").strip()),
    }
    missing_sources = [
        key
        for key, path in source_paths.items()
        if path is None or not path.exists()
    ]
    if missing_sources:
        raise HTTPException(
            status_code=409,
            detail=f"当前任务缺少 publish 所需产物：{', '.join(missing_sources)}。",
        )

    active_paths = _active_data_paths()
    published_at = _utc_now()
    backup_dir = active_paths["backupRoot"] / (
        f"manual-promote-{job_id}-{published_at.strftime('%Y%m%d-%H%M%S')}"
    )
    backup_dir.mkdir(parents=True, exist_ok=True)

    if active_paths["parquet"].exists():
        shutil.copy2(active_paths["parquet"], backup_dir / active_paths["parquet"].name)
    if active_paths["manifest"].exists():
        shutil.copy2(active_paths["manifest"], backup_dir / active_paths["manifest"].name)
    if active_paths["fingerprint"].exists():
        shutil.copy2(active_paths["fingerprint"], backup_dir / active_paths["fingerprint"].name)
    if active_paths["refreshReport"].exists():
        shutil.copy2(active_paths["refreshReport"], backup_dir / active_paths["refreshReport"].name)
    if active_paths["partition"].exists():
        shutil.copytree(
            active_paths["partition"],
            backup_dir / active_paths["partition"].name,
        )

    shutil.copy2(source_paths["parquet"], active_paths["parquet"])
    shutil.copy2(source_paths["manifest"], active_paths["manifest"])
    shutil.copy2(source_paths["fingerprint"], active_paths["fingerprint"])
    shutil.copy2(source_paths["refreshReport"], active_paths["refreshReport"])
    if active_paths["partition"].exists():
        shutil.rmtree(active_paths["partition"])
    shutil.copytree(source_paths["partition"], active_paths["partition"])

    payload["publication"] = {
        "publishedAt": published_at.isoformat(),
        "publishedBy": triggered_by.strip() or "anonymous",
        "backupDir": _relative_to_project(backup_dir),
        "activeParquetPath": _relative_to_project(active_paths["parquet"]),
        "activeManifestPath": _relative_to_project(active_paths["manifest"]),
        "activePartitionPath": _relative_to_project(active_paths["partition"]),
        "activeFingerprintPath": _relative_to_project(active_paths["fingerprint"]),
        "activeRefreshReportPath": _relative_to_project(active_paths["refreshReport"]),
    }
    _persist_job_state(payload)
    _append_log(
        _job_log_path(job_id),
        (
            f"[{published_at.isoformat()}] published candidate to active dataset by "
            f"{triggered_by.strip() or 'anonymous'}"
        ),
    )
    return _serialize_job_state(payload, include_log_tail=True)


def _run_logged_command(
    *,
    label: str,
    args: list[str],
    log_path: Path,
) -> None:
    rendered_command = " ".join(shlex.quote(arg) for arg in args)
    _append_log(log_path, f"\n=== {label} ===\n$ {rendered_command}")
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        args,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    try:
        if process.stdout is not None:
            for line in process.stdout:
                _append_log(log_path, line.rstrip("\n"))
    finally:
        if process.stdout is not None:
            process.stdout.close()
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"{label} 失败，退出码 {return_code}")


def _prepare_initial_job_state(
    *,
    job_id: str,
    month: str,
    triggered_by: str,
    upload_filename: str,
    stored_upload_path: Path,
    file_sha256: str | None = None,
    baseline_path: Path | None = None,
    baseline_source: str | None = None,
) -> dict[str, Any]:
    now = _utc_now().isoformat()
    artifacts: dict[str, Any] = {
        "jobDir": _relative_to_project(_job_dir(job_id)),
        "logPath": _relative_to_project(_job_log_path(job_id)),
    }
    if baseline_path is not None:
        artifacts["baselinePath"] = _relative_to_project(baseline_path)
    if baseline_source:
        artifacts["baselineSource"] = baseline_source
    return {
        "jobId": job_id,
        "month": month,
        "status": "queued",
        "phase": "queued",
        "triggeredBy": triggered_by,
        "createdAt": now,
        "updatedAt": now,
        "startedAt": None,
        "finishedAt": None,
        "error": None,
        "upload": {
            "originalFilename": upload_filename,
            "storedPath": _relative_to_project(stored_upload_path),
            "sizeBytes": stored_upload_path.stat().st_size,
            "sha256": file_sha256,
        },
        "plan": None,
        "artifacts": artifacts,
        "summaries": {},
        "logPath": _relative_to_project(_job_log_path(job_id)),
    }


def _prepare_upload_session_state(
    *,
    upload_id: str,
    filename: str,
    size_bytes: int,
    resume_key: str | None,
    triggered_by: str,
) -> dict[str, Any]:
    now = _utc_now().isoformat()
    total_chunks = max((size_bytes + UPLOAD_CHUNK_SIZE_BYTES - 1) // UPLOAD_CHUNK_SIZE_BYTES, 1)
    return {
        "uploadId": upload_id,
        "filename": filename,
        "sizeBytes": size_bytes,
        "chunkSize": UPLOAD_CHUNK_SIZE_BYTES,
        "totalChunks": total_chunks,
        "receivedChunks": [],
        "chunkDigests": {},
        "uploadedBytes": 0,
        "status": "pending",
        "resumeKey": resume_key,
        "fileSha256": None,
        "triggeredBy": triggered_by,
        "createdAt": now,
        "updatedAt": now,
        "completedAt": None,
        "assembledPath": None,
    }


def initiate_jato_monthly_update_upload(
    *,
    filename: str,
    size_bytes: Any,
    resume_key: str | None,
    triggered_by: str,
) -> dict[str, Any]:
    normalized_filename = _validate_upload_filename(filename)
    normalized_size = _normalize_size_bytes(size_bytes)
    normalized_resume_key = str(resume_key or "").strip() or None
    existing = _find_upload_session_by_resume_key(
        resume_key=normalized_resume_key or "",
        filename=normalized_filename,
        size_bytes=normalized_size,
    )
    if existing is not None:
        return _serialize_upload_session(existing)
    upload_id = f"jato-upload-{uuid4().hex[:10]}"
    session_dir = _upload_session_dir(upload_id)
    session_dir.mkdir(parents=True, exist_ok=True)
    _upload_session_chunk_dir(upload_id).mkdir(parents=True, exist_ok=True)
    state = _prepare_upload_session_state(
        upload_id=upload_id,
        filename=normalized_filename,
        size_bytes=normalized_size,
        resume_key=normalized_resume_key,
        triggered_by=triggered_by.strip() or "anonymous",
    )
    _persist_upload_session(state)
    return _serialize_upload_session(state)


def get_jato_monthly_update_upload(upload_id: str) -> dict[str, Any]:
    return _serialize_upload_session(_load_upload_session(upload_id))


def upload_jato_monthly_update_chunk(
    *,
    upload_id: str,
    part_number: int,
    content: bytes,
    chunk_sha256: str,
) -> dict[str, Any]:
    state = _load_upload_session(upload_id)
    status = str(state.get("status", "pending"))
    if status in {"completed", "consumed"}:
        raise HTTPException(status_code=409, detail="上传会话已完成，不能继续写入分片。")

    total_chunks = int(state.get("totalChunks", 0))
    size_bytes = int(state.get("sizeBytes", 0))
    chunk_size = int(state.get("chunkSize", UPLOAD_CHUNK_SIZE_BYTES))
    if part_number < 1 or part_number > total_chunks:
        raise HTTPException(status_code=400, detail="分片序号超出范围。")

    expected_size = _expected_chunk_size(
        size_bytes=size_bytes,
        chunk_size=chunk_size,
        total_chunks=total_chunks,
        part_number=part_number,
    )
    if len(content) != expected_size:
        raise HTTPException(
            status_code=400,
            detail=f"分片大小不匹配，期望 {expected_size} 字节，实际 {len(content)} 字节。",
        )
    normalized_chunk_sha256 = _normalize_sha256(
        chunk_sha256,
        detail="分片 SHA-256 无效。",
    )
    actual_chunk_sha256 = _sha256_hex_for_bytes(content)
    if actual_chunk_sha256 != normalized_chunk_sha256:
        raise HTTPException(status_code=400, detail="分片内容校验失败，请重试。")

    chunk_dir = _upload_session_chunk_dir(upload_id)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = chunk_dir / _chunk_file_name(part_number)
    existing_digests = state.get("chunkDigests")
    digest_map = existing_digests if isinstance(existing_digests, dict) else {}
    if (
        chunk_path.exists()
        and chunk_path.stat().st_size == expected_size
        and str(digest_map.get(str(part_number), "")) == normalized_chunk_sha256
    ):
        state["receivedChunks"] = _collect_uploaded_chunk_numbers(upload_id)
        state["uploadedBytes"] = _uploaded_chunk_bytes(upload_id)
        state["chunkDigests"] = digest_map
        state["status"] = "uploading"
        _persist_upload_session(state)
        return _serialize_upload_session(state)

    chunk_path.write_bytes(content)
    digest_map[str(part_number)] = normalized_chunk_sha256
    state["chunkDigests"] = digest_map
    state["receivedChunks"] = _collect_uploaded_chunk_numbers(upload_id)
    state["uploadedBytes"] = _uploaded_chunk_bytes(upload_id)
    state["status"] = "uploading"
    _persist_upload_session(state)
    return _serialize_upload_session(state)


def complete_jato_monthly_update_upload(*, upload_id: str) -> dict[str, Any]:
    state = _load_upload_session(upload_id)
    total_chunks = int(state.get("totalChunks", 0))
    received_chunks = _collect_uploaded_chunk_numbers(upload_id)
    if len(received_chunks) != total_chunks or received_chunks != list(range(1, total_chunks + 1)):
        raise HTTPException(status_code=409, detail="上传分片尚未齐全，不能完成组装。")

    filename = str(state.get("filename", "jato-update.xlsx"))
    assembled_path = _upload_session_assembled_path(upload_id, filename)
    assembled_path.parent.mkdir(parents=True, exist_ok=True)
    existing_digests = state.get("chunkDigests")
    digest_map = existing_digests if isinstance(existing_digests, dict) else {}
    file_hasher = hashlib.sha256()
    with assembled_path.open("wb") as target:
        for part_number in range(1, total_chunks + 1):
            chunk_path = _upload_session_chunk_dir(upload_id) / _chunk_file_name(part_number)
            expected_digest = str(digest_map.get(str(part_number), "")).strip().lower()
            with chunk_path.open("rb") as source:
                chunk_bytes = source.read()
            if not chunk_bytes:
                raise HTTPException(status_code=500, detail="发现空分片，无法完成组装。")
            if expected_digest:
                actual_chunk_digest = _sha256_hex_for_bytes(chunk_bytes)
                if actual_chunk_digest != expected_digest:
                    raise HTTPException(status_code=500, detail="分片校验失败，请重新上传缺失分片。")
            target.write(chunk_bytes)
            file_hasher.update(chunk_bytes)

    if assembled_path.stat().st_size != int(state.get("sizeBytes", 0)):
        raise HTTPException(status_code=500, detail="组装后的文件大小校验失败，请重新上传。")

    shutil.rmtree(_upload_session_chunk_dir(upload_id), ignore_errors=True)
    state["receivedChunks"] = received_chunks
    state["uploadedBytes"] = assembled_path.stat().st_size
    state["status"] = "completed"
    state["completedAt"] = _utc_now().isoformat()
    state["assembledPath"] = _relative_to_project(assembled_path)
    state["fileSha256"] = file_hasher.hexdigest()
    _persist_upload_session(state)
    return _serialize_upload_session(state)


def _queue_monthly_update_job_from_stored_upload(
    *,
    job_id: str,
    month: str,
    triggered_by: str,
    upload_filename: str,
    stored_upload_path: Path,
    file_sha256: str | None = None,
) -> dict[str, Any]:
    if stored_upload_path.stat().st_size <= 0:
        raise HTTPException(status_code=400, detail="上传文件为空，无法启动月更任务。")
    normalized_file_sha256 = (
        _normalize_sha256(file_sha256, detail="文件 SHA-256 无效。")
        if file_sha256
        else _sha256_hex_for_path(stored_upload_path)
    )
    baseline_path, baseline_source = _require_latest_baseline()

    state = _prepare_initial_job_state(
        job_id=job_id,
        month=month,
        triggered_by=triggered_by.strip() or "anonymous",
        upload_filename=upload_filename,
        stored_upload_path=stored_upload_path,
        file_sha256=normalized_file_sha256,
        baseline_path=baseline_path,
        baseline_source=baseline_source,
    )
    _persist_job_state(state)
    _append_log(
        _job_log_path(job_id),
        f"[{_utc_now().isoformat()}] queued monthly update for {month}",
    )
    _launch_job_thread(job_id)
    return _serialize_job_state(state, include_log_tail=True)


def _run_job(job_id: str) -> None:
    state = _load_job_state(job_id)
    log_path = _job_log_path(job_id)
    upload_payload = state.get("upload")
    if not isinstance(upload_payload, dict):
        raise RuntimeError("任务缺少 upload 信息")
    stored_path_value = upload_payload.get("storedPath")
    if not stored_path_value:
        raise RuntimeError("任务缺少 upload storedPath")
    stored_upload_path = PROJECT_ROOT / str(stored_path_value)

    state["status"] = "running"
    state["phase"] = "preparing"
    state["startedAt"] = _utc_now().isoformat()
    _persist_job_state(state)

    try:
        artifacts = state.get("artifacts")
        baseline_relative_path = (
            artifacts.get("baselinePath")
            if isinstance(artifacts, dict)
            else None
        )
        baseline_source = (
            str(artifacts.get("baselineSource"))
            if isinstance(artifacts, dict) and artifacts.get("baselineSource")
            else None
        )
        baseline_path = _project_path(str(baseline_relative_path)) if baseline_relative_path else None
        if baseline_path is None:
            baseline_path, baseline_source = _require_latest_baseline()
            state["artifacts"] = artifacts if isinstance(artifacts, dict) else {}
            state["artifacts"]["baselinePath"] = _relative_to_project(baseline_path)
            state["artifacts"]["baselineSource"] = baseline_source
            _persist_job_state(state)
        elif not baseline_path.exists():
            raise RuntimeError(
                "任务绑定的 baseline 不存在："
                f"{_relative_to_project(baseline_path) or str(baseline_path)}"
            )

        if baseline_source == "archive":
            _append_log(
                log_path,
                (
                    f"[{_utc_now().isoformat()}] active baseline 缺失，"
                    "使用 archive baseline: "
                    f"{_relative_to_project(baseline_path) or str(baseline_path)}"
                ),
            )

        if not PREPARE_SCRIPT_PATH.exists():
            raise RuntimeError(f"找不到脚本: {_relative_to_project(PREPARE_SCRIPT_PATH)}")

        prepare_args = [
            sys.executable,
            str(PREPARE_SCRIPT_PATH),
            "--month",
            str(state["month"]),
            "--baseline",
            str(baseline_path),
            "--patch",
            str(stored_upload_path),
        ]
        _run_logged_command(label="Prepare monthly update", args=prepare_args, log_path=log_path)

        plan_path = PROJECT_ROOT / "01_RAW_DATA" / "patches" / str(state["month"]) / "monthly_update_plan.md"
        if not plan_path.exists():
            raise RuntimeError("prepare 完成后未生成 monthly_update_plan.md")

        parsed_plan = _parse_plan_markdown(plan_path)
        state["plan"] = {
            "path": _relative_to_project(plan_path),
            "compareId": parsed_plan.get("compareId"),
            "compareCommand": parsed_plan.get("compareCommand"),
            "refreshCommand": parsed_plan.get("refreshCommand"),
        }
        artifacts = state.get("artifacts")
        state["artifacts"] = artifacts if isinstance(artifacts, dict) else {}
        state["artifacts"].update(
            {
                "jobDir": _relative_to_project(_job_dir(job_id)),
                "logPath": _relative_to_project(log_path),
                "baselinePath": parsed_plan.get("baselinePath"),
                "stagedPatchPath": parsed_plan.get("patchPath"),
                "planPath": _relative_to_project(plan_path),
                "reviewDir": parsed_plan.get("reviewDir"),
                "rawCompareReportPath": parsed_plan.get("rawCompareReportPath"),
                "stagingOutputPath": parsed_plan.get("stagingOutputPath"),
                "manifestPath": parsed_plan.get("manifestPath"),
                "partitionOutputPath": parsed_plan.get("partitionOutputPath"),
                "refreshReportPath": parsed_plan.get("refreshReportPath"),
                "fingerprintPath": parsed_plan.get("fingerprintPath"),
            }
        )
        _persist_job_state(state)

        state["phase"] = "raw_compare"
        _persist_job_state(state)
        compare_args = _command_to_args(str(parsed_plan["compareCommand"]))
        _run_logged_command(label="Raw compare review", args=compare_args, log_path=log_path)
        raw_compare_report = _read_json_if_exists(parsed_plan.get("rawCompareReportPath"))
        if raw_compare_report is None:
            raise RuntimeError("raw compare 完成后未找到 raw_compare_report.json")
        summaries = state.get("summaries")
        state["summaries"] = summaries if isinstance(summaries, dict) else {}
        state["summaries"]["rawCompare"] = _summarize_raw_compare_report(raw_compare_report)
        _persist_job_state(state)

        state["phase"] = "refresh"
        _persist_job_state(state)
        refresh_args = _command_to_args(str(parsed_plan["refreshCommand"]))
        _run_logged_command(label="Candidate refresh", args=refresh_args, log_path=log_path)
        refresh_report = _read_json_if_exists(parsed_plan.get("refreshReportPath"))
        if refresh_report is None:
            raise RuntimeError("refresh 完成后未找到 refresh_job_report.json")
        state["summaries"]["refresh"] = _summarize_refresh_report(refresh_report)
        state["status"] = "success"
        state["phase"] = "completed"
        state["finishedAt"] = _utc_now().isoformat()
        _persist_job_state(state)
    except Exception as exc:
        state["status"] = "failed"
        state["phase"] = "failed"
        state["finishedAt"] = _utc_now().isoformat()
        state["error"] = str(exc)
        _persist_job_state(state)
        _append_log(log_path, "\n=== Failed ===")
        _append_log(log_path, str(exc))
        _append_log(log_path, traceback.format_exc())
    finally:
        _RUNNING_THREADS.pop(job_id, None)


def _launch_job_thread(job_id: str) -> None:
    worker = threading.Thread(
        target=_run_job,
        args=(job_id,),
        name=f"jato-monthly-update-{job_id}",
        daemon=True,
    )
    _RUNNING_THREADS[job_id] = worker
    worker.start()


def create_jato_monthly_update_job(
    *,
    month: str,
    file: UploadFile,
    triggered_by: str,
) -> dict[str, Any]:
    normalized_month = _normalize_month(month)
    filename = _validate_upload(file)

    job_id = f"jato-update-{uuid4().hex[:8]}"
    uploads_dir = _job_dir(job_id) / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_upload_path = uploads_dir / filename

    with stored_upload_path.open("wb") as handle:
        shutil.copyfileobj(file.file, handle)

    return _queue_monthly_update_job_from_stored_upload(
        job_id=job_id,
        month=normalized_month,
        triggered_by=triggered_by,
        upload_filename=filename,
        stored_upload_path=stored_upload_path,
    )


def create_jato_monthly_update_job_from_upload(
    *,
    month: str,
    upload_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    normalized_month = _normalize_month(month)
    state = _load_upload_session(upload_id)
    if str(state.get("status", "")) != "completed":
        raise HTTPException(status_code=409, detail="上传尚未完成组装，不能创建月更任务。")

    filename = _validate_upload_filename(str(state.get("filename", "jato-update.xlsx")))
    assembled_path = _upload_session_assembled_path(upload_id, filename)
    if not assembled_path.exists():
        raise HTTPException(status_code=409, detail="组装后的上传文件不存在，请重新上传。")
    file_sha256 = _normalize_sha256(
        state.get("fileSha256"),
        detail="上传文件指纹缺失，请重新完成组装。",
    )

    job_id = f"jato-update-{uuid4().hex[:8]}"
    uploads_dir = _job_dir(job_id) / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_upload_path = uploads_dir / filename
    shutil.move(str(assembled_path), str(stored_upload_path))
    try:
        result = _queue_monthly_update_job_from_stored_upload(
            job_id=job_id,
            month=normalized_month,
            triggered_by=triggered_by,
            upload_filename=filename,
            stored_upload_path=stored_upload_path,
            file_sha256=file_sha256,
        )
    except Exception:
        upload_session_dir = _upload_session_dir(upload_id)
        upload_session_dir.mkdir(parents=True, exist_ok=True)
        if stored_upload_path.exists() and not assembled_path.exists():
            shutil.move(str(stored_upload_path), str(assembled_path))
        shutil.rmtree(_job_dir(job_id), ignore_errors=True)
        raise
    shutil.rmtree(_upload_session_dir(upload_id), ignore_errors=True)
    return result


def retry_failed_jato_monthly_update_job(
    *,
    source_job_id: str,
    triggered_by: str,
) -> dict[str, Any]:
    source_state = _load_job_state(source_job_id)
    if str(source_state.get("status", "")) != "failed":
        raise HTTPException(
            status_code=409,
            detail="只有 failed 的月更任务才能直接重试。",
        )

    source_upload = source_state.get("upload")
    if not isinstance(source_upload, dict):
        raise HTTPException(status_code=409, detail="原任务缺少 upload 信息，无法直接重试。")

    stored_path_value = source_upload.get("storedPath")
    if not stored_path_value:
        raise HTTPException(
            status_code=409,
            detail="原任务的上传副本已不存在，请重新上传文件后再试。",
        )

    source_upload_path = _project_path(str(stored_path_value))
    if source_upload_path is None or not source_upload_path.exists():
        raise HTTPException(
            status_code=409,
            detail="原任务的上传副本已不存在，请重新上传文件后再试。",
        )

    month = _normalize_month(str(source_state.get("month", "")))
    filename = _validate_upload_filename(
        str(source_upload.get("originalFilename") or source_upload_path.name)
    )
    job_id = f"jato-update-{uuid4().hex[:8]}"
    uploads_dir = _job_dir(job_id) / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    stored_upload_path = uploads_dir / filename
    shutil.copy2(source_upload_path, stored_upload_path)
    try:
        result = _queue_monthly_update_job_from_stored_upload(
            job_id=job_id,
            month=month,
            triggered_by=triggered_by,
            upload_filename=filename,
            stored_upload_path=stored_upload_path,
            file_sha256=(
                str(source_upload.get("sha256"))
                if source_upload.get("sha256") is not None
                else None
            ),
        )
    except Exception:
        shutil.rmtree(_job_dir(job_id), ignore_errors=True)
        raise

    payload = _load_job_state(job_id)
    artifacts = payload.get("artifacts")
    payload["artifacts"] = artifacts if isinstance(artifacts, dict) else {}
    payload["artifacts"]["retriedFromJobId"] = source_job_id
    _persist_job_state(payload)
    return _serialize_job_state(payload, include_log_tail=True)


def list_jato_monthly_update_jobs(*, limit: int = 20) -> dict[str, Any]:
    items = [
        _serialize_job_state(payload, include_log_tail=False)
        for payload in _list_job_state_payloads()
    ]

    items.sort(key=lambda item: str(item.get("createdAt", "")), reverse=True)
    sliced = items[:limit]
    return {"rows": len(sliced), "items": sliced}


def get_jato_monthly_update_job(job_id: str) -> dict[str, Any]:
    payload = _load_job_state(job_id)
    return _serialize_job_state(payload, include_log_tail=True)


def run_jato_monthly_update_cleanup(*, triggered_by: str) -> dict[str, Any]:
    job_payloads = _list_job_state_payloads()
    running_jobs = [
        str(payload.get("jobId", ""))
        for payload in job_payloads
        if str(payload.get("status", "")) in {"queued", "running"}
    ]
    if running_jobs:
        raise HTTPException(
            status_code=409,
            detail="存在运行中的月更任务，暂时不能执行一键清理。",
        )

    baseline_files = _list_supported_excel_files(BASELINE_ROOT)
    latest_baseline = _latest_baseline_file(baseline_files)
    archived_baselines: list[str] = []
    if latest_baseline is not None:
        for path in baseline_files:
            if path == latest_baseline:
                continue
            target = _move_to_archive(path, HISTORY_ARCHIVE_ROOT / "baseline")
            archived_baselines.append(_relative_to_project(target) or str(target))

    patch_dirs = sorted([path for path in PATCHES_ROOT.glob("*") if path.is_dir()])
    latest_patch_dir = _latest_patch_dir(patch_dirs)
    archived_patch_dirs: list[str] = []
    if latest_patch_dir is not None:
        for path in patch_dirs:
            if path == latest_patch_dir:
                continue
            target = _move_to_archive(path, HISTORY_ARCHIVE_ROOT / "patches")
            archived_patch_dirs.append(_relative_to_project(target) or str(target))

    removed_job_upload_dirs: list[str] = []
    for payload in job_payloads:
        if str(payload.get("status", "")) != "success":
            continue
        job_id = str(payload.get("jobId", "")).strip()
        if not job_id:
            continue
        upload_dir = _job_dir(job_id) / "uploads"
        if not upload_dir.exists():
            continue
        shutil.rmtree(upload_dir)
        removed_job_upload_dirs.append(_relative_to_project(upload_dir) or str(upload_dir))
        upload_payload = payload.get("upload")
        if isinstance(upload_payload, dict):
            upload_payload["storedPath"] = None
        _persist_job_state(payload)

    cleaned_at = _utc_now().isoformat()
    return {
        "cleanedAt": cleaned_at,
        "triggeredBy": triggered_by.strip() or "anonymous",
        "activeBaselinePath": _relative_to_project(latest_baseline),
        "activePatchMonth": latest_patch_dir.name if latest_patch_dir is not None else None,
        "archivedBaselineCount": len(archived_baselines),
        "archivedBaselines": archived_baselines,
        "archivedPatchDirCount": len(archived_patch_dirs),
        "archivedPatchDirs": archived_patch_dirs,
        "removedJobUploadDirCount": len(removed_job_upload_dirs),
        "removedJobUploadDirs": removed_job_upload_dirs,
    }
