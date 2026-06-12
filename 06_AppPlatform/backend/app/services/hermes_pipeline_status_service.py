"""Hermes pipeline runtime status contract.

Standard status files live at:
  hermes/reports/pipeline_status/{pipeline_id}.json

The service also hydrates legacy scheduled_fetch_status.json and report
artifacts so Sentinel can move from "status missing" to evidence-backed health.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from app.core.config import JATO_MONTHLY_UPDATE_JOB_ROOT, PROJECT_ROOT


_project_root: Path | None = None
_jato_monthly_update_job_root: Path | None = None

EXPECTED_PIPELINE_IDS = (
    "jato_etl",
    "msrp_dryrun",
    "msrp_ingest",
    "msrp_current_price_snapshot",
    "msrp_readiness_audit",
    "voc_forum_sync",
    "country_news_sync",
    "ai_intelligence_enrichment_smoke",
    "unified_scraping_readiness",
    "source_quality",
)

PIPELINE_STATUS_VALUES = {"success", "degraded", "failed", "missing", "running", "skipped", "unknown"}
DEGRADED_STATUS_VALUES = {"degraded", "partial_success"}
FAILED_STATUS_VALUES = {"failed", "failure", "error"}

LEGACY_STATUS_KEY_MAP = {
    "news": "country_news_sync",
    "news_batch_b": "country_news_sync_b",
    "voc": "voc_forum_sync",
    "msrp_dryrun": "msrp_dryrun",
    "msrp_ingest": "msrp_ingest",
    "jato_etl": "jato_etl",
}


def _root() -> Path:
    global _project_root
    if _project_root is None:
        _project_root = PROJECT_ROOT
    return _project_root


def _status_dir() -> Path:
    return _root() / "hermes" / "reports" / "pipeline_status"


def _jato_job_root() -> Path:
    if _jato_monthly_update_job_root is not None:
        return _jato_monthly_update_job_root
    if _project_root is not None:
        return _root() / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    return JATO_MONTHLY_UPDATE_JOB_ROOT


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (TypeError, ValueError):
        return None


def _format_dt(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    parsed = _parse_dt(value)
    if parsed:
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    text = str(value).strip()
    return text or None


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _coerce_status(value: Any) -> str:
    status = str(value or "unknown").strip().lower()
    if status in FAILED_STATUS_VALUES:
        return "failed"
    if status in DEGRADED_STATUS_VALUES:
        return "degraded"
    if status in PIPELINE_STATUS_VALUES:
        return status
    return "unknown"


def _list_value(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value:
        return [value]
    return []


def _duration_seconds(started_at: Any, finished_at: Any) -> int:
    start_dt = _parse_dt(started_at)
    finish_dt = _parse_dt(finished_at)
    if not start_dt or not finish_dt:
        return 0
    return max(0, int((finish_dt - start_dt).total_seconds()))


def normalize_pipeline_status_record(record: dict[str, Any]) -> dict[str, Any]:
    pipeline_id = str(record.get("pipelineId") or "").strip()
    if not pipeline_id:
        raise ValueError("pipelineId is required")

    finished_at = _format_dt(record.get("finishedAt"))
    last_run_at = _format_dt(record.get("lastRunAt")) or finished_at
    started_at = _format_dt(record.get("startedAt"))

    normalized = {
        "pipelineId": pipeline_id,
        "status": _coerce_status(record.get("status")),
        "lastRunAt": last_run_at,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "exitCode": record.get("exitCode"),
        "durationSeconds": _coerce_int(
            record.get("durationSeconds")
            or _duration_seconds(started_at, finished_at)
        ),
        "recordsProcessed": _coerce_int(
            record.get("recordsProcessed", record.get("successCount", 0)),
        ),
        "failedCount": _coerce_int(
            record.get("failedCount", record.get("failureCount", 0)),
        ),
        "warningCount": _coerce_int(record.get("warningCount")),
        "artifactRefs": [str(item) for item in _list_value(record.get("artifactRefs"))],
        "source": str(record.get("source") or "").strip(),
        "message": str(record.get("message") or record.get("lastError") or "").strip(),
    }

    for key in (
        "runId",
        "countryCount",
        "observedCountryCount",
        "missingCountryCount",
        "successCount",
        "emptyCount",
        "passPct",
        "okPct",
        "schemaVersion",
        "requiresReview",
        "dryRunBeforeIngest",
        "jobId",
        "month",
        "batchId",
        "phase",
        "triggeredBy",
        "snapshotWeek",
        "priceAlertSummary",
        "readinessStatus",
        "smokeStatus",
        "contractStatus",
        "stageStatus",
        "requiredCountryCount",
        "jobsByKind",
        "failedStageCount",
        "mappingErrorCount",
        "statusCounts",
        "runtimeCounts",
        "metadata",
        "news",
        "voc",
        "warnings",
        "errorType",
        "goalCompletionStatus",
        "localP0Ready",
        "msrpMissingRequirementKeys",
        "sourceDraftTodoPlaceholderCount",
        "productionStatus",
    ):
        if key in record:
            normalized[key] = record.get(key)
    return normalized


def write_pipeline_status(record: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_pipeline_status_record(record)
    status_dir = _status_dir()
    status_dir.mkdir(parents=True, exist_ok=True)
    path = status_dir / f"{normalized['pipelineId']}.json"
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n")
    tmp_path.replace(path)
    return normalized


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _read_standard_status_files() -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    status_dir = _status_dir()
    if not status_dir.is_dir():
        return records
    for path in sorted(status_dir.glob("*.json")):
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue
        payload.setdefault("pipelineId", path.stem)
        try:
            record = normalize_pipeline_status_record(payload)
        except ValueError:
            continue
        record["statusPath"] = _relative_path(path)
        record["standardStatusFile"] = True
        records[record["pipelineId"]] = record
    return records


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(_root().resolve()))
    except ValueError:
        return str(path)


def _legacy_scheduled_status_records() -> dict[str, dict[str, Any]]:
    path = _root() / "03_Scripts" / "logs" / "scheduled_fetch_status.json"
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return {}

    records: dict[str, dict[str, Any]] = {}
    for legacy_key, entry in payload.items():
        if not isinstance(entry, dict):
            continue
        pipeline_id = LEGACY_STATUS_KEY_MAP.get(str(legacy_key))
        if not pipeline_id:
            continue
        artifact_refs = []
        if entry.get("artifactPath"):
            artifact_refs.append(entry["artifactPath"])
        try:
            record = normalize_pipeline_status_record({
                "pipelineId": pipeline_id,
                "status": entry.get("status"),
                "lastRunAt": entry.get("lastRunAt"),
                "recordsProcessed": entry.get("successCount"),
                "failedCount": entry.get("failureCount", entry.get("failedCount", 0)),
                "warningCount": entry.get("warningCount", 0),
                "artifactRefs": artifact_refs,
                "source": "03_Scripts/logs/scheduled_fetch_status.json",
                "message": entry.get("lastError") or entry.get("reason") or "",
                "metadata": entry.get("metadata") or entry.get("runtimeMetadata"),
                "runId": entry.get("runId"),
                "countryCount": entry.get("countryCount"),
                "observedCountryCount": entry.get("observedCountryCount"),
                "missingCountryCount": entry.get("missingCountryCount"),
            })
        except ValueError:
            continue
        record["legacyStatusKey"] = legacy_key
        record["statusPath"] = _relative_path(path)
        record["standardStatusFile"] = False
        records[pipeline_id] = record
    return records


def _source_quality_report_status() -> dict[str, dict[str, Any]]:
    path = _root() / "hermes" / "reports" / "source_quality_report.json"
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return {}
    generated_at = payload.get("generatedAt")
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    high_risk = _coerce_int(summary.get("highRisk"))
    degraded = _coerce_int(summary.get("degraded"))
    return {
        "source_quality": normalize_pipeline_status_record({
            "pipelineId": "source_quality",
            "status": "success",
            "lastRunAt": generated_at,
            "finishedAt": generated_at,
            "recordsProcessed": summary.get("totalSources", 0),
            "failedCount": 0,
            "warningCount": 0,
            "artifactRefs": ["hermes/reports/source_quality_report.json"],
            "source": "03_Scripts/hermes/hermes_source_quality.py",
            "message": f"{degraded} degraded sources, {high_risk} high-risk sources",
        })
    }


def _job_state_sort_key(payload: dict[str, Any], path: Path) -> str:
    for key in ("finishedAt", "updatedAt", "startedAt", "createdAt"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
    except OSError:
        return ""


def _jato_monthly_update_job_status() -> dict[str, dict[str, Any]]:
    """Hydrate jato_etl from the existing monthly update job_state ledger."""
    job_root = _jato_job_root()
    if not job_root.is_dir():
        return {}

    latest_payload: dict[str, Any] | None = None
    latest_path: Path | None = None
    latest_key = ""
    for path in job_root.glob("*/job_state.json"):
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue
        sort_key = _job_state_sort_key(payload, path)
        if latest_payload is None or sort_key > latest_key:
            latest_payload = payload
            latest_path = path
            latest_key = sort_key

    if latest_payload is None:
        return {}

    summaries = latest_payload.get("summaries") if isinstance(latest_payload.get("summaries"), dict) else {}
    refresh = summaries.get("refresh") if isinstance(summaries.get("refresh"), dict) else {}
    artifacts = latest_payload.get("artifacts") if isinstance(latest_payload.get("artifacts"), dict) else {}
    artifact_refs = []
    for key in (
        "logPath",
        "planPath",
        "rawCompareReportPath",
        "stagingOutputPath",
        "manifestPath",
        "partitionOutputPath",
        "refreshReportPath",
        "fingerprintPath",
    ):
        value = artifacts.get(key)
        if isinstance(value, str) and value.strip():
            artifact_refs.append(value.strip())
    if latest_path is not None:
        artifact_refs.insert(0, _relative_path(latest_path))

    job_status = str(latest_payload.get("status") or "unknown").strip().lower()
    status = "success" if job_status == "success" else "failed" if job_status == "failed" else "unknown"
    message = str(latest_payload.get("error") or "").strip()
    if not message:
        message = (
            f"jobId={latest_payload.get('jobId', '')} "
            f"month={latest_payload.get('month', '')} phase={latest_payload.get('phase', '')}"
        ).strip()

    record = normalize_pipeline_status_record({
        "pipelineId": "jato_etl",
        "status": status,
        "lastRunAt": (
            latest_payload.get("finishedAt")
            or latest_payload.get("updatedAt")
            or latest_payload.get("startedAt")
            or latest_payload.get("createdAt")
        ),
        "startedAt": latest_payload.get("startedAt"),
        "finishedAt": latest_payload.get("finishedAt"),
        "exitCode": 0 if status == "success" else 1 if status == "failed" else None,
        "recordsProcessed": refresh.get("rowCount", 0),
        "failedCount": 1 if status == "failed" else 0,
        "warningCount": 0,
        "artifactRefs": artifact_refs,
        "source": "app.services.jato_monthly_update_service.job_state",
        "message": message,
        "jobId": latest_payload.get("jobId"),
        "month": latest_payload.get("month"),
        "batchId": latest_payload.get("batchId"),
        "phase": latest_payload.get("phase"),
        "triggeredBy": latest_payload.get("triggeredBy"),
    })
    if latest_path is not None:
        record["statusPath"] = _relative_path(latest_path)
    record["standardStatusFile"] = False
    record["derivedFrom"] = "jato_monthly_update_job_state"
    return {"jato_etl": record}


def _missing_record(pipeline_id: str) -> dict[str, Any]:
    message = "No standard pipeline status record has been written yet."
    if pipeline_id == "jato_etl":
        message = "No recent JATO ETL run recorded in pipeline_status."
    return normalize_pipeline_status_record({
        "pipelineId": pipeline_id,
        "status": "missing",
        "lastRunAt": None,
        "exitCode": None,
        "source": "",
        "message": message,
    })


def list_pipeline_statuses(
    *,
    include_missing: bool = True,
    expected_pipeline_ids: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    expected = list(expected_pipeline_ids or EXPECTED_PIPELINE_IDS)
    records = _read_standard_status_files()
    for pipeline_id, record in _legacy_scheduled_status_records().items():
        records.setdefault(pipeline_id, record)
    for pipeline_id, record in _jato_monthly_update_job_status().items():
        records.setdefault(pipeline_id, record)
    for pipeline_id, record in _source_quality_report_status().items():
        records.setdefault(pipeline_id, record)

    if include_missing:
        for pipeline_id in expected:
            records.setdefault(pipeline_id, _missing_record(pipeline_id))

    ordered = sorted(records.values(), key=lambda item: str(item.get("pipelineId") or ""))
    return ordered


def get_pipeline_status(pipeline_id: str) -> dict[str, Any]:
    target = str(pipeline_id or "").strip()
    for record in list_pipeline_statuses(include_missing=True):
        if record.get("pipelineId") == target:
            return record
    return _missing_record(target)


def _pipeline_stale_hours(pipeline_id: str) -> int | None:
    mapping = {
        "country_news_sync": 30,
        "country_news_sync_b": 30,
        "voc_forum_sync": 30,
        "msrp_dryrun": 30,
        "msrp_ingest": 192,
        "msrp_current_price_snapshot": 192,
        "msrp_readiness_audit": 30,
        "ai_intelligence_enrichment_smoke": 30,
        "unified_scraping_readiness": 30,
        "source_quality": 26,
        "jato_etl": None,
    }
    return mapping.get(pipeline_id)


def classify_pipeline_health(record: dict[str, Any]) -> dict[str, Any]:
    pipeline_id = str(record.get("pipelineId") or "")
    status = _coerce_status(record.get("status"))
    last_run_at = record.get("lastRunAt")
    failed_count = _coerce_int(record.get("failedCount"))
    warning_count = _coerce_int(record.get("warningCount"))

    if status == "missing":
        return {
            "overall": "warning",
            "severity": "low" if pipeline_id == "jato_etl" else "medium",
            "type": f"pipeline_{pipeline_id}_missing",
            "message": record.get("message") or f"Pipeline '{pipeline_id}' has no status record.",
        }
    if status == "failed":
        return {
            "overall": "critical",
            "severity": "critical" if pipeline_id == "msrp_ingest" else "high",
            "type": f"pipeline_{pipeline_id}_failed",
            "message": f"Pipeline '{pipeline_id}' last run failed.",
        }
    if status == "running":
        return {
            "overall": "ok",
            "severity": "none",
            "type": f"pipeline_{pipeline_id}_running",
            "message": f"Pipeline '{pipeline_id}' is currently running.",
        }
    if status == "degraded" or failed_count > 0 or warning_count > 0:
        return {
            "overall": "warning",
            "severity": "medium",
            "type": f"pipeline_{pipeline_id}_degraded",
            "message": (
                f"Pipeline '{pipeline_id}' last run was degraded "
                f"({record.get('recordsProcessed', 0)} processed, {failed_count} failed, "
                f"{warning_count} warnings)."
            ),
        }

    stale_threshold = _pipeline_stale_hours(pipeline_id)
    last_dt = _parse_dt(last_run_at)
    if stale_threshold and last_dt:
        age_hours = (_utc_now() - last_dt).total_seconds() / 3600
        if age_hours > stale_threshold:
            return {
                "overall": "warning",
                "severity": "high" if age_hours > stale_threshold * 2 else "medium",
                "type": f"pipeline_{pipeline_id}_stale",
                "message": (
                    f"Pipeline '{pipeline_id}' last ran {age_hours:.0f}h ago "
                    f"(threshold: {stale_threshold}h)."
                ),
                "ageHours": round(age_hours, 1),
            }
    elif stale_threshold and status not in {"unknown", "skipped"}:
        return {
            "overall": "warning",
            "severity": "medium",
            "type": f"pipeline_{pipeline_id}_never_run",
            "message": f"Pipeline '{pipeline_id}' has no lastRunAt timestamp.",
        }

    return {
        "overall": "ok",
        "severity": "none",
        "type": f"pipeline_{pipeline_id}_ok",
        "message": f"Pipeline '{pipeline_id}' status is ok.",
    }


def detect_missing_pipeline_status(
    expected_pipeline_ids: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    expected = list(expected_pipeline_ids or EXPECTED_PIPELINE_IDS)
    records = {item["pipelineId"]: item for item in list_pipeline_statuses(include_missing=False)}
    return [_missing_record(pipeline_id) for pipeline_id in expected if pipeline_id not in records]
