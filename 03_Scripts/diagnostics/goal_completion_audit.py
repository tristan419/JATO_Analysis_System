#!/usr/bin/env python3
"""Completion audit for the MSRP/finance/config/unified scraping goal.

This report is intentionally stricter than the P0 readiness checks. It keeps
local feature readiness separate from full PRD completion evidence such as
source-draft coverage and production deployment state.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
import json
from pathlib import Path
import socket
import sys
from typing import Any, Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = REPO_ROOT / "03_Scripts"
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(HERMES_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(HERMES_SCRIPT_DIR))

try:
    from pipeline_status_writer import write_pipeline_status
except ImportError:  # pragma: no cover - optional for isolated unit imports.
    write_pipeline_status = None  # type: ignore[assignment]

try:
    from msrp_source_review_queue import build_source_review_queue
except ImportError:  # pragma: no cover - optional for isolated unit imports.
    build_source_review_queue = None  # type: ignore[assignment]


SCHEMA_VERSION = "jato_goal_completion_audit_v2"
PIPELINE_ID = "goal_completion_audit"
PRICE_ALERT_REVIEW_QUEUE_SCHEMA_VERSION = "msrp_price_alert_review_queue_v1"
PRICE_ALERT_REVIEW_QUEUE_RELATIVE_PATH = (
    "03_Scripts/diagnostics/artifacts/msrp_price_alert_review_queue.json"
)
SOURCE_REVIEW_QUEUE_SCHEMA_VERSION = "msrp_source_review_queue_v1"
SOURCE_REVIEW_QUEUE_RELATIVE_PATH = (
    "03_Scripts/diagnostics/artifacts/msrp_source_review_queue.json"
)
SOURCE_REPAIR_BACKLOG_RELATIVE_PATH = (
    "03_Scripts/diagnostics/artifacts/msrp_source_repair_backlog.json"
)
SOURCE_REFERENCE_EVIDENCE_RELATIVE_PATH = (
    "03_Scripts/diagnostics/artifacts/msrp_source_reference_evidence.json"
)
DEFAULT_SOURCE_DRAFT_DIR = "07_ScrapingToolkit/source_drafts/suv_only_country_model_top30"
DEFAULT_REQUIRED_SOURCE_COUNTRIES = (
    "at",
    "be",
    "ch",
    "cz",
    "de",
    "dk",
    "es",
    "fi",
    "fr",
    "gr",
    "hr",
    "hu",
    "it",
    "nl",
    "no",
    "pl",
    "pt",
    "ro",
    "se",
    "si",
    "sk",
)
REQUIRED_MSRP_REQUIREMENT_KEYS = (
    "source_registry",
    "official_msrp_ingest",
    "weekly_snapshot",
    "current_price",
    "price_history",
    "price_alerts",
    "monitoring_events",
    "review_queue",
    "auto_review_scoring",
    "sales_effectiveness",
    "finance_monthly_lease_subsidy_net",
    "official_config_table_pipeline",
    "multi_source_reconciliation",
    "dryrun_governance",
    "pipeline_orchestration",
    "frontend_management_views",
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _resolve_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else repo_root / path


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(path)


def _safe_token(value: str | None, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    safe = "".join(ch if ch.isalnum() else "-" for ch in text)
    return "-".join(part for part in safe.split("-") if part) or fallback


def _history_suffix(report: dict[str, Any]) -> str:
    generated = str(report.get("generatedAtUtc") or _utc_now_iso())
    stamp = (
        generated.replace(":", "")
        .replace("-", "")
        .replace("+", "z")
        .replace(".", "-")
    )
    return _safe_token(stamp, "unknown-time")


def _csv_arg(value: str | Sequence[str]) -> tuple[str, ...]:
    values = value.split(",") if isinstance(value, str) else value
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        code = str(item).strip().lower()
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(code)
    return tuple(result)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _pipeline_status(repo_root: Path, pipeline_id: str) -> dict[str, Any]:
    path = repo_root / "hermes" / "reports" / "pipeline_status" / f"{pipeline_id}.json"
    payload = _read_json(path)
    if payload is None:
        return {
            "pipelineId": pipeline_id,
            "status": "missing",
            "statusPath": _display_path(path),
        }
    payload.setdefault("pipelineId", pipeline_id)
    payload["statusPath"] = _display_path(path)
    return payload


def _artifact_path(repo_root: Path, artifact_ref: object) -> Path:
    path = Path(str(artifact_ref or "")).expanduser()
    return path if path.is_absolute() else repo_root / path


def _read_msrp_readiness_report(
    repo_root: Path,
    status_record: dict[str, Any],
) -> dict[str, Any] | None:
    artifact_refs = status_record.get("artifactRefs")
    candidates: list[Path] = []
    if isinstance(artifact_refs, list):
        for artifact_ref in artifact_refs:
            path = _artifact_path(repo_root, artifact_ref)
            if path.name == "msrp_readiness_audit.json":
                candidates.insert(0, path)
            elif path.suffix == ".json" and "msrp_readiness_audit" in path.name:
                candidates.append(path)
    candidates.append(repo_root / "hermes" / "reports" / "msrp_readiness_audit.json")

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        payload = _read_json(path)
        if isinstance(payload, dict) and payload.get("schemaVersion") == "msrp_official_price_readiness_v1":
            return payload
    return None


def _requirement_by_key(report: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(report, dict):
        return {}
    requirements = report.get("requirements")
    if not isinstance(requirements, list):
        return {}
    keyed: dict[str, dict[str, Any]] = {}
    for item in requirements:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if key:
            keyed[key] = item
    return keyed


def _load_price_alert_review_queue_artifact(
    repo_root: Path,
) -> tuple[dict[str, Any] | None, str]:
    path = repo_root / PRICE_ALERT_REVIEW_QUEUE_RELATIVE_PATH
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return None, _display_path(path)
    return payload, _display_path(path)


def _load_source_review_queue_artifact(
    repo_root: Path,
) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
    queue_path = repo_root / SOURCE_REVIEW_QUEUE_RELATIVE_PATH
    backlog_path = repo_root / SOURCE_REPAIR_BACKLOG_RELATIVE_PATH
    reference_path = repo_root / SOURCE_REFERENCE_EVIDENCE_RELATIVE_PATH
    queue = _read_json(queue_path)
    backlog = _read_json(backlog_path)
    reference_evidence = _read_json(reference_path)
    backlog_run_id = (
        str(backlog.get("runId") or "").strip()
        if isinstance(backlog, dict)
        else ""
    )
    queue_run_id = (
        str(queue.get("backlogRunId") or "").strip()
        if isinstance(queue, dict)
        else ""
    )
    queue_matches_backlog = bool(
        isinstance(queue, dict)
        and (not backlog_run_id or not queue_run_id or queue_run_id == backlog_run_id)
    )
    meta: dict[str, Any] = {
        "sourceReviewQueuePath": _display_path(queue_path),
        "sourceRepairBacklogPath": _display_path(backlog_path),
        "sourceReferenceEvidencePath": _display_path(reference_path),
        "sourceRepairBacklogRunId": backlog_run_id or None,
        "sourceReviewQueueBacklogRunId": queue_run_id or None,
        "sourceReviewQueueBacklogRunMatches": queue_matches_backlog,
        "sourceReviewQueueSource": "missing",
    }
    if isinstance(queue, dict) and queue_matches_backlog:
        meta["sourceReviewQueueSource"] = "artifact"
        return queue, _display_path(queue_path), meta

    if isinstance(backlog, dict) and build_source_review_queue is not None:
        try:
            payload = build_source_review_queue(
                backlog,
                reference_evidence if isinstance(reference_evidence, dict) else None,
            )
            if isinstance(payload, dict):
                meta["sourceReviewQueueSource"] = "dynamic_backlog"
                meta["staleSourceReviewQueuePath"] = (
                    _display_path(queue_path) if isinstance(queue, dict) else None
                )
                meta["staleSourceReviewQueueBacklogRunId"] = queue_run_id or None
                meta["sourceReviewQueueBacklogRunId"] = payload.get("backlogRunId")
                meta["sourceReviewQueueBacklogRunMatches"] = True
                return payload, f"{_display_path(backlog_path)} (dynamic)", meta
        except Exception as exc:  # noqa: BLE001 - audit should report stale/missing queue.
            meta["sourceReviewQueueBuildError"] = str(exc)

    if isinstance(queue, dict):
        meta["sourceReviewQueueSource"] = "stale_artifact"
        return queue, _display_path(queue_path), meta
    return None, _display_path(queue_path), meta


def _msrp_detail_requirements(
    *,
    msrp_report: dict[str, Any] | None,
    msrp_status: dict[str, Any],
) -> list[dict[str, Any]]:
    by_key = _requirement_by_key(msrp_report)
    evidence_root = msrp_status.get("statusPath", "")
    requirements: list[dict[str, Any]] = []
    for key in REQUIRED_MSRP_REQUIREMENT_KEYS:
        detail = by_key.get(key)
        runtime = detail.get("runtime") if isinstance(detail, dict) and isinstance(detail.get("runtime"), dict) else {}
        evidence = []
        if isinstance(detail, dict):
            evidence = [str(item) for item in detail.get("evidence") or [] if item]
        if evidence_root:
            evidence.append(str(evidence_root))
        requirements.append(
            _requirement(
                key=f"msrp_{key}",
                title=str(detail.get("title") if isinstance(detail, dict) else key),
                status=str(detail.get("status") if isinstance(detail, dict) else "missing"),
                evidence=evidence,
                runtime=runtime,
                note=str(
                    detail.get("note")
                    if isinstance(detail, dict)
                    else "Detailed MSRP readiness evidence is missing."
                ),
            )
        )
    return requirements


def _dedupe_evidence(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    evidence: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        evidence.append(text)
    return evidence


def _requirements_by_report_key(
    requirements: Sequence[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("key") or ""): item
        for item in requirements
        if isinstance(item, dict) and item.get("key")
    }


def _price_alert_review_closure_requirement(
    msrp_detail_requirements: Sequence[dict[str, Any]],
    *,
    price_alert_review_queue: dict[str, Any] | None = None,
    price_alert_review_queue_path: str | None = None,
) -> dict[str, Any]:
    by_key = _requirements_by_report_key(msrp_detail_requirements)
    review = by_key.get("msrp_review_queue", {})
    sales = by_key.get("msrp_sales_effectiveness", {})
    alerts = by_key.get("msrp_price_alerts", {})
    review_runtime = (
        review.get("runtime")
        if isinstance(review.get("runtime"), dict)
        else {}
    )
    artifact_summary = (
        price_alert_review_queue.get("summary")
        if isinstance(price_alert_review_queue, dict)
        and isinstance(price_alert_review_queue.get("summary"), dict)
        else {}
    )

    def runtime_or_artifact(runtime_key: str, artifact_key: str) -> Any:
        if runtime_key in review_runtime:
            return review_runtime.get(runtime_key)
        return artifact_summary.get(artifact_key)

    schema_version = review_runtime.get("priceAlertReviewQueueSchemaVersion")
    if not schema_version and isinstance(price_alert_review_queue, dict):
        schema_version = price_alert_review_queue.get("schemaVersion")
    case_count = _safe_int(
        runtime_or_artifact("priceAlertReviewCaseCount", "totalCases"),
        0,
    )
    follow_up_count = _safe_int(
        runtime_or_artifact(
            "priceAlertReviewEffectivenessFollowUpCount",
            "effectivenessFollowUpCount",
        ),
        0,
    )
    linked_count = _safe_int(
        runtime_or_artifact(
            "priceAlertReviewEffectivenessLinkedCount",
            "effectivenessLinkedCount",
        ),
        0,
    )
    missing_count = _safe_int(
        runtime_or_artifact(
            "priceAlertReviewEffectivenessMissingCount",
            "effectivenessMissingCount",
        ),
        0,
    )
    queue_schema_ok = schema_version == PRICE_ALERT_REVIEW_QUEUE_SCHEMA_VERSION
    queue_covered = bool(
        review_runtime.get("priceAlertReviewQueueCovered")
        or queue_schema_ok
    )
    effectiveness_linkage_ok = (
        follow_up_count == 0
        or (linked_count >= follow_up_count and missing_count == 0)
    )
    base_paths_pass = (
        review.get("status") == "passed"
        and sales.get("status") == "passed"
        and alerts.get("status") == "passed"
        and queue_schema_ok
        and queue_covered
    )
    passed = base_paths_pass and effectiveness_linkage_ok
    degraded = (
        review.get("status") == "passed"
        and queue_schema_ok
        and queue_covered
        and not effectiveness_linkage_ok
    )
    evidence = _dedupe_evidence([
        *(review.get("evidence") or []),
        *(sales.get("evidence") or []),
        *(alerts.get("evidence") or []),
        review_runtime.get("priceAlertReviewQueuePath"),
        price_alert_review_queue_path if isinstance(price_alert_review_queue, dict) else None,
    ])

    return _requirement(
        key="msrp_price_alert_review_effectiveness_closure",
        title="Price alert review queue and sales-effectiveness closure",
        status="passed" if passed else "degraded" if degraded else "missing",
        evidence=evidence,
        runtime={
            "priceAlertReviewQueueSchemaVersion": schema_version,
            "priceAlertReviewCaseCount": case_count,
            "priceAlertReviewEffectivenessFollowUpCount": follow_up_count,
            "priceAlertReviewEffectivenessLinkedCount": linked_count,
            "priceAlertReviewEffectivenessMissingCount": missing_count,
            "priceAlertReviewQueueCovered": queue_covered,
            "effectivenessLinkageStatus": (
                "ok" if effectiveness_linkage_ok else "missing_linkage"
            ),
            "requiredSchemas": [PRICE_ALERT_REVIEW_QUEUE_SCHEMA_VERSION],
            "dependentRequirements": {
                "reviewQueue": review.get("status"),
                "salesEffectiveness": sales.get("status"),
                "priceAlerts": alerts.get("status"),
            },
        },
        note=(
            "Full goal completion requires the weekly price-alert review queue "
            "to expose the v1 schema and link every sales-effectiveness "
            "follow-up back to an analyzed price event."
        ),
    )


def _source_review_queue_coverage_requirement(
    msrp_detail_requirements: Sequence[dict[str, Any]],
    *,
    source_review_queue: dict[str, Any] | None = None,
    source_review_queue_path: str | None = None,
    source_review_queue_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    by_key = _requirements_by_report_key(msrp_detail_requirements)
    dryrun = by_key.get("msrp_dryrun_governance", {})
    dryrun_runtime = (
        dryrun.get("runtime")
        if isinstance(dryrun.get("runtime"), dict)
        else {}
    )
    artifact_summary = (
        source_review_queue.get("summary")
        if isinstance(source_review_queue, dict)
        and isinstance(source_review_queue.get("summary"), dict)
        else {}
    )

    def runtime_or_artifact(runtime_key: str, artifact_key: str) -> Any:
        if artifact_key in artifact_summary:
            return artifact_summary.get(artifact_key)
        return dryrun_runtime.get(runtime_key)

    schema_version = dryrun_runtime.get("sourceReviewQueueSchemaVersion")
    if not schema_version and isinstance(source_review_queue, dict):
        schema_version = source_review_queue.get("schemaVersion")
    case_count = _safe_int(
        runtime_or_artifact("sourceReviewQueueCaseCount", "totalCases"),
        0,
    )
    source_repair_count = _safe_int(
        runtime_or_artifact("sourceRepairIssueCount", "sourceRepairCount"),
        0,
    )
    transient_recheck_count = _safe_int(
        runtime_or_artifact("transientRecheckCount", "transientRecheckCount"),
        0,
    )
    business_resolution_count = _safe_int(
        runtime_or_artifact("businessResolutionCount", "businessResolutionCount"),
        0,
    )
    expected_count = 0
    if not artifact_summary:
        expected_count = _safe_int(
            dryrun_runtime.get("sourceReviewQueueExpectedCaseCount"),
            0,
        )
    if artifact_summary or expected_count <= 0:
        expected_count = (
            source_repair_count
            + transient_recheck_count
            + business_resolution_count
        )
    schema_ok = schema_version == SOURCE_REVIEW_QUEUE_SCHEMA_VERSION
    queue_matches_backlog = bool(
        (source_review_queue_meta or {}).get(
            "sourceReviewQueueBacklogRunMatches",
            True,
        )
    )
    if isinstance(source_review_queue, dict):
        queue_complete = bool(
            schema_ok
            and queue_matches_backlog
            and (expected_count <= 0 or case_count >= expected_count)
        )
    else:
        queue_complete = bool(
            dryrun_runtime.get("sourceReviewQueueComplete")
            and schema_ok
            and (expected_count <= 0 or case_count >= expected_count)
        )
    passed = dryrun.get("status") == "passed" and queue_complete
    degraded = (
        dryrun.get("status") in {"passed", "degraded"}
        and schema_ok
        and case_count > 0
        and not queue_complete
    )
    evidence = _dedupe_evidence([
        *(dryrun.get("evidence") or []),
        "GET /hermes/msrp-country-progress",
        source_review_queue_path if isinstance(source_review_queue, dict) else None,
    ])

    return _requirement(
        key="msrp_source_review_queue_coverage",
        title="MSRP source repair and transient recheck queue coverage",
        status="passed" if passed else "degraded" if degraded else "missing",
        evidence=evidence,
        runtime={
            "sourceReviewQueueSchemaVersion": schema_version,
            "sourceReviewQueueSchemaOk": schema_ok,
            "sourceReviewQueueCaseCount": case_count,
            "sourceReviewQueueExpectedCaseCount": expected_count,
            "sourceReviewQueueComplete": queue_complete,
            "sourceReviewQueuePath": source_review_queue_path,
            "sourceReviewQueueBacklogRunId": (
                source_review_queue.get("backlogRunId")
                if isinstance(source_review_queue, dict)
                else None
            ),
            "sourceReviewQueueBacklogRunMatches": queue_matches_backlog,
            "sourceReviewQueueSource": (source_review_queue_meta or {}).get(
                "sourceReviewQueueSource",
            ),
            "sourceRepairBacklogPath": (source_review_queue_meta or {}).get(
                "sourceRepairBacklogPath",
            ),
            "sourceRepairBacklogRunId": (source_review_queue_meta or {}).get(
                "sourceRepairBacklogRunId",
            ),
            "sourceReviewQueueBuildError": (source_review_queue_meta or {}).get(
                "sourceReviewQueueBuildError",
            ),
            "sourceRepairIssueCount": source_repair_count,
            "transientRecheckCount": transient_recheck_count,
            "businessResolutionCount": business_resolution_count,
            "dependentRequirements": {
                "dryrunGovernance": dryrun.get("status"),
            },
            "requiredSchemas": [SOURCE_REVIEW_QUEUE_SCHEMA_VERSION],
        },
        note=(
            "Full MSRP completion requires Hermes dryrun governance to expose "
            "a complete source-review queue covering every source repair, "
            "business-resolution, and transient recheck case."
        ),
    )


def _requirement(
    *,
    key: str,
    title: str,
    status: str,
    evidence: list[str],
    runtime: dict[str, Any],
    note: str,
) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "status": status,
        "evidence": evidence,
        "runtime": runtime,
        "note": note,
    }


def _overall_status(requirements: Sequence[dict[str, Any]]) -> str:
    statuses = {str(item.get("status") or "unknown") for item in requirements}
    if statuses & {"missing", "failed", "not_checked"}:
        return "in_progress"
    if "degraded" in statuses:
        return "degraded"
    return "complete"


def _iter_source_yaml_files(source_draft_dir: Path) -> Iterable[Path]:
    for path in sorted(source_draft_dir.rglob("*.yaml")):
        relative_parts = path.relative_to(source_draft_dir).parts
        if path.name.startswith("_") or any(part.startswith("_") for part in relative_parts):
            continue
        yield path


def _source_draft_coverage(
    source_draft_dir: Path,
    required_countries: Sequence[str],
) -> dict[str, Any]:
    files = list(_iter_source_yaml_files(source_draft_dir))
    by_country = Counter(path.relative_to(source_draft_dir).parts[0] for path in files)
    required = set(required_countries)
    missing_countries = sorted(required - set(by_country))
    todo_files: list[str] = []
    todo_count = 0
    for path in files:
        text = path.read_text(encoding="utf-8", errors="replace")
        count = text.count("TODO_")
        if count:
            todo_count += count
            todo_files.append(_display_path(path))
    return {
        "sourceDraftDir": _display_path(source_draft_dir),
        "sourceDraftCount": len(files),
        "countryCount": len(by_country),
        "filesByCountry": dict(sorted(by_country.items())),
        "missingRequiredCountries": missing_countries,
        "todoPlaceholderCount": todo_count,
        "todoFileCount": len(todo_files),
        "todoSampleFiles": todo_files[:20],
    }


@contextmanager
def _resolve_hostname_to_ip(hostname: str | None, resolve_ip: str | None) -> Iterable[None]:
    if not hostname or not resolve_ip:
        yield
        return
    original_getaddrinfo = socket.getaddrinfo

    def patched_getaddrinfo(host: str, port: Any, *args: Any, **kwargs: Any):
        target_host = resolve_ip if host == hostname else host
        return original_getaddrinfo(target_host, port, *args, **kwargs)

    socket.getaddrinfo = patched_getaddrinfo
    try:
        yield
    finally:
        socket.getaddrinfo = original_getaddrinfo


def _fetch_json(
    url: str,
    timeout_seconds: int,
    *,
    resolve_ip: str | None = None,
) -> tuple[dict[str, Any] | None, str | None, int | None]:
    try:
        request = Request(url, headers={"User-Agent": "codex-goal-completion-audit"})
        hostname = urlparse(url).hostname
        with _resolve_hostname_to_ip(hostname, resolve_ip):
            with urlopen(request, timeout=max(1, timeout_seconds)) as response:
                payload = json.loads(response.read())
        return payload if isinstance(payload, dict) else {}, None, 200
    except HTTPError as exc:
        detail = exc.read(500).decode("utf-8", errors="replace")
        return None, detail, exc.code
    except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        return None, str(exc), None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _effective_progress_gate(progress: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(progress, dict):
        return {
            "effectiveGateStatus": "missing",
            "effectiveGateBasis": "missing",
            "stableCoverage": {},
        }

    progress_status = progress.get("status")
    if not isinstance(progress_status, dict):
        progress_status = {}
    stable = progress.get("stableCoverage")
    if not isinstance(stable, dict):
        stable = {}

    active_gate = str(progress_status.get("gateStatus") or "").strip().lower()
    if active_gate == "allowed":
        return {
            "effectiveGateStatus": "allowed",
            "effectiveGateBasis": "active",
            "stableCoverage": stable,
        }

    threshold = _safe_float(
        stable.get("gateThreshold", progress_status.get("gateThreshold")),
        70.0,
    )
    stable_pass_rate = _safe_float(
        stable.get("sourcePassRate", stable.get("stablePassRate")),
        0.0,
    )
    ready_country_count = _safe_int(stable.get("readyCountryCount"), 0)
    blocked_country_count = _safe_int(stable.get("blockedCountryCount"), 0)
    stable_ready = (
        stable_pass_rate >= threshold
        and ready_country_count > 0
        and blocked_country_count == 0
        and not bool(stable.get("activeRunRunning"))
        and not bool(stable.get("activeRunPartial"))
    )
    return {
        "effectiveGateStatus": "allowed" if stable_ready else "blocked",
        "effectiveGateBasis": "stable" if stable_ready else "active",
        "stableCoverage": stable,
    }


def _remote_price_alert_review_queue(progress: dict[str, Any] | None) -> dict[str, Any]:
    queue = progress.get("priceAlertReviewQueue") if isinstance(progress, dict) else {}
    if not isinstance(queue, dict):
        queue = {}
    summary = queue.get("summary")
    if not isinstance(summary, dict):
        summary = {}
    follow_up_count = _safe_int(summary.get("effectivenessFollowUpCount"), 0)
    linked_count = _safe_int(summary.get("effectivenessLinkedCount"), 0)
    missing_count = _safe_int(summary.get("effectivenessMissingCount"), 0)
    linkage_ok = (
        follow_up_count == 0
        or (linked_count >= follow_up_count and missing_count == 0)
    )
    schema_version = queue.get("schemaVersion")
    schema_ok = schema_version == PRICE_ALERT_REVIEW_QUEUE_SCHEMA_VERSION
    return {
        "schemaVersion": schema_version,
        "schemaOk": schema_ok,
        "snapshotWeek": queue.get("snapshotWeek"),
        "totalCases": _safe_int(summary.get("totalCases"), 0),
        "highPriorityAlertCount": _safe_int(summary.get("highPriorityAlertCount"), 0),
        "missingEvidenceCount": _safe_int(summary.get("missingEvidenceCount"), 0),
        "effectivenessFollowUpCount": follow_up_count,
        "effectivenessLinkedCount": linked_count,
        "effectivenessMissingCount": missing_count,
        "effectivenessLinkageStatus": "ok" if linkage_ok else "missing_linkage",
        "passed": schema_ok and linkage_ok,
    }


def _remote_source_review_queue(progress: dict[str, Any] | None) -> dict[str, Any]:
    queue = progress.get("sourceReviewQueue") if isinstance(progress, dict) else {}
    if not isinstance(queue, dict):
        queue = {}
    summary = queue.get("summary")
    if not isinstance(summary, dict):
        summary = {}
    backlog = progress.get("sourceRepairBacklog") if isinstance(progress, dict) else {}
    if not isinstance(backlog, dict):
        backlog = {}

    source_repair_count = _safe_int(summary.get("sourceRepairCount"), 0)
    business_resolution_count = _safe_int(summary.get("businessResolutionCount"), 0)
    transient_recheck_count = _safe_int(summary.get("transientRecheckCount"), 0)
    expected_count = (
        source_repair_count
        + business_resolution_count
        + transient_recheck_count
    )
    backlog_expected_count = (
        _safe_int(
            backlog.get("sourceRepairIssueCount", backlog.get("totalIssueCount")),
            0,
        )
        + _safe_int(backlog.get("businessResolutionCount"), 0)
        + _safe_int(backlog.get("transientRegressionCount"), 0)
    )
    if backlog_expected_count > 0:
        expected_count = backlog_expected_count

    schema_version = queue.get("schemaVersion")
    schema_ok = schema_version == SOURCE_REVIEW_QUEUE_SCHEMA_VERSION
    case_count = _safe_int(summary.get("totalCases"), 0)
    complete = schema_ok and (expected_count <= 0 or case_count >= expected_count)
    return {
        "schemaVersion": schema_version,
        "schemaOk": schema_ok,
        "totalCases": case_count,
        "expectedCases": expected_count,
        "sourceRepairCount": source_repair_count,
        "businessResolutionCount": business_resolution_count,
        "transientRecheckCount": transient_recheck_count,
        "queueCoverageStatus": "complete" if complete else "incomplete",
        "passed": complete,
    }


def _remote_checks(
    remote_api_base: str | None,
    timeout_seconds: int,
    *,
    resolve_ip: str | None = None,
) -> dict[str, Any]:
    if not remote_api_base:
        return {
            "status": "not_checked",
            "note": "Pass --remote-api-base to verify deployed API state.",
        }
    base = remote_api_base.rstrip("/")
    snapshot, snapshot_error, snapshot_code = _fetch_json(
        f"{base}/msrp/current-prices/snapshot",
        timeout_seconds,
        resolve_ip=resolve_ip,
    )
    progress, progress_error, progress_code = _fetch_json(
        f"{base}/hermes/msrp-country-progress",
        timeout_seconds,
        resolve_ip=resolve_ip,
    )
    unified, unified_error, unified_code = _fetch_json(
        f"{base}/hermes/pipeline/status/unified_scraping_readiness",
        timeout_seconds,
        resolve_ip=resolve_ip,
    )
    monitoring, monitoring_error, monitoring_code = _fetch_json(
        f"{base}/msrp/monitoring/events",
        timeout_seconds,
        resolve_ip=resolve_ip,
    )
    progress_status = progress.get("status") if isinstance(progress, dict) else {}
    if not isinstance(progress_status, dict):
        progress_status = {}
    progress_gate = _effective_progress_gate(progress)
    stable_coverage = progress_gate["stableCoverage"]
    price_alert_review_queue = _remote_price_alert_review_queue(progress)
    source_review_queue = _remote_source_review_queue(progress)
    passed = (
        snapshot_code == 200
        and isinstance(snapshot, dict)
        and snapshot.get("schemaVersion") == "msrp_current_price_snapshot_v1"
        and progress_code == 200
        and progress_gate["effectiveGateStatus"] == "allowed"
        and unified_code == 200
        and isinstance(unified, dict)
        and unified.get("status") == "success"
        and monitoring_code == 200
        and isinstance(monitoring, dict)
        and monitoring.get("schemaVersion") == "msrp_monitoring_events_v1"
        and price_alert_review_queue["passed"]
        and source_review_queue["passed"]
    )
    return {
        "status": "passed" if passed else "missing",
        "apiBase": base,
        "resolveIp": resolve_ip,
        "snapshot": {
            "httpStatus": snapshot_code,
            "schemaVersion": snapshot.get("schemaVersion") if isinstance(snapshot, dict) else None,
            "snapshotWeek": snapshot.get("snapshotWeek") if isinstance(snapshot, dict) else None,
            "error": snapshot_error,
        },
        "msrpCountryProgress": {
            "httpStatus": progress_code,
            "runId": progress_status.get("runId"),
            "gateStatus": progress_status.get("gateStatus"),
            "effectiveGateStatus": progress_gate["effectiveGateStatus"],
            "effectiveGateBasis": progress_gate["effectiveGateBasis"],
            "overallPassPct": progress_status.get("overallPassPct"),
            "stableCoverage": {
                "gateThreshold": stable_coverage.get("gateThreshold"),
                "stablePassRate": stable_coverage.get("stablePassRate"),
                "sourcePassRate": stable_coverage.get("sourcePassRate"),
                "readyCountryCount": stable_coverage.get("readyCountryCount"),
                "blockedCountryCount": stable_coverage.get("blockedCountryCount"),
                "probeRegressionCount": stable_coverage.get("probeRegressionCount"),
                "latestRunId": stable_coverage.get("latestRunId"),
                "activeRunId": stable_coverage.get("activeRunId"),
            },
            "error": progress_error,
        },
        "unifiedScrapingReadiness": {
            "httpStatus": unified_code,
            "status": unified.get("status") if isinstance(unified, dict) else None,
            "readinessStatus": unified.get("readinessStatus") if isinstance(unified, dict) else None,
            "error": unified_error,
        },
        "msrpMonitoringEvents": {
            "httpStatus": monitoring_code,
            "schemaVersion": monitoring.get("schemaVersion") if isinstance(monitoring, dict) else None,
            "eventCount": (
                monitoring.get("summary", {}).get("eventCount")
                if isinstance(monitoring, dict) and isinstance(monitoring.get("summary"), dict)
                else None
            ),
            "timelineEventCount": (
                monitoring.get("summary", {}).get("timelineEventCount")
                if isinstance(monitoring, dict) and isinstance(monitoring.get("summary"), dict)
                else None
            ),
            "sourceRiskCount": (
                monitoring.get("summary", {}).get("sourceRiskCount")
                if isinstance(monitoring, dict) and isinstance(monitoring.get("summary"), dict)
                else None
            ),
            "warningCount": (
                len(monitoring.get("warnings"))
                if isinstance(monitoring, dict) and isinstance(monitoring.get("warnings"), list)
                else None
            ),
            "error": monitoring_error,
        },
        "priceAlertReviewQueue": {
            key: value
            for key, value in price_alert_review_queue.items()
            if key != "passed"
        },
        "sourceReviewQueue": {
            key: value
            for key, value in source_review_queue.items()
            if key != "passed"
        },
    }


def build_goal_completion_report(
    *,
    repo_root: str | Path | None = None,
    source_draft_dir: str | Path = DEFAULT_SOURCE_DRAFT_DIR,
    required_source_countries: Sequence[str] = DEFAULT_REQUIRED_SOURCE_COUNTRIES,
    remote_api_base: str | None = None,
    remote_resolve_ip: str | None = None,
    timeout_seconds: int = 15,
) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve() if repo_root else REPO_ROOT
    resolved_source_dir = _resolve_path(root, source_draft_dir)
    msrp_status = _pipeline_status(root, "msrp_readiness_audit")
    msrp_report = _read_msrp_readiness_report(root, msrp_status)
    price_alert_review_queue, price_alert_review_queue_path = (
        _load_price_alert_review_queue_artifact(root)
    )
    (
        source_review_queue,
        source_review_queue_path,
        source_review_queue_meta,
    ) = _load_source_review_queue_artifact(root)
    msrp_detail_requirements = _msrp_detail_requirements(
        msrp_report=msrp_report,
        msrp_status=msrp_status,
    )
    msrp_review_closure_requirement = _price_alert_review_closure_requirement(
        msrp_detail_requirements,
        price_alert_review_queue=price_alert_review_queue,
        price_alert_review_queue_path=price_alert_review_queue_path,
    )
    msrp_source_review_queue_requirement = _source_review_queue_coverage_requirement(
        msrp_detail_requirements,
        source_review_queue=source_review_queue,
        source_review_queue_path=source_review_queue_path,
        source_review_queue_meta=source_review_queue_meta,
    )
    msrp_completion_requirements = [
        *msrp_detail_requirements,
        msrp_review_closure_requirement,
        msrp_source_review_queue_requirement,
    ]
    unified_status = _pipeline_status(root, "unified_scraping_readiness")
    source_coverage = _source_draft_coverage(
        resolved_source_dir,
        required_source_countries,
    )
    remote = _remote_checks(
        remote_api_base,
        timeout_seconds,
        resolve_ip=remote_resolve_ip,
    )

    msrp_missing_keys = [
        item["key"]
        for item in msrp_completion_requirements
        if item.get("status") != "passed"
    ]
    msrp_ready = (
        msrp_status.get("status") == "success"
        and msrp_status.get("readinessStatus") == "passed"
        and not msrp_missing_keys
    )
    unified_ready = (
        unified_status.get("status") == "success"
        and unified_status.get("readinessStatus") == "passed"
        and unified_status.get("contractStatus") == "ok"
        and unified_status.get("stageStatus") == "ok"
    )
    source_status = "passed"
    if source_coverage["missingRequiredCountries"]:
        source_status = "missing"
    elif source_coverage["todoPlaceholderCount"] > 0:
        source_status = "degraded"

    requirements = [
        _requirement(
            key="msrp_official_price_p0",
            title="MSRP official price local P0 chain",
            status="passed" if msrp_ready else "missing",
            evidence=[msrp_status.get("statusPath", "")],
            runtime=msrp_status,
            note=(
                "Aggregate gate: every detailed MSRP readiness requirement "
                "and the review-queue sales-effectiveness closure below must "
                "be passed."
            ),
        ),
        *msrp_detail_requirements,
        msrp_review_closure_requirement,
        msrp_source_review_queue_requirement,
        _requirement(
            key="unified_scraping_contract_and_stage",
            title="Unified ScrapeJob contract and stage smoke",
            status="passed" if unified_ready else "missing",
            evidence=[unified_status.get("statusPath", "")],
            runtime=unified_status,
            note="Covers MSRP/news/VOC/policy/incentive/spec job mapping and fixture Fetcher->Extractor->Normalizer->Sink stages.",
        ),
        _requirement(
            key="msrp_21_country_source_draft_coverage",
            title="MSRP 21-country SUV Top30 source draft coverage",
            status=source_status,
            evidence=[source_coverage["sourceDraftDir"]],
            runtime=source_coverage,
            note="Full PRD completion still requires eliminating TODO placeholders and validating real source extraction across all countries.",
        ),
        _requirement(
            key="production_deployment_state",
            title="Production deployment reflects current readiness",
            status=str(remote.get("status") or "not_checked"),
            evidence=[remote.get("apiBase", "")] if remote.get("apiBase") else [],
            runtime=remote,
            note="Production is complete only when deployed API exposes current snapshot, monitoring events, effective dryrun gate, and unified readiness success.",
        ),
    ]
    status_counts = dict(sorted(Counter(item["status"] for item in requirements).items()))
    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": _overall_status(requirements),
        "generatedAtUtc": _utc_now_iso(),
        "summary": {
            "requirementCount": len(requirements),
            "statusCounts": status_counts,
            "localP0Ready": msrp_ready and unified_ready,
            "msrpReady": msrp_ready,
            "unifiedReady": unified_ready,
            "msrpDetailedRequirementCount": len(msrp_detail_requirements),
            "msrpDetailedPassedCount": sum(
                1 for item in msrp_detail_requirements
                if item.get("status") == "passed"
            ),
            "msrpCompletionRequirementCount": len(msrp_completion_requirements),
            "msrpCompletionPassedCount": sum(
                1 for item in msrp_completion_requirements
                if item.get("status") == "passed"
            ),
            "msrpMissingRequirementKeys": msrp_missing_keys,
            "priceAlertReviewCaseCount": (
                msrp_review_closure_requirement["runtime"].get(
                    "priceAlertReviewCaseCount"
                )
            ),
            "priceAlertReviewEffectivenessLinkedCount": (
                msrp_review_closure_requirement["runtime"].get(
                    "priceAlertReviewEffectivenessLinkedCount"
                )
            ),
            "priceAlertReviewEffectivenessMissingCount": (
                msrp_review_closure_requirement["runtime"].get(
                    "priceAlertReviewEffectivenessMissingCount"
                )
            ),
            "sourceReviewQueueCaseCount": (
                msrp_source_review_queue_requirement["runtime"].get(
                    "sourceReviewQueueCaseCount"
                )
            ),
            "sourceReviewQueueExpectedCaseCount": (
                msrp_source_review_queue_requirement["runtime"].get(
                    "sourceReviewQueueExpectedCaseCount"
                )
            ),
            "sourceReviewQueueComplete": (
                msrp_source_review_queue_requirement["runtime"].get(
                    "sourceReviewQueueComplete"
                )
            ),
            "sourceDraftTodoPlaceholderCount": source_coverage["todoPlaceholderCount"],
            "sourceDraftCountryCount": source_coverage["countryCount"],
            "productionStatus": remote.get("status"),
        },
        "requirements": requirements,
    }


def _markdown_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    lines = [
        "# JATO MSRP / Finance / Config / Unified Goal Completion Audit",
        "",
        f"**Generated:** {report.get('generatedAtUtc', '-')}",
        f"**Status:** {report.get('status', '-')}",
        f"**Local P0 ready:** {summary.get('localP0Ready', False)}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Requirements | {summary.get('requirementCount', 0)} |",
        f"| Passed | {(summary.get('statusCounts') or {}).get('passed', 0)} |",
        f"| Degraded | {(summary.get('statusCounts') or {}).get('degraded', 0)} |",
        f"| Missing | {(summary.get('statusCounts') or {}).get('missing', 0)} |",
        f"| Not checked | {(summary.get('statusCounts') or {}).get('not_checked', 0)} |",
        f"| MSRP detailed passed | {summary.get('msrpDetailedPassedCount', 0)} / {summary.get('msrpDetailedRequirementCount', 0)} |",
        f"| MSRP completion passed | {summary.get('msrpCompletionPassedCount', 0)} / {summary.get('msrpCompletionRequirementCount', 0)} |",
        f"| Price-alert review cases | {summary.get('priceAlertReviewCaseCount', 0)} |",
        f"| Price-alert linked effectiveness | {summary.get('priceAlertReviewEffectivenessLinkedCount', 0)} |",
        f"| Price-alert missing effectiveness | {summary.get('priceAlertReviewEffectivenessMissingCount', 0)} |",
        f"| Source-review queue cases | {summary.get('sourceReviewQueueCaseCount', 0)} |",
        f"| Source-review queue expected | {summary.get('sourceReviewQueueExpectedCaseCount', 0)} |",
        f"| Source-review queue complete | {summary.get('sourceReviewQueueComplete', False)} |",
        f"| Source draft countries | {summary.get('sourceDraftCountryCount', 0)} |",
        f"| Source TODO placeholders | {summary.get('sourceDraftTodoPlaceholderCount', 0)} |",
        "",
        "## Requirements",
        "",
        "| Key | Status | Note |",
        "|---|---|---|",
    ]
    for item in report.get("requirements") or []:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_cell(item.get("key")),
                    _markdown_cell(item.get("status")),
                    _markdown_cell(item.get("note")),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_outputs(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_root = Path(out_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    latest_json = output_root / "goal_completion_audit.json"
    latest_md = output_root / "goal_completion_audit.md"
    suffix = _history_suffix(report)
    hist_json = output_root / f"goal_completion_audit_{suffix}.json"
    hist_md = output_root / f"goal_completion_audit_{suffix}.md"
    json_text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    md_text = _render_markdown(report)
    latest_json.write_text(json_text, encoding="utf-8")
    latest_md.write_text(md_text, encoding="utf-8")
    hist_json.write_text(json_text, encoding="utf-8")
    hist_md.write_text(md_text, encoding="utf-8")
    return {
        "latestJson": _display_path(latest_json),
        "latestMarkdown": _display_path(latest_md),
        "historicalJson": _display_path(hist_json),
        "historicalMarkdown": _display_path(hist_md),
    }


def write_status_record(
    report: dict[str, Any],
    *,
    artifact_refs: Sequence[str],
) -> dict[str, Any] | None:
    if write_pipeline_status is None:
        return None
    status = str(report.get("status") or "in_progress")
    pipeline_status = "success" if status == "complete" else "degraded" if status == "degraded" else "failed"
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    status_counts = summary.get("statusCounts") if isinstance(summary.get("statusCounts"), dict) else {}
    failed_count = int(status_counts.get("missing") or 0) + int(status_counts.get("not_checked") or 0)
    warning_count = int(status_counts.get("degraded") or 0)
    return write_pipeline_status(
        pipeline_id=PIPELINE_ID,
        status=pipeline_status,
        started_at=report.get("generatedAtUtc"),
        finished_at=_utc_now_iso(),
        exit_code=0 if status in {"complete", "degraded"} else 1,
        records_processed=int(summary.get("requirementCount") or 0),
        failed_count=failed_count,
        warning_count=warning_count,
        artifact_refs=list(artifact_refs),
        source=PIPELINE_ID,
        message=(
            f"Goal completion={status}; localP0={summary.get('localP0Ready')}, "
            f"sourceTODO={summary.get('sourceDraftTodoPlaceholderCount')}, "
            f"production={summary.get('productionStatus')}."
        ),
        extra={
            "goalCompletionStatus": status,
            "localP0Ready": summary.get("localP0Ready", False),
            "statusCounts": status_counts,
            "msrpMissingRequirementKeys": summary.get("msrpMissingRequirementKeys", []),
            "priceAlertReviewCaseCount": summary.get("priceAlertReviewCaseCount", 0),
            "priceAlertReviewEffectivenessLinkedCount": summary.get(
                "priceAlertReviewEffectivenessLinkedCount",
                0,
            ),
            "priceAlertReviewEffectivenessMissingCount": summary.get(
                "priceAlertReviewEffectivenessMissingCount",
                0,
            ),
            "sourceReviewQueueCaseCount": summary.get("sourceReviewQueueCaseCount", 0),
            "sourceReviewQueueExpectedCaseCount": summary.get(
                "sourceReviewQueueExpectedCaseCount",
                0,
            ),
            "sourceReviewQueueComplete": summary.get("sourceReviewQueueComplete", False),
            "sourceDraftTodoPlaceholderCount": summary.get("sourceDraftTodoPlaceholderCount", 0),
            "productionStatus": summary.get("productionStatus"),
        },
        repo_root=REPO_ROOT,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit full goal completion across MSRP, finance, official config, and unified scraping.",
    )
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--source-draft-dir", default=DEFAULT_SOURCE_DRAFT_DIR)
    parser.add_argument("--required-source-countries", default=",".join(DEFAULT_REQUIRED_SOURCE_COUNTRIES))
    parser.add_argument("--required-ai-countries", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--remote-api-base", default=None)
    parser.add_argument(
        "--remote-resolve-ip",
        default=None,
        help="Resolve the remote API hostname to this IP while keeping the URL host for TLS/SNI.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=15)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--write-status", action="store_true")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless status is complete.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_goal_completion_report(
        repo_root=args.repo_root,
        source_draft_dir=args.source_draft_dir,
        required_source_countries=_csv_arg(args.required_source_countries),
        remote_api_base=args.remote_api_base,
        remote_resolve_ip=args.remote_resolve_ip,
        timeout_seconds=max(1, int(args.timeout_seconds)),
    )
    artifacts: dict[str, str] = {}
    if args.out_dir:
        artifacts = write_outputs(report, args.out_dir)
        report["reportArtifacts"] = artifacts
    if args.write_status:
        status_record = write_status_record(report, artifact_refs=list(artifacts.values()))
        if status_record is not None:
            report["pipelineStatus"] = status_record
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.strict and report["status"] != "complete":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
