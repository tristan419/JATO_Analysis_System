"""Hermes Governance Layer — API routes.

Read/write Hermes JSON reports, registry files, and script execution.
"""

from __future__ import annotations

import json
import os
import re
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query
from fastapi.responses import PlainTextResponse

from app.core.security import ROLE_LEVEL, get_current_user, require_min_role
from app.services.hermes_cost_ledger_service import (
    build_daily_cost_heatmap,
    load_cost_records,
)
from app.services.hermes_ops_runner_service import (
    HELP_TEXT,
    HERMES_SCRIPTS,
    HermesRunError,
    execute_hermes_command,
    get_command_help,
    list_run_commands,
)

router = APIRouter(prefix="/hermes", tags=["hermes"])

# routes/hermes.py → api/routes → app/api → backend/app → 06_AppPlatform → repo_root
PROJECT_ROOT = Path(__file__).resolve().parents[5]
HERMES_DIR = PROJECT_ROOT / "hermes"
SCRIPTS_DIR = PROJECT_ROOT / "03_Scripts" / "hermes"
REPORTS_DIR = HERMES_DIR / "reports"
ACTIVITY_LOG = HERMES_DIR / "activity_log.jsonl"
BUDGET_DAILY_CNY = 20
BUDGET_MONTHLY_CNY = 500
ALERT_EMAIL = "tristanlyk@gmail.com"
SOURCE_URL_PATTERN = re.compile(r"https?://[^\s\"')<>]+")
EXTERNAL_ACCESS_FAILURES = {
    "forbidden_403",
    "anti_bot_access_denied",
}
TRANSIENT_RECHECK_FAILURES = {
    "http_timeout",
    "dns_resolution_failed",
    "network_unavailable",
    "dynamic_price_not_ready",
    "js_required_or_selector_timeout",
}
PIPELINE_RUNTIME_FAILURES = {
    "db_or_backend_write_failed",
    "runner_browser_launch_failed",
}
MSRP_COUNTRY_LABELS = {
    "at": "Austria",
    "be": "Belgium",
    "ch": "Switzerland",
    "cz": "Czech Republic",
    "de": "Germany",
    "dk": "Denmark",
    "es": "Spain",
    "fi": "Finland",
    "fr": "France",
    "gr": "Greece",
    "hr": "Croatia",
    "hu": "Hungary",
    "it": "Italy",
    "nl": "Netherlands",
    "no": "Norway",
    "pl": "Poland",
    "pt": "Portugal",
    "ro": "Romania",
    "se": "Sweden",
    "si": "Slovenia",
    "sk": "Slovakia",
}

def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Not found: {path.relative_to(PROJECT_ROOT)}")
    return json.loads(path.read_text())


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _msrp_country_label(country_code: str) -> str:
    code = str(country_code or "").strip().lower()
    return MSRP_COUNTRY_LABELS.get(code, code.upper())


def _msrp_run_recency_key(run: dict[str, Any]) -> tuple[str, str]:
    run_id = str(run.get("runId") or "")
    timestamp = str(
        run.get("finishedAt")
        or run.get("startedAt")
        or run.get("updatedAt")
        or ""
    )
    return timestamp, run_id


def _sort_msrp_runs_index(index_data: dict[str, Any]) -> dict[str, Any]:
    runs = index_data.get("runs")
    if not isinstance(runs, list):
        return index_data
    return {
        **index_data,
        "runs": sorted(
            [run for run in runs if isinstance(run, dict)],
            key=_msrp_run_recency_key,
            reverse=True,
        ),
    }


def _msrp_artifacts_dir() -> Path:
    return PROJECT_ROOT / "03_Scripts" / "diagnostics" / "artifacts"


def _default_source_repair_backlog() -> dict[str, Any]:
    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": None,
        "generatedAt": None,
        "totalIssueCount": 0,
        "transientRegressionCount": 0,
        "sourceRepairIssueCount": 0,
        "externalAccessIssueCount": 0,
        "externalAccessIssues": [],
        "pipelineIssueCount": 0,
        "pipelineIssues": [],
        "topSourceHosts": [],
        "groups": [],
    }


def _default_source_reference_evidence() -> dict[str, Any]:
    return {
        "schemaVersion": "msrp_source_reference_evidence_v1",
        "generatedAt": None,
        "backlogRunId": None,
        "referenceSource": "EVKX",
        "referencePolicy": "reference_only_review_required",
        "officialSourceRequiredForIngest": True,
        "officialIngestEligible": False,
        "summary": {
            "evidenceItemCount": 0,
            "localReferenceCount": 0,
            "missingLocalReferenceCount": 0,
            "officialIngestEligibleCount": 0,
        },
        "items": [],
    }


def _default_source_accessibility_audit() -> dict[str, Any]:
    return {
        "schemaVersion": "msrp_source_accessibility_audit_v1",
        "generatedAt": None,
        "backlogRunId": None,
        "includeTransient": False,
        "summary": {
            "sourceRepairIssueCount": 0,
            "transientRegressionCount": 0,
            "probedSourceCount": 0,
            "probeStatusCounts": {},
            "recommendedActionCounts": {},
            "retryableNetworkCount": 0,
            "officialProxyRequiredCount": 0,
            "tlsHandshakeFailedCount": 0,
            "dnsUnresolvedCount": 0,
        },
        "items": [],
    }


def _load_msrp_source_repair_backlog() -> dict[str, Any]:
    backlog = _read_json_if_exists(_msrp_artifacts_dir() / "msrp_source_repair_backlog.json")
    return backlog if isinstance(backlog, dict) else _default_source_repair_backlog()


def _load_msrp_source_reference_evidence(run_id: str | None = None) -> dict[str, Any]:
    evidence = _read_json_if_exists(_msrp_artifacts_dir() / "msrp_source_reference_evidence.json")
    if not isinstance(evidence, dict):
        return _default_source_reference_evidence()
    evidence_run_id = str(evidence.get("backlogRunId") or "")
    target_run_id = str(run_id or "")
    if target_run_id and evidence_run_id and evidence_run_id != target_run_id:
        return _default_source_reference_evidence()
    return evidence


def _load_msrp_source_accessibility_audit(run_id: str | None = None) -> dict[str, Any]:
    audit = _read_json_if_exists(_msrp_artifacts_dir() / "msrp_source_accessibility_audit.json")
    if not isinstance(audit, dict):
        return _default_source_accessibility_audit()
    audit_run_id = str(audit.get("backlogRunId") or "")
    target_run_id = str(run_id or "")
    if target_run_id and audit_run_id and audit_run_id != target_run_id:
        return _default_source_accessibility_audit()
    return audit


def _with_source_reference_evidence(progress: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(progress)
    run_id = str((enriched.get("status") or {}).get("runId") or "")
    if not isinstance(enriched.get("sourceReferenceEvidence"), dict):
        enriched["sourceReferenceEvidence"] = _load_msrp_source_reference_evidence(run_id)
    if not isinstance(enriched.get("sourceAccessibilityAudit"), dict):
        enriched["sourceAccessibilityAudit"] = _load_msrp_source_accessibility_audit(run_id)
    return enriched


def _source_url(source: dict[str, Any]) -> str:
    url = str(source.get("finalUrl") or source.get("sourceUrl") or "").strip()
    if url:
        return url
    for key in ("extractorError", "error"):
        match = SOURCE_URL_PATTERN.search(str(source.get(key) or ""))
        if match:
            return match.group(0).rstrip(".,")
    return ""


def _source_host(source: dict[str, Any]) -> str:
    url = _source_url(source)
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _normalize_host_groups(hosts: dict[str, dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for host, data in hosts.items():
        normalized.append({
            "host": host,
            "count": int(data.get("count") or 0),
            "affectedCountries": sorted(data.get("affectedCountries") or []),
            "affectedCountryCount": len(data.get("affectedCountries") or []),
            "sampleSources": list(data.get("sources") or [])[:10],
            "sampleUrls": list(data.get("urls") or [])[:5],
        })
    normalized.sort(key=lambda item: (-int(item["count"]), str(item["host"])))
    return normalized[:limit]


def _priority_weight(name: str, default: float) -> float:
    raw = os.getenv(f"JATO_MSRP_REPAIR_WEIGHT_{name}")
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _priority_band(
    score: float,
    source_repair_count: int,
    transient_count: int,
    external_access_count: int = 0,
    pipeline_issue_count: int = 0,
) -> str:
    if source_repair_count <= 0 and transient_count > 0:
        return "recheck"
    if source_repair_count <= 0 and pipeline_issue_count > 0:
        return "pipeline"
    if source_repair_count <= 0 and external_access_count > 0:
        return "external_access"
    if score >= 80 or source_repair_count >= 10:
        return "critical"
    if score >= 45 or source_repair_count >= 3:
        return "high"
    if score >= 15:
        return "medium"
    return "low"


def _priority_review_assist(
    failure_reason: str,
    source_repair_count: int,
    external_access_count: int = 0,
    pipeline_issue_count: int = 0,
) -> dict[str, str]:
    if source_repair_count <= 0 and external_access_count > 0:
        return {
            "preferred": "official_proxy_or_configurator_api",
            "llmFit": "low",
            "neuralNetworkFit": "not_recommended",
            "reason": "Official source access is blocked by the current fetch path; use an official proxy or configurator API before selector repair.",
        }
    if source_repair_count <= 0 and pipeline_issue_count > 0:
        return {
            "preferred": "fix_runner_or_pipeline",
            "llmFit": "low",
            "neuralNetworkFit": "not_recommended",
            "reason": "The dryrun failed in the runner or pipeline environment; fix the runtime before repairing source definitions.",
        }
    if source_repair_count <= 0:
        return {
            "preferred": "rule_based_recheck",
            "llmFit": "low",
            "neuralNetworkFit": "not_recommended",
            "reason": "Historical pass exists; rerun or inspect network conditions before source repair.",
        }
    if failure_reason in {"no_observation_extracted", "validation_rejected_all"}:
        return {
            "preferred": "rule_based_then_llm",
            "llmFit": "medium",
            "neuralNetworkFit": "not_recommended_until_labeled_corpus",
            "reason": "Rules identify the failure class; an LLM can propose selector or extraction repair from page evidence.",
        }
    return {
        "preferred": "rule_based",
        "llmFit": "low",
        "neuralNetworkFit": "not_recommended_until_labeled_corpus",
        "reason": "Use deterministic retry/proxy/source diagnostics before model-assisted repair.",
    }


def _priority_fields(group: dict[str, Any]) -> dict[str, Any]:
    source_repair_count = int(group["sourceRepairIssueCount"])
    transient_count = int(group["transientRegressionCount"])
    external_access_count = int(group.get("externalAccessIssueCount") or 0)
    pipeline_issue_count = int(group.get("pipelineIssueCount") or 0)
    affected_country_count = len(group["affectedCountries"])
    host_counts = [int(data.get("count") or 0) for data in group["hosts"].values()]
    top_host_count = max(host_counts or [0])
    score = (
        source_repair_count * _priority_weight("SOURCE_REPAIR", 10.0)
        + affected_country_count * _priority_weight("COUNTRY", 6.0)
        + top_host_count * _priority_weight("HOST_CLUSTER", 3.0)
        + transient_count * _priority_weight("TRANSIENT_RECHECK", 1.5)
        + external_access_count * _priority_weight("EXTERNAL_ACCESS", 5.0)
        + pipeline_issue_count * _priority_weight("PIPELINE_ISSUE", 2.0)
    )
    return {
        "priorityScore": round(score, 1),
        "priorityBand": _priority_band(
            score,
            source_repair_count,
            transient_count,
            external_access_count,
            pipeline_issue_count,
        ),
        "priorityWeights": {
            "sourceRepair": _priority_weight("SOURCE_REPAIR", 10.0),
            "country": _priority_weight("COUNTRY", 6.0),
            "hostCluster": _priority_weight("HOST_CLUSTER", 3.0),
            "transientRecheck": _priority_weight("TRANSIENT_RECHECK", 1.5),
            "externalAccess": _priority_weight("EXTERNAL_ACCESS", 5.0),
            "pipelineIssue": _priority_weight("PIPELINE_ISSUE", 2.0),
        },
        "reviewAssist": _priority_review_assist(
            str(group["failureReason"]),
            source_repair_count,
            external_access_count,
            pipeline_issue_count,
        ),
    }


def _is_external_access_issue(source: dict[str, Any], failure_reason: str) -> bool:
    if failure_reason not in EXTERNAL_ACCESS_FAILURES:
        return False
    brand = str(source.get("brand") or "").strip().upper()
    return brand == "TESLA" or _source_host(source) == "tesla.com"


def _is_pipeline_issue(source: dict[str, Any], failure_reason: str) -> bool:
    if failure_reason in PIPELINE_RUNTIME_FAILURES:
        return True
    return str(source.get("recommendedStrategy") or "") == "pipeline_error_not_source_error"


def _source_key(country_code: str | None, source: dict[str, Any]) -> tuple[str, str] | None:
    country = str(country_code or source.get("country") or source.get("countryCode") or "").strip().lower()
    source_code = str(source.get("sourceCode") or source.get("code") or "").strip()
    if not country or not source_code:
        return None
    return country, source_code


def _source_is_pass(source: dict[str, Any]) -> bool:
    status = str(source.get("rawStatus") or source.get("status") or "").lower()
    try:
        valid = int(source.get("valid") or 0)
    except (TypeError, ValueError):
        valid = 0
    return (
        not source.get("failureReason")
        and (status == "pass" or valid > 0)
        and status not in {"empty", "error", "exception", "fail"}
    )


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    counts: dict[str, int] = {}
    for name, count in value.items():
        label = str(name or "").strip() or "unknown"
        counts[label] = counts.get(label, 0) + _int_value(count)
    return counts


def _transient_lookup_from_stable_coverage(
    stable_coverage: dict[str, Any] | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    if not isinstance(stable_coverage, dict):
        return {}
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for sample in stable_coverage.get("probeRegressionSamples") or []:
        if not isinstance(sample, dict):
            continue
        key = _source_key(str(sample.get("countryCode") or ""), sample)
        if key:
            lookup[key] = sample
    return lookup


def _historical_good_sources_from_index(
    current_run_id: str | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    index = _load_msrp_runs_index()
    if not index:
        return {}
    current = str(current_run_id or "")
    good_sources: dict[tuple[str, str], dict[str, Any]] = {}
    for run in index.get("runs") or []:
        run_id = str(run.get("runId") or "")
        if not run_id or run_id == current:
            continue
        report_path = _artifact_path_from_ref(run.get("artifactPath"))
        report = _read_json_if_exists(report_path) if report_path else None
        if not report or report.get("schemaVersion") != "msrp_dryrun_report_v3":
            report = _load_msrp_dryrun_report(run_id)
        if not report:
            continue
        observed_at = str(run.get("finishedAt") or report.get("generatedAt") or "")
        for country in report.get("countriesDetail") or []:
            country_code = str(country.get("countryCode") or "").lower()
            for source in country.get("sources") or []:
                key = _source_key(country_code, source)
                if not key or key in good_sources or not _source_is_pass(source):
                    continue
                good_sources[key] = {
                    "runId": run_id,
                    "observedAt": observed_at,
                    "valid": source.get("valid"),
                }
    return good_sources


def _source_repair_backlog_from_current(
    current: dict[str, Any] | None,
    *,
    stable_coverage: dict[str, Any] | None = None,
    last_known_good: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not current:
        return _default_source_repair_backlog()
    groups: dict[str, dict[str, Any]] = {}
    top_hosts: dict[str, dict[str, Any]] = {}
    transient_lookup = _transient_lookup_from_stable_coverage(stable_coverage)
    historical_good = last_known_good or {}
    for country in current.get("countries") or []:
        country_code = str(country.get("countryCode") or "").lower()
        for source in country.get("sources") or []:
            reason = source.get("failureReason")
            if not reason:
                continue
            reason = str(reason)
            source_code = str(source.get("sourceCode") or source.get("code") or "")
            recommended = str(source.get("recommendedStrategy") or "diagnose_with_msrp_page_analyzer")
            key = _source_key(country_code, source)
            stable_sample = transient_lookup.get(key) if key else None
            historical_sample = historical_good.get(key) if key else None
            is_transient = (
                bool(stable_sample or historical_sample)
                or reason in TRANSIENT_RECHECK_FAILURES
            )
            group = groups.setdefault(reason, {
                "failureReason": reason,
                "count": 0,
                "transientRegressionCount": 0,
                "sourceRepairIssueCount": 0,
                "externalAccessIssueCount": 0,
                "pipelineIssueCount": 0,
                "recommendedStrategies": {},
                "affectedCountries": set(),
                "sources": [],
                "transientSources": [],
                "externalAccessSources": [],
                "pipelineSources": [],
                "hosts": {},
                "status": "new",
            })
            group["count"] += 1
            is_external_access = _is_external_access_issue(source, reason) and not is_transient
            is_pipeline_issue = (
                _is_pipeline_issue(source, reason)
                and not is_transient
                and not is_external_access
            )
            if is_transient:
                group["transientRegressionCount"] += 1
                group["transientSources"].append({
                    "countryCode": country_code,
                    "sourceCode": source_code,
                    "failureReason": reason,
                    "recommendedStrategy": recommended,
                    "lastKnownGoodRunId": (
                        (stable_sample or {}).get("stableRunId")
                        or (historical_sample or {}).get("runId")
                    ),
                    "lastKnownGoodAt": (historical_sample or {}).get("observedAt"),
                    "recommendedAction": "recheck_before_source_repair",
                })
            elif is_external_access:
                group["externalAccessIssueCount"] += 1
                group["externalAccessSources"].append({
                    "countryCode": country_code,
                    "sourceCode": source_code,
                    "brand": str(source.get("brand") or "").strip().upper(),
                    "sourceUrl": _source_url(source),
                    "host": _source_host(source),
                    "failureReason": reason,
                    "recommendedStrategy": recommended,
                    "recommendedAction": "official_proxy_or_configurator_api",
                })
            elif is_pipeline_issue:
                group["pipelineIssueCount"] += 1
                group["pipelineSources"].append({
                    "countryCode": country_code,
                    "sourceCode": source_code,
                    "brand": str(source.get("brand") or "").strip().upper(),
                    "sourceUrl": _source_url(source),
                    "host": _source_host(source),
                    "failureReason": reason,
                    "recommendedStrategy": recommended,
                    "recommendedAction": "fix_runner_or_pipeline",
                })
            else:
                group["sourceRepairIssueCount"] += 1
            group["recommendedStrategies"][recommended] = group["recommendedStrategies"].get(recommended, 0) + 1
            if country_code:
                group["affectedCountries"].add(country_code)
            if source_code:
                group["sources"].append(source_code)
            host = _source_host(source)
            url = _source_url(source)
            if host:
                for host_bucket in (
                    group["hosts"].setdefault(host, {"count": 0, "affectedCountries": set(), "sources": [], "urls": []}),
                    top_hosts.setdefault(host, {"count": 0, "affectedCountries": set(), "sources": [], "urls": []}),
                ):
                    host_bucket["count"] += 1
                    if country_code:
                        host_bucket["affectedCountries"].add(country_code)
                    if source_code:
                        host_bucket["sources"].append(source_code)
                    if url and url not in host_bucket["urls"]:
                        host_bucket["urls"].append(url)

    normalized_groups: list[dict[str, Any]] = []
    for group in groups.values():
        strategies = group["recommendedStrategies"]
        recommended_strategy = max(strategies, key=strategies.get) if strategies else "diagnose_with_msrp_page_analyzer"
        transient_count = int(group["transientRegressionCount"])
        source_repair_count = int(group["sourceRepairIssueCount"])
        external_access_count = int(group["externalAccessIssueCount"])
        pipeline_issue_count = int(group["pipelineIssueCount"])
        normalized_groups.append({
            "failureReason": group["failureReason"],
            "count": group["count"],
            "transientRegressionCount": transient_count,
            "sourceRepairIssueCount": source_repair_count,
            "externalAccessIssueCount": external_access_count,
            "pipelineIssueCount": pipeline_issue_count,
            **_priority_fields(group),
            "recommendedAction": (
                "recheck_before_source_repair"
                if transient_count and not source_repair_count
                else "fix_runner_or_pipeline"
                if pipeline_issue_count and not source_repair_count
                else "official_proxy_or_configurator_api"
                if external_access_count and not source_repair_count
                else "repair_source_definition"
            ),
            "recommendedStrategy": recommended_strategy,
            "recommendedStrategies": strategies,
            "affectedCountries": sorted(group["affectedCountries"]),
            "affectedCountryCount": len(group["affectedCountries"]),
            "sampleSources": group["sources"][:20],
            "sampleTransientRegressions": group["transientSources"][:8],
            "sampleExternalAccessIssues": group["externalAccessSources"][:8],
            "externalAccessIssues": group["externalAccessSources"],
            "samplePipelineIssues": group["pipelineSources"][:8],
            "pipelineIssues": group["pipelineSources"],
            "topSourceHosts": _normalize_host_groups(group["hosts"]),
            "status": group["status"],
        })
    normalized_groups.sort(key=lambda item: (
        0
        if int(item["sourceRepairIssueCount"]) > 0
        else 1
        if int(item.get("pipelineIssueCount") or 0) > 0
        else 2
        if int(item.get("externalAccessIssueCount") or 0) > 0
        else 3,
        -float(item["priorityScore"]),
        -int(item["count"]),
        str(item["failureReason"]),
    ))
    transient_regression_count = sum(int(item["transientRegressionCount"]) for item in normalized_groups)
    source_repair_issue_count = sum(int(item["sourceRepairIssueCount"]) for item in normalized_groups)
    external_access_issue_count = sum(int(item.get("externalAccessIssueCount") or 0) for item in normalized_groups)
    pipeline_issue_count = sum(int(item.get("pipelineIssueCount") or 0) for item in normalized_groups)
    external_access_issues = [
        source
        for item in normalized_groups
        for source in item.get("externalAccessIssues") or []
    ]
    pipeline_issues = [
        source
        for item in normalized_groups
        for source in item.get("pipelineIssues") or []
    ]
    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": current.get("runId"),
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "partial": bool(current.get("partial")),
        "totalIssueCount": sum(int(item["count"]) for item in normalized_groups),
        "transientRegressionCount": transient_regression_count,
        "sourceRepairIssueCount": source_repair_issue_count,
        "externalAccessIssueCount": external_access_issue_count,
        "externalAccessIssues": external_access_issues,
        "pipelineIssueCount": pipeline_issue_count,
        "pipelineIssues": pipeline_issues,
        "topSourceHosts": _normalize_host_groups(top_hosts),
        "groups": normalized_groups,
    }


def _source_repair_backlog_from_report(report: dict[str, Any]) -> dict[str, Any]:
    current = {
        "runId": report.get("runId"),
        "partial": False,
        "countries": [
            {
                "countryCode": str(country.get("countryCode") or "").lower(),
                "sources": country.get("sources") or [],
            }
            for country in report.get("countriesDetail") or []
        ],
    }
    backlog = _source_repair_backlog_from_current(
        current,
        last_known_good=_historical_good_sources_from_index(str(report.get("runId") or "")),
    )
    if int(backlog.get("totalIssueCount") or 0) > 0:
        return backlog
    return _load_msrp_source_repair_backlog()


def _progress_has_source_host_backlog(progress: dict[str, Any] | None) -> bool:
    if not isinstance(progress, dict):
        return False
    backlog = progress.get("sourceRepairBacklog")
    if not isinstance(backlog, dict):
        return False
    if "transientRegressionCount" not in backlog or "sourceRepairIssueCount" not in backlog:
        return False
    if backlog.get("topSourceHosts"):
        return True
    return any(
        isinstance(group, dict) and bool(group.get("topSourceHosts"))
        for group in backlog.get("groups") or []
    )


def _load_msrp_dryrun_report(run_id: str | None = None) -> dict[str, Any] | None:
    artifact_dir = _msrp_artifacts_dir()
    path = artifact_dir / f"dryrun_report_{run_id}.json" if run_id else artifact_dir / "dryrun_report.json"
    report = _read_json_if_exists(path)
    if report and report.get("schemaVersion") == "msrp_dryrun_report_v3":
        return report
    return None


def _artifact_path_from_ref(path_ref: str | None) -> Path | None:
    if not path_ref:
        return None
    path = Path(path_ref)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _load_msrp_runs_index() -> dict[str, Any] | None:
    index = _read_json_if_exists(_msrp_artifacts_dir() / "dryrun_runs_index.json")
    return index if isinstance(index, dict) else None


def _load_latest_indexed_msrp_dryrun_report() -> dict[str, Any] | None:
    index = _load_msrp_runs_index()
    latest_run_id = str((index or {}).get("latestRunId") or "")
    if not latest_run_id:
        return None

    fallback_paths: list[Path] = [_msrp_artifacts_dir() / f"dryrun_report_{latest_run_id}.json"]
    for run in (index or {}).get("runs") or []:
        if run.get("runId") != latest_run_id:
            continue
        artifact_path = _artifact_path_from_ref(run.get("artifactPath"))
        if artifact_path:
            fallback_paths.insert(0, artifact_path)
        break

    seen: set[Path] = set()
    for path in fallback_paths:
        if path in seen:
            continue
        seen.add(path)
        report = _read_json_if_exists(path)
        if report and report.get("schemaVersion") == "msrp_dryrun_report_v3":
            return report
    return None


def _is_empty_msrp_progress(payload: dict[str, Any] | None) -> bool:
    if not payload:
        return True
    status = payload.get("status") or {}
    findings = payload.get("findings") or []
    return (
        not payload.get("countries")
        and not status.get("runId")
        and any(f.get("type") == "no_dryrun_report" for f in findings if isinstance(f, dict))
    )


def _msrp_progress_country_entry(country: dict[str, Any]) -> dict[str, Any]:
    code = str(country.get("countryCode") or country.get("country") or "").lower()
    pass_pct_raw = (
        country.get("passPct")
        if country.get("passPct") is not None
        else country.get("passRate")
    )
    entry = {
        "countryCode": code,
        "countryLabel": country.get("countryLabel") or _msrp_country_label(code),
        "total": int(country.get("total") or 0),
        "pass": int(country.get("pass") or 0),
        "empty": int(country.get("empty") or 0),
        "fail": int(country.get("fail") or 0),
        "errors": int(country.get("errors") or 0),
        "passPct": float(pass_pct_raw or 0.0),
        "status": country.get("status") or ("success" if country.get("completed") else "unknown"),
        "topFailureReason": country.get("topFailureReason"),
        "failureBreakdown": country.get("failureBreakdown") or {},
        "strategyRecommendations": country.get("strategyRecommendations") or {},
        "financeObservationCandidates": _int_value(country.get("financeObservationCandidates")),
        "financeMonthlyPaymentCount": _int_value(country.get("financeMonthlyPaymentCount")),
        "financeSemanticsCounts": _count_map(country.get("financeSemanticsCounts")),
        "financeTypeCounts": _count_map(country.get("financeTypeCounts")),
    }
    for key in ("runId", "batch", "timestamp", "gateStatus", "runStatus", "isLatestRun", "completed"):
        if key in country:
            entry[key] = country.get(key)
    return entry


def _msrp_dashboard_context() -> dict[str, Any]:
    try:
        from app.services.msrp_dryrun_progress import get_dryrun_dashboard
        dashboard = get_dryrun_dashboard()
    except Exception:
        return {}
    if not isinstance(dashboard, dict):
        return {}

    all_countries = [
        _msrp_progress_country_entry(country)
        for country in (dashboard.get("allCountries") or [])
        if isinstance(country, dict)
    ]
    stable_coverage = dashboard.get("stableCoverage") if isinstance(dashboard.get("stableCoverage"), dict) else {}
    stable_latest_run_id = stable_coverage.get("latestRunId") or next(
        (
            country.get("runId")
            for country in all_countries
            if country.get("runId")
        ),
        None,
    )
    return {
        "dashboard": dashboard,
        "allCountriesLatest": all_countries,
        "stableCoverage": stable_coverage,
        "stableLatestRunId": stable_latest_run_id,
    }


def _with_msrp_latest_context(
    progress: dict[str, Any],
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    if not context:
        return progress
    all_countries = context.get("allCountriesLatest")
    stable_coverage = context.get("stableCoverage")
    if not all_countries and not stable_coverage:
        return progress

    enriched = dict(progress)
    if all_countries and not enriched.get("allCountriesLatest"):
        enriched["allCountriesLatest"] = all_countries
    if stable_coverage and not enriched.get("stableCoverage"):
        enriched["stableCoverage"] = stable_coverage

    status = dict(enriched.get("status") or {})
    stable_latest_run_id = context.get("stableLatestRunId")
    if stable_latest_run_id and not status.get("stableLatestRunId"):
        status["stableLatestRunId"] = stable_latest_run_id
    active_run_id = (stable_coverage or {}).get("activeRunId") if isinstance(stable_coverage, dict) else None
    if active_run_id and not status.get("activeRunId"):
        status["activeRunId"] = active_run_id
    enriched["status"] = status
    return enriched


def _msrp_progress_from_report(report: dict[str, Any]) -> dict[str, Any]:
    summary = report.get("summary") or {}
    gate_threshold = int(summary.get("gateThreshold") or 70)
    gate_status = str(summary.get("gateStatus") or "blocked")
    pass_pct = float(summary.get("passPct") or 0.0)
    countries_detail = report.get("countriesDetail") or []

    countries: list[dict[str, Any]] = []
    top_blocking: list[dict[str, Any]] = []
    failure_reasons: dict[str, int] = {}

    for country in countries_detail:
        code = str(country.get("countryCode") or "?").lower()
        country_pct = float(country.get("passPct") or 0.0)
        failure_breakdown = country.get("failureBreakdown") or {}
        strategy_recs = country.get("strategyRecommendations") or {}
        top_reason = max(failure_breakdown, key=failure_breakdown.get) if failure_breakdown else None

        entry = _msrp_progress_country_entry({
            **country,
            "countryCode": code,
            "passPct": country_pct,
            "topFailureReason": top_reason,
            "failureBreakdown": failure_breakdown,
            "strategyRecommendations": strategy_recs,
        })
        countries.append(entry)

        if country_pct < gate_threshold or country_pct < 50:
            top_blocking.append({
                "countryCode": code,
                "passPct": country_pct,
                "reason": top_reason or "unknown",
                "recommendedAction": f"Review {top_reason or 'failures'} for {code}",
            })

        for reason, count in failure_breakdown.items():
            failure_reasons[str(reason)] = failure_reasons.get(str(reason), 0) + int(count or 0)

    findings: list[dict[str, Any]] = []
    for country in report.get("missingCountries") or []:
        findings.append({
            "type": "missing_country",
            "severity": "critical",
            "message": f"Country '{country}' is missing from the dryrun run.",
            "country": country,
        })
    for country in report.get("duplicateCountries") or []:
        findings.append({
            "type": "duplicate_country",
            "severity": "warning",
            "message": f"Country '{country}' appears multiple times in the same run.",
            "country": country,
        })
    for country in countries:
        country_pct = float(country.get("passPct") or 0.0)
        if country_pct < 50:
            findings.append({
                "type": "country_low_pass_rate",
                "severity": "critical",
                "message": f"Country '{country['countryCode']}' pass rate is {country_pct}% (<50%).",
                "country": country["countryCode"],
                "passPct": country_pct,
            })
        elif country_pct < gate_threshold:
            findings.append({
                "type": "country_below_gate",
                "severity": "warning",
                "message": (
                    f"Country '{country['countryCode']}' pass rate is "
                    f"{country_pct}% (<{gate_threshold}% gate)."
                ),
                "country": country["countryCode"],
                "passPct": country_pct,
            })
    if gate_status == "blocked":
        findings.append({
            "type": "ingest_gate_blocked",
            "severity": "critical",
            "message": f"Ingest gate blocked: overall pass rate {pass_pct}% < {gate_threshold}% threshold.",
            "passPct": pass_pct,
            "gateThreshold": gate_threshold,
        })

    overall = "critical" if any(f.get("severity") == "critical" for f in findings) else (
        "warning" if findings else "ok"
    )

    return _with_source_reference_evidence({
        "probe": "pipeline.msrp_country_progress",
        "overall": overall,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": {
            "runId": report.get("runId"),
            "schemaVersion": report.get("schemaVersion"),
            "overallPassPct": pass_pct,
            "gateThreshold": gate_threshold,
            "gateStatus": gate_status,
            "expectedCountries": report.get("expectedCountries", []),
            "observedCountries": report.get("observedCountries", []),
            "missingCountries": report.get("missingCountries", []),
            "duplicateCountries": report.get("duplicateCountries", []),
            "financeObservationCandidates": _int_value(summary.get("financeObservationCandidates")),
            "financeMonthlyPaymentCount": _int_value(summary.get("financeMonthlyPaymentCount")),
            "financeSemanticsCounts": _count_map(summary.get("financeSemanticsCounts")),
            "financeTypeCounts": _count_map(summary.get("financeTypeCounts")),
        },
        "countries": countries,
        "topBlockingCountries": sorted(top_blocking, key=lambda item: item["passPct"]),
        "topFailureReasons": [
            {"reason": reason, "count": count}
            for reason, count in sorted(failure_reasons.items(), key=lambda item: -item[1])[:5]
        ],
        "sourceRepairBacklog": _source_repair_backlog_from_report(report),
        "findings": findings,
    })


def _missing_msrp_progress() -> dict[str, Any]:
    return _with_source_reference_evidence({
        "probe": "pipeline.msrp_country_progress",
        "overall": "critical",
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": {},
        "countries": [],
        "topBlockingCountries": [],
        "topFailureReasons": [],
        "sourceRepairBacklog": _default_source_repair_backlog(),
        "findings": [{
            "type": "no_dryrun_report",
            "severity": "critical",
            "message": "No dryrun report found. MSRP dryrun may not have run yet.",
        }],
    })


def _msrp_progress_from_partial_current(
    current: dict[str, Any] | None,
    stable_coverage: dict[str, Any] | None = None,
    all_countries_latest: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if not current or not current.get("available") or not current.get("partial"):
        return None

    is_running = bool(current.get("running"))
    partial_finding = {
        "type": "dryrun_running_without_aggregate" if is_running else "dryrun_partial_without_aggregate",
        "severity": "warning",
        "message": (
            "MSRP dryrun is running and country-level partial artifacts are available; aggregate report is pending."
            if is_running
            else "MSRP dryrun partial artifacts are available, but no active run is detected; aggregate report is pending."
        ),
        "runId": current.get("runId"),
    }
    countries: list[dict[str, Any]] = []
    top_blocking: list[dict[str, Any]] = []
    failure_reasons: dict[str, int] = {}
    findings: list[dict[str, Any]] = [partial_finding]
    for country in current.get("countries") or []:
        code = str(country.get("countryCode") or "").lower()
        entry = _msrp_progress_country_entry({
            **country,
            "status": country.get("status") or ("success" if country.get("completed") else "running"),
        })
        countries.append(entry)
        country_pct = float(entry.get("passPct") or 0.0)
        if country.get("completed") and country_pct < 50:
            findings.append({
                "type": "country_low_pass_rate",
                "severity": "critical",
                "message": f"Country '{code}' pass rate is {country_pct}% (<50%).",
                "country": code,
                "passPct": country_pct,
            })
        if country.get("completed") and country_pct < 70:
            top_blocking.append({
                "countryCode": code,
                "passPct": country_pct,
                "reason": entry.get("topFailureReason") or "unknown",
                "recommendedAction": f"Review {entry.get('topFailureReason') or 'failures'} for {code}",
            })
        for reason, count in entry["failureBreakdown"].items():
            failure_reasons[str(reason)] = failure_reasons.get(str(reason), 0) + int(count or 0)
        if not country.get("completed"):
            findings.append({
                "type": "country_running",
                "severity": "info",
                "message": f"Country '{code}' has not finished in the active dryrun.",
                "country": code,
            })

    overall = "critical" if any(finding.get("severity") == "critical" for finding in findings) else (
        "warning" if findings else "ok"
    )

    latest_countries = [
        _msrp_progress_country_entry(country)
        for country in (all_countries_latest or [])
        if isinstance(country, dict)
    ]
    coverage = stable_coverage if isinstance(stable_coverage, dict) else {}
    stable_latest_run_id = coverage.get("latestRunId") or next(
        (
            country.get("runId")
            for country in latest_countries
            if country.get("runId")
        ),
        None,
    )

    return _with_source_reference_evidence({
        "probe": "pipeline.msrp_country_progress",
        "overall": overall,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": {
            "runId": current.get("runId"),
            "schemaVersion": current.get("schemaVersion"),
            "running": is_running,
            "partial": True,
            "overallPassPct": float(current.get("overallPassRate") or 0.0),
            "gateThreshold": current.get("gateThreshold"),
            "gateStatus": current.get("gateStatus") or "pending",
            "expectedCountries": current.get("expectedCountries") or [],
            "observedCountries": current.get("observedCountries") or [],
            "missingCountries": current.get("missingCountries") or [],
            "duplicateCountries": [],
            "stableLatestRunId": stable_latest_run_id,
            "activeRunId": coverage.get("activeRunId") or current.get("runId"),
            "financeObservationCandidates": _int_value(current.get("financeObservationCandidates")),
            "financeMonthlyPaymentCount": _int_value(current.get("financeMonthlyPaymentCount")),
            "financeSemanticsCounts": _count_map(current.get("financeSemanticsCounts")),
            "financeTypeCounts": _count_map(current.get("financeTypeCounts")),
        },
        "countries": countries,
        "allCountriesLatest": latest_countries or countries,
        "stableCoverage": coverage,
        "topBlockingCountries": sorted(top_blocking, key=lambda item: item["passPct"]),
        "topFailureReasons": [
            {"reason": reason, "count": count}
            for reason, count in sorted(failure_reasons.items(), key=lambda item: (-item[1], item[0]))[:5]
        ],
        "sourceRepairBacklog": _source_repair_backlog_from_current(
            current,
            stable_coverage=stable_coverage,
        ),
        "findings": findings,
    })


def _partial_msrp_progress(
    context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    context = context or _msrp_dashboard_context()
    dashboard = context.get("dashboard") if isinstance(context.get("dashboard"), dict) else {}
    current = dashboard.get("current") if isinstance(dashboard, dict) else None
    return _msrp_progress_from_partial_current(
        current if isinstance(current, dict) else None,
        context.get("stableCoverage") if isinstance(context.get("stableCoverage"), dict) else None,
        context.get("allCountriesLatest") if isinstance(context.get("allCountriesLatest"), list) else None,
    )


@router.get("/overview")
def hermes_overview(_=Depends(require_min_role("viewer"))) -> dict:
    """Return a consolidated Hermes governance overview."""
    overview: dict[str, Any] = {
        "registries": {},
        "reports": {},
        "proposals": {"total": 0, "implemented": 0, "pending": 0, "draft": 0},
        "gaps": {"total": 0, "open": 0, "resolved": 0},
    }
    try:
        import yaml as _yaml
    except ImportError:
        return {"error": "PyYAML not installed in backend venv. Run: pip install pyyaml"}

    # Registry counts
    for name in ["source", "pipeline", "feature", "prompt", "artifact"]:
        fname = f"{name}_registry.yaml"
        path = HERMES_DIR / fname
        if not path.is_file():
            overview["registries"][name] = -2  # file not found
            continue
        try:
            data = _yaml.safe_load(path.read_text())
            key = f"{name}s" if not name.endswith("s") else name
            items = data.get(key, []) if data else []
            overview["registries"][name] = len(items) if isinstance(items, list) else 0
        except Exception as exc:
            overview["registries"][name] = -1  # parse error

    # Report availability
    report_files = {
        "pipelineHealth": "pipeline_health.json",
        "sourceQuality": "source_quality_report.json",
        "costReport": "cost_report.json",
        "codeAudit": "hermes_code_audit_report.json",
    }
    for key, fname in report_files.items():
        overview["reports"][key] = (REPORTS_DIR / fname).is_file()

    # Proposals
    prop_path = HERMES_DIR / "proposal_registry.yaml"
    if prop_path.is_file():
        try:
            data = _yaml.safe_load(prop_path.read_text())
            proposals = data.get("proposals", []) if data else []
            overview["proposals"]["total"] = len(proposals)
            overview["proposals"]["implemented"] = sum(1 for p in proposals if p.get("status") == "implemented")
            overview["proposals"]["pending"] = sum(1 for p in proposals if p.get("status") == "pending_review")
            overview["proposals"]["draft"] = sum(1 for p in proposals if p.get("status") == "draft")
        except Exception:
            pass

    # Gaps
    gaps_path = HERMES_DIR / "governance_gaps.yaml"
    if gaps_path.is_file():
        try:
            data = _yaml.safe_load(gaps_path.read_text())
            gaps = data.get("gaps", []) if data else []
            overview["gaps"]["total"] = len(gaps)
            overview["gaps"]["open"] = sum(1 for g in gaps if g.get("status") == "open")
            overview["gaps"]["resolved"] = sum(1 for g in gaps if g.get("status") == "resolved")
        except Exception:
            pass

    return overview


@router.get("/pipeline-health")
def hermes_pipeline_health(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the latest pipeline health report."""
    return _read_json(REPORTS_DIR / "pipeline_health.json")


@router.get("/pipeline/status")
def hermes_pipeline_statuses(_=Depends(require_min_role("viewer"))) -> list[dict]:
    """Return standard Hermes pipeline runtime status records."""
    from app.services.hermes_pipeline_status_service import list_pipeline_statuses
    return list_pipeline_statuses(include_missing=True)


@router.get("/pipeline/status/{pipeline_id}")
def hermes_pipeline_status(
    pipeline_id: str,
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return one standard Hermes pipeline runtime status record."""
    from app.services.hermes_pipeline_status_service import get_pipeline_status
    return get_pipeline_status(pipeline_id)


@router.get("/source-quality")
def hermes_source_quality(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the latest source quality report."""
    return _read_json(REPORTS_DIR / "source_quality_report.json")


@router.get("/cost")
def hermes_cost_report(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the latest cost report."""
    return _read_json(REPORTS_DIR / "cost_report.json")


@router.get("/msrp-country-progress")
def hermes_msrp_country_progress(
    run_id: str | None = Query(None),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return MSRP country progress for latest or specific run_id."""
    if run_id:
        report_path = REPORTS_DIR / f"msrp_country_progress_{run_id}.json"
        static_progress = _read_json_if_exists(report_path)
        if static_progress and not _is_empty_msrp_progress(static_progress):
            return _with_source_reference_evidence(static_progress)
        dryrun_report = _load_msrp_dryrun_report(run_id)
        if dryrun_report:
            return _msrp_progress_from_report(dryrun_report)
        return _read_json(report_path)

    static_progress = _read_json_if_exists(REPORTS_DIR / "msrp_country_progress.json")
    latest_report = _load_msrp_dryrun_report() or _load_latest_indexed_msrp_dryrun_report()
    static_run_id = (static_progress or {}).get("status", {}).get("runId")
    latest_run_id = (latest_report or {}).get("runId")
    dashboard_context = _msrp_dashboard_context()
    partial_progress = _partial_msrp_progress(dashboard_context)
    partial_status = (partial_progress or {}).get("status") or {}
    partial_run_id = partial_status.get("runId")
    if (
        partial_progress
        and partial_status.get("running")
        and partial_run_id
        and partial_run_id not in {known_run_id for known_run_id in (static_run_id, latest_run_id) if known_run_id}
    ):
        return partial_progress
    if latest_report and (
        _is_empty_msrp_progress(static_progress)
        or static_run_id != latest_run_id
        or not _progress_has_source_host_backlog(static_progress)
    ):
        return _with_msrp_latest_context(
            _msrp_progress_from_report(latest_report),
            dashboard_context,
    )
    if static_progress and not _is_empty_msrp_progress(static_progress):
        return _with_source_reference_evidence(
            _with_msrp_latest_context(static_progress, dashboard_context)
        )
    if latest_report:
        return _with_msrp_latest_context(
            _msrp_progress_from_report(latest_report),
            dashboard_context,
        )
    if partial_progress:
        return partial_progress
    if static_progress:
        return _with_source_reference_evidence(
            _with_msrp_latest_context(static_progress, dashboard_context)
        )
    return _with_msrp_latest_context(_missing_msrp_progress(), dashboard_context)


@router.get("/msrp-dryrun-history")
def hermes_msrp_dryrun_history(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the MSRP dryrun runs index (history of all runs)."""
    path = PROJECT_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "dryrun_runs_index.json"
    if not path.is_file():
        return {
            "schemaVersion": "msrp_dryrun_runs_index_v1",
            "updatedAt": None,
            "latestRunId": None,
            "runs": [],
        }
    return _sort_msrp_runs_index(_read_json(path))


@router.get("/code-audit")
def hermes_code_audit(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the latest code audit report."""
    return _read_json(REPORTS_DIR / "hermes_code_audit_report.json")


@router.get("/proposals")
def hermes_proposals(_=Depends(require_min_role("viewer")),
    status: str | None = Query(None, description="Filter by status: draft, pending_review, implemented"),
) -> list[dict]:
    """Return proposals from the registry."""
    path = HERMES_DIR / "proposal_registry.yaml"
    if not path.is_file():
        return []
    import yaml
    data = yaml.safe_load(path.read_text())
    proposals = data.get("proposals", []) if data else []
    if status:
        proposals = [p for p in proposals if p.get("status") == status]
    return proposals


@router.get("/gaps")
def hermes_gaps(_=Depends(require_min_role("viewer")),
    status: str | None = Query(
        None,
        description="Filter by status: open, resolved, in_progress. "
                    "Omit for all statuses.",
    ),
    category: str | None = Query(
        None,
        description="Filter by category: prompt, pipeline, env, test, "
                    "source_quality, backlog, docs, feature. "
                    "Omit for all categories.",
    ),
) -> list[dict]:
    """List governance gaps from the registry.

    Returns all gaps when no filters are provided.  Filters are independent
    (AND logic) — combining ``?status=open&category=test`` returns only
    open gaps in the test category.

    **Empty states**:
        - YAML file missing → ``[]`` (200)
        - No gaps match filters → ``[]`` (200)
        - Unknown status/category → ``[]`` (200; safe ignore)
    """
    path = HERMES_DIR / "governance_gaps.yaml"
    if not path.is_file():
        return []
    import yaml
    try:
        data = yaml.safe_load(path.read_text())
    except Exception:
        return []
    gaps: list[dict] = data.get("gaps", []) if data else []
    if status:
        gaps = [g for g in gaps if g.get("status") == status]
    if category:
        gaps = [g for g in gaps if g.get("category") == category]
    return gaps


@router.get("/features")
def hermes_features(_=Depends(require_min_role("viewer"))) -> list[dict]:
    """Return features from the registry."""
    path = HERMES_DIR / "feature_registry.yaml"
    if not path.is_file():
        return []
    import yaml
    data = yaml.safe_load(path.read_text())
    features = data.get("features", []) if data else []
    from app.services.hermes_devsync_service import apply_curated_kanban_feature_evidence
    return apply_curated_kanban_feature_evidence(features)


@router.get("/toolchain")
def hermes_toolchain(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the Hermes tool chain inventory — what scripts exist and how they connect."""
    scripts_dir = PROJECT_ROOT / "03_Scripts" / "hermes"
    scripts: list[dict] = []
    for f in sorted(scripts_dir.glob("*.py")):
        if f.name.startswith("_"):
            continue
        scripts.append({
            "name": f.name,
            "path": str(f.relative_to(PROJECT_ROOT)),
            "sizeBytes": f.stat().st_size,
        })

    registries = [
        {"name": f.name, "path": str(f.relative_to(PROJECT_ROOT))}
        for f in sorted(HERMES_DIR.glob("*.yaml"))
    ]

    reports = [
        {"name": f.name, "path": str(f.relative_to(PROJECT_ROOT))}
        for f in sorted(REPORTS_DIR.glob("*.json"))
    ] if REPORTS_DIR.is_dir() else []

    # Development workflow steps
    workflow = [
        {"step": 1, "phase": "Phase 0", "script": "asset_map", "action": "REPOSITORY_ASSET_MAP.md", "description": "Full repository inventory scan"},
        {"step": 2, "phase": "Phase 1", "script": "registries", "action": "hermes/*.yaml (8 files)", "description": "Registry foundation — 71 seed entries"},
        {"step": 3, "phase": "Phase 2", "script": "hermes_intake.py", "action": "PRD → impact report", "description": "Pre-development impact analysis"},
        {"step": 4, "phase": "—", "script": "Claude Code", "action": "implementation", "description": "Develop feature per PRD + intake report"},
        {"step": 5, "phase": "Phase 3", "script": "hermes_code_audit.py", "action": "git diff → audit report", "description": "Post-development diff scan (10 rules)"},
        {"step": 6, "phase": "Phase 4", "script": "hermes_pipeline_audit.py", "action": "pipeline health report", "description": "Cross-reference systemd/Airflow/GH Actions/artifacts"},
        {"step": 7, "phase": "Phase 5", "script": "hermes_source_quality.py", "action": "source quality scores", "description": "Score VOC/News/MSRP source health"},
        {"step": 8, "phase": "Phase 5", "script": "hermes_evidence_writer.py", "action": "evidence_ledger.jsonl", "description": "Extract fact/quote/event evidence"},
        {"step": 9, "phase": "Phase 5", "script": "hermes_answer_audit.py", "action": "answer_audit.jsonl", "description": "Audit Country Assistant answers"},
        {"step": 10, "phase": "Phase 5.5", "script": "hermes_cost_report.py", "action": "cost_report.json", "description": "Track Flash/Pro token costs vs budget"},
        {"step": 11, "phase": "Phase 6", "script": "hermes API + UI", "action": "/data-management → Hermes tab", "description": "Governance dashboard"},
    ]

    return {
        "scripts": scripts,
        "registries": registries,
        "reports": reports,
        "workflow": workflow,
        "scriptCount": len(scripts),
        "registryCount": len(registries),
        "reportCount": len(reports),
    }


@router.get("/architecture")
def hermes_architecture(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the Hermes governance architecture — modules, dependencies, and work routing."""
    modules = [
        {
            "governor": "Code Governor",
            "icon": "code",
            "phase": "Phase 2-3",
            "scripts": ["hermes_intake.py", "hermes_code_audit.py"],
            "inputs": ["PRD.md", "git diff (base..HEAD)", "hermes/*.yaml registries"],
            "outputs": ["intake report (.md + .json)", "code audit report (.md + .json)"],
            "answers": [
                "这个 PRD 会影响哪些功能/管道/源/prompt？",
                "Claude Code 改完有没有漏 registry/docs/tests？",
                "有没有 secret 泄露或 schema 变更无 migration？",
            ],
            "triggers": "每次写新 PRD 或 push 代码后手动运行",
        },
        {
            "governor": "Pipeline Governor",
            "icon": "pipeline",
            "phase": "Phase 4-4.5",
            "scripts": ["hermes_pipeline_audit.py"],
            "inputs": ["hermes/pipeline_registry.yaml", ".github/workflows/", "airflow/dags/", "03_Scripts/deploy/systemd/", "scheduled_fetch_status.json"],
            "outputs": ["pipeline health report (.md)", "pipeline_health.json"],
            "answers": [
                "哪些 pipeline 在生产？哪些是手动后备？",
                "Country News 有没有重复调度？",
                "哪个 artifact 被哪个 feature 消费？",
                "VOC 源错误有没有被结构化追踪？",
            ],
            "triggers": "定期运行（建议每周）或 pipeline 变更后",
        },
        {
            "governor": "Intelligence Governor",
            "icon": "intelligence",
            "phase": "Phase 5-5.5",
            "scripts": ["hermes_source_quality.py", "hermes_evidence_writer.py", "hermes_answer_audit.py", "hermes_cost_report.py"],
            "inputs": [
                "hermes/source_registry.yaml",
                "hermes/answer_audit.jsonl",
                "hermes/agent_usage.jsonl",
                "hermes/eval/eval_usage.jsonl",
                "hermes/model_pricing.yaml",
                "VOC/News/MSRP artifacts",
            ],
            "outputs": ["source quality report (.json)", "evidence_ledger.jsonl", "answer audit (.jsonl)", "cost report (.json)"],
            "answers": [
                "VOC/News/MSRP 源质量如何？哪个该降级？",
                "国家助手回答有没有证据？幻觉风险多高？",
                "Flash vs Pro 各花了多少钱？有没有超预算？",
                "哪些 evidence 可以跨回答复用？",
            ],
            "triggers": "每次 VOC/News 管道运行后，或国家助手回答后",
        },
        {
            "governor": "Knowledge Governor",
            "icon": "knowledge",
            "phase": "Phase 1 + ongoing",
            "scripts": ["hermes_registry_loader.py", "hermes_text_matcher.py", "hermes/*.yaml"],
            "inputs": ["REPOSITORY_ASSET_MAP.md", "CLAUDE.md", "Markdown_Readme/", "hermes/*.yaml"],
            "outputs": ["8 registry YAML files (71+ entries)", "feature/pipeline/source/prompt/artifact registries"],
            "answers": [
                "系统里到底有什么功能/管道/源/prompt？",
                "哪个功能没有 owner？哪个管道没有注册？",
                "新 PRD 和现有功能有没有重叠？",
                "GitNexus / Roadmap / CLAUDE.md 是否一致？",
            ],
            "triggers": "每次新增功能/管道/源/prompt 后更新 registry",
        },
    ]

    # Dependency graph: who reads/writes what
    deps = [
        {"from": "hermes_registry_loader.py", "to": "ALL scripts", "what": "reads hermes/*.yaml → returns typed dicts"},
        {"from": "hermes_text_matcher.py", "to": "hermes_intake.py", "what": "keyword extraction + scoring engine"},
        {"from": "hermes_intake.py", "to": "developer", "what": "PRD → impact report → Claude Code brief"},
        {"from": "Claude Code", "to": "hermes_code_audit.py", "what": "git diff → 10-rule audit scan"},
        {"from": "hermes_pipeline_audit.py", "to": "developer", "what": "scans systemd/Airflow/GH Actions → health report"},
        {"from": "hermes_source_quality.py", "to": "developer", "what": "scores VOC/News/MSRP source health 0-100"},
        {"from": "hermes_evidence_writer.py", "to": "hermes_answer_audit.py", "what": "JSONL evidence → answer groundedness scoring"},
        {"from": "hermes_cost_report.py", "to": "developer", "what": "audit records + pricing → budget tracking"},
        {"from": "hermes API (/v1/hermes/*)", "to": "DataManagementPage", "what": "JSON → UI dashboard"},
        {"from": "systemd timers", "to": "hermes_pipeline_audit.py", "what": "pipeline runtime data → health report"},
        {"from": "VOC fetcher", "to": "hermes_source_quality.py", "what": "runtime errors → source quality scores"},
        {"from": "Country Copilot", "to": "hermes_answer_audit.py", "what": "answers → answer audit records"},
    ]

    # Work routing guide
    routing = [
        {"task": "我要写新功能 PRD", "ask": "Hermes Code Governor (Phase 2)", "run": "python 03_Scripts/hermes/hermes_intake.py prd.md", "gets": "影响分析报告 + Claude Code 开发 brief"},
        {"task": "Claude Code 刚改完代码", "ask": "Hermes Code Governor (Phase 3)", "run": "python 03_Scripts/hermes/hermes_code_audit.py --base main --head HEAD", "gets": "diff 风险报告（secret/registry/schema/schedule）"},
        {"task": "我想看所有 pipeline 健康状态", "ask": "Hermes Pipeline Governor (Phase 4)", "run": "python 03_Scripts/hermes/hermes_pipeline_audit.py", "gets": "管道健康报告 + pipeline_health.json"},
        {"task": "VOC 源抓取质量如何", "ask": "Hermes Intelligence Governor (Phase 5)", "run": "python 03_Scripts/hermes/hermes_source_quality.py", "gets": "源质量评分表（healthy/watch/degraded）"},
        {"task": "国家助手回答有没有幻觉", "ask": "Hermes Intelligence Governor (Phase 5)", "run": "python 03_Scripts/hermes/hermes_answer_audit.py --sample", "gets": "回答审计报告（groundedness/hallucination/cost）"},
        {"task": "Flash/Pro 花了多少钱", "ask": "Hermes Intelligence Governor (Phase 5.5)", "run": "python 03_Scripts/hermes/hermes_cost_report.py", "gets": "成本报告（按模型/模式分拆 + 预算追踪）"},
        {"task": "系统里到底注册了哪些功能/源/管道", "ask": "Hermes Knowledge Governor (Phase 1)", "run": "cat hermes/{feature,pipeline,source}_registry.yaml", "gets": "YAML 总账本"},
        {"task": "部署一直失败是为什么", "ask": "Hermes Code Governor (Phase 3)", "run": "python 03_Scripts/hermes/hermes_code_audit.py + 看 GitHub Actions logs", "gets": "部署失败诊断报告"},
        {"task": "在 Data Management UI 看 Hermes", "ask": "Hermes Phase 6", "run": "访问 /data-management → Hermes Governance 标签", "gets": "可视化治理看板"},
    ]

    return {
        "modules": modules,
        "dependencies": deps,
        "routing": routing,
    }


# ── Script Execution ──────────────────────────────────────────────


def _check_command_role(command_id: str, user_role: str) -> None:
    """Raise 403 if *user_role* is insufficient for a known *command_id*.

    Unknown commands are not gated here — ``execute_hermes_command()``
    validates the command and returns 400 for them.
    """
    info = HERMES_SCRIPTS.get(command_id)
    if info is None:
        return  # let execute_hermes_command return 400
    min_role = info.get("requiredRole", "admin")
    if ROLE_LEVEL.get(user_role, 0) < ROLE_LEVEL.get(min_role, 0):
        raise HTTPException(403, f"Command '{command_id}' requires {min_role} role")


@router.get("/run/{command}/help")
def hermes_run_help(command: str, _=Depends(require_min_role("viewer"))):
    """Return help for a specific Hermes command."""
    if command == "all":
        return PlainTextResponse(HELP_TEXT)
    try:
        return get_command_help(command)
    except HermesRunError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc


@router.post("/run/{command}")
def hermes_run(command: str, user=Depends(require_min_role("viewer"))):
    """Execute a Hermes script and return its output."""
    _check_command_role(command, user.role)
    try:
        return execute_hermes_command(
            command,
            actor=getattr(user, "name", "unknown"),
        )
    except HermesRunError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc


@router.get("/run")
def hermes_list_commands():
    """List all available Hermes run commands."""
    return list_run_commands()


# ── Source Drill-down ─────────────────────────────────────────────

@router.get("/source/{source_id}")
def hermes_source_detail(source_id: str, _=Depends(require_min_role("viewer"))) -> dict:
    """Return full detail for a single source including linked evidence."""
    path = HERMES_DIR / "source_registry.yaml"
    if not path.is_file():
        raise HTTPException(404, "Source registry not found")
    import yaml
    data = yaml.safe_load(path.read_text())
    sources = data.get("sources", []) if data else []
    source = next((s for s in sources if s.get("sourceId") == source_id), None)
    if not source:
        raise HTTPException(404, f"Source not found: {source_id}")

    # Linked evidence from ledger
    evidence: list[dict] = []
    ev_path = HERMES_DIR / "evidence_ledger.jsonl"
    if ev_path.is_file():
        for line in ev_path.read_text().strip().split("\n"):
            if line.strip():
                try:
                    rec = json.loads(line)
                    if source_id in str(rec.get("sourceRef", "")) or source_id in str(rec.get("artifactId", "")):
                        evidence.append(rec)
                except Exception:
                    pass

    # Find producing/consuming pipelines
    pipe_path = HERMES_DIR / "pipeline_registry.yaml"
    pipelines: list[dict] = []
    if pipe_path.is_file():
        pipe_data = yaml.safe_load(pipe_path.read_text())
        for p in (pipe_data.get("pipelines", []) if pipe_data else []):
            for out in (p.get("outputs", []) or []):
                if source_id in str(out) or source.get("name","") in str(out):
                    pipelines.append({"pipelineId": p.get("pipelineId"), "name": p.get("name"), "type": p.get("type")})
                    break

    return {
        "source": source,
        "linkedEvidence": evidence,
        "linkedEvidenceCount": len(evidence),
        "linkedPipelines": pipelines,
    }


@router.get("/source/{source_id}/health-history")
def hermes_source_health_history(source_id: str, _=Depends(require_min_role("viewer"))) -> dict:
    """Return health history for a source (from status JSON and quality report)."""
    sq = _read_json(REPORTS_DIR / "source_quality_report.json")
    source_score = None
    if sq:
        for s in sq.get("sources", []):
            if s.get("sourceId") == source_id:
                source_score = s
                break

    status = _read_json(PROJECT_ROOT / "03_Scripts" / "logs" / "scheduled_fetch_status.json")

    return {
        "sourceId": source_id,
        "qualityScore": source_score,
        "fetchStatus": status.get("voc", {}) if status else {},
    }


# ── Activity & Cost Heatmap ────────────────────────────────────────

def _log_activity(command: str, script: str, exit_code: int, started_at: str) -> None:
    """Append an activity record to the activity log."""
    record = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "command": command,
        "script": script,
        "exitCode": exit_code,
        "startedAt": started_at,
    }
    try:
        ACTIVITY_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(ACTIVITY_LOG, "a") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:
        # Log to stderr so the error is visible in server logs
        import sys as _sys
        print(f"[hermes] Failed to write activity log: {exc}", file=_sys.stderr)


def _send_budget_alert(subject: str, body: str) -> bool:
    """Try to send a budget alert email. Returns True if sent."""
    smtp_host = os.getenv("HERMES_SMTP_HOST", "")
    smtp_port = int(os.getenv("HERMES_SMTP_PORT", "587"))
    smtp_user = os.getenv("HERMES_SMTP_USER", "")
    smtp_pass = os.getenv("HERMES_SMTP_PASS", "")
    if not smtp_host or not smtp_user:
        return False
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[Hermes Budget Alert] {subject}"
        msg["From"] = smtp_user
        msg["To"] = ALERT_EMAIL
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        return True
    except Exception:
        return False


@router.get("/activity-heatmap")
def hermes_activity_heatmap(days: int = 30, _=Depends(require_min_role("viewer"))) -> dict:
    """Return Hermes activity data for heatmap visualization."""
    records: list[dict] = []
    if ACTIVITY_LOG.is_file():
        for line in ACTIVITY_LOG.read_text().strip().split("\n"):
            if line.strip():
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass

    from collections import Counter
    date_counts = Counter(r["timestamp"][:10] for r in records)
    command_counts = Counter(r["command"] for r in records)

    # Build daily grid for last N days
    today = datetime.now(timezone.utc).date()
    days_list: list[dict] = []
    for i in range(days):
        d = today.replace(day=1) if False else today
        from datetime import timedelta
        d = today - timedelta(days=days - 1 - i)
        ds = d.strftime("%Y-%m-%d")
        days_list.append({"date": ds, "count": date_counts.get(ds, 0)})

    return {
        "totalRecords": len(records),
        "days": days_list,
        "byCommand": dict(command_counts),
        "lastRun": records[-1] if records else None,
    }


@router.get("/cost-heatmap")
def hermes_cost_heatmap(days: int = 30, _=Depends(require_min_role("viewer"))) -> dict:
    """Return daily cost data for heatmap visualization."""
    heatmap = build_daily_cost_heatmap(
        PROJECT_ROOT,
        days=max(1, min(days, 365)),
        daily_budget_cny=BUDGET_DAILY_CNY,
        monthly_budget_cny=BUDGET_MONTHLY_CNY,
    )
    alerts = heatmap.get("alerts", [])

    # Send email alert if budget exceeded
    if alerts:
        body = (
            "Hermes Cost Alert\n\n"
            f"Monthly total: {heatmap['totalCny']:.2f} CNY / {BUDGET_MONTHLY_CNY} CNY\n"
            f"Daily budget: {BUDGET_DAILY_CNY} CNY\n\nAlerts:\n"
        )
        body += "\n".join(f"- {a}" for a in alerts)
        body += f"\n\nView: https://www.ojeur.cloud/data-management → Hermes Governance"
        email_sent = _send_budget_alert("Budget Alert", body)
    else:
        email_sent = False

    return {
        **heatmap,
        "emailSent": email_sent,
        "alertEmail": ALERT_EMAIL,
    }


@router.get("/daily-summary")
def hermes_daily_summary(_=Depends(require_min_role("viewer"))) -> dict:
    """Return a combined activity+cost summary for today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Activity today
    activity_today = 0
    if ACTIVITY_LOG.is_file():
        for line in ACTIVITY_LOG.read_text().strip().split("\n"):
            if line.strip() and today in line:
                activity_today += 1

    # Cost today
    cost_cny = 0.0
    for record in load_cost_records(PROJECT_ROOT):
        created_at = str(record.get("createdAt") or record.get("recordedAt") or "")
        if created_at.startswith(today):
            cost_cny += float(record.get("estimatedCostCny") or 0)

    return {
        "date": today,
        "activityCount": activity_today,
        "costCny": round(cost_cny, 4),
        "dailyBudgetCny": BUDGET_DAILY_CNY,
        "monthlyBudgetCny": BUDGET_MONTHLY_CNY,
        "costStatus": "ok" if cost_cny <= BUDGET_DAILY_CNY else "over_daily",
    }


# ── Feature Kanban ─────────────────────────────────────────────────

@router.get("/feature-kanban")
def hermes_feature_kanban(_=Depends(require_min_role("viewer"))) -> dict:
    """Return features grouped by implementation status for kanban visualization."""
    path = HERMES_DIR / "feature_registry.yaml"
    if not path.is_file():
        raise HTTPException(404, "Feature registry not found")
    import yaml
    data = yaml.safe_load(path.read_text())
    features = data.get("features", []) if data else []

    columns: dict[str, list[dict]] = {
        "planned": [],
        "beta": [],
        "active": [],
        "archived": [],
    }

    COLORS: dict[str, str] = {
        "active": "#22c55e",
        "beta": "#3b82f6",
        "archived": "#94a3b8",
        "planned": "#f59e0b",
    }

    # Phase mapping: infer phase from implementation status
    def _infer_phase(f: dict) -> str:
        status = f.get("status", "")
        impl = f.get("implementationStatus", "")
        notes = str(f.get("notes", "") or "")
        if impl == "implemented":
            return "Phase 6 — Deployed"
        if impl == "partial":
            if status == "beta":
                return "Phase 3-4 — Beta testing"
            return "Phase 5 — Integration"
        if status == "planned":
            return "Phase 2 — PRD ready"
        if status == "archived":
            return "Archived"
        if impl == "prd_only":
            return "Phase 2 — PRD only"
        if any(k in notes.lower() for k in ["phase 0", "phase 1", "phase 2", "phase 3", "phase 4", "phase 5"]):
            for kw in ["phase 0", "phase 1", "phase 2", "phase 3", "phase 4", "phase 5", "phase 6"]:
                if kw in notes.lower():
                    return kw.capitalize()
        return "Phase 2 — Defined"

    for f in features:
        status = f.get("status", "unknown")
        impl = f.get("implementationStatus", "unknown")
        column = status if status in columns else "active"
        columns[column].append({
            "featureId": f.get("featureId", "?"),
            "name": f.get("name", "?"),
            "status": status,
            "implementationStatus": impl,
            "phase": _infer_phase(f),
            "riskLevel": f.get("riskLevel", "low"),
            "routes": f.get("routes", []),
            "backendApis": (f.get("backendApis", []) or [])[:3],
            "tests": f.get("tests", []),
            "docs": f.get("docs", []),
            "knownIssues": f.get("knownIssues", []),
            "governanceStatus": f.get("governanceStatus", "unmanaged"),
            "color": COLORS.get(status, "#94a3b8"),
        })

    return {
        "columns": {
            "planned":  {"label": "Planned",  "color": "#f59e0b", "features": columns["planned"]},
            "beta":     {"label": "Beta",     "color": "#3b82f6", "features": columns["beta"]},
            "active":   {"label": "Active",   "color": "#22c55e", "features": columns["active"]},
            "archived": {"label": "Archived", "color": "#94a3b8", "features": columns["archived"]},
        },
        "summary": {
            "total": len(features),
            "active": len(columns["active"]),
            "beta": len(columns["beta"]),
            "planned": len(columns["planned"]),
            "archived": len(columns["archived"]),
            "withTests": sum(1 for f in features if f.get("tests")),
            "withDocs": sum(1 for f in features if f.get("docs")),
            "withIssues": sum(1 for f in features if f.get("knownIssues")),
        },
    }


@router.get("/evidence-ledger")
def hermes_evidence_ledger(
    limit: int = Query(20, ge=1, le=100, description="Max records to return (1-100)"),
    days: int = Query(7, ge=1, le=90, description="Lookback window in days (1-90)"),
) -> dict:
    """Return recent evidence ledger entries with type breakdown.

    **Query parameters**:

    - ``limit`` (default 20, max 100): number of records in the response.
    - ``days`` (default 7, max 90): filter to entries within this lookback
      window (midnight-aligned UTC).

    **Response shape**::

        {
          "totalCount": 143,        // all-time count (ignores days filter)
          "records": [...],         // recent entries, sorted newest-first
          "byType": {"fact": 12, "event": 3},
          "rangeStart": "2026-05-08T...",  // oldest in filtered set
          "rangeEnd": "2026-05-15T..."     // newest in filtered set
        }

    **Empty states**:
        - JSONL file missing → ``{totalCount:0, records:[], byType:{}, rangeStart:"", rangeEnd:""}``
        - No entries in lookback → ``records:[]``, ``byType:{}``, range fields ``""``
    """
    path = HERMES_DIR / "evidence_ledger.jsonl"
    if not path.is_file():
        return {"totalCount": 0, "records": [], "byType": {}, "rangeStart": "", "rangeEnd": ""}
    entries: list[dict] = []
    for line in path.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
    entries.sort(key=lambda e: e.get("createdAt", ""), reverse=True)
    all_count = len(entries)
    if days:
        cutoff = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff = cutoff - timedelta(days=days - 1)
        cutoff_str = cutoff.isoformat()
        entries = [e for e in entries if (e.get("createdAt") or "") >= cutoff_str]
    by_type: dict[str, int] = {}
    for e in entries:
        t = e.get("type") or "unknown"
        by_type[t] = by_type.get(t, 0) + 1
    return {
        "totalCount": all_count,
        "records": entries[:limit],
        "byType": by_type,
        "rangeStart": entries[-1].get("createdAt", "") if entries else "",
        "rangeEnd": entries[0].get("createdAt", "") if entries else "",
    }


# Cache for markdown diagram scan
_md_diagrams_cache: dict[str, Any] = {"data": None, "mtimes": {}}

MERMAID_BLOCK_RE = re.compile(r"```mermaid\s*\n([\s\S]*?)```", re.MULTILINE)


def _classify_hermes_diagram(file_path: str, title: str, raw: str) -> dict[str, str]:
    haystack = f"{file_path} {title} {raw[:500]}".lower()
    if any(token in haystack for token in ["sentinel", "notification", "inbox", "alert"]):
        return {"category": "sentinel", "categoryLabel": "Sentinel"}
    if any(token in haystack for token in ["devsync", "dev event", "feature registry", "evidence ledger", "governance gap"]):
        return {"category": "devsync", "categoryLabel": "DevSync"}
    if any(token in haystack for token in ["deploy", "github actions", "systemctl", "ssh", "tencent"]):
        return {"category": "deploy", "categoryLabel": "Deploy"}
    if any(token in haystack for token in ["jato", "monthly update", "market scan", "parquet", "redis"]):
        return {"category": "data", "categoryLabel": "Data Pipeline"}
    if any(token in haystack for token in ["chat", "command", "gateway", "run"]):
        return {"category": "gateway", "categoryLabel": "Gateway"}
    if any(token in haystack for token in ["source", "cost", "audit", "quality"]):
        return {"category": "audit", "categoryLabel": "Audit"}
    if any(token in haystack for token in ["hermes", "architecture", "core", "ledger"]):
        return {"category": "core", "categoryLabel": "Core"}
    return {"category": "other", "categoryLabel": "Other"}


@router.get("/reports/full-design-document")
def hermes_full_design_document(_=Depends(require_min_role("viewer"))) -> dict:
    """Return the Hermes full design document as Markdown text for in-app reading."""
    path = HERMES_DIR / "reports" / "HERMES_FULL_DESIGN_DOCUMENT.md"
    if not path.is_file():
        return {"exists": False, "path": str(path.relative_to(PROJECT_ROOT)), "content": "", "updatedAt": None}
    stat = path.stat()
    return {
        "exists": True,
        "path": str(path.relative_to(PROJECT_ROOT)),
        "content": path.read_text(encoding="utf-8", errors="replace"),
        "updatedAt": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


@router.get("/markdown-diagrams")
def hermes_markdown_diagrams(_=Depends(require_min_role("viewer")),
    file_filter: str | None = Query(
        None,
        description="Substring match on file path (case-insensitive). "
                    "Omit to return diagrams from all files.",
    ),
) -> list[dict]:
    """Scan Markdown_Readme/ for mermaid diagram blocks.

    Recursively walks ``Markdown_Readme/``, extracts every fenced
    `` ```mermaid `` block, and returns them as structured records ready
    for client-side rendering.

    **Caching**: results are cached in-memory.  The cache is invalidated
    automatically when any scanned file's ``st_mtime`` changes or a file
    is added/removed.

    **Response shape** — each list element::

        {
          "file": "Markdown_Readme/Fullstack/WORKFLOWS/ETL.md",
          "title": "Monthly pipeline stages",   // nearest preceding heading
          "diagramIndex": 0,
          "raw": "flowchart TD\\n  A --> B\\n  ...",
          "type": "flowchart"                   // inferred from first line
        }

    **Empty states**:
        - ``Markdown_Readme/`` missing → ``[]`` (200)
        - No mermaid blocks found → ``[]`` (200)
    """
    md_root = PROJECT_ROOT / "Markdown_Readme"
    if not md_root.is_dir():
        return []

    # Check if cache is valid (no mtime changes, no file deletions)
    cache_valid = _md_diagrams_cache["data"] is not None
    if cache_valid:
        for fpath_str, cached_mtime in list(_md_diagrams_cache["mtimes"].items()):
            p = Path(fpath_str)
            if not p.is_file() or p.stat().st_mtime != cached_mtime:
                cache_valid = False
                break

    if not cache_valid:
        diagrams: list[dict] = []
        mtimes: dict[str, float] = {}
        for md_file in sorted(md_root.rglob("*.md")):
            try:
                content = md_file.read_text()
            except Exception:
                continue
            blocks = MERMAID_BLOCK_RE.findall(content)
            if not blocks:
                continue
            rel_path = str(md_file.relative_to(PROJECT_ROOT))
            mtimes[rel_path] = md_file.stat().st_mtime
            # Find heading preceding each mermaid block
            remaining = content
            for idx, block_src in enumerate(blocks):
                block_src = block_src.strip()
                if not block_src:
                    continue  # skip empty blocks
                # Determine diagram type from first line
                first_line = block_src.split("\n")[0].strip()
                dtype = "flowchart"
                if first_line.startswith("sequenceDiagram"):
                    dtype = "sequenceDiagram"
                elif first_line.startswith("flowchart") or first_line.startswith("graph"):
                    dtype = "flowchart"
                elif first_line.startswith("classDiagram"):
                    dtype = "classDiagram"
                elif first_line.startswith("stateDiagram"):
                    dtype = "stateDiagram"
                elif first_line.startswith("gantt"):
                    dtype = "gantt"
                elif first_line.startswith("pie"):
                    dtype = "pie"
                # Try to find a heading preceding this block in remaining text
                title = ""
                block_pos = remaining.find("```mermaid")
                if block_pos > 0:
                    before = remaining[:block_pos]
                    headings = re.findall(r"^#{1,4}\s+(.+)$", before, re.MULTILINE)
                    if headings:
                        title = headings[-1].strip()
                    # Advance past this block so next iteration looks at subsequent text
                    end_pos = remaining.find("```", block_pos + len("```mermaid") + len(block_src))
                    if end_pos < 0:
                        end_pos = block_pos + len("```mermaid") + len(block_src)
                    remaining = remaining[end_pos + 3:]
                diagrams.append({
                    "file": rel_path,
                    "title": title,
                    "diagramIndex": idx,
                    "raw": block_src,
                    "type": dtype,
                    **_classify_hermes_diagram(rel_path, title, block_src),
                })
        _md_diagrams_cache["data"] = diagrams
        _md_diagrams_cache["mtimes"] = mtimes

    result: list[dict] = _md_diagrams_cache["data"]  # type: ignore[assignment]
    if file_filter:
        result = [d for d in result if file_filter.lower() in d["file"].lower()]
    return result


# ═══════════════════════════════════════════════════════════════════════
# Hermes Chat Gateway
# ═══════════════════════════════════════════════════════════════════════


@router.post("/chat")
def hermes_chat(payload: dict = Body(...), _=Depends(require_min_role("viewer"))) -> dict:
    """Natural-language entry point for Hermes.

    **Request**::

        {
          "message": "show open governance gaps",
          "sessionId": "optional-existing-session",
          "context": {"userRole": "user|admin|developer"}
        }

    **Response** — ``replyType`` determines shape:

    - ``direct_answer``: ``answer`` + ``dataRefs``
    - ``run_created``: ``answer`` + ``runId`` + ``tasks`` + ``command``
    - ``clarification_needed``: ``answer`` + ``suggestedActions``
    - ``blocked_by_policy``: ``answer`` + block reason
    """
    from app.services.hermes_chat_service import (  # lazy import
        add_message,
        cleanup_old_sessions,
        create_run_response,
        create_session,
        generate_direct_answer,
        router as intent_router,
    )
    message = (payload.get("message") or "").strip()
    if not message:
        raise HTTPException(400, "message is required")

    session_id = payload.get("sessionId") or ""
    context: dict = payload.get("context") or {}

    # Classify intent
    classification = intent_router.classify(message, context)

    # Generate response
    intent = classification["intent"]
    exec_mode = classification["executionMode"]
    entities = classification.get("entities", {})

    if exec_mode == "direct_answer":
        resp = generate_direct_answer(intent, entities)
    elif exec_mode == "create_run":
        resp = create_run_response(intent, entities)
    elif exec_mode == "blocked_by_policy":
        resp = {
            "replyType": "blocked_by_policy",
            "answer": classification.get("blockReason", "This action is blocked by policy."),
            "intent": intent,
            "entities": entities,
            "dataRefs": [],
            "suggestedActions": [],
        }
    else:
        suggested = classification.get("suggestedIntents", [])
        resp = {
            "replyType": "clarification_needed",
            "answer": "I'm not sure what you want me to do. Did you mean one of these?",
            "intent": intent,
            "entities": entities,
            "dataRefs": [],
            "suggestedActions": [
                {"label": si.replace("_", " ").title(), "action": "retry_with_intent", "intent": si}
                for si in suggested
            ],
        }

    resp["confidence"] = classification["confidence"]

    # Session management
    if not session_id:
        s = create_session()
        session_id = s["sessionId"]
    message_id = f"msg_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    add_message(session_id, {
        "messageId": message_id,
        "role": "user",
        "content": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    add_message(session_id, {
        "messageId": message_id + "_resp",
        "role": "assistant",
        "content": resp["answer"],
        "replyType": resp["replyType"],
        "intent": intent,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    # Cleanup old sessions (run occasionally)
    try:
        cleanup_old_sessions(24)
    except Exception:
        pass

    resp["sessionId"] = session_id
    resp["messageId"] = message_id
    return resp


@router.get("/chat/sessions")
def hermes_chat_sessions(limit: int = Query(20, ge=1, le=100), _=Depends(require_min_role("viewer"))) -> list[dict]:
    """List recent chat sessions."""
    from app.services.hermes_chat_service import list_sessions
    return list_sessions(limit)


@router.get("/chat/sessions/{session_id}")
def hermes_chat_session(session_id: str, _=Depends(require_min_role("viewer"))) -> dict:
    """Get a chat session with all messages."""
    from app.services.hermes_chat_service import get_session
    s = get_session(session_id)
    if s is None:
        raise HTTPException(404, f"Session not found: {session_id}")
    return s


@router.get("/commands")
def hermes_commands(_=Depends(require_min_role("viewer"))) -> list[dict]:
    """Return all executable commands with parameters.

    Each command includes label, description, required role, and
    parameter schema so the frontend can render buttons or
    auto-complete from natural-language input.
    """
    def _role(cmd_id: str) -> str:
        return HERMES_SCRIPTS.get(cmd_id, {}).get("requiredRole", "admin")

    return [
        {
            "commandId": "pipeline-audit",
            "label": "Pipeline Audit",
            "description": "Scan systemd/Airflow/GH Actions for pipeline health.",
            "requiredRole": _role("pipeline-audit"),
            "mapsToIntent": "pipeline_audit",
            "parameters": [],
        },
        {
            "commandId": "source-quality",
            "label": "Source Audit",
            "description": "Score VOC/News/MSRP source health 0-100.",
            "requiredRole": _role("source-quality"),
            "mapsToIntent": "source_audit",
            "parameters": [],
        },
        {
            "commandId": "cost-report",
            "label": "Cost Report",
            "description": "Calculate Flash/Pro token costs vs monthly budget.",
            "requiredRole": _role("cost-report"),
            "mapsToIntent": "cost_refresh",
            "parameters": [],
        },
        {
            "commandId": "code-audit",
            "label": "Code Audit",
            "description": "Run 10-rule git diff audit scan.",
            "requiredRole": _role("code-audit"),
            "mapsToIntent": "code_audit",
            "parameters": [
                {"name": "base", "type": "string", "required": False, "default": "main"},
                {"name": "head", "type": "string", "required": False, "default": "HEAD"},
            ],
        },
        {
            "commandId": "evidence",
            "label": "Evidence Writer",
            "description": "Extract structured facts from artifacts into JSONL.",
            "requiredRole": _role("evidence"),
            "mapsToIntent": "evidence_refresh",
            "parameters": [],
        },
        {
            "commandId": "answer-audit",
            "label": "Answer Audit",
            "description": "Generate sample answer quality audits.",
            "requiredRole": _role("answer-audit"),
            "mapsToIntent": "evidence_refresh",
            "parameters": [],
        },
    ]


@router.post("/commands/execute")
def hermes_command_execute(payload: dict = Body(...), user=Depends(require_min_role("viewer"))) -> dict:
    """Execute a Hermes command via the centralised ops runner.

    **Request**::

        {
          "commandId": "source-quality",
          "parameters": {},
          "sessionId": "optional"
        }

    **Response**: same as ``POST /run/{command}``.
    """
    command_id = (payload.get("commandId") or "").strip()
    if not command_id:
        raise HTTPException(400, "commandId is required")

    _check_command_role(command_id, user.role)

    try:
        return execute_hermes_command(
            command_id,
            parameters=payload.get("parameters") or None,
            actor=getattr(user, "name", "unknown"),
            session_id=payload.get("sessionId") or "",
        )
    except HermesRunError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc


# ═══════════════════════════════════════════════════════════════════════
# Hermes DevSync — Development Governance Loop
# ═══════════════════════════════════════════════════════════════════════


@router.post("/dev/events")
def hermes_dev_event_post(payload: dict = Body(...), _=Depends(require_min_role("admin"))) -> dict:
    """Append a development event (from Claude Code or other source).

    **Request**::

        {
          "eventType": "implementation_completed",
          "source": "claude_code",
          "title": "Feature title",
          "summary": "What was done",
          "linkedFeatureIds": ["feature-id"],
          "changedFiles": ["path/to/file.py"],
          "addedEndpoints": ["POST /some/endpoint"],
          "tests": {"backend": "647 passed"},
          "risks": [],
          "nextSteps": []
        }
    """
    from app.services.hermes_devsync_service import append_dev_event
    event = append_dev_event(payload)
    return event


@router.get("/dev/events")
def hermes_dev_events(
    event_type: str | None = Query(None, description="Filter: implementation_completed, test_run, ..."),
    source: str | None = Query(None, description="Filter: claude_code, web, manual, ..."),
    limit: int = Query(50, ge=1, le=200),
) -> list[dict]:
    """List development events."""
    from app.services.hermes_devsync_service import list_dev_events
    return list_dev_events(event_type=event_type, source=source, limit=limit)


@router.post("/dev/sync")
def hermes_dev_sync(
    payload: dict = Body(default_factory=dict),
    source: str | None = Query(None, description="Caller: github_actions, claude_code, manual"),
    authorization: str | None = Header(None, alias="Authorization"),
    user=Depends(get_current_user),
) -> dict:
    """Trigger DevSync — read dev events, update features, generate MD, write evidence, create gaps.

    **Authentication** (for GitHub Actions)::

        Authorization: Bearer <HERMES_SYNC_TOKEN>

    **Idempotency**: if ``commitSha`` + ``workflowRunId`` were already synced,
    returns ``{"status": "already_synced", ...}``.

    **Request body** (optional)::

        {
          "source": "github_actions",
          "commitSha": "abc123",
          "workflowRunId": "12345",
          "branch": "main"
        }
    """
    from app.services.hermes_devsync_service import sync_dev_events

    # Token auth for GitHub Actions — fail-closed
    sync_token = os.getenv("HERMES_SYNC_TOKEN", "").strip()
    if source == "github_actions":
        if not sync_token:
            raise HTTPException(401, "GitHub Actions sync requires HERMES_SYNC_TOKEN to be configured")
        token = ""
        if authorization and authorization.startswith("Bearer "):
            token = authorization[7:].strip()
        if token != sync_token:
            raise HTTPException(401, "Invalid or missing sync token")
    elif ROLE_LEVEL.get(user.role, 0) < ROLE_LEVEL["developer"]:
        raise HTTPException(403, "Forbidden")

    # Idempotency check
    commit_sha = payload.get("commitSha", "") if isinstance(payload, dict) else ""
    run_id = payload.get("workflowRunId", "") if isinstance(payload, dict) else ""
    expected_deploy = None
    if commit_sha:
        from app.services.hermes_deploy_status_service import record_expected_deploy
        expected_deploy = record_expected_deploy(payload, source=source or payload.get("source") or "unknown")

    if commit_sha and run_id:
        idem_path = HERMES_DIR / "dev_events" / f"_sync_{commit_sha}_{run_id}.json"
        if idem_path.is_file():
            return {
                "status": "already_synced",
                "commitSha": commit_sha,
                "workflowRunId": run_id,
                "expectedDeploy": expected_deploy,
            }

    result = sync_dev_events()
    if expected_deploy:
        result["expectedDeploy"] = expected_deploy

    # Write idempotency marker
    if commit_sha and run_id:
        try:
            idem_path = HERMES_DIR / "dev_events" / f"_sync_{commit_sha}_{run_id}.json"
            idem_path.parent.mkdir(parents=True, exist_ok=True)
            idem_path.write_text(json.dumps({"syncedAt": datetime.now(timezone.utc).isoformat()}))
        except Exception:
            pass

    return result


@router.get("/deploy/status")
def hermes_deploy_status(_=Depends(require_min_role("viewer"))) -> dict:
    """Return deployed release metadata and expected GitHub commit drift."""
    from app.services.hermes_deploy_status_service import get_deploy_status
    return get_deploy_status()


@router.get("/history/events")
def hermes_history_events(
    source: str | None = Query(None),
    workstream: str | None = Query(None),
    model: str | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return normalized Hermes history events from git and governance ledgers."""
    from app.services.hermes_history_service import list_history_events

    return list_history_events(
        source=source,
        workstream=workstream,
        model=model,
        limit=limit,
    )


@router.get("/history/clusters")
def hermes_history_clusters(
    level: str = Query("feature", description="epic, workstream, feature, session, or commit"),
    y_axis: str = Query("workstream", alias="yAxis", description="workstream, phase, risk, or session"),
    workstream: str | None = Query(None),
    limit: int = Query(160, ge=1, le=300),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return rule-based history clusters for the Hermes History Map."""
    from app.services.hermes_history_service import list_history_clusters

    return list_history_clusters(
        level=level,
        y_axis=y_axis,
        workstream=workstream,
        limit=limit,
    )


@router.get("/progress/features")
def hermes_progress_features(_=Depends(require_min_role("viewer"))) -> list[dict]:
    """Return feature progress states for the Hermes cockpit."""
    from app.services.hermes_history_service import list_progress_features

    return list_progress_features()


@router.get("/progress/swimlanes")
def hermes_progress_swimlanes(_=Depends(require_min_role("viewer"))) -> dict:
    """Return feature progress grouped by workstream."""
    from app.services.hermes_history_service import get_progress_swimlanes

    return get_progress_swimlanes()


@router.get("/workflow/cockpit")
def hermes_workflow_cockpit(_=Depends(require_min_role("viewer"))) -> dict:
    """Return model/session workflow aggregation for the Hermes cockpit."""
    from app.services.hermes_history_service import get_workflow_cockpit

    return get_workflow_cockpit()


@router.get("/goals/features")
def hermes_goal_features(_=Depends(require_min_role("viewer"))) -> dict:
    """Return read-only Feature PMO goals from Markdown, registries, and evidence."""
    from app.services.hermes_feature_goal_service import list_feature_goals

    return list_feature_goals()


@router.get("/goals/features/{feature_id}")
def hermes_goal_feature(feature_id: str, _=Depends(require_min_role("viewer"))) -> dict:
    """Return one computed Feature PMO goal."""
    from app.services.hermes_feature_goal_service import get_feature_goal

    return get_feature_goal(feature_id)


@router.get("/goals/swimlanes")
def hermes_goal_swimlanes(_=Depends(require_min_role("viewer"))) -> dict:
    """Return Feature PMO goals grouped by workstream."""
    from app.services.hermes_feature_goal_service import get_feature_goal_swimlanes

    return get_feature_goal_swimlanes()


@router.get("/reuse/candidates")
def hermes_reuse_candidates(
    feature_id: str = Query(..., alias="featureId"),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Return reuse candidates for a tracked feature."""
    from app.services.hermes_feature_goal_service import get_reuse_candidates_for_feature

    return get_reuse_candidates_for_feature(feature_id)


@router.get("/dev/features")
def hermes_dev_features(
    status: str | None = Query(None),
    category: str | None = Query(None),
) -> list[dict]:
    """List features from the DevSync feature registry."""
    from app.services.hermes_devsync_service import list_features
    return list_features(status=status, category=category)


@router.get("/dev/features/{feature_id}")
def hermes_dev_feature(feature_id: str, _=Depends(require_min_role("viewer"))) -> dict:
    """Get a single feature by ID."""
    from app.services.hermes_devsync_service import get_feature
    f = get_feature(feature_id)
    if f is None:
        raise HTTPException(404, f"Feature not found: {feature_id}")
    return f


@router.get("/dev/workspace-health")
def hermes_dev_workspace_health(_=Depends(require_min_role("viewer"))) -> dict:
    """Return workspace health — uncommitted changes, unsynced events, risk level.

    Detects blind spots where code changed but no dev event was written.
    Only works when running locally (needs git + repo access).
    """
    from app.services.hermes_workspace_health_service import get_workspace_health

    health = get_workspace_health()
    health.setdefault("pushedUnsyncedEvents", 0)
    return health


# ═══════════════════════════════════════════════════════════════
# Hermes Sentinel — Unified Proactive Monitoring
# ═══════════════════════════════════════════════════════════════


@router.get("/sentinel/status")
def hermes_sentinel_status(_=Depends(require_min_role("viewer"))) -> dict:
    """Run all probes, return aggregated Sentinel status."""
    from app.services.hermes_sentinel_service import run_all_probes
    return run_all_probes()


@router.get("/sentinel/notifications")
def hermes_sentinel_notifications(_=Depends(require_min_role("viewer")),
    limit: int = Query(50, ge=1, le=200),
    status: str | None = Query(None),
) -> list[dict]:
    """List recent sentinel notifications."""
    from app.services.hermes_sentinel_service import get_notifications
    return get_notifications(limit=limit, status=status)


@router.post("/sentinel/ack/{notification_id}")
def hermes_sentinel_ack(notification_id: str, _=Depends(require_min_role("viewer"))) -> dict:
    """Mark a notification as acknowledged."""
    from app.services.hermes_sentinel_service import ack_notification
    n = ack_notification(notification_id)
    if n is None:
        raise HTTPException(404, f"Notification not found: {notification_id}")
    return n


@router.post("/sentinel/notifications/{notification_id}/status")
def hermes_sentinel_set_notification_status(
    notification_id: str,
    payload: dict = Body(...),
    _=Depends(require_min_role("viewer")),
) -> dict:
    """Move a Sentinel notification between inbox mailbox states."""
    from app.services.hermes_sentinel_service import set_notification_status

    try:
        n = set_notification_status(notification_id, str(payload.get("status", "")))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if n is None:
        raise HTTPException(404, f"Notification not found: {notification_id}")
    return n
