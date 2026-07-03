#!/usr/bin/env python3
"""Hermes MSRP Country Progress Probe — Phase 6.

Reads the v3 dryrun_report.json and produces country-level progress
status with severity findings.

Usage:
  python 03_Scripts/hermes/hermes_msrp_country_progress.py
  python 03_Scripts/hermes/hermes_msrp_country_progress.py --out-dir hermes/reports
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
STATUS_FILE_PATH = REPO_ROOT / "03_Scripts" / "logs" / "scheduled_fetch_status.json"
FALLBACK_REPORT_PATH = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "dryrun_report.json"
RUNS_INDEX_PATH = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "dryrun_runs_index.json"
SOURCE_REPAIR_BACKLOG_PATH = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_source_repair_backlog.json"
SOURCE_REFERENCE_EVIDENCE_PATH = (
    REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_source_reference_evidence.json"
)
SOURCE_ACCESSIBILITY_AUDIT_PATH = (
    REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_source_accessibility_audit.json"
)
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
COUNTRY_LABELS = {
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


def _load_dryrun_report() -> dict | None:
    """Load dryrun report from status file pointer, then fallback."""
    if STATUS_FILE_PATH.is_file():
        try:
            status = json.loads(STATUS_FILE_PATH.read_text())
            msrp = status.get("msrp_dryrun") or {}
            artifact_path = msrp.get("artifactPath")
            if artifact_path:
                p = REPO_ROOT / artifact_path
                if p.is_file():
                    return json.loads(p.read_text())
        except Exception:
            pass
    if FALLBACK_REPORT_PATH.is_file():
        try:
            return json.loads(FALLBACK_REPORT_PATH.read_text())
        except Exception:
            pass
    return None


def _load_source_repair_backlog() -> dict:
    if SOURCE_REPAIR_BACKLOG_PATH.is_file():
        try:
            return json.loads(SOURCE_REPAIR_BACKLOG_PATH.read_text())
        except Exception:
            pass
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


def _load_runs_index() -> dict[str, Any] | None:
    if not RUNS_INDEX_PATH.is_file():
        return None
    try:
        data = json.loads(RUNS_INDEX_PATH.read_text())
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _default_source_reference_evidence() -> dict:
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


def _load_source_reference_evidence(run_id: str | None = None) -> dict:
    if SOURCE_REFERENCE_EVIDENCE_PATH.is_file():
        try:
            data = json.loads(SOURCE_REFERENCE_EVIDENCE_PATH.read_text())
            if not isinstance(data, dict):
                return _default_source_reference_evidence()
            evidence_run_id = str(data.get("backlogRunId") or "")
            target_run_id = str(run_id or "")
            if target_run_id and evidence_run_id and evidence_run_id != target_run_id:
                return _default_source_reference_evidence()
            return data
        except Exception:
            pass
    return _default_source_reference_evidence()


def _default_source_accessibility_audit() -> dict:
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


def _load_source_accessibility_audit(run_id: str | None = None) -> dict:
    if SOURCE_ACCESSIBILITY_AUDIT_PATH.is_file():
        try:
            data = json.loads(SOURCE_ACCESSIBILITY_AUDIT_PATH.read_text())
            if not isinstance(data, dict):
                return _default_source_accessibility_audit()
            audit_run_id = str(data.get("backlogRunId") or "")
            target_run_id = str(run_id or "")
            if target_run_id and audit_run_id and audit_run_id != target_run_id:
                return _default_source_accessibility_audit()
            return data
        except Exception:
            pass
    return _default_source_accessibility_audit()


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
    return host[4:] if host.startswith("www.") else host


def _source_issue_detail_from_report_source(
    *,
    source: dict[str, Any],
    country_code: str,
    source_code: str,
    failure_reason: str,
    recommended_strategy: str,
    last_good: dict[str, Any] | None = None,
    transient_recheck: bool = False,
    external_access: bool = False,
    pipeline_issue: bool = False,
) -> dict[str, Any]:
    source_url = _source_url(source)
    detail: dict[str, Any] = {
        "countryCode": country_code,
        "sourceCode": source_code,
        "brand": source.get("brand"),
        "sourceUrl": source_url,
        "host": _source_host(source),
        "status": source.get("status"),
        "rawStatus": source.get("rawStatus"),
        "valid": _int_value(source.get("valid")),
        "extracted": _int_value(source.get("extracted")),
        "failureReason": failure_reason,
        "recommendedStrategy": recommended_strategy,
        "sourceRepairIssue": (
            not transient_recheck
            and not external_access
            and not pipeline_issue
        ),
        "transientRegression": transient_recheck,
        "externalAccessIssue": external_access,
        "pipelineIssue": pipeline_issue,
        "recommendedAction": (
            "recheck_before_source_repair"
            if transient_recheck
            else "official_proxy_or_configurator_api"
            if external_access
            else "fix_runner_or_pipeline"
            if pipeline_issue
            else "repair_source_definition"
        ),
    }
    for key in (
        "httpStatus",
        "finalUrl",
        "extractorName",
        "coverageLevel",
        "rejectedReasons",
        "rejectedRules",
        "rejectionReasonCounts",
        "rejectionRuleCounts",
        "sampleRejectedObservations",
    ):
        value = source.get(key)
        if value not in (None, ""):
            detail[key] = value
    error_text = str(source.get("extractorError") or source.get("error") or "")
    if error_text:
        detail["errorSnippet"] = error_text.replace("\n", " ")[:500]
    if last_good:
        detail["lastKnownGoodRunId"] = last_good.get("runId")
        detail["lastKnownGoodAt"] = last_good.get("observedAt")
        detail["lastKnownGoodValid"] = last_good.get("valid")
    if not source_url:
        detail.pop("sourceUrl", None)
    if not detail.get("brand"):
        detail.pop("brand", None)
    if not detail.get("host"):
        detail.pop("host", None)
    return detail


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
            "reason": (
                "Official source access is blocked by the current fetch path; "
                "use an official proxy or configurator API before selector repair."
            ),
        }
    if source_repair_count <= 0 and pipeline_issue_count > 0:
        return {
            "preferred": "fix_runner_or_pipeline",
            "llmFit": "low",
            "neuralNetworkFit": "not_recommended",
            "reason": (
                "The dryrun failed in the runner or pipeline environment; fix "
                "the local/server runtime before repairing source definitions."
            ),
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
            "reason": (
                "Rules identify the failure class; an LLM can propose selector or "
                "extraction repair from page evidence."
            ),
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


def _source_status_value(source: dict[str, Any]) -> str:
    return str(source.get("rawStatus") or source.get("status") or "").lower()


def _source_valid_count(source: dict[str, Any]) -> int:
    return _int_value(source.get("valid"))


def _source_is_empty(source: dict[str, Any]) -> bool:
    return _source_status_value(source) == "empty"


def _source_is_error(source: dict[str, Any]) -> bool:
    return _source_status_value(source) in {"error", "exception"}


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


def _artifact_path_from_ref(path_ref: str | None) -> Path | None:
    if not path_ref:
        return None
    path = Path(path_ref)
    return path if path.is_absolute() else REPO_ROOT / path


def _load_v3_report(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    return data if data.get("schemaVersion") == "msrp_dryrun_report_v3" else None


def _run_recency_key(run: dict[str, Any]) -> tuple[str, str]:
    run_id = str(run.get("runId") or "")
    timestamp = str(
        run.get("finishedAt")
        or run.get("startedAt")
        or run.get("updatedAt")
        or ""
    )
    return timestamp, run_id


def _country_label(country_code: str) -> str:
    code = str(country_code or "").lower()
    return COUNTRY_LABELS.get(code, code.upper())


def _status_for_pass_rate(pass_rate: float) -> str:
    if pass_rate >= 90:
        return "success"
    if pass_rate >= 50:
        return "degraded"
    return "failure"


def _normalize_source_for_progress(
    source: dict[str, Any],
    index: int,
    total: int,
) -> dict[str, Any]:
    raw_status = _source_status_value(source)
    if _source_is_pass(source):
        status = "pass"
    elif _source_is_empty(source):
        status = "empty"
    else:
        status = "fail"
    payload = {
        "index": int(source.get("index") or index),
        "totalInCountry": int(source.get("totalInCountry") or total),
        "sourceCode": str(source.get("sourceCode") or source.get("code") or ""),
        "status": status,
        "rawStatus": raw_status,
        "valid": _source_valid_count(source),
        "extracted": _int_value(source.get("extracted")),
        "rejected": _int_value(source.get("rejected")),
        "failureReason": source.get("failureReason"),
        "recommendedStrategy": source.get("recommendedStrategy"),
    }
    for key in ("sourceUrl", "finalUrl", "httpStatus", "extractorError", "error"):
        value = source.get(key)
        if value not in (None, ""):
            payload[key] = value
    return payload


def _source_counts(sources: list[dict[str, Any]]) -> tuple[int, int, int, int]:
    pass_count = sum(1 for source in sources if source["status"] == "pass")
    empty_count = sum(1 for source in sources if source["status"] == "empty")
    error_count = sum(1 for source in sources if _source_is_error(source))
    fail_count = sum(
        1
        for source in sources
        if source["status"] == "fail" and not _source_is_error(source)
    )
    return pass_count, empty_count, fail_count, error_count


def _country_gate_threshold(
    run_meta: dict[str, Any],
    report: dict[str, Any],
) -> float:
    summary = report.get("summary") or {}
    for value in (run_meta.get("gateThreshold"), summary.get("gateThreshold")):
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 70.0


def _country_from_v3(
    country: dict[str, Any],
    *,
    run_meta: dict[str, Any],
    latest_run_id: str,
) -> dict[str, Any] | None:
    code = str(country.get("countryCode") or country.get("country") or "").lower()
    if not re.fullmatch(r"[a-z]{2}", code):
        return None
    total = _int_value(country.get("total"))
    sources = [
        _normalize_source_for_progress(source, index, total)
        for index, source in enumerate(country.get("sources") or [], start=1)
    ]
    if sources:
        pass_count, empty_count, fail_count, error_count = _source_counts(sources)
    else:
        pass_count = _int_value(country.get("pass"))
        empty_count = _int_value(country.get("empty"))
        fail_count = _int_value(country.get("fail"))
        error_count = _int_value(country.get("errors"))
    pass_pct = round(pass_count / max(total, 1) * 100, 1) if total else float(country.get("passPct") or 0)
    run_id = str(run_meta.get("runId") or "")
    top_failure = country.get("topFailureReason")
    failure_breakdown = country.get("failureBreakdown") or {}
    if not top_failure and failure_breakdown:
        top_failure = max(failure_breakdown, key=failure_breakdown.get)
    return {
        "countryCode": code,
        "countryLabel": _country_label(code),
        "total": total,
        "pass": pass_count,
        "empty": empty_count,
        "fail": fail_count,
        "errors": error_count,
        "passPct": pass_pct,
        "status": (
            "missing"
            if country.get("status") == "missing"
            else _status_for_pass_rate(pass_pct)
        ),
        "topFailureReason": top_failure,
        "failureBreakdown": failure_breakdown,
        "strategyRecommendations": country.get("strategyRecommendations") or {},
        "financeObservationCandidates": _int_value(country.get("financeObservationCandidates")),
        "financeMonthlyPaymentCount": _int_value(country.get("financeMonthlyPaymentCount")),
        "financeSemanticsCounts": _count_map(country.get("financeSemanticsCounts")),
        "financeTypeCounts": _count_map(country.get("financeTypeCounts")),
        "runId": run_id,
        "batch": run_meta.get("batch") or "",
        "timestamp": run_meta.get("finishedAt") or run_meta.get("startedAt") or "",
        "gateStatus": run_meta.get("gateStatus"),
        "runStatus": run_meta.get("status"),
        "isLatestRun": run_id == latest_run_id,
        "completed": country.get("status") != "missing",
        "sources": sources,
    }


def _is_stable_country_observation(
    country: dict[str, Any],
    run_meta: dict[str, Any],
    report: dict[str, Any],
) -> bool:
    if not country.get("completed", True):
        return False
    try:
        pass_pct = float(country.get("passPct") or 0)
    except (TypeError, ValueError):
        pass_pct = 0.0
    return pass_pct >= _country_gate_threshold(run_meta, report)


def _all_country_latest_from_runs_index() -> list[dict[str, Any]]:
    index_data = _load_runs_index()
    if not index_data:
        return []
    latest_run_id = str(index_data.get("latestRunId") or "")
    stable_by_code: dict[str, dict[str, Any]] = {}
    fallback_by_code: dict[str, dict[str, Any]] = {}
    for run_meta in sorted(
        index_data.get("runs") or [],
        key=_run_recency_key,
        reverse=True,
    ):
        artifact_path = _artifact_path_from_ref(run_meta.get("artifactPath"))
        report = _load_v3_report(artifact_path)
        if not report:
            continue
        for country in report.get("countriesDetail") or []:
            normalized = _country_from_v3(
                country,
                run_meta=run_meta,
                latest_run_id=latest_run_id,
            )
            if not normalized:
                continue
            code = normalized["countryCode"]
            fallback_by_code.setdefault(code, normalized)
            if code not in stable_by_code and _is_stable_country_observation(
                normalized,
                run_meta,
                report,
            ):
                stable_by_code[code] = normalized

    countries_by_code = {
        code: stable_by_code.get(code, fallback)
        for code, fallback in fallback_by_code.items()
    }
    return sorted(
        countries_by_code.values(),
        key=lambda item: (
            0 if item.get("isLatestRun") else 1,
            str(item.get("countryLabel") or item.get("countryCode") or ""),
        ),
    )


def _finance_summary_from_countries(countries: list[dict[str, Any]]) -> dict[str, Any]:
    semantics: dict[str, int] = {}
    types: dict[str, int] = {}
    for country in countries:
        for key, target in (
            ("financeSemanticsCounts", semantics),
            ("financeTypeCounts", types),
        ):
            for name, count in (country.get(key) or {}).items():
                label = str(name or "").strip() or "unknown"
                target[label] = target.get(label, 0) + _int_value(count)
    return {
        "financeObservationCandidates": sum(
            _int_value(country.get("financeObservationCandidates"))
            for country in countries
        ),
        "financeMonthlyPaymentCount": sum(
            _int_value(country.get("financeMonthlyPaymentCount"))
            for country in countries
        ),
        "financeSemanticsCounts": dict(
            sorted(semantics.items(), key=lambda item: (-item[1], item[0]))
        ),
        "financeTypeCounts": dict(
            sorted(types.items(), key=lambda item: (-item[1], item[0]))
        ),
    }


def _stable_coverage_summary(
    all_countries: list[dict[str, Any]],
    current_report: dict[str, Any],
) -> dict[str, Any]:
    current_summary = current_report.get("summary") or {}
    gate_threshold = float(current_summary.get("gateThreshold") or 70)
    ready_countries = [
        country
        for country in all_countries
        if float(country.get("passPct") or 0) >= gate_threshold
    ]
    total_sources = sum(_int_value(country.get("total")) for country in all_countries)
    total_pass = sum(_int_value(country.get("pass")) for country in all_countries)
    finance = _finance_summary_from_countries(all_countries)
    latest_run_id = str(next(
        (
            country.get("runId")
            for country in all_countries
            if country.get("isLatestRun") and country.get("runId")
        ),
        "",
    ))
    if not latest_run_id:
        latest_run_id = str(next(
            (country.get("runId") for country in all_countries if country.get("runId")),
            "",
        ))
    active_run_id = str(current_report.get("runId") or "")
    stable_good_sources: dict[tuple[str, str], dict[str, Any]] = {}
    failure_counts: dict[str, int] = {}
    repair_samples: list[dict[str, Any]] = []
    source_rows_observed = 0
    for country in all_countries:
        country_code = str(country.get("countryCode") or "")
        for source in country.get("sources") or []:
            source_code = str(source.get("sourceCode") or "")
            if not country_code or not source_code:
                continue
            source_rows_observed += 1
            if source.get("status") == "pass":
                stable_good_sources[(country_code, source_code)] = {
                    **source,
                    "_runId": country.get("runId"),
                }
                continue
            reason = str(
                source.get("failureReason")
                or country.get("topFailureReason")
                or source.get("status")
                or "unknown"
            )
            failure_counts[reason] = failure_counts.get(reason, 0) + 1
            if len(repair_samples) < 8:
                repair_samples.append({
                    "countryCode": country_code,
                    "sourceCode": source_code,
                    "failureReason": reason,
                    "recommendedStrategy": source.get("recommendedStrategy"),
                    "runId": country.get("runId"),
                })

    active_sources: dict[tuple[str, str], dict[str, Any]] = {}
    for country in current_report.get("countriesDetail") or []:
        country_code = str(country.get("countryCode") or "").lower()
        total = _int_value(country.get("total"))
        for index, source in enumerate(country.get("sources") or [], start=1):
            normalized = _normalize_source_for_progress(source, index, total)
            source_code = str(normalized.get("sourceCode") or "")
            if country_code and source_code:
                active_sources[(country_code, source_code)] = normalized

    probe_regressions: list[dict[str, Any]] = []
    for (country_code, source_code), stable_source in stable_good_sources.items():
        active_source = active_sources.get((country_code, source_code))
        if not active_source or active_source.get("status") == "pass":
            continue
        probe_regressions.append({
            "countryCode": country_code,
            "sourceCode": source_code,
            "activeStatus": active_source.get("status"),
            "failureReason": active_source.get("failureReason"),
            "recommendedStrategy": active_source.get("recommendedStrategy"),
            "stableRunId": stable_source.get("_runId") or latest_run_id,
            "activeRunId": active_run_id,
            "lastKnownValid": stable_source.get("valid"),
        })

    source_count = source_rows_observed or total_sources
    source_pass_count = len(stable_good_sources) if source_rows_observed else total_pass
    return {
        "gateThreshold": gate_threshold,
        "countryCount": len(all_countries),
        "readyCountryCount": len(ready_countries),
        "blockedCountryCount": max(0, len(all_countries) - len(ready_countries)),
        "stablePassRate": (
            round(total_pass / max(total_sources, 1) * 100, 1)
            if total_sources > 0 else 0
        ),
        "totalSources": total_sources,
        "totalPass": total_pass,
        "financeObservationCandidates": finance["financeObservationCandidates"],
        "financeMonthlyPaymentCount": finance["financeMonthlyPaymentCount"],
        "financeSemanticsCounts": finance["financeSemanticsCounts"],
        "financeTypeCounts": finance["financeTypeCounts"],
        "sourceRowsObserved": source_rows_observed,
        "sourceCount": source_count,
        "readySourceCount": source_pass_count,
        "blockedSourceCount": max(0, source_count - source_pass_count),
        "sourcePassRate": (
            round(source_pass_count / max(source_count, 1) * 100, 1)
            if source_count > 0 else 0
        ),
        "topFailureReasons": [
            {"reason": reason, "count": count}
            for reason, count in sorted(
                failure_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[:8]
        ],
        "repairSourceSamples": repair_samples,
        "probeRegressionCount": len(probe_regressions),
        "probeRegressionSamples": probe_regressions[:8],
        "latestRunId": latest_run_id,
        "activeRunId": active_run_id,
        "activeRunRunning": False,
        "activeRunPartial": False,
        "activeRunPassRate": float(current_summary.get("passPct") or 0),
        "probeDiffersFromStableRun": bool(
            active_run_id and latest_run_id and active_run_id != latest_run_id
        ),
        "readyCountries": [
            str(country.get("countryCode") or "")
            for country in ready_countries
        ],
        "blockedCountries": [
            str(country.get("countryCode") or "")
            for country in all_countries
            if float(country.get("passPct") or 0) < gate_threshold
        ],
    }


def _strip_sources(country: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in country.items() if key != "sources"}


def _historical_good_sources(current_run_id: str | None) -> dict[tuple[str, str], dict[str, Any]]:
    if not RUNS_INDEX_PATH.is_file():
        return {}
    try:
        index_data = json.loads(RUNS_INDEX_PATH.read_text())
    except Exception:
        return {}
    current = str(current_run_id or "")
    good_sources: dict[tuple[str, str], dict[str, Any]] = {}
    for run in sorted(
        index_data.get("runs") or [],
        key=_run_recency_key,
        reverse=True,
    ):
        run_id = str(run.get("runId") or "")
        if not run_id or run_id == current:
            continue
        report = _load_v3_report(_artifact_path_from_ref(run.get("artifactPath")))
        if not report:
            report = _load_v3_report(
                REPO_ROOT
                / "03_Scripts"
                / "diagnostics"
                / "artifacts"
                / f"dryrun_report_{run_id}.json"
            )
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


def _source_repair_backlog_from_report(report: dict[str, Any], now: str) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    top_hosts: dict[str, dict[str, Any]] = {}
    last_known_good = _historical_good_sources(str(report.get("runId") or ""))
    for country in report.get("countriesDetail") or []:
        country_code = str(country.get("countryCode") or "").lower()
        for source in country.get("sources") or []:
            reason = source.get("failureReason")
            if not reason:
                continue
            reason = str(reason)
            source_code = str(source.get("sourceCode") or source.get("code") or "")
            recommended = str(source.get("recommendedStrategy") or "diagnose_with_msrp_page_analyzer")
            key = _source_key(country_code, source)
            last_good = last_known_good.get(key) if key else None
            is_transient = bool(last_good) or reason in TRANSIENT_RECHECK_FAILURES
            group = groups.setdefault(
                reason,
                {
                    "failureReason": reason,
                    "count": 0,
                    "transientRegressionCount": 0,
                    "sourceRepairIssueCount": 0,
                    "externalAccessIssueCount": 0,
                    "pipelineIssueCount": 0,
                    "recommendedStrategies": {},
                    "affectedCountries": set(),
                    "sources": [],
                    "sourceDetails": [],
                    "transientSources": [],
                    "externalAccessSources": [],
                    "pipelineSources": [],
                    "hosts": {},
                    "status": "new",
                },
            )
            group["count"] += 1
            is_external_access = _is_external_access_issue(source, reason) and not is_transient
            is_pipeline_issue = (
                _is_pipeline_issue(source, reason)
                and not is_transient
                and not is_external_access
            )
            source_detail = _source_issue_detail_from_report_source(
                source=source,
                country_code=country_code,
                source_code=source_code,
                failure_reason=reason,
                recommended_strategy=recommended,
                last_good=last_good,
                transient_recheck=is_transient,
                external_access=is_external_access,
                pipeline_issue=is_pipeline_issue,
            )
            if is_transient:
                group["transientRegressionCount"] += 1
                group["transientSources"].append(source_detail)
            elif is_external_access:
                group["externalAccessIssueCount"] += 1
                group["externalAccessSources"].append(source_detail)
            elif is_pipeline_issue:
                group["pipelineIssueCount"] += 1
                group["pipelineSources"].append(source_detail)
            else:
                group["sourceRepairIssueCount"] += 1
                group["sourceDetails"].append(source_detail)
            group["recommendedStrategies"][recommended] = group["recommendedStrategies"].get(recommended, 0) + 1
            if country_code:
                group["affectedCountries"].add(country_code)
            if source_code:
                group["sources"].append(source_code)
            host = _source_host(source)
            url = _source_url(source)
            if host:
                empty_host_bucket = {
                    "count": 0,
                    "affectedCountries": set(),
                    "sources": [],
                    "urls": [],
                }
                for host_bucket in (
                    group["hosts"].setdefault(host, dict(empty_host_bucket)),
                    top_hosts.setdefault(host, dict(empty_host_bucket)),
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
            "sourceRepairIssues": group["sourceDetails"][:20],
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
    source_issues = [
        source
        for item in normalized_groups
        for source in item.get("sourceRepairIssues") or []
    ]
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
    transient_regressions = [
        source
        for item in normalized_groups
        for source in item.get("sampleTransientRegressions") or []
    ]
    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": report.get("runId"),
        "generatedAt": now,
        "partial": False,
        "totalIssueCount": sum(int(item["count"]) for item in normalized_groups),
        "transientRegressionCount": transient_regression_count,
        "sourceRepairIssueCount": source_repair_issue_count,
        "externalAccessIssueCount": external_access_issue_count,
        "pipelineIssueCount": pipeline_issue_count,
        "sourceIssues": source_issues,
        "externalAccessIssues": external_access_issues,
        "pipelineIssues": pipeline_issues,
        "transientSourceRegressions": transient_regressions,
        "topSourceHosts": _normalize_host_groups(top_hosts),
        "groups": normalized_groups,
    }


def _severity(pass_pct: float, gate_threshold: int, is_missing: bool) -> str:
    if is_missing:
        return "critical"
    if pass_pct < gate_threshold:
        return "critical"
    if pass_pct < 50:
        return "critical"
    if pass_pct < 90:
        return "warning"
    return "ok"


def run(out_dir: str | None = None) -> dict:
    report = _load_dryrun_report()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not report:
        result = {
            "probe": "pipeline.msrp_country_progress",
            "overall": "critical",
            "generatedAt": now,
            "status": {},
            "countries": [],
            "topBlockingCountries": [],
            "topFailureReasons": [],
            "sourceRepairBacklog": _load_source_repair_backlog(),
            "sourceReferenceEvidence": _load_source_reference_evidence(),
            "sourceAccessibilityAudit": _load_source_accessibility_audit(),
            "findings": [{
                "type": "no_dryrun_report",
                "severity": "critical",
                "message": "No dryrun report found. MSRP dryrun may not have run yet.",
            }],
        }
        _write_outputs(result, out_dir)
        return result

    summary = report.get("summary", {})
    pass_pct = summary.get("passPct", 0.0)
    gate_threshold = summary.get("gateThreshold", 70)
    gate_status = summary.get("gateStatus", "blocked")
    countries_detail = report.get("countriesDetail") or []

    # Per-country status
    country_entries: list[dict] = []
    top_blocking: list[dict] = []
    for c in countries_detail:
        cc = c.get("countryCode", "?")
        c_pct = c.get("passPct", 0.0)
        c_status = c.get("status", "unknown")
        fb = c.get("failureBreakdown", {}) or {}
        sr = c.get("strategyRecommendations", {}) or {}
        top_reason = max(fb, key=fb.get) if fb else None

        entry = {
            "countryCode": cc,
            "total": c.get("total", 0),
            "pass": c.get("pass", 0),
            "empty": c.get("empty", 0),
            "fail": c.get("fail", 0),
            "errors": c.get("errors", 0),
            "passPct": c_pct,
            "status": c_status,
            "topFailureReason": top_reason,
            "failureBreakdown": fb,
            "strategyRecommendations": sr,
            "financeObservationCandidates": _int_value(c.get("financeObservationCandidates")),
            "financeMonthlyPaymentCount": _int_value(c.get("financeMonthlyPaymentCount")),
            "financeSemanticsCounts": _count_map(c.get("financeSemanticsCounts")),
            "financeTypeCounts": _count_map(c.get("financeTypeCounts")),
        }
        country_entries.append(entry)

        if c_pct < gate_threshold or c_pct < 50:
            top_blocking.append({
                "countryCode": cc,
                "passPct": c_pct,
                "reason": top_reason or "unknown",
                "recommendedAction": f"Review {top_reason or 'failures'} for {cc}",
            })

    # Aggregate failure reasons
    all_failure_reasons: dict[str, int] = {}
    for c in country_entries:
        for reason, count in (c.get("failureBreakdown") or {}).items():
            all_failure_reasons[reason] = all_failure_reasons.get(reason, 0) + count
    top_reasons = sorted(all_failure_reasons.items(), key=lambda x: -x[1])

    # Findings
    findings: list[dict] = []
    missing = report.get("missingCountries") or []
    duplicates = report.get("duplicateCountries") or []

    for mc in missing:
        findings.append({
            "type": "missing_country",
            "severity": "critical",
            "message": f"Country '{mc}' is missing from the dryrun run.",
            "country": mc,
        })

    for dc in duplicates:
        findings.append({
            "type": "duplicate_country",
            "severity": "warning",
            "message": f"Country '{dc}' appears multiple times in the same run.",
            "country": dc,
        })

    for c in country_entries:
        if c.get("passPct", 100) < 50:
            findings.append({
                "type": "country_low_pass_rate",
                "severity": "critical",
                "message": f"Country '{c['countryCode']}' pass rate is {c['passPct']}% (<50%).",
                "country": c["countryCode"],
                "passPct": c["passPct"],
            })
        elif c.get("passPct", 100) < gate_threshold:
            findings.append({
                "type": "country_below_gate",
                "severity": "warning",
                "message": f"Country '{c['countryCode']}' pass rate is {c['passPct']}% (<{gate_threshold}% gate).",
                "country": c["countryCode"],
                "passPct": c["passPct"],
            })

    if gate_status == "blocked":
        findings.append({
            "type": "ingest_gate_blocked",
            "severity": "critical",
            "message": f"Ingest gate blocked: overall pass rate {pass_pct}% < {gate_threshold}% threshold.",
            "passPct": pass_pct,
            "gateThreshold": gate_threshold,
        })

    overall = "critical" if any(f["severity"] == "critical" for f in findings) else \
              "warning" if findings else "ok"

    source_repair_backlog = _source_repair_backlog_from_report(report, now)
    if not source_repair_backlog.get("groups"):
        source_repair_backlog = _load_source_repair_backlog()

    all_countries_full = _all_country_latest_from_runs_index()
    if not all_countries_full:
        run_meta = {
            "runId": report.get("runId"),
            "batch": report.get("batch") or "",
            "finishedAt": report.get("generatedAt") or now,
            "status": summary.get("status"),
            "gateStatus": gate_status,
            "gateThreshold": gate_threshold,
        }
        all_countries_full = [
            normalized
            for country in countries_detail
            if (
                normalized := _country_from_v3(
                    country,
                    run_meta=run_meta,
                    latest_run_id=str(report.get("runId") or ""),
                )
            )
        ]
    stable_coverage = _stable_coverage_summary(all_countries_full, report)

    result = {
        "probe": "pipeline.msrp_country_progress",
        "overall": overall,
        "generatedAt": now,
        "status": {
            "runId": report.get("runId"),
            "schemaVersion": report.get("schemaVersion"),
            "overallPassPct": pass_pct,
            "gateThreshold": gate_threshold,
            "gateStatus": gate_status,
            "expectedCountries": report.get("expectedCountries", []),
            "observedCountries": report.get("observedCountries", []),
            "missingCountries": missing,
            "duplicateCountries": duplicates,
            "financeObservationCandidates": _int_value(summary.get("financeObservationCandidates")),
            "financeMonthlyPaymentCount": _int_value(summary.get("financeMonthlyPaymentCount")),
            "financeSemanticsCounts": _count_map(summary.get("financeSemanticsCounts")),
            "financeTypeCounts": _count_map(summary.get("financeTypeCounts")),
            "stableLatestRunId": stable_coverage.get("latestRunId"),
            "activeRunId": stable_coverage.get("activeRunId"),
        },
        "countries": country_entries,
        "topBlockingCountries": sorted(top_blocking, key=lambda x: x["passPct"]),
        "topFailureReasons": [{"reason": r, "count": c} for r, c in top_reasons[:5]],
        "sourceRepairBacklog": source_repair_backlog,
        "sourceReferenceEvidence": _load_source_reference_evidence(str(report.get("runId") or "")),
        "sourceAccessibilityAudit": _load_source_accessibility_audit(str(report.get("runId") or "")),
        "allCountriesLatest": [_strip_sources(country) for country in all_countries_full],
        "stableCoverage": stable_coverage,
        "findings": findings,
    }

    _write_outputs(result, out_dir)
    return result


def _write_outputs(result: dict, out_dir: str | None = None) -> None:
    out_base = Path(out_dir).resolve() if out_dir else REPO_ROOT / "hermes" / "reports"
    out_base.mkdir(parents=True, exist_ok=True)

    json_path = out_base / "msrp_country_progress.json"
    json_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"[country-progress] JSON: {json_path}")

    md_path = out_base / "msrp_country_progress.md"
    md_path.write_text(_render_markdown(result))
    print(f"[country-progress] Markdown: {md_path}")

    # Write historical copy if runId is available
    run_id = (result.get("status") or {}).get("runId")
    if run_id:
        hist_json = out_base / f"msrp_country_progress_{run_id}.json"
        hist_json.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
        hist_md = out_base / f"msrp_country_progress_{run_id}.md"
        hist_md.write_text(_render_markdown(result))
        print(f"[country-progress] Historical: {hist_json}")

    s = result.get("status", {})
    print(f"[country-progress] overall={result['overall']}, "
          f"passPct={s.get('overallPassPct', '?')}%, "
          f"gate={s.get('gateStatus', '?')}, "
          f"findings={len(result['findings'])}")


def _render_markdown(result: dict) -> str:
    lines: list[str] = []
    lines.append("# MSRP Country Progress\n")
    lines.append(f"**Generated:** {result['generatedAt']}\n")
    lines.append(f"**Overall:** {result['overall']}\n")

    status = result.get("status", {})
    lines.append("## Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Run ID | {status.get('runId', '-')} |")
    lines.append(f"| Overall passPct | {status.get('overallPassPct', '?')}% |")
    lines.append(f"| Gate threshold | {status.get('gateThreshold', '?')}% |")
    lines.append(f"| Gate status | {status.get('gateStatus', '?')} |")
    lines.append(f"| Latest stable run | {status.get('stableLatestRunId', '-')} |")
    lines.append(f"| Active run | {status.get('activeRunId', '-')} |")
    lines.append(f"| Expected countries | {len(status.get('expectedCountries', []))} |")
    lines.append(f"| Observed countries | {len(status.get('observedCountries', []))} |")
    lines.append(f"| Missing countries | {len(status.get('missingCountries', []))} |")
    lines.append(f"| Duplicate countries | {len(status.get('duplicateCountries', []))} |")
    lines.append(f"| Finance candidates | {status.get('financeObservationCandidates', 0)} |")
    lines.append(f"| Monthly offers | {status.get('financeMonthlyPaymentCount', 0)} |")
    lines.append("")

    stable_coverage = result.get("stableCoverage") or {}
    if stable_coverage.get("countryCount"):
        lines.append("## Stable Coverage\n")
        lines.append("| Metric | Value |")
        lines.append("|---|---:|")
        lines.append(
            f"| Ready countries | {stable_coverage.get('readyCountryCount', 0)}"
            f"/{stable_coverage.get('countryCount', 0)} |"
        )
        lines.append(f"| Stable pass rate | {stable_coverage.get('stablePassRate', 0)}% |")
        lines.append(
            f"| Ready sources | {stable_coverage.get('readySourceCount', 0)}"
            f"/{stable_coverage.get('sourceCount', 0)} |"
        )
        lines.append(f"| Source pass rate | {stable_coverage.get('sourcePassRate', 0)}% |")
        lines.append(f"| Probe regressions | {stable_coverage.get('probeRegressionCount', 0)} |")
        lines.append("")

    countries = result.get("countries", [])
    if countries:
        lines.append("## Country Progress\n")
        lines.append("| Country | Status | PassPct | Pass | Empty | Fail | Finance | Monthly | Top Failure |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")
        for c in sorted(countries, key=lambda x: x["passPct"]):
            lines.append(f"| {c['countryCode']} | {c['status']} | "
                        f"{c['passPct']}% | {c['pass']} | {c['empty']} | "
                        f"{c['fail']} | {c.get('financeObservationCandidates', 0)} | "
                        f"{c.get('financeMonthlyPaymentCount', 0)} | "
                        f"{c.get('topFailureReason', '-')} |")
        lines.append("")

    blocking = result.get("topBlockingCountries", [])
    if blocking:
        lines.append("## Blocking Countries\n")
        lines.append("| Country | Reason | Recommended Action |")
        lines.append("|---|---|---|")
        for b in blocking:
            lines.append(f"| {b['countryCode']} | {b.get('reason', '?')} "
                        f"| {b.get('recommendedAction', 'Review')} |")
        lines.append("")

    backlog = result.get("sourceRepairBacklog") or {}
    groups = backlog.get("groups") or []
    if groups:
        lines.append("## Source Repair Backlog\n")
        lines.append("| Failure Reason | Count | Recommended Strategy | Affected Countries |")
        lines.append("|---|---:|---|---|")
        for group in groups[:10]:
            countries = ", ".join(str(c).upper() for c in group.get("affectedCountries", []))
            lines.append(
                f"| {group.get('failureReason', '-')} | {group.get('count', 0)} | "
                f"{group.get('recommendedStrategy', '-')} | {countries or '-'} |"
            )
        lines.append("")

    reference_evidence = result.get("sourceReferenceEvidence") or {}
    reference_summary = reference_evidence.get("summary") or {}
    if reference_summary.get("evidenceItemCount") or reference_summary.get("localReferenceCount"):
        lines.append("## Source Reference Evidence\n")
        lines.append("| Metric | Value |")
        lines.append("|---|---:|")
        lines.append(f"| Evidence items | {reference_summary.get('evidenceItemCount', 0)} |")
        lines.append(f"| Local references | {reference_summary.get('localReferenceCount', 0)} |")
        lines.append(f"| Missing local references | {reference_summary.get('missingLocalReferenceCount', 0)} |")
        lines.append(f"| Official ingest eligible | {reference_summary.get('officialIngestEligibleCount', 0)} |")
        lines.append("")

    accessibility_audit = result.get("sourceAccessibilityAudit") or {}
    accessibility_summary = accessibility_audit.get("summary") or {}
    if accessibility_summary.get("probedSourceCount"):
        lines.append("## Source Accessibility Audit\n")
        lines.append("| Metric | Value |")
        lines.append("|---|---:|")
        lines.append(f"| Probed sources | {accessibility_summary.get('probedSourceCount', 0)} |")
        lines.append(f"| Retryable network | {accessibility_summary.get('retryableNetworkCount', 0)} |")
        lines.append(f"| Official proxy required | {accessibility_summary.get('officialProxyRequiredCount', 0)} |")
        lines.append(f"| TLS handshake failed | {accessibility_summary.get('tlsHandshakeFailedCount', 0)} |")
        lines.append(f"| DNS unresolved | {accessibility_summary.get('dnsUnresolvedCount', 0)} |")
        lines.append("")

    findings = result.get("findings", [])
    if findings:
        lines.append("## Findings\n")
        for f in findings:
            lines.append(f"- **[{f['severity']}]** {f['message']}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes MSRP Country Progress Probe")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()
    run(args.out_dir)


if __name__ == "__main__":
    main()
