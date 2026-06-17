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
SOURCE_URL_PATTERN = re.compile(r"https?://[^\s\"')<>]+")


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
        "topSourceHosts": [],
        "groups": [],
    }


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


def _priority_band(score: float, source_repair_count: int, transient_count: int) -> str:
    if source_repair_count <= 0 and transient_count > 0:
        return "recheck"
    if score >= 80 or source_repair_count >= 10:
        return "critical"
    if score >= 45 or source_repair_count >= 3:
        return "high"
    if score >= 15:
        return "medium"
    return "low"


def _priority_review_assist(failure_reason: str, source_repair_count: int) -> dict[str, str]:
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
    affected_country_count = len(group["affectedCountries"])
    host_counts = [int(data.get("count") or 0) for data in group["hosts"].values()]
    top_host_count = max(host_counts or [0])
    score = (
        source_repair_count * _priority_weight("SOURCE_REPAIR", 10.0)
        + affected_country_count * _priority_weight("COUNTRY", 6.0)
        + top_host_count * _priority_weight("HOST_CLUSTER", 3.0)
        + transient_count * _priority_weight("TRANSIENT_RECHECK", 1.5)
    )
    return {
        "priorityScore": round(score, 1),
        "priorityBand": _priority_band(score, source_repair_count, transient_count),
        "priorityWeights": {
            "sourceRepair": _priority_weight("SOURCE_REPAIR", 10.0),
            "country": _priority_weight("COUNTRY", 6.0),
            "hostCluster": _priority_weight("HOST_CLUSTER", 3.0),
            "transientRecheck": _priority_weight("TRANSIENT_RECHECK", 1.5),
        },
        "reviewAssist": _priority_review_assist(str(group["failureReason"]), source_repair_count),
    }


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


def _historical_good_sources(current_run_id: str | None) -> dict[tuple[str, str], dict[str, Any]]:
    if not RUNS_INDEX_PATH.is_file():
        return {}
    try:
        index_data = json.loads(RUNS_INDEX_PATH.read_text())
    except Exception:
        return {}
    current = str(current_run_id or "")
    good_sources: dict[tuple[str, str], dict[str, Any]] = {}
    for run in index_data.get("runs") or []:
        run_id = str(run.get("runId") or "")
        if not run_id or run_id == current:
            continue
        report = _load_v3_report(_artifact_path_from_ref(run.get("artifactPath")))
        if not report:
            report = _load_v3_report(REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / f"dryrun_report_{run_id}.json")
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
            is_transient = bool(last_good)
            group = groups.setdefault(reason, {
                "failureReason": reason,
                "count": 0,
                "transientRegressionCount": 0,
                "sourceRepairIssueCount": 0,
                "recommendedStrategies": {},
                "affectedCountries": set(),
                "sources": [],
                "transientSources": [],
                "hosts": {},
                "status": "new",
            })
            group["count"] += 1
            if is_transient:
                group["transientRegressionCount"] += 1
                group["transientSources"].append({
                    "countryCode": country_code,
                    "sourceCode": source_code,
                    "failureReason": reason,
                    "recommendedStrategy": recommended,
                    "lastKnownGoodRunId": last_good.get("runId"),
                    "lastKnownGoodAt": last_good.get("observedAt"),
                    "recommendedAction": "recheck_before_source_repair",
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
        normalized_groups.append({
            "failureReason": group["failureReason"],
            "count": group["count"],
            "transientRegressionCount": transient_count,
            "sourceRepairIssueCount": source_repair_count,
            **_priority_fields(group),
            "recommendedAction": (
                "recheck_before_source_repair"
                if transient_count and not source_repair_count
                else "repair_source_definition"
            ),
            "recommendedStrategy": recommended_strategy,
            "recommendedStrategies": strategies,
            "affectedCountries": sorted(group["affectedCountries"]),
            "affectedCountryCount": len(group["affectedCountries"]),
            "sampleSources": group["sources"][:20],
            "sampleTransientRegressions": group["transientSources"][:8],
            "topSourceHosts": _normalize_host_groups(group["hosts"]),
            "status": group["status"],
        })
    normalized_groups.sort(key=lambda item: (
        0 if int(item["sourceRepairIssueCount"]) > 0 else 1,
        -float(item["priorityScore"]),
        -int(item["count"]),
        str(item["failureReason"]),
    ))
    transient_regression_count = sum(int(item["transientRegressionCount"]) for item in normalized_groups)
    source_repair_issue_count = sum(int(item["sourceRepairIssueCount"]) for item in normalized_groups)
    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": report.get("runId"),
        "generatedAt": now,
        "partial": False,
        "totalIssueCount": sum(int(item["count"]) for item in normalized_groups),
        "transientRegressionCount": transient_regression_count,
        "sourceRepairIssueCount": source_repair_issue_count,
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
        },
        "countries": country_entries,
        "topBlockingCountries": sorted(top_blocking, key=lambda x: x["passPct"]),
        "topFailureReasons": [{"reason": r, "count": c} for r, c in top_reasons[:5]],
        "sourceRepairBacklog": source_repair_backlog,
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
    lines.append(f"| Expected countries | {len(status.get('expectedCountries', []))} |")
    lines.append(f"| Observed countries | {len(status.get('observedCountries', []))} |")
    lines.append(f"| Missing countries | {len(status.get('missingCountries', []))} |")
    lines.append(f"| Duplicate countries | {len(status.get('duplicateCountries', []))} |")
    lines.append(f"| Finance candidates | {status.get('financeObservationCandidates', 0)} |")
    lines.append(f"| Monthly offers | {status.get('financeMonthlyPaymentCount', 0)} |")
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
