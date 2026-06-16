#!/usr/bin/env python3
"""MSRP Dryrun Aggregator — Phase 4.

Reads per-country dryrun artifacts from a run directory and produces
a v3 aggregated dryrun report with pipeline/country/source layers.

Usage:
  python 03_Scripts/msrp_dryrun_aggregate.py \
    --run-dir 03_Scripts/logs/msrp-dryrun-20260521-033000 \
    --expected-countries "se fi no dk" \
    --out-latest 03_Scripts/diagnostics/artifacts/dryrun_report.json
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

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_URL_PATTERN = re.compile(r"https?://[^\s\"')<>]+")


def _load_country_artifact(path: Path) -> dict | None:
    if path.is_file():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return None


def _discover_country_artifacts(run_dir: Path) -> tuple[dict[str, dict], list[str]]:
    """Load all country artifacts from a run directory.

    The canonical country comes from the artifact body when present so a bad
    filename cannot silently create a second country in the aggregate report.
    """
    artifacts_dir = run_dir / "countries"
    if not artifacts_dir.is_dir():
        return {}, []
    artifacts: dict[str, dict] = {}
    duplicates: list[str] = []
    for p in sorted(artifacts_dir.glob("*.json")):
        data = _load_country_artifact(p)
        if not data:
            continue
        cc = str(data.get("country") or p.stem).strip().lower()
        if not cc:
            continue
        if cc in artifacts:
            duplicates.append(cc)
            continue
        artifacts[cc] = data
    return artifacts, sorted(set(duplicates))


def _gather_source_results(artifacts: dict[str, dict]) -> list[dict]:
    results: list[dict] = []
    seen: set[str] = set()
    for cc, data in artifacts.items():
        for r in (data.get("results") or []):
            key = r.get("code") or r.get("sourceCode") or f"{cc}_{len(results)}"
            if key in seen:
                continue
            seen.add(key)
            results.append(r)
    return results


def _status_for_pass_pct(pass_pct: float) -> str:
    if pass_pct >= 90:
        return "success"
    if pass_pct >= 50:
        return "degraded"
    return "failure"


def _gate_status(pass_pct: float, threshold: int) -> str:
    return "allowed" if pass_pct >= threshold else "blocked"


def _status_value(result: dict[str, Any]) -> str:
    return str(result.get("rawStatus") or result.get("status") or "").lower()


def _valid_count(result: dict[str, Any]) -> int:
    try:
        return int(result.get("valid") or 0)
    except (TypeError, ValueError):
        return 0


def _result_is_pass(result: dict[str, Any]) -> bool:
    status = _status_value(result)
    return _valid_count(result) > 0 and not result.get("failureReason") and status not in {"empty", "error", "exception"}


def _result_is_empty(result: dict[str, Any]) -> bool:
    return _status_value(result) == "empty"


def _result_is_error(result: dict[str, Any]) -> bool:
    return _status_value(result) in {"error", "exception"}


def _result_is_fail(result: dict[str, Any]) -> bool:
    return not _result_is_pass(result) and not _result_is_empty(result) and not _result_is_error(result)


def _normalize_source_result(result: dict[str, Any], country_code: str) -> dict[str, Any]:
    status = str(result.get("status") or "")
    normalized_status = "pass" if _result_is_pass(result) else ("empty" if _result_is_empty(result) else "fail")
    source_code = result.get("sourceCode") or result.get("code") or ""
    payload = {
        "index": int(result.get("index") or 0),
        "totalInCountry": int(result.get("totalInCountry") or 0),
        "country": result.get("country") or country_code,
        "sourceCode": source_code,
        "code": source_code,
        "status": normalized_status,
        "rawStatus": status,
        "valid": int(result.get("valid") or 0),
        "extracted": int(result.get("extracted") or 0),
        "rejected": int(result.get("rejected") or 0),
        "elapsedSeconds": float(result.get("elapsedSeconds") or result.get("elapsed") or 0),
        "failureReason": result.get("failureReason"),
        "recommendedStrategy": result.get("recommendedStrategy"),
        "severity": result.get("severity"),
        "error": result.get("error"),
    }
    for key in (
        "extractorError",
        "sourceUrl",
        "httpStatus",
        "finalUrl",
        "extractorName",
        "extractorVersion",
        "coverageLevel",
        "auditStatus",
        "attemptedStrategies",
        "winningStrategy",
    ):
        value = result.get(key)
        if value not in (None, ""):
            payload[key] = value
    return payload


def _artifact_count(data: dict[str, Any], key: str, fallback: int) -> int:
    value = data.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return fallback


def _compute_summary(artifacts: dict[str, dict], results: list[dict]) -> dict:
    if artifacts:
        total = 0
        pass_count = 0
        empty = 0
        fail = 0
        errors = 0
        for data in artifacts.values():
            d_results = data.get("results") or []
            total += _artifact_count(data, "total", len(d_results))
            if d_results:
                pass_count += sum(1 for r in d_results if _result_is_pass(r))
                empty += sum(1 for r in d_results if _result_is_empty(r))
                fail += sum(1 for r in d_results if _result_is_fail(r))
                errors += sum(1 for r in d_results if _result_is_error(r))
            else:
                pass_count += _artifact_count(data, "pass", 0)
                empty += _artifact_count(data, "empty", 0)
                fail += _artifact_count(data, "fail", 0)
                errors += _artifact_count(data, "errors", 0)
    else:
        total = len(results)
        pass_count = sum(1 for r in results if _result_is_pass(r))
        empty = sum(1 for r in results if _result_is_empty(r))
        fail = sum(1 for r in results if _result_is_fail(r))
        errors = sum(1 for r in results if _result_is_error(r))

    pass_pct = round(pass_count / total * 100, 1) if total > 0 else 0.0

    failure_breakdown: dict[str, int] = {}
    strategy_recs: dict[str, int] = {}
    for r in results:
        reason = r.get("failureReason")
        if reason:
            failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
        strat = r.get("recommendedStrategy")
        if strat:
            strategy_recs[strat] = strategy_recs.get(strat, 0) + 1

    return {
        "total": total,
        "pass": pass_count,
        "empty": empty,
        "fail": fail,
        "errors": errors,
        "passPct": pass_pct,
        "status": _status_for_pass_pct(pass_pct),
        "failureBreakdown": failure_breakdown,
        "strategyRecommendations": strategy_recs,
    }


def _build_countries_detail(
    artifacts: dict[str, dict],
    expected: list[str],
) -> tuple[list[dict], list[str]]:
    observed_codes = set(artifacts.keys())
    expected_set = set(expected)
    missing = sorted(expected_set - observed_codes)

    detail: list[dict] = []
    for cc in sorted(set(expected) | observed_codes):
        data = artifacts.get(cc)
        if not data:
            detail.append({
                "countryCode": cc,
                "total": 0, "pass": 0, "empty": 0, "fail": 0, "errors": 0,
                "passPct": 0.0, "status": "missing",
                "failureBreakdown": {}, "strategyRecommendations": {},
                "sources": [],
                "completed": False,
            })
            continue
        d_results = data.get("results") or []
        total = _artifact_count(data, "total", len(d_results))
        if d_results:
            d_pass = sum(1 for r in d_results if _result_is_pass(r))
            d_empty = sum(1 for r in d_results if _result_is_empty(r))
            d_fail = sum(1 for r in d_results if _result_is_fail(r))
            d_errors = sum(1 for r in d_results if _result_is_error(r))
        else:
            d_pass = _artifact_count(data, "pass", 0)
            d_empty = _artifact_count(data, "empty", 0)
            d_fail = _artifact_count(data, "fail", 0)
            d_errors = _artifact_count(data, "errors", 0)
        d_pct = round(d_pass / total * 100, 1) if total > 0 else 0.0
        d_status = _status_for_pass_pct(d_pct)

        d_fb: dict[str, int] = {}
        d_sr: dict[str, int] = {}
        for r in d_results:
            reason = r.get("failureReason")
            if reason:
                d_fb[reason] = d_fb.get(reason, 0) + 1
            strat = r.get("recommendedStrategy")
            if strat:
                d_sr[strat] = d_sr.get(strat, 0) + 1

        top_reason = max(d_fb, key=d_fb.get) if d_fb else None
        sources = [
            {
                **_normalize_source_result(result, cc),
                "index": idx,
                "totalInCountry": total,
            }
            for idx, result in enumerate(d_results, start=1)
        ]

        detail.append({
            "countryCode": cc,
            "total": total,
            "pass": d_pass,
            "empty": d_empty,
            "fail": d_fail,
            "errors": d_errors,
            "passPct": d_pct,
            "status": d_status,
            "topFailureReason": top_reason,
            "failureBreakdown": d_fb,
            "strategyRecommendations": d_sr,
            "sources": sources,
            "completed": True,
        })

    return detail, missing


def _parse_run_started_at(run_id: str) -> str:
    prefix = "msrp-dryrun-"
    if run_id.startswith(prefix):
        raw = run_id.removeprefix(prefix)
        try:
            return datetime.strptime(raw, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


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
    except (json.JSONDecodeError, OSError):
        return None
    return data if data.get("schemaVersion") == "msrp_dryrun_report_v3" else None


def _historical_good_sources(out_dir: Path, current_run_id: str | None) -> dict[tuple[str, str], dict[str, Any]]:
    index_path = out_dir / "dryrun_runs_index.json"
    if not index_path.is_file():
        return {}
    try:
        index_data = json.loads(index_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}

    good_sources: dict[tuple[str, str], dict[str, Any]] = {}
    current = str(current_run_id or "")
    for run in index_data.get("runs") or []:
        run_id = str(run.get("runId") or "")
        if not run_id or run_id == current:
            continue
        report = _load_v3_report(_artifact_path_from_ref(run.get("artifactPath")))
        if not report:
            fallback = out_dir / f"dryrun_report_{run_id}.json"
            report = _load_v3_report(fallback)
        if not report:
            continue
        observed_at = str(run.get("finishedAt") or report.get("generatedAt") or "")
        for country in report.get("countriesDetail") or []:
            country_code = str(country.get("countryCode") or "").strip().lower()
            for source in country.get("sources") or []:
                key = _source_key(country_code, source)
                if not key or key in good_sources or not _result_is_pass(source):
                    continue
                good_sources[key] = {
                    "runId": run_id,
                    "observedAt": observed_at,
                    "valid": source.get("valid"),
                }
    return good_sources


def _write_source_repair_backlog(report: dict[str, Any], out_dir: Path) -> None:
    groups: dict[str, dict[str, Any]] = {}
    top_hosts: dict[str, dict[str, Any]] = {}
    last_known_good = _historical_good_sources(out_dir, str(report.get("runId") or ""))
    for result in report.get("results") or []:
        reason = result.get("failureReason")
        if not reason:
            continue
        country = str(result.get("country") or "").lower()
        source_code = result.get("sourceCode") or result.get("code") or ""
        recommended = result.get("recommendedStrategy") or "diagnose_with_msrp_page_analyzer"
        key = _source_key(country, result)
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
                "countryCode": country,
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
        if country:
            group["affectedCountries"].add(country)
        if source_code:
            group["sources"].append(source_code)
        host = _source_host(result)
        url = _source_url(result)
        if host:
            for host_bucket in (
                group["hosts"].setdefault(host, {"count": 0, "affectedCountries": set(), "sources": [], "urls": []}),
                top_hosts.setdefault(host, {"count": 0, "affectedCountries": set(), "sources": [], "urls": []}),
            ):
                host_bucket["count"] += 1
                if country:
                    host_bucket["affectedCountries"].add(country)
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

    payload = {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": report.get("runId"),
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totalIssueCount": sum(int(item["count"]) for item in normalized_groups),
        "transientRegressionCount": transient_regression_count,
        "sourceRepairIssueCount": source_repair_issue_count,
        "topSourceHosts": _normalize_host_groups(top_hosts),
        "groups": normalized_groups,
    }
    json_path = out_dir / "msrp_source_repair_backlog.json"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

    lines = [
        "# MSRP Source Repair Backlog",
        "",
        f"Generated: {payload['generatedAt']}",
        f"Run ID: {payload.get('runId') or '-'}",
        f"Transient regressions: {payload['transientRegressionCount']}",
        f"Source repair issues: {payload['sourceRepairIssueCount']}",
        "",
        "| Failure reason | Priority | Count | Recheck | Source repair | Recommended strategy | Affected countries |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for item in normalized_groups:
        lines.append(
            "| {reason} | {priority} | {count} | {transient} | {source_repair} | {strategy} | {countries} |".format(
                reason=item["failureReason"],
                priority=f"{item['priorityBand']} {item['priorityScore']}",
                count=item["count"],
                transient=item["transientRegressionCount"],
                source_repair=item["sourceRepairIssueCount"],
                strategy=item["recommendedStrategy"],
                countries=", ".join(str(c).upper() for c in item["affectedCountries"]) or "-",
            )
        )
    lines.append("")
    md_path = out_dir / "msrp_source_repair_backlog.md"
    md_path.write_text("\n".join(lines))
    print(f"[aggregate] Source repair backlog: {json_path}")


def run(
    run_dir: str,
    expected_countries: list[str],
    out_latest: str | None = None,
) -> dict:
    run_dir_path = Path(run_dir).resolve()
    artifacts, artifact_duplicates = _discover_country_artifacts(run_dir_path)
    results = _gather_source_results(artifacts)
    summary = _compute_summary(artifacts, results)
    run_id = run_dir_path.name

    expected_countries_sorted = sorted({c.strip().lower() for c in expected_countries if c.strip()})
    expected_duplicates = sorted({
        c for c in expected_countries_sorted
        if [x.strip().lower() for x in expected_countries].count(c) > 1
    })
    countries_detail, missing = _build_countries_detail(
        artifacts, expected_countries_sorted,
    )
    duplicates = sorted(set(artifact_duplicates + expected_duplicates))
    gate_threshold = int(os.getenv("JATO_MSRP_MIN_DRYRUN_PASS_PCT", "70"))

    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": run_id,
        "batch": "batch_a",
        "expectedCountries": expected_countries_sorted,
        "observedCountries": sorted(artifacts.keys()),
        "missingCountries": missing,
        "duplicateCountries": duplicates,
        "summary": {
            **summary,
            "gateThreshold": gate_threshold,
            "gateStatus": _gate_status(summary["passPct"], gate_threshold),
        },
        "countriesDetail": countries_detail,
        "results": results,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if out_latest:
        out_path = Path(out_latest)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print(f"[aggregate] Latest report: {out_path}")

        # Write historical copy by runId (stable name, not timestamped)
        history_path = out_path.parent / f"dryrun_report_{run_id}.json"
        history_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print(f"[aggregate] Historical: {history_path}")
        _write_source_repair_backlog(report, out_path.parent)

        # Update dryrun_runs_index.json
        index_path = out_path.parent / "dryrun_runs_index.json"
        index_data: dict = {"schemaVersion": "msrp_dryrun_runs_index_v1", "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "latestRunId": run_id, "runs": []}
        if index_path.is_file():
            try:
                index_data = json.loads(index_path.read_text())
            except Exception:
                pass
        # Update or prepend this run
        existing_runs = index_data.get("runs", [])
        run_entry = {
            "runId": run_id,
            "mode": "dryrun",
            "batch": report.get("batch", "batch_a"),
            "startedAt": _parse_run_started_at(run_id),
            "finishedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": summary.get("status", "unknown"),
            "gateStatus": report["summary"]["gateStatus"],
            "gateThreshold": report["summary"]["gateThreshold"],
            "passPct": summary.get("passPct", 0.0),
            "total": summary.get("total", 0),
            "pass": summary.get("pass", 0),
            "empty": summary.get("empty", 0),
            "fail": summary.get("fail", 0),
            "errors": summary.get("errors", 0),
            "expectedCountryCount": len(expected_countries_sorted),
            "observedCountryCount": len(artifacts),
            "missingCountryCount": len(missing),
            "artifactPath": f"03_Scripts/diagnostics/artifacts/dryrun_report_{run_id}.json",
            "latestArtifactPath": _relative(out_path),
            "reportMdPath": f"hermes/reports/msrp_country_progress_{run_id}.md",
            "runDir": _relative(run_dir_path),
            "logFile": _relative(run_dir_path / "run.log"),
        }
        # Remove old entry with same runId if exists, then prepend
        existing_runs = [r for r in existing_runs if r.get("runId") != run_id]
        existing_runs.insert(0, run_entry)
        # Keep max 100 runs
        index_data["runs"] = existing_runs[:100]
        index_data["latestRunId"] = run_id
        index_data["updatedAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        index_path.write_text(json.dumps(index_data, indent=2, ensure_ascii=False) + "\n")
        print(f"[aggregate] Runs index updated: {index_path} ({len(index_data['runs'])} runs)")

    s = report["summary"]
    print(f"[aggregate] v3 report: {s['total']} sources, "
          f"passPct={s['passPct']}%, status={s['status']}, "
          f"gate={s['gateStatus']}, "
          f"countries={len(countries_detail)} "
          f"(missing={len(missing)}, dupes={len(duplicates)})")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="MSRP Dryrun Aggregator")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--expected-countries", required=True)
    parser.add_argument("--out-latest", default=None)
    args = parser.parse_args()
    expected = args.expected_countries.strip().split()
    run(args.run_dir, expected, args.out_latest)


if __name__ == "__main__":
    main()
