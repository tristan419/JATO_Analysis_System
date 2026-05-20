#!/usr/bin/env python3
"""Hermes Phase 5.1 — Source Quality Governor.

Score VOC / News / MSRP source health using registry data and runtime
artifacts. Deterministic — no LLM. Outputs structured reports.

Usage:
  python 03_Scripts/hermes/hermes_source_quality.py
  python 03_Scripts/hermes/hermes_source_quality.py --status-file path/to/status.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hermes_registry_loader import load_all_registries

REPO_ROOT = Path(__file__).resolve().parents[2]
DEGRADED_RUNTIME_STATUSES = {"degraded", "partial_success"}
FAILED_RUNTIME_STATUSES = {"failure"} | DEGRADED_RUNTIME_STATUSES


def _safe(v: Any, key: str, default: Any = None) -> Any:
    if isinstance(v, dict):
        return v.get(key, default)
    return default


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _load_status_file(path: str | None) -> dict[str, Any] | None:
    paths = [path] if path else []
    paths.append(str(REPO_ROOT / "03_Scripts" / "logs" / "scheduled_fetch_status.json"))
    for p in paths:
        if p and Path(p).is_file():
            try:
                return json.loads(open(p).read())
            except Exception:
                pass
    return None


def _status_key_for_source(source: dict) -> str | None:
    source_id = _safe(source, "sourceId", "")
    mapping = {
        "source.voc.batch_a": "voc",
        "source.news.batch_a": "news",
        "source.news.batch_b": "news_batch_b",
        "source.msrp.batch_a": "msrp_dryrun",
        "source.msrp.drafts_suv_top30": "msrp_dryrun",
        "source.msrp.production": "msrp_ingest",
    }
    return mapping.get(source_id)


def _runtime_status_for_source(source: dict, status_data: dict | None) -> dict[str, Any] | None:
    if not status_data:
        return None
    key = _status_key_for_source(source)
    if not key:
        return None
    entry = status_data.get(key)
    return entry if isinstance(entry, dict) else None


def _observed_with_runtime_status(source: dict, status_data: dict | None) -> dict[str, Any]:
    observed = dict(_safe(source, "lastObserved", {}) or {})
    runtime = _runtime_status_for_source(source, status_data)
    if not runtime:
        return observed

    if runtime.get("successCount") is not None:
        observed["successCount"] = _coerce_int(runtime.get("successCount"))

    failed = runtime.get("failureCount", runtime.get("failedCount"))
    if failed is not None:
        observed["failedCount"] = _coerce_int(failed)

    status = str(runtime.get("status") or "").strip().lower()
    last_run_at = runtime.get("lastRunAt")
    if last_run_at and status == "success":
        observed["lastSuccessAt"] = last_run_at
    elif last_run_at and status in FAILED_RUNTIME_STATUSES:
        observed["lastFailureAt"] = last_run_at
        observed["lastFailureReason"] = runtime.get("lastError") or f"runtime status={status}"

    return observed


def _compute_quality_score(source: dict, status_data: dict | None) -> dict:
    """Score a single source 0–100 using deterministic rules."""
    score = 100
    reasons: list[str] = []
    src_id = _safe(source, "sourceId", "?")
    src_type = _safe(source, "sourceType", "unknown")
    governance = _safe(source, "governanceStatus", "unmanaged")

    # Repeated failures
    last_obs = _observed_with_runtime_status(source, status_data)
    failed = _coerce_int(last_obs.get("failedCount"))
    if failed > 5:
        score -= 40
        reasons.append(f"repeated failures (failedCount={failed})")
    elif failed > 0:
        score -= 25
        reasons.append(f"has failures (failedCount={failed}), no structured reason")

    # Degraded status
    status = _safe(source, "status", "unknown")
    if status == "degraded":
        score -= 30
        reasons.append("status=degraded")
    elif status == "disabled":
        score -= 50
        reasons.append("status=disabled")

    runtime = _runtime_status_for_source(source, status_data)
    runtime_status = str(_safe(runtime, "status", "")).strip().lower()
    if runtime_status == "failure":
        score -= 35
        reasons.append("runtime status=failure")
    elif runtime_status in DEGRADED_RUNTIME_STATUSES:
        score -= 20
        reasons.append(f"runtime status={runtime_status}")

    # No recent success
    last_success = last_obs.get("lastSuccessAt")
    if not last_success:
        score -= 20
        reasons.append("no recent success recorded")

    # Governance
    if governance == "needs_review":
        score -= 15
        reasons.append("governanceStatus=needs_review")
    elif governance == "unmanaged":
        score -= 10
        reasons.append("governanceStatus=unmanaged")

    # Incomplete metadata
    quality = _safe(source, "quality", {}) or {}
    known_fields = ["successRate", "timeoutRate", "extractionQualityScore"]
    missing = sum(1 for k in known_fields if quality.get(k) is None)
    if missing >= 2:
        score -= 10
        reasons.append(f"incomplete metadata ({missing}/3 quality fields null)")

    # Known issues
    known_issues = _safe(source, "knownIssues", []) or []
    for issue in known_issues:
        if isinstance(issue, str):
            if any(kw in issue.lower() for kw in ["blocked", "403", "timeout", "unreachable", "unscheduled"]):
                score -= 10
                reasons.append(f"blocking issue: {issue[:60]}")

    score = max(0, score)

    if score >= 80:
        classification = "healthy"
    elif score >= 60:
        classification = "watch"
    elif score >= 30:
        classification = "degraded"
    else:
        classification = "disabled_candidate"

    return {
        "sourceId": src_id,
        "name": _safe(source, "name", src_id),
        "sourceType": src_type,
        "country": _safe(source, "country", "unknown"),
        "status": classification,
        "governanceStatus": governance,
        "qualityScore": score,
        "successCount": _coerce_int(last_obs.get("successCount")),
        "failedCount": failed,
        "lastSuccessAt": last_obs.get("lastSuccessAt"),
        "lastFailureAt": last_obs.get("lastFailureAt"),
        "lastFailureReason": last_obs.get("lastFailureReason"),
        "risk": "high" if score < 40 else ("medium" if score < 70 else "low"),
        "reasons": reasons,
        "recommendation": _generate_recommendation(score, reasons),
    }


def _generate_recommendation(score: int, reasons: list[str]) -> str:
    if score >= 80:
        return "Source is healthy. Continue monitoring."
    if score >= 60:
        return f"Watch. Issues: {'; '.join(reasons[:2])}"
    if score >= 30:
        return f"Consider degrading or reducing frequency. Issues: {'; '.join(reasons[:2])}"
    return f"Candidate for disable. Issues: {'; '.join(reasons[:2])}"


def _check_unstructured_failures(sources: list[dict]) -> list[dict]:
    """Detect sources with failures but no structured per-source error data."""
    findings: list[dict] = []
    for src in sources:
        last_obs = _safe(src, "lastObserved", {}) or {}
        failed = last_obs.get("failedCount", 0) or 0
        has_structured = last_obs.get("failedSources") is not None
        if failed > 0 and not has_structured:
            findings.append({
                "sourceId": _safe(src, "sourceId", "?"),
                "name": _safe(src, "name", "?"),
                "failedCount": failed,
                "finding": f"{failed} failures without per-source structured tracking (sourceId, url, error type, retryable)",
                "recommendation": "Add per-source failed_sources.json output to the crawler. Track source code, error type, and timestamp per failure.",
            })
    return findings


def _generate_report(scored: list[dict], fails: list[dict], source_findings: list[dict] | None = None) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    healthy = sum(1 for s in scored if s["status"] == "healthy")
    watch = sum(1 for s in scored if s["status"] == "watch")
    degraded = sum(1 for s in scored if s["status"] == "degraded")
    disabled = sum(1 for s in scored if s["status"] == "disabled_candidate")
    high = sum(1 for s in scored if s["risk"] == "high")

    lines: list[str] = []
    lines.append("# Hermes Source Quality Report\n")
    lines.append(f"**Generated:** {now}\n")

    lines.append("## 1. Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Total sources | {len(scored)} |")
    lines.append(f"| Healthy | {healthy} |")
    lines.append(f"| Watch | {watch} |")
    lines.append(f"| Degraded | {degraded} |")
    lines.append(f"| Disabled candidate | {disabled} |")
    lines.append(f"| High risk | {high} |")
    lines.append(f"| Unstructured failures | {len(fails)} |")
    lines.append("")

    lines.append("## 2. Source Health\n")
    lines.append("| Source ID | Type | Country | Status | Score | Risk | Recommendation |")
    lines.append("|---|---|---|---:|---|---|")
    for s in scored:
        lines.append(f"| `{s['sourceId']}` | {s['sourceType']} | {s['country']} | {s['status']} | {s['qualityScore']} | {s['risk']} | {s['recommendation'][:80]} |")
    lines.append("")

    if fails:
        lines.append("## 3. Unstructured Failures\n")
        for f in fails:
            lines.append(f"- **[{f['sourceId']}]** {f['finding']}")
            lines.append(f"  - Recommendation: {f['recommendation']}")
        lines.append("")

    if source_findings:
        lines.append(_generate_dryrun_source_findings_table(source_findings))

    section_num = 5 if source_findings else 4
    lines.append(f"## {section_num}. Registry Update Suggestions\n")
    for s in scored:
        if s["status"] in ("degraded", "disabled_candidate"):
            lines.append(f"- [ ] Update `{s['sourceId']}` status in source_registry.yaml ({s['status']})")
    if fails:
        lines.append("- [ ] Add per-source failure tracking to Governance Gaps")
        lines.append("- [ ] Create proposal for source quality scoring automation")
    lines.append("")

    return "\n".join(lines)


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _registry_quality_from_score(scored: dict, generated_at: str) -> dict[str, Any]:
    success_count = _coerce_int(scored.get("successCount"))
    failed_count = _coerce_int(scored.get("failedCount"))
    total = success_count + failed_count
    failure_rate = _rate(failed_count, total)
    reasons_text = " ".join(str(r).lower() for r in scored.get("reasons", []))

    if failed_count:
        timeout_rate = failure_rate if "timeout" in reasons_text else 0.0
        http403_rate = failure_rate if ("403" in reasons_text or "blocked" in reasons_text) else 0.0
    else:
        timeout_rate = 0.0
        http403_rate = 0.0

    return {
        "qualityScore": scored["qualityScore"],
        "qualityStatus": scored["status"],
        "risk": scored["risk"],
        "scoreGeneratedAt": generated_at,
        "scoringMethod": "hermes_source_quality_v1",
        "successRate": _rate(success_count, total),
        "failureRate": failure_rate,
        "timeoutRate": timeout_rate,
        "http403Rate": http403_rate,
        "extractionQualityScore": round(scored["qualityScore"] / 100, 4),
        "usefulContentRate": None,
        "duplicateRate": None,
    }


def _write_source_registry_scores(
    scored: list[dict],
    *,
    registry_dir: str | None,
    generated_at: str,
) -> Path:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to write source registry scores") from exc

    registry_base = Path(registry_dir).resolve() if registry_dir else REPO_ROOT / "hermes"
    registry_path = registry_base / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text()) or {}
    sources = data.get("sources")
    if not isinstance(sources, list):
        raise RuntimeError(f"Invalid source registry shape: {registry_path}")

    by_id = {item["sourceId"]: item for item in scored}
    for source in sources:
        scored_source = by_id.get(source.get("sourceId"))
        if not scored_source:
            continue

        last_observed = source.setdefault("lastObserved", {})
        last_observed["successCount"] = scored_source.get("successCount")
        last_observed["failedCount"] = scored_source.get("failedCount")
        last_observed["lastSuccessAt"] = scored_source.get("lastSuccessAt")
        last_observed["lastFailureAt"] = scored_source.get("lastFailureAt")
        last_observed["lastFailureReason"] = scored_source.get("lastFailureReason")
        source["quality"] = _registry_quality_from_score(scored_source, generated_at)

    registry_path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return registry_path


def _load_dryrun_artifact() -> dict | None:
    """Load the latest MSRP dryrun artifact for source-level failure breakdown."""
    paths = [
        REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "dryrun_report.json",
    ]
    for p in paths:
        if p.is_file():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None


def _add_dryrun_source_findings(report: dict) -> list[dict]:
    """Add source-level MSRP failure breakdown from the latest dryrun artifact."""
    artifact = _load_dryrun_artifact()
    if not artifact:
        return []
    results = artifact.get("results", []) or []
    if not results:
        return []

    findings: list[dict] = []
    seen_codes: set[str] = set()
    for r in results:
        code = r.get("code", "")
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        failure_reason = r.get("failureReason")
        if not failure_reason:
            continue
        findings.append({
            "sourceCode": code,
            "country": r.get("country", "?"),
            "status": r.get("status", "?"),
            "valid": r.get("valid", 0),
            "extracted": r.get("extracted", 0),
            "failureReason": failure_reason,
            "recommendedStrategy": r.get("recommendedStrategy", ""),
            "elapsed": r.get("elapsed", 0),
        })

    report["sourceLevelFindings"] = findings

    # Compute failure breakdown counts and add to unstructured failures
    if findings:
        fail_reasons: dict[str, int] = {}
        for f in findings:
            reason = f["failureReason"]
            fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
        report["failureBreakdown"] = fail_reasons

        # Add aggregate unstructured failure finding for MSRP
        report.setdefault("unstructuredFailures", []).append({
            "sourceId": "source.msrp.batch_a",
            "name": "MSRP Batch A (from dryrun artifact)",
            "failedCount": len(findings),
            "finding": f"{len(findings)} sources with classified failures in latest dryrun.",
            "recommendation": "Review source-level failureBreakdown and apply recommendedStrategy per source.",
            "sourceLevelFindingsLink": True,
        })

    return findings


def _generate_dryrun_source_findings_table(findings: list[dict]) -> str:
    if not findings:
        return ""
    lines: list[str] = []
    lines.append("\n## MSRP Source-Level Failure Breakdown\n")
    lines.append("| Country | Source Code | Status | Valid | Extracted | Failure Reason | Recommended Strategy | Elapsed |")
    lines.append("|---|---|---|---|---|---|---|")
    for f in findings:
        lines.append(
            f"| {f['country']} | `{f['sourceCode'][:40]}` | {f['status']} "
            f"| {f['valid']} | {f['extracted']} "
            f"| {f['failureReason']} | {f['recommendedStrategy']} | {f['elapsed']}s |"
        )
    lines.append("")
    if len(findings) > 20:
        lines.append(f"\n_Showing all {len(findings)} findings._\n")
    return "\n".join(lines)


def run(registry_dir: str | None = None, status_file: str | None = None) -> dict:
    print("[Hermes Source Quality] Scoring sources...")
    registries = load_all_registries(registry_dir)
    sources = registries.get("sources", [])
    status_data = _load_status_file(status_file)

    scored = [_compute_quality_score(s, status_data) for s in sources]
    scored.sort(key=lambda s: s["qualityScore"])
    fails = _check_unstructured_failures(sources)

    # Load dryrun artifact for source-level MSRP failure breakdown
    dryrun_findings = _add_dryrun_source_findings({
        "unstructuredFailures": fails,
    })

    healthy = sum(1 for s in scored if s["status"] == "healthy")
    watch = sum(1 for s in scored if s["status"] == "watch")
    degraded = sum(1 for s in scored if s["status"] == "degraded")
    high = sum(1 for s in scored if s["risk"] == "high")

    print(f"  {len(scored)} sources: {healthy} healthy, {watch} watch, {degraded} degraded, {high} high-risk")
    print(f"  {len(fails)} unstructured failure groups")
    if dryrun_findings:
        print(f"  {len(dryrun_findings)} source-level MSRP failures from dryrun artifact")

    result = {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "summary": {
            "totalSources": len(scored),
            "healthy": healthy,
            "watch": watch,
            "degraded": degraded,
            "highRisk": high,
            "unstructuredFailures": len(fails),
        },
        "sources": scored,
        "unstructuredFailures": fails,
    }

    # Include source-level findings if available
    if dryrun_findings:
        result["sourceLevelFindings"] = dryrun_findings
        fb = {}
        for f in dryrun_findings:
            reason = f["failureReason"]
            fb[reason] = fb.get(reason, 0) + 1
        result["failureBreakdown"] = fb

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Source Quality Governor")
    parser.add_argument("--registry-dir", default=None)
    parser.add_argument("--status-file", default=None)
    parser.add_argument("--out-json", default="hermes/reports/source_quality_report.json")
    parser.add_argument("--out-md", default="hermes/reports/source_quality_report.md")
    parser.add_argument(
        "--write-registry",
        action="store_true",
        help="Write computed quality fields back to hermes/source_registry.yaml",
    )
    args = parser.parse_args()
    os.chdir(REPO_ROOT)

    results = run(args.registry_dir, args.status_file)
    generated_at = results["generatedAt"]

    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(_generate_report(
        results["sources"],
        results["unstructuredFailures"],
        source_findings=results.get("sourceLevelFindings"),
    ))
    print(f"[Hermes Source Quality] Report: {out_md}")

    out_json = Path(args.out_json)
    out_json.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    print(f"[Hermes Source Quality] JSON: {out_json}")

    if args.write_registry:
        registry_path = _write_source_registry_scores(
            results["sources"],
            registry_dir=args.registry_dir,
            generated_at=generated_at,
        )
        print(f"[Hermes Source Quality] Registry updated: {registry_path}")


if __name__ == "__main__":
    main()
