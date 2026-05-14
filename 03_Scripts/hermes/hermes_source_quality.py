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


def _safe(v: Any, key: str, default: Any = None) -> Any:
    if isinstance(v, dict):
        return v.get(key, default)
    return default


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


def _compute_quality_score(source: dict, status_data: dict | None) -> dict:
    """Score a single source 0–100 using deterministic rules."""
    score = 100
    reasons: list[str] = []
    src_id = _safe(source, "sourceId", "?")
    src_type = _safe(source, "sourceType", "unknown")
    governance = _safe(source, "governanceStatus", "unmanaged")

    # Repeated failures
    last_obs = _safe(source, "lastObserved", {}) or {}
    failed = last_obs.get("failedCount", 0) or 0
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
        "successCount": last_obs.get("successCount", 0) or 0,
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


def _generate_report(scored: list[dict], fails: list[dict]) -> str:
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

    lines.append("## 4. Registry Update Suggestions\n")
    for s in scored:
        if s["status"] in ("degraded", "disabled_candidate"):
            lines.append(f"- [ ] Update `{s['sourceId']}` status in source_registry.yaml ({s['status']})")
    if fails:
        lines.append("- [ ] Add per-source failure tracking to Governance Gaps")
        lines.append("- [ ] Create proposal for source quality scoring automation")
    lines.append("")

    return "\n".join(lines)


def run(registry_dir: str | None = None, status_file: str | None = None) -> dict:
    print("[Hermes Source Quality] Scoring sources...")
    registries = load_all_registries(registry_dir)
    sources = registries.get("sources", [])
    status_data = _load_status_file(status_file)

    scored = [_compute_quality_score(s, status_data) for s in sources]
    scored.sort(key=lambda s: s["qualityScore"])
    fails = _check_unstructured_failures(sources)

    healthy = sum(1 for s in scored if s["status"] == "healthy")
    watch = sum(1 for s in scored if s["status"] == "watch")
    degraded = sum(1 for s in scored if s["status"] == "degraded")
    high = sum(1 for s in scored if s["risk"] == "high")

    print(f"  {len(scored)} sources: {healthy} healthy, {watch} watch, {degraded} degraded, {high} high-risk")
    print(f"  {len(fails)} unstructured failure groups")

    return {
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Source Quality Governor")
    parser.add_argument("--registry-dir", default=None)
    parser.add_argument("--status-file", default=None)
    parser.add_argument("--out-json", default="hermes/reports/source_quality_report.json")
    parser.add_argument("--out-md", default="hermes/reports/source_quality_report.md")
    args = parser.parse_args()
    os.chdir(REPO_ROOT)

    results = run(args.registry_dir, args.status_file)

    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(_generate_report(results["sources"], results["unstructuredFailures"]))
    print(f"[Hermes Source Quality] Report: {out_md}")

    out_json = Path(args.out_json)
    out_json.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    print(f"[Hermes Source Quality] JSON: {out_json}")


if __name__ == "__main__":
    main()
