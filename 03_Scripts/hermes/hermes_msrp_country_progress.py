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
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
STATUS_FILE_PATH = REPO_ROOT / "03_Scripts" / "logs" / "scheduled_fetch_status.json"
FALLBACK_REPORT_PATH = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "dryrun_report.json"


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
        },
        "countries": country_entries,
        "topBlockingCountries": sorted(top_blocking, key=lambda x: x["passPct"]),
        "topFailureReasons": [{"reason": r, "count": c} for r, c in top_reasons[:5]],
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
    lines.append("")

    countries = result.get("countries", [])
    if countries:
        lines.append("## Country Progress\n")
        lines.append("| Country | Status | PassPct | Pass | Empty | Fail | Top Failure |")
        lines.append("|---|---:|---:|---:|---:|---:|---|")
        for c in sorted(countries, key=lambda x: x["passPct"]):
            lines.append(f"| {c['countryCode']} | {c['status']} | "
                        f"{c['passPct']}% | {c['pass']} | {c['empty']} | "
                        f"{c['fail']} | {c.get('topFailureReason', '-')} |")
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
