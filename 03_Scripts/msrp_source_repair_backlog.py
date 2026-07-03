#!/usr/bin/env python3
"""MSRP Source Repair Backlog Generator.

Reads the latest dryrun_report.json and produces a structured repair
backlog grouped by failure reason, with per-source recommended actions.

Usage:
  python 03_Scripts/msrp_source_repair_backlog.py
  python 03_Scripts/msrp_source_repair_backlog.py --dryrun-artifact path/to/dryrun_report.json
  python 03_Scripts/msrp_source_repair_backlog.py --out-dir path/to/output
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    from msrp_dryrun_aggregate import _write_source_repair_backlog as _write_v3_source_repair_backlog
except ImportError:  # pragma: no cover - keeps old-report fallback usable in stripped script contexts.
    _write_v3_source_repair_backlog = None  # type: ignore[assignment]


def _load_dryrun_report(path: str | None) -> dict:
    paths = [path] if path else []
    paths.append(str(REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts" / "dryrun_report.json"))
    for p in paths:
        pp = Path(p)
        if pp.is_file():
            try:
                return json.loads(pp.read_text())
            except Exception:
                pass
    return {}


def _classify_no_observation(src: dict) -> str:
    """Refine recommended strategy for no_observation_extracted."""
    tier = (src.get("tier") or "http").lower()
    url = str(src.get("sourceUrl") or "")
    if tier == "http":
        return "try_scrapling_dynamic"
    if "pdf" in url.lower() or "price list" in url.lower():
        return "try_pdf_text"
    return "diagnose_with_msrp_page_analyzer"


def _classify_validation_fix(src: dict) -> tuple[str, str]:
    """Return (likely_cause, recommended_fix) for validation_rejected_all."""
    rejected = [str(r).lower() for r in (src.get("rejectedReasons") or [])]
    rejected_rules = [str(r).lower() for r in (src.get("rejectedRules") or [])]
    rejection_rule_counts = src.get("rejectionRuleCounts") or {}
    if isinstance(rejection_rule_counts, dict):
        rejected_rules.extend(str(r).lower() for r in rejection_rule_counts)
    if any("currency" in r for r in rejected):
        return "currency issue", "check default_currency in the source YAML"
    if (
        any(r == "price_range" for r in rejected_rules)
        or any(
            ("price" in r or "msrp_value" in r)
            and ("range" in r or "out" in r or "<" in r or ">" in r)
            for r in rejected
        )
    ):
        return "price range issue", "check price parsing and units in extraction config"
    if any("financ" in r or "leas" in r or "monthly" in r for r in rejected):
        return "finance/leasing price issue", "avoid monthly payment / leasing selector"
    if any("model" in r or "trim" in r for r in rejected):
        return "model/trim mapping issue", "repair model_rules / fixed_jato_model"
    if any("powertrain" in r or "engine" in r for r in rejected):
        return "powertrain mapping issue", "repair fixed_jato_powertrain / powertrain keywords"
    return "validation rule too strict", "review validation config thresholds"


def _build_backlog(report: dict) -> dict:
    """Build structured repair backlog from dryrun report."""
    results = report.get("results") or []
    total = report.get("total") or len(results)
    pass_count = report.get("pass") or sum(1 for r in results if r.get("failureReason") is None)
    failed = [r for r in results if r.get("failureReason") is not None]
    pass_pct = report.get("passPct") or round(pass_count / total * 100, 1) if total else 0.0

    # Group by failure reason
    by_reason: dict[str, list[dict]] = {}
    for src in failed:
        reason = src.get("failureReason", "unknown")
        by_reason.setdefault(reason, []).append(src)

    # Build backlog items with enhanced recommendations
    backlog_items: list[dict] = []
    for reason, sources in sorted(by_reason.items()):
        for src in sources:
            item = {
                "country": src.get("country", "?"),
                "sourceCode": src.get("code") or src.get("sourceCode", "?"),
                "sourceUrl": src.get("sourceUrl", ""),
                "extractorType": src.get("extractorType", ""),
                "tier": src.get("tier", ""),
                "status": src.get("status", "?"),
                "valid": src.get("valid", 0),
                "extracted": src.get("extracted", 0),
                "rejected": src.get("rejected", 0),
                "failureReason": reason,
                "elapsed": src.get("elapsed", 0),
            }
            if reason == "no_observation_extracted":
                item["recommendedStrategy"] = _classify_no_observation(src)
            elif reason == "validation_rejected_all":
                cause, fix = _classify_validation_fix(src)
                item["likelyCause"] = cause
                item["recommendedFix"] = fix
                item["recommendedStrategy"] = fix
            else:
                item["recommendedStrategy"] = src.get("recommendedStrategy",
                                                       "diagnose_with_msrp_page_analyzer")
            backlog_items.append(item)

    # Compute strategy counts
    strategy_counts: dict[str, int] = {}
    for item in backlog_items:
        strat = item.get("recommendedStrategy", "unknown")
        strategy_counts[strat] = strategy_counts.get(strat, 0) + 1

    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "summary": {
            "totalSources": total,
            "successCount": pass_count,
            "failedCount": len(failed),
            "passPct": pass_pct,
            "gateThreshold": report.get("gateThreshold", 70),
            "gateStatus": "blocked" if pass_pct < (report.get("gateThreshold") or 70) else "allowed",
        },
        "failureBreakdown": {reason: len(sources) for reason, sources in sorted(by_reason.items())},
        "strategyRecommendations": strategy_counts,
        "backlog": backlog_items,
    }


def _render_markdown(backlog: dict) -> str:
    """Render backlog as Markdown report."""
    s = backlog["summary"]
    lines: list[str] = []
    lines.append("# MSRP Source Repair Backlog\n")
    lines.append(f"**Generated:** {backlog['generatedAt']}\n")
    lines.append("## Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---:|")
    lines.append(f"| Total sources | {s['totalSources']} |")
    lines.append(f"| Success | {s['successCount']} |")
    lines.append(f"| Failed | {s['failedCount']} |")
    lines.append(f"| Pass rate | {s['passPct']}% |")
    lines.append(f"| Gate threshold | {s['gateThreshold']}% |")
    lines.append(f"| Gate status | {s['gateStatus']} |")
    lines.append("")

    fb = backlog["failureBreakdown"]
    if fb:
        lines.append("## Failure Breakdown\n")
        lines.append("| Failure Reason | Count |")
        lines.append("|---|---:|")
        for reason, count in sorted(fb.items(), key=lambda x: -x[1]):
            lines.append(f"| {reason} | {count} |")
        lines.append("")

    sr = backlog["strategyRecommendations"]
    if sr:
        lines.append("## Strategy Recommendations\n")
        lines.append("| Strategy | Count |")
        lines.append("|---|---:|")
        for strat, count in sorted(sr.items(), key=lambda x: -x[1]):
            lines.append(f"| {strat} | {count} |")
        lines.append("")

    for reason in sorted(backlog.get("failureBreakdown", {})):
        items = [i for i in backlog["backlog"] if i["failureReason"] == reason]
        lines.append(f"## {reason} ({len(items)} sources)\n")
        if reason == "validation_rejected_all":
            lines.append("| Country | Source | Likely Cause | Recommended Fix |")
            lines.append("|---|---|---|---|")
            for item in items:
                lines.append(f"| {item['country']} | `{item['sourceCode'][:40]}` "
                           f"| {item.get('likelyCause', '?')} | {item.get('recommendedFix', '?')} |")
        else:
            lines.append("| Country | Source | Extractor | Tier | Recommended Strategy | Elapsed |")
            lines.append("|---|---|---|---|---|---|")
            for item in items:
                lines.append(f"| {item['country']} | `{item['sourceCode'][:40]}` "
                           f"| {item.get('extractorType', '-')} | {item.get('tier', '-')} "
                           f"| {item['recommendedStrategy']} | {item['elapsed']}s |")
        lines.append("")

    return "\n".join(lines)


def run(dryrun_path: str | None = None, out_dir: str | None = None) -> dict:
    report = _load_dryrun_report(dryrun_path)
    if not report:
        print("[backlog] No dryrun report found.", file=sys.stderr)
        return {}

    out_base = Path(out_dir).resolve() if out_dir else REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"
    out_base.mkdir(parents=True, exist_ok=True)
    json_path = out_base / "msrp_source_repair_backlog.json"
    md_path = out_base / "msrp_source_repair_backlog.md"

    if report.get("schemaVersion") == "msrp_dryrun_report_v3" and _write_v3_source_repair_backlog:
        history_base = Path(dryrun_path).resolve().parent if dryrun_path else out_base
        _write_v3_source_repair_backlog(report, out_base, history_base)
        try:
            backlog = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            backlog = {}
        print(f"[backlog] JSON: {json_path}")
        print(f"[backlog] Markdown: {md_path}")
        print(
            "[backlog] "
            f"{backlog.get('sourceRepairIssueCount', 0)} source repair issues, "
            f"{backlog.get('externalAccessIssueCount', 0)} external access issues, "
            f"{backlog.get('transientRegressionCount', 0)} transient rechecks"
        )
        return backlog

    backlog = _build_backlog(report)
    json_path.write_text(json.dumps(backlog, indent=2, ensure_ascii=False) + "\n")
    print(f"[backlog] JSON: {json_path}")

    md_path.write_text(_render_markdown(backlog))
    print(f"[backlog] Markdown: {md_path}")

    s = backlog["summary"]
    print(f"[backlog] {s['failedCount']} failed sources, "
          f"passPct={s['passPct']}%, gate={s['gateStatus']}")
    return backlog


def main() -> None:
    parser = argparse.ArgumentParser(description="MSRP Source Repair Backlog Generator")
    parser.add_argument("--dryrun-artifact", default=None)
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()
    run(args.dryrun_artifact, args.out_dir)


if __name__ == "__main__":
    main()
