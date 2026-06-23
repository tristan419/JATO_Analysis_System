#!/usr/bin/env python3
"""Generate a human-readable summary of a Hermes server snapshot directory."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    records = []
    for line in path.read_text().strip().split("\n"):
        if line.strip():
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    return records


def _safe(v: Any, *keys: str) -> Any:
    for k in keys:
        if isinstance(v, dict) and k in v:
            v = v[k]
        else:
            return None
    return v


def generate(snapshot_dir: str) -> dict:
    base = Path(snapshot_dir)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    result: dict[str, Any] = {
        "generatedAt": now,
        "snapshotDir": str(base.resolve()),
        "files": {},
        "summary": {},
    }

    # File inventory
    all_files = sorted(base.rglob("*"))
    result["files"]["total"] = sum(1 for f in all_files if f.is_file())
    result["files"]["reports"] = sum(1 for f in all_files if "reports" in str(f) and f.is_file())

    # Pipeline health
    ph = _read_json(base / "hermes" / "reports" / "pipeline_health.json")
    if ph:
        s = ph.get("summary", {})
        result["summary"]["pipelineHealth"] = {
            "registeredPipelines": s.get("registeredPipelines", 0),
            "duplicateSchedulingRisks": s.get("duplicateSchedulingRisks", 0),
            "highRiskFindings": s.get("highRiskFindings", 0),
        }

    # Source quality
    sq = _read_json(base / "hermes" / "reports" / "source_quality_report.json")
    if sq:
        s = sq.get("summary", {})
        result["summary"]["sourceQuality"] = {
            "totalSources": s.get("totalSources", 0),
            "healthy": s.get("healthy", 0),
            "watch": s.get("watch", 0),
            "degraded": s.get("degraded", 0),
        }

    # Cost
    cr = _read_json(base / "hermes" / "reports" / "cost_report.json")
    if cr:
        s = cr.get("summary", {})
        result["summary"]["cost"] = {
            "totalEstimatedCostCny": s.get("totalEstimatedCostCny", 0),
            "budgetCny": s.get("budgetCny", 500),
            "budgetStatus": s.get("budgetStatus", "ok"),
        }

    # Status file
    sf = _read_json(base / "03_Scripts" / "logs" / "scheduled_fetch_status.json")
    if sf:
        result["summary"]["scheduledFetchStatus"] = {
            "pipelines": list(sf.keys()),
            "vocStatus": _safe(sf, "voc", "status"),
            "vocSuccessCount": _safe(sf, "voc", "successCount"),
        }

    # Evidence
    ev = _read_jsonl(base / "hermes" / "evidence_ledger.jsonl")
    result["summary"]["evidenceLedger"] = {"recordCount": len(ev)}

    # Answer audit
    aa = _read_jsonl(base / "hermes" / "answer_audit.jsonl")
    result["summary"]["answerAudit"] = {"recordCount": len(aa)}

    # Missing checks
    missing = []
    expected = [
        "hermes/reports/pipeline_health.json",
        "hermes/reports/source_quality_report.json",
        "hermes/reports/cost_report.json",
        "03_Scripts/logs/scheduled_fetch_status.json",
    ]
    for p in expected:
        if not (base / p).is_file():
            missing.append(p)
    result["summary"]["missingFiles"] = missing

    return result


def _report(results: dict) -> str:
    s = results["summary"]
    lines: list[str] = []
    lines.append("# Hermes Server Snapshot Summary\n")
    lines.append(f"**Generated:** {results['generatedAt']}")
    lines.append(f"**Snapshot:** `{results['snapshotDir']}`")
    lines.append(f"**Files:** {results['files']['total']} total, {results['files']['reports']} reports\n")

    ph = s.get("pipelineHealth")
    if ph:
        lines.append("## Pipeline Health\n")
        lines.append(f"| Metric | Value |")
        lines.append(f"|---|---|")
        lines.append(f"| Registered | {ph['registeredPipelines']} |")
        lines.append(f"| Duplicate Risks | {ph['duplicateSchedulingRisks']} |")
        lines.append(f"| High Risk | {ph['highRiskFindings']} |")
        lines.append("")

    sq = s.get("sourceQuality")
    if sq:
        lines.append("## Source Quality\n")
        lines.append(f"| Metric | Value |")
        lines.append(f"|---|---|")
        lines.append(f"| Total | {sq['totalSources']} |")
        lines.append(f"| Healthy | {sq['healthy']} |")
        lines.append(f"| Watch | {sq['watch']} |")
        lines.append(f"| Degraded | {sq['degraded']} |")
        lines.append("")

    cr = s.get("cost")
    if cr:
        lines.append("## Cost\n")
        lines.append(
            f"- Total: {cr['totalEstimatedCostCny']:.4f} CNY / {cr['budgetCny']} CNY budget ({cr['budgetStatus']})"
        )
        lines.append("")

    fs = s.get("scheduledFetchStatus")
    if fs:
        lines.append("## Scheduled Fetch\n")
        lines.append(f"- Pipelines: {', '.join(fs.get('pipelines', []))}")
        lines.append(f"- VOC: {fs.get('vocStatus', '?')} ({fs.get('vocSuccessCount', 0)} artifacts)")
        lines.append("")

    ev = s.get("evidenceLedger", {})
    aa = s.get("answerAudit", {})
    lines.append("## Evidence & Audit\n")
    lines.append(f"- Evidence records: {ev.get('recordCount', 0)}")
    lines.append(f"- Answer audits: {aa.get('recordCount', 0)}")
    lines.append("")

    missing = s.get("missingFiles", [])
    if missing:
        lines.append("## Missing\n")
        for m in missing:
            lines.append(f"- `{m}`")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description="Hermes Server Snapshot Summary Generator")
    p.add_argument("--snapshot-dir", default=".hermes_server_snapshot")
    p.add_argument("--out", default=".hermes_server_snapshot/SNAPSHOT_SUMMARY.md")
    p.add_argument("--json-out", default=".hermes_server_snapshot/SNAPSHOT_SUMMARY.json")
    args = p.parse_args()

    results = generate(args.snapshot_dir)
    Path(args.out).write_text(_report(results))
    Path(args.json_out).write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"Summary written: {args.out}")


if __name__ == "__main__":
    main()
