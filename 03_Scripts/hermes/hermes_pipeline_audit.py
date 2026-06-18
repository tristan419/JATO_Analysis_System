#!/usr/bin/env python3
"""Hermes Phase 4 — Pipeline Governor Audit.

Scan Airflow / systemd / GitHub Actions / crawler scripts / artifacts
and generate a pipeline health report. Audit-only — no modifications.

Usage:
  python 03_Scripts/hermes/hermes_pipeline_audit.py
  python 03_Scripts/hermes/hermes_pipeline_audit.py --out-md custom.md --out-json custom.json
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

from hermes_registry_loader import load_all_registries

REPO_ROOT = Path(__file__).resolve().parents[2]

# ── Discovery ────────────────────────────────────────────────────────


def _discover_systemd_units() -> list[dict]:
    """Find all systemd .service and .timer files in the repo."""
    units: list[dict] = []
    base = REPO_ROOT / "03_Scripts" / "deploy" / "systemd"
    if not base.is_dir():
        return units
    for f in sorted(base.glob("*.service")):
        content = f.read_text(errors="replace")
        schedule_match = re.search(r"OnCalendar=([^\n]+)", content)
        exec_match = re.search(r"ExecStart=([^\n]+)", content)
        units.append(
            {
                "name": f.stem,
                "path": str(f.relative_to(REPO_ROOT)),
                "type": "systemd_service",
                "schedule": schedule_match.group(1).strip() if schedule_match else None,
                "execStart": exec_match.group(1).strip() if exec_match else None,
                "contentSchedule": schedule_match is not None,
            }
        )
    for f in sorted(base.glob("*.timer")):
        content = f.read_text(errors="replace")
        schedule_match = re.search(r"OnCalendar=([^\n]+)", content)
        unit_match = re.search(r"Unit=([^\n]+)", content)
        units.append(
            {
                "name": f.stem,
                "path": str(f.relative_to(REPO_ROOT)),
                "type": "systemd_timer",
                "schedule": schedule_match.group(1).strip() if schedule_match else None,
                "triggersUnit": unit_match.group(1).strip() if unit_match else None,
            }
        )
    return units


def _discover_airflow_dags() -> list[dict]:
    """Find all Airflow DAG Python files."""
    dags: list[dict] = []
    base = REPO_ROOT / "airflow" / "dags"
    if not base.is_dir():
        return dags
    for f in sorted(base.glob("*.py")):
        if f.name.startswith("_"):
            continue
        content = f.read_text(errors="replace")
        dag_id_match = re.search(r'(?:dag_id|DAG)\s*[=:]\s*["\']([^"\']+)["\']', content)
        schedule_match = re.search(r'schedule\s*[=:]\s*["\']([^"\']+)["\']', content)
        cron_match = re.search(r'cron\s*[=:]\s*["\']([^"\']+)["\']', content)
        schedule_attr = re.search(r"schedule\s*=\s*([^,\n]+)", content)
        dags.append(
            {
                "name": f.stem,
                "path": str(f.relative_to(REPO_ROOT)),
                "type": "airflow_dag",
                "dagId": dag_id_match.group(1) if dag_id_match else f.stem,
                "schedule": (
                    schedule_match.group(1)
                    if schedule_match
                    else (
                        cron_match.group(1)
                        if cron_match
                        else (schedule_attr.group(1).strip() if schedule_attr else None)
                    )
                ),
            }
        )
    return dags


def _discover_github_workflows() -> list[dict]:
    """Find GitHub Actions workflow files with schedule or workflow_dispatch."""
    wfs: list[dict] = []
    base = REPO_ROOT / ".github" / "workflows"
    if not base.is_dir():
        return wfs
    for f in sorted(base.glob("*.yml")):
        content = f.read_text(errors="replace")
        has_schedule = "schedule:" in content
        has_dispatch = "workflow_dispatch" in content
        cron_matches = re.findall(r'cron:\s*["\']([^"\']+)["\']', content)

        wfs.append(
            {
                "name": f.stem,
                "path": str(f.relative_to(REPO_ROOT)),
                "type": "github_action",
                "hasSchedule": has_schedule,
                "hasDispatch": has_dispatch,
                "crons": cron_matches,
            }
        )
    return wfs


def _discover_python_scripts() -> list[dict]:
    """Find key Python scripts that are pipeline-like (ETL, sync, batch, etc.)."""
    scripts: list[dict] = []
    scan_dirs = [
        REPO_ROOT / "03_Scripts",
        REPO_ROOT / "07_ScrapingToolkit",
    ]
    key_patterns = [
        "sync",
        "fetch",
        "enrich",
        "ingest",
        "dryrun",
        "batch",
        "refresh",
        "elt",
        "partition",
        "precompute",
        "crawl",
        "scrape",
        "run_",
        "_runner",
        "_fetcher",
        "_enricher",
    ]
    for scan_dir in scan_dirs:
        if not scan_dir.is_dir():
            continue
        for f in sorted(scan_dir.rglob("*.py")):
            if any(p in f.name.lower() for p in key_patterns):
                if "__pycache__" in str(f) or "test" in f.name.lower():
                    continue
                scripts.append(
                    {
                        "name": f.stem,
                        "path": str(f.relative_to(REPO_ROOT)),
                        "type": "python_script",
                    }
                )
    return scripts


def _discover_shell_runners() -> list[dict]:
    """Find shell scripts that wrap pipeline execution."""
    scripts: list[dict] = []
    base = REPO_ROOT / "03_Scripts"
    if not base.is_dir():
        return scripts
    key_patterns = ["run_", "sync_", "deploy_", "schedule_", "batch_"]
    for f in sorted(base.rglob("*.sh")):
        if any(p in f.name.lower() for p in key_patterns):
            if "__pycache__" in str(f):
                continue
            scripts.append(
                {
                    "name": f.stem,
                    "path": str(f.relative_to(REPO_ROOT)),
                    "type": "shell_runner",
                }
            )
    return scripts


# ── Cross-reference & Checks ────────────────────────────────────────


def _safe_dict(d: Any, key: str, default: Any = None) -> Any:
    if isinstance(d, dict):
        return d.get(key, default)
    return default


def check_registry_coverage(
    discovered: dict[str, list],
    registry_pipelines: list[dict],
) -> list[dict]:
    """Find discovered pipelines that have no registry entry."""
    findings: list[dict] = []
    registered_paths: set[str] = set()
    for p in registry_pipelines:
        path = _safe_dict(p, "path", "")
        if path:
            registered_paths.add(str(path))

    for cat, items in discovered.items():
        for item in items:
            item_path = item.get("path", "")
            # Check if any registered pipeline has a matching path
            matched = any(item_path in rp or rp in item_path for rp in registered_paths)
            if not matched and item.get("type") not in ("shell_runner", "python_script"):
                findings.append(
                    {
                        "severity": "WARNING",
                        "area": "Registry Coverage",
                        "finding": f"Discovered {item['type']} not in Pipeline Registry: {item['name']} ({item_path})",
                        "item": item,
                    }
                )
    return findings


def check_duplicate_scheduling(
    systemd_units: list[dict],
    airflow_dags: list[dict],
    gh_workflows: list[dict],
    registry_pipelines: list[dict],
) -> list[dict]:
    """Detect pipelines that produce the same output on different schedules."""
    findings: list[dict] = []

    # Known: Country News Sync
    news_systemd = [u for u in systemd_units if "news" in u["name"].lower()]
    news_airflow = [d for d in airflow_dags if "news" in d["name"].lower() or "news" in d.get("dagId", "").lower()]
    news_gh = [w for w in gh_workflows if "news" in w["name"].lower()]

    if news_systemd and news_airflow:
        findings.append(
            {
                "severity": "HIGH",
                "area": "Duplicate Scheduling",
                "finding": "Country News Sync exists in both systemd timer and Airflow DAG",
                "pipelines": {
                    "systemd": [u["name"] for u in news_systemd],
                    "airflow": [d["dagId"] for d in news_airflow],
                },
                "recommendation": (
                    "Decide single production scheduler. Recommended: keep systemd timer "
                    "(runs on server, direct DB access). Deprecate Airflow DAG or change to manual-only."
                ),
                "gapRef": "gap.pipeline.duplicate_news_scheduling",
            }
        )

    if news_gh:
        findings.append(
            {
                "severity": "MEDIUM",
                "area": "Duplicate Scheduling",
                "finding": (
                    f"Country News Sync has GitHub Actions workflow: {[w['name'] for w in news_gh]}. "
                    "Verify it is workflow_dispatch only (no schedule)."
                ),
                "recommendation": "Ensure GitHub Actions news workflow is manual-only, not scheduled.",
            }
        )

    # MSRP overlap
    msrp_systemd = [u for u in systemd_units if "msrp" in u["name"].lower()]
    msrp_airflow = [d for d in airflow_dags if "msrp" in d["name"].lower() or "msrp" in d.get("dagId", "").lower()]
    if msrp_systemd and msrp_airflow:
        # Airflow MSRP is manual-only, so this is acceptable
        findings.append(
            {
                "severity": "LOW",
                "area": "Scheduling Overlap",
                "finding": (
                    "MSRP has both systemd timers (production) and Airflow DAG (manual-only). "
                    "Acceptable — no schedule conflict."
                ),
                "recommendation": "Keep current setup. Airflow DAG provides manual trigger alternative.",
            }
        )

    return findings


def check_artifact_map(
    registry_artifacts: list[dict],
    registry_pipelines: list[dict],
    registry_features: list[dict],
) -> list[dict]:
    """Build artifact producer/consumer map and find gaps."""
    findings: list[dict] = []

    for art in registry_artifacts:
        art_id = _safe_dict(art, "artifactId", "?")
        art_name = _safe_dict(art, "name", art_id)
        art_path = _safe_dict(art, "path", "")
        producers = _safe_dict(art, "producerPipelines", [])
        consumers = _safe_dict(art, "consumerFeatures", [])

        # Check producer exists in pipeline registry
        for prod_id in producers or []:
            if not any(_safe_dict(p, "pipelineId") == prod_id for p in registry_pipelines):
                findings.append(
                    {
                        "severity": "WARNING",
                        "area": "Artifact Producer Gap",
                        "finding": (
                            f"Artifact '{art_name}' references producer '{prod_id}' "
                            "not found in Pipeline Registry"
                        ),
                        "artifactId": art_id,
                    }
                )

        # Check consumer exists in feature registry
        for cons_id in consumers or []:
            if not any(_safe_dict(f, "featureId") == cons_id for f in registry_features):
                findings.append(
                    {
                        "severity": "INFO",
                        "area": "Artifact Consumer Gap",
                        "finding": (
                            f"Artifact '{art_name}' references consumer '{cons_id}' "
                            "not found in Feature Registry"
                        ),
                        "artifactId": art_id,
                    }
                )

        # Check if artifact path exists locally
        if art_path and not art_path.startswith("PostgreSQL:"):
            local_path = REPO_ROOT / art_path
            if local_path.exists():
                mtime = datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600
                if age_hours > 168:
                    findings.append(
                        {
                            "severity": "MEDIUM",
                            "area": "Stale Artifact",
                            "finding": f"Artifact '{art_name}' last modified {age_hours:.0f}h ago (>7d)",
                            "artifactId": art_id,
                            "path": art_path,
                        }
                    )
            else:
                # Not found locally — may only exist on server
                pass

    return findings


def check_status_coverage(
    status_json_path: str | None = None,
) -> dict:
    """Read scheduled_fetch_status.json if available and check coverage."""
    result: dict[str, Any] = {
        "exists": False,
        "data": {},
        "coveredPipelines": [],
        "missingCoverage": [],
    }

    paths_to_try = [
        status_json_path,
        str(REPO_ROOT / "03_Scripts" / "logs" / "scheduled_fetch_status.json"),
    ]
    status_data = None
    for p in paths_to_try:
        if p and Path(p).is_file():
            try:
                status_data = json.loads(Path(p).read_text())
                result["exists"] = True
                result["data"] = status_data
                break
            except Exception:
                pass

    if not result["exists"]:
        result["missingCoverage"].append(
            {
                "pipeline": "all",
                "finding": "scheduled_fetch_status.json not found locally. Status coverage unknown.",
            }
        )
        return result

    # Check which pipelines are covered
    expected_keys = ["news", "voc", "msrp", "jato_etl"]
    for key in expected_keys:
        if key in status_data:
            entry = status_data[key]
            result["coveredPipelines"].append(
                {
                    "pipeline": key,
                    "status": entry.get("status", "unknown"),
                    "lastRunAt": entry.get("lastRunAt"),
                    "successCount": entry.get("successCount", 0),
                    "failedCount": entry.get("failedCount", 0),
                }
            )
        else:
            result["missingCoverage"].append(
                {
                    "pipeline": key,
                    "finding": f"Pipeline '{key}' has no entry in scheduled_fetch_status.json",
                }
            )

    return result


def check_source_failure_tracking(
    status_data: dict | None,
) -> list[dict]:
    """Check whether VOC source errors are structured (per-source tracking)."""
    findings: list[dict] = []

    if not status_data:
        findings.append(
            {
                "severity": "WARNING",
                "area": "Source Failure Tracking",
                "finding": (
                    "No scheduled_fetch_status.json available. Cannot verify per-source error tracking "
                    "for VOC/News/MSRP."
                ),
                "recommendation": (
                    "Implement per-source failed_sources.json output in voc_fetcher.py "
                    "and news_runner.py."
                ),
            }
        )
        return findings

    voc = status_data.get("voc", {})
    if voc:
        failed = voc.get("failedCount", 0)
        if failed > 0:
            # Check if there's per-source breakdown
            has_per_source = voc.get("failedSources") is not None
            if not has_per_source:
                findings.append(
                    {
                        "severity": "WARNING",
                        "area": "Source Failure Tracking",
                        "finding": (
                            f"VOC reports {failed} failed sources but no per-source breakdown "
                            "(sourceId, url, error type)"
                        ),
                        "recommendation": (
                            "Add per-source error tracking to voc_fetcher.py. Output failed_sources.json "
                            "with source code, error type, timestamp."
                        ),
                    }
                )

    return findings


def check_news_batch_coverage(
    registry_sources: list[dict],
    registry_pipelines: list[dict],
) -> list[dict]:
    """Check if all news batches have scheduled pipelines."""
    findings: list[dict] = []
    news_sources = [s for s in registry_sources if _safe_dict(s, "sourceType") == "news"]

    for src in news_sources:
        src_id = _safe_dict(src, "sourceId", "?")
        known_issues = _safe_dict(src, "knownIssues", [])
        for issue in known_issues or []:
            if isinstance(issue, str) and "no scheduled" in issue.lower():
                findings.append(
                    {
                        "severity": "MEDIUM",
                        "area": "Unscheduled Source",
                        "finding": f"{src_id}: {issue}",
                        "recommendation": f"Add {src_id} to an existing systemd timer or create a new one.",
                    }
                )

    return findings


# ── Risk Scoring ─────────────────────────────────────────────────────


def score_pipeline_risk(
    pipeline: dict,
    findings: list[dict],
    status_coverage: dict,
) -> str:
    """Score risk for a single pipeline."""
    pid = _safe_dict(pipeline, "pipelineId", _safe_dict(pipeline, "name", "?"))
    high_keywords = ["ingest", "deploy", "production", "monthly_update"]
    for kw in high_keywords:
        if kw in str(pid).lower():
            return "high"

    # Check findings for this pipeline
    pipeline_findings = [
        f for f in findings if str(pid) in str(f.get("finding", "")) or str(pid) in str(f.get("pipelines", ""))
    ]
    if any(f["severity"] in ("HIGH", "BLOCKER") for f in pipeline_findings):
        return "high"

    covered = any(str(pid) in str(c.get("pipeline", "")) for c in status_coverage.get("coveredPipelines", []))
    if not covered and _safe_dict(pipeline, "type") in ("systemd_timer", "airflow_dag"):
        return "medium"

    return "low"


# ── Report Generation ────────────────────────────────────────────────


def _generate_report(results: dict) -> str:
    s = results["summary"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines: list[str] = []
    lines.append("# Hermes Pipeline Audit Report\n")
    lines.append(f"**Generated:** {now}\n")

    # §1 Summary
    lines.append("## 1. Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Registered pipelines | {s['registeredPipelines']} |")
    lines.append(f"| Discovered systemd units | {s['discoveredSystemd']} |")
    lines.append(f"| Discovered Airflow DAGs | {s['discoveredAirflow']} |")
    lines.append(f"| Discovered GitHub Actions | {s['discoveredGitHubActions']} |")
    lines.append(f"| Discovered scripts/runners | {s['discoveredScripts']} |")
    lines.append(f"| Duplicate scheduling risks | {s['duplicateSchedulingRisks']} |")
    lines.append(f"| Missing registry entries | {s['missingRegistryEntries']} |")
    lines.append(f"| Status coverage gaps | {s['statusCoverageGaps']} |")
    lines.append(f"| High-risk findings | {s['highRiskFindings']} |")
    lines.append("")

    # §2 Coverage
    lines.append("## 2. Pipeline Coverage\n")
    lines.append("| Pipeline ID | Type | Registry | Path | Risk |")
    lines.append("|---|---|---|---|---|")
    for p in results["allPipelines"]:
        pid = p.get("pipelineId", p.get("name", "?"))
        ptype = p.get("type", "?")
        reg = p.get("registryStatus", "unknown")
        path = p.get("path", "")[:60]
        risk = p.get("risk", "low")
        reg_icon = "registered" if reg == "registered" else "missing"
        lines.append(f"| {pid} | {ptype} | {reg_icon} | {path} | {risk} |")
    lines.append("")

    # §3 Duplicate Scheduling
    lines.append("## 3. Duplicate Scheduling Risks\n")
    dupes = results.get("duplicateScheduling", [])
    if dupes:
        for d in dupes:
            lines.append(f"### {d['severity']}: {d['area']}\n")
            lines.append(f"- **Finding:** {d['finding']}")
            lines.append(f"- **Recommendation:** {d.get('recommendation', 'Review and decide.')}")
            lines.append("")
    else:
        lines.append("_No duplicate scheduling risks detected._\n")
    lines.append("")

    # §4 Artifact Map
    lines.append("## 4. Artifact Producer / Consumer Map\n")
    lines.append("| Artifact | Producer | Consumer | Path | Freshness |")
    lines.append("|---|---|---|---|---|")
    for art in results.get("artifactMap", []):
        name = art.get("name", "?")
        prod = ", ".join(art.get("producers", []))[:40] or "—"
        cons = ", ".join(art.get("consumers", []))[:40] or "—"
        path = art.get("path", "")[:50]
        fresh = art.get("freshness", "unknown")
        lines.append(f"| {name} | {prod} | {cons} | {path} | {fresh} |")
    lines.append("")

    # §5 Status Coverage
    lines.append("## 5. Scheduled Status Coverage\n")
    sc = results.get("statusCoverage", {})
    if sc.get("exists"):
        lines.append(f"**Status file:** found\n")
        covered = sc.get("coveredPipelines", [])
        if covered:
            lines.append("| Pipeline | Status | Last Run | Success | Failed |")
            lines.append("|---|---|---|---|---|")
            for c in covered:
                success_count = c.get("successCount", 0)
                failed_count = c.get("failedCount", 0)
                lines.append(
                    f"| {c['pipeline']} | {c['status']} | {c.get('lastRunAt', '?')} | "
                    f"{success_count} | {failed_count} |"
                )
        missing = sc.get("missingCoverage", [])
        if missing:
            lines.append(f"\n**Missing coverage:** {len(missing)} pipeline(s)")
            for m in missing:
                lines.append(f"- {m['pipeline']}: {m['finding']}")
    else:
        lines.append("**Status file:** not found locally. Cannot verify status coverage.\n")
    lines.append("")

    # §6 Source Failure Tracking
    lines.append("## 6. Source Failure Tracking\n")
    sft = results.get("sourceFailureTracking", [])
    if sft:
        for f in sft:
            lines.append(f"- **[{f['severity']}]** {f['finding']}")
            if f.get("recommendation"):
                lines.append(f"  - Recommendation: {f['recommendation']}")
    else:
        lines.append("_Source failure tracking is adequate._")
    lines.append("")

    # §7 Findings
    lines.append("## 7. Findings\n")
    all_findings = results.get("allFindings", [])
    if all_findings:
        lines.append("| Severity | Area | Finding |")
        lines.append("|---|---|---|")
        for f in all_findings:
            sev = f["severity"]
            lines.append(f"| {sev} | {f['area']} | {f['finding'][:150]} |")
    else:
        lines.append("_No findings._")
    lines.append("")

    # §8 Registry Updates
    lines.append("## 8. Recommended Registry Updates\n")
    missing_reg = [f for f in all_findings if f["area"] == "Registry Coverage"]
    if missing_reg:
        lines.append(f"- [ ] Pipeline Registry: add {len(missing_reg)} missing pipeline(s)")
    dupes_needing = [d for d in dupes if d["severity"] == "HIGH"]
    if dupes_needing:
        lines.append("- [ ] Pipeline Registry: update duplicate scheduling notes")
    lines.append("- [ ] Review and update artifact freshness statuses")
    lines.append("")

    # §9 Next Actions
    lines.append("## 9. Next Actions\n")
    if dupes_needing:
        lines.append("- [ ] **P0:** Decide Country News production scheduler (systemd vs Airflow)")
    lines.append("- [ ] **P1:** Add per-source VOC failure tracking (sourceId, url, error type)")
    lines.append("- [ ] **P1:** Expand scheduled_fetch_status.json to cover News, MSRP, JATO ETL")
    lines.append("- [ ] **P2:** Add artifact freshness monitoring automation")
    lines.append("")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────


def run_audit(status_json_path: str | None = None) -> dict:
    print("[Hermes Pipeline Audit] Scanning pipelines...")

    # Load registries
    registries = load_all_registries()
    reg_pipelines = registries.get("pipelines", [])
    reg_artifacts = registries.get("artifacts", [])
    reg_features = registries.get("features", [])
    reg_sources = registries.get("sources", [])

    # Discover
    systemd_units = _discover_systemd_units()
    airflow_dags = _discover_airflow_dags()
    gh_workflows = _discover_github_workflows()
    py_scripts = _discover_python_scripts()
    sh_runners = _discover_shell_runners()

    print(f"  systemd: {len(systemd_units)} units")
    print(f"  Airflow: {len(airflow_dags)} DAGs")
    print(f"  GitHub Actions: {len(gh_workflows)} workflows (scheduled/dispatch)")
    print(f"  Python scripts: {len(py_scripts)} pipeline-like scripts")
    print(f"  Shell runners: {len(sh_runners)} runner scripts")

    # Build discovered dict
    discovered = {
        "systemd": systemd_units,
        "airflow": airflow_dags,
        "github": gh_workflows,
        "python": py_scripts,
        "shell": sh_runners,
    }

    all_findings: list[dict] = []

    # Check 1: Registry coverage
    all_findings.extend(check_registry_coverage(discovered, reg_pipelines))

    # Check 2: Duplicate scheduling
    dupes = check_duplicate_scheduling(systemd_units, airflow_dags, gh_workflows, reg_pipelines)
    all_findings.extend(dupes)

    # Check 3: Artifact map
    all_findings.extend(check_artifact_map(reg_artifacts, reg_pipelines, reg_features))

    # Check 4: Status coverage
    status_coverage = check_status_coverage(status_json_path)

    # Check 5: Source failure tracking
    status_data = status_coverage.get("data") if status_coverage.get("exists") else None
    all_findings.extend(check_source_failure_tracking(status_data))

    # Check 6: News batch coverage
    all_findings.extend(check_news_batch_coverage(reg_sources, reg_pipelines))

    # Build all pipelines list
    all_pipelines: list[dict] = []
    for p in reg_pipelines:
        pid = _safe_dict(p, "pipelineId", "?")
        risk = score_pipeline_risk(p, all_findings, status_coverage)
        all_pipelines.append(
            {
                "pipelineId": pid,
                "name": _safe_dict(p, "name", pid),
                "type": _safe_dict(p, "type", "?"),
                "path": _safe_dict(p, "path", ""),
                "registryStatus": "registered",
                "risk": risk,
                "status": "unknown",
            }
        )

    # Build artifact map
    artifact_map: list[dict] = []
    for art in reg_artifacts:
        art_id = _safe_dict(art, "artifactId", "?")
        art_path = _safe_dict(art, "path", "")
        freshness = "unknown"
        if art_path and not art_path.startswith("PostgreSQL:"):
            local = REPO_ROOT / art_path
            if local.exists():
                mtime = datetime.fromtimestamp(local.stat().st_mtime, tz=timezone.utc)
                hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600
                freshness = "fresh" if hours < 48 else ("watch" if hours < 168 else "stale")
            else:
                freshness = "server_only"

        artifact_map.append(
            {
                "artifactId": art_id,
                "name": _safe_dict(art, "name", art_id),
                "path": art_path,
                "producers": _safe_dict(art, "producerPipelines", []),
                "consumers": _safe_dict(art, "consumerFeatures", []),
                "freshness": freshness,
            }
        )

    # Summary
    high_risk = sum(1 for f in all_findings if f["severity"] in ("HIGH", "BLOCKER"))
    missing_reg = sum(1 for f in all_findings if f["area"] == "Registry Coverage")
    status_gaps = len(status_coverage.get("missingCoverage", []))

    summary = {
        "registeredPipelines": len(reg_pipelines),
        "discoveredSystemd": len(systemd_units),
        "discoveredAirflow": len(airflow_dags),
        "discoveredGitHubActions": sum(1 for w in gh_workflows if w["hasSchedule"] or w["hasDispatch"]),
        "discoveredScripts": len(py_scripts) + len(sh_runners),
        "duplicateSchedulingRisks": len(dupes),
        "missingRegistryEntries": missing_reg,
        "statusCoverageGaps": status_gaps,
        "highRiskFindings": high_risk,
    }

    return {
        "summary": summary,
        "allPipelines": all_pipelines,
        "duplicateScheduling": dupes,
        "artifactMap": artifact_map,
        "statusCoverage": {
            "exists": status_coverage["exists"],
            "coveredPipelines": status_coverage["coveredPipelines"],
            "missingCoverage": status_coverage["missingCoverage"],
        },
        "sourceFailureTracking": [f for f in all_findings if f["area"] == "Source Failure Tracking"],
        "allFindings": all_findings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hermes Pipeline Governor Audit — scan and report on pipeline health",
    )
    parser.add_argument("--registry-dir", default=None)
    parser.add_argument("--status-json", default=None)
    parser.add_argument("--out-md", default="hermes/reports/hermes_pipeline_audit_report.md")
    parser.add_argument("--out-json", default="hermes/reports/pipeline_health.json")
    args = parser.parse_args()

    os.chdir(REPO_ROOT)

    results = run_audit(args.status_json)

    out_md = REPO_ROOT / args.out_md
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(_generate_report(results), encoding="utf-8")
    print(f"[Hermes Pipeline Audit] Report: {out_md}")

    out_json = REPO_ROOT / args.out_json
    out_json.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    print(f"[Hermes Pipeline Audit] JSON: {out_json}")

    s = results["summary"]
    print(f"\n[Hermes Pipeline Audit] Summary:")
    print(
        f"  Registered: {s['registeredPipelines']} | Systemd: {s['discoveredSystemd']} | "
        f"Airflow: {s['discoveredAirflow']}"
    )
    print(
        f"  Duplicates: {s['duplicateSchedulingRisks']} | Missing reg: {s['missingRegistryEntries']} | "
        f"High risk: {s['highRiskFindings']}"
    )


if __name__ == "__main__":
    main()
