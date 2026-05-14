"""Hermes Governance Layer — API routes.

Reads Hermes JSON reports and registry files. Read-only. No modifications.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/hermes", tags=["hermes"])

PROJECT_ROOT = Path(__file__).resolve().parents[4]
HERMES_DIR = PROJECT_ROOT / "hermes"
REPORTS_DIR = HERMES_DIR / "reports"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Not found: {path.relative_to(PROJECT_ROOT)}")
    return json.loads(path.read_text())


@router.get("/overview")
def hermes_overview() -> dict:
    """Return a consolidated Hermes governance overview."""
    overview: dict[str, Any] = {
        "registries": {},
        "reports": {},
        "proposals": {"total": 0, "implemented": 0, "pending": 0, "draft": 0},
        "gaps": {"total": 0, "open": 0, "resolved": 0},
    }

    # Registry counts
    for name in ["source", "pipeline", "feature", "prompt", "artifact"]:
        fname = f"{name}_registry.yaml"
        path = HERMES_DIR / fname
        if path.is_file():
            try:
                import yaml
                data = yaml.safe_load(path.read_text())
                key = f"{name}s" if not name.endswith("s") else name
                items = data.get(key, data.get(f"{name}s", [])) if data else []
                overview["registries"][name] = len(items) if isinstance(items, list) else 0
            except Exception:
                overview["registries"][name] = -1

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
            import yaml
            data = yaml.safe_load(prop_path.read_text())
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
            import yaml
            data = yaml.safe_load(gaps_path.read_text())
            gaps = data.get("gaps", []) if data else []
            overview["gaps"]["total"] = len(gaps)
            overview["gaps"]["open"] = sum(1 for g in gaps if g.get("status") == "open")
            overview["gaps"]["resolved"] = sum(1 for g in gaps if g.get("status") == "resolved")
        except Exception:
            pass

    return overview


@router.get("/pipeline-health")
def hermes_pipeline_health() -> dict:
    """Return the latest pipeline health report."""
    return _read_json(REPORTS_DIR / "pipeline_health.json")


@router.get("/source-quality")
def hermes_source_quality() -> dict:
    """Return the latest source quality report."""
    return _read_json(REPORTS_DIR / "source_quality_report.json")


@router.get("/cost")
def hermes_cost_report() -> dict:
    """Return the latest cost report."""
    return _read_json(REPORTS_DIR / "cost_report.json")


@router.get("/code-audit")
def hermes_code_audit() -> dict:
    """Return the latest code audit report."""
    return _read_json(REPORTS_DIR / "hermes_code_audit_report.json")


@router.get("/proposals")
def hermes_proposals(
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


@router.get("/features")
def hermes_features() -> list[dict]:
    """Return features from the registry."""
    path = HERMES_DIR / "feature_registry.yaml"
    if not path.is_file():
        return []
    import yaml
    data = yaml.safe_load(path.read_text())
    return data.get("features", []) if data else []


@router.get("/toolchain")
def hermes_toolchain() -> dict:
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


@router.get("/evidence-ledger")
def hermes_evidence_ledger(
    limit: int = Query(20, ge=1, le=100),
) -> list[dict]:
    """Return recent evidence ledger entries."""
    path = HERMES_DIR / "evidence_ledger.jsonl"
    if not path.is_file():
        return []
    entries: list[dict] = []
    for line in path.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
    entries.sort(key=lambda e: e.get("createdAt", ""), reverse=True)
    return entries[:limit]
