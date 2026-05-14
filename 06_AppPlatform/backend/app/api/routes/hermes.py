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
