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


@router.get("/architecture")
def hermes_architecture() -> dict:
    """Return the Hermes governance architecture — modules, dependencies, and work routing."""
    modules = [
        {
            "governor": "Code Governor",
            "icon": "code",
            "phase": "Phase 2-3",
            "scripts": ["hermes_intake.py", "hermes_code_audit.py"],
            "inputs": ["PRD.md", "git diff (base..HEAD)", "hermes/*.yaml registries"],
            "outputs": ["intake report (.md + .json)", "code audit report (.md + .json)"],
            "answers": [
                "这个 PRD 会影响哪些功能/管道/源/prompt？",
                "Claude Code 改完有没有漏 registry/docs/tests？",
                "有没有 secret 泄露或 schema 变更无 migration？",
            ],
            "triggers": "每次写新 PRD 或 push 代码后手动运行",
        },
        {
            "governor": "Pipeline Governor",
            "icon": "pipeline",
            "phase": "Phase 4-4.5",
            "scripts": ["hermes_pipeline_audit.py"],
            "inputs": ["hermes/pipeline_registry.yaml", ".github/workflows/", "airflow/dags/", "03_Scripts/deploy/systemd/", "scheduled_fetch_status.json"],
            "outputs": ["pipeline health report (.md)", "pipeline_health.json"],
            "answers": [
                "哪些 pipeline 在生产？哪些是手动后备？",
                "Country News 有没有重复调度？",
                "哪个 artifact 被哪个 feature 消费？",
                "VOC 源错误有没有被结构化追踪？",
            ],
            "triggers": "定期运行（建议每周）或 pipeline 变更后",
        },
        {
            "governor": "Intelligence Governor",
            "icon": "intelligence",
            "phase": "Phase 5-5.5",
            "scripts": ["hermes_source_quality.py", "hermes_evidence_writer.py", "hermes_answer_audit.py", "hermes_cost_report.py"],
            "inputs": ["hermes/source_registry.yaml", "hermes/answer_audit.jsonl", "hermes/model_pricing.yaml", "VOC/News/MSRP artifacts"],
            "outputs": ["source quality report (.json)", "evidence_ledger.jsonl", "answer audit (.jsonl)", "cost report (.json)"],
            "answers": [
                "VOC/News/MSRP 源质量如何？哪个该降级？",
                "国家助手回答有没有证据？幻觉风险多高？",
                "Flash vs Pro 各花了多少钱？有没有超预算？",
                "哪些 evidence 可以跨回答复用？",
            ],
            "triggers": "每次 VOC/News 管道运行后，或国家助手回答后",
        },
        {
            "governor": "Knowledge Governor",
            "icon": "knowledge",
            "phase": "Phase 1 + ongoing",
            "scripts": ["hermes_registry_loader.py", "hermes_text_matcher.py", "hermes/*.yaml"],
            "inputs": ["REPOSITORY_ASSET_MAP.md", "CLAUDE.md", "Markdown_Readme/", "hermes/*.yaml"],
            "outputs": ["8 registry YAML files (71+ entries)", "feature/pipeline/source/prompt/artifact registries"],
            "answers": [
                "系统里到底有什么功能/管道/源/prompt？",
                "哪个功能没有 owner？哪个管道没有注册？",
                "新 PRD 和现有功能有没有重叠？",
                "GitNexus / Roadmap / CLAUDE.md 是否一致？",
            ],
            "triggers": "每次新增功能/管道/源/prompt 后更新 registry",
        },
    ]

    # Dependency graph: who reads/writes what
    deps = [
        {"from": "hermes_registry_loader.py", "to": "ALL scripts", "what": "reads hermes/*.yaml → returns typed dicts"},
        {"from": "hermes_text_matcher.py", "to": "hermes_intake.py", "what": "keyword extraction + scoring engine"},
        {"from": "hermes_intake.py", "to": "developer", "what": "PRD → impact report → Claude Code brief"},
        {"from": "Claude Code", "to": "hermes_code_audit.py", "what": "git diff → 10-rule audit scan"},
        {"from": "hermes_pipeline_audit.py", "to": "developer", "what": "scans systemd/Airflow/GH Actions → health report"},
        {"from": "hermes_source_quality.py", "to": "developer", "what": "scores VOC/News/MSRP source health 0-100"},
        {"from": "hermes_evidence_writer.py", "to": "hermes_answer_audit.py", "what": "JSONL evidence → answer groundedness scoring"},
        {"from": "hermes_cost_report.py", "to": "developer", "what": "audit records + pricing → budget tracking"},
        {"from": "hermes API (/v1/hermes/*)", "to": "DataManagementPage", "what": "JSON → UI dashboard"},
        {"from": "systemd timers", "to": "hermes_pipeline_audit.py", "what": "pipeline runtime data → health report"},
        {"from": "VOC fetcher", "to": "hermes_source_quality.py", "what": "runtime errors → source quality scores"},
        {"from": "Country Copilot", "to": "hermes_answer_audit.py", "what": "answers → answer audit records"},
    ]

    # Work routing guide
    routing = [
        {"task": "我要写新功能 PRD", "ask": "Hermes Code Governor (Phase 2)", "run": "python 03_Scripts/hermes/hermes_intake.py prd.md", "gets": "影响分析报告 + Claude Code 开发 brief"},
        {"task": "Claude Code 刚改完代码", "ask": "Hermes Code Governor (Phase 3)", "run": "python 03_Scripts/hermes/hermes_code_audit.py --base main --head HEAD", "gets": "diff 风险报告（secret/registry/schema/schedule）"},
        {"task": "我想看所有 pipeline 健康状态", "ask": "Hermes Pipeline Governor (Phase 4)", "run": "python 03_Scripts/hermes/hermes_pipeline_audit.py", "gets": "管道健康报告 + pipeline_health.json"},
        {"task": "VOC 源抓取质量如何", "ask": "Hermes Intelligence Governor (Phase 5)", "run": "python 03_Scripts/hermes/hermes_source_quality.py", "gets": "源质量评分表（healthy/watch/degraded）"},
        {"task": "国家助手回答有没有幻觉", "ask": "Hermes Intelligence Governor (Phase 5)", "run": "python 03_Scripts/hermes/hermes_answer_audit.py --sample", "gets": "回答审计报告（groundedness/hallucination/cost）"},
        {"task": "Flash/Pro 花了多少钱", "ask": "Hermes Intelligence Governor (Phase 5.5)", "run": "python 03_Scripts/hermes/hermes_cost_report.py", "gets": "成本报告（按模型/模式分拆 + 预算追踪）"},
        {"task": "系统里到底注册了哪些功能/源/管道", "ask": "Hermes Knowledge Governor (Phase 1)", "run": "cat hermes/{feature,pipeline,source}_registry.yaml", "gets": "YAML 总账本"},
        {"task": "部署一直失败是为什么", "ask": "Hermes Code Governor (Phase 3)", "run": "python 03_Scripts/hermes/hermes_code_audit.py + 看 GitHub Actions logs", "gets": "部署失败诊断报告"},
        {"task": "在 Data Management UI 看 Hermes", "ask": "Hermes Phase 6", "run": "访问 /data-management → Hermes Governance 标签", "gets": "可视化治理看板"},
    ]

    return {
        "modules": modules,
        "dependencies": deps,
        "routing": routing,
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
