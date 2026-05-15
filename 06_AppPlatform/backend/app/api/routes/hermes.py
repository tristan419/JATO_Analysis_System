"""Hermes Governance Layer — API routes.

Read/write Hermes JSON reports, registry files, and script execution.
"""

from __future__ import annotations

import json
import os
import smtplib
import subprocess
import sys
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

router = APIRouter(prefix="/hermes", tags=["hermes"])

# routes/hermes.py → api/routes → app/api → backend/app → 06_AppPlatform → repo_root
PROJECT_ROOT = Path(__file__).resolve().parents[5]
HERMES_DIR = PROJECT_ROOT / "hermes"
SCRIPTS_DIR = PROJECT_ROOT / "03_Scripts" / "hermes"
REPORTS_DIR = HERMES_DIR / "reports"
ACTIVITY_LOG = HERMES_DIR / "activity_log.jsonl"
BUDGET_DAILY_CNY = 20
BUDGET_MONTHLY_CNY = 500
ALERT_EMAIL = "tristanlyk@gmail.com"

HERMES_SCRIPTS = {
    "pipeline-audit": {"script": "hermes_pipeline_audit.py", "label": "Pipeline Audit", "desc": "Scan systemd/Airflow/GH Actions → health report"},
    "source-quality": {"script": "hermes_source_quality.py", "label": "Source Quality", "desc": "Score VOC/News/MSRP source health 0-100"},
    "cost-report":    {"script": "hermes_cost_report.py",    "label": "Cost Report",    "desc": "Flash/Pro cost vs 500 CNY budget"},
    "code-audit":     {"script": "hermes_code_audit.py",     "label": "Code Audit",     "desc": "git diff → 10-rule scan", "args": ["--base", "main", "--head", "HEAD"]},
    "intake":         {"script": "hermes_intake.py",         "label": "PRD Intake",      "desc": "PRD → impact report (needs --prd arg)", "args": []},
    "evidence":       {"script": "hermes_evidence_writer.py","label": "Evidence Writer", "desc": "Extract facts from artifacts → JSONL"},
    "answer-audit":   {"script": "hermes_answer_audit.py",   "label": "Answer Audit",    "desc": "Generate sample answer audits"},
}

HELP_TEXT = """
Hermes CLI — available commands:

  pipeline-audit  Scan systemd/Airflow/GH Actions → health report
  source-quality  Score VOC/News/MSRP source health 0-100
  cost-report     Flash/Pro cost vs 500 CNY budget
  code-audit      git diff → 10-rule audit scan
  intake          PRD impact analysis (needs --prd)
  evidence        Extract structured evidence → JSONL
  answer-audit    Generate sample answer audits

Usage: POST /v1/hermes/run/{command}
       GET  /v1/hermes/run/{command}/help
"""


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
    try:
        import yaml as _yaml
    except ImportError:
        return {"error": "PyYAML not installed in backend venv. Run: pip install pyyaml"}

    # Registry counts
    for name in ["source", "pipeline", "feature", "prompt", "artifact"]:
        fname = f"{name}_registry.yaml"
        path = HERMES_DIR / fname
        if not path.is_file():
            overview["registries"][name] = -2  # file not found
            continue
        try:
            data = _yaml.safe_load(path.read_text())
            key = f"{name}s" if not name.endswith("s") else name
            items = data.get(key, []) if data else []
            overview["registries"][name] = len(items) if isinstance(items, list) else 0
        except Exception as exc:
            overview["registries"][name] = -1  # parse error

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
            data = _yaml.safe_load(prop_path.read_text())
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
            data = _yaml.safe_load(gaps_path.read_text())
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


# ── Script Execution ──────────────────────────────────────────────

@router.get("/run/{command}/help")
def hermes_run_help(command: str):
    """Return help for a specific Hermes command."""
    if command == "all":
        return PlainTextResponse(HELP_TEXT)
    info = HERMES_SCRIPTS.get(command)
    if not info:
        raise HTTPException(404, f"Unknown command: {command}. Try: {', '.join(HERMES_SCRIPTS)}")
    return {
        "command": command,
        "script": info["script"],
        "label": info["label"],
        "desc": info["desc"],
        "defaultArgs": info.get("args", []),
    }


@router.post("/run/{command}")
def hermes_run(command: str):
    """Execute a Hermes script and return its output."""
    if command not in HERMES_SCRIPTS:
        raise HTTPException(400, f"Unknown command: {command}. Available: {', '.join(HERMES_SCRIPTS)}")

    info = HERMES_SCRIPTS[command]
    script_path = SCRIPTS_DIR / info["script"]
    if not script_path.is_file():
        raise HTTPException(500, f"Script not found: {script_path}")

    args = info.get("args", [])
    cmd = [sys.executable, str(script_path)] + list(args)

    started_at = datetime.now(timezone.utc).isoformat()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True, text=True, timeout=120,
        )
        # Log activity
        _log_activity(command, info["script"], result.returncode, started_at)
        return {
            "command": command,
            "script": info["script"],
            "exitCode": result.returncode,
            "stdout": result.stdout[-8000:],
            "stderr": result.stderr[-2000:],
            "status": "success" if result.returncode == 0 else "failed",
        }
    except subprocess.TimeoutExpired:
        return {"command": command, "exitCode": -1, "stdout": "", "stderr": "Timeout after 120s", "status": "timeout"}
    except Exception as exc:
        return {"command": command, "exitCode": -1, "stdout": "", "stderr": str(exc), "status": "error"}


@router.get("/run")
def hermes_list_commands():
    """List all available Hermes run commands."""
    return {
        "commands": {
            cmd: {"label": info["label"], "desc": info["desc"], "hasDefaultArgs": bool(info.get("args"))}
            for cmd, info in HERMES_SCRIPTS.items()
        }
    }


# ── Source Drill-down ─────────────────────────────────────────────

@router.get("/source/{source_id}")
def hermes_source_detail(source_id: str) -> dict:
    """Return full detail for a single source including linked evidence."""
    path = HERMES_DIR / "source_registry.yaml"
    if not path.is_file():
        raise HTTPException(404, "Source registry not found")
    import yaml
    data = yaml.safe_load(path.read_text())
    sources = data.get("sources", []) if data else []
    source = next((s for s in sources if s.get("sourceId") == source_id), None)
    if not source:
        raise HTTPException(404, f"Source not found: {source_id}")

    # Linked evidence from ledger
    evidence: list[dict] = []
    ev_path = HERMES_DIR / "evidence_ledger.jsonl"
    if ev_path.is_file():
        for line in ev_path.read_text().strip().split("\n"):
            if line.strip():
                try:
                    rec = json.loads(line)
                    if source_id in str(rec.get("sourceRef", "")) or source_id in str(rec.get("artifactId", "")):
                        evidence.append(rec)
                except Exception:
                    pass

    # Find producing/consuming pipelines
    pipe_path = HERMES_DIR / "pipeline_registry.yaml"
    pipelines: list[dict] = []
    if pipe_path.is_file():
        pipe_data = yaml.safe_load(pipe_path.read_text())
        for p in (pipe_data.get("pipelines", []) if pipe_data else []):
            for out in (p.get("outputs", []) or []):
                if source_id in str(out) or source.get("name","") in str(out):
                    pipelines.append({"pipelineId": p.get("pipelineId"), "name": p.get("name"), "type": p.get("type")})
                    break

    return {
        "source": source,
        "linkedEvidence": evidence,
        "linkedEvidenceCount": len(evidence),
        "linkedPipelines": pipelines,
    }


@router.get("/source/{source_id}/health-history")
def hermes_source_health_history(source_id: str) -> dict:
    """Return health history for a source (from status JSON and quality report)."""
    sq = _read_json(REPORTS_DIR / "source_quality_report.json")
    source_score = None
    if sq:
        for s in sq.get("sources", []):
            if s.get("sourceId") == source_id:
                source_score = s
                break

    status = _read_json(PROJECT_ROOT / "03_Scripts" / "logs" / "scheduled_fetch_status.json")

    return {
        "sourceId": source_id,
        "qualityScore": source_score,
        "fetchStatus": status.get("voc", {}) if status else {},
    }


# ── Activity & Cost Heatmap ────────────────────────────────────────

def _log_activity(command: str, script: str, exit_code: int, started_at: str) -> None:
    """Append an activity record to the activity log."""
    record = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "command": command,
        "script": script,
        "exitCode": exit_code,
        "startedAt": started_at,
    }
    try:
        ACTIVITY_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(ACTIVITY_LOG, "a") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _send_budget_alert(subject: str, body: str) -> bool:
    """Try to send a budget alert email. Returns True if sent."""
    smtp_host = os.getenv("HERMES_SMTP_HOST", "")
    smtp_port = int(os.getenv("HERMES_SMTP_PORT", "587"))
    smtp_user = os.getenv("HERMES_SMTP_USER", "")
    smtp_pass = os.getenv("HERMES_SMTP_PASS", "")
    if not smtp_host or not smtp_user:
        return False
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[Hermes Budget Alert] {subject}"
        msg["From"] = smtp_user
        msg["To"] = ALERT_EMAIL
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        return True
    except Exception:
        return False


@router.get("/activity-heatmap")
def hermes_activity_heatmap(days: int = 30) -> dict:
    """Return Hermes activity data for heatmap visualization."""
    records: list[dict] = []
    if ACTIVITY_LOG.is_file():
        for line in ACTIVITY_LOG.read_text().strip().split("\n"):
            if line.strip():
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass

    from collections import Counter
    date_counts = Counter(r["timestamp"][:10] for r in records)
    command_counts = Counter(r["command"] for r in records)

    # Build daily grid for last N days
    today = datetime.now(timezone.utc).date()
    days_list: list[dict] = []
    for i in range(days):
        d = today.replace(day=1) if False else today
        from datetime import timedelta
        d = today - timedelta(days=days - 1 - i)
        ds = d.strftime("%Y-%m-%d")
        days_list.append({"date": ds, "count": date_counts.get(ds, 0)})

    return {
        "totalRecords": len(records),
        "days": days_list,
        "byCommand": dict(command_counts),
        "lastRun": records[-1] if records else None,
    }


@router.get("/cost-heatmap")
def hermes_cost_heatmap(days: int = 30) -> dict:
    """Return daily cost data for heatmap visualization."""
    audit_path = HERMES_DIR / "answer_audit.jsonl"
    daily_costs: dict[str, float] = {}
    by_model: dict[str, float] = {}

    if audit_path.is_file():
        for line in audit_path.read_text().strip().split("\n"):
            if line.strip():
                try:
                    rec = json.loads(line)
                    date = rec.get("createdAt", "")[:10]
                    model = rec.get("modelUsed", "unknown")
                    input_t = rec.get("inputTokens", 0) or 0
                    output_t = rec.get("outputTokens", 0) or 0
                    # Flash pricing
                    cost = (input_t / 1_000_000) * 1.0 + (output_t / 1_000_000) * 2.0
                    if "pro" in model:
                        cost = (input_t / 1_000_000) * 3.0 + (output_t / 1_000_000) * 6.0
                    daily_costs[date] = daily_costs.get(date, 0) + cost
                    by_model[model] = by_model.get(model, 0) + cost
                except Exception:
                    pass

    from datetime import timedelta
    today = datetime.now(timezone.utc).date()
    days_list: list[dict] = []
    total = 0.0
    for i in range(days):
        d = today - timedelta(days=days - 1 - i)
        ds = d.strftime("%Y-%m-%d")
        cost = round(daily_costs.get(ds, 0), 4)
        total += cost
        over_daily = cost > BUDGET_DAILY_CNY
        days_list.append({"date": ds, "costCny": cost, "overDailyBudget": over_daily})

    over_monthly = total > BUDGET_MONTHLY_CNY
    monthly_status = "ok"
    alerts: list[str] = []
    if over_monthly:
        monthly_status = "exceeded"
        alerts.append(f"Monthly cost {total:.2f} CNY exceeds {BUDGET_MONTHLY_CNY} CNY budget")
    elif total > BUDGET_MONTHLY_CNY * 0.75:
        monthly_status = "warning"
        alerts.append(f"Monthly cost {total:.2f} CNY at {total/BUDGET_MONTHLY_CNY*100:.0f}% of {BUDGET_MONTHLY_CNY} CNY budget")

    # Send email alert if budget exceeded
    if alerts:
        body = f"Hermes Cost Alert\n\nMonthly total: {total:.2f} CNY / {BUDGET_MONTHLY_CNY} CNY\nDaily budget: {BUDGET_DAILY_CNY} CNY\n\nAlerts:\n"
        body += "\n".join(f"- {a}" for a in alerts)
        body += f"\n\nView: https://www.ojeur.cloud/data-management → Hermes Governance"
        email_sent = _send_budget_alert("Budget Alert", body)
    else:
        email_sent = False

    return {
        "days": days_list,
        "totalCny": round(total, 4),
        "dailyBudgetCny": BUDGET_DAILY_CNY,
        "monthlyBudgetCny": BUDGET_MONTHLY_CNY,
        "monthlyStatus": monthly_status,
        "byModelCny": {k: round(v, 4) for k, v in by_model.items()},
        "alerts": alerts,
        "emailSent": email_sent,
        "alertEmail": ALERT_EMAIL,
    }


@router.get("/daily-summary")
def hermes_daily_summary() -> dict:
    """Return a combined activity+cost summary for today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Activity today
    activity_today = 0
    if ACTIVITY_LOG.is_file():
        for line in ACTIVITY_LOG.read_text().strip().split("\n"):
            if line.strip() and today in line:
                activity_today += 1

    # Cost today
    cost_cny = 0.0
    audit_path = HERMES_DIR / "answer_audit.jsonl"
    if audit_path.is_file():
        for line in audit_path.read_text().strip().split("\n"):
            if line.strip() and today in line:
                try:
                    rec = json.loads(line)
                    input_t = rec.get("inputTokens", 0) or 0
                    output_t = rec.get("outputTokens", 0) or 0
                    cost_cny += (input_t / 1_000_000) * 1.0 + (output_t / 1_000_000) * 2.0
                except Exception:
                    pass

    return {
        "date": today,
        "activityCount": activity_today,
        "costCny": round(cost_cny, 4),
        "dailyBudgetCny": BUDGET_DAILY_CNY,
        "monthlyBudgetCny": BUDGET_MONTHLY_CNY,
        "costStatus": "ok" if cost_cny <= BUDGET_DAILY_CNY else "over_daily",
    }


# ── Feature Kanban ─────────────────────────────────────────────────

@router.get("/feature-kanban")
def hermes_feature_kanban() -> dict:
    """Return features grouped by implementation status for kanban visualization."""
    path = HERMES_DIR / "feature_registry.yaml"
    if not path.is_file():
        raise HTTPException(404, "Feature registry not found")
    import yaml
    data = yaml.safe_load(path.read_text())
    features = data.get("features", []) if data else []

    columns: dict[str, list[dict]] = {
        "planned": [],
        "beta": [],
        "active": [],
        "archived": [],
    }

    COLORS: dict[str, str] = {
        "active": "#22c55e",
        "beta": "#3b82f6",
        "archived": "#94a3b8",
        "planned": "#f59e0b",
    }

    # Phase mapping: infer phase from implementation status
    def _infer_phase(f: dict) -> str:
        status = f.get("status", "")
        impl = f.get("implementationStatus", "")
        notes = str(f.get("notes", "") or "")
        if impl == "implemented":
            return "Phase 6 — Deployed"
        if impl == "partial":
            if status == "beta":
                return "Phase 3-4 — Beta testing"
            return "Phase 5 — Integration"
        if status == "planned":
            return "Phase 2 — PRD ready"
        if status == "archived":
            return "Archived"
        if impl == "prd_only":
            return "Phase 2 — PRD only"
        if any(k in notes.lower() for k in ["phase 0", "phase 1", "phase 2", "phase 3", "phase 4", "phase 5"]):
            for kw in ["phase 0", "phase 1", "phase 2", "phase 3", "phase 4", "phase 5", "phase 6"]:
                if kw in notes.lower():
                    return kw.capitalize()
        return "Phase 2 — Defined"

    for f in features:
        status = f.get("status", "unknown")
        impl = f.get("implementationStatus", "unknown")
        column = status if status in columns else "active"
        columns[column].append({
            "featureId": f.get("featureId", "?"),
            "name": f.get("name", "?"),
            "status": status,
            "implementationStatus": impl,
            "phase": _infer_phase(f),
            "riskLevel": f.get("riskLevel", "low"),
            "routes": f.get("routes", []),
            "backendApis": (f.get("backendApis", []) or [])[:3],
            "tests": f.get("tests", []),
            "docs": f.get("docs", []),
            "knownIssues": f.get("knownIssues", []),
            "governanceStatus": f.get("governanceStatus", "unmanaged"),
            "color": COLORS.get(status, "#94a3b8"),
        })

    return {
        "columns": {
            "planned":  {"label": "Planned",  "color": "#f59e0b", "features": columns["planned"]},
            "beta":     {"label": "Beta",     "color": "#3b82f6", "features": columns["beta"]},
            "active":   {"label": "Active",   "color": "#22c55e", "features": columns["active"]},
            "archived": {"label": "Archived", "color": "#94a3b8", "features": columns["archived"]},
        },
        "summary": {
            "total": len(features),
            "active": len(columns["active"]),
            "beta": len(columns["beta"]),
            "planned": len(columns["planned"]),
            "archived": len(columns["archived"]),
            "withTests": sum(1 for f in features if f.get("tests")),
            "withDocs": sum(1 for f in features if f.get("docs")),
            "withIssues": sum(1 for f in features if f.get("knownIssues")),
        },
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
