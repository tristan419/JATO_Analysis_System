"""Hermes Chat Gateway — Intent Router & Chat Logic.

Rules-based intent classification (zero LLM cost) with entity extraction
and structured answer generation from existing Hermes data.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ── Intent definitions ───────────────────────────────────────────────

INTENT_DEFS: list[dict[str, Any]] = [
    {"intent": "system_status_query", "executionMode": "direct_answer",
     "keywords": ["status", "health", "overview", "summary", "状态", "概况",
                  "how is", "what is the status", "system status"],
     "dataEndpoints": ["/hermes/overview", "/hermes/daily-summary"]},
    {"intent": "run_status_query", "executionMode": "direct_answer",
     "keywords": ["run", "runs", "recent", "activity", "history", "last",
                  "运行", "最近", "活动", "历史", "what ran", "recent activity"],
     "dataEndpoints": ["/hermes/activity-heatmap"]},
    {"intent": "evidence_query", "executionMode": "direct_answer",
     "keywords": ["evidence", "ledger", "fact", "facts", "记录", "证据",
                  "show evidence", "what evidence", "evidence ledger"],
     "dataEndpoints": ["/hermes/evidence-ledger"]},
    {"intent": "gap_query", "executionMode": "direct_answer",
     "keywords": ["gap", "gaps", "governance", "issue", "issues", "漏洞", "问题", "治理",
                  "open gap", "show gap", "what gap"],
     "dataEndpoints": ["/hermes/gaps"]},
    {"intent": "cost_query", "executionMode": "direct_answer",
     "keywords": ["cost", "costs", "budget", "spend", "spending", "price",
                  "费用", "成本", "预算", "how much", "cost status"],
     "dataEndpoints": ["/hermes/cost-heatmap", "/hermes/cost"]},
    {"intent": "pipeline_query", "executionMode": "direct_answer",
     "keywords": ["pipeline", "pipelines", "schedule", "scheduling",
                  "管道", "调度", "pipeline health", "show pipeline"],
     "dataEndpoints": ["/hermes/pipeline-health"]},
    {"intent": "source_query", "executionMode": "direct_answer",
     "keywords": ["source", "sources", "data source", "feed", "爬虫", "源",
                  "source quality", "show source"],
     "dataEndpoints": ["/hermes/source-quality"]},
    {"intent": "diagram_query", "executionMode": "direct_answer",
     "keywords": ["diagram", "diagrams", "flowchart", "mermaid", "流程图",
                  "show diagram", "list diagram"],
     "dataEndpoints": ["/hermes/markdown-diagrams"]},
    {"intent": "feature_query", "executionMode": "direct_answer",
     "keywords": ["feature", "features", "kanban", "功能", "planned",
                  "show feature", "what feature"],
     "dataEndpoints": ["/hermes/feature-kanban", "/hermes/dev/features"]},
    {"intent": "source_audit", "executionMode": "create_run",
     "keywords": ["source audit", "audit source", "check source", "源审计", "检查源",
                  "scan source", "refresh source", "源刷新"],
     "command": "source-quality"},
    {"intent": "pipeline_audit", "executionMode": "create_run",
     "keywords": ["pipeline audit", "audit pipeline", "scan pipeline",
                  "管道审计", "检查管道", "pipeline scan"],
     "command": "pipeline-audit"},
    {"intent": "cost_refresh", "executionMode": "create_run",
     "keywords": ["cost report", "refresh cost", "update cost", "费用报告",
                  "recalculate cost", "cost refresh"],
     "command": "cost-report"},
    {"intent": "evidence_refresh", "executionMode": "create_run",
     "keywords": ["refresh evidence", "update evidence", "证据刷新",
                  "extract evidence", "证据提取"],
     "command": "evidence"},
    {"intent": "code_audit", "executionMode": "create_run",
     "keywords": ["code audit", "audit code", "代码审计", "scan code",
                  "git diff audit", "security scan"],
     "command": "code-audit"},
    {"intent": "dev_request", "executionMode": "blocked_by_policy",
     "keywords": ["deploy", "push", "commit", "merge", "delete", "改代码", "部署", "发布", "删除"]},
]

ENTITY_PATTERNS: dict[str, re.Pattern] = {
    "country": re.compile(
        r"\b(Sweden|Swedish|SE|Norway|Norwegian|NO|Finland|Finnish|FI|"
        r"Denmark|Danish|DK|Germany|German|DE|Spain|Spanish|ES|"
        r"Austria|AT|Czech|CZ|Croatia|HR|Hungary|HU|"
        r"瑞典|挪威|芬兰|丹麦|德国|西班牙|奥地利|捷克|克罗地亚|匈牙利)\b",
        re.IGNORECASE),
    "model": re.compile(
        r"\b(J\d+|JAECOO\s*\d+|OMODA\s*\d+|EXEED|XC\d+|GLC|C-Class|ID\.\d+|BEV|PHEV|HEV|ICE|MHEV)\b",
        re.IGNORECASE),
}

COUNTRY_MAP: dict[str, str] = {
    "sweden": "Sweden", "swedish": "Sweden", "se": "Sweden", "瑞典": "Sweden",
    "norway": "Norway", "norwegian": "Norway", "no": "Norway", "挪威": "Norway",
    "finland": "Finland", "finnish": "Finland", "fi": "Finland", "芬兰": "Finland",
    "denmark": "Denmark", "danish": "Denmark", "dk": "Denmark", "丹麦": "Denmark",
    "germany": "Germany", "german": "Germany", "de": "Germany", "德国": "Germany",
    "spain": "Spain", "spanish": "Spain", "es": "Spain", "西班牙": "Spain",
    "austria": "Austria", "at": "Austria", "奥地利": "Austria",
    "czech": "Czech", "cz": "Czech", "捷克": "Czech",
    "croatia": "Croatia", "hr": "Croatia", "克罗地亚": "Croatia",
    "hungary": "Hungary", "hu": "Hungary", "匈牙利": "Hungary",
}


class HermesIntentRouter:
    def classify(self, message: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        msg_lower = message.lower().strip()
        best: dict[str, Any] = {"intent": "unknown", "executionMode": "clarification_needed", "confidence": 0.0}
        for intent_def in INTENT_DEFS:
            score = 0.0
            for kw in intent_def.get("keywords", []):
                if kw.lower() in msg_lower:
                    score += len(kw) * 0.05
            score = min(score, 1.0)
            if score > best["confidence"]:
                best = {"intent": intent_def["intent"], "executionMode": intent_def["executionMode"], "confidence": score}
        entities = self._extract_entities(message)
        result = {"intent": best["intent"], "executionMode": best["executionMode"],
                  "confidence": best["confidence"], "entities": entities}
        if result["confidence"] < 0.15:
            result["intent"] = "unknown"
            result["executionMode"] = "clarification_needed"
            result["suggestedIntents"] = self._suggest_intents(msg_lower)
        if result["intent"] == "dev_request":
            role = (context or {}).get("userRole", "user")
            if role != "developer":
                result["executionMode"] = "blocked_by_policy"
                result["blockReason"] = "Dev requests require developer role."
        return result

    def _extract_entities(self, message: str) -> dict[str, Any]:
        entities: dict[str, Any] = {}
        for entity_type, pattern in ENTITY_PATTERNS.items():
            matches = pattern.findall(message)
            if matches:
                seen: set[str] = set()
                values: list[str] = []
                for m in matches:
                    normalized = COUNTRY_MAP.get(m.lower(), m) if entity_type == "country" else m
                    if normalized not in seen:
                        seen.add(normalized)
                        values.append(normalized)
                entities[entity_type] = values
        return entities

    def _suggest_intents(self, msg: str) -> list[str]:
        suggestions: list[tuple[float, str]] = []
        for intent_def in INTENT_DEFS:
            score = 0.0
            for kw in intent_def.get("keywords", []):
                if kw.lower() in msg:
                    score += len(kw) * 0.05
            if score > 0.0:
                suggestions.append((score, intent_def["intent"]))
        suggestions.sort(reverse=True)
        return [s[1] for s in suggestions[:3]]


router = HermesIntentRouter()


# ── Direct Answer Generator ──────────────────────────────────────────

def _load_overview() -> dict[str, Any]:
    from app.api.routes.hermes import HERMES_DIR
    try:
        import yaml
        path = HERMES_DIR / "governance_gaps.yaml"
        gaps = yaml.safe_load(path.read_text()) if path.is_file() else {"gaps": []}
        open_count = sum(1 for g in gaps.get("gaps", []) if g.get("status") == "open")
        resolved_count = sum(1 for g in gaps.get("gaps", []) if g.get("status") == "resolved")
        return {"gaps": {"total": len(gaps.get("gaps", [])), "open": open_count, "resolved": resolved_count}}
    except Exception:
        return {"gaps": {"total": 0, "open": 0, "resolved": 0}}


def _load_evidence_summary(days: int = 7) -> dict[str, Any]:
    from app.api.routes.hermes import HERMES_DIR
    path = HERMES_DIR / "evidence_ledger.jsonl"
    if not path.is_file():
        return {"totalCount": 0, "recentCount": 0, "byType": {}}
    entries: list[dict] = []
    for line in path.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    recent = [e for e in entries if (e.get("createdAt") or "") >= cutoff.isoformat()]
    by_type: dict[str, int] = {}
    for e in recent:
        t = e.get("type") or "unknown"
        by_type[t] = by_type.get(t, 0) + 1
    return {"totalCount": len(entries), "recentCount": len(recent), "byType": by_type}


def _load_activity_summary(days: int = 7) -> dict[str, Any]:
    from app.api.routes.hermes import ACTIVITY_LOG
    if not ACTIVITY_LOG.is_file():
        return {"totalRuns": 0, "recentRuns": 0}
    entries: list[dict] = []
    for line in ACTIVITY_LOG.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    recent = [e for e in entries if (e.get("timestamp") or "") >= cutoff.isoformat()]
    return {"totalRuns": len(entries), "recentRuns": len(recent)}


def generate_direct_answer(intent: str, entities: dict[str, Any]) -> dict[str, Any]:
    if intent == "gap_query":
        ov = _load_overview()
        g = ov["gaps"]
        answer = f"当前有 {g['open']} 个 open governance gaps，{g['resolved']} 个已解决（共 {g['total']} 个）。"
        return {"replyType": "direct_answer", "answer": answer, "intent": intent,
                "entities": entities, "dataRefs": ["/hermes/gaps?status=open"]}
    if intent == "cost_query":
        return {"replyType": "direct_answer",
                "answer": "Cost data is available on the Cost tab (heatmap + model breakdown).",
                "intent": intent, "entities": entities,
                "dataRefs": ["/hermes/cost-heatmap", "/hermes/cost"]}
    if intent == "evidence_query":
        es = _load_evidence_summary(7)
        answer = f"过去 7 天有 {es['recentCount']} 条新 evidence 记录（总计 {es['totalCount']} 条）。"
        if es["byType"]:
            answer += " 类型分布: " + ", ".join(f"{t}: {c}" for t, c in es["byType"].items()) + "。"
        return {"replyType": "direct_answer", "answer": answer, "intent": intent,
                "entities": entities, "dataRefs": ["/hermes/evidence-ledger?days=7"]}
    if intent == "system_status_query":
        ov = _load_overview()
        ac = _load_activity_summary(7)
        answer = (f"Hermes system status: {ov['gaps']['open']} open gaps, "
                  f"{ac['recentRuns']} runs in past 7 days.")
        return {"replyType": "direct_answer", "answer": answer, "intent": intent,
                "entities": entities, "dataRefs": ["/hermes/overview"]}
    tab_map = {"pipeline_query": "Activity", "source_query": "Activity",
               "run_status_query": "Activity", "diagram_query": "Diagrams", "feature_query": "Dev"}
    data_map = {"pipeline_query": ["/hermes/pipeline-health"], "source_query": ["/hermes/source-quality"],
                "run_status_query": ["/hermes/activity-heatmap"], "diagram_query": ["/hermes/markdown-diagrams"],
                "feature_query": ["/hermes/dev/features"]}
    answer = f"This information is available on the {tab_map.get(intent, 'Activity')} tab below."
    return {"replyType": "direct_answer", "answer": answer, "intent": intent,
            "entities": entities, "dataRefs": data_map.get(intent, [])}


def create_run_response(intent: str, entities: dict[str, Any]) -> dict[str, Any]:
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    command_map = {"source_audit": "source-quality", "pipeline_audit": "pipeline-audit",
                   "cost_refresh": "cost-report", "evidence_refresh": "evidence", "code_audit": "code-audit"}
    command = command_map.get(intent, "")
    task_labels = {"source_audit": ["source_quality_scan"], "pipeline_audit": ["pipeline_health_scan"],
                   "cost_refresh": ["cost_calculation"], "evidence_refresh": ["evidence_extraction"],
                   "code_audit": ["git_diff_scan", "rule_audit"]}
    tasks = task_labels.get(intent, ["execute"])
    answer = f"已创建 {intent} 任务（Run ID: {run_id}），共 {len(tasks)} 个子任务。"
    if entities.get("country"):
        answer = f"已创建 {entities['country']} {intent} 任务（Run ID: {run_id}），共 {len(tasks)} 个子任务。"
    return {"replyType": "run_created", "answer": answer, "intent": intent, "entities": entities,
            "runId": run_id, "command": command, "tasks": tasks,
            "dataRefs": [f"/hermes/run/{command}" if command else ""],
            "suggestedActions": [{"label": "View Run", "action": "view_run", "runId": run_id},
                                 {"label": "Run Now", "action": "execute_command", "command": command}]}


# ── Session management ───────────────────────────────────────────────

SESSIONS_DIR: Path | None = None


def _get_sessions_dir() -> Path:
    global SESSIONS_DIR
    if SESSIONS_DIR is None:
        from app.api.routes.hermes import PROJECT_ROOT
        SESSIONS_DIR = PROJECT_ROOT / "hermes" / "sessions"
    return SESSIONS_DIR


def _ensure_sessions_dir() -> Path:
    d = _get_sessions_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d


def create_session() -> dict[str, Any]:
    session_id = f"session_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    now = datetime.now(timezone.utc).isoformat()
    session = {"sessionId": session_id, "createdAt": now, "updatedAt": now, "messages": []}
    _save_session(session_id, session)
    return session


def get_session(session_id: str) -> dict[str, Any] | None:
    path = _get_sessions_dir() / f"{session_id}.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def add_message(session_id: str, message: dict) -> dict[str, Any]:
    session = get_session(session_id) or create_session()
    session["messages"].append(message)
    session["updatedAt"] = datetime.now(timezone.utc).isoformat()
    _save_session(session_id, session)
    return session


def list_sessions(limit: int = 20) -> list[dict[str, Any]]:
    d = _get_sessions_dir()
    if not d.is_dir():
        return []
    sessions: list[dict] = []
    for f in sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            s = json.loads(f.read_text())
            sessions.append({"sessionId": s.get("sessionId", f.stem),
                             "createdAt": s.get("createdAt", ""),
                             "updatedAt": s.get("updatedAt", ""),
                             "messageCount": len(s.get("messages", []))})
        except Exception:
            pass
        if len(sessions) >= limit:
            break
    return sessions


def _save_session(session_id: str, session: dict) -> None:
    d = _ensure_sessions_dir()
    path = d / f"{session_id}.json"
    path.write_text(json.dumps(session, indent=2, ensure_ascii=False))


def cleanup_old_sessions(max_age_hours: int = 24) -> int:
    d = _get_sessions_dir()
    if not d.is_dir():
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    removed = 0
    for f in d.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                f.unlink()
                removed += 1
        except Exception:
            pass
    return removed
