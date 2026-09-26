from __future__ import annotations

import ast
import json
import re
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import quote_plus
from urllib.parse import urlparse

from app.services.country_chat_service import answer_country_question
from app.services import engineering_variant_diff_service
from app.services.jato_agent_llm_judge_service import judge_side_by_side_with_llm
from app.services.jato_agent_llm_judge_service import list_reference_judge_paths
from app.services.jato_business_playbook_service import FAILURE_TAGS
from app.services.jato_business_playbook_service import build_business_playbook_context
from app.services.jato_business_playbook_service import infer_business_failure_tags
from app.services.jato_evidence_package_service import is_usable_evidence_ref
from app.services.jato_followup_service import normalize_follow_ups
from app.services.jato_mcp_tools_service import call_jato_mcp_tool
from app.services.jato_tool_coverage_guard_service import tool_satisfies_required
from app.services.jato_usage_tracker import (
    estimate_tool_call_tokens,
    get_eval_usage_summary,
    track_eval_run,
)
from app.services.jato_visual_artifact_service import build_visual_artifacts

_EVAL_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "hermes" / "eval"
_QUESTIONS_FILE = _EVAL_DIR / "eval_questions.jsonl"
_RESULTS_FILE = _EVAL_DIR / "eval_results.jsonl"
_SIDE_BY_SIDE_FILE = _EVAL_DIR / "eval_side_by_side.jsonl"
_CODEX_REVIEW_NOTES_FILE = _EVAL_DIR / "codex_review_notes.jsonl"
_CODEX_REVIEW_ARTIFACT_DIR = (
    _EVAL_DIR.parent.parent
    / "06_AppPlatform"
    / "frontend"
    / "artifacts"
    / "astrbot-review"
)
_MAX_RESULTS = 500
_MAX_SIDE_BY_SIDE_RESULTS = 300
_HUMAN_SCORE_STATUSES = {"pending", "scored", "skipped"}
_HUMAN_SCORE_WINNERS = {"", "astrbot", "countryCopilot", "tie", "unclear"}
_HUMAN_SCORE_SOURCES = {"", "manual", "codex_review", "llm_judge"}
_REPLACEMENT_BASELINE_SOURCES = {"manual", "llm_judge"}
_DEFAULT_HUMAN_SCORE_DIMENSIONS = [
    "correctness",
    "citationQuality",
    "hallucinationRisk",
    "toolChoice",
    "answerUsefulness",
]
_BUSINESS_SCORE_DIMENSIONS = [
    "intentAccuracy",
    "toolSelection",
    "grounding",
    "pmInsight",
    "actionability",
    "artifactQuality",
    "followUpValue",
    "presentationReadiness",
]
_BUSINESS_SCORE_LABELS = {
    "intentAccuracy": "意图识别是否准确",
    "toolSelection": "工具选择是否正确",
    "grounding": "数字是否可信",
    "pmInsight": "是否有产品经理视角",
    "actionability": "是否能转成业务动作",
    "artifactQuality": "图表/表格/证据产物是否有用",
    "followUpValue": "follow-up 是否有价值",
    "presentationReadiness": "表达是否适合汇报",
}
_BUSINESS_VALIDATION_QUESTIONS: list[dict[str, Any]] = [
    {
        "id": "biz-pricing-001",
        "category": "pricing",
        "country": "Sweden",
        "question": "瑞典 J7 HEV 应该怎么定价？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "expectedFollowUpTypes": ["compare", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-pricing-002",
        "category": "pricing",
        "country": "Sweden",
        "question": "J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "expectedFollowUpTypes": ["compare", "data_check", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-pricing-003",
        "category": "pricing",
        "country": "Sweden",
        "question": "O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_vehicle_variants"],
        "expectedFollowUpTypes": ["compare", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-pricing-004",
        "category": "pricing",
        "country": "Sweden",
        "question": "O9 在瑞典 53k-55k 欧元是否合理？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "expectedFollowUpTypes": ["compare", "data_check", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-report-001",
        "category": "report_generation",
        "country": "Sweden",
        "question": "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        "expectedIntent": "report_generation",
        "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
        "expectedFollowUpTypes": ["report", "action", "data_check"],
        "difficulty": "hard",
    },
    {
        "id": "biz-compare-001",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "J7 HEV 的核心竞品是谁？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set", "query_country_snapshot"],
        "expectedFollowUpTypes": ["compare", "drilldown", "action"],
        "difficulty": "medium",
    },
    {
        "id": "biz-compare-002",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "O5 BEV 应该对标 EX30 还是 EV3？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set", "compare_vehicle_variants"],
        "expectedFollowUpTypes": ["compare", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-compare-003",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "J8 7 座四驱为什么能打 Sorento？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set", "compare_vehicle_variants"],
        "expectedFollowUpTypes": ["why", "compare", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-compare-004",
        "category": "competitor_compare",
        "country": "Sweden",
        "question": "O9 和 XC60 / EX60 的定位差异是什么？",
        "expectedIntent": "competitor_compare",
        "expectedTools": ["compare_competitive_set", "query_msrp_pricing"],
        "expectedFollowUpTypes": ["compare", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-report-002",
        "category": "report_generation",
        "country": "Sweden",
        "question": "生成 O5 BEV 对标 EX30 和 EV3 的一页竞品汇报框架。",
        "expectedIntent": "report_generation",
        "expectedTools": ["compare_vehicle_variants", "query_msrp_pricing"],
        "expectedFollowUpTypes": ["report", "compare", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-market-001",
        "category": "market_overview",
        "country": "Sweden",
        "question": "瑞典 HEV 市场为什么适合 J7？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_country_snapshot", "analyze_market_dynamics"],
        "expectedFollowUpTypes": ["why", "compare", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-market-002",
        "category": "market_overview",
        "country": "Sweden",
        "question": "瑞典和芬兰销量差异为什么大？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_cross_country"],
        "expectedFollowUpTypes": ["compare", "why", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-market-003",
        "category": "market_overview",
        "country": "Sweden",
        "question": "北欧 BEV 增长是否会压缩 HEV 空间？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_cross_country"],
        "expectedFollowUpTypes": ["drilldown", "why", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-market-004",
        "category": "market_overview",
        "country": "Sweden",
        "question": "SUV A0/A 级为什么是主销结构？",
        "expectedIntent": "market_overview",
        "expectedTools": ["query_country_snapshot", "query_segment_breakdown"],
        "expectedFollowUpTypes": ["drilldown", "compare", "action"],
        "difficulty": "medium",
    },
    {
        "id": "biz-report-003",
        "category": "report_generation",
        "country": "Sweden",
        "question": "把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        "expectedIntent": "report_generation",
        "expectedTools": ["query_country_snapshot", "build_market_chart"],
        "expectedFollowUpTypes": ["report", "action", "drilldown"],
        "difficulty": "hard",
    },
    {
        "id": "biz-policy-001",
        "category": "policy_news",
        "country": "Sweden",
        "question": "Elbilspremien 2026 会影响哪些车型？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["search_market_news", "pageindex_search_documents"],
        "expectedFollowUpTypes": ["external_search", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-policy-002",
        "category": "policy_news",
        "country": "Sweden",
        "question": "瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["search_market_news", "query_country_snapshot"],
        "expectedFollowUpTypes": ["why", "compare", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-policy-003",
        "category": "policy_news",
        "country": "Sweden",
        "question": "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["search_market_news", "pageindex_search_documents"],
        "expectedFollowUpTypes": ["external_search", "action", "data_check"],
        "difficulty": "hard",
    },
    {
        "id": "biz-policy-004",
        "category": "policy_news",
        "country": "Sweden",
        "question": "BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        "expectedIntent": "news_policy_search",
        "expectedTools": ["search_market_news", "query_msrp_pricing"],
        "expectedFollowUpTypes": ["external_search", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-policy-005",
        "category": "policy_news",
        "country": "Sweden",
        "question": "大客户 leasing 场景下，PHEV 还有没有理由？",
        "expectedIntent": "pricing_analysis",
        "expectedTools": ["query_msrp_pricing", "search_market_news", "query_country_snapshot", "build_market_chart"],
        "expectedFollowUpTypes": ["compare", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-config-001",
        "category": "configuration",
        "country": "Sweden",
        "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants", "query_country_snapshot"],
        "expectedFollowUpTypes": ["compare", "why", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-config-002",
        "category": "configuration",
        "country": "Sweden",
        "question": "4.7m A-SUV 为什么要 95kWh + 双电机 + 800V？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["compare_vehicle_variants", "compare_competitive_set"],
        "expectedFollowUpTypes": ["compare", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-config-003",
        "category": "configuration",
        "country": "Sweden",
        "question": "北欧市场冬季包应该包含什么？",
        "expectedIntent": "configuration_analysis",
        "expectedTools": ["query_cross_country", "compare_vehicle_variants", "search_market_news", "build_market_chart"],
        "expectedFollowUpTypes": ["compare", "action", "data_check"],
        "difficulty": "medium",
    },
    {
        "id": "biz-voc-001",
        "category": "voc",
        "country": "Sweden",
        "question": "瑞典用户会不会把 V2H 当成真实购买卖点？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research", "search_market_news"],
        "expectedFollowUpTypes": ["why", "action", "data_check"],
        "difficulty": "medium",
    },
    {
        "id": "biz-voc-002",
        "category": "voc",
        "country": "Sweden",
        "question": "拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research", "search_market_news"],
        "expectedFollowUpTypes": ["compare", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-bom-001",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "OMODA9 一个版型多个物料号应该怎么解释？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_country_snapshot", "query_with_filters"],
        "expectedFollowUpTypes": ["drilldown", "data_check", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-bom-002",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "BOM、车型版本、内外饰颜色之间应该怎么建模？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_country_snapshot", "query_with_filters"],
        "expectedFollowUpTypes": ["drilldown", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-bom-003",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "当月选品表如何从物料号转成客户可编辑数量？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_country_snapshot", "query_with_filters"],
        "expectedFollowUpTypes": ["drilldown", "action", "report"],
        "difficulty": "hard",
    },
    {
        "id": "biz-bom-004",
        "category": "inventory_bom",
        "country": "Sweden",
        "question": "SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？",
        "expectedIntent": "inventory_analysis",
        "expectedTools": ["query_country_snapshot", "query_with_filters"],
        "expectedFollowUpTypes": ["compare", "data_check", "action"],
        "difficulty": "hard",
    },
    {
        "id": "biz-voc-003",
        "category": "voc",
        "country": "Sweden",
        "question": "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        "expectedIntent": "voc_analysis",
        "expectedTools": ["external_research", "search_market_news"],
        "expectedFollowUpTypes": ["why", "action", "report"],
        "difficulty": "hard",
    },
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_eval_id() -> str:
    return f"evalr_{uuid.uuid4().hex[:10]}"


def _generate_side_by_side_id() -> str:
    return f"evalcmp_{uuid.uuid4().hex[:10]}"


# ── Question loading ──


def load_eval_questions() -> dict[str, Any]:
    if not _QUESTIONS_FILE.exists():
        return {"items": [], "total": 0, "byCategory": {}}

    items: list[dict[str, Any]] = []
    by_category: dict[str, int] = {}
    with open(_QUESTIONS_FILE, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                item = json.loads(stripped)
                items.append(item)
                cat = str(item.get("category") or "unknown")
                by_category[cat] = by_category.get(cat, 0) + 1
            except json.JSONDecodeError:
                continue

    return {"items": items, "total": len(items), "byCategory": by_category}


def load_business_validation_questions() -> dict[str, Any]:
    items = [dict(item) for item in _BUSINESS_VALIDATION_QUESTIONS]
    by_category: dict[str, int] = {}
    for item in items:
        category = str(item.get("category") or "unknown")
        by_category[category] = by_category.get(category, 0) + 1
    return {
        "items": items,
        "total": len(items),
        "byCategory": by_category,
        "scoreDimensions": _business_score_dimensions(),
    }


def _find_eval_question(question_id: str) -> dict[str, Any]:
    questions = load_eval_questions()["items"]
    for question_def in questions:
        if question_def.get("id") == question_id:
            return question_def
    raise ValueError(f"Eval question not found: {question_id}")


def _find_business_question(question_id: str) -> dict[str, Any]:
    for question_def in _BUSINESS_VALIDATION_QUESTIONS:
        if question_def.get("id") == question_id:
            return dict(question_def)
    raise ValueError(f"Business validation question not found: {question_id}")


# ── Eval execution ──


def run_eval_question(question_id: str) -> dict[str, Any]:
    """Run a single eval question through the JATO agent and auto-score."""
    questions = load_eval_questions()["items"]
    question_def = None
    for q in questions:
        if q.get("id") == question_id:
            question_def = q
            break

    if question_def is None:
        raise ValueError(f"Eval question not found: {question_id}")

    country = str(question_def.get("country") or "Sweden")
    question_text = str(question_def.get("question") or "")

    try:
        result = call_jato_mcp_tool("route_agent_request", {
            "country": country,
            "question": question_text,
        })
    except Exception as exc:
        return _failed_eval_record(question_def, str(exc))

    scores = _auto_score(question_def, result)
    record = _build_eval_record(question_def, result, scores)

    # ── Usage tracking ──
    try:
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        tool_plan = data.get("retrievalToolPlan") or data.get("toolPlan") or {}
        steps = tool_plan.get("steps", []) if isinstance(tool_plan, dict) else []
        executed_steps = [s for s in steps if isinstance(s, dict) and s.get("executed")]
        tool_name = record.get("actualTool", "unknown")

        token_est = estimate_tool_call_tokens(tool_name, {"country": country, "question": question_text}, result)
        track_eval_run(
            eval_id=record["evalId"],
            question_id=question_def["id"],
            category=question_def.get("category", "unknown"),
            country=country,
            question=question_text,
            tool_calls=executed_steps if executed_steps else [{"tool": tool_name}],
            total_input_tokens=token_est["inputTokens"],
            total_output_tokens=token_est["outputTokens"],
            scores=scores,
        )
    except Exception:
        pass  # usage tracking is best-effort

    _append_result(record)
    return record


def run_eval_side_by_side_question(question_id: str) -> dict[str, Any]:
    """Run one fixed eval question through AstrBot and CountryCopilot.

    The record is intentionally separate from single-sided eval results because
    it is meant for human side-by-side scoring before any /copilot default-entry
    decision.
    """
    question_def = _find_eval_question(question_id)
    return _run_side_by_side_question_def(question_def, validation_type="eval")


def run_business_validation_question(question_id: str) -> dict[str, Any]:
    question_def = _find_business_question(question_id)
    return _run_side_by_side_question_def(question_def, validation_type="business")


def run_business_validation_category(category: str, limit: int = 5) -> dict[str, Any]:
    matched = [
        dict(question)
        for question in _BUSINESS_VALIDATION_QUESTIONS
        if question.get("category") == category
    ][: max(1, min(limit, 10))]
    results = [_safe_run_business_validation_question(question) for question in matched]
    return {
        "category": category,
        "total": len(results),
        "results": results,
        "summary": _summarize_side_by_side_results(results),
        "markdown": _build_business_validation_markdown(results),
    }


def run_business_validation_all(limit: int = 30) -> dict[str, Any]:
    matched = [dict(question) for question in _BUSINESS_VALIDATION_QUESTIONS[: max(1, min(limit, 30))]]
    results = [_safe_run_business_validation_question(question) for question in matched]
    summary = _summarize_side_by_side_results(results)
    return {
        "total": len(results),
        "results": results,
        "summary": summary,
        "markdown": _build_business_validation_markdown(results),
    }


def run_business_validation_judge_existing(
    *,
    category: str | None = None,
    limit: int = 30,
    latest_per_question: bool = True,
    score_ready_only: bool = False,
) -> dict[str, Any]:
    """Run the formal GPT judge against existing Business Validation records.

    This does not rerun AstrBot or CountryCopilot. It only turns already stored
    side-by-side records into replacement-baseline scores when the configured
    judge provider returns a complete 1-5 scoring packet.
    """
    records = _read_side_by_side_results()
    candidate_indexes = _business_judge_existing_candidate_indexes(
        records,
        category=category,
        latest_per_question=latest_per_question,
        score_ready_only=score_ready_only,
    )
    bounded_limit = max(1, min(limit, 30))
    selected_indexes = candidate_indexes[:bounded_limit]
    results: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    saved_count = 0
    failed_count = 0
    modified = False

    for position, record_index in enumerate(selected_indexes):
        record = records[record_index]
        judge_record = _enrich_business_record_for_read(record)
        judge_result = judge_side_by_side_with_llm(
            record=judge_record,
            score_dimensions=list(_BUSINESS_SCORE_DIMENSIONS),
            failure_taxonomy=list(FAILURE_TAGS),
        )
        status = str(judge_result.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        result_item = {
            "comparisonId": record.get("comparisonId", ""),
            "questionId": record.get("questionId", ""),
            "category": record.get("category", ""),
            "status": status,
            "reason": str(judge_result.get("reason") or ""),
            "saved": False,
        }
        if status != "ok":
            failed_count += 1
            results.append(result_item)
            if status in {"disabled", "missing_key"}:
                skipped_after_provider_stop = len(selected_indexes) - position - 1
                if skipped_after_provider_stop > 0:
                    status_counts["provider_not_ready_skipped"] = (
                        status_counts.get("provider_not_ready_skipped", 0)
                        + skipped_after_provider_stop
                    )
                break
            continue

        applied = _apply_business_llm_judge_result(
            record,
            judge_result,
            store_unsuccessful=True,
        )
        modified = True
        if not applied:
            failed_count += 1
            result_item["status"] = "incomplete_scores"
            result_item["reason"] = "Judge returned ok, but the score packet did not complete all required dimensions."
            status_counts["incomplete_scores"] = status_counts.get("incomplete_scores", 0) + 1
            results.append(result_item)
            continue

        record["businessValidation"] = _business_validation_projection(record)
        _apply_side_by_side_schema(record)
        saved_count += 1
        result_item["saved"] = True
        result_item["winner"] = record.get("humanScoring", {}).get("winner", "")
        result_item["astrbotScore"] = record.get("humanScoring", {}).get("scoreTotals", {}).get("astrbot", 0)
        result_item["countryCopilotScore"] = record.get("humanScoring", {}).get("scoreTotals", {}).get("countryCopilot", 0)
        results.append(result_item)

    if modified:
        _write_side_by_side_results(records)

    skipped_count = max(0, len(candidate_indexes) - len(results))
    report = get_business_validation_report(
        category=category,
        limit=100,
        latest_per_question=latest_per_question,
    )
    if saved_count:
        status = "scored"
    elif not candidate_indexes:
        status = "no_candidates"
    elif any(item.get("status") in {"disabled", "missing_key"} for item in results):
        status = "provider_not_ready"
    else:
        status = "no_scores_saved"
    return {
        "status": status,
        "category": category or "",
        "limit": bounded_limit,
        "latestPerQuestion": latest_per_question,
        "scoreReadyOnly": score_ready_only,
        "totalRecords": len(records),
        "candidateCount": len(candidate_indexes),
        "selectedCount": len(selected_indexes),
        "attemptedCount": len(results),
        "judgedCount": saved_count,
        "savedCount": saved_count,
        "failedCount": failed_count,
        "skippedCount": skipped_count,
        "statusCounts": dict(sorted(status_counts.items())),
        "results": results,
        "summary": report.get("summary", {}),
    }


def get_business_validation_report(
    category: str | None = None,
    limit: int = 100,
    latest_per_question: bool = True,
) -> dict[str, Any]:
    records = _business_side_by_side_records(category=category)
    if latest_per_question:
        records = _latest_records_per_question(records)
    page = records[: max(1, min(limit, 100))]
    evidence_repair_queue = _build_evidence_repair_queue(records)
    source_repair_backlog = _build_source_repair_backlog(evidence_repair_queue)
    summary = _with_repair_gap_summary(
        _summarize_side_by_side_results(records),
        evidence_repair_queue,
    )
    summary["sourceRepairBacklogCount"] = len(source_repair_backlog)
    return {
        "items": page,
        "total": len(records),
        "summary": summary,
        "evidenceRepairQueue": evidence_repair_queue,
        "sourceRepairBacklog": source_repair_backlog,
        "scoreDimensions": _business_score_dimensions(),
        "markdown": _build_business_validation_markdown(
            page,
            evidence_repair_queue=evidence_repair_queue,
            source_repair_backlog=source_repair_backlog,
        ),
    }


def list_codex_review_notes(limit: int = 100) -> dict[str, Any]:
    notes = _read_codex_review_notes()
    notes.reverse()
    page = notes[: max(1, min(limit, 100))]
    latest_by_question: dict[str, dict[str, Any]] = {}
    for note in page:
        question_id = str(note.get("questionId") or "")
        if question_id and question_id not in latest_by_question:
            latest_by_question[question_id] = note
    return {
        "items": page,
        "total": len(notes),
        "limit": max(1, min(limit, 100)),
        "latestByQuestionId": latest_by_question,
    }


def get_latest_codex_review_scoring_artifacts() -> dict[str, Any]:
    """Return the latest Codex review TSV handoff files for manual scoring.

    This is deliberately read-only. The returned TSV text can prefill the
    `/astrbot/eval` import box, but it must still be human-confirmed before any
    `manual` score is saved.
    """
    latest_dir = _latest_codex_review_artifact_dir()
    if latest_dir is None:
        return {
            "available": False,
            "reason": "No Codex review artifact directory found.",
            "warning": "Run `npm run review:astrbot -- --limit=8` from 06_AppPlatform/frontend first.",
        }

    manual_path = latest_dir / "manual_scoring_template.tsv"
    draft_path = latest_dir / "codex_draft_scoring_sheet.tsv"
    reference_json_path = latest_dir / "reference_judge_packet.json"
    reference_md_path = latest_dir / "reference_judge_packet.md"
    manual_text = _read_text_file(manual_path)
    draft_text = _read_text_file(draft_path)
    reference_json_text = _read_text_file(reference_json_path)
    reference_md_text = _read_text_file(reference_md_path)
    available = bool(manual_text or draft_text or reference_json_text or reference_md_text)
    return {
        "available": available,
        "runId": latest_dir.name,
        "artifactDir": str(latest_dir),
        "hasManualTemplate": bool(manual_text),
        "hasDraft": bool(draft_text),
        "hasReferenceJudgePacket": bool(reference_json_text or reference_md_text),
        "manualTemplatePath": str(manual_path) if manual_text else "",
        "codexDraftSheetPath": str(draft_path) if draft_text else "",
        "referenceJudgePacketJsonPath": str(reference_json_path) if reference_json_text else "",
        "referenceJudgePacketMdPath": str(reference_md_path) if reference_md_text else "",
        "manualTemplateText": manual_text,
        "codexDraftSheetText": draft_text,
        "referenceJudgePacketJsonText": reference_json_text,
        "referenceJudgePacketMdText": reference_md_text,
        "rowCount": _tsv_row_count(draft_text or manual_text),
        "warning": "Review/edit TSV or judge JSON before saving scores. Codex draft rows do not count until a human confirms them; accepted reference judge output is saved as llm_judge.",
        "reason": "" if available else "Latest Codex review artifact does not include scoring TSV or reference judge packet files.",
    }


def _safe_run_business_validation_question(question_def: dict[str, Any]) -> dict[str, Any]:
    try:
        return _run_side_by_side_question_def(question_def, validation_type="business")
    except Exception as exc:  # noqa: BLE001
        return {
            "comparisonId": _generate_side_by_side_id(),
            "runAt": _now_iso(),
            "validationType": "business",
            "questionId": question_def.get("id", "unknown"),
            "category": question_def.get("category", "unknown"),
            "country": question_def.get("country", "unknown"),
            "question": question_def.get("question", ""),
            "errors": {"comparison": str(exc)},
            "humanScoring": _initial_human_scoring("business"),
        }


def _run_side_by_side_question_def(
    question_def: dict[str, Any],
    *,
    validation_type: str,
) -> dict[str, Any]:
    country = str(question_def.get("country") or "Sweden")
    question_text = str(question_def.get("question") or "")

    astrbot_result: dict[str, Any] | None = None
    country_result: dict[str, Any] | None = None
    errors: dict[str, str] = {}

    try:
        astrbot_result = call_jato_mcp_tool("route_agent_request", {
            "country": country,
            "question": question_text,
        })
    except Exception as exc:  # noqa: BLE001
        errors["astrbot"] = str(exc)

    try:
        country_result = answer_country_question(
            country=country,
            question=question_text,
        )
    except Exception as exc:  # noqa: BLE001
        errors["countryCopilot"] = str(exc)

    astrbot_scores = _auto_score(question_def, astrbot_result) if astrbot_result else {"composite": 0, "error": errors.get("astrbot", "")}
    record = {
        "comparisonId": _generate_side_by_side_id(),
        "runAt": _now_iso(),
        "validationType": validation_type,
        "questionId": question_def["id"],
        "category": question_def.get("category", "unknown"),
        "country": country,
        "question": question_text,
        "expectedIntent": question_def.get("expectedIntent", ""),
        "expectedRetrievalPath": question_def.get("expectedRetrievalPath", ""),
        "expectedTools": question_def.get("expectedTools", []),
        "expectedFollowUpTypes": question_def.get("expectedFollowUpTypes", []),
        "scoreSchema": _business_score_dimensions() if validation_type == "business" else _default_score_dimensions(),
        "astrbot": _summarize_astrbot_side(
            astrbot_result,
            astrbot_scores,
            errors.get("astrbot"),
            country=country,
            question=question_text,
        ),
        "countryCopilot": _summarize_country_copilot_side(country_result, errors.get("countryCopilot")),
        "comparison": _build_side_by_side_comparison(
            astrbot_result,
            country_result,
            errors,
            question=question_text,
        ),
        "humanScoring": _initial_human_scoring(validation_type),
        "errors": errors,
    }
    if validation_type == "business":
        astrbot_side = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
        evidence_package = astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
        record["businessPlaybook"] = build_business_playbook_context(
            country=country,
            question=question_text,
            evidence_package=evidence_package,
            category=str(question_def.get("category") or ""),
        )
        record["failureTags"] = infer_business_failure_tags(record)
        _apply_business_llm_judge(record)
    else:
        record["businessPlaybook"] = {}
        record["failureTags"] = []
    record["businessValidation"] = _business_validation_projection(record) if validation_type == "business" else {}
    _apply_side_by_side_schema(record)
    _sync_side_by_side_display_metrics(record)
    _append_side_by_side_result(record)
    return record


def run_eval_side_by_side_category(category: str, limit: int = 5) -> dict[str, Any]:
    questions = load_eval_questions()["items"]
    matched = [q for q in questions if q.get("category") == category][:max(1, min(limit, 20))]

    results: list[dict[str, Any]] = []
    for question_def in matched:
        try:
            results.append(run_eval_side_by_side_question(str(question_def["id"])))
        except Exception as exc:  # noqa: BLE001
            results.append({
                "comparisonId": _generate_side_by_side_id(),
                "runAt": _now_iso(),
                "questionId": question_def.get("id", "unknown"),
                "category": question_def.get("category", category),
                "country": question_def.get("country", "unknown"),
                "question": question_def.get("question", ""),
                "errors": {"comparison": str(exc)},
                "humanScoring": {"status": "pending"},
            })

    return {
        "category": category,
        "total": len(results),
        "results": results,
        "summary": _summarize_side_by_side_results(results),
    }


def list_eval_side_by_side_results(
    category: str | None = None,
    limit: int = 30,
    offset: int = 0,
    latest_per_question: bool = False,
) -> dict[str, Any]:
    results = _read_side_by_side_results()
    results.reverse()

    filtered = [
        _enrich_business_record_for_read(record)
        if record.get("validationType") == "business"
        else record
        for record in results
    ]
    if category:
        filtered = [r for r in filtered if r.get("category") == category]
    if latest_per_question:
        filtered = _latest_records_per_question(filtered)

    total = len(filtered)
    page = filtered[offset : offset + max(1, min(limit, 100))]
    return {
        "items": page,
        "total": total,
        "limit": limit,
        "offset": offset,
        "summary": _summarize_side_by_side_results(filtered),
    }


def update_eval_side_by_side_human_score(
    comparison_id: str,
    scoring: dict[str, Any],
) -> dict[str, Any]:
    records = _read_side_by_side_results()
    if not records:
        raise ValueError(f"Side-by-side comparison not found: {comparison_id}")

    for record in records:
        if str(record.get("comparisonId") or "") != comparison_id:
            continue

        current = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
        status = (_text(scoring.get("status")) or _text(current.get("status")) or "pending").strip()
        requested_source = scoring.get("source") if "source" in scoring else None
        if requested_source is not None:
            source = _human_score_source(
                requested_source,
                default="manual" if status == "scored" else "",
            )
        elif status == "scored":
            current_source = _human_score_source(current.get("source"), default="")
            source = current_source if current_source and current_source != "llm_judge" else "manual"
        else:
            source = _human_score_source(current.get("source"), default="")
        winner = (_text(scoring.get("winner")) or "").strip()
        notes = _text(scoring.get("notes"))[:4000]
        dimensions = _string_list(scoring.get("dimensions")) or _string_list(current.get("dimensions")) or list(_DEFAULT_HUMAN_SCORE_DIMENSIONS)
        astrbot_score_source = current.get("astrbotScores")
        for key in ("astrbotScores", "astrbot_scores"):
            if key in scoring:
                astrbot_score_source = scoring.get(key)
                break
        astrbot_total_score = _manual_total_score_from_payload(
            scoring,
            ("astrbotTotal", "astrbot_total", "astrbotScore", "astrbot_score"),
        )
        if astrbot_total_score is not None:
            astrbot_score_source = _filled_manual_scores(dimensions, astrbot_total_score)
        country_score_source = current.get("countryCopilotScores")
        for key in ("countryCopilotScores", "copilotScores", "country_copilot_scores"):
            if key in scoring:
                country_score_source = scoring.get(key)
                break
        country_total_score = _manual_total_score_from_payload(
            scoring,
            (
                "countryCopilotTotal",
                "country_copilot_total",
                "countryCopilotScore",
                "country_copilot_score",
                "copilotTotal",
                "copilot_total",
                "copilotScore",
                "copilot_score",
            ),
        )
        if country_total_score is not None:
            country_score_source = _filled_manual_scores(dimensions, country_total_score)
        astrbot_scores = _read_manual_scores(
            astrbot_score_source,
            dimensions,
        )
        country_scores = _read_manual_scores(
            country_score_source,
            dimensions,
        )
        score_totals = _manual_score_totals(astrbot_scores, country_scores, dimensions)

        if status not in _HUMAN_SCORE_STATUSES:
            raise ValueError(f"Invalid human scoring status: {status}")
        if not winner and score_totals.get("complete") is True:
            winner = _winner_from_manual_totals(score_totals)
        if winner not in _HUMAN_SCORE_WINNERS:
            raise ValueError(f"Invalid human scoring winner: {winner}")
        if (status == "scored" or winner) and score_totals.get("complete") is not True:
            raise ValueError(
                f"Human scoring requires all {score_totals.get('requiredDimensions', len(dimensions))} "
                "dimensions for both AstrBot and CountryCopilot before marking a comparison as scored."
            )

        human_scoring = {
            "status": status,
            "source": source,
            "dimensions": dimensions,
            "winner": winner,
            "notes": notes,
            "astrbotScores": astrbot_scores,
            "countryCopilotScores": country_scores,
            "scoreTotals": score_totals,
            "updatedAt": _now_iso(),
        }
        judge_provider = _judge_provider_metadata(scoring.get("judgeProvider"))
        if judge_provider:
            human_scoring["judgeProvider"] = judge_provider
        record["humanScoring"] = human_scoring
        if record.get("validationType") == "business":
            if "failureTags" in scoring:
                record["failureTags"] = [
                    tag
                    for tag in _string_list(scoring.get("failureTags"))
                    if tag in set(FAILURE_TAGS)
                ]
            else:
                record["failureTags"] = infer_business_failure_tags(record)
            record["businessValidation"] = _business_validation_projection(record)
            _apply_side_by_side_schema(record)
        _write_side_by_side_results(records)
        return record

    raise ValueError(f"Side-by-side comparison not found: {comparison_id}")


def run_eval_category(category: str, limit: int = 10) -> dict[str, Any]:
    """Run all questions in a category."""
    questions = load_eval_questions()["items"]
    matched = [q for q in questions if q.get("category") == category][:limit]

    results: list[dict[str, Any]] = []
    for q in matched:
        try:
            record = run_eval_question(q["id"])
            results.append(record)
        except Exception as exc:
            results.append(_failed_eval_record(q, str(exc)))

    summary = _summarize_results(results)
    return {"category": category, "total": len(results), "results": results, "summary": summary}


def run_eval_full(questions_per_category: int = 5) -> dict[str, Any]:
    """Run a sample from each category."""
    all_results: list[dict[str, Any]] = []
    by_category: dict[str, Any] = {}
    categories = ["structured", "long_doc", "multi_hop", "fragmented", "mixed"]

    for cat in categories:
        cat_result = run_eval_category(cat, limit=questions_per_category)
        by_category[cat] = cat_result["summary"]
        all_results.extend(cat_result["results"])

    summary = _summarize_results(all_results)
    return {
        "totalRun": len(all_results),
        "byCategory": by_category,
        "overallSummary": summary,
        "results": all_results,
    }


# ── Results ──


def list_eval_results(
    category: str | None = None,
    limit: int = 30,
    offset: int = 0,
) -> dict[str, Any]:
    results = _read_results()
    results.reverse()  # newest first

    filtered = results
    if category:
        filtered = [r for r in results if r.get("category") == category]

    total = len(filtered)
    page = filtered[offset : offset + max(1, min(limit, 100))]
    return {
        "items": page,
        "total": total,
        "limit": limit,
        "offset": offset,
        "summary": _summarize_results(filtered),
    }


def get_eval_summary() -> dict[str, Any]:
    results = _read_results()
    if not results:
        return {"totalRuns": 0, "byCategory": {}, "overallScores": {}, "latestRunAt": None}

    by_category: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        cat = str(r.get("category") or "unknown")
        by_category.setdefault(cat, []).append(r)

    category_summaries = {
        cat: _summarize_results(items) for cat, items in by_category.items()
    }

    return {
        "totalRuns": len(results),
        "byCategory": category_summaries,
        "overallScores": _summarize_results(results),
        "latestRunAt": results[-1].get("runAt") if results else None,
    }


# ── Scoring engine ──


def _auto_score(question_def: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    """Auto-score a single eval result on objective metrics."""
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    evidence_pack = data.get("evidencePack") if isinstance(data.get("evidencePack"), dict) else {}
    answer = data.get("answer") if isinstance(data.get("answer"), dict) else {}
    quality_score = data.get("qualityScore") if isinstance(data.get("qualityScore"), dict) else {}
    retrieval = data.get("retrievalClassification") if isinstance(data.get("retrievalClassification"), dict) else {}
    primary_result = data.get("primaryResult") if isinstance(data.get("primaryResult"), dict) else {}
    primary_result_data = primary_result.get("data") if isinstance(primary_result.get("data"), dict) else {}
    chart_specs = primary_result_data.get("chartSpecs") if isinstance(primary_result_data.get("chartSpecs"), dict) else {}

    expected_path = question_def.get("expectedRetrievalPath", "")
    actual_path = retrieval.get("primaryPath", "")
    expected_tools = question_def.get("expectedTools", [])

    # 1. Retrieval path correctness (0-1)
    path_score = 1.0 if expected_path and actual_path == expected_path else 0.5 if actual_path else 0.0

    # 2. Evidence traceability (0-1)
    evidence_items = evidence_pack.get("items", [])
    evidence_count = len(evidence_items) if isinstance(evidence_items, list) else 0
    sources = evidence_pack.get("sources", [])
    source_count = len(sources) if isinstance(sources, list) else 0
    evidence_score = min(1.0, (evidence_count * 0.4 + source_count * 0.3))

    # 3. Citation coverage (0-1)
    has_metadata = bool(metadata)
    has_limitations = bool(evidence_pack.get("limitations"))
    citation_score = (0.5 if has_metadata else 0.0) + (0.3 if has_limitations else 0.0) + (0.2 if evidence_count > 0 else 0.0)

    # 4. Chart correctness (0-1, only if chart was expected)
    chart_expected = any("chart" in t for t in expected_tools)
    chart_count = chart_specs.get("chartCount", 0)
    chart_score = 1.0 if chart_expected and chart_count > 0 else 0.5 if chart_expected else 1.0

    # 5. Tool selection relevance (0-1)
    selected_tool = metadata.get("selectedTool", "")
    actual_tools = _actual_tools_from_result(result)
    missing_expected_tools = _missing_expected_tools(expected_tools, actual_tools)
    tool_score = _expected_tool_recall(expected_tools, actual_tools)
    business_synthesis_score = _business_synthesis_score(answer, quality_score)

    # Composite score (weighted)
    composite = round(
        path_score * 0.20
        + evidence_score * 0.25
        + citation_score * 0.15
        + chart_score * 0.15
        + tool_score * 0.25,
        3,
    )

    return {
        "composite": composite,
        "retrievalPathCorrectness": path_score,
        "evidenceTraceability": evidence_score,
        "citationCoverage": citation_score,
        "chartCorrectness": chart_score,
        "toolSelectionRelevance": tool_score,
        "businessSynthesis": business_synthesis_score,
        "breakdown": {
            "path": {"expected": expected_path, "actual": actual_path, "score": path_score},
            "evidence": {"itemCount": evidence_count, "sourceCount": source_count, "score": evidence_score},
            "tool": {
                "expected": expected_tools,
                "selected": selected_tool,
                "actual": actual_tools,
                "missing": missing_expected_tools,
                "score": tool_score,
            },
            "chart": {"expected": chart_expected, "produced": chart_count, "score": chart_score},
            "business": {"score": business_synthesis_score},
        },
    }


def _actual_tools_from_result(result: dict[str, Any]) -> list[str]:
    """Extract the tools that actually contributed to an AstrBot result."""
    if not isinstance(result, dict):
        return []
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    tools: list[str] = []

    _append_tool_name(tools, metadata.get("selectedTool"))
    for value in (
        metadata.get("toolsUsed"),
        metadata.get("toolCalls"),
        result.get("toolsUsed"),
        result.get("toolCalls"),
        data.get("toolsUsed"),
        data.get("toolCalls"),
    ):
        _append_tool_values(tools, value)

    tool_plan = data.get("retrievalToolPlan") or data.get("toolPlan")
    if isinstance(tool_plan, dict):
        steps = tool_plan.get("steps") if isinstance(tool_plan.get("steps"), list) else []
        for step in steps:
            if not isinstance(step, dict):
                continue
            if step.get("executed") is False:
                continue
            _append_tool_name(tools, step.get("toolName") or step.get("tool") or step.get("name"))

    for package_key in ("evidencePackage", "evidencePack"):
        package = data.get(package_key)
        if not isinstance(package, dict):
            continue
        tool_results = package.get("toolResults")
        if not isinstance(tool_results, list):
            continue
        for tool_result in tool_results:
            if not isinstance(tool_result, dict):
                continue
            _append_tool_name(tools, tool_result.get("toolName") or tool_result.get("tool") or tool_result.get("name"))

    return _dedupe_string_list(tools)


def _append_tool_values(target: list[str], value: Any) -> None:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                _append_tool_name(target, item.get("toolName") or item.get("tool") or item.get("name"))
            else:
                _append_tool_name(target, item)
        return
    if isinstance(value, dict):
        _append_tool_name(target, value.get("toolName") or value.get("tool") or value.get("name"))
        return
    _append_tool_name(target, value)


def _append_tool_name(target: list[str], value: Any) -> None:
    text = _text(value)
    if not text or text == "route_agent_request":
        return
    target.append(text)


def _expected_tool_recall(expected_tools: Any, actual_tools: list[str]) -> float:
    expected = _string_list(expected_tools)
    if not expected:
        return 1.0
    if not actual_tools:
        return 0.0
    matched = len(expected) - len(_missing_expected_tools(expected, actual_tools))
    return round(max(0.0, min(1.0, matched / len(expected))), 3)


def _missing_expected_tools(expected_tools: Any, actual_tools: list[str]) -> list[str]:
    expected = _string_list(expected_tools)
    actual = _string_list(actual_tools)
    missing: list[str] = []
    for required_tool in expected:
        if any(tool_satisfies_required(required_tool, executed_tool) for executed_tool in actual):
            continue
        missing.append(required_tool)
    return missing


def _business_synthesis_score(answer: dict[str, Any], quality_score: dict[str, Any]) -> float:
    explicit = _optional_float(quality_score.get("businessSynthesisScore"))
    if explicit is not None:
        return round(max(0.0, min(1.0, explicit)), 3)
    synthesis = answer.get("businessSynthesisPlan") if isinstance(answer.get("businessSynthesisPlan"), dict) else {}
    checks = [
        bool(str(answer.get("direct") or synthesis.get("executiveConclusion") or "").strip()),
        bool(answer.get("businessImplications") or synthesis.get("businessImplications")),
        bool(answer.get("recommendedActions") or synthesis.get("recommendedActions")),
        isinstance(synthesis.get("evidenceAlignment"), dict) and bool(synthesis.get("evidenceAlignment")),
        bool(answer.get("reportReadyBullets") or synthesis.get("reportReadyBullets")),
    ]
    return round(sum(1 for item in checks if item) / len(checks), 3)


def _summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {"count": 0, "avgComposite": 0, "avgEvidence": 0, "avgCitation": 0, "avgTool": 0}

    count = len(results)
    composites = []
    evidence_scores = []
    citation_scores = []
    tool_scores = []

    for r in results:
        scores = r.get("scores") or {}
        composites.append(scores.get("composite", 0))
        evidence_scores.append(scores.get("evidenceTraceability", 0))
        citation_scores.append(scores.get("citationCoverage", 0))
        tool_scores.append(scores.get("toolSelectionRelevance", 0))

    return {
        "count": count,
        "avgComposite": round(sum(composites) / count, 3) if count else 0,
        "avgEvidence": round(sum(evidence_scores) / count, 3) if count else 0,
        "avgCitation": round(sum(citation_scores) / count, 3) if count else 0,
        "avgTool": round(sum(tool_scores) / count, 3) if count else 0,
    }


def _summarize_side_by_side_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {
            "count": 0,
            "pendingHumanScoring": 0,
            "pendingBaselineScoring": 0,
            "scoredCount": 0,
            "baselineScoredCount": 0,
            "humanScoreSourceCounts": {},
            "baselineSourceCounts": {},
            "astrbotErrorCount": 0,
            "countryCopilotErrorCount": 0,
            "avgAstrBotComposite": 0,
            "avgAstrBotHumanScore": 0,
            "avgCountryCopilotHumanScore": 0,
            "humanWins": {"astrbot": 0, "countryCopilot": 0, "tie": 0, "unclear": 0},
            "failureTagCounts": {},
            "topFailureTags": [],
            "astrbotWinRate": 0,
            "categoryLevelScore": {},
            "judgeCalibration": _judge_calibration_summary([]),
            "referenceJudgePaths": list_reference_judge_paths(),
            "recommendedNextActions": [],
            "replacementReadinessVerdict": "not_enough_data",
            "replacementReadiness": _replacement_readiness_summary(
                count=0,
                scored=0,
                pending=0,
                source_counts={},
                verdict="not_enough_data",
                astrbot_win_rate=0,
                astrbot_avg=0,
                country_avg=0,
                astrbot_errors=0,
                country_errors=0,
                hallucination_risk_count=0,
                failure_tag_counts={},
            ),
            "selfTestBaseline": _self_test_baseline_summary(
                count=0,
                scored=0,
                pending=0,
                source_counts={},
                astrbot_win_rate=0,
                astrbot_avg=0,
                country_avg=0,
            ),
        }
    astrbot_scores = []
    astrbot_human_scores: list[float] = []
    country_human_scores: list[float] = []
    replacement_astrbot_scores: list[float] = []
    replacement_country_scores: list[float] = []
    astrbot_errors = 0
    country_errors = 0
    pending = 0
    scored = 0
    score_source_counts: dict[str, int] = {}
    replacement_source_counts: dict[str, int] = {}
    human_wins = {"astrbot": 0, "countryCopilot": 0, "tie": 0, "unclear": 0}
    replacement_wins = {"astrbot": 0, "countryCopilot": 0, "tie": 0, "unclear": 0}
    failure_tag_counts: dict[str, int] = {}
    by_category: dict[str, list[dict[str, Any]]] = {}
    for record in results:
        by_category.setdefault(str(record.get("category") or "unknown"), []).append(record)
        astrbot = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
        country = record.get("countryCopilot") if isinstance(record.get("countryCopilot"), dict) else {}
        scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
        if astrbot.get("error"):
            astrbot_errors += 1
        if country.get("error"):
            country_errors += 1
        if scoring.get("status") == "scored":
            scored += 1
            source = _human_score_source(scoring.get("source"), default="manual") or "manual"
            score_source_counts[source] = score_source_counts.get(source, 0) + 1
            if source in _REPLACEMENT_BASELINE_SOURCES:
                replacement_source_counts[source] = replacement_source_counts.get(source, 0) + 1
        else:
            pending += 1
        winner = str(scoring.get("winner") or "")
        if winner in human_wins:
            human_wins[winner] += 1
            source = _human_score_source(scoring.get("source"), default="")
            if scoring.get("status") == "scored" and source in _REPLACEMENT_BASELINE_SOURCES:
                replacement_wins[winner] += 1
        if scoring.get("status") == "scored":
            score_totals = scoring.get("scoreTotals") if isinstance(scoring.get("scoreTotals"), dict) else {}
            if score_totals.get("complete") is True:
                astrbot_human = _optional_float(score_totals.get("astrbot"))
                country_human = _optional_float(score_totals.get("countryCopilot"))
                if astrbot_human is not None:
                    astrbot_human_scores.append(astrbot_human)
                if country_human is not None:
                    country_human_scores.append(country_human)
                source = _human_score_source(scoring.get("source"), default="")
                if source in _REPLACEMENT_BASELINE_SOURCES:
                    if astrbot_human is not None:
                        replacement_astrbot_scores.append(astrbot_human)
                    if country_human is not None:
                        replacement_country_scores.append(country_human)
        score = astrbot.get("scores", {}).get("composite") if isinstance(astrbot.get("scores"), dict) else 0
        astrbot_scores.append(float(score or 0))
        for tag in _string_list(record.get("failureTags")):
            failure_tag_counts[tag] = failure_tag_counts.get(tag, 0) + 1
    count = len(results)
    baseline_source_counts = dict(sorted(score_source_counts.items()))
    baseline_scored = sum(baseline_source_counts.values())
    replacement_source_counts = dict(sorted(replacement_source_counts.items()))
    replacement_baseline_scored = sum(replacement_source_counts.values())
    top_failure_tags = [
        {"tag": tag, "count": value}
        for tag, value in sorted(failure_tag_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    ]
    astrbot_win_rate = round(human_wins["astrbot"] / scored, 3) if scored else 0
    replacement_win_rate = (
        round(replacement_wins["astrbot"] / replacement_baseline_scored, 3)
        if replacement_baseline_scored
        else 0
    )
    category_scores = {
        category: _category_side_by_side_score(items)
        for category, items in sorted(by_category.items())
    }
    replacement_astrbot_avg = round(sum(replacement_astrbot_scores) / len(replacement_astrbot_scores), 2) if replacement_astrbot_scores else 0
    replacement_country_avg = round(sum(replacement_country_scores) / len(replacement_country_scores), 2) if replacement_country_scores else 0
    readiness = _replacement_readiness_verdict(
        scored=replacement_baseline_scored,
        astrbot_win_rate=replacement_win_rate,
        astrbot_avg=replacement_astrbot_avg,
        country_avg=replacement_country_avg,
        astrbot_errors=astrbot_errors,
        hallucination_risk_count=failure_tag_counts.get("hallucination_risk", 0),
    )
    calibration = _judge_calibration_summary(results)
    recommendations = _top_failure_recommendations(failure_tag_counts)
    self_test_baseline = _self_test_baseline_from_records(
        results=results,
        source_counts=baseline_source_counts,
        saved_astrbot_scores=astrbot_human_scores,
        saved_country_scores=country_human_scores,
        saved_wins=human_wins,
    )
    return {
        "count": count,
        "pendingHumanScoring": pending,
        "pendingBaselineScoring": max(0, count - baseline_scored),
        "pendingReplacementBaselineScoring": max(0, count - replacement_baseline_scored),
        "scoredCount": scored,
        "baselineScoredCount": baseline_scored,
        "replacementBaselineScoredCount": replacement_baseline_scored,
        "humanScoreSourceCounts": baseline_source_counts,
        "baselineSourceCounts": baseline_source_counts,
        "replacementBaselineSourceCounts": replacement_source_counts,
        "astrbotErrorCount": astrbot_errors,
        "countryCopilotErrorCount": country_errors,
        "avgAstrBotComposite": round(sum(astrbot_scores) / count, 3) if count else 0,
        "avgAstrBotHumanScore": round(sum(astrbot_human_scores) / len(astrbot_human_scores), 2) if astrbot_human_scores else 0,
        "avgCountryCopilotHumanScore": round(sum(country_human_scores) / len(country_human_scores), 2) if country_human_scores else 0,
        "avgAstrBotReplacementScore": replacement_astrbot_avg,
        "avgCountryCopilotReplacementScore": replacement_country_avg,
        "humanWins": human_wins,
        "replacementWins": replacement_wins,
        "failureTagCounts": dict(sorted(failure_tag_counts.items(), key=lambda item: (-item[1], item[0]))),
        "topFailureTags": top_failure_tags,
        "astrbotWinRate": astrbot_win_rate,
        "replacementAstrbotWinRate": replacement_win_rate,
        "categoryLevelScore": category_scores,
        "judgeCalibration": calibration,
        "referenceJudgePaths": list_reference_judge_paths(),
        "recommendedNextActions": recommendations,
        "replacementReadinessVerdict": readiness,
        "replacementReadiness": _replacement_readiness_summary(
            count=count,
            scored=replacement_baseline_scored,
            pending=max(0, count - replacement_baseline_scored),
            source_counts=replacement_source_counts,
            verdict=readiness,
            astrbot_win_rate=replacement_win_rate,
            astrbot_avg=replacement_astrbot_avg,
            country_avg=replacement_country_avg,
            astrbot_errors=astrbot_errors,
            country_errors=country_errors,
            hallucination_risk_count=failure_tag_counts.get("hallucination_risk", 0),
            failure_tag_counts=failure_tag_counts,
        ),
        "selfTestBaseline": self_test_baseline,
    }


def _category_side_by_side_score(records: list[dict[str, Any]]) -> dict[str, Any]:
    astrbot_scores: list[float] = []
    copilot_scores: list[float] = []
    wins = {"astrbot": 0, "countryCopilot": 0, "tie": 0, "unclear": 0}
    scored = 0
    for record in records:
        scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
        totals = scoring.get("scoreTotals") if isinstance(scoring.get("scoreTotals"), dict) else {}
        if totals.get("complete") is True:
            scored += 1
            astrbot_value = _optional_float(totals.get("astrbot"))
            copilot_value = _optional_float(totals.get("countryCopilot"))
            if astrbot_value is not None:
                astrbot_scores.append(astrbot_value)
            if copilot_value is not None:
                copilot_scores.append(copilot_value)
        winner = str(scoring.get("winner") or "")
        if winner in wins:
            wins[winner] += 1
    return {
        "count": len(records),
        "scoredCount": scored,
        "avgAstrBot": round(sum(astrbot_scores) / len(astrbot_scores), 2) if astrbot_scores else 0,
        "avgCopilot": round(sum(copilot_scores) / len(copilot_scores), 2) if copilot_scores else 0,
        "astrbotWinRate": round(wins["astrbot"] / scored, 3) if scored else 0,
        "wins": wins,
    }


def _judge_calibration_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    calibration_records = [
        item
        for item in (_judge_calibration_projection(record) for record in records)
        if item["gptJudgeWinner"]
    ]
    reviewed = [item for item in calibration_records if item["humanWinner"]]
    match_count = sum(1 for item in reviewed if item["agreementStatus"] == "match")
    partial_count = sum(1 for item in reviewed if item["agreementStatus"] == "partial")
    mismatch_count = sum(1 for item in reviewed if item["agreementStatus"] == "mismatch")
    reviewed_count = len(reviewed)
    strict_rate = round(match_count / reviewed_count, 3) if reviewed_count else 0
    weighted_rate = round((match_count + partial_count * 0.5) / reviewed_count, 3) if reviewed_count else 0
    return {
        "gptJudgedCount": len(calibration_records),
        "humanReviewedCount": reviewed_count,
        "matchCount": match_count,
        "partialCount": partial_count,
        "mismatchCount": mismatch_count,
        "agreementRate": strict_rate,
        "weightedAgreementRate": weighted_rate,
        "needsHumanReviewCount": max(0, len(calibration_records) - reviewed_count),
        "mismatchExamples": [
            item
            for item in reviewed
            if item["agreementStatus"] == "mismatch"
        ][:10],
        "items": calibration_records[:100],
    }


def _judge_calibration_projection(record: dict[str, Any]) -> dict[str, Any]:
    llm_judge = record.get("llmJudge") if isinstance(record.get("llmJudge"), dict) else {}
    gpt_scores = llm_judge.get("scores") if llm_judge.get("status") == "ok" and isinstance(llm_judge.get("scores"), dict) else {}
    human = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
    human_is_review = human.get("status") == "scored" and human.get("source") != "llm_judge"
    gpt_winner = str(gpt_scores.get("winner") or "")
    human_winner = str(human.get("winner") or "") if human_is_review else ""
    return {
        "questionId": record.get("questionId", ""),
        "category": record.get("category", ""),
        "question": record.get("question", ""),
        "gptJudgeScores": {
            "astrbot": gpt_scores.get("astrbotScores", {}),
            "copilot": gpt_scores.get("countryCopilotScores", {}),
        },
        "gptJudgeWinner": gpt_winner,
        "gptFailureTags": _string_list(gpt_scores.get("failureTags")),
        "humanScores": {
            "astrbot": human.get("astrbotScores", {}) if human_is_review else {},
            "copilot": human.get("countryCopilotScores", {}) if human_is_review else {},
        },
        "humanWinner": human_winner,
        "humanFailureTags": _string_list(record.get("failureTags")) if human_is_review else [],
        "agreementStatus": _judge_agreement_status(gpt_winner, human_winner, gpt_scores, human) if human_is_review else "pending",
        "humanNotes": str(human.get("notes") or "") if human_is_review else "",
    }


def _judge_agreement_status(
    gpt_winner: str,
    human_winner: str,
    gpt_scores: dict[str, Any],
    human_scoring: dict[str, Any],
) -> str:
    if not gpt_winner or not human_winner:
        return "pending"
    if gpt_winner == human_winner:
        return "match"
    if "tie" in {gpt_winner, human_winner} or "unclear" in {gpt_winner, human_winner}:
        return "partial"
    gpt_delta = _score_delta(
        gpt_scores.get("astrbotScores") if isinstance(gpt_scores.get("astrbotScores"), dict) else {},
        gpt_scores.get("countryCopilotScores") if isinstance(gpt_scores.get("countryCopilotScores"), dict) else {},
    )
    human_delta = 0.0
    totals = human_scoring.get("scoreTotals") if isinstance(human_scoring.get("scoreTotals"), dict) else {}
    if totals:
        human_delta = (_optional_float(totals.get("delta")) or 0.0)
    if abs(gpt_delta) <= 0.5 or abs(human_delta) <= 0.5:
        return "partial"
    return "mismatch"


def _score_delta(left_scores: dict[str, Any], right_scores: dict[str, Any]) -> float:
    dimensions = [key for key in _BUSINESS_SCORE_DIMENSIONS if key in left_scores and key in right_scores]
    if not dimensions:
        return 0.0
    left_avg = sum(float(left_scores.get(key) or 0) for key in dimensions) / len(dimensions)
    right_avg = sum(float(right_scores.get(key) or 0) for key in dimensions) / len(dimensions)
    return round(left_avg - right_avg, 2)


def _top_failure_recommendations(failure_tag_counts: dict[str, int]) -> list[dict[str, Any]]:
    mapping = {
        "intent_wrong": ("Intent Router", "补充汽车业务例子和边界词，优先修正被路由到 general_qa 的真实问题。"),
        "tool_missing": ("IntentToolMatrix / ToolCard", "检查 requiredTools 与 optionalTools，补齐 pricing、policy、configuration 的强制工具映射。"),
        "evidence_missing": ("Data / Tool Result Mapping", "补数据源或增强工具结果到可引用证据映射。"),
        "answer_too_conservative": ("AnswerGroundingGuard / Fallback", "把 insufficient_evidence 改成 evidence-limited but useful answer：可判断部分、缺口、影响、下一步。"),
        "answer_too_generic": ("Business Composer", "强化国家、车型、竞品、价格带和业务动作的结构化输出，减少通用模板话术。"),
        "chart_not_useful": ("VisualArtifact Composer", "让定价、市场、竞品问题默认产出可读表格/图表/report block。"),
        "table_not_readable": ("VisualArtifact Renderer", "优化表格列裁剪、单位、排序和来源引用，避免证据表不可读。"),
        "pm_insight_weak": ("Business Composer Template", "强化产品经理判断：定位、场景、可赢点、短板、销售动作。"),
        "followup_low_value": ("FollowUp Planner", "让追问绑定业务意图、expectedTools 和 expectedOutput，避免泛泛继续问。"),
        "presentation_not_ready": ("Report Composer", "压缩成 PPT-ready key message、证据、产品含义、下一步动作。"),
        "hallucination_risk": ("Grounding Guard", "含数字结论必须有可引用证据；无证据数字降级为不确定表达并阻断 replacement readiness。"),
    }
    recommendations: list[dict[str, Any]] = []
    for tag, count in sorted(failure_tag_counts.items(), key=lambda item: (-item[1], item[0]))[:8]:
        module, action = mapping.get(tag, ("AstrBot Pipeline", "复核该 failureTag 的样本并补充对应规则。"))
        recommendations.append({
            "tag": tag,
            "count": count,
            "module": module,
            "recommendation": action,
            "priority": "P0" if tag in {"hallucination_risk", "tool_missing", "evidence_missing", "pm_insight_weak"} else "P1",
        })
    return recommendations


def _top_repair_gap_recommendations(top_repair_gaps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapping = {
        "coverage_diagnostic:no_current_prices_for_requested_models": (
            "MSRP Coverage / Source Repair",
            "优先补齐请求车型和竞品的当前 MSRP 来源，让 pricing / compare 问题先有可引用价格证据。",
            "P0",
        ),
        "current_msrp": (
            "Pricing Evidence Coverage",
            "这是数据覆盖缺口，不是 Agent 代码故障：已有价格样本时可先做走廊判断，但最终定价替换评估仍要补本车型官方 MSRP、版本价差和来源日期。",
            "P1",
        ),
        "own_model_price": (
            "Pricing Evidence",
            "补请求车型当前官方价格、来源 URL 和获取日期；这是判断目标价是否成立的前置证据。",
            "P0",
        ),
        "pricing_data_unavailable": (
            "Pricing Matrix Evidence",
            "补本车型与竞品 MSRP / TP / 月供价格矩阵，避免定价题只停留在框架和话术。",
            "P0",
        ),
        "competitor_price_range": (
            "Competitive Pricing Tool",
            "补竞品价格走廊和主销版本映射，避免 AstrBot 只能给定价框架而无法判断高低位。",
            "P0",
        ),
        "competitive_or_configuration_data_unavailable": (
            "Competitive / Configuration Evidence",
            "补竞品池、配置差异和版本定位证据；这是 J7 / O5 / O9 产品经理分析能否超过 Copilot 的核心缺口。",
            "P0",
        ),
        "coverage_diagnostic:no_config_projects_for_country": (
            "Engineering Config Coverage",
            "先导入或激活当前国家的工程配置项目，否则 compare_vehicle_variants 只能产出验证框架，不能给车型/版本配置差异。",
            "P0",
        ),
        "coverage_diagnostic:no_config_subjects_for_requested_models": (
            "Engineering Model Mapping",
            "把请求车型映射到工程配置 project / base variant / market variant，再重跑配置对比。",
            "P0",
        ),
        "coverage_diagnostic:insufficient_compare_subjects": (
            "Configuration Compare Input",
            "配置对比至少需要两个可比车型或版本；先补比较对象或把 segment 问题改走市场结构验证。",
            "P1",
        ),
        "configuration_delta": (
            "Configuration Compare",
            "补配置差异矩阵，把可感知高配、短板和销售话术从泛泛建议变成产品经理判断。",
            "P0",
        ),
        "published_date": (
            "Research Governance",
            "外部政策、新闻和 VOC 结论必须带发布日期；缺日期时只能作为背景，不能作为确定判断。",
            "P1",
        ),
        "specific_policy_source_evidence": (
            "Specific Policy Source",
            "补齐问题中点名政策的官方或可引用来源；泛 bonus/malus、vehicle tax 或市场背景只能作为交叉验证。",
            "P0",
        ),
        "external_research_claims_unavailable": (
            "External Research Evidence",
            "补 Tavily / web source refs，让政策、VOC、新闻判断像 Gemini research 一样可追溯。",
            "P1",
        ),
        "monthly_trend_series": (
            "Market Time Series",
            "补月度趋势序列，避免 market overview 只给静态快照，无法解释窗口期和变化原因。",
            "P1",
        ),
    }
    recommendations: list[dict[str, Any]] = []
    for repair_gap in top_repair_gaps[:8]:
        gap = str(repair_gap.get("gap") or repair_gap.get("tag") or "").strip()
        count = int(repair_gap.get("count") or 0)
        if not gap:
            continue
        module, action, priority = mapping.get(gap, (
            "Evidence Repair",
            "复核该 evidence gap 的样本，补齐工具结果到 EvidencePackage 的可引用证据映射。",
            "P1",
        ))
        sample_candidates = _string_list(repair_gap.get("sampleCandidates"))[:5]
        if gap == "minimum_external_sources" and _repair_gap_candidates_include_pricing(sample_candidates):
            module = "Pricing Source Materialization"
            action = (
                "补齐并物化官方价格/MSRP 来源：至少保留标题、URL、发布日期、车型/版本、币种和当前价格字段，"
                "生成 citation-ready price evidence 后再让 AstrBot 输出确定价格结论。"
            )
            priority = "P0"
        if sample_candidates:
            action = f"{action} 优先候选：{', '.join(sample_candidates)}。"
        sample_question_ids = _string_list(repair_gap.get("sampleQuestionIds"))[:5]
        if sample_question_ids:
            action = f"{action} 样本：{', '.join(sample_question_ids)}。"
        recommendations.append({
            "tag": gap,
            "gap": gap,
            "count": count,
            "module": module,
            "recommendation": action,
            "priority": priority,
            "source": "repair_gap",
            "sampleCandidates": sample_candidates,
            "sampleQuestionIds": sample_question_ids,
            "sampleQuestions": repair_gap.get("sampleQuestions") if isinstance(repair_gap.get("sampleQuestions"), list) else [],
        })
    return recommendations


def _repair_gap_candidates_include_pricing(sample_candidates: list[str]) -> bool:
    text = " ".join(str(item or "") for item in sample_candidates).casefold()
    return bool(text) and any(
        token in text
        for token in ("pricing", "official price", "msrp", "current price", "price source")
    )


def _with_repair_gap_summary(
    summary: dict[str, Any],
    evidence_repair_queue: list[dict[str, Any]],
) -> dict[str, Any]:
    repair_gap_counts: dict[str, int] = {}
    repair_gap_candidates: dict[str, list[str]] = {}
    repair_gap_questions: dict[str, list[dict[str, str]]] = {}
    for item in evidence_repair_queue:
        gap = str(item.get("primaryGap") or "").strip()
        if not gap:
            missing = item.get("missingEvidence") if isinstance(item.get("missingEvidence"), list) else []
            gap = str((missing[0] if missing and isinstance(missing[0], dict) else {}).get("name") or "").strip()
        if not gap:
            continue
        repair_gap_counts[gap] = repair_gap_counts.get(gap, 0) + 1
        question_bucket = repair_gap_questions.setdefault(gap, [])
        sample_question = _repair_gap_sample_question(item)
        if sample_question and all(existing.get("questionId") != sample_question["questionId"] for existing in question_bucket):
            question_bucket.append(sample_question)
        if not _repair_gap_uses_source_candidates(gap):
            continue
        source_candidates = item.get("sourceRepairCandidates") if isinstance(item.get("sourceRepairCandidates"), dict) else {}
        if not _source_repair_candidates_match_gap(gap, source_candidates):
            continue
        source_candidate_items = [
            *(source_candidates.get("ownModel") if isinstance(source_candidates.get("ownModel"), list) else []),
            *(source_candidates.get("competitorCorridor") if isinstance(source_candidates.get("competitorCorridor"), list) else []),
        ]
        candidate_labels = _relevant_source_candidate_labels(item, source_candidate_items)
        if candidate_labels:
            bucket = repair_gap_candidates.setdefault(gap, [])
            for label in candidate_labels:
                if label not in bucket and len(bucket) < 8:
                    bucket.append(label)
    top_repair_gaps = [
        {
            "gap": gap,
            "tag": gap,
            "count": count,
            "sampleCandidates": repair_gap_candidates.get(gap, [])[:5],
            "sampleQuestionIds": [
                item["questionId"]
                for item in repair_gap_questions.get(gap, [])[:5]
                if item.get("questionId")
            ],
            "sampleQuestions": repair_gap_questions.get(gap, [])[:5],
        }
        for gap, count in sorted(repair_gap_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    ]
    enriched = dict(summary)
    enriched["repairGapCounts"] = dict(sorted(repair_gap_counts.items(), key=lambda item: (-item[1], item[0])))
    enriched["topRepairGaps"] = top_repair_gaps
    repair_recommendations = _top_repair_gap_recommendations(top_repair_gaps)
    if not enriched.get("recommendedNextActions"):
        enriched["recommendedNextActions"] = repair_recommendations
    elif repair_recommendations:
        existing_tags = {
            str(item.get("tag") or item.get("gap") or "")
            for item in enriched.get("recommendedNextActions", [])
            if isinstance(item, dict)
        }
        enriched["recommendedNextActions"] = [
            *enriched.get("recommendedNextActions", []),
            *[item for item in repair_recommendations if item["tag"] not in existing_tags],
        ][:8]
    return enriched


def _repair_gap_sample_question(item: dict[str, Any]) -> dict[str, str]:
    question_id = str(item.get("questionId") or "").strip()
    if not question_id:
        return {}
    return {
        "questionId": question_id,
        "category": str(item.get("category") or "").strip(),
        "country": str(item.get("country") or "").strip(),
        "question": str(item.get("question") or "").strip(),
        "priority": str(item.get("priority") or "").strip(),
        "answerStatus": str(item.get("answerStatus") or "").strip(),
        "repairAction": _sanitize_evidence_repair_text(item.get("repairAction")),
    }


def _repair_gap_uses_source_candidates(gap: str) -> bool:
    """Attach source-repair candidates only to gaps where candidates are actionable."""
    normalized = str(gap or "").lower()
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "external_research_claims_unavailable",
            "minimum_external_sources",
            "leasing_tco_or_company_car_evidence",
            "specific_policy_source_evidence",
            "coverage_diagnostic:no_current_prices",
            "current_msrp",
            "official_msrp",
            "own_model_price",
            "competitor_price_range",
            "price_corridor",
            "query_msrp_pricing",
            "pricing",
        )
    )


def _source_repair_candidates_match_gap(gap: str, candidates: dict[str, Any]) -> bool:
    if not candidates:
        return False
    normalized_gap = str(gap or "").lower()
    data_status = str(candidates.get("dataStatus") or "").strip()
    if "leasing_tco_or_company_car_evidence" in normalized_gap:
        return data_status == "leasing_tco_source_candidates"
    if any(token in normalized_gap for token in ("external_research", "minimum_external_sources")):
        return data_status in {
            "external_research_query_candidates",
            "external_policy_source_candidates",
            "leasing_tco_source_candidates",
        } or "source_draft" in data_status
    if "specific_policy_source" in normalized_gap:
        return data_status == "external_policy_source_candidates"
    return data_status not in {"external_research_query_candidates"}


def _build_evidence_repair_queue(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    for record in records:
        astrbot = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
        missing_evidence = _repair_missing_evidence_items(astrbot.get("missingEvidence"))
        failure_tags = _string_list(record.get("failureTags"))
        if not missing_evidence and not failure_tags:
            continue
        has_blocking_missing = any(
            str(item.get("impact") or "") == "blocking"
            for item in missing_evidence
        )
        recommended_actions = _repair_recommended_action_items(
            astrbot.get("recommendedActions")
        )
        repair_recommended_actions = _compatible_recommended_actions(
            recommended_actions=recommended_actions,
            missing_evidence=missing_evidence,
        )
        source_repair_candidates = _repair_source_candidate_items(
            astrbot.get("sourceRepairCandidates")
        )
        source_repair_action = _source_repair_action_text(source_repair_candidates)
        recommended_repair_action = _repair_action_from_recommended_actions(
            recommended_actions=repair_recommended_actions,
            missing_evidence=missing_evidence,
        )
        repair_action = (
            source_repair_action
            if source_repair_action
            else recommended_repair_action
            if recommended_repair_action
            else _default_evidence_repair_action(missing_evidence)
        )
        repair_action = _sanitize_evidence_repair_text(repair_action)
        queue_priority = "P0" if failure_tags or has_blocking_missing else "P1"
        repair_tasks = _repair_tasks(
            question_id=str(record.get("questionId") or ""),
            category=str(record.get("category") or ""),
            country=str(record.get("country") or ""),
            selected_tool=str(astrbot.get("selectedTool") or ""),
            missing_evidence=missing_evidence,
            recommended_actions=repair_recommended_actions,
            source_repair_candidates=source_repair_candidates,
            repair_action=repair_action,
            queue_priority=queue_priority,
        )
        repair_tasks = [_sanitize_repair_task(task) for task in repair_tasks]
        display_recommended_actions = (
            _repair_queue_recommended_actions(
                recommended_actions=repair_recommended_actions,
                repair_action=repair_action,
            )
            if source_repair_action
            else repair_recommended_actions
        )
        repair_summary = _repair_summary(
            missing_evidence=missing_evidence,
            source_repair_candidates=source_repair_candidates,
            repair_action=repair_action,
            failure_tags=failure_tags,
        )
        queue.append({
            "questionId": str(record.get("questionId") or ""),
            "comparisonId": str(record.get("comparisonId") or ""),
            "category": str(record.get("category") or ""),
            "country": str(record.get("country") or ""),
            "question": str(record.get("question") or ""),
            "priority": queue_priority,
            "primaryGap": str(repair_summary.get("primaryGap") or ""),
            "answerStatus": str(
                astrbot.get("answerStatus")
                or astrbot.get("status")
                or "unknown"
            ),
            "selectedTool": str(astrbot.get("selectedTool") or ""),
            "failureTags": failure_tags,
            "missingEvidence": missing_evidence,
            "recommendedActions": display_recommended_actions,
            "sourceRepairCandidates": source_repair_candidates,
            "sourceSearchPlan": (
                source_repair_candidates.get("sourceSearchPlan")
                if isinstance(source_repair_candidates.get("sourceSearchPlan"), list)
                else []
            ),
            "repairSummary": repair_summary,
            "sourceCandidateCount": int(repair_summary.get("sourceCandidateCount") or 0),
            "sourceSummary": str(repair_summary.get("sourceSummary") or ""),
            "repairAction": repair_action,
            "commandHint": _repair_queue_command_hint(repair_tasks, repair_action),
            "repairTasks": repair_tasks,
        })
    return sorted(
        queue,
        key=lambda item: (
            0 if item.get("priority") == "P0" else 1,
            -len(item.get("missingEvidence") if isinstance(item.get("missingEvidence"), list) else []),
            str(item.get("questionId") or ""),
        ),
    )


def _build_source_repair_backlog(
    evidence_repair_queue: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for queue_item in evidence_repair_queue:
        if not isinstance(queue_item, dict):
            continue
        source_search_plan = queue_item.get("sourceSearchPlan")
        if not isinstance(source_search_plan, list):
            continue
        for plan_item in source_search_plan:
            if not isinstance(plan_item, dict):
                continue
            entry = _source_repair_backlog_entry(queue_item, plan_item)
            if not entry:
                continue
            key = _source_repair_backlog_key(entry)
            if not key:
                continue
            existing = grouped.get(key)
            if existing is None:
                grouped[key] = entry
                continue
            _merge_source_repair_backlog_entry(existing, entry)
    return sorted(
        grouped.values(),
        key=lambda item: (
            0 if item.get("priority") == "P0" else 1,
            -int(item.get("affectedCount") or 0),
            str(item.get("sourceType") or ""),
            str(item.get("label") or ""),
        ),
    )


def _source_repair_backlog_entry(
    queue_item: dict[str, Any],
    plan_item: dict[str, Any],
) -> dict[str, Any]:
    query = str(plan_item.get("sourceSearchQuery") or "").strip()
    source_url = str(plan_item.get("sourceUrl") or "").strip()
    domain = str(plan_item.get("candidateDomain") or "").strip()
    source_draft_path = str(plan_item.get("sourceDraftPath") or "").strip()
    relative_path = str(plan_item.get("relativePath") or "").strip()
    label = str(plan_item.get("label") or "").strip()
    if not any([query, source_url, domain, label]):
        return {}
    source_type = _source_repair_backlog_source_type(queue_item, plan_item)
    primary_gap = str(queue_item.get("primaryGap") or "").strip()
    question_id = str(queue_item.get("questionId") or "").strip()
    category = str(queue_item.get("category") or "").strip()
    country = str(queue_item.get("country") or "").strip()
    failure_tags = _string_list(queue_item.get("failureTags"))
    return {
        "priority": str(queue_item.get("priority") or "P1"),
        "sourceType": source_type,
        "role": str(plan_item.get("role") or "").strip(),
        "label": label,
        "brand": str(plan_item.get("brand") or "").strip(),
        "model": str(plan_item.get("model") or "").strip(),
        "candidateSourceType": str(plan_item.get("candidateSourceType") or "").strip(),
        "candidateDomain": domain,
        "sourceDraftPath": source_draft_path or relative_path,
        "relativePath": relative_path,
        "sourceSearchQuery": query,
        "sourceUrl": source_url,
        "sourceTypeRaw": str(plan_item.get("sourceType") or "").strip(),
        "priceSemantics": str(plan_item.get("priceSemantics") or "").strip(),
        "extractorType": str(plan_item.get("extractorType") or "").strip(),
        "defaultCurrency": str(plan_item.get("defaultCurrency") or "").strip(),
        "reviewPendingRows": int(plan_item.get("reviewPendingRows") or 0),
        "reviewPendingStatus": str(plan_item.get("reviewPendingStatus") or "").strip(),
        "reviewPendingObservations": [
            item
            for item in plan_item.get("reviewPendingObservations", [])
            if isinstance(item, dict)
        ][:4],
        "priceSelector": str(plan_item.get("priceSelector") or "").strip(),
        "materializationStatus": str(plan_item.get("materializationStatus") or "").strip(),
        "materializationReviewStatus": str(plan_item.get("materializationReviewStatus") or "").strip(),
        "materializationRiskFlags": _string_list(plan_item.get("materializationRiskFlags")),
        "materializationReadinessScore": plan_item.get("materializationReadinessScore"),
        "materializationMissingFields": _string_list(plan_item.get("materializationMissingFields")),
        "materializationRequiredFields": _string_list(plan_item.get("materializationRequiredFields")),
        "materializationNextStep": str(plan_item.get("materializationNextStep") or "").strip(),
        "materializationWorkflow": _string_list(plan_item.get("materializationWorkflow")),
        "materializationGate": str(plan_item.get("materializationGate") or "").strip(),
        "safeToAutoMaterialize": bool(plan_item.get("safeToAutoMaterialize")),
        "priceSanityRules": plan_item.get("priceSanityRules") if isinstance(plan_item.get("priceSanityRules"), dict) else {},
        "dryRunCommand": str(plan_item.get("dryRunCommand") or "").strip(),
        "submitCommand": str(plan_item.get("submitCommand") or "").strip(),
        "ingestApiPath": str(plan_item.get("ingestApiPath") or "").strip(),
        "reviewChecklist": _string_list(plan_item.get("reviewChecklist")),
        "affectedCount": 1,
        "questionIds": [question_id] if question_id else [],
        "categories": [category] if category else [],
        "countries": [country] if country else [],
        "primaryGaps": [primary_gap] if primary_gap else [],
        "failureTags": failure_tags,
        "recommendedAction": _source_repair_backlog_action(
            source_type,
            plan_item=plan_item,
        ),
    }


def _source_repair_backlog_key(entry: dict[str, Any]) -> str:
    source_type = str(entry.get("sourceType") or "").strip().lower()
    query = str(entry.get("sourceSearchQuery") or "").strip().lower()
    source_url = str(entry.get("sourceUrl") or "").strip().lower()
    domain = str(entry.get("candidateDomain") or "").strip().lower()
    label = str(entry.get("label") or "").strip().lower()
    role = str(entry.get("role") or "").strip().lower()
    value = query or source_url or "|".join(part for part in [domain, label, role] if part)
    return "|".join(part for part in [source_type, value] if part)


def _source_repair_backlog_source_type(
    queue_item: dict[str, Any],
    plan_item: dict[str, Any],
) -> str:
    candidates = (
        queue_item.get("sourceRepairCandidates")
        if isinstance(queue_item.get("sourceRepairCandidates"), dict)
        else {}
    )
    data_status = str(candidates.get("dataStatus") or "").strip()
    candidate_type = str(plan_item.get("candidateSourceType") or "").strip()
    primary_gap = str(queue_item.get("primaryGap") or "").strip()
    plan_text = " ".join(
        str(plan_item.get(key) or "")
        for key in ("brand", "model", "label", "sourceSearchQuery", "sourceUrl", "candidateSourceType")
    ).lower()
    text = " ".join([data_status, candidate_type, primary_gap, plan_text]).lower()
    if (
        "leasing_tco_or_company_car_evidence" in primary_gap
        or "leasing_tco" in text
        or any(token in text for token in ("company-car", "company car", "residual value", "bilförmån", "fleet tco"))
    ):
        return "leasing_tco_source"
    if any(token in text for token in ("policy", "news")):
        return "policy_news_source"
    if "external_research" in data_status.lower() and any(
        token in text for token in ("pricing", "official price", "msrp", "pris officiell", "price list")
    ):
        return "external_price_source"
    if any(token in text for token in ("external_research", "voc", "minimum_external_sources")):
        return "external_research_source"
    return "msrp_current_price_source"


def _source_repair_backlog_action(
    source_type: str,
    *,
    plan_item: dict[str, Any] | None = None,
) -> str:
    materialization_next_step = (
        str(plan_item.get("materializationNextStep") or "").strip()
        if isinstance(plan_item, dict)
        else ""
    )
    if materialization_next_step and source_type == "msrp_current_price_source":
        return f"{materialization_next_step} 然后重跑相关 pricing/compare 问题。"
    if source_type == "leasing_tco_source":
        return (
            "验证 leasing/TCO/company-car 来源，补齐月供、残值/RV、税务 benefit、年里程、充电条件、"
            "适用车型和计算口径；完成前不要生成当前价格记录。"
        )
    if source_type == "policy_news_source":
        return "验证政策/新闻官方来源，补齐发布日期、适用对象、限制条件和影响边界。"
    if source_type == "external_price_source":
        return "验证官方价格/MSRP 来源，补齐标题、URL、发布日期、车型/版本、币种和当前价格字段。"
    if source_type == "external_research_source":
        return "验证 VOC/媒体/论坛来源，保留标题、URL、发布日期和可引用原文要点。"
    return "验证官方价格来源，补齐版本/配置、币种、发布日期并生成当前价格记录。"


def _merge_source_repair_backlog_entry(
    target: dict[str, Any],
    incoming: dict[str, Any],
) -> None:
    target_priority = str(target.get("priority") or "P1")
    incoming_priority = str(incoming.get("priority") or "P1")
    if incoming_priority == "P0" and target_priority != "P0":
        target["priority"] = "P0"
    for key in ("questionIds", "categories", "countries", "primaryGaps", "failureTags"):
        target[key] = _dedupe_string_list([
            *_string_list(target.get(key)),
            *_string_list(incoming.get(key)),
        ])
    for key in (
        "sourceDraftPath",
        "relativePath",
        "sourceUrl",
        "candidateDomain",
        "sourceTypeRaw",
        "priceSemantics",
        "extractorType",
        "defaultCurrency",
        "priceSelector",
        "materializationStatus",
        "materializationReviewStatus",
        "materializationReadinessScore",
        "materializationNextStep",
        "materializationGate",
        "dryRunCommand",
        "submitCommand",
        "ingestApiPath",
    ):
        if not str(target.get(key) or "").strip() and str(incoming.get(key) or "").strip():
            target[key] = incoming[key]
    for key in (
        "materializationMissingFields",
        "materializationRequiredFields",
        "materializationWorkflow",
        "materializationRiskFlags",
        "reviewChecklist",
    ):
        target[key] = _dedupe_string_list([
            *_string_list(target.get(key)),
            *_string_list(incoming.get(key)),
        ])
    target["affectedCount"] = len(_string_list(target.get("questionIds")))


def _repair_queue_recommended_actions(
    *,
    recommended_actions: list[dict[str, str]],
    repair_action: str,
) -> list[dict[str, str]]:
    action = _sanitize_evidence_repair_text(repair_action)
    if not action:
        return recommended_actions
    first = dict(recommended_actions[0]) if recommended_actions else {}
    first["action"] = action
    first["priority"] = str(first.get("priority") or "P0")
    if not str(first.get("rationale") or "").strip():
        first["rationale"] = "根据当前 evidence repair queue 归一化后的首要补证动作。"
    rest = [
        item
        for item in recommended_actions[1:]
        if _sanitize_evidence_repair_text(item.get("action")) != action
    ]
    return [first, *rest]


def _sanitize_evidence_repair_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return _sanitize_business_review_line(text)


def _sanitize_repair_task(task: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(task)
    for key in ("title", "input", "output", "commandHint"):
        if key in sanitized:
            sanitized[key] = _sanitize_evidence_repair_text(sanitized.get(key))
    return sanitized


def _repair_missing_evidence_items(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, str]] = []
    for entry in value:
        if isinstance(entry, str):
            name = entry.strip()
            if name:
                items.append({"name": name, "reason": "", "impact": "weakens_answer"})
            continue
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or entry.get("label") or entry.get("id") or "").strip()
        if not name:
            continue
        items.append({
            "name": name,
            "reason": str(entry.get("reason") or entry.get("description") or entry.get("message") or "").strip(),
            "impact": str(entry.get("impact") or entry.get("severity") or "weakens_answer").strip(),
        })
    return items


def _repair_recommended_action_items(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, str]] = []
    for entry in value:
        if isinstance(entry, str):
            action = entry.strip()
            if action:
                items.append({"action": action, "rationale": "", "priority": "P1"})
            continue
        if not isinstance(entry, dict):
            continue
        action = str(
            entry.get("action")
            or entry.get("recommendation")
            or entry.get("label")
            or entry.get("nextAction")
            or ""
        ).strip()
        if not action:
            continue
        items.append({
            "action": action,
            "rationale": str(entry.get("rationale") or entry.get("reason") or entry.get("description") or "").strip(),
            "priority": str(entry.get("priority") or "P1").strip(),
        })
    return items


def _repair_action_from_recommended_actions(
    *,
    recommended_actions: list[dict[str, str]],
    missing_evidence: list[dict[str, str]],
) -> str:
    """Pick a recommended action only when it matches the primary evidence gap."""
    names = {str(item.get("name") or "").strip() for item in missing_evidence}
    has_pricing_matrix_gap = any("pricing_data_unavailable" in name for name in names)
    for action in recommended_actions:
        action_text = str(action.get("action") or "").strip()
        if not action_text:
            continue
        if has_pricing_matrix_gap and _is_competitor_only_price_repair_action(action_text):
            return "补齐本车型与竞品 MSRP / TP / 月供价格矩阵"
        return action_text
    return ""


def _compatible_recommended_actions(
    *,
    recommended_actions: list[dict[str, str]],
    missing_evidence: list[dict[str, str]],
) -> list[dict[str, str]]:
    if not recommended_actions:
        return []
    names = {str(item.get("name") or "").strip() for item in missing_evidence}
    has_own_model_price_gap = any(
        "current_msrp" in name
        or "own_model_price" in name
        or "no_current_prices" in name
        for name in names
    )
    has_competitor_price_gap = any(
        "price_corridor" in name
        or "competitor_price" in name
        or "pricing_data_unavailable" in name
        for name in names
    )
    if not has_own_model_price_gap or has_competitor_price_gap:
        return recommended_actions
    return [
        action
        for action in recommended_actions
        if _is_own_model_price_repair_action(str(action.get("action") or ""))
    ]


def _is_own_model_price_repair_action(action_text: str) -> bool:
    if _is_competitor_only_price_repair_action(action_text):
        return False
    lowered = action_text.lower()
    own_model_tokens = [
        "本车型",
        "请求车型",
        "目标车型",
        "官方价格",
        "当前价格",
        "当前官方",
        "来源",
        "own-model",
        "own model",
        "requested model",
        "current price",
        "source",
        "msrp",
    ]
    return any(token in action_text or token in lowered for token in own_model_tokens)


def _is_competitor_only_price_repair_action(action_text: str) -> bool:
    lowered = action_text.lower()
    mentions_competitor = "竞品" in action_text or "competitor" in lowered
    if not mentions_competitor:
        return False
    own_model_tokens = [
        "本车型",
        "请求车型",
        "目标车型",
        "own-model",
        "own model",
        "requested model",
        "current price",
        "当前价格",
        "当前官方",
    ]
    return not any(token in action_text or token in lowered for token in own_model_tokens)


def _repair_source_candidate_items(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    own_model = _compact_source_candidate_list(value.get("ownModel"))
    competitor_corridor = _compact_source_candidate_list(value.get("competitorCorridor"))
    candidate_count = len(own_model) + len(competitor_corridor)
    if candidate_count <= 0:
        return {}
    review_pending_count = int(value.get("reviewPendingObservationCount") or 0) or sum(
        int(entry.get("reviewPendingRows") or 0)
        for entry in [*own_model, *competitor_corridor]
        if isinstance(entry, dict)
    )
    source_search_plan = _source_repair_search_plan_from_candidates(
        own_model=own_model,
        competitor_corridor=competitor_corridor,
    )
    return {
        "dataStatus": str(value.get("dataStatus") or "source_draft_only_not_price_evidence"),
        "missingOwnModelSource": bool(value.get("missingOwnModelSource")),
        "ownModel": own_model,
        "competitorCorridor": competitor_corridor,
        "candidateCount": candidate_count,
        "materializedCandidateCount": int(value.get("materializedCandidateCount") or 0),
        "reviewPendingObservationCount": review_pending_count,
        "sourceSearchPlan": source_search_plan,
    }


def _compact_source_candidate_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, Any]] = []
    for entry in value[:8]:
        if not isinstance(entry, dict):
            continue
        source_code = str(entry.get("sourceCode") or "").strip()
        model = str(entry.get("model") or "").strip()
        source_url = str(entry.get("sourceUrl") or "").strip()
        if not source_code and not model:
            continue
        candidate_type = str(entry.get("candidateSourceType") or "").strip()
        draft_status = str(entry.get("draftStatus") or "").strip()
        source_draft_path = str(entry.get("sourceDraftPath") or "").strip()
        if not source_draft_path and (candidate_type == "source_draft" or draft_status.startswith("source_draft")):
            source_draft_path = str(entry.get("relativePath") or "").strip()
        item: dict[str, Any] = {
            "sourceCode": source_code,
            "brand": str(entry.get("brand") or "").strip(),
            "model": model,
            "sourceUrl": source_url,
            "relativePath": str(entry.get("relativePath") or "").strip(),
            "sourceDraftPath": source_draft_path,
            "draftStatus": draft_status,
            "currentPriceRows": int(entry.get("currentPriceRows") or 0),
            "reviewPendingRows": int(entry.get("reviewPendingRows") or 0),
            "reviewPendingStatus": str(entry.get("reviewPendingStatus") or "").strip(),
            "reviewPendingObservations": [
                item
                for item in entry.get("reviewPendingObservations", [])
                if isinstance(item, dict)
            ][:4],
            "candidateSourceType": candidate_type,
            "candidateDomain": str(entry.get("candidateDomain") or "").strip(),
            "sourceSearchQuery": _source_search_query_from_candidate(entry, source_url=source_url),
        }
        for key in (
            "sourceType",
            "priceSemantics",
            "extractorType",
            "defaultCurrency",
            "priceSelector",
            "trimSelector",
            "vehicleContainerSelector",
            "materializationStatus",
            "materializationReviewStatus",
            "materializationRiskFlags",
            "materializationReadinessScore",
            "materializationMissingFields",
            "materializationRequiredFields",
            "materializationNextStep",
            "materializationWorkflow",
            "materializationGate",
            "safeToAutoMaterialize",
            "priceSanityRules",
            "dryRunCommand",
            "submitCommand",
            "ingestApiPath",
            "reviewChecklist",
        ):
            if entry.get(key) not in (None, "", []):
                item[key] = entry[key]
        items.append(item)
    return items


def _source_search_query_from_candidate(entry: dict[str, Any], *, source_url: str = "") -> str:
    query = str(entry.get("sourceSearchQuery") or "").strip()
    if query:
        return query
    return _google_query_from_url(source_url or str(entry.get("sourceUrl") or ""))


def _google_query_from_url(source_url: str) -> str:
    url = str(source_url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    if "google." not in parsed.netloc.lower():
        return ""
    values = parse_qs(parsed.query).get("q") or []
    if not values:
        return ""
    return str(values[0] or "").strip()


def _source_repair_search_plan_from_candidates(
    *,
    own_model: list[dict[str, Any]],
    competitor_corridor: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for role, rows in (("own_model", own_model), ("competitor_corridor", competitor_corridor)):
        for entry in rows:
            item = _source_candidate_search_plan_item(entry, role=role)
            if item:
                plan.append(item)
    return plan[:8]


def _source_candidate_search_plan_item(entry: Any, *, role: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        return {}
    domain = str(entry.get("candidateDomain") or "").strip()
    source_type = str(entry.get("candidateSourceType") or "").strip()
    source_url = str(entry.get("sourceUrl") or "").strip()
    draft_status = str(entry.get("draftStatus") or "").strip()
    source_draft_path = str(entry.get("sourceDraftPath") or "").strip()
    relative_path = str(entry.get("relativePath") or "").strip()
    if not source_draft_path and (source_type == "source_draft" or draft_status.startswith("source_draft")):
        source_draft_path = relative_path
    query = (
        str(entry.get("sourceSearchQuery") or "").strip()
        or _source_search_query_from_candidate(entry, source_url=source_url)
        or (relative_path if draft_status == "candidate_search_query" else "")
    )
    if draft_status != "candidate_search_query" and not query and not domain and source_type != "source_draft" and not source_url:
        return {}
    brand = str(entry.get("brand") or "").strip()
    model = str(entry.get("model") or "").strip()
    label = " ".join(part for part in (brand, model) if part).strip() or str(entry.get("sourceCode") or "").strip()
    if not label:
        return {}
    return {
        "role": role,
        "label": label,
        "brand": brand,
        "model": model,
        "candidateSourceType": source_type,
        "candidateDomain": domain,
        "draftStatus": draft_status,
        "sourceDraftPath": source_draft_path,
        "relativePath": relative_path,
        "sourceSearchQuery": query,
        "sourceUrl": source_url,
        "sourceType": str(entry.get("sourceType") or "").strip(),
        "priceSemantics": str(entry.get("priceSemantics") or "").strip(),
        "extractorType": str(entry.get("extractorType") or "").strip(),
        "defaultCurrency": str(entry.get("defaultCurrency") or "").strip(),
        "reviewPendingRows": _source_candidate_review_pending_rows(entry),
        "reviewPendingStatus": str(entry.get("reviewPendingStatus") or "").strip(),
        "reviewPendingObservations": [
            item
            for item in entry.get("reviewPendingObservations", [])
            if isinstance(item, dict)
        ][:4],
        "priceSelector": str(entry.get("priceSelector") or "").strip(),
        "trimSelector": str(entry.get("trimSelector") or "").strip(),
        "vehicleContainerSelector": str(entry.get("vehicleContainerSelector") or "").strip(),
        "materializationStatus": str(entry.get("materializationStatus") or "").strip(),
        "materializationReviewStatus": str(entry.get("materializationReviewStatus") or "").strip(),
        "materializationRiskFlags": _string_list(entry.get("materializationRiskFlags")),
        "materializationReadinessScore": entry.get("materializationReadinessScore"),
        "materializationMissingFields": _string_list(entry.get("materializationMissingFields")),
        "materializationRequiredFields": _string_list(entry.get("materializationRequiredFields")),
        "materializationNextStep": str(entry.get("materializationNextStep") or "").strip(),
        "materializationWorkflow": _string_list(entry.get("materializationWorkflow")),
        "materializationGate": str(entry.get("materializationGate") or "").strip(),
        "safeToAutoMaterialize": bool(entry.get("safeToAutoMaterialize")),
        "priceSanityRules": entry.get("priceSanityRules") if isinstance(entry.get("priceSanityRules"), dict) else {},
        "dryRunCommand": str(entry.get("dryRunCommand") or "").strip(),
        "submitCommand": str(entry.get("submitCommand") or "").strip(),
        "ingestApiPath": str(entry.get("ingestApiPath") or "").strip(),
        "reviewChecklist": _string_list(entry.get("reviewChecklist")),
    }


def _source_candidate_search_hints(candidates: list[Any]) -> list[str]:
    hints: list[str] = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        plan_item = _source_candidate_search_plan_item(entry, role="")
        if not plan_item:
            continue
        label = plan_item.get("label") or ""
        query = plan_item.get("sourceSearchQuery") or ""
        domain = plan_item.get("candidateDomain") or ""
        if plan_item.get("candidateSourceType") == "source_draft" and plan_item.get("sourceUrl"):
            hints.append(f"{label}: {plan_item['sourceUrl']}")
        elif query:
            hints.append(f"{label}: {query}")
        elif domain:
            hints.append(f"{label}: site:{domain}")
        elif plan_item.get("sourceUrl"):
            hints.append(f"{label}: {plan_item['sourceUrl']}")
        elif label:
            hints.append(label)
    return hints


def _source_repair_action_text(candidates: dict[str, Any]) -> str:
    if not candidates:
        return ""
    own_model = candidates.get("ownModel") if isinstance(candidates.get("ownModel"), list) else []
    competitor_corridor = candidates.get("competitorCorridor") if isinstance(candidates.get("competitorCorridor"), list) else []
    own_labels = _source_candidate_action_labels(own_model)
    competitor_labels = _source_candidate_action_labels(competitor_corridor)
    pending_entries = [
        entry
        for entry in [*own_model, *competitor_corridor]
        if isinstance(entry, dict) and _source_candidate_review_pending_rows(entry) > 0
    ]
    pending_labels = _source_candidate_action_labels(pending_entries)
    pending_count = sum(_source_candidate_review_pending_rows(entry) for entry in pending_entries)
    if _is_policy_source_repair(candidates) and competitor_labels:
        return (
            "先用外部研究读取并确认政策/新闻官方来源候选"
            f"（{', '.join(competitor_labels[:4])}），补齐发布日期、适用对象和限制条件后再重跑 Business Validation；"
            "这些候选只是搜索/补证据入口，不能直接当作政策事实。"
        )
    if _is_leasing_tco_source_repair(candidates):
        labels = competitor_labels or own_labels
        return (
            "先在外部来源修复表中验证 leasing/TCO/company-car 来源候选"
            f"{_compact_source_candidate_suffix(labels)}，保留标题、URL、发布日期、月供/残值/税务 benefit 口径和适用车型后再重跑 Business Validation；"
            "这些候选只是补证入口，不能直接当作 company-car/TCO 结论。"
        )
    if _is_pricing_external_source_repair(candidates):
        labels = competitor_labels or own_labels
        return (
            "先在外部来源验证矩阵中验证官方价格/MSRP 来源候选"
            f"{_compact_source_candidate_suffix(labels)}，保留标题、URL、发布日期、车型/版本、币种和 MSRP/current price 后再重跑 Business Validation；"
            "这些候选只是补证线索，不能直接当作数值证据。"
        )
    if _is_external_query_source_repair(candidates):
        labels = competitor_labels or own_labels
        return (
            "先在外部来源修复表中验证 VOC/媒体/论坛检索线索"
            f"{_compact_source_candidate_suffix(labels)}，保留标题、URL、发布日期和可支撑原文要点后再重跑 Business Validation；"
            "这些检索线索只是补源入口，不能直接当作用户高频吐槽证据。"
        )
    if pending_entries:
        pending_examples = ", ".join(_compact_source_examples(pending_labels))
        example_text = f"，示例：{pending_examples}" if pending_examples else ""
        return (
            "先在 MSRP review queue 中审核已抓到的官方价格观察"
            f"（共{pending_count}条{example_text}），"
            "确认 trim/版本、币种、发布日期和来源后再生成 current price；"
            "这些观察现在只能作为待审核证据，不能直接当作确定 MSRP。"
        )
    if own_labels:
        if _all_source_candidates_are_search_queries(own_model):
            return (
                "先在 MSRP 来源验证表中验证本车型/竞品官方价格候选"
                f"{_compact_source_candidate_suffix(own_labels)}，确认 URL、版本/配置、币种、发布日期后生成当前价格记录；"
                "这些搜索候选只是补源入口，不能直接当作数值证据。"
            )
        if _any_source_candidates_are_search_queries(own_model):
            return (
                "先在 MSRP 来源验证表中分别验证本车型/竞品官方价格搜索候选和来源草稿"
                f"{_mixed_source_candidate_suffix(own_model)}，确认 URL、版本/配置、币种、发布日期后生成当前价格记录；"
                "这些候选只是补源入口，不能直接当作数值证据。"
            )
        return (
            "先在 MSRP 来源验证表中审核本车型/竞品 MSRP 来源草稿"
            f"{_compact_source_candidate_suffix(own_labels)}，生成当前价格记录后再重跑 Business Validation；"
            "这些草稿只是修复输入，不能直接当作数值证据。"
        )
    if competitor_labels:
        if _all_source_candidates_are_search_queries(competitor_corridor):
            return (
                "先在 MSRP 来源验证表中补齐本车型官方 MSRP 来源，并验证竞品价格搜索候选"
                f"{_compact_source_candidate_suffix(competitor_labels)}建立价格带修复清单；"
                "这些搜索候选只是补源入口，完成后重跑 Business Validation。"
            )
        return (
            "先在 MSRP 来源验证表中补齐本车型官方 MSRP 来源，并审核竞品价格走廊草稿"
            f"{_compact_source_candidate_suffix(competitor_labels)}作为修复清单；"
            "完成后重跑 Business Validation。"
        )
    return ""


def _compact_source_candidate_suffix(labels: list[str]) -> str:
    examples = [
        _truncate_repair_example(str(label or "").strip(), max_chars=64)
        for label in labels
        if str(label or "").strip()
    ][:2]
    if examples:
        return f"（共{len(labels)}项，示例：{', '.join(examples)}）"
    return f"（共{len(labels)}项）"


def _mixed_source_candidate_suffix(candidates: list[Any]) -> str:
    search_labels: list[str] = []
    draft_labels: list[str] = []
    materialized_labels: list[str] = []
    pending_labels: list[str] = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        label = _source_candidate_action_labels([entry])
        if not label:
            continue
        try:
            current_price_rows = int(entry.get("currentPriceRows") or 0)
        except (TypeError, ValueError):
            current_price_rows = 0
        if current_price_rows > 0:
            materialized_labels.extend(label)
        elif _source_candidate_review_pending_rows(entry) > 0:
            pending_labels.extend(label)
        elif str(entry.get("draftStatus") or "").strip() == "candidate_search_query":
            search_labels.extend(label)
        else:
            draft_labels.extend(label)
    parts = []
    if search_labels:
        parts.append(f"搜索候选{len(search_labels)}项：{', '.join(_compact_source_examples(search_labels))}")
    if draft_labels:
        parts.append(f"来源草稿{len(draft_labels)}项：{', '.join(_compact_source_examples(draft_labels))}")
    if pending_labels:
        parts.append(f"待审核观察{len(pending_labels)}项：{', '.join(_compact_source_examples(pending_labels))}")
    if materialized_labels:
        parts.append(f"已物化样本{len(materialized_labels)}项：{', '.join(_compact_source_examples(materialized_labels))}")
    if parts:
        return f"（{'；'.join(parts)}）"
    return _compact_source_candidate_suffix(_source_candidate_action_labels(candidates))


def _compact_source_examples(labels: list[str]) -> list[str]:
    return [
        _truncate_repair_example(str(label or "").strip(), max_chars=64)
        for label in labels
        if str(label or "").strip()
    ][:2]


def _truncate_repair_example(value: str, *, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars].rstrip()}..."


def _is_policy_source_repair(candidates: dict[str, Any]) -> bool:
    return str(candidates.get("dataStatus") or "").strip() == "external_policy_source_candidates"


def _is_leasing_tco_source_repair(candidates: dict[str, Any]) -> bool:
    return str(candidates.get("dataStatus") or "").strip() == "leasing_tco_source_candidates"


def _is_external_query_source_repair(candidates: dict[str, Any]) -> bool:
    return str(candidates.get("dataStatus") or "").strip() == "external_research_query_candidates"


def _is_pricing_external_source_repair(candidates: dict[str, Any]) -> bool:
    if not _is_external_query_source_repair(candidates):
        return False
    values: list[str] = []
    queries = candidates.get("queries") if isinstance(candidates.get("queries"), list) else []
    values.extend(str(query or "") for query in queries)
    for key in ("ownModel", "competitorCorridor", "sourceSearchPlan"):
        entries = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            values.extend(
                str(entry.get(field) or "")
                for field in (
                    "brand",
                    "model",
                    "label",
                    "sourceSearchQuery",
                    "candidateSourceType",
                    "sourceUrl",
                )
            )
    haystack = " ".join(values).casefold()
    if not haystack:
        return False
    if any(token in haystack for token in ("owner review", "complaint", "forum", "klagomål", "omdöme")):
        return False
    return any(token in haystack for token in ("pricing", "official price", "msrp", "pris officiell", "price list"))


def _all_source_candidates_are_search_queries(candidates: list[Any]) -> bool:
    rows = [entry for entry in candidates if isinstance(entry, dict)]
    if not rows:
        return False
    return all(str(entry.get("draftStatus") or "").strip() == "candidate_search_query" for entry in rows)


def _any_source_candidates_are_search_queries(candidates: list[Any]) -> bool:
    return any(
        str(entry.get("draftStatus") or "").strip() == "candidate_search_query"
        for entry in candidates
        if isinstance(entry, dict)
    )


def _source_candidate_labels(candidates: list[Any]) -> list[str]:
    labels: list[str] = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        brand = str(entry.get("brand") or "").strip()
        model = str(entry.get("model") or "").strip()
        source_code = str(entry.get("sourceCode") or "").strip()
        label = " ".join(part for part in [brand, model] if part).strip() or source_code
        if label:
            labels.append(label)
    return labels


def _source_candidate_action_labels(candidates: list[Any]) -> list[str]:
    labels: list[str] = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        model = str(entry.get("model") or "").strip()
        source_code = str(entry.get("sourceCode") or "").strip()
        label = model or source_code
        if label:
            labels.append(label)
    return labels


def _relevant_source_candidate_labels(record: dict[str, Any], candidates: list[Any]) -> list[str]:
    if not candidates:
        return []
    labels = _source_candidate_labels(candidates)
    tokens = _source_candidate_priority_tokens(str(record.get("question") or ""))
    if not tokens:
        return labels
    prioritized: list[str] = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        model = str(entry.get("model") or "").upper()
        label = " ".join(
            part
            for part in [str(entry.get("brand") or "").strip(), str(entry.get("model") or "").strip()]
            if part
        ).strip()
        if label and any(token == model or token in model for token in tokens):
            prioritized.append(label)
    return prioritized or labels


def _source_candidate_priority_tokens(question: str) -> list[str]:
    text = question.upper()
    if any(token in text for token in ("J7", "JAECOO 7", "JAECOO7")):
        return ["COROLLA CROSS", "RAV4", "SPORTAGE", "C-HR", "QASHQAI"]
    if any(token in text for token in ("O5", "OMODA 5", "OMODA5")):
        return ["EV3", "EX30", "ID.4", "ENYAQ", "EQA"]
    if any(token in text for token in ("O9", "OMODA 9", "OMODA9", "J8", "JAECOO 8", "JAECOO8")):
        return ["XC60", "EX60", "XC90", "EX90", "EV9", "SORENTO", "KODIAQ", "TAYRON"]
    return []


def _repair_tasks(
    *,
    question_id: str,
    category: str,
    country: str,
    selected_tool: str,
    missing_evidence: list[dict[str, str]],
    recommended_actions: list[dict[str, str]],
    source_repair_candidates: dict[str, Any],
    repair_action: str,
    queue_priority: str,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    seen_types: set[str] = set()
    own_model = (
        source_repair_candidates.get("ownModel")
        if isinstance(source_repair_candidates.get("ownModel"), list)
        else []
    )
    competitor_corridor = (
        source_repair_candidates.get("competitorCorridor")
        if isinstance(source_repair_candidates.get("competitorCorridor"), list)
        else []
    )
    own_labels = _source_candidate_labels(own_model)
    competitor_labels = _source_candidate_labels(competitor_corridor)
    own_search_hints = _source_candidate_search_hints(own_model)
    competitor_search_hints = _source_candidate_search_hints(competitor_corridor)
    own_search_plan = [
        item
        for item in _source_repair_search_plan_from_candidates(
            own_model=own_model,
            competitor_corridor=[],
        )
        if item.get("role") == "own_model"
    ]
    competitor_search_plan = [
        item
        for item in _source_repair_search_plan_from_candidates(
            own_model=[],
            competitor_corridor=competitor_corridor,
        )
        if item.get("role") == "competitor_corridor"
    ]
    external_query_repair = _is_external_query_source_repair(source_repair_candidates)
    policy_source_repair = _is_policy_source_repair(source_repair_candidates)
    leasing_tco_source_repair = _is_leasing_tco_source_repair(source_repair_candidates)
    own_model_is_search_query = _any_source_candidates_are_search_queries(own_model)
    competitor_corridor_is_search_query = _any_source_candidates_are_search_queries(competitor_corridor)
    materialized_count = int(source_repair_candidates.get("materializedCandidateCount") or 0)
    candidate_count = int(source_repair_candidates.get("candidateCount") or 0)
    primary_missing_name = _primary_missing_evidence_name(missing_evidence)
    competitor_gap_is_primary = _missing_name_is_competitor_price_gap(primary_missing_name)
    priority_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}

    def add_task(
        *,
        task_type: str,
        title: str,
        input_text: str,
        output_text: str,
        owner: str,
        priority: str = "P1",
        evidence_name: str = "",
        source_candidates: list[str] | None = None,
        source_search_plan: list[dict[str, str]] | None = None,
        command_hint: str = "",
    ) -> None:
        dedupe_key = f"{task_type}:{evidence_name or title}"
        normalized_priority = _repair_task_priority(priority, queue_priority)
        existing_core_task = next(
            (
                task
                for task in tasks
                if task_type in {
                    "own_model_msrp_source",
                    "competitor_price_corridor",
                    "external_source_repair",
                    "leasing_tco_evidence",
                }
                and str(task.get("taskType") or "") == task_type
            ),
            None,
        )
        if existing_core_task is not None:
            existing_priority = str(existing_core_task.get("priority") or "P1")
            if priority_order.get(normalized_priority, 9) < priority_order.get(existing_priority, 9):
                existing_core_task["priority"] = normalized_priority
                existing_core_task["input"] = _sanitize_evidence_repair_text(input_text)
                existing_core_task["evidenceName"] = evidence_name
                existing_core_task["commandHint"] = _sanitize_evidence_repair_text(command_hint)
                if source_search_plan:
                    existing_core_task["sourceSearchPlan"] = source_search_plan
            return
        if dedupe_key in seen_types or len(tasks) >= 5:
            return
        seen_types.add(dedupe_key)
        tasks.append({
            "taskId": _repair_task_id(question_id=question_id, task_type=task_type, index=len(tasks) + 1),
            "taskType": task_type,
            "title": _sanitize_evidence_repair_text(title),
            "input": _sanitize_evidence_repair_text(input_text),
            "output": _sanitize_evidence_repair_text(output_text),
            "owner": owner,
            "priority": normalized_priority,
            "status": "todo",
            "evidenceName": evidence_name,
            "sourceCandidates": source_candidates or [],
            "sourceSearchPlan": source_search_plan or [],
            "commandHint": _sanitize_evidence_repair_text(command_hint),
        })

    if external_query_repair and competitor_labels:
        add_task(
            task_type="external_source_repair",
            title="补齐 VOC/媒体/论坛来源",
            input_text="按外部研究检索候选确认可引用来源，保留标题、URL、发布日期和可支撑原文要点。",
            output_text="可引用外部来源证据，包含来源、日期、原文要点和可支撑的业务结论。",
            owner="Research/Data",
            priority="P1",
            evidence_name="external_research_claims_unavailable",
            source_candidates=competitor_labels[:5],
            command_hint=_repair_task_command_hint(
                task_type="external_source_repair",
                question_id=question_id,
                country=country,
                source_candidates=competitor_labels[:5],
            ),
        )

    if policy_source_repair and competitor_labels:
        source_candidates = competitor_search_hints[:5] or competitor_labels[:5]
        add_task(
            task_type="specific_policy_source_evidence",
            title="Attach named policy source evidence",
            input_text="按点名政策官方来源候选确认政策名、发布日期、适用对象、资格边界和车型影响。",
            output_text="问题中点名政策的官方或可引用来源，包含政策名、发布日期、适用对象、资格边界和车型影响。",
            owner="Research",
            priority="P1",
            evidence_name="specific_policy_source_evidence",
            source_candidates=source_candidates,
            source_search_plan=competitor_search_plan[:5],
            command_hint=_repair_task_command_hint(
                task_type="specific_policy_source_evidence",
                question_id=question_id,
                country=country,
                source_candidates=source_candidates,
            ),
        )

    if leasing_tco_source_repair and competitor_labels:
        source_candidates = competitor_search_hints[:5] or competitor_labels[:5]
        add_task(
            task_type="leasing_tco_evidence",
            title="Build leasing/TCO evidence",
            input_text="按 leasing/TCO/company-car 来源候选确认月供、残值/RV、税务 benefit、年里程、充电条件和适用车型。",
            output_text=_repair_task_output("leasing_tco_evidence"),
            owner="Data/Ops",
            priority="P0",
            evidence_name="leasing_tco_or_company_car_evidence",
            source_candidates=source_candidates,
            source_search_plan=competitor_search_plan[:5],
            command_hint=_repair_task_command_hint(
                task_type="leasing_tco_evidence",
                question_id=question_id,
                country=country,
                source_candidates=source_candidates,
            ),
        )

    if (
        not external_query_repair
        and not policy_source_repair
        and not leasing_tco_source_repair
        and competitor_gap_is_primary
        and competitor_labels
    ):
        priority = "P1" if materialized_count >= min(candidate_count, len(competitor_labels)) else "P0"
        source_candidates = (
            competitor_search_hints[:5]
            if competitor_corridor_is_search_query
            else competitor_labels[:5]
        )
        add_task(
            task_type="competitor_price_corridor",
            title="Validate competitor price corridor",
            input_text="审核同国家竞品来源候选，确认价格走廊覆盖是否足够。",
            output_text="竞品价格走廊证据，包含可比车型/版本和价格位置。",
            owner="Data/Ops",
            priority=priority,
            evidence_name="competitor_price_corridor",
            source_candidates=source_candidates,
            source_search_plan=competitor_search_plan[:5],
            command_hint=_repair_task_command_hint(
                task_type="competitor_price_corridor",
                question_id=question_id,
                country=country,
                source_candidates=source_candidates,
            ),
        )

    if bool(source_repair_candidates.get("missingOwnModelSource")):
        add_task(
            task_type="own_model_msrp_source",
            title="创建本车型官方 MSRP 来源",
            input_text=_repair_task_input(country=country, category=category, selected_tool=selected_tool),
            output_text="本车型当前价格证据，包含车型、版本、MSRP、币种、来源和获取时间。",
            owner="Data/Ops",
            priority="P1" if competitor_gap_is_primary else "P0",
            evidence_name="own_model_current_msrp",
            source_candidates=own_labels,
            source_search_plan=own_search_plan[:4],
            command_hint=_repair_task_command_hint(
                task_type="find_own_model_msrp_source" if own_model_is_search_query else "own_model_msrp_source",
                question_id=question_id,
                country=country,
                source_candidates=own_search_hints[:4] if own_model_is_search_query else own_labels,
            ),
        )

    if own_labels:
        source_candidates = own_search_hints[:4] if own_model_is_search_query else own_labels[:4]
        add_task(
            task_type="find_own_model_msrp_source" if own_model_is_search_query else "promote_own_model_source_draft",
            title="查找本车型官方 MSRP 价格候选" if own_model_is_search_query else "Promote own-model source drafts",
            input_text=(
                "按官方价格搜索候选确认本车型来源 URL、版本/配置、币种和发布日期，并生成可查询的当前价格记录。"
                if own_model_is_search_query
                else "审核本车型来源草稿，并生成可查询的当前价格记录。"
            ),
            output_text="本车型当前价格记录已入库，可进入证据包使用。",
            owner="Data/Ops",
            priority="P0",
            evidence_name="own_model_current_msrp",
            source_candidates=source_candidates,
            source_search_plan=own_search_plan[:4],
            command_hint=_repair_task_command_hint(
                task_type="find_own_model_msrp_source"
                if own_model_is_search_query
                else "promote_own_model_source_draft",
                question_id=question_id,
                country=country,
                source_candidates=source_candidates,
            ),
        )

    if not external_query_repair and not policy_source_repair and not leasing_tco_source_repair and competitor_labels:
        priority = "P1" if materialized_count >= min(candidate_count, len(competitor_labels)) else "P0"
        source_candidates = (
            competitor_search_hints[:5]
            if competitor_corridor_is_search_query
            else competitor_labels[:5]
        )
        add_task(
            task_type="competitor_price_corridor",
            title="Validate competitor price corridor",
            input_text=(
                "按竞品官方价格搜索候选确认可比车型/版本/币种和发布日期，建立价格走廊修复清单。"
                if competitor_corridor_is_search_query
                else "审核同国家竞品来源候选，确认价格走廊覆盖是否足够。"
            ),
            output_text="竞品价格走廊证据，包含可比车型/版本和价格位置。",
            owner="Data/Ops",
            priority=priority,
            evidence_name="competitor_price_corridor",
            source_candidates=source_candidates,
            source_search_plan=competitor_search_plan[:5],
            command_hint=_repair_task_command_hint(
                task_type="competitor_price_corridor",
                question_id=question_id,
                country=country,
                source_candidates=source_candidates,
            ),
        )

    for item in missing_evidence:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        reason = str(item.get("reason") or "").strip()
        impact = str(item.get("impact") or "weakens_answer").strip()
        task_type, title, owner = _repair_task_kind(name=name, category=category)
        priority = "P0" if impact == "blocking" else "P1"
        source_candidates = _repair_task_source_candidates(
            task_type=task_type,
            own_labels=own_labels,
            competitor_labels=competitor_labels,
        )
        add_task(
            task_type=task_type,
            title=title,
            input_text=reason or _repair_task_input(country=country, category=category, selected_tool=selected_tool),
            output_text=_repair_task_output(task_type),
            owner=owner,
            priority=priority,
            evidence_name=name,
            source_candidates=source_candidates,
            command_hint=_repair_task_command_hint(
                task_type="find_own_model_msrp_source"
                if task_type == "own_model_msrp_source" and own_model_is_search_query
                else task_type,
                question_id=question_id,
                country=country,
                source_candidates=source_candidates,
            ),
        )

    for action in recommended_actions[:2]:
        add_task(
            task_type="recommended_action",
            title=str(action.get("action") or "Apply recommended repair action").strip(),
            input_text=str(action.get("rationale") or repair_action or "").strip(),
            output_text="证据或路由行为已更新，然后重跑受影响的 Business Validation 问题。",
            owner="AstrBot Eval",
            priority=str(action.get("priority") or "P1"),
            evidence_name="recommended_action",
            command_hint=_repair_task_command_hint(
                task_type="recommended_action",
                question_id=question_id,
                country=country,
            ),
        )

    add_task(
        task_type="rerun_business_validation",
        title="Rerun affected validation item",
        input_text=f"数据/工具修复后的问题：{question_id or 'unknown'}。",
        output_text="生成新的 side-by-side 记录，包含更新后的证据包、失败标签和修复摘要。",
        owner="AstrBot Eval",
        priority="P1",
        evidence_name="validation_rerun",
        command_hint=_repair_task_command_hint(
            task_type="rerun_business_validation",
            question_id=question_id,
            country=country,
        ),
    )
    tasks.sort(key=lambda task: priority_order.get(str(task.get("priority") or "P1"), 9))
    for index, task in enumerate(tasks, start=1):
        task["taskId"] = _repair_task_id(
            question_id=question_id,
            task_type=str(task.get("taskType") or "repair_task"),
            index=index,
        )
    return tasks


def _primary_missing_evidence_name(missing_evidence: list[dict[str, str]]) -> str:
    for item in missing_evidence:
        if str(item.get("impact") or "") == "blocking":
            return str(item.get("name") or "")
    if missing_evidence:
        return str(missing_evidence[0].get("name") or "")
    return ""


def _missing_name_is_competitor_price_gap(name: str) -> bool:
    lowered = str(name or "").lower()
    return (
        "competitor_price" in lowered
        or "price_corridor" in lowered
        or "price_or_config_gap" in lowered
    )


def _repair_task_id(*, question_id: str, task_type: str, index: int) -> str:
    raw = f"{question_id or 'repair'}_{index}_{task_type}"
    safe = "".join(char.lower() if char.isalnum() else "_" for char in raw)
    return "_".join(part for part in safe.split("_") if part)[:96]


def _repair_task_priority(priority: str, queue_priority: str) -> str:
    requested = (priority or "P1").strip().upper()
    parent = (queue_priority or "P1").strip().upper()
    if parent != "P0" and requested == "P0":
        return "P1"
    return requested or "P1"


def _repair_task_input(*, country: str, category: str, selected_tool: str) -> str:
    parts = [
        f"country={country}" if country else "",
        f"category={category}" if category else "",
        f"selectedTool={selected_tool}" if selected_tool else "",
    ]
    return "; ".join(part for part in parts if part) or "Review the failing Business Validation record."


def _repair_task_kind(*, name: str, category: str) -> tuple[str, str, str]:
    lowered = f"{category} {name}".lower()
    if (
        "external_research" in lowered
        or "minimum_external_sources" in lowered
        or "voc" in lowered
    ):
        return ("external_source_repair", "补齐 VOC/媒体/论坛来源", "Research/Data")
    if "inventory_bom" in lowered or "query_with_filters_weak_evidence_refs" in lowered:
        return ("bom_entity_mapping_evidence", "Map BOM/entity evidence refs", "Product/Data")
    if (
        "current_msrp" in lowered
        or "current_prices" in lowered
        or "own_model_price" in lowered
        or "no_current_prices" in lowered
    ):
        return ("own_model_msrp_source", "Create own-model MSRP evidence", "Data/Ops")
    if "pricing_data_unavailable" in lowered or "price_matrix" in lowered:
        return ("pricing_matrix_evidence", "Build pricing matrix evidence", "Data/Ops")
    if any(token in lowered for token in ("leasing", "tco", "company_car", "company car", "residual", "fleet")):
        return ("leasing_tco_evidence", "Build leasing/TCO evidence", "Data/Ops")
    if "price_corridor" in lowered or "competitor_price" in lowered or "price_or_config_gap" in lowered:
        return ("competitor_price_corridor", "Complete price/value corridor evidence", "Data/Ops")
    if "no_config_projects" in lowered:
        return ("engineering_config_project_coverage", "Import engineering config projects", "Product/Data")
    if "no_config_subjects" in lowered:
        return ("engineering_config_model_mapping", "Map models to config variants", "Product/Data")
    if "competitive_or_configuration_data_unavailable" in lowered:
        return ("competitive_config_matrix", "Build competitor/configuration evidence matrix", "Product/PM")
    if any(token in lowered for token in ["config", "variant", "trim", "feature"]):
        return ("config_gap_evidence", "Map configuration gap evidence", "Product/PM")
    if "specific_policy_source" in lowered:
        return ("specific_policy_source_evidence", "Attach named policy source evidence", "Research")
    if any(token in lowered for token in ["policy", "published", "source_date", "date", "news"]):
        return ("source_date_evidence", "Attach dated external source evidence", "Research")
    if "tool" in lowered or "missing_required_tool" in lowered:
        return ("tool_coverage_guard", "Fix required tool coverage", "Engineering")
    return ("evidence_ref_mapping", "Map missing evidence", "Data/Ops")


def _repair_task_output(task_type: str) -> str:
    mapping = {
        "own_model_msrp_source": "本车型 MSRP/当前价格证据，包含来源和获取日期。",
        "pricing_matrix_evidence": "本车型与竞品 MSRP/TP/月供价格矩阵，包含来源、日期和可比版本口径。",
        "leasing_tco_evidence": "大客户 leasing/TCO/残值/company-car benefit 证据，包含来源、日期、计算口径和车型影响。",
        "competitor_price_corridor": "可比竞品价格走廊证据，并给出产品含义。",
        "external_source_repair": "可引用外部来源证据，包含来源、日期、原文要点和可支撑的业务结论。",
        "competitive_config_matrix": "竞品/配置矩阵证据，包含车型、版本、关键配置、用户价值和来源。",
        "config_gap_evidence": "配置差异证据，包含用户价值和优先级。",
        "bom_entity_mapping_evidence": "BOM/物料号/版本/颜色实体映射证据，包含关系、生命周期和可查询字段。",
        "source_date_evidence": "政策/新闻结论具备带日期的官方或行业来源。",
        "specific_policy_source_evidence": "问题中点名政策的官方或可引用来源，包含政策名、发布日期、适用对象、资格边界和车型影响。",
        "tool_coverage_guard": "IntentToolMatrix 与实际执行工具在该意图下对齐。",
        "evidence_ref_mapping": "缺失证据已进入证据包，并能在 Analysis Path 中看到。",
    }
    return mapping.get(task_type, "修复证据路径，并更新验证记录。")


def _repair_task_command_hint(
    *,
    task_type: str,
    question_id: str,
    country: str,
    source_candidates: list[str] | None = None,
) -> str:
    candidate_text = ", ".join((source_candidates or [])[:4])
    if task_type in {"own_model_msrp_source", "promote_own_model_source_draft"}:
        suffix = f" 候选来源：{candidate_text}。" if candidate_text else ""
        return (
            "用 Data Ops MSRP 来源流程审核/发布"
            f"{country or '目标国家'}的来源草稿；生成当前价格记录后，"
            f"重跑 {question_id or '受影响样本'}。"
            f"{suffix}"
        )
    if task_type == "find_own_model_msrp_source":
        suffix = f" 建议先查：{candidate_text}。" if candidate_text else ""
        return (
            "按官方价格搜索候选确认本车型来源 URL、版本/配置、币种和发布日期；"
            "生成当前价格记录后，"
            f"重跑 {question_id or '受影响样本'}。"
            "这些候选只是补源入口，不能直接当作官方 MSRP 证据。"
            f"{suffix}"
        )
    if task_type == "competitor_price_corridor":
        suffix = f" 建议先看：{candidate_text}。" if candidate_text else ""
        return (
            "先生成足够的同国家竞品 MSRP 价格行，形成价格走廊；"
            "完成前不要把走廊数字当作确定证据。"
            f"{suffix}"
        )
    if task_type == "external_source_repair":
        suffix = f" 建议先看：{candidate_text}。" if candidate_text else ""
        return (
            "按外部研究检索候选补齐可引用来源，保留标题、URL、发布日期和可支撑原文要点；"
            "完成前不要把检索 query 当作用户高频结论。"
            f"{suffix}"
        )
    if task_type == "leasing_tco_evidence":
        suffix = f" 建议先看：{candidate_text}。" if candidate_text else ""
        return (
            "按 leasing/TCO/company-car 来源候选补齐月供、残值/RV、税务 benefit、年里程、充电条件和适用车型；"
            "完成前不要把泛 leasing 目录或市场背景当作 company-car/TCO 结论。"
            f"{suffix}"
        )
    if task_type == "pricing_matrix_evidence":
        return (
            "补齐本车型与核心竞品的 MSRP、TP、月供和版本口径，"
            "形成同国家价格矩阵；完成前不要输出确定价格结论。"
        )
    if task_type == "source_date_evidence":
        return (
            "刷新外部研究，优先使用带发布日期的官方/行业来源；"
            "确认证据包里有可追溯日期。"
        )
    if task_type == "specific_policy_source_evidence":
        suffix = f" 建议先查：{candidate_text}。" if candidate_text else ""
        return (
            "按点名政策官方来源候选确认政策名、发布日期、适用对象、资格边界和车型影响；"
            f"完成前不要把泛政策背景当作 {question_id or '该样本'} 的确定政策事实。"
            f"{suffix}"
        )
    if task_type == "config_gap_evidence":
        return (
            "把工程配置表或版本配置行映射成可引用配置差异证据，"
            f"然后重跑 {question_id or '受影响配置样本'}。"
        )
    if task_type == "competitive_config_matrix":
        return (
            "补齐目标车型与核心竞品的配置矩阵，包括车型/版本、动力、电池/续航、冬季包、拖车/V2H/ADAS 等关键字段；"
            f"完成后重跑 {question_id or '受影响竞品配置样本'}。"
        )
    if task_type == "bom_entity_mapping_evidence":
        return (
            "把 BOM/物料号/车型版本/内外饰颜色映射成可引用实体关系证据，"
            "至少包含 material_code、variant、market、lifecycle_status 和 editable_quantity 口径；"
            f"完成后重跑 {question_id or '受影响 BOM 样本'}。"
        )
    if task_type == "tool_coverage_guard":
        return (
            "检查 IntentToolMatrix 必选工具与实际执行工具；"
            "补齐缺失工具路径后重跑样本。"
        )
    if task_type == "rerun_business_validation":
        return (
            "From backend: PYTHONPATH=. ../../.venv/bin/python -c "
            "\"from app.services.jato_eval_service import "
            "run_business_validation_question; "
            f"print(run_business_validation_question('{question_id}').get('comparisonId'))\""
        )
    if task_type == "recommended_action":
        return "执行上面的修复动作，然后重跑受影响的验证样本。"
    return "修复证据路径，检查 Analysis Path，再重跑验证。"


def _repair_task_source_candidates(
    *,
    task_type: str,
    own_labels: list[str],
    competitor_labels: list[str],
) -> list[str]:
    if task_type == "own_model_msrp_source":
        return own_labels[:4]
    if task_type == "competitor_price_corridor":
        return competitor_labels[:5]
    if task_type == "external_source_repair":
        return competitor_labels[:5]
    if task_type == "leasing_tco_evidence":
        return competitor_labels[:5]
    if task_type == "specific_policy_source_evidence":
        return competitor_labels[:5]
    return []


def _repair_summary(
    *,
    missing_evidence: list[dict[str, str]],
    source_repair_candidates: dict[str, Any],
    repair_action: str,
    failure_tags: list[str],
) -> dict[str, Any]:
    blocking_count = sum(
        1
        for item in missing_evidence
        if str(item.get("impact") or "") == "blocking"
    )
    weak_count = sum(
        1
        for item in missing_evidence
        if str(item.get("impact") or "") == "weakens_answer"
    )
    own_model = (
        source_repair_candidates.get("ownModel")
        if isinstance(source_repair_candidates.get("ownModel"), list)
        else []
    )
    competitor_corridor = (
        source_repair_candidates.get("competitorCorridor")
        if isinstance(source_repair_candidates.get("competitorCorridor"), list)
        else []
    )
    raw_candidate_count = (
        source_repair_candidates.get("candidateCount")
        if source_repair_candidates
        else 0
    )
    candidate_count = int(raw_candidate_count or 0)
    if candidate_count <= 0:
        candidate_count = len(own_model) + len(competitor_corridor)
    raw_materialized_count = (
        source_repair_candidates.get("materializedCandidateCount")
        if source_repair_candidates
        else 0
    )
    materialized_count = int(raw_materialized_count or 0)
    primary_gap = ""
    for item in missing_evidence:
        if str(item.get("impact") or "") == "blocking":
            primary_gap = str(item.get("name") or "")
            break
    if not primary_gap and missing_evidence:
        primary_gap = str(missing_evidence[0].get("name") or "")
    if not primary_gap and failure_tags:
        primary_gap = failure_tags[0]
    data_status = str(source_repair_candidates.get("dataStatus") or "").strip()
    if _is_leasing_tco_source_repair(source_repair_candidates) and any(
        str(item.get("name") or "") == "leasing_tco_or_company_car_evidence"
        for item in missing_evidence
    ):
        primary_gap = "leasing_tco_or_company_car_evidence"
    missing_own_model = bool(source_repair_candidates.get("missingOwnModelSource"))
    source_summary_parts = []
    if candidate_count > 0:
        if _is_policy_source_repair(source_repair_candidates):
            source_summary_parts.append(
                f"{materialized_count}/{candidate_count} 个政策来源候选已确认"
            )
        elif _is_leasing_tco_source_repair(source_repair_candidates):
            source_summary_parts.append(
                f"{candidate_count} 个 leasing/TCO/company-car 来源候选待验证"
            )
        elif _is_external_query_source_repair(source_repair_candidates):
            source_summary_parts.append(
                f"{candidate_count} 个外部研究检索候选待验证"
            )
        else:
            source_summary_parts.append(
                f"{materialized_count}/{candidate_count} 个来源候选已生成价格行"
            )
    if missing_own_model:
        source_summary_parts.append("本车型来源缺失")
    if data_status:
        source_summary_parts.append(_sanitize_evidence_repair_text(data_status.replace("_", " ")))
    return {
        "primaryGap": primary_gap,
        "missingEvidenceCount": len(missing_evidence),
        "blockingEvidenceCount": blocking_count,
        "weakEvidenceCount": weak_count,
        "sourceCandidateCount": candidate_count,
        "ownModelCandidateCount": len(own_model),
        "competitorCandidateCount": len(competitor_corridor),
        "materializedCandidateCount": materialized_count,
        "missingOwnModelSource": missing_own_model,
        "dataStatus": data_status,
        "sourceSummary": "; ".join(source_summary_parts),
        "nextStep": repair_action,
    }


def _default_evidence_repair_action(missing_evidence: list[dict[str, str]]) -> str:
    names = {str(item.get("name") or "") for item in missing_evidence}
    if any("coverage_diagnostic:no_current_prices_for_country" in name for name in names):
        return "补齐请求国家的当前价格观测后，再重跑 Business Validation。"
    if any("current_msrp" in name or "own_model_price" in name for name in names):
        return "先补齐本车型当前官方 MSRP、来源 URL 和获取日期，再输出确定价格数字。"
    if any("pricing_data_unavailable" in name for name in names):
        return "补齐本车型与竞品 MSRP / TP / 月供价格矩阵，并重跑定价验证样本。"
    if any("price_corridor" in name or "competitor_price_range" in name for name in names):
        return "补齐竞品价格走廊证据，并重跑定价验证样本。"
    return "补齐缺失证据后再重跑 Business Validation。"


def _repair_queue_command_hint(
    repair_tasks: list[dict[str, Any]],
    repair_action: str,
) -> str:
    for task in repair_tasks:
        if not isinstance(task, dict):
            continue
        command_hint = str(task.get("commandHint") or "").strip()
        if command_hint:
            return _sanitize_evidence_repair_text(command_hint)
    return _sanitize_evidence_repair_text(repair_action)


def _repair_action_text(item: dict[str, Any]) -> str:
    action = str(item.get("repairAction") or "").strip()
    if action:
        return _sanitize_evidence_repair_text(action)
    actions = item.get("recommendedActions") if isinstance(item.get("recommendedActions"), list) else []
    for entry in actions:
        if isinstance(entry, dict):
            action = str(entry.get("action") or "").strip()
            if action:
                return _sanitize_evidence_repair_text(action)
    missing = item.get("missingEvidence") if isinstance(item.get("missingEvidence"), list) else []
    return _sanitize_evidence_repair_text(_default_evidence_repair_action([
        entry for entry in missing if isinstance(entry, dict)
    ]))


def _repair_tasks_text(item: dict[str, Any]) -> str:
    tasks = item.get("repairTasks") if isinstance(item.get("repairTasks"), list) else []
    task_lines = []
    for task in tasks[:4]:
        if not isinstance(task, dict):
            continue
        title = str(task.get("title") or "").strip()
        if not title:
            continue
        priority = str(task.get("priority") or "P1").strip()
        owner = str(task.get("owner") or "").strip()
        task_line = f"[{priority}] {title}"
        if owner:
            task_line += f" ({owner})"
        task_lines.append(task_line)
    return "; ".join(task_lines)


def _replacement_readiness_verdict(
    *,
    scored: int,
    astrbot_win_rate: float,
    astrbot_avg: float,
    country_avg: float,
    astrbot_errors: int,
    hallucination_risk_count: int = 0,
) -> str:
    if scored < 10:
        return "not_enough_human_scores"
    if astrbot_errors > 0:
        return "not_ready_astrbot_errors"
    if hallucination_risk_count > 0:
        return "not_ready_hallucination_risk"
    if astrbot_win_rate >= 0.7 and astrbot_avg >= country_avg and astrbot_avg >= 4.0:
        return "ready_for_limited_default_trial"
    if astrbot_win_rate >= 0.55 and astrbot_avg >= 3.6:
        return "continue_shadow_mode"
    return "not_ready_keep_copilot_default"


def _replacement_readiness_summary(
    *,
    count: int,
    scored: int,
    pending: int,
    source_counts: dict[str, int],
    verdict: str,
    astrbot_win_rate: float,
    astrbot_avg: float,
    country_avg: float,
    astrbot_errors: int,
    country_errors: int,
    hallucination_risk_count: int,
    failure_tag_counts: dict[str, int],
) -> dict[str, Any]:
    min_required = min(count, max(8, (count * 7 + 9) // 10)) if count > 0 else 8
    failure_total = sum(int(value or 0) for value in failure_tag_counts.values())
    business_baseline_ready = scored >= min_required
    win_rate_ready = business_baseline_ready and astrbot_win_rate >= 0.7
    replacement_ready = verdict == "ready_for_limited_default_trial"
    execution_clean = astrbot_errors == 0 and country_errors == 0
    hallucination_clean = hallucination_risk_count == 0
    if replacement_ready:
        status = "ready"
    elif not execution_clean:
        status = "execution_blocked"
    elif not hallucination_clean:
        status = "grounding_blocked"
    elif not business_baseline_ready:
        status = "scoring_pending"
    elif not win_rate_ready:
        status = "quality_pending"
    else:
        status = "shadow_mode"

    reasons: list[str] = []
    if not execution_clean:
        reasons.append(f"Execution errors remain: AstrBot {astrbot_errors}, CountryCopilot {country_errors}.")
    if not hallucination_clean:
        reasons.append(f"Hallucination risk tags remain: {hallucination_risk_count}.")
    if not business_baseline_ready:
        reasons.append(f"Need {max(0, min_required - scored)} more manual/GPT business scores.")
    if business_baseline_ready and not win_rate_ready:
        reasons.append("AstrBot replacement win rate is below the 70% gate.")
    if not reasons and not replacement_ready:
        reasons.append(f"Backend verdict is {verdict}.")

    if not execution_clean:
        next_action = "Fix side-by-side execution errors before scoring more records."
    elif not hallucination_clean:
        next_action = "Fix grounding/hallucination-risk records before considering replacement."
    elif not business_baseline_ready:
        next_action = f"Score {max(0, min_required - scored)} more business records with manual or GPT judge sources."
    elif not win_rate_ready:
        next_action = "Review losing categories and improve AstrBot before switching default traffic."
    elif replacement_ready:
        next_action = "AstrBot can enter a limited default-route trial behind a feature flag."
    else:
        next_action = "Keep /copilot as default and continue shadow-mode validation."

    return {
        "status": status,
        "verdict": verdict,
        "replacementReady": replacement_ready,
        "businessBaselineReady": business_baseline_ready,
        "winRateReady": win_rate_ready,
        "executionClean": execution_clean,
        "hallucinationClean": hallucination_clean,
        "totalQuestions": count,
        "minimumRequiredScores": min_required,
        "scoredCount": scored,
        "pendingCount": pending,
        "sourceCounts": dict(sorted(source_counts.items())),
        "astrbotWinRate": astrbot_win_rate,
        "avgAstrBotScore": astrbot_avg,
        "avgCountryCopilotScore": country_avg,
        "astrbotErrorCount": astrbot_errors,
        "countryCopilotErrorCount": country_errors,
        "hallucinationRiskCount": hallucination_risk_count,
        "failureTagTotal": failure_total,
        "reasons": reasons,
        "recommendedNextAction": next_action,
    }


def _self_test_baseline_summary(
    *,
    count: int,
    scored: int,
    pending: int,
    source_counts: dict[str, int],
    astrbot_win_rate: float,
    astrbot_avg: float,
    country_avg: float,
) -> dict[str, Any]:
    min_required = min(count, max(8, (count * 7 + 9) // 10)) if count > 0 else 8
    self_test_ready = scored >= min_required
    codex_reviewed = int(source_counts.get("codex_review", 0) or 0) + int(source_counts.get("codex_review_draft", 0) or 0)
    trusted_baseline = int(source_counts.get("manual", 0) or 0) + int(source_counts.get("llm_judge", 0) or 0)
    if self_test_ready:
        status = "ready"
        next_action = "Use self-test findings to prioritize fixes, then confirm enough rows with manual or GPT judge before replacement."
    elif codex_reviewed > 0:
        status = "in_progress"
        next_action = f"Review {max(0, min_required - scored)} more records with Codex/manual/GPT to complete the self-test baseline."
    else:
        status = "not_started"
        next_action = "Run the AstrBot review harness or score records manually to build a self-test baseline."
    return {
        "status": status,
        "selfTestReady": self_test_ready,
        "totalQuestions": count,
        "minimumRequiredScores": min_required,
        "scoredCount": scored,
        "pendingCount": pending,
        "sourceCounts": dict(sorted(source_counts.items())),
        "codexReviewedCount": codex_reviewed,
        "trustedBaselineCount": trusted_baseline,
        "astrbotWinRate": astrbot_win_rate,
        "avgAstrBotScore": astrbot_avg,
        "avgCountryCopilotScore": country_avg,
        "recommendedNextAction": next_action,
    }


def _self_test_baseline_from_records(
    *,
    results: list[dict[str, Any]],
    source_counts: dict[str, int],
    saved_astrbot_scores: list[float],
    saved_country_scores: list[float],
    saved_wins: dict[str, int],
) -> dict[str, Any]:
    self_test_source_counts = dict(source_counts)
    astrbot_scores = list(saved_astrbot_scores)
    country_scores = list(saved_country_scores)
    wins = dict(saved_wins)
    saved_scored_questions = {
        question_id
        for record in results
        if (question_id := _text(record.get("questionId")))
        and _record_has_complete_human_scoring(record)
    }
    notes_by_question_id = _latest_codex_review_notes_by_question_id()
    for record in results:
        question_id = _text(record.get("questionId"))
        if not question_id or question_id in saved_scored_questions:
            continue
        note_projection = _codex_review_note_score_projection(notes_by_question_id.get(question_id))
        if not note_projection:
            continue
        self_test_source_counts["codex_review_draft"] = self_test_source_counts.get("codex_review_draft", 0) + 1
        astrbot_scores.append(note_projection["astrbotScore"])
        country_scores.append(note_projection["countryCopilotScore"])
        winner = note_projection["winner"]
        if winner in wins:
            wins[winner] = wins.get(winner, 0) + 1
    scored = sum(self_test_source_counts.values())
    return _self_test_baseline_summary(
        count=len(results),
        scored=scored,
        pending=max(0, len(results) - scored),
        source_counts=self_test_source_counts,
        astrbot_win_rate=round(wins.get("astrbot", 0) / scored, 3) if scored else 0,
        astrbot_avg=round(sum(astrbot_scores) / len(astrbot_scores), 2) if astrbot_scores else 0,
        country_avg=round(sum(country_scores) / len(country_scores), 2) if country_scores else 0,
    )


def _record_has_complete_human_scoring(record: dict[str, Any]) -> bool:
    scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
    totals = scoring.get("scoreTotals") if isinstance(scoring.get("scoreTotals"), dict) else {}
    return scoring.get("status") == "scored" and totals.get("complete") is True


def _latest_codex_review_notes_by_question_id(notes: list[dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    source_notes = notes if notes is not None else _read_codex_review_notes()
    for note in source_notes:
        question_id = _text(note.get("questionId"))
        if question_id:
            latest[question_id] = note
    return latest


def _codex_review_note_score_projection(note: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(note, dict):
        return None
    suggested_scores = note.get("suggestedScores") if isinstance(note.get("suggestedScores"), dict) else {}
    dimensions = list(_BUSINESS_SCORE_DIMENSIONS)
    astrbot_scores = _read_manual_scores(suggested_scores.get("astrbot"), dimensions)
    country_scores = _read_manual_scores(
        suggested_scores.get("countryCopilot") or suggested_scores.get("copilot"),
        dimensions,
    )
    totals = _manual_score_totals(astrbot_scores, country_scores, dimensions)
    if totals.get("complete") is not True:
        return None
    winner = _text(note.get("suggestedWinner")).strip()
    if winner not in _HUMAN_SCORE_WINNERS or not winner:
        winner = _winner_from_manual_totals(totals)
    return {
        "winner": winner,
        "astrbotScore": float(totals.get("astrbot") or 0),
        "countryCopilotScore": float(totals.get("countryCopilot") or 0),
    }


def _summarize_astrbot_side(
    result: dict[str, Any] | None,
    scores: dict[str, Any],
    error: str | None,
    *,
    country: str = "",
    question: str = "",
) -> dict[str, Any]:
    if not result:
        return {
            "status": "failed",
            "error": error or "AstrBot result unavailable",
            "scores": scores,
        }
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    answer = data.get("answer") if isinstance(data.get("answer"), dict) else {}
    retrieval = data.get("retrievalClassification") if isinstance(data.get("retrievalClassification"), dict) else {}
    evidence_pack = data.get("evidencePack") if isinstance(data.get("evidencePack"), dict) else {}
    evidence_package = data.get("evidencePackage") if isinstance(data.get("evidencePackage"), dict) else {}
    visual_artifacts = answer.get("visualArtifacts")
    if not isinstance(visual_artifacts, list):
        visual_artifacts = data.get("visualArtifacts") if isinstance(data.get("visualArtifacts"), list) else []
    model_usage = data.get("modelUsage") if isinstance(data.get("modelUsage"), dict) else {}
    quality_score = data.get("qualityScore") if isinstance(data.get("qualityScore"), dict) else {}
    follow_ups = answer.get("structuredFollowUps") if isinstance(answer.get("structuredFollowUps"), list) else answer.get("followUps")
    business_synthesis = answer.get("businessSynthesisPlan") if isinstance(answer.get("businessSynthesisPlan"), dict) else {}
    answer_preview = _business_review_answer_text(answer, question=question)
    actual_tools = _actual_tools_from_result(result)
    source_repair_candidates = _filter_source_repair_candidates_for_question(
        _source_repair_candidates_from_evidence_package(evidence_package),
        question=question,
        country=country,
    )
    source_repair_candidates = _source_repair_candidates_with_leasing_tco_candidates(
        source_repair_candidates,
        country=country,
        question=question,
        missing_evidence=evidence_package.get("missingEvidence"),
    )
    return {
        "status": "ok",
        "answerTitle": _text(answer.get("title")) or "AstrBot answer",
        "answerPreview": answer_preview[:3000],
        "bullets": _string_list(answer.get("bullets"))[:6],
        "limitations": _string_list(answer.get("limitations"))[:6],
        "answerStatus": _text(answer.get("answerStatus")) or "unknown",
        "confidence": _text(answer.get("confidence")),
        "selectedTool": _text(metadata.get("selectedTool")),
        "actualTools": actual_tools,
        "retrievalPath": _text(retrieval.get("primaryPath")),
        "allRetrievalPaths": _string_list(retrieval.get("allPaths")),
        "evidenceCount": _list_len(evidence_pack.get("items")),
        "sourceCount": max(
            _list_len(evidence_pack.get("sources")),
            _external_source_count_from_evidence_package(evidence_package),
        ),
        "evidencePackage": _shrink_for_record(evidence_package),
        "evidenceRefCount": _evidence_ref_count(evidence_package),
        "evidenceConfidence": _text(evidence_package.get("confidence")),
        "evidenceDigest": _string_list(answer.get("evidenceDigest"))[:6],
        "displayPlan": _text(answer.get("displayPlan")),
        "missingEvidence": _shrink_for_record(evidence_package.get("missingEvidence")),
        "sourceRepairCandidates": _shrink_for_record(source_repair_candidates),
        "qualityScore": quality_score,
        "visualArtifacts": _shrink_visual_artifacts_for_record(visual_artifacts),
        "businessSynthesisPlan": _shrink_for_record(business_synthesis),
        "businessSynthesisScore": quality_score.get("businessSynthesisScore") if isinstance(quality_score, dict) else 0,
        "recommendedActions": _shrink_for_record(answer.get("recommendedActions")),
        "reportReadyBullets": _string_list(answer.get("reportReadyBullets"))[:5],
        "businessImplications": _string_list(answer.get("businessImplications"))[:6],
        "followUps": _shrink_for_record(follow_ups),
        "chartCount": _chart_count_from_result(result),
        "composer": _text(answer.get("composer")),
        "modelUsageStatus": _text(model_usage.get("status")),
        "scores": scores,
    }


def _summarize_country_copilot_side(
    result: dict[str, Any] | None,
    error: str | None,
) -> dict[str, Any]:
    if not result:
        return {
            "status": "failed",
            "error": error or "CountryCopilot result unavailable",
        }
    grounding = result.get("grounding") if isinstance(result.get("grounding"), dict) else {}
    trust = grounding.get("trust") if isinstance(grounding.get("trust"), dict) else {}
    evidence_pack = result.get("evidencePack") if isinstance(result.get("evidencePack"), dict) else {}
    source_plan = result.get("sourcePlan") if isinstance(result.get("sourcePlan"), dict) else {}
    return {
        "status": "ok",
        "answerPreview": _text(result.get("answer"))[:900],
        "answerMode": _text(result.get("answerMode")),
        "provider": _text(result.get("provider")),
        "model": _text(result.get("model")),
        "providerReason": _text(result.get("providerReason"))[:500],
        "intentRoute": _text(result.get("intentRoute")),
        "focusedIntents": _string_list(result.get("focusedIntents"))[:8],
        "confidence": _text(trust.get("confidence")),
        "evidenceTableCount": _list_len(grounding.get("evidenceTables")),
        "sourceCount": _list_len(evidence_pack.get("sources")) or _list_len(source_plan.get("sources")),
        "chartLinkCount": _list_len(result.get("chartLinks")),
    }


def _build_side_by_side_comparison(
    astrbot_result: dict[str, Any] | None,
    country_result: dict[str, Any] | None,
    errors: dict[str, str],
    *,
    question: str = "",
) -> dict[str, Any]:
    astrbot_answer = {}
    if isinstance(astrbot_result, dict):
        data = astrbot_result.get("data") if isinstance(astrbot_result.get("data"), dict) else {}
        astrbot_answer = data.get("answer") if isinstance(data.get("answer"), dict) else {}
    country_answer = _text(country_result.get("answer")) if isinstance(country_result, dict) else ""
    question = ""
    if isinstance(astrbot_result, dict):
        data = astrbot_result.get("data") if isinstance(astrbot_result.get("data"), dict) else {}
        question = question or _text(data.get("question") or data.get("userQuestion"))
    astrbot_review_answer = _business_review_answer_text(astrbot_answer, question=question)
    return {
        "bothReturned": bool(astrbot_review_answer) and bool(country_answer),
        "requiresHumanScoring": True,
        "astrbotAnswerChars": len(astrbot_review_answer),
        "countryCopilotAnswerChars": len(country_answer),
        "answerLengthDelta": len(astrbot_review_answer) - len(country_answer),
        "errorCount": len(errors),
        "recommendedManualDecision": "score_before_switching_default_entry",
    }


def _source_repair_candidates_from_evidence_package(evidence_package: dict[str, Any]) -> dict[str, Any]:
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    missing_names = {
        str(item.get("name") or "")
        for item in missing
        if isinstance(item, dict)
    }
    external_queries: list[str] = []
    for item in tool_results:
        if not isinstance(item, dict):
            continue
        diagnostics = item.get("coverageDiagnostics") if isinstance(item.get("coverageDiagnostics"), dict) else {}
        candidates = diagnostics.get("sourceRepairCandidates") if isinstance(diagnostics.get("sourceRepairCandidates"), dict) else {}
        if candidates:
            return candidates
        queries = diagnostics.get("externalResearchQueries") if isinstance(diagnostics.get("externalResearchQueries"), list) else []
        external_queries.extend(str(query or "").strip() for query in queries if str(query or "").strip())
    if "leasing_tco_or_company_car_evidence" in missing_names:
        return _leasing_tco_source_repair_candidates(
            country=str(evidence_package.get("country") or ""),
            queries=external_queries,
        )
    return {}


def _leasing_tco_source_repair_candidates(*, country: str, queries: list[str] | None = None) -> dict[str, Any]:
    query_values = _dedupe_string_list([
        *_leasing_tco_default_source_queries(country),
        *(queries or []),
    ])[:8]
    candidates = [
        {
            "sourceCode": f"leasing-tco-{_country_key_for_policy_candidate(country)}-{index + 1}",
            "brand": "TCO",
            "model": _leasing_tco_candidate_label(query),
            "sourceUrl": f"https://www.google.com/search?q={quote_plus(query)}",
            "relativePath": query,
            "draftStatus": "candidate_search_query",
            "currentPriceRows": 0,
            "candidateSourceType": "leasing_tco_search",
            "candidateDomain": _domain_from_site_query(query),
            "sourceSearchQuery": query,
        }
        for index, query in enumerate(query_values)
    ]
    if not candidates:
        return {}
    return {
        "dataStatus": "leasing_tco_source_candidates",
        "missingOwnModelSource": False,
        "candidateCount": len(candidates),
        "materializedCandidateCount": 0,
        "ownModel": [],
        "competitorCorridor": candidates,
        "sourceSearchPlan": _source_repair_search_plan_from_candidates(
            own_model=[],
            competitor_corridor=candidates,
        ),
    }


def _leasing_tco_default_source_queries(country: str) -> list[str]:
    country_label = str(country or "Sweden").strip() or "Sweden"
    return [
        f"site:skatteverket.se bilförmån laddhybrid {country_label} 2026",
        f"{country_label} PHEV company car benefit tax 2026 Skatteverket",
        f"{country_label} PHEV leasing monthly payment residual value fleet TCO",
        f"{country_label} XC60 PHEV Kia Sportage PHEV leasing monthly payment residual value",
    ]


def _leasing_tco_candidate_label(query: str) -> str:
    value = str(query or "").strip()
    lowered = value.lower()
    if "skatteverket" in lowered or "bilförmån" in lowered or "benefit tax" in lowered:
        return "company-car tax / benefit formula source"
    if "residual" in lowered or "rv" in lowered:
        return "leasing monthly payment / residual value source"
    if "monthly" in lowered or "leasing" in lowered:
        return "leasing monthly payment source"
    return "leasing/TCO evidence source"


def _external_source_count_from_evidence_package(evidence_package: dict[str, Any]) -> int:
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    urls: set[str] = set()
    max_row_count = 0
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        tool_name = str(tool.get("toolName") or "")
        if tool_name not in {"external_research", "search_market_news", "read_web_page", "browser_snapshot", "pageindex_search_documents"}:
            continue
        try:
            max_row_count = max(max_row_count, int(float(str(tool.get("rowCount") or 0))))
        except (TypeError, ValueError):
            pass
        refs = tool.get("evidenceRefs") if isinstance(tool.get("evidenceRefs"), list) else []
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            source = str(ref.get("source") or ref.get("table") or ref.get("value") or "").strip()
            if source.startswith(("http://", "https://")):
                urls.add(source.split("#", 1)[0].split("?", 1)[0].rstrip("/"))
    return max(len(urls), max_row_count)


def _filter_source_repair_candidates_for_question(
    candidates: dict[str, Any],
    *,
    question: str = "",
    country: str = "",
) -> dict[str, Any]:
    if not candidates:
        return {}
    result = dict(candidates)
    data_status = str(result.get("dataStatus") or "").strip()
    if data_status in {"external_policy_source_candidates", "external_policy_source_required"}:
        result = _filter_policy_source_repair_candidates_for_question(
            result,
            question=question,
            country=country,
        )
    competitor_corridor = (
        result.get("competitorCorridor")
        if isinstance(result.get("competitorCorridor"), list)
        else []
    )
    tokens = _source_candidate_priority_tokens(question)
    if competitor_corridor and tokens and data_status not in {"external_policy_source_candidates", "external_policy_source_required"}:
        filtered = [
            entry for entry in competitor_corridor
            if isinstance(entry, dict) and _source_candidate_matches_priority(entry, tokens)
        ]
        if filtered:
            result["competitorCorridor"] = filtered
            own_model = result.get("ownModel") if isinstance(result.get("ownModel"), list) else []
            result["candidateCount"] = len(own_model) + len(filtered)
    return result


def _filter_policy_source_repair_candidates_for_question(
    candidates: dict[str, Any],
    *,
    question: str,
    country: str = "",
) -> dict[str, Any]:
    topic = _policy_source_repair_topic(question)
    if not topic:
        return candidates
    result = dict(candidates)
    own_model = [entry for entry in result.get("ownModel", []) if isinstance(entry, dict)] if isinstance(result.get("ownModel"), list) else []
    competitor_corridor = [
        entry for entry in result.get("competitorCorridor", []) if isinstance(entry, dict)
    ] if isinstance(result.get("competitorCorridor"), list) else []
    queries = [str(query or "").strip() for query in result.get("queries", []) if str(query or "").strip()] if isinstance(result.get("queries"), list) else []

    filtered_own = _filter_policy_source_candidate_entries(own_model, topic)
    filtered_corridor = _filter_policy_source_candidate_entries(competitor_corridor, topic)
    filtered_queries = _filter_policy_source_queries(queries, topic)
    had_policy_candidates = bool(own_model or competitor_corridor or queries)
    if had_policy_candidates and not (filtered_own or filtered_corridor or filtered_queries):
        generated = _topic_policy_source_repair_candidates(country=country, question=question, topic=topic)
        return generated or candidates
    if own_model:
        result["ownModel"] = filtered_own
    if competitor_corridor:
        result["competitorCorridor"] = filtered_corridor
    if queries:
        result["queries"] = filtered_queries
    candidate_count = len(result.get("ownModel", []) if isinstance(result.get("ownModel"), list) else [])
    candidate_count += len(result.get("competitorCorridor", []) if isinstance(result.get("competitorCorridor"), list) else [])
    candidate_count += len(result.get("queries", []) if isinstance(result.get("queries"), list) else [])
    result["candidateCount"] = candidate_count
    if isinstance(result.get("ownModel"), list) or isinstance(result.get("competitorCorridor"), list):
        result["sourceSearchPlan"] = _source_repair_search_plan_from_candidates(
            own_model=result.get("ownModel", []) if isinstance(result.get("ownModel"), list) else [],
            competitor_corridor=result.get("competitorCorridor", []) if isinstance(result.get("competitorCorridor"), list) else [],
        )
    return result


def _policy_source_repair_topic(question: str) -> str:
    text = _text(question).casefold()
    if not text:
        return ""
    if "elbilspremien" in text or "elbilspremie" in text:
        return "bev_subsidy"
    if any(token in text for token in ("补贴", "subsidy", "price cap", "价格上限", "prisgrans", "prisgräns")):
        return "bev_subsidy"
    if any(token in text for token in ("co₂", "co2", "税率", "税费", "company car", "benefit", "公司车", "bilförmån", "bilforman")):
        return "co2_tax"
    if "phev" in text and any(token in text for token in ("税", "tax", "benefit", "fleet", "leasing", "大客户")):
        return "co2_tax"
    return ""


def _filter_policy_source_candidate_entries(entries: list[dict[str, Any]], topic: str) -> list[dict[str, Any]]:
    scored: list[tuple[int, int, dict[str, Any]]] = []
    for index, entry in enumerate(entries):
        score = _policy_source_text_score(_policy_source_candidate_haystack(entry), topic)
        if score > 0:
            scored.append((score, index, entry))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [dict(entry) for _, _, entry in scored]


def _filter_policy_source_queries(queries: list[str], topic: str) -> list[str]:
    scored: list[tuple[int, int, str]] = []
    for index, query in enumerate(queries):
        score = _policy_source_text_score(query, topic)
        if score > 0:
            scored.append((score, index, query))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [query for _, _, query in scored]


def _policy_source_text_score(value: str, topic: str) -> int:
    text = _policy_source_match_text(value)
    if not text:
        return 0
    excluded = _policy_source_excluded_terms(topic)
    if excluded and any(term in text for term in excluded):
        return 0
    score = sum(1 for term in _policy_source_preferred_terms(topic) if term in text)
    if topic == "co2_tax" and "skatteverket" in text:
        score += 4
    if topic == "co2_tax" and ("transportstyrelsen" in text or "fordonsskatt" in text):
        score += 2
    if topic == "bev_subsidy" and ("regeringen" in text or "transportstyrelsen" in text):
        score += 3
    if topic == "bev_subsidy" and "skatteverket" in text and ("elbilspremie" in text or "elbilspremien" in text):
        score += 1
    return score


def _policy_source_preferred_terms(topic: str) -> tuple[str, ...]:
    if topic == "co2_tax":
        return (
            "skatteverket",
            "bilförmån",
            "bilforman",
            "förmån",
            "forman",
            "company car",
            "benefit",
            "co2",
            "co₂",
            "koldioxid",
            "laddhybrid",
            "phev",
            "fordonsskatt",
            "malus",
            "tjänstebil",
            "tjanstebil",
            "tax",
            "税率",
        )
    if topic == "bev_subsidy":
        return (
            "elbilspremie",
            "elbilspremien",
            "subsidy",
            "incentive",
            "price cap",
            "prisgrans",
            "prisgräns",
            "bonus",
            "regeringen",
            "transportstyrelsen",
        )
    return ()


def _policy_source_excluded_terms(topic: str) -> tuple[str, ...]:
    if topic == "co2_tax":
        return (
            "elbilspremie",
            "elbilspremien",
            "price cap",
            "prisgrans",
            "prisgräns",
            "subsidy",
            "purchase incentive",
        )
    return ()


def _policy_source_match_text(value: str) -> str:
    return " ".join(_text(value).casefold().replace("%20", " ").replace("+", " ").split())


def _policy_source_candidate_haystack(entry: dict[str, Any]) -> str:
    source_url = str(entry.get("sourceUrl") or "")
    return " ".join(
        str(value or "")
        for value in (
            entry.get("brand"),
            entry.get("model"),
            entry.get("sourceSearchQuery"),
            entry.get("relativePath"),
            entry.get("candidateDomain"),
            entry.get("candidateSourceType"),
            entry.get("sourceCode"),
            source_url,
            _google_query_from_url(source_url),
        )
    )


def _topic_policy_source_repair_candidates(*, country: str, question: str, topic: str) -> dict[str, Any]:
    queries = _topic_policy_source_queries(country=country, question=question, topic=topic)
    candidates = [
        {
            "sourceCode": f"policy-topic-{_country_key_for_policy_candidate(country)}-{index + 1}",
            "brand": "official",
            "model": _topic_policy_candidate_label(topic=topic, country=country, query=query),
            "sourceUrl": f"https://www.google.com/search?q={quote_plus(query)}",
            "relativePath": query,
            "draftStatus": "candidate_search_query",
            "currentPriceRows": 0,
            "candidateSourceType": "official_policy_search",
            "candidateDomain": _domain_from_site_query(query),
            "sourceSearchQuery": query,
        }
        for index, query in enumerate(queries)
    ]
    if not candidates:
        return {}
    return {
        "dataStatus": "external_policy_source_candidates",
        "missingOwnModelSource": False,
        "candidateCount": len(candidates),
        "materializedCandidateCount": 0,
        "ownModel": [],
        "competitorCorridor": candidates,
        "sourceSearchPlan": _source_repair_search_plan_from_candidates(
            own_model=[],
            competitor_corridor=candidates,
        ),
    }


def _topic_policy_source_queries(*, country: str, question: str, topic: str) -> list[str]:
    country_key = _country_key_for_policy_candidate(country)
    country_label = str(country or "Sweden").strip() or "Sweden"
    if country_key == "sweden" and topic == "co2_tax":
        return [
            "site:skatteverket.se bilförmån laddhybrid CO2 2026",
            "site:skatteverket.se fordonsskatt koldioxid laddhybrid 2026",
            "site:transportstyrelsen.se bonus malus koldioxid laddhybrid 2026",
        ]
    if country_key == "sweden" and topic == "bev_subsidy":
        return [
            "site:regeringen.se elbilspremie 2026 elbil prisgräns",
            "site:transportstyrelsen.se elbil bonus malus 2026 prisgräns",
            "site:skatteverket.se elbilspremie 2026",
        ]
    if topic == "co2_tax":
        return [
            f"{country_label} company car benefit CO2 vehicle tax 2026 official",
            f"{country_label} PHEV tax CO2 company car benefit 2026 government",
        ]
    if topic == "bev_subsidy":
        return [
            f"{country_label} electric vehicle subsidy price cap 2026 official government",
            f"{country_label} EV purchase incentive eligibility price threshold 2026 official",
        ]
    return []


def _topic_policy_candidate_label(*, topic: str, country: str, query: str) -> str:
    country_text = str(country or "").strip() or "Policy"
    domain = _domain_from_site_query(query)
    source = f": {domain}" if domain else ""
    if topic == "co2_tax":
        return f"{country_text} CO2/company-car tax source{source}"
    if topic == "bev_subsidy":
        return f"{country_text} BEV subsidy/price-cap source{source}"
    return f"{country_text} policy source{source}"


def _source_candidate_matches_priority(entry: dict[str, Any], tokens: list[str]) -> bool:
    model = _source_candidate_match_token(str(entry.get("model") or ""))
    brand_model = _source_candidate_match_token(
        " ".join(
            part
            for part in [str(entry.get("brand") or "").strip(), str(entry.get("model") or "").strip()]
            if part
        )
    )
    source_text = _source_candidate_match_token(_policy_source_candidate_haystack(entry))
    for token in tokens:
        normalized = _source_candidate_match_token(token)
        if not normalized:
            continue
        if normalized == model or normalized in model:
            return True
        if normalized == brand_model or normalized in brand_model:
            return True
        if normalized in source_text:
            return True
    return False


def _source_candidate_match_token(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def _source_repair_candidates_with_leasing_tco_candidates(
    candidates: dict[str, Any],
    *,
    country: str,
    question: str,
    missing_evidence: Any,
) -> dict[str, Any]:
    base_candidates = candidates if isinstance(candidates, dict) else {}
    if not _missing_evidence_has_name(missing_evidence, "leasing_tco_or_company_car_evidence"):
        return base_candidates
    if not _question_requires_leasing_tco_evidence(question):
        return base_candidates
    if _is_leasing_tco_source_repair(base_candidates):
        return base_candidates
    generated = _leasing_tco_source_repair_candidates(
        country=country,
        queries=_source_candidate_queries_from_candidates(base_candidates),
    )
    if not generated:
        return base_candidates
    if _source_candidate_count(base_candidates) <= 0:
        return generated
    generated_rows = [entry for entry in generated.get("competitorCorridor", []) if isinstance(entry, dict)]
    existing_rows = [entry for entry in base_candidates.get("competitorCorridor", []) if isinstance(entry, dict)]
    merged = _dedupe_source_candidates([*generated_rows, *existing_rows])
    result = dict(generated)
    result["competitorCorridor"] = merged
    result["ownModel"] = []
    result["candidateCount"] = len(merged)
    result["materializedCandidateCount"] = 0
    result["sourceSearchPlan"] = _source_repair_search_plan_from_candidates(
        own_model=[],
        competitor_corridor=merged,
    )
    return result


def _source_repair_candidates_with_external_research_candidates(
    candidates: dict[str, Any],
    *,
    country: str,
    question: str,
    intent: str,
    missing_evidence: Any,
) -> dict[str, Any]:
    base_candidates = candidates if isinstance(candidates, dict) else {}
    if _is_leasing_tco_source_repair(base_candidates) or _is_policy_source_repair(base_candidates):
        return base_candidates
    if _is_external_query_source_repair(base_candidates) and _source_candidate_count(base_candidates) > 0:
        return base_candidates
    if not _missing_evidence_requires_external_research_candidates(missing_evidence):
        return base_candidates
    if str(intent or "") == "news_policy_search" and _missing_evidence_has_name(missing_evidence, "specific_policy_source_evidence"):
        return base_candidates
    queries = _external_research_default_queries(country=country, question=question, intent=intent)
    if not queries:
        return base_candidates
    generated = _external_research_source_repair_candidates(
        country=country,
        question=question,
        intent=intent,
        queries=queries,
    )
    if not generated:
        return base_candidates
    if _source_candidate_count(base_candidates) <= 0:
        return generated
    if _source_repair_candidates_look_like_msrp(base_candidates):
        own_model = [
            entry for entry in base_candidates.get("ownModel", []) if isinstance(entry, dict)
        ]
        existing_corridor = [
            entry for entry in base_candidates.get("competitorCorridor", []) if isinstance(entry, dict)
        ]
        generated_corridor = [
            entry for entry in generated.get("competitorCorridor", []) if isinstance(entry, dict)
        ]
        competitor_corridor = _dedupe_source_candidates([
            *existing_corridor,
            *generated_corridor,
        ])
        result = dict(base_candidates)
        result["dataStatus"] = "source_draft_candidate_not_price_evidence"
        result["ownModel"] = own_model
        result["competitorCorridor"] = competitor_corridor
        result["queries"] = _dedupe_string_list([
            *_string_list(base_candidates.get("queries")),
            *_string_list(generated.get("queries")),
        ])[:8]
        result["candidateCount"] = len(own_model) + len(competitor_corridor)
        result["materializedCandidateCount"] = sum(
            1
            for entry in [*own_model, *competitor_corridor]
            if isinstance(entry, dict) and _source_candidate_current_price_rows(entry) > 0
        )
        result["sourceSearchPlan"] = _source_repair_search_plan_from_candidates(
            own_model=own_model,
            competitor_corridor=competitor_corridor,
        )
        return result
    existing_queries = _source_candidate_queries_from_candidates(base_candidates)
    merged_queries = _dedupe_string_list([*queries, *existing_queries])[:8]
    return _external_research_source_repair_candidates(
        country=country,
        question=question,
        intent=intent,
        queries=merged_queries,
    ) or generated


def _missing_evidence_requires_external_research_candidates(missing_evidence: Any) -> bool:
    return any(
        _missing_evidence_has_name(missing_evidence, name)
        for name in (
            "external_research_claims_unavailable",
            "minimum_external_sources",
            "consumer_signal",
        )
    )


def _external_research_source_repair_candidates(
    *,
    country: str,
    question: str,
    intent: str,
    queries: list[str],
) -> dict[str, Any]:
    query_values = _dedupe_string_list([query for query in queries if str(query or "").strip()])[:8]
    if not query_values:
        return {}
    subject = _external_research_subject(question)
    if str(intent or "") == "configuration_analysis":
        brand = "Config"
    elif str(intent or "") in {"pricing_analysis", "competitor_compare", "report_generation"} and _pricing_source_repair_query(question):
        brand = "Pricing"
    else:
        brand = "VOC"
    candidates = [
        {
            "sourceCode": f"external-research-{_country_key_for_policy_candidate(country)}-{index + 1}",
            "brand": brand,
            "model": _external_research_candidate_label(subject=subject, query=query, intent=intent),
            "sourceUrl": f"https://www.google.com/search?q={quote_plus(query)}",
            "relativePath": query,
            "draftStatus": "candidate_search_query",
            "currentPriceRows": 0,
            "candidateSourceType": "external_research_search",
            "candidateDomain": _domain_from_site_query(query),
            "sourceSearchQuery": query,
        }
        for index, query in enumerate(query_values)
    ]
    return {
        "dataStatus": "external_research_query_candidates",
        "missingOwnModelSource": False,
        "candidateCount": len(candidates),
        "materializedCandidateCount": 0,
        "ownModel": [],
        "competitorCorridor": candidates,
        "queries": query_values,
        "sourceSearchPlan": _source_repair_search_plan_from_candidates(
            own_model=[],
            competitor_corridor=candidates,
        ),
    }


def _external_research_default_queries(*, country: str, question: str, intent: str) -> list[str]:
    country_label = str(country or "").strip() or "target market"
    subject = _external_research_subject(question)
    text = str(question or "").casefold()
    if str(intent or "") in {"pricing_analysis", "competitor_compare", "report_generation"} and _pricing_source_repair_query(question):
        subjects = _pricing_source_subjects_for_eval(question)
        queries: list[str] = []
        for item in subjects[:4]:
            queries.append(f"{item} {country_label} official price MSRP")
        if country_label.casefold() in {"sweden", "瑞典"}:
            for item in subjects[:3]:
                queries.append(f"{item} Sverige pris officiell")
        for item in subjects[:3]:
            queries.append(f"{item} {country_label} price list trim MSRP")
        return _dedupe_string_list(queries)
    if "v2h" in text or "v2g" in text:
        return _dedupe_string_list([
            f"{country_label} V2H EV purchase driver owner review forum",
            f"{country_label} V2H home backup winter use electric vehicle media review",
            "Nordic V2H electric vehicle owner experience home backup review",
        ])
    if any(token in text for token in ("拖车", "tow", "roof", "冬季胎", "winter tyre", "winter tire", "däck")):
        return _dedupe_string_list([
            f"{country_label} SUV tow hook roof load winter tires owner forum",
            f"{country_label} EV SUV towing roof load winter tyres media review",
            "Nordic electric SUV tow hook roof load winter tires user review",
        ])
    if str(intent or "") == "configuration_analysis" and any(token in text for token in ("冬季", "winter", "北欧", "nordic")):
        return _dedupe_string_list([
            f"{country_label} Nordic EV winter package heat pump battery preconditioning test",
            f"{country_label} electric SUV winter tyres heat pump range winter review",
            "Nordic EV winter package heat pump battery preconditioning owner review",
        ])
    if any(token in text for token in ("omoda", "jaecoo", "o5", "o9", "j7", "j8")):
        return _dedupe_string_list([
            f"{country_label} {subject} owner review complaint forum",
            f"{country_label} {subject} media review reliability complaints",
            f"{subject} Europe owner review reliability complaints",
        ])
    return _dedupe_string_list([
        f"{country_label} {subject} owner review forum complaint",
        f"{country_label} {subject} media review user complaints product issue",
    ])


def _external_research_subject(question: str) -> str:
    text = str(question or "").strip()
    lower = text.casefold()
    if "v2h" in lower or "v2g" in lower:
        return "V2H EV purchase driver"
    if any(token in lower for token in ("拖车", "tow", "roof", "冬季胎", "winter tyre", "winter tire", "däck")):
        return "tow hook roof load winter tires SUV"
    if any(token in lower for token in ("冬季包", "winter package", "北欧")):
        return "Nordic winter package EV"
    tokens = re.findall(r"\b(?:OMODA|JAECOO|O5|O9|J7|J8|BEV|HEV|PHEV)\b", text, flags=re.IGNORECASE)
    if tokens:
        return " ".join(_dedupe_string_list([token.upper() for token in tokens]))
    return "automotive customer voice"


def _external_research_candidate_label(*, subject: str, query: str, intent: str) -> str:
    query_text = str(query or "").casefold()
    if str(intent or "") in {"pricing_analysis", "competitor_compare", "report_generation"} and any(
        token in query_text for token in ("price", "msrp", "pris", "official", "officiell")
    ):
        return _pricing_candidate_label_for_eval(query)
    if str(intent or "") == "configuration_analysis":
        if "winter" in query_text or "冬季" in query_text:
            return "winter-package external source"
        return "configuration external source"
    if "v2h" in query_text:
        return "V2H owner/media source"
    if any(token in query_text for token in ("tow", "roof", "winter tire", "winter tyre", "däck")):
        return "tow/roof-load/winter-tyre VOC source"
    return f"{subject} source"


def _pricing_source_repair_query(question: str) -> bool:
    text = str(question or "").casefold()
    return any(
        token in text
        for token in (
            "price",
            "pricing",
            "msrp",
            "official price",
            "current price",
            "list price",
            "定价",
            "价格",
            "售价",
            "价差",
            "便宜",
            "贵",
            "月供",
            "欧元",
            "官方价格",
            "当前价格",
        )
    ) or bool(re.search(r"\d+(?:[.,]\d+)?\s?(?:k|万)?\s?(?:eur|euro|€|sek|kr)", text))


def _pricing_source_subjects_for_eval(question: str) -> list[str]:
    text = str(question or "")
    lowered = text.casefold()
    candidates: list[str] = []
    known_models = [
        ("o5", "bev", "O5 BEV"),
        ("j7", "hev", "J7 HEV"),
        ("j7", "phev", "J7 PHEV"),
        ("sportage", "hev", "Kia Sportage HEV"),
        ("ev3", "", "Kia EV3"),
        ("ex30", "", "Volvo EX30"),
        ("xc60", "", "Volvo XC60"),
        ("ex60", "", "Volvo EX60"),
        ("sorento", "", "Kia Sorento"),
        ("rav4", "", "Toyota RAV4"),
        ("corolla cross", "", "Toyota Corolla Cross"),
        ("o9", "", "O9"),
        ("o5", "", "O5"),
        ("j8", "", "J8"),
        ("j7", "", "J7"),
    ]
    for required, optional, label in known_models:
        if required in lowered and (not optional or optional in lowered):
            candidates.append(label)
    tokens = re.findall(r"\b(?:OMODA|JAECOO|O5|O9|J7|J8|EV3|EX30|EX60|XC60|BEV|HEV|PHEV|Sportage|Sorento|RAV4)\b", text, flags=re.IGNORECASE)
    if tokens:
        candidates.append(" ".join(_dedupe_string_list([token.upper() for token in tokens])[:5]))
    return _dedupe_string_list(candidates) or ["target model"]


def _pricing_candidate_label_for_eval(query: str) -> str:
    text = str(query or "").casefold()
    for token, label in (
        ("o5 bev", "O5 BEV official price source"),
        ("kia ev3", "Kia EV3 official price source"),
        ("ev3", "Kia EV3 official price source"),
        ("j7 hev", "J7 HEV official price source"),
        ("sportage", "Kia Sportage official price source"),
        ("rav4", "Toyota RAV4 official price source"),
        ("ex30", "Volvo EX30 official price source"),
        ("xc60", "Volvo XC60 official price source"),
        ("o9", "O9 official price source"),
        ("j8", "J8 official price source"),
    ):
        if token in text:
            return label
    return "official price / MSRP source"


def _source_candidate_queries_from_candidates(candidates: dict[str, Any]) -> list[str]:
    queries: list[str] = []
    for entry in _source_candidate_rows(candidates):
        source_url = str(entry.get("sourceUrl") or "").strip()
        query = (
            str(entry.get("sourceSearchQuery") or "").strip()
            or _google_query_from_url(source_url)
            or str(entry.get("relativePath") or "").strip()
        )
        if query:
            queries.append(query)
    return _dedupe_string_list(queries)


def _source_repair_candidates_look_like_msrp(candidates: dict[str, Any]) -> bool:
    if not isinstance(candidates, dict) or not candidates:
        return False
    data_status = str(candidates.get("dataStatus") or "").lower()
    if "external_research" in data_status or "policy" in data_status or "news" in data_status:
        return False
    if any(token in data_status for token in ("current_price", "source_draft", "msrp", "price_source")):
        return True
    rows: list[Any] = []
    for key in ("ownModel", "competitorCorridor"):
        value = candidates.get(key)
        if isinstance(value, list):
            rows.extend(value)
    for entry in rows:
        if not isinstance(entry, dict):
            continue
        candidate_type = str(entry.get("candidateSourceType") or "").lower()
        draft_status = str(entry.get("draftStatus") or "").lower()
        if any(token in candidate_type for token in ("official_price", "brand_official", "source_draft")):
            return True
        if "source_draft" in draft_status or "current_price" in draft_status:
            return True
    return False


def _source_repair_candidates_with_named_policy_candidates(
    candidates: dict[str, Any],
    *,
    country: str,
    question: str,
    missing_evidence: Any,
) -> dict[str, Any]:
    if not _missing_evidence_has_name(missing_evidence, "specific_policy_source_evidence"):
        return candidates
    generated = _named_policy_source_repair_candidates(country=country, question=question)
    if not generated:
        return candidates
    if not candidates:
        return generated
    if str(candidates.get("dataStatus") or "") != "external_policy_source_candidates":
        return candidates
    existing = [entry for entry in candidates.get("competitorCorridor", []) if isinstance(entry, dict)]
    generated_rows = [entry for entry in generated.get("competitorCorridor", []) if isinstance(entry, dict)]
    merged = _dedupe_source_candidates([*generated_rows, *existing])
    result = dict(candidates)
    result["dataStatus"] = "external_policy_source_candidates"
    result["competitorCorridor"] = merged
    result["ownModel"] = [entry for entry in candidates.get("ownModel", []) if isinstance(entry, dict)]
    result["candidateCount"] = len(result["ownModel"]) + len(merged)
    result["materializedCandidateCount"] = 0
    result["sourceSearchPlan"] = _source_repair_search_plan_from_candidates(
        own_model=result["ownModel"],
        competitor_corridor=merged,
    )
    return result


def _missing_evidence_has_name(missing_evidence: Any, name: str) -> bool:
    if not isinstance(missing_evidence, list):
        return False
    return any(
        isinstance(item, dict) and str(item.get("name") or "") == name
        for item in missing_evidence
    )


def _named_policy_source_repair_candidates(*, country: str, question: str) -> dict[str, Any]:
    terms = _specific_policy_terms_from_question(question)
    if not terms:
        return {}
    queries = _named_policy_source_queries(country=country, terms=terms)
    candidates = [
        {
            "sourceCode": f"policy-named-{_country_key_for_policy_candidate(country)}-{index + 1}",
            "brand": "official",
            "model": _named_policy_candidate_label(country=country, terms=terms, query=query),
            "sourceUrl": f"https://www.google.com/search?q={quote_plus(query)}",
            "relativePath": query,
            "draftStatus": "candidate_search_query",
            "currentPriceRows": 0,
            "candidateSourceType": "official_policy_search",
            "candidateDomain": _domain_from_site_query(query),
            "sourceSearchQuery": query,
        }
        for index, query in enumerate(queries)
    ]
    if not candidates:
        return {}
    return {
        "dataStatus": "external_policy_source_candidates",
        "missingOwnModelSource": False,
        "candidateCount": len(candidates),
        "materializedCandidateCount": 0,
        "ownModel": [],
        "competitorCorridor": candidates,
        "sourceSearchPlan": _source_repair_search_plan_from_candidates(
            own_model=[],
            competitor_corridor=candidates,
        ),
    }


def _named_policy_source_queries(*, country: str, terms: list[str]) -> list[str]:
    country_key = _country_key_for_policy_candidate(country)
    policy = " ".join(terms)
    if country_key == "sweden":
        return [
            f"site:regeringen.se {policy} 2026 elbilspremie elbilspremien",
            f"site:transportstyrelsen.se {policy} 2026 elbilspremie elbilspremien",
            f"site:skatteverket.se {policy} 2026 elbilspremie elbilspremien",
        ]
    return [
        f"{country or 'country'} {policy} 2026 official government policy",
        f"site:gov.hu {policy} 2026 electric vehicle subsidy",
        f"site:kormany.hu {policy} 2026 electric vehicle subsidy",
    ]


def _named_policy_candidate_label(*, country: str, terms: list[str], query: str) -> str:
    country_text = str(country or "").strip() or "Policy"
    policy = " ".join(term for term in terms if term).strip() or "named policy"
    domain = _domain_from_site_query(query)
    if domain:
        return f"{country_text} {policy} source: {domain}"
    return f"{country_text} {policy} official source"


def _country_key_for_policy_candidate(country: str) -> str:
    text = str(country or "").casefold()
    if "sweden" in text or "sverige" in text or "瑞典" in text:
        return "sweden"
    if "hungary" in text or "magyar" in text or "匈牙利" in text:
        return "hungary"
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-") or "country"


def _domain_from_site_query(query: str) -> str:
    match = re.search(r"\bsite:([^\s]+)", str(query or ""), flags=re.IGNORECASE)
    return str(match.group(1) or "").strip() if match else ""


def _dedupe_source_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in candidates:
        query = str(entry.get("sourceSearchQuery") or entry.get("relativePath") or "").strip().casefold()
        source_url = str(entry.get("sourceUrl") or "").strip().casefold()
        source_code = str(entry.get("sourceCode") or "").strip().casefold()
        key = query or source_url or source_code
        if not key or key in seen:
            continue
        result.append(dict(entry))
        seen.add(key)
    return result[:8]


def _source_candidate_current_price_rows(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("currentPriceRows") or 0)
    except (TypeError, ValueError):
        return 0


def _source_candidate_review_pending_rows(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("reviewPendingRows") or 0)
    except (TypeError, ValueError):
        return 0


def _source_candidate_rows(candidates: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("ownModel", "competitorCorridor"):
        value = candidates.get(key) if isinstance(candidates, dict) else None
        if isinstance(value, list):
            rows.extend([entry for entry in value if isinstance(entry, dict)])
    return rows


def _source_candidate_count(candidates: Any) -> int:
    if not isinstance(candidates, dict):
        return 0
    raw_count = candidates.get("candidateCount")
    if raw_count is not None:
        try:
            return int(raw_count)
        except (TypeError, ValueError):
            pass
    own_model = candidates.get("ownModel") if isinstance(candidates.get("ownModel"), list) else []
    competitor_corridor = (
        candidates.get("competitorCorridor")
        if isinstance(candidates.get("competitorCorridor"), list)
        else []
    )
    return len(own_model) + len(competitor_corridor)


def _missing_evidence_needs_price_source_candidates(missing_evidence: Any) -> bool:
    if not isinstance(missing_evidence, list):
        return False
    for item in missing_evidence:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").lower()
        if name in {"current_msrp", "own_model_price", "pricing_data_unavailable"}:
            return True
        if "current_msrp" in name or "own_model_price" in name or "price_corridor" in name:
            return True
    return False


def _business_side_by_side_records(category: str | None = None) -> list[dict[str, Any]]:
    records = [
        _enrich_business_record_for_read(record)
        for record in reversed(_read_side_by_side_results())
        if record.get("validationType") == "business"
    ]
    if category:
        records = [record for record in records if record.get("category") == category]
    return records


def _business_judge_existing_candidate_indexes(
    records: list[dict[str, Any]],
    *,
    category: str | None,
    latest_per_question: bool,
    score_ready_only: bool = False,
) -> list[int]:
    indexes: list[int] = []
    seen_questions: set[str] = set()
    for record_index in range(len(records) - 1, -1, -1):
        record = records[record_index]
        if record.get("validationType") != "business":
            continue
        if category and record.get("category") != category:
            continue
        question_key = str(record.get("questionId") or record.get("question") or record.get("comparisonId") or "").strip()
        if latest_per_question and question_key:
            if question_key in seen_questions:
                continue
            seen_questions.add(question_key)
        if score_ready_only and not _business_record_is_score_ready(_enrich_business_record_for_read(record)):
            continue
        scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
        source = _human_score_source(scoring.get("source"), default="")
        if scoring.get("status") == "scored" and source in _REPLACEMENT_BASELINE_SOURCES:
            continue
        indexes.append(record_index)
    return indexes


_BUSINESS_REPAIR_FIRST_MISSING_EVIDENCE = {
    "competitive_or_configuration_data_unavailable",
    "current_msrp",
    "external_research_claims_unavailable",
    "leasing_tco_or_company_car_evidence",
    "monthly_trend_series",
    "own_model_price",
    "published_date",
    "specific_policy_source_evidence",
}


def _business_record_is_score_ready(record: dict[str, Any]) -> bool:
    comparison = record.get("comparison") if isinstance(record.get("comparison"), dict) else {}
    if comparison.get("bothReturned") is False:
        return False
    astrbot = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
    missing = astrbot.get("missingEvidence") if isinstance(astrbot.get("missingEvidence"), list) else []
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if _business_missing_evidence_is_repair_first(name):
            return False
    return True


def _business_missing_evidence_is_repair_first(name: str) -> bool:
    value = str(name or "").strip()
    if not value:
        return False
    return (
        value in _BUSINESS_REPAIR_FIRST_MISSING_EVIDENCE
        or value.startswith("coverage_diagnostic:")
        or value.endswith("_weak_evidence_refs")
    )


def _should_refresh_side_by_side_followups(record: dict[str, Any], astrbot_side: dict[str, Any]) -> bool:
    follow_ups = astrbot_side.get("followUps")
    if not isinstance(follow_ups, list) or not follow_ups:
        return True
    category = _text(record.get("category"))
    targeted_categories = {
        "competitor_compare",
        "configuration",
        "inventory_bom",
        "market_overview",
        "voc",
    }
    generic_markers = (
        "继续深挖数据",
        "看竞品/邻国对比",
        "转成业务动作",
        "生成汇报框架",
        "解释背后原因",
        "检查数据缺口",
        "查最新外部证据",
        "请使用",
        "query_",
        "contextSnapshot",
        "crossTabs",
        "能否帮我读取",
        "能否搜索",
        "...",
        "…",
        "Drill into the data",
        "Compare competitors/markets",
        "Turn into actions",
        "Build a report frame",
    )
    for item in follow_ups:
        if isinstance(item, dict):
            text = " ".join([_text(item.get("label")), _text(item.get("question"))])
        else:
            text = _text(item)
        if any(marker in text for marker in generic_markers):
            return True
        if category in targeted_categories and len(text) > 120:
            return True
    return False


def _side_by_side_followup_plan(
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any],
) -> dict[str, Any]:
    expected_types = record.get("expectedFollowUpTypes")
    if not isinstance(expected_types, list):
        expected_types = []
    category = _text(record.get("category"))
    question = _text(record.get("question"))
    if category == "inventory_bom" or any(token in question for token in ("BOM", "物料", "PI", "选品表")):
        expected_types = ["drilldown", "action", "report"]
    return {
        "intent": _text(evidence_package.get("intent") or record.get("expectedIntent") or astrbot_side.get("selectedTool")),
        "followUpTypes": [str(item) for item in expected_types if str(item).strip()],
        "allowedTools": _string_list(astrbot_side.get("actualTools")),
    }


def _latest_records_per_question(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        question_id = str(record.get("questionId") or record.get("question") or record.get("comparisonId") or "").strip()
        key = question_id or str(record.get("comparisonId") or id(record))
        if key in seen:
            continue
        seen.add(key)
        latest.append(record)
    return latest


def _enrich_business_record_for_read(record: dict[str, Any]) -> dict[str, Any]:
    if record.get("validationType") != "business":
        return record
    enriched = dict(record)
    astrbot_side = enriched.get("astrbot") if isinstance(enriched.get("astrbot"), dict) else {}
    astrbot_side = _normalize_astrbot_missing_evidence_for_current_policy(enriched, astrbot_side)
    astrbot_side = dict(astrbot_side)
    astrbot_side.setdefault("question", enriched.get("question"))
    astrbot_side.setdefault("category", enriched.get("category"))
    if isinstance(astrbot_side.get("sourceRepairCandidates"), dict):
        astrbot_side["sourceRepairCandidates"] = _filter_source_repair_candidates_for_question(
            astrbot_side["sourceRepairCandidates"],
            question=str(enriched.get("question") or ""),
            country=str(enriched.get("country") or ""),
        )
    astrbot_side["sourceRepairCandidates"] = _source_repair_candidates_with_leasing_tco_candidates(
        astrbot_side.get("sourceRepairCandidates")
        if isinstance(astrbot_side.get("sourceRepairCandidates"), dict)
        else {},
        country=str(enriched.get("country") or ""),
        question=str(enriched.get("question") or ""),
        missing_evidence=astrbot_side.get("missingEvidence"),
    )
    astrbot_side["sourceRepairCandidates"] = _source_repair_candidates_with_named_policy_candidates(
        astrbot_side.get("sourceRepairCandidates")
        if isinstance(astrbot_side.get("sourceRepairCandidates"), dict)
        else {},
        country=str(enriched.get("country") or ""),
        question=str(enriched.get("question") or ""),
        missing_evidence=astrbot_side.get("missingEvidence"),
    )
    evidence_package = astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
    if not astrbot_side.get("sourceRepairCandidates"):
        astrbot_side["sourceRepairCandidates"] = _source_repair_candidates_from_evidence_package(evidence_package)
    astrbot_side["sourceRepairCandidates"] = _source_repair_candidates_with_external_research_candidates(
        astrbot_side.get("sourceRepairCandidates")
        if isinstance(astrbot_side.get("sourceRepairCandidates"), dict)
        else {},
        country=str(enriched.get("country") or evidence_package.get("country") or ""),
        question=str(enriched.get("question") or ""),
        intent=str(evidence_package.get("intent") or enriched.get("expectedIntent") or ""),
        missing_evidence=astrbot_side.get("missingEvidence"),
    )
    if not _string_list(astrbot_side.get("actualTools")):
        astrbot_side["actualTools"] = _actual_tools_from_astrbot_side(astrbot_side)
    if _should_refresh_side_by_side_followups(enriched, astrbot_side):
        astrbot_side["followUps"] = normalize_follow_ups(
            [],
            country=str(enriched.get("country") or evidence_package.get("country") or "Sweden"),
            question=str(enriched.get("question") or ""),
            tools=_string_list(astrbot_side.get("actualTools")),
            evidence_plan=_side_by_side_followup_plan(enriched, astrbot_side, evidence_package),
        )
    if _should_refresh_side_by_side_visual_artifacts(astrbot_side, evidence_package):
        artifact_evidence_package = _evidence_package_with_side_repair_candidates(evidence_package, astrbot_side)
        astrbot_side["visualArtifacts"] = _shrink_visual_artifacts_for_record(
            build_visual_artifacts(
                question=str(enriched.get("question") or ""),
                answer=astrbot_side,
                evidence_package=artifact_evidence_package,
                charts=[],
            )
        )
    astrbot_side["visualArtifacts"] = _order_side_by_side_visual_artifacts_for_read(
        _text(evidence_package.get("intent")),
        astrbot_side.get("visualArtifacts"),
    )
    if _should_refresh_side_by_side_evidence_digest(
        astrbot_side,
        evidence_package,
        question=str(enriched.get("question") or ""),
    ):
        digest_evidence_package = _evidence_package_with_side_repair_candidates(evidence_package, astrbot_side)
        astrbot_side["evidenceDigest"] = _side_by_side_evidence_digest_from_package(
            digest_evidence_package,
            question=str(enriched.get("question") or ""),
        )
    if (
        not _text(astrbot_side.get("displayPlan"))
        or _side_by_side_should_refresh_display_plan(
            astrbot_side,
            question=str(enriched.get("question") or ""),
        )
    ):
        astrbot_side["displayPlan"] = _side_by_side_display_plan(
            astrbot_side,
            evidence_package,
            question=str(enriched.get("question") or ""),
        )
    astrbot_side["qualityScore"] = _normalize_astrbot_quality_score_for_read(enriched, astrbot_side)
    review_answer = _business_review_answer_text(astrbot_side)
    if review_answer:
        astrbot_side["answerPreview"] = review_answer[:3000]
    enriched["astrbot"] = astrbot_side
    if not isinstance(enriched.get("businessPlaybook"), dict) or not enriched.get("businessPlaybook"):
        enriched["businessPlaybook"] = build_business_playbook_context(
            country=str(enriched.get("country") or ""),
            question=str(enriched.get("question") or ""),
            evidence_package=evidence_package,
            category=str(enriched.get("category") or ""),
        )
    enriched["failureTags"] = _current_policy_failure_tags(enriched)
    validation = (
        dict(enriched["businessValidation"])
        if isinstance(enriched.get("businessValidation"), dict)
        else {}
    )
    validation.update(_business_validation_projection(enriched))
    enriched["businessValidation"] = validation
    _apply_side_by_side_schema(enriched)
    _sync_side_by_side_display_metrics(enriched)
    return _sanitize_business_report_display_fields(enriched)


def _evidence_package_with_side_repair_candidates(
    evidence_package: dict[str, Any],
    astrbot_side: dict[str, Any],
) -> dict[str, Any]:
    candidates = astrbot_side.get("sourceRepairCandidates")
    if not isinstance(candidates, dict) or not candidates:
        return evidence_package
    if isinstance(evidence_package.get("sourceRepairCandidates"), dict):
        return evidence_package
    merged = dict(evidence_package)
    merged["sourceRepairCandidates"] = dict(candidates)
    return merged


def _normalize_astrbot_quality_score_for_read(
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
) -> dict[str, Any]:
    quality = astrbot_side.get("qualityScore") if isinstance(astrbot_side.get("qualityScore"), dict) else {}
    if not quality:
        return {}
    normalized = dict(quality)
    failures = _string_list(normalized.get("failures"))
    category = _text(record.get("category"))
    evidence_package = astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
    has_actions = _astrbot_side_has_recommended_actions(astrbot_side)
    has_report_output = _has_report_outline_output(astrbot_side)
    has_supporting_evidence = bool(_string_list(astrbot_side.get("evidenceDigest"))) or _evidence_ref_count(evidence_package) > 0
    missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    missing_severity = _missing_evidence_display_severity([
        item for item in missing if isinstance(item, dict)
    ])
    if has_actions and "business_missing_recommended_actions" in failures:
        failures = [item for item in failures if item != "business_missing_recommended_actions"]
        normalized["actionabilityScore"] = max(_optional_float(normalized.get("actionabilityScore")) or 0.0, 1.0)
    if (
        category == "report_generation"
        and has_report_output
        and has_supporting_evidence
        and "missing_supporting_evidence" in failures
    ):
        failures = [item for item in failures if item != "missing_supporting_evidence"]
        normalized["groundingScore"] = max(_optional_float(normalized.get("groundingScore")) or 0.0, 0.85)
        normalized["businessCompletenessScore"] = max(_optional_float(normalized.get("businessCompletenessScore")) or 0.0, 0.9)
    normalized["failures"] = failures
    normalized["totalScore"] = _recompute_quality_total(normalized)
    if "missing_blocking_evidence" in failures or missing_severity == "blocking":
        normalized["totalScore"] = min(_optional_float(normalized.get("totalScore")) or 0.0, 0.74)
        normalized["businessCompletenessScore"] = min(
            _optional_float(normalized.get("businessCompletenessScore")) or 0.0,
            0.72,
        )
    elif "missing_supporting_evidence" in failures or missing_severity == "weakens_answer":
        normalized["totalScore"] = min(_optional_float(normalized.get("totalScore")) or 0.0, 0.88)
        normalized["businessCompletenessScore"] = min(
            _optional_float(normalized.get("businessCompletenessScore")) or 0.0,
            0.85,
        )
    return normalized


def _should_refresh_side_by_side_visual_artifacts(
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any],
) -> bool:
    intent = _text(evidence_package.get("intent"))
    if intent == "competitor_compare":
        return True
    if intent == "market_overview" and _side_by_side_needs_market_overview_artifact_refresh(
        astrbot_side,
        evidence_package=evidence_package,
    ):
        return True
    if intent == "configuration_analysis" and _side_by_side_needs_configuration_artifact_refresh(
        astrbot_side,
        evidence_package=evidence_package,
    ):
        return True
    if intent == "voc_analysis" and _side_by_side_needs_voc_artifact_refresh(astrbot_side):
        return True
    if intent in {"news_policy_search", "pricing_analysis"} and _side_by_side_needs_external_source_artifact_refresh(astrbot_side):
        return True
    if intent == "report_generation" and _side_by_side_needs_report_model_coverage_artifact_refresh(
        astrbot_side,
        evidence_package=evidence_package,
    ):
        return True
    if intent == "inventory_analysis" and _side_by_side_needs_bom_entity_artifact_refresh(astrbot_side):
        return True
    if intent in {"news_policy_search", "pricing_analysis"} and _side_by_side_needs_tco_artifact_refresh(astrbot_side):
        return True
    if intent == "pricing_analysis" and _side_by_side_needs_pricing_artifact_refresh(
        astrbot_side,
        evidence_package=evidence_package,
    ):
        return True
    if intent == "pricing_analysis" and _side_by_side_needs_pending_msrp_artifact_refresh(astrbot_side):
        return True
    if intent == "pricing_analysis" and _side_by_side_needs_msrp_repair_artifact(astrbot_side):
        return True
    return False


def _side_by_side_needs_market_overview_artifact_refresh(
    astrbot_side: dict[str, Any],
    *,
    evidence_package: dict[str, Any],
) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    if not isinstance(artifacts, list):
        return True
    values = [item for item in artifacts if isinstance(item, dict)]
    has_market_table = any(_text(item.get("id")) == "artifact_market_overview_table" for item in values)
    has_chart = any(_text(item.get("type")) == "chart" for item in values)
    has_market_structure_chart = any(
        _text(item.get("id")) == "artifact_market_structure_chart"
        for item in values
    )
    if _side_by_side_has_stale_market_overview_artifacts(values):
        return True
    if _market_overview_has_segment_sales_refs(evidence_package) and not has_market_structure_chart:
        return True
    return not (has_market_table and has_chart)


def _side_by_side_has_stale_market_overview_artifacts(artifacts: list[dict[str, Any]]) -> bool:
    stale_tokens = (
        "avgMsrp",
        "totalRows",
        "countryCount",
        "brandCount",
        "modelCount",
        "versionCount",
        "kpis.totalRows",
        "kpis.avgMsrp",
        "crossCountry.",
        "contextSnapshot.crossTabs",
    )
    for artifact in artifacts:
        artifact_id = _text(artifact.get("id"))
        if artifact_id not in {"artifact_market_overview_table", "artifact_metric_cards"}:
            continue
        text = str(artifact)
        if any(token in text for token in stale_tokens):
            return True
    return False


def _market_overview_has_segment_sales_refs(evidence_package: dict[str, Any]) -> bool:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    signals: set[str] = set()
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            continue
        refs = tool_result.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            label = _text(ref.get("label")).lower()
            if "crosstabs" not in label:
                continue
            if not any(token in label for token in ("drivebysegment", "segmentbyfuel", "segment", "suv")):
                continue
            if not re.search(r"(?:^|[.>/|])(?:sales|volume|registrations|registration|count|value|_total)$", label):
                continue
            if _optional_float(ref.get("value")) is None:
                continue
            signals.add(label)
    return len(signals) >= 2


def _side_by_side_needs_configuration_artifact_refresh(
    astrbot_side: dict[str, Any],
    *,
    evidence_package: dict[str, Any],
) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    if not isinstance(artifacts, list):
        return True
    values = [item for item in artifacts if isinstance(item, dict)]
    has_market_structure_chart = any(
        _text(item.get("id")) == "artifact_market_structure_chart"
        for item in values
    )
    has_external_repair_table = any(
        _text(item.get("id")) == "artifact_external_source_repair_table"
        for item in values
    )
    has_stale_external_repair_table = any(
        _is_stale_external_source_repair_artifact(item)
        for item in values
    )
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    has_external_gap = any(
        isinstance(item, dict)
        and _text(item.get("name")) in {"external_research_claims_unavailable", "minimum_external_sources"}
        for item in missing
    )
    candidates = astrbot_side.get("sourceRepairCandidates")
    has_external_candidates = (
        isinstance(candidates, dict)
        and _is_external_query_source_repair(candidates)
        and _source_candidate_count(candidates) > 0
    )
    if (has_external_gap or has_external_candidates) and (
        not has_external_repair_table or has_stale_external_repair_table
    ):
        return True
    if _configuration_has_market_structure_refs(evidence_package) and not has_market_structure_chart:
        return True
    for artifact in artifacts:
        if not isinstance(artifact, dict) or _text(artifact.get("id")) != "artifact_configuration_analysis_table":
            continue
        spec = artifact.get("spec") if isinstance(artifact.get("spec"), dict) else {}
        columns = [_text(item) for item in spec.get("columns", [])]
        return "validationData" not in columns or "sourceOrTool" not in columns or "acceptanceCriteria" not in columns
    return True


def _configuration_has_market_structure_refs(evidence_package: dict[str, Any]) -> bool:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    signals: set[str] = set()
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            continue
        refs = tool_result.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            label = _text(ref.get("label")).lower()
            has_cross_tab_signal = (
                "crosstabs" in label
                and any(token in label for token in ("drivebysegment", "segmentbyfuel", "registrationbyfuel"))
                and re.search(r"(?:^|[.>/|])(?:sales|volume|registrations|registration|count|value|_total)$", label)
            )
            has_powertrain_signal = (
                "contextsnapshot.powertrainmix" in label
                and re.search(r"(?:^|[.>/|])(?:sales|volume|registrations|registration|count|value|_total)$", label)
            )
            if not has_cross_tab_signal and not has_powertrain_signal:
                continue
            if _optional_float(ref.get("value")) is None:
                continue
            signals.add(label)
    return len(signals) >= 2


def _side_by_side_needs_voc_artifact_refresh(astrbot_side: dict[str, Any]) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    values = [item for item in artifacts if isinstance(item, dict)] if isinstance(artifacts, list) else []
    has_voc_table = any(
        _text(item.get("id")) in {"artifact_voc_analysis_table", "artifact_voc_analysis_framework_table"}
        for item in values
    )
    has_external_repair_table = any(
        _text(item.get("id")) == "artifact_external_source_repair_table"
        for item in values
    )
    has_stale_external_repair_table = any(
        _is_stale_external_source_repair_artifact(item)
        for item in values
    )
    candidates = astrbot_side.get("sourceRepairCandidates")
    has_external_candidates = False
    if isinstance(candidates, dict):
        data_status = _text(candidates.get("dataStatus")).lower()
        has_external_candidates = (
            any(token in data_status for token in ("external_research", "voc", "policy"))
            and _source_candidate_count(candidates) > 0
        )
    evidence_package = astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    has_voc_source_gap = any(
        isinstance(item, dict)
        and _text(item.get("name")) in {"external_research_claims_unavailable", "minimum_external_sources"}
        for item in missing
    )
    return (
        (has_voc_source_gap and (not has_voc_table or not has_external_repair_table))
        or (has_external_candidates and (not has_external_repair_table or has_stale_external_repair_table))
    )


def _side_by_side_needs_external_source_artifact_refresh(astrbot_side: dict[str, Any]) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    values = [item for item in artifacts if isinstance(item, dict)] if isinstance(artifacts, list) else []
    has_external_repair_table = any(
        _text(item.get("id")) == "artifact_external_source_repair_table"
        for item in values
    )
    has_stale_external_repair_table = any(
        _is_stale_external_source_repair_artifact(item)
        for item in values
    )
    evidence_package = astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    has_external_gap = any(
        isinstance(item, dict)
        and _text(item.get("name")) in {
            "specific_policy_source_evidence",
            "minimum_external_sources",
            "official_source",
            "source_date",
        }
        for item in missing
    )
    return has_external_gap and (not has_external_repair_table or has_stale_external_repair_table)


def _is_stale_external_source_repair_artifact(artifact: dict[str, Any]) -> bool:
    if _text(artifact.get("id")) != "artifact_external_source_repair_table":
        return False
    spec = artifact.get("spec") if isinstance(artifact.get("spec"), dict) else {}
    columns = [_text(item) for item in spec.get("columns", [])]
    if not columns:
        rows = ((artifact.get("data") or {}).get("rows") if isinstance(artifact.get("data"), dict) else [])
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            columns = list(rows[0].keys())
    return "sourceNeed" not in columns or "evidenceUse" not in columns or "canUseInAnswer" not in columns


def _side_by_side_needs_bom_entity_artifact_refresh(astrbot_side: dict[str, Any]) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    if isinstance(artifacts, list) and any(
        isinstance(item, dict) and _text(item.get("id")) == "artifact_bom_entity_validation_table"
        for item in artifacts
    ):
        return False
    evidence_package = astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    if any(
        isinstance(item, dict)
        and _text(item.get("name")) in {
            "bom_entity_mapping_evidence",
            "inventory_bom_weak_evidence_refs",
            "query_with_filters_weak_evidence_refs",
        }
        for item in missing
    ):
        return True
    text = " ".join(
        [
            _text(astrbot_side.get("answerPreview")),
            _text(astrbot_side.get("direct")),
            _text(astrbot_side.get("summary")),
        ]
    ).lower()
    return any(
        token in text
        for token in (
            "bom",
            "material",
            "materialcode",
            "variant",
            "lifecycle",
            "inventory",
            "物料",
            "版本",
            "内外饰",
            "颜色",
            "生命周期",
            "可编辑数量",
        )
    )


def _side_by_side_needs_report_model_coverage_artifact_refresh(
    astrbot_side: dict[str, Any],
    *,
    evidence_package: dict[str, Any],
) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    artifact_values = [item for item in artifacts if isinstance(item, dict)] if isinstance(artifacts, list) else []
    has_coverage_table = any(
        _text(item.get("id")) == "artifact_report_model_coverage_table"
        for item in artifact_values
    )
    has_coverage_chart = any(
        _text(item.get("id")) == "artifact_report_model_coverage_chart"
        for item in artifact_values
    )
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    has_model_coverage_gap = any(
        isinstance(item, dict)
        and _text(item.get("name")) in {
            "competitive_or_configuration_data_unavailable",
            "configuration_delta",
            "current_msrp",
            "coverage_diagnostic:no_current_prices_for_requested_models",
        }
        for item in missing
    )
    if has_coverage_table and has_model_coverage_gap and not has_coverage_chart:
        return True
    if has_coverage_table:
        return _side_by_side_report_artifacts_have_generic_noise(artifact_values, evidence_package)
    if has_model_coverage_gap:
        return True
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    model_count = 0
    for key in ("models", "competitors"):
        entity_values = entities.get(key)
        if isinstance(entity_values, list):
            model_count += len(entity_values)
    return model_count >= 3 and not any(
        _text(item.get("id")) == "artifact_report_generation_table"
        for item in artifact_values
    )


def _side_by_side_report_artifacts_have_generic_noise(
    artifacts: list[dict[str, Any]],
    evidence_package: dict[str, Any],
) -> bool:
    entity_keys = _side_by_side_report_entity_keys(evidence_package)
    for artifact in artifacts:
        artifact_id = _text(artifact.get("id"))
        data = artifact.get("data") if isinstance(artifact.get("data"), dict) else {}
        rows = data.get("rows") if isinstance(data.get("rows"), list) else []
        if artifact_id == "artifact_metric_cards":
            for row in rows:
                if not isinstance(row, dict):
                    continue
                label = _text(row.get("label")).casefold()
                if any(token in label for token in ("cumulativesales", "avgmsrp", "modelcount", "versioncount")):
                    return True
        if artifact_id == "artifact_report_generation_table" and entity_keys:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_text = _text(row)
                mentioned = _side_by_side_report_known_model_keys(row_text)
                if mentioned and not any(key in entity_keys for key in mentioned):
                    return True
    return False


def _side_by_side_report_entity_keys(evidence_package: dict[str, Any]) -> set[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    result: set[str] = set()
    for key in ("models", "competitors"):
        values = entities.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            normalized = re.sub(r"[^a-z0-9]+", "", _text(item).casefold())
            if normalized:
                result.add(normalized)
    return result


def _side_by_side_report_known_model_keys(value: str) -> set[str]:
    text = _text(value).casefold()
    models = {
        "ex30",
        "ex40",
        "ex60",
        "ev3",
        "ev9",
        "o5",
        "o5bev",
        "o9",
        "j7",
        "j8",
        "xc40",
        "xc60",
        "xc90",
        "rav4",
        "sportage",
        "sorento",
        "enyaq",
        "tayron",
        "id4",
        "id7",
        "modely",
    }
    normalized = re.sub(r"[^a-z0-9]+", "", text)
    return {model for model in models if model in normalized}


def _side_by_side_needs_tco_artifact_refresh(astrbot_side: dict[str, Any]) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    evidence_package = astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    has_tco_gap = any(
        isinstance(item, dict) and _text(item.get("name")) == "leasing_tco_or_company_car_evidence"
        for item in missing
    )
    if isinstance(artifacts, list) and any(
        isinstance(item, dict) and _text(item.get("id")) == "artifact_tco_validation_table"
        for item in artifacts
    ):
        return has_tco_gap and _side_by_side_has_stale_tco_pricing_artifacts(artifacts, evidence_package)
    if has_tco_gap:
        return True
    text = " ".join(
        [
            _text(astrbot_side.get("answerPreview")),
            _text(astrbot_side.get("direct")),
            _text(astrbot_side.get("summary")),
        ]
    ).lower()
    return any(
        token in text
        for token in (
            "leasing",
            "tco",
            "residual",
            "company car",
            "company-car",
            "fleet",
            "月供",
            "残值",
            "公司车",
            "大客户",
        )
    )


def _side_by_side_needs_pricing_artifact_refresh(
    astrbot_side: dict[str, Any],
    *,
    evidence_package: dict[str, Any],
) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    values = [item for item in artifacts if isinstance(item, dict)] if isinstance(artifacts, list) else []
    pricing_table = next(
        (
            item
            for item in values
            if _text(item.get("id")) == "artifact_pricing_analysis_table"
        ),
        None,
    )
    has_pricing_refs = any(
        "price" in _text(ref.get("label")).casefold()
        or "msrp" in _text(ref.get("label")).casefold()
        for ref in _side_by_side_evidence_refs(evidence_package)
    )
    if pricing_table is None:
        return has_pricing_refs
    data = pricing_table.get("data") if isinstance(pricing_table.get("data"), dict) else {}
    rows = data.get("rows") if isinstance(data.get("rows"), list) else []
    row_text = str(rows)
    if any(
        token in row_text
        for token in (
            "搜索候选",
            "来源草稿",
            "补证线索",
            "不能直接当作",
            "生成当前价格记录",
        )
    ):
        return True
    if _side_by_side_has_stale_o5_ev3_report_block(values):
        return True
    has_relative_delta = any(
        "user supplied relative price delta" in _text(ref.get("label")).casefold()
        for ref in _side_by_side_evidence_refs(evidence_package)
    )
    if has_relative_delta and not any(
        isinstance(row, dict)
        and (
            _text(row.get("model")) == "Relative price delta"
            or "用户给定价差" in _text(row.get("pricePosition"))
        )
        for row in rows
    ):
        return True
    return False


def _side_by_side_has_stale_o5_ev3_report_block(artifacts: list[dict[str, Any]]) -> bool:
    for artifact in artifacts:
        if _text(artifact.get("id")) != "artifact_report_block":
            continue
        data = artifact.get("data") if isinstance(artifact.get("data"), dict) else {}
        key_message = _text(data.get("keyMessage"))
        if (
            "O5" in key_message
            and "EV3" in key_message
            and "当前价格样本显示" in key_message
        ):
            return True
    return False


def _side_by_side_has_stale_tco_pricing_artifacts(
    artifacts: list[Any],
    evidence_package: dict[str, Any],
) -> bool:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    has_requested_models = any(
        isinstance(entities.get(key), list) and any(_text(item) for item in entities.get(key, []))
        for key in ("models", "competitors")
    )
    if has_requested_models:
        return False
    stale_ids = {
        "artifact_pricing_corridor_chart",
        "artifact_pricing_analysis_table",
        "artifact_metric_cards",
    }
    return any(
        isinstance(item, dict) and _text(item.get("id")) in stale_ids
        for item in artifacts
    )


def _side_by_side_needs_msrp_repair_artifact(astrbot_side: dict[str, Any]) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    if isinstance(artifacts, list) and any(
        isinstance(item, dict) and _text(item.get("id")) == "artifact_msrp_source_repair_table"
        for item in artifacts
    ):
        return False
    candidates = astrbot_side.get("sourceRepairCandidates")
    if not isinstance(candidates, dict):
        return False
    data_status = _text(candidates.get("dataStatus")).lower()
    if any(token in data_status for token in ("policy", "external_research", "voc")):
        return False
    return _source_candidate_count(candidates) > 0


def _side_by_side_needs_pending_msrp_artifact_refresh(astrbot_side: dict[str, Any]) -> bool:
    candidates = astrbot_side.get("sourceRepairCandidates")
    if not isinstance(candidates, dict):
        return False
    if not _source_repair_candidates_have_review_pending(candidates):
        return False
    artifacts = astrbot_side.get("visualArtifacts")
    if not isinstance(artifacts, list):
        return True
    ids = {_text(item.get("id")) for item in artifacts if isinstance(item, dict)}
    return "artifact_pending_msrp_review_table" not in ids


def _source_repair_candidates_have_review_pending(candidates: dict[str, Any]) -> bool:
    try:
        if int(candidates.get("reviewPendingObservationCount") or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    for key in ("ownModel", "competitorCorridor"):
        rows = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in rows:
            if isinstance(entry, dict) and _source_candidate_review_pending_rows(entry) > 0:
                return True
    return False


def _order_side_by_side_visual_artifacts_for_read(intent: str, artifacts: Any) -> list[dict[str, Any]]:
    if not isinstance(artifacts, list):
        return []
    values = [
        _sanitize_side_by_side_visual_artifact_for_read(dict(item), intent)
        for item in artifacts
        if isinstance(item, dict)
    ]
    if intent not in {
        "pricing_analysis",
        "competitor_compare",
        "report_generation",
        "market_overview",
        "configuration_analysis",
        "voc_analysis",
        "news_policy_search",
        "inventory_analysis",
    }:
        return values
    return [
        item
        for _, item in sorted(
            enumerate(values),
            key=lambda pair: (_side_by_side_visual_artifact_priority(intent, pair[1]), pair[0]),
        )
    ]


def _sanitize_side_by_side_visual_artifact_for_read(artifact: dict[str, Any], intent: str) -> dict[str, Any]:
    artifact_type = _text(artifact.get("type"))
    artifact_id = _text(artifact.get("id"))
    if artifact_type == "metric_cards":
        return _sanitize_metric_cards_visual_artifact_for_read(artifact)
    if artifact_id == "artifact_report_generation_table":
        return _sanitize_report_generation_table_visual_artifact_for_read(artifact)
    if artifact_type != "report_block":
        return artifact
    data = artifact.get("data")
    if not isinstance(data, dict):
        return artifact
    cleaned = dict(data)
    for key in ("keyMessage", "productImplication", "nextAction"):
        if key not in cleaned:
            continue
        text = _strip_source_candidate_boundary_sentence(_sanitize_business_review_line(_text(cleaned.get(key))))
        if key == "productImplication" and _is_generic_report_product_implication_for_read(text):
            text = _report_product_implication_fallback_for_read(intent)
        cleaned[key] = text
    artifact["data"] = cleaned
    return artifact


def _sanitize_report_generation_table_visual_artifact_for_read(artifact: dict[str, Any]) -> dict[str, Any]:
    data = artifact.get("data")
    if not isinstance(data, dict):
        return artifact
    rows = data.get("rows")
    if not isinstance(rows, list):
        return artifact
    cleaned_rows = []
    for row in rows:
        if not isinstance(row, dict):
            cleaned_rows.append(row)
            continue
        cleaned = dict(row)
        for key in ("section", "evidence", "source", "businessUse", "nextAction", "confidence"):
            if key not in cleaned:
                continue
            cleaned[key] = _sanitize_report_generation_table_cell_for_read(key, cleaned.get(key))
        cleaned_rows.append(cleaned)
    cleaned_data = dict(data)
    cleaned_data["rows"] = cleaned_rows
    artifact["data"] = cleaned_data
    return artifact


def _sanitize_report_generation_table_cell_for_read(key: str, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = _strip_source_candidate_boundary_sentence(_sanitize_business_review_line(_text(value)))
    if key != "evidence":
        return text
    match = re.match(r"^([^:：]+)\s*[:：]\s*(.+)$", text)
    if not match:
        return text
    label = _side_by_side_metric_card_label_for_read(match.group(1).strip())
    metric_value = match.group(2).strip()
    if label == "价格样本数":
        metric_value = re.sub(r"\s+(?:EUR|SEK|currency)\s*$", "", metric_value, flags=re.IGNORECASE)
    return f"{label}: {metric_value}" if label else text


def _sanitize_metric_cards_visual_artifact_for_read(artifact: dict[str, Any]) -> dict[str, Any]:
    data = artifact.get("data")
    if not isinstance(data, dict):
        return artifact
    rows = data.get("rows")
    if not isinstance(rows, list):
        return artifact
    cleaned_rows = []
    for row in rows:
        if not isinstance(row, dict):
            cleaned_rows.append(row)
            continue
        cleaned = dict(row)
        label = _text(cleaned.get("label"))
        if label:
            cleaned["label"] = _side_by_side_metric_card_label_for_read(label)
        source = _text(cleaned.get("source"))
        if source:
            cleaned["source"] = _sanitize_business_review_line(source)
        cleaned_rows.append(cleaned)
    cleaned_data = dict(data)
    cleaned_data["rows"] = cleaned_rows
    artifact["data"] = cleaned_data
    return artifact


def _side_by_side_metric_card_label_for_read(label: str) -> str:
    context_label = _side_by_side_context_ref_label(label)
    if context_label:
        return context_label
    model_metric_match = re.match(r"^([A-Za-z0-9][A-Za-z0-9 ._/-]{0,60})\.sales$", label)
    if model_metric_match:
        model_name = re.sub(r"\s+", " ", model_metric_match.group(1).replace("_", " ")).strip()
        if model_name:
            return f"{model_name} 销量"
    replacements = {
        "cumulativeSales": "累计销量",
        "avgMsrp": "平均 MSRP",
        "priceStats.min": "价格走廊下沿",
        "priceStats.max": "价格走廊上沿",
        "priceStats.avg": "价格样本均值",
        "priceStats.median": "价格样本中位数",
        "priceStats.count": "价格样本数",
        "priceStats.currency": "价格货币",
    }
    return replacements.get(label, _sanitize_business_review_line(label))


def _is_generic_report_product_implication_for_read(value: str) -> bool:
    text = _text(value)
    return text.startswith((
        "结论要能转成",
        "竞品定位方法",
        "定价走廊方法",
        "市场机会方法",
        "配置价值方法",
    )) or "下一步应补齐缺失证据后再收敛结论" in text


def _report_product_implication_fallback_for_read(intent: str) -> str:
    if intent == "competitor_compare":
        return "把已验证的销量、价格、级别或配置锚点转成定位差异、可赢点、短板和销售话术。"
    if intent == "pricing_analysis":
        return "把价格证据转成价格锚点、主推配置、风险边界和销售话术。"
    if intent == "configuration_analysis":
        return "把配置证据转成主销配置、可感知价值、成本风险和销售话术。"
    return "把可引用证据转成业务动作、风险边界和可复用汇报结构。"


def _side_by_side_visual_artifact_priority(intent: str, artifact: dict[str, Any]) -> int:
    artifact_id = _text(artifact.get("id"))
    artifact_type = _text(artifact.get("type"))
    if intent == "pricing_analysis":
        if artifact_id == "artifact_external_source_repair_table":
            return 0
        if artifact_id == "artifact_tco_validation_table":
            return 1
        if artifact_id == "artifact_pricing_corridor_chart":
            return 2
        if artifact_id == "artifact_pending_msrp_review_chart":
            return 3
        if artifact_id == "artifact_pricing_analysis_table":
            return 4
        if artifact_id == "artifact_pending_msrp_review_table":
            return 5
        if artifact_id == "artifact_msrp_source_repair_table":
            return 6
        if artifact_id == "artifact_pricing_analysis_framework_table":
            return 7
        if artifact_type == "report_block":
            return 8
        if artifact_type == "metric_cards":
            return 9
    if intent == "competitor_compare":
        if artifact_id == "artifact_market_structure_chart":
            return 0
        if artifact_id == "artifact_competitor_evidence_chart":
            return 1
        if artifact_id == "artifact_competitor_compare_table":
            return 2
        if artifact_id == "artifact_pending_msrp_review_chart":
            return 3
        if artifact_id == "artifact_pending_msrp_review_table":
            return 4
        if artifact_id == "artifact_msrp_source_repair_table":
            return 5
        if artifact_type == "report_block":
            return 6
        if artifact_type == "metric_cards":
            return 7
    if intent == "report_generation":
        if artifact_type == "report_block":
            return 0
        if artifact_id == "artifact_report_model_coverage_chart":
            return 1
        if artifact_id == "artifact_pricing_corridor_chart":
            return 1
        if artifact_id == "artifact_report_model_coverage_table":
            return 2
        if artifact_id == "artifact_report_pricing_table":
            return 3
        if artifact_id == "artifact_report_generation_table":
            return 4
        if artifact_type == "chart":
            return 5
        if artifact_type == "metric_cards":
            return 6
    if intent == "market_overview":
        if artifact_type == "chart":
            return 0
        if artifact_id == "artifact_market_overview_table":
            return 1
        if artifact_type == "report_block":
            return 2
        if artifact_type == "metric_cards":
            return 3
    if intent == "configuration_analysis":
        if artifact_id in {"artifact_market_structure_chart", "artifact_market_powertrain_mix_chart"}:
            return 0
        if artifact_id == "artifact_external_source_repair_table":
            return 1
        if artifact_id == "artifact_configuration_analysis_table":
            return 2
        if artifact_type == "report_block":
            return 3
        if artifact_type == "metric_cards":
            return 4
    if intent == "inventory_analysis":
        if artifact_id == "artifact_bom_entity_validation_table":
            return 0
        if artifact_id == "artifact_inventory_analysis_table":
            return 1
        if artifact_id.endswith("_table"):
            return 2
        if artifact_type == "report_block":
            return 3
        if artifact_type == "metric_cards":
            return 5
    if intent == "voc_analysis":
        if artifact_id == "artifact_external_source_repair_table":
            return 0
        if artifact_id in {"artifact_voc_analysis_table", "artifact_voc_analysis_framework_table"}:
            return 1
        if artifact_type == "report_block":
            return 2
        if artifact_type == "metric_cards":
            return 3
    if intent == "news_policy_search":
        if artifact_id == "artifact_external_source_repair_table":
            return 0
        if artifact_type == "table":
            return 1
        if artifact_type == "report_block":
            return 2
        if artifact_type == "chart":
            return 3
        if artifact_type == "metric_cards":
            return 4
    return 5


def _astrbot_side_has_recommended_actions(astrbot_side: dict[str, Any]) -> bool:
    actions = astrbot_side.get("recommendedActions")
    if isinstance(actions, list) and any(isinstance(item, dict) or _text(item) for item in actions):
        return True
    for line in _string_list(astrbot_side.get("reportReadyBullets")):
        if re.match(r"^(Next action|下一步动作)\s*[：:]", line, flags=re.IGNORECASE):
            return True
    synthesis = astrbot_side.get("businessSynthesisPlan")
    if isinstance(synthesis, dict):
        synthesis_actions = synthesis.get("recommendedActions")
        if isinstance(synthesis_actions, list) and any(isinstance(item, dict) or _text(item) for item in synthesis_actions):
            return True
    return False


def _recompute_quality_total(quality: dict[str, Any]) -> float:
    intent_score = _optional_float(quality.get("intentScore"))
    tool_score = _optional_float(quality.get("toolScore"))
    grounding_score = _optional_float(quality.get("groundingScore"))
    follow_score = _optional_float(quality.get("followUpScore"))
    safety_score = _optional_float(quality.get("safetyScore"))
    if None in {intent_score, tool_score, grounding_score, follow_score, safety_score}:
        return _optional_float(quality.get("totalScore")) or 0.0
    return round(
        (float(intent_score) * 0.2)
        + (float(tool_score) * 0.25)
        + (float(grounding_score) * 0.25)
        + (float(follow_score) * 0.15)
        + (float(safety_score) * 0.15),
        3,
    )


def _actual_tools_from_astrbot_side(astrbot_side: dict[str, Any]) -> list[str]:
    return _actual_tools_from_result(
        {
            "metadata": {
                "selectedTool": astrbot_side.get("selectedTool"),
                "toolsUsed": astrbot_side.get("toolsUsed"),
                "toolCalls": astrbot_side.get("toolCalls"),
            },
            "data": {
                "toolCalls": astrbot_side.get("toolCalls"),
                "toolsUsed": astrbot_side.get("toolsUsed"),
                "evidencePackage": astrbot_side.get("evidencePackage"),
            },
        }
    )


def _sanitize_business_report_display_fields(value: Any) -> Any:
    if isinstance(value, str):
        return _sanitize_business_report_display_text(value)
    if isinstance(value, list):
        return [_sanitize_business_report_display_fields(item) for item in value]
    if isinstance(value, dict):
        return {
            key: _sanitize_business_report_display_fields(item)
            for key, item in value.items()
        }
    return value


def _sanitize_business_report_display_text(value: str) -> str:
    text = _text(value)
    if not text:
        return ""
    internal_markers = (
        "Use this source",
        "Next:",
        "Business Composer:",
        "Grounding guard:",
        "published_date",
        "external_research_claims_unavailable",
        "source_repair_candidates",
        "decision_boundary",
        "weakens_answer",
        "source/date/count refs",
        "no supported claim",
        "no citation-ready",
        "External research was required",
        "JATO historical data is not",
        "Research policy requires publish dates",
        "partially_aligned",
        "evidenceRefs",
        "evidenceRef",
        "confidence high",
        "confidence medium",
        "confidence low",
        "置信度 high",
        "置信度 medium",
        "置信度 low",
        "这题需要先给业务立场",
    )
    if not any(marker in text for marker in internal_markers):
        return text
    if not _looks_like_user_visible_business_text(text):
        return text
    return _sanitize_business_review_line(text)


def _looks_like_user_visible_business_text(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if any(ch.isspace() for ch in text):
        return True
    if any(ch in text for ch in "：:。；;，,、（）()[]【】"):
        return True
    return len(text) > 80


def _normalize_astrbot_missing_evidence_for_current_policy(
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
) -> dict[str, Any]:
    """Normalize legacy persisted missingEvidence to the current scoring policy."""
    normalized_side = dict(astrbot_side)
    category = str(record.get("category") or "")
    expected_intent = str(record.get("expectedIntent") or "")
    question = str(record.get("question") or "")
    evidence_package = normalized_side.get("evidencePackage")
    normalized_package = None
    if isinstance(evidence_package, dict):
        normalized_package = _normalize_legacy_evidence_refs_for_read(dict(evidence_package))
        normalized_package = _normalize_legacy_configuration_coverage_for_read(record, normalized_package)
    missing: list[Any] = []
    top_level_missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    package_missing = (
        normalized_package.get("missingEvidence")
        if isinstance(normalized_package, dict) and isinstance(normalized_package.get("missingEvidence"), list)
        else []
    )
    missing.extend(top_level_missing)
    missing.extend(package_missing)

    normalized_missing = [
        _normalize_missing_evidence_item(
            item,
            category=category,
            expected_intent=expected_intent,
            question=question,
            has_user_target_price=_has_user_supplied_target_price_evidence(normalized_package),
        )
        for item in missing
        if isinstance(item, dict)
    ]
    normalized_missing = _ensure_missing_evidence_items([], normalized_missing)
    if _has_report_outline_output(astrbot_side):
        normalized_missing = [
            item
            for item in normalized_missing
            if str(item.get("name") or "") != "report_outline"
        ]
    if normalized_package and _has_published_date_ref(normalized_package):
        normalized_missing = [
            item
            for item in normalized_missing
            if str(item.get("name") or "") != "published_date"
        ]
    is_voc_record = (
        category == "voc"
        or expected_intent == "voc_analysis"
        or (
            isinstance(normalized_package, dict)
            and _text(normalized_package.get("intent")) == "voc_analysis"
        )
    )
    if is_voc_record and _has_citation_ready_external_voc_evidence(normalized_package, question=question):
        normalized_missing = [
            item
            for item in normalized_missing
            if str(item.get("name") or "") not in {"external_research_claims_unavailable", "minimum_external_sources"}
        ]
        if _voc_question_requires_frequency_evidence(question):
            normalized_missing = _ensure_missing_evidence_items(
                normalized_missing,
                [
                    {
                        "name": "voc_frequency_or_representativeness",
                        "reason": "Citation-ready VOC/external source exists, but frequency and representativeness are not yet quantified; do not claim the theme is high-frequency.",
                        "impact": "weakens_answer",
                    },
                ],
            )
    if _voc_record_needs_source_gap(
        record=record,
        astrbot_side=normalized_side,
        evidence_package=normalized_package,
    ):
        normalized_missing = _ensure_missing_evidence_items(
            normalized_missing,
            [
                {
                    "name": "external_research_claims_unavailable",
                    "reason": "No citation-ready VOC source-backed claim evidence was available.",
                    "impact": "weakens_answer",
                },
                {
                    "name": "minimum_external_sources",
                    "reason": "voc_analysis requires at least 1 citation-ready external or VOC source.",
                    "impact": "weakens_answer",
                },
            ],
        )
    if _inventory_record_needs_entity_gap(
        record=record,
        astrbot_side=normalized_side,
        evidence_package=normalized_package,
    ):
        normalized_missing = _ensure_missing_evidence_items(
            normalized_missing,
            [
                {
                    "name": "bom_entity_mapping_evidence",
                    "reason": "Inventory/BOM question has no material-code, color, lifecycle, order, or editable-quantity evidence refs.",
                    "impact": "weakens_answer",
                },
            ],
        )
    if _configuration_record_needs_matrix_gap(
        record=record,
        astrbot_side=normalized_side,
        evidence_package=normalized_package,
    ):
        normalized_missing = _ensure_missing_evidence_items(
            normalized_missing,
            [
                {
                    "name": "competitive_or_configuration_data_unavailable",
                    "reason": "Configuration question has no citation-ready battery, charging, winter-package, trim, or feature-delta evidence refs.",
                    "impact": "weakens_answer",
                },
            ],
        )
    if isinstance(normalized_package, dict):
        normalized_missing = _ensure_missing_evidence_items(
            normalized_missing,
            _coverage_diagnostic_missing_items(normalized_package),
        )
    if _report_generation_record_needs_model_coverage_gap(
        record=record,
        astrbot_side=normalized_side,
        evidence_package=normalized_package,
    ):
        normalized_missing = _ensure_missing_evidence_items(
            normalized_missing,
            [
                {
                    "name": "competitive_or_configuration_data_unavailable",
                    "reason": "Multi-model report question has no evidence refs covering all requested models.",
                    "impact": "weakens_answer",
                },
            ],
        )
    if _leasing_tco_record_needs_evidence_gap(
        record=record,
        astrbot_side=normalized_side,
        evidence_package=normalized_package,
    ):
        normalized_missing = _ensure_missing_evidence_items(
            normalized_missing,
            [
                {
                    "name": "leasing_tco_or_company_car_evidence",
                    "reason": "Question requires leasing, TCO, residual value, fleet, or company-car benefit evidence; current refs only provide generic price, market, or unrelated search context.",
                    "impact": "weakens_answer",
                },
            ],
        )
    if _specific_policy_record_needs_source_gap(
        record=record,
        astrbot_side=normalized_side,
        evidence_package=normalized_package,
    ):
        normalized_missing = _ensure_missing_evidence_items(
            normalized_missing,
            [
                {
                    "name": "specific_policy_source_evidence",
                    "reason": "Named policy question has no evidence refs that mention the requested policy by name.",
                    "impact": "weakens_answer",
                },
            ],
        )
    normalized_side["missingEvidence"] = normalized_missing
    if normalized_package is not None:
        normalized_package["missingEvidence"] = normalized_missing
        normalized_package = _normalize_evidence_package_status_for_missing_evidence(normalized_package, normalized_missing)
        normalized_side["evidencePackage"] = normalized_package
        normalized_side["evidenceRefCount"] = _evidence_ref_count(normalized_package)
    normalized_side = _normalize_astrbot_answer_status_for_missing_evidence(normalized_side, normalized_missing)
    return normalized_side


def _normalize_legacy_configuration_coverage_for_read(
    record: dict[str, Any],
    evidence_package: dict[str, Any],
) -> dict[str, Any]:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return evidence_package
    if _evidence_package_has_configuration_ref(evidence_package):
        return evidence_package

    updated_results: list[Any] = []
    additions: list[dict[str, Any]] = []
    changed = False
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            updated_results.append(tool_result)
            continue
        if str(tool_result.get("toolName") or "") != "compare_vehicle_variants":
            updated_results.append(tool_result)
            continue
        diagnostics = tool_result.get("coverageDiagnostics")
        if isinstance(diagnostics, dict) and str(diagnostics.get("diagnosis") or "").strip():
            updated_results.append(tool_result)
            continue
        if not _legacy_compare_vehicle_variants_is_empty(tool_result):
            updated_results.append(tool_result)
            continue
        query_models = _legacy_compare_vehicle_variant_query_models(record, evidence_package, tool_result)
        if len(query_models) < 2:
            updated_results.append(tool_result)
            continue
        diagnostics = _configuration_coverage_diagnostics_for_read(
            _text(record.get("country") or evidence_package.get("country")),
            tuple(query_models),
        )
        if not diagnostics:
            updated_results.append(tool_result)
            continue
        normalized_tool = dict(tool_result)
        normalized_tool["coverageDiagnostics"] = diagnostics
        if int(normalized_tool.get("rowCount") or 0) <= 1:
            normalized_tool["rowCount"] = 0
        additions.extend(_coverage_diagnostic_missing_items({"missingEvidence": [], "toolResults": [normalized_tool]}))
        updated_results.append(normalized_tool)
        changed = True

    if not changed:
        return evidence_package
    normalized_package = dict(evidence_package)
    normalized_package["toolResults"] = updated_results
    normalized_package["missingEvidence"] = _ensure_missing_evidence_items(
        evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else [],
        additions,
    )
    return normalized_package


def _legacy_compare_vehicle_variants_is_empty(tool_result: dict[str, Any]) -> bool:
    refs = tool_result.get("evidenceRefs")
    if isinstance(refs, list):
        usable_config_refs = [
            ref for ref in refs
            if isinstance(ref, dict)
            and is_usable_evidence_ref(ref)
            and _side_by_side_is_configuration_ref(ref, tool=tool_result)
        ]
        if usable_config_refs:
            return False
    return True


def _legacy_compare_vehicle_variant_query_models(
    record: dict[str, Any],
    evidence_package: dict[str, Any],
    tool_result: dict[str, Any],
) -> list[str]:
    values: list[str] = []
    query = tool_result.get("query") if isinstance(tool_result.get("query"), dict) else {}
    values.extend(_string_list(query.get("models")))
    if _text(query.get("model")):
        values.append(_text(query.get("model")))
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values.extend(_string_list(entities.get("models")))
    values.extend(_side_by_side_requested_entity_names(evidence_package, question=_text(record.get("question"))))
    return _side_by_side_unique_model_names(_dedupe_string_list(values))[:5]


@lru_cache(maxsize=128)
def _configuration_coverage_diagnostics_for_read(country: str, query_models: tuple[str, ...]) -> dict[str, Any]:
    models = [item for item in query_models if _text(item)]
    if len(models) < 2:
        models = models[:1]
    try:
        result = engineering_variant_diff_service.compare_market_variants_from_db(
            country=country or "Sweden",
            models=models,
            max_subjects=min(max(2, len(models)), 5),
            max_diff_features=1,
            max_common_features=0,
        )
    except Exception:  # noqa: BLE001
        return {}
    diagnostics = result.get("coverageDiagnostics") if isinstance(result, dict) and isinstance(result.get("coverageDiagnostics"), dict) else {}
    if not str(diagnostics.get("diagnosis") or "").strip():
        return {}
    return dict(diagnostics)


def _coverage_diagnostic_missing_items(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return []
    result: list[dict[str, Any]] = []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        diagnostics = tool.get("coverageDiagnostics")
        if not isinstance(diagnostics, dict):
            continue
        diagnosis = str(diagnostics.get("diagnosis") or "").strip()
        if not diagnosis:
            continue
        next_actions = diagnostics.get("nextActions") if isinstance(diagnostics.get("nextActions"), list) else []
        reason = next((str(item).strip() for item in next_actions if str(item).strip()), "")
        result.append({
            "name": f"coverage_diagnostic:{diagnosis}",
            "reason": reason or f"Coverage diagnostic reported {diagnosis}.",
            "impact": "weakens_answer",
        })
    return result


def _configuration_record_needs_matrix_gap(
    *,
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any] | None,
) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    category = _text(record.get("category"))
    expected_intent = _text(record.get("expectedIntent"))
    intent = _text(evidence_package.get("intent"))
    if category != "configuration" and expected_intent != "configuration_analysis" and intent != "configuration_analysis":
        return False
    if _evidence_package_has_configuration_ref(evidence_package):
        return False
    missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    return not any(
        isinstance(item, dict)
        and str(item.get("name") or "") in {
            "competitive_or_configuration_data_unavailable",
            "configuration_delta",
            "config_gap_evidence",
        }
        for item in missing
    )


def _evidence_package_has_configuration_ref(evidence_package: dict[str, Any]) -> bool:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            continue
        refs = tool_result.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if isinstance(ref, dict) and _side_by_side_is_configuration_ref(ref, tool=tool_result):
                return True
    return False


def _report_generation_record_needs_model_coverage_gap(
    *,
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any] | None,
) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    category = _text(record.get("category"))
    expected_intent = _text(record.get("expectedIntent"))
    intent = _text(evidence_package.get("intent"))
    if category != "report_generation" and expected_intent != "report_generation" and intent != "report_generation":
        return False
    question = _text(record.get("question"))
    requested = _side_by_side_unique_model_names(_side_by_side_requested_entity_names(evidence_package, question=question))
    if len(requested) < 2:
        return False
    if _side_by_side_report_model_coverage_is_complete(evidence_package, requested):
        return False
    missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    return not any(
        isinstance(item, dict)
        and str(item.get("name") or "") in {
            "competitive_or_configuration_data_unavailable",
            "configuration_delta",
            "current_msrp",
            "coverage_diagnostic:no_current_prices_for_requested_models",
        }
        for item in missing
    )


def _leasing_tco_record_needs_evidence_gap(
    *,
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any] | None,
) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    question = _text(record.get("question"))
    if not _question_requires_leasing_tco_evidence(question):
        return False
    if _evidence_package_has_leasing_tco_ref(evidence_package):
        return False
    missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    return not any(
        isinstance(item, dict)
        and str(item.get("name") or "") == "leasing_tco_or_company_car_evidence"
        for item in missing
    )


def _question_requires_leasing_tco_evidence(question: str) -> bool:
    text = _text(question).casefold()
    return any(
        token in text
        for token in (
            "benefit",
            "company car",
            "fleet",
            "leasing",
            "residual",
            "rv",
            "tco",
            "大客户",
            "公司车",
            "月供",
            "残值",
        )
    )


def _evidence_package_has_leasing_tco_ref(evidence_package: dict[str, Any]) -> bool:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            continue
        refs = tool_result.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            haystack = " ".join(
                _text(value).casefold()
                for value in (
                    ref.get("label"),
                    ref.get("value"),
                    ref.get("source"),
                    ref.get("table"),
                    ref.get("unit"),
                    tool_result.get("toolName"),
                    tool_result.get("sourceType"),
                )
            )
            strong_cost_tokens = (
                "benefit",
                "bilförmån",
                "förmån",
                "forman",
                "monthly payment",
                "monthly cost",
                "monthly lease",
                "residual",
                "residual value",
                "skatteverket",
                "tax benefit",
                "tax value",
                "tco",
                "total cost",
                "tjänstebil",
                "大客户成本",
                "公司车税",
                "总拥有成本",
                "月供",
                "残值",
                "税费",
            )
            if any(token in haystack for token in strong_cost_tokens) or re.search(r"\brv\b", haystack):
                return True
            if any(token in haystack for token in ("leasing", "lease", "fleet", "company car", "大客户", "公司车")) and any(
                token in haystack
                for token in (
                    "cost",
                    "payment",
                    "rate",
                    "tax",
                    "benefit",
                    "residual",
                    "tco",
                    "月供",
                    "残值",
                    "税",
                    "成本",
                )
            ):
                return True
    return False


def _specific_policy_record_needs_source_gap(
    *,
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any] | None,
) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    category = _text(record.get("category"))
    expected_intent = _text(record.get("expectedIntent"))
    intent = _text(evidence_package.get("intent"))
    if category != "policy_news" and expected_intent != "news_policy_search" and intent != "news_policy_search":
        return False
    policy_terms = _specific_policy_terms_from_question(_text(record.get("question")))
    if not policy_terms:
        return False
    if _evidence_package_mentions_all_terms(evidence_package, policy_terms):
        return False
    missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    return not any(
        isinstance(item, dict)
        and str(item.get("name") or "") == "specific_policy_source_evidence"
        for item in missing
    )


def _specific_policy_terms_from_question(question: str) -> list[str]:
    text = _text(question).casefold()
    if "elbilspremien" in text:
        return ["elbilspremien"]
    return []


def _evidence_package_mentions_all_terms(evidence_package: dict[str, Any], terms: list[str]) -> bool:
    if not terms:
        return True
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    terms_normalized = [_side_by_side_model_key(term) for term in terms if _side_by_side_model_key(term)]
    if not terms_normalized:
        return False
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            continue
        refs = tool_result.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            haystack = _side_by_side_model_key(
                " ".join(
                    _text(value)
                    for value in (
                        ref.get("label"),
                        ref.get("value"),
                        ref.get("source"),
                        ref.get("table"),
                    )
                )
            )
            if haystack and all(term in haystack for term in terms_normalized):
                return True
    return False


def _inventory_record_needs_entity_gap(
    *,
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any] | None,
) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    category = _text(record.get("category"))
    expected_intent = _text(record.get("expectedIntent"))
    intent = _text(evidence_package.get("intent"))
    if category != "inventory_bom" and expected_intent != "inventory_analysis" and intent != "inventory_analysis":
        return False
    digest = _side_by_side_evidence_digest_from_package(
        evidence_package,
        question=_text(record.get("question")),
    )
    if digest:
        return False
    missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    return not any(
        isinstance(item, dict)
        and str(item.get("name") or "") in {"bom_entity_mapping_evidence", "inventory_bom_weak_evidence_refs", "query_with_filters_weak_evidence_refs"}
        for item in missing
    )


def _voc_record_needs_source_gap(
    *,
    record: dict[str, Any],
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any] | None,
) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    category = _text(record.get("category"))
    expected_intent = _text(record.get("expectedIntent"))
    intent = _text(evidence_package.get("intent"))
    if "voc" not in {category, expected_intent, intent} and intent != "voc_analysis":
        return False
    digest = _side_by_side_evidence_digest_from_package(
        evidence_package,
        question=_text(record.get("question")),
    )
    if digest:
        return False
    missing = astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else []
    if any(
        isinstance(item, dict)
        and str(item.get("name") or "") in {"external_research_claims_unavailable", "minimum_external_sources"}
        for item in missing
    ):
        return False
    return True


def _has_citation_ready_external_voc_evidence(
    evidence_package: dict[str, Any] | None,
    *,
    question: str,
) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    for tool in tool_results:
        if not isinstance(tool, dict) or not _side_by_side_is_external_source_tool(tool):
            continue
        refs = tool.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        claim_refs = [
            ref
            for ref in refs
            if isinstance(ref, dict)
            and _side_by_side_is_external_claim_ref(ref)
            and _side_by_side_external_ref_matches_question(ref, question=question)
        ]
        if not claim_refs:
            continue
        if any(_side_by_side_ref_has_non_search_url(ref) for ref in claim_refs):
            return True
        if any(isinstance(ref, dict) and _side_by_side_ref_has_non_search_url(ref) for ref in refs):
            return True
    return False


def _side_by_side_is_external_source_tool(tool: dict[str, Any]) -> bool:
    source_type = _text(tool.get("sourceType")).casefold()
    tool_name = _text(tool.get("toolName"))
    return source_type in {"web", "voc", "policy"} or tool_name in {
        "external_research",
        "search_market_news",
        "read_web_page",
        "browser_snapshot",
        "pageindex_search_documents",
        "minirag_query_graph",
    }


def _side_by_side_is_external_claim_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    value = _text(ref.get("value")).strip()
    if not value or _side_by_side_is_non_search_url(value):
        return False
    if any(token in label for token in ("row_count", "rank", "rankseed", "metadata", "result_count")):
        return False
    if _optional_float(value) is not None:
        return False
    return len(value) >= 28


def _side_by_side_ref_has_non_search_url(ref: dict[str, Any]) -> bool:
    for value in (
        ref.get("value"),
        ref.get("source"),
        ref.get("table"),
        ref.get("sourceUrl"),
        ref.get("url"),
    ):
        if _side_by_side_is_non_search_url(_text(value)):
            return True
    return False


def _side_by_side_is_non_search_url(value: str) -> bool:
    text = _text(value).strip()
    if not text.startswith(("http://", "https://")):
        return False
    parsed = urlparse(text)
    host = parsed.netloc.casefold()
    if not host:
        return False
    if "google." in host and parsed.path.startswith("/search"):
        return False
    return True


def _side_by_side_external_ref_matches_question(ref: dict[str, Any], *, question: str) -> bool:
    terms = _side_by_side_voc_question_terms(question)
    if not terms:
        return True
    haystack = " ".join(
        _text(value).casefold()
        for value in (
            ref.get("label"),
            ref.get("value"),
            ref.get("source"),
            ref.get("table"),
        )
    )
    return any(term in haystack for term in terms)


def _side_by_side_voc_question_terms(question: str) -> list[str]:
    text = _text(question).casefold()
    terms: list[str] = []
    if "v2h" in text or "vehicle-to-home" in text or "vehicle to home" in text:
        terms.extend(["v2h", "vehicle-to-home", "vehicle to home", "bidirectional", "home backup", "energy backup"])
    if "拖车" in text or "tow" in text:
        terms.extend(["拖车", "tow", "towing", "trailer"])
    if "roof" in text or "车顶" in text or "行李架" in text:
        terms.extend(["roof", "roof load", "roof box", "roof boxes", "行李架"])
    if "冬季" in text or "winter" in text or "雪" in text:
        terms.extend(["冬季", "winter", "snow", "ski", "skis", "boots", "tyre", "tire"])
    if "omoda" in text or "jaecoo" in text:
        terms.extend(["omoda", "jaecoo", "owner", "review", "complaint", "forum"])
    if "用户" in text or "车主" in text:
        terms.extend(["owner", "user", "customer", "review", "forum", "用户", "车主"])
    return _dedupe_string_list(terms)


def _voc_question_requires_frequency_evidence(question: str) -> bool:
    text = _text(question).casefold()
    return any(token in text for token in ("高频", "frequency", "frequent", "representative", "代表性"))


def _ensure_missing_evidence_items(
    existing: list[dict[str, Any]],
    additions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    result = [dict(item) for item in existing if isinstance(item, dict)]
    seen = {str(item.get("name") or "") for item in result}
    for item in additions:
        name = str(item.get("name") or "")
        if not name or name in seen:
            continue
        result.append(dict(item))
        seen.add(name)
    return result


def _normalize_astrbot_answer_status_for_missing_evidence(
    astrbot_side: dict[str, Any],
    missing_evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    severity = _missing_evidence_display_severity(missing_evidence)
    if not severity:
        return astrbot_side
    normalized = dict(astrbot_side)
    current_status = _text(normalized.get("answerStatus"))
    if severity == "blocking":
        normalized["answerStatus"] = "insufficient_evidence" if current_status in {"", "answered"} else current_status
        normalized["confidence"] = "low"
        return normalized
    if current_status in {"", "answered"}:
        normalized["answerStatus"] = "partially_answered"
    if _text(normalized.get("confidence")) in {"", "high"}:
        normalized["confidence"] = "medium"
    return normalized


def _normalize_evidence_package_status_for_missing_evidence(
    evidence_package: dict[str, Any],
    missing_evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    severity = _missing_evidence_display_severity(missing_evidence)
    if not severity:
        return evidence_package
    normalized = dict(evidence_package)
    if severity == "blocking":
        normalized["confidence"] = "low"
    elif _text(normalized.get("confidence")) in {"", "high"}:
        normalized["confidence"] = "medium"
    return normalized


def _missing_evidence_display_severity(missing_evidence: list[dict[str, Any]]) -> str:
    impacts = {
        _text(item.get("impact"))
        for item in missing_evidence
        if isinstance(item, dict)
    }
    if "blocking" in impacts:
        return "blocking"
    if "weakens_answer" in impacts:
        return "weakens_answer"
    return ""


def _has_published_date_ref(evidence_package: dict[str, Any]) -> bool:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            continue
        refs = tool_result.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            label = str(ref.get("label") or "").lower()
            value = str(ref.get("value") or "").strip()
            if not value:
                continue
            if (
                "published" in label
                or label.endswith(".date")
                or label.endswith("_date")
                or label in {"date", "source_date"}
            ):
                return True
    return False


def _normalize_legacy_evidence_refs_for_read(evidence_package: dict[str, Any]) -> dict[str, Any]:
    """Restore legacy stringified evidenceRefs into reviewable lists at read time."""
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return evidence_package
    normalized_results: list[Any] = []
    changed = False
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            normalized_results.append(tool_result)
            continue
        refs = _parse_legacy_evidence_refs(tool_result.get("evidenceRefs"))
        if refs is None:
            normalized_results.append(tool_result)
            continue
        normalized_tool_result = dict(tool_result)
        normalized_tool_result["evidenceRefs"] = refs
        normalized_results.append(normalized_tool_result)
        changed = True
    if not changed:
        return evidence_package
    normalized_package = dict(evidence_package)
    normalized_package["toolResults"] = normalized_results
    return normalized_package


def _parse_legacy_evidence_refs(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, list):
        refs = [dict(item) for item in value if isinstance(item, dict)]
        return refs if len(refs) != len(value) else None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text.startswith("["):
        return None
    parsed: Any
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            if "refId" not in text and "label" not in text:
                return None
            return [{
                "refId": "legacy_evidence_refs_truncated",
                "label": "legacy evidence refs preview",
                "value": text[:500],
                "source": "legacy_business_validation_record",
                "retrievedAt": "",
                "legacyTruncated": True,
            }]
    if not isinstance(parsed, list):
        return None
    refs = [dict(item) for item in parsed if isinstance(item, dict)]
    return refs if refs else None


def _normalize_missing_evidence_item(
    item: dict[str, Any],
    *,
    category: str,
    expected_intent: str,
    question: str,
    has_user_target_price: bool = False,
) -> dict[str, Any]:
    normalized = dict(item)
    name = str(normalized.get("name") or "")
    if name == "report_outline":
        normalized["impact"] = "weakens_answer"
    if has_user_target_price and name in {"current_msrp", "own_model_price"}:
        normalized["impact"] = "weakens_answer"
    if (
        name == "configuration_delta"
        and (category == "competitor_compare" or expected_intent == "competitor_compare")
        and not _question_explicitly_requires_configuration_delta(question)
    ):
        normalized["impact"] = "weakens_answer"
    return normalized


def _has_user_supplied_target_price_evidence(evidence_package: dict[str, Any] | None) -> bool:
    if not isinstance(evidence_package, dict):
        return False
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    for tool_result in tool_results:
        if not isinstance(tool_result, dict):
            continue
        if str(tool_result.get("toolName") or "") != "user_supplied_target_price":
            continue
        refs = tool_result.get("evidenceRefs")
        if isinstance(refs, list) and any(isinstance(ref, dict) for ref in refs):
            return True
    return False


def _has_report_outline_output(astrbot_side: dict[str, Any]) -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    if isinstance(artifacts, list) and any(
        isinstance(item, dict) and str(item.get("type") or "") == "report_block"
        for item in artifacts
    ):
        return True
    report_ready_bullets = _string_list(astrbot_side.get("reportReadyBullets"))
    if len(report_ready_bullets) >= 3:
        return True
    synthesis = astrbot_side.get("businessSynthesisPlan")
    if isinstance(synthesis, dict):
        sections = synthesis.get("reportReadyBullets")
        if len(_string_list(sections)) >= 3:
            return True
    return False


def _question_explicitly_requires_configuration_delta(question: str) -> bool:
    text = str(question or "").lower()
    return any(
        token in text
        for token in (
            "配置",
            "版型",
            "规格",
            "feature",
            "features",
            "trim",
            "equipment",
            "battery",
            "电池",
            "续航",
            "冬季包",
            "座椅",
            "hud",
            "adas",
        )
    )


def _current_policy_failure_tags(record: dict[str, Any]) -> list[str]:
    scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
    llm_judge = record.get("llmJudge") if isinstance(record.get("llmJudge"), dict) else {}
    llm_scores = llm_judge.get("scores") if isinstance(llm_judge.get("scores"), dict) else {}
    human_tags = (
        _string_list(scoring.get("failureTags"))
        if str(scoring.get("status") or "") == "scored"
        else []
    )
    llm_tags = (
        _string_list(llm_scores.get("failureTags"))
        if str(llm_judge.get("status") or "") == "ok"
        else []
    )
    valid = set(FAILURE_TAGS)
    return _dedupe_string_list([
        *infer_business_failure_tags(record),
        *(tag for tag in llm_tags if tag in valid),
        *(tag for tag in human_tags if tag in valid),
    ])


def _business_review_answer_text(answer: dict[str, Any], *, question: str = "") -> str:
    """Build the side-by-side text reviewers should judge.

    AstrBot runtime answers keep rich structured fields. Business Validation
    needs a readable projection so reviewers do not see only the short direct
    sentence while bullets, actions and PPT-ready points sit in JSON fields.
    """
    if not isinstance(answer, dict):
        return ""
    if question and not _text(answer.get("question")):
        answer = {**answer, "question": question}
    existing_preview = _text(answer.get("answerPreview"))
    synthesis = answer.get("businessSynthesisPlan") if isinstance(answer.get("businessSynthesisPlan"), dict) else {}
    existing_direct = _business_review_existing_preview_direct(existing_preview)
    direct = _sanitize_business_review_line(
        _text(answer.get("direct") or existing_direct or existing_preview or synthesis.get("executiveConclusion"))
    )
    question_direct = _question_specific_business_review_direct(answer)
    if question_direct and (
        _looks_like_generic_business_review_direct(direct)
        or _should_prefer_question_specific_business_direct(answer, direct)
    ):
        direct = question_direct
    direct = _guard_unsupported_generic_business_review_direct(answer, direct)
    direct = _refresh_stale_business_review_direct(answer, direct)
    preview_bullets = _business_review_existing_preview_bullets(existing_preview)
    evidence_digest = _string_list(answer.get("evidenceDigest"))
    display_plan = _text(answer.get("displayPlan"))
    bullets = _dedupe_string_list([
        *_string_list(answer.get("bullets")),
        *_string_list(answer.get("keyTakeaways")),
        *preview_bullets,
    ])
    implications = _dedupe_string_list([
        *_string_list(answer.get("businessImplications")),
        *_string_list(synthesis.get("businessImplications")),
    ])
    actions = _business_review_action_lines(
        answer.get("recommendedActions")
        if isinstance(answer.get("recommendedActions"), list)
        else synthesis.get("recommendedActions")
    )
    actions = _business_review_actions_for_question(
        actions,
        question=_text(answer.get("question")),
    )
    current_source_repair_action = _business_review_current_source_repair_action(answer)
    if current_source_repair_action:
        actions = _dedupe_string_list([
            current_source_repair_action,
            *[
                line
                for line in actions
                if not _business_review_is_stale_source_repair_action(line)
            ],
        ])
    report_bullets = _dedupe_string_list([
        *_string_list(answer.get("reportReadyBullets")),
        *_string_list(synthesis.get("reportReadyBullets")),
    ])
    limitations = _dedupe_string_list([
        *_string_list(answer.get("limitations")),
        *_business_review_prefixed_lines(bullets, "risk"),
        *_business_review_risk_lines(synthesis.get("risksAndMissingEvidence")),
    ])
    if _has_report_outline_output(answer):
        limitations = [
            item
            for item in limitations
            if not _business_review_is_report_outline_limitation(item)
        ]

    direct_before_pm_cleanup = direct
    direct = _business_review_direct_for_pm(direct)
    direct_evidence_line = _business_review_direct_evidence_line(direct_before_pm_cleanup)
    voc_missing_source = _business_answer_is_voc_with_missing_source(answer)
    inventory_missing_entity = _business_answer_is_inventory_with_missing_entity(answer)
    configuration_missing_matrix = _business_answer_is_configuration_with_missing_matrix(answer)
    report_missing_model_coverage = _business_answer_is_report_with_missing_model_coverage(answer)
    leasing_tco_missing = _business_answer_is_leasing_tco_with_missing_evidence(answer)
    specific_policy_missing = _business_answer_is_specific_policy_with_missing_source(answer)
    report_evidence_bullets = (
        _business_review_prefixed_lines(report_bullets, "evidence")
        if evidence_digest
        else report_bullets
    )
    fallback_evidence_bullets = (
        []
        if evidence_digest
        or voc_missing_source
        or inventory_missing_entity
        or configuration_missing_matrix
        or report_missing_model_coverage
        or leasing_tco_missing
        or specific_policy_missing
        else bullets
    )
    evidence_lines = _filter_redundant_evidence_review_lines(
        [
            *(_business_review_digest_evidence_lines([direct_evidence_line, *evidence_digest], direct)),
            *(_business_review_evidence_lines([*fallback_evidence_bullets, *report_evidence_bullets], direct)),
        ],
        direct,
    )
    if voc_missing_source and not evidence_digest:
        evidence_lines = [
            "当前缺少可追溯 VOC 来源；JATO 市场数据只能作为背景，不能证明用户吐槽频次或高频主题。"
        ]
    if inventory_missing_entity and not evidence_digest:
        evidence_lines = [
            "当前缺少 BOM/库存实体映射证据；市场销量、车型数量或 MSRP 只能作为背景，不能证明物料号、颜色、订单生命周期或客户可编辑数量。"
        ]
    if configuration_missing_matrix and not evidence_digest:
        evidence_lines = [
            "当前缺少可追溯配置矩阵或工程配置证据；销量、竞品名称或市场规模只能作为背景，不能证明电池容量、800V、冬季包或高配价值。"
        ]
    if configuration_missing_matrix and evidence_digest:
        evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
        package_context_lines = [
            _sanitize_business_review_line(_side_by_side_ref_digest_line(ref))
            for ref in _side_by_side_configuration_market_context_refs(evidence_package)[:4]
        ]
        market_context_lines = [
            line
            for line in [*package_context_lines, *evidence_digest]
            if _business_review_is_configuration_market_context_line(line)
        ]
        if market_context_lines:
            non_market_evidence_lines = [
                line
                for line in evidence_lines
                if not _business_review_is_configuration_market_context_line(line)
            ]
            evidence_lines = _dedupe_string_list([
                *market_context_lines[:3],
                *non_market_evidence_lines,
            ])
    if report_missing_model_coverage:
        evidence_lines = _dedupe_string_list([
            "当前缺少多车型对标汇报所需的实体证据覆盖；单个竞品销量或市场背景只能作为线索，不能支撑 O5/EX30/EV3 这类完整对标页结论。",
            *evidence_lines,
        ])
    if leasing_tco_missing:
        evidence_lines = [
            line
            for line in evidence_lines
            if _business_review_is_leasing_tco_context_line(line)
        ]
        review_question = _text(answer.get("question") or question)
        evidence_lines = _dedupe_string_list([
            _business_review_leasing_tco_missing_line(review_question),
            *evidence_lines,
        ])
        company_car_direct = _business_review_company_car_direct_from_evidence(review_question, evidence_lines)
        if company_car_direct:
            direct = company_car_direct
        elif _business_review_is_fleet_leasing_question(review_question):
            leasing_direct = _business_review_leasing_tco_direct_from_evidence(evidence_lines)
            if leasing_direct:
                direct = leasing_direct
            else:
                direct = _business_review_leasing_tco_missing_direct(review_question)
        elif _looks_like_generic_business_review_direct(direct):
            direct = _business_review_leasing_tco_missing_direct(review_question)
    if specific_policy_missing:
        evidence_lines = _dedupe_string_list([
            "当前缺少问题中点名政策的官方或可引用来源；bonus/malus、vehicle tax 或市场背景只能作为交叉验证，不能证明 Elbilspremien 2026 的资格、价格上限或受影响车型。",
            *evidence_lines,
        ])
    configuration_direct = _business_review_configuration_direct_from_evidence(
        _text(answer.get("question") or question),
        evidence_lines,
    )
    if configuration_direct:
        direct = configuration_direct
    j8_sorento_direct = _business_review_j8_sorento_direct_from_evidence(
        _text(answer.get("question") or question),
        evidence_lines,
    )
    if j8_sorento_direct:
        direct = j8_sorento_direct
    judgment_lines = _filter_redundant_review_lines(
        _business_review_judgment_lines(implications, direct),
        direct,
        *evidence_lines,
    )
    if not judgment_lines:
        inline_judgment = _business_review_inline_business_judgment(direct)
        if inline_judgment and not _business_review_is_metric_fragment(inline_judgment):
            judgment_lines = [inline_judgment]
    action_lines = _filter_redundant_review_lines(
        _business_review_action_review_lines(actions),
        direct,
        *evidence_lines,
        *judgment_lines,
    )
    action_lines = _question_specific_business_review_action_lines(answer, action_lines)
    ppt_lines = _filter_redundant_review_lines(
        _business_review_ppt_lines([display_plan, *report_bullets], direct),
        direct,
        *evidence_lines,
        *judgment_lines,
        *action_lines,
    )
    ppt_lines = _question_specific_business_review_ppt_lines(answer, ppt_lines)
    limit_lines = _filter_redundant_review_lines(
        _business_review_limit_lines(limitations),
        direct,
        *evidence_lines,
        *judgment_lines,
        *action_lines,
        *ppt_lines,
    )

    sections: list[str] = []
    if direct:
        sections.append(direct)
    sections.extend(_business_review_section("关键证据", evidence_lines, limit=5))
    sections.extend(_business_review_section("产品经理判断", judgment_lines, limit=5))
    sections.extend(_business_review_section("下一步动作", action_lines, limit=4))
    sections.extend(_business_review_section("汇报口径", ppt_lines, limit=4))
    sections.extend(_business_review_section("证据边界", limit_lines, limit=4))
    text = _sanitize_business_review_text("\n\n".join(section for section in sections if section.strip()).strip())
    text = _align_business_review_text_with_visual_artifacts(text, answer)
    text = _remove_unsupported_voc_evidence_count_claims(text, answer)
    return _rewrite_business_review_text_for_question(text, answer)


def _business_review_is_report_outline_limitation(value: str) -> bool:
    text = _text(value).lower()
    normalized = re.sub(r"[\s_/-]+", "", text)
    return (
        "reportoutline" in normalized
        or "汇报结构" in text
        or "报告结构" in text
        or "ppt结构" in normalized
    )


def _align_business_review_text_with_visual_artifacts(value: str, answer: dict[str, Any]) -> str:
    """Keep stale Business Validation previews aligned with refreshed artifacts."""
    text = str(value or "")
    text = _align_report_pricing_review_text(text, answer)
    stale_ppt_chart_text = "用竞品矩阵展示销量/份额、级别、动力、价格和配置差异；可用柱状图比较核心竞品销量或份额。"
    if (
        "Competitor sales chart" not in text
        and "Competitor share chart" not in text
        and "Competitor price chart" not in text
        and "竞品销量图" not in text
        and "竞品份额图" not in text
        and "竞品价格图" not in text
        and stale_ppt_chart_text not in text
    ):
        return text
    artifacts = answer.get("visualArtifacts")
    if not isinstance(artifacts, list):
        return text
    has_competitor_chart = any(
        isinstance(item, dict)
        and _text(item.get("type")) == "chart"
        and (
            _text(item.get("id")) == "artifact_competitor_evidence_chart"
            or "competitor" in _text(item.get("title")).lower()
        )
        for item in artifacts
    )
    if has_competitor_chart:
        return text
    has_msrp_repair = any(
        isinstance(item, dict) and _text(item.get("id")) == "artifact_msrp_source_repair_table"
        for item in artifacts
    )
    has_competitor_table = any(
        isinstance(item, dict)
        and (
            _text(item.get("id")) in {
                "artifact_competitor_compare_table",
                "artifact_competitor_compare_framework_table",
            }
            or "competitor comparison table" in _text(item.get("title")).lower()
        )
        for item in artifacts
    )
    if has_msrp_repair and has_competitor_table:
        replacement = _rewrite_business_review_display_line(
            "展示骨架：先看 MSRP source validation table 补齐本车型/竞品官方价格来源，"
            "再用 Competitor comparison table 拆级别、动力类型、价格/配置差异和产品动作。"
        )
        ppt_replacement = (
            "用 MSRP 来源验证表列出本车型/竞品补源状态，再用竞品对比矩阵展示级别、动力、价格/配置差异；"
            "图表等 requested-model 销量或价格证据补齐后再生成。"
        )
    elif has_competitor_table:
        replacement = _rewrite_business_review_display_line("展示骨架：用 Competitor comparison table 拆级别、动力类型、价格/配置差异和产品动作。")
        ppt_replacement = (
            "用 Competitor comparison table 展示竞品角色、级别、动力、价格/配置差异和产品动作；"
            "图表等 requested-model 证据补齐后再生成。"
        )
    else:
        replacement = _rewrite_business_review_display_line("展示骨架：当前没有可渲染的竞品 chart，先补齐竞品证据后再生成图表。")
        ppt_replacement = "当前没有可渲染的竞品 chart，先补齐竞品证据后再生成图表。"
    text = re.sub(
        r"(?:展示骨架：先看 Competitor (?:sales|share|price) chart 判断竞品量级，"
        r"再用 Competitor comparison table 拆级别、动力类型、价格/配置差异和产品动作。"
        r"|输出视图：已生成 竞品(?:销量|份额|价格)图[^。]*。)",
        replacement,
        text,
    )
    return text.replace(
        stale_ppt_chart_text,
        ppt_replacement,
    )


def _align_report_pricing_review_text(value: str, answer: dict[str, Any]) -> str:
    text = str(value or "")
    artifacts = answer.get("visualArtifacts")
    if not isinstance(artifacts, list):
        return text
    artifact_ids = {
        _text(item.get("id"))
        for item in artifacts
        if isinstance(item, dict)
    }
    if "artifact_pricing_corridor_chart" not in artifact_ids or "artifact_report_pricing_table" not in artifact_ids:
        return text
    user_material_gap = _pricing_answer_uses_user_material_with_current_price_gap(answer)
    if "价格走廊图" in text and "价格证据表" in text and (not user_material_gap or "用户材料" in text):
        return text
    replacement = _report_pricing_display_replacement(user_material_gap=user_material_gap)
    generic_lines = (
        "用 report block 输出一页 PPT-ready 结构，并附证据表或图表作为 appendix。",
        "用 report block 输出一页 PPT-ready 结构，并附证据表或图表。",
        "用 report block 输出可复制的一页 PPT 结构，并附证据表或图表作为 appendix。",
        "用 report block 输出可复制的一页 PPT 结构，并附证据表或图表。",
        "用 PPT-ready block 输出可复制的一页 PPT 结构，并附证据表或图表作为 appendix。",
    )
    for generic in generic_lines:
        if generic in text:
            return text.replace(generic, replacement)
    if "## 汇报口径" in text:
        return text.replace("## 汇报口径", f"## 汇报口径\n- {replacement}", 1)
    return f"{text}\n\n## 汇报口径\n- {replacement}".strip()


def _report_pricing_display_replacement(*, user_material_gap: bool) -> str:
    if user_material_gap:
        return (
            "已生成 PPT-ready 汇报块、价格走廊图和价格证据表；"
            "价格图用于核对用户材料价格锚点、PVA 和样本价格背景，不能当作当前官方 MSRP 结论；"
            "当前官方 MSRP、竞品官方价格、月供/RV 仍需通过 MSRP source validation 补齐。"
        )
    return (
        "已生成 PPT-ready 汇报块、价格走廊图和价格证据表；"
        "先用汇报块复制一页结论，再用图表核对价格位置、价差、PVA 和来源边界。"
    )


def _pricing_answer_uses_user_material_with_current_price_gap(answer: dict[str, Any]) -> bool:
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    return _pricing_package_uses_user_material_with_current_price_gap(evidence_package)


def _remove_unsupported_voc_evidence_count_claims(value: str, answer: dict[str, Any]) -> str:
    if not _business_answer_is_voc_with_missing_source(answer):
        return value
    text = re.sub(r"（\s*\d+\s*条可引用证据\s*）", "", value)
    text = re.sub(r"，?\s*当前有\s*\d+\s*条可引用证据[^。；\n]*[。；]?", "。", text)
    return re.sub(r"。{2,}", "。", text).strip()


def _business_answer_is_voc_with_missing_source(answer: dict[str, Any]) -> bool:
    if not _business_answer_has_missing_evidence(answer, "external_research_claims_unavailable"):
        return False
    intent = _text(answer.get("expectedIntent") or answer.get("category"))
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    intent = _text(evidence_package.get("intent")) or intent
    return intent == "voc_analysis" or "voc" in intent


def _business_answer_is_inventory_with_missing_entity(answer: dict[str, Any]) -> bool:
    if not _business_answer_has_missing_evidence(answer, "bom_entity_mapping_evidence"):
        return False
    intent = _text(answer.get("expectedIntent") or answer.get("category"))
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    intent = _text(evidence_package.get("intent")) or intent
    return intent == "inventory_analysis" or "inventory" in intent or "bom" in intent


def _business_answer_is_configuration_with_missing_matrix(answer: dict[str, Any]) -> bool:
    if not _business_answer_has_missing_evidence(answer, "competitive_or_configuration_data_unavailable"):
        return False
    intent = _text(answer.get("expectedIntent") or answer.get("category"))
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    intent = _text(evidence_package.get("intent")) or intent
    return intent == "configuration_analysis" or "configuration" in intent


def _business_answer_is_report_with_missing_model_coverage(answer: dict[str, Any]) -> bool:
    if not _business_answer_has_missing_evidence(answer, "competitive_or_configuration_data_unavailable"):
        return False
    intent = _text(answer.get("expectedIntent") or answer.get("category"))
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    intent = _text(evidence_package.get("intent")) or intent
    return intent == "report_generation"


def _business_answer_is_leasing_tco_with_missing_evidence(answer: dict[str, Any]) -> bool:
    return _business_answer_has_missing_evidence(answer, "leasing_tco_or_company_car_evidence")


def _business_answer_is_specific_policy_with_missing_source(answer: dict[str, Any]) -> bool:
    return _business_answer_has_missing_evidence(answer, "specific_policy_source_evidence")


def _business_answer_has_missing_evidence(answer: dict[str, Any], name: str) -> bool:
    missing = answer.get("missingEvidence") if isinstance(answer.get("missingEvidence"), list) else []
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    package_missing = (
        evidence_package.get("missingEvidence")
        if isinstance(evidence_package.get("missingEvidence"), list)
        else []
    )
    return any(
        isinstance(item, dict) and str(item.get("name") or "") == name
        for item in [*missing, *package_missing]
    )


def _should_refresh_side_by_side_evidence_digest(
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any],
    *,
    question: str,
) -> bool:
    if not _string_list(astrbot_side.get("evidenceDigest")):
        return True
    if _side_by_side_gap_digest_lines(
        evidence_package,
        intent=_text(evidence_package.get("intent")),
        question=question,
    ):
        return True
    return _text(evidence_package.get("intent")) in {
        "configuration_analysis",
        "competitor_compare",
        "inventory_analysis",
        "market_overview",
        "news_policy_search",
        "pricing_analysis",
        "report_generation",
        "voc_analysis",
    }


def _side_by_side_evidence_digest_from_package(
    evidence_package: dict[str, Any],
    *,
    limit: int = 4,
    question: str = "",
) -> list[str]:
    if not isinstance(evidence_package, dict):
        return []
    intent = _text(evidence_package.get("intent"))
    gap_lines = _side_by_side_gap_digest_lines(
        evidence_package,
        intent=intent,
        question=question,
    )
    configuration_market_refs = (
        _side_by_side_configuration_market_context_refs(evidence_package)
        if intent == "configuration_analysis"
        else []
    )
    configuration_market_line_limit = (
        3
        if any(_text(ref.get("label")).startswith("crossCountry.") for ref in configuration_market_refs)
        else 2
    )
    configuration_market_lines = [
        _side_by_side_ref_digest_line(ref)
        for ref in configuration_market_refs[:configuration_market_line_limit]
    ]
    configuration_market_lines = [line for line in configuration_market_lines if line]
    refs: list[dict[str, Any]] = []
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        if configuration_market_lines:
            return _dedupe_string_list([*configuration_market_lines, *gap_lines])[:limit]
        return _side_by_side_append_attempted_tools(gap_lines, evidence_package)[:limit]
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        evidence_refs = tool.get("evidenceRefs")
        if not isinstance(evidence_refs, list):
            continue
        for ref in evidence_refs:
            if isinstance(ref, dict) and _side_by_side_is_business_ref(ref, intent=intent, tool=tool, question=question):
                refs.append(ref)
    if not refs:
        if configuration_market_lines:
            return _dedupe_string_list([*configuration_market_lines, *gap_lines])[:limit]
        return _side_by_side_append_attempted_tools(gap_lines, evidence_package)[:limit]
    if intent in {"competitor_compare", "report_generation"}:
        refs = _side_by_side_competitor_digest_refs(
            evidence_package,
            refs,
            question=question,
        )
        if not refs:
            return _side_by_side_append_attempted_tools(gap_lines, evidence_package)[:limit]
    if intent == "pricing_analysis" or (
        intent == "report_generation"
        and _side_by_side_has_user_material_pricing_refs(evidence_package)
    ):
        refs = _side_by_side_pricing_digest_refs(evidence_package, refs, question=question)
        if not refs:
            requested = _side_by_side_requested_entity_names(evidence_package, question=question)
            if _side_by_side_pricing_digest_is_tco_only(evidence_package, question=question, requested=requested):
                return _dedupe_string_list(gap_lines)[:limit]
            return _side_by_side_append_attempted_tools(gap_lines, evidence_package)[:limit]
        refs.sort(key=_side_by_side_pricing_ref_priority, reverse=True)
    elif intent == "news_policy_search":
        refs = _side_by_side_policy_digest_refs(
            evidence_package,
            refs,
            question=question,
        )
        if not refs:
            return _side_by_side_append_attempted_tools(gap_lines, evidence_package)[:limit]
        refs.sort(key=_side_by_side_ref_priority, reverse=True)
    elif intent == "market_overview":
        refs = _side_by_side_market_digest_refs(refs, question=question)
        if not refs:
            return _side_by_side_append_attempted_tools(gap_lines, evidence_package)[:limit]
        refs.sort(key=lambda ref: _side_by_side_market_ref_priority(ref, question=question), reverse=True)
    elif intent == "competitor_compare":
        # Keep the competitor-specific ordering above. Generic ref priority would
        # push 4WD/SUV context behind broader PHEV refs for J8/Sorento-style
        # positioning questions.
        pass
    else:
        refs.sort(key=_side_by_side_ref_priority, reverse=True)
    competitor_context_first = (
        intent == "competitor_compare"
        and bool(_side_by_side_competitor_market_context_refs(refs, question=question))
    )
    competitor_metric_first = (
        intent == "competitor_compare"
        and bool(gap_lines)
        and any(_side_by_side_is_competitor_metric_ref(ref) for ref in refs)
    )
    pricing_material_first = (
        intent in {"pricing_analysis", "report_generation"}
        and bool(gap_lines)
        and _side_by_side_has_user_material_pricing_refs(evidence_package)
    )
    has_requested_price_gap = (
        intent == "pricing_analysis"
        and _side_by_side_pricing_requested_model_price_gap(evidence_package)
    )
    has_leasing_tco_gap = _side_by_side_has_missing_evidence(
        evidence_package,
        "leasing_tco_or_company_car_evidence",
    )
    leasing_primary_gap_lines: list[str] = []
    leasing_trailing_gap_lines: list[str] = []
    if has_leasing_tco_gap:
        for line in gap_lines:
            if line.startswith("leasing/TCO/company-car 证据"):
                leasing_primary_gap_lines.append(line)
            else:
                leasing_trailing_gap_lines.append(line)
    lines: list[str] = (
        leasing_primary_gap_lines
        if has_leasing_tco_gap
        else []
        if (competitor_context_first or competitor_metric_first or pricing_material_first)
        else list(gap_lines)
    )
    if intent == "configuration_analysis" and configuration_market_lines:
        lines = [*configuration_market_lines, *gap_lines]
    output_limit = 6 if (competitor_context_first or has_leasing_tco_gap) else limit
    ref_limit = (
        max(1, output_limit - len(leasing_trailing_gap_lines))
        if has_leasing_tco_gap
        else output_limit
        if competitor_context_first
        else max(1, output_limit - len(gap_lines))
        if (competitor_metric_first or pricing_material_first)
        else output_limit
    )
    for ref in refs:
        label = _side_by_side_ref_label(ref)
        if (has_requested_price_gap or has_leasing_tco_gap) and _text(ref.get("label")).startswith("priceStats."):
            label = label.replace("价格样本", "背景价格样本", 1)
        value = _side_by_side_ref_value(ref)
        source = _side_by_side_ref_source_label(ref)
        line = f"{label} = {value}" if value else label
        if source and len(source) <= 42:
            line = f"{line}（{source}）"
        lines.append(line)
        if len(_dedupe_string_list(lines)) >= ref_limit:
            break
    if competitor_context_first and gap_lines:
        lines.extend(gap_lines)
    if competitor_metric_first and gap_lines:
        lines.extend(gap_lines)
    if pricing_material_first and gap_lines:
        lines.extend(gap_lines)
    if has_leasing_tco_gap and leasing_trailing_gap_lines:
        lines.extend(leasing_trailing_gap_lines)
    if gap_lines:
        return _side_by_side_append_attempted_tools(lines, evidence_package)[:output_limit]
    return _dedupe_string_list(lines)[:output_limit]


def _side_by_side_ref_digest_line(ref: dict[str, Any]) -> str:
    label = _side_by_side_ref_label(ref)
    value = _side_by_side_ref_value(ref)
    source = _side_by_side_ref_source_label(ref)
    line = f"{label} = {value}" if value else label
    if source and len(source) <= 42:
        line = f"{line}（{source}）"
    return line


def _side_by_side_configuration_market_context_refs(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return []
    refs: list[dict[str, Any]] = []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        evidence_refs = tool.get("evidenceRefs")
        if not isinstance(evidence_refs, list):
            continue
        for ref in evidence_refs:
            if not isinstance(ref, dict):
                continue
            if _side_by_side_configuration_market_context_ref_priority(ref)[0] > 0:
                refs.append(ref)
    return _side_by_side_dedupe_refs(
        sorted(refs, key=_side_by_side_configuration_market_context_ref_priority, reverse=True)
    )


def _side_by_side_configuration_market_context_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = _text(ref.get("label"))
    normalized = label.casefold()
    cross_country = re.match(
        r"crosscountry\.([^.]+)\.powertrainmix\.([^.]+)\.(?:sales|value)$",
        normalized,
        flags=re.IGNORECASE,
    )
    if cross_country:
        country_rank = {
            "sweden": 4,
            "finland": 3,
            "norway": 2,
            "denmark": 1,
        }.get(cross_country.group(1).casefold(), 0)
        powertrain_rank = {
            "bev": 90,
            "phev": 80,
            "hev": 70,
            "mhev": 60,
            "ice": 50,
        }.get(cross_country.group(2).casefold(), 40)
        return powertrain_rank + country_rank, label
    order = [
        ("drivebysegment.suv a.4wd_pct", 110),
        ("segmentbyfuel.suv a.bev_pct", 108),
        ("segmentbyfuel.suv a.phev_pct", 106),
        ("drivebysegment.suv a0.2wd_pct", 104),
        ("drivebysegment.suv a0.4wd_pct", 102),
        ("segmentbyfuel.suv a0.bev_pct", 100),
        ("registrationbyfuel.bev.business_pct", 98),
        ("registrationbyfuel.phev.business_pct", 96),
        ("drivebysegment.suv a0.sales", 94),
        ("drivebysegment.suv a.sales", 92),
        ("powertrainmix.bev.sales", 90),
        ("powertrainmix.phev.sales", 88),
        ("powertrainmix.hev.sales", 86),
        ("powertrainmix.mhev.sales", 84),
        ("powertrainmix.ice.sales", 82),
    ]
    for token, score in order:
        if token in normalized:
            return score, label
    if "contextsnapshot.powertrainmix" in normalized and normalized.endswith((".sales", ".value", ".share")):
        return 60, label
    if "contextsnapshot.crosstabs" not in normalized:
        return 0, label
    if not any(token in normalized for token in ("drivebysegment", "segmentbyfuel", "registrationbyfuel")):
        return 0, label
    if normalized.endswith(".sales"):
        return 60, label
    if normalized.endswith(("_pct", ".share")):
        return 50, label
    return 0, label


def _side_by_side_market_digest_refs(refs: list[dict[str, Any]], *, question: str) -> list[dict[str, Any]]:
    structural = [ref for ref in refs if _side_by_side_is_market_structural_ref(ref)]
    cross_country_sales = [
        ref
        for ref in refs
        if _side_by_side_is_cross_country_market_question(question)
        and _side_by_side_is_cross_country_sales_ref(ref)
    ]
    if structural or cross_country_sales:
        return _dedupe_refs_by_ref_id(cross_country_sales + structural)
    focused = [ref for ref in refs if not _side_by_side_is_generic_market_ref(ref)]
    return focused or refs


def _side_by_side_market_ref_priority(ref: dict[str, Any], *, question: str) -> tuple[int, str]:
    base_priority, label = _side_by_side_ref_priority(ref)
    normalized = _text(ref.get("label")).casefold()
    priority = base_priority
    requested_powertrains = _side_by_side_requested_powertrains(question)
    if _side_by_side_is_j7_hev_market_fit_question(question):
        if normalized.startswith("j7 hev user material"):
            method_order = {
                "j7 hev user material market window": 150,
                "j7 hev user material competitor pool": 148,
                "j7 hev user material positioning": 146,
                "j7 hev user material competitor corridor": 144,
            }
            return method_order.get(normalized, 104), label
        if normalized.startswith("j7 hev visible feature"):
            return 72, label
        if _side_by_side_label_has_powertrain(normalized, "HEV"):
            priority += 120
        elif _side_by_side_label_has_powertrain(normalized, "BEV"):
            priority += 50
        elif _side_by_side_label_has_powertrain(normalized, "PHEV"):
            priority += 30
        elif _side_by_side_label_has_powertrain(normalized, "MHEV"):
            priority += 18
        elif _side_by_side_label_has_powertrain(normalized, "ICE"):
            priority += 10
        elif _side_by_side_label_has_powertrain(normalized, "REEV"):
            priority -= 12
        if "contextsnapshot.crosstabs.drivebyfuel.hev" in normalized:
            priority += 35
        if "contextsnapshot.powertrainmix.hev" in normalized or "marketsnapshot.powertrainmix.hev" in normalized:
            priority += 30
        if "segmentbyfuel.suv a" in normalized and _side_by_side_label_has_powertrain(normalized, "HEV"):
            priority += 28
        if "drivebysegment.suv a0.sales" in normalized or "drivebysegment.suv a.sales" in normalized:
            priority += 24
    elif requested_powertrains:
        matched_requested = False
        for fuel in requested_powertrains:
            if _side_by_side_label_has_powertrain(normalized, fuel):
                priority += 120
                matched_requested = True
        if not matched_requested and any(
            _side_by_side_label_has_powertrain(normalized, fuel)
            for fuel in ("BEV", "PHEV", "HEV", "MHEV", "ICE", "REEV")
        ):
            priority -= 24
    return priority, label


def _side_by_side_requested_powertrains(question: str) -> set[str]:
    text = _text(question).casefold()
    fuels = {
        fuel
        for fuel in ("BEV", "PHEV", "HEV", "MHEV", "ICE", "REEV")
        if _side_by_side_label_has_powertrain(text, fuel)
    }
    aliases = {
        "纯电": "BEV",
        "电动车": "BEV",
        "插混": "PHEV",
        "混动": "HEV",
        "油车": "ICE",
        "燃油": "ICE",
    }
    for token, fuel in aliases.items():
        if token in text:
            fuels.add(fuel)
    return fuels


def _side_by_side_is_j7_hev_market_fit_question(question: str) -> bool:
    text = _text(question).casefold()
    return (
        "j7" in text
        and _side_by_side_label_has_powertrain(text, "HEV")
        and any(token in text for token in ("适合", "机会", "为什么", "worth", "fit", "opportunity"))
    )


def _side_by_side_label_has_powertrain(value: str, fuel: str) -> bool:
    token = str(fuel or "").strip().casefold()
    if not token:
        return False
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", _text(value).casefold()))


def _side_by_side_is_market_structural_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    source = _text(ref.get("source") or ref.get("table")).casefold()
    if _side_by_side_is_generic_market_ref(ref):
        return False
    haystack = f"{label} {source}"
    return any(
        token in haystack
        for token in (
            "crosscountry.",
            "contextsnapshot.crosstabs",
            "contextsnapshot.powertrainmix",
            "contextsnapshot.topmodels",
            "marketsnapshot.powertrainmix",
            "powertrainmix",
            "drivebysegment",
            "drivebyfuel",
            "segmentbyfuel",
            "registrationbyfuel",
            "registrationbysegment",
            "topmodels.",
            "j7 hev user material positioning",
            "j7 hev user material competitor corridor",
            "j7 hev user material competitor pool",
            "j7 hev user material market window",
            "bev",
            "phev",
            "hev",
            "mhev",
            "ice",
            "suv",
            "segment",
        )
    )


def _side_by_side_is_generic_market_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    generic_tokens = (
        "avgmsrp",
        "totalrows",
        "countrycount",
        "brandcount",
        "modelcount",
        "versioncount",
        "yearseries",
        "metadata.",
        "result_count",
    )
    if any(token in label for token in generic_tokens):
        return True
    if "cumulativesales" in label:
        return not label.startswith("crosscountry.")
    return False


def _side_by_side_is_cross_country_market_question(question: str) -> bool:
    text = _text(question).casefold()
    return any(token in text for token in ("cross-country", "cross country", "finland", "芬兰", "挪威", "丹麦", "北欧", "对比", "差异"))


def _side_by_side_is_cross_country_sales_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    return label.startswith("crosscountry.") and label.endswith(".kpis.cumulativesales")


def _dedupe_refs_by_ref_id(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        key = _text(ref.get("refId") or ref.get("label") or id(ref))
        if key in seen:
            continue
        seen.add(key)
        result.append(ref)
    return result


def _side_by_side_gap_digest_lines(
    evidence_package: dict[str, Any],
    *,
    intent: str,
    question: str,
) -> list[str]:
    if not isinstance(evidence_package, dict):
        return []
    missing_names = _side_by_side_missing_evidence_names(evidence_package)
    lines: list[str] = []
    if missing_names & {
        "coverage_diagnostic:no_current_prices_for_requested_models",
        "current_msrp",
        "current_official_msrp_cross_check",
        "own_model_price",
    }:
        if _side_by_side_has_user_material_pricing_refs(evidence_package):
            lines.append("当前官方 MSRP 交叉验证 = 待补本车型/竞品当前价格记录")
        else:
            lines.append("本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证")
        lines.extend(_side_by_side_msrp_source_progress_digest_lines(evidence_package))
    if "leasing_tco_or_company_car_evidence" in missing_names:
        lines.append("leasing/TCO/company-car 证据 = 待补月供、残值、税务 benefit 或大客户口径")
    if "specific_policy_source_evidence" in missing_names:
        policy_name = _side_by_side_named_policy_from_question(question)
        if policy_name:
            lines.append(f"点名政策来源 = 待补 {policy_name} 官方或可引用来源")
        else:
            lines.append("点名政策来源 = 待补官方或可引用政策来源")
    if missing_names & {
        "bom_entity_mapping_evidence",
        "inventory_bom_weak_evidence_refs",
        "query_with_filters_weak_evidence_refs",
    }:
        lines.append("BOM 实体证据 = 待补车型版本、物料号、颜色、订单生命周期映射")
        lines.append("缺口 = 当前市场销量/MSRP 不能证明 BOM 建模、生命周期或可编辑数量")
    if intent == "configuration_analysis" and "competitive_or_configuration_data_unavailable" in missing_names:
        lines.append(f"配置验证项 = {_side_by_side_configuration_topic_from_question(question)}")
        lines.append("证据状态 = 待补竞品配置/价格证据")
        lines.append("缺口 = 当前缺少可追溯配置矩阵或工程配置证据")
    if "coverage_diagnostic:no_config_projects_for_country" in missing_names:
        lines.append("工程配置覆盖 = 当前国家未导入或未激活工程配置项目")
        lines.append("缺口 = compare_vehicle_variants 不能生成车型/版本配置差异矩阵")
    if "coverage_diagnostic:no_config_subjects_for_requested_models" in missing_names:
        lines.append("工程配置映射 = 请求车型未映射到工程配置 project / base variant / market variant")
        lines.append("缺口 = 已有配置项目仍无法解析到可比较车型主体")
    if missing_names & {"external_research_claims_unavailable", "minimum_external_sources"}:
        if intent == "voc_analysis":
            lines.append("VOC 来源状态 = 待补可追溯媒体/论坛/用户原声")
            lines.append("缺口 = 当前市场数据不能证明用户吐槽频次或高频主题")
        elif intent == "configuration_analysis":
            lines.append("外部配置来源状态 = 待补可追溯冬季包/配置来源")
            lines.append("缺口 = 当前市场数据不能证明冬季包标准或用户配置需求")
        else:
            lines.append("外部来源状态 = 待补可追溯来源")
            lines.append("缺口 = 当前内部市场数据不能替代外部来源证据")
    if intent == "report_generation" and "competitive_or_configuration_data_unavailable" in missing_names:
        requested = _side_by_side_unique_model_names(
            _side_by_side_requested_entity_names(evidence_package, question=question)
        )
        if requested:
            lines.append(f"竞品汇报覆盖 = {' / '.join(requested[:4])} 待补完整 MSRP、配置/电池/续航和来源日期")
        else:
            lines.append("竞品汇报覆盖 = 待补完整 MSRP、配置/电池/续航和来源日期")
        lines.append("缺口 = 单个竞品销量或市场背景不能支撑完整对标页结论")
    return _dedupe_string_list(lines)


def _side_by_side_msrp_source_progress_digest_lines(evidence_package: dict[str, Any]) -> list[str]:
    candidates = evidence_package.get("sourceRepairCandidates") if isinstance(evidence_package.get("sourceRepairCandidates"), dict) else {}
    if not candidates:
        return []
    rows = [
        entry
        for key in ("ownModel", "competitorCorridor")
        for entry in (candidates.get(key) if isinstance(candidates.get(key), list) else [])
        if isinstance(entry, dict)
    ]
    if not rows:
        return []
    source_drafts: list[str] = []
    materialized: list[str] = []
    search_candidates: list[str] = []
    review_pending: list[str] = []
    review_pending_count = 0
    for entry in rows:
        label = _side_by_side_source_candidate_label(entry)
        if not label:
            continue
        try:
            current_price_rows = int(entry.get("currentPriceRows") or 0)
        except (TypeError, ValueError):
            current_price_rows = 0
        pending_rows = _source_candidate_review_pending_rows(entry)
        candidate_type = _text(entry.get("candidateSourceType"))
        draft_status = _text(entry.get("draftStatus"))
        if current_price_rows > 0 or draft_status == "current_price_materialized":
            materialized.append(label)
        elif pending_rows > 0:
            review_pending.append(label)
            review_pending_count += pending_rows
        elif candidate_type == "source_draft" or "source_draft" in draft_status:
            source_drafts.append(label)
        elif draft_status == "candidate_search_query":
            search_candidates.append(label)
    lines: list[str] = []
    if materialized:
        lines.append(f"已物化价格样本 = {'、'.join(_dedupe_string_list(materialized)[:3])}")
    if review_pending:
        lines.append(
            "已抓到待审核 MSRP 观察 = "
            f"{'、'.join(_dedupe_string_list(review_pending)[:3])}"
            f"（共{review_pending_count}条，未人工确认前不能当正式 current price）"
        )
    if source_drafts:
        lines.append(f"已定位 MSRP 来源草稿 = {'、'.join(_dedupe_string_list(source_drafts)[:3])}（待抽价/审核）")
    if search_candidates and not source_drafts:
        lines.append(f"MSRP 搜索候选 = {'、'.join(_dedupe_string_list(search_candidates)[:3])}（待确认官网来源）")
    return lines


def _side_by_side_source_candidate_label(entry: dict[str, Any]) -> str:
    brand = _text(entry.get("brand"))
    model = _text(entry.get("model"))
    label = " ".join(part for part in (brand, model) if part).strip() or _text(entry.get("sourceCode"))
    return _sanitize_business_review_line(label)


def _side_by_side_policy_digest_refs(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    *,
    question: str,
) -> list[dict[str, Any]]:
    missing_names = _side_by_side_missing_evidence_names(evidence_package)
    has_named_policy_gap = "specific_policy_source_evidence" in missing_names
    is_bev_subsidy_question = _side_by_side_is_bev_subsidy_question(question)
    if not has_named_policy_gap and not is_bev_subsidy_question:
        return refs
    terms = _specific_policy_terms_from_question(question)
    if has_named_policy_gap and "elbilspremien" not in terms and not is_bev_subsidy_question:
        return refs
    filtered: list[dict[str, Any]] = []
    for ref in refs:
        if has_named_policy_gap and terms and _side_by_side_ref_mentions_any_policy_term(ref, terms):
            filtered.append(ref)
            continue
        if is_bev_subsidy_question and not has_named_policy_gap and _side_by_side_is_bev_subsidy_policy_ref(ref):
            filtered.append(ref)
            continue
        if _side_by_side_is_bev_subsidy_context_ref(ref):
            filtered.append(ref)
    return filtered


def _side_by_side_is_bev_subsidy_question(question: str) -> bool:
    text = _text(question).casefold()
    if "elbilspremien" in text:
        return True
    return "bev" in text and any(token in text for token in ("补贴", "价格上限", "price cap", "subsidy"))


def _side_by_side_ref_mentions_any_policy_term(ref: dict[str, Any], terms: list[str]) -> bool:
    haystack = _side_by_side_model_key(
        " ".join(
            _text(value)
            for value in (
                ref.get("label"),
                ref.get("value"),
                ref.get("source"),
                ref.get("table"),
            )
        )
    )
    return bool(haystack) and any(_side_by_side_model_key(term) in haystack for term in terms)


def _side_by_side_is_bev_subsidy_policy_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    value = _text(ref.get("value")).casefold()
    haystack = f"{label} {value}"
    if "rank" in label:
        return False
    if not any(token in haystack for token in ("bonus", "low emission", "subsid", "elbil")):
        return False
    return label.endswith(".date") or ".date" in label


def _side_by_side_is_bev_subsidy_context_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    source = _text(ref.get("source") or ref.get("table")).casefold()
    haystack = f"{label} {source}"
    if "powertrainmix" in label:
        return ".bev." in label
    if "drivebysegment" in label or "segmentbyfuel" in label:
        return "bev" in label
    if "bev" in haystack and any(token in haystack for token in ("private", "sales", "share", "segment", "suv")):
        return True
    return False


def _side_by_side_append_attempted_tools(
    lines: list[str],
    evidence_package: dict[str, Any],
) -> list[str]:
    result = _dedupe_string_list(lines)
    tools = _side_by_side_evidence_tool_labels(evidence_package)
    if tools and result:
        result.append(f"本轮已查数据源 = {'、'.join(tools[:4])}")
    return _dedupe_string_list(result)


def _side_by_side_missing_evidence_names(evidence_package: dict[str, Any]) -> set[str]:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    return {
        _text(item.get("name"))
        for item in missing
        if isinstance(item, dict) and _text(item.get("name"))
    }


def _side_by_side_evidence_refs(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    refs: list[dict[str, Any]] = []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        evidence_refs = tool.get("evidenceRefs")
        if not isinstance(evidence_refs, list):
            continue
        refs.extend(ref for ref in evidence_refs if isinstance(ref, dict))
    return refs


def _side_by_side_evidence_tool_labels(evidence_package: dict[str, Any]) -> list[str]:
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    labels: list[str] = []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        labels.append(_side_by_side_tool_label(tool.get("toolName")))
    return _dedupe_string_list([label for label in labels if label])


def _side_by_side_tool_label(value: Any) -> str:
    name = _text(value)
    mapping = {
        "build_market_chart": "市场图表",
        "compare_competitive_set": "竞品池",
        "compare_vehicle_variants": "配置差异",
        "external_research": "外部研究",
        "minirag_query_graph": "知识库/VOC 检索",
        "pageindex_search_documents": "文档检索",
        "query_country_snapshot": "市场快照",
        "query_cross_country": "跨国对比",
        "query_competitive_landscape": "竞品格局",
        "query_msrp_pricing": "MSRP 价格",
        "query_price_positioning": "价格定位",
        "query_with_filters": "筛选查询",
        "search_market_news": "新闻/政策搜索",
    }
    return mapping.get(name, name)


def _side_by_side_has_missing_evidence(evidence_package: dict[str, Any], name: str) -> bool:
    return name in _side_by_side_missing_evidence_names(evidence_package)


def _side_by_side_named_policy_from_question(question: str) -> str:
    text = _text(question)
    lowered = text.casefold()
    if "elbilspremien" in lowered:
        return "Elbilspremien 2026"
    if "company car" in lowered or "benefit" in lowered or "公司车" in lowered:
        return "company car benefit"
    if "co₂" in lowered or "co2" in lowered or "税率" in lowered:
        return "CO2 税率阶梯"
    if "bev" in lowered and ("补贴" in lowered or "price cap" in lowered or "价格上限" in lowered):
        return "BEV 补贴价格上限"
    return ""


def _side_by_side_configuration_topic_from_question(question: str) -> str:
    text = _text(question).casefold()
    if "80kwh" in text or "80 kwh" in text:
        return "80kWh 长续航/高配安全边界、热泵、电池预热、快充和冬季舒适配置"
    if "95kwh" in text or "95 kwh" in text or "800v" in text or "双电机" in text:
        return "95kWh、双电机、800V、牵引/补能效率和价格带"
    if "冬季包" in text or "winter package" in text:
        return "热泵、电池预热、座椅/方向盘加热、冬季胎/TPMS 和真实冬季续航"
    if any(token in text for token in ("配置", "configuration", "feature", "trim")):
        return "must-have / visible value / optional 配置验证项"
    return "配置差异、用户价值、竞品矩阵和版本定位"


def _side_by_side_pricing_digest_refs(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    *,
    question: str,
) -> list[dict[str, Any]]:
    requested = _side_by_side_requested_entity_names(evidence_package, question=question)
    if _side_by_side_pricing_digest_is_tco_only(evidence_package, question=question, requested=requested):
        return _side_by_side_leasing_tco_digest_refs(refs)
    result: list[dict[str, Any]] = []
    seen_metric_labels: set[str] = set()
    for ref in refs:
        label = _text(ref.get("label")).casefold()
        source = _text(ref.get("source") or ref.get("table")).casefold()
        haystack = f"{label} {source}"
        if "row_count" in label or label.endswith(".count"):
            continue
        if _side_by_side_is_generic_market_pricing_digest_ref(ref):
            continue
        if label.startswith("pricestats.") or label.startswith("user supplied"):
            if label in seen_metric_labels:
                continue
            seen_metric_labels.add(label)
        if (
            requested
            and _side_by_side_is_model_level_pricing_ref(ref)
            and not _side_by_side_ref_mentions_any_model(ref, requested)
        ):
            continue
        if any(
            token in haystack
            for token in (
                "pricestats.",
                "msrp",
                "target price",
                "price gap",
                "price delta",
                "relative price",
                "price corridor",
                "competitor corridor",
                "monthly",
                "leasing",
                "rv",
                "residual",
                "pva",
                "价差",
                "价格",
            )
        ):
            result.append(ref)
    return result


def _side_by_side_is_generic_market_pricing_digest_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    source = _text(ref.get("source") or ref.get("table")).casefold()
    if label.startswith("pricestats."):
        return False
    generic_labels = {
        "avgmsrp",
        "cumulativesales",
        "totalrows",
        "countrycount",
        "brandcount",
        "modelcount",
        "versioncount",
    }
    if label in generic_labels:
        return True
    if label.endswith(".avgmsrp") or label.endswith(".cumulativesales"):
        return True
    return source in {"jato_country_snapshot", "jato_country_chart_deck"} and any(
        token in label
        for token in (
            "avgmsrp",
            "cumulativesales",
            "brandcount",
            "modelcount",
            "versioncount",
            "yearseries",
        )
    )


def _side_by_side_pricing_digest_is_tco_only(
    evidence_package: dict[str, Any],
    *,
    question: str,
    requested: list[str],
) -> bool:
    if requested:
        return False
    if not _side_by_side_has_missing_evidence(evidence_package, "leasing_tco_or_company_car_evidence"):
        return False
    text = _text(question).casefold()
    return any(
        token in text
        for token in ("leasing", "lease", "tco", "company car", "fleet", "大客户", "公司车", "月供", "残值")
    )


def _side_by_side_is_leasing_tco_ref(ref: dict[str, Any]) -> bool:
    haystack = " ".join(
        _text(ref.get(key)).casefold()
        for key in ("label", "value", "source", "table")
    )
    return any(
        token in haystack
        for token in (
            "monthly",
            "lease",
            "leasing",
            "payment",
            "residual",
            "rv",
            "company car",
            "benefit",
            "fleet",
            "business_pct",
            "private_pct",
            "registrationby",
            "月供",
            "残值",
            "公司车",
        )
    )


def _side_by_side_leasing_tco_digest_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    focused = [ref for ref in refs if _side_by_side_is_leasing_tco_ref(ref)]
    if not focused:
        return []
    channel_refs = [
        ref
        for ref in focused
        if _side_by_side_leasing_channel_ref_priority(ref)[0] > 0
    ]
    if channel_refs:
        return sorted(channel_refs, key=_side_by_side_leasing_channel_ref_priority, reverse=True)
    return focused


def _side_by_side_leasing_channel_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = _text(ref.get("label"))
    normalized = label.casefold()
    match = re.search(r"registrationbyfuel\.([^.]+)\.([^.]+)$", normalized)
    if not match:
        return 0, label
    fuel = match.group(1).upper()
    metric = match.group(2).lower()
    fuel_priority = {"PHEV": 100, "BEV": 80, "HEV": 60}.get(fuel, 0)
    metric_priority = {
        "business_pct": 9,
        "sales": 8,
        "private_pct": 7,
        "other_pct": 1,
    }.get(metric, 0)
    return fuel_priority + metric_priority, label


def _side_by_side_pricing_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = _text(ref.get("label"))
    leasing_priority = _side_by_side_leasing_channel_ref_priority(ref)
    if leasing_priority[0] > 0:
        return leasing_priority
    order = {
        "User supplied relative price delta": 100,
        "User supplied own-model target price min": 98,
        "User supplied own-model target price max": 97,
        "User supplied own-model target price midpoint": 96,
        "J7 HEV user material main trim MSRP": 94,
        "J7 HEV user material competitor corridor": 93,
        "J7 HEV user material price gap": 92,
        "J7 HEV user material PVA coverage": 91,
        "priceStats.min": 82,
        "priceStats.max": 81,
        "priceStats.avg": 80,
        "priceStats.median": 79,
        "User supplied price-delta direction": 60,
        "J7 HEV user material positioning": 59,
        "J7 HEV user material competitor pool": 58,
        "J7 HEV user material market window": 57,
    }
    return order.get(label, 0), label


def _side_by_side_has_user_material_pricing_refs(evidence_package: dict[str, Any]) -> bool:
    return any(
        _text(ref.get("label")).startswith("J7 HEV user material")
        for ref in _side_by_side_evidence_refs(evidence_package)
    )


def _side_by_side_is_model_level_pricing_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).casefold()
    if label.startswith("pricestats."):
        return False
    if label.startswith("user supplied"):
        return False
    return bool(
        re.search(r"\bmsrp\b", label)
        or ".msrp" in label
        or " main trim " in label
        or " price gap" in label
        or " competitor corridor" in label
    )


def _side_by_side_pricing_requested_model_price_gap(evidence_package: dict[str, Any]) -> bool:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    names = {
        _text(item.get("name"))
        for item in missing
        if isinstance(item, dict) and _text(item.get("name"))
    }
    return bool(
        names
        & {
            "coverage_diagnostic:no_current_prices_for_requested_models",
            "current_msrp",
            "current_official_msrp_cross_check",
            "own_model_price",
        }
    )


def _side_by_side_competitor_digest_refs(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
    *,
    question: str,
) -> list[dict[str, Any]]:
    requested = _side_by_side_requested_entity_names(evidence_package, question=question)
    market_context_refs = _side_by_side_competitor_market_context_refs(refs, question=question)
    if not requested:
        return market_context_refs or [ref for ref in refs if not _side_by_side_is_competitor_model_only_ref(ref)]
    matched = [
        ref
        for ref in refs
        if _side_by_side_ref_mentions_any_model(ref, requested)
    ]
    if not matched:
        return market_context_refs
    metric_matched = [ref for ref in matched if not _side_by_side_is_competitor_model_only_ref(ref)]
    if market_context_refs:
        return _side_by_side_dedupe_refs([*metric_matched, *market_context_refs])
    return metric_matched


def _side_by_side_is_competitor_metric_ref(ref: dict[str, Any]) -> bool:
    if _side_by_side_is_competitor_model_only_ref(ref):
        return False
    label = _text(ref.get("label")).casefold()
    return any(
        token in label
        for token in (
            ".sales",
            ".share",
            ".msrp",
            ".price",
            ".volume",
            "销量",
            "价格",
        )
    )


def _side_by_side_competitor_market_context_refs(
    refs: list[dict[str, Any]],
    *,
    question: str,
) -> list[dict[str, Any]]:
    question_text = _text(question).casefold()
    needs_context = (
        ("j8" in question_text and "sorento" in question_text)
        or any(token in question_text for token in ("7座", "7 座", "四驱", "4wd", "awd", "能打"))
    )
    if not needs_context:
        return []
    focused = [
        ref
        for ref in refs
        if _side_by_side_competitor_market_context_ref_priority(ref)[0] > 0
    ]
    return sorted(focused, key=_side_by_side_competitor_market_context_ref_priority, reverse=True)


def _side_by_side_competitor_market_context_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = _text(ref.get("label"))
    normalized = label.casefold()
    order = [
        ("drivebysegment.suv a.4wd_pct", 100),
        ("drivebysegment.suv b.4wd_pct", 99),
        ("drivebyfuel.phev.4wd_pct", 98),
        ("drivebysegment.suv a.sales", 96),
        ("segmentbyfuel.suv a.phev_pct", 94),
        ("registrationbyfuel.phev.business_pct", 92),
        ("registrationbyfuel.phev.sales", 90),
        ("registrationbysegment.suv a.business_pct", 88),
        ("segmentbyfuel.suv b.phev_pct", 86),
    ]
    for token, score in order:
        if token in normalized:
            return score, label
    return 0, label


def _side_by_side_dedupe_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        key = "|".join(
            _text(ref.get(part))
            for part in ("refId", "label", "value", "source")
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(ref)
    return result


def _side_by_side_requested_entity_names(evidence_package: dict[str, Any], *, question: str) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    result: list[str] = []
    for key in ("models", "competitors"):
        values = entities.get(key) if isinstance(entities.get(key), list) else []
        for value in values:
            text = _text(value)
            if text:
                result.append(text)
    result.extend(_side_by_side_model_mentions_from_question(question))
    return _dedupe_string_list(result)


def _side_by_side_unique_model_names(values: list[str]) -> list[str]:
    candidates: list[tuple[str, str]] = []
    for value in values:
        name = _text(value)
        key = _side_by_side_model_key(name)
        if not key:
            continue
        candidates.append((key, name))
    result: list[tuple[str, str]] = []
    for key, name in sorted(candidates, key=lambda item: len(item[0]), reverse=True):
        if any(key == existing_key or key in existing_key for existing_key, _ in result):
            continue
        result.append((key, name))
    return [name for _, name in result]


def _side_by_side_report_model_coverage_is_complete(evidence_package: dict[str, Any], requested: list[str]) -> bool:
    refs: list[dict[str, Any]] = []
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return False
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        evidence_refs = tool.get("evidenceRefs")
        if not isinstance(evidence_refs, list):
            continue
        for ref in evidence_refs:
            if isinstance(ref, dict) and _side_by_side_is_business_ref(ref, intent="report_generation", tool=tool):
                refs.append(ref)
    return _side_by_side_refs_cover_requested_models(refs, requested)


def _side_by_side_refs_cover_requested_models(refs: list[dict[str, Any]], requested: list[str]) -> bool:
    if not requested:
        return True
    return all(
        any(_side_by_side_ref_mentions_any_model(ref, [model]) for ref in refs)
        for model in requested
    )


def _side_by_side_model_mentions_from_question(question: str) -> list[str]:
    text = str(question or "")
    patterns = [
        r"\b[A-Z][A-Za-z0-9.-]{1,12}\s+(?:HEV|PHEV|BEV|EV|SUV|Recharge|E-Tech|e-tron)\b",
        r"\b(?:OMODA\s?9|OMODA9|OMODA\s?5|OMODA5|JAECOO\s?J7|JAECOO\s?J8|J8|J7|O9|O5|EX30|EX40|EX60|EX90|XC40|XC60|XC90|RAV4|MODEL Y|Sportage|Sorento|EV3|EV9|Enyaq|ID\.4|ID\.7|Kodiaq|Tayron)\b",
    ]
    candidates: list[str] = []
    for pattern in patterns:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
            value = " ".join(str(match).strip().split())
            if value:
                candidates.append(value)
    return _dedupe_string_list(candidates)


def _side_by_side_ref_mentions_any_model(ref: dict[str, Any], models: list[str]) -> bool:
    haystack = _side_by_side_model_key(
        " ".join(
            _text(ref.get(key))
            for key in ("label", "value", "source", "table")
        )
    )
    if not haystack:
        return False
    return any(
        bool(_side_by_side_model_key(model))
        and _side_by_side_model_key(model) in haystack
        for model in models
    )


def _side_by_side_is_competitor_model_only_ref(ref: dict[str, Any]) -> bool:
    label = _text(ref.get("label")).lower()
    return label.startswith("competitor.") and label.endswith(".model")


def _side_by_side_model_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _side_by_side_is_business_ref(
    ref: dict[str, Any],
    *,
    intent: str = "",
    tool: dict[str, Any] | None = None,
    question: str = "",
) -> bool:
    label = _text(ref.get("label")).lower()
    if not label:
        return False
    weak_tokens = ("row_count", "totalrows", "result_count", "metadata.", "latency", "debug", "raw")
    if any(token in label for token in weak_tokens):
        return False
    if intent == "voc_analysis":
        return _side_by_side_is_voc_ref(ref, tool=tool, question=question)
    if intent == "inventory_analysis":
        return _side_by_side_is_inventory_ref(ref, tool=tool)
    if intent == "configuration_analysis":
        return _side_by_side_is_configuration_ref(ref, tool=tool)
    return True


def _side_by_side_is_configuration_ref(ref: dict[str, Any], *, tool: dict[str, Any] | None = None) -> bool:
    tool = tool if isinstance(tool, dict) else {}
    label = _text(ref.get("label")).casefold()
    value = _text(ref.get("value")).casefold()
    source = _text(ref.get("source") or ref.get("table")).casefold()
    negative_haystack = " ".join([label, value, source, _text(tool.get("toolName")).casefold()])
    weak_market_tokens = (
        "avgmsrp",
        "brandcount",
        "competitor",
        "competitor.",
        "country_snapshot",
        "cumulativesales",
        "drivebysegment",
        "marketshare",
        "modelcount",
        "powertrainmix",
        "sales",
        "segmentby",
        "topmodels",
        "totalrows",
        "versioncount",
        "yearseries",
    )
    if any(token in negative_haystack.replace(".", "") for token in weak_market_tokens):
        return False
    positive_haystack = " ".join([label, value])
    return any(
        token in positive_haystack
        for token in (
            "800v",
            "adas",
            "awd",
            "battery",
            "charging",
            "commonfeatures",
            "differentfeatures",
            "dual motor",
            "equipment",
            "feature",
            "heat pump",
            "hud",
            "kwh",
            "motor",
            "range",
            "roof",
            "seat",
            "thermal",
            "tow",
            "trim",
            "v2h",
            "variant",
            "winter",
            "冬季",
            "座椅",
            "拖车",
            "热泵",
            "电池",
            "续航",
            "配置",
        )
    )


def _side_by_side_is_inventory_ref(ref: dict[str, Any], *, tool: dict[str, Any] | None = None) -> bool:
    tool = tool if isinstance(tool, dict) else {}
    haystack = " ".join(
        _text(value).casefold()
        for value in (
            ref.get("label"),
            ref.get("source"),
            ref.get("table"),
            ref.get("unit"),
            tool.get("toolName"),
            tool.get("sourceType"),
        )
    )
    weak_market_tokens = (
        "topmodels",
        "cumulativesales",
        "avgmsrp",
        "marketshare",
        "powertrainmix",
        "country_snapshot",
        "crosscountry",
    )
    if any(token in haystack.replace(".", "") for token in weak_market_tokens):
        return False
    return any(
        token in haystack
        for token in (
            "inventory.records",
            "bom.",
            "materialcode",
            "material_code",
            "material code",
            "lifecycle",
            "availableunits",
            "editable_quantity",
            "editable quantity",
            "order.",
            "stock.",
            "exterior",
            "interior",
            "colorspec",
            "color spec",
            "物料",
            "生命周期",
            "可编辑数量",
            "内饰",
            "外饰",
        )
    )


def _side_by_side_is_voc_ref(
    ref: dict[str, Any],
    *,
    tool: dict[str, Any] | None = None,
    question: str = "",
) -> bool:
    tool = tool if isinstance(tool, dict) else {}
    haystack = " ".join(
        _text(value).casefold()
        for value in (
            ref.get("label"),
            ref.get("source"),
            ref.get("table"),
            ref.get("unit"),
            tool.get("toolName"),
            tool.get("sourceType"),
        )
    )
    source_type = _text(tool.get("sourceType")).casefold()
    tool_name = _text(tool.get("toolName"))
    if (
        (source_type in {"web", "voc"} or tool_name in {"external_research", "search_market_news", "read_web_page", "browser_snapshot", "pageindex_search_documents"})
        and _side_by_side_is_external_claim_ref(ref)
        and _side_by_side_external_ref_matches_question(ref, question=question)
    ):
        return True
    if not any(
        token in haystack
        for token in (
            "voc",
            "voice of customer",
            "forum",
            "review",
            "complaint",
            "sentiment",
            "owner",
            "media",
            "用户",
            "吐槽",
            "投诉",
            "论坛",
            "口碑",
        )
    ):
        return False
    return source_type in {"web", "voc"} or tool_name in {
        "external_research",
        "search_market_news",
        "read_web_page",
        "browser_snapshot",
        "pageindex_search_documents",
    }


def _side_by_side_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = _text(ref.get("label")).lower()
    priority = 0
    for token in ("msrp", "price", "sales", "share", "volume", "model", "competitor", "bev", "hev", "phev", "segment", "feature", "policy"):
        if token in label:
            priority += 4
    for token in (
        "registrationbyfuel",
        "company car",
        "fleet",
        "benefit",
        "tco",
        "ayvens",
        "car cost index",
    ):
        if token in label:
            priority += 6
    if "business_pct" in label:
        priority += 10
    if "private_pct" in label:
        priority += 6
    value = ref.get("value")
    if isinstance(value, (int, float)) or re.search(r"\d", _text(value)):
        priority += 2
    return priority, label


def _side_by_side_ref_label(ref: dict[str, Any]) -> str:
    label = _text(ref.get("label"))
    context_label = _side_by_side_context_ref_label(label)
    if context_label:
        return context_label
    model_metric_match = re.match(r"^([A-Za-z0-9][A-Za-z0-9 ._/-]{0,60})\.sales$", label)
    if model_metric_match:
        model_name = re.sub(r"\s+", " ", model_metric_match.group(1).replace("_", " ")).strip()
        if model_name:
            return f"{model_name} 销量"
    replacements = {
        "priceStats.min": "价格样本最低值",
        "priceStats.max": "价格样本最高值",
        "priceStats.avg": "价格样本均值",
        "priceStats.median": "价格样本中位数",
        "User supplied own-model target price min": "用户目标价下沿",
        "User supplied own-model target price max": "用户目标价上沿",
        "User supplied own-model target price midpoint": "用户目标价中点",
        "User supplied relative price delta": "用户给定相对价差",
        "User supplied price-delta direction": "用户给定价差方向",
        "J7 HEV user material main trim MSRP": "J7 HEV 主销高配价格",
        "J7 HEV user material competitor corridor": "J7 HEV 竞品价格带",
        "J7 HEV user material price gap": "J7 HEV 高低配价差",
        "J7 HEV user material PVA coverage": "J7 HEV 高配 PVA 覆盖率",
        "J7 HEV user material positioning": "J7 HEV 定价定位",
        "J7 HEV user material competitor pool": "J7 HEV 竞品池",
        "J7 HEV user material market window": "J7 HEV 市场窗口",
    }
    return replacements.get(label, label)


def _side_by_side_ref_source_label(ref: dict[str, Any]) -> str:
    source = _text(ref.get("source") or ref.get("table"))
    source_labels = {
        "jato_msrp_postgres": "JATO MSRP 数据",
        "jato_price_positioning": "JATO 价格样本",
        "jato_country_chart_deck": "JATO 图表数据",
        "jato_country_snapshot": "JATO 市场快照",
        "jato_cross_country": "JATO 跨国对比",
        "jato_cross_reference": "JATO 交叉引用",
        "jato_filtered_query": "JATO 筛选查询",
        "jato_variant_diff_service": "JATO 配置差异",
    }
    return source_labels.get(source, source)


def _side_by_side_context_ref_label(label: str) -> str:
    match = re.match(r"crossCountry\.([^.]+)\.kpis\.cumulativeSales$", label, flags=re.IGNORECASE)
    if match:
        return f"{_side_by_side_country_display_name(match.group(1))} 累计销量"
    match = re.match(
        r"crossCountry\.([^.]+)\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.(?:sales|value)$",
        label,
        flags=re.IGNORECASE,
    )
    if match:
        return f"{_side_by_side_country_display_name(match.group(1))} {match.group(2).upper()} 动力销量"
    match = re.match(
        r"crossCountry\.([^.]+)\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.share$",
        label,
        flags=re.IGNORECASE,
    )
    if match:
        return f"{_side_by_side_country_display_name(match.group(1))} {match.group(2).upper()} 动力占比"
    match = re.match(r"contextSnapshot\.crossTabs\.registrationByFuel\.([^.]+)\.sales$", label)
    if match:
        return f"{match.group(1)} 注册量"
    match = re.match(r"contextSnapshot\.crossTabs\.registrationByFuel\.([^.]+)\.Business_pct$", label)
    if match:
        return f"{match.group(1)} 公司车注册占比"
    match = re.match(r"contextSnapshot\.crossTabs\.registrationByFuel\.([^.]+)\.Private_pct$", label)
    if match:
        return f"{match.group(1)} 私人注册占比"
    match = re.match(r"contextSnapshot\.crossTabs\.registrationBySegment\.([^.]+)\.sales$", label)
    if match:
        return f"{match.group(1)} 注册量"
    match = re.match(r"contextSnapshot\.crossTabs\.registrationBySegment\.([^.]+)\.Business_pct$", label)
    if match:
        return f"{match.group(1)} 公司车注册占比"
    match = re.match(r"contextSnapshot\.crossTabs\.registrationBySegment\.([^.]+)\.Private_pct$", label)
    if match:
        return f"{match.group(1)} 私人注册占比"
    match = re.match(r"contextSnapshot\.crossTabs\.driveBySegment\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 细分销量"
    match = re.match(r"contextSnapshot\.crossTabs\.driveBySegment\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 占比"
    match = re.match(r"contextSnapshot\.crossTabs\.segmentByFuel\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 细分销量"
    match = re.match(r"contextSnapshot\.crossTabs\.segmentByFuel\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 渗透率"
    match = re.match(r"contextSnapshot\.crossTabs\.driveByFuel\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力销量"
    match = re.match(r"contextSnapshot\.crossTabs\.driveByFuel\.([^.]+)\.([^.]+)_pct$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} {match.group(2).upper()} 占比"
    match = re.match(r"contextSnapshot\.crossTabs\.driveBySegment\.([^.]+)\.([^.]+)\.share$", label)
    if match:
        return f"{match.group(1)} {match.group(2)} 占比"
    match = re.match(r"contextSnapshot\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.share$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力占比"
    match = re.match(r"contextSnapshot\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.(?:sales|value)$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力销量"
    match = re.match(r"marketSnapshot\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.share$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力占比"
    match = re.match(r"marketSnapshot\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.(?:sales|value)$", label, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1)} 动力销量"
    match = re.match(r"contextSnapshot\.topModels\.([^.]+)\.sales$", label)
    if match:
        return f"{match.group(1)} 销量"
    return ""


def _side_by_side_country_display_name(value: str) -> str:
    mapping = {
        "sweden": "瑞典",
        "sverige": "瑞典",
        "se": "瑞典",
        "finland": "芬兰",
        "suomi": "芬兰",
        "fi": "芬兰",
        "norway": "挪威",
        "no": "挪威",
        "denmark": "丹麦",
        "dk": "丹麦",
    }
    text = _text(value)
    return mapping.get(text.casefold(), text)


def _side_by_side_ref_value(ref: dict[str, Any]) -> str:
    value = ref.get("value")
    if value is None or isinstance(value, bool):
        return ""
    unit = _text(ref.get("unit"))
    if isinstance(value, (int, float)):
        text = f"{value:,.0f}" if float(value).is_integer() else f"{value:,.1f}"
    else:
        raw_text = _text(value)
        try:
            numeric = float(raw_text.replace(",", ""))
        except ValueError:
            text = raw_text
        else:
            text = f"{numeric:,.0f}" if numeric.is_integer() else f"{numeric:,.1f}"
    if not text:
        return ""
    if not unit or unit.lower() in {"currency", "value"}:
        return text
    if unit == "%" and text.endswith("%"):
        return text
    if text.lower().endswith(unit.lower()):
        return text
    return f"{text} {unit}"


def _side_by_side_display_plan(
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any],
    *,
    question: str = "",
) -> str:
    artifacts = astrbot_side.get("visualArtifacts")
    if isinstance(artifacts, list) and artifacts:
        artifact_ids = _side_by_side_artifact_ids(artifacts)
        if any(
            isinstance(item, dict) and _text(item.get("id")) == "artifact_bom_entity_validation_table"
            for item in artifacts
        ):
            return (
                "先看 BOM/entity mapping validation table 核对 PI、market overlay、business variant、"
                "material code、颜色、生命周期和可编辑数量关系；只有真实库存/BOM rows 才进入库存证据表或指标卡。"
            )
        intent = _text(evidence_package.get("intent"))
        has_tco_validation_table = "artifact_tco_validation_table" in artifact_ids
        has_policy_pricing_table = "artifact_policy_pricing_table" in artifact_ids
        has_pricing_chart = "artifact_pricing_corridor_chart" in artifact_ids
        if has_policy_pricing_table:
            if has_pricing_chart:
                return (
                    "先看 policy/news evidence table 核对政策是否有效、来源日期和适用对象；"
                    "再看 Pricing evidence table / Pricing corridor chart 把政策价格上限转成 O5 BEV 与竞品价格动作。"
                    "TCO / company-car validation table 只作月供/RV 补充验证，不替代政策价格结论。"
                )
            return (
                "先看 policy/news evidence table 核对政策是否有效、来源日期和适用对象；"
                "再看 Pricing evidence table 把政策价格上限转成车型价格、配置和版本动作。"
            )
        if has_tco_validation_table and _side_by_side_tco_is_primary_display_question(
            astrbot_side,
            evidence_package,
            question=question,
        ):
            has_external_repair = "artifact_external_source_repair_table" in artifact_ids
            has_policy_table = bool(
                {
                    "artifact_news_policy_search_table",
                    "artifact_policy_market_context_table",
                }
                & artifact_ids
            )
            if has_policy_table:
                return (
                    "先看 TCO / company-car validation table 拆 benefit tax、月供、残值、年里程和充电条件；"
                    "再用 External source validation matrix / policy/news evidence table 核对官方来源和适用对象。"
                    "JATO channel mix 只能说明公司车暴露，不等于 TCO 更优。"
                )
            if has_external_repair:
                return (
                    "先看 TCO / company-car validation table 区分已有渠道暴露信号和待补月供/RV/tax benefit 证据；"
                    "再看 External source validation matrix 验证 leasing/TCO/company-car 来源。"
                    "没有月供、残值、benefit tax 前，不能把 PHEV 写成大客户主推结论。"
                )
            return (
                "先看 TCO / company-car validation table，把月供、残值/RV、tax benefit、年里程和充电条件拆成同假设验证；"
                "再把可追溯渠道或政策证据压成汇报块。"
            )
        if any(
            isinstance(item, dict) and _text(item.get("id")) == "artifact_external_source_repair_table"
            for item in artifacts
        ):
            if intent == "voc_analysis":
                return (
                    "先看 External source validation matrix 核对 VOC/媒体/论坛来源、发布日期和原文要点；"
                    "再看 VOC evidence/framework table，把可引用来源转成用户痛点、卖点和产品动作。"
                )
            if intent == "news_policy_search":
                return (
                    "先看 External source validation matrix 核对官方/可引用政策来源、发布日期、适用对象和限制；"
                    "再看 policy/news evidence table 和 report block 输出车型、价格和渠道动作。"
                )
            if intent == "pricing_analysis":
                return (
                    "先看 External source validation matrix 核对官方价格/MSRP 来源、发布日期、车型/版本、币种和价格字段；"
                    "验证后再把 citation-ready price evidence 接入价格走廊图、定价表和汇报块。"
                )
        if intent == "pricing_analysis":
            has_pricing_table = "artifact_pricing_analysis_table" in artifact_ids
            has_pricing_chart = "artifact_pricing_corridor_chart" in artifact_ids
            has_msrp_repair = "artifact_msrp_source_repair_table" in artifact_ids
            has_user_material_price = _pricing_package_uses_user_material_price(evidence_package)
            has_user_material_gap = _pricing_package_uses_user_material_with_current_price_gap(evidence_package)
            if has_pricing_chart and has_pricing_table:
                if has_user_material_price:
                    source_boundary = (
                        "当前官方 MSRP 和竞品官方价格仍需通过 MSRP source validation table 补齐。"
                        if has_user_material_gap or has_msrp_repair
                        else "用户材料价格不能直接当作当前官方 MSRP；正式定案前仍需用官网 MSRP、月供/RV 和竞品官方价格交叉验证。"
                    )
                    return (
                        "先看 Pricing corridor chart 核对用户材料价格锚点、PVA 和样本价格背景；"
                        "再看 Pricing evidence table 区分用户材料价格、样本走廊、月供/RV 缺口和证据边界。"
                        f"{source_boundary}"
                    )
                return (
                    "先看 Pricing corridor chart 判断目标价或官方 MSRP 在价格走廊中的位置；"
                    "再看 Pricing evidence table 拆 MSRP、价差、月供/RV 和证据边界。"
                )
            if has_pricing_table and has_msrp_repair:
                return (
                    "先看 Pricing evidence table 区分用户给定价差、样本价格背景和可用证据；"
                    "再看 MSRP source validation table 补齐本车型/竞品官方价格来源。"
                    "价格走廊图只在本车型/竞品官方 MSRP、用户目标价或明确竞品走廊可追溯后生成。"
                )
            if has_pricing_table:
                return (
                    "先看 Pricing evidence table 拆价格锚点、样本背景、月供/RV 缺口和证据边界；"
                    "没有可追溯价格锚点时暂不生成价格走廊图。"
                )
            if has_msrp_repair:
                return (
                    "先看 MSRP source validation table 补齐本车型/竞品官方价格来源、版本、币种和发布日期；"
                    "验证为 citation-ready price evidence 后再生成定价表、价格走廊图和汇报块。"
                )
        if intent == "competitor_compare":
            has_competitor_chart = "artifact_competitor_evidence_chart" in artifact_ids
            has_competitor_table = bool(
                {
                    "artifact_competitor_compare_table",
                    "artifact_competitor_compare_framework_table",
                }
                & artifact_ids
            )
            has_market_structure_chart = "artifact_market_structure_chart" in artifact_ids
            has_pending_msrp = bool(
                {
                    "artifact_pending_msrp_review_chart",
                    "artifact_pending_msrp_review_table",
                }
                & artifact_ids
            )
            has_msrp_repair = "artifact_msrp_source_repair_table" in artifact_ids
            if has_competitor_chart and has_competitor_table and not has_msrp_repair:
                return (
                    "先看 Competitor evidence chart 判断已验证竞品量级；"
                    "再看 Competitor comparison table 拆级别、动力、价格/配置差异和产品动作。"
                )
            if has_pending_msrp and has_competitor_table:
                return (
                    "先看 Competitor comparison table 判断主对标、校验锚点和可赢点；"
                    "Pending MSRP review table/chart 只作待审核价格线索；"
                    "再看 MSRP source validation table 补齐本车型/竞品官方价格来源。"
                )
            if has_market_structure_chart and has_competitor_table:
                return (
                    "先看 Market structure chart 判断细分市场、动力和场景背景；"
                    "再看 Competitor comparison table 拆定位差异和产品动作。"
                    "MSRP source validation table 用于补官方价格；市场结构图不能当作车型胜负或价格证据。"
                )
            if has_competitor_table and has_msrp_repair:
                return (
                    "先看 Competitor comparison table 拆对标角色、级别、动力和可赢点；"
                    "再看 MSRP source validation table 补齐本车型/竞品官方价格来源。"
                    "价格/配置证据补齐前，不生成确定的竞品胜负图。"
                )
        if intent == "report_generation":
            has_report_block = "artifact_report_block" in artifact_ids
            has_pricing_chart = "artifact_pricing_corridor_chart" in artifact_ids
            has_pricing_table = "artifact_report_pricing_table" in artifact_ids
            has_user_material_price = _pricing_package_uses_user_material_price(evidence_package)
            has_user_material_gap = _pricing_package_uses_user_material_with_current_price_gap(evidence_package)
            if has_report_block and has_pricing_chart and has_pricing_table and has_user_material_price:
                source_boundary = (
                    "当前官方 MSRP、竞品官方价格和月供/RV 仍需补源后才能定案。"
                    if has_user_material_gap
                    else "用户材料价格不能替代当前官方 MSRP，正式定案前仍需补官网 MSRP、竞品官方价格和月供/RV。"
                )
                return (
                    "先看 PPT-ready block 复制一页结论；"
                    "再看 Pricing corridor chart 和 Pricing evidence table 核对用户材料价格锚点、PVA 和样本价格背景。"
                    f"{source_boundary}"
                )
        artifact_names = _dedupe_string_list([
            _text(item.get("title") if isinstance(item, dict) else "") or _text(item.get("type") if isinstance(item, dict) else "")
            for item in artifacts
        ])[:3]
        if artifact_names:
            return f"优先展示已生成的 {' / '.join(artifact_names)}，再把关键证据压成可复制汇报块。"
    intent = _text(evidence_package.get("intent"))
    mapping = {
        "pricing_analysis": "用价格证据表展示本车型价格、目标价、竞品走廊和缺失的月供/RV。",
        "competitor_compare": "用 Competitor comparison table 展示车型、销量/份额、价格、配置差异和可赢点；有两个以上可比数值时再生成对应 chart。",
        "market_overview": "用 metric cards + market chart 展示市场规模、动力结构、趋势和机会 segment。",
        "configuration_analysis": "用配置差异表展示 must-have、visible value、cost/risk 和主销配置建议。",
        "inventory_analysis": "用 BOM/entity mapping validation table 展示 PI、市场、版本、物料号、颜色、生命周期和可编辑数量关系。",
        "news_policy_search": "用来源表展示政策日期、适用对象、车型影响和下一步查证动作。",
        "voc_analysis": "用 VOC 来源表和主题表展示用户痛点、来源可信度和可转化卖点。",
        "report_generation": "用 report block 输出一页 PPT-ready 结构，并附证据表或图表。",
    }
    return mapping.get(intent, "")


def _side_by_side_artifact_ids(artifacts: Any) -> set[str]:
    if not isinstance(artifacts, list):
        return set()
    return {
        _text(item.get("id"))
        for item in artifacts
        if isinstance(item, dict) and _text(item.get("id"))
    }


def _pricing_package_uses_user_material_with_current_price_gap(evidence_package: dict[str, Any]) -> bool:
    if not _pricing_package_uses_user_material_price(evidence_package):
        return False
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    return any(
        isinstance(item, dict)
        and _text(item.get("name")) == "coverage_diagnostic:no_current_prices_for_requested_models"
        for item in missing
    )


def _pricing_package_uses_user_material_price(evidence_package: dict[str, Any]) -> bool:
    refs = _side_by_side_evidence_refs(evidence_package)
    return any(
        _side_by_side_ref_is_user_material_price(ref)
        for ref in refs
    )


def _side_by_side_ref_is_user_material_price(ref: dict[str, Any]) -> bool:
    haystack = " ".join(
        _text(ref.get(key))
        for key in ("label", "source", "table")
    ).casefold()
    return (
        "user material" in haystack
        and any(token in haystack for token in ("msrp", "price", "corridor"))
    ) or "j7_hev_v4" in haystack or "j7_hev_method" in haystack


def _side_by_side_tco_is_primary_display_question(
    astrbot_side: dict[str, Any],
    evidence_package: dict[str, Any],
    *,
    question: str = "",
) -> bool:
    question_text = _text(question or astrbot_side.get("question") or evidence_package.get("question")).casefold()
    if _question_requires_leasing_tco_evidence(question_text):
        return True
    if "phev" in question_text and any(
        token in question_text
        for token in (
            "co₂",
            "co2",
            "0-75",
            "税",
            "tax",
            "benefit",
            "阶梯",
        )
    ):
        return True
    missing_sets = [
        astrbot_side.get("missingEvidence") if isinstance(astrbot_side.get("missingEvidence"), list) else [],
        evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else [],
    ]
    return any(
        isinstance(item, dict) and _text(item.get("name")) == "leasing_tco_or_company_car_evidence"
        for missing in missing_sets
        for item in missing
    )


def _side_by_side_should_refresh_display_plan(astrbot_side: dict[str, Any], *, question: str = "") -> bool:
    artifacts = astrbot_side.get("visualArtifacts")
    if not isinstance(artifacts, list):
        return False
    intent = _text(
        (astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}).get("intent")
    )
    artifact_ids = _side_by_side_artifact_ids(artifacts)
    current = _text(astrbot_side.get("displayPlan")).casefold()
    has_bom_entity_table = any(
        isinstance(item, dict) and _text(item.get("id")) == "artifact_bom_entity_validation_table"
        for item in artifacts
    )
    if has_bom_entity_table:
        return "bom/entity mapping validation table" not in current and "bom/实体映射验证表" not in current
    has_policy_pricing_table = "artifact_policy_pricing_table" in artifact_ids
    if has_policy_pricing_table:
        missing_policy_pricing_plan = not any(
            token in current
            for token in (
                "policy/news evidence table",
                "pricing evidence table",
                "pricing corridor chart",
                "政策价格",
                "价格上限",
            )
        )
        stale_tco_primary_plan = (
            "tco" in current
            and not _side_by_side_tco_is_primary_display_question(
                astrbot_side,
                astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {},
                question=question,
            )
        )
        return missing_policy_pricing_plan or stale_tco_primary_plan
    has_tco_validation_table = "artifact_tco_validation_table" in artifact_ids
    if has_tco_validation_table:
        if not _side_by_side_tco_is_primary_display_question(
            astrbot_side,
            astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {},
            question=question,
        ):
            return False
        return not any(
            token in current
            for token in (
                "tco",
                "company-car",
                "company car",
                "月供",
                "残值",
                "benefit tax",
                "tax benefit",
                "公司车",
            )
        )
    has_external_repair_table = any(
        isinstance(item, dict) and _text(item.get("id")) == "artifact_external_source_repair_table"
        for item in artifacts
    )
    if has_external_repair_table:
        return (
            "external source validation matrix" not in current
            and "外部来源验证矩阵" not in current
            and "外部来源修复表" not in current
        )
    if intent == "pricing_analysis":
        has_pricing_chart = "artifact_pricing_corridor_chart" in artifact_ids
        has_pricing_table = "artifact_pricing_analysis_table" in artifact_ids
        has_msrp_repair = "artifact_msrp_source_repair_table" in artifact_ids
        has_user_material_price = _pricing_package_uses_user_material_price(
            astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
        )
        has_user_material_gap = _pricing_package_uses_user_material_with_current_price_gap(
            astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
        )
        stale_chart_plan = (
            not has_pricing_chart
            and any(token in current for token in ("pricing corridor chart", "价格走廊图", "柱状图"))
        )
        stale_user_material_plan = has_user_material_price and "用户材料" not in current
        missing_repair_plan = has_msrp_repair and not any(
            token in current
            for token in ("msrp source validation", "官方价格", "msrp 来源", "msrp来源")
        )
        missing_table_plan = has_pricing_table and "pricing evidence table" not in current and "价格证据表" not in current
        return stale_chart_plan or stale_user_material_plan or missing_repair_plan or missing_table_plan
    if intent == "competitor_compare":
        has_competitor_chart = "artifact_competitor_evidence_chart" in artifact_ids
        has_competitor_table = bool(
            {
                "artifact_competitor_compare_table",
                "artifact_competitor_compare_framework_table",
            }
            & artifact_ids
        )
        has_msrp_repair = "artifact_msrp_source_repair_table" in artifact_ids
        stale_chart_plan = (
            not has_competitor_chart
            and any(token in current for token in ("可用柱状图", "competitor sales chart", "competitor price chart"))
        )
        missing_repair_plan = has_msrp_repair and not any(
            token in current
            for token in ("msrp source validation", "官方价格", "msrp 来源", "msrp来源")
        )
        missing_table_plan = (
            has_competitor_table
            and "competitor comparison table" not in current
            and "竞品矩阵" not in current
            and "竞品对比" not in current
        )
        return stale_chart_plan or missing_repair_plan or missing_table_plan
    if intent == "report_generation":
        has_user_material_price = _pricing_package_uses_user_material_price(
            astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
        )
        has_user_material_gap = _pricing_package_uses_user_material_with_current_price_gap(
            astrbot_side.get("evidencePackage") if isinstance(astrbot_side.get("evidencePackage"), dict) else {}
        )
        return (has_user_material_price or has_user_material_gap) and "用户材料" not in current
    return False


def _sanitize_business_review_text(value: str) -> str:
    lines = [
        _sanitize_business_review_line(line) if line.strip() else ""
        for line in _text(value).splitlines()
    ]
    return "\n".join(lines).strip()


def _business_review_existing_preview_direct(value: str) -> str:
    text = _text(value)
    if not text:
        return ""
    first = re.split(r"\n\s*\n(?=## )|\n(?=## )", text, maxsplit=1)[0]
    return _compact_business_review_existing_direct(first)


def _compact_business_review_existing_direct(value: str) -> str:
    sentences = _business_review_sentence_units(value)
    if len(sentences) <= 1:
        return _text(value)
    result: list[str] = []
    for sentence in sentences:
        text = _text(sentence)
        if not text:
            continue
        if _business_review_existing_direct_sentence_belongs_to_section(text):
            continue
        signature = _business_review_line_signature(text)
        references = [
            _business_review_line_signature(item)
            for item in result
        ]
        if _business_review_line_is_redundant(signature, references):
            continue
        result.append(text)
    return " ".join(result).strip() or _text(value)


def _business_review_sentence_units(value: str) -> list[str]:
    text = _text(value)
    if not text:
        return []
    parts = re.split(r"(?<=[。！？!?])\s*", text)
    return [part.strip() for part in parts if part.strip()]


def _business_review_existing_direct_sentence_belongs_to_section(value: str) -> bool:
    text = _text(value)
    return bool(re.match(r"^(建议动作|下一步(?:执行|动作)?|验证重点|当前证据状态|展示骨架|汇报口径)(?:\s*[：:]|是)", text))


def _business_review_existing_preview_bullets(value: str) -> list[str]:
    text = _text(value)
    if not text:
        return []
    lines = []
    section = ""
    structured_sections = {
        "关键证据",
        "产品经理判断",
        "下一步动作",
        "汇报口径",
        "证据边界",
        "key evidence",
        "product manager judgment",
        "next actions",
        "reporting",
        "evidence limits",
    }
    for line in text.splitlines():
        stripped = line.strip()
        heading = re.match(r"^#+\s*(.+?)\s*$", stripped)
        if heading:
            section = heading.group(1).strip().lower()
            continue
        if section in structured_sections:
            continue
        if stripped.startswith("- "):
            lines.append(stripped[2:].strip())
    return _dedupe_string_list(lines)


def _rewrite_business_review_text_for_question(value: str, answer: dict[str, Any]) -> str:
    direct = _question_specific_business_review_direct(answer)
    if not direct:
        return value
    parts = re.split(r"\n\s*\n(?=## )", value, maxsplit=1)
    first = parts[0] if parts else value
    if not _looks_like_generic_business_review_direct(first):
        return value
    if len(parts) == 2:
        return f"{direct}\n\n{parts[1]}".strip()
    return direct


def _business_review_direct_for_pm(value: str) -> str:
    text = _sanitize_business_review_line(value)
    if not text:
        return ""
    text = text.replace("已查数据：", "证据锚点：")
    text = re.sub(r"\s*(?:输出视图|展示)\s*[：:][^。！？!?]*(?:[。！？!?]|$)", " ", text)
    text = re.split(r"\bEvidence Limits\b|证据对齐[：:]", text, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    text = re.sub(r"下一步执行\s*[^。；]+[。；]?", "", text)
    text = re.sub(r"证据状态：[^。]+[。；]?", "", text)
    text = re.sub(r"分析对象：[^。]+[。；]?", "", text)
    text = _strip_source_candidate_boundary_sentence(text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("；。", "。").replace("。。", "。")
    return text.strip("； ")


def _strip_source_candidate_boundary_sentence(value: str) -> str:
    text = _text(value)
    if not text:
        return ""
    text = re.sub(
        r"\s*补源状态\s*[：:][^。；\n]*(?:[。；]|$)",
        "",
        text,
    )
    text = re.sub(
        r"\s*(?:搜索候选\d+项|来源草稿\d+项)\s*[：:][^。\n]*(?:[。]|$)",
        "",
        text,
    )
    text = re.sub(
        r"\s*(?:这些)?(?:搜索候选和来源草稿|搜索候选|来源草稿)(?:都)?(?:只是|仅是)?[^。；\n]*(?:补证线索|补数清单|官方价格证据|数值证据|当前价格记录)[^。；\n]*(?:[。；]|$)",
        "",
        text,
    )
    text = re.sub(
        r"\s*这些(?:搜索|检索)?候选只是补证线索[^。；\n]*(?:[。；]|$)",
        "",
        text,
    )
    text = re.sub(
        r"\s*这些检索线索只是补证线索[^。；\n]*(?:[。；]|$)",
        "",
        text,
    )
    text = re.sub(
        r"\s*这些候选只是补数清单[^。；\n]*(?:[。；]|$)",
        "",
        text,
    )
    return re.sub(r"\s+", " ", text).strip()


def _business_review_direct_evidence_line(value: str) -> str:
    text = _sanitize_business_review_line(value)
    if not text:
        return ""
    match = re.search(r"(证据状态：[^。]+|证据对齐[^。]+|当前有\s*\d+\s*条可引用证据[^。]*)", text)
    return match.group(1).strip() if match else ""


def _business_review_evidence_lines(lines: list[str], direct: str) -> list[str]:
    evidence_markers = (
        "当前有",
        "证据",
        "数据",
        "销量",
        "sales",
        "份额",
        "share",
        "占比",
        "市场",
        "价格",
        "价位",
        "msrp",
        "eur",
        "sek",
        "units",
        "unit",
        "source",
        "=",
        "%",
        "竞品",
        "配置",
        "pva",
        "来源",
    )
    cleaned: list[str] = []
    for line in _filter_redundant_review_lines(lines, direct):
        if _business_review_line_role(line) in {
            "action",
            "display",
            "key_message",
            "product_implication",
            "risk",
            "so_what",
            "title",
            "verdict",
        }:
            continue
        item = _business_review_clean_content_line(line)
        if not item:
            continue
        if _business_review_is_method_or_judgment_evidence_fallback(item):
            continue
        lower = item.lower()
        if any(marker in lower for marker in evidence_markers):
            cleaned.append(item)
    return _dedupe_string_list(cleaned)


def _business_review_is_method_or_judgment_evidence_fallback(value: str) -> bool:
    text = _text(value)
    if re.search(r"\d+\s*条可引用证据", text) and "待补可引用证据" not in text:
        return True
    if any(
        marker in text
        for marker in (
            "竞品判断应先锁定",
            "竞品定位方法",
            "结论要能转成",
            "结论要落成定位话术",
            "正面对抗、错位竞争或价格锚点",
            "用竞品矩阵展示",
            "生成竞品矩阵",
            "补齐缺失证据后再收敛结论",
            "下一步应补齐缺失证据",
            "VOC 判断",
            "VOC 证据方法",
            "用户声音/VOC 分析",
            "用来源表和主题表",
            "证据安全检查",
            "证据对齐：证据不足",
            "外部研究治理提醒",
            "数据来源有限",
            "用户基数小",
            "反馈样本不足",
            "未覆盖瑞典本地",
            "未获取到具体车型",
            "未包含 OMODA/JAECOO",
            "无法获取瑞典用户",
            "没有可追溯来源时",
            "不能声称高频",
            "只能给验证假设",
            "当前不能输出确定数字",
            "定价判断不能套用单一车型模板",
            "定价不能只看 MSRP",
            "若缺少最新价格证据",
            "第一版建议先输出价格矩阵模板",
            "必须放在同一页",
            "市场机会方法",
            "市场数据要落到机会 segment",
            "对 OMODA/JAECOO 的价值在于识别",
            "配置价值方法",
            "配置结论必须连接用户场景",
            "缺工程配置时先输出",
        )
    ):
        return True
    if any(marker in text for marker in ("数据缺失，无法", "缺失，无法", "未包含", "仅用于初步", "不能作为")):
        return True
    return False


def _business_review_digest_evidence_lines(lines: list[str], direct: str) -> list[str]:
    cleaned: list[str] = []
    for line in _filter_redundant_evidence_review_lines(lines, direct):
        if _business_review_line_role(line) in {
            "action",
            "display",
            "key_message",
            "product_implication",
            "risk",
            "so_what",
            "title",
            "verdict",
        }:
            continue
        item = _business_review_clean_content_line(line)
        if not item:
            continue
        if item.startswith("本轮已查数据源"):
            continue
        if _business_review_is_method_or_judgment_evidence_fallback(item):
            continue
        cleaned.append(item)
    return _dedupe_string_list(cleaned)


def _business_review_judgment_lines(lines: list[str], direct: str) -> list[str]:
    cleaned: list[str] = []
    for line in _filter_redundant_review_lines(lines, direct):
        if _business_review_line_role(line) in {"action", "risk", "verdict", "display"}:
            continue
        item = _strip_source_candidate_boundary_sentence(_business_review_clean_content_line(line))
        if not item:
            continue
        if _business_review_is_metric_fragment(item):
            continue
        if any(marker in item for marker in ("本轮工具链", "下一步应补齐缺失证据后再收敛结论")):
            continue
        if _business_review_is_source_rationale_line(item):
            continue
        if _business_review_is_method_or_judgment_evidence_fallback(item):
            continue
        cleaned.append(item)
    return _dedupe_string_list(cleaned)


def _business_review_is_source_rationale_line(value: str) -> bool:
    text = _text(value)
    normalized = text.casefold()
    return any(
        marker in normalized
        for marker in (
            "use this source",
            "citation candidate",
            "cross-check with internal evidence",
        )
    ) or any(
        marker in text
        for marker in (
            "先验证价格走廊、月供或竞品定位",
            "先确认官方来源、发布日期",
            "外部信息只作背景",
            "作为候选来源，并与内部证据交叉验证",
        )
    )


def _business_review_is_metric_fragment(value: str) -> bool:
    text = _text(value)
    return bool(re.match(r"^\d+(?:[.,]\d+)?\s*%?\s*[，,]", text))


def _business_review_inline_business_judgment(value: str) -> str:
    text = _strip_source_candidate_boundary_sentence(_sanitize_business_review_line(value))
    if not text:
        return ""
    for heading in (
        "产品动作",
        "市场机会",
        "机会判断",
        "市场判断",
        "配置判断",
        "电池判断",
        "用户场景",
        "产品边界",
        "决策口径",
        "业务含义",
        "价差边界",
        "价格判断",
        "目标价判断",
        "相对定价判断",
        "价差判断",
        "定价判断",
        "价格合理性判断",
        "用户价值",
    ):
        for marker in (f"{heading}：", f"{heading}:"):
            if marker not in text:
                continue
            section = text.split(marker, 1)[1].strip()
            if _business_review_line_role(section) == "display":
                continue
            for stop in (
                " 展示骨架",
                " 产品动作",
                " 市场机会",
                " 机会判断",
                " 市场判断",
                " 配置判断",
                " 电池判断",
                " 用户场景",
                " 产品边界",
                " Must-have",
                " Visible value",
                " 决策口径",
                " 业务含义",
                " 价差边界",
                " 用户价值",
                " 必须补证",
                " 缺 ",
                " 这些搜索候选",
                " 这些候选",
                " 这些检索线索",
                " 当前仍缺",
                " ##",
                "\n##",
            ):
                if stop in section:
                    section = section.split(stop, 1)[0].strip()
            return _strip_source_candidate_boundary_sentence(section)[:420]
    sentences = [
        re.sub(r"^直接结论\s*[：:]\s*", "", sentence.strip()).strip()
        for sentence in re.split(r"(?<=[。.!?])\s*", text)
        if sentence.strip()
    ]
    for markers in (
        ("产品动作", "对 OMODA/JAECOO 的动作", "J7 应"),
        ("低配", "高配", "价格锚点", "主销", "冬季", "续航", "公司车"),
    ):
        for item in sentences:
            if _business_review_line_role(item) == "display":
                continue
            if _business_review_is_method_or_judgment_evidence_fallback(item):
                continue
            if any(marker in item for marker in markers):
                return _strip_source_candidate_boundary_sentence(item)[:420]
    return ""


def _business_review_action_review_lines(lines: list[str]) -> list[str]:
    cleaned: list[str] = []
    for line in lines:
        item = _business_review_clean_content_line(line)
        if not item:
            continue
        cleaned.append(item)
    return _dedupe_string_list(cleaned)


def _question_specific_business_review_action_lines(answer: dict[str, Any], existing: list[str]) -> list[str]:
    question = _text(answer.get("question")).lower()
    if not question:
        return existing
    if "leasing" in question and "phev" in question:
        return [
            "P0 · 建立 PHEV fleet leasing TCO 表 — 同时纳入月供、残值、税费、燃油/用电、充电条件、年里程和冬季使用风险。",
            "P1 · 定义 PHEV 保留主推资格的客户场景 — 只保留长途、高里程、无稳定充电或低风险替代场景，避免泛化成所有公司车答案。",
            "P2 · 补齐 BEV/PHEV/HEV 同假设对比 — 用相同租期、年里程、首付、服务包和税费口径验证 PHEV 是否真的更稳。",
        ]
    if any(token in question for token in ("omoda9", "omoda 9", "o9")) and any(
        token in question for token in ("一个版型", "多个物料", "物料号")
    ):
        return [
            "P0 · 建物料号解释矩阵 — 把每个 material code 对应到 business variant、颜色/内饰、市场、生命周期和订单状态，先区分正常 SKU 与异常。",
            "P1 · 标记历史替代和 phase-out — 防止把旧物料、替代物料或市场专属物料误判为重复版型。",
            "P2 · 输出销售口径 — 前端只展示用户可理解的业务版型，物料号保留给供应链、车辆生成和审计回溯。",
        ]
    if "bom" in question and any(token in question for token in ("车型版本", "内外饰", "颜色", "建模")):
        return [
            "P0 · 固定实体层级 — 明确 PI header、market overlay、business variant、material code、color/interior、lifecycle 和 editable quantity 的主从关系。",
            "P1 · 建映射约束 — 定义版型到物料号、颜色到订单组合、市场到合规/价格/生命周期的一对多和多对一规则。",
            "P2 · 生成校验表 — 检查缺失映射、重复 SKU、跨市场混用、不可下单颜色组合和生命周期冲突。",
        ]
    if any(token in question for token in ("当月选品表", "可编辑数量")):
        return [
            "P0 · 定义可编辑数量公式 — 用可售库存/配额扣除已分配、冻结订单、phase-out、市场限制和不可下单颜色组合。",
            "P1 · 汇总到用户选择层 — 把 material code 数量汇总成车型版本 + 外观/内饰组合，避免用户直接编辑底层物料号。",
            "P2 · 保留审计回溯 — 每个可编辑数量都能展开到底层物料号、订单状态、生命周期和市场限制。",
        ]
    return existing


def _business_review_current_source_repair_action(answer: dict[str, Any]) -> str:
    candidates = answer.get("sourceRepairCandidates") if isinstance(answer.get("sourceRepairCandidates"), dict) else {}
    if not candidates:
        evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
        candidates = (
            evidence_package.get("sourceRepairCandidates")
            if isinstance(evidence_package.get("sourceRepairCandidates"), dict)
            else {}
        )
    if not candidates:
        return ""
    action = _source_repair_action_text(candidates)
    if not action:
        return ""
    if _is_policy_source_repair(candidates):
        rationale = "政策/新闻来源候选只是补证入口，补齐来源日期和适用条件后才能写时效性政策结论。"
    elif _is_external_query_source_repair(candidates):
        rationale = "VOC/媒体/论坛检索线索只是补证入口，补齐可引用来源和原文要点后才能判断用户高频痛点。"
    else:
        rationale = "当前价格覆盖诊断已找到来源修复候选；先把搜索候选或来源草稿生成当前价格记录，再写确定价格数字。"
    return f"P0 · {action} — {rationale}"


def _business_review_is_stale_source_repair_action(line: str) -> bool:
    text = str(line or "")
    markers = (
        "MSRP 来源修复表",
        "外部来源修复表",
        "External source repair table",
        "external source repair table",
        "官方价格搜索候选",
        "官方价格候选",
        "来源草稿",
        "current_price 行",
        "当前价格记录",
    )
    return any(marker in text for marker in markers)


def _question_specific_business_review_ppt_lines(answer: dict[str, Any], existing: list[str]) -> list[str]:
    question = _text(answer.get("question")).lower()
    if not question:
        return existing
    if "leasing" in question and "phev" in question:
        preferred = [
            line
            for line in existing
            if "tco" in line.lower() or "company-car" in line.lower() or "company car" in line.lower()
        ]
        result = preferred[:1] if preferred else [
            "优先展示 TCO/company-car 验证表和 PPT-ready 汇报块，把 PHEV 是否成立压成同假设成本对比。"
        ]
        result.append(
            "汇报页只保留 TCO/company-car 判断：月供、残值、税费、年里程、充电条件、冬季风险和 BEV/PHEV/HEV 同假设对比；不要把泛 MSRP、竞品走廊或普通定价模板当成 PHEV leasing 结论。"
        )
        return _dedupe_string_list(result)
    if "company car" in question and "benefit" in question and "bev" in question and "phev" in question:
        preferred = [
            line
            for line in existing
            if "tco" in line.lower() or "company-car" in line.lower() or "company car" in line.lower()
        ]
        result = preferred[:1] if preferred else [
            "优先展示 TCO/company-car 验证表，把 benefit tax、月供、残值、年里程、充电条件和 BEV/PHEV 同假设对比放在一页。"
        ]
        result.append(
            "汇报页只保留 company-car benefit 判断：JATO channel mix 是暴露信号，不是 TCO 结论；必须补官方 benefit formula、月供/RV 和使用场景后才能判断 BEV/PHEV 谁更优。"
        )
        return _dedupe_string_list(result)
    if "o5" in question and "ev3" in question and ("3k" in question or "3,000" in question or "便宜" in question):
        has_pricing_chart = _answer_has_visual_artifact(answer, "artifact_pricing_corridor_chart")
        preferred = [
            line
            for line in existing
            if "价格走廊" in line or "价格证据" in line or "pricing" in line.lower()
        ]
        fallback = (
            "优先展示价格走廊图和价格证据表，把 O5/EV3 的 3,000 EUR 价差假设拆成证据验证。"
            if has_pricing_chart
            else (
                "优先展示 Pricing evidence table 和 MSRP source validation table，把 3,000 EUR 价差假设、"
                "样本价格背景和 O5/EV3 官方 MSRP 缺口拆开；未补齐官方价格前不生成价格走廊图。"
            )
        )
        if preferred and (has_pricing_chart or not _business_review_line_mentions_pricing_chart(preferred[0])):
            result = preferred[:1]
        else:
            result = [fallback]
        result.append(
            "汇报页只围绕 O5 vs EV3：官方 MSRP 缺口、3,000 EUR 用户假设价差、电池/续航/配置差异、月供/RV 和品牌风险；不要泛化成普通价格走廊模板。"
        )
        return _dedupe_string_list(result)
    if "o9" in question and ("53k" in question or "55k" in question or "53k-55k" in question):
        preferred = [
            line
            for line in existing
            if "价格走廊" in line or "价格证据" in line or "pricing" in line.lower()
        ]
        result = preferred[:1] if preferred else [
            "优先展示价格走廊图和价格证据表，把 O9 53k-55k 目标价放到样本走廊上验证。"
        ]
        result.append(
            "汇报页只围绕 O9 53k-55k：目标价上下沿、样本走廊、配置/质保/空间价值、company-car 或 leasing 支撑；不要泛化成普通定价模板。"
        )
        return _dedupe_string_list(result)
    if any(token in question for token in ("吐槽", "voc", "用户声音", "owner review", "forum")):
        preferred = [
            line
            for line in existing
            if "voc" in line.lower() or "external source" in line.lower() or "外部来源" in line
        ]
        result = preferred[:1] if preferred else [
            "优先展示 VOC 验证框架表和外部来源验证矩阵，先证明来源、日期和原文要点，再写高频痛点。"
        ]
        result.append(
            "汇报页只写候选痛点、验证状态、可引用来源缺口和产品动作；没有媒体/论坛/用户原声前，不写成已验证高频吐槽。"
        )
        return _dedupe_string_list(result)
    return existing


def _answer_has_visual_artifact(answer: dict[str, Any], artifact_id: str) -> bool:
    artifacts = answer.get("visualArtifacts")
    return isinstance(artifacts, list) and any(
        isinstance(item, dict) and _text(item.get("id")) == artifact_id
        for item in artifacts
    )


def _business_review_line_mentions_pricing_chart(value: str) -> bool:
    text = _text(value).casefold()
    return any(token in text for token in ("pricing corridor chart", "价格走廊图", "柱状图"))


def _business_review_ppt_lines(lines: list[str], direct: str) -> list[str]:
    cleaned = [
        item
        for line in _filter_redundant_review_lines(lines, direct)
        if (item := _business_review_clean_content_line(line))
    ]
    return _dedupe_string_list(cleaned)


def _business_review_limit_lines(lines: list[str]) -> list[str]:
    cleaned: list[str] = []
    for line in lines:
        item = _business_review_clean_content_line(line)
        if not item:
            continue
        item = _business_review_rewrite_limit_line(item)
        if not item:
            continue
        cleaned.append(item)
    return _dedupe_string_list(cleaned)


def _business_review_rewrite_limit_line(value: str) -> str:
    text = _text(value)
    if not text:
        return ""
    normalized = text.lower()
    text = re.sub(r"^风险边界[：:]\s*", "", text)
    text = text.replace("；缓解方式：", "；建议：")
    text = text.replace("；建议： ", "；建议：")
    text = re.sub(r"^mixed[ _]currency[ _]unit\s*[：:]\s*", "价格单位风险：", text, flags=re.IGNORECASE)
    text = re.sub(r"^multiple[ _]pva[ _]values\s*[：:]\s*", "PVA 口径风险：", text, flags=re.IGNORECASE)
    text = re.sub(r"^non[ _]target[ _]market[ _]template[ _]residue\s*[：:]\s*", "非目标市场模板风险：", text, flags=re.IGNORECASE)
    text = re.sub(r"^competitor[ _]price[ _]basis[ _]mismatch\s*[：:]\s*", "价格口径不一致风险：", text, flags=re.IGNORECASE)
    text = re.sub(r"^(价格覆盖诊断：缺少请求车型当前价格)\s*[：:]\s*", r"\1：", text)
    if re.match(r"^证据对齐[：:]", text) or re.match(r"^证据状态[：:]\s*(部分对齐|证据一致)", text):
        return ""
    if "结论仍应随最新价格、政策和配置证据更新" in text:
        return ""
    if "支撑证据 missing" in text or "supporting evidence missing" in normalized:
        return ""
    text = text.replace("当前 query_msrp_pricing", "当前价格数据查询")
    if "query_msrp_pricing" in text:
        return text.replace("query_msrp_pricing", "价格数据查询")
    if "compare_vehicle_variants" in text:
        return text.replace("compare_vehicle_variants", "配置差异工具")
    if "compare_competitive_set" in text:
        return text.replace("compare_competitive_set", "竞品池工具")
    if "search_market_news" in text and ("论坛" in text or "媒体" in text or "车主" in text):
        return "仍需补瑞典本地汽车论坛、车主群和媒体测评来源；当前来源覆盖不足，不能写成已验证高频痛点。"
    if "tavily" in normalized and ("索引" in text or "搜索" in text):
        return "外部来源覆盖不足，不能把候选主题写成已验证高频痛点；需要补可引用原文、URL 和发布日期。"
    if any(token in normalized for token in ("ensun", "car leasing companies", "rankseed")):
        return "外部来源不足：当前只拿到泛租赁目录，未达到策略要求的可引用来源数量，不能支撑 PHEV leasing/TCO 或 company-car benefit 结论。"
    if "未使用 query_country_snapshot" in text:
        return "当前不能用销量规模推断用户反馈频次；VOC 结论必须先补可追溯用户原声或媒体测评。"
    if "未尝试读取具体" in text and ("媒体" in text or "网页" in text):
        return "仍需补具体瑞典汽车媒体、论坛或车主原文，保留 URL、发布日期和可支撑要点。"
    if "尚未在瑞典上市" in text or ("尚未上市" in text and any(token in normalized for token in ("o5", "omoda", "ev3"))):
        return "当前缺少目标车型在瑞典的可引用上市状态、销量、官方 MSRP 和配置/版本证据；不能把上市状态或市场表现写成确定事实。"
    if ("未进入top" in normalized or "未进入 top" in normalized) and any(token in normalized for token in ("ev3", "ex30", "o5")):
        return "当前缺少目标竞品的可引用销量/份额、官方 MSRP 和配置证据；不能用 top model 榜单缺失判断其需求强弱。"
    if "数据截至2025" in text or "数据截至 2025" in text or "data through 2025" in normalized:
        return "时间口径应以 EvidencePackage 的来源日期或 JATO 数据版本为准；没有可追溯来源日期时，不写确定数据截止月份。"
    if "ex60" in normalized and "ex40" in normalized and ("推断" in text or "infer" in normalized):
        return "EX60 缺少直接销量、价格和配置证据，不能用 EX40 替代或推断 EX60 需求。"
    if "ex60" in normalized and "尚未上市" in text:
        return "EX60 缺少可引用的瑞典上市状态、定价和销量证据，不能确认其上市状态或价格位置。"
    if "行业认知" in text or "industry knowledge" in normalized:
        if "xc60" in normalized and ("msrp" in normalized or "价格" in text or "定价" in text):
            return "定价数据中未直接返回 XC60 MSRP，不能用行业认知给出确定价格区间。"
        return "缺少可引用来源时，不能把行业认知写成已验证事实。"
    return text


def _business_review_clean_content_line(value: str) -> str:
    text = _sanitize_business_review_line(value)
    if not text:
        return ""
    if _business_review_is_runtime_line(text):
        return ""
    text = re.sub(r"^(Verdict|Why|So What|Action|Risk|Evidence|Limitations?)：\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(Verdict|Why|So What|Action|Risk|Evidence|Limitations?):\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(证据|可引用证据)\s*[：:]\s*", "", text)
    text = text.replace("Pricing corridor playbook：", "")
    text = text.replace("定价走廊方法：", "")
    text = text.replace("Competitor positioning playbook：", "")
    text = text.replace("Market opportunity playbook：", "")
    text = text.replace("Policy impact playbook：", "")
    text = text.replace("VOC evidence playbook：", "")
    text = text.replace("PPT-ready report playbook：", "")
    text = text.replace("Configuration value playbook：", "")
    text = text.replace("Inventory / BOM playbook：", "")
    text = text.replace("方法样例：", "定价方法：")
    text = text.replace("方法样例", "材料口径")
    if re.search(r"\bplaybook\b", text, flags=re.IGNORECASE):
        return ""
    if text.startswith("直接结论："):
        return ""
    if "外部搜索" in text and "尚未形成可引用" in text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _business_review_is_runtime_line(value: str) -> bool:
    text = _text(value)
    runtime_markers = (
        "本轮工具链已经覆盖",
        "下一步应补齐缺失证据后再收敛结论",
        "已覆盖 query_",
        "已覆盖 external_research",
        "已覆盖 compare_",
        "当前结论需要同时保留内部结构化数据和外部研究边界",
    )
    return any(marker in text for marker in runtime_markers)


def _business_review_line_role(value: str) -> str:
    text = _text(value).strip()
    if re.match(r"^P[0-3]\s*[·:：-]", text, flags=re.IGNORECASE):
        return "action"
    if re.match(r"^(建议动作|下一步动作|下一步执行|Next action)\s*[：:]", text, flags=re.IGNORECASE):
        return "action"
    if re.match(r"^(结论|直接结论|判断)\s*[：:]", text):
        return "verdict"
    if re.match(r"^(证据有限但可推进|风险边界|限制|证据边界|证据缺口|Limitations?)\s*[：:]", text, flags=re.IGNORECASE):
        return "risk"
    if re.match(r"^(展示|展示骨架|输出视图|Display)\s*[：:]", text, flags=re.IGNORECASE):
        return "display"
    if re.match(r"^(用户价值|产品经理判断|业务含义|So What)\s*[：:]", text, flags=re.IGNORECASE):
        return "so_what"
    if re.match(r"^(产品动作|Product action)\s*[：:]", text, flags=re.IGNORECASE):
        return "product_implication"
    if re.match(r"^(证据|可引用证据)\s*[：:]", text):
        return "evidence"
    if re.match(r"^Product implication\s*[：:]", text, flags=re.IGNORECASE):
        return "product_implication"
    if re.match(r"^Key message\s*[：:]", text, flags=re.IGNORECASE):
        return "key_message"
    if re.match(r"^Title\s*[：:]", text, flags=re.IGNORECASE):
        return "title"
    match = re.match(r"^(Verdict|Why|So What|Action|Risk|Evidence|Limitations?)\s*[：:]", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).lower().replace(" ", "_")


def _business_review_prefixed_lines(lines: list[str], role: str) -> list[str]:
    wanted = role.lower()
    matched: list[str] = []
    for line in lines:
        if _business_review_line_role(line) == wanted:
            matched.append(line)
    return _dedupe_string_list(matched)


def _should_prefer_question_specific_business_direct(answer: dict[str, Any], direct: str) -> bool:
    question = _text(answer.get("question")).lower()
    if _business_review_is_voc_frequency_question_with_sources(answer):
        return True
    if "j7" in question and "hev" in question and any(token in question for token in ("定价", "价格", "pricing")):
        return True
    if "j7" in question and "hev" in question and any(token in question for token in ("适合", "机会", "为什么", "fit", "opportunity")):
        return True
    if "leasing" in question and "phev" in question:
        return True
    if "company car" in question and "bev" in question and "phev" in question:
        return True
    if any(token in question for token in ("bom", "物料", "内外饰", "颜色", "可编辑数量", "选品表", "pi", "分市场")):
        return True
    return False


def _business_review_is_voc_frequency_question_with_sources(answer: dict[str, Any]) -> bool:
    question = _text(answer.get("question")).lower()
    if not any(
        token in question
        for token in (
            "高频",
            "用户声音",
            "voc",
            "拖车",
            "tow",
            "roof",
            "冬季胎",
            "winter tyre",
            "winter tire",
        )
    ):
        return False
    if not _business_answer_has_missing_evidence(answer, "voc_frequency_or_representativeness"):
        return False
    if _business_answer_has_missing_evidence(answer, "external_research_claims_unavailable"):
        return False
    if _business_answer_has_missing_evidence(answer, "minimum_external_sources"):
        return False
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    if _string_list(answer.get("evidenceDigest")):
        return True
    return _has_citation_ready_external_voc_evidence(evidence_package, question=_text(answer.get("question")))


def _looks_like_generic_business_review_direct(value: str) -> bool:
    text = _text(value)
    return any(
        marker in text
        for marker in (
            "这题需要先给业务立场",
            "市场数据要落到机会 segment",
            "业务分析 已有可追溯证据支撑",
            "已有可追溯证据支撑，当前最重要的业务含义是",
            "把证据转成业务动作、风险边界和可复用汇报结构",
            "政策/新闻分析必须先确认来源日期",
            "库存/BOM 问题应先建实体关系",
            "BOM/库存问题应先建立实体关系",
        )
    )


def _guard_unsupported_generic_business_review_direct(answer: dict[str, Any], direct: str) -> str:
    if not _looks_like_generic_business_review_direct(direct):
        return direct
    if _business_review_has_supporting_evidence(answer):
        return direct
    question = _text(answer.get("question"))
    subject = f"“{question}”" if question else "这个问题"
    return (
        f"直接结论：当前记录没有可引用证据支撑{subject}，不能把题面判断写成业务结论；"
        "应先补齐对应工具结果、证据表或来源后再评分。"
    )


def _refresh_stale_business_review_direct(answer: dict[str, Any], direct: str) -> str:
    question = _text(answer.get("question")).casefold()
    if "o5" not in question or "ev3" not in question:
        return direct
    if "当前价格样本显示" not in direct:
        return direct
    evidence_lines = _string_list(answer.get("evidenceDigest"))
    has_requested_price_gap = "当前仍缺 O5/EV3 官方 MSRP" in direct or any(
        "本题车型官方 MSRP" in line and "待补" in line
        for line in evidence_lines
    )
    if not has_requested_price_gap:
        return direct
    return re.sub(
        r"当前价格样本显示：([^。]+)。",
        r"非本题核心车型的已物化价格背景：\1；只能作为价格环境参考，不能当作 O5/EV3 官方 MSRP 或竞品价格走廊。",
        direct,
        count=1,
    )


def _business_review_has_supporting_evidence(answer: dict[str, Any]) -> bool:
    digest_lines = _string_list(answer.get("evidenceDigest"))
    if any(_business_review_line_has_positive_evidence(line) for line in digest_lines):
        return True
    evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
    return _evidence_ref_count(evidence_package) > 0


def _business_review_line_has_positive_evidence(value: str) -> bool:
    text = _text(value)
    if not text:
        return False
    lowered = text.casefold()
    if any(
        marker in lowered
        for marker in (
            "待补",
            "缺少",
            "缺口",
            "补证线索",
            "不能替代",
            "missing",
            "unavailable",
            "insufficient",
        )
    ):
        return False
    return "=" in text or "（" in text or "(" in text


def _question_specific_business_review_direct(answer: dict[str, Any]) -> str:
    question = _text(answer.get("question")).lower()
    if not question:
        return ""
    if "j7" in question and "hev" in question and any(token in question for token in ("定价", "价格", "pricing")):
        return _business_review_j7_pricing_direct_from_evidence(answer)
    if "hev" in question and "j7" in question and any(token in question for token in ("适合", "机会", "为什么")):
        return _business_review_j7_hev_market_direct_from_evidence(answer)
    if _business_review_is_voc_frequency_question_with_sources(answer):
        market = _side_by_side_country_display_name(_text(answer.get("country") or "Sweden"))
        return (
            f"直接结论：已有可追溯外部来源支持 {market} roof load、拖车/载物和冬季实用性是候选用户主题，"
            "但当前证据只能证明“可验证候选”，不能证明北欧用户已经形成高频需求。"
            "下一步应补车主/论坛/经销端样本频次和代表性，再决定是否写进主销配置话术。"
        )
    return ""


def _business_review_j7_pricing_direct_from_evidence(answer: dict[str, Any]) -> str:
    """Only project the J7 pricing method when the review record has matching evidence."""
    country_label = _side_by_side_country_display_name(_text(answer.get("country") or ""))
    question = _text(answer.get("question"))
    if country_label and country_label != "瑞典":
        return ""
    evidence_lines = _string_list(answer.get("evidenceDigest"))
    if not evidence_lines:
        evidence_package = answer.get("evidencePackage") if isinstance(answer.get("evidencePackage"), dict) else {}
        evidence_lines = _side_by_side_evidence_digest_from_package(evidence_package, question=question)
    joined = "\n".join(_text(line) for line in evidence_lines)
    if not joined:
        return ""
    main_trim = (
        _business_review_metric_from_line(joined, "J7 HEV 主销高配价格")
        or _business_review_metric_from_line(joined, "J7 HEV user material main trim MSRP")
    )
    corridor = (
        _business_review_metric_from_line(joined, "J7 HEV 竞品价格带")
        or _business_review_metric_from_line(joined, "J7 HEV user material competitor corridor")
    )
    price_gap = (
        _business_review_metric_from_line(joined, "J7 HEV 高低配价差")
        or _business_review_metric_from_line(joined, "J7 HEV user material price gap")
    )
    pva = (
        _business_review_metric_from_line(joined, "J7 HEV 高配 PVA 覆盖率")
        or _business_review_metric_from_line(joined, "J7 HEV user material PVA coverage")
    )
    positioning = (
        _business_review_metric_from_line(joined, "J7 HEV 定价定位")
        or _business_review_metric_from_line(joined, "J7 HEV user material positioning")
    )
    if not any((main_trim, corridor, price_gap, pva, positioning)):
        return ""
    market = country_label or "瑞典"
    evidence_parts: list[str] = []
    if positioning:
        evidence_parts.append(f"定位 {positioning}")
    if corridor:
        evidence_parts.append(f"竞品价格带 {corridor}")
    if main_trim:
        evidence_parts.append(f"主销高配 {main_trim}")
    if price_gap:
        evidence_parts.append(f"高低配价差 {price_gap}")
    if pva:
        evidence_parts.append(f"PVA 覆盖 {pva}")
    return (
        f"直接结论：{market} J7 HEV 定价可以把用户材料作为验证假设，而不是直接当作当前官方价格结论。"
        f"可引用证据显示：{'，'.join(evidence_parts[:5])}。"
        "业务含义是先用价格走廊和高配价值证明低配锚点/高配主推逻辑，再补当前官方 MSRP、竞品月供/RV、促销和配置交叉验证。"
    )


def _business_review_j7_hev_market_direct_from_evidence(answer: dict[str, Any]) -> str:
    evidence_lines = _string_list(answer.get("evidenceDigest"))
    joined = "\n".join(evidence_lines)
    hev_sales = _business_review_metric_from_line(joined, "HEV 动力销量")
    if not hev_sales:
        return ""
    market = _side_by_side_country_display_name(_text(answer.get("country") or "Sweden"))
    bev_sales = _business_review_metric_from_line(joined, "BEV 动力销量")
    phev_sales = _business_review_metric_from_line(joined, "PHEV 动力销量")
    market_window = _business_review_metric_from_line(joined, "J7 HEV 市场窗口")
    competitor_pool = _business_review_metric_from_line(joined, "J7 HEV 竞品池")
    context_parts = [f"HEV 动力销量 {hev_sales}"]
    if bev_sales:
        context_parts.append(f"BEV {bev_sales}")
    if phev_sales:
        context_parts.append(f"PHEV {phev_sales}")
    context = "，".join(context_parts)
    method_sentence = ""
    if market_window:
        method_sentence = f"用户 J7_HEV 方法论补充了车型级进入窗口：{market_window}。"
        if competitor_pool:
            method_sentence = f"{method_sentence} 竞品验证应先围绕 {competitor_pool} 做价格/配置矩阵。"
    return (
        f"直接结论：{market} HEV 市场对 J7 是值得继续验证的机会，但不能只靠国家级总量写成已确认进入结论。"
        f"可引用证据显示 {context}；这说明 HEV 有可量化需求池，同时 BEV/PHEV 压力也必须进入定位判断。"
        f"{method_sentence}"
        "下一步应补 HEV + SUV A0/A 结构、主销驱动形式、车型级竞品、当前价格和配置价值证据，再判断 J7 是否能形成可销售的进入点。"
    )


def _business_review_leasing_tco_direct_from_evidence(evidence_lines: list[str]) -> str:
    joined = "\n".join(_text(line) for line in evidence_lines)
    if "PHEV 注册量" not in joined and "PHEV 公司车注册占比" not in joined:
        return ""
    phev_sales = _business_review_metric_from_line(joined, "PHEV 注册量")
    phev_business = _business_review_metric_from_line(joined, "PHEV 公司车注册占比")
    phev_private = _business_review_metric_from_line(joined, "PHEV 私人注册占比")
    metric_parts = []
    if phev_business:
        metric_parts.append(f"公司车注册占比 {phev_business}")
    if phev_sales:
        metric_parts.append(f"PHEV 注册量 {phev_sales}")
    if phev_private:
        metric_parts.append(f"私人注册占比 {phev_private}")
    metric_text = "，".join(metric_parts)
    if not metric_text:
        return ""
    return (
        f"直接结论：大客户 leasing 场景下 PHEV 已有公司车暴露信号：{metric_text}。"
        "这支持把 PHEV 保留为 fleet/TCO 验证线，但当前仍缺月供、残值、税务 benefit 和大客户口径，"
        "不能直接证明 PHEV 应主推。"
    )


def _business_review_company_car_direct_from_evidence(question: str, evidence_lines: list[str]) -> str:
    question_text = _text(question).casefold()
    if "company car" not in question_text or "bev" not in question_text or "phev" not in question_text:
        return ""
    joined = "\n".join(_text(line) for line in evidence_lines)
    bev_sales = _business_review_metric_from_line(joined, "BEV 注册量")
    bev_business = _business_review_metric_from_line(joined, "BEV 公司车注册占比")
    phev_sales = _business_review_metric_from_line(joined, "PHEV 注册量")
    phev_business = _business_review_metric_from_line(joined, "PHEV 公司车注册占比")
    if not any((bev_sales, bev_business, phev_sales, phev_business)):
        return ""
    metric_parts = []
    if bev_business:
        metric_parts.append(f"BEV 公司车注册占比 {bev_business}")
    if phev_business:
        metric_parts.append(f"PHEV 公司车注册占比 {phev_business}")
    if bev_sales:
        metric_parts.append(f"BEV 注册量 {bev_sales}")
    if phev_sales:
        metric_parts.append(f"PHEV 注册量 {phev_sales}")
    return (
        f"直接结论：瑞典 company car benefit 对 BEV/PHEV 的差异已有渠道证据：{'，'.join(metric_parts[:4])}。"
        "BEV 更适合低使用成本和稳定充电条件下的公司车主线；PHEV 因公司车依赖仍高，可保留长途、无稳定充电或过渡型 fleet 场景。"
        "但当前仍缺 benefit tax、月供、残值、年里程和充电条件口径，不能直接证明哪条动力路线 TCO 更优。"
    )


def _business_review_leasing_tco_missing_line(question: str) -> str:
    text = _text(question).casefold()
    if "company car" in text and "bev" in text and "phev" in text:
        return (
            "当前缺少 leasing/TCO/残值或 company-car benefit 证据；市场规模或泛政策新闻只能作为背景，"
            "不能证明 BEV/PHEV company car benefit 差异、月供/残值或 TCO 优劣。"
        )
    if ("co₂" in text or "co2" in text or "税率" in text) and "phev" in text:
        return (
            "当前缺少 leasing/TCO/残值或 company-car benefit 证据；市场规模或泛政策新闻只能作为背景，"
            "不能证明 CO2 税率阶梯对 PHEV 的实际 TCO 或公司车税费优势。"
        )
    return (
        "当前缺少 leasing/TCO/残值或 company-car benefit 证据；MSRP、市场规模或泛政策新闻只能作为背景，"
        "不能证明大客户 leasing 场景下 PHEV 是否成立。"
    )


def _business_review_leasing_tco_missing_direct(question: str) -> str:
    text = _text(question).casefold()
    if "company car" in text and "bev" in text and "phev" in text:
        return (
            "直接结论：当前缺少 leasing/TCO/月供、残值或 company-car benefit 证据，"
            "不能直接证明 BEV/PHEV 哪条动力路线 TCO 更优；只能把 channel mix 当作暴露信号，"
            "下一步要补官方 benefit formula、月供/RV 和使用场景。"
        )
    return (
        "直接结论：当前缺少 leasing/TCO/月供、残值或 company-car benefit 证据，"
        "不能证明 PHEV 在大客户 leasing 场景下已经成立；只能把 PHEV leasing 作为待验证保留线，"
        "下一步要补税务 benefit、月供/RV、长途里程和充电条件。"
    )


def _business_review_is_fleet_leasing_question(question: str) -> bool:
    text = _text(question).casefold()
    if "company car" in text and "bev" in text and "phev" in text:
        return False
    return "phev" in text and any(token in text for token in ("leasing", "fleet", "大客户", "月供", "残值", "tco"))


def _business_review_configuration_direct_from_evidence(question: str, evidence_lines: list[str]) -> str:
    question_text = _text(question).casefold()
    if not any(token in question_text for token in ("80kwh", "80 kwh", "95kwh", "95 kwh", "800v", "冬季包", "配置")):
        return ""
    joined = "\n".join(_text(line) for line in evidence_lines)
    metric_parts = []
    for label in (
        "SUV A 4WD 占比",
        "SUV A BEV 渗透率",
        "SUV A PHEV 渗透率",
        "SUV A0 2WD 占比",
        "SUV A0 4WD 占比",
        "SUV A0 细分销量",
        "SUV A 细分销量",
        "BEV 公司车注册占比",
        "PHEV 公司车注册占比",
        "瑞典 BEV 动力销量",
        "挪威 BEV 动力销量",
        "芬兰 BEV 动力销量",
        "丹麦 BEV 动力销量",
        "BEV 动力销量",
        "PHEV 动力销量",
        "HEV 动力销量",
    ):
        value = _business_review_metric_from_line(joined, label)
        if value:
            metric_parts.append(f"{label} {value}")
    if not metric_parts:
        return ""
    if "80kwh" in question_text or "80 kwh" in question_text:
        topic = "A0/A SUV BEV 不应全系强推 80kWh；80kWh 应作为长续航/高配安全边界放在高配或长续航版本里验证"
        implication = "先用 80kWh、热泵、电池预热、快充和冬季舒适配置打高配价值，低配继续保价格锚点。"
    elif "95kwh" in question_text or "95 kwh" in question_text or "800v" in question_text:
        topic = "95kWh + 双电机 + 800V 应定位为高价值家庭/公司车 BEV 架构，而不是单纯堆参数"
        implication = "95kWh 负责冬季真实续航和长途余量，双电机负责湿滑路面牵引，800V 负责补能效率和 fleet 使用效率。"
    elif "冬季包" in question_text:
        topic = "北欧冬季包应按 must-have / visible value / optional 分层，不应只罗列舒适配置"
        implication = "Must-have 先覆盖热泵、电池预热、座椅/方向盘加热、除霜、冬季胎/TPMS 和充电预热，再把拖车钩、roof load、远程预热等做成可见价值。"
    else:
        topic = "配置方案应先落到用户场景、版本策略和竞品价值差，而不是只看装备清单"
        implication = "把配置拆成 must-have、visible value、optional 和 cost/risk，再决定主销版本。"
    return (
        f"直接结论：{topic}。"
        f"证据支撑：{'，'.join(metric_parts[:3])}。"
        f"业务含义：{implication}"
        "证据边界：这些市场数据只能支撑配置方向和用户场景判断，不能单独证明电池、续航、充电、冬季包、价格或竞品配置矩阵已经成立；"
        "因此当前应输出可验证的产品定义和配置验证表，而不是写成已证明的标配结论。"
    )


def _business_review_is_configuration_market_context_line(value: str) -> bool:
    text = _text(value)
    return any(token in text for token in ("细分销量", "动力销量", "渗透率", "公司车注册占比", "4WD 占比"))


def _business_review_is_leasing_tco_context_line(value: str) -> bool:
    text = _text(value)
    lowered = text.casefold()
    if not text:
        return False
    if any(token in lowered for token in ("ensun", "rankseed", "car leasing companies", ".source")):
        return False
    return any(
        token in text
        for token in (
            "leasing/TCO/company-car 证据",
            "PHEV 注册量",
            "PHEV 公司车注册占比",
            "PHEV 私人注册占比",
            "BEV 公司车注册占比",
            "BEV 私人注册占比",
            "月供",
            "残值",
            "税务 benefit",
            "company-car benefit",
            "TCO",
            "Bonus policy date",
            "Transportstyrelsen",
            "low emission vehicles",
        )
    )


def _business_review_metric_line_key(value: str) -> str:
    text = _text(value)
    text = re.sub(r"（[^）]*）", "", text)
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.casefold()


def _business_review_j8_sorento_direct_from_evidence(question: str, evidence_lines: list[str]) -> str:
    question_text = _text(question).casefold()
    if "j8" not in question_text or "sorento" not in question_text:
        return ""
    joined = "\n".join(_text(line) for line in evidence_lines)
    metric_parts = []
    for label in (
        "SUV A 4WD 占比",
        "SUV A 四驱占比",
        "SUV A 细分销量",
        "SUV B 4WD 占比",
        "PHEV 4WD 占比",
        "SUV A PHEV 渗透率",
        "SUV B PHEV 渗透率",
        "PHEV 公司车注册占比",
        "SUV A 公司车注册占比",
        "PHEV 注册量",
    ):
        value = _business_review_metric_from_line(joined, label)
        if value:
            metric_parts.append(f"{label} {value}")
    if not metric_parts:
        return ""
    return (
        f"直接结论：J8 7 座四驱对 Sorento 的打法有市场场景支撑：{'，'.join(metric_parts[:4])}。"
        "这说明卖点应落在 7 座、四驱、家庭/公司车、长途和高配价值组合，而不是只说尺寸。"
        "但当前仍缺 J8/Sorento 官方 MSRP、月供/RV 和逐项配置差异，不能写成已验证胜出。"
    )


def _business_review_metric_from_line(value: str, label: str) -> str:
    match = re.search(rf"(?:^|[\n\r])(?:[-*]\s*)?{re.escape(label)}\s*=\s*([^（\n]+)", value)
    if not match:
        return ""
    return match.group(1).strip()


def _business_review_section(title: str, lines: list[str], *, limit: int) -> list[str]:
    cleaned = [
        line
        for line in (_sanitize_business_review_line(_text(line))[:420] for line in lines)
        if line
    ]
    if not cleaned:
        return []
    return [f"## {title}\n" + "\n".join(f"- {line}" for line in cleaned[:limit])]


def _sanitize_business_review_line(value: str) -> str:
    text = _text(value)
    if not text:
        return ""
    replacements = {
        "Business Composer: evidence alignment is partially_aligned.": "证据对齐：部分对齐。",
        "Business Composer: evidence alignment is aligned.": "证据对齐：证据一致。",
        "Business Composer: evidence alignment is conflicting.": "证据对齐：证据冲突。",
        "Business Composer: evidence alignment is insufficient.": "证据对齐：证据不足。",
        "Business Composer:": "业务答案生成：",
        "Grounding guard:": "证据安全检查：",
        "Missing evidence:": "证据缺口：",
        "published_date": "来源发布日期",
        "external_research_claims_unavailable": "外部来源结论不足",
        "source_repair_candidates": "价格来源修复候选",
        "external_policy_source_candidates": "政策官方来源候选",
        "external policy source candidates": "政策官方来源候选",
        "candidate_search_query": "候选搜索查询",
        "candidate search query": "候选搜索查询",
        "decision_boundary": "决策边界",
        "weakens_answer": "会削弱结论",
        "source/date/count refs": "来源/日期/数量线索",
        "MSRP/current price coverage diagnostics found source repair candidates. Use them as the next data-repair queue before making numeric price claims.": "当前价格覆盖诊断已找到来源修复候选；先补数据再写确定价格数字。",
        "Map requested JATO model names to official MSRP model names or add missing current_price rows.": "把请求车型映射到官方 MSRP 车型名，或补齐缺失当前价格记录。",
        "竞品池中未包含J8，无法确认J8是否被定义为Sorento的直接竞品。": "当前缺少 J8/Sorento 直接对标关系、车型级销量/MSRP、配置差异和 TCO 证据；不能仅凭相邻竞品池判断已验证胜出。",
        "竞品池中未包含 J8，无法确认 J8 是否被定义为 Sorento 的直接竞品。": "当前缺少 J8/Sorento 直接对标关系、车型级销量/MSRP、配置差异和 TCO 证据；不能仅凭相邻竞品池判断已验证胜出。",
        "source repair candidates": "来源修复候选",
        "data-repair queue": "数据修复队列",
        "numeric price claims": "确定价格数字",
        "no supported claim or source-backed business finding was available": "尚未形成可引用的业务结论",
        "no citation-ready source-backed claim evidence": "尚未形成可引用的来源结论",
        "No citation-ready external source evidence": "没有可直接引用的外部来源证据",
        "External research returned only": "外部搜索目前只返回",
        "External research was required or attempted, but it returned": "外部搜索已执行，但目前只返回",
        "JATO historical data is not a direct validator for this policy/news claim. External research can be cross-checked against JATO market KPIs, top models, and powertrain mix.": "JATO 历史数据不能直接证明政策/新闻事实，但可以用市场 KPI、主销车型和动力结构做交叉验证。",
        "Research policy requires publish dates for time-sensitive policy or news claims.": "时效性政策/新闻结论需要发布日期。",
        "External web sources are citation candidates and should be cross-checked against JATO structured data for numeric market claims.": "外部网页来源只是引用候选，涉及市场数字时必须与 JATO 结构化数据交叉验证。",
        "Tavily advanced research was unavailable; fallback search providers were used.": "Tavily 高级研究不可用，本轮使用了备用搜索来源。",
        "No source URL was returned, so the answer must not claim current external facts.": "未返回来源 URL，因此不能声称已验证当前外部事实。",
        "Source score is relevance/authority, not fact confidence; fact confidence still depends on source content and cross-checks.": "来源分数代表相关性/权威性，不等同于事实置信度；事实仍需看原文内容和交叉验证。",
        "Source score is relevance/authority, not fact confidence; fact confidence also depends on policy fit and JATO cross-check.": "来源分数代表相关性/权威性，不等同于事实置信度；事实仍需看政策适配性和 JATO 交叉验证。",
        "Use this source to decide what needs official-source confirmation before making policy claims": "先确认官方来源、发布日期和适用车型/资格",
        "Use this source as external context, then anchor the market conclusion to internal JATO sales/share evidence": "外部信息只作背景，市场结论仍要回到 JATO 销量/份额证据",
        "Use this source to validate price corridor, monthly payment, or competitor positioning before making a firm pricing recommendation": "先验证价格走廊、月供或竞品定位，再给确定定价建议",
        "Use this source as a citation candidate and cross-check with internal evidence": "作为候选来源，并与内部证据交叉验证",
        "这题需要先给业务立场，再展开证据；当前判断是 ": "",
        "这题需要先给业务立场": "需要先给出明确业务立场",
        "Next: confirm official source, publish date, and affected vehicle eligibility": "确认官方来源、发布日期和受影响车型资格",
        "Next: quantify affected segments and generate an opportunity view": "量化受影响细分市场并生成机会视图",
        "Next: compare official/dealer/leasing prices with JATO pricing and competitor corridor": "对比官方/经销商/租赁价格与 JATO 价格走廊",
        "Next: turn the cited claims into a one-page deck section with limitations": "把引用结论整理成一页汇报并写明限制",
        "Next: validate the claim and attach the best available citation": "验证结论并附上最佳可用来源",
        "partially_aligned": "部分对齐",
        "aligned": "证据一致",
        "conflicting": "证据冲突",
        "insufficient": "证据不足",
        "evidenceRefs": "可引用证据",
        "evidenceRef": "可引用证据",
        "confidence high": "置信度高",
        "confidence medium": "置信度中",
        "confidence low": "置信度低",
        "置信度 high": "置信度高",
        "置信度 medium": "置信度中",
        "置信度 low": "置信度低",
        "mitigation:": "建议：",
        "coverage_diagnostic:no_current_prices_for_requested_models": "价格覆盖诊断：缺少请求车型当前价格",
        "coverage_diagnostic:no_current_prices_for_country": "价格覆盖诊断：缺少该国家当前价格",
        "coverage_diagnostic:no_config_projects_for_country": "配置覆盖诊断：该国家缺少工程配置项目",
        "coverage_diagnostic:no_config_subjects_for_requested_models": "配置覆盖诊断：请求车型未映射到配置主体",
        "coverage_diagnostic:insufficient_compare_subjects": "配置覆盖诊断：比较对象不足",
        "compare_vehicle_variants": "配置差异工具",
        "MSRP source validation table": "MSRP 来源验证表",
        "MSRP source repair table": "MSRP 来源验证表",
        "MSRP 来源 repair table": "MSRP 来源验证表",
        "MSRP source": "MSRP 来源",
        "competitor current price available own model missing": "已有竞品当前价格，本车型来源缺失",
        "source draft only not price evidence": "来源草稿尚未转成价格证据",
        "jato_msrp_postgres": "JATO MSRP 数据",
        "jato_price_positioning": "JATO 价格样本",
        "jato_country_chart_deck": "JATO 图表数据",
        "jato_country_snapshot": "JATO 市场快照",
        "jato_cross_country": "JATO 跨国对比",
        "jato_cross_reference": "JATO 交叉引用",
        "jato_filtered_query": "JATO 筛选查询",
        "user_question": "用户问题",
        "current_price rows": "当前价格记录",
        "current_price row": "当前价格记录",
        "current_price 行": "当前价格记录",
        "current_price": "当前价格记录",
        "current MSRP": "当前官方 MSRP",
        "accepted current MSRP": "已审核当前官方 MSRP",
        "Pricing reference sample chart": "价格参考样本图",
        "Pricing evidence table": "价格证据表",
        "current_prices table": "当前价格记录",
        "current_prices": "当前价格记录",
        "current_msrp": "官方 MSRP 交叉验证",
        "own_model_price": "本车型价格",
        "material_number": "物料号",
        "material_code": "物料编码",
        "available_units": "可用数量",
        "lifecycle_status": "生命周期状态",
        "editable_quantity": "可编辑数量",
        "TCO / company-car validation table": "TCO/company-car 验证表",
        "TCO / company car validation table": "TCO/company-car 验证表",
        "External source validation matrix": "外部来源验证矩阵",
        "External source repair table": "外部来源验证矩阵",
        "外部来源修复表": "外部来源验证矩阵",
        "VOC framework table": "VOC 验证框架表",
        "PPT-ready block": "PPT-ready 汇报块",
        "已尝试工具": "本轮已查数据源",
        "这些搜索候选只是补源入口": "这些候选只是补证线索",
        "这些候选只是补源入口": "这些候选只是补证线索",
        "这些检索线索只是补源入口": "这些检索线索只是补证线索",
        "补源线索": "补证线索",
        "补源入口": "补证线索",
        "补证入口": "补证线索",
        "当前结论需要同时保留内部结构化数据和外部研究边界。": "",
        "当前结论需要同时保留内部结构化数据和外部研究边界": "",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = text.replace("accepted 当前官方 MSRP", "已审核当前官方 MSRP")
    text = re.sub(
        r"(?<![A-Za-z0-9_.-])([A-Za-z0-9][A-Za-z0-9 _/-]{0,60})\.sales\b",
        lambda match: f"{re.sub(r'\\s+', ' ', match.group(1).replace('_', ' ')).strip()} 销量",
        text,
    )
    text = re.sub(
        r"\bJ7 HEV visible feature value\.([^=\n。；;]+)",
        lambda match: f"J7 HEV 可见配置价值：{match.group(1).strip()}",
        text,
    )
    text = _rewrite_business_review_context_ref_labels(text)
    text = _localize_business_review_country_text(text)
    text = _rewrite_business_review_display_line(text)
    text = re.sub(r"\b(\d+)\s+个\s+可引用证据\b", r"\1 条可引用证据", text)
    text = re.sub(r"\b(\d+)\s+个\s+evidence\s*ref(?:s)?\b", r"\1 条可引用证据", text, flags=re.IGNORECASE)
    text = re.sub(r"\bown model/current MSRP\b", "本车型/当前 MSRP", text, flags=re.IGNORECASE)
    text = re.sub(r"证据对齐\s+(部分对齐|证据一致|证据冲突|证据不足)", r"证据状态：\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _rewrite_business_review_context_ref_labels(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    ref_pattern = re.compile(
        r"\b("
        r"crossCountry\.[^.()\s，。；;：:]+(?:\.(?:powertrainMix|动力类型Mix)\.[^.()\s，。；;：:]+\.(?:sales|value|share)|\.kpis\.cumulativeSales)"
        r"|(?:contextSnapshot|marketSnapshot)\.(?:powertrainMix|动力类型Mix)\.[^.()\s，。；;：:]+\.(?:sales|value|share)"
        r")",
        flags=re.IGNORECASE,
    )

    def replace_ref(match: re.Match[str]) -> str:
        raw_label = match.group(1)
        display_label = _side_by_side_context_ref_label(raw_label)
        return display_label or raw_label

    return ref_pattern.sub(replace_ref, text)


def _rewrite_business_review_display_line(value: str) -> str:
    text = str(value or "").strip()
    if not re.match(r"^(展示|展示骨架|Display)\s*[：:]", text, flags=re.IGNORECASE):
        if not re.search(r"(展示骨架|Display)\s*[：:]", text, flags=re.IGNORECASE):
            return text
        return re.sub(
            r"((?:展示骨架|Display)\s*[：:][^。！？!?]*(?:[。！？!?]|$))",
            lambda match: _rewrite_business_review_display_line(match.group(1)),
            text,
            flags=re.IGNORECASE,
        )
    return _rewrite_business_review_display_segment(text)


def _rewrite_business_review_display_segment(value: str) -> str:
    text = str(value or "").strip()
    if not re.match(r"^(展示|展示骨架|Display)\s*[：:]", text, flags=re.IGNORECASE):
        return text
    text = re.sub(r"^(展示|展示骨架|Display)\s*[：:]\s*", "输出视图：", text, flags=re.IGNORECASE)
    title_replacements = {
        "Pricing corridor chart": "价格走廊图",
        "Pricing evidence table": "价格证据表",
        "MSRP source validation table": "MSRP 来源验证表",
        "MSRP source repair table": "MSRP 来源验证表",
        "Competitor comparison table": "竞品对比矩阵",
        "Competitor sales chart": "竞品销量图",
        "Competitor share chart": "竞品份额图",
        "Competitor price chart": "竞品价格图",
        "Market structure chart": "市场结构图",
        "Powertrain mix chart": "动力结构图",
        "Market decision table": "市场决策表",
        "Policy / news evidence table": "政策/新闻证据表",
        "Configuration validation matrix": "配置验证矩阵",
        "Inventory / BOM evidence table": "库存/BOM 证据表",
        "BOM / entity mapping validation table": "BOM/实体映射验证表",
        "TCO / company-car validation table": "TCO/company-car 验证表",
        "External source validation matrix": "外部来源验证矩阵",
        "External source repair table": "外部来源验证矩阵",
        "VOC framework table": "VOC 验证框架表",
        "VOC evidence table": "VOC 证据表",
        "PPT-ready block": "PPT-ready 汇报块",
        "Report evidence appendix": "报告证据附录",
        "Key metrics": "关键指标卡",
    }
    for source, target in title_replacements.items():
        text = text.replace(source, target)
    text = text.replace("先看 ", "已生成 ")
    text = text.replace("再用 ", "并用 ")
    text = text.replace("判断", "呈现")
    text = text.replace("补齐本车型/竞品官方价格来源", "列出本车型/竞品官方价格补源状态")
    text = text.replace("拆级别、动力类型、价格/配置差异和产品动作", "呈现级别、动力、价格/配置差异和产品动作")
    return text


def _localize_business_review_country_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"直接结论：Sweden(?=\s|的)", "直接结论：瑞典", text)
    text = re.sub(r"\bSweden\s+的\s*", "瑞典的", text)
    text = re.sub(r"\bSweden\s+汇报页", "瑞典汇报页", text)
    text = re.sub(r"\bSweden\s+市场", "瑞典市场", text)
    text = re.sub(r"\bSweden(?=\s+(?:累计销量|BEV|PHEV|HEV|ICE|MHEV|SUV|销量|动力销量))", "瑞典", text)
    text = re.sub(r"\bFinland(?=\s+(?:累计销量|BEV|PHEV|HEV|ICE|MHEV|SUV|销量|动力销量))", "芬兰", text)
    text = text.replace("Sweden/Finland", "瑞典/芬兰")
    text = text.replace("把 瑞典/芬兰", "把瑞典/芬兰")
    text = text.replace("side-by-side market table", "双边市场对比表")
    text = text.replace("top models", "主销车型")
    text = text.replace("把瑞典/芬兰 做成 双边市场对比表", "把瑞典/芬兰做成双边市场对比表")
    text = text.replace("和 主销车型", "和主销车型")
    text = re.sub(
        r"\bSweden\s+(?=(J7|J8|O5|O9|OMODA|JAECOO|BEV|PHEV|VOC|company|Elbilspremien|目标价|政策|用户|汇报|配置|竞品|市场))",
        "瑞典 ",
        text,
        flags=re.IGNORECASE,
    )
    text = text.replace("瑞典 的", "瑞典的")
    text = text.replace("直接结论：瑞典 目标价", "直接结论：瑞典目标价")
    return text


def _business_review_action_lines(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    lines: list[str] = []
    for item in value:
        if isinstance(item, dict):
            action = _text(item.get("action"))
            if not action:
                continue
            priority = _text(item.get("priority"))
            rationale = _text(item.get("rationale"))
            prefix = f"{priority} · " if priority else ""
            suffix = f" — {rationale}" if rationale else ""
            lines.append(f"{prefix}{action}{suffix}")
        else:
            line = _text(item)
            if line:
                lines.append(line)
    return _dedupe_string_list(lines)


def _business_review_actions_for_question(lines: list[str], *, question: str) -> list[str]:
    """Hide actions that belong exclusively to a different requested powertrain."""
    requested = _side_by_side_requested_powertrains(question)
    if not requested:
        return lines
    filtered: list[str] = []
    for line in lines:
        action_powertrains = _business_review_action_powertrains(line)
        if action_powertrains and action_powertrains.isdisjoint(requested):
            continue
        filtered.append(line)
    return filtered


def _business_review_action_powertrains(value: str) -> set[str]:
    text = _text(value).casefold()
    fuels = _side_by_side_requested_powertrains(text)
    if any(token in text for token in ("plug-in hybrid", "plug in hybrid", "plugin hybrid")):
        fuels.add("PHEV")
    elif re.search(r"(?<![a-z])hybrid(?![a-z])", text):
        fuels.add("HEV")
    return fuels


def _business_review_risk_lines(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    lines: list[str] = []
    for item in value:
        if isinstance(item, dict):
            name = _text(item.get("name")) or "missing_evidence"
            impact = _text(item.get("impact"))
            mitigation = _text(item.get("mitigation"))
            if impact and mitigation:
                lines.append(f"{name}: {impact}；mitigation: {mitigation}")
            elif impact:
                lines.append(f"{name}: {impact}")
        else:
            line = _text(item)
            if line:
                lines.append(line)
    return _dedupe_string_list(lines)


def _filter_redundant_review_lines(lines: list[str], *references: Any) -> list[str]:
    reference_signatures = [
        signature
        for reference in references
        for signature in _business_review_reference_signatures(reference)
    ]
    result: list[str] = []
    for line in lines:
        text = _text(line)
        if not text:
            continue
        signature = _business_review_line_signature(text)
        if _business_review_line_is_redundant(signature, reference_signatures):
            continue
        result.append(text)
    return _dedupe_string_list(result)


def _filter_redundant_evidence_review_lines(lines: list[str], *references: Any) -> list[str]:
    filtered = _filter_redundant_review_lines(lines, *references)
    for line in lines:
        text = _text(line)
        if not _business_review_is_source_metric_evidence_line(text):
            continue
        if text in filtered:
            continue
        filtered.append(text)
    return _dedupe_string_list(filtered)


def _business_review_is_source_metric_evidence_line(value: str) -> bool:
    text = _text(value)
    if "=" not in text:
        return False
    if not any(source in text for source in ("（JATO ", "（J7_HEV_", "（user_", "（J7_HEV")):
        return False
    return any(
        token in text
        for token in (
            "sales",
            "销量",
            "价格",
            "MSRP",
            "占比",
            "渗透率",
            "注册",
            "价差",
            "覆盖率",
        )
    )


def _business_review_reference_signatures(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = [value]
    return [
        signature
        for item in values
        if (signature := _business_review_line_signature(_text(item)))
    ]


def _business_review_line_signature(value: str) -> str:
    text = _sanitize_business_review_line(value)
    if not text:
        return ""
    text = re.sub(r"^#+\s*", "", text)
    text = re.sub(r"^[-*]\s*", "", text)
    text = re.sub(r"^P[0-3]\s*[·:：-]\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[（(]\s*\d+\s*条可引用证据\s*[）)]", "", text)
    text = re.sub(
        r"^(直接结论|配置判断|电池判断|用户场景|产品动作|建议动作|下一步动作?|关键证据|产品经理判断|汇报口径|证据边界|"
        r"Verdict|Why|So What|Action|Risk|Evidence|Limitations?)\s*[：:]\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[，。；：:、,.!！?？“”\"'（）()【】\[\]《》<>·\-_\/|]+", "", text)
    return text.lower()


def _business_review_line_is_redundant(signature: str, references: list[str]) -> bool:
    if not signature:
        return True
    for reference in references:
        if not reference:
            continue
        if signature == reference:
            return True
        if len(signature) >= 18 and (signature in reference or reference in signature):
            return True
        if min(len(signature), len(reference)) >= 28:
            similarity = SequenceMatcher(None, signature, reference).ratio()
            if similarity >= 0.86:
                return True
            match = SequenceMatcher(None, signature, reference).find_longest_match(
                0,
                len(signature),
                0,
                len(reference),
            )
            if match.size >= min(34, max(22, int(min(len(signature), len(reference)) * 0.72))):
                return True
        if _business_review_signature_overlap_ratio(signature, reference) >= 0.68:
            return True
    return False


def _business_review_signature_overlap_ratio(signature: str, reference: str) -> float:
    """Catch Chinese same-meaning review lines that differ by template wording."""
    if len(signature) < 24 or len(reference) < 24:
        return 0.0
    grams = _business_review_signature_bigrams(signature)
    reference_grams = _business_review_signature_bigrams(reference)
    if not grams or not reference_grams:
        return 0.0
    return len(grams & reference_grams) / len(grams)


def _business_review_signature_bigrams(value: str) -> set[str]:
    text = str(value or "")
    if len(text) < 2:
        return set()
    return {text[index:index + 2] for index in range(len(text) - 1)}


def _business_validation_projection(record: dict[str, Any]) -> dict[str, Any]:
    scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
    astrbot = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
    country = record.get("countryCopilot") if isinstance(record.get("countryCopilot"), dict) else {}
    return {
        "question": record.get("question", ""),
        "category": record.get("category", ""),
        "expectedIntent": record.get("expectedIntent", ""),
        "expectedTools": record.get("expectedTools", []),
        "astrbotAnswer": astrbot.get("answerPreview", ""),
        "copilotAnswer": country.get("answerPreview", ""),
        "astrbotEvidencePackage": astrbot.get("evidencePackage", {}),
        "astrbotVisualArtifacts": astrbot.get("visualArtifacts", []),
        "astrbotFollowUps": astrbot.get("followUps", []),
        "astrbotQualityScore": astrbot.get("qualityScore", {}),
        "astrbotEvidenceDigest": astrbot.get("evidenceDigest", []),
        "astrbotDisplayPlan": astrbot.get("displayPlan", ""),
        "astrbotScores": scoring.get("astrbotScores", {}),
        "copilotScores": scoring.get("countryCopilotScores", {}),
        "winner": scoring.get("winner", ""),
        "humanNotes": scoring.get("notes", ""),
        "failureTags": _string_list(record.get("failureTags")),
        "businessPlaybook": record.get("businessPlaybook", {}),
        "businessSynthesisPlan": astrbot.get("businessSynthesisPlan", {}),
        "recommendedActions": astrbot.get("recommendedActions", []),
        "reportReadyBullets": astrbot.get("reportReadyBullets", []),
        "businessSynthesisScore": astrbot.get("businessSynthesisScore", 0),
    }


def _apply_business_llm_judge(record: dict[str, Any]) -> None:
    judge_result = judge_side_by_side_with_llm(
        record=record,
        score_dimensions=list(_BUSINESS_SCORE_DIMENSIONS),
        failure_taxonomy=list(FAILURE_TAGS),
    )
    _apply_business_llm_judge_result(
        record,
        judge_result,
        store_unsuccessful=True,
    )


def _apply_business_llm_judge_result(
    record: dict[str, Any],
    judge_result: dict[str, Any],
    *,
    store_unsuccessful: bool,
) -> bool:
    if store_unsuccessful or judge_result.get("status") == "ok":
        record["llmJudge"] = judge_result
    if judge_result.get("status") != "ok":
        return False
    scores = judge_result.get("scores") if isinstance(judge_result.get("scores"), dict) else {}
    dimensions = list(_BUSINESS_SCORE_DIMENSIONS)
    astrbot_scores = _read_manual_scores(scores.get("astrbotScores"), dimensions)
    country_scores = _read_manual_scores(scores.get("countryCopilotScores"), dimensions)
    totals = _manual_score_totals(astrbot_scores, country_scores, dimensions)
    if totals.get("complete") is not True:
        return False
    winner = str(scores.get("winner") or "").strip()
    if not winner or winner not in _HUMAN_SCORE_WINNERS:
        winner = _winner_from_manual_totals(totals)
    record["humanScoring"] = {
        "status": "scored",
        "source": "llm_judge",
        "judgeProvider": judge_result.get("provider", {}),
        "dimensions": dimensions,
        "winner": winner,
        "notes": str(scores.get("notes") or "")[:4000],
        "astrbotScores": astrbot_scores,
        "countryCopilotScores": country_scores,
        "scoreTotals": totals,
        "updatedAt": _now_iso(),
    }
    inferred_tags = infer_business_failure_tags(record)
    judge_tags = [
        tag
        for tag in _string_list(scores.get("failureTags"))
        if tag in set(FAILURE_TAGS)
    ]
    record["failureTags"] = _dedupe_string_list([
        *_string_list(record.get("failureTags")),
        *inferred_tags,
        *judge_tags,
    ])
    return True


def _apply_side_by_side_schema(record: dict[str, Any]) -> None:
    """Expose the Business Validation Pack schema while preserving legacy nested fields."""
    scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
    astrbot = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
    country = record.get("countryCopilot") if isinstance(record.get("countryCopilot"), dict) else {}
    totals = scoring.get("scoreTotals") if isinstance(scoring.get("scoreTotals"), dict) else {}
    record["id"] = record.get("comparisonId", "")
    record["copilotAnswer"] = country.get("answerPreview", "")
    record["astrbotAnswer"] = astrbot.get("answerPreview", "")
    record["astrbotEvidencePackage"] = astrbot.get("evidencePackage", {})
    record["astrbotVisualArtifacts"] = astrbot.get("visualArtifacts", [])
    record["astrbotFollowUps"] = astrbot.get("followUps", [])
    record["astrbotQualityScore"] = astrbot.get("qualityScore", {})
    record["astrbotEvidenceDigest"] = astrbot.get("evidenceDigest", [])
    record["astrbotDisplayPlan"] = astrbot.get("displayPlan", "")
    record["scores"] = {
        "astrbot": scoring.get("astrbotScores", {}),
        "copilot": scoring.get("countryCopilotScores", {}),
        "astrbotAverage": totals.get("astrbot", 0),
        "copilotAverage": totals.get("countryCopilot", 0),
        "astrbotComposite": astrbot.get("scores", {}).get("composite", 0) if isinstance(astrbot.get("scores"), dict) else 0,
        "complete": totals.get("complete", False),
    }
    record["winner"] = scoring.get("winner", "")
    record["humanNotes"] = scoring.get("notes", "")


def _sync_side_by_side_display_metrics(record: dict[str, Any]) -> None:
    comparison = record.get("comparison")
    if not isinstance(comparison, dict):
        return
    astrbot_answer = _text(record.get("astrbotAnswer"))
    copilot_answer = _text(record.get("copilotAnswer"))
    comparison["bothReturned"] = bool(astrbot_answer) and bool(copilot_answer)
    comparison["astrbotAnswerChars"] = len(astrbot_answer)
    comparison["countryCopilotAnswerChars"] = len(copilot_answer)
    comparison["answerLengthDelta"] = len(astrbot_answer) - len(copilot_answer)


def _build_business_validation_markdown(
    records: list[dict[str, Any]],
    *,
    evidence_repair_queue: list[dict[str, Any]] | None = None,
    source_repair_backlog: list[dict[str, Any]] | None = None,
) -> str:
    repair_queue = evidence_repair_queue if evidence_repair_queue is not None else _build_evidence_repair_queue(records)
    backlog = source_repair_backlog if source_repair_backlog is not None else _build_source_repair_backlog(repair_queue)
    summary = _with_repair_gap_summary(
        _summarize_side_by_side_results(records),
        repair_queue,
    )
    summary["sourceRepairBacklogCount"] = len(backlog)
    calibration = summary.get("judgeCalibration") if isinstance(summary.get("judgeCalibration"), dict) else {}
    recommendations = summary.get("recommendedNextActions") if isinstance(summary.get("recommendedNextActions"), list) else []
    lines = [
        "# AstrBot Business Validation Report",
        "",
        f"- Total comparisons: {summary['count']}",
        f"- AstrBot average score: {summary.get('avgAstrBotHumanScore', 0)}",
        f"- CountryCopilot average score: {summary.get('avgCountryCopilotHumanScore', 0)}",
        f"- AstrBot win rate: {summary.get('astrbotWinRate', 0)}",
        f"- GPT-Human agreement rate: {calibration.get('agreementRate', 0)}",
        f"- GPT-Human weighted agreement rate: {calibration.get('weightedAgreementRate', 0)}",
        f"- GPT judged / human reviewed: {calibration.get('gptJudgedCount', 0)} / {calibration.get('humanReviewedCount', 0)}",
        f"- Scored: {summary.get('scoredCount', 0)}",
        f"- Baseline scored: {summary.get('baselineScoredCount', summary.get('scoredCount', 0))}",
        f"- Replacement baseline scored: {summary.get('replacementBaselineScoredCount', 0)}",
        f"- Pending human scoring: {summary['pendingHumanScoring']}",
        f"- Pending baseline scoring: {summary.get('pendingBaselineScoring', summary['pendingHumanScoring'])}",
        f"- Pending replacement baseline scoring: {summary.get('pendingReplacementBaselineScoring', summary['pendingHumanScoring'])}",
        f"- Human score sources: {json.dumps(summary.get('humanScoreSourceCounts', {}), ensure_ascii=False)}",
        f"- Baseline score sources: {json.dumps(summary.get('baselineSourceCounts', summary.get('humanScoreSourceCounts', {})), ensure_ascii=False)}",
        f"- Replacement baseline sources: {json.dumps(summary.get('replacementBaselineSourceCounts', {}), ensure_ascii=False)}",
        f"- AstrBot wins: {summary.get('humanWins', {}).get('astrbot', 0)}",
        f"- AstrBot replacement win rate: {summary.get('replacementAstrbotWinRate', 0)}",
        f"- CountryCopilot wins: {summary.get('humanWins', {}).get('countryCopilot', 0)}",
        f"- Ties: {summary.get('humanWins', {}).get('tie', 0)}",
        f"- Replacement readiness verdict: {summary.get('replacementReadinessVerdict', 'not_enough_data')}",
        f"- Category-level score: {json.dumps(summary.get('categoryLevelScore', {}), ensure_ascii=False)}",
        f"- Failure tags: {json.dumps(summary.get('failureTagCounts', {}), ensure_ascii=False)}",
        f"- Repair gaps: {json.dumps(summary.get('repairGapCounts', {}), ensure_ascii=False)}",
        f"- Source repair backlog items: {summary.get('sourceRepairBacklogCount', 0)}",
        "",
        "## Recommended Next Engineering Actions",
        "",
    ]
    if recommendations:
        for item in recommendations:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {item.get('priority', 'P1')} {item.get('tag', '')}: "
                f"{item.get('module', '')} — {item.get('recommendation', '')}"
            )
    else:
        lines.append("- No failure-driven action yet. Run and review more business validation records.")
    lines.extend([
        "",
        "## Evidence Repair Queue",
        "",
    ])
    if repair_queue:
        lines.extend([
            "| Priority | Question | Category | Status | Tool | Primary Gap | Missing Evidence | Source Summary | Repair Tasks | Command Hint | Repair Action |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ])
        for item in repair_queue[:20]:
            missing = item.get("missingEvidence") if isinstance(item.get("missingEvidence"), list) else []
            missing_text = "; ".join(
                str(entry.get("name") or "")
                for entry in missing
                if isinstance(entry, dict) and str(entry.get("name") or "").strip()
            )
            repair_summary = item.get("repairSummary") if isinstance(item.get("repairSummary"), dict) else {}
            source_summary = str(repair_summary.get("sourceSummary") or "").strip()
            repair_tasks = _repair_tasks_text(item)
            primary_gap = str(item.get("primaryGap") or repair_summary.get("primaryGap") or "").strip()
            command_hint = _sanitize_evidence_repair_text(item.get("commandHint"))
            action = _repair_action_text(item)
            lines.append(
                "| "
                + " | ".join([
                    _md_cell(str(item.get("priority") or "")),
                    _md_cell(str(item.get("question") or "")),
                    _md_cell(str(item.get("category") or "")),
                    _md_cell(str(item.get("answerStatus") or "")),
                    _md_cell(str(item.get("selectedTool") or "")),
                    _md_cell(primary_gap),
                    _md_cell(missing_text),
                    _md_cell(source_summary),
                    _md_cell(repair_tasks),
                    _md_cell(command_hint),
                    _md_cell(action),
                ])
                + " |"
            )
    else:
        lines.append("- No evidence repair items in the current report window.")
    lines.extend([
        "",
        "## Source Repair Backlog",
        "",
    ])
    if backlog:
        lines.extend([
            "| Priority | Source Type | Label | Draft Path | Search Query / URL | Affected | Questions | Gaps | Action |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ])
        for item in backlog[:20]:
            search_text = (
                str(item.get("sourceSearchQuery") or "").strip()
                or str(item.get("sourceUrl") or "").strip()
                or str(item.get("candidateDomain") or "").strip()
            )
            draft_path = str(item.get("sourceDraftPath") or item.get("relativePath") or "").strip()
            lines.append(
                "| "
                + " | ".join([
                    _md_cell(str(item.get("priority") or "")),
                    _md_cell(str(item.get("sourceType") or "")),
                    _md_cell(str(item.get("label") or "")),
                    _md_cell(draft_path),
                    _md_cell(search_text),
                    _md_cell(str(item.get("affectedCount") or 0)),
                    _md_cell(", ".join(_string_list(item.get("questionIds")))),
                    _md_cell(", ".join(_string_list(item.get("primaryGaps")))),
                    _md_cell(str(item.get("recommendedAction") or "")),
                ])
                + " |"
            )
    else:
        lines.append("- No source search backlog in the current report window.")
    lines.extend([
        "",
        "## GPT-Human Mismatch Examples",
        "",
    ])
    mismatch_examples = calibration.get("mismatchExamples") if isinstance(calibration.get("mismatchExamples"), list) else []
    if mismatch_examples:
        lines.extend([
            "| Question | Category | GPT Winner | Human Winner | Notes |",
            "| --- | --- | --- | --- | --- |",
        ])
        for item in mismatch_examples[:10]:
            if not isinstance(item, dict):
                continue
            lines.append(
                "| "
                + " | ".join([
                    _md_cell(str(item.get("question") or "")),
                    _md_cell(str(item.get("category") or "")),
                    _md_cell(str(item.get("gptJudgeWinner") or "")),
                    _md_cell(str(item.get("humanWinner") or "")),
                    _md_cell(str(item.get("humanNotes") or "")),
                ])
                + " |"
            )
    else:
        lines.append("- No GPT-Human mismatch examples yet.")
    lines.extend([
        "",
        "## Side-by-Side Records",
        "",
        "| Question | Category | AstrBot | CountryCopilot | Winner | Tags | Notes |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ])
    for record in records[:100]:
        scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
        totals = scoring.get("scoreTotals") if isinstance(scoring.get("scoreTotals"), dict) else {}
        astrbot_total = _format_manual_score(totals.get("astrbot"))
        copilot_total = _format_manual_score(totals.get("countryCopilot"))
        lines.append(
            "| "
            + " | ".join([
                _md_cell(str(record.get("question") or "")),
                _md_cell(str(record.get("category") or "")),
                astrbot_total,
                copilot_total,
                _md_cell(str(scoring.get("winner") or "")),
                _md_cell(", ".join(_string_list(record.get("failureTags")))),
                _md_cell(str(scoring.get("notes") or "")),
            ])
            + " |"
        )
    return "\n".join(lines)


def _initial_human_scoring(validation_type: str) -> dict[str, Any]:
    dimensions = list(_BUSINESS_SCORE_DIMENSIONS if validation_type == "business" else _DEFAULT_HUMAN_SCORE_DIMENSIONS)
    return {
        "status": "pending",
        "source": "",
        "dimensions": dimensions,
        "winner": "",
        "notes": "",
        "astrbotScores": {},
        "countryCopilotScores": {},
        "scoreTotals": _manual_score_totals({}, {}, dimensions),
    }


def _human_score_source(value: Any, *, default: str = "") -> str:
    source = _text(value).strip()
    if not source:
        return default
    return source if source in _HUMAN_SCORE_SOURCES else default


def _judge_provider_metadata(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    allowed_keys = ("source", "pathId", "label", "provider", "model", "apiBase", "keySource")
    result: dict[str, str] = {}
    for key in allowed_keys:
        text = _text(value.get(key))[:240]
        if text:
            result[key] = text
    return result


def _business_score_dimensions() -> list[dict[str, str]]:
    return [
        {"key": key, "label": _BUSINESS_SCORE_LABELS[key]}
        for key in _BUSINESS_SCORE_DIMENSIONS
    ]


def _default_score_dimensions() -> list[dict[str, str]]:
    return [
        {"key": key, "label": key}
        for key in _DEFAULT_HUMAN_SCORE_DIMENSIONS
    ]


def _read_manual_scores(value: Any, dimensions: list[str]) -> dict[str, int]:
    raw = value if isinstance(value, dict) else {}
    result: dict[str, int] = {}
    for key in dimensions:
        score = _optional_float(raw.get(key))
        if score is None:
            continue
        rounded = int(round(score))
        if rounded < 1:
            continue
        result[key] = min(5, rounded)
    return result


def _manual_total_score_from_payload(scoring: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        if key not in scoring:
            continue
        score = _optional_float(scoring.get(key))
        if score is None or score < 1:
            continue
        return min(5, int(round(score)))
    return None


def _filled_manual_scores(dimensions: list[str], score: int) -> dict[str, int]:
    bounded = min(5, max(1, int(score)))
    return {key: bounded for key in dimensions}


def _manual_score_totals(
    astrbot_scores: dict[str, int],
    country_scores: dict[str, int],
    dimensions: list[str],
) -> dict[str, float | int | bool]:
    required = len(dimensions)
    astrbot_completed = _completed_manual_score_count(astrbot_scores, dimensions)
    country_completed = _completed_manual_score_count(country_scores, dimensions)
    astrbot_complete = required > 0 and astrbot_completed == required
    country_complete = required > 0 and country_completed == required
    astrbot_total = _average_manual_score(astrbot_scores, dimensions) if astrbot_complete else 0
    country_total = _average_manual_score(country_scores, dimensions) if country_complete else 0
    return {
        "astrbot": astrbot_total,
        "countryCopilot": country_total,
        "astrbotCompleted": astrbot_completed,
        "countryCopilotCompleted": country_completed,
        "requiredDimensions": required,
        "astrbotComplete": astrbot_complete,
        "countryCopilotComplete": country_complete,
        "complete": astrbot_complete and country_complete,
        "delta": round(astrbot_total - country_total, 2) if astrbot_total and country_total else 0,
    }


def _winner_from_manual_totals(totals: dict[str, Any]) -> str:
    astrbot_total = _optional_float(totals.get("astrbot")) or 0
    country_total = _optional_float(totals.get("countryCopilot")) or 0
    if astrbot_total <= 0 or country_total <= 0:
        return "unclear"
    if astrbot_total > country_total:
        return "astrbot"
    if country_total > astrbot_total:
        return "countryCopilot"
    return "tie"


def _completed_manual_score_count(scores: dict[str, int], dimensions: list[str]) -> int:
    return sum(1 for key in dimensions if 1 <= int(scores.get(key) or 0) <= 5)


def _average_manual_score(scores: dict[str, int], dimensions: list[str]) -> float:
    values = [int(scores[key]) for key in dimensions if 1 <= int(scores.get(key) or 0) <= 5]
    if not values:
        return 0
    return round(sum(values) / len(dimensions), 2)


def _optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_manual_score(value: Any) -> str:
    numeric = _optional_float(value)
    if numeric is None or numeric == 0:
        return ""
    return f"{numeric:.2f}"


def _md_cell(value: str) -> str:
    text = str(value or "").replace("|", "\\|").replace("\n", " ").strip()
    if len(text) > 140:
        return f"{text[:137]}..."
    return text


def _evidence_ref_count(evidence_package: dict[str, Any]) -> int:
    tool_results = evidence_package.get("toolResults")
    if not isinstance(tool_results, list):
        return 0
    total = 0
    for item in tool_results:
        if not isinstance(item, dict):
            continue
        refs = item.get("evidenceRefs")
        if not isinstance(refs, list):
            continue
        total += sum(1 for ref in refs if is_usable_evidence_ref(ref))
    return total


def _shrink_for_record(value: Any, *, depth: int = 0, key_hint: str = "") -> Any:
    if key_hint in {
        "reviewChecklist",
        "materializationWorkflow",
        "materializationRequiredFields",
        "materializationMissingFields",
        "materializationRiskFlags",
        "nonMsrpTextSignals",
        "columns",
        "rawColumns",
    } and isinstance(value, list):
        return _string_list(value)[:16]
    if key_hint == "priceSanityRules" and isinstance(value, dict):
        return {
            str(key): _shrink_for_record(item, depth=depth + 1, key_hint=str(key))
            for key, item in list(value.items())[:12]
        }
    if key_hint == "evidenceRefs" and isinstance(value, list):
        return [
            {
                str(ref_key): _shrink_for_record(ref_value, depth=depth + 1, key_hint=str(ref_key))
                for ref_key, ref_value in list(ref.items())[:8]
            }
            for ref in value[:12]
            if isinstance(ref, dict)
        ]
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    if depth >= 3:
        return str(value)[:500]
    if isinstance(value, dict):
        return {
            str(key): _shrink_for_record(item, depth=depth + 1, key_hint=str(key))
            for key, item in list(value.items())[:16]
        }
    if isinstance(value, list):
        return [_shrink_for_record(item, depth=depth + 1, key_hint=key_hint) for item in value[:8]]
    if isinstance(value, str):
        return value[:1200]
    return value


def _shrink_visual_artifacts_for_record(value: Any) -> list[dict[str, Any]]:
    """Keep visual artifacts reviewable while trimming very large chart payloads."""
    if not isinstance(value, list):
        return []
    artifacts: list[dict[str, Any]] = []
    for raw_artifact in value[:6]:
        if not isinstance(raw_artifact, dict):
            continue
        artifact_type = _text(raw_artifact.get("type"))
        data = raw_artifact.get("data")
        spec = raw_artifact.get("spec") if isinstance(raw_artifact.get("spec"), dict) else {}
        artifact: dict[str, Any] = {
            "id": _text(raw_artifact.get("id")),
            "type": artifact_type,
            "title": _text(raw_artifact.get("title")),
            "subtitle": _text(raw_artifact.get("subtitle")),
            "fallbackReason": _text(raw_artifact.get("fallbackReason")),
            "sourceEvidenceRefs": _string_list(raw_artifact.get("sourceEvidenceRefs"))[:10],
        }
        if artifact_type == "table":
            artifact["data"] = _shrink_table_artifact_data(data)
            artifact["spec"] = {
                "columns": _string_list(spec.get("columns"))[:12],
                "maxRows": spec.get("maxRows"),
                "sortBy": _text(spec.get("sortBy")),
                "businessExplanation": _text(spec.get("businessExplanation"))[:500],
                "evidenceMode": _text(spec.get("evidenceMode")),
            }
        elif artifact_type == "metric_cards":
            artifact["data"] = _shrink_metric_artifact_data(data)
            artifact["spec"] = _shrink_for_record(spec)
        elif artifact_type == "report_block":
            artifact["data"] = _shrink_for_record(data)
            artifact["spec"] = _shrink_for_record(spec)
        elif artifact_type == "chart":
            artifact["data"] = _shrink_chart_artifact_data(data)
            artifact["spec"] = _shrink_chart_artifact_spec(spec)
        else:
            artifact["data"] = _shrink_for_record(data)
            artifact["spec"] = _shrink_for_record(spec)
        artifacts.append({key: item for key, item in artifact.items() if item not in ("", [], {})})
    return artifacts


def _shrink_table_artifact_data(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    rows = value.get("rows") if isinstance(value.get("rows"), list) else []
    return {
        "rows": [
            {
                str(key): _shrink_for_record(item, depth=1)
                for key, item in list(row.items())[:10]
            }
            for row in rows[:10]
            if isinstance(row, dict)
        ],
        "intentAnalysis": _shrink_for_record(value.get("intentAnalysis")),
    }


def _shrink_metric_artifact_data(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    rows = value.get("rows") if isinstance(value.get("rows"), list) else []
    return {
        "rows": [_shrink_for_record(row, depth=1) for row in rows[:6]],
        "intentAnalysis": _shrink_for_record(value.get("intentAnalysis")),
    }


def _shrink_chart_artifact_data(value: Any) -> list[Any]:
    if not isinstance(value, list):
        return []
    return [_shrink_for_record(item, depth=1) for item in value[:24]]


def _shrink_chart_artifact_spec(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    spec = {
        "chartType": _text(value.get("chartType")),
        "xField": _text(value.get("xField")),
        "yField": _text(value.get("yField")),
        "seriesField": _text(value.get("seriesField")),
        "note": _text(value.get("note"))[:500],
    }
    rows = value.get("data") if isinstance(value.get("data"), list) else []
    if rows:
        spec["data"] = [_shrink_for_record(row, depth=1) for row in rows[:24]]
    return {key: item for key, item in spec.items() if item not in ("", [], {})}


# ── Persistence ──


def _read_results() -> list[dict[str, Any]]:
    if not _RESULTS_FILE.exists():
        return []
    results: list[dict[str, Any]] = []
    with open(_RESULTS_FILE, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                results.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
    return results


def _read_side_by_side_results() -> list[dict[str, Any]]:
    if not _SIDE_BY_SIDE_FILE.exists():
        return []
    results: list[dict[str, Any]] = []
    with open(_SIDE_BY_SIDE_FILE, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                results.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
    return results


def _read_codex_review_notes() -> list[dict[str, Any]]:
    if not _CODEX_REVIEW_NOTES_FILE.exists():
        return []
    results: list[dict[str, Any]] = []
    with open(_CODEX_REVIEW_NOTES_FILE, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                note = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(note, dict) and note.get("source") == "codex_review":
                results.append(note)
    return results


def _latest_codex_review_artifact_dir() -> Path | None:
    if not _CODEX_REVIEW_ARTIFACT_DIR.exists():
        return None
    candidates = [
        item
        for item in _CODEX_REVIEW_ARTIFACT_DIR.iterdir()
        if item.is_dir()
    ]
    candidates.sort(key=lambda item: item.name, reverse=True)
    for candidate in candidates:
        if (candidate / "codex_review_report.md").exists():
            return candidate
    return candidates[0] if candidates else None


def _read_text_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _tsv_row_count(text: str) -> int:
    lines = [line for line in text.splitlines() if line.strip()]
    return max(0, len(lines) - 1)


def _append_result(record: dict[str, Any]) -> None:
    _RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    records = _read_results()
    records.append(record)
    records = records[-_MAX_RESULTS:]
    with open(_RESULTS_FILE, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _append_side_by_side_result(record: dict[str, Any]) -> None:
    records = _read_side_by_side_results()
    records.append(record)
    records = records[-_MAX_SIDE_BY_SIDE_RESULTS:]
    _write_side_by_side_results(records)


def _write_side_by_side_results(records: list[dict[str, Any]]) -> None:
    _SIDE_BY_SIDE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_SIDE_BY_SIDE_FILE, "w", encoding="utf-8") as fh:
        for item in records:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")


def _build_eval_record(
    question_def: dict[str, Any],
    result: dict[str, Any],
    scores: dict[str, Any],
) -> dict[str, Any]:
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    retrieval = data.get("retrievalClassification") if isinstance(data.get("retrievalClassification"), dict) else {}
    actual_tools = _actual_tools_from_result(result)

    return {
        "evalId": _generate_eval_id(),
        "runAt": _now_iso(),
        "questionId": question_def["id"],
        "category": question_def.get("category", "unknown"),
        "country": question_def.get("country", "unknown"),
        "question": question_def.get("question", ""),
        "expectedRetrievalPath": question_def.get("expectedRetrievalPath", ""),
        "expectedTools": question_def.get("expectedTools", []),
        "actualTool": metadata.get("selectedTool", ""),
        "actualTools": actual_tools,
        "actualRetrievalPath": retrieval.get("primaryPath", ""),
        "allRetrievalPaths": retrieval.get("allPaths", []),
        "evidenceCount": len((data.get("evidencePack") or {}).get("items", [])),
        "sourceCount": len((data.get("evidencePack") or {}).get("sources", [])),
        "chartCount": _chart_count_from_result(result),
        "scores": scores,
        "resultTool": result.get("tool", ""),
    }


def _failed_eval_record(question_def: dict[str, Any], error: str) -> dict[str, Any]:
    return {
        "evalId": _generate_eval_id(),
        "runAt": _now_iso(),
        "questionId": question_def.get("id", "unknown"),
        "category": question_def.get("category", "unknown"),
        "country": question_def.get("country", "unknown"),
        "question": question_def.get("question", ""),
        "expectedRetrievalPath": question_def.get("expectedRetrievalPath", ""),
        "expectedTools": question_def.get("expectedTools", []),
        "actualTool": "",
        "actualRetrievalPath": "",
        "allRetrievalPaths": [],
        "evidenceCount": 0,
        "sourceCount": 0,
        "chartCount": 0,
        "scores": {"composite": 0, "error": error},
        "resultTool": "",
        "error": error,
    }


def _chart_count_from_result(result: dict[str, Any]) -> int:
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    primary = data.get("primaryResult") if isinstance(data.get("primaryResult"), dict) else {}
    primary_data = primary.get("data") if isinstance(primary.get("data"), dict) else {}
    chart_specs = primary_data.get("chartSpecs") if isinstance(primary_data.get("chartSpecs"), dict) else {}
    return int(chart_specs.get("chartCount", 0))


def _text(value: Any) -> str:
    return str(value or "").strip()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _dedupe_string_list(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = _text(value)
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _list_len(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0
