from __future__ import annotations

import ast
import re
from datetime import datetime, timezone
from typing import Any, Literal, TypedDict

from app.services.jato_business_method_distillation_service import BusinessMethodDistillation
from app.services.jato_business_method_distillation_service import get_active_pricing_method
from app.services.jato_business_playbook_service import get_business_playbook
from app.services.jato_evidence_package_service import evidence_ref_count
from app.services.jato_evidence_package_service import evidence_tool_names
from app.services.jato_evidence_package_service import is_usable_evidence_ref


EvidenceAlignmentStatus = Literal["aligned", "partially_aligned", "conflicting", "insufficient"]


class EvidenceAlignment(TypedDict):
    status: EvidenceAlignmentStatus
    summary: str
    internalSignal: str
    externalSignal: str


class RecommendedAction(TypedDict):
    action: str
    rationale: str
    priority: Literal["P0", "P1", "P2"]
    evidenceRefs: list[str]
    citationIds: list[str]


class BusinessRisk(TypedDict):
    name: str
    impact: str
    mitigation: str


class BusinessSynthesisPlan(TypedDict):
    intent: str
    country: str
    executiveConclusion: str
    internalEvidenceSummary: str
    externalEvidenceSummary: str
    evidenceAlignment: EvidenceAlignment
    businessImplications: list[str]
    recommendedActions: list[RecommendedAction]
    risksAndMissingEvidence: list[BusinessRisk]
    reportReadyBullets: list[str]
    insightCardIds: list[str]
    methodDistillation: BusinessMethodDistillation | None


_INTENT_LABELS = {
    "pricing_analysis": "定价分析",
    "market_overview": "市场机会分析",
    "competitor_compare": "竞品定位分析",
    "configuration_analysis": "配置价值分析",
    "inventory_analysis": "库存/BOM 分析",
    "news_policy_search": "政策/外部研究分析",
    "report_generation": "汇报生成",
    "voc_analysis": "用户声音/VOC 分析",
}

_PLAYBOOK_SECTIONS = {
    "pricing_analysis": ["目标定位", "竞品价格走廊", "配置价值", "月供/公司车", "定价姿态", "风险和下一步"],
    "market_overview": ["市场结论", "动力/级别结构", "机会 segment", "对目标产品组合的含义", "产品动作"],
    "news_policy_search": ["政策事实边界", "影响车型", "价格门槛", "动力路线影响", "渠道/公司车影响", "产品动作"],
    "competitor_compare": ["对标结论", "竞品池", "价格/配置/空间/动力差异", "可赢点", "短板", "销售话术"],
    "configuration_analysis": ["配置问题本质", "用户场景", "竞品配置差异", "价格/价值影响", "主销配置建议"],
    "inventory_analysis": ["实体关系", "业务流程", "数据字段", "异常/生命周期", "落地动作"],
    "report_generation": ["核心结论", "证据", "业务含义", "建议动作", "汇报结构"],
}

_GAP_LABELS = {
    "market_kpis": "市场规模、份额、排名或趋势证据",
    "trend_or_mix": "动力结构变化或趋势证据",
    "competitor_set": "竞品池",
    "price_or_config_gap": "价格/配置差异",
    "trim": "版本/配置层级",
    "powertrain": "动力类型",
    "key_features": "核心配置",
    "user_value_impact": "用户价值影响",
    "stock_or_order_signal": "库存或订单信号",
    "market_context": "市场需求背景",
    "model_mapping": "车型映射",
    "business_impact": "业务影响",
    "source_date": "来源日期",
    "policy_effect": "政策影响",
    "basic_context": "基础市场上下文",
    "error_context": "错误上下文",
    "competitor_price_range": "竞品价格走廊",
    "current_msrp": "官方 MSRP 交叉验证",
    "current_official_msrp_cross_check": "官方 MSRP 交叉验证",
    "own_model_price": "本车型价格",
    "target_model_price": "目标车型价格",
    "price_corridor": "价格走廊",
    "configuration_delta": "配置差异",
    "feature_diff": "配置/功能差异",
    "competitor_pool": "竞品池",
    "supporting_evidence": "支撑证据",
    "market_snapshot_data_unavailable": "内部市场快照、HEV 销量/份额和车型结构证据",
    "model_level_market_opportunity_evidence": "车型级竞品、价格和配置机会证据",
    "jato_cross_check": "JATO 内部交叉验证",
    "report_outline": "汇报结构",
    "published_date": "来源发布日期",
    "official_source": "官方来源",
    "consumer_signal": "用户原声/VOC 信号",
    "fresh_external_signal": "最新外部信号",
    "external_research_claims_unavailable": "外部来源结论不足",
    "external_research_failed": "外部研究未成功返回来源",
    "minimum_external_sources": "外部来源数量不足",
    "monthly_trend_series": "月度趋势序列",
    "competitive_or_configuration_data_unavailable": "竞品/配置证据不足",
    "decision_boundary": "决策边界",
    "source_repair_candidates": "价格来源修复候选",
    "external_source_repair_candidates": "外部来源检索线索",
    "jato_external_conflict": "JATO 与外部证据冲突",
    "research_policy_warning": "外部研究治理提醒",
    "analyze_market_dynamics_weak_evidence_refs": "市场动态证据不足",
    "query_country_snapshot_weak_evidence_refs": "市场快照证据不足",
    "query_competitive_landscape_weak_evidence_refs": "竞品格局证据不足",
    "compare_competitive_set_weak_evidence_refs": "竞品池/价格走廊证据不足",
    "query_msrp_pricing_weak_evidence_refs": "MSRP/当前价格证据不足",
    "query_price_positioning_weak_evidence_refs": "价格定位证据不足",
    "compare_vehicle_variants_weak_evidence_refs": "配置差异证据不足",
    "build_market_chart_weak_evidence_refs": "趋势图表证据不足",
    "query_with_filters_weak_evidence_refs": "筛选数据证据不足",
    "external_research_weak_evidence_refs": "外部研究证据不足",
    "search_market_news_weak_evidence_refs": "新闻/政策证据不足",
    "read_web_page_weak_evidence_refs": "网页来源证据不足",
    "pageindex_search_documents_weak_evidence_refs": "文档检索证据不足",
    "minirag_query_graph_weak_evidence_refs": "知识图谱证据不足",
}


def build_business_synthesis_plan(
    *,
    answer: dict[str, Any] | None,
    evidence_package: dict[str, Any],
    country: str = "",
    question: str = "",
    evidence_plan: dict[str, Any] | None = None,
) -> BusinessSynthesisPlan:
    """Convert governed evidence into a deterministic PM-style synthesis plan."""
    evidence_plan = evidence_plan or {}
    evidence_package = _prepare_business_evidence_package(evidence_package, evidence_plan)
    intent = _normalize_intent(
        str(evidence_plan.get("intent") or evidence_package.get("intent") or "")
    )
    country_value = country or str(evidence_package.get("country") or "")
    method = get_active_pricing_method(
        country=country_value,
        model=_model_hint(evidence_plan=evidence_plan, evidence_package=evidence_package),
        question=question,
    ) if intent in {"pricing_analysis", "report_generation", "competitor_compare", "market_overview"} else None
    if method and not _business_method_supported_by_evidence(method, evidence_package):
        method = None
    playbook = get_business_playbook(intent)
    refs = _all_evidence_refs(evidence_package)
    relevant_ref_count = _intent_relevant_evidence_ref_count(evidence_package, intent)
    internal_summary = _evidence_summary(evidence_package, external=False)
    external_summary = _evidence_summary(evidence_package, external=True)
    alignment = _evidence_alignment(evidence_package, internal_summary, external_summary)
    insight_cards = _insight_cards(evidence_package)
    implications = _business_implications(intent, evidence_package, alignment, insight_cards, method=method, question=question)
    actions = _recommended_actions(intent, evidence_package, insight_cards, method=method, question=question)
    risks = _risks_and_missing(
        evidence_package,
        alignment,
        method=method,
        question=question,
        intent=intent,
    )
    report_bullets = _report_ready_bullets(
        intent=intent,
        country=country_value,
        question=question,
        alignment=alignment,
        implications=implications,
        actions=actions,
        refs=refs,
        evidence_package=evidence_package,
        method=method,
    )
    executive = _executive_conclusion(
        intent=intent,
        country=country_value,
        confidence=str(evidence_package.get("confidence") or "low"),
        alignment=alignment,
        refs=relevant_ref_count,
        first_action=actions[0]["action"] if actions else "",
        first_implication=implications[0] if implications else "",
        answer_status=str((answer or {}).get("answerStatus") or ""),
        method=method,
        evidence_package=evidence_package,
        question=question,
    )
    if playbook:
        title = _playbook_display_title(playbook["title"])
    else:
        title = _INTENT_LABELS.get(intent, "业务分析")
    has_market_fit_structure = (
        intent == "market_overview"
        and _is_market_fit_question(_normalize_question_text(question))
        and bool(
            _market_cross_tab_ref_value(evidence_package, table="driveByFuel", row="HEV", metric="sales")
            or _market_cross_tab_ref_value(evidence_package, table="driveBySegment", row="SUV A0", metric="sales")
            or _market_cross_tab_ref_value(evidence_package, table="driveBySegment", row="SUV A", metric="sales")
        )
    )
    should_decorate_implications = not (
        intent == "report_generation"
        or has_market_fit_structure
    )
    decorated_implications = (
        [f"{title}：{imp}" if i == 0 else imp for i, imp in enumerate(implications)]
        if should_decorate_implications
        else implications
    )
    if question:
        question_text = _display_question_subject(question)
        executive = f"{executive} 分析对象：{question_text}。"
    cleaned_implications = _dedupe([_clean_business_text(item) for item in decorated_implications])[:6]
    cleaned_report_bullets = _dedupe([_clean_business_text(item) for item in report_bullets])[:5]
    return {
        "intent": intent,
        "country": country_value,
        "executiveConclusion": _clean_business_text(executive),
        "internalEvidenceSummary": internal_summary,
        "externalEvidenceSummary": external_summary,
        "evidenceAlignment": alignment,
        "businessImplications": cleaned_implications,
        "recommendedActions": actions[:5],
        "risksAndMissingEvidence": risks[:6],
        "reportReadyBullets": cleaned_report_bullets,
        "insightCardIds": _insight_card_ids(insight_cards),
        "methodDistillation": method,
    }


def apply_business_composer(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    *,
    country: str = "",
    question: str = "",
    evidence_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_package = normalize_runtime_evidence_package_status(evidence_package)
    evidence_package = _prepare_business_evidence_package(evidence_package, evidence_plan or {})
    plan = build_business_synthesis_plan(
        answer=answer,
        evidence_package=evidence_package,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )
    _apply_configuration_topic_override(plan, evidence_package)
    frame = _business_frame(plan, evidence_package)
    enhanced = dict(answer)
    enhanced["evidencePackage"] = evidence_package
    if _has_target_policy_source_gap(evidence_package) and str(enhanced.get("answerStatus") or "").strip() == "answered":
        enhanced["answerStatus"] = "partially_answered"
    enhanced = _apply_runtime_missing_evidence_answer_status(enhanced, evidence_package)
    enhanced = _apply_scoped_evidence_answer_status(enhanced, evidence_package)
    enhanced["businessSynthesisPlan"] = plan
    enhanced["businessFrame"] = frame
    enhanced["recommendedActions"] = _public_recommended_actions(plan)
    enhanced["reportReadyBullets"] = plan["reportReadyBullets"]
    enhanced["businessImplications"] = plan["businessImplications"]
    enhanced["evidenceDigest"] = _business_evidence_digest_lines(plan, evidence_package)
    enhanced["displayPlan"] = _display_plan_for_intent(
        plan["intent"],
        evidence_package,
        question=question,
    )
    existing_title = str(enhanced.get("title") or "")
    structured_title = _report_title_from_plan(plan) or _user_title_from_plan(plan, evidence_package)
    if structured_title and (
        _should_replace_report_title(existing_title)
        or _looks_like_mixed_locale_title(existing_title)
        or _looks_like_raw_user_question_title(existing_title)
    ):
        enhanced["title"] = structured_title
    structured_summary = _structured_summary(plan, evidence_package)
    existing_summary = str(enhanced.get("summary") or "").strip()
    enhanced["summary"] = (
        structured_summary
        if _should_replace_business_text(existing_summary)
        else existing_summary
    )
    enhanced["keyTakeaways"] = _string_list(enhanced.get("keyTakeaways")) or _structured_key_takeaways(plan, evidence_package)
    structured_pm_insight = _structured_pm_insight(plan)
    existing_pm_insight = str(enhanced.get("pmInsight") or "").strip()
    enhanced["pmInsight"] = (
        structured_pm_insight
        if _should_replace_business_text(existing_pm_insight)
        else existing_pm_insight
    )
    if plan.get("methodDistillation"):
        enhanced["methodDistillation"] = plan["methodDistillation"]
    grounding = enhanced.get("grounding") if isinstance(enhanced.get("grounding"), dict) else {}
    method_distillation = plan.get("methodDistillation")
    method_warnings = (
        method_distillation.get("dataQualityWarnings")
        if isinstance(method_distillation, dict) and isinstance(method_distillation.get("dataQualityWarnings"), list)
        else []
    )
    enhanced["grounding"] = {
        **grounding,
        "confidence": str(enhanced.get("confidence") or evidence_package.get("confidence") or "low"),
        "businessSynthesisStatus": plan["evidenceAlignment"]["status"],
        "businessActionCount": len(plan["recommendedActions"]),
        "reportReadyBulletCount": len(plan["reportReadyBullets"]),
        "businessMethodType": method_distillation["methodType"] if method_distillation else "",
        "businessMethodWarningCount": len(method_warnings),
        "businessFrameSections": list(frame.keys()),
    }
    # The provider is responsible for the business narrative.  The deterministic
    # composer is a guardrail/fallback, not the default author of every answer.
    # Keeping a grounded provider answer here prevents different questions from
    # collapsing into the same "conclusion / evidence / next step" template.
    raw_provider_direct = str(answer.get("direct") or "")
    provider_direct, provider_narrative_status = _evaluate_grounded_provider_direct(
        raw_provider_direct,
        plan=plan,
        evidence_package=evidence_package,
        answer_status=str(enhanced.get("answerStatus") or ""),
        question=question,
    )
    direct = _clean_visible_direct_text(
        provider_direct or _compose_direct_answer(plan, evidence_package),
        strip_artifact_names=True,
    )
    evidence_lead = _evidence_backed_direct_lead(plan, evidence_package)
    if evidence_lead:
        enhanced["evidenceBackedLead"] = evidence_lead
    if not provider_direct and _should_add_evidence_backed_direct_lead(direct, evidence_lead, plan, evidence_package):
        direct = _clean_visible_direct_text(_bounded_direct_text([evidence_lead, direct], max_chars=1100))
    enhanced["direct"] = direct
    enhanced["grounding"] = {
        **enhanced["grounding"],
        "providerNarrativeStatus": provider_narrative_status,
        # Developer-only audit metadata: enough context to explain a rejected
        # provider narrative without leaking a raw model/tool transcript.
        "providerNarrativePreview": _clean_visible_direct_text(raw_provider_direct, strip_artifact_names=True)[:500],
    }
    enhanced["bullets"] = _compose_business_bullets(enhanced, evidence_package, plan, frame)
    enhanced["limitations"] = _compose_business_limitations(enhanced, plan, evidence_package)
    public_plan = _clean_visible_business_synthesis_plan(plan)
    enhanced["businessSynthesisPlan"] = public_plan
    enhanced["reportReadyBullets"] = public_plan["reportReadyBullets"]
    enhanced["businessImplications"] = public_plan["businessImplications"]
    return enhanced


def normalize_runtime_evidence_package_status(evidence_package: dict[str, Any]) -> dict[str, Any]:
    """Adjust display confidence for user-facing answers without changing evidence extraction rules."""
    if not isinstance(evidence_package, dict):
        return {}
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    severity = _runtime_missing_evidence_severity(missing)
    if not severity:
        return evidence_package
    normalized = dict(evidence_package)
    current_confidence = str(normalized.get("confidence") or "").strip().lower()
    if severity == "blocking":
        normalized["confidence"] = "low"
    elif current_confidence in {"", "high"}:
        normalized["confidence"] = "medium"
    return normalized


def _apply_runtime_missing_evidence_answer_status(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
) -> dict[str, Any]:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    severity = _runtime_missing_evidence_severity(missing)
    if not severity:
        return answer
    normalized = dict(answer)
    current_status = str(normalized.get("answerStatus") or normalized.get("status") or "").strip()
    if severity == "blocking":
        if current_status in {"", "answered"}:
            normalized["answerStatus"] = "insufficient_evidence"
        normalized["confidence"] = "low"
        _normalize_runtime_trust_confidence(normalized, "low")
        return normalized
    if current_status in {"", "answered"}:
        normalized["answerStatus"] = "partially_answered"
    if str(normalized.get("confidence") or "").strip().lower() in {"", "high"}:
        normalized["confidence"] = "medium"
    _normalize_runtime_trust_confidence(normalized, str(normalized.get("confidence") or "medium"))
    return normalized


def _apply_scoped_evidence_answer_status(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
) -> dict[str, Any]:
    if evidence_ref_count(evidence_package) > 0:
        return answer
    normalized = dict(answer)
    current_status = str(normalized.get("answerStatus") or normalized.get("status") or "").strip()
    if current_status not in {"tool_failed", "needs_user_data"}:
        normalized["answerStatus"] = "insufficient_evidence"
    normalized["confidence"] = "low"
    _normalize_runtime_trust_confidence(normalized, "low")
    return normalized


def _runtime_missing_evidence_severity(missing_evidence: list[Any]) -> str:
    impacts = {
        str(item.get("impact") or "").strip()
        for item in missing_evidence
        if isinstance(item, dict)
    }
    if "blocking" in impacts:
        return "blocking"
    if "weakens_answer" in impacts:
        return "weakens_answer"
    return ""


def _normalize_runtime_trust_confidence(answer: dict[str, Any], confidence: str) -> None:
    trust = answer.get("trust") if isinstance(answer.get("trust"), dict) else None
    if trust is not None:
        answer["trust"] = {**trust, "confidence": confidence}


def _compose_direct_answer(plan: BusinessSynthesisPlan, evidence_package: dict[str, Any]) -> str:
    method = plan.get("methodDistillation")
    if plan.get("intent") == "report_generation":
        report_direct = _compose_report_generation_direct_answer(plan, evidence_package)
        if report_direct:
            return report_direct
    if plan.get("intent") == "pricing_analysis":
        pricing_direct = _compose_pricing_direct_answer(plan, evidence_package)
        if pricing_direct:
            return pricing_direct
    if (
        plan.get("intent") in {"pricing_analysis", "report_generation"}
        and isinstance(method, dict)
        and method.get("methodType") == "pricing_positioning"
        and (plan.get("intent") == "report_generation" or evidence_ref_count(evidence_package) > 0)
    ):
        method_direct = _compose_pricing_method_report_direct(plan, method, evidence_package)
        if method_direct:
            return method_direct
    if plan.get("intent") == "news_policy_search":
        policy_direct = _compose_policy_direct_answer(plan, evidence_package)
        if policy_direct:
            return policy_direct
    if plan.get("intent") == "competitor_compare":
        competitor_direct = _compose_competitor_direct_answer(plan, evidence_package)
        if competitor_direct:
            return competitor_direct
    if plan.get("intent") == "configuration_analysis":
        configuration_direct = _compose_configuration_direct_answer(plan, evidence_package)
        if configuration_direct:
            return configuration_direct
    if plan.get("intent") == "inventory_analysis":
        inventory_direct = _compose_inventory_direct_answer(plan, evidence_package)
        if inventory_direct:
            return inventory_direct
    if plan.get("intent") == "voc_analysis":
        voc_direct = _compose_voc_direct_answer(plan, evidence_package)
        if voc_direct:
            return voc_direct
    executive = str(plan.get("executiveConclusion") or "").strip()
    display_note = _artifact_visual_backbone_note(str(plan.get("intent") or ""), evidence_package)
    report_lines = [
        str(item or "").strip()
        for item in plan.get("reportReadyBullets", [])
        if str(item or "").strip() and not str(item or "").strip().startswith("Title：")
    ]
    if not executive:
        return executive
    if evidence_ref_count(evidence_package) <= 0:
        return executive
    if plan.get("intent") == "market_overview":
        return executive
    if not report_lines and display_note:
        return _clean_business_text(_bounded_direct_text([executive, display_note], max_chars=1000))
    if not report_lines:
        return executive
    selected: list[str] = []
    for line in report_lines:
        if _is_redundant_direct_line(executive, line):
            continue
        selected.append(line)
        if len(selected) >= 2:
            break
    if not selected:
        return _clean_business_text(_bounded_direct_text([executive, display_note], max_chars=1000)) if display_note else executive
    return _clean_business_text(_bounded_direct_text([executive, display_note, *selected], max_chars=1000))


def _grounded_provider_direct(
    raw_direct: str,
    *,
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    answer_status: str,
    question: str,
) -> str:
    direct, _status = _evaluate_grounded_provider_direct(
        raw_direct,
        plan=plan,
        evidence_package=evidence_package,
        answer_status=answer_status,
        question=question,
    )
    return direct


def _evaluate_grounded_provider_direct(
    raw_direct: str,
    *,
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    answer_status: str,
    question: str,
) -> tuple[str, str]:
    """Keep a useful model narrative when its factual claims are evidence-safe.

    The deterministic composer still owns hard safety boundaries.  It should not,
    however, replace every valid provider answer with the same PM playbook prose.
    """
    direct = _clean_visible_direct_text(raw_direct, strip_artifact_names=True)
    if not _provider_direct_is_substantive(direct):
        return "", "replaced:too_short_or_placeholder"
    if str(answer_status or "").strip() not in {"answered", "partially_answered"}:
        return "", "replaced:answer_status"
    if evidence_ref_count(evidence_package) <= 0 or not _has_non_method_evidence(evidence_package):
        return "", "replaced:no_non_method_evidence"
    if _provider_direct_misses_relative_pricing_verdict(
        direct,
        evidence_package=evidence_package,
        question=question,
    ):
        return "", "replaced:relative_pricing_verdict_missing"
    if _provider_direct_requires_deterministic_boundary(
        direct,
        plan=plan,
        evidence_package=evidence_package,
        question=question,
    ):
        return "", "replaced:requires_hard_boundary"
    if _provider_direct_denies_available_evidence(direct, evidence_package):
        return "", "replaced:contradicts_available_evidence"
    unbacked_numeric_claims = _provider_direct_unbacked_numeric_claims(
        direct,
        evidence_package,
        question=question,
    )
    if unbacked_numeric_claims:
        return "", f"replaced:unbacked_numeric_claim:{','.join(unbacked_numeric_claims[:4])}"
    if _provider_direct_looks_like_composer_template(direct):
        return "", "replaced:provider_template"

    # The natural-language answer can remain concise, but it must visibly carry
    # evidence when the model did not quote a retrieved fact itself.
    if not _provider_direct_mentions_evidence_value(direct, evidence_package):
        anchor = _provider_evidence_anchor(plan, evidence_package)
        if anchor:
            direct = _clean_business_text(_bounded_direct_text([direct, f"依据：{anchor}。"], max_chars=1100))
    return direct, "kept"


def _provider_direct_misses_relative_pricing_verdict(
    direct: str,
    *,
    evidence_package: dict[str, Any],
    question: str,
) -> bool:
    """Reject safe but evasive prose for an explicit relative-price decision."""
    pair = _relative_pricing_pair(evidence_package, question)
    direction = pair.get("direction") if pair else ""
    if direction not in {"cheaper", "higher"}:
        return False

    text = _provider_direct_without_question_echo(direct, question)
    if direction == "cheaper":
        verdict_markers = (
            "应比",
            "应该比",
            "更强价格吸引力",
            "更有价格吸引力",
            "更低定价",
            "价格更低",
            "cheaper than",
            "lower than",
            "price advantage",
        )
    else:
        verdict_markers = (
            "应高于",
            "应该高于",
            "可以更贵",
            "适合更高定价",
            "更高定价",
            "higher than",
            "more expensive than",
            "premium over",
        )
    return not any(marker in text for marker in verdict_markers)


def _provider_direct_without_question_echo(direct: str, question: str) -> str:
    """Remove a repeated user question before checking whether the answer took a stance."""
    text = _normalize_question_text(direct)
    question_text = _normalize_question_text(question)
    if not question_text:
        return text
    variants = _dedupe([
        question_text,
        question_text.rstrip("。.!！?？；;"),
    ])
    for variant in variants:
        if variant:
            text = text.replace(variant, " ")
    return _normalize_space(text)


def _provider_direct_is_substantive(value: str) -> bool:
    text = _normalize_space(value)
    if len(text) < 60:
        return False
    placeholder = _normalize_question_text(text)
    return placeholder not in {
        "基于证据回答",
        "grounded answer",
        "evidence based answer",
        "analysis",
        "final answer",
    }


def _has_non_method_evidence(evidence_package: dict[str, Any]) -> bool:
    for tool in _tool_evidence_results(evidence_package):
        source_type = str(tool.get("sourceType") or "").strip().casefold()
        tool_name = str(tool.get("toolName") or "").strip().casefold()
        refs = tool.get("evidenceRefs") if isinstance(tool.get("evidenceRefs"), list) else []
        if not refs:
            continue
        if source_type in {"generated", "user_material"} or tool_name == "business_method_material":
            continue
        return True
    return False


def _provider_direct_requires_deterministic_boundary(
    direct: str,
    *,
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    question: str,
) -> bool:
    intent = str(plan.get("intent") or "")
    question_text = _normalize_question_text(question)
    if _has_target_policy_source_gap(evidence_package):
        return True
    if intent in {"news_policy_search", "report_generation"} and _is_policy_report_context(question_text, evidence_package):
        if not _has_official_policy_source_evidence(evidence_package):
            return True
    if intent == "pricing_analysis" and _has_missing_evidence(evidence_package, "current_msrp"):
        # A model may repeat a user-material price as if it were a live MSRP.
        return bool(_numeric_claim_tokens(direct))
    return False


def _provider_direct_looks_like_composer_template(value: str) -> bool:
    lowered = _normalize_question_text(value)
    markers = (
        "直接结论",
        "证据状态",
        "下一步执行",
        "当前能判断",
        "业务判断应先",
        "分析对象",
    )
    return sum(marker in lowered for marker in markers) >= 2


def _provider_direct_denies_available_evidence(direct: str, evidence_package: dict[str, Any]) -> bool:
    text = _normalize_question_text(direct)
    denies_data = _contains_any(text, (
        "数据为空",
        "0行",
        "0 行",
        "没有数据",
        "无可用数据",
        "无法计算市场规模",
        "market data is empty",
        "no available data",
        "no market data",
    ))
    if not denies_data:
        return False
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").casefold()
        if not any(token in label for token in ("sales", "volume", "share", "powertrain", "driveby", "segment")):
            continue
        value = _numeric_ref_value(ref)
        if value is not None and value > 0:
            return True
    return False


def _provider_direct_has_unbacked_numeric_claim(
    direct: str,
    evidence_package: dict[str, Any],
    *,
    question: str,
) -> bool:
    return bool(_provider_direct_unbacked_numeric_claims(direct, evidence_package, question=question))


def _provider_direct_unbacked_numeric_claims(
    direct: str,
    evidence_package: dict[str, Any],
    *,
    question: str,
) -> list[str]:
    direct_tokens = _numeric_claim_tokens(direct)
    if not direct_tokens:
        return []
    evidence_tokens = {
        _numeric_claim_key(token)
        for ref in _all_evidence_refs(evidence_package)
        for token in _numeric_claim_tokens(str(ref.get("value") or ""))
    }
    question_tokens = {_numeric_claim_key(token) for token in _numeric_claim_tokens(question)}
    return [
        token
        for token in direct_tokens
        if _numeric_claim_key(token) not in evidence_tokens and _numeric_claim_key(token) not in question_tokens
    ]


def _provider_direct_mentions_evidence_value(direct: str, evidence_package: dict[str, Any]) -> bool:
    direct_tokens = {_numeric_claim_key(token) for token in _numeric_claim_tokens(direct)}
    if not direct_tokens:
        return False
    evidence_tokens = {
        _numeric_claim_key(token)
        for ref in _all_evidence_refs(evidence_package)
        for token in _numeric_claim_tokens(str(ref.get("value") or ""))
    }
    return bool(direct_tokens & evidence_tokens)


def _numeric_claim_tokens(value: str) -> list[str]:
    """Normalize material numeric facts without treating one- or two-digit prose as data."""
    result: list[str] = []
    for raw in re.findall(r"\d{1,3}(?:[ ,]\d{3})*(?:\.\d+)?\s*%?|\d+\.\d+", str(value or "")):
        token = raw.replace(" ", "").replace(",", "").strip()
        numeric = token.rstrip("%")
        if "%" in token or "." in numeric or len(numeric) >= 3:
            result.append(token)
    return _dedupe(result)


def _numeric_claim_key(token: str) -> str:
    """Compare a numeric fact independent of display-only percent formatting."""
    value = str(token or "").replace(",", "").replace(" ", "").rstrip("%")
    if "." in value:
        value = value.rstrip("0").rstrip(".")
    return value


def _provider_evidence_anchor(plan: BusinessSynthesisPlan, evidence_package: dict[str, Any]) -> str:
    intent = str(plan.get("intent") or "")
    lines = [
        line
        for line in _evidence_digest_lines(evidence_package, intent, limit=4)
        if _is_concrete_evidence_digest_line(line)
    ]
    return "；".join(lines[:2])


def _evidence_backed_direct_lead(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    intent = str(plan.get("intent") or "")
    country_label = _country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场"))
    if intent == "pricing_analysis":
        verified_lines = _pricing_verified_evidence_lines(evidence_package, limit=4)
        material_lines = _pricing_user_material_hypothesis_lines(evidence_package, limit=2)
        if verified_lines:
            verified_text = "；".join(verified_lines[:4])
            material_text = "；".join(material_lines[:2])
            material_note = f" 用户材料假设：{material_text}，不能当作当前官方 MSRP。" if material_text else ""
            return _clean_business_text(
                f"已查数据：{country_label} {verified_text}。{material_note}"
                "业务判断：先用已验证价格/市场证据定价格边界，用户材料只能作为待验证定位假设。"
            )
        if material_lines:
            material_text = "；".join(material_lines[:2])
            return _clean_business_text(
                f"已查数据：{country_label} 本轮没有拿到本车型/核心竞品官方 MSRP、月供/RV 或成交支持。"
                f"用户材料假设：{material_text}，只能作为待验证定位口径。"
                "业务判断：不能用材料模板直接给最终价，下一步必须补官方价格、竞品价格和配置差异。"
            )
    digest_lines = [
        line
        for line in _evidence_digest_lines(evidence_package, intent, limit=6)
        if _is_concrete_evidence_digest_line(line)
    ][:4]
    if not digest_lines:
        return ""
    evidence_text = "；".join(digest_lines[:4])
    business_use = _evidence_backed_business_judgment(intent, country_label, evidence_package, plan)
    return _clean_business_text(f"已查数据：{country_label} {evidence_text}。业务判断：{business_use}")


def _evidence_backed_business_judgment(
    intent: str,
    country_label: str,
    evidence_package: dict[str, Any],
    plan: BusinessSynthesisPlan,
) -> str:
    if intent == "pricing_analysis":
        return "定价判断不能停在工具计划，应先用已查价格/市场证据画出价格边界，再把缺失的官方 MSRP、竞品价和配置价值列为定案缺口。"
    if intent == "competitor_compare":
        return "竞品判断应先落到有证据的对标边界；已有直接车型证据的可写角色，缺直接车型证据的只能写待验证角色和补证路径。"
    if intent == "market_overview":
        missing_names = _missing_evidence_names(evidence_package)
        has_internal_market_evidence = _market_fit_has_usable_internal_market_evidence(evidence_package)
        if (
            missing_names & {"market_snapshot_data_unavailable", "jato_cross_check", "internal_market_rows"}
            and not has_internal_market_evidence
        ):
            return "市场路线判断只能先作为方向假设；外部证据可用于政策/公司车背景，但不能替代内部销量、动力结构和车型证据。"
        context_text = _normalize_question_text(" ".join([plan.get("executiveConclusion", ""), *plan.get("reportReadyBullets", [])]))
        if _is_market_fit_question(context_text):
            target = _market_fit_target_label(context_text, plan.get("methodDistillation"), evidence_package)
            return f"{target} 的市场机会可以先作为优先验证入口；但是否上市/定价，还必须补车型级竞品、价格和配置证据。"
        return "市场总览已经可以先锁定机会入口、动力结构和级别/车型方向；下一步不是复述规模，而是把这些数字转成产品动作。"
    if intent == "configuration_analysis":
        return "配置判断应先把已查差异拆成 must-have、visible value 和 optional，再决定主销版本和配置包。"
    if intent == "inventory_analysis":
        return "库存/BOM 判断应先把可用数量、物料号、版本、市场和生命周期状态串起来；缺生命周期或市场 overlay 时，不能直接开放客户可编辑数量。"
    if intent == "news_policy_search":
        return "政策/新闻判断应先确认来源日期、适用对象和影响口径；缺官方来源时只能写影响方向，不能写确定车型名单。"
    if intent == "voc_analysis":
        return "VOC 判断应先区分真实用户原声、媒体观点和市场背景；没有用户声量证据时，不能把背景数据写成已验证高频吐槽。"
    return "业务判断应先给出可执行结论和风险边界，再列下一步动作。"


def _is_concrete_evidence_digest_line(value: str) -> bool:
    text = _normalize_space(str(value or ""))
    if not text:
        return False
    lowered = text.casefold()
    if any(
        marker in lowered
        for marker in (
            "待补",
            "未形成",
            "不足",
            "缺少",
            "缺口",
            "not available",
            "unavailable",
            "no current",
            "no official",
            "no citable",
            "已尝试工具",
        )
    ):
        return False
    return "=" in text or text.startswith("[R")


def _should_add_evidence_backed_direct_lead(
    direct: str,
    evidence_lead: str,
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> bool:
    if not direct or not evidence_lead:
        return False
    if evidence_ref_count(evidence_package) <= 0:
        return False
    direct_text = _normalize_question_text(direct)
    if direct_text.startswith(("已查数据", "已查证据", "市场证据", "可引用证据")):
        return False
    if direct_text.startswith("验证版定价立场"):
        return False
    if _direct_should_keep_verdict_first(direct_text):
        return False
    intent = str(plan.get("intent") or "")
    evidence_first_template_candidate = intent in {"pricing_analysis", "competitor_compare", "market_overview"} and (
        _direct_looks_like_template_conclusion(direct)
        or _direct_is_action_or_gap_heavy(direct)
        or _looks_like_generic_first_sentence(direct)
    )
    if not _direct_is_raw_or_weak_placeholder(direct) and not evidence_first_template_candidate:
        return False
    if _direct_contains_prioritized_evidence_value(direct, evidence_package, intent):
        return False
    if _direct_contains_evidence_digest_line(direct, evidence_package, intent):
        return False
    if (
        intent == "market_overview"
        and _missing_evidence_names(evidence_package)
        & {
            "market_snapshot_data_unavailable",
            "jato_cross_check",
            "internal_market_rows",
        }
        and not _market_fit_has_usable_internal_market_evidence(evidence_package)
    ):
        return False
    if evidence_first_template_candidate:
        return True
    if _direct_is_action_or_gap_heavy(direct):
        return True
    return _looks_like_generic_first_sentence(direct)


def _direct_is_raw_or_weak_placeholder(value: str) -> bool:
    text = _normalize_space(str(value or ""))
    lowered = text.casefold()
    weak_prefixes = (
        "grounded answer",
        "based on available evidence",
        "insufficient evidence",
        "基于证据回答",
        "证据不足",
        "当前证据不足",
    )
    return lowered.startswith(weak_prefixes)


def _direct_contains_prioritized_evidence_value(
    direct: str,
    evidence_package: dict[str, Any],
    intent: str,
) -> bool:
    normalized_direct = _normalize_compact_match_text(direct)
    if not normalized_direct:
        return False
    for ref in _prioritized_evidence_refs(evidence_package, intent)[:8]:
        if intent == "pricing_analysis" and _is_pricing_user_material_ref(ref):
            continue
        value = _format_evidence_ref_value(ref)
        if not value:
            continue
        normalized_value = _normalize_compact_match_text(value)
        if normalized_value and normalized_value in normalized_direct:
            return True
    return False


def _direct_contains_evidence_digest_line(
    direct: str,
    evidence_package: dict[str, Any],
    intent: str,
) -> bool:
    normalized_direct = _normalize_compact_match_text(direct)
    lines = (
        _pricing_verified_evidence_lines(evidence_package, limit=4)
        if intent == "pricing_analysis"
        else _evidence_digest_lines(evidence_package, intent, limit=4)
    )
    for line in lines:
        normalized_line = _normalize_compact_match_text(line)
        if normalized_line and normalized_line in normalized_direct:
            return True
    return False


def _normalize_compact_match_text(value: str) -> str:
    return re.sub(r"[\s,，。；;：:=/()（）\-_]+", "", str(value or "").casefold())


def _direct_is_action_or_gap_heavy(value: str) -> bool:
    text = _normalize_question_text(value)
    if not text:
        return False
    markers = (
        "下一步",
        "建议",
        "生成",
        "补齐",
        "补证",
        "缺",
        "当前仍",
        "证据不足",
        "不能给确定",
        "不能把",
        "待验证",
        "验证表",
        "矩阵",
    )
    return sum(1 for marker in markers if marker in text) >= 2


def _direct_should_keep_verdict_first(value: str) -> bool:
    text = _normalize_question_text(value)
    if not text:
        return False
    return any(
        marker in text
        for marker in (
            "不能生成跨国家销量差异结论",
            "不能生成跨国家",
            "路线判断只能先作为方向假设",
        )
    )


def _direct_looks_like_template_conclusion(value: str) -> bool:
    """Detect deterministic playbook prose so concrete evidence can lead the answer."""
    text = _normalize_question_text(value)
    if not text:
        return False
    template_markers = (
        "直接结论",
        "下一步执行",
        "证据状态",
        "置信度",
        "业务含义",
        "产品动作",
        "展示骨架",
        "数据边界",
        "数据缺口",
    )
    return sum(1 for marker in template_markers if marker in text) >= 3


def _apply_configuration_topic_override(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> None:
    profile = _configuration_topic_profile(plan, evidence_package)
    if not profile:
        return
    action_text = str(profile["action"])
    executive = str(profile.get("executiveConclusion") or "").strip()
    if executive:
        plan["executiveConclusion"] = executive
    existing_actions = plan.get("recommendedActions", [])
    first_existing = existing_actions[0] if existing_actions else {}
    evidence_refs = first_existing.get("evidenceRefs") if isinstance(first_existing, dict) else []
    citation_ids = first_existing.get("citationIds") if isinstance(first_existing, dict) else []
    topic_action: RecommendedAction = {
        "action": action_text,
        "rationale": str(profile["rationale"]),
        "priority": "P0",
        "evidenceRefs": [str(item) for item in evidence_refs] if isinstance(evidence_refs, list) else [],
        "citationIds": [str(item) for item in citation_ids] if isinstance(citation_ids, list) else [],
    }
    remaining_actions = [
        item
        for item in existing_actions
        if isinstance(item, dict) and _clean_action_text(str(item.get("action") or "")) != action_text
    ]
    plan["recommendedActions"] = [topic_action, *remaining_actions][:5]
    plan["businessImplications"] = _dedupe(
        [str(profile["implication"]), *[str(item) for item in plan.get("businessImplications", [])]]
    )[:5]
    plan["reportReadyBullets"] = [str(item) for item in profile["reportReadyBullets"]][:5]


def _configuration_topic_profile(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> dict[str, object] | None:
    if plan.get("intent") != "configuration_analysis":
        return None
    question_text = _normalize_question_text(" ".join([plan.get("executiveConclusion", ""), *plan.get("reportReadyBullets", [])]))
    evidence_note = _intent_evidence_count_note(
        _intent_relevant_evidence_ref_count(evidence_package, str(plan.get("intent") or "")),
        str(plan.get("intent") or ""),
    )
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])
    has_80kwh = "80kwh" in question_text or "80 kwh" in question_text
    has_winter_package = "冬季包" in question_text or "winter package" in question_text

    if has_winter_package and has_80kwh:
        return {
            "executiveConclusion": (
                "直接结论：北欧冬季包和 A0 SUV BEV 80kWh 目前只能作为联动版本策略假设；"
                "需要先补低温可用性、续航、价格和竞品配置证据，再判断是否成立。"
            ),
            "action": "生成北欧冬季包 + A0 SUV BEV 80kWh 版本策略验证表",
            "rationale": "复合配置问题需要同时解释冬季可用性、电池版本策略、价格锚点和高配价值。",
            "implication": "北欧冬季包和 80kWh 电池需要一起进入版本策略验证：先补低温可用性、续航、价格和竞品配置证据，再判断是否进入主销配置。",
            "summary": f"{plan['country']} A0 SUV BEV 的冬季包和 80kWh 目前只能作为版本策略假设验证，不能写成已成立配置结论；{evidence_note}。",
            "reportReadyBullets": [
                f"证据状态：当前缺少冬季包、80kWh、续航、价格和竞品配置矩阵的可引用证据；状态为{alignment_note}。",
                "验证路径：把热泵、电池预热/路线预热、座椅/方向盘加热、冬季胎/TPMS、充电预热和真实冬季续航放入同一张配置矩阵。",
                "产品动作：只有当价格、重量、续航和竞品标配证据成立时，才把 80kWh 和冬季包推进高配/长续航版本策略。",
                "下一步：生成北欧冬季包 + A0 SUV BEV 80kWh 版本策略验证表。",
            ],
        }
    if "95kwh" in question_text or "95 kwh" in question_text or "800v" in question_text or "双电机" in question_text:
        return {
            "executiveConclusion": (
                "直接结论：4.7m A-SUV 的 95kWh + 双电机 + 800V 目前只能作为高配架构假设；"
                "需要先验证续航、牵引、补能效率、成本和价格带是否互相支撑。"
            ),
            "action": "生成 95kWh / 双电机 / 800V 配置价值-成本验证表",
            "rationale": "高规格 BEV 架构问题需要验证续航、牵引、补能效率、成本和价格带是否互相支撑。",
            "implication": "95kWh + 双电机 + 800V 需要先验证续航、牵引、补能效率、成本和价格带，不能在缺证据时直接写成高价值架构成立。",
            "summary": f"{plan['country']} 4.7m A-SUV 的 95kWh / 双电机 / 800V 目前只能作为高配架构假设验证；{evidence_note}。",
            "reportReadyBullets": [
                f"证据状态：当前缺少 95kWh、双电机、800V、成本/重量、价格和竞品配置矩阵的可引用证据；状态为{alignment_note}。",
                "验证路径：分别验证冬季真实续航、湿滑路面牵引、补能效率、fleet 使用效率和竞品高配架构。",
                "产品动作：只有当成本、重量和竞品价格带支撑成立时，才判断是否拆成单电机长续航版和高配四驱快充版。",
                "下一步：生成 95kWh / 双电机 / 800V 配置价值-成本验证表。",
            ],
        }
    if has_80kwh:
        return {
            "executiveConclusion": (
                "直接结论：A0 SUV BEV 的 80kWh 目前不是已证实必需配置；"
                "需要先验证冬季真实续航、竞品长续航版本、重量、成本和价格压力。"
            ),
            "action": "生成 A0 SUV BEV 80kWh 续航-价格-重量验证表",
            "rationale": "80kWh 不能被简单写成必选卖点，需要验证真实冬季续航、用户场景、重量和价格压力。",
            "implication": "A0 SUV BEV 的 80kWh 需要先验证真实冬季续航、竞品长续航版本、重量、成本和价格压力，再决定是否作为高配/长续航卖点。",
            "summary": f"{plan['country']} A0 SUV BEV 的 80kWh 目前只能作为长续航/高配假设验证，不能写成全系必选；{evidence_note}。",
            "reportReadyBullets": [
                f"证据状态：当前缺少电池、续航、充电、价格或竞品配置矩阵的可引用证据；状态为{alignment_note}。",
                "验证路径：用冬季真实续航、竞品长续航版、重量、成本、MSRP/月供和用户场景验证 80kWh 是否有必要。",
                "产品动作：只有验证通过后，才把 80kWh 放入高配/长续航版本策略；低配是否保留价格锚点需要价格证据支持。",
                "下一步：生成 A0 SUV BEV 80kWh 续航-价格-重量验证表。",
            ],
        }
    if has_winter_package:
        return {
            "executiveConclusion": (
                "直接结论：北欧冬季包目前不能直接下 must-have 清单；"
                "需要先补逐项配置、价格、竞品标配和低温使用证据。"
            ),
            "action": "生成北欧冬季包 must-have / value / optional 配置清单",
            "rationale": "冬季包问题需要把低温可用性、可见舒适价值和户外/拖挂场景分层。",
            "implication": "北欧冬季包需要先补配置标配、价格、竞品和低温使用证据，再判断 must-have / visible value / optional 分层。",
            "summary": f"{plan['country']} 北欧冬季包目前只能先列验证路径，不能在缺少配置矩阵时直接下 must-have 清单；{evidence_note}。",
            "reportReadyBullets": [
                f"证据状态：当前缺少冬季包逐项配置、价格和竞品标配矩阵；状态为{alignment_note}。",
                "验证路径：把热管理、加热舒适、低温充电、轮胎/牵引和户外实用配置逐项映射到用户场景、成本和竞品标配状态。",
                "产品动作：验证后再区分 must-have、visible value 和 optional，避免把通用经验写成确定配置清单。",
                "下一步：生成北欧冬季包 must-have / value / optional 配置清单。",
            ],
        }
    return None


def _compose_policy_direct_answer(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    executive = _strip_direct_prefix(str(plan.get("executiveConclusion") or "").strip())
    if not executive:
        return ""
    text = _normalize_question_text(" ".join([executive, *plan.get("reportReadyBullets", [])]))
    country_label = _country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场"))
    action = ""
    if plan.get("recommendedActions"):
        action = _clean_action_text(str(plan["recommendedActions"][0].get("action") or ""))
    evidence_note = _evidence_count_note(evidence_ref_count(evidence_package))
    confidence_note = _confidence_label(str(evidence_package.get("confidence") or "low"))
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])
    display_note = _artifact_visual_backbone_note("news_policy_search", evidence_package)
    market_brief = _policy_market_context_brief(evidence_package, text)
    source_brief = _policy_external_evidence_brief(evidence_package)
    missing_boundary = _policy_missing_boundary_brief(evidence_package)

    if "elbilspremien" in text:
        target_policy_boundary = _elbilspremien_target_policy_boundary_note(evidence_package)
        elbil_action = (
            action
            if _action_matches_topic(action, ("elbilspremien", "受影响", "车型", "资格", "价格上限"))
            else "补齐官方政策原文、发布日期、资格/价格上限，并生成受影响车型矩阵"
        )
        parts = [
            f"政策边界：{country_label} Elbilspremien 2026 不能先点名确定受益车型，应先确认官方政策原文、发布日期、车型资格、价格上限、购买人群和交付时间。",
            target_policy_boundary,
            "受影响车型：优先看价格门槛内、私人零售敏感度高的 BEV SUV A0/A；超过资格门槛或交付节奏不匹配的车型，只能作为待验证对象。",
            market_brief,
            display_note,
            "产品动作：把低配/主销版作为补贴资格锚点，高配则必须证明补贴外的配置价值和品牌理由。",
            missing_boundary,
            f"下一步执行：{elbil_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=900))

    if _is_phev_fleet_leasing_question(text):
        parts = [
            f"直接结论：{_market_business_prefix(country_label, '大客户 leasing 场景')}下 PHEV 仍可能有理由，但只能作为条件成立的 fleet/TCO 验证线。",
            "判断口径：理由不来自泛泛“可油可电”，而来自月供、残值/RV、税务 benefit、年里程、充电条件、长途里程和冬季使用风险的组合。",
            "业务边界：如果这些口径算不出成本或使用风险优势，PHEV 就不应主推；缺少 leasing/TCO/company-car 来源时只能给验证框架，不能写确定财务结论。",
            market_brief,
            display_note,
            f"下一步执行：{action or '建立 PHEV fleet leasing TCO 表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=900))

    if "company car" in text and "bev" in text and "phev" in text:
        bev_logic, phev_logic = _policy_company_car_logic_lines(evidence_package)
        parts = [
            f"政策边界：{country_label} company car benefit 对 BEV 和 PHEV 的差异，不能只看补贴或排放标签，应拆 benefit tax、月供、残值、公司车政策、充电条件和实际里程。",
            market_brief,
            bev_logic,
            phev_logic,
            display_note,
            f"下一步执行：{action or '建立 BEV/PHEV company car benefit 对比表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1050))

    if ("co2" in text or "co₂" in text) and "phev" in text:
        phev_logic = _policy_co2_phev_logic_line(evidence_package)
        parts = [
            f"政策边界：{country_label} CO2 0-75g/km 阶梯只是 PHEV 的入场条件，不是自动利好；必须先核对官方税率/benefit 公式、发布日期和适用车辆。",
            market_brief,
            "判断口径：把认证 CO2、company car 税费、月供、残值、能耗、真实充电行为和用户里程放进同一张 TCO 表。",
            phev_logic,
            display_note,
            f"下一步执行：{action or '核对 PHEV 认证 CO2、税率阶梯、company car 计算公式和发布日期'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1050))

    if _is_bev_subsidy_cap_question(text):
        target_model = _policy_target_model(evidence_package, text)
        if _policy_bonus_has_ended_evidence(evidence_package):
            next_action = (
                f"补齐当前 {target_model} MSRP、竞品价格走廊/月供，并持续监控{country_label}是否发布新的 BEV 补贴计划"
            )
            if action and not _looks_like_source_repair_action(action):
                next_action = action
            evidence_brief = _policy_evidence_brief_after_verdict(source_brief)
            parts = [
                f"直接结论：{country_label} 当前不应把 BEV 补贴价格上限当作 {target_model} 的现行定价约束；已有官方交通主管来源显示低排放车辆 bonus 已结束。",
                evidence_brief,
                f"定价含义：{target_model} 不能只为了卡补贴门槛牺牲配置或毛利，第一版应回到竞品价格走廊、配置价值、月供/TCO 和库存节奏来定主销版。",
                market_brief,
                display_note,
                "保留边界：仍需继续核对是否存在新的补贴计划、价格门槛或特定人群资格；在新官方细则确认前，补贴价格上限只能作为历史敏感锚点和监控项。",
                missing_boundary,
                f"下一步执行：{next_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
            ]
            return _clean_business_text(_bounded_direct_text(parts, max_chars=900))
        if source_brief:
            evidence_brief = _policy_evidence_brief_after_verdict(source_brief)
            parts = [
                (
                    f"直接结论：{country_label} BEV 补贴价格上限会把 {target_model} 定价从单纯“比竞品便宜多少”改成"
                    "“低配/主销版能否进入资格门槛、高配能否证明补贴外价值”的版本策略问题；"
                    f"但当前证据还不能支持“{target_model} 确定适用”或“该价格上限已是现行约束”。"
                ),
                evidence_brief,
                f"产品动作：先把 {target_model} 拆成补贴内入门锚点和补贴外高配价值两套价格页，再用当前 MSRP、竞品走廊和月供/TCO 决定主销版。",
                market_brief,
                display_note,
                missing_boundary,
                f"下一步执行：{action or f'核对官方政策原文、{target_model} 资格和当前 MSRP'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
            ]
            return _clean_business_text(_bounded_direct_text(parts, max_chars=900))
        parts = [
            f"政策边界：{country_label} BEV 补贴价格上限不能默认当成现行约束，必须先核对官方原文、发布日期、适用人群、是否仍有效以及 {target_model} 是否符合资格。",
            "版本含义：如果价格上限仍有效，低配/主销版承担补贴资格锚点，高配需要证明补贴外的续航、配置、冬季包或品牌价值；如果已失效，它只能作为历史价格锚点。",
            f"情景矩阵：A 有效且 {target_model} 适用时，主销版优先卡进资格门槛；B 失效或不适用时，价格逻辑回到竞品走廊和配置价值；C 新计划未确认时，同时准备补贴内入门锚点和补贴外高配价值页。",
            market_brief,
            display_note,
            "产品动作：同时准备“补贴内资格锚点”和“补贴外高配价值”两套价格页，避免一个价格方案同时承担流量、利润和品牌任务。",
            missing_boundary,
            f"下一步执行：{action or f'核对{country_label} BEV 补贴价格上限是否仍有效及 {target_model} 是否适用'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=900))

    return _clean_business_text(_bounded_direct_text([
        source_brief,
        f"政策边界：{executive}",
        market_brief,
        display_note,
        "产品动作：先把政策事实拆成适用对象、价格门槛、动力路线、零售/公司车场景，再判断车型和配置动作。",
        missing_boundary,
        f"下一步执行：{action or '补官方来源、发布日期和 JATO 交叉验证'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ], max_chars=900))


def _policy_evidence_brief_after_verdict(source_brief: str) -> str:
    text = str(source_brief or "").strip()
    if not text:
        return ""
    return text.replace("已查证据：", "关键证据：", 1)


def _policy_external_evidence_brief(evidence_package: dict[str, Any]) -> str:
    groups = _policy_external_evidence_groups(evidence_package)
    if not groups:
        return ""
    parts: list[str] = []
    for group in groups[:2]:
        citation = str(group.get("citation") or "").strip()
        title = str(group.get("title") or "").strip()
        domain = str(group.get("domain") or "").strip()
        published = str(group.get("publishedAt") or "").strip()
        claim = _compact_policy_claim(str(group.get("claim") or "").strip())
        meta = "，".join(item for item in (domain, published) if item)
        head = " ".join(item for item in (citation, title) if item).strip()
        if meta:
            head = f"{head}（{meta}）"
        if claim:
            parts.append(f"{head}：{claim}")
        elif head:
            parts.append(head)
    if not parts:
        return ""
    return f"已查证据：{'；'.join(parts)}。"


def _policy_external_evidence_groups(evidence_package: dict[str, Any]) -> list[dict[str, str]]:
    groups: list[dict[str, str]] = []
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        if str(tool.get("toolName") or "") not in {"external_research", "search_market_news", "read_web_page", "browser_snapshot", "pageindex_search_documents"}:
            continue
        groups.extend(_external_ref_groups(_coerce_evidence_refs(tool.get("evidenceRefs"))))
    return groups


def _compact_policy_claim(value: str) -> str:
    text = _normalize_space(value)
    if not text:
        return ""
    cleaned = _clean_business_text(text)
    if len(cleaned) <= 180:
        return cleaned
    return f"{cleaned[:177].rstrip()}..."


def _policy_missing_boundary_brief(evidence_package: dict[str, Any]) -> str:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    names: list[str] = []
    for item in missing:
        if not isinstance(item, dict):
            continue
        raw = _normalize_space(str(item.get("name") or ""))
        if not raw:
            continue
        lowered = raw.casefold()
        if any(token in lowered for token in ("official", "source", "policy", "date", "eligibility", "msrp", "price", "external_research")):
            names.append(_missing_evidence_label(raw))
    names = _dedupe(names)[:3]
    if not names:
        return ""
    return f"仍缺证据：{', '.join(names)}；缺口补齐前，只能输出影响路径和价格页假设，不能写确定适用名单或确定 MSRP 结论。"


def _elbilspremien_target_policy_boundary_note(evidence_package: dict[str, Any]) -> str:
    if (
        not _has_missing_evidence(evidence_package, "target_policy_source")
        and not _has_missing_evidence(evidence_package, "external_research")
        and not _has_missing_evidence(evidence_package, "official_source")
    ):
        return ""
    if _policy_bonus_has_ended_evidence(evidence_package):
        return (
            "已查证据：Transportstyrelsen 低排放车辆 bonus 已结束；这只能说明旧 bonus 不是当前可用补贴，"
            "不能替代 Elbilspremien 2026 的官方原文、车型资格和价格上限。"
        )
    return (
        "证据边界：当前缺少 Elbilspremien 2026 官方政策原文或年份来源，因此只能输出受影响车型的候选筛选逻辑，"
        "不能写成确定补贴名单。"
    )


def _policy_market_context_brief(evidence_package: dict[str, Any], question_text: str) -> str:
    text = str(question_text or "").lower()
    stats = _policy_powertrain_stats(evidence_package)
    if not stats:
        return ""
    if "company car" in text and "bev" in text and "phev" in text:
        bev = stats.get("BEV", {})
        phev = stats.get("PHEV", {})
        bev_line = _policy_powertrain_line("BEV", bev, include_channel=True)
        phev_line = _policy_powertrain_line("PHEV", phev, include_channel=True)
        if bev_line and phev_line:
            return (
                f"市场证据：{bev_line}；{phev_line}。"
                "这说明 BEV 的绝对盘更大，PHEV 更依赖公司车渠道。"
            )
    if ("co2" in text or "co₂" in text) and "phev" in text:
        phev = stats.get("PHEV", {})
        phev_line = _policy_powertrain_line("PHEV", phev, include_channel=True)
        drive_line = _policy_drive_line(phev)
        if phev_line:
            suffix = f"，{drive_line}" if drive_line else ""
            return (
                f"市场证据：{phev_line}{suffix}。"
                "这说明公司车/TCO 场景确实重要，但不能自动证明税率阶梯有利。"
            )
    if "elbilspremien" in text or ("bev" in text and "补贴" in text):
        bev = stats.get("BEV", {})
        bev_line = _policy_powertrain_line("BEV", bev, include_channel=True)
        segment_line = _policy_segment_line(stats, "BEV")
        if bev_line:
            suffix = f"；{segment_line}" if segment_line else ""
            return (
                f"市场证据：{bev_line}{suffix}。"
                "这说明补贴影响应优先验证私人零售、价格门槛和高 BEV 渗透细分市场。"
            )
    return ""


def _policy_powertrain_stats(evidence_package: dict[str, Any]) -> dict[str, dict[str, str]]:
    return _powertrain_stats_from_evidence(evidence_package)


def _powertrain_stats_from_evidence(evidence_package: dict[str, Any]) -> dict[str, dict[str, str]]:
    return _powertrain_stats_from_refs(_all_evidence_refs(evidence_package))


def _powertrain_stats_from_refs(refs: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    priorities: dict[str, dict[str, int]] = {}
    fuels = ("PHEV", "MHEV", "BEV", "HEV", "ICE")
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if not label:
            continue
        parts = label.split(".")
        upper_parts = [part.upper() for part in parts]
        fuel = next((item for item in fuels if item in upper_parts), "")
        if not fuel:
            label_lower = label.casefold()
            fuel = next(
                (
                    item
                    for item in fuels
                    if re.search(rf"(?<![a-z0-9]){item.casefold()}(?![a-z0-9])", label_lower)
                ),
                "",
            )
        if not fuel:
            continue
        metric = str(parts[-1] if parts else "").strip()
        metric_key = _policy_metric_key(metric) or _powertrain_metric_key_from_label(label, ref)
        if not metric_key:
            continue
        bucket = result.setdefault(fuel, {})
        priority_bucket = priorities.setdefault(fuel, {})
        value = _format_evidence_ref_value(ref)
        if value:
            numeric_value = _numeric_ref_value(ref)
            if numeric_value is not None and numeric_value <= 0:
                continue
            priority = _powertrain_metric_source_priority(label, metric_key)
            existing_priority = priority_bucket.get(metric_key, -1)
            if metric_key not in bucket or priority >= existing_priority:
                bucket[metric_key] = value
                priority_bucket[metric_key] = priority
    return result


def _powertrain_metric_source_priority(label: str, metric_key: str) -> int:
    lower = str(label or "").casefold()
    metric = str(metric_key or "").strip().casefold()
    if "powertrainmix" in lower:
        return 50
    if any(token in lower for token in ("registrationbyfuel", "drivebyfuel")):
        return 45 if metric in {"business", "private", "2wd", "4wd"} else 35
    if ".sales" in lower or lower.endswith(" sales") or lower.endswith(".share"):
        if not any(token in lower for token in ("crosstabs", "segmentbyfuel", "drivebyfuel", "registrationbyfuel")):
            return 45
    if "segmentbyfuel" in lower:
        return 20
    return 10


def _policy_metric_key(metric: str) -> str:
    lower = str(metric or "").strip().lower()
    mapping = {
        "sales": "sales",
        "_total": "sales",
        "value": "sales",
        "share": "share",
        "mix": "share",
        "business_pct": "business",
        "private_pct": "private",
        "retail_pct": "private",
        "4wd_pct": "4wd",
        "2wd_pct": "2wd",
    }
    return mapping.get(lower, "")


def _powertrain_metric_key_from_label(label: str, ref: dict[str, Any]) -> str:
    lower = str(label or "").casefold()
    if "business" in lower or "fleet" in lower or "company" in lower:
        return "business"
    if "private" in lower or "retail" in lower:
        return "private"
    if "4wd" in lower or "awd" in lower:
        return "4wd"
    if "2wd" in lower:
        return "2wd"
    if any(token in lower for token in ("share", "mix", "penetration", "份额", "占比", "渗透")):
        return "share"
    if any(token in lower for token in ("sales", "volume", "registrations", "registration", "units", "销量")):
        return "sales"
    unit = str(ref.get("unit") or "").strip().casefold()
    if unit == "units":
        return "sales"
    return ""


def _policy_powertrain_line(fuel: str, stats: dict[str, str], *, include_channel: bool) -> str:
    if not stats:
        return ""
    parts: list[str] = []
    sales = str(stats.get("sales") or "").strip()
    share = str(stats.get("share") or "").strip()
    if sales:
        parts.append(f"{fuel} {sales}")
    elif share:
        parts.append(f"{fuel} share {share}")
    else:
        parts.append(fuel)
    if include_channel:
        channel_parts = []
        if stats.get("business"):
            channel_parts.append(f"Business {stats['business']}")
        if stats.get("private"):
            channel_parts.append(f"Private {stats['private']}")
        if channel_parts:
            parts.append("，".join(channel_parts))
    return "，".join(parts)


def _policy_drive_line(stats: dict[str, str]) -> str:
    drive_parts = []
    if stats.get("4wd"):
        drive_parts.append(f"4WD {stats['4wd']}")
    if stats.get("2wd"):
        drive_parts.append(f"2WD {stats['2wd']}")
    return "、".join(drive_parts)


def _policy_company_car_logic_lines(evidence_package: dict[str, Any]) -> tuple[str, str]:
    stats = _policy_powertrain_stats(evidence_package)
    bev_line = _policy_powertrain_line("BEV", stats.get("BEV", {}), include_channel=True)
    phev_line = _policy_powertrain_line("PHEV", stats.get("PHEV", {}), include_channel=True)
    bev_logic = "BEV 逻辑：更适合低使用成本、政策叙事和可充电条件稳定的公司车场景。"
    phev_logic = "PHEV 逻辑：只在长途、无稳定充电、高里程或低风险替代场景保留理由；如果 TCO/月供/残值算不出优势，就不应主推。"
    if bev_line:
        bev_logic = (
            f"BEV 逻辑：已查 {bev_line}，说明 BEV 可先承担公司车低使用成本、政策叙事和稳定充电场景；"
            "但仍要用月供、残值/RV 和税务 benefit 证明企业端真实成本。"
        )
    if phev_line:
        phev_logic = (
            f"PHEV 逻辑：已查 {phev_line}，说明 PHEV 更应作为公司车/TCO 条件验证线；"
            "只有长途、无稳定充电、高里程或低风险替代场景算得出优势，才适合主推。"
        )
    return bev_logic, phev_logic


def _policy_co2_phev_logic_line(evidence_package: dict[str, Any]) -> str:
    stats = _policy_powertrain_stats(evidence_package)
    phev = stats.get("PHEV", {})
    phev_line = _policy_powertrain_line("PHEV", phev, include_channel=True)
    drive_line = _policy_drive_line(phev)
    evidence_parts = [item for item in (phev_line, drive_line) if item]
    if evidence_parts:
        return (
            f"产品动作：已查 {'，'.join(evidence_parts)}，PHEV 应先保留为公司车/TCO 验证线；"
            "只有真实使用场景下总成本或风险比 HEV/BEV 更稳，才升级为主推。"
        )
    return "产品动作：PHEV 先保留为公司车/TCO 验证线；只有真实使用场景下总成本或风险比 HEV/BEV 更稳，才升级为主推。"


def _policy_segment_line(stats: dict[str, dict[str, str]], fuel: str) -> str:
    # Reserved for segment-level refs when tools expose them as `<segment>.<fuel>_pct`.
    return ""


def _compose_competitor_direct_answer(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    executive = _strip_direct_prefix(str(plan.get("executiveConclusion") or "").strip())
    if not executive:
        return ""
    question_text = _normalize_question_text(" ".join([executive, *plan.get("reportReadyBullets", [])]))
    action = ""
    if plan.get("recommendedActions"):
        action = _clean_action_text(str(plan["recommendedActions"][0].get("action") or ""))
    evidence_note = _evidence_count_note(evidence_ref_count(evidence_package))
    confidence_note = _confidence_label(str(evidence_package.get("confidence") or "low"))
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])
    country_label = _country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场"))
    positioning_brief = _competitor_positioning_evidence_brief(
        plan=plan,
        evidence_package=evidence_package,
        question_text=question_text,
    )

    fallback_action = "生成竞品对比表并补齐价格/配置证据"
    action = _competitor_safe_action(_topic_action(
        plan.get("recommendedActions", []),
        ("竞品", "对标", "compare", "competitor", "配置", "价格", "定位"),
        fallback_action,
    ), evidence_package) or fallback_action
    action = _competitor_next_action(action, country_label, evidence_package, question_text)
    digest = _evidence_digest_sentence(evidence_package, str(plan.get("intent") or ""), limit=4)
    generic_brief = _generic_competitor_evidence_brief(
        plan=plan,
        evidence_package=evidence_package,
        question_text=question_text,
    )
    if generic_brief:
        action = _public_direct_action_summary(action)
        display_action = _competitor_evidence_display_action(evidence_package)
        parts = [
            f"对标判断：{generic_brief}",
            f"关键证据：{country_label} {digest}。" if digest else "",
            display_action
            or "产品动作：把已查竞品证据先转成主对标、价格/配置校验锚点或销售替代对象；缺口模型不能写胜出，只能写待验证角色。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1250))
    if positioning_brief:
        parts = [
            f"对标判断：{positioning_brief}",
            f"关键证据：{country_label} {digest}。" if digest else "",
            "产品动作：把已验证的销量/价格/级别锚点先转成定位差异，再补目标车型价格、配置和用户场景；不要只停在“生成矩阵”。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1000))
    parts = [
        f"对标判断：{executive}",
        f"证据锚点：{digest}。" if digest else "",
        "竞品角色：先区分主对标、价格锚点、配置校验锚点和销售替代对象，避免把所有竞品等权比较。",
        "产品动作：把对比结果转成可赢点、短板、价格/配置边界和销售话术，而不是只列车型名称。",
        f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=900))


def _competitor_safe_action(action: str, evidence_package: dict[str, Any]) -> str:
    text = _clean_action_text(action)
    if not text:
        return ""
    if _action_mentions_unrequested_vehicle(text, evidence_package):
        return ""
    return text


def _competitor_next_action(
    action: str,
    country_label: str,
    evidence_package: dict[str, Any],
    question_text: str,
) -> str:
    cleaned = _strip_terminal_punctuation(_clean_action_text(action))
    generic_actions = {
        "",
        "生成竞品对比表",
        "生成竞品对比表和定位图",
        "生成竞品对比表并补齐价格/配置证据",
    }
    if cleaned and cleaned not in generic_actions:
        return cleaned
    targets, competitors = _competitor_requested_entities(evidence_package, question_text)
    target_text = " / ".join(targets[:2]) if targets else "目标车型"
    competitor_text = " / ".join(competitors[:3]) if competitors else "核心竞品"
    market = _market_label(country_label)
    scenario_dimensions = _competitor_scenario_dimensions(evidence_package, question_text)
    if scenario_dimensions:
        dimension_text = "、".join(scenario_dimensions)
        return (
            f"把{market} {target_text} vs {competitor_text} 做成车型级销量、MSRP、配置差异、月供/RV、"
            f"{dimension_text}对标表"
        )
    return f"把{market} {target_text} vs {competitor_text} 做成车型级销量、MSRP、配置差异和月供/RV 对标表"


def _competitor_positioning_evidence_brief(
    *,
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    question_text: str,
) -> str:
    groups = _competitor_metric_groups(evidence_package)
    sales_groups = [
        group
        for group in groups.values()
        if isinstance(group.get("salesValue"), (int, float))
    ]
    if len(sales_groups) < 2:
        return ""

    country_label = _market_label(_country_label(str(plan.get("country") or evidence_package.get("country") or "")))
    target_models, requested_competitors = _competitor_requested_entities(evidence_package, question_text)
    sorted_sales = sorted(
        sales_groups,
        key=lambda group: float(group.get("salesValue") or 0),
        reverse=True,
    )
    requested_models = [*target_models, *requested_competitors]
    requested_sales = [
        group
        for group in sorted_sales
        if _model_name_in_list(str(group.get("model") or ""), requested_models)
    ]
    if requested_models and not requested_sales:
        return ""
    sales_text = "、".join(
        f"{group['model']} {group['salesText']}"
        for group in (requested_sales or sorted_sales)[:4]
        if group.get("model") and group.get("salesText")
    )
    if not sales_text:
        return ""

    requested_with_sales = [
        group
        for group in (requested_sales or sorted_sales)
        if _model_name_in_list(str(group.get("model") or ""), requested_competitors)
    ]
    anchor = requested_with_sales[0] if requested_with_sales else (requested_sales or sorted_sales)[0]
    anchor_text = (
        f"{anchor['model']} 已有 {anchor['salesText']} 的销量锚点"
        if anchor.get("model") and anchor.get("salesText")
        else "已查请求车型有销量锚点"
    )

    missing_models = [
        model
        for model in [*target_models, *requested_competitors]
        if model and not _competitor_group_has_direct_metric(groups.get(_model_key(model)))
    ]
    missing_text = ""
    if missing_models:
        missing_text = (
            f"；但 {', '.join(_dedupe(missing_models)[:4])} 没有直接销量/MSRP，"
            "不能写成已验证的正面对抗"
        )

    target_text = target_models[0] if target_models else "目标车型"
    competitor_text = " / ".join(requested_competitors[:3]) if requested_competitors else "已查竞品"
    return (
        f"{country_label} {target_text} 与 {competitor_text} 的差异应先写成错位定位判断："
        f"{anchor_text}，可作为请求竞品的市场强度参考；"
        f"已查销量锚点包括 {sales_text}{missing_text}。"
    )


def _generic_competitor_evidence_brief(
    *,
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    question_text: str,
    include_empty_hypothesis: bool = False,
) -> str:
    groups = _competitor_metric_groups(evidence_package)
    country_label = _market_label(_country_label(str(plan.get("country") or evidence_package.get("country") or "")))
    target_models, requested_competitors = _competitor_requested_entities(evidence_package, question_text)
    requested_models = _dedupe([*target_models, *requested_competitors])
    if not requested_models:
        return ""
    target_text = " / ".join(target_models[:2]) if target_models else "目标车型"
    competitor_text = " / ".join(requested_competitors[:3]) if requested_competitors else "已查竞品"
    market_context = _competitor_market_context_note(evidence_package, question_text=question_text)
    if not groups:
        if not market_context:
            if not include_empty_hypothesis:
                return ""
            boundary = _competitor_missing_boundary(evidence_package)
            boundary_text = (
                boundary
                or "目标车型与请求竞品的直接销量、官方 MSRP、配置差异和月供/RV"
            )
            return (
                f"对标假设：{country_label} {target_text} 与 {competitor_text} 当前不能判断主对标或价格/配置校验锚点；"
                f"证据边界：待补 {boundary_text}。"
                "第一版只能保留请求竞品池和验证路径，不能按车型名称套固定主/辅对标模板，也不能写成已验证胜负。"
            )
        boundary = _competitor_missing_boundary(evidence_package)
        market_verdict = _competitor_market_context_business_verdict(
            country_label=country_label,
            target_text=target_text,
            competitor_text=competitor_text,
            question_text=question_text,
            evidence_package=evidence_package,
            boundary=boundary,
        )
        if market_verdict:
            return market_verdict
        boundary_sentence = f"证据边界：{boundary}。" if boundary else ""
        return (
            f"{country_label} {target_text} 与 {competitor_text} 的第一版结论不是“已胜出”，而是“有场景型切入理由”；"
            f"已查市场场景证据包括 {market_context}。"
            f"{boundary_sentence}"
            "这些证据能说明机会场景和验证方向，但不能替代车型级销量、MSRP、配置差异或 TCO 证据。"
        )
    requested_groups: list[dict[str, Any]] = []
    for model in requested_models:
        group = groups.get(_model_key(model))
        if group and _competitor_group_has_direct_metric(group):
            requested_groups.append(group)
    if not requested_groups:
        if market_context:
            boundary = _competitor_missing_boundary(evidence_package)
            market_verdict = _competitor_market_context_business_verdict(
                country_label=country_label,
                target_text=target_text,
                competitor_text=competitor_text,
                question_text=question_text,
                evidence_package=evidence_package,
                boundary=boundary,
            )
            if market_verdict:
                return market_verdict
            boundary_sentence = f"证据边界：{boundary}。" if boundary else ""
            return (
                f"{country_label} {target_text} 与 {competitor_text} 当前缺少直接车型级销量/MSRP/配置证据；"
                f"但第一版结论不是停止在工具计划，而是把已查市场场景证据转成场景型对标假设：{market_context}。"
                f"{boundary_sentence}"
                "这些证据能支撑可打理由和销售场景，但不能直接写车型级胜负。"
            )
        source_groups = [
            group
            for model in requested_models
            if (group := groups.get(_model_key(model))) and _competitor_group_has_source_status(group)
        ]
        if source_groups:
            source_text = "；".join(
                phrase
                for phrase in (_competitor_group_source_status_phrase(group) for group in source_groups[:3])
                if phrase
            )
            return (
                f"{country_label} {target_text} 与 {competitor_text} 现在不能直接写胜负或价差；"
                f"当前只有待物化来源线索：{source_text}。"
                "这些线索只能说明下一步可以审核/入库价格来源，不能当作已验证销量、MSRP 或配置差异。"
                "第一版结论应先输出竞品池和补证路径。"
            )
        return (
            f"{country_label} {target_text} 与 {competitor_text} 现在缺少直接车型级销量/MSRP/配置证据，"
            "不能用未请求车型、相邻竞品池或市场年序列替代请求竞品结论。"
            "第一版只应输出竞品池定义和补证路径，不能直接写胜负或价差。"
        )

    anchor = sorted(requested_groups, key=_competitor_group_evidence_score, reverse=True)[0]
    evidence_phrases = [
        phrase
        for phrase in (_competitor_group_evidence_phrase(group) for group in requested_groups[:4])
        if phrase
    ]
    missing_models = [
        model
        for model in requested_models
        if model and not _competitor_group_has_direct_metric(groups.get(_model_key(model)))
    ]
    anchor_text = _competitor_group_evidence_phrase(anchor)
    missing_text = (
        f"；但 {'、'.join(_dedupe(missing_models)[:4])} 仍缺直接销量/MSRP/配置证据，不能写成已验证胜出"
        if missing_models
        else ""
    )
    evidence_text = f"已查证据包括 {'；'.join(evidence_phrases)}" if evidence_phrases else "已有部分竞品证据"
    price_positioning = _competitor_price_positioning_sentence(
        target_models=target_models,
        competitors=requested_competitors,
        groups=groups,
    )
    price_sentence = f"{price_positioning}" if price_positioning else ""
    boundary = _competitor_missing_boundary(evidence_package)
    boundary_sentence = f"证据边界：{boundary}。" if boundary else ""
    return (
        f"{country_label} {target_text} 与 {competitor_text} 不应等权罗列；"
        f"当前最可引用锚点是 {anchor_text}。"
        f"{evidence_text}{missing_text}。"
        f"{price_sentence}"
        f"{boundary_sentence}"
        "因此第一版结论应围绕对标角色和验证路径，而不是直接写胜负。"
    )


def _is_competitor_report_scope(evidence_package: dict[str, Any], question_text: str) -> bool:
    targets, competitors = _competitor_requested_entities(evidence_package, question_text)
    return bool(targets and competitors)


def _competitor_report_subject_label(evidence_package: dict[str, Any], question_text: str) -> str:
    targets, competitors = _competitor_requested_entities(evidence_package, question_text)
    target_text = " / ".join(targets[:2]) if targets else "目标车型"
    competitor_text = " / ".join(competitors[:3]) if competitors else "核心竞品"
    return f"{target_text} vs {competitor_text}"


def _competitor_report_action_label(evidence_package: dict[str, Any], question_text: str) -> str:
    return f"生成 {_competitor_report_subject_label(evidence_package, question_text)} 一页竞品对标框架"


def _competitor_report_evidence_action_label(evidence_package: dict[str, Any], question_text: str) -> str:
    subject = _competitor_report_subject_label(evidence_package, question_text)
    return f"补齐 {subject} 的官方 MSRP、竞品价格走廊、版本、关键配置、月供/RV 和来源日期"


def _competitor_report_has_evidence_gap(evidence_package: dict[str, Any], question_text: str) -> bool:
    targets, competitors = _competitor_requested_entities(evidence_package, question_text)
    requested_models = _dedupe([*targets, *competitors])
    groups = _competitor_metric_groups(evidence_package)
    if not requested_models:
        return True
    if any(
        not _competitor_group_has_direct_metric(groups.get(_model_key(model)))
        for model in requested_models
    ):
        return True
    return bool(_competitor_missing_boundary(evidence_package))


def _generic_competitor_report_brief(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    question_text: str,
) -> str:
    return _generic_competitor_evidence_brief(
        plan={
            "country": country_label,
            "intent": "report_generation",
            "recommendedActions": [],
        },
        evidence_package=evidence_package,
        question_text=question_text,
        include_empty_hypothesis=True,
    )


def _competitor_market_context_note(evidence_package: dict[str, Any], *, question_text: str = "") -> str:
    metric_specs = _competitor_market_context_metric_specs(evidence_package, question_text)
    parts: list[str] = []
    for label, table, row, metric in metric_specs:
        value = _market_cross_tab_positive_ref_value(evidence_package, table=table, row=row, metric=metric)
        if value:
            parts.append(f"{label} {value}")
    return "，".join(_dedupe(parts)[:5])


def _competitor_market_context_metric_specs(
    evidence_package: dict[str, Any],
    question_text: str,
) -> list[tuple[str, str, str, str]]:
    if _competitor_prefers_large_suv_electrified_context(evidence_package, question_text):
        return [
            ("SUV B 四驱占比", "driveBySegment", "SUV B", "4WD_pct"),
            ("PHEV 四驱占比", "driveByFuel", "PHEV", "4WD_pct"),
            ("SUV B PHEV 渗透率", "segmentByFuel", "SUV B", "PHEV_pct"),
            ("PHEV 公司车注册占比", "registrationByFuel", "PHEV", "Business_pct"),
            ("SUV B 公司车注册占比", "registrationBySegment", "SUV B", "Business_pct"),
            ("SUV B 注册量", "driveBySegment", "SUV B", "sales"),
            ("SUV A 四驱占比", "driveBySegment", "SUV A", "4WD_pct"),
            ("SUV A PHEV 渗透率", "segmentByFuel", "SUV A", "PHEV_pct"),
        ]
    return [
        ("SUV A 注册量", "driveBySegment", "SUV A", "sales"),
        ("SUV A 四驱占比", "driveBySegment", "SUV A", "4WD_pct"),
        ("SUV A HEV 渗透率", "segmentByFuel", "SUV A", "HEV_pct"),
        ("SUV A PHEV 渗透率", "segmentByFuel", "SUV A", "PHEV_pct"),
        ("HEV 私人注册占比", "registrationByFuel", "HEV", "Private_pct"),
        ("HEV 公司车注册占比", "registrationByFuel", "HEV", "Business_pct"),
        ("SUV A 公司车注册占比", "registrationBySegment", "SUV A", "Business_pct"),
        ("SUV B 四驱占比", "driveBySegment", "SUV B", "4WD_pct"),
        ("SUV B PHEV 渗透率", "segmentByFuel", "SUV B", "PHEV_pct"),
        ("PHEV 公司车注册占比", "registrationByFuel", "PHEV", "Business_pct"),
    ]


def _competitor_market_context_business_verdict(
    *,
    country_label: str,
    target_text: str,
    competitor_text: str,
    question_text: str,
    evidence_package: dict[str, Any],
    boundary: str,
) -> str:
    market_context = _competitor_market_context_note(evidence_package, question_text=question_text)
    if not market_context:
        return ""
    scenario_reason = _competitor_market_context_scenario_reason(evidence_package, question_text=question_text)
    if not scenario_reason:
        return ""
    question_is_attack = _contains_any(
        question_text,
        ("能打", "打 ", "对抗", "挑战", "正面", "compete", "against", "vs"),
    )
    stance = (
        f"{target_text} 可以先按场景型挑战者去打 {competitor_text}"
        if question_is_attack
        else f"{target_text} 可以先写成 {competitor_text} 的场景型对标假设"
    )
    boundary_sentence = f"证据边界：{boundary}。" if boundary else ""
    return (
        f"{country_label} {target_text} 与 {competitor_text} 的第一版结论不是“已胜出”，而是“有场景型切入理由”："
        f"{stance}，因为 {scenario_reason}。"
        f"{boundary_sentence}"
        "这些证据能支撑可打理由和销售场景，但不能替代车型级销量、MSRP、配置差异或 TCO 证据。"
    )


def _competitor_market_context_scenario_reason(evidence_package: dict[str, Any], *, question_text: str = "") -> str:
    reason_parts: list[str] = []
    suv_b_or_phev_context = _competitor_prefers_large_suv_electrified_context(
        evidence_package,
        question_text,
    )
    suv_a_sales = _market_cross_tab_positive_ref_value(evidence_package, table="driveBySegment", row="SUV A", metric="sales")
    suv_b_sales = _market_cross_tab_positive_ref_value(evidence_package, table="driveBySegment", row="SUV B", metric="sales")
    suv_a_4wd = _market_cross_tab_positive_ref_value(evidence_package, table="driveBySegment", row="SUV A", metric="4WD_pct")
    suv_b_4wd = _market_cross_tab_positive_ref_value(evidence_package, table="driveBySegment", row="SUV B", metric="4WD_pct")
    suv_a_phev = _market_cross_tab_positive_ref_value(evidence_package, table="segmentByFuel", row="SUV A", metric="PHEV_pct")
    suv_b_phev = _market_cross_tab_positive_ref_value(evidence_package, table="segmentByFuel", row="SUV B", metric="PHEV_pct")
    phev_4wd = _market_cross_tab_positive_ref_value(evidence_package, table="driveByFuel", row="PHEV", metric="4WD_pct")
    phev_business = _market_cross_tab_positive_ref_value(evidence_package, table="registrationByFuel", row="PHEV", metric="Business_pct")

    if suv_b_or_phev_context and suv_b_sales:
        reason_parts.append(f"SUV B 有 {suv_b_sales} 规模")
    elif suv_a_sales:
        reason_parts.append(f"SUV A 有 {suv_a_sales} 规模")
    elif suv_b_sales:
        reason_parts.append(f"SUV B 有 {suv_b_sales} 规模")
    if suv_b_or_phev_context and suv_b_4wd:
        reason_parts.append(f"SUV B 四驱需求占比达到 {suv_b_4wd}")
    elif suv_a_4wd:
        reason_parts.append(f"SUV A 四驱需求占比达到 {suv_a_4wd}")
    elif suv_b_4wd:
        reason_parts.append(f"SUV B 四驱需求占比达到 {suv_b_4wd}")
    if suv_b_or_phev_context and phev_4wd:
        reason_parts.append(f"PHEV 四驱占比达到 {phev_4wd}")
    if suv_b_or_phev_context and suv_b_phev:
        reason_parts.append(f"SUV B PHEV 渗透率达到 {suv_b_phev}")
    elif suv_a_phev:
        reason_parts.append(f"SUV A PHEV 渗透率达到 {suv_a_phev}")
    elif suv_b_phev:
        reason_parts.append(f"SUV B PHEV 渗透率达到 {suv_b_phev}")
    if phev_business:
        reason_parts.append(f"PHEV 公司车注册占比达到 {phev_business}")
    return "，".join(_dedupe(reason_parts)[:5])


def _competitor_structural_context_text(evidence_package: dict[str, Any], question_text: str) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    entity_parts: list[str] = []
    for key in ("segments", "powertrains", "features", "useCases", "channels", "bodyStyles"):
        value = entities.get(key)
        if isinstance(value, list):
            entity_parts.extend(str(item or "") for item in value)
        elif isinstance(value, str):
            entity_parts.append(value)
    evidence_parts = [
        " ".join(str(ref.get(key) or "") for key in ("label", "value"))
        for ref in _all_evidence_refs(evidence_package)
        if isinstance(ref, dict)
    ]
    return _normalize_question_text(" ".join([question_text, *entity_parts, *evidence_parts]))


def _competitor_prefers_large_suv_electrified_context(
    evidence_package: dict[str, Any],
    question_text: str,
) -> bool:
    context = _competitor_structural_context_text(evidence_package, question_text)
    return _contains_any(
        context,
        (
            "7座", "7 座", "七座", "7 seats", "suv b", "phev", "四驱", "4wd", "awd",
            "company car", "公司车", "大客户", "fleet",
        ),
    )


def _competitor_scenario_dimensions(evidence_package: dict[str, Any], question_text: str) -> list[str]:
    context = _competitor_structural_context_text(evidence_package, question_text)
    dimensions: list[str] = []
    if _contains_any(context, ("7座", "7 座", "七座", "7 seats", "seat layout", "seats")):
        dimensions.append("座位布局")
    if _contains_any(context, ("四驱", "4wd", "awd")):
        dimensions.append("四驱")
    if _contains_any(context, ("company car", "公司车", "大客户", "fleet", "business_pct")):
        dimensions.append("公司车/TCO")
    if _contains_any(context, ("家庭", "长途", "family", "long distance", "touring")):
        dimensions.append("家庭长途场景")
    if _contains_any(context, ("拖车", "拖拽", "towing", "towbar", "roof load")):
        dimensions.append("拖拽/载重")
    if _contains_any(context, ("冬季", "winter", "snow", "低温")):
        dimensions.append("冬季适应性")
    return _dedupe(dimensions)


def _competitor_group_evidence_score(group: dict[str, Any]) -> int:
    score = 0
    if group.get("salesText"):
        score += 6
    if group.get("shareText"):
        score += 5
    if group.get("priceText"):
        score += 4
    if group.get("segment"):
        score += 2
    if group.get("powertrain"):
        score += 2
    return score


def _competitor_group_evidence_phrase(group: dict[str, Any]) -> str:
    model = str(group.get("model") or "").strip()
    if not model:
        return ""
    parts: list[str] = []
    if group.get("salesText"):
        parts.append(f"销量 {group['salesText']}")
    if group.get("shareText"):
        parts.append(f"份额 {group['shareText']}")
    if group.get("priceText"):
        parts.append(f"价格 {group['priceText']}")
    if group.get("segment"):
        parts.append(f"级别 {group['segment']}")
    if group.get("powertrain"):
        parts.append(f"动力 {group['powertrain']}")
    if not parts:
        return ""
    return f"{model}（{'、'.join(parts[:4])}）"


def _competitor_missing_boundary(evidence_package: dict[str, Any]) -> str:
    missing_names = _missing_evidence_names(evidence_package)
    lines: list[str] = []
    if missing_names & {
        "configuration_delta",
        "feature_diff",
        "key_features",
        "trim",
        "competitive_or_configuration_data_unavailable",
        "coverage_diagnostic:no_config_projects_for_country",
        "coverage_diagnostic:no_vehicle_variant_data_for_requested_models",
    }:
        lines.append("配置差异 = 待补逐项配置 / 版本 / 价值差异")
    if missing_names & {"monthly_payment", "leasing_payment", "rv", "residual_value"}:
        lines.append("月供/RV = 待补 leasing、残值和 company car 成本口径")
    if missing_names & {
        "current_msrp",
        "own_model_price",
        "competitor_price_range",
        "competitor_corridor",
        "coverage_diagnostic:no_current_prices_for_requested_models",
    }:
        lines.append("价格 = 待补本车型和核心竞品官方 MSRP / 当前价格来源")
    return "；".join(_dedupe(lines))


def _competitor_evidence_display_action(evidence_package: dict[str, Any]) -> str:
    market_context = _competitor_market_context_note(evidence_package)
    if market_context:
        boundary = _competitor_missing_boundary(evidence_package)
        boundary_clause = f"缺口矩阵需列出 {boundary}" if boundary else "缺口矩阵需列出车型级价格、配置、TCO 和来源状态"
        return (
            "产品动作：把已查市场场景证据先转成场景型可打理由和销售切入点，"
            f"核心证据是 {market_context}。"
            f"输出上用 metric cards / 场景证据表承载这些数字，{boundary_clause}；"
            "缺口模型不能写胜出，只能写待验证角色。"
        )
    groups = _competitor_metric_groups(evidence_package)
    usable_groups = [group for group in groups.values() if _competitor_group_has_direct_metric(group)]
    if usable_groups:
        fields: list[str] = []
        if any(group.get("salesText") for group in usable_groups):
            fields.append("销量/份额")
        if any(group.get("priceText") for group in usable_groups):
            fields.append("价格")
        if any(group.get("segment") for group in usable_groups):
            fields.append("级别")
        if any(group.get("powertrain") for group in usable_groups):
            fields.append("动力")
        field_text = "、".join(_dedupe(fields)) or "已查证据"
        return (
            "产品动作：把已查竞品证据先转成主对标、价格/配置校验锚点或销售替代对象；"
            f"展示方式：把可引用竞品锚点做成对比表，字段至少包括车型、{field_text}和证据来源；"
            "缺价格/配置/TCO 的模型只标为待验证角色。"
        )
    return ""


def _competitor_price_positioning_sentence(
    *,
    target_models: list[str],
    competitors: list[str],
    groups: dict[str, dict[str, Any]],
) -> str:
    target_groups = [
        groups.get(_model_key(model))
        for model in target_models
        if groups.get(_model_key(model)) and isinstance(groups.get(_model_key(model), {}).get("priceValue"), (int, float))
    ]
    competitor_groups = [
        groups.get(_model_key(model))
        for model in competitors
        if groups.get(_model_key(model)) and isinstance(groups.get(_model_key(model), {}).get("priceValue"), (int, float))
    ]
    if not target_groups or not competitor_groups:
        return ""
    target = target_groups[0] or {}
    target_model = str(target.get("model") or target_models[0] or "目标车型")
    target_value = float(target.get("priceValue") or 0)
    target_price = str(target.get("priceText") or _format_price_number(target_value)).strip()
    competitor_values = [float(group.get("priceValue") or 0) for group in competitor_groups if group]
    if not competitor_values:
        return ""
    low = min(competitor_values)
    high = max(competitor_values)
    comp_text = " / ".join(
        f"{group.get('model')} {group.get('priceText')}"
        for group in competitor_groups[:3]
        if group and group.get("model") and group.get("priceText")
    )
    comp_clause = f"；竞品价格锚点为 {comp_text}" if comp_text else ""
    if target_value < low:
        return (
            f"价格位置：{target_model} {target_price} 低于已查竞品价格下沿 {_format_price_number(low)}，"
            "可以先写成价格锚点/低风险进入假设，但还需要配置、TCO、品牌和残值证据证明不是单纯低价。"
            f"{comp_clause}。"
        )
    if target_value > high:
        return (
            f"价格位置：{target_model} {target_price} 高于已查竞品价格上沿 {_format_price_number(high)}，"
            "必须用配置、尺寸/级别、动力、质保、TCO 或渠道价值解释溢价，否则正面对抗风险偏高。"
            f"{comp_clause}。"
        )
    return (
        f"价格位置：{target_model} {target_price} 落在已查竞品价格带 {_format_price_number(low)}-{_format_price_number(high)} 内，"
        "可以进入主销版本和配置价值验证；下一步重点不是再确认方向，而是补齐配置差异、月供/RV 和销售话术。"
        f"{comp_clause}。"
    )


def _competitor_metric_groups(evidence_package: dict[str, Any]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip()
        if not label:
            continue
        lower_label = label.casefold()
        if "yearseries" in lower_label or lower_label.startswith("contextsnapshot."):
            continue
        model = _competitor_ref_model(label, ref.get("value"))
        if not model:
            continue
        key = _model_key(model)
        group = groups.setdefault(key, {"model": model})
        metric = label.lower().split(".")[-1]
        if metric in {"sales", "value", "volume", "count"}:
            numeric_value = _numeric_ref_value(ref)
            if numeric_value is not None and numeric_value > 0:
                group["salesValue"] = numeric_value
                group["salesText"] = _format_evidence_ref_value({**ref, "value": numeric_value})
        elif metric == "share":
            group["shareText"] = _format_evidence_ref_value(ref)
        elif metric in {"avgprice", "price", "msrp", "minprice", "maxprice"}:
            numeric_value = _numeric_ref_value(ref)
            if numeric_value is not None:
                group["priceValue"] = numeric_value
            group["priceText"] = _format_evidence_ref_value(ref)
        elif metric == "segment":
            group["segment"] = _format_evidence_ref_value(ref)
        elif metric == "powertrain":
            group["powertrain"] = _format_evidence_ref_value(ref)
        elif metric == "priceevidencestatus":
            group["priceEvidenceStatus"] = _format_evidence_ref_value(ref)
        elif metric == "sourcedraftpath":
            group["sourceDraftPath"] = _format_evidence_ref_value(ref)
    return groups


def _competitor_group_has_source_status(group: dict[str, Any] | None) -> bool:
    if not group:
        return False
    return bool(str(group.get("priceEvidenceStatus") or "").strip() or str(group.get("sourceDraftPath") or "").strip())


def _competitor_group_source_status_phrase(group: dict[str, Any]) -> str:
    model = str(group.get("model") or "").strip()
    if not model:
        return ""
    status = str(group.get("priceEvidenceStatus") or "").strip()
    path = str(group.get("sourceDraftPath") or "").strip()
    parts = []
    if status:
        parts.append(_public_price_source_status(status))
    if path and "source_draft" not in status.casefold():
        parts.append("来源草稿待审核")
    if not parts:
        return ""
    return f"{model}（{'、'.join(_dedupe(parts))}）"


def _public_price_source_status(status: str) -> str:
    normalized = _normalize_space(str(status or "")).casefold()
    mapping = {
        "source_draft_available": "MSRP 来源草稿待审核",
        "source draft available": "MSRP 来源草稿待审核",
        "candidate_search_query": "官方价格来源搜索候选待验证",
        "accepted_current_price": "已有当前价格记录",
        "review_pending": "价格观察待审核",
    }
    return mapping.get(normalized, "价格来源待验证")


def _competitor_ref_model(label: str, value: Any) -> str:
    text = str(label or "").strip()
    lower = text.lower()
    if lower.startswith("competitor.") and lower.endswith(".model"):
        return str(value or "").strip()
    parts = text.split(".")
    if len(parts) >= 2:
        metric = parts[-1].lower()
        if metric in {
            "sales", "value", "share", "volume", "count", "avgprice",
            "price", "msrp", "minprice", "maxprice", "segment", "powertrain",
            "priceevidencestatus", "sourcedraftpath",
        }:
            return ".".join(parts[:-1]).strip()
    return ""


def _competitor_requested_entities(
    evidence_package: dict[str, Any],
    question_text: str,
) -> tuple[list[str], list[str]]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    competitors = [
        str(item or "").strip()
        for item in (entities.get("competitors") if isinstance(entities.get("competitors"), list) else [])
        if str(item or "").strip()
    ]
    raw_targets = [
        str(item or "").strip()
        for item in (entities.get("models") if isinstance(entities.get("models"), list) else [])
        if str(item or "").strip()
    ]
    targets = [
        model
        for model in raw_targets
        if not _model_name_in_list(model, competitors)
    ]
    if not targets and raw_targets:
        targets = [raw_targets[0]]
    if targets or competitors:
        return _dedupe(targets), _dedupe(competitors)
    for token in re.findall(r"\b[A-Z][A-Z0-9-]{1,}(?:\s+[A-Z][A-Z0-9-]{1,})?\b", question_text.upper()):
        value = _normalize_space(token)
        if not value or _is_non_model_business_token(value):
            continue
        if not targets:
            targets.append(value)
        elif not _model_name_in_list(value, targets) and not _model_name_in_list(value, competitors):
            competitors.append(value)
    return _dedupe(targets), _dedupe(competitors)


def _is_non_model_business_token(value: str) -> bool:
    token = _model_key(value)
    return token in {
        "msrp",
        "tp",
        "rv",
        "tco",
        "url",
        "bev",
        "phev",
        "hev",
        "mhev",
        "ice",
        "awd",
        "4wd",
        "2wd",
        "suv",
        "mpv",
        "eur",
        "sek",
        "units",
        "jato",
        "kpi",
        "sales",
        "sale",
        "segment",
        "segments",
        "share",
        "volume",
        "count",
        "value",
        "price",
        "avgprice",
        "minprice",
        "maxprice",
        "powertrain",
        "leasing",
        "lease",
        "company",
        "companycar",
        "fleet",
        "business",
        "monthly",
        "payment",
        "residual",
        "configuration",
        "config",
        "feature",
        "ppt",
        "pptreadyblock",
        "title",
        "keymessage",
        "evidence",
        "product",
        "implication",
        "productimplication",
        "nextaction",
    }


def _competitor_group_has_direct_metric(group: dict[str, Any] | None) -> bool:
    if not group:
        return False
    return any(
        str(group.get(key) or "").strip()
        for key in ("salesText", "shareText", "priceText", "segment", "powertrain")
    )


def _model_name_in_list(model: str, values: list[str]) -> bool:
    key = _model_key(model)
    return bool(key) and any(_model_keys_match(key, _model_key(value)) for value in values)


def _model_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _model_keys_match(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    longer, shorter = (left, right) if len(left) > len(right) else (right, left)
    if len(shorter) < 3:
        return False
    return longer.endswith(shorter)


def _compose_report_generation_direct_answer(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    context = _normalize_question_text(" ".join([
        str(plan.get("executiveConclusion") or ""),
        *[str(item or "") for item in plan.get("reportReadyBullets", [])],
    ]))
    question_context = _report_plan_question_context(plan) or context
    country_label = _country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场"))
    action = ""
    if plan.get("recommendedActions"):
        action = _clean_action_text(str(plan["recommendedActions"][0].get("action") or ""))
    evidence_note = _evidence_count_note(evidence_ref_count(evidence_package))
    confidence_note = _confidence_label(str(evidence_package.get("confidence") or "low"))
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])
    method = plan.get("methodDistillation") if isinstance(plan.get("methodDistillation"), dict) else None

    if _is_policy_report_context(question_context, evidence_package):
        policy_direct = _compose_policy_direct_answer(plan, evidence_package)
        if policy_direct:
            return policy_direct

    if _is_bev_penetration_report(question_context):
        report_action = (
            action
            if _action_matches_topic(action, ("bev", "渗透", "趋势", "产品定义", "policy", "政策"))
            else "补齐 BEV 趋势和驱动因素证据，再生成一页产品定义建议 PPT block"
        )
        return _bev_penetration_report_direct_answer(
            country_label=country_label,
            evidence_package=evidence_package,
            action=report_action,
            alignment_note=alignment_note,
            evidence_note=evidence_note,
            confidence_note=confidence_note,
        )

    if method and _question_mentions_method_model(method, question_context or context):
        return _compose_pricing_method_report_direct(plan, method, evidence_package)

    generic_parts = _generic_report_generation_parts(
        plan=plan,
        evidence_package=evidence_package,
        country_label=country_label,
        question_text=question_context,
        action=action,
        alignment_note=alignment_note,
        evidence_note=evidence_note,
        confidence_note=confidence_note,
    )
    if generic_parts and _generic_competitor_evidence_brief(
        plan=plan,
        evidence_package=evidence_package,
        question_text=_generic_report_scope_text(evidence_package, question_context),
        include_empty_hypothesis=True,
    ):
        parts = [
            f"一页汇报结论：{generic_parts['keyMessage']}",
            generic_parts["evidence"],
            generic_parts["implication"],
            f"{generic_parts['nextAction']}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1150))

    if generic_parts:
        parts = [
            f"一页汇报结论：{generic_parts['keyMessage']}",
            generic_parts["evidence"],
            generic_parts["implication"],
            f"{generic_parts['nextAction']}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1150))

    return ""


def _generic_report_generation_parts(
    *,
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    country_label: str,
    question_text: str,
    action: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> dict[str, str]:
    refs = _all_evidence_refs(evidence_package)
    missing_note = _missing_evidence_note(evidence_package)
    missing_text = (
        f"当前缺口是{missing_note}，不能写成最终确定胜负"
        if missing_note and missing_note != "可引用证据"
        else ""
    )
    digest = _evidence_digest_sentence(evidence_package, "report_generation", limit=4)
    if not refs and not missing_text:
        return {}

    scope_text = _generic_report_scope_text(evidence_package, question_text)
    competitor_brief = _generic_competitor_evidence_brief(
        plan=plan,
        evidence_package=evidence_package,
        question_text=scope_text,
        include_empty_hypothesis=True,
    )
    subject = _generic_report_subject(evidence_package, scope_text)
    if competitor_brief:
        key_message = competitor_brief
        price_implication = _generic_report_price_product_implication(evidence_package, scope_text)
        implication = (
            f"Product implication：{price_implication}"
            if price_implication
            else (
                "Product implication：第一版报告应把已证实的竞品锚点和待补的目标车型价格/配置证据分开，"
                "先给对标角色、验证路径和销售话术方向，不要把缺证据的车型写成确定胜出。"
            )
        )
    else:
        key_message = (
            f"{country_label} {subject} 这页应先给可由证据支撑的业务结论，"
            "再把证据、缺口、产品含义和下一步动作压成一页。"
        )
        implication = (
            "Product implication：报告页需要把已查数据转成产品、价格、配置、渠道或汇报动作；"
            "缺证据的数字和胜负判断只写成验证项。"
        )

    evidence_items = digest or _evidence_label_note(refs)
    evidence = f"Evidence：{evidence_items}"
    if missing_text:
        evidence = f"{evidence}；{missing_text}；展示时应把缺口放进 missing-evidence matrix"
    else:
        evidence = f"{evidence}；证据状态为{alignment_note}；展示时应同步输出 evidence table / report block"
    next_action = _generic_report_next_action(action, evidence_package)
    return {
        "keyMessage": _clean_business_text(key_message),
        "evidence": _clean_business_text(evidence),
        "implication": _clean_business_text(implication),
        "nextAction": _clean_business_text(f"Next action：{next_action}"),
        "subject": subject,
        "evidenceNote": evidence_note,
        "confidenceNote": confidence_note,
    }


def _generic_report_price_product_implication(evidence_package: dict[str, Any], question_text: str) -> str:
    groups = _competitor_metric_groups(evidence_package)
    if not groups:
        return ""
    target_models, requested_competitors = _competitor_requested_entities(evidence_package, question_text)
    target_groups = [
        groups.get(_model_key(model))
        for model in target_models
        if groups.get(_model_key(model)) and isinstance(groups.get(_model_key(model), {}).get("priceValue"), (int, float))
    ]
    competitor_groups = [
        groups.get(_model_key(model))
        for model in requested_competitors
        if groups.get(_model_key(model)) and isinstance(groups.get(_model_key(model), {}).get("priceValue"), (int, float))
    ]
    if not target_groups and not competitor_groups:
        return ""
    target_label = " / ".join(target_models[:2]) if target_models else "目标车型"
    if not target_groups and competitor_groups:
        competitor_text = " / ".join(
            str(group.get("model") or "")
            for group in competitor_groups[:3]
            if group
        )
        return (
            f"先用 {competitor_text} 建立竞品价格锚点，再补 {target_label} 官方 MSRP、配置差异和月供/RV；"
            "缺目标车型价格时不能写确定胜负。"
        )
    if not competitor_groups:
        return (
            f"{target_label} 已有价格锚点，但竞品价格走廊仍缺口；报告页只能先写目标价验证，"
            "不能写成已验证正面对抗。"
        )
    target = target_groups[0] or {}
    target_model = str(target.get("model") or target_label)
    target_value = float(target.get("priceValue") or 0)
    competitor_values = [float(group.get("priceValue") or 0) for group in competitor_groups if group]
    if not competitor_values:
        return ""
    low = min(competitor_values)
    high = max(competitor_values)
    if target_value < low:
        return (
            f"{target_model} 可作为低位切入/价格锚点，但必须在同一页证明配置、质保/售后、月供/RV 和销售话术，"
            "否则低价会被理解成低价值。"
        )
    if target_value > high:
        return (
            f"{target_model} 高于已查竞品价格上沿，报告页必须给出尺寸/级别、配置、动力、TCO 或渠道价值的溢价证明；"
            "证据不足时应拆高低配或下调目标价。"
        )
    return (
        f"{target_model} 已进入已查竞品价格带，报告页应把价格位置、配置差异、月供/RV 和成交支持压成一页，"
        "重点验证主销版本而不是继续泛泛罗列竞品。"
    )


def _generic_report_subject(evidence_package: dict[str, Any], question_text: str) -> str:
    targets, competitors = _competitor_requested_entities(evidence_package, question_text)
    if targets and competitors:
        return f"{' / '.join(targets[:2])} 对标 {' / '.join(competitors[:3])}"
    requested = _requested_entity_names_from_package(evidence_package)
    if requested:
        return " / ".join(requested[:4])
    return _display_question_subject(question_text, max_chars=60)


def _report_plan_question_context(plan: BusinessSynthesisPlan) -> str:
    executive = str(plan.get("executiveConclusion") or "")
    match = re.search(r"分析对象\s*[：:]\s*(.+?)(?:。|$)", executive)
    if match:
        return _normalize_question_text(match.group(1))
    return ""


def _generic_report_scope_text(evidence_package: dict[str, Any], question_text: str) -> str:
    requested = _requested_entity_names_from_package(evidence_package)
    if requested:
        return " ".join(requested)
    text = str(question_text or "")
    return re.sub(
        r"\b(?:title|key message|evidence|product implication|next action|ppt-ready block|units|jato)\b",
        " ",
        text,
        flags=re.IGNORECASE,
    )


def _generic_report_next_action(action: str, evidence_package: dict[str, Any]) -> str:
    cleaned = _clean_action_text(action)
    if cleaned and not _is_generic_report_action(cleaned):
        return cleaned
    missing_names = _missing_evidence_names(evidence_package)
    if missing_names & {"current_msrp", "own_model_price", "competitor_price_range", "competitor_corridor", "target_model_price"}:
        return "补齐目标车型 MSRP、竞品价格走廊和月供/促销口径，再生成最终 PPT block"
    if missing_names & {"configuration_delta", "feature_diff", "key_features", "trim"}:
        return "补齐目标车型与竞品配置差异矩阵，再生成可复制的一页对标报告"
    if missing_names & {"market_kpis", "market_snapshot_data_unavailable", "trend_or_mix"}:
        return "补齐市场规模、份额、趋势和车型级机会证据，再生成市场机会页"
    return "把已查证据做成 evidence table / report block，并补齐缺口后定稿"


def _compose_pricing_direct_answer(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    executive = _strip_direct_prefix(str(plan.get("executiveConclusion") or "").strip())
    if not executive:
        return ""
    text = _normalize_question_text(" ".join([executive, *plan.get("reportReadyBullets", [])]))
    action = ""
    if plan.get("recommendedActions"):
        action = _clean_action_text(str(plan["recommendedActions"][0].get("action") or ""))
    evidence_note = _evidence_count_note(evidence_ref_count(evidence_package))
    confidence_note = _confidence_label(str(evidence_package.get("confidence") or "low"))
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])
    country_label = _country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场"))
    display_note = _pricing_visual_backbone_note(evidence_package)
    pending_msrp_note = _pending_msrp_review_summary_text(evidence_package)
    method = plan.get("methodDistillation") if isinstance(plan.get("methodDistillation"), dict) else None

    if "phev" in text and _contains_any(text, ("leasing", "lease", "大客户", "fleet", "公司车")):
        market_brief = _phev_leasing_market_context_brief(evidence_package)
        parts = [
            f"fleet leasing 判断：{country_label} 大客户 leasing 场景下 PHEV 仍可以保留理由，但不是因为“可油可电”本身，而是因为 TCO、月供、残值、公司车税、长途里程、充电条件和冬季风险能否共同成立。",
            market_brief or "市场证据：当前缺少可引用 PHEV 渠道/销量证据，因此只能先输出 TCO 验证框架，不能写成确定主推结论。",
            "业务含义：如果 PHEV 在 Business 盘、长途/无稳定充电、4WD/冬季使用或残值风险上能解释总成本，它可以作为公司车验证线；如果月供/RV/税费算不出优势，就不应主推。",
            "展示骨架：生成 PHEV fleet leasing TCO 表，把月供、残值、税费、燃油/用电、充电条件、长途里程和冬季风险放在同一张决策表。",
            f"下一步执行：{action or '建立 PHEV fleet leasing TCO 表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1080))

    target = _pricing_target_range(evidence_package)
    if target:
        target_text = _format_price_range(target)
        stats = _pricing_price_stats(evidence_package)
        position = _target_range_position_statement(target, stats) if stats else "需要补齐竞品价格统计后判断区间位置"
        subject = _pricing_target_model_label(evidence_package)
        subject_prefix = f"{subject} 在" if subject != "目标车型" else ""
        target_action = _pricing_target_range_action(action, subject)
        official_gap = _has_missing_evidence(evidence_package, "current_msrp")
        gap_note = "它仍是用户给定场景价，不是官方 MSRP，必须做官方价、月供和促销交叉验证。" if official_gap else "仍需和官方 MSRP、月供/RV、促销支持交叉验证。"
        parts = [
            f"目标价判断：{subject_prefix}{country_label} {target_text} 可以继续验证，但不能直接定案；当前参考价格样本显示：目标价{position}。",
            display_note,
            pending_msrp_note,
            f"业务含义：{_target_range_business_implication(target, stats)}",
            f"证据边界：{gap_note}",
            f"下一步执行：{target_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=980))

    relative_pricing_direct = _generic_relative_pricing_direct_answer(
        country_label=country_label,
        evidence_package=evidence_package,
        context_text=text,
        method=method,
        action=action,
        display_note=display_note,
        pending_msrp_note=pending_msrp_note,
        alignment_note=alignment_note,
        evidence_note=evidence_note,
        confidence_note=confidence_note,
    )
    if relative_pricing_direct:
        return relative_pricing_direct

    if method and _pricing_method_applies_to_current_scope(method, evidence_package, text) and plan["evidenceAlignment"]["status"] != "conflicting":
        method_model = str(method.get("model") or _pricing_subject_label(evidence_package) or "目标车型").strip()
        verified_lines = _pricing_verified_evidence_lines(evidence_package, limit=4)
        verified_text = "；".join(verified_lines) if verified_lines else f"当前没有拿到 {method_model} 或核心竞品的官方当前 MSRP / 月供 / RV 记录。"
        missing_note = _missing_evidence_note(evidence_package)
        source_repair_text = _source_repair_action_text(
            _source_repair_candidates_from_evidence_package(evidence_package),
            question=text,
            method=method,
        )
        positioning = (
            str((method.get("priceCorridor") or {}).get("positioning") or "").strip()
            if method
            else ""
        ) or "核心竞争带中段 + 高配主推"
        main_trim = (
            str((method.get("priceCorridor") or {}).get("mainTrimPrice") or "").strip()
            if method
            else ""
        )
        core_corridor = (
            str((method.get("priceCorridor") or {}).get("coreCorridor") or "").strip()
            if method
            else ""
        )
        main_trim_note = (
            f"；用户材料里的 {main_trim} 只能作为定位假设锚点，不能把 {main_trim} 写成最终定价"
            if main_trim
            else ""
        )
        next_action = action or f"生成 {method_model} 竞品价格/配置/月供矩阵并刷新 Pricing corridor chart"
        if source_repair_text and _looks_like_source_repair_action(next_action):
            next_action = f"完成官方 MSRP 物化后，重算 {method_model} 竞品价格走廊、高配价值覆盖和主销版本建议。"
        market_data_note = _pricing_live_market_evidence_note(evidence_package)
        if market_data_note and market_data_note.rstrip("。") in verified_text:
            market_data_note = ""
        if missing_note == "可引用证据" and evidence_ref_count(evidence_package) > 0:
            data_boundary = "数据边界：当前已有用户材料/工具证据，但官方 MSRP、核心竞品当前价格、月供/RV 和配置差异还没有全部交叉验证，所以不能把材料价格走廊当最终价。"
        else:
            data_boundary = f"数据缺口：缺少{missing_note}；官方 MSRP、核心竞品价格、月供/RV 和配置差异还没有全部变成可引用证据，所以不能把材料价格走廊当最终价。"
        validation_stance = _pricing_method_validation_stance(
            country_label=country_label,
            model=method_model,
            positioning=positioning,
            main_trim=main_trim,
            core_corridor=core_corridor,
            competitor_pool=[
                str(item or "").strip()
                for item in method.get("competitorPool", [])
                if str(item or "").strip()
            ] if method else [],
            evidence_package=evidence_package,
        )
        feature_value_note = _pricing_method_visible_feature_note(method_model, method)
        source_status_note = _pricing_source_repair_status_note(
            _source_repair_candidates_from_evidence_package(evidence_package),
            question=text,
            method=method,
        )
        parts = [
            validation_stance,
            f"关键证据：{verified_text}。",
            f"配置价值假设：{feature_value_note}" if feature_value_note else "",
            (
                "用户材料假设边界：上述价格、价差、PVA 或竞品池来自材料/方法样例，"
                f"不是当前官方 MSRP 或已验证竞品价，也不能写死具体差额{main_trim_note}。"
            ),
            f"JATO 工具数据补充：{market_data_note}" if market_data_note else "",
            data_boundary,
            display_note,
            source_status_note,
            f"补数动作：{_public_direct_action_summary(source_repair_text) or _pricing_method_repair_action(method_model, method)}",
            f"下一步执行：{_public_direct_action_summary(next_action)}。当前证据：{evidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=2400))

    if pending_msrp_note:
        parts = [
            f"价格证据判断：{country_label}市场现在不是完全无价格线索，{pending_msrp_note}",
            display_note,
            "业务含义：这些待审核价格可以先支撑版本价格阶梯、竞品走廊草案和 review 表，但不能直接写成正式成交价、确定价差或最终定价结论。",
            f"下一步执行：{_public_direct_action_summary(action) or '审核待确认 MSRP 观察，生成当前价格记录后再输出确定价格走廊和主销版本建议'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=980))

    generic_evidence_direct = _generic_pricing_evidence_direct_answer(
        country_label=country_label,
        evidence_package=evidence_package,
        action=action,
        display_note=display_note,
        alignment_note=alignment_note,
        evidence_note=evidence_note,
        confidence_note=confidence_note,
    )
    if generic_evidence_direct:
        return generic_evidence_direct

    generic_gap_direct = _generic_pricing_gap_direct_answer(
        country_label=country_label,
        evidence_package=evidence_package,
        action=action,
        display_note=display_note,
        alignment_note=alignment_note,
        evidence_note=evidence_note,
        confidence_note=confidence_note,
    )
    if generic_gap_direct:
        return generic_gap_direct

    return ""


def _pricing_method_applies_to_current_scope(
    method: BusinessMethodDistillation,
    evidence_package: dict[str, Any],
    context_text: str,
) -> bool:
    model = str(method.get("model") or "").strip()
    if not model:
        return False
    entities = _requested_entity_names_from_package(evidence_package)
    model_key = _model_key(model)
    if entities and any(_model_keys_match(model_key, _model_key(entity)) for entity in entities):
        return True
    context_key = _model_key(context_text)
    return bool(model_key and model_key in context_key)


def _question_mentions_method_model(method: BusinessMethodDistillation, question_text: str) -> bool:
    model_key = _model_key(str(method.get("model") or ""))
    question_key = _model_key(question_text)
    return bool(model_key and question_key and (model_key in question_key or _model_keys_match(model_key, question_key)))


def _pricing_method_validation_stance(
    *,
    country_label: str,
    model: str,
    positioning: str,
    main_trim: str,
    core_corridor: str,
    competitor_pool: list[str],
    evidence_package: dict[str, Any],
) -> str:
    stance = positioning or "核心竞争带中段 + 高配主推"
    main_trim_value = _pricing_material_value_by_tokens(
        evidence_package,
        ("main trim", "msrp", "price"),
        fallback=main_trim,
    )
    corridor_value = _pricing_material_value_by_tokens(
        evidence_package,
        ("competitor corridor", "price corridor"),
        fallback=core_corridor,
    )
    price_gap = _pricing_material_value_by_tokens(
        evidence_package,
        ("price gap", "high-low trim price gap"),
    )
    pva_coverage = _pricing_material_value_by_tokens(
        evidence_package,
        ("pva coverage",),
    )
    pool = _pricing_material_raw_value_by_tokens(
        evidence_package,
        ("competitor pool",),
    )
    if not pool and competitor_pool:
        pool = ", ".join(_dedupe(competitor_pool)[:5])
    evidence_bits: list[str] = []
    if main_trim_value and corridor_value:
        evidence_bits.append(f"主销高配假设 {main_trim_value} 落在 {corridor_value} 价格带内")
    elif main_trim_value:
        evidence_bits.append(f"主销高配假设 {main_trim_value} 可以作为待验证价格锚点")
    elif corridor_value:
        evidence_bits.append(f"竞品价格带假设为 {corridor_value}")
    if price_gap and pva_coverage:
        evidence_bits.append(f"高低配价差 {price_gap}、PVA 覆盖 {pva_coverage} 支撑高配价值假设")
    elif price_gap:
        evidence_bits.append(f"高低配价差 {price_gap} 需要用配置价值覆盖验证")
    elif pva_coverage:
        evidence_bits.append(f"PVA 覆盖 {pva_coverage} 可作为高配价值验证点")
    if pool:
        evidence_bits.append(f"竞品池先按 {pool} 交叉验证")
    evidence_text = f"；{'；'.join(evidence_bits)}" if evidence_bits else ""
    model_label = model or "目标车型"
    return _clean_business_text(
        f"验证版定价立场：{country_label} {model_label} 可以先按“{stance}”组织定价页，"
        f"低配做价格锚点，高配做主推版本{evidence_text}。"
        "这是一版可推进的定价假设，不是最终官方 MSRP。"
    )


def _pricing_method_visible_feature_note(model: str, method: BusinessMethodDistillation) -> str:
    features = [
        str(item.get("featureName") or "").strip()
        for item in method.get("featureValueClaims", [])
        if isinstance(item, dict) and str(item.get("featureName") or "").strip()
    ]
    if not features:
        return ""
    feature_text = "、".join(_dedupe(features)[:6])
    model_label = model or str(method.get("model") or "目标车型").strip() or "目标车型"
    return (
        f"{model_label} 的优势不应只讲单项油耗或空间，而应把 {feature_text} "
        "转成用户能感知的高配价值，用来解释高配主推和价差覆盖。"
    )


def _pricing_method_repair_action(model: str, method: BusinessMethodDistillation) -> str:
    competitor_pool = [
        str(item or "").strip()
        for item in method.get("competitorPool", [])
        if str(item or "").strip()
    ]
    pool_text = "、".join(competitor_pool[:5]) if competitor_pool else "核心竞品"
    model_label = model or "目标车型"
    return f"优先补齐 {model_label}、{pool_text} 的官方价格记录，再重算价格走廊和高配价值覆盖。"


def _pricing_material_value_by_tokens(
    evidence_package: dict[str, Any],
    tokens: tuple[str, ...],
    *,
    fallback: str = "",
) -> str:
    raw = _pricing_material_raw_value_by_tokens(evidence_package, tokens)
    if raw:
        return raw
    return _normalize_space(str(fallback or ""))


def _pricing_material_raw_value_by_tokens(evidence_package: dict[str, Any], tokens: tuple[str, ...]) -> str:
    for ref in _all_evidence_refs(evidence_package):
        if not _is_pricing_user_material_ref(ref):
            continue
        if not _pricing_user_material_ref_matches_scope(ref, evidence_package):
            continue
        label = str(ref.get("label") or "").strip().casefold()
        if not any(token in label for token in tokens):
            continue
        value = _format_evidence_ref_value(ref)
        if value:
            return value
    return ""


def _generic_relative_pricing_direct_answer(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    context_text: str,
    method: BusinessMethodDistillation | None,
    action: str,
    display_note: str,
    pending_msrp_note: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    pair = _relative_pricing_pair(evidence_package, context_text)
    if not pair:
        return ""
    target = pair["target"]
    competitor = pair["competitor"]
    direction = pair["direction"]
    scenario_delta = _pricing_delta_text(_pricing_user_relative_delta(evidence_package))
    if _relative_pricing_should_defer_to_method_stance(
        method=method,
        target=target,
        competitor=competitor,
        direction=direction,
        evidence_package=evidence_package,
        context_text=context_text,
    ):
        return ""
    price_evidence = _relative_pricing_price_evidence(evidence_package, target=target, competitor=competitor)
    has_price_refs = bool(price_evidence["official"] or price_evidence["userMaterial"])
    has_review_pending = _source_repair_has_review_pending(_source_repair_candidates_from_evidence_package(evidence_package))
    if direction == "compare" and (pending_msrp_note or has_review_pending) and not has_price_refs:
        return ""
    pair_tokens = _relative_pricing_action_tokens(target, competitor)
    topic_action = (
        action
        if _action_matches_topic(action, pair_tokens)
        else f"补齐 {target} 与 {competitor} 的 MSRP / TP / 月供 / RV / 配置差异矩阵"
    )
    parts = [
        _relative_pricing_stance(
            country_label=country_label,
            target=target,
            competitor=competitor,
            direction=direction,
            scenario_delta=scenario_delta,
        ),
        _relative_pricing_market_context_note(evidence_package, target=target, competitor=competitor, context_text=context_text),
        _relative_pricing_price_evidence_note(
            evidence_package,
            target=target,
            competitor=competitor,
            price_evidence=price_evidence,
        ),
        _relative_pricing_value_support_note(method, target=target, competitor=competitor),
        _relative_pricing_scenario_decision_note(
            target=target,
            competitor=competitor,
            direction=direction,
            scenario_delta=scenario_delta,
        ),
        display_note,
        pending_msrp_note,
        _relative_pricing_boundary_note(
            target=target,
            competitor=competitor,
            direction=direction,
            scenario_delta=scenario_delta,
        ),
        f"下一步执行：{topic_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=1180))


def _relative_pricing_should_defer_to_method_stance(
    *,
    method: BusinessMethodDistillation | None,
    target: str,
    competitor: str,
    direction: str,
    evidence_package: dict[str, Any],
    context_text: str,
) -> bool:
    if direction != "compare" or not method:
        return False
    if not _pricing_method_applies_to_current_scope(method, evidence_package, context_text):
        return False
    method_model_key = _model_key(str(method.get("model") or ""))
    if not method_model_key or not _model_keys_match(method_model_key, _model_key(target)):
        return False
    competitor_key = _model_key(competitor)
    method_pool_keys = {
        _model_key(str(item or ""))
        for item in method.get("competitorPool", [])
        if str(item or "").strip()
    }
    if competitor_key and competitor_key in method_pool_keys:
        return True
    corridor = method.get("priceCorridor") if isinstance(method.get("priceCorridor"), dict) else {}
    return bool(corridor.get("positioning") or corridor.get("coreCorridor"))


def _relative_pricing_pair(evidence_package: dict[str, Any], context_text: str) -> dict[str, str]:
    text = str(context_text or "").casefold()
    explicit_comparison = _contains_any(
        text,
        (
            "是否应该比",
            "应该比",
            "比 ",
            "比",
            "cheaper than",
            "lower than",
            "higher than",
            "more expensive than",
            "above",
            "below",
            " vs ",
            " versus ",
            "相比",
            "相对定价",
            "相对价格",
        ),
    )
    if not explicit_comparison:
        return {}
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    targets = [
        str(item or "").strip()
        for item in (entities.get("models") if isinstance(entities.get("models"), list) else [])
        if str(item or "").strip()
    ]
    competitors = [
        str(item or "").strip()
        for item in (entities.get("competitors") if isinstance(entities.get("competitors"), list) else [])
        if str(item or "").strip()
    ]
    if not competitors and len(targets) >= 2:
        competitors = targets[1:]
        targets = targets[:1]
    if not targets or not competitors:
        return {}
    target = targets[0]
    competitor = next((item for item in competitors if not _model_keys_match(_model_key(item), _model_key(target))), "")
    if not competitor:
        return {}
    direction = "compare"
    if _contains_any(text, ("便宜", "cheaper", "lower than", "below", "低于", "更低", "低价")):
        direction = "cheaper"
    elif _contains_any(text, ("更贵", "贵", "higher than", "above", "more expensive", "高于", "更高")):
        direction = "higher"
    structured_direction = str(_pricing_user_relative_delta(evidence_package).get("direction") or "").strip().casefold()
    if structured_direction in {"cheaper", "lower", "below"}:
        direction = "cheaper"
    elif structured_direction in {"more_expensive", "higher", "above"}:
        direction = "higher"
    return {"target": target, "competitor": competitor, "direction": direction}


def _relative_pricing_action_tokens(target: str, competitor: str) -> tuple[str, ...]:
    tokens = []
    for value in (target, competitor):
        parts = re.split(r"[^A-Za-z0-9]+", value)
        tokens.extend(part for part in parts if len(part) >= 2)
    return tuple(_dedupe([*tokens, target, competitor]))


def _relative_pricing_stance(
    *,
    country_label: str,
    target: str,
    competitor: str,
    direction: str,
    scenario_delta: str = "",
) -> str:
    if direction == "cheaper":
        if scenario_delta:
            return (
                f"相对定价判断：{target} 在{country_label}比 {competitor} 低 {scenario_delta} 可以作为待验证价格场景，"
                "但不是最终价差结论；该价差必须由双方官方 MSRP、月供/RV、配置价值和渠道场景共同证明。"
            )
        return (
            f"相对定价判断：{target} 在{country_label}应比 {competitor} 保持更强价格吸引力，"
            "但不能先写死具体价差；价差必须由市场结构、官方 MSRP、月供/RV 和配置价值共同证明。"
        )
    if direction == "higher":
        if scenario_delta:
            return (
                f"相对定价判断：{target} 在{country_label}比 {competitor} 高 {scenario_delta} 只有在配置、TCO、残值或渠道证据"
                "能够覆盖溢价时才成立；否则应回到同价带或更强价格吸引力。"
            )
        return (
            f"相对定价判断：{target} 在{country_label}只有在配置、TCO、残值或渠道证据明显强于 {competitor} 时才适合更高定价；"
            "否则应回到同价带或更强价格吸引力。"
        )
    return (
        f"相对定价判断：{target} 与 {competitor} 在{country_label}不能只按品牌或级别定价，"
        "应先用市场结构、官方 MSRP、月供/RV、配置差异和渠道场景建立证据矩阵。"
    )


def _relative_pricing_market_context_note(
    evidence_package: dict[str, Any],
    *,
    target: str,
    competitor: str,
    context_text: str,
) -> str:
    fuel = _relative_pricing_fuel(target, competitor, context_text, evidence_package)
    if not fuel:
        return ""
    fuel_sales = _market_cross_tab_ref_value(evidence_package, table="driveByFuel", row=fuel, metric="sales")
    fuel_2wd = _market_cross_tab_ref_value(evidence_package, table="driveByFuel", row=fuel, metric="2WD_pct")
    fuel_4wd = _market_cross_tab_ref_value(evidence_package, table="driveByFuel", row=fuel, metric="4WD_pct") or _market_cross_tab_ref_value(evidence_package, table="driveByFuel", row=fuel, metric="AWD_pct")
    fuel_business = _market_cross_tab_ref_value(evidence_package, table="registrationByFuel", row=fuel, metric="Business_pct")
    fuel_private = _market_cross_tab_ref_value(evidence_package, table="registrationByFuel", row=fuel, metric="Private_pct")
    parts: list[str] = []
    if fuel_sales:
        parts.append(f"{fuel} {fuel_sales}")
    drive_parts = []
    if fuel_2wd:
        drive_parts.append(f"2WD {fuel_2wd}")
    if fuel_4wd:
        drive_parts.append(f"4WD/AWD {fuel_4wd}")
    if drive_parts:
        parts.append(" / ".join(drive_parts))
    channel_parts = []
    if fuel_business:
        channel_parts.append(f"Business {fuel_business}")
    if fuel_private:
        channel_parts.append(f"Private {fuel_private}")
    if channel_parts:
        parts.append(" / ".join(channel_parts))
    segment_parts: list[str] = []
    for segment in ("SUV A0", "SUV A", "SUV B"):
        sales = _market_cross_tab_ref_value(evidence_package, table="driveBySegment", row=segment, metric="sales")
        mix = _market_cross_tab_ref_value(evidence_package, table="segmentByFuel", row=segment, metric=f"{fuel}_pct")
        if sales and mix:
            segment_parts.append(f"{segment} {sales} / {fuel} {mix}")
        elif sales:
            segment_parts.append(f"{segment} {sales}")
        elif mix:
            segment_parts.append(f"{segment} {fuel} {mix}")
    if segment_parts:
        parts.append("；".join(segment_parts[:3]))
    if not parts:
        return ""
    return (
        f"市场结构证据：已查 {'；'.join(parts)}。"
        f"这能支撑 {target} vs {competitor} 的相对定价方向判断，但不能替代 {competitor} 当前 MSRP、月供/RV 和配置差异验证。"
    )


def _relative_pricing_fuel(
    target: str,
    competitor: str,
    context_text: str,
    evidence_package: dict[str, Any],
) -> str:
    combined = " ".join([target, competitor, context_text]).casefold()
    for fuel in ("PHEV", "BEV", "HEV", "MHEV", "ICE"):
        if fuel.casefold() in combined:
            return fuel
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    powertrains = entities.get("powertrains") if isinstance(entities.get("powertrains"), list) else []
    for item in powertrains:
        value = str(item or "").strip().upper()
        if value in {"PHEV", "BEV", "HEV", "MHEV", "ICE"}:
            return value
    labels = " ".join(str(ref.get("label") or "") for ref in _all_evidence_refs(evidence_package)).casefold()
    for fuel in ("PHEV", "BEV", "HEV", "MHEV", "ICE"):
        if f".{fuel.casefold()}." in labels or f".{fuel.casefold()}_" in labels:
            return fuel
    return ""


def _relative_pricing_price_evidence_note(
    evidence_package: dict[str, Any],
    *,
    target: str,
    competitor: str,
    price_evidence: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    evidence = price_evidence or _relative_pricing_price_evidence(evidence_package, target=target, competitor=competitor)
    parts: list[str] = []
    if evidence["official"]:
        official_by_model = _relative_pricing_refs_by_model(evidence["official"], target=target, competitor=competitor)
        for model in (target, competitor):
            refs = official_by_model.get(model, [])
            if refs:
                parts.append(f"{model} {_relative_pricing_price_ref_summary(refs)}")
    if evidence["userMaterial"]:
        material_by_model = _relative_pricing_refs_by_model(evidence["userMaterial"], target=target, competitor=competitor)
        material_parts: list[str] = []
        for model in (target, competitor):
            refs = material_by_model.get(model, [])
            if refs:
                material_parts.append(f"{model} {_relative_pricing_price_ref_summary(refs)}")
        if material_parts:
            parts.append(f"用户材料价格假设 {'；'.join(material_parts[:3])}，不是当前官方 MSRP")
    source_status = _relative_pricing_source_status_note(evidence_package, target=target, competitor=competitor)
    if parts and source_status:
        return f"价格证据：已查 {'；'.join(parts[:4])}。{source_status}"
    if parts:
        return f"价格证据：已查 {'；'.join(parts[:4])}。"
    if source_status:
        return f"价格证据：当前还没有形成 {target} 与 {competitor} 的 accepted current MSRP / 月供/RV 可引用矩阵。{source_status}"
    return f"价格证据：当前还没有形成 {target} 与 {competitor} 的官方 MSRP、月供/RV 和促销支持可引用矩阵。"


def _relative_pricing_price_evidence(
    evidence_package: dict[str, Any],
    *,
    target: str,
    competitor: str,
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {"official": [], "userMaterial": [], "sourceStatus": []}
    for ref in _all_evidence_refs(evidence_package):
        if not _relative_pricing_ref_matches_model(ref, target, competitor):
            continue
        if not _relative_pricing_ref_looks_like_price_metric(ref):
            continue
        if _relative_pricing_ref_is_source_status(ref):
            result["sourceStatus"].append(ref)
        elif _relative_pricing_ref_is_user_material(ref, evidence_package):
            result["userMaterial"].append(ref)
        elif _relative_pricing_ref_is_official_price(ref):
            result["official"].append(ref)
    corridor_ref = _relative_pricing_corridor_ref(evidence_package)
    if corridor_ref:
        if _relative_pricing_ref_is_user_material(corridor_ref, evidence_package):
            result["userMaterial"].append(corridor_ref)
        elif _relative_pricing_ref_is_official_price(corridor_ref):
            result["official"].append(corridor_ref)
    return {
        key: _dedupe_ref_list(values)[:8]
        for key, values in result.items()
    }


def _relative_pricing_price_refs_for_model(evidence_package: dict[str, Any], model: str) -> list[dict[str, Any]]:
    model_key = _model_key(model)
    if not model_key:
        return []
    refs: list[dict[str, Any]] = []
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "")
        label_key = _model_key(label)
        if model_key not in label_key:
            continue
        lower = label.casefold()
        if not any(token in lower for token in ("msrp", "price", "minprice", "maxprice", "avgprice", "monthly", "residual", "rv", "pva")):
            continue
        if _format_evidence_ref_value(ref):
            refs.append(ref)
    return refs[:4]


def _relative_pricing_refs_by_model(
    refs: list[dict[str, Any]],
    *,
    target: str,
    competitor: str,
) -> dict[str, list[dict[str, Any]]]:
    result = {target: [], competitor: []}
    for ref in refs:
        if _relative_pricing_ref_matches_single_model(ref, target):
            result[target].append(ref)
        elif _relative_pricing_ref_matches_single_model(ref, competitor):
            result[competitor].append(ref)
        else:
            result[target].append(ref)
    return result


def _relative_pricing_ref_matches_model(ref: dict[str, Any], target: str, competitor: str) -> bool:
    return _relative_pricing_ref_matches_single_model(ref, target) or _relative_pricing_ref_matches_single_model(ref, competitor)


def _relative_pricing_ref_matches_single_model(ref: dict[str, Any], model: str) -> bool:
    model_key = _model_key(model)
    if not model_key:
        return False
    text = " ".join(
        str(ref.get(key) or "")
        for key in ("label", "source", "table")
    )
    return model_key in _model_key(text)


def _relative_pricing_ref_looks_like_price_metric(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    if any(token in label for token in ("sourcedraft", "candidatesource", "materializationstatus", "reviewpending", "currentpricerows")):
        return True
    return any(token in label for token in ("msrp", "price", "minprice", "maxprice", "avgprice", "monthly", "residual", "rv", "pva", "corridor"))


def _relative_pricing_ref_is_source_status(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    value = str(ref.get("value") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    return any(
        token in " ".join([label, value, source])
        for token in (
            "sourcedraft",
            "source_draft",
            "candidatesourcetype",
            "candidate_search_query",
            "materializationstatus",
            "ready_for_extraction",
            "reviewpending",
            "review_pending",
            "not_current_price",
        )
    )


def _relative_pricing_ref_is_user_material(ref: dict[str, Any], evidence_package: dict[str, Any]) -> bool:
    return _evidence_ref_is_user_material(ref) and _pricing_user_material_ref_matches_scope(ref, evidence_package)


def _relative_pricing_ref_is_official_price(ref: dict[str, Any]) -> bool:
    if _relative_pricing_ref_is_source_status(ref):
        return False
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    if _numeric_ref_value(ref) is None and not _numeric_ref_values(ref):
        return False
    if any(token in label for token in ("pva", "price gap", "high-low")):
        return False
    return any(token in source for token in ("current_price", "jato_msrp_postgres", "pricing", "postgres")) or label.startswith("pricing.records.")


def _relative_pricing_source_status_note(
    evidence_package: dict[str, Any],
    *,
    target: str,
    competitor: str,
) -> str:
    candidates = _source_repair_candidates_from_evidence_package(evidence_package)
    if _source_repair_has_review_pending(candidates):
        pending_note = _pending_msrp_review_summary_text(evidence_package)
        if pending_note:
            return f"待审核价格状态：{pending_note}不能直接写成正式成交价、确定价差或最终定价结论。"
        return "待审核价格状态：MSRP review queue 里有待审核观察，不能直接写成正式成交价、确定价差或最终定价结论。"
    labels = _relative_pricing_source_candidate_labels_for_pair(candidates, target=target, competitor=competitor)
    if labels:
        return (
            f"补源状态：{', '.join(labels[:4])} 只有搜索候选/来源草稿，"
            "需要审核来源、版本、币种、发布日期并 ingest 成 current price 后才能作为官方价格证据。"
        )
    return ""


def _relative_pricing_source_candidate_labels_for_pair(
    candidates: dict[str, Any],
    *,
    target: str,
    competitor: str,
) -> list[str]:
    result: list[str] = []
    target_token = _normalize_source_match_token(target)
    competitor_token = _normalize_source_match_token(competitor)
    for key in ("ownModel", "competitorCorridor"):
        rows = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in rows:
            if not isinstance(entry, dict):
                continue
            label = _source_candidate_labels([entry])
            if not label:
                continue
            candidate_text = _source_candidate_match_text(
                brand=str(entry.get("brand") or ""),
                model=str(entry.get("model") or ""),
            )
            if _source_candidate_token_matches(target_token, candidate_text) or _source_candidate_token_matches(competitor_token, candidate_text):
                result.extend(label)
    return _dedupe(result)


def _relative_pricing_corridor_ref(evidence_package: dict[str, Any]) -> dict[str, Any] | None:
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").casefold()
        if "competitor corridor" in label or "price corridor" in label:
            if _format_evidence_ref_value(ref):
                return ref
    return None


def _relative_pricing_price_ref_summary(refs: list[dict[str, Any]]) -> str:
    values: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "").casefold()
        value = _format_evidence_ref_value(ref)
        if not value:
            continue
        if "minprice" in label:
            values.append(f"min {value}")
        elif "maxprice" in label:
            values.append(f"max {value}")
        elif "monthly" in label:
            values.append(f"月供 {value}")
        elif "residual" in label or "rv" in label:
            values.append(f"RV {value}")
        elif "pva" in label:
            values.append(f"PVA {value}")
        else:
            values.append(value)
    return " / ".join(_dedupe(values)[:4])


def _relative_pricing_value_support_note(
    method: BusinessMethodDistillation | None,
    *,
    target: str,
    competitor: str,
) -> str:
    if not isinstance(method, dict):
        return (
            f"价值解释：{target} 与 {competitor} 的价差不能只靠价格本身，必须补齐配置差异、质保、ADAS/冬季包、空间/动力和品牌/RV 风险。"
        )
    method_model = str(method.get("model") or "")
    if method_model and not _model_keys_match(_model_key(method_model), _model_key(target)):
        return (
            f"价值解释：需要补齐 {target} 与 {competitor} 的配置差异、质保、ADAS/冬季包、空间/动力和品牌/RV 风险。"
        )
    features = method.get("featureValueClaims") if isinstance(method.get("featureValueClaims"), list) else []
    feature_names = [
        str(item.get("featureName") or "").strip()
        for item in features
        if isinstance(item, dict) and str(item.get("featureName") or "").strip()
    ]
    if feature_names:
        return (
            f"价值解释：{target} 需要用 { '、'.join(feature_names[:5]) } 等可见配置解释价差，而不是只靠低价。"
        )
    return (
        f"价值解释：{target} 的价格位置必须由可见配置、版本策略、PVA/配置价值和 {competitor} 的配置差异共同支撑。"
    )


def _relative_pricing_scenario_decision_note(
    *,
    target: str,
    competitor: str,
    direction: str,
    scenario_delta: str,
) -> str:
    if not scenario_delta:
        return ""
    if direction == "higher":
        return (
            f"场景验证：用户提出的高 {scenario_delta} 是决策输入，不是已查事实。"
            f"只有当 {target} 相对 {competitor} 的配置、月供/RV、TCO 或渠道价值至少覆盖该溢价时，场景才可进入定价页。"
        )
    return (
        f"场景验证：用户提出的低 {scenario_delta} 是决策输入，不是已查事实。"
        f"如果该差额能覆盖 {competitor} 的配置/续航/动力优势与 {target} 的品牌、残值或渠道风险，就可作为价格锚点；"
        "否则需要调整价差、版本或促销支持。"
    )


def _relative_pricing_boundary_note(
    *,
    target: str,
    competitor: str,
    direction: str,
    scenario_delta: str = "",
) -> str:
    scenario = f"用户场景价差 {scenario_delta}" if scenario_delta else "具体价差"
    if direction == "higher":
        return (
            f"价差边界：在没有 {target}/{competitor} 当前 MSRP、月供/RV、配置差异和渠道/TCO 证据前，"
            f"不能把{scenario}写成确定结论。"
        )
    return (
        f"价差边界：在没有 {target}/{competitor} 当前 MSRP、月供/RV、配置差异和渠道/TCO 证据前，不能写死{scenario}；"
        "合理做法是先判断价格吸引力方向，再用表格验证价差能否覆盖价值差异。"
    )


def _phev_leasing_market_context_brief(evidence_package: dict[str, Any]) -> str:
    stats = _policy_powertrain_stats(evidence_package)
    phev = stats.get("PHEV", {})
    if not phev:
        return ""
    phev_line = _policy_powertrain_line("PHEV", phev, include_channel=True)
    drive_line = _policy_drive_line(phev)
    details = [item for item in (phev_line, drive_line) if item]
    if not details:
        return ""
    return (
        f"市场证据：{'，'.join(details)}。"
        "这说明 PHEV 的 fleet 价值应优先看公司车渠道依赖、长途/冬季使用和 TCO，而不是把 PHEV 泛化成所有大客户答案。"
    )


def _compose_configuration_direct_answer(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    executive = _strip_direct_prefix(str(plan.get("executiveConclusion") or "").strip())
    if not executive:
        return ""
    question_text = _normalize_question_text(" ".join([executive, *plan.get("reportReadyBullets", [])]))
    action = ""
    if plan.get("recommendedActions"):
        action = _clean_action_text(str(plan["recommendedActions"][0].get("action") or ""))
    evidence_note = _evidence_count_note(evidence_ref_count(evidence_package))
    confidence_note = _confidence_label(str(evidence_package.get("confidence") or "low"))
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])
    has_80kwh = "80kwh" in question_text or "80 kwh" in question_text
    has_winter_package = "冬季包" in question_text or "winter package" in question_text
    has_high_spec_architecture = (
        "95kwh" in question_text
        or "95 kwh" in question_text
        or "800v" in question_text
        or "双电机" in question_text
    )
    display_note = _artifact_visual_backbone_note("configuration_analysis", evidence_package)
    market_context_note = _configuration_market_context_brief(evidence_package)

    generic_configuration_direct = _generic_configuration_evidence_direct_answer(
        country_label=_country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场")),
        evidence_package=evidence_package,
        action=action,
        display_note=display_note,
        market_context_note=market_context_note,
        alignment_note=alignment_note,
        evidence_note=evidence_note,
        confidence_note=confidence_note,
    )
    if generic_configuration_direct:
        return generic_configuration_direct
    if not (has_winter_package or has_80kwh or has_high_spec_architecture):
        context_gap_direct = _configuration_context_gap_direct_answer(
            country_label=_country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场")),
            evidence_package=evidence_package,
            action=action,
            display_note=display_note,
            market_context_note=market_context_note,
            alignment_note=alignment_note,
            evidence_note=evidence_note,
            confidence_note=confidence_note,
        )
        if context_gap_direct:
            return context_gap_direct

    if has_winter_package and has_80kwh:
        parts = [
            "配置结论：当前还没有拿到可引用的冬季包、80kWh、续航、价格和竞品配置矩阵；不能把冬季包 + 80kWh 写成确定版本策略，只能先作为待验证配置假设。",
            market_context_note,
            display_note,
            "可推进判断：市场结构只能说明需要验证冬季可用性、真实续航、补能效率和高配价值，不能替代逐项配置/价格证据。",
            f"下一步执行：生成北欧冬季包 + A0 SUV BEV 80kWh 版本策略验证表。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=980))

    if has_high_spec_architecture:
        raw_action = _topic_action(
            plan.get("recommendedActions", []),
            ("95", "800v", "双电机", "电池", "充电", "冬季", "配置"),
            "生成 95kWh / 双电机 / 800V 配置价值-成本验证表",
        )
        action = (
            raw_action
            if _action_matches_topic(raw_action, ("95", "800v", "双电机", "价值-成本", "验证表"))
            else "生成 95kWh / 双电机 / 800V 配置价值-成本验证表"
        )
        parts = [
            "配置结论：当前还没有拿到可引用的 95kWh、双电机、800V、价格和竞品配置矩阵；不能直接写成高价值架构成立，只能先作为高配架构假设验证。",
            market_context_note,
            display_note,
            "可推进判断：市场结构只能说明 BEV 高价值场景值得验证，不能替代续航、补能、重量、成本和竞品版本证据。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=950))

    if has_80kwh:
        raw_action = _topic_action(
            plan.get("recommendedActions", []),
            ("80", "电池", "续航", "冬季", "价格", "重量", "配置"),
            "生成 A0 SUV BEV 80kWh 续航-价格-重量验证表",
        )
        action = (
            raw_action
            if _action_matches_topic(raw_action, ("a0", "80", "续航", "重量", "验证表"))
            else "生成 A0 SUV BEV 80kWh 续航-价格-重量验证表"
        )
        parts = [
            "配置结论：当前还没有拿到可引用的电池、续航、充电、价格或竞品配置矩阵；不能把 80kWh 写成确定必需配置，只能把它作为长续航/高配假设进入验证。",
            market_context_note,
            display_note,
            "可推进判断：市场结构只能说明需要验证冬季续航、补能效率、价格压力和高配价值，不能替代 80kWh 与竞品长续航版本的逐项对比。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=950))

    if has_winter_package:
        raw_action = _topic_action(
            plan.get("recommendedActions", []),
            ("冬季包", "winter", "热泵", "座椅", "轮胎", "拖车", "roof"),
            "生成北欧冬季包 must-have / value / optional 配置清单",
        )
        action = (
            raw_action
            if _action_matches_topic(raw_action, ("冬季包", "winter", "热泵", "轮胎", "清单"))
            else "生成北欧冬季包 must-have / value / optional 配置清单"
        )
        parts = [
            "配置结论：当前还没有拿到可引用的冬季包逐项配置、价格和竞品标配矩阵；不能直接下 must-have 清单，只能先列验证路径。",
            market_context_note,
            display_note,
            "可推进判断：下一步需要把热管理、加热舒适、低温充电、轮胎/牵引和户外实用配置逐项映射到用户场景、成本和竞品标配状态。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=980))

    if "80kwh" in question_text or "80 kwh" in question_text or "冬季" in question_text or "winter" in question_text:
        action = "生成配置差异矩阵和主销配置建议"
        parts = [
            "配置结论：当前配置证据不足，不能只靠通用经验下结论；需要先补齐配置差异、价格和用户场景证据。",
            market_context_note,
            display_note,
            "可推进判断：先把电池、热泵/冬季包、座椅/方向盘加热、拖车/roof load、ADAS 和充电速度放进验证矩阵，再决定 must-have、visible value、nice-to-have 分层。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=900))

    action = _topic_action(
        plan.get("recommendedActions", []),
        ("配置", "冬季", "电池", "热泵", "座椅", "充电", "adas", "configuration", "feature"),
        "生成配置差异矩阵和主销配置建议",
    )
    parts = [
        f"配置判断：{executive}",
        market_context_note,
        "用户场景：先把配置拆成 must-have、visible value、nice-to-have 和 cost/risk，再映射到私人零售、公司车、冬季和家庭使用。",
        display_note,
        "产品动作：配置差异必须转成版本策略、主销配置和销售话术，不能停留在“配置更高”。",
        f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=900))


def _generic_configuration_evidence_direct_answer(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    action: str,
    display_note: str,
    market_context_note: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    refs = _configuration_takeaway_refs(_all_evidence_refs(evidence_package))
    if not refs:
        return ""
    subject = _configuration_subject_label(evidence_package)
    evidence_lines = _configuration_evidence_lines(refs, limit=5)
    if not evidence_lines:
        return ""
    next_action = _configuration_next_action(action, country_label, evidence_package)
    stance = _configuration_metric_business_stance(evidence_package, refs)
    gap_line = _configuration_gap_boundary(evidence_package)
    parts = [
        f"配置判断：{country_label} {subject} {stance}",
        f"已查配置证据：{'；'.join(evidence_lines)}。",
        market_context_note,
        display_note,
        f"证据边界：{gap_line}",
        f"下一步执行：{next_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=1150))


def _configuration_context_gap_direct_answer(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    action: str,
    display_note: str,
    market_context_note: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    context_lines = _configuration_context_gap_evidence_lines(evidence_package, limit=5)
    if not context_lines:
        return ""
    subject = _configuration_subject_label(evidence_package)
    gap_note = _configuration_tool_gap_note(evidence_package) or _configuration_gap_boundary(evidence_package)
    next_action = _configuration_next_action(action, country_label, evidence_package)
    parts = [
        f"配置判断：{country_label} {subject} 现在不能写成已验证配置胜负；当前已拿到竞品/价格/市场锚点，但配置矩阵还没查到。",
        f"已查锚点：{'；'.join(context_lines)}。",
        market_context_note,
        display_note,
        f"证据边界：{gap_note}",
        f"下一步执行：{next_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=1200))


def _configuration_context_gap_evidence_lines(evidence_package: dict[str, Any], *, limit: int) -> list[str]:
    refs = _all_evidence_refs(evidence_package)
    prioritized = sorted(
        [ref for ref in refs if _is_configuration_context_gap_ref(ref)],
        key=_configuration_context_gap_ref_priority,
        reverse=True,
    )
    lines: list[str] = []
    for ref in prioritized:
        line = _configuration_context_gap_ref_line(ref) or _evidence_ref_digest_line(ref)
        if line:
            lines.append(line)
        if len(_dedupe(lines)) >= limit:
            break
    return _dedupe(lines)[:limit]


def _is_configuration_context_gap_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    haystack = f"{label} {source}"
    if label.endswith(".model"):
        return False
    if any(token in label for token in ("row_count", "totalrows", "countrycount", "brandcount", "modelcount", "versioncount")):
        return False
    return any(
        token in haystack
        for token in (
            "competitor.",
            ".sales",
            "priceevidencestatus",
            "currentprice",
            "sourcedraft",
            "candidatesource",
            "materialization",
            "pricestats.",
            "avgmsrp",
            "cumulativesales",
            "segmentbyfuel",
            "drivebysegment",
        )
    )


def _configuration_context_gap_ref_line(ref: dict[str, Any]) -> str:
    label = str(ref.get("label") or "").strip()
    normalized = re.sub(r"[^a-z0-9.]+", "", label.casefold())
    model = _normalize_space(label.split(".", 1)[0].replace("_", " ")) if "." in label else ""
    value = _format_evidence_ref_value(ref)
    if not model or not value:
        return ""
    if normalized.endswith(".priceevidencestatus"):
        return f"{model} 价格来源状态 = {_public_price_source_status(value)}"
    if normalized.endswith(".sourcedraftpath"):
        return f"{model} 价格来源草稿 = {value}"
    if normalized.endswith(".currentpricerows"):
        return f"{model} 当前价格记录 = {value}"
    return ""


def _configuration_context_gap_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = str(ref.get("label") or "").casefold()
    source = str(ref.get("source") or ref.get("table") or "").casefold()
    haystack = f"{label} {source}"
    order = [
        ("competitor.", 120),
        (".sales", 115),
        ("priceevidencestatus", 105),
        ("currentpricerows", 100),
        ("sourcedraftpath", 95),
        ("candidatedomain", 90),
        ("pricestats.min", 80),
        ("pricestats.max", 78),
        ("pricestats.avg", 76),
        ("segmentbyfuel", 70),
        ("drivebysegment", 68),
        ("avgmsrp", 60),
        ("cumulativesales", 58),
    ]
    for token, score in order:
        if token in haystack:
            return score, label
    return 1, label


def _configuration_tool_gap_note(evidence_package: dict[str, Any]) -> str:
    missing_names = _missing_evidence_names(evidence_package)
    gap_parts: list[str] = []
    if any("no_config_projects" in name or "configuration" in name or "feature_diff" in name for name in missing_names):
        gap_parts.append("配置矩阵 = 当前市场未返回工程/官网配置项目，不能判断电池、续航、充电或 ADAS 差异")
    if any("no_current_prices" in name or "current_msrp" in name or "own_model_price" in name for name in missing_names):
        gap_parts.append("当前价 = 请求车型/竞品缺当前 MSRP 行，不能把价格样本写成官方价")
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        if str(tool.get("toolName") or "") != "compare_vehicle_variants":
            continue
        summary = str(tool.get("summary") or "")
        if "no variant/configuration matrix" in summary or "no subjects" in summary:
            gap_parts.append("compare_vehicle_variants = 未返回 subjects / feature deltas / common features")
    return "；".join(_dedupe(gap_parts))


def _configuration_safe_action(action: str, evidence_package: dict[str, Any]) -> str:
    text = _clean_action_text(action)
    if not text:
        return ""
    if _action_mentions_unrequested_vehicle(text, evidence_package):
        return ""
    return text


def _configuration_next_action(
    action: str,
    country_label: str,
    evidence_package: dict[str, Any],
) -> str:
    cleaned = _configuration_safe_action(action, evidence_package)
    if _configuration_action_is_strong(cleaned):
        return cleaned
    base = _configuration_matrix_action(country_label, evidence_package)
    if cleaned and _configuration_action_is_price_source_repair(cleaned):
        return f"{base}，并同步审核当前 MSRP / 官方价格来源"
    return base


def _configuration_action_is_strong(action: str) -> bool:
    text = _normalize_question_text(action)
    if not text:
        return False
    if _configuration_action_is_price_source_repair(action):
        return False
    matrix_action = (
        _contains_any(text, ("配置矩阵", "配置对比", "配置差异", "configuration validation", "feature delta", "版本配置"))
        and _contains_any(text, ("电池", "续航", "充电", "adas", "冬季", "质保", "msrp", "价格", "来源日期"))
    )
    topic_action = (
        _contains_any(text, ("验证表", "配置清单", "must-have", "value / optional"))
        and _contains_any(text, ("80kwh", "95kwh", "双电机", "800v", "冬季包", "热泵", "电池", "续航"))
    )
    return matrix_action or topic_action


def _configuration_action_is_price_source_repair(action: str) -> bool:
    text = _normalize_question_text(action)
    return _contains_any(text, ("msrp 来源", "官方价格", "来源草稿", "价格来源", "current price", "source draft"))


def _configuration_matrix_action(country_label: str, evidence_package: dict[str, Any]) -> str:
    subject = _configuration_subject_label(evidence_package)
    market = _market_label(country_label)
    return (
        f"把{market} {subject} 做成配置对比矩阵，字段包括电池/续航、充电、ADAS、冬季配置、质保、空间/拖车、MSRP 和来源日期"
    )


def _action_mentions_unrequested_vehicle(text: str, evidence_package: dict[str, Any]) -> bool:
    return _text_mentions_unrequested_vehicle(text, evidence_package)


def _configuration_subject_label(evidence_package: dict[str, Any]) -> str:
    entities = _requested_entity_names_from_package(evidence_package)
    if entities:
        return " / ".join(entities[:4])
    return "目标车型"


def _configuration_evidence_lines(refs: list[dict[str, Any]], *, limit: int) -> list[str]:
    lines: list[str] = []
    for ref in refs:
        line = _evidence_ref_digest_line(ref)
        if line:
            lines.append(line)
        if len(_dedupe(lines)) >= limit:
            break
    return _dedupe(lines)[:limit]


def _configuration_metric_business_stance(evidence_package: dict[str, Any], refs: list[dict[str, Any]]) -> str:
    comparison = _configuration_metric_comparison(evidence_package, refs)
    if comparison:
        return comparison
    topics = _configuration_topic_tags(refs)
    if "battery_range" in topics:
        return "核心差异先看电池/续航和补能效率，再判断价格、配置和品牌风险能否覆盖参数差距。"
    if "winter" in topics:
        return "核心差异先看低温可用性、热管理和冬季舒适配置，不能只把冬季配置写成装饰包。"
    if "visible_value" in topics:
        return "已查到可感知配置锚点，应把它们转成展厅话术和版本价值，而不是只列装备名称。"
    if "utility" in topics:
        return "核心差异应落到拖车、载重、roof load 和家庭/户外场景，判断这些配置能否成为北欧实用卖点。"
    if "safety_adas" in topics:
        return "配置差异应先验证 ADAS/安全配置的可用性和标配边界，再转成主销版本建议。"
    return "已有配置证据，可以先进入配置价值判断；但结论必须落到用户场景、版本策略和价格价值边界。"


def _configuration_metric_comparison(evidence_package: dict[str, Any], refs: list[dict[str, Any]]) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    targets = [str(item or "").strip() for item in (entities.get("models") if isinstance(entities.get("models"), list) else []) if str(item or "").strip()]
    competitors = [
        str(item or "").strip()
        for item in (entities.get("competitors") if isinstance(entities.get("competitors"), list) else [])
        if str(item or "").strip()
    ]
    if not targets or not competitors:
        return ""
    rows = _configuration_numeric_rows(refs)
    target_rows = [row for row in rows if _model_name_in_list(str(row["model"]), targets)]
    competitor_rows = [row for row in rows if _model_name_in_list(str(row["model"]), competitors)]
    if not target_rows or not competitor_rows:
        return ""
    priority = ("battery", "range", "charging")
    for metric in priority:
        target_metric_rows = [row for row in target_rows if row["metric"] == metric]
        competitor_metric_rows = [row for row in competitor_rows if row["metric"] == metric]
        if not target_metric_rows or not competitor_metric_rows:
            continue
        target_best = max(float(row["value"]) for row in target_metric_rows)
        competitor_best = max(float(row["value"]) for row in competitor_metric_rows)
        metric_label = {"battery": "电池容量", "range": "续航", "charging": "补能/充电性能"}[metric]
        if target_best < competitor_best:
            return (
                f"{metric_label}不是目标车型的参数领先项，目标车型需要用更低价格、可见配置、质保/售后或版本策略补偿竞品优势，"
                "不能直接写成配置胜出。"
            )
        if target_best > competitor_best:
            return (
                f"{metric_label}已形成目标车型的参数锚点，可以作为高配价值或主销版本理由；"
                "仍需用价格、月供/RV 和用户场景验证该优势是否能被用户感知。"
            )
        return f"{metric_label}与竞品接近，差异不应只看参数，应转向可见配置、价格和使用场景验证。"
    return ""


def _configuration_numeric_rows(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        metric = _configuration_metric_from_label(label)
        if not metric:
            continue
        value = _numeric_ref_value(ref)
        if value is None:
            continue
        model = _configuration_model_from_label(label)
        if not model:
            continue
        rows.append({"model": model, "metric": metric, "value": value})
    return rows


def _configuration_metric_from_label(label: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", str(label or "").casefold())
    if any(token in normalized for token in ("batterykwh", "batterysize", "batterycapacity", "usablebattery")):
        return "battery"
    if any(token in normalized for token in ("wltprange", "range", "rangekm")):
        return "range"
    if any(token in normalized for token in ("chargingkw", "dccharging", "fastcharging", "chargepower")):
        return "charging"
    return ""


def _configuration_model_from_label(label: str) -> str:
    text = str(label or "").strip()
    parts = [part.strip() for part in text.split(".") if part.strip()]
    if len(parts) >= 2:
        return ".".join(parts[:-1]).strip()
    return ""


def _configuration_topic_tags(refs: list[dict[str, Any]]) -> set[str]:
    haystack = " ".join(str(ref.get("label") or "") for ref in refs).casefold()
    tags: set[str] = set()
    if any(token in haystack for token in ("battery", "range", "charging", "charge", "续航", "电池", "充电")):
        tags.add("battery_range")
    if any(token in haystack for token in ("winter", "heat", "heat pump", "preheat", "冬季", "热泵", "加热")):
        tags.add("winter")
    if any(token in haystack for token in ("hud", "camera", "seat", "ventilat", "memory", "sunroof", "影像", "座椅", "天窗")):
        tags.add("visible_value")
    if any(token in haystack for token in ("tow", "roof", "load", "trailer", "拖车", "载重")):
        tags.add("utility")
    if any(token in haystack for token in ("adas", "safety", "assist", "安全", "辅助")):
        tags.add("safety_adas")
    return tags


def _configuration_gap_boundary(evidence_package: dict[str, Any]) -> str:
    missing_names = _missing_evidence_names(evidence_package)
    lines: list[str] = []
    if missing_names & {"current_msrp", "own_model_price", "competitor_price_range", "monthly_payment", "rv", "residual_value"}:
        lines.append("价格/月供/RV = 待补同口径价格和使用成本")
    if missing_names & {"configuration_delta", "feature_diff", "key_features", "trim", "competitive_or_configuration_data_unavailable"}:
        lines.append("配置差异 = 待补目标车型与核心竞品逐项配置/版本表")
    if missing_names & {"market_kpis", "segment_context", "trend_or_mix"}:
        lines.append("市场场景 = 待补级别/动力/渠道结构证据")
    if not lines:
        lines.append("仍需把配置证据与价格、版本策略、用户场景和销售话术交叉验证")
    return "；".join(_dedupe(lines))


def _configuration_market_context_brief(evidence_package: dict[str, Any]) -> str:
    refs = _configuration_market_context_refs(evidence_package)
    if not refs:
        return ""
    lines = [_evidence_ref_digest_line(ref) for ref in refs[:4]]
    evidence = "，".join(line for line in lines if line)
    if not evidence:
        return ""
    return (
        f"市场上下文：{evidence}。"
        "这些数据提供市场背景，但不能单独证明配置取舍；仍需要和电池、续航、充电、价格、竞品配置矩阵交叉验证。"
    )


def _compose_voc_direct_answer(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    executive = _strip_direct_prefix(str(plan.get("executiveConclusion") or "").strip())
    if not executive:
        return ""
    question_text = _normalize_question_text(" ".join([executive, *plan.get("reportReadyBullets", [])]))
    country_label = _country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场"))
    action = _topic_action(
        plan.get("recommendedActions", []),
        ("v2h", "voc", "用户", "原声", "媒体", "测评", "话术"),
        f"抓取{_market_regional_scope(country_label)} V2H 用户原声和媒体测评证据",
    )
    evidence_note = _evidence_count_note(evidence_ref_count(evidence_package))
    confidence_note = _confidence_label(str(evidence_package.get("confidence") or "low"))
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])

    if "v2h" in question_text:
        parts = [
            f"VOC 判断：{country_label} V2H 暂时不能定位为真实高频购买卖点，应定位为“高感知但待验证”的技术型加分项。",
            "用户价值：它更可能服务家庭能源、安全备份、冬季用车和科技形象叙事；如果没有用户原声，只能作为代理判断，不能当作消费者调研结论。",
            "产品动作：先把 V2H 测成家庭能源、冬季备份和科技形象三套话术，再用媒体测评、论坛评论和经销端反馈验证是否能转化购买。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=950))

    candidate_themes = _voc_candidate_theme_text(question_text)
    if candidate_themes:
        source_brief = _voc_source_evidence_brief(evidence_package)
        action = _topic_action(
            plan.get("recommendedActions", []),
            ("voc", "用户", "吐槽", "痛点", "配置", "冬季", "拖车", "roof", "售后", "话术"),
            "生成 VOC 主题表：来源、用户/媒体信号、产品含义、验证状态和销售动作",
        )
        if not _action_matches_topic(action, ("主题表", "产品动作", "吐槽", "配置", "售后", "交付", "销售动作")):
            action = "生成 VOC 主题表：来源、用户/媒体信号、产品含义、验证状态和销售动作"
        parts = [
            f"VOC 判断：{country_label} 当前不能把这些主题写成已验证“高频吐槽”，但可以先作为候选痛点池推进验证：{candidate_themes}。",
            source_brief or "可引用证据：当前缺少可追溯用户原声/媒体测评，不能把候选主题写成高频投诉结论。",
            "用户价值：先把外部来源、媒体观点、论坛噪音和真实用户原声分层；有来源但缺频次时，只能写“候选主题”，不能写“高频结论”。",
            "产品动作：把候选主题直接映射到配置包、交付检查、售后承诺和销售话术，例如冬季/拖车/roof load 走北欧实用场景，车机/ADAS 走试驾和交付说明，售后/品牌信任走质保和经销服务覆盖。",
            f"下一步执行：{action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1150))

    parts = [
        f"VOC 判断：{executive}",
        "用户价值：先区分真实用户痛点、媒体观点、论坛噪音和可转化卖点，再映射到配置、价格、售后和销售话术。",
        f"下一步执行：{action or '补 Tavily/web/VOC 可引用来源'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=900))


def _compose_inventory_direct_answer(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    executive = _strip_direct_prefix(str(plan.get("executiveConclusion") or "").strip())
    question_text = _normalize_question_text(" ".join([executive, *plan.get("reportReadyBullets", [])]))
    country_label = _country_label(str(plan.get("country") or evidence_package.get("country") or "当前市场"))
    action = _inventory_action(plan)
    evidence_note = _evidence_count_note(evidence_ref_count(evidence_package))
    confidence_note = _confidence_label(str(evidence_package.get("confidence") or "low"))
    alignment_note = _alignment_label(plan["evidenceAlignment"]["status"])

    if _is_pi_market_split_question(question_text):
        parts = [
            "直接结论：SE/FI 合并 PI、车辆分市场生成的逻辑原则上可以成立，但 PI 只承载共用计划/产品信息层。",
            "证据边界：正确结构应是 PI header + market overlay + materialCode / vehicle generation mapping；车辆生成、物料号、合规、价格、订单和库存生命周期必须保留 market-level overlay；不能用合并 PI 覆盖 SE/FI 市场差异。",
            f"下一步执行：{action or '定义 PI header + market overlay + vehicle/material generation mapping'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=900))

    brief = _inventory_bom_evidence_brief(evidence_package)
    missing = _inventory_missing_boundary(evidence_package)
    if brief:
        parts = [
            f"直接结论：{brief['model']} 一个版型多个物料号不能直接判错；当前证据显示 {brief['scope']}，应先按颜色/内饰、市场、PI 或生命周期拆分，再判断是否是正常拆分还是数据冲突。",
            f"已查证据：{brief['evidence']}",
            f"风险边界：{brief['risk'] or '同一业务版本多物料号必须能解释到颜色、市场、PI 或生命周期；否则不能生成客户可编辑数量。'}",
            missing,
            f"下一步执行：{action or '建立版本-颜色-物料号-生命周期映射表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1050))

    if not executive:
        return ""
    parts = [
        f"直接结论：{country_label} BOM/库存问题应先建立实体关系，再判断异常；{_strip_terminal_punctuation(executive)}。",
        "业务解释：一个版型多个物料号通常不能直接判错，必须看颜色/内饰组合、市场差异、PI、配置包、生命周期和订单状态。",
        f"下一步执行：{action or '建立版本-颜色-物料号-生命周期映射表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=900))


def _inventory_action(plan: BusinessSynthesisPlan) -> str:
    actions = plan.get("recommendedActions") if isinstance(plan.get("recommendedActions"), list) else []
    for item in actions:
        if not isinstance(item, dict):
            continue
        action = _clean_action_text(str(item.get("action") or ""))
        if _action_matches_topic(action, ("物料", "material", "bom", "生命周期", "lifecycle", "pi", "mapping", "映射", "颜色", "版本")):
            return action
    return ""


def _inventory_bom_evidence_brief(evidence_package: dict[str, Any]) -> dict[str, str]:
    fields = _inventory_bom_evidence_fields(evidence_package)
    material = fields.get("materialCode", "")
    if not material:
        return {}
    model = fields.get("model", "") or _inventory_model_label(evidence_package)
    market = fields.get("market", "")
    version = fields.get("version", "")
    scope_parts = [
        f"市场 {market}" if market else "",
        f"业务版本 {version}" if version else "",
        f"物料号 {material}",
    ]
    evidence_parts = [
        f"market={market}" if market else "",
        f"version={version}" if version else "",
        f"materialCode={material}",
    ]
    risk = fields.get("risk", "")
    if risk:
        evidence_parts.append(f"risk={_inventory_risk_public_text_for_composer(risk)}")
    return {
        "model": model,
        "scope": "、".join(item for item in scope_parts if item),
        "evidence": "；".join(item for item in evidence_parts if item),
        "risk": _inventory_risk_public_text_for_composer(risk),
    }


def _inventory_bom_evidence_fields(evidence_package: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip()
        record_match = re.match(
            r"^(?:inventory|material|bom|stock|order)\.records\.([^.]+)\.([^.]+)$",
            label,
            flags=re.IGNORECASE,
        )
        if record_match:
            fields.setdefault("model", _normalize_space(record_match.group(1).replace("_", " ")))
        metric = label.casefold().split(".")[-1].replace("_", "")
        value = _format_evidence_ref_value(ref)
        if not value:
            continue
        if metric in {"market", "country"}:
            fields.setdefault("market", value)
        elif metric == "model":
            fields.setdefault("model", value)
        elif metric in {"version", "trim", "variant"}:
            fields.setdefault("version", value)
        elif metric in {"materialcode", "sku", "partnumber"}:
            fields.setdefault("materialCode", value)
        elif metric in {"lifecycle", "status"}:
            fields.setdefault("lifecycle", value)
        elif metric == "risk" or "risk" in label.casefold() or "lifecycle risk" in label.casefold():
            fields.setdefault("risk", value)
    return fields


def _inventory_model_label(evidence_package: dict[str, Any]) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    models = entities.get("models") if isinstance(entities.get("models"), list) else []
    for item in models:
        value = _normalize_space(str(item or ""))
        if value:
            return value
    requested = _requested_entity_names_from_package(evidence_package)
    return requested[0] if requested else "目标车型"


def _inventory_missing_boundary(evidence_package: dict[str, Any]) -> str:
    missing = _missing_evidence_note(evidence_package)
    if not missing or missing == "可引用证据":
        return "仍需补齐：颜色/内饰、生命周期、可下单状态和客户可编辑数量；补齐前不能直接合并物料号或生成下单数量。"
    return f"仍需补齐：{missing}；补齐前不能直接合并物料号或生成下单数量。"


def _inventory_risk_public_text_for_composer(value: str) -> str:
    text = str(value or "").strip()
    lower = text.casefold()
    if "duplicate" in lower or "多个" in text or "重复" in text:
        return "同一业务版本存在多个物料号，需按颜色/市场/PI/生命周期拆分"
    if "lifecycle" in lower or "生命周期" in text:
        return "生命周期状态需确认，避免历史或停用物料进入新订单"
    if "missing" in lower or "缺" in text:
        return "缺少关键映射字段，不能直接生成客户可编辑数量"
    return text


def _voc_candidate_theme_text(question_text: str) -> str:
    text = _normalize_question_text(question_text)
    themes: list[str] = []
    if _contains_any(text, ("omoda", "jaecoo", "吐槽", "抱怨", "投诉", "差评")):
        themes.extend([
            "新品牌信任/售后服务",
            "车机/ADAS/交付体验",
            "冬季续航和充电可靠性",
        ])
    if _contains_any(text, ("拖车", "tow", "roof", "冬季胎", "winter tyre", "roof load")):
        themes.extend([
            "拖车钩/roof load/户外装载",
            "冬季胎和低温可用性",
        ])
    if _contains_any(text, ("配置", "使用场景", "高频需求", "真实需求", "卖点")):
        themes.extend([
            "可见配置价值",
            "销售话术可转化性",
        ])
    return "、".join(_dedupe(themes)[:5])


def _voc_source_evidence_brief(evidence_package: dict[str, Any]) -> str:
    refs = _raw_evidence_refs(evidence_package)
    source_titles: list[str] = []
    themes: list[str] = []
    claim_snippet = ""
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        lower_label = label.lower()
        value = _normalize_space(str(ref.get("value") or ""))
        source = _normalize_space(str(ref.get("source") or ref.get("table") or ""))
        haystack = f"{label} {value} {source}".lower()
        if not _contains_any(
            haystack,
            (
                "omoda",
                "jaecoo",
                "voc",
                "review",
                "complaint",
                "dealer",
                "service",
                "warranty",
                "winter",
                "charging",
                "range",
                "adas",
                "software",
                "delivery",
                "forum",
                "media",
                "source",
                "claim",
                "summary",
            ),
        ):
            continue
        if lower_label.endswith((".source", ".url")) or value.startswith(("http://", "https://")):
            title = _voc_source_title(label)
            if title:
                source_titles.append(title)
            continue
        if lower_label.endswith((".claim", ".summary", ".topic", ".title")) and value:
            theme = _voc_theme_label(value)
            if theme:
                themes.append(theme)
            if not claim_snippet:
                claim_snippet = _short_business_phrase(value, max_chars=72)
    if not source_titles and not themes and not claim_snippet:
        return ""
    title_text = f"（{_dedupe(source_titles)[0]}）" if source_titles else ""
    theme_text = "、".join(_dedupe(themes)[:3]) or "来源可信度/主题聚类"
    claim_text = f"，来源信号是“{claim_snippet}”" if claim_snippet else ""
    return (
        f"可引用证据：当前来源只能支持把 {theme_text} 作为候选验证方向{title_text}{claim_text}；"
        "还不能证明瑞典真实用户已经形成高频吐槽。"
    )


def _raw_evidence_refs(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        for ref in _coerce_evidence_refs(tool.get("evidenceRefs")):
            if isinstance(ref, dict):
                refs.append(ref)
    return refs


def _voc_source_title(label: str) -> str:
    text = re.sub(r"\.(source|url)$", "", str(label or "").strip(), flags=re.IGNORECASE)
    text = _normalize_space(text)
    if len(text) > 84:
        text = f"{text[:81]}..."
    return text


def _voc_theme_label(text: str) -> str:
    normalized = _normalize_question_text(text)
    if _contains_any(normalized, ("dealer", "service", "warranty", "brand", "售后", "质保", "经销", "品牌", "delivery", "交付")):
        return "新品牌信任/售后与交付体验"
    if _contains_any(normalized, ("adas", "hud", "camera", "infotainment", "software", "车机", "影像", "软件")):
        return "车机/ADAS/座舱体验"
    if _contains_any(normalized, ("winter", "range", "charging", "battery", "续航", "充电", "电池", "低温")):
        return "冬季续航和充电可靠性"
    if _contains_any(normalized, ("tow", "roof", "ski", "拖车", "行李架", "冬季胎", "roof load")):
        return "拖车钩/roof load/冬季实用场景"
    return "来源可信度/主题聚类"


def _short_business_phrase(value: str, *, max_chars: int = 80) -> str:
    text = _normalize_space(str(value or "")).strip("“”\"'")
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3]}..."


def _strip_direct_prefix(value: str) -> str:
    text = str(value or "").strip()
    return re.sub(r"^直接结论[:：]\s*", "", text).strip()


def _compose_pricing_method_report_direct(
    plan: BusinessSynthesisPlan,
    method: BusinessMethodDistillation,
    evidence_package: dict[str, Any],
) -> str:
    country_label = _country_label(str(plan.get("country") or method.get("market") or "当前市场"))
    model = str(method.get("model") or "").strip() or "目标车型"
    playbook = method.get("pricingPlaybook") if isinstance(method.get("pricingPlaybook"), dict) else {}
    price_corridor = method.get("priceCorridor") if isinstance(method.get("priceCorridor"), dict) else {}
    version_strategy = method.get("versionStrategy") if isinstance(method.get("versionStrategy"), dict) else {}
    positioning = str(price_corridor.get("positioning") or "").strip()
    core_corridor = str(price_corridor.get("coreCorridor") or "").strip()
    main_trim_price = str(price_corridor.get("mainTrimPrice") or "").strip()
    price_gap = _short_price_gap(str(price_corridor.get("priceGap") or version_strategy.get("priceGap") or ""))
    pva_coverage = _short_percentage(str(version_strategy.get("pvaCoverage") or playbook.get("pva_validation") or ""))
    competitor_pool = [
        str(item or "").strip()
        for item in method.get("competitorPool", [])
        if str(item or "").strip()
    ]
    features = [
        str(item.get("featureName") or "").strip()
        for item in method.get("featureValueClaims", [])
        if isinstance(item, dict) and str(item.get("featureName") or "").strip()
    ][:7]
    sales_talk = [
        str(item or "").strip()
        for item in version_strategy.get("salesTalk", [])
        if str(item or "").strip()
    ]
    market_window = _strip_terminal_punctuation(str(playbook.get("market_window") or "")).strip()
    competitor_sentence = _strip_terminal_punctuation(str(playbook.get("competitor_corridor") or "")).strip()
    value_sentence = _strip_terminal_punctuation(str(playbook.get("product_value_delta") or "")).strip()
    first_action = ""
    if plan.get("recommendedActions"):
        first_action = _clean_action_text(str(plan["recommendedActions"][0].get("action") or ""))
    official_gap = (
        _has_missing_evidence(evidence_package, "current_msrp")
        or _has_missing_evidence(evidence_package, "own_model_price")
        or _has_missing_evidence(evidence_package, "competitor_price_range")
        or not _has_non_method_pricing_anchor(evidence_package)
    )
    method_source = str(method.get("sourceName") or method.get("deckTitle") or "用户材料").strip()
    source_boundary = (
        f"注意：以下是从 {method_source} 蒸馏出的定价假设，不是当前官方 MSRP 或已验证竞品价格。"
        if official_gap
        else f"证据边界：以下包含 {method_source} 的方法论材料，仍需持续和官方 MSRP、竞品价格、月供/RV 交叉验证。"
    )
    conclusion_verb = "可以先形成" if official_gap else "可以采用"

    price_bits = []
    if main_trim_price and main_trim_price not in competitor_sentence:
        price_bits.append(f"主销高配 {main_trim_price}")
    if core_corridor and core_corridor not in competitor_sentence:
        price_bits.append(f"落在 {core_corridor} 核心价格带")
    price_note = f"，{'，'.join(price_bits)}" if price_bits else ""
    competitor_note = "、".join(competitor_pool[:5]) if competitor_pool else "核心同级 HEV/SUV 竞品"
    feature_note = "、".join(features[:7]) if features else value_sentence or "可感知高配"
    gap_bits = []
    if price_gap:
        gap_bits.append(f"高低配价差 {price_gap}")
    if pva_coverage:
        gap_bits.append(f"PVA 覆盖约 {pva_coverage}")
    gap_note = f"，{'，'.join(gap_bits)}" if gap_bits else ""
    talk_note = "、".join(sales_talk[:4]) if sales_talk else "好看、省心、可见配置、高价值感"

    verified_lines = _pricing_verified_evidence_lines(evidence_package, limit=3)
    hypothesis_lines = _pricing_user_material_hypothesis_lines(evidence_package, limit=4)
    verified_note = "；".join(verified_lines) if verified_lines else "本轮未拿到本车型/核心竞品官方当前 MSRP。"
    hypothesis_positioning = positioning or "低配锚点 + 高配主推"
    hypothesis_note = "；".join(hypothesis_lines) if hypothesis_lines else f"{model} 用户材料定位假设：{hypothesis_positioning}"
    if competitor_sentence and core_corridor and core_corridor in hypothesis_note:
        competitor_sentence = f"竞品池先按 {competitor_note} 交叉验证"

    parts = [
        f"已验证证据：{verified_note}。",
        f"{country_label} {model} 定价页现在应先做成验证版，而不是最终定价页：已查证据只能支撑价格走廊和高配价值验证，不能把用户材料价写成官方 MSRP。",
        f"用户材料价格假设（用户材料假设）：{hypothesis_note}。{source_boundary}",
    ]
    if market_window:
        parts.append(f"材料中的市场层面假设：{market_window}。")
    if competitor_sentence:
        parts.append(f"待验证竞品层面：{competitor_sentence}{price_note}。")
    else:
        parts.append(f"待验证竞品层面：{model} 可先用 {competitor_note}{price_note} 做候选池。")
    parts.append(f"待验证配置层面：{model} 的优势假设不在单项参数极限，而在 {feature_note}等可感知高配{gap_note}。")
    parts.append(f"因此，低配可先作为价格锚点、高配作为主推版本的待验证假设，用“{talk_note}”组织销售话术，但页面必须标注材料假设和证据缺口。")
    if first_action:
        parts.append(f"下一步执行：{first_action}。")
    if official_gap:
        parts.append("结论边界：未补齐本车型官方 MSRP、核心竞品官方价格、月供/RV 和配置差异前，不能把 34,720 EUR 或材料价格走廊写成最终定价结论。")
    return _clean_business_text(_bounded_direct_text(parts, max_chars=1100))


def _has_non_method_pricing_anchor(evidence_package: dict[str, Any]) -> bool:
    requested_model_keys = _requested_pricing_anchor_model_keys(evidence_package)
    for ref in _all_evidence_refs(evidence_package):
        source = str(ref.get("source") or ref.get("table") or "").casefold()
        if "business_method_material" in source or "user_question" in source:
            continue
        label = str(ref.get("label") or "").casefold()
        if not label or "pricestats" in label:
            continue
        if "current msrp" in label or "own-model msrp" in label:
            return True
        if any(token in label for token in ("pricing.records", ".msrp", ".minprice", ".maxprice")):
            model_key = _pricing_anchor_model_key_from_label(label)
            if not requested_model_keys or any(_model_keys_match(model_key, requested_key) for requested_key in requested_model_keys):
                return True
    return False


def _requested_pricing_anchor_model_keys(evidence_package: dict[str, Any]) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values: list[str] = []
    for key in ("models", "competitors"):
        raw_values = entities.get(key)
        if isinstance(raw_values, list):
            values.extend(str(item or "").strip() for item in raw_values)
    return [_model_key(value) for value in values if _model_key(value)]


def _pricing_ref_matches_requested_entity_scope(ref: dict[str, Any], evidence_package: dict[str, Any]) -> bool:
    requested_markers = _pricing_entity_markers_from_keys(_requested_pricing_anchor_model_keys(evidence_package))
    if not requested_markers:
        return True
    ref_markers = _pricing_ref_model_markers(ref)
    if not ref_markers:
        return True
    return bool(requested_markers & ref_markers)


def _pricing_entity_markers_from_keys(keys: list[str]) -> set[str]:
    markers: set[str] = set()
    for key in keys:
        markers.update(_pricing_model_markers_from_text(key))
    return markers


def _pricing_ref_model_markers(ref: dict[str, Any]) -> set[str]:
    return _pricing_model_markers_from_text(
        " ".join(
            str(ref.get(key) or "")
            for key in ("label", "source", "table")
            if str(ref.get(key) or "").strip()
        )
    )


def _pricing_model_markers_from_text(value: str) -> set[str]:
    text = _model_key(value)
    if not text:
        return set()
    aliases = {
        "j7": ("j7", "jaecooj7"),
        "j8": ("j8", "jaecooj8"),
        "o5": ("o5", "omoda5", "omoda05"),
        "o9": ("o9", "omoda9", "omoda09"),
        "ev3": ("ev3", "kiaev3"),
        "sportage": ("sportage", "kiasportage"),
        "rav4": ("rav4", "toyotarav4"),
        "corollacross": ("corollacross", "toyotacorollacross"),
        "chr": ("chr", "toyotachr"),
        "qashqai": ("qashqai", "nissanqashqai"),
        "sorento": ("sorento", "kiasorento"),
        "ex30": ("ex30", "volvoex30"),
        "xc60": ("xc60", "volvoxc60"),
        "ex60": ("ex60", "volvoex60"),
    }
    markers: set[str] = set()
    for marker, tokens in aliases.items():
        if any(token in text for token in tokens):
            markers.add(marker)
    return markers


def _pricing_anchor_model_key_from_label(label: str) -> str:
    parts = [part.strip() for part in str(label or "").split(".") if part.strip()]
    if len(parts) >= 4 and parts[0] == "pricing" and parts[1] == "records":
        return _model_key(parts[2])
    if len(parts) >= 2 and parts[-1] in {"msrp", "minprice", "maxprice"}:
        return _model_key(".".join(parts[:-1]))
    return ""


def _short_price_gap(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(\d[\d,.\s]*)\s?(EUR|€|SEK|kr|USD)", text, flags=re.IGNORECASE)
    if match:
        amount = _normalize_space(match.group(1)).replace(" ", "")
        unit = match.group(2)
        separator = "" if unit == "€" else " "
        return f"{amount}{separator}{unit}"
    return _strip_terminal_punctuation(text)


def _short_percentage(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"\d+(?:[.,]\d+)?\s?%", text)
    if match:
        return match.group(0).replace(" ", "")
    return _strip_terminal_punctuation(text)


def _same_business_sentence(first: str, second: str) -> bool:
    first_norm = re.sub(r"[\s，。；：:、/+\\-]+", "", first.lower())
    second_norm = re.sub(r"[\s，。；：:、/+\\-]+", "", second.lower())
    if not first_norm or not second_norm:
        return False
    return first_norm in second_norm or second_norm in first_norm


def _is_redundant_direct_line(executive: str, line: str) -> bool:
    if _same_business_sentence(executive, line):
        return True
    executive_text = _normalize_question_text(executive)
    line_text = _normalize_question_text(line)
    if _is_hev_phev_route_question(executive_text) and _is_hev_phev_route_question(line_text):
        return True
    if (
        "bev" in executive_text
        and "hev" in executive_text
        and _contains_any(executive_text, ("压缩", "挤压", "空间"))
        and (
            "展示骨架" in line_text
            or "市场判断应落到" in line_text
            or "当前结论需要同时保留" in line_text
            or "market decision table" in line_text
        )
    ):
        return True
    if _is_pi_market_split_question(executive_text) and _is_pi_market_split_question(line_text):
        return True
    action_match = re.search(r"下一步执行[:：]\s*([^。；;]+)", executive)
    if action_match and "建议动作" in line:
        action = _normalize_question_text(action_match.group(1))
        return bool(action and action in line_text)
    return False


def _bounded_direct_text(parts: list[str], *, max_chars: int = 1000) -> str:
    accepted: list[str] = []
    current_len = 0
    for part in parts:
        clean = str(part or "").strip()
        if not clean:
            continue
        next_len = current_len + len(clean) + (2 if accepted else 0)
        if accepted and next_len > max_chars:
            break
        accepted.append(clean)
        current_len = next_len
    return "\n\n".join(accepted)


def _structured_summary(plan: BusinessSynthesisPlan, evidence_package: dict[str, Any]) -> str:
    intent = plan["intent"]
    country = _country_label(plan["country"] or "当前市场")
    ref_count = evidence_ref_count(evidence_package)
    if intent == "market_overview":
        focus = "key metrics、动力/级别结构和可进入 segment"
        if _has_market_top_model_refs(_all_evidence_refs(evidence_package)):
            focus = "key metrics、动力结构、top models 和可进入 segment"
        return f"{_market_label(country)}总览应先读 {focus}，再转成产品动作；{_evidence_availability_note(ref_count)}。"
    if intent == "pricing_analysis":
        pending = _pending_msrp_review_summary_text(evidence_package)
        pending_suffix = f"；{pending.rstrip('。')}" if pending else ""
        return f"{_market_possessive(country)}定价分析应同时验证 MSRP、竞品价格走廊、leasing/RV/company car 与配置价值，再输出主销版本和价格姿态；{_evidence_availability_note(ref_count)}{pending_suffix}。"
    if intent == "competitor_compare":
        return f"{_market_possessive(country)}竞品对比应输出 competitor table、feature delta 和 positioning statement，用来判断正面对抗、错位竞争或价格锚点；{_evidence_availability_note(ref_count)}。"
    if intent == "configuration_analysis":
        profile = _configuration_topic_profile(plan, evidence_package)
        if profile and str(profile.get("summary") or "").strip():
            return str(profile["summary"])
        return f"{_market_possessive(country)}配置分析应形成 trim/config table、must-have features、配置 gap 和主销配置建议；{_evidence_availability_note(ref_count)}。"
    if intent == "inventory_analysis":
        return f"{_market_possessive(country)}库存/BOM 分析应先理清 stock、material、version、market、颜色和生命周期关系，再给出风险与下一步动作；{_evidence_availability_note(ref_count)}。"
    if intent == "report_generation":
        return f"{_market_possessive(country)}汇报生成应先给可拍板结论，再压成证据、产品含义和下一步动作；{_evidence_availability_note(ref_count)}。"
    return plan["executiveConclusion"]


def _public_recommended_actions(plan: BusinessSynthesisPlan) -> list[RecommendedAction]:
    country_label = _country_label(plan.get("country") or "当前市场")
    actions: list[RecommendedAction] = []
    for item in plan.get("recommendedActions", []):
        if not isinstance(item, dict):
            continue
        public_item = dict(item)
        action = str(public_item.get("action") or "")
        rationale = str(public_item.get("rationale") or "")
        public_item["action"] = _localize_public_market_text(action, country_label)
        public_item["rationale"] = _localize_public_market_text(rationale, country_label)
        actions.append(public_item)  # type: ignore[arg-type]
    return actions


def _localize_public_market_text(value: str, country_label: str) -> str:
    text = str(value or "")
    target_market = str(country_label or "").strip() or "当前市场"
    return text.replace("目标市场", target_market)


def _structured_key_takeaways(plan: BusinessSynthesisPlan, evidence_package: dict[str, Any]) -> list[str]:
    intent = plan["intent"]
    refs = _all_evidence_refs(evidence_package)
    first_action = plan["recommendedActions"][0]["action"] if plan["recommendedActions"] else "补齐核心证据后再收敛结论"
    implication = _structured_pm_insight(plan)
    evidence_note = _evidence_label_note(refs)
    if intent == "market_overview":
        return [
            f"Key metrics：{_market_takeaway_note(refs, ('sales', 'volume', 'share', 'market'))}",
            f"Powertrain mix：{_market_takeaway_note(refs, ('bev', 'phev', 'hev', 'ice', 'powertrain', 'fuel'))}",
            f"Top models：{_market_top_models_takeaway_note(plan, evidence_package, refs)}",
            f"Product implication：{implication}",
        ]
    if intent == "pricing_analysis":
        pending_takeaway = _pending_msrp_review_takeaway(evidence_package)
        target_takeaways = _pricing_target_range_key_takeaways(evidence_package, first_action=first_action)
        if target_takeaways:
            return _dedupe([pending_takeaway, *target_takeaways] if pending_takeaway else target_takeaways)[:4]
        report_lines = _report_slide_lines_from_plan(plan)
        if report_lines:
            takeaways = [
                f"MSRP / pricing stance：{report_lines[0]}",
                f"Competitor corridor：{_best_report_line(report_lines, ('竞品', 'corridor', '价格带', '价格判断'))}",
                f"Leasing/RV/company car：{_evidence_label_note(refs, ('leasing', 'monthly', 'rv', 'residual', 'company car'))}",
                f"Recommendation：{_best_report_line(report_lines, ('下一步', '动作', '风险')) or first_action}",
            ]
            return _dedupe([pending_takeaway, *takeaways] if pending_takeaway else takeaways)[:4]
        takeaways = [
            f"MSRP：{_evidence_label_note(refs, ('msrp', 'price', 'pricing'))}",
            f"Competitor corridor：{_evidence_label_note(refs, ('corridor', 'competitor', 'range'))}",
            f"Leasing/RV/company car：{_evidence_label_note(refs, ('leasing', 'monthly', 'rv', 'residual', 'company car'))}",
            f"Recommendation：{first_action}",
        ]
        return _dedupe([pending_takeaway, *takeaways] if pending_takeaway else takeaways)[:4]
    if intent == "competitor_compare":
        competitor_refs = _competitor_evidence_digest_refs(evidence_package, refs)
        return [
            f"Competitor table：{_evidence_label_note(competitor_refs, limit=5)}",
            f"Feature delta：{_evidence_label_note(competitor_refs, ('feature', 'config', 'battery', 'range', 'trim', 'equipment'))}",
            f"Positioning statement：{implication}",
        ]
    if intent == "configuration_analysis":
        config_refs = _configuration_takeaway_refs(refs)
        return [
            "Validation matrix：Configuration validation matrix",
            f"Must-have features：{_configuration_topic_note(plan, config_refs)}",
            f"Evidence status：{_configuration_evidence_status_note(evidence_package, config_refs)}",
            f"Recommendation：{first_action}",
        ]
    if intent == "inventory_analysis":
        return [
            f"Stock/material/BOM logic：{_evidence_label_note(refs, ('stock', 'inventory', 'bom', 'material', 'variant', 'version'))}",
            f"Risk：{_evidence_label_note(refs, ('risk', 'lifecycle', 'missing', 'duplicate', 'conflict'))}",
            f"Next action：{first_action}",
        ]
    if intent == "report_generation":
        slide_lines = _report_slide_lines_from_plan(plan)
        if slide_lines:
            return slide_lines
        return [
            "Title：用业务问题或车型/市场作为标题。",
            f"Key message：{plan['executiveConclusion']}",
            f"Evidence：{evidence_note}",
            f"Product implication：{implication}",
            f"Next action：{first_action}",
        ]
    return _dedupe([plan["executiveConclusion"], *plan["reportReadyBullets"]])[:5]


def _structured_pm_insight(plan: BusinessSynthesisPlan) -> str:
    for item in plan["businessImplications"]:
        text = str(item or "").strip()
        if text:
            return text
    return "把证据转成产品、价格、配置、渠道或汇报动作，而不是停留在事实复述。"


def _configuration_takeaway_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ref in refs:
        label = str(ref.get("label") or "").lower()
        source = str(ref.get("source") or ref.get("table") or "").lower()
        haystack = f"{label} {source}"
        normalized = re.sub(r"[^a-z0-9]+", "", label)
        if normalized in {"versioncount", "modelcount", "brandcount", "countrycount", "rowcount", "totalrows"}:
            continue
        if any(token in haystack for token in ("cumulativesales", "avgmsrp", "yearseries", "country_snapshot")):
            continue
        if any(
            token in haystack
            for token in (
                "configuration_delta",
                "variant_compare",
                "variant_diff",
                "engineering",
                "battery",
                "range",
                "charging",
                "heat",
                "winter",
                "tow",
                "roof",
                "seat",
                "camera",
                "hud",
                "adas",
                "trim",
                "feature",
                "equipment",
            )
        ):
            result.append(ref)
    return result


def _configuration_market_context_refs(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    refs = _all_evidence_refs(evidence_package)
    focused = [
        ref
        for ref in refs
        if _configuration_market_context_ref_priority(ref)[0] > 0
    ]
    return _dedupe_ref_list(sorted(focused, key=_configuration_market_context_ref_priority, reverse=True))


def _configuration_market_context_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = str(ref.get("label") or "")
    normalized = label.casefold()
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
        ("registrationbyfuel.bev.sales", 90),
        ("registrationbyfuel.phev.sales", 88),
        ("powertrainmix.bev.sales", 86),
        ("powertrainmix.phev.sales", 84),
        ("powertrainmix.hev.sales", 82),
        ("powertrainmix.mhev.sales", 80),
        ("powertrainmix.ice.sales", 78),
        ("动力类型mix.bev.sales", 86),
        ("动力类型mix.phev.sales", 84),
        ("动力类型mix.hev.sales", 82),
        ("动力类型mix.mhev.sales", 80),
        ("动力类型mix.ice.sales", 78),
    ]
    for token, score in order:
        if token in normalized:
            return score, label
    if (
        "contextsnapshot.powertrainmix" in normalized
        or "contextsnapshot.动力类型mix" in normalized
        or "crosscountry.powertrainmix" in normalized
        or "crosscountry.动力类型mix" in normalized
    ) and normalized.endswith((".sales", ".value", ".share")):
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


def _configuration_topic_note(plan: BusinessSynthesisPlan, refs: list[dict[str, Any]]) -> str:
    evidence_note = _evidence_label_note(refs, ("winter", "tow", "heat", "seat", "camera", "hud", "adas", "battery", "range", "charging"))
    if evidence_note != "待补可引用证据":
        return evidence_note
    text = _normalize_question_text(" ".join([plan.get("executiveConclusion", ""), *plan.get("reportReadyBullets", [])]))
    if "80kwh" in text or "80 kwh" in text:
        return "80kWh 长续航/高配安全边界、热泵、电池预热、快充和冬季舒适配置"
    if "冬季包" in text or "winter package" in text:
        return "热泵、电池预热、座椅/方向盘加热、冬季胎/TPMS 和真实冬季续航"
    if "95kwh" in text or "95 kwh" in text or "800v" in text or "双电机" in text:
        return "95kWh、双电机、800V、牵引/补能效率和价格带"
    return "must-have / visible value / optional 配置验证项"


def _configuration_evidence_status_note(evidence_package: dict[str, Any], refs: list[dict[str, Any]]) -> str:
    if refs:
        return _evidence_label_note(refs, ("configuration", "config", "feature", "battery", "range", "winter", "trim", "equipment"))
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        if name == "competitive_or_configuration_data_unavailable":
            return "待补竞品配置/价格证据"
    return "待补竞品配置/价格证据"


def _report_slide_lines_from_plan(plan: BusinessSynthesisPlan) -> list[str]:
    lines = [str(item or "").strip() for item in plan.get("reportReadyBullets", [])]
    lines = [item for item in lines if item]
    if len(lines) >= 3:
        return lines[:5]
    return []


def _best_report_line(lines: list[str], tokens: tuple[str, ...]) -> str:
    lowered_tokens = tuple(token.lower() for token in tokens)
    for line in lines:
        lower_line = line.lower()
        if any(token in lower_line for token in lowered_tokens):
            return line
    return lines[0] if lines else ""


def _report_title_from_plan(plan: BusinessSynthesisPlan) -> str:
    if plan.get("intent") != "report_generation":
        return ""
    for item in plan.get("reportReadyBullets", []):
        text = str(item or "").strip()
        if text.startswith("Title："):
            return text.replace("Title：", "", 1).strip()
    return ""


def _pricing_target_range_key_takeaways(
    evidence_package: dict[str, Any],
    *,
    first_action: str,
) -> list[str]:
    target = _pricing_target_range(evidence_package)
    if not target:
        return []
    stats = _pricing_price_stats(evidence_package)
    target_text = _format_price_range(target)
    takeaways = [f"Target price：{target_text}（用户输入场景）"]
    min_price = stats.get("min")
    max_price = stats.get("max")
    if min_price is not None and max_price is not None:
        sample_bits = [f"{_format_price_number(float(min_price))}-{_format_price_number(float(max_price))}"]
        median = stats.get("median")
        avg = stats.get("avg")
        if median is not None:
            sample_bits.append(f"median {_format_price_number(float(median))}")
        if avg is not None:
            sample_bits.append(f"avg {_format_price_number(float(avg))}")
        takeaways.append(f"Reference sample：{', '.join(sample_bits)}")
    if stats:
        takeaways.append(f"Position：{_target_range_position_statement(target, stats)}")
    gap_note = _pricing_target_gap_takeaway(evidence_package)
    if gap_note:
        takeaways.append(f"Gap：{gap_note}")
    elif first_action:
        takeaways.append(f"Recommendation：{first_action}")
    return _dedupe(takeaways)[:4]


def _pricing_target_gap_takeaway(evidence_package: dict[str, Any]) -> str:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    missing_names = [
        str(item.get("name") or "")
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    if missing_names:
        missing_labels = _missing_evidence_label_list(missing_names)
        if missing_labels:
            return f"待补{missing_labels}，最终价格锚点仍需官方 MSRP、月供/RV/company car 和配置价值交叉验证"
    return "最终价格锚点仍需官方 MSRP、月供/RV/company car 和配置价值交叉验证"


def _pricing_target_range_title(prefix: str, evidence_package: dict[str, Any]) -> str:
    if not _pricing_target_range(evidence_package):
        return ""
    subject = _pricing_target_model_label(evidence_package)
    if subject == "目标车型":
        return f"{prefix}目标价合理性判断"
    return f"{prefix}{subject} 目标价合理性判断"


def _pricing_target_model_label(evidence_package: dict[str, Any]) -> str:
    targets, _ = _competitor_requested_entities(evidence_package, "")
    if targets:
        return targets[0]
    return "目标车型"


def _pricing_target_range_action(action: str, subject: str) -> str:
    cleaned = _clean_action_text(action)
    if cleaned and "本车型" not in cleaned:
        return cleaned
    if subject == "目标车型":
        return "补齐目标车型官方 MSRP、核心竞品价格走廊、月供/RV 和配置价值表"
    return f"补齐 {subject} 官方 MSRP、核心竞品价格走廊、月供/RV 和配置价值表"


def _user_title_from_plan(plan: BusinessSynthesisPlan, evidence_package: dict[str, Any]) -> str:
    country = _country_label(str(plan.get("country") or evidence_package.get("country") or ""))
    intent = str(plan.get("intent") or "")
    context = _normalize_question_text(" ".join([
        str(plan.get("executiveConclusion") or ""),
        *[str(item or "") for item in plan.get("reportReadyBullets", [])],
    ]))
    prefix = f"{country} · " if country and country != "当前市场" else ""
    if intent == "pricing_analysis":
        target_title = _pricing_target_range_title(prefix, evidence_package)
        if target_title:
            return target_title
        relative_pair = _relative_pricing_pair(evidence_package, context)
        if relative_pair:
            return f"{prefix}{relative_pair['target']} vs {relative_pair['competitor']} 定价判断"
        method = plan.get("methodDistillation") if isinstance(plan.get("methodDistillation"), dict) else None
        if method:
            method_model = str(method.get("model") or "").strip()
            if method_model:
                return f"{prefix}{method_model} 验证版定价逻辑"
        return f"{prefix}定价分析"
    if intent == "competitor_compare":
        target_models, requested_competitors = _competitor_requested_entities(evidence_package, context)
        if target_models and requested_competitors:
            target_text = " / ".join(target_models[:2])
            competitor_text = " / ".join(requested_competitors[:3])
            if _contains_any(context, ("能打", "打 ", "对抗", "挑战", "正面", "compete", "against", "vs")):
                return f"{prefix}{target_text} vs {competitor_text} 场景型对标判断"
            return f"{prefix}{target_text} vs {competitor_text} 对标判断"
        return f"{prefix}竞品定位分析"
    if intent == "market_overview":
        return f"{prefix}市场机会判断"
    if intent == "configuration_analysis":
        if "冬季包" in context and ("80kwh" in context or "80 kwh" in context):
            return f"{prefix}冬季包 + 80kWh 版本策略"
        if "冬季包" in context or "winter package" in context:
            return f"{prefix}北欧冬季包配置判断"
        if "95kwh" in context or "95 kwh" in context or "800v" in context or "双电机" in context:
            return f"{prefix}95kWh / 双电机 / 800V 配置判断"
        if "80kwh" in context or "80 kwh" in context:
            return f"{prefix}A0 SUV BEV 80kWh 配置判断"
        return f"{prefix}配置价值判断"
    if intent == "news_policy_search":
        return f"{prefix}政策影响判断"
    if intent == "voc_analysis":
        if "v2h" in context:
            return f"{prefix}V2H 用户卖点验证"
        return f"{prefix}VOC 用户声音判断"
    return ""


def _should_replace_report_title(value: str) -> bool:
    text = _normalize_space(value)
    lower = text.lower()
    return (
        lower in {"", "analysis", "report", "market", "pricing", "grounded agent answer"}
        or lower.endswith("· 汇报生成")
        or lower.endswith(" · report generation")
    )


def _looks_like_mixed_locale_title(value: str) -> bool:
    text = _normalize_space(value)
    if not text:
        return True
    lower = text.lower()
    english_country_prefixes = (
        "sweden 的",
        "finland 的",
        "norway 的",
        "denmark 的",
        "hungary 的",
        "germany 的",
        "austria 的",
        "italy 的",
        "poland 的",
        "france 的",
        "netherlands 的",
    )
    english_country_tokens = tuple(item.replace(" 的", "") for item in english_country_prefixes)
    has_chinese = bool(re.search(r"[\u4e00-\u9fff]", text))
    has_english_country = any(token.strip() and token.strip() in lower for token in english_country_tokens)
    return (
        lower.startswith(english_country_prefixes)
        or " 的 " in text
        or (has_chinese and has_english_country and ("相关问题" in text or "·" in text))
    )


def _looks_like_raw_user_question_title(value: str) -> bool:
    text = _normalize_space(value)
    if not text:
        return True
    lower = text.lower()
    question_markers = (
        "？",
        "?",
        "是不是",
        "会不会",
        "为什么",
        "怎么",
        "应该",
        "有没有",
        "是否",
        "what ",
        "why ",
        "how ",
        "should ",
    )
    return any(marker in lower or marker in text for marker in question_markers)


def _should_replace_business_text(value: str) -> bool:
    text = _normalize_space(value)
    if not text:
        return True
    if len(text) < 18:
        return True
    lower = text.lower()
    generic_markers = (
        "analysis complete",
        "grounded answer",
        "based on the available evidence",
        "here is the analysis",
        "以下是分析",
        "基于证据回答",
        "综合来看",
        "需要进一步分析",
    )
    if any(marker in lower for marker in generic_markers):
        return True
    business_markers = (
        "msrp",
        "competitor",
        "corridor",
        "segment",
        "powertrain",
        "pricing",
        "bev",
        "phev",
        "hev",
        "bom",
        "库存",
        "竞品",
        "价格",
        "配置",
        "政策",
        "车型",
        "主销",
        "渠道",
        "月供",
        "证据",
    )
    return not any(marker in lower for marker in business_markers)


def _evidence_label_note(
    refs: list[dict[str, Any]],
    tokens: tuple[str, ...] | None = None,
    *,
    limit: int = 4,
) -> str:
    labels: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if not label:
            continue
        if _looks_like_technical_ref(ref) or _evidence_ref_is_zero_volume(ref):
            continue
        if tokens:
            source = str(ref.get("source") or ref.get("table") or "").lower()
            lower_label = label.lower()
            if not any(token in lower_label or token in source for token in tokens):
                continue
        public_label = _public_evidence_ref_label(ref) or _public_market_evidence_ref_label(ref)
        if not public_label:
            continue
        value = _format_evidence_ref_value(ref)
        labels.append(f"{public_label} {value}".strip())
    if labels:
        return " / ".join(_dedupe(labels)[:limit])
    return "待补可引用证据"


def _market_takeaway_note(refs: list[dict[str, Any]], tokens: tuple[str, ...] | None = None) -> str:
    labels: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        if not label:
            continue
        if tokens:
            source = str(ref.get("source") or ref.get("table") or "").lower()
            lower_label = label.lower()
            if not any(token in lower_label or token in source for token in tokens):
                continue
        public_label = _public_market_evidence_ref_label(ref)
        if not public_label:
            continue
        value = _format_evidence_ref_value(ref)
        labels.append(f"{public_label} {value}".strip())
    if labels:
        return " / ".join(_dedupe(labels)[:4])
    return "待补可引用证据"


def _market_top_models_takeaway_note(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> str:
    note = _market_takeaway_note(refs, ("model", "top", "ranking", "rank"))
    if note != "待补可引用证据":
        return note
    if not _is_model_level_market_gap(evidence_package):
        return note
    country_label = _market_label(_country_label(plan.get("country") or evidence_package.get("country") or "当前市场"))
    target_entities = _dedupe_entity_names_by_specificity(_requested_entity_names_from_package(evidence_package))
    target_text = " / ".join(target_entities[:3]) if target_entities else "目标车型"
    scope = _model_level_validation_scope(
        evidence_package,
        method=plan.get("methodDistillation") if isinstance(plan.get("methodDistillation"), dict) else None,
    )
    return (
        f"当前工具未返回{country_label} {target_text} 的车型级销量/价格记录；"
        f"下一步补齐{scope}的 MSRP、配置、月供/RV 和车型级销量矩阵。"
    )


def _has_market_top_model_refs(refs: list[dict[str, Any]]) -> bool:
    return bool(_market_top_models_evidence(refs))


def _is_model_level_market_gap(evidence_package: dict[str, Any]) -> bool:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    missing_names = {
        str(item.get("name") or "").strip()
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    }
    return bool(
        missing_names
        & {
            "model_level_market_opportunity_evidence",
            "competitive_or_configuration_data_unavailable",
            "competitor_set",
            "price_or_config_gap",
            "competitor_price_range",
            "configuration_delta",
        }
    )


def _model_level_validation_scope(
    evidence_package: dict[str, Any],
    *,
    method: BusinessMethodDistillation | None = None,
) -> str:
    competitors: list[str] = []
    if _method_matches_current_evidence(method, evidence_package):
        competitors.extend(
            str(item or "").strip()
            for item in (method.get("competitorPool") if isinstance(method.get("competitorPool"), list) else [])
            if str(item or "").strip()
        )
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    competitors.extend(
        str(item or "").strip()
        for item in (entities.get("competitors") if isinstance(entities.get("competitors"), list) else [])
        if str(item or "").strip()
    )
    if competitors:
        return "、".join(_dedupe(competitors)[:4])
    targets = [
        str(item or "").strip()
        for item in (entities.get("models") if isinstance(entities.get("models"), list) else [])
        if str(item or "").strip()
    ]
    targets = _dedupe_entity_names_by_specificity(targets)
    if targets:
        return "、".join(_dedupe(targets)[:3]) + " 与目标竞品"
    return "目标车型与目标竞品"


def _market_fit_target_label(
    question_text: str,
    method: BusinessMethodDistillation | None,
    evidence_package: dict[str, Any] | None = None,
    *,
    powertrain: str = "",
) -> str:
    package = evidence_package if isinstance(evidence_package, dict) else {}
    selected_powertrain = powertrain or _market_opportunity_powertrain(package, question_text)

    def with_powertrain(model: str) -> str:
        normalized_model = str(model or "").strip()
        if not normalized_model or not selected_powertrain or selected_powertrain in normalized_model.upper():
            return normalized_model
        return f"{normalized_model} {selected_powertrain}"

    if (
        _method_matches_current_evidence(method, evidence_package)
        and _question_mentions_method_model(method, question_text)
    ):
        model = str(method.get("model") or "").strip()
        if model:
            return with_powertrain(model)
    entities = package.get("entities") if isinstance(package.get("entities"), dict) else {}
    models = _dedupe_entity_names_by_specificity([
        str(item or "").strip()
        for item in (entities.get("models") if isinstance(entities.get("models"), list) else [])
        if str(item or "").strip()
    ])
    if models:
        return with_powertrain(models[0])
    question_model = _market_model_from_question(question_text)
    if question_model:
        return with_powertrain(question_model)
    return f"{selected_powertrain} 产品线" if selected_powertrain else "目标产品线"


def _market_model_from_question(question_text: str) -> str:
    candidates = re.findall(r"\b[A-Za-z][A-Za-z0-9-]*\d[A-Za-z0-9-]*\b", str(question_text or ""))
    for candidate in reversed(candidates):
        normalized = candidate.upper()
        if re.fullmatch(r"(?:SUV)?[A-E]\d?", normalized):
            continue
        if normalized in {"2WD", "4WD"}:
            continue
        return candidate.upper()
    return ""


def _market_fit_competitor_pool_label(
    evidence_package: dict[str, Any],
    *,
    method: BusinessMethodDistillation | None = None,
) -> str:
    competitors: list[str] = []
    if _method_matches_current_evidence(method, evidence_package):
        competitors.extend(
            str(item or "").strip()
            for item in (method.get("competitorPool") if isinstance(method.get("competitorPool"), list) else [])
            if str(item or "").strip()
        )
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    competitors.extend(
        str(item or "").strip()
        for item in (entities.get("competitors") if isinstance(entities.get("competitors"), list) else [])
        if str(item or "").strip()
    )
    values = _dedupe(competitors)
    return "、".join(values[:4]) if values else "核心竞品池"


def _method_matches_current_evidence(
    method: BusinessMethodDistillation | None,
    evidence_package: dict[str, Any] | None,
) -> bool:
    if not isinstance(method, dict):
        return False
    if not isinstance(evidence_package, dict):
        return True
    return _business_method_supported_by_evidence(method, evidence_package)


def _market_fit_has_usable_internal_market_evidence(evidence_package: dict[str, Any]) -> bool:
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool_result in tool_results:
        if not isinstance(tool_result, dict) or not tool_result.get("success"):
            continue
        source_type = str(tool_result.get("sourceType") or "").strip().lower()
        if source_type not in {"jato_parquet", "generated"}:
            continue
        row_count = tool_result.get("rowCount")
        refs = tool_result.get("evidenceRefs") if isinstance(tool_result.get("evidenceRefs"), list) else []
        if isinstance(row_count, (int, float)) and row_count <= 0 and not refs:
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            value = ref.get("value")
            if value in (None, "", 0, "0"):
                continue
            label = _normalize_question_text(f"{ref.get('refId') or ''} {ref.get('label') or ''}")
            if _contains_any(
                label,
                (
                    "hev",
                    "bev",
                    "phev",
                    "suv",
                    "segment",
                    "powertrain",
                    "fuel",
                    "drivebyfuel",
                    "drivebysegment",
                    "registrationbyfuel",
                    "registrationbysegment",
                    "sales",
                    "share",
                    "volume",
                    "penetration",
                    "mix",
                ),
            ):
                return True
    return False


def _market_fit_gap_action(
    country_label: str,
    target: str,
    competitor_pool: str,
    powertrain: str = "目标动力",
) -> str:
    return (
        f"补齐{_market_label(country_label)} {target} 的 {powertrain} SUV A0/A、"
        f"{competitor_pool}和价格/配置证据"
    )


def _market_fit_matrix_action(country_label: str, target: str, competitor_pool: str) -> str:
    if competitor_pool and competitor_pool != "核心竞品池":
        return f"建立 {target} vs {competitor_pool} 车型级价格/配置矩阵和市场结构表"
    return f"建立 {_market_label(country_label)} {target} 在 SUV A0/A 的车型级价格/配置矩阵、竞品池和市场结构表"


def _dedupe_entity_names_by_specificity(values: list[str]) -> list[str]:
    cleaned = _dedupe([_normalize_space(str(value or "")) for value in values if str(value or "").strip()])
    if len(cleaned) <= 1:
        return cleaned
    keys = {value: _model_key(value) for value in cleaned}
    result: list[str] = []
    for value in cleaned:
        key = keys.get(value, "")
        if key and any(key != other_key and key in other_key for other_key in keys.values()):
            continue
        result.append(value)
    return result or cleaned


def _public_market_evidence_ref_label(ref: dict[str, Any]) -> str:
    label = _normalize_space(str(ref.get("label") or ""))
    if not label:
        return ""
    context_label = _public_context_evidence_ref_label(label)
    if context_label:
        return context_label
    parts = [part.strip() for part in re.split(r"[.>/|]", label) if part.strip()]
    lowered = [part.lower() for part in parts]
    if "crosstabs" in lowered:
        cross_tab_index = lowered.index("crosstabs")
        if len(parts) > cross_tab_index + 2:
            dimension = parts[cross_tab_index + 1].replace("_", " ").strip().lower()
            signal_parts = parts[cross_tab_index + 2 : -1]
            signal = " ".join(part.replace("_", " ").strip() for part in signal_parts if part.strip())
            metric = parts[-1].replace("_", " ").strip()
            if signal and metric.lower() in {"sales", "volume", "value", "count", "registration", "registrations"}:
                if "segment" in dimension:
                    return f"{signal} 细分销量"
                if "fuel" in dimension or "powertrain" in dimension:
                    return f"{signal} 动力销量"
                if "registration" in dimension:
                    return f"{signal} 注册量"
                return f"{signal} 销量"
            if signal and metric and metric.lower() not in {"sales", "volume", "value", "count", "registration", "registrations"}:
                return f"{signal} {_public_market_metric_label(metric)}"
            if signal:
                return signal
    if "powertrainmix" in lowered and len(parts) >= 3:
        powertrain = parts[-2].upper()
        metric = parts[-1].lower()
        if metric in {"sales", "value", "volume", "count", "registrations", "registration"}:
            return f"{powertrain} 动力销量"
        if metric in {"share", "mix", "penetration"} or metric.endswith("_pct"):
            return f"{powertrain} 动力占比"
        return powertrain
    if "topmodels" in lowered and len(parts) >= 3:
        return parts[-2]
    if "yearseries" in lowered and len(parts) >= 3:
        return parts[-2]
    if len(parts) >= 2:
        metric = parts[-1].lower()
        if metric in {"sales", "volume", "value", "count", "share", "mix", "penetration"}:
            return parts[-2]
    if "contextsnapshot" in label.lower():
        return ""
    return _public_evidence_ref_label(ref)


def _public_context_evidence_ref_label(label: str) -> str:
    text = _normalize_space(str(label or ""))
    if not text:
        return ""
    match = re.match(r"crossCountry\.([^.]+)\.kpis\.cumulativeSales$", text, flags=re.IGNORECASE)
    if match:
        return f"{_country_label(match.group(1))}累计销量"
    match = re.match(
        r"crossCountry\.([^.]+)\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.(?:sales|value)$",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return f"{_country_label(match.group(1))}{match.group(2).upper()} 动力销量"
    match = re.match(
        r"crossCountry\.([^.]+)\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.share$",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return f"{_country_label(match.group(1))}{match.group(2).upper()} 动力占比"
    match = re.match(r"(?:contextSnapshot|marketSnapshot)\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.share$", text, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).upper()} 动力占比"
    match = re.match(r"(?:contextSnapshot|marketSnapshot)\.(?:powertrainMix|动力类型Mix)\.([^.]+)\.(?:sales|value)$", text, flags=re.IGNORECASE)
    if match:
        return f"{match.group(1).upper()} 动力销量"
    return ""


def _public_market_metric_label(metric: str) -> str:
    text = _normalize_space(str(metric or "").replace("_", " "))
    lower = text.lower()
    if lower == "pct":
        return "占比"
    if lower.endswith(" pct"):
        return f"{text[:-4].strip()} 占比"
    if lower in {"share", "mix", "penetration"}:
        return "占比"
    if lower.endswith(" share"):
        return f"{text[:-6].strip()} 占比"
    return text


def _compose_business_bullets(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    plan: BusinessSynthesisPlan,
    frame: dict[str, str],
) -> list[str]:
    existing = [_clean_business_text(item) for item in _string_list(answer.get("bullets"))]
    priority_existing = _ordered_existing_guidance_bullets(existing)
    if plan["intent"] == "market_overview" and _is_market_fit_public_plan(plan):
        country_label = _country_label(str(plan.get("country") or "当前市场"))
        public_actions = _public_recommended_actions(plan)
        action_text = public_actions[0]["action"] if public_actions else frame["action"]
        report_bullets = [
            _localize_public_market_text(str(item), country_label)
            for item in _string_list(plan.get("reportReadyBullets"))
        ]
        return _dedupe_business_bullets([
            *report_bullets,
            f"下一步动作：{action_text}。",
        ])[:5]
    if plan["intent"] == "report_generation":
        report_lines = _report_slide_lines_from_plan(plan)
        if report_lines:
            if priority_existing and _should_prioritize_existing_guidance(answer, evidence_package):
                return _dedupe_business_bullets([*priority_existing[:5], *report_lines[:1], *priority_existing[5:]])[:6]
            return _dedupe_business_bullets(report_lines[:5])
    if plan["intent"] in {"news_policy_search", "competitor_compare", "configuration_analysis"}:
        report_lines = _report_slide_lines_from_plan(plan)
        if report_lines:
            if priority_existing and _should_prioritize_existing_guidance(answer, evidence_package):
                return _dedupe_business_bullets([*priority_existing[:5], *report_lines[:1], *priority_existing[5:]])[:6]
            return _dedupe_business_bullets(report_lines[:5])
    if (
        plan["intent"] == "pricing_analysis"
        and isinstance(plan.get("methodDistillation"), dict)
        and plan["methodDistillation"].get("methodType") == "pricing_positioning"
        and plan["evidenceAlignment"]["status"] != "insufficient"
        and evidence_ref_count(evidence_package) > 0
    ):
        pricing_method_bullets = _pricing_method_business_bullets(plan, evidence_package, frame)
        if pricing_method_bullets:
            return pricing_method_bullets
    evidence_digest = _evidence_digest_sentence(evidence_package, plan["intent"], limit=4)
    evidence_text = (
        f"已查数据：{evidence_digest}。{_evidence_quality_suffix(evidence_package, plan['intent'])}"
        if evidence_digest
        else frame["why"]
    )
    display_plan = _display_plan_for_intent(plan["intent"], evidence_package)
    conclusion_bullet = f"结论：{_clean_visible_direct_text(_strip_direct_prefix(frame['verdict']))}"
    bullets = [
        f"证据：{evidence_text}",
        f"产品经理判断：{frame['soWhat']}",
    ]
    if not _business_bullet_repeats_direct(conclusion_bullet, str(answer.get("direct") or "")):
        bullets.insert(0, conclusion_bullet)
    pending_review_bullet = _pending_msrp_review_bullet(evidence_package) if plan["intent"] == "pricing_analysis" else ""
    if pending_review_bullet:
        bullets.append(pending_review_bullet)
    if display_plan:
        bullets.append(f"展示：{display_plan}")
    bullets.extend([
        f"下一步动作：{frame['action']}",
        f"风险边界：{frame['risk']}",
    ])
    limited_progress = _evidence_limited_progress_bullet(plan, evidence_package)
    if limited_progress:
        bullets.insert(3, limited_progress)
    if evidence_ref_count(evidence_package) == 0:
        bullets.append("补数前仍可推进：先输出竞品池、价格/配置验证表、政策查证路径和汇报结构，等可引用证据回来后再写确定数字。")
    priority_generated = [
        item
        for item in bullets
        if item.startswith("补数前仍可推进") or item.startswith("待审核价格")
    ]
    regular_generated = [item for item in bullets if item not in priority_generated]
    if priority_existing and _should_prioritize_existing_guidance(answer, evidence_package):
        return _dedupe_business_bullets([*priority_existing[:5], *priority_generated, *regular_generated[:1], *priority_existing[5:]])[:6]
    return _dedupe_business_bullets([*regular_generated, *priority_generated])[:6]


def _business_bullet_repeats_direct(bullet: str, direct: str) -> bool:
    direct_text = _clean_visible_direct_text(_strip_direct_prefix(direct))
    bullet_text = _clean_visible_direct_text(_strip_direct_prefix(re.sub(r"^\s*结论\s*[：:]\s*", "", str(bullet or ""))))
    if not direct_text or not bullet_text:
        return False
    return _same_business_sentence(direct_text, bullet_text)


def _pricing_method_business_bullets(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    frame: dict[str, str],
) -> list[str]:
    report_lines = [
        _clean_business_text(str(item or ""))
        for item in plan.get("reportReadyBullets", [])
        if str(item or "").strip()
    ]
    if not report_lines:
        return []
    display_plan = _display_plan_for_intent(plan["intent"], evidence_package)
    bullets = [f"结论：{report_lines[0]}"]
    if len(report_lines) > 1:
        bullets.append(f"证据链：{report_lines[1]}")
    pending_review_bullet = _pending_msrp_review_bullet(evidence_package)
    if pending_review_bullet:
        bullets.append(pending_review_bullet)
    if len(report_lines) > 2:
        bullets.append(f"产品经理判断：{report_lines[2]}")
    if len(report_lines) > 3:
        bullets.append(f"风险边界：{report_lines[3]}")
    if display_plan:
        bullets.append(f"展示：{display_plan}")
    bullets.append(f"下一步动作：{frame['action']}")
    return _dedupe_business_bullets(bullets)[:6]


def _is_market_fit_public_plan(plan: BusinessSynthesisPlan) -> bool:
    text = _normalize_question_text(" ".join([
        str(plan.get("executiveConclusion") or ""),
        *[str(item or "") for item in _string_list(plan.get("reportReadyBullets"))],
    ]))
    return "待验证机会" in text and _is_market_fit_question(text)


def _evidence_limited_progress_bullet(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> str:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    missing_names = [
        str(item.get("name") or "").strip()
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    if not missing_names or evidence_ref_count(evidence_package) <= 0:
        return ""
    missing_labels = _missing_evidence_label_list(missing_names)
    if plan["intent"] == "pricing_analysis" and any(
        "current_msrp" in name or "own_model_price" in name or "price_corridor" in name
        for name in missing_names
    ):
        return (
            f"证据有限但可推进：当前可先做目标价/价格走廊的场景判断；缺{missing_labels}会影响最终价格锚点、价差和主销版本结论；"
            "下一步补齐本车型官方 MSRP、竞品价格走廊、月供/RV/company car 后再定稿。"
        )
    action = plan["recommendedActions"][0]["action"] if plan["recommendedActions"] else "补齐缺失证据后再定稿"
    return (
        f"证据有限但可推进：当前已有部分证据可支撑方向判断；缺{missing_labels}会削弱最终结论；"
        f"下一步执行：{action}。"
    )


def _missing_evidence_label_list(names: list[str]) -> str:
    labels = []
    for name in names[:3]:
        label = _GAP_LABELS.get(name, _risk_display_name(name))
        if label:
            labels.append(label)
    return "、".join(_dedupe(labels)) or "关键证据"


def _business_frame(plan: BusinessSynthesisPlan, evidence_package: dict[str, Any]) -> dict[str, str]:
    first_implication = _first_non_empty(plan["businessImplications"])
    first_action = ""
    if plan["recommendedActions"]:
        first_action = plan["recommendedActions"][0]["action"]
    risk = _risk_summary(plan)
    refs = _intent_relevant_evidence_ref_count(evidence_package, str(plan.get("intent") or ""))
    confidence = str(evidence_package.get("confidence") or "low")
    verdict = plan["executiveConclusion"]
    why = plan["evidenceAlignment"]["summary"]
    if refs:
        why = f"{why} 当前有 {_intent_evidence_count_note(refs, str(plan.get('intent') or ''))}，置信度{_confidence_label(confidence)}。"
    else:
        why = f"{why} {_intent_missing_evidence_note(str(plan.get('intent') or ''))}，置信度{_confidence_label(confidence)}。"
    return {
        "verdict": verdict,
        "why": why,
        "soWhat": first_implication or "需要把证据继续收敛成产品、价格、配置、渠道或汇报动作。",
        "action": first_action or "补齐核心证据后生成可复用业务表和汇报页。",
        "risk": risk,
    }


_INTENT_EVIDENCE_TOKENS: dict[str, tuple[str, ...]] = {
    "pricing_analysis": ("msrp", "price", "pricing", "corridor", "target", "monthly", "rv", "pva", "trim", "gap"),
    "market_overview": ("sales", "volume", "share", "market", "segment", "bev", "hev", "phev", "suv", "penetration"),
    "competitor_compare": ("competitor", "sales", "share", "model", "segment", "sorento", "sportage", "rav4", "variant", "feature", "configuration"),
    "configuration_analysis": ("trim", "config", "feature", "equipment", "battery", "range", "winter", "tow", "heat", "adas", "variant"),
    "inventory_analysis": ("stock", "inventory", "bom", "material", "variant", "version", "order", "lifecycle"),
    "news_policy_search": ("policy", "source", "date", "published", "tax", "benefit", "subsidy", "price cap", "eligibility"),
    "voc_analysis": ("voc", "forum", "review", "user", "source", "count", "complaint", "sentiment"),
    "report_generation": ("msrp", "price", "share", "sales", "segment", "bev", "hev", "phev", "feature", "policy"),
}

_CITATION_FIRST_EVIDENCE_INTENTS = {"news_policy_search", "voc_analysis", "report_generation"}
_CITATION_AWARE_EVIDENCE_INTENTS = _CITATION_FIRST_EVIDENCE_INTENTS | {"competitor_compare"}


def _evidence_digest_lines(
    evidence_package: dict[str, Any],
    intent: str = "",
    *,
    limit: int = 4,
) -> list[str]:
    intent_key = str(intent or "")
    external_lines = _external_citation_digest_lines(evidence_package, limit=limit)
    if intent_key == "news_policy_search" and external_lines:
        return external_lines[:limit]
    if intent_key == "pricing_analysis":
        pricing_lines = _pricing_evidence_digest_lines(evidence_package, limit=limit)
        if pricing_lines:
            return pricing_lines
    refs = _prioritized_evidence_refs(evidence_package, intent)
    if external_lines and intent_key in _CITATION_AWARE_EVIDENCE_INTENTS:
        refs = [ref for ref in refs if not _is_external_citation_member_ref(ref)]
    if intent_key == "voc_analysis" and not external_lines:
        refs = [ref for ref in refs if _is_voc_evidence_ref(ref)]
    if intent_key == "competitor_compare":
        refs = _competitor_evidence_digest_refs(evidence_package, refs)
    lines: list[str] = list(external_lines) if intent_key in _CITATION_AWARE_EVIDENCE_INTENTS else []
    for ref in refs:
        if _evidence_ref_is_zero_volume(ref):
            continue
        if _evidence_ref_is_non_business_metadata(ref):
            continue
        label = _public_scoped_evidence_ref_label(ref)
        if not label:
            continue
        value = _format_evidence_ref_value(ref)
        source = _public_evidence_ref_source(ref)
        line = f"{label} = {value}" if value else label
        if source:
            line = f"{line}（{source}）"
        lines.append(_clean_business_text(line))
        if len(_dedupe(lines)) >= limit:
            break
    return _dedupe(lines)[:limit]


def _business_evidence_digest_lines(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    *,
    limit: int = 4,
) -> list[str]:
    intent = str(plan.get("intent") or "")
    if intent == "configuration_analysis":
        return _configuration_evidence_digest_lines(plan, evidence_package, limit=limit)
    return _evidence_digest_lines(evidence_package, intent, limit=limit)


def _configuration_evidence_digest_lines(
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
    *,
    limit: int,
) -> list[str]:
    refs = _configuration_takeaway_refs(_all_evidence_refs(evidence_package))
    market_refs = _configuration_market_context_refs(evidence_package)
    lines: list[str] = []
    for ref in refs:
        line = _evidence_ref_digest_line(ref)
        if line:
            lines.append(line)
        if len(_dedupe(lines)) >= limit:
            return _dedupe(lines)[:limit]
    if not refs and market_refs:
        for ref in market_refs[:2]:
            line = _evidence_ref_digest_line(ref)
            if line:
                lines.append(line)
            if len(_dedupe(lines)) >= limit:
                return _dedupe(lines)[:limit]
    topic = _configuration_topic_note(plan, refs)
    status = _configuration_evidence_status_note(evidence_package, refs)
    if topic:
        lines.append(f"配置验证项 = {topic}")
    if status:
        lines.append(f"证据状态 = {status}")
    missing_note = _missing_evidence_note(evidence_package)
    if missing_note and missing_note != "可引用证据":
        lines.append(f"缺口 = {missing_note}")
    tools = [item for item in evidence_tool_names(evidence_package) if item != "business_method_material"]
    if tools:
        tool_labels = _dedupe([_tool_business_label(item) for item in tools])[:3]
        lines.append(f"已尝试工具 = {'、'.join(tool_labels)}")
    return _dedupe(lines)[:limit]


def _competitor_evidence_digest_refs(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requested = _requested_entity_names_from_package(evidence_package)
    market_context_refs = _competitor_market_context_digest_refs(evidence_package, refs)
    if not requested:
        return market_context_refs or refs
    matched = [ref for ref in refs if _ref_mentions_any_model(ref, requested)]
    if not matched:
        return market_context_refs
    metric_matched = [ref for ref in matched if not _is_competitor_model_only_ref(ref)]
    if market_context_refs:
        return _dedupe_ref_list([*metric_matched[:2], *market_context_refs, *metric_matched[2:]])
    return metric_matched


def _competitor_market_context_digest_refs(
    evidence_package: dict[str, Any],
    refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not _competitor_package_needs_market_context(evidence_package):
        return []
    focused = [
        ref
        for ref in refs
        if _competitor_market_context_ref_priority(ref)[0] > 0
        and not _competitor_digest_ref_is_zero_percentage(ref)
    ]
    return sorted(focused, key=_competitor_market_context_ref_priority, reverse=True)


def _competitor_package_needs_market_context(evidence_package: dict[str, Any]) -> bool:
    question_text = _competitor_package_question_text(evidence_package).casefold()
    if _competitor_prefers_large_suv_electrified_context(evidence_package, question_text):
        return True
    return _contains_any(
        question_text,
        [
            "7座",
            "7 座",
            "四驱",
            "4wd",
            "awd",
            "能打",
        ],
    )


def _competitor_package_question_text(evidence_package: dict[str, Any]) -> str:
    parts: list[str] = []
    for tool in evidence_package.get("toolResults") or []:
        if not isinstance(tool, dict):
            continue
        query = tool.get("query")
        if isinstance(query, dict):
            parts.append(str(query.get("question") or ""))
    return _normalize_space(" ".join(parts))


def _competitor_market_context_ref_priority(ref: dict[str, Any]) -> tuple[int, str]:
    label = str(ref.get("label") or "")
    normalized = label.casefold()
    order = [
        ("drivebysegment.suv b.4wd_pct", 104),
        ("drivebyfuel.phev.4wd_pct", 102),
        ("segmentbyfuel.suv b.phev_pct", 100),
        ("registrationbyfuel.phev.business_pct", 98),
        ("drivebysegment.suv b.sales", 96),
        ("drivebysegment.suv a.4wd_pct", 94),
        ("drivebysegment.suv a.sales", 92),
        ("registrationbyfuel.phev.sales", 90),
        ("registrationbysegment.suv a.business_pct", 88),
        ("segmentbyfuel.suv a.phev_pct", 86),
    ]
    for token, score in order:
        if token in normalized:
            return score, label
    return 0, label


def _competitor_digest_ref_is_zero_percentage(ref: dict[str, Any]) -> bool:
    unit = str(ref.get("unit") or "").casefold()
    label = str(ref.get("label") or "").casefold()
    if "%" not in unit and not any(token in label for token in ("_pct", ".pct", "share")):
        return False
    value = _numeric_ref_value(ref)
    return value is not None and value <= 0


def _dedupe_ref_list(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        key = "|".join(
            str(ref.get(part) or "")
            for part in ("refId", "label", "value", "source")
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(ref)
    return result


def _is_competitor_model_only_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").strip().lower()
    return label.startswith("competitor.") and label.endswith(".model")


def _requested_entity_names_from_package(evidence_package: dict[str, Any]) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    result: list[str] = []
    for key in ("models", "competitors"):
        values = entities.get(key) if isinstance(entities.get(key), list) else []
        for value in values:
            text = _normalize_space(str(value or ""))
            if text:
                result.append(text)
    return _dedupe(result)


def _prepare_business_evidence_package(
    evidence_package: dict[str, Any],
    evidence_plan: dict[str, Any],
) -> dict[str, Any]:
    package = _with_evidence_plan_entities(evidence_package, evidence_plan)
    return _scope_evidence_package_to_requested_entities(package)


def _with_evidence_plan_entities(
    evidence_package: dict[str, Any],
    evidence_plan: dict[str, Any],
) -> dict[str, Any]:
    package_entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    plan_entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    if not plan_entities:
        return evidence_package
    merged = dict(package_entities)
    changed = False
    for key, value in plan_entities.items():
        if key in merged:
            continue
        if isinstance(value, list):
            merged[key] = list(value)
            changed = True
        elif isinstance(value, (str, int, float)) and str(value).strip():
            merged[key] = value
            changed = True
    if not changed:
        return evidence_package
    updated = dict(evidence_package)
    updated["entities"] = merged
    return updated


_KNOWN_ENTITY_SCOPE_MODELS = (
    "J7 HEV",
    "J7",
    "J8",
    "O5 BEV",
    "O5",
    "O9",
    "OMODA 5",
    "OMODA 9",
    "Corolla Cross",
    "RAV4",
    "Sportage",
    "Qashqai",
    "C-HR",
    "EV3",
    "EX30",
    "EX60",
    "XC60",
    "Sorento",
    "Tucson",
    "Model Y",
    "ID.7",
)


def _scope_evidence_package_to_requested_entities(evidence_package: dict[str, Any]) -> dict[str, Any]:
    requested = _requested_entity_names_from_package(evidence_package)
    if not requested:
        return evidence_package
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    if not tool_results and not evidence_package.get("insightCards"):
        return evidence_package

    scoped_tools: list[dict[str, Any]] = []
    changed = False
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        original_refs = _coerce_evidence_refs(tool.get("evidenceRefs"))
        scoped_refs = [
            ref
            for ref in original_refs
            if _evidence_ref_in_requested_entity_scope(ref, evidence_package)
        ]
        if original_refs and len(scoped_refs) != len(original_refs):
            changed = True

        tool_text = _tool_result_scope_text(tool)
        has_requested_ref = any(_ref_mentions_requested_entity(ref, requested) for ref in scoped_refs)
        if original_refs and not has_requested_ref and _text_mentions_unrequested_vehicle(tool_text, evidence_package):
            changed = True
            continue
        if not scoped_refs and original_refs and _text_mentions_unrequested_vehicle(tool_text, evidence_package):
            changed = True
            continue
        if not original_refs and _text_mentions_unrequested_vehicle(tool_text, evidence_package):
            changed = True
            continue

        if original_refs:
            scoped_tool = dict(tool)
            scoped_tool["evidenceRefs"] = scoped_refs
            scoped_tools.append(scoped_tool)
        else:
            scoped_tools.append(dict(tool))

    insight_cards = evidence_package.get("insightCards") if isinstance(evidence_package.get("insightCards"), list) else []
    scoped_cards = [
        dict(card)
        for card in insight_cards
        if isinstance(card, dict)
        and not _text_mentions_unrequested_vehicle(_insight_card_scope_text(card), evidence_package)
    ]
    if len(scoped_cards) != len([card for card in insight_cards if isinstance(card, dict)]):
        changed = True

    if not changed:
        return evidence_package

    updated = dict(evidence_package)
    updated["toolResults"] = scoped_tools
    if insight_cards:
        updated["insightCards"] = scoped_cards
    if not _all_evidence_refs(updated):
        updated["confidence"] = "low"
        updated["jatoCrossCheck"] = {
            "status": "insufficient",
            "summary": "可引用证据未匹配当前问题的目标车型或竞品实体。",
        }
        missing = updated.get("missingEvidence") if isinstance(updated.get("missingEvidence"), list) else []
        updated["missingEvidence"] = [
            *missing,
            {
                "name": "requested_entity_evidence",
                "reason": "No scoped evidence refs matched the requested models or competitors.",
                "impact": "weakens_answer",
            },
        ]
    return updated


def _evidence_ref_in_requested_entity_scope(ref: dict[str, Any], evidence_package: dict[str, Any]) -> bool:
    if not _ref_mentions_known_vehicle(ref):
        return True
    requested = _requested_entity_names_from_package(evidence_package)
    if not requested:
        return True
    return _ref_mentions_requested_entity(ref, requested)


def _ref_mentions_known_vehicle(ref: dict[str, Any]) -> bool:
    return _text_mentions_any_known_vehicle(
        " ".join(
            str(ref.get(key) or "")
            for key in ("label", "value", "source", "table")
        )
    )


def _ref_mentions_requested_entity(ref: dict[str, Any], requested: list[str]) -> bool:
    text = " ".join(
        str(ref.get(key) or "")
        for key in ("label", "value", "source", "table")
    )
    haystack = _model_key(text)
    if not haystack:
        return False
    for value in requested:
        requested_key = _model_key(value)
        if requested_key and (requested_key in haystack or haystack in requested_key):
            return True
    return any(
        _model_key(model)
        and _model_key(model) in haystack
        and _known_model_matches_requested_entity(model, requested)
        for model in _KNOWN_ENTITY_SCOPE_MODELS
    )


def _text_mentions_unrequested_vehicle(text: str, evidence_package: dict[str, Any]) -> bool:
    requested = _requested_entity_names_from_package(evidence_package)
    if not requested:
        return False
    haystack = _model_key(text)
    if not haystack:
        return False
    for model in _KNOWN_ENTITY_SCOPE_MODELS:
        model_key = _model_key(model)
        if model_key and model_key in haystack and not _known_model_matches_requested_entity(model, requested):
            return True
    return False


def _known_model_matches_requested_entity(model: str, requested: list[str]) -> bool:
    model_key = _model_key(model)
    if not model_key:
        return False
    if _model_name_in_list(model, requested):
        return True
    requested_keys = [_model_key(item) for item in requested]
    return any(
        requested_key
        and (model_key in requested_key or requested_key in model_key)
        for requested_key in requested_keys
    )


def _text_mentions_any_known_vehicle(text: str) -> bool:
    haystack = _model_key(text)
    if not haystack:
        return False
    return any(_model_key(model) and _model_key(model) in haystack for model in _KNOWN_ENTITY_SCOPE_MODELS)


def _tool_result_scope_text(tool: dict[str, Any]) -> str:
    key_findings = tool.get("keyFindings") if isinstance(tool.get("keyFindings"), list) else []
    query = tool.get("query") if isinstance(tool.get("query"), dict) else {}
    return " ".join(
        [
            str(tool.get("toolName") or ""),
            str(tool.get("summary") or ""),
            " ".join(str(item or "") for item in key_findings),
            " ".join(str(value or "") for value in query.values()),
        ]
    )


def _insight_card_scope_text(card: dict[str, Any]) -> str:
    return " ".join(
        str(card.get(key) or "")
        for key in ("title", "claim", "implication", "recommendedAction")
    )


def _ref_mentions_any_model(ref: dict[str, Any], models: list[str]) -> bool:
    haystack = _model_key(
        " ".join(
            str(ref.get(key) or "")
            for key in ("label", "value", "source", "table")
        )
    )
    return bool(haystack) and any(_model_key(model) and _model_key(model) in haystack for model in models)


def _is_external_citation_member_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").strip()
    _, suffix = _split_external_ref_label(label)
    return suffix in {"source", "claim", "date", "rank", "rankSeed"}


def _is_voc_evidence_ref(ref: dict[str, Any]) -> bool:
    haystack = " ".join(
        str(ref.get(key) or "").casefold()
        for key in ("label", "source", "table", "unit")
    )
    return any(
        token in haystack
        for token in (
            "voc",
            "voice of customer",
            "forum",
            "review",
            "complaint",
            "sentiment",
            "owner",
            "用户",
            "吐槽",
            "投诉",
            "论坛",
            "口碑",
        )
    )


def _pricing_evidence_digest_lines(
    evidence_package: dict[str, Any],
    *,
    limit: int,
) -> list[str]:
    refs = _all_evidence_refs(evidence_package)
    if not refs:
        return _pricing_missing_evidence_digest_lines(evidence_package, limit=limit)
    has_requested_price_gap = _pricing_requested_model_price_gap(evidence_package)
    lines: list[str] = []
    if has_requested_price_gap:
        lines.append("本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证")
    material_refs = sorted(
        [
            ref
            for ref in refs
            if _pricing_user_material_ref_priority(str(ref.get("label") or "")) < 100
            and _pricing_user_material_ref_matches_scope(ref, evidence_package)
        ],
        key=lambda ref: (
            _pricing_user_material_ref_priority_for_gap(str(ref.get("label") or ""))
            if has_requested_price_gap
            else _pricing_user_material_ref_priority(str(ref.get("label") or "")),
            str(ref.get("refId") or ""),
        ),
    )
    if has_requested_price_gap and material_refs:
        for ref in material_refs:
            line = _evidence_ref_digest_line(ref)
            if line:
                lines.append(line)
            if len(_dedupe(lines)) >= limit:
                break
        if len(_dedupe(lines)) < limit:
            lines.extend(_pricing_background_sample_digest_lines(refs, limit=limit - len(_dedupe(lines))))
        return _dedupe(lines)[:limit]
    priority_labels = (
        "priceStats.min",
        "priceStats.max",
        "User supplied own-model target price min",
        "User supplied own-model target price max",
        "User supplied own-model target price midpoint",
        "User supplied relative price delta",
        "User supplied price-delta direction",
        "priceStats.avg",
        "priceStats.median",
    )
    for label in priority_labels:
        ref = next((item for item in refs if str(item.get("label") or "").strip() == label), None)
        if not ref:
            continue
        line = _evidence_ref_digest_line(ref)
        if has_requested_price_gap and label.startswith("priceStats."):
            line = line.replace("价格样本", "背景价格样本", 1)
        if line:
            lines.append(line)
        if len(_dedupe(lines)) >= limit:
            break
    for ref in material_refs:
        line = _evidence_ref_digest_line(ref)
        if line:
            lines.append(line)
        if len(_dedupe(lines)) >= limit:
            break
    if len(_dedupe(lines)) < limit:
        lines.extend(_pricing_missing_evidence_digest_lines(evidence_package, limit=limit - len(_dedupe(lines))))
    return _dedupe(lines)[:limit]


def _pricing_verified_evidence_lines(evidence_package: dict[str, Any], *, limit: int) -> list[str]:
    """Return pricing evidence that came from tools/data, not user-material playbooks."""
    lines: list[str] = []
    has_requested_price_gap = _pricing_requested_model_price_gap(evidence_package)
    stats_line = _pricing_stats_evidence_line(_pricing_price_stats(evidence_package))
    if stats_line and not has_requested_price_gap:
        lines.append(stats_line)
    market_note = _pricing_live_market_evidence_note(evidence_package)
    if market_note:
        lines.append(market_note.rstrip("。"))
    if len(_dedupe(lines)) >= limit:
        return _dedupe(lines)[:limit]
    for ref in _all_evidence_refs(evidence_package):
        if _is_pricing_user_material_ref(ref):
            continue
        if not _pricing_ref_matches_requested_entity_scope(ref, evidence_package):
            continue
        label = str(ref.get("label") or "").strip().casefold()
        source = str(ref.get("source") or ref.get("table") or "").strip().casefold()
        if not label:
            continue
        if label.startswith("pricestats."):
            continue
        if _pricing_ref_is_secondary_cross_tab_sales(label):
            continue
        if market_note and _pricing_ref_is_market_note_powertrain_ref(label):
            continue
        if "sourcedraft" in label or "materialization" in label or "candidate" in label:
            continue
        if "jato_price_positioning" in source and "pricestats." in label:
            continue
        if not _pricing_ref_is_verified_business_signal(label, source):
            continue
        line = _evidence_ref_digest_line(ref)
        if line:
            lines.append(line)
        if len(_dedupe(lines)) >= limit:
            return _dedupe(lines)[:limit]
    return _dedupe(lines)[:limit]


def _pricing_user_material_hypothesis_lines(evidence_package: dict[str, Any], *, limit: int) -> list[str]:
    refs = sorted(
        [
            ref
            for ref in _all_evidence_refs(evidence_package)
            if _is_pricing_user_material_ref(ref)
            and _pricing_user_material_ref_matches_scope(ref, evidence_package)
        ],
        key=lambda ref: (
            _pricing_user_material_ref_priority(str(ref.get("label") or "")),
            str(ref.get("refId") or ""),
        ),
    )
    lines: list[str] = []
    for ref in refs:
        line = _evidence_ref_digest_line(ref)
        if line:
            lines.append(line)
        if len(_dedupe(lines)) >= limit:
            break
    return _dedupe(lines)[:limit]


def _is_pricing_user_material_ref(ref: dict[str, Any]) -> bool:
    if not _evidence_ref_is_user_material(ref):
        return False
    claim_type = str(ref.get("claimType") or "").strip().casefold()
    if claim_type.startswith(("pricing_", "competitor_", "main_trim_", "trim_price_", "pva_", "market_window_")):
        return True
    label = str(ref.get("label") or "")
    return _pricing_user_material_ref_priority(label) < 100


def _pricing_ref_is_verified_business_signal(label: str, source: str) -> bool:
    if any(token in source for token in ("jato_country_chart_deck", "jato_country_snapshot", "jato_parquet")):
        return any(token in label for token in ("sales", "share", "_pct", "mix", "segment", "powertrain", "fuel"))
    if any(token in source for token in ("jato_msrp", "postgres", "price", "pricing")):
        return any(token in label for token in ("pricing.records.", ".msrp", ".price", "current msrp", "own-model msrp", "premium msrp"))
    return any(
        token in label
        for token in (
            "current msrp",
            "own-model msrp",
            "premium msrp",
            "competitor corridor",
            "pricing.records.",
            ".msrp",
            ".price",
        )
    )


def _pricing_ref_is_secondary_cross_tab_sales(label: str) -> bool:
    return bool(
        re.match(
            r"(?:contextsnapshot|marketsnapshot)\.crosstabs\.drivebyfuel\.[^.]+\.(?:sales|value|volume|count)$",
            str(label or "").strip(),
            flags=re.IGNORECASE,
        )
    )


def _pricing_ref_is_powertrain_mix_summary(label: str) -> bool:
    return bool(
        re.match(
            r"(?:contextsnapshot|marketsnapshot)\.powertrainmix\.[^.]+\.(?:sales|share|value|volume|count)$",
            str(label or "").strip(),
            flags=re.IGNORECASE,
        )
    )


def _pricing_ref_is_market_note_powertrain_ref(label: str) -> bool:
    normalized = str(label or "").strip()
    if _pricing_ref_is_powertrain_mix_summary(normalized):
        return True
    return bool(
        re.match(
            r"(?:contextsnapshot|marketsnapshot)\.crosstabs\.(?:drivebyfuel|registrationbyfuel)\.[^.]+\.(?:sales|share|value|volume|count|2wd_pct|4wd_pct|awd_pct|business_pct|private_pct|retail_pct)$",
            normalized,
            flags=re.IGNORECASE,
        )
    )


def _pricing_missing_evidence_digest_lines(evidence_package: dict[str, Any], *, limit: int) -> list[str]:
    missing_names = _missing_evidence_names(evidence_package)
    lines: list[str] = []
    if "missing_required_tool:query_msrp_pricing" in missing_names:
        lines.append("缺少MSRP/当前价格工具结果")
    if missing_names & {"current_msrp", "current_official_msrp_cross_check", "own_model_price"}:
        lines.append("本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证")
    if missing_names & {"competitor_price_range", "competitor_corridor", "coverage_diagnostic:no_current_prices_for_requested_models"}:
        lines.append("竞品价格走廊 = 待补核心竞品官方价格 / 月供 / 促销口径")
    if missing_names & {"monthly_payment", "leasing_payment", "rv", "residual_value"}:
        lines.append("月供/RV = 待补 leasing、残值和 company car 成本口径")
    tools = [item for item in evidence_tool_names(evidence_package) if item != "business_method_material"]
    if tools:
        tool_labels = _dedupe([_tool_business_label(item) for item in tools])[:3]
        lines.append(f"已尝试工具 = {'、'.join(tool_labels)}，但未形成可引用价格证据")
    if not lines:
        lines.append("价格证据 = 待补官方 MSRP、竞品价格走廊、月供/RV 和配置价值证据")
    return _dedupe(lines)[:limit]


def _generic_pricing_evidence_direct_answer(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    action: str,
    display_note: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    rows = _pricing_anchor_rows(evidence_package)
    stats = _pricing_price_stats(evidence_package)
    if not rows and not stats:
        return ""
    subject = _pricing_subject_label(evidence_package)
    target_rows = [row for row in rows if row["role"] == "target"]
    competitor_rows = [row for row in rows if row["role"] == "competitor"]
    evidence_lines = _pricing_anchor_evidence_lines(rows, stats=stats, limit=5)
    if not evidence_lines:
        return ""
    stance = _pricing_anchor_business_stance(subject=subject, target_rows=target_rows, competitor_rows=competitor_rows, stats=stats)
    gap_digest = "；".join(_generic_pricing_evidence_boundary_lines(evidence_package, limit=4))
    parts = [
        f"定价判断：{country_label} {subject} {stance}",
        f"已查价格证据：{'；'.join(evidence_lines)}。",
        display_note,
        f"证据边界：{gap_digest or '月供/RV、促销支持、配置差异和官方来源仍需交叉验证'}。",
        f"下一步执行：{action or '把本车型与核心竞品价格、配置和月供/RV 放进同一张价格证据表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=1150))


def _generic_pricing_evidence_boundary_lines(evidence_package: dict[str, Any], *, limit: int) -> list[str]:
    missing_names = _missing_evidence_names(evidence_package)
    lines = [
        item
        for item in _pricing_missing_evidence_digest_lines(evidence_package, limit=limit)
        if not item.startswith("已尝试工具")
    ]
    if missing_names & {"configuration_delta", "feature_diff", "key_features", "trim"}:
        lines.append("配置差异 = 待补目标车型与核心竞品配置 / 电池 / ADAS / 质保价值")
    if missing_names & {"campaign_support", "discount", "promotion"}:
        lines.append("促销支持 = 待补 campaign / discount / dealer support")
    return _dedupe(lines)[:limit]


def _pricing_anchor_rows(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    targets = [str(item or "").strip() for item in (entities.get("models") if isinstance(entities.get("models"), list) else []) if str(item or "").strip()]
    competitors = [
        str(item or "").strip()
        for item in (entities.get("competitors") if isinstance(entities.get("competitors"), list) else [])
        if str(item or "").strip()
    ]
    requested = [*targets, *competitors]
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip()
        if not _is_pricing_anchor_ref(label):
            continue
        value = _numeric_ref_value(ref)
        if value is None:
            continue
        model = _pricing_anchor_model_label(ref, evidence_package)
        if requested and model and not _model_name_in_list(model, requested):
            continue
        role = _pricing_anchor_role(model, targets=targets, competitors=competitors)
        value_text = _format_evidence_ref_value(ref)
        if not value_text:
            continue
        key = f"{role}:{_model_key(model)}:{value_text}:{label.casefold()}"
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "model": model or ("目标车型" if role == "target" else "价格锚点"),
                "role": role,
                "value": value,
                "valueText": value_text,
                "label": label,
            }
        )
    return rows


def _is_pricing_anchor_ref(label: str) -> bool:
    lower = str(label or "").strip().casefold()
    if not lower:
        return False
    if lower.startswith("pricestats.") or "target price" in lower or "relative price delta" in lower:
        return False
    if "pva" in lower or "coverage" in lower or "corridor" in lower:
        return False
    return any(token in lower for token in ("pricing.records.", ".msrp", ".price", ".avgprice", ".minprice", ".maxprice", "current msrp", "own-model msrp"))


def _pricing_anchor_model_label(ref: dict[str, Any], evidence_package: dict[str, Any]) -> str:
    label = str(ref.get("label") or "").strip()
    model = _competitor_chart_model_from_label(label)
    if model:
        return model
    lower = label.casefold()
    if "current msrp" in lower or "own-model msrp" in lower:
        entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
        models = entities.get("models") if isinstance(entities.get("models"), list) else []
        for item in models:
            value = str(item or "").strip()
            if value:
                return value
    return ""


def _pricing_anchor_role(model: str, *, targets: list[str], competitors: list[str]) -> str:
    if model and _model_name_in_list(model, targets):
        return "target"
    if model and _model_name_in_list(model, competitors):
        return "competitor"
    return "reference"


def _pricing_anchor_evidence_lines(rows: list[dict[str, Any]], *, stats: dict[str, float], limit: int) -> list[str]:
    lines: list[str] = []
    for role in ("target", "competitor", "reference"):
        for row in [item for item in rows if item["role"] == role]:
            model = str(row.get("model") or "价格锚点")
            value_text = str(row.get("valueText") or "")
            if not value_text:
                continue
            label = "本车型" if role == "target" else "竞品" if role == "competitor" else "参考"
            lines.append(f"{label} {model} 价格 {value_text}")
            if len(_dedupe(lines)) >= limit:
                return _dedupe(lines)[:limit]
    stats_line = _pricing_stats_evidence_line(stats)
    if stats_line:
        lines.append(stats_line)
    return _dedupe(lines)[:limit]


def _pricing_anchor_business_stance(
    *,
    subject: str,
    target_rows: list[dict[str, Any]],
    competitor_rows: list[dict[str, Any]],
    stats: dict[str, float],
) -> str:
    target = target_rows[0] if target_rows else None
    competitor_values = [float(row["value"]) for row in competitor_rows if isinstance(row.get("value"), (int, float))]
    if target and competitor_values:
        target_value = float(target["value"])
        low = min(competitor_values)
        high = max(competitor_values)
        target_model = str(target.get("model") or subject or "本车型")
        target_price = str(target.get("valueText") or "").strip() or _pricing_anchor_price_text(target_value, [target])
        competitor_range = _pricing_anchor_range_text(low, high, competitor_rows)
        if target_value < low:
            return (
                f"已有本车型和竞品价格锚点，{target_model} {target_price} 低于已查竞品价格带 {competitor_range}，"
                "适合先写成价格锚点/低风险进入假设，但还不能定最终成交价。"
            )
        if target_value > high:
            return (
                f"已有本车型和竞品价格锚点，{target_model} {target_price} 高于已查竞品价格带 {competitor_range}，"
                "必须用配置、品牌、TCO 或渠道价值解释溢价，否则定价风险偏高。"
            )
        return (
            f"已有本车型和竞品价格锚点，{target_model} {target_price} 落在已查竞品价格带 {competitor_range} 内，"
            "可以进入价格走廊和主销版本验证。"
        )
    if target and stats:
        position = _price_position_statement(float(target["value"]), stats)
        target_model = str(target.get("model") or subject or "本车型")
        target_price = str(target.get("valueText") or "").strip() or _pricing_anchor_price_text(float(target["value"]), [target])
        return f"已有本车型价格锚点和价格样本统计，{target_model} {target_price} 的位置：{position}；但还需要确认样本是否就是核心竞品池。"
    if competitor_rows:
        return "已有竞品价格锚点，可以先建立竞品价格走廊；但目标车型价格缺失时，不能直接给目标 MSRP 或确定价差。"
    if stats:
        return "已有价格样本统计，可以先判断参考价格区间；但缺少本车型和核心竞品逐车型价格时，不能写成最终价格走廊。"
    return f"已有{subject}价格线索，可以先进入价格证据表验证。"


def _pricing_anchor_range_text(low: float, high: float, rows: list[dict[str, Any]]) -> str:
    unit = _pricing_anchor_unit(rows)
    if abs(low - high) < 0.000001:
        return _pricing_anchor_price_text(low, rows, unit=unit)
    suffix = f" {unit}" if unit else ""
    return f"{_format_price_number(float(low))}-{_format_price_number(float(high))}{suffix}"


def _pricing_anchor_price_text(value: float, rows: list[dict[str, Any]], *, unit: str | None = None) -> str:
    resolved_unit = unit if unit is not None else _pricing_anchor_unit(rows)
    suffix = f" {resolved_unit}" if resolved_unit else ""
    return f"{_format_price_number(float(value))}{suffix}"


def _pricing_anchor_unit(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        value_text = str(row.get("valueText") or "").strip()
        match = re.search(r"\s([A-Z]{2,5}|[A-Za-z]{2,5})$", value_text)
        if match:
            return match.group(1)
    return ""


def _generic_pricing_gap_direct_answer(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    action: str,
    display_note: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    missing_names = _missing_evidence_names(evidence_package)
    pricing_gap_names = {
        "current_msrp",
        "current_official_msrp_cross_check",
        "own_model_price",
        "competitor_price_range",
        "competitor_corridor",
        "monthly_payment",
        "leasing_payment",
        "rv",
        "residual_value",
        "coverage_diagnostic:no_current_prices_for_requested_models",
    }
    if not (missing_names & pricing_gap_names):
        return ""
    subject = _pricing_subject_label(evidence_package)
    gap_digest = "；".join(_pricing_missing_evidence_digest_lines(evidence_package, limit=3))
    parts = [
        f"直接结论：{country_label} {subject} 定价现在不能给确定数字，也不能给确定价格，置信度{confidence_note}；但可以先确定验证框架：价格走廊、竞品池、配置价值和购买场景。",
        f"关键缺口：{gap_digest}。",
        display_note,
        "业务含义：当前能推进的是价格矩阵和证据表，而不是把任何材料价、参考样本或模型推测写成最终 MSRP。",
        f"下一步执行：{action or '补齐本车型与竞品 MSRP / TP / 月供价格矩阵'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=980))


def _pricing_subject_label(evidence_package: dict[str, Any]) -> str:
    entities = _requested_entity_names_from_package(evidence_package)
    if entities:
        return " / ".join(entities[:3])
    return "目标车型"


def _business_method_supported_by_evidence(
    method: BusinessMethodDistillation,
    evidence_package: dict[str, Any],
) -> bool:
    """Only use distilled playbooks when the current evidence package supports that scope."""
    if method.get("methodType") != "pricing_positioning":
        return True
    model_key = _model_key(str(method.get("model") or ""))
    refs = _all_evidence_refs(evidence_package)
    if not refs:
        return False

    material_refs = [ref for ref in refs if _is_business_method_material_ref(ref)]
    scoped_material_refs = [
        ref for ref in material_refs
        if _pricing_user_material_ref_matches_scope(ref, evidence_package)
    ]
    if scoped_material_refs:
        return True
    if material_refs:
        return False

    return False


def _is_business_method_material_ref(ref: dict[str, Any]) -> bool:
    return _is_pricing_user_material_ref(ref)


def _evidence_ref_is_user_material(ref: dict[str, Any]) -> bool:
    source_type = str(ref.get("sourceType") or "").strip().casefold()
    tool_name = str(ref.get("evidenceToolName") or "").strip().casefold()
    table = str(ref.get("table") or "").strip().casefold()
    source = str(ref.get("source") or "").strip().casefold()
    label = str(ref.get("label") or "").strip().casefold()
    return (
        source_type == "user_material"
        or tool_name == "business_method_material"
        or table == "business_method_material"
        or "user material" in label
        or "method_fallback" in source
    )


def _pricing_background_sample_digest_lines(refs: list[dict[str, Any]], *, limit: int) -> list[str]:
    lines: list[str] = []
    for label in ("priceStats.min", "priceStats.max", "priceStats.avg", "priceStats.median"):
        ref = next((item for item in refs if str(item.get("label") or "").strip() == label), None)
        if not ref:
            continue
        line = _evidence_ref_digest_line(ref)
        if line:
            lines.append(line.replace("价格样本", "背景价格样本", 1))
        if len(_dedupe(lines)) >= limit:
            break
    return _dedupe(lines)[:limit]


def _pricing_user_material_ref_priority(label: str) -> int:
    lower = str(label or "").strip().lower()
    if "user material" not in lower and not _looks_like_pricing_material_label(lower):
        return 100
    if "market window" in lower:
        return 10
    if "main trim" in lower and ("msrp" in lower or "price" in lower):
        return 20
    if "competitor corridor" in lower or "price corridor" in lower:
        return 30
    if "price gap" in lower:
        return 40
    if "pva" in lower and "coverage" in lower:
        return 50
    if "positioning" in lower:
        return 60
    if "competitor pool" in lower:
        return 70
    return 100


def _pricing_user_material_ref_priority_for_gap(label: str) -> int:
    lower = str(label or "").strip().lower()
    if "user material" not in lower and not _looks_like_pricing_material_label(lower):
        return 100
    if "main trim" in lower and ("msrp" in lower or "price" in lower):
        return 10
    if "competitor corridor" in lower or "price corridor" in lower:
        return 20
    if "price gap" in lower:
        return 30
    if "pva" in lower and "coverage" in lower:
        return 40
    if "competitor pool" in lower:
        return 50
    if "market window" in lower:
        return 60
    if "positioning" in lower:
        return 70
    return 100


def _looks_like_pricing_material_label(label: str) -> bool:
    lower = str(label or "").strip().lower()
    if not lower:
        return False
    return any(
        token in lower
        for token in (
            "main trim msrp",
            "main trim price",
            "competitor corridor",
            "price corridor",
            "high-low trim price gap",
            "trim price gap",
            "price gap",
            "pva coverage",
            "market window",
            "positioning",
            "competitor pool",
        )
    )


def _pricing_user_material_ref_matches_scope(ref: dict[str, Any], evidence_package: dict[str, Any]) -> bool:
    """Keep user material tied to its country/model scope instead of letting it become a global template."""
    label = str(ref.get("label") or "")
    source = str(ref.get("source") or ref.get("table") or "")
    value = str(ref.get("value") or "")
    country = _canonical_market_country(str(evidence_package.get("country") or ""))
    explicit_country = _canonical_market_country(str(ref.get("country") or ""))
    if explicit_country and country and explicit_country != country:
        return False
    country_hint = _pricing_user_material_country_hint(" ".join([label, source, value]))
    if not country_hint:
        country_hint = _pricing_user_material_group_country_hint(ref, evidence_package)
    if country_hint and country and country_hint != country:
        return False
    requested_names = _requested_entity_names_from_package(evidence_package)
    if not requested_names:
        return True
    entity_ids = [str(item or "").strip() for item in ref.get("entityIds", []) if str(item or "").strip()] if isinstance(ref.get("entityIds"), list) else []
    if entity_ids:
        return any(
            _model_keys_match(_model_key(entity), _model_key(requested))
            for entity in entity_ids
            for requested in requested_names
        )
    material_key = _model_key(" ".join([label, source]))
    requested_keys = [_model_key(name) for name in requested_names if _model_key(name)]
    if not material_key:
        return True
    entity_matches = any(
        requested_key in material_key
        or material_key in requested_key
        or _model_keys_match(material_key, requested_key)
        for requested_key in requested_keys
    )
    if not entity_matches:
        return False
    if country_hint or explicit_country:
        return True
    return _legacy_material_source_matches_registered_method(source, country, requested_names)


def _pricing_user_material_group_country_hint(ref: dict[str, Any], evidence_package: dict[str, Any]) -> str:
    ref_id = str(ref.get("refId") or "").strip()
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict) or str(tool.get("toolName") or "") != "business_method_material":
            continue
        tool_refs = _coerce_evidence_refs(tool.get("evidenceRefs"))
        if ref_id and not any(str(item.get("refId") or "").strip() == ref_id for item in tool_refs):
            continue
        values = [str(tool.get("summary") or ""), *[str(item or "") for item in tool.get("keyFindings", []) if item]]
        for item in tool_refs:
            values.extend([str(item.get("label") or ""), str(item.get("value") or ""), str(item.get("source") or "")])
        return _pricing_user_material_country_hint(" ".join(values))
    return ""


def _legacy_material_source_matches_registered_method(
    source: str,
    country: str,
    requested_names: list[str],
) -> bool:
    source_key = _model_key(source)
    for requested in requested_names:
        method = get_active_pricing_method(country=country, model=requested, question="")
        if not method:
            continue
        registered_sources = (
            str(method.get("sourceName") or ""),
            str(method.get("deckTitle") or ""),
        )
        method_model_key = _model_key(str(method.get("model") or ""))
        if method_model_key and method_model_key in source_key:
            return True
        if any(
            registered and (
                _model_key(registered) == source_key
                or _model_key(registered) in source_key
                or source_key in _model_key(registered)
            )
            for registered in registered_sources
        ):
            return True
    return False


def _pricing_user_material_country_hint(value: str) -> str:
    text = str(value or "").casefold()
    negative_sweden_markers = ("不要回答瑞典", "不是瑞典", "非瑞典", "not sweden", "not about sweden", "do not answer sweden")
    if any(marker in text for marker in negative_sweden_markers):
        text = text.replace("瑞典", "").replace("sweden", "")
    for token, country in {
        "sweden": "Sweden",
        "瑞典": "Sweden",
        "finland": "Finland",
        "芬兰": "Finland",
        "norway": "Norway",
        "挪威": "Norway",
        "denmark": "Denmark",
        "丹麦": "Denmark",
        "hungary": "Hungary",
        "匈牙利": "Hungary",
        "germany": "Germany",
        "德国": "Germany",
    }.items():
        if token in text:
            return country
    return ""


def _pricing_requested_model_price_gap(evidence_package: dict[str, Any]) -> bool:
    missing_names = _missing_evidence_names(evidence_package)
    return bool(
        missing_names
        & {
            "coverage_diagnostic:no_current_prices_for_requested_models",
            "current_msrp",
            "current_official_msrp_cross_check",
            "own_model_price",
        }
    )


def _evidence_ref_digest_line(ref: dict[str, Any]) -> str:
    if _evidence_ref_is_zero_volume(ref):
        return ""
    label = _public_scoped_evidence_ref_label(ref)
    if not label:
        return ""
    value = _format_evidence_ref_value(ref)
    source = _public_evidence_ref_source(ref)
    line = f"{label} = {value}" if value else label
    if source:
        line = f"{line}（{source}）"
    return _clean_business_text(line)


def _public_scoped_evidence_ref_label(ref: dict[str, Any]) -> str:
    label = _public_evidence_ref_label(ref)
    if not label:
        return ""
    period_label = _normalize_space(str(ref.get("periodLabel") or ""))
    if not period_label or period_label == "时间范围未标注":
        return label
    return f"{period_label} {label}"


def _external_citation_digest_lines(
    evidence_package: dict[str, Any],
    *,
    limit: int,
) -> list[str]:
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    lines: list[str] = []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        if str(tool.get("toolName") or "") not in {"external_research", "search_market_news", "read_web_page", "browser_snapshot", "pageindex_search_documents"}:
            continue
        groups = _external_ref_groups(_coerce_evidence_refs(tool.get("evidenceRefs")))
        for group in groups:
            title = group.get("title", "")
            if not title:
                continue
            citation = group.get("citation", "")
            domain = group.get("domain", "")
            published = group.get("publishedAt", "")
            meta = "，".join(item for item in (domain, published) if item)
            line = f"{citation} {title}" if citation else title
            if meta:
                line = f"{line}（{meta}）"
            lines.append(_clean_business_text(line))
            if len(_dedupe(lines)) >= limit:
                return _dedupe(lines)[:limit]
    return _dedupe(lines)[:limit]


def _external_ref_groups(refs: list[Any]) -> list[dict[str, str]]:
    groups: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        label = str(ref.get("label") or "").strip()
        if not label:
            continue
        base, suffix = _split_external_ref_label(label)
        if not base:
            continue
        if base not in groups:
            groups[base] = {"title": base}
            order.append(base)
        group = groups[base]
        raw_value = ref.get("value")
        value = "" if raw_value is None else _normalize_space(str(raw_value))
        source = _normalize_space(str(ref.get("source") or ref.get("table") or ""))
        if suffix == "source":
            group["url"] = value if value.startswith(("http://", "https://")) else source
        elif suffix == "date":
            group["publishedAt"] = value[:10]
        elif suffix == "rank":
            rank = re.sub(r"[^0-9]+", "", value)
            if rank:
                group["citation"] = f"[R{rank}]"
        elif suffix == "claim":
            group["claim"] = value
            if source.startswith(("http://", "https://")) and not group.get("url"):
                group["url"] = source
    result: list[dict[str, str]] = []
    for base in order:
        group = groups[base]
        url = group.get("url", "")
        group["domain"] = _domain_from_url(url)
        if not group.get("citation"):
            group["citation"] = f"[R{len(result) + 1}]"
        if _external_group_is_displayable(group):
            result.append(group)
    return result


def _split_external_ref_label(label: str) -> tuple[str, str]:
    for suffix in ("source", "claim", "date", "rank", "rankSeed"):
        marker = f".{suffix}"
        if label.endswith(marker):
            return label[: -len(marker)].strip(), suffix
    return "", ""


def _domain_from_url(url: str) -> str:
    match = re.search(r"https?://([^/]+)", str(url or ""))
    return match.group(1).removeprefix("www.") if match else ""


def _external_group_is_displayable(group: dict[str, str]) -> bool:
    title = str(group.get("title") or "").strip()
    if not title:
        return False
    lowered = title.casefold()
    if lowered in {"row_count", "rank", "rankseed"}:
        return False
    return bool(group.get("url") or group.get("claim") or group.get("domain"))


def _evidence_digest_sentence(
    evidence_package: dict[str, Any],
    intent: str = "",
    *,
    limit: int = 4,
) -> str:
    lines = _evidence_digest_lines(evidence_package, intent, limit=limit)
    return "；".join(lines)


def _prioritized_evidence_refs(evidence_package: dict[str, Any], intent: str = "") -> list[dict[str, Any]]:
    refs = _all_evidence_refs(evidence_package)
    if not refs:
        return []
    tokens = _INTENT_EVIDENCE_TOKENS.get(str(intent or ""), ())

    def score(ref: dict[str, Any], index: int) -> tuple[int, int]:
        haystack = " ".join(
            str(ref.get(key) or "").lower()
            for key in ("label", "source", "table", "unit")
        )
        value = ref.get("value")
        priority = 0
        if tokens and any(token in haystack for token in tokens):
            priority += 8
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            priority += 4
        elif str(value or "").strip():
            priority += 2
        if _looks_like_business_ref(ref):
            priority += 3
        if _looks_like_technical_ref(ref):
            priority -= 6
        return (priority, -index)

    return [
        ref
        for _, ref in sorted(
            ((score(ref, index), ref) for index, ref in enumerate(refs)),
            key=lambda item: item[0],
            reverse=True,
        )
    ]


def _looks_like_business_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    if _evidence_ref_is_non_business_metadata(ref):
        return False
    return any(
        token in label
        for token in (
            "msrp",
            "price",
            "share",
            "sales",
            "volume",
            "segment",
            "model",
            "competitor",
            "battery",
            "feature",
            "trim",
            "material",
            "policy",
            "pva",
        )
    )


def _looks_like_technical_ref(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").lower()
    return _evidence_ref_is_non_business_metadata(ref) or any(
        token in label
        for token in (
            "totalrows",
            "row count",
            "row_count",
            "debug",
            "raw",
            "trace",
            "latency",
            "materializationreadinessscore",
            "materializationstatus",
            "priceevidencerole",
            "candidatesourcetype",
            "candidatedomain",
            "priceevidencestatus",
            "currentpricerows",
            "sourcedraftpath",
            "relativepath",
            "dryruncommand",
        )
    )


def _evidence_ref_is_non_business_metadata(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "")
    normalized = re.sub(r"[^a-z0-9]+", "", label.casefold())
    if normalized in {"rowcount", "count"}:
        return True
    return normalized.endswith(
        (
            "rowcount",
            "totalrows",
            "materializationreadinessscore",
            "materializationstatus",
            "priceevidencerole",
            "priceevidencestatus",
            "candidatesourcetype",
            "candidatedomain",
            "currentpricerows",
            "sourcedraftpath",
            "relativepath",
            "dryruncommand",
            "sourceurl",
        )
    )


def _public_evidence_ref_label_is_internal_status(label: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "", str(label or "").casefold())
    return _evidence_ref_is_non_business_metadata({"label": label}) or normalized.endswith(
        (
            "materializationreadinessscore",
            "materializationstatus",
            "priceevidencerole",
            "priceevidencestatus",
            "candidatesourcetype",
            "sourcedraftpath",
            "relativepath",
            "dryruncommand",
        )
    )


def _public_evidence_ref_label(ref: dict[str, Any]) -> str:
    label = _normalize_space(str(ref.get("label") or ""))
    if _public_evidence_ref_label_is_internal_status(label):
        return ""
    context_label = _public_context_evidence_ref_label(label)
    if context_label:
        return context_label
    material_label = _public_user_material_evidence_ref_label(label)
    if material_label:
        return material_label
    inventory_label = _public_inventory_evidence_ref_label(label)
    if inventory_label:
        return inventory_label
    if label.startswith("contextSnapshot."):
        context_label = _public_market_evidence_ref_label(ref)
        if context_label:
            return context_label
    model_metric_match = re.match(
        r"(?<![A-Za-z0-9_.-])([A-Za-z0-9][A-Za-z0-9 _/-]{0,60})\.(sales|segment|powertrain|seats|avgPrice|msrp|price|battery_kWh|WLTP_range_km)$",
        label,
        flags=re.IGNORECASE,
    )
    if model_metric_match:
        model_name = _normalize_space(model_metric_match.group(1).replace("_", " "))
        metric = model_metric_match.group(2).lower()
        metric_labels = {
            "sales": "销量",
            "segment": "级别",
            "powertrain": "动力",
            "seats": "座位",
            "avgprice": "平均价格",
            "msrp": "MSRP",
            "price": "价格",
            "battery_kwh": "电池容量",
            "wltp_range_km": "WLTP 续航",
        }
        metric_label = metric_labels.get(metric, metric)
        if model_name:
            return f"{model_name} {metric_label}"
    premium_msrp_match = re.match(r"^(.+?)\s+Premium MSRP$", label, flags=re.IGNORECASE)
    if premium_msrp_match:
        subject = _normalize_space(premium_msrp_match.group(1).replace("_", " "))
        if subject:
            return f"{subject} 高配 MSRP"
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
        "competitor corridor": "竞品价格走廊",
    }
    return replacements.get(label, label)


def _public_user_material_evidence_ref_label(label: str) -> str:
    text = str(label or "").strip()
    match = re.match(r"^(.+?)\s+user material\s+(.+)$", text, flags=re.IGNORECASE)
    if match:
        subject = _normalize_space(match.group(1).replace("_", " "))
        field = _normalize_space(match.group(2).replace("_", " ")).lower()
    else:
        feature_match = re.match(r"^(.+?)\s+visible feature value\.(.+)$", text, flags=re.IGNORECASE)
        if feature_match:
            subject = _normalize_space(feature_match.group(1).replace("_", " "))
            feature = _normalize_space(feature_match.group(2).replace("_", " "))
            return f"{subject} 可感知配置价值：{feature}" if subject and feature else ""
        direct_match = re.match(
            r"^(.+?)\s+(main trim msrp|main trim price|competitor corridor|price corridor|high-low trim price gap|trim price gap|price gap|pva coverage|market window|positioning|competitor pool)$",
            text,
            flags=re.IGNORECASE,
        )
        if not direct_match:
            return ""
        subject = _normalize_space(direct_match.group(1).replace("_", " "))
        field = _normalize_space(direct_match.group(2).replace("_", " ")).lower()
    if not subject or not field:
        return ""
    field_label = {
        "market window": "市场窗口",
        "main trim msrp": "主销高配价格",
        "main trim price": "主销高配价格",
        "competitor corridor": "竞品价格带",
        "price corridor": "竞品价格带",
        "high-low trim price gap": "高低配价差",
        "trim price gap": "高低配价差",
        "price gap": "高低配价差",
        "pva coverage": "高配 PVA 覆盖率",
        "positioning": "定价定位",
        "competitor pool": "竞品池",
    }.get(field)
    if not subject or not field_label:
        return ""
    return f"{subject} {field_label}"


def _public_inventory_evidence_ref_label(label: str) -> str:
    text = _normalize_space(str(label or ""))
    if not text:
        return ""
    match = re.match(
        r"^(?:inventory|material|bom|stock|order)\.records\.([^.]+)\.([^.]+)$",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        model = _normalize_space(match.group(1).replace("_", " "))
        metric = match.group(2).replace("_", "").casefold()
        metric_label = {
            "market": "市场",
            "country": "市场",
            "model": "车型",
            "version": "业务版本",
            "trim": "业务版本",
            "variant": "业务版本",
            "materialcode": "物料号",
            "sku": "物料号",
            "partnumber": "物料号",
            "availableunits": "可用数量",
            "units": "可用数量",
            "stock": "库存数量",
            "quantity": "数量",
            "qty": "数量",
            "lifecycle": "生命周期",
            "status": "状态",
            "risk": "风险",
            "exterior": "外饰",
            "interior": "内饰",
            "color": "颜色",
            "colour": "颜色",
            "colorspec": "颜色组合",
        }.get(metric)
        if model and metric_label:
            return f"{model} {metric_label}"
    if text.casefold() == "bom material lifecycle risk":
        return "BOM 物料生命周期风险"
    return ""


def _format_evidence_ref_value(ref: dict[str, Any]) -> str:
    value = ref.get("value")
    if value is None or isinstance(value, bool):
        return ""
    unit = str(ref.get("unit") or "").strip()
    if isinstance(value, (int, float)):
        value_text = f"{value:,.0f}" if float(value).is_integer() else f"{value:,.1f}"
    else:
        value_text = _format_simple_evidence_value_text(_normalize_space(str(value)), unit)
    if not value_text:
        return ""
    if not unit or unit.lower() in {"value", "currency"}:
        return value_text
    if unit == "%":
        return value_text if value_text.endswith("%") else f"{value_text}%"
    if unit.lower() == "units":
        return value_text if value_text.lower().endswith("units") else f"{value_text} units"
    if value_text.lower().endswith(unit.lower()):
        return value_text
    return f"{value_text} {unit}"


def _format_simple_evidence_value_text(value_text: str, unit: str = "") -> str:
    if not value_text:
        return ""
    match = re.fullmatch(
        r"(-?\d+(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?)(?:\s*([A-Za-z%][A-Za-z/%.-]*))?",
        value_text,
    )
    if not match:
        return value_text
    numeric_text = match.group(1)
    explicit_unit = match.group(2) or ""
    should_format = bool(unit or explicit_unit)
    try:
        numeric_value = float(numeric_text.replace(",", ""))
    except ValueError:
        return value_text
    if not should_format and abs(numeric_value) < 10000:
        return value_text
    formatted = f"{numeric_value:,.0f}" if numeric_value.is_integer() else f"{numeric_value:,.1f}"
    if explicit_unit == "%":
        return f"{formatted}%"
    if explicit_unit:
        return f"{formatted} {explicit_unit}"
    return formatted


def _market_cross_tab_ref_value(
    evidence_package: dict[str, Any],
    *,
    table: str,
    row: str,
    metric: str,
) -> str:
    ref = _market_cross_tab_ref(evidence_package, table=table, row=row, metric=metric)
    if not ref:
        return ""
    return (
        _format_period_scoped_ref_value(ref)
        if _market_metric_uses_absolute_period(metric)
        else _format_evidence_ref_value(ref)
    )


def _market_cross_tab_positive_ref_value(
    evidence_package: dict[str, Any],
    *,
    table: str,
    row: str,
    metric: str,
) -> str:
    ref = _market_cross_tab_ref(evidence_package, table=table, row=row, metric=metric)
    if not ref:
        return ""
    value = _numeric_ref_value(ref)
    if value is None or value <= 0:
        return ""
    return (
        _format_period_scoped_ref_value(ref)
        if _market_metric_uses_absolute_period(metric)
        else _format_evidence_ref_value(ref)
    )


def _market_metric_uses_absolute_period(metric: str) -> bool:
    return str(metric or "").strip().casefold() in {
        "sales",
        "value",
        "volume",
        "count",
        "registrations",
        "registration",
    }


def _format_period_scoped_ref_value(ref: dict[str, Any]) -> str:
    value = _format_evidence_ref_value(ref)
    if not value:
        return ""
    period_label = _normalize_space(str(ref.get("periodLabel") or ""))
    if not period_label or period_label == "时间范围未标注":
        return value
    return f"{value}（{period_label}）"


def _market_cross_tab_ref(
    evidence_package: dict[str, Any],
    *,
    table: str,
    row: str,
    metric: str,
) -> dict[str, Any] | None:
    table_token = str(table or "").strip().casefold()
    row_token = str(row or "").strip().casefold()
    metric_token = str(metric or "").strip().casefold()
    if not table_token or not row_token or not metric_token:
        return None
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip().casefold()
        parts = [part.strip() for part in label.split(".") if part.strip()]
        if len(parts) < 3:
            continue
        if table_token not in label:
            continue
        if parts[-2] != row_token or parts[-1] != metric_token:
            continue
        if _format_evidence_ref_value(ref):
            return ref
    return None


def _public_evidence_ref_source(ref: dict[str, Any]) -> str:
    source = _normalize_space(str(ref.get("source") or ref.get("table") or ""))
    if not source:
        return ""
    source_key = source.casefold()
    if (
        _evidence_ref_is_user_material(ref)
        or str(ref.get("sourceType") or "").strip().casefold() == "user_material"
        or source == "business_method_material"
        or "method_fallback" in source_key
        or source_key.endswith(".pptx")
    ):
        return "用户材料"
    if "source_drafts/" in source_key or source_key.startswith("source_drafts"):
        return "来源草稿"
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
    if source in source_labels:
        return source_labels[source]
    if len(source) > 42:
        return ""
    return source


def _evidence_quality_suffix(evidence_package: dict[str, Any], intent: str = "") -> str:
    refs = _intent_relevant_evidence_ref_count(evidence_package, intent)
    confidence = _confidence_label(str(evidence_package.get("confidence") or "low"))
    if refs <= 0:
        return f"{_intent_missing_evidence_note(intent)}。"
    return f"当前有 {_intent_evidence_count_note(refs, intent)}，置信度{confidence}。"


def _intent_relevant_evidence_ref_count(evidence_package: dict[str, Any], intent: str = "") -> int:
    if str(intent or "") != "voc_analysis":
        return evidence_ref_count(evidence_package)
    external_count = len(_external_citation_digest_lines(evidence_package, limit=20))
    voc_ref_count = len([ref for ref in _all_evidence_refs(evidence_package) if _is_voc_evidence_ref(ref)])
    return external_count + voc_ref_count


def _intent_evidence_count_note(refs: int, intent: str = "") -> str:
    if str(intent or "") == "voc_analysis":
        if refs <= 0:
            return "当前缺少可追溯 VOC 来源"
        return f"{refs} 条可追溯 VOC/外部来源证据"
    return _evidence_count_note(refs)


def _intent_evidence_availability_note(refs: int, intent: str = "") -> str:
    if refs > 0:
        return f"当前有 {_intent_evidence_count_note(refs, intent)}"
    return _intent_missing_evidence_note(intent)


def _intent_missing_evidence_note(intent: str = "") -> str:
    if str(intent or "") == "voc_analysis":
        return "当前缺少可追溯 VOC 来源"
    return "当前缺少可引用证据"


def _display_plan_for_intent(
    intent: str,
    evidence_package: dict[str, Any],
    *,
    question: str = "",
) -> str:
    source_repair = _source_repair_candidates_from_evidence_package(evidence_package)
    if (
        str(intent or "") == "pricing_analysis"
        and _is_leasing_tco_source_context(question=question, evidence_package=evidence_package)
    ):
        return (
            "优先看 TCO/company-car 验证表，把月供、残值、税费/benefit formula、年里程、充电条件和 BEV/PHEV/HEV "
            "同假设对比放在一张表里；JATO 渠道/市场数据只作为背景，不替代 TCO 证据。"
        )
    if str(intent or "") == "voc_analysis" and (
        _is_external_query_source_repair(source_repair)
        or _has_missing_evidence(evidence_package, "external_research_claims_unavailable")
        or _has_missing_evidence(evidence_package, "minimum_external_sources")
        or _has_missing_evidence(evidence_package, "consumer_signal")
    ):
        return (
            "先看外部来源验证矩阵，确认 VOC/媒体/论坛来源、发布日期和原文要点；"
            "再看 VOC 主题表，把已验证来源转成用户痛点、可转化卖点和产品动作。"
        )
    if str(intent or "") == "news_policy_search" and (
        _is_policy_source_repair(source_repair)
        or _has_target_policy_source_gap(evidence_package)
        or _has_missing_evidence(evidence_package, "minimum_external_sources")
        or _has_missing_evidence(evidence_package, "official_source")
    ):
        return (
            "先看外部来源验证矩阵，确认官方/可引用来源、发布日期、适用对象和政策限制；"
            "再看政策证据表和 report block 输出车型、价格和渠道动作。"
        )
    if evidence_ref_count(evidence_package) <= 0:
        return ""
    if str(intent or "") == "pricing_analysis":
        return _pricing_display_plan_for_evidence(evidence_package, source_repair)
    if str(intent or "") == "market_overview":
        return _market_visual_backbone_note(evidence_package)
    if str(intent or "") == "competitor_compare":
        return _competitor_display_plan_for_evidence(evidence_package, source_repair)

    mapping = {
        "configuration_analysis": "用配置差异表展示 trim、must-have、visible value、cost/risk，再把主销配置建议压成 report block。",
        "inventory_analysis": "用 BOM/库存关系表展示车型版本、物料号、颜色、市场、订单和生命周期异常。",
        "news_policy_search": "用来源表展示政策/新闻日期、适用对象和影响路径，再用 report block 输出车型/价格/渠道动作。",
        "voc_analysis": "用来源表和主题表展示 VOC 来源、用户痛点、频次/可信度和可转化卖点。",
        "report_generation": "用 report block 输出可复制的一页 PPT 结构，并附证据表或图表作为 appendix。",
    }
    return mapping.get(str(intent or ""), "用证据表展示已查数据，再把结论压成可复用 report block。")


def _pricing_display_plan_for_evidence(evidence_package: dict[str, Any], source_repair: dict[str, Any]) -> str:
    has_source_gap = _has_msrp_source_repair_gap(evidence_package) or bool(source_repair)
    parts: list[str] = []
    if _pricing_has_reference_sample_stats(evidence_package) and not _pricing_has_explicit_corridor_ref(evidence_package):
        parts.append(
            "先看 Pricing reference sample chart 判断目标价/本车型 MSRP 相对参考样本的位置；"
            "再看 Pricing evidence table 拆本车型 MSRP、目标价、参考样本、月供/RV 和配置价值边界；"
            "不能把参考样本当作核心竞品走廊。"
        )
    elif _pricing_has_explicit_corridor_ref(evidence_package):
        parts.append(
            "先看 Pricing corridor chart 判断目标价或本车型价格在竞品走廊中的位置；"
            "再看 Pricing evidence table 拆本车型 MSRP、目标价、竞品走廊、月供/RV、PVA 和配置价值边界。"
        )
    else:
        parts.append(
            "先看 Pricing evidence table，把本车型 MSRP、目标价、待补竞品价格、月供/RV 缺口和配置价值边界放在同一张表里。"
        )
    if has_source_gap:
        parts.append(
            "价格来源缺口先看 MSRP source validation table，确认官方 URL、版本/trim、币种和发布日期后，再把数值写成确定 MSRP 或竞品价格结论。"
        )
    return " ".join(parts)


def _competitor_display_plan_for_evidence(evidence_package: dict[str, Any], source_repair: dict[str, Any]) -> str:
    metric = _competitor_chart_metric(evidence_package)
    has_source_gap = _has_msrp_source_repair_gap(evidence_package) or bool(source_repair)
    parts: list[str] = []
    if metric:
        parts.append(
            f"先看 Competitor {metric} chart 判断已查竞品量级；"
            "再看 Competitor comparison table 拆对标角色、级别、动力、价格/配置差异、可赢点、短板和产品动作。"
        )
    elif has_source_gap:
        parts.append(
            "先看 MSRP source validation table 补齐本车型/竞品官方价格来源；"
            "再看 Competitor comparison table 拆对标角色、级别、动力、价格/配置差异和产品动作。"
        )
    else:
        parts.append(
            "先看 Competitor comparison table，把已查竞品锚点、级别、动力、价格/配置差异、可赢点、短板和产品动作放在同一张表里。"
        )
    if has_source_gap and metric and "MSRP source validation table" not in parts[0]:
        parts.append("价格/版本来源缺口另看 MSRP source validation table，确认官方 URL、trim、币种和发布日期。")
    return " ".join(parts)


def _first_non_empty(values: list[str]) -> str:
    for item in values:
        text = str(item or "").strip()
        if text:
            return text
    return ""


def _ordered_existing_guidance_bullets(existing: list[str]) -> list[str]:
    prefixes = (
        "当前能判断",
        "缺少证据",
        "证据需求",
        "下一步动作",
        "建议查数动作",
        "建议输出形态",
        "建议调用工具",
        "下一步建议",
        "下一步补齐",
        "可先输出",
        "影响范围",
        "在补数前",
    )
    result: list[str] = []
    seen: set[str] = set()
    for prefix in prefixes:
        for item in existing:
            if item in seen or not item.startswith(prefix):
                continue
            seen.add(item)
            result.append(item)
            break
    return result


def _should_prioritize_existing_guidance(answer: dict[str, Any], evidence_package: dict[str, Any]) -> bool:
    if str(answer.get("answerStatus") or answer.get("status") or "").strip() == "insufficient_evidence":
        return True
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    return any(isinstance(item, dict) and item.get("impact") == "blocking" for item in missing)


def _risk_summary(plan: BusinessSynthesisPlan) -> str:
    if not plan["risksAndMissingEvidence"]:
        return "结论仍需随最新价格、政策、配置和库存证据更新。"
    risk = plan["risksAndMissingEvidence"][0]
    return f"{_risk_display_name(risk['name'])}：{_risk_display_impact(risk)}；建议：{risk['mitigation']}"


def _compose_business_limitations(
    answer: dict[str, Any],
    plan: BusinessSynthesisPlan,
    evidence_package: dict[str, Any],
) -> list[str]:
    existing = [
        item
        for item in _string_list(answer.get("limitations"))
        if not _limitation_conflicts_with_time_series_scope(item, evidence_package)
    ]
    generated = [
        f"证据对齐：{_alignment_label(plan['evidenceAlignment']['status'])}。",
    ]
    for risk in plan["risksAndMissingEvidence"][:3]:
        generated.append(f"风险边界：{_risk_display_name(risk['name'])}：{_risk_display_impact(risk)}；建议：{risk['mitigation']}")
    return _dedupe([_clean_business_text(item) for item in [*existing, *generated] if str(item or "").strip()])[:10]


def _limitation_conflicts_with_time_series_scope(
    limitation: str,
    evidence_package: dict[str, Any],
) -> bool:
    text = str(limitation or "").casefold()
    if not any(token in text for token in ("月度", "趋势", "monthly", "trend")):
        return False
    denial_markers = (
        "整体市场",
        "未单独标注",
        "未按动力",
        "没有按动力",
        "未筛选",
        "overall market",
        "not filtered",
        "not separately",
    )
    if not any(marker in text for marker in denial_markers):
        return False
    tools = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tools:
        if not isinstance(tool, dict) or tool.get("toolName") != "query_time_series" or not tool.get("success"):
            continue
        query = tool.get("query") if isinstance(tool.get("query"), dict) else {}
        if not any(str(query.get(key) or "").strip() for key in ("powertrain", "segment")):
            continue
        if any(
            str(ref.get("label") or "").lower().startswith("monthseries.")
            for ref in _coerce_evidence_refs(tool.get("evidenceRefs"))
        ):
            return True
    return False


def _normalize_intent(intent: str) -> str:
    value = str(intent or "").strip()
    aliases = {
        "policy": "news_policy_search",
        "policy_analysis": "news_policy_search",
        "inventory_bom": "inventory_analysis",
        "bom": "inventory_analysis",
        "configuration": "configuration_analysis",
        "compare": "competitor_compare",
    }
    return aliases.get(value, value or "general_qa")


def _model_hint(*, evidence_plan: dict[str, Any], evidence_package: dict[str, Any]) -> str:
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    if not entities:
        entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    models = entities.get("models") if isinstance(entities.get("models"), list) else []
    for item in models:
        value = str(item or "").strip()
        if value:
            return value
    return ""


def _all_evidence_refs(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        for ref in _coerce_evidence_refs(tool.get("evidenceRefs")):
            if is_usable_evidence_ref(ref):
                normalized_ref = dict(ref)
                normalized_ref.setdefault("evidenceToolName", str(tool.get("toolName") or ""))
                normalized_ref.setdefault("sourceType", str(tool.get("sourceType") or ""))
                refs.append(normalized_ref)
    return refs


def _policy_bonus_has_ended_evidence(evidence_package: dict[str, Any]) -> bool:
    values: list[str] = []
    for ref in _all_evidence_refs(evidence_package):
        values.extend([str(ref.get("label") or ""), str(ref.get("value") or ""), str(ref.get("source") or "")])
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        values.append(str(tool.get("summary") or ""))
        key_findings = tool.get("keyFindings") if isinstance(tool.get("keyFindings"), list) else []
        values.extend(str(item or "") for item in key_findings)
    text = " ".join(values).casefold()
    if not any(token in text for token in ("bonus", "elbil", "low emission", "låg utsläpp", "lag utslapp")):
        return False
    return any(
        token in text
        for token in (
            "has ended",
            "ended",
            "has expired",
            "expired",
            "avslutad",
            "avslutats",
            "upphört",
            "upphort",
            "inte längre",
            "not available",
        )
    )


def _looks_like_source_repair_action(value: str) -> bool:
    text = _clean_action_text(value).casefold()
    return any(
        token in text
        for token in (
            "候选",
            "补证据入口",
            "重跑 business validation",
            "不能直接当作政策事实",
            "不能直接当作官方价格证据",
            "确认官方来源",
            "official source",
            "official-source confirmation",
            "policy claims",
            "是否仍有效",
            "是否适用",
            "source repair",
            "repair candidate",
        )
    )


def _coerce_evidence_refs(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text or text == "[]":
            return []
        if text.startswith("["):
            try:
                parsed = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                return []
            return parsed if isinstance(parsed, list) else []
    return []


def _normalize_question_text(question: str) -> str:
    return _normalize_space(str(question or "")).lower()


def _display_question_subject(question: str, *, max_chars: int = 120) -> str:
    text = _normalize_space(str(question or ""))
    text = re.sub(r"请先判断国家[，,、\s]*", "", text)
    text = re.sub(r"(?:不要|别)回答[^，,。.!！?？；;]{1,32}[，,。.!！?？；;]?", "", text)
    text = re.sub(r"(?:不要|别)用[^，,。.!！?？；;]{1,32}[，,。.!！?？；;]?", "", text)
    text = re.sub(r"\b(?:do not|don't|dont)\s+(?:answer|use)\s+[^，,。.!！?？；;]{1,32}[，,。.!！?？；;]?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[，,、\s]*(?:请|麻烦)?(?:简短|简单|直接|一句话)(?:回答|回复|说)?[，,、\s。.!！?？；;]*$", "", text)
    text = re.sub(
        r"[，,、\s]*(?:please\s+)?(?:answer\s+)?(?:briefly|shortly|in\s+brief)[。.!！?？；;]*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = _strip_terminal_punctuation(_normalize_space(text))
    text = re.sub(r"[，,、\s]*(?:请|麻烦|谢谢)$", "", text).strip()
    text = _strip_terminal_punctuation(_normalize_space(text))
    return text[:max_chars] if text else "当前问题"


def _clean_external_research_query_text(value: str, *, max_chars: int = 160) -> str:
    text = _display_question_subject(str(value or ""), max_chars=max_chars)
    text = re.sub(
        r"\b(?:do not|don't|dont|don’t)\s+"
        r"(?:answer|use|analyze|analyse|reply with|respond with)\s+"
        r"[^，,。.!！?？；;]{1,48}[，,。.!！?？；;]?",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"[，,、\s]+$", "", _normalize_space(text))
    return _strip_terminal_punctuation(text)[:max_chars]


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token.lower() in text for token in tokens)


def _is_phev_fleet_leasing_question(text: str) -> bool:
    if "phev" not in text:
        return False
    if _contains_any(text, ("co2", "co₂")):
        return False
    if _contains_any(text, ("leasing", "lease", "大客户", "fleet")):
        return True
    return (
        _contains_any(text, ("company car", "公司车"))
        and _contains_any(text, ("理由", "还有没有", "主推"))
        and "bev" not in text
    )


def _looks_like_generic_first_sentence(value: str) -> bool:
    text = str(value or "")
    generic_tokens = (
        "应先验证",
        "不能只",
        "不是只",
        "要先",
        "必须连接",
        "需要把",
    )
    return any(token in text for token in generic_tokens)


def _evidence_summary(evidence_package: dict[str, Any], *, external: bool) -> str:
    source_types = {"web", "policy", "voc"} if external else {"jato_parquet", "postgres", "engineering", "generated"}
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    findings: list[str] = []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        source_type = str(tool.get("sourceType") or "")
        if source_type not in source_types:
            continue
        key_findings = tool.get("keyFindings") if isinstance(tool.get("keyFindings"), list) else []
        summary = str(tool.get("summary") or "").strip()
        if key_findings:
            findings.extend(_public_evidence_finding(str(item)) for item in key_findings[:2] if str(item or "").strip())
        elif summary:
            findings.append(_public_evidence_finding(summary))
    if findings:
        return "；".join(_dedupe(findings)[:3])
    return "暂未形成外部来源证据。" if external else "暂未形成可引用的 JATO/内部结构化证据。"


def _public_evidence_finding(value: str) -> str:
    text = _normalize_space(value)
    lower = text.lower()
    if not text:
        return ""
    if "source/date/count refs" in lower or "no supported claim" in lower or "no citation-ready" in lower:
        return "外部来源只返回来源/日期/数量线索，尚未形成可引用的业务结论。"
    if "external research returned only" in lower:
        return "外部研究结果还需要补充可引用结论。"
    if "jato historical data is not a direct validator" in lower:
        return "JATO 历史数据只能做市场结构交叉验证，不能直接证明外部政策或 VOC 事实。"
    if re.search(r"[a-z_]{4,}", text) and not re.search(r"[\u4e00-\u9fff]", text):
        return "该证据需要转译成业务结论后再用于回答。"
    return text


def _evidence_alignment(
    evidence_package: dict[str, Any],
    internal_summary: str,
    external_summary: str,
) -> EvidenceAlignment:
    intent = str(evidence_package.get("intent") or "")
    if intent == "voc_analysis" and _has_missing_evidence(evidence_package, "external_research_claims_unavailable"):
        return {
            "status": "insufficient",
            "summary": "当前缺少可追溯 VOC 来源；JATO 市场数据只能作为背景，不能证明用户吐槽频次或高频主题。",
            "internalSignal": internal_summary,
            "externalSignal": external_summary,
        }
    cross_check = evidence_package.get("jatoCrossCheck") if isinstance(evidence_package.get("jatoCrossCheck"), dict) else {}
    raw_status = str(cross_check.get("status") or cross_check.get("rawStatus") or "").strip()
    if raw_status in {"matched", "aligned"}:
        status: EvidenceAlignmentStatus = "aligned"
        summary = "JATO 内部证据与外部研究方向一致，可以形成带置信度的业务判断。"
    elif raw_status in {"partially_matched", "partially_aligned", "warning"}:
        status = "partially_aligned"
        summary = "内部和外部证据方向基本一致，但口径、时间或对象不同，需要在结论里保留边界。"
    elif raw_status == "conflicting":
        status = "conflicting"
        summary = "外部来源与 JATO 内部证据存在冲突，不能直接写成确定结论。"
    elif evidence_ref_count(evidence_package) == 0:
        status = "insufficient"
        summary = "当前没有可引用证据，只能给框架、验证路径和下一步动作。"
    else:
        status = "partially_aligned"
        summary = "已有部分证据可用，但内部/外部交叉验证还不完整。"
    explicit = str(cross_check.get("summary") or "").strip()
    if explicit:
        summary = f"{summary} {_public_cross_check_summary(explicit)}"
    return {
        "status": status,
        "summary": summary,
        "internalSignal": internal_summary,
        "externalSignal": external_summary,
    }


def _public_cross_check_summary(value: str) -> str:
    text = _normalize_space(value)
    if not text:
        return ""
    replacements = {
        "No matching JATO structured context was available for this external claim.": "缺少可匹配的 JATO 内部结构化交叉验证。",
        "No lightweight JATO market context was available for this research question.": "轻量市场快照也未返回可用上下文。",
    }
    for raw, public in replacements.items():
        text = text.replace(raw, public)
    if re.search(r"[a-z_]{4,}", text) and not re.search(r"[\u4e00-\u9fff]", text):
        return "内部/外部交叉验证还没有形成可直接引用的业务结论。"
    return text


def _insight_cards(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    cards = evidence_package.get("insightCards") if isinstance(evidence_package.get("insightCards"), list) else []
    return [dict(card) for card in cards if isinstance(card, dict)][:3]


def _business_implications(
    intent: str,
    evidence_package: dict[str, Any],
    alignment: EvidenceAlignment,
    insight_cards: list[dict[str, Any]],
    *,
    method: BusinessMethodDistillation | None = None,
    question: str = "",
) -> list[str]:
    result: list[str] = []
    question_text = _normalize_question_text(question)
    country_label = _country_label(str(evidence_package.get("country") or "当前市场"))
    topic_specific_report = (
        intent == "report_generation"
        and (
            _is_bev_penetration_report(question_text)
            or (method is not None and _question_mentions_method_model(method, question_text))
        )
    )
    bonus_ended_policy = (
        intent == "news_policy_search"
        and _is_bev_subsidy_cap_question(question_text)
        and _policy_bonus_has_ended_evidence(evidence_package)
    )
    if method and intent == "pricing_analysis":
        playbook = method.get("pricingPlaybook") if isinstance(method.get("pricingPlaybook"), dict) else {}
        price_corridor = method.get("priceCorridor") if isinstance(method.get("priceCorridor"), dict) else {}
        version_strategy = method.get("versionStrategy") if isinstance(method.get("versionStrategy"), dict) else {}
        model = str(method.get("model") or "目标车型").strip()
        positioning = str(price_corridor.get("positioning") or "").strip()
        method_lines = [
            (
                f"{model} 方法样例：{positioning}，先用市场窗口和竞品走廊定位，再用配置价值解释高配。"
                if positioning
                else f"{model} 方法样例：先用市场窗口和竞品走廊定位，再用配置价值解释主销版本。"
            ),
            str(playbook.get("competitor_corridor") or "").strip(),
            str(playbook.get("product_value_delta") or "").strip(),
            str(playbook.get("main_trim_strategy") or version_strategy.get("mainTrimStrategy") or "").strip(),
        ]
        price_gap = str(version_strategy.get("priceGap") or price_corridor.get("priceGap") or "").strip()
        pva_coverage = str(version_strategy.get("pvaCoverage") or playbook.get("pva_validation") or "").strip()
        if not method_lines[-1] and (price_gap or pva_coverage):
            method_lines[-1] = (
                f"主销版本策略需要把价差{f' {price_gap}' if price_gap else ''}"
                f"{f'、PVA 覆盖 {pva_coverage}' if pva_coverage else ''}和可感知配置价值连起来验证。"
            )
        result.extend([line for line in method_lines if line])
    if intent == "report_generation" and method and _question_mentions_method_model(method, question_text):
        result.extend([
            f"{method['model']} 报告页的业务含义是低配做价格锚点、高配做主推版本，而不是只展示一个建议价。",
            "一页汇报应把市场窗口、竞品走廊、配置价值和 PVA 覆盖率连成销售可用的话术链。",
        ])
    if intent == "report_generation" and _is_bev_penetration_report(question_text):
        result.extend([
            "BEV 渗透率变化要先转成产品定义检查项：续航、充电、冬季包、价格门槛、公司车和渠道优先级。",
            "正式页不能只放单点渗透率，必须补趋势、细分市场、政策/价格/供给驱动后再写确定产品结论。",
        ])
    if bonus_ended_policy:
        target_model = _policy_target_model(evidence_package, question_text)
        result.extend([
            f"官方 bonus 已结束后，{target_model} 定价不应围绕补贴门槛倒推，而应回到竞品价格走廊、配置价值和月供/TCO。",
            "历史补贴上限仍可作为价格敏感锚点和政策监控项，但不能为了卡历史门槛牺牲主销配置或毛利。",
            f"下一步应把 {target_model} MSRP、同价带 BEV SUV 竞品价格和 24/36 个月月供放进同一张定价矩阵。",
        ])
    else:
        result.extend(_question_specific_business_implications(
            intent,
            question_text,
            country_label=country_label,
            evidence_package=evidence_package,
            method=method,
        ))
    for card in insight_cards:
        implication = str(card.get("implication") or "").strip()
        claim = str(card.get("claim") or "").strip()
        if bonus_ended_policy and _looks_like_source_repair_action(implication or claim):
            continue
        if implication:
            result.append(implication)
        elif claim:
            result.append(claim)
    if alignment["status"] == "conflicting":
        result.append("先把冲突证据拆成内部口径、外部口径和时间口径，业务动作只能进入人工复核。")
    elif alignment["status"] == "insufficient":
        result.append("当前不能输出确定数字，但可以先确定分析框架、竞品池、关键假设和补数优先级。")
    defaults = {
        "pricing_analysis": [
            "定价判断不能套用单一车型模板，应先验证目标车型所属价格走廊、竞品池、配置价值和购买场景。",
            "定价不能只看 MSRP，应同时验证竞品走廊、配置价值、月供/company car、残值和促销空间。",
            "若缺少最新价格证据，第一版建议先输出价格矩阵模板和验证路径，而不是直接报价格。",
        ],
        "market_overview": [
            "市场数据要落到机会 segment 和动力路线选择，不能只复述销量或份额。",
            "对目标产品组合的价值在于识别优先进入的级别、动力和竞品锚点。",
        ],
        "news_policy_search": [
            "政策/新闻影响必须拆到车型、价格门槛、动力类型、零售与公司车场景。",
            "来源日期或官方来源不足时，只能给影响路径和查证动作。",
        ],
        "competitor_compare": [
            "竞品对比先定义对标关系，再判断正面对抗、错位竞争或价格锚点。",
            "结论要能转成配置、价格、销售话术和报告页。",
        ],
        "configuration_analysis": [
            "配置结论必须连接用户场景，例如冬季、拖车、充电、家庭出行和公司车使用。",
            "缺工程配置时先输出配置验证清单和主销配置假设。",
        ],
        "inventory_analysis": [
            "库存/BOM 问题应先把车型版本、物料号、颜色、市场、PI 和订单生命周期分层。",
            "如果没有底表证据，仍可以先定义实体关系和异常处理规则。",
        ],
        "report_generation": [
            "汇报生成要先压出 key message，再用 evidence、product implication 和 next action 支撑，而不是把长答案搬进 PPT。",
            "缺数据时应把缺口写成验证项，保留可复制的一页汇报结构。",
        ],
        "voc_analysis": [
            "VOC 结论要先区分真实用户痛点、媒体观点、论坛噪音和可转化卖点。",
            "没有可追溯来源时不能说“高频吐槽”，但可以先给主题假设、检索路径和产品含义。",
        ],
    }
    if not topic_specific_report and not bonus_ended_policy:
        result.extend(defaults.get(intent, ["把证据转成业务动作、风险边界和可复用汇报结构。"]))
    tools = evidence_tool_names(evidence_package)
    if tools and bonus_ended_policy:
        tool_labels = _dedupe([_tool_business_label(item) for item in tools])[:4]
        target_model = _policy_target_model(evidence_package, question_text)
        result.append(f"本轮工具链已经覆盖 {'、'.join(tool_labels)}，下一步应补 {target_model} 价格、竞品走廊和月供表。")
    elif tools:
        tool_labels = _dedupe([_tool_business_label(item) for item in tools])[:4]
        result.append(f"本轮工具链已经覆盖 {'、'.join(tool_labels)}，下一步应补齐缺失证据后再收敛结论。")
    return _dedupe(result)[:6]


def _recommended_actions(
    intent: str,
    evidence_package: dict[str, Any],
    insight_cards: list[dict[str, Any]],
    *,
    method: BusinessMethodDistillation | None = None,
    question: str = "",
) -> list[RecommendedAction]:
    refs = [str(ref.get("refId") or "") for ref in _all_evidence_refs(evidence_package) if str(ref.get("refId") or "").strip()]
    result: list[RecommendedAction] = []
    result.extend(_missing_required_tool_actions(evidence_package, refs))
    question_text = _normalize_question_text(question)
    source_repair_action = _source_repair_recommended_action(
        evidence_package,
        refs,
        question=question_text,
        method=method,
    )
    if source_repair_action:
        result.append(source_repair_action)
    topic_specific_report = (
        intent == "report_generation"
        and (
            _is_bev_penetration_report(question_text)
            or (method is not None and _question_mentions_method_model(method, question_text))
        )
    )
    if intent == "report_generation" and _is_bev_penetration_report(question_text):
        result.extend([
            {
                "action": "补齐 BEV 年/月度渗透率、SUV A0/A 细分和政策/价格/供给驱动证据",
                "rationale": "BEV 产品定义页需要先证明变化来自趋势、细分结构还是外部驱动，不能只靠单点图表或模板。",
                "priority": "P0",
                "evidenceRefs": refs[:3],
                "citationIds": [],
            },
            {
                "action": "把 BEV 趋势转成续航、充电、冬季包、价格门槛和公司车场景的产品定义建议",
                "rationale": "用户要的是产品定义建议页，下一步应把证据直接落到配置和价格动作。",
                "priority": "P1",
                "evidenceRefs": refs[:3],
                "citationIds": [],
            },
        ])
    if intent == "report_generation" and method and _question_mentions_method_model(method, question_text):
        model = str(method.get("model") or "目标车型").strip()
        result.extend([
            {
                "action": f"把 {model} 一页汇报写成市场窗口、竞品走廊、配置价值、低配锚点和高配主推",
                "rationale": f"该动作来自已蒸馏的 {model} pricing method，能把用户材料转成可复制 PPT 结构。",
                "priority": "P0",
                "evidenceRefs": refs[:3],
                "citationIds": [],
            },
        ])
    country_label = _country_label(str(evidence_package.get("country") or ""))
    if intent == "news_policy_search" and _is_bev_subsidy_cap_question(question_text) and _policy_bonus_has_ended_evidence(evidence_package):
        question_actions = _bev_subsidy_cap_bonus_ended_recommended_actions(
            refs,
            country_label=country_label,
            target_model=_policy_target_model(evidence_package, question_text),
        )
    else:
        question_actions = _question_specific_recommended_actions(
            intent,
            question_text,
            refs,
            country_label=country_label,
            evidence_package=evidence_package,
            method=method,
        )
    result.extend(question_actions)
    for index, card in enumerate(insight_cards):
        action = str(card.get("recommendedAction") or "").strip()
        if not action:
            continue
        if (
            intent == "news_policy_search"
            and _is_bev_subsidy_cap_question(question_text)
            and _policy_bonus_has_ended_evidence(evidence_package)
            and _looks_like_source_repair_action(action)
        ):
            continue
        result.append({
            "action": action,
            "rationale": str(card.get("claim") or card.get("implication") or "Insight card suggests this next step.").strip(),
            "priority": "P0" if index == 0 else "P1",
            "evidenceRefs": refs[:3],
            "citationIds": _string_list(card.get("citations"))[:4],
        })
    if method and intent == "pricing_analysis":
        warnings = method.get("dataQualityWarnings") if isinstance(method.get("dataQualityWarnings"), list) else []
        warning_refs = [
            str(warning.get("code") or "")
            for warning in warnings[:2]
            if isinstance(warning, dict) and str(warning.get("code") or "").strip()
        ]
        model = str(method.get("model") or _pricing_subject_label(evidence_package) or "目标车型").strip()
        method_actions = [
            ("补齐本车型与竞品 MSRP / TP / 月供价格矩阵", "P0"),
            (f"把 {model} 低配锚点和高配主推写成一页定价建议", "P1"),
            (f"统一 {model} 的 PVA、价格单位和本地税费口径", "P1"),
        ]
        for action, priority_value in method_actions:
            priority: Literal["P0", "P1", "P2"] = priority_value  # type: ignore[assignment]
            result.append({
                "action": action,
                "rationale": f"该动作来自已蒸馏的 {model} pricing method，可把用户材料转成可验证分析链。",
                "priority": priority,
                "evidenceRefs": refs[:3],
                "citationIds": warning_refs,
            })
    playbook = get_business_playbook(intent)
    actions = (
        []
        if topic_specific_report or question_actions
        else playbook["nextActions"] if playbook else ["补齐核心证据", "生成业务对比表", "输出汇报页"]
    )
    for index, action in enumerate(actions[:4]):
        priority: Literal["P0", "P1", "P2"] = "P0" if not result and index == 0 else "P1" if index < 2 else "P2"
        result.append({
            "action": action,
            "rationale": _action_rationale(intent, action),
            "priority": priority,
            "evidenceRefs": refs[:3],
            "citationIds": [],
        })
    deduped = _dedupe_actions(result)
    if intent == "market_overview":
        deduped = _specific_market_overview_recommended_actions(
            deduped,
            country_label=country_label,
            evidence_package=evidence_package,
        )
    if intent == "competitor_compare":
        deduped = _specific_competitor_recommended_actions(
            deduped,
            country_label=country_label,
            evidence_package=evidence_package,
            question_text=question_text,
        )
    if intent in {"pricing_analysis", "report_generation"} and method:
        deduped = _specific_pricing_method_recommended_actions(
            deduped,
            method=method,
            evidence_package=evidence_package,
            intent=intent,
        )
    if intent == "configuration_analysis":
        deduped = _specific_configuration_recommended_actions(
            deduped,
            country_label=country_label,
            evidence_package=evidence_package,
        )
    if intent == "voc_analysis":
        deduped = _specific_voc_recommended_actions(
            deduped,
            country_label=country_label,
            evidence_package=evidence_package,
            question_text=question_text,
        )
    if intent == "news_policy_search":
        deduped = _specific_policy_recommended_actions(
            deduped,
            country_label=country_label,
            evidence_package=evidence_package,
            question_text=question_text,
        )
    return deduped[:5]


def _missing_required_tool_actions(
    evidence_package: dict[str, Any],
    refs: list[str],
) -> list[RecommendedAction]:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    actions: list[RecommendedAction] = []
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name.startswith("missing_required_tool:"):
            continue
        tool_name = name.replace("missing_required_tool:", "", 1).strip()
        if not tool_name:
            continue
        actions.append({
            "action": _required_tool_business_action(tool_name),
            "rationale": (
                f"当前分析链缺少{_tool_business_label(tool_name)}结果；"
                f"{str(item.get('reason') or '缺失后不能形成完整证据链。').strip()}"
            ),
            "priority": "P0",
            "evidenceRefs": refs[:3],
            "citationIds": [],
        })
    return actions


def _source_repair_recommended_action(
    evidence_package: dict[str, Any],
    refs: list[str],
    *,
    question: str = "",
    method: BusinessMethodDistillation | None = None,
) -> RecommendedAction | None:
    source_repair = _source_repair_candidates_from_evidence_package(evidence_package)
    if _is_policy_source_repair(source_repair) and _has_official_policy_source_evidence(evidence_package):
        return None
    repair_text = _source_repair_action_text(source_repair, question=question, method=method)
    if not repair_text:
        return None
    if _is_policy_source_repair(source_repair):
        rationale = "外部研究诊断已记录政策/新闻官方来源候选。先补齐来源、发布日期、适用对象和限制条件，再写时效性政策结论。"
    elif _is_leasing_tco_source_context(question=question, evidence_package=evidence_package):
        rationale = "外部研究诊断已记录 leasing/TCO/company-car 补源线索。先补齐月供、残值、税务 benefit、年里程和充电条件口径，再判断 PHEV 是否能支撑大客户场景。"
    elif _is_external_query_source_repair(source_repair):
        rationale = "外部研究诊断已记录本轮 VOC/媒体/论坛检索线索。先补齐可引用来源和原文要点，再判断是否属于高频用户痛点。"
    elif _source_repair_has_review_pending(source_repair):
        rationale = "MSRP/current price 覆盖诊断已找到待审核官方价格观察。先审核 observation 的 trim、币种、发布日期和来源，再生成 current price 后写确定价格数字。"
    else:
        rationale = "MSRP/current price 覆盖诊断已找到来源修复候选。先补齐价格来源并生成当前价格记录，再写确定价格数字。"
    return {
        "action": repair_text,
        "rationale": rationale,
        "priority": "P0",
        "evidenceRefs": refs[:3],
        "citationIds": [],
    }


def _pending_msrp_review_summary_text(evidence_package: dict[str, Any]) -> str:
    rows = _pending_msrp_review_observation_rows(evidence_package)
    count = _pending_msrp_review_observation_count(evidence_package, rows)
    if count <= 0:
        return ""
    example_text = _pending_msrp_review_examples_text(rows)
    example_suffix = f"（{example_text}）" if example_text else ""
    return (
        f"已抓到 {count} 条官方来源待审核 MSRP 观察{example_suffix}，"
        "可用于价格阶梯/版本价差 review 骨架；未审核前不能当正式 current MSRP。"
    )


def _pending_msrp_review_takeaway(evidence_package: dict[str, Any]) -> str:
    summary = _pending_msrp_review_summary_text(evidence_package)
    if not summary:
        return ""
    return f"MSRP review：{summary}"


def _pending_msrp_review_bullet(evidence_package: dict[str, Any]) -> str:
    summary = _pending_msrp_review_summary_text(evidence_package)
    if not summary:
        return ""
    return f"待审核价格：{summary}"


def _pending_msrp_review_observation_rows(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = _source_repair_candidates_from_evidence_package(evidence_package)
    rows: list[dict[str, Any]] = []
    for key, role in (("ownModel", "本车型"), ("competitorCorridor", "竞品走廊")):
        entries = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            observations = entry.get("reviewPendingObservations") if isinstance(entry.get("reviewPendingObservations"), list) else []
            if observations:
                for observation in observations:
                    if not isinstance(observation, dict):
                        continue
                    row = {
                        "role": role,
                        "brand": str(observation.get("brand") or entry.get("brand") or "").strip(),
                        "model": str(observation.get("model") or entry.get("model") or "").strip(),
                        "trim": str(observation.get("trim") or observation.get("variant") or entry.get("trim") or "").strip(),
                        "sourceMsrpValue": observation.get("sourceMsrpValue"),
                        "sourceCurrency": str(observation.get("sourceCurrency") or entry.get("defaultCurrency") or "").strip(),
                        "msrpValue": observation.get("msrpValue"),
                        "currency": str(observation.get("currency") or "").strip(),
                        "sourceUrl": str(observation.get("sourceUrl") or entry.get("sourceUrl") or "").strip(),
                        "evidenceStatus": str(observation.get("evidenceStatus") or entry.get("reviewPendingStatus") or "review_pending_not_current_price").strip(),
                    }
                    rows.append(row)
                continue
            if _source_candidate_review_pending_rows(entry) <= 0:
                continue
            rows.append({
                "role": role,
                "brand": str(entry.get("brand") or "").strip(),
                "model": str(entry.get("model") or "").strip(),
                "trim": str(entry.get("trim") or "").strip(),
                "sourceMsrpValue": None,
                "sourceCurrency": str(entry.get("defaultCurrency") or "").strip(),
                "msrpValue": None,
                "currency": "",
                "sourceUrl": str(entry.get("sourceUrl") or "").strip(),
                "evidenceStatus": str(entry.get("reviewPendingStatus") or "review_pending_not_current_price").strip(),
            })
    return _dedupe_pending_msrp_review_rows(rows)


def _dedupe_pending_msrp_review_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = "|".join(
            str(row.get(part) or "")
            for part in ("role", "brand", "model", "trim", "sourceMsrpValue", "sourceCurrency", "msrpValue", "currency", "sourceUrl")
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _pending_msrp_review_observation_count(
    evidence_package: dict[str, Any],
    rows: list[dict[str, Any]],
) -> int:
    candidates = _source_repair_candidates_from_evidence_package(evidence_package)
    counted = 0
    for key in ("ownModel", "competitorCorridor"):
        entries = candidates.get(key) if isinstance(candidates.get(key), list) else []
        for entry in entries:
            if isinstance(entry, dict):
                counted += _source_candidate_review_pending_rows(entry)
    try:
        top_level_count = int(candidates.get("reviewPendingObservationCount") or 0)
    except (TypeError, ValueError):
        top_level_count = 0
    return max(counted, top_level_count, len(rows))


def _pending_msrp_review_examples_text(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    range_text = _pending_msrp_review_range_text(rows)
    example_rows = sorted(rows, key=_pending_msrp_review_sort_key)
    examples: list[str] = []
    for row in example_rows:
        label = _pending_msrp_review_row_label(row)
        price = _pending_msrp_review_price_text(row)
        if not label and not price:
            continue
        examples.append(" ".join(item for item in (label, price) if item))
        if len(examples) >= 3:
            break
    parts = [range_text, f"示例：{'、'.join(examples)}" if examples else ""]
    return "；".join(item for item in parts if item)


def _pending_msrp_review_range_text(rows: list[dict[str, Any]]) -> str:
    by_currency: dict[str, list[float]] = {}
    for row in rows:
        currency = str(row.get("sourceCurrency") or row.get("currency") or "").strip()
        value = _pending_msrp_numeric_price(row)
        if not currency or value is None:
            continue
        by_currency.setdefault(currency, []).append(value)
    if not by_currency:
        return ""
    currency, values = max(by_currency.items(), key=lambda item: len(item[1]))
    if len(values) < 2:
        return ""
    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        return f"待审核价格点 {_format_price_number(min_value)} {currency}"
    return f"待审核价格阶梯 {_format_price_number(min_value)}-{_format_price_number(max_value)} {currency}"


def _pending_msrp_review_sort_key(row: dict[str, Any]) -> tuple[int, float, str]:
    value = _pending_msrp_numeric_price(row)
    if value is None:
        return (1, 0.0, _pending_msrp_review_row_label(row))
    return (0, value, _pending_msrp_review_row_label(row))


def _pending_msrp_numeric_price(row: dict[str, Any]) -> float | None:
    for key in ("sourceMsrpValue", "msrpValue"):
        value = row.get(key)
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, (int, float)):
            return float(value)
        match = re.search(r"-?\d[\d,]*(?:\.\d+)?", str(value))
        if match:
            try:
                return float(match.group(0).replace(",", ""))
            except ValueError:
                continue
    return None


def _pending_msrp_review_price_text(row: dict[str, Any]) -> str:
    currency = str(row.get("sourceCurrency") or row.get("currency") or "").strip()
    value = _pending_msrp_numeric_price(row)
    if value is not None and currency:
        return f"{_format_price_number(value)} {currency}"
    if value is not None:
        return _format_price_number(value)
    raw_value = str(row.get("sourceMsrpValue") or row.get("msrpValue") or "").strip()
    return f"{raw_value} {currency}".strip() if raw_value else ""


def _pending_msrp_review_row_label(row: dict[str, Any]) -> str:
    model = str(row.get("model") or "").strip()
    trim = str(row.get("trim") or "").strip()
    if trim:
        return trim
    brand = str(row.get("brand") or "").strip()
    return " ".join(item for item in (brand, model) if item).strip()


def _source_repair_candidates_from_evidence_package(evidence_package: dict[str, Any]) -> dict[str, Any]:
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for item in tool_results:
        if not isinstance(item, dict):
            continue
        diagnostics = item.get("coverageDiagnostics") if isinstance(item.get("coverageDiagnostics"), dict) else {}
        candidates = diagnostics.get("sourceRepairCandidates") if isinstance(diagnostics.get("sourceRepairCandidates"), dict) else {}
        if candidates:
            return candidates
        queries = diagnostics.get("externalResearchQueries") if isinstance(diagnostics.get("externalResearchQueries"), list) else []
        query_texts = _dedupe([
            cleaned
            for query in queries
            if (cleaned := _clean_external_research_query_text(str(query or "")))
        ])
        if query_texts:
            return {
                "dataStatus": "external_research_query_candidates",
                "queries": query_texts[:5],
                "candidateCount": len(query_texts),
                "materializedCandidateCount": 0,
            }
    return {}


def _is_leasing_tco_source_context(
    *,
    question: str = "",
    evidence_package: dict[str, Any] | None = None,
) -> bool:
    question_text = _normalize_question_text(question)
    has_question_signal = (
        "phev" in question_text
        and _contains_any(
            question_text,
            ("leasing", "lease", "大客户", "fleet", "公司车", "company car", "tco", "月供", "残值", "benefit"),
        )
    )
    if has_question_signal:
        return True
    if not isinstance(evidence_package, dict):
        return False
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    missing_names = {
        str(item.get("name") or "")
        for item in missing
        if isinstance(item, dict)
    }
    return "leasing_tco_or_company_car_evidence" in missing_names


def _source_repair_action_text(
    candidates: dict[str, Any],
    *,
    question: str = "",
    method: BusinessMethodDistillation | None = None,
) -> str:
    if not candidates:
        return ""
    own_model = candidates.get("ownModel") if isinstance(candidates.get("ownModel"), list) else []
    competitor_corridor = candidates.get("competitorCorridor") if isinstance(candidates.get("competitorCorridor"), list) else []
    own_labels = _source_candidate_labels(own_model)
    competitor_labels = _relevant_source_candidate_labels(
        competitor_corridor,
        question=question,
        method=method,
    )
    pending_entries = [
        entry
        for entry in [*own_model, *competitor_corridor]
        if isinstance(entry, dict) and _source_candidate_review_pending_rows(entry) > 0
    ]
    pending_labels = _source_candidate_labels(pending_entries)
    pending_count = sum(_source_candidate_review_pending_rows(entry) for entry in pending_entries)
    if _is_policy_source_repair(candidates) and competitor_labels:
        return (
            "先用外部研究读取并确认政策/新闻官方来源候选"
            f"（{', '.join(competitor_labels[:4])}），补齐发布日期、适用对象和限制条件后再重跑 Business Validation；"
            "这些候选只是搜索/补证据入口，不能直接当作政策事实。"
        )
    if _is_external_query_source_repair(candidates):
        queries = [
            _clean_external_research_query_text(str(query or "").strip())
            for query in candidates.get("queries", [])
            if _clean_external_research_query_text(str(query or "").strip())
        ]
        if queries:
            examples = _compact_external_query_examples(queries)
            example_text = (
                f"（共{len(queries)}条，示例：{'; '.join(examples)}）"
                if examples
                else f"（共{len(queries)}条）"
            )
            if _is_leasing_tco_source_context(question=question):
                return (
                    "先在外部来源修复表中验证 leasing/TCO/company-car 补源线索"
                    f"{example_text}，保留标题、URL、发布日期、月供/残值/税务 benefit 口径和适用车型后再重跑 Business Validation；"
                    "这些检索线索只是补证入口，不能直接当作 PHEV 大客户 TCO 结论。"
                )
            return (
                "先在外部来源修复表中验证 VOC/媒体/论坛检索线索"
                f"{example_text}，保留标题、URL、发布日期和可支撑原文要点后再重跑 Business Validation；"
                "这些检索线索只是补源入口，不能直接当作用户高频吐槽证据。"
            )
    if pending_entries:
        pending_examples = ", ".join(_compact_external_query_examples(pending_labels))
        example_text = f"，示例：{pending_examples}" if pending_examples else ""
        return (
            "先在 MSRP review queue 中审核已抓到的官方价格观察"
            f"（共{pending_count}条{example_text}），"
            "确认 trim/版本、币种、发布日期和来源后再生成 current price；"
            "这些观察现在只能作为待审核证据，不能直接当作确定 MSRP。"
        )
    if own_labels:
        ready_note = _source_repair_ready_draft_note(own_model, competitor_corridor)
        if _all_source_candidates_are_search_queries(own_model):
            return (
                "先在 MSRP 来源验证表中验证本车型/竞品官方价格候选"
                f"{_compact_source_candidate_suffix(own_labels)}，确认 URL、版本/配置、币种、发布日期后生成当前价格记录；"
                f"{ready_note}"
                "这些搜索候选只是补源入口，不能直接当作官方价格证据。"
            )
        if _any_source_candidates_are_search_queries(own_model):
            return (
                "先在 MSRP 来源验证表中分别验证本车型/竞品官方价格搜索候选和来源草稿"
                f"{_mixed_source_candidate_suffix(own_model)}，确认 URL、版本/配置、币种、发布日期后生成当前价格记录；"
                f"{ready_note}"
                "搜索候选和来源草稿都只是补源入口，不能直接当作官方价格证据。"
            )
        return (
            "先在 MSRP 来源验证表中审核本车型/竞品 MSRP 来源草稿"
            f"{_compact_source_candidate_suffix(own_labels)}，生成当前价格记录后再重跑 Business Validation；"
            f"{ready_note}"
            "这些草稿只是补数输入，不能直接当作官方价格证据。"
        )
    if competitor_labels:
        ready_note = _source_repair_ready_draft_note(own_model, competitor_corridor)
        if _all_source_candidates_are_search_queries(competitor_corridor):
            return (
                "先在 MSRP 来源验证表中补齐本车型官方 MSRP 来源，并验证竞品价格搜索候选"
                f"{_compact_source_candidate_suffix(competitor_labels)}建立价格带；"
                f"{ready_note}"
                "这些搜索候选只是补源入口，不能直接当作官方价格证据。"
            )
        return (
            "先在 MSRP 来源验证表中补齐本车型官方 MSRP 来源，并审核竞品价格走廊候选"
            f"{_compact_source_candidate_suffix(competitor_labels)}用于价格带修复；"
            f"{ready_note}"
            "这些候选只是补数清单，不能直接当作官方价格证据。"
        )
    return ""


def _pricing_source_repair_status_note(
    candidates: dict[str, Any],
    *,
    question: str = "",
    method: BusinessMethodDistillation | None = None,
) -> str:
    if not candidates:
        return ""
    entries = [
        entry
        for key in ("ownModel", "competitorCorridor")
        for entry in (candidates.get(key) if isinstance(candidates.get(key), list) else [])
        if isinstance(entry, dict)
    ]
    if not entries:
        return ""
    relevant_entries = _relevant_source_candidate_entries(entries, question=question, method=method)
    if relevant_entries:
        entries = relevant_entries
    current_labels = _source_candidate_labels([
        entry for entry in entries if _source_candidate_current_price_rows(entry) > 0
    ])
    pending_labels = _source_candidate_labels([
        entry for entry in entries if _source_candidate_review_pending_rows(entry) > 0
    ])
    draft_labels = _source_candidate_labels([
        entry
        for entry in entries
        if _source_candidate_current_price_rows(entry) <= 0
        and _source_candidate_review_pending_rows(entry) <= 0
        and str(entry.get("candidateSourceType") or "").strip() == "source_draft"
    ])
    search_labels = _source_candidate_labels([
        entry
        for entry in entries
        if _source_candidate_current_price_rows(entry) <= 0
        and _source_candidate_review_pending_rows(entry) <= 0
        and str(entry.get("draftStatus") or "").strip() == "candidate_search_query"
    ])
    bits: list[str] = []
    if current_labels:
        bits.append(f"已有当前价格记录：{', '.join(current_labels[:4])}")
    if pending_labels:
        bits.append(f"待审核价格观察：{', '.join(pending_labels[:4])}")
    if draft_labels:
        bits.append(f"来源草稿待抽取前审核：{', '.join(draft_labels[:4])}")
    if search_labels:
        bits.append(f"官方价格搜索候选：{', '.join(search_labels[:4])}")
    if not bits:
        return ""
    if not current_labels:
        bits.append("当前还没有可直接引用为 MSRP 的价格记录")
    return "价格来源状态：" + "；".join(bits) + "。"


def _relevant_source_candidate_entries(
    candidates: list[dict[str, Any]],
    *,
    question: str = "",
    method: BusinessMethodDistillation | None = None,
) -> list[dict[str, Any]]:
    tokens = _source_candidate_priority_tokens(question=question, method=method)
    if not tokens:
        return candidates
    relevant: list[dict[str, Any]] = []
    for entry in candidates:
        brand = str(entry.get("brand") or "").strip()
        model = str(entry.get("model") or "").strip()
        candidate_text = _source_candidate_match_text(brand=brand, model=model)
        if any(_source_candidate_token_matches(token, candidate_text) for token in tokens):
            relevant.append(entry)
    return relevant


def _compact_source_candidate_suffix(labels: list[str]) -> str:
    examples = _compact_external_query_examples(labels)
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
        label = _source_candidate_labels([entry])
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
        parts.append(f"搜索候选{len(search_labels)}项：{', '.join(_compact_external_query_examples(search_labels))}")
    if draft_labels:
        parts.append(f"来源草稿{len(draft_labels)}项：{', '.join(_compact_external_query_examples(draft_labels))}")
    if pending_labels:
        parts.append(f"待审核观察{len(pending_labels)}项：{', '.join(_compact_external_query_examples(pending_labels))}")
    if materialized_labels:
        parts.append(f"已物化样本{len(materialized_labels)}项：{', '.join(_compact_external_query_examples(materialized_labels))}")
    if parts:
        return f"（{'；'.join(parts)}）"
    return _compact_source_candidate_suffix(_source_candidate_labels(candidates))


def _compact_external_query_examples(queries: list[str]) -> list[str]:
    examples: list[str] = []
    for query in queries:
        text = _clean_business_text(query)
        if not text:
            continue
        examples.append(_truncate_public_query(text, max_chars=64))
        if len(examples) >= 2:
            break
    return examples


def _source_candidate_review_pending_rows(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("reviewPendingRows") or 0)
    except (TypeError, ValueError):
        return 0


def _source_candidate_current_price_rows(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("currentPriceRows") or 0)
    except (TypeError, ValueError):
        return 0


def _source_repair_ready_draft_note(*candidate_groups: list[Any]) -> str:
    ready_entries: list[dict[str, Any]] = []
    for group in candidate_groups:
        for entry in group:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("candidateSourceType") or "").strip() != "source_draft":
                continue
            try:
                current_price_rows = int(entry.get("currentPriceRows") or 0)
            except (TypeError, ValueError):
                current_price_rows = 0
            if current_price_rows > 0:
                continue
            status = str(entry.get("materializationStatus") or "").strip()
            dry_run = str(entry.get("dryRunCommand") or "").strip()
            if status == "ready_for_extraction" or dry_run:
                ready_entries.append(entry)
    labels = _source_candidate_labels(ready_entries)
    if not labels:
        return ""
    examples = ", ".join(_compact_external_query_examples(labels))
    return (
        f"其中 {len(labels)} 个来源草稿已可进入抽取前审核"
        f"{f'，示例：{examples}' if examples else ''}，应先人工确认版本/配置、币种、日期和价格合理性，再写入 current price；"
    )


def _source_repair_has_review_pending(candidates: dict[str, Any]) -> bool:
    for key in ("ownModel", "competitorCorridor"):
        rows = candidates.get(key) if isinstance(candidates.get(key), list) else []
        if any(isinstance(entry, dict) and _source_candidate_review_pending_rows(entry) > 0 for entry in rows):
            return True
    try:
        return int(candidates.get("reviewPendingObservationCount") or 0) > 0
    except (TypeError, ValueError):
        return False


def _truncate_public_query(value: str, *, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars].rstrip()}..."


def _is_policy_source_repair(candidates: dict[str, Any]) -> bool:
    return str(candidates.get("dataStatus") or "").strip() == "external_policy_source_candidates"


def _is_external_query_source_repair(candidates: dict[str, Any]) -> bool:
    return str(candidates.get("dataStatus") or "").strip() == "external_research_query_candidates"


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


def _has_official_policy_source_evidence(evidence_package: dict[str, Any]) -> bool:
    official_domains = (
        "transportstyrelsen.se",
        "regeringen.se",
        "skatteverket.se",
        "government.se",
        "europa.eu",
    )
    values: list[str] = []
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tool_results:
        if not isinstance(tool, dict):
            continue
        tool_name = str(tool.get("toolName") or "")
        if tool_name not in {"external_research", "search_market_news", "read_web_page", "pageindex_search_documents"}:
            continue
        values.extend([str(tool.get("summary") or ""), str(tool.get("keyFindings") or "")])
        refs = tool.get("evidenceRefs") if isinstance(tool.get("evidenceRefs"), list) else []
        for ref in refs:
            if isinstance(ref, dict):
                values.extend([str(ref.get("label") or ""), str(ref.get("value") or ""), str(ref.get("source") or "")])
    text = " ".join(values).casefold()
    return any(domain in text for domain in official_domains)


def _is_bev_subsidy_cap_question(text: str) -> bool:
    normalized = _normalize_question_text(text)
    return (
        "价格上限" in normalized
        and "bev" in normalized
        and _contains_any(normalized, ("补贴", "bonus", "subsidy", "incentive", "资格", "政策"))
    )


def _policy_target_model(evidence_package: dict[str, Any], question_text: str) -> str:
    targets, _ = _competitor_requested_entities(evidence_package, question_text)
    return targets[0] if targets else "目标 BEV"


def _bev_subsidy_cap_bonus_ended_recommended_actions(
    refs: list[str],
    *,
    country_label: str = "",
    target_model: str,
) -> list[RecommendedAction]:
    market = _country_label(country_label or "当前市场")
    return [
        {
            "action": f"补齐当前 {target_model} MSRP、竞品价格走廊和 24/36 个月月供",
            "rationale": f"官方 bonus 已结束后，{target_model} 定价应回到竞品价格、配置价值和用户月供，而不是为了卡历史补贴上限牺牲配置或毛利。",
            "priority": "P0",
            "evidenceRefs": refs[:3],
            "citationIds": [],
        },
        {
            "action": f"生成补贴失效口径下的 {target_model} 主销版定价页",
            "rationale": "销售和产品页需要把历史补贴锚点、竞品走廊、配置价值和风险边界合并成一页可汇报结论。",
            "priority": "P0",
            "evidenceRefs": refs[:3],
            "citationIds": [],
        },
        {
            "action": f"监控{market}是否发布新的 BEV 补贴计划、价格门槛或特定人群资格",
            "rationale": "如果新政策出现，价格上限会重新影响入门版锚点和高配价值解释。",
            "priority": "P1",
            "evidenceRefs": refs[:3],
            "citationIds": [],
        },
    ]


def _source_candidate_labels(candidates: list[Any]) -> list[str]:
    labels: list[str] = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        model = str(entry.get("model") or "").strip()
        source_code = str(entry.get("sourceCode") or "").strip()
        label = model or source_code
        if label:
            labels.append(label)
    return _dedupe(labels)


def _relevant_source_candidate_labels(
    candidates: list[Any],
    *,
    question: str = "",
    method: BusinessMethodDistillation | None = None,
) -> list[str]:
    labels = _source_candidate_labels(candidates)
    tokens = _source_candidate_priority_tokens(question=question, method=method)
    if not labels or not tokens:
        return labels
    relevant: list[str] = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        brand = str(entry.get("brand") or "").strip()
        model = str(entry.get("model") or "").strip()
        label = model or str(entry.get("sourceCode") or "").strip()
        candidate_text = _source_candidate_match_text(brand=brand, model=model)
        if label and any(_source_candidate_token_matches(token, candidate_text) for token in tokens):
            relevant.append(label)
    return _dedupe(relevant) or labels


def _source_candidate_priority_tokens(
    *,
    question: str = "",
    method: BusinessMethodDistillation | None = None,
) -> list[str]:
    tokens: list[str] = []
    if method:
        if str(method.get("model") or "").strip():
            tokens.append(str(method.get("model") or ""))
        tokens.extend(str(item or "") for item in method.get("competitorPool", []))
    text = str(question or "").upper()
    if any(token in text for token in ("J7", "JAECOO 7", "JAECOO7")):
        tokens.extend(["J7 HEV", "J7", "Corolla Cross", "RAV4", "Sportage", "C-HR", "Qashqai"])
    if any(token in text for token in ("O5", "OMODA 5", "OMODA5")):
        tokens.extend(["EV3", "EX30", "ID.4", "Enyaq", "EQA"])
    if any(token in text for token in ("O9", "OMODA 9", "OMODA9", "J8", "JAECOO 8", "JAECOO8")):
        tokens.extend(["XC60", "EX60", "XC90", "EX90", "EV9", "Sorento", "Kodiaq", "Tayron"])
    return _dedupe([_normalize_source_match_token(item) for item in tokens if str(item or "").strip()])


def _source_candidate_match_text(*, brand: str, model: str) -> str:
    return _normalize_source_match_token(" ".join(part for part in [brand, model] if part))


def _source_candidate_token_matches(token: str, candidate_text: str) -> bool:
    if not token or not candidate_text:
        return False
    return token == candidate_text or token in candidate_text or candidate_text.endswith(f" {token}")


def _normalize_source_match_token(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def _required_tool_expected_evidence(tool_name: str) -> str:
    mapping = {
        "query_msrp_pricing": "本车型当前 MSRP/价格",
        "compare_competitive_set": "竞品池和价格/价值走廊",
        "query_price_positioning": "目标价所在价格带位置",
        "query_country_snapshot": "市场规模、份额和结构快照",
        "build_market_chart": "趋势图表序列",
        "compare_vehicle_variants": "版本/配置差异",
        "external_research": "带日期的外部来源",
        "search_market_news": "带日期的新闻或政策来源",
        "pageindex_search_documents": "文档引用",
        "minirag_query_graph": "多跳文档证据",
        "query_with_filters": "筛选后的库存或市场数据行",
    }
    return mapping.get(tool_name, "必需工具输出")


def _required_tool_business_action(tool_name: str) -> str:
    evidence = _required_tool_expected_evidence(tool_name)
    artifact = {
        "query_msrp_pricing": "Pricing evidence table",
        "compare_competitive_set": "Competitor comparison table",
        "query_price_positioning": "Pricing corridor chart",
        "query_country_snapshot": "Market decision table",
        "build_market_chart": "Market structure chart",
        "compare_vehicle_variants": "Configuration validation matrix",
        "external_research": "Source evidence table",
        "search_market_news": "Policy / news evidence table",
        "pageindex_search_documents": "Report evidence appendix",
        "minirag_query_graph": "Report evidence appendix",
        "query_with_filters": "Inventory / BOM evidence table",
    }.get(tool_name, "Evidence table")
    return f"补齐{evidence}证据，并生成 {artifact}"


def _risks_and_missing(
    evidence_package: dict[str, Any],
    alignment: EvidenceAlignment,
    *,
    method: BusinessMethodDistillation | None = None,
    question: str = "",
    intent: str = "",
) -> list[BusinessRisk]:
    result: list[BusinessRisk] = []
    if alignment["status"] == "conflicting":
        result.append({
            "name": "jato_external_conflict",
            "impact": "关键结论需要人工复核，不能直接进入定价或产品决策。",
            "mitigation": "按来源、时间、车型口径拆开冲突，再补一次 JATO 内部交叉验证。",
        })
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        if (
            name == "minimum_external_sources"
            and _is_leasing_tco_source_context(question=question, evidence_package=evidence_package)
        ):
            result.append({
                "name": name,
                "impact": "可用外部来源数量不足，不能把泛 leasing 目录或市场背景写成 PHEV 大客户 TCO/company-car 结论。",
                "mitigation": "补 Skatteverket/company-car benefit、官方税费口径、租赁月供、残值/RV、年里程和充电条件来源，再做 BEV/PHEV/HEV 同假设比较。",
            })
            continue
        result.append({
            "name": name,
            "impact": _missing_impact(name, str(item.get("reason") or item.get("impact") or "").strip()),
            "mitigation": _missing_mitigation(name),
        })
    governance = evidence_package.get("researchGovernance") if isinstance(evidence_package.get("researchGovernance"), dict) else {}
    warnings = governance.get("policyWarnings") if isinstance(governance.get("policyWarnings"), list) else []
    for warning in warnings[:3]:
        result.append({
            "name": "research_policy_warning",
            "impact": str(warning),
            "mitigation": "补官方来源、发布日期或更高质量来源后再把影响写成确定事实。",
        })
    source_repair = _source_repair_candidates_from_evidence_package(evidence_package)
    source_repair_text = _source_repair_action_text(
        source_repair,
        question=question,
        method=method,
    )
    if source_repair_text:
        is_leasing_repair = _is_leasing_tco_source_context(
            question=question,
            evidence_package=evidence_package,
        )
        result.append({
            "name": "external_source_repair_candidates" if _is_external_query_source_repair(source_repair) else "source_repair_candidates",
            "impact": (
                "当前只有 leasing/TCO/company-car 检索线索，还没有可引用的月供、残值、税务 benefit 或 fleet 使用口径，不能写成 PHEV 大客户优势。"
                if is_leasing_repair
                else (
                "当前只有外部检索线索，还没有可引用的 VOC/媒体/论坛来源，不能写成高频用户结论。"
                if _is_external_query_source_repair(source_repair)
                else "当前 MSRP/竞品价格走廊仍有来源覆盖缺口，数字价格结论不能直接写死。"
                )
            ),
            "mitigation": source_repair_text,
        })
    if method:
        warnings = method.get("dataQualityWarnings") if isinstance(method.get("dataQualityWarnings"), list) else []
        for warning in warnings[:4]:
            if not isinstance(warning, dict):
                continue
            result.append({
                "name": str(warning.get("code") or "method_data_quality_warning"),
                "impact": str(warning.get("impact") or "用户材料方法论仍需当前价格、政策和配置证据交叉验证。"),
                "mitigation": str(warning.get("mitigation") or "补齐官方 MSRP、竞品价格、配置差异和本地税费口径后再写成确定结论。"),
            })
    if not result:
        result.append({
            "name": "decision_boundary",
            "impact": "结论仍应随最新价格、政策和配置证据更新。",
            "mitigation": "保留可引用证据和来源日期，进入人工业务验收。",
        })
    return _dedupe_risks(result)


def _question_specific_business_implications(
    intent: str,
    question_text: str,
    *,
    country_label: str = "",
    evidence_package: dict[str, Any] | None = None,
    method: BusinessMethodDistillation | None = None,
) -> list[str]:
    result: list[str] = []
    if intent == "market_overview" and _is_market_fit_question(question_text):
        package = evidence_package or {}
        powertrain = _market_opportunity_powertrain(package, question_text) or "目标动力"
        powertrain_sales = _market_cross_tab_ref_value(package, table="driveByFuel", row=powertrain, metric="sales")
        suv_a0_sales = _market_cross_tab_ref_value(package, table="driveBySegment", row="SUV A0", metric="sales")
        suv_a_sales = _market_cross_tab_ref_value(package, table="driveBySegment", row="SUV A", metric="sales")
        powertrain_2wd = _market_cross_tab_ref_value(package, table="driveByFuel", row=powertrain, metric="2WD_pct")
        if powertrain_sales or suv_a0_sales or suv_a_sales:
            target = _market_fit_target_label(question_text, method, package, powertrain=powertrain)
            validation_scope = _model_level_validation_scope(package, method=method)
            metric_parts = []
            if powertrain_sales:
                metric_parts.append(f"{powertrain} {powertrain_sales}")
            if suv_a0_sales:
                metric_parts.append(f"SUV A0 {suv_a0_sales}")
            if suv_a_sales:
                metric_parts.append(f"SUV A {suv_a_sales}")
            if powertrain_2wd:
                metric_parts.append(f"{powertrain} 2WD {powertrain_2wd}")
            result.extend([
                f"{_market_label(country_label)} {target} 应先作为 {powertrain} + SUV A0/A 的低风险进入验证线；已查 {'，'.join(metric_parts)}，产品页应聚焦主销驱动形式、价格敏感和可见配置价值，并建立{validation_scope}对标。",
                f"下一步不是继续泛问是否适合，而是补齐{validation_scope}的 MSRP、配置价值、月供/RV 和渠道场景，判断是否进入定价页。",
            ])
    if intent == "market_overview" and _is_hev_phev_route_question(question_text):
        result.extend([
            "HEV/PHEV 路线判断要先拆用户场景：无稳定充电、价格敏感和低风险换购更适合 HEV；公司车、长途和税费/TCO 有优势时才给 PHEV 主推资格。",
            "产品动作不应直接二选一，而应把 HEV 做低风险主线、PHEV 做政策/公司车/TCO 验证线，等价格、税费和补能证据补齐后再定主推权重。",
        ])
    if intent == "report_generation" and _is_competitor_report_scope(evidence_package or {}, question_text):
        package = evidence_package or {}
        subject = _competitor_report_subject_label(package, question_text)
        report_brief = _generic_competitor_report_brief(
            country_label=country_label,
            evidence_package=package,
            question_text=question_text,
        )
        result.extend([
            f"{subject} 竞品页应先写清对标角色的证据状态：{report_brief}",
            "这页要输出定位假设、核心差异、风险边界和补证动作，而不是把请求车型等权罗列或写成已验证胜负。",
        ])
    if intent == "news_policy_search":
        if "elbilspremien" in question_text:
            result.extend([
                "Elbilspremien 影响要先按资格、价格上限、车型类型、购买人群和交付时间拆，不应直接点名确定受益车型。",
                "业务动作应落到 BEV SUV A0/A 的版本价格门槛、低配锚点和补贴敏感度。",
            ])
        if "company car" in question_text and "bev" in question_text and "phev" in question_text:
            result.extend([
                "Company car 场景要把 BEV/PHEV 差异拆成 benefit tax、月供、充电条件、长途里程和公司车政策。",
                "BEV 更适合政策/低使用成本叙事，PHEV 只在无稳定充电、高里程或低风险替代场景保留理由。",
            ])
        if _is_phev_fleet_leasing_question(question_text):
            result.extend([
                "PHEV 大客户 leasing 不是政策新闻题的泛化回答，而是 fleet/TCO 验证题：必须把月供、残值/RV、税务 benefit、年里程、充电条件和冬季风险放进同一张判断表。",
                "如果这些口径算不出成本或使用风险优势，PHEV 只能保留为验证线，不能直接作为大客户主推方案。",
            ])
        if ("co2" in question_text or "co₂" in question_text) and "phev" in question_text:
            result.extend([
                "CO2 0-75g/km 阶梯只是 PHEV 的入场条件，不是自动利好；是否有利取决于官方税率/benefit 公式、认证排放和适用日期。",
                "PHEV 不能只靠低排放标签成立，必须在 company car/TCO、月供、残值、能耗、真实充电行为和用户里程里证明比 HEV/BEV 更稳。",
            ])
        if _is_bev_subsidy_cap_question(question_text):
            target_model = _policy_target_model(evidence_package or {}, question_text)
            result.extend([
                "BEV 补贴价格上限问题必须先确认当前政策是否仍有效、是否有新计划、价格门槛和适用人群，不能默认存在现行补贴上限。",
                f"如果价格上限有效，{target_model} 低配/主销版应承担资格锚点，高配则需要证明补贴外的配置价值和品牌理由；如果已失效，则该上限只能作为历史价格锚点。",
            ])
    if intent == "voc_analysis" and "v2h" in question_text:
        market = str(country_label or "").strip() or "当前市场"
        result.extend([
            f"V2H 在{market}应先作为“高感知但待验证”的技术加分项，不应被写成真实高频购买卖点；如果缺少直接 VOC，只能把 BEV/家庭 SUV/私人用户结构当做代理判断，不是消费者调研结论。",
            "验证重点应放在家庭能源、安全备份、冬季用车、科技形象和经销端话术是否能转化购买。",
        ])
    if intent == "pricing_analysis" and "phev" in question_text and _contains_any(question_text, ("leasing", "lease", "大客户", "fleet", "公司车")):
        result.extend([
            "大客户 leasing 下 PHEV 的理由要由 TCO、月供、残值、税费、长途里程和充电条件共同证明。",
            "若 PHEV 不能在公司车成本或使用风险上比 BEV/HEV 更稳，就不应作为主推动力路线。",
        ])
    if intent == "inventory_analysis" and _is_pi_market_split_question(question_text):
        result.extend([
            "SE/FI 合并 PI 可以作为共用计划/产品信息层，但车辆生成、物料号、合规、价格、订单和库存状态必须保留 market-level overlay。",
            "正确口径不是把车辆也合并生成，而是建立 PI header、market allocation、material code、vehicle generation 和 lifecycle 的映射关系。",
            "缺少 SE/FI 底表时只能判断原则是否合理，不能确认当前系统实现已经正确。",
        ])
    return result


def _question_specific_recommended_actions(
    intent: str,
    question_text: str,
    refs: list[str],
    *,
    country_label: str = "",
    evidence_package: dict[str, Any] | None = None,
    method: BusinessMethodDistillation | None = None,
) -> list[RecommendedAction]:
    def action(
        label: str,
        rationale: str,
        priority: Literal["P0", "P1", "P2"] = "P1",
    ) -> RecommendedAction:
        return {
            "action": label,
            "rationale": rationale,
            "priority": priority,
            "evidenceRefs": refs[:3],
            "citationIds": [],
        }

    result: list[RecommendedAction] = []
    if intent == "market_overview" and _is_market_fit_question(question_text):
        package = evidence_package or {}
        powertrain = _market_opportunity_powertrain(package, question_text) or "目标动力"
        target = _market_fit_target_label(question_text, method, package, powertrain=powertrain)
        competitor_pool = _market_fit_competitor_pool_label(package, method=method)
        has_internal_market_evidence = _market_fit_has_usable_internal_market_evidence(package)
        primary_action = (
            _market_fit_matrix_action(country_label, target, competitor_pool)
            if has_internal_market_evidence
            else _market_fit_gap_action(country_label, target, competitor_pool, powertrain)
        )
        secondary_action = (
            _market_fit_gap_action(country_label, target, competitor_pool, powertrain)
            if has_internal_market_evidence
            else _market_fit_matrix_action(country_label, target, competitor_pool)
        )
        result.extend([
            action(
                primary_action,
                (
                    f"先把已查 {powertrain} 需求、SUV A0/A 结构和 {competitor_pool} 放进同一张可验证业务表，再决定是否进入定价页。"
                    if has_internal_market_evidence
                    else f"{target} 是否继续推进，必须先验证 {powertrain} 需求、SUV A0/A 结构、车型级竞品窗口和价格/配置价值。"
                ),
                "P0",
            ),
            action(
                secondary_action,
                (
                    f"用 {competitor_pool}、价格锚点、可见配置价值和用车成本判断 {target} 是否有进入理由。"
                    if has_internal_market_evidence
                    else f"等 {powertrain}/SUV/竞品/价格证据齐后，再把 {target} 进入理由转成矩阵和汇报页。"
                ),
                "P1",
            ),
            action(
                f"确认是否进入 {target} 定价页",
                "只有市场结构、竞品和价格/配置证据闭环后，才进入定价方案。",
                "P1",
            ),
        ])
    if intent == "market_overview" and _is_hev_phev_route_question(question_text):
        result.extend([
            action(
                "建立 HEV vs PHEV 场景决策表",
                "用私人零售、公司车、长途里程、充电条件、税费/TCO 和价格带判断哪条动力路线应主推。",
                "P0",
            ),
            action(
                "补齐 PHEV 税费/company car、月供和充电条件证据",
                "PHEV 只有在 TCO 或使用风险上形成优势时，才应从验证线升级为主推线。",
                "P1",
            ),
        ])
    if intent == "report_generation" and _is_competitor_report_scope(evidence_package or {}, question_text):
        package = evidence_package or {}
        subject = _competitor_report_subject_label(package, question_text)
        report_action = action(
            _competitor_report_action_label(package, question_text),
            f"用 {subject} 的同口径价格、配置、销量和用户场景证据判断主对标和校验锚点，避免报告页只变成固定竞品角色模板。",
            "P0",
        )
        evidence_action = action(
            _competitor_report_evidence_action_label(package, question_text),
            "这些字段决定目标车型的价格带、可赢点、短板和汇报可信度。",
            "P0",
        )
        if _competitor_report_has_evidence_gap(package, question_text):
            report_action["priority"] = "P1"
            result.extend([evidence_action, report_action])
        else:
            evidence_action["priority"] = "P1"
            result.extend([report_action, evidence_action])
    if intent == "news_policy_search":
        if "elbilspremien" in question_text:
            result.extend([
                action("补官方政策原文、发布日期和资格/价格上限", "没有官方政策口径时不能点名确定受益车型。", "P0"),
                action("把 BEV SUV A0/A 车型按价格门槛和私人零售敏感度分组", "补贴影响需要落到车型资格和版本定价动作。", "P1"),
            ])
        if "company car" in question_text and "bev" in question_text and "phev" in question_text:
            result.extend([
                action("建立 BEV/PHEV company car benefit 对比表", "用 benefit tax、月供、残值、充电条件和里程场景判断动力路线。", "P0"),
                action("拆分私人零售与大客户公司车两套销售话术", "两类用户对补贴、税费和使用风险的敏感点不同。", "P1"),
            ])
        if _is_phev_fleet_leasing_question(question_text):
            result.extend([
                action("建立 PHEV fleet leasing TCO 表", "同时纳入月供、残值/RV、税务 benefit、年里程、充电条件、燃油/用电和冬季风险。", "P0"),
                action("验证 leasing/TCO/company-car 来源候选", "补齐月供、残值/RV、税务 benefit、年里程、适用车型和计算口径后，再判断 PHEV 是否有主推资格。", "P0"),
                action("定义 PHEV 只在哪些大客户场景保留主推资格", "防止把 PHEV 泛化成所有 fleet 或 company-car 场景的答案。", "P1"),
            ])
        if ("co2" in question_text or "co₂" in question_text) and "phev" in question_text:
            result.extend([
                action("核对 PHEV 认证 CO2、税率阶梯、company car 计算公式和发布日期", "PHEV 利好必须先确认官方公式、适用时间和车辆资格。", "P0"),
                action("输出 PHEV vs HEV/BEV 的 company car TCO 场景表", "用月供、残值、税费、能耗、真实充电行为和用户里程决定是否主推。", "P1"),
            ])
        if _is_bev_subsidy_cap_question(question_text):
            market = _country_label(country_label or "当前市场")
            target_model = _policy_target_model(evidence_package or {}, question_text)
            result.extend([
                action(f"核对{market} BEV 补贴价格上限是否仍有效及 {target_model} 是否适用", "先确认政策原文、发布日期、适用人群和价格门槛，避免把历史上限当成现行约束。", "P0"),
                action(f"核对 {target_model} 低配/主销版是否压进补贴价格门槛", "如果政策仍有效，价格上限会直接影响低配锚点和高配价值解释。", "P0"),
                action(f"生成补贴内/补贴外两套 {target_model} 定价页", "让销售和产品定义同时看到资格、配置价值和风险边界。", "P1"),
            ])
    if intent == "voc_analysis" and "v2h" in question_text:
        regional_scope = _market_regional_scope(country_label)
        result.extend([
            action(f"抓取{regional_scope} V2H 用户原声和媒体测评证据", "V2H 是否是真实购买卖点必须靠可追溯 VOC 来源验证。", "P0"),
            action("把 V2H 测成家庭能源、冬季备份和科技形象三套话术", "若无法成为主卖点，也可能作为高配或品牌技术感加分项。", "P1"),
        ])
    if intent == "pricing_analysis" and "phev" in question_text and _contains_any(question_text, ("leasing", "lease", "大客户", "fleet", "公司车")):
        result.extend([
            action("建立 PHEV fleet leasing TCO 表", "同时纳入月供、残值、税费、燃油/用电、充电条件和长途里程。", "P0"),
            action("定义 PHEV 只在哪些大客户场景保留主推资格", "防止把 PHEV 泛化成所有公司车场景的答案。", "P1"),
        ])
    if intent == "inventory_analysis" and _is_pi_market_split_question(question_text):
        result.extend([
            action(
                "定义 PI header + market overlay + vehicle/material generation mapping",
                "合并 PI 只能解决共用计划/产品信息，车辆生成和物料/库存必须按市场保留可追溯映射。",
                "P0",
            ),
            action(
                "补齐 SE/FI 的 PI、materialCode、market、vehicle generation 和 lifecycle 底表",
                "没有底表就不能确认合并 PI 是否真的不会破坏市场差异、合规和订单生命周期。",
                "P1",
            ),
        ])
    return result


def _question_specific_policy_bullets(
    *,
    country_label: str,
    question: str,
    alignment_note: str,
    actions: list[RecommendedAction],
    ref_note: str,
    evidence_package: dict[str, Any],
) -> list[str]:
    text = _normalize_question_text(question)
    first_action = actions[0]["action"] if actions else "查政策原文并交叉验证车型价格"
    if "elbilspremien" in text:
        return [
            f"{country_label} Elbilspremien 2026 影响应先按资格、价格上限、购买人群和交付时间拆分{ref_note}。",
            f"优先受影响的是价格门槛内、私人零售敏感度高的 BEV SUV A0/A；没有官方条文和发布日期时不能点名确定受益车型，当前证据状态为{alignment_note}。",
            f"建议动作：{first_action}。",
        ]
    if "company car" in text and "bev" in text and "phev" in text:
        return [
            f"{country_label} company car benefit 应拆成 benefit tax、月供、残值、充电条件和里程场景{ref_note}。",
            f"BEV 更适合政策/低使用成本叙事，PHEV 只在长途、无稳定充电或低风险替代场景保留理由；当前证据状态为{alignment_note}。",
            f"建议动作：{first_action}。",
        ]
    if _is_phev_fleet_leasing_question(text):
        return [
            f"{country_label} PHEV 大客户 leasing 要按 fleet/TCO 验证线处理，而不是只查政策新闻{ref_note}。",
            f"判断口径应同时纳入月供、残值/RV、税务 benefit、年里程、充电条件、长途里程和冬季使用风险；当前证据状态为{alignment_note}，缺这些口径时不能直接主推。",
            f"建议动作：{first_action}。",
        ]
    if ("co2" in text or "co₂" in text) and "phev" in text:
        return [
            f"{country_label} CO2 0-75g/km 阶梯只是 PHEV 的入场条件，不是自动利好；必须和认证排放、官方税率/benefit 公式、月供、残值、能耗和真实充电行为一起判断{ref_note}。",
            f"不能只因 PHEV 低排放就判断有利；当前证据状态为{alignment_note}，应先把税费/TCO 公式、发布日期和适用车辆补齐。",
            f"建议动作：{first_action}。",
        ]
    if _is_bev_subsidy_cap_question(text):
        target_model = _policy_target_model(evidence_package, text)
        return [
            f"{country_label} BEV 补贴价格上限问题要先确认政策是否仍有效、是否有新计划、价格门槛和适用人群{ref_note}。",
            f"如果价格上限有效，{target_model} 低配/主销版承担补贴资格锚点，高配需要证明补贴外的配置价值；如果已失效，只能把历史上限作为价格锚点。当前证据状态为{alignment_note}。",
            f"建议动作：{first_action}。",
        ]
    return []


def _question_specific_voc_bullets(
    *,
    country_label: str,
    question: str,
    alignment_note: str,
    actions: list[RecommendedAction],
    ref_note: str,
) -> list[str]:
    text = _normalize_question_text(question)
    first_action = actions[0]["action"] if actions else "补论坛/媒体/VOC 来源并按主题聚类"
    if "v2h" not in text:
        return []
    return [
        f"{country_label} V2H 暂时应定位为高感知但待验证的技术加分项，不能直接写成高频购买卖点{ref_note}。",
        f"验证重点是家庭能源、安全备份、冬季用车、科技形象和经销端话术是否能转化购买；当前证据状态为{alignment_note}。",
        f"建议动作：{first_action}。",
    ]


def _question_specific_market_overview_bullets(
    *,
    country_label: str,
    question: str,
    alignment_note: str,
    actions: list[RecommendedAction],
    ref_note: str,
    evidence_package: dict[str, Any] | None = None,
    method: BusinessMethodDistillation | None = None,
) -> list[str]:
    text = _normalize_question_text(question)
    first_action = actions[0]["action"] if actions else "建立 HEV vs PHEV 场景决策表"
    if _is_market_fit_question(text):
        package = evidence_package or {}
        powertrain = _market_opportunity_powertrain(package, text) or "目标动力"
        target = _market_fit_target_label(text, method, package, powertrain=powertrain)
        competitor_pool = _market_fit_competitor_pool_label(package, method=method)
        competitor_window = competitor_pool if "竞品" in competitor_pool else f"{competitor_pool} 竞品窗口"
        has_refs = "当前缺少可引用证据" not in ref_note
        if has_refs:
            return [
                f"{_market_label(country_label)} {target} 的优先验证入口已有市场结构证据支撑{ref_note}。",
                f"验证重点是把 {powertrain} 需求、SUV A0/A 车型结构、{competitor_window}和 {target} 价格/配置价值闭环；当前证据状态为{alignment_note}，还不能直接定稿上市或定价。",
                f"建议动作：{first_action}。",
            ]
        return [
            f"{_market_label(country_label)} {target} 现在应作为“待验证机会”，不能直接写成已确认进入机会{ref_note}。",
            f"验证重点是 {powertrain} 需求、SUV A0/A 车型结构、{competitor_window}，以及 {target} 的价格/配置价值是否能闭环；当前证据状态为{alignment_note}。",
            f"建议动作：{first_action}。",
        ]
    if _is_suv_a0_a_structure_question(text):
        return [
            f"{_market_label(country_label)} SUV A0/A 主销结构应先用 segment cross-tab 验证销量、动力结构和驱动形式{ref_note}。",
            f"产品含义要落到 BEV/HEV/PHEV 的价格带、续航/冬季包和配置价值，而不是泛泛解释 SUV 受欢迎；当前证据状态为{alignment_note}。",
            f"建议动作：{first_action}。",
        ]
    if not _is_hev_phev_route_question(text):
        return []
    return [
        f"{_market_label(country_label)} HEV/PHEV 路线判断不能直接二选一，应先拆私人零售、公司车、长途里程、充电条件、税费/TCO 和价格带{ref_note}。",
        f"更稳的第一版打法是 HEV 做低风险主线，PHEV 做公司车/TCO 验证线；当前证据状态为{alignment_note}，不能把 PHEV 主推写成确定结论。",
        f"建议动作：{first_action}。",
    ]


def _report_ready_bullets(
    *,
    intent: str,
    country: str,
    question: str = "",
    alignment: EvidenceAlignment,
    implications: list[str],
    actions: list[RecommendedAction],
    refs: list[dict[str, Any]],
    evidence_package: dict[str, Any],
    method: BusinessMethodDistillation | None = None,
) -> list[str]:
    ref_note = f"（{_evidence_count_note(len(refs))}）" if refs else "（当前缺少可引用证据）"
    alignment_note = _alignment_label(alignment["status"])
    country_label = _country_label(country or "当前市场")
    first_action = _clean_action_text(actions[0]["action"] if actions else "")
    if intent == "pricing_analysis" and method:
        playbook = method.get("pricingPlaybook") if isinstance(method.get("pricingPlaybook"), dict) else {}
        price_corridor = method.get("priceCorridor") if isinstance(method.get("priceCorridor"), dict) else {}
        version_strategy = method.get("versionStrategy") if isinstance(method.get("versionStrategy"), dict) else {}
        model = str(method.get("model") or _pricing_subject_label(evidence_package) or "目标车型").strip()
        main_trim_price = str(price_corridor.get("mainTrimPrice") or "").strip()
        price_gap = _short_price_gap(str(version_strategy.get("priceGap") or price_corridor.get("priceGap") or ""))
        pva_coverage = _short_percentage(str(version_strategy.get("pvaCoverage") or ""))
        value_note_parts = []
        if main_trim_price:
            value_note_parts.append(f"主销高配参考 {main_trim_price}")
        if price_gap:
            value_note_parts.append(f"高低配价差 {price_gap}")
        if pva_coverage:
            value_note_parts.append(f"PVA 覆盖 {pva_coverage}")
        value_note = f"；{'，'.join(value_note_parts)}" if value_note_parts else ""
        warnings_note = (
            "；仍需补官方 MSRP、竞品价格和 PVA 口径交叉验证"
            if method.get("dataQualityWarnings")
            else ""
        )
        verified_lines = _pricing_verified_evidence_lines(evidence_package, limit=2)
        hypothesis_lines = _pricing_user_material_hypothesis_lines(evidence_package, limit=3)
        verified_note = "；".join(verified_lines) if verified_lines else "本轮未拿到本车型/核心竞品官方当前 MSRP。"
        positioning = str(price_corridor.get("positioning") or "低配锚点 + 高配主推").strip()
        hypothesis_note = "；".join(hypothesis_lines) if hypothesis_lines else f"{model} 用户材料定位假设：{positioning}"
        product_value = str(playbook.get("product_value_delta") or "配置价值需要通过可感知高配和用户场景验证。").strip()
        sales_talk = playbook.get("sales_talk_track") if isinstance(playbook.get("sales_talk_track"), list) else []
        sales_talk_text = "、".join(str(item or "").strip() for item in sales_talk if str(item or "").strip()) or "可见配置、高价值感、本地使用场景"
        bullets = [
            f"{country_label} {model} 定价页应先写成验证版：低配锚点、高配主推可以作为假设，但最终 MSRP 需要官方价格和竞品走廊证明{ref_note}。",
            f"已验证证据：{verified_note}。",
            f"用户材料价格假设（用户材料假设）：{hypothesis_note}{value_note}{warnings_note}。",
            f"配置价值：{product_value} 销售话术聚焦“{sales_talk_text}”。",
            f"缺口/下一步：补齐官方 MSRP、核心竞品当前价格、月供/RV、PVA 口径和配置差异；下一步应 {first_action or '补齐竞品价格、PVA 和税费口径'}。",
        ]
    elif intent == "pricing_analysis":
        bullets = [
            f"{country_label} 定价逻辑应先验证目标车型的价格走廊、竞品池、配置价值和用户购买场景{ref_note}。",
            f"市场窗口、竞品走廊和配置差异要一起验证；若 MSRP、竞品价格、leasing/RV 或配置估值缺失，不能直接给确定价格。",
            f"建议动作：{first_action or '生成价格矩阵并补齐竞品证据'}，并把竞品格局、配置价值、月供/company car 和风险边界写成一页汇报。",
        ]
    elif intent == "market_overview":
        bullets = _question_specific_market_overview_bullets(
            country_label=country_label,
            question=question,
            alignment_note=alignment_note,
            actions=actions,
            ref_note=ref_note,
            evidence_package=evidence_package,
            method=method,
        ) or [
            f"{_market_label(country_label)}判断应落到机会 segment、动力结构和车型进入顺序，而不是只复述销量{ref_note}。",
            f"证据状态为{alignment_note}，当前结论需要同时保留内部结构化数据和外部研究边界。",
            f"建议动作：{first_action or '生成一页市场机会框架'}。",
        ]
    elif intent == "news_policy_search":
        bullets = _question_specific_policy_bullets(
            country_label=country_label,
            question=question,
            alignment_note=alignment_note,
            actions=actions,
            ref_note=ref_note,
            evidence_package=evidence_package,
        ) or [
            f"{country_label} 政策/新闻影响应拆为车型、价格门槛、动力路线、零售和公司车场景{ref_note}。",
            f"证据状态为{alignment_note}，没有官方来源日期时不能写成确定政策结论。",
            f"建议动作：{actions[0]['action'] if actions else '查政策原文并交叉验证车型价格'}。",
        ]
    elif intent == "competitor_compare":
        question_text = _normalize_question_text(question)
        has_requested_pair = _is_competitor_report_scope(evidence_package, question_text)
        competitor_fallback = (
            _competitor_report_action_label(evidence_package, question_text)
            if has_requested_pair
            else "生成竞品对比表并补齐价格/配置证据"
        )
        competitor_action = _topic_action(
            actions,
            ("竞品", "对标", "compare", "competitor", "定位", "vs", "决策矩阵"),
            competitor_fallback,
            skip_source_repair=True,
        )
        bullets = [
            f"{_market_label(country_label)}的竞品判断应先锁定竞品池，再拆价格、尺寸/级别、动力、配置和用户场景{ref_note}。",
            f"结论要落成定位话术：正面对抗、错位竞争或价格锚点，而不是只列车型名称；当前证据状态为{alignment_note}。",
            f"建议动作：{competitor_action}，并输出可赢点、短板和销售话术。",
        ]
    elif intent == "configuration_analysis":
        question_text = _normalize_question_text(question)
        config_tokens = (
            ("冬季", "80", "电池", "热泵", "座椅", "充电", "adas", "winter")
            if ("80kwh" in question_text or "80 kwh" in question_text or "冬季" in question_text or "winter" in question_text)
            else ("配置", "configuration", "feature")
        )
        config_action = (
            "生成配置差异矩阵和主销配置建议"
            if ("80kwh" in question_text or "80 kwh" in question_text or "冬季" in question_text or "winter" in question_text)
            else _topic_action(actions, config_tokens, "生成配置差异矩阵和主销配置建议")
        )
        bullets = [
            f"{_market_label(country_label)}的配置判断必须连接真实使用场景：冬季、续航、充电、拖车/载重、ADAS 和家庭/公司车用途{ref_note}。",
            f"不能只说“配置更高”，要拆成 must-have、visible value、nice-to-have 和 cost/risk；当前证据状态为{alignment_note}。",
            f"建议动作：{config_action}，把配置差异转成用户价值和版本策略。",
        ]
    elif intent == "inventory_analysis":
        question_text = _normalize_question_text(question)
        if _is_pi_market_split_question(question_text):
            first_inventory_action = actions[0]["action"] if actions else "定义 PI header + market overlay + vehicle/material generation mapping"
            market_scope = _pi_market_scope_label(country_label, question_text)
            bullets = [
                f"{market_scope} 合并 PI 可以做共用计划/产品信息层，但车辆生成、物料号、市场合规、价格、订单和库存生命周期必须按市场拆分{ref_note}。",
                f"正确结构应是 PI header + market overlay + materialCode / vehicle generation mapping；不能用合并 PI 覆盖 SE/FI 的市场差异，当前证据状态为{alignment_note}。",
                f"建议动作：{first_inventory_action}，再补 SE/FI 底表验证每个市场的物料、车辆生成和生命周期规则。",
            ]
        else:
            bullets = [
                f"{country_label} BOM/库存问题应先建立实体关系：车型版本、物料号、市场、颜色、PI、订单和客户可编辑数量{ref_note}。",
                f"一个版型多个物料号通常不能直接判错，必须看生命周期、颜色/内饰组合、市场差异、配置包和订单状态；当前证据状态为{alignment_note}。",
                f"建议动作：{actions[0]['action'] if actions else '画实体关系并定义物料号生命周期'}，再补底表字段验证异常规则。",
            ]
    elif intent == "report_generation":
        bullets = _report_generation_bullets(
            country_label=country_label,
            question=question,
            alignment_note=alignment_note,
            implications=implications,
            actions=actions,
            refs=refs,
            evidence_package=evidence_package,
            method=method,
            ref_note=ref_note,
        )
    elif intent == "voc_analysis":
        bullets = _question_specific_voc_bullets(
            country_label=country_label,
            question=question,
            alignment_note=alignment_note,
            actions=actions,
            ref_note=ref_note,
        ) or [
            f"{country_label} VOC 判断要区分真实用户痛点、媒体观点、论坛噪音和可转化卖点{ref_note}。",
            f"没有可追溯来源时不能声称高频，只能给验证假设、检索路径和产品含义；当前证据状态为{alignment_note}。",
            f"建议动作：{actions[0]['action'] if actions else '补论坛/媒体/VOC 来源并按主题聚类'}。",
        ]
    else:
        first_implication = implications[0] if implications else "当前需要把证据转成业务动作。"
        bullets = [
            f"{_market_possessive(country_label)}核心判断：{first_implication} {ref_note}。",
            f"证据状态为{alignment_note}，结论应保留来源和缺口边界。",
            f"建议动作：{actions[0]['action'] if actions else '补齐证据后输出汇报结构'}。",
        ]
    return _dedupe(bullets)


def _report_generation_bullets(
    *,
    country_label: str,
    question: str,
    alignment_note: str,
    implications: list[str],
    actions: list[RecommendedAction],
    refs: list[dict[str, Any]],
    evidence_package: dict[str, Any],
    method: BusinessMethodDistillation | None,
    ref_note: str,
) -> list[str]:
    text = _normalize_question_text(question)
    first_action = actions[0]["action"] if actions else ""
    if _is_generic_report_action(first_action):
        first_action = ""
    title = _report_generation_title(
        country_label=country_label,
        question=question,
        method=method,
        evidence_package=evidence_package,
    )
    if method and _question_mentions_method_model(method, text):
        model = str(method.get("model") or "目标车型").strip()
        price_corridor = method.get("priceCorridor") if isinstance(method.get("priceCorridor"), dict) else {}
        positioning = str(price_corridor.get("positioning") or "低配锚点 + 高配主推").strip()
        key_message = (
            f"{model} 定价页应先做成“验证版价格走廊”："
            "低配锚点、高配主推是用户材料假设，最终价格必须等官方 MSRP、竞品价和月供/RV 补齐。"
        )
        verified_lines = _pricing_verified_evidence_lines(evidence_package, limit=2)
        hypothesis_lines = _pricing_user_material_hypothesis_lines(evidence_package, limit=3)
        verified_note = "；".join(verified_lines) if verified_lines else "本轮未拿到本车型/核心竞品官方当前 MSRP。"
        hypothesis_note = "；".join(hypothesis_lines) if hypothesis_lines else f"{model} 用户材料定位假设：{positioning}"
        evidence = (
            f"Evidence：已验证证据：{verified_note}；用户材料假设：{hypothesis_note}；"
            f"缺口：官方 MSRP、竞品当前价格、月供/RV、PVA 口径和配置差异仍需补齐；证据状态为{alignment_note}{ref_note}。"
        )
        implication = (
            "Product implication：销售话术可以先围绕可见高配和省心价值组织，但页面必须标注哪些是材料假设、哪些是工具已验证数据。"
        )
        next_action = first_action or "补齐当前 MSRP、竞品价格走廊、月供/company car 和 PVA 证据后出最终页"
    elif _is_policy_report_context(text, evidence_package):
        return _policy_report_generation_bullets(
            country_label=country_label,
            question_text=text,
            alignment_note=alignment_note,
            actions=actions,
            refs=refs,
            evidence_package=evidence_package,
            ref_note=ref_note,
        )
    elif _is_bev_penetration_report(text):
        evidence_brief = _bev_penetration_report_evidence_brief_from_refs(refs)
        key_message = (
            f"{country_label} BEV 渗透率变化这页应定位为产品定义验证页：先看趋势、细分市场、政策/价格/供给驱动，"
            "再转成续航、充电、冬季包、价格门槛和公司车场景动作。"
        )
        evidence = evidence_brief or (
            f"Evidence：当前{ref_note}，正式汇报需补 BEV 年/月度渗透率、SUV A0/A 与 SUV A 细分、政策日期、"
            f"价格带和车型供给证据；证据状态为{alignment_note}。"
        )
        implication = (
            "Product implication：不要只说 BEV 份额变化，要判断哪些产品定义项必须前置，哪些 HEV/PHEV 场景仍可保留。"
        )
        next_action = (
            first_action
            if _action_matches_topic(first_action, ("bev", "渗透", "趋势", "产品定义", "policy", "政策"))
            else "补齐 BEV 趋势和驱动因素证据，再生成一页产品定义建议 PPT block"
        )
    elif _generic_competitor_evidence_brief(
        plan={
            "intent": "report_generation",
            "country": country_label,
            "executiveConclusion": "",
            "internalEvidenceSummary": "",
            "externalEvidenceSummary": "",
            "evidenceAlignment": {
                "status": "partially_aligned",
                "summary": "",
                "internalSignal": "",
                "externalSignal": "",
            },
            "businessImplications": implications,
            "recommendedActions": actions,
            "risksAndMissingEvidence": [],
            "reportReadyBullets": [],
            "insightCardIds": [],
            "methodDistillation": method,
        },
        evidence_package=evidence_package,
        question_text=_generic_report_scope_text(evidence_package, text),
        include_empty_hypothesis=True,
    ):
        generic_parts = _generic_report_generation_parts(
            plan={
                "intent": "report_generation",
                "country": country_label,
                "executiveConclusion": "",
                "internalEvidenceSummary": "",
                "externalEvidenceSummary": "",
                "evidenceAlignment": {
                    "status": "partially_aligned",
                    "summary": "",
                    "internalSignal": "",
                    "externalSignal": "",
                },
                "businessImplications": implications,
                "recommendedActions": actions,
                "risksAndMissingEvidence": [],
                "reportReadyBullets": [],
                "insightCardIds": [],
                "methodDistillation": method,
            },
            evidence_package=evidence_package,
            country_label=country_label,
            question_text=text,
            action=first_action,
            alignment_note=alignment_note,
            evidence_note=_evidence_count_note(len(refs)),
            confidence_note=_confidence_label(str(evidence_package.get("confidence") or "low")),
        )
        key_message = generic_parts["keyMessage"]
        evidence = generic_parts["evidence"]
        implication = generic_parts["implication"]
        next_action = generic_parts["nextAction"].removeprefix("Next action：")
    else:
        generic_parts = _generic_report_generation_parts(
            plan={
                "intent": "report_generation",
                "country": country_label,
                "executiveConclusion": "",
                "internalEvidenceSummary": "",
                "externalEvidenceSummary": "",
                "evidenceAlignment": {
                    "status": "partially_aligned",
                    "summary": "",
                    "internalSignal": "",
                    "externalSignal": "",
                },
                "businessImplications": implications,
                "recommendedActions": actions,
                "risksAndMissingEvidence": [],
                "reportReadyBullets": [],
                "insightCardIds": [],
                "methodDistillation": method,
            },
            evidence_package=evidence_package,
            country_label=country_label,
            question_text=question,
            action=first_action,
            alignment_note=alignment_note,
            evidence_note=_evidence_count_note(len(refs)),
            confidence_note=_confidence_label(str(evidence_package.get("confidence") or "low")),
        )
        if generic_parts:
            key_message = generic_parts["keyMessage"]
            evidence = generic_parts["evidence"]
            implication = generic_parts["implication"]
            next_action = generic_parts["nextAction"].removeprefix("Next action：")
        else:
            implication = _strip_terminal_punctuation(_first_non_empty(implications))
            key_message = (
                implication
                or f"{country_label} 这页报告要先给业务判断，再把证据、产品含义和下一步动作压成一页。"
            )
            evidence = f"Evidence：{_evidence_label_note(refs)}；证据状态为{alignment_note}{ref_note}。"
            implication = f"Product implication：{implication or '把结论转成产品、价格、配置、渠道或汇报动作。'}"
            next_action = first_action or "补齐关键证据并输出 PPT-ready block"
    return _dedupe([
        f"Title：{title}",
        f"Key message：{key_message}",
        evidence,
        implication,
        f"Next action：{next_action}",
    ])


def _policy_report_generation_bullets(
    *,
    country_label: str,
    question_text: str,
    alignment_note: str,
    actions: list[RecommendedAction],
    refs: list[dict[str, Any]],
    evidence_package: dict[str, Any],
    ref_note: str,
) -> list[str]:
    first_action = _clean_action_text(actions[0]["action"] if actions else "")
    title = _policy_report_title(country_label, question_text)
    source_brief = _policy_external_evidence_brief(evidence_package)
    market_brief = _policy_market_context_brief(evidence_package, question_text)
    missing_boundary = _policy_missing_boundary_brief(evidence_package)
    evidence_parts = []
    if source_brief:
        evidence_parts.append(source_brief.removeprefix("已查证据：").rstrip("。"))
    if market_brief:
        evidence_parts.append(market_brief.removeprefix("市场证据：").rstrip("。"))
    if not evidence_parts:
        evidence_parts.append(f"当前{ref_note}，仍需补官方来源、发布日期、资格门槛和车型适用证据")
    missing_note = f"；{missing_boundary.rstrip('。')}" if missing_boundary else ""
    if "elbilspremien" in question_text:
        key_message = (
            f"{country_label} Elbilspremien 2026 不能直接点名确定受影响车型；"
            "应先做官方来源校验，再用 JATO 市场结构筛选 BEV SUV A0/A 等候选影响池。"
        )
        implication = (
            "Product implication：低配/主销版可能承担资格锚点，高配必须证明补贴外配置价值；"
            "但车型名单、价格上限和交付时间要等政策来源确认后才能固化。"
        )
        fallback_action = "补齐官方政策原文、发布日期、资格/价格上限，并生成受影响车型矩阵"
        next_action = (
            first_action
            if _action_matches_topic(first_action, ("elbilspremien", "受影响", "车型", "资格", "价格上限"))
            else fallback_action
        )
    elif "company car" in question_text and "bev" in question_text and "phev" in question_text:
        key_message = (
            f"{country_label} company car 影响不能只按动力标签判断；"
            "必须把 benefit tax、月供、残值、充电条件和实际里程放进同一张 TCO 表。"
        )
        implication = (
            "Product implication：BEV 与 PHEV 应分成公司车/TCO 场景验证，而不是在报告里写成单一政策利好。"
        )
        fallback_action = "建立 BEV/PHEV company car benefit 对比表"
        next_action = first_action or fallback_action
    elif ("co2" in question_text or "co₂" in question_text) and "phev" in question_text:
        key_message = (
            f"{country_label} PHEV CO2 阶梯只是入场条件；"
            "是否有利取决于税费公式、company car 场景、真实充电行为和 TCO。"
        )
        implication = (
            "Product implication：PHEV 页要把认证 CO2、税费、月供/RV 和用户里程合并验证，不能只用排放标签下结论。"
        )
        fallback_action = "核对 PHEV CO2 税率阶梯、company car 公式和车型认证值"
        next_action = first_action or fallback_action
    else:
        key_message = (
            f"{country_label} 政策/外部来源页应先证明来源、日期和适用对象，再转成车型、价格、动力和渠道动作。"
        )
        implication = (
            "Product implication：没有来源日期和资格口径时，只能输出影响路径、候选车型池和补证动作，不能写确定名单或确定价格约束。"
        )
        fallback_action = "补官方来源、发布日期和 JATO 交叉验证"
        next_action = first_action or fallback_action
    return _dedupe([
        f"Title：{title}",
        f"Key message：{key_message}",
        f"Evidence：{'；'.join(evidence_parts[:3])}{missing_note}；证据状态为{alignment_note}。",
        implication,
        f"Next action：{next_action}",
    ])


def _report_generation_title(
    *,
    country_label: str,
    question: str,
    method: BusinessMethodDistillation | None,
    evidence_package: dict[str, Any],
) -> str:
    text = _normalize_question_text(question)
    if method and _question_mentions_method_model(method, text):
        model = str(method.get("model") or "目标车型").strip()
        return f"{country_label} {model} 验证版定价逻辑"
    if _is_bev_penetration_report(text):
        return f"{country_label} BEV 渗透率变化对产品定义的影响"
    if _is_policy_report_context(text, evidence_package):
        return _policy_report_title(country_label, text)
    if _is_competitor_report_scope(evidence_package, text):
        return f"{country_label} {_competitor_report_subject_label(evidence_package, text)} 竞品定位页"
    if question:
        return _display_question_subject(question, max_chars=80)
    return f"{country_label} 市场分析一页汇报"


def _is_policy_report_context(text: str, evidence_package: dict[str, Any]) -> bool:
    context = _normalize_question_text(text)
    if _contains_any(
        context,
        (
            "elbilspremien",
            "policy",
            "policies",
            "incentive",
            "subsidy",
            "bonus",
            "benefit",
            "company car",
            "co2",
            "co₂",
            "tax",
            "news",
            "政策",
            "补贴",
            "税",
            "新闻",
            "受影响",
            "价格上限",
            "资格",
            "车型资格",
        ),
    ):
        return True
    if _has_target_policy_source_gap(evidence_package):
        return True
    return False


def _policy_report_title(country_label: str, text: str) -> str:
    if "elbilspremien" in text:
        return f"{country_label} Elbilspremien 2026 影响判断"
    if "company car" in text and "bev" in text and "phev" in text:
        return f"{country_label} BEV/PHEV company car 影响判断"
    if ("co2" in text or "co₂" in text) and "phev" in text:
        return f"{country_label} PHEV CO2 税费影响判断"
    if "bev" in text and ("补贴" in text or "subsidy" in text or "incentive" in text or "bonus" in text):
        return f"{country_label} BEV 补贴影响判断"
    return f"{country_label} 政策/外部来源影响判断"


def _is_bev_penetration_report(text: str) -> bool:
    return "bev" in text and _contains_any(text, ("渗透", "penetration", "增长", "变化"))


def _bev_penetration_report_direct_answer(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    action: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    refs = _all_evidence_refs(evidence_package)
    evidence_brief = _bev_penetration_report_evidence_brief_from_refs(refs)
    display_note = _artifact_visual_backbone_note("report_generation", evidence_package)
    if evidence_brief:
        parts = [
            f"直接结论：{country_label} BEV 渗透率变化已经可以先做一页产品定义验证页初稿，但页面主结论不能停在“BEV 增长”；要把动力结构、趋势/年度背景和主销车型转成产品动作。",
            evidence_brief,
            "产品定义动作：BEV 线优先前置真实续航、低温充电/热管理、冬季包、价格门槛和 company car TCO；HEV/PHEV 场景保留为无稳定充电、长途和低风险替代，不应在 BEV 页里被完全抹掉。",
            "证据边界：如果缺完整年/月度趋势、SUV A0/A 细分、政策日期或价格带证据，汇报页只能写产品定义方向，不能写成最终历史归因。",
            display_note,
            f"下一步执行：{action or '补齐 BEV 趋势和驱动因素证据，再生成一页产品定义建议 PPT block'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
        ]
        return _clean_business_text(_bounded_direct_text(parts, max_chars=1200))
    return _clean_business_text(_bounded_direct_text([
        f"直接结论：{country_label} BEV 渗透率变化这页应作为产品定义验证页，而不是只做数据摘录或模板说明。",
        "Key message 要聚焦趋势、细分市场、政策/价格/供给驱动，再落到续航、充电、冬季包、价格门槛和公司车场景动作；当前仍缺完整年/月度趋势和驱动因素证据，不能写确定历史趋势。",
        display_note,
        f"下一步执行：{action or '补齐 BEV 趋势和驱动因素证据，再生成一页产品定义建议 PPT block'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ], max_chars=1000))


def _bev_penetration_report_evidence_brief_from_refs(refs: list[dict[str, Any]]) -> str:
    powertrain = _powertrain_stats_from_refs(refs)
    powertrain_parts = []
    for fuel in ("BEV", "PHEV", "HEV"):
        line = _report_powertrain_line(fuel, powertrain.get(fuel, {}))
        if line:
            powertrain_parts.append(line)
    trend = _report_trend_points(refs, preferred_metric="bevshare")
    annual_totals = [] if trend else _report_annual_total_points(refs)
    top_models = _report_top_model_points(refs)
    evidence_parts: list[str] = []
    if powertrain_parts:
        evidence_parts.append("动力结构：" + "；".join(powertrain_parts))
    if trend:
        evidence_parts.append("趋势点：" + "，".join(trend))
    elif annual_totals:
        evidence_parts.append("年度总量背景：" + "，".join(annual_totals))
    if top_models:
        evidence_parts.append("主销车型：" + "，".join(top_models))
    if not evidence_parts:
        return ""
    return "Evidence：" + "。".join(evidence_parts) + "。"


def _report_powertrain_line(fuel: str, stats: dict[str, str]) -> str:
    if not stats:
        return ""
    parts: list[str] = []
    if stats.get("sales"):
        parts.append(str(stats["sales"]))
    if stats.get("share"):
        parts.append(f"份额 {stats['share']}")
    if not parts:
        return fuel
    return f"{fuel} " + "，".join(parts)


def _report_trend_points(refs: list[dict[str, Any]], *, preferred_metric: str) -> list[str]:
    points: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        if "contextSnapshot.yearSeries." not in label:
            continue
        parts = [part for part in label.split(".") if part]
        if len(parts) < 4:
            continue
        period = parts[-2]
        metric = parts[-1]
        value = _format_evidence_ref_value(ref)
        if not value:
            continue
        item = f"{period} {metric} {value}"
        if preferred_metric.casefold() in metric.casefold():
            points.append(item)
    return points[:4]


def _report_annual_total_points(refs: list[dict[str, Any]]) -> list[str]:
    points: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        if "contextSnapshot.yearSeries." not in label:
            continue
        parts = [part for part in label.split(".") if part]
        if len(parts) < 4:
            continue
        period = parts[-2]
        metric = parts[-1].casefold()
        if metric not in {"value", "sales", "volume", "registrations", "total"}:
            continue
        value = _format_evidence_ref_value(ref)
        if value:
            points.append(f"{period} {value}")
        if len(points) >= 4:
            break
    return points


def _report_top_model_points(refs: list[dict[str, Any]]) -> list[str]:
    models: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        if "contextSnapshot.topModels." not in label or not label.endswith(".sales"):
            continue
        parts = [part for part in label.split(".") if part]
        if len(parts) < 4:
            continue
        model = parts[-2]
        value = _format_evidence_ref_value(ref)
        if model and value:
            models.append(f"{model} {value}")
    return models[:4]


def _is_hev_phev_route_question(text: str) -> bool:
    return (
        "hev" in text
        and "phev" in text
        and _contains_any(text, ("适合", "推", "主推", "应该", "路线", "route", "prioritize", "push", "lead"))
    )


def _is_suv_a0_a_structure_question(text: str) -> bool:
    return (
        "suv" in text
        and _contains_any(text, ("a0", "a 级", "a级", "suv a", "segment", "细分", "级别"))
        and _contains_any(text, ("主销", "结构", "为什么", "原因", "why", "driver", "drivers", "集中"))
    )


def _is_pi_market_split_question(text: str) -> bool:
    return (
        "pi" in text
        and _contains_any(text, ("合并", "共用", "combined", "merge", "shared"))
        and _contains_any(text, ("分市场", "按市场", "市场生成", "se/fi", "se fi", "瑞典", "芬兰", "market"))
        and _contains_any(text, ("车辆", "生成", "物料", "bom", "material", "vehicle"))
    )


def _pi_market_scope_label(country_label: str, text: str) -> str:
    normalized = str(text or "").casefold()
    if (
        "se/fi" in normalized
        or "se fi" in normalized
        or ("瑞典" in normalized and "芬兰" in normalized)
        or ("sweden" in normalized and "finland" in normalized)
    ):
        return "SE/FI"
    return str(country_label or "").strip() or "目标市场"


def _is_generic_report_action(action: str) -> bool:
    lower = str(action or "").strip().lower()
    return lower in {
        "",
        "生成一页 ppt block",
        "generate one ppt block",
        "生成一页 ppt-ready block",
    }


def _action_matches_topic(action: str, tokens: tuple[str, ...]) -> bool:
    lower = str(action or "").lower()
    return any(token in lower for token in tokens)


def _topic_action(
    actions: list[RecommendedAction],
    tokens: tuple[str, ...],
    fallback: str,
    *,
    skip_source_repair: bool = False,
) -> str:
    for action in actions:
        value = _clean_action_text(str(action.get("action") or ""))
        if skip_source_repair and _looks_like_source_repair_action(value):
            continue
        if value and _action_matches_topic(value, tokens):
            return value
    return fallback


def _public_direct_action_summary(value: str) -> str:
    text = _clean_business_text(str(value or ""))
    if not text:
        return ""
    lowered = text.casefold()
    if "msrp 来源验证表" in lowered or "来源草稿" in text or "官方价格搜索候选" in text:
        return "先审核本车型与核心竞品官方价格来源，写入当前价格记录后，再重算价格走廊、配置价值和月供/残值。"
    if "msrp review queue" in lowered:
        return "先审核待确认 MSRP 观察，生成当前价格记录后再输出确定价格走廊和主销版本建议。"
    replacements = {
        "current price": "当前价格记录",
        "current MSRP": "当前官方 MSRP",
        "campaign support": "促销支持",
        "dry-run": "抽取前审核",
        "ingest": "写入",
    }
    for raw, replacement in replacements.items():
        text = re.sub(raw, replacement, text, flags=re.IGNORECASE)
    return text


def _playbook_display_title(title: str) -> str:
    mapping = {
        "Pricing corridor playbook": "定价走廊方法",
        "Market opportunity playbook": "市场机会方法",
        "Competitor positioning playbook": "竞品定位方法",
        "Configuration value playbook": "配置价值方法",
        "Policy impact playbook": "政策影响方法",
        "Inventory and BOM logic playbook": "库存/BOM 方法",
        "Inventory / BOM playbook": "库存/BOM 方法",
        "VOC evidence playbook": "VOC 证据方法",
        "PPT-ready report playbook": "汇报生成方法",
    }
    return mapping.get(str(title or "").strip(), str(title or "").strip() or "业务分析方法")


def _executive_conclusion(
    *,
    intent: str,
    country: str,
    confidence: str,
    alignment: EvidenceAlignment,
    refs: int,
    first_action: str,
    first_implication: str,
    answer_status: str,
    method: BusinessMethodDistillation | None = None,
    evidence_package: dict[str, Any] | None = None,
    question: str = "",
) -> str:
    country_label = _country_label(country or "当前市场")
    label = _INTENT_LABELS.get(intent, "业务分析")
    first_action = _clean_action_text(first_action)
    first_implication = _strip_terminal_punctuation(first_implication)
    if intent == "pricing_analysis":
        target_verdict = _pricing_target_price_verdict(
            evidence_package or {},
            country_label=country_label,
            confidence=confidence,
            alignment=alignment,
            refs=refs,
            first_action=first_action,
        )
        if target_verdict:
            return target_verdict
    if intent == "market_overview" and _is_hev_phev_route_question(_normalize_question_text(question)):
        return _hev_phev_route_executive_conclusion(
            country_label=country_label,
            confidence=confidence,
            alignment=alignment,
            refs=refs,
            first_action=first_action,
            evidence_package=evidence_package or {},
        )
    if intent == "market_overview":
        cross_tab_conclusion = _market_powertrain_opportunity_cross_tab_conclusion(
            text=_normalize_question_text(question),
            country_label=country_label,
            evidence_package=evidence_package or {},
            refs=refs,
            confidence_note=_confidence_label(confidence),
            alignment_note=_alignment_label(alignment["status"]),
            action=first_action,
            method=method,
        )
        if cross_tab_conclusion:
            return cross_tab_conclusion
    if intent == "market_overview" and _has_market_decision_blocking_gap(evidence_package or {}):
        market_fit_conclusion = _market_fit_gap_conclusion(
            country_label=country_label,
            evidence_package=evidence_package or {},
            question=question,
            refs=refs,
            confidence=confidence,
            first_action=first_action,
            method=method,
        )
        if market_fit_conclusion:
            return market_fit_conclusion
    if alignment["status"] == "insufficient" or answer_status in {"insufficient_evidence", "tool_failed", "needs_user_data"}:
        missing_note = _missing_evidence_note(evidence_package or {})
        if intent == "voc_analysis" and "v2h" in _normalize_question_text(question):
            return _v2h_voc_insufficient_evidence_conclusion(
                country_label=country_label,
                missing_note=missing_note,
                first_action=first_action,
                refs=refs,
                confidence=confidence,
            )
        if method and intent == "pricing_analysis":
            return (
                f"直接结论：{_market_possessive(country_label)} {method['model']} 定价暂时不能给最新确定数字，因为缺少{missing_note}；"
                f"只能把用户材料蒸馏出的“{method['priceCorridor']['positioning']}”作为定位假设推进，不是当前官方 MSRP 结论。"
                "先用低配做价格锚点、高配做主推版本的假设，补齐本车型官方 MSRP、竞品走廊、月供/company car、PVA 和税费口径后再下最终价格。"
                "不能把材料主销价或材料价格走廊写成最终定价。"
            f"{_evidence_availability_note(refs)}，置信度{_confidence_label(confidence)}。"
            )
        return _insufficient_evidence_bridge_conclusion(
            intent=intent,
            country_label=country_label,
            label=label,
            missing_note=missing_note,
            first_implication=first_implication,
            first_action=first_action,
            refs=refs,
            confidence=confidence,
            evidence_package=evidence_package or {},
        )
    if intent == "market_overview" and _has_market_decision_blocking_gap(evidence_package or {}):
        missing_note = _missing_evidence_note(evidence_package or {})
        target = _market_fit_target_label(question, method, evidence_package or {})
        return (
            f"直接结论：{_market_possessive(country_label)}{label}目前不能下“适合/不适合”的确定结论，因为缺少{missing_note}；"
            f"当前能做的是把外部来源作为背景，先验证目标动力销量/份额、目标级别结构、竞品池和 {target} 价格/配置证据。"
            f"下一步执行：{first_action or '补齐 HEV 市场和竞品证据'}。"
            f"{_evidence_availability_note(refs)}，置信度{_confidence_label(confidence)}。"
        )
    question_specific = _question_specific_executive_conclusion(
        intent=intent,
        country_label=country_label,
        confidence=confidence,
        alignment=alignment,
        refs=refs,
        first_action=first_action,
        first_implication=first_implication,
        method=method,
        evidence_package=evidence_package or {},
        question=question,
    )
    if question_specific:
        return question_specific
    evidence_first = _evidence_first_executive_conclusion(
        intent=intent,
        country_label=country_label,
        label=label,
        confidence=confidence,
        alignment=alignment,
        refs=refs,
        first_action=first_action,
        first_implication=first_implication,
        method=method,
        evidence_package=evidence_package or {},
    )
    if evidence_first:
        return evidence_first
    if method and intent == "pricing_analysis":
        return (
            f"直接结论：{_market_possessive(country_label)} {method['model']} 定价应按“{method['priceCorridor']['positioning']}”组织，"
            f"把市场窗口、竞品走廊、可感知配置价值和版本策略连成一条分析链。下一步应执行：{first_action or '补齐价格矩阵'}。"
            f"证据状态：{_alignment_label(alignment['status'])}，置信度{_confidence_label(confidence)}。"
        )
    if alignment["status"] == "conflicting":
        return (
            f"直接结论：{_market_possessive(country_label)} {label} 目前不能直接下确定判断，因为内部和外部证据存在冲突；"
            f"先做口径复核，再推进 {first_action or '下一步验证'}。当前置信度{_confidence_label(confidence)}。"
        )
    intent_specific = _intent_executive_conclusion(
        intent=intent,
        country_label=country_label,
        confidence=confidence,
        alignment=alignment,
        refs=refs,
        first_action=first_action,
        first_implication=first_implication,
    )
    if intent_specific:
        return intent_specific
    return (
        f"直接结论：{_market_possessive(country_label)} {label} 已有可追溯证据支撑，当前最重要的业务含义是"
        f" {first_implication or '把证据转成产品、价格或渠道动作'}；下一步应执行：{first_action or '形成可复用汇报页'}。"
        f"证据状态：{_alignment_label(alignment['status'])}，置信度{_confidence_label(confidence)}。"
        )


def _v2h_voc_insufficient_evidence_conclusion(
    *,
    country_label: str,
    missing_note: str,
    first_action: str,
    refs: int,
    confidence: str,
) -> str:
    action = first_action or f"抓取{_market_regional_scope(country_label)} V2H 用户原声和媒体测评证据"
    return _clean_business_text(
        _bounded_direct_text(
            [
                (
                    f"直接结论：{country_label} V2H 暂时不能定位为真实高频购买卖点；"
                    "它更适合作为“高感知但待验证”的技术加分项，用来支撑家庭能源、安全备份、冬季用车和科技形象叙事。"
                ),
                (
                    f"证据边界：当前缺少{missing_note}，所以不能声称用户已经高频购买或高频吐槽 V2H；"
                    "若只有市场结构或车型背景，只能作为代理判断，不是消费者调研结论。"
                ),
                (
                    f"下一步：{action}；同时先输出 V2H 主题假设、来源清单和产品含义框架，"
                    f"等可追溯 VOC 来源回来后再固化结论。{_intent_evidence_availability_note(refs, 'voc_analysis')}，"
                    f"置信度{_confidence_label(confidence)}。"
                ),
            ],
            max_chars=900,
        )
    )


def _missing_evidence_note(evidence_package: dict[str, Any]) -> str:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    names = [
        _missing_evidence_label(str(item.get("name") or "").strip())
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    if names:
        return "、".join(names[:3])
    return "可引用证据"


def _insufficient_evidence_bridge_conclusion(
    *,
    intent: str,
    country_label: str,
    label: str,
    missing_note: str,
    first_implication: str,
    first_action: str,
    refs: int,
    confidence: str,
    evidence_package: dict[str, Any],
) -> str:
    scope_note = _country_scope_mismatch_note(evidence_package)
    if scope_note:
        next_action = _country_scope_mismatch_next_action(
            country_label=country_label,
            intent=intent,
            first_action=first_action,
            evidence_package=evidence_package,
        )
        return _clean_business_text(
            _bounded_direct_text(
                [
                    (
                        f"直接结论：{_market_possessive(country_label)}{label}现在不能给确定数字，"
                        f"因为{scope_note}。"
                    ),
                    (
                        f"当前可判断：{_can_judge_without_full_evidence(intent, evidence_package, first_implication)}"
                    ),
                    (
                        f"下一步：{next_action}；同时先输出{_gap_interim_deliverable(intent)}，"
                        "等目标市场 evidenceRef 回来后再固化结论。"
                        f"{_intent_evidence_availability_note(refs, intent)}，置信度{_confidence_label(confidence)}。"
                    ),
                ],
                max_chars=950,
            )
        )
    blocked_conclusion = (
        "高频吐槽、具体配置痛点、用户场景频次或购买转化不能写成确定事实。"
        if intent == "voc_analysis"
        else "价格、销量、份额、政策日期、月供/RV、配置差异或竞品胜负不能写成确定事实。"
    )
    evidence_target = "可追溯 VOC 来源" if intent == "voc_analysis" else "evidenceRef"
    return _clean_business_text(
        _bounded_direct_text(
            [
                (
                    f"直接结论：{_market_possessive(country_label)}{label}现在不能给确定数字，"
                    f"但不应停在“证据不足”。当前可判断：{_can_judge_without_full_evidence(intent, evidence_package, first_implication)}"
                ),
                (
                    f"不能下结论：缺少{missing_note}，所以{blocked_conclusion}"
                ),
                (
                    f"下一步：{first_action or _default_gap_action(intent)}；"
                    f"同时先输出{_gap_interim_deliverable(intent)}，等{evidence_target}回来后再固化结论。"
                    f"{_intent_evidence_availability_note(refs, intent)}，置信度{_confidence_label(confidence)}。"
                ),
            ],
            max_chars=950,
        )
    )


def _can_judge_without_full_evidence(
    intent: str,
    evidence_package: dict[str, Any],
    first_implication: str,
) -> str:
    tools = evidence_tool_names(evidence_package)
    tool_note = ""
    if tools:
        tool_labels = _dedupe([_tool_business_label(item) for item in tools])[:3]
        tool_note = f"本轮已尝试 {'、'.join(tool_labels)}，"
    defaults = {
        "pricing_analysis": "可以先判断定价问题需要本车型价格、竞品走廊、月供/RV 和配置价值四类证据，并搭好价格矩阵。",
        "competitor_compare": "可以先判断对标关系应拆成主对标、价格锚点、配置校验锚点和销售替代对象。",
        "market_overview": "可以先判断市场问题应拆到规模、动力结构、级别结构、竞品池和产品动作。",
        "configuration_analysis": "可以先判断配置问题应落到用户场景、must-have、visible value、成本风险和版本策略。",
        "inventory_analysis": "可以先判断 BOM/库存问题应拆成车型版本、物料号、颜色、市场、PI 和生命周期关系。",
        "news_policy_search": "可以先判断政策问题应拆到官方来源、发布日期、适用对象、价格门槛和动力路线影响。",
        "voc_analysis": "可以先判断 VOC 问题应区分真实用户痛点、媒体观点、论坛噪音和可转化卖点；若当前只有市场结构证据，这只能作为代理判断，不是消费者调研结论。",
        "report_generation": "可以先判断汇报页需要 key message、evidence、product implication 和 next action 四段结构。",
    }
    implication = _strip_terminal_punctuation(first_implication)
    if implication and not _looks_like_generic_first_sentence(implication):
        return f"{tool_note}{implication}。"
    return f"{tool_note}{defaults.get(intent, '可以先确定分析框架、证据口径和下一步动作。')}"


def _country_scope_mismatch_note(evidence_package: dict[str, Any]) -> str:
    for diagnostics in _coverage_diagnostics(evidence_package):
        if str(diagnostics.get("diagnosis") or "").strip() != "country_scope_mismatch":
            continue
        requested = _country_label(str(diagnostics.get("requestedCountry") or ""))
        returned = _country_label(str(diagnostics.get("returnedCountry") or ""))
        if requested and returned:
            return f"本轮工具返回的是{returned}证据，不是用户请求的{requested}证据"
        if returned:
            return f"本轮工具返回的是{returned}证据，不是用户请求市场证据"
        return "本轮工具返回了非目标市场证据"
    return ""


def _country_scope_mismatch_next_action(
    *,
    country_label: str,
    intent: str,
    first_action: str,
    evidence_package: dict[str, Any],
) -> str:
    target_market = _country_label(str(country_label or evidence_package.get("country") or "当前市场"))
    required_tools = _missing_required_tool_names(evidence_package)
    if required_tools:
        tool_labels = _dedupe([_tool_business_label(tool) for tool in required_tools])[:3]
        return f"按{target_market}重新调用{'、'.join(tool_labels)}工具，并过滤非目标市场结果"
    action = _strip_terminal_punctuation(first_action)
    if action:
        return f"按{target_market}重跑数据查询后再执行：{action}"
    return f"按{target_market}重跑{_default_gap_action(intent)}，并过滤非目标市场结果"


def _coverage_diagnostics(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    tools = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        diagnostics = tool.get("coverageDiagnostics")
        if isinstance(diagnostics, dict):
            result.append(diagnostics)
    return result


def _missing_required_tool_names(evidence_package: dict[str, Any]) -> list[str]:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    result: list[str] = []
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name.startswith("missing_required_tool:"):
            continue
        tool_name = name.replace("missing_required_tool:", "", 1).strip()
        if tool_name and tool_name not in result:
            result.append(tool_name)
    return result


def _default_gap_action(intent: str) -> str:
    mapping = {
        "pricing_analysis": "补齐本车型官方 MSRP、竞品价格走廊、月供/RV 和配置价值证据",
        "competitor_compare": "补齐竞品池、价格/配置差异和销量/场景证据",
        "market_overview": "补齐市场规模、动力结构、级别结构和趋势图表",
        "configuration_analysis": "补齐版本/配置表、竞品配置差异和用户价值证据",
        "inventory_analysis": "补齐版本、物料号、颜色、市场和订单生命周期底表",
        "news_policy_search": "补官方来源、发布日期、资格门槛和 JATO 交叉验证",
        "voc_analysis": "补媒体测评、论坛评论、用户原声和来源日期",
        "report_generation": "补齐一页汇报所需的关键证据和引用来源",
    }
    return mapping.get(intent, "补齐核心证据")


def _gap_interim_deliverable(intent: str) -> str:
    mapping = {
        "pricing_analysis": "价格矩阵、竞品走廊和高配价值验证表",
        "competitor_compare": "竞品角色矩阵和可赢点/短板清单",
        "market_overview": "市场机会框架、优先 segment 假设和图表占位",
        "configuration_analysis": "must-have / visible value / optional 配置验证表",
        "inventory_analysis": "BOM 实体关系和异常生命周期表",
        "news_policy_search": "政策影响路径和来源查证清单",
        "voc_analysis": "VOC 主题假设、来源清单和产品含义框架",
        "report_generation": "PPT-ready block 草稿",
    }
    return mapping.get(intent, "业务框架和验证清单")


def _missing_evidence_names(evidence_package: dict[str, Any]) -> set[str]:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    return {
        str(item.get("name") or "").strip()
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    }


def _has_market_decision_blocking_gap(evidence_package: dict[str, Any]) -> bool:
    missing_names = _missing_evidence_names(evidence_package)
    return bool(
        missing_names
        & {
            "market_snapshot_data_unavailable",
            "jato_cross_check",
            "supporting_evidence",
        }
    )


def _market_fit_gap_conclusion(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    question: str,
    refs: int,
    confidence: str,
    first_action: str,
    method: BusinessMethodDistillation | None = None,
) -> str:
    text = _normalize_question_text(question)
    if not _is_market_fit_question(text):
        return ""
    if (
        not _has_missing_evidence(evidence_package, "market_snapshot_data_unavailable")
        or _market_fit_has_usable_internal_market_evidence(evidence_package)
    ):
        return ""
    powertrain = _market_opportunity_powertrain(evidence_package, text) or "目标动力"
    target = _market_fit_target_label(text, method, evidence_package, powertrain=powertrain)
    competitor_pool = _market_fit_competitor_pool_label(evidence_package, method=method)
    action = _market_fit_next_action(
        first_action,
        country_label,
        target=target,
        competitor_pool=competitor_pool,
        powertrain=powertrain,
    )
    return (
        f"直接结论：暂不能把{_market_label(country_label)}判定为 {target} 的已验证进入机会；"
        f"当前外部证据只能作为政策/电动化背景，不能证明 {powertrain} 需求、SUV A0/A 结构和 {target} 竞品定位已经成立。"
        f"可先保留“待验证机会”立场：如果后续数据证明 {powertrain} SUV A0/A 有稳定需求、{competitor_pool} 存在供给或价格窗口，"
        f"且 {target} 的价格、配置和渠道价值能够闭环，才适合进入产品与定价验证。"
        f"{_market_fit_evidence_note(evidence_package, refs)}，置信度{_confidence_label(confidence)}。"
        f"下一步执行：{action}，再决定是否进入定价页。"
    )


def _is_market_fit_question(text: str) -> bool:
    normalized = _normalize_question_text(text)
    if _is_hev_phev_route_question(normalized):
        return False
    has_fit_language = _contains_any(
        normalized,
        (
            "适合",
            "机会",
            "进入",
            "验证",
            "值得",
            "继续",
            "fit",
            "suitable",
            "validate",
            "validation",
            "worth",
            "why",
        ),
    )
    has_market_scope = _contains_any(
        normalized,
        ("市场", "market", "bev", "phev", "hev", "mhev", "ice", "suv", "产品线", "车型"),
    )
    return has_fit_language and has_market_scope


def _market_fit_evidence_note(evidence_package: dict[str, Any], refs: int) -> str:
    missing_note = _missing_evidence_note(evidence_package)
    if refs > 0:
        return f"当前有 {_evidence_count_note(refs)}，但缺少{missing_note}"
    return f"当前缺少可引用证据，尤其缺少{missing_note}"


def _market_fit_next_action(
    first_action: str,
    country_label: str,
    *,
    target: str = "目标产品",
    competitor_pool: str = "核心竞品池",
    powertrain: str = "目标动力",
) -> str:
    action = _strip_terminal_punctuation(first_action)
    generic_actions = {
        "",
        "拆到车型/品牌",
        "做邻国 side-by-side",
        "生成市场机会页",
        "按动力类型找进入点",
        "量化受影响细分市场，并生成市场机会视图",
    }
    if action in generic_actions:
        target_market = str(country_label or "").strip() or "当前市场"
        return (
            f"补齐{target_market} {powertrain} 市场规模、SUV A0/A 结构、"
            f"{competitor_pool} 和 {target} 价格/配置证据"
        )
    if action.startswith("补齐目标市场"):
        target_market = str(country_label or "").strip() or "当前市场"
        localized = action.replace("目标市场", target_market, 1)
        tail = f"{competitor_pool} 和 {target} 价格/配置证据"
        if tail not in localized:
            localized = f"{localized}、{tail}"
        return localized
    return action


def _missing_evidence_label(name: str) -> str:
    value = str(name or "").strip()
    if not value:
        return "证据缺口"
    normalized_value = value.replace(" ", "_")
    if value.startswith("missing_required_tool:"):
        tool_name = value.replace("missing_required_tool:", "", 1).strip()
        return f"{_tool_business_label(tool_name)}工具结果"
    if value.startswith("target_policy_source:"):
        return "目标政策原文/年份来源"
    if value == "coverage_diagnostic:country_scope_mismatch":
        return "目标市场数据"
    if value.startswith("coverage_diagnostic:no_current_prices"):
        return "当前价格覆盖缺口"
    if value.startswith("coverage_diagnostic:no_config_projects"):
        return "配置/版本差异覆盖缺口"
    if value in _GAP_LABELS:
        return _GAP_LABELS[value]
    if normalized_value in _GAP_LABELS:
        return _GAP_LABELS[normalized_value]
    if value.endswith("_weak_evidence_refs"):
        tool_name = value.replace("_weak_evidence_refs", "")
        return f"{_tool_business_label(tool_name)}证据不足"
    return value.replace("_", " ")


def _risk_display_name(name: str) -> str:
    return _missing_evidence_label(name)


def _risk_display_impact(risk: BusinessRisk) -> str:
    name = str(risk.get("name") or "")
    return _missing_impact(name, str(risk.get("impact") or ""))


def _missing_impact(name: str, fallback: str = "") -> str:
    lower = str(name or "").lower()
    if lower.startswith("missing_required_tool:"):
        tool_name = lower.replace("missing_required_tool:", "", 1).strip()
        return f"缺少{_tool_business_label(tool_name)}结果，当前结论只能给框架，不能给确定数字或确定排名。"
    if lower.startswith("target_policy_source:"):
        return "没有命中用户点名的政策名称和年份来源，政策影响只能写成候选判断，不能点名确定受益车型或约束价格。"
    if "coverage_diagnostic:country_scope_mismatch" in lower:
        return "工具返回了非目标市场证据，当前不能把这些数字写成用户请求市场的结论。"
    if "external_research_claims_unavailable" in lower:
        return "外部搜索只提供来源、日期或数量线索，还没有形成可引用的政策/VOC/新闻结论。"
    if "published_date" in lower:
        return "来源缺少发布日期，不能判断政策、新闻或 VOC 信号是否仍然有效。"
    if "minimum_external_sources" in lower:
        return "可用外部来源数量不足，不能把候选主题写成已验证高频事实。"
    if "coverage_diagnostic:no_current_prices" in lower:
        return "当前价格库没有覆盖请求车型或竞品，定价结论不能直接写成官方价格判断。"
    if "coverage_diagnostic:no_config_projects" in lower:
        return "当前配置库没有覆盖请求车型或市场，配置差异、版本价差和可见卖点不能写成确定结论。"
    if "current_msrp" in lower or "own_model_price" in lower:
        return "已有价格样本只能支撑走廊判断，仍缺本车型官方 MSRP 交叉验证，价格锚点和价差判断不能写成最终结论。"
    if "competitor_price_range" in lower or "price_corridor" in lower:
        return "缺少竞品价格走廊，无法判断目标价格处在低位、中段还是高位。"
    if "configuration" in lower or "feature" in lower or "battery" in lower:
        return "缺少配置差异证据，不能把卖点、短板或版本价差写成确定结论。"
    if "monthly_trend" in lower:
        return "缺少月度趋势，当前只能读静态结构，不能解释窗口期和变化速度。"
    if "decision_boundary" in lower:
        return "结论仍应随最新价格、政策和配置证据更新。"
    if "external_source_repair_candidates" in lower:
        return "当前只有外部检索线索，还没有可引用来源，不能直接当作用户高频吐槽证据。"
    if "source_repair_candidates" in lower:
        return "当前只有来源修复候选，不能直接当作官方价格证据。"
    cleaned = _clean_business_text(fallback)
    if not cleaned or re.search(r"[A-Za-z_]{4,}", cleaned):
        return "该证据缺口会削弱业务结论，需要补齐后再写确定判断。"
    return cleaned


def _tool_business_label(tool_name: str) -> str:
    mapping = {
        "query_msrp_pricing": "MSRP/当前价格",
        "compare_competitive_set": "竞品池/价格走廊",
        "query_competitive_landscape": "竞品格局",
        "query_price_positioning": "目标价格定位",
        "query_country_snapshot": "市场快照",
        "analyze_market_dynamics": "市场动态分析",
        "build_market_chart": "趋势图表",
        "compare_vehicle_variants": "配置差异",
        "external_research": "外部研究",
        "search_market_news": "新闻/政策搜索",
        "read_web_page": "网页来源读取",
        "pageindex_search_documents": "文档检索",
        "minirag_query_graph": "多跳知识检索",
        "query_with_filters": "筛选查询",
    }
    return mapping.get(str(tool_name or "").strip(), str(tool_name or "").strip() or "必需")


def _confidence_label(confidence: str) -> str:
    mapping = {
        "high": "高",
        "medium": "中",
        "low": "低",
    }
    return mapping.get(str(confidence or "").strip().lower(), str(confidence or "未知"))


def _has_target_policy_source_gap(evidence_package: dict[str, Any]) -> bool:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    return any(
        isinstance(item, dict)
        and str(item.get("name") or "").startswith("target_policy_source:")
        for item in missing
    )


def _alignment_label(status: str) -> str:
    mapping = {
        "aligned": "证据一致",
        "partially_aligned": "部分对齐",
        "conflicting": "证据冲突",
        "insufficient": "证据不足",
    }
    return mapping.get(str(status or "").strip(), str(status or "未知"))


def _evidence_count_note(refs: int) -> str:
    if refs <= 0:
        return "当前缺少可引用证据"
    return f"{refs} 条可引用证据"


def _evidence_availability_note(refs: int) -> str:
    return f"当前有 {_evidence_count_note(refs)}" if refs > 0 else "当前缺少可引用证据"


def _country_label(country: str) -> str:
    mapping = {
        "sweden": "瑞典",
        "finland": "芬兰",
        "norway": "挪威",
        "denmark": "丹麦",
        "germany": "德国",
        "hungary": "匈牙利",
        "austria": "奥地利",
        "italy": "意大利",
        "poland": "波兰",
        "france": "法国",
        "netherlands": "荷兰",
    }
    value = str(country or "").strip()
    return mapping.get(value.lower(), value or "当前市场")


def _market_regional_scope(country_label: str) -> str:
    value = str(country_label or "").strip() or "当前市场"
    if value in {"瑞典", "芬兰", "挪威", "丹麦"}:
        return f"{value}/北欧"
    if value == "匈牙利":
        return "匈牙利/中东欧"
    if value == "当前市场":
        return "当前市场/目标区域"
    return f"{value}/目标区域"


def _market_possessive(country_label: str) -> str:
    value = str(country_label or "").strip()
    if not value:
        return "当前市场的"
    if re.search(r"[\u4e00-\u9fff]", value):
        return f"{value}的"
    return f"{value} 的"


def _market_label(country_label: str) -> str:
    value = str(country_label or "").strip()
    if not value:
        return "当前市场"
    if re.search(r"[\u4e00-\u9fff]", value) and not value.endswith("市场"):
        return f"{value}市场"
    return value


def _market_business_prefix(country_label: str, subject: str) -> str:
    country = str(country_label or "").strip()
    topic = str(subject or "").strip()
    if not topic:
        return _market_label(country)
    if not country:
        return topic
    if re.search(r"[\u4e00-\u9fff]", country) and re.match(r"[\u4e00-\u9fff]", topic):
        return f"{country}{topic}"
    return f"{country} {topic}"


def _evidence_first_executive_conclusion(
    *,
    intent: str,
    country_label: str,
    label: str,
    confidence: str,
    alignment: EvidenceAlignment,
    refs: int,
    first_action: str,
    first_implication: str,
    method: BusinessMethodDistillation | None,
    evidence_package: dict[str, Any],
) -> str:
    if refs <= 0:
        return ""
    if alignment["status"] == "conflicting":
        return ""
    if method is not None and intent in {"pricing_analysis", "report_generation"}:
        return ""
    claims = _evidence_claim_lines(evidence_package, limit=3)
    if len(claims) < 2:
        return ""
    implication = _strip_terminal_punctuation(first_implication)
    if not implication or _looks_like_generic_business_implication(implication):
        implication = _evidence_claim_business_implication(intent)
    action = first_action or _evidence_claim_next_action(intent)
    lead_claim = claims[0]
    supporting_claims = claims[1:]
    return _clean_business_text(
        _bounded_direct_text(
            [
                f"直接结论：{_market_possessive(country_label)}{label}先看已查到的数据：{lead_claim}。",
                f"关键证据：{'；'.join(supporting_claims)}。",
                (
                    f"业务含义：{implication}。下一步执行 {action}。"
                    f"证据状态：{_alignment_label(alignment['status'])}，{_evidence_count_note(refs)}，"
                    f"置信度{_confidence_label(confidence)}。"
                ),
            ],
            max_chars=950,
        )
    )


def _evidence_claim_lines(evidence_package: dict[str, Any], *, limit: int = 3) -> list[str]:
    claims: list[str] = []
    seen: set[str] = set()
    for ref in _all_evidence_refs(evidence_package):
        claim = _evidence_claim_from_ref(ref)
        key = _normalize_space(claim).casefold()
        if not claim or key in seen:
            continue
        seen.add(key)
        claims.append(claim)
        if len(claims) >= limit:
            return claims
    for tool in _tool_evidence_results(evidence_package):
        findings = tool.get("keyFindings") if isinstance(tool.get("keyFindings"), list) else []
        for finding in findings:
            claim = _public_evidence_finding(str(finding or ""))
            key = _normalize_space(claim).casefold()
            if not claim or key in seen or _looks_like_internal_evidence_code(claim):
                continue
            seen.add(key)
            claims.append(claim)
            if len(claims) >= limit:
                return claims
    return claims


def _evidence_claim_from_ref(ref: dict[str, Any]) -> str:
    if _evidence_ref_is_zero_volume(ref):
        return ""
    label = _evidence_claim_label(str(ref.get("label") or ""))
    if not label:
        return ""
    if _evidence_ref_label_is_metadata(label):
        return ""
    value = _evidence_claim_value(ref.get("value"), str(ref.get("unit") or ""))
    if not value:
        return ""
    return f"{label} = {value}"


def _evidence_ref_is_zero_volume(ref: dict[str, Any]) -> bool:
    label = str(ref.get("label") or "").casefold()
    unit = str(ref.get("unit") or "").casefold()
    has_volume_label = any(token in label for token in ("sales", "volume"))
    has_count_label = any(token in label for token in (".value", ".count", "count"))
    has_volume_unit = any(token in unit for token in ("unit", "vehicle", "car", "count"))
    if not has_volume_label and not (has_count_label and has_volume_unit):
        return False
    if unit and not has_volume_unit:
        return False
    value = _numeric_ref_value(ref)
    return value is not None and value <= 0


def _evidence_claim_label(label: str) -> str:
    text = _normalize_space(label)
    text = re.sub(r"^crossCountry\.([^.\s]+)\.", r"\1 ", text, flags=re.IGNORECASE)
    text = re.sub(r"^(?:market|snapshot|contextSnapshot|crossSectionData)\.", "", text, flags=re.IGNORECASE)
    text = text.replace(".", " / ")
    text = text.replace("_", " ")
    return _normalize_space(text).strip(" /")


def _evidence_ref_label_is_metadata(label: str) -> bool:
    lowered = label.casefold()
    metadata_tokens = (
        "/ source",
        ".source",
        "source url",
        "/ url",
        ".url",
        "/ rank",
        ".rank",
        "retrieved",
    )
    return any(token in lowered for token in metadata_tokens)


def _evidence_claim_value(value: Any, unit: str) -> str:
    if value is None:
        return ""
    text = _normalize_space(str(value))
    if not text:
        return ""
    normalized_unit = _normalize_space(unit)
    if not normalized_unit or normalized_unit.casefold() in {"currency", "text", "string"}:
        return text
    if normalized_unit in text:
        return text
    if normalized_unit == "%" and "%" in text:
        return text
    return f"{text} {normalized_unit}"


def _tool_evidence_results(evidence_package: dict[str, Any]) -> list[dict[str, Any]]:
    tools = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    return [item for item in tools if isinstance(item, dict) and item.get("success") is not False]


def _looks_like_internal_evidence_code(value: str) -> bool:
    text = str(value or "")
    return bool(re.search(r"\b[a-z]+(?:_[a-z0-9]+){2,}\b", text)) and not re.search(r"[\u4e00-\u9fff]", text)


def _looks_like_generic_business_implication(value: str) -> bool:
    text = _normalize_question_text(value)
    generic_tokens = (
        "不能只",
        "不应只",
        "应先",
        "先把",
        "要先",
        "不能套用",
        "缺少",
        "当前不能输出确定数字",
        "把证据转成",
    )
    return any(token in text for token in generic_tokens)


def _evidence_claim_business_implication(intent: str) -> str:
    mapping = {
        "pricing_analysis": "先把已查价格、竞品和配置证据转成价格走廊，再判断低配锚点、高配主推或价值溢价是否成立",
        "market_overview": "先用销量、份额、动力结构和级别集中度判断机会入口，再落到车型和价格动作",
        "competitor_compare": "先用已查竞品规模、价格和配置差异判断对标角色，再决定正面对抗、错位竞争或价格锚点",
        "configuration_analysis": "先用配置差异和用户场景证据判断哪些配置能转成可感知价值，再决定主销版本",
        "inventory_analysis": "先用车型、版本、物料号和市场证据建立实体关系，再判断异常和生命周期动作",
        "news_policy_search": "先用来源日期、适用对象和市场证据限定政策影响，再转成车型、价格和渠道动作",
        "voc_analysis": "先用可追溯来源和主题证据区分真实痛点、媒体观点和论坛噪音，再决定是否转成卖点",
        "report_generation": "先把证据压成 key message、supporting evidence 和 next action，再进入 PPT-ready 输出",
    }
    return mapping.get(intent, "先把可引用证据转成业务判断，再输出下一步动作")


def _evidence_claim_next_action(intent: str) -> str:
    mapping = {
        "pricing_analysis": "生成价格走廊和竞品价格表",
        "market_overview": "生成市场结构图和机会 segment 表",
        "competitor_compare": "生成竞品对比表和定位图",
        "configuration_analysis": "生成配置价值验证矩阵",
        "inventory_analysis": "生成 BOM / 物料号关系表",
        "news_policy_search": "生成政策来源表和车型影响矩阵",
        "voc_analysis": "生成 VOC 来源验证表和主题聚类",
        "report_generation": "生成 PPT-ready report block 和 evidence appendix",
    }
    return mapping.get(intent, "生成证据表和业务结论块")


def _intent_executive_conclusion(
    *,
    intent: str,
    country_label: str,
    confidence: str,
    alignment: EvidenceAlignment,
    refs: int,
    first_action: str,
    first_implication: str,
) -> str:
    evidence_note = _evidence_count_note(refs)
    confidence_note = _confidence_label(confidence)
    alignment_note = _alignment_label(alignment["status"])
    action = first_action or "补齐核心证据并生成可复用输出"
    implication = _strip_terminal_punctuation(first_implication)
    if intent == "pricing_analysis":
        return (
            f"直接结论：{country_label} 定价判断应围绕价格走廊、竞品池、配置价值和月供/company car 场景展开；"
            f"{implication or '先确定价格锚点、价值边界和目标用户场景，再决定低价切入、贴近主流还是价值溢价'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "market_overview":
        return (
            f"直接结论：{country_label} 市场总览的重点不是复述份额，而是找出机会 segment、动力结构变化和产品进入顺序；"
            f"{implication or '先把销量/份额拆到动力和级别，再落到目标产品组合的可进入点'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "competitor_compare":
        return (
            f"直接结论：{country_label} 竞品对比要先锁定竞品池，再判断正面对抗、错位竞争还是价格锚点；"
            f"{implication or '结论必须转成可赢点、短板、配置差异和销售话术'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "configuration_analysis":
        return (
            f"直接结论：{country_label} 配置判断要连接真实用户场景，而不是只列装备；"
            f"{implication or '把冬季、续航、充电、拖车/载重、ADAS 与价格价值一起判断'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "inventory_analysis":
        return (
            f"直接结论：{country_label} 库存/BOM 问题应先建实体关系，再判断异常；"
            f"{implication or '车型版本、物料号、市场、颜色、PI、订单和生命周期必须分层建模'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "news_policy_search":
        return (
            f"直接结论：{country_label} 政策/新闻分析必须先确认来源日期和适用对象，再转成车型、价格门槛和渠道动作；"
            f"{implication or '没有官方来源时只能给影响路径，不能写成确定政策事实'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "report_generation":
        return (
            f"直接结论：{country_label} 这页汇报必须先给可拍板的业务立场，而不是先解释 PPT 模板；"
            f"{implication or '把可用证据转成产品含义、风险边界和下一步补证动作'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "voc_analysis":
        voc_implication = implication
        if "真实用户痛点、媒体观点、论坛噪音和可转化卖点" in voc_implication:
            voc_implication = "先按来源可信度和主题聚类验证，再映射到配置、价格、售后和销售话术"
        return (
            f"直接结论：{country_label} VOC 分析要把真实用户痛点、媒体观点、论坛噪音和可转化卖点拆开；"
            f"{voc_implication or '没有可追溯来源时只能给主题假设和检索路径，不能声称高频'}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    return ""


def _question_specific_executive_conclusion(
    *,
    intent: str,
    country_label: str,
    confidence: str,
    alignment: EvidenceAlignment,
    refs: int,
    first_action: str,
    first_implication: str,
    method: BusinessMethodDistillation | None,
    evidence_package: dict[str, Any],
    question: str,
) -> str:
    text = _normalize_question_text(question)
    if not text:
        return ""
    evidence_note = _evidence_count_note(refs)
    confidence_note = _confidence_label(confidence)
    alignment_note = _alignment_label(alignment["status"])
    action = first_action or "补齐核心证据并生成可复用输出"
    if intent == "report_generation" and _is_generic_report_action(action):
        action = "补齐关键证据并生成一页 PPT-ready block"
    if intent == "inventory_analysis" and _is_pi_market_split_question(text):
        market_scope = _pi_market_scope_label(country_label, text)
        return (
            f"直接结论：{market_scope} 合并 PI、车辆分市场生成的逻辑原则上可以成立，但前提是 PI 只承载共用计划/产品信息层，"
            "车辆生成、物料号、市场合规、价格、订单和库存生命周期必须保留 market-level overlay。"
            "正确结构应是 PI header + market overlay + materialCode / vehicle generation mapping。"
            "如果合并 PI 会覆盖 SE/FI 的市场差异、物料映射或车辆生成规则，就不应合并到执行层。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "report_generation" and _is_bev_penetration_report(text):
        bev_action = (
            action
            if _action_matches_topic(action, ("bev", "渗透", "趋势", "产品定义", "policy", "政策"))
            else "补齐 BEV 趋势和驱动因素证据，再生成一页产品定义建议 PPT block"
        )
        return _bev_penetration_report_direct_answer(
            country_label=country_label,
            evidence_package=evidence_package or {},
            action=bev_action,
            alignment_note=alignment_note,
            evidence_note=evidence_note,
            confidence_note=confidence_note,
        )
    if method and _question_mentions_method_model(method, text) and intent == "report_generation":
        model = str(method.get("model") or "目标车型").strip()
        price_corridor = method.get("priceCorridor") if isinstance(method.get("priceCorridor"), dict) else {}
        positioning = str(price_corridor.get("positioning") or "低配锚点 + 高配主推").strip()
        verified_lines = _pricing_verified_evidence_lines(evidence_package, limit=2)
        hypothesis_lines = _pricing_user_material_hypothesis_lines(evidence_package, limit=2)
        verified_note = "；".join(verified_lines) if verified_lines else "本轮未拿到本车型/核心竞品官方当前 MSRP"
        hypothesis_note = (
            "；".join(hypothesis_lines)
            if hypothesis_lines
            else f"用户材料定位假设为“{positioning}”"
        )
        return (
            f"直接结论：{country_label} {model} 这页 PPT 应写成“验证版定价逻辑”，不能直接把“{positioning}”当成最终 MSRP 结论。"
            f"已验证证据：{verified_note}。用户材料价格假设（用户材料假设）：{hypothesis_note}。"
            "业务含义是低配锚点、高配主推可以先作为待验证话术，但页面必须标注官方 MSRP、竞品价、月供/RV 和 PVA 口径缺口。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "report_generation" and _is_competitor_report_scope(evidence_package, text):
        report_brief = _generic_competitor_report_brief(
            country_label=country_label,
            evidence_package=evidence_package,
            question_text=text,
        )
        return _clean_business_text(
            f"直接结论：{report_brief}"
            "一页竞品汇报应把证据状态、待验证角色和补证路径放在同一页，不能按车型名称套固定主/辅对标模板。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "news_policy_search":
        if "elbilspremien" in text:
            return (
                f"直接结论：{country_label} Elbilspremien 2026 的影响应先按车型资格、价格上限、购买人群和交付时间拆开，"
                "优先影响的是价格门槛内、私人零售敏感度高的 BEV SUV A0/A 车型；但在没有官方条文、发布日期和价格门槛证据前，"
                "不能点名确定受益车型。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        if "company car" in text and "bev" in text and "phev" in text:
            return (
                f"直接结论：{country_label} company car benefit 对 BEV 和 PHEV 的差异，不能只看补贴，而要拆 benefit tax、月供、"
                "残值、公司车政策、充电条件和实际里程。BEV 更容易拿到低使用成本和政策叙事，PHEV 的理由只在长途/无稳定充电/低风险替代场景成立。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        if _is_phev_fleet_leasing_question(text):
            tco_action = (
                action
                if _action_matches_topic(action, ("tco", "leasing", "月供", "残值", "company car", "大客户", "fleet", "税务", "benefit"))
                else "建立 PHEV fleet leasing TCO 表"
            )
            return (
                f"直接结论：{_market_business_prefix(country_label, '大客户 leasing 场景')}下 PHEV 仍可能有理由，但只能作为条件成立的 fleet/TCO 验证线；"
                "理由不来自泛泛“可油可电”，而来自月供、残值/RV、税务 benefit、年里程、充电条件、长途里程和冬季使用风险的组合。"
                "如果这些口径算不出成本或使用风险优势，PHEV 就不应主推。"
                f"下一步执行 {tco_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        if ("co2" in text or "co₂" in text) and "phev" in text:
            return (
                f"直接结论：{country_label} CO2 0-75g/km 阶梯只是 PHEV 的入场条件，不是自动利好；"
                "结论必须由实际认证 CO2、官方税率/benefit 公式、company car 月供、残值、能耗和真实充电行为共同证明。"
                "如果这些口径算不出 TCO 或使用风险优势，PHEV 就只能作为公司车验证线，不能直接主推。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        if _is_bev_subsidy_cap_question(text):
            target_model = _policy_target_model(evidence_package, text)
            return (
                f"直接结论：{country_label} BEV 补贴价格上限对 {target_model} 的作用，是把定价从“相对竞品便宜多少”改成"
                f"“能否压进资格门槛并保住配置价值”。若 {target_model} 高配超过门槛，低配/主销版就要承担入门资格锚点。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
    if intent == "voc_analysis" and "v2h" in text:
        return (
            f"直接结论：{country_label} V2H 暂时不应被写成真实高频购买卖点，而应定位为“高感知但待验证”的技术型加分项。"
            "它更可能服务家庭能源、安全备份、冬季用车和科技形象叙事；若当前只有市场结构证据，这只能作为代理判断，不是消费者调研结论。"
            "是否能转化购买，仍需要用户原声、媒体测评和经销端话术验证。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "pricing_analysis" and "phev" in text and _contains_any(text, ("leasing", "lease", "大客户", "fleet", "公司车")):
        return (
            f"直接结论：{_market_business_prefix(country_label, '大客户 leasing 场景')}下 PHEV 仍可能有理由，但理由不来自泛泛“可油可电”，"
            "而来自 TCO、月供、残值、公司车税、长途里程、充电条件和冬季使用风险的组合。若这些口径算不出优势，PHEV 就不应主推。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "pricing_analysis":
        relative_direct = _generic_relative_pricing_direct_answer(
            country_label=country_label,
            evidence_package=evidence_package,
            context_text=text,
            method=method,
            action=action,
            display_note=_pricing_visual_backbone_note(evidence_package),
            pending_msrp_note=_pending_msrp_review_summary_text(evidence_package),
            alignment_note=alignment_note,
            evidence_note=evidence_note,
            confidence_note=confidence_note,
        )
        if relative_direct:
            return f"直接结论：{relative_direct}"
    if method and _question_mentions_method_model(method, text) and intent == "pricing_analysis":
        model = str(method.get("model") or "目标车型").strip()
        price_corridor = method.get("priceCorridor") if isinstance(method.get("priceCorridor"), dict) else {}
        positioning = str(price_corridor.get("positioning") or "低配锚点 + 高配主推").strip()
        verified_lines = _pricing_verified_evidence_lines(evidence_package, limit=2)
        hypothesis_lines = _pricing_user_material_hypothesis_lines(evidence_package, limit=2)
        verified_note = "；".join(verified_lines) if verified_lines else "本轮未拿到本车型/核心竞品官方当前 MSRP"
        hypothesis_note = (
            "；".join(hypothesis_lines)
            if hypothesis_lines
            else f"用户材料定位假设为“{positioning}”"
        )
        return (
            f"直接结论：{country_label} {model} 定价现在应先做验证版，不能直接给最终 MSRP。"
            f"已验证证据：{verified_note}。用户材料价格假设（用户材料假设）：{hypothesis_note}。"
            "低配锚点和高配主推只能作为待验证话术，必须补齐官方 MSRP、竞品价格、月供/RV 和配置价值证据后再定稿。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    if intent == "competitor_compare":
        generic_direct = _generic_competitor_evidence_brief(
            plan={
                "country": country_label,
                "intent": intent,
                "recommendedActions": [],
            },
            evidence_package=evidence_package,
            question_text=text,
        )
        if generic_direct:
            return (
                f"直接结论：{generic_direct}"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        if method and _question_mentions_method_model(method, text) and _contains_any(text, ("核心竞品", "竞品是谁", "competitor")):
            pool_items = [str(item or "").strip() for item in method.get("competitorPool", []) if str(item or "").strip()]
            if not pool_items:
                return ""
            pool = ", ".join(pool_items[:5])
            model = str(method.get("model") or "目标车型").strip()
            return (
                f"直接结论：{country_label} {model} 的核心竞品应先收敛到 {pool} 这一组同价带/同场景车型，"
                "再按价格、动力、空间、可见配置和品牌风险拆优先级。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
    if intent == "market_overview":
        drive_strategy_direct = _market_drive_strategy_conclusion(
            text=text,
            country_label=country_label,
            evidence_package=evidence_package,
        )
        if drive_strategy_direct:
            return drive_strategy_direct
        if _is_hev_phev_route_question(text):
            return (
                f"直接结论：{_market_label(country_label)}现在不应把 HEV/PHEV 简化成二选一；更稳的打法是先以 HEV 做低风险主线，"
                "用价格敏感、无稳定充电和低使用风险场景承接主流需求，同时把 PHEV 放进公司车、长途里程、税费/TCO 和补能条件里验证。"
                "如果 PHEV 在月供、残值、税费或公司车政策上没有形成清晰优势，就不应先于 HEV 成为主推。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        powertrain_opportunity = _market_powertrain_opportunity_cross_tab_conclusion(
            text=text,
            country_label=country_label,
            evidence_package=evidence_package,
            refs=refs,
            confidence_note=confidence_note,
            alignment_note=alignment_note,
            action=action,
            method=method,
        )
        if powertrain_opportunity:
            return powertrain_opportunity
        if "bev" in text and "hev" in text and _contains_any(text, ("压缩", "挤压", "空间")):
            evidence_brief = _market_powertrain_compression_evidence_brief(evidence_package)
            display_note = _artifact_visual_backbone_note("market_overview", evidence_package)
            scope_label = _regional_scope_label(question, country_label)
            parts = [
                f"直接结论：{scope_label} BEV 增长会压缩 HEV 空间，但不是把 HEV 一次性替代掉；"
                "BEV 会优先吃掉公司车、电动化 SUV 和政策敏感需求，HEV 仍保留无充电条件、价格敏感和低使用风险用户。",
                evidence_brief,
                display_note,
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
            ]
            return _clean_business_text(_bounded_direct_text(parts, max_chars=1050))
        if "suv" in text and _contains_any(text, ("a0", "a 级", "a级", "主销", "结构")):
            segment_conclusion = _market_suv_a0_a_structure_conclusion(
                country_label=country_label,
                evidence_package=evidence_package,
                action=action,
                alignment_note=alignment_note,
                evidence_note=evidence_note,
                confidence_note=confidence_note,
            )
            if segment_conclusion:
                return segment_conclusion
            return (
                f"直接结论：{country_label} SUV A0/A 成为主销结构，应先用 segment cross-tab 验证销量、动力结构和驱动形式，而不是只给泛 SUV 原因。"
                "当前可先按家庭空间、城市通勤、冬季通过性、公司车税务和电动化平台成本建立假设；但缺 SUV A0/A 细分证据时不能写成已验证结论。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        if "瑞典" in text and "芬兰" in text and _contains_any(text, ("差异", "为什么", "销量")):
            country_pair_conclusion = _market_country_pair_sales_difference_conclusion(
                evidence_package=evidence_package,
                action=action,
                alignment_note=alignment_note,
                evidence_note=evidence_note,
                confidence_note=confidence_note,
            )
            if country_pair_conclusion:
                return country_pair_conclusion
        generic_market_direct = _generic_market_overview_evidence_direct(
            country_label=country_label,
            evidence_package=evidence_package,
            action=action,
            alignment_note=alignment_note,
            evidence_note=evidence_note,
            confidence_note=confidence_note,
        )
        if generic_market_direct:
            return generic_market_direct
    if refs and first_implication and _looks_like_generic_first_sentence(first_implication):
        if intent == "voc_analysis":
            return (
                f"直接结论：{_market_possessive(country_label)}用户声音判断应先验证来源可信度和主题聚类，再决定能否写成高频需求；"
                f"{_strip_terminal_punctuation(first_implication)}。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        if intent == "news_policy_search":
            return (
                f"直接结论：{_market_possessive(country_label)}政策/新闻判断应先确认官方来源、发布日期和适用对象，再转成车型、价格和渠道影响；"
                f"{_strip_terminal_punctuation(first_implication)}。"
                f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
            )
        return (
            f"直接结论：{country_label} 需要先给业务立场，再展开证据；当前判断是 {_strip_terminal_punctuation(first_implication)}。"
            f"下一步执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    _ = evidence_package
    return ""


def _market_drive_strategy_conclusion(
    *,
    text: str,
    country_label: str,
    evidence_package: dict[str, Any],
) -> str:
    """Answer an explicit 2WD/4WD decision when the retrieved mix supports it.

    This is a reusable evidence-to-decision rule, not a model-specific reply:
    it only fires for a direct drivetrain comparison and keeps the model-level
    configuration decision provisional until price and variant evidence exist.
    """
    normalized = _normalize_question_text(text)
    asks_for_drive_choice = (
        _contains_any(normalized, ("2wd", "两驱"))
        and _contains_any(normalized, ("4wd", "四驱", "awd"))
        and _contains_any(
            normalized,
            ("主销", "主推", "优先", "先做", "作为", "primary", "prioritise", "prioritize", "mainstream", "lead with"),
        )
    )
    if not asks_for_drive_choice:
        return ""
    fuel = "PHEV" if "phev" in normalized else "HEV"
    two_wd = _market_cross_tab_ref_value(
        evidence_package,
        table="driveByFuel",
        row=fuel,
        metric="2WD_pct",
    )
    four_wd = (
        _market_cross_tab_ref_value(
            evidence_package,
            table="driveByFuel",
            row=fuel,
            metric="4WD_pct",
        )
        or _market_cross_tab_ref_value(
            evidence_package,
            table="driveByFuel",
            row=fuel,
            metric="AWD_pct",
        )
    )
    two_wd_value = _first_number(str(two_wd or ""))
    four_wd_value = _first_number(str(four_wd or ""))
    if two_wd_value is None or four_wd_value is None:
        return ""

    market = _market_label(country_label)
    if two_wd_value > four_wd_value:
        primary, secondary = "2WD", "4WD"
        primary_share, secondary_share = two_wd, four_wd
    elif four_wd_value > two_wd_value:
        primary, secondary = "4WD", "2WD"
        primary_share, secondary_share = four_wd, two_wd
    else:
        return (
            f"{market} {fuel} 的已查驱动结构中 2WD 与 4WD 占比相同（均为 {two_wd}），"
            "当前不能仅凭市场结构确定主销驱动形式，还需要车型级价格、配置和渠道场景证据。"
        )
    subject = _market_fit_target_label(normalized, None, evidence_package, powertrain=fuel)
    return (
        f"{market} {subject} 应优先以 {primary} 作为主销方向：本轮 {fuel} 驱动结构中 {primary} 占 {primary_share}，"
        f"明显高于 {secondary} 的 {secondary_share}。{secondary} 更适合作为特定气候、牵引或高配需求的补充版本；"
        "这是一条市场结构结论，最终版型配比仍需用目标车型价格、配置差异和渠道需求验证。"
    )


def _is_market_opportunity_question(text: str) -> bool:
    normalized = _normalize_question_text(text)
    return _contains_any(
        normalized,
        ("机会", "适合", "值得", "进入", "验证", "opportunity", "fit", "suitable", "worth", "validate"),
    )


def _market_opportunity_powertrain(evidence_package: dict[str, Any], text: str) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    entity_powertrains = entities.get("powertrains") if isinstance(entities.get("powertrains"), list) else []
    for item in entity_powertrains:
        value = str(item or "").strip().upper()
        if value in {"BEV", "PHEV", "HEV", "MHEV", "ICE"}:
            return value
    normalized = _normalize_question_text(text)
    for powertrain in ("PHEV", "BEV", "HEV", "MHEV", "ICE"):
        if powertrain.casefold() in normalized:
            return powertrain
    available = [
        powertrain
        for powertrain, values in _powertrain_stats_from_evidence(evidence_package).items()
        if values.get("sales") or values.get("share")
    ]
    return available[0] if len(available) == 1 else ""


def _market_powertrain_opportunity_cross_tab_conclusion(
    *,
    text: str,
    country_label: str,
    evidence_package: dict[str, Any],
    refs: int,
    confidence_note: str,
    alignment_note: str,
    action: str = "",
    method: BusinessMethodDistillation | None = None,
) -> str:
    if not _is_market_opportunity_question(text):
        return ""
    powertrain = _market_opportunity_powertrain(evidence_package, text)
    if not powertrain:
        return ""
    powertrain_sales = _market_cross_tab_ref_value(
        evidence_package,
        table="driveByFuel",
        row=powertrain,
        metric="sales",
    )
    powertrain_stats = _powertrain_stats_from_evidence(evidence_package)
    selected_powertrain = powertrain_stats.get(powertrain, {})
    if not powertrain_sales:
        powertrain_sales = selected_powertrain.get("sales", "")
    powertrain_share = selected_powertrain.get("share", "")
    suv_a0_a_concentration = _market_snapshot_ref_value(
        evidence_package,
        required_tokens=("suv", "a0", "concentration"),
    )
    suv_a0_sales = _market_cross_tab_ref_value(
        evidence_package,
        table="driveBySegment",
        row="SUV A0",
        metric="sales",
    )
    suv_a_sales = _market_cross_tab_ref_value(
        evidence_package,
        table="driveBySegment",
        row="SUV A",
        metric="sales",
    )
    powertrain_2wd = _market_cross_tab_ref_value(
        evidence_package,
        table="driveByFuel",
        row=powertrain,
        metric="2WD_pct",
    )
    powertrain_4wd = _market_cross_tab_ref_value(
        evidence_package,
        table="driveByFuel",
        row=powertrain,
        metric="4WD_pct",
    )
    if not (powertrain_sales or powertrain_share or suv_a0_sales or suv_a_sales or suv_a0_a_concentration):
        return ""

    market = _market_label(country_label)
    method_matches_scope = bool(method and _question_mentions_method_model(method, text))
    target = _market_fit_target_label(text, method, evidence_package, powertrain=powertrain)
    segment_parts: list[str] = []
    if suv_a0_sales:
        segment_parts.append(f"SUV A0 {suv_a0_sales}")
    if suv_a_sales:
        segment_parts.append(f"SUV A {suv_a_sales}")
    segment_text = "、".join(segment_parts)
    segment_clause = f"，细分结构里 {segment_text}" if segment_text else ""
    drive_clause = ""
    if powertrain_2wd and powertrain_4wd:
        drive_clause = f"，{powertrain} 内部两驱占 {powertrain_2wd}、四驱占 {powertrain_4wd}"
    elif powertrain_2wd:
        drive_clause = f"，{powertrain} 内部两驱占 {powertrain_2wd}"
    elif powertrain_4wd:
        drive_clause = f"，{powertrain} 内部四驱占 {powertrain_4wd}"
    total_powertrain_sales = str(powertrain_stats.get(powertrain, {}).get("sales") or "").strip()
    cross_tab_scope_note = ""
    if powertrain_sales:
        if total_powertrain_sales and not _same_market_numeric_value(powertrain_sales, total_powertrain_sales):
            powertrain_metric = f"{powertrain} cross-tab 覆盖样本 {powertrain_sales}"
            cross_tab_scope_note = f"国家动力总量口径的 {powertrain} 为 {total_powertrain_sales}，两者不直接相加或比较"
        else:
            powertrain_metric = f"{powertrain} 规模 {powertrain_sales}"
    elif powertrain_share:
        powertrain_metric = f"{powertrain} 份额 {powertrain_share}"
    else:
        powertrain_metric = f"{powertrain} 规模待补"
    if powertrain_sales and powertrain_share:
        powertrain_metric = f"{powertrain_metric}，份额 {powertrain_share}"
    concentration_clause = f"，SUV A0/A 集中度 {suv_a0_a_concentration}" if suv_a0_a_concentration else ""
    pressure_clause = _market_powertrain_pressure_clause(powertrain_stats, powertrain)
    scale_trend_clause = _market_scale_trend_clause(evidence_package)
    method_market_window = ""
    method_competitor_pool = ""
    if method_matches_scope:
        playbook = method.get("pricingPlaybook") if isinstance(method.get("pricingPlaybook"), dict) else {}
        method_market_window = _normalize_space(str(playbook.get("market_window") or ""))
        competitors = _dedupe([str(item or "").strip() for item in method.get("competitorPool", []) if str(item or "").strip()])[:4]
        if competitors:
            method_competitor_pool = "、".join(competitors)

    if segment_text or drive_clause or suv_a0_a_concentration:
        source_label = "cross-tab" if segment_text or drive_clause else "market snapshot"
        scope_clause = f"{cross_tab_scope_note}。" if cross_tab_scope_note else ""
        evidence_clause = f"内部 JATO {source_label} 显示 {powertrain_metric}{segment_clause}{concentration_clause}{drive_clause}。{scope_clause}{pressure_clause}{scale_trend_clause}"
        implication = (
            f"这说明下一步判断不能停在国家级总览，而要落到 {powertrain} + SUV A0/A + 主销驱动形式，"
            "再补车型级竞品、价格和配置证据。"
        )
    else:
        evidence_clause = f"内部 JATO powertrain mix 显示 {powertrain_metric}。{pressure_clause}{scale_trend_clause}"
        implication = (
            f"这能证明存在可量化的 {powertrain} 需求池，但还不能单独证明 {target} 已适配；"
            f"下一步必须补 {powertrain} + SUV A0/A 结构、主销驱动形式、车型级竞品、价格和配置证据。"
        )
    if method_market_window:
        implication = (
            f"{implication} 用户材料方法论补充了市场窗口：{method_market_window}"
        )
        if method_competitor_pool:
            implication = f"{implication} 车型级验证应先围绕 {method_competitor_pool} 做价格/配置矩阵。"
    next_action = (
        f"补齐{market} {powertrain} SUV A0/A 的车型级竞品池、价格/配置证据，并生成市场结构表"
        if target.endswith("产品线") and not method_matches_scope
        else f"补齐{market} {target} 在 {powertrain} SUV A0/A 场景下的车型级价格/配置矩阵，并交叉验证当前 MSRP"
    )
    if method_competitor_pool:
        next_action = f"用 {method_competitor_pool} 生成 {target} 车型级价格/配置矩阵，并交叉验证当前 MSRP"
    if action and not _looks_like_source_repair_action(action):
        next_action = action
    subject = _market_powertrain_opportunity_subject(market, target)
    return (
        f"{subject}机会入口已有市场结构证据支撑：{powertrain} + SUV A0/A 可作为优先验证入口；"
        f"但这还不是最终上市或定价结论。"
        f"{evidence_clause}"
        f"{implication}"
        f"下一步执行 {next_action}。证据状态：{alignment_note}，{_evidence_count_note(refs)}，置信度{confidence_note}。"
    )


def _market_powertrain_opportunity_subject(market: str, target: str) -> str:
    cleaned_market = market.strip()
    cleaned_target = target.strip()
    if not cleaned_target:
        return cleaned_market
    if cleaned_target.endswith("产品线"):
        return f"{cleaned_market} {cleaned_target}"
    return f"{cleaned_market} {cleaned_target} 的"


def _market_powertrain_pressure_clause(
    powertrain_stats: dict[str, dict[str, str]],
    powertrain: str,
) -> str:
    selected_sales = str(powertrain_stats.get(powertrain, {}).get("sales") or "").strip()
    comparison_parts: list[str] = []
    for fuel in ("BEV", "PHEV", "HEV", "MHEV", "ICE"):
        if fuel == powertrain:
            continue
        sales = str(powertrain_stats.get(fuel, {}).get("sales") or "").strip()
        if sales:
            comparison_parts.append(f"{fuel} {sales}")
    if not comparison_parts:
        return ""
    selected_part = f"，国家总量口径 {powertrain} {selected_sales}" if selected_sales else ""
    return (
        f"动力结构对比显示 {'、'.join(comparison_parts)}{selected_part}，"
        f"因此 {powertrain} 机会应按真实需求池、细分集中度和渠道场景判断，不能只凭动力路线标签下结论。"
    )


def _same_market_numeric_value(left: str, right: str) -> bool:
    def parse(value: str) -> float | None:
        normalized = str(value or "").replace(",", "").strip()
        match = re.search(r"-?\d+(?:\.\d+)?", normalized)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None

    left_value = parse(left)
    right_value = parse(right)
    return left_value is not None and right_value is not None and abs(left_value - right_value) < 0.001


def _market_scale_trend_clause(evidence_package: dict[str, Any]) -> str:
    refs = _all_evidence_refs(evidence_package)
    parts = [
        item
        for item in (
            _market_total_size_evidence(refs),
            _market_monthly_series_evidence(evidence_package),
            _market_annual_total_trend_evidence(refs),
        )
        if item
    ]
    if not parts:
        return ""
    return f"市场盘子：{'；'.join(parts)}。"


def _generic_market_overview_evidence_direct(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    action: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    refs = _all_evidence_refs(evidence_package)
    if not refs:
        return ""
    evidence_parts = _generic_market_evidence_parts(evidence_package)
    if not evidence_parts:
        return ""
    evidence_text = "；".join(evidence_parts[:4])
    next_action = _market_overview_next_action(action, country_label, evidence_package)
    market = _market_label(country_label)
    requested_entities = _requested_entity_names_from_package(evidence_package)
    target_text = " / ".join(requested_entities[:3])
    if target_text:
        direct_line = (
            f"{market} {target_text} 的机会初筛必须先看已查数据：{evidence_text}。"
            "这些证据可以支持市场入口判断，但还不能单独拍板车型进入。"
        )
        implication_line = (
            f"业务含义：这批数据只能证明 {target_text} 的市场入口、动力结构和竞品池方向，"
            "还需要补目标车型 MSRP、配置差异、月供/RV 和渠道场景后才能写成确定打法。"
        )
    else:
        direct_line = (
            f"{market}总览先看已查数据：{evidence_text}。"
            "这些证据应先用于锁定机会入口、动力结构和级别/车型方向。"
        )
        implication_line = (
            "业务含义：先把机会拆到动力结构、SUV/级别结构和 Top model/竞品池，"
            "再补具体车型 MSRP、配置差异、月供/RV 和渠道场景。"
        )
    display_note = _artifact_visual_backbone_note("market_overview", evidence_package)
    parts = [
        direct_line,
        implication_line,
        display_note,
        f"下一步执行：{next_action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。",
    ]
    return _clean_business_text(_bounded_direct_text(parts, max_chars=1250))


def _market_overview_next_action(
    action: str,
    country_label: str,
    evidence_package: dict[str, Any],
) -> str:
    cleaned = _strip_terminal_punctuation(_clean_action_text(action))
    generic_actions = {
        "",
        "拆到车型/品牌",
        "做邻国 side-by-side",
        "生成市场机会页",
        "按动力类型找进入点",
        "量化受影响细分市场，并生成市场机会视图",
        "生成市场结构图、机会 segment 表和目标车型价格/配置补证清单",
    }
    if cleaned and cleaned not in generic_actions:
        return cleaned

    market = _market_label(country_label)
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    models = _dedupe([
        str(item or "").strip()
        for item in (entities.get("models") if isinstance(entities.get("models"), list) else [])
        if str(item or "").strip()
    ])
    powertrains = _dedupe([
        str(item or "").strip().upper()
        for item in (entities.get("powertrains") if isinstance(entities.get("powertrains"), list) else [])
        if str(item or "").strip()
    ])
    segments = _dedupe([
        str(item or "").strip()
        for item in (entities.get("segments") if isinstance(entities.get("segments"), list) else [])
        if str(item or "").strip()
    ])
    inferred_segments = _market_overview_inferred_segments(evidence_package)
    segment_text = " / ".join(segments[:2] or inferred_segments[:2])
    powertrain_text = " / ".join(powertrains[:2]) or _market_overview_primary_powertrain(evidence_package)
    model_text = " / ".join(models[:2])

    if model_text:
        scope = f"{market} {model_text}"
        if powertrain_text and powertrain_text not in model_text.upper():
            scope = f"{scope} {powertrain_text}"
        if segment_text:
            return f"把{scope} 拆到 {segment_text} 车型级竞品池、MSRP、配置差异和月供/RV 表"
        return f"把{scope} 拆到车型级竞品池、MSRP、配置差异和月供/RV 表"
    if powertrain_text and segment_text:
        return f"拆到{market} {powertrain_text} {segment_text} 车型级竞品池、价格和配置矩阵"
    if powertrain_text:
        return f"拆到{market} {powertrain_text} 车型级销量、价格、配置和渠道场景"
    if segment_text:
        return f"拆到{market} {segment_text} 动力结构、Top model、价格和配置矩阵"
    return f"拆到{market} 动力结构、SUV/级别结构、Top model、MSRP 和配置矩阵"


def _specific_market_overview_recommended_actions(
    actions: list[RecommendedAction],
    *,
    country_label: str,
    evidence_package: dict[str, Any],
) -> list[RecommendedAction]:
    generic_actions = {
        "",
        "拆到车型/品牌",
        "做邻国 side-by-side",
        "生成市场机会页",
        "按动力类型找进入点",
        "量化受影响细分市场，并生成市场机会视图",
    }
    result: list[RecommendedAction] = []
    inserted_specific = False
    for item in actions:
        action_text = _strip_terminal_punctuation(_clean_action_text(str(item.get("action") or "")))
        if action_text in generic_actions:
            if inserted_specific:
                continue
            replacement = dict(item)
            replacement["action"] = _market_overview_next_action(action_text, country_label, evidence_package)
            replacement["rationale"] = (
                "把已查市场结构落到车型级竞品池、MSRP、配置、月供/RV 和渠道场景，"
                "这样才可以从市场总览进入产品动作。"
            )
            replacement["priority"] = "P0"
            result.append(replacement)  # type: ignore[arg-type]
            inserted_specific = True
            continue
        result.append(item)
    if (
        result
        and not inserted_specific
        and _market_overview_should_preserve_evidence_gap_action(evidence_package, str(result[0].get("action") or ""))
    ):
        return _dedupe_actions(result)
    if result and not inserted_specific and not _market_overview_action_is_primary(str(result[0].get("action") or "")):
        primary_index = next(
            (
                index
                for index, item in enumerate(result[1:], start=1)
                if str(item.get("action") or "").strip()
                and not _looks_like_source_repair_action(str(item.get("action") or ""))
                and _market_overview_action_is_primary(str(item.get("action") or ""))
            ),
            -1,
        )
        first_is_repair = (
            _looks_like_source_repair_action(str(result[0].get("action") or ""))
            or _market_overview_action_is_evidence_repair(str(result[0].get("action") or ""))
        )
        if primary_index > 0:
            primary = dict(result.pop(primary_index))
            primary["priority"] = "P0"
            result.insert(0, primary)  # type: ignore[arg-type]
        elif first_is_repair:
            refs = _dedupe([
                str(ref.get("refId") or "")
                for ref in _all_evidence_refs(evidence_package)
                if str(ref.get("refId") or "").strip()
            ])[:3]
            result.insert(0, {
                "action": _market_overview_next_action("", country_label, evidence_package),
                "rationale": (
                    "先把已查市场结构转成车型级业务验证动作；"
                    "MSRP/来源修复保留为后续补证动作，不能抢占主业务建议。"
                ),
                "priority": "P0",
                "evidenceRefs": refs,
                "citationIds": [],
            })
        inserted_specific = True
    if not result:
        result.append({
            "action": _market_overview_next_action("", country_label, evidence_package),
            "rationale": "把已查市场结构落到车型级证据表，避免停在市场总览。",
            "priority": "P0",
            "evidenceRefs": [],
            "citationIds": [],
        })
    return _dedupe_actions(result)


def _market_overview_action_is_primary(action: str) -> bool:
    text = _normalize_question_text(action)
    if not text or _looks_like_source_repair_action(text):
        return False
    if _market_overview_action_is_evidence_repair(text):
        return False
    return _contains_any(
        text,
        (
            "建立",
            "生成",
            "拆到",
            "输出",
            "矩阵",
            "表",
            "图",
            "竞品池",
            "车型级",
            "market comparison",
            "validation table",
        ),
    )


def _market_overview_should_preserve_evidence_gap_action(evidence_package: dict[str, Any], action: str) -> bool:
    return (
        _market_overview_action_is_evidence_repair(action)
        and not _market_fit_has_usable_internal_market_evidence(evidence_package)
    )


def _market_overview_action_is_evidence_repair(action: str) -> bool:
    text = _normalize_question_text(action)
    return (
        text.startswith("补齐")
        and _contains_any(text, ("证据", "来源", "官方", "msrp"))
        and not _contains_any(text, ("矩阵", "表", "图", "matrix", "table", "chart"))
    )


def _specific_competitor_recommended_actions(
    actions: list[RecommendedAction],
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    question_text: str,
) -> list[RecommendedAction]:
    generic_actions = {
        "",
        "生成竞品对比表",
        "生成竞品对比表和定位图",
        "生成竞品对比表并补齐价格/配置证据",
    }
    result: list[RecommendedAction] = []
    inserted_specific = False
    specific_action = _competitor_next_action("", country_label, evidence_package, question_text)
    for item in actions:
        action_text = _strip_terminal_punctuation(_clean_action_text(str(item.get("action") or "")))
        if action_text in generic_actions:
            if inserted_specific:
                continue
            replacement = dict(item)
            replacement["action"] = specific_action
            replacement["rationale"] = (
                "把已查竞品证据落到同口径车型级销量、MSRP、配置差异、月供/RV 和定位角色，"
                "避免只停在泛对比表。"
            )
            replacement["priority"] = "P0"
            result.append(replacement)  # type: ignore[arg-type]
            inserted_specific = True
            continue
        result.append(item)
    if (
        result
        and not inserted_specific
        and _looks_like_source_repair_action(str(result[0].get("action") or ""))
    ):
        primary_index = next(
            (
                index
                for index, item in enumerate(result[1:], start=1)
                if str(item.get("action") or "").strip()
                and not _looks_like_source_repair_action(str(item.get("action") or ""))
            ),
            -1,
        )
        if primary_index > 0:
            primary = dict(result.pop(primary_index))
            primary["priority"] = "P0"
            result.insert(0, primary)  # type: ignore[arg-type]
        elif not any(str(item.get("action") or "") == specific_action for item in result):
            refs = _dedupe([
                str(ref.get("refId") or "")
                for ref in _all_evidence_refs(evidence_package)
                if str(ref.get("refId") or "").strip()
            ])[:3]
            result.insert(0, {
                "action": specific_action,
                "rationale": (
                    "先把已查竞品和市场场景证据组织成可读的对标矩阵；"
                    "MSRP 来源修复作为后续补证动作保留。"
                ),
                "priority": "P0",
                "evidenceRefs": refs,
                "citationIds": [],
            })
        inserted_specific = True
    if not result:
        result.append({
            "action": specific_action,
            "rationale": "把竞品问题转成车型级对标证据表，避免只给定位判断。",
            "priority": "P0",
            "evidenceRefs": [],
            "citationIds": [],
        })
    return _dedupe_actions(result)


def _specific_pricing_method_recommended_actions(
    actions: list[RecommendedAction],
    *,
    method: BusinessMethodDistillation,
    evidence_package: dict[str, Any],
    intent: str,
) -> list[RecommendedAction]:
    model = str(method.get("model") or _pricing_subject_label(evidence_package) or "目标车型").strip()
    primary_action = (
        f"把 {model} 一页汇报写成市场窗口、竞品走廊、配置价值、低配锚点和高配主推"
        if intent == "report_generation"
        else f"生成 {model} 验证版价格走廊、高配主推和竞品价格矩阵"
    )
    result = [dict(item) for item in actions if isinstance(item, dict)]
    if result and _pricing_method_action_is_primary(str(result[0].get("action") or "")):
        result[0]["priority"] = "P0"
        return _dedupe_actions(result)[:5]
    primary_index = next(
        (
            index
            for index, item in enumerate(result)
            if _pricing_method_action_is_primary(str(item.get("action") or ""))
        ),
        -1,
    )
    if primary_index >= 0:
        primary = dict(result.pop(primary_index))
        primary["priority"] = "P0"
        result.insert(0, primary)  # type: ignore[arg-type]
    elif (
        not result
        or _looks_like_source_repair_action(str(result[0].get("action") or ""))
        or _pricing_method_action_is_backend_first(str(result[0].get("action") or ""))
    ):
        refs = _dedupe([
            str(ref.get("refId") or "")
            for ref in _all_evidence_refs(evidence_package)
            if str(ref.get("refId") or "").strip()
        ])[:3]
        result.insert(0, {
            "action": primary_action,
            "rationale": (
                "先把已验证市场证据和用户材料方法论转成用户可读的定价页；"
                "MSRP 来源验证保留为后续补证动作，不能抢占主业务建议。"
            ),
            "priority": "P0",
            "evidenceRefs": refs,
            "citationIds": [],
        })
    return _dedupe_actions(result)[:5]


def _pricing_method_action_is_primary(action: str) -> bool:
    text = _normalize_question_text(action)
    if not text or _looks_like_source_repair_action(text):
        return False
    if _pricing_method_action_is_backend_first(text):
        return False
    return _contains_any(
        text,
        (
            "一页汇报",
            "定价页",
            "定价建议",
            "验证版价格",
            "价格走廊",
            "价格矩阵",
            "高配主推",
            "低配锚点",
            "竞品走廊",
            "竞品价格",
            "pricing corridor",
        ),
    )


def _pricing_method_action_is_backend_first(action: str) -> bool:
    text = _normalize_question_text(action)
    return _contains_any(
        text,
        (
            "补跑",
            "调用",
            "来源验证表",
            "review queue",
            "source repair",
            "官方价格候选",
            "来源草稿",
            "生成当前价格记录",
        ),
    )


def _specific_voc_recommended_actions(
    actions: list[RecommendedAction],
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    question_text: str,
) -> list[RecommendedAction]:
    primary_action = _voc_primary_action(country_label, question_text)
    result = [dict(item) for item in actions if isinstance(item, dict)]
    if result and _voc_action_is_primary(str(result[0].get("action") or ""), question_text):
        result[0]["priority"] = "P0"
        return _dedupe_actions(result)[:5]
    primary_index = next(
        (
            index
            for index, item in enumerate(result)
            if _voc_action_is_primary(str(item.get("action") or ""), question_text)
        ),
        -1,
    )
    if primary_index >= 0:
        primary = dict(result.pop(primary_index))
        primary["priority"] = "P0"
        result.insert(0, primary)  # type: ignore[arg-type]
    elif not result or _looks_like_source_repair_action(str(result[0].get("action") or "")):
        refs = _dedupe([
            str(ref.get("refId") or "")
            for ref in _all_evidence_refs(evidence_package)
            if str(ref.get("refId") or "").strip()
        ])[:3]
        result.insert(0, {
            "action": primary_action,
            "rationale": (
                "先把 VOC 问题转成用户可评审的主题/场景/产品动作验证表；"
                "外部来源修复保留为补证动作，不能抢占主业务建议。"
            ),
            "priority": "P0",
            "evidenceRefs": refs,
            "citationIds": [],
        })
    return _dedupe_actions(result)[:5]


def _voc_primary_action(country_label: str, question_text: str) -> str:
    if "v2h" in _normalize_question_text(question_text):
        regional_scope = _market_regional_scope(country_label)
        return f"抓取{regional_scope} V2H 用户原声和媒体测评证据"
    return "生成 VOC 来源验证表、主题聚类和产品动作矩阵"


def _voc_action_is_primary(action: str, question_text: str) -> bool:
    text = _normalize_question_text(action)
    if not text or _looks_like_source_repair_action(text):
        return False
    if "v2h" in _normalize_question_text(question_text):
        return "v2h" in text and _contains_any(
            text,
            ("用户原声", "媒体测评", "抓取", "验证表", "三场景", "家庭能源", "冬季备份", "科技形象", "话术"),
        )
    return _contains_any(text, ("voc", "主题", "聚类", "产品动作", "验证表", "痛点"))


def _specific_policy_recommended_actions(
    actions: list[RecommendedAction],
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    question_text: str,
) -> list[RecommendedAction]:
    primary_action = _policy_primary_action(country_label, question_text, evidence_package)
    result = [dict(item) for item in actions if isinstance(item, dict)]
    if result and _policy_action_is_primary(str(result[0].get("action") or ""), question_text):
        result[0]["priority"] = "P0"
        return _dedupe_actions(result)[:5]
    primary_index = next(
        (
            index
            for index, item in enumerate(result)
            if _policy_action_is_primary(str(item.get("action") or ""), question_text)
        ),
        -1,
    )
    if primary_index >= 0:
        primary = dict(result.pop(primary_index))
        primary["priority"] = "P0"
        result.insert(0, primary)  # type: ignore[arg-type]
    elif not result or _looks_like_source_repair_action(str(result[0].get("action") or "")):
        refs = _dedupe([
            str(ref.get("refId") or "")
            for ref in _all_evidence_refs(evidence_package)
            if str(ref.get("refId") or "").strip()
        ])[:3]
        result.insert(0, {
            "action": primary_action,
            "rationale": (
                "先把政策问题转成车型、价格、渠道和 TCO 的业务验证表；"
                "外部官方来源修复保留为补证动作，不能抢占主业务建议。"
            ),
            "priority": "P0",
            "evidenceRefs": refs,
            "citationIds": [],
        })
    return _dedupe_actions(result)[:5]


def _policy_primary_action(
    country_label: str,
    question_text: str,
    evidence_package: dict[str, Any],
) -> str:
    text = _normalize_question_text(question_text)
    if ("co2" in text or "co₂" in text) and "phev" in text:
        return "核对 PHEV 认证 CO2、税率阶梯、company car 计算公式和发布日期"
    if _is_phev_fleet_leasing_question(text):
        return "建立 PHEV fleet leasing TCO 表"
    if "company car" in text and "bev" in text and "phev" in text:
        return "建立 BEV/PHEV company car benefit 对比表"
    if "elbilspremien" in text:
        return "补齐官方政策原文、发布日期、资格/价格上限，并生成受影响车型矩阵"
    if _is_bev_subsidy_cap_question(text):
        target_model = _policy_target_model(evidence_package, text)
        return f"核对{country_label} BEV 补贴价格上限是否仍有效及 {target_model} 是否适用"
    return "生成政策来源表、车型影响矩阵和渠道/TCO 下一步验证表"


def _policy_action_is_primary(action: str, question_text: str) -> bool:
    text = _normalize_question_text(action)
    question = _normalize_question_text(question_text)
    if not text or _looks_like_source_repair_action(text):
        return False
    if ("co2" in question or "co₂" in question) and "phev" in question:
        return "phev" in text and _contains_any(
            text,
            ("co2", "co₂", "认证", "税率", "company car", "benefit", "tco", "公式", "发布日期"),
        )
    if _is_phev_fleet_leasing_question(question):
        return _contains_any(text, ("phev", "leasing", "tco", "月供", "残值", "公司车"))
    if "company car" in question and "bev" in question and "phev" in question:
        return _contains_any(text, ("company car", "benefit", "bev/phev", "tco", "对比表"))
    if "elbilspremien" in question:
        return _contains_any(text, ("elbilspremien", "资格", "价格上限", "受影响车型", "官方政策"))
    if _is_bev_subsidy_cap_question(question):
        if "msrp" in text and _contains_any(text, ("补齐", "当前", "价格走廊", "月供")):
            return True
        return _contains_any(text, ("价格上限", "补贴", "资格", "当前 msrp", "适用"))
    return _contains_any(text, ("政策来源表", "车型影响矩阵", "渠道", "tco", "政策事实"))


def _specific_configuration_recommended_actions(
    actions: list[RecommendedAction],
    *,
    country_label: str,
    evidence_package: dict[str, Any],
) -> list[RecommendedAction]:
    matrix_action = _configuration_matrix_action(country_label, evidence_package)
    result: list[RecommendedAction] = []
    has_matrix_action = False
    for item in actions:
        action_text = _configuration_safe_action(str(item.get("action") or ""), evidence_package)
        if _configuration_action_is_strong(action_text):
            has_matrix_action = True
            result.append(item)
            continue
        result.append(item)
    if not has_matrix_action:
        refs = []
        for item in actions:
            evidence_refs = item.get("evidenceRefs") if isinstance(item, dict) else []
            if isinstance(evidence_refs, list):
                refs = [str(ref or "") for ref in evidence_refs if str(ref or "").strip()][:3]
                if refs:
                    break
        result.insert(
            0,
            {
                "action": matrix_action,
                "rationale": (
                    "配置问题必须先把目标车型和竞品放到同口径配置矩阵，"
                    "再用 MSRP、来源日期和用户场景判断可赢点、短板和主销配置。"
                ),
                "priority": "P0",
                "evidenceRefs": refs,
                "citationIds": [],
            },
        )
    return _dedupe_actions(result)


def _market_overview_inferred_segments(evidence_package: dict[str, Any]) -> list[str]:
    segments: list[str] = []
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "")
        match = re.search(r"(?:driveBySegment|segmentByFuel)\.([A-Za-z0-9 ]+)\.", label, flags=re.IGNORECASE)
        if not match:
            continue
        segment = _normalize_space(match.group(1))
        if segment:
            segments.append(segment)
    return _dedupe(segments)


def _market_overview_primary_powertrain(evidence_package: dict[str, Any]) -> str:
    powertrain = ""
    best_value = -1.0
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "")
        match = re.search(r"(?:powertrainMix|driveByFuel)\.([A-Za-z0-9]+)\.(?:sales|value|volume|count)$", label, flags=re.IGNORECASE)
        if not match:
            continue
        value = _numeric_ref_value(ref)
        if value is None or value <= best_value:
            continue
        powertrain = match.group(1).upper()
        best_value = float(value)
    return powertrain


def _generic_market_evidence_parts(evidence_package: dict[str, Any]) -> list[str]:
    refs = _all_evidence_refs(evidence_package)
    parts: list[str] = []
    market_size = _market_total_size_evidence(refs)
    if market_size:
        parts.append(market_size)
    monthly_trend = _market_monthly_series_evidence(evidence_package)
    if monthly_trend:
        parts.append(monthly_trend)
    annual_trend = _market_annual_total_trend_evidence(refs)
    if annual_trend:
        parts.append(annual_trend)
    powertrain = _market_powertrain_mix_evidence(evidence_package)
    if powertrain:
        parts.append(powertrain)
    segment = _market_segment_structure_evidence(refs)
    if segment:
        parts.append(segment)
    top_models = _market_top_models_evidence(refs)
    if top_models:
        parts.append(top_models)
    return _dedupe(parts)


def _market_monthly_series_evidence(evidence_package: dict[str, Any]) -> str:
    for tool in evidence_package.get("toolResults", []) if isinstance(evidence_package.get("toolResults"), list) else []:
        if not isinstance(tool, dict) or tool.get("toolName") != "query_time_series" or not tool.get("success"):
            continue
        query = tool.get("query") if isinstance(tool.get("query"), dict) else {}
        points: list[tuple[str, dict[str, Any], float]] = []
        for ref in _coerce_evidence_refs(tool.get("evidenceRefs")):
            label = str(ref.get("label") or "")
            match = re.match(r"monthSeries\.(.+)\.(?:sales|volume|registrations|value)$", label, flags=re.IGNORECASE)
            value = _numeric_ref_value(ref)
            if not match or value is None:
                continue
            points.append((match.group(1), ref, value))
        if len(points) < 2:
            continue
        first_period, first_ref, first_value = points[0]
        last_period, last_ref, last_value = points[-1]
        scope_parts = [
            str(query.get(key) or "").strip()
            for key in ("powertrain", "segment")
            if str(query.get(key) or "").strip()
        ]
        scope = " ".join(_dedupe(scope_parts)) or "目标市场"
        direction = "上升" if last_value > first_value else "下降" if last_value < first_value else "持平"
        recent = "、".join(
            f"{period} {_format_evidence_ref_value(ref)}"
            for period, ref, _value in points[-3:]
        )
        return (
            f"{scope} 月度注册量从 {first_period} {_format_evidence_ref_value(first_ref)}"
            f"到 {last_period} {_format_evidence_ref_value(last_ref)}，整体{direction}；"
            f"最近三个月为 {recent}"
        )
    return ""


def _market_total_size_evidence(refs: list[dict[str, Any]]) -> str:
    for ref in refs:
        label = str(ref.get("label") or "")
        if re.search(r"(?:marketSnapshot|contextSnapshot|crossCountry\.[^.]+)\.kpis\.cumulativeSales$", label, flags=re.IGNORECASE):
            value = _format_evidence_ref_value(ref)
            if value:
                return f"累计销量 {value}"
    for ref in refs:
        label = str(ref.get("label") or "").casefold()
        if any(token in label for token in ("totalvolume", "totalsales", "cumulativesales")):
            value = _format_evidence_ref_value(ref)
            if value:
                return f"市场规模 {value}"
    return ""


def _market_annual_total_trend_evidence(
    refs: list[dict[str, Any]],
    *,
    current_year: int | None = None,
) -> str:
    points: list[tuple[int, str, float | None]] = []
    for ref in refs:
        label = str(ref.get("label") or "").strip()
        match = re.search(
            r"(?:^|\.)(?:yearSeries|annualSeries)\.(\d{4})\.(?:value|sales|volume|registrations|total)$",
            label,
            flags=re.IGNORECASE,
        )
        if not match:
            continue
        value = _format_evidence_ref_value(ref)
        if not value:
            continue
        points.append((int(match.group(1)), value, _numeric_ref_value(ref)))
    if len(points) < 2:
        return ""
    deduped: dict[int, tuple[int, str, float | None]] = {}
    for point in points:
        deduped[point[0]] = point
    ordered = [deduped[year] for year in sorted(deduped)]
    if len(ordered) < 2:
        return ""
    active_year = current_year or datetime.now(timezone.utc).year
    completed = [point for point in ordered if point[0] < active_year]
    in_progress = [point for point in ordered if point[0] >= active_year]
    comparable = completed if len(completed) >= 2 else ordered
    first = comparable[0]
    last = comparable[-1]
    recent = comparable[-3:] if len(comparable) >= 3 else comparable
    recent_text = " -> ".join(f"{year} {value}" for year, value, _ in recent)
    movement = ""
    if first[2] is not None and last[2] is not None and first[2] > 0:
        change_pct = ((last[2] - first[2]) / first[2]) * 100
        if abs(change_pct) >= 0.5:
            direction = "增长" if change_pct > 0 else "下降"
            movement = f"，较{first[0]}年{direction}{abs(change_pct):.1f}%"
    in_progress_note = ""
    if in_progress:
        latest_year, latest_value, _ = in_progress[-1]
        in_progress_note = f"；{latest_year}年内累计 {latest_value}，不与完整年直接比较"
    return f"年度走势 {recent_text}{movement}{in_progress_note}"


def _market_powertrain_mix_evidence(evidence_package: dict[str, Any]) -> str:
    stats = _powertrain_stats_from_evidence(evidence_package)
    lines: list[str] = []
    for fuel in ("BEV", "PHEV", "HEV", "ICE"):
        values = stats.get(fuel, {})
        sales = str(values.get("sales") or "").strip()
        share = str(values.get("share") or "").strip()
        if sales and share:
            lines.append(f"{fuel} {sales} / {share}")
        elif sales:
            lines.append(f"{fuel} {sales}")
        elif share:
            lines.append(f"{fuel} {share}")
    if not lines:
        return ""
    return f"动力结构 {'，'.join(lines[:4])}"


def _market_segment_structure_evidence(refs: list[dict[str, Any]]) -> str:
    rows: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        match = re.search(r"(?:contextSnapshot|marketSnapshot)\.crossTabs\.driveBySegment\.([^.]+)\.(?:sales|value)$", label, flags=re.IGNORECASE)
        if not match:
            continue
        value = _format_evidence_ref_value(ref)
        if value:
            rows.append(f"{match.group(1)} {value}")
    if not rows:
        return ""
    return f"级别结构 {'，'.join(_dedupe(rows)[:4])}"


def _market_top_models_evidence(refs: list[dict[str, Any]]) -> str:
    rows: list[str] = []
    for ref in refs:
        label = str(ref.get("label") or "")
        match = re.search(r"(?:contextSnapshot\.)?topModels\.([^.]+)\.sales$", label, flags=re.IGNORECASE)
        if not match:
            continue
        value = _format_evidence_ref_value(ref)
        if value:
            rows.append(f"{match.group(1)} {value}")
    if not rows:
        return ""
    return f"Top models {'，'.join(_dedupe(rows)[:4])}"


def _market_snapshot_ref_value(
    evidence_package: dict[str, Any],
    *,
    required_tokens: tuple[str, ...],
) -> str:
    tokens = tuple(str(item or "").strip().casefold() for item in required_tokens if str(item or "").strip())
    if not tokens:
        return ""
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").casefold()
        if not all(token in label for token in tokens):
            continue
        value = _format_evidence_ref_value(ref)
        if value:
            return value
    return ""


def _market_suv_a0_a_structure_conclusion(
    *,
    country_label: str,
    evidence_package: dict[str, Any],
    action: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    suv_a0_sales = _market_cross_tab_ref_value(evidence_package, table="driveBySegment", row="SUV A0", metric="sales")
    suv_a_sales = _market_cross_tab_ref_value(evidence_package, table="driveBySegment", row="SUV A", metric="sales")
    suv_a0_bev = _market_cross_tab_ref_value(evidence_package, table="segmentByFuel", row="SUV A0", metric="BEV_pct")
    suv_a_bev = _market_cross_tab_ref_value(evidence_package, table="segmentByFuel", row="SUV A", metric="BEV_pct")
    suv_a0_phev = _market_cross_tab_ref_value(evidence_package, table="segmentByFuel", row="SUV A0", metric="PHEV_pct")
    suv_a_phev = _market_cross_tab_ref_value(evidence_package, table="segmentByFuel", row="SUV A", metric="PHEV_pct")
    suv_a0_4wd = _market_cross_tab_ref_value(evidence_package, table="driveBySegment", row="SUV A0", metric="4WD_pct")
    suv_a_4wd = _market_cross_tab_ref_value(evidence_package, table="driveBySegment", row="SUV A", metric="4WD_pct")
    suv_a0_a_concentration = _market_snapshot_ref_value(
        evidence_package,
        required_tokens=("suv", "a0", "concentration"),
    )

    segment_parts = []
    if suv_a0_sales:
        segment_parts.append(f"SUV A0 {suv_a0_sales}")
    if suv_a_sales:
        segment_parts.append(f"SUV A {suv_a_sales}")
    if not segment_parts and not suv_a0_a_concentration:
        return ""

    mix_parts = []
    if suv_a0_bev:
        mix_parts.append(f"SUV A0 BEV {suv_a0_bev}")
    if suv_a_bev:
        mix_parts.append(f"SUV A BEV {suv_a_bev}")
    if suv_a0_phev:
        mix_parts.append(f"SUV A0 PHEV {suv_a0_phev}")
    if suv_a_phev:
        mix_parts.append(f"SUV A PHEV {suv_a_phev}")
    if suv_a0_4wd:
        mix_parts.append(f"SUV A0 4WD {suv_a0_4wd}")
    if suv_a_4wd:
        mix_parts.append(f"SUV A 4WD {suv_a_4wd}")

    market = _market_label(country_label)
    if segment_parts:
        evidence_line = f"内部 JATO cross-tab 显示 {'、'.join(segment_parts)}"
    else:
        evidence_line = f"内部 JATO market snapshot 显示 SUV A0/A 集中度 {suv_a0_a_concentration}"
    if mix_parts:
        evidence_line = f"{evidence_line}；结构信号包括 {'、'.join(mix_parts[:4])}"
    if suv_a0_a_concentration and segment_parts:
        evidence_line = f"{evidence_line}；market snapshot 还显示 SUV A0/A 集中度 {suv_a0_a_concentration}"
    monthly_trend = _market_monthly_series_evidence(evidence_package)
    trend_line = f"月度走势方面，{monthly_trend}。" if monthly_trend else ""
    boundary = (
        "这已经可以作为主销结构假设和优先验证入口，但还不能替代 segment cross-tab 的销量、动力结构和驱动形式证据。"
        if not segment_parts
        else ""
    )
    business_subject = _market_business_subject(evidence_package)
    return (
        f"直接结论：{market} SUV A0/A 成为主销结构，不是因为“泛 SUV 热”，而是因为这些级别同时覆盖家庭空间、城市通勤、冬季通过性、公司车/私人两用和电动化成本平衡。"
        f"{evidence_line}。"
        f"{trend_line}"
        f"{boundary}"
        f"对{business_subject}的动作不是泛泛上 SUV，而是把 BEV/HEV/PHEV 分别落到 SUV A0/A 的主销价格带、续航/冬季包和配置价值。"
        f"下一步执行 {action or '把 SUV A0/A 拆到车型级竞品池、动力结构和价格/配置表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
    )


def _market_country_pair_sales_difference_conclusion(
    *,
    evidence_package: dict[str, Any],
    action: str,
    alignment_note: str,
    evidence_note: str,
    confidence_note: str,
) -> str:
    covered = _covered_country_entities(evidence_package)
    countries = _country_entities(evidence_package) or covered[:2]
    pair_label = _country_pair_label(countries)
    if len(countries) < 2:
        covered_text = "、".join(_country_label(item) for item in covered) if covered else "当前工具"
        return (
            "直接结论：当前不能生成跨国家销量差异结论；"
            f"本轮只有{covered_text}侧可引用市场证据，缺少用户请求两侧的 market snapshot / cross-country 证据。"
            "下一步必须执行 query_cross_country，并显式传入要比较的国家、总销量、BEV/PHEV/HEV mix、SUV segment 和 Top models。"
            f"证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    missing = _missing_country_entities(evidence_package, countries)
    covered_labels = [_country_label(item) for item in covered]
    missing_labels = [_country_label(item) for item in missing]
    if missing:
        covered_text = "、".join(covered_labels) if covered_labels else "当前国家"
        missing_text = "、".join(missing_labels)
        return (
            f"直接结论：当前不能把{pair_label}销量差异写成确定结论；本轮只有{covered_text}侧可引用市场证据，"
            f"缺少{missing_text}的 market snapshot / cross-country 证据。"
            f"可以先判断的是：{covered_text}侧证据只能解释本国市场结构、动力结构和 Top model，不足以证明两国差异原因。"
            "下一步必须执行 query_cross_country，拿到两国总销量、BEV/PHEV/HEV mix、SUV segment 和 Top models 后，再输出差异原因、产品进入顺序和图表。"
            f"证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )

    first_country, second_country = countries[:2]
    first_label = _country_label(first_country)
    second_label = _country_label(second_country)
    first_total = _cross_country_metric_value(evidence_package, first_country, ("kpis.totalSales", "kpis.cumulativeSales", "kpis.totalVolume", "sales"))
    second_total = _cross_country_metric_value(evidence_package, second_country, ("kpis.totalSales", "kpis.cumulativeSales", "kpis.totalVolume", "sales"))
    first_bev = _cross_country_metric_value(evidence_package, first_country, ("powertrainMix.BEV.share", "powertrainMix.BEV.sales", "BEV.share", "BEV.sales"))
    second_bev = _cross_country_metric_value(evidence_package, second_country, ("powertrainMix.BEV.share", "powertrainMix.BEV.sales", "BEV.share", "BEV.sales"))
    total_clause = ""
    if first_total and second_total:
        total_clause = f"双边证据显示{first_label}销量口径为 {first_total}，{second_label}为 {second_total}；"
    elif first_total or second_total:
        total_clause = f"双边证据里已有销量口径：{first_label} {first_total or '待补'}，{second_label} {second_total or '待补'}；"
    bev_clause = ""
    if first_bev or second_bev:
        bev_clause = f"BEV 结构口径为{first_label} {first_bev or '待补'}、{second_label} {second_bev or '待补'}；"
    business_subject = _market_business_subject(evidence_package)
    return (
        f"直接结论：{pair_label}销量差异可以进入双边对比判断，但结论必须沿着市场体量、动力结构、SUV/车型结构和渠道场景拆解，不能只做泛泛国家差异解释。"
        f"{total_clause}{bev_clause}"
        f"对{business_subject}的动作是先把 {first_label}/{second_label} 做成 side-by-side market table，再决定各市场的规模、公司车、高配验证、价格敏感和低风险配置分工。"
        f"下一步执行 {action or f'生成 {first_country} vs {second_country} market comparison table 和图表'}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
    )


def _country_pair_label(countries: list[str]) -> str:
    labels = [_country_label(item) for item in countries[:2] if _country_label(item)]
    if len(labels) >= 2:
        return f"{labels[0]}和{labels[1]}"
    return "两国"


def _market_business_subject(evidence_package: dict[str, Any]) -> str:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values: list[str] = []
    for key in ("models", "brands"):
        candidates = entities.get(key) if isinstance(entities.get(key), list) else []
        values.extend(
            _normalize_space(str(item or ""))
            for item in candidates
            if _normalize_space(str(item or ""))
        )
    names = _dedupe_entity_names_by_specificity(_dedupe(values))
    return " / ".join(names[:3]) if names else "目标产品组合"


def _country_entities(evidence_package: dict[str, Any]) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    countries = entities.get("countries") if isinstance(entities.get("countries"), list) else []
    result: list[str] = []
    for item in countries:
        country = _canonical_market_country(str(item or ""))
        if country and country not in result:
            result.append(country)
    return result


def _covered_country_entities(evidence_package: dict[str, Any]) -> list[str]:
    result: list[str] = []
    for tool in evidence_package.get("toolResults", []) if isinstance(evidence_package.get("toolResults"), list) else []:
        if not isinstance(tool, dict) or not tool.get("success"):
            continue
        refs = _coerce_evidence_refs(tool.get("evidenceRefs"))
        if not refs:
            continue
        tool_name = str(tool.get("toolName") or "")
        query = tool.get("query") if isinstance(tool.get("query"), dict) else {}
        if tool_name in {"query_country_snapshot", "build_market_chart", "query_with_filters"}:
            country = _canonical_market_country(str(query.get("country") or ""))
            if country and country not in result:
                result.append(country)
        for ref in refs:
            if not is_usable_evidence_ref(ref):
                continue
            label = str(ref.get("label") or "")
            match = re.search(r"crossCountry\.([^.]+)\.", label)
            if not match:
                continue
            country = _canonical_market_country(match.group(1))
            if country and country not in result:
                result.append(country)
    return result


def _missing_country_entities(evidence_package: dict[str, Any], countries: list[str]) -> list[str]:
    missing_names = {
        str(item.get("name") or "")
        for item in evidence_package.get("missingEvidence", [])
        if isinstance(item, dict)
    }
    covered = set(_covered_country_entities(evidence_package))
    result: list[str] = []
    for item in countries:
        country = _canonical_market_country(item)
        if not country:
            continue
        if f"missing_country_snapshot:{country}" in missing_names or country not in covered:
            result.append(country)
    return result


def _cross_country_metric_value(evidence_package: dict[str, Any], country: str, metric_suffixes: tuple[str, ...]) -> str:
    country_token = _canonical_market_country(country).casefold()
    suffixes = tuple(item.casefold() for item in metric_suffixes)
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "")
        label_folded = label.casefold()
        if f"crosscountry.{country_token}." not in label_folded:
            continue
        if not any(label_folded.endswith(suffix) or suffix in label_folded for suffix in suffixes):
            continue
        value = _format_evidence_ref_value(ref)
        if value:
            return value
    return ""


def _canonical_market_country(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    mapping = {
        "sweden": "Sweden",
        "瑞典": "Sweden",
        "finland": "Finland",
        "芬兰": "Finland",
        "norway": "Norway",
        "挪威": "Norway",
        "denmark": "Denmark",
        "丹麦": "Denmark",
        "hungary": "Hungary",
        "匈牙利": "Hungary",
        "germany": "Germany",
        "德国": "Germany",
    }
    return mapping.get(token.casefold(), token)


def _market_powertrain_compression_evidence_brief(evidence_package: dict[str, Any]) -> str:
    cross_country = _cross_country_powertrain_stats(evidence_package)
    if len(cross_country) >= 2:
        return _market_cross_country_powertrain_compression_brief(cross_country)
    stats = _powertrain_stats_from_evidence(evidence_package)
    bev = stats.get("BEV", {})
    hev = stats.get("HEV", {})
    phev = stats.get("PHEV", {})
    if not (bev or hev):
        return ""

    lines = [
        _market_powertrain_full_line("BEV", bev),
        _market_powertrain_full_line("HEV", hev),
        _market_powertrain_full_line("PHEV", phev),
    ]
    evidence_line = "；".join(line for line in lines if line)
    if not evidence_line:
        return ""

    comparison = _market_powertrain_comparison_phrase(bev, hev)
    if comparison:
        implication = (
            f"{comparison}；产品动作应把 BEV 当作高渗透/公司车压力源，"
            "把 HEV 定义到无稳定充电、价格敏感、低使用风险和 SUV A0/A 实用场景。"
        )
    else:
        implication = (
            "这说明结论不能停留在趋势判断，要把动力结构、渠道结构和主销级别拆开；"
            "BEV 负责验证政策/公司车压力，HEV 负责验证低风险替代和价格锚点空间。"
        )
    return f"市场证据：{evidence_line}。{implication}"


def _regional_scope_label(question: str, fallback: str) -> str:
    text = str(question or "").casefold()
    if any(token in text for token in ("北欧", "nordic", "nordics", "northern europe")):
        return "北欧"
    if any(token in text for token in ("scandinavia", "scandinavian", "斯堪的纳维亚")):
        return "斯堪的纳维亚"
    return fallback


def _cross_country_powertrain_stats(evidence_package: dict[str, Any]) -> dict[str, dict[str, dict[str, Any]]]:
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "")
        match = re.search(r"^crossCountry\.([^.]+)\.powertrainMix\.([^.]+)\.([^.]+)$", label)
        if not match:
            continue
        country, fuel, metric = match.group(1), match.group(2).upper(), match.group(3)
        if fuel not in {"BEV", "HEV", "PHEV"}:
            continue
        metric_key = _policy_metric_key(metric) or _powertrain_metric_key_from_label(label, ref)
        if metric_key not in {"sales", "share"}:
            continue
        bucket = result.setdefault(country, {}).setdefault(fuel, {})
        bucket[metric_key] = _format_evidence_ref_value(ref)
        numeric = _numeric_ref_value(ref)
        if numeric is not None:
            bucket[f"{metric_key}Value"] = numeric
    return result


def _market_cross_country_powertrain_compression_brief(
    stats_by_country: dict[str, dict[str, dict[str, Any]]],
) -> str:
    country_lines: list[str] = []
    compression_count = 0
    comparable_count = 0
    for country, fuels in stats_by_country.items():
        bev = fuels.get("BEV", {})
        hev = fuels.get("HEV", {})
        if not bev and not hev:
            continue
        bev_text = _cross_country_powertrain_short_line("BEV", bev)
        hev_text = _cross_country_powertrain_short_line("HEV", hev)
        if bev.get("salesValue") is not None and hev.get("salesValue") is not None:
            comparable_count += 1
            if float(bev["salesValue"]) > float(hev["salesValue"]):
                compression_count += 1
        if bev_text or hev_text:
            country_lines.append(f"{country} {bev_text}{'，' if bev_text and hev_text else ''}{hev_text}")
    if not country_lines:
        return ""
    if comparable_count and compression_count == comparable_count:
        implication = "这些国家的可比样本里 BEV 规模均高于 HEV，说明 BEV 对 HEV 的空间压缩是区域性压力。"
    elif comparable_count and compression_count > 0:
        implication = "不同国家节奏不一致，BEV 已在部分市场压过 HEV，但仍需要保留 HEV 的价格敏感和无稳定充电场景。"
    else:
        implication = "当前区域证据能支持 BEV/HEV 空间对比，但还不足以判断所有国家的压缩强度。"
    return f"区域证据：{'；'.join(country_lines[:4])}。{implication}"


def _cross_country_powertrain_short_line(fuel: str, stats: dict[str, Any]) -> str:
    if not stats:
        return ""
    parts: list[str] = []
    if stats.get("sales"):
        parts.append(str(stats["sales"]))
    if stats.get("share"):
        parts.append(f"份额 {stats['share']}")
    if not parts:
        return fuel
    return f"{fuel} " + "，".join(parts)


def _market_powertrain_full_line(fuel: str, stats: dict[str, str]) -> str:
    if not stats:
        return ""
    parts: list[str] = []
    if stats.get("sales"):
        parts.append(str(stats["sales"]))
    if stats.get("share"):
        parts.append(f"份额 {stats['share']}")
    if stats.get("business"):
        parts.append(f"Business {stats['business']}")
    if stats.get("private"):
        parts.append(f"Private {stats['private']}")
    if not parts:
        return fuel
    return f"{fuel} " + "，".join(parts)


def _market_powertrain_comparison_phrase(bev: dict[str, str], hev: dict[str, str]) -> str:
    sales_phrase = _market_powertrain_metric_comparison("BEV", bev.get("sales", ""), "HEV", hev.get("sales", ""), "规模")
    if sales_phrase:
        return sales_phrase
    share_phrase = _market_powertrain_metric_comparison("BEV", bev.get("share", ""), "HEV", hev.get("share", ""), "份额")
    if share_phrase:
        return share_phrase
    return ""


def _market_powertrain_metric_comparison(
    left_label: str,
    left_value: str,
    right_label: str,
    right_value: str,
    metric_label: str,
) -> str:
    left = _first_number(left_value)
    right = _first_number(right_value)
    if left is None or right is None or right <= 0:
        return ""
    if left >= right * 1.8:
        return f"{left_label} 的{metric_label}明显高于 {right_label}"
    if left > right:
        return f"{left_label} 的{metric_label}高于 {right_label}"
    if right >= left * 1.8:
        return f"{right_label} 的{metric_label}明显高于 {left_label}"
    return f"{left_label} 与 {right_label} 的{metric_label}接近，需要看渠道和级别结构"


def _first_number(value: str) -> float | None:
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", str(value or ""))
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _pricing_target_price_verdict(
    evidence_package: dict[str, Any],
    *,
    country_label: str,
    confidence: str,
    alignment: EvidenceAlignment,
    refs: int,
    first_action: str,
) -> str:
    target = _pricing_target_range(evidence_package)
    if not target:
        return ""
    stats = _pricing_price_stats(evidence_package)
    target_text = _format_price_range(target)
    action = first_action or "补齐官方 MSRP、竞品价格走廊和月供/RV 后再定最终价格"
    evidence_note = _evidence_count_note(refs)
    confidence_note = _confidence_label(confidence)
    alignment_note = _alignment_label(alignment["status"])
    official_gap = _has_missing_evidence(evidence_package, "current_msrp")
    gap_text = "但它仍是用户给定场景价，不是官方 MSRP，需要交叉验证；" if official_gap else ""
    if stats.get("min") is not None and stats.get("max") is not None:
        min_price = float(stats["min"])
        max_price = float(stats["max"])
        target_min = float(target["min"])
        target_max = float(target["max"])
        inside = min_price <= target_min and target_max <= max_price
        position = _target_range_position_statement(target, stats)
        reasonableness = _target_range_reasonableness_label(target_min, target_max, min_price, max_price) if not inside else "具备继续验证的合理性"
        return (
            f"直接结论：{country_label} 目标价 {target_text} {reasonableness}：它{position}。"
            f"{gap_text}下一步应执行 {action}。"
            f"证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
        )
    return (
        f"直接结论：{country_label} 目标价 {target_text} 可以作为定价场景输入，但当前缺少可引用竞品价格走廊，不能判断最终合理性。"
        f"{gap_text}下一步应执行 {action}。证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
    )


def _pricing_user_relative_delta(evidence_package: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").casefold()
        if "user supplied relative price delta" in label:
            value = _format_evidence_ref_value(ref)
            if value:
                result["delta"] = value
        elif "user supplied price-delta direction" in label:
            direction = str(ref.get("value") or "").strip().casefold()
            if direction:
                result["direction"] = direction
    return result


def _pricing_delta_text(delta: dict[str, str]) -> str:
    value = str(delta.get("delta") or "").strip()
    if not value:
        return ""
    value = value.replace("价差", "").strip()
    direction = str(delta.get("direction") or "").strip().casefold()
    if direction in {"cheaper", "lower", "below"}:
        return f"{value}"
    if direction in {"more_expensive", "higher", "above"}:
        return f"高 {value}"
    return value


def _pricing_stats_evidence_line(stats: dict[str, float]) -> str:
    if not stats:
        return ""
    parts: list[str] = []
    if stats.get("min") is not None and stats.get("max") is not None:
        parts.append(
            "参考价格样本区间约 "
            f"{_format_price_sample_number(float(stats['min']))}-{_format_price_sample_number(float(stats['max']))}"
        )
    if stats.get("median") is not None:
        parts.append(f"中位数约 {_format_price_sample_number(float(stats['median']))}")
    if stats.get("avg") is not None:
        parts.append(f"均值约 {_format_price_sample_number(float(stats['avg']))}")
    return "，".join(parts)


def _format_price_sample_number(value: float) -> str:
    return f"{int(round(value)):,}"


def _pricing_live_market_evidence_note(evidence_package: dict[str, Any]) -> str:
    stats = _powertrain_stats_from_evidence(evidence_package)
    requested_fuels = _pricing_requested_powertrains(evidence_package)
    fuels = requested_fuels or ["HEV", "BEV", "PHEV"]
    lines = [_pricing_market_signal_line(fuel, stats.get(fuel, {})) for fuel in fuels]
    evidence_line = "；".join(line for line in lines if line)
    if not evidence_line:
        return ""
    note = f"本轮 JATO 市场信号显示 {evidence_line}"
    material_market = _pricing_user_material_market_window(evidence_package)
    if material_market and any(token in material_market for token in ("22,816", "22816")):
        note += "；用户材料里的 HEV 22,816 台属于材料周期口径，需和本轮 JATO 数据周期统一后再写进最终定价页"
    else:
        note += "；这能支撑动力结构背景，但仍需和车型级 HEV SUV A0/A、竞品销量和价格口径交叉验证"
    return f"{note}。"


def _pricing_requested_powertrains(evidence_package: dict[str, Any]) -> list[str]:
    entities = evidence_package.get("entities") if isinstance(evidence_package.get("entities"), dict) else {}
    values: list[str] = []
    for key in ("powertrains", "powertrain", "fuelTypes", "models", "competitors"):
        raw = entities.get(key)
        if isinstance(raw, list):
            values.extend(str(item or "") for item in raw)
        elif isinstance(raw, str):
            values.append(raw)
    requested: list[str] = []
    text = " ".join(values).casefold()
    for fuel in ("HEV", "PHEV", "BEV", "MHEV", "ICE"):
        token = fuel.casefold()
        if re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text):
            requested.append(fuel)
    return _dedupe(requested)


def _pricing_market_signal_line(fuel: str, stats: dict[str, str]) -> str:
    if not stats:
        return ""
    volume = str(stats.get("sales") or "").strip()
    share = str(stats.get("share") or "").strip()
    structure_parts: list[str] = []
    if stats.get("2wd"):
        structure_parts.append(f"2WD {stats['2wd']}")
    if stats.get("4wd"):
        structure_parts.append(f"4WD {stats['4wd']}")
    if stats.get("business"):
        structure_parts.append(f"Business {stats['business']}")
    if stats.get("private"):
        structure_parts.append(f"Private {stats['private']}")
    lead = f"{fuel} 总量信号 {volume}" if volume else f"{fuel} 份额信号 {share}" if share else fuel
    if structure_parts:
        return f"{lead}；{fuel} 结构拆分 {'，'.join(structure_parts)}"
    return lead


def _pricing_user_material_market_window(evidence_package: dict[str, Any]) -> str:
    for ref in _all_evidence_refs(evidence_package):
        if not _pricing_user_material_ref_matches_scope(ref, evidence_package):
            continue
        label = str(ref.get("label") or "").casefold()
        if "user material" not in label or "market window" not in label:
            continue
        value = str(ref.get("value") or "").strip()
        if value:
            return value
    return ""


def _pricing_has_model_price_ref(evidence_package: dict[str, Any], model: str) -> bool:
    model_key = _model_key(model)
    if not model_key:
        return False
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "")
        label_key = _model_key(label)
        if model_key not in label_key:
            continue
        if not any(token in label.casefold() for token in ("msrp", "price", "avgprice", "minprice", "maxprice")):
            continue
        if _numeric_ref_value(ref) is not None:
            return True
    return False


def _pricing_has_any_model_price_ref(evidence_package: dict[str, Any], models: tuple[str, ...]) -> bool:
    return any(_pricing_has_model_price_ref(evidence_package, model) for model in models)


def _hev_phev_route_executive_conclusion(
    *,
    country_label: str,
    confidence: str,
    alignment: EvidenceAlignment,
    refs: int,
    first_action: str,
    evidence_package: dict[str, Any],
) -> str:
    action = first_action or "建立 HEV vs PHEV 场景决策表"
    evidence_note = _evidence_count_note(refs)
    confidence_note = _confidence_label(confidence)
    alignment_note = _alignment_label(alignment["status"])
    missing_note = _missing_evidence_note(evidence_package)
    route_evidence_note = _hev_phev_route_evidence_note(evidence_package)
    external_context_note = _hev_phev_route_external_context_note(evidence_package)
    if route_evidence_note:
        boundary = route_evidence_note
        if _has_market_decision_blocking_gap(evidence_package):
            boundary += f"证据边界：当前仍缺少{missing_note}，所以这是产品路线初判，不是最终销量/份额结论。"
    elif external_context_note:
        boundary = (
            f"{external_context_note}"
            f"证据边界：当前仍缺少{missing_note}，外部背景不能替代内部销量、动力结构和车型证据，"
            "所以这是产品路线初判，不是最终销量/份额结论。"
        )
    else:
        boundary = (
            f"证据边界：当前仍缺少{missing_note}，所以这是产品路线初判，不是最终销量/份额结论。"
            if _has_market_decision_blocking_gap(evidence_package)
            else "证据边界：后续仍要用销量、份额、税费和月供数据校准主推权重。"
        )
    return (
        f"直接结论：{_market_label(country_label)}现阶段不要把 HEV/PHEV 简化成二选一；"
        "第一版更稳的是 HEV 做低风险主线，PHEV 做公司车/TCO 验证线。"
        "HEV 负责承接价格敏感、无稳定充电、低使用风险和换购省心场景；"
        "PHEV 只有在公司车税费、月供、残值、长途里程或补能条件上形成优势时，才升级为主推。"
        f"{boundary}下一步执行：{action}。"
        f"证据状态：{alignment_note}，{evidence_note}，置信度{confidence_note}。"
    )


def _hev_phev_route_evidence_note(evidence_package: dict[str, Any]) -> str:
    stats = _powertrain_stats_from_evidence(evidence_package)
    lines = [
        _hev_phev_route_powertrain_line("HEV", stats.get("HEV", {})),
        _hev_phev_route_powertrain_line("PHEV", stats.get("PHEV", {})),
    ]
    evidence_line = "；".join(line for line in lines if line)
    if not evidence_line:
        return ""
    segment_line = _segment_by_fuel_route_evidence_note(evidence_package, fuels=("HEV", "PHEV"))
    if segment_line:
        evidence_line = f"{evidence_line}；{segment_line}"
    missing = [fuel for fuel in ("HEV", "PHEV") if not stats.get(fuel)]
    if missing:
        return f"已查数据：{evidence_line}；本轮缺少{'/'.join(missing)}可比销量/份额，所以不能把路线权重写死。"
    return f"已查数据：{evidence_line}。"


def _hev_phev_route_external_context_note(evidence_package: dict[str, Any]) -> str:
    lines: list[str] = []
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool_result in tool_results:
        if not isinstance(tool_result, dict) or not tool_result.get("success"):
            continue
        source_type = str(tool_result.get("sourceType") or "").strip().casefold()
        if source_type not in {"web", "policy", "news", "external"}:
            continue
        refs = tool_result.get("evidenceRefs") if isinstance(tool_result.get("evidenceRefs"), list) else []
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            line = _evidence_ref_digest_line(ref)
            if line:
                lines.append(line)
            if len(_dedupe(lines)) >= 3:
                break
        if len(_dedupe(lines)) >= 3:
            break
    if not lines:
        return ""
    return f"外部背景：{'；'.join(_dedupe(lines)[:3])}。"


def _hev_phev_route_powertrain_line(fuel: str, stats: dict[str, str]) -> str:
    if not stats:
        return ""
    parts: list[str] = []
    if stats.get("sales"):
        parts.append(str(stats["sales"]))
    if stats.get("share"):
        parts.append(f"份额 {stats['share']}")
    if stats.get("business"):
        parts.append(f"Business {stats['business']}")
    if stats.get("private"):
        parts.append(f"Private {stats['private']}")
    if stats.get("2wd"):
        parts.append(f"2WD {stats['2wd']}")
    if stats.get("4wd"):
        parts.append(f"4WD {stats['4wd']}")
    if not parts:
        return fuel
    return f"{fuel} " + "，".join(parts[:5])


def _segment_by_fuel_route_evidence_note(evidence_package: dict[str, Any], *, fuels: tuple[str, ...]) -> str:
    rows: dict[str, dict[str, str]] = {}
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip()
        lower = label.casefold()
        if "segmentbyfuel" not in lower:
            continue
        parts = label.split(".")
        upper_parts = [part.upper() for part in parts]
        fuel = next((item for item in fuels if item in upper_parts), "")
        if not fuel:
            continue
        segment = _segment_by_fuel_label_segment(parts, fuel)
        if not segment:
            continue
        metric = str(parts[-1] if parts else "").strip().casefold()
        metric_key = _policy_metric_key(metric) or _powertrain_metric_key_from_label(label, ref)
        if metric_key not in {"sales", "share"}:
            continue
        value = _format_evidence_ref_value(ref)
        if not value:
            continue
        rows.setdefault(segment, {})[fuel] = f"{fuel} {value}"
    lines = [
        f"{segment} 细分 " + "，".join(value for fuel, value in fuel_values.items() if value)
        for segment, fuel_values in rows.items()
        if fuel_values
    ]
    return "；".join(lines[:3])


def _segment_by_fuel_label_segment(parts: list[str], fuel: str) -> str:
    upper_fuel = fuel.upper()
    for index, part in enumerate(parts):
        if part.upper() != upper_fuel:
            continue
        if index <= 0:
            return ""
        segment_parts: list[str] = []
        cursor = index - 1
        while cursor >= 0 and parts[cursor].casefold() not in {"segmentbyfuel", "crosstabs", "contextsnapshot"}:
            segment_parts.insert(0, parts[cursor])
            cursor -= 1
        return " ".join(segment_parts).strip()
    return ""


def _pricing_target_range(evidence_package: dict[str, Any]) -> dict[str, float | str] | None:
    refs = _all_evidence_refs(evidence_package)
    values: dict[str, float | str] = {}
    for ref in refs:
        label = str(ref.get("label") or "").lower()
        if "user supplied own-model target price" not in label:
            continue
        numeric = _numeric_ref_value(ref)
        if numeric is None:
            continue
        if " min" in label:
            values["min"] = numeric
        elif " max" in label:
            values["max"] = numeric
        elif " midpoint" in label:
            values["midpoint"] = numeric
        values["currency"] = str(ref.get("unit") or values.get("currency") or "currency")
    if "min" not in values or "max" not in values:
        return None
    if "midpoint" not in values:
        values["midpoint"] = (float(values["min"]) + float(values["max"])) / 2
    return values


def _pricing_price_stats(evidence_package: dict[str, Any]) -> dict[str, float]:
    result: dict[str, float] = {}
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").lower()
        if "pricestats." not in label:
            continue
        numeric = _numeric_ref_value(ref)
        if numeric is None:
            continue
        if label.endswith(".min"):
            result["min"] = numeric
        elif label.endswith(".max"):
            result["max"] = numeric
        elif label.endswith(".avg"):
            result["avg"] = numeric
        elif label.endswith(".median"):
            result["median"] = numeric
    return result


def _pricing_visual_backbone_note(evidence_package: dict[str, Any]) -> str:
    if _pricing_chart_anchor_count(evidence_package) >= 2:
        if _pricing_has_reference_sample_stats(evidence_package) and not _pricing_has_explicit_corridor_ref(evidence_package):
            return (
                "展示骨架：先看 Pricing reference sample chart 判断目标价/本车型 MSRP 相对参考价格样本的位置，"
                "再用 Pricing evidence table 补月供/RV、PVA、配置价值和官方竞品价格边界。"
            )
        return (
            "展示骨架：先看 Pricing corridor chart 判断目标价/本车型 MSRP 相对竞品走廊的位置，"
            "再用 Pricing evidence table 补月供/RV、PVA 和配置价值边界。"
        )
    if evidence_ref_count(evidence_package) > 0:
        if _pricing_has_reference_sample_stats(evidence_package) and not _pricing_has_explicit_corridor_ref(evidence_package):
            return "展示骨架：用 Pricing evidence table 把 MSRP、目标价、参考价格样本、月供/RV 和配置价值放进同一张决策表。"
        return "展示骨架：用 Pricing evidence table 把 MSRP、目标价、竞品走廊、月供/RV 和配置价值放进同一张决策表。"
    return ""


def _pricing_chart_anchor_count(evidence_package: dict[str, Any]) -> int:
    count = 0
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").lower()
        if _pricing_visual_ref_is_noise(label):
            continue
        if "competitor corridor" in label or "price corridor" in label:
            count += min(2, len(_numeric_ref_values(ref)))
            continue
        if _pricing_visual_label_is_chart_anchor(label) and _numeric_ref_value(ref) is not None:
            count += 1
    return count


def _pricing_has_reference_sample_stats(evidence_package: dict[str, Any]) -> bool:
    labels = {str(ref.get("label") or "").strip().casefold() for ref in _all_evidence_refs(evidence_package)}
    return any(label in {"pricestats.min", "pricestats.max", "pricestats.avg", "pricestats.median"} for label in labels)


def _pricing_has_explicit_corridor_ref(evidence_package: dict[str, Any]) -> bool:
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip().casefold()
        if _pricing_visual_ref_is_noise(label):
            continue
        if "competitor corridor" in label or "price corridor" in label:
            return True
    return False


def _pricing_visual_ref_is_noise(label: str) -> bool:
    return any(
        token in label
        for token in (
            "monthly",
            "leasing",
            "leasepayment",
            "residual",
            "rv",
            "pva",
            "coverage",
            "gap",
            ".powertrain",
            ".fuel",
        )
    )


def _pricing_visual_label_is_chart_anchor(label: str) -> bool:
    if "target price midpoint" in label:
        return True
    if any(token in label for token in ("main trim msrp", "own-model msrp", "current msrp", "premium msrp")):
        return True
    if label in {"pricestats.min", "pricestats.max", "pricestats.avg", "pricestats.median"}:
        return True
    if label.startswith("pricing.records."):
        metric = label.split(".")[-1]
        return metric in {"msrp", "price", "avgprice", "medianprice", "minprice", "maxprice"}
    return False


def _competitor_visual_backbone_note(evidence_package: dict[str, Any]) -> str:
    metric = _competitor_chart_metric(evidence_package)
    if metric:
        return (
            f"展示骨架：先看 Competitor {metric} chart 判断竞品量级，"
            "再用 Competitor comparison table 拆级别、动力类型、价格/配置差异和产品动作。"
        )
    if _has_msrp_source_repair_gap(evidence_package):
        return (
            "展示骨架：先看 MSRP source validation table 补齐本车型/竞品官方价格来源，"
            "再用 Competitor comparison table 拆级别、动力类型、价格/配置差异和产品动作。"
        )
    if evidence_ref_count(evidence_package) > 0:
        return "展示骨架：用 Competitor comparison table 把竞品角色、级别、动力、价格/配置差异和产品动作放在一张矩阵里。"
    return ""


def _competitor_chart_metric(evidence_package: dict[str, Any]) -> str:
    buckets: dict[str, set[str]] = {"sales": set(), "share": set(), "price": set()}
    targets, competitors = _competitor_requested_entities(evidence_package, "")
    requested_models = [*targets, *competitors]
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").strip()
        metric = label.lower().split(".")[-1]
        model = _competitor_chart_model_from_label(label)
        if not model:
            continue
        if requested_models and not _model_name_in_list(model, requested_models):
            continue
        if _numeric_ref_value(ref) is None:
            continue
        if metric in {"sales", "value", "volume", "count"}:
            buckets["sales"].add(model)
        elif metric == "share":
            buckets["share"].add(model)
        elif metric in {"avgprice", "price", "msrp", "minprice", "maxprice"}:
            buckets["price"].add(model)
    for key in ("sales", "share", "price"):
        if len(buckets[key]) >= 2:
            return key
    return ""


def _has_msrp_source_repair_gap(evidence_package: dict[str, Any]) -> bool:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").lower()
        reason = str(item.get("reason") or "").lower()
        haystack = f"{name} {reason}"
        if any(token in haystack for token in ("current_msrp", "own_model_price", "no_current_prices", "source_repair", "msrp")):
            return True
    return False


def _artifact_visual_backbone_note(intent: str, evidence_package: dict[str, Any]) -> str:
    if evidence_ref_count(evidence_package) <= 0:
        return ""
    intent_key = str(intent or "")
    if intent_key == "pricing_analysis":
        return _pricing_visual_backbone_note(evidence_package)
    if intent_key == "competitor_compare":
        return _competitor_visual_backbone_note(evidence_package)
    if intent_key == "market_overview":
        return _market_visual_backbone_note(evidence_package)
    if intent_key == "news_policy_search":
        return "展示骨架：先看 Policy / news evidence table 核对来源、日期、适用对象、影响车型和风险，再转成价格/渠道动作。"
    if intent_key == "configuration_analysis":
        return "展示骨架：先看 Configuration validation matrix，把配置项拆成 validation data、source/tool、acceptance criteria、current status 和 priority。"
    if intent_key == "inventory_analysis":
        return "展示骨架：先看 BOM / entity mapping validation table，核对 PI、market overlay、business variant、material code、颜色、生命周期和可编辑数量关系；有真实库存/BOM rows 时再看 Inventory / BOM evidence table。"
    if intent_key == "voc_analysis":
        return "展示骨架：先看 VOC evidence table，把来源、用户/媒体信号、产品含义和验证状态放在同一张表。"
    if intent_key == "report_generation":
        if _pricing_chart_anchor_count(evidence_package) >= 2:
            return (
                "展示骨架：先看 PPT-ready block 拿一页结论，再看 Pricing corridor chart 判断价格位置，"
                "最后用 Pricing evidence table 和 Report evidence appendix 核对价格、PVA、竞品池和来源边界。"
            )
        return "展示骨架：先看 PPT-ready block，再用 Report evidence appendix 核对每条证据的来源、业务用途和下一步动作。"
    return ""


def _market_visual_backbone_note(evidence_package: dict[str, Any]) -> str:
    if _market_cross_tab_sales_anchor_count(evidence_package) >= 2:
        return (
            "展示骨架：先看 Market structure chart 把 HEV、SUV A0/A 等 cross-tab 销量变成可视化，"
            "再用 Key metrics 和 Market decision table 把动力结构、级别结构和 top models 转成机会 segment 与产品动作。"
        )
    if _market_powertrain_chart_anchor_count(evidence_package) >= 2:
        return (
            "展示骨架：先看 Powertrain mix chart 判断 BEV/PHEV/HEV 量级，再用 Key metrics 和 Market decision table "
            "把销量/份额、级别结构和 top models 转成机会 segment 与产品动作。"
        )
    return (
        "展示骨架：先看 Key metrics 和 Market decision table，把销量/份额、动力结构、级别结构和 top models "
        "转成机会 segment 与产品动作。"
    )


def _market_cross_tab_sales_anchor_count(evidence_package: dict[str, Any]) -> int:
    count = 0
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").lower()
        if "crosstabs" not in label:
            continue
        metric = label.split(".")[-1]
        if metric not in {"sales", "volume", "registrations", "registration", "value", "count", "_total"}:
            continue
        if _numeric_ref_value(ref) is not None:
            count += 1
    return count


def _market_powertrain_chart_anchor_count(evidence_package: dict[str, Any]) -> int:
    count = 0
    for ref in _all_evidence_refs(evidence_package):
        label = str(ref.get("label") or "").lower()
        if not any(token in label for token in ("powertrainmix", "bev", "phev", "hev", "mhev", "ice", "reev")):
            continue
        metric = label.split(".")[-1]
        if metric not in {"sales", "volume", "registrations", "registration", "value", "count", "_total", "share", "mix", "penetration"} and not metric.endswith("_pct"):
            continue
        if _numeric_ref_value(ref) is not None:
            count += 1
    return count


def _competitor_chart_model_from_label(label: str) -> str:
    text = str(label or "").strip()
    parts = text.split(".")
    if len(parts) < 2:
        return ""
    metric = parts[-1].lower()
    if metric not in {"sales", "value", "volume", "count", "share", "avgprice", "price", "msrp", "minprice", "maxprice"}:
        return ""
    if text.lower().startswith("sales.rankings.") and len(parts) >= 4:
        return parts[2].strip()
    if text.lower().startswith("pricing.records.") and len(parts) >= 4:
        return ".".join(parts[2:-1]).strip()
    return ".".join(parts[:-1]).strip()


def _numeric_ref_value(ref: dict[str, Any]) -> float | None:
    value = ref.get("value")
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    match = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _numeric_ref_values(ref: dict[str, Any]) -> list[float]:
    value = ref.get("value")
    if isinstance(value, bool) or value is None:
        return []
    if isinstance(value, (int, float)):
        return [float(value)]
    text = re.sub(r"(?<=\d)\s*[-–—]\s*(?=\d)", " ", str(value))
    values: list[float] = []
    for match in re.finditer(r"-?\d+(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", text):
        try:
            values.append(float(match.group(0).replace(",", "")))
        except ValueError:
            continue
    return values


def _format_price_range(target: dict[str, float | str]) -> str:
    currency = str(target.get("currency") or "currency")
    return f"{_format_price_number(float(target['min']))}-{_format_price_number(float(target['max']))} {currency}"


def _format_price_number(value: float) -> str:
    return f"{int(value):,}" if value.is_integer() else f"{value:,.1f}"


def _price_position_statement(midpoint: float, stats: dict[str, float]) -> str:
    median = stats.get("median")
    avg = stats.get("avg")
    min_price = stats.get("min")
    max_price = stats.get("max")
    corridor = ""
    if min_price is not None and max_price is not None:
        corridor = f"位于参考价格样本区间 {_format_price_number(min_price)}-{_format_price_number(max_price)} 内"
    if median is not None and avg is not None:
        if midpoint <= median:
            return f"{corridor}，接近或低于样本中位数 {_format_price_number(median)}，更像稳健切入价"
        if midpoint <= avg:
            return f"{corridor}，高于中位数 {_format_price_number(median)} 但低于均值 {_format_price_number(avg)}，更像核心带中段偏稳健定位"
        return f"{corridor}，高于样本均值 {_format_price_number(avg)}，需要更强配置、质保或品牌理由支撑"
    return corridor or "需要补齐竞品价格统计后判断区间位置"


def _target_range_position_statement(target: dict[str, float | str], stats: dict[str, float]) -> str:
    try:
        target_min = float(target["min"])
        target_max = float(target["max"])
        midpoint = float(target["midpoint"])
    except (KeyError, TypeError, ValueError):
        return "需要补齐目标价区间后判断价格位置"

    min_price = stats.get("min")
    max_price = stats.get("max")
    if min_price is None or max_price is None:
        return _price_position_statement(midpoint, stats)

    min_price = float(min_price)
    max_price = float(max_price)
    corridor = f"参考价格样本区间 {_format_price_number(min_price)}-{_format_price_number(max_price)}"
    midpoint_context = _target_midpoint_context(midpoint, stats)

    if target_min >= min_price and target_max <= max_price:
        suffix = f"，{midpoint_context}" if midpoint_context else ""
        return f"整体位于{corridor}内{suffix}"

    if target_min < min_price and target_max > max_price:
        return (
            f"目标价区间 {_format_price_number(target_min)}-{_format_price_number(target_max)} "
            f"覆盖并超出{corridor}，下沿偏进攻、上沿偏溢价；必须拆成低配锚点、主销价和高配成交/campaign 场景分别验证"
        )

    if target_min < min_price <= target_max <= max_price:
        inside_note = (
            f"上沿 {_format_price_number(target_max)} 进入{corridor}"
            if target_max > min_price
            else f"上沿 {_format_price_number(target_max)} 贴近{corridor}下沿"
        )
        midpoint_note = (
            f"，中点 {_format_price_number(midpoint)} 仍低于样本下沿 {_format_price_number(min_price)}"
            if midpoint < min_price
            else f"，{midpoint_context}" if midpoint_context else ""
        )
        return (
            f"下沿 {_format_price_number(target_min)} 低于{corridor}的下沿 {_format_price_number(min_price)}，"
            f"{inside_note}{midpoint_note}，更像低位切入价或入门锚点，需要确认不会损害残值和版本锚点"
        )

    if min_price <= target_min <= max_price < target_max:
        if midpoint > max_price:
            return (
                f"下沿 {_format_price_number(target_min)} 仍贴近{corridor}的上沿，"
                f"但中点 {_format_price_number(midpoint)} 和上沿 {_format_price_number(target_max)} "
                f"已高于参考样本上沿 {_format_price_number(max_price)}，需要更强配置、质保、公司车或 leasing 价值支撑"
            )
        return (
            f"下沿和中点仍位于{corridor}内，但上沿 {_format_price_number(target_max)} "
            f"已高于参考样本上沿 {_format_price_number(max_price)}，需要把它当作上沿成交或 campaign/RV 场景验证"
        )

    if target_min > max_price:
        return (
            f"整体高于{corridor}的上沿 {_format_price_number(max_price)}，"
            "必须先证明配置价值、公司车 TCO 或品牌溢价，否则不宜写成已被竞品走廊支撑"
        )

    if target_max < min_price:
        return (
            f"整体低于{corridor}的下沿 {_format_price_number(min_price)}，"
            "更像强进攻价或清库存价，需要确认不会损害残值和版本锚点"
        )

    return _price_position_statement(midpoint, stats)


def _target_range_business_implication(
    target: dict[str, float | str],
    stats: dict[str, float],
) -> str:
    try:
        target_min = float(target["min"])
        target_max = float(target["max"])
    except (KeyError, TypeError, ValueError):
        return "先补齐目标价区间和竞品价格样本，再判断价格锚点、主销版本和成交支持。"

    sample_min = stats.get("min")
    sample_max = stats.get("max")
    if sample_max is not None and target_max > float(sample_max):
        return (
            "目标价上沿超过参考样本上沿，必须用尺寸/级别、动力、配置、质保、公司车或 leasing 价值解释溢价；"
            "如果这些证据补不回来，应收窄或下调目标价，或用 campaign/RV 支撑成交。"
        )
    if sample_min is not None and target_min < float(sample_min):
        return (
            "目标价下沿低于参考样本下沿，可以作为入门锚点或进攻场景，但必须验证配置删减、版本锚点、残值和毛利风险，"
            "不能把低价本身当作竞争力结论。"
        )
    if sample_min is not None and sample_max is not None:
        return (
            "目标价落在参考样本区间内，可以继续验证主销版本；下一步要用逐车型 MSRP、配置价值、月供/RV、"
            "company car 和 campaign 条件证明它位于正确竞争带，而不是仅凭样本统计定案。"
        )
    return "当前只能保留目标价场景；补齐竞品价格走廊、配置价值、月供/RV 和 company car 条件后再确定主销价格。"


def _target_range_reasonableness_label(
    target_min: float,
    target_max: float,
    min_price: float,
    max_price: float,
) -> str:
    if target_max < min_price:
        return "需要作为低位进攻价复核"
    if target_min > max_price:
        return "需要作为上沿/溢价场景复核"
    if target_min < min_price and target_max <= max_price:
        return "需要作为低位切入价复核"
    if target_min >= min_price and target_max > max_price:
        return "需要作为上沿/溢价场景复核"
    if target_min < min_price and target_max > max_price:
        return "需要拆成低配锚点和高配溢价两套场景复核"
    return "需要作为价格边界场景复核"


def _target_midpoint_context(midpoint: float, stats: dict[str, float]) -> str:
    median = stats.get("median")
    avg = stats.get("avg")
    if median is None or avg is None:
        return ""
    median = float(median)
    avg = float(avg)
    if midpoint <= median:
        return f"中点 {_format_price_number(midpoint)} 接近或低于样本中位数 {_format_price_number(median)}，更像稳健切入价"
    if midpoint <= avg:
        return (
            f"中点 {_format_price_number(midpoint)} 高于中位数 {_format_price_number(median)} "
            f"但低于均值 {_format_price_number(avg)}，更像核心带中段偏稳健定位"
        )
    return f"中点 {_format_price_number(midpoint)} 高于样本均值 {_format_price_number(avg)}，需要更强配置、质保或品牌理由支撑"


def _has_missing_evidence(evidence_package: dict[str, Any], token: str) -> bool:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    return any(
        isinstance(item, dict)
        and token in str(item.get("name") or "").lower()
        for item in missing
    )


def _strip_terminal_punctuation(value: str) -> str:
    return str(value or "").strip().rstrip("。.!！?？；; ")


def _clean_action_text(value: str) -> str:
    text = _strip_terminal_punctuation(value)
    action_map = {
        "next: quantify affected segments and generate an opportunity view": "量化受影响细分市场，并生成市场机会视图",
        "next: confirm official source, publish date, and affected vehicle eligibility": "确认官方来源、发布日期和受影响车型资格",
    }
    cleaned = action_map.get(text.lower(), text)
    return re.sub(r"^(?:下一步|next)\s*[:：]\s*", "", cleaned, flags=re.IGNORECASE).strip()


def _clean_business_text(value: str) -> str:
    text = str(value or "").strip()
    text = _replace_internal_gap_codes(text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"下一步执行\s+(?=[^：:])", "下一步执行：", text)
    text = re.sub(r"([。！？!?])(?:[。！？!?\.])+", r"\1", text)
    return text.replace("。，", "，").replace("。；", "；")


def _clean_visible_direct_text(value: str, *, strip_artifact_names: bool = False) -> str:
    text = _dedupe_repeated_display_backbone(_clean_business_text(_strip_visible_governance_labels(value)))
    if strip_artifact_names:
        text = _localize_visible_artifact_names(text)
    return text


def _dedupe_repeated_display_backbone(value: str) -> str:
    # Display guidance belongs to displayPlan / visualArtifacts, not the
    # executive answer. Keep the main answer focused on conclusion, evidence,
    # missing data and next action.
    text = re.sub(r"展示骨架：[^。]+。", "", str(value or "")).strip()
    return re.sub(r"[ \t]{2,}", " ", text)


def _localize_visible_artifact_names(value: str) -> str:
    """Keep internal artifact labels out of the user-facing direct answer."""
    text = str(value or "")
    replacements = (
        ("Pricing reference sample chart", "参考价格样本图"),
        ("Pricing corridor chart", "价格走廊图"),
        ("Pricing evidence table", "价格证据表"),
        ("MSRP source validation table", "MSRP 来源验证表"),
        ("Competitor sales chart", "竞品销量图"),
        ("Competitor share chart", "竞品份额图"),
        ("Competitor price chart", "竞品价格图"),
        ("Competitor comparison table", "竞品对比表"),
        ("Market structure chart", "市场结构图"),
        ("Powertrain mix chart", "动力结构图"),
        ("Market decision table", "市场决策表"),
        ("Policy / news evidence table", "政策/新闻来源表"),
        ("Configuration validation matrix", "配置验证矩阵"),
        ("BOM / entity mapping validation table", "BOM/实体映射验证表"),
        ("Inventory / BOM evidence table", "库存/BOM 证据表"),
        ("VOC evidence table", "VOC 证据表"),
        ("Report evidence appendix", "报告证据附录"),
        ("PPT-ready block", "汇报块"),
        ("report block", "汇报块"),
    )
    for raw, replacement in replacements:
        text = text.replace(raw, replacement)
    return re.sub(r"[ \t]{2,}", " ", text).strip()


def _clean_visible_business_synthesis_plan(plan: BusinessSynthesisPlan) -> BusinessSynthesisPlan:
    public_plan = dict(plan)
    country_label = _country_label(str(plan.get("country") or "当前市场"))
    public_plan["executiveConclusion"] = _clean_visible_direct_text(str(plan.get("executiveConclusion") or ""))
    public_plan["businessImplications"] = _dedupe(
        [
            _localize_public_market_text(_clean_visible_direct_text(item), country_label)
            for item in _string_list(plan.get("businessImplications"))
        ]
    )[:6]
    public_plan["reportReadyBullets"] = _dedupe(
        [
            _localize_public_market_text(_clean_visible_direct_text(item), country_label)
            for item in _string_list(plan.get("reportReadyBullets"))
        ]
    )[:5]
    public_plan["recommendedActions"] = _public_recommended_actions(plan)
    return public_plan  # type: ignore[return-value]


def _strip_visible_governance_labels(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"\s*分析对象[:：][^\n\r]*", "", text)
    text = re.sub(r"证据状态[:：][^。！？!?]*(?:[。！？!?]|$)", "", text)
    text = re.sub(r"当前证据状态为[^，。！？!?；;]*(?:，|[。！？!?；;]|$)", "", text)
    text = re.sub(r"证据状态为[^，。！？!?；;]*(?:，|[。！？!?；;]|$)", "", text)
    text = re.sub(r"（\s*\d+\s*条可引用证据\s*）", "", text)
    text = re.sub(r"（\s*当前缺少可引用证据\s*）", "", text)
    text = text.replace("证据边界：", "")
    text = re.sub(r"([。！？!?；;])\s*([。！？!?；;])+", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _replace_internal_gap_codes(value: str) -> str:
    text = str(value or "")
    coverage_code_labels = {
        "coverage_diagnostic:no_current_prices_for_requested_models": "当前价格覆盖缺口",
        "coverage_diagnostic:no_config_projects_for_country": "配置/版本差异覆盖缺口",
        "coverage_diagnostic:country_scope_mismatch": "目标市场数据",
    }
    for code, label in coverage_code_labels.items():
        text = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(code)}(?![A-Za-z0-9_])", label, text, flags=re.IGNORECASE)
        spaced = code.replace("_", " ")
        text = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(spaced)}(?![A-Za-z0-9_])", label, text, flags=re.IGNORECASE)
    for code in sorted(_GAP_LABELS, key=len, reverse=True):
        if code == "trim":
            continue
        label = _GAP_LABELS[code]
        text = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(code)}(?![A-Za-z0-9_])", label, text)
        if "_" in code and code not in {"current_msrp", "current_official_msrp_cross_check"}:
            spaced = code.replace("_", " ")
            text = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(spaced)}(?![A-Za-z0-9_])", label, text, flags=re.IGNORECASE)

    def weak_gap_replacement(match: re.Match[str]) -> str:
        tool_name = match.group(1).replace(" ", "_")
        return f"{_tool_business_label(tool_name)}证据不足"

    text = re.sub(
        r"\b([a-z][a-z0-9_ ]{2,}?)\s+weak evidence refs\b",
        weak_gap_replacement,
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\b([a-z][a-z0-9_]+)_weak_evidence_refs\b",
        lambda match: f"{_tool_business_label(match.group(1))}证据不足",
        text,
        flags=re.IGNORECASE,
    )
    return text


def _action_rationale(intent: str, action: str) -> str:
    if intent == "pricing_analysis":
        return "定价问题需要把 MSRP、竞品走廊、配置价值和使用场景放在同一张决策表里。"
    if intent == "market_overview":
        return "市场问题只有转成 segment 机会和产品动作后，才对产品经理有决策价值。"
    if intent == "news_policy_search":
        return "政策影响必须先验证官方来源和时间，再转成车型/价格/渠道动作。"
    if intent == "competitor_compare":
        return "竞品问题需要先确定对标关系，再输出可赢点、短板和销售话术。"
    if intent == "configuration_analysis":
        return "配置问题必须连接用户场景和竞品价值差，否则无法支持主销配置选择。"
    if intent == "inventory_analysis":
        return "库存/BOM 问题需要先固定实体关系和生命周期，避免后续数据口径混乱。"
    if intent == "report_generation":
        return "报告生成要把结论压缩成可复制的一页结构，并明确证据、产品含义和下一步动作。"
    if intent == "voc_analysis":
        return "VOC 问题需要可追溯来源和主题聚类，才能把用户声音转成配置、价格或销售动作。"
    return f"{action} 是把证据转成可执行业务输出的下一步。"


def _missing_mitigation(name: str) -> str:
    lower = name.lower()
    if "external_research_claims_unavailable" in lower:
        return "补 Tavily/web/VOC 可引用来源，保留标题、URL、发布日期和可支撑的原文要点。"
    if "published_date" in lower:
        return "补来源发布日期或官方公告时间，再判断政策/VOC/新闻信号是否仍可用于当前决策。"
    if "minimum_external_sources" in lower:
        return "补媒体测评、论坛评论、用户原声或经销端反馈来源，再按主题聚类。"
    if "external_source_repair_candidates" in lower:
        return "按检索线索读取真实网页或 VOC 来源，保留标题、URL、发布日期和可支撑原文要点。"
    if any(token in lower for token in ("price", "msrp", "corridor")):
        return "调用价格/竞品价格工具，补齐官方 MSRP、竞品走廊和月供假设。"
    if any(token in lower for token in ("policy", "source", "date", "official")):
        return "补官方来源、发布日期和政策原文，再做 JATO 交叉验证。"
    if any(token in lower for token in ("configuration", "trim", "feature", "battery")):
        return "补工程配置或竞品配置表，生成配置差异矩阵。"
    if any(token in lower for token in ("inventory", "stock", "bom", "version", "material")):
        return "补底表字段和生命周期口径，先画实体关系再落实现。"
    return "补齐对应可引用证据后再写确定数字或确定竞品结论。"


def _insight_card_ids(cards: list[dict[str, Any]]) -> list[str]:
    result: list[str] = []
    for index, card in enumerate(cards):
        value = str(card.get("id") or card.get("citationId") or card.get("title") or f"insight_{index + 1}").strip()
        if value:
            result.append(value[:80])
    return _dedupe(result)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]


def _dedupe(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _dedupe_business_bullets(items: list[str]) -> list[str]:
    result: list[str] = []
    seen_text: set[str] = set()
    seen_action_keys: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen_text:
            continue
        action_key = _business_bullet_action_key(text)
        if action_key and any(
            _action_key_covers(existing, action_key) or _action_key_covers(action_key, existing)
            for existing in seen_action_keys
        ):
            continue
        if action_key:
            seen_action_keys.append(action_key)
        display_text = _normalize_business_bullet_action_prefix(text) if action_key else text
        if display_text in seen_text:
            continue
        seen_text.add(display_text)
        result.append(display_text)
    return result


def _normalize_business_bullet_action_prefix(value: str) -> str:
    text = _normalize_space(str(value or ""))
    return re.sub(r"^(?:建议动作|下一步执行)\s*[：:]\s*", "下一步动作：", text)


def _business_bullet_action_key(value: str) -> str:
    text = _normalize_space(str(value or ""))
    if not re.match(r"^(?:建议动作|下一步动作|下一步执行|建议查数动作)\s*[：:]", text):
        return ""
    action = re.sub(
        r"^(?:建议动作|下一步动作|下一步执行|建议查数动作)\s*[：:]\s*",
        "",
        text,
    )
    cleaned = _clean_action_text(action)
    return _action_dedupe_key(cleaned)


def _dedupe_actions(items: list[RecommendedAction]) -> list[RecommendedAction]:
    result: list[RecommendedAction] = []
    seen: list[str] = []
    for item in items:
        cleaned_action = _clean_action_text(item["action"])
        if not cleaned_action:
            continue
        cleaned_item: RecommendedAction = {**item, "action": cleaned_action}
        key = _action_dedupe_key(cleaned_action)
        if not key:
            continue
        covered_index = next(
            (
                index
                for index, existing_key in enumerate(seen)
                if _action_key_covers(existing_key, key)
            ),
            -1,
        )
        if covered_index >= 0:
            continue
        narrower_index = next(
            (
                index
                for index, existing_key in enumerate(seen)
                if _action_key_covers(key, existing_key)
            ),
            -1,
        )
        if narrower_index >= 0:
            seen.pop(narrower_index)
            result.pop(narrower_index)
        seen.append(key)
        result.append(cleaned_item)
    return result


def _action_dedupe_key(value: str) -> str:
    text = _clean_action_text(str(value or "")).lower()
    text = re.sub(r"[\s,，。；;:：/、|+＋()（）\[\]【】\-]+", "", text)
    return text


def _action_key_covers(existing_key: str, candidate_key: str) -> bool:
    if not existing_key or not candidate_key:
        return False
    if existing_key == candidate_key:
        return True
    short, long = (candidate_key, existing_key) if len(candidate_key) <= len(existing_key) else (existing_key, candidate_key)
    if len(short) < 6:
        return False
    return short in long and len(long) - len(short) >= 2


def _dedupe_risks(items: list[BusinessRisk]) -> list[BusinessRisk]:
    result: list[BusinessRisk] = []
    seen: set[str] = set()
    for item in items:
        key = item["name"]
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result
