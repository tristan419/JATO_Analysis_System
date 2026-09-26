from __future__ import annotations

import re
from typing import Any, TypedDict

from app.services.jato_evidence_package_service import evidence_ref_count
from app.services.jato_evidence_package_service import evidence_tool_names
from app.services.jato_tool_coverage_guard_service import tool_satisfies_required


class BusinessPlaybook(TypedDict):
    id: str
    intent: str
    categories: list[str]
    title: str
    decisionFrame: list[str]
    requiredSections: list[str]
    evidenceChecklist: list[str]
    recommendedTools: list[str]
    nextActions: list[str]
    followUpIntents: list[str]


FAILURE_TAGS = [
    "intent_wrong",
    "tool_missing",
    "evidence_missing",
    "answer_too_conservative",
    "answer_too_generic",
    "chart_not_useful",
    "table_not_readable",
    "pm_insight_weak",
    "followup_low_value",
    "presentation_not_ready",
    "hallucination_risk",
]

_HANDLED_EVIDENCE_GAP_EXEMPT_BLOCKING_NAMES = {
    "minimum_external_sources",
    "external_research_claims_unavailable",
}

_HANDLED_EVIDENCE_GAP_REQUIRED_NAMES = {
    "leasing_tco_or_company_car_evidence",
    "minimum_external_sources",
    "external_research_claims_unavailable",
}

_PLAYBOOKS: dict[str, BusinessPlaybook] = {
    "pricing_analysis": {
        "id": "pricing_analysis",
        "intent": "pricing_analysis",
        "categories": ["pricing"],
        "title": "Pricing corridor playbook",
        "decisionFrame": [
            "先判断目标车型应落在哪个竞品价格走廊，而不是直接给孤立价格。",
            "把 MSRP、竞品池、配置差异、月供/公司车税和促销支持拆开说明。",
            "证据不足时仍要给出定价判断框架、缺口和下一步查数动作。",
        ],
        "requiredSections": ["直接定价立场", "竞品价格走廊", "配置/价值差异", "缺失证据", "下一步动作"],
        "evidenceChecklist": ["own_model_price", "competitor_price_range", "configuration_delta", "leasing_or_tax_context"],
        "recommendedTools": ["query_msrp_pricing", "compare_competitive_set", "compare_vehicle_variants", "search_market_news"],
        "nextActions": ["补齐本车型与竞品 MSRP / TP / 月供价格矩阵", "生成价格矩阵", "拆分私人零售和 company car 场景", "输出一页定价建议"],
        "followUpIntents": ["compare", "data_check", "action", "report"],
    },
    "market_overview": {
        "id": "market_overview",
        "intent": "market_overview",
        "categories": ["market_overview"],
        "title": "Market opportunity playbook",
        "decisionFrame": [
            "不要只复述市场数据，要指出机会 segment、动力结构变化和对目标产品组合的动作含义。",
            "把市场规模、份额、动力类型、车型级结构和邻国对比放在同一条分析线里。",
            "数据充分时给机会判断；数据不足时给拆解路径和最小证据清单。",
        ],
        "requiredSections": ["市场结构判断", "机会 segment", "动力/车型变化", "业务动作", "风险和补数"],
        "evidenceChecklist": ["market_kpis", "trend_or_mix", "segment_split", "top_models"],
        "recommendedTools": ["query_country_snapshot", "query_segment_breakdown", "build_market_chart", "analyze_market_dynamics"],
        "nextActions": ["拆到车型/品牌", "做邻国 side-by-side", "生成市场机会页", "按动力类型找进入点"],
        "followUpIntents": ["drilldown", "compare", "why", "action"],
    },
    "competitor_compare": {
        "id": "competitor_compare",
        "intent": "competitor_compare",
        "categories": ["competitor_compare"],
        "title": "Competitor positioning playbook",
        "decisionFrame": [
            "先定义竞品池，再比较价格、尺寸/级别、动力、配置和用户场景。",
            "结论要落到定位：应该正面对标、错位竞争，还是只作为价格锚点。",
            "如果没有竞品证据，不要硬判胜负，改为输出竞品验证表。",
        ],
        "requiredSections": ["竞品池定义", "定位差异", "价格/配置差异", "可赢点和短板", "下一步验证"],
        "evidenceChecklist": ["competitor_pool", "configuration_delta", "price_delta", "segment_context"],
        "recommendedTools": ["compare_competitive_set", "compare_vehicle_variants", "query_msrp_pricing", "query_country_snapshot"],
        "nextActions": ["生成竞品对比表", "补价格/配置证据", "拆用户场景", "形成定位话术"],
        "followUpIntents": ["compare", "why", "action", "report"],
    },
    "configuration_analysis": {
        "id": "configuration_analysis",
        "intent": "configuration_analysis",
        "categories": ["configuration"],
        "title": "Configuration value playbook",
        "decisionFrame": [
            "配置分析必须连接用户场景，不能只列装备。",
            "把电池、续航、充电、冬季包、拖车/载重、ADAS 和价格差一起判断。",
            "证据不足时给出配置验证清单和主销配置假设。",
        ],
        "requiredSections": ["配置问题本质", "用户场景", "竞品配置差异", "价格/价值影响", "主销配置建议"],
        "evidenceChecklist": ["trim", "powertrain", "key_features", "competitor_feature_delta"],
        "recommendedTools": ["compare_vehicle_variants", "query_msrp_pricing", "compare_competitive_set", "search_market_news"],
        "nextActions": ["补工程配置表", "做竞品配置矩阵", "验证冬季/拖车/V2H 场景", "输出主销配置建议"],
        "followUpIntents": ["compare", "why", "action", "data_check"],
    },
    "news_policy_search": {
        "id": "policy_analysis",
        "intent": "news_policy_search",
        "categories": ["policy", "policy_news"],
        "title": "Policy impact playbook",
        "decisionFrame": [
            "政策问题先确认政策文本和时间，再拆影响对象、价格门槛、用户类型和业务动作。",
            "新闻/政策没有来源日期时不能给确定影响，只能给影响路径和查证计划。",
            "要把政策影响转成车型、价格、动力和渠道建议。",
        ],
        "requiredSections": ["政策事实边界", "影响车型/用户", "商业影响路径", "不确定性", "下一步查证"],
        "evidenceChecklist": ["source_date", "policy_effect", "affected_models", "threshold_or_tax_rule"],
        "recommendedTools": ["search_market_news", "pageindex_search_documents", "read_web_page", "query_msrp_pricing"],
        "nextActions": ["查政策原文", "和车型价格上限交叉验证", "拆 BEV/PHEV/company car 影响", "输出政策影响页"],
        "followUpIntents": ["external_search", "data_check", "action", "report"],
    },
    "inventory_analysis": {
        "id": "inventory_bom",
        "intent": "inventory_analysis",
        "categories": ["inventory_bom"],
        "title": "Inventory and BOM logic playbook",
        "decisionFrame": [
            "BOM/库存问题先建模实体关系，再讨论业务流程。",
            "把车型版本、物料号、颜色、市场、PI、订单和客户可编辑数量分层说明。",
            "如果没有底表证据，仍要给数据模型和流程建议，不要只说查不到。",
        ],
        "requiredSections": ["实体关系", "业务流程", "数据表/字段建议", "异常和生命周期", "下一步落地"],
        "evidenceChecklist": ["available_units", "market", "version", "material_number", "lifecycle_status"],
        "recommendedTools": ["query_country_snapshot", "query_with_filters", "compare_vehicle_variants"],
        "nextActions": ["画实体关系", "定义物料号生命周期", "补选品表字段", "验证 SE/FI 分市场生成逻辑"],
        "followUpIntents": ["drilldown", "data_check", "action", "report"],
    },
    "voc_analysis": {
        "id": "voc_analysis",
        "intent": "voc_analysis",
        "categories": ["voc"],
        "title": "VOC evidence playbook",
        "decisionFrame": [
            "VOC 问题要区分真实用户痛点、媒体观点、论坛噪音和销售可转化卖点。",
            "把用户声音映射到配置、价格、售后、冬季场景和报告话术。",
            "没有可追溯来源时不能声称高频，只能给检索路径和验证假设。",
        ],
        "requiredSections": ["用户声音来源", "高频痛点/卖点", "产品含义", "风险边界", "下一步验证"],
        "evidenceChecklist": ["source_url", "user_quote_or_snippet", "theme_frequency", "affected_scenario"],
        "recommendedTools": ["external_research", "search_market_news", "read_web_page"],
        "nextActions": ["补论坛/媒体/VOC 来源", "按主题聚类用户声音", "映射到配置和销售话术", "输出 VOC 一页报告"],
        "followUpIntents": ["why", "external_search", "action", "report"],
    },
    "report_generation": {
        "id": "report_generation",
        "intent": "report_generation",
        "categories": ["report_generation"],
        "title": "PPT-ready report playbook",
        "decisionFrame": [
            "报告生成不是重写长文，而是把结论、证据、产品含义和下一步动作收敛成一页。",
            "每页必须有明确 key message，证据必须可追溯，结尾必须能转成业务动作。",
            "缺数据时要把缺口写成验证项，而不是隐藏不确定性。",
        ],
        "requiredSections": ["Title", "Key message", "Evidence", "Product implication", "Next action"],
        "evidenceChecklist": ["key_metric", "source_ref", "business_implication", "next_action"],
        "recommendedTools": ["query_country_snapshot", "query_msrp_pricing", "compare_competitive_set", "build_market_chart"],
        "nextActions": ["生成一页 PPT block", "补齐证据引用", "压缩成汇报语言", "列出后续分析页"],
        "followUpIntents": ["report", "action", "data_check", "compare"],
    },
}

_ALIASES = {
    "pricing": "pricing_analysis",
    "policy": "news_policy_search",
    "policy_news": "news_policy_search",
    "policy_analysis": "news_policy_search",
    "inventory_bom": "inventory_analysis",
    "bom": "inventory_analysis",
    "configuration": "configuration_analysis",
    "compare": "competitor_compare",
    "voc": "voc_analysis",
    "report": "report_generation",
}

_GAP_LABELS = {
    "competitor_price_range": "竞品价格走廊",
    "current_msrp": "官方 MSRP 交叉验证",
    "current_official_msrp_cross_check": "官方 MSRP 交叉验证",
    "own_model_price": "本车型价格",
    "price_corridor": "价格走廊",
    "configuration_delta": "配置差异",
    "feature_diff": "配置/功能差异",
    "competitor_pool": "竞品池",
    "supporting_evidence": "支撑证据",
    "market_snapshot_data_unavailable": "内部市场快照、HEV 销量/份额和车型结构证据",
    "jato_cross_check": "JATO 内部交叉验证",
    "published_date": "来源发布日期",
    "official_source": "官方来源",
    "consumer_signal": "用户原声/VOC 信号",
    "external_research_claims_unavailable": "外部来源结论不足",
    "monthly_trend_series": "月度趋势序列",
    "competitive_or_configuration_data_unavailable": "竞品/配置证据不足",
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


def list_business_playbooks() -> dict[str, BusinessPlaybook]:
    return {key: _copy_playbook(value) for key, value in _PLAYBOOKS.items()}


def get_business_playbook(intent_or_category: str | None) -> BusinessPlaybook | None:
    key = str(intent_or_category or "").strip()
    if not key:
        return None
    normalized = _ALIASES.get(key, key)
    playbook = _PLAYBOOKS.get(normalized)
    if playbook:
        return _copy_playbook(playbook)
    for candidate in _PLAYBOOKS.values():
        if key in candidate["categories"] or key == candidate["id"]:
            return _copy_playbook(candidate)
    return None


def build_business_playbook_context(
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any] | None = None,
    evidence_package: dict[str, Any] | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    evidence_plan = evidence_plan or {}
    evidence_package = evidence_package or {}
    category_key = str(category or "").strip()
    intent = (
        category_key
        or str(evidence_plan.get("intent") or "").strip()
        or str(evidence_package.get("intent") or "").strip()
    )
    playbook = get_business_playbook(category_key) or get_business_playbook(intent)
    if not playbook:
        return {}
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    return {
        "id": playbook["id"],
        "intent": playbook["intent"],
        "title": playbook["title"],
        "country": country,
        "question": question[:500],
        "requiredSections": playbook["requiredSections"],
        "decisionFrame": playbook["decisionFrame"],
        "evidenceChecklist": playbook["evidenceChecklist"],
        "recommendedTools": playbook["recommendedTools"],
        "nextActions": playbook["nextActions"],
        "followUpIntents": playbook["followUpIntents"],
        "insufficientEvidencePolicy": [
            "不要只停在“证据不足”。",
            "必须说明当前仍能做什么业务判断。",
            "必须说明缺什么证据、缺口影响哪个结论。",
            "必须给出下一步工具/数据动作和可点击追问。",
        ],
        "currentEvidence": {
            "confidence": evidence_package.get("confidence", "unknown"),
            "evidenceRefCount": evidence_ref_count(evidence_package),
            "toolsUsed": evidence_tool_names(evidence_package),
            "missingEvidence": missing[:6],
        },
    }


def enhance_answer_with_business_playbook(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    *,
    country: str = "",
    question: str = "",
    evidence_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    playbook_context = build_business_playbook_context(
        country=country or str(evidence_package.get("country") or ""),
        question=question,
        evidence_plan=evidence_plan,
        evidence_package=evidence_package,
    )
    if not playbook_context:
        return answer

    enhanced = dict(answer)
    enhanced["businessPlaybook"] = playbook_context
    status = str(enhanced.get("answerStatus") or "")
    refs = evidence_ref_count(evidence_package)
    direct = str(enhanced.get("direct") or "").strip()

    if status in {"insufficient_evidence", "tool_failed", "needs_user_data"} or refs == 0:
        enhanced["direct"] = _insufficient_direct(playbook_context)
        enhanced["bullets"] = _insufficient_bullets(playbook_context, evidence_package)
    elif _looks_like_governance_answer(direct):
        existing = _string_list(enhanced.get("bullets"))
        enhanced["bullets"] = _dedupe([
            *existing,
            f"业务打法：{playbook_context['decisionFrame'][0]}",
            f"建议输出结构：{' / '.join(playbook_context['requiredSections'][:4])}。",
            f"下一步动作：{playbook_context['nextActions'][0]}。",
        ])[:8]

    return enhanced


def infer_business_failure_tags(record: dict[str, Any]) -> list[str]:
    astrbot = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
    comparison = record.get("comparison") if isinstance(record.get("comparison"), dict) else {}
    scoring = record.get("humanScoring") if isinstance(record.get("humanScoring"), dict) else {}
    tags: list[str] = []
    missing_evidence = astrbot.get("missingEvidence") if isinstance(astrbot.get("missingEvidence"), list) else []
    missing_names = [
        str(item.get("name") or "").strip()
        for item in missing_evidence
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    blocking_missing_names = [
        str(item.get("name") or "").strip()
        for item in missing_evidence
        if (
            isinstance(item, dict)
            and str(item.get("impact") or "") == "blocking"
            and str(item.get("name") or "").strip()
        )
    ]

    answer_status = str(astrbot.get("answerStatus") or "")
    handled_evidence_gap = _has_handled_evidence_gap(
        astrbot,
        missing_names=missing_names,
        blocking_missing_names=blocking_missing_names,
    )
    if answer_status in {"insufficient_evidence", "tool_failed", "needs_user_data"}:
        tags.extend(["answer_too_conservative", "evidence_missing"])
    if answer_status == "insufficient_evidence" and int(astrbot.get("evidenceRefCount") or 0) > 0:
        tags.append("answer_too_conservative")
    if any(name.startswith("missing_required_tool:") for name in missing_names):
        tags.append("tool_missing")
    if blocking_missing_names and not handled_evidence_gap:
        tags.append("evidence_missing")

    answer_chars = int(comparison.get("astrbotAnswerChars") or 0)
    length_delta = int(comparison.get("answerLengthDelta") or 0)
    if (
        (answer_chars < 320 or length_delta < -600)
        and _business_output_score(astrbot) < 0.75
        and not _has_business_output(astrbot)
    ):
        tags.append("answer_too_generic")

    expected_tools = _string_list(record.get("expectedTools"))
    tools_used = _astrbot_tools(astrbot)
    quality_failures = _quality_failures(astrbot)
    if any(item.startswith("missing_required_tools") for item in quality_failures):
        tags.append("tool_missing")
    elif expected_tools and not any(
        tool_satisfies_required(expected_tool, used_tool)
        for expected_tool in expected_tools
        for used_tool in tools_used
    ):
        tags.append("tool_missing")

    expected_intent = str(record.get("expectedIntent") or "")
    if expected_intent:
        if any(item.startswith("intent_mismatch") for item in quality_failures):
            tags.append("intent_wrong")

    preview = str(astrbot.get("answerPreview") or "")
    country = str(record.get("country") or "")
    if (
        country
        and not _mentions_country_or_region_context(preview, country)
        and not _has_country_scoped_business_evidence(astrbot, country)
    ):
        tags.append("answer_too_generic")
    if _looks_like_governance_answer(preview) and not _has_business_output(astrbot):
        tags.append("answer_too_generic")
    evidence_refs = int(astrbot.get("evidenceRefCount") or 0)
    if evidence_refs == 0 or ("missing_blocking_evidence" in quality_failures and not handled_evidence_gap):
        tags.append("evidence_missing")
    if "grounding_incomplete" in quality_failures and not handled_evidence_gap:
        tags.append("evidence_missing")
    question_text = str(record.get("question") or "").lower()
    if int(astrbot.get("chartCount") or 0) == 0 and _explicitly_expects_chart(question_text):
        tags.append("chart_not_useful")
    if _expects_report_artifact(record) and not _has_visual_artifact_type(astrbot, "report_block"):
        tags.append("presentation_not_ready")
    if evidence_refs > 0 and not astrbot.get("visualArtifacts") and _expects_table_artifact(record):
        tags.append("table_not_readable")

    if scoring.get("scoreTotals", {}).get("complete") is True:
        astr_scores = scoring.get("astrbotScores") if isinstance(scoring.get("astrbotScores"), dict) else {}
        if int(astr_scores.get("pmInsight") or 0) <= 3:
            tags.append("pm_insight_weak")
        if int(astr_scores.get("actionability") or 0) <= 3:
            tags.append("answer_too_conservative")
        if int(astr_scores.get("followUpValue") or 0) <= 3:
            tags.append("followup_low_value")
        if int(astr_scores.get("artifactQuality") or 0) <= 3:
            tags.append("chart_not_useful")
        if int(astr_scores.get("presentationReadiness") or 0) <= 3:
            tags.append("presentation_not_ready")
        if int(astr_scores.get("grounding") or 0) <= 2:
            tags.append("hallucination_risk")

    return [tag for tag in _dedupe(tags) if tag in FAILURE_TAGS]


def _has_handled_evidence_gap(
    astrbot: dict[str, Any],
    *,
    missing_names: list[str],
    blocking_missing_names: list[str],
) -> bool:
    if not missing_names and not blocking_missing_names:
        return False
    if any(name not in _HANDLED_EVIDENCE_GAP_EXEMPT_BLOCKING_NAMES for name in blocking_missing_names):
        return False
    if not any(name in _HANDLED_EVIDENCE_GAP_REQUIRED_NAMES for name in missing_names):
        return False
    if str(astrbot.get("answerStatus") or "") not in {"answered", "partially_answered"}:
        return False
    if int(astrbot.get("evidenceRefCount") or 0) <= 0:
        return False
    if _business_output_score(astrbot) < 0.7 and not _has_business_output(astrbot):
        return False
    if not (astrbot.get("visualArtifacts") or astrbot.get("recommendedActions") or astrbot.get("reportReadyBullets")):
        return False
    preview = str(astrbot.get("answerPreview") or "")
    if not any(token in preview for token in ("缺", "待补", "不能", "边界", "下一步")):
        return False
    if not any(token in preview for token in ("直接结论", "判断", "支持", "保留", "验证线", "动作")):
        return False
    return True


def _copy_playbook(playbook: BusinessPlaybook) -> BusinessPlaybook:
    return {
        "id": playbook["id"],
        "intent": playbook["intent"],
        "categories": list(playbook["categories"]),
        "title": playbook["title"],
        "decisionFrame": list(playbook["decisionFrame"]),
        "requiredSections": list(playbook["requiredSections"]),
        "evidenceChecklist": list(playbook["evidenceChecklist"]),
        "recommendedTools": list(playbook["recommendedTools"]),
        "nextActions": list(playbook["nextActions"]),
        "followUpIntents": list(playbook["followUpIntents"]),
    }


def _insufficient_direct(playbook_context: dict[str, Any]) -> str:
    country = str(playbook_context.get("country") or "当前市场")
    title = str(playbook_context.get("title") or "business analysis playbook")
    return (
        f"当前证据还不足以给 {country} 的确定数字结论，但不能停在“查不到”。"
        f"这轮应按 {title} 先给业务判断框架：明确可判断部分、证据缺口、缺口影响和下一步查数动作。"
    )


def _insufficient_bullets(playbook_context: dict[str, Any], evidence_package: dict[str, Any]) -> list[str]:
    current = playbook_context.get("currentEvidence") if isinstance(playbook_context.get("currentEvidence"), dict) else {}
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    missing_names = [
        _missing_evidence_label(str(item.get("name") or ""))
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    return [
        f"当前能判断：问题应按 {playbook_context.get('title')} 处理，输出结构应覆盖 {' / '.join(playbook_context.get('requiredSections', [])[:4])}。",
        f"已有证据：{len(current.get('toolsUsed') or [])} 个工具、{current.get('evidenceRefCount', 0)} 个 evidenceRef，置信度 {current.get('confidence', 'unknown')}。",
        f"缺少证据：{', '.join(missing_names[:4]) if missing_names else ', '.join(playbook_context.get('evidenceChecklist', [])[:4])}。",
        "影响范围：没有这些证据时，价格、销量、份额、政策日期、税费、月供和竞品胜负不能写成确定事实。",
        f"下一步动作：{'; '.join(playbook_context.get('nextActions', [])[:3])}。",
        f"建议查数动作：{_data_action_text(playbook_context, missing_names)}。",
        f"建议输出形态：{_artifact_output_text(playbook_context)}。",
    ]


def _missing_evidence_label(name: str) -> str:
    value = str(name or "").strip()
    if not value:
        return "证据缺口"
    if value.startswith("missing_required_tool:"):
        tool_name = value.replace("missing_required_tool:", "", 1).strip()
        return f"{_tool_business_label(tool_name)}工具结果"
    if value.startswith("coverage_diagnostic:no_current_prices"):
        return "当前价格覆盖缺口"
    if value in _GAP_LABELS:
        return _GAP_LABELS[value]
    if value.endswith("_weak_evidence_refs"):
        tool_name = value.replace("_weak_evidence_refs", "")
        return f"{_tool_business_label(tool_name)}证据不足"
    return value.replace("_", " ")


def _data_action_text(playbook_context: dict[str, Any], missing_names: list[str]) -> str:
    if missing_names:
        return f"优先补齐{', '.join(missing_names[:4])}，再重算结论"
    checklist = [
        str(item or "").strip()
        for item in playbook_context.get("evidenceChecklist", [])
        if str(item or "").strip()
    ]
    if checklist:
        return f"优先补齐{', '.join(checklist[:4])}，再重算结论"
    return "先补齐关键证据，再重算结论"


def _artifact_output_text(playbook_context: dict[str, Any]) -> str:
    sections = [
        str(item or "").strip()
        for item in playbook_context.get("requiredSections", [])
        if str(item or "").strip()
    ]
    if sections:
        return " / ".join(sections[:4])
    return "结论 / 证据 / 缺口 / 下一步动作"


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


def _looks_like_governance_answer(text: str) -> bool:
    value = str(text or "")
    if len(value) < 180:
        return True
    markers = [
        "当前应优先依据",
        "本次使用",
        "证据来自",
        "当前工具没有查到足够",
        "不能给出确定数字",
    ]
    return sum(1 for marker in markers if marker in value) >= 2


def _astrbot_tools(astrbot: dict[str, Any]) -> list[str]:
    tools = []
    selected = str(astrbot.get("selectedTool") or "").strip()
    if selected:
        tools.append(selected)
    evidence_package = astrbot.get("evidencePackage") if isinstance(astrbot.get("evidencePackage"), dict) else {}
    tools.extend(evidence_tool_names(evidence_package))
    return _dedupe(tools)


def _business_output_score(astrbot: dict[str, Any]) -> float:
    quality = astrbot.get("qualityScore") if isinstance(astrbot.get("qualityScore"), dict) else {}
    for key in ("businessSynthesisScore", "businessCompletenessScore", "totalScore"):
        value = quality.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


def _has_business_output(astrbot: dict[str, Any]) -> bool:
    if astrbot.get("recommendedActions") or astrbot.get("businessImplications") or astrbot.get("reportReadyBullets"):
        return True
    synthesis = astrbot.get("businessSynthesisPlan") if isinstance(astrbot.get("businessSynthesisPlan"), dict) else {}
    return bool(
        synthesis.get("recommendedActions")
        or synthesis.get("businessImplications")
        or synthesis.get("reportReadyBullets")
    )


def _has_country_scoped_business_evidence(astrbot: dict[str, Any], country: str) -> bool:
    evidence_package = astrbot.get("evidencePackage") if isinstance(astrbot.get("evidencePackage"), dict) else {}
    package_country = str(evidence_package.get("country") or "")
    if not package_country or not _mentions_country_or_region_context(package_country, country):
        return False
    refs = int(astrbot.get("evidenceRefCount") or 0) or evidence_ref_count(evidence_package)
    if refs <= 0:
        return False
    has_output = _has_business_output(astrbot)
    artifacts = astrbot.get("visualArtifacts")
    has_artifacts = isinstance(artifacts, list) and any(isinstance(item, dict) for item in artifacts)
    digest_text = " ".join(_string_list(astrbot.get("evidenceDigest")))
    display_text = str(astrbot.get("displayPlan") or "")
    combined = f"{digest_text} {display_text}"
    has_business_signal = any(
        token in combined
        for token in (
            "细分销量",
            "注册量",
            "销量",
            "市场",
            "配置验证项",
            "竞品",
            "价格",
            "MSRP",
            "BEV",
            "HEV",
            "PHEV",
            "BOM",
            "物料",
            "VOC",
            "政策",
            "TCO",
            "company-car",
        )
    )
    return bool((has_output or has_artifacts) and has_business_signal)


def _expects_table_artifact(record: dict[str, Any]) -> bool:
    category = str(record.get("category") or "")
    expected_intent = str(record.get("expectedIntent") or "")
    return category in {"pricing", "competitor_compare", "configuration", "inventory_bom", "report_generation"} or expected_intent in {
        "pricing_analysis",
        "competitor_compare",
        "configuration_analysis",
        "inventory_analysis",
        "report_generation",
    }


def _expects_report_artifact(record: dict[str, Any]) -> bool:
    category = str(record.get("category") or "")
    expected_intent = str(record.get("expectedIntent") or "")
    question = str(record.get("question") or "").lower()
    return (
        category == "report_generation"
        or expected_intent == "report_generation"
        or any(token in question for token in ("report", "ppt", "slide", "汇报", "报告", "一页", "大纲"))
    )


def _explicitly_expects_chart(question_text: str) -> bool:
    return any(token in question_text for token in ("chart", "plot", "graph", "trend", "图表", "趋势", "折线", "可视化"))


def _has_visual_artifact_type(astrbot: dict[str, Any], artifact_type: str) -> bool:
    artifacts = astrbot.get("visualArtifacts")
    if not isinstance(artifacts, list):
        return False
    return any(isinstance(item, dict) and item.get("type") == artifact_type for item in artifacts)


def _quality_failures(astrbot: dict[str, Any]) -> list[str]:
    quality = astrbot.get("qualityScore") if isinstance(astrbot.get("qualityScore"), dict) else {}
    return _string_list(quality.get("failures"))


def _country_zh(country: str) -> str:
    mapping = {
        "sweden": "瑞典",
        "finland": "芬兰",
        "norway": "挪威",
        "denmark": "丹麦",
        "germany": "德国",
    }
    return mapping.get(country.strip().lower(), "")


def _mentions_country_or_region_context(text: str, country: str) -> bool:
    value = str(text or "")
    country_value = str(country or "").strip()
    if not country_value:
        return True
    if country_value.lower() in value.lower():
        return True
    zh_label = _country_zh(country_value)
    if zh_label and zh_label in value:
        return True
    if any(_contains_country_alias(value, alias) for alias in _country_context_aliases(country_value)):
        return True
    return any(region in value for region in _regional_context_labels(country_value))


def _contains_country_alias(text: str, alias: str) -> bool:
    normalized_text = str(text or "").casefold()
    normalized_alias = str(alias or "").strip().casefold()
    if not normalized_alias:
        return False
    if len(normalized_alias) <= 3:
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(normalized_alias)}(?![a-z0-9])", normalized_text))
    return normalized_alias in normalized_text


def _country_context_aliases(country: str) -> list[str]:
    aliases = {
        "sweden": ["SE", "SWE", "Sverige", "Swedish", "SE/FI"],
        "finland": ["FI", "FIN", "Suomi", "Finnish", "SE/FI"],
        "norway": ["NO", "NOR", "Norge", "Norwegian"],
        "denmark": ["DK", "DNK", "Danmark", "Danish"],
        "germany": ["DE", "DEU", "Deutschland", "German"],
        "hungary": ["HU", "HUN", "Magyarország", "Hungarian"],
    }
    return aliases.get(str(country or "").strip().casefold(), [])


def _regional_context_labels(country: str) -> list[str]:
    if country.strip().lower() in {"sweden", "finland", "norway", "denmark"}:
        return ["北欧", "Nordic", "Nordics", "Scandinavia", "Scandinavian"]
    return []


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
