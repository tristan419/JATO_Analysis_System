from __future__ import annotations

import re
from typing import Any, Literal

from app.services.jato_business_composer_service import apply_business_composer
from app.services.jato_business_composer_service import normalize_runtime_evidence_package_status
from app.services.jato_business_playbook_service import enhance_answer_with_business_playbook
from app.services.jato_evidence_package_service import evidence_ref_count


AnswerStatus = Literal[
    "answered",
    "partially_answered",
    "insufficient_evidence",
    "tool_failed",
    "needs_user_data",
]

_NUMERIC_CLAIM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])(?:\d+(?:[.,]\d+)?\s?(?:%|k|K|m|M|SEK|EUR|USD|€|kr|units?|辆|台|万|百万|亿元|月供|km|kWh)?)"
)
_CONTROL_PROTOCOL_PATTERN = re.compile(r"^\s*(TOOL|ARGS|REASON):", re.IGNORECASE | re.MULTILINE)

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
    "current_msrp": "当前 MSRP",
    "current_official_msrp_cross_check": "官方 MSRP 交叉验证",
    "own_model_price": "本车型价格",
    "price_corridor": "价格走廊",
    "configuration_delta": "配置差异",
    "feature_diff": "配置/功能差异",
    "competitor_pool": "竞品池",
    "supporting_evidence": "支撑证据",
    "market_snapshot_data_unavailable": "内部市场快照、HEV 销量/份额和车型结构证据",
    "model_level_market_opportunity_evidence": "车型级竞品、价格和配置机会证据",
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


def apply_answer_grounding_guard(
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    *,
    country: str = "",
    question: str = "",
    evidence_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_package = normalize_runtime_evidence_package_status(evidence_package)
    guarded = dict(answer)
    refs = evidence_ref_count(evidence_package)
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    confidence = str(evidence_package.get("confidence") or "low")
    direct = str(guarded.get("direct") or "").strip()
    bullets = _string_list(guarded.get("bullets"))
    limitations = _string_list(guarded.get("limitations"))
    control_protocol = _CONTROL_PROTOCOL_PATTERN.search(direct) is not None
    has_numeric_claim = _contains_numeric_claim(direct) or any(_contains_numeric_claim(item) for item in bullets)
    blocking_missing = any(isinstance(item, dict) and item.get("impact") == "blocking" for item in missing)
    weakening_missing = any(isinstance(item, dict) and item.get("impact") == "weakens_answer" for item in missing)
    tool_failed = any(isinstance(item, dict) and str(item.get("name") or "").endswith("_failed") for item in missing)
    research_governance = evidence_package.get("researchGovernance") if isinstance(evidence_package.get("researchGovernance"), dict) else {}
    jato_cross_check = evidence_package.get("jatoCrossCheck") if isinstance(evidence_package.get("jatoCrossCheck"), dict) else {}
    policy_status = str(research_governance.get("policyStatus") or "")
    cross_check_status = str(jato_cross_check.get("status") or "")

    status: AnswerStatus = "answered"
    warnings: list[str] = []

    if tool_failed and refs == 0:
        status = "tool_failed"
        warnings.append("tool_failed_without_evidence")
    elif refs == 0 and has_numeric_claim:
        status = "insufficient_evidence"
        warnings.append("numeric_claim_without_evidence_ref")
    elif refs == 0:
        status = "insufficient_evidence"
        warnings.append("no_evidence_refs")
    elif blocking_missing:
        status = "partially_answered"
        warnings.append("blocking_evidence_missing")
    elif weakening_missing:
        status = "partially_answered"
        warnings.append("supporting_evidence_missing")

    if policy_status == "blocking":
        status = "partially_answered" if refs > 0 else "insufficient_evidence"
        warnings.append("research_policy_blocking")
    elif policy_status == "warning" and status == "answered":
        status = "partially_answered"
        warnings.append("research_policy_warning")

    if cross_check_status == "conflicting":
        status = "partially_answered" if refs > 0 else "insufficient_evidence"
        warnings.append("jato_cross_check_conflicting")

    if control_protocol:
        status = "insufficient_evidence"
        warnings.append("internal_control_protocol_detected")

    if status in {"insufficient_evidence", "tool_failed"}:
        guarded["direct"] = _insufficient_direct(evidence_package)
        guarded["bullets"] = _fallback_bullets(evidence_package)
    elif status == "partially_answered" and _has_missing_required_tool(missing):
        guarded["bullets"] = _dedupe([*bullets, *_fallback_bullets(evidence_package)])
    else:
        guarded["bullets"] = bullets

    guarded["limitations"] = _guarded_limitations(limitations, missing, warnings)
    guarded["confidence"] = confidence
    guarded["answerStatus"] = status
    guarded["evidencePackage"] = evidence_package
    guarded["grounding"] = {
        "evidenceId": evidence_package.get("evidenceId", ""),
        "confidence": confidence,
        "evidenceRefCount": refs,
        "missingEvidenceCount": len(missing),
        "warnings": warnings,
        "researchPolicyStatus": policy_status,
        "jatoCrossCheckStatus": cross_check_status,
    }
    enhanced = enhance_answer_with_business_playbook(
        guarded,
        evidence_package,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )
    return apply_business_composer(
        enhanced,
        evidence_package,
        country=country,
        question=question,
        evidence_plan=evidence_plan,
    )


def has_internal_control_protocol(value: str) -> bool:
    return _CONTROL_PROTOCOL_PATTERN.search(str(value or "")) is not None


def _contains_numeric_claim(value: str) -> bool:
    return _NUMERIC_CLAIM_PATTERN.search(str(value or "")) is not None


def _insufficient_direct(evidence_package: dict[str, Any]) -> str:
    intent = str(evidence_package.get("intent") or "general_qa")
    country = str(evidence_package.get("country") or "current market")
    intent_label = _intent_label(intent)
    market_label = _market_possessive(_country_label(country))
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    missing_names = [
        _missing_evidence_label(str(item.get("name") or ""))
        for item in missing
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]
    missing_note = ", ".join(missing_names[:3]) if missing_names else "可引用证据"
    data_note = _recommended_data_note(intent, missing_names)
    return (
        f"当前不能对{market_label}{intent_label}给出确定数字，因为缺少 {missing_note}。"
        "但这轮仍可推进：先给出非数字判断框架、竞品/配置/政策验证路径和风险边界；"
        f"下一步优先补齐 {data_note}，再把价格、销量、份额、月供或政策日期写成确定结论。"
    )


def _fallback_bullets(evidence_package: dict[str, Any]) -> list[str]:
    missing = evidence_package.get("missingEvidence")
    intent = str(evidence_package.get("intent") or "general_qa")
    tool_results = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    executed_tools = [
        _tool_business_label(str(item.get("toolName") or ""))
        for item in tool_results
        if isinstance(item, dict) and str(item.get("toolName") or "").strip()
    ]
    missing_names = [
        _missing_evidence_label(str(item.get("name") or ""))
        for item in missing
        if isinstance(missing, list) and isinstance(item, dict) and str(item.get("name") or "").strip()
    ] if isinstance(missing, list) else []
    result = [
        f"当前能判断：问题已被识别为{_intent_label(intent)}，已执行工具为 {', '.join(executed_tools) if executed_tools else '未成功执行工具'}。",
    ]
    if isinstance(missing, list) and missing:
        result.append(f"缺少证据：{', '.join(missing_names[:4])}。")
    else:
        result.append("缺少证据：当前证据包没有可引用证据。")
    result.extend([
        "影响范围：价格、销量、份额、政策日期、税费、月供和竞品结论不能写成确定事实。",
        f"建议查数动作：补齐 {_recommended_data_note(intent, missing_names)}，再生成可引用的数字结论。",
        f"建议输出形态：{_recommended_artifact_note(intent)}。",
        "在补数前可以先输出非数字框架：竞品池、价格走廊逻辑、配置价值、用户场景和风险假设。",
    ])
    return result


def _recommended_data_note(intent: str, missing_names: list[str]) -> str:
    cleaned_missing = [str(item or "").strip() for item in missing_names if str(item or "").strip()]
    if cleaned_missing:
        return "、".join(cleaned_missing[:4])
    mapping = {
        "pricing_analysis": "本车型 MSRP、竞品价格走廊、月供/RV、配置价值",
        "competitor_compare": "竞品池、价格差异、配置差异、销量/场景锚点",
        "market_overview": "市场总量、动力结构、segment 拆分、Top models",
        "configuration_analysis": "版本配置、关键特征、竞品配置差异、价格价值",
        "inventory_analysis": "市场、版本、物料号、可售数量、生命周期",
        "voc_analysis": "来源 URL、用户原声、主题频次、影响场景",
        "news_policy_search": "政策原文、发布日期、适用对象、价格/税费门槛",
        "report_generation": "关键数字、来源引用、业务含义、下一步动作",
    }
    return mapping.get(intent, "关键证据、来源引用和业务影响")


def _recommended_artifact_note(intent: str) -> str:
    mapping = {
        "pricing_analysis": "Pricing evidence table / pricing corridor chart / PPT-ready pricing block",
        "competitor_compare": "Competitor comparison table / competitor chart / positioning report block",
        "market_overview": "Market decision table / market structure chart / key metric cards",
        "configuration_analysis": "Configuration validation matrix / feature-value table",
        "inventory_analysis": "Inventory / BOM evidence table / entity mapping table",
        "voc_analysis": "VOC evidence table / theme summary block",
        "news_policy_search": "Policy / news evidence table / source timeline",
        "report_generation": "PPT-ready block / evidence appendix",
    }
    return mapping.get(intent, "evidence table / report block")


def _intent_label(intent: str) -> str:
    mapping = {
        "pricing_analysis": "定价分析",
        "competitor_compare": "竞品对比",
        "market_overview": "市场概览",
        "configuration_analysis": "配置分析",
        "inventory_analysis": "经销存/BOM 分析",
        "voc_analysis": "用户声音分析",
        "news_policy_search": "新闻/政策分析",
        "report_generation": "汇报生成",
        "coding_debug": "开发调试",
        "general_qa": "通用问题",
    }
    return mapping.get(str(intent or "").strip(), str(intent or "").replace("_", " ") or "分析")


def _country_label(country: str) -> str:
    mapping = {
        "Sweden": "瑞典",
        "Finland": "芬兰",
        "Norway": "挪威",
        "Denmark": "丹麦",
        "Germany": "德国",
        "Hungary": "匈牙利",
        "Austria": "奥地利",
        "Italy": "意大利",
        "Poland": "波兰",
        "France": "法国",
        "Netherlands": "荷兰",
        "current market": "当前市场",
    }
    return mapping.get(str(country or "").strip(), str(country or "").strip() or "当前市场")


def _market_possessive(country_label: str) -> str:
    value = str(country_label or "").strip() or "当前市场"
    if any("\u4e00" <= char <= "\u9fff" for char in value):
        return f"{value}的" if value.endswith("市场") else f"{value}市场的"
    return f"{value} market "


def _recommended_tools_for_intent(intent: str) -> list[str]:
    mapping = {
        "pricing_analysis": ["query_msrp_pricing", "compare_competitive_set", "query_price_positioning", "search_market_news"],
        "competitor_compare": ["compare_competitive_set", "compare_vehicle_variants", "query_msrp_pricing"],
        "market_overview": ["query_country_snapshot", "build_market_chart", "analyze_market_dynamics"],
        "configuration_analysis": ["compare_vehicle_variants", "query_msrp_pricing"],
        "inventory_analysis": ["query_country_snapshot", "query_with_filters"],
        "voc_analysis": ["search_market_news", "minirag_query_graph"],
        "news_policy_search": ["search_market_news", "pageindex_search_documents", "read_web_page"],
        "report_generation": ["build_market_chart", "query_country_snapshot", "search_market_news"],
    }
    return mapping.get(intent, ["query_country_snapshot", "search_market_news"])


def _missing_evidence_label(name: str) -> str:
    value = str(name or "").strip()
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
    return value.replace("_", " ") or "证据缺口"


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


def _has_missing_required_tool(missing: list[Any]) -> bool:
    return any(
        isinstance(item, dict)
        and str(item.get("name") or "").startswith("missing_required_tool:")
        for item in missing
    )


def _guarded_limitations(
    limitations: list[str],
    missing: list[Any],
    warnings: list[str],
) -> list[str]:
    result = list(limitations)
    if warnings:
        result.append(f"证据安全检查：{', '.join(_warning_label(item) for item in warnings)}。")
    if "jato_cross_check_conflicting" in warnings:
        result.append("外部来源和内部 JATO 数据尚未对齐，数字或市场方向结论需要人工复核。")
    if "research_policy_blocking" in warnings:
        result.append("外部研究证据未满足治理要求，需要补官方来源、来源数量、发布日期或 JATO 交叉验证。")
    for item in missing[:4]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        impact = str(item.get("impact") or "").strip()
        if name:
            impact_note = f"（{_impact_label(impact)}）" if impact else ""
            result.append(f"证据缺口：{_missing_evidence_label(name)}{impact_note}。")
    return _dedupe(result)


def _impact_label(value: str) -> str:
    mapping = {
        "weakens_answer": "会削弱结论",
        "blocking": "阻断确定结论",
        "optional": "可选补强",
    }
    key = str(value or "").strip()
    return mapping.get(key, key.replace("_", " ") or "会影响结论")


def _warning_label(value: str) -> str:
    mapping = {
        "jato_cross_check_conflicting": "JATO 与外部证据冲突",
        "research_policy_blocking": "外部研究证据未达标",
        "missing_evidence": "证据缺口",
        "numeric_claim_without_ref": "数字结论缺少引用",
    }
    key = str(value or "").strip()
    return mapping.get(key, key.replace("_", " ") or "证据提醒")


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            result.append(text)
    return result


def _dedupe(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
