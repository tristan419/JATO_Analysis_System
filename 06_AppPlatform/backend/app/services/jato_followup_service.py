from __future__ import annotations

import os
import re
import uuid
from typing import Any

from app.services.jato_agent_planning_service import build_evidence_plan


FOLLOW_UP_INTENTS = {
    "drilldown",
    "compare",
    "why",
    "action",
    "data_check",
    "external_search",
    "report",
}


def structured_followups_enabled() -> bool:
    raw = os.getenv("APP_ASTRBOT_FOLLOWUPS_V2_ENABLED", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def normalize_follow_ups(
    value: Any,
    *,
    country: str,
    question: str,
    tools: list[str] | None = None,
    evidence_plan: dict[str, Any] | None = None,
    evidence_package: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    plan = evidence_plan or build_evidence_plan(country, question)
    priority_follow_ups = _country_scope_mismatch_follow_ups(
        country=country,
        question=question,
        evidence_plan=plan,
        evidence_package=evidence_package or {},
    )
    normalized: list[dict[str, Any]] = []
    if isinstance(value, list):
        for index, item in enumerate(value):
            follow_up = _normalize_one_follow_up(
                item,
                index=index,
                country=country,
                question=question,
                tools=tools or [],
                evidence_plan=plan,
            )
            if follow_up:
                normalized.append(follow_up)
    if not normalized:
        normalized = generate_follow_ups(
            country=country,
            question=question,
            tools=tools or [],
            evidence_plan=plan,
        )
    if priority_follow_ups:
        safe_follow_ups = _country_scope_mismatch_safe_follow_ups(
            country=country,
            question=question,
            tools=tools or [],
            evidence_plan=plan,
            evidence_package=evidence_package or {},
        )
        return _renumber_follow_ups(_dedupe_follow_ups([*priority_follow_ups, *safe_follow_ups]))[:4]
    normalized = _dedupe_follow_ups(normalized)
    normalized = _ensure_requested_follow_up_types(
        normalized,
        country=country,
        question=question,
        tools=tools or [],
        evidence_plan=plan,
    )
    return normalized[:4]


def _country_scope_mismatch_follow_ups(
    *,
    country: str,
    question: str,
    evidence_plan: dict[str, Any],
    evidence_package: dict[str, Any],
) -> list[dict[str, Any]]:
    mismatch = _country_scope_mismatch(evidence_package)
    if not mismatch:
        return []
    chinese = _contains_chinese(question)
    requested = _country_display_name(mismatch.get("requestedCountry") or country or "当前市场", chinese=chinese)
    returned = _country_display_name(mismatch.get("returnedCountry") or "", chinese=chinese)
    expected_tools = _missing_required_tools(evidence_package) or _planned_tool_names(evidence_plan)
    intent = str(evidence_plan.get("intent") or "market_overview")
    if chinese:
        label = f"重查{requested}数据"
        returned_note = f"，不要使用{returned}结果" if returned else ""
        follow_question = (
            f"按{requested}重新调用{_tool_list_label(expected_tools, chinese=True)}工具{returned_note}，"
            f"过滤非目标市场证据，并重新回答：{question}"
        )
        reason = f"上一轮工具返回了{returned or '非目标市场'}证据，不能支撑{requested}结论；下一步必须重查目标市场数据。"
    else:
        label = f"Re-query {requested} data"
        returned_note = f" and exclude {returned} results" if returned else ""
        follow_question = (
            f"Re-run {_tool_list_label(expected_tools, chinese=False)} for {requested}{returned_note}, "
            f"filter out off-scope market evidence, and answer again: {question}"
        )
        reason = f"The previous tool result was scoped to {returned or 'another market'}, so it cannot support a {requested} conclusion."
    return [{
        "id": _follow_up_id("data_check", follow_question),
        "label": label,
        "question": follow_question,
        "intent": "data_check",
        "reason": reason,
        "expectedTools": expected_tools[:4],
        "expectedOutput": _expected_output("data_check" if intent else "data_check"),
        "priority": 1,
        "risk": "country_scope_mismatch",
    }]


def _country_scope_mismatch_safe_follow_ups(
    *,
    country: str,
    question: str,
    tools: list[str],
    evidence_plan: dict[str, Any],
    evidence_package: dict[str, Any],
) -> list[dict[str, Any]]:
    mismatch = _country_scope_mismatch(evidence_package)
    if not mismatch:
        return []
    chinese = _contains_chinese(question)
    requested = _country_display_name(mismatch.get("requestedCountry") or country or "当前市场", chinese=chinese)
    returned = _country_display_name(mismatch.get("returnedCountry") or "", chinese=chinese)
    planned_tools = tools or _planned_tool_names(evidence_plan)
    target_model = _target_model_label(evidence_plan, question, chinese=chinese)
    model_clause = f"{target_model} " if target_model else ""
    model_object = target_model or ("目标车型" if chinese else "the target model")
    if chinese:
        candidates = [
            {
                "label": f"生成{requested}市场图",
                "question": f"按{requested}重新生成市场规模、动力结构、SUV 结构和 Top model 图表，不使用{returned or '非目标市场'}结果。",
                "intent": "drilldown",
                "reason": "错国证据不能支撑市场结论，下一步应先重建目标市场图表。",
                "expectedTools": ["query_country_snapshot", "build_market_chart"],
                "expectedOutput": "chart",
                "risk": "country_scope_mismatch",
            },
            {
                "label": f"补{requested}竞品证据",
                "question": f"按{requested}重新查询 {model_clause}核心竞品池、官方 MSRP、主销价、月供和配置差异，再判断{model_object}是否适合进入。",
                "intent": "compare",
                "reason": "车型进入判断必须用目标市场竞品和价格证据，不能沿用其他市场材料。",
                "expectedTools": ["compare_competitive_set", "query_msrp_pricing", "compare_vehicle_variants"],
                "expectedOutput": "table",
                "risk": "country_scope_mismatch",
            },
            {
                "label": f"生成{requested}补证清单",
                "question": f"列出回答这个问题还需要补齐的{requested}证据：市场规模、动力结构、SUV 结构、{model_clause}竞品价格、配置差异和来源日期。",
                "intent": "report",
                "reason": "在目标市场证据回来前，先输出补证清单比继续写结论更可靠。",
                "expectedTools": planned_tools,
                "expectedOutput": "checklist",
                "risk": "country_scope_mismatch",
            },
        ]
    else:
        candidates = [
            {
                "label": f"Build {requested} market charts",
                "question": f"Rebuild market-size, powertrain-mix, SUV-structure, and top-model charts for {requested}, excluding {returned or 'off-scope market'} results.",
                "intent": "drilldown",
                "reason": "Off-scope evidence cannot support the market conclusion; rebuild target-market charts first.",
                "expectedTools": ["query_country_snapshot", "build_market_chart"],
                "expectedOutput": "chart",
                "risk": "country_scope_mismatch",
            },
            {
                "label": f"Fetch {requested} competitor evidence",
                "question": f"Re-query the {requested} {model_clause}competitor pool, official MSRP, main-trim price, monthly payment, and configuration deltas before judging whether {model_object} fits the market.",
                "intent": "compare",
                "reason": "Entry-fit conclusions need target-market competitor and pricing evidence.",
                "expectedTools": ["compare_competitive_set", "query_msrp_pricing", "compare_vehicle_variants"],
                "expectedOutput": "table",
                "risk": "country_scope_mismatch",
            },
            {
                "label": f"List {requested} evidence gaps",
                "question": f"List the remaining {requested} evidence gaps: market size, powertrain mix, SUV structure, {model_clause}competitor pricing, configuration deltas, and source dates.",
                "intent": "report",
                "reason": "Before target-market evidence returns, an evidence checklist is more reliable than a conclusion.",
                "expectedTools": planned_tools,
                "expectedOutput": "checklist",
                "risk": "country_scope_mismatch",
            },
        ]
    return _structured_follow_ups_from_candidates(candidates)


def _country_scope_mismatch(evidence_package: dict[str, Any]) -> dict[str, str]:
    tools = evidence_package.get("toolResults") if isinstance(evidence_package.get("toolResults"), list) else []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        diagnostics = tool.get("coverageDiagnostics")
        if not isinstance(diagnostics, dict):
            continue
        if str(diagnostics.get("diagnosis") or "").strip() != "country_scope_mismatch":
            continue
        requested = str(diagnostics.get("requestedCountry") or "").strip()
        returned = str(diagnostics.get("returnedCountry") or "").strip()
        return {"requestedCountry": requested, "returnedCountry": returned}
    return {}


def _missing_required_tools(evidence_package: dict[str, Any]) -> list[str]:
    missing = evidence_package.get("missingEvidence") if isinstance(evidence_package.get("missingEvidence"), list) else []
    tools: list[str] = []
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name.startswith("missing_required_tool:"):
            continue
        tool_name = name.replace("missing_required_tool:", "", 1).strip()
        if tool_name:
            tools.append(tool_name)
    return _dedupe_strings(tools)


def _tool_list_label(tools: list[str], *, chinese: bool) -> str:
    labels = [_tool_display_name(tool, chinese=chinese) for tool in tools[:3]]
    labels = [label for label in labels if label]
    if not labels:
        return "目标市场数据" if chinese else "target-market data"
    return "、".join(labels) if chinese else ", ".join(labels)


def _tool_display_name(tool_name: str, *, chinese: bool) -> str:
    value = str(tool_name or "").strip()
    if not chinese:
        return value or "required tool"
    mapping = {
        "query_country_snapshot": "市场快照",
        "build_market_chart": "趋势图表",
        "analyze_market_dynamics": "市场动态",
        "analyze_model_performance": "车型表现",
        "query_msrp_pricing": "MSRP/当前价格",
        "compare_competitive_set": "竞品池/价格走廊",
        "compare_vehicle_variants": "配置差异",
        "query_with_filters": "筛选查询",
        "search_market_news": "外部研究",
        "search_market_news": "新闻/政策搜索",
    }
    return mapping.get(value, value or "必需工具")


def _target_model_label(evidence_plan: dict[str, Any], question: str, *, chinese: bool) -> str:
    entities = evidence_plan.get("entities") if isinstance(evidence_plan.get("entities"), dict) else {}
    models = entities.get("models") if isinstance(entities.get("models"), list) else []
    for model in models:
        label = str(model or "").strip()
        if label:
            return label
    text = str(question or "")
    match = re.search(
        r"\b(?:OMODA\s?9|OMODA9|OMODA\s?5|OMODA5|JAECOO\s?J7|JAECOO\s?J8|J7\s?HEV|T7\s?HEV|T8\s?PHEV|J8|J7|T7|T8|O9|O5|EX30|EV3|RAV4|Sportage|Sorento|Tucson|Corolla\s?Cross)\b",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return re.sub(r"\s+", " ", match.group(0)).strip()
    return "目标车型" if chinese else "target model"


def _is_sweden_j7_hev_scope(country: str, question: str) -> bool:
    country_key = str(country or "").strip().casefold()
    question_text = f" {str(question or '').casefold()} "
    non_sweden_tokens = (
        "匈牙利",
        "hungary",
        "hungarian",
        " hu ",
        "芬兰",
        "finland",
        "finnish",
        " fi ",
        "挪威",
        "norway",
        "norwegian",
        " no ",
        "丹麦",
        "denmark",
        "danish",
        " dk ",
        "德国",
        "germany",
        "german",
        " de ",
        "不要回答瑞典",
        "not sweden",
        "do not answer sweden",
    )
    if any(token in question_text for token in non_sweden_tokens):
        return False
    return country_key in {"sweden", "sverige", "se", "swe", "瑞典"} or any(
        token in question_text for token in ("sweden", "sverige", "瑞典", " se ")
    )


def follow_up_questions(follow_ups: Any) -> list[str]:
    if not isinstance(follow_ups, list):
        return []
    questions: list[str] = []
    seen: set[str] = set()
    for item in follow_ups:
        if isinstance(item, dict):
            text = str(item.get("question") or item.get("label") or "").strip()
        else:
            text = str(item or "").strip()
        if text and text not in seen:
            seen.add(text)
            questions.append(text)
    return questions


def serialize_follow_ups(follow_ups: list[dict[str, Any]]) -> list[dict[str, Any]] | list[str]:
    if structured_followups_enabled():
        return follow_ups[:4]
    return follow_up_questions(follow_ups)[:4]


def generate_follow_ups(
    *,
    country: str,
    question: str,
    tools: list[str],
    evidence_plan: dict[str, Any],
) -> list[dict[str, Any]]:
    chinese = _contains_chinese(question)
    country_label = _country_display_name(country or "当前市场", chinese=chinese)
    intent = str(evidence_plan.get("intent") or "market_overview")
    requested_types = [
        item
        for item in (evidence_plan.get("followUpTypes") if isinstance(evidence_plan.get("followUpTypes"), list) else [])
        if str(item) in FOLLOW_UP_INTENTS
    ] or ["drilldown", "compare", "action", "report"]
    used_tools = tools or _planned_tool_names(evidence_plan)
    business_follow_ups = _business_follow_ups(
        country=country_label,
        question=question,
        analysis_intent=intent,
    )
    if business_follow_ups:
        return business_follow_ups[:4]
    result: list[dict[str, Any]] = []
    for follow_type in requested_types[:4]:
        result.append(_build_follow_up(
            follow_type=follow_type,
            country=country_label,
            question=question,
            analysis_intent=intent,
            tools=used_tools,
            priority=len(result) + 1,
        ))
    return result[:4]


def _ensure_requested_follow_up_types(
    follow_ups: list[dict[str, Any]],
    *,
    country: str,
    question: str,
    tools: list[str],
    evidence_plan: dict[str, Any],
) -> list[dict[str, Any]]:
    requested_types = [
        str(item).strip()
        for item in (evidence_plan.get("followUpTypes") if isinstance(evidence_plan.get("followUpTypes"), list) else [])
        if str(item).strip() in FOLLOW_UP_INTENTS
    ]
    if not requested_types:
        return _dedupe_follow_ups(follow_ups)[:4]
    existing_intents = {str(item.get("intent") or "").strip() for item in follow_ups}
    required_types = requested_types[:2]
    if 3 <= len(follow_ups) <= 4 and all(item in existing_intents for item in required_types):
        return _dedupe_follow_ups(follow_ups)[:4]
    analysis_intent = str(evidence_plan.get("intent") or "market_overview")
    country_label = _country_display_name(country or "当前市场", chinese=_contains_chinese(question))
    used_tools = tools or _planned_tool_names(evidence_plan)
    by_intent: dict[str, dict[str, Any]] = {}
    for item in follow_ups:
        intent = str(item.get("intent") or "").strip()
        if intent and intent not in by_intent:
            by_intent[intent] = item
    ordered: list[dict[str, Any]] = []
    for follow_type in requested_types[:4]:
        if follow_type in by_intent:
            ordered.append(dict(by_intent[follow_type]))
            continue
        ordered.append(_build_follow_up(
            follow_type=follow_type,
            country=country_label,
            question=question,
            analysis_intent=analysis_intent,
            tools=used_tools,
            priority=len(ordered) + 1,
        ))
    ordered_keys = {
        str(item.get("question") or item.get("id") or "").strip()
        for item in ordered
    }
    for item in follow_ups:
        key = str(item.get("question") or item.get("id") or "").strip()
        if key and key in ordered_keys:
            continue
        ordered.append(dict(item))
    for index, item in enumerate(ordered):
        item["priority"] = index + 1
    return _dedupe_follow_ups(ordered)[:4]


def _business_follow_ups(
    *,
    country: str,
    question: str,
    analysis_intent: str,
) -> list[dict[str, Any]]:
    text = str(question or "").casefold()
    policy_follow_ups = _o5_bev_policy_follow_ups(
        country=country,
        question=question,
        analysis_intent=analysis_intent,
        normalized_question=text,
    )
    if policy_follow_ups:
        return policy_follow_ups
    policy_follow_ups = _co2_phev_policy_follow_ups(
        country=country,
        question=question,
        analysis_intent=analysis_intent,
        normalized_question=text,
    )
    if policy_follow_ups:
        return policy_follow_ups
    specific_follow_ups = _question_specific_business_follow_ups(
        country=country,
        question=question,
        analysis_intent=analysis_intent,
        normalized_question=text,
    )
    if specific_follow_ups:
        return specific_follow_ups
    if "j7" not in text or "hev" not in text:
        return []
    if analysis_intent not in {"pricing_analysis", "report_generation", "competitor_compare", "market_overview"}:
        return []
    if not _is_sweden_j7_hev_scope(country, question):
        return []
    chinese = _contains_chinese(question)
    market = _country_display_name(country, chinese=chinese)
    if chinese:
        candidates = [
            {
                "label": "补竞品价格矩阵",
                "question": f"基于{market}市场，把 J7 HEV 与 Corolla Cross、RAV4、C-HR、Qashqai 的 MSRP、主销价和月供做成价格走廊表。",
                "intent": "compare",
                "reason": "J7 HEV 定价是否成立，下一步最需要验证核心竞品价格走廊，而不是继续泛泛讨论市场。",
                "expectedTools": ["query_msrp_pricing", "compare_competitive_set", "query_country_snapshot"],
                "expectedOutput": "table",
            },
            {
                "label": "拆市场窗口结构",
                "question": f"把{market} J7 HEV 机会拆到 HEV 规模、SUV A0/A 结构、Toyota/Kia 竞品池和价格带，判断是否真有进入窗口。",
                "intent": "drilldown",
                "reason": "定价结论必须先验证市场窗口和车型结构，否则价格矩阵容易脱离真实需求。",
                "expectedTools": ["query_country_snapshot", "build_market_chart", "compare_competitive_set"],
                "expectedOutput": "chart",
            },
            {
                "label": "验证高配价差和话术",
                "question": f"验证{market} J7 HEV 高低配 3,230€ 价差，是否能被 7年质保、540°影像、HUD、座椅通风/记忆等可感知配置覆盖，并拆成展厅话术。",
                "intent": "action",
                "reason": "产品经理分析需要把高配主推落到可感知价值、版本策略和销售动作。",
                "expectedTools": ["compare_vehicle_variants", "query_msrp_pricing", "query_country_snapshot"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "生成一页 PPT block",
                "question": f"生成{market} J7 HEV 定价一页 PPT block：Title、Key message、Evidence、Product implication、Next action。",
                "intent": "report",
                "reason": "当前分析已经具备汇报页骨架，适合直接沉淀成可复制产出物。",
                "expectedTools": ["query_country_snapshot", "query_msrp_pricing", "compare_competitive_set"],
                "expectedOutput": "report",
            },
        ]
    else:
        candidates = [
            {
                "label": "Build price matrix",
                "question": f"For {market}, build a J7 HEV price corridor table against Corolla Cross, RAV4, C-HR, and Qashqai using MSRP, main-trim price, and monthly payment where available.",
                "intent": "compare",
                "reason": "The next PM step is to validate the competitor price corridor instead of asking a generic market follow-up.",
                "expectedTools": ["query_msrp_pricing", "compare_competitive_set", "query_country_snapshot"],
                "expectedOutput": "table",
            },
            {
                "label": "Drill market window",
                "question": f"Break the {market} J7 HEV opportunity into HEV size, SUV A0/A structure, Toyota/Kia competitor pool, and price bands to verify the entry window.",
                "intent": "drilldown",
                "reason": "Pricing should first validate the market window and segment structure, not only a price comparison.",
                "expectedTools": ["query_country_snapshot", "build_market_chart", "compare_competitive_set"],
                "expectedOutput": "chart",
            },
            {
                "label": "Validate trim value",
                "question": f"Validate whether the {market} J7 HEV high-low trim price gap is covered by visible features such as warranty, 540 camera, HUD, and ventilated/memory seats, then turn it into showroom sales talk.",
                "intent": "action",
                "reason": "PM analysis should convert the high-trim push into perceptible value, version strategy, and sales action.",
                "expectedTools": ["compare_vehicle_variants", "query_msrp_pricing", "query_country_snapshot"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "Create PPT block",
                "question": f"Create a one-page {market} J7 HEV pricing PPT block with Title, Key message, Evidence, Product implication, and Next action.",
                "intent": "report",
                "reason": "The analysis is ready to become a reusable report artifact.",
                "expectedTools": ["query_country_snapshot", "query_msrp_pricing", "compare_competitive_set"],
                "expectedOutput": "report",
            },
        ]
    return [
        {
            "id": _follow_up_id(str(item["intent"]), str(item["question"])),
            "label": str(item["label"]),
            "question": str(item["question"]),
            "intent": str(item["intent"]),
            "reason": str(item["reason"]),
            "expectedTools": _string_list(item["expectedTools"])[:4],
            "expectedOutput": str(item["expectedOutput"]),
            "priority": index + 1,
        }
        for index, item in enumerate(candidates)
    ]


def _question_specific_business_follow_ups(
    *,
    country: str,
    question: str,
    analysis_intent: str,
    normalized_question: str,
) -> list[dict[str, Any]]:
    chinese = _contains_chinese(question)
    market = _country_display_name(country, chinese=chinese)
    candidates: list[dict[str, Any]] = []
    if analysis_intent == "market_overview" and "瑞典" in question and "芬兰" in question and _contains_any(normalized_question, ("差异", "why", "销量")):
        candidates = [
            {
                "label": "拆瑞典/芬兰差异驱动",
                "question": "把瑞典和芬兰销量差异拆成市场体量、动力结构、SUV 结构、公司车渠道和主销品牌五个驱动，并生成 side-by-side 表。",
                "intent": "drilldown",
                "reason": "用户问的是差异原因，下一步应把国家差异拆成可解释的数据维度。",
                "expectedTools": ["query_cross_country", "build_market_chart", "analyze_market_dynamics"],
                "expectedOutput": "table",
            },
            {
                "label": "找 OJ 进入动作",
                "question": "基于瑞典/芬兰差异，分别给出 OMODA/JAECOO 在瑞典和芬兰的产品、价格和渠道进入动作。",
                "intent": "action",
                "reason": "市场差异最终要转成不同国家的产品动作，而不是停在国家解释。",
                "expectedTools": ["query_cross_country", "query_msrp_pricing", "compare_competitive_set"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "画双边市场图",
                "question": "生成瑞典 vs 芬兰的销量、BEV/PHEV/HEV 动力结构和 SUV 结构对比图，用于一页汇报。",
                "intent": "report",
                "reason": "这类问题适合用图表把国家差异变成可复用汇报素材。",
                "expectedTools": ["query_cross_country", "build_market_chart"],
                "expectedOutput": "chart",
            },
        ]
    elif analysis_intent == "market_overview" and "suv" in normalized_question and _contains_any(normalized_question, ("a0", "a 级", "a级", "主销", "结构")):
        candidates = [
            {
                "label": "拆 SUV A0/A 数据",
                "question": f"把{market} SUV A0/A 拆到销量、BEV/PHEV/HEV 渗透率、2WD/4WD 和主销车型，判断为什么成为主销结构。",
                "intent": "drilldown",
                "reason": "SUV A0/A 主销原因必须由细分销量、动力和驱动结构支撑。",
                "expectedTools": ["query_country_snapshot", "build_market_chart", "analyze_market_dynamics"],
                "expectedOutput": "chart",
            },
            {
                "label": "对比竞品/邻国结构",
                "question": f"对比{market} SUV A0/A 与邻国或核心竞品所在细分市场的销量、动力结构和价格带，判断这个主销结构是否可复制。",
                "intent": "compare",
                "reason": "主销结构需要和邻国/竞品池对比，避免把单一市场结构误判成普遍机会。",
                "expectedTools": ["query_cross_country", "compare_competitive_set", "query_msrp_pricing"],
                "expectedOutput": "table",
            },
            {
                "label": "映射到车型机会",
                "question": f"基于{market} SUV A0/A 结构，给出 OMODA/JAECOO 在 BEV、HEV、PHEV 上应优先切入的车型和价格带。",
                "intent": "action",
                "reason": "市场结构分析的下一步是车型和价格带动作。",
                "expectedTools": ["query_country_snapshot", "query_msrp_pricing", "compare_competitive_set"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "生成机会页",
                "question": f"生成{market} SUV A0/A 市场机会一页 PPT block：主结论、关键证据、机会 segment、产品动作和风险。",
                "intent": "report",
                "reason": "该问题适合沉淀为市场机会页，方便产品评审复用。",
                "expectedTools": ["query_country_snapshot", "build_market_chart"],
                "expectedOutput": "report",
            },
        ]
    elif analysis_intent == "inventory_analysis" and _contains_any(normalized_question, ("bom", "物料", "内外饰", "颜色", "选品表", "可编辑数量", "se/fi", "pi")):
        candidates = [
            {
                "label": "画实体关系图",
                "question": "把 PI header、market overlay、business variant、material code、外观/内饰、lifecycle/orderability 和 editable quantity 画成实体关系表。",
                "intent": "drilldown",
                "reason": "BOM 问题首先要固定实体层级，否则后续数量和物料号解释都会混乱。",
                "expectedTools": ["query_inventory_pipeline", "compare_vehicle_variants"],
                "expectedOutput": "table",
            },
            {
                "label": "定义校验规则",
                "question": "定义 BOM 映射校验规则：重复物料号、历史替代、市场专属、不可下单颜色组合、phase-out 和跨市场混用。",
                "intent": "action",
                "reason": "用户需要的是可执行的数据治理规则，不是泛泛解释 BOM。",
                "expectedTools": ["query_inventory_pipeline", "query_country_snapshot"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "生成管理表",
                "question": "生成一张 BOM / 物料号管理表，列出实体层、映射字段、校验逻辑、用户可见字段和审计字段。",
                "intent": "report",
                "reason": "BOM 分析应沉淀成可落地的后台表结构和操作口径。",
                "expectedTools": ["query_inventory_pipeline"],
                "expectedOutput": "table",
            },
        ]
    elif analysis_intent == "configuration_analysis" and _contains_any(normalized_question, ("80kwh", "95kwh", "冬季包", "battery", "winter", "800v", "双电机")):
        topic = "北欧冬季包" if "冬季包" in question else ("95kWh + 双电机 + 800V" if "95" in normalized_question or "800v" in normalized_question else "80kWh 电池")
        candidates = [
            {
                "label": "做配置价值矩阵",
                "question": f"把{market}{topic}拆成用户场景、竞品配置、成本/重量风险、可见价值和主销版本建议，生成配置价值矩阵。",
                "intent": "compare",
                "reason": "配置问题需要用竞品和用户场景验证价值，而不是只列装备。",
                "expectedTools": ["compare_vehicle_variants", "query_country_snapshot", "query_msrp_pricing"],
                "expectedOutput": "table",
            },
            {
                "label": "解释北欧需求原因",
                "question": f"解释{market}为什么需要验证{topic}：冬季续航、安全冗余、公司车使用、拖挂/长途场景和竞品预期分别贡献多少。",
                "intent": "why",
                "reason": "配置需求要先解释用户场景和市场预期，避免把工程规格直接当成购买理由。",
                "expectedTools": ["query_country_snapshot", "compare_vehicle_variants", "search_market_news"],
                "expectedOutput": "summary",
            },
            {
                "label": "拆版本策略",
                "question": f"给出{market}{topic}的低配/高配/选装包版本策略，并说明哪些配置必须标配、哪些适合做价值包。",
                "intent": "action",
                "reason": "配置结论最终要落到版本和配置包动作。",
                "expectedTools": ["compare_vehicle_variants", "query_msrp_pricing"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "补竞品配置证据",
                "question": f"检查{market}{topic}还缺哪些竞品配置、续航/电池、价格和官方配置表证据。",
                "intent": "data_check",
                "reason": "当前配置判断不能脱离可引用配置矩阵和竞品证据。",
                "expectedTools": ["compare_vehicle_variants", "search_market_news", "query_msrp_pricing"],
                "expectedOutput": "summary",
            },
        ]
    elif analysis_intent == "voc_analysis":
        candidates = [
            {
                "label": "解释吐槽触发场景",
                "question": f"解释{market}用户为什么会在冬季续航、拖车/roof load、V2H、售后信任或车机体验上产生吐槽，并按购买阶段排序。",
                "intent": "why",
                "reason": "VOC 分析先要说明用户抱怨从哪个使用场景触发，避免只列关键词。",
                "expectedTools": ["search_market_news", "read_web_page"],
                "expectedOutput": "summary",
            },
            {
                "label": "验证真实用户来源",
                "question": f"搜索并读取{market}用户论坛、媒体测评和车主评论，验证这些主题是否是真实高频痛点，而不是候选假设。",
                "intent": "external_search",
                "reason": "VOC 问题必须先补可引用用户原声和媒体来源，不能只靠市场结构推断。",
                "expectedTools": ["search_market_news", "read_web_page"],
                "expectedOutput": "summary",
            },
            {
                "label": "聚类痛点到配置动作",
                "question": f"把{market} VOC 主题聚类到冬季使用、充电/续航、空间便利、售后信任和配置可见价值，并转成产品/销售动作。",
                "intent": "action",
                "reason": "用户声音的价值在于转成配置、交付、售后和话术动作。",
                "expectedTools": ["search_market_news", "compare_vehicle_variants"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "生成 VOC 验证表",
                "question": f"生成{market} VOC 验证表：主题、来源 URL、发布日期、原文要点、频次信号、产品含义和能否引用。",
                "intent": "report",
                "reason": "VOC 需要先变成可审计来源表，再写高频结论。",
                "expectedTools": ["search_market_news", "read_web_page"],
                "expectedOutput": "table",
            },
        ]
    elif analysis_intent == "competitor_compare" and _contains_any(normalized_question, ("o5", "ex30", "ev3", "o9", "xc60", "ex60", "j8", "sorento")):
        candidates = [
            {
                "label": "解释胜负前提",
                "question": f"解释{market}本车型为什么可能赢或输：把竞品池、价格带、尺寸/座位、动力形式、配置可见价值和使用场景逐项拆开。",
                "intent": "why",
                "reason": "用户问的是竞争逻辑，先把胜负前提拆清楚，再进入表格和销售动作。",
                "expectedTools": ["compare_competitive_set", "query_country_snapshot", "compare_vehicle_variants"],
                "expectedOutput": "summary",
            },
            {
                "label": "补竞品证据矩阵",
                "question": f"把{market}本车型与核心竞品的 MSRP、销量、动力、尺寸、配置和用户场景做成竞品证据矩阵。",
                "intent": "compare",
                "reason": "竞品问题下一步应补证据矩阵，而不是继续泛泛问原因。",
                "expectedTools": ["compare_competitive_set", "query_msrp_pricing", "compare_vehicle_variants"],
                "expectedOutput": "table",
            },
            {
                "label": "拆错位定位动作",
                "question": f"基于{market}竞品证据，判断本车型应正面对抗、错位竞争还是做价格锚点，并给出销售话术。",
                "intent": "action",
                "reason": "产品经理需要把竞品对比转成定位和销售动作。",
                "expectedTools": ["compare_competitive_set", "query_msrp_pricing"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "生成对标页",
                "question": f"生成{market}竞品对标一页 PPT block：主对标、价格/配置差异、可赢点、短板和补证清单。",
                "intent": "report",
                "reason": "竞品分析适合沉淀成一页可复制汇报结构。",
                "expectedTools": ["compare_competitive_set", "query_msrp_pricing", "compare_vehicle_variants"],
                "expectedOutput": "report",
            },
        ]
    if not candidates:
        return []
    return _structured_follow_ups_from_candidates(candidates)


def _structured_follow_ups_from_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": _follow_up_id(str(item["intent"]), str(item["question"])),
            "label": str(item["label"]),
            "question": str(item["question"]),
            "intent": str(item["intent"]),
            "reason": str(item["reason"]),
            "expectedTools": _string_list(item["expectedTools"])[:4],
            "expectedOutput": str(item["expectedOutput"]),
            "priority": index + 1,
        }
        for index, item in enumerate(candidates)
    ]


def _o5_bev_policy_follow_ups(
    *,
    country: str,
    question: str,
    analysis_intent: str,
    normalized_question: str,
) -> list[dict[str, Any]]:
    if analysis_intent not in {"news_policy_search", "pricing_analysis", "report_generation"}:
        return []
    if not ("o5" in normalized_question and "bev" in normalized_question and ("补贴" in question or "subsid" in normalized_question) and "价格上限" in question):
        return []
    chinese = _contains_chinese(question)
    market = _country_display_name(country, chinese=chinese)
    if chinese:
        candidates = [
            {
                "label": "核对官方政策边界",
                "question": f"查询{market} BEV 补贴价格上限的官方原文、发布日期、适用人群和是否仍有效，并说明 O5 BEV 是否适用。",
                "intent": "external_search",
                "reason": "这个问题的第一风险是把历史上限或未确认新政策当成现行规则，必须先查政策事实边界。",
                "expectedTools": ["search_market_news", "read_web_page"],
                "expectedOutput": "summary",
            },
            {
                "label": "做补贴内外定价页",
                "question": f"基于{market}政策边界，生成 O5 BEV 补贴内/补贴外两套定价页：低配锚点、高配价值、风险和销售话术。",
                "intent": "report",
                "reason": "补贴价格上限最终要转成两个可执行版本策略，而不是只讨论政策本身。",
                "expectedTools": ["query_msrp_pricing", "compare_competitive_set", "search_market_news"],
                "expectedOutput": "report",
            },
            {
                "label": "对比 EX30 / EV3 门槛",
                "question": f"把{market} O5 BEV 与 EX30、EV3 的 MSRP、主销配置、续航/电池和补贴资格风险做成对比表。",
                "intent": "compare",
                "reason": "是否压进价格门槛必须和核心竞品的价格/配置一起判断。",
                "expectedTools": ["query_msrp_pricing", "compare_vehicle_variants", "compare_competitive_set"],
                "expectedOutput": "table",
            },
            {
                "label": "拆低配/高配动作",
                "question": f"给出{market} O5 BEV 低配作为补贴资格锚点、高配作为配置价值版本的产品动作清单。",
                "intent": "action",
                "reason": "用户下一步需要的是版本策略和销售动作，而不是泛泛政策解释。",
                "expectedTools": ["query_msrp_pricing", "compare_vehicle_variants", "search_market_news"],
                "expectedOutput": "recommendation",
            },
        ]
    else:
        candidates = [
            {
                "label": "Verify policy boundary",
                "question": f"Verify whether the {market} BEV subsidy price cap is still active, its official source, publish date, eligible buyers, and whether O5 BEV qualifies.",
                "intent": "external_search",
                "reason": "The first risk is treating a historical or unverified cap as a current rule.",
                "expectedTools": ["search_market_news", "read_web_page"],
                "expectedOutput": "summary",
            },
            {
                "label": "Build in/out pricing pages",
                "question": f"Create two {market} O5 BEV pricing pages: subsidy-qualified entry trim and above-cap high-trim value story.",
                "intent": "report",
                "reason": "The policy question should become executable version strategy.",
                "expectedTools": ["query_msrp_pricing", "compare_competitive_set", "search_market_news"],
                "expectedOutput": "report",
            },
            {
                "label": "Compare EX30 / EV3 threshold",
                "question": f"Compare O5 BEV with EX30 and EV3 in {market} by MSRP, main-trim configuration, battery/range, and subsidy threshold risk.",
                "intent": "compare",
                "reason": "Price-cap fit only matters relative to competitor price and configuration.",
                "expectedTools": ["query_msrp_pricing", "compare_vehicle_variants", "compare_competitive_set"],
                "expectedOutput": "table",
            },
            {
                "label": "Split trim actions",
                "question": f"Turn the {market} O5 BEV subsidy-cap analysis into low-trim anchor and high-trim value actions.",
                "intent": "action",
                "reason": "The next useful step is a version and sales-action decision.",
                "expectedTools": ["query_msrp_pricing", "compare_vehicle_variants", "search_market_news"],
                "expectedOutput": "recommendation",
            },
        ]
    return [
        {
            "id": _follow_up_id(str(item["intent"]), str(item["question"])),
            "label": str(item["label"]),
            "question": str(item["question"]),
            "intent": str(item["intent"]),
            "reason": str(item["reason"]),
            "expectedTools": _string_list(item["expectedTools"])[:4],
            "expectedOutput": str(item["expectedOutput"]),
            "priority": index + 1,
        }
        for index, item in enumerate(candidates)
    ]


def _co2_phev_policy_follow_ups(
    *,
    country: str,
    question: str,
    analysis_intent: str,
    normalized_question: str,
) -> list[dict[str, Any]]:
    if analysis_intent not in {"news_policy_search", "pricing_analysis", "report_generation"}:
        return []
    if not ("phev" in normalized_question and ("co2" in normalized_question or "co₂" in normalized_question)):
        return []
    if not ("0-75" in normalized_question or "75g" in normalized_question or "税率阶梯" in question):
        return []
    chinese = _contains_chinese(question)
    market = _country_display_name(country, chinese=chinese)
    if chinese:
        candidates = [
            {
                "label": "核对 CO2/税率公式",
                "question": f"查询{market} CO2 0-75g/km 税率阶梯、company car benefit 公式、发布日期和 PHEV 认证 CO2 适用条件。",
                "intent": "external_search",
                "reason": "这个问题的第一风险是把低排放区间误判成自动利好，必须先确认官方公式和适用边界。",
                "expectedTools": ["search_market_news", "read_web_page"],
                "expectedOutput": "summary",
            },
            {
                "label": "做 PHEV TCO 场景表",
                "question": f"把{market} PHEV 与 HEV/BEV 在月供、残值、税费、能耗、真实充电行为和长途里程下做成 company car TCO 场景表。",
                "intent": "compare",
                "reason": "是否有利最终要由 TCO 和使用风险证明，而不是只由 CO2 区间判断。",
                "expectedTools": ["query_msrp_pricing", "compare_vehicle_variants", "search_market_news"],
                "expectedOutput": "table",
            },
            {
                "label": "拆公司车适用场景",
                "question": f"拆分哪些{market} company car 场景下 PHEV 仍有理由，哪些场景应转向 BEV 或 HEV。",
                "intent": "action",
                "reason": "产品动作需要回答 PHEV 应保留在哪些用户和渠道场景，而不是泛化成全市场主推。",
                "expectedTools": ["query_country_snapshot", "query_msrp_pricing", "search_market_news"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "生成政策影响 PPT block",
                "question": f"生成一页{market} CO2 0-75g/km 阶梯对 PHEV 的政策影响 PPT block：结论、证据、TCO 口径、风险和下一步。",
                "intent": "report",
                "reason": "该问题适合沉淀成可评审的政策影响页，方便人工确认和后续汇报复用。",
                "expectedTools": ["search_market_news", "query_msrp_pricing", "compare_vehicle_variants"],
                "expectedOutput": "report",
            },
        ]
    else:
        candidates = [
            {
                "label": "Verify CO2 tax formula",
                "question": f"Verify the {market} CO2 0-75g/km tax band, company car benefit formula, publish date, and PHEV certified CO2 applicability.",
                "intent": "external_search",
                "reason": "The first risk is treating a low-emission band as an automatic benefit without checking the official formula.",
                "expectedTools": ["search_market_news", "read_web_page"],
                "expectedOutput": "summary",
            },
            {
                "label": "Build PHEV TCO scenarios",
                "question": f"Build a {market} company-car TCO scenario table comparing PHEV with HEV/BEV by monthly payment, residual value, tax, energy use, charging behavior, and long-distance mileage.",
                "intent": "compare",
                "reason": "PHEV advantage must be proven by TCO and usage-risk scenarios, not just by CO2 band.",
                "expectedTools": ["query_msrp_pricing", "compare_vehicle_variants", "search_market_news"],
                "expectedOutput": "table",
            },
            {
                "label": "Split company-car fit",
                "question": f"Split which {market} company-car scenarios still justify PHEV and which should move to BEV or HEV.",
                "intent": "action",
                "reason": "The useful PM output is a scenario boundary, not a generic PHEV yes/no.",
                "expectedTools": ["query_country_snapshot", "query_msrp_pricing", "search_market_news"],
                "expectedOutput": "recommendation",
            },
            {
                "label": "Create policy PPT block",
                "question": f"Create a one-page {market} policy-impact PPT block for the CO2 0-75g/km band and PHEV: conclusion, evidence, TCO logic, risks, and next step.",
                "intent": "report",
                "reason": "The analysis should become a reviewable policy-impact artifact.",
                "expectedTools": ["search_market_news", "query_msrp_pricing", "compare_vehicle_variants"],
                "expectedOutput": "report",
            },
        ]
    return [
        {
            "id": _follow_up_id(str(item["intent"]), str(item["question"])),
            "label": str(item["label"]),
            "question": str(item["question"]),
            "intent": str(item["intent"]),
            "reason": str(item["reason"]),
            "expectedTools": _string_list(item["expectedTools"])[:4],
            "expectedOutput": str(item["expectedOutput"]),
            "priority": index + 1,
        }
        for index, item in enumerate(candidates)
    ]


def _normalize_one_follow_up(
    item: Any,
    *,
    index: int,
    country: str,
    question: str,
    tools: list[str],
    evidence_plan: dict[str, Any],
) -> dict[str, Any] | None:
    if isinstance(item, dict):
        item_question = _text(item.get("question")) or _text(item.get("label"))
        label = _text(item.get("label")) or _short_label(item_question)
        if not item_question:
            return None
        follow_intent = _valid_followup_intent(_text(item.get("intent")), index, evidence_plan)
        expected_tools = _string_list(item.get("expectedTools")) or tools or _tools_for_followup_intent(follow_intent, evidence_plan)
        follow_up = {
            "id": _text(item.get("id")) or _follow_up_id(follow_intent, item_question),
            "label": label,
            "question": item_question,
            "intent": follow_intent,
            "reason": _text(item.get("reason")) or _default_reason(follow_intent),
            "expectedTools": expected_tools[:4],
            "expectedOutput": _text(item.get("expectedOutput")) or _expected_output(follow_intent),
            "priority": _safe_priority(item.get("priority"), index),
        }
        risk = _text(item.get("risk"))
        if risk:
            follow_up["risk"] = risk
        return follow_up

    text = str(item or "").strip()
    if not text:
        return None
    follow_intent = _valid_followup_intent("", index, evidence_plan)
    return {
        "id": _follow_up_id(follow_intent, text),
        "label": _short_label(text),
        "question": text,
        "intent": follow_intent,
        "reason": _default_reason(follow_intent),
        "expectedTools": (tools or _tools_for_followup_intent(follow_intent, evidence_plan))[:4],
        "expectedOutput": _expected_output(follow_intent),
        "priority": index + 1,
    }


def _build_follow_up(
    *,
    follow_type: str,
    country: str,
    question: str,
    analysis_intent: str,
    tools: list[str],
    priority: int,
) -> dict[str, Any]:
    chinese = _contains_chinese(question)
    intent_specific = _intent_artifact_follow_up(
        follow_type=follow_type,
        country=country,
        question=question,
        analysis_intent=analysis_intent,
        chinese=chinese,
    )
    if intent_specific:
        expected_tools = _tools_for_followup_type(follow_type, tools)
        return {
            "id": _follow_up_id(follow_type, f"{intent_specific['label']}:{intent_specific['question']}"),
            "label": intent_specific["label"],
            "question": intent_specific["question"],
            "intent": follow_type,
            "reason": intent_specific["reason"],
            "expectedTools": expected_tools,
            "expectedOutput": intent_specific["expectedOutput"],
            "priority": priority,
        }
    if follow_type == "compare":
        label = "看竞品/邻国对比" if chinese else "Compare competitors/markets"
        follow_question = (
            f"把{country}的结论和核心竞品、邻国或上一周期做 side-by-side 对比。"
            if chinese
            else f"Compare this {country} finding against key competitors, neighboring markets, or the previous period."
        )
    elif follow_type == "why":
        label = "解释背后原因" if chinese else "Explain the drivers"
        follow_question = (
            f"结合销量、价格、政策和新闻，解释{country}这个变化背后的主要原因。"
            if chinese
            else f"Explain the main drivers behind this {country} movement using sales, pricing, policy, and news evidence."
        )
    elif follow_type == "action":
        label = "转成业务动作" if chinese else "Turn into actions"
        follow_question = (
            f"基于当前证据，给出{country}的产品、定价或配置动作建议。"
            if chinese
            else f"Turn the current evidence into product, pricing, or configuration actions for {country}."
        )
    elif follow_type == "data_check":
        label = "检查数据缺口" if chinese else "Check data gaps"
        follow_question = (
            f"检查{country}这个问题还缺哪些 MSRP、配置、销量或政策证据。"
            if chinese
            else f"Check which MSRP, configuration, sales, or policy evidence is still missing for {country}."
        )
    elif follow_type == "external_search":
        label = "查最新外部证据" if chinese else "Search fresh evidence"
        follow_question = (
            f"查询最近 30 天内{country}相关政策、新闻或竞品官网变化。"
            if chinese
            else f"Search the latest 30-day policy, news, or competitor website changes for {country}."
        )
    elif follow_type == "report":
        label = "生成汇报框架" if chinese else "Build a report frame"
        follow_question = (
            f"把这轮分析整理成一页汇报框架，包含结论、证据、风险和下一步。"
            if chinese
            else "Turn this analysis into a one-page report outline with conclusion, evidence, risks, and next steps."
        )
    else:
        label = "继续深挖数据" if chinese else "Drill into the data"
        follow_question = (
            f"把{country}的结果继续拆到车型、品牌、动力类型或价格带层面。"
            if chinese
            else f"Drill the {country} result down by model, brand, powertrain, or price band."
        )

    expected_tools = _tools_for_followup_type(follow_type, tools)
    return {
        "id": _follow_up_id(follow_type, f"{label}:{follow_question}"),
        "label": label,
        "question": follow_question,
        "intent": follow_type,
        "reason": _business_reason(follow_type, analysis_intent, chinese),
        "expectedTools": expected_tools,
        "expectedOutput": _expected_output(follow_type),
        "priority": priority,
    }


def _intent_artifact_follow_up(
    *,
    follow_type: str,
    country: str,
    question: str,
    analysis_intent: str,
    chinese: bool,
) -> dict[str, str]:
    if analysis_intent == "pricing_analysis":
        return _pricing_artifact_follow_up(follow_type, country=country, question=question, chinese=chinese)
    if analysis_intent == "competitor_compare":
        return _competitor_artifact_follow_up(follow_type, country=country, question=question, chinese=chinese)
    if analysis_intent == "market_overview":
        return _market_artifact_follow_up(follow_type, country=country, question=question, chinese=chinese)
    return {}


def _pricing_artifact_follow_up(
    follow_type: str,
    *,
    country: str,
    question: str,
    chinese: bool,
) -> dict[str, str]:
    subject = _target_model_label({"entities": {}}, question, chinese=chinese)
    if chinese:
        mapping = {
            "compare": {
                "label": "生成价格走廊表",
                "question": f"把{country}{subject}和核心竞品的 MSRP、主销价、月供/RV、促销支持做成 Pricing evidence table，并判断价格位置。",
                "reason": "定价问题的下一步应直接补价格矩阵，而不是泛泛对比。",
                "expectedOutput": "table",
            },
            "data_check": {
                "label": "补官方价格证据",
                "question": f"检查{country}{subject}还缺哪些官方 MSRP、竞品价格、月供/RV、配置价值和来源日期证据。",
                "reason": "没有可引用价格来源时，不能把目标价或材料价写成确定结论。",
                "expectedOutput": "checklist",
            },
            "action": {
                "label": "拆定价动作",
                "question": f"基于{country}{subject}的价格证据，给出低配锚点、高配主推、campaign/RV 和销售话术动作。",
                "reason": "价格分析最终要落到版本策略和销售动作。",
                "expectedOutput": "recommendation",
            },
            "report": {
                "label": "生成定价 PPT block",
                "question": f"生成{country}{subject}一页定价 PPT block：Key message、价格证据、配置价值、风险边界和下一步补数。",
                "reason": "定价判断适合沉淀成可复制汇报块。",
                "expectedOutput": "report",
            },
        }
    else:
        mapping = {
            "compare": {
                "label": "Build pricing corridor",
                "question": f"Build a {country} {subject} pricing evidence table with MSRP, main-trim price, monthly payment/RV, campaign support, and competitor position.",
                "reason": "Pricing questions need a corridor table, not a generic comparison.",
                "expectedOutput": "table",
            },
            "data_check": {
                "label": "Check price evidence",
                "question": f"Check which official MSRP, competitor price, monthly payment/RV, configuration-value, and source-date evidence is still missing for {country} {subject}.",
                "reason": "Target prices or user-material prices cannot become facts without source evidence.",
                "expectedOutput": "checklist",
            },
            "action": {
                "label": "Split pricing actions",
                "question": f"Turn the {country} {subject} pricing evidence into entry-trim anchor, high-trim push, campaign/RV, and sales-talk actions.",
                "reason": "Pricing analysis should become version and sales actions.",
                "expectedOutput": "recommendation",
            },
            "report": {
                "label": "Create pricing PPT block",
                "question": f"Create a one-page {country} {subject} pricing PPT block with key message, price evidence, configuration value, risk boundary, and next data step.",
                "reason": "The pricing judgment should become a reusable report artifact.",
                "expectedOutput": "report",
            },
        }
    return mapping.get(follow_type, {})


def _competitor_artifact_follow_up(
    follow_type: str,
    *,
    country: str,
    question: str,
    chinese: bool,
) -> dict[str, str]:
    subject = _target_model_label({"entities": {}}, question, chinese=chinese)
    if chinese:
        mapping = {
            "compare": {
                "label": "生成竞品矩阵",
                "question": f"把{country}{subject}和核心竞品的销量、价格、级别、动力、尺寸/座位、配置可见价值做成 Competitor comparison table。",
                "reason": "竞品问题下一步应补可扫读矩阵，而不是继续泛泛比较。",
                "expectedOutput": "table",
            },
            "why": {
                "label": "解释胜负前提",
                "question": f"解释{country}{subject}能赢或会输的前提：价格锚点、配置价值、用户场景、品牌风险和渠道条件。",
                "reason": "胜负判断需要先拆可验证前提。",
                "expectedOutput": "analysis",
            },
            "action": {
                "label": "拆定位动作",
                "question": f"基于{country}{subject}竞品证据，判断正面对抗、错位竞争或价格锚点，并输出可赢点、短板和销售话术。",
                "reason": "竞品分析要落到定位和销售动作。",
                "expectedOutput": "recommendation",
            },
            "report": {
                "label": "生成对标 PPT block",
                "question": f"生成{country}{subject}竞品对标 PPT block：主对标、价格/配置差异、可赢点、短板、补证清单。",
                "reason": "竞品结论适合沉淀成一页汇报。",
                "expectedOutput": "report",
            },
        }
    else:
        mapping = {
            "compare": {
                "label": "Build competitor matrix",
                "question": f"Build a {country} {subject} competitor table with sales, price, segment, powertrain, size/seats, and visible configuration value.",
                "reason": "Competitor questions need a scannable matrix, not a generic comparison.",
                "expectedOutput": "table",
            },
            "why": {
                "label": "Explain win/loss conditions",
                "question": f"Explain the {country} {subject} win/loss conditions: price anchor, configuration value, user scenario, brand risk, and channel fit.",
                "reason": "Win/loss should be decomposed into verifiable conditions first.",
                "expectedOutput": "analysis",
            },
            "action": {
                "label": "Split positioning actions",
                "question": f"Use the {country} {subject} competitor evidence to decide direct attack, differentiated positioning, or price anchor, then produce wins, weaknesses, and sales talk.",
                "reason": "Competitor analysis should become positioning and sales action.",
                "expectedOutput": "recommendation",
            },
            "report": {
                "label": "Create benchmark PPT block",
                "question": f"Create a {country} {subject} competitor PPT block with primary benchmark, price/config gaps, win points, weaknesses, and evidence gaps.",
                "reason": "The benchmark conclusion should become a one-page report artifact.",
                "expectedOutput": "report",
            },
        }
    return mapping.get(follow_type, {})


def _market_artifact_follow_up(
    follow_type: str,
    *,
    country: str,
    question: str,
    chinese: bool,
) -> dict[str, str]:
    if chinese:
        mapping = {
            "drilldown": {
                "label": "生成市场结构图",
                "question": f"把{country}市场拆到总量、动力结构、SUV segment、Top models 和价格带，并生成 market structure chart。",
                "reason": "市场总览下一步应把机会拆到可视化结构。",
                "expectedOutput": "chart",
            },
            "compare": {
                "label": "做邻国/竞品对比表",
                "question": f"把{country}与邻国或核心竞品市场做 side-by-side table，对比销量、动力 mix、SUV 结构和进入机会。",
                "reason": "市场机会需要横向对比确认是否可复制。",
                "expectedOutput": "table",
            },
            "action": {
                "label": "映射产品进入动作",
                "question": f"基于{country}市场结构，给出 OMODA/JAECOO 的车型优先级、动力路线、价格带和渠道动作。",
                "reason": "市场分析最终要转成产品进入顺序。",
                "expectedOutput": "recommendation",
            },
            "report": {
                "label": "生成市场机会页",
                "question": f"生成{country}市场机会 PPT block：机会 segment、关键证据、产品动作、风险和下一步补数。",
                "reason": "市场总览适合沉淀成机会页。",
                "expectedOutput": "report",
            },
        }
    else:
        mapping = {
            "drilldown": {
                "label": "Build market structure chart",
                "question": f"Break {country} into total size, powertrain mix, SUV segments, top models, and price bands, then build a market structure chart.",
                "reason": "Market overview should become a visual opportunity structure.",
                "expectedOutput": "chart",
            },
            "compare": {
                "label": "Build market comparison table",
                "question": f"Compare {country} with neighboring markets or core competitor markets by volume, powertrain mix, SUV structure, and entry opportunity.",
                "reason": "Market opportunity needs a side-by-side check.",
                "expectedOutput": "table",
            },
            "action": {
                "label": "Map entry actions",
                "question": f"Map the {country} market structure into OMODA/JAECOO model priority, powertrain route, price band, and channel actions.",
                "reason": "Market analysis should become entry sequence and product action.",
                "expectedOutput": "recommendation",
            },
            "report": {
                "label": "Create market opportunity page",
                "question": f"Create a {country} market opportunity PPT block with opportunity segment, key evidence, product action, risk, and next data step.",
                "reason": "Market overview should become a reusable opportunity page.",
                "expectedOutput": "report",
            },
        }
    return mapping.get(follow_type, {})


def _tools_for_followup_type(follow_type: str, tools: list[str]) -> list[str]:
    if follow_type == "compare":
        preferred = ["query_msrp_pricing", "compare_vehicle_variants", "query_country_snapshot"]
    elif follow_type == "why":
        preferred = ["analyze_market_dynamics", "search_market_news", "query_country_snapshot"]
    elif follow_type == "action":
        preferred = ["query_msrp_pricing", "compare_vehicle_variants", "query_country_snapshot"]
    elif follow_type == "data_check":
        preferred = ["query_country_snapshot", "query_msrp_pricing", "compare_vehicle_variants"]
    elif follow_type == "external_search":
        preferred = ["search_market_news", "read_web_page", "browser_snapshot"]
    elif follow_type == "report":
        preferred = ["build_market_chart", "query_country_snapshot", "search_market_news"]
    else:
        preferred = ["query_country_snapshot", "build_market_chart", "query_with_filters"]
    merged = [tool for tool in preferred if tool in tools] + [tool for tool in preferred if tool not in tools]
    return _dedupe_strings(merged)[:3]


def _tools_for_followup_intent(intent: str, evidence_plan: dict[str, Any]) -> list[str]:
    return _tools_for_followup_type(intent, _planned_tool_names(evidence_plan))


def _planned_tool_names(evidence_plan: dict[str, Any]) -> list[str]:
    allowed = evidence_plan.get("allowedTools")
    if isinstance(allowed, list):
        return [str(item) for item in allowed if str(item or "").strip()]
    tool_plan = evidence_plan.get("toolPlan")
    if isinstance(tool_plan, list):
        return [
            str(item.get("toolName"))
            for item in tool_plan
            if isinstance(item, dict) and str(item.get("toolName") or "").strip()
        ]
    return ["query_country_snapshot"]


def _valid_followup_intent(value: str, index: int, evidence_plan: dict[str, Any]) -> str:
    normalized = value.strip().lower()
    if normalized in FOLLOW_UP_INTENTS:
        return normalized
    plan_types = evidence_plan.get("followUpTypes")
    if isinstance(plan_types, list) and plan_types:
        candidate = str(plan_types[min(index, len(plan_types) - 1)]).strip().lower()
        if candidate in FOLLOW_UP_INTENTS:
            return candidate
    fallback = ["drilldown", "compare", "action", "report"]
    return fallback[min(index, len(fallback) - 1)]


def _default_reason(intent: str) -> str:
    mapping = {
        "drilldown": "当前结论需要继续拆到更细业务维度验证。",
        "compare": "对比竞品、邻国或历史周期能判断结论是否有业务意义。",
        "why": "原因分析需要交叉验证销量、价格、政策和新闻。",
        "action": "用户通常需要把分析转成产品、定价或配置动作。",
        "data_check": "先检查证据缺口可以避免基于不完整数据做结论。",
        "external_search": "最新政策、新闻或官网变化可能影响当前判断。",
        "report": "报告框架能把分析沉淀为可复用产出物。",
    }
    return mapping.get(intent, "推荐的下一步分析路径。")


def _business_reason(intent: str, analysis_intent: str, chinese: bool) -> str:
    if chinese:
        return f"当前问题识别为 {analysis_intent}，下一步适合走 {intent} 路径继续验证业务价值。"
    return f"The current question is routed as {analysis_intent}; the next useful step is a {intent} path."


def _expected_output(intent: str) -> str:
    mapping = {
        "drilldown": "chart",
        "compare": "table",
        "why": "analysis",
        "action": "recommendation",
        "data_check": "checklist",
        "external_search": "summary",
        "report": "report",
    }
    return mapping.get(intent, "summary")


def _follow_up_id(intent: str, text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", f"{intent}_{text}".lower()).strip("_")[:48]
    return f"fu_{slug}_{uuid.uuid5(uuid.NAMESPACE_URL, text).hex[:8]}"


def _short_label(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return "继续分析"
    return cleaned[:28] + ("..." if len(cleaned) > 28 else "")


def _safe_priority(value: Any, index: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return index + 1
    return max(1, min(parsed, 99))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return _dedupe_strings([str(item).strip() for item in value if str(item or "").strip()])


def _dedupe_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _dedupe_follow_ups(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen_questions: set[str] = set()
    for item in sorted(values, key=lambda follow_up: int(follow_up.get("priority") or 99)):
        question = str(item.get("question") or "").strip()
        if not question or question in seen_questions:
            continue
        seen_questions.add(question)
        result.append(item)
    return result


def _renumber_follow_ups(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for index, item in enumerate(values):
        updated = dict(item)
        updated["priority"] = index + 1
        result.append(updated)
    return result


def _text(value: Any) -> str:
    return str(value or "").strip() if value is not None else ""


def _contains_chinese(value: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in value)


def _contains_any(value: str, tokens: tuple[str, ...]) -> bool:
    return any(token in value for token in tokens)


def _country_display_name(value: str, *, chinese: bool) -> str:
    country = str(value or "").strip() or "当前市场"
    if not chinese:
        return country
    mapping = {
        "sweden": "瑞典",
        "finland": "芬兰",
        "norway": "挪威",
        "denmark": "丹麦",
        "germany": "德国",
        "hungary": "匈牙利",
    }
    return mapping.get(country.casefold(), country)
