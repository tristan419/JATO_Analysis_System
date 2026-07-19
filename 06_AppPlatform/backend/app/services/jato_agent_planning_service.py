from __future__ import annotations

import re
from typing import Any

from app.services.jato_business_method_distillation_service import get_active_pricing_method
from app.services.jato_country_resolution_service import COUNTRY_MENTION_ALIASES
from app.services.jato_country_resolution_service import resolve_effective_country
from app.services.jato_intent_tool_matrix_service import get_intent_tool_rule
from app.services.jato_intent_tool_matrix_service import merge_allowed_tools_with_rule
from app.services.jato_tool_registry_service import allowed_tools_for_intent
from app.services.jato_tool_registry_service import get_tool_card


SUPPORTED_INTENTS = {
    "market_overview",
    "pricing_analysis",
    "competitor_compare",
    "configuration_analysis",
    "inventory_analysis",
    "voc_analysis",
    "news_policy_search",
    "report_generation",
    "coding_debug",
    "general_qa",
}

_KNOWN_MODEL_NAME_PATTERN = (
    r"OMODA\s?9|OMODA9|OMODA\s?5|OMODA5|JAECOO\s?J7|JAECOO\s?J8|"
    r"J8|J7|O9|O5|EX30|EX40|EX60|EX90|XC40|XC60|XC90|"
    r"RAV4|Corolla\s+Cross|C-HR|Qashqai|Tucson|"
    r"MODEL Y|Sportage|Sorento|EV3|EV9|Enyaq|ID\.4|ID\.7|Kodiaq|Tayron"
)

_BRAND_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("OMODA", (r"\bOMODA\b", r"欧萌达")),
    ("JAECOO", (r"\bJAECOO\b", r"捷途")),
    ("Toyota", (r"\bToyota\b", r"丰田")),
    ("Kia", (r"\bKia\b", r"起亚")),
    ("Volvo", (r"\bVolvo\b", r"沃尔沃")),
    ("Skoda", (r"\bSkoda\b", r"斯柯达")),
    ("Volkswagen", (r"\bVolkswagen\b", r"\bVW\b", r"大众")),
)

_FEATURE_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("冬季包", (r"冬季包", r"winter package")),
    ("冬季胎", (r"冬季胎", r"winter tires?")),
    ("热泵", (r"热泵", r"heat pump")),
    ("电池预热", (r"电池预热", r"battery pre(?:-| )?conditioning")),
    ("座椅加热", (r"座椅加热", r"heated seats?")),
    ("方向盘加热", (r"方向盘加热", r"heated steering")),
    ("拖车钩", (r"拖车钩", r"tow hook", r"towbar", r"trailer hitch")),
    ("roof load", (r"roof load", r"车顶载重")),
    ("V2H", (r"\bV2H\b", r"vehicle[- ]to[- ]home")),
    ("V2L", (r"\bV2L\b", r"vehicle[- ]to[- ]load")),
    ("HUD", (r"\bHUD\b", r"head[- ]up display", r"抬头显示")),
    ("ADAS", (r"\bADAS\b", r"驾驶辅助", r"智能驾驶")),
    ("540°全景影像", (r"540°?全景影像", r"540\\s?degree")),
    ("座椅通风", (r"座椅通风", r"ventilated seats?")),
    ("座椅记忆", (r"座椅记忆", r"seat memory")),
    ("感应电尾门", (r"感应电尾门", r"hands[- ]free tailgate")),
    ("全景天窗", (r"全景天窗", r"panoramic roof")),
    ("80kWh", (r"80\s?kwh", r"80\s?度电")),
    ("95kWh", (r"95\s?kwh", r"95\s?度电")),
    ("800V", (r"800\s?v", r"800伏")),
    ("双电机", (r"双电机", r"dual motor")),
)


def build_evidence_plan(country: str, question: str) -> dict[str, Any]:
    effective_country = resolve_effective_country(country, question)
    routed = route_intent(effective_country, question)
    allowed_tools = routed["allowedTools"]
    evidence_needed = _evidence_needed_for_question(routed["intent"], question)
    tool_plan_tools = _tool_plan_tools(
        allowed_tools,
        required_tools=routed["requiredTools"],
    )
    tool_plan = [
        {
            "toolName": tool_name,
            "input": _tool_input(tool_name, effective_country, question, routed),
            "expectedEvidence": _expected_evidence(tool_name),
            "costLevel": (get_tool_card(tool_name) or {}).get("costLevel", "low"),
            "latencyLevel": (get_tool_card(tool_name) or {}).get("latencyLevel", "fast"),
        }
        for tool_name in tool_plan_tools
    ]
    return {
        "intent": routed["intent"],
        "country": effective_country,
        "entities": routed["entities"],
        "evidenceNeeded": evidence_needed,
        "toolPlan": tool_plan,
        "allowedTools": allowed_tools,
        "requiredTools": routed["requiredTools"],
        "optionalTools": routed["optionalTools"],
        "mustHaveEvidence": routed["mustHaveEvidence"],
        "answerMustMention": routed["answerMustMention"],
        "answerMode": routed["answerMode"],
        "followUpTypes": routed["followUpTypes"],
        "shouldUseWeb": routed["shouldUseWeb"],
    }


def route_intent(country: str, question: str) -> dict[str, Any]:
    text = str(question or "").lower()
    intent = _classify_intent(text)
    countries = _extract_country_candidates(country, question)
    models = _extract_model_candidates(question)
    brands = _extract_brand_candidates(question)
    features = _extract_feature_candidates(question)
    powertrains = _extract_powertrain_candidates(question)
    competitors = _extract_competitor_candidates(question)
    method_competitors = _pricing_method_competitors(country, question, models=models)
    if method_competitors and intent in {"pricing_analysis", "competitor_compare", "report_generation", "market_overview"}:
        competitors = _dedupe([*competitors, *method_competitors])
    competitors = _filter_competitors_for_models(competitors, models[:1])
    allowed_tools = _allowed_tools(intent, text, countries=countries)
    allowed_tools = merge_allowed_tools_with_rule(intent, allowed_tools)
    rule = get_intent_tool_rule(intent)
    required_tools = _required_tools_for_question(intent, text, rule["requiredTools"], countries=countries)
    optional_tools = _optional_tools_for_question(intent, text, rule["optionalTools"])
    allowed_tools = _dedupe([*required_tools, *allowed_tools, *optional_tools])
    if not _allows_cross_country_tool(intent, text, countries):
        required_tools = [tool for tool in required_tools if tool != "query_cross_country"]
        optional_tools = [tool for tool in optional_tools if tool != "query_cross_country"]
        allowed_tools = [tool for tool in allowed_tools if tool != "query_cross_country"]
    if intent == "pricing_analysis" and not _allows_pricing_source_repair(text):
        allowed_tools = [tool for tool in allowed_tools if tool != "search_market_news"]
    if intent == "market_overview" and not _allows_market_external_research(text):
        optional_tools = [tool for tool in optional_tools if tool != "search_market_news"]
        allowed_tools = [tool for tool in allowed_tools if tool != "search_market_news"]
    if intent == "report_generation" and not _report_generation_needs_external_context(text):
        external_tools = {"search_market_news", "pageindex_search_documents", "minirag_query_graph"}
        optional_tools = [tool for tool in optional_tools if tool not in external_tools]
        allowed_tools = [tool for tool in allowed_tools if tool not in external_tools]
    return {
        "intent": intent,
        "country": country,
        "entities": {
            "countries": countries,
            "models": models,
            "competitors": competitors,
            "brands": brands,
            "features": features,
            "powertrains": powertrains,
        },
        "requiredEvidence": [item["name"] for item in _evidence_needed_for_question(intent, question)],
        "allowedTools": allowed_tools,
        "requiredTools": required_tools,
        "optionalTools": optional_tools,
        "mustHaveEvidence": rule["mustHaveEvidence"],
        "answerMustMention": rule["answerMustMention"],
        "shouldUseWeb": any((get_tool_card(tool) or {}).get("requiresNetwork") for tool in allowed_tools),
        "answerMode": _answer_mode(intent, text),
        "followUpTypes": _follow_up_types(intent),
    }


def _tool_plan_tools(allowed_tools: list[str], *, required_tools: list[str]) -> list[str]:
    """Keep every required evidence tool even when a task needs more than four tools."""
    limit = max(4, len(required_tools))
    return _dedupe([*required_tools, *allowed_tools])[:limit]


def _classify_intent(text: str) -> str:
    if _contains_any(text, ["code", "debug", "traceback", "报错", "代码", "bug"]):
        return "coding_debug"
    if _contains_any(text, ["ppt", "report", "slide", "汇报", "报告", "一页", "framework", "大纲"]):
        return "report_generation"
    if _contains_any(text, [
        "voc", "forum", "review", "complaint", "voice of customer",
        "口碑", "论坛", "投诉", "消费者", "车主", "用户声音", "用户需求",
        "购买卖点", "卖点", "高频需求", "真实需求", "痛点", "用户会不会",
        "吐槽", "抱怨", "差评", "用户反馈",
    ]):
        return "voc_analysis"
    if _contains_any(text, [
        "news", "policy", "subsidy", "tax", "incentive", "latest", "research", "source", "sources",
        "citation", "citations", "search", "web", "tavily", "新闻", "政策", "补贴", "税", "最新",
        "研究", "来源", "引用", "搜索", "检索", "联网", "网页", "elbilspremien", "benefit",
        "company car", "co₂", "co2", "emission", "排放", "税率", "阶梯", "价格上限",
    ]):
        return "news_policy_search"
    if _contains_any(text, [
        "inventory", "stock", "order", "bom", "material", "material code", "经销存", "库存", "订单",
        "物料", "物料号", "选品表", "生命周期", "合并 pi", "分市场", "客户可编辑数量",
        "车辆生成", "车辆分市场", "市场生成",
    ]) or re.search(r"\bpi\b", text):
        return "inventory_analysis"
    # A named vehicle-vs-vehicle decision is a competitor question even when the
    # user asks for price/configuration evidence.  Pricing becomes the intent
    # only when the user is explicitly deciding a price, not merely requesting
    # price evidence to support a competitive argument.
    if _is_named_competitor_decision_question(text):
        return "competitor_compare"
    if _contains_any(text, [
        "price", "pricing", "msrp", "lease", "leasing", "monthly", "tco", "residual value",
        "定价", "价格", "售价", "月供", "残值", "便宜", "贵", "合理", "价差", "价格带",
    ]) or re.search(r"\brv\b", text) or _contains_currency_signal(text):
        return "pricing_analysis"
    if _is_powertrain_route_question(text):
        return "market_overview"
    if _is_model_market_validation_question(text):
        return "market_overview"
    if _is_powertrain_space_question(text):
        return "market_overview"
    if _is_strong_configuration_question(text):
        return "configuration_analysis"
    if _contains_any(text, [
        "compare", "vs", "versus", "against", "competitor", "rival",
        "竞品", "对比", "相比", "对标", "能打", "核心竞品", "定位差异", "定位区别", "定位",
    ]):
        return "competitor_compare"
    if _contains_any(text, [
        "configuration", "config", "variant", "trim", "feature", "spec", "battery", "range",
        "kwh", "800v", "dual motor", "配置", "版型", "规格", "续航", "电池", "冬季包",
        "拖车", "双电机", "座椅", "hud", "adas", "roof load",
    ]):
        return "configuration_analysis"
    if _contains_any(text, ["why", "driver", "drivers", "原因", "为什么", "背后", "影响因素"]):
        return "market_overview"
    if _contains_any(text, ["market", "share", "volume", "ranking", "trend", "chart", "销量", "份额", "排名", "趋势", "图表", "市场"]):
        return "market_overview"
    return "general_qa"


def _is_named_competitor_decision_question(text: str) -> bool:
    models = _extract_model_candidates(text)
    if len(models) < 2:
        return False
    return _contains_any(text, [
        "为什么能打",
        "能打",
        "竞争优势",
        "可赢点",
        "胜出",
        "对标",
        "核心竞品",
        "竞品",
        "定位差异",
        "定位区别",
        "竞争",
        "对手",
        "应该对标",
        "谁更",
    ])


def _is_strong_configuration_question(text: str) -> bool:
    return _contains_any(text, [
        "battery", "range", "kwh", "800v", "dual motor", "preconditioning",
        "电池", "续航", "冬季包", "拖车", "双电机", "座椅", "hud", "adas", "roof load",
        "热泵", "全景影像", "配置包", "配置差异", "配置对比", "配置表", "配置矩阵",
        "配置怎么讲", "配置逻辑", "配置价值", "版本差异", "版本对比",
    ])


def _is_powertrain_route_question(text: str) -> bool:
    powertrains = _extract_powertrain_candidates(text)
    if len(powertrains) < 2:
        return False
    return _contains_any(
        text,
        [
            "还是",
            "or",
            "vs",
            "versus",
            "对比",
            "相比",
            "适合",
            "优先",
            "主推",
            "推",
            "路线",
            "结构",
            "进入顺序",
            "which",
        ],
    )


def _is_model_market_validation_question(text: str) -> bool:
    has_model_or_powertrain = bool(
        re.search(r"\b(?:j7|j8|j9|o5|o9|omoda|jaecoo|hev|phev|bev|ev)\b", text)
        or _contains_any(text, ["车型", "产品", "动力路线", "车系"])
    )
    if not has_model_or_powertrain:
        return False
    return _contains_any(
        text,
        [
            "适合",
            "适不适合",
            "值得",
            "验证",
            "继续验证",
            "机会",
            "市场情况",
            "市场表现",
            "市场现状",
            "市场空间",
            "市场规模",
            "情况怎么样",
            "进入",
            "上市",
            "推进",
            "可行",
            "可不可行",
            "有没有理由",
            "market fit",
            "validate",
            "validation",
            "worth",
            "opportunity",
            "entry",
            "launch",
            "feasible",
        ],
    )


def _is_powertrain_space_question(text: str) -> bool:
    has_powertrain = bool(
        re.search(r"\b(?:bev|phev|hev|ev|ice)\b", text)
        or _contains_any(text, ["纯电", "插混", "混动", "燃油", "动力路线", "动力结构"])
    )
    if not has_powertrain:
        return False
    return _contains_any(
        text,
        [
            "增长",
            "下滑",
            "压缩",
            "替代",
            "挤压",
            "空间",
            "趋势",
            "渗透率",
            "结构变化",
            "增长是否",
            "会不会压缩",
            "会压缩",
            "growth",
            "decline",
            "replace",
            "substitute",
            "compress",
            "space",
            "penetration",
        ],
    )


def _is_segment_structure_question(text: str) -> bool:
    has_segment = "suv" in text and _contains_any(text, ["a0", "a 级", "a级", "suv a", "segment", "细分", "级别"])
    if not has_segment:
        return False
    return _contains_any(text, ["主销", "结构", "为什么", "原因", "why", "driver", "drivers", "机会", "集中"])


def _is_powertrain_market_structure_question(text: str) -> bool:
    has_powertrain = bool(
        re.search(r"\b(?:bev|phev|hev|ev|ice)\b", text)
        or _contains_any(text, ["纯电", "插混", "混动", "动力路线", "动力结构"])
    )
    if not has_powertrain:
        return False
    return _contains_any(
        text,
        [
            "市场",
            "机会",
            "适合",
            "适不适合",
            "验证",
            "继续验证",
            "为什么",
            "原因",
            "结构",
            "主销",
            "集中",
            "worth",
            "fit",
            "opportunity",
            "market",
            "why",
        ],
    )


def _allowed_tools(intent: str, text: str, *, countries: list[str] | None = None) -> list[str]:
    if intent == "inventory_analysis":
        if _is_pi_market_split_question(text) and _has_multiple_countries(countries or []):
            return ["query_cross_country", "query_with_filters", "query_country_snapshot"]
        return ["query_country_snapshot", "query_with_filters"]
    if intent == "coding_debug":
        return ["query_country_snapshot"]
    if intent == "competitor_compare":
        if _contains_any(text, ["config", "feature", "trim", "配置", "版型", "规格"]):
            return ["compare_vehicle_variants", "query_msrp_pricing", "query_country_snapshot"]
        return ["query_msrp_pricing", "compare_vehicle_variants", "query_country_snapshot"]
    if intent == "market_overview" and (_is_segment_structure_question(text) or _is_powertrain_market_structure_question(text)):
        tools = ["query_country_snapshot", "build_market_chart", "query_segment_breakdown", "query_with_filters", "analyze_market_dynamics", "analyze_model_performance"]
        if _market_overview_needs_competitor_evidence(text):
            tools.extend(["compare_competitive_set", "query_msrp_pricing"])
        return _dedupe(tools)
    if intent == "market_overview" and _is_cross_country_question(text, countries or []):
        return ["query_cross_country", "query_country_snapshot", "build_market_chart", "analyze_market_dynamics"]
    if intent == "market_overview" and _contains_any(text, ["why", "driver", "drivers", "原因", "为什么", "背后", "影响因素"]):
        return ["analyze_model_performance", "analyze_market_dynamics", "query_country_snapshot"]
    if intent == "news_policy_search":
        if "http://" in text or "https://" in text:
            return ["read_web_page", "browser_snapshot", "query_country_snapshot", "build_market_chart", "search_market_news"]
        if _needs_policy_pricing_context(text):
            return [
                "search_market_news",
                "query_msrp_pricing",
                "query_country_snapshot",
                "build_market_chart",
                "pageindex_search_documents",
                "minirag_query_graph",
            ]
        if _contains_any(text, ["research", "source", "sources", "citation", "citations", "tavily", "联网", "来源", "引用", "检索"]):
            return ["search_market_news", "query_country_snapshot", "build_market_chart", "pageindex_search_documents", "minirag_query_graph"]
        return ["search_market_news", "query_country_snapshot", "build_market_chart", "pageindex_search_documents", "minirag_query_graph"]
    if intent == "voc_analysis":
        return ["search_market_news", "query_country_snapshot", "minirag_query_graph"]
    if intent == "report_generation":
        if _report_generation_needs_external_context(text):
            return ["search_market_news", "query_country_snapshot", "build_market_chart", "pageindex_search_documents", "minirag_query_graph"]
        if _contains_any(text, ["price", "pricing", "msrp", "定价", "价格", "售价", "月供", "便宜", "合理", "价差"]) or _contains_currency_signal(text):
            return ["query_msrp_pricing", "compare_competitive_set", "build_market_chart", "query_country_snapshot"]
        if _contains_any(text, ["compare", "vs", "versus", "competitor", "rival", "竞品", "对比", "对标", "定位差异"]):
            return ["compare_vehicle_variants", "query_msrp_pricing", "compare_competitive_set", "build_market_chart"]
        if _contains_any(text, ["market", "share", "penetration", "trend", "市场", "份额", "渗透率", "变化", "趋势"]):
            return ["query_country_snapshot", "build_market_chart"]
        return ["build_market_chart", "query_country_snapshot"]
    return allowed_tools_for_intent(intent)


def _required_tools_for_question(intent: str, text: str, default_tools: list[str], *, countries: list[str] | None = None) -> list[str]:
    if intent == "inventory_analysis":
        if _is_pi_market_split_question(text) and _has_multiple_countries(countries or []):
            return ["query_cross_country", "query_with_filters"]
        return list(default_tools)
    if intent == "pricing_analysis":
        required = list(default_tools)
        if _pricing_needs_competitor_context(text):
            required.append("compare_competitive_set")
        if _needs_price_corridor(text):
            required.append("compare_competitive_set")
            required.append("query_price_positioning")
        if _pricing_needs_configuration_context(text):
            required.append("compare_vehicle_variants")
        if _pricing_needs_market_context(text):
            required.append("query_country_snapshot")
            required.append("build_market_chart")
        if _pricing_needs_leasing_evidence(text):
            required.append("query_leasing_offers")
        if _is_fleet_leasing_powertrain_question(text):
            required.append("search_market_news")
        if _needs_pricing_external_context(text):
            required.append("search_market_news")
        if _is_fleet_leasing_powertrain_question(text):
            required.append("query_country_snapshot")
            required.append("build_market_chart")
        return _dedupe(required)
    if intent == "competitor_compare" and _needs_competitor_gap_context(text):
        if _competitor_needs_market_context(text):
            return _dedupe([
                *default_tools,
                "build_market_chart",
                "query_msrp_pricing",
                "compare_vehicle_variants",
            ])
        return _dedupe([*default_tools, "compare_vehicle_variants", "query_msrp_pricing"])
    if intent == "configuration_analysis":
        required = ["query_cross_country"] if _is_cross_country_question(text, countries or []) else list(default_tools)
        needs_competitor_context = _contains_any(text, [
            "compare", "vs", "versus", "competitor", "rival", "竞品", "对比", "对标", "相比",
        ])
        if needs_competitor_context:
            required = _dedupe(["compare_competitive_set", *required])
        if required == ["query_cross_country"]:
            required.append("compare_vehicle_variants")
        if _contains_any(text, ["冬季包", "冬季胎", "拖车", "roof load", "高频需求", "用户声音", "吐槽"]):
            required.append("search_market_news")
        if _contains_any(text, ["95kwh", "95 kwh", "800v", "800 v", "双电机", "4.7m", "4.7 m", "能打", "竞品"]):
            required.append("compare_competitive_set")
        if _contains_any(text, ["80kwh", "80 kwh", "a0", "小型suv", "小型 suv", "segment", "细分市场", "市场"]):
            if "query_cross_country" not in required:
                required.append("query_country_snapshot")
        if _configuration_needs_market_chart_context(text):
            required.append("build_market_chart")
        return _dedupe(required)
    if intent == "market_overview" and _is_cross_country_question(text, countries or []):
        return ["query_cross_country"]
    if intent == "market_overview" and (_is_segment_structure_question(text) or _is_powertrain_market_structure_question(text)):
        required = ["query_country_snapshot", "build_market_chart", "query_segment_breakdown"]
        if _market_question_needs_time_series(text):
            required.append("query_time_series")
        if _market_overview_needs_competitor_evidence(text):
            required.append("compare_competitive_set")
            required.append("query_msrp_pricing")
        return _dedupe(required)
    if intent == "news_policy_search":
        required = list(default_tools)
        if _needs_policy_pricing_context(text):
            required.append("query_msrp_pricing")
        return _dedupe(required)
    if intent == "market_overview" and _market_question_needs_time_series(text):
        return _dedupe([*default_tools, "query_time_series"])
    if intent != "report_generation":
        return list(default_tools)
    if _needs_policy_source_context(text):
        required = ["search_market_news", "query_country_snapshot", "build_market_chart"]
        if _needs_policy_pricing_context(text):
            required.append("query_msrp_pricing")
        return _dedupe(required)
    if _contains_any(text, ["price", "pricing", "msrp", "定价", "价格", "售价", "月供", "便宜", "合理", "价差"]) or _contains_currency_signal(text):
        return ["query_msrp_pricing", "compare_competitive_set", "build_market_chart"]
    if _contains_any(text, ["compare", "vs", "versus", "competitor", "rival", "竞品", "对比", "对标", "定位差异"]):
        return ["compare_competitive_set", "compare_vehicle_variants", "query_msrp_pricing"]
    if _contains_any(text, ["market", "share", "penetration", "trend", "市场", "份额", "渗透率", "变化", "趋势"]):
        return ["query_country_snapshot", "build_market_chart"]
    return list(default_tools)


def _needs_competitor_gap_context(text: str) -> bool:
    return _contains_any(text, [
        "vs",
        "versus",
        "against",
        "compare",
        "difference",
        "delta",
        "gap",
        "positioning",
        "对比",
        "相比",
        "对标",
        "差异",
        "区别",
        "定位差异",
        "定位区别",
        "能打",
        "为什么",
    ])


def _market_overview_needs_competitor_evidence(text: str) -> bool:
    return _contains_any(text, [
        "competitor",
        "competitors",
        "rival",
        "rivals",
        "vs",
        "versus",
        "price",
        "pricing",
        "configuration",
        "config",
        "feature",
        "竞品",
        "对标",
        "价格",
        "定价",
        "配置",
        "车型级竞品",
        "车型级价格",
        "车型级配置",
        "价格矩阵",
        "配置矩阵",
    ])


def _market_question_needs_time_series(text: str) -> bool:
    return _contains_any(text, [
        "monthly trend",
        "yearly trend",
        "time series",
        "month by month",
        "monthly",
        "yearly",
        "trend",
        "月度走势",
        "月度趋势",
        "逐月",
        "年度趋势",
        "时间序列",
        "走势",
        "趋势",
    ])


def _competitor_needs_market_context(text: str) -> bool:
    if "j8" in text and "sorento" in text:
        return True
    if _contains_any(text, ["7座", "7 座", "四驱", "4wd", "awd", "sorento"]):
        return True
    return _contains_any(text, ["能打", "为什么"]) and _contains_any(
        text,
        ["phev", "hev", "bev", "suv", "company car", "公司车", "家庭", "场景", "冬季"],
    )


def _optional_tools_for_question(intent: str, text: str, default_tools: list[str]) -> list[str]:
    if intent == "pricing_analysis" and _needs_pricing_external_context(text):
        return _dedupe([*default_tools, "compare_competitive_set", "query_price_positioning", "search_market_news", "query_country_snapshot"])
    if intent == "pricing_analysis" and _allows_pricing_source_repair(text):
        return _dedupe([*default_tools, "compare_competitive_set", "query_price_positioning", "query_country_snapshot", "search_market_news"])
    if intent == "competitor_compare" and _allows_competitor_price_source_repair(text):
        return _dedupe([*default_tools, "query_msrp_pricing", "compare_vehicle_variants", "query_country_snapshot", "search_market_news"])
    if intent == "pricing_analysis":
        return [tool for tool in default_tools if tool != "search_market_news"]
    if intent != "report_generation":
        return list(default_tools)
    if _report_generation_needs_external_context(text):
        return _dedupe([*default_tools, "search_market_news", "query_country_snapshot", "build_market_chart"])
    if _contains_any(text, ["price", "pricing", "msrp", "定价", "价格", "售价", "月供", "便宜", "合理", "价差"]) or _contains_currency_signal(text):
        return _dedupe([*default_tools, "build_market_chart", "query_country_snapshot"])
    if _contains_any(text, ["compare", "vs", "versus", "competitor", "rival", "竞品", "对比", "对标", "定位差异"]):
        return _dedupe([*default_tools, "compare_competitive_set", "build_market_chart", "query_country_snapshot"])
    return [
        tool
        for tool in default_tools
        if tool not in {"search_market_news", "pageindex_search_documents", "minirag_query_graph"}
    ]


def _allows_market_external_research(text: str) -> bool:
    return _contains_any(
        text,
        [
            "research",
            "source",
            "sources",
            "citation",
            "citations",
            "latest",
            "news",
            "policy",
            "subsidy",
            "tax",
            "tavily",
            "联网",
            "来源",
            "引用",
            "检索",
            "最新",
            "新闻",
            "政策",
            "补贴",
            "税",
        ],
    )


def _evidence_needed_for_intent(intent: str) -> list[dict[str, Any]]:
    mapping: dict[str, list[tuple[str, str]]] = {
        "market_overview": [
            ("market_kpis", "需要市场规模、份额、排名或趋势作为基础证据。"),
            ("trend_or_mix", "需要能解释结构变化的趋势或动力类型拆分。"),
        ],
        "pricing_analysis": [
            ("current_msrp", "定价问题必须先验证当前 MSRP 或价格样本。"),
            ("price_corridor", "需要竞品价格走廊判断定位是否合理。"),
        ],
        "competitor_compare": [
            ("competitor_set", "需要明确竞品池和同级对比对象。"),
            ("price_or_config_gap", "需要价格、配置或销量差异解释对比结论。"),
        ],
        "configuration_analysis": [
            ("feature_diff", "配置问题必须读取车型/版本配置差异。"),
            ("user_value_impact", "需要把配置差异转成用户价值或价格影响。"),
        ],
        "inventory_analysis": [
            ("stock_or_order_signal", "经销存问题需要库存、订单或物料状态证据。"),
            ("market_context", "需要市场需求背景避免只看供应侧。"),
        ],
        "voc_analysis": [
            ("consumer_signal", "VOC 问题需要消费者评论、投诉或论坛证据。"),
            ("market_context", "缺少直接 VOC 时，需要用注册结构、动力/细分市场和用户场景做代理判断，并明确这不是消费者调研结论。"),
            ("model_mapping", "需要映射到具体车型或竞品。"),
        ],
        "news_policy_search": [
            ("fresh_external_signal", "新闻政策问题需要最新外部或文档证据。"),
            ("business_impact", "需要说明政策/新闻对销量、价格或产品动作的影响。"),
        ],
        "report_generation": [
            ("supporting_evidence", "需要图表、表格或来源支撑报告内容。"),
            ("report_outline", "报告类问题需要清晰输出结构。"),
        ],
        "coding_debug": [
            ("error_context", "代码问题需要错误上下文和影响范围。"),
        ],
        "general_qa": [
            ("basic_context", "通用问题至少需要基础市场上下文或说明无法验证。"),
        ],
    }
    return [
        {"name": name, "reason": reason, "priority": index + 1}
        for index, (name, reason) in enumerate(mapping.get(intent, mapping["general_qa"]))
    ]


def _evidence_needed_for_question(intent: str, question: str) -> list[dict[str, Any]]:
    items = _evidence_needed_for_intent(intent)
    if intent == "pricing_analysis" and _pricing_needs_leasing_evidence(str(question or "").casefold()):
        items.append({
            "name": "leasing_tco_or_company_car_evidence",
            "reason": "月供、残值、合同期限或总合同成本必须来自可追溯的 leasing offer。",
            "priority": len(items) + 1,
        })
    return items


def _tool_input(tool_name: str, country: str, question: str, routed: dict[str, Any]) -> dict[str, Any]:
    if tool_name in {"read_web_page", "browser_snapshot"}:
        return {"country": country, "question": question, "url": _first_url(question)}
    payload: dict[str, Any] = {"country": country, "question": question}
    countries = routed.get("entities", {}).get("countries") if isinstance(routed.get("entities"), dict) else []
    if tool_name == "query_cross_country":
        if not isinstance(countries, list) or len(countries) < 2:
            countries = [country]
        payload["countries"] = ", ".join(str(item) for item in countries if str(item or "").strip())
        return payload
    if isinstance(countries, list) and len(countries) > 1:
        payload["countries"] = [str(item) for item in countries if str(item or "").strip()]
    models = routed.get("entities", {}).get("models") if isinstance(routed.get("entities"), dict) else []
    competitors = routed.get("entities", {}).get("competitors") if isinstance(routed.get("entities"), dict) else []
    brands = routed.get("entities", {}).get("brands") if isinstance(routed.get("entities"), dict) else []
    features = routed.get("entities", {}).get("features") if isinstance(routed.get("entities"), dict) else []
    if isinstance(models, list) and models:
        payload["models"] = models[:4]
        payload["model"] = models[0]
    if isinstance(brands, list) and brands and tool_name in {
        "search_market_news",
        "query_country_snapshot",
        "query_with_filters",
        "compare_competitive_set",
        "query_msrp_pricing",
        "query_leasing_offers",
    }:
        brand_values = [str(item).strip() for item in brands if str(item or "").strip()]
        payload["brands"] = brand_values[:6]
        if len(brand_values) == 1:
            payload["brand"] = brand_values[0]
    if isinstance(features, list) and features and tool_name in {
        "search_market_news",
        "compare_vehicle_variants",
        "compare_competitive_set",
        "query_country_snapshot",
        "query_with_filters",
    }:
        feature_values = [str(item).strip() for item in features if str(item or "").strip()]
        payload["features"] = feature_values[:8]
        payload["featureKeywords"] = " ".join(feature_values[:8])
    if isinstance(competitors, list) and competitors and tool_name in {"query_msrp_pricing", "query_leasing_offers", "compare_competitive_set", "query_price_positioning", "compare_vehicle_variants"}:
        competitor_values = [str(item).strip() for item in competitors if str(item or "").strip()]
        payload["competitors"] = competitor_values[:6]
        if tool_name == "query_msrp_pricing":
            model_values = [str(item).strip() for item in (models or []) if str(item or "").strip()]
            payload["models"] = _dedupe([*model_values, *competitor_values])[:8]
        if tool_name == "query_leasing_offers":
            model_values = [str(item).strip() for item in (models or []) if str(item or "").strip()]
            payload["models"] = _dedupe([*model_values, *competitor_values])[:8]
        if tool_name == "compare_vehicle_variants":
            model_values = [str(item).strip() for item in (models or []) if str(item or "").strip()]
            payload["models"] = _dedupe([*model_values, *competitor_values])[:8]
    if tool_name in {"query_with_filters", "query_time_series", "query_segment_breakdown", "query_powertrain_trend"}:
        powertrain = _powertrain_filter_from_question(question, routed)
        if powertrain:
            payload["powertrain"] = powertrain
        segment = _segment_filter_from_question(question)
        if segment:
            payload["segment"] = segment
    if tool_name == "query_time_series":
        payload["granularity"] = (
            "yearly"
            if _contains_any(str(question or "").casefold(), ["yearly", "annual", "年度", "逐年"])
            else "monthly"
        )
    if tool_name == "query_price_positioning" and _needs_price_corridor(str(question or "").lower()):
        payload.pop("model", None)
        payload.pop("models", None)
    return payload


def _powertrain_filter_from_question(question: str, routed: dict[str, Any]) -> str:
    entities = routed.get("entities") if isinstance(routed.get("entities"), dict) else {}
    powertrains = entities.get("powertrains") if isinstance(entities.get("powertrains"), list) else []
    if len([item for item in powertrains if str(item or "").strip()]) > 1:
        return ""
    for item in powertrains:
        token = str(item or "").strip().upper()
        if token in {"BEV", "PHEV", "HEV", "EV", "ICE", "MHEV", "REEV"}:
            return "BEV" if token == "EV" else token
    text = str(question or "").casefold()
    for token in ("PHEV", "HEV", "BEV", "MHEV", "REEV", "ICE"):
        if re.search(rf"(?<![a-z0-9]){token.casefold()}(?![a-z0-9])", text):
            return token
    if "插混" in text:
        return "PHEV"
    if "混动" in text:
        return "HEV"
    if "纯电" in text or "电动" in text:
        return "BEV"
    return ""


def _extract_powertrain_candidates(question: str) -> list[str]:
    text = str(question or "")
    text_lower = text.casefold()
    matches: list[tuple[int, int, str]] = []
    patterns = [
        ("PHEV", r"(?<![a-z0-9])phev(?![a-z0-9])|插混|插电混动|插电式混动"),
        ("HEV", r"(?<![a-z0-9])hev(?![a-z0-9])|(?<!插电)混动|油混"),
        ("BEV", r"(?<![a-z0-9])bev(?![a-z0-9])|(?<![a-z0-9])ev(?![a-z0-9])|纯电"),
        ("MHEV", r"(?<![a-z0-9])mhev(?![a-z0-9])|轻混"),
        ("REEV", r"(?<![a-z0-9])reev(?![a-z0-9])|增程"),
        ("ICE", r"(?<![a-z0-9])ice(?![a-z0-9])|燃油"),
    ]
    for order, (token, pattern) in enumerate(patterns):
        for match in re.finditer(pattern, text_lower):
            matches.append((match.start(), order, token))
    matches.sort()
    return _dedupe([token for _, _, token in matches])


def _segment_filter_from_question(question: str) -> str:
    text = str(question or "").casefold()
    if any(token in text for token in ("suv a0/a", "suv a0 / a", "suv-a0/a", "suv a0和a", "suv a0 和 a")):
        return ""
    if "suv a0" in text or "suv-a0" in text or "小型 suv" in text or "小型suv" in text:
        return "SUV A0"
    if "suv a" in text or "suv-a" in text or "a 级" in text or "a级" in text:
        return "SUV A"
    return ""


def _expected_evidence(tool_name: str) -> str:
    mapping = {
        "query_country_snapshot": "market KPIs, brand/model rankings, powertrain mix",
        "query_cross_country": "cross-country market KPIs, powertrain mix, and top-model comparison",
        "query_time_series": "monthly or yearly market trend points for the requested scope",
        "build_market_chart": "chart specs and trend series",
        "query_msrp_pricing": "current MSRP records and price corridor",
        "query_leasing_offers": "monthly payment, term, mileage, residual value, and total contract cost",
        "compare_vehicle_variants": "trim, feature, battery, range, and configuration deltas",
        "search_market_news": "news, policy, VOC, and public freshness signals",
        "read_web_page": "static public page text and source URL",
        "browser_snapshot": "rendered public page snapshot and optional screenshot",
        "pageindex_search_documents": "document sections and policy/report citations",
        "minirag_query_graph": "multi-hop entity relationships and supporting chunks",
    }
    return mapping.get(tool_name, "tool-specific evidence")


def _answer_mode(intent: str, text: str) -> str:
    if intent == "report_generation":
        return "report"
    if _contains_any(text, ["chart", "plot", "graph", "图表", "画图"]):
        return "chart"
    if intent in {"market_overview", "pricing_analysis", "competitor_compare", "configuration_analysis", "news_policy_search"}:
        return "analysis"
    return "short"


def _follow_up_types(intent: str) -> list[str]:
    if intent == "pricing_analysis":
        return ["compare", "data_check", "action", "report"]
    if intent == "competitor_compare":
        return ["compare", "action", "report", "why"]
    if intent == "configuration_analysis":
        return ["compare", "action", "data_check", "report"]
    if intent == "news_policy_search":
        return ["why", "compare", "action", "external_search"]
    if intent == "voc_analysis":
        return ["why", "external_search", "action", "report"]
    if intent == "report_generation":
        return ["report", "action", "data_check", "compare"]
    return ["drilldown", "compare", "action", "report"]


def _extract_model_candidates(question: str) -> list[str]:
    text = str(question or "")
    patterns = [
        (
            r"\b[A-Z][A-Za-z0-9.-]{0,20}(?:\s+[A-Z][A-Za-z0-9.-]{0,20}){0,2}"
            r"\s+(?:HEV|PHEV|BEV|EV|SUV|Recharge|E-Tech|e-tron)\b",
            0,
        ),
        (rf"\b(?:{_KNOWN_MODEL_NAME_PATTERN})\b", re.IGNORECASE),
    ]
    candidates: list[str] = []
    for pattern, flags in patterns:
        for match in re.findall(pattern, text, flags=flags):
            value = _strip_country_prefix_from_model_candidate(str(match).strip())
            if value and value.lower() not in {item.lower() for item in candidates}:
                candidates.append(value)
    return _dedupe_model_candidates_by_specificity(candidates)[:6]


def _strip_country_prefix_from_model_candidate(value: str) -> str:
    text = str(value or "").strip()
    for alias, _country in sorted(COUNTRY_MENTION_ALIASES, key=lambda item: len(item[0]), reverse=True):
        match = re.match(rf"^{re.escape(alias)}\s+", text, flags=re.IGNORECASE)
        if match:
            return text[match.end():].strip()
    return text


def _dedupe_model_candidates_by_specificity(candidates: list[str]) -> list[str]:
    cleaned = _dedupe([str(item or "").strip() for item in candidates if str(item or "").strip()])
    if len(cleaned) <= 1:
        return cleaned
    keys = {item: _model_seed_key(item) for item in cleaned}
    result: list[str] = []
    for item in cleaned:
        key = keys.get(item, "")
        if key and any(key != other_key and key in other_key for other_key in keys.values()):
            continue
        result.append(item)
    return result or cleaned


def _extract_brand_candidates(question: str) -> list[str]:
    text = str(question or "")
    brands: list[str] = []
    for brand, patterns in _BRAND_PATTERNS:
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            brands.append(brand)
    return _dedupe(brands)[:6]


def _extract_feature_candidates(question: str) -> list[str]:
    text = str(question or "")
    features: list[str] = []
    for feature, patterns in _FEATURE_PATTERNS:
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            features.append(feature)
    return _dedupe(features)[:8]


def _extract_competitor_candidates(question: str) -> list[str]:
    models = _extract_model_candidates(question)
    if len(models) >= 2:
        return models[1:6]
    return []


def _pricing_method_competitors(country: str, question: str, *, models: list[str]) -> list[str]:
    method = get_active_pricing_method(
        country=country,
        model=", ".join(str(item) for item in models),
        question=question,
    )
    if not method:
        return []
    return [
        str(item or "").strip()
        for item in method.get("competitorPool", [])
        if str(item or "").strip()
    ][:6]


def _filter_competitors_for_models(competitors: list[str], models: list[str]) -> list[str]:
    model_keys = {_model_seed_key(item) for item in models if _model_seed_key(item)}
    result: list[str] = []
    for competitor in competitors:
        key = _model_seed_key(competitor)
        if not key:
            continue
        if any(_model_seed_matches(key, model_key) for model_key in model_keys):
            continue
        result.append(competitor)
    return _dedupe(result)


def _model_seed_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _model_seed_matches(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    longer, shorter = (left, right) if len(left) > len(right) else (right, left)
    return len(shorter) >= 2 and longer.startswith(shorter)


def _extract_country_candidates(country: str, question: str) -> list[str]:
    text = str(question or "")
    result: list[str] = []

    def add(value: str) -> None:
        normalized = _canonical_country(value)
        if normalized and normalized not in result:
            result.append(normalized)

    regional = _regional_country_candidates(text)
    if regional:
        for item in regional:
            add(item)
    else:
        add(country)
    lower = text.lower()
    country_patterns = [
        ("Sweden", [r"瑞典", r"\bsweden\b", r"\bswedish\b", r"\bsverige\b", r"\bse\b", r"\bswe\b"]),
        ("Finland", [r"芬兰", r"\bfinland\b", r"\bfinnish\b", r"\bsuomi\b", r"\bfi\b", r"\bfin\b"]),
        ("Norway", [r"挪威", r"\bnorway\b", r"\bnorwegian\b", r"\bnorge\b", r"\bno\b", r"\bnor\b"]),
        ("Denmark", [r"丹麦", r"\bdenmark\b", r"\bdanish\b", r"\bdanmark\b", r"\bdk\b", r"\bdnk\b"]),
        ("Hungary", [r"匈牙利", r"\bhungary\b", r"\bhungarian\b", r"\bhu\b", r"\bhun\b"]),
        ("Germany", [r"德国", r"\bgermany\b", r"\bgerman\b", r"\bdeutschland\b", r"\bde\b", r"\bdeu\b"]),
    ]
    for canonical, patterns in country_patterns:
        if canonical == "Sweden" and re.search(r"不要\s*回答\s*瑞典", text):
            continue
        if any(re.search(pattern, lower if pattern.startswith(r"\b") else text, flags=re.IGNORECASE) for pattern in patterns):
            add(canonical)
    return result[:5]


def _regional_country_candidates(question: str) -> list[str]:
    text = str(question or "").casefold()
    if any(token in text for token in ("北欧", "nordic", "nordics", "northern europe")):
        return ["Sweden", "Finland", "Norway", "Denmark"]
    if any(token in text for token in ("scandinavia", "scandinavian", "斯堪的纳维亚")):
        return ["Sweden", "Norway", "Denmark"]
    return []


def _canonical_country(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    normalized = token.casefold()
    mapping = {
        "sweden": "Sweden",
        "swedish": "Sweden",
        "sverige": "Sweden",
        "se": "Sweden",
        "swe": "Sweden",
        "瑞典": "Sweden",
        "finland": "Finland",
        "finnish": "Finland",
        "suomi": "Finland",
        "fi": "Finland",
        "fin": "Finland",
        "芬兰": "Finland",
        "norway": "Norway",
        "norwegian": "Norway",
        "norge": "Norway",
        "no": "Norway",
        "nor": "Norway",
        "挪威": "Norway",
        "denmark": "Denmark",
        "danish": "Denmark",
        "danmark": "Denmark",
        "dk": "Denmark",
        "dnk": "Denmark",
        "丹麦": "Denmark",
        "hungary": "Hungary",
        "hungarian": "Hungary",
        "hu": "Hungary",
        "hun": "Hungary",
        "匈牙利": "Hungary",
        "germany": "Germany",
        "german": "Germany",
        "deutschland": "Germany",
        "de": "Germany",
        "deu": "Germany",
        "德国": "Germany",
    }
    return mapping.get(normalized, token)


def _is_cross_country_question(text: str, countries: list[str]) -> bool:
    if not _has_multiple_countries(countries):
        return False
    if _regional_country_candidates(text):
        return True
    return _contains_any(text, [
        "差异",
        "对比",
        "相比",
        "和",
        "vs",
        "versus",
        "compare",
        "comparison",
        "between",
        "cross-country",
        "邻国",
        "两国",
    ])


def _allows_cross_country_tool(intent: str, text: str, countries: list[str]) -> bool:
    if _is_cross_country_question(text, countries):
        return True
    return (
        intent == "inventory_analysis"
        and _is_pi_market_split_question(text)
        and _has_multiple_countries(countries)
    )


def _configuration_needs_market_chart_context(text: str) -> bool:
    return _contains_any(
        text,
        [
            "80kwh",
            "80 kwh",
            "95kwh",
            "95 kwh",
            "800v",
            "800 v",
            "双电机",
            "4.7m",
            "4.7 m",
            "a0",
            "小型suv",
            "小型 suv",
            "冬季包",
            "冬季胎",
            "北欧",
            "市场",
            "company car",
            "公司车",
        ],
    )


def _has_multiple_countries(countries: list[str]) -> bool:
    return len([item for item in countries if str(item or "").strip()]) >= 2


def _is_pi_market_split_question(text: str) -> bool:
    has_pi = bool(re.search(r"\bpi\b", text))
    if not has_pi:
        return False
    has_market_split = _contains_any(text, [
        "se/fi",
        "se-fi",
        "瑞典和芬兰",
        "sweden and finland",
        "合并",
        "分市场",
        "车辆生成",
        "车辆分市场",
        "market overlay",
    ])
    return has_market_split


def _contains_any(text: str, needles: list[str]) -> bool:
    return any(needle.lower() in text for needle in needles)


def _contains_currency_signal(text: str) -> bool:
    return bool(re.search(r"\d+(?:[.,]\d+)?\s?(?:k|万)?\s?(?:eur|euro|€|sek|kr|欧元|瑞典克朗)", text))


def _needs_pricing_external_context(text: str) -> bool:
    return _contains_any(text, [
        "leasing", "lease", "company car", "fleet", "b2b", "tax", "subsidy", "policy",
        "incentive", "regulation", "benefit", "tco",
        "大客户", "公司车", "税", "补贴", "政策", "激励", "法规", "总拥有成本", "残值",
    ])


def _report_generation_needs_external_context(text: str) -> bool:
    return _contains_any(text, [
        "source",
        "sources",
        "citation",
        "citations",
        "research",
        "news",
        "policy",
        "subsidy",
        "tax",
        "latest",
        "search",
        "web",
        "tavily",
        "regulation",
        "incentive",
        "来源",
        "引用",
        "检索",
        "搜索",
        "联网",
        "新闻",
        "政策",
        "补贴",
        "税",
        "最新",
        "法规",
        "激励",
        "elbilspremien",
    ])


def _allows_pricing_source_repair(text: str) -> bool:
    return _needs_pricing_external_context(text) or _needs_price_corridor(text) or _contains_any(text, [
        "msrp",
        "official price",
        "current price",
        "price source",
        "competitor price",
        "list price",
        "官网价格",
        "官方价格",
        "当前价格",
        "竞品价格",
        "价格来源",
        "售价",
        "月供",
    ])


def _allows_competitor_price_source_repair(text: str) -> bool:
    return _needs_competitor_gap_context(text) or _contains_any(text, [
        "price",
        "pricing",
        "msrp",
        "售价",
        "价格",
        "定价",
        "对标",
        "定位差异",
        "能打",
    ])


def _is_fleet_leasing_powertrain_question(text: str) -> bool:
    return _contains_any(text, ["phev", "bev", "hev"]) and _contains_any(text, [
        "company car", "fleet", "b2b", "tco", "tax", "benefit",
        "大客户", "公司车", "税", "总持有成本",
    ])


def _pricing_needs_leasing_evidence(text: str) -> bool:
    return _contains_any(text, [
        "leasing",
        "lease",
        "monthly payment",
        "residual value",
        "company car",
        "fleet",
        "tco",
        "月供",
        "残值",
        "公司车",
        "大客户",
        "总持有成本",
    ]) or bool(re.search(r"(?<![a-z0-9])rv(?![a-z0-9])", text))


def _needs_policy_pricing_context(text: str) -> bool:
    policy_signal = _contains_any(text, [
        "subsidy",
        "incentive",
        "bonus",
        "cap",
        "price cap",
        "threshold",
        "eligibility",
        "补贴",
        "激励",
        "价格上限",
        "上限",
        "门槛",
        "资格",
        "适用",
    ])
    pricing_signal = _contains_any(text, [
        "price",
        "pricing",
        "msrp",
        "定价",
        "价格",
        "售价",
        "价位",
        "价格带",
    ]) or _contains_currency_signal(text)
    return policy_signal and pricing_signal


def _needs_policy_source_context(text: str) -> bool:
    return _contains_any(text, [
        "news",
        "policy",
        "subsidy",
        "tax",
        "incentive",
        "bonus",
        "latest",
        "research",
        "source",
        "sources",
        "citation",
        "citations",
        "elbilspremien",
        "benefit",
        "company car",
        "co₂",
        "co2",
        "emission",
        "新闻",
        "政策",
        "补贴",
        "税",
        "最新",
        "研究",
        "来源",
        "引用",
        "出处",
        "受影响",
        "价格上限",
        "门槛",
        "资格",
    ])


def _needs_price_corridor(text: str) -> bool:
    return _contains_any(text, [
        "reasonable", "unreasonable", "cheap", "expensive", "corridor", "price band",
        "便宜", "贵", "合理", "价差", "价格带", "价格走廊", "同价带",
    ]) or _contains_currency_signal(text)


def _pricing_needs_competitor_context(text: str) -> bool:
    return _contains_any(text, [
        "competitor",
        "competitors",
        "rival",
        "rivals",
        "benchmark",
        "vs",
        "versus",
        "竞品",
        "对标",
        "竞品格局",
        "核心竞品",
        "同价带",
    ])


def _pricing_needs_configuration_context(text: str) -> bool:
    return _contains_any(text, [
        "equipment",
        "configuration",
        "feature",
        "features",
        "trim",
        "variant",
        "spec",
        "配置",
        "配置差异",
        "配置价值",
        "版型",
        "规格",
        "版本",
        "高配",
        "低配",
    ])


def _pricing_needs_market_context(text: str) -> bool:
    return _contains_any(text, [
        "market",
        "share",
        "volume",
        "segment",
        "powertrain",
        "chart",
        "市场",
        "销量",
        "份额",
        "细分",
        "动力",
        "图表",
    ])


def _dedupe(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _first_url(question: str) -> str:
    match = re.search(r"https?://[^\s)）]+", str(question or ""))
    return match.group(0) if match else ""
