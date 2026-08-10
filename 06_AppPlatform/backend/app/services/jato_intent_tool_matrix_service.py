from __future__ import annotations

from typing import Any, TypedDict


class IntentToolRule(TypedDict):
    requiredTools: list[str]
    optionalTools: list[str]
    mustHaveEvidence: list[str]
    answerMustMention: list[str]


_INTENT_TOOL_MATRIX: dict[str, IntentToolRule] = {
    "pricing_analysis": {
        "requiredTools": ["query_msrp_pricing", "build_market_chart"],
        "optionalTools": ["compare_competitive_set", "query_price_positioning", "query_leasing_offers", "query_country_snapshot", "search_market_news"],
        "mustHaveEvidence": ["own_model_price", "market_context", "competitor_price_range"],
        "answerMustMention": ["MSRP", "market context", "competitor corridor", "pricing stance"],
    },
    "competitor_compare": {
        "requiredTools": ["compare_competitive_set"],
        "optionalTools": ["query_msrp_pricing", "compare_vehicle_variants", "query_country_snapshot"],
        "mustHaveEvidence": ["competitor_pool"],
        "answerMustMention": ["competitor pool", "price/value position", "configuration path"],
    },
    "market_overview": {
        "requiredTools": ["query_country_snapshot", "build_market_chart"],
        "optionalTools": ["query_segment_breakdown", "query_with_filters", "query_time_series", "query_cross_country", "analyze_market_dynamics", "analyze_model_performance", "search_market_news"],
        "mustHaveEvidence": ["market_kpis", "trend_or_mix", "segment_or_channel_structure"],
        "answerMustMention": ["market size", "share", "trend", "segment/channel structure"],
    },
    "configuration_analysis": {
        "requiredTools": ["compare_vehicle_variants"],
        "optionalTools": ["query_msrp_pricing", "query_country_snapshot", "query_competitive_landscape"],
        "mustHaveEvidence": ["trim", "powertrain", "key_features"],
        "answerMustMention": ["trim", "feature", "configuration delta"],
    },
    "inventory_analysis": {
        "requiredTools": ["query_country_snapshot", "query_with_filters"],
        "optionalTools": ["query_time_series"],
        "mustHaveEvidence": ["available_units", "market", "version"],
        "answerMustMention": ["inventory signal", "order signal", "market context"],
    },
    "voc_analysis": {
        "requiredTools": ["search_market_news", "query_country_snapshot"],
        "optionalTools": ["minirag_query_graph", "read_web_page"],
        "mustHaveEvidence": ["consumer_signal", "source_date", "market_context", "model_mapping"],
        "answerMustMention": ["consumer signal", "market proxy", "source"],
    },
    "news_policy_search": {
        "requiredTools": ["search_market_news", "query_country_snapshot", "build_market_chart"],
        "optionalTools": ["read_web_page", "browser_snapshot", "pageindex_search_documents", "minirag_query_graph"],
        "mustHaveEvidence": ["source_date", "policy_effect", "market_context"],
        "answerMustMention": ["source", "date", "business impact", "market context"],
    },
    "report_generation": {
        "requiredTools": ["build_market_chart"],
        "optionalTools": ["query_country_snapshot", "search_market_news"],
        "mustHaveEvidence": ["supporting_evidence"],
        "answerMustMention": ["conclusion", "evidence", "risk"],
    },
    "coding_debug": {
        "requiredTools": [],
        "optionalTools": ["query_country_snapshot"],
        "mustHaveEvidence": ["error_context"],
        "answerMustMention": ["cause", "fix", "verification"],
    },
    "general_qa": {
        "requiredTools": ["query_country_snapshot"],
        "optionalTools": [],
        "mustHaveEvidence": ["basic_context"],
        "answerMustMention": ["definition", "context"],
    },
}


def list_intent_tool_matrix() -> dict[str, IntentToolRule]:
    return {intent: _copy_rule(rule) for intent, rule in _INTENT_TOOL_MATRIX.items()}


def get_intent_tool_rule(intent: str) -> IntentToolRule:
    rule = _INTENT_TOOL_MATRIX.get(intent) or _INTENT_TOOL_MATRIX["general_qa"]
    return _copy_rule(rule)


def allowed_tools_for_intent_rule(intent: str, *, extra_tools: list[str] | None = None) -> list[str]:
    rule = get_intent_tool_rule(intent)
    return _dedupe([*rule["requiredTools"], *rule["optionalTools"], *(extra_tools or [])])


def merge_allowed_tools_with_rule(intent: str, candidate_tools: list[str]) -> list[str]:
    rule = get_intent_tool_rule(intent)
    matrix_tools = [*rule["requiredTools"], *rule["optionalTools"]]
    return _dedupe([*rule["requiredTools"], *candidate_tools, *matrix_tools])


def intent_rule_metadata(intent: str) -> dict[str, Any]:
    rule = get_intent_tool_rule(intent)
    return {
        "requiredTools": rule["requiredTools"],
        "optionalTools": rule["optionalTools"],
        "mustHaveEvidence": rule["mustHaveEvidence"],
        "answerMustMention": rule["answerMustMention"],
    }


def _copy_rule(rule: IntentToolRule) -> IntentToolRule:
    return {
        "requiredTools": list(rule["requiredTools"]),
        "optionalTools": list(rule["optionalTools"]),
        "mustHaveEvidence": list(rule["mustHaveEvidence"]),
        "answerMustMention": list(rule["answerMustMention"]),
    }


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
