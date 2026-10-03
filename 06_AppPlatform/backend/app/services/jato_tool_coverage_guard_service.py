from __future__ import annotations

from typing import Any


_REQUIRED_TOOL_ALIASES: dict[str, set[str]] = {
    "query_country_snapshot": {
        "query_country_snapshot",
        "query_cross_country",
        "build_market_chart",
        "analyze_market_dynamics",
        "analyze_model_performance",
        "query_with_filters",
    },
    "query_cross_country": {
        "query_cross_country",
    },
    "search_market_news": {
        "search_market_news",
        "pageindex_search_documents",
        "read_web_page",
        "browser_snapshot",
    },
    "pageindex_search_documents": {
        "pageindex_search_documents",
        "search_market_news",
        "read_web_page",
    },
    "build_market_chart": {
        "build_market_chart",
    },
    "query_msrp_pricing": {
        "query_msrp_pricing",
    },
    "compare_competitive_set": {
        "compare_competitive_set",
        "analyze_model_performance",
    },
    "compare_vehicle_variants": {
        "compare_vehicle_variants",
    },
    "query_with_filters": {
        "query_with_filters",
    },
}


def missing_required_tools(
    evidence_plan: dict[str, Any],
    executed_tools: list[str],
    *,
    allowed_tools: list[str] | None = None,
) -> list[str]:
    """Return required tools that are not satisfied by executed tool names."""
    required_tools = _string_list(evidence_plan.get("requiredTools"))
    if not required_tools:
        return []
    allowed = set(_string_list(allowed_tools if allowed_tools is not None else evidence_plan.get("allowedTools")))
    executed = _string_list(executed_tools)
    required_set = set(required_tools)
    result: list[str] = []
    for tool_name in required_tools:
        if allowed and tool_name not in allowed:
            continue
        shadowed_aliases = required_set - {tool_name}
        if any(
            tool_satisfies_required(tool_name, executed_tool)
            and executed_tool not in shadowed_aliases
            for executed_tool in executed
        ):
            continue
        result.append(tool_name)
    return result


def tool_satisfies_required(required_tool: str, executed_tool: str) -> bool:
    required = str(required_tool or "").strip()
    executed = str(executed_tool or "").strip()
    if not required or not executed:
        return False
    if required == executed:
        return True
    aliases = _REQUIRED_TOOL_ALIASES.get(required, {required})
    return executed in aliases


def required_tool_args(
    evidence_plan: dict[str, Any],
    tool_name: str,
    *,
    country: str,
    question: str,
) -> dict[str, Any]:
    """Reuse the EvidencePlan tool input; fall back to safe country/question args."""
    for item in evidence_plan.get("toolPlan", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("toolName") or "").strip() != tool_name:
            continue
        input_value = item.get("input")
        if isinstance(input_value, dict):
            return _with_defaults(input_value, country=country, question=question)
    return _with_defaults({}, country=country, question=question)


def _with_defaults(value: dict[str, Any], *, country: str, question: str) -> dict[str, Any]:
    result = dict(value)
    result.setdefault("country", country)
    result.setdefault("question", question)
    return result


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]
