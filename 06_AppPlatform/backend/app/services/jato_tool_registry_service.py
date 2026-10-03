from __future__ import annotations

from typing import Any, Literal, TypedDict


CostLevel = Literal["low", "medium", "high"]
LatencyLevel = Literal["fast", "normal", "slow"]


class ToolCard(TypedDict, total=False):
    name: str
    description: str
    intentTags: list[str]
    inputSchema: dict[str, Any]
    outputSchema: dict[str, Any]
    costLevel: CostLevel
    latencyLevel: LatencyLevel
    requiresNetwork: bool
    requiresTenantData: bool
    cacheTtlSeconds: int
    examples: list[str]


_TOOL_CARDS: dict[str, ToolCard] = {
    "query_country_snapshot": {
        "name": "query_country_snapshot",
        "description": "Return governed JATO market KPIs, rankings, powertrain mix, and country snapshots.",
        "intentTags": ["market_overview", "drilldown", "data_check"],
        "inputSchema": {"country": "string", "question": "string?"},
        "outputSchema": {"kpis": "object", "topBrands": "array", "topModels": "array", "powertrainMix": "array"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["瑞典 BEV 市场概览", "Rank top 10 BEV models in Germany"],
    },
    "build_market_chart": {
        "name": "build_market_chart",
        "description": "Build chart-ready market trend context and Plotly chart specs from JATO data.",
        "intentTags": ["market_overview", "drilldown", "report"],
        "inputSchema": {"country": "string", "question": "string"},
        "outputSchema": {"chartSpecs": "object", "contextSnapshot": "object"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["Draw a 2025 BEV trend chart for Sweden", "画瑞典 HEV 月度趋势图"],
    },
    "query_cross_country": {
        "name": "query_cross_country",
        "description": "Compare market KPIs, powertrain mix, and top models across multiple countries.",
        "intentTags": ["market_overview", "compare", "why", "data_check"],
        "inputSchema": {"countries": "string", "question": "string"},
        "outputSchema": {"countries": "array", "comparison": "object"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["瑞典和芬兰销量差异为什么大", "Compare Sweden vs Finland BEV adoption"],
    },
    "query_segment_breakdown": {
        "name": "query_segment_breakdown",
        "description": "Return JATO cross-tab structure for segment, powertrain, drive type, and registration channel analysis.",
        "intentTags": ["market_overview", "drilldown", "data_check", "competitor_compare"],
        "inputSchema": {"country": "string", "question": "string?", "segment": "string?", "powertrain": "string?"},
        "outputSchema": {"driveByFuel": "array", "driveBySegment": "array", "segmentByFuel": "array", "registrationByFuel": "array", "registrationBySegment": "array"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["瑞典 HEV 市场为什么适合 J7", "Break Sweden SUV A0/A by fuel and drive type"],
    },
    "query_with_filters": {
        "name": "query_with_filters",
        "description": "Return filtered JATO market rows using country, powertrain, segment, brand, model, and year filters.",
        "intentTags": ["market_overview", "inventory_analysis", "drilldown", "data_check"],
        "inputSchema": {"country": "string", "question": "string?", "powertrain": "string?", "segment": "string?", "brand": "string?", "model": "string?"},
        "outputSchema": {"results": "object", "appliedFilters": "object"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["Filter Sweden market by HEV", "查询瑞典 SUV A0 HEV 主销车型"],
    },
    "query_msrp_pricing": {
        "name": "query_msrp_pricing",
        "description": "Return MSRP or current price records for country/model/version pricing analysis.",
        "intentTags": ["pricing_analysis", "competitor_compare", "data_check"],
        "inputSchema": {"country": "string", "question": "string?", "model": "string?", "models": "array?", "competitors": "array?"},
        "outputSchema": {"items": "array", "priceStats": "object?"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["查询瑞典 J7 HEV 当前价格", "Compare EX30 vs Zeekr X MSRP"],
    },
    "query_leasing_offers": {
        "name": "query_leasing_offers",
        "description": "Return governed lease offers, monthly payments, contract terms, residual values, and total contract cost from the leasing store.",
        "intentTags": ["pricing_analysis", "competitor_compare", "data_check", "action"],
        "inputSchema": {"country": "string", "model": "string?", "models": "array?", "brand": "string?", "lease_type": "string?", "status": "string?"},
        "outputSchema": {"items": "array", "leasingStats": "object?", "coverageDiagnostics": "object?"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 3600,
        "examples": ["查询瑞典车型的 36 个月月供和 RV", "Compare fleet leasing cost and residual value"],
    },
    "compare_vehicle_variants": {
        "name": "compare_vehicle_variants",
        "description": "Compare vehicle variants, trims, feature differences, and specifications.",
        "intentTags": ["configuration_analysis", "competitor_compare", "action"],
        "inputSchema": {"country": "string", "question": "string", "models": "array?"},
        "outputSchema": {"subjects": "array", "diffFeatures": "array"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["对比 J7 HEV 和 RAV4 配置差异", "Compare Q4 e-tron vs EQB equipment"],
    },
    "query_time_series": {
        "name": "query_time_series",
        "description": "Return governed monthly or yearly JATO trend series for the requested market filters.",
        "intentTags": ["market_overview", "drilldown", "report_generation"],
        "inputSchema": {"country": "string", "question": "string?", "powertrain": "string?", "segment": "string?", "year": "number?"},
        "outputSchema": {"series": "array", "filters": "object"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["Show Hungary HEV monthly trend", "画芬兰 BEV 年度趋势"],
    },
    "query_price_positioning": {
        "name": "query_price_positioning",
        "description": "Return governed MSRP distribution and competitive price-positioning statistics for a market scope.",
        "intentTags": ["pricing_analysis", "competitor_compare", "data_check"],
        "inputSchema": {"country": "string", "question": "string?", "model": "string?", "powertrain": "string?", "segment": "string?"},
        "outputSchema": {"priceStats": "object", "items": "array?"},
        "costLevel": "low",
        "latencyLevel": "fast",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["Show Sweden SUV A HEV price corridor", "判断目标价位于市场价格带哪个位置"],
    },
    "query_competitive_landscape": {
        "name": "query_competitive_landscape",
        "description": "Return a governed competitive set with sales, pricing, and configuration context for one target model.",
        "intentTags": ["competitor_compare", "pricing_analysis", "configuration_analysis"],
        "inputSchema": {"country": "string", "model": "string", "question": "string?"},
        "outputSchema": {"competitors": "array", "pricing": "object?", "features": "object?"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": False,
        "requiresTenantData": True,
        "cacheTtlSeconds": 86400,
        "examples": ["Find competitors for a new C-SUV", "比较目标车型的销量、价格和配置位置"],
    },
    "search_market_news": {
        "name": "search_market_news",
        "description": "Search news, policy, VOC, and public freshness signals with governed fallback.",
        "intentTags": ["news_policy_search", "voc_analysis", "external_search", "why"],
        "inputSchema": {"country": "string", "question": "string", "limit": "number?"},
        "outputSchema": {"items": "array"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": True,
        "requiresTenantData": False,
        "cacheTtlSeconds": 3600,
        "examples": ["查询最近 30 天瑞典 BEV 政策新闻", "What are Swedish consumers saying about EX30 range anxiety?"],
    },
    "read_web_page": {
        "name": "read_web_page",
        "description": "Read static public HTTP/HTTPS page text with SSRF safeguards.",
        "intentTags": ["external_search", "news_policy_search", "report_generation"],
        "inputSchema": {"url": "string", "question": "string?"},
        "outputSchema": {"title": "string", "textPreview": "string", "links": "array"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": True,
        "requiresTenantData": False,
        "cacheTtlSeconds": 3600,
        "examples": ["Summarize this public policy page", "读取竞品官网配置页面"],
    },
    "browser_snapshot": {
        "name": "browser_snapshot",
        "description": "Capture read-only rendered public page snapshot when browser runtime is available.",
        "intentTags": ["external_search", "report_generation"],
        "inputSchema": {"url": "string", "question": "string?", "capture_screenshot": "boolean?"},
        "outputSchema": {"title": "string", "textPreview": "string", "screenshot": "object?"},
        "costLevel": "high",
        "latencyLevel": "slow",
        "requiresNetwork": True,
        "requiresTenantData": False,
        "cacheTtlSeconds": 1800,
        "examples": ["给我这个竞品官网页面的浏览器快照", "Capture a rendered policy page summary"],
    },
    "pageindex_search_documents": {
        "name": "pageindex_search_documents",
        "description": "Search long documents, reports, manuals, and policies through PageIndex fallback path.",
        "intentTags": ["news_policy_search", "report_generation", "external_search"],
        "inputSchema": {"country": "string", "question": "string"},
        "outputSchema": {"sections": "array", "summary": "string"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": True,
        "requiresTenantData": False,
        "cacheTtlSeconds": 3600,
        "examples": ["Find PHEV eligibility thresholds in a policy PDF", "检索产品手册中的电池质保条款"],
    },
    "minirag_query_graph": {
        "name": "minirag_query_graph",
        "description": "Query multi-hop relationships across policies, models, brands, and market factors.",
        "intentTags": ["why", "competitor_compare", "news_policy_search"],
        "inputSchema": {"country": "string", "question": "string"},
        "outputSchema": {"paths": "array", "entities": "array", "supportingChunks": "array"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": True,
        "requiresTenantData": False,
        "cacheTtlSeconds": 3600,
        "examples": ["How do Swedish PHEV subsidy changes affect XC60 PHEV positioning?"],
    },
    "analyze_model_performance": {
        "name": "analyze_model_performance",
        "description": "Cross-reference sales, pricing, variants, and news for one model performance question.",
        "intentTags": ["market_overview", "why", "competitor_compare"],
        "inputSchema": {"country": "string", "question": "string"},
        "outputSchema": {"findings": "object", "items": "array?"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": True,
        "requiresTenantData": True,
        "cacheTtlSeconds": 3600,
        "examples": ["为什么 EX40 在瑞典卖得好", "Why does Model Y outperform competitors?"],
    },
    "compare_competitive_set": {
        "name": "compare_competitive_set",
        "description": "Compare a model against competitors across sales, pricing, and features.",
        "intentTags": ["competitor_compare", "pricing_analysis", "configuration_analysis"],
        "inputSchema": {"country": "string", "question": "string", "model": "string?", "models": "array?", "competitors": "array?"},
        "outputSchema": {"findings": "object", "items": "array?"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": True,
        "requiresTenantData": True,
        "cacheTtlSeconds": 3600,
        "examples": ["对比 J7 HEV 与 RAV4、Sportage", "Compare EX30 vs EV3 and Zeekr X"],
    },
    "analyze_market_dynamics": {
        "name": "analyze_market_dynamics",
        "description": "Analyze market changes with trend, news, policy, and pricing cross-references.",
        "intentTags": ["why", "news_policy_search", "market_overview"],
        "inputSchema": {"country": "string", "question": "string"},
        "outputSchema": {"dynamics": "object", "items": "array?"},
        "costLevel": "medium",
        "latencyLevel": "normal",
        "requiresNetwork": True,
        "requiresTenantData": True,
        "cacheTtlSeconds": 3600,
        "examples": ["解释 PHEV 份额变化背后的政策和供给因素"],
    },
}


def list_tool_cards() -> list[ToolCard]:
    return [dict(card) for card in _TOOL_CARDS.values()]


def get_tool_card(name: str) -> ToolCard | None:
    card = _TOOL_CARDS.get(str(name or "").strip())
    return dict(card) if card else None


def allowed_tools_for_intent(intent: str) -> list[str]:
    normalized = str(intent or "").strip()
    result = [
        name
        for name, card in _TOOL_CARDS.items()
        if normalized in card.get("intentTags", [])
    ]
    if result:
        return result
    if normalized == "general_qa":
        return ["query_country_snapshot"]
    return ["query_country_snapshot", "build_market_chart"]


def filter_tool_descriptors_for_allowed(
    descriptors: list[dict[str, Any]],
    allowed_tools: list[str],
) -> list[dict[str, Any]]:
    allowed = {str(tool) for tool in allowed_tools}
    filtered = [
        descriptor
        for descriptor in descriptors
        if str(descriptor.get("name") or "") in allowed
    ]
    return filtered or descriptors
