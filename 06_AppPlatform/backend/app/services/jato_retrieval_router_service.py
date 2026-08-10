from __future__ import annotations

from typing import Any

# ── Retrieval path definitions ──

RETRIEVAL_PATHS = {
    "structured_mcp": {
        "label": "Structured MCP Tools",
        "description": "Parquet, PostgreSQL, MSRP, vehicle config, chart data — the factual data backbone.",
        "suitableFor": [
            "numeric queries",
            "rankings",
            "pricing",
            "vehicle config comparisons",
            "chart data",
            "market KPIs",
        ],
        "unsuitableFor": [
            "long natural-language documents",
            "multi-hop entity reasoning",
            "external web freshness",
        ],
        "primaryTools": [
            "query_country_snapshot",
            "build_market_chart",
            "query_msrp_pricing",
            "compare_vehicle_variants",
        ],
        "priority": 1,
    },
    "hybrid_rag": {
        "label": "Hybrid RAG",
        "description": "News, VOC, forum fragments — multi-source short-text recall.",
        "suitableFor": [
            "market news",
            "policy announcements",
            "VOC forum discussions",
            "consumer sentiment fragments",
        ],
        "unsuitableFor": [
            "structured numeric queries",
            "exact page-level citations",
            "multi-hop entity reasoning",
        ],
        "primaryTools": [
            "search_market_news",
        ],
        "priority": 2,
    },
    "web_search": {
        "label": "Web Search",
        "description": "External freshness fallback for information not yet in JATO data.",
        "suitableFor": [
            "latest external facts",
            "breaking policy changes",
            "public web pages not in JATO",
        ],
        "unsuitableFor": [
            "JATO-internal data already indexed",
            "vehicle configurations",
            "pricing records",
        ],
        "primaryTools": [
            "read_web_page",
            "browser_snapshot",
            "search_market_news",  # reuses Tavily-first web search
        ],
        "priority": 3,
    },
    "pageindex": {
        "label": "PageIndex Document Search",
        "description": "Long PDFs, policy reports, product manuals — hierarchical tree index retrieval.",
        "suitableFor": [
            "policy PDFs with numbered clauses",
            "competitor reports",
            "product manuals",
            "long markdown PRDs",
        ],
        "unsuitableFor": [
            "high-frequency fragment text",
            "structured numeric queries",
            "database-level data",
        ],
        "primaryTools": [
            "pageindex_search_documents",
            "pageindex_get_section",
            "pageindex_list_documents",
        ],
        "status": "active",
        "fallbackPath": "hybrid_rag",
        "priority": 4,
    },
    "minirag": {
        "label": "MiniRAG Graph Retrieval",
        "description": "Multi-hop entity relationships — policies, brands, models, consumer concerns.",
        "suitableFor": [
            "country-policy-vehicle segment chains",
            "brand-model-powertrain-price band relationships",
            "consumer concern → model mapping",
        ],
        "unsuitableFor": [
            "single-document page citations",
            "pure numeric queries",
            "database-level aggregation",
        ],
        "primaryTools": [
            "minirag_query_graph",
            "minirag_explain_entity",
            "minirag_update_corpus",
        ],
        "status": "active",
        "fallbackPath": "hybrid_rag",
        "priority": 5,
    },
}


def classify_retrieval_intent(question: str) -> dict[str, Any]:
    """Classify a user question into one or more retrieval paths with reasoning."""
    text = str(question or "").lower().strip()
    results: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    # ── structured_mcp checks ──
    structured_signals: list[str] = []
    if _contains_any(
        text,
        [
            "chart", "plot", "graph", "trend", "走势", "趋势", "画图", "作图", "图表",
            "share", "份额", "volume", "销量", "ranking", "排名", "top", "kpi",
        ],
    ):
        structured_signals.append("chart_or_kpi")
    if _contains_any(
        text,
        [
            "msrp", "price", "pricing", "价格", "定价", "售价", "报价",
            "cost", "residual", "rv", "tco",
        ],
    ):
        structured_signals.append("pricing")
    if _contains_any(
        text,
        [
            "variant", "trim", "feature", "configuration", "config",
            "配置", "版型", "差异", "spec", "规格", "发动机", "变速箱",
            "battery", "续航",
        ],
    ) or (_contains_any(text, ["range"]) and not _contains_any(text, ["range anxiety", "续航焦虑", "anxiety"])):
        structured_signals.append("variant_or_feature")
    if _contains_any(
        text,
        [
            "bev", "phev", "hev", "icev", "mhev", "电动", "混动", "燃油",
            "powertrain", "动力", "segment", "细分", "brand", "品牌",
            "model", "车型", "market", "市场",
        ],
    ) and not _contains_any(text, ["policy", "政策", "regulation", "法规"]):
        structured_signals.append("market_structure")

    if structured_signals:
        results.append({
            "path": "structured_mcp",
            "confidence": "high" if len(structured_signals) >= 2 else "medium",
            "signals": structured_signals,
            "reason": f"Structured data indicators: {', '.join(structured_signals)}",
        })
        seen_paths.add("structured_mcp")

    # ── hybrid_rag checks ──
    rag_signals: list[str] = []
    if _contains_any(
        text,
        [
            "news", "新闻", "article", "报道", "headline", "headlines",
            "digest", "update", "recent", "最新", "latest",
        ],
    ):
        rag_signals.append("news_or_recent")
    if _contains_any(
        text,
        [
            "voc", "论坛", "forum", "review", "评价", "口碑", "complaint",
            "投诉", "consumer", "消费者", "owner", "车主", "sentiment",
            "续航焦虑", "range anxiety",
        ],
    ):
        rag_signals.append("voc_or_consumer")
    if _contains_any(
        text,
        [
            "policy", "政策", "subsidy", "补贴", "regulation", "法规",
            "tax", "税", "incentive", "激励", "mandate", "elbilspremien",
            "benefit", "company car", "co₂", "co2", "emission", "排放",
            "税率", "阶梯", "价格上限",
        ],
    ):
        rag_signals.append("policy_or_regulation")

    if rag_signals:
        results.append({
            "path": "hybrid_rag",
            "confidence": "high" if len(rag_signals) >= 2 else "medium",
            "signals": rag_signals,
            "reason": f"News/VOC/Policy indicators: {', '.join(rag_signals)}",
        })
        seen_paths.add("hybrid_rag")

    # ── web_search checks: explicit external / latest requests ──
    explicit_url = "http://" in text or "https://" in text
    if explicit_url or _contains_any(
        text,
        [
            "web search", "搜索网页", "search the web", "google", "latest on",
            "what is the latest", "current news about", "find online",
            "research", "sources", "citation", "citations", "tavily",
            "网上", "互联网", "外部", "联网", "来源", "引用", "检索",
        ],
    ) or (_contains_any(text, ["latest", "最新", "breaking"]) and not rag_signals):
        results.append({
            "path": "web_search",
            "confidence": "high" if explicit_url else "medium",
            "signals": ["explicit_url"] if explicit_url else ["explicit_web_request", "research_request"],
            "reason": "Explicit URL read request" if explicit_url else "Explicit or implied external web freshness request",
        })
        seen_paths.add("web_search")

    # ── pageindex checks: long document / PDF / clause references ──
    pageindex_signals: list[str] = []
    if _contains_any(
        text,
        [
            "pdf", "document", "文档", "report", "报告", "manual", "手册",
            "chapter", "章节", "clause", "条款", "article", "page", "页",
            "paragraph", "段落", "appendix", "附录",
        ],
    ):
        pageindex_signals.append("document_reference")
    if _contains_any(text, ["第几条", "第几款", "第几页", "section", "clause"]):
        pageindex_signals.append("clause_lookup")

    # Boost pageindex to high confidence when explicit doc/clause references found
    pageindex_confidence = "high" if pageindex_signals else "medium"
    if pageindex_signals:
        results.append({
            "path": "pageindex",
            "confidence": pageindex_confidence,
            "signals": pageindex_signals,
            "reason": f"Document/page reference indicators: {', '.join(pageindex_signals)}",
            "status": "active",
            "fallbackPath": "hybrid_rag",
        })
        seen_paths.add("pageindex")

    # ── minirag checks: multi-hop entity relationships ──
    minirag_signals: list[str] = []
    entity_count = _count_entities(text)
    if entity_count >= 3:
        minirag_signals.append(f"multi_entity_{entity_count}")
    if _contains_any(
        text,
        [
            "relationship", "关系", "impact", "影响", "affect", "indirect",
            "间接", "chain", "链", "connect", "关联", "link between",
            "how does", "怎么影响", "how will", "drive", "cause", "导致",
        ],
    ):
        minirag_signals.append("relationship_query")
    if _contains_any(
        text,
        [
            "opportunity", "机会", "threat", "威胁", "positioning",
            "定位", "competitive", "竞争",
        ],
    ) and entity_count >= 2:
        minirag_signals.append("competitive_analysis")

    # Boost minirag confidence when strong multi-hop/entity signals present
    minirag_confidence = "high" if (len(minirag_signals) >= 2 or entity_count >= 4) else ("medium" if minirag_signals else "low")
    if minirag_signals and entity_count >= 2:
        results.append({
            "path": "minirag",
            "confidence": minirag_confidence,
            "signals": minirag_signals,
            "reason": f"Multi-hop entity relationship indicators: {', '.join(minirag_signals)}",
            "status": "active",
            "fallbackPath": "hybrid_rag" if "hybrid_rag" in seen_paths else "structured_mcp",
        })
        seen_paths.add("minirag")

    # ── always include structured_mcp as fallback when nothing matches ──
    if not results:
        results.append({
            "path": "structured_mcp",
            "confidence": "low",
            "signals": ["default_fallback"],
            "reason": "No strong signal detected; defaulting to structured MCP for governed data.",
        })
        seen_paths.add("structured_mcp")

    # ── determine primary / secondary split ──
    sorted_results = sorted(
        results,
        key=lambda r: (
            0 if r["confidence"] == "high" else 1 if r["confidence"] == "medium" else 2,
            RETRIEVAL_PATHS.get(r["path"], {}).get("priority", 99),
        ),
    )
    primary = sorted_results[0]
    secondary = sorted_results[1:] if len(sorted_results) > 1 else []

    return {
        "primary": primary,
        "secondary": [s["path"] for s in secondary],
        "allPaths": [r["path"] for r in sorted_results],
        "decisions": sorted_results,
        "question": question,
    }


def build_retrieval_tool_plan(
    classification: dict[str, Any],
    country: str,
    question: str,
) -> dict[str, Any]:
    """Build an ordered tool execution plan from retrieval classification."""
    primary = classification["primary"]
    decisions = classification.get("decisions", [])
    tool_plan: list[dict[str, Any]] = []
    step = 0

    for decision in decisions:
        path = decision["path"]
        path_def = RETRIEVAL_PATHS.get(path, {})
        tools = path_def.get("primaryTools", [])
        status = decision.get("status", path_def.get("status", "active"))

        for tool in tools:
            step += 1
            plan_step: dict[str, Any] = {
                "step": step,
                "tool": tool,
                "path": path,
                "pathLabel": path_def.get("label", path),
                "reason": decision["reason"],
                "confidence": decision["confidence"],
                "status": status,
                "arguments": {
                    "country": country,
                    "question": question,
                },
            }
            if status == "planned":
                fallback = decision.get("fallbackPath") or path_def.get("fallbackPath")
                plan_step["fallback"] = fallback
            tool_plan.append(plan_step)

    return {
        "primaryPath": primary["path"],
        "primaryLabel": RETRIEVAL_PATHS.get(primary["path"], {}).get("label", primary["path"]),
        "secondaryPaths": classification.get("secondary", []),
        "allPaths": classification.get("allPaths", []),
        "steps": tool_plan,
        "totalSteps": len(tool_plan),
        "activeSteps": len([s for s in tool_plan if s.get("status", "active") == "active"]),
        "plannedSteps": len([s for s in tool_plan if s.get("status") == "planned"]),
    }


def merge_evidence_pack(
    results_by_path: dict[str, dict[str, Any]],
    classification: dict[str, Any],
) -> dict[str, Any]:
    """Merge evidence items from multiple retrieval paths into a unified evidence pack."""
    items: list[dict[str, Any]] = []
    all_limitations: list[str] = []
    sources: set[str] = set()
    truncated = False
    paths_contributed: list[str] = []

    for path, result in results_by_path.items():
        if not isinstance(result, dict):
            continue
        path_def = RETRIEVAL_PATHS.get(path, {})
        metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
        data = result.get("data") if isinstance(result.get("data"), dict) else {}

        source = str(metadata.get("source") or path)
        sources.add(source)

        item = {
            "path": path,
            "pathLabel": path_def.get("label", path),
            "tool": result.get("tool"),
            "source": source,
            "truncated": bool(metadata.get("truncated")),
            "limitations": metadata.get("limitations") if isinstance(metadata.get("limitations"), list) else [],
            "itemCount": _count_data_items(data),
        }
        items.append(item)
        paths_contributed.append(path)
        truncated = truncated or bool(metadata.get("truncated"))
        all_limitations.extend(item["limitations"])

    return {
        "items": items,
        "sourceCount": len(sources),
        "sources": sorted(sources),
        "pathsContributed": paths_contributed,
        "totalPaths": len(results_by_path),
        "truncated": truncated,
        "limitations": all_limitations[:20],
        "classification": {
            "primaryPath": classification.get("primary", {}).get("path"),
            "allPaths": classification.get("allPaths", []),
        },
    }


# ── helpers ──


def _contains_any(text: str, needles: list[str]) -> bool:
    return any(needle in text for needle in needles)


def _count_entities(text: str) -> int:
    """Naive entity count based on automotive domain keywords."""
    entity_markers = [
        "sweden", "germany", "norway", "denmark", "finland", "netherlands",
        "france", "uk", "italy", "spain", "china", "japan", "korea",
        "bev", "phev", "hev", "icev", "mhev",
        "volvo", "toyota", "volkswagen", "bmw", "mercedes", "audi",
        "tesla", "ford", "hyundai", "kia", "skoda", "seat",
        "xc60", "xc90", "xc40", "j7", "kodiaq", "tiguan",
        "瑞典", "德国", "挪威",
        "补贴", "政策", "法规", "elbilspremien", "物料", "物料号", "bom",
    ]
    count = 0
    text_lower = text.lower()
    for marker in entity_markers:
        if marker in text_lower:
            count += 1
    return count


def _count_data_items(data: dict[str, Any]) -> int:
    items = data.get("items")
    if isinstance(items, list):
        return len(items)
    # count non-empty top-level keys as rough item estimate
    return len([v for v in data.values() if v not in (None, [], {})])
