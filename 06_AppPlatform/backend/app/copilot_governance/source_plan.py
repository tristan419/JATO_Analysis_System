"""Source Planner — determines which data sources a question needs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

SourceLane = Literal[
    "structured_bi",
    "canonical_entity",
    "voc",
    "policy_tax",
    "news",
    "live_web",
]

ExecutionMode = Literal[
    "structured_only",
    "canonical_only",
    "evidence_only",
    "hybrid",
]


class SourcePlanItem(BaseModel):
    source_id: str
    source_lane: SourceLane
    required: bool = True
    reason: str = ""
    expected_output: str = ""
    fallback_source_id: str | None = None
    freshness_required: bool = False
    confidence_weight: float = Field(default=1.0, ge=0.0, le=1.0)


class SourcePlan(BaseModel):
    question: str = ""
    intent: str = ""
    execution_mode: ExecutionMode = "structured_only"
    items: list[SourcePlanItem] = Field(default_factory=list)
    answer_mode: str = "quick_answer"
    requires_sql_planner: bool = False
    requires_tool_planner: bool = False
    requires_evidence_retrieval: bool = False
    max_execution_steps: int = 4


# ── Rule-based planner ────────────────────────────────────────────

_SOURCE_RULES: list[tuple[list[str], str, str, str]] = [
    # (intent_keywords, source_id, source_lane, reason)
    (["brand-ranking", "segment-analysis", "trend-summary", "powertrain-mix",
      "nev-analysis", "origin-analysis", "general-summary", "metric",
      "销量", "排名", "份额", "趋势", "品牌", "车型", "动力", "细分", "市场"],
     "jato_sales_parquet", "structured_bi", "Structured sales data for aggregation and ranking."),
    (["positioning", "pricing", "msrp", "price", "precise",
      "价格", "定价", "msrp", "多少钱", "价位"],
     "current_price_postgres", "canonical_entity", "Reviewed MSRP and price corridor."),
    (["voc", "complaint", "feedback", "review", "user",
      "反馈", "抱怨", "投诉", "用户", "口碑", "论坛"],
     "voc_forum_artifacts", "voc", "Qualitative customer feedback and forum evidence."),
    (["policy", "tax", "malus", "subsidy", "co2", "carbon", "regulation", "law",
      "政策", "税", "补贴", "法规", "碳", "排放"],
     "country_profiles", "policy_tax", "Policy, tax, and regulatory context."),
    (["news", "latest", "recent", "update", "event",
      "新闻", "最新", "最近", "事件", "热点"],
     "news_digest", "news", "Recent news and market events."),
]


def plan_sources(intent: str, question: str = "") -> SourcePlan:
    from app.copilot_governance.intent import LEGACY_TO_GOVERNED_INTENT

    lowered = (question or "").lower()
    items: list[SourcePlanItem] = []
    seen: set[str] = set()

    governed_intent = LEGACY_TO_GOVERNED_INTENT.get(intent, intent)

    # Intent-based rules — add sources based on governed intent
    _INTENT_SOURCES: dict[str, list[tuple[str, str, str]]] = {
        "metric_query": [("jato_sales_parquet", "structured_bi", "Structured data for metrics.")],
        "distribution": [("jato_sales_parquet", "structured_bi", "Distribution analysis.")],
        "trend": [("jato_sales_parquet", "structured_bi", "Trend analysis.")],
        "comparison": [
            ("jato_sales_parquet", "structured_bi", "Market comparison data."),
            ("current_price_postgres", "canonical_entity", "Price comparison."),
        ],
        "pricing_strategy": [
            ("jato_sales_parquet", "structured_bi", "Market structure context."),
            ("current_price_postgres", "canonical_entity", "Current MSRP data."),
        ],
        "product_strategy": [
            ("jato_sales_parquet", "structured_bi", "Market structure context."),
            ("current_price_postgres", "canonical_entity", "Price corridor context."),
            ("country_profiles", "policy_tax", "Policy context."),
        ],
        "policy_tax": [
            ("country_profiles", "policy_tax", "Structured tax and subsidy rules."),
        ],
        "voc_insight": [
            ("voc_forum_artifacts", "voc", "Customer feedback evidence."),
        ],
        "news_intelligence": [
            ("news_digest", "news", "Recent news evidence."),
        ],
        "country_report": [
            ("jato_sales_parquet", "structured_bi", "Full market data."),
        ],
    }

    if governed_intent in _INTENT_SOURCES:
        for source_id, lane, reason in _INTENT_SOURCES[governed_intent]:
            if source_id not in seen:
                seen.add(source_id)
                items.append(SourcePlanItem(
                    source_id=source_id, source_lane=lane,
                    required=True, reason=reason,
                ))

    # Keyword-based rules — supplement with question-level keyword matching
    for keywords, source_id, lane, reason in _SOURCE_RULES:
        if any(kw in intent.lower() or kw in lowered for kw in keywords):
            if source_id not in seen:
                seen.add(source_id)
                items.append(SourcePlanItem(
                    source_id=source_id, source_lane=lane,
                    required=False, reason=f"Keyword match: {reason}",
                ))

    if not items:
        items.append(SourcePlanItem(
            source_id="jato_sales_parquet",
            source_lane="structured_bi",
            required=True,
            reason="Default: structured sales data for general questions.",
        ))

    lanes = {item.source_lane for item in items}
    if len(lanes) >= 2:
        execution_mode: ExecutionMode = "hybrid"
    elif "structured_bi" in lanes:
        execution_mode = "structured_only"
    elif "canonical_entity" in lanes:
        execution_mode = "canonical_only"
    else:
        execution_mode = "evidence_only"

    return SourcePlan(
        question=question,
        intent=governed_intent,
        execution_mode=execution_mode,
        items=items,
        requires_sql_planner="structured_bi" in lanes,
        requires_tool_planner="canonical_entity" in lanes,
        requires_evidence_retrieval=bool(lanes - {"structured_bi", "canonical_entity"}),
    )
