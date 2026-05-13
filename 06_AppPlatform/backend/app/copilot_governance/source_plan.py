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


def plan_sources(intent: str, question: str = "") -> SourcePlan:
    """Map intent (from existing LLM-driven inference) to required data sources.

    No keyword matching — intent already tells us what the user wants.
    """
    from app.copilot_governance.intent import LEGACY_TO_GOVERNED_INTENT

    items: list[SourcePlanItem] = []
    seen: set[str] = set()
    governed_intent = LEGACY_TO_GOVERNED_INTENT.get(intent, intent)

    # Intent → sources (model-driven intent, deterministic mapping)
    _INTENT_SOURCES: dict[str, list[tuple[str, str, str]]] = {
        "metric_query":       [("jato_sales_parquet", "structured_bi", "Structured data.")],
        "distribution":       [("jato_sales_parquet", "structured_bi", "Distribution analysis.")],
        "trend":              [("jato_sales_parquet", "structured_bi", "Trend analysis.")],
        "comparison":         [("jato_sales_parquet", "structured_bi", "Market comparison."), ("current_price_postgres", "canonical_entity", "Price comparison.")],
        "pricing_strategy":   [("jato_sales_parquet", "structured_bi", "Market context."), ("current_price_postgres", "canonical_entity", "Current MSRP.")],
        "product_strategy":   [("jato_sales_parquet", "structured_bi", "Market context."), ("current_price_postgres", "canonical_entity", "Price corridor."), ("country_profiles", "policy_tax", "Policy context.")],
        "policy_tax":         [("country_profiles", "policy_tax", "Tax and subsidy rules.")],
        "voc_insight":        [("voc_forum_artifacts", "voc", "Customer feedback.")],
        "news_intelligence":  [("news_digest", "news", "Recent news."), ("country_profiles", "policy_tax", "Policy context for news.")],
        "country_report":     [("jato_sales_parquet", "structured_bi", "Full market data.")],
    }

    if governed_intent in _INTENT_SOURCES:
        for source_id, lane, reason in _INTENT_SOURCES[governed_intent]:
            if source_id not in seen:
                seen.add(source_id)
                items.append(SourcePlanItem(source_id=source_id, source_lane=lane, required=True, reason=reason))

    if not items:
        items.append(SourcePlanItem(source_id="jato_sales_parquet", source_lane="structured_bi", required=True, reason="Default source for general questions."))

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
        question=question, intent=governed_intent, execution_mode=execution_mode, items=items,
        requires_sql_planner="structured_bi" in lanes,
        requires_tool_planner="canonical_entity" in lanes,
        requires_evidence_retrieval=bool(lanes - {"structured_bi", "canonical_entity"}),
    )
