"""Unified intent taxonomy for the governed Country Copilot."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

CountryCopilotIntent = Literal[
    "fact_lookup",
    "metric_query",
    "comparison",
    "trend",
    "distribution",
    "correlation",
    "pricing_strategy",
    "product_strategy",
    "policy_tax",
    "voc_insight",
    "news_intelligence",
    "country_report",
    "deck_generation",
    "chitchat",
]

QuestionType = Literal["direct", "analytical", "strategic", "report", "chitchat"]


class IntentRecognitionResult(BaseModel):
    intent: str
    confidence: float = Field(ge=0.0, le=1.0)
    question_type: QuestionType = "direct"
    requires_structured_data: bool = False
    requires_canonical_entity_data: bool = False
    requires_unstructured_evidence: bool = False
    requires_policy_context: bool = False
    requires_news_context: bool = False
    requires_chart: bool = False
    requires_table: bool = False
    requires_report: bool = False
    rationale: str = ""


# Map existing country_chat_service intents to the new taxonomy.
LEGACY_TO_GOVERNED_INTENT: dict[str, str] = {
    "brand-ranking": "metric_query",
    "segment-analysis": "distribution",
    "origin-analysis": "distribution",
    "market-context": "news_intelligence",
    "powertrain-mix": "distribution",
    "nev-analysis": "distribution",
    "positioning-analysis": "comparison",
    "trend-summary": "trend",
    "competitive": "comparison",
    "pricing-summary": "pricing_strategy",
    "general-summary": "country_report",
    "precise-lookup": "pricing_strategy",
}
