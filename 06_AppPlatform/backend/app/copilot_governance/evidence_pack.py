"""Evidence Pack — the Answer Composer's single trusted input."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class EvidenceSource(BaseModel):
    source_id: str
    source_lane: str
    source_name: str = ""
    freshness: str | None = None
    confidence: Literal["high", "medium", "low"] = "medium"
    coverage: Literal["strong", "partial", "thin", "missing"] = "partial"
    limitations: list[str] = Field(default_factory=list)


class EvidenceClaim(BaseModel):
    claim_id: str = ""
    claim: str
    claim_type: Literal[
        "fact", "metric", "trend", "comparison",
        "risk", "recommendation", "qualitative_signal",
    ] = "fact"
    supporting_source_ids: list[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"] = "medium"
    limitations: list[str] = Field(default_factory=list)


class EvidencePack(BaseModel):
    evidence_pack_id: str = ""
    question: str = ""
    country: str | None = None
    intent: str = ""
    answer_mode: str = ""
    sources: list[EvidenceSource] = Field(default_factory=list)
    claims: list[EvidenceClaim] = Field(default_factory=list)
    tables: list[dict[str, object]] = Field(default_factory=list)
    charts: list[dict[str, object]] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    next_data_needed: list[str] = Field(default_factory=list)


def build_evidence_pack_from_snapshot(
    snapshot: dict,
    source_plan=None,
    question: str = "",
    intent: str = "",
    country: str | None = None,
    extra: dict | None = None,
) -> EvidencePack:
    sources: list[EvidenceSource] = []
    cross_tabs = snapshot.get("crossTabs", {})
    if isinstance(cross_tabs, dict) and cross_tabs.get("availableDimensions"):
        sources.append(EvidenceSource(
            source_id="jato_sales_parquet",
            source_lane="structured_bi",
            source_name="JATO Sales Parquet",
            freshness=snapshot.get("periodLabel", ""),
            confidence="high",
            coverage="strong",
        ))
    if snapshot.get("overviewSummary"):
        sources.append(EvidenceSource(
            source_id="market_scan_deck",
            source_lane="structured_bi",
            source_name="Market Scan Deck",
            coverage="strong",
            confidence="high",
        ))
    if snapshot.get("newsDigest"):
        sources.append(EvidenceSource(
            source_id="news_digest",
            source_lane="news",
            source_name="News Digest",
            confidence="medium",
            coverage="partial",
        ))

    if source_plan is not None:
        for item in getattr(source_plan, "items", []):
            if not any(s.source_id == item.source_id for s in sources):
                sources.append(EvidenceSource(
                    source_id=item.source_id,
                    source_lane=item.source_lane,
                    source_name=item.source_id,
                    coverage="partial",
                    confidence="medium",
                ))

    limitations: list[str] = []
    if not cross_tabs:
        limitations.append("交叉维度分析数据不可用。")
    if not snapshot.get("newsDigest"):
        limitations.append("当前无新闻证据。")

    tax_estimate = None
    if extra and extra.get("tax_estimate"):
        tax_estimate = extra["tax_estimate"]

    return EvidencePack(
        evidence_pack_id="",
        question=question,
        country=country,
        intent=intent,
        sources=sources,
        limitations=limitations,
        **({"tables": [{"title": "税负估算", "columns": ["项目", "金额", "周期"], "rows": [
            [c.label, str(c.amount), c.period]
            for c in (tax_estimate.one_time_costs + tax_estimate.annual_costs)
        ]}]} if tax_estimate else {}),
    )
