from typing import Literal

from pydantic import BaseModel, Field


class ReviewAutoResolveRequest(BaseModel):
    decided_by: str
    country: str | None = None
    brand: str | None = None
    model: str | None = None
    note: str | None = None
    limit: int = Field(default=500, ge=1, le=1000)


class ReviewDecisionCreate(BaseModel):
    decision: Literal["approve", "reject", "remap"]
    accepted_observation_id: str | None = None
    decided_official_model: str | None = None
    decided_official_trim: str | None = None
    note: str | None = None
    decided_by: str
    link_confidence: int = Field(default=100, ge=0, le=100)
    link_source: str | None = None
    link_notes: str | None = None
    mismatch_reason_category: Literal[
        "naming_mismatch",
        "timing_mismatch",
        "market_mismatch",
        "granularity_mismatch",
    ] | None = None
