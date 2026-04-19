from typing import Literal

from pydantic import BaseModel, Field


class ReviewDecisionCreate(BaseModel):
    decision: Literal["approve", "reject", "remap"]
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
