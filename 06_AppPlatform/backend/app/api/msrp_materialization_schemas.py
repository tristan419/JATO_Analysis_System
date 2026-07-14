from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _to_camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(part.capitalize() for part in tail)


class MaterializationContract(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
        extra="forbid",
        str_strip_whitespace=True,
    )


class MaterializationApprovalCreateRequest(MaterializationContract):
    operation: Literal["materialize"] = "materialize"
    scope_kind: Literal["observations", "batch"]
    scrape_batch_id: UUID | None = None
    observation_ids: list[UUID] = Field(min_length=1, max_length=500)
    gate_decision_ids: list[UUID] = Field(min_length=1, max_length=500)
    reason: str = Field(min_length=8, max_length=2000)
    expires_at_utc: datetime | None = None
    rollback_plan_ref: str = Field(min_length=3, max_length=1000)
    compensation_plan_ref: str = Field(min_length=3, max_length=1000)

    @model_validator(mode="after")
    def validate_exact_scope(self):
        if len(self.observation_ids) != len(self.gate_decision_ids):
            raise ValueError(
                "observationIds and gateDecisionIds must have equal length"
            )
        if len(set(self.observation_ids)) != len(self.observation_ids):
            raise ValueError("observationIds must be unique")
        if len(set(self.gate_decision_ids)) != len(self.gate_decision_ids):
            raise ValueError("gateDecisionIds must be unique")
        if self.scope_kind == "batch" and self.scrape_batch_id is None:
            raise ValueError("scrapeBatchId is required for batch scope")
        if self.scope_kind == "observations" and self.scrape_batch_id is not None:
            raise ValueError(
                "scrapeBatchId is only valid when scopeKind is batch"
            )
        return self


class MaterializationExecuteRequest(MaterializationContract):
    approval_id: UUID
    run_id: str = Field(min_length=3, max_length=200)
    idempotency_key: str = Field(min_length=8, max_length=300)


class MaterializationBatchExecuteRequest(MaterializationExecuteRequest):
    limit: int = Field(default=500, ge=1, le=500)
