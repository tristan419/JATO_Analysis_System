"""QueryPlan — structured query contract for governed execution."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class QueryMetric(BaseModel):
    field: str
    aggregation: Literal["sum", "avg", "min", "max", "count", "count_distinct", "median"]
    alias: str


class QueryFilter(BaseModel):
    field: str
    operator: Literal["=", "!=", "in", "not_in", ">=", "<=", ">", "<", "between", "contains"]
    value: Any


class QueryPlan(BaseModel):
    query_id: str
    dataset_id: str
    query_type: Literal["aggregate", "detail", "ranking", "trend", "distribution", "correlation"]
    filters: list[QueryFilter] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    metrics: list[QueryMetric] = Field(default_factory=list)
    order_by: list[dict[str, str]] = Field(default_factory=list)
    limit: int = 1000
    time_grain: str | None = None
    rationale: str = ""
