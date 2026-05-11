"""Catalog data models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FieldCatalogItem(BaseModel):
    name: str
    semantic_role: str
    data_type: str
    filterable: bool = False
    aggregatable: bool = False
    groupable: bool = False
    description: str | None = None


class DatasetCatalogItem(BaseModel):
    dataset_id: str
    display_name: str
    source_lane: str
    storage_type: str
    grain: str = ""
    owner: str | None = None
    freshness_field: str | None = None
    latest_period: str | None = None
    fields: list[FieldCatalogItem] = Field(default_factory=list)
    allowed_intents: list[str] = Field(default_factory=list)
    required_filters: list[str] = Field(default_factory=list)
    join_keys: list[str] = Field(default_factory=list)
    governance: dict[str, Any] = Field(default_factory=dict)
