"""Pydantic schemas for Order Genius module."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


# ── Material Master Upload ────────────────────────────────────────────


class InitiateMaterialUploadRequest(BaseModel):
    file_name: str
    total_size: int = Field(ge=1)
    chunk_size: int = Field(default=5 * 1024 * 1024, ge=1024, le=50 * 1024 * 1024)


class UploadSessionResponse(BaseModel):
    upload_id: str
    file_name: str
    total_size: int
    chunk_size: int
    total_chunks: int
    uploaded_chunks: list[int]
    status: str


class MaterialSkuPreviewRow(BaseModel):
    row_index: int
    sheet_name: str
    brand: str
    model_name: str
    version: str
    exterior_color_name: str
    exterior_color_code: str
    exterior_color_type: str
    interior_color_name: str | None = None
    bom_template: str | None = None
    material_code: str
    base_fob_eur: float | None = None
    powertrain: str | None = None
    warnings: list[str] = []


class MaterialUploadPreview(BaseModel):
    upload_id: str
    total_rows: int
    new_skus: int
    existing_skus: int
    sheet_names: list[str]
    rows: list[MaterialSkuPreviewRow]
    warnings: list[str]


class PublishBaselineRequest(BaseModel):
    notes: str | None = None


class PublishBaselineResponse(BaseModel):
    baseline_version_id: str
    baseline_name: str
    sku_count: int
    fob_count: int
    status: str


# ── Payment Term & Colour Surcharge ───────────────────────────────────


class PaymentTermRuleOut(BaseModel):
    payment_term_rule_id: str
    payment_term_code: str
    payment_method: str
    lc_days: int
    fob_adjustment_eur: float
    adjustment_rate: float | None = None
    is_active: bool


class ColourSurchargeRuleOut(BaseModel):
    colour_surcharge_rule_id: str
    brand: str
    colour_type: str
    surcharge_eur: float
    is_active: bool


class ColourSurchargeUpdate(BaseModel):
    brand: str = Field(min_length=1)
    colourType: str = Field(min_length=1)
    surchargeEur: float = Field(ge=0)


# ── Order Genius Matrix ──────────────────────────────────────────────


class MonthCell(BaseModel):
    quantity: int
    is_editable: bool


class MaterialSkuMatrixRow(BaseModel):
    material_code: str
    brand: str
    model_name: str
    version: str
    colour: str
    colourCode: str | None = None
    interiorColorName: str | None = None
    interiorColourCode: str | None = None
    interiorPackage: str | None = None
    editionTag: str | None = None
    powertrain: str | None = None
    fob_eur: float | None = None
    lifecycle_status: str
    editable: bool
    display_style: str | None = None  # "strikethrough" for historical
    remark: str | None = None
    months: dict[str, MonthCell]  # key: month number as string "1".."12"
    ttl: int


class MatrixResponse(BaseModel):
    country_code: str
    country_name: str | None = None
    payment_term_code: str | None = None
    year: int
    rows: list[MaterialSkuMatrixRow]
    total_rows: int


class OrderGeniusOptions(BaseModel):
    country_code: str
    payment_term_code: str | None = None
    brands: list[str]
    models: list[str]
    powertrains: list[str]
    versions: list[str]
    colours: list[str]
    material_codes: list[str]


class QuantityCellUpdate(BaseModel):
    country_code: str
    order_year: int
    order_month: int = Field(ge=1, le=12)
    material_code: str
    quantity: int = Field(ge=0)
    row_version: int = Field(default=1, ge=1)


class QuantityCellResponse(BaseModel):
    order_quantity_cell_id: str
    country_code: str
    order_year: int
    order_month: int
    material_code: str
    quantity: int
    fob_eur: float
    row_version: int


class RemarkUpdate(BaseModel):
    remark: str
    row_version: int = Field(default=1, ge=1)


class RemarkResponse(BaseModel):
    material_code: str
    remark: str | None
    row_version: int


# ── Export ────────────────────────────────────────────────────────────


class ExportRequest(BaseModel):
    country: str
    year: int
    include_historical_with_quantity: bool = True
