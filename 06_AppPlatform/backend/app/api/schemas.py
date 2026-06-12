from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class FiltersOptionsRequest(BaseModel):
    column: str
    filters: dict[str, list[str]] = Field(default_factory=dict)


class AnalysisResponse(BaseModel):
    route: str
    rows: int
    items: list[dict]


class AnalysisRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    group_by: str | None = None
    metric_candidates: list[str] = Field(default_factory=list)
    top_n: int = 20
    prefer_precomputed: bool = True


class TimeSeriesRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    grain: str = "month"
    top_n: int = 36


class OverviewRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    prefer_precomputed: bool = True
    top_n: int = 24


class DetailQueryRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    columns: list[str] = Field(default_factory=list)
    page: int = 1
    page_size: int = 200
    exclude_zero_sales: bool = False


class DetailCsvRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    columns: list[str] = Field(default_factory=list)
    max_rows: int = 5000
    exclude_zero_sales: bool = False


class DashboardTimeRange(BaseModel):
    start: str
    end: str


class AdvancedChartRequest(BaseModel):
    group: str
    chart: str
    filters: dict[str, list[str]] = Field(default_factory=dict)
    top_n: int = 20
    options: dict[str, object] = Field(default_factory=dict)
    time_range: DashboardTimeRange | None = None


class GroupedTimeSeriesRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    grain: str = "year"
    group_by: str | None = None
    share_split_by: str | None = None
    top_n: int = 10
    include_others: bool = True
    time_range: DashboardTimeRange | None = None


class ModelVersionsRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    model_name: str
    top_n: int = 50
    time_range: DashboardTimeRange | None = None


class PositioningMapRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    target_length: float | None = None
    target_msrp: float | None = None
    length_range: float = 600
    manual_competitors: list[str] = Field(default_factory=list)
    top_n: int = 80
    n_clusters: int = 4
    time_range: DashboardTimeRange | None = None


class RvFinanceVehicle(BaseModel):
    vehicle: str = "Vehicle"
    msrp: float = 30000
    down_pct: float = 20
    rv_pct: float = 45
    apr_pct: float = 3.5
    term: int = 36


class RvFinanceRequest(BaseModel):
    vehicles: list[RvFinanceVehicle] = Field(default_factory=list)
    currency: str = "EUR"
    fx_rate: float | None = None
    sensitivity_vehicle_idx: int = 0


class MarketScanDeckRequest(BaseModel):
    country: str | None = None
    target_period: str | None = None
    time_range: dict[str, str] | None = None
    fuel_types: list[str] = Field(default_factory=list)
    trend_window_months: int = 24
    origin_window_months: int = 30
    body_window_months: int = 30
    ranking_limit: int = Field(default=10, ge=10)
    drilldown_segments: list[str] = Field(default_factory=list)
    body_types: list[str] = Field(default_factory=list)
    view: str | None = None  # "overview" | "origin" | "segment" | "drilldown" | "suvAll" | "suvA" | "suvB" | "bodyType"


class PositioningPricingDeckRequest(BaseModel):
    country: str | None = None
    target_period: str | None = None
    time_range: dict[str, str] | None = None
    fuel_types: list[str] = Field(default_factory=list)
    sales_mode: Literal["month", "ytd", "rolling12"] = "month"
    top_n: int = Field(default=50, ge=1, le=200)
    msrp_min: float | None = Field(default=None, ge=0)
    msrp_max: float | None = Field(default=None, ge=0)
    length_min: float | None = Field(default=None, ge=0)
    length_max: float | None = Field(default=None, ge=0)
    price_band_size: int | None = Field(default=None, ge=500, le=200000)


class VersionComparisonDeckRequest(BaseModel):
    country: str | None = None
    target_period: str | None = None
    time_range: dict[str, str] | None = None
    fuel_types: list[str] = Field(default_factory=list)
    sales_mode: Literal["month", "ytd", "rolling12"] = "month"
    comparison_mode: Literal["same_segment", "free_comparison"] = "same_segment"
    segment: str | None = None
    models: list[str] = Field(default_factory=list)
    msrp_min: float | None = Field(default=None, ge=0)
    msrp_max: float | None = Field(default=None, ge=0)
    price_band_size: int | None = Field(default=None, ge=500, le=200000)
    body_type: str | None = None
    drive_types: list[str] = Field(default_factory=list)
    segments: list[str] = Field(default_factory=list)
    length_min: float | None = Field(default=None, ge=0)
    length_max: float | None = Field(default=None, ge=0)


class CountryChatTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    extracted_params: dict[str, object] = Field(default_factory=dict)
    intent_route: str | None = None


class CountryChatRequest(BaseModel):
    country: str
    question: str
    history: list[CountryChatTurn] = Field(default_factory=list)
    refresh_news: bool = False
    model: str | None = None


class CountryChatDeckRequest(BaseModel):
    country: str
    question: str = ""
    intents: list[str] = Field(default_factory=list)
    extracted_params: dict[str, object] = Field(default_factory=dict)
    selected_year: int | None = None
    selected_model: str | None = None
    model_top_n: int | None = None


class CountryNewsRefreshRequest(BaseModel):
    country: str
    limit: int | None = None
    persist: bool = True
    enrich_with_gemini: bool | None = None


class CrudItem(BaseModel):
    id: str
    code: str
    name: str
    status: str = "active"
    notes: str = ""


class CrudItemCreate(BaseModel):
    code: str
    name: str
    status: str = "active"
    notes: str = ""


class CrudItemPatch(BaseModel):
    code: str | None = None
    name: str | None = None
    status: str | None = None
    notes: str | None = None


CrudSortBy = Literal["code", "name", "status", "created", "updated"]
CrudSortOrder = Literal["asc", "desc"]


class ConfigProjectCreate(BaseModel):
    project_code: str
    brand: str
    model: str
    market_country: str
    display_name: str
    status: str = "active"


class ConfigProjectPatch(BaseModel):
    brand: str | None = None
    model: str | None = None
    market_country: str | None = None
    display_name: str | None = None
    status: str | None = None


class ConfigImportRunRequest(BaseModel):
    source_file_path: str
    sheet_name: str = "Data Export"
    source_schema_version: str | None = None
    replace_mode: Literal["full_replace", "incremental"] = "full_replace"
    valid_from_date: date | None = None
    notes: str | None = None


class ConfigFeatureValueUpdate(BaseModel):
    raw_value: str | None = None
    updated_by: str | None = None
    expected_version: int = 1
    comment: str | None = None


class ConfigFeatureValueCreate(BaseModel):
    trim_id: str
    feature_id: str
    raw_value: str
    updated_by: str | None = None


class VehicleTrimUpdate(BaseModel):
    brand: str | None = None
    model_name: str | None = None
    trim_name: str | None = None
    energy_type: str | None = None
    drivetrain: str | None = None
    engine: str | None = None
    model_year: str | None = None
    status: str | None = None


class MsrpSourceCreate(BaseModel):
    source_code: str
    country: str
    brand: str
    source_url: str
    source_type: str
    tier: int = Field(default=3, ge=1, le=5)
    extractor_name: str
    extractor_version: str
    price_semantics: str
    requires_location: bool = False
    enabled: bool = True
    notes: str | None = None


class MsrpSourcePatch(BaseModel):
    country: str | None = None
    brand: str | None = None
    source_url: str | None = None
    source_type: str | None = None
    tier: int | None = Field(default=None, ge=1, le=5)
    extractor_name: str | None = None
    extractor_version: str | None = None
    price_semantics: str | None = None
    requires_location: bool | None = None
    enabled: bool | None = None
    notes: str | None = None


class MsrpObservationIngest(BaseModel):
    source_id: str
    country: str
    brand: str
    jato_model: str
    jato_trim: str
    jato_powertrain: str | None = None
    official_model: str
    official_trim: str
    official_edition: str | None = None
    official_powertrain: str | None = None
    msrp_value: float
    currency: str
    tax_included: bool
    price_label: str
    availability_text: str | None = None
    observed_at_utc: datetime | None = None
    source_url: str
    source_snapshot_path: str | None = None
    source_payload_hash: str | None = None
    extraction_version: str
    match_confidence: float
    match_status: Literal[
        "auto_accepted",
        "review_required",
        "human_approved",
        "rejected",
    ]
    match_reason_json: dict[str, object] | None = None
    source_context_json: dict[str, object] | None = None
    candidate_matches_json: list[dict[str, object]] | None = None
    price_semantics: str | None = None
    monthly_payment: float | None = None
    down_payment: float | None = None
    down_payment_pct: float | None = None
    term_months: int | None = None
    apr: float | None = None
    effective_apr: float | None = None
    balloon_payment: float | None = None
    finance_type: str | None = None
    total_credit_cost: float | None = None
    total_amount_payable: float | None = None
    annual_mileage_limit: int | None = None
    offer_valid_until: date | None = None
    subsidy_amount: float | None = None
    net_price_after_subsidy: float | None = None
    finance_currency: str | None = None


class MsrpObservationCreate(MsrpObservationIngest):
    pass


class MsrpObservationPatch(BaseModel):
    source_id: str | None = None
    country: str | None = None
    brand: str | None = None
    jato_model: str | None = None
    jato_trim: str | None = None
    jato_powertrain: str | None = None
    official_model: str | None = None
    official_trim: str | None = None
    official_edition: str | None = None
    official_powertrain: str | None = None
    msrp_value: float | None = None
    currency: str | None = None
    tax_included: bool | None = None
    price_label: str | None = None
    availability_text: str | None = None
    observed_at_utc: datetime | None = None
    source_url: str | None = None
    source_snapshot_path: str | None = None
    source_payload_hash: str | None = None
    extraction_version: str | None = None
    match_confidence: float | None = None
    match_status: Literal[
        "auto_accepted",
        "review_required",
        "human_approved",
        "rejected",
        "override_applied",
    ] | None = None
    match_reason_json: dict[str, object] | None = None
    source_context_json: dict[str, object] | None = None
    candidate_matches_json: list[dict[str, object]] | None = None


class ScrapeBatchIngestRequest(BaseModel):
    batch_code: str
    trigger_type: str
    scope_country: str
    scope_brands: list[str] = Field(default_factory=list)
    failed_count: int = 0
    notes: str | None = None
    started_at_utc: datetime | None = None
    finished_at_utc: datetime | None = None
    observations: list[MsrpObservationIngest] = Field(default_factory=list)


class CurrentPriceMaterializeRequest(BaseModel):
    country: str | None = None
    brand: str | None = None
    jato_model: str | None = None
    limit: int = 500


class CurrentPriceRemapRequest(BaseModel):
    decided_by: str
    note: str | None = None


class ReviewDecisionCreate(BaseModel):
    decision: Literal["approve", "reject", "remap"]
    decided_official_model: str | None = None
    decided_official_trim: str | None = None
    note: str | None = None
    decided_by: str
    persist_override: bool = False
    override_reason: str | None = None
    valid_from_date: date | None = None
    link_confidence: int = Field(default=100, ge=0, le=100)
    link_source: str | None = None
    link_notes: str | None = None
    mismatch_reason_category: Literal[
        "naming_mismatch",
        "timing_mismatch",
        "market_mismatch",
        "granularity_mismatch",
    ] | None = None


class MatchOverrideCreate(BaseModel):
    country: str
    brand: str
    jato_model: str
    jato_trim: str
    jato_powertrain: str | None = None
    official_model: str
    official_trim: str
    valid_from_date: date
    valid_to_date: date | None = None
    override_reason: str
    created_by: str


class MatchOverridePatch(BaseModel):
    official_model: str | None = None
    official_trim: str | None = None
    jato_powertrain: str | None = None
    valid_from_date: date | None = None
    valid_to_date: date | None = None
    override_reason: str | None = None
    created_by: str | None = None


class JatoMsrpLinkCreate(BaseModel):
    country: str
    brand: str
    jato_model: str
    jato_trim: str
    jato_powertrain: str | None = None
    official_model: str
    official_trim: str
    official_edition: str | None = None
    official_powertrain: str | None = None
    confidence: int = Field(default=80, ge=0, le=100)
    link_source: str
    is_active: bool = True
    notes: str | None = None


class JatoMsrpLinkPatch(BaseModel):
    official_model: str | None = None
    official_trim: str | None = None
    official_edition: str | None = None
    official_powertrain: str | None = None
    confidence: int | None = Field(default=None, ge=0, le=100)
    link_source: str | None = None
    is_active: bool | None = None
    notes: str | None = None


# ── Advanced Analysis ──

class AdvancedAnalysisBaseRequest(BaseModel):
    country: str | None = None
    target_period: str | None = None
    time_range: dict[str, str] | None = None
    fuel_types: list[str] = Field(default_factory=list)
    segments: list[str] = Field(default_factory=list)


class AdvancedAnalysisKpiRequest(AdvancedAnalysisBaseRequest):
    group_by: list[str] = Field(default_factory=lambda: ["segment", "model"])
    top_n: int = Field(default=50, ge=5, le=500)


class AdvancedAnalysisShiftShareRequest(AdvancedAnalysisBaseRequest):
    base_period: str | None = None
    cell_dims: list[str] = Field(default_factory=lambda: ["segment"])


class AdvancedAnalysisSeasonalRequest(AdvancedAnalysisBaseRequest):
    model_filter: str | None = None
    segment_filter: str | None = None


class AdvancedAnalysisCellAttributionRequest(AdvancedAnalysisBaseRequest):
    cell_dims: list[str] = Field(default_factory=lambda: ["segment", "registration_type", "drive_type"])
    top_n_cells: int = Field(default=20, ge=5, le=100)


class AdvancedAnalysisTransferMatrixRequest(AdvancedAnalysisBaseRequest):
    cell_dims: list[str] = Field(default_factory=lambda: ["segment"])
    top_n_models: int = Field(default=20, ge=5, le=100)


class AdvancedAnalysisNestedShiftShareRequest(AdvancedAnalysisBaseRequest):
    base_period: str | None = None
    hierarchy: list[str] = Field(default_factory=lambda: ["segment", "registration_type", "drive_type"])


class AdvancedAnalysisDrilldownRequest(AdvancedAnalysisBaseRequest):
    base_period: str | None = None
    scope_filters: list[dict[str, str]] = Field(default_factory=list)
    top_n: int = Field(default=20, ge=5, le=100)


class AdvancedAnalysisTransferMartRequest(AdvancedAnalysisBaseRequest):
    base_period: str | None = None
    sales_mode: Literal["month", "ytd", "rolling12"] = "month"
    scope_filters: list[dict[str, str]] = Field(default_factory=list)
    top_n: int = Field(default=25, ge=5, le=100)


class AdvancedAnalysisCompetitorSetRequest(AdvancedAnalysisBaseRequest):
    base_period: str | None = None
    sales_mode: Literal["month", "ytd", "rolling12"] = "month"
    scope_filters: list[dict[str, str]] = Field(default_factory=list)
    target_model: str | None = None
    profile_specs: dict[str, float] = Field(default_factory=dict)
    top_n: int = Field(default=12, ge=5, le=50)
