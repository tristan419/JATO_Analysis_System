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


class AdvancedChartRequest(BaseModel):
    group: str
    chart: str
    filters: dict[str, list[str]] = Field(default_factory=dict)
    top_n: int = 20
    options: dict[str, object] = Field(default_factory=dict)


class GroupedTimeSeriesRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    grain: str = "year"
    group_by: str | None = None
    top_n: int = 10
    include_others: bool = True


class ModelVersionsRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    model_name: str
    top_n: int = 50


class PositioningMapRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    target_length: float | None = None
    target_msrp: float | None = None
    length_range: float = 600
    manual_competitors: list[str] = Field(default_factory=list)
    top_n: int = 80
    n_clusters: int = 4


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
    fuel_types: list[str] = Field(default_factory=list)
    trend_window_months: int = 24
    origin_window_months: int = 30
    body_window_months: int = 30
    ranking_limit: int = 12
    drilldown_segment: str | None = None


class CountryChatTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class CountryChatRequest(BaseModel):
    country: str
    question: str
    history: list[CountryChatTurn] = Field(default_factory=list)


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


class MsrpSourceCreate(BaseModel):
    source_code: str
    country: str
    brand: str
    source_url: str
    source_type: str
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


class MatchOverrideCreate(BaseModel):
    country: str
    brand: str
    jato_model: str
    jato_trim: str
    official_model: str
    official_trim: str
    valid_from_date: date
    valid_to_date: date | None = None
    override_reason: str
    created_by: str


class MatchOverridePatch(BaseModel):
    official_model: str | None = None
    official_trim: str | None = None
    valid_from_date: date | None = None
    valid_to_date: date | None = None
    override_reason: str | None = None
    created_by: str | None = None
