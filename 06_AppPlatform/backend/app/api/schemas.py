from pydantic import BaseModel, Field
from typing import Literal


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
