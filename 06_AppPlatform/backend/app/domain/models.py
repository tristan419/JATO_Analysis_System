from pydantic import BaseModel, Field


class FilterPayload(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)


class AnalysisQuery(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)
    group_by: str | None = None
    metric_candidates: list[str] = Field(default_factory=list)
    top_n: int = 20
    prefer_precomputed: bool = True


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


class CrudItemUpdate(BaseModel):
    code: str | None = None
    name: str | None = None
    status: str | None = None
    notes: str | None = None
