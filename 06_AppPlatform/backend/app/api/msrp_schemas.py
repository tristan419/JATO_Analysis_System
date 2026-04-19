from pydantic import BaseModel, Field


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
    link_source: str = "manual"
    is_active: bool = True
    notes: str | None = None


class JatoMsrpLinkPatch(BaseModel):
    country: str | None = None
    brand: str | None = None
    jato_model: str | None = None
    jato_trim: str | None = None
    jato_powertrain: str | None = None
    official_model: str | None = None
    official_trim: str | None = None
    official_edition: str | None = None
    official_powertrain: str | None = None
    confidence: int | None = Field(default=None, ge=0, le=100)
    link_source: str | None = None
    is_active: bool | None = None
    notes: str | None = None
