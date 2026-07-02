"""Core models for the unified scraping pipeline.

These models are intentionally runtime-neutral. Existing MSRP, news, and VOC
runners can map into them without changing their fetch/extract behavior.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


JobKind = Literal["msrp", "news", "voc", "policy", "incentive", "spec"]
FetcherKind = Literal[
    "httpx",
    "requests",
    "scrapling",
    "playwright",
    "firecrawl_scrape",
    "firecrawl_crawl",
    "crawlee",
]
ExtractorKind = Literal[
    "css_rules",
    "schema_org",
    "pdf_table",
    "llm_extract",
    "rss",
    "json_path",
    "http_json",
    "http_text",
    "pdf_text",
    "scrapling",
    "playwright_card_flow",
]
DocumentContentType = Literal["html", "xml", "json", "pdf", "text", "binary"]
ScrapeRunStatus = Literal["pending", "running", "ok", "failed", "skipped"]
ObservationQuality = Literal["high", "medium", "low", "reject"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_domain(value: str) -> str:
    candidate = value.strip().lower()
    if not candidate:
        raise ValueError("domain must not be empty")
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    domain = parsed.netloc or parsed.path
    domain = domain.strip().strip("/")
    if not domain or "/" in domain:
        raise ValueError(f"invalid domain: {value!r}")
    return domain


def canonical_job_id(
    *,
    kind: JobKind,
    country_code: str,
    source_code: str,
    subject: str | None = None,
) -> str:
    """Build the stable job id form used by queue rows and logs."""
    parts = [
        kind,
        country_code.strip().lower(),
        source_code.strip().lower(),
    ]
    if subject:
        parts.append(subject.strip().lower())
    normalized = [
        "".join(ch if ch.isalnum() else "_" for ch in part).strip("_")
        for part in parts
    ]
    return ":".join(part for part in normalized if part)


class FreshnessPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_age_hours: int = Field(gt=0)
    skip_if_fresh: bool = True


class ScrapeJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=3, max_length=96)
    kind: JobKind
    url: str = Field(min_length=1)
    fetcher: FetcherKind
    extractor: ExtractorKind
    extractor_config: dict[str, Any] = Field(default_factory=dict)
    schema_ref: str = Field(min_length=1)
    freshness: FreshnessPolicy
    priority: int = Field(default=50, ge=0, le=100)
    allow_domains: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        candidate = value.strip()
        parsed = urlparse(candidate)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("url must be an absolute http(s) URL")
        return candidate

    @field_validator("allow_domains", mode="before")
    @classmethod
    def _normalize_allow_domains(cls, value: Any) -> list[str]:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            items = [value]
        else:
            items = list(value)
        seen: set[str] = set()
        domains: list[str] = []
        for item in items:
            domain = _normalize_domain(str(item))
            if domain in seen:
                continue
            seen.add(domain)
            domains.append(domain)
        return domains

    @model_validator(mode="after")
    def _default_allow_domain(self) -> "ScrapeJob":
        if self.allow_domains:
            return self
        parsed = urlparse(self.url)
        self.allow_domains.append(_normalize_domain(parsed.netloc))
        return self

    def to_queue_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class FetchMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    final_url: str | None = None
    status_code: int | None = Field(default=None, ge=100, le=599)
    fetched_at: datetime = Field(default_factory=_utc_now)
    headers: dict[str, str] = Field(default_factory=dict)


class RawDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=3, max_length=96)
    content_type: DocumentContentType
    body_text: str | None = None
    body_bytes: bytes | None = None
    metadata: FetchMetadata = Field(default_factory=FetchMetadata)

    @model_validator(mode="after")
    def _require_body(self) -> "RawDocument":
        if self.body_text is None and self.body_bytes is None:
            raise ValueError("RawDocument requires body_text or body_bytes")
        return self


class StructuredObservation(BaseModel):
    """Extractor output before shared normalization and persistence."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=3, max_length=96)
    kind: JobKind
    schema_ref: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    extracted_at: datetime = Field(default_factory=_utc_now)
    source_url: str | None = None
    source_document_ref: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _require_payload(self) -> "StructuredObservation":
        if not self.payload:
            raise ValueError("StructuredObservation payload must not be empty")
        return self


class NormalizedObservation(BaseModel):
    """Normalizer output ready for a domain sink."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=3, max_length=96)
    kind: JobKind
    schema_ref: str = Field(min_length=1)
    record_key: str = Field(min_length=1, max_length=160)
    payload: dict[str, Any] = Field(default_factory=dict)
    quality: ObservationQuality = "medium"
    normalized_at: datetime = Field(default_factory=_utc_now)
    source_url: str | None = None
    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("record_key")
    @classmethod
    def _normalize_record_key(cls, value: str) -> str:
        key = value.strip()
        if not key:
            raise ValueError("record_key must not be empty")
        return key

    @model_validator(mode="after")
    def _require_payload(self) -> "NormalizedObservation":
        if not self.payload:
            raise ValueError("NormalizedObservation payload must not be empty")
        return self


class SinkResult(BaseModel):
    """Persistence result shared by file, database, and artifact sinks."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=3, max_length=96)
    kind: JobKind
    sink_name: str = Field(min_length=1)
    status: ScrapeRunStatus
    rows_in: int = Field(ge=0)
    rows_written: int = Field(default=0, ge=0)
    rows_skipped: int = Field(default=0, ge=0)
    artifact_refs: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_row_counts(self) -> "SinkResult":
        if self.rows_written + self.rows_skipped > self.rows_in:
            raise ValueError("rows_written + rows_skipped must not exceed rows_in")
        return self


class ScrapeRunLog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=3, max_length=96)
    kind: JobKind
    started_at: datetime = Field(default_factory=_utc_now)
    finished_at: datetime | None = None
    status: ScrapeRunStatus = "running"
    fetcher: FetcherKind | None = None
    extractor: ExtractorKind | None = None
    http_status: int | None = Field(default=None, ge=100, le=599)
    bytes_fetched: int | None = Field(default=None, ge=0)
    rows_out: int | None = Field(default=None, ge=0)
    error_class: str | None = None
    error_detail: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.finished_at is None:
            return None
        return max(0.0, (self.finished_at - self.started_at).total_seconds())

    def finish(
        self,
        *,
        status: Literal["ok", "failed", "skipped"],
        rows_out: int | None = None,
        error_class: str | None = None,
        error_detail: str | None = None,
    ) -> "ScrapeRunLog":
        return self.model_copy(
            update={
                "status": status,
                "finished_at": _utc_now(),
                "rows_out": rows_out,
                "error_class": error_class,
                "error_detail": error_detail,
            }
        )
