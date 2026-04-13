from __future__ import annotations

from datetime import date, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TimestampMixin:
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class ImportBatch(Base):
    __tablename__ = "import_batches"
    __table_args__ = (
        Index(
            "ix_ops_import_batches_domain_created",
            "domain",
            "created_at_utc",
        ),
        Index("ix_ops_import_batches_status", "import_status"),
        {"schema": "ops"},
    )

    import_batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    domain: Mapped[str] = mapped_column(Text, nullable=False)
    source_file_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_file_path: Mapped[str] = mapped_column(Text, nullable=False)
    source_file_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    import_status: Mapped[str] = mapped_column(Text, nullable=False)
    row_count: Mapped[int] = mapped_column(default=0, nullable=False)
    error_count: Mapped[int] = mapped_column(default=0, nullable=False)
    triggered_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ConfigProject(TimestampMixin, Base):
    __tablename__ = "config_projects"
    __table_args__ = (
        UniqueConstraint(
            "project_code",
            name="uq_config_projects_project_code",
        ),
        Index(
            "ix_engineering_config_projects_brand_model",
            "brand",
            "model",
        ),
        Index(
            "ix_engineering_config_projects_market_country",
            "market_country",
        ),
        {"schema": "engineering"},
    )

    project_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    project_code: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    market_country: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="active")


class MsrpSource(TimestampMixin, Base):
    __tablename__ = "sources"
    __table_args__ = (
        UniqueConstraint("source_code", name="uq_sources_source_code"),
        Index("ix_msrp_sources_country_brand", "country", "brand"),
        Index("ix_msrp_sources_enabled", "enabled"),
        {"schema": "msrp"},
    )

    source_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    source_code: Mapped[str] = mapped_column(Text, nullable=False)
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    extractor_name: Mapped[str] = mapped_column(Text, nullable=False)
    extractor_version: Mapped[str] = mapped_column(Text, nullable=False)
    price_semantics: Mapped[str] = mapped_column(Text, nullable=False)
    requires_location: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class MatchOverride(TimestampMixin, Base):
    __tablename__ = "match_overrides"
    __table_args__ = (
        UniqueConstraint(
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "official_model",
            "official_trim",
            "valid_from_date",
            name="uq_match_overrides_business_key",
        ),
        Index(
            "ix_review_match_overrides_country_brand_model",
            "country",
            "brand",
            "jato_model",
        ),
        {"schema": "review"},
    )

    override_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    jato_model: Mapped[str] = mapped_column(Text, nullable=False)
    jato_trim: Mapped[str] = mapped_column(Text, nullable=False)
    official_model: Mapped[str] = mapped_column(Text, nullable=False)
    official_trim: Mapped[str] = mapped_column(Text, nullable=False)
    valid_from_date: Mapped[date] = mapped_column(Date, nullable=False)
    valid_to_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    override_reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)


class ConfigImportBatch(Base):
    __tablename__ = "config_import_batches"
    __table_args__ = (
        Index(
            "ix_engineering_config_import_batches_project_created",
            "project_id",
            "created_at_utc",
        ),
        Index(
            "ix_engineering_config_import_batches_status",
            "import_status",
        ),
        {"schema": "engineering"},
    )

    config_import_batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    project_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.config_projects.project_id"),
        nullable=False,
    )
    import_batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ops.import_batches.import_batch_id"),
        nullable=False,
    )
    source_schema_version: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    replace_mode: Mapped[str] = mapped_column(Text, nullable=False)
    import_status: Mapped[str] = mapped_column(Text, nullable=False)
    row_count: Mapped[int] = mapped_column(default=0, nullable=False)
    valid_from_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ConfigVariant(TimestampMixin, Base):
    __tablename__ = "config_variants"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "config_import_batch_id",
            "row_hash",
            name="uq_config_variants_batch_row_hash",
        ),
        Index(
            "ix_engineering_config_variants_project_active",
            "project_id",
            "is_active",
        ),
        Index(
            "ix_engineering_config_variants_brand_model_country",
            "brand",
            "model",
            "market_country",
        ),
        Index(
            "ix_engineering_config_variants_trim_name",
            "trim_name",
        ),
        {"schema": "engineering"},
    )

    variant_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    project_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.config_projects.project_id"),
        nullable=False,
    )
    config_import_batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "engineering.config_import_batches.config_import_batch_id"
        ),
        nullable=False,
    )
    external_row_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    trim_name: Mapped[str] = mapped_column(Text, nullable=False)
    version_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    market_country: Mapped[str] = mapped_column(Text, nullable=False)
    powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_style: Mapped[str | None] = mapped_column(Text, nullable=True)
    drive_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    battery_kwh: Mapped[float | None] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )
    range_km: Mapped[float | None] = mapped_column(
        Numeric(10, 2),
        nullable=True,
    )
    target_msrp: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )
    row_hash: Mapped[str] = mapped_column(Text, nullable=False)
    attributes_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    source_file_path: Mapped[str | None] = mapped_column(Text, nullable=True)


class ScrapeBatch(Base):
    __tablename__ = "scrape_batches"
    __table_args__ = (
        UniqueConstraint("batch_code", name="uq_scrape_batches_batch_code"),
        Index(
            "ix_msrp_scrape_batches_country_started",
            "scope_country",
            "started_at_utc",
        ),
        Index("ix_msrp_scrape_batches_status", "status"),
        {"schema": "msrp"},
    )

    scrape_batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    batch_code: Mapped[str] = mapped_column(Text, nullable=False)
    trigger_type: Mapped[str] = mapped_column(Text, nullable=False)
    scope_country: Mapped[str] = mapped_column(Text, nullable=False)
    scope_brands_json: Mapped[list[str] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    candidate_count: Mapped[int] = mapped_column(default=0, nullable=False)
    success_count: Mapped[int] = mapped_column(default=0, nullable=False)
    review_required_count: Mapped[int] = mapped_column(
        default=0,
        nullable=False,
    )
    failed_count: Mapped[int] = mapped_column(default=0, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class MsrpObservation(TimestampMixin, Base):
    __tablename__ = "observations"
    __table_args__ = (
        Index(
            "ix_msrp_observations_country_brand_model_observed",
            "country",
            "brand",
            "jato_model",
            "observed_at_utc",
        ),
        Index(
            "ix_msrp_observations_match_status_observed",
            "match_status",
            "observed_at_utc",
        ),
        Index(
            "ix_msrp_observations_source_payload_hash",
            "source_payload_hash",
        ),
        {"schema": "msrp"},
    )

    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    scrape_batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.scrape_batches.scrape_batch_id"),
        nullable=False,
    )
    source_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.sources.source_id"),
        nullable=False,
    )
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    jato_model: Mapped[str] = mapped_column(Text, nullable=False)
    jato_trim: Mapped[str] = mapped_column(Text, nullable=False)
    jato_powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_model: Mapped[str] = mapped_column(Text, nullable=False)
    official_trim: Mapped[str] = mapped_column(Text, nullable=False)
    official_edition: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_powertrain: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    msrp_value: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False)
    source_msrp_value: Mapped[float] = mapped_column(
        Numeric(14, 2),
        nullable=False,
    )
    source_currency: Mapped[str] = mapped_column(Text, nullable=False)
    fx_rate_to_eur: Mapped[float] = mapped_column(
        Numeric(14, 8),
        nullable=False,
    )
    fx_rate_as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    fx_source: Mapped[str] = mapped_column(Text, nullable=False)
    tax_included: Mapped[bool] = mapped_column(Boolean, nullable=False)
    price_label: Mapped[str] = mapped_column(Text, nullable=False)
    availability_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_snapshot_path: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    source_payload_hash: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    extraction_version: Mapped[str] = mapped_column(Text, nullable=False)
    match_confidence: Mapped[float] = mapped_column(
        Numeric(5, 4),
        nullable=False,
    )
    match_status: Mapped[str] = mapped_column(Text, nullable=False)
    match_reason_json: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
    )


class CurrentPrice(Base):
    __tablename__ = "current_prices"
    __table_args__ = (
        UniqueConstraint(
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            name="uq_current_prices_business_key",
        ),
        Index("ix_msrp_current_prices_country_brand", "country", "brand"),
        Index("ix_msrp_current_prices_jato_model", "jato_model"),
        {"schema": "msrp"},
    )

    current_price_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    jato_model: Mapped[str] = mapped_column(Text, nullable=False)
    jato_trim: Mapped[str] = mapped_column(Text, nullable=False)
    jato_powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_model: Mapped[str] = mapped_column(Text, nullable=False)
    official_trim: Mapped[str] = mapped_column(Text, nullable=False)
    official_edition: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_powertrain: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    effective_observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    current_msrp_value: Mapped[float] = mapped_column(
        Numeric(14, 2),
        nullable=False,
    )
    currency: Mapped[str] = mapped_column(Text, nullable=False)
    source_msrp_value: Mapped[float] = mapped_column(
        Numeric(14, 2),
        nullable=False,
    )
    source_currency: Mapped[str] = mapped_column(Text, nullable=False)
    fx_rate_to_eur: Mapped[float] = mapped_column(
        Numeric(14, 8),
        nullable=False,
    )
    fx_rate_as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    fx_source: Mapped[str] = mapped_column(Text, nullable=False)
    tax_included: Mapped[bool] = mapped_column(Boolean, nullable=False)
    match_confidence: Mapped[float] = mapped_column(
        Numeric(5, 4),
        nullable=False,
    )
    match_status: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_snapshot_path: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    last_price_change_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    updated_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class PriceHistory(Base):
    """Compressed price time-series: each row = one price period.

    When a price changes, the current open period (valid_to_utc IS NULL)
    is closed and a new period is opened.  This gives queryable
    "from when to when was this (country, brand, model, trim) at price X"
    without scanning the full observations table.
    """

    __tablename__ = "price_history"
    __table_args__ = (
        Index(
            "ix_msrp_price_history_business_key",
            "country",
            "brand",
            "jato_model",
            "jato_trim",
        ),
        Index(
            "ix_msrp_price_history_open_period",
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "valid_to_utc",
        ),
        {"schema": "msrp"},
    )

    price_history_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    jato_model: Mapped[str] = mapped_column(Text, nullable=False)
    jato_trim: Mapped[str] = mapped_column(Text, nullable=False)
    msrp_value: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False)
    source_msrp_value: Mapped[float] = mapped_column(
        Numeric(14, 2),
        nullable=False,
    )
    source_currency: Mapped[str] = mapped_column(Text, nullable=False)
    valid_from_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    valid_to_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_confirmed_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    started_by_observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    ended_by_observation_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=True,
    )
    last_confirmed_by_observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ReviewCase(TimestampMixin, Base):
    __tablename__ = "review_cases"
    __table_args__ = (
        UniqueConstraint(
            "observation_id",
            name="uq_review_cases_observation_id",
        ),
        Index(
            "ix_review_cases_status_created",
            "review_status",
            "created_at_utc",
        ),
        Index("ix_review_cases_country_brand", "country", "brand"),
        Index("ix_review_cases_current_assignee", "current_assignee"),
        {"schema": "review"},
    )

    review_case_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    jato_model: Mapped[str] = mapped_column(Text, nullable=False)
    jato_trim: Mapped[str] = mapped_column(Text, nullable=False)
    jato_powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_model: Mapped[str] = mapped_column(Text, nullable=False)
    official_trim: Mapped[str] = mapped_column(Text, nullable=False)
    official_edition: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_powertrain: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    candidate_matches_json: Mapped[list[dict] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    match_confidence: Mapped[float] = mapped_column(
        Numeric(5, 4),
        nullable=False,
    )
    review_status: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_snapshot_path: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    current_assignee: Mapped[str | None] = mapped_column(Text, nullable=True)


class ReviewDecision(Base):
    __tablename__ = "review_decisions"
    __table_args__ = (
        Index(
            "ix_review_decisions_case_decided",
            "review_case_id",
            "decided_at_utc",
        ),
        Index("ix_review_decisions_observation", "observation_id"),
        {"schema": "review"},
    )

    review_decision_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    review_case_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("review.review_cases.review_case_id"),
        nullable=False,
    )
    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(Text, nullable=False)
    decided_official_model: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    decided_official_trim: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_by: Mapped[str] = mapped_column(Text, nullable=False)
    decided_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
