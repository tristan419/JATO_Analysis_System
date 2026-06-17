from __future__ import annotations

from datetime import date, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKeyConstraint,
    ForeignKey,
    Integer,
    Index,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
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


class CountryNewsDigest(TimestampMixin, Base):
    __tablename__ = "country_news_digests"
    __table_args__ = (
        UniqueConstraint(
            "country_code",
            name="uq_country_news_digests_country_code",
        ),
        Index(
            "ix_ops_country_news_digests_synced",
            "synced_at_utc",
        ),
        Index(
            "ix_ops_country_news_digests_published",
            "published_at_utc",
        ),
        {"schema": "ops"},
    )

    country_news_digest_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    country_label: Mapped[str] = mapped_column(Text, nullable=False)
    article_count: Mapped[int] = mapped_column(default=0, nullable=False)
    published_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    synced_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    headline: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    highlights_json: Mapped[list[str] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    summary_provider: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    summary_model: Mapped[str | None] = mapped_column(Text, nullable=True)
    auto_review_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    publish_tier: Mapped[str | None] = mapped_column(Text, nullable=True)
    publish_decision: Mapped[str | None] = mapped_column(Text, nullable=True)


class CountryNewsArticle(TimestampMixin, Base):
    __tablename__ = "country_news_articles"
    __table_args__ = (
        UniqueConstraint(
            "country_code",
            "source_url",
            name="uq_country_news_articles_country_url",
        ),
        Index(
            "ix_ops_country_news_articles_country_published",
            "country_code",
            "published_at_utc",
        ),
        Index(
            "ix_ops_country_news_articles_country_synced",
            "country_code",
            "synced_at_utc",
        ),
        Index(
            "ix_ops_country_news_articles_source_code",
            "source_code",
        ),
        {"schema": "ops"},
    )

    country_news_article_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    country_label: Mapped[str] = mapped_column(Text, nullable=False)
    source_code: Mapped[str] = mapped_column(Text, nullable=False)
    publisher: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    raw_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    tags_json: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    raw_payload_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    intelligence_provider: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    intelligence_model: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    auto_review_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    publish_tier: Mapped[str | None] = mapped_column(Text, nullable=True)
    publish_decision: Mapped[str | None] = mapped_column(Text, nullable=True)
    synced_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class VocSourceRun(TimestampMixin, Base):
    __tablename__ = "voc_source_runs"
    __table_args__ = (
        UniqueConstraint(
            "source_code",
            "collected_at_utc",
            name="uq_voc_source_runs_source_collected",
        ),
        Index(
            "ix_ops_voc_source_runs_country_collected",
            "country_code",
            "collected_at_utc",
        ),
        Index(
            "ix_ops_voc_source_runs_publish_tier",
            "publish_tier",
        ),
        {"schema": "ops"},
    )

    voc_source_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    country_label: Mapped[str] = mapped_column(Text, nullable=False)
    source_code: Mapped[str] = mapped_column(Text, nullable=False)
    site_name: Mapped[str] = mapped_column(Text, nullable=False)
    site_type: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str | None] = mapped_column(Text, nullable=True)
    taxonomy_profile: Mapped[str | None] = mapped_column(Text, nullable=True)
    collected_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    source_file_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_meta_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    landing_page_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    collection_strategy_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    taxonomy_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    auto_review_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    publish_tier: Mapped[str | None] = mapped_column(Text, nullable=True)
    publish_decision: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    publish_ready_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    errors_json: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)


class VocRawDocument(TimestampMixin, Base):
    __tablename__ = "voc_raw_documents"
    __table_args__ = (
        UniqueConstraint(
            "voc_source_run_id",
            "source_url",
            name="uq_voc_raw_documents_run_url",
        ),
        Index(
            "ix_ops_voc_raw_documents_country_collected",
            "country_code",
            "collected_at_utc",
        ),
        Index(
            "ix_ops_voc_raw_documents_publish_tier",
            "publish_tier",
        ),
        {"schema": "ops"},
    )

    voc_raw_document_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    voc_source_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ops.voc_source_runs.voc_source_run_id"),
        nullable=False,
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    country_label: Mapped[str] = mapped_column(Text, nullable=False)
    source_code: Mapped[str] = mapped_column(Text, nullable=False)
    site_name: Mapped[str] = mapped_column(Text, nullable=False)
    site_type: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    page_kind: Mapped[str | None] = mapped_column(Text, nullable=True)
    link_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    collected_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    auto_review_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    publish_tier: Mapped[str | None] = mapped_column(Text, nullable=True)
    publish_decision: Mapped[str | None] = mapped_column(Text, nullable=True)


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
        Index("ix_msrp_sources_tier", "tier"),
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
    tier: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
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


class JatoMsrpLink(TimestampMixin, Base):
    __tablename__ = "jato_msrp_links"
    __table_args__ = (
        UniqueConstraint(
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "jato_powertrain",
            "official_model",
            "official_trim",
            "official_edition",
            "official_powertrain",
            name="uq_jato_msrp_links_business_key",
        ),
        Index(
            "ix_msrp_jato_msrp_links_jato_key",
            "country",
            "brand",
            "jato_model",
            "jato_powertrain",
        ),
        Index(
            "ix_msrp_jato_msrp_links_official_key",
            "country",
            "brand",
            "official_model",
            "official_powertrain",
        ),
        Index("ix_msrp_jato_msrp_links_active", "is_active"),
        {"schema": "msrp"},
    )

    link_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    jato_model: Mapped[str] = mapped_column(Text, nullable=False)
    jato_trim: Mapped[str] = mapped_column(Text, nullable=False)
    jato_powertrain: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="",
    )
    official_model: Mapped[str] = mapped_column(Text, nullable=False)
    official_trim: Mapped[str] = mapped_column(Text, nullable=False)
    official_edition: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="",
    )
    official_powertrain: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="",
    )
    confidence: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=80,
    )
    link_source: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="manual",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class MatchOverride(TimestampMixin, Base):
    __tablename__ = "match_overrides"
    __table_args__ = (
        CheckConstraint(
            "valid_to_date IS NULL OR valid_to_date >= valid_from_date",
            name="ck_review_match_overrides_valid_window",
        ),
        UniqueConstraint(
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "jato_powertrain",
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
            "jato_powertrain",
        ),
        Index(
            "ix_review_match_overrides_lookup",
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "valid_from_date",
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
    jato_powertrain: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="",
    )
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
            "ix_engineering_config_import_batches_import_batch",
            "import_batch_id",
        ),
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
            "ix_engineering_config_variants_import_batch",
            "config_import_batch_id",
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


class ConfigBaseVariant(TimestampMixin, Base):
    __tablename__ = "base_variants"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "business_key",
            name="uq_config_base_variants_project_key",
        ),
        Index(
            "ix_engineering_base_variants_project_model",
            "project_id",
            "model",
        ),
        Index(
            "ix_engineering_base_variants_brand_model",
            "brand",
            "model",
        ),
        {"schema": "engineering"},
    )

    base_variant_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    project_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.config_projects.project_id"),
        nullable=False,
    )
    business_key: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    trim_name: Mapped[str] = mapped_column(Text, nullable=False)
    version_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    base_features_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    base_feature_labels_json: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    source_variant_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )
    market_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )


class ConfigMarketVariant(TimestampMixin, Base):
    __tablename__ = "market_variants"
    __table_args__ = (
        UniqueConstraint(
            "source_variant_id",
            name="uq_config_market_variants_source_variant",
        ),
        Index(
            "ix_engineering_market_variants_project_country",
            "project_id",
            "market_country",
        ),
        Index(
            "ix_engineering_market_variants_base_country",
            "base_variant_id",
            "market_country",
        ),
        {"schema": "engineering"},
    )

    market_variant_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    project_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.config_projects.project_id"),
        nullable=False,
    )
    base_variant_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.base_variants.base_variant_id"),
        nullable=False,
    )
    source_variant_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.config_variants.variant_id"),
        nullable=False,
    )
    external_row_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    market_country: Mapped[str] = mapped_column(Text, nullable=False)
    target_msrp: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    source_file_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    override_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )


class ConfigMarketFeatureOverride(TimestampMixin, Base):
    __tablename__ = "market_feature_overrides"
    __table_args__ = (
        CheckConstraint(
            "(CASE WHEN bool_value IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN number_value IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN text_value IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN json_value IS NOT NULL THEN 1 ELSE 0 END) = 1",
            name="ck_engineering_market_feature_overrides_single_value",
        ),
        UniqueConstraint(
            "market_variant_id",
            "feature_code",
            name="uq_config_market_feature_overrides_market_feature",
        ),
        Index(
            "ix_engineering_market_feature_overrides_project_feature",
            "project_id",
            "feature_code",
        ),
        Index(
            "ix_engineering_market_feature_overrides_market",
            "market_variant_id",
        ),
        Index(
            "ix_engineering_market_feature_overrides_source_variant",
            "source_variant_id",
        ),
        {"schema": "engineering"},
    )

    feature_override_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    project_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.config_projects.project_id"),
        nullable=False,
    )
    market_variant_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.market_variants.market_variant_id"),
        nullable=False,
    )
    source_variant_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering.config_variants.variant_id"),
        nullable=True,
    )
    feature_code: Mapped[str] = mapped_column(Text, nullable=False)
    feature_label: Mapped[str] = mapped_column(Text, nullable=False)
    value_type: Mapped[str] = mapped_column(Text, nullable=False)
    bool_value: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    number_value: Mapped[float | None] = mapped_column(
        Numeric(14, 4),
        nullable=True,
    )
    text_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_value: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    availability: Mapped[str | None] = mapped_column(Text, nullable=True)
    package_code: Mapped[str | None] = mapped_column(Text, nullable=True)


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
            "ix_msrp_observations_scrape_batch",
            "scrape_batch_id",
        ),
        Index(
            "ix_msrp_observations_source",
            "source_id",
        ),
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
    source_context_json: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
    )


class FinanceObservation(TimestampMixin, Base):
    __tablename__ = "finance_observations"
    __table_args__ = (
        Index(
            "ix_msrp_finance_observations_observation",
            "observation_id",
        ),
        Index(
            "ix_msrp_finance_observations_scrape_batch",
            "scrape_batch_id",
        ),
        Index(
            "ix_msrp_finance_observations_country_brand_model",
            "country",
            "brand",
            "jato_model",
        ),
        Index(
            "ix_msrp_finance_observations_semantics",
            "price_semantics",
            "finance_type",
        ),
        {"schema": "msrp"},
    )

    finance_observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id", ondelete="CASCADE"),
        nullable=False,
    )
    scrape_batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.scrape_batches.scrape_batch_id", ondelete="CASCADE"),
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
    price_semantics: Mapped[str] = mapped_column(Text, nullable=False)
    finance_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    monthly_payment: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    monthly_payment_eur: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    down_payment: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    down_payment_eur: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    down_payment_pct: Mapped[float | None] = mapped_column(
        Numeric(8, 4),
        nullable=True,
    )
    term_months: Mapped[int | None] = mapped_column(Integer, nullable=True)
    apr: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)
    effective_apr: Mapped[float | None] = mapped_column(
        Numeric(8, 4),
        nullable=True,
    )
    balloon_payment: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    balloon_payment_eur: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    total_credit_cost: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    total_credit_cost_eur: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    total_amount_payable: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    total_amount_payable_eur: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    annual_mileage_limit: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    offer_valid_until: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
    )
    subsidy_amount: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    subsidy_amount_eur: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    net_price_after_subsidy: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    net_price_after_subsidy_eur: Mapped[float | None] = mapped_column(
        Numeric(14, 2),
        nullable=True,
    )
    currency: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finance_context_json: Mapped[dict | None] = mapped_column(
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
            "jato_powertrain",
            name="uq_current_prices_business_key",
        ),
        Index("ix_msrp_current_prices_country_brand", "country", "brand"),
        Index("ix_msrp_current_prices_jato_model", "jato_model"),
        Index(
            "ix_msrp_current_prices_effective_observation",
            "effective_observation_id",
        ),
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
    jato_powertrain: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="",
    )
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
        CheckConstraint(
            "valid_to_utc IS NULL OR valid_to_utc > valid_from_utc",
            name="ck_msrp_price_history_valid_window",
        ),
        Index(
            "ix_msrp_price_history_business_key",
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "jato_powertrain",
        ),
        Index(
            "ix_msrp_price_history_started_by_observation",
            "started_by_observation_id",
        ),
        Index(
            "ix_msrp_price_history_ended_by_observation",
            "ended_by_observation_id",
        ),
        Index(
            "ix_msrp_price_history_last_confirmed_observation",
            "last_confirmed_by_observation_id",
        ),
        Index(
            "ix_msrp_price_history_open_period",
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "jato_powertrain",
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
    jato_powertrain: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="",
    )
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
            "review_case_id",
            "observation_id",
            name="uq_review_cases_case_observation_pair",
        ),
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
        ForeignKeyConstraint(
            ["review_case_id", "observation_id"],
            [
                "review.review_cases.review_case_id",
                "review.review_cases.observation_id",
            ],
            name="fk_review_decisions_case_observation_pair",
        ),
        Index(
            "ix_review_decisions_case_observation",
            "review_case_id",
            "observation_id",
        ),
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


# ── engineering_config schema ──────────────────────────────────────


class FeatureCatalog(Base):
    __tablename__ = "feature_catalog"
    __table_args__ = (
        UniqueConstraint(
            "category",
            "standard_field_name",
            name="uq_feature_catalog_category_field",
        ),
        Index("ix_feature_catalog_category", "category"),
        Index("ix_feature_catalog_feature_code", "feature_code"),
        {"schema": "engineering_config"},
    )

    feature_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    standard_field_name: Mapped[str] = mapped_column(Text, nullable=False)
    feature_code: Mapped[str] = mapped_column(Text, nullable=False)
    unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    data_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="string",
    )
    aliases: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    display_order: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class VehicleTrim(Base):
    __tablename__ = "vehicle_trims"
    __table_args__ = (
        Index("ix_vehicle_trims_brand", "brand"),
        Index("ix_vehicle_trims_model", "model_name"),
        Index("ix_vehicle_trims_status", "status"),
        Index("ix_vehicle_trims_identity_key", "identity_key"),
        {"schema": "engineering_config"},
    )

    trim_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    source_upload_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ops.import_batches.import_batch_id"),
        nullable=True,
    )
    identity_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    material_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    vehicle_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    market: Mapped[str | None] = mapped_column(Text, nullable=True)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    trim_name: Mapped[str] = mapped_column(Text, nullable=False)
    full_trim_name: Mapped[str] = mapped_column(Text, nullable=False)
    energy_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    drivetrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    engine: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_year: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="active",
    )
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


class TrimFeatureValue(Base):
    __tablename__ = "trim_feature_values"
    __table_args__ = (
        UniqueConstraint(
            "trim_id",
            "feature_id",
            name="uq_trim_feature_values_trim_feature",
        ),
        Index("ix_trim_feature_values_trim", "trim_id"),
        Index("ix_trim_feature_values_feature", "feature_id"),
        Index("ix_trim_feature_values_availability", "availability"),
        {"schema": "engineering_config"},
    )

    value_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    trim_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering_config.vehicle_trims.trim_id"),
        nullable=False,
    )
    feature_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("engineering_config.feature_catalog.feature_id"),
        nullable=False,
    )
    raw_value: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    availability: Mapped[str] = mapped_column(Text, nullable=False)
    unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_row: Mapped[int] = mapped_column(Integer, nullable=False)
    source_column: Mapped[str] = mapped_column(Text, nullable=False)
    source_upload_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ops.import_batches.import_batch_id"),
        nullable=True,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)
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


class ConfigAuditLog(Base):
    __tablename__ = "config_audit_log"
    __table_args__ = (
        Index("ix_config_audit_log_entity", "entity_type", "entity_id"),
        Index("ix_config_audit_log_changed_at", "changed_at_utc"),
        {"schema": "engineering_config"},
    )

    audit_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    field_name: Mapped[str] = mapped_column(Text, nullable=False)
    old_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    changed_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    changed_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    source: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="manual",
    )
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)


class ConfigVersion(Base):
    __tablename__ = "config_versions"
    __table_args__ = (
        Index("ix_config_versions_identity_key", "identity_key"),
        Index("ix_config_versions_status", "status"),
        Index("ix_config_versions_trim_created", "trim_id", "created_at_utc"),
        {"schema": "engineering_config"},
    )
    version_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    trim_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("engineering_config.vehicle_trims.trim_id"), nullable=False)
    identity_key: Mapped[str] = mapped_column(Text, nullable=False)
    material_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    vehicle_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    market: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_year: Mapped[str | None] = mapped_column(Text, nullable=True)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    trim_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="draft")
    version_no: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    source_upload_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey("ops.import_batches.import_batch_id"), nullable=True)
    parent_version_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey("engineering_config.config_versions.version_id"), nullable=True)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    published_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


# ── Auth ──


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("username", name="uq_users_username"),
        Index("ix_users_role", "role"),
        {"schema": "auth"},
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    username: Mapped[str] = mapped_column(Text, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(
        Text, nullable=False, default="viewer"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )
    primary_country_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    secondary_country_codes: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    preferred_landing_page: Mapped[str | None] = mapped_column(Text, nullable=True)
    email: Mapped[str | None] = mapped_column(Text, nullable=True)
    oauth_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    oauth_subject: Mapped[str | None] = mapped_column(Text, nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
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


class RoleUpgradeRequest(Base):
    __tablename__ = "role_upgrade_requests"
    __table_args__ = (
        Index("ix_role_upgrade_requests_status", "status"),
        Index("ix_role_upgrade_requests_user", "user_id"),
        {"schema": "auth"},
    )
    request_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    user_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("auth.users.id"), nullable=False
    )
    username: Mapped[str] = mapped_column(Text, nullable=False)
    current_role: Mapped[str] = mapped_column(Text, nullable=False)
    requested_role: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    reviewed_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ── ordering schema ──────────────────────────────────────────────────


class MaterialBaselineVersion(Base):
    __tablename__ = "material_baseline_version"
    __table_args__ = {"schema": "ordering"}

    baseline_version_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    source_upload_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    source_file_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_file_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    baseline_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="published")
    published_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class MaterialSkuMaster(TimestampMixin, Base):
    __tablename__ = "material_sku_master"
    __table_args__ = (
        UniqueConstraint(
            "baseline_version_id", "material_code",
            name="uq_ordering_sku_baseline_material",
        ),
        Index("ix_ordering_sku_lookup", "brand", "model_name", "version", "exterior_color_code"),
        Index("ix_ordering_sku_material_code", "material_code"),
        Index("ix_ordering_sku_active", "is_active", "lifecycle_status"),
        {"schema": "ordering"},
    )

    material_sku_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    baseline_version_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ordering.material_baseline_version.baseline_version_id"),
        nullable=False,
    )
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    exterior_color_name: Mapped[str] = mapped_column(Text, nullable=False)
    exterior_color_code: Mapped[str] = mapped_column(Text, nullable=False)
    exterior_color_type: Mapped[str] = mapped_column(Text, nullable=False)
    colour_hex: Mapped[str | None] = mapped_column(Text, nullable=True)
    colour_code_confirmed: Mapped[bool] = mapped_column(Boolean, default=True)
    colour_tier: Mapped[str] = mapped_column(Text, default="single", comment="single | dual | special")
    interior_color_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    interior_colour_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    interior_package: Mapped[str | None] = mapped_column(Text, nullable=True)
    edition_tag: Mapped[str | None] = mapped_column(Text, nullable=True)
    bom_template: Mapped[str | None] = mapped_column(Text, nullable=True)
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    lifecycle_status: Mapped[str] = mapped_column(Text, nullable=False, default="active")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    effective_from_month: Mapped[str | None] = mapped_column(Text, nullable=True)
    effective_to_month: Mapped[str | None] = mapped_column(Text, nullable=True)
    remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    source_sheet_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_row_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_payload_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)


class CountryPaymentTermMaster(TimestampMixin, Base):
    __tablename__ = "country_payment_term_master"
    __table_args__ = (
        Index(
            "uq_ordering_country_payment_active",
            "country_code",
            unique=True,
            postgresql_where=text("is_active = true"),
        ),
        {"schema": "ordering"},
    )

    country_payment_term_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    country_name: Mapped[str] = mapped_column(Text, nullable=False)
    payment_term_code: Mapped[str] = mapped_column(Text, nullable=False)
    payment_method: Mapped[str] = mapped_column(Text, nullable=False)
    lc_days: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    valid_from_month: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        comment="YYYY-MM when this term became effective",
    )
    valid_to_month: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        comment="YYYY-MM when this term ended; NULL = still effective",
    )
    remark: Mapped[str | None] = mapped_column(Text, nullable=True)


class PaymentTermPriceRule(TimestampMixin, Base):
    __tablename__ = "payment_term_price_rule"
    __table_args__ = (
        Index(
            "uq_ordering_payment_term_rule_active",
            "payment_term_code",
            unique=True,
            postgresql_where=text("is_active = true"),
        ),
        {"schema": "ordering"},
    )

    payment_term_rule_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    payment_term_code: Mapped[str] = mapped_column(Text, nullable=False)
    payment_method: Mapped[str] = mapped_column(Text, nullable=False)
    lc_days: Mapped[int] = mapped_column(Integer, nullable=False)
    fob_adjustment_eur: Mapped[float] = mapped_column(
        Numeric(12, 2), nullable=False, default=0
    )
    adjustment_rate: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class BrandColourSurchargeRule(TimestampMixin, Base):
    __tablename__ = "brand_colour_surcharge_rule"
    __table_args__ = (
        Index(
            "uq_ordering_colour_surcharge_active",
            "brand", "colour_type",
            unique=True,
            postgresql_where=text("is_active = true"),
        ),
        {"schema": "ordering"},
    )

    colour_surcharge_rule_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    colour_type: Mapped[str] = mapped_column(Text, nullable=False)
    surcharge_eur: Mapped[float] = mapped_column(
        Numeric(12, 2), nullable=False, default=0
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class CountrySkuFobResolved(TimestampMixin, Base):
    __tablename__ = "country_sku_fob_resolved"
    __table_args__ = (
        Index(
            "uq_ordering_country_sku_fob_active",
            "country_code", "material_code", "payment_term_code",
            unique=True,
            postgresql_where=text("is_active = true"),
        ),
        {"schema": "ordering"},
    )

    country_sku_fob_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    baseline_version_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ordering.material_baseline_version.baseline_version_id"),
        nullable=False,
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    payment_term_code: Mapped[str] = mapped_column(Text, nullable=False)
    base_fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    payment_term_adjustment_eur: Mapped[float | None] = mapped_column(
        Numeric(12, 2), nullable=True,
    )
    colour_surcharge_eur: Mapped[float | None] = mapped_column(
        Numeric(12, 2), nullable=True,
    )
    uploaded_fob_eur: Mapped[float | None] = mapped_column(
        Numeric(12, 2), nullable=True,
    )
    final_fob_eur: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    fob_source_country_code: Mapped[str | None] = mapped_column(
        Text, nullable=True,
    )
    fob_source_mode: Mapped[str] = mapped_column(
        Text, nullable=False, default="explicit_price_by_payment_term",
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class CountryMaterialFinance(TimestampMixin, Base):
    __tablename__ = "country_material_finance"
    __table_args__ = (
        Index(
            "uq_ordering_country_material_finance_active",
            "country_code", "material_code",
            unique=True,
            postgresql_where=text("is_active = true"),
        ),
        Index("ix_ordering_country_material_finance_country", "country_code"),
        Index("ix_ordering_country_material_finance_material", "material_code"),
        {"schema": "ordering"},
    )

    country_material_finance_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    retail_price_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    wholesale_price_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    dealer_price_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    cost_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    margin_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    margin_rate: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    vehicle_margin_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    vehicle_margin_rate: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    vehicle_profit_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    vehicle_profit_rate: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    fob_delta_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    margin_delta_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    memo: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_mode: Mapped[str] = mapped_column(Text, nullable=False, default="manual")
    source_payload_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class CountryFobSourceMapping(TimestampMixin, Base):
    __tablename__ = "country_fob_source_mapping"
    __table_args__ = (
        Index(
            "uq_ordering_country_fob_source_mapping_active",
            "target_country_code", "target_payment_term_code",
            unique=True,
            postgresql_where=text("is_active = true"),
        ),
        {"schema": "ordering"},
    )

    country_fob_source_mapping_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    target_country_code: Mapped[str] = mapped_column(Text, nullable=False)
    target_payment_term_code: Mapped[str] = mapped_column(Text, nullable=False)
    source_country_code: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    remark: Mapped[str | None] = mapped_column(Text, nullable=True)


class OrderQuantityCell(TimestampMixin, Base):
    __tablename__ = "order_quantity_cell"
    __table_args__ = (
        UniqueConstraint(
            "country_code", "order_year", "order_month", "material_code",
            name="uq_ordering_quantity_cell",
        ),
        CheckConstraint(
            "order_month BETWEEN 1 AND 12",
            name="ck_ordering_quantity_month",
        ),
        CheckConstraint(
            "quantity >= 0",
            name="ck_ordering_quantity_non_negative",
        ),
        {"schema": "ordering"},
    )

    order_quantity_cell_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    order_year: Mapped[int] = mapped_column(Integer, nullable=False)
    order_month: Mapped[int] = mapped_column(Integer, nullable=False)
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fob_eur: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    row_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class PiOrderHeader(TimestampMixin, Base):
    __tablename__ = "pi_order_header"
    __table_args__ = (
        UniqueConstraint("pi_code", name="uq_pi_order_header_pi_code"),
        UniqueConstraint(
            "ordering_account_code", "order_month", "pi_sequence_no",
            name="uq_pi_order_header_account_month_seq",
        ),
        Index("ix_pi_order_header_country_month", "country_code", "order_month"),
        Index("ix_pi_order_header_ordering_account", "ordering_account_code", "order_month"),
        Index("ix_pi_order_header_status", "status"),
        {"schema": "ordering"},
    )

    pi_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    pi_code: Mapped[str] = mapped_column(Text, nullable=False)
    official_pi_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    country_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    ordering_account_code: Mapped[str] = mapped_column(Text, nullable=False)
    ordering_account_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    market_country_codes: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    shipment_batch_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    port_of_discharge: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    order_month: Mapped[str] = mapped_column(Text, nullable=False)
    pi_sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    shipping_schedule_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    feishu_tracking_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    ship_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    etd: Mapped[date | None] = mapped_column(Date, nullable=True)
    eta: Mapped[date | None] = mapped_column(Date, nullable=True)
    actual_departure_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    actual_arrival_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    ready_for_pickup_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="draft")
    remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class PiOrderLine(TimestampMixin, Base):
    __tablename__ = "pi_order_line"
    __table_args__ = (
        UniqueConstraint("pi_line_code", name="uq_pi_order_line_code"),
        UniqueConstraint(
            "pi_id", "line_sequence_no",
            name="uq_pi_order_line_pi_line_seq",
        ),
        CheckConstraint("quantity >= 0", name="ck_pi_order_line_quantity_non_negative"),
        Index("ix_pi_order_line_pi_id", "pi_id"),
        Index("ix_pi_order_line_material_code", "material_code"),
        {"schema": "ordering"},
    )

    pi_line_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    pi_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ordering.pi_order_header.pi_id", ondelete="CASCADE"),
        nullable=False,
    )
    pi_code: Mapped[str] = mapped_column(Text, nullable=False)
    pi_line_code: Mapped[str] = mapped_column(Text, nullable=False)
    line_sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    material_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    bom: Mapped[str | None] = mapped_column(Text, nullable=True)
    brand: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    exterior_color_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    exterior_color_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    interior_color_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    interior_colour_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    amount_eur: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class PiOrderLineAllocation(TimestampMixin, Base):
    __tablename__ = "pi_order_line_allocation"
    __table_args__ = (
        UniqueConstraint(
            "pi_line_id", "market_country_code",
            name="uq_pi_order_line_alloc_line_country",
        ),
        CheckConstraint("quantity >= 0", name="ck_pi_order_line_alloc_quantity_non_negative"),
        Index("ix_pi_order_line_alloc_line", "pi_line_id"),
        Index("ix_pi_order_line_alloc_market_month", "market_country_code", "order_year", "order_month"),
        Index("ix_pi_order_line_alloc_material", "material_code"),
        {"schema": "ordering"},
    )

    pi_line_allocation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    pi_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ordering.pi_order_header.pi_id", ondelete="CASCADE"),
        nullable=False,
    )
    pi_line_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ordering.pi_order_line.pi_line_id", ondelete="CASCADE"),
        nullable=False,
    )
    pi_code: Mapped[str] = mapped_column(Text, nullable=False)
    pi_line_code: Mapped[str] = mapped_column(Text, nullable=False)
    market_country_code: Mapped[str] = mapped_column(Text, nullable=False)
    order_year: Mapped[int] = mapped_column(Integer, nullable=False)
    order_month: Mapped[int] = mapped_column(Integer, nullable=False)
    material_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class PiVehicleUnit(TimestampMixin, Base):
    __tablename__ = "pi_vehicle_unit"
    __table_args__ = (
        UniqueConstraint("car_code", name="uq_pi_vehicle_unit_car_code"),
        UniqueConstraint("pi_code", "car_code", name="uq_pi_vehicle_unit_pi_car"),
        Index(
            "uq_pi_vehicle_unit_vin_not_null",
            "vin",
            unique=True,
            postgresql_where=text("vin IS NOT NULL AND vin <> ''"),
        ),
        Index("ix_pi_vehicle_unit_pi_code", "pi_code"),
        Index("ix_pi_vehicle_unit_line_code", "pi_line_code"),
        Index("ix_pi_vehicle_unit_country_status", "country_code", "allocation_status", "logistics_status"),
        Index("ix_pi_vehicle_unit_eta", "eta"),
        Index("ix_pi_vehicle_unit_ready", "ready_for_pickup_date"),
        {"schema": "ordering"},
    )

    vehicle_unit_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    pi_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ordering.pi_order_header.pi_id", ondelete="CASCADE"),
        nullable=False,
    )
    pi_line_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("ordering.pi_order_line.pi_line_id", ondelete="CASCADE"),
        nullable=False,
    )
    pi_code: Mapped[str] = mapped_column(Text, nullable=False)
    pi_line_code: Mapped[str] = mapped_column(Text, nullable=False)
    car_code: Mapped[str] = mapped_column(Text, nullable=False)
    vin: Mapped[str | None] = mapped_column(Text, nullable=True)
    material_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    bom: Mapped[str | None] = mapped_column(Text, nullable=True)
    brand: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    exterior_color_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    exterior_color_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    interior_color_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    interior_colour_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    production_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    etd: Mapped[date | None] = mapped_column(Date, nullable=True)
    eta: Mapped[date | None] = mapped_column(Date, nullable=True)
    actual_departure_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    actual_arrival_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    ready_for_pickup_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    ship_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    dealer_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    dealer_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    customer_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    allocation_status: Mapped[str] = mapped_column(Text, nullable=False, default="unallocated")
    logistics_status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class MaterialSkuRemarkHistory(Base):
    __tablename__ = "material_sku_remark_history"
    __table_args__ = (
        Index(
            "ix_ordering_remark_history_code_updated",
            "material_code", "updated_at_utc",
        ),
        {"schema": "ordering"},
    )

    remark_history_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    old_remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class MaterialLifecycle(Base):
    """Country-level material validity timeline.

    Tracks which material_code was active for a given product identity
    in a specific country during a time window.  Supports material
    switch-over visibility and historical quantity lookups.
    """

    __tablename__ = "material_lifecycle"
    __table_args__ = (
        Index(
            "ix_ordering_lifecycle_country_product",
            "country_code", "product_identity",
        ),
        Index(
            "ix_ordering_lifecycle_material_code",
            "material_code",
        ),
        {"schema": "ordering"},
    )

    lifecycle_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    product_identity: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="brand|model_name|version|powertrain",
    )
    valid_from: Mapped[date] = mapped_column(Date, nullable=False)
    valid_to: Mapped[date | None] = mapped_column(
        Date, nullable=True,
        comment="NULL = currently active",
    )
    lifecycle_status: Mapped[str] = mapped_column(
        Text, nullable=False, default="active",
        comment="active | phased_out | replaced",
    )
    replaced_by_code: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        comment="Replacement material code",
    )
    remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"MaterialLifecycle({self.country_code} {self.material_code} "
            f"{self.valid_from} → {self.valid_to or 'present'})"
        )


class PaymentTermAuditLog(Base):
    """Immutable audit trail for payment term changes."""

    __tablename__ = "payment_term_audit_log"
    __table_args__ = (
        Index("ix_ordering_pt_audit_country", "country_code"),
        {"schema": "ordering"},
    )

    audit_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="create | update | close | correct",
    )
    old_payment_term_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_payment_term_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    old_valid_from: Mapped[str | None] = mapped_column(Text, nullable=True)
    old_valid_to: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_valid_from: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_valid_to: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    impacted_order_months: Mapped[int | None] = mapped_column(Integer, nullable=True)
    actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class FobResolvedHistory(Base):
    """Immutable audit trail for every FOB value change during baseline publish.

    Written BEFORE the upsert so old/new values are captured in one row.
    """

    __tablename__ = "fob_resolved_history"
    __table_args__ = (
        Index("ix_ordering_fob_history_code", "material_code"),
        Index("ix_ordering_fob_history_country_code", "country_code", "material_code"),
        {"schema": "ordering"},
    )

    fob_history_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4,
    )
    country_sku_fob_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), nullable=False,
    )
    baseline_version_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), nullable=True,
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    payment_term_code: Mapped[str] = mapped_column(Text, nullable=False)
    old_uploaded_fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    new_uploaded_fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    old_final_fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    new_final_fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    changed_by: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="trigger name, e.g. publish_baseline",
    )
    changed_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )


class QuantityCellHistory(Base):
    """Immutable audit trail for every order quantity cell edit."""

    __tablename__ = "quantity_cell_history"
    __table_args__ = (
        Index(
            "ix_ordering_qty_history_cell",
            "country_code", "order_year", "order_month", "material_code",
        ),
        {"schema": "ordering"},
    )

    quantity_history_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4,
    )
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    order_year: Mapped[int] = mapped_column(Integer, nullable=False)
    order_month: Mapped[int] = mapped_column(Integer, nullable=False)
    material_code: Mapped[str] = mapped_column(Text, nullable=False)
    old_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    new_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    old_fob_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    new_fob_eur: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    changed_by: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="username who made the change",
    )
    changed_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )


# ── Lease Comparison ───────────────────────────────────────────────


class LeaseOffer(TimestampMixin, Base):
    """A single lease offer card — Private/Fleet/Financial leasing."""

    __tablename__ = "lease_offers"
    __table_args__ = (
        Index("ix_leasing_offer_country", "country_code"),
        Index("ix_leasing_offer_brand_model", "brand", "model_name"),
        Index("ix_leasing_offer_status", "status"),
        {"schema": "leasing"},
    )

    offer_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    country_code: Mapped[str] = mapped_column(Text, nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False, default="EUR")
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    powertrain: Mapped[str | None] = mapped_column(Text, nullable=True)
    segment: Mapped[str | None] = mapped_column(Text, nullable=True)
    lease_type: Mapped[str] = mapped_column(Text, nullable=False, default="private")
    provider: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── FX ──
    fx_rate_to_eur: Mapped[float | None] = mapped_column(Numeric(14, 8), nullable=True)
    fx_rate_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    fx_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    fx_locked: Mapped[bool] = mapped_column(Boolean, default=True)

    # ── Financials (original currency) ──
    monthly_payment: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    monthly_payment_eur: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    effective_monthly_eur: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    down_payment: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    down_payment_eur: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    upfront_amount: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    upfront_treatment: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Lease terms ──
    term_months: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mileage_per_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cap_cost: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    cap_cost_eur: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    residual_value: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    residual_value_eur: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    residual_value_percent: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)
    apr_percent: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)
    money_factor: Mapped[float | None] = mapped_column(Numeric(12, 8), nullable=True)
    apr_source: Mapped[str | None] = mapped_column(Text, nullable=True, default="manual")

    # ── Inclusions & flags ──
    rv_guaranteed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    service_included: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    insurance_included: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    tyre_included: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    vat_included: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    deposit_required: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    deposit_refundable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # ── Meta ──
    status: Mapped[str] = mapped_column(Text, nullable=False, default="draft")
    source_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    expiry_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    total_contract_cost_eur: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    risk_level: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_version: Mapped[int] = mapped_column(Integer, default=1)


class LeaseOfferVersion(TimestampMixin, Base):
    """Immutable snapshot of a lease offer before modification."""

    __tablename__ = "lease_offer_versions"
    __table_args__ = (
        Index("ix_leasing_version_offer", "offer_id"),
        {"schema": "leasing"},
    )

    version_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    offer_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("leasing.lease_offers.offer_id", ondelete="CASCADE"), nullable=False,
    )
    version_no: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    change_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    changed_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class LeaseCompareSet(TimestampMixin, Base):
    """Named set of selected offer IDs for comparison."""

    __tablename__ = "lease_compare_sets"
    __table_args__ = (
        Index("ix_leasing_compare_country", "country_code"),
        {"schema": "leasing"},
    )

    compare_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    country_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected_offer_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
