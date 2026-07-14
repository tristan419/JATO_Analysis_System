from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class GovernanceTimestampMixin:
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


class MsrpMonitoringTarget(GovernanceTimestampMixin, Base):
    __tablename__ = "monitoring_targets"
    __table_args__ = (
        UniqueConstraint(
            "target_key",
            name="uq_msrp_monitoring_targets_target_key",
        ),
        CheckConstraint(
            "roster_type IN ('country_top30', 'manual', 'future_roster')",
            name="ck_msrp_monitoring_targets_roster_type",
        ),
        CheckConstraint(
            "monitoring_status IN "
            "('pending', 'active', 'degraded', 'manual_evidence_required', "
            "'paused', 'retired')",
            name="ck_msrp_monitoring_targets_status",
        ),
        CheckConstraint(
            "roster_rank IS NULL OR roster_rank > 0",
            name="ck_msrp_monitoring_targets_roster_rank",
        ),
        CheckConstraint(
            "row_version > 0",
            name="ck_msrp_monitoring_targets_row_version",
        ),
        Index(
            "ix_msrp_monitoring_targets_country_brand",
            "country",
            "brand",
        ),
        Index(
            "ix_msrp_monitoring_targets_status_rank",
            "monitoring_status",
            "roster_rank",
        ),
        {"schema": "msrp"},
    )

    target_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    target_key: Mapped[str] = mapped_column(Text, nullable=False)
    country: Mapped[str] = mapped_column(Text, nullable=False)
    brand: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    trim_scope: Mapped[str | None] = mapped_column(Text, nullable=True)
    powertrain_scope: Mapped[str | None] = mapped_column(Text, nullable=True)
    roster_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="manual",
        server_default=text("'manual'"),
    )
    roster_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    monitoring_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="pending",
        server_default=text("'pending'"),
    )
    active_source_version_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "msrp.source_versions.source_version_id",
            name="fk_msrp_monitoring_targets_active_source_version",
            use_alter=True,
        ),
        nullable=True,
    )
    fallback_source_version_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "msrp.source_versions.source_version_id",
            name="fk_msrp_monitoring_targets_fallback_source_version",
            use_alter=True,
        ),
        nullable=True,
    )
    schedule_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    owner: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )


class MsrpGovernanceGateDecision(Base):
    __tablename__ = "governance_gate_decisions"
    __table_args__ = (
        CheckConstraint(
            "schema_version = '1.0'",
            name="ck_msrp_governance_gate_decisions_schema_version",
        ),
        Index(
            "ix_msrp_governance_gate_decisions_target_evaluated",
            "target_id",
            "evaluated_at_utc",
        ),
        Index(
            "ix_msrp_governance_gate_decisions_observation",
            "observation_id",
        ),
        {"schema": "msrp"},
    )

    gate_decision_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    schema_version: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="1.0",
        server_default=text("'1.0'"),
    )
    target_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.monitoring_targets.target_id"),
        nullable=False,
    )
    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    source_gate_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
    mapping_gate_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
    fx_gate_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    eligible_for_local_materialization: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
    )
    eligible_for_normalized_materialization: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
    )
    evaluation_context_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    evaluated_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MsrpSourceVersion(GovernanceTimestampMixin, Base):
    __tablename__ = "source_versions"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "version_number",
            name="uq_msrp_source_versions_source_number",
        ),
        UniqueConstraint(
            "source_id",
            "profile_sha256",
            name="uq_msrp_source_versions_source_profile_sha256",
        ),
        CheckConstraint(
            "version_number > 0",
            name="ck_msrp_source_versions_version_number",
        ),
        CheckConstraint(
            "version_status IN "
            "('draft', 'validated', 'dryrun_passed', 'approved', 'published', "
            "'superseded', 'rejected', 'rolled_back')",
            name="ck_msrp_source_versions_status",
        ),
        CheckConstraint(
            "valid_until IS NULL OR valid_from IS NULL OR valid_until >= valid_from",
            name="ck_msrp_source_versions_validity",
        ),
        Index(
            "ix_msrp_source_versions_target_status",
            "target_id",
            "version_status",
        ),
        Index(
            "ix_msrp_source_versions_source_status",
            "source_id",
            "version_status",
        ),
        {"schema": "msrp"},
    )

    source_version_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    source_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.sources.source_id"),
        nullable=False,
    )
    target_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.monitoring_targets.target_id"),
        nullable=False,
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    profile_json: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    profile_yaml: Mapped[str] = mapped_column(Text, nullable=False)
    profile_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_refs_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    extractor_name: Mapped[str] = mapped_column(Text, nullable=False)
    extractor_type: Mapped[str] = mapped_column(Text, nullable=False)
    extractor_version: Mapped[str] = mapped_column(Text, nullable=False)
    semantic_lane: Mapped[str] = mapped_column(Text, nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False)
    tax_mode: Mapped[str] = mapped_column(Text, nullable=False)
    valid_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_until: Mapped[date | None] = mapped_column(Date, nullable=True)
    previous_version_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.source_versions.source_version_id"),
        nullable=True,
    )
    validation_summary_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    dryrun_summary_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    replay_summary_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    conflict_summary_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    gate_result_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    version_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="draft",
        server_default=text("'draft'"),
    )
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    approved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    published_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class MsrpSourceEvidenceAsset(Base):
    __tablename__ = "source_evidence_assets"
    __table_args__ = (
        UniqueConstraint(
            "target_id",
            "sha256",
            name="uq_msrp_source_evidence_assets_target_sha256",
        ),
        CheckConstraint(
            "evidence_type IN "
            "('official_url', 'uploaded_pdf', 'downloaded_pdf', "
            "'html_snapshot', 'api_snapshot', 'screenshot')",
            name="ck_msrp_source_evidence_assets_type",
        ),
        CheckConstraint(
            "lifecycle_state IN ('active', 'superseded', 'rejected')",
            name="ck_msrp_source_evidence_assets_state",
        ),
        CheckConstraint(
            "size_bytes IS NULL OR size_bytes >= 0",
            name="ck_msrp_source_evidence_assets_size",
        ),
        CheckConstraint(
            "page_count IS NULL OR page_count > 0",
            name="ck_msrp_source_evidence_assets_pages",
        ),
        CheckConstraint(
            "valid_until IS NULL OR valid_from IS NULL OR valid_until >= valid_from",
            name="ck_msrp_source_evidence_assets_validity",
        ),
        Index(
            "ix_msrp_source_evidence_assets_target_captured",
            "target_id",
            "captured_at_utc",
        ),
        Index(
            "ix_msrp_source_evidence_assets_source",
            "source_id",
        ),
        {"schema": "msrp"},
    )

    evidence_asset_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    target_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.monitoring_targets.target_id"),
        nullable=True,
    )
    source_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.sources.source_id"),
        nullable=True,
    )
    repair_case_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "msrp.repair_cases.case_id",
            name="fk_msrp_source_evidence_assets_repair_case",
            use_alter=True,
        ),
        nullable=True,
    )
    evidence_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    redirect_chain_json: Mapped[list[str] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    official_domain_verified: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    filename: Mapped[str | None] = mapped_column(Text, nullable=True)
    mime_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    mime_signature: Mapped[str | None] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    sha256: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    document_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_until: Mapped[date | None] = mapped_column(Date, nullable=True)
    page_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    text_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    semantic_lane: Mapped[str] = mapped_column(Text, nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="active",
        server_default=text("'active'"),
    )
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MsrpObservationEvidenceLink(Base):
    __tablename__ = "observation_evidence_links"
    __table_args__ = (
        UniqueConstraint(
            "observation_id",
            "evidence_asset_id",
            "evidence_role",
            name="uq_msrp_observation_evidence_links_asset_role",
        ),
        CheckConstraint(
            "evidence_role IN ('raw_payload', 'price_page', 'supporting')",
            name="ck_msrp_observation_evidence_links_role",
        ),
        CheckConstraint(
            "evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_msrp_observation_evidence_links_sha256",
        ),
        Index(
            "ix_msrp_observation_evidence_links_observation_role",
            "observation_id",
            "evidence_role",
        ),
        Index(
            "ix_msrp_observation_evidence_links_evidence_asset",
            "evidence_asset_id",
        ),
        Index(
            "ix_msrp_observation_evidence_links_source_version",
            "source_version_id",
        ),
        {"schema": "msrp"},
    )

    observation_evidence_link_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    evidence_asset_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.source_evidence_assets.evidence_asset_id"),
        nullable=False,
    )
    source_version_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.source_versions.source_version_id"),
        nullable=False,
    )
    evidence_role: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    linked_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    evidence_asset: Mapped["MsrpSourceEvidenceAsset"] = relationship(
        "MsrpSourceEvidenceAsset",
        lazy="joined",
        viewonly=True,
    )


class MsrpEvidenceUploadSession(GovernanceTimestampMixin, Base):
    __tablename__ = "evidence_upload_sessions"
    __table_args__ = (
        CheckConstraint(
            "upload_status IN "
            "('initiated', 'uploading', 'completed', 'failed', 'expired')",
            name="ck_msrp_evidence_upload_sessions_status",
        ),
        CheckConstraint(
            "expected_size_bytes > 0",
            name="ck_msrp_evidence_upload_sessions_size",
        ),
        CheckConstraint(
            "chunk_size_bytes > 0",
            name="ck_msrp_evidence_upload_sessions_chunk_size",
        ),
        CheckConstraint(
            "row_version > 0",
            name="ck_msrp_evidence_upload_sessions_row_version",
        ),
        Index(
            "ix_msrp_evidence_upload_sessions_status_expiry",
            "upload_status",
            "expires_at_utc",
        ),
        Index(
            "ix_msrp_evidence_upload_sessions_target",
            "target_id",
            "created_at_utc",
        ),
        {"schema": "msrp"},
    )

    upload_session_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    target_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.monitoring_targets.target_id"),
        nullable=False,
    )
    source_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.sources.source_id"),
        nullable=True,
    )
    repair_case_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "msrp.repair_cases.case_id",
            name="fk_msrp_evidence_upload_sessions_repair_case",
            use_alter=True,
        ),
        nullable=True,
    )
    completed_evidence_asset_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.source_evidence_assets.evidence_asset_id"),
        nullable=True,
    )
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    official_domain: Mapped[str] = mapped_column(Text, nullable=False)
    original_filename: Mapped[str] = mapped_column(Text, nullable=False)
    expected_mime_type: Mapped[str] = mapped_column(Text, nullable=False)
    expected_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    expected_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    chunk_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    received_parts_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    staging_key: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    semantic_lane: Mapped[str] = mapped_column(Text, nullable=False)
    document_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_until: Mapped[date | None] = mapped_column(Date, nullable=True)
    upload_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="initiated",
        server_default=text("'initiated'"),
    )
    expires_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    row_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )


class MsrpGovernanceRepairCase(GovernanceTimestampMixin, Base):
    __tablename__ = "repair_cases"
    __table_args__ = (
        CheckConstraint(
            "repair_domain IN "
            "('source', 'parser', 'semantic', 'result', 'mapping', 'fx', 'runtime')",
            name="ck_msrp_repair_cases_domain",
        ),
        CheckConstraint(
            "severity IN ('low', 'medium', 'high', 'critical')",
            name="ck_msrp_repair_cases_severity",
        ),
        CheckConstraint(
            "case_status IN "
            "('open', 'diagnosing', 'awaiting_evidence', 'proposal_ready', "
            "'dryrun_passed', 'awaiting_approval', 'resolved', 'rejected', "
            "'paused', 'superseded')",
            name="ck_msrp_repair_cases_status",
        ),
        CheckConstraint(
            "priority BETWEEN 0 AND 100",
            name="ck_msrp_repair_cases_priority",
        ),
        CheckConstraint(
            "occurrence_count > 0",
            name="ck_msrp_repair_cases_occurrence_count",
        ),
        CheckConstraint(
            "row_version > 0",
            name="ck_msrp_repair_cases_row_version",
        ),
        Index(
            "ix_msrp_repair_cases_queue",
            "case_status",
            "priority",
            "last_seen_at_utc",
        ),
        Index(
            "ix_msrp_repair_cases_target_domain",
            "target_id",
            "repair_domain",
        ),
        Index("ix_msrp_repair_cases_open_dedupe", "open_dedupe_key"),
        {"schema": "msrp"},
    )

    case_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    open_dedupe_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    repair_domain: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.monitoring_targets.target_id"),
        nullable=True,
    )
    source_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.sources.source_id"),
        nullable=True,
    )
    observation_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=True,
    )
    mapping_reference: Mapped[str | None] = mapped_column(Text, nullable=True)
    fx_run_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "msrp.fx_normalization_runs.fx_run_id",
            name="fk_msrp_repair_cases_fx_run",
            use_alter=True,
        ),
        nullable=True,
    )
    case_type: Mapped[str] = mapped_column(Text, nullable=False)
    failure_classifier: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=50)
    first_seen_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    last_seen_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    occurrence_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    recent_run_ids_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    evidence_refs_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    manual_evidence_required: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    agent_run_refs_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    proposal_refs_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    case_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="open",
        server_default=text("'open'"),
    )
    resolution_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    recurrence_of_case_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.repair_cases.case_id"),
        nullable=True,
    )
    owner: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    row_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )


class MsrpRepairProposal(GovernanceTimestampMixin, Base):
    __tablename__ = "repair_proposals"
    __table_args__ = (
        CheckConstraint(
            "proposal_origin IN ('manual', 'deterministic', 'hermes_agent')",
            name="ck_msrp_repair_proposals_origin",
        ),
        CheckConstraint(
            "dpv4_metadata_json IS NULL OR proposal_origin = 'hermes_agent'",
            name="ck_msrp_repair_proposals_dpv4_boundary",
        ),
        CheckConstraint(
            "proposal_status IN "
            "('draft', 'validated', 'dryrun_passed', 'submitted', 'approved', "
            "'rejected', 'published', 'superseded')",
            name="ck_msrp_repair_proposals_status",
        ),
        Index("ix_msrp_repair_proposals_case", "case_id", "created_at_utc"),
        Index(
            "ix_msrp_repair_proposals_status",
            "proposal_status",
            "updated_at_utc",
        ),
        {"schema": "msrp"},
    )

    proposal_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    case_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.repair_cases.case_id"),
        nullable=False,
    )
    target_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.monitoring_targets.target_id"),
        nullable=True,
    )
    source_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.sources.source_id"),
        nullable=True,
    )
    source_version_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.source_versions.source_version_id"),
        nullable=True,
    )
    proposal_origin: Mapped[str] = mapped_column(Text, nullable=False)
    proposal_type: Mapped[str] = mapped_column(Text, nullable=False)
    agent_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    agent_step_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dpv4_metadata_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    input_evidence_refs_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    proposed_change_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
    field_diff_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    assumptions_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    unresolved_questions_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    risk_flags_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    validation_result_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    dryrun_result_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    replay_result_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    conflict_result_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    gate_result_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    proposal_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="draft",
        server_default=text("'draft'"),
    )
    author: Mapped[str] = mapped_column(Text, nullable=False)
    reviewer: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class MsrpResultCorrectionDecision(GovernanceTimestampMixin, Base):
    __tablename__ = "result_correction_decisions"
    __table_args__ = (
        CheckConstraint(
            "decision_status IN ('draft', 'submitted', 'approved', 'rejected', 'applied')",
            name="ck_msrp_result_corrections_status",
        ),
        Index(
            "ix_msrp_result_corrections_observation",
            "original_observation_id",
        ),
        Index(
            "ix_msrp_result_corrections_gate_decision",
            "gate_decision_id",
        ),
        {"schema": "msrp"},
    )

    correction_decision_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    original_observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    gate_decision_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.governance_gate_decisions.gate_decision_id"),
        nullable=False,
    )
    original_current_price_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.current_prices.current_price_id"),
        nullable=True,
    )
    original_price_history_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.price_history.price_history_id"),
        nullable=True,
    )
    correction_type: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_refs_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    source_version_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.source_versions.source_version_id"),
        nullable=True,
    )
    corrected_inputs_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
    replay_result_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    gate_result_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
    replacement_observation_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=True,
    )
    rematerialization_refs_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    decision_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="draft",
        server_default=text("'draft'"),
    )
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    approved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class MsrpFxNormalizationRun(Base):
    __tablename__ = "fx_normalization_runs"
    __table_args__ = (
        CheckConstraint(
            "run_status IN ('pending', 'validated', 'approved', 'failed', 'superseded')",
            name="ck_msrp_fx_normalization_runs_status",
        ),
        CheckConstraint(
            "rate_to_normalized > 0",
            name="ck_msrp_fx_normalization_runs_rate",
        ),
        Index(
            "ix_msrp_fx_normalization_runs_observation",
            "observation_id",
            "created_at_utc",
        ),
        Index(
            "ix_msrp_fx_normalization_runs_gate_decision",
            "gate_decision_id",
        ),
        {"schema": "msrp"},
    )

    fx_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    gate_decision_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.governance_gate_decisions.gate_decision_id"),
        nullable=False,
    )
    local_currency: Mapped[str] = mapped_column(Text, nullable=False)
    local_value: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    fx_provider: Mapped[str] = mapped_column(Text, nullable=False)
    rate_to_normalized: Mapped[Decimal] = mapped_column(
        Numeric(18, 10),
        nullable=False,
    )
    rate_effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    rate_retrieved_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    policy_version: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_currency: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_value: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    gate_result_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
    run_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="pending",
        server_default=text("'pending'"),
    )
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    superseded_run_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.fx_normalization_runs.fx_run_id"),
        nullable=True,
    )
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    approved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MsrpGovernanceAuditEvent(Base):
    __tablename__ = "governance_audit_events"
    __table_args__ = (
        UniqueConstraint(
            "action",
            "idempotency_key",
            name="uq_msrp_governance_audit_idempotency",
        ),
        Index(
            "ix_msrp_governance_audit_entity",
            "entity_type",
            "entity_id",
            "occurred_at_utc",
        ),
        {"schema": "msrp"},
    )

    audit_event_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    actor_role: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    before_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    after_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    metadata_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    occurred_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
