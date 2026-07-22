"""Add MSRP self-healing governance foundation.

Revision ID: 20260714_0044
Revises: 20260709_0043
Create Date: 2026-07-14 11:10:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260714_0044"
down_revision = "20260709_0043"
branch_labels = None
depends_on = None


UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB()


def upgrade() -> None:
    op.create_table(
        "monitoring_targets",
        sa.Column(
            "target_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("target_key", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("trim_scope", sa.Text(), nullable=True),
        sa.Column("powertrain_scope", sa.Text(), nullable=True),
        sa.Column(
            "roster_type",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'manual'"),
        ),
        sa.Column("roster_rank", sa.Integer(), nullable=True),
        sa.Column(
            "monitoring_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("active_source_version_id", UUID, nullable=True),
        sa.Column("fallback_source_version_id", UUID, nullable=True),
        sa.Column("schedule_json", JSONB, nullable=True),
        sa.Column("owner", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "row_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "roster_type IN ('country_top30', 'manual', 'future_roster')",
            name="ck_msrp_monitoring_targets_roster_type",
        ),
        sa.CheckConstraint(
            "monitoring_status IN "
            "('pending', 'active', 'degraded', 'manual_evidence_required', "
            "'paused', 'retired')",
            name="ck_msrp_monitoring_targets_status",
        ),
        sa.CheckConstraint(
            "roster_rank IS NULL OR roster_rank > 0",
            name="ck_msrp_monitoring_targets_roster_rank",
        ),
        sa.CheckConstraint(
            "row_version > 0",
            name="ck_msrp_monitoring_targets_row_version",
        ),
        sa.UniqueConstraint(
            "target_key",
            name="uq_msrp_monitoring_targets_target_key",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_monitoring_targets_country_brand",
        "monitoring_targets",
        ["country", "brand"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_monitoring_targets_status_rank",
        "monitoring_targets",
        ["monitoring_status", "roster_rank"],
        schema="msrp",
    )

    op.create_table(
        "governance_gate_decisions",
        sa.Column(
            "gate_decision_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "schema_version",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'1.0'"),
        ),
        sa.Column(
            "target_id",
            UUID,
            sa.ForeignKey("msrp.monitoring_targets.target_id"),
            nullable=False,
        ),
        sa.Column(
            "observation_id",
            UUID,
            sa.ForeignKey("msrp.observations.observation_id"),
            nullable=False,
        ),
        sa.Column("source_gate_json", JSONB, nullable=False),
        sa.Column("mapping_gate_json", JSONB, nullable=False),
        sa.Column("fx_gate_json", JSONB, nullable=True),
        sa.Column(
            "eligible_for_local_materialization",
            sa.Boolean(),
            nullable=False,
        ),
        sa.Column(
            "eligible_for_normalized_materialization",
            sa.Boolean(),
            nullable=False,
        ),
        sa.Column("evaluation_context_json", JSONB, nullable=True),
        sa.Column("evaluated_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "schema_version = '1.0'",
            name="ck_msrp_governance_gate_decisions_schema_version",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_governance_gate_decisions_target_evaluated",
        "governance_gate_decisions",
        ["target_id", "evaluated_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_governance_gate_decisions_observation",
        "governance_gate_decisions",
        ["observation_id"],
        schema="msrp",
    )

    op.create_table(
        "source_versions",
        sa.Column(
            "source_version_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "source_id",
            UUID,
            sa.ForeignKey("msrp.sources.source_id"),
            nullable=False,
        ),
        sa.Column(
            "target_id",
            UUID,
            sa.ForeignKey("msrp.monitoring_targets.target_id"),
            nullable=False,
        ),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("profile_json", JSONB, nullable=False),
        sa.Column("profile_yaml", sa.Text(), nullable=False),
        sa.Column("profile_sha256", sa.Text(), nullable=False),
        sa.Column(
            "evidence_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("extractor_name", sa.Text(), nullable=False),
        sa.Column("extractor_type", sa.Text(), nullable=False),
        sa.Column("extractor_version", sa.Text(), nullable=False),
        sa.Column("semantic_lane", sa.Text(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("tax_mode", sa.Text(), nullable=False),
        sa.Column("valid_from", sa.Date(), nullable=True),
        sa.Column("valid_until", sa.Date(), nullable=True),
        sa.Column(
            "previous_version_id",
            UUID,
            sa.ForeignKey("msrp.source_versions.source_version_id"),
            nullable=True,
        ),
        sa.Column("validation_summary_json", JSONB, nullable=True),
        sa.Column("dryrun_summary_json", JSONB, nullable=True),
        sa.Column("replay_summary_json", JSONB, nullable=True),
        sa.Column("conflict_summary_json", JSONB, nullable=True),
        sa.Column("gate_result_json", JSONB, nullable=False),
        sa.Column(
            "version_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'draft'"),
        ),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("approved_by", sa.Text(), nullable=True),
        sa.Column("approved_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decision_reason", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "version_number > 0",
            name="ck_msrp_source_versions_version_number",
        ),
        sa.CheckConstraint(
            "version_status IN "
            "('draft', 'validated', 'dryrun_passed', 'approved', 'published', "
            "'superseded', 'rejected', 'rolled_back')",
            name="ck_msrp_source_versions_status",
        ),
        sa.CheckConstraint(
            "valid_until IS NULL OR valid_from IS NULL OR valid_until >= valid_from",
            name="ck_msrp_source_versions_validity",
        ),
        sa.UniqueConstraint(
            "source_id",
            "version_number",
            name="uq_msrp_source_versions_source_number",
        ),
        sa.UniqueConstraint(
            "source_id",
            "profile_sha256",
            name="uq_msrp_source_versions_source_profile_sha256",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_source_versions_target_status",
        "source_versions",
        ["target_id", "version_status"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_source_versions_source_status",
        "source_versions",
        ["source_id", "version_status"],
        schema="msrp",
    )
    op.create_index(
        "uq_msrp_source_versions_one_published",
        "source_versions",
        ["source_id"],
        unique=True,
        schema="msrp",
        postgresql_where=sa.text("version_status = 'published'"),
    )
    op.create_foreign_key(
        "fk_msrp_monitoring_targets_active_source_version",
        "monitoring_targets",
        "source_versions",
        ["active_source_version_id"],
        ["source_version_id"],
        source_schema="msrp",
        referent_schema="msrp",
    )
    op.create_foreign_key(
        "fk_msrp_monitoring_targets_fallback_source_version",
        "monitoring_targets",
        "source_versions",
        ["fallback_source_version_id"],
        ["source_version_id"],
        source_schema="msrp",
        referent_schema="msrp",
    )

    op.create_table(
        "source_evidence_assets",
        sa.Column(
            "evidence_asset_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "target_id",
            UUID,
            sa.ForeignKey("msrp.monitoring_targets.target_id"),
            nullable=True,
        ),
        sa.Column(
            "source_id",
            UUID,
            sa.ForeignKey("msrp.sources.source_id"),
            nullable=True,
        ),
        sa.Column("repair_case_id", UUID, nullable=True),
        sa.Column("evidence_type", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("final_url", sa.Text(), nullable=True),
        sa.Column("redirect_chain_json", JSONB, nullable=True),
        sa.Column(
            "official_domain_verified",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("filename", sa.Text(), nullable=True),
        sa.Column("mime_type", sa.Text(), nullable=True),
        sa.Column("mime_signature", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("storage_key", sa.Text(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=False),
        sa.Column("captured_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("document_date", sa.Date(), nullable=True),
        sa.Column("valid_from", sa.Date(), nullable=True),
        sa.Column("valid_until", sa.Date(), nullable=True),
        sa.Column("page_count", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=True),
        sa.Column("text_hash", sa.Text(), nullable=True),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("semantic_lane", sa.Text(), nullable=False),
        sa.Column(
            "lifecycle_state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "evidence_type IN "
            "('official_url', 'uploaded_pdf', 'downloaded_pdf', "
            "'html_snapshot', 'api_snapshot', 'screenshot')",
            name="ck_msrp_source_evidence_assets_type",
        ),
        sa.CheckConstraint(
            "lifecycle_state IN ('active', 'superseded', 'rejected')",
            name="ck_msrp_source_evidence_assets_state",
        ),
        sa.CheckConstraint(
            "size_bytes IS NULL OR size_bytes >= 0",
            name="ck_msrp_source_evidence_assets_size",
        ),
        sa.CheckConstraint(
            "page_count IS NULL OR page_count > 0",
            name="ck_msrp_source_evidence_assets_pages",
        ),
        sa.CheckConstraint(
            "valid_until IS NULL OR valid_from IS NULL OR valid_until >= valid_from",
            name="ck_msrp_source_evidence_assets_validity",
        ),
        sa.UniqueConstraint(
            "target_id",
            "sha256",
            name="uq_msrp_source_evidence_assets_target_sha256",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_source_evidence_assets_target_captured",
        "source_evidence_assets",
        ["target_id", "captured_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_source_evidence_assets_source",
        "source_evidence_assets",
        ["source_id"],
        schema="msrp",
    )

    op.create_table(
        "evidence_upload_sessions",
        sa.Column(
            "upload_session_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "target_id",
            UUID,
            sa.ForeignKey("msrp.monitoring_targets.target_id"),
            nullable=False,
        ),
        sa.Column(
            "source_id",
            UUID,
            sa.ForeignKey("msrp.sources.source_id"),
            nullable=True,
        ),
        sa.Column("repair_case_id", UUID, nullable=True),
        sa.Column(
            "completed_evidence_asset_id",
            UUID,
            sa.ForeignKey("msrp.source_evidence_assets.evidence_asset_id"),
            nullable=True,
        ),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("official_domain", sa.Text(), nullable=False),
        sa.Column("original_filename", sa.Text(), nullable=False),
        sa.Column("expected_mime_type", sa.Text(), nullable=False),
        sa.Column("expected_size_bytes", sa.Integer(), nullable=False),
        sa.Column("expected_sha256", sa.Text(), nullable=False),
        sa.Column("chunk_size_bytes", sa.Integer(), nullable=False),
        sa.Column(
            "received_parts_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("staging_key", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("semantic_lane", sa.Text(), nullable=False),
        sa.Column("document_date", sa.Date(), nullable=True),
        sa.Column("valid_from", sa.Date(), nullable=True),
        sa.Column("valid_until", sa.Date(), nullable=True),
        sa.Column(
            "upload_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'initiated'"),
        ),
        sa.Column("expires_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column(
            "row_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "upload_status IN "
            "('initiated', 'uploading', 'completed', 'failed', 'expired')",
            name="ck_msrp_evidence_upload_sessions_status",
        ),
        sa.CheckConstraint(
            "expected_size_bytes > 0",
            name="ck_msrp_evidence_upload_sessions_size",
        ),
        sa.CheckConstraint(
            "chunk_size_bytes > 0",
            name="ck_msrp_evidence_upload_sessions_chunk_size",
        ),
        sa.CheckConstraint(
            "row_version > 0",
            name="ck_msrp_evidence_upload_sessions_row_version",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_evidence_upload_sessions_status_expiry",
        "evidence_upload_sessions",
        ["upload_status", "expires_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_evidence_upload_sessions_target",
        "evidence_upload_sessions",
        ["target_id", "created_at_utc"],
        schema="msrp",
    )

    op.create_table(
        "fx_normalization_runs",
        sa.Column(
            "fx_run_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "observation_id",
            UUID,
            sa.ForeignKey("msrp.observations.observation_id"),
            nullable=False,
        ),
        sa.Column(
            "gate_decision_id",
            UUID,
            sa.ForeignKey("msrp.governance_gate_decisions.gate_decision_id"),
            nullable=False,
        ),
        sa.Column("local_currency", sa.Text(), nullable=False),
        sa.Column("local_value", sa.Numeric(14, 2), nullable=False),
        sa.Column("fx_provider", sa.Text(), nullable=False),
        sa.Column("rate_to_normalized", sa.Numeric(18, 10), nullable=False),
        sa.Column("rate_effective_date", sa.Date(), nullable=False),
        sa.Column(
            "rate_retrieved_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("policy_version", sa.Text(), nullable=False),
        sa.Column("normalized_currency", sa.Text(), nullable=False),
        sa.Column("normalized_value", sa.Numeric(14, 2), nullable=False),
        sa.Column("gate_result_json", JSONB, nullable=False),
        sa.Column(
            "run_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("decision_reason", sa.Text(), nullable=True),
        sa.Column(
            "superseded_run_id",
            UUID,
            sa.ForeignKey("msrp.fx_normalization_runs.fx_run_id"),
            nullable=True,
        ),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("approved_by", sa.Text(), nullable=True),
        sa.Column("approved_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "run_status IN ('pending', 'validated', 'approved', 'failed', 'superseded')",
            name="ck_msrp_fx_normalization_runs_status",
        ),
        sa.CheckConstraint(
            "rate_to_normalized > 0",
            name="ck_msrp_fx_normalization_runs_rate",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_fx_normalization_runs_observation",
        "fx_normalization_runs",
        ["observation_id", "created_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_fx_normalization_runs_gate_decision",
        "fx_normalization_runs",
        ["gate_decision_id"],
        schema="msrp",
    )

    op.create_table(
        "repair_cases",
        sa.Column(
            "case_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("open_dedupe_key", sa.Text(), nullable=True),
        sa.Column("repair_domain", sa.Text(), nullable=False),
        sa.Column(
            "target_id",
            UUID,
            sa.ForeignKey("msrp.monitoring_targets.target_id"),
            nullable=True,
        ),
        sa.Column(
            "source_id",
            UUID,
            sa.ForeignKey("msrp.sources.source_id"),
            nullable=True,
        ),
        sa.Column(
            "observation_id",
            UUID,
            sa.ForeignKey("msrp.observations.observation_id"),
            nullable=True,
        ),
        sa.Column("mapping_reference", sa.Text(), nullable=True),
        sa.Column(
            "fx_run_id",
            UUID,
            sa.ForeignKey("msrp.fx_normalization_runs.fx_run_id"),
            nullable=True,
        ),
        sa.Column("case_type", sa.Text(), nullable=False),
        sa.Column("failure_classifier", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("first_seen_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "occurrence_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "recent_run_ids_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "evidence_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "manual_evidence_required",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "agent_run_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "proposal_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "case_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'open'"),
        ),
        sa.Column("resolution_json", JSONB, nullable=True),
        sa.Column(
            "recurrence_of_case_id",
            UUID,
            sa.ForeignKey("msrp.repair_cases.case_id"),
            nullable=True,
        ),
        sa.Column("owner", sa.Text(), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column(
            "row_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "repair_domain IN "
            "('source', 'parser', 'semantic', 'result', 'mapping', 'fx', 'runtime')",
            name="ck_msrp_repair_cases_domain",
        ),
        sa.CheckConstraint(
            "severity IN ('low', 'medium', 'high', 'critical')",
            name="ck_msrp_repair_cases_severity",
        ),
        sa.CheckConstraint(
            "case_status IN "
            "('open', 'diagnosing', 'awaiting_evidence', 'proposal_ready', "
            "'dryrun_passed', 'awaiting_approval', 'resolved', 'rejected', "
            "'paused', 'superseded')",
            name="ck_msrp_repair_cases_status",
        ),
        sa.CheckConstraint(
            "priority BETWEEN 0 AND 100",
            name="ck_msrp_repair_cases_priority",
        ),
        sa.CheckConstraint(
            "occurrence_count > 0",
            name="ck_msrp_repair_cases_occurrence_count",
        ),
        sa.CheckConstraint(
            "row_version > 0",
            name="ck_msrp_repair_cases_row_version",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_repair_cases_queue",
        "repair_cases",
        ["case_status", "priority", "last_seen_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_repair_cases_target_domain",
        "repair_cases",
        ["target_id", "repair_domain"],
        schema="msrp",
    )
    op.create_index(
        "uq_msrp_repair_cases_open_dedupe",
        "repair_cases",
        ["open_dedupe_key"],
        unique=True,
        schema="msrp",
        postgresql_where=sa.text("open_dedupe_key IS NOT NULL"),
    )
    op.create_foreign_key(
        "fk_msrp_source_evidence_assets_repair_case",
        "source_evidence_assets",
        "repair_cases",
        ["repair_case_id"],
        ["case_id"],
        source_schema="msrp",
        referent_schema="msrp",
    )
    op.create_foreign_key(
        "fk_msrp_evidence_upload_sessions_repair_case",
        "evidence_upload_sessions",
        "repair_cases",
        ["repair_case_id"],
        ["case_id"],
        source_schema="msrp",
        referent_schema="msrp",
    )

    op.create_table(
        "repair_proposals",
        sa.Column(
            "proposal_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "case_id",
            UUID,
            sa.ForeignKey("msrp.repair_cases.case_id"),
            nullable=False,
        ),
        sa.Column(
            "target_id",
            UUID,
            sa.ForeignKey("msrp.monitoring_targets.target_id"),
            nullable=True,
        ),
        sa.Column(
            "source_id",
            UUID,
            sa.ForeignKey("msrp.sources.source_id"),
            nullable=True,
        ),
        sa.Column(
            "source_version_id",
            UUID,
            sa.ForeignKey("msrp.source_versions.source_version_id"),
            nullable=True,
        ),
        sa.Column("proposal_origin", sa.Text(), nullable=False),
        sa.Column("proposal_type", sa.Text(), nullable=False),
        sa.Column("agent_run_id", sa.Text(), nullable=True),
        sa.Column("agent_step_id", sa.Text(), nullable=True),
        sa.Column("dpv4_metadata_json", JSONB, nullable=True),
        sa.Column(
            "input_evidence_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("proposed_change_json", JSONB, nullable=False),
        sa.Column(
            "field_diff_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "assumptions_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "unresolved_questions_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "risk_flags_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("validation_result_json", JSONB, nullable=True),
        sa.Column("dryrun_result_json", JSONB, nullable=True),
        sa.Column("replay_result_json", JSONB, nullable=True),
        sa.Column("conflict_result_json", JSONB, nullable=True),
        sa.Column("gate_result_json", JSONB, nullable=True),
        sa.Column(
            "proposal_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'draft'"),
        ),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("reviewer", sa.Text(), nullable=True),
        sa.Column("reviewed_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decision_reason", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "proposal_origin IN ('manual', 'deterministic', 'hermes_agent')",
            name="ck_msrp_repair_proposals_origin",
        ),
        sa.CheckConstraint(
            "dpv4_metadata_json IS NULL OR proposal_origin = 'hermes_agent'",
            name="ck_msrp_repair_proposals_dpv4_boundary",
        ),
        sa.CheckConstraint(
            "proposal_status IN "
            "('draft', 'validated', 'dryrun_passed', 'submitted', 'approved', "
            "'rejected', 'published', 'superseded')",
            name="ck_msrp_repair_proposals_status",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_repair_proposals_case",
        "repair_proposals",
        ["case_id", "created_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_repair_proposals_status",
        "repair_proposals",
        ["proposal_status", "updated_at_utc"],
        schema="msrp",
    )

    op.create_table(
        "result_correction_decisions",
        sa.Column(
            "correction_decision_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "original_observation_id",
            UUID,
            sa.ForeignKey("msrp.observations.observation_id"),
            nullable=False,
        ),
        sa.Column(
            "gate_decision_id",
            UUID,
            sa.ForeignKey("msrp.governance_gate_decisions.gate_decision_id"),
            nullable=False,
        ),
        sa.Column(
            "original_current_price_id",
            UUID,
            sa.ForeignKey("msrp.current_prices.current_price_id"),
            nullable=True,
        ),
        sa.Column(
            "original_price_history_id",
            UUID,
            sa.ForeignKey("msrp.price_history.price_history_id"),
            nullable=True,
        ),
        sa.Column("correction_type", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "evidence_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "source_version_id",
            UUID,
            sa.ForeignKey("msrp.source_versions.source_version_id"),
            nullable=True,
        ),
        sa.Column("corrected_inputs_json", JSONB, nullable=False),
        sa.Column("replay_result_json", JSONB, nullable=True),
        sa.Column("gate_result_json", JSONB, nullable=False),
        sa.Column(
            "replacement_observation_id",
            UUID,
            sa.ForeignKey("msrp.observations.observation_id"),
            nullable=True,
        ),
        sa.Column(
            "rematerialization_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "decision_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'draft'"),
        ),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("approved_by", sa.Text(), nullable=True),
        sa.Column("approved_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "decision_status IN ('draft', 'submitted', 'approved', 'rejected', 'applied')",
            name="ck_msrp_result_corrections_status",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_result_corrections_observation",
        "result_correction_decisions",
        ["original_observation_id"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_result_corrections_gate_decision",
        "result_correction_decisions",
        ["gate_decision_id"],
        schema="msrp",
    )

    op.create_table(
        "governance_audit_events",
        sa.Column(
            "audit_event_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_id", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("actor_role", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("before_json", JSONB, nullable=True),
        sa.Column("after_json", JSONB, nullable=True),
        sa.Column("metadata_json", JSONB, nullable=True),
        sa.Column(
            "occurred_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "action",
            "idempotency_key",
            name="uq_msrp_governance_audit_idempotency",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_governance_audit_entity",
        "governance_audit_events",
        ["entity_type", "entity_id", "occurred_at_utc"],
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_msrp_governance_audit_entity",
        table_name="governance_audit_events",
        schema="msrp",
    )
    op.drop_table("governance_audit_events", schema="msrp")

    op.drop_index(
        "ix_msrp_result_corrections_gate_decision",
        table_name="result_correction_decisions",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_result_corrections_observation",
        table_name="result_correction_decisions",
        schema="msrp",
    )
    op.drop_table("result_correction_decisions", schema="msrp")

    op.drop_index(
        "ix_msrp_repair_proposals_status",
        table_name="repair_proposals",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_repair_proposals_case",
        table_name="repair_proposals",
        schema="msrp",
    )
    op.drop_table("repair_proposals", schema="msrp")

    op.drop_constraint(
        "fk_msrp_source_evidence_assets_repair_case",
        "source_evidence_assets",
        schema="msrp",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_msrp_evidence_upload_sessions_repair_case",
        "evidence_upload_sessions",
        schema="msrp",
        type_="foreignkey",
    )
    op.drop_index(
        "uq_msrp_repair_cases_open_dedupe",
        table_name="repair_cases",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_repair_cases_target_domain",
        table_name="repair_cases",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_repair_cases_queue",
        table_name="repair_cases",
        schema="msrp",
    )
    op.drop_table("repair_cases", schema="msrp")

    op.drop_index(
        "ix_msrp_fx_normalization_runs_gate_decision",
        table_name="fx_normalization_runs",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_fx_normalization_runs_observation",
        table_name="fx_normalization_runs",
        schema="msrp",
    )
    op.drop_table("fx_normalization_runs", schema="msrp")

    op.drop_index(
        "ix_msrp_evidence_upload_sessions_target",
        table_name="evidence_upload_sessions",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_evidence_upload_sessions_status_expiry",
        table_name="evidence_upload_sessions",
        schema="msrp",
    )
    op.drop_table("evidence_upload_sessions", schema="msrp")

    op.drop_index(
        "ix_msrp_source_evidence_assets_source",
        table_name="source_evidence_assets",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_source_evidence_assets_target_captured",
        table_name="source_evidence_assets",
        schema="msrp",
    )
    op.drop_table("source_evidence_assets", schema="msrp")

    op.drop_constraint(
        "fk_msrp_monitoring_targets_fallback_source_version",
        "monitoring_targets",
        schema="msrp",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_msrp_monitoring_targets_active_source_version",
        "monitoring_targets",
        schema="msrp",
        type_="foreignkey",
    )
    op.drop_index(
        "uq_msrp_source_versions_one_published",
        table_name="source_versions",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_source_versions_source_status",
        table_name="source_versions",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_source_versions_target_status",
        table_name="source_versions",
        schema="msrp",
    )
    op.drop_table("source_versions", schema="msrp")

    op.drop_index(
        "ix_msrp_governance_gate_decisions_observation",
        table_name="governance_gate_decisions",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_governance_gate_decisions_target_evaluated",
        table_name="governance_gate_decisions",
        schema="msrp",
    )
    op.drop_table("governance_gate_decisions", schema="msrp")

    op.drop_index(
        "ix_msrp_monitoring_targets_status_rank",
        table_name="monitoring_targets",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_monitoring_targets_country_brand",
        table_name="monitoring_targets",
        schema="msrp",
    )
    op.drop_table("monitoring_targets", schema="msrp")
