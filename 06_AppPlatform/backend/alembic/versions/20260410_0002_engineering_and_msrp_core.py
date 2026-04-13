"""Add engineering import and MSRP observation core tables.

Revision ID: 20260410_0002
Revises: 20260410_0001
Create Date: 2026-04-10 00:30:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0002"
down_revision = "20260410_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "config_import_batches",
        sa.Column(
            "config_import_batch_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "import_batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("source_schema_version", sa.Text(), nullable=True),
        sa.Column("replace_mode", sa.Text(), nullable=False),
        sa.Column("import_status", sa.Text(), nullable=False),
        sa.Column(
            "row_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("valid_from_date", sa.Date(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["engineering.config_projects.project_id"],
        ),
        sa.ForeignKeyConstraint(
            ["import_batch_id"],
            ["ops.import_batches.import_batch_id"],
        ),
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_import_batches_project_created",
        "config_import_batches",
        ["project_id", "created_at_utc"],
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_import_batches_status",
        "config_import_batches",
        ["import_status"],
        schema="engineering",
    )

    op.create_table(
        "config_variants",
        sa.Column(
            "variant_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "config_import_batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("external_row_key", sa.Text(), nullable=True),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("trim_name", sa.Text(), nullable=False),
        sa.Column("version_name", sa.Text(), nullable=True),
        sa.Column("market_country", sa.Text(), nullable=False),
        sa.Column("powertrain", sa.Text(), nullable=True),
        sa.Column("body_style", sa.Text(), nullable=True),
        sa.Column("drive_type", sa.Text(), nullable=True),
        sa.Column("battery_kwh", sa.Numeric(10, 2), nullable=True),
        sa.Column("range_km", sa.Numeric(10, 2), nullable=True),
        sa.Column("target_msrp", sa.Numeric(14, 2), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column("row_hash", sa.Text(), nullable=False),
        sa.Column("attributes_json", postgresql.JSONB(), nullable=True),
        sa.Column("source_file_path", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["engineering.config_projects.project_id"],
        ),
        sa.ForeignKeyConstraint(
            ["config_import_batch_id"],
            ["engineering.config_import_batches.config_import_batch_id"],
        ),
        sa.UniqueConstraint(
            "project_id",
            "config_import_batch_id",
            "row_hash",
            name="uq_config_variants_batch_row_hash",
        ),
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_variants_project_active",
        "config_variants",
        ["project_id", "is_active"],
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_variants_brand_model_country",
        "config_variants",
        ["brand", "model", "market_country"],
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_variants_trim_name",
        "config_variants",
        ["trim_name"],
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_variants_attributes_json",
        "config_variants",
        ["attributes_json"],
        unique=False,
        schema="engineering",
        postgresql_using="gin",
    )

    op.create_table(
        "scrape_batches",
        sa.Column(
            "scrape_batch_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("batch_code", sa.Text(), nullable=False),
        sa.Column("trigger_type", sa.Text(), nullable=False),
        sa.Column("scope_country", sa.Text(), nullable=False),
        sa.Column("scope_brands_json", postgresql.JSONB(), nullable=True),
        sa.Column(
            "candidate_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "success_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "review_required_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "failed_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.UniqueConstraint("batch_code", name="uq_scrape_batches_batch_code"),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_scrape_batches_country_started",
        "scrape_batches",
        ["scope_country", "started_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_scrape_batches_status",
        "scrape_batches",
        ["status"],
        schema="msrp",
    )

    op.create_table(
        "observations",
        sa.Column(
            "observation_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "scrape_batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "source_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("jato_model", sa.Text(), nullable=False),
        sa.Column("jato_trim", sa.Text(), nullable=False),
        sa.Column("official_model", sa.Text(), nullable=False),
        sa.Column("official_trim", sa.Text(), nullable=False),
        sa.Column("msrp_value", sa.Numeric(14, 2), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("tax_included", sa.Boolean(), nullable=False),
        sa.Column("price_label", sa.Text(), nullable=False),
        sa.Column("availability_text", sa.Text(), nullable=True),
        sa.Column("observed_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_snapshot_path", sa.Text(), nullable=True),
        sa.Column("source_payload_hash", sa.Text(), nullable=True),
        sa.Column("extraction_version", sa.Text(), nullable=False),
        sa.Column("match_confidence", sa.Numeric(5, 4), nullable=False),
        sa.Column("match_status", sa.Text(), nullable=False),
        sa.Column("match_reason_json", postgresql.JSONB(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["scrape_batch_id"],
            ["msrp.scrape_batches.scrape_batch_id"],
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["msrp.sources.source_id"],
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_observations_country_brand_model_observed",
        "observations",
        ["country", "brand", "jato_model", "observed_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_observations_match_status_observed",
        "observations",
        ["match_status", "observed_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_observations_source_payload_hash",
        "observations",
        ["source_payload_hash"],
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_msrp_observations_source_payload_hash",
        table_name="observations",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_observations_match_status_observed",
        table_name="observations",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_observations_country_brand_model_observed",
        table_name="observations",
        schema="msrp",
    )
    op.drop_table("observations", schema="msrp")

    op.drop_index(
        "ix_msrp_scrape_batches_status",
        table_name="scrape_batches",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_scrape_batches_country_started",
        table_name="scrape_batches",
        schema="msrp",
    )
    op.drop_table("scrape_batches", schema="msrp")

    op.drop_index(
        "ix_engineering_config_variants_attributes_json",
        table_name="config_variants",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_config_variants_trim_name",
        table_name="config_variants",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_config_variants_brand_model_country",
        table_name="config_variants",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_config_variants_project_active",
        table_name="config_variants",
        schema="engineering",
    )
    op.drop_table("config_variants", schema="engineering")

    op.drop_index(
        "ix_engineering_config_import_batches_status",
        table_name="config_import_batches",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_config_import_batches_project_created",
        table_name="config_import_batches",
        schema="engineering",
    )
    op.drop_table("config_import_batches", schema="engineering")
