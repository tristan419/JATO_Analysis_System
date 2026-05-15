"""Add engineering_config schema for vehicle config matrix.

Revision ID: 20260515_0014
Revises: 20260421_0013
Create Date: 2026-05-15
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB


revision = "20260515_0014"
down_revision = "20260421_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS engineering_config")

    op.create_table(
        "feature_catalog",
        sa.Column(
            "feature_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("category", sa.Text, nullable=False),
        sa.Column("standard_field_name", sa.Text, nullable=False),
        sa.Column("feature_code", sa.Text, nullable=False),
        sa.Column("unit", sa.Text, nullable=True),
        sa.Column(
            "data_type",
            sa.Text,
            nullable=False,
            server_default=sa.text("'string'"),
        ),
        sa.Column("aliases", JSONB, nullable=True),
        sa.Column("display_order", sa.Integer, nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        schema="engineering_config",
    )

    op.create_table(
        "vehicle_trims",
        sa.Column(
            "trim_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "source_upload_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("ops.import_batches.import_batch_id"),
            nullable=True,
        ),
        sa.Column("brand", sa.Text, nullable=False),
        sa.Column("model_name", sa.Text, nullable=False),
        sa.Column("trim_name", sa.Text, nullable=False),
        sa.Column("full_trim_name", sa.Text, nullable=False),
        sa.Column("energy_type", sa.Text, nullable=True),
        sa.Column("drivetrain", sa.Text, nullable=True),
        sa.Column("engine", sa.Text, nullable=True),
        sa.Column("model_year", sa.Text, nullable=True),
        sa.Column(
            "status",
            sa.Text,
            nullable=False,
            server_default=sa.text("'active'"),
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
        schema="engineering_config",
    )

    op.create_table(
        "trim_feature_values",
        sa.Column(
            "value_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "trim_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("engineering_config.vehicle_trims.trim_id"),
            nullable=False,
        ),
        sa.Column(
            "feature_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("engineering_config.feature_catalog.feature_id"),
            nullable=False,
        ),
        sa.Column("raw_value", sa.Text, nullable=False),
        sa.Column("normalized_value", sa.Text, nullable=True),
        sa.Column("availability", sa.Text, nullable=False),
        sa.Column("unit", sa.Text, nullable=True),
        sa.Column("source_row", sa.Integer, nullable=False),
        sa.Column("source_column", sa.Text, nullable=False),
        sa.Column(
            "source_upload_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("ops.import_batches.import_batch_id"),
            nullable=True,
        ),
        sa.Column("version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("updated_by", sa.Text, nullable=True),
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
        schema="engineering_config",
    )

    op.create_table(
        "config_audit_log",
        sa.Column(
            "audit_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("entity_type", sa.Text, nullable=False),
        sa.Column("entity_id", PGUUID(as_uuid=True), nullable=False),
        sa.Column("field_name", sa.Text, nullable=False),
        sa.Column("old_value", sa.Text, nullable=True),
        sa.Column("new_value", sa.Text, nullable=True),
        sa.Column("changed_by", sa.Text, nullable=True),
        sa.Column(
            "changed_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "source",
            sa.Text,
            nullable=False,
            server_default=sa.text("'manual'"),
        ),
        sa.Column("comment", sa.Text, nullable=True),
        schema="engineering_config",
    )

    op.create_index(
        "ix_feature_catalog_category",
        "feature_catalog",
        ["category"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_feature_catalog_feature_code",
        "feature_catalog",
        ["feature_code"],
        schema="engineering_config",
    )
    op.create_unique_constraint(
        "uq_feature_catalog_category_field",
        "feature_catalog",
        ["category", "standard_field_name"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_vehicle_trims_brand",
        "vehicle_trims",
        ["brand"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_vehicle_trims_model",
        "vehicle_trims",
        ["model_name"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_vehicle_trims_status",
        "vehicle_trims",
        ["status"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_trim_feature_values_trim",
        "trim_feature_values",
        ["trim_id"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_trim_feature_values_feature",
        "trim_feature_values",
        ["feature_id"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_trim_feature_values_availability",
        "trim_feature_values",
        ["availability"],
        schema="engineering_config",
    )
    op.create_unique_constraint(
        "uq_trim_feature_values_trim_feature",
        "trim_feature_values",
        ["trim_id", "feature_id"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_config_audit_log_entity",
        "config_audit_log",
        ["entity_type", "entity_id"],
        schema="engineering_config",
    )
    op.create_index(
        "ix_config_audit_log_changed_at",
        "config_audit_log",
        ["changed_at_utc"],
        schema="engineering_config",
    )


def downgrade() -> None:
    op.drop_table("config_audit_log", schema="engineering_config")
    op.drop_table("trim_feature_values", schema="engineering_config")
    op.drop_table("vehicle_trims", schema="engineering_config")
    op.drop_table("feature_catalog", schema="engineering_config")
    op.execute("DROP SCHEMA IF EXISTS engineering_config")
