"""Add normalized engineering base and market override tables.

Revision ID: 20260417_0011
Revises: 20260417_0010
Create Date: 2026-04-17 19:20:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260417_0011"
down_revision = "20260417_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "base_variants",
        sa.Column(
            "base_variant_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("business_key", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("trim_name", sa.Text(), nullable=False),
        sa.Column("version_name", sa.Text(), nullable=True),
        sa.Column("powertrain", sa.Text(), nullable=True),
        sa.Column("base_features_json", postgresql.JSONB(), nullable=True),
        sa.Column("base_feature_labels_json", postgresql.JSONB(), nullable=True),
        sa.Column(
            "source_variant_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "market_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
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
        sa.UniqueConstraint(
            "project_id",
            "business_key",
            name="uq_config_base_variants_project_key",
        ),
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_base_variants_project_model",
        "base_variants",
        ["project_id", "model"],
        unique=False,
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_base_variants_brand_model",
        "base_variants",
        ["brand", "model"],
        unique=False,
        schema="engineering",
    )

    op.create_table(
        "market_variants",
        sa.Column(
            "market_variant_id",
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
            "base_variant_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "source_variant_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("external_row_key", sa.Text(), nullable=True),
        sa.Column("market_country", sa.Text(), nullable=False),
        sa.Column("target_msrp", sa.Numeric(14, 2), nullable=True),
        sa.Column("source_file_path", sa.Text(), nullable=True),
        sa.Column(
            "override_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
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
            ["base_variant_id"],
            ["engineering.base_variants.base_variant_id"],
        ),
        sa.ForeignKeyConstraint(
            ["source_variant_id"],
            ["engineering.config_variants.variant_id"],
        ),
        sa.UniqueConstraint(
            "source_variant_id",
            name="uq_config_market_variants_source_variant",
        ),
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_market_variants_project_country",
        "market_variants",
        ["project_id", "market_country"],
        unique=False,
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_market_variants_base_country",
        "market_variants",
        ["base_variant_id", "market_country"],
        unique=False,
        schema="engineering",
    )

    op.create_table(
        "market_feature_overrides",
        sa.Column(
            "feature_override_id",
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
            "market_variant_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "source_variant_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("feature_code", sa.Text(), nullable=False),
        sa.Column("feature_label", sa.Text(), nullable=False),
        sa.Column("value_type", sa.Text(), nullable=False),
        sa.Column("bool_value", sa.Boolean(), nullable=True),
        sa.Column("number_value", sa.Numeric(14, 4), nullable=True),
        sa.Column("text_value", sa.Text(), nullable=True),
        sa.Column("json_value", postgresql.JSONB(), nullable=True),
        sa.Column("availability", sa.Text(), nullable=True),
        sa.Column("package_code", sa.Text(), nullable=True),
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
            ["market_variant_id"],
            ["engineering.market_variants.market_variant_id"],
        ),
        sa.ForeignKeyConstraint(
            ["source_variant_id"],
            ["engineering.config_variants.variant_id"],
        ),
        sa.UniqueConstraint(
            "market_variant_id",
            "feature_code",
            name="uq_config_market_feature_overrides_market_feature",
        ),
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_market_feature_overrides_project_feature",
        "market_feature_overrides",
        ["project_id", "feature_code"],
        unique=False,
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_market_feature_overrides_market",
        "market_feature_overrides",
        ["market_variant_id"],
        unique=False,
        schema="engineering",
    )

    op.alter_column(
        "base_variants",
        "source_variant_count",
        server_default=None,
        schema="engineering",
    )
    op.alter_column(
        "base_variants",
        "market_count",
        server_default=None,
        schema="engineering",
    )
    op.alter_column(
        "market_variants",
        "override_count",
        server_default=None,
        schema="engineering",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_engineering_market_feature_overrides_market",
        table_name="market_feature_overrides",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_market_feature_overrides_project_feature",
        table_name="market_feature_overrides",
        schema="engineering",
    )
    op.drop_table("market_feature_overrides", schema="engineering")
    op.drop_index(
        "ix_engineering_market_variants_base_country",
        table_name="market_variants",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_market_variants_project_country",
        table_name="market_variants",
        schema="engineering",
    )
    op.drop_table("market_variants", schema="engineering")
    op.drop_index(
        "ix_engineering_base_variants_brand_model",
        table_name="base_variants",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_base_variants_project_model",
        table_name="base_variants",
        schema="engineering",
    )
    op.drop_table("base_variants", schema="engineering")
