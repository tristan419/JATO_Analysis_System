"""Bootstrap foundation business tables.

Revision ID: 20260410_0001
Revises:
Create Date: 2026-04-10 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS ops")
    op.execute("CREATE SCHEMA IF NOT EXISTS engineering")
    op.execute("CREATE SCHEMA IF NOT EXISTS msrp")
    op.execute("CREATE SCHEMA IF NOT EXISTS review")

    op.create_table(
        "import_batches",
        sa.Column(
            "import_batch_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("domain", sa.Text(), nullable=False),
        sa.Column("source_file_name", sa.Text(), nullable=False),
        sa.Column("source_file_path", sa.Text(), nullable=False),
        sa.Column("source_file_hash", sa.Text(), nullable=True),
        sa.Column("import_status", sa.Text(), nullable=False),
        sa.Column(
            "row_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "error_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("triggered_by", sa.Text(), nullable=True),
        sa.Column("started_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "finished_at_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        schema="ops",
    )
    op.create_index(
        "ix_ops_import_batches_domain_created",
        "import_batches",
        ["domain", "created_at_utc"],
        schema="ops",
    )
    op.create_index(
        "ix_ops_import_batches_status",
        "import_batches",
        ["import_status"],
        schema="ops",
    )

    op.create_table(
        "config_projects",
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("project_code", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("market_country", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
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
        sa.UniqueConstraint(
            "project_code",
            name="uq_config_projects_project_code",
        ),
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_projects_brand_model",
        "config_projects",
        ["brand", "model"],
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_projects_market_country",
        "config_projects",
        ["market_country"],
        schema="engineering",
    )

    op.create_table(
        "sources",
        sa.Column(
            "source_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("source_code", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("extractor_name", sa.Text(), nullable=False),
        sa.Column("extractor_version", sa.Text(), nullable=False),
        sa.Column("price_semantics", sa.Text(), nullable=False),
        sa.Column(
            "requires_location",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column("notes", sa.Text(), nullable=True),
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
        sa.UniqueConstraint("source_code", name="uq_sources_source_code"),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_sources_country_brand",
        "sources",
        ["country", "brand"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_sources_enabled",
        "sources",
        ["enabled"],
        schema="msrp",
    )

    op.create_table(
        "match_overrides",
        sa.Column(
            "override_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("jato_model", sa.Text(), nullable=False),
        sa.Column("jato_trim", sa.Text(), nullable=False),
        sa.Column("official_model", sa.Text(), nullable=False),
        sa.Column("official_trim", sa.Text(), nullable=False),
        sa.Column("valid_from_date", sa.Date(), nullable=False),
        sa.Column("valid_to_date", sa.Date(), nullable=True),
        sa.Column("override_reason", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
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
        sa.UniqueConstraint(
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "official_model",
            "official_trim",
            "valid_from_date",
            name="uq_match_overrides_business_key",
        ),
        schema="review",
    )
    op.create_index(
        "ix_review_match_overrides_country_brand_model",
        "match_overrides",
        ["country", "brand", "jato_model"],
        schema="review",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_review_match_overrides_country_brand_model",
        table_name="match_overrides",
        schema="review",
    )
    op.drop_table("match_overrides", schema="review")

    op.drop_index(
        "ix_msrp_sources_enabled",
        table_name="sources",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_sources_country_brand",
        table_name="sources",
        schema="msrp",
    )
    op.drop_table("sources", schema="msrp")

    op.drop_index(
        "ix_engineering_config_projects_market_country",
        table_name="config_projects",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_config_projects_brand_model",
        table_name="config_projects",
        schema="engineering",
    )
    op.drop_table("config_projects", schema="engineering")

    op.drop_index(
        "ix_ops_import_batches_status",
        table_name="import_batches",
        schema="ops",
    )
    op.drop_index(
        "ix_ops_import_batches_domain_created",
        table_name="import_batches",
        schema="ops",
    )
    op.drop_table("import_batches", schema="ops")

    op.execute("DROP SCHEMA IF EXISTS review")
    op.execute("DROP SCHEMA IF EXISTS msrp")
    op.execute("DROP SCHEMA IF EXISTS engineering")
    op.execute("DROP SCHEMA IF EXISTS ops")
