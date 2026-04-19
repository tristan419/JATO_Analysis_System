"""Add MSRP source tier and explicit JATO-to-MSRP links.

Revision ID: 20260417_0010
Revises: 20260417_0009
Create Date: 2026-04-17 16:30:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260417_0010"
down_revision = "20260417_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sources",
        sa.Column("tier", sa.Integer(), nullable=False, server_default="3"),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_sources_tier",
        "sources",
        ["tier"],
        unique=False,
        schema="msrp",
    )
    op.execute(
        """
        UPDATE msrp.sources
        SET tier = CASE
            WHEN lower(coalesce(source_type, '')) IN (
                'official',
                'official_site',
                'official_configurator',
                'official_price_list',
                'official_brand_site',
                'government'
            ) THEN 1
            WHEN lower(coalesce(source_type, '')) IN (
                'reference_catalog',
                'reference_site',
                'dealer_site',
                'dealer_group'
            ) THEN 2
            WHEN lower(coalesce(source_type, '')) IN (
                'marketplace',
                'classifieds',
                'third_party',
                'automotive_media',
                'news',
                'forum',
                'social'
            ) THEN 4
            WHEN lower(coalesce(price_semantics, '')) LIKE '%reference%'
                THEN 2
            ELSE 3
        END
        """
    )
    op.create_table(
        "jato_msrp_links",
        sa.Column(
            "link_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("jato_model", sa.Text(), nullable=False),
        sa.Column("jato_trim", sa.Text(), nullable=False),
        sa.Column(
            "jato_powertrain",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        sa.Column("official_model", sa.Text(), nullable=False),
        sa.Column("official_trim", sa.Text(), nullable=False),
        sa.Column(
            "official_edition",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        sa.Column(
            "official_powertrain",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        sa.Column(
            "confidence",
            sa.Integer(),
            nullable=False,
            server_default="80",
        ),
        sa.Column(
            "link_source",
            sa.Text(),
            nullable=False,
            server_default="manual",
        ),
        sa.Column(
            "is_active",
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
        sa.UniqueConstraint(
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
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_jato_msrp_links_jato_key",
        "jato_msrp_links",
        ["country", "brand", "jato_model", "jato_powertrain"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_jato_msrp_links_official_key",
        "jato_msrp_links",
        ["country", "brand", "official_model", "official_powertrain"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_jato_msrp_links_active",
        "jato_msrp_links",
        ["is_active"],
        unique=False,
        schema="msrp",
    )
    op.alter_column("sources", "tier", server_default=None, schema="msrp")
    op.alter_column(
        "jato_msrp_links",
        "jato_powertrain",
        server_default=None,
        schema="msrp",
    )
    op.alter_column(
        "jato_msrp_links",
        "official_edition",
        server_default=None,
        schema="msrp",
    )
    op.alter_column(
        "jato_msrp_links",
        "official_powertrain",
        server_default=None,
        schema="msrp",
    )
    op.alter_column(
        "jato_msrp_links",
        "confidence",
        server_default=None,
        schema="msrp",
    )
    op.alter_column(
        "jato_msrp_links",
        "link_source",
        server_default=None,
        schema="msrp",
    )
    op.alter_column(
        "jato_msrp_links",
        "is_active",
        server_default=None,
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_msrp_jato_msrp_links_active",
        table_name="jato_msrp_links",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_jato_msrp_links_official_key",
        table_name="jato_msrp_links",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_jato_msrp_links_jato_key",
        table_name="jato_msrp_links",
        schema="msrp",
    )
    op.drop_table("jato_msrp_links", schema="msrp")
    op.drop_index(
        "ix_msrp_sources_tier",
        table_name="sources",
        schema="msrp",
    )
    op.drop_column("sources", "tier", schema="msrp")