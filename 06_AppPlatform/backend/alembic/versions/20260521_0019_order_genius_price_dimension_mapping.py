"""Add Order Genius FOB price-dimension columns and source mappings.

Revision ID: 20260521_0019
Revises: 20260521_0018
Create Date: 2026-05-21
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID


revision = "20260521_0019"
down_revision = "20260521_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "country_sku_fob_resolved",
        sa.Column("uploaded_fob_eur", sa.Numeric(12, 2), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "country_sku_fob_resolved",
        sa.Column("fob_source_country_code", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "country_sku_fob_resolved",
        sa.Column(
            "fob_source_mode",
            sa.Text(),
            nullable=False,
            server_default="explicit_price_by_payment_term",
        ),
        schema="ordering",
    )
    op.execute(
        """
        UPDATE ordering.country_sku_fob_resolved
        SET uploaded_fob_eur = base_fob_eur
        WHERE uploaded_fob_eur IS NULL
        """
    )

    op.drop_index(
        "uq_ordering_country_sku_fob_active",
        table_name="country_sku_fob_resolved",
        schema="ordering",
    )
    op.create_index(
        "uq_ordering_country_sku_fob_active",
        "country_sku_fob_resolved",
        ["country_code", "material_code", "payment_term_code"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )

    op.create_table(
        "country_fob_source_mapping",
        sa.Column(
            "country_fob_source_mapping_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("target_country_code", sa.Text(), nullable=False),
        sa.Column("target_payment_term_code", sa.Text(), nullable=False),
        sa.Column("source_country_code", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("remark", sa.Text(), nullable=True),
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
        schema="ordering",
    )
    op.create_index(
        "uq_ordering_country_fob_source_mapping_active",
        "country_fob_source_mapping",
        ["target_country_code", "target_payment_term_code"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_ordering_country_fob_source_mapping_active",
        table_name="country_fob_source_mapping",
        schema="ordering",
    )
    op.drop_table("country_fob_source_mapping", schema="ordering")

    op.drop_index(
        "uq_ordering_country_sku_fob_active",
        table_name="country_sku_fob_resolved",
        schema="ordering",
    )
    op.create_index(
        "uq_ordering_country_sku_fob_active",
        "country_sku_fob_resolved",
        ["country_code", "material_code"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )
    op.drop_column(
        "country_sku_fob_resolved",
        "fob_source_mode",
        schema="ordering",
    )
    op.drop_column(
        "country_sku_fob_resolved",
        "fob_source_country_code",
        schema="ordering",
    )
    op.drop_column(
        "country_sku_fob_resolved",
        "uploaded_fob_eur",
        schema="ordering",
    )
