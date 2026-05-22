"""Make base_fob_eur, payment_term_adjustment_eur, colour_surcharge_eur nullable.

These three price columns on country_sku_fob_resolved were originally NOT NULL
but in practice a SKU may have only a final_fob_eur without explicit breakdowns.
Null means "not provided" rather than zero (which is a valid value).

Revision ID: 20260522_0020
Revises: 20260521_0019
Create Date: 2026-05-22
"""

from alembic import op
import sqlalchemy as sa


revision = "20260522_0020"
down_revision = "20260521_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "country_sku_fob_resolved",
        "base_fob_eur",
        existing_type=sa.Numeric(12, 2),
        nullable=True,
        schema="ordering",
    )
    op.alter_column(
        "country_sku_fob_resolved",
        "payment_term_adjustment_eur",
        existing_type=sa.Numeric(12, 2),
        nullable=True,
        existing_server_default=sa.text("'0'::numeric"),
        schema="ordering",
    )
    op.alter_column(
        "country_sku_fob_resolved",
        "colour_surcharge_eur",
        existing_type=sa.Numeric(12, 2),
        nullable=True,
        existing_server_default=sa.text("'0'::numeric"),
        schema="ordering",
    )


def downgrade() -> None:
    # Backfill NULLs to zero before restoring NOT NULL
    op.execute(
        """
        UPDATE ordering.country_sku_fob_resolved
        SET base_fob_eur = 0 WHERE base_fob_eur IS NULL
        """
    )
    op.execute(
        """
        UPDATE ordering.country_sku_fob_resolved
        SET payment_term_adjustment_eur = 0 WHERE payment_term_adjustment_eur IS NULL
        """
    )
    op.execute(
        """
        UPDATE ordering.country_sku_fob_resolved
        SET colour_surcharge_eur = 0 WHERE colour_surcharge_eur IS NULL
        """
    )
    op.alter_column(
        "country_sku_fob_resolved",
        "colour_surcharge_eur",
        existing_type=sa.Numeric(12, 2),
        nullable=False,
        existing_server_default=sa.text("'0'::numeric"),
        schema="ordering",
    )
    op.alter_column(
        "country_sku_fob_resolved",
        "payment_term_adjustment_eur",
        existing_type=sa.Numeric(12, 2),
        nullable=False,
        existing_server_default=sa.text("'0'::numeric"),
        schema="ordering",
    )
    op.alter_column(
        "country_sku_fob_resolved",
        "base_fob_eur",
        existing_type=sa.Numeric(12, 2),
        nullable=False,
        schema="ordering",
    )
