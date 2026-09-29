"""Add exact lifecycle dates to material templates.

Revision ID: 20260929_0051
Revises: 20260928_0050
Create Date: 2026-09-29
"""

from alembic import op
import sqlalchemy as sa


revision = "20260929_0051"
down_revision = "20260928_0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "material_sku_master",
        sa.Column("effective_from_date", sa.Date(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "material_sku_master",
        sa.Column("effective_to_date", sa.Date(), nullable=True),
        schema="ordering",
    )
    op.execute(
        """
        UPDATE ordering.material_sku_master
        SET effective_from_date = CASE
            WHEN effective_from_month ~ '^\\d{4}-\\d{2}-\\d{2}$'
                THEN effective_from_month::date
            WHEN effective_from_month ~ '^\\d{4}-\\d{2}$'
                THEN to_date(effective_from_month || '-01', 'YYYY-MM-DD')
            ELSE NULL
        END,
        effective_to_date = CASE
            WHEN effective_to_month ~ '^\\d{4}-\\d{2}-\\d{2}$'
                THEN effective_to_month::date
            WHEN effective_to_month ~ '^\\d{4}-\\d{2}$'
                THEN (
                    date_trunc('month', to_date(effective_to_month || '-01', 'YYYY-MM-DD'))
                    + interval '1 month - 1 day'
                )::date
            ELSE NULL
        END
        WHERE effective_from_month IS NOT NULL OR effective_to_month IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_column("material_sku_master", "effective_to_date", schema="ordering")
    op.drop_column("material_sku_master", "effective_from_date", schema="ordering")
