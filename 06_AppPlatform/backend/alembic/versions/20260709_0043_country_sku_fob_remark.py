"""Add country FOB remark.

Revision ID: 20260709_0043
Revises: 20260707_0042
Create Date: 2026-07-09 00:43:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260709_0043"
down_revision = "20260707_0042"
branch_labels = None
depends_on = None


def _has_country_sku_fob_remark() -> bool:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'ordering'
              AND table_name = 'country_sku_fob_resolved'
              AND column_name = 'remark'
            """
        )
    ).first()
    return row is not None


def upgrade() -> None:
    if not _has_country_sku_fob_remark():
        op.add_column(
            "country_sku_fob_resolved",
            sa.Column("remark", sa.Text(), nullable=True),
            schema="ordering",
        )


def downgrade() -> None:
    if _has_country_sku_fob_remark():
        op.drop_column("country_sku_fob_resolved", "remark", schema="ordering")
