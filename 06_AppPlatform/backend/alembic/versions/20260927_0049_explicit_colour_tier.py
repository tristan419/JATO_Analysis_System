"""Require an explicit material colour tier.

Revision ID: 20260927_0049
Revises: 20260925_0048
Create Date: 2026-09-27
"""

from alembic import op


revision = "20260927_0049"
down_revision = "20260925_0048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE ordering.material_sku_master
        ALTER COLUMN colour_tier DROP DEFAULT
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE ordering.material_sku_master
        ALTER COLUMN colour_tier SET DEFAULT 'single'
        """
    )
