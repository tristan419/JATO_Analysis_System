"""Add valid_from_month / valid_to_month to country_payment_term_master.

Allows historical payment terms to coexist; only one row per country
with is_active=true.  FOB resolution looks up the payment term valid
at the time of the order month.

Revision ID: 20260523_0022
Revises: 20260522_0021
Create Date: 2026-05-23
"""

from alembic import op
import sqlalchemy as sa


revision = "20260523_0022"
down_revision = "20260522_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "country_payment_term_master",
        sa.Column("valid_from_month", sa.Text(), nullable=True,
                  comment="YYYY-MM when this term became effective"),
        schema="ordering",
    )
    op.add_column(
        "country_payment_term_master",
        sa.Column("valid_to_month", sa.Text(), nullable=True,
                  comment="YYYY-MM when this term ended; NULL = still effective"),
        schema="ordering",
    )
    # Set all existing active rows to 2024-01 (earliest known order data)
    op.execute(
        """
        UPDATE ordering.country_payment_term_master
        SET valid_from_month = '2024-01'
        WHERE valid_from_month IS NULL AND is_active = true
        """
    )


def downgrade() -> None:
    op.drop_column("country_payment_term_master", "valid_to_month", schema="ordering")
    op.drop_column("country_payment_term_master", "valid_from_month", schema="ordering")
