"""Add payment_term_audit_log for Payment Term Admin changes.

Revision ID: 20260523_0023
Revises: 20260523_0022
Create Date: 2026-05-23
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID


revision = "20260523_0023"
down_revision = "20260523_0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payment_term_audit_log",
        sa.Column(
            "audit_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column(
            "action",
            sa.Text(),
            nullable=False,
            comment="create | update | close | correct",
        ),
        sa.Column("old_payment_term_code", sa.Text(), nullable=True),
        sa.Column("new_payment_term_code", sa.Text(), nullable=True),
        sa.Column("old_valid_from", sa.Text(), nullable=True),
        sa.Column("old_valid_to", sa.Text(), nullable=True),
        sa.Column("new_valid_from", sa.Text(), nullable=True),
        sa.Column("new_valid_to", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("impacted_order_months", sa.Integer(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Index("ix_ordering_pt_audit_country", "country_code"),
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_table("payment_term_audit_log", schema="ordering")
