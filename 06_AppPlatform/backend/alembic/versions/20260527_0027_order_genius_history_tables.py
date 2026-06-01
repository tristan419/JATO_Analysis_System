"""Add fob_resolved_history and quantity_cell_history for Order Genius audit.

Revision ID: 20260527_0027
Revises: 20260526_0026
Create Date: 2026-05-27
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID


revision = "20260527_0027"
down_revision = "20260526_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "fob_resolved_history",
        sa.Column(
            "fob_history_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_sku_fob_id", PGUUID(as_uuid=True), nullable=False),
        sa.Column("baseline_version_id", PGUUID(as_uuid=True), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("material_code", sa.Text(), nullable=False),
        sa.Column("payment_term_code", sa.Text(), nullable=False),
        sa.Column("old_uploaded_fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("new_uploaded_fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("old_final_fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("new_final_fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("changed_by", sa.Text(), nullable=True, comment="trigger name, e.g. publish_baseline"),
        sa.Column(
            "changed_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Index("ix_ordering_fob_history_code", "material_code"),
        sa.Index("ix_ordering_fob_history_country_code", "country_code", "material_code"),
        schema="ordering",
    )

    op.create_table(
        "quantity_cell_history",
        sa.Column(
            "quantity_history_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("order_year", sa.Integer(), nullable=False),
        sa.Column("order_month", sa.Integer(), nullable=False),
        sa.Column("material_code", sa.Text(), nullable=False),
        sa.Column("old_quantity", sa.Integer(), nullable=True),
        sa.Column("new_quantity", sa.Integer(), nullable=False),
        sa.Column("old_fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("new_fob_eur", sa.Numeric(12, 2), nullable=False),
        sa.Column("changed_by", sa.Text(), nullable=True, comment="username who made the change"),
        sa.Column(
            "changed_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Index("ix_ordering_qty_history_cell", "country_code", "order_year", "order_month", "material_code"),
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_table("quantity_cell_history", schema="ordering")
    op.drop_table("fob_resolved_history", schema="ordering")
