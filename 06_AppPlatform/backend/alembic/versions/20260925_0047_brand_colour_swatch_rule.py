"""Persist shared BOM colour standards independently of SKU rows.

Revision ID: 20260925_0047
Revises: 20260715_0046
Create Date: 2026-09-25 00:47:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260925_0047"
down_revision = "20260715_0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "brand_colour_swatch_rule",
        sa.Column(
            "brand_colour_swatch_rule_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("colour_code", sa.Text(), nullable=False),
        sa.Column("colour_name", sa.Text(), nullable=False),
        sa.Column("colour_hex", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema="ordering",
    )
    op.create_index(
        "uq_ordering_brand_colour_swatch_active",
        "brand_colour_swatch_rule",
        ["brand", "colour_code"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_ordering_brand_colour_swatch_active",
        table_name="brand_colour_swatch_rule",
        schema="ordering",
    )
    op.drop_table("brand_colour_swatch_rule", schema="ordering")
