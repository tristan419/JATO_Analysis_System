"""Add colour metadata and interior fields to material_sku_master.

Revision ID: 20260530_0028
Revises: 20260527_0027
Create Date: 2026-05-30
"""

from alembic import op
import sqlalchemy as sa


revision = "20260530_0028"
down_revision = "20260527_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "material_sku_master",
        sa.Column("colour_hex", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "material_sku_master",
        sa.Column(
            "colour_code_confirmed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        schema="ordering",
    )
    op.add_column(
        "material_sku_master",
        sa.Column(
            "colour_tier",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'single'"),
            comment="single | dual | special",
        ),
        schema="ordering",
    )
    op.add_column(
        "material_sku_master",
        sa.Column("interior_colour_code", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "material_sku_master",
        sa.Column("interior_package", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "material_sku_master",
        sa.Column("edition_tag", sa.Text(), nullable=True),
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_column("material_sku_master", "edition_tag", schema="ordering")
    op.drop_column("material_sku_master", "interior_package", schema="ordering")
    op.drop_column("material_sku_master", "interior_colour_code", schema="ordering")
    op.drop_column("material_sku_master", "colour_tier", schema="ordering")
    op.drop_column("material_sku_master", "colour_code_confirmed", schema="ordering")
    op.drop_column("material_sku_master", "colour_hex", schema="ordering")
