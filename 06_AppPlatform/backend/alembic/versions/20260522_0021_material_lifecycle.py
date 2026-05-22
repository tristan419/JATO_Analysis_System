"""Add material_lifecycle table for country-level material validity tracking.

Revision ID: 20260522_0021
Revises: 20260522_0020
Create Date: 2026-05-22
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID


revision = "20260522_0021"
down_revision = "20260522_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "material_lifecycle",
        sa.Column(
            "lifecycle_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("material_code", sa.Text(), nullable=False),
        sa.Column(
            "product_identity",
            sa.Text(),
            nullable=False,
            comment="brand|model_name|version|powertrain",
        ),
        sa.Column("valid_from", sa.Date(), nullable=False),
        sa.Column(
            "valid_to",
            sa.Date(),
            nullable=True,
            comment="NULL = currently active",
        ),
        sa.Column(
            "lifecycle_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
            comment="active | phased_out | replaced",
        ),
        sa.Column(
            "replaced_by_code",
            sa.Text(),
            nullable=True,
            comment="Replacement material code",
        ),
        sa.Column("remark", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Index(
            "ix_ordering_lifecycle_country_product",
            "country_code",
            "product_identity",
        ),
        sa.Index(
            "ix_ordering_lifecycle_material_code",
            "material_code",
        ),
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_table("material_lifecycle", schema="ordering")
