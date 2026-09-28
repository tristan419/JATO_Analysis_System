"""Add date-effective country/template Single base FOB periods.

Revision ID: 20260928_0050
Revises: 20260927_0049
Create Date: 2026-09-28
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID


revision = "20260928_0050"
down_revision = "20260927_0049"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "country_template_fob_period",
        sa.Column(
            "country_template_fob_period_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("bom_template", sa.Text(), nullable=False),
        sa.Column("valid_from", sa.Date(), nullable=False),
        sa.Column("valid_to", sa.Date(), nullable=True),
        sa.Column("base_fob_eur", sa.Numeric(12, 2), nullable=False),
        sa.Column("remark", sa.Text(), nullable=True),
        sa.Column("row_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "country_code",
            "bom_template",
            "valid_from",
            name="uq_country_template_fob_period_start",
        ),
        sa.CheckConstraint(
            "valid_to IS NULL OR valid_to >= valid_from",
            name="ck_country_template_fob_period_window",
        ),
        sa.CheckConstraint(
            "base_fob_eur >= 0",
            name="ck_country_template_fob_period_non_negative",
        ),
        schema="ordering",
    )
    op.create_index(
        "ix_country_template_fob_period_lookup",
        "country_template_fob_period",
        ["country_code", "bom_template", "valid_from", "valid_to"],
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_table("country_template_fob_period", schema="ordering")
