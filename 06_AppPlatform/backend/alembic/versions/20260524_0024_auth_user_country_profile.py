"""Add country preferences to auth users.

Revision ID: 20260524_0024
Revises: 20260523_0023
Create Date: 2026-05-24
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260524_0024"
down_revision = "20260523_0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("primary_country_code", sa.Text(), nullable=True),
        schema="auth",
    )
    op.add_column(
        "users",
        sa.Column(
            "secondary_country_codes",
            JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        schema="auth",
    )
    op.add_column(
        "users",
        sa.Column("preferred_landing_page", sa.Text(), nullable=True),
        schema="auth",
    )


def downgrade() -> None:
    op.drop_column("users", "preferred_landing_page", schema="auth")
    op.drop_column("users", "secondary_country_codes", schema="auth")
    op.drop_column("users", "primary_country_code", schema="auth")
