"""Add display_name column to auth users.

Revision ID: 20260526_0026
Revises: 20260526_0025
Create Date: 2026-05-26
"""

from alembic import op
import sqlalchemy as sa


revision = "20260526_0026"
down_revision = "20260526_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("display_name", sa.Text(), nullable=True),
        schema="auth",
    )


def downgrade() -> None:
    op.drop_column("users", "display_name", schema="auth")
