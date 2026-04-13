"""Add structured edition and powertrain fields to MSRP entities.

Revision ID: 20260411_0005
Revises: 20260411_0004
Create Date: 2026-04-11 21:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260411_0005"
down_revision = "20260411_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "observations",
        sa.Column("jato_powertrain", sa.Text(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "observations",
        sa.Column("official_edition", sa.Text(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "observations",
        sa.Column("official_powertrain", sa.Text(), nullable=True),
        schema="msrp",
    )

    op.add_column(
        "current_prices",
        sa.Column("jato_powertrain", sa.Text(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "current_prices",
        sa.Column("official_edition", sa.Text(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "current_prices",
        sa.Column("official_powertrain", sa.Text(), nullable=True),
        schema="msrp",
    )

    op.add_column(
        "review_cases",
        sa.Column("jato_powertrain", sa.Text(), nullable=True),
        schema="review",
    )
    op.add_column(
        "review_cases",
        sa.Column("official_edition", sa.Text(), nullable=True),
        schema="review",
    )
    op.add_column(
        "review_cases",
        sa.Column("official_powertrain", sa.Text(), nullable=True),
        schema="review",
    )


def downgrade() -> None:
    op.drop_column("review_cases", "official_powertrain", schema="review")
    op.drop_column("review_cases", "official_edition", schema="review")
    op.drop_column("review_cases", "jato_powertrain", schema="review")

    op.drop_column("current_prices", "official_powertrain", schema="msrp")
    op.drop_column("current_prices", "official_edition", schema="msrp")
    op.drop_column("current_prices", "jato_powertrain", schema="msrp")

    op.drop_column("observations", "official_powertrain", schema="msrp")
    op.drop_column("observations", "official_edition", schema="msrp")
    op.drop_column("observations", "jato_powertrain", schema="msrp")
