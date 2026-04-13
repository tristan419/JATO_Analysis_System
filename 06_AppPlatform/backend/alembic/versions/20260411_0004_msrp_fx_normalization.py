"""Normalize MSRP observations to EUR with locked FX metadata.

Revision ID: 20260411_0004
Revises: 20260411_0003
Create Date: 2026-04-11 14:45:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260411_0004"
down_revision = "20260411_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "observations",
        sa.Column("source_msrp_value", sa.Numeric(14, 2), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "observations",
        sa.Column("source_currency", sa.Text(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "observations",
        sa.Column("fx_rate_to_eur", sa.Numeric(14, 8), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "observations",
        sa.Column("fx_rate_as_of_date", sa.Date(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "observations",
        sa.Column("fx_source", sa.Text(), nullable=True),
        schema="msrp",
    )

    op.execute(
        """
        UPDATE msrp.observations
        SET source_msrp_value = msrp_value,
            source_currency = currency,
            fx_rate_to_eur = CASE WHEN currency = 'EUR' THEN 1.0 ELSE 1.0 END,
            fx_rate_as_of_date = COALESCE(DATE(observed_at_utc), CURRENT_DATE),
            fx_source = CASE WHEN currency = 'EUR' THEN 'identity' ELSE 'legacy-pre-fx' END
        """
    )
    op.execute(
        """
        UPDATE msrp.observations
        SET currency = 'EUR'
        WHERE currency = 'EUR'
        """
    )

    op.alter_column(
        "observations",
        "source_msrp_value",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "observations",
        "source_currency",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "observations",
        "fx_rate_to_eur",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "observations",
        "fx_rate_as_of_date",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "observations",
        "fx_source",
        nullable=False,
        schema="msrp",
    )

    op.add_column(
        "current_prices",
        sa.Column("source_msrp_value", sa.Numeric(14, 2), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "current_prices",
        sa.Column("source_currency", sa.Text(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "current_prices",
        sa.Column("fx_rate_to_eur", sa.Numeric(14, 8), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "current_prices",
        sa.Column("fx_rate_as_of_date", sa.Date(), nullable=True),
        schema="msrp",
    )
    op.add_column(
        "current_prices",
        sa.Column("fx_source", sa.Text(), nullable=True),
        schema="msrp",
    )

    op.execute(
        """
        UPDATE msrp.current_prices
        SET source_msrp_value = current_msrp_value,
            source_currency = currency,
            fx_rate_to_eur = CASE WHEN currency = 'EUR' THEN 1.0 ELSE 1.0 END,
            fx_rate_as_of_date = CURRENT_DATE,
            fx_source = CASE WHEN currency = 'EUR' THEN 'identity' ELSE 'legacy-pre-fx' END
        """
    )
    op.execute(
        """
        UPDATE msrp.current_prices
        SET currency = 'EUR'
        WHERE currency = 'EUR'
        """
    )

    op.alter_column(
        "current_prices",
        "source_msrp_value",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "current_prices",
        "source_currency",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "current_prices",
        "fx_rate_to_eur",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "current_prices",
        "fx_rate_as_of_date",
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "current_prices",
        "fx_source",
        nullable=False,
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_column("current_prices", "fx_source", schema="msrp")
    op.drop_column("current_prices", "fx_rate_as_of_date", schema="msrp")
    op.drop_column("current_prices", "fx_rate_to_eur", schema="msrp")
    op.drop_column("current_prices", "source_currency", schema="msrp")
    op.drop_column("current_prices", "source_msrp_value", schema="msrp")

    op.drop_column("observations", "fx_source", schema="msrp")
    op.drop_column("observations", "fx_rate_as_of_date", schema="msrp")
    op.drop_column("observations", "fx_rate_to_eur", schema="msrp")
    op.drop_column("observations", "source_currency", schema="msrp")
    op.drop_column("observations", "source_msrp_value", schema="msrp")