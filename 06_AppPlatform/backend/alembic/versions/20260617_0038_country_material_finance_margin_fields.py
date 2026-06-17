"""Add margin/profit delta fields for country material finance."""

revision = "20260617_0038"
down_revision = "20260617_0037"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    op.add_column(
        "country_material_finance",
        sa.Column("vehicle_margin_eur", sa.Numeric(12, 2), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "country_material_finance",
        sa.Column("vehicle_margin_rate", sa.Numeric(10, 6), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "country_material_finance",
        sa.Column("vehicle_profit_eur", sa.Numeric(12, 2), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "country_material_finance",
        sa.Column("vehicle_profit_rate", sa.Numeric(10, 6), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "country_material_finance",
        sa.Column("fob_delta_eur", sa.Numeric(12, 2), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "country_material_finance",
        sa.Column("margin_delta_eur", sa.Numeric(12, 2), nullable=True),
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_column("country_material_finance", "margin_delta_eur", schema="ordering")
    op.drop_column("country_material_finance", "fob_delta_eur", schema="ordering")
    op.drop_column("country_material_finance", "vehicle_profit_rate", schema="ordering")
    op.drop_column("country_material_finance", "vehicle_profit_eur", schema="ordering")
    op.drop_column("country_material_finance", "vehicle_margin_rate", schema="ordering")
    op.drop_column("country_material_finance", "vehicle_margin_eur", schema="ordering")
