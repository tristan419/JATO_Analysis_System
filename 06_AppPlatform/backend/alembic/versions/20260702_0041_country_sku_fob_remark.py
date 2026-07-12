"""Add country FOB remark field."""

revision = "20260702_0041"
down_revision = "20260622_0040"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    op.add_column(
        "country_sku_fob_resolved",
        sa.Column("remark", sa.Text(), nullable=True),
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_column(
        "country_sku_fob_resolved",
        "remark",
        schema="ordering",
    )
