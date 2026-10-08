"""Allow a confirmed shared colour name while its swatch is still missing."""

from alembic import op
import sqlalchemy as sa


revision = "20261008_0054"
down_revision = "20261004_0053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "brand_colour_swatch_rule", "colour_hex",
        existing_type=sa.Text(), nullable=True, schema="ordering",
    )


def downgrade() -> None:
    # PostgreSQL rejects this if name-only rules remain; never invent HEX or
    # delete confirmed business data merely to make a downgrade succeed.
    op.alter_column(
        "brand_colour_swatch_rule", "colour_hex",
        existing_type=sa.Text(), nullable=False, schema="ordering",
    )
