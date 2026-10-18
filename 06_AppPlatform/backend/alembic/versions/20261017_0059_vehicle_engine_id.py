"""Nullable engine identifier; no business-data backfill."""
from alembic import op
import sqlalchemy as sa

revision = "20261017_0059"
down_revision = "20261015_0058"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pi_vehicle_unit", sa.Column("engine_id", sa.Text(), nullable=True), schema="ordering")


def downgrade() -> None:
    op.drop_column("pi_vehicle_unit", "engine_id", schema="ordering")
