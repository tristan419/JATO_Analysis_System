"""Shared text column definitions and nullable per-vehicle values; no backfill."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20261018_0060"
down_revision = "20261017_0059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("vehicle_custom_column", sa.Column("column_id", sa.Text(), primary_key=True),
        sa.Column("label", sa.Text(), nullable=False), sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False), schema="ordering")
    op.add_column("pi_vehicle_unit", sa.Column("custom_values", postgresql.JSONB(), nullable=True), schema="ordering")
    op.create_index("uq_vehicle_custom_column_active_label", "vehicle_custom_column", [sa.text("lower(label)")],
                    unique=True, postgresql_where=sa.text("is_active = true"), schema="ordering")


def downgrade() -> None:
    op.drop_column("pi_vehicle_unit", "custom_values", schema="ordering")
    op.drop_table("vehicle_custom_column", schema="ordering")
