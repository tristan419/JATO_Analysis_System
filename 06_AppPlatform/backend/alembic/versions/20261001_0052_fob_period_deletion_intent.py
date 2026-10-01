"""Retain country period deletion intent without a second pricing model."""
from alembic import op
import sqlalchemy as sa

revision = "20261001_0052"
down_revision = "20260929_0051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("country_template_fob_period", sa.Column("status", sa.Text(), nullable=False, server_default="active"), schema="ordering")
    op.create_check_constraint("ck_fob_period_status", "country_template_fob_period", "status IN ('active', 'deleted', 'default')", schema="ordering")
    op.drop_constraint("uq_country_template_fob_period_start", "country_template_fob_period", schema="ordering", type_="unique")
    op.create_index("uq_country_template_fob_period_start", "country_template_fob_period", ["country_code", "bom_template", "valid_from"], unique=True, schema="ordering", postgresql_where=sa.text("status = 'active'"))


def downgrade() -> None:
    # Repeated starts are valid history now; do not delete history to force a downgrade.
    raise RuntimeError("Restore a database snapshot to downgrade FOB period history safely.")
