"""Retain country-column deletions separately from individual deleted phases."""
from alembic import op

revision = "20261028_0061"
down_revision = "20261018_0060"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("ck_fob_period_status", "country_template_fob_period", type_="check", schema="ordering")
    op.create_check_constraint("ck_fob_period_status", "country_template_fob_period",
        "status IN ('active', 'deleted', 'default', 'country_deleted')", schema="ordering")
    op.drop_constraint("ck_fob_period_or_default_scope", "country_template_fob_period", type_="check", schema="ordering")
    op.create_check_constraint("ck_fob_period_or_default_scope", "country_template_fob_period",
        "(valid_from IS NOT NULL AND base_fob_eur IS NOT NULL) OR "
        "(status IN ('default', 'country_deleted') AND valid_from IS NULL AND valid_to IS NULL AND base_fob_eur IS NULL)",
        schema="ordering")


def downgrade() -> None:
    # No data rewrite: the old constraints reject downgrade while trash exists.
    op.drop_constraint("ck_fob_period_status", "country_template_fob_period", type_="check", schema="ordering")
    op.create_check_constraint("ck_fob_period_status", "country_template_fob_period",
        "status IN ('active', 'deleted', 'default')", schema="ordering")
    op.drop_constraint("ck_fob_period_or_default_scope", "country_template_fob_period", type_="check", schema="ordering")
    op.create_check_constraint("ck_fob_period_or_default_scope", "country_template_fob_period",
        "(valid_from IS NOT NULL AND base_fob_eur IS NOT NULL) OR "
        "(status = 'default' AND valid_from IS NULL AND valid_to IS NULL AND base_fob_eur IS NULL)", schema="ordering")
