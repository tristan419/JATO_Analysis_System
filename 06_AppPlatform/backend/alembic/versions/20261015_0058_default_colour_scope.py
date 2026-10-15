"""Default colour scope in the existing template/country configuration table.

Revision ID: 20261015_0058
Revises: 20261015_0057
"""
from alembic import op
import sqlalchemy as sa

revision = "20261015_0058"
down_revision = "20261015_0057"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("country_template_fob_period", "valid_from", nullable=True, schema="ordering")
    op.alter_column("country_template_fob_period", "base_fob_eur", nullable=True, schema="ordering")
    op.create_check_constraint("ck_fob_period_or_default_scope", "country_template_fob_period",
        "(valid_from IS NOT NULL AND base_fob_eur IS NOT NULL) OR "
        "(status = 'default' AND valid_from IS NULL AND valid_to IS NULL AND base_fob_eur IS NULL)", schema="ordering")
    op.create_index("uq_country_template_fob_default", "country_template_fob_period",
        ["country_code", "bom_template"], unique=True, schema="ordering",
        postgresql_where=sa.text("status = 'default' AND valid_from IS NULL"))


def downgrade() -> None:
    # Existing default records must be explicitly removed before reverting nullability.
    op.drop_index("uq_country_template_fob_default", table_name="country_template_fob_period", schema="ordering")
    op.drop_constraint("ck_fob_period_or_default_scope", "country_template_fob_period", schema="ordering")
    op.alter_column("country_template_fob_period", "valid_from", nullable=False, schema="ordering")
    op.alter_column("country_template_fob_period", "base_fob_eur", nullable=False, schema="ordering")
