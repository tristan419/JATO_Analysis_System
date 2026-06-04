"""Add Lease Comparison tables — lease_offers, lease_offer_versions, lease_compare_sets"""

revision = "20260604_0031"
down_revision = "20260602_0030"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS leasing")

    op.create_table(
        "lease_offers",
        sa.Column("offer_id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False, server_default="EUR"),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("model_name", sa.Text(), nullable=False),
        sa.Column("version", sa.Text(), nullable=True),
        sa.Column("powertrain", sa.Text(), nullable=True),
        sa.Column("segment", sa.Text(), nullable=True),
        sa.Column("lease_type", sa.Text(), nullable=False, server_default="private"),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("fx_rate_to_eur", sa.Numeric(14, 8), nullable=True),
        sa.Column("fx_rate_date", sa.Date(), nullable=True),
        sa.Column("fx_source", sa.Text(), nullable=True),
        sa.Column("fx_locked", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("monthly_payment", sa.Numeric(14, 2), nullable=True),
        sa.Column("monthly_payment_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("effective_monthly_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("down_payment", sa.Numeric(14, 2), nullable=True),
        sa.Column("down_payment_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("upfront_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("upfront_treatment", sa.Text(), nullable=True),
        sa.Column("term_months", sa.Integer(), nullable=True),
        sa.Column("mileage_per_year", sa.Integer(), nullable=True),
        sa.Column("cap_cost", sa.Numeric(14, 2), nullable=True),
        sa.Column("cap_cost_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("residual_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("residual_value_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("residual_value_percent", sa.Numeric(8, 4), nullable=True),
        sa.Column("apr_percent", sa.Numeric(8, 4), nullable=True),
        sa.Column("money_factor", sa.Numeric(12, 8), nullable=True),
        sa.Column("apr_source", sa.Text(), nullable=True, server_default="manual"),
        sa.Column("rv_guaranteed", sa.Boolean(), nullable=True),
        sa.Column("service_included", sa.Boolean(), nullable=True),
        sa.Column("insurance_included", sa.Boolean(), nullable=True),
        sa.Column("tyre_included", sa.Boolean(), nullable=True),
        sa.Column("vat_included", sa.Boolean(), nullable=True),
        sa.Column("deposit_required", sa.Boolean(), nullable=True),
        sa.Column("deposit_refundable", sa.Boolean(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="draft"),
        sa.Column("source_type", sa.Text(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("expiry_date", sa.Date(), nullable=True),
        sa.Column("total_contract_cost_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("risk_level", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column("row_version", sa.Integer(), server_default="1"),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Index("ix_leasing_offer_country", "country_code"),
        sa.Index("ix_leasing_offer_brand_model", "brand", "model_name"),
        sa.Index("ix_leasing_offer_status", "status"),
        schema="leasing",
    )

    op.create_table(
        "lease_offer_versions",
        sa.Column("version_id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("offer_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("leasing.lease_offers.offer_id", ondelete="CASCADE"), nullable=False),
        sa.Column("version_no", sa.Integer(), nullable=False),
        sa.Column("snapshot_json", postgresql.JSONB(), nullable=False),
        sa.Column("change_reason", sa.Text(), nullable=True),
        sa.Column("changed_by", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Index("ix_leasing_version_offer", "offer_id"),
        schema="leasing",
    )

    op.create_table(
        "lease_compare_sets",
        sa.Column("compare_id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("country_code", sa.Text(), nullable=True),
        sa.Column("selected_offer_ids", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Index("ix_leasing_compare_country", "country_code"),
        schema="leasing",
    )


def downgrade() -> None:
    op.drop_table("lease_compare_sets", schema="leasing")
    op.drop_table("lease_offer_versions", schema="leasing")
    op.drop_table("lease_offers", schema="leasing")
    op.execute("DROP SCHEMA IF EXISTS leasing")
