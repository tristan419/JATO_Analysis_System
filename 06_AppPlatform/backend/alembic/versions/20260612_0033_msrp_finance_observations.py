"""Add MSRP finance observations."""

revision = "20260612_0033"
down_revision = "20260612_0032"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def _has_table(table_name: str, *, schema: str = "msrp") -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS msrp")

    if _has_table("finance_observations"):
        return

    op.create_table(
        "finance_observations",
        sa.Column(
            "finance_observation_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "observation_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(
                "msrp.observations.observation_id",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column(
            "scrape_batch_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(
                "msrp.scrape_batches.scrape_batch_id",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("jato_model", sa.Text(), nullable=False),
        sa.Column("jato_trim", sa.Text(), nullable=False),
        sa.Column("jato_powertrain", sa.Text(), nullable=True),
        sa.Column("official_model", sa.Text(), nullable=False),
        sa.Column("official_trim", sa.Text(), nullable=False),
        sa.Column("official_edition", sa.Text(), nullable=True),
        sa.Column("official_powertrain", sa.Text(), nullable=True),
        sa.Column("price_semantics", sa.Text(), nullable=False),
        sa.Column("finance_type", sa.Text(), nullable=True),
        sa.Column("monthly_payment", sa.Numeric(14, 2), nullable=True),
        sa.Column("monthly_payment_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("down_payment", sa.Numeric(14, 2), nullable=True),
        sa.Column("down_payment_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("down_payment_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("term_months", sa.Integer(), nullable=True),
        sa.Column("apr", sa.Numeric(8, 4), nullable=True),
        sa.Column("effective_apr", sa.Numeric(8, 4), nullable=True),
        sa.Column("balloon_payment", sa.Numeric(14, 2), nullable=True),
        sa.Column("balloon_payment_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("total_credit_cost", sa.Numeric(14, 2), nullable=True),
        sa.Column("total_credit_cost_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("total_amount_payable", sa.Numeric(14, 2), nullable=True),
        sa.Column("total_amount_payable_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("annual_mileage_limit", sa.Integer(), nullable=True),
        sa.Column("offer_valid_until", sa.Date(), nullable=True),
        sa.Column("subsidy_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("subsidy_amount_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("net_price_after_subsidy", sa.Numeric(14, 2), nullable=True),
        sa.Column(
            "net_price_after_subsidy_eur",
            sa.Numeric(14, 2),
            nullable=True,
        ),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("observed_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finance_context_json", postgresql.JSONB(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_finance_observations_observation",
        "finance_observations",
        ["observation_id"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_finance_observations_scrape_batch",
        "finance_observations",
        ["scrape_batch_id"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_finance_observations_country_brand_model",
        "finance_observations",
        ["country", "brand", "jato_model"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_finance_observations_semantics",
        "finance_observations",
        ["price_semantics", "finance_type"],
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_table("finance_observations", schema="msrp")
