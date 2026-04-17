"""Expand MSRP business key for variants and add EVKX source context.

Revision ID: 20260417_0009
Revises: 20260415_0008
Create Date: 2026-04-17 10:30:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260417_0009"
down_revision = "20260415_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "observations",
        sa.Column("source_context_json", postgresql.JSONB(), nullable=True),
        schema="msrp",
    )

    op.add_column(
        "price_history",
        sa.Column(
            "jato_powertrain",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        schema="msrp",
    )
    op.execute(
        """
        UPDATE msrp.price_history ph
        SET jato_powertrain = COALESCE(NULLIF(o.jato_powertrain, ''), '')
        FROM msrp.observations o
        WHERE ph.started_by_observation_id = o.observation_id
        """
    )
    op.alter_column(
        "price_history",
        "jato_powertrain",
        server_default=None,
        schema="msrp",
    )

    op.execute(
        """
        UPDATE msrp.current_prices cp
        SET jato_powertrain = COALESCE(NULLIF(o.jato_powertrain, ''), '')
        FROM msrp.observations o
        WHERE cp.effective_observation_id = o.observation_id
          AND cp.jato_powertrain IS NULL
        """
    )
    op.execute(
        """
        UPDATE msrp.current_prices
        SET jato_powertrain = ''
        WHERE jato_powertrain IS NULL
        """
    )
    op.alter_column(
        "current_prices",
        "jato_powertrain",
        existing_type=sa.Text(),
        nullable=False,
        server_default="",
        schema="msrp",
    )
    op.drop_constraint(
        "uq_current_prices_business_key",
        "current_prices",
        schema="msrp",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_current_prices_business_key",
        "current_prices",
        ["country", "brand", "jato_model", "jato_trim", "jato_powertrain"],
        schema="msrp",
    )
    op.alter_column(
        "current_prices",
        "jato_powertrain",
        server_default=None,
        schema="msrp",
    )

    op.drop_index(
        "ix_msrp_price_history_business_key",
        table_name="price_history",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_price_history_open_period",
        table_name="price_history",
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_business_key",
        "price_history",
        ["country", "brand", "jato_model", "jato_trim", "jato_powertrain"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_open_period",
        "price_history",
        [
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "jato_powertrain",
            "valid_to_utc",
        ],
        unique=False,
        schema="msrp",
    )

    op.add_column(
        "match_overrides",
        sa.Column(
            "jato_powertrain",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        schema="review",
    )
    op.drop_constraint(
        "uq_match_overrides_business_key",
        "match_overrides",
        schema="review",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_match_overrides_business_key",
        "match_overrides",
        [
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "jato_powertrain",
            "official_model",
            "official_trim",
            "valid_from_date",
        ],
        schema="review",
    )
    op.drop_index(
        "ix_review_match_overrides_country_brand_model",
        table_name="match_overrides",
        schema="review",
    )
    op.create_index(
        "ix_review_match_overrides_country_brand_model",
        "match_overrides",
        ["country", "brand", "jato_model", "jato_powertrain"],
        unique=False,
        schema="review",
    )
    op.alter_column(
        "match_overrides",
        "jato_powertrain",
        server_default=None,
        schema="review",
    )


def downgrade() -> None:
    op.alter_column(
        "match_overrides",
        "jato_powertrain",
        existing_type=sa.Text(),
        nullable=True,
        schema="review",
    )
    op.drop_index(
        "ix_review_match_overrides_country_brand_model",
        table_name="match_overrides",
        schema="review",
    )
    op.create_index(
        "ix_review_match_overrides_country_brand_model",
        "match_overrides",
        ["country", "brand", "jato_model"],
        unique=False,
        schema="review",
    )
    op.drop_constraint(
        "uq_match_overrides_business_key",
        "match_overrides",
        schema="review",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_match_overrides_business_key",
        "match_overrides",
        [
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "official_model",
            "official_trim",
            "valid_from_date",
        ],
        schema="review",
    )
    op.drop_column("match_overrides", "jato_powertrain", schema="review")

    op.drop_index(
        "ix_msrp_price_history_open_period",
        table_name="price_history",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_price_history_business_key",
        table_name="price_history",
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_business_key",
        "price_history",
        ["country", "brand", "jato_model", "jato_trim"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_open_period",
        "price_history",
        ["country", "brand", "jato_model", "jato_trim", "valid_to_utc"],
        unique=False,
        schema="msrp",
    )
    op.drop_column("price_history", "jato_powertrain", schema="msrp")

    op.alter_column(
        "current_prices",
        "jato_powertrain",
        existing_type=sa.Text(),
        nullable=True,
        schema="msrp",
    )
    op.drop_constraint(
        "uq_current_prices_business_key",
        "current_prices",
        schema="msrp",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_current_prices_business_key",
        "current_prices",
        ["country", "brand", "jato_model", "jato_trim"],
        schema="msrp",
    )

    op.drop_column("observations", "source_context_json", schema="msrp")
