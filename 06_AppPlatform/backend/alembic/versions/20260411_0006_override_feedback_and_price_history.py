"""Add price_history table and override lookup index for feedback loop.

Revision ID: 20260411_0006
Revises: 20260411_0005
Create Date: 2026-04-11 23:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260411_0006"
down_revision = "20260411_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- msrp.price_history: compressed price time-series --
    op.create_table(
        "price_history",
        sa.Column(
            "price_history_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("jato_model", sa.Text(), nullable=False),
        sa.Column("jato_trim", sa.Text(), nullable=False),
        sa.Column("msrp_value", sa.Numeric(14, 2), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("source_msrp_value", sa.Numeric(14, 2), nullable=False),
        sa.Column("source_currency", sa.Text(), nullable=False),
        sa.Column(
            "valid_from_utc",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "valid_to_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "started_by_observation_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "ended_by_observation_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["started_by_observation_id"],
            ["msrp.observations.observation_id"],
        ),
        sa.ForeignKeyConstraint(
            ["ended_by_observation_id"],
            ["msrp.observations.observation_id"],
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_business_key",
        "price_history",
        ["country", "brand", "jato_model", "jato_trim"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_open_period",
        "price_history",
        ["country", "brand", "jato_model", "jato_trim", "valid_to_utc"],
        schema="msrp",
    )

    # -- review.match_overrides: add lookup index for feedback loop --
    # The existing index ix_review_match_overrides_country_brand_model
    # covers (country, brand, jato_model).  Add a covering index that
    # also includes jato_trim + valid_from_date for efficient override
    # lookups during ingest pre-match.
    op.create_index(
        "ix_review_match_overrides_lookup",
        "match_overrides",
        [
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            "valid_from_date",
        ],
        schema="review",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_review_match_overrides_lookup",
        table_name="match_overrides",
        schema="review",
    )
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
    op.drop_table("price_history", schema="msrp")
