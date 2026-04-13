"""Add current prices and review loop tables.

Revision ID: 20260411_0003
Revises: 20260410_0002
Create Date: 2026-04-11 10:30:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260411_0003"
down_revision = "20260410_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "current_prices",
        sa.Column(
            "current_price_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("jato_model", sa.Text(), nullable=False),
        sa.Column("jato_trim", sa.Text(), nullable=False),
        sa.Column("official_model", sa.Text(), nullable=False),
        sa.Column("official_trim", sa.Text(), nullable=False),
        sa.Column(
            "effective_observation_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("current_msrp_value", sa.Numeric(14, 2), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("tax_included", sa.Boolean(), nullable=False),
        sa.Column("match_confidence", sa.Numeric(5, 4), nullable=False),
        sa.Column("match_status", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_snapshot_path", sa.Text(), nullable=True),
        sa.Column(
            "last_price_change_at_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["effective_observation_id"],
            ["msrp.observations.observation_id"],
        ),
        sa.UniqueConstraint(
            "country",
            "brand",
            "jato_model",
            "jato_trim",
            name="uq_current_prices_business_key",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_current_prices_country_brand",
        "current_prices",
        ["country", "brand"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_current_prices_jato_model",
        "current_prices",
        ["jato_model"],
        schema="msrp",
    )

    op.create_table(
        "review_cases",
        sa.Column(
            "review_case_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "observation_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("jato_model", sa.Text(), nullable=False),
        sa.Column("jato_trim", sa.Text(), nullable=False),
        sa.Column("official_model", sa.Text(), nullable=False),
        sa.Column("official_trim", sa.Text(), nullable=False),
        sa.Column("candidate_matches_json", postgresql.JSONB(), nullable=True),
        sa.Column("match_confidence", sa.Numeric(5, 4), nullable=False),
        sa.Column("review_status", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_snapshot_path", sa.Text(), nullable=True),
        sa.Column("current_assignee", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["observation_id"],
            ["msrp.observations.observation_id"],
        ),
        sa.UniqueConstraint(
            "observation_id",
            name="uq_review_cases_observation_id",
        ),
        schema="review",
    )
    op.create_index(
        "ix_review_cases_status_created",
        "review_cases",
        ["review_status", "created_at_utc"],
        schema="review",
    )
    op.create_index(
        "ix_review_cases_country_brand",
        "review_cases",
        ["country", "brand"],
        schema="review",
    )
    op.create_index(
        "ix_review_cases_current_assignee",
        "review_cases",
        ["current_assignee"],
        schema="review",
    )

    op.create_table(
        "review_decisions",
        sa.Column(
            "review_decision_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "review_case_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "observation_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("decision", sa.Text(), nullable=False),
        sa.Column("decided_official_model", sa.Text(), nullable=True),
        sa.Column("decided_official_trim", sa.Text(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("decided_by", sa.Text(), nullable=False),
        sa.Column(
            "decided_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["review_case_id"],
            ["review.review_cases.review_case_id"],
        ),
        sa.ForeignKeyConstraint(
            ["observation_id"],
            ["msrp.observations.observation_id"],
        ),
        schema="review",
    )
    op.create_index(
        "ix_review_decisions_case_decided",
        "review_decisions",
        ["review_case_id", "decided_at_utc"],
        schema="review",
    )
    op.create_index(
        "ix_review_decisions_observation",
        "review_decisions",
        ["observation_id"],
        schema="review",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_review_decisions_observation",
        table_name="review_decisions",
        schema="review",
    )
    op.drop_index(
        "ix_review_decisions_case_decided",
        table_name="review_decisions",
        schema="review",
    )
    op.drop_table("review_decisions", schema="review")

    op.drop_index(
        "ix_review_cases_current_assignee",
        table_name="review_cases",
        schema="review",
    )
    op.drop_index(
        "ix_review_cases_country_brand",
        table_name="review_cases",
        schema="review",
    )
    op.drop_index(
        "ix_review_cases_status_created",
        table_name="review_cases",
        schema="review",
    )
    op.drop_table("review_cases", schema="review")

    op.drop_index(
        "ix_msrp_current_prices_jato_model",
        table_name="current_prices",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_current_prices_country_brand",
        table_name="current_prices",
        schema="msrp",
    )
    op.drop_table("current_prices", schema="msrp")
