"""Add database guardrails for temporal overlap and FK consistency.

Revision ID: 20260421_0013
Revises: 20260419_0012
Create Date: 2026-04-21 10:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260421_0013"
down_revision = "20260419_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    op.create_index(
        "ix_engineering_config_import_batches_import_batch",
        "config_import_batches",
        ["import_batch_id"],
        unique=False,
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_config_variants_import_batch",
        "config_variants",
        ["config_import_batch_id"],
        unique=False,
        schema="engineering",
    )
    op.create_index(
        "ix_engineering_market_feature_overrides_source_variant",
        "market_feature_overrides",
        ["source_variant_id"],
        unique=False,
        schema="engineering",
    )

    op.create_check_constraint(
        "ck_engineering_market_feature_overrides_single_value",
        "market_feature_overrides",
        "(CASE WHEN bool_value IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN number_value IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN text_value IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN json_value IS NOT NULL THEN 1 ELSE 0 END) = 1",
        schema="engineering",
    )

    op.create_index(
        "ix_msrp_observations_scrape_batch",
        "observations",
        ["scrape_batch_id"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_observations_source",
        "observations",
        ["source_id"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_current_prices_effective_observation",
        "current_prices",
        ["effective_observation_id"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_started_by_observation",
        "price_history",
        ["started_by_observation_id"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_ended_by_observation",
        "price_history",
        ["ended_by_observation_id"],
        unique=False,
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_price_history_last_confirmed_observation",
        "price_history",
        ["last_confirmed_by_observation_id"],
        unique=False,
        schema="msrp",
    )

    op.create_check_constraint(
        "ck_msrp_price_history_valid_window",
        "price_history",
        "valid_to_utc IS NULL OR valid_to_utc > valid_from_utc",
        schema="msrp",
    )
    op.execute(
        """
        ALTER TABLE msrp.price_history
        ADD CONSTRAINT ex_msrp_price_history_business_effective_period
        EXCLUDE USING gist (
            country WITH =,
            brand WITH =,
            jato_model WITH =,
            jato_trim WITH =,
            jato_powertrain WITH =,
            tstzrange(
                valid_from_utc,
                COALESCE(valid_to_utc, 'infinity'::timestamptz),
                '[)'
            ) WITH &&
        )
        """
    )

    op.create_check_constraint(
        "ck_review_match_overrides_valid_window",
        "match_overrides",
        "valid_to_date IS NULL OR valid_to_date >= valid_from_date",
        schema="review",
    )
    op.execute(
        """
        ALTER TABLE review.match_overrides
        ADD CONSTRAINT ex_review_match_overrides_business_effective_period
        EXCLUDE USING gist (
            country WITH =,
            brand WITH =,
            jato_model WITH =,
            jato_trim WITH =,
            jato_powertrain WITH =,
            official_model WITH =,
            official_trim WITH =,
            daterange(
                valid_from_date,
                COALESCE(valid_to_date + 1, 'infinity'::date),
                '[)'
            ) WITH &&
        )
        """
    )

    op.create_unique_constraint(
        "uq_review_cases_case_observation_pair",
        "review_cases",
        ["review_case_id", "observation_id"],
        schema="review",
    )
    op.create_index(
        "ix_review_decisions_case_observation",
        "review_decisions",
        ["review_case_id", "observation_id"],
        unique=False,
        schema="review",
    )
    op.create_foreign_key(
        "fk_review_decisions_case_observation_pair",
        "review_decisions",
        "review_cases",
        ["review_case_id", "observation_id"],
        ["review_case_id", "observation_id"],
        source_schema="review",
        referent_schema="review",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_review_decisions_case_observation_pair",
        "review_decisions",
        schema="review",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_review_cases_case_observation_pair",
        "review_cases",
        schema="review",
        type_="unique",
    )
    op.execute(
        "DROP INDEX IF EXISTS review.ix_review_decisions_case_observation"
    )

    op.execute(
        "ALTER TABLE review.match_overrides DROP CONSTRAINT ex_review_match_overrides_business_effective_period"
    )
    op.drop_constraint(
        "ck_review_match_overrides_valid_window",
        "match_overrides",
        schema="review",
        type_="check",
    )

    op.execute(
        "ALTER TABLE msrp.price_history DROP CONSTRAINT ex_msrp_price_history_business_effective_period"
    )
    op.drop_constraint(
        "ck_msrp_price_history_valid_window",
        "price_history",
        schema="msrp",
        type_="check",
    )

    op.drop_index(
        "ix_msrp_price_history_last_confirmed_observation",
        table_name="price_history",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_price_history_ended_by_observation",
        table_name="price_history",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_price_history_started_by_observation",
        table_name="price_history",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_current_prices_effective_observation",
        table_name="current_prices",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_observations_source",
        table_name="observations",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_observations_scrape_batch",
        table_name="observations",
        schema="msrp",
    )

    op.drop_constraint(
        "ck_engineering_market_feature_overrides_single_value",
        "market_feature_overrides",
        schema="engineering",
        type_="check",
    )
    op.drop_index(
        "ix_engineering_market_feature_overrides_source_variant",
        table_name="market_feature_overrides",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_config_variants_import_batch",
        table_name="config_variants",
        schema="engineering",
    )
    op.drop_index(
        "ix_engineering_config_import_batches_import_batch",
        table_name="config_import_batches",
        schema="engineering",
    )