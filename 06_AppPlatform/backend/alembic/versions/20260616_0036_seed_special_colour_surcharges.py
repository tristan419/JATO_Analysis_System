"""Seed special colour surcharge rules for BOM Admin."""

revision = "20260616_0036"
down_revision = "20260616_0035"
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO ordering.brand_colour_surcharge_rule (
            colour_surcharge_rule_id,
            brand,
            colour_type,
            surcharge_eur,
            is_active,
            created_at_utc,
            updated_at_utc
        )
        VALUES
            (gen_random_uuid(), 'OMODA',  'special', 200, true, now(), now()),
            (gen_random_uuid(), 'JAECOO', 'special', 300, true, now(), now())
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM ordering.brand_colour_surcharge_rule
        WHERE brand IN ('OMODA', 'JAECOO')
          AND colour_type = 'special'
          AND surcharge_eur IN (200, 300)
        """
    )
