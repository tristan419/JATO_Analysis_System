"""Seed Netherlands ordering country option when missing."""

revision = "20260616_0035"
down_revision = "20260616_0034"
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO ordering.country_payment_term_master (
            country_payment_term_id,
            country_code,
            country_name,
            payment_term_code,
            payment_method,
            lc_days,
            is_active,
            valid_from_month,
            valid_to_month,
            remark
        )
        SELECT
            gen_random_uuid(),
            'NL',
            'Netherlands',
            'TT',
            'TT',
            0,
            true,
            NULL,
            NULL,
            'Seeded by 20260616_0035 for BOM/account country options'
        WHERE NOT EXISTS (
            SELECT 1
            FROM ordering.country_payment_term_master
            WHERE country_code = 'NL'
              AND is_active = true
        )
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM ordering.country_payment_term_master
        WHERE country_code = 'NL'
          AND remark = 'Seeded by 20260616_0035 for BOM/account country options'
        """
    )
