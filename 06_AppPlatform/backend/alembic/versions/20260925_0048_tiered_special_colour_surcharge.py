"""Bind colour-specific surcharge overrides to an explicit BOM tier."""

from alembic import op


revision = "20260925_0048"
down_revision = "20260925_0047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE ordering.special_colour_surcharge_rule
        ADD COLUMN IF NOT EXISTS colour_tier text NOT NULL DEFAULT 'special'
        """
    )
    op.execute(
        """
        ALTER TABLE ordering.special_colour_surcharge_rule
        DROP CONSTRAINT IF EXISTS ck_special_colour_surcharge_rule_tier
        """
    )
    op.execute(
        """
        ALTER TABLE ordering.special_colour_surcharge_rule
        ADD CONSTRAINT ck_special_colour_surcharge_rule_tier
        CHECK (colour_tier IN ('dual', 'special'))
        """
    )
    op.execute(
        """
        DROP INDEX IF EXISTS ordering.uq_ordering_special_colour_surcharge_active;
        DROP INDEX IF EXISTS ordering.uq_ordering_special_colour_surcharge_global_active;
        DROP INDEX IF EXISTS ordering.ix_ordering_special_colour_surcharge_lookup;
        CREATE UNIQUE INDEX uq_ordering_special_colour_surcharge_active
        ON ordering.special_colour_surcharge_rule
            (brand, model_name, colour_code, colour_tier)
        WHERE is_active = true AND model_name IS NOT NULL;
        CREATE UNIQUE INDEX uq_ordering_special_colour_surcharge_global_active
        ON ordering.special_colour_surcharge_rule
            (brand, colour_code, colour_tier)
        WHERE is_active = true AND model_name IS NULL;
        CREATE INDEX ix_ordering_special_colour_surcharge_lookup
        ON ordering.special_colour_surcharge_rule (brand, colour_code, colour_tier);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM ordering.special_colour_surcharge_rule
                WHERE colour_tier <> 'special'
            ) THEN
                RAISE EXCEPTION
                    'Cannot downgrade tiered colour surcharge rules while Dual rules exist';
            END IF;
        END $$;
        """
    )
    op.execute(
        """
        DROP INDEX IF EXISTS ordering.uq_ordering_special_colour_surcharge_active;
        DROP INDEX IF EXISTS ordering.uq_ordering_special_colour_surcharge_global_active;
        DROP INDEX IF EXISTS ordering.ix_ordering_special_colour_surcharge_lookup;
        CREATE UNIQUE INDEX uq_ordering_special_colour_surcharge_active
        ON ordering.special_colour_surcharge_rule (brand, model_name, colour_code)
        WHERE is_active = true AND model_name IS NOT NULL;
        CREATE UNIQUE INDEX uq_ordering_special_colour_surcharge_global_active
        ON ordering.special_colour_surcharge_rule (brand, colour_code)
        WHERE is_active = true AND model_name IS NULL;
        CREATE INDEX ix_ordering_special_colour_surcharge_lookup
        ON ordering.special_colour_surcharge_rule (brand, colour_code);
        ALTER TABLE ordering.special_colour_surcharge_rule DROP COLUMN colour_tier;
        """
    )
