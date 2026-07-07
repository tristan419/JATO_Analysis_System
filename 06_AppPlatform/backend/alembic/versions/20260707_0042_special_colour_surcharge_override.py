"""Add per-special-colour surcharge override rules."""

revision = "20260707_0042"
down_revision = "20260702_0041"
branch_labels = None
depends_on = None

from alembic import op


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ordering.special_colour_surcharge_rule (
            special_colour_surcharge_rule_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            brand text NOT NULL,
            model_name text NULL,
            colour_code text NOT NULL,
            colour_name text NULL,
            surcharge_eur numeric(12, 2) NOT NULL DEFAULT 0,
            is_active boolean NOT NULL DEFAULT true,
            created_at_utc timestamptz NOT NULL DEFAULT now(),
            updated_at_utc timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ordering_special_colour_surcharge_active
        ON ordering.special_colour_surcharge_rule (brand, model_name, colour_code)
        WHERE is_active = true AND model_name IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ordering_special_colour_surcharge_global_active
        ON ordering.special_colour_surcharge_rule (brand, colour_code)
        WHERE is_active = true AND model_name IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ordering_special_colour_surcharge_lookup
        ON ordering.special_colour_surcharge_rule (brand, colour_code)
        """
    )
    op.execute(
        """
        INSERT INTO ordering.special_colour_surcharge_rule (
            brand,
            model_name,
            colour_code,
            colour_name,
            surcharge_eur,
            is_active,
            created_at_utc,
            updated_at_utc
        )
        VALUES ('OMODA', 'OMODA9 SHS', 'UE', 'Matte gray', 300, true, now(), now())
        ON CONFLICT DO NOTHING
        """
    )
    op.execute(
        """
        WITH affected AS (
            SELECT
                f.country_sku_fob_id,
                f.baseline_version_id,
                f.country_code,
                f.material_code,
                f.payment_term_code,
                f.uploaded_fob_eur,
                f.final_fob_eur AS old_final_fob_eur,
                base.base_fob_eur,
                COALESCE(special_rule.surcharge_eur, brand_rule.surcharge_eur, 0) AS surcharge_eur,
                ROUND((base.base_fob_eur + COALESCE(special_rule.surcharge_eur, brand_rule.surcharge_eur, 0))::numeric, 2) AS new_final_fob_eur
            FROM ordering.country_sku_fob_resolved f
            JOIN ordering.material_sku_master sku
              ON sku.material_code = f.material_code
             AND sku.is_active = true
            LEFT JOIN LATERAL (
                SELECT r.surcharge_eur
                FROM ordering.special_colour_surcharge_rule r
                WHERE sku.colour_tier = 'special'
                  AND r.brand = sku.brand
                  AND r.colour_code = UPPER(sku.exterior_color_code)
                  AND r.is_active = true
                  AND (r.model_name = sku.model_name OR r.model_name IS NULL)
                ORDER BY (r.model_name IS NULL), r.updated_at_utc DESC
                LIMIT 1
            ) special_rule ON true
            LEFT JOIN ordering.brand_colour_surcharge_rule brand_rule
              ON brand_rule.brand = sku.brand
             AND brand_rule.colour_type = sku.colour_tier
             AND brand_rule.is_active = true
            JOIN LATERAL (
                SELECT MIN(sf.final_fob_eur) AS base_fob_eur
                FROM ordering.country_sku_fob_resolved sf
                JOIN ordering.material_sku_master ssku
                  ON ssku.material_code = sf.material_code
                 AND ssku.is_active = true
                WHERE sf.country_code = f.country_code
                  AND sf.is_active = true
                  AND sf.final_fob_eur > 0
                  AND ssku.bom_template = sku.bom_template
                  AND ssku.colour_tier = 'single'
                  AND ssku.material_code <> sku.material_code
            ) base ON base.base_fob_eur IS NOT NULL
            WHERE f.is_active = true
              AND f.final_fob_eur > 0
              AND COALESCE(f.fob_source_mode, '') NOT IN ('manual_edit', 'manual_country_adjust')
              AND sku.colour_tier IN ('dual', 'special')
              AND (
                  ROUND(f.final_fob_eur::numeric, 2) <> ROUND((base.base_fob_eur + COALESCE(special_rule.surcharge_eur, brand_rule.surcharge_eur, 0))::numeric, 2)
                  OR f.base_fob_eur IS DISTINCT FROM base.base_fob_eur
                  OR f.colour_surcharge_eur IS DISTINCT FROM COALESCE(special_rule.surcharge_eur, brand_rule.surcharge_eur, 0)
              )
        ),
        history AS (
            INSERT INTO ordering.fob_resolved_history (
                fob_history_id,
                country_sku_fob_id,
                baseline_version_id,
                country_code,
                material_code,
                payment_term_code,
                old_uploaded_fob_eur,
                new_uploaded_fob_eur,
                old_final_fob_eur,
                new_final_fob_eur,
                changed_by,
                changed_at_utc
            )
            SELECT
                gen_random_uuid(),
                country_sku_fob_id,
                baseline_version_id,
                country_code,
                material_code,
                payment_term_code,
                uploaded_fob_eur,
                uploaded_fob_eur,
                old_final_fob_eur,
                new_final_fob_eur,
                'colour_surcharge_migration',
                now()
            FROM affected
            WHERE ROUND(old_final_fob_eur::numeric, 2) <> ROUND(new_final_fob_eur::numeric, 2)
            RETURNING country_sku_fob_id
        )
        UPDATE ordering.country_sku_fob_resolved f
        SET
            base_fob_eur = affected.base_fob_eur,
            colour_surcharge_eur = NULLIF(affected.surcharge_eur, 0),
            final_fob_eur = affected.new_final_fob_eur,
            updated_at_utc = now()
        FROM affected
        WHERE f.country_sku_fob_id = affected.country_sku_fob_id
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ordering.special_colour_surcharge_rule")
