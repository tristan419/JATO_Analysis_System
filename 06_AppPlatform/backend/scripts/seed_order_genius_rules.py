#!/usr/bin/env python3
"""Seed Order Genius rules tables with initial production values.

Usage:
    cd 06_AppPlatform/backend
    python scripts/seed_order_genius_rules.py

Tables seeded:
    - ordering.brand_colour_surcharge_rule
    - ordering.payment_term_price_rule
    - ordering.country_payment_term_master
    - ordering.country_fob_source_mapping

All inserts use ON CONFLICT DO NOTHING — safe to re-run.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import text

from app.db.session import get_engine


def seed() -> None:
    engine = get_engine()
    with engine.connect() as conn:
        # --- Brand colour surcharge rules ---
        conn.execute(
            text(
                """
                INSERT INTO ordering.brand_colour_surcharge_rule
                    (colour_surcharge_rule_id, brand, colour_type,
                     surcharge_eur, is_active, created_at_utc, updated_at_utc)
                VALUES
                    (gen_random_uuid(), 'OMODA',  'single', 0,   true, now(), now()),
                    (gen_random_uuid(), 'OMODA',  'dual',   200, true, now(), now()),
                    (gen_random_uuid(), 'JAECOO', 'single', 0,   true, now(), now()),
                    (gen_random_uuid(), 'JAECOO', 'dual',   300, true, now(), now())
                ON CONFLICT DO NOTHING
                """
            )
        )

        # --- Payment term price rules ---
        # Set fob_adjustment_eur = 0 until finance confirms real values.
        conn.execute(
            text(
                """
                INSERT INTO ordering.payment_term_price_rule
                    (payment_term_rule_id, payment_term_code, payment_method,
                     lc_days, fob_adjustment_eur, is_active,
                     created_at_utc, updated_at_utc)
                VALUES
                    (gen_random_uuid(), 'TT',    'TT', 0,   0, true, now(), now()),
                    (gen_random_uuid(), 'LC60',  'LC', 60,  0, true, now(), now()),
                    (gen_random_uuid(), 'LC90',  'LC', 90,  0, true, now(), now()),
                    (gen_random_uuid(), 'LC120', 'LC', 120, 0, true, now(), now())
                ON CONFLICT DO NOTHING
                """
            )
        )

        # --- Country payment terms ---
        conn.execute(
            text(
                """
                INSERT INTO ordering.country_payment_term_master
                    (country_payment_term_id, country_code, country_name,
                     payment_term_code, payment_method, lc_days,
                     is_active, created_at_utc, updated_at_utc)
                VALUES
                    (gen_random_uuid(), 'RO', 'Romania',         'LC90',  'LC', 90,  true, now(), now()),
                    (gen_random_uuid(), 'CZ', 'Czech Republic',  'TT',    'TT', 0,   true, now(), now()),
                    (gen_random_uuid(), 'SE', 'Sweden',          'LC60',  'LC', 60,  true, now(), now()),
                    (gen_random_uuid(), 'FI', 'Finland',         'LC120', 'LC', 120, true, now(), now()),
                    (gen_random_uuid(), 'HR', 'Croatia',         'TT',    'TT', 0,   true, now(), now()),
                    (gen_random_uuid(), 'GR', 'Greece',          'TT',    'TT', 0,   true, now(), now()),
                    (gen_random_uuid(), 'AT', 'Austria',         'TT',    'TT', 0,   true, now(), now()),
                    (gen_random_uuid(), 'HU', 'Hungary',         'TT',    'TT', 0,   true, now(), now()),
                    (gen_random_uuid(), 'LV', 'Latvia',          'TT',    'TT', 0,   true, now(), now()),
                    (gen_random_uuid(), 'PL', 'Poland',          'TT',    'TT', 0,   true, now(), now())
                ON CONFLICT DO NOTHING
                """
            )
        )

        # --- Explicit FOB source mappings ---
        conn.execute(
            text(
                """
                INSERT INTO ordering.country_fob_source_mapping
                    (country_fob_source_mapping_id, target_country_code,
                     target_payment_term_code, source_country_code, is_active,
                     remark, created_at_utc, updated_at_utc)
                VALUES
                    (gen_random_uuid(), 'RO', 'LC90',  'HR', true,
                     'Romania uses Croatia uploaded FOB when RO price is absent', now(), now()),
                    (gen_random_uuid(), 'FI', 'LC120', 'SE', true,
                     'Finland uses Sweden uploaded FOB when FI price is absent', now(), now())
                ON CONFLICT DO NOTHING
                """
            )
        )

        conn.commit()

    # Verify
    with engine.connect() as conn:
        surcharges = conn.execute(
            text(
                "SELECT brand, colour_type, surcharge_eur "
                "FROM ordering.brand_colour_surcharge_rule "
                "WHERE is_active = true ORDER BY brand, colour_type"
            )
        ).fetchall()
        terms = conn.execute(
            text(
                "SELECT payment_term_code, payment_method, lc_days "
                "FROM ordering.payment_term_price_rule "
                "WHERE is_active = true ORDER BY lc_days"
            )
        ).fetchall()
        countries = conn.execute(
            text(
                "SELECT country_code, country_name, payment_term_code "
                "FROM ordering.country_payment_term_master "
                "WHERE is_active = true ORDER BY country_code"
            )
        ).fetchall()
        source_mappings = conn.execute(
            text(
                "SELECT target_country_code, target_payment_term_code, source_country_code "
                "FROM ordering.country_fob_source_mapping "
                "WHERE is_active = true ORDER BY target_country_code, target_payment_term_code"
            )
        ).fetchall()

    print("Colour surcharges:")
    for s in surcharges:
        print(f"  {s[0]} {s[1]}: +{s[2]} EUR")
    print(f"  ({len(surcharges)} rows)")

    print("Payment term rules:")
    for t in terms:
        print(f"  {t[0]} ({t[1]} {t[2]}d)")
    print(f"  ({len(terms)} rows)")

    print("Country payment terms:")
    for c in countries:
        print(f"  {c[0]} ({c[1]}): {c[2]}")
    print(f"  ({len(countries)} rows)")

    print("Country FOB source mappings:")
    for m in source_mappings:
        print(f"  {m[0]} {m[1]} -> {m[2]}")
    print(f"  ({len(source_mappings)} rows)")

    print("\nSeed complete.")


if __name__ == "__main__":
    seed()
