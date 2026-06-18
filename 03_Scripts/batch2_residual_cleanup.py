#!/usr/bin/env python3
"""
Targeted cleanup for residual TODO_*_KEYWORD placeholders left after
the main batch-fill scripts.  Covers edge-case powertrain types:
  - LPG variants (Dacia Duster ECO-G, Renault Captur)
  - Missing MHEV/PHEV/ICE/BEV/HEV where scaffold included the block
  - Non-applicable powertrain blocks → keyword set to vehicle-type-specific exclusion

Run:
    python 03_Scripts/batch2_residual_cleanup.py [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30"

# Each entry: (file_path_relative_to_BASE, placeholder, replacement_keywords)
# For non-applicable powertrains we still fill with brand-level wording
# so the rule block has _some_ keyword (it simply won't match = 0 confidence, safe).
FIXES: list[tuple[str, str, list[str]]] = [
    # =============== HU ===============
    # LPG
    ("hu/05_dacia_duster_hu.yaml", "TODO_LPG_KEYWORD", ["LPG", "autógáz", "ECO-G"]),
    ("hu/19_renault_captur_hu.yaml", "TODO_LPG_KEYWORD", ["LPG", "autógáz"]),
    ("hu/11_ford_puma_hu.yaml", "TODO_LPG_KEYWORD", ["LPG", "autógáz"]),
    # PHEV - Renault Captur E-Tech Plug-in Hybrid
    ("hu/19_renault_captur_hu.yaml", "TODO_PHEV_KEYWORD", ["plug-in hibrid", "Plug-in Hybrid", "E-Tech Plug-in"]),
    # MHEV - Opel Frontera mild hybrid
    ("hu/27_opel_frontera_hu.yaml", "TODO_MHEV_KEYWORD", ["mild hybrid", "lágy hibrid", "48V"]),
    # MHEV - Nissan X-Trail
    ("hu/18_nissan_x_trail_hu.yaml", "TODO_MHEV_KEYWORD", ["mild hybrid", "lágy hibrid", "48V"]),
    # ICE - Toyota Yaris Cross (hybrid-only in EU → use generic petrol keyword)
    ("hu/04_toyota_yaris_cross_hu.yaml", "TODO_ICE_KEYWORD", ["benzin", "1.5"]),
    # ICE - Toyota RAV4 (hybrid-only in EU since gen5)
    ("hu/17_toyota_rav4_hu.yaml", "TODO_ICE_KEYWORD", ["benzin", "2.5"]),
    # HEV - Toyota Aygo X (ICE-only, no hybrid → keep benign keywords)
    ("hu/29_toyota_aygo_x_hu.yaml", "TODO_HEV_KEYWORD", ["hibrid"]),
    # HEV - Omoda 5 (ICE/PHEV, no HEV)
    ("hu/25_omoda_5_hu.yaml", "TODO_HEV_KEYWORD", ["hibrid"]),
    # BEV - Ford Puma (Puma Gen-E announced)
    ("hu/11_ford_puma_hu.yaml", "TODO_BEV_KEYWORD", ["elektromos", "Electric", "Puma Gen-E"]),
    # =============== NO ===============
    # ICE - Porsche Macan (new gen BEV-only)
    ("no/27_porsche_macan_no.yaml", "TODO_ICE_KEYWORD", ["bensin", "diesel"]),
    # ICE - Peugeot 5008
    ("no/28_peugeot_5008_no.yaml", "TODO_ICE_KEYWORD", ["bensin", "diesel", "PureTech"]),
    # =============== AT ===============
    # MHEV - Hyundai Kona
    ("at/23_hyundai_kona_at.yaml", "TODO_MHEV_KEYWORD", ["Mild-Hybrid", "48V"]),
    # BEV - Ford Puma (Puma Gen-E)
    ("at/30_ford_puma_at.yaml", "TODO_BEV_KEYWORD", ["Elektro", "Electric", "Puma Gen-E"]),
    # LPG - Dacia Duster ECO-G
    ("at/05_dacia_duster_at.yaml", "TODO_LPG_KEYWORD", ["LPG", "Autogas", "ECO-G"]),
    # ICE - Toyota Yaris Cross (hybrid-only → generic)
    ("at/14_toyota_yaris_cross_at.yaml", "TODO_ICE_KEYWORD", ["Benzin", "1.5"]),
    # =============== CZ ===============
    # LPG
    ("cz/05_dacia_duster_cz.yaml", "TODO_LPG_KEYWORD", ["LPG", "autogas", "ECO-G"]),
    ("cz/12_renault_captur_cz.yaml", "TODO_LPG_KEYWORD", ["LPG", "autogas"]),
    # PHEV - Renault Captur E-Tech
    ("cz/12_renault_captur_cz.yaml", "TODO_PHEV_KEYWORD", ["plug-in hybrid", "Plug-in Hybrid", "E-Tech Plug-in"]),
    # MHEV
    ("cz/15_ford_kuga_cz.yaml", "TODO_MHEV_KEYWORD", ["mild hybrid", "48V", "EcoBoost Hybrid"]),
    ("cz/23_hyundai_kona_cz.yaml", "TODO_MHEV_KEYWORD", ["mild hybrid", "48V"]),
    ("cz/17_peugeot_2008_cz.yaml", "TODO_MHEV_KEYWORD", ["mild hybrid", "48V"]),
    # BEV - Ford Puma
    ("cz/20_ford_puma_cz.yaml", "TODO_BEV_KEYWORD", ["elektrický", "Electric", "Puma Gen-E"]),
    # ICE
    ("cz/16_toyota_yaris_cross_cz.yaml", "TODO_ICE_KEYWORD", ["benzín", "1.5"]),
    ("cz/08_toyota_rav4_cz.yaml", "TODO_ICE_KEYWORD", ["benzín", "2.5"]),
    ("cz/30_suzuki_vitara_cz.yaml", "TODO_ICE_KEYWORD", ["benzín", "Boosterjet", "1.5"]),
    # =============== CH ===============
    # LPG - Dacia Duster ECO-G
    ("ch/11_dacia_duster_ch.yaml", "TODO_LPG_KEYWORD", ["LPG", "Autogas", "ECO-G"]),
    # PHEV - Audi Q3 Sportback / Q3
    ("ch/17_audi_q3_sportback_ch.yaml", "TODO_PHEV_KEYWORD", ["Plug-in-Hybrid", "TFSI e"]),
    ("ch/25_audi_q3_ch.yaml", "TODO_PHEV_KEYWORD", ["Plug-in-Hybrid", "TFSI e"]),
    # ICE - Toyota (hybrid-only in CH)
    ("ch/20_toyota_yaris_cross_ch.yaml", "TODO_ICE_KEYWORD", ["Benzin", "1.5"]),
    ("ch/30_toyota_rav4_ch.yaml", "TODO_ICE_KEYWORD", ["Benzin", "2.5"]),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    fixed = failed = 0
    for rel, placeholder, keywords in FIXES:
        path = BASE / rel
        if not path.exists():
            print(f"  MISSING: {rel}")
            failed += 1
            continue

        raw = path.read_text(encoding="utf-8")
        if placeholder not in raw:
            print(f"  ALREADY CLEAN: {rel} ({placeholder})")
            continue

        kw_lines = "\n".join(f"      - {kw}" for kw in keywords)
        raw = re.sub(
            rf"(\s+keywords:\n)\s+- {re.escape(placeholder)}",
            rf"\1{kw_lines}",
            raw,
        )

        tag = "WOULD FIX" if args.dry_run else "FIXED"
        if not args.dry_run:
            path.write_text(raw, encoding="utf-8")
        print(f"  {tag}: {rel} → {placeholder}")
        fixed += 1

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"\n[{mode}] fixed={fixed}  failed={failed}")


if __name__ == "__main__":
    main()
