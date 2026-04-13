#!/usr/bin/env python3
"""
Batch-fill powertrain keywords, source URLs, and currency for all 30
Czech (cz) draft YAML files that still contain todo.invalid placeholders.

Czech automotive terminology:
  BEV  = "elektrický", "elektromobil", "čistě elektrický", "electric"
  PHEV = "plug-in hybrid", "plug-in hybridní", "PHEV"
  HEV  = "hybrid", "hybridní", "plný hybrid", "self-charging hybrid"
  MHEV = "mild hybrid", "48V", "MHEV"
  ICE  = "benzín", "nafta", "diesel"
  Currency: CZK

Run:
    python 03_Scripts/batch_fill_cz_keywords.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DRAFT_DIR = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30" / "cz"

CURRENCY = "CZK"

# --------------- keyword / URL registry ---------------
REGISTRY: dict[str, dict[str, Any]] = {
    # ---- Skoda (dominant Czech brand) ----
    "01_skoda_kamiq_cz": {
        "url": "https://www.skoda-auto.cz/modely/kamiq",
        "keywords": {
            "ICE": ["benzín", "TSI"],
        },
    },
    "02_skoda_karoq_cz": {
        "url": "https://www.skoda-auto.cz/modely/karoq",
        "keywords": {
            "ICE": ["benzín", "nafta", "TSI", "TDI"],
        },
    },
    "03_skoda_kodiaq_cz": {
        "url": "https://www.skoda-auto.cz/modely/kodiaq",
        "keywords": {
            "PHEV": ["plug-in hybrid", "plug-in hybridní", "iV", "Kodiaq iV"],
            "MHEV": ["mild hybrid", "eTSI", "48V"],
            "ICE": ["benzín", "nafta", "TSI", "TDI"],
        },
    },
    "07_skoda_elroq_cz": {
        "url": "https://www.skoda-auto.cz/modely/elroq",
        "keywords": {
            "BEV": ["elektrický", "elektromobil", "čistě elektrický", "Elroq"],
        },
    },
    "14_skoda_enyaq_cz": {
        "url": "https://www.skoda-auto.cz/modely/enyaq",
        "keywords": {
            "BEV": ["elektrický", "elektromobil", "čistě elektrický", "Enyaq"],
        },
    },
    # ---- Hyundai ----
    "04_hyundai_tucson_cz": {
        "url": "https://www.hyundai.cz/modely/tucson",
        "keywords": {
            "PHEV": ["plug-in hybrid", "Tucson Plug-in Hybrid"],
            "HEV": ["hybrid", "hybridní", "Tucson Hybrid"],
            "MHEV": ["mild hybrid", "48V", "MHEV"],
            "ICE": ["benzín", "nafta"],
        },
    },
    "23_hyundai_kona_cz": {
        "url": "https://www.hyundai.cz/modely/kona",
        "keywords": {
            "BEV": ["elektrický", "KONA Electric", "elektromobil"],
            "HEV": ["hybrid", "KONA Hybrid"],
            "ICE": ["benzín"],
        },
    },
    # ---- Dacia ----
    "05_dacia_duster_cz": {
        "url": "https://www.dacia.cz/modely/duster.html",
        "keywords": {
            "HEV": ["hybrid", "Hybrid 140", "hybridní"],
            "MHEV": ["mild hybrid", "48V", "TCe"],
            "ICE": ["benzín", "TCe", "ECO-G"],
        },
    },
    "09_dacia_bigster_cz": {
        "url": "https://www.dacia.cz/modely/bigster.html",
        "keywords": {
            "HEV": ["hybrid", "Hybrid 155", "hybridní"],
            "MHEV": ["mild hybrid", "48V", "TCe"],
            "ICE": ["benzín", "TCe"],
        },
    },
    # ---- MG ----
    "06_mg_zs_cz": {
        "url": "https://www.mgmotor.cz/modely/mg-zs",
        "keywords": {
            "BEV": ["elektrický", "electric", "ZS EV", "elektromobil"],
            "HEV": ["hybrid", "ZS Hybrid+"],
            "ICE": ["benzín"],
        },
    },
    # ---- Toyota ----
    "08_toyota_rav4_cz": {
        "url": "https://www.toyota.cz/new-cars/rav4",
        "keywords": {
            "PHEV": ["plug-in hybrid", "Plug-in Hybrid", "RAV4 PHEV"],
            "HEV": ["hybrid", "Hybrid Synergy Drive", "plný hybrid"],
        },
    },
    "13_toyota_c_hr_cz": {
        "url": "https://www.toyota.cz/new-cars/c-hr",
        "keywords": {
            "PHEV": ["plug-in hybrid", "Plug-in Hybrid"],
            "HEV": ["hybrid", "Hybrid Synergy Drive"],
        },
    },
    "16_toyota_yaris_cross_cz": {
        "url": "https://www.toyota.cz/new-cars/yaris-cross",
        "keywords": {
            "HEV": ["hybrid", "Hybrid Synergy Drive", "plný hybrid"],
        },
    },
    # ---- VW ----
    "10_volkswagen_tiguan_cz": {
        "url": "https://www.volkswagen.cz/modely/tiguan",
        "keywords": {
            "PHEV": ["plug-in hybrid", "eHybrid", "Tiguan eHybrid"],
            "MHEV": ["mild hybrid", "eTSI", "48V"],
            "ICE": ["benzín", "nafta", "TSI", "TDI"],
        },
    },
    "24_volkswagen_tayron_cz": {
        "url": "https://www.volkswagen.cz/modely/tayron",
        "keywords": {
            "PHEV": ["plug-in hybrid", "eHybrid", "Tayron eHybrid"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzín", "nafta", "TSI", "TDI"],
        },
    },
    # ---- KIA ----
    "11_kia_sportage_cz": {
        "url": "https://www.kia.com/cz/modely/sportage/",
        "keywords": {
            "PHEV": ["plug-in hybrid", "Plug-in Hybrid"],
            "HEV": ["hybrid", "hybridní", "HEV"],
            "MHEV": ["mild hybrid", "48V", "MHEV"],
            "ICE": ["benzín", "nafta"],
        },
    },
    "29_kia_stonic_cz": {
        "url": "https://www.kia.com/cz/modely/stonic/",
        "keywords": {
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzín"],
        },
    },
    # ---- Renault ----
    "12_renault_captur_cz": {
        "url": "https://www.renault.cz/vozidla/captur.html",
        "keywords": {
            "HEV": ["hybrid", "E-Tech Hybrid", "E-Tech Full Hybrid"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzín", "TCe"],
        },
    },
    # ---- Ford ----
    "15_ford_kuga_cz": {
        "url": "https://www.ford.cz/suvs-crossovers/kuga",
        "keywords": {
            "PHEV": ["plug-in hybrid", "Plug-In Hybrid", "PHEV"],
            "HEV": ["hybrid", "Full Hybrid", "FHEV"],
            "ICE": ["benzín", "nafta", "EcoBoost", "EcoBlue"],
        },
    },
    "20_ford_puma_cz": {
        "url": "https://www.ford.cz/suvs-crossovers/puma",
        "keywords": {
            "MHEV": ["mild hybrid", "EcoBoost Hybrid", "48V"],
            "ICE": ["benzín", "EcoBoost"],
        },
    },
    # ---- Peugeot ----
    "17_peugeot_2008_cz": {
        "url": "https://www.peugeot.cz/modely/suv-2008.html",
        "keywords": {
            "BEV": ["elektrický", "e-2008", "elektromobil"],
            "ICE": ["benzín", "PureTech"],
        },
    },
    # ---- Suzuki ----
    "18_suzuki_s_cross_cz": {
        "url": "https://www.suzuki.cz/automobily/s-cross",
        "keywords": {
            "HEV": ["hybrid", "Full Hybrid", "híbridní"],
            "MHEV": ["mild hybrid", "48V", "SHVS"],
        },
    },
    "30_suzuki_vitara_cz": {
        "url": "https://www.suzuki.cz/automobily/vitara",
        "keywords": {
            "HEV": ["hybrid", "Full Hybrid"],
            "MHEV": ["mild hybrid", "48V", "SHVS"],
        },
    },
    # ---- KGM (SsangYong) ----
    "19_kgm_korando_cz": {
        "url": "https://www.kgm-motors.cz/modely/korando",
        "keywords": {
            "BEV": ["elektrický", "e-Motion", "electric"],
            "ICE": ["benzín", "nafta"],
        },
    },
    # ---- Tesla ----
    "21_tesla_model_y_cz": {
        "url": "https://www.tesla.com/cs_cz/modely",
        "keywords": {
            "BEV": ["elektrický", "electric", "Model Y", "čistě elektrický"],
        },
    },
    # ---- Cupra ----
    "22_cupra_formentor_cz": {
        "url": "https://www.cupraofficial.cz/modely/formentor",
        "keywords": {
            "PHEV": ["plug-in hybrid", "eHybrid", "VZ e-HYBRID"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzín", "TSI"],
        },
    },
    # ---- Jaecoo ----
    "25_jaecoo_7_cz": {
        "url": "https://www.jaecoo.cz/j7",
        "keywords": {
            "PHEV": ["plug-in hybrid", "PHEV"],
            "ICE": ["benzín"],
        },
    },
    # ---- Nissan ----
    "26_nissan_qashqai_cz": {
        "url": "https://www.nissan.cz/vozidla/nove-vozy/qashqai.html",
        "keywords": {
            "MHEV": ["mild hybrid", "48V", "MHEV"],
            "HEV": ["hybrid", "e-POWER"],
            "ICE": ["benzín"],
        },
    },
    # ---- Volvo ----
    "27_volvo_xc90_cz": {
        "url": "https://www.volvocars.com/cz/cars/xc90/",
        "keywords": {
            "PHEV": ["plug-in hybrid", "Recharge", "T8"],
            "MHEV": ["mild hybrid", "B5", "B6"],
            "ICE": ["benzín", "nafta"],
        },
    },
    "28_volvo_xc60_cz": {
        "url": "https://www.volvocars.com/cz/cars/xc60/",
        "keywords": {
            "PHEV": ["plug-in hybrid", "Recharge", "T8"],
            "MHEV": ["mild hybrid", "B5", "B6"],
            "ICE": ["benzín", "nafta"],
        },
    },
}


def apply_updates(path: Path, entry: dict[str, Any], *, dry_run: bool) -> bool:
    """Apply URL, currency, and keyword replacements.  Returns True if changed."""
    raw = path.read_text(encoding="utf-8")
    original = raw

    url = entry["url"]
    kw_map = entry["keywords"]

    raw = re.sub(
        r"(source_url:\s+)https://todo\.invalid/\S+",
        rf"\g<1>{url}",
        raw,
    )
    raw = re.sub(
        r"(  url:\s+)https://todo\.invalid/\S+",
        rf"\g<1>{url}",
        raw,
    )
    raw = re.sub(
        r"(default_currency:\s+)TODO\b",
        rf"\g<1>{CURRENCY}",
        raw,
    )

    for pt, keywords in kw_map.items():
        placeholder = f"TODO_{pt}_KEYWORD"
        if placeholder in raw:
            kw_lines = "\n".join(f"      - {kw}" for kw in keywords)
            raw = re.sub(
                rf"(\s+keywords:\n)\s+- {re.escape(placeholder)}",
                rf"\1{kw_lines}",
                raw,
            )

    raw = re.sub(
        r"notes: Draft scaffold generated from country×model top30 backlog\.[^\n]+(?:\n  [^\n]+)*",
        (
            "notes: Keywords filled from official manufacturer website research (CZ market).\n"
            "  CSS selectors still require browser-based inspection before promotion."
        ),
        raw,
    )

    changed = raw != original
    if changed and not dry_run:
        path.write_text(raw, encoding="utf-8")
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing files.")
    args = parser.parse_args()

    if not DRAFT_DIR.is_dir():
        print(f"ERROR: {DRAFT_DIR} not found", file=sys.stderr)
        sys.exit(1)

    matched = skipped = 0
    for yaml_path in sorted(DRAFT_DIR.glob("*.yaml")):
        stem = yaml_path.stem
        if stem not in REGISTRY:
            print(f"  SKIP (no registry entry): {yaml_path.name}")
            skipped += 1
            continue
        entry = REGISTRY[stem]
        changed = apply_updates(yaml_path, entry, dry_run=args.dry_run)
        tag = "WOULD UPDATE" if args.dry_run else "UPDATED"
        if changed:
            print(f"  {tag}: {yaml_path.name}")
        else:
            print(f"  NO CHANGE: {yaml_path.name}")
        matched += 1

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"\n[{mode}] matched={matched}  skipped={skipped}")


if __name__ == "__main__":
    main()
