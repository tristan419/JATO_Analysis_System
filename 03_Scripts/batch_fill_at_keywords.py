#!/usr/bin/env python3
"""
Batch-fill powertrain keywords, source URLs, and currency for all 30
Austrian (at) draft YAML files that still contain todo.invalid placeholders.

German automotive terminology (Austria):
  BEV  = "Elektro", "elektrisch", "vollelektrisch", "electric"
  PHEV = "Plug-in-Hybrid", "Plug-in Hybrid", "PHEV"
  HEV  = "Hybrid", "Vollhybrid", "Full Hybrid"
  MHEV = "Mild-Hybrid", "mild hybrid", "MHEV", "48V"
  ICE  = "Benzin", "Diesel"
  Currency: EUR

Run:
    python 03_Scripts/batch_fill_at_keywords.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DRAFT_DIR = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30" / "at"

CURRENCY = "EUR"

# --------------- keyword / URL registry ---------------
REGISTRY: dict[str, dict[str, Any]] = {
    # ---- Tesla (BEV) ----
    "01_tesla_model_y_at": {
        "url": "https://www.tesla.com/de_at/modely",
        "keywords": {
            "BEV": ["Elektro", "electric", "Model Y", "vollelektrisch", "Langstrecke"],
        },
    },
    # ---- VW group ----
    "02_volkswagen_tiguan_at": {
        "url": "https://www.volkswagen.at/modelle/tiguan",
        "keywords": {
            "PHEV": ["eHybrid", "Plug-in-Hybrid", "Plug-in Hybrid", "Tiguan eHybrid"],
            "MHEV": ["Mild-Hybrid", "eTSI", "mild hybrid"],
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    "10_volkswagen_t_cross_at": {
        "url": "https://www.volkswagen.at/modelle/t-cross",
        "keywords": {
            "ICE": ["Benzin", "TSI"],
        },
    },
    "13_volkswagen_t_roc_at": {
        "url": "https://www.volkswagen.at/modelle/t-roc",
        "keywords": {
            "MHEV": ["Mild-Hybrid", "eTSI", "mild hybrid"],
            "ICE": ["Benzin", "TSI"],
        },
    },
    "29_volkswagen_id_4_at": {
        "url": "https://www.volkswagen.at/modelle/id4",
        "keywords": {
            "BEV": ["Elektro", "ID.4", "vollelektrisch", "elektrisch"],
        },
    },
    # ---- Skoda (VW group) ----
    "03_skoda_karoq_at": {
        "url": "https://www.skoda.at/modelle/karoq",
        "keywords": {
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    "06_skoda_elroq_at": {
        "url": "https://www.skoda.at/modelle/elroq",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "Elroq", "elektrisch"],
        },
    },
    "16_skoda_enyaq_at": {
        "url": "https://www.skoda.at/modelle/enyaq",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "Enyaq", "elektrisch"],
        },
    },
    "17_skoda_kodiaq_at": {
        "url": "https://www.skoda.at/modelle/kodiaq",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "iV", "Kodiaq iV"],
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    "20_skoda_kamiq_at": {
        "url": "https://www.skoda.at/modelle/kamiq",
        "keywords": {
            "ICE": ["Benzin", "TSI"],
        },
    },
    # ---- SEAT / Cupra (VW group) ----
    "18_seat_arona_at": {
        "url": "https://www.seat.at/modelle/arona",
        "keywords": {
            "ICE": ["Benzin", "TSI"],
        },
    },
    "27_seat_ateca_at": {
        "url": "https://www.seat.at/modelle/ateca",
        "keywords": {
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    "09_cupra_terramar_at": {
        "url": "https://www.cupraofficial.at/modelle/terramar",
        "keywords": {
            "PHEV": ["eHybrid", "Plug-in-Hybrid", "VZ e-HYBRID"],
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "TSI"],
        },
    },
    "24_cupra_formentor_at": {
        "url": "https://www.cupraofficial.at/modelle/formentor",
        "keywords": {
            "PHEV": ["eHybrid", "Plug-in-Hybrid", "VZ e-HYBRID"],
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "TSI"],
        },
    },
    # ---- Dacia ----
    "04_dacia_bigster_at": {
        "url": "https://www.dacia.at/modelle/bigster.html",
        "keywords": {
            "HEV": ["Hybrid", "Hybrid 155", "Vollhybrid"],
            "MHEV": ["Mild-Hybrid", "48V", "TCe"],
            "ICE": ["Benzin", "TCe"],
        },
    },
    "05_dacia_duster_at": {
        "url": "https://www.dacia.at/modelle/duster.html",
        "keywords": {
            "HEV": ["Hybrid", "Hybrid 140", "Vollhybrid"],
            "MHEV": ["Mild-Hybrid", "48V", "TCe"],
            "ICE": ["Benzin", "TCe", "ECO-G"],
        },
    },
    # ---- MG ----
    "07_mg_zs_at": {
        "url": "https://www.mgmotor.at/modelle/mg-zs",
        "keywords": {
            "BEV": ["Elektro", "electric", "ZS EV", "vollelektrisch"],
            "HEV": ["Hybrid", "ZS Hybrid+"],
            "ICE": ["Benzin"],
        },
    },
    # ---- Hyundai ----
    "08_hyundai_tucson_at": {
        "url": "https://www.hyundai.com/at/de/modelle/tucson.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "Plug-in Hybrid", "Tucson Plug-in Hybrid"],
            "HEV": ["Hybrid", "Tucson Hybrid", "Vollhybrid"],
            "MHEV": ["Mild-Hybrid", "48V", "MHEV"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    "23_hyundai_kona_at": {
        "url": "https://www.hyundai.com/at/de/modelle/kona.html",
        "keywords": {
            "BEV": ["Elektro", "KONA Electric", "vollelektrisch"],
            "HEV": ["Hybrid", "KONA Hybrid"],
            "ICE": ["Benzin"],
        },
    },
    # ---- Mazda ----
    "11_mazda_cx_30_at": {
        "url": "https://www.mazda.at/modelle/mazda-cx-30/",
        "keywords": {
            "MHEV": ["Mild-Hybrid", "M Hybrid", "e-Skyactiv", "24V"],
            "ICE": ["Benzin", "Diesel", "Skyactiv-G", "Skyactiv-D"],
        },
    },
    # ---- BMW ----
    "12_bmw_x1_at": {
        "url": "https://www.bmw.at/de/modelle/x-modelle/x1.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "xDrive25e", "xDrive30e"],
            "MHEV": ["Mild-Hybrid", "48V"],
            "ICE": ["Benzin", "Diesel", "sDrive18i", "xDrive23d"],
        },
    },
    "19_bmw_ix1_at": {
        "url": "https://www.bmw.at/de/modelle/x-modelle/ix1.html",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "iX1", "elektrisch"],
        },
    },
    "21_bmw_x3_at": {
        "url": "https://www.bmw.at/de/modelle/x-modelle/x3.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "xDrive30e"],
            "MHEV": ["Mild-Hybrid", "48V"],
            "ICE": ["Benzin", "Diesel", "xDrive20i", "xDrive20d"],
        },
    },
    # ---- Toyota ----
    "14_toyota_yaris_cross_at": {
        "url": "https://www.toyota.at/new-cars/yaris-cross",
        "keywords": {
            "HEV": ["Hybrid", "Hybrid Synergy Drive", "Vollhybrid"],
        },
    },
    # ---- BYD ----
    "15_byd_seal_u_at": {
        "url": "https://www.byd.com/at/car/seal-u",
        "keywords": {
            "BEV": ["Elektro", "electric", "Seal U", "vollelektrisch"],
            "PHEV": ["Plug-in-Hybrid", "DM-i", "Seal U DM-i"],
        },
    },
    "22_byd_sealion_7_at": {
        "url": "https://www.byd.com/at/car/seal-ion-7",
        "keywords": {
            "BEV": ["Elektro", "electric", "Sealion 7", "vollelektrisch"],
        },
    },
    # ---- Mercedes ----
    "26_mercedes_glc_at": {
        "url": "https://www.mercedes-benz.at/passengercars/models/suv/glc/overview.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "EQ Power", "GLC 300 e"],
            "MHEV": ["Mild-Hybrid", "48V", "EQ Boost"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    # ---- Peugeot (Stellantis) ----
    "28_peugeot_3008_at": {
        "url": "https://www.peugeot.at/modelle/suv-3008.html",
        "keywords": {
            "BEV": ["Elektro", "E-3008", "vollelektrisch"],
            "PHEV": ["Plug-in-Hybrid", "HYBRID"],
            "MHEV": ["Mild-Hybrid", "48V"],
            "ICE": ["Benzin", "PureTech"],
        },
    },
    # ---- Ford ----
    "30_ford_puma_at": {
        "url": "https://www.ford.at/suvs-crossovers/puma",
        "keywords": {
            "MHEV": ["Mild-Hybrid", "EcoBoost Hybrid", "48V"],
            "ICE": ["Benzin", "EcoBoost"],
        },
    },
    # ---- Audi ----
    "25_audi_q8_at": {
        "url": "https://www.audi.at/modelle/q8",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "TFSI e", "Q8 TFSI e"],
            "MHEV": ["Mild-Hybrid", "48V", "MHEV"],
            "ICE": ["Benzin", "Diesel", "TFSI", "TDI"],
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
            "notes: Keywords filled from official manufacturer website research (AT market).\n"
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
