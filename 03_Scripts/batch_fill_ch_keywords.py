#!/usr/bin/env python3
"""
Batch-fill powertrain keywords, source URLs, and currency for all 30
Swiss (ch) draft YAML files that still contain todo.invalid placeholders.

Swiss-German automotive terminology (same as AT/DE):
  BEV  = "Elektro", "elektrisch", "vollelektrisch"
  PHEV = "Plug-in-Hybrid", "Plug-in Hybrid"
  HEV  = "Hybrid", "Vollhybrid", "Full Hybrid"
  MHEV = "Mild-Hybrid", "mild hybrid", "48V"
  ICE  = "Benzin", "Diesel"
  Currency: CHF

Run:
    python 03_Scripts/batch_fill_ch_keywords.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DRAFT_DIR = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30" / "ch"

CURRENCY = "CHF"

# --------------- keyword / URL registry ---------------
REGISTRY: dict[str, dict[str, Any]] = {
    # ---- VW group ----
    "01_volkswagen_tiguan_ch": {
        "url": "https://www.volkswagen.ch/de/modelle/tiguan.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "eHybrid", "Tiguan eHybrid"],
            "MHEV": ["Mild-Hybrid", "eTSI", "mild hybrid"],
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    "21_volkswagen_t_roc_ch": {
        "url": "https://www.volkswagen.ch/de/modelle/t-roc.html",
        "keywords": {
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "TSI"],
        },
    },
    "28_volkswagen_tayron_ch": {
        "url": "https://www.volkswagen.ch/de/modelle/tayron.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "eHybrid", "Tayron eHybrid"],
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    # ---- Tesla ----
    "02_tesla_model_y_ch": {
        "url": "https://www.tesla.com/de_ch/modely",
        "keywords": {
            "BEV": ["Elektro", "electric", "Model Y", "vollelektrisch"],
        },
    },
    # ---- Skoda ----
    "03_skoda_kodiaq_ch": {
        "url": "https://www.skoda.ch/modelle/kodiaq",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "iV", "Kodiaq iV"],
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    "04_skoda_elroq_ch": {
        "url": "https://www.skoda.ch/modelle/elroq",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "Elroq", "elektrisch"],
        },
    },
    "07_skoda_karoq_ch": {
        "url": "https://www.skoda.ch/modelle/karoq",
        "keywords": {
            "ICE": ["Benzin", "Diesel", "TSI", "TDI"],
        },
    },
    "09_skoda_enyaq_ch": {
        "url": "https://www.skoda.ch/modelle/enyaq",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "Enyaq", "elektrisch"],
        },
    },
    # ---- Mercedes ----
    "05_mercedes_glc_ch": {
        "url": "https://www.mercedes-benz.ch/de/passengercars/models/suv/glc/overview.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "EQ Power", "GLC 300 e"],
            "MHEV": ["Mild-Hybrid", "48V", "EQ Boost"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    "15_mercedes_gla_ch": {
        "url": "https://www.mercedes-benz.ch/de/passengercars/models/suv/gla/overview.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "EQ Power", "GLA 250 e"],
            "MHEV": ["Mild-Hybrid", "48V", "EQ Boost"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    "26_mercedes_gle_ch": {
        "url": "https://www.mercedes-benz.ch/de/passengercars/models/suv/gle/overview.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "EQ Power", "GLE 350 de"],
            "MHEV": ["Mild-Hybrid", "48V", "EQ Boost"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    # ---- BMW ----
    "06_bmw_x1_ch": {
        "url": "https://www.bmw.ch/de/modelle/x-modelle/x1.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "xDrive25e", "xDrive30e"],
            "MHEV": ["Mild-Hybrid", "48V"],
            "ICE": ["Benzin", "Diesel", "sDrive18i", "xDrive23d"],
        },
    },
    "08_bmw_x3_ch": {
        "url": "https://www.bmw.ch/de/modelle/x-modelle/x3.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "xDrive30e"],
            "MHEV": ["Mild-Hybrid", "48V"],
            "ICE": ["Benzin", "Diesel", "xDrive20i", "xDrive20d"],
        },
    },
    "18_bmw_x5_ch": {
        "url": "https://www.bmw.ch/de/modelle/x-modelle/x5.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "xDrive50e"],
            "MHEV": ["Mild-Hybrid", "48V"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    "24_bmw_ix1_ch": {
        "url": "https://www.bmw.ch/de/modelle/x-modelle/ix1.html",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "iX1", "elektrisch"],
        },
    },
    # ---- Volvo ----
    "10_volvo_xc60_ch": {
        "url": "https://www.volvocars.com/ch/cars/xc60/",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "Recharge", "T8"],
            "MHEV": ["Mild-Hybrid", "B5", "B6"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    "12_volvo_ex30_ch": {
        "url": "https://www.volvocars.com/ch/cars/ex30/",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "EX30", "elektrisch"],
        },
    },
    # ---- Dacia ----
    "11_dacia_duster_ch": {
        "url": "https://www.dacia.ch/modelle/duster.html",
        "keywords": {
            "HEV": ["Hybrid", "Hybrid 140", "Vollhybrid"],
            "MHEV": ["Mild-Hybrid", "48V", "TCe"],
            "ICE": ["Benzin", "TCe", "ECO-G"],
        },
    },
    "19_dacia_bigster_ch": {
        "url": "https://www.dacia.ch/modelle/bigster.html",
        "keywords": {
            "HEV": ["Hybrid", "Hybrid 155", "Vollhybrid"],
            "MHEV": ["Mild-Hybrid", "48V", "TCe"],
            "ICE": ["Benzin", "TCe"],
        },
    },
    # ---- KIA ----
    "13_kia_sportage_ch": {
        "url": "https://www.kia.com/ch/de/modelle/sportage/",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "Plug-in Hybrid"],
            "HEV": ["Hybrid", "Vollhybrid", "HEV"],
            "MHEV": ["Mild-Hybrid", "48V", "MHEV"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    # ---- Cupra ----
    "14_cupra_terramar_ch": {
        "url": "https://www.cupraofficial.ch/de/modelle/terramar",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "eHybrid", "VZ e-HYBRID"],
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "TSI"],
        },
    },
    "29_cupra_formentor_ch": {
        "url": "https://www.cupraofficial.ch/de/modelle/formentor",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "eHybrid", "VZ e-HYBRID"],
            "MHEV": ["Mild-Hybrid", "eTSI"],
            "ICE": ["Benzin", "TSI"],
        },
    },
    # ---- Hyundai ----
    "16_hyundai_tucson_ch": {
        "url": "https://www.hyundai.com/ch/de/modelle/tucson.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "Plug-in Hybrid", "Tucson Plug-in Hybrid"],
            "HEV": ["Hybrid", "Tucson Hybrid", "Vollhybrid"],
            "MHEV": ["Mild-Hybrid", "48V", "MHEV"],
            "ICE": ["Benzin", "Diesel"],
        },
    },
    "27_hyundai_kona_ch": {
        "url": "https://www.hyundai.com/ch/de/modelle/kona.html",
        "keywords": {
            "BEV": ["Elektro", "KONA Electric", "vollelektrisch"],
            "HEV": ["Hybrid", "KONA Hybrid"],
            "ICE": ["Benzin"],
        },
    },
    # ---- Audi ----
    "17_audi_q3_sportback_ch": {
        "url": "https://www.audi.ch/de/modelle/q3-sportback.html",
        "keywords": {
            "MHEV": ["Mild-Hybrid", "48V", "MHEV"],
            "ICE": ["Benzin", "Diesel", "TFSI", "TDI"],
        },
    },
    "22_audi_q5_ch": {
        "url": "https://www.audi.ch/de/modelle/q5.html",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "TFSI e", "Q5 TFSI e"],
            "MHEV": ["Mild-Hybrid", "48V", "MHEV"],
            "ICE": ["Benzin", "Diesel", "TFSI", "TDI"],
        },
    },
    "23_audi_q4_e_tron_ch": {
        "url": "https://www.audi.ch/de/modelle/q4-e-tron.html",
        "keywords": {
            "BEV": ["Elektro", "vollelektrisch", "e-tron", "elektrisch"],
        },
    },
    "25_audi_q3_ch": {
        "url": "https://www.audi.ch/de/modelle/q3.html",
        "keywords": {
            "MHEV": ["Mild-Hybrid", "48V", "MHEV"],
            "ICE": ["Benzin", "Diesel", "TFSI", "TDI"],
        },
    },
    # ---- Toyota ----
    "20_toyota_yaris_cross_ch": {
        "url": "https://www.toyota.ch/new-cars/yaris-cross",
        "keywords": {
            "HEV": ["Hybrid", "Hybrid Synergy Drive", "Vollhybrid"],
        },
    },
    "30_toyota_rav4_ch": {
        "url": "https://www.toyota.ch/new-cars/rav4",
        "keywords": {
            "PHEV": ["Plug-in-Hybrid", "Plug-in Hybrid", "RAV4 PHEV"],
            "HEV": ["Hybrid", "Hybrid Synergy Drive", "Vollhybrid"],
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
            "notes: Keywords filled from official manufacturer website research (CH market).\n"
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
