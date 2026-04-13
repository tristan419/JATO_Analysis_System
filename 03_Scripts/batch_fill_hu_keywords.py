#!/usr/bin/env python3
"""
Batch-fill powertrain keywords, source URLs, and currency for all 30
Hungarian (hu) draft YAML files that still contain todo.invalid placeholders.

Hungarian keyword evidence:
  - Suzuki HU (auto.suzuki.hu): "S-CROSS HIBRID", "VITARA HIBRID",
    "LÁGY HIBRID" (mild hybrid), "eVITARA" (electric, 14,190,000 Ft)
    Prices in Ft (HUF): S-Cross from 9,922,500 Ft, Vitara from 9,462,500 Ft
  - Toyota HU (toyota.hu): "Hybrid" / "Plug-in hibrid", RAV4 from 17,660,000 Ft
    "2.5 TNGA PHEV", "Hybrid Synergy Drive"
  - Hyundai HU (hyundai.hu): Categories: "Belsőégésű" (ICE), "Hybrid", "Elektromos"
    TUCSON Hybrid, TUCSON Plug-in Hybrid, KONA / KONA Electric
  - General Hungarian automotive terminology:
    BEV  = "elektromos", "electric", "tisztán elektromos", "villany"
    PHEV = "plug-in hibrid", "plug-in hybrid", "tölthetõ hibrid"
    HEV  = "hibrid", "hybrid", "öntöltő hibrid"
    MHEV = "lágy hibrid", "mild hybrid", "MHEV", "48V"
    ICE  = "benzin", "benzines", "dízel", "diesel"
  - Currency: HUF (Ft)

Run:
    python 03_Scripts/batch_fill_hu_keywords.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DRAFT_DIR = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30" / "hu"

CURRENCY = "HUF"

# --------------- keyword / URL registry ---------------
REGISTRY: dict[str, dict[str, Any]] = {
    # ---- Suzuki (verified from auto.suzuki.hu) ----
    "01_suzuki_s_cross_hu": {
        "url": "https://auto.suzuki.hu/cars/s-cross",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "S-CROSS HIBRID", "Hybrid Synergy"],
            "MHEV": ["lágy hibrid", "mild hybrid", "48V", "MHEV"],
            "ICE": ["benzin", "benzines"],
        },
    },
    "02_suzuki_vitara_hu": {
        "url": "https://auto.suzuki.hu/cars/vitara",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "VITARA HIBRID"],
            "MHEV": ["lágy hibrid", "mild hybrid", "48V", "MHEV"],
            "ICE": ["benzin", "benzines"],
        },
    },
    # ---- Nissan ----
    "03_nissan_qashqai_hu": {
        "url": "https://www.nissan.hu/jarmuvek/uj-jarmuvek/qashqai.html",
        "keywords": {
            "HEV": ["e-POWER", "e-POWER Hybrid", "hibrid"],
            "MHEV": ["mild hybrid", "MHEV", "48V"],
            "ICE": ["benzin", "dízel", "diesel"],
        },
    },
    "18_nissan_x_trail_hu": {
        "url": "https://www.nissan.hu/jarmuvek/uj-jarmuvek/x-trail.html",
        "keywords": {
            "HEV": ["e-POWER", "e-POWER Hybrid", "hibrid"],
            "ICE": ["benzin", "dízel"],
        },
    },
    # ---- Toyota (verified from toyota.hu) ----
    "04_toyota_yaris_cross_hu": {
        "url": "https://www.toyota.hu/new-cars/yaris-cross",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "Hybrid Synergy Drive"],
        },
    },
    "06_toyota_c_hr_hu": {
        "url": "https://www.toyota.hu/new-cars/c-hr",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "Plug-in Hybrid 220"],
            "HEV": ["hibrid", "hybrid", "Hybrid Synergy Drive"],
        },
    },
    "09_toyota_corolla_cross_hu": {
        "url": "https://www.toyota.hu/new-cars/corolla-cross",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "Hybrid Synergy Drive"],
        },
    },
    "17_toyota_rav4_hu": {
        "url": "https://www.toyota.hu/new-cars/rav4",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "2.5 TNGA PHEV"],
            "HEV": ["hibrid", "hybrid", "Hybrid Synergy Drive"],
        },
    },
    "29_toyota_aygo_x_hu": {
        "url": "https://www.toyota.hu/new-cars/aygo-x",
        "keywords": {
            "ICE": ["benzin", "benzines", "1.0 VVT-i"],
        },
    },
    # ---- KIA ----
    "07_kia_sportage_hu": {
        "url": "https://www.kia.com/hu/modellek/sportage/",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "Sportage Plug-In Hybrid"],
            "HEV": ["hibrid", "hybrid", "Sportage Hybrid"],
            "MHEV": ["mild hybrid", "MHEV", "48V"],
            "ICE": ["benzin", "dízel", "diesel", "1.6 T-GDi"],
        },
    },
    # ---- Hyundai (verified from hyundai.hu) ----
    "08_hyundai_tucson_hu": {
        "url": "https://hyundai.hu/modellek/tucson-hybrid-facelift/",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "Plug-in Hybrid Facelift"],
            "HEV": ["hibrid", "hybrid", "Tucson Hybrid"],
            "MHEV": ["mild hybrid", "MHEV", "48V"],
            "ICE": ["benzin", "dízel", "diesel"],
        },
    },
    # ---- Ford ----
    "10_ford_kuga_hu": {
        "url": "https://www.ford.hu/suvs-crossovers/kuga",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "PHEV"],
            "HEV": ["hibrid", "hybrid", "Full Hybrid"],
            "MHEV": ["mild hybrid", "EcoBlue Hybrid", "MHEV", "48V"],
            "ICE": ["benzin", "dízel", "diesel", "EcoBoost", "EcoBlue"],
        },
    },
    "11_ford_puma_hu": {
        "url": "https://www.ford.hu/suvs-crossovers/puma",
        "keywords": {
            "MHEV": ["mild hybrid", "EcoBoost Hybrid", "MHEV", "48V"],
            "ICE": ["benzin", "EcoBoost"],
        },
    },
    # ---- VW group ----
    "12_volkswagen_t_roc_hu": {
        "url": "https://www.volkswagen.hu/hu/modellek/t-roc.html",
        "keywords": {
            "MHEV": ["mild hybrid", "eTSI", "hybrid"],
            "ICE": ["benzin", "TSI"],
        },
    },
    "16_volkswagen_tiguan_hu": {
        "url": "https://www.volkswagen.hu/hu/modellek/tiguan.html",
        "keywords": {
            "PHEV": ["eHybrid", "plug-in hibrid", "plug-in hybrid", "Tiguan eHybrid"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzin", "dízel", "TSI", "TDI"],
        },
    },
    # ---- Skoda (VW group) ----
    "13_skoda_kodiaq_hu": {
        "url": "https://www.skoda.hu/modelle/kodiaq",
        "keywords": {
            "PHEV": ["plug-in hibrid", "iV", "plug-in hybrid", "Kodiaq iV"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzin", "dízel", "TSI", "TDI"],
        },
    },
    # ---- MG ----
    "14_mg_zs_hu": {
        "url": "https://www.mgmotor.hu/model/mg-zs",
        "keywords": {
            "BEV": ["elektromos", "electric", "ZS EV", "tisztán elektromos"],
            "HEV": ["hibrid", "hybrid", "ZS Hybrid+"],
            "ICE": ["benzin", "benzines"],
        },
    },
    # ---- Tesla ----
    "15_tesla_model_y_hu": {
        "url": "https://www.tesla.com/hu_hu/modely",
        "keywords": {
            "BEV": ["elektromos", "electric", "Model Y", "long range", "performance"],
        },
    },
    # ---- Renault ----
    "19_renault_captur_hu": {
        "url": "https://www.renault.hu/modellvalasztek/captur.html",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "E-TECH Hybrid", "E-TECH Full Hybrid"],
            "MHEV": ["mild hybrid", "48V", "TCe"],
            "ICE": ["benzin", "TCe"],
        },
    },
    # ---- KGM (SsangYong) ----
    "20_kgm_korando_hu": {
        "url": "https://www.kgmmotors.hu/korando",
        "keywords": {
            "ICE": ["benzin", "dízel", "diesel"],
        },
    },
    # ---- Volvo ----
    "21_volvo_xc60_hu": {
        "url": "https://www.volvocars.com/hu/build/xc60/",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "recharge", "T6", "T8"],
            "MHEV": ["mild hybrid", "MHEV", "B4", "B5", "B6"],
            "ICE": ["benzin", "dízel"],
        },
    },
    "26_volvo_xc40_hu": {
        "url": "https://www.volvocars.com/hu/build/xc40/",
        "keywords": {
            "BEV": ["elektromos", "electric", "single motor", "twin motor"],
            "PHEV": ["plug-in hibrid", "recharge", "T4", "T5"],
            "MHEV": ["mild hybrid", "MHEV", "B3", "B4", "B5"],
            "ICE": ["benzin", "dízel"],
        },
    },
    # ---- Peugeot (Stellantis) ----
    "22_peugeot_3008_hu": {
        "url": "https://www.peugeot.hu/modelljeink/3008-suv.html",
        "keywords": {
            "BEV": ["elektromos", "electric", "E-3008", "tisztán elektromos"],
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "HYBRID"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzin", "PureTech"],
        },
    },
    "28_peugeot_2008_hu": {
        "url": "https://www.peugeot.hu/modelljeink/2008-suv.html",
        "keywords": {
            "BEV": ["elektromos", "electric", "E-2008", "tisztán elektromos"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzin", "PureTech"],
        },
    },
    # ---- Jaecoo (Chinese brand) ----
    "23_jaecoo_7_hu": {
        "url": "https://www.jaecoo.com/hu/models/j7",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "PHEV"],
            "ICE": ["benzin", "benzines"],
        },
    },
    # ---- BMW ----
    "24_bmw_x5_hu": {
        "url": "https://www.bmw.hu/hu/modellek/x-sorozat/x5.html",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "xDrive50e", "iPerformance"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzin", "dízel", "diesel", "xDrive30d", "xDrive40i"],
        },
    },
    # ---- Omoda (Chery) ----
    "25_omoda_5_hu": {
        "url": "https://www.omoda.com/hu/models/omoda-5",
        "keywords": {
            "BEV": ["elektromos", "electric", "Omoda 5 EV", "tisztán elektromos"],
            "ICE": ["benzin", "benzines", "1.5T"],
        },
    },
    # ---- Opel (Stellantis) ----
    "27_opel_frontera_hu": {
        "url": "https://www.opel.hu/modellek/frontera.html",
        "keywords": {
            "BEV": ["elektromos", "electric", "Frontera Electric", "tisztán elektromos"],
            "HEV": ["hibrid", "hybrid"],
            "ICE": ["benzin", "benzines"],
        },
    },
    # ---- Dacia ----
    "05_dacia_duster_hu": {
        "url": "https://www.dacia.hu/modelljeink/duster.html",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "Hybrid 140"],
            "MHEV": ["mild hybrid", "48V", "TCe"],
            "ICE": ["benzin", "TCe", "ECO-G"],
        },
    },
    "30_dacia_bigster_hu": {
        "url": "https://www.dacia.hu/modelljeink/bigster.html",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "Hybrid 155"],
            "MHEV": ["mild hybrid", "48V", "TCe"],
            "ICE": ["benzin", "TCe"],
        },
    },
}


def apply_updates(path: Path, entry: dict[str, Any], *, dry_run: bool) -> bool:
    """Apply URL, currency, and keyword replacements.  Returns True if changed."""
    raw = path.read_text(encoding="utf-8")
    original = raw

    url = entry["url"]
    kw_map = entry["keywords"]

    # 1) Replace source_url
    raw = re.sub(
        r"(source_url:\s+)https://todo\.invalid/\S+",
        rf"\g<1>{url}",
        raw,
    )

    # 2) Replace profile.url (indented)
    raw = re.sub(
        r"(  url:\s+)https://todo\.invalid/\S+",
        rf"\g<1>{url}",
        raw,
    )

    # 3) Replace default_currency: TODO
    raw = re.sub(
        r"(default_currency:\s+)TODO\b",
        rf"\g<1>{CURRENCY}",
        raw,
    )

    # 4) Replace TODO_*_KEYWORD entries in powertrain_rules
    for pt, keywords in kw_map.items():
        placeholder = f"TODO_{pt}_KEYWORD"
        if placeholder in raw:
            kw_lines = "\n".join(f"      - {kw}" for kw in keywords)
            raw = re.sub(
                rf"(\s+keywords:\n)\s+- {re.escape(placeholder)}",
                rf"\1{kw_lines}",
                raw,
            )

    # 5) Update notes
    raw = re.sub(
        r"notes: Draft scaffold generated from country×model top30 backlog\.[^\n]+(?:\n  [^\n]+)*",
        (
            "notes: Keywords filled from official manufacturer website research (HU market).\n"
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
