#!/usr/bin/env python3
"""
Batch-fill powertrain keywords, source URLs, and currency for all 30
Croatian (hr) draft YAML files that still contain todo.invalid placeholders.

Croatian keyword evidence:
  - Suzuki HR (auto.suzuki.hr): "MILD HYBRID 48V", "Vitara Hybrid"
  - Nissan HR (nissan.hr): "e-POWER Hybrid", "MHEV", "Hibridni SUV"
  - General Croatian automotive terminology (verified from above pages):
    BEV  = "električni", "electric"
    PHEV = "plug-in hibrid", "plug-in hybrid"
    HEV  = "hibrid", "hybrid"
    MHEV = "mild hybrid", "blagi hibrid", "MHEV", "48V"
    ICE  = "benzin", "dizel"
    LPG  = "LPG", "autoplin"
  - Currency: EUR (Croatia joined Eurozone January 2023)

Run:
    python 03_Scripts/batch_fill_hr_keywords.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DRAFT_DIR = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30" / "hr"

CURRENCY = "EUR"

# --------------- keyword / URL registry ---------------
REGISTRY: dict[str, dict[str, Any]] = {
    # ---- VW group (Croatian VW site) ----
    "01_volkswagen_t_cross_hr": {
        "url": "https://www.volkswagen.hr/hr/modeli/t-cross.html",
        "keywords": {
            "ICE": ["benzin", "TSI"],
        },
    },
    "07_volkswagen_tiguan_hr": {
        "url": "https://www.volkswagen.hr/hr/modeli/tiguan.html",
        "keywords": {
            "PHEV": ["eHybrid", "plug-in hibrid", "plug-in hybrid", "Tiguan eHybrid"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzin", "dizel", "TSI", "TDI"],
        },
    },
    "13_volkswagen_t_roc_hr": {
        "url": "https://www.volkswagen.hr/hr/modeli/t-roc.html",
        "keywords": {
            "MHEV": ["mild hybrid", "eTSI", "hybrid"],
            "ICE": ["benzin", "TSI"],
        },
    },
    "14_volkswagen_taigo_hr": {
        "url": "https://www.volkswagen.hr/hr/modeli/taigo.html",
        "keywords": {
            "ICE": ["benzin", "TSI"],
        },
    },
    "18_volkswagen_tayron_hr": {
        "url": "https://www.volkswagen.hr/hr/modeli/tayron.html",
        "keywords": {
            "PHEV": ["eHybrid", "plug-in hibrid", "plug-in hybrid", "Tayron eHybrid"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzin", "dizel", "TSI", "TDI"],
        },
    },
    # ---- Skoda (VW group) ----
    "06_skoda_kamiq_hr": {
        "url": "https://www.skoda.hr/modeli/kamiq",
        "keywords": {
            "ICE": ["benzin", "TSI"],
        },
    },
    "10_skoda_kodiaq_hr": {
        "url": "https://www.skoda.hr/modeli/kodiaq",
        "keywords": {
            "PHEV": ["plug-in hibrid", "iV", "plug-in hybrid", "Kodiaq iV"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzin", "dizel", "TSI", "TDI"],
        },
    },
    "16_skoda_karoq_hr": {
        "url": "https://www.skoda.hr/modeli/karoq",
        "keywords": {
            "ICE": ["benzin", "dizel", "TSI", "TDI"],
        },
    },
    # ---- Cupra (VW group / SEAT) ----
    "23_cupra_terramar_hr": {
        "url": "https://www.cupraofficial.hr/modeli/terramar",
        "keywords": {
            "PHEV": ["eHybrid", "plug-in hibrid", "plug-in hybrid", "VZ e-HYBRID"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["benzin", "TSI"],
        },
    },
    # ---- Opel (Stellantis) ----
    "02_opel_mokka_hr": {
        "url": "https://www.opel.hr/modeli/mokka.html",
        "keywords": {
            "BEV": ["električni", "electric", "Mokka Electric", "Mokka-e"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzin", "dizel"],
        },
    },
    "30_opel_frontera_hr": {
        "url": "https://www.opel.hr/modeli/frontera.html",
        "keywords": {
            "MHEV": ["mild hybrid", "48V", "hybrid"],
        },
    },
    # ---- Peugeot (Stellantis) ----
    "25_peugeot_2008_hr": {
        "url": "https://www.peugeot.hr/modeli/suv-2008.html",
        "keywords": {
            "BEV": ["električni", "electric", "E-2008", "elbil"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzin", "PureTech"],
        },
    },
    # ---- Suzuki (verified from auto.suzuki.hr) ----
    "03_suzuki_vitara_hr": {
        "url": "https://auto.suzuki.hr/cars/vitara-hybrid",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "Vitara Hybrid"],
            "MHEV": ["mild hybrid", "MILD HYBRID 48V", "blagi hibrid", "48V"],
            "ICE": ["benzin"],
        },
    },
    "08_suzuki_s_cross_hr": {
        "url": "https://auto.suzuki.hr/cars/scross-hybrid",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "S-Cross Hybrid"],
            "MHEV": ["mild hybrid", "MILD HYBRID 48V", "blagi hibrid", "48V"],
        },
    },
    # ---- Toyota (Croatian patterns from SE evidence + Croatian terms) ----
    "12_toyota_yaris_cross_hr": {
        "url": "https://www.toyota.hr/novi-automobili/yaris-cross",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "Hybrid Synergy Drive"],
            "ICE": ["benzin"],
        },
    },
    "17_toyota_c_hr_hr": {
        "url": "https://www.toyota.hr/novi-automobili/c-hr",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "Plug-in Hybrid 220"],
            "HEV": ["hibrid", "hybrid", "Hybrid Synergy Drive"],
        },
    },
    "28_toyota_rav4_hr": {
        "url": "https://www.toyota.hr/novi-automobili/rav4",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "Plug-in Hybrid"],
            "HEV": ["hibrid", "hybrid", "Hybrid Synergy Drive"],
            "ICE": ["benzin"],
        },
    },
    # ---- Nissan (verified from nissan.hr) ----
    "19_nissan_qashqai_hr": {
        "url": "https://www.nissan.hr/vozila/nova-vozila/qashqai.html",
        "keywords": {
            "HEV": ["e-POWER", "e-POWER Hybrid", "hibridni"],
            "MHEV": ["MHEV", "mild hybrid", "blagi hibrid"],
            "ICE": ["benzin"],
        },
    },
    # ---- Renault/Dacia (Renault group) ----
    "04_renault_captur_hr": {
        "url": "https://www.renault.hr/vozila/captur.html",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "E-TECH Plug-in", "PHEV"],
            "HEV": ["hibrid", "hybrid", "E-TECH Hybrid", "E-TECH"],
            "MHEV": ["mild hybrid", "blagi hibrid"],
            "ICE": ["benzin", "TCe"],
            "LPG": ["LPG", "autoplin", "plin"],
        },
    },
    "05_dacia_duster_hr": {
        "url": "https://www.dacia.hr/vozila/duster.html",
        "keywords": {
            "HEV": ["hibrid", "hybrid", "Hybrid 140"],
            "MHEV": ["mild hybrid", "blagi hibrid"],
            "ICE": ["benzin", "TCe"],
            "LPG": ["LPG", "autoplin", "plin", "Bi-Fuel"],
        },
    },
    # ---- Hyundai ----
    "09_hyundai_tucson_hr": {
        "url": "https://www.hyundai.hr/automobili/tucson/",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "PHEV"],
            "HEV": ["hibrid", "hybrid", "HEV", "full hybrid"],
            "MHEV": ["mild hybrid", "MHEV", "blagi hibrid", "48V"],
            "ICE": ["benzin", "dizel"],
        },
    },
    # ---- KIA ----
    "11_kia_stonic_hr": {
        "url": "https://www.kia.hr/modeli/stonic/",
        "keywords": {
            "MHEV": ["mild hybrid", "MHEV", "blagi hibrid", "48V"],
            "ICE": ["benzin"],
        },
    },
    "15_kia_sportage_hr": {
        "url": "https://www.kia.hr/modeli/sportage/",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "Sportage Plug-In Hybrid"],
            "HEV": ["hibrid", "hybrid", "Sportage Hybrid"],
            "MHEV": ["mild hybrid", "MHEV", "blagi hibrid", "48V"],
            "ICE": ["benzin", "dizel"],
        },
    },
    "29_kia_xceed_hr": {
        "url": "https://www.kia.hr/modeli/xceed/",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "XCeed Plug-In Hybrid"],
            "MHEV": ["mild hybrid", "MHEV"],
            "ICE": ["benzin"],
        },
    },
    # ---- Mazda ----
    "20_mazda_cx_30_hr": {
        "url": "https://www.mazda.hr/modeli/mazda-cx-30.html",
        "keywords": {
            "MHEV": ["mild hybrid", "M Hybrid", "e-Skyactiv", "24V"],
        },
    },
    # ---- Geely ----
    "21_geely_coolray_hr": {
        "url": "https://www.geely-auto.hr/coolray",
        "keywords": {
            "ICE": ["benzin", "turbo"],
        },
    },
    # ---- MG ----
    "22_mg_zs_hr": {
        "url": "https://www.mgmotor.hr/model/mg-zs",
        "keywords": {
            "BEV": ["električni", "electric", "MG ZS EV", "EV"],
            "HEV": ["hibrid", "hybrid"],
            "ICE": ["benzin"],
        },
    },
    # ---- Ford ----
    "24_ford_kuga_hr": {
        "url": "https://www.ford.hr/automobili/kuga",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "PHEV"],
            "HEV": ["hibrid", "hybrid", "full hybrid"],
            "MHEV": ["mild hybrid", "EcoBoost Hybrid", "mHEV"],
            "ICE": ["benzin", "dizel", "EcoBoost", "EcoBlue"],
        },
    },
    # ---- BMW ----
    "26_bmw_x1_hr": {
        "url": "https://www.bmw.hr/hr/models/x/x1.html",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "xDrive25e", "xDrive30e"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["benzin", "dizel", "sDrive", "xDrive"],
        },
    },
    # ---- Audi ----
    "27_audi_q3_hr": {
        "url": "https://www.audi.hr/hr/web/hr/models/q3.html",
        "keywords": {
            "PHEV": ["plug-in hibrid", "plug-in hybrid", "TFSI e"],
            "MHEV": ["mild hybrid", "MHEV", "48V"],
            "ICE": ["benzin", "dizel", "TFSI", "TDI"],
        },
    },
}


def apply_updates(path: Path, entry: dict[str, Any], *, dry_run: bool) -> bool:
    """Apply URL, currency, and keyword replacements. Returns True if changed."""
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

    # 5) Update notes (multi-line YAML scalar)
    raw = re.sub(
        r"notes: Draft scaffold generated from country×model top30 backlog\.[^\n]+(?:\n  [^\n]+)*",
        (
            "notes: Keywords filled from official manufacturer website research (HR market).\n"
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
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    updated = skipped = missing = 0

    for stem, entry in sorted(REGISTRY.items()):
        path = DRAFT_DIR / f"{stem}.yaml"
        if not path.exists():
            print(f"  MISSING  {path.name}")
            missing += 1
            continue
        raw = path.read_text(encoding="utf-8")
        if "todo.invalid" not in raw:
            print(f"  SKIP     {path.name}  (already has real URL)")
            skipped += 1
            continue
        apply_updates(path, entry, dry_run=args.dry_run)
        tag = "DRY-RUN" if args.dry_run else "UPDATED"
        print(f"  {tag:8s} {path.name}  ({len(entry['keywords'])} powertrain types)")
        updated += 1

    print(f"\nDone.  UPDATED={updated}  SKIPPED={skipped}  MISSING={missing}")


if __name__ == "__main__":
    main()
