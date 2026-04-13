#!/usr/bin/env python3
"""
Batch-fill powertrain keywords, source URLs, and currency for all 26
Swedish (se) draft YAML files that still contain todo.invalid placeholders.

Keyword evidence sourced from official manufacturer websites:
  - Toyota SE:  toyota.se/new-cars/{model}
  - VW SE:      volkswagen.se/sv/modeller/{model}.html
  - KIA SE:     kia.com/se/nya-bilar/{model}/ (model overview page)
  - Volvo SE:   volvocars.com/se/build/{model}/ (XC60 production pattern)
  - Skoda SE:   VW-group Swedish conventions
  - Peugeot SE: Stellantis Swedish conventions
  - Cupra SE:   VW-group Swedish conventions (SEAT/Cupra brand)
  - Tesla SE:   tesla.com/sv_se/model*
  - Polestar SE: polestar.com/se/polestar-4/
  - BMW/Mercedes/Audi SE: standard German-brand Swedish patterns

Run:
    python 03_Scripts/batch_fill_se_keywords.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DRAFT_DIR = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30" / "se"

# --------------- keyword / URL registry ---------------
# Each entry: filename_stem -> {url, currency, keywords: {powertrain_type: [kw, ...]}}
# keywords dict keys must match the powertrain types already present in
# the file's powertrain_rules (BEV, PHEV, HEV, MHEV, ICE).

REGISTRY: dict[str, dict[str, Any]] = {
    # ---- BEV-only models ----
    "03_tesla_model_y_se": {
        "url": "https://www.tesla.com/sv_se/modely",
        "keywords": {
            "BEV": ["electric", "model y", "long range", "performance", "elbil"],
        },
    },
    "04_kia_ev3_se": {
        "url": "https://www.kia.com/se/nya-bilar/ev3/premiere",
        "keywords": {
            "BEV": ["ev3", "electric", "elbil", "el-suv", "helelektrisk"],
        },
    },
    "08_polestar_4_se": {
        "url": "https://www.polestar.com/se/polestar-4/",
        "keywords": {
            "BEV": ["polestar 4", "electric", "elbil", "single motor", "twin motor"],
        },
    },
    "10_volkswagen_id_4_se": {
        "url": "https://www.volkswagen.se/sv/modeller/id4.html",
        "keywords": {
            "BEV": ["id.4", "id4", "electric", "elbil", "helelektrisk"],
        },
    },
    "14_skoda_enyaq_se": {
        "url": "https://www.skoda.se/modeller/enyaq",
        "keywords": {
            "BEV": ["enyaq", "electric", "elbil", "helelektrisk"],
        },
    },
    "19_volvo_ec40_se": {
        "url": "https://www.volvocars.com/se/build/ec40-electric/",
        "keywords": {
            "BEV": ["ec40-electric", "ec40", "electric", "single motor", "twin motor"],
        },
    },
    "21_kia_ev9_se": {
        "url": "https://www.kia.com/se/nya-bilar/ev9/",
        "keywords": {
            "BEV": ["ev9", "electric", "elbil", "helelektrisk"],
        },
    },
    "25_kia_ev6_se": {
        "url": "https://www.kia.com/se/nya-bilar/ev6/upptack/",
        "keywords": {
            "BEV": ["ev6", "electric", "elbil", "helelektrisk"],
        },
    },
    "27_bmw_ix1_se": {
        "url": "https://www.bmw.se/sv/alla-modeller/ix1.html",
        "keywords": {
            "BEV": ["ix1", "electric", "elbil", "elektrisk"],
        },
    },
    "28_mercedes_eqa_se": {
        "url": "https://www.mercedes-benz.se/passengercars/models/suv/eqa/overview.html",
        "keywords": {
            "BEV": ["eqa", "electric", "elbil", "elektrisk"],
        },
    },
    "29_audi_q4_e_tron_se": {
        "url": "https://www.audi.se/se/web/sv/models/q4-e-tron.html",
        "keywords": {
            "BEV": ["q4 e-tron", "e-tron", "electric", "elbil", "elektrisk"],
        },
    },
    "30_volvo_ex90_se": {
        "url": "https://www.volvocars.com/se/build/ex90-electric/",
        "keywords": {
            "BEV": ["ex90-electric", "ex90", "electric", "single motor", "twin motor"],
        },
    },
    # ---- Toyota models (verified from toyota.se) ----
    "07_toyota_rav4_se": {
        "url": "https://www.toyota.se/new-cars/rav4",
        "keywords": {
            "PHEV": ["laddhybrid", "plug-in hybrid", "Laddhybrid Synergy Drive"],
            "HEV": ["hybrid", "Hybrid Synergy Drive", "2,5 VVT-i Hybrid"],
        },
    },
    "11_toyota_yaris_cross_se": {
        "url": "https://www.toyota.se/new-cars/yaris-cross",
        "keywords": {
            "HEV": ["hybrid", "TNGA HEV", "Hybrid Synergy Drive"],
        },
    },
    "16_toyota_corolla_cross_se": {
        "url": "https://www.toyota.se/new-cars/corolla-cross",
        "keywords": {
            "HEV": ["hybrid", "Hybrid Synergy Drive", "Hybrid Synergi Drive"],
        },
    },
    "18_toyota_c_hr_se": {
        "url": "https://www.toyota.se/new-cars/c-hr",
        "keywords": {
            "PHEV": ["laddhybrid", "plug-in hybrid", "Laddhybrid Synergy Drive", "Plug-in Hybrid 220"],
            "HEV": ["hybrid", "Hybrid Synergy Drive"],
        },
    },
    # ---- KIA models (verified from kia.com/se) ----
    "13_kia_sportage_se": {
        "url": "https://www.kia.com/se/nya-bilar/sportage/upptack/",
        "keywords": {
            "PHEV": ["plug-in hybrid", "laddhybrid", "laddbar", "Sportage Plug-In Hybrid"],
            "HEV": ["hybrid", "Sportage Hybrid"],
            "ICE": ["bensin", "diesel", "1.6 T-GDi"],
        },
    },
    # ---- VW models (verified from volkswagen.se) ----
    "09_volkswagen_tiguan_se": {
        "url": "https://www.volkswagen.se/sv/modeller/tiguan.html",
        "keywords": {
            "PHEV": ["eHybrid", "laddhybrid", "Tiguan eHybrid", "laddbar"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["bensin", "diesel", "TSI", "TDI", "bensinare"],
        },
    },
    "12_volkswagen_t_roc_se": {
        "url": "https://www.volkswagen.se/sv/modeller/t-roc.html",
        "keywords": {
            "MHEV": ["hybrid", "hybridsuven", "eTSI", "mild hybrid"],
            "ICE": ["bensin", "TSI"],
        },
    },
    "23_volkswagen_tayron_se": {
        "url": "https://www.volkswagen.se/sv/modeller/tayron.html",
        "keywords": {
            "PHEV": ["eHybrid", "laddhybrid", "Tayron eHybrid", "laddbar"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["bensin", "TSI", "diesel", "TDI"],
        },
    },
    # ---- Skoda (VW-group SE conventions) ----
    "06_skoda_kodiaq_se": {
        "url": "https://www.skoda.se/modeller/kodiaq",
        "keywords": {
            "PHEV": ["laddhybrid", "iV", "plug-in hybrid", "Kodiaq iV"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["bensin", "diesel", "TSI", "TDI"],
        },
    },
    # ---- Peugeot (Stellantis SE conventions) ----
    "15_peugeot_3008_se": {
        "url": "https://www.peugeot.se/modeller/suv-3008.html",
        "keywords": {
            "BEV": ["electric", "elbil", "E-3008", "elektrisk"],
            "PHEV": ["laddhybrid", "plug-in hybrid", "HYBRID"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["bensin", "PureTech"],
        },
    },
    "17_peugeot_2008_se": {
        "url": "https://www.peugeot.se/modeller/suv-2008.html",
        "keywords": {
            "BEV": ["electric", "elbil", "E-2008", "elektrisk"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["bensin", "PureTech"],
        },
    },
    "22_peugeot_5008_se": {
        "url": "https://www.peugeot.se/modeller/suv-5008.html",
        "keywords": {
            "BEV": ["electric", "elbil", "E-5008", "elektrisk"],
            "PHEV": ["laddhybrid", "plug-in hybrid", "HYBRID"],
            "MHEV": ["mild hybrid", "48V"],
            "ICE": ["bensin", "PureTech"],
        },
    },
    # ---- Cupra (VW-group / SEAT SE conventions) ----
    "20_cupra_terramar_se": {
        "url": "https://www.cupraofficial.se/modeller/terramar",
        "keywords": {
            "PHEV": ["eHybrid", "laddhybrid", "plug-in hybrid", "VZ e-HYBRID"],
            "MHEV": ["mild hybrid", "eTSI"],
            "ICE": ["bensin", "TSI"],
        },
    },
    # ---- Volvo (XC40 multi-powertrain, from XC60 production pattern) ----
    "24_volvo_xc40_se": {
        "url": "https://www.volvocars.com/se/build/xc40/",
        "keywords": {
            "BEV": ["xc40-electric", "electric", "single motor", "twin motor"],
            "PHEV": ["laddhybrid", "plug-in hybrid", "recharge", "t4", "t5"],
            "MHEV": ["mild hybrid", "mhev", "b3", "b4", "b5"],
            "ICE": ["bensin", "diesel", "b3", "b4"],
        },
    },
}

CURRENCY = "SEK"


def load_yaml(path: Path) -> tuple[str, Any]:
    """Return (raw_text, parsed_data)."""
    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)
    return raw, data


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
            # Build YAML list lines (indented to match existing)
            kw_lines = "\n".join(f"      - {kw}" for kw in keywords)
            # Replace the single placeholder line with actual keyword list
            raw = re.sub(
                rf"(\s+keywords:\n)\s+- {re.escape(placeholder)}",
                rf"\1{kw_lines}",
                raw,
            )

    # 5) Update notes (multi-line YAML scalar: match first line + continuation)
    raw = re.sub(
        r"notes: Draft scaffold generated from country×model top30 backlog\.[^\n]+(?:\n  [^\n]+)*",
        (
            "notes: Keywords filled from official manufacturer website research (SE market).\n"
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
                        help="Show what would change without writing files")
    args = parser.parse_args()

    updated = 0
    skipped = 0
    missing = 0

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
        changed = apply_updates(path, entry, dry_run=args.dry_run)
        tag = "DRY-RUN" if args.dry_run else "UPDATED"
        print(f"  {tag:8s} {path.name}  ({len(entry['keywords'])} powertrain types)")
        updated += 1

    print(f"\nDone.  UPDATED={updated}  SKIPPED={skipped}  MISSING={missing}")


if __name__ == "__main__":
    main()
