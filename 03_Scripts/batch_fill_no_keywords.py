#!/usr/bin/env python3
"""
Batch-fill powertrain keywords, source URLs, and currency for all 30
Norwegian (no) draft YAML files that still contain todo.invalid placeholders.

Norway is overwhelmingly BEV-dominant (28 of 30 top models are pure electric).

Norwegian keyword evidence:
  - Toyota NO (toyota.no/nybil/bz4x): "elbil", "elektrisk", "helelektrisk",
    prices in NOK (kr): bZ4X from 457,100 kr to 628,675 kr
  - General Norwegian automotive terminology:
    BEV  = "elbil", "elektrisk", "electric", "helelektrisk"
    PHEV = "ladbar hybrid", "plug-in hybrid", "laddbar"
    HEV  = "hybrid", "selvladende hybrid"
    MHEV = "mild hybrid", "48V"
    ICE  = "bensin", "diesel"
  - Currency: NOK (kr)

Run:
    python 03_Scripts/batch_fill_no_keywords.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DRAFT_DIR = ROOT / "07_ScrapingToolkit" / "source_drafts" / "suv_only_country_model_top30" / "no"

CURRENCY = "NOK"

# --------------- keyword / URL registry ---------------
REGISTRY: dict[str, dict[str, Any]] = {
    # ---- BEV-only models (dominant in NO market) ----
    "01_tesla_model_y_no": {
        "url": "https://www.tesla.com/no_NO/modely",
        "keywords": {
            "BEV": ["elbil", "electric", "Model Y", "long range", "performance", "elektrisk"],
        },
    },
    "02_volkswagen_id_4_no": {
        "url": "https://www.volkswagen.no/no/modeller/id4.html",
        "keywords": {
            "BEV": ["elbil", "ID.4", "electric", "helelektrisk", "elektrisk"],
        },
    },
    "03_toyota_bz4x_no": {
        "url": "https://www.toyota.no/nybil/bz4x",
        "keywords": {
            "BEV": ["elbil", "bZ4X", "electric", "helelektrisk", "elektrisk"],
        },
    },
    "04_volvo_ex40_no": {
        "url": "https://www.volvocars.com/no/build/ex40-electric/",
        "keywords": {
            "BEV": ["elbil", "electric", "single motor", "twin motor", "EX40"],
        },
    },
    "05_volvo_ex30_no": {
        "url": "https://www.volvocars.com/no/build/ex30-electric/",
        "keywords": {
            "BEV": ["elbil", "electric", "single motor", "twin motor", "EX30"],
        },
    },
    "06_skoda_enyaq_no": {
        "url": "https://www.skoda.no/modeller/enyaq",
        "keywords": {
            "BEV": ["elbil", "electric", "Enyaq", "helelektrisk"],
        },
    },
    "07_byd_sealion_7_no": {
        "url": "https://www.byd.com/no/car/seal-ion-7",
        "keywords": {
            "BEV": ["elbil", "electric", "Sealion 7", "elektrisk"],
        },
    },
    "08_nissan_ariya_no": {
        "url": "https://www.nissan.no/biler/nye-biler/ariya.html",
        "keywords": {
            "BEV": ["elbil", "electric", "Ariya", "helelektrisk"],
        },
    },
    "09_ford_explorer_ev_no": {
        "url": "https://www.ford.no/elektrisk/explorer",
        "keywords": {
            "BEV": ["elbil", "electric", "Explorer", "helelektrisk"],
        },
    },
    "10_skoda_elroq_no": {
        "url": "https://www.skoda.no/modeller/elroq",
        "keywords": {
            "BEV": ["elbil", "electric", "Elroq", "helelektrisk"],
        },
    },
    "11_bmw_ix1_no": {
        "url": "https://www.bmw.no/no/alle-modeller/ix1.html",
        "keywords": {
            "BEV": ["elbil", "electric", "iX1", "helelektrisk"],
        },
    },
    "12_xpeng_g6_no": {
        "url": "https://www.xpeng.com/no/g6",
        "keywords": {
            "BEV": ["elbil", "electric", "G6", "helelektrisk"],
        },
    },
    "13_audi_q6_e_tron_no": {
        "url": "https://www.audi.no/no/web/no/modeller/q6-e-tron.html",
        "keywords": {
            "BEV": ["elbil", "electric", "Q6 e-tron", "e-tron", "helelektrisk"],
        },
    },
    "14_audi_q4_e_tron_no": {
        "url": "https://www.audi.no/no/web/no/modeller/q4-e-tron.html",
        "keywords": {
            "BEV": ["elbil", "electric", "Q4 e-tron", "e-tron", "helelektrisk"],
        },
    },
    "15_kia_ev3_no": {
        "url": "https://www.kia.com/no/modeller/ev3/",
        "keywords": {
            "BEV": ["elbil", "electric", "EV3", "helelektrisk"],
        },
    },
    "16_hyundai_ioniq_5_no": {
        "url": "https://www.hyundai.com/no/no/modeller/ioniq-5.html",
        "keywords": {
            "BEV": ["elbil", "electric", "IONIQ 5", "helelektrisk"],
        },
    },
    "17_polestar_4_no": {
        "url": "https://www.polestar.com/no/polestar-4/",
        "keywords": {
            "BEV": ["elbil", "electric", "Polestar 4", "helelektrisk"],
        },
    },
    "18_hyundai_kona_no": {
        "url": "https://www.hyundai.com/no/no/modeller/kona-electric.html",
        "keywords": {
            "BEV": ["elbil", "electric", "KONA Electric", "helelektrisk"],
        },
    },
    "19_volvo_ex90_no": {
        "url": "https://www.volvocars.com/no/build/ex90-electric/",
        "keywords": {
            "BEV": ["elbil", "electric", "EX90", "single motor", "twin motor"],
        },
    },
    "20_mercedes_eqb_no": {
        "url": "https://www.mercedes-benz.no/passengercars/models/suv/eqb/overview.html",
        "keywords": {
            "BEV": ["elbil", "electric", "EQB", "helelektrisk"],
        },
    },
    "21_mercedes_eqa_no": {
        "url": "https://www.mercedes-benz.no/passengercars/models/suv/eqa/overview.html",
        "keywords": {
            "BEV": ["elbil", "electric", "EQA", "helelektrisk"],
        },
    },
    "22_bmw_ix_no": {
        "url": "https://www.bmw.no/no/alle-modeller/ix.html",
        "keywords": {
            "BEV": ["elbil", "electric", "iX", "helelektrisk"],
        },
    },
    "23_mg_s5_no": {
        "url": "https://www.mgmotor.no/modeller/mg-s5",
        "keywords": {
            "BEV": ["elbil", "electric", "S5", "helelektrisk"],
        },
    },
    "24_ford_capri_no": {
        "url": "https://www.ford.no/elektrisk/capri",
        "keywords": {
            "BEV": ["elbil", "electric", "Capri", "helelektrisk"],
        },
    },
    "25_xpeng_g9_no": {
        "url": "https://www.xpeng.com/no/g9",
        "keywords": {
            "BEV": ["elbil", "electric", "G9", "helelektrisk"],
        },
    },
    # ---- Toyota Yaris Cross (one of very few non-BEV in NO top 30) ----
    "26_toyota_yaris_cross_no": {
        "url": "https://www.toyota.no/nybil/yaris-cross",
        "keywords": {
            "HEV": ["hybrid", "selvladende hybrid", "Hybrid Synergy Drive"],
        },
    },
    # ---- Porsche Macan (new gen is all-electric) ----
    "27_porsche_macan_no": {
        "url": "https://www.porsche.com/norway/models/macan/",
        "keywords": {
            "BEV": ["elbil", "electric", "Macan Electric", "helelektrisk"],
        },
    },
    # ---- Peugeot 5008 (BEV/PHEV/ICE variants) ----
    "28_peugeot_5008_no": {
        "url": "https://www.peugeot.no/modeller/suv-5008.html",
        "keywords": {
            "BEV": ["elbil", "electric", "E-5008", "helelektrisk"],
            "PHEV": ["ladbar hybrid", "plug-in hybrid", "HYBRID"],
        },
    },
    "29_bmw_ix2_no": {
        "url": "https://www.bmw.no/no/alle-modeller/ix2.html",
        "keywords": {
            "BEV": ["elbil", "electric", "iX2", "helelektrisk"],
        },
    },
    "30_polestar_3_no": {
        "url": "https://www.polestar.com/no/polestar-3/",
        "keywords": {
            "BEV": ["elbil", "electric", "Polestar 3", "helelektrisk"],
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
            "notes: Keywords filled from official manufacturer website research (NO market).\n"
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
