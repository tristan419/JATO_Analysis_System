#!/usr/bin/env python3
"""Batch-fill CSS selectors for draft YAMLs based on brand selector profiles.

Usage:
    python batch_fill_brand_css.py [--dry-run] [--brand BRAND] [--country CC]

Reads brand CSS profiles and applies them to matching draft YAMLs
in source_drafts/suv_only_country_model_top30/.
"""

from __future__ import annotations

import os
import re
import sys
import yaml

DRAFT_BASE = os.path.join(
    os.path.dirname(__file__),
    "..",
    "07_ScrapingToolkit",
    "source_drafts",
    "suv_only_country_model_top30",
)

BATCH_1_2_COUNTRIES = ["se", "hr", "hu", "no", "at", "cz", "ch"]

# ──────────────────────────────────────────────────────────
# Brand CSS selector profiles
# Each profile defines the CSS selectors to fill into drafts.
# ──────────────────────────────────────────────────────────

BRAND_CSS_PROFILES: dict[str, dict] = {
    "VOLVO": {
        "strategy": "css",
        "css": {
            "vehicle_container": '[data-testid="selection-card"]',
            "model": "",
            "trim": '[data-testid="title"]::text',
            "price": '[data-testid="price"]::text',
            "exclude_if_selector": 'small[data-sources*="valueDescription"]',
        },
        "exclude_price_prefixes": {
            "se": ["Från"],
            "no": ["Fra"],
            "hu": ["tól", "Tól"],
            "cz": ["od", "Od"],
            "ch": ["Ab", "ab"],
            "hr": ["Od"],
            "at": ["Ab", "ab"],
            "_default": ["From"],
        },
        "price_label": {
            "se": "Volvo build-sida pris",
            "no": "Volvo byggeside pris",
            "hu": "Volvo konfigurátor ár",
            "cz": "Volvo konfigurátor cena",
            "ch": "Volvo Konfigurator Preis",
            "hr": "Volvo konfigurator cijena",
            "at": "Volvo Konfigurator Preis",
            "_default": "Volvo build price",
        },
    },
    # BMW per-model pages use the same attr_json as the all-models page
    "BMW": {
        "strategy": "attr_json",
        "attr_json": {
            "vehicle_container": ".cmp-allmodelscard",
            "filter_attr": "data-card-filter-info",
            "tracking_attr": "data-tracking-attributes",
            "price_key": "price",
            "fuel_key": "fuelType",
            "category_key": "category",
            "series_key": "series",
            "name_key": "name",
            "range_key": "range",
        },
        "css": {
            "vehicle_container": ".cmp-allmodelscard",
            "model": ".cmp-allmodelscarddetail__series::text",
            "trim": "",
            "price": "",
        },
        "price_label": {
            "se": "Rekommenderat pris inkl. moms",
            "no": "Veiledende pris inkl. mva.",
            "hu": "Listaár ÁFA-val",
            "cz": "Doporučená cena vč. DPH",
            "ch": "UVP inkl. MwSt.",
            "hr": "Preporučena cijena s PDV-om",
            "at": "UVP inkl. MwSt.",
            "_default": "UVP inkl. MwSt.",
        },
    },
}


def _get_locale_value(mapping: dict, country: str) -> str | list:
    """Get locale-specific value or default."""
    return mapping.get(country, mapping.get("_default", ""))


def apply_profile(filepath: str, profile: dict, country: str, dry_run: bool = False) -> bool:
    """Apply a brand CSS profile to a draft YAML file.
    
    Returns True if the file was modified.
    """
    with open(filepath) as f:
        content = f.read()

    if "TODO_SELECTOR" not in content:
        return False

    data = yaml.safe_load(content)
    if data is None:
        return False

    p = data.get("profile", {})
    css_section = p.get("css", {})
    modified = False

    strategy = profile.get("strategy", "css")

    # Apply CSS selectors
    if "css" in profile:
        css_profile = profile["css"]
        for key in ("vehicle_container", "model", "trim", "price"):
            if css_section.get(key) == "TODO_SELECTOR":
                new_val = css_profile.get(key, "")
                # In original content, replace the TODO_SELECTOR for this key
                old_pattern = f"    {key}: TODO_SELECTOR"
                if key == "model" and new_val == "":
                    new_line = f"    {key}: ''"
                elif new_val:
                    new_line = f"    {key}: '{new_val}'"
                else:
                    new_line = f"    {key}: ''"
                content = content.replace(old_pattern, new_line, 1)
                modified = True

        # Add exclude_if_selector
        exclude_sel = css_profile.get("exclude_if_selector", "")
        if exclude_sel:
            content = content.replace(
                "  exclude_price_prefixes:",
                f"  exclude_if_selector: '{exclude_sel}'\n  exclude_price_prefixes:",
                1,
            )

    # Apply attr_json strategy
    if strategy == "attr_json" and "attr_json" in profile:
        aj = profile["attr_json"]
        # Insert attr_json section before css section
        attr_json_yaml = "  attr_json:\n"
        for key, val in aj.items():
            attr_json_yaml += f"    {key}: \"{val}\"\n" if isinstance(val, str) else f"    {key}: {val}\n"
        content = content.replace(
            "  css:\n",
            attr_json_yaml + "  css:\n",
            1,
        )
        modified = True

    # Apply exclude_price_prefixes
    if "exclude_price_prefixes" in profile:
        prefixes = _get_locale_value(profile["exclude_price_prefixes"], country)
        if isinstance(prefixes, str):
            prefixes = [prefixes] if prefixes else []
        if prefixes:
            # Replace empty exclude_price_prefixes
            old_exc = "  exclude_price_prefixes:\n"
            # Check if it already has entries or is empty list
            if "  exclude_price_prefixes: []\n" in content:
                new_exc = "  exclude_price_prefixes:\n"
                for px in prefixes:
                    new_exc += f"  - {px}\n"
                content = content.replace("  exclude_price_prefixes: []\n", new_exc, 1)
                modified = True
            elif re.search(r"  exclude_price_prefixes:\n  - ", content):
                pass  # Already has entries
            elif "  exclude_price_prefixes:\n" in content:
                # Empty list with no entries - check what follows
                pass  # Keep existing

    # Apply price_label
    if "price_label" in profile:
        label = _get_locale_value(profile["price_label"], country)
        if label and "TODO verify local MSRP label" in content:
            content = content.replace("TODO verify local MSRP label", label, 1)
            modified = True

    if modified and not dry_run:
        with open(filepath, "w") as f:
            f.write(content)

    return modified


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Don't write files")
    parser.add_argument("--brand", default="", help="Only process this brand")
    parser.add_argument("--country", default="", help="Only process this country")
    args = parser.parse_args()

    countries = [args.country] if args.country else BATCH_1_2_COUNTRIES
    brands = [args.brand.upper()] if args.brand else list(BRAND_CSS_PROFILES.keys())

    total_modified = 0
    for cc in countries:
        cc_dir = os.path.join(DRAFT_BASE, cc)
        if not os.path.isdir(cc_dir):
            continue
        for fn in sorted(os.listdir(cc_dir)):
            if not fn.endswith(".yaml"):
                continue
            fpath = os.path.join(cc_dir, fn)
            with open(fpath) as f:
                data = yaml.safe_load(f)
            brand = data.get("brand", "")
            if brand not in brands:
                continue
            profile = BRAND_CSS_PROFILES.get(brand)
            if not profile:
                continue
            if apply_profile(fpath, profile, cc, dry_run=args.dry_run):
                tag = "[DRY-RUN] " if args.dry_run else ""
                print(f"{tag}UPDATED  {cc}/{fn}")
                total_modified += 1
            else:
                print(f"SKIP     {cc}/{fn}  (no TODO_SELECTOR)")

    print(f"\n{'[DRY-RUN] ' if args.dry_run else ''}Total modified: {total_modified}")


if __name__ == "__main__":
    main()
