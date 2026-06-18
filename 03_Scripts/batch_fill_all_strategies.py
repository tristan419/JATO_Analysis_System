#!/usr/bin/env python3
"""Batch-fill extraction configs for all Batch 1+2 draft YAMLs.

For each file:
  1. Add json_script_selector (universal ld+json fallback)
  2. Fill brand-specific CSS / attr_json selectors if known
  3. Replace TODO_SELECTOR with '' (empty) in css section
"""

import os
import re
import sys
import yaml
import copy
from pathlib import Path

BATCH_COUNTRIES = ["se", "hr", "hu", "no", "at", "cz", "ch"]
DRAFTS_ROOT = Path(__file__).resolve().parent.parent / ("07_ScrapingToolkit/source_drafts/suv_only_country_model_top30")

# ── Price label mappings per locale ─────────────────────────────────
PRICE_LABELS = {
    "se": "Rekommenderat cirkapris inkl. moms",
    "no": "Veiledende pris inkl. mva",
    "hr": "Preporučena maloprodajna cijena s PDV-om",
    "hu": "Ajánlott fogyasztói ár (ÁFA-val)",
    "at": "Unverbindliche Preisempfehlung inkl. MwSt",
    "cz": "Doporučená cena včetně DPH",
    "ch": "Unverbindliche Preisempfehlung inkl. MwSt",
}

# ── Brand-specific CSS profiles ────────────────────────────────────
# Each profile overrides the css: section in the YAML.
# If a brand uses json_script_selector as primary, css can remain empty
# (it's a fallback that silently returns nothing).

BRAND_CSS_PROFILES = {
    "VOLVO": {
        "strategy": "css",
        "css": {
            "vehicle_container": '[data-testid="selection-card"]',
            "model": "",
            "trim": '[data-testid="title"]',
            "price": '[data-testid="price"]',
        },
        "exclude_price_prefixes_by_locale": {
            "se": ["Från ", "Leasing"],
            "no": ["Fra ", "Leasing"],
            "hr": ["Od ", "Leasing"],
            "hu": ["Tól ", "Lízing"],
            "at": ["Ab ", "Leasing"],
            "cz": ["Od ", "Leasing"],
            "ch": ["Ab ", "Leasing"],
        },
        "price_labels_by_locale": {
            "se": "Rekommenderat cirkapris",
            "no": "Veiledende pris",
            "hr": "Preporučena cijena",
            "hu": "Ajánlott ár",
            "at": "Empfohlener Richtpreis",
            "cz": "Doporučená cena",
            "ch": "Empfohlener Richtpreis",
        },
    },
    "KIA": {
        "strategy": "css",
        "css": {
            "vehicle_container": "li.category_item",
            "model": "",
            "trim": "strong",
            "price": ".price",
        },
    },
    "NISSAN": {
        "strategy": "css",
        "css": {
            "vehicle_container": ".title-payment-info-container",
            "model": "",
            "trim": "",
            "price": ".msrp-price",
        },
    },
    # Toyota and Skoda rely on json_script (ld+json) — no CSS needed
    "TOYOTA": {"strategy": "json_script"},
    "SKODA": {"strategy": "json_script"},
}


def update_one(filepath: Path, dry_run: bool = True) -> dict:
    """Update a single draft YAML file. Returns status dict."""
    with open(filepath) as f:
        raw = f.read()

    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        return {"file": str(filepath), "status": "skip", "reason": "not a dict"}

    brand = data.get("brand", "")
    country_code = filepath.parent.name
    profile = data.get("profile", {})
    if not profile:
        return {"file": str(filepath), "status": "skip", "reason": "no profile"}

    changes = []

    # ── 1. Add json_script_selector if missing ──────────────────────
    if "json_script_selector" not in profile:
        profile["json_script_selector"] = "script[type='application/ld+json']"
        changes.append("added json_script_selector")

    # ── 2. Apply brand-specific CSS profile ─────────────────────────
    brand_profile = BRAND_CSS_PROFILES.get(brand)
    css_section = profile.get("css", {})
    if not isinstance(css_section, dict):
        css_section = {}

    if brand_profile and brand_profile.get("strategy") == "css":
        bp_css = brand_profile["css"]
        for key in ["vehicle_container", "model", "trim", "price"]:
            if key in bp_css:
                old = css_section.get(key, "TODO_SELECTOR")
                new = bp_css[key]
                if old != new:
                    css_section[key] = new
                    changes.append(f"css.{key}={new!r}")
        if "exclude_if_selector" in bp_css:
            css_section["exclude_if_selector"] = bp_css["exclude_if_selector"]
            changes.append("css.exclude_if_selector set")
        # Locale-specific exclude_price_prefixes
        prefixes = (brand_profile.get("exclude_price_prefixes_by_locale") or {}).get(country_code)
        if prefixes:
            profile["exclude_price_prefixes"] = prefixes
            changes.append(f"exclude_price_prefixes={prefixes}")
        # Locale-specific price labels
        plabel = (brand_profile.get("price_labels_by_locale") or {}).get(country_code)
        if plabel:
            profile["default_price_label"] = plabel
            changes.append(f"price_label={plabel}")
        profile["css"] = css_section
    else:
        # No CSS strategy — remove the css section entirely so
        # the extractor skips straight from json_script to "no strategy".
        has_todo = any(css_section.get(k) == "TODO_SELECTOR" for k in ["vehicle_container", "model", "trim", "price"])
        if has_todo:
            profile.pop("css", None)
            changes.append("css section removed (no css strategy)")
        elif not any(css_section.get(k) for k in ["vehicle_container"]):
            # vehicle_container is empty — also remove to avoid crash
            profile.pop("css", None)
            changes.append("css section removed (empty container)")

    # ── 4. Fix price label if still placeholder ─────────────────────
    if "TODO" in str(profile.get("default_price_label", "")):
        label = PRICE_LABELS.get(country_code, profile.get("default_price_label", ""))
        profile["default_price_label"] = label
        changes.append(f"price_label fixed")

    data["profile"] = profile

    if not changes:
        return {"file": str(filepath), "status": "unchanged", "changes": []}

    if not dry_run:
        # Preserve comments header by rewriting carefully
        # Find where YAML data starts (after comment block)
        lines = raw.split("\n")
        comment_lines = []
        for line in lines:
            if line.startswith("#"):
                comment_lines.append(line)
            else:
                break
        header = "\n".join(comment_lines) + "\n" if comment_lines else ""
        yaml_out = yaml.dump(
            data,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )
        with open(filepath, "w") as f:
            f.write(header + yaml_out)

    return {"file": str(filepath), "status": "updated", "changes": changes}


def main():
    dry_run = "--apply" not in sys.argv
    if dry_run:
        print("=== DRY RUN (use --apply to write) ===\n")

    stats = {"updated": 0, "unchanged": 0, "skip": 0, "errors": 0}
    by_brand = {}

    for country in BATCH_COUNTRIES:
        cdir = DRAFTS_ROOT / country
        if not cdir.is_dir():
            print(f"  ⚠ {country}/ not found")
            continue
        for fn in sorted(cdir.iterdir()):
            if fn.suffix != ".yaml":
                continue
            try:
                result = update_one(fn, dry_run=dry_run)
            except Exception as e:
                result = {"file": str(fn), "status": "error", "reason": str(e)}
                stats["errors"] += 1
                continue

            status = result["status"]
            stats[status] = stats.get(status, 0) + 1

            # Track by brand
            with open(fn) as f:
                data = yaml.safe_load(f)
            brand = data.get("brand", "?") if isinstance(data, dict) else "?"
            by_brand.setdefault(brand, []).append(result)

            if status == "updated":
                changes_str = ", ".join(result.get("changes", []))
                print(f"  ✅ {fn.name}: {changes_str}")
            elif status == "skip":
                print(f"  ⚠ {fn.name}: {result.get('reason')}")

    print(f"\n{'─'*60}")
    print(
        f"Summary: {stats['updated']} updated, {stats['unchanged']} unchanged, "
        f"{stats['skip']} skipped, {stats.get('errors', 0)} errors"
    )
    print(f"{'─'*60}")

    # Brand summary
    print(f"\n{'Brand':15s} {'Files':>5s} {'Updated':>7s} {'Strategy'}")
    print(f"{'─'*55}")
    for brand in sorted(by_brand.keys()):
        results = by_brand[brand]
        updated = sum(1 for r in results if r["status"] == "updated")
        strat = (
            "css"
            if brand in BRAND_CSS_PROFILES and BRAND_CSS_PROFILES[brand].get("strategy") == "css"
            else "json_script"
        )
        print(f"{brand:15s} {len(results):5d} {updated:7d}  {strat}")

    if dry_run:
        print(f"\n** Dry run complete. Use --apply to write changes. **")


if __name__ == "__main__":
    main()
