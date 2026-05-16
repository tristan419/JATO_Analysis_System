#!/usr/bin/env python3
"""Add json_script_selector to whitelist-brand sources missing it.

Per the deep research report (2026-05-16) and the April 12 MSRP plan §5.2:
12 brands are verified to supply schema.org JSON-LD (ld+json) on their
configurator pages. This script adds the selector to scrapling-type sources
in batch A countries so the extractor priority chain can use JSON-LD before
falling back to CSS.

Whitelist (12 brands): Toyota, Volvo, Skoda, Kia, Hyundai, Dacia, Ford,
Peugeot, Nissan, Opel, Mercedes, Renault
"""

import os
import sys
from collections import defaultdict

WHITELIST_BRANDS = {
    "TOYOTA", "VOLVO", "SKODA", "KIA", "HYUNDAI", "DACIA", "FORD",
    "PEUGEOT", "NISSAN", "OPEL", "MERCEDES", "RENAULT",
}

BATCH_A_COUNTRIES = ["se", "dk", "fi", "no", "de", "fr", "it", "pl", "cz", "at", "hu", "hr"]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DRAFTS = os.path.join(BASE_DIR, "07_ScrapingToolkit", "source_drafts", "suv_only_country_model_top30")

JSONLD_LINE = "  json_script_selector: script[type='application/ld+json']\n"


def source_has_jsonld(lines: list[str]) -> bool:
    """Check if the file already has json_script_selector configured."""
    for line in lines:
        if "json_script_selector" in line:
            return True
    return False


def add_jsonld_to_file(filepath: str) -> bool:
    """Insert json_script_selector before the css: line if not present."""
    with open(filepath, "r") as fh:
        lines = fh.readlines()

    if source_has_jsonld(lines):
        return False

    # Validate: must be scrapling type
    is_scrapling = any("extractor_type: scrapling" in line for line in lines)
    if not is_scrapling:
        return False

    # Find the css: line (at profile level, i.e. 2-space indent under profile:)
    # Insert json_script_selector right before it
    inserted = False
    new_lines = []
    for line in lines:
        if not inserted and line == "  css:\n":
            new_lines.append(JSONLD_LINE)
            inserted = True
        new_lines.append(line)

    if not inserted:
        print(f"  [WARN] No '  css:' line found — skipping: {filepath}")
        return False

    with open(filepath, "w") as fh:
        fh.writelines(new_lines)

    return True


def main():
    stats = defaultdict(lambda: {"total": 0, "added": 0, "had": 0})

    for cc in BATCH_A_COUNTRIES:
        cc_dir = os.path.join(SOURCE_DRAFTS, cc)
        if not os.path.isdir(cc_dir):
            continue

        for fname in sorted(os.listdir(cc_dir)):
            if not fname.endswith(".yaml") and not fname.endswith(".yml"):
                continue

            fpath = os.path.join(cc_dir, fname)
            with open(fpath, "r") as fh:
                first_lines = "".join(fh.readlines()[:10])

            # Extract brand from first 10 lines
            brand = None
            for line in first_lines.split("\n"):
                if line.startswith("brand:"):
                    brand = line.split(":", 1)[1].strip()
                    break

            if brand not in WHITELIST_BRANDS:
                continue

            stats[brand]["total"] += 1

            if source_has_jsonld(open(fpath).readlines()):
                stats[brand]["had"] += 1
                continue

            if add_jsonld_to_file(fpath):
                stats[brand]["added"] += 1
                print(f"  + {cc}/{fname}")

    # Report
    print(f"\n{'='*60}")
    print(f"{'Brand':<15s} {'Total':>5s} {'Had':>5s} {'Added':>5s} {'Coverage':>10s}")
    print(f"{'-'*60}")
    grand_total = grand_had = grand_added = 0
    for brand in sorted(stats):
        d = stats[brand]
        grand_total += d["total"]
        grand_had += d["had"]
        grand_added += d["added"]
        pct = (d["had"] + d["added"]) / d["total"] * 100 if d["total"] else 0
        print(f"{brand:<15s} {d['total']:>5d} {d['had']:>5d} {d['added']:>5d} {pct:>9.1f}%")
    print(f"{'-'*60}")
    print(f"{'TOTAL':<15s} {grand_total:>5d} {grand_had:>5d} {grand_added:>5d}")
    print(f"\nAdded json_script_selector to {grand_added} source files.")
    print(f"Whitelist brand coverage now: {grand_had + grand_added}/{grand_total}")


if __name__ == "__main__":
    main()
