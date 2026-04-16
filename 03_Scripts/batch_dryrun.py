#!/usr/bin/env python3
"""Batch dry-run all draft sources for specified countries.

Runs each source individually to isolate failures, and produces
a summary report.
"""
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Callable

# Ensure jato_scraper is importable
_toolkit_dir = str(
    Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
)
if _toolkit_dir not in sys.path:
    sys.path.insert(0, _toolkit_dir)

_TOOLKIT_ROOT = Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
_DRAFTS_DIR = _TOOLKIT_ROOT / "source_drafts" / "suv_only_country_model_top30"
STRICT_EXIT = os.getenv("JATO_STRICT_EXIT", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

BATCH_COUNTRIES = {
    "1": ["se", "hr"],
    "2": ["hu", "no", "at", "cz", "ch"],
    "all": ["se", "hr", "hu", "no", "at", "cz", "ch"],
}

log = logging.getLogger(__name__)


def _resolve_scraper_functions() -> tuple[Callable, Callable]:
    from jato_scraper.config_loader import load_all_sources
    from jato_scraper.runner import run_scrape

    return load_all_sources, run_scrape


def _promoted_code_for_draft(code: str) -> str:
    if not code.endswith("_draft_scrapling"):
        return code
    return code.replace("_draft_scrapling", "_scrapling")


def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    batch = sys.argv[1] if len(sys.argv) > 1 else "all"
    countries = BATCH_COUNTRIES.get(batch, batch.split(","))
    load_all_sources, run_scrape = _resolve_scraper_functions()

    # Load both promoted sources and draft sources
    promoted_codes = set(load_all_sources())
    draft_codes = load_all_sources(sources_dir=_DRAFTS_DIR)
    draft_codes = [c for c in draft_codes if c.endswith("_draft_scrapling")]
    target_codes = []
    skipped_promoted = []
    for code in draft_codes:
        promoted_code = _promoted_code_for_draft(code)
        if promoted_code in promoted_codes:
            skipped_promoted.append((code, promoted_code))
            continue
        # source code format: brand_model_COUNTRY_draft_scrapling
        # Extract country suffix: last segment before "_draft_scrapling"
        parts = code.replace("_draft_scrapling", "").rsplit("_", 1)
        if len(parts) >= 2:
            cc = parts[-1]
            if cc in countries:
                target_codes.append((cc, code))

    target_codes.sort()
    print(f"Batch {batch}: {len(target_codes)} sources across {countries}")
    if skipped_promoted:
        print(
            "Skipped "
            f"{len(skipped_promoted)} promoted draft(s) because matching "
            "production sources already exist."
        )
    print(f"{'='*70}\n")

    results = []
    pass_count = 0
    fail_count = 0
    empty_count = 0
    error_count = 0

    for i, (cc, code) in enumerate(target_codes, 1):
        t0 = time.time()
        try:
            summary = run_scrape(
                source_codes=[code], dry_run=True
            )
            src = summary["sources"].get(code, {})
            status = src.get("status", "error")
            valid = src.get("valid", 0)
            extracted = src.get("extracted", 0)
            rejected = src.get("rejected", 0)
            elapsed = time.time() - t0

            if status == "dry_run" and valid > 0:
                icon = "✅"
                pass_count += 1
            elif status == "empty":
                icon = "⬚"
                empty_count += 1
            elif status == "error":
                icon = "❌"
                error_count += 1
            else:
                icon = "⚠"
                fail_count += 1

            print(
                f"  [{i:3d}/{len(target_codes)}] {icon} {code:50s} "
                f"valid={valid} extracted={extracted} rejected={rejected} "
                f"({elapsed:.1f}s)"
            )
            results.append({
                "country": cc,
                "code": code,
                "status": status,
                "valid": valid,
                "extracted": extracted,
                "rejected": rejected,
                "elapsed": round(elapsed, 1),
            })
        except Exception as e:
            elapsed = time.time() - t0
            print(
                f"  [{i:3d}/{len(target_codes)}] ❌ {code:50s} "
                f"ERROR: {e!s:.60s}"
            )
            results.append({
                "country": cc,
                "code": code,
                "status": "exception",
                "error": str(e)[:200],
                "elapsed": round(elapsed, 1),
            })
            error_count += 1

    # Summary
    total = len(target_codes)
    print(f"\n{'='*70}")
    print(f"Results: {pass_count}/{total} PASS, {empty_count} empty, "
          f"{fail_count} rejected-all, {error_count} errors")
    print(f"{'='*70}")

    # By-country summary
    from collections import Counter
    by_country = {}
    for r in results:
        cc = r["country"]
        by_country.setdefault(cc, Counter())
        if r["status"] == "dry_run" and r.get("valid", 0) > 0:
            by_country[cc]["pass"] += 1
        elif r["status"] == "empty":
            by_country[cc]["empty"] += 1
        else:
            by_country[cc]["fail"] += 1

    print(
        f"\n{'Country':8s} {'Pass':>6s} {'Empty':>6s} "
        f"{'Fail':>6s} {'Total':>6s}"
    )
    for cc in sorted(by_country):
        c = by_country[cc]
        t = c["pass"] + c["empty"] + c["fail"]
        print(f"{cc:8s} {c['pass']:6d} {c['empty']:6d} {c['fail']:6d} {t:6d}")

    # Save report
    report_path = Path(__file__).parent / "diagnostics" / "artifacts" / "dryrun_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as f:
        json.dump({
            "batch": batch,
            "countries": countries,
            "total": total,
            "pass": pass_count,
            "empty": empty_count,
            "fail": fail_count,
            "errors": error_count,
            "results": results,
        }, f, indent=2)
    print(f"\nReport saved to {report_path}")

    if STRICT_EXIT and (fail_count > 0 or error_count > 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
