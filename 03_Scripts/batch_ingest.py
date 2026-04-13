#!/usr/bin/env python3
"""Batch ingest — run non-dry-run scrape and POST the result to backend.

Usage:
    python batch_ingest.py all          # All Batch 1+2 countries
    python batch_ingest.py se           # Single country
    python batch_ingest.py hu,at,ch     # Multiple countries
"""
import logging
import os
import sys
import time
from pathlib import Path

_toolkit_dir = str(
    Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
)
if _toolkit_dir not in sys.path:
    sys.path.insert(0, _toolkit_dir)

from jato_scraper.config_loader import load_all_sources
from jato_scraper.runner import run_scrape

_TOOLKIT_ROOT = Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
_DRAFTS_DIR = _TOOLKIT_ROOT / "source_drafts" / "suv_only_country_model_top30"

BATCH_COUNTRIES = {
    "1": ["se", "hr"],
    "2": ["hu", "no", "at", "cz", "ch"],
    "all": ["se", "hr", "hu", "no", "at", "cz", "ch"],
}

API_BASE = os.getenv("JATO_API_BASE", "http://localhost:8000/v1").rstrip("/")
STRICT_EXIT = os.getenv("JATO_STRICT_EXIT", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
log = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    batch = sys.argv[1] if len(sys.argv) > 1 else "all"
    countries = BATCH_COUNTRIES.get(batch, batch.split(","))

    # Load both promoted sources and draft sources
    all_codes = load_all_sources()
    all_codes += load_all_sources(sources_dir=_DRAFTS_DIR)
    draft_codes = [c for c in all_codes if c.endswith("_draft_scrapling")]
    target_codes = []
    for code in draft_codes:
        parts = code.replace("_draft_scrapling", "").rsplit("_", 1)
        if len(parts) >= 2:
            cc = parts[-1]
            if cc in countries:
                target_codes.append((cc, code))

    target_codes.sort()
    print(
        f"Batch ingest {batch}: {len(target_codes)} sources across {countries}"
    )
    print(f"API: {API_BASE}")
    print(f"{'='*70}\n")

    ok_count = 0
    empty_count = 0
    fail_count = 0

    for i, (cc, code) in enumerate(target_codes, 1):
        t0 = time.time()
        try:
            summary = run_scrape(
                source_codes=[code],
                api_base=API_BASE,
                trigger_type="manual",
                dry_run=False,
            )
            src = summary["sources"].get(code, {})
            status = src.get("status", "error")
            valid = src.get("valid", 0)
            elapsed = time.time() - t0

            if status == "ok":
                icon = "✅"
                ok_count += 1
            elif status == "empty":
                icon = "⬚"
                empty_count += 1
            else:
                icon = "❌"
                fail_count += 1
                if "error" in src:
                    print(f"    error: {src['error'][:120]}")

            print(
                f"  [{i:3d}/{len(target_codes)}] {icon} {code:50s} "
                f"status={status} valid={valid} ({elapsed:.1f}s)"
            )
        except Exception as e:
            elapsed = time.time() - t0
            print(
                f"  [{i:3d}/{len(target_codes)}] ❌ {code:50s} "
                f"EXCEPTION: {e!s:.80s}"
            )
            fail_count += 1

    total = len(target_codes)
    print(f"\n{'='*70}")
    print(
        f"Ingest: {ok_count}/{total} OK, {empty_count} empty, "
        f"{fail_count} failed"
    )
    print(f"{'='*70}")

    if STRICT_EXIT and fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
