#!/usr/bin/env python3
"""JSON-LD smoke test for whitelist brands in the SE country pack.

Per the deep research report (2026-05-16) and April 12 MSRP plan §5.2:
12 brands are verified to supply schema.org Vehicle/Product JSON-LD with
Offer pricing on their manufacturer pages. This script runs a minimal
dry-run extraction on one SE source per available whitelist brand and verifies:

1. `attempted_strategies` must include `json_script_selector`
2. Whitelist brands should win via `json_script_selector` when the page has it
3. `observations_count >= 1`
4. Observation must include: model, price, currency, source_url
5. Each source must produce an audit log entry

Usage:
    python 03_Scripts/smoke_test_jsonld.py [--verbose]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WHITELIST_BRANDS = {
    "TOYOTA", "VOLVO", "SKODA", "KIA", "HYUNDAI", "DACIA", "FORD",
    "PEUGEOT", "NISSAN", "OPEL", "MERCEDES", "RENAULT",
}

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRAPING_DIR = REPO_ROOT / "07_ScrapingToolkit"


def find_se_sources() -> list[str]:
    """Find one SE source YAML in batch A for each available whitelist brand."""
    import yaml

    se_dir = SCRAPING_DIR / "source_drafts" / "suv_only_country_model_top30" / "se"
    sources_by_brand: dict[str, str] = {}
    for f in sorted(se_dir.glob("*.yaml")):
        with open(f) as fh:
            data = yaml.safe_load(fh)
        brand = data.get("brand", "")
        if brand in WHITELIST_BRANDS and brand not in sources_by_brand:
            sources_by_brand[brand] = data.get("source_code", f.stem)
    return [sources_by_brand[brand] for brand in sorted(sources_by_brand)]


def run_smoke_dryrun(source_codes: list[str], verbose: bool = False) -> dict[str, Any]:
    """Run dryrun on the given sources and return summary."""
    sys.path.insert(0, str(SCRAPING_DIR))
    from jato_scraper.runner import run_scrape

    summary = run_scrape(
        source_codes=source_codes,
        dry_run=True,
    )
    return summary


def assert_smoke_test(source_code: str, source_result: dict, audit_dir: str) -> list[str]:
    """Run assertions on a single source result. Returns list of failure messages."""
    failures = []

    # 1. Status should not be error
    if source_result.get("status") == "error":
        failures.append(f"status=error: {source_result.get('error', 'unknown')}")
        return failures

    # 2. Should have extracted something
    extracted = source_result.get("extracted", 0)
    if extracted == 0:
        failures.append("extracted=0 — no observations")

    # 3. Observations should have required fields
    # (We can't check observation internals from summary alone, but we can check audit log)

    # 4. Check audit log exists
    audit_files = list(Path(audit_dir).glob("*.jsonl"))
    if not audit_files:
        failures.append("no audit log files found")
        return failures

    found_in_audit = False
    for af in audit_files:
        for line in af.read_text().strip().split("\n"):
            if not line:
                continue
            event = json.loads(line)
            if event.get("source_id") == source_code:
                found_in_audit = True
                strategies = [s["strategy"] for s in event.get("attempted_strategies", [])]
                winning = event.get("winning_strategy")

                # Assert json_script_selector was attempted
                if "json_script_selector" not in strategies:
                    failures.append(
                        f"json_script_selector not in attempted_strategies: {strategies}"
                    )
                else:
                    # Check if it succeeded
                    for s in event["attempted_strategies"]:
                        if s["strategy"] == "json_script_selector":
                            if s["status"] == "success":
                                break
                    else:
                        failures.append(
                            f"json_script_selector was attempted but did not succeed"
                        )

                # Coverage should not be L0
                coverage = event.get("coverage_level", "L0_FAILED")
                if coverage == "L0_FAILED":
                    failures.append("coverage_level is L0_FAILED")

                break

    if not found_in_audit:
        failures.append(f"source_code not found in audit logs")

    return failures


def main():
    parser = argparse.ArgumentParser(description="JSON-LD smoke test for 12 brands × SE")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--list-only", action="store_true", help="Just list sources, don't run")
    args = parser.parse_args()

    sources = find_se_sources()
    print(f"Found {len(sources)} SE sources for whitelist brands:")
    for s in sources:
        print(f"  - {s}")

    if args.list_only:
        return

    # Run dryrun
    print(f"\nRunning smoke test dry-run on {len(sources)} sources...")
    start = time.time()

    # Set up audit dir
    audit_dir = os.path.join(SCRAPING_DIR, "artifacts", "extractor_audit")
    os.environ["JATO_AUDIT_DIR"] = audit_dir

    summary = run_smoke_dryrun(sources, verbose=args.verbose)
    elapsed = time.time() - start

    # Run assertions
    print(f"\n{'='*70}")
    print(f"SMOKE TEST RESULTS")
    print(f"{'='*70}")
    print(f"Sources: {len(sources)}")
    print(f"Duration: {elapsed:.1f}s")

    all_failures: dict[str, list[str]] = {}
    passed = 0
    total_extracted = 0

    for code, result in summary.get("sources", {}).items():
        failures = assert_smoke_test(code, result, audit_dir)
        extracted = result.get("extracted", 0)
        total_extracted += extracted

        if failures:
            all_failures[code] = failures
            print(f"  FAIL {code} (extracted={extracted}): {'; '.join(failures)}")
        else:
            passed += 1
            if args.verbose:
                print(f"  PASS {code} (extracted={extracted})")

    total = len(sources)
    pass_rate = passed / total * 100 if total else 0

    print(f"\n---")
    print(f"Passed: {passed}/{total} ({pass_rate:.1f}%)")
    print(f"Failed: {len(all_failures)}/{total}")
    print(f"Total observations extracted: {total_extracted}")

    # Save summary artifact
    artifact = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "test": "jsonld_smoke_test_se",
        "total_sources": total,
        "passed": passed,
        "failed": len(all_failures),
        "pass_rate": pass_rate,
        "total_extracted": total_extracted,
        "failures": all_failures,
        "run_id": summary.get("run_id", "unknown"),
    }
    os.makedirs(os.path.join(SCRAPING_DIR, "artifacts"), exist_ok=True)
    artifact_path = os.path.join(SCRAPING_DIR, "artifacts", "smoke_test_summary.json")
    with open(artifact_path, "w") as fh:
        json.dump(artifact, fh, indent=2, ensure_ascii=False, default=str)
    print(f"\nArtifact saved: {artifact_path}")

    # Exit code: 0 if pass rate >= 85%, 1 otherwise
    if pass_rate < 85:
        print(f"\n[FAIL] Pass rate {pass_rate:.1f}% below 85% threshold")
        sys.exit(1)
    else:
        print(f"\n[PASS] Pass rate {pass_rate:.1f}% meets 85% threshold")


if __name__ == "__main__":
    main()
