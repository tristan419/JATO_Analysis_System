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


def _classify_dryrun_failure(
    src: dict,
    exception: Exception | None = None,
) -> dict:
    """Classify a dry-run failure and recommend next strategy."""
    status = src.get("status", "")
    error = str(src.get("error", "") or (str(exception) if exception else ""))
    valid = src.get("valid", 0)
    extracted = src.get("extracted", 0)
    error_lower = error.lower()

    if status == "dry_run" and valid > 0:
        return {"failureReason": None, "recommendedStrategy": None, "severity": "info"}

    if exception or status in ("exception", "error"):
        if "waiting for" in error_lower or "playwright" in error_lower:
            return {"failureReason": "js_required_or_selector_timeout", "recommendedStrategy": "try_playwright_card_flow", "severity": "warning"}
        if "timeout" in error_lower:
            return {"failureReason": "http_timeout", "recommendedStrategy": "retry_or_reduce_concurrency", "severity": "warning"}
        if "403" in error_lower or "forbidden" in error_lower:
            return {"failureReason": "forbidden_403", "recommendedStrategy": "manual_review_or_proxy_required", "severity": "error"}
        if "selector" in error_lower or "no elements" in error_lower or "TODO_SELECTOR" in error:
            return {"failureReason": "selector_empty", "recommendedStrategy": "try_scrapling_dynamic_or_playwright", "severity": "warning"}
        if "502" in error_lower or "503" in error_lower or "bad gateway" in error_lower:
            return {"failureReason": "db_or_backend_write_failed", "recommendedStrategy": "pipeline_error_not_source_error", "severity": "error"}
        return {"failureReason": "unknown", "recommendedStrategy": "diagnose_with_msrp_page_analyzer", "severity": "warning"}

    if status == "empty":
        if "TODO_SELECTOR" in error:
            return {"failureReason": "selector_empty", "recommendedStrategy": "try_scrapling_dynamic_or_playwright", "severity": "warning"}
        if "json" in error_lower and ("noth" in error_lower or "falling back" in error_lower):
            return {"failureReason": "json_ld_empty", "recommendedStrategy": "try_css_or_attr_json", "severity": "warning"}
        return {"failureReason": "no_observation_extracted", "recommendedStrategy": "diagnose_with_msrp_page_analyzer", "severity": "warning"}

    if extracted > 0 and valid == 0:
        rejected_reasons = [str(r).lower() for r in src.get("rejectedReasons", [])]
        if any("currency" in r for r in rejected_reasons):
            return {"failureReason": "currency_mismatch", "recommendedStrategy": "check_default_currency", "severity": "warning"}
        if any("price" in r and ("range" in r or "out" in r) for r in rejected_reasons):
            return {"failureReason": "price_out_of_range", "recommendedStrategy": "check_currency_and_price_semantics", "severity": "warning"}
        return {"failureReason": "validation_rejected_all", "recommendedStrategy": "review_validation_rules", "severity": "warning"}

    return {"failureReason": "unknown", "recommendedStrategy": "diagnose_with_msrp_page_analyzer", "severity": "info"}


def _write_dryrun_status(
    countries: list[str],
    pass_count: int,
    empty_count: int,
    fail_count: int,
    error_count: int,
    total: int = 0,
) -> None:
    """Write msrp_dryrun status to scheduled_fetch_status.json."""
    import json as _json
    from datetime import datetime as _datetime, timezone as _timezone
    status_path = Path(__file__).resolve().parent / "logs" / "scheduled_fetch_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if status_path.exists():
        try:
            existing = _json.loads(status_path.read_text())
        except (_json.JSONDecodeError, OSError):
            existing = {}
    total_ok = pass_count
    total_fail = fail_count + error_count
    country_total = len(countries)
    pass_pct = round(pass_count / total * 100, 1) if total > 0 else 0.0
    if pass_pct >= 90:
        status = "success"
    elif pass_pct >= 50:
        status = "degraded"
    else:
        status = "failure"
    existing["msrp_dryrun"] = {
        "lastRunAt": _datetime.now(_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "countryCount": country_total,
        "totalSources": total,
        "successCount": total_ok,
        "failureCount": total_fail,
        "passPct": pass_pct,
        "artifactPath": "03_Scripts/diagnostics/artifacts/dryrun_report.json",
        "schemaVersion": "msrp_dryrun_report_v2",
    }
    status_path.write_text(_json.dumps(existing, indent=2) + "\n")
    print(f"[status] msrp_dryrun={status} passPct={pass_pct}% written to {status_path}")


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

            classification = _classify_dryrun_failure(src)

            print(
                f"  [{i:3d}/{len(target_codes)}] {icon} {code:50s} "
                f"valid={valid} extracted={extracted} rejected={rejected} "
                f"({elapsed:.1f}s)"
            )
            result_entry = {
                "country": cc,
                "code": code,
                "status": status,
                "valid": valid,
                "extracted": extracted,
                "rejected": rejected,
                "elapsed": round(elapsed, 1),
            }
            if classification.get("failureReason"):
                result_entry["failureReason"] = classification["failureReason"]
                result_entry["recommendedStrategy"] = classification["recommendedStrategy"]
                result_entry["severity"] = classification["severity"]
            results.append(result_entry)
        except Exception as e:
            elapsed = time.time() - t0
            classification = _classify_dryrun_failure({}, exception=e)
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
                "failureReason": classification.get("failureReason", "unknown"),
                "recommendedStrategy": classification.get("recommendedStrategy", "diagnose_with_msrp_page_analyzer"),
                "severity": classification.get("severity", "warning"),
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

    # Build failure breakdown and strategy recommendations
    failure_breakdown: dict[str, int] = {}
    strategy_recs: dict[str, int] = {}
    for r in results:
        reason = r.get("failureReason")
        if reason:
            failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
        strat = r.get("recommendedStrategy")
        if strat:
            strategy_recs[strat] = strategy_recs.get(strat, 0) + 1

    _write_dryrun_status(countries, pass_count, empty_count, fail_count, error_count, total=total)

    # Save report (timestamped + latest symlink for history)
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_dir = Path(__file__).parent / "diagnostics" / "artifacts"
    report_dir.mkdir(parents=True, exist_ok=True)

    pass_pct = round(pass_count / total * 100, 1) if total > 0 else 0.0
    report_payload = {
        "schemaVersion": "msrp_dryrun_report_v2",
        "batch": batch,
        "countries": countries,
        "total": total,
        "pass": pass_count,
        "empty": empty_count,
        "fail": fail_count,
        "errors": error_count,
        "passPct": pass_pct,
        "failureBreakdown": failure_breakdown,
        "strategyRecommendations": strategy_recs,
        "results": results,
        "savedAt": datetime.now(timezone.utc).isoformat(),
    }

    # Timestamped copy for history
    history_path = report_dir / f"dryrun_report_{ts}.json"
    with open(history_path, "w") as f:
        json.dump(report_payload, f, indent=2)

    # Also overwrite latest for backward compat
    latest_path = report_dir / "dryrun_report.json"
    with open(latest_path, "w") as f:
        json.dump(report_payload, f, indent=2)

    print(f"\nReport saved to {latest_path} (history: {history_path.name})")

    if STRICT_EXIT and (fail_count > 0 or error_count > 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
