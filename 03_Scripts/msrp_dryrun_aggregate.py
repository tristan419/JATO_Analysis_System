#!/usr/bin/env python3
"""MSRP Dryrun Aggregator — Phase 4.

Reads per-country dryrun artifacts from a run directory and produces
a v3 aggregated dryrun report with pipeline/country/source layers.

Usage:
  python 03_Scripts/msrp_dryrun_aggregate.py \
    --run-dir 03_Scripts/logs/msrp-dryrun-20260521-033000 \
    --expected-countries "se fi no dk" \
    --out-latest 03_Scripts/diagnostics/artifacts/dryrun_report.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_country_artifact(path: Path) -> dict | None:
    if path.is_file():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return None


def _discover_country_artifacts(run_dir: Path) -> dict[str, dict]:
    """Load all country artifacts from a run directory."""
    artifacts_dir = run_dir / "countries"
    if not artifacts_dir.is_dir():
        return {}
    artifacts: dict[str, dict] = {}
    for p in sorted(artifacts_dir.glob("*.json")):
        cc = p.stem
        data = _load_country_artifact(p)
        if data:
            artifacts[cc] = data
    return artifacts


def _gather_source_results(artifacts: dict[str, dict]) -> list[dict]:
    results: list[dict] = []
    seen: set[str] = set()
    for cc, data in artifacts.items():
        for r in (data.get("results") or []):
            key = r.get("code") or r.get("sourceCode") or f"{cc}_{len(results)}"
            if key in seen:
                continue
            seen.add(key)
            results.append(r)
    return results


def _compute_summary(results: list[dict]) -> dict:
    total = len(results)
    empty = sum(1 for r in results if r.get("status") == "empty")
    fail = sum(1 for r in results if r.get("status") in ("error", "exception"))
    rejected = sum(1 for r in results if r.get("failureReason") == "validation_rejected_all")
    pass_count = total - empty - fail - rejected
    pass_count = max(0, pass_count)
    pass_pct = round(pass_count / total * 100, 1) if total > 0 else 0.0

    if pass_pct >= 90:
        status = "success"
    elif pass_pct >= 50:
        status = "degraded"
    else:
        status = "failure"

    failure_breakdown: dict[str, int] = {}
    strategy_recs: dict[str, int] = {}
    for r in results:
        reason = r.get("failureReason")
        if reason:
            failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
        strat = r.get("recommendedStrategy")
        if strat:
            strategy_recs[strat] = strategy_recs.get(strat, 0) + 1

    return {
        "total": total,
        "pass": pass_count,
        "empty": empty,
        "fail": fail,
        "errors": len([r for r in results if r.get("status") == "exception"]),
        "passPct": pass_pct,
        "status": status,
        "failureBreakdown": failure_breakdown,
        "strategyRecommendations": strategy_recs,
    }


def _build_countries_detail(
    artifacts: dict[str, dict],
    expected: list[str],
) -> tuple[list[dict], list[str], list[str]]:
    observed_codes = set(artifacts.keys())
    expected_set = set(expected)
    missing = sorted(expected_set - observed_codes)
    duplicates = sorted([c for c in expected if list(artifacts.keys()).count(c) > 1])

    detail: list[dict] = []
    for cc in sorted(expected):
        data = artifacts.get(cc)
        if not data:
            detail.append({
                "countryCode": cc,
                "total": 0, "pass": 0, "empty": 0, "fail": 0, "errors": 0,
                "passPct": 0.0, "status": "missing",
                "failureBreakdown": {}, "strategyRecommendations": {},
            })
            continue
        d_results = data.get("results") or []
        total = len(d_results)
        d_pass = sum(1 for r in d_results if r.get("failureReason") is None)
        d_empty = sum(1 for r in d_results if r.get("status") == "empty")
        d_fail = sum(1 for r in d_results if r.get("status") in ("error", "exception"))
        d_errors = sum(1 for r in d_results if r.get("status") == "exception")
        d_pct = round(d_pass / total * 100, 1) if total > 0 else 0.0
        d_status = "success" if d_pct >= 90 else ("degraded" if d_pct >= 50 else "failure")

        d_fb: dict[str, int] = {}
        d_sr: dict[str, int] = {}
        for r in d_results:
            reason = r.get("failureReason")
            if reason:
                d_fb[reason] = d_fb.get(reason, 0) + 1
            strat = r.get("recommendedStrategy")
            if strat:
                d_sr[strat] = d_sr.get(strat, 0) + 1

        top_reason = max(d_fb, key=d_fb.get) if d_fb else None

        detail.append({
            "countryCode": cc,
            "total": total,
            "pass": d_pass,
            "empty": d_empty,
            "fail": d_fail,
            "errors": d_errors,
            "passPct": d_pct,
            "status": d_status,
            "topFailureReason": top_reason,
            "failureBreakdown": d_fb,
            "strategyRecommendations": d_sr,
        })

    return detail, missing, duplicates


def run(
    run_dir: str,
    expected_countries: list[str],
    out_latest: str | None = None,
) -> dict:
    run_dir_path = Path(run_dir).resolve()
    artifacts = _discover_country_artifacts(run_dir_path)
    results = _gather_source_results(artifacts)
    summary = _compute_summary(results)
    run_id = run_dir_path.name

    expected_countries_sorted = sorted(set(expected_countries))
    countries_detail, missing, duplicates = _build_countries_detail(
        artifacts, expected_countries_sorted,
    )

    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": run_id,
        "batch": "batch_a",
        "expectedCountries": expected_countries_sorted,
        "observedCountries": sorted(artifacts.keys()),
        "missingCountries": missing,
        "duplicateCountries": duplicates,
        "summary": {
            **summary,
            "gateThreshold": int(os.getenv("JATO_MSRP_MIN_DRYRUN_PASS_PCT", "70")),
            "gateStatus": "allowed" if summary["passPct"] >= int(os.getenv("JATO_MSRP_MIN_DRYRUN_PASS_PCT", "70")) else "blocked",
        },
        "countriesDetail": countries_detail,
        "results": results,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if out_latest:
        out_path = Path(out_latest)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print(f"[aggregate] Latest report: {out_path}")

        # Also write timestamped copy
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        history_path = out_path.parent / f"dryrun_report_{ts}.json"
        history_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print(f"[aggregate] History: {history_path}")

    s = report["summary"]
    print(f"[aggregate] v3 report: {s['total']} sources, "
          f"passPct={s['passPct']}%, status={s['status']}, "
          f"gate={s['gateStatus']}, "
          f"countries={len(countries_detail)} "
          f"(missing={len(missing)}, dupes={len(duplicates)})")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="MSRP Dryrun Aggregator")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--expected-countries", required=True)
    parser.add_argument("--out-latest", default=None)
    args = parser.parse_args()
    expected = args.expected_countries.strip().split()
    run(args.run_dir, expected, args.out_latest)


if __name__ == "__main__":
    main()
