#!/usr/bin/env python3
"""Governed Copilot eval harness — runs test cases and reports metrics."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

_HERE = Path(__file__).resolve().parent
_BACKEND = _HERE.parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

_CASES_FILE = _HERE / "cases.yaml"


def load_cases() -> list[dict]:
    with open(_CASES_FILE) as fh:
        data = yaml.safe_load(fh)
    return list(data.get("cases", []))


def run_case(case: dict) -> dict:
    from app.copilot_governance.source_plan import plan_sources
    from app.copilot_governance.intent import LEGACY_TO_GOVERNED_INTENT

    question = case["question"]
    country = case.get("country", "")
    expected_intent = case.get("expected_intent", "")
    expected_mode = case.get("expected_execution_mode", "")
    expected_sources = set(case.get("expected_sources", []))
    optional_sources = set(case.get("optional_sources", []))

    sp = plan_sources(expected_intent, question)
    actual_mode = sp.execution_mode
    actual_sources = {item.source_id for item in sp.items}

    intent_ok = expected_intent in LEGACY_TO_GOVERNED_INTENT or True
    mode_ok = actual_mode == expected_mode
    required_covered = expected_sources.issubset(actual_sources)
    optional_covered = optional_sources.issubset(actual_sources) if optional_sources else True

    return {
        "id": case["id"],
        "question": question,
        "country": country,
        "expected_intent": expected_intent,
        "actual_mode": actual_mode,
        "expected_mode": expected_mode,
        "mode_ok": mode_ok,
        "expected_sources": sorted(expected_sources),
        "actual_sources": sorted(actual_sources),
        "required_covered": required_covered,
        "optional_covered": optional_covered,
        "passed": mode_ok and required_covered,
    }


def main() -> int:
    cases = load_cases()
    if not cases:
        print("No eval cases found.")
        return 1

    results = []
    for case in cases:
        try:
            results.append(run_case(case))
        except Exception as exc:
            results.append({
                "id": case.get("id", "?"),
                "error": str(exc),
                "passed": False,
            })

    passed = sum(1 for r in results if r.get("passed"))
    mode_ok = sum(1 for r in results if r.get("mode_ok"))
    src_ok = sum(1 for r in results if r.get("required_covered"))

    total = len(results)
    print(f"\n{'='*60}")
    print(f"Governed Copilot Eval — {total} cases")
    print(f"{'='*60}")
    print(f"  Overall pass:        {passed}/{total} ({passed*100//total}%)")
    print(f"  Execution mode OK:   {mode_ok}/{total}")
    print(f"  Required sources OK: {src_ok}/{total}")
    print()

    for r in results:
        status = "✅" if r.get("passed") else "❌"
        print(f"  {status} {r['id']}")
        if not r.get("passed"):
            if "error" in r:
                print(f"      Error: {r['error']}")
            else:
                print(f"      Expected mode: {r.get('expected_mode')}, actual: {r.get('actual_mode')}")
                print(f"      Expected sources: {r.get('expected_sources')}, actual: {r.get('actual_sources')}")

    print()
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
