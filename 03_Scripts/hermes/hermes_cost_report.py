#!/usr/bin/env python3
"""Hermes Phase 5.5 - Cost Governor.

Aggregate Country Copilot answer audits and AstrBot usage ledgers into the
same Hermes cost report. Deterministic. No LLM. No API calls.

Usage:
  python 03_Scripts/hermes/hermes_cost_report.py
  python 03_Scripts/hermes/hermes_cost_report.py --pricing hermes/model_pricing.yaml
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = REPO_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services.hermes_cost_ledger_service import (  # noqa: E402
    build_cost_report,
    generate_markdown_report,
)


def run(
    pricing_path: str = "hermes/model_pricing.yaml",
    audit_path: str = "hermes/answer_audit.jsonl",
    agent_usage_path: str = "hermes/agent_usage.jsonl",
    eval_usage_path: str = "hermes/eval/eval_usage.jsonl",
) -> dict[str, Any]:
    print("[Hermes Cost Report] Calculating...")
    results = build_cost_report(
        REPO_ROOT,
        pricing_path=pricing_path,
        answer_audit_path=audit_path,
        agent_usage_path=agent_usage_path,
        eval_usage_path=eval_usage_path,
    )

    summary = results.get("summary", {})
    by_model = results.get("byModel", {})
    by_source = results.get("bySource", {})
    print(f"  Records: {summary.get('recordCount', 0)}")
    print(
        "  Total cost: "
        f"{float(summary.get('totalEstimatedCostCny') or 0):.4f} "
        f"{results.get('currency', 'CNY')} ({summary.get('budgetStatus', 'ok')})"
    )
    print(
        "  AstrBot cost: "
        f"{float(summary.get('astrbotEstimatedCostCny') or 0):.4f} "
        f"{results.get('currency', 'CNY')} "
        f"({by_source.get('astrbot', {}).get('records', 0)} records)"
    )
    print(f"  Flash: {by_model.get('deepseek-v4-flash', {}).get('records', 0)} records")
    print(f"  Pro: {by_model.get('deepseek-v4-pro', {}).get('records', 0)} records")
    return results


def _resolve_output(path: str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return REPO_ROOT / target


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Cost Governor")
    parser.add_argument("--pricing", default="hermes/model_pricing.yaml")
    parser.add_argument("--answer-audit", default="hermes/answer_audit.jsonl")
    parser.add_argument("--agent-usage", default="hermes/agent_usage.jsonl")
    parser.add_argument("--eval-usage", default="hermes/eval/eval_usage.jsonl")
    parser.add_argument("--out-json", default="hermes/reports/cost_report.json")
    parser.add_argument("--out-md", default="hermes/reports/cost_report.md")
    args = parser.parse_args()
    os.chdir(REPO_ROOT)

    results = run(args.pricing, args.answer_audit, args.agent_usage, args.eval_usage)

    out_json = _resolve_output(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps(results, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"[Hermes Cost Report] JSON: {out_json}")

    out_md = _resolve_output(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(generate_markdown_report(results), encoding="utf-8")
    print(f"[Hermes Cost Report] Markdown: {out_md}")


if __name__ == "__main__":
    main()
