#!/usr/bin/env python3
"""Hermes Phase 5.5 — Cost Governor.

Calculate estimated costs from answer audit JSONL and model pricing config.
Deterministic. No LLM. No API calls.

Usage:
  python 03_Scripts/hermes/hermes_cost_report.py
  python 03_Scripts/hermes/hermes_cost_report.py --pricing hermes/model_pricing.yaml
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]


def _safe(v: Any, key: str, default: Any = None) -> Any:
    if isinstance(v, dict):
        return v.get(key, default)
    return default


def load_pricing(path: str) -> dict:
    """Load model pricing YAML."""
    import yaml

    p = Path(path)
    if not p.is_absolute():
        p = REPO_ROOT / p
    if not p.is_file():
        print(f"[WARN] Pricing file not found: {p}. Using defaults.")
        return _default_pricing()
    return yaml.safe_load(p.read_text()) or _default_pricing()


def _default_pricing() -> dict:
    return {
        "currency": "CNY",
        "models": {
            "deepseek-v4-flash": {
                "pricing": {
                    "inputCacheHitCnyPerMillionTokens": 0.02,
                    "inputCacheMissCnyPerMillionTokens": 1.0,
                    "outputCnyPerMillionTokens": 2.0,
                    "discount": {"active": False},
                }
            },
            "deepseek-v4-pro": {
                "pricing": {
                    "inputCacheHitCnyPerMillionTokens": 0.025,
                    "inputCacheMissCnyPerMillionTokens": 3.0,
                    "outputCnyPerMillionTokens": 6.0,
                    "discount": {"active": True, "validUntil": "2026-05-31T23:59:00+08:00"},
                }
            },
        },
        "monthlyBudget": {"totalBudgetCny": 500, "warningThresholdRatio": 0.75},
    }


def _model_pricing(pricing_data: dict, model: str) -> dict:
    models = pricing_data.get("models", {})
    return models.get(model, {}).get("pricing", {})


def _calc_cost(audit: dict, pricing: dict) -> dict:
    """Calculate cost for a single audit record."""
    model = audit.get("modelUsed", "deepseek-v4-flash")
    mp = _model_pricing(pricing, model)

    input_tokens = audit.get("inputTokens", 0) or 0
    output_tokens = audit.get("outputTokens", 0) or 0
    cache_hit = audit.get("cacheHitInputTokens", 0) or 0
    cache_miss = audit.get("cacheMissInputTokens", 0) or 0

    # If no cache split, assume all cache miss
    if not cache_hit and not cache_miss and input_tokens > 0:
        cache_miss = input_tokens

    hit_price = mp.get("inputCacheHitCnyPerMillionTokens", 0)
    miss_price = mp.get("inputCacheMissCnyPerMillionTokens", 0)
    out_price = mp.get("outputCnyPerMillionTokens", 0)

    input_cost = (cache_hit / 1_000_000) * hit_price + (cache_miss / 1_000_000) * miss_price
    output_cost = (output_tokens / 1_000_000) * out_price
    total = input_cost + output_cost

    discount = mp.get("discount", {}) or {}
    discount_active = discount.get("active", False)
    if discount_active:
        valid_until = discount.get("validUntil", "")
        if valid_until:
            try:
                from datetime import datetime as dt

                expires = dt.fromisoformat(valid_until.replace("+08:00", "+08:00"))
                if dt.now(timezone.utc) > expires:
                    discount_active = False
            except Exception:
                pass

    return {
        "model": model,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "cacheHitInputTokens": cache_hit,
        "cacheMissInputTokens": cache_miss,
        "cacheHitRatio": round(cache_hit / max(input_tokens, 1), 3),
        "estimatedCostCny": round(total, 6),
        "discountActive": discount_active,
        "hasCacheSplit": bool(audit.get("cacheHitInputTokens") or audit.get("cacheMissInputTokens")),
    }


def _generate_routing_findings(audits: list[dict]) -> list[dict]:
    """Detect routing policy violations."""
    findings: list[dict] = []
    for a in audits:
        mode = a.get("answerMode", "")
        model = a.get("modelUsed", "")

        if mode == "direct_lookup" and model == "deepseek-v4-pro":
            findings.append(
                {
                    "severity": "WARNING",
                    "finding": f"direct_lookup using Pro: {a.get('question', '')[:60]}",
                    "recommendation": "direct_lookup should use Flash or no LLM.",
                }
            )
        if mode == "insufficient_evidence" and model == "deepseek-v4-pro":
            findings.append(
                {
                    "severity": "WARNING",
                    "finding": "insufficient_evidence using Pro",
                    "recommendation": "No evidence available — should not consume Pro budget.",
                }
            )
        if mode == "hypothesis" and model == "deepseek-v4-pro":
            findings.append(
                {
                    "severity": "INFO",
                    "finding": "hypothesis using Pro — review necessity",
                    "recommendation": "Flash is sufficient for speculative answers.",
                }
            )

    return findings


def _generate_report(results: dict) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    s = results["summary"]
    by_model = results["byModel"]
    by_mode = results["byAnswerMode"]
    top = results["topExpensiveRecords"][:10]
    findings = results["routingFindings"]

    lines: list[str] = []
    lines.append("# Hermes Cost Report\n")
    lines.append(f"**Generated:** {now}\n")

    lines.append("## 1. Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Currency | {results['currency']} |")
    lines.append(f"| Total estimated cost | {s['totalEstimatedCostCny']:.4f} CNY |")
    lines.append(f"| Total input tokens | {s['totalInputTokens']:,} |")
    lines.append(f"| Total output tokens | {s['totalOutputTokens']:,} |")
    lines.append(f"| Cache-hit input tokens | {s['totalCacheHitInputTokens']:,} |")
    lines.append(f"| Cache-miss input tokens | {s['totalCacheMissInputTokens']:,} |")
    lines.append(f"| Cache hit ratio | {s['cacheHitRatio']:.1%} |")
    lines.append(f"| Budget | {s['budgetCny']:.0f} CNY |")
    budget_icon = (
        "exceeded" if s["budgetStatus"] == "exceeded" else ("warning" if s["budgetStatus"] == "warning" else "ok")
    )
    lines.append(f"| Budget status | {budget_icon} |")
    lines.append("")

    lines.append("## 2. Cost by Model\n")
    lines.append("| Model | Records | Input Tokens | Output Tokens | Estimated Cost (CNY) | Discount |")
    lines.append("|---|---:|---:|---:|---:|")
    for mid, m in by_model.items():
        d = "active" if m.get("discountActive") else "none"
        estimated_cost = f"{m['estimatedCostCny']:.4f}"
        lines.append(
            f"| {mid} | {m['records']} | {m['inputTokens']:,} | "
            f"{m['outputTokens']:,} | {estimated_cost} | {d} |"
        )
    lines.append("")

    if by_mode:
        lines.append("## 3. Cost by Answer Mode\n")
        lines.append("| Answer Mode | Records | Model Mix | Estimated Cost (CNY) |")
        lines.append("|---|---:|---:|---:|")
        for mid, m in by_mode.items():
            mix = m.get("modelMix", "")
            lines.append(f"| {mid} | {m['records']} | {mix} | {m['estimatedCostCny']:.4f} |")
        lines.append("")

    if top:
        lines.append("## 4. Top Expensive Records\n")
        lines.append("| Answer ID | Mode | Model | Input | Output | Cost (CNY) |")
        lines.append("|---|---|---:|---:|---:|")
        for t in top:
            aid = t.get("answerId", "")[-20:]
            lines.append(
                f"| `...{aid}` | {t['answerMode']} | {t['modelUsed']} "
                f"| {t['inputTokens']:,} | {t['outputTokens']:,} | {t['estimatedCostCny']:.6f} |"
            )
        lines.append("")

    if findings:
        lines.append("## 5. Routing Findings\n")
        lines.append("| Severity | Finding | Recommendation |")
        lines.append("|---|---|---|")
        for f in findings:
            finding = f["finding"][:100]
            recommendation = f["recommendation"][:100]
            lines.append(f"| {f['severity']} | {finding} | {recommendation} |")
        lines.append("")

    lines.append("## 6. Notes\n")
    lines.append("- Pricing loaded from `hermes/model_pricing.yaml`. Verify against DeepSeek billing console.")
    lines.append(
        "- Cache split fields (cacheHitInputTokens/cacheMissInputTokens) not yet present "
        "in audit records — assuming all cache-miss."
    )
    lines.append("- Pro discount (2.5折) is time-limited. Review cost estimates before 2026-05-31.")
    lines.append("")

    return "\n".join(lines)


def run(
    pricing_path: str = "hermes/model_pricing.yaml",
    audit_path: str = "hermes/answer_audit.jsonl",
) -> dict:
    print("[Hermes Cost Report] Calculating...")
    pricing = load_pricing(pricing_path)
    currency = pricing.get("currency", "CNY")
    budget = pricing.get("monthlyBudget", {})
    budget_cny = budget.get("totalBudgetCny", 500)
    warn_ratio = budget.get("warningThresholdRatio", 0.75)

    # Load audit records
    audit_file = Path(audit_path)
    if not audit_file.is_absolute():
        audit_file = REPO_ROOT / audit_file

    audits: list[dict] = []
    if audit_file.is_file():
        for line in audit_file.read_text().strip().split("\n"):
            if line.strip():
                try:
                    audits.append(json.loads(line))
                except Exception:
                    pass
    print(f"  {len(audits)} audit records loaded")

    # Calculate per-record costs
    records = [_calc_cost(a, pricing) for a in audits]

    # Summary
    total_cost = sum(r["estimatedCostCny"] for r in records)
    total_input = sum(r["inputTokens"] for r in records)
    total_output = sum(r["outputTokens"] for r in records)
    total_cache_hit = sum(r["cacheHitInputTokens"] for r in records)
    total_cache_miss = sum(r["cacheMissInputTokens"] for r in records)
    cache_hit_ratio = total_cache_hit / max(total_input, 1)

    budget_status = "ok"
    if total_cost >= budget_cny:
        budget_status = "exceeded"
    elif total_cost >= budget_cny * warn_ratio:
        budget_status = "warning"

    # By model
    by_model: dict[str, dict] = {}
    for r in records:
        m = r["model"]
        if m not in by_model:
            mp = _model_pricing(pricing, m)
            disc = mp.get("discount", {}) or {}
            by_model[m] = {
                "records": 0,
                "inputTokens": 0,
                "outputTokens": 0,
                "estimatedCostCny": 0.0,
                "discountActive": disc.get("active", False),
                "discountValidUntil": disc.get("validUntil", ""),
            }
        by_model[m]["records"] += 1
        by_model[m]["inputTokens"] += r["inputTokens"]
        by_model[m]["outputTokens"] += r["outputTokens"]
        by_model[m]["estimatedCostCny"] += r["estimatedCostCny"]

    # By answer mode
    by_mode: dict[str, dict] = {}
    for i, r in enumerate(records):
        a = audits[i]
        mode = a.get("answerMode", "unknown")
        if mode not in by_mode:
            by_mode[mode] = {"records": 0, "estimatedCostCny": 0.0, "models": set()}
        by_mode[mode]["records"] += 1
        by_mode[mode]["estimatedCostCny"] += r["estimatedCostCny"]
        by_mode[mode]["models"].add(r["model"])

    by_mode_out: dict[str, dict] = {}
    for mid, m in by_mode.items():
        by_mode_out[mid] = {
            "records": m["records"],
            "estimatedCostCny": round(m["estimatedCostCny"], 6),
            "modelMix": ", ".join(sorted(m["models"])),
        }

    # Top expensive
    combined = [{**audits[i], **records[i]} for i in range(len(records))]
    combined.sort(key=lambda x: x["estimatedCostCny"], reverse=True)

    # Routing findings
    routing = _generate_routing_findings(audits)

    print(f"  Total cost: {total_cost:.4f} {currency} ({budget_status})")
    print(f"  Flash: {by_model.get('deepseek-v4-flash', {}).get('records', 0)} records")
    print(f"  Pro: {by_model.get('deepseek-v4-pro', {}).get('records', 0)} records")

    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "currency": currency,
        "summary": {
            "totalEstimatedCostCny": round(total_cost, 4),
            "totalInputTokens": total_input,
            "totalOutputTokens": total_output,
            "totalCacheHitInputTokens": total_cache_hit,
            "totalCacheMissInputTokens": total_cache_miss,
            "cacheHitRatio": round(cache_hit_ratio, 3),
            "budgetCny": budget_cny,
            "budgetStatus": budget_status,
        },
        "byModel": by_model,
        "byAnswerMode": by_mode_out,
        "topExpensiveRecords": combined[:10],
        "routingFindings": routing,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Cost Governor")
    parser.add_argument("--pricing", default="hermes/model_pricing.yaml")
    parser.add_argument("--answer-audit", default="hermes/answer_audit.jsonl")
    parser.add_argument("--out-json", default="hermes/reports/cost_report.json")
    parser.add_argument("--out-md", default="hermes/reports/cost_report.md")
    args = parser.parse_args()
    os.chdir(REPO_ROOT)

    results = run(args.pricing, args.answer_audit)

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    print(f"[Hermes Cost Report] JSON: {out_json}")

    out_md = Path(args.out_md)
    out_md.write_text(_generate_report(results))
    print(f"[Hermes Cost Report] Markdown: {out_md}")


if __name__ == "__main__":
    main()
