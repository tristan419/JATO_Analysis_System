#!/usr/bin/env python3
"""Hermes Phase 5.3 — Country Assistant Answer Audit.

Define answer audit schema and generate sample audits.
Deterministic scoring — no LLM.

Usage:
  python 03_Scripts/hermes/hermes_answer_audit.py --sample
  python 03_Scripts/hermes/hermes_answer_audit.py --question "..." --answer-mode deep_report
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hermes_registry_loader import load_all_registries

REPO_ROOT = Path(__file__).resolve().parents[2]

ANSWER_MODES = [
    "direct_lookup",
    "short_answer",
    "grounded_analysis",
    "deep_report",
    "hypothesis",
    "insufficient_evidence",
]

MODEL_PRICING: dict[str, dict[str, float]] = {
    "deepseek-v4-flash": {
        "input_per_million": 0.14,
        "output_per_million": 0.28,
        "cached_input_per_million": 0.03,
    },
    "deepseek-v4-pro": {
        "input_per_million": 1.74,
        "output_per_million": 3.48,
        "cached_input_per_million": 0.35,
    },
}


def _new_answer_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    short = str(uuid.uuid4())[:6]
    return f"answer.{ts}.{short}"


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Estimate USD cost for token usage."""
    pricing = MODEL_PRICING.get(model, {})
    if not pricing:
        return 0.0
    input_cost = (input_tokens / 1_000_000) * pricing.get("input_per_million", 0)
    output_cost = (output_tokens / 1_000_000) * pricing.get("output_per_million", 0)
    return round(input_cost + output_cost, 6)


def _score_groundedness(
    answer_mode: str,
    evidence_ids: list[str],
    tools_used: list[str],
) -> float:
    """Score groundedness 0.0–1.0."""
    if answer_mode == "direct_lookup":
        return 1.0 if tools_used else 0.9
    if answer_mode == "insufficient_evidence":
        return 0.0
    ev_count = len(evidence_ids)
    tool_count = len(tools_used)
    if ev_count >= 3 and tool_count >= 2:
        return 0.9
    if ev_count >= 1:
        return 0.7
    if tool_count >= 1:
        return 0.5
    if answer_mode in ("deep_report", "grounded_analysis"):
        return 0.3  # Should have evidence
    return 0.5


def _score_citation_coverage(evidence_ids: list[str], tools_used: list[str]) -> float:
    """Score how well the answer covers evidence."""
    expected = max(1, len(tools_used))
    if expected == 0:
        return 0.0
    return min(1.0, len(evidence_ids) / expected)


def _score_hallucination_risk(
    answer_mode: str,
    evidence_ids: list[str],
    tools_used: list[str],
) -> float:
    """Score hallucination risk (lower = better)."""
    if answer_mode in ("deep_report", "grounded_analysis") and not evidence_ids and not tools_used:
        return 0.7  # High risk
    if evidence_ids:
        has_fact = any("jato_fact" in e or "msrp_fact" in e for e in evidence_ids)
        if has_fact:
            return 0.1
        return 0.3
    if tools_used:
        return 0.3
    if answer_mode == "hypothesis":
        return 0.5
    return 0.4


def _score_actionability(answer_mode: str, tools_used: list[str]) -> float:
    """Score actionability 0.0–1.0."""
    if answer_mode == "insufficient_evidence":
        return 0.0
    if answer_mode == "hypothesis":
        return 0.3
    if answer_mode == "direct_lookup":
        return 0.8
    if tools_used and len(tools_used) >= 2:
        return 0.8
    if answer_mode == "deep_report":
        return 0.7
    return 0.5


def create_audit_record(
    question: str = "",
    answer_mode: str = "grounded_analysis",
    model: str = "deepseek-v4-flash",
    prompt_version: str = "unversioned",
    tools_used: list[str] | None = None,
    evidence_ids: list[str] | None = None,
    input_tokens: int = 0,
    output_tokens: int = 0,
) -> dict:
    """Create a structured answer audit record."""
    tools = tools_used or []
    evidence = evidence_ids or []

    groundedness = _score_groundedness(answer_mode, evidence, tools)
    citation = _score_citation_coverage(evidence, tools)
    hallucination = _score_hallucination_risk(answer_mode, evidence, tools)
    actionability = _score_actionability(answer_mode, tools)
    cost = _estimate_cost(model, input_tokens, output_tokens)

    return {
        "answerId": _new_answer_id(),
        "question": question,
        "answerMode": answer_mode,
        "modelUsed": model,
        "promptVersion": prompt_version,
        "toolsUsed": tools,
        "evidenceIds": evidence,
        "groundednessScore": round(groundedness, 2),
        "citationCoverageScore": round(citation, 2),
        "hallucinationRiskScore": round(hallucination, 2),
        "actionabilityScore": round(actionability, 2),
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "estimatedCostUsd": cost,
        "shouldCache": groundedness >= 0.7 and hallucination <= 0.3,
        "shouldEnterKnowledgeBase": groundedness >= 0.9 and actionability >= 0.7,
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _generate_sample_audits() -> list[dict]:
    """Generate representative sample audit records."""
    samples = [
        create_audit_record(
            question="瑞典 BEV 市场 Q1 2026 销量排名",
            answer_mode="direct_lookup",
            model="deepseek-v4-flash",
            tools_used=["query_market_scan", "jato_parquet_read"],
            evidence_ids=["evidence.jato.sweden_bev_q1"],
            input_tokens=3000,
            output_tokens=800,
        ),
        create_audit_record(
            question="Can J7 PHEV sell well in Sweden compared to Kodiaq and Tiguan?",
            answer_mode="grounded_analysis",
            model="deepseek-v4-flash",
            tools_used=["query_market_scan", "query_positioning_map", "read_msrp_current"],
            evidence_ids=[
                "evidence.jato.sweden_suv_c_segment",
                "evidence.msrp.j7_phev_se",
                "evidence.msrp.kodiaq_se",
            ],
            input_tokens=12000,
            output_tokens=2000,
        ),
        create_audit_record(
            question="瑞典消费者为什么选择电动车？",
            answer_mode="deep_report",
            model="deepseek-v4-pro",
            tools_used=["query_market_scan", "read_voc_enriched", "read_news_digest"],
            evidence_ids=[
                "evidence.jato.sweden_bev_share",
                "evidence.voc.sweden_ev_motivation",
                "evidence.news.sweden_subsidy_2026",
            ],
            input_tokens=25000,
            output_tokens=4000,
        ),
        create_audit_record(
            question="J7 PHEV 在挪威会卖得好吗？",
            answer_mode="hypothesis",
            model="deepseek-v4-flash",
            tools_used=["query_market_scan"],
            evidence_ids=[],
            input_tokens=5000,
            output_tokens=1200,
        ),
        create_audit_record(
            question="瑞典和挪威的 MSRP 对比",
            answer_mode="direct_lookup",
            model="deepseek-v4-flash",
            tools_used=["read_msrp_current"],
            evidence_ids=["evidence.msrp.j7_se", "evidence.msrp.j7_no"],
            input_tokens=2000,
            output_tokens=400,
        ),
        create_audit_record(
            question="VOC forum 中有没有提到 J7 的续航焦虑？",
            answer_mode="insufficient_evidence",
            model="deepseek-v4-flash",
            tools_used=[],
            evidence_ids=[],
            input_tokens=1500,
            output_tokens=300,
        ),
    ]
    return samples


def _generate_report(samples: list[dict]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    avg_groundedness = sum(s["groundednessScore"] for s in samples) / max(len(samples), 1)
    avg_hallucination = sum(s["hallucinationRiskScore"] for s in samples) / max(len(samples), 1)
    pro_count = sum(1 for s in samples if s["modelUsed"] == "deepseek-v4-pro")
    flash_count = sum(1 for s in samples if s["modelUsed"] == "deepseek-v4-flash")
    total_cost = sum(s["estimatedCostUsd"] for s in samples)

    lines: list[str] = []
    lines.append("# Hermes Answer Audit Report\n")
    lines.append(f"**Generated:** {now}\n")

    lines.append("## 1. Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Total audit records | {len(samples)} |")
    lines.append(f"| Average groundedness | {avg_groundedness:.2f} |")
    lines.append(f"| Average hallucination risk | {avg_hallucination:.2f} |")
    lines.append(f"| Pro usage count | {pro_count} |")
    lines.append(f"| Flash usage count | {flash_count} |")
    lines.append(f"| Total estimated cost | ${total_cost:.4f} |")
    lines.append("")

    lines.append("## 2. Sample Audits\n")
    lines.append("| Answer ID | Mode | Model | Evidence | Groundedness | Hallucination Risk | Cost |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for s in samples:
        aid = s["answerId"][-20:]
        lines.append(
            f"| `...{aid}` | {s['answerMode']} | {s['modelUsed']} "
            f"| {len(s['evidenceIds'])} | {s['groundednessScore']} | {s['hallucinationRiskScore']} | ${s['estimatedCostUsd']:.4f} |"
        )
    lines.append("")

    lines.append("## 3. Recommendations\n")
    if pro_count > 0:
        lines.append(f"- [ ] {pro_count} Pro model usages — verify if Flash would suffice for lower-cost alternatives")
    high_halluc = [s for s in samples if s["hallucinationRiskScore"] >= 0.5]
    if high_halluc:
        lines.append(f"- [ ] {len(high_halluc)} answers with high hallucination risk — add evidence before answering")
    low_grounded = [s for s in samples if s["groundednessScore"] < 0.5]
    if low_grounded:
        lines.append(f"- [ ] {len(low_grounded)} answers with low groundedness — insufficient evidence or no tools used")
    cacheable = [s for s in samples if s.get("shouldCache")]
    if cacheable:
        lines.append(f"- [ ] {len(cacheable)} answers could be cached to reduce API cost")
    lines.append("")

    return "\n".join(lines)


def run(sample_mode: bool = True) -> dict:
    print("[Hermes Answer Audit] Generating answer audit records...")
    samples = _generate_sample_audits()
    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totalRecords": len(samples),
        "audits": samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Country Assistant Answer Audit")
    parser.add_argument("--sample", action="store_true", default=True, help="Generate sample audits")
    parser.add_argument("--question", default=None, help="Question text (for single audit)")
    parser.add_argument("--answer-mode", default="grounded_analysis", choices=ANSWER_MODES)
    parser.add_argument("--model", default="deepseek-v4-flash")
    parser.add_argument("--evidence-ids", default="", help="Comma-separated evidence IDs")
    parser.add_argument("--tools-used", default="", help="Comma-separated tool names")
    parser.add_argument("--input-tokens", type=int, default=0)
    parser.add_argument("--output-tokens", type=int, default=0)
    parser.add_argument("--prompt-version", default="unversioned")
    parser.add_argument("--out", default="hermes/answer_audit.jsonl")
    parser.add_argument("--report", default="hermes/reports/answer_audit_report.md")
    parser.add_argument("--registry-dir", default=None)
    args = parser.parse_args()
    os.chdir(REPO_ROOT)

    if args.question:
        evidence = [e.strip() for e in args.evidence_ids.split(",") if e.strip()]
        tools = [t.strip() for t in args.tools_used.split(",") if t.strip()]
        audits = [create_audit_record(
            question=args.question,
            answer_mode=args.answer_mode,
            model=args.model,
            prompt_version=args.prompt_version,
            tools_used=tools,
            evidence_ids=evidence,
            input_tokens=args.input_tokens,
            output_tokens=args.output_tokens,
        )]
    else:
        audits = _generate_sample_audits()

    # Write JSONL
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "a") as fh:
        for audit in audits:
            fh.write(json.dumps(audit, ensure_ascii=False) + "\n")
    print(f"[Hermes Answer Audit] Ledger: {out_path} ({len(audits)} records)")

    # Write report
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(_generate_report(audits))
    print(f"[Hermes Answer Audit] Report: {report_path}")


if __name__ == "__main__":
    main()
