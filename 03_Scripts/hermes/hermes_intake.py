#!/usr/bin/env python3
"""Hermes Phase 2 — PRD Intake.

Read a PRD markdown file, match against Hermes registries, and generate:
  - hermes/reports/hermes_intake_report.md
  - hermes/reports/hermes_intake_report.json

Usage:
  python 03_Scripts/hermes/hermes_intake.py path/to/PRD.md
  python 03_Scripts/hermes/hermes_intake.py path/to/PRD.md --out custom_report.md
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from hermes_registry_loader import load_all_registries
from hermes_text_matcher import (
    detect_risks,
    extract_prd_info,
    match_artifacts,
    match_features,
    match_pipelines,
    match_prompts,
    match_sources,
)


def _repo_root() -> Path:
    """Find the repository root relative to this script."""
    return Path(__file__).resolve().parents[2]


def _ensure_output_dir(report_path: str) -> Path:
    """Make sure the output directory exists."""
    p = Path(report_path)
    if not p.is_absolute():
        p = _repo_root() / p
    p.parent.mkdir(parents=True, exist_ok=True)
    return p.resolve()


def _read_prd(prd_path: str) -> str:
    """Read a PRD file. Exit gracefully if not found."""
    p = Path(prd_path)
    if not p.is_absolute():
        p = _repo_root() / p
    if not p.is_file():
        print(f"[ERROR] PRD file not found: {p}")
        sys.exit(1)
    return p.read_text(encoding="utf-8")


def _format_match_table(matches: list[dict], id_field: str, name_field: str) -> str:
    """Render a markdown table of matched entries."""
    if not matches:
        return "_No matches found._\n"

    lines = [
        f"| {id_field} | {name_field} | Score | Confidence | Why |",
        f"|---|{'---' if name_field else ''}|---:|---|---|",
    ]
    for m in matches[:10]:
        e = m["entry"]
        eid = e.get(id_field, "?")
        name = e.get(name_field, "") if name_field else ""
        score = m["score"]
        conf = m["confidence"]
        why = "; ".join(m["reasons"][:3])
        if name_field:
            lines.append(f"| `{eid}` | {name} | {score:.1f} | {conf} | {why} |")
        else:
            lines.append(f"| `{eid}` | {score:.1f} | {conf} | {why} |")
    return "\n".join(lines) + "\n"


def _generate_report(
    prd_info: dict,
    matched: dict,
    risks: list[dict],
    prd_path: str,
    out_path: str,
) -> str:
    """Generate the markdown intake report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    title = prd_info.get("title") or Path(prd_path).stem

    lines: list[str] = []
    lines.append(f"# Hermes Requirement Intake Report\n")
    lines.append(f"## 1. PRD Summary\n")
    lines.append(f"- **PRD file:** `{prd_path}`")
    lines.append(f"- **Title:** {title}")
    lines.append(f"- **Generated at:** {now}")

    headings = prd_info.get("headings", [])
    if headings:
        lines.append(f"- **Sections detected:** {len(headings)}")
    kws = prd_info.get("detected_keywords", {})
    if kws:
        kw_summary = ", ".join(f"{cat}({len(v)})" for cat, v in sorted(kws.items()))
        lines.append(f"- **Keywords detected:** {kw_summary}")

    routes = prd_info.get("routes", [])
    api_paths = prd_info.get("api_paths", [])
    if routes:
        lines.append(f"- **Routes mentioned:** {', '.join(f'`{r}`' for r in routes[:8])}")
    if api_paths:
        lines.append(f"- **API paths mentioned:** {', '.join(f'`{a}`' for a in api_paths[:8])}")
    lines.append("")

    # ── Matched Features ──
    lines.append(f"## 2. Matched Features\n")
    lines.append(_format_match_table(matched["features"], "featureId", "name"))

    # ── Matched Pipelines ──
    lines.append(f"## 3. Matched Pipelines\n")
    lines.append(_format_match_table(matched["pipelines"], "pipelineId", "name"))

    # ── Matched Sources ──
    lines.append(f"## 4. Matched Sources\n")
    lines.append(_format_match_table(matched["sources"], "sourceId", "name"))

    # ── Matched Prompts ──
    lines.append(f"## 5. Matched Prompts\n")
    lines.append(_format_match_table(matched["prompts"], "promptId", "name"))

    # ── Matched Artifacts ──
    lines.append(f"## 6. Matched Artifacts\n")
    lines.append(_format_match_table(matched["artifacts"], "artifactId", "name"))

    # ── Risks ──
    lines.append(f"## 7. Risk Assessment\n")
    if risks:
        lines.append(f"| Area | Risk | Reason |")
        lines.append(f"|---|---|---|")
        for r in risks:
            lines.append(f"| {r['area']} | {r['risk']} | {r['reason']} |")
    else:
        lines.append("_No specific risks detected._")
    lines.append("")

    # ── Required Registry Updates ──
    lines.append(f"## 8. Required Registry Updates\n")
    has_updates = False
    if matched["features"]:
        lines.append(f"- [ ] Feature Registry: review {len(matched['features'])} matched features")
        has_updates = True
    if matched["pipelines"]:
        lines.append(f"- [ ] Pipeline Registry: review {len(matched['pipelines'])} matched pipelines")
        has_updates = True
    if matched["sources"]:
        lines.append(f"- [ ] Source Registry: review {len(matched['sources'])} matched sources")
        has_updates = True
    if matched["prompts"]:
        lines.append(f"- [ ] Prompt Registry: review {len(matched['prompts'])} matched prompts")
        has_updates = True
    if matched["artifacts"]:
        lines.append(f"- [ ] Artifact Registry: review {len(matched['artifacts'])} matched artifacts")
        has_updates = True
    if not has_updates:
        lines.append("_No registry updates identified._")
    lines.append("")

    # ── Required Tests ──
    lines.append(f"## 9. Required Tests\n")
    kws_set = set()
    for v in kws.values():
        kws_set.update(v)
    has_backend = bool({"backend", "api", "fastapi", "endpoint", "service"} & kws_set)
    has_frontend = bool({"frontend", "react", "component", "page", "ui", "route"} & kws_set)
    has_pipeline = bool({"pipeline", "crawler", "airflow", "systemd", "etl"} & kws_set)
    has_prompt = bool({"prompt", "llm", "deepseek"} & kws_set)
    has_contract = has_backend and has_frontend

    if has_backend:
        lines.append("- [ ] Backend unit tests")
    if has_frontend:
        lines.append("- [ ] Frontend component tests")
    if has_contract:
        lines.append("- [ ] API contract test (backend serializer ↔ frontend type)")
    if has_pipeline:
        lines.append("- [ ] Pipeline integration test")
    if has_prompt:
        lines.append("- [ ] Prompt / answer snapshot test")
    if not any([has_backend, has_frontend, has_pipeline, has_prompt]):
        lines.append("_No specific test requirements detected._")
    lines.append("")

    # ── Claude Code Task Brief ──
    lines.append(f"## 10. Claude Code Task Brief\n")
    lines.append("```txt")
    lines.append(f"Feature: {title}")
    lines.append("Goal: See PRD for full requirements.")

    affected_files = prd_info.get("file_paths", [])
    if affected_files:
        lines.append(f"Affected files: {', '.join(affected_files[:8])}")
    else:
        lines.append("Affected files: (determine from scope)")

    reg_ids: list[str] = []
    for cat in ["features", "pipelines", "prompts", "artifacts"]:
        for m in matched[cat][:3]:
            eid = m["entry"].get(
                f"{cat.rstrip('s')}Id",
                m["entry"].get(
                    "featureId",
                    m["entry"].get("pipelineId", m["entry"].get("promptId", m["entry"].get("artifactId", "?"))),
                ),
            )
            reg_ids.append(eid)
    if reg_ids:
        lines.append(f"Affected registries: {', '.join(reg_ids[:8])}")

    test_items: list[str] = []
    if has_backend:
        test_items.append("Backend unit tests")
    if has_frontend:
        test_items.append("Frontend component tests")
    if has_contract:
        test_items.append("API contract test")
    lines.append(f"Required tests: {', '.join(test_items) if test_items else 'Review PRD for test requirements'}")

    lines.append("Do not do:")
    lines.append("  - Do not auto-deploy")
    lines.append("  - Do not modify production env")
    lines.append("  - Do not change DB schema without migration")
    lines.append("  - Do not skip registry updates")

    lines.append("Acceptance criteria:")
    lines.append("  - See PRD §11 or equivalent")
    lines.append("```")
    lines.append("")

    # ── Human Review ──
    lines.append(f"## 11. Human Review Notes\n")
    low_conf = []
    id_fields = ["featureId", "pipelineId", "sourceId", "promptId", "artifactId"]
    for cat in ["features", "pipelines", "sources", "prompts", "artifacts"]:
        for m in matched[cat]:
            if m["confidence"] == "low":
                eid = "?"
                for fld in id_fields:
                    if fld in m["entry"]:
                        eid = str(m["entry"][fld])
                        break
                low_conf.append(f"  - `{eid}` ({cat}, score={m['score']})")
    if low_conf:
        lines.append("- **Low-confidence matches (please verify):**")
        lines.extend(low_conf[:8])
    else:
        lines.append("- No low-confidence matches to verify.")

    lines.append(
        "- **Suggested next step:** Review matched registries, confirm scope, "
        "then proceed with Claude Code implementation."
    )
    lines.append("")

    return "\n".join(lines)


def _generate_json(
    prd_info: dict,
    matched: dict,
    risks: list[dict],
    prd_path: str,
) -> dict:
    """Generate machine-readable JSON report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _simplify(matches: list[dict]) -> list[dict]:
        return [
            {
                "id": m["entry"].get(
                    next((k for k in m["entry"] if k.endswith("Id")), "id"),
                    "?",
                ),
                "name": m["entry"].get("name", ""),
                "score": m["score"],
                "confidence": m["confidence"],
                "reasons": m["reasons"],
            }
            for m in matches
        ]

    brief_lines: list[str] = []
    brief_lines.append(f"Feature: {prd_info.get('title', 'Unknown')}")
    brief_lines.append("Goal: See PRD for full requirements.")
    brief_lines.append("Do not auto-deploy. Do not modify production env.")

    return {
        "prdFile": prd_path,
        "title": prd_info.get("title", ""),
        "generatedAt": now,
        "matchedFeatures": _simplify(matched["features"]),
        "matchedPipelines": _simplify(matched["pipelines"]),
        "matchedSources": _simplify(matched["sources"]),
        "matchedPrompts": _simplify(matched["prompts"]),
        "matchedArtifacts": _simplify(matched["artifacts"]),
        "riskAssessment": risks,
        "requiredRegistryUpdates": [
            f"{cat} registry: {len(matched[cat])} matches"
            for cat in ["features", "pipelines", "sources", "prompts", "artifacts"]
            if matched[cat]
        ],
        "requiredTests": [
            t
            for t in [
                (
                    "Backend unit tests"
                    if any(kw in str(prd_info.get("detected_keywords", {})) for kw in ["backend", "api"])
                    else None
                ),
                (
                    "Frontend component tests"
                    if any(kw in str(prd_info.get("detected_keywords", {})) for kw in ["frontend", "component"])
                    else None
                ),
            ]
            if t
        ],
        "claudeCodeTaskBrief": "\n".join(brief_lines),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hermes PRD Intake — analyze PRD impact using Hermes registries",
    )
    parser.add_argument(
        "prd",
        help="Path to the PRD markdown file",
    )
    parser.add_argument(
        "--registry-dir",
        default=None,
        help="Path to hermes/ registry directory (auto-detect if omitted)",
    )
    parser.add_argument(
        "--out",
        default="hermes/reports/hermes_intake_report.md",
        help="Output path for the markdown report (default: hermes/reports/hermes_intake_report.md)",
    )
    parser.add_argument(
        "--json-out",
        default=None,
        help="Output path for JSON report (default: derived from --out)",
    )
    args = parser.parse_args()

    # Resolve report paths
    repo = _repo_root()
    out_md = _ensure_output_dir(args.out)
    if args.json_out:
        out_json = _ensure_output_dir(args.json_out)
    else:
        out_json = out_md.parent / (out_md.stem + ".json")

    # Load registries
    print(f"[Hermes] Loading registries...")
    registries = load_all_registries(args.registry_dir)

    # Read PRD
    print(f"[Hermes] Reading PRD: {args.prd}")
    prd_text = _read_prd(args.prd)
    prd_info = extract_prd_info(prd_text, args.prd)

    print(f"[Hermes] Title: {prd_info['title']}")
    kws = prd_info["detected_keywords"]
    if kws:
        for cat, words in sorted(kws.items()):
            print(f"  Keywords [{cat}]: {', '.join(words)}")

    # Match
    print(f"[Hermes] Matching against registries...")
    matched = {
        "features": match_features(prd_info, registries.get("features", [])),
        "pipelines": match_pipelines(prd_info, registries.get("pipelines", [])),
        "sources": match_sources(prd_info, registries.get("sources", [])),
        "prompts": match_prompts(prd_info, registries.get("prompts", [])),
        "artifacts": match_artifacts(prd_info, registries.get("artifacts", [])),
    }

    for cat, items in matched.items():
        high = sum(1 for m in items if m["confidence"] == "high")
        med = sum(1 for m in items if m["confidence"] == "medium")
        low = sum(1 for m in items if m["confidence"] == "low")
        if items:
            print(f"  {cat}: {len(items)} matches (H:{high} M:{med} L:{low})")

    # Risks
    risks = detect_risks(prd_info)
    print(f"  Risks: {len(risks)} areas identified")

    # Generate reports
    md_report = _generate_report(prd_info, matched, risks, args.prd, str(out_md))
    out_md.write_text(md_report, encoding="utf-8")
    print(f"[Hermes] Markdown report: {out_md}")

    json_report = _generate_json(prd_info, matched, risks, args.prd)
    out_json.write_text(
        json.dumps(json_report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"[Hermes] JSON report: {out_json}")

    print(f"[Hermes] Intake complete.")


if __name__ == "__main__":
    main()
