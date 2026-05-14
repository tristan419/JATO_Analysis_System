#!/usr/bin/env python3
"""Hermes Phase 5.2 — Evidence Ledger Writer.

Extract structured evidence from known artifact types into:
  hermes/evidence_ledger.jsonl

Deterministic — no LLM. Does not guess schema. Does not invent claims.

Usage:
  python 03_Scripts/hermes/hermes_evidence_writer.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hermes_registry_loader import load_all_registries

REPO_ROOT = Path(__file__).resolve().parents[2]

EVIDENCE_TYPES = {
    "jato_fact",
    "msrp_fact",
    "config_fact",
    "voc_quote",
    "news_event",
    "llm_inference",
}


def _safe(v: Any, key: str, default: Any = None) -> Any:
    if isinstance(v, dict):
        return v.get(key, default)
    return default


def _new_evidence_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    short = str(uuid.uuid4())[:6]
    return f"evidence.{ts}.{short}"


def _make_evidence(
    etype: str,
    claim: str,
    source_ref: str,
    artifact_id: str = "",
    country: str = "",
    brand: str = "",
    model: str = "",
    confidence: float = 0.6,
    source_url: str = "",
) -> dict:
    return {
        "evidenceId": _new_evidence_id(),
        "evidenceType": etype,
        "country": country,
        "brand": brand,
        "model": model,
        "segment": "",
        "fuelType": "",
        "claim": claim,
        "sourceRef": source_ref,
        "sourceUrl": source_url,
        "artifactId": artifact_id,
        "confidence": confidence,
        "supportCount": 0,
        "contradictionCount": 0,
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _try_extract_voc_evidence(artifacts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Try to extract VOC evidence from raw artifacts if available."""
    evidence: list[dict] = []
    skipped: list[dict] = []

    voc_raw = [a for a in artifacts if "voc.raw" in _safe(a, "artifactId", "")]
    voc_enriched = [a for a in artifacts if "voc.enriched" in _safe(a, "artifactId", "")]

    if voc_raw:
        art = voc_raw[0]
        art_path = _safe(art, "path", "")
        local = REPO_ROOT / art_path if art_path else None
        if local and local.is_dir():
            json_files = sorted(local.glob("**/*.json"))
            for jf in json_files[:20]:
                try:
                    data = json.loads(jf.read_text())
                    if isinstance(data, list):
                        for item in data[:5]:
                            if isinstance(item, dict):
                                text = item.get("text", item.get("content", item.get("body", "")))
                                if text and len(str(text)) > 20:
                                    evidence.append(_make_evidence(
                                        "voc_quote",
                                        str(text)[:300],
                                        f"VOC artifact: {jf.name}",
                                        _safe(art, "artifactId", "artifact.voc.raw"),
                                        country=item.get("country", item.get("country_code", "")),
                                        confidence=0.6,
                                    ))
                except Exception:
                    pass
        else:
            skipped.append({
                "artifactId": _safe(art, "artifactId", "?"),
                "path": art_path,
                "reason": "VOC raw artifact directory not found locally (server-only)",
            })
    elif voc_enriched:
        skipped.append({
            "artifactId": _safe(voc_enriched[0], "artifactId", "?"),
            "reason": "VOC enriched signals exist but raw text extraction from enriched format needs schema",
        })
    else:
        skipped.append({"artifactId": "artifact.voc.*", "reason": "No VOC artifacts found in registry"})

    return evidence, skipped


def _try_extract_news_evidence(artifacts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Try to extract news evidence from news status data."""
    evidence: list[dict] = []
    skipped: list[dict] = []

    news_digest = [a for a in artifacts if "news.digest" in _safe(a, "artifactId", "")]
    status_path = REPO_ROOT / "03_Scripts" / "logs" / "scheduled_fetch_status.json"

    if status_path.is_file():
        try:
            status = json.loads(status_path.read_text())
            voc = status.get("voc", {})
            if voc and voc.get("status") == "success":
                evidence.append(_make_evidence(
                    "news_event",
                    f"News fetch pipeline completed: {voc.get('successCount', 0)} articles, {voc.get('failedCount', 0)} failures",
                    "scheduled_fetch_status.json",
                    "artifact.status_json",
                    confidence=0.9,
                ))
        except Exception:
            pass

    if news_digest:
        art = news_digest[0]
        if "PostgreSQL" in _safe(art, "path", ""):
            skipped.append({
                "artifactId": _safe(art, "artifactId", "?"),
                "reason": "News digest stored in PostgreSQL — needs DB connection for extraction",
            })
    else:
        skipped.append({"artifactId": "artifact.news.*", "reason": "No local news artifact path available"})

    return evidence, skipped


def _try_extract_msrp_evidence(artifacts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Try to extract MSRP evidence from known source data."""
    evidence: list[dict] = []
    skipped: list[dict] = []

    msrp_obs = [a for a in artifacts if "msrp.observations" in _safe(a, "artifactId", "")]
    msrp_prices = [a for a in artifacts if "msrp.current_prices" in _safe(a, "artifactId", "")]

    if msrp_obs:
        art = msrp_obs[0]
        if "PostgreSQL" in _safe(art, "path", ""):
            skipped.append({
                "artifactId": _safe(art, "artifactId", "?"),
                "reason": "MSRP observations in PostgreSQL — needs DB connection for extraction",
            })
        # Add a fact from registry metadata
        last_obs = _safe(art, "lastObserved", {}) or {}
        note = _safe(art, "notes", "")
        if note:
            evidence.append(_make_evidence(
                "msrp_fact",
                note,
                "artifact registry metadata",
                _safe(art, "artifactId", ""),
                confidence=0.8,
            ))

    if msrp_prices:
        skipped.append({
            "artifactId": _safe(msrp_prices[0], "artifactId", "?"),
            "reason": "Current prices in PostgreSQL — needs DB connection for extraction",
        })

    return evidence, skipped


def _try_extract_jato_evidence(artifacts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Try to extract JATO facts from known parquet metadata."""
    evidence: list[dict] = []
    skipped: list[dict] = []

    jato_parquet = [a for a in artifacts if "jato.parquet" in _safe(a, "artifactId", "")]
    if jato_parquet:
        art = jato_parquet[0]
        art_path = _safe(art, "path", "")
        local = REPO_ROOT / art_path if art_path else None
        if local and local.is_file():
            evidence.append(_make_evidence(
                "jato_fact",
                f"JATO full archive parquet available at {art_path} ({local.stat().st_size / 1e9:.1f} GB)",
                art_path,
                _safe(art, "artifactId", ""),
                confidence=0.95,
            ))
        else:
            skipped.append({
                "artifactId": _safe(art, "artifactId", "?"),
                "path": art_path,
                "reason": "JATO parquet not found locally (large file, server-only)",
            })

    return evidence, skipped


def _generate_report(
    evidence: list[dict],
    skipped: list[dict],
    artifact_types: dict[str, int],
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines: list[str] = []
    lines.append("# Hermes Evidence Ledger Report\n")
    lines.append(f"**Generated:** {now}\n")

    lines.append("## 1. Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Evidence records written | {len(evidence)} |")
    for etype, count in sorted(artifact_types.items()):
        lines.append(f"| {etype} | {count} |")
    lines.append(f"| Skipped artifacts | {len(skipped)} |")
    lines.append("")

    if skipped:
        lines.append("## 2. Skipped Artifacts\n")
        lines.append("| Artifact | Reason |")
        lines.append("|---|---|")
        for s in skipped:
            lines.append(f"| `{s.get('artifactId', '?')}` | {s.get('reason', '')} |")
        lines.append("")

    lines.append("## 3. Evidence Samples\n")
    if evidence:
        lines.append("| Evidence ID | Type | Claim | Source |")
        lines.append("|---|---|---|---|")
        for e in evidence[:20]:
            lines.append(f"| `{e['evidenceId']}` | {e['evidenceType']} | {e['claim'][:80]} | {e['sourceRef'][:40]} |")
    else:
        lines.append("_No evidence extracted. Most artifacts are in PostgreSQL or server-only paths._")
    lines.append("")

    lines.append("## 4. Recommendations\n")
    if skipped:
        pg_count = sum(1 for s in skipped if "PostgreSQL" in s.get("reason", ""))
        server_count = sum(1 for s in skipped if "server-only" in s.get("reason", ""))
        if pg_count > 0:
            lines.append(f"- [ ] {pg_count} artifact(s) in PostgreSQL — add DB connection support in Phase 6")
        if server_count > 0:
            lines.append(f"- [ ] {server_count} artifact(s) server-only — run evidence extraction on server")
        lines.append("- [ ] Define artifact extraction schema for each artifact type")
    lines.append("")

    return "\n".join(lines)


def run(registry_dir: str | None = None) -> dict:
    print("[Hermes Evidence Writer] Extracting evidence...")
    registries = load_all_registries(registry_dir)
    artifacts = registries.get("artifacts", [])

    all_evidence: list[dict] = []
    all_skipped: list[dict] = []
    type_counts: dict[str, int] = {}

    for extractor in [
        ("VOC", _try_extract_voc_evidence),
        ("News", _try_extract_news_evidence),
        ("MSRP", _try_extract_msrp_evidence),
        ("JATO", _try_extract_jato_evidence),
    ]:
        name, fn = extractor
        ev, sk = fn(artifacts)
        all_evidence.extend(ev)
        all_skipped.extend(sk)
        for e in ev:
            t = e["evidenceType"]
            type_counts[t] = type_counts.get(t, 0) + 1
        print(f"  {name}: {len(ev)} evidence, {len(sk)} skipped")

    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evidence": all_evidence,
        "skipped": all_skipped,
        "typeCounts": type_counts,
        "totalEvidence": len(all_evidence),
        "totalSkipped": len(all_skipped),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Evidence Ledger Writer")
    parser.add_argument("--registry-dir", default=None)
    parser.add_argument("--out", default="hermes/evidence_ledger.jsonl")
    parser.add_argument("--report", default="hermes/reports/evidence_ledger_report.md")
    args = parser.parse_args()
    os.chdir(REPO_ROOT)

    results = run(args.registry_dir)

    # Write JSONL
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as fh:
        for e in results["evidence"]:
            fh.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"[Hermes Evidence Writer] Ledger: {out_path}")

    # Write report
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(_generate_report(
        results["evidence"],
        results["skipped"],
        results["typeCounts"],
    ))
    print(f"[Hermes Evidence Writer] Report: {report_path}")


if __name__ == "__main__":
    main()
