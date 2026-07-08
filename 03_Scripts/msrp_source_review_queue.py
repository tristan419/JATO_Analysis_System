#!/usr/bin/env python3
"""Build a source-level MSRP review queue from dryrun governance artifacts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_DIR = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"
DEFAULT_BACKLOG_PATH = DEFAULT_ARTIFACT_DIR / "msrp_source_repair_backlog.json"
DEFAULT_REFERENCE_PATH = DEFAULT_ARTIFACT_DIR / "msrp_source_reference_evidence.json"

QUEUE_TYPE_BY_FIELD = (
    ("sourceRepairIssues", "source_repair"),
    ("businessResolutionIssues", "business_resolution"),
    ("transientRegressions", "transient_recheck"),
)
QUEUE_TYPE_SUMMARY_KEY = {
    "source_repair": "sourceRepairCount",
    "business_resolution": "businessResolutionCount",
    "transient_recheck": "transientRecheckCount",
}
QUEUE_TYPE_ORDER = {
    "source_repair": 0,
    "business_resolution": 1,
    "transient_recheck": 2,
}
PRIORITY_BAND_ORDER = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "business": 3,
    "recheck": 4,
    "low": 5,
}


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"Could not read JSON artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return payload


def _source_key(country_code: object, source_code: object) -> tuple[str, str]:
    return (
        str(country_code or "").strip().lower(),
        str(source_code or "").strip(),
    )


def _case_id(country_code: str, source_code: str, failure_reason: str) -> str:
    raw = f"{country_code}:{source_code}:{failure_reason}"
    return "msrp_source_review:" + re.sub(r"[^a-z0-9_.:-]+", "_", raw.lower())


def _reference_items_by_source(
    reference_evidence: dict[str, Any] | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    if not reference_evidence:
        return {}
    items_by_source: dict[tuple[str, str], dict[str, Any]] = {}
    for item in reference_evidence.get("items") or []:
        if not isinstance(item, dict):
            continue
        country_code = str(item.get("countryCode") or "").strip().lower()
        for source_code in item.get("sourceCodes") or []:
            key = _source_key(country_code, source_code)
            if key[0] and key[1]:
                items_by_source[key] = item
    return items_by_source


def _safe_float(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _reference_assist_for(
    group: dict[str, Any],
    reference_item: dict[str, Any] | None,
) -> dict[str, Any]:
    if reference_item and isinstance(reference_item.get("referenceAssist"), dict):
        return dict(reference_item["referenceAssist"])
    if isinstance(group.get("referenceAssist"), dict):
        return dict(group["referenceAssist"])
    return {}


def _build_queue_item(
    *,
    source: dict[str, Any],
    group: dict[str, Any],
    queue_type: str,
    reference_item: dict[str, Any] | None,
    backlog_run_id: str | None,
    reference_generated_at: str | None,
) -> dict[str, Any]:
    country_code = str(source.get("countryCode") or "").strip().lower()
    source_code = str(source.get("sourceCode") or "").strip()
    failure_reason = str(
        source.get("failureReason") or group.get("failureReason") or ""
    ).strip()
    reference_assist = _reference_assist_for(group, reference_item)
    local_references = (
        list(reference_item.get("localPriceReferences") or [])
        if reference_item
        else []
    )
    reference_policy = (
        str(
            (reference_item or {}).get("referencePolicy")
            or reference_assist.get("referencePolicy")
            or ""
        ).strip()
        or None
    )
    official_required = bool(
        reference_assist.get("officialSourceRequiredForIngest")
        if reference_assist
        else True
    )
    official_ingest_eligible = bool(
        (reference_item or {}).get("officialIngestEligible")
        if reference_item
        else False
    )

    return {
        "caseId": _case_id(country_code, source_code, failure_reason),
        "queueType": queue_type,
        "countryCode": country_code,
        "sourceCode": source_code,
        "brand": source.get("brand"),
        "host": source.get("host"),
        "sourceUrl": source.get("sourceUrl"),
        "status": source.get("status"),
        "rawStatus": source.get("rawStatus"),
        "failureReason": failure_reason,
        "recommendedAction": source.get("recommendedAction")
        or group.get("recommendedAction"),
        "recommendedStrategy": source.get("recommendedStrategy")
        or group.get("recommendedStrategy"),
        "priorityBand": group.get("priorityBand") or "low",
        "priorityScore": _safe_float(group.get("priorityScore")),
        "referenceAssistPreferred": reference_assist.get("preferred"),
        "referenceSource": (reference_item or {}).get("referenceSource")
        or reference_assist.get("thirdPartyReference"),
        "referencePolicy": reference_policy,
        "officialSourceRequiredForIngest": official_required,
        "officialIngestEligible": official_ingest_eligible,
        "reviewRecommendation": (reference_item or {}).get("reviewRecommendation")
        or "repair_official_source",
        "modelQuery": (reference_item or {}).get("modelQuery"),
        "pricingCountry": (reference_item or {}).get("pricingCountry"),
        "localReferenceCount": len(local_references),
        "localPriceReferences": local_references[:8],
        "acceptanceRules": reference_assist.get("acceptanceRules") or [],
        "evidence": {
            "backlogRunId": backlog_run_id,
            "referenceEvidenceGeneratedAt": reference_generated_at,
            "valid": _safe_int(source.get("valid")),
            "extracted": _safe_int(source.get("extracted")),
            "extractorName": source.get("extractorName"),
            "coverageLevel": source.get("coverageLevel"),
            "errorSnippet": source.get("errorSnippet"),
            "lastKnownGoodRunId": source.get("lastKnownGoodRunId"),
        },
    }


def build_source_review_queue(
    backlog: dict[str, Any],
    reference_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reference_by_source = _reference_items_by_source(reference_evidence)
    backlog_run_id = str(backlog.get("runId") or "") or None
    reference_generated_at = (
        str((reference_evidence or {}).get("generatedAt") or "") or None
    )
    items: list[dict[str, Any]] = []
    for group in backlog.get("groups") or []:
        if not isinstance(group, dict):
            continue
        for field, queue_type in QUEUE_TYPE_BY_FIELD:
            for source in group.get(field) or []:
                if not isinstance(source, dict):
                    continue
                key = _source_key(source.get("countryCode"), source.get("sourceCode"))
                if not key[0] or not key[1]:
                    continue
                items.append(
                    _build_queue_item(
                        source=source,
                        group=group,
                        queue_type=queue_type,
                        reference_item=reference_by_source.get(key),
                        backlog_run_id=backlog_run_id,
                        reference_generated_at=reference_generated_at,
                    )
                )

    items.sort(
        key=lambda item: (
            QUEUE_TYPE_ORDER.get(str(item.get("queueType")), 99),
            PRIORITY_BAND_ORDER.get(str(item.get("priorityBand")), 99),
            -_safe_float(item.get("priorityScore")),
            str(item.get("countryCode") or ""),
            str(item.get("sourceCode") or ""),
        )
    )
    country_codes = {
        str(item.get("countryCode") or "").upper()
        for item in items
        if str(item.get("countryCode") or "").strip()
    }
    summary = {
        "totalCases": len(items),
        "sourceRepairCount": 0,
        "businessResolutionCount": 0,
        "transientRecheckCount": 0,
        "referenceOnlyCount": sum(
            1
            for item in items
            if item.get("referencePolicy") == "reference_only_review_required"
        ),
        "officialSourceRequiredCount": sum(
            1 for item in items if item.get("officialSourceRequiredForIngest")
        ),
        "officialIngestEligibleCount": sum(
            1 for item in items if item.get("officialIngestEligible")
        ),
        "localReferenceCount": sum(
            _safe_int(item.get("localReferenceCount")) for item in items
        ),
        "countryCount": len(country_codes),
        "countries": sorted(country_codes),
    }
    for item in items:
        key = QUEUE_TYPE_SUMMARY_KEY.get(str(item.get("queueType")))
        if key:
            summary[key] += 1

    return {
        "schemaVersion": "msrp_source_review_queue_v1",
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "backlogRunId": backlog.get("runId"),
        "referenceEvidenceGeneratedAt": (reference_evidence or {}).get("generatedAt"),
        "officialSourceRequiredForIngest": True,
        "officialIngestEligible": False,
        "summary": summary,
        "items": items,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# MSRP Source Review Queue",
        "",
        f"Generated: {payload.get('generatedAt') or '-'}",
        f"Backlog run: {payload.get('backlogRunId') or '-'}",
        "Policy: third-party references are review-only and never official ingest evidence.",
        "",
        f"Total cases: {summary.get('totalCases', 0)}",
        f"Source repair: {summary.get('sourceRepairCount', 0)}",
        f"Business resolution: {summary.get('businessResolutionCount', 0)}",
        f"Transient recheck: {summary.get('transientRecheckCount', 0)}",
        f"Reference-only cases: {summary.get('referenceOnlyCount', 0)}",
        "",
        "| Type | Country | Source | Brand | Failure | Action | References | Ingest eligible |",
        "|---|---|---|---|---|---|---:|---|",
    ]
    for item in payload.get("items") or []:
        lines.append(
            "| {queue_type} | {country} | `{source}` | {brand} | {failure} | {action} | {refs} | {eligible} |".format(
                queue_type=item.get("queueType") or "-",
                country=str(item.get("countryCode") or "-").upper(),
                source=item.get("sourceCode") or "-",
                brand=item.get("brand") or "-",
                failure=item.get("failureReason") or "-",
                action=item.get("recommendedAction") or "-",
                refs=item.get("localReferenceCount") or 0,
                eligible="yes" if item.get("officialIngestEligible") else "no",
            )
        )
    return "\n".join(lines) + "\n"


def run(
    *,
    backlog_path: str | None = None,
    reference_path: str | None = None,
    out_dir: str | None = None,
) -> dict[str, Any]:
    backlog_file = Path(backlog_path or DEFAULT_BACKLOG_PATH)
    reference_file = Path(reference_path or DEFAULT_REFERENCE_PATH)
    backlog = _load_json(backlog_file)
    reference_evidence = _load_json(reference_file) if reference_file.is_file() else None
    payload = build_source_review_queue(backlog, reference_evidence)

    output_dir = Path(out_dir).resolve() if out_dir else DEFAULT_ARTIFACT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "msrp_source_review_queue.json"
    md_path = output_dir / "msrp_source_review_queue.md"
    json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    print(f"[review-queue] JSON: {json_path}")
    print(f"[review-queue] Markdown: {md_path}")
    print(
        "[review-queue] "
        f"{payload['summary']['totalCases']} cases, "
        f"{payload['summary']['referenceOnlyCount']} reference-only"
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build source-level MSRP review queue from dryrun artifacts."
    )
    parser.add_argument("--backlog", default=None)
    parser.add_argument("--reference", default=None)
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()
    run(
        backlog_path=args.backlog,
        reference_path=args.reference,
        out_dir=args.out_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
