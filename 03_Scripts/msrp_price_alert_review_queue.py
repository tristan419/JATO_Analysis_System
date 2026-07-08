#!/usr/bin/env python3
"""Materialize a review queue from weekly MSRP price alert snapshots."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT_PATH = REPO_ROOT / "hermes" / "reports" / "msrp_current_price_snapshot.json"
DEFAULT_ARTIFACT_DIR = REPO_ROOT / "03_Scripts" / "diagnostics" / "artifacts"
PRIORITY_ORDER = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"Could not read JSON artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return payload


def _text(value: object) -> str:
    return str(value if value is not None else "").strip()


def _is_complete_evidence(alert: dict[str, Any]) -> bool:
    status = _text(alert.get("evidenceStatus")).lower()
    has_evidence_id = any(
        _text(alert.get(key))
        for key in ("currentEvidenceId", "previousEvidenceId", "evidenceId")
    )
    has_source_ref = any(
        _text(alert.get(key))
        for key in (
            "sourceUrl",
            "sourceSnapshotPath",
            "sourceDocumentPath",
            "sourceName",
        )
    )
    return status == "complete" and has_evidence_id and has_source_ref


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "y", "on"}


def _case_id(alert: dict[str, Any]) -> str:
    raw = _text(alert.get("alertId")) or ":".join([
        _text(alert.get("country")),
        _text(alert.get("brand")),
        _text(alert.get("jatoModel")),
        _text(alert.get("jatoTrim")),
        _text(alert.get("changedAtUtc")),
    ])
    safe = re.sub(r"[^a-z0-9_.:-]+", "_", raw.lower())
    return f"msrp_price_alert_review:{safe or 'unknown'}"


def _review_reasons(alert: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    severity = _text(alert.get("severity")).lower() or "info"
    direction = _text(alert.get("direction")).lower()
    action = _text(alert.get("recommendedAction")).lower()
    if _truthy(alert.get("isHighPriority")) or severity == "critical":
        reasons.append("high_priority_price_alert")
    elif _truthy(alert.get("isThresholdAlert")) or severity == "warning":
        reasons.append("threshold_price_alert")
    if not _is_complete_evidence(alert):
        reasons.append("missing_or_incomplete_evidence")
    if _truthy(alert.get("sourceCurrencyChanged")):
        reasons.append("source_currency_changed")
    if "source_semantics" in action or "currency" in action:
        reasons.append("source_semantics_review")
    if direction == "decrease" and (
        _truthy(alert.get("isThresholdAlert")) or severity in {"critical", "warning"}
    ):
        reasons.append("queue_sales_effectiveness_follow_up")
    if action and action != "keep_monitoring":
        reasons.append(f"recommended_action:{action}")
    return list(dict.fromkeys(reasons))


def _review_priority(alert: dict[str, Any], reasons: list[str]) -> str:
    severity = _text(alert.get("severity")).lower() or "info"
    if (
        severity == "critical"
        or _truthy(alert.get("isHighPriority"))
        or "source_currency_changed" in reasons
    ):
        return "critical"
    if (
        severity == "warning"
        or _truthy(alert.get("isThresholdAlert"))
        or "missing_or_incomplete_evidence" in reasons
    ):
        return "high"
    if reasons:
        return "medium"
    return "low"


def _should_queue(alert: dict[str, Any], reasons: list[str]) -> bool:
    if reasons:
        return True
    return _review_priority(alert, reasons) != "low"


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = _text(item.get(key)) or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _build_queue_item(
    alert: dict[str, Any],
    *,
    snapshot_week: str | None,
    snapshot_generated_at: str | None,
) -> dict[str, Any]:
    reasons = _review_reasons(alert)
    priority = _review_priority(alert, reasons)
    current_price = (
        alert.get("currentPrice")
        if isinstance(alert.get("currentPrice"), dict)
        else {}
    )
    return {
        "caseId": _case_id(alert),
        "queueType": "price_alert_review",
        "reviewStatus": "open",
        "reviewPriority": priority,
        "reviewReasons": reasons,
        "alertId": alert.get("alertId"),
        "priceEventId": alert.get("alertId"),
        "country": alert.get("country"),
        "brand": alert.get("brand"),
        "jatoModel": alert.get("jatoModel"),
        "jatoTrim": alert.get("jatoTrim"),
        "jatoPowertrain": alert.get("jatoPowertrain"),
        "officialModel": current_price.get("officialModel"),
        "officialTrim": current_price.get("officialTrim"),
        "officialPowertrain": current_price.get("officialPowertrain"),
        "eventType": alert.get("eventType"),
        "direction": alert.get("direction"),
        "severity": alert.get("severity"),
        "changedAtUtc": alert.get("changedAtUtc"),
        "recommendedAction": alert.get("recommendedAction"),
        "isThresholdAlert": _truthy(alert.get("isThresholdAlert")),
        "isHighPriority": _truthy(alert.get("isHighPriority")),
        "requiresHumanReview": True,
        "requiresOfficialEvidence": True,
        "requiresSalesEffectivenessFollowUp": (
            "queue_sales_effectiveness_follow_up" in reasons
        ),
        "officialEvidenceComplete": _is_complete_evidence(alert),
        "currentObservationId": alert.get("currentObservationId"),
        "previousObservationId": alert.get("previousObservationId"),
        "currentEvidenceId": alert.get("currentEvidenceId"),
        "previousEvidenceId": alert.get("previousEvidenceId"),
        "evidenceStatus": alert.get("evidenceStatus"),
        "sourceType": alert.get("sourceType"),
        "sourceName": alert.get("sourceName"),
        "sourceUrl": alert.get("sourceUrl"),
        "sourceSnapshotPath": alert.get("sourceSnapshotPath"),
        "currentSourceMsrpValue": alert.get("currentSourceMsrpValue"),
        "previousSourceMsrpValue": alert.get("previousSourceMsrpValue"),
        "currentSourceCurrency": alert.get("currentSourceCurrency"),
        "previousSourceCurrency": alert.get("previousSourceCurrency"),
        "sourceCurrencyChanged": _truthy(alert.get("sourceCurrencyChanged")),
        "deltaSourceMsrpValue": alert.get("deltaSourceMsrpValue"),
        "deltaMsrpValue": alert.get("deltaMsrpValue"),
        "deltaPct": alert.get("deltaPct"),
        "evidence": {
            "snapshotWeek": snapshot_week,
            "snapshotGeneratedAtUtc": snapshot_generated_at,
            "latestPriceHistoryId": (
                alert.get("latestPrice", {}).get("priceHistoryId")
                if isinstance(alert.get("latestPrice"), dict)
                else None
            ),
            "previousPriceHistoryId": (
                alert.get("previousPrice", {}).get("priceHistoryId")
                if isinstance(alert.get("previousPrice"), dict)
                else None
            ),
            "currentPriceId": current_price.get("currentPriceId"),
            "matchConfidence": current_price.get("matchConfidence"),
            "matchStatus": current_price.get("matchStatus"),
        },
    }


def _summary(items: list[dict[str, Any]], alert_count: int) -> dict[str, Any]:
    countries = sorted({
        _text(item.get("country")).upper()
        for item in items
        if _text(item.get("country"))
    })
    return {
        "totalCases": len(items),
        "sourceAlertCount": alert_count,
        "skippedAlertCount": max(alert_count - len(items), 0),
        "thresholdAlertCount": sum(1 for item in items if item.get("isThresholdAlert")),
        "highPriorityAlertCount": sum(1 for item in items if item.get("isHighPriority")),
        "missingEvidenceCount": sum(
            1 for item in items if not item.get("officialEvidenceComplete")
        ),
        "sourceCurrencyReviewCount": sum(
            1 for item in items if item.get("sourceCurrencyChanged")
        ),
        "effectivenessFollowUpCount": sum(
            1 for item in items if item.get("requiresSalesEffectivenessFollowUp")
        ),
        "priceDropCount": sum(1 for item in items if item.get("direction") == "decrease"),
        "priceIncreaseCount": sum(
            1 for item in items if item.get("direction") == "increase"
        ),
        "priorityCounts": _count_by(items, "reviewPriority"),
        "severityCounts": _count_by(items, "severity"),
        "countryCount": len(countries),
        "countries": countries,
    }


def build_price_alert_review_queue(snapshot: dict[str, Any]) -> dict[str, Any]:
    warnings: list[str] = []
    if snapshot.get("schemaVersion") != "msrp_current_price_snapshot_v1":
        warnings.append("unexpected_snapshot_schema")
    snapshot_week = _text(snapshot.get("snapshotWeek")) or None
    snapshot_generated_at = _text(snapshot.get("generatedAtUtc")) or None
    alerts = [
        item
        for item in list(snapshot.get("priceAlerts") or [])
        if isinstance(item, dict)
    ]
    items = [
        _build_queue_item(
            alert,
            snapshot_week=snapshot_week,
            snapshot_generated_at=snapshot_generated_at,
        )
        for alert in alerts
        if _should_queue(alert, _review_reasons(alert))
    ]
    items.sort(
        key=lambda item: (
            PRIORITY_ORDER.get(_text(item.get("reviewPriority")), 99),
            -float(item.get("isHighPriority") is True),
            -abs(float(item.get("deltaPct") or 0)),
            _text(item.get("country")),
            _text(item.get("brand")),
            _text(item.get("jatoModel")),
            _text(item.get("caseId")),
        )
    )
    return {
        "schemaVersion": "msrp_price_alert_review_queue_v1",
        "generatedAt": _utc_now(),
        "sourceSnapshotSchemaVersion": snapshot.get("schemaVersion"),
        "snapshotWeek": snapshot_week,
        "snapshotGeneratedAtUtc": snapshot_generated_at,
        "officialSourceRequiredForResolution": True,
        "warnings": warnings,
        "summary": _summary(items, len(alerts)),
        "items": items,
    }


def _markdown_cell(value: object) -> str:
    text = str(value if value is not None else "-").replace("\n", " ")
    return text.replace("|", "\\|")


def _render_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# MSRP Price Alert Review Queue",
        "",
        f"Generated: {payload.get('generatedAt') or '-'}",
        f"Snapshot week: {payload.get('snapshotWeek') or '-'}",
        "Policy: price changes require official source evidence before resolution.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Review cases | {summary.get('totalCases', 0)} |",
        f"| Source alerts | {summary.get('sourceAlertCount', 0)} |",
        f"| Skipped alerts | {summary.get('skippedAlertCount', 0)} |",
        f"| High-priority alerts | {summary.get('highPriorityAlertCount', 0)} |",
        f"| Missing evidence | {summary.get('missingEvidenceCount', 0)} |",
        f"| Currency reviews | {summary.get('sourceCurrencyReviewCount', 0)} |",
        f"| Sales effectiveness follow-up | {summary.get('effectivenessFollowUpCount', 0)} |",
        "",
        "## Cases",
        "",
    ]
    items = [item for item in payload.get("items") or [] if isinstance(item, dict)]
    if not items:
        lines.append("No price alert review cases.")
        return "\n".join(lines) + "\n"
    lines.extend([
        "| Priority | Country | Brand | Model | Trim | Direction | Severity | Delta % | Evidence | Action |",
        "|---|---|---|---|---|---|---|---:|---|---|",
    ])
    for item in items:
        lines.append(
            "| "
            + " | ".join([
                _markdown_cell(item.get("reviewPriority")),
                _markdown_cell(item.get("country")),
                _markdown_cell(item.get("brand")),
                _markdown_cell(item.get("jatoModel")),
                _markdown_cell(item.get("jatoTrim")),
                _markdown_cell(item.get("direction")),
                _markdown_cell(item.get("severity")),
                _markdown_cell(item.get("deltaPct")),
                _markdown_cell(item.get("evidenceStatus")),
                _markdown_cell(item.get("recommendedAction")),
            ])
            + " |"
        )
    return "\n".join(lines) + "\n"


def run(
    *,
    snapshot_path: str | None = None,
    out_dir: str | None = None,
) -> dict[str, Any]:
    snapshot_file = Path(snapshot_path or DEFAULT_SNAPSHOT_PATH)
    snapshot = _load_json(snapshot_file)
    payload = build_price_alert_review_queue(snapshot)
    output_dir = Path(out_dir).resolve() if out_dir else DEFAULT_ARTIFACT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "msrp_price_alert_review_queue.json"
    md_path = output_dir / "msrp_price_alert_review_queue.md"
    json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    print(f"[price-alert-review-queue] JSON: {json_path}")
    print(f"[price-alert-review-queue] Markdown: {md_path}")
    print(
        "[price-alert-review-queue] "
        f"{payload['summary']['totalCases']} cases from "
        f"{payload['summary']['sourceAlertCount']} alerts"
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build MSRP price alert review queue from weekly snapshot."
    )
    parser.add_argument("--snapshot", default=None)
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()
    run(snapshot_path=args.snapshot, out_dir=args.out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
