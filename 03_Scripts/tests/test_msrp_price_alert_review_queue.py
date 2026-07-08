from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "msrp_price_alert_review_queue.py"


def load_module():
    module_name = "msrp_price_alert_review_queue_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


queue_script = load_module()


def _alert(
    *,
    alert_id: str,
    severity: str,
    direction: str,
    delta_pct: float,
    evidence_status: str = "complete",
    threshold: bool = True,
    high_priority: bool = False,
    source_currency_changed: bool = False,
    recommended_action: str = "review_price_change_event",
) -> dict[str, object]:
    return {
        "alertId": alert_id,
        "country": "SE",
        "brand": "Volvo",
        "jatoModel": "XC60",
        "jatoTrim": "Plus Dark",
        "jatoPowertrain": "PHEV",
        "eventType": "msrp_drop" if direction == "decrease" else "msrp_increase",
        "direction": direction,
        "severity": severity,
        "changedAtUtc": "2026-06-11T20:00:00+00:00",
        "recommendedAction": recommended_action,
        "isThresholdAlert": threshold,
        "isHighPriority": high_priority,
        "currentObservationId": "obs-current-1",
        "previousObservationId": "obs-previous-1",
        "currentEvidenceId": "obs-current-1",
        "previousEvidenceId": "obs-previous-1",
        "evidenceStatus": evidence_status,
        "sourceType": "official_page",
        "sourceName": "volvo_se_official",
        "sourceUrl": "https://www.volvocars.com/se/",
        "sourceSnapshotPath": "snapshots/volvo.html",
        "currentSourceMsrpValue": 569900,
        "previousSourceMsrpValue": 599900,
        "currentSourceCurrency": "SEK",
        "previousSourceCurrency": "SEK",
        "sourceCurrencyChanged": source_currency_changed,
        "deltaSourceMsrpValue": -30000,
        "deltaMsrpValue": -2608.7,
        "deltaPct": delta_pct,
        "currentPrice": {
            "currentPriceId": "cp-1",
            "officialModel": "XC60",
            "officialTrim": "Plus Dark",
            "officialPowertrain": "PHEV",
            "matchConfidence": 0.91,
            "matchStatus": "auto_accepted",
        },
        "latestPrice": {"priceHistoryId": "ph-current"},
        "previousPrice": {"priceHistoryId": "ph-previous"},
    }


def sample_snapshot() -> dict[str, object]:
    skipped_info = _alert(
        alert_id="msrp-alert:info",
        severity="info",
        direction="increase",
        delta_pct=0.5,
        threshold=False,
        recommended_action="keep_monitoring",
    )
    missing_evidence = _alert(
        alert_id="msrp-alert:missing",
        severity="info",
        direction="increase",
        delta_pct=1.2,
        evidence_status="missing_evidence",
        threshold=False,
    )
    missing_evidence.update({
        "currentEvidenceId": "",
        "previousEvidenceId": "",
        "sourceUrl": "",
        "sourceSnapshotPath": "",
        "sourceName": "",
    })
    return {
        "schemaVersion": "msrp_current_price_snapshot_v1",
        "generatedAtUtc": "2026-06-11T20:00:00Z",
        "snapshotWeek": "2026-W24",
        "summary": {
            "currentPriceCount": 3,
            "priceAlertSummary": {
                "priceChangeEventCount": 3,
                "thresholdAlertCount": 1,
                "highPriorityAlertCount": 1,
            },
        },
        "priceAlerts": [
            _alert(
                alert_id="msrp-alert:critical",
                severity="critical",
                direction="decrease",
                delta_pct=-7.5,
                high_priority=True,
                recommended_action="review_price_drop_and_queue_sales_effectiveness",
            ),
            missing_evidence,
            skipped_info,
        ],
        "priceSalesEffectiveness": {
            "schemaVersion": "msrp_price_sales_effectiveness_v1",
            "summary": {
                "priceEventCount": 1,
                "analyzedEventCount": 1,
                "labelCounts": {"positive": 1},
                "limit": 10,
            },
            "items": [
                {
                    "analysisId": "msrp-effectiveness:se:volvo:xc60:2026-06",
                    "priceEventId": "msrp-alert:critical",
                    "priceEventMonth": "2026-06",
                    "priceChangeDirection": "down",
                    "baselineAvgSales": 100.0,
                    "postAvgSales": 128.0,
                    "salesDelta": 28.0,
                    "salesDeltaPct": 28.0,
                    "effectivenessLabel": "positive",
                    "confidenceNote": "unit-test",
                }
            ],
        },
    }


def test_build_price_alert_review_queue_filters_and_summarizes() -> None:
    payload = queue_script.build_price_alert_review_queue(sample_snapshot())

    assert payload["schemaVersion"] == "msrp_price_alert_review_queue_v1"
    assert payload["snapshotWeek"] == "2026-W24"
    assert payload["summary"] == {
        "totalCases": 2,
        "sourceAlertCount": 3,
        "skippedAlertCount": 1,
        "thresholdAlertCount": 1,
        "highPriorityAlertCount": 1,
        "missingEvidenceCount": 1,
        "sourceCurrencyReviewCount": 0,
        "effectivenessFollowUpCount": 1,
        "effectivenessLinkedCount": 1,
        "effectivenessMissingCount": 0,
        "effectivenessLabelCounts": {"positive": 1},
        "priceDropCount": 1,
        "priceIncreaseCount": 1,
        "priorityCounts": {"critical": 1, "high": 1},
        "severityCounts": {"critical": 1, "info": 1},
        "countryCount": 1,
        "countries": ["SE"],
    }

    critical, missing = payload["items"]
    assert critical["caseId"] == "msrp_price_alert_review:msrp-alert:critical"
    assert critical["reviewPriority"] == "critical"
    assert critical["requiresSalesEffectivenessFollowUp"] is True
    assert critical["officialEvidenceComplete"] is True
    assert critical["salesEffectivenessAvailable"] is True
    assert critical["salesEffectivenessLabel"] == "positive"
    assert critical["salesEffectiveness"]["salesDeltaPct"] == 28.0
    assert critical["evidence"]["latestPriceHistoryId"] == "ph-current"
    assert critical["evidence"]["matchStatus"] == "auto_accepted"

    assert missing["caseId"] == "msrp_price_alert_review:msrp-alert:missing"
    assert missing["reviewPriority"] == "high"
    assert missing["officialEvidenceComplete"] is False
    assert "missing_or_incomplete_evidence" in missing["reviewReasons"]


def test_source_currency_change_forces_critical_review() -> None:
    snapshot = sample_snapshot()
    alert = _alert(
        alert_id="msrp-alert:currency",
        severity="info",
        direction="increase",
        delta_pct=0,
        threshold=False,
        source_currency_changed=True,
        recommended_action="review_currency_or_source_semantics",
    )
    snapshot["priceAlerts"] = [alert]

    payload = queue_script.build_price_alert_review_queue(snapshot)

    assert payload["summary"]["totalCases"] == 1
    assert payload["summary"]["sourceCurrencyReviewCount"] == 1
    item = payload["items"][0]
    assert item["reviewPriority"] == "critical"
    assert item["reviewReasons"] == [
        "source_currency_changed",
        "source_semantics_review",
        "recommended_action:review_currency_or_source_semantics",
    ]


def test_run_writes_json_and_markdown(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps(sample_snapshot()), encoding="utf-8")

    payload = queue_script.run(
        snapshot_path=str(snapshot_path),
        out_dir=str(tmp_path),
    )

    json_path = tmp_path / "msrp_price_alert_review_queue.json"
    md_path = tmp_path / "msrp_price_alert_review_queue.md"
    assert json_path.exists()
    assert md_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["summary"][
        "totalCases"
    ] == 2
    markdown = md_path.read_text(encoding="utf-8")
    assert "Policy: price changes require official source evidence" in markdown
    assert "| Sales effectiveness linked | 1 |" in markdown
    assert "| critical | SE | Volvo | XC60 | Plus Dark | decrease | critical | -7.5 | complete | positive | review_price_drop_and_queue_sales_effectiveness |" in markdown
    assert payload["summary"]["skippedAlertCount"] == 1
