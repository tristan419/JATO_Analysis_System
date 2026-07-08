from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "hermes"
    / "hermes_msrp_current_price_snapshot.py"
)


def load_module():
    module_name = "hermes_msrp_current_price_snapshot_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


snapshot_module = load_module()


def sample_snapshot() -> dict[str, object]:
    return {
        "schemaVersion": "msrp_current_price_snapshot_v1",
        "generatedAtUtc": "2026-06-11T20:00:00+00:00",
        "snapshotWeek": "2026-W24",
        "filters": {"country": "SE", "brand": "Volvo", "jatoModel": "XC60"},
        "summary": {
            "currentPriceCount": 1,
            "returnedCurrentPriceCount": 1,
            "priceAlertCount": 1,
            "returnedPriceAlertCount": 1,
            "priceAlertThresholdPct": 3.0,
            "priceAlertSummary": {
                "priceChangeEventCount": 1,
                "thresholdAlertCount": 1,
                "highPriorityAlertCount": 1,
                "directionCounts": {"decrease": 1},
                "severityCounts": {"critical": 1},
            },
            "limit": 5,
        },
        "currentPrices": [{"currentPriceId": "cp-1"}],
        "priceAlerts": [
            {
                "country": "SE",
                "brand": "Volvo|Polestar",
                "jatoModel": "XC60",
                "jatoTrim": "Plus\nDark",
                "direction": "decrease",
                "severity": "critical",
                "deltaPct": -6.0,
                "recommendedAction": "review_price_drop",
            }
        ],
        "warnings": [],
    }


def sample_effectiveness() -> dict[str, object]:
    return {
        "schemaVersion": "msrp_price_sales_effectiveness_v1",
        "generatedAtUtc": "2026-06-11T20:00:01+00:00",
        "filters": {"country": "SE", "brand": "Volvo", "jatoModel": "XC60"},
        "window": {
            "baselineWindowMonths": 3,
            "postWindowMonths": 3,
            "postLagMonths": 1,
            "minMonths": 1,
        },
        "summary": {
            "priceEventCount": 1,
            "analyzedEventCount": 1,
            "labelCounts": {"positive": 1},
            "limit": 5,
        },
        "items": [
            {
                "country": "SE",
                "brand": "Volvo",
                "jatoModel": "XC60",
                "priceEventMonth": "2026-03",
                "priceChangeDirection": "down",
                "salesDeltaPct": 18.5,
                "effectivenessLabel": "positive",
            }
        ],
        "warnings": [],
    }


def sample_reconciliation() -> dict[str, object]:
    return {
        "schemaVersion": "msrp_multi_source_reconciliation_v1",
        "generatedAtUtc": "2026-06-11T20:00:02+00:00",
        "filters": {"country": "SE", "brand": "Volvo", "jatoModel": "XC60"},
        "thresholdPct": 1.0,
        "summary": {
            "observationRows": 4,
            "reconciliationGroupCount": 1,
            "statusCounts": {"conflict": 1},
            "limit": 5,
        },
        "items": [
            {
                "country": "SE",
                "brand": "Volvo",
                "jatoModel": "XC60",
                "jatoTrim": "Ultra",
                "status": "conflict",
                "recommendedAction": "review_conflicting_sources",
                "sourceCount": 2,
                "spreadPct": 4.2,
            }
        ],
    }


def sample_finance() -> dict[str, object]:
    return {
        "rows": 1,
        "total": 1,
        "limit": 5,
        "offset": 0,
        "summary": {
            "priceSemanticsCounts": {"lease_monthly": 1},
            "financeTypeCounts": {"private_lease": 1},
            "monthlyPaymentCount": 1,
            "monthlyPaymentEurMin": 520.87,
            "monthlyPaymentEurMax": 520.87,
            "netPriceAfterSubsidyCount": 1,
            "netPriceAfterSubsidyEurMin": 65043.48,
            "netPriceAfterSubsidyEurMax": 65043.48,
            "subsidyObservationCount": 1,
        },
        "items": [
            {
                "country": "SE",
                "brand": "Volvo",
                "jatoModel": "XC60",
                "priceSemantics": "lease_monthly",
                "financeType": "private_lease",
                "monthlyPaymentEur": 520.87,
                "netPriceAfterSubsidyEur": 65043.48,
            }
        ],
    }


def empty_snapshot() -> dict[str, object]:
    return {
        **sample_snapshot(),
        "summary": {
            "currentPriceCount": 0,
            "returnedCurrentPriceCount": 0,
            "priceAlertCount": 0,
            "returnedPriceAlertCount": 0,
            "priceAlertThresholdPct": 3.0,
            "priceAlertSummary": {
                "priceChangeEventCount": 0,
                "thresholdAlertCount": 0,
                "highPriorityAlertCount": 0,
                "directionCounts": {},
                "severityCounts": {},
            },
            "limit": 5,
        },
        "currentPrices": [],
        "priceAlerts": [],
        "warnings": [],
    }


def test_render_markdown_escapes_table_cells() -> None:
    markdown = snapshot_module._render_markdown({
        **sample_snapshot(),
        "summary": {
            **sample_snapshot()["summary"],
            "effectivenessSummary": sample_effectiveness()["summary"],
            "reconciliationSummary": sample_reconciliation()["summary"],
            "financeSummary": sample_finance()["summary"],
        },
        "priceSalesEffectiveness": sample_effectiveness(),
        "multiSourceReconciliation": sample_reconciliation(),
        "financeObservations": sample_finance(),
    })

    assert "| Current prices | 1 |" in markdown
    assert "Volvo\\|Polestar" in markdown
    assert "Plus Dark" in markdown
    assert "## Sales Effectiveness" in markdown
    assert "| positive | 1 |" in markdown
    assert "| SE | Volvo | XC60 | 2026-03 | down | 18.5 | positive |" in markdown
    assert "## Multi-source Reconciliation" in markdown
    assert "| conflict | 1 |" in markdown
    assert "| SE | Volvo | XC60 | Ultra | conflict | 2 | 4.2 | review_conflicting_sources |" in markdown
    assert "## Finance And Net Incentives" in markdown
    assert "| Monthly payment rows | 1 |" in markdown
    assert "| SE | Volvo | XC60 | lease_monthly | private_lease | 520.87 | 65,043.48 |" in markdown


def test_write_outputs_accepts_out_dir_outside_repo(tmp_path: Path) -> None:
    artifacts = snapshot_module._write_outputs(sample_snapshot(), tmp_path)

    assert artifacts["latestJson"] == str(tmp_path / "msrp_current_price_snapshot.json")
    assert (tmp_path / "msrp_current_price_snapshot.json").exists()
    assert (tmp_path / "msrp_current_price_snapshot.md").exists()
    assert len(list(tmp_path.glob("msrp_current_price_snapshot_*.json"))) == 1
    assert len(list(tmp_path.glob("msrp_current_price_snapshot_*.md"))) == 1


def test_run_writes_degraded_status_for_high_priority_alert(
    monkeypatch,
    tmp_path: Path,
) -> None:
    status_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        snapshot_module,
        "_fetch_snapshot",
        lambda **_: sample_snapshot(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_effectiveness",
        lambda **_: sample_effectiveness(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_reconciliation",
        lambda **_: {**sample_reconciliation(), "summary": {
            **sample_reconciliation()["summary"],
            "statusCounts": {"aligned": 1},
        }},
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_finance_observations",
        lambda **_: sample_finance(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "write_pipeline_status",
        lambda **kwargs: status_calls.append(kwargs) or kwargs,
    )

    result = snapshot_module.run(
        api_base="http://127.0.0.1:8000/v1",
        out_dir=tmp_path,
        country=None,
        brand=None,
        jato_model=None,
        limit=5,
        threshold_pct=3.0,
        timeout_seconds=1,
    )

    assert result["pipelineStatus"] == "degraded"
    assert status_calls[0]["status"] == "degraded"
    assert status_calls[0]["records_processed"] == 1
    assert status_calls[0]["warning_count"] == 1
    assert status_calls[0]["extra"]["effectivenessSummary"] == {
        "priceEventCount": 1,
        "analyzedEventCount": 1,
        "labelCounts": {"positive": 1},
        "limit": 5,
    }
    assert status_calls[0]["extra"]["reconciliationSummary"] == {
        "observationRows": 4,
        "reconciliationGroupCount": 1,
        "statusCounts": {"aligned": 1},
        "limit": 5,
    }
    assert status_calls[0]["extra"]["financeSummary"]["monthlyPaymentCount"] == 1
    assert result["snapshot"]["priceSalesEffectiveness"]["schemaVersion"] == (
        "msrp_price_sales_effectiveness_v1"
    )
    assert result["snapshot"]["multiSourceReconciliation"]["schemaVersion"] == (
        "msrp_multi_source_reconciliation_v1"
    )
    assert result["snapshot"]["financeObservations"]["total"] == 1


def test_run_degrades_for_reconciliation_conflicts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    status_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        snapshot_module,
        "_fetch_snapshot",
        lambda **_: {
            **sample_snapshot(),
            "summary": {
                **sample_snapshot()["summary"],
                "priceAlertSummary": {
                    "priceChangeEventCount": 0,
                    "thresholdAlertCount": 0,
                    "highPriorityAlertCount": 0,
                    "directionCounts": {},
                    "severityCounts": {},
                },
            },
            "priceAlerts": [],
        },
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_effectiveness",
        lambda **_: sample_effectiveness(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_reconciliation",
        lambda **_: sample_reconciliation(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_finance_observations",
        lambda **_: sample_finance(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "write_pipeline_status",
        lambda **kwargs: status_calls.append(kwargs) or kwargs,
    )

    result = snapshot_module.run(
        api_base="http://127.0.0.1:8000/v1",
        out_dir=tmp_path,
        country=None,
        brand=None,
        jato_model=None,
        limit=5,
        threshold_pct=3.0,
        timeout_seconds=1,
    )

    assert result["pipelineStatus"] == "degraded"
    assert status_calls[0]["warning_count"] == 1
    assert status_calls[0]["extra"]["reconciliationSummary"]["statusCounts"] == {
        "conflict": 1
    }


def test_run_writes_degraded_status_for_empty_current_prices(
    monkeypatch,
    tmp_path: Path,
) -> None:
    status_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        snapshot_module,
        "_fetch_snapshot",
        lambda **_: empty_snapshot(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_effectiveness",
        lambda **_: {
            **sample_effectiveness(),
            "summary": {
                "priceEventCount": 0,
                "analyzedEventCount": 0,
                "labelCounts": {},
                "limit": 5,
            },
            "items": [],
        },
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_reconciliation",
        lambda **_: {
            **sample_reconciliation(),
            "summary": {
                "observationRows": 0,
                "reconciliationGroupCount": 0,
                "statusCounts": {},
                "limit": 5,
            },
            "items": [],
        },
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_finance_observations",
        lambda **_: {**sample_finance(), "rows": 0, "total": 0, "items": []},
    )
    monkeypatch.setattr(
        snapshot_module,
        "write_pipeline_status",
        lambda **kwargs: status_calls.append(kwargs) or kwargs,
    )

    result = snapshot_module.run(
        api_base="http://127.0.0.1:8000/v1",
        out_dir=tmp_path,
        country=None,
        brand=None,
        jato_model=None,
        limit=5,
        threshold_pct=3.0,
        timeout_seconds=1,
    )

    assert result["pipelineStatus"] == "degraded"
    assert result["snapshot"]["warnings"] == ["no_current_prices_available"]
    assert status_calls[0]["status"] == "degraded"
    assert status_calls[0]["records_processed"] == 0
    assert status_calls[0]["warning_count"] == 1
    assert status_calls[0]["extra"]["warnings"] == [
        "no_current_prices_available"
    ]
    markdown = (tmp_path / "msrp_current_price_snapshot.md").read_text()
    assert "## Warnings" in markdown
    assert "- no_current_prices_available" in markdown


def test_run_degrades_when_effectiveness_fetch_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    status_calls: list[dict[str, object]] = []

    def fail_effectiveness(**_: object) -> dict[str, object]:
        raise RuntimeError("effectiveness unavailable")

    monkeypatch.setattr(
        snapshot_module,
        "_fetch_snapshot",
        lambda **_: sample_snapshot(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_effectiveness",
        fail_effectiveness,
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_reconciliation",
        lambda **_: {
            **sample_reconciliation(),
            "summary": {
                "observationRows": 0,
                "reconciliationGroupCount": 0,
                "statusCounts": {},
                "limit": 5,
            },
            "items": [],
        },
    )
    monkeypatch.setattr(
        snapshot_module,
        "_fetch_finance_observations",
        lambda **_: sample_finance(),
    )
    monkeypatch.setattr(
        snapshot_module,
        "write_pipeline_status",
        lambda **kwargs: status_calls.append(kwargs) or kwargs,
    )

    result = snapshot_module.run(
        api_base="http://127.0.0.1:8000/v1",
        out_dir=tmp_path,
        country=None,
        brand=None,
        jato_model=None,
        limit=5,
        threshold_pct=3.0,
        timeout_seconds=1,
    )

    assert result["pipelineStatus"] == "degraded"
    assert result["snapshot"]["warnings"] == [
        "effectiveness_unavailable:RuntimeError"
    ]
    assert status_calls[0]["status"] == "degraded"
    assert status_calls[0]["warning_count"] == 2
    assert status_calls[0]["extra"]["effectivenessSummary"] == {}


def test_main_writes_failed_status_when_snapshot_fetch_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    status_calls: list[dict[str, object]] = []

    def fail_fetch(**_: object) -> dict[str, object]:
        raise RuntimeError("backend unavailable")

    monkeypatch.setattr(snapshot_module, "_fetch_snapshot", fail_fetch)
    monkeypatch.setattr(
        snapshot_module,
        "write_pipeline_status",
        lambda **kwargs: status_calls.append(kwargs) or kwargs,
    )

    exit_code = snapshot_module.main([
        "--api-base",
        "http://127.0.0.1:9/v1",
        "--out-dir",
        str(tmp_path),
    ])

    assert exit_code == 1
    assert status_calls[0]["status"] == "failed"
    assert status_calls[0]["exit_code"] == 1
    assert status_calls[0]["extra"]["errorType"] == "RuntimeError"
    assert set(status_calls[0]["extra"]["artifactRefsByName"]) == {
        "latestJson",
        "latestMarkdown",
        "historicalJson",
        "historicalMarkdown",
    }
    assert len(status_calls[0]["artifact_refs"]) == 4
    snapshot = snapshot_module.json.loads(
        (tmp_path / "msrp_current_price_snapshot.json").read_text(
            encoding="utf-8",
        )
    )
    assert snapshot["summary"]["currentPriceCount"] == 0
    assert snapshot["warnings"] == ["snapshot_fetch_failed:RuntimeError"]
    markdown = (tmp_path / "msrp_current_price_snapshot.md").read_text(
        encoding="utf-8",
    )
    assert "## Warnings" in markdown
    assert "- snapshot_fetch_failed:RuntimeError" in markdown
