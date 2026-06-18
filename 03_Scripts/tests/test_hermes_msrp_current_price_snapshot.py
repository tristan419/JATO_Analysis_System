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
        },
        "priceSalesEffectiveness": sample_effectiveness(),
    })

    assert "| Current prices | 1 |" in markdown
    assert "Volvo\\|Polestar" in markdown
    assert "Plus Dark" in markdown
    assert "## Sales Effectiveness" in markdown
    assert "| positive | 1 |" in markdown
    assert "| SE | Volvo | XC60 | 2026-03 | down | 18.5 | positive |" in markdown


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
    assert result["snapshot"]["priceSalesEffectiveness"]["schemaVersion"] == (
        "msrp_price_sales_effectiveness_v1"
    )


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
    assert status_calls[0]["extra"] == {"errorType": "RuntimeError"}
