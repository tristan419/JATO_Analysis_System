from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "ai_intelligence_enrichment_smoke.py"
)


def load_module():
    module_name = "ai_intelligence_enrichment_smoke_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


smoke_module = load_module()


def test_run_smoke_verifies_15_country_news_and_voc_enrichment(tmp_path: Path) -> None:
    report = smoke_module.run_smoke(
        artifact_root=tmp_path,
        required_countries=smoke_module.DEFAULT_REQUIRED_COUNTRIES,
    )

    assert report["schemaVersion"] == smoke_module.SCHEMA_VERSION
    assert report["status"] == "ok"
    assert report["warnings"] == []
    assert report["summary"]["requiredCountryCount"] == 15
    assert report["summary"]["news"]["countryCount"] == 15
    assert report["summary"]["news"]["marketEventCount"] == 15
    assert report["summary"]["news"]["missingDigestCountries"] == []
    assert report["summary"]["news"]["missingEvidenceCountries"] == []
    assert report["summary"]["voc"]["countryCount"] == 15
    assert report["summary"]["voc"]["documentCount"] == 30
    assert report["summary"]["voc"]["signalObservationCount"] > 0
    assert report["summary"]["voc"]["missingEvidenceCountries"] == []
    assert report["summary"]["voc"]["missingPainPointCountries"] == []
    assert report["summary"]["voc"]["missingSentimentCountries"] == []
    assert Path(report["artifacts"]["newsEnrichment"]).exists()
    assert Path(report["artifacts"]["vocRoot"]).exists()
    assert report["artifacts"]["vocCollectionSummary"]["country_count"] == 15
    assert all(
        country["weeklyDigestReady"]
        for country in report["countries"]["news"]
    )
    assert all(
        int(country["evidenceCardCount"]) > 0
        for country in report["countries"]["voc"]
    )


def test_run_smoke_degrades_when_required_country_is_missing(
    tmp_path: Path,
) -> None:
    report = smoke_module.run_smoke(
        artifact_root=tmp_path,
        required_countries=("SE", "XX"),
        write_voc_collection=False,
    )

    assert report["status"] == "degraded"
    assert "news_country_missing:xx" in report["warnings"]
    assert "voc_country_missing:xx" in report["warnings"]


def test_main_prints_json_report(capsys, tmp_path: Path) -> None:
    exit_code = smoke_module.main(
        [
            "--artifact-root",
            str(tmp_path),
            "--required-countries",
            "se,fi,no,dk",
            "--strict",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["summary"]["requiredCountryCount"] == 4
    assert payload["summary"]["news"]["countryCount"] == 4
    assert payload["summary"]["voc"]["countryCount"] == 4


def test_write_outputs_creates_latest_and_historical_reports(tmp_path: Path) -> None:
    report = smoke_module.run_smoke(
        artifact_root=tmp_path / "fixtures",
        required_countries=("SE", "FI"),
    )

    artifacts = smoke_module.write_outputs(report, tmp_path / "reports")

    assert artifacts["latestJson"] == str(tmp_path / "reports" / "ai_intelligence_enrichment_smoke.json")
    assert artifacts["latestMarkdown"] == str(tmp_path / "reports" / "ai_intelligence_enrichment_smoke.md")
    assert (tmp_path / "reports" / "ai_intelligence_enrichment_smoke.json").exists()
    assert (tmp_path / "reports" / "ai_intelligence_enrichment_smoke.md").exists()
    assert len(list((tmp_path / "reports").glob("ai_intelligence_enrichment_smoke_*.json"))) == 1
    assert len(list((tmp_path / "reports").glob("ai_intelligence_enrichment_smoke_*.md"))) == 1
    markdown = (tmp_path / "reports" / "ai_intelligence_enrichment_smoke.md").read_text(encoding="utf-8")
    assert "# AI News & VOC Enrichment Smoke" in markdown
    assert "| News | 2 | 2 | - | 0 |" in markdown


def test_write_status_record_maps_smoke_status(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        smoke_module,
        "write_pipeline_status",
        lambda **kwargs: calls.append(kwargs) or kwargs,
    )
    report = smoke_module.run_smoke(
        artifact_root=tmp_path / "fixtures",
        required_countries=("SE", "XX"),
        write_voc_collection=False,
    )

    status_record = smoke_module.write_status_record(
        report,
        artifact_refs=["hermes/reports/ai_intelligence_enrichment_smoke.json"],
    )

    assert status_record["pipeline_id"] == "ai_intelligence_enrichment_smoke" or status_record["pipelineId"] == "ai_intelligence_enrichment_smoke"
    assert calls[0]["status"] == "degraded"
    assert calls[0]["warning_count"] >= 1
    assert calls[0]["artifact_refs"] == ["hermes/reports/ai_intelligence_enrichment_smoke.json"]


def test_write_domain_status_records_marks_news_and_voc_as_derived(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        smoke_module,
        "write_pipeline_status",
        lambda **kwargs: calls.append(kwargs) or {
            "pipelineId": kwargs["pipeline_id"],
            "status": kwargs["status"],
            **(kwargs.get("extra") or {}),
        },
    )
    report = smoke_module.run_smoke(
        artifact_root=tmp_path / "fixtures",
        required_countries=("SE", "FI"),
    )

    records = smoke_module.write_domain_status_records(
        report,
        artifact_refs=["hermes/reports/ai_intelligence_enrichment_smoke.json"],
    )

    assert [call["pipeline_id"] for call in calls] == ["country_news_sync", "voc_forum_sync"]
    assert [record["pipelineId"] for record in records] == ["country_news_sync", "voc_forum_sync"]
    assert all(call["status"] == "success" for call in calls)
    assert all(call["source"] == "ai_intelligence_enrichment_smoke" for call in calls)
    assert all(call["extra"]["derivedFrom"] == "ai_intelligence_enrichment_smoke" for call in calls)
    assert calls[0]["records_processed"] == 2
    assert calls[1]["records_processed"] == 4


def test_main_can_write_report_and_status(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        smoke_module,
        "write_pipeline_status",
        lambda **kwargs: {
            "pipelineId": kwargs["pipeline_id"],
            "status": kwargs["status"],
            **(kwargs.get("extra") or {}),
        },
    )

    exit_code = smoke_module.main(
        [
            "--artifact-root",
            str(tmp_path / "fixtures"),
            "--out-dir",
            str(tmp_path / "reports"),
            "--required-countries",
            "se,fi",
            "--write-status",
            "--strict",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["reportArtifacts"]["latestJson"].endswith("ai_intelligence_enrichment_smoke.json")
    assert payload["pipelineStatus"]["pipelineId"] == "ai_intelligence_enrichment_smoke"
    assert [item["pipelineId"] for item in payload["pipelineStatuses"]] == ["country_news_sync", "voc_forum_sync"]
    assert all(
        item["derivedFrom"] == "ai_intelligence_enrichment_smoke"
        for item in payload["pipelineStatuses"]
    )
