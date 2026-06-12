from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "unified_scraping_readiness_audit.py"
)


def load_module():
    module_name = "unified_scraping_readiness_audit_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


readiness = load_module()


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_msrp_source(base: Path, country: str) -> None:
    source_code = f"codex_model_{country}_draft_scrapling"
    _write_text(
        base / country / f"{source_code}.yaml",
        f"""
source_code: "{source_code}"
country: "{country.upper()}"
brand: "CODEX"
source_url: "https://example.{country}/prices"
extractor_type: "scrapling"
profile:
  url: "https://example.{country}/prices"
  tier: "requests"
  css:
    vehicle_container: ".vehicle"
schedule:
  frequency: "weekly"
""".strip(),
    )


def _write_news_batch(path: Path, countries: tuple[str, ...]) -> None:
    blocks = []
    for country in countries:
        blocks.append(
            f"""
  - country_code: "{country.upper()}"
    country_label: "{country.upper()}"
    feeds:
      - source_code: "{country}_news"
        publisher: "Codex News"
        feed_url: "https://news.example.{country}/feed.xml"
        language: "en"
        tags: [market]
""".rstrip()
        )
    _write_text(
        path,
        "batch_code: test_news\n"
        "description: test\n"
        "countries:\n"
        + "\n".join(blocks)
        + "\n",
    )


def _write_voc_batch(path: Path, countries: tuple[str, ...]) -> None:
    blocks = []
    for country in countries:
        blocks.append(
            f"""
  - country_code: "{country.upper()}"
    country_label: "{country.upper()}"
    languages: [en]
    taxonomy_profile: "default"
    sources:
      - source_code: "{country}_forum"
        site_name: "Codex Forum"
        site_url: "https://forum.example.{country}"
        site_type: "forum"
        extractor: "scrapling"
        language: "en"
        tags: [owner]
""".rstrip()
        )
    _write_text(
        path,
        "batch_code: test_voc\n"
        "description: test\n"
        "countries:\n"
        + "\n".join(blocks)
        + "\n",
    )


def _write_domain_batch(path: Path, kind: str, countries: tuple[str, ...]) -> None:
    blocks = []
    for country in countries:
        extractor = "css_rules" if kind == "spec" else "llm_extract"
        fetcher = "scrapling" if kind == "spec" else "requests"
        blocks.append(
            f"""
  - country_code: "{country.upper()}"
    country_label: "{country.upper()}"
    sources:
      - source_code: "{country}_{kind}_source"
        source_name: "Codex {kind.title()} Source"
        url: "https://{kind}.example.{country}/source"
        source_kind: "government"
        topics: ["pricing"]
        fetcher: "{fetcher}"
        extractor: "{extractor}"
        freshness_hours: 168
        brand: "CODEX"
        model: "MODEL"
""".rstrip()
        )
    _write_text(
        path,
        f"batch_code: test_{kind}\n"
        "description: test\n"
        "countries:\n"
        + "\n".join(blocks)
        + "\n",
    )


def _write_all_inputs(tmp_path: Path, countries: tuple[str, ...] = ("se", "fi")) -> dict[str, Path]:
    msrp_dir = tmp_path / "msrp"
    for country in countries:
        _write_msrp_source(msrp_dir, country)
    paths = {
        "msrp_dir": msrp_dir,
        "news_batch": tmp_path / "news.yaml",
        "voc_batch": tmp_path / "voc.yaml",
        "policy_batch": tmp_path / "policy.yaml",
        "incentive_batch": tmp_path / "incentive.yaml",
        "spec_batch": tmp_path / "spec.yaml",
    }
    _write_news_batch(paths["news_batch"], countries)
    _write_voc_batch(paths["voc_batch"], countries)
    _write_domain_batch(paths["policy_batch"], "policy", countries)
    _write_domain_batch(paths["incentive_batch"], "incentive", countries)
    _write_domain_batch(paths["spec_batch"], "spec", countries)
    return paths


def _report_kwargs(tmp_path: Path, inputs: dict[str, Path]) -> dict[str, object]:
    return {
        "repo_root": tmp_path,
        "artifact_root": tmp_path / "stage-artifacts",
        "msrp_dir": inputs["msrp_dir"],
        "news_batch": inputs["news_batch"],
        "voc_batch": inputs["voc_batch"],
        "policy_batch": inputs["policy_batch"],
        "incentive_batch": inputs["incentive_batch"],
        "spec_batch": inputs["spec_batch"],
        "required_countries": ("se", "fi"),
        "required_kinds": ("msrp", "news", "voc", "policy", "incentive", "spec"),
    }


def test_build_readiness_report_passes_when_contract_and_stage_are_ok(tmp_path: Path) -> None:
    inputs = _write_all_inputs(tmp_path)

    report = readiness.build_readiness_report(**_report_kwargs(tmp_path, inputs))

    assert report["schemaVersion"] == readiness.SCHEMA_VERSION
    assert report["status"] == "passed"
    assert report["summary"]["contractStatus"] == "ok"
    assert report["summary"]["stageStatus"] == "ok"
    assert report["summary"]["configuredJobCount"] == 12
    assert report["summary"]["sampledJobCount"] == 6
    assert report["summary"]["warningCount"] == 0
    assert report["warnings"] == []


def test_build_readiness_report_degrades_on_missing_kind_country_coverage(tmp_path: Path) -> None:
    inputs = _write_all_inputs(tmp_path, countries=("se",))
    kwargs = _report_kwargs(tmp_path, inputs)
    kwargs["required_countries"] = ("se",)
    kwargs["required_countries_by_kind"] = {"news": ("se", "de")}

    report = readiness.build_readiness_report(**kwargs)

    assert report["status"] == "degraded"
    assert report["summary"]["contractStatus"] == "degraded"
    assert report["summary"]["stageStatus"] == "ok"
    assert "contract:missing_news_coverage:de" in report["warnings"]


def test_write_outputs_creates_latest_and_historical_artifacts(tmp_path: Path) -> None:
    inputs = _write_all_inputs(tmp_path)
    report = readiness.build_readiness_report(**_report_kwargs(tmp_path, inputs))

    artifacts = readiness.write_outputs(report, tmp_path / "reports")

    assert Path(artifacts["latestJson"]).exists()
    assert Path(artifacts["latestMarkdown"]).exists()
    assert Path(artifacts["historicalJson"]).exists()
    assert Path(artifacts["historicalMarkdown"]).exists()
    payload = json.loads(Path(artifacts["latestJson"]).read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert "Unified Scraping Readiness" in Path(
        artifacts["latestMarkdown"]
    ).read_text(encoding="utf-8")


def test_write_status_record_maps_readiness_to_pipeline_status(monkeypatch, tmp_path: Path) -> None:
    inputs = _write_all_inputs(tmp_path)
    report = readiness.build_readiness_report(**_report_kwargs(tmp_path, inputs))
    captured: dict[str, object] = {}

    def fake_write_pipeline_status(**kwargs):
        captured.update(kwargs)
        return {"pipelineId": kwargs["pipeline_id"], "status": kwargs["status"]}

    monkeypatch.setattr(readiness, "write_pipeline_status", fake_write_pipeline_status)

    status_record = readiness.write_status_record(
        report,
        started_at="2026-06-12T00:00:00Z",
        artifact_refs=["hermes/reports/unified_scraping_readiness.json"],
    )

    assert status_record == {
        "pipelineId": readiness.PIPELINE_ID,
        "status": "success",
    }
    assert captured["records_processed"] == 12
    assert captured["warning_count"] == 0
    assert captured["artifact_refs"] == [
        "hermes/reports/unified_scraping_readiness.json"
    ]


def test_main_prints_report_and_status_record(monkeypatch, capsys, tmp_path: Path) -> None:
    inputs = _write_all_inputs(tmp_path)

    def fake_write_pipeline_status(**kwargs):
        return {"pipelineId": kwargs["pipeline_id"], "status": kwargs["status"]}

    monkeypatch.setattr(readiness, "write_pipeline_status", fake_write_pipeline_status)

    exit_code = readiness.main(
        [
            "--repo-root",
            str(tmp_path),
            "--artifact-root",
            str(tmp_path / "stage-artifacts"),
            "--out-dir",
            str(tmp_path / "reports"),
            "--write-status",
            "--strict",
            "--msrp-dir",
            str(inputs["msrp_dir"]),
            "--news-batch",
            str(inputs["news_batch"]),
            "--voc-batch",
            str(inputs["voc_batch"]),
            "--policy-batch",
            str(inputs["policy_batch"]),
            "--incentive-batch",
            str(inputs["incentive_batch"]),
            "--spec-batch",
            str(inputs["spec_batch"]),
            "--required-countries",
            "se,fi",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["pipelineStatus"] == {
        "pipelineId": readiness.PIPELINE_ID,
        "status": "success",
    }
    assert Path(payload["reportArtifacts"]["latestJson"]).exists()
