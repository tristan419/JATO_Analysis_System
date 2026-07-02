from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "engineering_config_source_sync.py"


def load_module():
    module_name = "engineering_config_source_sync_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


sync = load_module()


def _write_spec_batch(path: Path, countries: tuple[str, ...] = ("se", "fi")) -> None:
    blocks: list[str] = []
    for country in countries:
        upper = country.upper()
        blocks.append(
            f"""
  - country_code: "{upper}"
    country_label: "{upper}"
    sources:
      - source_code: "{country}_codex_specs"
        source_name: "Codex {upper} official specs"
        url: "https://spec.example.{country}/model"
        source_kind: "manufacturer_official"
        brand: "CODEX"
        model: "MODEL"
        topics: ["trim_features", "equipment"]
        fetcher: "scrapling"
        extractor: "css_rules"
        freshness_hours: 168
        priority: 75
""".rstrip()
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "batch_code: test_spec\n"
        "description: test\n"
        "countries:\n"
        + "\n".join(blocks)
        + "\n",
        encoding="utf-8",
    )


def test_build_source_sync_report_maps_spec_sources_to_engineering_config_contract(
    tmp_path: Path,
) -> None:
    spec_batch = tmp_path / "spec.yaml"
    _write_spec_batch(spec_batch, ("se", "fi", "no", "dk"))

    report = sync.build_source_sync_report(
        repo_root=tmp_path,
        spec_batch=spec_batch,
        artifact_root=tmp_path / "stage",
    )

    assert report["schemaVersion"] == "engineering_config_source_sync_v1"
    assert report["status"] == "passed"
    assert report["summary"]["sourceCount"] == 4
    assert report["summary"]["countries"] == ["dk", "fi", "no", "se"]
    assert report["summary"]["schemaRefs"] == {"SpecFeatureObservation": 4}
    assert report["summary"]["stageSampledCount"] == 4
    assert report["summary"]["warehouseLandingTrimRows"] == 4
    assert report["summary"]["warehouseLandingFeatureRows"] == 4
    assert report["stageSmoke"]["status"] == "ok"
    landing = report["warehouseLanding"]
    assert landing["schemaVersion"] == "engineering_config_landing_v1"
    assert landing["adapter"] == "spec_feature_observation_to_engineering_config_landing_v1"
    assert landing["importBatch"]["domain"] == "engineering_config"
    assert landing["importBatch"]["sourceFileName"] == "spec.yaml"
    assert landing["vehicleTrims"][0]["brand"] == "CODEX"
    assert landing["vehicleTrims"][0]["market"] == "DK"
    assert landing["trimFeatureValues"][0]["featureCode"] == "fixture_standard_equipment"
    assert landing["trimFeatureValues"][0]["availability"] == "standard"
    assert report["warehouseContract"]["tables"] == [
        "ops.import_batches",
        "engineering_config.vehicle_trims",
        "engineering_config.trim_feature_values",
        "engineering_config.config_versions",
    ]
    assert (
        report["warehouseContract"]["landingAdapter"]
        == "spec_feature_observation_to_engineering_config_landing_v1"
    )


def test_build_source_sync_report_degrades_when_required_country_is_missing(
    tmp_path: Path,
) -> None:
    spec_batch = tmp_path / "spec.yaml"
    _write_spec_batch(spec_batch, ("se",))

    report = sync.build_source_sync_report(
        repo_root=tmp_path,
        spec_batch=spec_batch,
        required_countries=("se", "fi"),
        run_stage_smoke=False,
    )

    assert report["status"] == "degraded"
    assert report["summary"]["missingRequiredCountries"] == ["fi"]
    assert "missing_spec_source_country:fi" in report["warnings"]
    assert report["stageSmoke"]["status"] == "not_run"


def test_write_outputs_and_status_record(monkeypatch, tmp_path: Path) -> None:
    spec_batch = tmp_path / "spec.yaml"
    _write_spec_batch(spec_batch, ("se", "fi"))
    report = sync.build_source_sync_report(
        repo_root=tmp_path,
        spec_batch=spec_batch,
        required_countries=("se", "fi"),
        artifact_root=tmp_path / "stage",
    )
    captured: dict[str, object] = {}

    def fake_write_pipeline_status(**kwargs):
        captured.update(kwargs)
        return {"pipelineId": kwargs["pipeline_id"], "status": kwargs["status"]}

    monkeypatch.setattr(sync, "write_pipeline_status", fake_write_pipeline_status)
    artifacts = sync.write_outputs(report, tmp_path / "reports")
    status_record = sync.write_status_record(
        report,
        started_at="2026-06-17T00:00:00Z",
        artifact_refs=list(artifacts.values()),
    )

    assert Path(artifacts["latestJson"]).exists()
    payload = json.loads(Path(artifacts["latestJson"]).read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert "Engineering Config Source Sync" in Path(
        artifacts["latestMarkdown"]
    ).read_text(encoding="utf-8")
    assert status_record == {
        "pipelineId": sync.PIPELINE_ID,
        "status": "success",
    }
    assert captured["records_processed"] == 2
    assert captured["artifact_refs"] == list(artifacts.values())
