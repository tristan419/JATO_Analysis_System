from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "unified_scraping_stage_smoke.py"
)


def load_module():
    module_name = "unified_scraping_stage_smoke_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


stage_smoke = load_module()


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


def _write_news_batch(path: Path, country: str) -> None:
    _write_text(
        path,
        f"""
batch_code: "test_news"
description: "test"
countries:
  - country_code: "{country.upper()}"
    country_label: "{country.upper()}"
    feeds:
      - source_code: "{country}_news"
        publisher: "Codex News"
        feed_url: "https://news.example.{country}/feed.xml"
        language: "en"
""".strip(),
    )


def _write_voc_batch(path: Path, country: str) -> None:
    _write_text(
        path,
        f"""
batch_code: "test_voc"
description: "test"
countries:
  - country_code: "{country.upper()}"
    country_label: "{country.upper()}"
    languages: ["en"]
    taxonomy_profile: "default"
    sources:
      - source_code: "{country}_forum"
        site_name: "Codex Forum"
        site_url: "https://forum.example.{country}"
        site_type: "forum"
        extractor: "scrapling"
        language: "en"
""".strip(),
    )


def _write_domain_batch(path: Path, kind: str, country: str) -> None:
    extractor = "css_rules" if kind == "spec" else "llm_extract"
    fetcher = "scrapling" if kind == "spec" else "requests"
    _write_text(
        path,
        f"""
batch_code: "test_{kind}"
description: "test"
countries:
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
        brand: "CODEX"
        model: "MODEL"
""".strip(),
    )


def _write_all_inputs(tmp_path: Path, country: str = "se") -> dict[str, Path]:
    msrp_dir = tmp_path / "msrp"
    _write_msrp_source(msrp_dir, country)
    paths = {
        "msrp_dir": msrp_dir,
        "news_batch": tmp_path / "news.yaml",
        "voc_batch": tmp_path / "voc.yaml",
        "policy_batch": tmp_path / "policy.yaml",
        "incentive_batch": tmp_path / "incentive.yaml",
        "spec_batch": tmp_path / "spec.yaml",
    }
    _write_news_batch(paths["news_batch"], country)
    _write_voc_batch(paths["voc_batch"], country)
    _write_domain_batch(paths["policy_batch"], "policy", country)
    _write_domain_batch(paths["incentive_batch"], "incentive", country)
    _write_domain_batch(paths["spec_batch"], "spec", country)
    return paths


def test_run_stage_smoke_samples_all_required_kinds(tmp_path: Path) -> None:
    inputs = _write_all_inputs(tmp_path)

    report = stage_smoke.run_stage_smoke(
        repo_root=tmp_path,
        artifact_root=tmp_path / "artifacts",
        msrp_dir=inputs["msrp_dir"],
        news_batch=inputs["news_batch"],
        voc_batch=inputs["voc_batch"],
        policy_batch=inputs["policy_batch"],
        incentive_batch=inputs["incentive_batch"],
        spec_batch=inputs["spec_batch"],
        required_kinds=("msrp", "news", "voc", "policy", "incentive", "spec"),
    )

    assert report["status"] == "ok"
    assert report["summary"]["sampledByKind"] == {
        "incentive": 1,
        "msrp": 1,
        "news": 1,
        "policy": 1,
        "spec": 1,
        "voc": 1,
    }
    assert report["summary"]["failedStageCount"] == 0
    assert report["warnings"] == []
    assert all(
        Path(item["sink"]["artifact_refs"][0]).exists()
        for item in report["stageResults"]
    )


def test_main_prints_stage_smoke_json(capsys, tmp_path: Path) -> None:
    inputs = _write_all_inputs(tmp_path)

    exit_code = stage_smoke.main(
        [
            "--repo-root",
            str(tmp_path),
            "--artifact-root",
            str(tmp_path / "artifacts"),
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
            "--required-kinds",
            "msrp,news,voc,policy,incentive,spec",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["schemaVersion"] == stage_smoke.SCHEMA_VERSION
    assert payload["status"] == "ok"
