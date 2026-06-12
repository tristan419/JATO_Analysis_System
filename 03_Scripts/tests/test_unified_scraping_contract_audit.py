from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "unified_scraping_contract_audit.py"
)


def load_module():
    module_name = "unified_scraping_contract_audit_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


audit_module = load_module()


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_msrp_source(base: Path, country: str, source_code: str) -> None:
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
    country_blocks = []
    for country in countries:
        country_blocks.append(
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
        + "\n".join(country_blocks)
        + "\n",
    )


def _write_voc_batch(path: Path, countries: tuple[str, ...]) -> None:
    country_blocks = []
    for country in countries:
        country_blocks.append(
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
        + "\n".join(country_blocks)
        + "\n",
    )


def _write_domain_batch(path: Path, kind: str, countries: tuple[str, ...]) -> None:
    country_blocks = []
    for country in countries:
        source_code = f"{country}_{kind}_source"
        extractor = "css_rules" if kind == "spec" else "llm_extract"
        fetcher = "scrapling" if kind == "spec" else "requests"
        country_blocks.append(
            f"""
  - country_code: "{country.upper()}"
    country_label: "{country.upper()}"
    sources:
      - source_code: "{source_code}"
        source_name: "Codex {kind.title()} Source"
        url: "https://{kind}.example.{country}/source"
        source_kind: "government"
        topics: ["pricing"]
        fetcher: "{fetcher}"
        extractor: "{extractor}"
        freshness_hours: 168
""".rstrip()
        )
    _write_text(
        path,
        f"batch_code: test_{kind}\n"
        "description: test\n"
        "countries:\n"
        + "\n".join(country_blocks)
        + "\n",
    )


def test_run_audit_reports_ok_for_complete_required_country_contracts(tmp_path: Path) -> None:
    countries = ("se", "fi", "no", "dk")
    msrp_dir = tmp_path / "msrp"
    for country in countries:
        _write_msrp_source(
            msrp_dir,
            country,
            f"codex_model_{country}_draft_scrapling",
        )
    news_batch = tmp_path / "news.yaml"
    voc_batch = tmp_path / "voc.yaml"
    _write_news_batch(news_batch, countries)
    _write_voc_batch(voc_batch, countries)
    policy_batch = tmp_path / "policy.yaml"
    incentive_batch = tmp_path / "incentive.yaml"
    spec_batch = tmp_path / "spec.yaml"
    _write_domain_batch(policy_batch, "policy", countries)
    _write_domain_batch(incentive_batch, "incentive", countries)
    _write_domain_batch(spec_batch, "spec", countries)

    report = audit_module.run_audit(
        repo_root=tmp_path,
        msrp_dir=msrp_dir,
        news_batch=news_batch,
        voc_batch=voc_batch,
        policy_batch=policy_batch,
        incentive_batch=incentive_batch,
        spec_batch=spec_batch,
        required_countries=countries,
    )

    assert report["status"] == "ok"
    assert report["summary"]["totalJobs"] == 24
    assert report["summary"]["jobsByKind"] == {
        "incentive": 4,
        "msrp": 4,
        "news": 4,
        "policy": 4,
        "spec": 4,
        "voc": 4,
    }
    assert report["summary"]["mappingErrorCount"] == 0
    assert report["warnings"] == []
    assert report["summary"]["countryCoverage"]["dk"] == {
        "msrp": 1,
        "news": 1,
        "policy": 1,
        "incentive": 1,
        "spec": 1,
        "voc": 1,
    }


def test_run_audit_allows_kind_specific_country_requirements(tmp_path: Path) -> None:
    base_countries = ("se", "fi", "no", "dk")
    intelligence_countries = ("se", "fi", "no", "dk", "de")
    msrp_dir = tmp_path / "msrp"
    for country in base_countries:
        _write_msrp_source(
            msrp_dir,
            country,
            f"codex_model_{country}_draft_scrapling",
        )
    news_batch = tmp_path / "news.yaml"
    voc_batch = tmp_path / "voc.yaml"
    _write_news_batch(news_batch, intelligence_countries)
    _write_voc_batch(voc_batch, intelligence_countries)
    policy_batch = tmp_path / "policy.yaml"
    incentive_batch = tmp_path / "incentive.yaml"
    spec_batch = tmp_path / "spec.yaml"
    _write_domain_batch(policy_batch, "policy", base_countries)
    _write_domain_batch(incentive_batch, "incentive", base_countries)
    _write_domain_batch(spec_batch, "spec", base_countries)

    report = audit_module.run_audit(
        repo_root=tmp_path,
        msrp_dir=msrp_dir,
        news_batch=news_batch,
        voc_batch=voc_batch,
        policy_batch=policy_batch,
        incentive_batch=incentive_batch,
        spec_batch=spec_batch,
        required_countries=base_countries,
        required_countries_by_kind={
            "news": intelligence_countries,
            "voc": intelligence_countries,
        },
    )

    assert report["status"] == "ok"
    assert report["inputs"]["requiredCountriesByKind"]["msrp"] == list(base_countries)
    assert report["inputs"]["requiredCountriesByKind"]["news"] == list(intelligence_countries)
    assert report["summary"]["countryCoverage"]["de"]["news"] == 1
    assert report["summary"]["countryCoverage"]["de"]["voc"] == 1
    assert report["summary"]["countryCoverage"]["de"]["policy"] == 0


def test_run_audit_reports_kind_specific_missing_coverage(tmp_path: Path) -> None:
    base_countries = ("se",)
    msrp_dir = tmp_path / "msrp"
    _write_msrp_source(msrp_dir, "se", "codex_model_se_draft_scrapling")
    news_batch = tmp_path / "news.yaml"
    voc_batch = tmp_path / "voc.yaml"
    _write_news_batch(news_batch, ("se", "de"))
    _write_voc_batch(voc_batch, ("se",))
    policy_batch = tmp_path / "policy.yaml"
    incentive_batch = tmp_path / "incentive.yaml"
    spec_batch = tmp_path / "spec.yaml"
    _write_domain_batch(policy_batch, "policy", base_countries)
    _write_domain_batch(incentive_batch, "incentive", base_countries)
    _write_domain_batch(spec_batch, "spec", base_countries)

    report = audit_module.run_audit(
        repo_root=tmp_path,
        msrp_dir=msrp_dir,
        news_batch=news_batch,
        voc_batch=voc_batch,
        policy_batch=policy_batch,
        incentive_batch=incentive_batch,
        spec_batch=spec_batch,
        required_countries=base_countries,
        required_countries_by_kind={
            "news": ("se", "de"),
            "voc": ("se", "de"),
        },
    )

    assert report["status"] == "degraded"
    assert "missing_news_coverage:de" not in report["warnings"]
    assert "missing_voc_coverage:de" in report["warnings"]


def test_run_audit_degrades_for_mapping_errors_and_missing_coverage(tmp_path: Path) -> None:
    msrp_dir = tmp_path / "msrp"
    _write_msrp_source(msrp_dir, "se", "codex_model_se_draft_scrapling")
    _write_text(
        msrp_dir / "dk" / "broken.yaml",
        """
source_code: "codex_model_dk_draft_selenium"
country: "DK"
brand: "CODEX"
source_url: "https://example.dk/prices"
extractor_type: "selenium"
profile:
  url: "https://example.dk/prices"
""".strip(),
    )
    news_batch = tmp_path / "news.yaml"
    voc_batch = tmp_path / "voc.yaml"
    _write_news_batch(news_batch, ("se", "dk"))
    _write_voc_batch(voc_batch, ("se", "dk"))
    policy_batch = tmp_path / "policy.yaml"
    incentive_batch = tmp_path / "incentive.yaml"
    spec_batch = tmp_path / "spec.yaml"
    _write_domain_batch(policy_batch, "policy", ("se", "dk"))
    _write_domain_batch(incentive_batch, "incentive", ("se", "dk"))
    _write_domain_batch(spec_batch, "spec", ("se", "dk"))

    report = audit_module.run_audit(
        repo_root=tmp_path,
        msrp_dir=msrp_dir,
        news_batch=news_batch,
        voc_batch=voc_batch,
        policy_batch=policy_batch,
        incentive_batch=incentive_batch,
        spec_batch=spec_batch,
        required_countries=("se", "dk"),
    )

    assert report["status"] == "degraded"
    assert report["summary"]["mappingErrorCount"] == 1
    assert report["mappingErrors"][0]["kind"] == "msrp"
    assert "unsupported MSRP extractor_type" in report["mappingErrors"][0]["error"]
    assert "missing_msrp_coverage:dk" in report["warnings"]


def test_main_prints_json_report(capsys, tmp_path: Path) -> None:
    countries = ("se",)
    msrp_dir = tmp_path / "msrp"
    _write_msrp_source(msrp_dir, "se", "codex_model_se_draft_scrapling")
    news_batch = tmp_path / "news.yaml"
    voc_batch = tmp_path / "voc.yaml"
    _write_news_batch(news_batch, countries)
    _write_voc_batch(voc_batch, countries)
    policy_batch = tmp_path / "policy.yaml"
    incentive_batch = tmp_path / "incentive.yaml"
    spec_batch = tmp_path / "spec.yaml"
    _write_domain_batch(policy_batch, "policy", countries)
    _write_domain_batch(incentive_batch, "incentive", countries)
    _write_domain_batch(spec_batch, "spec", countries)

    exit_code = audit_module.main(
        [
            "--repo-root",
            str(tmp_path),
            "--msrp-dir",
            str(msrp_dir),
            "--news-batch",
            str(news_batch),
            "--voc-batch",
            str(voc_batch),
            "--policy-batch",
            str(policy_batch),
            "--incentive-batch",
            str(incentive_batch),
            "--spec-batch",
            str(spec_batch),
            "--required-countries",
            "se",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["schemaVersion"] == audit_module.SCHEMA_VERSION
    assert payload["status"] == "ok"
