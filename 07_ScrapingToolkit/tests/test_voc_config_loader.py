from pathlib import Path

from jato_scraper.voc_config_loader import load_voc_batch_config
from jato_scraper.voc_runner import build_voc_collection_plan


def test_load_voc_batch_config_reads_country_files() -> None:
    batch = load_voc_batch_config(
        Path(__file__).resolve().parents[1] / "voc_sources" / "batch_a.yaml"
    )

    assert batch.batch_code == "country_voc_batch_a"
    assert len(batch.countries) == 8
    assert batch.countries[0].country_code == "SE"
    assert len(batch.countries[0].sources) == 3
    assert batch.countries[0].sources[0].source_code == "se_teslaclubsweden_forum"
    assert batch.countries[0].sources[1].site_type == "forum"
    assert batch.countries[0].sources[2].site_type == "media_comments"
    norway = next(country for country in batch.countries if country.country_code == "NO")
    assert norway.sources[0].language == "no"


def test_build_voc_collection_plan_filters_countries() -> None:
    batch = load_voc_batch_config(
        Path(__file__).resolve().parents[1] / "voc_sources" / "batch_a.yaml"
    )

    payload = build_voc_collection_plan(
        batch,
        country_filter={"FI", "NO"},
        output_root="04_Processed_data/voc",
    )

    assert payload["country_count"] == 2
    assert payload["source_count"] == 6
    assert payload["countries"][0]["country_code"] == "FI"
    assert payload["countries"][0]["raw_output_path"].endswith("/fi/raw")
    assert len(payload["countries"][0]["sources"]) == 3
    assert payload["countries"][0]["taxonomy"]["profile"] == "nordic_core"
    assert "painPoints" in payload["countries"][0]["taxonomy"]
    assert payload["countries"][0]["taxonomy"]["themeTags"]
    assert payload["countries"][0]["taxonomy"]["personaCohorts"]
    assert payload["countries"][0]["taxonomy"]["productCatalog"]
    assert payload["countries"][0]["taxonomy"]["crossAnalysisAxes"][0]["key"] == "product_vs_pain_point"
    assert any(
        item["key"] == "attribute_affinity"
        for item in payload["countries"][0]["taxonomy"]["crossAnalysisAxes"]
    )
    assert (
        payload["countries"][0]["sources"][0]["collection_strategy"]["primaryUnit"]
        == "discussion_thread"
    )
    assert payload["countries"][1]["country_code"] == "NO"
    assert (
        payload["countries"][1]["sources"][1]["collection_strategy"]["primaryUnit"]
        == "article_comment_page"
    )


def test_load_voc_batch_config_rejects_boolean_like_language(tmp_path: Path) -> None:
    country_path = tmp_path / "country.yaml"
    country_path.write_text(
        """
country_code: SE
country_label: Sweden
languages: [sv, en]
taxonomy_profile: nordic_core
sources:
  - source_code: se_demo
    site_name: Demo
    site_url: https://example.com
    site_type: forum
    extractor: scrapling
    language: no
""".strip(),
        encoding="utf-8",
    )

    batch_path = tmp_path / "batch.yaml"
    batch_path.write_text(
        f"""
batch_code: demo_batch
description: Demo
countries:
  - country_file: {country_path.name}
""".strip(),
        encoding="utf-8",
    )

    try:
        load_voc_batch_config(batch_path)
    except ValueError as exc:
        assert "language must be quoted text" in str(exc)
    else:
        raise AssertionError("Expected boolean-like language to be rejected")
