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
    assert batch.countries[0].sources[0].source_code == "se_voc_public_seed"


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
    assert payload["source_count"] == 2
    assert payload["countries"][0]["country_code"] == "FI"
    assert payload["countries"][0]["raw_output_path"].endswith("/fi/raw")
    assert payload["countries"][1]["country_code"] == "NO"
