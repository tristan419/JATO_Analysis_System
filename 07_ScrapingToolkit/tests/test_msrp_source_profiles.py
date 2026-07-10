from pathlib import Path

from jato_scraper.config_loader import (
    _build_scrapling_profile,
    _load_yaml_mapping,
)


def test_nissan_italy_qashqai_profile_avoids_network_idle_timeout() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "it"
        / "16_nissan_qashqai_it.yaml"
    )
    source = _load_yaml_mapping(source_path)

    profile = _build_scrapling_profile(source["profile"])

    assert profile.url == "https://www.nissan.it/cf/veicoli/veicoli-nuovi/qashqai"
    assert profile.network_idle is False
    assert profile.load_dom is False
    assert profile.timeout_ms == 45_000
    assert profile.wait_ms == 7_000
