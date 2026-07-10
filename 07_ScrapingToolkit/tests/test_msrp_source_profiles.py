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


def test_volkswagen_germany_model_pages_use_static_official_starting_prices() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "de"
    )
    expected = {
        "01_volkswagen_t_roc_de.yaml": "https://www.volkswagen.de/de/modelle/der-neue-t-roc.html",
        "02_volkswagen_tiguan_de.yaml": "https://www.volkswagen.de/de/modelle/tiguan.html",
        "03_volkswagen_t_cross_de.yaml": "https://www.volkswagen.de/de/modelle/der-t-cross.html",
    }

    for filename, url in expected.items():
        source = _load_yaml_mapping(source_root / filename)
        profile = _build_scrapling_profile(source["profile"])

        assert source["extractor_type"] == "scrapling"
        assert source["source_url"] == url
        assert profile.url == url
        assert profile.tier == "http"
        assert profile.network_idle is False
        assert profile.text_regex is not None
        assert profile.text_regex.include_element_html is True
        assert profile.text_regex.entry_patterns[0].price_label == (
            "Preis ab inkl. MwSt."
        )
