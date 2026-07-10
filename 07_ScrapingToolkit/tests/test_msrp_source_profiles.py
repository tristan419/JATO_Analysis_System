from pathlib import Path
import re

from jato_scraper.config_loader import (
    _build_http_text_profile,
    _build_pdf_text_profile,
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


def test_italy_repaired_msrp_profiles_use_official_list_price_lanes() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "it"
    )
    expected = {
        "03_dacia_duster_it.yaml": (
            "https://www.dacia.it/gamma-ibrida-ed-elettrica/duster-suv.html",
            "official_model_page_list_price",
        ),
        "08_renault_captur_it.yaml": (
            "https://www.renault.it/veicoli-ibridi/captur/versioni-e-prezzi.html",
            "official_configurator_model_min_price",
        ),
        "26_suzuki_vitara_it.yaml": (
            "https://auto.suzuki.it/modello/VITARA_HYBRID_2024/listino.aspx",
            "official_price_list_table",
        ),
        "07_toyota_aygo_x_it.yaml": (
            "https://www.toyota.it/gamma/aygo-x",
            "official_sales_hub_list_price",
        ),
        "19_toyota_c_hr_it.yaml": (
            "https://www.toyota.it/gamma/c-hr",
            "official_sales_hub_list_price",
        ),
        "02_toyota_yaris_cross_it.yaml": (
            "https://www.toyota.it/gamma/yaris-cross",
            "official_sales_hub_list_price",
        ),
    }

    for filename, (url, reason_kind) in expected.items():
        source = _load_yaml_mapping(source_root / filename)
        profile = _build_http_text_profile(source["profile"])

        assert source["extractor_type"] == "http_text"
        assert source["source_url"] == url
        assert profile.url == url
        assert profile.default_currency == "EUR"
        assert profile.default_tax_included is True
        assert profile.match_status == "review_required"
        assert profile.match_reason["kind"] == reason_kind
        assert profile.entry_patterns
        for entry in profile.entry_patterns:
            assert re.compile(entry.pattern)


def test_italy_renault_and_toyota_profiles_do_not_extract_discounted_prices() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "it"
    )
    captur = _load_yaml_mapping(source_root / "08_renault_captur_it.yaml")
    aygo = _load_yaml_mapping(source_root / "07_toyota_aygo_x_it.yaml")
    c_hr = _load_yaml_mapping(source_root / "19_toyota_c_hr_it.yaml")
    yaris_cross = _load_yaml_mapping(source_root / "02_toyota_yaris_cross_it.yaml")

    assert "discountedPrice" not in captur["profile"]["entry_patterns"][0]["pattern"]
    for source in (aygo, c_hr, yaris_cross):
        pattern = source["profile"]["entry_patterns"][0]["pattern"]
        assert "listWithDiscount" not in pattern
        assert "price&#34;:\\{&#34;list" in pattern


def test_italy_price_semantic_repairs_do_not_select_monthly_payments() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "it"
    )
    junior = _load_yaml_mapping(source_root / "21_alfa_romeo_junior_it.yaml")
    tucson = _load_yaml_mapping(source_root / "22_hyundai_tucson_it.yaml")

    junior_profile = _build_scrapling_profile(junior["profile"])
    tucson_profile = _build_pdf_text_profile(tucson["profile"])

    assert junior["extractor_type"] == "scrapling"
    assert junior_profile.text_regex is not None
    junior_pattern = junior_profile.text_regex.entry_patterns[0].pattern
    assert "Prezzo di listino" in junior_pattern
    assert "mese" not in junior_pattern
    assert "5\\.000" in junior_pattern

    assert tucson["extractor_type"] == "pdf_text"
    assert tucson_profile.prefer_curl_download is True
    assert tucson_profile.entry_patterns[0].price_label == (
        "Prezzo di listino IVA inclusa, IPT e PFU esclusi"
    )
    assert "chiavi" not in tucson_profile.entry_patterns[0].pattern


def test_italy_ford_profiles_use_current_official_price_list_pdfs() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "it"
    )
    expected = {
        "28_ford_kuga_it.yaml": "https://www.ford.it/content/dam/guxeu/it/documents/pricelists/cars/PL-Kuga_CX482_Pricelist_MY27_2C-2026.pdf",
        "06_ford_puma_it.yaml": "https://www.ford.it/content/dam/guxeu/it/documents/pricelists/cars/PL-ford_NewPuma.pdf",
    }

    for filename, url in expected.items():
        source = _load_yaml_mapping(source_root / filename)
        profile = _build_pdf_text_profile(source["profile"])

        assert source["extractor_type"] == "pdf_text"
        assert source["source_url"] == url
        assert profile.url == url
        assert profile.prefer_curl_download is True
        assert profile.default_tax_included is True
        assert profile.entry_patterns[0].price_label == (
            "Prezzo chiavi in mano IVA inclusa, IPT e PFU esclusi"
        )
