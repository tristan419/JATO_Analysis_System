from pathlib import Path
import re

from jato_scraper.config_loader import (
    _build_http_json_profile,
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


def test_italy_jeep_and_mercedes_profiles_use_model_specific_list_prices() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "it"
    )
    expected = {
        "01_jeep_avenger_it.yaml": (
            "https://www.jeep-official.it/nuova-jeep-avenger/elettrica",
            "official_model_page_list_price",
            "BEV",
        ),
        "23_mercedes_gla_it.yaml": (
            "https://www.mercedes-benz.it/passengercars/models/suv/gla/overview.html",
            "official_model_page_json_ld_offer",
            None,
        ),
    }

    for filename, (url, reason_kind, powertrain) in expected.items():
        source = _load_yaml_mapping(source_root / filename)
        profile = _build_http_text_profile(source["profile"])

        assert source["extractor_type"] == "http_text"
        assert source["source_url"] == url
        assert profile.url == url
        assert profile.default_tax_included is True
        assert profile.match_status == "review_required"
        assert profile.match_reason["kind"] == reason_kind
        assert profile.fixed_jato_powertrain == powertrain
        assert profile.entry_patterns
        assert re.compile(profile.entry_patterns[0].pattern)


def test_italy_skoda_kamiq_profile_uses_the_official_price_list_pdf() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "it"
        / "25_skoda_kamiq_it.yaml"
    )
    source = _load_yaml_mapping(source_path)
    profile = _build_pdf_text_profile(source["profile"])

    assert source["extractor_type"] == "pdf_text"
    assert source["source_url"] == (
        "https://www.skoda-auto.it/_doc/e5e8f12e-7439-4cc7-ae31-c9415350f09d"
    )
    assert profile.prefer_curl_download is True
    assert profile.default_tax_included is True
    assert profile.fixed_jato_powertrain == "ICE"
    assert profile.match_reason["document_valid_from"] == "2026-03-03"
    assert profile.entry_patterns[0].price_label == (
        "Prezzo chiavi in mano, IVA 22% e messa su strada incluse; IPT esclusa"
    )


def test_sweden_skoda_enyaq_profile_excludes_private_lease_context() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "se"
        / "14_skoda_enyaq_se.yaml"
    )
    source = _load_yaml_mapping(source_path)
    profile = _build_scrapling_profile(source["profile"])

    assert source["price_semantics"] == "base_msrp"
    assert profile.text_regex is not None
    assert len(profile.text_regex.entry_patterns) == 1
    assert "Privatleasing" not in profile.text_regex.entry_patterns[0].pattern
    assert "monthly_payment" not in profile.text_regex.entry_patterns[0].pattern
    assert profile.pricing_context is None


def test_hungary_mercedes_eqa_profile_uses_current_official_list_price_pdf() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "hu"
        / "31_mercedes_eqa_hu.yaml"
    )
    source = _load_yaml_mapping(source_path)
    profile = _build_pdf_text_profile(source["profile"])

    assert source["extractor_type"] == "pdf_text"
    assert source["source_type"] == "official_price_list"
    assert source["source_url"] == (
        "https://www.mercedes-benz.hu/passengercars/finance/arlista.html"
    )
    assert profile.url.endswith("Mercedes-Benz_elektromos_modellek_alaparlista_2026.06.09.pdf")
    assert profile.prefer_curl_download is True
    assert profile.default_price_label == "Listaár HUF (Ajánlott fogyasztói ár)"
    assert profile.fixed_jato_powertrain == "BEV"
    assert profile.match_reason["document_valid_from"] == "2026-06-09"
    assert "Havi fizetendő" not in profile.entry_patterns[0].pattern
    assert re.compile(profile.entry_patterns[0].pattern)


def test_hungary_bmw_x5_profile_uses_current_official_compare_price() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "hu"
        / "24_bmw_x5_hu.yaml"
    )
    source = _load_yaml_mapping(source_path)
    profile = _build_http_text_profile(source["profile"])

    assert source["extractor_type"] == "http_text"
    assert source["source_url"].endswith("/X/G65/61JF/S02TE/content.q")
    assert profile.timeout_seconds == 8
    assert profile.default_currency == "HUF"
    assert profile.default_tax_included is True
    assert profile.match_status == "review_required"
    assert profile.match_reason["kind"] == "official_compare_page_starting_price"
    assert profile.entry_patterns[0].official_trim == "X5 40 xDrive starting price"
    assert re.compile(profile.entry_patterns[0].pattern)


def test_hungary_hyundai_tucson_profile_uses_current_official_list_price_pdf() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "hu"
        / "08_hyundai_tucson_hu.yaml"
    )
    source = _load_yaml_mapping(source_path)
    profile = _build_pdf_text_profile(source["profile"])

    assert source["extractor_type"] == "pdf_text"
    assert source["source_type"] == "official_price_list"
    assert profile.url.endswith("TucsonFL_MY27_20260617.pdf")
    assert profile.prefer_curl_download is True
    assert profile.default_tax_included is True
    assert profile.match_reason["document_valid_from"] == "2026-06-17"
    assert profile.default_price_label == "Listaár (ÁFA és regisztrációs adóval)"
    assert len(profile.entry_patterns) == 4
    for entry in profile.entry_patterns:
        assert re.compile(entry.pattern)


def test_hungary_kgm_korando_profile_uses_current_official_list_price_pdf() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "hu"
        / "20_kgm_korando_hu.yaml"
    )
    source = _load_yaml_mapping(source_path)
    profile = _build_pdf_text_profile(source["profile"])

    assert source["extractor_type"] == "pdf_text"
    assert source["source_type"] == "official_price_list"
    assert source["source_url"] == profile.url
    assert profile.prefer_curl_download is True
    assert profile.default_price_label == "Listaár (ÁFA és regisztrációs adóval)"
    assert profile.fixed_jato_powertrain == "ICE"
    assert profile.match_reason["document_valid_from"] == "2026-02-12"
    assert len(profile.entry_patterns) == 4
    for entry in profile.entry_patterns:
        assert "Promóciós ár" not in entry.pattern
        assert re.compile(entry.pattern)


def test_hungary_suzuki_profiles_use_official_configurator_list_prices_only() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "hu"
    )
    expected = {
        "01_suzuki_s_cross_hu.yaml": ("S-CROSS", "R5[67][0-9A-Z]"),
        "02_suzuki_vitara_hu.yaml": ("VITARA", "V6G[0-9A-Z]"),
    }

    for filename, (model, trim_pattern) in expected.items():
        source = _load_yaml_mapping(source_root / filename)
        profile = _build_http_text_profile(source["profile"])
        entry = profile.entry_patterns[0]

        assert source["source_code"].endswith("_draft_scrapling")
        assert source["extractor_type"] == "http_text"
        assert source["source_url"] == "https://konfigurator.suzuki.hu/"
        assert profile.url == source["source_url"]
        assert profile.default_currency == "HUF"
        assert profile.default_tax_included is True
        assert profile.prefer_curl_fetch is True
        assert profile.fixed_model == model
        assert profile.fixed_jato_powertrain == "MHEV"
        assert profile.match_reason["kind"] == "official_configurator_embedded_list_price"
        assert profile.match_reason["price_field"] == "prices.list"
        assert profile.match_reason["excluded_field"] == "prices.discount"
        assert entry.price_label == "Listaár (ÁFA-val)"
        assert trim_pattern in entry.pattern
        assert "&quot;prices&quot;:\\{&quot;list&quot;" in entry.pattern
        assert "discount" not in entry.pattern
        assert re.compile(entry.pattern)


def test_hungary_volkswagen_profiles_use_official_configurator_list_price_api() -> None:
    source_root = (
        Path(__file__).resolve().parents[1]
        / "source_drafts"
        / "suv_only_country_model_top30"
        / "hu"
    )
    expected = {
        "12_volkswagen_t_roc_hu.yaml": ("T-ROC", "042"),
        "16_volkswagen_tiguan_hu.yaml": ("TIGUAN", "192"),
    }
    api_url = (
        "https://cc.porscheinformatik.com/cc-hu/be/hu_HU_VW22/"
        "api/v2/modelgroup?brand=V"
    )

    for filename, (model, group_code) in expected.items():
        source = _load_yaml_mapping(source_root / filename)
        profile = _build_http_json_profile(source["profile"])

        assert source["extractor_type"] == "http_json"
        assert source["source_url"] == api_url
        assert profile.url == api_url.removesuffix("?brand=V")
        assert profile.params == {"brand": "V"}
        assert profile.default_currency == "HUF"
        assert profile.default_tax_included is True
        assert profile.fixed_model == model
        assert profile.field_mapping.vehicles_path == "modelgroups"
        assert profile.field_mapping.items_paths == ("variants", "models")
        assert profile.field_mapping.price == "listPrice.gross"
        assert profile.filters[0].path == "modelGroupNumber"
        assert profile.filters[0].equals == (group_code,)
        assert profile.match_reason["kind"] == "official_configurator_modelgroup_list_price"
        if model == "TIGUAN":
            assert profile.min_price_group is not None
            assert profile.min_price_group.key == "modelGroupNumber"
            assert profile.min_price_group.price == "listPrice.gross"
        else:
            assert profile.min_price_group is None
