from pathlib import Path
import re
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.config_loader import (
    _build_http_json_profile,
    _build_http_text_profile,
    _build_pdf_text_profile,
    _build_scrapling_profile,
)


HU_SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
    / "hu"
)

PROFILE_BUILDERS = {
    "http_json": _build_http_json_profile,
    "http_text": _build_http_text_profile,
    "pdf_text": _build_pdf_text_profile,
    "scrapling": _build_scrapling_profile,
}

REPAIRED_HU_PROFILES = (
    ("01_suzuki_s_cross_hu.yaml", "http_text", "suzuki.hu"),
    ("02_suzuki_vitara_hu.yaml", "http_text", "suzuki.hu"),
    ("07_kia_sportage_hu.yaml", "pdf_text", "kia.com"),
    ("08_hyundai_tucson_hu.yaml", "pdf_text", "hyundai.hu"),
    ("12_volkswagen_t_roc_hu.yaml", "http_json", "porscheinformatik.com"),
    ("13_skoda_kodiaq_hu.yaml", "scrapling", "skoda.hu"),
    ("14_mg_zs_hu.yaml", "pdf_text", "mgmotor.eu"),
    ("16_volkswagen_tiguan_hu.yaml", "http_json", "porscheinformatik.com"),
    ("20_kgm_korando_hu.yaml", "pdf_text", "kgmhungary.hu"),
    ("22_peugeot_3008_hu.yaml", "pdf_text", "peugeot.hu"),
    ("24_bmw_x5_hu.yaml", "http_text", "bmw.hu"),
    ("28_peugeot_2008_hu.yaml", "pdf_text", "peugeot.hu"),
    ("31_mercedes_eqa_hu.yaml", "pdf_text", "mercedes-benz.hu"),
)


def _load_source(filename: str) -> dict[str, object]:
    source = yaml.safe_load((HU_SOURCE_ROOT / filename).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    return source


@pytest.mark.parametrize(
    ("filename", "extractor_type", "official_host_suffix"),
    REPAIRED_HU_PROFILES,
)
def test_repaired_hungary_profiles_keep_official_base_msrp_contract(
    filename: str,
    extractor_type: str,
    official_host_suffix: str,
) -> None:
    source = _load_source(filename)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_host = urlparse(str(source["source_url"])).hostname or ""
    profile_host = urlparse(str(profile_data["url"])).hostname or ""
    assert source_host == official_host_suffix or source_host.endswith(
        f".{official_host_suffix}"
    )
    if extractor_type != "pdf_text" or filename != "08_hyundai_tucson_hu.yaml":
        assert profile_host == official_host_suffix or profile_host.endswith(
            f".{official_host_suffix}"
        )

    assert source["extractor_type"] == extractor_type
    assert source["source_type"] in {
        "manufacturer_official",
        "official_price_list",
    }
    assert source["price_semantics"] == "base_msrp"
    assert profile_data["default_currency"] == "HUF"
    assert profile_data["default_tax_included"] is True

    profile = PROFILE_BUILDERS[extractor_type](profile_data)
    assert profile.url == profile_data["url"]


@pytest.mark.parametrize(
    "filename",
    (
        "07_kia_sportage_hu.yaml",
        "08_hyundai_tucson_hu.yaml",
        "14_mg_zs_hu.yaml",
        "20_kgm_korando_hu.yaml",
        "22_peugeot_3008_hu.yaml",
        "28_peugeot_2008_hu.yaml",
        "31_mercedes_eqa_hu.yaml",
    ),
)
def test_hungary_pdf_profiles_parse_deterministic_list_price_patterns(
    filename: str,
) -> None:
    source = _load_source(filename)
    profile = _build_pdf_text_profile(source["profile"])

    assert profile.entry_patterns
    assert profile.default_price_label
    for entry in profile.entry_patterns:
        assert "(?P<price>" in entry.pattern
        assert "monthly_payment" not in entry.pattern
        assert "Havi fizetendő" not in entry.pattern
        assert re.compile(entry.pattern)


def test_hungary_configurator_profiles_exclude_discount_and_finance_facts() -> None:
    for filename in (
        "01_suzuki_s_cross_hu.yaml",
        "02_suzuki_vitara_hu.yaml",
    ):
        source = _load_source(filename)
        profile = _build_http_text_profile(source["profile"])
        entry = profile.entry_patterns[0]

        assert profile.prefer_curl_fetch is True
        assert profile.match_reason["price_field"] == "prices.list"
        assert profile.match_reason["excluded_field"] == "prices.discount"
        assert "discount" not in entry.pattern

    for filename in (
        "12_volkswagen_t_roc_hu.yaml",
        "16_volkswagen_tiguan_hu.yaml",
    ):
        source = _load_source(filename)
        profile = _build_http_json_profile(source["profile"])

        assert profile.field_mapping.price == "listPrice.gross"
        assert profile.match_reason["kind"] == (
            "official_configurator_modelgroup_list_price"
        )
