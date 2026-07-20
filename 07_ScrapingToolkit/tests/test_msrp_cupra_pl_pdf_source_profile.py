from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_pdf_text_profile
from jato_scraper.extractors.pdf_text import PdfTextExtractor


SOURCE_PATH = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
    / "pl"
    / "27_cupra_formentor_pl.yaml"
)


def test_repaired_cupra_pl_profile_extracts_gross_regular_prices_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = yaml.safe_load(SOURCE_PATH.read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "www.cupraofficial.pl"
    assert source_url.endswith(".pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["browser_download_fallback"] is True
    assert profile_data.get("prefer_curl_download") is not True
    assert profile_data["default_currency"] == "PLN"
    assert profile_data["default_tax_included"] is True
    assert "brutto" in profile_data["default_price_label"]
    assert profile_data["match_reason"]["document_valid_from"] == "2026-07-02"
    assert profile_data["match_reason"]["document_audited_on"] == "2026-07-15"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == (
        "b049f91852a469208633e30028aa0baefbfe5efc8b81a8d5a5b667ea0fe28656"
    )
    assert profile_data["match_reason"]["price_column"] == (
        "gross_regular_price_before_special_offer"
    )
    assert "css" not in profile_data

    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code=str(source["source_code"]),
            country=str(source["country"]),
            brand=str(source["brand"]),
            source_url=source_url,
            source_type=str(source["source_type"]),
            price_semantics=str(source["price_semantics"]),
        ),
        _build_pdf_text_profile(profile_data),
    )
    sample_text = """
    Leasing Jak Abonament 528 zł netto/mies. Rata kredytu 613 zł brutto.
    Cena brutto dla produktów Leasing i Kredyt Klasyczny
    Formentor 1.5 TSI 150 KM manualna 6-biegowa benzyna 145 000 zł 145 000 zł 528 zł 613 zł
    Formentor 1.5 eTSI mHEV 150 KM DSG automatyczna 7-biegowa hybryda mHEV
    160 700 zł 160 700 zł 590 zł 679 zł
    Formentor 1.5 e-HYBRID 204 KM DSG automatyczna 6-biegowa hybryda Plug-in
    183 500 zł 213 500 zł 203 500 zł 213 500 zł 323 zł 533 zł
    Formentor 2.0 TDI 150 KM DSG automatyczna 7-biegowa diesel
    165 100 zł 165 100 zł 736 zł 862 zł
    Oferta specjalna bez finansowania
    Formentor 1.5 TSI 150 KM manualna 6-biegowa benzyna 135 000 zł
    Formentor 1.5 eTSI mHEV 150 KM DSG automatyczna 7-biegowa hybryda mHEV
    150 700 zł
    Formentor 2.0 TDI 150 KM DSG automatyczna 7-biegowa diesel 155 100 zł
    """
    monkeypatch.setattr(extractor, "_extract_text", lambda: sample_text)

    observations = extractor.extract()
    actual = [
        (observation.msrp_value, observation.official_powertrain, observation.jato_powertrain)
        for observation in observations
    ]
    assert actual == [
        (145_000.0, "1.5 TSI 150 KM 6MT", "ICE"),
        (160_700.0, "1.5 eTSI mHEV 150 KM 7DSG", "MHEV"),
        (213_500.0, "1.5 e-HYBRID 204 KM 6DSG", "PHEV"),
        (165_100.0, "2.0 TDI 150 KM 7DSG", "ICE"),
    ]
    excluded = {
        323.0,
        528.0,
        533.0,
        613.0,
        135_000.0,
        150_700.0,
        155_100.0,
        183_500.0,
        203_500.0,
    }
    assert all(observation.msrp_value not in excluded for observation in observations)
    assert all(observation.tax_included for observation in observations)
    assert all(observation.match_confidence == 0.9 for observation in observations)
    assert all(observation.match_status == "review_required" for observation in observations)
