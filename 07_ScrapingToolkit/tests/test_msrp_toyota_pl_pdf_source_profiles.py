from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_pdf_text_profile
from jato_scraper.extractors.pdf_text import PdfTextExtractor


SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
)

TOYOTA_PL_PDF_CASES = (
    (
        "pl/14_toyota_corolla_cross_pl.yaml",
        "2026-07-01",
        "6ea8985175d2e7f215906bd67cf05da4fe93c70895ca31f75d6044e62ba8eaa9",
        """
        Rata Leasingu KINTO ONE 1 386 PLN/MC NETTO.
        Cena specjalna 143 800 PLN.
        5-drzwiowy crossover Comfort Style Tour de Pologne GR SPORT Executive
        1.8 Hybrid 140 KM e-CVT
        cena katalogowa 145 900 157 900 157 900
        cena specjalna 143 800 155 600 155 600
        2.0 Hybrid Dynamic Force 180 KM e-CVT
        cena katalogowa 154 900 166 900 166 900 178 900 181 900
        cena specjalna 152 600 164 400 164 400 176 300 179 200
        2.0 Hybrid Dynamic Force 180 KM AWD-i e-CVT
        cena katalogowa - 176 900 176 900 188 900 191 900
        cena specjalna 174 300 174 300 186 100 189 100
        """,
        [
            (145_900.0, "COMFORT", "HEV"),
            (154_900.0, "COMFORT", "HEV"),
            (176_900.0, "STYLE", "HEV"),
        ],
    ),
    (
        "pl/07_toyota_rav4_pl.yaml",
        "2026-03-02",
        "c0a77ea07651bdb22dd388cb3cf798d96950cd7e4f68099f80912eb55f57bbc2",
        """
        Rata Leasingu KINTO ONE 1 569 PLN/MC NETTO.
        Cena specjalna 181 600 PLN.
        SUV z napędem 4×2 Comfort Style GR SPORT Executive
        2.5 Hybrid Dynamic Force 185 KM FWD e-CVT
        cena katalogowa 195 900 214 900 - 232 900
        cena specjalna 181 600 199 300 215 900
        SUV z napędem 4×4 Comfort Style GR SPORT Executive
        2.5 Hybrid Dynamic Force 194 KM AWD-i e-CVT
        cena katalogowa 205 900 224 900 242 900 242 900
        cena specjalna 190 900 208 500 225 200 225 200
        2.5 Plug-in Hybrid 309 KM AWD-i e-CVT
        cena katalogowa 232 900 251 900 269 900 269 900
        cena specjalna 215 900 233 600 250 200 250 200
        """,
        [
            (195_900.0, "COMFORT", "HEV"),
            (205_900.0, "COMFORT", "HEV"),
            (232_900.0, "COMFORT", "PHEV"),
        ],
    ),
)


@pytest.mark.parametrize(
    ("relative_path", "valid_from", "sha256", "sample_text", "expected"),
    TOYOTA_PL_PDF_CASES,
)
def test_repaired_toyota_pl_pdf_profiles_extract_only_gross_catalog_prices(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: str,
    valid_from: str,
    sha256: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
) -> None:
    source = yaml.safe_load((SOURCE_ROOT / relative_path).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "pdf.sites.toyota.pl"
    assert source_url.endswith(".pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["default_currency"] == "PLN"
    assert profile_data["default_tax_included"] is True
    assert "katalogowa brutto" in profile_data["default_price_label"]
    assert profile_data["match_reason"]["document_valid_from"] == valid_from
    assert profile_data["match_reason"]["document_audited_on"] == "2026-07-15"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == sha256
    assert "css" not in profile_data
    assert "text_regex" not in profile_data

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
    monkeypatch.setattr(extractor, "_extract_text", lambda: sample_text)

    observations = extractor.extract()
    actual = [
        (observation.msrp_value, observation.official_trim, observation.jato_powertrain)
        for observation in observations
    ]
    assert actual == expected
    assert all(observation.tax_included for observation in observations)
    assert all(observation.match_confidence == 0.9 for observation in observations)
    assert all(observation.match_status == "review_required" for observation in observations)
    excluded_prices = {
        1_386.0,
        1_569.0,
        143_800.0,
        152_600.0,
        174_300.0,
        181_600.0,
        190_900.0,
        215_900.0,
    }
    assert all(observation.msrp_value not in excluded_prices for observation in observations)
