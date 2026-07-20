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

SKODA_PL_PDF_CASES = (
    (
        "pl/20_skoda_kamiq_pl.yaml",
        "2026-05-15",
        "c3109695d95220ed71c8876e5cb2112402e2463e27477f70c4a4515a0c99fb01",
        """
        Leasing od 759 zł netto/mies. Suma korzyści do 10 900 zł.
        1.0 TSI 70 kW (95KM) manualna, 5-biegowa
        86 300 zł 92 300 zł 88 300 zł 95 550 zł 104 550 zł 99 550 zł
        1.0 TSI 85 kW (115 KM) automatyczna, 7-biegowa DSG
        97 100 zł 103 100 zł 99 100 zł 106 550 zł 115 550 zł 110 550 zł
        1.5 TSI 110 kW (150 KM) automatyczna, 7-biegowa DSG
        - - 113 200 zł 122 200 zł 117 200 zł 115 900 zł 124 900 zł 118 900 zł
        """,
        [
            (92_300.0, "ESSENCE", "ICE"),
            (103_100.0, "ESSENCE", "ICE"),
            (122_200.0, "SELECTION", "ICE"),
        ],
        {759.0, 10_900.0, 86_300.0, 88_300.0, 97_100.0, 99_100.0, 113_200.0},
    ),
    (
        "pl/21_skoda_karoq_pl.yaml",
        "2026-06-12",
        "8b60a4c935f78450f11bae282e7a81e8c2d92256daf14047cfcaf2d8b1b7b0d9",
        """
        Leasing od 980 zł/msc. Suma korzyści do 17 600 zł.
        1.0 TSI 85 kW (115 KM) manualna, 6-biegowa
        103 000 zł 113 000 zł 110 000 zł 115 000 zł 125 000 zł 122 000 zł
        2.0 TSI 4x4 140 kW (190 KM) automat., 7-biegowa DSG
        - - - - - - 163 400 zł 177 400 zł 171 400 zł
        2.0 TDI 4x4 110 kW (150 KM) automat., 7-biegowa DSG
        - - 155 600 zł 165 600 zł 162 600 zł 164 550 zł 175 550 zł 171 550 zł
        """,
        [
            (113_000.0, "ESSENCE", "ICE"),
            (177_400.0, "SPORTLINE", "ICE"),
            (165_600.0, "SELECTION", "ICE"),
        ],
        {980.0, 17_600.0, 103_000.0, 110_000.0, 155_600.0, 163_400.0, 171_400.0},
    ),
)


@pytest.mark.parametrize(
    ("relative_path", "created_on", "sha256", "sample_text", "expected", "excluded"),
    SKODA_PL_PDF_CASES,
)
def test_repaired_skoda_pl_profiles_extract_regular_prices_not_promotions(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: str,
    created_on: str,
    sha256: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
    excluded: set[float],
) -> None:
    source = yaml.safe_load((SOURCE_ROOT / relative_path).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "www.skoda.pl"
    assert urlparse(source_url).path.startswith("/_doc/")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["prefer_curl_download"] is True
    assert profile_data["default_currency"] == "PLN"
    assert profile_data["default_tax_included"] is True
    assert "regularna brutto" in profile_data["default_price_label"]
    assert profile_data["match_reason"]["document_scope"] == "RP2026/MY2027"
    assert profile_data["match_reason"]["document_pdf_created_on"] == created_on
    assert profile_data["match_reason"]["document_audited_on"] == "2026-07-15"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == sha256
    assert profile_data["match_reason"]["price_column"] == (
        "crossed_out_regular_price_before_reduction"
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
    assert all(observation.msrp_value not in excluded for observation in observations)
