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

OPEL_PL_PDF_CASES = (
    (
        "pl/13_opel_mokka_pl.yaml",
        "f393e45f964356d2595fdf97f09c32f79840be5dc5486c19245990327ae044cf",
        """
        Cena promocyjna od 94 500 zł. Pożyczka 3x33, cena pojazdu 94 950 zł.
        Turbo M6 136 KM 113 500 94 500 96 500 zł
        Turbo Hybrid eDCT 145 KM 125 500 106 500 108 500 zł
        Electric e156KM 54 kWh 156 KM 161 500 131 500 129 500 zł
        GSE e281KM 54 kWh 281 KM x x x x x x x x x x x x 205 100 175 100 173 100 zł
        """,
        [
            (113_500.0, "EDITION", "ICE"),
            (125_500.0, "EDITION", "MHEV"),
            (161_500.0, "EDITION", "BEV"),
            (205_100.0, "GSE", "BEV"),
        ],
    ),
    (
        "pl/16_opel_grandland_pl.yaml",
        "771a5627b2fd163d6e989fbcad6854e2722d4f9da5e7c76de966d9019871ffb6",
        """
        Cena promocyjna od 131 100 zł. Leasing od 101,8 procent.
        Turbo Hybrid eDTC6 145 KM 150 100 131 100 133 100 zł
        1.6 Plug-in Hybrid eDCT7 225 KM 190 800 171 800 173 800 zł
        Electric 73 kWh e213 KM 202 300 172 300 170 300 zł
        Electric 98 kWh e230 KM 215 300 185 300 183 300 zł
        Electric 73 kWh 4x4 e325 KM x x x x x x 259 000 209 000 207 000
        """,
        [
            (150_100.0, "EDITION", "MHEV"),
            (190_800.0, "EDITION", "PHEV"),
            (202_300.0, "EDITION", "BEV"),
            (215_300.0, "EDITION", "BEV"),
            (259_000.0, "ULTIMATE", "BEV"),
        ],
    ),
)


@pytest.mark.parametrize(
    ("relative_path", "sha256", "sample_text", "expected"),
    OPEL_PL_PDF_CASES,
)
def test_repaired_opel_pl_pdf_profiles_extract_only_gross_catalog_prices(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: str,
    sha256: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
) -> None:
    source = yaml.safe_load((SOURCE_ROOT / relative_path).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "www.opel.pl"
    assert source_url.endswith(".pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["prefer_curl_download"] is True
    assert profile_data["default_currency"] == "PLN"
    assert profile_data["default_tax_included"] is True
    assert "detaliczna brutto" in profile_data["default_price_label"]
    assert profile_data["match_reason"]["document_valid_from"] == "2026-07-01"
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
        94_500.0,
        94_950.0,
        106_500.0,
        131_100.0,
        131_500.0,
        171_800.0,
        172_300.0,
        175_100.0,
        185_300.0,
        209_000.0,
    }
    assert all(observation.msrp_value not in excluded_prices for observation in observations)
