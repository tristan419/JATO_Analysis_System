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

HYUNDAI_PL_PDF_CASES = (
    (
        "pl/09_hyundai_kona_pl.yaml",
        "6db13cb9713e5f034f17f3f24e5d8d441d3e89fbdbaa1ab9e098e265ec11908d",
        """
        Cena promocyjna od 98 700 zł. Leasing od 101%, Kredyt 60/40.
        SMART 1.0 T-GDI 6MT 2WD (115 KM) 118 700 zł 98 700 zł 105 700 zł
        EXECUTIVE
        1.0 T-GDI 6MT 2WD (115 KM) 128 200 zł 108 200 zł 115 200 zł
        1.6 T-GDI 7DCT 2WD (150 KM) 143 200 zł 121 200 zł 124 200 zł
        PLATINUM
        1.6 T-GDI 6MT 2WD (150 KM) 147 700 zł 125 700 zł 128 700 zł
        1.6 T-GDI 7DCT 4WD (180 KM) 175 700 zł 153 700 zł 156 700 zł
        """,
        [
            (118_700.0, "SMART", "ICE"),
            (143_200.0, "EXECUTIVE", "ICE"),
            (175_700.0, "PLATINUM", "ICE"),
        ],
        {98_700.0, 105_700.0, 121_200.0, 153_700.0},
    ),
    (
        "pl/01_hyundai_tucson_pl.yaml",
        "d9d7713112afc842cabb2a24e8368cb0b7a45fb2aba0ca7e6a57c3d5aaf982ed",
        """
        Leasing od 101%, wpłata własna 61 950 zł.
        MODERN PLUS 1.6 T-GDI 6MT 2WD (150 KM) 144 900 zł 123 900 zł 127 900 zł
        SMART
        1.6 T-GDI 6MT 2WD (150 KM) 152 500 zł 129 500 zł 133 500 zł
        1.6 T-GDI 7DCT 4WD (180 KM) 173 800 zł 150 800 zł 154 800 zł
        SMART BLACK
        1.6 T-GDI 7DCT 2WD (150 KM) 177 300 zł 154 300 zł 158 300 zł
        EXECUTIVE
        1.6 T-GDI 7DCT 4WD (180 KM) 194 300 zł 171 300 zł 175 300 zł
        ULTIMATE
        1.6 T-GDI 7DCT 2WD (150 KM) 208 100 zł 185 100 zł 189 100 zł
        1.6 T-GDI 7DCT 4WD (180 KM) 224 100 zł 201 100 zł 205 100 zł
        """,
        [
            (144_900.0, "MODERN PLUS", "ICE"),
            (173_800.0, "SMART", "ICE"),
            (224_100.0, "ULTIMATE", "ICE"),
        ],
        {61_950.0, 123_900.0, 150_800.0, 171_300.0, 201_100.0},
    ),
    (
        "pl/26_hyundai_santa_fe_pl.yaml",
        "4f881f3ece62bf50a087e8619678309a30ac231b974a769cf61409d05845bf58",
        """
        Kredyt 60/40 RRSO 0%. Promocyjny upust 40 000 zł.
        SMART 1.6 T-GDI HEV 6AT 2WD (239KM) 5 209 900 zł 169 900 zł 174 900 zł
        EXECUTIVE
        1.6 T-GDI HEV 6AT 2WD (239KM) 5 246 900 zł 206 900 zł 211 900 zł
        1.6 T-GDI HEV 6AT 4WD (239KM) 5 256 900 zł 216 900 zł 221 900 zł
        PLATINUM
        1.6 T-GDI HEV 6AT 4WD (239KM) 5 272 900 zł 232 900 zł 237 900 zł
        1.6 T-GDI HEV 6AT 4WD (239KM) 7 277 900 zł 237 900 zł 242 900 zł
        CALLIGRAPHY
        1.6 T-GDI HEV 6AT 4WD (239KM) 6 292 900 zł 252 900 zł 257 900 zł
        """,
        [
            (209_900.0, "SMART", "HEV"),
            (256_900.0, "EXECUTIVE", "HEV"),
            (277_900.0, "PLATINUM", "HEV"),
            (292_900.0, "CALLIGRAPHY", "HEV"),
        ],
        {40_000.0, 169_900.0, 216_900.0, 237_900.0, 252_900.0},
    ),
)


@pytest.mark.parametrize(
    ("relative_path", "sha256", "sample_text", "expected", "excluded_prices"),
    HYUNDAI_PL_PDF_CASES,
)
def test_repaired_hyundai_pl_pdf_profiles_extract_only_vat_inclusive_base_prices(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: str,
    sha256: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
    excluded_prices: set[float],
) -> None:
    source = yaml.safe_load((SOURCE_ROOT / relative_path).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "dmassets.hyundai.com"
    assert urlparse(source_url).path.endswith("pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["prefer_curl_download"] is True
    assert profile_data["default_currency"] == "PLN"
    assert profile_data["default_tax_included"] is True
    assert "podstawowa brutto" in profile_data["default_price_label"]
    assert profile_data["match_reason"]["document_valid_from"] == "2026-07-03"
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
    assert all(observation.msrp_value not in excluded_prices for observation in observations)
