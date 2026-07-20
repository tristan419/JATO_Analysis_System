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

PEUGEOT_PDF_CASES = (
    (
        "it/14_peugeot_2008_it.yaml",
        "EUR",
        """
        Prezzo Promo 19.950 EUR, 35 rate da 109 EUR.
        FULL ELECTRIC 39.500 € 41.100 € 41.900 € 44.400 €
        HYBRID 32.370 € 32.620 € 34.220 € 36.020 € 38.120 €
        Hybrid 110 cv e-DCS6 31.170 € 31.420 € 33.020 €
        BENZINA 28.350 € 28.600 € 30.200 €
        """,
        [
            (39_500.0, "STYLE", "BEV"),
            (32_370.0, "STYLE", "MHEV"),
            (31_170.0, "STYLE", "MHEV"),
            (28_350.0, "STYLE", "ICE"),
        ],
    ),
    (
        "it/15_peugeot_3008_it.yaml",
        "EUR",
        """
        Prezzo Promo 30.470 EUR, 35 rate da 249 EUR.
        Motore Elettrico 210 cv (157 kW)-Batteria 73 kWh 0 7
        € 43,780 € 45,180 € 48,630 € 51,830
        Motore Elettrico 230 cv (170 KW) Batteria 96,9 kWh LONG RANGE 0 7
        € 48,430 € 49,830 € 52,680 € 56,080
        Dual Motor 325cv (239 kW) Batteria 73 kWh 0 -
        € 47,580 € 50,630 € 53,830
        Plug-in Hybrid 225 e-DCS7 55-58 8
        € 46,580 € 47,980 € 51,030 € 54,030
        Hybrid 145 e-DCS6 120 - 126 7
        € 41,650 € 43,050 € 46,000 € 48,600
        """,
        [
            (43_780.0, "ALLURE", "BEV"),
            (48_430.0, "ALLURE", "BEV"),
            (47_580.0, "ALLURE BUSINESS", "BEV"),
            (46_580.0, "ALLURE", "PHEV"),
            (41_650.0, "ALLURE", "MHEV"),
        ],
    ),
    (
        "pl/06_peugeot_3008_pl.yaml",
        "PLN",
        """
        Rata 1 299 zł miesięcznie. Cena promocyjna 139 900 zł.
        HYBRID (g/km)(1)
        156 450 zł 165 450 zł 170 800 zł 184 750 zł
        PLUG-IN HYBRID
        191 100 zł 200 100 zł 205 700 zł 221 650 zł
        ELEKTRYCZNY
        Silnik elektryczny 210 Akumulator 73 kWh 203 500 zł 212 500 zł
        Silnik elektryczny 325 Akumulator 73 kWh 243 950 zł
        """,
        [
            (156_450.0, "ALLURE", "MHEV"),
            (191_100.0, "ALLURE", "PHEV"),
            (203_500.0, "ALLURE", "BEV"),
            (243_950.0, "GT EXCLUSIVE", "BEV"),
        ],
    ),
    (
        "pl/08_peugeot_5008_pl.yaml",
        "PLN",
        """
        Rata 1 499 zł miesięcznie. Cena promocyjna 149 900 zł.
        HYBRID (g/km)(1)
        171 950 zł 180 950 zł 187 300 zł 201 250 zł
        PLUG-IN HYBRID
        204 550 zł 213 550 zł 220 150 zł 236 100 zł
        ELEKTRYCZNY
        Silnik elektryczny 210 / Akumulator 73 kWh 216 750 zł 225 750 zł
        Silnik elektryczny 325 / Akumulator 73 kWh 259 200 zł
        """,
        [
            (171_950.0, "ALLURE", "MHEV"),
            (204_550.0, "ALLURE", "PHEV"),
            (216_750.0, "ALLURE", "BEV"),
            (259_200.0, "GT EXCLUSIVE", "BEV"),
        ],
    ),
)


@pytest.mark.parametrize(
    ("relative_path", "currency", "sample_text", "expected"),
    PEUGEOT_PDF_CASES,
)
def test_repaired_peugeot_pdf_profiles_extract_only_official_gross_prices(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: str,
    currency: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
) -> None:
    source = yaml.safe_load((SOURCE_ROOT / relative_path).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    host = urlparse(source_url).hostname or ""
    assert host in {"www.peugeot.it", "www.peugeot.pl"}
    assert source_url.endswith(".pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["default_currency"] == currency
    assert profile_data["default_tax_included"] is True
    assert profile_data["match_reason"]["document_valid_from"] == "2026-07-01"
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
    assert all(observation.msrp_value not in {109.0, 249.0, 1_299.0, 1_499.0} for observation in observations)
    assert all(observation.msrp_value not in {19_950.0, 30_470.0, 139_900.0, 149_900.0} for observation in observations)
