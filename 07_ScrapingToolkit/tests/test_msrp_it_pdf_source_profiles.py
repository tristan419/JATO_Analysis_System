from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_pdf_text_profile
from jato_scraper.extractors.pdf_text import PdfTextExtractor


IT_SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
    / "it"
)


def _extract_observations(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    sample_text: str,
):
    source = yaml.safe_load((IT_SOURCE_ROOT / filename).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code=str(source["source_code"]),
            country=str(source["country"]),
            brand=str(source["brand"]),
            source_url=str(source["source_url"]),
            source_type=str(source["source_type"]),
            price_semantics=str(source["price_semantics"]),
        ),
        _build_pdf_text_profile(profile_data),
    )
    monkeypatch.setattr(extractor, "_extract_text", lambda: sample_text)
    return source, profile_data, extractor.extract()


@pytest.mark.parametrize(
    ("filename", "valid_from", "sha256", "sample_text", "expected"),
    (
        (
            "02_toyota_yaris_cross_it.yaml",
            "2026-02-28",
            "0d824faa2e708075241754ac1c36592fa7ca072bcab66a7e214da651d20b4b19",
            """
            € Prezzo iva incl. Active Trend Trend (130) Lounge GR SPORT Trend (130) Lounge
            Chiavi in mano*
            28.750 € 31.350 € 31.850 € 34.250 € 35.550 € 34.350 € 36.750 €
            IVA inclusa
            27.700 € 30.300 € 30.800 € 33.200 € 34.500 € 33.300 € 35.700 €
            IVA esclusa
            22.705 € 24.836 € 25.246 € 27.213 € 28.279 € 27.295 € 29.262 €
            """,
            [
                (28_750.0, "Active", "HEV"),
                (31_350.0, "Trend", "HEV"),
                (31_850.0, "Trend (130)", "HEV"),
                (34_250.0, "Lounge", "HEV"),
                (35_550.0, "GR SPORT", "HEV"),
                (34_350.0, "Trend (130) AWD-i", "HEV"),
                (36_750.0, "Lounge AWD-i", "HEV"),
            ],
        ),
        (
            "07_toyota_aygo_x_it.yaml",
            "2025-10-08",
            "6c67e55331f1e147324048da549c667af038eebf80fd072076e861744d32a47f",
            """
            € Prezzo iva incl. Aygo X Icon Premium GR SPORT
            Chiavi in mano*
            20.850 € 23.350 € 24.950 € 25.950 €
            IVA inclusa
            19.900 € 22.400 € 24.000 € 25.000 €
            IVA esclusa
            16.311 € 18.361 € 19.672 € 20.492 €
            """,
            [
                (20_850.0, "Aygo X", "HEV"),
                (23_350.0, "Icon", "HEV"),
                (24_950.0, "Premium", "HEV"),
                (25_950.0, "GR SPORT", "HEV"),
            ],
        ),
        (
            "19_toyota_c_hr_it.yaml",
            "2026-04-02",
            "6a122e30c0b2416fcfad7d77cd71865f7ae24f2f230ee20eaddb7109f74cb128",
            """
            Active Trend Lounge Lounge Hero GR SPORT Active Trend Lounge Lounge Hero GR SPORT
            Chiavi in mano*
            36.250 € 38.950 € 41.950 € 43.450 € 43.450 €
            37.450 € 40.450 € 43.450 € 44.950 € 44.950 €
            IVA inclusa
            35.120 € 37.820 € 40.820 € 42.320 € 42.320 €
            36.320 € 39.320 € 42.320 € 43.820 € 43.820 €
            """,
            [
                (36_250.0, "Active", "HEV"),
                (38_950.0, "Trend", "HEV"),
                (41_950.0, "Lounge", "HEV"),
                (43_450.0, "Lounge Hero", "HEV"),
                (43_450.0, "GR SPORT", "HEV"),
                (37_450.0, "Active", "PHEV"),
                (40_450.0, "Trend", "PHEV"),
                (43_450.0, "Lounge", "PHEV"),
                (44_950.0, "Lounge Hero", "PHEV"),
                (44_950.0, "GR SPORT", "PHEV"),
            ],
        ),
    ),
)
def test_toyota_it_profiles_extract_only_turnkey_vat_inclusive_prices(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    valid_from: str,
    sha256: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
) -> None:
    source, profile_data, observations = _extract_observations(
        monkeypatch,
        filename,
        sample_text,
    )

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "scene7.toyota.eu"
    assert source_url.endswith(".pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["default_currency"] == "EUR"
    assert profile_data["default_tax_included"] is True
    assert profile_data["match_reason"]["document_valid_from"] == valid_from
    assert profile_data["match_reason"]["document_audited_on"] == "2026-07-20"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == sha256
    assert profile_data["match_reason"]["price_column"] == "turnkey_vat_inclusive"
    actual = [
        (
            observation.msrp_value,
            observation.official_trim,
            observation.jato_powertrain,
        )
        for observation in observations
    ]
    assert actual == expected
    excluded = {
        16_311.0,
        19_900.0,
        22_705.0,
        27_700.0,
        35_120.0,
        43_820.0,
    }
    assert all(observation.msrp_value not in excluded for observation in observations)


def test_ford_kuga_it_profile_extracts_turnkey_column_for_all_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sample_text = """
    FTI1 28.679,39 36.000,00
    FTI2 30.933,48 38.750,00
    FTI3 32.777,75 41.000,00
    FTI4 36.261,35 45.250,00
    FST1 29.908,89 37.500,00
    FST2 30.728,57 38.500,00
    FST3 32.162,99 40.250,00
    FST4 34.007,25 42.500,00
    FST5 37.490,86 46.750,00
    FTX1 33.597,42 42.000,00
    FTX2 35.031,84 43.750,00
    FTX3 36.876,11 46.000,00
    FTX4 40.359,71 50.250,00
    FCX1 33.597,42 42.000,00
    FCX2 35.031,84 43.750,00
    FCX3 36.876,11 46.000,00
    FCX4 40.359,71 50.250,00
    FBC1 37.490,86 46.750,00
    FBC2 39.335,12 49.000,00
    FBC3 42.818,73 53.250,00
    """
    source, profile_data, observations = _extract_observations(
        monkeypatch,
        "28_ford_kuga_it.yaml",
        sample_text,
    )

    assert urlparse(str(source["source_url"])).hostname == "www.ford.it"
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["match_reason"]["document_scope"] == "Kuga MY2027.00"
    assert profile_data["match_reason"]["document_valid_from"] == "2026-06-22"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == (
        "1737338c866bcfa5cbcf8ff3127c332d3b361d6337bce04ce914172e407899ac"
    )
    assert len(observations) == 20
    assert [observation.msrp_value for observation in observations[:4]] == [
        36_000.0,
        38_750.0,
        41_000.0,
        45_250.0,
    ]
    assert [observation.msrp_value for observation in observations[-3:]] == [
        46_750.0,
        49_000.0,
        53_250.0,
    ]
    assert all(observation.tax_included for observation in observations)
    assert all(observation.match_confidence == 0.92 for observation in observations)
    assert all(
        observation.msrp_value
        not in {28_679.39, 30_933.48, 32_777.75, 42_818.73}
        for observation in observations
    )
