from pathlib import Path
import re
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import (
    _build_pdf_text_profile,
    _build_scrapling_profile,
)
from jato_scraper.extractors.pdf_text import PdfTextExtractor


PL_SOURCE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "source_drafts"
    / "suv_only_country_model_top30"
    / "pl"
)


def _load_source(filename: str) -> dict[str, object]:
    source = yaml.safe_load((PL_SOURCE_ROOT / filename).read_text(encoding="utf-8"))
    assert isinstance(source, dict)
    return source


def _extract_pdf_observations(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    sample_text: str,
):
    source = _load_source(filename)
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


def test_repaired_seat_ateca_pl_profile_extracts_regular_gross_prices_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sample_text = """
    Cennik Ateca z rabatem do 16 000 zł
    Oferta bez promocyjnego finansowania
    Wersja Silnik Skrzynia biegow Naped Cena brutto
    Reference+ 1.0 TSI 115 KM manualna 6-biegowa benzyna
    96 900 zł 108 900 zł 108 900 zł
    Style 1.5 TSI 150 KM manualna 6-biegowa benzyna
    115 500 zł 131 500 zł 131 500 zł
    Style 1.5 TSI 150 KM DSG automatyczna 7-biegowa benzyna
    126 900 zł 142 900 zł 142 900 zł
    Style 2.0 TDI 150 KM DSG automatyczna 7-biegowa diesel
    138 300 zł 154 300 zł 154 300 zł
    FR 1.5 TSI 150 KM manualna 6-biegowa benzyna
    134 600 zł 150 600 zł 150 600 zł
    FR 1.5 TSI 150 KM DSG automatyczna 7-biegowa benzyna
    145 900 zł 161 900 zł 161 900 zł
    FR 2.0 TDI 150 KM DSG automatyczna 7-biegowa diesel
    157 500 zł 173 500 zł 173 500 zł
    Rata kredytu 1 209,39 zł
    """
    source, profile_data, observations = _extract_pdf_observations(
        monkeypatch,
        "11_seat_ateca_pl.yaml",
        sample_text,
    )

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "www.seat.pl"
    assert source_url.endswith("cennik_SEAT_Ateca_MY26PY26.pdf")
    assert source["source_type"] == "official_price_list"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["browser_download_fallback"] is True
    assert profile_data["match_reason"]["document_scope"] == "MY2026/PY2026"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == (
        "e508b619c233cbfb6cda1282b504ff381d790fcc1c3ddb47b6aba078d945a223"
    )
    assert profile_data["match_reason"]["price_column"] == (
        "gross_regular_price_before_special_offer"
    )
    assert [observation.msrp_value for observation in observations] == [
        108_900.0,
        131_500.0,
        142_900.0,
        154_300.0,
        150_600.0,
        161_900.0,
        173_500.0,
    ]
    excluded = {1_209.39, 96_900.0, 115_500.0, 126_900.0, 138_300.0}
    assert all(observation.msrp_value not in excluded for observation in observations)
    assert all(observation.jato_powertrain == "ICE" for observation in observations)


@pytest.mark.parametrize(
    ("filename", "valid_from", "sha256", "sample_text", "expected"),
    (
        (
            "29_kia_ev6_pl.yaml",
            "2026-06-18",
            "81bee97fbd8479c7f175d50297319d51958f8089326fa2dffac6becee6f29a59",
            """
            Napęd Zasięg (km) Air Earth GT-Line cykl miejski cykl mieszany
            63 kWh 170 KM RWD 644 428 212 900 227 900 -
            84 kWh 229 KM RWD 766 - 751 582 - 560 230 900 247 900 266 900
            84 kWh 325 KM AWD 669 522 - 270 900 289 900
            """,
            [
                (212_900.0, "Air", "BEV"),
                (227_900.0, "Earth", "BEV"),
                (230_900.0, "Air", "BEV"),
                (247_900.0, "Earth", "BEV"),
                (266_900.0, "GT-Line", "BEV"),
                (270_900.0, "Earth", "BEV"),
                (289_900.0, "GT-Line", "BEV"),
            ],
        ),
        (
            "30_kia_sorento_pl.yaml",
            "2026-07-01",
            "8029e05b197eb751d89dfedac4a231872bb9f6100888219214ddb0a905a7f361",
            """
            silnik benzynowy Hybrid
            L 7-miejscowy XL 7-miejscowy Prestige Line 6- lub 7-miejscowy
            1.6 T-GDI 239 KM 6AT 2WD 203 400 222 900 251 400
            1.6 T-GDI 239 KM 6AT AWD 212 400 231 900 260 400
            silnik benzynowy Plug-in Hybrid
            1.6 T-GDI 288 KM 6AT AWD 250 900 270 400 298 900
            Diesel
            2.2 CRDi SCR 193 KM 8DCT AWD 246 900 264 900 294 900
            Leasing od 100% wplata wlasna 45%
            """,
            [
                (203_400.0, "L", "HEV"),
                (222_900.0, "XL", "HEV"),
                (251_400.0, "Prestige Line", "HEV"),
                (212_400.0, "L", "HEV"),
                (231_900.0, "XL", "HEV"),
                (260_400.0, "Prestige Line", "HEV"),
                (250_900.0, "L", "PHEV"),
                (270_400.0, "XL", "PHEV"),
                (298_900.0, "Prestige Line", "PHEV"),
                (246_900.0, "L", "ICE"),
                (264_900.0, "XL", "ICE"),
                (294_900.0, "Prestige Line", "ICE"),
            ],
        ),
    ),
)
def test_repaired_kia_pl_pdf_profiles_extract_all_gross_list_prices(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    valid_from: str,
    sha256: str,
    sample_text: str,
    expected: list[tuple[float, str, str]],
) -> None:
    source, profile_data, observations = _extract_pdf_observations(
        monkeypatch,
        filename,
        sample_text,
    )

    source_url = str(source["source_url"])
    assert urlparse(source_url).hostname == "www.kia.com"
    assert source_url.endswith(".pdf")
    assert source["source_type"] == "official_price_list"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "pdf_text"
    assert profile_data["match_reason"]["document_valid_from"] == valid_from
    assert profile_data["match_reason"]["document_audited_on"] == "2026-07-19"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == sha256
    actual = [
        (
            observation.msrp_value,
            observation.official_trim,
            observation.jato_powertrain,
        )
        for observation in observations
    ]
    assert actual == expected
    assert all(observation.tax_included for observation in observations)
    assert all(observation.match_status == "review_required" for observation in observations)


def test_repaired_porsche_pl_profile_targets_only_base_cayenne_coupe() -> None:
    source = _load_source("28_porsche_cayenne_coupe_pl.yaml")
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    source_url = str(source["source_url"])
    assert source_url == "https://models.porsche.com/pl-PL/model-start/cayenne"
    assert source["source_type"] == "manufacturer_official"
    assert source["price_semantics"] == "base_msrp"
    assert source["extractor_type"] == "scrapling"
    assert "css" not in profile_data
    assert profile_data["fixed_jato_powertrain"] == "ICE"

    profile = _build_scrapling_profile(profile_data)
    assert profile.text_regex is not None
    assert len(profile.text_regex.entry_patterns) == 1
    entry = profile.text_regex.entry_patterns[0]
    sample_text = """
    Cayenne Coupe Electric Od 463 000 PLN 442 KM
    Cayenne Coupe Od 502 000 PLN 353 KM
    Cayenne Coupe Black Edition Od 562 000 PLN 353 KM
    Cayenne E-Hybrid Coupe Od 527 000 PLN 470 KM
    """
    matches = list(re.finditer(entry.pattern, sample_text, flags=re.IGNORECASE | re.DOTALL))
    assert [match.group("price") for match in matches] == ["502 000"]
