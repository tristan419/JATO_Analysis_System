from pathlib import Path
import re
from urllib.parse import urlparse

import pytest
import yaml

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import (
    _build_http_json_profile,
    _build_pdf_text_profile,
    _build_scrapling_profile,
    _resolve_profile_raw,
)
from jato_scraper.extractors.http_json import HttpJsonExtractor
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


def test_repaired_evoque_pl_profile_extracts_gross_prices_not_lease_payments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sample_text = """
    LEASING TO GO
    S 2.0D 163KM 2 248
    S 2.0D 204KM 2 414
    CENNIK
    Wszystkie podane ceny sa cenami brutto wyrazonymi w PLN.
    WERSJA WYPOSAZENIA SILNIK RODZAJ PALIWA MOC SILNIKA NAPED SKRZYNIA BIEGOW CENA
    S
    2.0 L R4 Diesel 163 KM AWD Automatyczna 240 500
    2.0 L R4 Diesel 204 KM AWD Automatyczna 258 300
    1.5 L R3 Benzyna 160 KM FWD Automatyczna 218 900
    1.5 L R3 Hybryda PHEV 269 KM AWD Automatyczna 305 600
    DYNAMIC SE
    2.0 L R4 Diesel 163 KM AWD Automatyczna 276 300
    2.0 L R4 Diesel 204 KM AWD Automatyczna 291 200
    1.5 L R3 Benzyna 160 KM FWD Automatyczna 256 800
    1.5 L R3 Hybryda PHEV 269 KM AWD Automatyczna 340 000
    AUTOBIOGRAPHY
    2.0 L R4 Diesel 204 KM AWD Automatyczna 328 400
    1.5 L R3 Hybryda PHEV 269 KM AWD Automatyczna 379 500
    """
    source, profile_data, observations = _extract_pdf_observations(
        monkeypatch,
        "04_land_rover_range_rover_evoque_pl.yaml",
        sample_text,
    )

    assert source["source_type"] == "official_price_list"
    assert source["extractor_type"] == "pdf_text"
    assert urlparse(str(source["source_url"])).hostname == "www.rangerover.com"
    assert profile_data["match_reason"]["document_scope"] == "MY2026.5"
    assert profile_data["match_reason"]["document_sha256_at_audit"] == (
        "e936d725459100dbc5d64211d77ec560d9a96e0bac3ce870ba5aa5b340f92141"
    )
    assert [observation.msrp_value for observation in observations] == [
        240_500.0,
        258_300.0,
        218_900.0,
        305_600.0,
        276_300.0,
        291_200.0,
        256_800.0,
        340_000.0,
        328_400.0,
        379_500.0,
    ]
    assert all(observation.msrp_value not in {2_248.0, 2_414.0} for observation in observations)
    assert [observation.jato_powertrain for observation in observations].count("PHEV") == 3


@pytest.mark.parametrize(
    ("filename", "sample", "expected_price", "excluded_price"),
    (
        (
            "10_volkswagen_tiguan_pl.yaml",
            {
                "sections": [
                    {
                        "groups": [
                            {
                                "name": "Benzyna bezołowiowa",
                                "items": [
                                    {
                                        "equipmentLine": "Life",
                                        "engineFullName": "1,5 eTSI 96kW/131KM",
                                        "price": 146_390,
                                        "oldPrice": 166_390,
                                    }
                                ],
                            }
                        ]
                    }
                ]
            },
            166_390.0,
            146_390.0,
        ),
        (
            "12_volkswagen_touareg_pl.yaml",
            {
                "sections": [
                    {
                        "groups": [
                            {
                                "name": "Plug-In-Hybrid",
                                "items": [
                                    {
                                        "equipmentLine": "Elegance",
                                        "engineFullName": "3.0 V6 TFSI eHybrid",
                                        "price": 305_990,
                                    }
                                ],
                            }
                        ]
                    }
                ]
            },
            305_990.0,
            2_680.0,
        ),
        (
            "18_volkswagen_tiguan_allspace_pl.yaml",
            {
                "sections": [
                    {
                        "groups": [
                            {
                                "name": "Benzyna bezołowiowa",
                                "items": [
                                    {
                                        "equipmentLine": "Life",
                                        "engineFullName": "1.5 TSI EVO 110kW/150KM",
                                        "price": 165_390,
                                    }
                                ],
                            }
                        ]
                    }
                ]
            },
            165_390.0,
            1_626.0,
        ),
        (
            "19_volkswagen_t_roc_pl.yaml",
            {
                "sections": [
                    {},
                    {},
                    {
                        "groups": [
                            {
                                "name": "Benzyna bezołowiowa",
                                "items": [
                                    {
                                        "equipmentLine": "Life",
                                        "engineFullName": "1.0 TSI 85kW/116KM",
                                        "price": 103_490,
                                        "priceBeforeDiscount": 109_490,
                                        "oldPrice": 127_490,
                                    }
                                ],
                            }
                        ]
                    },
                ]
            },
            127_490.0,
            103_490.0,
        ),
    ),
)
def test_repaired_volkswagen_pl_profiles_use_official_catalog_json(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    sample: dict[str, object],
    expected_price: float,
    excluded_price: float,
) -> None:
    source_path = PL_SOURCE_ROOT / filename
    source = _load_source(filename)
    profile_data = _resolve_profile_raw(source_path, source)
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code=str(source["source_code"]),
            country=str(source["country"]),
            brand=str(source["brand"]),
            source_url=str(source["source_url"]),
            source_type=str(source["source_type"]),
            price_semantics=str(source["price_semantics"]),
        ),
        _build_http_json_profile(profile_data),
    )
    monkeypatch.setattr(extractor, "_fetch", lambda: sample)

    observations = extractor.extract()

    assert source["source_type"] == "official_price_list"
    assert source["extractor_type"] == "http_json"
    assert urlparse(str(source["source_url"])).hostname == "cenniki.volkswagen.pl"
    assert urlparse(str(profile_data["url"])).hostname == "cenniki.volkswagen.pl"
    assert [observation.msrp_value for observation in observations] == [expected_price]
    assert all(observation.msrp_value != excluded_price for observation in observations)
    assert observations[0].price_label == "Cena katalogowa brutto"
    assert observations[0].tax_included is True


def test_repaired_suzuki_vitara_pl_profile_selects_2026_retail_column() -> None:
    source = _load_source("25_suzuki_vitara_pl.yaml")
    profile_data = source["profile"]
    assert isinstance(profile_data, dict)

    assert source["source_url"] == "https://suzuki.pl/auto/cennik"
    assert source["source_type"] == "official_price_list"
    assert source["extractor_type"] == "scrapling"
    assert profile_data["tier"] == "stealth"
    assert profile_data["text_regex"]["source_selector"] == (
        'div.tab-content[tab-content-id="2026-232"]'
    )

    profile = _build_scrapling_profile(profile_data)
    assert profile.text_regex is not None
    entry = profile.text_regex.entry_patterns[0]
    sample_text = """
    1.4 BoosterJet mild Hybrid 2WD 6MT Premium
    697 zł
    858 zł
    107 100 zł
    89 900 zł
    1.4 BoosterJet mild Hybrid 4WD 6AT Elegance Sun
    1 347 zł
    1 657 zł
    146 100 zł
    139 900 zł
    """
    matches = list(re.finditer(entry.pattern, sample_text, flags=re.IGNORECASE | re.DOTALL))

    assert [match.group("trim") for match in matches] == [
        "1.4 BoosterJet mild Hybrid 2WD 6MT Premium",
        "1.4 BoosterJet mild Hybrid 4WD 6AT Elegance Sun",
    ]
    assert [match.group("price") for match in matches] == ["107 100", "146 100"]
    assert all(match.group("price") not in {"697", "858", "89 900", "139 900"} for match in matches)
