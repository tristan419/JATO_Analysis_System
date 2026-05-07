from jato_scraper.base import ExtractorConfig
from jato_scraper.extractors.http_json import (
    FieldMapping,
    HttpJsonExtractor,
    HttpJsonProfile,
)


def test_http_json_flattens_nested_items_and_joins_trim_fields(monkeypatch):
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="vw_touareg_pl_test",
            country="波兰",
            brand="VOLKSWAGEN",
            source_url="https://cenniki.volkswagen.pl/Touareg.html",
        ),
        HttpJsonProfile(
            url="https://example.invalid/touareg.json",
            fixed_model="TOUAREG",
            default_currency="PLN",
            default_tax_included=True,
            default_price_label="Cena katalogowa brutto",
            field_mapping=FieldMapping(
                model="",
                trim=("equipmentLine", "engineFullName"),
                price="price",
                vehicles_path="sections.0.groups",
                items_path="items",
                availability="name",
            ),
        ),
    )

    sample = {
        "sections": [
            {
                "groups": [
                    {
                        "name": "Plug-In-Hybrid",
                        "items": [
                            {
                                "equipmentLine": "Elegance",
                                "engineFullName": "3.0 V6 TFSI eHybrid 4MOTION | 381KM",
                                "price": 305990,
                            },
                            {
                                "equipmentLine": "R Final Edition",
                                "engineFullName": "3.0 V6 TFSI eHybrid 4MOTION | 462KM",
                                "price": 357190,
                            },
                        ],
                    }
                ]
            }
        ]
    }

    monkeypatch.setattr(extractor, "_fetch", lambda: sample)
    results = extractor.extract()

    assert len(results) == 2
    assert results[0].official_model == "TOUAREG"
    assert results[0].official_trim == (
        "Elegance / 3.0 V6 TFSI eHybrid 4MOTION | 381KM"
    )
    assert results[0].msrp_value == 305990
    assert results[0].currency == "PLN"
    assert results[0].availability_text == "Plug-In-Hybrid"
    assert results[0].source_url == "https://cenniki.volkswagen.pl/Touareg.html"


def test_http_json_supports_list_indexes_in_paths():
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="vw_tiguan_allspace_pl_test",
            country="波兰",
            brand="VOLKSWAGEN",
            source_url="https://cenniki.volkswagen.pl/Tiguan-Allspace.html",
        ),
        HttpJsonProfile(
            url="https://example.invalid/allspace.json",
            fixed_model="TIGUAN ALLSPACE",
            default_currency="PLN",
            default_tax_included=True,
            field_mapping=FieldMapping(
                model="",
                trim=("equipmentLine", "engineFullName"),
                price="price",
                vehicles_path="sections.0.groups.1.items",
            ),
        ),
    )

    sample = {
        "sections": [
            {
                "groups": [
                    {"items": []},
                    {
                        "items": [
                            {
                                "equipmentLine": "R-Line",
                                "engineFullName": (
                                    "2.0 TDI SCR | 147kW/200KM | automatyczna, DSG 7-stopniowa"
                                ),
                                "price": 229490,
                            }
                        ]
                    },
                ]
            }
        ]
    }

    extractor._navigate(sample)
    results = extractor._map(extractor._navigate(sample) or [])

    assert len(results) == 1
    assert results[0].official_model == "TIGUAN ALLSPACE"
    assert results[0].official_trim.startswith("R-Line / 2.0 TDI SCR")
