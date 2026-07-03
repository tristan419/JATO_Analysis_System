from datetime import date

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_http_json_profile
from jato_scraper.extractors.http_json import (
    FieldMapping,
    HttpJsonExtractor,
    HttpJsonProfile,
    LookupMapping,
    MinPriceGroup,
    PricingContextMapping,
    ValueFilter,
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


def test_http_json_decodes_html_entity_thousand_separator(monkeypatch):
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="kgm_korando_cz_test",
            country="捷克",
            brand="KGM",
            source_url="https://konfigurator.kgmcars.cz/",
        ),
        HttpJsonProfile(
            url="https://example.invalid/korando.json",
            fixed_model="KORANDO",
            default_currency="CZK",
            default_tax_included=True,
            field_mapping=FieldMapping(
                model="",
                trim="trim",
                price="price",
                vehicles_path="items",
            ),
        ),
    )

    monkeypatch.setattr(
        extractor,
        "_fetch",
        lambda: {"items": [{"trim": "Style", "price": "549&nbsp;900"}]},
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].msrp_value == 549_900.0


def test_http_json_joins_lookup_mapped_fields(monkeypatch):
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="citroen_c3_aircross_dk_test",
            country="DK",
            brand="CITROEN",
            source_url="https://www.citroen.dk/prislister",
        ),
        HttpJsonProfile(
            url="https://example.invalid/citroen.json",
            fixed_model="C3 AIRCROSS",
            default_currency="DKK",
            field_mapping=FieldMapping(
                model="",
                trim=(
                    LookupMapping(
                        source_path="trimId",
                        collection_path="trims",
                        key_path="id",
                        value_path="name",
                    ),
                    LookupMapping(
                        source_path="engineId",
                        collection_path="engines",
                        key_path="id",
                        value_path="name",
                    ),
                ),
                price="price",
                vehicles_path="variants",
                availability="campaignLabel",
            ),
        ),
    )

    sample = {
        "variants": [
            {
                "price": 199990,
                "trimId": 1059,
                "engineId": 276,
                "campaignLabel": "Serviceaktiveret garanti",
            }
        ],
        "trims": [{"id": 1059, "name": "YOU"}],
        "engines": [{"id": 276, "name": "Electric 113 HK"}],
    }

    monkeypatch.setattr(extractor, "_fetch", lambda: sample)
    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "C3 AIRCROSS"
    assert results[0].official_trim == "YOU / Electric 113 HK"
    assert results[0].msrp_value == 199990
    assert results[0].currency == "DKK"


def test_http_json_filters_entries_and_maps_dict_lookup(monkeypatch):
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="bmw_x1_fr_test",
            country="法国",
            brand="BMW",
            source_url="https://configure.bmw.fr/fr_FR/configure/U11/31EE",
        ),
        HttpJsonProfile(
            url="https://example.invalid/bmw-u11.json",
            fixed_model="X1",
            fixed_jato_model="X1",
            fixed_jato_powertrain="ICE",
            fixed_official_powertrain="ICE",
            copy_trim_to_jato_trim=True,
            match_confidence=0.86,
            match_reason={"kind": "official_vehicle_tree_price"},
            field_mapping=FieldMapping(
                vehicles_path=(
                    "X.modelRanges.U11.lines.BASIC_LINE.includedTransmissionVariants"
                ),
                trim=LookupMapping(
                    source_path="configuration.modelCode",
                    collection_path="X.modelRanges.U11.models",
                    value_path="phrases.fr.longDescription",
                ),
                price="prices.grossListPrice",
                currency="",
                tax_included="",
                price_label="",
                availability="configuration.modelCode",
            ),
            filters=(
                ValueFilter(path="configuration.modelCode", equals=("31EE",)),
            ),
        ),
    )

    sample = {
        "X": {
            "modelRanges": {
                "U11": {
                    "models": {
                        "11HM": {
                            "phrases": {
                                "fr": {"longDescription": "BMW iX1 eDrive20"}
                            }
                        },
                        "31EE": {
                            "phrases": {
                                "fr": {"longDescription": "BMW X1 sDrive20i"}
                            }
                        },
                    },
                    "lines": {
                        "BASIC_LINE": {
                            "includedTransmissionVariants": [
                                {
                                    "configuration": {"modelCode": "11HM"},
                                    "prices": {"grossListPrice": 46990.0},
                                },
                                {
                                    "configuration": {"modelCode": "31EE"},
                                    "prices": {"grossListPrice": 48050.0},
                                },
                            ]
                        }
                    },
                }
            }
        }
    }

    monkeypatch.setattr(extractor, "_fetch", lambda: sample)
    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "X1"
    assert results[0].official_trim == "BMW X1 sDrive20i"
    assert results[0].jato_model == "X1"
    assert results[0].jato_trim == "BMW X1 sDrive20i"
    assert results[0].jato_powertrain == "ICE"
    assert results[0].official_powertrain == "ICE"
    assert results[0].match_confidence == 0.86
    assert results[0].match_reason == {"kind": "official_vehicle_tree_price"}
    assert results[0].msrp_value == 48050.0
    assert results[0].availability_text == "31EE"


def test_http_json_groups_entries_by_min_price_before_mapping(monkeypatch):
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="suzuki_scross_hr_test",
            country="克罗地亚",
            brand="SUZUKI",
            source_url="https://auto.suzuki.hr/cars/scross-hybrid",
        ),
        HttpJsonProfile(
            url="https://example.invalid/sellingPrices",
            fixed_model="S-CROSS",
            default_currency="EUR",
            field_mapping=FieldMapping(
                vehicles_path="data.0.sellingPriceDetails",
                trim="variantCode",
                price="price2",
            ),
            min_price_group=MinPriceGroup(key="variantCode", price="price2"),
            pricing_context=PricingContextMapping(
                fields={"external_color_code": "colorCode"}
            ),
        ),
    )

    sample = {
        "data": [
            {
                "sellingPriceDetails": [
                    {
                        "variantCode": "R573",
                        "colorCode": "ZQ4",
                        "price2": 23790.49,
                    },
                    {
                        "variantCode": "R573",
                        "colorCode": "26U",
                        "price2": 23123.23,
                    },
                    {
                        "variantCode": "R5Q0",
                        "colorCode": "ZQ4",
                        "price2": 27146.83,
                    },
                ]
            }
        ]
    }

    monkeypatch.setattr(extractor, "_fetch", lambda: sample)
    results = extractor.extract()

    assert len(results) == 2
    assert results[0].official_trim == "R573"
    assert results[0].msrp_value == 23123.23
    assert results[0].raw_payload["pricingContext"]["external_color_code"] == "26U"
    assert results[1].official_trim == "R5Q0"
    assert results[1].msrp_value == 27146.83


def test_http_json_renders_today_template_in_request_values():
    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True}

    class DummySession:
        def __init__(self):
            self.headers = {}
            self.calls = []

        def post(self, url, *, json, params, timeout):
            self.calls.append(
                {
                    "url": url,
                    "json": json,
                    "params": params,
                    "timeout": timeout,
                }
            )
            return DummyResponse()

    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="bmw_template_test",
            country="法国",
            brand="BMW",
            source_url="https://configure.bmw.fr/fr_FR/configure/U11/11HM",
        ),
        HttpJsonProfile(
            url="https://example.invalid/effect-dates/{today}",
            method="POST",
            params={"order-date": "{today}"},
            body={"validityDates": {"taxDate": "{current_date}"}},
        ),
    )
    session = DummySession()
    extractor._session = session

    assert extractor._fetch() == {"ok": True}

    today = date.today().isoformat()
    assert session.calls == [
        {
            "url": f"https://example.invalid/effect-dates/{today}",
            "json": {"validityDates": {"taxDate": today}},
            "params": {"order-date": today},
            "timeout": 30,
        }
    ]


def test_http_json_adds_pricing_context_from_profile(monkeypatch):
    extractor = HttpJsonExtractor(
        ExtractorConfig(
            source_code="volvo_xc60_se_lease_json",
            country="se",
            brand="VOLVO",
            source_url="https://www.volvocars.com/se/cars/xc60/",
            price_semantics="lease_monthly",
        ),
        HttpJsonProfile(
            url="https://example.invalid/volvo-offers.json",
            fixed_model="XC60",
            default_currency="SEK",
            default_price_label="Private lease",
            field_mapping=FieldMapping(
                model="",
                trim="trimName",
                price="lease.monthlyText",
                currency="lease.currency",
                vehicles_path="offers",
            ),
            pricing_context=PricingContextMapping(
                fields={
                    "monthly_payment": "lease.monthlyText",
                    "term_months": "lease.termText",
                    "annual_mileage_limit": "lease.mileageText",
                },
                constants={
                    "price_semantics": "lease_monthly",
                    "finance_type": "private_lease",
                    "finance_currency": "SEK",
                },
            ),
        ),
    )

    monkeypatch.setattr(
        extractor,
        "_fetch",
        lambda: {
            "offers": [
                {
                    "trimName": "Ultra",
                    "lease": {
                        "monthlyText": "5 990 kr/mån",
                        "termText": "36 månader",
                        "mileageText": "15000 km/år",
                        "currency": "SEK",
                    },
                }
            ]
        },
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "XC60"
    assert results[0].official_trim == "Ultra"
    assert results[0].msrp_value == 5990.0
    assert results[0].raw_payload["pricingContext"] == {
        "price_semantics": "lease_monthly",
        "finance_type": "private_lease",
        "finance_currency": "SEK",
        "monthly_payment": 5990.0,
        "term_months": 36,
        "annual_mileage_limit": 15000,
    }


def test_config_loader_builds_http_json_lookup_mapping():
    profile = _build_http_json_profile(
        {
            "url": "https://example.invalid/citroen.json",
            "field_mapping": {
                "vehicles_path": "variants",
                "trim": [
                    {
                        "source_path": "trimId",
                        "collection_path": "trims",
                        "key_path": "id",
                        "value_path": "name",
                    }
                ],
                "price": "price",
            },
        }
    )

    trim_mapping = profile.field_mapping.trim
    assert isinstance(trim_mapping, tuple)
    assert isinstance(trim_mapping[0], LookupMapping)
    assert trim_mapping[0].source_path == "trimId"
    assert trim_mapping[0].collection_path == "trims"


def test_config_loader_builds_http_json_pricing_context_profile():
    profile = _build_http_json_profile(
        {
            "url": "https://example.invalid/volvo-offers.json",
            "field_mapping": {
                "vehicles_path": "offers",
                "trim": "trimName",
                "price": "lease.monthlyText",
            },
            "pricing_context": {
                "fields": {
                    "monthly_payment": "lease.monthlyText",
                    "term_months": "lease.termText",
                },
                "constants": {
                    "price_semantics": "lease_monthly",
                    "finance_type": "private_lease",
                },
            },
        }
    )

    assert profile.pricing_context is not None
    assert profile.pricing_context.fields == {
        "monthly_payment": "lease.monthlyText",
        "term_months": "lease.termText",
    }
    assert profile.pricing_context.constants == {
        "price_semantics": "lease_monthly",
        "finance_type": "private_lease",
    }


def test_config_loader_builds_http_json_min_price_group_profile():
    profile = _build_http_json_profile(
        {
            "url": "https://example.invalid/sellingPrices",
            "field_mapping": {
                "vehicles_path": "data.0.sellingPriceDetails",
                "trim": "variantCode",
                "price": "price2",
            },
            "min_price_group": {
                "key": "variantCode",
                "price": "price2",
            },
        }
    )

    assert profile.min_price_group is not None
    assert profile.min_price_group.key == "variantCode"
    assert profile.min_price_group.price == "price2"
