from unittest.mock import MagicMock, patch

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_scrapling_profile
from jato_scraper.extractors.scrapling_web import (
    CssMapping,
    ScraplingExtractor,
    ScraplingProfile,
    TextRegexEntryPattern,
    TextRegexMapping,
)


def build_extractor() -> ScraplingExtractor:
    return ScraplingExtractor(
        ExtractorConfig(
            source_code="test_source",
            country="SE",
            brand="Volkswagen",
            source_url="https://example.com",
        ),
        ScraplingProfile(url="https://example.com"),
    )


def test_resolve_json_path_walks_graph_lists() -> None:
    extractor = build_extractor()

    payload = {
        "@graph": [
            {
                "@type": "BreadcrumbList",
                "itemListElement": [],
            },
            {
                "@type": "Car",
                "offers": [
                    {"name": "ID.4 Pro", "price": 499000},
                    {"name": "ID.4 GTX", "price": 579000},
                ],
            },
        ]
    }

    resolved = extractor._resolve_json_path(payload, "offers")

    assert resolved == [
        {"name": "ID.4 Pro", "price": 499000},
        {"name": "ID.4 GTX", "price": 579000},
    ]


def test_resolve_json_path_inherits_parent_fields() -> None:
    extractor = build_extractor()

    payload = {
        "vehicle": {
            "name": "Volkswagen ID.4",
            "model": "ID.4",
            "description": "Battery electric SUV",
            "offers": [
                {"price": 499000, "priceCurrency": "SEK"},
                {
                    "price": 579000,
                    "priceCurrency": "SEK",
                    "name": "Volkswagen ID.4 GTX",
                },
            ],
        }
    }

    resolved = extractor._resolve_json_path(payload, "vehicle.offers")

    assert resolved == [
        {
            "name": "Volkswagen ID.4",
            "model": "ID.4",
            "description": "Battery electric SUV",
            "price": 499000,
            "priceCurrency": "SEK",
        },
        {
            "name": "Volkswagen ID.4 GTX",
            "model": "ID.4",
            "description": "Battery electric SUV",
            "price": 579000,
            "priceCurrency": "SEK",
        },
    ]


def _mock_page_with_css(cards: list[dict[str, str | None]]) -> MagicMock:
    page = MagicMock()
    containers = []
    for card in cards:
        el = MagicMock()

        def _css_side_effect(selector, _card=card):
            mock_result = MagicMock()
            if "model-name" in selector:
                mock_result.get.return_value = _card.get("model")
            elif "trim-name" in selector:
                mock_result.get.return_value = _card.get("trim")
            elif "price" in selector:
                mock_result.get.return_value = _card.get("price")
            else:
                mock_result.get.return_value = None
            return mock_result

        el.css = _css_side_effect
        containers.append(el)

    page.css.return_value = containers
    return page


def _mock_page_with_text(text: str) -> MagicMock:
    page = MagicMock()
    element = MagicMock()
    text_result = MagicMock()
    text_result.get.return_value = text
    element.css.return_value = text_result
    page.css.return_value = [element]
    return page


@patch.object(ScraplingExtractor, "_fetch")
def test_css_extract_honors_include_if_text_contains(mock_fetch) -> None:
    mock_fetch.return_value = _mock_page_with_css(
        [
            {"model": "Tiguan", "trim": "", "price": "Od 39.870,00 € z DDV"},
            {"model": "Taigo", "trim": "", "price": "Od 23.132,00 € z DDV"},
        ]
    )
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="vw_si_tiguan",
            country="SI",
            brand="Volkswagen",
            source_url="https://www.volkswagen.si/modeli-in-konfigurator/vsi-modeli",
        ),
        ScraplingProfile(
            url="https://www.volkswagen.si/modeli-in-konfigurator/vsi-modeli",
            css=CssMapping(
                vehicle_container="article[aria-labelledby]",
                model=".model-name::text",
                trim=".trim-name::text",
                price=".price::text",
                include_if_text_contains=("tiguan",),
            ),
            default_currency="EUR",
            fixed_model="TIGUAN",
            fixed_jato_model="TIGUAN",
            copy_trim_to_jato_trim=True,
        ),
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "TIGUAN"
    assert results[0].msrp_value == 39_870.0


@patch.object(ScraplingExtractor, "_fetch")
def test_text_regex_extracts_price_list_script_text(mock_fetch) -> None:
    mock_fetch.return_value = _mock_page_with_text(
        "Modelprogram Active 57,7 kWh SUV 4x2 "
        "Vejl. udsalgspris inkl. lev.omk. 299.990 "
        "Style 319.990"
    )
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="toyota_bz4x_dk",
            country="DK",
            brand="Toyota",
            source_url="https://example.com",
        ),
        ScraplingProfile(
            url="https://example.com",
            text_regex=TextRegexMapping(
                source_selector="script",
                entry_patterns=(
                    TextRegexEntryPattern(
                        pattern=(
                            r"Active 57,7 kWh\s+SUV 4x2.*?"
                            r"Vejl\. udsalgspris inkl\. lev\.omk\.\s+"
                            r"(?P<price>\d{3}\.\d{3})"
                        ),
                        official_trim="Active 57,7 kWh",
                        official_powertrain="BEV",
                    ),
                ),
            ),
            default_currency="DKK",
            fixed_model="BZ4X",
            fixed_jato_model="BZ4X",
            fixed_jato_powertrain="BEV",
            copy_trim_to_jato_trim=True,
        ),
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "BZ4X"
    assert results[0].official_trim == "Active 57,7 kWh"
    assert results[0].msrp_value == 299_990.0
    assert results[0].currency == "DKK"
    assert results[0].jato_powertrain == "BEV"


def test_config_loader_builds_scrapling_text_regex_profile() -> None:
    profile = _build_scrapling_profile(
        {
            "url": "https://example.com",
            "text_regex": {
                "source_selector": "script",
                "entry_patterns": [
                    {
                        "pattern": r"Active\s+(?P<price>\d{3}\.\d{3})",
                        "official_trim": "Active",
                        "official_powertrain": "HEV",
                    }
                ],
            },
        }
    )

    assert profile.text_regex is not None
    assert profile.text_regex.source_selector == "script"
    assert len(profile.text_regex.entry_patterns) == 1
    assert profile.text_regex.entry_patterns[0].official_trim == "Active"
