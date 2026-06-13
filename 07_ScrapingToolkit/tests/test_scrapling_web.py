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


def _mock_page_with_descendant_text(selector: str, text: str) -> MagicMock:
    page = MagicMock()
    element = MagicMock()
    element.css.return_value = MagicMock()
    element.css.return_value.get.return_value = ""
    element.get.return_value = ""
    text_result = MagicMock()
    text_result.getall.return_value = text.split("\n")

    def _css_side_effect(query: str):
        if query == f"{selector} ::text":
            return text_result
        if query == selector:
            return [element]
        return MagicMock()

    page.css.side_effect = _css_side_effect
    return page


def _mock_page_with_descendant_text_and_html(
    selector: str,
    text: str,
    html: str,
) -> MagicMock:
    page = MagicMock()
    element = MagicMock()
    text_result = MagicMock()
    text_result.getall.return_value = text.split("\n")
    element.css.return_value = text_result
    element.get.return_value = html

    def _css_side_effect(query: str):
        if query == f"{selector} ::text":
            return text_result
        if query == selector:
            return [element]
        return MagicMock()

    page.css.side_effect = _css_side_effect
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


@patch.object(ScraplingExtractor, "_fetch")
def test_text_regex_matches_exact_hyundai_model_price_raw(mock_fetch) -> None:
    mock_fetch.return_value = _mock_page_with_text(
        (
            '{"models":['
            '{"name":"INSTER","url":"/modeller/inster","priceRaw":"174995",'
            '"priceCurrency":"DKK","isCampaign":false},'
            '{"name":"IONIQ 5","url":"/modeller/ioniq-5",'
            '"price":"279.995 kr.","isCampaign":false},'
            '{"name":"IONIQ 5","url":"/modeller/ioniq-5",'
            '"image":{"altText":"Hyundai IONIQ 5"},'
            '"pimId":"HY_IONIQ5_NE_DK_IONIQ5_MY27",'
            '"price":"279995","priceRaw":"279995",'
            '"priceCurrency":"DKK","priceFormatted":"279.995 kr.",'
            '"isCampaign":false},'
            '{"name":"IONIQ 5 N","url":"/modeller/ioniq-5-n",'
            '"priceRaw":"539995","priceCurrency":"DKK",'
            '"isCampaign":false}'
            "]}"
        )
    )
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="hyundai_ioniq_5_dk",
            country="DK",
            brand="Hyundai",
            source_url="https://example.com",
        ),
        ScraplingProfile(
            url="https://example.com",
            text_regex=TextRegexMapping(
                source_selector="script",
                entry_patterns=(
                    TextRegexEntryPattern(
                        pattern=(
                            r'"name":"IONIQ 5","url":"/modeller/ioniq-5",'
                            r'"price":"(?P<price>\d{3}\.\d{3})\s*kr\.",'
                            r'"isCampaign":false'
                        ),
                        official_trim="Entry",
                        official_powertrain="BEV",
                    ),
                    TextRegexEntryPattern(
                        pattern=(
                            r'"name":"IONIQ 5".{0,700}?"url":"/modeller/ioniq-5"'
                            r'.{0,1400}?"priceRaw":"(?P<price>\d+)"'
                            r'.{0,200}?"priceCurrency":"DKK"'
                            r'.{0,200}?"isCampaign":false'
                        ),
                        official_trim="Entry",
                        official_powertrain="BEV",
                    ),
                ),
            ),
            default_currency="DKK",
            fixed_model="IONIQ 5",
            fixed_jato_model="IONIQ 5",
            fixed_jato_powertrain="BEV",
            copy_trim_to_jato_trim=True,
        ),
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "IONIQ 5"
    assert results[0].official_trim == "Entry"
    assert results[0].msrp_value == 279_995.0
    assert results[0].currency == "DKK"
    assert results[0].jato_model == "IONIQ 5"
    assert results[0].jato_powertrain == "BEV"


@patch.object(ScraplingExtractor, "_fetch")
def test_text_regex_ignores_kia_se_global_menu_price(mock_fetch) -> None:
    global_menu_text = (
        "Kia modellprogram Erbjudanden "
        "Rek. ca pris från 200 500 kr "
        "Detta pris hör till en annan menybil."
    )
    cases = (
        (
            "kia_ev3_se",
            "EV3",
            r"Kia EV3 kostar.*?Rek\s*ca\s*pris\s*från\s+"
            r"(?P<price>\d{3}[\s\xa0]\d{3})\s*kr",
            "Entry",
            "BEV",
            "Här nedan kan du se vad de olika modellversionerna av Kia EV3 "
            "kostar. Rek ca pris från 434\xa0900 kr",
            434_900.0,
        ),
        (
            "kia_sportage_se",
            "SPORTAGE",
            r"Nya Kia Sportage Plug-In Hybrid.*?Rek\s*ca\s*pris\s*från\s+"
            r"(?P<price>\d{3}[\s\xa0]\d{3})\s*kr",
            "Plug-In Hybrid",
            "PHEV",
            "Nya Kia Sportage Plug-In Hybrid erbjuder hög komfort. "
            "Rek ca pris från 508\xa0900 kr",
            508_900.0,
        ),
        (
            "kia_ev9_se",
            "EV9",
            r"Helelektriska Kia EV9.*?Rek\s*ca\s*pris\s*från\s+"
            r"(?P<price>\d{3}[\s\xa0]\d{3})\s*kr",
            "Entry",
            "BEV",
            "Helelektriska Kia EV9 har plats för familjen. "
            "Rek ca pris från 701\xa0900 kr",
            701_900.0,
        ),
        (
            "kia_ev6_se",
            "EV6",
            r"Nya Kia EV6\..*?Rek\s*ca\s*pris\s*från\s+"
            r"(?P<price>\d{3}[\s\xa0]\d{3})\s*kr",
            "Entry",
            "BEV",
            "Nya Kia EV6. Rek ca pris från 596\xa0400 kr",
            596_400.0,
        ),
    )

    for (
        source_code,
        model,
        pattern,
        trim,
        powertrain,
        model_page_text,
        expected_price,
    ) in cases:
        mock_fetch.return_value = _mock_page_with_text(
            f"{global_menu_text}\n{model_page_text}"
        )
        extractor = ScraplingExtractor(
            ExtractorConfig(
                source_code=source_code,
                country="SE",
                brand="Kia",
                source_url="https://example.com",
            ),
            ScraplingProfile(
                url="https://example.com",
                text_regex=TextRegexMapping(
                    source_selector="body",
                    entry_patterns=(
                        TextRegexEntryPattern(
                            pattern=pattern,
                            official_trim=trim,
                            official_powertrain=powertrain,
                        ),
                    ),
                ),
                default_currency="SEK",
                fixed_model=model,
                fixed_jato_model=model,
                fixed_jato_powertrain=powertrain,
                copy_trim_to_jato_trim=True,
            ),
        )

        results = extractor.extract()

        assert len(results) == 1
        assert results[0].official_model == model
        assert results[0].official_trim == trim
        assert results[0].msrp_value == expected_price
        assert results[0].currency == "SEK"
        assert results[0].msrp_value != 200_500.0


@patch.object(ScraplingExtractor, "_fetch")
def test_text_regex_extracts_descendant_body_text(mock_fetch) -> None:
    mock_fetch.return_value = _mock_page_with_descendant_text(
        "body",
        "\n".join(
            [
                "BMW iX1",
                "iX1 eDrive20 M Sport",
                "Drivkraft",
                "El",
                "Vejl. Pris kr.",
                "369.900",
            ]
        ),
    )
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="bmw_ix1_dk",
            country="DK",
            brand="BMW",
            source_url="https://example.com",
        ),
        ScraplingProfile(
            url="https://example.com",
            text_regex=TextRegexMapping(
                source_selector="body",
                entry_patterns=(
                    TextRegexEntryPattern(
                        pattern=(
                            r"iX1 eDrive20 M Sport.{0,500}?"
                            r"Vejl\. Pris kr\.\s+"
                            r"(?P<price>\d{3}\.\d{3})"
                        ),
                        official_trim="eDrive20 M Sport",
                        official_powertrain="BEV",
                    ),
                ),
            ),
            default_currency="DKK",
            fixed_model="IX1",
            fixed_jato_model="IX1",
            fixed_jato_powertrain="BEV",
            copy_trim_to_jato_trim=True,
        ),
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "IX1"
    assert results[0].official_trim == "eDrive20 M Sport"
    assert results[0].msrp_value == 369_900.0


@patch.object(ScraplingExtractor, "_fetch")
def test_text_regex_can_include_selected_element_html(mock_fetch) -> None:
    mock_fetch.return_value = _mock_page_with_descendant_text_and_html(
        "model-page-wrapper-component",
        "MGS5\n${button.modelVersionName}",
        (
            "<model-page-wrapper-component "
            "initial-model=\"mgs5\" "
            ":hero-section='{\"headerSubtitle\":\"&lt;h2&gt;"
            "Fra kr. 294 900,-&lt;sup&gt;(3)&lt;/sup&gt;"
            "&lt;/h2&gt;\"}'></model-page-wrapper-component>"
        ),
    )
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="mg_s5_no",
            country="NO",
            brand="MG",
            source_url="https://example.com",
        ),
        ScraplingProfile(
            url="https://example.com",
            text_regex=TextRegexMapping(
                source_selector="model-page-wrapper-component",
                include_element_html=True,
                entry_patterns=(
                    TextRegexEntryPattern(
                        pattern=r"Fra kr\. (?P<price>\d{3}\s\d{3}),-",
                        official_trim="Comfort",
                        official_powertrain="BEV",
                    ),
                ),
            ),
            default_currency="NOK",
            fixed_model="S5",
            fixed_jato_model="S5",
            fixed_jato_powertrain="BEV",
            copy_trim_to_jato_trim=True,
        ),
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "S5"
    assert results[0].official_trim == "Comfort"
    assert results[0].msrp_value == 294_900.0


def test_config_loader_builds_scrapling_text_regex_profile() -> None:
    profile = _build_scrapling_profile(
        {
            "url": "https://example.com",
            "text_regex": {
                "source_selector": "script",
                "include_element_html": True,
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
    assert profile.text_regex.include_element_html is True
    assert len(profile.text_regex.entry_patterns) == 1
    assert profile.text_regex.entry_patterns[0].official_trim == "Active"
