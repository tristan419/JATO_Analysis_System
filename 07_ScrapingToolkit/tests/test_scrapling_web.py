from unittest.mock import MagicMock, patch

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_scrapling_profile
from jato_scraper.extractors.scrapling_web import (
    CssMapping,
    PricingContextMapping,
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


def test_fetch_metadata_reads_response_status_url_and_content_type() -> None:
    extractor = build_extractor()
    page = type(
        "FakePage",
        (),
        {
            "status": "403",
            "url": "https://www.tesla.com/sv_se/modely",
            "headers": {"content-type": "text/html"},
        },
    )()

    assert extractor._fetch_metadata(page) == {
        "httpStatus": 403,
        "finalUrl": "https://www.tesla.com/sv_se/modely",
        "contentType": "text/html",
    }


def test_fetch_failure_records_original_error(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("JATO_AUDIT_DIR", str(tmp_path))
    extractor = build_extractor()
    extractor.run_id = "run_fetch_failure"
    extractor._last_fetch_error = "TimeoutError: Page.goto: Timeout 30000ms exceeded"
    monkeypatch.setattr(extractor, "_fetch", lambda: None)

    assert extractor.extract() == []
    assert extractor.last_audit_event is not None
    assert (
        extractor.last_audit_event["error"]
        == "TimeoutError: Page.goto: Timeout 30000ms exceeded"
    )


def test_access_denied_body_records_audit_error(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("JATO_AUDIT_DIR", str(tmp_path))
    page = _mock_page_with_descendant_text(
        "body",
        "\n".join([
            "Access Denied",
            "You don't have permission to access http://www.tesla.com/de_at/modely on this server.",
            "https://errors.edgesuite.net/18.ab30d417.example",
        ]),
    )
    page.status = 200
    page.url = "https://www.tesla.com/de_at/modely"
    page.headers = {"content-type": "text/html"}
    extractor = build_extractor()
    extractor.run_id = "run_access_denied"
    monkeypatch.setattr(extractor, "_fetch", lambda: page)

    assert extractor.extract() == []
    assert extractor.last_audit_event is not None
    assert extractor.last_audit_event["error"].startswith("anti_bot_access_denied")
    assert extractor.last_audit_event["httpStatus"] == 200


def test_fetch_passes_browser_runtime_options(monkeypatch) -> None:
    from scrapling.fetchers import StealthyFetcher

    calls: list[dict[str, object]] = []
    page = MagicMock()

    def fake_fetch(url: str, **kwargs):
        calls.append({"url": url, **kwargs})
        return page

    monkeypatch.setattr(StealthyFetcher, "fetch", fake_fetch)
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="volvo_xc90_se",
            country="SE",
            brand="VOLVO",
            source_url="https://www.volvocars.com/se/build/xc90-hybrid/",
        ),
        ScraplingProfile(
            url="https://www.volvocars.com/se/build/xc90-hybrid/",
            tier="stealth",
            timeout_ms=45_000,
            wait_ms=500,
            load_dom=False,
            disable_resources=True,
            retries=2,
            retry_delay_seconds=0.25,
        ),
    )

    assert extractor._fetch() is page
    assert calls == [{
        "url": "https://www.volvocars.com/se/build/xc90-hybrid/",
        "headless": True,
        "network_idle": True,
        "load_dom": False,
        "timeout": 45_000,
        "wait": 500,
        "disable_resources": True,
        "retries": 2,
        "retry_delay": 0.25,
        "solve_cloudflare": False,
    }]


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
def test_text_regex_ignores_skoda_se_hero_footnote_marker(mock_fetch) -> None:
    cases = (
        (
            "skoda_kodiaq_se",
            "KODIAQ",
            r"Kodiaq\s+Selection Explore.*?Bygg din Kodiaq.*?"
            r"Från\s+1\s+(?P<price>\d{3}[\s\xa0]\d{3})\s*kr\s+"
            r"Prislista",
            "Selection Explore",
            "MHEV",
            "\n".join(
                [
                    "Kodiaq",
                    "Selection Explore",
                    "Škodas största SUV med möjlighet till 7 platser.",
                    "Bygg din Kodiaq",
                    "Boka provkörning",
                    "Jämför bilar",
                    "Från",
                    "1",
                    "409 500",
                    "kr",
                    "Prislista",
                    "Senaste erbjudanden",
                    "Pris från 409 500 kr (Ord.pris 436 900 kr)",
                ]
            ),
            409_500.0,
            1_409_500.0,
        ),
        (
            "skoda_enyaq_se",
            "ENYAQ",
            r"Enyaq\s+Solid Edition.*?Bygg din Enyaq.*?"
            r"Från\s+1\s+(?P<price>\d{3}[\s\xa0]\d{3})\s*kr\s+"
            r"Prislista",
            "Solid Edition",
            "BEV",
            "\n".join(
                [
                    "Enyaq",
                    "Solid Edition",
                    "En el-SUV med räckvidd upp till 574 km.",
                    "Bygg din Enyaq",
                    "Boka provkörning",
                    "Jämför bilar",
                    "Från",
                    "1",
                    "599 500",
                    "kr",
                    "Prislista",
                    "Privatleasing från 5 295 kr/mån",
                ]
            ),
            599_500.0,
            1_599_500.0,
        ),
    )

    for (
        source_code,
        model,
        pattern,
        trim,
        powertrain,
        body_text,
        expected_price,
        footnote_polluted_price,
    ) in cases:
        mock_fetch.return_value = _mock_page_with_text(body_text)
        extractor = ScraplingExtractor(
            ExtractorConfig(
                source_code=source_code,
                country="SE",
                brand="SKODA",
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
        assert results[0].msrp_value != footnote_polluted_price
        assert results[0].currency == "SEK"


@patch.object(ScraplingExtractor, "_fetch")
def test_text_regex_finance_named_groups_feed_pricing_context(
    mock_fetch,
) -> None:
    mock_fetch.return_value = _mock_page_with_text(
        "\n".join(
            [
                "Enyaq",
                "Solid Edition",
                "En el-SUV med räckvidd upp till 574 km.",
                "Bygg din Enyaq",
                "Boka provkörning",
                "Jämför bilar",
                "Från",
                "1",
                "599 500",
                "kr",
                "Prislista",
                "Privatleasing från 5 295 kr/mån",
            ]
        )
    )
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="skoda_enyaq_se",
            country="SE",
            brand="SKODA",
            source_url="https://example.com",
        ),
        ScraplingProfile(
            url="https://example.com",
            text_regex=TextRegexMapping(
                source_selector="body",
                entry_patterns=(
                    TextRegexEntryPattern(
                        pattern=(
                            r"Enyaq\s+Solid Edition.*?"
                            r"Bygg din Enyaq.*?"
                            r"Från\s+1\s+"
                            r"(?P<price>\d{3}[\s\xa0]\d{3})"
                            r"\s*kr\s+Prislista.*?"
                            r"Privatleasing\s+från\s+"
                            r"(?P<monthly_payment>\d{1,2}[\s\xa0]\d{3})"
                            r"\s*kr/mån"
                        ),
                        official_trim="Solid Edition",
                        official_powertrain="BEV",
                    ),
                ),
            ),
            pricing_context=PricingContextMapping(
                fields={
                    "monthly_payment": "regexGroups.monthly_payment",
                },
                constants={
                    "price_semantics": "lease_monthly",
                    "finance_type": "private_lease",
                    "finance_currency": "SEK",
                },
            ),
            default_currency="SEK",
            default_price_label="Rekommenderat cirkapris inkl. moms",
            fixed_model="ENYAQ",
            fixed_jato_model="ENYAQ",
            fixed_jato_powertrain=None,
            copy_trim_to_jato_trim=True,
            confidence_rules={
                "base": 0.28,
                "fixed_model_bonus": 0.18,
                "fixed_jato_model_bonus": 0.12,
                "model_rule_bonus": 0.12,
                "trim_present_bonus": 0.1,
                "copy_trim_to_jato_trim_bonus": 0.09,
                "parsed_price_text_bonus": 0.03,
                "currency_bonus": 0.01,
                "price_label_bonus": 0.02,
                "trim_keyword_bonuses": [
                    {
                        "key": "trim_keyword_solid_edition",
                        "label": "Trim keyword matched: Solid Edition",
                        "keyword": "Solid Edition",
                        "delta": 0.04,
                    },
                ],
                "price_band_bonuses": [
                    {
                        "key": "price_band_entry",
                        "label": "Entry price band matched",
                        "min": 550000,
                        "max": 650000,
                        "delta": 0.05,
                    },
                    {
                        "key": "price_band_mid",
                        "label": "Mid price band matched",
                        "min": 650001,
                        "max": 800000,
                        "delta": 0.01,
                    },
                    {
                        "key": "price_band_high",
                        "label": "High price band matched",
                        "min": 800001,
                        "delta": 0.03,
                    },
                ],
                "powertrain_bonuses": [
                    {
                        "key": "powertrain_bev",
                        "label": "Powertrain matched: BEV",
                        "powertrain": "BEV",
                        "delta": 0.03,
                    },
                ],
                "clamp_min": 0.0,
                "clamp_max": 1.0,
            },
            auto_accept_gates={
                "review_threshold": 0.95,
                "semi_auto_threshold": 0.98,
                "require_powertrain_match": True,
                "force_review_if_powertrain_missing": True,
                "force_review_if_powertrain_ambiguous": True,
                "force_review_for_special_edition": True,
            },
        ),
    )

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].msrp_value == 599_500.0
    assert results[0].raw_payload["regexGroups"]["monthly_payment"] == "5 295"
    assert results[0].raw_payload["pricingContext"] == {
        "price_semantics": "lease_monthly",
        "finance_type": "private_lease",
        "finance_currency": "SEK",
        "monthly_payment": 5295.0,
    }
    assert results[0].match_confidence == 0.95
    assert results[0].match_status == "auto_accepted"
    gate = results[0].match_reason["autoAcceptGate"]
    assert gate["tier"] == "semi_auto"
    assert gate["finalStatus"] == "auto_accepted"


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


def test_config_loader_builds_scrapling_pricing_context_profile() -> None:
    profile = _build_scrapling_profile(
        {
            "url": "https://example.com",
            "pricing_context": {
                "fields": {
                    "monthly_payment": "lease.monthlyText",
                    "term_months": "lease.termText",
                },
                "constants": {
                    "price_semantics": "lease_monthly",
                    "finance_type": "private_lease",
                    "finance_currency": "SEK",
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
        "finance_currency": "SEK",
    }


def test_build_observation_adds_pricing_context_from_profile() -> None:
    extractor = ScraplingExtractor(
        ExtractorConfig(
            source_code="volvo_xc60_se_lease",
            country="SE",
            brand="Volvo",
            source_url="https://example.com",
        ),
        ScraplingProfile(
            url="https://example.com",
            pricing_context=PricingContextMapping(
                fields={
                    "monthly_payment": "lease.monthlyText",
                    "term_months": "lease.termText",
                },
                constants={
                    "price_semantics": "lease_monthly",
                    "finance_type": "private_lease",
                    "finance_currency": "SEK",
                },
            ),
            default_currency="SEK",
            fixed_model="XC60",
            fixed_jato_model="XC60",
            fixed_jato_powertrain="PHEV",
            copy_trim_to_jato_trim=True,
        ),
    )

    observation = extractor._build_observation(
        official_model="XC60",
        official_trim="Ultra",
        msrp_value=5990,
        currency="SEK",
        raw_payload={
            "priceText": "5 990 kr/mån",
            "lease": {
                "monthlyText": "5 990 kr/mån",
                "termText": "36 månader",
            },
        },
    )

    assert observation is not None
    assert observation.raw_payload["pricingContext"] == {
        "price_semantics": "lease_monthly",
        "finance_type": "private_lease",
        "finance_currency": "SEK",
        "monthly_payment": 5990.0,
        "term_months": 36,
    }


def test_config_loader_builds_scrapling_browser_runtime_options() -> None:
    profile = _build_scrapling_profile(
        {
            "url": "https://www.volvocars.com/se/build/xc90-hybrid/",
            "tier": "stealth",
            "timeout_ms": 45000,
            "wait_ms": 500,
            "load_dom": False,
            "disable_resources": True,
            "retries": 2,
            "retry_delay_seconds": 0.25,
        }
    )

    assert profile.timeout_ms == 45_000
    assert profile.wait_ms == 500
    assert profile.load_dom is False
    assert profile.disable_resources is True
    assert profile.retries == 2
    assert profile.retry_delay_seconds == 0.25
