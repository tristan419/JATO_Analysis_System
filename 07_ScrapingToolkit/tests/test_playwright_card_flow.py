from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_playwright_profile
from jato_scraper.extractors.playwright_card_flow import (
    PlaywrightCardFlowExtractor,
    PlaywrightCardFlowProfile,
    PlaywrightError,
    _is_plausible_msrp_value,
    _is_retryable_navigation_error,
    parse_price,
)


def build_extractor() -> PlaywrightCardFlowExtractor:
    return PlaywrightCardFlowExtractor(
        ExtractorConfig(
            source_code="volkswagen_id_4_se",
            country="SE",
            brand="VOLKSWAGEN",
            source_url="https://www.volkswagen.se/sv/bygg-din-bil.html",
        ),
        PlaywrightCardFlowProfile(
            url="https://www.volkswagen.se/sv/bygg-din-bil.html/__app/30280.app",
        ),
    )


def test_retryable_navigation_error_detection() -> None:
    assert _is_retryable_navigation_error(
        PlaywrightError("Page.goto: net::ERR_CONNECTION_CLOSED")
    )
    assert not _is_retryable_navigation_error(
        PlaywrightError("Page.goto: net::ERR_CERT_AUTHORITY_INVALID")
    )


def test_goto_retries_connection_closed_once() -> None:
    extractor = build_extractor()

    class FakePage:
        def __init__(self) -> None:
            self.calls = 0

        def goto(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise PlaywrightError("Page.goto: net::ERR_CONNECTION_CLOSED")
            return object()

    page = FakePage()

    extractor._goto(page)

    assert page.calls == 2


def test_parse_price_prefers_plausible_msrp_over_model_numbers() -> None:
    assert parse_price("ID.4 Pure\n39 990 €\nMOOTTORIT (1)") == 39_990.0
    assert (
        parse_price("Comfort Business\n23\xa0668,64\xa0€\nTilavuus 1L")
        == 23_668.64
    )
    assert not _is_plausible_msrp_value(parse_price("MOOTTORIT (1 )"))


def test_extract_price_text_falls_back_when_selector_reads_step_count() -> None:
    extractor = PlaywrightCardFlowExtractor(
        ExtractorConfig(
            source_code="volkswagen_t_cross_fi",
            country="FI",
            brand="VOLKSWAGEN",
            source_url="https://www.volkswagen.fi/fi/rakenna-auto.html",
        ),
        PlaywrightCardFlowProfile(
            url="https://www.volkswagen.fi/fi/rakenna-auto.html",
            detail_price_selector='[data-testid="prices"]',
        ),
    )

    class FakeFirst:
        def inner_text(self, *, timeout: int) -> str:
            return "MOOTTORIT (1 )"

    class FakeLocator:
        first = FakeFirst()

        def count(self) -> int:
            return 1

    class FakeCard:
        def locator(self, selector: str) -> FakeLocator:
            assert selector == '[data-testid="prices"]'
            return FakeLocator()

    detail_text = "Comfort Business\n23\xa0668,64\xa0€\nMOOTTORIT (1 )\nTilavuus 1L"

    assert extractor._extract_price_text(FakeCard(), detail_text) == (
        "Comfort Business 23 668,64 € MOOTTORIT (1 ) Tilavuus 1L"
    )


def test_config_loader_builds_trim_price_ready_timeout() -> None:
    profile = _build_playwright_profile(
        {
            "url": "https://example.invalid/configurator",
            "extract_from_trim_cards": True,
            "trim_price_ready_timeout_ms": 12_000,
        }
    )

    assert profile.trim_price_ready_timeout_ms == 12_000
