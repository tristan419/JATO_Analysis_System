from jato_scraper.base import ExtractorConfig
from jato_scraper.extractors.playwright_card_flow import (
    PlaywrightCardFlowExtractor,
    PlaywrightCardFlowProfile,
    PlaywrightError,
    _is_retryable_navigation_error,
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
