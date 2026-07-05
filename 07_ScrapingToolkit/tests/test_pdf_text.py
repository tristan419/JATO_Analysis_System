import subprocess
import sys
import types

import requests

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_pdf_text_profile
from jato_scraper.extractors.pdf_text import (
    PdfTextEntryPattern,
    PdfTextExtractor,
    PdfTextProfile,
    parse_price,
)


def test_parse_price_collapses_pdf_table_line_breaks():
    assert parse_price("3\n8 .\n1\n90,\n-") == 38_190.0


def test_parse_price_decodes_html_entity_thousand_separator():
    assert parse_price("549&nbsp;900 Kč") == 549_900.0


def test_pdf_text_extracts_entries_and_applies_price_delta(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="vw_touareg_sk_scrapling",
            country="斯洛伐克",
            brand="VOLKSWAGEN",
            source_url="https://www.vw.sk/touareg/touareg/cenniky-a-katalogy",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/touareg.pdf",
            fixed_model="TOUAREG",
            fixed_jato_model="TOUAREG",
            copy_trim_to_jato_trim=True,
            default_currency="EUR",
            default_tax_included=True,
            default_price_label="Cenníková cena s DPH",
            match_confidence=0.84,
            match_status="review_required",
            entry_patterns=(
                PdfTextEntryPattern(
                    pattern=(
                        r"RC81\*\s+Limited\s+Bonus\s+Akciová\s+cena1\s+"
                        r"\*4JL1\s+-8\s+000\s+€\s+(?P<price>\d{1,3}(?:\s\d{3})+)\s+€"
                    ),
                    official_trim="Limited",
                    official_powertrain="3.0 V6 TDI 8-st. automatická / 4x4 170 kW (231 k)",
                    jato_powertrain="ICE",
                    price_delta=8000,
                ),
                PdfTextEntryPattern(
                    pattern=(
                        r"RC83\*\s+Elegance\s+Limited\s+Bonus\s+Akciová\s+cena1\s+"
                        r"\*YJL1.*?-8\s+000\s+€\s+(?P<price>\d{1,3}(?:\s\d{3})+)\s+€"
                    ),
                    official_trim="Elegance Limited",
                    official_powertrain=(
                        "3.0 V6 TSI eHybrid 8-st. automatická / 4x4 280 kW (381 k)"
                    ),
                    jato_powertrain="PHEV",
                    price_delta=8000,
                ),
            ),
        ),
    )

    sample_text = """
    Platí od 1.4.2026
    Obj. kód
    RC81* Limited Bonus Akciová
    cena1
    *4JL1 -8 000 € 55 990 €

    Obj. kód
    RC83* Elegance Limited Bonus Akciová
    cena1
    *YJL1 3.0 V6 TSI eHybrid (plug-in hybrid) -8 000 € 79 590 €
    """
    monkeypatch.setattr(extractor, "_extract_text", lambda: sample_text)

    results = extractor.extract()

    assert len(results) == 2
    assert results[0].official_model == "TOUAREG"
    assert results[0].official_trim == "Limited"
    assert results[0].official_powertrain == (
        "3.0 V6 TDI 8-st. automatická / 4x4 170 kW (231 k)"
    )
    assert results[0].msrp_value == 63_990.0
    assert results[0].currency == "EUR"
    assert results[0].jato_model == "TOUAREG"
    assert results[0].jato_trim == "Limited"
    assert results[0].jato_powertrain == "ICE"
    assert results[0].price_label == "Cenníková cena s DPH"
    assert results[0].match_confidence == 0.84
    assert results[1].msrp_value == 87_590.0
    assert results[1].jato_powertrain == "PHEV"


def test_pdf_text_profile_accepts_legacy_curl_download_fallback() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/eqa.pdf",
            "curl_download_fallback": True,
            "entry_patterns": [],
        }
    )

    assert profile.browser_download_fallback is True


def test_pdf_text_profile_accepts_preferred_curl_download() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/sealion.pdf",
            "prefer_curl_download": True,
            "entry_patterns": [],
        }
    )

    assert profile.prefer_curl_download is True


def test_pdf_text_profile_can_ignore_environment_proxy() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/cz-price-list.pdf",
            "ignore_environment_proxy": True,
            "entry_patterns": [],
        }
    )

    assert profile.ignore_environment_proxy is True


def test_pdf_text_profile_accepts_headers() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/jeep-avenger.pdf",
            "headers": {
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.8",
            },
            "entry_patterns": [],
        }
    )

    assert profile.headers == {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.8",
    }


def test_pdf_text_applies_profile_headers_to_requests_and_curl() -> None:
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="jeep_avenger_si_draft_scrapling",
            country="斯洛文尼亚",
            brand="JEEP",
            source_url="https://example.invalid/jeep-avenger.pdf",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/jeep-avenger.pdf",
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "sl-SI,sl;q=0.9,en;q=0.8",
            },
        ),
    )

    assert extractor._session.headers["User-Agent"] == "Mozilla/5.0"
    assert (
        extractor._session.headers["Accept-Language"]
        == "sl-SI,sl;q=0.9,en;q=0.8"
    )
    assert extractor._curl_header_args() == [
        "-H",
        "User-Agent: Mozilla/5.0",
        "-H",
        "Accept-Language: sl-SI,sl;q=0.9,en;q=0.8",
    ]


def test_pdf_text_profile_accepts_direct_download_fallback() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/cz-price-list.pdf",
            "direct_download_fallback": True,
            "entry_patterns": [],
        }
    )

    assert profile.direct_download_fallback is True


def test_pdf_text_profile_accepts_multiple_urls() -> None:
    profile = _build_pdf_text_profile(
        {
            "urls": [
                "https://example.invalid/tucson-ice.pdf",
                "https://example.invalid/tucson-hybrid.pdf",
            ],
            "entry_patterns": [],
        }
    )

    assert profile.url == "https://example.invalid/tucson-ice.pdf"
    assert profile.urls == (
        "https://example.invalid/tucson-ice.pdf",
        "https://example.invalid/tucson-hybrid.pdf",
    )


def test_pdf_text_uses_curl_fallback_after_requests_timeout(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="bmw_x1_fi_draft_scrapling",
            country="芬兰",
            brand="BMW",
            source_url="https://www.bmw.fi/fi/footer/osta/hinnastot-ja-esitteet.html",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/x1.pdf",
            timeout_seconds=2,
            retry_attempts=1,
            retry_delay_seconds=0,
            browser_download_fallback=True,
        ),
    )

    def fail_request(*_args, **_kwargs):
        raise requests.ReadTimeout("read timed out")

    monkeypatch.setattr(extractor._session, "get", fail_request)
    fallback_timeouts = []

    def fail_browser(timeout, **_kwargs):
        fallback_timeouts.append(("browser", timeout))
        return None

    def fetch_with_curl(timeout, **_kwargs):
        fallback_timeouts.append(("curl", timeout))
        return b"%PDF-1.7\n"

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_browser", fail_browser)
    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fetch_with_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert fallback_timeouts == [("browser", 2), ("curl", 30)]


def test_pdf_text_uses_browser_fallback_after_requests_failure(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="peugeot_2008_si_draft_scrapling",
            country="斯洛文尼亚",
            brand="PEUGEOT",
            source_url="https://www.peugeot.si/novi-2008",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url=(
                "https://www.peugeot.si/content/dam/peugeot/slovenia/"
                "katalogi-in-ceniki/Cenik_in_cenik_opcij_2008.pdf"
            ),
            timeout_seconds=3,
            browser_download_fallback=True,
        ),
    )

    def fail_request(*_args, **_kwargs):
        raise requests.HTTPError("403 Client Error")

    browser_timeouts = []

    def fetch_with_browser(timeout, **_kwargs):
        browser_timeouts.append(timeout)
        return b"%PDF-1.7\n"

    def fail_curl(*_args, **_kwargs):
        raise AssertionError("curl should not run when browser download succeeds")

    monkeypatch.setattr(extractor._session, "get", fail_request)
    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_browser", fetch_with_browser)
    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fail_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert browser_timeouts == [3]


def test_pdf_text_browser_fallback_reads_inline_pdf_response(monkeypatch):
    class FakePlaywrightTimeoutError(Exception):
        pass

    class FakeDownloadContext:
        value = None

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            raise FakePlaywrightTimeoutError("download did not start")

    class FakeResponse:
        headers = {"content-type": "application/pdf"}

        def body(self):
            return b"%PDF-1.7\ninline"

    class FakePage:
        goto_calls = 0

        def expect_download(self, *, timeout):
            assert timeout == 5_000
            return FakeDownloadContext()

        def goto(self, url, *, wait_until, timeout):
            self.goto_calls += 1
            assert url == "https://example.invalid/inline.pdf"
            assert wait_until == "commit"
            assert timeout == 5_000
            if self.goto_calls == 1:
                return None
            return FakeResponse()

    class FakeContext:
        def new_page(self):
            return FakePage()

    class FakeBrowser:
        def new_context(self, **kwargs):
            assert kwargs["accept_downloads"] is True
            return FakeContext()

        def close(self):
            pass

    class FakeChromium:
        def launch(self, *, headless):
            assert headless is True
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

    class FakeSyncPlaywright:
        def __enter__(self):
            return FakePlaywright()

        def __exit__(self, _exc_type, _exc, _tb):
            pass

    playwright_package = types.ModuleType("playwright")
    playwright_sync_api = types.ModuleType("playwright.sync_api")
    playwright_sync_api.Error = Exception
    playwright_sync_api.TimeoutError = FakePlaywrightTimeoutError
    playwright_sync_api.sync_playwright = lambda: FakeSyncPlaywright()
    monkeypatch.setitem(sys.modules, "playwright", playwright_package)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", playwright_sync_api)

    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="jeep_avenger_si_draft_scrapling",
            country="斯洛文尼亚",
            brand="JEEP",
            source_url="https://example.invalid/inline.pdf",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(url="https://example.invalid/inline.pdf", timeout_seconds=5),
    )

    assert extractor._fetch_pdf_bytes_with_browser(5) == b"%PDF-1.7\ninline"


def test_pdf_text_prefers_curl_download_before_requests(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="byd_sealion_7_at_draft_scrapling",
            country="奥地利",
            brand="BYD",
            source_url="https://www.bydauto.at/modelle/sealion-7",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/sealion.pdf",
            timeout_seconds=2,
            prefer_curl_download=True,
        ),
    )

    def fail_request(*_args, **_kwargs):
        raise AssertionError("requests should not run when curl is preferred")

    monkeypatch.setattr(extractor._session, "get", fail_request)
    curl_timeouts = []

    def fetch_with_curl(timeout, **_kwargs):
        curl_timeouts.append(timeout)
        return b"%PDF-1.7\n"

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fetch_with_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert curl_timeouts == [30]


def test_pdf_text_direct_download_fallback_uses_proxy_first_then_direct(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="mg_zs_cz_draft_scrapling",
            country="捷克",
            brand="MG",
            source_url="https://www.mgmotor-czech.cz/UserFiles/ceniky/cenik_nove_ZS_CZ.pdf",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/zs.pdf",
            timeout_seconds=2,
            prefer_curl_download=True,
            direct_download_fallback=True,
        ),
    )
    curl_modes = []

    def fail_request(*_args, **_kwargs):
        raise requests.exceptions.ProxyError("proxy refused")

    def fetch_with_curl(timeout, *, url=None, ignore_environment_proxy=None):
        curl_modes.append(ignore_environment_proxy)
        if ignore_environment_proxy:
            return b"%PDF-1.7\n"
        return None

    monkeypatch.setattr(extractor._session, "get", fail_request)
    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fetch_with_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert curl_modes == [None, True]


def test_pdf_text_curl_fallback_keeps_curl_default_user_agent(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="bmw_x1_fi_draft_scrapling",
            country="芬兰",
            brand="BMW",
            source_url="https://www.bmw.fi/fi/footer/osta/hinnastot-ja-esitteet.html",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(url="https://example.invalid/x1.pdf"),
    )
    commands = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        output_path = command[command.index("-o") + 1]
        with open(output_path, "wb") as f:
            f.write(b"%PDF-1.7\n")
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert extractor._fetch_pdf_bytes_with_curl(30) == b"%PDF-1.7\n"
    assert "--user-agent" not in commands[0]


def test_pdf_text_curl_fallback_can_ignore_environment_proxy(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="mg_zs_cz_draft_scrapling",
            country="捷克",
            brand="MG",
            source_url="https://www.mgmotor-czech.cz/UserFiles/ceniky/cenik_nove_ZS_CZ.pdf",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/zs.pdf",
            ignore_environment_proxy=True,
        ),
    )
    captured_env = {}

    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("ALL_PROXY", "socks5h://127.0.0.1:7897")

    def fake_run(command, **kwargs):
        captured_env.update(kwargs.get("env") or {})
        output_path = command[command.index("-o") + 1]
        with open(output_path, "wb") as f:
            f.write(b"%PDF-1.7\n")
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert extractor._fetch_pdf_bytes_with_curl(30) == b"%PDF-1.7\n"
    assert "HTTP_PROXY" not in captured_env
    assert "HTTPS_PROXY" not in captured_env
    assert "ALL_PROXY" not in captured_env


def test_pdf_text_records_download_failure_in_strategy_audit(tmp_path, monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="mg_zs_cz_draft_scrapling",
            country="捷克",
            brand="MG",
            source_url="https://www.mgmotor-czech.cz/UserFiles/ceniky/cenik_nove_ZS_CZ.pdf",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(url="https://example.invalid/zs.pdf"),
    )
    extractor.run_id = "pdf-download-failure-test"
    monkeypatch.setenv("JATO_AUDIT_DIR", str(tmp_path))

    def fail_fetch(*_args, **_kwargs):
        extractor._last_fetch_error = (
            "pdf_direct_download_failed: Could not resolve host: example.invalid"
        )
        return None

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes", fail_fetch)

    assert extractor.extract() == []
    assert extractor.last_audit_event is not None
    assert extractor.last_audit_event["error"] == (
        "pdf_direct_download_failed: Could not resolve host: example.invalid"
    )
