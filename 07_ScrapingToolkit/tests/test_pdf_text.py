import subprocess
from types import SimpleNamespace

import requests

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_pdf_text_profile
from jato_scraper.extractors import pdf_text as pdf_text_module
from jato_scraper.extractors.pdf_text import (
    PdfTextEntryPattern,
    PdfTextExtractor,
    PdfTextProfile,
    parse_price,
)


def test_parse_price_collapses_pdf_table_line_breaks():
    assert parse_price("3\n8 .\n1\n90,\n-") == 38_190.0


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

    assert profile.curl_download_fallback is True
    assert profile.browser_download_fallback is False


def test_pdf_text_profile_accepts_browser_download_fallback() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/2008.pdf",
            "browser_download_fallback": True,
            "entry_patterns": [],
        }
    )

    assert profile.browser_download_fallback is True
    assert profile.curl_download_fallback is False


def test_pdf_text_browser_fallback_reads_inline_pdf_response(monkeypatch) -> None:
    state = SimpleNamespace(context_closed=False, browser_closed=False)
    pdf_blob = b"%PDF-1.7\ninline response"

    class FakeResponse:
        def body(self):
            return pdf_blob

    class FakePage:
        def set_default_timeout(self, timeout):
            assert timeout == 30_000

        def on(self, event, callback):
            assert event == "download"
            assert callable(callback)

        def goto(self, url, *, wait_until, timeout):
            assert url == "https://example.invalid/inline.pdf"
            assert wait_until == "commit"
            assert timeout == 30_000
            return FakeResponse()

    class FakeContext:
        def new_page(self):
            return FakePage()

        def close(self):
            state.context_closed = True

    class FakeBrowser:
        def new_context(self, **kwargs):
            assert kwargs["accept_downloads"] is True
            return FakeContext()

        def close(self):
            state.browser_closed = True

    class FakeChromium:
        def launch(self, *, headless):
            assert headless is True
            return FakeBrowser()

    class FakePlaywrightManager:
        def __enter__(self):
            return SimpleNamespace(chromium=FakeChromium())

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(
        pdf_text_module,
        "sync_playwright",
        FakePlaywrightManager,
    )
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="inline_pdf",
            country="波兰",
            brand="SEAT",
            source_url="https://example.invalid/inline.pdf",
        ),
        PdfTextProfile(
            url="https://example.invalid/inline.pdf",
            browser_download_fallback=True,
        ),
    )

    assert extractor._fetch_pdf_bytes_with_browser(30) == pdf_blob
    assert state.context_closed is True
    assert state.browser_closed is True


def test_pdf_text_browser_fallback_reads_attachment_download(
    monkeypatch,
    tmp_path,
) -> None:
    state = SimpleNamespace(
        browser_closed=False,
        context_closed=False,
        download_callback=None,
    )
    pdf_path = tmp_path / "price-list.pdf"
    pdf_path.write_bytes(b"%PDF-1.7\nattachment")

    class FakeDownload:
        def path(self):
            return str(pdf_path)

    class FakePage:
        def set_default_timeout(self, _timeout):
            return None

        def on(self, event, callback):
            assert event == "download"
            state.download_callback = callback

        def goto(self, *_args, **_kwargs):
            raise RuntimeError("Download is starting")

        def wait_for_timeout(self, timeout):
            assert timeout == 100
            state.download_callback(FakeDownload())

    class FakeContext:
        def new_page(self):
            return FakePage()

        def close(self):
            state.context_closed = True

    class FakeBrowser:
        def new_context(self, **_kwargs):
            return FakeContext()

        def close(self):
            state.browser_closed = True

    class FakeChromium:
        def launch(self, **_kwargs):
            return FakeBrowser()

    class FakePlaywrightManager:
        def __enter__(self):
            return SimpleNamespace(chromium=FakeChromium())

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(pdf_text_module, "sync_playwright", FakePlaywrightManager)
    monkeypatch.setattr(pdf_text_module, "PlaywrightError", RuntimeError)
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="attachment_pdf",
            country="波兰",
            brand="SEAT",
            source_url="https://example.invalid/attachment.pdf",
        ),
        PdfTextProfile(
            url="https://example.invalid/attachment.pdf",
            browser_download_fallback=True,
        ),
    )

    assert extractor._fetch_pdf_bytes_with_browser(30) == (
        b"%PDF-1.7\nattachment"
    )
    assert state.context_closed is True
    assert state.browser_closed is True


def test_pdf_text_profile_accepts_preferred_curl_download() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/sealion.pdf",
            "prefer_curl_download": True,
            "entry_patterns": [],
        }
    )

    assert profile.prefer_curl_download is True


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
            curl_download_fallback=True,
        ),
    )

    def fail_request(*_args, **_kwargs):
        raise requests.ReadTimeout("read timed out")

    monkeypatch.setattr(extractor._session, "get", fail_request)
    fallback_timeouts = []

    def fetch_with_curl(timeout, url=None):
        fallback_timeouts.append((timeout, url))
        return b"%PDF-1.7\n"

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fetch_with_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert fallback_timeouts == [(30, "https://example.invalid/x1.pdf")]


def test_pdf_text_uses_browser_fallback_after_requests_timeout(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="peugeot_2008_it_draft_scrapling",
            country="意大利",
            brand="PEUGEOT",
            source_url="https://www.peugeot.it/modelli/2008.html",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/2008.pdf",
            timeout_seconds=2,
            retry_attempts=1,
            retry_delay_seconds=0,
            browser_download_fallback=True,
        ),
    )

    def fail_request(*_args, **_kwargs):
        raise requests.ReadTimeout("read timed out")

    monkeypatch.setattr(extractor._session, "get", fail_request)
    browser_timeouts = []

    def fetch_with_browser(timeout, url=None):
        browser_timeouts.append((timeout, url))
        return b"%PDF-1.7\n"

    def fail_curl(*_args, **_kwargs):
        raise AssertionError("curl should not run after successful browser fallback")

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_browser", fetch_with_browser)
    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fail_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert browser_timeouts == [(30, "https://example.invalid/2008.pdf")]


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

    def fetch_with_curl(timeout, url=None):
        curl_timeouts.append((timeout, url))
        return b"%PDF-1.7\n"

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fetch_with_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert curl_timeouts == [(30, "https://example.invalid/sealion.pdf")]


def test_pdf_text_uses_poppler_when_pypdf_returns_blank_text(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="kgm_korando_hu_draft_scrapling",
            country="匈牙利",
            brand="KGM",
            source_url="https://example.invalid/korando.pdf",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(url="https://example.invalid/korando.pdf"),
    )

    class FakePage:
        def extract_text(self):
            return ""

    class FakeReader:
        def __init__(self, _stream):
            self.pages = [FakePage()]

    commands = []

    def fake_run(command, **kwargs):
        commands.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=b"Lista\xc3\xa1r 9 299 000 Ft", stderr=b"")

    monkeypatch.setattr(pdf_text_module, "PdfReader", FakeReader)
    monkeypatch.setattr(pdf_text_module.shutil, "which", lambda name: "/usr/bin/pdftotext")
    monkeypatch.setattr(pdf_text_module.subprocess, "run", fake_run)

    assert extractor._extract_text_from_pdf_bytes(b"%PDF-1.7\n") == "Listaár 9 299 000 Ft"
    assert commands[0][0][:4] == ["/usr/bin/pdftotext", "-layout", "-enc", "UTF-8"]


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
