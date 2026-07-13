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


def test_pdf_text_profile_accepts_preferred_curl_download() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/sealion.pdf",
            "urls": ["https://example.invalid/sealion-extra.pdf"],
            "prefer_curl_download": True,
            "entry_patterns": [],
        }
    )

    assert profile.prefer_curl_download is True
    assert profile.urls == ("https://example.invalid/sealion-extra.pdf",)


def test_pdf_text_profile_accepts_custom_headers() -> None:
    profile = _build_pdf_text_profile(
        {
            "url": "https://example.invalid/avenger.pdf",
            "headers": {
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "sl-SI,sl;q=0.9",
            },
            "entry_patterns": [],
        }
    )

    assert profile.headers == {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "sl-SI,sl;q=0.9",
    }


def test_pdf_text_extracts_text_from_primary_and_additional_urls(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="hyundai_kona_nl_draft_scrapling",
            country="荷兰",
            brand="HYUNDAI",
            source_url="https://www.hyundai.com/nl/nl/kona-family.html",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/kona-hybrid.pdf",
            urls=("https://example.invalid/kona-electric.pdf",),
        ),
    )
    fetched_urls = []

    def fake_fetch(url):
        fetched_urls.append(url)
        return b"%PDF-1.7\n"

    class FakePage:
        def __init__(self, text: str) -> None:
            self._text = text

        def extract_text(self) -> str:
            return self._text

    class FakeReader:
        def __init__(self, _blob) -> None:
            url = fetched_urls[-1]
            self.pages = [FakePage(f"text for {url}")]

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_url", fake_fetch)
    monkeypatch.setattr(pdf_text_module, "PdfReader", FakeReader)

    text = extractor._extract_text()

    assert fetched_urls == [
        "https://example.invalid/kona-hybrid.pdf",
        "https://example.invalid/kona-electric.pdf",
    ]
    assert "text for https://example.invalid/kona-hybrid.pdf" in text
    assert "text for https://example.invalid/kona-electric.pdf" in text


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


def test_pdf_text_curl_fallback_forwards_custom_headers(monkeypatch):
    extractor = PdfTextExtractor(
        ExtractorConfig(
            source_code="jeep_avenger_si_draft_scrapling",
            country="斯洛文尼亚",
            brand="JEEP",
            source_url="https://www.jeep.com/si/cenik-in-tehnicne-informacije.html",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        PdfTextProfile(
            url="https://example.invalid/avenger.pdf",
            headers={"User-Agent": "Mozilla/5.0"},
        ),
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
    assert "-H" in commands[0]
    assert "User-Agent: Mozilla/5.0" in commands[0]
