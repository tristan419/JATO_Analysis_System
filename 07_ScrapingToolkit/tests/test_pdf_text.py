import subprocess

import requests

from jato_scraper.base import ExtractorConfig
from jato_scraper.extractors.pdf_text import (
    PdfTextEntryPattern,
    PdfTextExtractor,
    PdfTextProfile,
)


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

    def fetch_with_curl(timeout):
        fallback_timeouts.append(timeout)
        return b"%PDF-1.7\n"

    monkeypatch.setattr(extractor, "_fetch_pdf_bytes_with_curl", fetch_with_curl)

    assert extractor._fetch_pdf_bytes() == b"%PDF-1.7\n"
    assert fallback_timeouts == [30]


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
