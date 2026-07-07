import subprocess

import requests

from jato_scraper.base import ExtractorConfig
from jato_scraper.config_loader import _build_http_text_profile
from jato_scraper.extractors.http_text import (
    HttpTextEntryPattern,
    HttpTextExtractor,
    HttpTextProfile,
)


def test_http_text_extracts_embedded_configurator_price(monkeypatch):
    extractor = HttpTextExtractor(
        ExtractorConfig(
            source_code="mazda_cx_30_hr_draft_scrapling",
            country="克罗地亚",
            brand="MAZDA",
            source_url="https://www.mazda.hr/konfigurirajte/MAZDA%20CX-30/5WGN/",
            source_type="manufacturer_official",
            price_semantics="base_msrp",
        ),
        HttpTextProfile(
            url="https://www.mazda.hr/konfigurirajte/MAZDA%20CX-30/5WGN/",
            fixed_model="CX-30",
            fixed_jato_model="CX-30",
            fixed_jato_powertrain="MHEV",
            copy_trim_to_jato_trim=True,
            default_currency="EUR",
            default_tax_included=True,
            default_price_label="Preporučena cijena s PDV-om i PPMV",
            match_confidence=0.82,
            match_status="review_required",
            match_reason={"kind": "official_configurator_grade_price"},
            entry_patterns=(
                HttpTextEntryPattern(
                    pattern=(
                        r'"gradeName":"(?P<trim>Prime-Line)".{0,3000}?'
                        r'"newPrice":\{"priceValue":(?P<price>\d+\.\d+)'
                    ),
                    official_powertrain="2.5L e-SKYACTIV G 140 M Hybrid 6MT 2WD",
                    price_label="Preporučena cijena s PDV-om i PPMV",
                ),
            ),
        ),
    )
    html = (
        '{"gradeName":"Prime-Line","description":"base",'
        '"newPrice":{"priceValue":28604.05,"currency":"EUR"}}'
    )
    monkeypatch.setattr(extractor, "_fetch_text", lambda: html)

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_model == "CX-30"
    assert results[0].official_trim == "Prime-Line"
    assert results[0].official_powertrain == "2.5L e-SKYACTIV G 140 M Hybrid 6MT 2WD"
    assert results[0].msrp_value == 28604.05
    assert results[0].currency == "EUR"
    assert results[0].jato_model == "CX-30"
    assert results[0].jato_trim == "Prime-Line"
    assert results[0].jato_powertrain == "MHEV"
    assert results[0].match_confidence == 0.82


def test_http_text_profile_builds_entry_patterns() -> None:
    profile = _build_http_text_profile(
        {
            "url": "https://example.invalid/model",
            "timeout_seconds": 45,
            "headers": {"Accept-Language": "hr-HR"},
            "entry_patterns": [
                {
                    "pattern": r'"priceValue":(?P<price>\d+\.\d+)',
                    "official_trim": "Prime-Line",
                    "jato_powertrain": "MHEV",
                }
            ],
        }
    )

    assert profile.timeout_seconds == 45
    assert profile.headers == {"Accept-Language": "hr-HR"}
    assert profile.entry_patterns[0].official_trim == "Prime-Line"
    assert profile.entry_patterns[0].jato_powertrain == "MHEV"


def test_http_text_uses_default_curl_fallback_after_requests_tls_error(monkeypatch) -> None:
    extractor = HttpTextExtractor(
        ExtractorConfig(
            source_code="peugeot_3008_pt_draft_scrapling",
            country="葡萄牙",
            brand="PEUGEOT",
            source_url="https://www.peugeot.pt/showroom/peugeot-3008/3008-hybrid.html",
            source_type="manufacturer_official",
            price_semantics="base_msrp",
        ),
        HttpTextProfile(
            url="https://www.peugeot.pt/showroom/peugeot-3008/3008-hybrid.html",
            fixed_model="3008",
            fixed_jato_model="3008",
            copy_trim_to_jato_trim=True,
            default_currency="EUR",
            entry_patterns=(
                HttpTextEntryPattern(
                    pattern=(
                        r"3008\s+Allure\s+Hybrid\s+145\s+cv\s+e-DCS6"
                        r".{0,300}?PVPR\s+de\s+(?P<price>\d{1,3}(?:\.\d{3})*,\d{2})\s*€"
                    ),
                    official_trim="Allure",
                    official_powertrain="Hybrid 145 cv e-DCS6",
                    jato_powertrain="MHEV",
                ),
            ),
        ),
    )

    def fail_request(*_args, **_kwargs):
        raise requests.exceptions.SSLError("[SSL: UNEXPECTED_EOF_WHILE_READING]")

    monkeypatch.setattr(extractor._session, "get", fail_request)
    commands = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        output_path = command[command.index("-o") + 1]
        with open(output_path, "wb") as handle:
            handle.write(
                b"exemplo para 3008 Allure Hybrid 145 cv e-DCS6, "
                b"PVPR de 41.486,01\xe2\x82\xac, PVP campanha de 37.486,01\xe2\x82\xac"
            )
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr(subprocess, "run", fake_run)

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].msrp_value == 41486.01
    assert results[0].official_trim == "Allure"
    assert results[0].jato_powertrain == "MHEV"
    assert "--user-agent" not in commands[0]
    assert "--http1.1" in commands[0]
