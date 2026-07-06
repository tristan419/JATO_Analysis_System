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


def test_http_text_cleans_html_entities_from_match_fields(monkeypatch):
    extractor = HttpTextExtractor(
        ExtractorConfig(
            source_code="dacia_duster_ro_draft_scrapling",
            country="罗马尼亚",
            brand="DACIA",
            source_url="https://www.dacia.ro/preturi-de-lista.html",
            source_type="official_price_list",
            price_semantics="base_msrp",
        ),
        HttpTextProfile(
            url="https://www.dacia.ro/preturi-de-lista.html",
            fixed_model="DUSTER",
            fixed_jato_model="DUSTER",
            copy_trim_to_jato_trim=True,
            default_currency="EUR",
            default_tax_included=True,
            default_price_label="Preț de listă (TVA inclus)",
            match_confidence=0.86,
            match_status="review_required",
            entry_patterns=(
                HttpTextEntryPattern(
                    pattern=(
                        r"<td><p>(?P<trim>journey&nbsp;hybrid-G 150 4X4)</p>"
                        r"</td><td><p>(?P<price>29,100)&nbsp;EUR"
                    ),
                    official_powertrain="hybrid-G list price",
                    jato_powertrain="LPG",
                ),
            ),
        ),
    )
    html = "<td><p>journey&nbsp;hybrid-G 150 4X4</p></td><td><p>29,100&nbsp;EUR"
    monkeypatch.setattr(extractor, "_fetch_text", lambda: html)

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_trim == "journey hybrid-G 150 4X4"
    assert results[0].jato_trim == "journey hybrid-G 150 4X4"
    assert results[0].raw_payload["match_groups"]["trim"] == "journey hybrid-G 150 4X4"


def test_http_text_profile_builds_entry_patterns() -> None:
    profile = _build_http_text_profile(
        {
            "url": "https://example.invalid/model",
            "timeout_seconds": 45,
            "headers": {"Accept-Language": "hr-HR"},
            "prefer_curl_fetch": True,
            "curl_fallback": True,
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
    assert profile.prefer_curl_fetch is True
    assert profile.curl_fallback is True
    assert profile.entry_patterns[0].official_trim == "Prime-Line"
    assert profile.entry_patterns[0].jato_powertrain == "MHEV"


def test_http_text_uses_curl_fallback_after_requests_failure(monkeypatch):
    extractor = HttpTextExtractor(
        ExtractorConfig(
            source_code="mg_zs_es_draft_scrapling",
            country="西班牙",
            brand="MG",
            source_url="https://www.mgmotor.eu/es-ES/configurator/zs",
            source_type="manufacturer_official",
            price_semantics="base_msrp",
        ),
        HttpTextProfile(
            url="https://www.mgmotor.eu/es-ES/configurator/zs",
            fixed_model="ZS",
            fixed_jato_model="ZS",
            copy_trim_to_jato_trim=True,
            default_currency="EUR",
            default_tax_included=True,
            default_price_label="Precio de catálogo incl. IVA",
            match_confidence=0.84,
            match_status="review_required",
            curl_fallback=True,
            entry_patterns=(
                HttpTextEntryPattern(
                    pattern=(
                        r"field_version_versionTitle.*?>"
                        r"(?P<trim>Standard)</span>.*?"
                        r"paymentOptionBasePrice.*?"
                        r"(?P<price>20\.840)"
                    ),
                    official_powertrain="1.5L gasoline",
                    jato_powertrain="ICE",
                ),
            ),
        ),
    )

    def raise_ssl_error(*_args, **_kwargs):
        raise requests.exceptions.SSLError("tls eof")

    def fake_run(cmd, **_kwargs):
        assert cmd[0] == "curl"
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=(
                "field_version_versionTitle\"><span>Standard</span>"
                "paymentOptionBasePrice\">20.840</div>"
            ),
            stderr="",
        )

    monkeypatch.setattr(extractor._session, "get", raise_ssl_error)
    monkeypatch.setattr(subprocess, "run", fake_run)

    results = extractor.extract()

    assert len(results) == 1
    assert results[0].official_trim == "Standard"
    assert results[0].msrp_value == 20840
    assert results[0].jato_powertrain == "ICE"
