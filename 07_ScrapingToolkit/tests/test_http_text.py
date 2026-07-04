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
