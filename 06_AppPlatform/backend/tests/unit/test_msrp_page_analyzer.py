from __future__ import annotations

from app.scraper import enable_external_scraper_package


enable_external_scraper_package()

from jato_scraper.llm.msrp_page_analyzer import (  # noqa: E402
  analyze_page_heuristics,
  build_page_evidence,
  parse_json_object,
)


def test_build_page_evidence_extracts_structured_signals():
    html = """
    <html>
      <head>
        <title>Volkswagen Tiguan | Recommended Retail Price</title>
        <meta
          name="description"
          content="Build your Tiguan and compare trims."
        />
        <script type="application/ld+json">{"@type":"Product"}</script>
      </head>
      <body>
        <h1>Tiguan</h1>
        <a href="/sv/bygg-din-bil.html/__app/31106.app">
          Build your Volkswagen
        </a>
        <div data-testid="trimcard">Life Edition</div>
        <div>Rekommenderat cirkapris inkl. moms 459 900 kr</div>
      </body>
    </html>
    """

    evidence = build_page_evidence(html=html, url="https://example.com")

    assert evidence["page"]["title"] == (
        "Volkswagen Tiguan | Recommended Retail Price"
    )
    assert evidence["signals"]["ld_json_script_count"] == 1
    assert "trimcard" in evidence["signals"]["data_testids"]
    assert evidence["signals"]["candidate_links"] == [
        "/sv/bygg-din-bil.html/__app/31106.app"
    ]
    assert "rekommenderat cirkapris" in (
      evidence["signals"]["keyword_hits"]["msrp"]
    )


def test_analyze_page_heuristics_prefers_playwright_for_configurator():
    html = """
    <html>
      <head>
        <title>Volkswagen Tiguan | Recommended Retail Price</title>
        <script type="application/ld+json">{"@type":"Product"}</script>
      </head>
      <body>
        <h1>Tiguan</h1>
        <a href="/sv/bygg-din-bil.html/__app/31106.app">Build your car</a>
        <div data-testid="trimcard">Life Edition</div>
        <div data-testid="engine-card">eHybrid 272 hk</div>
        <div data-testid="price-container">
          Rekommenderat cirkapris inkl. moms 459 900 kr
        </div>
      </body>
    </html>
    """

    evidence = build_page_evidence(html=html, url="https://example.com")
    analysis = analyze_page_heuristics(evidence)

    assert analysis["page_semantics"] == "base_msrp"
    assert analysis["recommended_extractor"] == "playwright"
    assert analysis["powertrain_granularity"] == "engine_level"
    assert analysis["should_use_llm_in_pipeline"] is True
    assert analysis["selector_hints"]["trim_card_selector"] == (
        "[data-testid='trimcard']"
    )
    assert analysis["selector_hints"]["detail_card_selector"] == (
        "[data-testid='engine-card']"
    )


def test_parse_json_object_accepts_wrapped_payload():
    text = (
      "Analysis follows:\n```json\n{\n"
      '  "page_semantics": "base_msrp"\n'
      "}\n```"
    )

    payload = parse_json_object(text)

    assert payload == {"page_semantics": "base_msrp"}
