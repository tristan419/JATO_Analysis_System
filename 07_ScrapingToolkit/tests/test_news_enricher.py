import json
from pathlib import Path

from jato_scraper import news_enricher


def _raw_news_payload() -> list[dict[str, object]]:
    countries = []
    for country_code, country_label, title in (
        (
            "SE",
            "Sweden / 瑞典",
            "Sweden confirms new EV incentive rules for company fleets",
        ),
        (
            "FI",
            "Finland / 芬兰",
            "Finland EV demand rises as charging network expands",
        ),
        (
            "NO",
            "Norway / 挪威",
            "Norway weighs changes to electric vehicle tax benefits",
        ),
        (
            "DK",
            "Denmark / 丹麦",
            "Denmark new-car registrations surge as Tesla Model Y prices fall",
        ),
    ):
        countries.append(
            {
                "country_code": country_code,
                "country_label": country_label,
                "source_count": 1,
                "article_count": 1,
                "articles": [
                    {
                        "source_code": f"{country_code.lower()}_demo_news",
                        "country_code": country_code,
                        "country_label": country_label,
                        "publisher": "Reuters",
                        "title": title,
                        "url": f"https://news.example/{country_code.lower()}/ev",
                        "summary": (
                            "The article discusses EV policy, pricing, charging "
                            "and market demand signals for automotive planning."
                        ),
                        "published_at": "2026-06-10T08:00:00+00:00",
                        "tags": ["market", "policy", "automotive"],
                        "raw_payload": {"language": "en"},
                    }
                ],
            }
        )
    return [
        {
            "batch_code": "country_news_batch_a",
            "description": "demo",
            "country_count": 4,
            "article_count": 4,
            "countries": countries,
            "errors": [],
        }
    ]


def test_build_news_enrichment_produces_market_events_and_weekly_digest() -> None:
    payload = news_enricher.build_news_enrichment(
        _raw_news_payload(),
        required_countries=("SE", "FI", "NO", "DK"),
        generated_at_utc="2026-06-12T00:00:00Z",
    )

    assert payload["schemaVersion"] == "news_ai_enrichment_v1"
    assert payload["countryCount"] == 4
    assert payload["marketEventCount"] == 4
    assert payload["warnings"] == []
    se = next(country for country in payload["countries"] if country["countryCode"] == "SE")
    event = se["marketEvents"][0]
    assert event["eventType"] == "policy_regulation"
    assert event["marketImpact"] == "high"
    assert event["sourceTier"] == "trusted_media"
    assert event["relatedEntities"]["countries"][0]["countryCode"] == "SE"
    assert event["evidenceCard"]["supportedClaim"].startswith("SE has")
    assert event["translation"]["zhSummary"].startswith("SE 政策/法规信号")
    assert se["weeklyDigest"]["digestWeek"] == "2026-W24"
    assert se["weeklyDigest"]["evidenceCards"]


def test_news_enricher_identifies_pricing_and_entities() -> None:
    article = {
        "source_code": "dk_demo_news",
        "country_code": "DK",
        "country_label": "Denmark / 丹麦",
        "publisher": "Automotive News Europe",
        "title": "Tesla Model Y prices fall as Denmark demand rises",
        "url": "https://news.example/dk/model-y",
        "summary": "Lower MSRP and stronger BEV demand are reshaping the SUV market.",
        "published_at": "2026-06-10T08:00:00+00:00",
        "tags": ["market"],
        "raw_payload": {"language": "en"},
    }

    event = news_enricher.enrich_news_article(article)

    assert event["eventType"] == "pricing_event"
    assert event["relatedEntities"]["brands"] == ["TESLA"]
    assert event["relatedEntities"]["models"] == ["MODEL Y"]
    assert "BEV" in event["relatedEntities"]["powertrains"]
    assert event["confidence"] >= 0.75


def test_write_news_enrichment_output_writes_json(tmp_path: Path) -> None:
    payload = news_enricher.build_news_enrichment(
        _raw_news_payload(),
        required_countries=("SE",),
        generated_at_utc="2026-06-12T00:00:00Z",
    )

    output_path = news_enricher.write_news_enrichment_output(
        payload,
        output_root=tmp_path,
    )

    assert output_path.exists()
    loaded = json.loads(output_path.read_text(encoding="utf-8"))
    assert loaded["schemaVersion"] == "news_ai_enrichment_v1"
    assert output_path.name.startswith("news_ai_enriched_")
