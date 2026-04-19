from __future__ import annotations

from app.services import news_wiki_service


_NEWS_DIGEST = {
    "countryCode": "DE",
    "countryLabel": "Germany / 德国",
    "articleCount": 2,
    "updatedAt": "2026-04-15T08:00:00+00:00",
    "headline": "Germany reviews EV company-car tax support",
    "summary": (
        "Germany reviews EV company-car tax support；"
        "Chinese-brand pricing pressure rises in Germany"
    ),
    "highlights": [
        "Germany reviews EV company-car tax support",
        "Chinese-brand pricing pressure rises in Germany",
    ],
    "summaryProvider": "gemini",
    "summaryModel": "gemini-2.5-flash",
}

_MARKET_EVENTS = [
    {
        "countryCode": "DE",
        "countryLabel": "Germany / 德国",
        "publisher": "Reuters",
        "title": "Germany reviews EV company-car tax support",
        "summary": "Fleet incentives remain central for electrification uptake.",
        "url": "https://example.com/de/tax-support",
        "publishedAt": "2026-04-15T08:00:00+00:00",
        "tags": ["market", "policy", "fleet"],
    },
    {
        "countryCode": "DE",
        "countryLabel": "Germany / 德国",
        "publisher": "Automotive News Europe",
        "title": "Chinese-brand pricing pressure rises in Germany",
        "summary": "Competitive pricing is reshaping mainstream EV comparisons.",
        "url": "https://example.com/de/pricing-pressure",
        "publishedAt": "2026-04-14T07:30:00+00:00",
        "tags": ["market", "competition", "pricing"],
    },
]


def test_query_news_wiki_prefers_specific_pricing_event(monkeypatch) -> None:
    monkeypatch.setattr(
        news_wiki_service.local_wiki_service,
        "embed_text",
        lambda text: [1.0, 0.0],
    )

    hits = news_wiki_service.query_news_wiki(
        "中国品牌定价压力",
        news_digest=_NEWS_DIGEST,
        market_events=_MARKET_EVENTS,
        limit=3,
    )

    assert hits
    assert hits[0]["kind"] == "event"
    assert "pricing pressure" in str(hits[0]["title"])
    assert hits[0]["url"] == "https://example.com/de/pricing-pressure"
    assert "competition" in hits[0]["matchedTopics"]
    assert "pricing" in hits[0]["matchedTopics"]


def test_query_news_wiki_returns_relevance_signals(monkeypatch) -> None:
    monkeypatch.setattr(
        news_wiki_service.local_wiki_service,
        "embed_text",
        lambda text: [1.0, 0.0],
    )

    hits = news_wiki_service.query_news_wiki(
        "company-car tax support",
        news_digest=_NEWS_DIGEST,
        market_events=_MARKET_EVENTS,
        limit=3,
    )

    assert hits
    top_hit = hits[0]
    assert top_hit["url"] == "https://example.com/de/tax-support"
    assert "tax" in top_hit["matchedTopics"]
    assert "fleet" in top_hit["matchedTopics"]
    assert "topic:tax" in top_hit["relevanceSignals"]
    assert any(signal.startswith("term:") for signal in top_hit["relevanceSignals"])
