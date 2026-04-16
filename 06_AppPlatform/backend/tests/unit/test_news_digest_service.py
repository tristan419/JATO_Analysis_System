from __future__ import annotations

import io
import json

from app.services import news_digest_service

from jato_scraper.news_base import CountryNewsConfig
from jato_scraper.news_base import NewsArticle
from urllib.error import HTTPError


_GERMANY_CONFIG = CountryNewsConfig(
    country_code="DE",
    country_label="Germany / 德国",
    feeds=(),
)


def test_get_country_news_payload_prefers_store(monkeypatch) -> None:
    monkeypatch.setattr(
        news_digest_service,
        "resolve_country_news_code",
        lambda country: "DE",
    )
    monkeypatch.setattr(
        news_digest_service,
        "_ensure_country_indexes",
        lambda: ({"DE": _GERMANY_CONFIG}, {}),
    )

    stored_payload = {
        "countryCode": "DE",
        "countryLabel": "Germany / 德国",
        "marketEvents": [{"title": "stored", "url": "https://example.com"}],
        "newsDigest": {"articleCount": 1, "headline": "stored"},
    }
    monkeypatch.setattr(
        news_digest_service,
        "_load_country_news_payload_from_store",
        lambda *args, **kwargs: stored_payload,
    )

    fetch_called = False

    def _unexpected_fetch(*args, **kwargs):
        nonlocal fetch_called
        fetch_called = True
        return []

    monkeypatch.setattr(
        news_digest_service,
        "_fetch_country_articles",
        _unexpected_fetch,
    )

    payload = news_digest_service.get_country_news_payload("Germany")

    assert payload is stored_payload
    assert fetch_called is False


def test_get_country_news_payload_does_not_live_fetch_by_default(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        news_digest_service,
        "resolve_country_news_code",
        lambda country: "DE",
    )
    monkeypatch.setattr(
        news_digest_service,
        "_ensure_country_indexes",
        lambda: ({"DE": _GERMANY_CONFIG}, {}),
    )
    monkeypatch.setattr(
        news_digest_service,
        "_load_country_news_payload_from_store",
        lambda *args, **kwargs: None,
    )

    def _unexpected_fetch(*args, **kwargs):
        raise AssertionError("live fetch should not run on the request path")

    monkeypatch.setattr(
        news_digest_service,
        "_fetch_country_articles",
        _unexpected_fetch,
    )

    payload = news_digest_service.get_country_news_payload("Germany")

    assert payload["countryCode"] == "DE"
    assert payload["marketEvents"] == []
    assert payload["newsDigest"] is None


def test_refresh_country_news_uses_gemini_only_for_prefetched_digest(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        news_digest_service,
        "resolve_country_news_code",
        lambda country: "DE",
    )
    monkeypatch.setattr(
        news_digest_service,
        "_ensure_country_indexes",
        lambda: ({"DE": _GERMANY_CONFIG}, {}),
    )
    monkeypatch.setattr(
        news_digest_service,
        "_gemini_enrichment_enabled",
        lambda explicit: True,
    )
    monkeypatch.setattr(
        news_digest_service,
        "_database_available",
        lambda: False,
    )

    monkeypatch.setattr(
        news_digest_service,
        "_fetch_country_articles",
        lambda *args, **kwargs: [
            NewsArticle(
                source_code="de_market",
                country_code="DE",
                country_label="Germany / 德国",
                publisher="Reuters",
                title="Germany reviews EV tax incentive",
                summary="Tax support may be narrowed for fleets.",
                url="https://example.com/de/tax",
                published_at="2026-04-15T08:00:00+00:00",
                tags=("policy",),
            )
        ],
    )
    monkeypatch.setattr(
        news_digest_service,
        "_generate_gemini_news_digest",
        lambda *args, **kwargs: {
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "headline": "德国车队税收激励进入调整窗口",
            "summary": "政策预期收紧，企业用户购车情绪将受影响。",
            "highlights": ["企业税激励是德国电动化核心抓手"],
            "events": [
                {
                    "url": "https://example.com/de/tax",
                    "summary": "车队税收激励或收紧",
                    "tags": ["policy", "fleet"],
                }
            ],
        },
    )

    payload = news_digest_service.refresh_country_news(
        "Germany",
        persist=False,
        enrich_with_gemini=True,
    )

    assert payload["newsDigest"]["summaryProvider"] == "gemini"
    assert payload["newsDigest"]["headline"] == "德国车队税收激励进入调整窗口"
    assert payload["marketEvents"][0]["summary"] == "车队税收激励或收紧"
    assert payload["marketEvents"][0]["tags"] == ["policy", "fleet"]


def test_refresh_country_news_marks_stored_snapshot_stale_when_fetch_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        news_digest_service,
        "resolve_country_news_code",
        lambda country: "DE",
    )
    monkeypatch.setattr(
        news_digest_service,
        "_ensure_country_indexes",
        lambda: ({"DE": _GERMANY_CONFIG}, {}),
    )
    monkeypatch.setattr(
        news_digest_service,
        "_fetch_country_articles",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        news_digest_service,
        "_load_country_news_payload_from_store",
        lambda *args, **kwargs: {
            "countryCode": "DE",
            "countryLabel": "Germany / 德国",
            "marketEvents": [],
            "newsDigest": {
                "articleCount": 2,
                "headline": "stored headline",
                "stale": False,
            },
        },
    )

    payload = news_digest_service.refresh_country_news(
        "Germany",
        persist=False,
    )

    assert payload["newsDigest"]["stale"] is True


def test_get_country_news_ops_status_reports_snapshot_metadata(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        news_digest_service,
        "resolve_country_news_code",
        lambda country: "DE",
    )
    monkeypatch.setattr(
        news_digest_service,
        "_ensure_country_indexes",
        lambda: ({"DE": _GERMANY_CONFIG}, {}),
    )
    monkeypatch.setattr(
        news_digest_service,
        "_database_available",
        lambda: True,
    )
    monkeypatch.setattr(
        news_digest_service,
        "_gemini_api_key",
        lambda: "gemini-key",
    )
    monkeypatch.setattr(
        news_digest_service,
        "_load_country_news_payload_from_store",
        lambda *args, **kwargs: {
            "countryCode": "DE",
            "countryLabel": "Germany / 德国",
            "marketEvents": [
                {"title": "stored", "url": "https://example.com"}
            ],
            "newsDigest": {
                "articleCount": 1,
                "headline": "stored",
                "summaryProvider": "gemini",
                "summaryModel": "gemini-2.5-flash",
                "syncTimestamp": "2026-04-15T08:05:00+00:00",
                "updatedAt": "2026-04-15T08:00:00+00:00",
                "stale": False,
            },
        },
    )

    status = news_digest_service.get_country_news_ops_status("Germany")

    assert status["countryCode"] == "DE"
    assert status["configured"] is True
    assert status["databaseEnabled"] is True
    assert status["hasSnapshot"] is True
    assert status["summaryProvider"] == "gemini"
    assert status["summaryModel"] == "gemini-2.5-flash"
    assert status["geminiConfigured"] is True
    assert status["geminiModel"] == news_digest_service.DEFAULT_GEMINI_MODEL


def test_generate_gemini_news_digest_retries_http_429(monkeypatch) -> None:
    monkeypatch.setattr(
        news_digest_service,
        "_gemini_api_key",
        lambda: "gemini-key",
    )
    monkeypatch.setattr(news_digest_service.time, "sleep", lambda _: None)

    article = NewsArticle(
        source_code="de_market",
        country_code="DE",
        country_label="Germany / 德国",
        publisher="Reuters",
        title="Germany reviews EV tax incentive",
        summary="Tax support may be narrowed for fleets.",
        url="https://example.com/de/tax",
        published_at="2026-04-15T08:00:00+00:00",
        tags=("policy",),
    )

    attempts = {"count": 0}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": json.dumps(
                                            {
                                                "headline": "德国补贴政策调整",
                                                "summary": "政策收紧，但市场仍有支撑。",
                                                "highlights": ["补贴调整"],
                                                "events": [
                                                    {
                                                        "url": article.url,
                                                        "summary": (
                                                            "补贴调整进入执行阶段"
                                                        ),
                                                        "tags": ["policy"],
                                                    }
                                                ],
                                            },
                                            ensure_ascii=False,
                                        )
                                    }
                                ]
                            }
                        }
                    ]
                }
            ).encode("utf-8")

    def _fake_urlopen(request, timeout):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise HTTPError(
                request.full_url,
                429,
                "Too Many Requests",
                {"Retry-After": "0"},
                io.BytesIO(b"rate limited"),
            )
        return _Response()

    monkeypatch.setattr(news_digest_service, "urlopen", _fake_urlopen)

    payload = news_digest_service._generate_gemini_news_digest(
        _GERMANY_CONFIG,
        [article],
    )

    assert attempts["count"] == 2
    assert payload is not None
    assert payload["provider"] == "gemini"
    assert payload["headline"] == "德国补贴政策调整"


def test_generate_gemini_news_digest_returns_none_after_retry_budget(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        news_digest_service,
        "_gemini_api_key",
        lambda: "gemini-key",
    )
    monkeypatch.setattr(news_digest_service.time, "sleep", lambda _: None)

    article = NewsArticle(
        source_code="de_market",
        country_code="DE",
        country_label="Germany / 德国",
        publisher="Reuters",
        title="Germany reviews EV tax incentive",
        summary="Tax support may be narrowed for fleets.",
        url="https://example.com/de/tax",
        published_at="2026-04-15T08:00:00+00:00",
        tags=("policy",),
    )

    original_retry_budget = news_digest_service.DEFAULT_GEMINI_MAX_RETRIES
    monkeypatch.setattr(news_digest_service, "DEFAULT_GEMINI_MAX_RETRIES", 1)

    def _always_fail(request, timeout):
        raise HTTPError(
            request.full_url,
            429,
            "Too Many Requests",
            {"Retry-After": "0"},
            io.BytesIO(b"rate limited"),
        )

    monkeypatch.setattr(news_digest_service, "urlopen", _always_fail)

    payload = news_digest_service._generate_gemini_news_digest(
        _GERMANY_CONFIG,
        [article],
    )

    assert original_retry_budget >= 1
    assert payload is None
