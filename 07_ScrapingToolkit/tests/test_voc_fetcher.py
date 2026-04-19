import json
from pathlib import Path

from jato_scraper.voc_base import CountryVocConfig
from jato_scraper.voc_base import VocBatchConfig
from jato_scraper.voc_base import VocSourceConfig
from jato_scraper.voc_fetcher import build_voc_raw_collection
from jato_scraper.voc_fetcher import collect_source_documents


def test_collect_source_documents_fetches_ranked_same_site_pages(monkeypatch) -> None:
    source = VocSourceConfig(
        source_code="se_demo_forum",
        country_code="SE",
        country_label="Sweden",
        site_name="Demo Forum",
        site_url="https://example.com/forum/",
        site_type="ev_community",
        language="sv",
    )
    pages = {
        "https://example.com/forum/": {
            "url": "https://example.com/forum/",
            "title": "Demo Forum",
            "publishedAt": None,
            "summary": "Landing page",
            "text": "Forum landing page text",
            "links": [
                {"href": "/forum/thread-charging-1001", "text": "Charging issue after update"},
                {"href": "/forum/topic-winter-range-2025", "text": "Winter range discussion"},
                {"href": "/login", "text": "Login"},
                {"href": "https://external.example.net/story", "text": "External story"},
            ],
        },
        "https://example.com/forum/thread-charging-1001": {
            "url": "https://example.com/forum/thread-charging-1001",
            "title": "Charging issue after update",
            "publishedAt": "2026-04-18T08:00:00Z",
            "summary": "Owners discuss charging instability.",
            "text": "Owners report charging failures and software warnings after the latest update.",
            "links": [],
        },
        "https://example.com/forum/topic-winter-range-2025": {
            "url": "https://example.com/forum/topic-winter-range-2025",
            "title": "Winter range discussion",
            "publishedAt": "2026-04-17T08:00:00Z",
            "summary": "Range concerns in cold weather.",
            "text": "Drivers compare winter efficiency and public charging reliability.",
            "links": [],
        },
    }

    def fake_fetch(url: str, timeout_seconds: int = 20) -> dict:
        return {**pages[url], "statusCode": 200}

    monkeypatch.setattr("jato_scraper.voc_fetcher.fetch_public_page", fake_fetch)

    payload = collect_source_documents(
        source,
        taxonomy_profile="nordic_core",
        max_links=2,
    )

    assert payload["documentCount"] == 2
    assert payload["landingPage"]["candidateCount"] == 2
    assert {document["url"] for document in payload["documents"]} == {
        "https://example.com/forum/thread-charging-1001",
        "https://example.com/forum/topic-winter-range-2025",
    }
    assert all(document["pageKind"] == "discussion_thread" for document in payload["documents"])
    assert payload["errors"] == []


def test_build_voc_raw_collection_writes_source_payloads(tmp_path: Path, monkeypatch) -> None:
    source = VocSourceConfig(
        source_code="se_demo_media",
        country_code="SE",
        country_label="Sweden",
        site_name="Demo Media",
        site_url="https://example.com/news/",
        site_type="media_comments",
        language="sv",
    )
    batch = VocBatchConfig(
        batch_code="voc_demo_batch",
        description="Demo batch",
        countries=(
            CountryVocConfig(
                country_code="SE",
                country_label="Sweden",
                languages=("sv", "en"),
                taxonomy_profile="nordic_core",
                sources=(source,),
            ),
        ),
    )

    def fake_fetch(url: str, timeout_seconds: int = 20) -> dict:
        assert url == "https://example.com/news/"
        return {
            "url": url,
            "title": "Demo Media Home",
            "publishedAt": None,
            "summary": "Landing page summary",
            "text": "Main market reaction page with visible reader feedback and launch discussion.",
            "links": [],
            "statusCode": 200,
        }

    monkeypatch.setattr("jato_scraper.voc_fetcher.fetch_public_page", fake_fetch)

    payload = build_voc_raw_collection(
        batch,
        output_root=tmp_path,
        max_links_per_source=3,
    )

    assert payload["country_count"] == 1
    assert payload["source_count"] == 1
    assert payload["document_count"] == 1
    output_path = Path(payload["countries"][0]["sources"][0]["output_path"])
    assert output_path.exists()

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved["documentCount"] == 1
    assert saved["documents"][0]["pageKind"] == "landing_page"
    assert saved["documents"][0]["siteType"] == "media_comments"
