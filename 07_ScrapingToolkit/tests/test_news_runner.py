from pathlib import Path

from jato_scraper.news_config_loader import load_news_batch_config
from jato_scraper.news_runner import parse_feed_xml
from jato_scraper.news_runner import write_news_batch_output


_RSS_SAMPLE = """
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item>
      <title>Finland EV demand holds up</title>
      <link>https://example.com/finland-ev</link>
      <pubDate>Wed, 15 Apr 2026 08:00:00 GMT</pubDate>
      <description>
        <![CDATA[<p>Fleet orders are driving the latest demand mix.</p>]]>
      </description>
    </item>
  </channel>
</rss>
""".strip()


def test_load_news_batch_config_reads_country_feeds(tmp_path: Path) -> None:
    batch_path = tmp_path / "batch.yaml"
    batch_path.write_text(
        """
batch_code: demo_batch
description: Demo batch
countries:
  - country_code: FI
    country_label: Finland / 芬兰
    feeds:
      - source_code: fi_demo
        publisher: Demo Publisher
        feed_url: https://example.com/rss
        language: en
        tags: [market, policy]
        include_keywords: [Finland, EV]
        exclude_keywords: [truck]
""".strip(),
        encoding="utf-8",
    )

    batch = load_news_batch_config(batch_path)

    assert batch.batch_code == "demo_batch"
    assert len(batch.countries) == 1
    assert batch.countries[0].feeds[0].source_code == "fi_demo"
    assert batch.countries[0].feeds[0].include_keywords == (
        "Finland",
        "EV",
    )
    assert batch.countries[0].feeds[0].exclude_keywords == ("truck",)


def test_parse_feed_xml_reads_rss_items() -> None:
    batch = load_news_batch_config(
        Path(__file__).resolve().parents[1] / "news_sources" / "batch_a.yaml"
    )
    feed = batch.countries[1].feeds[0]

    articles = parse_feed_xml(_RSS_SAMPLE, feed, limit_per_feed=3)

    assert len(articles) == 1
    assert articles[0].country_code == "FI"
    assert articles[0].title == "Finland EV demand holds up"
    assert (
        articles[0].summary
        == "Fleet orders are driving the latest demand mix."
    )


def test_parse_feed_xml_tolerates_leading_whitespace() -> None:
    batch = load_news_batch_config(
        Path(__file__).resolve().parents[1] / "news_sources" / "batch_a.yaml"
    )
    feed = batch.countries[0].feeds[0]

    articles = parse_feed_xml(f"\n  {_RSS_SAMPLE}", feed, limit_per_feed=3)

    assert len(articles) == 1
    assert articles[0].country_code == "SE"


def test_parse_feed_xml_applies_feed_keyword_filters() -> None:
    batch = load_news_batch_config(
        Path(__file__).resolve().parents[1] / "news_sources" / "batch_a.yaml"
    )
    feed = batch.countries[1].feeds[1]

    articles = parse_feed_xml(_RSS_SAMPLE, feed, limit_per_feed=3)

    assert len(articles) == 1
    assert articles[0].country_code == "FI"


def test_load_news_batch_config_rejects_boolean_like_country_codes(
    tmp_path: Path,
) -> None:
    batch_path = tmp_path / "invalid_batch.yaml"
    batch_path.write_text(
        """
batch_code: demo_batch
description: Demo batch
countries:
  - country_code: false
    country_label: Norway
    feeds:
      - source_code: demo_feed
        publisher: Demo Publisher
        feed_url: https://example.com/rss
        language: en
""".strip(),
        encoding="utf-8",
    )

    try:
        load_news_batch_config(batch_path)
    except ValueError as exc:
        assert "country_code must be quoted text" in str(exc)
    else:
        raise AssertionError("Expected boolean-like country_code to be rejected")


def test_batch_a_countries_use_multi_source_feeds() -> None:
    batch = load_news_batch_config(
        Path(__file__).resolve().parents[1] / "news_sources" / "batch_a.yaml"
    )

    countries = {
        country.country_code: country
        for country in batch.countries
    }

    for country_code, country in countries.items():
        assert len(country.feeds) >= 4
        publishers = {
            feed.publisher
            for feed in country.feeds
        }
        assert "Google News" in publishers
        assert len(publishers) >= 3
        assert "ACEA" in publishers
        assert "Transport & Environment" in publishers
        assert any(
            publisher not in {
                "Google News",
                "ACEA",
                "Transport & Environment",
            }
            for publisher in publishers
        )
        assert "NO" in countries


def test_write_news_batch_output_writes_timestamped_file(tmp_path: Path) -> None:
    payload = [
        {
            "batch_code": "demo_batch",
            "country_count": 2,
            "article_count": 5,
            "countries": [],
            "errors": [],
        }
    ]

    output_path = write_news_batch_output(payload, output_root=tmp_path)

    assert output_path.parent == tmp_path
    assert output_path.exists()
    assert "news_batch_" in output_path.name
