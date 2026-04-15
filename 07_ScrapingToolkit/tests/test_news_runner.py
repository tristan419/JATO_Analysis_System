from pathlib import Path

from jato_scraper.news_config_loader import load_news_batch_config
from jato_scraper.news_runner import parse_feed_xml


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
""".strip(),
        encoding="utf-8",
    )

    batch = load_news_batch_config(batch_path)

    assert batch.batch_code == "demo_batch"
    assert len(batch.countries) == 1
    assert batch.countries[0].feeds[0].source_code == "fi_demo"


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

