"""Standalone RSS/Atom batch runner for country automotive news."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import html
import json
import logging
from pathlib import Path
import re
from typing import Any
from xml.etree import ElementTree as ET

import requests

from jato_scraper.news_base import NewsArticle, NewsBatchConfig, NewsFeedConfig
from jato_scraper.news_config_loader import load_news_batch_config

log = logging.getLogger(__name__)


def _tag_name(tag: str) -> str:
    return tag.split("}", 1)[-1].lower()


def _first_child_text(element: ET.Element, names: tuple[str, ...]) -> str:
    for child in element:
        if _tag_name(child.tag) in names:
            return "".join(child.itertext()).strip()
    return ""


def _first_link(element: ET.Element) -> str:
    for child in element:
        if _tag_name(child.tag) != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return href.strip()
        text = "".join(child.itertext()).strip()
        if text:
            return text
    return ""


def _clean_text(value: str) -> str:
    unescaped = html.unescape(value or "")
    without_tags = re.sub(r"<[^>]+>", " ", unescaped)
    return re.sub(r"\s+", " ", without_tags).strip()


def _normalize_published_at(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def parse_feed_xml(
    xml_text: str,
    feed: NewsFeedConfig,
    limit_per_feed: int = 5,
) -> list[NewsArticle]:
    root = ET.fromstring(xml_text)
    tag = _tag_name(root.tag)
    items: list[ET.Element]
    if tag == "rss":
        channel = next(
            (child for child in root if _tag_name(child.tag) == "channel"),
            None,
        )
        items = [] if channel is None else [
            child for child in channel if _tag_name(child.tag) == "item"
        ]
    elif tag == "feed":
        items = [child for child in root if _tag_name(child.tag) == "entry"]
    else:
        raise ValueError(f"Unsupported feed type: {root.tag}")

    articles: list[NewsArticle] = []
    for item in items:
        title = _clean_text(_first_child_text(item, ("title",)))
        url = _first_link(item)
        summary = _clean_text(
            _first_child_text(
                item,
                ("description", "summary", "content", "content:encoded"),
            )
        )
        published_at = _normalize_published_at(
            _first_child_text(
                item,
                ("pubdate", "published", "updated", "issued"),
            )
        )
        if not title or not url:
            continue
        articles.append(
            NewsArticle(
                source_code=feed.source_code,
                country_code=feed.country_code,
                country_label=feed.country_label,
                publisher=feed.publisher,
                title=title,
                url=url,
                summary=summary or None,
                published_at=published_at,
                tags=feed.tags,
                raw_payload={"language": feed.language},
            )
        )
        if len(articles) >= max(1, int(limit_per_feed)):
            break
    return articles


def fetch_feed_articles(
    feed: NewsFeedConfig,
    limit_per_feed: int = 5,
    timeout_seconds: int = 20,
) -> list[NewsArticle]:
    response = requests.get(feed.feed_url, timeout=timeout_seconds)
    response.raise_for_status()
    return parse_feed_xml(
        response.text,
        feed=feed,
        limit_per_feed=limit_per_feed,
    )


def run_news_batch(
    batch: NewsBatchConfig,
    limit_per_feed: int = 5,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    seen_urls: set[str] = set()
    countries_payload: list[dict[str, Any]] = []
    article_count = 0
    errors: list[dict[str, str]] = []

    for country in batch.countries:
        country_articles: list[dict[str, Any]] = []
        for feed in country.feeds:
            try:
                articles = fetch_feed_articles(
                    feed,
                    limit_per_feed=limit_per_feed,
                    timeout_seconds=timeout_seconds,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "News feed failed for %s: %s",
                    feed.source_code,
                    exc,
                )
                errors.append(
                    {
                        "source_code": feed.source_code,
                        "country_code": feed.country_code,
                        "error": str(exc),
                    }
                )
                continue

            for article in articles:
                if article.url in seen_urls:
                    continue
                seen_urls.add(article.url)
                country_articles.append(asdict(article))
                article_count += 1

        countries_payload.append(
            {
                "country_code": country.country_code,
                "country_label": country.country_label,
                "source_count": len(country.feeds),
                "article_count": len(country_articles),
                "articles": country_articles,
            }
        )

    return {
        "batch_code": batch.batch_code,
        "description": batch.description,
        "country_count": len(batch.countries),
        "article_count": article_count,
        "countries": countries_payload,
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch automotive news for configured country batches.",
    )
    parser.add_argument(
        "--batch-files",
        nargs="+",
        required=True,
        help="Batch YAML files under news_sources/ or absolute paths.",
    )
    parser.add_argument(
        "--limit-per-feed",
        type=int,
        default=5,
        help="Maximum articles to keep per feed.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=20,
        help="HTTP timeout per feed request.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional JSON output path.",
    )
    args = parser.parse_args(argv)

    payload = [
        run_news_batch(
            load_news_batch_config(batch_file),
            limit_per_feed=args.limit_per_feed,
            timeout_seconds=args.timeout_seconds,
        )
        for batch_file in args.batch_files
    ]

    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
