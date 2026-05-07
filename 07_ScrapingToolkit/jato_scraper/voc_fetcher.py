"""Lightweight public VOC raw collector."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

from jato_scraper.voc_base import VocBatchConfig
from jato_scraper.voc_base import VocSourceConfig
from jato_scraper.voc_config_loader import load_voc_batch_config
from jato_scraper.voc_taxonomy import get_source_collection_strategy
from jato_scraper.voc_taxonomy import get_voc_taxonomy_profile

try:
    from lxml import html as lxml_html
except ModuleNotFoundError:  # pragma: no cover - stdlib fallback stays exercised
    lxml_html = None

try:
    from trafilatura import extract as trafilatura_extract
except ModuleNotFoundError:  # pragma: no cover - dependency remains optional at import time
    trafilatura_extract = None

REPO_ROOT = Path(__file__).resolve().parents[2]
_WHITESPACE_RE = re.compile(r"\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\s*[;\n]+\s*")
_NEGATIVE_LINK_HINTS = (
    "login",
    "signin",
    "signup",
    "register",
    "privacy",
    "cookie",
    "kontakt",
    "contact",
    "feed",
    "rss",
    "tag/",
    "category/",
    "#comment",
    "/user/",
    "/konto",
)
_POSITIVE_HINTS_BY_SITE_TYPE = {
    "forum": ("thread", "topic", "forum", "discussion", "showtopic", "viewtopic"),
    "ev_community": ("thread", "topic", "forum", "discussion", "charging", "battery", "ev", "electric"),
    "media_comments": ("article", "news", "review", "test", "drive", "launch", "story", "bil", "auto"),
    "consumer_media": ("article", "news", "test", "consumer", "guide", "advice", "problem", "issue"),
    "industry_media": ("article", "news", "dealer", "service", "market", "industry"),
}
_TEXT_TARGET_XPATH = (
    "//article//*[self::h1 or self::h2 or self::h3 or self::p or self::li or self::blockquote]/text()"
    " | //main//*[self::h1 or self::h2 or self::h3 or self::p or self::li or self::blockquote]/text()"
    " | //body//*[self::h1 or self::h2 or self::h3 or self::p or self::li or self::blockquote]/text()"
)
_TEXT_UNIT_XPATH = (
    "//article//*[self::p or self::li or self::blockquote]"
    " | //main//*[self::p or self::li or self::blockquote]"
    " | //body//*[self::p or self::li or self::blockquote]"
)
_COMMENT_UNIT_HINTS = ("comment", "reply", "post", "message", "forum", "discussion")


def _normalize_space(value: Any) -> str:
    return _WHITESPACE_RE.sub(" ", str(value or "")).strip()


def _text_sentences(text: str) -> list[str]:
    return [segment.strip() for segment in _SENTENCE_SPLIT_RE.split(text) if segment.strip()]


def _build_sentence_window_units(text: str) -> list[str]:
    units: list[str] = []
    current: list[str] = []
    current_chars = 0
    for sentence in _text_sentences(text):
        normalized = _normalize_space(sentence)
        if not normalized:
            continue
        current.append(normalized)
        current_chars += len(normalized)
        joined = " ".join(current)
        if len(current) >= 2 or current_chars >= 260:
            units.append(joined)
            current = []
            current_chars = 0
    if current:
        units.append(" ".join(current))
    return units


def _extract_content_units_from_tree(tree: Any, *, page_url: str) -> list[dict[str, Any]]:
    units: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, element in enumerate(tree.xpath(_TEXT_UNIT_XPATH), start=1):
        text = _normalize_space(" ".join(element.itertext()))
        if not text:
            continue
        normalized_key = text.casefold()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        container_tokens = " ".join(
            str(value or "").strip().lower()
            for value in element.xpath(
                "ancestor-or-self::*[@class or @id]/@class | ancestor-or-self::*[@class or @id]/@id"
            )
            if str(value or "").strip()
        )
        if "reply" in container_tokens:
            unit_type = "reply_post"
        elif "comment" in container_tokens:
            unit_type = "comment"
        elif any(hint in container_tokens for hint in _COMMENT_UNIT_HINTS):
            unit_type = "discussion_post"
        elif getattr(element, "tag", "") == "blockquote":
            unit_type = "quote_block"
        elif getattr(element, "tag", "") == "li":
            unit_type = "list_item"
        else:
            unit_type = "content_block"
        author = _normalize_space(
            element.xpath(
                "string((ancestor-or-self::*[contains(@class,'comment') or contains(@class,'reply') or contains(@class,'post')][1]"
                "//*[contains(@class,'author') or contains(@class,'user') or contains(@class,'name')][1])[1])"
            )
        )
        published_at = _normalize_space(
            element.xpath(
                "string((ancestor-or-self::*[contains(@class,'comment') or contains(@class,'reply') or contains(@class,'post')][1]//time[1]/@datetime)[1])"
            )
        )
        units.append(
            {
                "unitId": f"{page_url}#unit-{index}",
                "unitType": unit_type,
                "unitSource": "fetch_lxml_block",
                "text": text,
                "author": author or None,
                "publishedAt": published_at or None,
            }
        )
    return units


def _derive_content_units_from_text(text: str, *, page_url: str) -> list[dict[str, Any]]:
    units: list[dict[str, Any]] = []
    for index, unit_text in enumerate(_build_sentence_window_units(text), start=1):
        units.append(
            {
                "unitId": f"{page_url}#derived-{index}",
                "unitType": "sentence_window",
                "unitSource": "fetch_sentence_window",
                "text": unit_text,
                "author": None,
                "publishedAt": None,
            }
        )
    return units


def _normalize_country_filter(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    normalized = {value.strip().upper() for value in values if value.strip()}
    return normalized or None


def _resolve_repo_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return REPO_ROOT / candidate


def _normalize_host(value: str) -> str:
    return str(value or "").strip().lower().removeprefix("www.")


def _same_site(base_url: str, candidate_url: str) -> bool:
    base_host = _normalize_host(urlparse(base_url).netloc)
    candidate_host = _normalize_host(urlparse(candidate_url).netloc)
    if not base_host or not candidate_host:
        return False
    return (
        candidate_host == base_host
        or candidate_host.endswith(f".{base_host}")
        or base_host.endswith(f".{candidate_host}")
    )


def _score_voc_document(
    *,
    source: VocSourceConfig,
    document: dict[str, Any],
) -> dict[str, Any]:
    title = _normalize_space(document.get("title"))
    summary = _normalize_space(document.get("summary"))
    raw_text = _normalize_space(document.get("rawText"))
    page_kind = _normalize_space(document.get("pageKind"))
    score = 0
    signals: list[str] = []
    warnings: list[str] = []

    if urlparse(str(document.get("url") or "")).netloc:
        score += 1
        signals.append("url-host-present")
    else:
        warnings.append("missing-url-host")

    if len(title) >= 20:
        score += 1
        signals.append("title-length-ok")
    else:
        warnings.append("short-title")

    if len(raw_text) >= 800:
        score += 2
        signals.append("rich-body")
    elif len(raw_text) >= 250:
        score += 1
        signals.append("body-present")
    else:
        warnings.append("thin-body")

    if summary:
        score += 1
        signals.append("summary-present")
    else:
        warnings.append("missing-summary")

    if document.get("publishedAt"):
        score += 1
        signals.append("published-at-present")
    else:
        warnings.append("missing-published-at")

    if page_kind and page_kind != "landing_page":
        score += 1
        signals.append(f"page-kind:{page_kind}")
    else:
        warnings.append("landing-page-fallback")

    if source.site_type in {"ev_community", "forum"} and page_kind in {
        "discussion_thread",
        "thread",
    }:
        score += 1
        signals.append("discussion-unit-match")
    elif source.site_type in {"media_comments", "consumer_media", "industry_media"} and page_kind in {
        "article_comment_page",
        "consumer_editorial_page",
        "industry_article_page",
    }:
        score += 1
        signals.append("editorial-unit-match")

    if score >= 6:
        publish_tier = "high"
        publish_decision = "auto_publish"
    elif score >= 4:
        publish_tier = "medium"
        publish_decision = "candidate_publish"
    else:
        publish_tier = "low"
        publish_decision = "hold_raw"

    return {
        "version": "voc-auto-review-v1",
        "score": score,
        "maxScore": 8,
        "publishTier": publish_tier,
        "publishDecision": publish_decision,
        "signals": signals,
        "warnings": warnings,
    }


def _summarize_voc_auto_review(
    *,
    documents: list[dict[str, Any]],
    errors: list[dict[str, str]],
    candidate_count: int,
) -> dict[str, Any]:
    counter = Counter(
        str((document.get("autoReview") or {}).get("publishTier") or "unknown")
        for document in documents
    )
    publish_ready = sum(
        1
        for document in documents
        if str((document.get("autoReview") or {}).get("publishDecision") or "")
        != "hold_raw"
    )
    if int(counter.get("high", 0)) > 0:
        publish_tier = "high"
        publish_decision = "auto_publish"
    elif int(counter.get("medium", 0)) > 0 and publish_ready > 0:
        publish_tier = "medium"
        publish_decision = "candidate_publish"
    elif documents:
        publish_tier = "low"
        publish_decision = "hold_raw"
    else:
        publish_tier = None
        publish_decision = None
    return {
        "version": "voc-auto-review-v1",
        "candidateCount": candidate_count,
        "reviewedCount": len(documents),
        "publishReadyCount": publish_ready,
        "heldRawCount": max(0, len(documents) - publish_ready),
        "errorCount": len(errors),
        "publishTier": publish_tier,
        "publishDecision": publish_decision,
        "tierCounts": {
            "high": int(counter.get("high", 0)),
            "medium": int(counter.get("medium", 0)),
            "low": int(counter.get("low", 0)),
        },
    }


class _FallbackPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[dict[str, str]] = []
        self._current_link: dict[str, str] | None = None
        self._in_title = False
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self.meta: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {key.lower(): (value or "") for key, value in attrs}
        tag_name = tag.lower()
        if tag_name == "a":
            href = attrs_map.get("href", "").strip()
            self._current_link = {"href": href, "text": ""}
        elif tag_name == "title":
            self._in_title = True
        elif tag_name == "meta":
            name = attrs_map.get("name", "").strip().lower()
            prop = attrs_map.get("property", "").strip().lower()
            content = attrs_map.get("content", "").strip()
            if content:
                if name:
                    self.meta[name] = content
                if prop:
                    self.meta[prop] = content

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.lower()
        if tag_name == "a" and self._current_link is not None:
            href = self._current_link.get("href", "").strip()
            if href:
                self.links.append(
                    {
                        "href": href,
                        "text": _normalize_space(self._current_link.get("text", "")),
                    }
                )
            self._current_link = None
        elif tag_name == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        normalized = _normalize_space(data)
        if not normalized:
            return
        if self._in_title:
            self.title_parts.append(normalized)
        if self._current_link is not None:
            current = self._current_link.get("text", "")
            self._current_link["text"] = f"{current} {normalized}".strip()
        self.text_parts.append(normalized)


def _extract_page_fields(page_url: str, html_text: str) -> dict[str, Any]:
    if lxml_html is not None:
        tree = lxml_html.fromstring(html_text)
        title = _normalize_space(
            tree.xpath("string(//meta[@property='og:title']/@content)")
            or tree.xpath("string(//title)")
            or tree.xpath("string(//h1[1])")
        )
        published_at = _normalize_space(
            tree.xpath("string(//meta[@property='article:published_time']/@content)")
            or tree.xpath("string(//meta[@name='article:published_time']/@content)")
            or tree.xpath("string(//time[1]/@datetime)")
        )
        summary = _normalize_space(
            tree.xpath("string(//meta[@name='description']/@content)")
            or tree.xpath("string(//meta[@property='og:description']/@content)")
        )
        fallback_text_nodes = [
            _normalize_space(node)
            for node in tree.xpath(_TEXT_TARGET_XPATH)
            if _normalize_space(node)
        ]
        fallback_text = _normalize_space(" ".join(fallback_text_nodes))
        trafilatura_text = None
        if trafilatura_extract is not None:
            trafilatura_text = _normalize_space(
                trafilatura_extract(
                    html_text,
                    url=page_url,
                    fast=True,
                    favor_precision=True,
                    include_comments=True,
                    include_tables=False,
                    deduplicate=True,
                    output_format="txt",
                ),
            )
        text = fallback_text
        text_method = "lxml_xpath"
        if trafilatura_text:
            trafilatura_word_count = len(trafilatura_text.split())
            minimum_viable_length = max(80, min(200, len(fallback_text) // 4))
            if (
                len(trafilatura_text) >= 120
                or trafilatura_word_count >= 20
                or (
                    trafilatura_word_count >= 10
                    and len(trafilatura_text) >= minimum_viable_length
                )
                or not fallback_text
            ):
                text = trafilatura_text
                text_method = "trafilatura"
        content_units = _extract_content_units_from_tree(tree, page_url=page_url)
        if not content_units:
            content_units = _derive_content_units_from_text(text, page_url=page_url)
        links = []
        for element in tree.xpath("//a[@href]"):
            href = _normalize_space(element.attrib.get("href"))
            if not href:
                continue
            links.append(
                {
                    "href": href,
                    "text": _normalize_space(" ".join(element.itertext())),
                }
            )
        return {
            "url": page_url,
            "title": title,
            "publishedAt": published_at or None,
            "summary": summary or None,
            "text": text,
            "textExtraction": {
                "method": text_method,
            },
            "contentUnits": content_units,
            "links": links,
        }

    parser = _FallbackPageParser()
    parser.feed(html_text)
    parser_text = _normalize_space(" ".join(parser.text_parts))
    return {
        "url": page_url,
        "title": _normalize_space(" ".join(parser.title_parts)),
        "publishedAt": (
            parser.meta.get("article:published_time")
            or parser.meta.get("published_time")
            or None
        ),
        "summary": (
            parser.meta.get("description")
            or parser.meta.get("og:description")
            or None
        ),
        "text": parser_text,
        "textExtraction": {
            "method": "html_parser",
        },
        "contentUnits": _derive_content_units_from_text(parser_text, page_url=page_url),
        "links": parser.links,
    }


def _score_candidate_link(
    *,
    source: VocSourceConfig,
    url: str,
    link_text: str,
) -> float:
    normalized_url = url.casefold()
    normalized_text = link_text.casefold()
    haystack = f"{normalized_url} {normalized_text}".strip()
    if not haystack:
        return -1.0
    if any(hint in normalized_url for hint in _NEGATIVE_LINK_HINTS):
        return -1.0

    score = 0.0
    site_type_hints = _POSITIVE_HINTS_BY_SITE_TYPE.get(
        source.site_type,
        _POSITIVE_HINTS_BY_SITE_TYPE["forum"],
    )
    if any(hint in haystack for hint in site_type_hints):
        score += 2.0
    if re.search(r"/20\d{2}/|-\d{4,}", normalized_url):
        score += 1.0
    if len(normalized_text) >= 24:
        score += 0.5
    if source.site_type in {"forum", "ev_community"} and "forum" in normalized_url:
        score += 0.75
    if source.site_type in {"media_comments", "consumer_media", "industry_media"} and any(
        hint in normalized_url for hint in ("news", "article", "test", "review", "story")
    ):
        score += 0.75
    return score


def _select_candidate_links(
    *,
    source: VocSourceConfig,
    landing_page: dict[str, Any],
    max_links: int,
) -> list[dict[str, str]]:
    candidates: list[tuple[float, str, str]] = []
    seen: set[str] = set()
    for link in landing_page.get("links") or []:
        href = _normalize_space(link.get("href"))
        if not href:
            continue
        absolute = urljoin(source.site_url, href)
        if not absolute.startswith(("http://", "https://")):
            continue
        if not _same_site(source.site_url, absolute):
            continue
        normalized_absolute = absolute.rstrip("/")
        if normalized_absolute in seen:
            continue
        seen.add(normalized_absolute)
        score = _score_candidate_link(
            source=source,
            url=absolute,
            link_text=_normalize_space(link.get("text")),
        )
        if score <= 0:
            continue
        candidates.append((score, normalized_absolute, _normalize_space(link.get("text"))))
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [
        {"url": url, "linkText": link_text}
        for _, url, link_text in candidates[: max(1, int(max_links))]
    ]


def _http_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (compatible; JATOAnalysisSystem/1.0; "
            "+https://github.com/tristan419/JATO_Analysis_System)"
        )
    }


def fetch_public_page(url: str, timeout_seconds: int = 20) -> dict[str, Any]:
    response = requests.get(
        url,
        timeout=timeout_seconds,
        allow_redirects=True,
        headers=_http_headers(),
    )
    response.raise_for_status()
    html_text = response.text
    page = _extract_page_fields(str(response.url), html_text)
    page["statusCode"] = response.status_code
    return page


def collect_source_documents(
    source: VocSourceConfig,
    *,
    taxonomy_profile: str,
    max_links: int = 5,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    collected_at = datetime.now(UTC).isoformat()
    strategy = get_source_collection_strategy(source.site_type)
    taxonomy = get_voc_taxonomy_profile(taxonomy_profile)
    try:
        landing_page = fetch_public_page(source.site_url, timeout_seconds=timeout_seconds)
    except requests.RequestException as exc:
        errors = [{"url": source.site_url, "error": str(exc)}]
        return {
            "source": asdict(source),
            "taxonomyProfile": taxonomy_profile,
            "taxonomy": taxonomy,
            "collectionStrategy": strategy,
            "collectedAt": collected_at,
            "autoReview": _summarize_voc_auto_review(
                documents=[],
                errors=errors,
                candidate_count=0,
            ),
            "landingPage": {
                "url": source.site_url,
                "title": None,
                "publishedAt": None,
                "summary": None,
                "candidateCount": 0,
            },
            "documentCount": 0,
            "documents": [],
            "errors": errors,
        }
    candidates = _select_candidate_links(
        source=source,
        landing_page=landing_page,
        max_links=max_links,
    )
    documents: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for candidate in candidates:
        target_url = str(candidate["url"]).strip()
        try:
            page = fetch_public_page(target_url, timeout_seconds=timeout_seconds)
        except requests.RequestException as exc:
            errors.append({"url": target_url, "error": str(exc)})
            continue
        raw_text = _normalize_space(page.get("text"))
        if not raw_text:
            continue
        documents.append(
            {
                "sourceCode": source.source_code,
                "countryCode": source.country_code,
                "countryLabel": source.country_label,
                "siteName": source.site_name,
                "siteType": source.site_type,
                "language": source.language,
                "url": page.get("url"),
                "title": page.get("title"),
                "pageKind": strategy["primaryUnit"],
                "linkText": candidate.get("linkText"),
                "publishedAt": page.get("publishedAt"),
                "summary": page.get("summary"),
                "rawText": raw_text,
                "textExtraction": page.get("textExtraction"),
                "contentUnits": list(page.get("contentUnits") or []),
                "excerpt": raw_text[:400],
                "collectedAt": collected_at,
            }
        )

    if not documents:
        landing_text = _normalize_space(landing_page.get("text"))
        if landing_text:
            documents.append(
                {
                    "sourceCode": source.source_code,
                    "countryCode": source.country_code,
                    "countryLabel": source.country_label,
                    "siteName": source.site_name,
                    "siteType": source.site_type,
                    "language": source.language,
                    "url": landing_page.get("url"),
                    "title": landing_page.get("title"),
                    "pageKind": "landing_page",
                    "linkText": None,
                    "publishedAt": landing_page.get("publishedAt"),
                    "summary": landing_page.get("summary"),
                    "rawText": landing_text,
                    "textExtraction": landing_page.get("textExtraction"),
                    "contentUnits": list(landing_page.get("contentUnits") or []),
                    "excerpt": landing_text[:400],
                    "collectedAt": collected_at,
                }
            )

    reviewed_documents = []
    for document in documents:
        reviewed_document = dict(document)
        reviewed_document["autoReview"] = _score_voc_document(
            source=source,
            document=reviewed_document,
        )
        reviewed_documents.append(reviewed_document)

    auto_review = _summarize_voc_auto_review(
        documents=reviewed_documents,
        errors=errors,
        candidate_count=len(candidates),
    )

    return {
        "source": asdict(source),
        "taxonomyProfile": taxonomy_profile,
        "taxonomy": taxonomy,
        "collectionStrategy": strategy,
        "collectedAt": collected_at,
        "autoReview": auto_review,
        "landingPage": {
            "url": landing_page.get("url"),
            "title": landing_page.get("title"),
            "publishedAt": landing_page.get("publishedAt"),
            "summary": landing_page.get("summary"),
            "textExtraction": landing_page.get("textExtraction"),
            "candidateCount": len(candidates),
        },
        "documentCount": len(reviewed_documents),
        "documents": reviewed_documents,
        "errors": errors,
    }


def build_voc_raw_collection(
    batch: VocBatchConfig,
    *,
    country_filter: set[str] | None = None,
    output_root: str | Path = "04_Processed_data/voc",
    max_links_per_source: int = 5,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    root = _resolve_repo_path(output_root)
    countries_payload: list[dict[str, Any]] = []
    source_count = 0
    document_count = 0

    for country in batch.countries:
        if country_filter and country.country_code.upper() not in country_filter:
            continue
        raw_root = root / country.country_code.lower() / "raw"
        raw_root.mkdir(parents=True, exist_ok=True)
        source_payloads: list[dict[str, Any]] = []
        for source in country.sources:
            collected = collect_source_documents(
                source,
                taxonomy_profile=country.taxonomy_profile,
                max_links=max_links_per_source,
                timeout_seconds=timeout_seconds,
            )
            output_path = raw_root / f"{source.source_code}.json"
            output_path.write_text(
                json.dumps(collected, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            source_payloads.append(
                {
                    "source_code": source.source_code,
                    "site_name": source.site_name,
                    "site_type": source.site_type,
                    "document_count": collected["documentCount"],
                    "publish_ready_count": int(
                        ((collected.get("autoReview") or {}).get("publishReadyCount") or 0),
                    ),
                    "error_count": len(collected["errors"]),
                    "output_path": str(output_path),
                }
            )
            source_count += 1
            document_count += int(collected["documentCount"])
        countries_payload.append(
            {
                "country_code": country.country_code,
                "country_label": country.country_label,
                "taxonomy_profile": country.taxonomy_profile,
                "source_count": len(country.sources),
                "document_count": sum(
                    int(item["document_count"]) for item in source_payloads
                ),
                "sources": source_payloads,
            }
        )

    return {
        "batch_code": batch.batch_code,
        "description": batch.description,
        "country_count": len(countries_payload),
        "source_count": source_count,
        "document_count": document_count,
        "countries": countries_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collect lightweight public VOC raw documents from configured sources.",
    )
    parser.add_argument(
        "--batch-files",
        nargs="+",
        required=True,
        help="Batch YAML files under voc_sources/ or absolute paths.",
    )
    parser.add_argument(
        "--countries",
        nargs="*",
        help="Optional list of country codes to keep.",
    )
    parser.add_argument(
        "--output-root",
        default="04_Processed_data/voc",
        help="Root path for country raw output.",
    )
    parser.add_argument(
        "--max-links-per-source",
        type=int,
        default=5,
        help="Maximum number of article or thread links to fetch per source.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=20,
        help="HTTP timeout per page request.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional summary JSON output path.",
    )
    args = parser.parse_args(argv)

    payload = [
        build_voc_raw_collection(
            load_voc_batch_config(batch_file),
            country_filter=_normalize_country_filter(args.countries),
            output_root=args.output_root,
            max_links_per_source=max(1, args.max_links_per_source),
            timeout_seconds=max(5, args.timeout_seconds),
        )
        for batch_file in args.batch_files
    ]
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        output_path = _resolve_repo_path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
