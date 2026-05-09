from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
import html
import json
import os
import re
import time
from typing import Any
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import quote_plus
from urllib.parse import urlencode
from urllib.request import Request
from urllib.request import urlopen
import xml.etree.ElementTree as ET


DEFAULT_SEARCH_TIMEOUT_SECONDS = max(
    1,
    min(3, int(os.getenv("APP_WEB_SEARCH_TIMEOUT_SECONDS", "3").strip() or "3")),
)
DEFAULT_SEARCH_TOTAL_TIMEOUT_SECONDS = max(
    3,
    min(
        10,
        int(os.getenv("APP_WEB_SEARCH_TOTAL_TIMEOUT_SECONDS", "6").strip() or "6"),
    ),
)

_LATIN_TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9-]{1,}")
_TAG_PATTERN = re.compile(r"<[^>]+>")
_COUNTRY_ALIASES = {
    "丹麦": "Denmark Danmark",
    "克罗地亚": "Croatia Hrvatska",
    "匈牙利": "Hungary Magyarország",
    "奥地利": "Austria Österreich",
    "希腊": "Greece",
    "德国": "Germany Deutschland",
    "意大利": "Italy Italia",
    "挪威": "Norway Norge",
    "捷克": "Czech Republic Czechia",
    "斯洛伐克": "Slovakia",
    "斯洛文尼亚": "Slovenia",
    "比利时": "Belgium Belgique België",
    "法国": "France",
    "波兰": "Poland Polska",
    "瑞典": "Sweden Sverige",
    "瑞士": "Switzerland Schweiz Suisse",
    "罗马尼亚": "Romania",
    "芬兰": "Finland Suomi",
    "荷兰": "Netherlands Nederland",
    "葡萄牙": "Portugal",
    "西班牙": "Spain España",
}
_QUESTION_NOISE_TOKENS = {
    "recent",
    "latest",
    "news",
    "market",
    "country",
    "explain",
    "about",
    "what",
    "when",
    "where",
    "have",
    "with",
}


@dataclass(frozen=True)
class WebSearchResult:
    title: str
    url: str
    snippet: str
    source: str
    publishedAt: str
    provider: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def search_market_news(
    *,
    country: str,
    question: str,
    limit: int = 6,
) -> list[dict[str, str]]:
    query = _build_market_news_query(country=country, question=question)
    providers = (
        _search_google_news_rss,
        _search_tavily,
        _search_google_custom_search,
        _search_serpapi,
    )
    deadline = time.monotonic() + DEFAULT_SEARCH_TOTAL_TIMEOUT_SECONDS
    for provider in providers:
        if time.monotonic() >= deadline:
            break
        results = provider(query=query, limit=limit)
        if results:
            deduped = _dedupe_results(results)
            ranked = sorted(
                enumerate(deduped),
                key=lambda item: (
                    -_score_result_relevance(item[1], query),
                    item[0],
                ),
            )
            return [result.to_dict() for _, result in ranked[:limit]]
    return []


def _build_market_news_query(*, country: str, question: str) -> str:
    tokens: list[str] = []
    seen: set[str] = set()
    for token in _LATIN_TOKEN_PATTERN.findall(question):
        normalized = token.strip()
        lowered = normalized.lower()
        if len(normalized) < 2 or lowered in _QUESTION_NOISE_TOKENS:
            continue
        if lowered in seen:
            continue
        tokens.append(normalized)
        seen.add(lowered)

    country_alias = _COUNTRY_ALIASES.get(str(country).strip(), str(country).strip())
    if tokens:
        return " ".join([*tokens, country_alias, "2026"])
    return " ".join(part for part in [question.strip(), country_alias, "automotive news"] if part)


def _search_tavily(*, query: str, limit: int) -> list[WebSearchResult]:
    api_key = os.getenv("TAVILY_API_KEY", "").strip()
    if not api_key:
        return []
    request = Request(
        "https://api.tavily.com/search",
        data=json.dumps(
            {
                "api_key": api_key,
                "query": query,
                "search_depth": "basic",
                "topic": "news",
                "max_results": limit,
                "include_answer": False,
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        payload = _read_json(request)
    except (HTTPError, URLError, TimeoutError, ValueError):
        return []
    items = payload.get("results") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return []
    return [
        WebSearchResult(
            title=_clean_text(item.get("title")),
            url=_clean_text(item.get("url")),
            snippet=_clean_text(item.get("content")),
            source=_source_from_url(item.get("url")),
            publishedAt=_clean_text(item.get("published_date")),
            provider="tavily",
        )
        for item in items
        if isinstance(item, dict) and _clean_text(item.get("title"))
    ]


def _search_google_custom_search(*, query: str, limit: int) -> list[WebSearchResult]:
    api_key = (
        os.getenv("GOOGLE_CUSTOM_SEARCH_API_KEY", "").strip()
        or os.getenv("GOOGLE_SEARCH_API_KEY", "").strip()
    )
    search_engine_id = (
        os.getenv("GOOGLE_CUSTOM_SEARCH_ENGINE_ID", "").strip()
        or os.getenv("GOOGLE_CSE_ID", "").strip()
    )
    if not api_key or not search_engine_id:
        return []
    params = urlencode(
        {
            "key": api_key,
            "cx": search_engine_id,
            "q": query,
            "num": max(1, min(limit, 10)),
        }
    )
    request = Request(
        f"https://www.googleapis.com/customsearch/v1?{params}",
        headers={"Accept": "application/json"},
    )
    try:
        payload = _read_json(request)
    except (HTTPError, URLError, TimeoutError, ValueError):
        return []
    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return []
    return [
        WebSearchResult(
            title=_clean_text(item.get("title")),
            url=_clean_text(item.get("link")),
            snippet=_clean_text(item.get("snippet")),
            source=_source_from_url(item.get("link")),
            publishedAt="",
            provider="google-cse",
        )
        for item in items
        if isinstance(item, dict) and _clean_text(item.get("title"))
    ]


def _search_serpapi(*, query: str, limit: int) -> list[WebSearchResult]:
    api_key = os.getenv("SERPAPI_API_KEY", "").strip()
    if not api_key:
        return []
    params = urlencode(
        {
            "engine": "google_news",
            "q": query,
            "api_key": api_key,
            "num": max(1, min(limit, 10)),
        }
    )
    request = Request(
        f"https://serpapi.com/search.json?{params}",
        headers={"Accept": "application/json"},
    )
    try:
        payload = _read_json(request)
    except (HTTPError, URLError, TimeoutError, ValueError):
        return []
    items = payload.get("news_results") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return []
    return [
        WebSearchResult(
            title=_clean_text(item.get("title")),
            url=_clean_text(item.get("link")),
            snippet=_clean_text(item.get("snippet")),
            source=_clean_text(item.get("source")) or _source_from_url(item.get("link")),
            publishedAt=_clean_text(item.get("date")),
            provider="serpapi",
        )
        for item in items
        if isinstance(item, dict) and _clean_text(item.get("title"))
    ]


def _search_google_news_rss(*, query: str, limit: int) -> list[WebSearchResult]:
    url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(request, timeout=DEFAULT_SEARCH_TIMEOUT_SECONDS) as response:
            root = ET.fromstring(response.read())
    except (HTTPError, URLError, TimeoutError, ET.ParseError):
        return []

    results: list[WebSearchResult] = []
    for item in root.findall("./channel/item")[:limit]:
        raw_title = _clean_text(item.findtext("title"))
        title, source = _split_google_news_title(raw_title)
        link = _clean_text(item.findtext("link"))
        snippet = _clean_text(item.findtext("description"))
        published_at = _format_rss_date(item.findtext("pubDate"))
        if title:
            results.append(
                WebSearchResult(
                    title=title,
                    url=link,
                    snippet=snippet,
                    source=source,
                    publishedAt=published_at,
                    provider="google-news-rss",
                )
            )
    return results


def _read_json(request: Request) -> dict[str, Any]:
    with urlopen(request, timeout=DEFAULT_SEARCH_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def _dedupe_results(results: list[WebSearchResult]) -> list[WebSearchResult]:
    deduped: list[WebSearchResult] = []
    seen: set[str] = set()
    for result in results:
        key = (result.url or result.title).strip().casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped


def _score_result_relevance(result: WebSearchResult, query: str) -> int:
    query_tokens = [
        token.casefold()
        for token in _LATIN_TOKEN_PATTERN.findall(query)
        if len(token) >= 3 and token.casefold() not in _QUESTION_NOISE_TOKENS
    ]
    if not query_tokens:
        return 0

    title = result.title.casefold()
    body = f"{result.title} {result.snippet} {result.source}".casefold()
    score = 0
    for token in dict.fromkeys(query_tokens):
        if token in title:
            score += 20
        elif token in body:
            score += 8
    return score


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = _TAG_PATTERN.sub(" ", text)
    return " ".join(text.split())


def _split_google_news_title(title: str) -> tuple[str, str]:
    if " - " not in title:
        return title, ""
    headline, source = title.rsplit(" - ", 1)
    return headline.strip(), source.strip()


def _format_rss_date(value: Any) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).date().isoformat()
    except (TypeError, ValueError):
        return raw


def _source_from_url(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    match = re.search(r"https?://([^/]+)", text)
    return match.group(1).removeprefix("www.") if match else ""
