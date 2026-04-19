from __future__ import annotations

from datetime import UTC
from datetime import datetime
import re
from typing import Any

from app.services import local_wiki_service

_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-]*|[\u4e00-\u9fff]+")
_TOPIC_PHRASES = {
    "policy": (
        "policy",
        "policies",
        "regulation",
        "regulations",
        "法规",
        "政策",
        "rules",
        "rule",
    ),
    "incentive": (
        "incentive",
        "incentives",
        "subsidy",
        "subsidies",
        "补贴",
        "激励",
        "支持",
        "support",
    ),
    "tax": (
        "tax",
        "taxes",
        "taxation",
        "company-car tax",
        "company car tax",
        "税",
        "税收",
    ),
    "tariff": (
        "tariff",
        "tariffs",
        "duty",
        "duties",
        "关税",
    ),
    "competition": (
        "competition",
        "competitive",
        "pricing pressure",
        "pressure",
        "中国品牌",
        "chinese brand",
        "chinese-brand",
        "竞争",
        "竞品",
        "价格战",
    ),
    "pricing": (
        "price",
        "prices",
        "pricing",
        "msrp",
        "售价",
        "价格",
        "定价",
        "价位",
    ),
    "ev": (
        "ev",
        "bev",
        "phev",
        "electric vehicle",
        "electric vehicles",
        "electric",
        "electrification",
        "新能源",
        "电动车",
        "纯电",
        "插混",
    ),
    "production": (
        "production",
        "factory",
        "plant",
        "manufacturing",
        "产能",
        "工厂",
        "生产",
    ),
    "launch": (
        "launch",
        "launches",
        "released",
        "release",
        "上市",
        "发布",
        "新车",
    ),
    "fleet": (
        "fleet",
        "company-car",
        "company car",
        "leasing",
        "corporate",
        "车队",
        "企业用车",
        "租赁",
    ),
}
_STOP_TERMS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "what",
    "which",
    "country",
    "market",
    "latest",
    "news",
    "最近",
    "最新",
    "新闻",
    "市场",
}


def _similarity(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_phrase(value: Any) -> str:
    return _normalize_text(value).casefold()


def _tokenize_terms(value: Any) -> list[str]:
    normalized = _normalize_phrase(value)
    if not normalized:
        return []
    return [
        token
        for token in _TOKEN_PATTERN.findall(normalized)
        if len(token) >= 2 and token not in _STOP_TERMS
    ]


def _infer_topics(*values: Any) -> list[str]:
    joined = " ".join(_normalize_phrase(value) for value in values if _normalize_phrase(value))
    if not joined:
        return []
    return [
        topic
        for topic, phrases in _TOPIC_PHRASES.items()
        if any(phrase in joined for phrase in phrases)
    ]


def _parse_public_datetime(value: Any) -> datetime | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _freshness_bonus(value: Any) -> float:
    published_at = _parse_public_datetime(value)
    if published_at is None:
        return 0.0
    age = datetime.now(UTC) - published_at
    if age.days <= 7:
        return 0.04
    if age.days <= 30:
        return 0.02
    return 0.0


def _build_news_documents(
    *,
    news_digest: dict[str, Any] | None,
    market_events: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []

    digest = news_digest or {}
    headline = _normalize_text(digest.get("headline"))
    summary = _normalize_text(digest.get("summary"))
    highlights = [
        _normalize_text(item)
        for item in (digest.get("highlights") or [])
        if _normalize_text(item)
    ]
    if headline or summary or highlights:
        documents.append(
            {
                "kind": "digest",
                "title": headline or "News digest",
                "summary": summary,
                "publishedAt": _normalize_text(digest.get("updatedAt")),
                "provider": _normalize_text(digest.get("summaryProvider")),
                "model": _normalize_text(digest.get("summaryModel")),
                "tags": ["digest", "summary", "policy", "market"],
                "url": "",
                "search_text": " ".join(
                    [
                        headline,
                        summary,
                        " ".join(highlights),
                        _normalize_text(digest.get("countryLabel")),
                        _normalize_text(digest.get("summaryProvider")),
                        _normalize_text(digest.get("summaryModel")),
                    ]
                ).strip(),
            }
        )

    for event in market_events or []:
        title = _normalize_text(event.get("title"))
        summary = _normalize_text(event.get("summary"))
        publisher = _normalize_text(event.get("publisher"))
        tags = [
            _normalize_text(tag)
            for tag in (event.get("tags") or [])
            if _normalize_text(tag)
        ]
        if not title and not summary:
            continue
        documents.append(
            {
                "kind": "event",
                "title": title or publisher or "Market event",
                "summary": summary,
                "publisher": publisher,
                "publishedAt": _normalize_text(event.get("publishedAt")),
                "tags": tags,
                "url": _normalize_text(event.get("url")),
                "search_text": " ".join(
                    [
                        publisher,
                        title,
                        summary,
                        " ".join(tags),
                        _normalize_text(event.get("countryLabel")),
                    ]
                ).strip(),
            }
        )

    return documents


def _score_document_relevance(
    *,
    query: str,
    query_embedding: list[float],
    document: dict[str, Any],
) -> tuple[float, list[str], list[str], list[str]]:
    text = _normalize_text(document.get("search_text"))
    if not text:
        return 0.0, [], [], []

    base_score = _similarity(
        query_embedding,
        local_wiki_service.embed_text(text),
    )
    query_terms = set(_tokenize_terms(query))
    document_terms = set(_tokenize_terms(text))
    matched_terms = sorted(query_terms & document_terms)

    query_topics = set(_infer_topics(query))
    document_topics = set(
        _infer_topics(
            text,
            " ".join(document.get("tags") or []),
            document.get("title"),
            document.get("summary"),
        )
    )
    matched_topics = sorted(query_topics & document_topics)

    score = float(base_score)
    if matched_topics:
        score += 0.18 * min(3, len(matched_topics))
    if matched_terms:
        score += 0.05 * min(4, len(matched_terms))
    if document.get("kind") == "event" and (matched_topics or matched_terms):
        score += 0.05
    score += _freshness_bonus(document.get("publishedAt"))

    relevance_signals: list[str] = []
    relevance_signals.extend(f"topic:{topic}" for topic in matched_topics[:3])
    relevance_signals.extend(f"term:{term}" for term in matched_terms[:3])
    if document.get("kind") == "event" and (matched_topics or matched_terms):
        relevance_signals.append("kind:event")

    return score, matched_topics, matched_terms, relevance_signals


def query_news_wiki(
    query: str,
    *,
    news_digest: dict[str, Any] | None,
    market_events: list[dict[str, Any]] | None,
    limit: int = 4,
) -> list[dict[str, Any]]:
    normalized_query = _normalize_text(query)
    if not normalized_query:
        return []

    documents = _build_news_documents(
        news_digest=news_digest,
        market_events=market_events,
    )
    if not documents:
        return []

    query_embedding = local_wiki_service.embed_text(normalized_query)
    ranked = []
    for document in documents:
        score, matched_topics, matched_terms, relevance_signals = (
            _score_document_relevance(
                query=normalized_query,
                query_embedding=query_embedding,
                document=document,
            )
        )
        if not _normalize_text(document.get("search_text")):
            continue
        ranked.append(
            (
                score,
                matched_topics,
                matched_terms,
                relevance_signals,
                document,
            )
        )

    ranked.sort(
        key=lambda item: (
            item[0],
            len(item[1]),
            len(item[2]),
            1 if item[4].get("kind") == "event" else 0,
            _normalize_text(item[4].get("publishedAt")),
        ),
        reverse=True,
    )
    top_hits = []
    for score, matched_topics, matched_terms, relevance_signals, document in ranked[
        : max(1, int(limit))
    ]:
        top_hits.append(
            {
                "kind": document.get("kind"),
                "title": document.get("title"),
                "summary": document.get("summary"),
                "publisher": document.get("publisher"),
                "url": document.get("url"),
                "publishedAt": document.get("publishedAt"),
                "tags": document.get("tags", []),
                "provider": document.get("provider"),
                "model": document.get("model"),
                "matchedTopics": matched_topics,
                "matchedTerms": matched_terms[:5],
                "relevanceSignals": relevance_signals,
                "score": round(float(score), 4),
            }
        )
    return top_hits
