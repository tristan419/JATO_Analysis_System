from __future__ import annotations

from typing import Any

from app.services import local_wiki_service


def _similarity(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


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
        text = _normalize_text(document.get("search_text"))
        if not text:
            continue
        score = _similarity(
            query_embedding,
            local_wiki_service.embed_text(text),
        )
        ranked.append((score, document))

    ranked.sort(key=lambda item: item[0], reverse=True)
    top_hits = []
    for score, document in ranked[: max(1, int(limit))]:
        top_hits.append(
            {
                "kind": document.get("kind"),
                "title": document.get("title"),
                "summary": document.get("summary"),
                "publisher": document.get("publisher"),
                "publishedAt": document.get("publishedAt"),
                "tags": document.get("tags", []),
                "provider": document.get("provider"),
                "model": document.get("model"),
                "score": round(float(score), 4),
            }
        )
    return top_hits