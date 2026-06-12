"""Heuristic AI-style enrichment for country news raw artifacts.

This module turns the existing RSS raw batch output into deterministic market
events, evidence cards, entity hints, and weekly digest artifacts. It is a
network-free fallback contract; hosted LLM enrichment can replace or augment
these fields later without changing downstream consumers.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
from pathlib import Path
import re
from typing import Any, Iterable, Sequence
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENRICHED_OUTPUT_ROOT = REPO_ROOT / "04_Processed_data" / "news" / "ai_enriched"
SCHEMA_VERSION = "news_ai_enrichment_v1"
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9+.\-]*", flags=re.IGNORECASE)
_NON_KEY_RE = re.compile(r"[^a-z0-9]+")

_EVENT_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "policy_regulation",
        (
            "policy",
            "regulation",
            "regulatory",
            "rules",
            "law",
            "legislation",
            "co2 tax",
            "malus",
            "bonus",
            "emissions",
        ),
    ),
    (
        "incentive_subsidy",
        (
            "incentive",
            "subsidy",
            "subsidies",
            "grant",
            "support scheme",
            "tax benefit",
            "bonus écologique",
        ),
    ),
    (
        "pricing_event",
        (
            "price",
            "prices",
            "pricing",
            "msrp",
            "discount",
            "cheaper",
            "affordable",
            "expensive",
        ),
    ),
    (
        "launch_product",
        (
            "launch",
            "launches",
            "launched",
            "unveils",
            "reveals",
            "new model",
            "goes on sale",
        ),
    ),
    (
        "sales_demand",
        (
            "sales",
            "registrations",
            "demand",
            "market share",
            "growth",
            "decline",
            "orders",
        ),
    ),
    (
        "charging_infrastructure",
        (
            "charging",
            "charger",
            "fast-charging",
            "infrastructure",
            "charging network",
        ),
    ),
    (
        "recall_quality",
        (
            "recall",
            "defect",
            "software issue",
            "safety issue",
            "investigation",
        ),
    ),
    (
        "production_supply",
        (
            "factory",
            "plant",
            "production",
            "supply",
            "battery supply",
            "manufacturing",
        ),
    ),
    (
        "competition",
        (
            "competition",
            "competitive",
            "pricing pressure",
            "chinese brand",
            "chinese-brand",
            "tariff",
        ),
    ),
)
_POWERTRAIN_RULES = {
    "BEV": ("bev", "electric vehicle", "electric vehicles", "evs", "full electric"),
    "PHEV": ("phev", "plug-in hybrid", "plug in hybrid"),
    "HEV": ("hev", "hybrid", "mild hybrid"),
    "ICE": ("diesel", "petrol", "gasoline", "combustion"),
}
_BRAND_RULES = {
    "AUDI": ("audi",),
    "BMW": ("bmw",),
    "BYD": ("byd",),
    "CITROEN": ("citroen", "citroën"),
    "DACIA": ("dacia",),
    "FORD": ("ford",),
    "HYUNDAI": ("hyundai",),
    "KIA": ("kia",),
    "MERCEDES-BENZ": ("mercedes", "mercedes-benz"),
    "NISSAN": ("nissan",),
    "PEUGEOT": ("peugeot",),
    "RENAULT": ("renault",),
    "SKODA": ("skoda", "škoda"),
    "TESLA": ("tesla",),
    "TOYOTA": ("toyota",),
    "VOLKSWAGEN": ("volkswagen", "vw"),
    "VOLVO": ("volvo",),
}
_MODEL_RULES = {
    "MODEL Y": ("model y",),
    "XC60": ("xc60",),
    "EX30": ("ex30",),
    "ID.4": ("id.4", "id 4"),
    "ENYAQ": ("enyaq",),
    "RAV4": ("rav4", "rav 4"),
    "TUCSON": ("tucson",),
    "SPORTAGE": ("sportage",),
    "QASHQAI": ("qashqai",),
}
_POSITIVE_HINTS = (
    "growth",
    "rise",
    "rises",
    "surge",
    "boost",
    "support",
    "incentive",
    "strong",
    "record",
    "affordable",
)
_NEGATIVE_HINTS = (
    "decline",
    "falls",
    "drop",
    "uncertainty",
    "tariff",
    "recall",
    "cuts",
    "ends",
    "expensive",
    "weak",
    "lag",
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _tokens(value: str) -> set[str]:
    return {token.casefold() for token in _TOKEN_RE.findall(value)}


def _slug(value: Any, default: str = "unknown") -> str:
    normalized = _NON_KEY_RE.sub("_", _text(value).casefold()).strip("_")
    return normalized or default


def _country_code(value: Any) -> str:
    return _text(value).upper()


def _country_name(country_label: str, country_code: str) -> str:
    label = _text(country_label)
    if label:
        return label.split("/", 1)[0].strip()
    return country_code.upper()


def _article_text(article: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in (
            _text(article.get("title")),
            _text(article.get("summary")),
            _text(article.get("publisher")),
            " ".join(_text(tag) for tag in article.get("tags") or []),
        )
        if part
    )


def _first_sentence(*values: Any, limit: int = 220) -> str:
    joined = " ".join(_text(value) for value in values if _text(value))
    if not joined:
        return ""
    sentence = re.split(r"(?<=[.!?])\s+", joined, maxsplit=1)[0].strip()
    return sentence[:limit]


def _infer_event_type(text: str) -> str:
    lowered = text.casefold()
    for event_type, hints in _EVENT_RULES:
        if any(hint in lowered for hint in hints):
            return event_type
    return "market_update"


def _infer_tags(text: str, existing_tags: Iterable[Any]) -> list[str]:
    tags = {_text(tag).casefold() for tag in existing_tags if _text(tag)}
    event_type = _infer_event_type(text)
    tags.add(event_type)
    if any(token in text.casefold() for token in ("ev", "bev", "electric")):
        tags.add("ev")
    if any(token in text.casefold() for token in ("tax", "subsid", "incentive")):
        tags.add("policy")
    return sorted(tags)


def _infer_entities(
    text: str,
    *,
    country_code: str,
    country_label: str,
) -> dict[str, Any]:
    lowered = text.casefold()
    brands = [
        brand
        for brand, aliases in _BRAND_RULES.items()
        if any(alias in lowered for alias in aliases)
    ]
    models = [
        model
        for model, aliases in _MODEL_RULES.items()
        if any(alias in lowered for alias in aliases)
    ]
    powertrains = [
        powertrain
        for powertrain, aliases in _POWERTRAIN_RULES.items()
        if any(alias in lowered for alias in aliases)
    ]
    return {
        "countries": [
            {
                "countryCode": country_code.upper(),
                "countryName": _country_name(country_label, country_code),
            }
        ],
        "brands": brands,
        "models": models,
        "powertrains": powertrains,
    }


def _infer_sentiment_and_impact(text: str, event_type: str) -> tuple[str, str]:
    lowered = text.casefold()
    positive = sum(1 for hint in _POSITIVE_HINTS if hint in lowered)
    negative = sum(1 for hint in _NEGATIVE_HINTS if hint in lowered)
    if negative > positive:
        sentiment = "negative"
    elif positive > negative:
        sentiment = "positive"
    else:
        sentiment = "neutral"

    if event_type in {"policy_regulation", "incentive_subsidy", "pricing_event"}:
        impact = "high"
    elif event_type in {"sales_demand", "competition", "launch_product"}:
        impact = "medium"
    else:
        impact = "monitor"
    if sentiment == "negative" and impact == "monitor":
        impact = "medium"
    return sentiment, impact


def _source_tier(article: dict[str, Any]) -> str:
    publisher = _text(article.get("publisher")).casefold()
    source_code = _text(article.get("source_code") or article.get("sourceCode")).casefold()
    tags = {_text(tag).casefold() for tag in article.get("tags") or []}
    if "google news" in publisher:
        return "aggregator"
    if any(token in publisher for token in ("government", "ministry", "commission")):
        return "official"
    if any(token in publisher for token in ("acea", "transport & environment")):
        return "association"
    if any(token in publisher for token in ("reuters", "bloomberg", "automotive news")):
        return "trusted_media"
    if "local" in tags or "local" in source_code:
        return "local_media"
    return "standard_media"


def _confidence(article: dict[str, Any], event_type: str) -> float:
    score = 0.35
    if urlparse(_text(article.get("url"))).netloc:
        score += 0.15
    if len(_text(article.get("title"))) >= 24:
        score += 0.1
    if len(_text(article.get("summary"))) >= 40:
        score += 0.1
    if event_type != "market_update":
        score += 0.1
    tier = _source_tier(article)
    if tier in {"official", "association", "trusted_media", "local_media"}:
        score += 0.1
    return round(min(score, 0.95), 2)


def enrich_news_article(
    article: dict[str, Any],
    *,
    country_code: str | None = None,
    country_label: str | None = None,
) -> dict[str, Any]:
    resolved_country_code = _country_code(
        country_code or article.get("country_code") or article.get("countryCode")
    )
    resolved_country_label = _text(
        country_label or article.get("country_label") or article.get("countryLabel")
    )
    text = _article_text(article)
    event_type = _infer_event_type(text)
    sentiment, market_impact = _infer_sentiment_and_impact(text, event_type)
    entities = _infer_entities(
        text,
        country_code=resolved_country_code,
        country_label=resolved_country_label,
    )
    title = _text(article.get("title"))
    publisher = _text(article.get("publisher"))
    summary = _text(article.get("summary"))
    url = _text(article.get("url"))
    published_at = _text(article.get("published_at") or article.get("publishedAt"))
    confidence = _confidence(article, event_type)
    event_id = (
        f"news:{resolved_country_code.lower()}:"
        f"{_slug(article.get('source_code') or article.get('sourceCode'))}:"
        f"{_slug(title)[:48]}"
    )
    excerpt = _first_sentence(summary, title, limit=260)
    tags = _infer_tags(text, article.get("tags") or [])

    return {
        "eventId": event_id,
        "countryCode": resolved_country_code,
        "countryLabel": resolved_country_label,
        "sourceCode": _text(article.get("source_code") or article.get("sourceCode")),
        "publisher": publisher,
        "sourceTier": _source_tier(article),
        "title": title,
        "url": url,
        "summary": summary,
        "publishedAt": published_at or None,
        "eventType": event_type,
        "marketImpact": market_impact,
        "sentiment": sentiment,
        "confidence": confidence,
        "tags": tags,
        "relatedEntities": entities,
        "evidenceCard": {
            "title": title,
            "url": url,
            "publisher": publisher,
            "publishedAt": published_at or None,
            "excerpt": excerpt,
            "supportedClaim": _supported_claim(
                country_code=resolved_country_code,
                event_type=event_type,
                title=title,
            ),
            "confidence": confidence,
        },
        "translation": {
            "mode": "heuristic_template",
            "sourceLanguage": (article.get("raw_payload") or {}).get("language")
            or article.get("language"),
            "zhSummary": _zh_summary(
                country_code=resolved_country_code,
                event_type=event_type,
                title=title,
            ),
        },
    }


def _supported_claim(*, country_code: str, event_type: str, title: str) -> str:
    label = event_type.replace("_", " ")
    return f"{country_code.upper()} has a {label} signal: {title}".strip()


def _zh_summary(*, country_code: str, event_type: str, title: str) -> str:
    labels = {
        "policy_regulation": "政策/法规信号",
        "incentive_subsidy": "补贴/激励信号",
        "pricing_event": "价格信号",
        "launch_product": "新车/产品信号",
        "sales_demand": "销量/需求信号",
        "charging_infrastructure": "充电基础设施信号",
        "recall_quality": "质量/召回信号",
        "production_supply": "生产/供应信号",
        "competition": "竞争格局信号",
        "market_update": "市场动态信号",
    }
    return f"{country_code.upper()} {labels.get(event_type, '市场动态信号')}：{title}"


def _week_key(value: str | None, fallback: str) -> str:
    raw = _text(value)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(fallback.replace("Z", "+00:00"))
        except ValueError:
            parsed = datetime.now(UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    iso = parsed.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _build_weekly_digest(
    *,
    country_code: str,
    country_label: str,
    events: list[dict[str, Any]],
    generated_at_utc: str,
) -> dict[str, Any] | None:
    if not events:
        return None
    week_counts = Counter(
        _week_key(event.get("publishedAt"), generated_at_utc)
        for event in events
    )
    digest_week = week_counts.most_common(1)[0][0]
    event_type_counts = Counter(event["eventType"] for event in events)
    impact_counts = Counter(event["marketImpact"] for event in events)
    top_events = sorted(
        events,
        key=lambda item: (float(item.get("confidence") or 0), item.get("publishedAt") or ""),
        reverse=True,
    )[:5]
    country_name = _country_name(country_label, country_code)
    headline = (
        f"{country_name} weekly market digest: "
        f"{event_type_counts.most_common(1)[0][0].replace('_', ' ')} leads"
    )
    return {
        "digestWeek": digest_week,
        "headline": headline,
        "summary": (
            f"{len(events)} enriched news events for {country_name}; "
            f"{impact_counts.get('high', 0)} high-impact signals."
        ),
        "eventTypeCounts": dict(sorted(event_type_counts.items())),
        "marketImpactCounts": dict(sorted(impact_counts.items())),
        "highlights": [
            event["translation"]["zhSummary"]
            for event in top_events[:3]
        ],
        "evidenceCards": [
            event["evidenceCard"]
            for event in top_events
        ],
        "llmStatus": "not_requested",
    }


def _flatten_batches(raw_payload: Any) -> list[dict[str, Any]]:
    if isinstance(raw_payload, list):
        return [item for item in raw_payload if isinstance(item, dict)]
    if isinstance(raw_payload, dict):
        return [raw_payload]
    raise ValueError("News raw payload must be a JSON object or list of objects")


def build_news_enrichment(
    raw_payload: Any,
    *,
    required_countries: Sequence[str] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    generated_at = generated_at_utc or _utc_now_iso()
    required = {
        _country_code(country)
        for country in (required_countries or [])
        if _text(country)
    }
    batches = _flatten_batches(raw_payload)
    batch_codes = [
        _text(batch.get("batch_code") or batch.get("batchCode"))
        for batch in batches
        if _text(batch.get("batch_code") or batch.get("batchCode"))
    ]
    country_outputs: list[dict[str, Any]] = []
    flat_events: list[dict[str, Any]] = []
    warnings: list[str] = []

    for batch in batches:
        for country in batch.get("countries") or []:
            if not isinstance(country, dict):
                continue
            country_code = _country_code(
                country.get("country_code") or country.get("countryCode")
            )
            if required and country_code not in required:
                continue
            country_label = _text(
                country.get("country_label") or country.get("countryLabel")
            )
            articles = [
                item
                for item in (country.get("articles") or [])
                if isinstance(item, dict)
            ]
            events = [
                enrich_news_article(
                    article,
                    country_code=country_code,
                    country_label=country_label,
                )
                for article in articles
            ]
            flat_events.extend(events)
            event_type_counts = Counter(event["eventType"] for event in events)
            source_tier_counts = Counter(event["sourceTier"] for event in events)
            confidence_values = [float(event["confidence"]) for event in events]
            country_outputs.append(
                {
                    "countryCode": country_code,
                    "countryLabel": country_label,
                    "articleCount": len(articles),
                    "marketEventCount": len(events),
                    "eventTypeCounts": dict(sorted(event_type_counts.items())),
                    "sourceTierCounts": dict(sorted(source_tier_counts.items())),
                    "confidenceAvg": (
                        round(sum(confidence_values) / len(confidence_values), 3)
                        if confidence_values
                        else None
                    ),
                    "marketEvents": events,
                    "weeklyDigest": _build_weekly_digest(
                        country_code=country_code,
                        country_label=country_label,
                        events=events,
                        generated_at_utc=generated_at,
                    ),
                }
            )

    observed_countries = {country["countryCode"] for country in country_outputs}
    for country in sorted(required - observed_countries):
        warnings.append(f"missing_country:{country.lower()}")

    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAtUtc": generated_at,
        "batchCodes": batch_codes,
        "countryCount": len(country_outputs),
        "articleCount": sum(country["articleCount"] for country in country_outputs),
        "marketEventCount": len(flat_events),
        "eventTypeCounts": dict(sorted(Counter(event["eventType"] for event in flat_events).items())),
        "countries": country_outputs,
        "warnings": warnings,
    }


def _resolve_repo_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return REPO_ROOT / candidate


def write_news_enrichment_output(
    payload: dict[str, Any],
    *,
    output_path: str | Path | None = None,
    output_root: str | Path = DEFAULT_ENRICHED_OUTPUT_ROOT,
) -> Path:
    if output_path is None:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        destination = _resolve_repo_path(output_root) / f"news_ai_enriched_{timestamp}.json"
    else:
        destination = _resolve_repo_path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return destination


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build AI-style market events and weekly digest from news raw JSON.",
    )
    parser.add_argument("--input", required=True, help="Raw news JSON from news_runner.")
    parser.add_argument("--output", help="Optional enriched JSON output path.")
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_ENRICHED_OUTPUT_ROOT.relative_to(REPO_ROOT)),
    )
    parser.add_argument(
        "--required-countries",
        default="",
        help="Comma-separated country codes expected in the raw payload.",
    )
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)

    raw = json.loads(_resolve_repo_path(args.input).read_text(encoding="utf-8"))
    required_countries = [
        part.strip()
        for part in str(args.required_countries).split(",")
        if part.strip()
    ]
    payload = build_news_enrichment(raw, required_countries=required_countries)
    write_news_enrichment_output(
        payload,
        output_path=args.output,
        output_root=args.output_root,
    )
    if not args.output:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.strict and payload["warnings"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
