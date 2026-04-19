from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from sqlalchemy.orm import Session

from app.db.session import get_session_factory
from app.infra import msrp_repository


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_upper(value: object | None) -> str:
    return _normalize_text(value).upper()


def _normalize_model_queries(
    *,
    model: str | None = None,
    models: list[str] | None = None,
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in [model, *(models or [])]:
        normalized = _normalize_upper(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _normalize_powertrain(value: str | None) -> str:
    return _normalize_upper(value)


def _model_match_score(price: Any, query_model: str) -> int:
    if not query_model:
        return 1
    jato_model = _normalize_upper(getattr(price, "jato_model", None))
    official_model = _normalize_upper(getattr(price, "official_model", None))
    if jato_model == query_model:
        return 4
    if official_model == query_model:
        return 3
    if query_model in jato_model:
        return 2
    if query_model in official_model:
        return 1
    return 0


def lookup_current_msrp(
    session: Session,
    *,
    country: str,
    brand: str | None = None,
    model: str | None = None,
    models: list[str] | None = None,
    powertrain: str | None = None,
    limit_per_model: int = 4,
    max_items: int = 12,
) -> dict[str, Any]:
    query_models = _normalize_model_queries(model=model, models=models)
    normalized_powertrain = _normalize_powertrain(powertrain)

    candidate_by_key: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for query_model in query_models:
        prices = msrp_repository.list_current_prices(
            session,
            country=country,
            brand=brand,
            jato_model=query_model,
            limit=max(limit_per_model * 3, 12),
            offset=0,
        )
        ranked: list[dict[str, Any]] = []
        for price in prices:
            price_powertrain = _normalize_powertrain(
                getattr(price, "jato_powertrain", None)
            )
            if normalized_powertrain and price_powertrain != normalized_powertrain:
                continue
            score = _model_match_score(price, query_model)
            if score <= 0:
                continue
            ranked.append(
                {
                    "price": price,
                    "queryModel": query_model,
                    "score": score,
                    "updatedRank": (
                        getattr(price, "updated_at_utc", None).timestamp()
                        if getattr(price, "updated_at_utc", None) is not None
                        else 0.0
                    ),
                }
            )

        ranked.sort(
            key=lambda item: (
                -int(item["score"]),
                -float(item["updatedRank"]),
                float(getattr(item["price"], "current_msrp_value", 0.0) or 0.0),
            )
        )

        for item in ranked[: max(1, limit_per_model)]:
            price = item["price"]
            business_key = (
                _normalize_upper(getattr(price, "country", None)),
                _normalize_upper(getattr(price, "brand", None)),
                _normalize_upper(getattr(price, "jato_model", None)),
                _normalize_upper(getattr(price, "jato_trim", None)),
                _normalize_powertrain(getattr(price, "jato_powertrain", None)),
            )
            existing = candidate_by_key.get(business_key)
            if existing is None or (
                int(item["score"]),
                float(item["updatedRank"]),
            ) > (
                int(existing["score"]),
                float(existing["updatedRank"]),
            ):
                candidate_by_key[business_key] = item

    selected = sorted(
        candidate_by_key.values(),
        key=lambda item: (
            -int(item["score"]),
            -float(item["updatedRank"]),
            _normalize_upper(getattr(item["price"], "brand", None)),
            _normalize_upper(getattr(item["price"], "jato_model", None)),
            _normalize_upper(getattr(item["price"], "jato_trim", None)),
        ),
    )[: max(1, max_items)]

    observation_ids = [
        getattr(item["price"], "effective_observation_id", None)
        for item in selected
        if getattr(item["price"], "effective_observation_id", None) is not None
    ]
    observations = msrp_repository.list_observations_by_ids(session, observation_ids)
    observation_by_id = {item.observation_id: item for item in observations}
    source_ids = [
        getattr(item, "source_id", None)
        for item in observations
        if getattr(item, "source_id", None) is not None
    ]
    sources = msrp_repository.list_sources_by_ids(session, source_ids)
    source_by_id = {item.source_id: item for item in sources}

    items: list[dict[str, Any]] = []
    source_counter: Counter[int] = Counter()
    model_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    latest_updated_at = ""
    for selected_item in selected:
        price = selected_item["price"]
        observation = observation_by_id.get(price.effective_observation_id)
        source = source_by_id.get(getattr(observation, "source_id", None))
        source_tier = int(source.tier) if source and source.tier is not None else None
        if source_tier is not None:
            source_counter[source_tier] += 1
        updated_at = (
            price.updated_at_utc.isoformat()
            if getattr(price, "updated_at_utc", None) is not None
            else None
        )
        if updated_at and updated_at > latest_updated_at:
            latest_updated_at = updated_at
        payload = {
            "brand": _normalize_text(price.brand),
            "model": _normalize_text(price.jato_model),
            "trim": _normalize_text(price.jato_trim),
            "powertrain": _normalize_text(price.jato_powertrain),
            "officialModel": _normalize_text(price.official_model) or None,
            "officialTrim": _normalize_text(price.official_trim) or None,
            "officialEdition": _normalize_text(price.official_edition) or None,
            "msrp": float(price.current_msrp_value),
            "currency": _normalize_text(price.currency),
            "updatedAt": updated_at,
            "sourceCode": _normalize_text(getattr(source, "source_code", None)) or None,
            "sourceType": _normalize_text(getattr(source, "source_type", None)) or None,
            "sourceTier": source_tier,
            "sourceUrl": _normalize_text(price.source_url) or None,
            "matchedBy": selected_item["queryModel"],
        }
        items.append(payload)
        model_groups[payload["model"]].append(payload)

    model_summaries: list[dict[str, Any]] = []
    for model_name, grouped_items in sorted(model_groups.items()):
        msrps = [
            float(item["msrp"])
            for item in grouped_items
            if item.get("msrp") is not None
        ]
        model_summaries.append(
            {
                "model": model_name,
                "trimCount": len(grouped_items),
                "entryMsrp": min(msrps) if msrps else None,
                "maxMsrp": max(msrps) if msrps else None,
                "currency": grouped_items[0].get("currency"),
            }
        )

    return {
        "queryModels": query_models,
        "matchedModels": [item["model"] for item in model_summaries],
        "powertrain": normalized_powertrain or None,
        "items": items,
        "modelSummaries": model_summaries,
        "sourceSummary": [
            {"tier": tier, "count": count}
            for tier, count in sorted(source_counter.items())
        ],
        "latestUpdatedAt": latest_updated_at or None,
    }


def lookup_current_msrp_from_db(
    *,
    country: str,
    brand: str | None = None,
    model: str | None = None,
    models: list[str] | None = None,
    powertrain: str | None = None,
    limit_per_model: int = 4,
    max_items: int = 12,
) -> dict[str, Any]:
    query_models = _normalize_model_queries(model=model, models=models)
    empty_payload = {
        "queryModels": query_models,
        "matchedModels": [],
        "powertrain": _normalize_powertrain(powertrain) or None,
        "items": [],
        "modelSummaries": [],
        "sourceSummary": [],
        "latestUpdatedAt": None,
    }
    if not query_models:
        return empty_payload

    try:
        session_factory = get_session_factory()
    except Exception:  # noqa: BLE001
        return empty_payload

    try:
        with session_factory() as session:
            return lookup_current_msrp(
                session,
                country=country,
                brand=brand,
                model=model,
                models=models,
                powertrain=powertrain,
                limit_per_model=limit_per_model,
                max_items=max_items,
            )
    except Exception:  # noqa: BLE001
        return empty_payload
