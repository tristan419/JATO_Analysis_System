from datetime import datetime, timezone
from uuid import uuid4
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import (
    CurrentPrice,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ReviewCase,
    ReviewDecision,
)
from app.infra import msrp_repository as repo
from app.infra import review_repository
from app.services.fx_service import convert_amount_to_eur
from app.services.msrp_workflow_service import (
    ELIGIBLE_CURRENT_PRICE_STATUSES,
    REVIEW_REQUIRED_STATUS,
    _ensure_review_case,
    create_scrape_batch_ingest,
    materialize_current_price_from_observation,
)
from app.services.msrp_link_service import upsert_jato_msrp_link
from app.services.payload_serializers import (
    jato_msrp_link_payload,
    observation_payload,
    scrape_batch_payload,
    source_payload,
)


def _commit_or_conflict(session: Session, detail: str) -> None:
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=detail) from exc


def list_msrp_sources(
    session: Session,
    source_code: str | None,
    country: str | None,
    brand: str | None,
    source_type: str | None,
    enabled: bool | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_sources(
        session,
        source_code,
        country,
        brand,
        source_type,
        enabled,
        limit,
    )
    return {
        "rows": len(items),
        "items": [source_payload(item) for item in items],
    }


def create_msrp_source(session: Session, data: dict) -> dict[str, object]:
    data.setdefault("tier", 3)
    source = MsrpSource(**data)
    repo.add_source(session, source)
    _commit_or_conflict(session, "Source code already exists")
    session.refresh(source)
    return source_payload(source)


def update_msrp_source(
    session: Session,
    source_id: str,
    data: dict,
) -> dict[str, object] | None:
    source = repo.get_source(session, UUID(source_id))
    if source is None:
        return None
    for key, value in data.items():
        if value is not None:
            setattr(source, key, value)
    source.updated_at_utc = datetime.now(timezone.utc)
    _commit_or_conflict(session, "Source code already exists")
    session.refresh(source)
    return source_payload(source)


def deactivate_msrp_source(
    session: Session,
    source_id: str,
) -> dict[str, object] | None:
    source = repo.get_source(session, UUID(source_id))
    if source is None:
        return None
    source.enabled = False
    source.updated_at_utc = datetime.now(timezone.utc)
    _commit_or_conflict(session, "Source code already exists")
    session.refresh(source)
    return source_payload(source)


def list_jato_msrp_links(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    official_model: str | None,
    is_active: bool | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_jato_msrp_links(
        session,
        country,
        brand,
        jato_model,
        official_model,
        is_active,
        limit,
    )
    return {
        "rows": len(items),
        "items": [jato_msrp_link_payload(item) for item in items],
    }


def create_jato_msrp_link(
    session: Session,
    data: dict[str, object],
) -> dict[str, object]:
    link = upsert_jato_msrp_link(
        session,
        country=_require_text(data.get("country"), "country"),
        brand=_require_text(data.get("brand"), "brand"),
        jato_model=_require_text(data.get("jato_model"), "jato_model"),
        jato_trim=_require_text(data.get("jato_trim"), "jato_trim"),
        jato_powertrain=_optional_text(data.get("jato_powertrain")),
        official_model=_require_text(
            data.get("official_model"),
            "official_model",
        ),
        official_trim=_require_text(
            data.get("official_trim"),
            "official_trim",
        ),
        official_edition=_optional_text(data.get("official_edition")),
        official_powertrain=_optional_text(data.get("official_powertrain")),
        confidence=int(data.get("confidence") or 80),
        link_source=_require_text(data.get("link_source"), "link_source"),
        notes=_optional_text(data.get("notes")),
    )
    link.is_active = bool(data.get("is_active", True))
    _commit_or_conflict(session, "Link already exists")
    session.refresh(link)
    return jato_msrp_link_payload(link)


def update_jato_msrp_link(
    session: Session,
    link_id: str,
    data: dict[str, object],
) -> dict[str, object] | None:
    link = repo.get_jato_msrp_link(session, UUID(link_id))
    if link is None:
        return None
    for key, value in data.items():
        if value is not None:
            setattr(link, key, value)
    link.updated_at_utc = datetime.now(timezone.utc)
    if link.is_active:
        sibling_links = repo.list_jato_msrp_links_for_key(
            session,
            link.country,
            link.brand,
            link.jato_model,
            link.jato_trim,
            link.jato_powertrain,
            is_active=None,
        )
        for sibling in sibling_links:
            if sibling.link_id != link.link_id:
                sibling.is_active = False
                sibling.updated_at_utc = link.updated_at_utc
    _commit_or_conflict(session, "Link update conflicted with existing data")
    session.refresh(link)
    return jato_msrp_link_payload(link)


def deactivate_jato_msrp_link(
    session: Session,
    link_id: str,
) -> dict[str, object] | None:
    link = repo.get_jato_msrp_link(session, UUID(link_id))
    if link is None:
        return None
    link.is_active = False
    link.updated_at_utc = datetime.now(timezone.utc)
    _commit_or_conflict(session, "Link update conflicted with existing data")
    session.refresh(link)
    return jato_msrp_link_payload(link)


def list_scrape_batches(
    session: Session,
    scope_country: str | None,
    status: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_scrape_batches(session, scope_country, status, limit)
    return {
        "rows": len(items),
        "items": [scrape_batch_payload(item) for item in items],
    }


def list_observations(
    session: Session,
    scrape_batch_id: UUID | None,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    match_status: str | None,
    source_code: str | None,
    source_type: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_observations(
        session,
        scrape_batch_id,
        country,
        brand,
        jato_model,
        match_status,
        source_code,
        source_type,
        limit,
    )
    sources = repo.list_sources_by_ids(
        session,
        [item.source_id for item in items],
    )
    source_by_id = {item.source_id: item for item in sources}
    return {
        "rows": len(items),
        "items": [
            observation_payload(item, source_by_id.get(item.source_id))
            for item in items
        ],
    }


def _require_text(value: object | None, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} is required",
        )
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_observation_mutation(
    session: Session,
    data: dict[str, object],
) -> tuple[dict[str, object], MsrpSource]:
    source_id = data.get("source_id")
    if not source_id:
        raise HTTPException(status_code=400, detail="source_id is required")
    source = repo.get_source(session, UUID(str(source_id)))
    if source is None:
        raise HTTPException(status_code=400, detail="MSRP source not found")

    observed_at_utc = data.get("observed_at_utc")
    if not isinstance(observed_at_utc, datetime):
        raise HTTPException(
            status_code=400,
            detail="observed_at_utc is required",
        )

    source_price = float(data["msrp_value"])
    source_currency = _require_text(data.get("currency"), "currency").upper()
    msrp_value_eur, fx_quote = convert_amount_to_eur(
        source_price,
        source_currency,
        observed_at_utc,
    )
    normalized = {
        "source": source,
        "country": _require_text(data.get("country"), "country"),
        "brand": _require_text(data.get("brand"), "brand"),
        "jato_model": _require_text(data.get("jato_model"), "jato_model"),
        "jato_trim": _require_text(data.get("jato_trim"), "jato_trim"),
        "jato_powertrain": _optional_text(data.get("jato_powertrain")),
        "official_model": _require_text(
            data.get("official_model"), "official_model"
        ),
        "official_trim": _require_text(
            data.get("official_trim"), "official_trim"
        ),
        "official_edition": _optional_text(data.get("official_edition")),
        "official_powertrain": _optional_text(
            data.get("official_powertrain")
        ),
        "msrp_value": msrp_value_eur,
        "currency": "EUR",
        "source_msrp_value": source_price,
        "source_currency": source_currency,
        "fx_rate_to_eur": fx_quote.rate_to_eur,
        "fx_rate_as_of_date": fx_quote.as_of_date,
        "fx_source": fx_quote.source,
        "tax_included": bool(data.get("tax_included")),
        "price_label": _require_text(data.get("price_label"), "price_label"),
        "availability_text": _optional_text(data.get("availability_text")),
        "observed_at_utc": observed_at_utc,
        "source_url": _require_text(data.get("source_url"), "source_url"),
        "source_snapshot_path": _optional_text(
            data.get("source_snapshot_path")
        ),
        "source_payload_hash": _optional_text(
            data.get("source_payload_hash")
        ),
        "extraction_version": _require_text(
            data.get("extraction_version"), "extraction_version"
        ),
        "match_confidence": float(data.get("match_confidence") or 0.0),
        "match_status": _require_text(
            data.get("match_status"),
            "match_status",
        ),
        "match_reason_json": data.get("match_reason_json"),
        "source_context_json": data.get("source_context_json"),
        "candidate_matches_json": data.get("candidate_matches_json"),
    }
    return normalized, source


def _serialize_observation_row(
    session: Session,
    observation: MsrpObservation,
) -> dict[str, object]:
    source = repo.get_source(session, observation.source_id)
    return observation_payload(observation, source)


def create_observation(
    session: Session,
    data: dict[str, object],
) -> dict[str, object]:
    normalized, _ = _normalize_observation_mutation(session, data)
    batch_code = (
        f"manual-observation-{uuid4().hex[:12]}"
    )
    result = create_scrape_batch_ingest(
        session,
        {
            "batch_code": batch_code,
            "trigger_type": "manual_observation_crud",
            "scope_country": normalized["country"],
            "scope_brands": [normalized["brand"]],
            "failed_count": 0,
            "notes": "Created from MSRP observation CRUD",
            "started_at_utc": normalized["observed_at_utc"],
            "finished_at_utc": normalized["observed_at_utc"],
            "observations": [
                {
                    "source_id": str(data["source_id"]),
                    "country": normalized["country"],
                    "brand": normalized["brand"],
                    "jato_model": normalized["jato_model"],
                    "jato_trim": normalized["jato_trim"],
                    "jato_powertrain": normalized["jato_powertrain"],
                    "official_model": normalized["official_model"],
                    "official_trim": normalized["official_trim"],
                    "official_edition": normalized["official_edition"],
                    "official_powertrain": normalized["official_powertrain"],
                    "msrp_value": normalized["source_msrp_value"],
                    "currency": normalized["source_currency"],
                    "tax_included": normalized["tax_included"],
                    "price_label": normalized["price_label"],
                    "availability_text": normalized["availability_text"],
                    "observed_at_utc": normalized["observed_at_utc"],
                    "source_url": normalized["source_url"],
                    "source_snapshot_path": normalized["source_snapshot_path"],
                    "source_payload_hash": normalized["source_payload_hash"],
                    "extraction_version": normalized["extraction_version"],
                    "match_confidence": normalized["match_confidence"],
                    "match_status": normalized["match_status"],
                    "match_reason_json": normalized["match_reason_json"],
                    "source_context_json": normalized["source_context_json"],
                    "candidate_matches_json": normalized[
                        "candidate_matches_json"
                    ],
                }
            ],
        },
    )
    sample_observations = list(result.get("sampleObservations") or [])
    if not sample_observations:
        raise HTTPException(
            status_code=500,
            detail="Observation create did not return a sample observation",
        )
    observation_id = sample_observations[0].get("observationId")
    observation = repo.get_observation(session, UUID(str(observation_id)))
    if observation is None:
        raise HTTPException(status_code=404, detail="Observation not found")
    return _serialize_observation_row(session, observation)


def _assert_observation_mutable(
    session: Session,
    observation: MsrpObservation,
) -> ReviewCase | None:
    current_price = session.execute(
        select(CurrentPrice).where(
            CurrentPrice.effective_observation_id == observation.observation_id
        )
    ).scalar_one_or_none()
    if current_price is not None:
        raise HTTPException(
            status_code=409,
            detail="Observation is already materialized into current price",
        )

    price_history_count = int(
        session.execute(
            select(func.count())
            .select_from(PriceHistory)
            .where(
                or_(
                    PriceHistory.started_by_observation_id
                    == observation.observation_id,
                    PriceHistory.ended_by_observation_id
                    == observation.observation_id,
                    PriceHistory.last_confirmed_by_observation_id
                    == observation.observation_id,
                )
            )
        ).scalar_one()
    )
    if price_history_count > 0:
        raise HTTPException(
            status_code=409,
            detail="Observation already participates in price history",
        )

    decision_count = int(
        session.execute(
            select(func.count())
            .select_from(ReviewDecision)
            .where(ReviewDecision.observation_id == observation.observation_id)
        ).scalar_one()
    )
    if decision_count > 0:
        raise HTTPException(
            status_code=409,
            detail="Observation already has review decisions",
        )

    return review_repository.get_review_case_by_observation(
        session,
        observation.observation_id,
    )


def update_observation(
    session: Session,
    observation_id: str,
    data: dict[str, object],
) -> dict[str, object] | None:
    observation = repo.get_observation(session, UUID(observation_id))
    if observation is None:
        return None

    review_case = _assert_observation_mutable(session, observation)
    merged_data: dict[str, object] = {
        "source_id": str(observation.source_id),
        "country": observation.country,
        "brand": observation.brand,
        "jato_model": observation.jato_model,
        "jato_trim": observation.jato_trim,
        "jato_powertrain": observation.jato_powertrain,
        "official_model": observation.official_model,
        "official_trim": observation.official_trim,
        "official_edition": observation.official_edition,
        "official_powertrain": observation.official_powertrain,
        "msrp_value": float(observation.source_msrp_value),
        "currency": observation.source_currency,
        "tax_included": observation.tax_included,
        "price_label": observation.price_label,
        "availability_text": observation.availability_text,
        "observed_at_utc": observation.observed_at_utc,
        "source_url": observation.source_url,
        "source_snapshot_path": observation.source_snapshot_path,
        "source_payload_hash": observation.source_payload_hash,
        "extraction_version": observation.extraction_version,
        "match_confidence": float(observation.match_confidence),
        "match_status": observation.match_status,
        "match_reason_json": observation.match_reason_json,
        "source_context_json": observation.source_context_json,
        "candidate_matches_json": (
            review_case.candidate_matches_json if review_case else None
        ),
    }
    merged_data.update(data)
    normalized, source = _normalize_observation_mutation(session, merged_data)

    observation.source_id = source.source_id
    observation.country = str(normalized["country"])
    observation.brand = str(normalized["brand"])
    observation.jato_model = str(normalized["jato_model"])
    observation.jato_trim = str(normalized["jato_trim"])
    observation.jato_powertrain = normalized["jato_powertrain"]
    observation.official_model = str(normalized["official_model"])
    observation.official_trim = str(normalized["official_trim"])
    observation.official_edition = normalized["official_edition"]
    observation.official_powertrain = normalized["official_powertrain"]
    observation.msrp_value = float(normalized["msrp_value"])
    observation.currency = str(normalized["currency"])
    observation.source_msrp_value = float(normalized["source_msrp_value"])
    observation.source_currency = str(normalized["source_currency"])
    observation.fx_rate_to_eur = float(normalized["fx_rate_to_eur"])
    observation.fx_rate_as_of_date = normalized["fx_rate_as_of_date"]
    observation.fx_source = str(normalized["fx_source"])
    observation.tax_included = bool(normalized["tax_included"])
    observation.price_label = str(normalized["price_label"])
    observation.availability_text = normalized["availability_text"]
    observation.observed_at_utc = normalized["observed_at_utc"]
    observation.source_url = str(normalized["source_url"])
    observation.source_snapshot_path = normalized["source_snapshot_path"]
    observation.source_payload_hash = normalized["source_payload_hash"]
    observation.extraction_version = str(normalized["extraction_version"])
    observation.match_confidence = float(normalized["match_confidence"])
    observation.match_status = str(normalized["match_status"])
    observation.match_reason_json = normalized["match_reason_json"]
    observation.source_context_json = normalized["source_context_json"]
    observation.updated_at_utc = datetime.now(timezone.utc)

    candidate_matches_json = normalized["candidate_matches_json"]
    if observation.match_status == REVIEW_REQUIRED_STATUS:
        review_case = _ensure_review_case(
            session,
            observation,
            candidate_matches_json,
        )
    elif review_case is not None:
        review_case.country = observation.country
        review_case.brand = observation.brand
        review_case.jato_model = observation.jato_model
        review_case.jato_trim = observation.jato_trim
        review_case.jato_powertrain = observation.jato_powertrain
        review_case.official_model = observation.official_model
        review_case.official_trim = observation.official_trim
        review_case.official_edition = observation.official_edition
        review_case.official_powertrain = observation.official_powertrain
        review_case.match_confidence = observation.match_confidence
        review_case.source_url = observation.source_url
        review_case.source_snapshot_path = observation.source_snapshot_path
        review_case.candidate_matches_json = candidate_matches_json
        review_case.review_status = (
            "rejected"
            if observation.match_status == "rejected"
            else "approved"
            if observation.match_status in ELIGIBLE_CURRENT_PRICE_STATUSES
            else "open"
        )
        review_case.updated_at_utc = datetime.now(timezone.utc)

    if observation.match_status in ELIGIBLE_CURRENT_PRICE_STATUSES:
        materialize_current_price_from_observation(
            session,
            observation,
            price_history_enabled=repo.has_price_history_table(session),
        )

    _commit_or_conflict(session, "Observation update conflicted with data")
    session.refresh(observation)
    return _serialize_observation_row(session, observation)


def delete_observation(
    session: Session,
    observation_id: str,
) -> dict[str, object] | None:
    observation = repo.get_observation(session, UUID(observation_id))
    if observation is None:
        return None

    review_case = _assert_observation_mutable(session, observation)
    payload = _serialize_observation_row(session, observation)
    try:
        if review_case is not None:
            session.delete(review_case)
            session.flush()
        session.delete(observation)
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Observation is still referenced by review workflow",
        ) from exc
    return payload
