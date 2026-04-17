from datetime import datetime, timezone
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import (
    CurrentPrice,
    MatchOverride,
    ReviewDecision,
)
from app.infra import msrp_repository
from app.infra import review_repository as repo
from app.services.msrp_workflow_service import (
    materialize_current_price_from_observation,
)
from app.services.payload_serializers import (
    current_price_payload,
    observation_payload,
    override_payload,
    review_case_payload,
    review_decision_payload,
)


def _commit_or_conflict(session: Session, detail: str) -> None:
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=detail) from exc


def list_match_overrides(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_match_overrides(
        session,
        country,
        brand,
        jato_model,
        limit,
    )
    return {
        "rows": len(items),
        "items": [override_payload(item) for item in items],
    }


def create_match_override(session: Session, data: dict) -> dict[str, object]:
    payload = dict(data)
    payload["jato_powertrain"] = str(
        payload.get("jato_powertrain") or ""
    ).strip()
    override = MatchOverride(**payload)
    repo.add_match_override(session, override)
    _commit_or_conflict(session, "Override already exists")
    session.refresh(override)
    return override_payload(override)


def update_match_override(
    session: Session,
    override_id: str,
    data: dict,
) -> dict[str, object] | None:
    override = repo.get_match_override(session, UUID(override_id))
    if override is None:
        return None
    for key, value in data.items():
        if value is not None:
            if key == "jato_powertrain":
                value = str(value).strip()
            setattr(override, key, value)
    override.updated_at_utc = datetime.now(timezone.utc)
    _commit_or_conflict(session, "Override already exists")
    session.refresh(override)
    return override_payload(override)


def delete_match_override(
    session: Session,
    override_id: str,
) -> dict[str, object] | None:
    override = repo.get_match_override(session, UUID(override_id))
    if override is None:
        return None
    payload = override_payload(override)
    repo.delete_match_override(session, override)
    session.commit()
    return payload


def list_review_cases(
    session: Session,
    review_status: str | None,
    country: str | None,
    brand: str | None,
    current_assignee: str | None,
    limit: int,
    offset: int,
    model: str | None = None,
) -> dict[str, object]:
    total = repo.count_review_cases(
        session,
        review_status,
        country,
        brand,
        current_assignee,
        model,
    )
    items = repo.list_review_cases(
        session,
        review_status,
        country,
        brand,
        current_assignee,
        limit,
        offset,
        model,
    )
    if not items:
        return {
            "rows": 0,
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [],
        }
    observations = msrp_repository.list_observations_by_ids(
        session,
        [item.observation_id for item in items],
    )
    observation_by_id = {
        observation.observation_id: observation for observation in observations
    }
    sources = msrp_repository.list_sources_by_ids(
        session,
        [observation.source_id for observation in observations],
    )
    source_by_id = {source.source_id: source for source in sources}
    return {
        "rows": len(items),
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [
            review_case_payload(
                item,
                observation_by_id.get(item.observation_id),
                source_by_id.get(
                    observation_by_id[item.observation_id].source_id
                )
                if item.observation_id in observation_by_id
                else None,
            )
            for item in items
        ],
    }


def get_review_case_detail(
    session: Session,
    review_case_id: str,
) -> dict[str, object] | None:
    review_case = repo.get_review_case(session, UUID(review_case_id))
    if review_case is None:
        return None
    observation = msrp_repository.get_observation(
        session,
        review_case.observation_id,
    )
    source = (
        msrp_repository.get_source(session, observation.source_id)
        if observation is not None
        else None
    )
    decisions = repo.list_review_decisions(session, review_case.review_case_id)
    current_price = msrp_repository.get_current_price_by_key(
        session,
        review_case.country,
        review_case.brand,
        review_case.jato_model,
        review_case.jato_trim,
        review_case.jato_powertrain,
    )
    current_price_observation = (
        msrp_repository.get_observation(
            session,
            current_price.effective_observation_id,
        )
        if current_price is not None
        else None
    )
    current_price_source = (
        msrp_repository.get_source(session, current_price_observation.source_id)
        if current_price_observation is not None
        else None
    )
    return {
        "reviewCase": review_case_payload(review_case, observation, source),
        "observation": (
            observation_payload(observation)
            if observation is not None
            else None
        ),
        "decisions": [review_decision_payload(item) for item in decisions],
        "currentPrice": (
            current_price_payload(current_price, current_price_source)
            if current_price is not None
            else None
        ),
    }


def create_review_decision(
    session: Session,
    review_case_id: str,
    data: dict,
) -> dict[str, object]:
    review_case = repo.get_review_case(session, UUID(review_case_id))
    if review_case is None:
        raise HTTPException(status_code=404, detail="Review case not found")

    observation = msrp_repository.get_observation(
        session,
        review_case.observation_id,
    )
    if observation is None:
        raise HTTPException(status_code=404, detail="Observation not found")

    decision_name = str(data.get("decision") or "").strip()
    decided_by = str(data.get("decided_by") or "").strip()
    if not decision_name or not decided_by:
        raise HTTPException(
            status_code=400,
            detail="decision and decided_by are required",
        )

    decided_official_model = data.get("decided_official_model")
    decided_official_trim = data.get("decided_official_trim")
    if decision_name == "remap":
        if not decided_official_model or not decided_official_trim:
            raise HTTPException(
                status_code=400,
                detail=(
                    "remap requires decided_official_model and "
                    "decided_official_trim"
                ),
            )

    current_price: CurrentPrice | None = None
    override: MatchOverride | None = None
    if decision_name in {"approve", "remap"}:
        observation.official_model = (
            str(decided_official_model).strip()
            if decided_official_model
            else observation.official_model
        )
        observation.official_trim = (
            str(decided_official_trim).strip()
            if decided_official_trim
            else observation.official_trim
        )
        observation.match_status = "human_approved"
        review_case.review_status = "approved"
        review_case.official_model = observation.official_model
        review_case.official_trim = observation.official_trim
        review_case.official_edition = observation.official_edition
        review_case.official_powertrain = observation.official_powertrain
        review_case.jato_powertrain = observation.jato_powertrain
        current_price = materialize_current_price_from_observation(
            session,
            observation,
        )
    elif decision_name == "reject":
        observation.match_status = "rejected"
        review_case.review_status = "rejected"
    else:
        raise HTTPException(
            status_code=400,
            detail="Unsupported review decision",
        )

    review_case.current_assignee = decided_by
    review_case.updated_at_utc = datetime.now(timezone.utc)
    observation.updated_at_utc = datetime.now(timezone.utc)
    existing_reason = observation.match_reason_json or {}
    if not isinstance(existing_reason, dict):
        existing_reason = {"previous": existing_reason}
    existing_reason["reviewDecision"] = decision_name
    existing_reason["reviewNote"] = data.get("note")
    existing_reason["decidedBy"] = decided_by
    observation.match_reason_json = existing_reason

    review_decision = ReviewDecision(
        review_case_id=review_case.review_case_id,
        observation_id=observation.observation_id,
        decision=decision_name,
        decided_official_model=(
            observation.official_model
            if decision_name in {"approve", "remap"}
            else None
        ),
        decided_official_trim=(
            observation.official_trim
            if decision_name in {"approve", "remap"}
            else None
        ),
        note=data.get("note"),
        decided_by=decided_by,
    )
    repo.add_review_decision(session, review_decision)

    if bool(data.get("persist_override")):
        if decision_name not in {"approve", "remap"}:
            raise HTTPException(
                status_code=400,
                detail="persist_override only applies to approve/remap",
            )
        override = MatchOverride(
            country=observation.country,
            brand=observation.brand,
            jato_model=observation.jato_model,
            jato_trim=observation.jato_trim,
            jato_powertrain=str(observation.jato_powertrain or "").strip(),
            official_model=observation.official_model,
            official_trim=observation.official_trim,
            valid_from_date=(
                data.get("valid_from_date")
                or observation.observed_at_utc.date()
            ),
            valid_to_date=None,
            override_reason=(
                data.get("override_reason")
                or data.get("note")
                or "Review decision override"
            ),
            created_by=decided_by,
        )
        repo.add_match_override(session, override)

    _commit_or_conflict(
        session,
        "Review decision conflicted with existing data",
    )
    session.refresh(review_case)
    session.refresh(review_decision)
    if current_price is not None:
        session.refresh(current_price)
    if override is not None:
        session.refresh(override)

    source = msrp_repository.get_source(session, observation.source_id)
    current_price_source = (
        msrp_repository.get_source(session, observation.source_id)
        if current_price is not None
        else None
    )

    return {
        "reviewCase": review_case_payload(review_case, observation, source),
        "decision": review_decision_payload(review_decision),
        "observation": observation_payload(observation),
        "currentPrice": (
            current_price_payload(current_price, current_price_source)
            if current_price is not None
            else None
        ),
        "override": (
            override_payload(override) if override is not None else None
        ),
    }
