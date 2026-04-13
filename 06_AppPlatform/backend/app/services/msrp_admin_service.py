from datetime import datetime, timezone
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import MsrpSource
from app.infra import msrp_repository as repo
from app.services.payload_serializers import (
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
    enabled: bool | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_sources(
        session,
        source_code,
        country,
        brand,
        enabled,
        limit,
    )
    return {
        "rows": len(items),
        "items": [source_payload(item) for item in items],
    }


def create_msrp_source(session: Session, data: dict) -> dict[str, object]:
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
    limit: int,
) -> dict[str, object]:
    items = repo.list_observations(
        session,
        scrape_batch_id,
        country,
        brand,
        jato_model,
        match_status,
        limit,
    )
    return {
        "rows": len(items),
        "items": [observation_payload(item) for item in items],
    }
