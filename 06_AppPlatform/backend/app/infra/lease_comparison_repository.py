"""Repository for Lease Comparison — lease_offers, versions, compare sets."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import LeaseCompareSet, LeaseOffer, LeaseOfferVersion


def add_offer(session: Session, offer: LeaseOffer) -> LeaseOffer:
    session.add(offer)
    session.flush()
    return offer


def get_offer_by_id(session: Session, offer_id: str) -> LeaseOffer | None:
    return session.get(LeaseOffer, UUID(offer_id))


def list_offers(
    session: Session,
    country: str | None = None,
    brand: str | None = None,
    model_name: str | None = None,
    lease_type: str | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[LeaseOffer]:
    stmt = select(LeaseOffer).order_by(LeaseOffer.created_at_utc.desc())
    if country:
        stmt = stmt.where(LeaseOffer.country_code == country.upper())
    if brand:
        stmt = stmt.where(LeaseOffer.brand.ilike(f"%{brand}%"))
    if model_name:
        stmt = stmt.where(LeaseOffer.model_name.ilike(f"%{model_name}%"))
    if lease_type:
        stmt = stmt.where(LeaseOffer.lease_type == lease_type)
    if status:
        stmt = stmt.where(LeaseOffer.status == status)
    return list(session.execute(stmt.limit(limit)).scalars().all())


def update_offer(session: Session, offer: LeaseOffer) -> LeaseOffer:
    session.flush()
    return offer


def delete_offer(session: Session, offer: LeaseOffer) -> None:
    session.delete(offer)
    session.flush()


def add_version(session: Session, version: LeaseOfferVersion) -> LeaseOfferVersion:
    session.add(version)
    session.flush()
    return version


def list_versions(session: Session, offer_id: str) -> list[LeaseOfferVersion]:
    return list(session.execute(
        select(LeaseOfferVersion)
        .where(LeaseOfferVersion.offer_id == UUID(offer_id))
        .order_by(LeaseOfferVersion.version_no.desc())
    ).scalars().all())


def max_version_no(session: Session, offer_id: str) -> int | None:
    return session.execute(
        select(func.max(LeaseOfferVersion.version_no))
        .where(LeaseOfferVersion.offer_id == UUID(offer_id))
    ).scalar()


def add_compare_set(session: Session, cs: LeaseCompareSet) -> LeaseCompareSet:
    session.add(cs)
    session.flush()
    return cs


def list_compare_sets(session: Session, country: str | None = None) -> list[LeaseCompareSet]:
    stmt = select(LeaseCompareSet).order_by(LeaseCompareSet.created_at_utc.desc())
    if country:
        stmt = stmt.where(LeaseCompareSet.country_code == country.upper())
    return list(session.execute(stmt.limit(50)).scalars().all())
