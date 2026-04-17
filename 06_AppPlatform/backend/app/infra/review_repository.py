from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.orm import Session

from app.db.models import MatchOverride, ReviewCase, ReviewDecision
from app.services.country_service import country_filter_aliases


def _normalize_powertrain(value: str | None) -> str:
    return str(value or "").strip()


def list_match_overrides(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
) -> list[MatchOverride]:
    stmt: Select[tuple[MatchOverride]] = select(MatchOverride)
    if country:
        stmt = stmt.where(
            func.lower(MatchOverride.country).in_(
                country_filter_aliases(country)
            )
        )
    if brand:
        stmt = stmt.where(MatchOverride.brand == brand)
    if jato_model:
        stmt = stmt.where(MatchOverride.jato_model == jato_model)
    stmt = stmt.order_by(
        MatchOverride.updated_at_utc.desc(),
        MatchOverride.valid_from_date.desc(),
    ).limit(max(1, min(int(limit), 200)))
    return session.execute(stmt).scalars().all()


def get_match_override(
    session: Session,
    override_id: object,
) -> MatchOverride | None:
    return session.get(MatchOverride, override_id)


def add_match_override(
    session: Session,
    override: MatchOverride,
) -> MatchOverride:
    session.add(override)
    return override


def find_applicable_override(
    session: Session,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str | None,
    observation_date: object,
) -> MatchOverride | None:
    """Find the most recent active override for a business key + date.

    Used during batch ingest to auto-apply previously-approved mappings
    so that repeated observations skip manual review.
    """
    normalized_powertrain = _normalize_powertrain(jato_powertrain)
    stmt: Select[tuple[MatchOverride]] = select(MatchOverride).where(
        MatchOverride.country == country,
        MatchOverride.brand == brand,
        MatchOverride.jato_model == jato_model,
        MatchOverride.jato_trim == jato_trim,
        MatchOverride.valid_from_date <= observation_date,
        or_(
            MatchOverride.valid_to_date.is_(None),
            MatchOverride.valid_to_date >= observation_date,
        ),
    )
    if normalized_powertrain:
        stmt = stmt.where(
            MatchOverride.jato_powertrain.in_([normalized_powertrain, ""])
        ).order_by(
            case(
                (MatchOverride.jato_powertrain == normalized_powertrain, 0),
                else_=1,
            ),
            MatchOverride.valid_from_date.desc(),
        )
    else:
        stmt = stmt.where(MatchOverride.jato_powertrain == "").order_by(
            MatchOverride.valid_from_date.desc()
        )
    stmt = stmt.limit(1)
    return session.execute(stmt).scalar_one_or_none()


def list_active_match_overrides_by_key(
    session: Session,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str | None,
    as_of_date: object,
) -> list[MatchOverride]:
    normalized_powertrain = _normalize_powertrain(jato_powertrain)
    stmt: Select[tuple[MatchOverride]] = select(MatchOverride).where(
        MatchOverride.country == country,
        MatchOverride.brand == brand,
        MatchOverride.jato_model == jato_model,
        MatchOverride.jato_trim == jato_trim,
        MatchOverride.valid_from_date <= as_of_date,
        or_(
            MatchOverride.valid_to_date.is_(None),
            MatchOverride.valid_to_date >= as_of_date,
        ),
    )
    if normalized_powertrain:
        stmt = stmt.where(
            MatchOverride.jato_powertrain.in_([normalized_powertrain, ""])
        ).order_by(
            case(
                (MatchOverride.jato_powertrain == normalized_powertrain, 0),
                else_=1,
            ),
            MatchOverride.valid_from_date.desc(),
        )
    else:
        stmt = stmt.where(MatchOverride.jato_powertrain == "").order_by(
            MatchOverride.valid_from_date.desc()
        )
    return session.execute(stmt).scalars().all()


def delete_match_override(
    session: Session,
    override: MatchOverride,
) -> None:
    session.delete(override)


def _apply_review_case_filters(
    stmt: Select,
    review_status: str | None,
    country: str | None,
    brand: str | None,
    current_assignee: str | None,
    model: str | None = None,
) -> Select:
    if review_status:
        stmt = stmt.where(ReviewCase.review_status == review_status)
    if country:
        stmt = stmt.where(
            func.lower(ReviewCase.country).in_(
                country_filter_aliases(country)
            )
        )
    if brand:
        stmt = stmt.where(
            or_(
                func.lower(ReviewCase.brand).contains(brand.strip().lower()),
                func.lower(ReviewCase.brand) == brand.strip().lower(),
            )
        )
    if model:
        pattern = f"%{model.strip().lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(ReviewCase.jato_model).like(pattern),
                func.lower(ReviewCase.official_model).like(pattern),
            )
        )
    if current_assignee:
        stmt = stmt.where(ReviewCase.current_assignee == current_assignee)
    return stmt


def count_review_cases(
    session: Session,
    review_status: str | None,
    country: str | None,
    brand: str | None,
    current_assignee: str | None,
    model: str | None = None,
) -> int:
    stmt = select(func.count()).select_from(ReviewCase)
    stmt = _apply_review_case_filters(
        stmt,
        review_status,
        country,
        brand,
        current_assignee,
        model,
    )
    return int(session.execute(stmt).scalar_one())


def count_distinct_countries(session: Session) -> int:
    stmt = select(func.count(func.distinct(ReviewCase.country))).select_from(
        ReviewCase
    )
    return int(session.execute(stmt).scalar_one())


def list_review_cases(
    session: Session,
    review_status: str | None,
    country: str | None,
    brand: str | None,
    current_assignee: str | None,
    limit: int,
    offset: int,
    model: str | None = None,
) -> list[ReviewCase]:
    stmt: Select[tuple[ReviewCase]] = select(ReviewCase)
    stmt = _apply_review_case_filters(
        stmt,
        review_status,
        country,
        brand,
        current_assignee,
        model,
    )
    stmt = stmt.order_by(
        ReviewCase.updated_at_utc.desc(),
        ReviewCase.created_at_utc.desc(),
    ).offset(max(0, int(offset))).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()


def get_review_case(
    session: Session,
    review_case_id: object,
) -> ReviewCase | None:
    return session.get(ReviewCase, review_case_id)


def get_review_case_by_observation(
    session: Session,
    observation_id: object,
) -> ReviewCase | None:
    stmt: Select[tuple[ReviewCase]] = select(ReviewCase).where(
        ReviewCase.observation_id == observation_id
    )
    return session.execute(stmt).scalar_one_or_none()


def add_review_case(
    session: Session,
    review_case: ReviewCase,
) -> ReviewCase:
    session.add(review_case)
    return review_case


def list_review_decisions(
    session: Session,
    review_case_id: object,
) -> list[ReviewDecision]:
    stmt: Select[tuple[ReviewDecision]] = select(ReviewDecision).where(
        ReviewDecision.review_case_id == review_case_id
    )
    stmt = stmt.order_by(ReviewDecision.decided_at_utc.desc())
    return session.execute(stmt).scalars().all()


def add_review_decision(
    session: Session,
    decision: ReviewDecision,
) -> ReviewDecision:
    session.add(decision)
    return decision
