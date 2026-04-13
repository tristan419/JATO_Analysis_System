from sqlalchemy import (
    Select,
    and_,
    distinct,
    func,
    inspect,
    or_,
    select,
    tuple_,
)
from sqlalchemy.orm import Session

from app.db.models import (
    CurrentPrice,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ScrapeBatch,
)
from app.services.country_service import country_filter_aliases


def list_sources(
    session: Session,
    source_code: str | None,
    country: str | None,
    brand: str | None,
    enabled: bool | None,
    limit: int,
) -> list[MsrpSource]:
    stmt: Select[tuple[MsrpSource]] = select(MsrpSource)
    if source_code:
        stmt = stmt.where(MsrpSource.source_code == source_code)
    if country:
        stmt = stmt.where(
            func.lower(MsrpSource.country).in_(country_filter_aliases(country))
        )
    if brand:
        stmt = stmt.where(MsrpSource.brand == brand)
    if enabled is not None:
        stmt = stmt.where(MsrpSource.enabled == enabled)
    stmt = stmt.order_by(
        MsrpSource.updated_at_utc.desc(),
        MsrpSource.source_code.asc(),
    ).limit(max(1, min(int(limit), 200)))
    return session.execute(stmt).scalars().all()


def get_source(session: Session, source_id: object) -> MsrpSource | None:
    return session.get(MsrpSource, source_id)


def list_sources_by_ids(
    session: Session,
    source_ids: list[object],
) -> list[MsrpSource]:
    if not source_ids:
        return []
    stmt: Select[tuple[MsrpSource]] = select(MsrpSource).where(
        MsrpSource.source_id.in_(source_ids)
    )
    return session.execute(stmt).scalars().all()


def add_source(session: Session, source: MsrpSource) -> MsrpSource:
    session.add(source)
    return source


def get_scrape_batch(
    session: Session,
    scrape_batch_id: object,
) -> ScrapeBatch | None:
    return session.get(ScrapeBatch, scrape_batch_id)


def add_scrape_batch(
    session: Session,
    batch: ScrapeBatch,
) -> ScrapeBatch:
    session.add(batch)
    return batch


def add_observations(
    session: Session,
    observations: list[MsrpObservation],
) -> list[MsrpObservation]:
    session.add_all(observations)
    return observations


def get_observation(
    session: Session,
    observation_id: object,
) -> MsrpObservation | None:
    return session.get(MsrpObservation, observation_id)


def list_observations_by_ids(
    session: Session,
    observation_ids: list[object],
) -> list[MsrpObservation]:
    if not observation_ids:
        return []
    stmt: Select[tuple[MsrpObservation]] = select(MsrpObservation).where(
        MsrpObservation.observation_id.in_(observation_ids)
    )
    return session.execute(stmt).scalars().all()


def list_scrape_batches(
    session: Session,
    scope_country: str | None,
    status: str | None,
    limit: int,
) -> list[ScrapeBatch]:
    stmt: Select[tuple[ScrapeBatch]] = select(ScrapeBatch)
    if scope_country:
        stmt = stmt.where(ScrapeBatch.scope_country == scope_country)
    if status:
        stmt = stmt.where(ScrapeBatch.status == status)
    stmt = stmt.order_by(
        ScrapeBatch.started_at_utc.desc()
    ).limit(max(1, min(int(limit), 200)))
    return session.execute(stmt).scalars().all()


def list_observations(
    session: Session,
    scrape_batch_id: object | None,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    match_status: str | None,
    limit: int,
) -> list[MsrpObservation]:
    stmt: Select[tuple[MsrpObservation]] = select(MsrpObservation)
    if scrape_batch_id is not None:
        stmt = stmt.where(MsrpObservation.scrape_batch_id == scrape_batch_id)
    if country:
        stmt = stmt.where(
            func.lower(MsrpObservation.country).in_(
                country_filter_aliases(country)
            )
        )
    if brand:
        stmt = stmt.where(
            func.lower(MsrpObservation.brand).contains(brand.strip().lower())
        )
    if jato_model:
        pattern = f"%{jato_model.strip().lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(MsrpObservation.jato_model).like(pattern),
                func.lower(MsrpObservation.official_model).like(pattern),
            )
        )
    if match_status:
        stmt = stmt.where(MsrpObservation.match_status == match_status)
    stmt = stmt.order_by(
        MsrpObservation.observed_at_utc.desc(),
        MsrpObservation.created_at_utc.desc(),
    ).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()


def list_materializable_observations(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
) -> list[MsrpObservation]:
    stmt: Select[tuple[MsrpObservation]] = select(MsrpObservation).where(
        MsrpObservation.match_status.in_(["auto_accepted", "human_approved"])
    )
    if country:
        stmt = stmt.where(
            func.lower(MsrpObservation.country).in_(
                country_filter_aliases(country)
            )
        )
    if brand:
        stmt = stmt.where(
            func.lower(MsrpObservation.brand).contains(brand.strip().lower())
        )
    if jato_model:
        pattern = f"%{jato_model.strip().lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(MsrpObservation.jato_model).like(pattern),
                func.lower(MsrpObservation.official_model).like(pattern),
            )
        )
    stmt = stmt.order_by(
        MsrpObservation.observed_at_utc.desc(),
        MsrpObservation.created_at_utc.desc(),
    ).limit(max(1, min(int(limit), 2000)))
    return session.execute(stmt).scalars().all()


def get_current_price_by_key(
    session: Session,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
) -> CurrentPrice | None:
    stmt: Select[tuple[CurrentPrice]] = select(CurrentPrice).where(
        CurrentPrice.country == country,
        CurrentPrice.brand == brand,
        CurrentPrice.jato_model == jato_model,
        CurrentPrice.jato_trim == jato_trim,
    )
    return session.execute(stmt).scalar_one_or_none()


def get_current_price(
    session: Session,
    current_price_id: object,
) -> CurrentPrice | None:
    return session.get(CurrentPrice, current_price_id)


def add_current_price(
    session: Session,
    current_price: CurrentPrice,
) -> CurrentPrice:
    session.add(current_price)
    return current_price


def delete_current_price(
    session: Session,
    current_price: CurrentPrice,
) -> None:
    session.delete(current_price)


def _apply_current_price_filters(
    stmt: Select,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
) -> Select:
    if country:
        stmt = stmt.where(
            func.lower(CurrentPrice.country).in_(
                country_filter_aliases(country)
            )
        )
    if brand:
        stmt = stmt.where(
            func.lower(CurrentPrice.brand).contains(brand.strip().lower())
        )
    if jato_model:
        pattern = f"%{jato_model.strip().lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(CurrentPrice.jato_model).like(pattern),
                func.lower(CurrentPrice.official_model).like(pattern),
            )
        )
    return stmt


def count_current_prices(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
) -> int:
    stmt = select(func.count()).select_from(CurrentPrice)
    stmt = _apply_current_price_filters(stmt, country, brand, jato_model)
    return int(session.execute(stmt).scalar_one())


def list_current_prices(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    offset: int,
) -> list[CurrentPrice]:
    stmt: Select[tuple[CurrentPrice]] = select(CurrentPrice)
    stmt = _apply_current_price_filters(stmt, country, brand, jato_model)
    stmt = stmt.order_by(
        CurrentPrice.updated_at_utc.desc(),
        CurrentPrice.country.asc(),
        CurrentPrice.brand.asc(),
        CurrentPrice.jato_model.asc(),
    ).offset(max(0, int(offset))).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()


def count_current_price_alerts(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
) -> int:
    if not has_price_history_table(session):
        return 0

    filtered_current_prices = _apply_current_price_filters(
        select(
            CurrentPrice.country.label("country"),
            CurrentPrice.brand.label("brand"),
            CurrentPrice.jato_model.label("jato_model"),
            CurrentPrice.jato_trim.label("jato_trim"),
        ),
        country,
        brand,
        jato_model,
    ).subquery("filtered_current_prices")

    alert_keys = (
        select(
            PriceHistory.country.label("country"),
            PriceHistory.brand.label("brand"),
            PriceHistory.jato_model.label("jato_model"),
            PriceHistory.jato_trim.label("jato_trim"),
        )
        .group_by(
            PriceHistory.country,
            PriceHistory.brand,
            PriceHistory.jato_model,
            PriceHistory.jato_trim,
        )
        .having(
            func.count(
                distinct(
                    tuple_(
                        PriceHistory.source_msrp_value,
                        PriceHistory.source_currency,
                    )
                )
            )
            > 1
        )
        .subquery("alert_keys")
    )

    stmt = select(func.count()).select_from(
        filtered_current_prices.join(
            alert_keys,
            and_(
                filtered_current_prices.c.country == alert_keys.c.country,
                filtered_current_prices.c.brand == alert_keys.c.brand,
                filtered_current_prices.c.jato_model
                == alert_keys.c.jato_model,
                filtered_current_prices.c.jato_trim == alert_keys.c.jato_trim,
            ),
        )
    )
    return int(session.execute(stmt).scalar_one())


def has_price_history_table(session: Session) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    return inspect(bind).has_table("price_history", schema="msrp")


# -- Price history helpers --------------------------------------------------


def get_open_price_period(
    session: Session,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
) -> PriceHistory | None:
    """Return the currently open price period (valid_to_utc IS NULL)."""
    stmt: Select[tuple[PriceHistory]] = select(PriceHistory).where(
        PriceHistory.country == country,
        PriceHistory.brand == brand,
        PriceHistory.jato_model == jato_model,
        PriceHistory.jato_trim == jato_trim,
        PriceHistory.valid_to_utc.is_(None),
    )
    return session.execute(stmt).scalar_one_or_none()


def add_price_history(
    session: Session,
    price_history: PriceHistory,
) -> PriceHistory:
    session.add(price_history)
    return price_history


def list_price_history(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    jato_trim: str | None,
    limit: int,
) -> list[PriceHistory]:
    stmt: Select[tuple[PriceHistory]] = select(PriceHistory)
    if country:
        stmt = stmt.where(
            func.lower(PriceHistory.country).in_(
                country_filter_aliases(country)
            )
        )
    if brand:
        stmt = stmt.where(PriceHistory.brand == brand)
    if jato_model:
        stmt = stmt.where(PriceHistory.jato_model == jato_model)
    if jato_trim:
        stmt = stmt.where(PriceHistory.jato_trim == jato_trim)
    stmt = stmt.order_by(
        PriceHistory.country.asc(),
        PriceHistory.brand.asc(),
        PriceHistory.jato_model.asc(),
        PriceHistory.jato_trim.asc(),
        PriceHistory.valid_from_utc.desc(),
    ).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()
