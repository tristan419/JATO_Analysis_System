from datetime import datetime

from sqlalchemy import (
    Select,
    and_,
    case,
    func,
    inspect,
    or_,
    select,
)
from sqlalchemy.orm import Session

from app.db.models import (
    CurrentPrice,
    FinanceObservation,
    JatoMsrpLink,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ScrapeBatch,
)
from app.services.country_service import country_filter_aliases
from app.services.msrp_official_source_policy import (
    enabled_official_msrp_source_predicate,
)


def _normalize_powertrain(value: str | None) -> str:
    return str(value or "").strip()


def list_sources(
    session: Session,
    source_code: str | None,
    country: str | None,
    brand: str | None,
    source_type: str | None,
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
    if source_type:
        stmt = stmt.where(MsrpSource.source_type == source_type)
    if enabled is not None:
        stmt = stmt.where(MsrpSource.enabled == enabled)
    stmt = stmt.order_by(
        MsrpSource.tier.asc(),
        MsrpSource.updated_at_utc.desc(),
        MsrpSource.source_code.asc(),
    ).limit(max(1, min(int(limit), 200)))
    return session.execute(stmt).scalars().all()


def get_source(session: Session, source_id: object) -> MsrpSource | None:
    return session.get(MsrpSource, source_id)


def get_source_by_code(
    session: Session,
    source_code: str,
) -> MsrpSource | None:
    stmt: Select[tuple[MsrpSource]] = select(MsrpSource).where(
        MsrpSource.source_code == source_code
    )
    return session.execute(stmt).scalar_one_or_none()


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


def list_jato_msrp_links(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    official_model: str | None,
    is_active: bool | None,
    limit: int,
) -> list[JatoMsrpLink]:
    stmt: Select[tuple[JatoMsrpLink]] = select(JatoMsrpLink)
    if country:
        stmt = stmt.where(
            func.lower(JatoMsrpLink.country).in_(country_filter_aliases(country))
        )
    if brand:
        stmt = stmt.where(func.lower(JatoMsrpLink.brand).contains(brand.strip().lower()))
    if jato_model:
        stmt = stmt.where(
            func.lower(JatoMsrpLink.jato_model).contains(jato_model.strip().lower())
        )
    if official_model:
        stmt = stmt.where(
            func.lower(JatoMsrpLink.official_model).contains(
                official_model.strip().lower()
            )
        )
    if is_active is not None:
        stmt = stmt.where(JatoMsrpLink.is_active == is_active)
    stmt = stmt.order_by(
        JatoMsrpLink.is_active.desc(),
        JatoMsrpLink.confidence.desc(),
        JatoMsrpLink.updated_at_utc.desc(),
    ).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()


def get_jato_msrp_link(
    session: Session,
    link_id: object,
) -> JatoMsrpLink | None:
    return session.get(JatoMsrpLink, link_id)


def add_jato_msrp_link(
    session: Session,
    link: JatoMsrpLink,
) -> JatoMsrpLink:
    session.add(link)
    return link


def list_jato_msrp_links_for_key(
    session: Session,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str | None,
    *,
    is_active: bool | None = None,
) -> list[JatoMsrpLink]:
    normalized_powertrain = _normalize_powertrain(jato_powertrain)
    stmt: Select[tuple[JatoMsrpLink]] = select(JatoMsrpLink).where(
        JatoMsrpLink.country == country,
        JatoMsrpLink.brand == brand,
        JatoMsrpLink.jato_model == jato_model,
        JatoMsrpLink.jato_trim == jato_trim,
    )
    if normalized_powertrain:
        stmt = stmt.where(
            JatoMsrpLink.jato_powertrain.in_([normalized_powertrain, ""])
        ).order_by(
            case(
                (JatoMsrpLink.jato_powertrain == normalized_powertrain, 0),
                else_=1,
            ),
            JatoMsrpLink.is_active.desc(),
            JatoMsrpLink.confidence.desc(),
            JatoMsrpLink.updated_at_utc.desc(),
        )
    else:
        stmt = stmt.where(JatoMsrpLink.jato_powertrain == "").order_by(
            JatoMsrpLink.is_active.desc(),
            JatoMsrpLink.confidence.desc(),
            JatoMsrpLink.updated_at_utc.desc(),
        )
    if is_active is not None:
        stmt = stmt.where(JatoMsrpLink.is_active == is_active)
    return session.execute(stmt).scalars().all()


def find_active_jato_msrp_link(
    session: Session,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str | None,
) -> JatoMsrpLink | None:
    links = list_jato_msrp_links_for_key(
        session,
        country,
        brand,
        jato_model,
        jato_trim,
        jato_powertrain,
        is_active=True,
    )
    return links[0] if links else None


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


def add_finance_observations(
    session: Session,
    observations: list[FinanceObservation],
) -> list[FinanceObservation]:
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


def has_finance_observations_table(session: Session) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    return inspect(bind).has_table("finance_observations", schema="msrp")


def _apply_finance_observation_filters(
    stmt: Select,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    price_semantics: str | None,
    finance_type: str | None,
    has_monthly_payment: bool | None,
    has_subsidy: bool | None,
    has_net_price_after_subsidy: bool | None,
) -> Select:
    if country:
        stmt = stmt.where(
            func.lower(FinanceObservation.country).in_(
                country_filter_aliases(country)
            )
        )
    if brand:
        stmt = stmt.where(
            func.lower(FinanceObservation.brand).contains(
                brand.strip().lower()
            )
        )
    if jato_model:
        pattern = f"%{jato_model.strip().lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(FinanceObservation.jato_model).like(pattern),
                func.lower(FinanceObservation.official_model).like(pattern),
            )
        )
    if price_semantics:
        stmt = stmt.where(
            FinanceObservation.price_semantics == price_semantics
        )
    if finance_type:
        stmt = stmt.where(FinanceObservation.finance_type == finance_type)
    if has_monthly_payment is not None:
        has_monthly_value = or_(
            FinanceObservation.monthly_payment.is_not(None),
            FinanceObservation.monthly_payment_eur.is_not(None),
        )
        stmt = stmt.where(
            has_monthly_value
            if has_monthly_payment
            else ~has_monthly_value
        )
    if has_subsidy is not None:
        has_subsidy_value = or_(
            FinanceObservation.subsidy_amount.is_not(None),
            FinanceObservation.subsidy_amount_eur.is_not(None),
        )
        stmt = stmt.where(
            has_subsidy_value
            if has_subsidy
            else ~has_subsidy_value
        )
    if has_net_price_after_subsidy is not None:
        has_net_value = or_(
            FinanceObservation.net_price_after_subsidy.is_not(None),
            FinanceObservation.net_price_after_subsidy_eur.is_not(None),
        )
        stmt = stmt.where(
            has_net_value
            if has_net_price_after_subsidy
            else ~has_net_value
        )
    return stmt


def _finance_count_map(
    session: Session,
    column: object,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    price_semantics: str | None,
    finance_type: str | None,
    has_monthly_payment: bool | None,
    has_subsidy: bool | None,
    has_net_price_after_subsidy: bool | None,
) -> dict[str, int]:
    stmt = select(column, func.count()).select_from(FinanceObservation)
    stmt = _apply_finance_observation_filters(
        stmt,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
    ).group_by(column)
    return {
        str(value or "").strip() or "unknown": int(count or 0)
        for value, count in session.execute(stmt).all()
    }


def summarize_finance_observations(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    price_semantics: str | None,
    finance_type: str | None,
    has_monthly_payment: bool | None,
    has_subsidy: bool | None,
    has_net_price_after_subsidy: bool | None,
) -> dict[str, object]:
    monthly_present = or_(
        FinanceObservation.monthly_payment.is_not(None),
        FinanceObservation.monthly_payment_eur.is_not(None),
    )
    subsidy_present = or_(
        FinanceObservation.subsidy_amount.is_not(None),
        FinanceObservation.subsidy_amount_eur.is_not(None),
    )
    net_present = or_(
        FinanceObservation.net_price_after_subsidy.is_not(None),
        FinanceObservation.net_price_after_subsidy_eur.is_not(None),
    )
    stmt = select(
        func.sum(case((monthly_present, 1), else_=0)),
        func.min(FinanceObservation.monthly_payment_eur),
        func.max(FinanceObservation.monthly_payment_eur),
        func.sum(case((net_present, 1), else_=0)),
        func.min(FinanceObservation.net_price_after_subsidy_eur),
        func.max(FinanceObservation.net_price_after_subsidy_eur),
        func.sum(case((subsidy_present, 1), else_=0)),
    ).select_from(FinanceObservation)
    stmt = _apply_finance_observation_filters(
        stmt,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
    )
    (
        monthly_count,
        monthly_min,
        monthly_max,
        net_count,
        net_min,
        net_max,
        subsidy_count,
    ) = session.execute(stmt).one()

    return {
        "priceSemanticsCounts": _finance_count_map(
            session,
            FinanceObservation.price_semantics,
            country,
            brand,
            jato_model,
            price_semantics,
            finance_type,
            has_monthly_payment,
            has_subsidy,
            has_net_price_after_subsidy,
        ),
        "financeTypeCounts": _finance_count_map(
            session,
            FinanceObservation.finance_type,
            country,
            brand,
            jato_model,
            price_semantics,
            finance_type,
            has_monthly_payment,
            has_subsidy,
            has_net_price_after_subsidy,
        ),
        "monthlyPaymentCount": int(monthly_count or 0),
        "monthlyPaymentEurMin": (
            float(monthly_min) if monthly_min is not None else None
        ),
        "monthlyPaymentEurMax": (
            float(monthly_max) if monthly_max is not None else None
        ),
        "netPriceAfterSubsidyCount": int(net_count or 0),
        "netPriceAfterSubsidyEurMin": (
            float(net_min) if net_min is not None else None
        ),
        "netPriceAfterSubsidyEurMax": (
            float(net_max) if net_max is not None else None
        ),
        "subsidyObservationCount": int(subsidy_count or 0),
    }


def count_finance_observations(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    price_semantics: str | None,
    finance_type: str | None,
    has_monthly_payment: bool | None,
    has_subsidy: bool | None,
    has_net_price_after_subsidy: bool | None,
) -> int:
    stmt = select(func.count()).select_from(FinanceObservation)
    stmt = _apply_finance_observation_filters(
        stmt,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
    )
    return int(session.execute(stmt).scalar_one())


def list_finance_observations(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    price_semantics: str | None,
    finance_type: str | None,
    has_monthly_payment: bool | None,
    has_subsidy: bool | None,
    has_net_price_after_subsidy: bool | None,
    limit: int,
    offset: int,
) -> list[FinanceObservation]:
    stmt: Select[tuple[FinanceObservation]] = select(FinanceObservation)
    stmt = _apply_finance_observation_filters(
        stmt,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
    )
    stmt = stmt.order_by(
        FinanceObservation.observed_at_utc.desc(),
        FinanceObservation.created_at_utc.desc(),
    ).offset(max(0, int(offset))).limit(max(1, min(int(limit), 500)))
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
    source_code: str | None,
    source_type: str | None,
    limit: int,
) -> list[MsrpObservation]:
    stmt: Select[tuple[MsrpObservation]] = select(MsrpObservation)
    joined_source = False
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
    if source_code or source_type:
        stmt = stmt.join(
            MsrpSource,
            MsrpSource.source_id == MsrpObservation.source_id,
        )
        joined_source = True
    if source_code:
        stmt = stmt.where(MsrpSource.source_code == source_code)
    if source_type:
        stmt = stmt.where(MsrpSource.source_type == source_type)
    stmt = stmt.order_by(
        MsrpObservation.observed_at_utc.desc(),
        MsrpObservation.created_at_utc.desc(),
    ).limit(max(1, min(int(limit), 500)))
    if joined_source:
        stmt = stmt.distinct()
    return session.execute(stmt).scalars().all()


def list_materializable_observations(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
) -> list[MsrpObservation]:
    stmt: Select[tuple[MsrpObservation]] = (
        select(MsrpObservation)
        .join(MsrpSource, MsrpSource.source_id == MsrpObservation.source_id)
        .where(
            MsrpObservation.match_status.in_(["auto_accepted", "human_approved"]),
            enabled_official_msrp_source_predicate(
                MsrpSource.source_type,
                MsrpSource.enabled,
            ),
        )
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


def list_reconciliation_observations(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
) -> list[MsrpObservation]:
    stmt: Select[tuple[MsrpObservation]] = (
        select(MsrpObservation)
        .join(MsrpSource, MsrpSource.source_id == MsrpObservation.source_id)
        .where(
            MsrpObservation.match_status.in_(
                ["auto_accepted", "human_approved", "override_applied"]
            ),
            enabled_official_msrp_source_predicate(
                MsrpSource.source_type,
                MsrpSource.enabled,
            ),
        )
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
    ).limit(max(1, min(int(limit), 5000)))
    return session.execute(stmt).scalars().all()


def get_current_price_by_key(
    session: Session,
    country: str,
    brand: str,
    jato_model: str,
    jato_trim: str,
    jato_powertrain: str | None = None,
) -> CurrentPrice | None:
    stmt: Select[tuple[CurrentPrice]] = select(CurrentPrice).where(
        CurrentPrice.country == country,
        CurrentPrice.brand == brand,
        CurrentPrice.jato_model == jato_model,
        CurrentPrice.jato_trim == jato_trim,
        CurrentPrice.jato_powertrain == _normalize_powertrain(jato_powertrain),
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


def _normalize_price_alert_direction(direction: str | None) -> str:
    value = str(direction or "all").strip().lower()
    if value in {"drops", "drop", "decrease", "decreases"}:
        return "drops"
    if value in {"increases", "increase"}:
        return "increases"
    return "all"


def _price_history_event_keys_subquery(
    direction: str | None = None,
    changed_since: datetime | None = None,
    threshold_pct: float | None = None,
):
    partition_columns = (
        PriceHistory.country,
        PriceHistory.brand,
        PriceHistory.jato_model,
        PriceHistory.jato_trim,
        PriceHistory.jato_powertrain,
    )
    previous_source_value = func.lag(PriceHistory.source_msrp_value).over(
        partition_by=partition_columns,
        order_by=PriceHistory.valid_from_utc.asc(),
    )
    previous_source_currency = func.lag(PriceHistory.source_currency).over(
        partition_by=partition_columns,
        order_by=PriceHistory.valid_from_utc.asc(),
    )
    previous_msrp_value = func.lag(PriceHistory.msrp_value).over(
        partition_by=partition_columns,
        order_by=PriceHistory.valid_from_utc.asc(),
    )
    history_events = select(
        PriceHistory.country.label("country"),
        PriceHistory.brand.label("brand"),
        PriceHistory.jato_model.label("jato_model"),
        PriceHistory.jato_trim.label("jato_trim"),
        PriceHistory.jato_powertrain.label("jato_powertrain"),
        PriceHistory.msrp_value.label("msrp_value"),
        PriceHistory.source_msrp_value.label("source_msrp_value"),
        PriceHistory.source_currency.label("source_currency"),
        PriceHistory.valid_from_utc.label("valid_from_utc"),
        previous_msrp_value.label("previous_msrp_value"),
        previous_source_value.label("previous_source_msrp_value"),
        previous_source_currency.label("previous_source_currency"),
    ).subquery("price_history_events")

    stmt = select(
        history_events.c.country,
        history_events.c.brand,
        history_events.c.jato_model,
        history_events.c.jato_trim,
        history_events.c.jato_powertrain,
    ).where(
        history_events.c.previous_source_msrp_value.is_not(None),
        or_(
            history_events.c.source_msrp_value
            != history_events.c.previous_source_msrp_value,
            history_events.c.source_currency
            != history_events.c.previous_source_currency,
        ),
    )
    if changed_since is not None:
        stmt = stmt.where(history_events.c.valid_from_utc >= changed_since)
    source_currency_matches = (
        history_events.c.source_currency
        == history_events.c.previous_source_currency
    )
    source_change_value = (
        history_events.c.source_msrp_value
        - history_events.c.previous_source_msrp_value
    )
    eur_change_value = (
        history_events.c.msrp_value
        - history_events.c.previous_msrp_value
    )
    monitoring_change_value = case(
        (source_currency_matches, source_change_value),
        else_=eur_change_value,
    )
    monitoring_previous_value = case(
        (
            source_currency_matches,
            history_events.c.previous_source_msrp_value,
        ),
        else_=history_events.c.previous_msrp_value,
    )
    safe_threshold_pct = max(0.0, float(threshold_pct or 0.0))
    if safe_threshold_pct > 0:
        change_pct = func.abs(
            (monitoring_change_value / monitoring_previous_value) * 100.0
        )
        stmt = stmt.where(
            monitoring_previous_value != 0,
            change_pct >= safe_threshold_pct,
        )
    normalized_direction = _normalize_price_alert_direction(direction)
    if normalized_direction == "drops":
        stmt = stmt.where(monitoring_change_value < 0)
    elif normalized_direction == "increases":
        stmt = stmt.where(monitoring_change_value > 0)
    return (
        stmt.group_by(
            history_events.c.country,
            history_events.c.brand,
            history_events.c.jato_model,
            history_events.c.jato_trim,
            history_events.c.jato_powertrain,
        )
        .subquery("alert_keys")
    )


def _current_price_alert_keys_subquery(
    direction: str | None = None,
    changed_since: datetime | None = None,
    threshold_pct: float | None = None,
):
    return (
        _price_history_event_keys_subquery(direction, changed_since, threshold_pct)
        if direction is not None or changed_since is not None or threshold_pct is not None
        else _price_history_event_keys_subquery()
    )


def count_current_price_alerts(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    direction: str | None = None,
    changed_since: datetime | None = None,
    threshold_pct: float | None = None,
) -> int:
    if not has_price_history_table(session):
        return 0

    filtered_current_prices = _apply_current_price_filters(
        select(
            CurrentPrice.country.label("country"),
            CurrentPrice.brand.label("brand"),
            CurrentPrice.jato_model.label("jato_model"),
            CurrentPrice.jato_trim.label("jato_trim"),
            CurrentPrice.jato_powertrain.label("jato_powertrain"),
        ),
        country,
        brand,
        jato_model,
    ).subquery("filtered_current_prices")

    alert_keys = _current_price_alert_keys_subquery(direction, changed_since, threshold_pct)
    stmt = select(func.count()).select_from(
        filtered_current_prices.join(
            alert_keys,
            and_(
                filtered_current_prices.c.country == alert_keys.c.country,
                filtered_current_prices.c.brand == alert_keys.c.brand,
                filtered_current_prices.c.jato_model
                == alert_keys.c.jato_model,
                filtered_current_prices.c.jato_trim == alert_keys.c.jato_trim,
                filtered_current_prices.c.jato_powertrain
                == alert_keys.c.jato_powertrain,
            ),
        )
    )
    return int(session.execute(stmt).scalar_one())


def list_current_price_alerts(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    offset: int,
    direction: str | None = None,
    changed_since: datetime | None = None,
    threshold_pct: float | None = None,
) -> list[CurrentPrice]:
    if not has_price_history_table(session):
        return []

    alert_keys = _current_price_alert_keys_subquery(direction, changed_since, threshold_pct)
    stmt: Select[tuple[CurrentPrice]] = select(CurrentPrice).join(
        alert_keys,
        and_(
            CurrentPrice.country == alert_keys.c.country,
            CurrentPrice.brand == alert_keys.c.brand,
            CurrentPrice.jato_model == alert_keys.c.jato_model,
            CurrentPrice.jato_trim == alert_keys.c.jato_trim,
            CurrentPrice.jato_powertrain == alert_keys.c.jato_powertrain,
        ),
    )
    stmt = _apply_current_price_filters(stmt, country, brand, jato_model)
    stmt = stmt.order_by(
        CurrentPrice.last_price_change_at_utc.desc().nullslast(),
        CurrentPrice.updated_at_utc.desc(),
        CurrentPrice.country.asc(),
        CurrentPrice.brand.asc(),
        CurrentPrice.jato_model.asc(),
    ).offset(max(0, int(offset))).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()


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
    jato_powertrain: str | None = None,
) -> PriceHistory | None:
    """Return the currently open price period (valid_to_utc IS NULL)."""
    stmt: Select[tuple[PriceHistory]] = select(PriceHistory).where(
        PriceHistory.country == country,
        PriceHistory.brand == brand,
        PriceHistory.jato_model == jato_model,
        PriceHistory.jato_trim == jato_trim,
        PriceHistory.jato_powertrain == _normalize_powertrain(jato_powertrain),
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
    jato_powertrain: str | None,
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
    if jato_powertrain is not None:
        stmt = stmt.where(
            PriceHistory.jato_powertrain
            == _normalize_powertrain(jato_powertrain)
        )
    stmt = stmt.order_by(
        PriceHistory.country.asc(),
        PriceHistory.brand.asc(),
        PriceHistory.jato_model.asc(),
        PriceHistory.jato_trim.asc(),
        PriceHistory.jato_powertrain.asc(),
        PriceHistory.valid_from_utc.desc(),
    ).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()
