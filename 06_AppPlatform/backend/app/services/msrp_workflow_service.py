from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import (
    CurrentPrice,
    MsrpObservation,
    PriceHistory,
    ReviewCase,
    ReviewDecision,
    ScrapeBatch,
)
from app.infra import msrp_repository as msrp_repo
from app.infra import review_repository as review_repo
from app.services.fx_service import convert_amount_to_eur
from app.services.payload_serializers import (
    current_price_payload,
    observation_payload,
    price_history_payload,
    review_case_payload,
    review_decision_payload,
    scrape_batch_payload,
)


ELIGIBLE_CURRENT_PRICE_STATUSES = {
    "auto_accepted",
    "human_approved",
    "override_applied",
}
REVIEW_REQUIRED_STATUS = "review_required"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _commit_or_conflict(session: Session, detail: str) -> None:
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=detail) from exc


def materialize_current_price_from_observation(
    session: Session,
    observation: MsrpObservation,
    *,
    price_history_enabled: bool | None = None,
) -> CurrentPrice | None:
    if observation.match_status not in ELIGIBLE_CURRENT_PRICE_STATUSES:
        return None

    current_price = msrp_repo.get_current_price_by_key(
        session,
        observation.country,
        observation.brand,
        observation.jato_model,
        observation.jato_trim,
    )
    last_price_change_at_utc = observation.observed_at_utc
    price_changed = False
    if current_price is not None:
        existing_source = float(current_price.source_msrp_value)
        incoming_source = float(observation.source_msrp_value)
        same_currency = (
            current_price.source_currency
            == observation.source_currency
        )
        if same_currency and existing_source == incoming_source:
            last_price_change_at_utc = current_price.last_price_change_at_utc
        else:
            price_changed = True
        current_price.official_model = observation.official_model
        current_price.official_trim = observation.official_trim
        current_price.official_edition = observation.official_edition
        current_price.official_powertrain = observation.official_powertrain
        current_price.jato_powertrain = observation.jato_powertrain
        current_price.effective_observation_id = observation.observation_id
        current_price.current_msrp_value = observation.msrp_value
        current_price.currency = observation.currency
        current_price.source_msrp_value = observation.source_msrp_value
        current_price.source_currency = observation.source_currency
        current_price.fx_rate_to_eur = observation.fx_rate_to_eur
        current_price.fx_rate_as_of_date = observation.fx_rate_as_of_date
        current_price.fx_source = observation.fx_source
        current_price.tax_included = observation.tax_included
        current_price.match_confidence = observation.match_confidence
        current_price.match_status = observation.match_status
        current_price.source_url = observation.source_url
        current_price.source_snapshot_path = observation.source_snapshot_path
        current_price.last_price_change_at_utc = last_price_change_at_utc
        current_price.updated_at_utc = _utc_now()
    else:
        price_changed = True
        current_price = CurrentPrice(
            country=observation.country,
            brand=observation.brand,
            jato_model=observation.jato_model,
            jato_trim=observation.jato_trim,
            jato_powertrain=observation.jato_powertrain,
            official_model=observation.official_model,
            official_trim=observation.official_trim,
            official_edition=observation.official_edition,
            official_powertrain=observation.official_powertrain,
            effective_observation_id=observation.observation_id,
            current_msrp_value=observation.msrp_value,
            currency=observation.currency,
            source_msrp_value=observation.source_msrp_value,
            source_currency=observation.source_currency,
            fx_rate_to_eur=observation.fx_rate_to_eur,
            fx_rate_as_of_date=observation.fx_rate_as_of_date,
            fx_source=observation.fx_source,
            tax_included=observation.tax_included,
            match_confidence=observation.match_confidence,
            match_status=observation.match_status,
            source_url=observation.source_url,
            source_snapshot_path=observation.source_snapshot_path,
            last_price_change_at_utc=observation.observed_at_utc,
        )
        msrp_repo.add_current_price(session, current_price)

    if price_history_enabled is None:
        price_history_enabled = msrp_repo.has_price_history_table(session)

    if price_history_enabled:
        open_period = msrp_repo.get_open_price_period(
            session,
            observation.country,
            observation.brand,
            observation.jato_model,
            observation.jato_trim,
        )
        if price_changed or open_period is None:
            _record_price_period(session, observation, open_period=open_period)
        else:
            _refresh_open_price_period(open_period, observation)

    return current_price


def _record_price_period(
    session: Session,
    observation: MsrpObservation,
    *,
    open_period: PriceHistory | None = None,
) -> None:
    """Close any open price period and open a new one.

    This produces compressed time-series rows:
    "(country, brand, model, trim) was at price X from valid_from to valid_to."
    """
    if open_period is None:
        open_period = msrp_repo.get_open_price_period(
            session,
            observation.country,
            observation.brand,
            observation.jato_model,
            observation.jato_trim,
        )
    if open_period is not None:
        open_period.valid_to_utc = observation.observed_at_utc
        open_period.ended_by_observation_id = observation.observation_id

    new_period = PriceHistory(
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        msrp_value=observation.msrp_value,
        currency=observation.currency,
        source_msrp_value=observation.source_msrp_value,
        source_currency=observation.source_currency,
        valid_from_utc=observation.observed_at_utc,
        last_confirmed_at_utc=observation.observed_at_utc,
        started_by_observation_id=observation.observation_id,
        last_confirmed_by_observation_id=observation.observation_id,
    )
    msrp_repo.add_price_history(session, new_period)


def _refresh_open_price_period(
    open_period: PriceHistory,
    observation: MsrpObservation,
) -> None:
    if observation.observed_at_utc < open_period.last_confirmed_at_utc:
        return
    open_period.last_confirmed_at_utc = observation.observed_at_utc
    open_period.last_confirmed_by_observation_id = (
        observation.observation_id
    )


def _ensure_review_case(
    session: Session,
    observation: MsrpObservation,
    candidate_matches_json: list[dict[str, object]] | None,
) -> ReviewCase:
    review_case = review_repo.get_review_case_by_observation(
        session,
        observation.observation_id,
    )
    if review_case is not None:
        review_case.review_status = "open"
        review_case.candidate_matches_json = candidate_matches_json
        review_case.match_confidence = observation.match_confidence
        review_case.official_model = observation.official_model
        review_case.official_trim = observation.official_trim
        review_case.official_edition = observation.official_edition
        review_case.official_powertrain = observation.official_powertrain
        review_case.jato_powertrain = observation.jato_powertrain
        review_case.source_url = observation.source_url
        review_case.source_snapshot_path = observation.source_snapshot_path
        review_case.updated_at_utc = _utc_now()
        return review_case

    review_case = ReviewCase(
        observation_id=observation.observation_id,
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=observation.jato_powertrain,
        official_model=observation.official_model,
        official_trim=observation.official_trim,
        official_edition=observation.official_edition,
        official_powertrain=observation.official_powertrain,
        candidate_matches_json=candidate_matches_json,
        match_confidence=observation.match_confidence,
        review_status="open",
        source_url=observation.source_url,
        source_snapshot_path=observation.source_snapshot_path,
        current_assignee=None,
    )
    review_repo.add_review_case(session, review_case)
    return review_case


def create_scrape_batch_ingest(
    session: Session,
    data: dict[str, object],
) -> dict[str, object]:
    observations_payload = list(data.get("observations") or [])
    failed_count = max(0, int(data.get("failed_count") or 0))
    if not observations_payload and failed_count <= 0:
        raise HTTPException(
            status_code=400,
            detail=(
                "Scrape batch ingest requires observations or failed_count."
            ),
        )

    scope_country = str(data.get("scope_country") or "").strip()
    if not scope_country:
        raise HTTPException(
            status_code=400,
            detail="scope_country is required",
        )

    scope_brands = [
        str(item).strip()
        for item in list(data.get("scope_brands") or [])
        if str(item).strip()
    ]
    batch = ScrapeBatch(
        batch_code=str(data.get("batch_code") or "").strip(),
        trigger_type=str(data.get("trigger_type") or "manual").strip(),
        scope_country=scope_country,
        scope_brands_json=scope_brands or None,
        candidate_count=0,
        success_count=0,
        review_required_count=0,
        failed_count=0,
        status="pending",
        started_at_utc=data.get("started_at_utc") or _utc_now(),
        finished_at_utc=data.get("finished_at_utc"),
        notes=str(data.get("notes") or "").strip() or None,
    )
    msrp_repo.add_scrape_batch(session, batch)
    session.flush()

    observations: list[MsrpObservation] = []
    review_cases: list[ReviewCase] = []
    current_prices: list[CurrentPrice] = []
    price_history_enabled = msrp_repo.has_price_history_table(session)

    for item in observations_payload:
        source = msrp_repo.get_source(session, UUID(str(item["source_id"])))
        if source is None:
            raise HTTPException(
                status_code=400,
                detail=f"MSRP source not found: {item['source_id']}",
            )
        observation_country = str(item.get("country") or "").strip()
        if observation_country != scope_country:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Observation country must match scrape batch "
                    "scope_country."
                ),
            )
        observation_brand = str(item.get("brand") or "").strip()
        if scope_brands and observation_brand not in scope_brands:
            raise HTTPException(
                status_code=400,
                detail="Observation brand must stay within scope_brands.",
            )
        observed_at_utc = item.get("observed_at_utc") or _utc_now()
        source_msrp_value = float(item["msrp_value"])
        source_currency = str(item.get("currency") or "").strip().upper()
        msrp_value_eur, fx_quote = convert_amount_to_eur(
            source_msrp_value,
            source_currency,
            observed_at_utc,
        )
        observation = MsrpObservation(
            scrape_batch_id=batch.scrape_batch_id,
            source_id=source.source_id,
            country=observation_country,
            brand=observation_brand,
            jato_model=str(item.get("jato_model") or "").strip(),
            jato_trim=str(item.get("jato_trim") or "").strip(),
            jato_powertrain=(
                str(item.get("jato_powertrain")).strip()
                if item.get("jato_powertrain") is not None
                else None
            ),
            official_model=str(item.get("official_model") or "").strip(),
            official_trim=str(item.get("official_trim") or "").strip(),
            official_edition=(
                str(item.get("official_edition")).strip()
                if item.get("official_edition") is not None
                else None
            ),
            official_powertrain=(
                str(item.get("official_powertrain")).strip()
                if item.get("official_powertrain") is not None
                else None
            ),
            msrp_value=msrp_value_eur,
            currency="EUR",
            source_msrp_value=source_msrp_value,
            source_currency=source_currency,
            fx_rate_to_eur=fx_quote.rate_to_eur,
            fx_rate_as_of_date=fx_quote.as_of_date,
            fx_source=fx_quote.source,
            tax_included=bool(item.get("tax_included")),
            price_label=str(item.get("price_label") or "unknown").strip(),
            availability_text=(
                str(item.get("availability_text")).strip()
                if item.get("availability_text") is not None
                else None
            ),
            observed_at_utc=observed_at_utc,
            source_url=str(item.get("source_url") or "").strip(),
            source_snapshot_path=(
                str(item.get("source_snapshot_path")).strip()
                if item.get("source_snapshot_path") is not None
                else None
            ),
            source_payload_hash=(
                str(item.get("source_payload_hash")).strip()
                if item.get("source_payload_hash") is not None
                else None
            ),
            extraction_version=str(
                item.get("extraction_version") or ""
            ).strip(),
            match_confidence=float(item.get("match_confidence") or 0.0),
            match_status=str(
                item.get("match_status") or "review_required"
            ).strip(),
            match_reason_json=item.get("match_reason_json"),
        )
        observations.append(observation)

    msrp_repo.add_observations(session, observations)
    session.flush()

    success_count = 0
    review_required_count = 0
    override_applied_count = 0
    for observation, payload in zip(
        observations,
        observations_payload,
        strict=False,
    ):
        if observation.match_status == REVIEW_REQUIRED_STATUS:
            override = review_repo.find_applicable_override(
                session,
                observation.country,
                observation.brand,
                observation.jato_model,
                observation.jato_trim,
                observation.observed_at_utc.date()
                if hasattr(observation.observed_at_utc, "date")
                else observation.observed_at_utc,
            )
            if override is not None:
                observation.official_model = override.official_model
                observation.official_trim = override.official_trim
                observation.match_status = "override_applied"
                existing_reason = observation.match_reason_json or {}
                if not isinstance(existing_reason, dict):
                    existing_reason = {"previous": existing_reason}
                existing_reason["overrideApplied"] = {
                    "overrideId": str(override.override_id),
                    "overrideReason": override.override_reason,
                    "appliedOfficialModel": override.official_model,
                    "appliedOfficialTrim": override.official_trim,
                    "validFrom": override.valid_from_date.isoformat(),
                    "validTo": (
                        override.valid_to_date.isoformat()
                        if override.valid_to_date
                        else None
                    ),
                }
                observation.match_reason_json = existing_reason
                override_applied_count += 1
                # Fall through to current price materialization below
            else:
                review_case = _ensure_review_case(
                    session,
                    observation,
                    payload.get("candidate_matches_json"),
                )
                review_cases.append(review_case)
                review_required_count += 1
                continue
        if observation.match_status in ELIGIBLE_CURRENT_PRICE_STATUSES:
            current_price = materialize_current_price_from_observation(
                session,
                observation,
                price_history_enabled=price_history_enabled,
            )
            if current_price is not None:
                current_prices.append(current_price)
            success_count += 1

    batch.candidate_count = len(observations) + failed_count
    batch.success_count = success_count
    batch.review_required_count = review_required_count
    batch.failed_count = failed_count
    if failed_count > 0:
        batch.status = "completed_with_errors"
    elif review_required_count > 0:
        batch.status = "completed_with_review"
    else:
        batch.status = "completed"
    batch.finished_at_utc = batch.finished_at_utc or _utc_now()

    _commit_or_conflict(session, "Scrape batch code already exists")
    session.refresh(batch)
    return {
        "scrapeBatch": scrape_batch_payload(batch),
        "observationRows": len(observations),
        "reviewCasesCreated": len(review_cases),
        "overrideAppliedCount": override_applied_count,
        "currentPricesTouched": len(current_prices),
        "sampleObservations": [
            observation_payload(item) for item in observations[:10]
        ],
        "sampleReviewCases": [
            review_case_payload(item) for item in review_cases[:10]
        ],
        "sampleCurrentPrices": [
            current_price_payload(item) for item in current_prices[:10]
        ],
    }


def list_current_prices(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    offset: int,
) -> dict[str, object]:
    total = msrp_repo.count_current_prices(session, country, brand, jato_model)
    items = msrp_repo.list_current_prices(
        session,
        country,
        brand,
        jato_model,
        limit,
        offset,
    )
    price_alert_count = msrp_repo.count_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
    )
    return {
        "rows": len(items),
        "total": total,
        "limit": limit,
        "offset": offset,
        "priceAlertCount": price_alert_count,
        "items": [current_price_payload(item) for item in items],
    }


def _retire_active_overrides(
    session: Session,
    observation: MsrpObservation,
    *,
    as_of_date,
) -> int:
    active_overrides = review_repo.list_active_match_overrides_by_key(
        session,
        observation.country,
        observation.brand,
        observation.jato_model,
        observation.jato_trim,
        as_of_date,
    )
    if not active_overrides:
        return 0

    retire_before = _utc_now().date() - timedelta(days=1)
    retired = 0
    for override in active_overrides:
        if override.valid_from_date <= retire_before:
            if (
                override.valid_to_date is None
                or override.valid_to_date > retire_before
            ):
                override.valid_to_date = retire_before
                override.updated_at_utc = _utc_now()
                retired += 1
        else:
            review_repo.delete_match_override(session, override)
            retired += 1
    return retired


def remap_current_price(
    session: Session,
    current_price_id: str,
    data: dict[str, object],
) -> dict[str, object]:
    current_price = msrp_repo.get_current_price(
        session,
        UUID(current_price_id),
    )
    if current_price is None:
        raise HTTPException(status_code=404, detail="Current price not found")

    decided_by = str(data.get("decided_by") or "").strip()
    if not decided_by:
        raise HTTPException(
            status_code=400,
            detail="decided_by is required",
        )

    observation = msrp_repo.get_observation(
        session,
        current_price.effective_observation_id,
    )
    if observation is None:
        raise HTTPException(status_code=404, detail="Observation not found")

    existing_review_case = review_repo.get_review_case_by_observation(
        session,
        observation.observation_id,
    )
    review_case = _ensure_review_case(
        session,
        observation,
        (
            existing_review_case.candidate_matches_json
            if existing_review_case is not None
            else None
        ),
    )

    now = _utc_now()
    review_case.review_status = "open"
    review_case.current_assignee = None
    review_case.updated_at_utc = now

    observation.match_status = REVIEW_REQUIRED_STATUS
    observation.updated_at_utc = now
    match_reason = observation.match_reason_json or {}
    if not isinstance(match_reason, dict):
        match_reason = {"previous": match_reason}
    match_reason["returnedFromCurrentPrice"] = {
        "currentPriceId": str(current_price.current_price_id),
        "returnedBy": decided_by,
        "returnedAtUtc": now.isoformat(),
        "note": str(data.get("note") or "").strip() or None,
    }
    observation.match_reason_json = match_reason

    overrides_retired = _retire_active_overrides(
        session,
        observation,
        as_of_date=now.date(),
    )

    if msrp_repo.has_price_history_table(session):
        open_period = msrp_repo.get_open_price_period(
            session,
            current_price.country,
            current_price.brand,
            current_price.jato_model,
            current_price.jato_trim,
        )
        if open_period is not None:
            open_period.valid_to_utc = now
            open_period.ended_by_observation_id = None

    msrp_repo.delete_current_price(session, current_price)

    reopen_decision = ReviewDecision(
        review_case_id=review_case.review_case_id,
        observation_id=observation.observation_id,
        decision="reopen",
        decided_official_model=observation.official_model,
        decided_official_trim=observation.official_trim,
        note=(
            str(data.get("note") or "").strip()
            or "Returned from MSRP current price"
        ),
        decided_by=decided_by,
    )
    review_repo.add_review_decision(session, reopen_decision)

    _commit_or_conflict(
        session,
        "Current price remap conflicted with existing data",
    )

    session.refresh(review_case)
    session.refresh(reopen_decision)
    source = msrp_repo.get_source(session, observation.source_id)

    return {
        "currentPriceId": current_price_id,
        "observationId": str(observation.observation_id),
        "reviewCase": review_case_payload(review_case, observation, source),
        "decision": review_decision_payload(reopen_decision),
        "overridesRetired": overrides_retired,
        "removedFromCurrentPrices": True,
    }


def list_price_history(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    jato_trim: str | None,
    limit: int,
) -> dict[str, object]:
    if not msrp_repo.has_price_history_table(session):
        return {
            "rows": 0,
            "items": [],
            "warning": "price_history_unavailable",
        }

    items = msrp_repo.list_price_history(
        session,
        country,
        brand,
        jato_model,
        jato_trim,
        limit,
    )
    return {
        "rows": len(items),
        "items": [price_history_payload(item) for item in items],
    }


def materialize_current_prices(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
) -> dict[str, object]:
    observations = msrp_repo.list_materializable_observations(
        session,
        country,
        brand,
        jato_model,
        limit,
    )
    touched: list[CurrentPrice] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    price_history_enabled = msrp_repo.has_price_history_table(session)
    for observation in observations:
        business_key = (
            observation.country,
            observation.brand,
            observation.jato_model,
            observation.jato_trim,
        )
        if business_key in seen_keys:
            continue
        seen_keys.add(business_key)
        current_price = materialize_current_price_from_observation(
            session,
            observation,
            price_history_enabled=price_history_enabled,
        )
        if current_price is not None:
            touched.append(current_price)

    _commit_or_conflict(
        session,
        "Current price materialization hit a conflict",
    )
    return {
        "candidateObservations": len(observations),
        "materializedKeys": len(touched),
        "items": [current_price_payload(item) for item in touched[:50]],
    }
