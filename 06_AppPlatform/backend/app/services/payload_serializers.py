"""Shared payload serialization for ORM models → camelCase dicts.

Single source of truth for all JSON payloads returned from service
layer.  Import these helpers instead of redefining them per service.
"""
from app.db.models import (
    CurrentPrice,
    FinanceObservation,
    JatoMsrpLink,
    MatchOverride,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ReviewCase,
    ReviewDecision,
    ScrapeBatch,
)
from app.services.country_service import to_display_country


def _optional_text(value: object | None) -> str | None:
    text = str(value or "").strip()
    return text or None


def scrape_batch_payload(
    batch: ScrapeBatch,
) -> dict[str, object]:
    return {
        "scrapeBatchId": str(batch.scrape_batch_id),
        "batchCode": batch.batch_code,
        "triggerType": batch.trigger_type,
        "scopeCountry": to_display_country(batch.scope_country),
        "scopeBrands": batch.scope_brands_json,
        "candidateCount": batch.candidate_count,
        "successCount": batch.success_count,
        "reviewRequiredCount": batch.review_required_count,
        "failedCount": batch.failed_count,
        "status": batch.status,
        "startedAtUtc": (
            batch.started_at_utc.isoformat()
            if batch.started_at_utc is not None
            else None
        ),
        "finishedAtUtc": (
            batch.finished_at_utc.isoformat()
            if batch.finished_at_utc is not None
            else None
        ),
        "notes": batch.notes,
    }


def observation_payload(
    obs: MsrpObservation,
    source: MsrpSource | None = None,
) -> dict[str, object]:
    payload = {
        "observationId": str(obs.observation_id),
        "scrapeBatchId": str(obs.scrape_batch_id),
        "sourceId": str(obs.source_id),
        "country": to_display_country(obs.country),
        "brand": obs.brand,
        "jatoModel": obs.jato_model,
        "jatoTrim": obs.jato_trim,
        "jatoPowertrain": _optional_text(obs.jato_powertrain),
        "officialModel": obs.official_model,
        "officialTrim": obs.official_trim,
        "officialEdition": obs.official_edition,
        "officialPowertrain": obs.official_powertrain,
        "msrpValue": float(obs.msrp_value),
        "currency": obs.currency,
        "sourceMsrpValue": float(obs.source_msrp_value),
        "sourceCurrency": obs.source_currency,
        "fxRateToEur": float(obs.fx_rate_to_eur),
        "fxRateAsOfDate": obs.fx_rate_as_of_date.isoformat(),
        "fxSource": obs.fx_source,
        "taxIncluded": obs.tax_included,
        "priceLabel": obs.price_label,
        "availabilityText": obs.availability_text,
        "observedAtUtc": obs.observed_at_utc.isoformat(),
        "sourceUrl": obs.source_url,
        "sourceSnapshotPath": obs.source_snapshot_path,
        "sourcePayloadHash": obs.source_payload_hash,
        "extractionVersion": obs.extraction_version,
        "matchConfidence": float(obs.match_confidence),
        "matchStatus": obs.match_status,
        "matchReason": obs.match_reason_json,
        "sourceContext": obs.source_context_json,
        "createdAtUtc": obs.created_at_utc.isoformat(),
        "updatedAtUtc": obs.updated_at_utc.isoformat(),
    }
    if source is not None:
        payload.update(
            {
                "sourceCode": source.source_code,
                "sourceType": source.source_type,
                "extractorName": source.extractor_name,
                "extractorVersion": source.extractor_version,
            }
        )
    return payload


def _optional_float(value: object | None) -> float | None:
    if value is None:
        return None
    return float(value)


def finance_observation_payload(
    item: FinanceObservation,
) -> dict[str, object]:
    return {
        "financeObservationId": str(item.finance_observation_id),
        "observationId": str(item.observation_id),
        "scrapeBatchId": str(item.scrape_batch_id),
        "country": to_display_country(item.country),
        "brand": item.brand,
        "jatoModel": item.jato_model,
        "jatoTrim": item.jato_trim,
        "jatoPowertrain": _optional_text(item.jato_powertrain),
        "officialModel": item.official_model,
        "officialTrim": item.official_trim,
        "officialEdition": item.official_edition,
        "officialPowertrain": item.official_powertrain,
        "priceSemantics": item.price_semantics,
        "financeType": item.finance_type,
        "monthlyPayment": _optional_float(item.monthly_payment),
        "monthlyPaymentEur": _optional_float(item.monthly_payment_eur),
        "downPayment": _optional_float(item.down_payment),
        "downPaymentEur": _optional_float(item.down_payment_eur),
        "downPaymentPct": _optional_float(item.down_payment_pct),
        "termMonths": item.term_months,
        "apr": _optional_float(item.apr),
        "effectiveApr": _optional_float(item.effective_apr),
        "balloonPayment": _optional_float(item.balloon_payment),
        "balloonPaymentEur": _optional_float(item.balloon_payment_eur),
        "totalCreditCost": _optional_float(item.total_credit_cost),
        "totalCreditCostEur": _optional_float(item.total_credit_cost_eur),
        "totalAmountPayable": _optional_float(item.total_amount_payable),
        "totalAmountPayableEur": _optional_float(
            item.total_amount_payable_eur
        ),
        "annualMileageLimit": item.annual_mileage_limit,
        "offerValidUntil": (
            item.offer_valid_until.isoformat()
            if item.offer_valid_until is not None
            else None
        ),
        "subsidyAmount": _optional_float(item.subsidy_amount),
        "subsidyAmountEur": _optional_float(item.subsidy_amount_eur),
        "netPriceAfterSubsidy": _optional_float(
            item.net_price_after_subsidy
        ),
        "netPriceAfterSubsidyEur": _optional_float(
            item.net_price_after_subsidy_eur
        ),
        "currency": item.currency,
        "sourceUrl": item.source_url,
        "observedAtUtc": item.observed_at_utc.isoformat(),
        "financeContext": item.finance_context_json,
        "createdAtUtc": item.created_at_utc.isoformat(),
        "updatedAtUtc": item.updated_at_utc.isoformat(),
    }


def current_price_payload(
    cp: CurrentPrice,
    source: MsrpSource | None = None,
) -> dict[str, object]:
    payload = {
        "currentPriceId": str(cp.current_price_id),
        "country": to_display_country(cp.country),
        "brand": cp.brand,
        "jatoModel": cp.jato_model,
        "jatoTrim": cp.jato_trim,
        "jatoPowertrain": _optional_text(cp.jato_powertrain),
        "officialModel": cp.official_model,
        "officialTrim": cp.official_trim,
        "officialEdition": cp.official_edition,
        "officialPowertrain": cp.official_powertrain,
        "effectiveObservationId": str(
            cp.effective_observation_id
        ),
        "currentMsrpValue": float(cp.current_msrp_value),
        "currency": cp.currency,
        "sourceMsrpValue": float(cp.source_msrp_value),
        "sourceCurrency": cp.source_currency,
        "fxRateToEur": float(cp.fx_rate_to_eur),
        "fxRateAsOfDate": cp.fx_rate_as_of_date.isoformat(),
        "fxSource": cp.fx_source,
        "taxIncluded": cp.tax_included,
        "matchConfidence": float(cp.match_confidence),
        "matchStatus": cp.match_status,
        "sourceUrl": cp.source_url,
        "sourceSnapshotPath": cp.source_snapshot_path,
        "lastPriceChangeAtUtc": (
            cp.last_price_change_at_utc.isoformat()
            if cp.last_price_change_at_utc is not None
            else None
        ),
        "updatedAtUtc": cp.updated_at_utc.isoformat(),
    }
    if source is not None:
        payload.update(
            {
                "sourceCode": source.source_code,
                "sourceType": source.source_type,
                "extractorName": source.extractor_name,
                "extractorVersion": source.extractor_version,
            }
        )
    return payload


def review_case_payload(
    rc: ReviewCase,
    obs: MsrpObservation | None = None,
    source: MsrpSource | None = None,
) -> dict[str, object]:
    payload = {
        "reviewCaseId": str(rc.review_case_id),
        "observationId": str(rc.observation_id),
        "country": to_display_country(rc.country),
        "brand": rc.brand,
        "jatoModel": rc.jato_model,
        "jatoTrim": rc.jato_trim,
        "jatoPowertrain": _optional_text(rc.jato_powertrain),
        "officialModel": rc.official_model,
        "officialTrim": rc.official_trim,
        "officialEdition": rc.official_edition,
        "officialPowertrain": rc.official_powertrain,
        "candidateMatches": rc.candidate_matches_json,
        "matchConfidence": float(rc.match_confidence),
        "reviewStatus": rc.review_status,
        "sourceUrl": rc.source_url,
        "sourceSnapshotPath": rc.source_snapshot_path,
        "currentAssignee": rc.current_assignee,
        "createdAtUtc": rc.created_at_utc.isoformat(),
        "updatedAtUtc": rc.updated_at_utc.isoformat(),
    }
    if source is not None:
        payload.update(
            {
                "sourceCode": source.source_code,
                "sourceRegistryUrl": source.source_url,
                "sourceType": source.source_type,
                "extractorName": source.extractor_name,
                "extractorVersion": source.extractor_version,
            }
        )
    if obs is not None:
        payload.update(
            {
                "msrpValue": float(obs.msrp_value),
                "currency": obs.currency,
                "sourceMsrpValue": float(obs.source_msrp_value),
                "sourceCurrency": obs.source_currency,
                "fxRateToEur": float(obs.fx_rate_to_eur),
                "fxRateAsOfDate": obs.fx_rate_as_of_date.isoformat(),
                "fxSource": obs.fx_source,
                "priceLabel": obs.price_label,
                "observedAtUtc": obs.observed_at_utc.isoformat(),
                "matchReason": obs.match_reason_json,
            }
        )
    return payload


def review_decision_payload(
    rd: ReviewDecision,
) -> dict[str, object]:
    return {
        "reviewDecisionId": str(rd.review_decision_id),
        "reviewCaseId": str(rd.review_case_id),
        "observationId": str(rd.observation_id),
        "decision": rd.decision,
        "decidedOfficialModel": rd.decided_official_model,
        "decidedOfficialTrim": rd.decided_official_trim,
        "note": rd.note,
        "decidedBy": rd.decided_by,
        "decidedAtUtc": rd.decided_at_utc.isoformat(),
    }


def source_payload(
    source: MsrpSource,
) -> dict[str, object]:
    return {
        "sourceId": str(source.source_id),
        "sourceCode": source.source_code,
        "country": to_display_country(source.country),
        "brand": source.brand,
        "sourceUrl": source.source_url,
        "sourceType": source.source_type,
        "tier": int(source.tier if source.tier is not None else 3),
        "extractorName": source.extractor_name,
        "extractorVersion": source.extractor_version,
        "priceSemantics": source.price_semantics,
        "requiresLocation": source.requires_location,
        "enabled": source.enabled,
        "notes": source.notes,
        "createdAtUtc": source.created_at_utc.isoformat(),
        "updatedAtUtc": source.updated_at_utc.isoformat(),
    }


def jato_msrp_link_payload(
    link: JatoMsrpLink,
) -> dict[str, object]:
    return {
        "linkId": str(link.link_id),
        "country": to_display_country(link.country),
        "brand": link.brand,
        "jatoModel": link.jato_model,
        "jatoTrim": link.jato_trim,
        "jatoPowertrain": _optional_text(link.jato_powertrain),
        "officialModel": link.official_model,
        "officialTrim": link.official_trim,
        "officialEdition": _optional_text(link.official_edition),
        "officialPowertrain": _optional_text(link.official_powertrain),
        "confidence": int(link.confidence),
        "linkSource": link.link_source,
        "isActive": bool(link.is_active),
        "notes": link.notes,
        "createdAtUtc": link.created_at_utc.isoformat(),
        "updatedAtUtc": link.updated_at_utc.isoformat(),
    }


def override_payload(
    o: MatchOverride,
) -> dict[str, object]:
    return {
        "overrideId": str(o.override_id),
        "country": to_display_country(o.country),
        "brand": o.brand,
        "jatoModel": o.jato_model,
        "jatoTrim": o.jato_trim,
        "jatoPowertrain": _optional_text(o.jato_powertrain),
        "officialModel": o.official_model,
        "officialTrim": o.official_trim,
        "validFromDate": o.valid_from_date.isoformat(),
        "validToDate": (
            o.valid_to_date.isoformat()
            if o.valid_to_date is not None
            else None
        ),
        "overrideReason": o.override_reason,
        "createdBy": o.created_by,
        "createdAtUtc": o.created_at_utc.isoformat(),
        "updatedAtUtc": o.updated_at_utc.isoformat(),
    }


def price_history_payload(
    ph: PriceHistory,
) -> dict[str, object]:
    return {
        "priceHistoryId": str(ph.price_history_id),
        "country": to_display_country(ph.country),
        "brand": ph.brand,
        "jatoModel": ph.jato_model,
        "jatoTrim": ph.jato_trim,
        "jatoPowertrain": _optional_text(ph.jato_powertrain),
        "msrpValue": float(ph.msrp_value),
        "currency": ph.currency,
        "sourceMsrpValue": float(ph.source_msrp_value),
        "sourceCurrency": ph.source_currency,
        "validFromUtc": ph.valid_from_utc.isoformat(),
        "validToUtc": (
            ph.valid_to_utc.isoformat()
            if ph.valid_to_utc is not None
            else None
        ),
        "lastConfirmedAtUtc": ph.last_confirmed_at_utc.isoformat(),
        "startedByObservationId": str(ph.started_by_observation_id),
        "endedByObservationId": (
            str(ph.ended_by_observation_id)
            if ph.ended_by_observation_id is not None
            else None
        ),
        "lastConfirmedByObservationId": str(
            ph.last_confirmed_by_observation_id
        ),
        "createdAtUtc": ph.created_at_utc.isoformat(),
    }
